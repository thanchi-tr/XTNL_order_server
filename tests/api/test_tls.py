"""TLS: a real handshake with a throwaway self-signed certificate (Git for Windows' openssl), TLS 1.2+ only,
AEAD suites only, certificate pinning by the DER SHA-256 fingerprint, and the signed local self-check."""
from __future__ import annotations

import asyncio
import io
import logging
import ssl
import subprocess
import time
from collections.abc import Callable
from pathlib import Path

import fakeredis
import pytest

from src.api import selfcheck
from src.api.auth import Authenticator
from src.api.pinescript import PinescriptService, now_ms
from src.api.server import ApiServer, Limits
from src.api.settings import ApiSettings
from src.api.tls import (TLS12_CIPHERS, der_of, fingerprint, fingerprint_of_der, matches_pin,
                         normalise_fingerprint, pinned_client_context, server_context)

from tests.api.conftest import SECRET, SECRET_TEXT, Clock, FakeCompiler, exchange, find_openssl, request_bytes

TlsFiles = tuple[Path, Path, Path, Path]


async def serve_tls(crt: Path, key: Path, clock: Callable[[], int] = Clock()) -> tuple[asyncio.Server, int,
                                                                                PinescriptService]:
    service = PinescriptService(fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer(), db=2), FakeCompiler(),
                                clock=clock)
    api = ApiServer(Authenticator(SECRET), service, rate_per_min=600, clock=clock)
    server = await api.serve("127.0.0.1", 0, server_context(crt, key))
    return server, server.sockets[0].getsockname()[1], service


async def stop(server: asyncio.Server, service: PinescriptService) -> None:
    server.close()
    await service.close()


def test_fingerprint_matches_openssl(tls_files: TlsFiles) -> None:
    crt = tls_files[0]
    openssl = find_openssl()
    assert openssl is not None
    out = subprocess.run([openssl, "x509", "-in", str(crt), "-noout", "-fingerprint", "-sha256"],
                         check=True, capture_output=True, text=True).stdout
    assert out.strip().split("=", 1)[1] == fingerprint(crt)
    hexed = normalise_fingerprint(fingerprint(crt))
    assert len(hexed) == 64 and normalise_fingerprint(hexed.upper()) == hexed
    with pytest.raises(ValueError):
        normalise_fingerprint("AB:CD")


async def test_handshake_and_signed_health_over_tls(tls_files: TlsFiles) -> None:
    crt, key, _, _ = tls_files
    server, port, service = await serve_tls(crt, key)
    try:
        r = await exchange(port, request_bytes("GET", "/v1/health"), ssl_ctx=pinned_client_context())
        assert r.status == 200 and r.body["ok"] is True
        reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=pinned_client_context())
        sslobj = writer.get_extra_info("ssl_object")
        assert matches_pin(sslobj.getpeercert(binary_form=True), fingerprint(crt))
        assert sslobj.version() in ("TLSv1.2", "TLSv1.3")
        writer.close()
    finally:
        await stop(server, service)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
async def test_old_tls_versions_are_refused(tls_files: TlsFiles) -> None:
    crt, key, _, _ = tls_files
    server, port, service = await serve_tls(crt, key)
    try:
        ctx = pinned_client_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1
        ctx.maximum_version = ssl.TLSVersion.TLSv1_1
        try:
            ctx.set_ciphers("ALL:@SECLEVEL=0")
        except ssl.SSLError:
            pass
        with pytest.raises((ssl.SSLError, ConnectionError, OSError)):
            await asyncio.open_connection("127.0.0.1", port, ssl=ctx)
    finally:
        await stop(server, service)


async def test_tls12_offers_only_forward_secret_aead_suites(tls_files: TlsFiles) -> None:
    crt, key, _, _ = tls_files
    names = {c["name"] for c in server_context(crt, key).get_ciphers() if c["protocol"] == "TLSv1.2"}
    assert names and all(n.startswith("ECDHE-") and ("GCM" in n or "CHACHA20" in n) for n in names)
    assert "!SHA1" in TLS12_CIPHERS


async def test_plain_http_to_the_tls_port_gets_nothing(tls_files: TlsFiles) -> None:
    crt, key, _, _ = tls_files
    server, port, service = await serve_tls(crt, key)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(request_bytes("GET", "/v1/health"))
        await writer.drain()
        try:
            raw = await asyncio.wait_for(reader.read(), 12)
        except (ConnectionError, OSError):
            raw = b""
        assert b"xtnl-order-server" not in raw
        writer.close()
    finally:
        await stop(server, service)


def settings_for(crt: Path, key: Path, port: int) -> ApiSettings:
    return ApiSettings(secret=SECRET, port=port, bind="127.0.0.1", cert_path=crt, key_path=key)


async def test_selfcheck_pins_the_local_certificate(tls_files: TlsFiles) -> None:
    crt, key, other_crt, other_key = tls_files
    server, port, service = await serve_tls(crt, key, now_ms)      # the self-check signs with the real clock
    try:
        code, line = await asyncio.to_thread(selfcheck.check, settings_for(crt, key, port))
        assert code == 0 and line.startswith("healthy:") and SECRET_TEXT not in line
        code, line = await asyncio.to_thread(selfcheck.check, settings_for(other_crt, other_key, port))
        assert code == 3 and "pin mismatch" in line
    finally:
        await stop(server, service)
    code, line = await asyncio.to_thread(selfcheck.check, settings_for(crt, key, port), timeout_s=1)
    assert code == 1 and line.startswith("not reachable")


def test_selfcheck_cli(tls_files: TlsFiles, tmp_path: Path) -> None:
    crt = tls_files[0]
    out = io.StringIO()
    assert selfcheck.main(["--fingerprint"], environ={"XTNL_API_TLS_CERT": str(crt)}, out=out) == 0
    assert out.getvalue().strip() == f"sha256 fingerprint: {fingerprint(crt)}"
    out = io.StringIO()
    assert selfcheck.main([], environ={"XTNL_API_TLS_CERT": str(crt)}, out=out) == 78        # no secret
    assert "XTNL_API_SECRET" in out.getvalue()
    out = io.StringIO()
    env = {"XTNL_API_SECRET": SECRET_TEXT, "XTNL_API_TLS_CERT": str(tmp_path / "none.crt")}
    assert selfcheck.main([], environ=env, out=out) == 78
    assert SECRET_TEXT not in out.getvalue()


def test_der_reader(tls_files: TlsFiles, tmp_path: Path) -> None:
    crt = tls_files[0]
    assert fingerprint_of_der(der_of(crt)) == fingerprint(crt)
    bad = tmp_path / "bad.crt"
    bad.write_text("nothing here", encoding="ascii")
    with pytest.raises(ValueError):
        der_of(bad)


# ------------------------------------------------------------------------------------------------ caps before TLS
async def serve_tls_limited(crt: Path, key: Path, limits: Limits) -> tuple[asyncio.Server, ApiServer, int,
                                                                          PinescriptService]:
    service = PinescriptService(fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer(), db=2), FakeCompiler(),
                                clock=Clock())
    api = ApiServer(Authenticator(SECRET), service, rate_per_min=600, limits=limits, clock=Clock())
    server = await api.serve("127.0.0.1", 0, server_context(crt, key))
    return server, api, server.sockets[0].getsockname()[1], service


async def closed_without_answer(port: int, timeout: float = 2.0) -> bool:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        raw = await asyncio.wait_for(reader.read(), timeout)
    except (ConnectionError, OSError):
        raw = b""
    finally:
        writer.close()
    return raw == b""


async def test_sockets_that_never_send_a_client_hello_are_counted_and_capped_per_ip(tls_files: TlsFiles) -> None:
    """Regression: the caps used to apply only after the TLS handshake, so one address could hold any number of
    sockets that never sent a ClientHello (each for the 10 s handshake timeout) while _open stayed 0."""
    crt, key, _, _ = tls_files
    server, api, port, service = await serve_tls_limited(
        crt, key, Limits(max_connections_per_ip=2, handshake_timeout_s=1.0))
    try:
        held = [await asyncio.open_connection("127.0.0.1", port) for _ in range(2)]   # TCP only, no ClientHello
        await asyncio.sleep(0.1)
        assert api._open == 2 and api._per_ip == {"127.0.0.1": 2}
        started = time.monotonic()
        assert await closed_without_answer(port)                  # the third is refused before any handshake
        assert time.monotonic() - started < 1.0
        assert api._open == 2
        await asyncio.sleep(1.5)                                  # the silent sockets time out in the handshake
        assert api._open == 0 and api._per_ip == {}
        for _, w in held:
            w.close()
        r = await exchange(port, request_bytes("GET", "/v1/health"), ssl_ctx=pinned_client_context())
        assert r.status == 200                                    # a real client gets a slot again
    finally:
        await stop(server, service)


async def test_total_cap_counts_connections_still_in_the_handshake(tls_files: TlsFiles) -> None:
    crt, key, _, _ = tls_files
    server, api, port, service = await serve_tls_limited(
        crt, key, Limits(max_connections=3, max_connections_per_ip=8, handshake_timeout_s=2.0))
    try:
        held = [await asyncio.open_connection("127.0.0.1", port) for _ in range(3)]
        await asyncio.sleep(0.1)
        assert api._open == 3
        assert await closed_without_answer(port)
        for _, w in held:
            w.close()
    finally:
        await stop(server, service)


async def test_new_connections_per_ip_are_rate_limited_before_tls(tls_files: TlsFiles) -> None:
    crt, key, _, _ = tls_files
    server, api, port, service = await serve_tls_limited(crt, key, Limits(max_new_connections_per_ip_per_min=2))
    try:
        for _ in range(2):
            r = await exchange(port, request_bytes("GET", "/v1/health"), ssl_ctx=pinned_client_context())
            assert r.status == 200
        assert await closed_without_answer(port)                  # third new connection in the same minute
    finally:
        await stop(server, service)


async def test_refusals_are_logged_at_a_limited_rate(tls_files: TlsFiles, caplog: pytest.LogCaptureFixture) -> None:
    crt, key, _, _ = tls_files
    server, api, port, service = await serve_tls_limited(
        crt, key, Limits(max_connections_per_ip=1, handshake_timeout_s=3.0))
    try:
        with caplog.at_level(logging.INFO, logger="xtnl.api"):
            _, holder = await asyncio.open_connection("127.0.0.1", port)
            await asyncio.sleep(0.05)
            for _ in range(20):
                assert await closed_without_answer(port)
            holder.close()
        refused = [r for r in caplog.records if getattr(r, "event", "") == "connection_refused"]
        assert len(refused) == 1 and vars(refused[0])["reason"] == "connection_cap_per_ip"
        assert api.refusals.suppressed == 19
        assert not [r for r in caplog.records if getattr(r, "event", "") == "request"]   # no per-socket lines
    finally:
        await stop(server, service)