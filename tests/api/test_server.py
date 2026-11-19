"""The HTTP layer against an in-process server on 127.0.0.1 (plain TCP; TLS is covered in test_tls.py): routes,
signature checks, size / header / rate limits, slow clients, JSON handling, cache outage and the request log."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from src.api.server import Limits

from tests.api.conftest import SECRET, SECRET_TEXT, FakeCompiler, HarnessFactory, exchange, request_bytes

JSON_HEADERS = {"content-type": "application/json", "cache-control": "no-store", "connection": "close"}


def assert_json_headers(headers: dict[str, str]) -> None:
    for k, v in JSON_HEADERS.items():
        assert headers.get(k) == v, k


async def test_health(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    r = await h.send("GET", "/v1/health")
    assert r.status == 200 and r.body == {"ok": True, "service": "xtnl-order-server", "api": 1}
    assert_json_headers(r.headers)
    assert r.headers["x-content-type-options"] == "nosniff"


@pytest.mark.parametrize("kw", [{"sign": False}, {"secret": b"77" * 32}])
async def test_unsigned_or_wrongly_signed_is_401(make_harness: HarnessFactory, kw: dict[str, Any]) -> None:
    h = await make_harness()
    r = await h.send("GET", "/v1/health", **kw)
    assert r.status == 401 and r.body == {"error": "unauthorized"}
    assert_json_headers(r.headers)


async def test_expired_is_401(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    r = await exchange(h.port, request_bytes("GET", "/v1/health", now_ms=h.clock.ms - 61_000))
    assert r.status == 401


async def test_replayed_request_is_401(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    data = request_bytes("GET", "/v1/health", now_ms=h.clock.ms, nonce="ab" * 16)
    assert (await exchange(h.port, data)).status == 200
    assert (await exchange(h.port, data)).status == 401


async def test_query_string_is_not_signed_and_is_ignored(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    assert (await h.send("GET", "/v1/health?x=1")).status == 200


@pytest.mark.parametrize("method,path", [("POST", "/v1/health"), ("DELETE", "/v1/pinescript/latest"),
                                         ("GET", "/v1/pinescript/compile"), ("GET", "/"), ("GET", "/v1/health/"),
                                         ("GET", "/V1/HEALTH"), ("GET", "/v1/../v1/health"), ("PUT", "/v2/x")])
async def test_unknown_path_or_method_is_404_with_an_empty_body(make_harness: HarnessFactory, method: str,
                                                                path: str) -> None:
    h = await make_harness()
    for signed in (True, False):
        r = await h.send(method, path, sign=signed)
        assert r.status == 404 and r.body == {}
        assert_json_headers(r.headers)


async def test_oversize_body_is_413_and_never_read(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    body = b'{"force": false, "pad": "' + b"x" * 5000 + b'"}'
    r = await h.send("POST", "/v1/pinescript/compile", body)
    assert r.status == 413 and r.body == {"error": "too_large"}
    assert h.compiler.calls == []


async def test_body_of_exactly_4_kib_is_accepted(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    prefix = b'{"force": false, "pad": "'
    body = prefix + b"x" * (4096 - len(prefix) - 2) + b'"}'
    assert len(body) == 4096
    assert (await h.send("POST", "/v1/pinescript/compile", body)).status == 202


async def test_oversize_head_is_431(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    r = await h.send("GET", "/v1/health", headers={"X-Pad": "y" * 9000})
    assert r.status == 431


async def test_too_many_headers_is_431(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    r = await h.send("GET", "/v1/health", headers={f"X-H{i}": "1" for i in range(40)})
    assert r.status == 431


@pytest.mark.parametrize("extra", [{"Transfer-Encoding": "chunked"}, {"Content-Length": "abc"}])
async def test_framing_tricks_are_400(make_harness: HarnessFactory, extra: dict[str, str]) -> None:
    h = await make_harness()
    assert (await h.send("GET", "/v1/health", headers=extra)).status == 400


async def test_duplicate_signature_header_is_400(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    data = request_bytes("GET", "/v1/health", now_ms=h.clock.ms)
    data = data.replace(b"\r\n\r\n", b"\r\nX-XTNL-Signature: " + b"0" * 64 + b"\r\n\r\n")
    assert (await exchange(h.port, data)).status == 400


async def test_garbage_request_line_is_400(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    r = await exchange(h.port, b"HELLO\r\n\r\n")
    assert r.status == 400


async def test_rate_limit_is_429_after_30_requests_a_minute(make_harness: HarnessFactory) -> None:
    h = await make_harness(rate_per_min=30)
    statuses = [(await h.send("GET", "/v1/health")).status for _ in range(30)]
    assert statuses == [200] * 30
    r = await h.send("GET", "/v1/health")
    assert r.status == 429 and r.body == {"error": "rate_limited"} and int(r.headers["retry-after"]) >= 1
    assert (await h.send("GET", "/v1/nope", sign=False)).status == 429     # unsigned traffic counts too
    h.clock.ms += 2_000                                                    # one token back
    assert (await h.send("GET", "/v1/health")).status == 200


async def test_slow_client_is_cut_off_by_the_head_deadline(make_harness: HarnessFactory) -> None:
    h = await make_harness(limits=Limits(header_timeout_s=0.3))
    reader, writer = await asyncio.open_connection("127.0.0.1", h.port)
    started = time.monotonic()
    try:
        for chunk in (b"GET /v1/health HTTP/1.1\r\n", b"X-A: 1\r\n", b"X-B: 2\r\n"):   # never finishes the head
            writer.write(chunk)
            await writer.drain()
            await asyncio.sleep(0.1)
        raw = await asyncio.wait_for(reader.read(), 3)
    finally:
        writer.close()
    assert time.monotonic() - started < 2
    assert raw.startswith(b"HTTP/1.1 408")


async def test_slow_body_is_cut_off(make_harness: HarnessFactory) -> None:
    h = await make_harness(limits=Limits(body_timeout_s=0.3))
    data = request_bytes("POST", "/v1/pinescript/compile", b'{"force": false}', now_ms=h.clock.ms)
    reader, writer = await asyncio.open_connection("127.0.0.1", h.port)
    try:
        writer.write(data[:-5])                                           # body short by 5 bytes
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), 3)
    finally:
        writer.close()
    assert raw.startswith(b"HTTP/1.1 408")
    assert h.compiler.calls == []


async def test_connection_cap_per_ip(make_harness: HarnessFactory) -> None:
    h = await make_harness(limits=Limits(max_connections_per_ip=2, header_timeout_s=2))
    held = [await asyncio.open_connection("127.0.0.1", h.port) for _ in range(2)]
    await asyncio.sleep(0.05)
    reader, writer = await asyncio.open_connection("127.0.0.1", h.port)
    raw = await asyncio.wait_for(reader.read(), 2)                         # closed without an answer
    assert raw == b""
    writer.close()
    for _, w in held:
        w.close()


async def test_compile_and_latest_flow(make_harness: HarnessFactory) -> None:
    gate = asyncio.Event()
    h = await make_harness(compiler=FakeCompiler(gate=gate))
    r = await h.send("POST", "/v1/pinescript/compile", b'{"force": false}')
    assert r.status == 202 and r.body == {"week": "2026-W46", "status": "compiling"}
    assert_json_headers(r.headers)
    latest = await h.send("GET", "/v1/pinescript/latest")
    assert latest.status == 200 and latest.body["status"] == "compiling"
    gate.set()
    await asyncio.sleep(0.05)
    latest = await h.send("GET", "/v1/pinescript/latest")
    assert latest.body["status"] == "ready" and [s["symbol"] for s in latest.body["scripts"]] == ["EURUSD", "AUDUSD"]
    again = await h.send("POST", "/v1/pinescript/compile", b'{"force": false}')
    assert again.body["status"] == "ready" and len(h.compiler.calls) == 1
    forced = await h.send("POST", "/v1/pinescript/compile", b'{"force": true}')
    assert forced.body["status"] == "compiling"
    await asyncio.sleep(0.05)
    assert len(h.compiler.calls) == 2


async def test_compile_without_body_means_force_false(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    assert (await h.send("POST", "/v1/pinescript/compile")).status == 202


@pytest.mark.parametrize("body,ctype,status", [(b"{not json", "application/json", 400),
                                               (b'{"force": "yes"}', "application/json", 400),
                                               (b"[true]", "application/json", 400),
                                               (b'{"force": true}', "text/plain", 415),
                                               (b'{"force": true}', None, 415)])
async def test_bad_compile_bodies(make_harness: HarnessFactory, body: bytes, ctype: str | None, status: int) -> None:
    h = await make_harness()
    r = await h.send("POST", "/v1/pinescript/compile", body, content_type=ctype)
    assert r.status == status and set(r.body) == {"error"}
    assert h.compiler.calls == []


class DownRedis:
    async def get(self, name: str) -> Any:
        raise RedisConnectionError("Error 10061 connecting to 127.0.0.1:56380. secret-ish detail")

    async def set(self, name: str, value: str, ex: int | None = None) -> Any:
        raise RedisConnectionError("down")


async def test_cache_outage_is_503_without_detail(make_harness: HarnessFactory) -> None:
    h = await make_harness(redis=DownRedis())
    for method, path in (("GET", "/v1/pinescript/latest"), ("POST", "/v1/pinescript/compile")):
        r = await h.send(method, path)
        assert r.status == 503 and r.body == {"error": "cache_unavailable"}
        assert b"10061" not in r.raw and b"56380" not in r.raw
    assert (await h.send("GET", "/v1/health")).status == 200               # health needs no cache


class BrokenService:
    async def latest(self) -> dict[str, Any]:
        raise ZeroDivisionError("internal detail /c/Users/secret")


async def test_internal_error_is_500_without_detail(make_harness: HarnessFactory) -> None:
    h = await make_harness()
    h.api.service = BrokenService()   # type: ignore[assignment]
    h.api._routes[("GET", "/v1/pinescript/latest")] = lambda req: BrokenService().latest()   # type: ignore[assignment,return-value]
    r = await h.send("GET", "/v1/pinescript/latest")
    assert r.status == 500 and r.body == {"error": "internal"}
    assert b"ZeroDivision" not in r.raw and b"secret" not in r.raw and b"Traceback" not in r.raw


async def test_request_log_carries_no_secret_signature_or_body(make_harness: HarnessFactory,
                                                               caplog: pytest.LogCaptureFixture) -> None:
    h = await make_harness()
    body = b'{"force": true, "pad": "bodymarker"}'
    data = request_bytes("POST", "/v1/pinescript/compile", body, now_ms=h.clock.ms, nonce="cd" * 16)
    signature = data.split(b"X-XTNL-Signature: ")[1].split(b"\r\n")[0].decode()
    with caplog.at_level(logging.INFO, logger="xtnl.api"):
        assert (await exchange(h.port, data)).status == 202
        assert (await h.send("GET", "/v1/health", sign=False)).status == 401
        await asyncio.sleep(0.05)
    requests = [r for r in caplog.records if getattr(r, "event", "") == "request"]
    assert len(requests) == 2
    first = vars(requests[0])
    assert (first["method"], first["path"], first["status"], first["client"]) == (
        "POST", "/v1/pinescript/compile", 202, "127.0.0.1")
    assert isinstance(first["latency_ms"], int)
    assert vars(requests[1])["reason"] == "missing_headers"
    everything = json.dumps([{k: str(v) for k, v in vars(r).items()} for r in caplog.records])
    for needle in (SECRET_TEXT, SECRET.decode(), signature, "cd" * 16, "bodymarker"):
        assert needle not in everything, needle
