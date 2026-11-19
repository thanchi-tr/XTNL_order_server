"""Shared fixtures of the order-server API tests: a secret, signed raw HTTP requests, an in-process server on
127.0.0.1 (plain TCP for the protocol tests, TLS with a throwaway certificate for the TLS tests) and fakeredis."""
from __future__ import annotations

import asyncio
import json
import secrets
import shutil
import ssl
import subprocess
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.api.auth import Authenticator, signed_headers
from src.api.pinescript import PinescriptService
from src.api.server import ApiServer, Limits
from src.reports.lifecycle import Window

SECRET_TEXT = "5f" * 32           # 32 bytes as hex: the shape new-api-credentials.ps1 creates
SECRET = SECRET_TEXT.encode()
NOW = 1_794_657_600_000           # 2026-11-14T12:00:00Z (Saturday of ISO week 2026-W46)


@dataclass
class Clock:
    ms: int = NOW

    def __call__(self) -> int:
        return self.ms


@dataclass
class FakeCompiler:
    """Counts calls; blocks on `gate` when set; returns `scripts` or raises `error`."""

    scripts: Sequence[tuple[str, str]] = (("EURUSD", "//@version=6\nindicator(\"E\")"), ("AUDUSD", "//@version=6"))
    error: BaseException | None = None
    gate: asyncio.Event | None = None
    calls: list[Window] = field(default_factory=list)

    async def __call__(self, window: Window, at_ms: int) -> Sequence[tuple[str, str]]:
        self.calls.append(window)
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        return self.scripts


def request_bytes(method: str, path: str, body: bytes = b"", *, secret: bytes = SECRET, now_ms: int = NOW,
                  nonce: str | None = None, headers: dict[str, str] | None = None, sign: bool = True,
                  content_type: str | None = "application/json") -> bytes:
    h: dict[str, str] = {"Host": "127.0.0.1"}
    if sign:
        h.update(signed_headers(secret, method, path.split("?", 1)[0], body, now_ms=now_ms,
                                nonce=nonce or secrets.token_hex(16)))
    if body:
        h["Content-Length"] = str(len(body))
        if content_type:
            h["Content-Type"] = content_type
    h.update(headers or {})
    head = f"{method} {path} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in h.items()) + "\r\n"
    return head.encode("latin-1") + body


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: Any
    raw: bytes


def parse_response(raw: bytes) -> Response:
    head, _, payload = raw.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {k.strip().lower(): v.strip() for k, _, v in (ln.partition(":") for ln in lines[1:])}
    return Response(status, headers, json.loads(payload) if payload else None, raw)


async def exchange(port: int, data: bytes, *, ssl_ctx: ssl.SSLContext | None = None, timeout: float = 5.0) -> Response:
    reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=ssl_ctx)
    try:
        writer.write(data)
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    return parse_response(raw)


@dataclass
class Harness:
    api: ApiServer
    port: int
    redis: Any
    compiler: FakeCompiler
    clock: Clock
    service: PinescriptService

    async def send(self, method: str, path: str, body: bytes = b"", **kw: Any) -> Response:
        return await exchange(self.port, request_bytes(method, path, body, now_ms=self.clock.ms, **kw))


HarnessFactory = Callable[..., Awaitable[Harness]]


@pytest.fixture
async def make_harness() -> AsyncIterator[HarnessFactory]:
    made: list[tuple[asyncio.Server, Harness]] = []

    async def factory(*, rate_per_min: int = 600, limits: Limits = Limits(), compiler: FakeCompiler | None = None,
                      redis: Any = None) -> Harness:
        clock = Clock()
        comp = compiler or FakeCompiler()
        cache = redis if redis is not None else fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer(), db=2)
        service = PinescriptService(cache, comp, clock=clock)
        api = ApiServer(Authenticator(SECRET), service, rate_per_min=rate_per_min, limits=limits, clock=clock)
        server = await api.serve("127.0.0.1", 0, None)
        port = server.sockets[0].getsockname()[1]
        h = Harness(api, port, cache, comp, clock, service)
        made.append((server, h))
        return h

    yield factory
    for server, h in made:
        server.close()
        await h.service.close()
        try:
            await asyncio.wait_for(server.wait_closed(), 2)
        except TimeoutError:
            pass


# ------------------------------------------------------------------------------------------------ TLS
def find_openssl() -> str | None:
    for cand in (shutil.which("openssl"), r"C:\Program Files\Git\usr\bin\openssl.exe",
                 r"C:\Program Files\Git\mingw64\bin\openssl.exe"):
        if cand and Path(cand).exists():
            return cand
    return None


def make_cert(openssl: str, out: Path, name: str = "server") -> tuple[Path, Path]:
    crt, key = out / f"{name}.crt", out / f"{name}.key"
    subprocess.run([openssl, "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                    "-keyout", str(key), "-out", str(crt), "-days", "2", "-subj", "/CN=xtnl-order-server",
                    "-addext", "subjectAltName=IP:127.0.0.1"], check=True, capture_output=True, timeout=60)
    return crt, key


@pytest.fixture(scope="session")
def tls_files(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path, Path, Path]:
    openssl = find_openssl()
    if openssl is None:
        pytest.skip("openssl not found (Git for Windows ships one)")
    out = tmp_path_factory.mktemp("api-tls")
    crt, key = make_cert(openssl, out, "server")
    other_crt, other_key = make_cert(openssl, out, "other")
    return crt, key, other_crt, other_key
