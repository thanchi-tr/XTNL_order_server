"""The order-server API's HTTPS server: one request per connection, strict limits, three routes.

Per connection, in order (each failure answers and closes; nothing internal is ever in a response):
  0. the socket is accepted as plain TCP with reading paused, so nothing is read or buffered before step 1;
  1. connection caps, BEFORE the TLS handshake: at most max_connections open, max_connections_per_ip per client
     address and max_new_connections_per_ip_per_min new connections per address (refused with an abort, no TLS);
     the TLS handshake then runs inside the counted slot (start_tls, handshake_timeout_s), so a client that
     connects and never sends a ClientHello holds one of its own few slots for at most that long. Refusals are
     logged at most once per REFUSAL_LOG_EVERY_S (with the number suppressed), so a flood cannot flood the log;
  2. request head (request line + headers) within HEADER_TIMEOUT_S in total and MAX_HEAD_BYTES (408 / 431),
     at most MAX_HEADERS headers, no Transfer-Encoding, no duplicate Content-Length or X-XTNL-* header (400);
  3. rate limit per client IP (429 with Retry-After);
  4. Content-Length above MAX_BODY_BYTES (413, the body is never read);
  5. path / method allow-list: anything but the three routes is 404 with the body {};
  6. body within BODY_TIMEOUT_S (408);
  7. HMAC signature, clock window, replay cache (401 {"error": "unauthorized"});
  8. JSON body (400 / 415), then the route (503 when the Redis cache is unreachable).
A slow client is cut off by the total deadlines (slowloris), never by a per-read idle timer it could reset.
Every response: application/json, Cache-Control no-store, Connection close. One log line per request: method,
path, status, client IP, latency (and the refusal reason): never a header, body, signature or the secret.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import re
import ssl
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final

from src.api.auth import NONCE_HEADER, SIG_HEADER, TS_HEADER, Authenticator, AuthResult, RateLimiter
from src.api.pinescript import PinescriptService, now_ms

log = logging.getLogger("xtnl.api")

SERVICE: Final = "xtnl-order-server"
API_VERSION: Final = 1
HEALTH: Final = ("GET", "/v1/health")
COMPILE: Final = ("POST", "/v1/pinescript/compile")
LATEST: Final = ("GET", "/v1/pinescript/latest")
ROUTES: Final = frozenset({HEALTH, COMPILE, LATEST})

REFUSAL_LOG_EVERY_S: Final = 10.0
REASONS: Final = {200: "OK", 202: "Accepted", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
                  408: "Request Timeout", 413: "Content Too Large", 415: "Unsupported Media Type",
                  429: "Too Many Requests", 431: "Request Header Fields Too Large", 500: "Internal Server Error",
                  503: "Service Unavailable"}
_TOKEN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
_REQUEST_LINE = re.compile(r"([A-Z]{1,10}) (/[^\s]*) HTTP/1\.[01]")
_SINGLE = frozenset({"content-length", "content-type", TS_HEADER, NONCE_HEADER, SIG_HEADER})


@dataclass(frozen=True)
class Limits:
    max_head_bytes: int = 8192
    max_headers: int = 32
    max_body_bytes: int = 4096
    handshake_timeout_s: float = 5.0
    header_timeout_s: float = 5.0
    body_timeout_s: float = 10.0
    write_timeout_s: float = 15.0
    max_connections: int = 128
    max_connections_per_ip: int = 8
    max_new_connections_per_ip_per_min: int = 120


class _Reject(Exception):
    def __init__(self, status: int, body: dict[str, Any] | None = None, reason: str = "",
                 headers: dict[str, str] | None = None) -> None:
        super().__init__(status)
        self.status = status
        self.body = body if body is not None else {}
        self.reason = reason
        self.headers = headers or {}


class _Silent(Exception):
    """Close without answering (the client went away or broke the connection)."""


@dataclass
class Request:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes = b""


def client_ip(writer: asyncio.StreamWriter) -> str:
    peer = writer.get_extra_info("peername")
    host = peer[0] if isinstance(peer, tuple) and peer else "unknown"
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return str(host)
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        return str(addr.ipv4_mapped)
    return str(addr)


def render(status: int, body: dict[str, Any], extra: dict[str, str] | None = None) -> bytes:
    payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
    head = [f"HTTP/1.1 {status} {REASONS.get(status, 'Error')}", "Content-Type: application/json",
            "Cache-Control: no-store", f"Content-Length: {len(payload)}", "Connection: close",
            "X-Content-Type-Options: nosniff"]
    head += [f"{k}: {v}" for k, v in (extra or {}).items()]
    return ("\r\n".join(head) + "\r\n\r\n").encode("ascii") + payload


def parse_head(raw: bytes, limits: Limits) -> Request:
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        raise _Reject(400, {"error": "bad_request"}, "non_ascii_head") from None
    lines = text[:-4].split("\r\n") if text.endswith("\r\n\r\n") else text.split("\r\n")
    m = _REQUEST_LINE.fullmatch(lines[0])
    if m is None:
        raise _Reject(400, {"error": "bad_request"}, "request_line")
    method, target = m.group(1), m.group(2)
    path = target.split("?", 1)[0]
    if len(lines) - 1 > limits.max_headers:
        raise _Reject(431, {"error": "headers_too_large"}, "too_many_headers")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, sep, value = line.partition(":")
        if not sep or not _TOKEN.fullmatch(name):
            raise _Reject(400, {"error": "bad_request"}, "header_syntax")
        key = name.lower()
        if key in headers and key in _SINGLE:
            raise _Reject(400, {"error": "bad_request"}, "duplicate_header")
        headers[key] = value.strip(" \t") if key not in headers else headers[key] + ", " + value.strip(" \t")
    if "transfer-encoding" in headers:
        raise _Reject(400, {"error": "bad_request"}, "transfer_encoding")
    return Request(method, path, headers)


def content_length(req: Request) -> int:
    raw = req.headers.get("content-length")
    if raw is None:
        return 0
    if not re.fullmatch(r"[0-9]{1,9}", raw):
        raise _Reject(400, {"error": "bad_request"}, "content_length")
    return int(raw)


Handler = Callable[[Request], Awaitable[tuple[int, dict[str, Any]]]]


class _PausedProtocol(asyncio.StreamReaderProtocol):
    """Pauses reading as soon as the socket is accepted, before the handler runs: no byte (a ClientHello, a
    request) is read or buffered until the handler has counted the connection and decided to serve it."""

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        if isinstance(transport, asyncio.ReadTransport):
            transport.pause_reading()
        super().connection_made(transport)


class _RefusalLog:
    """At most one warning per every_s, carrying how many refusals were not logged in between."""

    def __init__(self, every_s: float = REFUSAL_LOG_EVERY_S, clock: Callable[[], float] = time.monotonic) -> None:
        self.every_s = every_s
        self._clock = clock
        self._next = 0.0
        self.suppressed = 0

    def refused(self, reason: str, ip: str) -> None:
        now = self._clock()
        if now < self._next:
            self.suppressed += 1
            return
        self._next = now + self.every_s
        log.warning("connection refused", extra={"event": "connection_refused", "reason": reason, "client": ip,
                                                 "suppressed": self.suppressed})
        self.suppressed = 0


class ApiServer:
    def __init__(self, auth: Authenticator, service: PinescriptService, *, rate_per_min: int,
                 limits: Limits = Limits(), clock: Callable[[], int] = now_ms) -> None:
        self.auth = auth
        self.service = service
        self.limits = limits
        self.rate = RateLimiter(rate_per_min)
        self.connect_rate = RateLimiter(limits.max_new_connections_per_ip_per_min)
        self.refusals = _RefusalLog()
        self._clock = clock
        self._ssl: ssl.SSLContext | None = None
        self._open = 0
        self._per_ip: dict[str, int] = {}
        self._routes: dict[tuple[str, str], Handler] = {HEALTH: self._health, COMPILE: self._compile,
                                                         LATEST: self._latest}

    # ------------------------------------------------------------------------------------------ routes
    async def _health(self, req: Request) -> tuple[int, dict[str, Any]]:
        return 200, {"ok": True, "service": SERVICE, "api": API_VERSION}

    async def _compile(self, req: Request) -> tuple[int, dict[str, Any]]:
        force = False
        if req.body.strip():
            ctype = req.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if ctype != "application/json":
                raise _Reject(415, {"error": "json_only"}, "content_type")
            try:
                doc = json.loads(req.body)
            except (UnicodeDecodeError, ValueError):
                raise _Reject(400, {"error": "bad_json"}, "bad_json") from None
            if not isinstance(doc, dict) or not isinstance(doc.get("force", False), bool):
                raise _Reject(400, {"error": "bad_json"}, "bad_force")
            force = bool(doc.get("force", False))
        return 202, await self.service.start(force=force)

    async def _latest(self, req: Request) -> tuple[int, dict[str, Any]]:
        return 200, await self.service.latest()

    # ------------------------------------------------------------------------------------------ connection
    def _refusal(self, ip: str) -> str:
        """Why a new connection from ip is refused (empty = accepted). Checked before any byte is read."""
        if self._open >= self.limits.max_connections:
            return "connection_cap"
        if self._per_ip.get(ip, 0) >= self.limits.max_connections_per_ip:
            return "connection_cap_per_ip"
        if not self.connect_rate.allow(ip, self._clock()):
            return "connection_rate"
        return ""

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        ip = client_ip(writer)
        refusal = self._refusal(ip)
        if refusal:
            self.refusals.refused(refusal, ip)
            writer.transport.abort()          # reading never started: nothing read, no TLS, no answer
            return
        self._open += 1
        self._per_ip[ip] = self._per_ip.get(ip, 0) + 1
        try:
            if not await self._start(writer):
                writer.transport.abort()      # handshake failed or timed out: no log line per attempt
                return
            await self._serve(reader, writer, ip)
        finally:
            self._open -= 1
            left = self._per_ip.get(ip, 1) - 1
            if left <= 0:
                self._per_ip.pop(ip, None)
            else:
                self._per_ip[ip] = left

    async def _start(self, writer: asyncio.StreamWriter) -> bool:
        """Plain TCP: resume reading. TLS: the handshake, inside the counted slot, within handshake_timeout_s."""
        if self._ssl is None:
            transport = writer.transport
            if isinstance(transport, asyncio.ReadTransport):
                transport.resume_reading()
            return True
        timeout = self.limits.handshake_timeout_s
        try:
            await asyncio.wait_for(writer.start_tls(self._ssl, ssl_handshake_timeout=timeout,
                                                    ssl_shutdown_timeout=5.0), timeout + 1.0)
        except (OSError, TimeoutError, ssl.SSLError, ConnectionError, RuntimeError):
            return False
        return True

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, ip: str) -> None:
        started = time.monotonic()
        method = path = "-"
        status = 0
        reason = ""
        try:
            try:
                req = await self._read(reader, ip)
                method, path = req.method, req.path
                status, body = await self._dispatch(req)
                extra: dict[str, str] = {}
            except _Reject as rej:
                status, body, reason, extra = rej.status, rej.body, rej.reason, rej.headers
            except _Silent as s:
                reason = str(s) or "client_gone"
                return
            except Exception as exc:   # never a stack trace or exception text in a response
                status, body, reason, extra = 500, {"error": "internal"}, type(exc).__name__, {}
                log.error("request failed", extra={"event": "internal_error", "exc_type": type(exc).__name__})
            try:
                writer.write(render(status, body, extra))
                await asyncio.wait_for(writer.drain(), self.limits.write_timeout_s)
            except (OSError, TimeoutError, ssl.SSLError):
                reason = reason or "write_failed"
        finally:
            await self._close(writer)
            fields: dict[str, Any] = {"event": "request", "method": method, "path": path[:120], "status": status,
                                      "client": ip, "latency_ms": int((time.monotonic() - started) * 1000)}
            if reason:
                fields["reason"] = reason
            log.info("request", extra=fields)

    async def _read(self, reader: asyncio.StreamReader, ip: str) -> Request:
        lim = self.limits
        try:
            raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), lim.header_timeout_s)
        except TimeoutError:
            raise _Reject(408, {"error": "timeout"}, "head_timeout") from None
        except asyncio.LimitOverrunError:
            raise _Reject(431, {"error": "headers_too_large"}, "head_too_large") from None
        except asyncio.IncompleteReadError as exc:
            if len(exc.partial) > lim.max_head_bytes:
                raise _Reject(431, {"error": "headers_too_large"}, "head_too_large") from None
            raise _Silent("client_gone") from None
        except (OSError, ssl.SSLError):
            raise _Silent("connection_error") from None
        if len(raw) > lim.max_head_bytes:
            raise _Reject(431, {"error": "headers_too_large"}, "head_too_large")
        req = parse_head(raw, lim)
        if not self.rate.allow(ip, self._clock()):
            raise _Reject(429, {"error": "rate_limited"}, "rate_limited",
                          {"Retry-After": str(self.rate.retry_after_s(ip))})
        length = content_length(req)
        if length > lim.max_body_bytes:
            raise _Reject(413, {"error": "too_large"}, "body_too_large")
        if (req.method, req.path) not in ROUTES:
            raise _Reject(404, {}, "no_route")
        if length:
            try:
                req.body = await asyncio.wait_for(reader.readexactly(length), lim.body_timeout_s)
            except TimeoutError:
                raise _Reject(408, {"error": "timeout"}, "body_timeout") from None
            except asyncio.IncompleteReadError:
                raise _Silent("client_gone") from None
            except (OSError, ssl.SSLError):
                raise _Silent("connection_error") from None
        return req

    async def _dispatch(self, req: Request) -> tuple[int, dict[str, Any]]:
        result = self.auth.check(req.method, req.path, req.headers, req.body, self._clock())
        if result is not AuthResult.OK:
            raise _Reject(401, {"error": "unauthorized"}, result.value)
        try:
            return await self._routes[(req.method, req.path)](req)
        except _Reject:
            raise
        except Exception as exc:
            if _is_cache_error(exc):
                log.warning("cache unavailable", extra={"event": "cache_unavailable",
                                                        "exc_type": type(exc).__name__})
                raise _Reject(503, {"error": "cache_unavailable"}, "cache_unavailable") from None
            raise

    @staticmethod
    async def _close(writer: asyncio.StreamWriter) -> None:
        try:
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), 5)
        except Exception:
            pass

    async def serve(self, host: str, port: int, ssl_ctx: ssl.SSLContext | None) -> asyncio.Server:
        """Listen as plain TCP; with ssl_ctx every connection is upgraded by handle() AFTER the connection caps
        (never start_server(ssl=...), whose handshake runs before any cap could count the socket)."""
        self._ssl = ssl_ctx
        loop = asyncio.get_running_loop()
        limit = self.limits.max_head_bytes

        def factory() -> _PausedProtocol:
            return _PausedProtocol(asyncio.StreamReader(limit=limit, loop=loop), self.handle, loop=loop)

        return await loop.create_server(factory, host, port, backlog=64)


def _is_cache_error(exc: BaseException) -> bool:
    try:
        from redis.exceptions import RedisError
    except ImportError:   # pragma: no cover - redis is a project dependency
        return isinstance(exc, (ConnectionError, TimeoutError))
    return isinstance(exc, (RedisError, ConnectionError, TimeoutError, OSError))
