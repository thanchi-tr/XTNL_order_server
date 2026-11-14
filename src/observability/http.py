"""A minimal read-only HTTP/1.1 server on asyncio streams (standard library only; IMPLEMENTATION_PLAN 5.6).

Used by the metrics exporter and the Melbourne dashboard. GET and HEAD only (anything else is 405): neither
has a write path. One request per connection (`Connection: close`), bounded header size and read timeout,
so a slow or hostile client can never hold the event loop or grow memory.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Final
from urllib.parse import parse_qs, urlsplit

log = logging.getLogger(__name__)

MAX_HEADER_BYTES: Final = 8192
READ_TIMEOUT_S: Final = 5.0
_REASONS: Final = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed",
                   408: "Request Timeout", 500: "Internal Server Error", 503: "Service Unavailable"}


@dataclass(frozen=True)
class Request:
    method: str
    path: str
    query: Mapping[str, list[str]] = field(default_factory=dict)

    def arg(self, name: str, default: str | None = None) -> str | None:
        v = self.query.get(name)
        return v[0] if v else default


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes
    content_type: str = "text/plain; charset=utf-8"

    @staticmethod
    def text(status: int, text: str, content_type: str = "text/plain; charset=utf-8") -> Response:
        return Response(status, text.encode("utf-8"), content_type)


Handler = Callable[[Request], Awaitable[Response]]


def parse_addr(text: str, default_port: int) -> tuple[str, int]:
    """'host:port', ':port', 'host' or '[v6]:port' -> (host, port). ValueError on a bad port."""
    text = text.strip()
    if text.startswith("["):
        host, _, rest = text[1:].partition("]")
        port = rest.lstrip(":")
        return host, int(port) if port else default_port
    if text.count(":") == 1:
        host, _, port = text.partition(":")
        return host or "127.0.0.1", int(port)
    return text or "127.0.0.1", default_port


async def _read_head(reader: asyncio.StreamReader) -> bytes:
    data = await reader.readuntil(b"\r\n\r\n")
    if len(data) > MAX_HEADER_BYTES:
        raise ValueError("header too large")
    return data


def _response_bytes(resp: Response, head_only: bool) -> bytes:
    reason = _REASONS.get(resp.status, "OK")
    head = (f"HTTP/1.1 {resp.status} {reason}\r\nContent-Type: {resp.content_type}\r\n"
            f"Content-Length: {len(resp.body)}\r\nCache-Control: no-store\r\n"
            "X-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n").encode("ascii")
    return head if head_only else head + resp.body


async def handle_connection(handler: Handler, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    resp: Response
    head_only = False
    try:
        try:
            raw = await asyncio.wait_for(_read_head(reader), READ_TIMEOUT_S)
        except (asyncio.LimitOverrunError, asyncio.IncompleteReadError, ValueError):
            resp = Response.text(400, "bad request\n")
        except TimeoutError:
            resp = Response.text(408, "timeout\n")
        else:
            line = raw.split(b"\r\n", 1)[0].decode("latin-1")
            parts = line.split(" ")
            if len(parts) != 3 or not parts[2].startswith("HTTP/"):
                resp = Response.text(400, "bad request\n")
            elif parts[0] not in ("GET", "HEAD"):
                resp = Response.text(405, "read-only: GET and HEAD only\n")
            else:
                head_only = parts[0] == "HEAD"
                url = urlsplit(parts[1])
                try:
                    resp = await handler(Request(parts[0], url.path or "/", parse_qs(url.query)))
                except Exception as exc:
                    log.warning("http handler failed for %s: %s", url.path, type(exc).__name__)
                    resp = Response.text(500, "internal error\n")
        writer.write(_response_bytes(resp, head_only))
        await asyncio.wait_for(writer.drain(), READ_TIMEOUT_S)
    except (ConnectionError, TimeoutError):
        pass
    finally:
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), 1.0)
        except (ConnectionError, TimeoutError):
            pass


async def start_server(handler: Handler, host: str, port: int) -> asyncio.Server:
    async def on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_connection(handler, reader, writer)
    return await asyncio.start_server(on_client, host, port, limit=MAX_HEADER_BYTES)


def bound_port(server: asyncio.Server) -> int:
    for sock in server.sockets:
        return int(sock.getsockname()[1])
    raise RuntimeError("server has no socket")


__all__ = ["Handler", "Request", "Response", "bound_port", "handle_connection", "parse_addr", "start_server"]
