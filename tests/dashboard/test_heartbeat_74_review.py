"""IMPLEMENTATION_PLAN 7.4 review: the dashboard heartbeat listener bounds each request as a whole and answers (and
closes) a connection whose read fails with any OSError (ssl.SSLError included). Loopback sockets only."""
from __future__ import annotations

import asyncio
import ssl

import pytest

from src.dashboard import heartbeat as H
from tests.dashboard.test_heartbeat_74 import TOKEN, payload


async def test_a_client_trickling_header_lines_is_cut_off_by_the_request_deadline(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(H, "REQUEST_TIMEOUT_S", 0.3)
    board = H.HeartbeatBoard()
    listener = H.HeartbeatListener(H.HeartbeatListenSettings("127.0.0.1", 0, TOKEN), board)
    server = await listener.start()
    port = server.sockets[0].getsockname()[1]
    try:
        body = payload().to_json().encode()
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write(b"POST /xtnl/heartbeat HTTP/1.1\r\n")
            for i in range(8):                 # each line well inside HEADER_TIMEOUT_S, the whole far beyond 0.3 s
                writer.write(f"X-Pad-{i}: x\r\n".encode())
                await writer.drain()
                await asyncio.sleep(0.1)
            writer.write(f"Authorization: Bearer {TOKEN}\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body)
            await writer.drain()
        except OSError:                        # the server already gave up on us (the point of the test)
            pass
        try:
            out: bytes | None = await asyncio.wait_for(reader.read(), 5.0)
        except ConnectionError:                # the server closed with our late bytes unread (an RST on Windows)
            out = None
        writer.close()
        assert out is None or out.startswith(b"HTTP/1.1 400")
        assert board.latest() is None and board.accepted == 0
    finally:
        server.close()
        await server.wait_closed()


class _FailingReader:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def readline(self) -> bytes:
        raise self._exc


class _Writer:
    def __init__(self) -> None:
        self.data = b""
        self.closed = False

    def write(self, b: bytes) -> None:
        self.data += b

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


@pytest.mark.parametrize("exc", [ssl.SSLError("bad record mac"), OSError("reset"), ConnectionResetError()])
async def test_a_read_failing_with_any_os_error_is_answered_and_closed(exc: BaseException) -> None:
    listener = H.HeartbeatListener(H.HeartbeatListenSettings("127.0.0.1", 0, TOKEN), H.HeartbeatBoard())
    w = _Writer()
    await listener.handle(_FailingReader(exc), w)       # type: ignore[arg-type]
    assert w.closed and w.data.startswith(b"HTTP/1.1 400")
