"""Engine PostgreSQL client TCP keepalives ~5 s (operations.md §4; IMPLEMENTATION_PLAN 6.4). No server needed."""
from __future__ import annotations

import socket
from typing import Any

import pytest

from src.db import pg


class FakeSock:
    def __init__(self, family: int = socket.AF_INET) -> None:
        self.family = family
        self.opts: dict[tuple[int, int], int] = {}
        self.ioctls: list[Any] = []

    def setsockopt(self, level: int, opt: int, value: int) -> None:
        self.opts[(level, opt)] = value

    def ioctl(self, code: int, value: Any) -> None:
        self.ioctls.append((code, value))


class FakeTransport:
    def __init__(self, sock: Any) -> None:
        self.sock = sock

    def get_extra_info(self, name: str) -> Any:
        return self.sock if name == "socket" else None


class FakeRaw:
    def __init__(self, sock: Any) -> None:
        self._transport = FakeTransport(sock)
        self.codecs: list[str] = []

    async def set_type_codec(self, typename: str, **_: Any) -> None:
        self.codecs.append(typename)


def test_keepalive_values_detect_a_dead_peer_in_seconds() -> None:
    assert pg.KEEPALIVE_IDLE_S == 5
    detect_s = pg.KEEPALIVE_IDLE_S + pg.KEEPALIVE_COUNT * pg.KEEPALIVE_INTERVAL_S
    assert detect_s <= 10 and pg.TCP_USER_TIMEOUT_MS == detect_s * 1000


def test_configure_sets_keepalive_and_every_supported_tcp_option() -> None:
    sock = FakeSock()
    done = pg.configure_tcp_keepalive(sock)
    assert sock.opts[(socket.SOL_SOCKET, socket.SO_KEEPALIVE)] == 1
    for name, value in (("TCP_KEEPIDLE", 5), ("TCP_KEEPINTVL", 1), ("TCP_KEEPCNT", 3),
                        ("TCP_USER_TIMEOUT", pg.TCP_USER_TIMEOUT_MS)):
        opt = getattr(socket, name, None)
        if opt is not None:
            assert sock.opts[(socket.IPPROTO_TCP, opt)] == value, name
            assert name in done


def test_unix_socket_is_left_alone() -> None:
    family = getattr(socket, "AF_UNIX", 1)
    sock = FakeSock(family=family)
    assert pg.configure_tcp_keepalive(sock) == []
    assert sock.opts == {}


def test_real_tcp_socket_gets_so_keepalive() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:   # never connected: no network
        pg.configure_tcp_keepalive(s)
        assert s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
        idle = getattr(socket, "TCP_KEEPIDLE", None)
        if idle is not None:
            assert s.getsockopt(socket.IPPROTO_TCP, idle) == 5


async def test_pool_init_hook_sets_keepalives_before_codecs() -> None:
    sock = FakeSock()
    raw = FakeRaw(sock)
    await pg.init_connection(raw)
    assert sock.opts[(socket.SOL_SOCKET, socket.SO_KEEPALIVE)] == 1
    assert raw.codecs == ["jsonb", "json"]


async def test_pool_init_without_a_socket_still_installs_codecs(caplog: pytest.LogCaptureFixture) -> None:
    raw = FakeRaw(None)
    await pg.init_connection(raw)
    assert raw.codecs == ["jsonb", "json"]
    assert "keepalives not set" in caplog.text


def test_asyncpg_connection_still_has_the_transport_slot() -> None:
    """The hook reads asyncpg's private `_transport`; fail here if an upgrade renames it."""
    asyncpg_connection = pytest.importorskip("asyncpg.connection")
    assert "_transport" in asyncpg_connection.Connection.__slots__
