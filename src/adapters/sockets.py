"""Reset-on-exit options for every broker socket (operations.md §3, adapter-isolation.md §5).

Broker sockets use ``SO_LINGER(on, 0)`` and ``TCP_USER_TIMEOUT <= 2 s``: when the fence hard-kills the
engine (or a socket is closed), the kernel resets the connection instead of flushing it, and unacked data
times out within 2 s. The takeover settle window (lease TTL + TCP_USER_TIMEOUT + broker ack) relies on it.

- StoneX REST: :func:`reset_http_client` (httpx ``socket_options`` on its connection pool).
- StoneX Lightstreamer: :func:`install_lightstreamer_socket_options` gives the library's process-wide aiohttp
  session a connector whose sockets come from :func:`reset_socket_factory`.
- cTrader (websockets): :func:`apply_reset_options` on the connected socket.

``TCP_USER_TIMEOUT`` exists on Linux (production); where the platform has no such option (Windows dev
boxes) only ``SO_LINGER`` is set.
"""
from __future__ import annotations

import logging
import socket
import struct
import sys
import threading
from typing import Any, Final

import httpx

log = logging.getLogger(__name__)

TCP_USER_TIMEOUT_MS: Final = 2000
LIGHTSTREAMER_INSTALL_TIMEOUT_S: Final = 5.0

SocketOption = tuple[int, int, int] | tuple[int, int, bytes]


def linger_reset() -> bytes:
    """struct linger {on=1, linger=0}: close() sends RST. Windows uses two u_shorts, POSIX two ints."""
    return struct.pack("HH" if sys.platform == "win32" else "ii", 1, 0)


def reset_socket_options() -> list[SocketOption]:
    """The (level, option, value) triples for a broker socket on this platform."""
    options: list[SocketOption] = [(socket.SOL_SOCKET, socket.SO_LINGER, linger_reset())]
    user_timeout = getattr(socket, "TCP_USER_TIMEOUT", None)
    if user_timeout is not None:
        options.append((socket.IPPROTO_TCP, int(user_timeout), TCP_USER_TIMEOUT_MS))
    return options


def apply_reset_options(sock: socket.socket) -> bool:
    """Set the options on ``sock``. False (with a warning) when the platform refused one of them."""
    try:
        for level, option, value in reset_socket_options():
            sock.setsockopt(level, option, value)
    except OSError as exc:
        log.warning("broker socket: could not set SO_LINGER / TCP_USER_TIMEOUT: %s", exc)
        return False
    return True


def reset_socket_factory(addr_info: tuple[Any, ...]) -> socket.socket:
    """aiohttp ``TCPConnector(socket_factory=...)``: a plain TCP socket with the reset options."""
    family, type_, proto = addr_info[0], addr_info[1], addr_info[2]
    sock = socket.socket(family=family, type=type_, proto=proto)
    apply_reset_options(sock)
    return sock


def reset_http_transport() -> httpx.AsyncHTTPTransport:
    return httpx.AsyncHTTPTransport(socket_options=reset_socket_options())


def reset_http_client(timeout_s: float) -> httpx.AsyncClient:
    """The StoneX REST client: every pooled connection gets the reset options."""
    return httpx.AsyncClient(timeout=timeout_s, transport=reset_http_transport())


# --- Lightstreamer --------------------------------------------------------------------------------------
_ls_lock = threading.Lock()
_ls_installed = False


def install_lightstreamer_socket_options() -> bool:
    """Make lightstreamer-client-lib open its sockets through :func:`reset_socket_factory`.

    The library (2.x) keeps one process-wide ``aiohttp.ClientSession`` on its own IO thread
    (``com_lightstreamer_net.SessionPy``) and exposes no socket hook, so its session is replaced, on that
    thread, by one whose ``TCPConnector`` uses the factory, and ``SessionPy.getInstance`` is wrapped so a
    re-created session keeps it. Idempotent and thread-safe; call it before the first connect. Returns
    False (and logs an error) when the library layout is not the expected one: streaming still works,
    without the reset options, and the error says so.
    """
    global _ls_installed
    with _ls_lock:
        if _ls_installed:
            return True
        try:
            import aiohttp
            from lightstreamer.client import com_lightstreamer_net as net

            session_cls: Any = net.SessionPy
            io_thread: Any = net.ls_io_thread
            jar = net.CookieHelper.getInstance().jar
        except Exception as exc:  # noqa: BLE001 - library layout changed or not installed
            log.error("stonex: Lightstreamer sockets keep the default linger (cannot install options: %s)", exc)
            return False

        def new_session() -> Any:
            return aiohttp.ClientSession(cookie_jar=jar,
                                         connector=aiohttp.TCPConnector(socket_factory=reset_socket_factory))

        async def swap() -> None:
            old = session_cls.session
            session_cls.session = new_session()
            if old is not None:
                await old.close()

        def get_instance() -> Any:
            if session_cls.session is None:
                session_cls.session = new_session()
            return session_cls.session

        try:
            io_thread.submit_coro(swap()).result(timeout=LIGHTSTREAMER_INSTALL_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001
            log.error("stonex: Lightstreamer sockets keep the default linger (session swap failed: %s)", exc)
            return False
        session_cls.getInstance = staticmethod(get_instance)
        _ls_installed = True
        return True
