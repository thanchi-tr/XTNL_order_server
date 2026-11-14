"""systemd notify protocol (operations.md §3: `WatchdogSec=3`, `WatchdogSignal=SIGKILL`).

`SdNotifier.notify("WATCHDOG=1")` sends one datagram to $NOTIFY_SOCKET. It is a no-op on Windows or
when the variable is unset (not running under systemd). The socket is non-blocking: a full socket
buffer drops the datagram instead of blocking the event loop.
"""
from __future__ import annotations

import logging
import os
import socket
import sys

log = logging.getLogger(__name__)


class SdNotifier:
    def __init__(self, notify_socket: str | None = None) -> None:
        path = notify_socket if notify_socket is not None else os.environ.get("NOTIFY_SOCKET")
        self._addr: str | None = None
        self._sock: socket.socket | None = None
        if not path or sys.platform == "win32" or not hasattr(socket, "AF_UNIX"):
            return
        self._addr = "\0" + path[1:] if path.startswith("@") else path
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sock.setblocking(False)
            self._sock = sock
        except OSError as exc:
            log.warning("sd_notify unavailable: %r", exc)
            self._addr = None

    @property
    def enabled(self) -> bool:
        return self._sock is not None

    def notify(self, state: str) -> bool:
        if self._sock is None or self._addr is None:
            return False
        try:
            self._sock.sendto(state.encode(), self._addr)
            return True
        except OSError as exc:
            log.warning("sd_notify %s failed: %r", state, exc)
            return False

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None
