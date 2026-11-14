"""Hard exits (operations.md §3, §4).

The fence and the LEADER_INIT / DRAINING deadlines hard-exit the engine with `os._exit`: no
`finally` blocks, no atexit handlers, no graceful socket close. The kernel resets the sockets: every
broker socket (StoneX REST and Lightstreamer, cTrader) is set to SO_LINGER(on, 0) and TCP_USER_TIMEOUT
<= 2 s (src/adapters/sockets.py; a Lightstreamer client on an aiohttp too old for its socket factory logs an
error and streams without them), and systemd (`Restart=always`) restarts the process at STARTING. Every component takes the exit function as an injectable dependency so tests never exit.
"""
from __future__ import annotations

import logging
import os
import sys
from collections.abc import Callable
from typing import Final, NoReturn

log = logging.getLogger(__name__)

ExitFn = Callable[[str], None]

EXIT_CODES: Final[dict[str, int]] = {
    "fence": 75,            # lease not renewed for 12 s, or etcd reported the lease gone
    "init_failed": 76,      # LEADER_INIT failed or passed its 120 s deadline
    "abdicate": 77,         # DRAINING passed its 10 s deadline
    "critical_task": 78,    # a leadership-critical task died (orchestrator, lease-keeper link)
    "watchdog": 79,         # event loop could not assert a valid lease
}


def exit_code(reason: str) -> int:
    return EXIT_CODES.get(reason.split(":", 1)[0], 70)


def hard_exit(reason: str) -> NoReturn:
    """Log critically and terminate the process immediately (never returns)."""
    try:
        log.critical("HARD EXIT: %s", reason)
        sys.stderr.write(f"xtnl engine hard exit: {reason}\n")
        sys.stderr.flush()
    finally:
        os._exit(exit_code(reason))
