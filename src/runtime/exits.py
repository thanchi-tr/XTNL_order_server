"""Hard exits (operations.md §3, §4).

The fence and the LEADER_INIT / DRAINING deadlines hard-exit the engine with `os._exit`: no
`finally` blocks, no atexit handlers, no graceful socket close. The kernel resets the sockets: every
broker socket (StoneX REST and Lightstreamer, cTrader) is set to SO_LINGER(on, 0) and TCP_USER_TIMEOUT
<= 2 s (src/adapters/sockets.py; a Lightstreamer client on an aiohttp too old for its socket factory logs an
error and streams without them), and systemd (`Restart=always`) restarts the process at STARTING. Every component takes the exit function as an injectable dependency so tests never exit.

Exit before I/O (live review 2026-11-25 round 3, §3 "hard-exits the engine (os._exit)"): `hard_exit` arms a
daemon backstop timer (`os._exit` with the same code after `HARD_EXIT_BACKSTOP_S`) before its log line and its
stderr write, so a log handler or a stderr pipe that has stopped draining can never keep the process alive. The
lease keeper's own fence backstop (`src/runtime/lease.py`) is armed earlier still; with a fence at 12 s both land
inside the TTL of 15 s.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
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

# The forced exit after a blocked log / stderr write: short, and 12 s + 2 s still lands inside the 15 s TTL (§3).
HARD_EXIT_BACKSTOP_S: Final[float] = 2.0


def exit_code(reason: str) -> int:
    return EXIT_CODES.get(reason.split(":", 1)[0], 70)


def _arm_backstop(exit_now: Callable[[int], object], code: int) -> None:
    """Start the daemon timer that forces the exit; never raises (a refused thread still reaches the finally)."""
    try:
        timer = threading.Timer(HARD_EXIT_BACKSTOP_S, exit_now, (code,))
        timer.daemon = True
        timer.name = "hard-exit-backstop"
        timer.start()
    except BaseException:  # noqa: BLE001 - e.g. no new thread at interpreter shutdown: the finally still exits
        pass


def hard_exit(reason: str) -> NoReturn:
    """Arm the exit backstop, log critically and terminate the process immediately (never returns)."""
    code = exit_code(reason)
    exit_now = os._exit          # bound once: the backstop and the finally call the same function
    try:
        _arm_backstop(exit_now, code)
        log.critical("HARD EXIT: %s", reason)
        sys.stderr.write(f"xtnl engine hard exit: {reason}\n")
        sys.stderr.flush()
    finally:
        exit_now(code)
