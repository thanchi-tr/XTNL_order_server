"""hard_exit reaches os._exit even when its own log line or stderr write blocks (live review 2026-11-25 round 3;
operations.md §1 / §3: a fenced engine hard-exits, os._exit, and systemd / run-local.ps1 restart it).

The engine's only log handler writes to a stderr pipe that a supervising script reads; when that reader stops
draining, a write blocks. hard_exit arms a daemon backstop timer before any I/O, so the exit still happens.
os._exit is patched in every test: nothing here ends the test process."""
from __future__ import annotations

import io
import logging
import os
import sys
import threading
import time
from collections.abc import Callable, Iterator

import pytest

from src.runtime import exits as exits_mod
from src.runtime.exits import EXIT_CODES, hard_exit


class BlockingHandler(logging.Handler):
    """A handler whose emit() blocks until the test ends (a stalled stderr pipe)."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.unblock = threading.Event()
        self.entered = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        self.entered.set()
        self.unblock.wait()


class BlockingStream(io.TextIOBase):
    """A stderr whose write() blocks until the test ends."""

    def __init__(self) -> None:
        self.unblock = threading.Event()
        self.entered = threading.Event()

    def write(self, s: str) -> int:
        self.entered.set()
        self.unblock.wait()
        return len(s)


class Recorder:
    """Stands in for os._exit: records each exit code (it returns, so tests can observe it)."""

    def __init__(self) -> None:
        self.codes: list[int] = []
        self.called = threading.Event()

    def __call__(self, code: int) -> None:
        self.codes.append(code)
        self.called.set()


@pytest.fixture
def fake_exit(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    rec = Recorder()
    monkeypatch.setattr(os, "_exit", rec)
    monkeypatch.setattr(exits_mod, "HARD_EXIT_BACKSTOP_S", 0.2, raising=False)
    return rec


@pytest.fixture
def blocked_root() -> Iterator[BlockingHandler]:
    root = logging.getLogger()
    handler = BlockingHandler()
    old_level = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        handler.unblock.set()
        root.removeHandler(handler)
        root.setLevel(old_level)


def start_daemon(fn: Callable[[], None]) -> threading.Thread:
    t = threading.Thread(target=fn, daemon=True)
    t.start()
    return t


def test_a_blocked_log_handler_still_exits_with_the_reason_code(fake_exit: Recorder,
                                                                 blocked_root: BlockingHandler) -> None:
    t = start_daemon(lambda: hard_exit("fence: lease not renewed for 12.5 s"))
    assert blocked_root.entered.wait(1.0), "hard_exit did not log"
    assert fake_exit.called.wait(1.5), "a blocked log handler kept the process from exiting"
    assert fake_exit.codes[0] == EXIT_CODES["fence"]
    assert t.is_alive()                     # the caller is still stuck in the log call: the backstop exited


def test_a_blocked_stderr_still_exits(fake_exit: Recorder, monkeypatch: pytest.MonkeyPatch) -> None:
    stream = BlockingStream()
    monkeypatch.setattr(sys, "stderr", stream)
    try:
        start_daemon(lambda: hard_exit("abdicate: deadline passed"))
        assert stream.entered.wait(1.0), "hard_exit did not write to stderr"
        assert fake_exit.called.wait(1.5), "a blocked stderr kept the process from exiting"
        assert fake_exit.codes[0] == EXIT_CODES["abdicate"]
    finally:
        stream.unblock.set()


def test_the_backstop_is_armed_before_the_log_line(fake_exit: Recorder, monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    real_timer = threading.Timer

    def timer(*args: object, **kwargs: object) -> threading.Timer:
        order.append("backstop")
        return real_timer(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(threading, "Timer", timer)
    monkeypatch.setattr(exits_mod.log, "critical", lambda *a, **k: order.append("log"))
    hard_exit("init_failed: deadline")
    assert order == ["backstop", "log"]
    assert fake_exit.codes[0] == EXIT_CODES["init_failed"]   # the unblocked path exits at once


def test_a_refused_backstop_thread_still_exits(fake_exit: Recorder, monkeypatch: pytest.MonkeyPatch) -> None:
    def refused(*args: object, **kwargs: object) -> threading.Timer:
        raise RuntimeError("can't create new thread at interpreter shutdown")

    monkeypatch.setattr(threading, "Timer", refused)
    hard_exit("watchdog: lease deadline passed")
    assert fake_exit.codes == [EXIT_CODES["watchdog"]]


def test_the_backstop_is_short_and_inside_the_ttl() -> None:
    backstop_s = getattr(exits_mod, "HARD_EXIT_BACKSTOP_S", None)
    assert backstop_s is not None and 0 < backstop_s and 12.0 + backstop_s < 15.0   # §3: fence 12 s < TTL 15 s


def test_the_backstop_timer_is_a_daemon(fake_exit: Recorder) -> None:
    hard_exit("critical_task: orchestrator")
    timers = [t for t in threading.enumerate() if t.name == "hard-exit-backstop"]
    assert timers and all(t.daemon for t in timers)
    deadline = time.monotonic() + 1.0
    while any(t.is_alive() for t in timers) and time.monotonic() < deadline:
        time.sleep(0.01)
