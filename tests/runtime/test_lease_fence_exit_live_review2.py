"""A fence reaches its exit even when logging blocks (live review 2026-11-25; operations.md §3: at 12 s without
renewal the keeper hard-exits the engine, os._exit, off the event loop).

The engine's only log handler writes to a stderr pipe that a supervising script reads; when that reader stops
draining, a log write blocks while holding the handler lock. A keeper-thread log call must then neither keep the
keeper from fencing nor keep a fenced engine from exiting."""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator

import pytest

from src.runtime import lease as lease_mod
from src.runtime.exits import hard_exit
from src.runtime.lease import LeaseKeeper, decouple_keeper_logging, restore_keeper_logging
from tests.runtime.fakes import Exits, FakeClock, FakeEtcd


class BlockingHandler(logging.Handler):
    """A handler whose emit() blocks until the test ends (a stalled stderr pipe)."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.release = threading.Event()
        self.entered = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        self.entered.set()
        self.release.wait()

    def attach(self) -> None:
        root = logging.getLogger()
        root.addHandler(self)
        root.setLevel(logging.DEBUG)


@pytest.fixture
def blocked_root() -> Iterator[BlockingHandler]:
    """A blocking root handler, attached by the test (`attach()`) once its set-up has logged what it needs."""
    root = logging.getLogger()
    handler = BlockingHandler()
    old_level = root.level
    try:
        yield handler
    finally:
        handler.release.set()
        root.removeHandler(handler)
        root.setLevel(old_level)
        restore_keeper_logging()


def run_bounded(fn: Callable[[], None], limit_s: float) -> bool:
    """Run fn on its own daemon thread; True when it finished within limit_s (a block never hangs the test)."""
    t = threading.Thread(target=fn, daemon=True)
    t.start()
    t.join(limit_s)
    return not t.is_alive()


def wait_for(cond: Callable[[], bool], limit_s: float) -> float | None:
    start = time.monotonic()
    while time.monotonic() - start < limit_s:
        if cond():
            return time.monotonic() - start
        time.sleep(0.01)
    return None


def test_fence_calls_the_exit_without_logging_first(blocked_root: BlockingHandler) -> None:
    """The duplicate CRITICAL "FENCE" line before the fence function is gone: hard_exit logs the reason after it
    has armed its exit. A blocked handler (no keeper queue installed) cannot hold the fence back."""
    clk = FakeClock(1000.0)
    etcd = FakeEtcd(clk)
    exits = Exits()
    k = LeaseKeeper(etcd, clock=clk, fence=exits)
    k.campaign("n1")
    blocked_root.attach()

    def fence_now() -> None:
        clk.t += 12.5                       # no keepalive attempt: the fence check comes first
        k.tick()

    assert run_bounded(fence_now, 1.0), "the fence blocked on a log write"
    assert exits.reasons and exits.reasons[0].startswith("fence: lease not renewed")


def test_keepalive_failure_log_cannot_stall_the_keeper(blocked_root: BlockingHandler) -> None:
    """`lease keepalive failed` is logged on the keeper thread on every failed renewal; with the keeper's queue
    installed (start() does it) the call returns at once and the keeper still fences past fence_after_s."""
    decouple_keeper_logging()
    clk = FakeClock(1000.0)
    etcd = FakeEtcd(clk)
    exits = Exits()
    k = LeaseKeeper(etcd, clock=clk, fence=exits)
    k.campaign("n1")
    blocked_root.attach()

    def failing_keepalive(lease_id: int, *, timeout_s: float) -> int:
        raise lease_mod.EtcdError("etcd unreachable")
    etcd.lease_keepalive = failing_keepalive  # type: ignore[method-assign]

    def drive() -> None:
        while not exits.reasons and clk.t < 1015.0:
            clk.t += 0.5
            k.tick()

    started = time.monotonic()
    assert run_bounded(drive, 1.5), "a keeper log call blocked on the stalled handler"
    assert time.monotonic() - started < 1.5
    assert exits.reasons and exits.reasons[0].startswith("fence: lease not renewed")
    # the listener is the one stuck in the blocked handler, never the keeper; nothing was dropped
    assert wait_for(blocked_root.entered.is_set, 1.0) is not None


def test_keeper_records_still_reach_the_root_handlers_after_the_queue() -> None:
    """The queue keeps every line (§17: the log is the only channel on the single PC) and delivers it to the
    handlers propagation would have reached."""
    got: list[logging.LogRecord] = []

    class Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            got.append(record)

    root = logging.getLogger()
    h = Collect(logging.DEBUG)
    root.addHandler(h)
    try:
        decouple_keeper_logging()
        for i in range(50):
            lease_mod.log.warning("line %d", i)
        assert wait_for(lambda: len(got) >= 50, 2.0) is not None
        assert [r.getMessage() for r in got[:50]] == [f"line {i}" for i in range(50)]
    finally:
        root.removeHandler(h)
        restore_keeper_logging()
    assert lease_mod.log.propagate is True


def test_backstop_exits_when_the_fence_function_blocks() -> None:
    """hard_exit (or anything before os._exit) blocked on a log write: the backstop armed before any I/O forces
    the exit with the fence's reason."""
    clk = FakeClock(1000.0)
    etcd = FakeEtcd(clk)
    stuck = threading.Event()
    forced: list[str] = []

    def blocking_fence(reason: str) -> None:
        stuck.wait()                        # never returns until the test ends

    k = LeaseKeeper(etcd, clock=clk, fence=blocking_fence, backstop=forced.append, backstop_after_s=0.2)
    k.campaign("n1")
    clk.t += 12.5
    threading.Thread(target=k.tick, daemon=True).start()
    try:
        took = wait_for(lambda: bool(forced), 1.5)
        assert took is not None, "the backstop did not fire"
        assert forced[0].startswith("fence: lease not renewed")
    finally:
        stuck.set()


def test_backstop_is_armed_before_the_sleep_fence_hook() -> None:
    """§3: the sleep-fence record runs just before the hard exit, best effort; a hook blocked in its file write or
    its log call cannot keep the process alive."""
    clk = FakeClock(1000.0)
    etcd = FakeEtcd(clk)
    stuck = threading.Event()
    forced: list[str] = []
    readings = iter([(0.0, 0.0), (20.0, 0.0)])          # the machine slept 20 s between the two iterations

    k = LeaseKeeper(etcd, clock=clk, fence=Exits(), wall=clk, on_suspended_fence=lambda s: stuck.wait() and None,
                    suspend_clock=lambda: next(readings), backstop=forced.append, backstop_after_s=0.2)
    k.campaign("n1")
    clk.t += 0.1
    k.tick()
    clk.t += 20.0
    threading.Thread(target=k.tick, daemon=True).start()
    try:
        assert wait_for(lambda: bool(forced), 1.5) is not None, "the hook held the exit back"
        assert "process suspended" in forced[0]
    finally:
        stuck.set()


def test_production_fence_gets_the_os_exit_backstop_and_injected_fences_none() -> None:
    clk = FakeClock(1000.0)
    prod = LeaseKeeper(FakeEtcd(clk), clock=clk, fence=hard_exit)
    test = LeaseKeeper(FakeEtcd(clk), clock=clk, fence=Exits())
    assert prod._backstop is lease_mod._exit_now
    assert test._backstop is None
    assert prod._backstop_after_s == lease_mod.FENCE_BACKSTOP_S
    assert 12.0 + lease_mod.FENCE_BACKSTOP_S < 15      # the forced exit still lands inside the TTL
