"""UptimeTracker (src/runtime/uptime.py): engine_runs start / heartbeat / stop, sleep detection, the leader's
adapter_coverage heartbeat, XTNL_UPTIME_PING_S. No network: an in-memory store and injected clocks."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from src.observability.metrics import MetricsRegistry
from src.runtime.uptime import (
    SleepDetector,
    SuspendEvent,
    UptimeTracker,
    ping_s_from_env,
)

T0 = 1_700_000_000_000


@dataclass
class Clocks:
    wall: int = T0
    mono: float = 1000.0

    def advance(self, s: float, *, wall: float | None = None) -> None:
        self.mono += s
        self.wall += int((s if wall is None else wall) * 1000)


@dataclass
class Store:
    calls: list[tuple[object, ...]] = field(default_factory=list)
    fail: set[str] = field(default_factory=set)
    rows: dict[str, dict[str, object]] = field(default_factory=dict)
    coverage: dict[str, int] = field(default_factory=dict)
    epoch_now: int = 7

    def _maybe_fail(self, name: str) -> None:
        if name in self.fail:
            raise ConnectionError("postgres down")

    async def start_run(self, run_id: str, node_id: str, at_ms: int) -> None:
        self._maybe_fail("start_run")
        self.calls.append(("start_run", run_id, node_id, at_ms))
        self.rows.setdefault(run_id, {"started": at_ms, "last_seen": at_ms, "gaps": 0, "stopped": None})

    async def heartbeat(self, run_id: str, at_ms: int) -> bool:
        self._maybe_fail("heartbeat")
        self.calls.append(("heartbeat", at_ms))
        row = self.rows.get(run_id)
        if row is None:
            return False
        row["last_seen"] = at_ms
        return True

    async def note_suspended(self, run_id: str, at_ms: int) -> None:
        self._maybe_fail("note_suspended")
        self.calls.append(("note_suspended", at_ms))
        self.rows[run_id]["gaps"] = int(self.rows[run_id]["gaps"]) + 1  # type: ignore[call-overload]

    async def stop_run(self, run_id: str, at_ms: int, reason: str) -> None:
        self._maybe_fail("stop_run")
        self.calls.append(("stop_run", at_ms, reason))
        self.rows[run_id]["stopped"] = (at_ms, reason)

    async def advance_coverage(self, epoch_token: int, adapter_id: str, at_ms: int, run_id: str | None) -> bool:
        self._maybe_fail("advance_coverage")
        self.calls.append(("coverage", epoch_token, adapter_id, at_ms))
        if epoch_token != self.epoch_now:
            return False
        self.coverage[adapter_id] = max(self.coverage.get(adapter_id, 0), at_ms)
        return True

    def names(self) -> list[object]:
        return [c[0] for c in self.calls]


def tracker(store: Store, c: Clocks, **kw: object) -> UptimeTracker:
    return UptimeTracker(store, "pc-1", clock_ms=lambda: c.wall, monotonic=lambda: c.mono,  # type: ignore[arg-type]
                         run_id="00000000-0000-0000-0000-000000000001", **kw)  # type: ignore[arg-type]


# ===================================================================== XTNL_UPTIME_PING_S
def test_ping_interval_default_range_and_refusals() -> None:
    assert ping_s_from_env({}) == 900
    assert ping_s_from_env({"XTNL_UPTIME_PING_S": "60"}) == 60
    assert ping_s_from_env({"XTNL_UPTIME_PING_S": " 1800 "}) == 1800
    for bad in ("59", "1801", "15m", "-1"):
        with pytest.raises(ValueError, match="XTNL_UPTIME_PING_S"):
            ping_s_from_env({"XTNL_UPTIME_PING_S": bad})
    with pytest.raises(ValueError):
        UptimeTracker(Store(), "n", ping_s=30)


# ===================================================================== sleep detection
def test_sleep_detector_wall_jump_stall_and_normal_ticks() -> None:
    d = SleepDetector(5.0)
    assert d.threshold_ms == 120_000
    d.reset(T0, 100.0)
    assert d.observe(T0 + 5_000, 105.0) is None                       # a normal tick
    assert d.observe(T0 + 15_000, 112.0) is None                      # jitter / a slow tick
    ev = d.observe(T0 + 15_000 + 3_600_000, 117.0)                    # wall +1 h, monotonic +5 s: slept
    assert ev is not None and ev.kind == "wall_jump" and ev.wall_gap_ms == 3_600_000
    ev2 = d.observe(T0 + 15_000 + 3_600_000 + 1_800_000, 117.0 + 1800)  # both +30 min: suspended / stalled
    assert ev2 is not None and ev2.kind == "stalled"
    assert d.observe(T0 + 10_000, 117.0 + 1805) is None               # wall stepped back: not a suspension
    d2 = SleepDetector(120.0)
    assert d2.threshold_ms == 240_000                                  # max(2 x tick, 120 s)


# ===================================================================== start / heartbeat / stop
async def test_start_row_heartbeat_every_ping_and_clean_stop() -> None:
    s, c = Store(), Clocks()
    t = tracker(s, c, ping_s=900)
    await t.tick()
    assert s.names() == ["start_run", "heartbeat"]
    for _ in range(10):                                                # 50 s: not due
        c.advance(5)
        await t.tick()
    assert s.names().count("heartbeat") == 1
    c.advance(900)                                                     # a long but monotonic-matched gap
    await t.tick()
    assert s.names().count("heartbeat") == 2 and t.suspended_gaps == 1  # 905 s since the last tick: stalled
    t.note_stop_reason("SIGINT")
    await t.stop("stopped")
    assert s.calls[-1] == ("stop_run", c.wall, "SIGINT")              # the first reason wins
    await t.stop("again")
    assert s.names().count("stop_run") == 1                            # idempotent


async def test_heartbeat_cadence_follows_the_ping_interval() -> None:
    s, c = Store(), Clocks()
    t = tracker(s, c, ping_s=60)
    await t.tick()
    for _ in range(12):
        c.advance(5)
        await t.tick()
    assert s.names().count("heartbeat") == 2                           # t=0 and t=60 s
    assert t.last_seen_ms == T0 + 60_000


async def test_a_failed_start_row_is_retried_and_a_missing_row_rewritten() -> None:
    s, c = Store(fail={"start_run"}), Clocks()
    t = tracker(s, c)
    await t.tick()
    assert not t.row_written and "heartbeat" not in s.names()
    s.fail.clear()
    c.advance(5)
    await t.tick()
    assert t.row_written and s.names() == ["start_run", "heartbeat"]
    s.rows.clear()                                                     # e.g. the row was lost
    assert await t.heartbeat() is False and not t.row_written
    c.advance(5)
    await t.tick()
    assert s.names().count("start_run") == 2


async def test_stop_writes_the_start_row_first_if_it_never_got_written() -> None:
    s, c = Store(fail={"start_run"}), Clocks()
    t = tracker(s, c)
    await t.tick()
    s.fail.clear()
    await t.stop()
    assert s.names() == ["start_run", "stop_run"] and s.calls[-1][2] == "stopped"


async def test_stop_failure_is_logged_not_raised() -> None:
    s, c = Store(), Clocks()
    t = tracker(s, c)
    await t.tick()
    s.fail.add("stop_run")
    await t.stop("SIGTERM")                                            # no exception
    assert t.stopped


# ===================================================================== suspension
async def test_suspension_counts_writes_heartbeats_and_triggers_the_resume_hook() -> None:
    s, c = Store(), Clocks()
    seen: list[SuspendEvent] = []
    t = tracker(s, c, ping_s=900, on_suspended=seen.append)
    await t.tick()
    c.advance(5, wall=7_200)                                           # PC slept 2 h; monotonic paused
    await t.tick()
    assert t.suspended_gaps == 1 and len(seen) == 1 and seen[0].kind == "wall_jump"
    assert s.names() == ["start_run", "heartbeat", "note_suspended", "heartbeat"]   # heartbeat forced at once
    assert s.rows[t.run_id]["gaps"] == 1
    reg = MetricsRegistry("pc-1")
    reg.register("u", t.samples)
    text = reg.render()
    assert 'xtnl_suspended_gaps_total{node="pc-1"} 1' in text
    assert 'xtnl_engine_uptime_seconds{node="pc-1"} 7200' in text


async def test_a_suspended_gap_that_cannot_be_written_is_written_later() -> None:
    s, c = Store(), Clocks()
    t = tracker(s, c)
    await t.tick()
    s.fail.add("note_suspended")
    c.advance(5, wall=3_600)
    await t.tick()
    assert t.suspended_gaps == 1 and s.rows[t.run_id]["gaps"] == 0
    s.fail.clear()
    c.advance(5)
    await t.tick()
    assert s.rows[t.run_id]["gaps"] == 1


async def test_a_failing_resume_hook_never_stops_the_tracker() -> None:
    s, c = Store(), Clocks()

    def boom(_: SuspendEvent) -> None:
        raise RuntimeError("no session")

    t = tracker(s, c, on_suspended=boom)
    await t.tick()
    c.advance(5, wall=600)
    await t.tick()
    assert t.suspended_gaps == 1


# ===================================================================== coverage
async def test_coverage_is_written_for_eligible_adapters_only_while_leader() -> None:
    s, c = Store(), Clocks()
    target: list[tuple[int, Sequence[str]] | None] = [None]
    t = tracker(s, c, ping_s=900, coverage=lambda: target[0])
    await t.tick()                                                     # not leader: nothing
    assert "coverage" not in s.names()
    target[0] = (7, ["stonex"])                                        # caught up and streaming
    c.advance(5)
    await t.tick()                                                     # newly eligible: written at once
    assert s.coverage == {"stonex": T0 + 5_000}
    c.advance(5)
    await t.tick()                                                     # not due: not rewritten
    assert s.names().count("coverage") == 1
    c.advance(900)
    await t.tick()                                                     # heartbeat: every eligible adapter
    assert s.coverage["stonex"] == c.wall
    target[0] = (6, ["stonex"])                                        # a stale epoch: skipped, not advanced
    before = s.coverage["stonex"]
    c.advance(900)
    await t.tick()
    assert s.coverage["stonex"] == before


async def test_an_adapter_eligible_again_is_written_at_once() -> None:
    s, c = Store(), Clocks()
    target: list[tuple[int, Sequence[str]] | None] = [(7, ["stonex"])]
    t = tracker(s, c, ping_s=900, coverage=lambda: target[0])
    await t.tick()
    target[0] = (7, [])                                                # a resume catch-up holds it
    c.advance(5)
    await t.tick()
    target[0] = (7, ["stonex"])
    c.advance(5)
    await t.tick()
    assert s.coverage["stonex"] == T0 + 10_000


# ===================================================================== task
async def test_run_loop_survives_store_errors_and_stops_on_request() -> None:
    s, c = Store(fail={"start_run", "heartbeat"}), Clocks()
    t = UptimeTracker(s, "pc-1", tick_s=0.01, clock_ms=lambda: c.wall, monotonic=lambda: c.mono)  # type: ignore[arg-type]
    stop = asyncio.Event()
    task = asyncio.create_task(t.run(stop))
    await asyncio.sleep(0.05)
    s.fail.clear()
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert t.row_written and "heartbeat" in s.names()
