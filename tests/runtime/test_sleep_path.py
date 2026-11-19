"""PC sleep: detection, the restart path and how the ARM treats it (plan S38g, round 14; design §A7 "PC sleep").

* The clock detectors of md-writer and the BarClock see a Windows sleep (both clocks move together) as a suspend.
* A sleep longer than the 12 s fence: the lease keeper fences on wake (unchanged), names the suspend in the reason
  and records it node-locally first (`SleepFenceLog`).
* After that restart the ARM leaves the sleep's own term end out of `unclean_restarts` (a crash still counts) and
  reports the sleep restart once; the bars the sleep covered are spanned, and a release decided just before the
  sleep is still decided right after it (no FAULT page).
* The session tells its suspend listeners (the BarClock latch, md-writer) on `resume_after_sleep`.

Synthetic ids and times only.
"""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import fakeredis

from src.marketdata.clockstep import STEP, SUSPEND, SUSPEND_AFTER_MS, WallMonoDetector
from src.marketdata.timing import COLD_SKEW, pre_fire_ms
from src.marketdata.types import M15
from src.riskmgr.budget import ErrorBudget
from src.riskmgr.config import ArmSettings
from src.riskmgr.sm11 import ArmState
from src.runtime.lease import SUSPEND_GAP_S, LeaseKeeper, system_suspend_clock
from src.runtime.optional_plan import SuspendLatch
from src.runtime.session import EngineSession, SessionContext
from src.runtime.sleeplog import (
    SLEEP_RESTART_MATCH_MS,
    SleepAwareRestarts,
    SleepFenceLog,
    without_sleep_restarts,
)
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.riskmgr.failed_fakes import failed_rig
from tests.riskmgr.test_arm_join import join_rig
from tests.riskmgr.tracker_fakes import image, position
from tests.runtime.fakes import Exits, FakeClock, FakeEtcd
from tests.runtime.test_uptime_wiring import EPOCH, run_id

TF = M15.timeframe_ms
T0 = 1_773_152_100_000


# ---------------------------------------------------------------------------------------------------- detection
def test_a_windows_sleep_moves_both_clocks_and_is_a_suspend() -> None:
    plain = WallMonoDetector()
    watch = WallMonoDetector(suspend_after_ms=SUSPEND_AFTER_MS)
    for d in (plain, watch):
        d.observe(T0, 5_000)
    # a 60 s sleep: wall and monotonic both advance 60 s (Windows), no step between them
    assert plain.observe(T0 + 60_000, 65_000) is None
    got = watch.observe(T0 + 60_000, 65_000)
    assert got is not None and got.kind == SUSPEND and (got.last_wall_ms, got.mono_delta_ms) == (T0, 60_000)
    assert watch.suspends == 1 and watch.steps == 0
    # a normal 250 ms pass is nothing; a wall step is still a step
    assert watch.observe(T0 + 60_250, 65_250) is None
    step = watch.observe(T0 + 70_250, 65_500)
    assert step is not None and step.kind == STEP


def test_the_lease_keeper_names_and_records_a_sleep_fence(tmp_path: Path) -> None:
    clk = FakeClock(1000.0)
    wall = FakeClock(T0 / 1000)
    etcd = FakeEtcd(clk)
    exits = Exits()
    awake = FakeClock(500.0)                                 # the suspend clock's non-sleeping half
    log = SleepFenceLog(tmp_path / "sleep_fences.jsonl", clock_ms=lambda: int(wall.t * 1000))
    k = LeaseKeeper(etcd, clock=clk, fence=exits, wall=wall, on_suspended_fence=log.record,
                    suspend_clock=lambda: (clk.t, awake.t))
    k.campaign("n1")
    for _ in range(5):                                       # healthy: renewing every second
        clk.advance(1.0)
        wall.advance(1.0)
        awake.advance(1.0)
        k.tick()
    assert exits.reasons == []
    clk.advance(90.0)                                        # the PC slept 90 s (monotonic counts on Windows)
    wall.advance(90.0)                                       # ... and the unbiased clock did not move
    k.tick()
    assert len(exits.reasons) == 1 and exits.reasons[0].startswith("fence")
    assert "process suspended 90 s" in exits.reasons[0] and k.suspended_fences == 1
    fences = log.fences()
    assert [(f.gap_ms, f.reported) for f in fences] == [(90_000, False)]


def test_a_stalled_process_is_an_ordinary_fence_not_a_sleep_fence(tmp_path: Path) -> None:
    """Round 14 review: a 90 s keeper gap the suspend clock does not confirm (a GIL hog, a VM pause, swap thrashing:
    the non-sleeping clock moved too) or a wall step alone is an ordinary fence: named a stall, never recorded, so
    its restart still counts toward the FAILED restart budget. With no suspend clock every fence is ordinary."""
    for case in ("stall", "wall_step", "no_clock", "unreadable"):
        clk, wall, awake = FakeClock(1000.0), FakeClock(T0 / 1000), FakeClock(500.0)
        etcd, exits = FakeEtcd(clk), Exits()
        log = SleepFenceLog(tmp_path / f"{case}.jsonl", clock_ms=lambda: T0)

        def unreadable() -> tuple[float, float]:
            raise OSError("no clock")
        suspend = {"no_clock": None, "unreadable": unreadable}.get(case, lambda: (clk.t, awake.t))
        k = LeaseKeeper(etcd, clock=clk, fence=exits, wall=wall, on_suspended_fence=log.record,
                        suspend_clock=suspend)
        k.campaign("n1")
        for _ in range(3):
            clk.advance(1.0)
            wall.advance(1.0)
            awake.advance(1.0)
            k.tick()
        if case == "wall_step":
            wall.advance(90.0)                               # the wall clock stepped; nothing else moved
            k.tick()
            assert exits.reasons == [], case                 # the lease is still fresh: no fence at all
            clk.advance(13.0)                                # then a plain 13 s stall fences it
            awake.advance(13.0)
            wall.advance(13.0)
        else:
            clk.advance(90.0)                                # the process did not run; the machine was awake
            awake.advance(90.0)
            wall.advance(90.0)
        k.tick()
        assert len(exits.reasons) == 1 and "process stalled" in exits.reasons[0], case
        assert "suspended" not in exits.reasons[0] and k.suspended_fences == 0 and log.fences() == [], case


def test_the_system_suspend_clock_reads_two_clocks() -> None:
    clock = system_suspend_clock()
    if clock is None:                                        # a platform without one: every fence is ordinary
        return
    counting, awake = clock()
    assert counting > 0 and awake > 0 and abs(counting - awake) >= 0


def test_an_ordinary_fence_is_not_a_sleep_fence(tmp_path: Path) -> None:
    clk = FakeClock(1000.0)
    etcd = FakeEtcd(clk)
    exits = Exits()
    calls: list[float] = []
    k = LeaseKeeper(etcd, clock=clk, fence=exits, wall=clk, on_suspended_fence=calls.append)
    k.campaign("n1")
    etcd.down = True
    while not exits.reasons:
        clk.advance(0.05)
        k.tick()
    assert "suspended" not in exits.reasons[0] and calls == [] and SUSPEND_GAP_S == 5.0


# ---------------------------------------------------------------------------------------------------- the record
def test_the_sleep_log_keeps_24_hours_reports_once_and_skips_torn_lines(tmp_path: Path) -> None:
    now = [T0]
    log = SleepFenceLog(tmp_path / "s.jsonl", clock_ms=lambda: now[0])
    log.record(30.0)
    now[0] += 3_600_000
    log.record(45.5)
    with open(log.path, "a", encoding="utf-8") as fh:
        fh.write('{"at_ms": "x"}\n{torn')
    assert [f.gap_ms for f in log.fences()] == [30_000, 45_500]
    assert [f.gap_ms for f in log.take_unreported()] == [30_000, 45_500]
    assert log.take_unreported() == []                       # reported once
    now[0] += 24 * 3_600_000                                 # the first one is older than 24 h at the next write
    log.record(10.0)
    assert [f.gap_ms for f in log.fences()] == [45_500, 10_000]
    assert all(set(json.loads(x)) == {"at_ms", "gap_ms", "reported"} for x in log.path.read_text().splitlines())


def test_a_missing_or_unwritable_log_never_raises(tmp_path: Path) -> None:
    log = SleepFenceLog(tmp_path / "missing" / "s.jsonl")
    assert log.fences() == [] and log.take_unreported() == []
    blocked = tmp_path / "file"
    blocked.write_text("x")
    SleepFenceLog(blocked / "s.jsonl").record(20.0)          # a path under a file: logged, never raised


# ---------------------------------------------------------------------------------------------------- the ARM
def test_a_sleep_restart_is_not_an_unclean_restart_but_a_crash_still_is() -> None:
    fence = T0
    restart = T0 + 40_000                                    # the next LEADER_INIT closed the old term `lost`
    crash1, crash2 = T0 + 600_000, T0 + 900_000
    assert without_sleep_restarts([restart, crash1, crash2], [fence]) == [crash1, crash2]
    assert without_sleep_restarts([restart], []) == [restart]                       # no record: it counts
    assert without_sleep_restarts([fence + SLEEP_RESTART_MATCH_MS + 1], [fence]) == [fence + SLEEP_RESTART_MATCH_MS + 1]
    two_sleeps = without_sleep_restarts([T0 + 10_000, T0 + 20_000], [T0, T0 + 15_000])
    assert two_sleeps == []                                   # each sleep matches its own restart, once


async def test_the_restart_counter_reaches_failed_on_crashes_only() -> None:
    class History:
        async def unclean_ends_since(self, since_ms: int) -> list[int]:
            return [T0 + 40_000, T0 + 300_000, T0 + 600_000]

    r = failed_rig()
    fail = replace(r.budget.settings, restarts_n=3, restart_window_s=1800)
    now = T0 + 700_000
    # three restarts, one of them a sleep's: below the limit, no FAILED
    budget = ErrorBudget(fail, calendar=r.calendar, health=r.health, pg=r.pg)
    ends = await SleepAwareRestarts(History(), lambda: [T0]).unclean_ends_since(now - 1_800_000)
    budget.note_restarts(ends, now, armed=True)
    assert budget.take() == []
    # the same three as crashes: ARM-global FAILED unclean_restarts
    crashes = ErrorBudget(fail, calendar=r.calendar, health=r.health, pg=r.pg)
    ends = await SleepAwareRestarts(History(), lambda: []).unclean_ends_since(now - 1_800_000)
    crashes.note_restarts(ends, now, armed=True)
    assert [f.reason for f in crashes.take()] == ["unclean_restarts"]
    # an unreadable record: every end counts (fail toward FAILED)

    def boom() -> list[int]:
        raise OSError("unreadable")
    assert await SleepAwareRestarts(History(), boom).unclean_ends_since(0) == [T0 + 40_000, T0 + 300_000,
                                                                              T0 + 600_000]


async def test_drill_sleep_60_s_after_a_releasing_bar_end_completes_without_a_fault_page() -> None:
    """Design §A7 drill: the PC sleeps 60 s starting 30 s after a releasing bar end (longer than the fence: the
    engine restarts into a new term). The release is decided right after the restart, with no FAULT page."""
    j = join_rig()
    try:
        await j.tracker.step()
        await j.notices.step()
        cursor = j.row().last_decided_bar
        assert cursor is not None
        b1, b2 = cursor + TF, cursor + 2 * TF
        fire = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
        j.md.final_bar(b1, "1.2050")
        j.md.live_quote("1.2150", fire - 1_000)
        await j.at(fire + 100)
        j.stop_version("1.2000", b2 - 60_000)                # the user moved to break-even before B2 ended
        j.snapshot.images = [image(position("P1", stop="1.2000", updated_ms=b2 - 60_000))]
        j.md.final_bar(b2, "1.2150")                         # B2 confirms the trigger at target: a release bar
        await j.at(b2 + 500)                                 # its POST not decided yet (bar just in)
        j.clock.now = b2 + 30_000                            # ... the PC sleeps 60 s from here: fence, restart
        j.new_term()
        await j.at(b2 + 90_000 + 2_000)
        post = [r for r in j.decisions("POST") if r.bar_end_ms == b2]
        assert [(r.outcome, r.trigger_met) for r in post] == [("COMPLETE", True)]
        assert j.row().state == ArmState.STRATEGY_COMPLETE.value
        assert j.rec.pages == []
    finally:
        j.barclock.close()


async def test_a_short_sleep_holds_one_barclock_pass_through_the_latch() -> None:
    j = join_rig()
    latch = SuspendLatch()
    j.barclock._suspended = latch.take  # noqa: SLF001
    try:
        await j.tracker.step()
        await j.at(j.clock.now + 250)
        passes, steps = j.barclock.counters.passes, j.barclock.counters.suspends
        latch.note()
        await j.at(j.clock.now + 250)
        assert j.barclock.counters.suspends == steps + 1 and j.barclock.counters.passes == passes + 1
        held_at = j.barclock.last_pass_ms
        await j.at(j.clock.now + 250)                        # the next pass runs normally
        assert j.barclock.counters.suspends == steps + 1 and j.barclock.last_pass_ms != held_at
    finally:
        j.barclock.close()


# ---------------------------------------------------------------------------------------------------- the session
async def test_resume_after_sleep_tells_the_suspend_listeners(tmp_path: Path) -> None:
    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [mock_adapter(fixture(wall_ms()))], lease=FakeLease(),
                         run_id_reader=run_id, exit_fn=lambda why: None, timings=FAST_TIMINGS)
    s = EngineSession(ctx, EPOCH)
    heard: list[int | None] = []

    def boom(_ms: int | None) -> None:
        raise RuntimeError("listener failed")

    try:
        s.add_suspend_listener(boom)                         # a raising listener never stops the others
        s.add_suspend_listener(heard.append)
        assert s.resume_after_sleep(45_000) is False         # no catch-up in this session: as before
        assert heard == [45_000]
    finally:
        await s.aclose()
