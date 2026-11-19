"""Stuck-frontier detection, classified by cause (plan S22, R11-M11; design §B14, §B6.6)."""
from __future__ import annotations

import pytest

from src.riskmgr.frontier import (
    STUCK_FRONTIER,
    STUCK_GRACE_MS,
    FrontierView,
    FrontierWatch,
    StuckCause,
    classify,
)

BAR = 1_700_000_000_000


def run(watch: FrontierWatch, views: list[FrontierView], start: int, seconds: int, *, counting: bool = True,
        pool_failing: bool = False, step_s: int = 1) -> list[object]:
    out = []
    for s in range(0, seconds + 1, step_s):
        fault = watch.observe(views, start + s * 1_000, counting=counting, pool_failing=pool_failing)
        if fault is not None:
            out.append((s, fault))
    return out


def test_a_stuck_frontier_with_failing_arm_writes_is_left_to_the_pool_down_row() -> None:
    assert classify(pool_failing=True) is StuckCause.ARM_WRITE
    watch = FrontierWatch()
    views = [FrontierView(position_id="p1", overdue_bar_ms=BAR)]
    # past the grace and past any fail_pool_down_s value (300-3600 s): never a second fault for one pool outage
    assert run(watch, views, 0, 3_700, pool_failing=True, step_s=10) == []
    assert watch.elapsed_ms("p1") == 3_700_000
    # the pool comes back but the frontier is still stuck: DATA, filed at once (the grace has long passed)
    fault = watch.observe(views, 3_701_000, counting=True)
    assert fault is not None and fault.cause is StuckCause.DATA and fault.key == f"{STUCK_FRONTIER}:data"


def test_a_due_fault_is_filed_once_per_episode() -> None:
    watch = FrontierWatch()
    views = [FrontierView(position_id="p1", overdue_bar_ms=BAR)]
    faults = run(watch, views, 0, 600)
    assert [s for s, _ in faults] == [STUCK_GRACE_MS // 1_000]   # not again on every pass after the grace
    # the cursor moves (episode over), then a new bar sticks: a new episode, filed again at its grace
    assert watch.observe([FrontierView(position_id="p1", overdue_bar_ms=None)], 601_000, counting=True) is None
    later = [FrontierView(position_id="p1", overdue_bar_ms=BAR + 900_000)]
    faults = run(watch, later, 602_000, 200)
    assert [s for s, _ in faults] == [STUCK_GRACE_MS // 1_000]


def test_a_data_stuck_frontier_fails_at_once_after_the_grace() -> None:
    watch = FrontierWatch()
    views = [FrontierView(position_id="p2", overdue_bar_ms=BAR), FrontierView(position_id="p1", overdue_bar_ms=BAR)]
    faults = run(watch, views, 0, STUCK_GRACE_MS // 1_000)
    assert len(faults) == 1
    second, fault = faults[0]  # type: ignore[misc]
    assert second == STUCK_GRACE_MS // 1_000
    assert fault.cause is StuckCause.DATA and fault.position_ids == ("p1", "p2") and fault.oldest_bar_ms == BAR


def test_failed_or_paused_positions_and_non_counting_time_never_count() -> None:
    watch = FrontierWatch()
    held = [FrontierView(position_id="p1", overdue_bar_ms=BAR, held=True)]
    assert run(watch, held, 0, 600) == []
    views = [FrontierView(position_id="p1", overdue_bar_ms=BAR)]
    assert run(watch, views, 700_000, 600, counting=False) == []       # no ARM running, engine PostgreSQL down
    assert watch.elapsed_ms("p1") == 0


def test_counting_resumes_from_its_total_after_a_pause_and_a_held_pass_resets_it() -> None:
    watch = FrontierWatch()
    views = [FrontierView(position_id="p1", overdue_bar_ms=BAR)]
    assert run(watch, views, 0, 60, step_s=30) == []                    # 60 s counting
    assert watch.elapsed_ms("p1") == 60_000
    assert run(watch, views, 90_000, 570, counting=False, step_s=30) == []   # a 600 s pause (passes to 660 s)
    assert watch.elapsed_ms("p1") == 60_000                              # the pause neither adds nor resets
    assert watch.observe(views, 690_000, counting=True) is None          # counting resumes at this pass
    assert watch.observe(views, 720_000, counting=True) is None and watch.elapsed_ms("p1") == 90_000
    fault = watch.observe(views, 750_000, counting=True)                 # exactly 120 s of counting time
    assert fault is not None and fault.cause is StuckCause.DATA and watch.elapsed_ms("p1") == STUCK_GRACE_MS
    # a held pass in between restarts the clock
    other = FrontierWatch()
    run(other, views, 0, 100)
    assert other.elapsed_ms("p1") == 100_000
    other.observe([FrontierView(position_id="p1", overdue_bar_ms=BAR, held=True)], 101_000, counting=True)
    assert other.elapsed_ms("p1") == 0
    assert run(other, views, 102_000, 119) == [] and other.elapsed_ms("p1") == 119_000


def test_the_clock_restarts_when_the_cursor_moves() -> None:
    watch = FrontierWatch()
    views = [FrontierView(position_id="p1", overdue_bar_ms=BAR)]
    run(watch, views, 0, 100)
    assert watch.elapsed_ms("p1") == 100_000
    moved = [FrontierView(position_id="p1", overdue_bar_ms=BAR + 900_000)]
    watch.observe(moved, 101_000, counting=True)
    assert watch.elapsed_ms("p1") == 0
    watch.observe([FrontierView(position_id="p1", overdue_bar_ms=None)], 102_000, counting=True)
    assert watch.elapsed_ms("p1") == 0


def test_bad_thresholds_are_refused() -> None:
    with pytest.raises(ValueError):
        FrontierWatch(grace_ms=0)
