"""ARM liveness pages (plan S38g, round 14; design §B8 "Liveness").

* An armed position that silently stops being decided (more than 2 OPEN bars past their cap, or no schedule) is
  counted (the dead-man's `arm_overdue_positions`) and paged once per episode; paused, FAILED-held and released rows
  never count.
* A stalled BarClock or arm-tracker pass pages once per episode.
* The arm-relay backlog pages once per episode.

Synthetic ids and times only.
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from src.marketdata.timing import COLD_SKEW, decision_cap_ms
from src.marketdata.types import M15
from src.riskmgr.liveness import (
    NO_SCHEDULE,
    LivenessSettings,
    LivenessWatch,
    overdue_positions,
    undecided_open_bars,
)
from src.riskmgr.ports import Pager
from src.riskmgr.sm11 import ArmState
from tests.riskmgr.test_arm_join import join_rig
from tests.riskmgr.tracker_fakes import FakeGrid

TF = M15.timeframe_ms


class Pages:
    def __init__(self) -> None:
        self.got: list[tuple[str, str, str]] = []

    def page(self, severity: str, title: str, body: str) -> None:
        self.got.append((severity, title, body))

    def titles(self) -> list[str]:
        return [t for _, t, _ in self.got]


def watch(**kw: int) -> tuple[LivenessWatch, Pages]:
    pages = Pages()
    return LivenessWatch(Pager(pages.page, lambda t, b: None), LivenessSettings(**kw)), pages


def always(_a: str, _s: str) -> bool:
    return True


def never_held(_row: object) -> bool:
    return False


# ---------------------------------------------------------------------------------------------------- overdue
async def test_an_armed_position_not_decided_for_more_than_2_open_bars_is_overdue_and_paged_once() -> None:
    j = join_rig()
    try:
        await j.tracker.step()                               # arms
        await j.tracker.step()                               # reads the armed row
        rows = j.tracker.live_rows()
        (row,) = rows
        assert row.state == ArmState.ARMED.value and row.last_decided_bar is not None
        grid, cursor = FakeGrid(), row.last_decided_bar

        def cap(end: int) -> int:
            return decision_cap_ms(end, M15, COLD_SKEW)
        # the BarClock decides nothing (it never runs here): 2 bars past their cap are not yet overdue
        two = cap(cursor + 2 * TF) + 1
        assert undecided_open_bars(row, grid, two, always) == 2
        assert overdue_positions(rows, grid, two, schedule_known=always, held=never_held, paused=False) == {}
        three = cap(cursor + 3 * TF) + 1
        got = overdue_positions(rows, grid, three, schedule_known=always, held=never_held, paused=False)
        assert got == {row.ref: 3}
        w, pages = watch()
        w.positions(got)
        w.positions(got)                                             # once per episode
        assert pages.titles() == ["ARM: armed position not decided for 2 OPEN bars"]
        assert row.ref in pages.got[0][2] and "3 OPEN bars" in pages.got[0][2]
        w.positions({})                                              # decided again
        w.positions(got)                                             # a new episode pages again
        assert len(pages.got) == 2
        # paused, FAILED-held (a raising predicate counts the row), released, suspended: never overdue
        assert overdue_positions(rows, grid, three, schedule_known=always, held=never_held, paused=True) == {}
        assert overdue_positions(rows, grid, three, schedule_known=always, held=lambda r: True, paused=False) == {}

        def boom(_r: object) -> bool:
            raise RuntimeError("x")
        assert overdue_positions(rows, grid, three, schedule_known=always, held=boom, paused=False) == got
        for state in (ArmState.STRATEGY_COMPLETE, ArmState.SUSPENDED, ArmState.FAILED):
            other = (replace(row, state=state.value),)
            assert overdue_positions(other, grid, three, schedule_known=always, held=never_held,
                                     paused=False) == {}
    finally:
        j.barclock.close()


async def test_a_row_whose_symbol_has_no_schedule_is_overdue_never_zero() -> None:
    j = join_rig()
    try:
        await j.tracker.step()                               # arms
        await j.tracker.step()                               # reads the armed row
        rows = j.tracker.live_rows()
        (row,) = rows
        none = overdue_positions(rows, FakeGrid(), j.clock.now, schedule_known=lambda a, s: False,
                                 held=never_held, paused=False)
        assert none == {row.ref: NO_SCHEDULE}

        def boom(_a: str, _s: str) -> bool:
            raise RuntimeError("calendar")
        assert undecided_open_bars(row, FakeGrid(), j.clock.now, boom) == NO_SCHEDULE
        w, pages = watch()
        w.positions(none)
        assert "no loaded market schedule" in pages.got[0][2]
    finally:
        j.barclock.close()


async def test_decided_bars_keep_the_position_live() -> None:
    """The BarClock decides (here: spans the late bars at their caps): the cursor moves and nothing is overdue."""
    j = join_rig()
    try:
        await j.tracker.step()
        await j.tracker.step()
        (row,) = j.tracker.live_rows()
        assert row.last_decided_bar is not None
        later = decision_cap_ms(row.last_decided_bar + 4 * TF, M15, COLD_SKEW) + 1
        await j.at(later)                                    # undecided bars past their cap are spanned
        await j.tracker.step()
        assert overdue_positions(j.tracker.live_rows(), FakeGrid(), later, schedule_known=always,
                                 held=never_held, paused=False) == {}
        assert j.barclock.last_pass_ms == later and j.tracker.last_pass_ms is not None
    finally:
        j.barclock.close()


# ---------------------------------------------------------------------------------------------------- loops, relay
def test_a_stalled_barclock_or_tracker_pages_once_per_episode() -> None:
    w, pages = watch(barclock_stale_ms=60_000, tracker_stale_ms=120_000)
    t = 1_000_000
    w.loops(t, barclock_last_pass_ms=t, tracker_last_pass_ms=t)
    w.loops(t + 59_000, barclock_last_pass_ms=t, tracker_last_pass_ms=t)
    assert pages.got == []
    w.loops(t + 61_000, barclock_last_pass_ms=t, tracker_last_pass_ms=t)
    w.loops(t + 90_000, barclock_last_pass_ms=t, tracker_last_pass_ms=t)
    assert pages.titles() == ["ARM: decision loop stalled"]
    w.loops(t + 121_000, barclock_last_pass_ms=t, tracker_last_pass_ms=t)
    assert pages.titles() == ["ARM: decision loop stalled", "ARM: arm-tracker stalled"]
    w.loops(t + 122_000, barclock_last_pass_ms=t + 122_000, tracker_last_pass_ms=t + 122_000)   # recovered
    w.loops(t + 200_000, barclock_last_pass_ms=t + 122_000, tracker_last_pass_ms=t + 199_000)
    assert pages.titles()[-1] == "ARM: decision loop stalled" and len(pages.got) == 3
    # a loop that never completes a pass pages once that long after the first look; one not running never
    w2, pages2 = watch()
    w2.loops(t, barclock_last_pass_ms=None, tracker_last_pass_ms=None, barclock_running=False)
    w2.loops(t + 130_000, barclock_last_pass_ms=None, tracker_last_pass_ms=None, barclock_running=False)
    assert pages2.titles() == ["ARM: arm-tracker stalled"] and "no completed pass" in pages2.got[0][2]


def test_a_barclock_that_starts_after_a_long_hold_is_not_paged_before_it_could_pass() -> None:
    """Round 14 review: the BarClock not allowed to run for 120 s (a slow FAILED start, a global FAILED that clears),
    with no pass or an old one: the first look after it may run again gives no page; it pages only once it has had
    `barclock_stale_ms` of running time without a pass, and a hold again forgets that start."""
    t = 2_000_000
    for last in (None, t - 300_000):
        w, pages = watch(barclock_stale_ms=60_000)
        w.loops(t, barclock_last_pass_ms=last, tracker_last_pass_ms=t, barclock_running=False)
        w.loops(t + 120_000, barclock_last_pass_ms=last, tracker_last_pass_ms=t + 120_000, barclock_running=False)
        w.loops(t + 121_000, barclock_last_pass_ms=last, tracker_last_pass_ms=t + 120_000)     # now allowed to run
        assert pages.got == []
        w.loops(t + 180_000, barclock_last_pass_ms=last, tracker_last_pass_ms=t + 175_000)
        assert pages.got == []
        w.loops(t + 182_000, barclock_last_pass_ms=last, tracker_last_pass_ms=t + 175_000)    # 61 s running, no pass
        assert pages.titles() == ["ARM: decision loop stalled"]
        w.loops(t + 183_000, barclock_last_pass_ms=last, tracker_last_pass_ms=t + 175_000, barclock_running=False)
        w.loops(t + 400_000, barclock_last_pass_ms=last, tracker_last_pass_ms=t + 395_000)    # resumes again
        assert len(pages.got) == 1


def test_an_overdue_position_counts_only_once_confirmed_after_a_barclock_pass() -> None:
    """Round 14 review (restart after a long downtime): the tracker's rows still show the old cursor after the
    BarClock wrote the DOWNTIME span. A position first seen overdue counts only on tracker rows read after the watch
    saw a completed BarClock pass that started after the first sighting, or after `overdue_confirm_ms`."""
    w, _pages = watch(overdue_confirm_ms=120_000)
    t = 5_000_000
    raw = {"p1": 12}
    # first sighting: the rows were read before the restart's span; the BarClock's last pass started earlier
    assert w.confirm(raw, t, rows_read_ms=t - 4_000, barclock_last_pass_ms=t - 100) == {}
    # a pass that started after the sighting completed: seen now, but the rows were still read before that
    assert w.confirm(raw, t + 5_000, rows_read_ms=t - 4_000, barclock_last_pass_ms=t + 4_750) == {}
    assert w.confirm(raw, t + 9_000, rows_read_ms=t + 4_900, barclock_last_pass_ms=t + 8_750) == {}
    # rows read after the watch saw that pass no longer show it overdue: the episode ends with nothing counted
    assert w.confirm({}, t + 15_000, rows_read_ms=t + 14_000, barclock_last_pass_ms=t + 14_750) == {}
    # a position still overdue on rows read after the confirming pass counts
    assert w.confirm(raw, t + 20_000, rows_read_ms=t + 19_000, barclock_last_pass_ms=t + 19_750) == {}
    assert w.confirm(raw, t + 25_000, rows_read_ms=t + 24_000, barclock_last_pass_ms=t + 24_750) == {}
    assert w.confirm(raw, t + 30_000, rows_read_ms=t + 29_000, barclock_last_pass_ms=t + 29_750) == raw
    # no BarClock pass at all (or no rows read after one): counted after `overdue_confirm_ms`
    w2, _ = watch(overdue_confirm_ms=120_000)
    assert w2.confirm(raw, t, rows_read_ms=t, barclock_last_pass_ms=None) == {}
    assert w2.confirm(raw, t + 119_000, rows_read_ms=t + 118_000, barclock_last_pass_ms=None) == {}
    assert w2.confirm(raw, t + 120_000, rows_read_ms=t + 118_000, barclock_last_pass_ms=None) == raw


def test_the_relay_backlog_pages_once_per_episode() -> None:
    w, pages = watch(relay_backlog_ms=900_000)
    t = 10_000_000
    w.relay(t, None)
    w.relay(t, t - 899_000)
    assert pages.got == []
    w.relay(t, t - 901_000)
    w.relay(t + 5_000, t - 901_000)
    assert pages.titles() == ["ARM: notice relay backlog"] and "901 s" in pages.got[0][2]
    w.relay(t + 10_000, None)                                       # drained
    w.relay(t + 20_000, t - 2_000_000)
    assert len(pages.got) == 2


def test_settings_are_bounded() -> None:
    with pytest.raises(ValueError):
        LivenessSettings(overdue_open_bars=0)
    with pytest.raises(ValueError):
        LivenessSettings(relay_backlog_ms=0)
