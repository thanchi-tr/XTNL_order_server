"""Round 14 review (plan S38g): the liveness watch after a restart that follows a long downtime.

The single PC is off overnight while a 15m position is ARMED. On restart the arm-tracker's rows still carry the old
cursor (`last_decided_bar` hours back) until its next pass, while the new term's BarClock spans the missed bars
within about 250 ms. `OptionalRuntime.overdue_now` / `liveness_pass` / `arm_overdue_positions` (the real methods,
over the real tracker and BarClock of the riskmgr join rig) must neither page "armed position not decided" nor
report a dead-man count above 0 in that window; a position that really stays undecided still counts.

Synthetic ids and times only.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from src.marketdata.timing import COLD_SKEW, decision_cap_ms
from src.marketdata.types import M15
from src.riskmgr.liveness import OVERDUE_TITLE, LivenessWatch, overdue_positions
from src.riskmgr.ports import Pager
from src.riskmgr.sm11 import ArmState
from src.runtime.optional_plan import OptionalRuntime
from tests.riskmgr.test_arm_join import Join, join_rig
from tests.riskmgr.tracker_fakes import FakeGrid

TF = M15.timeframe_ms


class Grid(FakeGrid):
    """The SM10 grid with every symbol's schedule loaded (`CalendarGrid.has_schedule`)."""

    def has_schedule(self, adapter_id: str, symbol: str) -> bool:
        return True


class Pages:
    def __init__(self) -> None:
        self.got: list[tuple[str, str, str]] = []

    def page(self, severity: str, title: str, body: str) -> None:
        self.got.append((severity, title, body))

    def titles(self) -> list[str]:
        return [t for _, t, _ in self.got]


def runtime_over(j: Join, pages: Pages) -> Any:
    """A stand-in `OptionalRuntime` whose ARM graph holds the rig's tracker and (current) BarClock; the methods under
    test are the runtime's own."""
    rt: Any = SimpleNamespace(
        liveness=LivenessWatch(Pager(pages.page, lambda t, b: None)), liveness_errors=0,
        session=SimpleNamespace(ctx=SimpleNamespace(clock_ms=lambda: j.clock.now)),
        pause_reason=lambda: None, _may_decide=lambda _g: True)
    rt.graph = SimpleNamespace(
        tracker=j.tracker, clock=j.barclock, grid=Grid(), started=True,
        failed=SimpleNamespace(scopes=SimpleNamespace(covers_row=lambda _row, _series: False)),
        relay=SimpleNamespace(oldest_undelivered_ms=lambda: None))
    rt.overdue_now = lambda now: OptionalRuntime.overdue_now(rt, now)
    return rt


def liveness(rt: Any, now: int) -> int | None:
    """One `arm-status` round: the liveness pass, then the dead-man's read of the count."""
    OptionalRuntime.liveness_pass(rt, now)
    return OptionalRuntime.arm_overdue_positions(rt)


async def test_a_restart_after_a_long_downtime_never_pages_or_reports_the_stale_cursor() -> None:
    j = join_rig()
    try:
        await j.tracker.step()                                   # arms
        await j.tracker.step()                                   # reads the armed row
        (row,) = j.tracker.live_rows()
        assert row.state == ArmState.ARMED.value and row.last_decided_bar is not None
        cursor = row.last_decided_bar
        # the PC was off for 3 h: the restart's first tracker pass still holds the row with the old cursor
        restart = decision_cap_ms(cursor + 12 * TF, M15, COLD_SKEW) + 1
        j.clock.now = restart
        j.new_term()                                             # a new term's BarClock, no pass yet
        pages = Pages()
        rt = runtime_over(j, pages)
        stale = overdue_positions(j.tracker.live_rows(), Grid(), restart, schedule_known=lambda a, s: True,
                                  held=lambda r: False, paused=False)
        assert stale == {row.ref: 12}                            # what the stale rows alone would report
        assert liveness(rt, restart) == 0
        # the BarClock is allowed to decide: its first pass spans the missed bars (the DOWNTIME catch-up)
        await j.at(restart + 250)
        rt.graph.clock = j.barclock
        assert j.store.rows[row.key].last_decided_bar is not None
        assert j.store.rows[row.key].last_decided_bar > cursor
        # the arm-status loop lands in the window before the tracker's next pass (rows still stale): nothing counts
        for dt in (5_000, 9_000):
            j.clock.now = restart + dt
            await j.barclock.step()
            assert liveness(rt, restart + dt) == 0
        # the tracker's next pass reads the spanned cursor: still nothing, then and later
        j.clock.now = restart + 10_000
        await j.tracker.step()
        for dt in (10_000, 15_000, 200_000):
            assert liveness(rt, restart + dt) == 0
        assert OVERDUE_TITLE not in pages.titles() and rt.liveness_errors == 0
    finally:
        j.barclock.close()


async def test_a_position_that_stays_undecided_after_the_restart_still_counts_and_pages() -> None:
    """The confirmation only waits for evidence: when the BarClock completes passes that decide nothing for the
    position (here: it never sees the row, its store answers no rows), tracker rows read after such a pass still
    show it overdue, so it counts and pages once."""
    j = join_rig()
    try:
        await j.tracker.step()
        await j.tracker.step()
        (row,) = j.tracker.live_rows()
        assert row.last_decided_bar is not None
        restart = decision_cap_ms(row.last_decided_bar + 12 * TF, M15, COLD_SKEW) + 1
        pages = Pages()
        rt = runtime_over(j, pages)
        rt.graph.clock = SimpleNamespace(last_pass_ms=None)     # a BarClock that passes without deciding the row
        j.clock.now = restart
        assert liveness(rt, restart) == 0
        rt.graph.clock.last_pass_ms = restart + 4_750
        j.clock.now = restart + 5_000
        assert liveness(rt, restart + 5_000) == 0                # the pass is seen; the rows were read before it
        j.clock.now = restart + 6_000
        await j.tracker.step()                                   # rows read after the pass: still overdue
        rt.graph.clock.last_pass_ms = restart + 9_750
        j.clock.now = restart + 10_000
        assert liveness(rt, restart + 10_000) == 1
        assert pages.titles().count(OVERDUE_TITLE) == 1
    finally:
        j.barclock.close()
