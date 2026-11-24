"""15m bar grid, pre-close bars and the 17:00 New York roll across both 2026 US DST changes (real CalendarGrid over
the SM10 FX schedule; bar ends stay on the UTC 15m grid, the close / open / break follow New York local time)."""
from __future__ import annotations

import pytest

from src.marketdata.timing import COLD_SKEW, overlaps_roll
from src.marketdata.types import M15
from src.riskmgr.guards.first_bar import bar0_bucket_ms
from src.riskmgr.replay import first_open_bar_end_ms
from src.runtime.arm_ports import CalendarGrid
from tests.calendar.helpers import fx, ny
from tests.riskmgr.tracker_fakes import ADAPTER

TF = M15.timeframe_ms


def grid() -> CalendarGrid:
    sched = fx(ADAPTER, "SYN.A")
    return CalendarGrid(schedule_of=lambda a, s: sched, bars_capable=lambda a: True)


@pytest.mark.parametrize("fri,sun,mon", [
    ((2026, 3, 6), (2026, 3, 8), (2026, 3, 9)),     # Friday EST close, Sunday EDT open (spring forward)
    ((2026, 10, 30), (2026, 11, 1), (2026, 11, 2)),  # Friday EDT close, Sunday EST open (fall back)
])
def test_dst_weekend_grid(fri: tuple[int, int, int], sun: tuple[int, int, int], mon: tuple[int, int, int]) -> None:
    g = grid()
    close = ny(*fri, 17, 0)
    open_ = ny(*sun, 17, 0)
    assert close % TF == 0 and open_ % TF == 0
    ends = g.open_bar_ends(ADAPTER, "SYN.A", M15, close - 2 * TF, open_ + 2 * TF)
    assert ends == [close - TF, close, open_ + TF, open_ + 2 * TF]       # nothing in the weekend
    assert g.next_open_ms(ADAPTER, "SYN.A", close) == open_             # the pre-close bar names the reopen
    assert g.next_open_ms(ADAPTER, "SYN.A", close - TF) is None         # an ordinary bar before it
    assert first_open_bar_end_ms(open_, M15, COLD_SKEW) == open_ + TF
    brk = ny(*mon, 17, 0)
    assert g.next_open_ms(ADAPTER, "SYN.A", brk) == ny(*mon, 17, 5)
    # the roll caveat sits on the 17:00-17:15 New York bucket in both offsets, never on 16:45-17:00
    assert overlaps_roll(brk, brk + TF) and not overlaps_roll(brk - TF, brk)
    # a fill exactly at 17:00 New York (the bar end) belongs to the 17:00 bucket (Q7)
    assert bar0_bucket_ms("fill_bucket", brk) == brk
    assert bar0_bucket_ms("fill_bucket", brk - 1) == brk - TF
