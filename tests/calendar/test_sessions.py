"""Pure schedule arithmetic: phases, DST-safe opens / closes, holidays (operations.md §10, §14)."""
from __future__ import annotations

import pytest

from src.calendar.schedule import ScheduleError, SymbolSchedule, symbol_schedule_from_json
from src.calendar.sessions import (
    Phase,
    last_close_ms,
    last_open_ms,
    next_open_ms,
    opens_between,
    phase_at,
)
from tests.calendar.helpers import FX_SCHEDULE, fx, ny, utc


def test_phases_of_a_normal_week() -> None:
    s = fx()
    assert phase_at(s, ny(2026, 11, 11, 12)) is Phase.OPEN          # Wednesday midday
    assert phase_at(s, ny(2026, 11, 11, 17, 2)) is Phase.ROLLOVER   # daily break
    assert phase_at(s, ny(2026, 11, 11, 17, 5)) is Phase.OPEN       # break end is exclusive
    assert phase_at(s, ny(2026, 11, 13, 16, 59)) is Phase.OPEN      # Friday before close
    assert phase_at(s, ny(2026, 11, 13, 17, 0)) is Phase.CLOSED     # weekly close
    assert phase_at(s, ny(2026, 11, 14, 12)) is Phase.CLOSED        # Saturday
    assert phase_at(s, ny(2026, 11, 15, 16, 59)) is Phase.CLOSED    # Sunday before open
    assert phase_at(s, ny(2026, 11, 15, 17, 0)) is Phase.OPEN       # Sunday open


def test_sunday_open_is_17_new_york_across_spring_forward() -> None:
    s = fx()
    # 2026-03-01 is EST (UTC-5); 2026-03-08 the clocks go forward at 02:00 (EDT, UTC-4)
    assert next_open_ms(s, ny(2026, 2, 28, 12)) == utc(2026, 3, 1, 22)
    assert next_open_ms(s, ny(2026, 3, 7, 12)) == utc(2026, 3, 8, 21)
    assert next_open_ms(s, ny(2026, 3, 7, 12)) == ny(2026, 3, 8, 17)


def test_sunday_open_across_fall_back() -> None:
    s = fx()
    # 2026-11-01: clocks go back at 02:00 EDT -> 01:00 EST; the open is 17:00 EST = 22:00 UTC
    assert next_open_ms(s, ny(2026, 10, 31, 12)) == utc(2026, 11, 1, 22)
    assert last_close_ms(s, ny(2026, 11, 1, 3)) == utc(2026, 10, 30, 21)   # Friday 17:00 EDT


def test_session_in_the_dst_gap_still_found() -> None:
    sched = {"sessions": [{"open": {"day": "SUN", "time": "02:30"}, "close": {"day": "FRI", "time": "17:00"}}]}
    s = symbol_schedule_from_json("a", "X", "America/New_York", sched)
    nxt = next_open_ms(s, ny(2026, 3, 7, 12))
    assert nxt is not None
    assert phase_at(s, nxt) is Phase.OPEN and phase_at(s, nxt - 1) is Phase.CLOSED


def test_next_open_after_break_and_last_open() -> None:
    s = fx()
    assert next_open_ms(s, ny(2026, 11, 11, 17, 1)) == ny(2026, 11, 11, 17, 5)
    assert last_open_ms(s, ny(2026, 11, 11, 17, 10)) == ny(2026, 11, 11, 17, 5)
    assert last_open_ms(s, ny(2026, 11, 14, 12)) == ny(2026, 11, 12, 17, 5)


def test_holidays_and_early_close() -> None:
    s = fx(holidays=[{"date": "2026-12-25", "reason": "Christmas"},
                     {"start": "2026-12-24T13:00", "end": "2026-12-25T00:00", "reason": "early close"}])
    assert phase_at(s, ny(2026, 12, 24, 12, 59)) is Phase.OPEN
    assert phase_at(s, ny(2026, 12, 24, 13, 0)) is Phase.CLOSED
    assert phase_at(s, ny(2026, 12, 25, 10)) is Phase.CLOSED
    assert last_close_ms(s, ny(2026, 12, 25, 10)) == ny(2026, 12, 24, 13)
    # Friday is a holiday and the weekly close follows: next open is Sunday 17:00
    assert next_open_ms(s, ny(2026, 12, 25, 10)) == ny(2026, 12, 27, 17)


def test_opens_between() -> None:
    s = fx()
    assert opens_between(s, ny(2026, 11, 14, 10), ny(2026, 11, 15, 18))
    assert not opens_between(s, ny(2026, 11, 14, 10), ny(2026, 11, 15, 16))
    assert not opens_between(s, ny(2026, 11, 15, 18), ny(2026, 11, 15, 10))


def test_week_wrapping_and_24x7_sessions() -> None:
    s = symbol_schedule_from_json("a", "BTC", "UTC", {"sessions": [
        {"open": {"day": "MON", "time": "00:00"}, "close": {"day": "MON", "time": "00:00"}}]})
    assert phase_at(s, utc(2026, 11, 14, 3)) is Phase.OPEN
    assert next_open_ms(s, utc(2026, 11, 14, 3)) is None


def test_break_crossing_midnight() -> None:
    sched = dict(FX_SCHEDULE)
    sched["breaks"] = [{"start": "23:55", "end": "00:10", "days": ["TUE"]}]
    s = symbol_schedule_from_json("a", "X", "America/New_York", sched)
    assert phase_at(s, ny(2026, 11, 10, 23, 58)) is Phase.ROLLOVER   # Tuesday
    assert phase_at(s, ny(2026, 11, 11, 0, 5)) is Phase.ROLLOVER     # after midnight (Wednesday)
    assert phase_at(s, ny(2026, 11, 12, 0, 5)) is Phase.OPEN         # Thursday: no break
    assert next_open_ms(s, ny(2026, 11, 10, 23, 58)) == ny(2026, 11, 11, 0, 10)


@pytest.mark.parametrize("schedule,holidays,tz", [
    ({"sessions": []}, [], "America/New_York"),
    ({"sessions": [{"open": {"day": "XYZ", "time": "17:00"}, "close": {"day": "FRI", "time": "17:00"}}]}, [],
     "America/New_York"),
    (FX_SCHEDULE, [{"start": "2026-12-24T13:00", "end": "2026-12-24T12:00"}], "America/New_York"),
    (FX_SCHEDULE, [{"start": "2026-12-24T13:00+00:00", "end": "2026-12-24T15:00+00:00"}], "America/New_York"),
    (FX_SCHEDULE, [], "Mars/Olympus"),
])
def test_bad_schedules_rejected(schedule: dict[str, object], holidays: list[dict[str, str]], tz: str) -> None:
    with pytest.raises(ScheduleError):
        symbol_schedule_from_json("a", "X", tz, schedule, holidays)


# ---- 4.9: a 24/7 session reports a start time (the gate must not deny it forever) ------------------------
def _always_open(holidays: list[dict[str, str]] | None = None) -> SymbolSchedule:
    return symbol_schedule_from_json("a", "BTC", "UTC", {"sessions": [
        {"open": {"day": "MON", "time": "00:00"}, "close": {"day": "MON", "time": "00:00"}}]}, holidays or [])


def test_24x7_session_reports_a_start_time_so_last_open_is_never_none() -> None:
    from src.calendar.sessions import ALWAYS_OPEN_SINCE_MS

    s = _always_open()
    for now in (utc(2026, 11, 14, 3), utc(2027, 3, 1, 12), utc(2027, 9, 30, 23, 59)):
        assert last_open_ms(s, now) == ALWAYS_OPEN_SINCE_MS            # stable, never None, never moves forward
    assert ALWAYS_OPEN_SINCE_MS <= utc(2026, 11, 14, 3)


def test_24x7_session_with_a_holiday_reports_the_reopen() -> None:
    s = _always_open([{"date": "2026-12-25"}])
    assert phase_at(s, utc(2026, 12, 25, 12)) is Phase.CLOSED
    assert last_open_ms(s, utc(2026, 12, 25, 12)) is None                            # closed: no open to report
    assert last_open_ms(s, utc(2026, 12, 27, 12)) == utc(2026, 12, 26)                # inside the horizon: the edge
    assert last_open_ms(s, utc(2027, 2, 1, 12)) == utc(2026, 12, 26)                  # long after: still that reopen


def test_closed_symbol_without_an_open_in_the_horizon_is_still_unknown() -> None:
    s = fx(holidays=[{"start": "2026-11-01T00:00", "end": "2026-12-31T00:00", "reason": "long closure"}])
    assert phase_at(s, ny(2026, 12, 1, 12)) is Phase.CLOSED
    assert last_open_ms(s, ny(2026, 12, 1, 12)) is None
