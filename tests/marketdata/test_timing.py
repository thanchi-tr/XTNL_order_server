"""Per-bar-spec timing table and the one cap clock (plan S12; design §A9, §B5.2)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.marketdata import timing
from src.marketdata.timing import (
    COLD_SKEW,
    TIMING_TABLE,
    SkewEstimate,
    decision_cap_ms,
    first_read_ms,
    last_roll_ms,
    next_read_ms,
    overlaps_roll,
    post_grace_end_ms,
    pre_fire_ms,
    pre_offset_problem,
    pre_valid_until_ms,
    rollback_n_problem,
    settle_ms,
    timing_for,
    timing_problem,
    verification_read_ms,
)
from src.marketdata.types import H1, M1, M5, M15
from src.pubsub import tradingday

T = 1_700_000_100_000   # a 15m bar end
ROW = TIMING_TABLE[M15]


def test_v1_has_only_the_15m_row() -> None:
    assert list(TIMING_TABLE) == [M15]
    assert timing_for(M15) is ROW and timing_problem(M15) is None
    for spec in (M1, M5, H1):
        assert timing_for(spec) is None
        problem = timing_problem(spec)
        assert problem is not None and spec.name in problem
    assert ROW.cross_check_tick_spec == M15


def test_decision_cap_is_local_t_plus_timeframe_plus_p50_plus_settle() -> None:
    assert decision_cap_ms(T, M15, COLD_SKEW) == T + 900_000 + 0 + 5_000
    assert decision_cap_ms(T, M15, SkewEstimate(p50_ms=800, settle_ms=1_200)) == T + 900_000 + 800 + 1_200


def test_decision_cap_with_a_negative_p50() -> None:
    # The broker clock ahead of the local clock: received − broker is negative, so the cap moves earlier.
    skew = SkewEstimate(p50_ms=-1_500, settle_ms=400)
    assert decision_cap_ms(T, M15, skew) == T + 900_000 - 1_500 + 400
    assert first_read_ms(T, skew) == T - 1_100
    assert pre_fire_ms(T, skew, 40) == T - 1_500 - 40_000


@pytest.mark.parametrize(("p50", "p99", "resolution", "expected"), [
    (0, 0, 0, 250), (100, 200, 0, 350), (0, 10_000, 0, 5_000), (0, 10_000, 1_000, 6_000), (500, 100, 0, 250),
])
def test_settle_is_clamped(p50: int, p99: int, resolution: int, expected: int) -> None:
    assert settle_ms(p50, p99, resolution) == expected


def test_reread_schedule_steps_and_stops_at_the_cap() -> None:
    skew = SkewEstimate(p50_ms=0, settle_ms=1_000)
    cap = decision_cap_ms(T, M15, skew)
    first = first_read_ms(T, skew)
    assert first == T + 1_000
    assert next_read_ms(T, first, ROW, skew) == first + 5_000              # every 5 s until T + post_grace
    assert next_read_ms(T, T + 59_000, ROW, skew) == T + 64_000
    assert next_read_ms(T, T + 60_000, ROW, skew) == T + 90_000           # every 30 s until T + 5 min
    assert next_read_ms(T, T + 299_000, ROW, skew) == T + 329_000
    assert next_read_ms(T, T + 300_000, ROW, skew) == T + 360_000         # every 60 s until the cap
    assert next_read_ms(T, cap - 60_000, ROW, skew) is None               # at the cap live re-reading stops
    assert next_read_ms(T, cap - 60_001, ROW, skew) == cap - 1


def test_broker_bar_lag_starts_the_back_off() -> None:
    skew = SkewEstimate(p50_ms=0, settle_ms=1_000)
    # At or below post_grace the lag changes nothing (the default 5 s lag).
    assert next_read_ms(T, T + 60_000, ROW, skew, lag_ms=5_000) == T + 90_000
    # A longer PR3 lag keeps the 5 s cadence until T + lag, then backs off as usual.
    assert next_read_ms(T, T + 60_000, ROW, skew, lag_ms=120_000) == T + 65_000
    assert next_read_ms(T, T + 119_000, ROW, skew, lag_ms=120_000) == T + 124_000
    assert next_read_ms(T, T + 120_000, ROW, skew, lag_ms=120_000) == T + 150_000
    assert next_read_ms(T, T + 300_000, ROW, skew, lag_ms=300_000) == T + 360_000   # the largest allowed lag


def test_reread_schedule_counts_about_thirty_reads() -> None:
    skew = SkewEstimate(p50_ms=200, settle_ms=800)
    reads = [first_read_ms(T, skew)]
    while (nxt := next_read_ms(T, reads[-1], ROW, skew)) is not None:
        reads.append(nxt)
    assert 25 <= len(reads) <= 35
    assert reads[-1] < decision_cap_ms(T, M15, skew)


def test_reread_schedule_uses_the_local_bar_end() -> None:
    late, early = SkewEstimate(p50_ms=2_000, settle_ms=500), SkewEstimate(p50_ms=-2_000, settle_ms=500)
    # 61 s after the broker bar end is still inside post_grace on the local clock when p50 = +2 s.
    assert next_read_ms(T, T + 61_000, ROW, late) == T + 66_000
    assert next_read_ms(T, T + 61_000, ROW, early) == T + 91_000


def test_verification_read() -> None:
    cap = decision_cap_ms(T, M15, COLD_SKEW)
    assert verification_read_ms(T + 10_000, cap, ROW) == T + 70_000
    assert verification_read_ms(cap - 50_000, cap, ROW) == cap - 30_000
    assert verification_read_ms(cap - 30_000, cap, ROW) is None    # no time remains: unverified_final
    assert verification_read_ms(cap - 10_000, cap, ROW) is None


@pytest.mark.parametrize("offset", [30, 31, 40, 45])
def test_pre_window(offset: int) -> None:
    """S38c (round 12): valid until fire + 10 s, a 10 s window at every allowed offset, ending before the bar end."""
    skew = SkewEstimate(p50_ms=300, settle_ms=700)
    fire = pre_fire_ms(T, skew, offset)
    assert fire == T + 300 - offset * 1000
    assert pre_valid_until_ms(fire, T, ROW, skew) == fire + 10_000
    assert pre_valid_until_ms(fire, T, ROW, skew) <= T + 300 - 20_000
    assert pre_offset_problem(M15, offset) is None


def test_the_pre_window_is_ten_seconds_at_both_ends_of_the_range() -> None:
    """D1 allows 30-45 s: the window no longer shrinks to nothing at 30 s (it did under min(fire + 10 s, T − 30 s))."""
    for skew in (COLD_SKEW, SkewEstimate(p50_ms=-8_000, settle_ms=700), SkewEstimate(p50_ms=4_000, settle_ms=700)):
        for offset in (ROW.pre_offset_min_s, ROW.pre_offset_max_s):
            fire = pre_fire_ms(T, skew, offset)
            assert pre_valid_until_ms(fire, T, ROW, skew) - fire == 10_000
    assert (ROW.pre_offset_min_s, ROW.pre_offset_max_s) == (30, 45)


def test_every_allowed_pre_offset_leaves_a_validity_window() -> None:
    skew = SkewEstimate(p50_ms=-400, settle_ms=700)
    allowed = [o for o in range(0, 120) if pre_offset_problem(M15, o) is None]
    assert allowed == list(range(ROW.pre_offset_min_s, ROW.pre_offset_max_s + 1))
    for offset in allowed:
        fire = pre_fire_ms(T, skew, offset)
        assert pre_valid_until_ms(fire, T, ROW, skew) > fire


@pytest.mark.parametrize("offset", [29, 10, 46, 0])
def test_pre_offset_bounds(offset: int) -> None:
    assert pre_offset_problem(M15, offset) is not None
    assert pre_offset_problem(M1, 40) is not None


def test_post_grace_and_rollback_bounds() -> None:
    assert post_grace_end_ms(T, ROW, SkewEstimate(p50_ms=-100, settle_ms=250)) == T - 100 + 60_000
    assert rollback_n_problem(M15, 2) is None and rollback_n_problem(M15, 1) is not None
    assert rollback_n_problem(M5, 8) is not None
    assert ROW.rollback_n_default == 8 and ROW.unsupported_lag_ms == 300_000


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def test_roll_copy_equals_the_pubsub_helper_across_dst_and_weekends() -> None:
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    for hours in range(0, 24 * 300, 7):
        now = _ms(start + timedelta(hours=hours, minutes=13))
        assert last_roll_ms(now) == tradingday.last_roll_ms(now)
    assert timing.ROLL_TIME == tradingday.ROLL_TIME


def test_roll_copy_equals_the_pubsub_helper_at_the_roll_instant() -> None:
    # Every day (weekdays, Friday, the weekend and both 2026 DST changes): 1 ms before, at and after 17:00 New York.
    day = datetime(2026, 3, 1, tzinfo=timing.NEW_YORK).date()
    for _ in range(300):
        roll = _ms(datetime.combine(day, timing.ROLL_TIME, tzinfo=timing.NEW_YORK))
        for now in (roll - 1, roll, roll + 1):
            assert last_roll_ms(now) == tradingday.last_roll_ms(now), (day, now - roll)
        day += timedelta(days=1)


def test_overlaps_roll() -> None:
    roll = last_roll_ms(_ms(datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc)))   # Wed 17:00 New York (EDT)
    assert roll == _ms(datetime(2026, 7, 15, 21, 0, tzinfo=timezone.utc))
    assert overlaps_roll(roll, roll + 900_000)                     # 17:00-17:15 holds the roll
    assert overlaps_roll(roll - 900_000 + 1, roll + 1)
    assert not overlaps_roll(roll - 900_000, roll)                 # 16:45-17:00 ends at the roll
    assert not overlaps_roll(roll + 900_000, roll + 1_800_000)
    assert not overlaps_roll(roll, roll)
