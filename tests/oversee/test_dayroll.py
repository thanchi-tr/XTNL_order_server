"""17:00 America/New_York roll and the sod_marks row (oversee-rules.md §2, tier-persistence.md §4)."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from src.oversee.config import OverseeConfig
from src.oversee.dayroll import NEW_YORK, build_sod_mark, latest_roll_ms, trading_day
from src.oversee.snapshot import SymbolQuotes
from tests.oversee.helpers import AD, account, clean_mark, flow, position, snap, sod

D = Decimal


def ny(*args: int) -> int:
    return int(datetime(*args, tzinfo=NEW_YORK).timestamp() * 1000)


def utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def test_roll_is_dst_aware() -> None:
    assert utc(latest_roll_ms(ny(2026, 11, 11, 12))).hour == 22   # EST: 17:00 = 22:00 UTC
    assert utc(latest_roll_ms(ny(2026, 7, 15, 12))).hour == 21    # EDT: 17:00 = 21:00 UTC


def test_roll_boundary_and_trading_day_label() -> None:
    assert latest_roll_ms(ny(2026, 11, 11, 17)) == ny(2026, 11, 11, 17)
    assert latest_roll_ms(ny(2026, 11, 11, 16, 59)) == ny(2026, 11, 10, 17)
    assert trading_day(ny(2026, 11, 11, 17, 0, 1)) == date(2026, 11, 12)


def test_weekend_belongs_to_fridays_roll_and_sunday_is_not_a_roll() -> None:
    fri = ny(2026, 11, 13, 17)
    assert latest_roll_ms(ny(2026, 11, 14, 10)) == fri
    assert latest_roll_ms(ny(2026, 11, 15, 18)) == fri
    assert trading_day(ny(2026, 11, 15, 18)) == date(2026, 11, 16)


def test_sod_mark_from_pre_roll_marks_on_time() -> None:
    roll = ny(2026, 11, 11, 17)
    quotes = {(AD, "EURUSD"): SymbolQuotes(last_clean=clean_mark("1.1100", roll - 1000))}
    s = snap(now=roll + 5000, sod=None, quotes=quotes, positions=(position("P", "BUY", "100000", "1.1000"),))
    row = build_sod_mark(s, roll, roll + 5000, OverseeConfig())
    assert not row.partial_session
    assert row.sod_equity == D("101000.0000") and row.trading_day == date(2026, 11, 12)


def test_sod_mark_late_or_missing_input_is_partial() -> None:
    roll = ny(2026, 11, 11, 17)
    s = snap(now=roll + 31_000, sod=None, quotes={}, positions=(position("P"),))
    row = build_sod_mark(s, roll, roll + 31_000, OverseeConfig())
    assert row.partial_session and "late" in row.partial_reasons
    assert any(r.startswith("missing_mark") for r in row.partial_reasons)


def test_sod_mark_old_pre_roll_mark_is_partial() -> None:
    roll = ny(2026, 11, 11, 17)
    quotes = {(AD, "EURUSD"): SymbolQuotes(last_clean=clean_mark("1.1", roll - 301_000))}
    s = snap(now=roll + 1000, sod=None, quotes=quotes, positions=(position("P"),))
    assert build_sod_mark(s, roll, roll + 1000, OverseeConfig()).partial_session


def test_sod_mark_unexplained_increase_at_roll_and_post_roll_backout() -> None:
    roll = ny(2026, 11, 11, 17)
    prev = sod("100000", roll=ny(2026, 11, 10, 17))
    s = snap(now=roll + 1000, sod=None, account=account("111000"), prev_sod=prev,
             prev_day_cash_flows=(flow("pnl", "TRADE_PNL", "500", roll - 5000),),
             cash_flows=(flow("dep", "DEPOSIT", "1000", roll + 500),))
    row = build_sod_mark(s, roll, roll + 1000, OverseeConfig())
    assert row.balance == D(110000)                       # post-roll deposit backed out
    assert row.unexplained_increase_at_roll == D(9500)    # 10 000 move, 500 explained by P&L
    assert row.sod_equity == D(100500)
