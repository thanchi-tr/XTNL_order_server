"""oversee-rules.md §5 shared risk definitions."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.dto import OrderStatus
from src.oversee.risk import (
    MarkBook,
    MarkQuality,
    RiskUnknown,
    effective_stop,
    equity_mark,
    needs_stop,
    net_transfers_today,
    opening_units,
    reducing_units,
    remaining_unexplained_at_roll,
    risk,
    select_mark,
    unexplained_balance_delta,
)
from src.oversee.snapshot import SymbolQuotes, TimedMid
from tests.oversee.helpers import AD, ROLL, T0, account, clean_mark, flow, order, position, quote, sod, spec

D = Decimal
KEY = (AD, "EURUSD")


def book(quotes: dict | None = None, instruments: dict | None = None, now: int = T0) -> MarkBook:
    return MarkBook(quotes if quotes is not None else {KEY: quote("1.1000", now)},
                    instruments if instruments is not None else {KEY: spec()}, now, 5000)


# ---------------------------------------------------------------- risk()

def test_risk_buy_directional() -> None:
    p = position("P", "BUY", "100000", "1.1000", stop="1.0900")
    fig = risk(p, D(100000), book(), "USD", [p], [])
    assert fig.value == D("1000.0000") and not fig.no_stop and not fig.stale


def test_risk_sell_directional() -> None:
    p = position("P", "SELL", "50000", "1.1000", stop="1.1100")
    assert risk(p, D(50000), book(), "USD", [p], []).value == D("500.0000")


@pytest.mark.parametrize("stop", ["1.1000", "1.1050"])
def test_stop_at_or_past_breakeven_counts_zero(stop: str) -> None:
    p = position("P", "BUY", stop=stop)
    assert risk(p, D(100000), book(), "USD", [p], []).value == 0


def test_working_order_stop_on_wrong_side_is_unknown() -> None:
    o = order("O", "BUY", OrderStatus.WORKING, entry="1.1000", stop="1.1100")
    with pytest.raises(RiskUnknown) as exc:
        risk(o, D(100000), book(), "USD", [], [o])
    assert exc.value.reason == "risk_unknown"


def test_missing_instrument_spec_is_unknown() -> None:
    p = position("P", stop="1.09")
    with pytest.raises(RiskUnknown, match="instrument_unknown"):
        risk(p, D(1), book(instruments={}), "USD", [p], [])


def test_no_mid_ever_is_mark_unknown() -> None:
    p = position("P", stop="1.09")
    with pytest.raises(RiskUnknown) as exc:
        risk(p, D(1), book(quotes={}), "USD", [p], [])
    assert exc.value.reason == "mark_unknown"


def test_no_stop_is_flagged_not_risked() -> None:
    p = position("P")
    fig = risk(p, D(100000), book(), "USD", [p], [])
    assert fig.no_stop and fig.value == 0


def test_fx_conversion_via_cross() -> None:
    """USDJPY position in a GBP account: JPY -> USD -> GBP at clean mids."""
    jpy, gbp = (AD, "USDJPY"), (AD, "GBPUSD")
    b = MarkBook({jpy: quote("150", T0, symbol="USDJPY"), gbp: quote("1.25", T0, symbol="GBPUSD")},
                 {jpy: spec("USDJPY", "USD", "JPY"), gbp: spec("GBPUSD", "GBP", "USD")}, T0, 5000)
    p = position("P", "BUY", "10000", "150", stop="149", symbol="USDJPY")
    fig = risk(p, D(10000), b, "GBP", [p], [])
    assert fig.value.quantize(D("0.0001")) == (D(10000) / D(150) / D("1.25")).quantize(D("0.0001"))


def test_fx_conversion_inverse_and_missing_path() -> None:
    b = book()
    assert b.fx(AD, "USD", "EUR").value == 1 / D("1.1000")
    with pytest.raises(RiskUnknown):
        b.fx(AD, "USD", "CHF")


# ---------------------------------------------------------------- units and stops

def test_reducing_units_zero_unless_reduce_only() -> None:
    o = order("O", "SELL", linked="P")
    assert reducing_units(o, [position("P")], [o]) == 0


def test_reducing_units_closing_trade_with_excess_opening() -> None:
    p = position("P", "BUY", "60000")
    o = order("C", "SELL", ordered="100000", reduce_only=True, linked="P", entry="1.0900")
    assert reducing_units(o, [p], [o]) == D(60000)
    assert opening_units(o, [p], [o]) == D(40000)
    assert needs_stop(o, [p], [o])


def test_stonex_partial_fill_parent_plus_child() -> None:
    """Parent BUY 10 000, 6 000 filled (position shares the id), stop child covers all 10 000."""
    parent = order("P1", "BUY", OrderStatus.PARTIALLY_FILLED, ordered="10000", remaining="4000")
    pos = position("P1", "BUY", "6000")
    child = order("C1", "SELL", OrderStatus.WORKING, ordered="10000", entry="1.0900", reduce_only=True,
                  linked="P1")
    orders, positions = [parent, child], [pos]
    assert reducing_units(child, positions, orders) == D(10000)
    assert opening_units(child, positions, orders) == 0
    assert not needs_stop(child, positions, orders)
    assert opening_units(parent, positions, orders) == D(4000)
    assert effective_stop(pos, positions, orders) == D("1.0900")
    assert effective_stop(parent, positions, orders) == D("1.0900")


def test_child_that_does_not_cover_units_is_no_stop() -> None:
    parent = order("P1", "BUY", OrderStatus.PARTIALLY_FILLED, ordered="10000", remaining="4000")
    pos = position("P1", "BUY", "6000")
    child = order("C1", "SELL", ordered="8000", entry="1.0900", reduce_only=True, linked="P1")
    assert effective_stop(pos, [pos], [parent, child]) is None


def test_take_profit_child_is_not_a_stop() -> None:
    pos = position("P", "BUY", "1000")
    tp = order("T", "SELL", ordered="1000", entry="1.1200", reduce_only=True, linked="P")
    assert effective_stop(pos, [pos], [tp]) is None


def test_terminal_order_has_no_opening_units_and_needs_no_stop() -> None:
    o = order("O", status=OrderStatus.FILLED, remaining="0")
    assert opening_units(o, [], [o]) == 0 and not needs_stop(o, [], [o])


def test_closed_position_needs_no_stop() -> None:
    p = position("P", status="CLOSED")
    assert not needs_stop(p, [p], []) and needs_stop(position("Q"), [], [])


# ---------------------------------------------------------------- marks

def test_clean_mark_when_fresh() -> None:
    m = select_mark(KEY, quote("1.1", T0 - 5000), T0, 5000)
    assert m.quality is MarkQuality.CLEAN and not m.stale


def test_ticking_without_clean_quote_marks_liquidation_side() -> None:
    q = SymbolQuotes(latest=quote("1.1", T0).latest, clean_mid=TimedMid(D("1.09"), T0 - 6000))
    long_mark = select_mark(KEY, q, T0, 5000, "BUY")
    short_mark = select_mark(KEY, q, T0, 5000, "SELL")
    assert long_mark.quality is MarkQuality.LIQUIDATION and long_mark.price == D("1.09995")
    assert short_mark.price == D("1.10005") and long_mark.stale


def test_no_quote_at_all_uses_last_mid_stale() -> None:
    q = SymbolQuotes(latest=quote("1.1", T0 - 9000).latest, clean_mid=TimedMid(D("1.09"), T0 - 7000))
    m = select_mark(KEY, q, T0, 5000)
    assert m.quality is MarkQuality.LAST_MID and m.price == D("1.09") and m.stale


def test_closed_session_uses_last_clean_mid() -> None:
    q = SymbolQuotes(latest=quote("1.2", T0).latest, clean_mid=TimedMid(D("1.1"), T0 - 60_000), session="CLOSED")
    m = select_mark(KEY, q, T0, 5000)
    assert m.quality is MarkQuality.CLOSED_LAST_CLEAN and m.price == D("1.1") and m.stale


def test_redis_down_falls_back_to_last_clean_marks_stale() -> None:
    q = SymbolQuotes(last_clean=clean_mark("1.1", T0 - 100))
    m = select_mark(KEY, q, T0, 5000)
    assert m.stale and m.price == D("1.1")


def test_never_seen_is_unknown() -> None:
    with pytest.raises(RiskUnknown):
        select_mark(KEY, SymbolQuotes(), T0, 5000)


def test_stale_mark_propagates_to_risk() -> None:
    q = SymbolQuotes(latest=quote("1.1", T0 - 9000).latest)
    p = position("P", stop="1.09")
    fig = risk(p, D(100000), book({KEY: q}), "USD", [p], [])
    assert fig.stale == frozenset({KEY})


# ---------------------------------------------------------------- equity and cash ledger

def test_equity_mark_balance_plus_open_pnl() -> None:
    p = position("P", "BUY", "100000", "1.0900")
    eq = equity_mark(D(100000), "USD", [p], book(), [], None)
    assert eq.value == D("101000.0000") and eq.unexplained_delta is None


def test_unbooked_swap_and_commission_are_added() -> None:
    b = book(instruments={KEY: spec(swap_booked=False)})
    p = position("P", "BUY", "100000", "1.1000", accrued_swap=D("-12"), commission=D("-3"))
    assert equity_mark(D(100000), "USD", [p], b, [], None).value == D("99985.0000")


def test_unexplained_delta_counts_every_ledger_kind() -> None:
    flows = [flow("d", "DEPOSIT", "1000", ROLL + 1), flow("t", "TRADE_PNL", "-400", ROLL + 2),
             flow("old", "DEPOSIT", "50", ROLL - 1)]
    delta = unexplained_balance_delta(D(100600), D(100000), flows, ROLL, book(), "USD")
    assert delta.value == 0


def test_unexplained_increase_is_excluded_from_equity() -> None:
    eq = equity_mark(D(105000), "USD", [], book(), [], sod("100000"))
    assert eq.value == D(100000) and eq.excluded_increase == D(5000)


def test_unexplained_decrease_counts_as_loss() -> None:
    eq = equity_mark(D(95000), "USD", [], book(), [], sod("100000"))
    assert eq.value == D(95000) and eq.unexplained_delta == D(-5000)


def test_net_transfers_today_late_pre_roll_deposit_up_to_uiar() -> None:
    s = sod("110000", "100000", uiar="10000", written=ROLL + 1000)
    late = flow("late", "DEPOSIT", "12000", ROLL - 60_000, committed=ROLL + 5000)
    today = flow("w", "WITHDRAWAL", "500", ROLL + 10)
    pre_w = flow("pw", "WITHDRAWAL", "300", ROLL - 10, committed=ROLL + 5000)
    nt = net_transfers_today([late, today, pre_w], s, book(), "USD")
    assert nt.value == D(10000) - D(500)
    assert remaining_unexplained_at_roll([late], s, book(), "USD") == 0
    assert remaining_unexplained_at_roll([], s, book(), "USD") == D(10000)


def test_account_helper_is_usd() -> None:
    assert account().currency == "USD"
