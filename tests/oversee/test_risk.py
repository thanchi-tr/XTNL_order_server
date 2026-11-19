"""oversee-rules.md §5 shared risk definitions."""
from __future__ import annotations

import random
from decimal import Decimal

import pytest

from src.dto import OrderDTO, OrderKind, OrderStatus, PositionDTO, Side
from src.oversee.risk import (
    STOP_CAVEATS,
    STOP_KIND_INCONSISTENT,
    STOP_KIND_UNKNOWN,
    Mark,
    MarkBook,
    MarkQuality,
    RiskUnknown,
    StopHandle,
    effective_stop,
    effective_stop_detail,
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

# S38f: the S07 / PR1 stop rules below run only with XTNL_OVERSEE_STOP_AT_ENTRY=1; the pre-S07 tests here hold under
# both (tests/oversee/test_stop_rule_flag.py holds the flag-off rule to commit 31d9b58's).
pytestmark = pytest.mark.usefixtures("stop_at_entry_on")

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


# ---------------------------------------------------------------- effective_stop_detail (design PR1, S07)
# Synthetic EURUSD-style prices only; no real account values.


def mark_at(mid: str, quality: MarkQuality = MarkQuality.CLEAN) -> Mark:
    return Mark(KEY, D(mid), quality, T0)


def child(oid: str, level: str, kind: OrderKind | None, units: str = "100000", side: Side = "SELL",
          status: OrderStatus = OrderStatus.WORKING, link: str = "P") -> OrderDTO:
    return order(oid, side, status, ordered=units, entry=level, reduce_only=True, linked=link, kind=kind,
                 remaining="0" if status.terminal else None)


def test_stop_child_at_break_even_counts() -> None:
    """The stop that "disappeared": a StoneX STOP child moved to the entry price is still the stop."""
    p = position("P", "BUY", entry="1.1000")
    c = child("C", "1.1000", "STOP")
    detail = effective_stop_detail(p, [p], [c], mark_at("1.1050"))
    assert detail is not None and detail.level == D("1.1000") and detail.caveats == ()
    assert detail.handles == (StopHandle("CHILD_ORDER", "C", D("1.1000"), D(100000), "STOP"),)
    assert effective_stop(p, [p], [c]) == D("1.1000")
    fig = risk(p, D(100000), book({KEY: quote("1.1050", T0)}), "USD", [p], [c])
    assert fig.value == 0 and not fig.no_stop


def test_stop_child_beyond_entry_counts_short_side() -> None:
    p = position("P", "SELL", entry="1.1000")
    c = child("C", "1.0950", "STOP", side="BUY")
    detail = effective_stop_detail(p, [p], [c], mark_at("1.0900"))
    assert detail is not None and detail.level == D("1.0950") and detail.caveats == ()


def test_limit_child_never_counts() -> None:
    p = position("P", "BUY", entry="1.1000")
    assert effective_stop_detail(p, [p], [child("L", "1.0900", "LIMIT")]) is None
    assert effective_stop(p, [p], [child("L", "1.0900", "LIMIT")]) is None


def test_none_kind_child_keeps_the_loss_side_rule_with_caveat() -> None:
    p = position("P", "BUY", entry="1.1000")
    detail = effective_stop_detail(p, [p], [child("C", "1.0900", None)])
    assert detail is not None and detail.level == D("1.0900") and detail.caveats == (STOP_KIND_UNKNOWN,)
    assert effective_stop_detail(p, [p], [child("C", "1.1000", None)]) is None          # at entry: not counted


def test_none_kind_child_at_entry_is_not_counted_but_is_a_caveat() -> None:
    p = position("P", "BUY", entry="1.1000")
    s, n = child("S", "1.0900", "STOP"), child("N", "1.1000", None)
    detail = effective_stop_detail(p, [p], [s, n], mark_at("1.1000"))
    assert detail is not None and [h.id for h in detail.handles] == ["S"]
    assert detail.caveats == (STOP_KIND_UNKNOWN,)


def test_partial_coverage_is_no_stop() -> None:
    p = position("P", "BUY", units="100000", entry="1.1000")
    assert effective_stop_detail(p, [p], [child("C", "1.0900", "STOP", units="60000")]) is None
    two = [child("C1", "1.0900", "STOP", units="60000"), child("C2", "1.0850", "STOP", units="40000")]
    detail = effective_stop_detail(p, [p], two)
    assert detail is not None and detail.level == D("1.0850") and len(detail.handles) == 2   # most distant


def test_terminal_and_unlinked_children_do_not_count() -> None:
    p = position("P", "BUY", entry="1.1000")
    gone = child("C", "1.0900", "STOP", status=OrderStatus.CANCELLED)
    other = child("D", "1.0900", "STOP", link="Q")
    assert effective_stop_detail(p, [p], [gone, other]) is None


def test_position_field_handle_for_a_ctrader_position_stop() -> None:
    p = position("P", "SELL", units="5000", entry="1.1000", stop="1.1100", adapter="pepperstone")
    detail = effective_stop_detail(p, [p], [], mark_at("1.1000"))
    assert detail is not None and detail.level == D("1.1100") and detail.caveats == ()
    assert detail.handles == (StopHandle("POSITION_FIELD", "P", D("1.1100"), D(5000), None),)


def test_stop_loss_field_wins_over_children() -> None:
    p = position("P", "BUY", entry="1.1000", stop="1.0950")
    detail = effective_stop_detail(p, [p], [child("C", "1.0900", "STOP")])
    assert detail is not None and detail.level == D("1.0950") and detail.handles[0].kind == "POSITION_FIELD"


@pytest.mark.parametrize("side,level,mid,inconsistent", [
    ("BUY", "1.0900", "1.1000", False),     # sell stop below the market: triggerable
    ("BUY", "1.1000", "1.1000", True),      # touched, awaiting its fill
    ("BUY", "1.1200", "1.1000", True),      # take-profit mislabelled as STOP
    ("SELL", "1.1100", "1.1000", False),
    ("SELL", "1.1000", "1.1000", True),
    ("SELL", "1.0800", "1.1000", True),
])
def test_triggerable_side_against_a_fresh_mark(side: Side, level: str, mid: str, inconsistent: bool) -> None:
    p = position("P", side, entry="1.1000")
    c = child("C", level, "STOP", side="SELL" if side == "BUY" else "BUY")
    detail = effective_stop_detail(p, [p], [c], mark_at(mid))
    assert detail is not None and detail.level == D(level)          # the level is kept either way
    assert (STOP_KIND_INCONSISTENT in detail.caveats) is inconsistent


def test_triggerable_side_not_judged_without_a_usable_mark() -> None:
    p = position("P", "BUY", entry="1.1000")
    c = child("C", "1.1200", "STOP")
    for m in (None, mark_at("1.1000", MarkQuality.LAST_MID)):       # no mark; OPEN with a frozen feed
        detail = effective_stop_detail(p, [p], [c], m)
        assert detail is not None and detail.caveats == ()
    for q in (MarkQuality.LIQUIDATION, MarkQuality.CLOSED_LAST_CLEAN):
        detail = effective_stop_detail(p, [p], [c], mark_at("1.1000", q))
        assert detail is not None and detail.caveats == (STOP_KIND_INCONSISTENT,)


def test_children_of_an_unfilled_entry_order_are_not_judged_against_the_mark() -> None:
    """A buy-stop entry above the market with its sell-stop child also above the market is not inconsistent."""
    entry = order("E", "BUY", OrderStatus.WORKING, entry="1.1100")
    c = child("C", "1.1050", "STOP", link="E")
    detail = effective_stop_detail(entry, [], [entry, c], mark_at("1.1000"))
    assert detail is not None and detail.level == D("1.1050") and detail.caveats == ()


def test_break_even_stop_across_a_weekend_close_is_judged_against_the_last_mark() -> None:
    p = position("P", "BUY", entry="1.1000")
    c = child("C", "1.1000", "STOP")
    closed = SymbolQuotes(last_clean=clean_mark("1.1080", T0 - 3_600_000), session="CLOSED")
    m = select_mark(KEY, closed, T0, 5000, "BUY")
    assert m.quality is MarkQuality.CLOSED_LAST_CLEAN
    detail = effective_stop_detail(p, [p], [c], m)
    assert detail is not None and detail.level == D("1.1000") and detail.caveats == ()
    gapped = SymbolQuotes(last_clean=clean_mark("1.0950", T0 - 3_600_000), session="CLOSED")
    detail = effective_stop_detail(p, [p], [c], select_mark(KEY, gapped, T0, 5000, "BUY"))
    assert detail is not None and detail.caveats == (STOP_KIND_INCONSISTENT,)


@pytest.mark.parametrize("status", [OrderStatus.WORKING, OrderStatus.PENDING])
def test_unfilled_entry_order_with_stop_child_beyond_entry_is_no_stop(status: OrderStatus) -> None:
    """As before S07: before any fill a STOP child beyond the entry (in practice a mislabelled take-profit) is
    not a stop at all, so risk() reports no_stop (not UNKNOWN, which would blind the aggregate rule) and
    MissingStopLossRule goes `missing_stop_loss` after its grace."""
    entry = order("E", "BUY", status, entry="1.1000")
    c = child("C", "1.1050", "STOP", link="E")
    assert effective_stop_detail(entry, [], [entry, c], mark_at("1.1000")) is None
    assert effective_stop(entry, [], [entry, c]) is None
    fig = risk(entry, D(100000), book(), "USD", [], [entry, c])
    assert fig.no_stop and fig.value == 0
    short = order("S", "SELL", status, entry="1.1000")
    below = child("B", "1.0950", "STOP", side="BUY", link="S")
    assert effective_stop(short, [], [short, below]) is None
    ok = child("C", "1.0950", "STOP", link="E")
    assert risk(entry, D(100000), book(), "USD", [], [entry, ok]).value == D("500.0000")
    at = child("C", "1.1000", "STOP", link="E")                       # at entry: counted, risk 0
    assert effective_stop(entry, [], [entry, at]) == D("1.1000")


def test_stop_field_beyond_an_unfilled_entry_stays_unknown() -> None:
    entry = order("E", "BUY", OrderStatus.WORKING, entry="1.1000", stop="1.1050")
    with pytest.raises(RiskUnknown) as exc:
        risk(entry, D(100000), book(), "USD", [], [entry])
    assert exc.value.reason == "risk_unknown"


def test_partially_filled_entry_keeps_a_stop_child_beyond_entry() -> None:
    """Part of it is filled, so a STOP moved to or beyond entry is a break-even stop (PR1)."""
    entry = order("E", "BUY", OrderStatus.PARTIALLY_FILLED, entry="1.1000", remaining="50000")
    c = child("C", "1.1050", "STOP", units="50000", link="E")
    assert effective_stop(entry, [], [entry, c]) == D("1.1050")


def test_inconsistent_ids_name_only_the_inconsistent_child() -> None:
    p = position("P", "BUY", units="100000", entry="1.1000")
    good = child("G", "1.0900", "STOP", units="60000")
    bad = child("T", "1.1200", "STOP", units="40000")
    detail = effective_stop_detail(p, [p], [good, bad], mark_at("1.1000"))
    assert detail is not None and detail.caveats == (STOP_KIND_INCONSISTENT,)
    assert detail.inconsistent_ids == ("T",) and {h.id for h in detail.handles} == {"G", "T"}
    clean = effective_stop_detail(p, [p], [good, child("T", "1.0950", "STOP", units="40000")], mark_at("1.1000"))
    assert clean is not None and clean.inconsistent_ids == () and clean.caveats == ()


def test_market_entry_order_with_a_stop_child_has_a_stop() -> None:
    entry = order("E", "BUY", OrderStatus.WORKING, entry=None)
    stop, none = child("C", "1.0950", "STOP", link="E"), child("N", "1.0950", None, link="E")
    assert effective_stop(entry, [], [entry, stop]) == D("1.0950")
    assert effective_stop(entry, [], [entry, none]) is None          # a None kind needs an entry to compare


SIDES: list[Side] = ["BUY", "SELL"]
KINDS: list[OrderKind | None] = ["STOP", "LIMIT", None]
LEVELS = ["1.0800", "1.0900", "1.0950", "1.1000", "1.1050", "1.1100", "1.1200"]
STATUSES = [OrderStatus.WORKING, OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED,
            OrderStatus.FILLED]


def _random_entities(rng: random.Random) -> tuple[list[PositionDTO], list[OrderDTO]]:
    positions: list[PositionDTO] = []
    orders: list[OrderDTO] = []
    for i in range(rng.randint(1, 3)):
        stop = rng.choice([None, None, rng.choice(LEVELS)])
        positions.append(position(f"P{i}", rng.choice(SIDES), units=str(rng.choice([1000, 5000, 10000])),
                                  entry=rng.choice(LEVELS), stop=stop))
    for i in range(rng.randint(0, 3)):
        orders.append(order(f"E{i}", rng.choice(SIDES), rng.choice(STATUSES[:3]),
                            ordered=str(rng.choice([1000, 5000])), entry=rng.choice([None, *LEVELS])))
    targets = [p.position_id for p in positions] + [o.broker_order_id for o in orders] + ["X"]
    for i in range(rng.randint(0, 6)):
        status = rng.choice(STATUSES)
        orders.append(order(f"C{i}", rng.choice(SIDES), status, ordered=str(rng.choice([1000, 2500, 5000, 10000])),
                            remaining="0" if status.terminal else None, entry=rng.choice(LEVELS),
                            reduce_only=rng.random() < 0.9, linked=rng.choice(targets), kind=rng.choice(KINDS)))
    return positions, orders


def _oracle(e: OrderDTO | PositionDTO, positions: list[PositionDTO],
            orders: list[OrderDTO]) -> tuple[Decimal | None, bool]:
    """The expected level and whether a None-kind candidate child exists, recomputed from the spec (§5, PR1)
    independently of src/oversee/risk.py."""
    if e.stop_loss is not None:
        return e.stop_loss, False
    eid = e.position_id if isinstance(e, PositionDTO) else e.broker_order_id
    unfilled = (isinstance(e, OrderDTO) and e.status in (OrderStatus.PENDING, OrderStatus.WORKING)
                and e.entry_price is not None)
    cands = [o for o in orders
             if o.is_reduce_only and not o.status.terminal and o.linked_position_id == eid
             and o.symbol == e.symbol and o.side != e.side and o.entry_price is not None]
    loss = (lambda x: x < e.entry_price) if e.side == "BUY" else (lambda x: x > e.entry_price)  # noqa: E731
    beyond = (lambda x: x > e.entry_price) if e.side == "BUY" else (lambda x: x < e.entry_price)  # noqa: E731
    counted = []
    for o in cands:
        assert o.entry_price is not None
        if o.order_kind == "STOP" and not (unfilled and beyond(o.entry_price)):
            counted.append(o)
        elif o.order_kind is None and e.entry_price is not None and loss(o.entry_price):
            counted.append(o)
    has_none = any(o.order_kind is None for o in cands)
    if not counted:
        return None, has_none
    need = sum((p.units or D(0) for p in positions
                if p.position_id == eid and p.status == "OPEN" and p.symbol == e.symbol and p.side == e.side),
               D(0))
    need += sum((o.remaining_units or D(0) for o in orders
                 if o.broker_order_id == eid and not o.is_reduce_only and not o.status.terminal
                 and o.symbol == e.symbol and o.side == e.side), D(0))
    if sum((o.remaining_units or D(0) for o in counted), D(0)) < need:
        return None, has_none
    levels = [o.entry_price for o in counted if o.entry_price is not None]
    return (min(levels) if e.side == "BUY" else max(levels)), has_none


def test_property_effective_stop_equals_detail_level() -> None:
    """effective_stop == effective_stop_detail(...).level == an independent oracle of the spec over generated
    entity sets, with and without a mark."""
    rng = random.Random(20261116)
    marks = [None, mark_at("1.1000"), mark_at("1.0900", MarkQuality.LAST_MID),
             mark_at("1.1100", MarkQuality.CLOSED_LAST_CLEAN)]
    checked = 0
    for _ in range(400):
        positions, orders = _random_entities(rng)
        for e in [*positions, *orders]:
            level = effective_stop(e, positions, orders)
            expected, has_none = _oracle(e, positions, orders)
            assert level == expected
            for m in marks:
                detail = effective_stop_detail(e, positions, orders, m)
                assert (detail.level if detail is not None else None) == level
                if detail is None:
                    continue
                checked += 1
                assert (STOP_KIND_UNKNOWN in detail.caveats) is has_none
                assert bool(detail.inconsistent_ids) is (STOP_KIND_INCONSISTENT in detail.caveats)
                assert set(detail.inconsistent_ids) <= {h.id for h in detail.handles}
                assert set(detail.caveats) <= STOP_CAVEATS and list(detail.caveats) == sorted(detail.caveats)
                assert all(h.order_kind != "LIMIT" for h in detail.handles)
                assert detail.level in {h.level for h in detail.handles}
                if m is None:
                    assert STOP_KIND_INCONSISTENT not in detail.caveats
    assert checked > 100
