"""StoneX fetch_history versions and links (live probe 2026-11-14, read-only): a stop / limit history row yields its
placement (WORKING at CreatedDateTimeUtc) before its end, the stop-loss / take-profit children are reduce-only and
linked to their entry (trade history OpeningOrderIds of a filled child; else the same-second X+1 / X+2 bracket
rule), an opening trade listing only its own id is an opening trade, and a parent the history cannot prove is never
guessed. Rows are shaped like the live ones: OrderReference / TradeReference empty, OrderApplicabilityId constant.
No network (fake CIAPI)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from src.adapters.base import cash_flows_partial
from src.adapters.stonex.history import map_stop_limit_history, stop_limit_parents
from src.dto import OrderDTO, OrderStatus

from .conftest import AUDUSD_ID, EURUSD_ID, TRADING, FakeStoneX, make_adapter, wcf

T0 = 1_794_000_000_000
UNTIL = T0 + 6 * 3_600_000
S = 1_000
M = 60_000
X = 1038153117          # the live ids: entry X, stop-loss X+1, take-profit X+2


def sl_row(order_id: int, *, direction: str, type_id: int, status: int, created: int, changed: int,
           level: float, market: str = EURUSD_ID) -> dict[str, Any]:
    """An order/stoplimitorderhistory row with the live field set (no reference links)."""
    return {"OrderId": order_id, "MarketId": int(market), "MarketName": "x", "Direction": direction,
            "TypeId": type_id, "StatusId": status, "OriginalQuantity": 5000, "TriggerPrice": level, "Price": 0,
            "TradingAccountId": int(TRADING), "Currency": "USD", "Guaranteed": False, "KnockOutLevel": None,
            "TrailingDistance": None, "Source": "G2", "OrderApplicabilityId": 2, "OrderReference": None,
            "TradeReference": None, "CreatedDateTimeUtc": wcf(created), "LastChangedDateTimeUtc": wcf(changed)}


def trade_row(order_id: int, *, direction: str, executed: int, changed: int | None = None,
              opening: list[int] | None = None, pnl: float | None = None, price: float = 1.087) -> dict[str, Any]:
    row: dict[str, Any] = {"OrderId": order_id, "OpeningOrderIds": opening or [], "MarketId": int(EURUSD_ID),
                           "Direction": direction, "OriginalQuantity": 5000, "Quantity": 5000, "Price": price,
                           "TradingAccountId": int(TRADING), "IsCloseBy": False, "OrderReference": None,
                           "TradeReference": None, "ExecutedDateTimeUtc": wcf(executed),
                           "LastChangedDateTimeUtc": wcf(changed if changed is not None else executed)}
    if pnl is not None:
        row["RealisedPnl"] = pnl
        row["RealisedPnlCurrency"] = "AUD"
    return row


def filled_bracket(fake: FakeStoneX, x: int = X, placed: int = T0 + 10 * M) -> tuple[int, int, int]:
    """The live shape of 1038153117: SELL stop entry X placed with its BUY stop-loss X+1 and BUY take-profit X+2 in
    the same second; X fills; later X+1 fills (closing X, with P&L) and X+2 is cancelled. The opening trade lists
    only its own id and has no P&L, and its last change moved to the close."""
    filled, closed = placed + 6 * M + 29 * S, placed + 102 * M
    fake.stop_limit_history += [
        sl_row(x, direction="Sell", type_id=2, status=9, created=placed, changed=filled, level=1.0850),
        sl_row(x + 1, direction="Buy", type_id=2, status=9, created=placed, changed=closed, level=1.0900),
        sl_row(x + 2, direction="Buy", type_id=3, status=4, created=placed, changed=closed, level=1.0750),
    ]
    fake.history += [
        trade_row(x, direction="Sell", executed=filled, changed=closed, opening=[x], price=1.0849),
        trade_row(x + 1, direction="Buy", executed=closed, opening=[x], pnl=-12.5, price=1.0901),
    ]
    return placed, filled, closed


def versions(orders: tuple[OrderDTO, ...], oid: int) -> list[tuple[OrderStatus, int, bool, str | None]]:
    return [(o.status, o.broker_updated_at_ms, o.is_reduce_only, o.linked_position_id)
            for o in orders if o.broker_order_id == str(oid)]


async def test_a_filled_bracket_yields_placement_and_end_versions_with_its_children_linked(
        fake_broker: FakeStoneX) -> None:
    placed, filled, closed = filled_bracket(fake_broker)
    adapter = make_adapter(fake_broker, stop_limit_history=True)
    await adapter.connect()
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete, h.notes
    # the entry: WORKING when placed, FILLED at its execution (never reduce-only: its own id is no link)
    assert versions(h.orders, X) == [(OrderStatus.WORKING, placed, False, None),
                                     (OrderStatus.FILLED, filled, False, None)]
    # stop-loss: linked by the trade history (it filled, closing X); take-profit: by the same-second bracket rule
    assert versions(h.orders, X + 1) == [(OrderStatus.WORKING, placed, True, str(X)),
                                         (OrderStatus.FILLED, closed, True, str(X))]
    assert versions(h.orders, X + 2) == [(OrderStatus.WORKING, placed, True, str(X)),
                                         (OrderStatus.CANCELLED, closed, True, str(X))]
    by = {(o.broker_order_id, o.status): o for o in h.orders}
    working_sl = by[(str(X + 1), OrderStatus.WORKING)]
    assert working_sl.entry_price == Decimal("1.09") and working_sl.remaining_units == working_sl.ordered_units
    assert by[(str(X), OrderStatus.WORKING)].entry_price == Decimal("1.085")       # the stop entry level
    assert by[(str(X), OrderStatus.FILLED)].entry_price == Decimal("1.0849")       # the execution price
    # contract: the versions of one order oldest first (here: all rows by broker time)
    times = [o.broker_updated_at_ms for o in h.orders]
    assert times == sorted(times)
    (pos,) = h.positions
    assert (pos.position_id, pos.status, pos.broker_updated_at_ms, pos.entry_price) == (
        str(X), "CLOSED", closed, Decimal("1.0849"))                               # entry from the opening trade
    await adapter.close()


async def test_a_bracket_that_never_filled_links_its_legs_to_the_pending_entry(fake_broker: FakeStoneX) -> None:
    placed, ended = T0 + 30 * M, T0 + 45 * M
    fake_broker.stop_limit_history = [
        sl_row(X, direction="Buy", type_id=2, status=4, created=placed, changed=ended, level=1.09),
        sl_row(X + 1, direction="Sell", type_id=2, status=4, created=placed + 400, changed=ended, level=1.08),
        sl_row(X + 2, direction="Sell", type_id=3, status=4, created=placed + 400, changed=ended, level=1.10),
    ]
    adapter = make_adapter(fake_broker, stop_limit_history=True)
    await adapter.connect()
    h = await adapter.fetch_history(T0, UNTIL)
    assert versions(h.orders, X) == [(OrderStatus.WORKING, placed, False, None),
                                     (OrderStatus.CANCELLED, ended, False, None)]
    for leg in (X + 1, X + 2):
        assert versions(h.orders, leg) == [(OrderStatus.WORKING, placed + 400, True, str(X)),
                                           (OrderStatus.CANCELLED, ended, True, str(X))]
    await adapter.close()


def test_a_parent_the_history_cannot_prove_stays_unlinked() -> None:
    t = T0 + 5 * M
    rows = [
        # 100 / 101 SELL and 102 BUY in one second: 102 has two candidate entries -> ambiguous, never guessed
        ("C", sl_row(100, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("C", sl_row(101, direction="Sell", type_id=3, status=4, created=t, changed=t + M, level=1.10)),
        ("C", sl_row(102, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=1.09)),
        # 201 BUY, one second after its candidate 200 SELL: not placed together
        ("C", sl_row(200, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("C", sl_row(201, direction="Buy", type_id=2, status=4, created=t + S, changed=t + M, level=1.09)),
        # 301 on another market than 300
        ("C", sl_row(300, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("C", sl_row(301, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=0.6,
                     market=AUDUSD_ID)),
        # 401 on the same side as 400
        ("C", sl_row(400, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("C", sl_row(401, direction="Sell", type_id=3, status=4, created=t, changed=t + M, level=1.07)),
        # 501 of another account than 500
        ("C", sl_row(500, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("D", sl_row(501, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=1.09)),
    ]
    parents, unlinked = stop_limit_parents(rows)
    assert parents == {}
    assert unlinked == ["100", "101", "102", "200", "201", "300", "301", "400", "401", "500", "501"]
    out = map_stop_limit_history(rows, since_ms=T0, until_ms=UNTIL, lookup=lambda _m: None,
                                 known_parent=lambda _o: None)
    assert not any(o.is_reduce_only or o.linked_position_id for o in out.order_versions())
    assert out.unlinked == unlinked and not out.notes and not out.limits      # informational: never incomplete


def test_parent_evidence_order_and_a_child_is_never_a_parent() -> None:
    t = T0 + 5 * M
    rows = [
        ("C", sl_row(10, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("C", sl_row(11, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=1.09)),
        ("C", sl_row(12, direction="Buy", type_id=3, status=4, created=t, changed=t + M, level=1.07)),
        # a second bracket placed in the same second: 13's neighbours 12 / 11 are children, never its parent
        ("C", sl_row(13, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.081)),
        ("C", sl_row(14, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=1.091)),
        # ParentOrderId on the row and the adapter's own memo beat the bracket rule
        ("C", {**sl_row(20, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=1.09),
               "ParentOrderId": 7}),
        ("C", sl_row(30, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=1.09)),
    ]
    trades = [("C", trade_row(14, direction="Buy", executed=t + M, opening=[13], pnl=1.0))]
    parents, unlinked = stop_limit_parents(rows, trades, known_parent=lambda oid: "8" if oid == "30" else None)
    assert parents == {"11": "10", "12": "10", "14": "13", "20": "7", "30": "8"}
    assert unlinked == []                                  # 10 and 13 are entries with children


def test_a_row_without_a_creation_time_keeps_a_single_version() -> None:
    row = sl_row(40, direction="Sell", type_id=2, status=4, created=T0, changed=T0 + M, level=1.08)
    del row["CreatedDateTimeUtc"]
    same = sl_row(41, direction="Sell", type_id=2, status=4, created=T0 + M, changed=T0 + M, level=1.08)
    out = map_stop_limit_history([("C", row), ("C", same)], since_ms=T0, until_ms=UNTIL, lookup=lambda _m: None,
                                 known_parent=lambda _o: None)
    assert [(o.broker_order_id, o.status) for o in out.order_versions()] == [
        ("40", OrderStatus.CANCELLED), ("41", OrderStatus.CANCELLED)]        # created == ended: no placement


async def test_an_opening_trade_naming_only_itself_is_neither_reduce_only_nor_a_partial_ledger(
        fake_broker: FakeStoneX) -> None:
    """Live 2026-11-14: 5 of 5 opening trades list OpeningOrderIds = [own id] and no RealisedPnl. Taken for closing
    trades they were reduce-only orders linked to themselves and made the cash-flow ledger 'partial'."""
    placed, filled, closed = filled_bracket(fake_broker)
    adapter = make_adapter(fake_broker, stop_limit_history=True)
    await adapter.connect()
    flows = await adapter.fetch_cash_flows(T0)
    assert cash_flows_partial(flows) == ()
    assert [f.transaction_id for f in flows] == [f"stonex:trade:{X + 1}"]
    h = await adapter.fetch_history(T0, UNTIL)
    assert versions(h.orders, X)[-1] == (OrderStatus.FILLED, filled, False, None)
    await adapter.close()


def test_two_entries_placed_together_are_never_parent_and_child() -> None:
    """An OCO straddle (SELL stop 600, BUY stop 601, same second, adjacent ids) has the shape of an entry and its
    stop-loss; only the whole live bracket shape (X, X+1, X+2) links. A one-leg bracket (700 + 701) is not proof
    either, and an order the trade history shows as an opening fill (801, filled straddle leg) is never a child."""
    t = T0 + 5 * M
    rows = [
        ("C", sl_row(600, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("C", sl_row(601, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=1.09)),
        ("C", sl_row(700, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("C", sl_row(701, direction="Buy", type_id=2, status=4, created=t, changed=t + M, level=1.09)),
        # 800 SELL stop, 801 BUY stop (filled: an opening trade), 802 BUY: shaped like a full bracket of 800
        ("C", sl_row(800, direction="Sell", type_id=2, status=4, created=t, changed=t + M, level=1.08)),
        ("C", sl_row(801, direction="Buy", type_id=2, status=9, created=t, changed=t + M, level=1.09)),
        ("C", sl_row(802, direction="Buy", type_id=3, status=4, created=t, changed=t + M, level=1.10)),
    ]
    trades = [("C", trade_row(801, direction="Buy", executed=t + M, opening=[801]))]
    parents, unlinked = stop_limit_parents(rows, trades)
    assert parents == {}
    assert unlinked == ["600", "601", "700", "701", "800", "801", "802"]


def test_an_order_that_ended_pending_is_placed_pending_never_working() -> None:
    """StoneX 10 (red card), 8 (yellow card) and 1 map to PENDING: a WORKING placement before it would be an illegal
    WORKING -> PENDING transition (the end parked in the DLQ)."""
    rows = [("C", sl_row(900, direction="Sell", type_id=2, status=10, created=T0, changed=T0 + M, level=1.08)),
            ("C", sl_row(901, direction="Sell", type_id=2, status=4, created=T0, changed=T0 + M, level=1.08))]
    out = map_stop_limit_history(rows, since_ms=T0, until_ms=UNTIL, lookup=lambda _m: None,
                                 known_parent=lambda _o: None)
    trail = [(o.broker_order_id, o.status) for o in out.order_versions()]
    assert trail == [("900", OrderStatus.PENDING), ("901", OrderStatus.WORKING),
                     ("900", OrderStatus.PENDING), ("901", OrderStatus.CANCELLED)]
