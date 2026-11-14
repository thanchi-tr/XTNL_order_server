"""ORDERS stream mapping: order -> position emission, closing trades, if-done children."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.adapters.stonex.instruments import spec_from_market_info
from src.adapters.stonex.mapping import UnmappableError
from src.adapters.stonex.orders import ORDER_FIELDS, OrderItem, OrderStreamMapper
from src.dto import InstrumentSpec, OrderDTO, OrderStatus, PositionDTO

from .conftest import AUDUSD_ID, CLIENT, EURUSD_ID, MARKETS, TRADING, wcf

T0 = 1_767_225_600_000


def specs() -> dict[str, InstrumentSpec]:
    out = {}
    for mid in (EURUSD_ID, AUDUSD_ID):
        spec = spec_from_market_info(MARKETS[mid])
        assert spec is not None
        out[mid] = spec
    return out


def mapper(known: dict[str, InstrumentSpec] | None = None) -> OrderStreamMapper:
    table = specs() if known is None else known
    return OrderStreamMapper(lookup=lambda m: table.get(m) if m else None,
                             account_for=lambda t: CLIENT if t in (TRADING, None) else None)


def frame(order_id: str, status: str, *, type_: str = "1", direction: str = "1", qty: str | None = "10000",
          orig: str | None = None, open_price: str | None = "1.08500", exec_price: str | None = "0",
          ts: int = T0, market: str = EURUSD_ID) -> dict[str, str | None]:
    return {"OrderId": order_id, "Direction": direction, "Type": type_, "ExecutionPrice": exec_price,
            "LastChangedTime": wcf(ts), "Status": status, "Quantity": qty, "OpenPrice": open_price,
            "ReasonId": "1", "MarketId": market, "TradingAccountId": TRADING, "OriginalQuantity": orig}


def run(m: OrderStreamMapper, values: dict[str, str | None]) -> list[OrderDTO | PositionDTO]:
    item = OrderItem.from_frame(values)
    assert item is not None
    return m.map(item)


def orders(out: list[OrderDTO | PositionDTO]) -> list[OrderDTO]:
    return [o for o in out if isinstance(o, OrderDTO)]


def positions(out: list[OrderDTO | PositionDTO]) -> list[PositionDTO]:
    return [p for p in out if isinstance(p, PositionDTO)]


def test_field_list_is_the_proven_xtnl_list() -> None:
    assert set(ORDER_FIELDS) == {"OrderId", "Direction", "Type", "ExecutionPrice", "LastChangedTime", "Status",
                                 "Quantity", "OpenPrice", "ReasonId", "MarketId", "TradingAccountId",
                                 "OriginalQuantity"}


def test_frame_without_order_id_is_ignored() -> None:
    assert OrderItem.from_frame({"OrderId": None}) is None


def test_market_order_lifecycle_emits_position_then_position_only() -> None:
    m = mapper()
    pending = run(m, frame("100", "1", ts=T0))
    assert [o.status for o in orders(pending)] == [OrderStatus.PENDING] and not positions(pending)
    o = orders(pending)[0]
    assert (o.side, o.symbol, o.account_id, o.client_order_id) == ("BUY", "EURUSD", CLIENT, "100")
    assert o.ordered_units == Decimal(10000) and o.remaining_units == Decimal(10000)
    assert (o.broker_updated_at_ms, o.source, o.observed_seq, o.seq_id) == (T0, "STREAM", 0, 0)

    filled = run(m, frame("100", "3", ts=T0 + 10, open_price="1.08512"))
    assert [x.status for x in orders(filled)] == [OrderStatus.FILLED]
    assert orders(filled)[0].remaining_units == 0 and not orders(filled)[0].is_reduce_only
    (pos,) = positions(filled)
    assert (pos.position_id, pos.status, pos.units, pos.entry_price, pos.side) == (
        "100", "OPEN", Decimal(10000), Decimal("1.08512"), "BUY")
    assert pos.broker_updated_at_ms == T0 + 10 and pos.stop_loss is None and pos.opened_by_client_order_id == "100"

    partial = run(m, frame("100", "3", qty="4000", ts=T0 + 20))
    assert not orders(partial)   # order terminal: position only
    assert [(p.status, p.units) for p in positions(partial)] == [("OPEN", Decimal(4000))]

    closed = run(m, frame("100", "9", qty="4000", ts=T0 + 30))
    assert not orders(closed)
    assert [(p.status, p.units) for p in positions(closed)] == [("CLOSED", Decimal(0))]
    assert m.watermark_ms == T0 + 30


def test_quantity_zero_closes_the_position() -> None:
    m = mapper()
    run(m, frame("101", "3"))
    (pos,) = positions(run(m, frame("101", "3", qty="0", ts=T0 + 5)))
    assert pos.status == "CLOSED" and pos.units == 0


def test_entry_limit_order_fills_into_a_position() -> None:
    m = mapper()
    m.learn_parent("102", None)   # REST said: standalone entry order
    (working,) = run(m, frame("102", "2", type_="3", open_price="1.07000"))
    assert isinstance(working, OrderDTO) and working.status == OrderStatus.WORKING
    assert working.entry_price == Decimal("1.07000") and not working.is_reduce_only
    out = run(m, frame("102", "3", type_="1", open_price="1.07000", ts=T0 + 1))
    assert [o.status for o in orders(out)] == [OrderStatus.FILLED]
    assert [p.status for p in positions(out)] == ["OPEN"]


def test_closing_trade_first_seen_closed_is_reduce_only_and_never_a_position() -> None:
    m = mapper()
    run(m, frame("200", "3", direction="1"))                           # long position 200
    out = run(m, frame("201", "9", direction="0", qty="10000", ts=T0 + 5))
    assert not positions(out)
    (closing,) = orders(out)
    assert closing.status == OrderStatus.FILLED and closing.is_reduce_only
    assert closing.linked_position_id == "200" and closing.side == "SELL"


def test_closing_trade_link_is_none_when_ambiguous() -> None:
    m = mapper()
    run(m, frame("210", "3", direction="1"))
    run(m, frame("211", "3", direction="1"))
    (closing,) = orders(run(m, frame("212", "9", direction="0", ts=T0 + 5)))
    assert closing.is_reduce_only and closing.linked_position_id is None


def test_position_known_from_rest_closes_instead_of_becoming_a_closing_trade() -> None:
    m = mapper()
    rest_pos = PositionDTO(position_id="300", account_id=CLIENT, adapter_id="stonex", symbol="EURUSD",
                           side="BUY", units=Decimal(5000), raw_qty=Decimal(5000), entry_price=Decimal("1.1"),
                           status="OPEN", source="REST")
    m.learn_position(rest_pos, EURUSD_ID)
    out = run(m, frame("300", "9", qty="5000"))
    assert not orders(out)
    assert [(p.position_id, p.status) for p in positions(out)] == [("300", "CLOSED")]


def test_child_stop_from_rest_parent_map_is_reduce_only_and_linked() -> None:
    m = mapper()
    m.learn_parent("401", "400")
    run(m, frame("400", "3", direction="1"))
    (stop,) = run(m, frame("401", "2", type_="2", direction="0", open_price="1.08000", ts=T0 + 1))
    assert isinstance(stop, OrderDTO)
    assert stop.is_reduce_only and stop.linked_position_id == "400"
    assert stop.status == OrderStatus.WORKING and stop.entry_price == Decimal("1.08000")
    assert stop.stop_loss is None
    # never folded into the parent position
    (pos,) = positions(run(m, frame("400", "3", direction="1", ts=T0 + 2)))
    assert pos.stop_loss is None
    # the child's fill is an order fill only (the parent's own item reduces / closes the position)
    filled = run(m, frame("401", "9", type_="2", direction="0", ts=T0 + 3))
    assert [(o.status, o.is_reduce_only) for o in orders(filled)] == [(OrderStatus.FILLED, True)]
    assert not positions(filled)


def test_pending_bracket_links_to_parent_by_creation_time() -> None:
    m = mapper()
    run(m, frame("500", "2", type_="3", direction="1", ts=T0))         # entry limit (unknown parentage)
    (child,) = run(m, frame("501", "1", type_="2", direction="0", ts=T0, open_price="1.06000"))
    assert isinstance(child, OrderDTO)
    assert child.status == OrderStatus.PENDING and child.is_reduce_only and child.linked_position_id == "500"
    assert m.parent_of("501") == "500"


def test_child_quantity_falls_back_to_parent() -> None:
    m = mapper()
    m.learn_parent("601", "600")
    run(m, frame("600", "2", type_="3", qty="30000"))
    (child,) = run(m, frame("601", "1", type_="2", direction="0", qty=None))
    assert isinstance(child, OrderDTO) and child.ordered_units == Decimal(30000)


def test_needs_parent_lookup_only_for_unknown_stop_limit_orders() -> None:
    m = mapper()
    stop = OrderItem.from_frame(frame("700", "2", type_="2"))
    trade = OrderItem.from_frame(frame("701", "1", type_="1"))
    assert stop is not None and trade is not None
    assert m.needs_parent_lookup(stop) and not m.needs_parent_lookup(trade)
    m.mark_looked_up("700")
    assert not m.needs_parent_lookup(stop)


def test_unknown_instrument_emits_raw_quantity() -> None:
    m = mapper(known={})
    out = run(m, frame("800", "3", market=AUDUSD_ID, qty="15000"))
    (order,) = orders(out)
    (pos,) = positions(out)
    assert order.instrument_unknown and order.ordered_units is None and order.raw_qty == Decimal(15000)
    assert order.symbol == f"UNKNOWN:{AUDUSD_ID}"
    assert pos.instrument_unknown and pos.units is None and pos.raw_qty == Decimal(15000)


def test_cancel_and_reject_are_terminal_orders() -> None:
    m = mapper()
    m.learn_parent("900", None)
    run(m, frame("900", "2", type_="2"))
    (cancelled,) = run(m, frame("900", "4", type_="2", ts=T0 + 1))
    assert isinstance(cancelled, OrderDTO) and cancelled.status == OrderStatus.CANCELLED
    assert cancelled.remaining_units == Decimal(10000)
    (rejected,) = run(m, frame("901", "5"))
    assert isinstance(rejected, OrderDTO) and rejected.status == OrderStatus.REJECTED


@pytest.mark.parametrize("bad", [{"Status": None}, {"Direction": None}, {"TradingAccountId": "9999"}])
def test_unmappable_items_raise(bad: dict[str, str | None]) -> None:
    m = OrderStreamMapper(lookup=lambda _m: None, account_for=lambda t: CLIENT if t == TRADING else None)
    values = frame("950", "2") | bad
    with pytest.raises(UnmappableError):
        run(m, values)
