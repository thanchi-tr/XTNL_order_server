"""K8 (adapter-isolation.md §2 StoneX): the PositionDTO(OPEN) emitted by an order's first filled ORDERS item takes
its entry price from REST (GET order/{id}/openposition, else GET order/{id}), read in the background while the item
is held (the stream never waits on REST). Without a REST answer the stream price stands in."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import TypeVar

from src.adapters.base import StreamEventKind
from src.adapters.stonex.adapter import StoneXAdapter
from src.adapters.stonex.orders import OrderItem, OrderStreamMapper
from src.dto import OrderDTO, OrderStatus, PositionDTO

from .conftest import CLIENT, EURUSD_ID, TRADING, FakeStoneX, FakeStreamTransport, make_adapter, wait_until, wcf
from .test_stonex_adapter import order_frame

T0 = 1_767_225_600_000
T = TypeVar("T")


async def take(gen: AsyncIterator[T], n: int, timeout: float = 2.0) -> list[T]:
    return [await asyncio.wait_for(gen.__anext__(), timeout) for _ in range(n)]


async def _subscribed(fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> tuple[StoneXAdapter, AsyncIterator[object]]:
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    fake_stream.event("ORDERS", StreamEventKind.SUBSCRIBED)
    await asyncio.wait_for(first, 2)
    return adapter, gen


def _open_position_row(order_id: int, price: float) -> dict[str, object]:
    return {"OrderId": order_id, "MarketId": int(EURUSD_ID), "Direction": "buy", "Quantity": 10000,
            "Price": price, "TradingAccountId": int(TRADING), "LastChangedDateTimeUTC": wcf(T0)}


async def test_k8_first_fill_takes_its_entry_from_openposition(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    fake_broker.order_positions["900"] = _open_position_row(900, 1.08612)
    adapter, gen = await _subscribed(fake_broker, fake_stream)
    fake_stream.emit("ORDERS", order_frame("900", "3", OpenPrice="1.08500", LastChangedTime=wcf(T0 + 1)))
    filled, opened = await take(gen, 2)
    assert isinstance(filled, OrderDTO) and filled.status == OrderStatus.FILLED
    assert isinstance(opened, PositionDTO) and opened.entry_price == Decimal("1.08612")
    assert "/TradingAPI/order/900/openposition" in fake_broker.paths()
    # A later item's OpenPrice never overwrites the REST entry (and it is not read again).
    reads = fake_broker.paths().count("/TradingAPI/order/900/openposition")
    fake_stream.emit("ORDERS", order_frame("900", "3", OpenPrice="1.09000", Quantity="5000",
                                           LastChangedTime=wcf(T0 + 2)))
    (reduced,) = await take(gen, 1)
    assert isinstance(reduced, PositionDTO) and reduced.entry_price == Decimal("1.08612")
    assert fake_broker.paths().count("/TradingAPI/order/900/openposition") == reads
    await gen.aclose()
    await adapter.close()


async def test_k8_falls_back_to_the_trade_order_when_openposition_is_not_served(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    fake_broker.closed_order_ids.add("901")                      # openposition answers 403 / 80001
    fake_broker.orders["901"] = {"TradeOrder": {"OrderId": 901, "MarketId": int(EURUSD_ID), "Direction": "Buy",
                                                "Quantity": 10000, "Price": 1.08777, "Status": 3,
                                                "TradingAccountId": int(TRADING)}}
    adapter, gen = await _subscribed(fake_broker, fake_stream)
    fake_stream.emit("ORDERS", order_frame("901", "3", OpenPrice="1.08500", LastChangedTime=wcf(T0 + 1)))
    _, opened = await take(gen, 2)
    assert isinstance(opened, PositionDTO) and opened.entry_price == Decimal("1.08777")
    await gen.aclose()
    await adapter.close()


async def test_k8_falls_back_to_the_trade_order_when_openposition_fails_5xx(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    """Any failed openposition read (here HTTP 500), not only 403 / 80001, moves on to GET order/{id}."""
    import httpx
    fake_broker.one_shot["/TradingAPI/order/903/openposition"] = httpx.Response(
        500, json={"HttpStatus": 500, "ErrorMessage": "server error"})
    fake_broker.orders["903"] = {"TradeOrder": {"OrderId": 903, "MarketId": int(EURUSD_ID), "Direction": "Buy",
                                                "Quantity": 10000, "Price": 1.08777, "Status": 3,
                                                "TradingAccountId": int(TRADING)}}
    adapter, gen = await _subscribed(fake_broker, fake_stream)
    fake_stream.emit("ORDERS", order_frame("903", "3", OpenPrice="1.08500", LastChangedTime=wcf(T0 + 1)))
    _, opened = await take(gen, 2)
    assert isinstance(opened, PositionDTO) and opened.entry_price == Decimal("1.08777")
    assert "/TradingAPI/order/903" in fake_broker.paths()
    await gen.aclose()
    await adapter.close()


async def test_k8_a_failed_openposition_read_in_the_side_lookup_still_reads_the_order(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    """Relation branch (no Direction on the item): the order body gives the side, a 500 on openposition is not
    fatal, and the entry still comes from the TradeOrder."""
    import httpx
    fake_broker.one_shot["/TradingAPI/order/904/openposition"] = httpx.Response(
        500, json={"HttpStatus": 500, "ErrorMessage": "server error"})
    fake_broker.orders["904"] = {"TradeOrder": {"OrderId": 904, "MarketId": int(EURUSD_ID), "Direction": "Buy",
                                                "Quantity": 10000, "Price": 1.08777, "Status": 3,
                                                "TradingAccountId": int(TRADING)}}
    adapter, gen = await _subscribed(fake_broker, fake_stream)
    fake_stream.emit("ORDERS", order_frame("904", "3", Direction=None, OpenPrice="1.08500",
                                           LastChangedTime=wcf(T0 + 1)))
    filled, opened = await take(gen, 2)
    assert isinstance(filled, OrderDTO) and filled.side == "BUY"
    assert isinstance(opened, PositionDTO) and opened.entry_price == Decimal("1.08777")
    assert fake_broker.paths().count("/TradingAPI/order/904") == 1          # each body read at most once
    await gen.aclose()
    await adapter.close()


async def test_k8_without_a_rest_answer_the_stream_price_stands_in(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter, gen = await _subscribed(fake_broker, fake_stream)  # no open position, no trade order served
    fake_stream.emit("ORDERS", order_frame("902", "3", OpenPrice="1.08500", LastChangedTime=wcf(T0 + 1)))
    _, opened = await take(gen, 2)
    assert isinstance(opened, PositionDTO) and opened.entry_price == Decimal("1.08500")
    assert "/TradingAPI/order/902/openposition" in fake_broker.paths()   # REST was asked first
    await gen.aclose()
    await adapter.close()


def _item(order_id: str, status: str, open_price: str = "1.08500", ts: int = T0) -> OrderItem:
    item = OrderItem.from_frame(order_frame(order_id, status, OpenPrice=open_price, LastChangedTime=wcf(ts)))
    assert item is not None
    return item


def test_k8_mapper_asks_once_per_first_fill_only() -> None:
    m = OrderStreamMapper(lookup=lambda _: None, account_for=lambda _: CLIENT)
    assert not m.needs_entry_lookup(_item("10", "2"))               # working: no position yet
    first = _item("10", "3")
    assert m.needs_entry_lookup(first)
    m.mark_entry_looked_up("10")
    assert not m.needs_entry_lookup(first)                           # one read per order
    m.learn_entry("10", Decimal("1.2"))
    out = m.map(first)
    pos = next(x for x in out if isinstance(x, PositionDTO))
    assert pos.entry_price == Decimal("1.2")
    assert not m.needs_entry_lookup(_item("10", "3", ts=T0 + 1))     # the position exists now


def test_k8_a_rest_position_entry_is_kept_against_a_later_open_price() -> None:
    m = OrderStreamMapper(lookup=lambda _: None, account_for=lambda _: CLIENT)
    rest = PositionDTO(position_id="20", account_id=CLIENT, adapter_id="stonex", symbol="EURUSD", side="BUY",
                       units=Decimal(10000), entry_price=Decimal("1.1"), status="OPEN",
                       broker_updated_at_ms=T0, source="REST")
    m.learn_position(rest, EURUSD_ID)
    assert not m.needs_entry_lookup(_item("20", "3"))
    (pos,) = [x for x in m.map(_item("20", "3", open_price="1.25", ts=T0 + 1)) if isinstance(x, PositionDTO)]
    assert pos.entry_price == Decimal("1.1")


def test_k8_without_rest_a_later_open_price_still_updates_a_stream_entry() -> None:
    m = OrderStreamMapper(lookup=lambda _: None, account_for=lambda _: CLIENT)
    m.map(_item("30", "3", open_price="1.20"))
    (pos,) = [x for x in m.map(_item("30", "3", open_price="1.21", ts=T0 + 1)) if isinstance(x, PositionDTO)]
    assert pos.entry_price == Decimal("1.21")
