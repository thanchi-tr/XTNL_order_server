from __future__ import annotations

import asyncio

import pytest

from src.pubsub.bus import ACCOUNT_TOPIC, ORDERS_TOPIC, POSITIONS_TOPIC, PRICES_TOPIC, EventBus, PricePut
from tests.pubsub.conftest import account, cash_flow, live, order, position, price


def test_constructible_without_loop_and_four_isolated_topics() -> None:
    bus = EventBus()
    assert set(bus.depths()) == {ORDERS_TOPIC, POSITIONS_TOPIC, PRICES_TOPIC, ACCOUNT_TOPIC}
    topics = [bus.orders, bus.positions, bus.prices, bus.account]
    assert len({id(t) for t in topics}) == 4
    assert bus.queue(ORDERS_TOPIC) is bus.orders


def test_price_surge_never_delays_or_drops_entity_items() -> None:
    bus = EventBus(prices_maxsize=50)
    for i in range(10_000):
        bus.publish_price_nowait(price(symbol=f"S{i % 200}", ts=i))
    o, p, a, cf, lv = order(), position(), account(), cash_flow(), live()
    bus.publish_order_nowait(o)
    bus.publish_position_nowait(p)
    bus.publish_account_nowait(a)
    bus.publish_account_nowait(cf)
    bus.publish_account_nowait(lv)
    assert bus.orders.get_nowait() is o
    assert bus.positions.get_nowait() is p
    assert [bus.account.get_nowait() for _ in range(3)] == [a, cf, lv]
    assert bus.prices.qsize() == 50
    assert bus.prices.stats.dropped > 0


def test_price_coalesces_latest_per_adapter_and_symbol_in_fifo_order() -> None:
    bus = EventBus()
    assert bus.publish_price_nowait(price("EURUSD", "1.1", ts=1)) is PricePut.QUEUED
    assert bus.publish_price_nowait(price("GBPUSD", "1.3", ts=2)) is PricePut.QUEUED
    assert bus.publish_price_nowait(price("EURUSD", "1.2", ts=3, adapter_id="other")) is PricePut.QUEUED
    assert bus.publish_price_nowait(price("EURUSD", "1.15", ts=4)) is PricePut.COALESCED
    got = [bus.prices.get_nowait() for _ in range(bus.prices.qsize())]
    assert [(p.adapter_id, p.symbol, p.timestamp_ms) for p in got] == [
        ("mock", "EURUSD", 4), ("mock", "GBPUSD", 2), ("other", "EURUSD", 3)]
    assert bus.prices.stats.coalesced == 1


def test_full_price_topic_drops_new_keys_but_still_coalesces() -> None:
    bus = EventBus(prices_maxsize=2)
    bus.publish_price_nowait(price("A"))
    bus.publish_price_nowait(price("B"))
    assert bus.publish_price_nowait(price("C")) is PricePut.DROPPED
    assert bus.publish_price_nowait(price("A", "2.0")) is PricePut.COALESCED
    assert bus.prices.get_nowait().bid == 2


def test_entity_topics_raise_queue_full_and_count() -> None:
    bus = EventBus(orders_maxsize=1, positions_maxsize=1, account_maxsize=1)
    bus.publish_order_nowait(order("O1"))
    with pytest.raises(asyncio.QueueFull):
        bus.publish_order_nowait(order("O2"))
    bus.publish_position_nowait(position())
    with pytest.raises(asyncio.QueueFull):
        bus.publish_position_nowait(position("P2"))
    bus.publish_account_nowait(account())
    with pytest.raises(asyncio.QueueFull):
        bus.publish_account_nowait(cash_flow())
    assert bus.stats()[ORDERS_TOPIC].queue_full == 1
    assert bus.depths() == {ORDERS_TOPIC: 1, POSITIONS_TOPIC: 1, PRICES_TOPIC: 0, ACCOUNT_TOPIC: 1}
    assert bus.fill_ratios()[ORDERS_TOPIC] == 1.0


def test_bounded_topics_required() -> None:
    with pytest.raises(ValueError):
        EventBus(orders_maxsize=0)


async def test_price_get_waits_for_a_tick() -> None:
    bus = EventBus()
    getter = asyncio.create_task(bus.prices.get())
    await asyncio.sleep(0)
    assert not getter.done()
    bus.publish_price_nowait(price("EURUSD"))
    got = await asyncio.wait_for(getter, 1)
    assert got.symbol == "EURUSD"
    with pytest.raises(asyncio.QueueEmpty):
        bus.prices.get_nowait()


async def test_publish_never_awaits() -> None:
    """Publishing a full burst completes without yielding to the loop."""
    bus = EventBus(orders_maxsize=5)
    ran = False

    async def other() -> None:
        nonlocal ran
        ran = True

    t = asyncio.create_task(other())
    for i in range(5):
        bus.publish_order_nowait(order(f"O{i}"))
    for i in range(1000):
        bus.publish_price_nowait(price(f"S{i}"))
    assert not ran
    await t
