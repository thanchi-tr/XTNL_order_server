from __future__ import annotations

import asyncio
from pathlib import Path

from src.ports import CommitOutcome
from src.pubsub.bus import EventBus
from src.pubsub.consumers import AccountImageProjector, OrderStateConsumer, PositionStateConsumer, PriceSubscriber
from src.pubsub.spooler import DlqSpooler
from tests.pubsub.conftest import FakeCommitter, FakeDlqSink, account, cash_flow, live, order, position, price


def setup(tmp_path: Path) -> tuple[EventBus, FakeCommitter, DlqSpooler]:
    return EventBus(), FakeCommitter(), DlqSpooler(FakeDlqSink(), spill_path=tmp_path / "s.bin")


async def test_each_consumer_calls_the_right_port_method(tmp_path: Path) -> None:
    bus, com, sp = setup(tmp_path)
    o, p, a, cf, lv, px = order(), position(), account(), cash_flow(), live(), price()
    bus.publish_order_nowait(o)
    bus.publish_position_nowait(p)
    bus.publish_account_nowait(a)
    bus.publish_account_nowait(cf)
    bus.publish_account_nowait(lv)
    bus.publish_price_nowait(px)
    oc = OrderStateConsumer(bus.orders, com, sp)
    for c in (oc, PositionStateConsumer(bus.positions, com, sp), AccountImageProjector(bus.account, com, sp),
              PriceSubscriber(bus.prices, com, sp)):
        await c.process_available()
    assert com.calls == [("commit_entity", o), ("commit_entity", p), ("commit_entity", a),
                         ("commit_cash_flow", cf), ("write_live", lv), ("write_live", px)]
    assert oc.outcomes == {CommitOutcome.APPLIED: 1}
    assert len(sp) == 0


async def test_exception_parks_consumer_error(tmp_path: Path) -> None:
    bus, com, sp = setup(tmp_path)
    com.fail = RuntimeError("pg constraint")
    bus.publish_order_nowait(order("O7"))
    c = OrderStateConsumer(bus.orders, com, sp)
    await c.process_available()
    assert c.failed == 1
    row = sp._ring[0]
    assert (row.entity_id, row.error_reason, row.event_type) == ("O7", "consumer_error", "ORDER")


async def test_timeout_parks_timeout(tmp_path: Path) -> None:
    bus, com, sp = setup(tmp_path)
    com.delay_s = 1.0
    bus.publish_account_nowait(cash_flow("T1"))
    c = AccountImageProjector(bus.account, com, sp, timeout_s=0.01)
    await c.process_available()
    assert sp._ring[0].error_reason == "timeout"
    assert sp._ring[0].event_type == "CASH_FLOW"


async def test_price_and_live_failures_are_never_parked(tmp_path: Path) -> None:
    bus, com, sp = setup(tmp_path)
    com.fail = RuntimeError("redis down")
    bus.publish_price_nowait(price())
    bus.publish_account_nowait(live())
    await PriceSubscriber(bus.prices, com, sp).process_available()
    await AccountImageProjector(bus.account, com, sp).process_available()
    assert len(sp) == 0


async def test_full_ring_waits_and_always_parks(tmp_path: Path) -> None:
    """5.2: a failed consumer commit is always written to events_dlq. With the ring full the consumer waits
    for the spooler's flush to make room instead of dropping the row, and pages once."""
    bus, com, _ = setup(tmp_path)
    sink = FakeDlqSink()
    sp = DlqSpooler(sink, capacity=1, spill_path=tmp_path / "s.bin")
    com.fail = RuntimeError("x")
    pages: list[tuple[str, str, str]] = []
    bus.publish_position_nowait(position("P1"))
    bus.publish_position_nowait(position("P2"))
    c = PositionStateConsumer(bus.positions, com, sp, pager=lambda *a: pages.append(a), park_retry_s=0.001)
    task = asyncio.create_task(c.process_available())
    for _ in range(50):
        await asyncio.sleep(0.005)
    assert not task.done() and c.park_waits == 1          # P2 waits: never dropped
    await sp.flush()                                       # the flusher makes room
    await asyncio.wait_for(task, 2)
    await sp.flush()
    assert sorted(r.entity_id for r in sink.rows.values()) == ["P1", "P2"]
    titles = [t for _, t, _ in pages]
    assert titles == ["consumer commit failed: positions", "DLQ ring full: positions"]   # throttled per reason


async def test_consumer_failure_pages_every_reason(tmp_path: Path) -> None:
    bus, com, sp = setup(tmp_path)
    pages: list[tuple[str, str, str]] = []
    com.fail = RuntimeError("pg constraint")
    bus.publish_order_nowait(order("O1"))
    c = OrderStateConsumer(bus.orders, com, sp, pager=lambda *a: pages.append(a))
    await c.process_available()
    assert pages and pages[0][0] == "high" and "O1" in pages[0][2] and "consumer_error" in pages[0][2]


async def test_run_task_consumes_and_does_not_block_producers(tmp_path: Path) -> None:
    bus, com, sp = setup(tmp_path)
    com.delay_s = 0.01
    c = OrderStateConsumer(bus.orders, com, sp)
    task = asyncio.create_task(c.run())
    for i in range(5):
        bus.publish_order_nowait(order(f"O{i}"))   # never awaits even while the consumer is busy
    for _ in range(200):
        if c.processed == 5:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    assert c.processed == 5


async def test_consumers_settle_the_commit_barrier(tmp_path: Path) -> None:
    """reconciled-before-commit: the consumer reports each row committed (or failed) to the topic's
    CommitBarrier, which is what releases a reconcile pass's gap clear."""
    from dataclasses import replace

    bus, com, sp = setup(tmp_path)
    o, a = replace(order("O1"), seq_id=11), replace(account(), seq_id=12)
    results: list[frozenset[str]] = []
    bus.commit_barrier.track("mock", [(11, "orders"), (12, "account")], results.append)
    bus.publish_order_nowait(o)
    bus.publish_account_nowait(a)
    await OrderStateConsumer(bus.orders, com, sp).process_available()
    assert results == [] and bus.commit_barrier.pending("mock") == 1
    com.fail = RuntimeError("pg down")
    await AccountImageProjector(bus.account, com, sp).process_available()   # parked, not committed
    assert results == [frozenset({"account"})]
