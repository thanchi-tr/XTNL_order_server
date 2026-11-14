"""fetch_history contract (BrokerHistory, the base default) and the MockBrokerAdapter's scripted history: the
off-time catch-up read the engine runs at start-up and after the PC wakes. No network I/O."""
from __future__ import annotations

import asyncio
import dataclasses
from decimal import Decimal

import pytest

from src.adapters.base import (
    BrokerAdapter,
    BrokerError,
    BrokerHistory,
    HistoryNotes,
    assert_read_only,
    covered_from,
    history_read,
)
from src.adapters.mock import (
    OFF_SINCE_MS,
    OFF_UNTIL_MS,
    MockBrokerAdapter,
    build_fx_fixture,
    build_offline_history_fixture,
)
from src.adapters.pepperstone.adapter import PepperstoneAdapter
from src.adapters.ratelimit import RestPriority, rest_priority
from src.adapters.stonex.adapter import StoneXAdapter
from src.dto import OrderDTO, OrderStatus, PositionDTO


def test_broker_history_is_frozen_and_unsupported_covers_nothing() -> None:
    h = BrokerHistory.unsupported("x", 100, 200)
    assert (h.complete, h.notes, h.orders, h.positions) == (False, ("history not supported",), (), ())
    assert not h.retryable                       # no facility: the same answer every time
    assert h.covered_from_ms == 200 and h.since_ms == 100 and h.until_ms == 200
    with pytest.raises(dataclasses.FrozenInstanceError):
        h.complete = True  # type: ignore[misc]


def test_covered_from_follows_the_retention() -> None:
    assert covered_from(100, 1_000, None) == 100
    assert covered_from(100, 1_000, 0) == 100
    assert covered_from(100, 1_000, 500) == 500
    assert covered_from(800, 1_000, 500) == 800


async def test_base_default_fetch_history_is_empty_and_incomplete() -> None:
    adapter = MockBrokerAdapter(adapter_id="plain")
    h = await BrokerAdapter.fetch_history(adapter, 10, 20)       # the inherited default, not the mock's
    assert isinstance(h, BrokerHistory)
    assert h.adapter_id == "plain" and not h.complete and h.notes == ("history not supported",)
    assert h.orders == () and h.positions == ()
    assert BrokerAdapter.history_retention_ms.fget(adapter) is None   # type: ignore[attr-defined]


@pytest.mark.parametrize("cls", [MockBrokerAdapter, StoneXAdapter, PepperstoneAdapter])
def test_history_keeps_every_adapter_read_only(cls: type) -> None:
    assert_read_only(cls)
    assert callable(getattr(cls, "fetch_history"))
    assert isinstance(getattr(cls, "history_retention_ms"), property)


# ===================================================================== mock
async def test_offline_fixture_lists_orders_placed_and_finished_while_off() -> None:
    adapter = MockBrokerAdapter.from_fixture(build_offline_history_fixture())
    h = await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)
    assert h.complete and h.notes == () and h.covered_from_ms == OFF_SINCE_MS
    by_id = {o.broker_order_id: o for o in h.orders}
    assert set(by_id) == {"H-MKT-1", "H-RT-1", "H-RT-1-X", "H-STP-1"}
    assert by_id["H-MKT-1"].status == OrderStatus.FILLED and by_id["H-MKT-1"].remaining_units == 0
    assert by_id["H-STP-1"].status == OrderStatus.CANCELLED          # placed and cancelled while off
    assert by_id["H-RT-1-X"].is_reduce_only and by_id["H-RT-1-X"].linked_position_id == "H-RT-1"
    positions = {p.position_id: p for p in h.positions}
    assert positions["H-MKT-1"].status == "OPEN" and positions["H-RT-1"].status == "CLOSED"
    assert positions["H-RT-1"].units == 0
    # every row carries a broker time inside the window, so a later stream row always compares newer
    assert all(OFF_SINCE_MS <= r.broker_updated_at_ms <= OFF_UNTIL_MS for r in [*h.orders, *h.positions])
    assert [o.broker_updated_at_ms for o in h.orders] == sorted(o.broker_updated_at_ms for o in h.orders)
    # the broker's current book agrees: the cancelled stop is not working, the closed position not open
    assert "H-STP-1" not in {o.broker_order_id for o in await adapter.fetch_open_orders()}
    status = await adapter.fetch_order_status("H-STP-1")
    assert status is not None and status.status == OrderStatus.CANCELLED
    assert "H-RT-1" not in {p.position_id for p in await adapter.fetch_positions()}


async def test_mock_history_only_lists_rows_of_the_window() -> None:
    adapter = MockBrokerAdapter.from_fixture(build_offline_history_fixture())
    before = await adapter.fetch_history(OFF_SINCE_MS - 10_000, OFF_SINCE_MS - 1)
    assert before.orders == () and before.positions == () and before.complete
    first_half = await adapter.fetch_history(OFF_SINCE_MS, (OFF_SINCE_MS + OFF_UNTIL_MS) // 2)
    assert {o.broker_order_id for o in first_half.orders} == {"H-MKT-1", "H-RT-1"}
    assert {p.position_id for p in first_half.positions} == {"H-MKT-1"}
    assert adapter.history_requests[-1] == (OFF_SINCE_MS, (OFF_SINCE_MS + OFF_UNTIL_MS) // 2)


async def test_mock_history_keeps_the_latest_row_per_entity() -> None:
    fx = build_fx_fixture()
    adapter = MockBrokerAdapter.from_fixture(fx)
    working = OrderDTO(client_order_id="S1", broker_order_id="S1", account_id="MOCK-AUD-1", adapter_id="mock",
                       symbol="EURUSD", side="SELL", status=OrderStatus.WORKING, entry_price=Decimal("1.07"),
                       ordered_units=Decimal(1000), remaining_units=Decimal(1000), broker_updated_at_ms=1_000,
                       source="REST")
    cancelled = dataclasses.replace(working, status=OrderStatus.CANCELLED, broker_updated_at_ms=2_000)
    opened = PositionDTO(position_id="P9", account_id="MOCK-AUD-1", adapter_id="mock", symbol="EURUSD",
                         side="BUY", units=Decimal(1000), entry_price=Decimal("1.08"), status="OPEN",
                         broker_updated_at_ms=1_500, source="REST")
    adapter.change_while_off(working, cancelled, opened)
    h = await adapter.fetch_history(0, 3_000)
    assert [(o.broker_order_id, o.status) for o in h.orders] == [("S1", OrderStatus.CANCELLED)]
    assert [p.position_id for p in h.positions] == ["P9"]
    assert (await adapter.fetch_order_status("S1")) == cancelled          # the book follows the change
    assert "P9" in {p.position_id for p in await adapter.fetch_positions()}


async def test_mock_history_retention_and_scripted_incompleteness_fail_closed() -> None:
    adapter = MockBrokerAdapter.from_fixture(build_offline_history_fixture(), history_retention_ms=3_600_000)
    assert adapter.history_retention_ms == 3_600_000
    h = await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)
    assert not h.complete and h.covered_from_ms == OFF_UNTIL_MS - 3_600_000 and not h.retryable
    assert all(r.broker_updated_at_ms >= h.covered_from_ms for r in [*h.orders, *h.positions])
    adapter = MockBrokerAdapter.from_fixture(build_offline_history_fixture())
    adapter.set_history_incomplete("page capped")
    h = await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)
    assert not h.complete and h.notes == ("page capped",) and len(h.orders) == 4 and not h.retryable
    adapter.set_history_incomplete("page failed: TimeoutError", retryable=True)
    h = await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)
    assert not h.complete and h.retryable
    adapter.set_history_incomplete()
    assert (await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)).complete
    adapter.set_history_supported(False)
    h = await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)
    assert not h.complete and h.notes == ("history not supported",) and h.orders == ()


def test_an_incomplete_history_of_unknown_cause_is_retryable_by_default() -> None:
    """Fail closed: an adapter that does not classify its incompleteness gets its window re-read."""
    h = BrokerHistory("x", 0, 10, covered_from_ms=0, orders=(), positions=(), complete=False, notes=("?",))
    assert h.retryable and h.unmapped == 0
    notes = HistoryNotes()
    notes.limit("retention")
    assert not notes.retryable
    notes.append("page failed")
    assert notes.retryable and notes.unique() == ("retention", "page failed")


async def test_history_rows_count_on_the_read_never_on_the_live_stream_counters() -> None:
    """A row the adapter cannot map inside `history_read()` counts on that read; the same skip in another task
    (a live stream pump running meanwhile) still counts on `unmapped_rows`, so a live interruption survives."""
    adapter = MockBrokerAdapter.from_fixture(build_offline_history_fixture())
    adapter.set_history_unmapped(2)
    h = await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)
    assert not h.complete and h.retryable and h.unmapped == 2 and adapter.unmapped_rows["orders"] == 0
    with history_read() as outer:
        await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)              # nested: added to the outer read
        adapter.skip_row("orders", "a by-id answer read in this task")

        async def pump() -> None:                                          # a stream task spawned meanwhile
            adapter.skip_row("orders", "live row")

        await asyncio.create_task(pump())
    assert outer.unmapped == 3                   # 2 history rows + the skip in this task
    assert adapter.unmapped_rows["orders"] == 1  # the stream task's skip stays a live interruption


async def test_mock_history_is_a_scripted_rest_read() -> None:
    adapter = MockBrokerAdapter.from_fixture(build_offline_history_fixture())
    with rest_priority(RestPriority.RECONCILE):
        await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)
    assert adapter.rest_calls[-1] == ("fetch_history", RestPriority.RECONCILE)
    adapter.fail_next("fetch_history", BrokerError("scripted"))
    with pytest.raises(BrokerError):
        await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)
    assert (await adapter.fetch_history(OFF_SINCE_MS, OFF_UNTIL_MS)).complete


def test_from_fixture_still_refuses_unknown_arguments() -> None:
    with pytest.raises(TypeError):
        MockBrokerAdapter.from_fixture(build_fx_fixture(), bogus=1)
