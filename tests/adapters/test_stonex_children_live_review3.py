"""Live review round 3 (2026-11-25): round 1's widening of the StoneX accepted deviation and its fetch_history filter
are reverted.

- ``adapter-isolation.md`` accepted deviation (2026-11-21) covers one shape only: a child nested in a **working
  entry's IfDone** list maps ``PENDING`` at the entry row's ``LastChangedDateTimeUTC``. Any other child of a working
  entry (nested directly as StopOrder / LimitOrder, or listed as its own ``activestoplimitorders`` row) keeps the
  plain active-list mapping: ``WORKING`` with no broker time of its own. The adapter never invents a status
  (IMPLEMENTATION_PLAN 8.1); the real REST shape of those children was never captured.
- ``adapter-isolation.md`` §4: ``fetch_history`` lists every order whose broker state changed in the window,
  terminal or not. An untimed working row cannot be placed in or out of the window, so it is offered; the
  orchestrator skips it as unversioned and the catch-up is incomplete (``operations.md`` §17 step 3,
  ``oversee-rules.md`` §8 ``catchup_unverified``)."""
from __future__ import annotations

from typing import Any

import pytest

from src.adapters.base import BrokerHistory
from src.adapters.stonex.adapter import StoneXAdapter
from src.dto import OrderDTO, OrderStatus

from .conftest import EURUSD_ID, TRADING, FakeStoneX, make_adapter, wcf
from .test_stonex_history import trade

T0 = 1_767_225_600_000
UNTIL = T0 + 3_600_000


def entry_row(oid: int = 970, *, ts: int | None = T0, **extra: Any) -> dict[str, Any]:
    """A working sell-stop entry as listed on activestoplimitorders (no Status of its own)."""
    row: dict[str, Any] = {"OrderId": oid, "MarketId": int(EURUSD_ID), "Direction": "Sell", "Quantity": 10000,
                           "TriggerPrice": 1.08, "Type": 2, "ParentOrderId": None,
                           "TradingAccountId": int(TRADING), **extra}
    if ts is not None:
        row["LastChangedDateTimeUTC"] = wcf(ts)
    return row


def child_row(oid: int, type_: int, parent: int | None = 970) -> dict[str, Any]:
    """A bracket child listed as its own active row, with no Status and no time."""
    return {"OrderId": oid, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 10000,
            "TriggerPrice": 1.083 if type_ == 2 else 1.075, "Type": type_, "ParentOrderId": parent,
            "TradingAccountId": int(TRADING)}


DIRECT: dict[str, Any] = {"StopOrder": {"OrderId": 971, "TriggerPrice": 1.083},
                          "LimitOrder": {"OrderId": 972, "TriggerPrice": 1.075}}
IF_DONE = [{"Stop": {"OrderId": 971, "TriggerPrice": 1.083}}, {"Limit": {"OrderId": 972, "TriggerPrice": 1.075}}]


async def open_orders(fake: FakeStoneX) -> dict[str, OrderDTO]:
    adapter = make_adapter(fake)
    await adapter.connect()
    try:
        return {o.broker_order_id: o for o in await adapter.fetch_open_orders()}
    finally:
        await adapter.close()


async def stop_reads(fake: FakeStoneX) -> dict[str, OrderDTO]:
    adapter = make_adapter(fake)
    await adapter.connect()
    try:
        return {o.broker_order_id: o for o in (await adapter.fetch_active_stop_orders()).orders}
    finally:
        await adapter.close()


async def history(fake: FakeStoneX) -> BrokerHistory:
    adapter: StoneXAdapter = make_adapter(fake, stop_limit_history=True)
    await adapter.connect()
    try:
        return await adapter.fetch_history(T0 - 60_000, UNTIL)
    finally:
        await adapter.close()


def assert_children(by_id: dict[str, OrderDTO], status: OrderStatus, ts: int, entry: str = "970") -> None:
    for cid, kind in (("971", "STOP"), ("972", "LIMIT")):
        child = by_id[cid]
        assert (child.status, child.broker_updated_at_ms, child.order_kind) == (status, ts, kind), cid
        assert child.is_reduce_only and child.linked_position_id == entry and child.source == "REST"
    assert by_id[entry].status == OrderStatus.WORKING and not by_id[entry].is_reduce_only


# ---- the accepted deviation: the IfDone shape only ---------------------------------------------------------------
async def test_if_done_children_of_a_working_entry_are_pending_at_the_entry_time(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(IfDone=IF_DONE)]
    assert_children(await open_orders(fake_broker), OrderStatus.PENDING, T0)


async def test_directly_nested_children_of_a_working_entry_stay_untimed_working(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(**DIRECT)]
    assert_children(await open_orders(fake_broker), OrderStatus.WORKING, 0)


@pytest.mark.parametrize("entry_first", [True, False])
async def test_own_row_children_of_a_working_entry_stay_untimed_working(
        fake_broker: FakeStoneX, entry_first: bool) -> None:
    rows = [child_row(971, 2), child_row(972, 3)]
    fake_broker.actives = [entry_row(), *rows] if entry_first else [*rows, entry_row()]
    assert_children(await open_orders(fake_broker), OrderStatus.WORKING, 0)


async def test_own_rows_linked_by_the_entrys_direct_nesting_stay_untimed_working(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [child_row(971, 2, parent=None), child_row(972, 3, parent=None), entry_row(**DIRECT)]
    assert_children(await open_orders(fake_broker), OrderStatus.WORKING, 0)


async def test_stop_reads_keep_the_if_done_deviation_only(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(IfDone=IF_DONE)]
    assert_children(await stop_reads(fake_broker), OrderStatus.PENDING, T0)
    fake_broker.actives = [entry_row(**DIRECT)]
    assert_children(await stop_reads(fake_broker), OrderStatus.WORKING, 0)
    fake_broker.actives = [child_row(971, 2), entry_row(), child_row(972, 3)]
    assert_children(await stop_reads(fake_broker), OrderStatus.WORKING, 0)


# ---- fetch_history offers untimed working rows (the orchestrator skips and reports them) --------------------------
async def test_history_offers_an_open_positions_untimed_children(fake_broker: FakeStoneX) -> None:
    fake_broker.history = [trade(900, T0 + 60_000)]            # opened in the window, still open
    fake_broker.positions = [{"OrderId": 900, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 10000,
                              "Price": 1.087, "TradingAccountId": int(TRADING),
                              "LastChangedDateTimeUTC": wcf(T0 + 60_000),
                              "StopOrder": {"OrderId": 901, "TriggerPrice": 1.08},
                              "LimitOrder": {"OrderId": 902, "TriggerPrice": 1.09}}]
    h = await history(fake_broker)
    untimed = {o.broker_order_id for o in h.orders if o.broker_updated_at_ms == 0}
    assert untimed == {"901", "902"}
    assert all(o.status == OrderStatus.WORKING for o in h.orders if o.broker_order_id in untimed)
    assert [p.position_id for p in h.positions] == ["900"]


async def test_history_offers_an_untimed_entry_and_its_if_done_children(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(ts=None, IfDone=IF_DONE)]
    h = await history(fake_broker)
    assert {o.broker_order_id: o.broker_updated_at_ms for o in h.orders} == {"970": 0, "971": 0, "972": 0}


async def test_history_offers_own_row_children_of_a_timed_entry_untimed(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(), child_row(971, 2), child_row(972, 3)]
    by_id = {o.broker_order_id: o for o in (await history(fake_broker)).orders}
    assert set(by_id) == {"970", "971", "972"} and by_id["970"].broker_updated_at_ms == T0
    assert_children(by_id, OrderStatus.WORKING, 0)


async def test_history_keeps_if_done_children_of_a_timed_entry_at_the_entry_time(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(IfDone=IF_DONE)]
    by_id = {o.broker_order_id: o for o in (await history(fake_broker)).orders}
    assert set(by_id) == {"970", "971", "972"}
    assert_children(by_id, OrderStatus.PENDING, T0)


async def test_history_still_drops_a_timed_working_row_before_the_window(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(ts=T0 - 86_400_000, IfDone=IF_DONE)]
    assert (await history(fake_broker)).orders == ()
