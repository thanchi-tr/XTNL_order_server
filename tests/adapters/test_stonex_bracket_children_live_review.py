"""Live review 2026-11-24 (catch-up run 71, entry 1040256856 and its children 1040256857 / 1040256858): the REST
snapshot mapped the stop / limit children of a WORKING stop entry as untimed WORKING, while the stream had stated
them PENDING at the entry's broker time. The 2026-11-21 fix covered only children nested in the entry's IfDone list.

Now any child of an active, unfilled entry maps PENDING at the entry row's LastChangedDateTimeUTC, whatever the
shape: nested in IfDone, nested directly as StopOrder / LimitOrder, or listed as its own activestoplimitorders row
(with ParentOrderId, or linked by the entry's nesting). A row that states its own Status / time keeps it, and the
children of an open position stay WORKING.

fetch_history leaves untimed working rows out: they are present state the snapshot before the catch-up committed,
and the orchestrator refuses an untimed history row in a broker-time domain (incomplete, PAGE[high])."""
from __future__ import annotations

from typing import Any

import pytest

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


def child_row(oid: int, type_: int, parent: int | None = 970, **extra: Any) -> dict[str, Any]:
    """A bracket child listed as its own active row: no Status, no time (the live shape that went WORKING)."""
    return {"OrderId": oid, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 10000,
            "TriggerPrice": 1.083 if type_ == 2 else 1.075, "Type": type_, "ParentOrderId": parent,
            "TradingAccountId": int(TRADING), **extra}


DIRECT: dict[str, Any] = {"StopOrder": {"OrderId": 971, "TriggerPrice": 1.083}, "LimitOrder": {"OrderId": 972, "TriggerPrice": 1.075}}
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


def assert_bracket_pending(by_id: dict[str, OrderDTO], entry: str = "970", ts: int = T0) -> None:
    for cid, kind in (("971", "STOP"), ("972", "LIMIT")):
        child = by_id[cid]
        assert (child.status, child.broker_updated_at_ms, child.order_kind) == (OrderStatus.PENDING, ts, kind), cid
        assert child.is_reduce_only and child.linked_position_id == entry and child.source == "REST"
    assert by_id[entry].status == OrderStatus.WORKING and not by_id[entry].is_reduce_only
    assert by_id[entry].broker_updated_at_ms == ts


# ---- the shapes of a working entry's children -----------------------------------------------------------------
async def test_children_nested_directly_under_the_entrys_stop_and_limit_order_are_pending(
        fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(**DIRECT)]
    assert_bracket_pending(await open_orders(fake_broker))


@pytest.mark.parametrize("entry_first", [True, False])
async def test_children_listed_on_their_own_with_parent_order_id_are_pending(
        fake_broker: FakeStoneX, entry_first: bool) -> None:
    rows = [child_row(971, 2), child_row(972, 3)]
    fake_broker.actives = [entry_row(), *rows] if entry_first else [*rows, entry_row()]
    assert_bracket_pending(await open_orders(fake_broker))


@pytest.mark.parametrize("nesting", ["if_done", "direct"])
async def test_children_listed_on_their_own_and_nested_in_the_entry_are_pending(
        fake_broker: FakeStoneX, nesting: str) -> None:
    extra: dict[str, Any] = {"IfDone": IF_DONE} if nesting == "if_done" else dict(DIRECT)
    # the own rows name no parent: the entry's nesting links them (mapper memo)
    fake_broker.actives = [child_row(971, 2, parent=None), child_row(972, 3, parent=None), entry_row(**extra)]
    assert_bracket_pending(await open_orders(fake_broker))


async def test_an_own_row_that_states_its_status_and_time_keeps_them(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(), child_row(971, 2, Status=2, LastChangedDateTimeUTC=wcf(T0 + 5_000)),
                           child_row(972, 3, Status=2)]
    by_id = await open_orders(fake_broker)
    assert (by_id["971"].status, by_id["971"].broker_updated_at_ms) == (OrderStatus.WORKING, T0 + 5_000)
    # a stated status is kept; with no time of its own it takes the entry row's broker time
    assert (by_id["972"].status, by_id["972"].broker_updated_at_ms) == (OrderStatus.WORKING, T0)
    assert all(by_id[c].linked_position_id == "970" and by_id[c].is_reduce_only for c in ("971", "972"))


async def test_children_of_an_open_position_stay_working(fake_broker: FakeStoneX) -> None:
    fake_broker.positions = [{"OrderId": 900, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 10000,
                              "Price": 1.087, "TradingAccountId": int(TRADING),
                              "LastChangedDateTimeUTC": wcf(T0),
                              "StopOrder": {"OrderId": 901, "TriggerPrice": 1.08},
                              "LimitOrder": {"OrderId": 902, "TriggerPrice": 1.09}}]
    # the position's stop is also listed on actives (names its parent; no Status or time) and carries a nested
    # LimitOrder of its own: neither is the child of a working entry
    fake_broker.actives = [child_row(901, 2, parent=900, Direction="Sell",
                                     LimitOrder={"OrderId": 903, "TriggerPrice": 1.09})]
    by_id = await open_orders(fake_broker)
    for cid in ("901", "902", "903"):
        assert (by_id[cid].status, by_id[cid].broker_updated_at_ms) == (OrderStatus.WORKING, 0), cid
        assert by_id[cid].is_reduce_only
    assert by_id["901"].linked_position_id == "900" and by_id["902"].linked_position_id == "900"


async def test_a_child_row_whose_entry_is_not_listed_stays_working(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [child_row(971, 2, parent=960)]       # its parent is a filled position not read here
    by_id = await open_orders(fake_broker)
    assert (by_id["971"].status, by_id["971"].broker_updated_at_ms) == (OrderStatus.WORKING, 0)
    assert by_id["971"].linked_position_id == "960"


async def test_a_terminal_holder_row_does_not_make_its_children_pending(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(Status=3, **DIRECT)]       # stated Open (filled): not a working entry
    by_id = await open_orders(fake_broker)
    assert all(by_id[c].status == OrderStatus.WORKING for c in ("971", "972"))


# ---- fetch_active_stop_orders (ARM stop reads) uses the same mapping ---------------------------------------------
async def test_stop_reads_map_directly_nested_children_pending(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(**DIRECT)]
    assert_bracket_pending(await stop_reads(fake_broker))


async def test_stop_reads_map_own_child_rows_pending(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [child_row(971, 2), entry_row(), child_row(972, 3)]
    assert_bracket_pending(await stop_reads(fake_broker))


# ---- fetch_history: no untimed working row is offered to the catch-up -------------------------------------------
async def history(fake: FakeStoneX) -> Any:
    adapter: StoneXAdapter = make_adapter(fake, stop_limit_history=True)
    await adapter.connect()
    try:
        return await adapter.fetch_history(T0 - 60_000, UNTIL)
    finally:
        await adapter.close()


async def test_history_leaves_out_an_open_positions_untimed_children(fake_broker: FakeStoneX) -> None:
    fake_broker.history = [trade(900, T0 + 60_000)]            # opened in the window, still open
    fake_broker.positions = [{"OrderId": 900, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 10000,
                              "Price": 1.087, "TradingAccountId": int(TRADING),
                              "LastChangedDateTimeUTC": wcf(T0 + 60_000),
                              "StopOrder": {"OrderId": 901, "TriggerPrice": 1.08},
                              "LimitOrder": {"OrderId": 902, "TriggerPrice": 1.09}}]
    h = await history(fake_broker)
    assert not any(o.broker_updated_at_ms == 0 for o in h.orders)
    assert {o.broker_order_id for o in h.orders}.isdisjoint({"901", "902"})
    assert [p.position_id for p in h.positions] == ["900"]
    assert h.complete and h.notes == ()


@pytest.mark.parametrize("shape", ["direct", "own_rows"])
async def test_history_keeps_a_timed_entrys_children_at_the_entry_time(fake_broker: FakeStoneX, shape: str) -> None:
    fake_broker.actives = ([entry_row(**DIRECT)] if shape == "direct"
                           else [entry_row(), child_row(971, 2), child_row(972, 3)])
    h = await history(fake_broker)
    by_id = {o.broker_order_id: o for o in h.orders}
    assert set(by_id) == {"970", "971", "972"}
    assert_bracket_pending(by_id)
    assert h.complete and h.notes == ()


async def test_history_leaves_out_an_untimed_entry_and_its_children(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(ts=None, IfDone=IF_DONE)]
    h = await history(fake_broker)
    assert h.orders == () and h.complete and h.notes == ()


async def test_history_still_drops_a_timed_working_row_before_the_window(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [entry_row(ts=T0 - 86_400_000, **DIRECT)]
    h = await history(fake_broker)
    assert h.orders == () and h.complete
