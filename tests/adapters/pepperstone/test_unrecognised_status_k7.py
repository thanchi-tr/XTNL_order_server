"""K7 (adapter-isolation.md §1): a cTrader order / position with an unrecognised status is never mapped to an
invented status. It is counted as unmappable (``unmapped_rows``) and, on a stream, opens that stream's gap."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, TypeVar

import pytest

from src.adapters.base import StreamEvent, StreamEventKind
from src.adapters.pepperstone import PepperstoneAdapter
from src.adapters.pepperstone.mapping import UnmappableError, map_order, map_position
from src.dto import OrderStatus

from .conftest import ACCOUNT, EURUSD, FakeCTrader, make_adapter, order, position

T = TypeVar("T")


def _lookup(_: int | None) -> None:
    return None


async def take(gen: AsyncIterator[T], n: int, timeout: float = 2.0) -> list[T]:
    return [await asyncio.wait_for(gen.__anext__(), timeout) for _ in range(n)]


async def connected(fake: FakeCTrader) -> PepperstoneAdapter:
    adapter = make_adapter(fake)
    await adapter.connect()
    await adapter.fetch_instruments()
    return adapter


@pytest.mark.parametrize("status", ["ORDER_STATUS_SOMETHING_NEW", 9, "42", "garbage"])
def test_k7_unrecognised_order_status_is_unmappable(status: Any) -> None:
    with pytest.raises(UnmappableError, match="unrecognised status"):
        map_order(order(status=status), account_id=str(ACCOUNT), lookup=_lookup, source="REST")


@pytest.mark.parametrize("status", [None, "", "ORDER_STATUS_ACCEPTED", 1, "ACCEPTED"])
def test_k7_absent_or_known_order_status_still_maps(status: Any) -> None:
    row = order()
    if status is None:
        del row["orderStatus"]
    else:
        row["orderStatus"] = status
    dto = map_order(row, account_id=str(ACCOUNT), lookup=_lookup, source="REST")
    assert dto.status == OrderStatus.WORKING                 # ACCEPTED, or the proto default when absent


@pytest.mark.parametrize("status", [7, "POSITION_STATUS_UNKNOWN_NEW", "x"])
def test_k7_unrecognised_position_status_is_unmappable(status: Any) -> None:
    with pytest.raises(UnmappableError, match="unrecognised status"):
        map_position(position(status=status), account_id=str(ACCOUNT), lookup=_lookup, source="REST")


def test_k7_created_and_error_positions_are_still_skipped_not_unmappable() -> None:
    for status in (3, 4, "POSITION_STATUS_CREATED", "POSITION_STATUS_ERROR"):
        assert map_position(position(status=status), account_id=str(ACCOUNT), lookup=_lookup, source="REST") is None


async def test_k7_snapshot_rows_with_unrecognised_status_are_counted(fake: FakeCTrader) -> None:
    fake.orders = [order(701), order(702, status="ORDER_STATUS_MYSTERY")]
    fake.positions = [position(501), position(502, status=99)]
    adapter = await connected(fake)
    try:
        before = adapter.unmapped_rows.get("orders", 0)
        orders = await adapter.fetch_open_orders()
        positions = await adapter.fetch_positions()
        assert [o.broker_order_id for o in orders] == ["701"]
        assert [p.position_id for p in positions] == ["501"]
        assert adapter.unmapped_rows["orders"] == before + 2    # the orchestrator opens the orders gap on growth
    finally:
        await adapter.close()


async def test_k7_stream_rows_with_unrecognised_status_open_a_gap(fake: FakeCTrader) -> None:
    adapter = await connected(fake)
    orders = adapter.stream_order_updates()
    positions = adapter.stream_positions()
    try:
        for gen in (orders, positions):
            [ev] = await take(gen, 1)
            assert isinstance(ev, StreamEvent) and ev.kind == StreamEventKind.SUBSCRIBED
        await adapter._ensure_symbols([EURUSD])
        before = adapter.unmapped_rows.get("orders", 0)
        fake.push(2126, {"ctidTraderAccountId": ACCOUNT, "executionType": "ORDER_ACCEPTED",
                         "order": order(801, status="ORDER_STATUS_FROM_THE_FUTURE")})
        [gap] = await take(orders, 1)
        assert isinstance(gap, StreamEvent) and gap.kind == StreamEventKind.GAP and "unmappable" in gap.detail
        fake.push(2126, {"ctidTraderAccountId": ACCOUNT, "executionType": "ORDER_FILLED",
                         "position": position(601, status=77)})
        [gap_p] = await take(positions, 1)
        assert isinstance(gap_p, StreamEvent) and gap_p.kind == StreamEventKind.GAP
        assert adapter.unmapped_rows["orders"] >= before + 2
    finally:
        await adapter.close()
