"""IMPLEMENTATION_PLAN 8.1: StoneX by-id REST reads (`fetch_order_status` / `fetch_position_status`) of a row the
adapter cannot map count in `unmapped_rows` and raise, never answer None ("the broker has none") and never invent
a status; only an explicit broker "not found" is recorded as absence (`note_absent` / `absence_probe`)."""
from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from src.adapters.base import BrokerError, absence_probe, note_absent
from src.adapters.mock import MockBrokerAdapter
from src.adapters.stonex.mapping import UnmappableError, map_stop_limit_order, map_trade_order
from src.dto import OrderStatus

from .conftest import EURUSD_ID, TRADING, FakeStoneX, make_adapter, wcf
from .pepperstone.conftest import FakeCTrader
from .pepperstone.conftest import make_adapter as make_pp_adapter

T0 = 1_700_000_000_000
ORDER_PATH = "/TradingAPI/order/{}"
POSITION_PATH = "/TradingAPI/order/{}/openposition"


def _trade(order_id: int = 900, **over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"OrderId": order_id, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 1000,
                           "Price": 1.08, "Status": 3, "TradingAccountId": int(TRADING),
                           "LastChangedDateTimeUTC": wcf(T0)}
    row.update(over)
    return {k: v for k, v in row.items() if v is not ...}


def _stop_limit(order_id: int = 910, **over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"OrderId": order_id, "MarketId": int(EURUSD_ID), "Direction": "Sell", "Quantity": 1000,
                           "TriggerPrice": 1.07, "Status": 2, "Type": "Stop", "TradingAccountId": int(TRADING),
                           "ParentOrderId": 5, "LastChangedDateTimeUTC": wcf(T0)}
    row.update(over)
    return {k: v for k, v in row.items() if v is not ...}


def _lookup(_market_id: str | None) -> None:
    return None


@pytest.fixture
def fake() -> FakeCTrader:
    return FakeCTrader()


# ===================================================================== mapping
def test_map_trade_order_raises_instead_of_returning_none() -> None:
    for bad, why in ((_trade(Status=7), "unrecognised Status"), (_trade(Status=...), "unrecognised Status"),
                     (_trade(Direction=...), "OrderId / Direction"), (_trade(OrderId=...), "OrderId / Direction")):
        with pytest.raises(UnmappableError, match=why):
            map_trade_order(bad, account_id="C", lookup=_lookup, reduce_only=False, linked_position_id=None)


def test_map_stop_limit_order_strict_status_never_invents_working() -> None:
    for status in (7, ...):
        raw = _stop_limit(Status=status)
        # the active list (and a position's nested child) keeps the documented WORKING default ...
        assert map_stop_limit_order(raw, account_id="C", lookup=_lookup, parent_id="5").status == OrderStatus.WORKING
        # ... a by-id read, which also returns orders that are no longer active, does not
        with pytest.raises(UnmappableError, match="unrecognised Status"):
            map_stop_limit_order(raw, account_id="C", lookup=_lookup, parent_id="5", strict_status=True)
    ok = map_stop_limit_order(_stop_limit(Status=4), account_id="C", lookup=_lookup, parent_id="5", strict_status=True)
    assert ok.status != OrderStatus.WORKING


# ===================================================================== StoneX by-id reads
async def test_stonex_trade_order_it_cannot_map_is_counted_and_raises(fake_broker: FakeStoneX) -> None:
    fake_broker.orders["901"] = {"TradeOrder": _trade(901, Status=7), "StopLimitOrder": None}
    fake_broker.orders["902"] = {"TradeOrder": _trade(902, Status=...), "StopLimitOrder": None}
    fake_broker.orders["903"] = {"TradeOrder": _trade(903, Direction=...), "StopLimitOrder": None}
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    try:
        for n, oid in enumerate(("901", "902", "903"), start=1):
            with absence_probe() as probe, pytest.raises(BrokerError, match="unmappable"):
                await adapter.fetch_order_status(oid)
            assert not probe.absent("ORDER", oid)
            assert adapter.unmapped_rows["orders"] == n
    finally:
        await adapter.close()


async def test_stonex_by_id_stop_limit_with_unrecognised_status_is_not_working(fake_broker: FakeStoneX) -> None:
    fake_broker.orders["911"] = {"StopLimitOrder": _stop_limit(911, Status=7), "TradeOrder": None}
    fake_broker.orders["912"] = {"StopLimitOrder": _stop_limit(912, Status=...), "TradeOrder": None}
    fake_broker.orders["913"] = {"StopLimitOrder": _stop_limit(913, Status=2), "TradeOrder": None}
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    try:
        for oid in ("911", "912"):
            with pytest.raises(BrokerError, match="unmappable"):
                await adapter.fetch_order_status(oid)
        assert adapter.unmapped_rows["orders"] == 2
        ok = await adapter.fetch_order_status("913")
        assert ok is not None and ok.status == OrderStatus.WORKING and adapter.unmapped_rows["orders"] == 2
    finally:
        await adapter.close()


async def test_stonex_by_id_explicit_not_found_is_noted_and_nothing_else_is(fake_broker: FakeStoneX) -> None:
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    try:
        # explicit: both objects null (the fake's default answer), HTTP 404
        with absence_probe() as probe:
            assert await adapter.fetch_order_status("990") is None
            fake_broker.one_shot[ORDER_PATH.format(991)] = httpx.Response(404, json={"HttpStatus": 404})
            assert await adapter.fetch_order_status("991") is None
            assert await adapter.fetch_position_status("992") is None             # {"OpenPosition": null}
            fake_broker.one_shot[POSITION_PATH.format(993)] = httpx.Response(404, json={"HttpStatus": 404})
            assert await adapter.fetch_position_status("993") is None
        assert probe.absent("ORDER", "990") and probe.absent("ORDER", "991")
        assert probe.absent("POSITION", "992") and probe.absent("POSITION", "993")
        assert not probe.absent("POSITION", "990") and not probe.absent("ORDER", "992")   # per (kind, id)
        assert adapter.unmapped_rows["orders"] == 0

        # bodies that prove nothing: never None, counted, raise
        with absence_probe() as probe:
            fake_broker.orders["994"] = {}                                         # neither key present
            fake_broker.orders["995"] = {"TradeOrder": None}                       # StopLimitOrder not stated
            fake_broker.orders["996"] = {"TradeOrder": "x", "StopLimitOrder": None}   # not a row
            for oid in ("994", "995", "996"):
                with pytest.raises(BrokerError, match="unmappable"):
                    await adapter.fetch_order_status(oid)
            fake_broker.one_shot[POSITION_PATH.format(997)] = httpx.Response(200, json={"Other": 1})
            with pytest.raises(BrokerError, match="unmappable"):
                await adapter.fetch_position_status("997")
        assert adapter.unmapped_rows["orders"] == 4
        assert not any(probe.absent(k, i) for k in ("ORDER", "POSITION") for i in ("994", "995", "996", "997"))

        # an empty 2xx body is not a 404: it raises (not counted: there is no row to count)
        for path, read in ((ORDER_PATH.format(998), adapter.fetch_order_status),
                           (POSITION_PATH.format(998), adapter.fetch_position_status)):
            fake_broker.one_shot[path] = httpx.Response(200, content=b"")
            with absence_probe() as probe, pytest.raises(BrokerError, match="empty body"):
                await read("998")
            assert not probe.absent("ORDER", "998") and not probe.absent("POSITION", "998")
    finally:
        await adapter.close()


async def test_stonex_unmappable_open_position_by_id_stays_counted(fake_broker: FakeStoneX) -> None:
    fake_broker.order_positions["700"] = {"OrderId": 700, "MarketId": int(EURUSD_ID), "Quantity": 1000,
                                          "TradingAccountId": int(TRADING)}          # no Direction
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    try:
        with absence_probe() as probe, pytest.raises(BrokerError, match="unmappable"):
            await adapter.fetch_position_status("700")
        assert not probe.absent("POSITION", "700") and adapter.unmapped_rows["orders"] == 1
    finally:
        await adapter.close()


async def test_stonex_by_id_row_of_an_unknown_trading_account_is_counted_and_raises(fake_broker: FakeStoneX) -> None:
    """Skeptic 8.1: a by-id row whose TradingAccountId is missing or not in the directory cannot be mapped (the
    stream path raises UnmappableError for it): it counts in unmapped_rows and raises, never answers None.
    Two client accounts, so the directory's single-account fallback does not apply."""
    fake_broker.account_info["clientAccounts"].append({"clientAccountId": 1002, "tradingAccountId": 2002,
                                                       "clientAccountCurrency": "AUD"})
    fake_broker.account_info["TradingAccounts"].append({"ClientAccountId": 1002, "TradingAccountId": 2002,
                                                        "TradingAccountCode": "TA2", "TradingAccountStatus": "Open",
                                                        "TradingAccountType": "CFD"})
    fake_broker.orders["920"] = {"TradeOrder": _trade(920, TradingAccountId=424242), "StopLimitOrder": None}
    fake_broker.orders["921"] = {"StopLimitOrder": _stop_limit(921, TradingAccountId=...), "TradeOrder": None}
    fake_broker.order_positions["922"] = {"OrderId": 922, "MarketId": int(EURUSD_ID), "Direction": "Buy",
                                          "Quantity": 1000, "TradingAccountId": 424242}
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    try:
        with absence_probe() as probe:
            for oid in ("920", "921"):
                with pytest.raises(BrokerError, match="unmappable: unknown trading account"):
                    await adapter.fetch_order_status(oid)
            with pytest.raises(BrokerError, match="unmappable: unknown trading account"):
                await adapter.fetch_position_status("922")
        assert adapter.unmapped_rows["orders"] == 3
        assert not any(probe.absent(k, i) for k in ("ORDER", "POSITION") for i in ("920", "921", "922"))
    finally:
        await adapter.close()


# ===================================================================== probe semantics
async def test_absence_probe_is_per_context_and_inert_outside_a_block() -> None:
    note_absent("ORDER", "X")                                   # no probe open: a no-op
    with absence_probe() as outer:
        with absence_probe() as inner:
            note_absent("ORDER", "A")
        note_absent("ORDER", "B")

        async def other_task() -> None:                         # a concurrent caller has its own context
            with absence_probe():
                note_absent("ORDER", "C")
        await asyncio.create_task(other_task())
    assert inner.absent("ORDER", "A") and not inner.absent("ORDER", "B")
    assert outer.absent("ORDER", "B") and not outer.absent("ORDER", "A") and not outer.absent("ORDER", "C")


async def test_mock_and_pepperstone_note_only_explicit_not_found(fake: FakeCTrader) -> None:
    mock = MockBrokerAdapter(adapter_id="mock")
    with absence_probe() as probe:
        assert await mock.fetch_order_status("M1") is None
        assert await mock.fetch_position_status("M2") is None
    assert probe.absent("ORDER", "M1") and probe.absent("POSITION", "M2")

    adapter = make_pp_adapter(fake)
    await adapter.connect()
    await adapter.fetch_instruments()
    try:
        with absence_probe() as probe:
            assert await adapter.fetch_order_status("999") is None        # ORDER_NOT_FOUND
            assert await adapter.fetch_order_status("not-a-number") is None
            assert await adapter.fetch_position_status("12345") is None   # neither reconcile nor deals prove it
        assert probe.absent("ORDER", "999")
        assert not probe.absent("ORDER", "not-a-number") and not probe.absent("POSITION", "12345")
    finally:
        await adapter.close()


async def test_a_closed_position_answering_403_80001_reads_as_no_longer_open_without_an_absence(
        fake_broker: FakeStoneX) -> None:
    """Live finding (2026-11-14): StoneX answers GET order/{id}/openposition of a CLOSED position with HTTP 403 /
    ErrorCode 80001. Treated as an error, it failed every snapshot (the stored OPEN position is re-read by id) and
    SM4 looped forever after a position closed while the engine was off. It now reads as "no longer open" (None),
    but never as the explicit not-found the DLQ may act on; any other 403 still raises."""
    fake_broker.closed_order_ids.add("1038847936")
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    try:
        with absence_probe() as probe:
            assert await adapter.fetch_position_status("1038847936") is None
        assert not probe.absent("POSITION", "1038847936")
        fake_broker.one_shot["/TradingAPI/order/555/openposition"] = httpx.Response(
            403, json={"HttpStatus": 403, "ErrorCode": 9999, "ErrorMessage": "forbidden"})
        with pytest.raises(BrokerError, match="403"):
            await adapter.fetch_position_status("555")
    finally:
        await adapter.close()
