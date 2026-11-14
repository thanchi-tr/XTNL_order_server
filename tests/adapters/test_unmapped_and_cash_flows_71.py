"""IMPLEMENTATION_PLAN 7.1 (adapters): every unmappable broker row (stream or REST) counts in `unmapped_rows`, a
by-id read of an unmappable row never answers "the broker has none", and a partial `fetch_cash_flows` result is a
`CashFlowBatch` with `partial_reasons` (never one that looks complete). No network I/O."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from src.adapters.base import (
    BrokerError,
    CashFlowBatch,
    StreamEvent,
    StreamEventKind,
    cash_flows_partial,
    new_unmapped_rows,
    note_unmapped,
)
from src.adapters.mock import MockBrokerAdapter
from src.adapters.pepperstone import adapter as pp_adapter_mod
from src.adapters.pepperstone.protocol import Frame
from src.adapters.stonex.mapping import map_trade_history, map_trade_history_checked
from src.adapters.stonex.streaming import _SubscriptionListener
from src.dto import CashFlowDTO

from .conftest import EURUSD_ID, TRADING, FakeStoneX, make_adapter, wcf
from .pepperstone.conftest import ACCOUNT, T0 as PP_T0, FakeCTrader, order, position, wait_until
from .pepperstone.conftest import make_adapter as make_pp_adapter

T0 = 1_767_225_600_000


# ===================================================================== base helpers
def test_cash_flow_batch_is_a_list_that_carries_its_partial_reasons() -> None:
    cf = CashFlowDTO(transaction_id="t1", account_id="A", adapter_id="mock", kind="DEPOSIT", amount=Decimal(1),
                     currency="AUD", broker_ts=1)
    complete = CashFlowBatch([cf])
    assert complete == [cf] and not complete.partial and cash_flows_partial(complete) == ()
    partial = CashFlowBatch([cf], ["page capped", "page capped", "row unmappable"])
    assert partial == [cf] and partial.partial
    assert cash_flows_partial(partial) == ("page capped", "row unmappable")       # de-duplicated, ordered
    assert cash_flows_partial([cf]) == ()                                       # plain list: complete


def test_a_malformed_partial_marker_reads_as_partial() -> None:
    class Odd(list[CashFlowDTO]):
        partial_reasons = "yes"

    assert cash_flows_partial(Odd()) == ("partial marker unreadable",)


def test_note_unmapped_counts_per_stream() -> None:
    counters = new_unmapped_rows()
    assert counters == {"orders": 0, "account": 0, "prices": 0}
    note_unmapped(counters, "x", "orders", "why")
    note_unmapped(counters, "x", "orders", "why")
    note_unmapped(counters, "x", "account", "why")
    assert counters == {"orders": 2, "account": 1, "prices": 0}


# ===================================================================== StoneX: REST rows
def _bad_position(order_id: int = 700) -> dict[str, Any]:
    return {"OrderId": order_id, "MarketId": int(EURUSD_ID), "Quantity": 1000, "Price": 1.08,   # no Direction
            "TradingAccountId": int(TRADING), "LastChangedDateTimeUTC": wcf(T0)}


async def test_stonex_rest_rows_it_cannot_map_are_counted(fake_broker: FakeStoneX) -> None:
    fake_broker.positions = [_bad_position()]
    fake_broker.actives = [
        {"OrderId": None, "MarketId": int(EURUSD_ID), "Direction": "Sell", "Quantity": 1000, "TriggerPrice": 1.08,
         "TradingAccountId": int(TRADING), "Type": "Stop", "Status": 2},                    # no OrderId
        {"OrderId": 801, "MarketId": int(EURUSD_ID), "Quantity": 1000, "TriggerPrice": 1.08,
         "TradingAccountId": int(TRADING), "Type": "Stop", "Status": 2},                    # no Direction
    ]
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    assert adapter.unmapped_rows == {"orders": 0, "account": 0, "prices": 0}
    assert await adapter.fetch_positions() == []
    assert adapter.unmapped_rows["orders"] == 1
    await adapter.fetch_open_orders()
    # the bad position is read again by fetch_open_orders (positions carry attached orders) but only the two
    # active rows count there: the position row itself is not mapped by fetch_open_orders
    assert adapter.unmapped_rows["orders"] == 3
    await adapter.close()


async def test_stonex_by_id_read_of_an_unmappable_row_raises_instead_of_answering_none(
        fake_broker: FakeStoneX) -> None:
    fake_broker.order_positions["700"] = _bad_position()
    fake_broker.orders["777"] = {"StopLimitOrder": {"OrderId": 777, "MarketId": int(EURUSD_ID), "Quantity": 1000,
                                                    "TriggerPrice": 1.08, "Status": 2,
                                                    "TradingAccountId": int(TRADING), "ParentOrderId": 5},
                                 "TradeOrder": None}
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    with pytest.raises(BrokerError, match="unmappable"):
        await adapter.fetch_position_status("700")
    with pytest.raises(BrokerError, match="unmappable"):
        await adapter.fetch_order_status("777")
    assert adapter.unmapped_rows["orders"] == 2
    assert await adapter.fetch_order_status("999") is None          # a real "none" stays None
    await adapter.close()


async def test_stonex_margin_without_cash_counts_on_the_account_stream(fake_broker: FakeStoneX) -> None:
    fake_broker.margin = {"NetEquity": 1}
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    with pytest.raises(BrokerError):
        await adapter.fetch_account_snapshot()
    assert adapter.unmapped_rows["account"] == 1
    await adapter.close()


async def test_stonex_unmappable_stream_frames_are_counted_and_open_a_gap(fake_broker: FakeStoneX) -> None:
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    out = await adapter._map_order_frame({"Status": "1"})                     # no OrderId
    assert [type(x) for x in out] == [StreamEvent] and out[0].kind is StreamEventKind.GAP
    assert adapter.unmapped_rows["orders"] == 1
    out = await adapter._map_order_frame({"OrderId": "5", "Status": "1", "MarketId": EURUSD_ID,
                                          "TradingAccountId": TRADING, "LastChangedTime": wcf(T0)})   # no Direction
    assert any(isinstance(x, StreamEvent) and x.kind is StreamEventKind.GAP for x in out)
    assert adapter.unmapped_rows["orders"] == 2
    directory = adapter.account_directory
    assert directory is not None
    out_a = adapter._map_account_frame(directory, {"_client": directory.client_ids[0], "NetEquity": "1"})
    assert [x.kind for x in out_a if isinstance(x, StreamEvent)] == [StreamEventKind.GAP]
    assert adapter.unmapped_rows["account"] == 1
    await adapter.close()


def test_a_malformed_lightstreamer_update_counts_as_a_lost_frame() -> None:
    class Sink:
        lost: list[int] = []

        def on_frame(self, values: dict[str, Any]) -> None: ...

        def on_event(self, kind: StreamEventKind, detail: str) -> None: ...

        def on_lost(self, count: int) -> None:
            self.lost.append(count)

    class Broken:
        def getValue(self, name: str) -> str:
            raise RuntimeError("bad update")

    sink = Sink()
    _SubscriptionListener(("OrderId",), sink).onItemUpdate(Broken())     # never raises into the library thread
    assert sink.lost == [1]


# ===================================================================== StoneX: cash flows
def test_map_trade_history_reports_closing_rows_it_cannot_map() -> None:
    rows = [
        {"OrderId": 1, "OpeningOrderIds": [0], "RealisedPnl": 10, "RealisedPnlCurrency": "AUD",
         "ExecutedDateTimeUtc": wcf(T0 + 1)},
        {"OrderId": 2, "OpeningOrderIds": [0], "RealisedPnlCurrency": "AUD",
         "ExecutedDateTimeUtc": wcf(T0 + 2)},                                     # closing, no P&L
        {"OrderId": 3, "RealisedPnl": 5, "RealisedPnlCurrency": "AUD"},           # closing, no time
        {"OrderId": 4, "OpeningOrderIds": [0], "RealisedPnl": 1, "ExecutedDateTimeUtc": wcf(T0 + 3)},   # no ccy
        {"OrderId": 5, "OpeningOrderIds": [0], "ExecutedDateTimeUtc": wcf(T0 - 10)},   # before since: not ours
        {"OrderId": 6, "OpeningOrderIds": [], "ExecutedDateTimeUtc": wcf(T0 + 4)},     # an opening, no P&L
    ]
    flows, skipped = map_trade_history_checked(rows, account_id="A", since_ms=T0)
    assert [f.transaction_id for f in flows] == ["stonex:trade:1"]
    assert len(skipped) == 3 and "RealisedPnl" in skipped[0] and "ExecutedDateTimeUtc" in skipped[1]
    assert "currency" in skipped[2]
    assert map_trade_history(rows, account_id="A", since_ms=T0) == flows       # old API unchanged


async def test_stonex_cash_flows_are_partial_when_the_page_is_capped_or_a_row_is_unmappable(
        fake_broker: FakeStoneX, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.adapters.stonex import adapter as sx_mod

    good = {"OrderId": 9, "OpeningOrderIds": [8], "RealisedPnl": 33.5, "RealisedPnlCurrency": "AUD",
            "ExecutedDateTimeUtc": wcf(T0 + 5)}
    fake_broker.history = [good]
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    flows = await adapter.fetch_cash_flows(T0)
    assert isinstance(flows, CashFlowBatch) and not flows.partial and len(flows) == 1

    fake_broker.history = [good, {"OrderId": 10, "OpeningOrderIds": [7], "RealisedPnlCurrency": "AUD",
                                  "ExecutedDateTimeUtc": wcf(T0 + 6)}]
    flows = await adapter.fetch_cash_flows(T0)
    assert [f.transaction_id for f in flows] == ["stonex:trade:9"]
    assert flows.partial and "closing trade 10" in flows.partial_reasons[0]

    monkeypatch.setattr(sx_mod, "TRADE_HISTORY_MAX", 1)
    fake_broker.history = [good]
    flows = await adapter.fetch_cash_flows(T0)
    assert len(flows) == 1 and any("maxResults=1" in r for r in cash_flows_partial(flows))
    await adapter.close()


# ===================================================================== Pepperstone
@pytest.fixture
def fake() -> FakeCTrader:
    return FakeCTrader()


async def _pp(fake: FakeCTrader, **kw: Any) -> pp_adapter_mod.PepperstoneAdapter:
    adapter = make_pp_adapter(fake, **kw)
    await adapter.connect()
    await adapter.fetch_instruments()
    return adapter


def _no_side(row: dict[str, Any]) -> dict[str, Any]:
    del row["tradeData"]["tradeSide"]
    return row


async def test_pepperstone_rest_rows_it_cannot_map_are_counted(fake: FakeCTrader) -> None:
    fake.positions = [position(501), _no_side(position(502))]
    fake.orders = [order(701), _no_side(order(702))]
    adapter = await _pp(fake)
    try:
        assert {p.position_id for p in await adapter.fetch_positions()} == {"501"}
        assert adapter.unmapped_rows["orders"] == 1
        assert {o.broker_order_id for o in await adapter.fetch_open_orders()} == {"701"}
        assert adapter.unmapped_rows["orders"] == 2
        with pytest.raises(BrokerError, match="unmappable"):
            await adapter.fetch_position_status("502")
        fake.order_details[702] = _no_side(order(702))
        with pytest.raises(BrokerError, match="unmappable"):
            await adapter.fetch_order_status("702")
        assert adapter.unmapped_rows["orders"] == 4
        assert await adapter.fetch_order_status("999") is None
    finally:
        await adapter.close()


async def test_pepperstone_unmappable_stream_events_are_counted(fake: FakeCTrader) -> None:
    adapter = await _pp(fake)
    try:
        out = await adapter._map_order_frame(Frame(2126, {"order": _no_side(order(705))}, None))
        assert [x.kind for x in out if isinstance(x, StreamEvent)] == [StreamEventKind.GAP]
        out = await adapter._map_position_frame(Frame(2126, {"position": _no_side(position(506))}, None))
        assert [x.kind for x in out if isinstance(x, StreamEvent)] == [StreamEventKind.GAP]
        assert adapter.unmapped_rows["orders"] == 2
        out_a = adapter._map_account_frame(Frame(2123, {"trader": {"balanceVersion": 1}}, None))
        assert [x.kind for x in out_a if isinstance(x, StreamEvent)] == [StreamEventKind.GAP]
        assert adapter.unmapped_rows["account"] == 1
    finally:
        await adapter.close()


async def test_pepperstone_frames_the_session_could_not_hand_over_are_counted(fake: FakeCTrader) -> None:
    adapter = await _pp(fake)
    try:
        fake.socket.inbox.put_nowait("{not json")
        await wait_until(lambda: adapter.unmapped_rows["orders"] == 1 and adapter.unmapped_rows["account"] == 1)
    finally:
        await adapter.close()


def _dw(hid: int, ts: int, **kw: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"operationType": 0, "balanceHistoryId": hid, "balance": 1_000_000, "delta": 500_000,
                           "changeBalanceTimestamp": ts, "balanceVersion": 40, "moneyDigits": 2}
    row.update(kw)
    return row


async def test_pepperstone_cash_flows_are_partial_when_a_row_is_unmappable_or_the_deals_truncated(
        fake: FakeCTrader, monkeypatch: pytest.MonkeyPatch) -> None:
    now = PP_T0 + 60_000
    since = now - 3_600_000
    fake.cash_flows = [_dw(31, now - 1000)]
    adapter = await _pp(fake)
    try:
        flows = await adapter.fetch_cash_flows(since)
        assert isinstance(flows, CashFlowBatch) and not flows.partial and len(flows) == 1

        fake.cash_flows = [_dw(31, now - 1000), _dw(32, now - 900, delta=None)]       # no delta: unmappable
        fake.deals = [{"dealId": 7, "positionId": 1, "executionTimestamp": now - 500, "symbolId": 1,
                       "closePositionDetail": {"balance": 1, "moneyDigits": 2}}]          # closing, no grossProfit
        flows = await adapter.fetch_cash_flows(since)
        assert [f.transaction_id for f in flows] == ["pepperstone:balance:31"]
        assert flows.partial and len(flows.partial_reasons) == 2
        assert any("32" in r for r in flows.partial_reasons) and any("deal 7" in r for r in flows.partial_reasons)

        fake.cash_flows, fake.deals = [], []
        real = fake.respond

        def has_more(pt: int, p: dict[str, Any]) -> Any:
            res = real(pt, p)
            if pt == 2133 and isinstance(res, dict):
                res["hasMore"] = True
            return res

        monkeypatch.setattr(fake, "respond", has_more)
        monkeypatch.setattr(pp_adapter_mod, "MAX_WINDOW_SPLITS", 1)
        flows = await adapter.fetch_cash_flows(since)
        assert flows.partial and all("still truncated" in r for r in flows.partial_reasons)
    finally:
        await adapter.close()


async def test_pepperstone_unmappable_balance_events_and_trailing_stops_are_counted(fake: FakeCTrader) -> None:
    """Skeptic re-check: the deposit / withdrawal, closing-deal and trailing-stop stream paths each count (they
    also emit a GAP, but the counter is what the orchestrator reads before and after a REST pass)."""
    adapter = await _pp(fake)
    try:
        assert adapter._currency is not None            # the GAPs below are not the "currency unknown" one
        out = adapter._map_account_frame(Frame(2126, {"depositWithdraw": _dw(41, PP_T0, balanceVersion=None)},
                                               None))
        assert [x.kind for x in out if isinstance(x, StreamEvent)] == [StreamEventKind.GAP]
        assert adapter.unmapped_rows["account"] == 1
        out = adapter._map_account_frame(Frame(2126, {"deal": {"dealId": 8, "positionId": 1,
                                                               "executionTimestamp": PP_T0, "symbolId": 1,
                                                               "closePositionDetail": {"moneyDigits": 2}}}, None))
        assert [x.kind for x in out if isinstance(x, StreamEvent)] == [StreamEventKind.GAP]
        assert adapter.unmapped_rows["account"] == 2
        out_p = await adapter._map_position_frame(Frame(2107, {"positionId": 999, "stopPrice": 1.07,
                                                               "utcLastUpdateTimestamp": PP_T0}, None))
        assert [x.kind for x in out_p if isinstance(x, StreamEvent)] == [StreamEventKind.GAP]
        assert adapter.unmapped_rows["orders"] == 1
    finally:
        await adapter.close()


async def test_pepperstone_a_frame_whose_handler_fails_is_counted(fake: FakeCTrader,
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    """Skeptic re-check: a decoded frame whose dispatch raises is lost like an undecodable one (both streams)."""
    real = pp_adapter_mod.PepperstoneAdapter._on_frame

    def flaky(self: pp_adapter_mod.PepperstoneAdapter, frame: Frame) -> None:
        if frame.payload_type == 2123:
            raise RuntimeError("handler bug")
        real(self, frame)

    monkeypatch.setattr(pp_adapter_mod.PepperstoneAdapter, "_on_frame", flaky)
    adapter = await _pp(fake)
    try:
        assert adapter.unmapped_rows == {"orders": 0, "account": 0, "prices": 0}
        fake.push(2123, {"ctidTraderAccountId": ACCOUNT, "trader": {"balance": 1, "balanceVersion": 43}})
        await wait_until(lambda: adapter.unmapped_rows["orders"] == 1 and adapter.unmapped_rows["account"] == 1)
    finally:
        await adapter.close()


async def test_stonex_attached_child_it_cannot_map_is_counted(fake_broker: FakeStoneX) -> None:
    """Skeptic re-check: an if-done child nested in an open position that cannot be mapped (no Direction on the
    child or its parent) counts like a skipped active order."""
    row = _bad_position(710)
    row["StopOrder"] = {"OrderId": 711, "TriggerPrice": 1.07, "Status": 2}
    fake_broker.positions = [row]
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    assert await adapter.fetch_open_orders() == []
    assert adapter.unmapped_rows["orders"] == 1
    await adapter.close()


async def test_pepperstone_account_frame_before_the_currency_is_known_is_counted(fake: FakeCTrader) -> None:
    adapter = make_pp_adapter(fake)
    out = adapter._map_account_frame(Frame(2123, {"ctidTraderAccountId": ACCOUNT, "trader": {}}, None))
    assert [x.kind for x in out if isinstance(x, StreamEvent)] == [StreamEventKind.GAP]
    assert adapter.unmapped_rows["account"] == 1


# ===================================================================== Mock
async def test_mock_scripts_unmapped_rows_and_partial_cash_flows() -> None:
    adapter = MockBrokerAdapter()
    adapter.skip_row("orders")
    adapter.skip_row("account")
    assert adapter.unmapped_rows == {"orders": 1, "account": 1, "prices": 0}
    assert cash_flows_partial(await adapter.fetch_cash_flows(0)) == ()
    adapter.set_cash_flows_partial("history capped")
    assert cash_flows_partial(await adapter.fetch_cash_flows(0)) == ("history capped",)
    adapter.set_cash_flows_partial()
    assert cash_flows_partial(await adapter.fetch_cash_flows(0)) == ()
