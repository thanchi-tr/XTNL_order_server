"""Plan S04: the optional ``fetch_active_stop_orders`` capability (design PR9, R11-M20): one read-only list read per
adapter (StoneX ``activestoplimitorders`` per trading account, GET only; cTrader the open positions from one
reconcile request), the base default, and the mock's scripted read. No network."""
from __future__ import annotations

import ast
import inspect
import textwrap
from dataclasses import replace
from decimal import Decimal

import httpx

from src.adapters.base import STOP_ORDERS_NOT_SUPPORTED, BrokerAdapter, assert_read_only
from src.adapters.mock import MockBrokerAdapter
from src.adapters.pepperstone import PepperstoneAdapter
from src.adapters.pepperstone.protocol import PT_RECONCILE_REQ
from src.adapters.ratelimit import RestPriority, rest_priority
from src.adapters.stonex.adapter import StoneXAdapter
from src.adapters.transport import is_denied_path
from src.dto import OrderDTO, OrderStatus, PositionDTO

from .conftest import CLIENT, EURUSD_ID, TRADING, USDJPY_ID, FakeStoneX, make_adapter
from .pepperstone.conftest import FakeCTrader, position
from .pepperstone.conftest import make_adapter as make_ctrader


class _Bare(MockBrokerAdapter):
    """An adapter that keeps the base default."""

    fetch_active_stop_orders = BrokerAdapter.fetch_active_stop_orders


async def test_base_default_is_not_supported() -> None:
    answer = await _Bare().fetch_active_stop_orders()
    assert not answer.supported and answer.orders == () and answer.positions == ()
    assert answer.note == STOP_ORDERS_NOT_SUPPORTED and answer.adapter_id == "mock"
    for cls in (StoneXAdapter, PepperstoneAdapter, MockBrokerAdapter):
        assert cls.fetch_active_stop_orders is not BrokerAdapter.fetch_active_stop_orders
        assert_read_only(cls)


# --- StoneX ----------------------------------------------------------------------------------------------------
class TwoAccounts(FakeStoneX):
    """Two trading accounts, each with its own active stop / limit rows."""

    def __init__(self) -> None:
        super().__init__()
        self.account_info["TradingAccounts"].append(
            {"ClientAccountId": int(CLIENT), "TradingAccountId": 2002, "TradingAccountCode": "TA2",
             "TradingAccountStatus": "Open", "TradingAccountType": "CFD"})
        self.per_account: dict[str, list[dict[str, object]]] = {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/TradingAPI/order/activestoplimitorders" and request.headers.get("Session"):
            self.requests.append(request)
            rows = self.per_account.get(request.url.params["TradingAccountId"], [])
            return httpx.Response(200, json={"ActiveStopLimitOrders": rows})
        return super().handler(request)


def _row(order_id: int, kind: str, *, trading: int = int(TRADING), market: str = EURUSD_ID,
         **extra: object) -> dict[str, object]:
    return {"OrderId": order_id, "ParentOrderId": None, "MarketId": int(market), "Direction": "Sell",
            "Quantity": 5000, "TriggerPrice": 1.07, "TradingAccountId": trading, "Type": kind, "Status": 2,
            **extra}


async def test_stonex_one_get_per_trading_account_typed_and_never_denied() -> None:
    fake = TwoAccounts()
    fake.per_account = {
        TRADING: [_row(610, "Stop"), _row(600, "Limit", market=USDJPY_ID,
                                         IfDone=[{"Stop": {"OrderId": 601, "TriggerPrice": 149.5}}])],
        "2002": [_row(620, "Stop", trading=2002)],
    }
    adapter = make_adapter(fake)
    await adapter.connect()
    await adapter.fetch_instruments()
    before = len(fake.requests)
    answer = await adapter.fetch_active_stop_orders()
    sent = fake.requests[before:]
    reads = [r for r in sent if r.url.path.endswith("/order/activestoplimitorders")]
    assert [r.url.params["TradingAccountId"] for r in reads] == [TRADING, "2002"]      # one list read per account
    assert all(r.method == "GET" and not is_denied_path(r.url.path) for r in sent)
    assert not any("/order/" in r.url.path and r.url.path.split("/")[3].isdigit() for r in sent)  # no by-id read
    assert answer.supported and answer.positions == () and answer.adapter_id == "stonex"
    kinds = {o.broker_order_id: o.order_kind for o in answer.orders}
    assert kinds == {"610": "STOP", "600": "LIMIT", "601": "STOP", "620": "STOP"}
    child = next(o for o in answer.orders if o.broker_order_id == "601")
    assert child.is_reduce_only and child.linked_position_id == "600" and child.source == "REST"
    await adapter.close()


async def test_stonex_unmappable_row_is_counted_not_dropped_silently() -> None:
    fake = TwoAccounts()
    fake.per_account = {TRADING: [{"MarketId": int(EURUSD_ID), "Type": "Stop"}]}     # no OrderId
    adapter = make_adapter(fake)
    await adapter.connect()
    unmapped = dict(adapter.unmapped_rows)
    answer = await adapter.fetch_active_stop_orders()
    assert answer.orders == () and adapter.unmapped_rows["orders"] == unmapped["orders"] + 1
    await adapter.close()


def test_stonex_stop_read_builds_no_http_client_of_its_own() -> None:
    """The read goes through the adapter's own StoneXRestClient (ReadOnlyTransport); the method constructs no
    client and names no other transport."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(StoneXAdapter.fetch_active_stop_orders)))
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert not names & {"httpx", "AsyncClient", "Client", "ReadOnlyTransport", "StoneXRestClient"}
    assert "active_stop_limit_orders" in attrs and "_rest" in attrs


# --- cTrader -------------------------------------------------------------------------------------------------
async def test_ctrader_lists_open_positions_from_one_reconcile() -> None:
    fake = FakeCTrader()
    fake.positions = [position(501, sl=1.08), position(502, sl=None)]
    adapter = make_ctrader(fake)
    await adapter.connect()
    await adapter.fetch_instruments()
    before = len(fake.sent)
    answer = await adapter.fetch_active_stop_orders()
    types = [f["payloadType"] for f in fake.sent[before:]]
    assert types == [PT_RECONCILE_REQ]                                                 # one list read
    assert answer.supported and answer.orders == ()
    stops = {p.position_id: p.stop_loss for p in answer.positions}
    assert stops == {"501": Decimal("1.08"), "502": None}
    await adapter.close()


# --- MockBrokerAdapter ---------------------------------------------------------------------------------------
def _order(oid: str, kind: str | None, status: OrderStatus = OrderStatus.WORKING) -> OrderDTO:
    return OrderDTO(client_order_id=oid, broker_order_id=oid, account_id="ACC", adapter_id="mock", symbol="EURUSD",
                    side="SELL", status=status, entry_price=Decimal("1.07"), ordered_units=Decimal(1000),
                    remaining_units=Decimal(1000), is_reduce_only=True, order_kind=kind,  # type: ignore[arg-type]
                    broker_updated_at_ms=1)


async def test_mock_lists_typed_working_orders_or_positions_and_records_the_priority() -> None:
    pos = PositionDTO(position_id="P1", account_id="ACC", adapter_id="mock", symbol="EURUSD", side="BUY",
                      units=Decimal(1000), entry_price=Decimal("1.1"), status="OPEN", stop_loss=Decimal("1.05"))
    mock = MockBrokerAdapter(orders=[_order("S1", "STOP"), _order("L1", "LIMIT"), _order("U1", None),
                                     _order("S2", "STOP", OrderStatus.CANCELLED)],
                             positions=[pos, replace(pos, position_id="P2", status="CLOSED")])
    with rest_priority(RestPriority.BARS):
        answer = await mock.fetch_active_stop_orders()
    assert {o.broker_order_id for o in answer.orders} == {"S1", "L1"} and answer.positions == ()
    assert mock.rest_calls == [("fetch_active_stop_orders", RestPriority.BARS)] and mock.stop_order_reads == 1
    mock.set_stop_orders_supported(True, positions=True)
    answer = await mock.fetch_active_stop_orders()
    assert answer.orders == () and [p.position_id for p in answer.positions] == ["P1"]
    mock.set_stop_orders_supported(False)
    answer = await mock.fetch_active_stop_orders()
    assert not answer.supported and mock.stop_order_reads == 2
