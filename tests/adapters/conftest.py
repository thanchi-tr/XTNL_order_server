"""Shared fakes for the adapter tests: a fake StoneX REST server (httpx.MockTransport) and a fake
Lightstreamer transport. No network I/O anywhere."""
from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

from src.adapters.base import StreamEventKind
from src.adapters.stonex.adapter import StoneXAdapter
from src.adapters.stonex.rest import StoneXRestClient
from src.adapters.stonex.streaming import FrameSink
from src.config import StoneXSettings

V2 = "https://sx.test/v2"
V1 = "https://sx.test/TradingAPI"
CLIENT = "1001"
TRADING = "2001"
EURUSD_ID = "401484347"
AUDUSD_ID = "401484317"
USDJPY_ID = "401484406"
SESSION = "SESSION-TOKEN-1"

MARKETS: dict[str, dict[str, Any]] = {
    EURUSD_ID: {"MarketId": int(EURUSD_ID), "Name": "EUR/USD (per 0.0001) CFD", "PriceDecimalPlaces": 5,
                "WebMinSize": 1000, "QuantityConversionFactor": 1, "MarketUnderlyingTypeId": 4},
    AUDUSD_ID: {"MarketId": int(AUDUSD_ID), "Name": "AUD/USD (per 0.0001) CFD", "PriceDecimalPlaces": 5,
                "WebMinSize": 1000, "QuantityConversionFactor": 1, "MarketUnderlyingTypeId": 4},
    USDJPY_ID: {"MarketId": int(USDJPY_ID), "Name": "USD/JPY (per 0.01) CFD", "PriceDecimalPlaces": 3,
                "WebMinSize": 1000, "MarketUnderlyingTypeId": 4},
}


def make_settings() -> StoneXSettings:
    return StoneXSettings(username="probe-user", password="pw-SECRET", app_key="key-SECRET",
                          base_url_v2=V2, base_url_v1=V1, stream_url="https://push.sx.test", env="sandbox")


def wcf(ms: int) -> str:
    return f"/Date({ms})/"


def executed_ms(row: dict[str, Any]) -> int:
    """ExecutedDateTimeUtc of a fake trade history row ('/Date(ms)/')."""
    text = str(row.get("ExecutedDateTimeUtc", "/Date(0)/"))
    return int(text[text.index("(") + 1:text.index(")")])


class FakeStoneX:
    """Minimal CIAPI behaviour: LogOn, session check, read endpoints; records every request."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.logon_body: dict[str, Any] = {"Session": SESSION, "PasswordChangeRequired": False,
                                           "AllowedAccountOperator": False, "StatusCode": 1, "Is2FAEnabled": False}
        self.account_info: dict[str, Any] = {
            "clientAccounts": [{"clientAccountId": int(CLIENT), "tradingAccountId": int(TRADING),
                                "clientAccountCurrency": "AUD"}],
            "TradingAccounts": [{"ClientAccountId": int(CLIENT), "TradingAccountId": int(TRADING),
                                 "TradingAccountCode": "TA1", "TradingAccountStatus": "Open",
                                 "TradingAccountType": "CFD"}],
        }
        self.margin: dict[str, Any] = {"Cash": 100000.5, "Margin": 2500, "MarginIndicator": 2.5,
                                       "NetEquity": 100150.25, "OpenTradeEquity": 149.75, "TradableFunds": 97650,
                                       "PendingFunds": 0, "TradingResource": 100150.25,
                                       "TotalMarginRequirement": 2400, "CurrencyId": 6, "CurrencyIsoCode": "AUD"}
        self.positions: list[dict[str, Any]] = []
        self.actives: list[dict[str, Any]] = []
        self.history: list[dict[str, Any]] = []
        #: None: tradehistory returns every row (older tests). "asc" / "desc": honour from + maxResults, in that
        #: time order (by ExecutedDateTimeUtc).
        self.history_paging: str | None = None
        self.stop_limit_history: list[dict[str, Any]] = []
        self.orders: dict[str, dict[str, Any]] = {}
        self.order_active: dict[str, dict[str, Any]] = {}
        self.order_positions: dict[str, dict[str, Any]] = {}
        self.closed_order_ids: set[str] = set()     # openposition answers 403 / 80001 (live behaviour)
        self.markets = dict(MARKETS)
        self.search: list[dict[str, Any]] = [{"MarketId": int(m), "Name": v["Name"]} for m, v in MARKETS.items()]
        self.one_shot: dict[str, httpx.Response] = {}

    def paths(self) -> list[str]:
        return [r.url.path for r in self.requests]

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path in self.one_shot:
            return self.one_shot.pop(path)
        if path == "/v2/Session" and request.method == "POST":
            return httpx.Response(200, json=self.logon_body)
        if request.headers.get("Session") != SESSION or request.headers.get("UserName") != "probe-user":
            return httpx.Response(401, json={"HttpStatus": 401, "ErrorMessage": "Session invalid", "ErrorCode": 4011})
        if path == "/v2/userAccount/ClientAndTradingAccount":
            return httpx.Response(200, json=self.account_info)
        if path == "/v2/margin/clientAccountMargin":
            return httpx.Response(200, json=self.margin)
        if path == "/TradingAPI/order/openpositions":
            return httpx.Response(200, json={"OpenPositions": self.positions})
        if path == "/TradingAPI/order/activestoplimitorders":
            return httpx.Response(200, json={"ActiveStopLimitOrders": self.actives})
        if path == "/TradingAPI/order/tradehistory":
            rows = self.history
            if self.history_paging is not None:
                from_ms = int(request.url.params["from"]) * 1000
                limit = int(request.url.params["maxResults"])
                rows = sorted((r for r in rows if executed_ms(r) >= from_ms), key=executed_ms,
                              reverse=self.history_paging == "desc")[:limit]
            return httpx.Response(200, json={"TradeHistory": rows, "SupplementalOpenOrders": []})
        if path == "/TradingAPI/order/stoplimitorderhistory":
            # As measured live (2026-11-14): ``from`` (seconds) filters by last change, oldest-first, then capped.
            limit = int(request.url.params["maxResults"])
            rows = self.stop_limit_history
            if "from" in request.url.params:
                from_ms = int(request.url.params["from"]) * 1000
                rows = sorted((r for r in rows if (_stop_ts(r) or 0) >= from_ms), key=lambda r: _stop_ts(r) or 0)
            return httpx.Response(200, json={"StopLimitOrderHistory": rows[:limit]})
        if path == "/v2/market/information":
            ids = [str(i) for i in json.loads(request.content)["marketIds"]]
            return httpx.Response(200, json={"MarketInformations": [self.markets[i] for i in ids if i in self.markets]})
        if path == "/TradingAPI/market/search":
            q = request.url.params.get("Query", "")
            return httpx.Response(200, json={"Markets": [m for m in self.search if str(m["Name"]).startswith(q)]})
        if path == "/v2/Session/validate":
            return httpx.Response(200, json={"IsAuthenticated": True})
        if path == "/TradingAPI/session/deleteSession":
            return httpx.Response(200, json={"LoggedOut": True})
        parts = path.split("/")
        if len(parts) >= 4 and parts[2] == "order" and parts[3].isdigit():
            oid = parts[3]
            if len(parts) == 4:
                return httpx.Response(200, json=self.orders.get(oid, {"TradeOrder": None, "StopLimitOrder": None}))
            if parts[4] == "openposition":
                if oid in self.closed_order_ids:
                    return httpx.Response(403, json={"HttpStatus": 403, "ErrorCode": 80001,
                                                     "ErrorMessage": "You are not authorized you access this order."})
                return httpx.Response(200, json={"OpenPosition": self.order_positions.get(oid)})
            if parts[4] == "activestoplimitorder":
                return httpx.Response(200, json={"ActiveStopLimitOrder": self.order_active.get(oid)})
        return httpx.Response(404, json={"HttpStatus": 404, "ErrorMessage": "not found", "ErrorCode": 4040})



def _stop_ts(row: dict[str, Any]) -> int | None:
    from src.adapters.stonex.wire import parse_broker_time_ms
    return parse_broker_time_ms(row.get("LastChangedDateTimeUtc"))


@dataclass
class FakeSub:
    data_adapter: str
    items: list[str]
    fields: list[str]
    sink: FrameSink
    active: bool = True
    mode: str = "DISTINCT"


@dataclass
class FakeStreamTransport:
    """Stands in for LightstreamerTransport; frames are delivered from a separate thread."""

    subs: list[FakeSub] = field(default_factory=list)
    connected_as: tuple[str, str] | None = None
    on_status: Callable[[str], None] | None = None
    on_server_error: Callable[[int, str], None] | None = None
    disconnects: int = 0

    def connect(self, user: str, password: str, on_status: Callable[[str], None],
                on_server_error: Callable[[int, str], None]) -> None:
        self.connected_as = (user, password)
        self.on_status, self.on_server_error = on_status, on_server_error
        on_status("CONNECTED:WS-STREAMING")

    def subscribe(self, data_adapter: str, items: Sequence[str], fields: Sequence[str], sink: FrameSink,
                  *, mode: str = "DISTINCT") -> object:
        sub = FakeSub(data_adapter, list(items), list(fields), sink)
        sub.mode = mode
        self.subs.append(sub)
        return sub

    def unsubscribe(self, handle: object) -> None:
        assert isinstance(handle, FakeSub)
        handle.active = False

    def disconnect(self) -> None:
        self.disconnects += 1

    # --- test drivers (library-thread side) ---
    def find(self, data_adapter: str, item: str | None = None) -> FakeSub:
        for sub in self.subs:
            if sub.data_adapter == data_adapter and sub.active and (item is None or item in sub.items):
                return sub
        raise AssertionError(f"no active {data_adapter} subscription for {item}")

    def emit(self, data_adapter: str, values: dict[str, str | None], item: str | None = None) -> None:
        sub = self.find(data_adapter, item)
        thread = threading.Thread(target=sub.sink.on_frame, args=(dict(values),))
        thread.start()
        thread.join()

    def event(self, data_adapter: str, kind: StreamEventKind, detail: str = "", item: str | None = None) -> None:
        sub = self.find(data_adapter, item)
        thread = threading.Thread(target=sub.sink.on_event, args=(kind, detail))
        thread.start()
        thread.join()

    def status(self, status: str) -> None:
        assert self.on_status is not None
        thread = threading.Thread(target=self.on_status, args=(status,))
        thread.start()
        thread.join()


async def wait_until(cond: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


@pytest.fixture
def fake_broker() -> FakeStoneX:
    return FakeStoneX()


@pytest.fixture
def fake_stream() -> FakeStreamTransport:
    return FakeStreamTransport()


def make_rest(fake: FakeStoneX) -> StoneXRestClient:
    return StoneXRestClient.create(make_settings(), http=httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))


def make_adapter(fake: FakeStoneX, stream: FakeStreamTransport | None = None, **kwargs: Any) -> StoneXAdapter:
    kwargs.setdefault("heartbeat_s", 60.0)
    if stream is not None:
        kwargs.setdefault("stream_transport_factory", lambda: stream)
    return StoneXAdapter(make_settings(), rest=make_rest(fake), **kwargs)
