"""Fake cTrader Open API server (JSON over an in-memory 'WebSocket') for the Pepperstone tests.

No network I/O anywhere: the adapter's connector returns a FakeSocket whose send() is answered by
FakeCTrader and whose recv() reads frames the fake (or the test) pushes.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

import pytest

from src.adapters.pepperstone.adapter import PepperstoneAdapter
from src.adapters.pepperstone.tokens import TokenStore
from src.config import PepperstoneSettings

ACCOUNT = 9001
EURUSD, AUDUSD, USDJPY = 1, 2, 3
T0 = 1_767_225_600_000

ACCESS_1, REFRESH_1 = "ACCESS-TOKEN-1-SECRET", "REFRESH-TOKEN-1-SECRET"
ACCESS_2, REFRESH_2 = "ACCESS-TOKEN-2-SECRET", "REFRESH-TOKEN-2-SECRET"
CLIENT_SECRET = "CLIENT-SECRET-XYZ"


def make_settings(env: str = "sandbox") -> PepperstoneSettings:
    return PepperstoneSettings(client_id="client-123", client_secret=CLIENT_SECRET, access_token=ACCESS_1,
                               refresh_token=REFRESH_1, account_id=ACCOUNT, host="fake.ctrader.test",
                               env="live_readonly" if env == "live_readonly" else "sandbox")


class SocketClosed(Exception):
    pass


class FakeSocket:
    def __init__(self, server: FakeCTrader) -> None:
        self.server = server
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.closed = False
        self.sent: list[dict[str, Any]] = []

    async def send(self, message: str) -> None:
        if self.closed:
            raise SocketClosed("send on closed socket")
        frame = json.loads(message)
        self.sent.append(frame)
        self.server.sent.append(frame)
        self.server.handle(self, frame)

    async def recv(self) -> str:
        item = await self.inbox.get()
        if item is None:
            raise SocketClosed("connection closed")
        return item

    async def close(self) -> None:
        self.closed = True
        self.inbox.put_nowait(None)

    def push(self, payload_type: int, payload: dict[str, Any], msg_id: str | None = None) -> None:
        frame: dict[str, Any] = {"payloadType": payload_type, "payload": payload}
        if msg_id is not None:
            frame["clientMsgId"] = msg_id
        self.inbox.put_nowait(json.dumps(frame))

    def drop(self) -> None:
        """The server side drops the connection."""
        self.inbox.put_nowait(None)


def position(pid: int = 501, *, volume: int = 1_000_000, side: str = "BUY", status: int = 1, symbol: int = EURUSD,
             price: float = 1.0855, sl: float | None = 1.08, tp: float | None = None, ts: int = T0,
             swap: int = -125, commission: int = -350, used_margin: int = 33333) -> dict[str, Any]:
    row: dict[str, Any] = {
        "positionId": pid,
        "tradeData": {"symbolId": symbol, "volume": volume, "tradeSide": side, "openTimestamp": ts - 1000},
        "positionStatus": status, "swap": swap, "price": price, "utcLastUpdateTimestamp": ts,
        "commission": commission, "usedMargin": used_margin, "moneyDigits": 2,
    }
    if sl is not None:
        row["stopLoss"] = sl
    if tp is not None:
        row["takeProfit"] = tp
    return row


def order(oid: int = 701, *, order_type: str = "LIMIT", status: str = "ORDER_STATUS_ACCEPTED", volume: int = 500_000,
          side: str = "SELL", symbol: int = EURUSD, ts: int = T0, **extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "orderId": oid,
        "tradeData": {"symbolId": symbol, "volume": volume, "tradeSide": side, "openTimestamp": ts - 1000},
        "orderType": order_type, "orderStatus": status, "utcLastUpdateTimestamp": ts,
    }
    row.update(extra)
    return row


class FakeCTrader:
    """Answers allow-listed requests; records every frame the adapter sent."""

    def __init__(self) -> None:
        self.sockets: list[FakeSocket] = []
        self.sent: list[dict[str, Any]] = []
        self.valid_access = {ACCESS_1}
        self.refresh_map = {REFRESH_1: (ACCESS_2, REFRESH_2)}
        self.scope: Any = "SCOPE_VIEW"
        self.is_live = False
        self.errors: dict[int, list[str]] = {}
        self.assets = [{"assetId": 1, "name": "EUR"}, {"assetId": 2, "name": "USD"}, {"assetId": 3, "name": "AUD"},
                       {"assetId": 4, "name": "JPY"}]
        self.light = [
            {"symbolId": EURUSD, "symbolName": "EURUSD", "enabled": True, "baseAssetId": 1, "quoteAssetId": 2},
            {"symbolId": AUDUSD, "symbolName": "AUDUSD", "enabled": True, "baseAssetId": 3, "quoteAssetId": 2},
            {"symbolId": USDJPY, "symbolName": "USDJPY", "enabled": True, "baseAssetId": 2, "quoteAssetId": 4},
        ]
        self.full = {
            EURUSD: {"symbolId": EURUSD, "digits": 5, "pipPosition": 4, "lotSize": 10_000_000, "minVolume": 100_000},
            AUDUSD: {"symbolId": AUDUSD, "digits": 5, "pipPosition": 4, "lotSize": 10_000_000, "minVolume": 100_000},
            USDJPY: {"symbolId": USDJPY, "digits": 3, "pipPosition": 2, "lotSize": 10_000_000, "minVolume": 100_000},
        }
        self.trader: dict[str, Any] = {"ctidTraderAccountId": ACCOUNT, "balance": 1_000_050, "balanceVersion": 42,
                                       "depositAssetId": 3, "moneyDigits": 2}
        self.positions: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.deals: list[dict[str, Any]] = []
        self.cash_flows: list[dict[str, Any]] = []
        self.order_details: dict[int, dict[str, Any]] = {}
        self.order_history: list[dict[str, Any]] = []     # ProtoOAOrderListReq (by utcLastUpdateTimestamp)
        self.page_limit: int | None = None                 # deal / order list: rows per answer before hasMore
        self.spot_subscriptions: set[int] = set()
        self.spot_snapshot: dict[int, dict[str, Any]] = {}     # pushed right after a subscribe response
        self.on_request: Callable[[FakeSocket, dict[str, Any]], None] | None = None

    # --- connector -------------------------------------------------------------------------------
    async def connect(self, url: str) -> FakeSocket:
        assert url == "wss://fake.ctrader.test:5036"
        sock = FakeSocket(self)
        self.sockets.append(sock)
        return sock

    @property
    def socket(self) -> FakeSocket:
        return self.sockets[-1]

    def types_sent(self) -> list[int]:
        return [int(f["payloadType"]) for f in self.sent]

    def push(self, payload_type: int, payload: dict[str, Any]) -> None:
        self.socket.push(payload_type, payload)

    # --- request handling --------------------------------------------------------------------------
    def handle(self, sock: FakeSocket, frame: dict[str, Any]) -> None:
        pt = int(frame["payloadType"])
        payload: dict[str, Any] = frame.get("payload") or {}
        msg_id = frame.get("clientMsgId")
        if pt == 51:
            return
        if self.on_request is not None:
            self.on_request(sock, frame)
        queued = self.errors.get(pt)
        if queued:
            code = queued.pop(0)
            sock.push(2142, {"errorCode": code, "description": f"fake {code}"}, msg_id)
            return
        res = self.respond(pt, payload)
        if res is None:
            return
        if isinstance(res, str):
            sock.push(2142, {"errorCode": res, "description": f"fake {res}"}, msg_id)
        else:
            sock.push(pt + 1, res, msg_id)
            if pt == 2127:
                for symbol_id in payload["symbolId"]:
                    if symbol_id in self.spot_snapshot:
                        sock.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": symbol_id,
                                         **self.spot_snapshot[symbol_id]})

    def respond(self, pt: int, p: dict[str, Any]) -> dict[str, Any] | str | None:
        if pt == 2100:
            return {} if p.get("clientSecret") == CLIENT_SECRET else "CH_CLIENT_AUTH_FAILURE"
        if pt == 2149:
            if p.get("accessToken") not in self.valid_access:
                return "CH_ACCESS_TOKEN_INVALID"
            res: dict[str, Any] = {"accessToken": p["accessToken"],
                                   "ctidTraderAccount": [{"ctidTraderAccountId": ACCOUNT, "isLive": self.is_live,
                                                          "traderLogin": 123}]}
            if self.scope is not None:
                res["permissionScope"] = self.scope
            return res
        if pt == 2102:
            if p.get("accessToken") not in self.valid_access:
                return "CH_ACCESS_TOKEN_INVALID"
            return {"ctidTraderAccountId": p["ctidTraderAccountId"]}
        if pt == 2173:
            pair = self.refresh_map.get(p.get("refreshToken", ""))
            if pair is None:
                return "INVALID_REQUEST"
            self.valid_access = {pair[0]}
            return {"accessToken": pair[0], "refreshToken": pair[1], "tokenType": "bearer", "expiresIn": 2_628_000}
        if pt == 2104:
            return {"version": "88"}
        if p.get("ctidTraderAccountId") != ACCOUNT:
            return "CH_CTID_TRADER_ACCOUNT_NOT_FOUND"
        if pt == 2112:
            return {"asset": self.assets}
        if pt == 2114:
            return {"symbol": self.light}
        if pt == 2116:
            return {"symbol": [self.full[i] for i in p["symbolId"] if i in self.full]}
        if pt == 2121:
            return {"trader": self.trader}
        if pt == 2124:
            return {"position": self.positions, "order": self.orders}
        if pt == 2127:
            self.spot_subscriptions.update(p["symbolId"])
            return {}
        if pt == 2129:
            self.spot_subscriptions.difference_update(p["symbolId"])
            return {}
        if pt == 2133:
            return self._page("deal", [d for d in self.deals
                                       if p["fromTimestamp"] <= d["executionTimestamp"] < p["toTimestamp"]])
        if pt == 2175:
            return self._page("order", [o for o in self.order_history
                                        if p["fromTimestamp"] <= o["utcLastUpdateTimestamp"] < p["toTimestamp"]])
        if pt == 2179:
            return {"deal": [d for d in self.deals if d.get("positionId") == p["positionId"]
                             and p["fromTimestamp"] <= d["executionTimestamp"] < p["toTimestamp"]], "hasMore": False}
        if pt == 2143:
            return {"depositWithdraw": [c for c in self.cash_flows
                                        if p["fromTimestamp"] <= c["changeBalanceTimestamp"] < p["toTimestamp"]]}
        if pt == 2181:
            found = self.order_details.get(p["orderId"])
            return {"order": found, "deal": []} if found is not None else "ORDER_NOT_FOUND"
        return "UNSUPPORTED_MESSAGE"

    def _page(self, key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if self.page_limit is not None and len(rows) > self.page_limit:
            return {key: rows[:self.page_limit], "hasMore": True}
        return {key: rows, "hasMore": False}


async def wait_until(cond: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


@pytest.fixture
def fake() -> FakeCTrader:
    return FakeCTrader()


def make_adapter(fake: FakeCTrader, *, env: str = "sandbox", token_store: TokenStore | None = None,
                 wall_ms: Callable[[], int] | None = None, **kwargs: Any) -> PepperstoneAdapter:
    kwargs.setdefault("heartbeat_s", 60.0)
    kwargs.setdefault("historical_interval_s", 0.0)
    kwargs.setdefault("request_timeout_s", 2.0)
    return PepperstoneAdapter(make_settings(env), connector=fake.connect, token_store=token_store,
                              wall_ms=wall_ms or (lambda: T0 + 60_000), **kwargs)
