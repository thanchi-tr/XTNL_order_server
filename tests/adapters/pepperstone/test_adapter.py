"""PepperstoneAdapter end to end against the fake cTrader server (no network)."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, TypeVar

import pytest

from src.adapters.base import (
    AuthFailedError,
    SessionExpiredError,
    StreamEvent,
    StreamEventKind,
    assert_read_only,
)
from src.adapters.pepperstone import PepperstoneAdapter
from src.adapters.pepperstone.probe import run as run_probe
from src.adapters.pepperstone.protocol import Frame
from src.adapters.pepperstone.tokens import InMemoryTokenStore, TokenPair
from src.dto import AccountDTO, AccountLive, CashFlowDTO, OrderDTO, OrderStatus, PositionDTO, PriceDTO

from .conftest import (
    ACCESS_1,
    ACCESS_2,
    ACCOUNT,
    AUDUSD,
    CLIENT_SECRET,
    EURUSD,
    REFRESH_1,
    REFRESH_2,
    T0,
    USDJPY,
    FakeCTrader,
    make_adapter,
    order,
    position,
    wait_until,
)

T = TypeVar("T")


async def take(gen: AsyncIterator[T], n: int, timeout: float = 2.0) -> list[T]:
    return [await asyncio.wait_for(gen.__anext__(), timeout) for _ in range(n)]


async def close_gen(gen: AsyncIterator[Any]) -> None:
    closer = getattr(gen, "aclose", None)
    if closer is not None:
        await closer()


async def connected(fake: FakeCTrader, **kwargs: Any) -> PepperstoneAdapter:
    adapter = make_adapter(fake, **kwargs)
    await adapter.connect()
    await adapter.fetch_instruments()
    return adapter


def test_interface_is_read_only() -> None:
    assert_read_only(PepperstoneAdapter)


# --- auth ---------------------------------------------------------------------------------------------
async def test_auth_flow(fake: FakeCTrader) -> None:
    lease_checks: list[int] = []
    adapter = make_adapter(fake, pre_connect_check=lambda: lease_checks.append(1))
    await adapter.connect()
    try:
        assert fake.types_sent() == [2100, 2149, 2102, 2112, 2121]
        app_auth, by_token, account_auth = fake.sent[0], fake.sent[1], fake.sent[2]
        assert app_auth["payload"] == {"clientId": "client-123", "clientSecret": CLIENT_SECRET}
        assert by_token["payload"] == {"accessToken": ACCESS_1}
        assert account_auth["payload"] == {"ctidTraderAccountId": ACCOUNT, "accessToken": ACCESS_1}
        assert all("clientMsgId" in f for f in fake.sent)
        assert adapter.permission_scope == "SCOPE_VIEW" and adapter.currency == "AUD"
        assert adapter.token_generation == 0 and len(lease_checks) >= 2
        specs = {s.symbol: s for s in await adapter.fetch_instruments()}
        assert set(specs) == set()                          # nothing configured, nothing held yet
    finally:
        await adapter.close()
    assert fake.socket.closed


async def test_bad_client_secret_is_auth_failed(fake: FakeCTrader) -> None:
    fake.errors[2100] = ["CH_CLIENT_AUTH_FAILURE"]
    adapter = make_adapter(fake)
    with pytest.raises(AuthFailedError):
        await adapter.connect()
    assert fake.socket.closed


async def test_trade_scope_is_refused(fake: FakeCTrader) -> None:
    fake.scope = "SCOPE_TRADE"
    with pytest.raises(AuthFailedError, match="scope"):
        await make_adapter(fake).connect()
    assert 2102 not in fake.types_sent()                     # never authorises the account


async def test_live_readonly_requires_reported_view_scope_and_a_live_account(fake: FakeCTrader) -> None:
    fake.scope = None
    fake.is_live = True
    with pytest.raises(AuthFailedError, match="scope"):
        await make_adapter(fake, env="live_readonly").connect()
    fake.scope = 0                                           # SCOPE_VIEW as a number
    fake.is_live = False
    with pytest.raises(AuthFailedError, match="not a live account"):
        await make_adapter(fake, env="live_readonly").connect()
    fake.is_live = True
    adapter = make_adapter(fake, env="live_readonly")
    await adapter.connect()
    await adapter.close()
    with pytest.raises(AuthFailedError, match="BROKER_ENV=sandbox"):
        await make_adapter(fake).connect()


# --- token refresh ------------------------------------------------------------------------------------
async def test_invalid_token_refreshes_saves_then_authorises_with_new_generation(fake: FakeCTrader) -> None:
    fake.valid_access = {ACCESS_2}                            # ACCESS_1 expired at the broker
    store = InMemoryTokenStore()
    saved_before_auth: list[bool] = []

    def watch(sock: Any, frame: dict[str, Any]) -> None:
        if frame["payloadType"] == 2102:
            saved_before_auth.append(store.pair is not None and store.pair.access_token == ACCESS_2)

    fake.on_request = watch
    adapter = make_adapter(fake, token_store=store)
    await adapter.connect()
    try:
        assert fake.types_sent()[:5] == [2100, 2149, 2173, 2149, 2102]
        assert fake.sent[2]["payload"] == {"refreshToken": REFRESH_1}
        assert fake.sent[4]["payload"]["accessToken"] == ACCESS_2
        assert saved_before_auth == [True]
        assert adapter.token_generation == 1
        assert store.pair is not None and store.pair.refresh_token == REFRESH_2 and store.pair.generation == 1
        assert store.pair.expires_at_ms == T0 + 60_000 + 2_628_000_000
    finally:
        await adapter.close()


async def test_stored_pair_wins_and_is_refreshed_before_expiry(fake: FakeCTrader) -> None:
    store = InMemoryTokenStore(TokenPair(ACCESS_1, REFRESH_1, T0 + 3_600_000, 5))   # expires within the margin
    adapter = make_adapter(fake, token_store=store)
    await adapter.connect()
    try:
        assert fake.types_sent()[:3] == [2100, 2173, 2149]
        assert adapter.token_generation == 6 and store.pair is not None and store.pair.access_token == ACCESS_2
    finally:
        await adapter.close()


async def test_token_refused_after_fresh_token_is_auth_failed(fake: FakeCTrader) -> None:
    def refuse_all(sock: Any, frame: dict[str, Any]) -> None:
        if frame["payloadType"] in (2149, 2102):
            fake.valid_access.clear()                         # even the refreshed token is refused

    fake.on_request = refuse_all
    with pytest.raises(AuthFailedError, match="after a fresh token"):
        await make_adapter(fake).connect()


async def test_refresh_disabled_reports_instead_of_rotating(fake: FakeCTrader) -> None:
    fake.valid_access = {ACCESS_2}
    adapter = make_adapter(fake, allow_token_refresh=False)
    with pytest.raises(AuthFailedError):
        await adapter.connect()
    assert 2173 not in fake.types_sent()


# --- reconcile mapping ---------------------------------------------------------------------------
async def test_reconcile_maps_positions_and_orders(fake: FakeCTrader) -> None:
    fake.positions = [position(501, volume=1_000_000, side="BUY", sl=1.08, tp=1.1),
                      position(502, volume=250_000, side="SELL", symbol=USDJPY, price=155.123, sl=None)]
    fake.orders = [
        order(701, order_type="LIMIT", volume=500_000, side="SELL", limitPrice=1.095, stopLoss=1.1,
              clientOrderId="cid-701"),
        order(702, order_type="STOP", volume=300_000, side="BUY", stopPrice=1.07, executedVolume=100_000),
        order(703, order_type="LIMIT", volume=1_000_000, side="SELL", limitPrice=1.09, closingOrder=True,
              positionId=501),
        order(704, order_type="STOP_LOSS_TAKE_PROFIT", volume=1_000_000, side="SELL", stopPrice=1.08,
              positionId=501),
    ]
    adapter = await connected(fake)
    try:
        assert {s.symbol for s in adapter._instruments.values()} == {"EURUSD", "USDJPY"}
        eur = adapter._instruments["EURUSD"]
        assert eur.unit_kind == "CENTI_UNITS" and eur.point_size == Decimal("0.0001")
        assert eur.min_qty == Decimal(1000) and eur.contract_size == Decimal(100_000)
        assert eur.base_ccy == "EUR" and eur.quote_ccy == "USD" and eur.pnl_ccy == "USD"
        assert eur.broker_symbol == "1" and eur.swap_booked_to_balance is False

        positions = {p.position_id: p for p in await adapter.fetch_positions()}
        p1 = positions["501"]
        assert p1.units == Decimal(10_000) and p1.side == "BUY" and p1.status == "OPEN"
        assert p1.entry_price == Decimal("1.0855") and p1.stop_loss == Decimal("1.08") and p1.take_profit == Decimal("1.1")
        assert p1.accrued_swap == Decimal("-1.25") and p1.commission == Decimal("-3.50")
        assert p1.broker_updated_at_ms == T0 and p1.broker_seq is None and p1.source == "REST"
        assert p1.account_id == str(ACCOUNT) and p1.adapter_id == "pepperstone" and not p1.instrument_unknown
        p2 = positions["502"]
        assert p2.symbol == "USDJPY" and p2.units == Decimal(2500) and p2.stop_loss is None

        orders = {o.broker_order_id: o for o in await adapter.fetch_open_orders()}
        assert set(orders) == {"701", "702", "703"}           # working protection order not emitted
        o1 = orders["701"]
        assert o1.status == OrderStatus.WORKING and o1.entry_price == Decimal("1.095")
        assert o1.ordered_units == Decimal(5000) and o1.remaining_units == Decimal(5000)
        assert o1.stop_loss == Decimal("1.1") and o1.client_order_id == "cid-701" and not o1.is_reduce_only
        o2 = orders["702"]
        assert o2.status == OrderStatus.PARTIALLY_FILLED and o2.remaining_units == Decimal(2000)
        assert o2.entry_price == Decimal("1.07") and o2.client_order_id is None
        o3 = orders["703"]
        assert o3.is_reduce_only and o3.linked_position_id == "501"

        _, lives = await adapter.fetch_account_snapshot()
        assert lives[0].margin_used == Decimal("666.66") and lives[0].equity is None
    finally:
        await adapter.close()


async def test_unknown_symbol_is_instrument_unknown(fake: FakeCTrader) -> None:
    fake.positions = [position(509, symbol=77)]
    adapter = await connected(fake)
    try:
        [p] = await adapter.fetch_positions()
        assert p.instrument_unknown and p.units is None and p.raw_qty == Decimal(1_000_000)
        assert p.symbol == "UNKNOWN:77"
    finally:
        await adapter.close()


async def test_account_snapshot(fake: FakeCTrader) -> None:
    adapter = await connected(fake)
    try:
        accounts, lives = await adapter.fetch_account_snapshot()
        acct = accounts[0]
        assert acct.balance == Decimal("10000.50") and acct.currency == "AUD"
        assert acct.broker_seq == 42 and acct.broker_updated_at_ms == 0 and acct.source == "REST"
        assert lives[0].equity is None and lives[0].margin_used == Decimal(0)
    finally:
        await adapter.close()


async def test_order_status_and_not_found(fake: FakeCTrader) -> None:
    fake.order_details[801] = order(801, order_type="MARKET", status="ORDER_STATUS_FILLED", volume=200_000,
                                    side="BUY", executedVolume=200_000, executionPrice=1.0861)
    adapter = await connected(fake)
    try:
        o = await adapter.fetch_order_status("801")
        assert o is not None and o.status == OrderStatus.FILLED and o.remaining_units == Decimal(0)
        assert o.entry_price == Decimal("1.0861") and o.ordered_units == Decimal(2000)
        assert await adapter.fetch_order_status("999") is None
    finally:
        await adapter.close()


# --- deals -> positions / cash flows ------------------------------------------------------------------
def deal(deal_id: int, *, pid: int = 601, volume: int = 1_000_000, side: str = "BUY", ts: int, px: float = 1.08,
         close: dict[str, Any] | None = None) -> dict[str, Any]:
    row: dict[str, Any] = {"dealId": deal_id, "orderId": deal_id + 10, "positionId": pid, "volume": volume,
                           "filledVolume": volume, "symbolId": EURUSD, "executionTimestamp": ts,
                           "utcLastUpdateTimestamp": ts, "executionPrice": px, "tradeSide": side, "dealStatus": 2,
                           "commission": -175, "moneyDigits": 2}
    if close is not None:
        row["closePositionDetail"] = close
    return row


CLOSE_DETAIL = {"entryPrice": 1.08, "grossProfit": 5000, "swap": -120, "commission": -350, "balance": 1_004_580,
                "closedVolume": 1_000_000, "balanceVersion": 43, "moneyDigits": 2}


async def test_position_status_rebuilt_from_deals(fake: FakeCTrader) -> None:
    now = T0 + 60_000
    fake.deals = [deal(1, ts=now - 3_600_000), deal(2, side="SELL", ts=now - 60_000, px=1.085, close=CLOSE_DETAIL)]
    adapter = await connected(fake)
    try:
        p = await adapter.fetch_position_status("601")
        assert p is not None and p.status == "CLOSED" and p.units == Decimal(0) and p.side == "BUY"
        assert p.entry_price == Decimal("1.08") and p.broker_updated_at_ms == now - 60_000
        assert 2179 in fake.types_sent()
        fake.deals = [deal(1, ts=now - 3_600_000)]
        p_open = await adapter.fetch_position_status("601")
        assert p_open is not None and p_open.status == "OPEN" and p_open.units == Decimal(10_000)
        assert await adapter.fetch_position_status("12345") is None
    finally:
        await adapter.close()


async def test_cash_flows_since(fake: FakeCTrader) -> None:
    now = T0 + 60_000
    since = now - 10 * 86_400_000                               # spans two one-week windows
    fake.deals = [deal(1, ts=now - 9 * 86_400_000), deal(2, side="SELL", ts=now - 1000, close=CLOSE_DETAIL)]
    fake.cash_flows = [
        {"operationType": "DEPOSIT", "balanceHistoryId": 31, "balance": 1_000_000, "delta": 500_000,
         "changeBalanceTimestamp": now - 8 * 86_400_000, "balanceVersion": 40, "moneyDigits": 2},
        {"operationType": 1, "balanceHistoryId": 32, "balance": 990_000, "delta": 10_000,
         "changeBalanceTimestamp": now - 2 * 86_400_000, "balanceVersion": 41, "moneyDigits": 2},
        {"operationType": 0, "balanceHistoryId": 30, "balance": 500_000, "delta": 500_000,
         "changeBalanceTimestamp": since - 1, "balanceVersion": 39, "moneyDigits": 2},
    ]
    adapter = await connected(fake)
    try:
        flows = await adapter.fetch_cash_flows(since)
        by_id = {f.transaction_id: f for f in flows}
        assert set(by_id) == {"pepperstone:balance:31", "pepperstone:balance:32", "pepperstone:deal:2"}
        assert by_id["pepperstone:balance:31"].kind == "DEPOSIT" and by_id["pepperstone:balance:31"].amount == Decimal(5000)
        assert by_id["pepperstone:balance:32"].kind == "WITHDRAWAL" and by_id["pepperstone:balance:32"].amount == Decimal(-100)
        pnl = by_id["pepperstone:deal:2"]
        assert pnl.kind == "TRADE_PNL" and pnl.amount == Decimal("45.30") and pnl.currency == "AUD"
        assert [f.broker_ts for f in flows] == sorted(f.broker_ts for f in flows)
        windows = [f["payload"] for f in fake.sent if f["payloadType"] == 2143]
        assert len(windows) == 2 and all(w["toTimestamp"] - w["fromTimestamp"] <= 7 * 86_400_000 for w in windows)
    finally:
        await adapter.close()


# --- streams ------------------------------------------------------------------------------------------
async def test_execution_events_map_orders_positions_and_account(fake: FakeCTrader) -> None:
    adapter = await connected(fake)
    orders = adapter.stream_order_updates()
    positions = adapter.stream_positions()
    account = adapter.stream_account()
    try:
        for gen in (orders, positions, account):
            [ev] = await take(gen, 1)
            assert isinstance(ev, StreamEvent) and ev.kind == StreamEventKind.SUBSCRIBED
        await adapter.ensure_ticks([])                          # instruments already loaded for EURUSD below
        await adapter._ensure_symbols([EURUSD])
        filled = order(901, order_type="MARKET", status="ORDER_STATUS_FILLED", volume=1_000_000, side="BUY",
                       executedVolume=1_000_000, executionPrice=1.0855, positionId=601, ts=T0 + 5)
        fake.push(2126, {"ctidTraderAccountId": ACCOUNT, "executionType": "ORDER_FILLED", "order": filled,
                         "position": position(601, ts=T0 + 5, sl=None),
                         "deal": deal(1, ts=T0 + 5)})
        [o] = await take(orders, 1)
        assert isinstance(o, OrderDTO) and o.status == OrderStatus.FILLED and o.broker_order_id == "901"
        assert o.source == "STREAM" and o.broker_updated_at_ms == T0 + 5
        [p] = await take(positions, 1)
        assert isinstance(p, PositionDTO) and p.position_id == "601" and p.units == Decimal(10_000)
        assert p.status == "OPEN" and p.stop_loss is None and p.source == "STREAM"
        [live] = await take(account, 1)
        assert isinstance(live, AccountLive) and live.margin_used == Decimal("333.33")

        # trailing stop moves the position's native stop
        fake.push(2107, {"ctidTraderAccountId": ACCOUNT, "positionId": 601, "orderId": 902, "stopPrice": 1.081,
                         "utcLastUpdateTimestamp": T0 + 6})
        [moved] = await take(positions, 1)
        assert isinstance(moved, PositionDTO) and moved.stop_loss == Decimal("1.081")
        assert moved.broker_updated_at_ms == T0 + 6

        # the closing deal: position CLOSED, TRADE_PNL + balance (balanceVersion as broker_seq)
        closing = order(903, order_type="MARKET", status="ORDER_STATUS_FILLED", volume=1_000_000, side="SELL",
                        executedVolume=1_000_000, executionPrice=1.0901, closingOrder=True, positionId=601, ts=T0 + 9)
        fake.push(2126, {"ctidTraderAccountId": ACCOUNT, "executionType": 3, "order": closing,
                         "position": position(601, status=2, ts=T0 + 9),
                         "deal": deal(2, side="SELL", ts=T0 + 9, close=CLOSE_DETAIL)})
        [o2] = await take(orders, 1)
        assert isinstance(o2, OrderDTO) and o2.is_reduce_only and o2.linked_position_id == "601"
        [p2] = await take(positions, 1)
        assert isinstance(p2, PositionDTO) and p2.status == "CLOSED" and p2.units == Decimal(0)
        flow, acct, live2 = await take(account, 3)
        assert isinstance(flow, CashFlowDTO) and flow.kind == "TRADE_PNL" and flow.amount == Decimal("45.30")
        assert flow.transaction_id == "pepperstone:deal:2" and flow.broker_ts == T0 + 9
        assert isinstance(acct, AccountDTO) and acct.balance == Decimal("10045.80") and acct.broker_seq == 43
        assert isinstance(live2, AccountLive) and live2.margin_used == Decimal(0)
        assert adapter.stream_watermark_ms == T0 + 9

        # a deposit
        fake.push(2126, {"ctidTraderAccountId": ACCOUNT, "executionType": "DEPOSIT_WITHDRAW",
                         "depositWithdraw": {"operationType": "DEPOSIT", "balanceHistoryId": 77, "balance": 1_104_580,
                                             "delta": 100_000, "changeBalanceTimestamp": T0 + 20,
                                             "balanceVersion": 44, "moneyDigits": 2}})
        dep, acct2 = await take(account, 2)
        assert isinstance(dep, CashFlowDTO) and dep.kind == "DEPOSIT" and dep.amount == Decimal(1000)
        assert isinstance(acct2, AccountDTO) and acct2.broker_seq == 44 and acct2.balance == Decimal("11045.80")

        # trader update; events for another account are ignored
        fake.push(2123, {"ctidTraderAccountId": 1234, "trader": {"balance": 1, "balanceVersion": 1}})
        fake.push(2123, {"ctidTraderAccountId": ACCOUNT, "trader": {**fake.trader, "balanceVersion": 45}})
        [acct3] = await take(account, 1)
        assert isinstance(acct3, AccountDTO) and acct3.broker_seq == 45 and acct3.source == "STREAM"
    finally:
        await adapter.close()


async def test_working_protection_order_is_not_streamed(fake: FakeCTrader) -> None:
    adapter = await connected(fake)
    orders = adapter.stream_order_updates()
    try:
        await take(orders, 1)
        await adapter._ensure_symbols([EURUSD])
        sltp = order(950, order_type="STOP_LOSS_TAKE_PROFIT", volume=1_000_000, stopPrice=1.07, positionId=601)
        fake.push(2126, {"ctidTraderAccountId": ACCOUNT, "executionType": "ORDER_ACCEPTED", "order": sltp})
        fake.push(2126, {"ctidTraderAccountId": ACCOUNT, "executionType": "ORDER_FILLED",
                         "order": {**sltp, "orderStatus": "ORDER_STATUS_FILLED", "executedVolume": 1_000_000}})
        [o] = await take(orders, 1)
        assert isinstance(o, OrderDTO) and o.status == OrderStatus.FILLED and o.is_reduce_only
        assert o.linked_position_id == "601"
    finally:
        await adapter.close()


async def test_spot_prices(fake: FakeCTrader) -> None:
    adapter = await connected(fake)
    ticks = adapter.stream_ticks(["EURUSD", "USDJPY", "NOPE"])
    try:
        [sub] = await take(ticks, 1)
        assert isinstance(sub, StreamEvent) and sub.kind == StreamEventKind.SUBSCRIBED
        assert fake.spot_subscriptions == {EURUSD, USDJPY}
        req = next(f for f in fake.sent if f["payloadType"] == 2127)
        assert req["payload"]["subscribeToSpotTimestamp"] is True
        fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": EURUSD, "bid": 108_512, "ask": 108_524,
                         "timestamp": T0 + 1})
        fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": EURUSD, "ask": 108_530})   # bid unchanged
        fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": AUDUSD, "bid": 1, "ask": 2})  # not wanted: dropped by the reader
        fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": USDJPY, "bid": 15_512_300, "ask": 15_513_000,
                         "timestamp": T0 + 3})
        p1, p2, p3 = await take(ticks, 3)
        assert isinstance(p1, PriceDTO) and p1.bid == Decimal("1.08512") and p1.ask == Decimal("1.08524")
        assert p1.spread_pips == Decimal("1.2") and p1.timestamp_ms == T0 + 1 and p1.symbol == "EURUSD"
        assert isinstance(p2, PriceDTO) and p2.bid == Decimal("1.08512") and p2.ask == Decimal("1.08530")
        assert p2.timestamp_ms == T0 + 60_000                    # no broker time: receipt time (unversioned)
        assert isinstance(p3, PriceDTO) and p3.symbol == "USDJPY" and p3.bid == Decimal("155.123")
        assert p3.spread_pips == Decimal("0.7")

        await adapter.ensure_ticks(["AUDUSD"])
        assert AUDUSD in fake.spot_subscriptions
        fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": AUDUSD, "bid": 65_001, "ask": 65_011})
        added, p4 = await take(ticks, 2)
        # design PR6: the symbol added on the live stream reports its own SUBSCRIBED, naming it
        assert isinstance(added, StreamEvent) and added.kind == StreamEventKind.SUBSCRIBED
        assert added.market == "AUDUSD"
        assert isinstance(p4, PriceDTO) and p4.symbol == "AUDUSD" and p4.ask == Decimal("0.65011")
        await adapter.ensure_ticks(["AUDUSD"])                     # already subscribed: no second event
    finally:
        await close_gen(ticks)
        await wait_until(lambda: not fake.spot_subscriptions)    # unsubscribed when the stream ends
        await adapter.close()


async def test_a_refused_kept_spot_never_fails_the_stream_asked_for(fake: FakeCTrader) -> None:
    """S05 review: a spot kept from an earlier ensure_ticks (e.g. a tick-only symbol) but not asked for now is
    subscribed again on its own; the broker refusing it is only logged, the stream reports SUBSCRIBED, and
    `subscribed_price_markets` names only what is really subscribed (no interval for the refused one)."""
    adapter = await connected(fake)

    def refuse_audusd(sock: Any, frame: dict[str, Any]) -> None:
        if frame["payloadType"] == 2127 and frame["payload"]["symbolId"] == [AUDUSD]:
            fake.errors.setdefault(2127, []).append("INVALID_REQUEST")

    fake.on_request = refuse_audusd
    await adapter.ensure_ticks(["AUDUSD"])            # no price stream yet: only remembered
    assert adapter.subscribed_price_markets == frozenset()
    ticks = adapter.stream_ticks(["EURUSD"])
    try:
        [sub] = await take(ticks, 1)
        assert isinstance(sub, StreamEvent) and sub.kind == StreamEventKind.SUBSCRIBED and sub.market is None
        assert fake.spot_subscriptions == {EURUSD}
        assert adapter.subscribed_price_markets == frozenset({"EURUSD"})
    finally:
        await close_gen(ticks)
        await adapter.close()


async def test_snapshot_spot_right_after_the_subscribe_response_is_kept(fake: FakeCTrader) -> None:
    fake.spot_snapshot[EURUSD] = {"bid": 108_500, "ask": 108_510, "timestamp": T0 + 2}
    adapter = await connected(fake)
    ticks = adapter.stream_ticks(["EURUSD"])
    try:
        sub, price = await take(ticks, 2)
        assert isinstance(sub, StreamEvent) and sub.kind == StreamEventKind.SUBSCRIBED
        assert isinstance(price, PriceDTO) and price.bid == Decimal("1.08500") and price.timestamp_ms == T0 + 2
    finally:
        await close_gen(ticks)
        await adapter.close()


async def test_heartbeats_both_ways(fake: FakeCTrader) -> None:
    adapter = make_adapter(fake, heartbeat_s=0.05)
    await adapter.connect()
    account = adapter.stream_account()
    try:
        [sub] = await take(account, 1)
        assert sub.kind == StreamEventKind.SUBSCRIBED          # type: ignore[union-attr]
        await wait_until(lambda: fake.types_sent().count(51) >= 2)
        assert all(f["payload"] == {} for f in fake.sent if f["payloadType"] == 51)
        fake.push(51, {})
        [hb] = await take(account, 1)
        assert isinstance(hb, StreamEvent) and hb.kind == StreamEventKind.HEARTBEAT and hb.stream == "account"
    finally:
        await adapter.close()


async def test_idle_socket_is_dropped(fake: FakeCTrader) -> None:
    adapter = make_adapter(fake, heartbeat_s=0.02, idle_timeout_s=0.1)
    await adapter.connect()
    orders = adapter.stream_order_updates()
    try:
        await take(orders, 1)
        kinds: list[StreamEventKind] = []
        while StreamEventKind.DISCONNECTED not in kinds:       # heartbeats (if any), then the drop
            [item] = await take(orders, 1)
            assert isinstance(item, StreamEvent)
            kinds.append(item.kind)
        assert fake.socket.closed and set(kinds) <= {StreamEventKind.HEARTBEAT, StreamEventKind.DISCONNECTED}
    finally:
        await adapter.close()


async def test_socket_loss_then_reconnect_reports_gap_and_resubscribes(fake: FakeCTrader) -> None:
    adapter = await connected(fake)
    orders = adapter.stream_order_updates()
    ticks = adapter.stream_ticks(["EURUSD"])
    try:
        await take(orders, 1)
        await take(ticks, 1)
        fake.socket.drop()
        [lost] = await take(orders, 1)
        [lost_p] = await take(ticks, 1)
        assert lost.kind == StreamEventKind.DISCONNECTED and lost_p.kind == StreamEventKind.DISCONNECTED  # type: ignore[union-attr]
        with pytest.raises(SessionExpiredError):
            await adapter.fetch_positions()
        fake.spot_subscriptions.clear()
        await adapter.connect()
        assert len(fake.sockets) == 2
        gap, resub = await take(orders, 2)
        assert isinstance(gap, StreamEvent) and gap.kind == StreamEventKind.GAP
        assert isinstance(resub, StreamEvent) and resub.kind == StreamEventKind.SUBSCRIBED
        gap_p, resub_p = await take(ticks, 2)
        assert gap_p.kind == StreamEventKind.GAP and resub_p.kind == StreamEventKind.SUBSCRIBED  # type: ignore[union-attr]
        assert fake.spot_subscriptions == {EURUSD}
        assert fake.sockets[1].sent[0]["payloadType"] == 2100
    finally:
        await close_gen(orders)
        await close_gen(ticks)
        await adapter.close()


@pytest.mark.parametrize("event", [
    (2147, {"ctidTraderAccountIds": [ACCOUNT], "reason": "revoked"}),
    (2164, {"ctidTraderAccountId": ACCOUNT}),
    (2148, {"reason": "server shutdown"}),
])
async def test_auth_lost_events(fake: FakeCTrader, event: tuple[int, dict[str, Any]]) -> None:
    adapter = await connected(fake)
    orders = adapter.stream_order_updates()
    account = adapter.stream_account()
    try:
        await take(orders, 1)
        await take(account, 1)
        fake.push(*event)
        [a] = await take(orders, 1)
        [b] = await take(account, 1)
        assert a.kind == StreamEventKind.AUTH_LOST and b.kind == StreamEventKind.AUTH_LOST  # type: ignore[union-attr]
        with pytest.raises(SessionExpiredError):
            await adapter.fetch_open_orders()
    finally:
        await adapter.close()


async def test_order_stream_overflow_opens_a_gap(fake: FakeCTrader) -> None:
    adapter = await connected(fake, queue_size=2)
    orders = adapter.stream_order_updates()
    try:
        await take(orders, 1)
        for i in range(5):
            fake.push(2126, {"ctidTraderAccountId": ACCOUNT, "executionType": 2,
                             "order": order(1000 + i, volume=100_000)})
        await wait_until(lambda: adapter._channels["orders"].gaps_opened == 1)
        items = await take(orders, 3)
        assert isinstance(items[0], StreamEvent) and items[0].kind == StreamEventKind.GAP
        assert [i.broker_order_id for i in items[1:] if isinstance(i, OrderDTO)] == ["1000", "1001"]
    finally:
        await adapter.close()


async def test_price_overflow_is_counted_per_symbol(fake: FakeCTrader) -> None:
    """design PR6: spot frames dropped for a full price channel are counted per canonical symbol (monotonic,
    never a gap); a symbol id without a known symbol is keyed by its id."""
    adapter = await connected(fake, queue_size=2)
    ticks = adapter.stream_ticks(["EURUSD", "USDJPY"])
    try:
        [sub] = await take(ticks, 1)
        assert isinstance(sub, StreamEvent) and sub.kind == StreamEventKind.SUBSCRIBED
        for i in range(4):
            fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": EURUSD, "bid": 108_500 + i, "ask": 108_520})
        fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": USDJPY, "bid": 15_512_300, "ask": 15_513_000})
        await wait_until(lambda: adapter._channels["prices"].dropped_frames == 3)
        assert adapter.dropped_price_frames == {"EURUSD": 2, "USDJPY": 1}
        adapter._count_price_drop(Frame(2131, {"symbolId": 999_999}, None))
        assert adapter.dropped_price_frames == {"EURUSD": 2, "USDJPY": 1, "999999": 1}
        items = await take(ticks, 2)
        assert all(isinstance(i, PriceDTO) for i in items)          # no GAP for prices
    finally:
        await close_gen(ticks)
        await adapter.close()


async def test_fetch_before_connect_is_session_expired(fake: FakeCTrader) -> None:
    adapter = make_adapter(fake)
    with pytest.raises(SessionExpiredError):
        await adapter.fetch_positions()


async def test_probe_prints_no_secrets(fake: FakeCTrader, capsys: pytest.CaptureFixture[str]) -> None:
    fake.positions = [position(501)]
    adapter = make_adapter(fake, allow_token_refresh=False)
    code = await run_probe(adapter._settings, adapter=adapter)
    out = capsys.readouterr()
    assert code == 0, out.err
    assert "balance=10000.50 AUD" in out.out and "open positions: 1" in out.out
    for secret in (CLIENT_SECRET, ACCESS_1, REFRESH_1, ACCESS_2):
        assert secret not in out.out and secret not in out.err
    assert set(fake.types_sent()) <= {2100, 2149, 2102, 2112, 2121, 2104, 2124, 2114, 2116, 51}
