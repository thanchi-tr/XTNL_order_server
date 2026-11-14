"""StoneXAdapter end to end against a fake CIAPI server and a fake Lightstreamer transport."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, TypeVar

import pytest

from src.adapters.base import (
    BrokerError,
    SessionExpiredError,
    StreamEvent,
    StreamEventKind,
    assert_read_only,
)
from src.adapters.stonex.adapter import ACCOUNT_FIELDS, StoneXAdapter
from src.adapters.stonex.orders import ORDER_FIELDS
from src.dto import AccountDTO, AccountLive, OrderDTO, OrderStatus, PositionDTO, PriceDTO

from .conftest import (
    AUDUSD_ID,
    CLIENT,
    EURUSD_ID,
    SESSION,
    TRADING,
    USDJPY_ID,
    FakeStoneX,
    FakeStreamTransport,
    make_adapter,
    wait_until,
    wcf,
)

T0 = 1_767_225_600_000
T = TypeVar("T")


async def take(gen: AsyncIterator[T], n: int, timeout: float = 2.0) -> list[T]:
    out: list[T] = []
    for _ in range(n):
        out.append(await asyncio.wait_for(gen.__anext__(), timeout))
    return out


def position_row(order_id: int = 555, qty: int = 25000) -> dict[str, Any]:
    return {"OrderId": order_id, "MarketId": int(EURUSD_ID), "Direction": "buy", "Quantity": qty,
            "Price": 1.0855, "TradingAccountId": int(TRADING), "LastChangedDateTimeUTC": wcf(T0),
            "StopOrder": {"OrderId": order_id + 1, "TriggerPrice": 1.08, "Quantity": qty}}


def order_frame(order_id: str, status: str, **kw: str | None) -> dict[str, str | None]:
    values: dict[str, str | None] = {
        "OrderId": order_id, "Direction": "1", "Type": "1", "ExecutionPrice": "0", "LastChangedTime": wcf(T0),
        "Status": status, "Quantity": "10000", "OpenPrice": "1.08500", "ReasonId": "1", "MarketId": EURUSD_ID,
        "TradingAccountId": TRADING, "OriginalQuantity": "10000"}
    values.update(kw)
    return values


def test_adapter_is_read_only_and_declares_versioning() -> None:
    assert_read_only(StoneXAdapter)
    assert StoneXAdapter.adapter_id == "stonex"
    v = StoneXAdapter.VERSIONING
    assert not v["ORDER"].has_broker_seq and v["ORDER"].has_broker_time
    assert not v["POSITION"].has_broker_seq and v["POSITION"].has_broker_time
    assert not v["ACCOUNT"].has_broker_seq and not v["ACCOUNT"].has_broker_time


async def test_connect_runs_lease_check_and_discovers_accounts(fake_broker: FakeStoneX) -> None:
    checks: list[str] = []
    adapter = make_adapter(fake_broker, pre_connect_check=lambda: checks.append("lease"))
    await adapter.connect()
    assert checks == ["lease"]
    directory = adapter.account_directory
    assert directory is not None and directory.client_ids == (CLIENT,) and directory.currency_of(CLIENT) == "AUD"
    await adapter.close()
    assert fake_broker.paths()[-1] == "/TradingAPI/session/deleteSession"


async def test_lease_check_failure_prevents_logon(fake_broker: FakeStoneX) -> None:
    def fenced() -> None:
        raise RuntimeError("lease expired")

    adapter = make_adapter(fake_broker, pre_connect_check=fenced)
    with pytest.raises(RuntimeError):
        await adapter.connect()
    assert fake_broker.requests == []


async def test_fetch_before_connect_is_session_expired(fake_broker: FakeStoneX) -> None:
    with pytest.raises(SessionExpiredError):
        await make_adapter(fake_broker).fetch_positions()


async def test_snapshot_fetches(fake_broker: FakeStoneX) -> None:
    fake_broker.positions = [position_row()]
    fake_broker.actives = [
        {"OrderId": 556, "ParentOrderId": None, "MarketId": int(EURUSD_ID), "Direction": "Sell", "Quantity": 25000,
         "TriggerPrice": 1.08, "TradingAccountId": int(TRADING), "Type": "Stop", "Status": 2},
        {"OrderId": 600, "ParentOrderId": None, "MarketId": int(USDJPY_ID), "Direction": "Buy", "Quantity": 5000,
         "TriggerPrice": 150.25, "TradingAccountId": int(TRADING), "Type": "Limit", "Status": 2,
         "IfDone": [{"Stop": {"OrderId": 601, "TriggerPrice": 149.5}}]},
    ]
    fake_broker.history = [{"OrderId": 9, "OpeningOrderIds": [8], "RealisedPnl": 33.5, "RealisedPnlCurrency": "AUD",
                            "ExecutedDateTimeUtc": wcf(T0 + 5)}]
    adapter = make_adapter(fake_broker)
    await adapter.connect()

    specs = await adapter.fetch_instruments()
    # held markets plus the pair converting a USD pnl into the AUD account currency (no AUD/JPY market listed)
    assert {s.symbol for s in specs} == {"EURUSD", "USDJPY", "AUDUSD"}

    (pos,) = await adapter.fetch_positions()
    assert (pos.position_id, pos.account_id, pos.units, pos.source) == ("555", CLIENT, Decimal(25000), "REST")

    orders = {o.broker_order_id: o for o in await adapter.fetch_open_orders()}
    assert set(orders) == {"556", "600", "601"}
    assert orders["556"].is_reduce_only and orders["556"].linked_position_id == "555"   # attached to position
    assert not orders["600"].is_reduce_only and orders["600"].symbol == "USDJPY"
    child = orders["601"]
    assert child.is_reduce_only and child.linked_position_id == "600" and child.side == "SELL"
    assert child.symbol == "USDJPY" and child.ordered_units == Decimal(5000)

    accounts, lives = await adapter.fetch_account_snapshot()
    assert accounts[0].balance == Decimal("100000.5") and accounts[0].currency == "AUD"
    assert lives[0].equity == Decimal("100150.25") and lives[0].margin_used == Decimal(2400)

    flows = await adapter.fetch_cash_flows(T0)
    assert [(f.transaction_id, f.amount) for f in flows] == [("stonex:trade:9", Decimal("33.5"))]
    history_req = [r for r in fake_broker.requests if r.url.path.endswith("/tradehistory")][0]
    assert history_req.url.params["from"] == str(T0 // 1000)
    await adapter.close()


async def test_unknown_market_triggers_rate_limited_refresh(fake_broker: FakeStoneX) -> None:
    clock = [1000.0]
    fake_broker.positions = [position_row()]
    del fake_broker.markets[EURUSD_ID]      # market info not available yet
    adapter = make_adapter(fake_broker, monotonic=lambda: clock[0])
    await adapter.connect()
    (pos,) = await adapter.fetch_positions()
    assert pos.instrument_unknown and pos.units is None and pos.raw_qty == Decimal(25000)
    info_calls = lambda: sum(1 for r in fake_broker.requests if r.url.path == "/v2/market/information")  # noqa: E731
    assert info_calls() == 1
    await adapter.fetch_positions()
    assert info_calls() == 1                  # rate limited (< 60 s)
    from .conftest import MARKETS
    fake_broker.markets[EURUSD_ID] = MARKETS[EURUSD_ID]
    clock[0] += 61
    (pos2,) = await adapter.fetch_positions()
    assert info_calls() == 2 and not pos2.instrument_unknown and pos2.symbol == "EURUSD"


async def test_fetch_by_id(fake_broker: FakeStoneX) -> None:
    fake_broker.order_positions["555"] = position_row()
    fake_broker.orders["777"] = {"StopLimitOrder": {"OrderId": 777, "MarketId": int(EURUSD_ID), "Direction": "Sell",
                                                    "Quantity": 25000, "TriggerPrice": 1.08, "Status": 2,
                                                    "TradingAccountId": int(TRADING)}, "TradeOrder": None}
    fake_broker.order_active["777"] = {"OrderId": 777, "ParentOrderId": 555}
    fake_broker.orders["888"] = {"TradeOrder": {"OrderId": 888, "MarketId": int(EURUSD_ID), "Direction": "Buy",
                                                "Quantity": 1000, "Price": 1.09, "Status": 3,
                                                "TradingAccountId": int(TRADING)}}
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    pos = await adapter.fetch_position_status("555")
    assert pos is not None and pos.status == "OPEN"
    assert await adapter.fetch_position_status("999") is None
    stop = await adapter.fetch_order_status("777")
    assert stop is not None and stop.is_reduce_only and stop.linked_position_id == "555"
    trade = await adapter.fetch_order_status("888")
    assert trade is not None and trade.status == OrderStatus.FILLED and trade.remaining_units == 0
    assert await adapter.fetch_order_status("999") is None


async def test_order_stream_maps_frames_from_the_library_thread(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    fake_broker.order_active["701"] = {"OrderId": 701, "ParentOrderId": 700}
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    assert fake_stream.connected_as == ("probe-user", SESSION)
    sub = fake_stream.find("ORDERS")
    assert sub.mode == "DISTINCT"  # every order state change is its own event; MERGE could fold one away
    assert sub.items == [f"ID.{CLIENT}"] and sub.fields == list(ORDER_FIELDS)

    fake_stream.event("ORDERS", StreamEventKind.SUBSCRIBED)
    ev = await asyncio.wait_for(first, 2)
    assert isinstance(ev, StreamEvent) and ev.kind == StreamEventKind.SUBSCRIBED and ev.stream == "orders"

    fake_stream.emit("ORDERS", order_frame("700", "3", LastChangedTime=wcf(T0 + 1)))
    filled, opened = await take(gen, 2)
    assert isinstance(filled, OrderDTO) and filled.status == OrderStatus.FILLED
    assert isinstance(opened, PositionDTO) and opened.status == "OPEN" and opened.broker_updated_at_ms == T0 + 1

    fake_stream.emit("ORDERS", order_frame("701", "2", Type="2", Direction="0", OpenPrice="1.08000",
                                           LastChangedTime=wcf(T0 + 2)))
    (stop,) = await take(gen, 1)
    assert isinstance(stop, OrderDTO) and stop.is_reduce_only and stop.linked_position_id == "700"
    assert "/TradingAPI/order/701/activestoplimitorder" in fake_broker.paths()
    assert adapter.stream_watermark_ms == T0 + 2

    fake_stream.emit("ORDERS", order_frame("702", "2", Direction=None))   # side unknown anywhere -> GAP
    (gap,) = await take(gen, 1)
    assert isinstance(gap, StreamEvent) and gap.kind == StreamEventKind.GAP
    assert "/TradingAPI/order/702" in fake_broker.paths()                 # it was looked up first

    # Live finding (2026-11-13): a new order's first ORDERS item can come without Direction. The side is read
    # by id in the background (never inline) and the held item then maps without a gap.
    fake_broker.orders["703"] = {"TradeOrder": {"OrderId": 703, "Direction": "Buy"}, "StopLimitOrder": None}
    fake_stream.emit("ORDERS", order_frame("703", "2", Direction=None, LastChangedTime=wcf(T0 + 3)))
    (late,) = await take(gen, 1)
    assert isinstance(late, OrderDTO) and late.broker_order_id == "703" and late.side == "BUY"

    await gen.aclose()
    assert not sub.active   # unsubscribed on close
    await adapter.close()
    assert fake_stream.disconnects == 1


async def test_order_overflow_opens_a_gap_and_never_blocks(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream, queue_size=2)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    for i in range(5):
        fake_stream.emit("ORDERS", order_frame(str(800 + i), "2", Type="1"))
    out = [await asyncio.wait_for(first, 2), *await take(gen, 2)]
    kinds = [type(x).__name__ if not isinstance(x, StreamEvent) else x.kind.value for x in out]
    assert kinds[0] == "GAP" and kinds[1:] == ["OrderDTO", "OrderDTO"]
    await gen.aclose()
    await adapter.close()


async def test_disconnect_and_subscription_errors_become_events(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    fake_stream.event("ORDERS", StreamEventKind.ERROR, "code=17 bad adapter")
    err = await asyncio.wait_for(first, 2)
    assert isinstance(err, StreamEvent) and err.kind == StreamEventKind.ERROR and "17" in err.detail
    fake_stream.status("STALLED")
    fake_stream.status("DISCONNECTED:WILL-RETRY")     # still down: no duplicate event
    (down,) = await take(gen, 1)
    assert isinstance(down, StreamEvent) and down.kind == StreamEventKind.DISCONNECTED
    assert fake_stream.on_server_error is not None
    fake_stream.on_server_error(1, "user/password check failed")
    (auth,) = await take(gen, 1)
    assert isinstance(auth, StreamEvent) and auth.kind == StreamEventKind.AUTH_LOST
    await gen.aclose()
    await adapter.close()


async def test_heartbeat_only_while_connected(fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream, heartbeat_s=0.05)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    (hb,) = await take(gen, 1)
    assert isinstance(hb, StreamEvent) and hb.kind == StreamEventKind.HEARTBEAT
    fake_stream.status("DISCONNECTED")
    (down,) = await take(gen, 1)
    assert isinstance(down, StreamEvent) and down.kind == StreamEventKind.DISCONNECTED
    nxt = asyncio.ensure_future(gen.__anext__())
    await asyncio.sleep(0.2)
    assert not nxt.done()          # no heartbeat while the socket is down
    nxt.cancel()
    await adapter.close()


async def test_account_stream(fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    gen = adapter.stream_account()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    sub = fake_stream.find("CLIENTACCOUNTMARGIN")
    assert sub.mode == "MERGE"     # likewise for CLIENTACCOUNTMARGIN
    assert sub.items == [f"ID.{CLIENT}"] and sub.fields == list(ACCOUNT_FIELDS)
    frame = {"Cash": "1000", "Margin": "50", "NetEquity": "1010", "OpenTradeEquity": "10",
             "TradeableFunds": "960", "TotalMarginRequirement": "50", "CurrencyISO": "AUD"}
    fake_stream.emit("CLIENTACCOUNTMARGIN", frame)
    account = await asyncio.wait_for(first, 2)
    (live,) = await take(gen, 1)
    assert isinstance(account, AccountDTO) and account.balance == 1000 and account.account_id == CLIENT
    assert account.broker_updated_at_ms == 0 and account.source == "STREAM"
    assert isinstance(live, AccountLive) and live.free_margin == 960
    fake_stream.emit("CLIENTACCOUNTMARGIN", frame | {"NetEquity": "1020", "OpenTradeEquity": "20"})
    (live2,) = await take(gen, 1)          # balance unchanged: AccountLive only
    assert isinstance(live2, AccountLive) and live2.equity == 1020
    fake_stream.emit("CLIENTACCOUNTMARGIN", frame | {"Cash": "1005"})
    acct2, _ = await take(gen, 2)
    assert isinstance(acct2, AccountDTO) and acct2.balance == 1005
    await gen.aclose()
    await adapter.close()


async def test_price_stream_resolves_symbols_and_drops_on_overflow(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream, queue_size=2)
    await adapter.connect()
    gen = adapter.stream_ticks(["EURUSD", "NOPE"])
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    sub = fake_stream.find("PRICES", f"ID.{EURUSD_ID}")
    assert sub.fields == ["MarketId", "TickDateRaw", "Bid", "Offer", "StatusSummary"]
    assert sub.mode == "MERGE"     # live StoneX feeds PRICES only to MERGE subscriptions (DISTINCT gets nothing)
    assert "/TradingAPI/market/search" in fake_broker.paths()

    tick = {"MarketId": EURUSD_ID, "TickDateRaw": str(T0), "Bid": "1.08600", "Offer": "1.08610",
            "StatusSummary": "0"}
    fake_stream.emit("PRICES", tick | {"MarketId": AUDUSD_ID})       # crossed subscription: ignored
    for i in range(5):
        fake_stream.emit("PRICES", tick | {"TickDateRaw": str(T0 + i)})
    got = [await asyncio.wait_for(first, 2), *await take(gen, 1)]
    assert all(isinstance(p, PriceDTO) and p.symbol == "EURUSD" for p in got)
    assert [p.timestamp_ms for p in got if isinstance(p, PriceDTO)] == [T0, T0 + 1]   # rest dropped, no GAP
    assert adapter._channels["prices"].dropped_frames == 3

    await adapter.ensure_ticks(["USDJPY", "EURUSD"])
    assert fake_stream.find("PRICES", f"ID.{USDJPY_ID}").active
    assert sum(1 for s in fake_stream.subs if s.data_adapter == "PRICES") == 2   # EURUSD not subscribed twice
    fake_stream.emit("PRICES", {"MarketId": USDJPY_ID, "TickDateRaw": str(T0), "Bid": "150.10", "Offer": "150.12",
                                "StatusSummary": "1"}, item=f"ID.{USDJPY_ID}")
    (jpy,) = await take(gen, 1)
    assert isinstance(jpy, PriceDTO) and jpy.symbol == "USDJPY" and jpy.spread_pips == 2 and not jpy.tradeable
    await gen.aclose()
    await adapter.close()


async def test_ensure_ticks_before_stream_is_remembered(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream, symbol_market_ids={"AUDUSD": AUDUSD_ID})
    await adapter.connect()
    await adapter.ensure_ticks(["AUDUSD"])
    gen = adapter.stream_ticks([])
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    assert fake_stream.find("PRICES").items == [f"ID.{AUDUSD_ID}"]
    first.cancel()
    await adapter.close()


async def test_reconnect_replaces_the_streaming_session(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    await adapter.connect()       # re-authentication: old Lightstreamer session is dropped
    ev = await asyncio.wait_for(first, 2)
    assert isinstance(ev, StreamEvent) and ev.kind == StreamEventKind.DISCONNECTED
    assert fake_stream.disconnects == 1
    await gen.aclose()
    await adapter.close()


async def test_second_consumer_replaces_the_first(fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    gen1 = adapter.stream_order_updates()
    t1 = asyncio.ensure_future(gen1.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    gen2 = adapter.stream_order_updates()
    t2 = asyncio.ensure_future(gen2.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 2)
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(t1, 2)
    assert not fake_stream.subs[0].active and fake_stream.subs[1].active
    fake_stream.event("ORDERS", StreamEventKind.SUBSCRIBED)
    assert isinstance(await asyncio.wait_for(t2, 2), StreamEvent)
    await gen2.aclose()
    await adapter.close()


async def test_probe_prints_no_secrets(fake_broker: FakeStoneX, capsys: pytest.CaptureFixture[str]) -> None:
    from src.adapters.stonex.probe import run

    from .conftest import make_settings
    fake_broker.positions = [position_row()]
    adapter = make_adapter(fake_broker)
    assert await run(make_settings(), adapter=adapter) == 0
    out = capsys.readouterr().out
    assert CLIENT in out and "EURUSD" in out and "open positions: 1" in out
    for secret in ("pw-SECRET", "key-SECRET", SESSION):
        assert secret not in out
    assert "pw-SECRET" not in repr(make_settings())


async def test_probe_reports_broker_errors(fake_broker: FakeStoneX, capsys: pytest.CaptureFixture[str]) -> None:
    from src.adapters.stonex.probe import run

    from .conftest import make_settings
    fake_broker.logon_body = {"Session": SESSION, "PasswordChangeRequired": True}
    assert await run(make_settings(), adapter=make_adapter(fake_broker)) == 1
    assert "AuthFailedError" in capsys.readouterr().err


async def test_margin_without_cash_is_an_error(fake_broker: FakeStoneX) -> None:
    fake_broker.margin = {"NetEquity": 1}
    adapter = make_adapter(fake_broker)
    await adapter.connect()
    with pytest.raises(BrokerError):
        await adapter.fetch_account_snapshot()


# --- single-flight Lightstreamer session (review finding stonex-concurrent-transport-race) ---------------
class SlowStreamTransport(FakeStreamTransport):
    """A FakeStreamTransport whose connect blocks the worker thread, and that records unsubscribes."""

    def __init__(self, delay_s: float = 0.1, fail: bool = False) -> None:
        super().__init__()
        self.delay_s, self.fail = delay_s, fail
        self.unsubscribed: list[object] = []

    def connect(self, user: str, password: str, on_status: Any, on_server_error: Any) -> None:
        import time
        time.sleep(self.delay_s)
        if self.fail:
            raise ConnectionError("lightstreamer unreachable")
        super().connect(user, password, on_status, on_server_error)

    def unsubscribe(self, handle: object) -> None:
        self.unsubscribed.append(handle)
        super().unsubscribe(handle)


def counting_factory(*specs: dict[str, Any]) -> tuple[list[SlowStreamTransport], Any]:
    made: list[SlowStreamTransport] = []

    def factory() -> SlowStreamTransport:
        t = SlowStreamTransport(**(specs[len(made)] if len(made) < len(specs) else {}))
        made.append(t)
        return t
    return made, factory


async def test_concurrent_streams_share_one_lightstreamer_session(fake_broker: FakeStoneX) -> None:
    made, factory = counting_factory()
    adapter = make_adapter(fake_broker, stream_transport_factory=factory)
    await adapter.connect()
    await adapter.ensure_ticks(["AUDUSD"])
    gens: list[AsyncIterator[Any]] = [adapter.stream_order_updates(), adapter.stream_account(),
                                      adapter.stream_ticks([])]
    firsts = [asyncio.ensure_future(g.__anext__()) for g in gens]   # the orchestrator starts all pumps at once
    await wait_until(lambda: sum(len(t.subs) for t in made) == 3)
    assert len(made) == 1, f"{len(made)} Lightstreamer sessions for one adapter"
    only = made[0]
    assert {s.data_adapter for s in only.subs} == {"ORDERS", "CLIENTACCOUNTMARGIN", "PRICES"}
    only.status("DISCONNECTED")            # one lost socket ends every stream on it
    for fut in firsts:
        ev = await asyncio.wait_for(fut, 2)
        assert isinstance(ev, StreamEvent) and ev.kind == StreamEventKind.DISCONNECTED
    for g in gens:
        await g.aclose()   # type: ignore[attr-defined]
    await adapter.close()
    assert only.disconnects == 1


async def test_close_during_connect_disconnects_the_pending_session(fake_broker: FakeStoneX) -> None:
    made, factory = counting_factory({"delay_s": 0.2})
    adapter = make_adapter(fake_broker, stream_transport_factory=factory)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(made) == 1)
    await adapter.close()                 # before the Lightstreamer connect returns
    with pytest.raises(BrokerError):
        await asyncio.wait_for(first, 2)
    assert made[0].disconnects == 1 and made[0].subs == []


async def test_failed_connect_is_shared_then_retried(fake_broker: FakeStoneX) -> None:
    made, factory = counting_factory({"fail": True}, {})
    adapter = make_adapter(fake_broker, stream_transport_factory=factory)
    await adapter.connect()
    gens = [adapter.stream_order_updates(), adapter.stream_account()]
    results = await asyncio.gather(*(g.__anext__() for g in gens), return_exceptions=True)
    assert all(isinstance(r, ConnectionError) for r in results) and len(made) == 1
    assert made[0].disconnects == 1
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(made) == 2 and len(made[1].subs) == 1)
    first.cancel()
    await adapter.close()


async def test_release_unsubscribes_on_the_session_that_created_the_handles(fake_broker: FakeStoneX) -> None:
    made, factory = counting_factory({"delay_s": 0}, {"delay_s": 0})
    adapter = make_adapter(fake_broker, stream_transport_factory=factory)
    await adapter.connect()
    orders = adapter.stream_order_updates()
    t1 = asyncio.ensure_future(orders.__anext__())
    await wait_until(lambda: len(made) == 1 and len(made[0].subs) == 1)
    await adapter.connect()               # re-auth drops session #1
    assert isinstance(await asyncio.wait_for(t1, 2), StreamEvent)
    account = adapter.stream_account()
    t2 = asyncio.ensure_future(account.__anext__())
    await wait_until(lambda: len(made) == 2 and len(made[1].subs) == 1)
    await orders.aclose()  # type: ignore[attr-defined]  # releases handles that belong to session #1
    assert made[1].unsubscribed == [] and made[1].subs[0].active
    t2.cancel()
    await adapter.close()


async def test_reauth_during_connect_supersedes_the_old_token_session(fake_broker: FakeStoneX) -> None:
    """A re-auth while the first Lightstreamer connect is still in flight must not let that
    connect (which carries the old session token) become the adapter's session."""
    made, factory = counting_factory({"delay_s": 0.2}, {"delay_s": 0})
    adapter = make_adapter(fake_broker, stream_transport_factory=factory)
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(made) == 1)
    await adapter.connect()               # new LogOn while session #1 is still connecting
    with pytest.raises(BrokerError):
        await asyncio.wait_for(first, 2)
    assert made[0].disconnects == 1 and made[0].subs == []
    assert adapter._transport is None
    gen2 = adapter.stream_order_updates()
    nxt = asyncio.ensure_future(gen2.__anext__())
    await wait_until(lambda: len(made) == 2 and len(made[1].subs) == 1)
    assert adapter._transport is made[1] and made[1].disconnects == 0
    nxt.cancel()
    await adapter.close()


async def test_waiter_never_subscribes_on_a_session_stopped_before_it_resumed(fake_broker: FakeStoneX) -> None:
    """close() / re-auth can run after the shared connect finished but before a waiter resumes."""
    made, factory = counting_factory({"delay_s": 0.05})
    adapter = make_adapter(fake_broker, stream_transport_factory=factory)
    await adapter.connect()
    loop = asyncio.get_running_loop()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(made) == 1)
    task = adapter._connecting
    assert task is not None
    stops: list[asyncio.Task[None]] = []
    # Runs after the connect completes and before the waiter resumes (done callbacks run in order;
    # the eager task executes _stop_transport's synchronous part at once).
    task.add_done_callback(lambda _t: stops.append(
        asyncio.eager_task_factory(loop, adapter._stop_transport("stopped mid-handover"))))
    with pytest.raises(BrokerError):
        await asyncio.wait_for(first, 2)
    await asyncio.gather(*stops)
    assert made[0].subs == [] and made[0].disconnects == 1
    await adapter.close()


async def test_prices_never_subscribe_on_a_session_replaced_during_market_lookup(fake_broker: FakeStoneX) -> None:
    made, factory = counting_factory({"delay_s": 0}, {"delay_s": 0})
    adapter = make_adapter(fake_broker, stream_transport_factory=factory)
    await adapter.connect()
    release = asyncio.Event()
    entered = asyncio.Event()

    async def slow_markets(symbols: Any) -> list[str]:
        entered.set()
        await release.wait()
        return [AUDUSD_ID]
    adapter._markets_for = slow_markets  # type: ignore[method-assign]
    gen = adapter.stream_ticks(["AUDUSD"])
    first = asyncio.ensure_future(gen.__anext__())
    await asyncio.wait_for(entered.wait(), 2)
    await adapter.connect()               # re-auth stops session #1 during the REST lookup
    release.set()
    with pytest.raises(BrokerError):
        await asyncio.wait_for(first, 2)
    assert len(made) == 1 and made[0].subs == [] and made[0].disconnects == 1
    await adapter.close()


async def test_empty_price_stream_reports_subscribed_and_later_symbols_still_attach(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    # Live finding (2026-11-13): before the first snapshot nothing is held, so SM4 asks for no symbols; the
    # stream must still report SUBSCRIBED or SM4's subscribe timeout closes the socket in a loop.
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    gen = adapter.stream_ticks([])
    first = await asyncio.wait_for(gen.__anext__(), 2)
    assert isinstance(first, StreamEvent) and first.kind is StreamEventKind.SUBSCRIBED and first.stream == "prices"
    assert not [s for s in fake_stream.subs if s.data_adapter == "PRICES"]
    await adapter.ensure_ticks(["EURUSD"])
    assert fake_stream.find("PRICES", f"ID.{EURUSD_ID}").active
    await gen.aclose()
    await adapter.close()
