"""Plan S03: price stream fields (PR2), per-market stream events and lost updates (PR6, IN-13), and the
``XTNL_STONEX_PRICE_UNFILTERED`` flag (PR3). No network: fake CIAPI, fake Lightstreamer, fake cTrader."""
from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, TypeVar
from unittest.mock import MagicMock

import lightstreamer.client as ls
import pytest

from src.adapters.base import StreamEvent, StreamEventKind
from src.adapters.stonex.mapping import map_price
from src.adapters.stonex.streaming import (
    MARKET_KEY,
    RECEIVED_KEY,
    BoundSink,
    FrameChannel,
    LightstreamerTransport,
    MarketDropCounter,
)
from src.config import StoneXSettings
from src.dto import PriceDTO

from .conftest import EURUSD_ID, USDJPY_ID, FakeStoneX, FakeStreamTransport, make_adapter, wait_until
from .test_streaming_bridge import RecordingSink

T0 = 1_767_225_600_000
T = TypeVar("T")


async def take(gen: AsyncIterator[T], n: int, timeout: float = 2.0) -> list[T]:
    return [await asyncio.wait_for(gen.__anext__(), timeout) for _ in range(n)]


def from_thread(fn: Any, *args: Any) -> None:
    t = threading.Thread(target=fn, args=args, name="ls-library-thread")
    t.start()
    t.join()


# --- map_price: ts_source and received_ms (PR2) -----------------------------------------------------
def spec() -> Any:
    from src.adapters.mock import fx_spec
    return fx_spec("EURUSD", adapter_id="stonex", broker_symbol=EURUSD_ID)


def test_map_price_broker_time_is_broker_sourced() -> None:
    p = map_price({"MarketId": EURUSD_ID, "TickDateRaw": str(T0), "Bid": "1.5", "Offer": "1.6",
                   "StatusSummary": "0"}, spec(), T0 + 120)
    assert p is not None and p.ts_source == "BROKER" and p.timestamp_ms == T0 and p.received_ms == T0 + 120


@pytest.mark.parametrize("raw", [None, "", "garbage"])
def test_map_price_without_tick_time_is_receipt_sourced(raw: str | None) -> None:
    p = map_price({"MarketId": EURUSD_ID, "TickDateRaw": raw, "Bid": "1.5", "Offer": "1.6", "StatusSummary": "0"},
                  spec(), T0 + 120)
    assert p is not None and p.ts_source == "RECEIPT" and p.timestamp_ms == T0 + 120 and p.received_ms == T0 + 120


# --- the receipt stamp is taken on the library thread ------------------------------------------------
async def test_price_channel_stamps_receipt_on_the_library_thread() -> None:
    clock_threads: list[str] = []

    def clock() -> int:
        clock_threads.append(threading.current_thread().name)
        return T0 + 42

    ch = FrameChannel("prices", asyncio.get_running_loop(), maxsize=4, drop_when_full=True, heartbeat_s=60,
                      is_connected=lambda: True, clock_ms=clock, stamp_receipt=True)
    from_thread(ch.on_frame, {"Bid": "1"})
    frame = await asyncio.wait_for(ch.get(), 1)
    assert isinstance(frame, dict) and frame[RECEIVED_KEY] == str(T0 + 42)
    assert clock_threads == ["ls-library-thread"]


async def test_orders_channel_does_not_stamp() -> None:
    ch = FrameChannel("orders", asyncio.get_running_loop(), maxsize=4, drop_when_full=False, heartbeat_s=60,
                      is_connected=lambda: True, clock_ms=lambda: 7)
    from_thread(ch.on_frame, {"OrderId": "1"})
    frame = await asyncio.wait_for(ch.get(), 1)
    assert frame == {"OrderId": "1"}


# --- per-market events and lost updates (PR6, IN-13) ----------------------------------------------------
def test_bound_sink_names_its_market_only_for_price_subscriptions() -> None:
    sink = RecordingSink()
    BoundSink(sink, MARKET_KEY, "42", check_field="MarketId").on_event(StreamEventKind.SUBSCRIBED, "")
    BoundSink(sink, "_client", "1001").on_event(StreamEventKind.SUBSCRIBED, "")
    BoundSink(sink, MARKET_KEY, "42").on_lost(3)
    assert sink.markets == ["42", None, "42"] and sink.lost == [3]


async def test_lost_price_updates_are_counted_per_market_never_a_gap() -> None:
    drops = MarketDropCounter()
    ch = FrameChannel("prices", asyncio.get_running_loop(), maxsize=1, drop_when_full=True, heartbeat_s=60,
                      is_connected=lambda: True, clock_ms=lambda: 7, drops=drops)
    eur, jpy = BoundSink(ch, MARKET_KEY, "1"), BoundSink(ch, MARKET_KEY, "2")
    from_thread(eur.on_lost, 4)
    from_thread(jpy.on_lost, 1)
    from_thread(eur.on_frame, {"Bid": "1"})
    from_thread(jpy.on_frame, {"Bid": "1"})          # channel full: dropped, counted for market 2
    await asyncio.sleep(0)
    assert drops.snapshot() == {"1": 4, "2": 2}
    assert ch.gaps_opened == 0
    frame = await asyncio.wait_for(ch.get(), 1)
    assert isinstance(frame, dict) and frame[MARKET_KEY] == "1"


async def test_lost_order_updates_still_open_a_gap() -> None:
    ch = FrameChannel("orders", asyncio.get_running_loop(), maxsize=4, drop_when_full=False, heartbeat_s=60,
                      is_connected=lambda: True, clock_ms=lambda: 7)
    from_thread(BoundSink(ch, "_client", "1001").on_lost, 2)
    ev = await asyncio.wait_for(ch.get(), 1)
    assert isinstance(ev, StreamEvent) and ev.kind is StreamEventKind.GAP and ev.market is None
    assert ch.drops.snapshot() == {}


async def test_stonex_stream_carries_stamp_market_and_drop_counts(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    def clock() -> int:     # the library thread (the fake's emit thread) reads T0 + 5, the event loop T0 + 9
        return T0 + 9 if threading.current_thread() is threading.main_thread() else T0 + 5

    adapter = make_adapter(fake_broker, fake_stream, queue_size=1, wall_ms=clock)
    await adapter.connect()
    gen = adapter.stream_ticks(["EURUSD"])
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    fake_stream.event("PRICES", StreamEventKind.SUBSCRIBED)
    sub = await asyncio.wait_for(first, 2)
    assert isinstance(sub, StreamEvent) and sub.kind is StreamEventKind.SUBSCRIBED
    assert sub.market == "EURUSD" and sub.stream == "prices"

    tick = {"MarketId": EURUSD_ID, "TickDateRaw": str(T0), "Bid": "1.08600", "Offer": "1.08610",
            "StatusSummary": "0"}
    fake_stream.emit("PRICES", tick)
    fake_stream.emit("PRICES", tick)                       # queue of one is full: dropped
    from_thread(fake_stream.find("PRICES").sink.on_lost, 2)
    (price,) = await take(gen, 1)
    assert isinstance(price, PriceDTO) and price.ts_source == "BROKER" and price.timestamp_ms == T0
    assert price.received_ms == T0 + 5                     # stamped on the library thread, not at mapping
    assert adapter.dropped_price_frames == {"EURUSD": 3}

    await adapter.ensure_ticks(["USDJPY"])
    fake_stream.event("PRICES", StreamEventKind.ERROR, "code=17", item=f"ID.{USDJPY_ID}")
    (err,) = await take(gen, 1)
    assert isinstance(err, StreamEvent) and err.kind is StreamEventKind.ERROR and err.market == "USDJPY"
    fake_stream.status("DISCONNECTED")
    (lost,) = await take(gen, 1)
    assert isinstance(lost, StreamEvent) and lost.kind is StreamEventKind.DISCONNECTED and lost.market is None
    await gen.aclose()
    await adapter.close()
    assert adapter.dropped_price_frames == {"EURUSD": 3}      # monotonic across channels


# --- XTNL_STONEX_PRICE_UNFILTERED (off by default) -----------------------------------------------------
@pytest.fixture
def max_frequency_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, object]]:
    calls: list[tuple[str, object]] = []
    original = ls.Subscription.setRequestedMaxFrequency

    def spy(self: ls.Subscription, freq: object) -> None:
        calls.append((self.getDataAdapter(), freq))
        original(self, freq)

    monkeypatch.setattr(ls.Subscription, "setRequestedMaxFrequency", spy)
    return calls


def subscribe_all(transport: LightstreamerTransport) -> None:
    transport._client = MagicMock()
    transport.subscribe("PRICES", ["ID.1"], ["Bid"], RecordingSink(), mode="MERGE")
    transport.subscribe("CLIENTACCOUNTMARGIN", ["ID.2"], ["Cash"], RecordingSink(), mode="MERGE")
    transport.subscribe("ORDERS", ["ID.2"], ["OrderId"], RecordingSink(), mode="DISTINCT")


def test_flag_off_never_calls_set_requested_max_frequency(max_frequency_calls: list[tuple[str, object]]) -> None:
    subscribe_all(LightstreamerTransport("https://push.sx.test"))
    assert max_frequency_calls == []


def test_flag_on_asks_unfiltered_for_prices_only(max_frequency_calls: list[tuple[str, object]]) -> None:
    subscribe_all(LightstreamerTransport("https://push.sx.test", price_unfiltered=True))
    assert max_frequency_calls == [("PRICES", "unfiltered")]


BASE_ENV = {"STONE_X_USERNAME": "u", "STONE_X_PASS": "p", "STONE_X_APP_KEY": "k", "BROKER_ENV": "sandbox"}


@pytest.mark.parametrize(("value", "expected"), [
    (None, False), ("", False), ("0", False), ("off", False), ("maybe", False), ("1", True), ("true", True),
])
def test_flag_is_parsed_tolerantly(value: str | None, expected: bool) -> None:
    env = dict(BASE_ENV)
    if value is not None:
        env["XTNL_STONEX_PRICE_UNFILTERED"] = value
    assert StoneXSettings.from_env(env).price_unfiltered is expected


@pytest.mark.parametrize(("value", "warned"), [
    (None, False), ("", False), ("off", False), ("NO", False), ("1", False), ("on", False), ("ture", True),
])
def test_unreadable_flag_warns_without_the_value_and_never_raises(
        value: str | None, warned: bool, caplog: pytest.LogCaptureFixture) -> None:
    env = dict(BASE_ENV)
    if value is not None:
        env["XTNL_STONEX_PRICE_UNFILTERED"] = value
    with caplog.at_level("WARNING", logger="src.config"):
        settings = StoneXSettings.from_env(env)
    assert settings.price_unfiltered is (value in ("1", "on"))
    flagged = [r.getMessage() for r in caplog.records if "XTNL_STONEX_PRICE_UNFILTERED" in r.getMessage()]
    assert bool(flagged) is warned
    assert all("ture" not in m for m in flagged)               # the name only, never the value


def test_event_for_an_unknown_market_keeps_its_market_id() -> None:
    adapter = make_adapter(FakeStoneX())
    unknown = StreamEvent(StreamEventKind.ERROR, "prices", "code=17", market="999999")
    assert adapter._market_event(unknown).market == "999999"          # never None (None = the whole stream)
    whole = StreamEvent(StreamEventKind.DISCONNECTED, "prices")
    assert adapter._market_event(whole).market is None


def test_default_transport_follows_the_flag() -> None:
    from src.adapters.stonex.adapter import StoneXAdapter
    for on in (False, True):
        env = dict(BASE_ENV, XTNL_STONEX_PRICE_UNFILTERED="1" if on else "0")
        transport = StoneXAdapter(StoneXSettings.from_env(env))._transport_factory()
        assert isinstance(transport, LightstreamerTransport) and transport._price_unfiltered is on


# --- Pepperstone (cTrader spots) ------------------------------------------------------------------------
async def test_pepperstone_spots_carry_ts_source_and_receipt() -> None:
    from .pepperstone.conftest import ACCOUNT, EURUSD, FakeCTrader, make_adapter as make_ctrader
    from .pepperstone.conftest import T0 as C_T0

    fake = FakeCTrader()
    adapter = make_ctrader(fake)
    await adapter.connect()
    await adapter.fetch_instruments()
    ticks = adapter.stream_ticks(["EURUSD"])
    try:
        (sub,) = await take(ticks, 1)
        assert isinstance(sub, StreamEvent) and sub.kind is StreamEventKind.SUBSCRIBED and sub.market is None
        fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": EURUSD, "bid": 108_512, "ask": 108_524,
                         "timestamp": C_T0 + 1})
        fake.push(2131, {"ctidTraderAccountId": ACCOUNT, "symbolId": EURUSD, "ask": 108_530})
        p1, p2 = await take(ticks, 2)
        assert isinstance(p1, PriceDTO) and p1.ts_source == "BROKER" and p1.timestamp_ms == C_T0 + 1
        assert p1.received_ms == C_T0 + 60_000                     # the fake's wall clock at the reader
        assert isinstance(p2, PriceDTO) and p2.ts_source == "RECEIPT"
        assert p2.timestamp_ms == p2.received_ms == C_T0 + 60_000
        assert p2.bid == Decimal("1.08512")
    finally:
        closer = getattr(ticks, "aclose", None)
        if closer is not None:
            await closer()
        await adapter.close()
