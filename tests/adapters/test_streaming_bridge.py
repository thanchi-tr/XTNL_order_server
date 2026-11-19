"""Lightstreamer bridge: listener duck-typing against the real library, FrameChannel hand-over."""
from __future__ import annotations

import asyncio
import threading
from typing import Any
from unittest.mock import MagicMock

import lightstreamer.client as ls

from src.adapters.base import StreamEvent, StreamEventKind
from src.adapters.stonex.streaming import (
    BoundSink,
    FrameChannel,
    LightstreamerTransport,
    _ClientListener,
    _SubscriptionListener,
)


class RecordingSink:
    def __init__(self) -> None:
        self.frames: list[dict[str, str | None]] = []
        self.events: list[tuple[StreamEventKind, str]] = []
        self.lost: list[int] = []
        self.markets: list[str | None] = []      # the market of each event / lost-update call, in order

    def on_frame(self, values: dict[str, str | None]) -> None:
        self.frames.append(values)

    def on_event(self, kind: StreamEventKind, detail: str, market: str | None = None) -> None:
        self.events.append((kind, detail))
        self.markets.append(market)

    def on_lost(self, count: int, market: str | None = None) -> None:
        self.lost.append(count)
        self.markets.append(market)


def public(cls: type) -> set[str]:
    return {n for n in vars(cls) if n.startswith("on")}


def test_listeners_define_every_library_callback() -> None:
    assert public(ls.SubscriptionListener) <= set(dir(_SubscriptionListener))
    assert public(ls.ClientListener) <= set(dir(_ClientListener))


def test_subscription_listener_maps_callbacks_and_never_raises() -> None:
    sink = RecordingSink()
    listener = _SubscriptionListener(["OrderId", "Status"], sink)
    update = MagicMock()
    update.getValue.side_effect = lambda f: {"OrderId": "1", "Status": "2"}.get(f)
    listener.onItemUpdate(update)
    listener.onSubscription()
    listener.onSubscriptionError(17, "bad adapter")
    listener.onUnsubscription()
    listener.onItemLostUpdates("ID.1", 1, 3)
    assert sink.frames == [{"OrderId": "1", "Status": "2"}]
    assert [k for k, _ in sink.events] == [StreamEventKind.SUBSCRIBED, StreamEventKind.ERROR,
                                           StreamEventKind.UNSUBSCRIBED]
    assert sink.lost == [3]
    broken = MagicMock()
    broken.getValue.side_effect = RuntimeError("haxe blew up")
    listener.onItemUpdate(broken)        # swallowed: must never escape into the library thread


def test_client_listener_forwards_status_and_errors() -> None:
    seen: list[Any] = []
    listener = _ClientListener(seen.append, lambda c, m: seen.append((c, m)))
    listener.onStatusChange("CONNECTED:WS-STREAMING")
    listener.onServerError("1", "user/password check failed")
    assert seen == ["CONNECTED:WS-STREAMING", (1, "user/password check failed")]


def test_lightstreamer_transport_builds_a_distinct_subscription_without_network() -> None:
    transport = LightstreamerTransport("https://push.sx.test")
    client = MagicMock()
    transport._client = client
    handle = transport.subscribe("ORDERS", ["ID.1001"], ["OrderId", "Status"], RecordingSink())
    assert isinstance(handle, ls.Subscription)
    assert handle.getMode() == "DISTINCT" and handle.getDataAdapter() == "ORDERS"
    assert list(handle.getItems()) == ["ID.1001"] and list(handle.getFields()) == ["OrderId", "Status"]
    client.subscribe.assert_called_once_with(handle)
    transport.unsubscribe(handle)
    client.unsubscribe.assert_called_once_with(handle)
    transport.disconnect()
    client.disconnect.assert_called_once()


def test_bound_sink_injects_key_and_rejects_crossed_frames() -> None:
    sink = RecordingSink()
    bound = BoundSink(sink, "_market", "42", check_field="MarketId")
    bound.on_frame({"MarketId": "42", "Bid": "1"})
    bound.on_frame({"MarketId": "43", "Bid": "1"})
    bound.on_frame({"MarketId": None, "Bid": "2"})
    assert [f["_market"] for f in sink.frames] == ["42", "42"] and bound.mismatched == 1
    bound.on_event(StreamEventKind.SUBSCRIBED, "")
    assert sink.events == [(StreamEventKind.SUBSCRIBED, "_market=42")]


def channel(loop: asyncio.AbstractEventLoop, *, drop: bool, maxsize: int = 2,
            connected: bool = True) -> FrameChannel:
    return FrameChannel("orders" if not drop else "prices", loop, maxsize=maxsize, drop_when_full=drop,
                        heartbeat_s=60, is_connected=lambda: connected, clock_ms=lambda: 7)


def from_thread(fn: Any, *args: Any) -> None:
    t = threading.Thread(target=fn, args=args)
    t.start()
    t.join()


async def test_channel_hands_frames_over_from_another_thread() -> None:
    ch = channel(asyncio.get_running_loop(), drop=False)
    from_thread(ch.on_frame, {"a": "1"})
    from_thread(ch.on_event, StreamEventKind.SUBSCRIBED, "x")
    first = await asyncio.wait_for(ch.get(), 1)
    second = await asyncio.wait_for(ch.get(), 1)
    assert isinstance(first, StreamEvent) and first.kind == StreamEventKind.SUBSCRIBED and first.at_ms == 7
    assert second == {"a": "1"}


async def test_order_channel_overflow_is_one_gap_event() -> None:
    ch = channel(asyncio.get_running_loop(), drop=False)
    for i in range(5):
        from_thread(ch.on_frame, {"n": str(i)})
    from_thread(ch.on_lost, 2)
    await asyncio.sleep(0)
    gap = await ch.get()
    assert isinstance(gap, StreamEvent) and gap.kind == StreamEventKind.GAP and "5 frame" in gap.detail
    assert [await ch.get(), await ch.get()] == [{"n": "0"}, {"n": "1"}]
    assert ch.gaps_opened == 1


async def test_price_channel_overflow_drops_silently() -> None:
    ch = channel(asyncio.get_running_loop(), drop=True)
    for i in range(5):
        from_thread(ch.on_frame, {"n": str(i)})
    from_thread(ch.on_lost, 9)    # lost price updates are not a gap
    await asyncio.sleep(0)
    assert [await ch.get(), await ch.get()] == [{"n": "0"}, {"n": "1"}]
    assert ch.dropped_frames == 3 and ch.gaps_opened == 0
    ch.close()
    assert await ch.get() is None


async def test_channel_close_unblocks_the_consumer() -> None:
    ch = channel(asyncio.get_running_loop(), drop=False)
    waiter = asyncio.ensure_future(ch.get())
    await asyncio.sleep(0.01)
    ch.close()
    assert await asyncio.wait_for(waiter, 1) is None


async def test_callbacks_after_loop_close_are_ignored() -> None:
    loop = asyncio.new_event_loop()
    ch = FrameChannel("orders", loop, maxsize=2, drop_when_full=False, heartbeat_s=60, is_connected=lambda: True)
    loop.close()
    ch.on_frame({"a": "1"})     # must not raise into the library thread
    ch.on_event(StreamEventKind.SUBSCRIBED, "")
