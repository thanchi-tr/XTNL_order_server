"""Lightstreamer -> asyncio bridge for StoneX streams (adapter-isolation.md §5, dto-contracts.md §4).

Listener callbacks run on the library's own thread. They copy field values into a plain dict and
hand it to the event loop with ``loop.call_soon_threadsafe``; they never block and never raise.
Each stream has one bounded FrameChannel:
- prices: a full channel drops the frame (backpressure_overflow: drop price frames at the parser);
- orders / account: a full channel drops the frame and emits StreamEvent(GAP) so SM4 resyncs.
A price channel stamps each frame's local receipt time on the library thread (``RECEIVED_KEY``, design PR2) and
counts dropped frames and the server's lost updates per market (``MarketDropCounter``, design PR6, IN-13).
The library sits behind the small StreamTransport protocol so tests inject a fake (no network).
"""
from __future__ import annotations

import asyncio
import collections
import logging
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from src.adapters.base import StreamEvent, StreamEventKind, StreamName
from src.adapters.sockets import install_lightstreamer_socket_options

log = logging.getLogger(__name__)

Frame = dict[str, str | None]
StatusCallback = Callable[[str], None]
ServerErrorCallback = Callable[[int, str], None]

#: Frame key the bound sink of a PRICES subscription adds: the subscribed StoneX MarketId.
MARKET_KEY = "_market"
#: Frame key a stamping channel adds on the library thread: local receipt time in ms, as a decimal string
#: (adapter-internal, never a broker field).
RECEIVED_KEY = "_xtnl_received_ms"


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class FrameSink(Protocol):
    """Receives one subscription's traffic. Called on the Lightstreamer thread."""

    def on_frame(self, values: Frame) -> None: ...

    def on_event(self, kind: StreamEventKind, detail: str, market: str | None = None) -> None: ...

    def on_lost(self, count: int, market: str | None = None) -> None: ...


class MarketDropCounter:
    """Price frames that never reached the consumer, per market (design PR6, IN-13): frames dropped for a full
    channel plus the updates the server reports lost (``onItemLostUpdates``). Monotonic, kept across channels and
    sessions, written from the library thread and the event loop (hence the lock). A frame without a market counts
    under ``""``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}

    def add(self, market: str | None, count: int) -> None:
        if count <= 0:
            return
        key = market or ""
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + count

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)


class StreamTransport(Protocol):
    """The small surface of the Lightstreamer client the adapter uses (read-only subscribes)."""

    def connect(self, user: str, password: str, on_status: StatusCallback,
                on_server_error: ServerErrorCallback) -> None: ...

    def subscribe(self, data_adapter: str, items: Sequence[str], fields: Sequence[str],
                  sink: FrameSink, *, mode: str = "DISTINCT") -> object: ...

    def unsubscribe(self, handle: object) -> None: ...

    def disconnect(self) -> None: ...


class FrameChannel:
    """Bounded hand-over of one stream's frames from the library thread into asyncio."""

    def __init__(self, stream: StreamName, loop: asyncio.AbstractEventLoop, *, maxsize: int,
                 drop_when_full: bool, heartbeat_s: float, is_connected: Callable[[], bool],
                 clock_ms: Callable[[], int] = now_ms, stamp_receipt: bool = False,
                 drops: MarketDropCounter | None = None) -> None:
        """``stamp_receipt``: add ``RECEIVED_KEY`` (``clock_ms()`` on the library thread) to every frame handed
        over. ``drops``: where a drop-mode channel counts its dropped frames and lost updates per market."""
        self.stream: StreamName = stream
        self._loop = loop
        self._maxsize = maxsize
        self._drop = drop_when_full
        self._heartbeat_s = heartbeat_s
        self._is_connected = is_connected
        self._clock_ms = clock_ms
        self._stamp = stamp_receipt
        self.drops = drops if drops is not None else MarketDropCounter()
        self._frames: asyncio.Queue[Frame] = asyncio.Queue(maxsize=maxsize)
        self._control: collections.deque[StreamEvent] = collections.deque()
        self._local: collections.deque[Frame] = collections.deque()
        self._wake = asyncio.Event()
        self._gap_frames = 0
        self._last_heartbeat = loop.time()
        self.closed = False
        self.dropped_frames = 0
        self.gaps_opened = 0

    # --- library thread side (never blocks, never raises) -----------------------------------
    def on_frame(self, values: Frame) -> None:
        if self.closed:
            return
        if self._drop and self._frames.qsize() >= self._maxsize:
            self.dropped_frames += 1   # cheap early drop of prices, before any loop hop
            self.drops.add(values.get(MARKET_KEY), 1)
            return
        if self._stamp:
            values[RECEIVED_KEY] = str(self._clock_ms())   # receipt time on the library thread (PR2)
        self._post(self._accept_frame, values)

    def on_event(self, kind: StreamEventKind, detail: str, market: str | None = None) -> None:
        if not self.closed:
            self._post(self._accept_event, StreamEvent(kind, self.stream, detail, self._clock_ms(), market))

    def on_lost(self, count: int, market: str | None = None) -> None:
        """The server dropped updates (onItemLostUpdates): a gap for orders / account; counted per market on a
        drop-mode (prices) channel, where a lost price update is not a gap (IN-13)."""
        if self.closed:
            return
        if self._drop:
            self.drops.add(market, count)
        else:
            self._post(self._open_gap, count)

    def _post(self, fn: Callable[[Any], None], arg: Any) -> None:
        try:
            self._loop.call_soon_threadsafe(fn, arg)
        except RuntimeError:  # loop closed during shutdown
            pass

    # --- event loop side -----------------------------------------------------------------------
    def _accept_frame(self, values: Frame) -> None:
        try:
            self._frames.put_nowait(values)
        except asyncio.QueueFull:
            if self._drop:
                self.dropped_frames += 1
                self.drops.add(values.get(MARKET_KEY), 1)
            else:
                self._open_gap(1)
            return
        self._wake.set()

    def _accept_event(self, event: StreamEvent) -> None:
        self._control.append(event)
        self._wake.set()

    def _open_gap(self, count: int) -> None:
        if self._gap_frames == 0:
            self.gaps_opened += 1
        self._gap_frames += count
        self._wake.set()

    def put_local(self, frame: Frame) -> None:
        """Event loop side: hand the consumer an adapter-internal frame (e.g. 'parent lookup done').
        It is delivered after pending control events and before queued broker frames, and never
        dropped for a full queue (it carries no broker data)."""
        if not self.closed:
            self._local.append(frame)
            self._wake.set()

    def put_event(self, kind: StreamEventKind, detail: str = "") -> None:
        """Enqueue a control event from the event loop."""
        self._accept_event(StreamEvent(kind, self.stream, detail, self._clock_ms()))

    def close(self) -> None:
        self.closed = True
        self._wake.set()

    async def get(self) -> Frame | StreamEvent | None:
        """Next control event, GAP, HEARTBEAT or frame; None once closed."""
        while True:
            if self._control:
                return self._control.popleft()
            if self._local:
                return self._local.popleft()
            if self._gap_frames:
                count, self._gap_frames = self._gap_frames, 0
                return StreamEvent(StreamEventKind.GAP, self.stream,
                                   f"{count} frame(s) not handed over", self._clock_ms())
            if self.closed:
                return None
            wait = self._last_heartbeat + self._heartbeat_s - self._loop.time()
            if wait <= 0:
                self._last_heartbeat = self._loop.time()
                if self._is_connected():
                    return StreamEvent(StreamEventKind.HEARTBEAT, self.stream, "", self._clock_ms())
                continue
            try:
                return self._frames.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=wait)
            except TimeoutError:
                pass

    def qsize(self) -> int:
        return self._frames.qsize()


class BoundSink:
    """Adds a bound key to each frame (the subscribed market / account) and checks it. Bound to ``MARKET_KEY``, its
    subscription events and lost updates carry the market (design PR6)."""

    def __init__(self, channel: FrameSink, key: str, value: str, *, check_field: str | None = None) -> None:
        self._channel = channel
        self._key = key
        self._value = value
        self._check = check_field
        self._market = value if key == MARKET_KEY else None
        self.mismatched = 0

    def on_frame(self, values: Frame) -> None:
        if self._check is not None:
            seen = values.get(self._check)
            if seen not in (None, "") and str(seen).strip() != self._value:
                self.mismatched += 1   # crossed subscription: never attribute it to this market
                return
        values[self._key] = self._value
        self._channel.on_frame(values)

    def on_event(self, kind: StreamEventKind, detail: str, market: str | None = None) -> None:
        self._channel.on_event(kind, f"{self._key}={self._value} {detail}".strip(), market or self._market)

    def on_lost(self, count: int, market: str | None = None) -> None:
        self._channel.on_lost(count, market or self._market)


# --- the real Lightstreamer client ---------------------------------------------------------------
class _SubscriptionListener:
    """Duck-typed lightstreamer SubscriptionListener: every callback is defined (the library
    reads them by attribute) and none of them may raise into the library thread."""

    def __init__(self, fields: Sequence[str], sink: FrameSink) -> None:
        self._fields = tuple(fields)
        self._sink = sink

    def onItemUpdate(self, update: Any) -> None:
        try:
            self._sink.on_frame({f: update.getValue(f) for f in self._fields})
        except Exception:  # noqa: BLE001 - must never escape into the library thread
            # Never silently dropped (dto-contracts.md §4): the frame counts as lost, so its channel emits a
            # GAP (orders / account resync; a lost price frame is only counted).
            log.exception("stonex stream: malformed item update lost (gap)")
            try:
                self._sink.on_lost(1)
            except Exception:  # noqa: BLE001
                log.exception("stonex stream: lost-frame callback failed")

    def onSubscription(self) -> None:
        self._safe(StreamEventKind.SUBSCRIBED, "")

    def onUnsubscription(self) -> None:
        self._safe(StreamEventKind.UNSUBSCRIBED, "")

    def onSubscriptionError(self, code: Any, message: Any) -> None:
        self._safe(StreamEventKind.ERROR, f"code={code} {message}")

    def onItemLostUpdates(self, item_name: Any, item_pos: Any, lost_updates: Any) -> None:
        try:
            self._sink.on_lost(int(lost_updates))
        except Exception:  # noqa: BLE001
            log.exception("stonex stream: lost-updates callback failed")

    def _safe(self, kind: StreamEventKind, detail: str) -> None:
        try:
            self._sink.on_event(kind, detail)
        except Exception:  # noqa: BLE001
            log.exception("stonex stream: event callback failed")

    def onClearSnapshot(self, item_name: Any, item_pos: Any) -> None: ...
    def onCommandSecondLevelItemLostUpdates(self, lost: Any, key: Any) -> None: ...
    def onCommandSecondLevelSubscriptionError(self, code: Any, message: Any, key: Any) -> None: ...
    def onEndOfSnapshot(self, item_name: Any, item_pos: Any) -> None: ...
    def onListenEnd(self, *args: Any) -> None: ...
    def onListenStart(self, *args: Any) -> None: ...
    def onRealMaxFrequency(self, frequency: Any) -> None: ...


class _ClientListener:
    def __init__(self, on_status: StatusCallback, on_server_error: ServerErrorCallback) -> None:
        self._on_status = on_status
        self._on_server_error = on_server_error

    def onStatusChange(self, status: Any) -> None:
        try:
            self._on_status(str(status))
        except Exception:  # noqa: BLE001
            log.exception("stonex stream: status callback failed")

    def onServerError(self, code: Any, message: Any) -> None:
        try:
            self._on_server_error(int(code), str(message))
        except Exception:  # noqa: BLE001
            log.exception("stonex stream: server error callback failed")

    def onPropertyChange(self, prop: Any) -> None: ...
    def onListenEnd(self, *args: Any) -> None: ...
    def onListenStart(self, *args: Any) -> None: ...


class LightstreamerTransport:
    """StreamTransport over lightstreamer-client-lib (adapter set STREAMINGALL, mode DISTINCT).

    ``price_unfiltered`` (``XTNL_STONEX_PRICE_UNFILTERED=1``, off by default, turned on only after the PR3 probe)
    asks for ``setRequestedMaxFrequency("unfiltered")`` on PRICES subscriptions; no other subscription changes."""

    def __init__(self, url: str, adapter_set: str = "STREAMINGALL",
                 install_socket_options: Callable[[], bool] = install_lightstreamer_socket_options,
                 *, price_unfiltered: bool = False) -> None:
        self._url = url
        self._price_unfiltered = price_unfiltered
        self._adapter_set = adapter_set
        self._install_socket_options = install_socket_options
        self._client: Any = None

    def connect(self, user: str, password: str, on_status: StatusCallback,
                on_server_error: ServerErrorCallback) -> None:
        """Runs on a worker thread (asyncio.to_thread). The library's sockets get SO_LINGER(on, 0) and
        TCP_USER_TIMEOUT before its first connection (src/adapters/sockets.py)."""
        self._install_socket_options()
        from lightstreamer.client import LightstreamerClient

        client = LightstreamerClient(self._url, self._adapter_set)
        client.connectionDetails.setUser(user)
        client.connectionDetails.setPassword(password)   # the StoneX session token
        client.addListener(_ClientListener(on_status, on_server_error))
        client.connect()
        self._client = client

    def subscribe(self, data_adapter: str, items: Sequence[str], fields: Sequence[str],
                  sink: FrameSink, *, mode: str = "DISTINCT") -> object:
        """`mode`: DISTINCT for ORDERS (every state change is an event); MERGE for PRICES and
        CLIENTACCOUNTMARGIN. Measured on the live account (2026-11-13): StoneX sends PRICES and
        CLIENTACCOUNTMARGIN updates only to MERGE subscriptions; DISTINCT ones are accepted but never fed."""
        from lightstreamer.client import Subscription

        if self._client is None:
            raise RuntimeError("Lightstreamer client not connected")
        if mode not in ("DISTINCT", "MERGE"):
            raise ValueError(f"unsupported Lightstreamer mode {mode!r}")
        sub = Subscription(mode, list(items), list(fields))
        sub.setDataAdapter(data_adapter)
        if mode == "MERGE":
            sub.setRequestedSnapshot("yes")   # the current value at once, then every change
            if data_adapter == "PRICES" and self._price_unfiltered:
                sub.setRequestedMaxFrequency("unfiltered")   # every update, none merged away (PR3, flag on)
        sub.addListener(_SubscriptionListener(fields, sink))
        self._client.subscribe(sub)
        return sub

    def unsubscribe(self, handle: object) -> None:
        if self._client is not None:
            self._client.unsubscribe(handle)

    def disconnect(self) -> None:
        if self._client is not None:
            self._client.disconnect()
            self._client = None
