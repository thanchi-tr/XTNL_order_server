"""Per-stream bounded hand-over from the cTrader socket reader to one stream consumer
(dto-contracts.md §4, adapter-isolation.md §5).

The reader calls :meth:`EventChannel.offer` synchronously; it never blocks:
- prices: a full channel drops the frame (backpressure_overflow: drop price frames at the parser);
- orders / positions / account: a full channel drops the frame and the consumer gets StreamEvent(GAP).
Control events (SUBSCRIBED, DISCONNECTED, AUTH_LOST, GAP) jump the queue. While the session is live,
the consumer gets a HEARTBEAT every ``heartbeat_s`` (transport liveness for the conn: keys).
"""
from __future__ import annotations

import asyncio
import collections
from collections.abc import Callable

from src.adapters.base import StreamEvent, StreamEventKind, StreamName
from src.adapters.pepperstone.protocol import Frame


class EventChannel:
    def __init__(self, name: str, stream: StreamName, *, maxsize: int, drop_when_full: bool, heartbeat_s: float,
                 is_live: Callable[[], bool], clock_ms: Callable[[], int]) -> None:
        self.name = name
        self.stream: StreamName = stream
        self._maxsize = maxsize
        self._drop = drop_when_full
        self._heartbeat_s = heartbeat_s
        self._is_live = is_live
        self._clock_ms = clock_ms
        self._frames: collections.deque[Frame] = collections.deque()
        self._control: collections.deque[StreamEvent] = collections.deque()
        self._wake = asyncio.Event()
        self._gap_frames = 0
        self._last_heartbeat = asyncio.get_running_loop().time()
        self.closed = False
        self.dropped_frames = 0
        self.gaps_opened = 0

    def offer(self, frame: Frame) -> None:
        """Reader side: never blocks, never raises."""
        if self.closed:
            return
        if len(self._frames) >= self._maxsize:
            if self._drop:
                self.dropped_frames += 1
            else:
                if self._gap_frames == 0:
                    self.gaps_opened += 1
                self._gap_frames += 1
                self._wake.set()
            return
        self._frames.append(frame)
        self._wake.set()

    def put_event(self, kind: StreamEventKind, detail: str = "") -> None:
        if self.closed:
            return
        self._control.append(StreamEvent(kind, self.stream, detail, self._clock_ms()))
        self._wake.set()

    def clear_frames(self) -> None:
        """Drop queued frames of a lost socket (a reconnect reports the GAP instead)."""
        self._frames.clear()

    def close(self) -> None:
        self.closed = True
        self._wake.set()

    def qsize(self) -> int:
        return len(self._frames)

    async def get(self) -> Frame | StreamEvent | None:
        """Next control event, GAP, HEARTBEAT or frame; None once closed."""
        loop = asyncio.get_running_loop()
        while True:
            if self._control:
                return self._control.popleft()
            if self._gap_frames:
                count, self._gap_frames = self._gap_frames, 0
                return StreamEvent(StreamEventKind.GAP, self.stream, f"{count} frame(s) not handed over",
                                   self._clock_ms())
            if self.closed:
                return None
            wait = self._last_heartbeat + self._heartbeat_s - loop.time()
            if wait <= 0:
                self._last_heartbeat = loop.time()
                if self._is_live():
                    return StreamEvent(StreamEventKind.HEARTBEAT, self.stream, "", self._clock_ms())
                continue
            if self._frames:
                return self._frames.popleft()
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=wait)
            except TimeoutError:
                pass
