"""Per-subscription liveness and its ordered mirror into StreamHealthSink (adapter-isolation.md §5).

The parser path never awaits: health changes are enqueued synchronously and one worker task
applies them in order, so a mark_lost is never overtaken by a later mark_reconciled.
A failed mark_lost / mark_reconciled is retried (the fence must not fail open); heartbeats are best-effort.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Literal

from src.ports import StreamHealthSink, StreamKind

log = logging.getLogger(__name__)

HealthOp = Literal["lost", "reconciled", "heartbeat"]
STREAMS: tuple[StreamKind, ...] = ("prices", "orders", "account")


@dataclass
class SubscriptionState:
    """One subscription of one adapter. `lost` mirrors stream_health.conn_lost_at being set
    (LEADER_INIT sets it for every row, so a new process starts lost)."""

    subscribed: bool = False
    lost: bool = True
    subscribed_seq: int = 0   # HLC when the SUBSCRIBED ack was observed
    dropped_seq: int = 0      # HLC of the last frame that could not be handed over
    #: Local time until which this stream's conn: key is believed present (PEXPIRE 30 s after the last
    #: heartbeat / reconcile it was written by; a mark_lost deletes it at once). The conn-absence alert reads it.
    key_until_ms: int = 0


class HealthMirror:
    def __init__(self, sink: StreamHealthSink, retry_delay_s: float = 1.0) -> None:
        self._sink = sink
        self._ops: deque[tuple[HealthOp, str, StreamKind]] = deque()
        self._wake = asyncio.Event()
        self._retry_delay_s = retry_delay_s

    def enqueue(self, op: HealthOp, adapter_id: str, stream: StreamKind) -> None:
        if op == "heartbeat" and (op, adapter_id, stream) in self._ops:
            return  # one pending refresh is enough
        self._ops.append((op, adapter_id, stream))
        self._wake.set()

    def pending(self) -> list[tuple[HealthOp, str, StreamKind]]:
        return list(self._ops)

    async def drain(self) -> bool:
        """Apply queued ops in order. False if a durable op failed (left at the head for retry)."""
        while self._ops:
            op, adapter_id, stream = self._ops[0]
            try:
                if op == "lost":
                    await self._sink.mark_lost(adapter_id, stream)
                elif op == "reconciled":
                    await self._sink.mark_reconciled(adapter_id, stream)
                else:
                    await self._sink.heartbeat(adapter_id, stream)
            except Exception:
                if op == "heartbeat":
                    log.warning("heartbeat %s:%s failed", adapter_id, stream, exc_info=True)
                else:
                    log.exception("stream_health %s %s:%s failed; will retry", op, adapter_id, stream)
                    return False
            self._ops.popleft()
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self._wake.clear()
            ok = await self.drain()
            timeout = self._retry_delay_s if not ok else None
            try:
                await asyncio.wait_for(self._wake.wait(), timeout)
            except TimeoutError:
                pass
