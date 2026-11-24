"""Feed observer: consumes the Redis Stream `oversee:feed` and wakes the RiskSweep (oversee-rules.md §1, §3).

It evaluates nothing, never touches rule states or the halt level, never publishes to the bus and
never calls a BrokerAdapter. For each entry it calls `sweep_now(account_id)`, then XACKs. It
consumes nothing while `redis:boot_id` != `run_id:epoch` (SM8 not READY). Entries idle > 30 s
are XAUTOCLAIMed; an entry delivered 5 times is quarantined with an alert and XACKed.

A lost XREADGROUP reply (a socket timeout after Redis had already moved entries into this consumer's pending
list) would otherwise wait 30-60 s for XAUTOCLAIM. A stream that can read this consumer's own pending list
(`OwnPendingReader.read_own_pending`, XREADGROUP with id 0) is drained first: at every `run()` (the supervisor
re-enters it on the same instance after a crash), after a read error and after an empty read (which may be a
swallowed timeout). The drain counts as a delivery, so the max_deliveries quarantine still applies.
The Redis implementation of `FeedStream` is src.cache.feed_stream.RedisFeedStream (re-exported here).
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Final, Protocol, runtime_checkable

from src.cache import keys as K
from src.cache.feed_stream import FEED_GROUP, FeedEntry, RedisFeedStream
from src.oversee.types import Page

FEED_STREAM: Final = K.FEED
ENTITY_TYPES = frozenset({"ORDER", "POSITION", "ACCOUNT"})

__all__ = [
    "ENTITY_TYPES", "FEED_GROUP", "FEED_STREAM", "FeedConfig", "FeedEntry", "FeedObserver", "FeedStream",
    "InMemoryFeedStream", "OwnPendingReader", "QuarantineSink", "RecordingQuarantine", "RedisFeedStream", "SweepSignal",
]


class FeedStream(Protocol):
    async def boot_id(self) -> str | None: ...

    async def read_group(self, count: int, block_ms: int) -> list[FeedEntry]: ...

    async def autoclaim(self, min_idle_ms: int, count: int) -> list[FeedEntry]: ...

    async def ack(self, entry_ids: Sequence[str]) -> None: ...


@runtime_checkable
class OwnPendingReader(Protocol):
    async def read_own_pending(self, count: int) -> list[FeedEntry]:
        """This consumer's own pending entries (XREADGROUP id 0, no BLOCK), each counted as one more delivery."""
        ...


class SweepSignal(Protocol):
    def sweep_now(self, account_id: str) -> None: ...


class QuarantineSink(Protocol):
    async def quarantine(self, entry: FeedEntry, reason: str) -> None:
        """Insert into events_quarantine."""
        ...


@dataclass(frozen=True)
class FeedConfig:
    batch: int = 100
    block_ms: int = 1_000
    claim_idle_ms: int = 30_000
    claim_every_ms: int = 30_000
    max_deliveries: int = 5
    paused_poll_ms: int = 500


class FeedObserver:
    def __init__(self, stream: FeedStream, sweep: SweepSignal, expected_boot_id: Callable[[], str],
                 quarantine: QuarantineSink, clock: Callable[[], int], config: FeedConfig = FeedConfig(),
                 pager: Callable[[Page], None] | None = None) -> None:
        self._stream = stream
        self._sweep = sweep
        self._expected = expected_boot_id
        self._quarantine = quarantine
        self._clock = clock
        self._cfg = config
        self._pager = pager
        self._last_claim_ms: int | None = None
        self._stopping = False
        self._drain_own = True     # drain this consumer's own pending list before the next '>' read
        self.liveness_ms: int | None = None

    async def ready(self) -> bool:
        """SM8 READY: redis:boot_id = run_id:epoch. Any error counts as not ready."""
        try:
            return await self._stream.boot_id() == self._expected()
        except Exception:
            return False

    async def _handle(self, entries: Sequence[FeedEntry]) -> int:
        done: list[str] = []
        for e in entries:
            acct = e.account_id
            if e.deliveries >= self._cfg.max_deliveries or acct is None:
                reason = "max_deliveries" if acct is not None else "malformed_entry"
                await self._quarantine.quarantine(e, reason)
                if self._pager is not None:
                    self._pager(Page("high", "feed_entry_quarantined", acct, f"{e.entry_id} {reason}", self._clock()))
            else:
                self._sweep.sweep_now(acct)   # wake-up first, then XACK (§3)
            done.append(e.entry_id)
        if done:
            await self._stream.ack(done)
        return len(entries)

    async def claim_once(self) -> int:
        if not await self.ready():
            return 0
        self._last_claim_ms = self._clock()
        return await self._handle(await self._stream.autoclaim(self._cfg.claim_idle_ms, self._cfg.batch))

    async def drain_own(self) -> int:
        """Handle this consumer's own pending entries (a lost reply), without waiting claim_idle_ms."""
        if not isinstance(self._stream, OwnPendingReader):
            self._drain_own = False
            return 0
        total = 0
        while True:
            got = await self._stream.read_own_pending(self._cfg.batch)
            total += await self._handle(got)
            if len(got) < self._cfg.batch:
                break
        self._drain_own = False
        return total

    async def poll_once(self) -> int:
        """One read; returns the number of entries handled (0 while paused)."""
        self.liveness_ms = self._clock()
        if not await self.ready():
            return 0
        now = self._clock()
        if self._last_claim_ms is None or now - self._last_claim_ms >= self._cfg.claim_every_ms:
            await self.claim_once()
        handled = await self.drain_own() if self._drain_own else 0
        try:
            entries = await self._stream.read_group(self._cfg.batch, self._cfg.block_ms)
        except BaseException:
            self._drain_own = True
            raise
        if not entries:
            self._drain_own = True      # an empty read may be a timed-out one whose entries Redis delivered
        return handled + await self._handle(entries)

    def stop(self) -> None:
        self._stopping = True

    async def run(self) -> None:
        self._drain_own = True          # a supervised restart: the last read may have lost its reply
        while not self._stopping:
            if not await self.ready():
                self.liveness_ms = self._clock()
                await asyncio.sleep(self._cfg.paused_poll_ms / 1000)
                continue
            await self.poll_once()


@dataclass
class InMemoryFeedStream:
    """Test double with consumer-group semantics: pending entries, delivery counts, idle times and the
    consumer each pending entry is delivered to (`consumer` is this observer's; `read_group_as` simulates
    another one)."""

    boot: str | None = None
    entries: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    pending: dict[str, tuple[dict[str, str], int, int]] = field(default_factory=dict)  # id -> (fields, n, at)
    acked: list[str] = field(default_factory=list)
    clock: Callable[[], int] = lambda: 0
    consumer: str = "self"
    owners: dict[str, str] = field(default_factory=dict)   # pending id -> consumer
    _next: int = 0

    def add(self, account_id: str, entity_type: str = "ORDER") -> str:
        entry_id = f"{len(self.entries) + 1}-0"
        self.entries.append((entry_id, {"account_id": account_id, "entity_type": entity_type}))
        return entry_id

    async def boot_id(self) -> str | None:
        return self.boot

    async def read_group(self, count: int, block_ms: int) -> list[FeedEntry]:
        return await self.read_group_as(self.consumer, count)

    async def read_group_as(self, consumer: str, count: int) -> list[FeedEntry]:
        out = []
        for entry_id, fields in self.entries[self._next:self._next + count]:
            self.pending[entry_id] = (fields, 1, self.clock())
            self.owners[entry_id] = consumer
            out.append(FeedEntry(entry_id, fields, 1))
        self._next += len(out)
        return out

    async def read_own_pending(self, count: int) -> list[FeedEntry]:
        out: list[FeedEntry] = []
        now = self.clock()
        for entry_id, (fields, n, _at) in list(self.pending.items()):
            if len(out) >= count:
                break
            if self.owners.get(entry_id, self.consumer) == self.consumer:
                self.pending[entry_id] = (fields, n + 1, now)
                out.append(FeedEntry(entry_id, fields, n + 1))
        return out

    async def autoclaim(self, min_idle_ms: int, count: int) -> list[FeedEntry]:
        out = []
        now = self.clock()
        for entry_id, (fields, n, at) in list(self.pending.items())[:count]:
            if now - at >= min_idle_ms:
                self.pending[entry_id] = (fields, n + 1, now)
                self.owners[entry_id] = self.consumer
                out.append(FeedEntry(entry_id, fields, n + 1))
        return out

    async def ack(self, entry_ids: Sequence[str]) -> None:
        for entry_id in entry_ids:
            self.pending.pop(entry_id, None)
            self.owners.pop(entry_id, None)
            self.acked.append(entry_id)


@dataclass
class RecordingQuarantine:
    rows: list[tuple[FeedEntry, str]] = field(default_factory=list)

    async def quarantine(self, entry: FeedEntry, reason: str) -> None:
        self.rows.append((entry, reason))
