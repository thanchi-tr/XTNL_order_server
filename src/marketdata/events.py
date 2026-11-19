"""The pull cursor over the rollback set, and BROKER re-fetch requests (design §A10, §A13; plan S16, R11-M16).

**`events_since(series, cursor)`** serves, from the rollback set `md:rb:{series}` (the newest N bar-end events, scored
by the per-series `write_seq`), every entry with a higher `write_seq` than the cursor's, in `write_seq` order:
revisions of older ids included, so a same-id revision reaches a consumer with no wake-up. The consumer re-applies
them in bar order and compares ids and hashes (same id and hash: a no-op).

`CURSOR_EXPIRED` (`CursorExpired(generation)`) is judged on `write_seq` and the generation:

* the cursor belongs to another md generation (its `write_seq` part is valid only within the generation it was read
  under), or carries a `write_seq` but no generation;
* its `write_seq` is older than the set's oldest entry minus one (entries in between were trimmed), or newer than the
  newest (the store lost them);
* a stored entry does not decode or does not name its own bar end (a corrupt set: the consumer reads by bucket, where
  the port validates and quarantines).

The **restart form** `EventCursor(last_decided_bar, 0)` (the consumer's durable acknowledgement after a generation
change, which never stores a `write_seq`) gets every entry whose bar end is at or after `last_decided_bar`, provided
the set still reaches back that far (its oldest bar end at or before it) **and** holds every grid bucket from there
to its newest bar end; otherwise `CURSOR_EXPIRED`. The set is the newest N entries by `write_seq`, not by bar end (a
backfill writes newest first, an old bucket can be revised after a trim), so a low bar end can survive while buckets
after it were trimmed: a hole in `(last_decided_bar, newest]` would skip bars with nothing to show for it. A bucket
with no bar (a closed market) also reads as a hole: the consumer then reads by bucket, which is always safe.

After `CURSOR_EXPIRED` the consumer reads the bars it needs by bucket and, for a BROKER bucket missing from the store,
calls `refetch(series, from, to)`: `RefetchQueue` coalesces the requests for `md-bars` (S17), which re-fetches them in
the BARS lane and validates them before writing. TICK buckets cannot be re-fetched (`Unsupported`).

Pure functions over decoded store values; nothing here formats a price.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from src.marketdata.codec import DecodedMember, decode_member
from src.marketdata.store.reads import RollbackEntry
from src.marketdata.types import BarEndEvent, CursorExpired, EventCursor, MdGeneration, SeriesId, Source, Unsupported

REFETCH_QUEUE_MAX: Final = 256


def rollback_events(series: SeriesId, entries: Sequence[RollbackEntry]) -> list[BarEndEvent] | None:
    """The rollback entries as events in `write_seq` order, or None when one does not decode or names another bar
    end (no prices are kept: the event carries the id, the hash, `write_seq` and the revision)."""
    out: list[BarEndEvent] = []
    seen: set[tuple[int, str]] = set()
    for entry in sorted(entries, key=lambda e: e.write_seq):
        decoded = decode_member(series, entry.member)
        if not isinstance(decoded, DecodedMember) or decoded.bar.bucket_end_ms != entry.bar_end_ms:
            return None
        revised = any(end == entry.bar_end_ms for end, _ in seen)
        seen.add((entry.bar_end_ms, decoded.content_hash))
        out.append(BarEndEvent(series, entry.bar_end_ms, decoded.content_hash, entry.write_seq,
                               decoded.bar.revision, revised or decoded.bar.revision > 0))
    return out


def _covers(series: SeriesId, ends: set[int], from_end_ms: int) -> bool:
    """True when `ends` holds every grid bar end in `(from_end_ms, max(ends)]` (step = the series' timeframe)."""
    step = series.spec.timeframe_ms
    newest = max(ends)
    if newest <= from_end_ms:
        return True
    if (newest - from_end_ms) % step:
        return False
    needed = (newest - from_end_ms) // step
    return sum(1 for e in ends if from_end_ms < e <= newest and (e - from_end_ms) % step == 0) == needed


def events_after(series: SeriesId, entries: Sequence[RollbackEntry], cursor: EventCursor,
                 generation: MdGeneration) -> tuple[BarEndEvent, ...] | CursorExpired:
    """`events_since` on the entries of one read session read under `generation` (see the module text)."""
    events = rollback_events(series, entries)
    if events is None:
        return CursorExpired(generation)
    if cursor.write_seq == 0:
        if not events or min(e.bar_end_ms for e in events) > cursor.bar_end_ms                 or not _covers(series, {e.bar_end_ms for e in events}, cursor.bar_end_ms):
            return CursorExpired(generation)
        return tuple(e for e in events if e.bar_end_ms >= cursor.bar_end_ms)
    if cursor.generation is None or cursor.generation != generation:
        return CursorExpired(generation)
    if not events:
        return CursorExpired(generation) if cursor.write_seq > 0 else ()
    lo, hi = events[0].write_seq, events[-1].write_seq
    if cursor.write_seq < lo - 1 or cursor.write_seq > hi:
        return CursorExpired(generation)
    return tuple(e for e in events if e.write_seq > cursor.write_seq)


@dataclass(frozen=True)
class RefetchRequest:
    series: SeriesId
    from_bucket_ms: int
    to_bucket_ms: int


class RefetchQueue:
    """BROKER re-fetch requests for `md-bars` (S17), coalesced per series (ranges merged), bounded: at the bound the
    oldest series' request is dropped and counted (the consumer asks again while the bucket stays missing)."""

    def __init__(self, *, limit: int = REFETCH_QUEUE_MAX) -> None:
        self._limit = limit
        self._pending: OrderedDict[SeriesId, tuple[int, int]] = OrderedDict()
        self.dropped = 0
        self.requested = 0

    def request(self, series: SeriesId, from_bucket_ms: int, to_bucket_ms: int) -> None | Unsupported:
        if series.source is not Source.BROKER:
            return Unsupported("tick_series")
        if to_bucket_ms < from_bucket_ms or not (series.spec.on_grid(from_bucket_ms)
                                                 and series.spec.on_grid(to_bucket_ms)):
            return Unsupported("bucket_range")
        self.requested += 1
        held = self._pending.get(series)
        if held is not None:
            self._pending[series] = (min(held[0], from_bucket_ms), max(held[1], to_bucket_ms))
            return None
        if len(self._pending) >= self._limit:
            self._pending.popitem(last=False)
            self.dropped += 1
        self._pending[series] = (from_bucket_ms, to_bucket_ms)
        return None

    def __call__(self, series: SeriesId, from_bucket_ms: int, to_bucket_ms: int) -> None:
        """The `MdWriter` `refetch` callback form (queue bound reached for a BROKER bucket)."""
        self.request(series, from_bucket_ms, to_bucket_ms)

    def drain(self) -> list[RefetchRequest]:
        out = [RefetchRequest(s, lo, hi) for s, (lo, hi) in self._pending.items()]
        self._pending.clear()
        return out

    def __len__(self) -> int:
        return len(self._pending)
