"""The PR9 stop-read schedule (design §B2 PR9, §B6.3 (2), §B6.4; plan S27, R11-M20).

Active only with `XTNL_ARM_STOP_READS=1` (`ArmSettings.stop_reads`, off by default: then nothing is ever queued and
every view is empty). Reads go through `StopHandleReadPort` (PR12): **one list read per adapter per anchor** for all
its armed handles (`StopHandlesPort` names them; S31 fills it from the tracker's book), never one call per handle.
Reads run as their own tasks, so the BarClock never awaits broker I/O; a pass only looks at what has returned.

* **PRE anchor:** one read per adapter queued at local `T + p50 − pre_offset − 15 s` (15 s before PRE fires; `BARS`
  waits at most 15 s, so the request leaves by the fire time). PRE waits for it until its valid-until and accepts it
  up to 30 s old. The positions armed when the read was queued are remembered: **a position armed after its PRE read
  was queued gets no PRE that bar** (`pre_covers`).
* **POST anchor:** one read per adapter queued at local `T + p50`, in parallel with the first `md-bars` attempt; it
  gives `S_at_T` (`StopReadView.at_bar_end`). When the POST is ready and the newest read is older than 15 s, one more
  read (retried every 10 s while the POST waits, within `decision_cap`); the POST waits for it.
* **On demand** before every `LATE`, `REOPEN` or `CORRECTED` item: a read when the newest one is older than 15 s.
  `REOPEN` reads are staggered per adapter (at most one per 10 s) and coalesced: while any read of the adapter is in
  flight, an item waits for it instead of starting another. A POST anchor is queued only within 60 s of `T + p50`
  (a BarClock that starts later relies on the POST re-read).
* **Age** counts from the request's send time (`StopHandleRead.sent_ms`, after any `BARS` wait), never from the
  queue time. Bounds: 30 s at PRE, 15 s otherwise. `stop_stale` is counted per position (a metric only).

Ids, handle ids, codes and times only: a read result here holds no level (the ARM takes the level from PostgreSQL
once it shows the read's committed version, PR9). Nothing here logs a value.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Generator, Mapping, Sequence, Set
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Protocol

from src.marketdata.ports import HandleRead, HandleRef, HandleStatus, StopHandleRead, StopHandleReadPort
from src.marketdata.timing import SkewEstimate, local_bar_end_ms
from src.riskmgr.types import ArmEvent

log = logging.getLogger(__name__)

PRE_LEAD_MS: Final = 15_000
"""The PRE read is queued this long before PRE fires."""
PRE_BOUND_MS: Final = 30_000
BOUND_MS: Final = 15_000
RETRY_MS: Final = 10_000
"""A POST re-read, and a REOPEN read per adapter, at most once per 10 s."""
POST_ANCHOR_WINDOW_MS: Final = 60_000
"""The POST anchor is queued only within `post_grace` of `T + p50` (a BarClock started later uses the POST re-read)."""
KEEP_MS: Final = 2 * 3_600_000
"""Anchor books are kept this long behind the oldest bar end in view (bounded memory)."""


class Anchor(str, Enum):
    PRE = "PRE"
    POST = "POST"
    REREAD = "REREAD"
    ON_DEMAND = "ON_DEMAND"
    REOPEN = "REOPEN"


class _Running(Protocol):
    """A read task (`asyncio.create_task`), typed structurally: `asyncio.Task` is off the lockout allow-list."""

    def done(self) -> bool: ...

    def cancel(self) -> bool: ...

    def __await__(self) -> Generator[Any, None, None]: ...


class StopHandlesPort(Protocol):
    def handles(self, adapter_id: str) -> Mapping[str, tuple[HandleRef, ...]]:
        """The armed stop handles of one adapter by position ref (`adapter_id:position_id`), from memory (no I/O)."""
        ...


@dataclass(frozen=True)
class AdapterRead:
    """One completed list read of one adapter. `sent_ms` None: the read failed (`Transient` / `Unsupported`)."""

    adapter_id: str
    anchor: Anchor
    bar_end_ms: int | None
    queued_ms: int
    sent_ms: int | None
    handles: tuple[HandleRead, ...] = ()

    @property
    def ok(self) -> bool:
        return self.sent_ms is not None

    def age_ms(self, now_ms: int) -> int | None:
        return None if self.sent_ms is None else now_ms - self.sent_ms

    def covers(self, refs: Sequence[HandleRef]) -> bool:
        """Every handle came back `OK` or `NOT_FOUND` (a `TRANSIENT` handle is not read)."""
        got = {h.handle: h.status for h in self.handles}
        return self.ok and all(got.get(r) in (HandleStatus.OK, HandleStatus.NOT_FOUND) for r in refs)


@dataclass(frozen=True)
class StopReadView:
    """What one evaluation is given: the bar-end read of bar `T` (the `T + p50` read, for `S_at_T`) and the newest
    completed read (for the post-guards and pre-guard (2))."""

    at_bar_end: AdapterRead | None = None
    current: AdapterRead | None = None

    def fresh(self, event: ArmEvent, now_ms: int) -> bool:
        """The newest read is within its bound (30 s at PRE, 15 s otherwise)."""
        age = None if self.current is None else self.current.age_ms(now_ms)
        return age is not None and age <= (PRE_BOUND_MS if event is ArmEvent.PRE else BOUND_MS)


def bound_ms(event: ArmEvent) -> int:
    return PRE_BOUND_MS if event is ArmEvent.PRE else BOUND_MS


def pre_queue_ms(bar_end_ms: int, skew: SkewEstimate, pre_offset_s: int) -> int:
    """Local `T + p50 − pre_offset − 15 s`."""
    return local_bar_end_ms(bar_end_ms, skew) - pre_offset_s * 1000 - PRE_LEAD_MS


def post_queue_ms(bar_end_ms: int, skew: SkewEstimate) -> int:
    """Local `T + p50`."""
    return local_bar_end_ms(bar_end_ms, skew)


@dataclass(frozen=True)
class ReadBook:
    """One adapter as the BarClock sees it this pass: its skew and the bar ends whose anchors may be due (the
    newest bar end at or before now and the next one, on the broker grid)."""

    adapter_id: str
    skew: SkewEstimate
    last_bar_end_ms: int
    next_bar_end_ms: int
    cap_ms: int
    """`decision_cap` of `last_bar_end_ms`: its POST anchor is never queued after it."""


@dataclass
class StopReadCounters:
    reads: int = 0
    failed: int = 0
    pre_reads: int = 0
    post_reads: int = 0
    rereads: int = 0
    on_demand: int = 0
    reopen_reads: int = 0
    no_send_time: int = 0
    """Reads that returned without `sent_ms` (also counted in `failed`)."""


class StopReads:
    """The PR9 schedule of one term (see the module docstring)."""

    def __init__(self, port: StopHandleReadPort | None, handles: StopHandlesPort | None, *, enabled: bool,
                 pre_offset_s: int) -> None:
        self._port = port
        self._book = handles
        self._enabled = enabled and port is not None and handles is not None
        self._pre_offset_s = pre_offset_s
        self.counters = StopReadCounters()
        self._queued: set[tuple[str, Anchor, int]] = set()
        self._pre_refs: dict[tuple[str, int], frozenset[str]] = {}
        self._anchor_reads: dict[tuple[str, Anchor, int], AdapterRead] = {}
        self._newest: dict[str, AdapterRead] = {}
        self._inflight: dict[tuple[str, Anchor], _Running] = {}
        self._last_try: dict[tuple[str, Anchor], int] = {}
        self._stale: dict[str, int] = {}
        self._stale_seen: set[tuple[str, str, int]] = set()

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ------------------------------------------------------------------------------------------------ the schedule
    def tick(self, now_ms: int, books: Sequence[ReadBook]) -> None:
        """Queue the anchor reads that are due (each once per adapter, anchor and bar end)."""
        if not self._enabled:
            return
        for b in books:
            pre_at = pre_queue_ms(b.next_bar_end_ms, b.skew, self._pre_offset_s)
            if pre_at <= now_ms < local_bar_end_ms(b.next_bar_end_ms, b.skew):
                self._anchor(b.adapter_id, Anchor.PRE, b.next_bar_end_ms, now_ms)
            post_at = post_queue_ms(b.last_bar_end_ms, b.skew)
            if post_at <= now_ms < min(b.cap_ms, post_at + POST_ANCHOR_WINDOW_MS):
                self._anchor(b.adapter_id, Anchor.POST, b.last_bar_end_ms, now_ms)
        self._prune(books)

    def _anchor(self, adapter_id: str, anchor: Anchor, bar_end_ms: int, now_ms: int) -> None:
        key = (adapter_id, anchor, bar_end_ms)
        if key in self._queued:
            return
        refs = self._handles(adapter_id)
        self._queued.add(key)
        if anchor is Anchor.PRE:
            self._pre_refs[(adapter_id, bar_end_ms)] = frozenset(refs)
            self.counters.pre_reads += 1
        else:
            self.counters.post_reads += 1
        if refs:
            self._start(adapter_id, anchor, bar_end_ms, now_ms)

    def _handles(self, adapter_id: str) -> dict[str, tuple[HandleRef, ...]]:
        return {} if self._book is None else dict(self._book.handles(adapter_id))

    def _start(self, adapter_id: str, anchor: Anchor, bar_end_ms: int | None, now_ms: int) -> bool:
        lane = (adapter_id, anchor)
        running = self._inflight.get(lane)
        if running is not None and not running.done():
            return False                        # coalesced: one read in flight per adapter and anchor kind
        refs = tuple(dict.fromkeys(h for hs in self._handles(adapter_id).values() for h in hs))
        if self._port is None or not refs:
            return False
        self._last_try[lane] = now_ms
        self.counters.reads += 1
        self._inflight[lane] = asyncio.create_task(self._read(adapter_id, anchor, bar_end_ms, now_ms, refs))
        return True

    async def _read(self, adapter_id: str, anchor: Anchor, bar_end_ms: int | None, queued_ms: int,
                    refs: tuple[HandleRef, ...]) -> None:
        port = self._port
        if port is None:
            return
        try:
            result = await port.read_stop_handles(adapter_id, refs)
        except Exception:                      # a port answers with values; a raise is a failed read, never fatal
            result = None
        if isinstance(result, StopHandleRead):
            read = AdapterRead(adapter_id, anchor, bar_end_ms, queued_ms, result.sent_ms, result.handles)
            if result.sent_ms is None:          # no send stamp: its age is unknown, so it can never be fresh
                self.counters.failed += 1
                self.counters.no_send_time += 1
                log.warning("stop read of adapter %s came back without a send time (%s)", adapter_id, anchor.value)
        else:
            read = AdapterRead(adapter_id, anchor, bar_end_ms, queued_ms, None)
            self.counters.failed += 1
        if bar_end_ms is not None and anchor in (Anchor.PRE, Anchor.POST):
            self._anchor_reads[(adapter_id, anchor, bar_end_ms)] = read
        held = self._newest.get(adapter_id)
        if read.ok and (held is None or held.sent_ms is None or (read.sent_ms or 0) >= held.sent_ms):
            self._newest[adapter_id] = read

    def _prune(self, books: Sequence[ReadBook]) -> None:
        """Forget anchors of bars more than `KEEP_MS` older than the oldest bar any adapter still looks at."""
        if not books:
            return
        keep_from = min(b.last_bar_end_ms for b in books) - KEEP_MS
        for key in [k for k in self._queued if k[2] < keep_from]:
            self._queued.discard(key)
            self._anchor_reads.pop(key, None)
        for pkey in [k for k in self._pre_refs if k[1] < keep_from]:
            del self._pre_refs[pkey]
        self._stale_seen = {s for s in self._stale_seen if s[2] >= keep_from}

    # ------------------------------------------------------------------------------------------------ requests
    def request(self, adapter_id: str, event: ArmEvent, now_ms: int) -> bool:
        """Before a bar-end item: True when the newest read is within 15 s; otherwise queue a read (a POST re-read
        or an on-demand read, REOPEN ones staggered per adapter) and return False (the item waits for it)."""
        if not self._enabled:
            return True
        newest = self._newest.get(adapter_id)
        age = None if newest is None else newest.age_ms(now_ms)
        if age is not None and age <= BOUND_MS:
            return True
        if any(lane[0] == adapter_id and not t.done() for lane, t in self._inflight.items()):
            return False                        # a read of this adapter is in flight: wait for it (coalesced)
        anchor = {ArmEvent.POST: Anchor.REREAD, ArmEvent.REOPEN: Anchor.REOPEN}.get(event, Anchor.ON_DEMAND)
        last = self._last_try.get((adapter_id, anchor))
        if last is not None and now_ms - last < RETRY_MS:
            return False
        if self._start(adapter_id, anchor, None, now_ms):
            if anchor is Anchor.REREAD:
                self.counters.rereads += 1
            elif anchor is Anchor.REOPEN:
                self.counters.reopen_reads += 1
            else:
                self.counters.on_demand += 1
        return False

    # ------------------------------------------------------------------------------------------------ views
    def view(self, adapter_id: str, bar_end_ms: int) -> StopReadView:
        if not self._enabled:
            return StopReadView()
        return StopReadView(self._anchor_reads.get((adapter_id, Anchor.POST, bar_end_ms)),
                            self._newest.get(adapter_id))

    def pre_state(self, adapter_id: str, bar_end_ms: int, ref: str) -> str:
        """`off` (PR9 off), `not_queued`, `not_covered` (armed after the read was queued: no PRE this bar),
        `pending` (the read has not returned) or `ready`."""
        if not self._enabled:
            return "off"
        refs = self._pre_refs.get((adapter_id, bar_end_ms))
        if refs is None:
            return "not_queued"
        if ref not in refs:
            return "not_covered"
        return "ready" if (adapter_id, Anchor.PRE, bar_end_ms) in self._anchor_reads else "pending"

    def note_stale(self, ref: str, event: ArmEvent, bar_end_ms: int) -> None:
        """Count `stop_stale` once per position, event and bar (a metric, never a counter of §B14)."""
        key = (ref, event.value, bar_end_ms)
        if key in self._stale_seen:
            return
        self._stale_seen.add(key)
        self._stale[ref] = self._stale.get(ref, 0) + 1

    def stale_counts(self) -> dict[str, int]:
        return dict(self._stale)

    def forget(self, live_refs: Set[str]) -> None:
        """Drop the `stop_stale` counts of positions no longer live (bounded memory on a 24/7 node)."""
        self._stale = {ref: n for ref, n in self._stale.items() if ref in live_refs}
        self._stale_seen = {s for s in self._stale_seen if s[0] in live_refs}

    async def drain(self) -> None:
        """Wait for every read in flight (tests and shutdown)."""
        tasks = [t for t in self._inflight.values() if not t.done()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def close(self) -> None:
        for t in self._inflight.values():
            if not t.done():
                t.cancel()
        self._inflight.clear()


__all__ = [
    "Anchor", "AdapterRead", "BOUND_MS", "PRE_BOUND_MS", "PRE_LEAD_MS", "RETRY_MS", "ReadBook", "StopHandlesPort",
    "StopReadCounters", "StopReadView", "StopReads", "bound_ms", "post_queue_ms", "pre_queue_ms",
]
