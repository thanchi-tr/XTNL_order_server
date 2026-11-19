"""The `MarketDataPort` implementation over `redis-md` (design §A10, §A13, §A4 check 5, §A7 AWS; plan S16).

* **Read sessions.** Every store read is one session: `md:boot` and `md:writer` read together before the reads and
  `md:boot` again after them; when the two sentinels differ the reads are discarded (`DataUnavailable(generation_
  changed)`). The generation is checked per session, never per stored bar: a bar written under an earlier generation
  stays usable, checked by its content hash and §A4, so a restart never quarantines history. Every result carries
  the generation it was read under.
* **STALE_NODE** (§A7 AWS): reads trust the store only while `md:writer`'s epoch equals the current leader epoch
  (`leader_epoch()`); otherwise `DataUnavailable(stale_node)` (and `Liveness.STALE_NODE` for quotes). An unknown
  leader epoch counts as stale (fail closed).
* **Decision reads** (`bars`) are bucket-addressed and return `DataUnavailable` unless the series is LIVE, or
  BACKFILLING with the needed range complete (`SeriesStateBook.readable`); a FAILED series or a global ARM FAILED
  gives `DataUnavailable(failed)`, any other state `not_live`. All market-data reads finish before any PostgreSQL
  transaction begins (the caller's rule).
* **Read-side validation** of every stored bar (§A4 checks 1, 2, 4 and the keyed hash, `validate.check_stored`)
  against the keyring's retained key ids, so both ids verify during a rotation (R11-M13). A failing bar is returned
  `blind` (its values are not to be used) and handed to `md-writer` to be moved to `md:quarantine`
  (`quarantine_sink`); a hash under a key id the ring no longer holds is returned `blind` too but never quarantined
  (missing, not corrupt).
* **`as_of_ms`** reads `md:bars` and `md:bars_rev` together: per bucket the revision with `final_at_ms ≤ as_of` and
  `superseded_at_ms > as_of` (the current member counts as never superseded); among several, the highest revision,
  the current member first.
* `quote` and `forming_bar` come from the in-process tick hook (the freshest copy); `best_bars` is display only.
* `events_since` and `refetch`: `events.py`.
* **S18:** `define` / `demand` / `release` go to the demand book (`demand.DemandBook`; without one every lease is
  refused). `indicator` is a decision read: its spec is resolved (`md-indicators`, else `md:registry:ind`; neither:
  `DataUnavailable(not_registered)`) and every bar series it reads, through `IndicatorRef` inputs too, passes the
  same SM12 / global FAILED gate as `bars` over the buckets read (FAILED: `failed`; not LIVE, nor BACKFILLING with
  the range complete: `not_live`). It reads `md:ind:{id}:{output}` in one read session; a newest value that is a
  FAULTED marker is `DataUnavailable(indicator_faulted)`, and older markers in an `n` read are dropped (a marker
  carries no value); with `as_of_ms`, a value that became final after `as_of_ms` is recomputed by
  `md-indicators` from the `as_of` bars (`IndicatorReads.value_as_of`; `superseded_expired` when a needed revision
  has aged out of `md:bars_rev`). `provisional` comes from `md-indicators` and the hook's forming bars.
  `wake_indicator` (the `MdIndicators.on_updated` listener) routes `indicator_updated` to the callbacks.
* Timeouts and connection errors are values (`store_timeout`, `store_error`), never exceptions (500 ms read client).

Nothing here logs or formats a price.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal
from typing import Final, Protocol

from src.marketdata.codec import DecodedMember, decode_member
from src.marketdata.demand import spec_id_of
from src.marketdata.events import RefetchQueue, events_after
from src.marketdata.hashing import HashStatus, Keyring
from src.marketdata.indicators.base import IndicatorSpec, IndicatorUpdate, IndicatorValue, decode_value
from src.marketdata.indicators.registry import REGISTRY, IndicatorRegistry, input_series
from src.marketdata.ports import (
    BarCallback,
    BarsRead,
    EventsRead,
    FormingRead,
    IndicatorCallback,
    IndicatorRead,
    MarketDataPort,
    ProvisionalRead,
    QuoteRead,
    SeriesStatus,
)
from src.marketdata.store import keys as K
from src.marketdata.store.reads import StoreReader
from src.marketdata.store.runner import TIMEOUT, StoreUnavailable
from src.marketdata.types import (
    FAILED,
    GENERATION_CHANGED,
    INDICATOR_FAULTED,
    NOT_LIVE,
    STALE_NODE,
    STORE_ERROR,
    STORE_TIMEOUT,
    Bar,
    BarEndEvent,
    BarSpec,
    CursorExpired,
    DataUnavailable,
    EventCursor,
    LeaseClass,
    Liveness,
    MdGeneration,
    Quality,
    Quote,
    SeriesId,
    Side,
    Sm12State,
    Source,
    Unsupported,
)
from src.marketdata.validate import check_stored

log = logging.getLogger(__name__)

NO_BOOT_READ: Final = "no_boot"
NOT_REGISTERED: Final = "not_registered"
QUOTE_STALE_MS: Final = 30_000
MAX_BARS_PER_READ: Final = 5_000
QUARANTINE_SENT_MAX: Final = 4_096
_ZERO: Final = Decimal(0)


class HookReads(Protocol):
    """The in-process tick hook (`MdHook`): the latest validated quote (now, or as it was at a time) and the forming
    bars."""

    def quote(self, adapter_id: str, symbol: str) -> Quote | None: ...

    def quote_at(self, adapter_id: str, symbol: str, at_ms: int) -> Quote | None: ...

    def forming(self, adapter_id: str, symbol: str) -> tuple[Bar, ...]: ...


class StateGate(Protocol):
    def state(self, series: SeriesId) -> Sm12State: ...

    def readable(self, series: SeriesId, from_ms: int | None, to_ms: int | None) -> bool: ...

    def gaps(self, series: SeriesId) -> int: ...


QuarantineSink = Callable[[Bar, str, str], bool]


class DemandPort(Protocol):
    """The S18 demand book (`DemandBook`)."""

    def define(self, spec: SeriesId | IndicatorSpec) -> str: ...

    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool: ...

    async def release(self, spec_id: str, consumer: str) -> None: ...


class IndicatorReads(Protocol):
    """`md-indicators` (`MdIndicators`): the `as_of` recompute and `provisional`."""

    async def value_as_of(self, indicator_id: str, bucket_start_ms: int,
                          as_of_ms: int) -> dict[str, IndicatorValue] | DataUnavailable: ...

    def provisional(self, indicator_id: str, output: str,
                    forming: Callable[[SeriesId], Bar | None]) -> Decimal | None: ...

    def input_series(self, indicator_id: str) -> tuple[SeriesId, ...] | None:
        """Every bar series the indicator reads; None when it was never tracked there."""
        ...


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def token_epoch(text: str | None) -> int | None:
    """The epoch of an `md:writer` token (`<node>:<epoch>:...`), or None (also used by `arm replay --rerun`)."""
    return _token_epoch(text)


def _token_epoch(text: str | None) -> int | None:
    if text is None:
        return None
    parts = text.split(":")
    return int(parts[1]) if len(parts) == 4 and parts[1].isascii() and parts[1].isdigit() else None


def _blind(series: SeriesId, bucket_start_ms: int, revision: int = 0) -> Bar:
    """A placeholder for a stored bar that failed: quality `blind`, no values (zeros)."""
    return Bar(series=series, bucket_start_ms=bucket_start_ms, open=_ZERO, high=_ZERO, low=_ZERO, close=_ZERO,
               tick_count=0, quality=Quality.BLIND, revision=revision, final_at_ms=None)


class _Unavailable(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class MdPort:
    """`MarketDataPort` over the `md_reader` client (structurally typed against the Protocol)."""

    def __init__(self, reader: StoreReader, *, keyring: Keyring, book: StateGate,
                 leader_epoch: Callable[[], int | None], hook: HookReads | None = None,
                 quarantine_sink: QuarantineSink | None = None, refetch_queue: RefetchQueue | None = None,
                 global_failed: Callable[[], bool] = lambda: False, clock_ms: Callable[[], int] = _wall_ms,
                 quote_stale_ms: int = QUOTE_STALE_MS, demand_book: DemandPort | None = None,
                 indicators: IndicatorReads | None = None, registry: IndicatorRegistry = REGISTRY) -> None:
        self._reader = reader
        self._keyring = keyring
        self._book = book
        self._leader_epoch = leader_epoch
        self._hook = hook
        self._quarantine = quarantine_sink
        self._refetch = refetch_queue or RefetchQueue()
        self._global_failed = global_failed
        self._clock_ms = clock_ms
        self._quote_stale_ms = quote_stale_ms
        self._closed: list[BarCallback] = []
        self._revised: list[BarCallback] = []
        self._demand = demand_book
        self._ind = indicators
        self._registry = registry
        self._ind_callbacks: list[IndicatorCallback] = []
        self.ind_undecodable = 0
        self._sent: set[tuple[SeriesId, int, str]] = set()
        self.quarantine_requests = 0
        self.hash_missing = 0

    @property
    def refetch_queue(self) -> RefetchQueue:
        return self._refetch

    # -------------------------------------------------------------------------------------------- sessions
    async def _head(self) -> K.BootSentinel:
        """The session's opening sentinel (raises `_Unavailable`: store error, no `md:boot`, STALE_NODE)."""
        try:
            head = await self._reader.head()
        except StoreUnavailable as e:
            raise _Unavailable(STORE_TIMEOUT if e.kind == TIMEOUT else STORE_ERROR) from None
        if head.boot is None:
            raise _Unavailable(NO_BOOT_READ)
        epoch = self._leader_epoch()
        if epoch is None or _token_epoch(head.writer) != epoch:
            raise _Unavailable(STALE_NODE)
        return head.boot

    async def _tail(self, opening: K.BootSentinel) -> MdGeneration:
        """`md:boot` again after the reads: another sentinel discards them."""
        try:
            after = await self._reader.boot()
        except StoreUnavailable as e:
            raise _Unavailable(STORE_TIMEOUT if e.kind == TIMEOUT else STORE_ERROR) from None
        if after is None or after.text != opening.text:
            raise _Unavailable(GENERATION_CHANGED)
        return opening.generation

    def _gate(self, series: SeriesId, from_ms: int | None, to_ms: int | None) -> None:
        if self._global_failed() or self._book.state(series) is Sm12State.FAILED:
            raise _Unavailable(FAILED)
        if not self._book.readable(series, from_ms, to_ms):
            raise _Unavailable(NOT_LIVE)

    # -------------------------------------------------------------------------------------------- quotes, forming
    async def quote(self, adapter_id: str, symbol: str) -> QuoteRead | DataUnavailable:
        """The hook's latest validated quote: LIVE when received within `quote_stale_ms`, else STALE (no quote);
        `STALE_NODE` (no quote) when the store's writer epoch is not the leader's."""
        try:
            head = await self._reader.head()
        except StoreUnavailable as e:
            return DataUnavailable(STORE_TIMEOUT if e.kind == TIMEOUT else STORE_ERROR)
        if head.boot is None:
            return DataUnavailable(NO_BOOT_READ)
        epoch = self._leader_epoch()
        if epoch is None or _token_epoch(head.writer) != epoch:
            return QuoteRead(None, Liveness.STALE_NODE, head.boot.generation)
        q = None if self._hook is None else self._hook.quote(adapter_id, symbol)
        fresh = q is not None and self._clock_ms() - q.received_ms <= self._quote_stale_ms
        return QuoteRead(q if fresh else None, Liveness.LIVE if fresh else Liveness.STALE, head.boot.generation)

    async def quote_at(self, adapter_id: str, symbol: str, at_ms: int) -> QuoteRead | DataUnavailable:
        """The hook's quote as it was at local `at_ms` (`MdHook.quote_at`, round 13): LIVE when it was received
        within `quote_stale_ms` before `at_ms`, else STALE; the same store-head and epoch checks as `quote`."""
        try:
            head = await self._reader.head()
        except StoreUnavailable as e:
            return DataUnavailable(STORE_TIMEOUT if e.kind == TIMEOUT else STORE_ERROR)
        if head.boot is None:
            return DataUnavailable(NO_BOOT_READ)
        epoch = self._leader_epoch()
        if epoch is None or _token_epoch(head.writer) != epoch:
            return QuoteRead(None, Liveness.STALE_NODE, head.boot.generation)
        q = None if self._hook is None else self._hook.quote_at(adapter_id, symbol, at_ms)
        fresh = q is not None and q.received_ms <= at_ms and at_ms - q.received_ms <= self._quote_stale_ms
        return QuoteRead(q if fresh else None, Liveness.LIVE if fresh else Liveness.STALE, head.boot.generation)

    async def forming_bar(self, series: SeriesId) -> FormingRead | DataUnavailable:
        try:
            opening = await self._head()
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        if self._hook is not None:
            for b in self._hook.forming(series.adapter_id, series.symbol):
                if b.series == series:
                    return FormingRead(b, b.bucket_start_ms, opening.generation)
        return FormingRead(None, None, opening.generation)

    # -------------------------------------------------------------------------------------------- bars
    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable:
        ranged = from_bucket_ms is not None or to_bucket_ms is not None
        forms = (at_bucket_ms is not None) + (n is not None) + ranged
        if forms != 1 or (from_bucket_ms is None) != (to_bucket_ms is None):
            return DataUnavailable("bad_request")
        lo: int | None = at_bucket_ms if at_bucket_ms is not None else from_bucket_ms
        hi: int | None = at_bucket_ms if at_bucket_ms is not None else to_bucket_ms
        if lo is not None and hi is not None and (hi < lo or (hi - lo) // series.spec.timeframe_ms >= MAX_BARS_PER_READ):
            return DataUnavailable("bad_request")
        if n is not None and not 1 <= n <= MAX_BARS_PER_READ:
            return DataUnavailable("bad_request")
        try:
            self._gate(series, lo, hi)
            opening = await self._head()
            bars = await self._read_bars(series, lo, hi, n, as_of_ms)
            generation = await self._tail(opening)
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        except StoreUnavailable as e:
            return DataUnavailable(STORE_TIMEOUT if e.kind == TIMEOUT else STORE_ERROR)
        return BarsRead(tuple(bars), generation)

    async def best_bars(self, adapter_id: str, symbol: str, spec_name: str, side: Side,
                        n: int) -> BarsRead | DataUnavailable:
        """Display only, never the ARM: BROKER bars where present, else TICK; no SM12 gate."""
        try:
            broker = SeriesId(adapter_id, symbol, BarSpec.parse(spec_name), side, Source.BROKER)
        except ValueError:
            return DataUnavailable("bad_request")
        if not 1 <= n <= MAX_BARS_PER_READ:
            return DataUnavailable("bad_request")
        tick = replace(broker, source=Source.TICK)
        try:
            opening = await self._head()
            bars = await self._read_bars(broker, None, None, n, None)
            if not bars:
                bars = await self._read_bars(tick, None, None, n, None)
            generation = await self._tail(opening)
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        except StoreUnavailable as e:
            return DataUnavailable(STORE_TIMEOUT if e.kind == TIMEOUT else STORE_ERROR)
        return BarsRead(tuple(bars), generation)

    async def _read_bars(self, series: SeriesId, lo: int | None, hi: int | None, n: int | None,
                         as_of_ms: int | None) -> list[Bar]:
        if lo is not None and hi is not None:
            current = await self._reader.bars_range(series, lo, hi)
        else:
            current = await self._reader.bars_newest(series, n or 1)
        candidates: dict[int, list[tuple[int, str]]] = {}   # bucket → (priority, member); current first
        for start, member in current:
            candidates.setdefault(start, []).append((1, member))
        if as_of_ms is not None:
            r_lo = lo if lo is not None else (min(candidates) if candidates else None)
            r_hi = hi if hi is not None else (max(candidates) if candidates else None)
            if r_lo is not None and r_hi is not None:
                for start, member, superseded in await self._reader.superseded_range(series, r_lo, r_hi):
                    if superseded > as_of_ms:
                        candidates.setdefault(start, []).append((0, member))
        now = self._clock_ms()
        out: list[Bar] = []
        for start in sorted(candidates):
            chosen = self._choose(series, start, candidates[start], as_of_ms, now)
            if chosen is not None:
                out.append(chosen)
        return out

    def _choose(self, series: SeriesId, start: int, members: list[tuple[int, str]], as_of_ms: int | None,
                now: int) -> Bar | None:
        """Validate the candidates of one bucket and pick the one current at `as_of_ms` (the current member when
        `as_of_ms` is None)."""
        best: tuple[int, int, Bar] | None = None
        for priority, member in members:
            bar = self._validated(series, start, member, now)
            if as_of_ms is not None and bar.quality is not Quality.BLIND:
                if bar.final_at_ms is None or bar.final_at_ms > as_of_ms:
                    continue
            elif as_of_ms is not None:
                continue                         # a failing candidate is never the as-of revision
            key = (bar.revision, priority)
            if best is None or key > best[:2]:
                best = (bar.revision, priority, bar)
        return None if best is None else best[2]

    def _validated(self, series: SeriesId, start: int, member: str, now: int) -> Bar:
        decoded = decode_member(series, member)
        if not isinstance(decoded, DecodedMember):
            self._send_quarantine(_blind(series, start), decoded.reason, member)
            return _blind(series, start)
        bar = decoded.bar
        if bar.bucket_start_ms != start:
            self._send_quarantine(bar, "grid_range", member)
            return _blind(series, start, bar.revision)
        checked = check_stored(bar, decoded.content_hash, self._keyring, now_ms=now)
        if checked.usable:
            return bar
        if checked.result.ok and checked.hash_status is HashStatus.MISSING:
            self.hash_missing += 1               # a key id no longer held: missing, never corrupt
            return _blind(series, start, bar.revision)
        self._send_quarantine(bar, checked.result.failed[0].value if checked.result.failed else "content_hash",
                              member)
        return _blind(series, start, bar.revision)

    def _send_quarantine(self, bar: Bar, check: str, member: str) -> None:
        key = (bar.series, bar.bucket_start_ms, member)
        if self._quarantine is None or key in self._sent:
            return
        if len(self._sent) >= QUARANTINE_SENT_MAX:
            self._sent.clear()
        self._sent.add(key)
        try:
            if self._quarantine(bar, check, member):
                self.quarantine_requests += 1
        except Exception:   # the writer's fault never fails a read
            log.warning("md port: quarantine hand-off failed for %s", bar.series.canonical)

    # -------------------------------------------------------------------------------------------- indicators
    async def indicator(self, indicator_id: str, output: str, *, at_bucket_ms: int | None = None,
                        n: int | None = None, as_of_ms: int | None = None) -> IndicatorRead | DataUnavailable:
        if (at_bucket_ms is None) == (n is None) or (n is not None and not 1 <= n <= MAX_BARS_PER_READ):
            return DataUnavailable("bad_request")
        try:
            K.indicator(indicator_id, output)
        except ValueError:
            return DataUnavailable("bad_request")
        if self._global_failed():
            return DataUnavailable(FAILED)
        series = self._ind.input_series(indicator_id) if self._ind is not None else None
        try:
            opening = await self._head()
            if series is None:                  # not tracked in this process: the store's definition
                defs = await self._reader.registry_ind()
                series = input_series(self._registry, lambda i: IndicatorSpec.parse(defs[i]) if i in defs else None,
                                      indicator_id)
            if at_bucket_ms is not None:
                raw = await self._reader.indicator_range(indicator_id, output, at_bucket_ms, at_bucket_ms)
            else:
                raw = await self._reader.indicator_newest(indicator_id, output, n or 1)
            generation = await self._tail(opening)
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        except StoreUnavailable as e:
            return DataUnavailable(STORE_TIMEOUT if e.kind == TIMEOUT else STORE_ERROR)
        if not series:
            return DataUnavailable(NOT_REGISTERED)
        values: list[IndicatorValue] = []
        for _, member in raw:
            v = decode_value(indicator_id, output, member)
            if v is None:
                self.ind_undecodable += 1
                continue
            values.append(v)
        lo = at_bucket_ms if at_bucket_ms is not None else (values[0].bucket_start_ms if values else None)
        hi = at_bucket_ms if at_bucket_ms is not None else (values[-1].bucket_start_ms if values else None)
        try:
            for s in series:                    # the decision gate of `bars`, for every input series
                self._gate(s, lo, hi)
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        if values and values[-1].faulted:
            return DataUnavailable(INDICATOR_FAULTED)
        values = [v for v in values if not v.faulted]     # an older marker (before md_rebuild) holds no value
        if as_of_ms is not None:
            kept: list[IndicatorValue] = []
            for v in values:
                if v.final_at_ms is not None and v.final_at_ms <= as_of_ms:
                    kept.append(v)
                    continue
                if self._ind is None:
                    continue                    # no recompute wired: not current at as_of
                again = await self._ind.value_as_of(indicator_id, v.bucket_start_ms, as_of_ms)
                if isinstance(again, DataUnavailable):
                    return again
                if output in again:
                    kept.append(again[output])
            values = kept
        return IndicatorRead(tuple(values), generation)

    async def provisional(self, indicator_id: str, output: str) -> ProvisionalRead | DataUnavailable:
        try:
            opening = await self._head()
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        if self._ind is None:
            return DataUnavailable("not_registered")
        hook = self._hook

        def forming(series: SeriesId) -> Bar | None:
            if hook is None:
                return None
            return next((b for b in hook.forming(series.adapter_id, series.symbol) if b.series == series), None)

        return ProvisionalRead(self._ind.provisional(indicator_id, output, forming), opening.generation)

    def on_indicator_updated(self, callback: IndicatorCallback) -> None:
        self._ind_callbacks.append(callback)

    def wake_indicator(self, update: IndicatorUpdate) -> None:
        """`MdIndicators.on_updated` listener: route `indicator_updated` to the callbacks (ids only)."""
        for callback in self._ind_callbacks:
            try:
                callback(update)
            except Exception:   # a consumer's fault never stops md-indicators
                log.warning("md port: indicator callback failed for %s", update.indicator_id)

    # -------------------------------------------------------------------------------------------- demand
    def define(self, spec: SeriesId | IndicatorSpec) -> str:
        return self._demand.define(spec) if self._demand is not None else spec_id_of(spec)

    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        """A lease through the demand book; refused without one (the ARM then arms nothing on this series)."""
        if self._demand is None:
            return False
        return await self._demand.demand(spec_id, consumer, lease_class)

    async def release(self, spec_id: str, consumer: str) -> None:
        if self._demand is not None:
            await self._demand.release(spec_id, consumer)

    # -------------------------------------------------------------------------------------------- wake-ups
    def on_bar_closed(self, callback: BarCallback) -> None:
        self._closed.append(callback)

    def on_bar_revised(self, callback: BarCallback) -> None:
        self._revised.append(callback)

    def wake(self, event: BarEndEvent) -> None:
        """`MdWriter.on_released` listener: route the released event to the wake-up callbacks (ids only)."""
        for callback in self._revised if event.revised else self._closed:
            try:
                callback(event)
            except Exception:   # a consumer's fault never stops the writer
                log.warning("md port: wake-up callback failed for %s", event.series.canonical)

    # -------------------------------------------------------------------------------------------- pull cursor
    async def events_since(self, series: SeriesId,
                           cursor: EventCursor) -> EventsRead | CursorExpired | DataUnavailable:
        try:
            opening = await self._head()
            entries = await self._reader.rollback(series)
            generation = await self._tail(opening)
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        except StoreUnavailable as e:
            return DataUnavailable(STORE_TIMEOUT if e.kind == TIMEOUT else STORE_ERROR)
        result = events_after(series, entries, cursor, generation)
        if isinstance(result, CursorExpired):
            return result
        return EventsRead(result, generation)

    async def refetch(self, series: SeriesId, from_bucket_ms: int, to_bucket_ms: int) -> None | Unsupported:
        return self._refetch.request(series, from_bucket_ms, to_bucket_ms)

    # -------------------------------------------------------------------------------------------- status
    async def md_generation(self) -> MdGeneration | DataUnavailable:
        try:
            opening = await self._head()
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        return opening.generation

    async def series_status(self, series: SeriesId) -> SeriesStatus | DataUnavailable:
        generation = await self.md_generation()
        if isinstance(generation, DataUnavailable):
            return generation
        return SeriesStatus(self._book.state(series), self._book.gaps(series), generation)


def as_market_data_port(port: MdPort) -> MarketDataPort:
    """`MdPort` is a `MarketDataPort` (checked by mypy here; S31 hands it to the ARM through this type)."""
    return port


__all__ = ["MdPort", "HookReads", "StateGate", "DemandPort", "IndicatorReads", "as_market_data_port", "token_epoch"]
