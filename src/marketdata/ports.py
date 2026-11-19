"""Ports of the market-data layer (design §A10, PR12, §B3; plan S12).

Protocols only. The scoped packages never import `src.runtime`, `src.adapters` or the orchestrator: they receive these
narrow ports, implemented in `src/runtime/md_ports.py` (S31) as `__slots__` wrappers that keep the orchestrator
private. Every result is a value (`DataUnavailable`, `Unsupported`, `Transient`, `CursorExpired`), never a broker or
store exception, so one bad response or a slow store never raises into a task.

* `MarketDataPort`: the one read port of `redis-md` (ARM now, automated strategies later). Every result carries the md
  generation it was read under (§A13). Decision reads are bucket-addressed and return `DataUnavailable` unless the
  series is LIVE, or BACKFILLING with the needed range complete. S18 adds the indicator reads (`indicator`,
  `provisional`, `on_indicator_updated`) and `define` (the spec id of a series or an indicator for `demand`).
* `BrokerBarsPort`: `fetch_price_bars` at REST priority `BARS` (count or range form, R11-M15).
* `StopHandleReadPort`: PR9's one list read per adapter for a set of stop handles.
* `StampPort` (task liveness), `PagePort` (operational pages, S15), `CalendarPort` (`state()`; None counts as
  closed), `AdapterHealthPort` (broker outage predicates of §B14), `NodeRecordPort` (this node's sentinel record for
  the §A7 classification).
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Protocol

from src.calendar.machine import OPEN_STATES, SessionState
from src.dto.bars import PriceBarsResponse
from src.marketdata.indicators.base import IndicatorSpec, IndicatorUpdate, IndicatorValue
from src.marketdata.types import (
    Bar,
    BarEndEvent,
    CursorExpired,
    DataUnavailable,
    EventCursor,
    LeaseClass,
    Liveness,
    MdGeneration,
    Quote,
    SeriesId,
    Side,
    Sm12State,
    Transient,
    Unsupported,
)
from src.oversee.risk import StopHandleKind


# ---------------------------------------------------------------------------------------------------- read values
@dataclass(frozen=True)
class QuoteRead:
    quote: Quote | None
    liveness: Liveness
    generation: MdGeneration


@dataclass(frozen=True)
class FormingRead:
    bar: Bar | None
    coverage_start_ms: int | None
    generation: MdGeneration


@dataclass(frozen=True)
class BarsRead:
    """Bars oldest first, single-source; a bucket with no bar is absent (absent means blind, §A4)."""

    bars: tuple[Bar, ...]
    generation: MdGeneration


@dataclass(frozen=True)
class EventsRead:
    """`events_since`: every rollback-set entry after the cursor, revisions of older ids included (§A13)."""

    events: tuple[BarEndEvent, ...]
    generation: MdGeneration

    def next_cursor(self, previous: EventCursor) -> EventCursor:
        """The cursor for the next pull: the last event's `(bar_end_ms, write_seq)` under this read's generation, or
        `previous` re-labelled with it when nothing was returned."""
        last = self.events[-1].cursor if self.events else previous
        return EventCursor(last.bar_end_ms, last.write_seq, self.generation)


@dataclass(frozen=True)
class SeriesStatus:
    state: Sm12State
    gaps: int
    generation: MdGeneration


@dataclass(frozen=True)
class IndicatorRead:
    """Indicator values oldest first (quantised, with quality, warm and lineage); the newest one FAULTED is
    `DataUnavailable(indicator_faulted)` instead."""

    values: tuple[IndicatorValue, ...]
    generation: MdGeneration


@dataclass(frozen=True)
class ProvisionalRead:
    """`provisional()` from the forming bar (display only; never stored, hidden from `repr`)."""

    value: Decimal | None = field(repr=False)
    generation: MdGeneration


BarCallback = Callable[[BarEndEvent], None]
IndicatorCallback = Callable[[IndicatorUpdate], None]


class MarketDataPort(Protocol):
    async def quote(self, adapter_id: str, symbol: str) -> QuoteRead | DataUnavailable: ...

    async def quote_at(self, adapter_id: str, symbol: str, at_ms: int) -> QuoteRead | DataUnavailable:
        """The latest validated quote as it was at local time `at_ms` (the PRE quote frozen at fire, round 13):
        LIVE when it was at most `quote_stale_ms` old at `at_ms`, else STALE (no quote)."""
        ...

    async def forming_bar(self, series: SeriesId) -> FormingRead | DataUnavailable: ...

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable:
        """One bucket (`at_bucket_ms`), the newest `n`, or a range; `as_of_ms` reads point-in-time (§A10)."""
        ...

    async def best_bars(self, adapter_id: str, symbol: str, spec_name: str, side: Side,
                        n: int) -> BarsRead | DataUnavailable:
        """BROKER where present, else TICK, labelled: display only, never the ARM."""
        ...

    async def indicator(self, indicator_id: str, output: str, *, at_bucket_ms: int | None = None,
                        n: int | None = None, as_of_ms: int | None = None) -> IndicatorRead | DataUnavailable:
        """One bucket or the newest `n` values; `as_of_ms` recomputes the canonical value from the `as_of` bars when
        the stored one became final later (§A10)."""
        ...

    async def provisional(self, indicator_id: str, output: str) -> ProvisionalRead | DataUnavailable: ...

    def define(self, spec: SeriesId | IndicatorSpec) -> str:
        """The spec id of a series or an indicator, registered for `demand` (S18)."""
        ...

    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        """Assert a demand lease; False when refused at the hard ceiling (§A6)."""
        ...

    async def release(self, spec_id: str, consumer: str) -> None: ...

    def on_bar_closed(self, callback: BarCallback) -> None:
        """Wake-ups only: a consumer always re-reads through the port."""
        ...

    def on_bar_revised(self, callback: BarCallback) -> None: ...

    def on_indicator_updated(self, callback: IndicatorCallback) -> None:
        """Wake-ups only, after the value write (§A6 event order)."""
        ...

    async def events_since(self, series: SeriesId,
                           cursor: EventCursor) -> EventsRead | CursorExpired | DataUnavailable: ...

    async def refetch(self, series: SeriesId, from_bucket_ms: int, to_bucket_ms: int) -> None | Unsupported:
        """Ask `md-bars` to re-fetch BROKER buckets (§A13); TICK series answer `Unsupported`."""
        ...

    async def md_generation(self) -> MdGeneration | DataUnavailable: ...

    async def series_status(self, series: SeriesId) -> SeriesStatus | DataUnavailable: ...


# ---------------------------------------------------------------------------------------------------- broker reads
BarsResult = PriceBarsResponse | Unsupported | Transient
"""A decoded broker answer, or a port-level value (decode errors, timeouts and BARS-cap refusals are `Transient`)."""


class BrokerBarsPort(Protocol):
    async def fetch_price_bars(self, adapter_id: str, symbol: str, side: Side, timeframe_s: int, *,
                               count: int | None = None, from_ms: int | None = None,
                               to_ms: int | None = None) -> BarsResult: ...


class HandleStatus(str, Enum):
    OK = "OK"
    NOT_FOUND = "NOT_FOUND"
    TRANSIENT = "TRANSIENT"


@dataclass(frozen=True)
class HandleRef:
    """A stop handle as the ARM pinned it: its own `stop_loss` field (POSITION_FIELD) or a child order."""

    kind: StopHandleKind
    id: str


@dataclass(frozen=True)
class HandleRead:
    """One handle of a list read: `OK` with the broker request's send time and the committed version label (the
    ARM uses the level only once PostgreSQL shows that version, PR9), `NOT_FOUND` or `TRANSIENT`."""

    handle: HandleRef
    status: HandleStatus
    sent_ms: int | None = None
    version: str | None = None


@dataclass(frozen=True)
class StopHandleRead:
    adapter_id: str
    handles: tuple[HandleRead, ...]
    sent_ms: int | None


StopHandleReadResult = StopHandleRead | Unsupported | Transient


class StopHandleReadPort(Protocol):
    async def read_stop_handles(self, adapter_id: str,
                                handles: Sequence[HandleRef]) -> StopHandleReadResult:
        """One list read per adapter for all its armed handles (PR9, R11-M20); never one call per handle."""
        ...


# ---------------------------------------------------------------------------------------------------- runtime ports
class StampPort(Protocol):
    """Task liveness (the supervisor's `TaskContext`, behind a wrapper)."""

    def stamp(self) -> None: ...

    def set_exempt(self, reason: str | None) -> None: ...


class PagePort(Protocol):
    """An operational page (`EngineSession.page`, behind a wrapper). Title and body carry codes, series ids, counts
    and percentages only, never a price."""

    def page(self, severity: str, title: str, body: str) -> None: ...


class CalendarPort(Protocol):
    def state(self, adapter_id: str, symbol: str) -> SessionState | None:
        """SM10 state; None (no calendar loaded, unknown symbol) counts as closed."""
        ...


def calendar_open(calendar: CalendarPort, adapter_id: str, symbol: str) -> bool:
    """True only for an OPEN or OPEN_BY_EVIDENCE state; None and every other state count as closed."""
    return calendar.state(adapter_id, symbol) in OPEN_STATES


class AdapterHealthPort(Protocol):
    """The §B14 broker-outage predicates (cap counters pause while either holds; `broker_unavailable`)."""

    def is_streaming(self, adapter_id: str) -> bool: ...

    def rest_failing_everywhere(self, adapter_id: str) -> bool | None:
        """True while the adapter's REST reads fail at every priority; None when unknown (no recent outcome at a
        priority other than `BARS`, `RestBudgetStats.failing_at_every_priority`): then no `broker_unavailable` page
        and bar-read failures do not count toward FAILED (§B14, R11-M10)."""
        ...


@dataclass(frozen=True)
class NodeMdRecord:
    """This node's record of the `md:boot` sentinel it wrote and the newest `last_write_ms` its writer wrote (§A7
    classification; kept in its own `arm_status` row). Ids and times only."""

    node: str
    sentinel: str
    last_write_ms: int


class NodeRecordPort(Protocol):
    async def load_record(self) -> NodeMdRecord | None: ...

    async def save_record(self, record: NodeMdRecord) -> None: ...
