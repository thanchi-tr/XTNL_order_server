"""Market-data core value types (design §A3, §A10, §A13; plan S12).

Pure values only: no I/O, no clock, no store. Prices (`Bar` OHLC, `Quote` bid / ask) are left out of `repr`, so an
accidental log of a bar or a quote shows its series, bucket and flags, never a price (design §B3: market-data logs and
pages carry series ids, buckets, field names and revisions, never values).

* `BarSpec`: a timeframe on the UTC grid (TradingView FX boundaries). v1 knows 1m, 5m, 15m and 1h (design §A3);
  4h and 1d need the 17:00 New York session anchor and come later with their own golden tests.
* `SeriesId`: `(adapter, symbol, bar_spec, side, source)`, single-source and never mixed. Its canonical string is the
  `{series}` part of every `md:*` key and the series part of every content hash and event id.
* `Bar`, `Quality`, `Quote`, `Liveness`, `MdGeneration`, `BarEndEvent`, `EventCursor`, `Sm12State`, `LeaseClass`.
* Port result values (never exceptions, design PR12): `DataUnavailable(reason)`, `Unsupported(reason)`,
  `Transient(reason)`, `CursorExpired()`. Their reasons are codes (series ids, check names), never prices.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Final

# ---------------------------------------------------------------------------------------------------- bar spec
SPEC_NAMES: Final = {60: "1m", 300: "5m", 900: "15m", 3600: "1h"}
"""The v1 timeframes (seconds → name). All are on the UTC grid."""
_SPEC_BY_NAME: Final = {name: tf for tf, name in SPEC_NAMES.items()}


@dataclass(frozen=True, order=True)
class BarSpec:
    """A bar timeframe on the UTC grid: bucket `[start, start + timeframe)` with `start % timeframe == 0`."""

    timeframe_s: int

    def __post_init__(self) -> None:
        if self.timeframe_s not in SPEC_NAMES:
            raise ValueError(f"bar spec of {self.timeframe_s} s is not one of {sorted(SPEC_NAMES.values())}")

    @classmethod
    def parse(cls, text: str) -> BarSpec:
        tf = _SPEC_BY_NAME.get(text.strip())
        if tf is None:
            raise ValueError(f"unknown bar spec {text!r}; expected one of {sorted(_SPEC_BY_NAME)}")
        return cls(tf)

    @property
    def name(self) -> str:
        return SPEC_NAMES[self.timeframe_s]

    @property
    def timeframe_ms(self) -> int:
        return self.timeframe_s * 1000

    def bucket_start_ms(self, ts_ms: int) -> int:
        """The start of the bucket holding `ts_ms` (floor to the grid; correct for negative times too)."""
        return ts_ms - ts_ms % self.timeframe_ms

    def bucket_end_ms(self, bucket_start_ms: int) -> int:
        return bucket_start_ms + self.timeframe_ms

    def on_grid(self, ts_ms: int) -> bool:
        return ts_ms % self.timeframe_ms == 0

    def __str__(self) -> str:
        return self.name


M1: Final = BarSpec(60)
M5: Final = BarSpec(300)
M15: Final = BarSpec(900)
H1: Final = BarSpec(3600)


# ---------------------------------------------------------------------------------------------------- series
class Side(str, Enum):
    """The quote side a series is built from (values match `src.dto.bars.BarSide`)."""

    BID = "BID"
    ASK = "ASK"


class Source(str, Enum):
    """`TICK`: built from this node's ticks (best effort, never backfilled). `BROKER`: the broker's own bars only."""

    TICK = "TICK"
    BROKER = "BROKER"


_PART_RE: Final = re.compile(r"[A-Za-z0-9._/#-]+")
"""Allowed characters of the adapter and symbol parts: no `:` (the separator), `|` (the member separator), space,
braces or glob characters, so a series id is safe inside any `md:*` key."""


@dataclass(frozen=True, order=True)
class SeriesId:
    """`(adapter, symbol, bar_spec, side, source)`; `canonical` = `adapter:symbol:spec:side:source`."""

    adapter_id: str
    symbol: str
    spec: BarSpec
    side: Side
    source: Source

    def __post_init__(self) -> None:
        for part, value in (("adapter_id", self.adapter_id), ("symbol", self.symbol)):
            if not _PART_RE.fullmatch(value):
                raise ValueError(f"series {part} must match {_PART_RE.pattern}")

    @property
    def canonical(self) -> str:
        return f"{self.adapter_id}:{self.symbol}:{self.spec.name}:{self.side.value}:{self.source.value}"

    @classmethod
    def parse(cls, text: str) -> SeriesId:
        parts = text.split(":")
        if len(parts) != 5:
            raise ValueError("a series id has five ':'-separated parts")
        adapter, symbol, spec, side, source = parts
        try:
            return cls(adapter, symbol, BarSpec.parse(spec), Side(side), Source(source))
        except ValueError as e:
            raise ValueError(f"invalid series id: {e}") from None

    def __str__(self) -> str:
        return self.canonical


# ---------------------------------------------------------------------------------------------------- bars
class Quality(str, Enum):
    """Design §A4 Quality. `NO_TRADE` needs positive evidence; `BLIND` is unknown (a quarantined bar included)."""

    COMPLETE = "complete"
    PARTIAL = "partial"
    DEGRADED = "degraded"
    NO_TRADE = "no_trade"
    BLIND = "blind"


@dataclass(frozen=True, kw_only=True)
class Bar:
    """One closed (or forming) bar of a series. `revision` is node-local and never part of an id (§A13);
    `final_at_ms` is None while the bar is not final. OHLC are excluded from `repr`."""

    series: SeriesId
    bucket_start_ms: int
    open: Decimal = field(repr=False)
    high: Decimal = field(repr=False)
    low: Decimal = field(repr=False)
    close: Decimal = field(repr=False)
    tick_count: int = 0
    quality: Quality = Quality.COMPLETE
    revision: int = 0
    final_at_ms: int | None = None

    @property
    def bucket_end_ms(self) -> int:
        return self.series.spec.bucket_end_ms(self.bucket_start_ms)

    @property
    def event_id(self) -> tuple[str, int]:
        """`(series, bar_end_ms)`: the same for the same bar on any node and after any reset (§A13)."""
        return (self.series.canonical, self.bucket_end_ms)


@dataclass(frozen=True, kw_only=True)
class Quote:
    """The latest validated quote of `(adapter, symbol)`; bid / ask excluded from `repr`."""

    adapter_id: str
    symbol: str
    bid: Decimal = field(repr=False)
    ask: Decimal = field(repr=False)
    broker_ms: int
    received_ms: int
    broker_timed: bool = True


class Liveness(str, Enum):
    LIVE = "LIVE"
    STALE = "STALE"
    STALE_NODE = "STALE_NODE"   # the store's writer epoch is not the current leader's (§A7 AWS)


class Sm12State(str, Enum):
    """SM12, per series (design §A7)."""

    EMPTY = "EMPTY"
    BACKFILLING = "BACKFILLING"
    LIVE = "LIVE"
    GAPPED = "GAPPED"
    FAILED = "FAILED"


class LeaseClass(str, Enum):
    """Demand lease classes, `ARMED > PREWARM` (design §A6)."""

    ARMED = "ARMED"
    PREWARM = "PREWARM"


# ---------------------------------------------------------------------------------------------------- generation, events
@dataclass(frozen=True, order=True)
class MdGeneration:
    """The md generation `(epoch revision, n)` (§A13): only rises, compared as a tuple."""

    epoch_rev: int
    n: int

    def __str__(self) -> str:
        return f"{self.epoch_rev}.{self.n}"


@dataclass(frozen=True, order=True)
class EventCursor:
    """A consumer's pull cursor `(bar_end_ms, write_seq)` (§A13); the `write_seq` part is valid only within the md
    generation it was read under (`generation`, not part of the order). `write_seq` 0 is the restart form: the
    consumer's durable acknowledgement `last_decided_bar` with no `write_seq` (after a generation change, S16)."""

    bar_end_ms: int
    write_seq: int
    generation: MdGeneration | None = field(default=None, compare=False)


@dataclass(frozen=True)
class BarEndEvent:
    """A bar-end key event (`bar_closed` / `bar_revised`): id `(series, bar_end_ms)` plus the keyed content hash.
    `write_seq` orders the rollback set; `revision` is node-local information, never identity."""

    series: SeriesId
    bar_end_ms: int
    content_hash: str
    write_seq: int
    revision: int
    revised: bool = False
    """`bar_revised` (an earlier bar of this id was replaced) rather than `bar_closed`; wake-up routing only."""

    @property
    def event_id(self) -> tuple[str, int]:
        return (self.series.canonical, self.bar_end_ms)

    @property
    def cursor(self) -> EventCursor:
        return EventCursor(self.bar_end_ms, self.write_seq)


# ---------------------------------------------------------------------------------------------------- port result values
# Reason codes of `DataUnavailable` (the list grows with later stages; codes only, never values).
NOT_LIVE: Final = "not_live"
CLOCK_STEP: Final = "clock_step"
SOURCE_MISMATCH: Final = "source_mismatch"
INVALID_BAR: Final = "invalid_bar"
INDICATOR_FAULTED: Final = "indicator_faulted"
QUARANTINED: Final = "quarantined"
STORE_TIMEOUT: Final = "store_timeout"
STORE_ERROR: Final = "store_error"
GENERATION_CHANGED: Final = "generation_changed"
STALE_NODE: Final = "stale_node"
FAILED: Final = "failed"


@dataclass(frozen=True)
class DataUnavailable:
    """A read that cannot be served now (design §A10); `reason` is a code such as `source_mismatch`."""

    reason: str


@dataclass(frozen=True)
class Unsupported:
    """The capability does not exist for this adapter / side (design PR3 go / no-go, PR12)."""

    reason: str = ""


@dataclass(frozen=True)
class Transient:
    """A read that failed this time (timeout, decode error, BARS cap refusal) and may succeed later (PR11, PR12)."""

    reason: str = ""


@dataclass(frozen=True)
class CursorExpired:
    """The cursor's `write_seq` is older than the rollback set or belongs to another generation (§A13)."""

    generation: MdGeneration | None = None
