"""ARM core value types (design §B6.1, §B6.4, §B7, §B8, §B14; plan S19).

Pure values only: no I/O, no clock, no store.

* σ (`sigma`): +1 for a long (`BUY`) position, −1 for a short (`SELL`), so `favour = σ·(close − E)`,
  `risk = σ·(E − S)` and `R = σ·(E − S0)` read the same for both sides.
* `Resource` (v1: `STOP_LEVEL` only), `ArmEvent` (PRE, POST and the bar-end re-runs LATE, REOPEN, CORRECTED),
  `BarEvent` (what a strategy's `triggers()` returns), `StopBasis`, `S0Source`, `EntryTimeSource`, `CloseSource`,
  `ScopeKind` (the §B14 FAILED scopes).
* `RMultiple` (0.01 R) and `Ratio` (0.001): the only typed decision metrics (`DECISION_METRICS`); dimensionless.
* `HandleView`, `BarContext`, `ArmContext`: what a strategy and a guard see. **No units** (design §B6.1).
* Input ids with keyed hashes (`SeriesInput`, `IndicatorInput`) and the HMAC `inputs_digest` (§B8): the only form in
  which market data is ever referenced from PostgreSQL. Every hash is `key id ‖ HMAC-SHA256[:16]` under the
  market-data keyring (`src.marketdata.hashing`), so it cannot be brute-forced back into prices.

Order-state levels (entry, stops, S0) and the instrument increment are kept out of every `repr`: the R distance is
never logged or shown (design §B3), and bars and quotes already hide their prices.
"""
from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Context, Decimal, InvalidOperation
from enum import Enum
from typing import Final

from src.dto import OrderKind, Side
from src.marketdata.hashing import HASH_BYTES, Keyring, KeyringError, key_id_of
from src.marketdata.indicators.base import IndicatorValue
from src.marketdata.types import Bar, BarSpec, Quote, SeriesId
from src.oversee.risk import StopHandleKind


# ---------------------------------------------------------------------------------------------------- σ
def sigma(side: Side) -> int:
    """+1 for `BUY` (long), −1 for `SELL` (short)."""
    if side == "BUY":
        return 1
    if side == "SELL":
        return -1
    raise ValueError("side must be BUY or SELL")


# ---------------------------------------------------------------------------------------------------- enums
class Resource(str, Enum):
    """What a strategy manages. The v1 registry accepts only `STOP_LEVEL` (design §B6.1 Resources)."""

    STOP_LEVEL = "STOP_LEVEL"


class ArmEvent(str, Enum):
    """`PRE` (anchor 1, provisional, never actionable), `POST` (anchor 2, final) and the bar-end re-runs."""

    PRE = "PRE"
    POST = "POST"
    LATE = "LATE"
    REOPEN = "REOPEN"
    CORRECTED = "CORRECTED"


BAR_END_EVENTS: Final = frozenset({ArmEvent.POST, ArmEvent.LATE, ArmEvent.REOPEN, ArmEvent.CORRECTED})
"""Events decided on the broker's final bar-end close: the only ones that may release a position (§B6.3 (7a))."""
TRIGGER_EVENTS: Final = frozenset({ArmEvent.PRE, ArmEvent.POST})
"""The events a strategy may declare in `triggers()` (v1 accepts only `BarEvent`)."""


@dataclass(frozen=True, order=True)
class BarEvent:
    """`triggers(params)` → `[BarEvent(bar_spec, PRE | POST)]`."""

    bar_spec: BarSpec
    event: ArmEvent

    def __post_init__(self) -> None:
        if self.event not in TRIGGER_EVENTS:
            raise ValueError("a strategy triggers on PRE or POST only")


class StopBasis(str, Enum):
    """Which stop the trigger used at a bar end (§B6.4), stored on the decision."""

    STREAM = "STREAM"
    READ = "READ"
    PRIOR_VERSION = "PRIOR_VERSION"
    AT_PRE = "AT_PRE"


class S0Source(str, Enum):
    """§B7: `AT_FILL` only from a live stream version within fill + 10 s; a history level is `HISTORY_LAST_LEVEL`
    (treated as `FIRST_SEEN` with a caveat)."""

    AT_FILL = "AT_FILL"
    FIRST_SEEN = "FIRST_SEEN"
    HISTORY_LAST_LEVEL = "HISTORY_LAST_LEVEL"


class EntryTimeSource(str, Enum):
    STREAM = "STREAM"
    BROKER = "BROKER"
    ESTIMATED = "ESTIMATED"


INDICATOR_SCOPE_MARK: Final = "ind"
"""The part of a SERIES scope id that names an indicator (round 13, plan S38d)."""


def indicator_scope_id(adapter_id: str, symbol: str, indicator_id: str) -> str:
    """The SERIES-kind FAILED scope id of one indicator (a **guard-input scope**, round 13): `adapter:symbol:ind:<id>`.
    An indicator fault fails this scope, never its input series, so a FAULTED ATR stops only the positions that
    still depend on it (FirstBarGuard before bar 0 is decided) and never blocks the input series' bar reads."""
    return f"{adapter_id}:{symbol}:{INDICATOR_SCOPE_MARK}:{indicator_id}"


def indicator_of_scope(scope_id: str) -> str | None:
    """The indicator id of an indicator scope id (`indicator_scope_id`), else None (a series' canonical id)."""
    parts = scope_id.split(":")
    if len(parts) == 4 and parts[2] == INDICATOR_SCOPE_MARK and parts[3]:
        return parts[3]
    return None


class CloseSource(str, Enum):
    """`QUOTE` on PRE rows, `BROKER` on every bar-end row; there is no TICK close (§B8)."""

    QUOTE = "QUOTE"
    BROKER = "BROKER"


class ScopeKind(str, Enum):
    """The FAILED scopes of §B14 (`arm_failures.scope_kind`)."""

    SERIES = "SERIES"
    ADAPTER = "ADAPTER"
    STRATEGY_VERSION = "STRATEGY_VERSION"
    ARM = "ARM"
    POSITION = "POSITION"
    """One position (`adapter_id:position_id`): a stuck replay frontier on fewer than `fail_frontier_positions_n`
    positions fails only those positions (round 12, plan S38c; migration 0018)."""


# ---------------------------------------------------------------------------------------------------- metrics
R_QUANTUM: Final = Decimal("0.01")
RATIO_QUANTUM: Final = Decimal("0.001")
_METRIC_CTX: Final = Context(prec=40)
"""Quantisation and `BarContext.r` run under this fixed context, never the ambient one, so they are the same in every
replay."""


def _on_grid(value: Decimal, quantum: Decimal) -> bool:
    """Finite, on the grid, in canonical form (exactly the quantum's exponent, no negative zero), so equal metrics
    always have the same text, stored value and hash."""
    try:
        return (isinstance(value, Decimal) and value.is_finite()
                and value.as_tuple().exponent == quantum.as_tuple().exponent
                and not (value.is_zero() and value.is_signed())
                and value == value.quantize(quantum, context=_METRIC_CTX))
    except InvalidOperation:
        return False


def _quantise(value: Decimal, quantum: Decimal) -> Decimal:
    q = value.quantize(quantum, rounding=ROUND_HALF_EVEN, context=_METRIC_CTX)
    return q.copy_abs() if q.is_zero() else q


@dataclass(frozen=True, order=True)
class RMultiple:
    """A dimensionless multiple of R, quantised to 0.01 R (`ROUND_HALF_EVEN`). Build with `RMultiple.of`."""

    value: Decimal

    def __post_init__(self) -> None:
        if not _on_grid(self.value, R_QUANTUM):
            raise ValueError("an R multiple is a finite value on the 0.01 grid, with two decimals")

    @classmethod
    def of(cls, value: Decimal) -> RMultiple:
        return cls(_quantise(value, R_QUANTUM))


@dataclass(frozen=True, order=True)
class Ratio:
    """A dimensionless ratio, quantised to 0.001 (`ROUND_HALF_EVEN`). Build with `Ratio.of`."""

    value: Decimal

    def __post_init__(self) -> None:
        if not _on_grid(self.value, RATIO_QUANTUM):
            raise ValueError("a ratio is a finite value on the 0.001 grid, with three decimals")

    @classmethod
    def of(cls, value: Decimal) -> Ratio:
        return cls(_quantise(value, RATIO_QUANTUM))


MetricValue = RMultiple | Ratio
METRIC_TYPES: Final = (RMultiple, Ratio)


# ---------------------------------------------------------------------------------------------------- contexts
@dataclass(frozen=True, kw_only=True)
class HandleView:
    """One current stop handle (PR1 `effective_stop_detail()`), with the time its level was read. No units."""

    kind: StopHandleKind
    handle_id: str
    level: Decimal = field(repr=False)
    read_ms: int | None = None
    order_kind: OrderKind | None = None


@dataclass(frozen=True, kw_only=True)
class ArmContext:
    """What is known at arming (`init_state`): side, entry, S0 and their sources, the fill bucket."""

    position_id: str
    adapter_id: str
    symbol: str
    side: Side
    bar_spec: BarSpec
    entry: Decimal = field(repr=False)
    s0: Decimal = field(repr=False)
    s0_source: S0Source
    entry_time_ms: int | None
    entry_time_source: EntryTimeSource
    armed_at_ms: int

    @property
    def sigma(self) -> int:
        return sigma(self.side)

    @property
    def fill_bucket_ms(self) -> int | None:
        return None if self.entry_time_ms is None else self.bar_spec.bucket_start_ms(self.entry_time_ms)


@dataclass(frozen=True, kw_only=True)
class BarContext:
    """Everything one evaluation of one position at one event may read (design §B6.1 BarContext), filled from
    `MarketDataPort` and PR1's `effective_stop_detail()` by the engine. Strategies and guards are pure functions of
    it (plus their pinned params and state).

    `stop` is `S` (the current effective level, Oversee's `effective_stop` by construction), `stop_at_t` is `S_at_T`
    at bar-end events with its `stop_basis`, `pre_basis` the stored `S_pre` while set (§B6.3). `bar` is the broker's
    final bar `T` on the trigger side at bar-end events (None at PRE); `quote` the trigger-side quote frozen at PRE
    fire time (None at bar-end events). `history` holds the BROKER bars of `required_history`, `indicators` the
    values of `required_indicators`, keyed `(indicator_id, output)`. No units.

    `decision_cap_ms` is this bar's `decision_cap` on the local clock (§B5.2, `src.marketdata.timing.decision_cap_ms`
    with the adapter's skew), so a guard can say how long a PENDING verdict may wait (FirstBarGuard: until bar 0's
    cap). `entry_time_ambiguous` is True when the fill lies within its timestamp uncertainty of a bucket boundary and
    the history execution time did not settle it (§B6.5): the entry time is then permanently not exact."""

    position_id: str
    adapter_id: str
    symbol: str
    side: Side
    bar_spec: BarSpec
    event: ArmEvent
    bar_end_ms: int
    bar_index: int | None
    entry: Decimal = field(repr=False)
    entry_time_ms: int | None = None
    entry_time_source: EntryTimeSource = EntryTimeSource.ESTIMATED
    entry_time_ambiguous: bool = False
    decision_cap_ms: int | None = None
    s0: Decimal = field(repr=False)
    s0_source: S0Source = S0Source.FIRST_SEEN
    handles: tuple[HandleView, ...] = ()
    stop: Decimal | None = field(default=None, repr=False)
    stop_at_t: Decimal | None = field(default=None, repr=False)
    stop_basis: StopBasis | None = None
    pre_basis: Decimal | None = field(default=None, repr=False)
    price_increment: Decimal | None = field(default=None, repr=False)
    min_stop_distance: Decimal | None = field(default=None, repr=False)
    bar: Bar | None = None
    quote: Quote | None = None
    history: tuple[tuple[SeriesId, tuple[Bar, ...]], ...] = ()
    indicators: tuple[tuple[tuple[str, str], tuple[IndicatorValue, ...]], ...] = ()
    exit_quote_live: bool = False
    bucket_evaluable: bool = True
    account_image_fresh: bool = True
    image_reasons: tuple[str, ...] = ()
    caveats: tuple[str, ...] = ()

    @property
    def sigma(self) -> int:
        return sigma(self.side)

    @property
    def r(self) -> Decimal:
        """`R = σ·(E − S0)` (a price distance: used in arithmetic only, never logged, stored or shown), computed
        under a fixed 40-digit context so it never depends on the ambient one."""
        return _METRIC_CTX.multiply(Decimal(self.sigma), _METRIC_CTX.subtract(self.entry, self.s0))

    def history_of(self, series: SeriesId) -> tuple[Bar, ...]:
        for s, bars in self.history:
            if s == series:
                return bars
        return ()

    def indicator_values(self, indicator_id: str, output: str) -> tuple[IndicatorValue, ...]:
        for key, values in self.indicators:
            if key == (indicator_id, output):
                return values
        return ()


# ---------------------------------------------------------------------------------------------------- keyed input ids
def _require_keyed(text: str) -> None:
    if key_id_of(text) is None:
        raise ValueError("an input hash is a keyed hash: key id then 32 hex digits")


@dataclass(frozen=True, order=True)
class SeriesInput:
    """A bar input of a decision: `(series_id, bar_end_ms, key_id‖hash)` (the bar's keyed content hash, §A13)."""

    series_id: str
    bar_end_ms: int
    content_hash: str

    def __post_init__(self) -> None:
        SeriesId.parse(self.series_id)
        _require_keyed(self.content_hash)

    @property
    def text(self) -> str:
        return f"S|{self.series_id}|{self.bar_end_ms}|{self.content_hash}"


@dataclass(frozen=True, order=True)
class IndicatorInput:
    """An indicator input: `(indicator_id, output, bar_end_ms, window_start_bucket, key_id‖hash)`, the hash being
    `window_hash` over the keyed hashes of the window's bars."""

    indicator_id: str
    output: str
    bar_end_ms: int
    window_start_bucket_ms: int
    content_hash: str

    def __post_init__(self) -> None:
        _require_keyed(self.content_hash)
        if "|" in self.indicator_id or "|" in self.output:
            raise ValueError("indicator id and output carry no '|'")

    @property
    def text(self) -> str:
        return (f"I|{self.indicator_id}|{self.output}|{self.bar_end_ms}|{self.window_start_bucket_ms}|"
                f"{self.content_hash}")


def _keyed(keyring: Keyring, message: str) -> str:
    key = keyring.key(keyring.current_id)
    if key is None:   # impossible for a ring built by Keyring.of
        raise KeyringError("the keyring holds no current key")
    mac = hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()[:HASH_BYTES]
    return f"{keyring.current_id:02x}{mac.hex()}"


def window_hash(keyring: Keyring, indicator_id: str, output: str, bar_end_ms: int, window_start_bucket_ms: int,
                bar_hashes: tuple[str, ...]) -> str:
    """The keyed hash of an indicator input: an HMAC over the keyed hashes of its window's bars (oldest first).
    Fields are joined with '|', so the indicator id and output may not hold one (as in `IndicatorInput`)."""
    if "|" in indicator_id or "|" in output:
        raise ValueError("indicator id and output carry no '|'")
    for h in bar_hashes:
        _require_keyed(h)
    return _keyed(keyring, "W|" + "|".join((indicator_id, output, str(bar_end_ms), str(window_start_bucket_ms),
                                           *bar_hashes)))


def lineage_hash(keyring: Keyring, indicator_id: str, output: str, bar_end_ms: int, window_start_bucket_ms: int,
                 input_digest: str, value_revision: int) -> str:
    """The keyed hash of an indicator input as the engine records it (round 13, plan S38d): an HMAC over the value's
    lineage (its window's input digest by bar id and revision, and the value revision), never the value. A revised
    window bar or a recomputed value changes it, so a correction compare sees the indicator input change."""
    if "|" in indicator_id or "|" in output or "|" in input_digest:
        raise ValueError("indicator id, output and digest carry no '|'")
    return _keyed(keyring, "L|" + "|".join((indicator_id, output, str(bar_end_ms), str(window_start_bucket_ms),
                                           input_digest, str(value_revision))))


def inputs_digest(keyring: Keyring, series_inputs: tuple[SeriesInput, ...],
                  indicator_inputs: tuple[IndicatorInput, ...]) -> str:
    """The decision's `inputs_digest` (§B8): an HMAC under the current key over every input id and hash, in sorted
    order (so the digest does not depend on the order the engine read them in)."""
    parts = [i.text for i in sorted(series_inputs)] + [i.text for i in sorted(indicator_inputs)]
    return _keyed(keyring, "D|" + "\n".join(parts))
