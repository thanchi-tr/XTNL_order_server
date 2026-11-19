"""The `Indicator` Protocol and its value types (design §A6, §A3 `md:ind`; plan S18, R11-m).

General from v1, so a multi-input, multi-output or multi-timeframe indicator needs no protocol, store or port change:

* `KIND`, `VERSION` (frozen; the registry is keyed on `(KIND, VERSION)`), `OUTPUTS` (each an `Output` with its unit
  `PRICE`, `RATIO` or `COUNT`), `DECIMAL_CONTEXT` (pinned per VERSION), `LOWER_BOUND` (the class's declared range, ATR
  ≥ 0) and `EXACT_SLIDING` (an O(1) incremental path is allowed only when the sliding form is exact; ATR's windowed
  seed is not, so its canonical value is computed on every update).
* `parse_params(raw)`: bounds; unknown keys rejected (`ParamError`, naming the key, never a value).
* `clock(spec)`: the output timeframe; `inputs(spec)`: `SeriesRef` / `IndicatorRef` (a DAG); `window(spec)`: per input
  `Count(n)` or `Anchored(anchor_spec, max_bars)` (R11-m: no fixed-count assumption).
* `seed(spec, history)` → `State` at the window's end (the canonical rule); `step(spec, state, aligned)` → the next
  state and outputs (pure, deterministic); `outputs(spec, state)`; `provisional(spec, state, forming)`.

**Alignment rule** (the one rule for same, lower and higher timeframes): an input bar or value is available at the
clock bucket `t` when it closed by `t`'s end (`bucket_end ≤ t_end`): a same-timeframe input contributes its bar for
`t`, a lower one its closed child bars, a higher one its last **closed** bar (no lookahead).

**Precision** (R11-m): values are exact `Decimal` under the pinned context and quantised only for storage and display,
per output by its unit: PRICE `price_increment × 10^-4` (the instrument's increment, pinned in the spec), RATIO
`1e-6`, COUNT `1`, `ROUND_HALF_EVEN`. Decisions recompute the exact canonical value from the bars.

Prices and values are kept out of every `repr` (`IndicatorSpec.price_increment` too: it is an instrument fact that
is never shown).
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Context, Decimal, InvalidOperation
from enum import Enum
from typing import ClassVar, Final, Protocol

from src.marketdata.codec import QUALITY_CODES
from src.marketdata.hashing import canonical_decimal
from src.marketdata.types import Bar, BarSpec, Quality, SeriesId

# ---------------------------------------------------------------------------------------------------- units, quantum
class Unit(str, Enum):
    PRICE = "PRICE"
    RATIO = "RATIO"
    COUNT = "COUNT"


RATIO_QUANTUM: Final = Decimal("0.000001")
COUNT_QUANTUM: Final = Decimal(1)
PRICE_QUANTUM_EXP: Final = -4
"""A PRICE output is quantised to `price_increment × 10^-4`."""

_WORD_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")
_KIND_RE: Final = re.compile(r"[A-Z][A-Z0-9_]{0,31}")
ID_RE: Final = re.compile(r"[0-9a-f]{16}")


class ParamError(ValueError):
    """Bad indicator parameters; the text names the key and its bounds, never a value."""


@dataclass(frozen=True)
class Output:
    name: str
    unit: Unit

    def __post_init__(self) -> None:
        if not _WORD_RE.fullmatch(self.name):
            raise ValueError(f"output name must match {_WORD_RE.pattern}")


def quantum(unit: Unit, price_increment: Decimal) -> Decimal:
    """The storage quantum of an output (pinned per VERSION through the unit and the spec's increment)."""
    if unit is Unit.PRICE:
        return price_increment.scaleb(PRICE_QUANTUM_EXP)
    if unit is Unit.RATIO:
        return RATIO_QUANTUM
    return COUNT_QUANTUM


def quantise(value: Decimal, q: Decimal, ctx: Context) -> Decimal:
    """`value` on the grid `q`, `ROUND_HALF_EVEN`, under the indicator's pinned context."""
    return value.quantize(q, rounding=ROUND_HALF_EVEN, context=ctx)


# ---------------------------------------------------------------------------------------------------- inputs, windows
@dataclass(frozen=True, order=True)
class SeriesRef:
    series: SeriesId


@dataclass(frozen=True, order=True)
class IndicatorRef:
    indicator_id: str
    output: str


InputRef = SeriesRef | IndicatorRef


@dataclass(frozen=True)
class Count:
    """The last `n` evaluable bars (or values) available at the clock bucket."""

    n: int

    def __post_init__(self) -> None:
        if self.n < 1:
            raise ValueError("a Count window holds at least one bar")


@dataclass(frozen=True)
class Anchored:
    """Bars from the start of the `anchor_spec` bucket holding the clock bucket (e.g. a session start), at most
    `max_bars`."""

    anchor_spec: BarSpec
    max_bars: int

    def __post_init__(self) -> None:
        if self.max_bars < 1:
            raise ValueError("an Anchored window holds at least one bar")


Window = Count | Anchored


@dataclass(frozen=True, kw_only=True)
class Point:
    """An indicator value as an input of a dependent indicator (value hidden from `repr`)."""

    bucket_start_ms: int
    value: Decimal = field(repr=False)
    final_at_ms: int | None = None


@dataclass(frozen=True)
class NoTrade:
    bucket_start_ms: int


@dataclass(frozen=True)
class Closed:
    bucket_start_ms: int


Aligned = Bar | Point | NoTrade | Closed | tuple[Bar, ...]
History = Mapping[InputRef, Sequence[Bar | Point]]


def available_at(bucket_end_ms: int, clock_bucket_start_ms: int, clock: BarSpec) -> bool:
    """The alignment rule: closed by the end of the clock bucket."""
    return bucket_end_ms <= clock.bucket_end_ms(clock_bucket_start_ms)


# ---------------------------------------------------------------------------------------------------- state
@dataclass(frozen=True)
class State:
    """An indicator's state: named fields as canonical text (prices included, so hidden from `repr`). Checkpoints
    store `encode()` in `md:ind_ckpt` (redis-md only)."""

    fields: tuple[tuple[str, str], ...] = field(default=(), repr=False)

    @classmethod
    def of(cls, **values: Decimal | int | str | None) -> State:
        out: list[tuple[str, str]] = []
        for name in sorted(values):
            v = values[name]
            if v is None:
                continue
            out.append((name, canonical_decimal(v) if isinstance(v, Decimal) else str(v)))
        return cls(tuple(out))

    def text(self, name: str) -> str | None:
        for k, v in self.fields:
            if k == name:
                return v
        return None

    def dec(self, name: str) -> Decimal | None:
        raw = self.text(name)
        return None if raw is None else Decimal(raw)

    def num(self, name: str, default: int = 0) -> int:
        raw = self.text(name)
        return default if raw is None else int(raw)

    def encode(self) -> str:
        return json.dumps(dict(self.fields), sort_keys=True, separators=(",", ":"))

    @classmethod
    def decode(cls, text: str) -> State | None:
        """The state of a checkpoint, or None when it does not decode (never raises)."""
        try:
            raw = json.loads(text)
        except ValueError:
            return None
        if not isinstance(raw, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in raw.items()):
            return None
        return cls(tuple(sorted(raw.items())))


# ---------------------------------------------------------------------------------------------------- spec
@dataclass(frozen=True, kw_only=True)
class IndicatorSpec:
    """One indicator instance: class `(kind, version)`, its primary series, normalised params and the instrument's
    price increment (pinned for the PRICE quantum). `indicator_id = sha256(canonical)[:16]`."""

    kind: str
    version: int
    series: SeriesId
    params: tuple[tuple[str, str], ...] = ()
    price_increment: Decimal = field(repr=False)

    def __post_init__(self) -> None:
        if not _KIND_RE.fullmatch(self.kind) or not 1 <= self.version <= 9_999:
            raise ValueError("indicator kind / version out of form")
        if not self.price_increment.is_finite() or self.price_increment <= 0:
            raise ValueError("price increment must be positive")

    @property
    def canonical(self) -> str:
        return json.dumps({"k": self.kind, "v": self.version, "s": self.series.canonical, "p": dict(self.params),
                           "q": canonical_decimal(self.price_increment)}, sort_keys=True, separators=(",", ":"))

    @property
    def indicator_id(self) -> str:
        return hashlib.sha256(self.canonical.encode("utf-8")).hexdigest()[:16]

    def param(self, name: str) -> str:
        for k, v in self.params:
            if k == name:
                return v
        raise KeyError(name)

    @classmethod
    def parse(cls, text: str) -> IndicatorSpec | None:
        """The spec of an `md:registry:ind` definition, or None (never raises)."""
        try:
            raw = json.loads(text)
            if not isinstance(raw, dict) or set(raw) != {"k", "v", "s", "p", "q"}:
                return None
            params = raw["p"]
            if not isinstance(params, dict) or not all(isinstance(v, str) for v in params.values()):
                return None
            return cls(kind=str(raw["k"]), version=int(raw["v"]), series=SeriesId.parse(str(raw["s"])),
                       params=tuple(sorted(params.items())), price_increment=Decimal(str(raw["q"])))
        except (ValueError, TypeError, KeyError, InvalidOperation):
            return None


# ---------------------------------------------------------------------------------------------------- the Protocol
class Indicator(Protocol):
    KIND: ClassVar[str]
    VERSION: ClassVar[int]
    OUTPUTS: ClassVar[tuple[Output, ...]]
    DECIMAL_CONTEXT: ClassVar[Context]
    LOWER_BOUND: ClassVar[Decimal | None]
    EXACT_SLIDING: ClassVar[bool]

    def parse_params(self, raw: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        """Normalised params; `ParamError` for an unknown key or a value out of bounds."""
        ...

    def clock(self, spec: IndicatorSpec) -> BarSpec: ...

    def inputs(self, spec: IndicatorSpec) -> tuple[InputRef, ...]: ...

    def window(self, spec: IndicatorSpec) -> Mapping[InputRef, Window]: ...

    def warm_counts(self, spec: IndicatorSpec) -> Mapping[InputRef, int]:
        """Evaluable bars per input a `warm` value needs, the indicator's own declared window `W(input)` (ATR: `W`).
        The engine never assumes an ATR `length` (round 12, S38c): warm-up, backfill depth, admission and retention
        fit read only these three declarations, so a new indicator class needs no engine change."""
        ...

    def warm_span(self, spec: IndicatorSpec) -> int:
        """Clock buckets before `t` that must hold no `blind` bucket for a `warm` value: the clean tail (ATR:
        `3 × length`; another indicator declares its own, 0 included)."""
        ...

    def backfill_bars(self, spec: IndicatorSpec) -> int:
        """Bars of the primary series the backfill, admission and retention checks use: `W` plus the warm-up the
        indicator declares (ATR: `W + 3 × length`)."""
        ...

    def seed(self, spec: IndicatorSpec, history: History) -> State: ...

    def step(self, spec: IndicatorSpec, state: State,
             aligned: Mapping[InputRef, Aligned]) -> tuple[State, Mapping[str, Decimal]]: ...

    def outputs(self, spec: IndicatorSpec, state: State) -> Mapping[str, Decimal] | None:
        """The outputs the state holds, or None when there is not enough history for any value."""
        ...

    def provisional(self, spec: IndicatorSpec, state: State,
                    forming: Mapping[InputRef, Bar | Point]) -> Mapping[str, Decimal]: ...


# ---------------------------------------------------------------------------------------------------- values
@dataclass(frozen=True)
class Lineage:
    """`(indicator_id, window_start_bucket, input_revision_digest, value_revision)` plus the FAULTED reason."""

    indicator_id: str
    window_start_ms: int | None
    input_digest: str
    value_revision: int
    reason: str = ""


@dataclass(frozen=True, kw_only=True)
class IndicatorValue:
    """One stored value (quantised). `warm` here is the window rule (W evaluable bars, no blind bucket in the warm
    span); a consumer also needs the value's bucket to equal the latest closed input bucket. A FAULTED marker has no
    value and the reason in its lineage."""

    indicator_id: str
    output: str
    bucket_start_ms: int
    value: Decimal | None = field(repr=False)
    quality: Quality
    warm: bool
    final_at_ms: int | None
    lineage: Lineage

    @property
    def faulted(self) -> bool:
        return self.value is None


@dataclass(frozen=True)
class IndicatorUpdate:
    """`indicator_updated`, in process: ids and buckets only (a wake-up; consumers re-read through the port)."""

    indicator_id: str
    bucket_start_ms: int
    bar_end_ms: int
    faulted: bool = False


def input_digest(bars: Sequence[Bar]) -> str:
    """A digest of the window's inputs by id and revision only (series, bucket, revision): never a price."""
    h = hashlib.sha256()
    for b in bars:
        h.update(f"{b.series.canonical}|{b.bucket_start_ms}|{b.revision};".encode("ascii"))
    return h.hexdigest()[:16]


# ---------------------------------------------------------------------------------------------------- member codec
FAULT_VALUE: Final = "F"
_QUALITY_BY_CODE: Final = {code: q for q, code in QUALITY_CODES.items()}
_VALUE_MEMBER_RE: Final = re.compile(
    r"([0-9]{13})\|(F|-?[0-9]+(?:\.[0-9]+)?)\|([CPDNB])\|([01])\|([0-9]{13})\|([0-9]{13})\|([0-9a-f]{16})\|"
    r"([0-9]{4})\|([a-z][a-z0-9_]{0,31}|-)"
)
VALUE_REVISION_MAX: Final = 9_999


def encode_value(v: IndicatorValue) -> str:
    """`<bucket:13>|<value|F>|<quality>|<warm>|<final_at:13>|<window_start:13>|<digest:16>|<rev:4>|<reason|->`."""
    if v.value is None:
        text = FAULT_VALUE
    else:
        text = format(v.value, "f")
    reason = v.lineage.reason or "-"
    if reason != "-" and not _WORD_RE.fullmatch(reason):
        raise ValueError("a fault reason is a short code")
    if not ID_RE.fullmatch(v.lineage.input_digest):
        raise ValueError("input digest out of form")
    return (f"{v.bucket_start_ms:013d}|{text}|{QUALITY_CODES[v.quality]}|{1 if v.warm else 0}|"
            f"{v.final_at_ms or 0:013d}|{v.lineage.window_start_ms or 0:013d}|{v.lineage.input_digest}|"
            f"{min(v.lineage.value_revision, VALUE_REVISION_MAX):04d}|{reason}")


def decode_value(indicator_id: str, output: str, member: str | bytes) -> IndicatorValue | None:
    """A stored value, or None for a member not of the written form (never raises)."""
    try:
        text = member.decode("ascii") if isinstance(member, bytes) else member
    except UnicodeDecodeError:
        return None
    m = _VALUE_MEMBER_RE.fullmatch(text)
    if m is None:
        return None
    raw = m.group(2)
    value = None if raw == FAULT_VALUE else Decimal(raw)
    final_at = int(m.group(5))
    window_start = int(m.group(6))
    reason = "" if m.group(9) == "-" else m.group(9)
    return IndicatorValue(indicator_id=indicator_id, output=output, bucket_start_ms=int(m.group(1)), value=value,
                          quality=_QUALITY_BY_CODE[m.group(3)], warm=m.group(4) == "1",
                          final_at_ms=final_at or None,
                          lineage=Lineage(indicator_id, window_start or None, m.group(7), int(m.group(8)), reason))


__all__ = [
    "Unit", "Output", "quantum", "quantise", "SeriesRef", "IndicatorRef", "InputRef", "Count", "Anchored", "Window",
    "Point", "NoTrade", "Closed", "Aligned", "History", "available_at", "State", "IndicatorSpec", "Indicator",
    "Lineage", "IndicatorValue", "IndicatorUpdate", "input_digest", "encode_value", "decode_value", "ParamError", "ID_RE",
]
