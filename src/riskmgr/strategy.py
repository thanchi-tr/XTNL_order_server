"""The stop-loss strategy contract (design §B6.1; plan S19).

`SlStrategy` is the Protocol every strategy class implements; the registry (`src/riskmgr/registry.py`) is keyed on
`(KIND, IMPL_VERSION)` and accepts only `RESOURCE = STOP_LEVEL`. Strategies are **pure**: every method is a
deterministic function of its arguments, computed under the class's pinned `DECIMAL_CONTEXT` (the job runner, S22,
enters it), so a decision re-runs identically in replay and corrections.

* **Params:** `parse_params(raw)` takes the strategy's own keys (the registry splits off `guards` and parses each
  entry with its guard class) and returns them normalised as sorted `(key, canonical text)` pairs; an unknown key or
  an out-of-bound value raises `ParamError`, whose text names the key and its bounds, never the value. The pinned
  form is `StrategyParams` (own pairs plus the parsed guard list, in declared order). Envelope keys are local engine
  settings and never strategy params (§B6.1, `src/riskmgr/envelope.py`).
* **State:** `StrategyState` holds named fields as canonical text (`arm_positions.strategy_state`). It holds
  order-state facts and flags only, never a bar, quote or indicator value (design §B3 / §B8).
* **Evaluation:** `propose(params, ctx, state, event)` → `Evaluation` = `trigger_met` (the bar-end condition for the
  target holds, R11-m), a `Proposal[STOP_LEVEL]` (handle → target) or a KIND-scoped reason, typed metrics and caveats.
  The release check (§B6.3 (7a)) is defined on the Evaluation and `at_target`, never on one kind's reasons.

Deviation from the §B6.1 sketch: the pinned params are passed explicitly as the first argument of every method that
needs them (`propose(params, ctx, state, event)`), because the sketch leaves their source implicit; `required_indicators`
and `required_history` return templates on `(bar_spec, side)` (always BROKER), which the arm-tracker resolves to
concrete series and `IndicatorSpec`s per position (adapter, symbol, price increment).
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Context, Decimal, InvalidOperation
from enum import Enum
from typing import ClassVar, Final, Protocol

from src.dto import Side
from src.marketdata.hashing import canonical_decimal
from src.marketdata.indicators.base import IndicatorSpec
from src.marketdata.ports import HandleRef
from src.marketdata.types import Bar, BarSpec, Quality, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.outcomes import Category, Outcome
from src.riskmgr.types import ArmContext, ArmEvent, BarContext, BarEvent, MetricValue, RMultiple, Ratio, Resource

STRATEGY_KIND_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")
PARAM_KEY_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")
VERSION_MAX: Final = 9_999
DECIMAL_TEXT_RE: Final = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)")
"""The text form a numeric param may take (ASCII digits only; no whitespace, underscore, exponent or NaN)."""


class ParamError(ValueError):
    """Bad strategy or guard parameters; the text names the key and its bounds, never a value."""


# ---------------------------------------------------------------------------------------------------- param readers
def reject_unknown(raw: Mapping[str, object], allowed: frozenset[str], owner: str) -> None:
    """`ParamError` naming the first unknown key (sorted), or a key that is not text."""
    if not all(isinstance(k, str) for k in raw):
        raise ParamError(f"{owner}: parameter names are text")
    for key in sorted(raw):
        if key not in allowed:
            raise ParamError(f"{owner}: unknown parameter {key[:40]!r}")


def read_choice(raw: Mapping[str, object], key: str, options: tuple[str, ...], default: str, owner: str) -> str:
    value = raw.get(key, default)
    if not isinstance(value, str) or value not in options:
        raise ParamError(f"{owner}: {key} must be one of {', '.join(options)}")
    return value


def read_decimal(raw: Mapping[str, object], key: str, lo: Decimal, hi: Decimal, default: Decimal,
                 owner: str) -> Decimal:
    """A number in `[lo, hi]` (text, int or Decimal; never a float or a bool), as an exact Decimal. Text must be a
    plain decimal (`DECIMAL_TEXT_RE`): no whitespace, underscores or exponent, so a non-canonical form is refused
    rather than quietly normalised."""
    value = raw.get(key, default)
    try:
        if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
            raise InvalidOperation
        if isinstance(value, str) and not DECIMAL_TEXT_RE.fullmatch(value):
            raise InvalidOperation
        d = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ParamError(f"{owner}: {key} must be a number in [{lo}, {hi}]") from None
    if not d.is_finite() or not lo <= d <= hi:
        raise ParamError(f"{owner}: {key} must be a number in [{lo}, {hi}]")
    return d


def read_int(raw: Mapping[str, object], key: str, lo: int, hi: int, default: int, owner: str) -> int:
    d = read_decimal(raw, key, Decimal(lo), Decimal(hi), Decimal(default), owner)
    if d != d.to_integral_value():
        raise ParamError(f"{owner}: {key} must be an integer in [{lo}, {hi}]")
    return int(d)


def param_pairs(**values: str | int | Decimal) -> tuple[tuple[str, str], ...]:
    """Normalised params: sorted `(key, canonical text)` pairs (Decimals without trailing zeros)."""
    out: list[tuple[str, str]] = []
    for key in sorted(values):
        if not PARAM_KEY_RE.fullmatch(key):
            raise ValueError("parameter key out of form")
        v = values[key]
        out.append((key, canonical_decimal(v) if isinstance(v, Decimal) else str(v)))
    return tuple(out)


def _lookup(pairs: tuple[tuple[str, str], ...], name: str) -> str:
    for k, v in pairs:
        if k == name:
            return v
    raise KeyError(name)


# ---------------------------------------------------------------------------------------------------- params
@dataclass(frozen=True, order=True)
class GuardSpec:
    """One parsed entry of `params.guards`: `(kind, version, normalised params)`, pinned with the strategy."""

    kind: str
    version: int
    params: tuple[tuple[str, str], ...] = ()

    def param(self, name: str) -> str:
        return _lookup(self.params, name)

    def canonical(self) -> dict[str, object]:
        return {"kind": self.kind, "version": self.version, "params": dict(self.params)}


@dataclass(frozen=True)
class StrategyParams:
    """The pinned params of one position (§B6.1 Pinned per position): the strategy's own normalised pairs plus its
    guard list in declared order. `params_hash` covers both, so the pin's hash also covers the guard list."""

    kind: str
    impl_version: int
    values: tuple[tuple[str, str], ...] = ()
    guards: tuple[GuardSpec, ...] = ()

    def param(self, name: str) -> str:
        return _lookup(self.values, name)

    @property
    def canonical(self) -> str:
        return json.dumps({"kind": self.kind, "impl_version": self.impl_version, "params": dict(self.values),
                           "guards": [g.canonical() for g in self.guards]}, sort_keys=True, separators=(",", ":"))

    @property
    def params_hash(self) -> str:
        return hashlib.sha256(self.canonical.encode("utf-8")).hexdigest()[:16]

    @property
    def guards_hash(self) -> str:
        text = json.dumps([g.canonical() for g in self.guards], sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class StrategyState:
    """A strategy's per-position state: named fields as canonical text (`arm_positions.strategy_state`). Order-state
    facts and flags only; hidden from `repr` all the same."""

    fields: tuple[tuple[str, str], ...] = field(default=(), repr=False)

    @classmethod
    def of(cls, **values: Decimal | int | str | bool | None) -> StrategyState:
        out: list[tuple[str, str]] = []
        for name in sorted(values):
            v = values[name]
            if v is None:
                continue
            if isinstance(v, bool):
                text = "1" if v else "0"
            else:
                text = canonical_decimal(v) if isinstance(v, Decimal) else str(v)
            out.append((name, text))
        return cls(tuple(out))

    def text(self, name: str) -> str | None:
        for k, v in self.fields:
            if k == name:
                return v
        return None

    def flag(self, name: str) -> bool:
        return self.text(name) == "1"

    def names(self) -> frozenset[str]:
        return frozenset(k for k, _ in self.fields)

    def encode(self) -> str:
        return json.dumps(dict(self.fields), sort_keys=True, separators=(",", ":"))

    @classmethod
    def decode(cls, text: str) -> StrategyState | None:
        """The stored state, or None when it does not decode (never raises)."""
        try:
            raw = json.loads(text)
        except ValueError:
            return None
        if not isinstance(raw, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in raw.items()):
            return None
        return cls(tuple(sorted(raw.items())))


@dataclass(frozen=True)
class Refuse:
    """`migrate_state` refuses: the old pin is kept and the `arm_repin` request is reported moot."""

    reason: str


# ---------------------------------------------------------------------------------------------------- requirements
@dataclass(frozen=True, order=True)
class IndicatorNeed:
    """A required indicator on the position's BROKER series `(bar_spec, side)`: resolved per position (`resolve`)."""

    kind: str
    version: int
    bar_spec: BarSpec
    side: QuoteSide
    params: tuple[tuple[str, str], ...] = ()

    def resolve(self, adapter_id: str, symbol: str, price_increment: Decimal) -> IndicatorSpec:
        series = SeriesId(adapter_id, symbol, self.bar_spec, self.side, Source.BROKER)
        return IndicatorSpec(kind=self.kind, version=self.version, series=series, params=self.params,
                             price_increment=price_increment)


@dataclass(frozen=True, order=True)
class HistoryNeed:
    """BROKER bars of `(bar_spec, side)` the strategy or guard reads from its context: the newest `bars`
    **evaluable** bars (§A4: not `blind` / `no_trade`, final) ending at the decided bar, the same `Count` rule as
    `md-indicators` (a `Count(n)` window counts evaluable bars, never raw buckets). The reader (S21) reads further
    back past non-evaluable buckets until `held_by` is true, the read returns nothing older or the series retention
    is reached (as `md-indicators`' `_extend_back` does), so a `no_trade` or `blind` bucket inside the span never
    shortens the window. The non-evaluable bars inside the span may be passed along; consumers skip them. Fewer
    evaluable bars reach the context only when the series holds no more."""

    bar_spec: BarSpec
    side: QuoteSide
    bars: int

    def __post_init__(self) -> None:
        if self.bars < 1:
            raise ValueError("a history need holds at least one bar")

    def resolve(self, adapter_id: str, symbol: str) -> SeriesId:
        return SeriesId(adapter_id, symbol, self.bar_spec, self.side, Source.BROKER)

    def held_by(self, bars: Sequence[Bar], upto_end_ms: int) -> bool:
        """`bars` hold at least `self.bars` evaluable bars (distinct buckets) ending at or before `upto_end_ms`."""
        buckets = {b.bucket_start_ms for b in bars
                   if b.quality not in (Quality.BLIND, Quality.NO_TRADE) and b.final_at_ms is not None
                   and b.bucket_end_ms <= upto_end_ms}
        return len(buckets) >= self.bars


class S0Requirement(str, Enum):
    AT_FILL = "AT_FILL"
    ANY = "ANY"


class EntryTimeRequirement(str, Enum):
    EXACT = "EXACT"
    ANY = "ANY"


@dataclass(frozen=True)
class ArmingRequirements:
    """Unmet at arming = `UNSUPPORTED` (§B7)."""

    s0: S0Requirement = S0Requirement.ANY
    entry_time: EntryTimeRequirement = EntryTimeRequirement.ANY


class LoosenPolicy(str, Enum):
    REARM = "REARM"
    TELL_ONLY = "TELL_ONLY"


class ReopenPolicy(str, Enum):
    REUSE_TARGET = "REUSE_TARGET"
    REPROPOSE = "REPROPOSE"


class ReleasePolicy(str, Enum):
    """Whether the engine's release check (§B6.3 (7a)) applies to a strategy (round 12, plan S38c):
    `ON_CONFIRMED_TARGET` releases a position (`COMPLETE`, SM11 `STRATEGY_COMPLETE`) when a final bar-end decision
    has `trigger_met` and every handle at the target (kind (a)); `NEVER` keeps it managed (a trailing or continuing
    strategy whose target keeps moving): at target with the trigger met it stays TRACKING
    (`AT_TARGET_UNCONFIRMED`). PRE and between-bar moves never release either way (D3)."""

    ON_CONFIRMED_TARGET = "ON_CONFIRMED_TARGET"
    NEVER = "NEVER"


# ---------------------------------------------------------------------------------------------------- evaluation
@dataclass(frozen=True)
class Proposal:
    """`Proposal[STOP_LEVEL]` = {stop handle → target} (order-state levels; hidden from `repr` all the same)."""

    targets: tuple[tuple[HandleRef, Decimal], ...] = field(default=(), repr=False)
    resource: Resource = Resource.STOP_LEVEL

    def __post_init__(self) -> None:
        refs = [h for h, _ in self.targets]
        if len(set(refs)) != len(refs):
            raise ValueError("a proposal names each handle once")
        if not all(t.is_finite() for _, t in self.targets):
            raise ValueError("a proposal target is a finite level")

    def target(self, handle: HandleRef) -> Decimal | None:
        for h, t in self.targets:
            if h == handle:
                return t
        return None


@dataclass(frozen=True)
class Evaluation:
    """`propose`'s result: `trigger_met` means "the bar-end condition for the target holds" (R11-m); then a proposal
    (possibly naming no handle when every handle is already at the target) or a KIND-scoped reason from `REASONS`.
    `target` is the level the proposal was built against (an order-state level, optional, only with a proposal), so
    `commit_state` can record it even when the proposal names no handle; hidden from `repr` all the same."""

    trigger_met: bool
    proposal: Proposal | None = None
    reason: str = ""
    metrics: tuple[tuple[str, MetricValue], ...] = ()
    caveats: tuple[str, ...] = ()
    target: Decimal | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (self.proposal is None) == (not self.reason):
            raise ValueError("an evaluation carries a proposal or a reason, not both and not neither")
        if self.target is not None and (self.proposal is None or not self.target.is_finite()):
            raise ValueError("an evaluation target is a finite level and goes with a proposal")
        names = [n for n, _ in self.metrics]
        if len(set(names)) != len(names):
            raise ValueError("a metric is named once")
        if not all(isinstance(v, (RMultiple, Ratio)) for _, v in self.metrics):
            raise ValueError("a decision metric is an RMultiple or a Ratio")

    def metric(self, name: str) -> MetricValue | None:
        for n, v in self.metrics:
            if n == name:
                return v
        return None


# ---------------------------------------------------------------------------------------------------- the Protocol
class SlStrategy(Protocol):
    """Design §B6.1. Frozen per `(KIND, IMPL_VERSION)`; never removed from the registry while pinned."""

    KIND: ClassVar[str]
    IMPL_VERSION: ClassVar[int]
    RESOURCE: ClassVar[Resource]
    DECIMAL_CONTEXT: ClassVar[Context]
    REASONS: ClassVar[Mapping[str, Category]]
    DECISION_METRICS: ClassVar[Mapping[str, type[RMultiple] | type[Ratio]]]
    REQUIRED_GUARDS: ClassVar[frozenset[str]]
    REOPEN_POLICY: ClassVar[ReopenPolicy]
    RELEASE_POLICY: ClassVar[ReleasePolicy]
    """Whether a confirmed bar end at the target releases the position (§B6.3 (7a), S38c)."""
    STATE_SCHEMA: ClassVar[frozenset[str]]
    """The field names `StrategyState` may hold for this version."""

    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        """The strategy's own keys (never `guards`, never an envelope key), normalised; `ParamError` otherwise."""
        ...

    def migrate_state(self, from_impl_version: int, old_state: StrategyState) -> StrategyState | Refuse:
        """Run only by `arm_repin` on the target version, inside its fenced CAS; `Refuse` keeps the old pin."""
        ...

    def triggers(self, params: StrategyParams) -> tuple[BarEvent, ...]: ...

    def trigger_side(self, params: StrategyParams, side: Side) -> QuoteSide:
        """The quote side of the trigger close for a position on `side` (Q5): the engine fills `BarContext.bar` with
        the final BROKER bar of this side at bar-end events and freezes this side's quote at PRE (S21)."""
        ...

    def guards(self, params: StrategyParams) -> tuple[GuardSpec, ...]:
        """The parsed `params.guards`, in declared order (the conformance suite checks this contract)."""
        ...

    def loosen_policy(self, params: StrategyParams) -> LoosenPolicy: ...

    def latch(self, params: StrategyParams) -> bool:
        """True when a trigger recorded on a final bar keeps re-proposing (the advice stands through a HOLD). The
        pipeline (S21) reads it for SM11 §B12 "POST without ADJUST and latch off: withdrawn", as it reads
        `loosen_policy`: the engine never reads a strategy-specific key."""
        ...

    def required_indicators(self, params: StrategyParams) -> tuple[IndicatorNeed, ...]: ...

    def required_history(self, params: StrategyParams) -> tuple[HistoryNeed, ...]: ...

    def arming_requirements(self, params: StrategyParams) -> ArmingRequirements: ...

    def init_state(self, params: StrategyParams, arm_ctx: ArmContext) -> StrategyState: ...

    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                event: ArmEvent) -> Evaluation: ...

    def commit_state(self, params: StrategyParams, state: StrategyState, evaluation: Evaluation,
                     final_outcome: Outcome) -> StrategyState:
        """Final POST / LATE / REOPEN only; records `trigger_met` whatever the post-guards said."""
        ...

    def at_target(self, params: StrategyParams, ctx: BarContext, state: StrategyState) -> bool:
        """Every current handle at or beyond the unrounded target (`σ·(level − target) ≥ 0`; for on-grid levels this
        equals at or beyond the target rounded toward profit, and a handle exactly at an off-grid `E` with offset 0
        counts). Only a bar-end check may release (§B6.3 (7a)); at
        PRE the pipeline (S21) reads it only to drop the item ("at PRE a position with at_target gets no item"), so
        it must be a pure function of the context at PRE too."""
        ...


def metric_problems(owner: str, metrics: Mapping[str, object]) -> list[str]:
    """Problems of a `DECISION_METRICS` declaration: names in form, values `RMultiple` or `Ratio` only."""
    problems: list[str] = []
    for name, kind in metrics.items():
        if not isinstance(name, str) or not PARAM_KEY_RE.fullmatch(name):
            problems.append(f"{owner}: metric name out of form")
        if kind is not RMultiple and kind is not Ratio:
            problems.append(f"{owner}: metric {name} must be RMultiple or Ratio")
    return problems


def sequence_of(value: object) -> Sequence[object] | None:
    """`value` as a list or tuple (a JSON array), else None; text is not a sequence here."""
    return value if isinstance(value, (list, tuple)) else None
