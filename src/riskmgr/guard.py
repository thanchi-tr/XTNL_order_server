"""The guard contract (design §B6.1 Guard contract; plan S19).

A guard is a pinned, versioned check the engine runs between the pre-guards and `propose`, in the order the strategy
row lists it, and only on an event in its `EVENTS` (which always include POST). On another event where `applies()`
holds (PRE), it is not evaluated: `propose` still runs and the item carries the guard's `conditional_label`.

`evaluate(params, ctx)` → `GuardResult`: `PASS`, `HOLD(reason)`, `PENDING(until_ms)` or `UNKNOWN(reason)`. The engine
maps `PENDING(until)` to `DATA_UNAVAILABLE(guard_pending)` up to `until` (never later than `decision_cap`), then to
`unknown_policy(params)`: `final_verdict` is that rule (plan S20), using the guard's `on_timeout(params)` for a
PENDING whose window has passed. A guard reason is KIND-scoped, stored with the guard's KIND and categorised by its
`REASONS`. Guards are pure, like strategies, and a fault inside one is attributed to the guard's own
`(KIND, VERSION)` (scope id `guard:<KIND>:<VERSION>`, S22). Params are passed explicitly, as for strategies.

`POST` in `EVENTS` stands for every bar-end decision (`BAR_END_EVENTS`: POST and its LATE, REOPEN and CORRECTED
re-runs), which all decide on the broker's final bar; only `PRE` is outside a POST-only guard's events.

`known_before_bar_end(params, ctx)` is a verdict that is already certain before the bar end without reading any
market data (FirstBarGuard: `PRE_ANCHOR` on the entry bucket under `next_full_bucket`, §B6.5): at PRE the engine shows
it instead of the conditional label. It is not a run of the check.

A verdict may carry typed metrics (`RMultiple` / `Ratio`, e.g. the first-bar ratio quantised for the notice, §B8) and
caveat codes (`borderline`, `roll_affected`). Codes only, never a price.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar, Final, Protocol

from src.riskmgr.outcomes import CODE_RE, REASON_RE, Category
from src.riskmgr.strategy import PARAM_KEY_RE, GuardSpec, HistoryNeed, IndicatorNeed, StrategyState
from src.riskmgr.types import ArmEvent, BarContext, MetricValue, RMultiple, Ratio

GUARD_KIND_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")
GUARD_EVENTS: Final = frozenset({ArmEvent.PRE, ArmEvent.POST})


class GuardStatus(str, Enum):
    PASS = "PASS"
    HOLD = "HOLD"
    PENDING = "PENDING"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class GuardResult:
    """One verdict. `HOLD` and `UNKNOWN` carry a reason code from the guard's `REASONS` and optionally a `detail`
    code naming the sub-reason (`FIRST_BAR_UNKNOWN(atr_degenerate | timeout | stale)`, §B6.5, §B6.6); `PENDING`
    carries `until_ms`. `metrics` (typed `RMultiple` / `Ratio`) and `caveats` (engine-form codes) may ride on any
    verdict but PENDING. Build with the class methods."""

    status: GuardStatus
    reason: str = ""
    until_ms: int | None = None
    detail: str = ""
    metrics: tuple[tuple[str, MetricValue], ...] = ()
    caveats: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        needs_reason = self.status in (GuardStatus.HOLD, GuardStatus.UNKNOWN)
        if needs_reason != bool(self.reason) or (self.reason and not REASON_RE.fullmatch(self.reason)):
            raise ValueError("HOLD and UNKNOWN carry a reason code; PASS and PENDING carry none")
        if self.detail and (not needs_reason or not CODE_RE.fullmatch(self.detail)):
            raise ValueError("a detail is an engine-form code on a HOLD or UNKNOWN only")
        if (self.status is GuardStatus.PENDING) != (self.until_ms is not None):
            raise ValueError("PENDING, and only PENDING, carries until_ms")
        if self.status is GuardStatus.PENDING and (self.metrics or self.caveats):
            raise ValueError("PENDING carries no metric and no caveat")
        names = [n for n, _ in self.metrics]
        if len(set(names)) != len(names) or not all(isinstance(n, str) and PARAM_KEY_RE.fullmatch(n) for n in names):
            raise ValueError("a guard metric is named once, in code form")
        if not all(isinstance(v, (RMultiple, Ratio)) for _, v in self.metrics):
            raise ValueError("a guard metric is an RMultiple or a Ratio")
        if not all(isinstance(c, str) and CODE_RE.fullmatch(c) for c in self.caveats):
            raise ValueError("a caveat is an engine-form code")

    @classmethod
    def passed(cls, *, metrics: tuple[tuple[str, MetricValue], ...] = (),
               caveats: tuple[str, ...] = ()) -> GuardResult:
        return cls(GuardStatus.PASS, metrics=metrics, caveats=caveats)

    @classmethod
    def hold(cls, reason: str, detail: str = "", *, metrics: tuple[tuple[str, MetricValue], ...] = (),
             caveats: tuple[str, ...] = ()) -> GuardResult:
        return cls(GuardStatus.HOLD, reason, detail=detail, metrics=metrics, caveats=caveats)

    @classmethod
    def pending(cls, until_ms: int) -> GuardResult:
        return cls(GuardStatus.PENDING, until_ms=until_ms)

    @classmethod
    def unknown(cls, reason: str, detail: str = "", *, metrics: tuple[tuple[str, MetricValue], ...] = (),
                caveats: tuple[str, ...] = ()) -> GuardResult:
        return cls(GuardStatus.UNKNOWN, reason, detail=detail, metrics=metrics, caveats=caveats)

    def metric(self, name: str) -> MetricValue | None:
        for n, v in self.metrics:
            if n == name:
                return v
        return None


class UnknownPolicy(str, Enum):
    """What an UNKNOWN (or a PENDING past its window) becomes: hold this bar, or skip the test (Q3)."""

    HOLD_BAR = "HOLD_BAR"
    SKIP = "SKIP"


class Guard(Protocol):
    """Design §B6.1. Keyed `(KIND, VERSION)`; never removed while pinned."""

    KIND: ClassVar[str]
    VERSION: ClassVar[int]
    EVENTS: ClassVar[frozenset[ArmEvent]]
    REASONS: ClassVar[Mapping[str, Category]]

    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        """One entry's params, normalised; `ParamError` for an unknown key or an out-of-bound value."""
        ...

    def required_indicators(self, params: GuardSpec) -> tuple[IndicatorNeed, ...]: ...

    def required_history(self, params: GuardSpec) -> tuple[HistoryNeed, ...]: ...

    def applies(self, params: GuardSpec, ctx: BarContext, state: StrategyState) -> bool: ...

    def evaluate(self, params: GuardSpec, ctx: BarContext) -> GuardResult: ...

    def known_before_bar_end(self, params: GuardSpec, ctx: BarContext) -> GuardResult | None:
        """A HOLD already certain before the bar end from order state and the clock alone (no market data), shown
        at PRE instead of the conditional label; None otherwise."""
        ...

    def on_timeout(self, params: GuardSpec) -> GuardResult:
        """What a PENDING becomes once its window has passed: an UNKNOWN (or HOLD) with a reason from `REASONS`
        (FirstBarGuard: `FIRST_BAR_UNKNOWN(timeout)`), before `unknown_policy` applies."""
        ...

    def unknown_policy(self, params: GuardSpec) -> UnknownPolicy: ...

    def conditional_label(self, params: GuardSpec) -> str:
        """The caveat on an applicable event outside `EVENTS` (FirstBarGuard: bar 0's PRE)."""
        ...


def skipped_caveat(kind: str) -> str:
    """The caveat a verdict carries when `unknown_policy` SKIP turned an UNKNOWN into a PASS (`first_bar_skipped`)."""
    return f"{kind}_skipped"


def final_verdict(guard: Guard, params: GuardSpec, result: GuardResult, now_ms: int) -> GuardResult:
    """The guard verdict the pipeline acts on at `now_ms` (design §B6.1 Guard contract, §B6.5; Q3):

    * `PENDING(until)` before `until` stays PENDING (the engine records `DATA_UNAVAILABLE`, e.g. `first_bar_pending`,
      and decides again later); at or after `until` it becomes `guard.on_timeout(params)`;
    * an `UNKNOWN` (given or from the timeout) under `HOLD_BAR` is a `HOLD` with the same reason, detail, metrics and
      caveats (hold this bar, and say so); under `SKIP` it is a `PASS` carrying the `<kind>_skipped` caveat (the test
      is skipped, never silently);
    * `PASS` and `HOLD` are returned unchanged.

    Pure: `now_ms` is the decision time on the local clock (the same clock as `until`)."""
    if result.status is GuardStatus.PENDING:
        if result.until_ms is not None and now_ms < result.until_ms:
            return result
        result = guard.on_timeout(params)
    if result.status is not GuardStatus.UNKNOWN:
        return result
    if guard.unknown_policy(params) is UnknownPolicy.SKIP:
        return GuardResult.passed(metrics=result.metrics,
                                  caveats=(*result.caveats, skipped_caveat(guard.KIND)))
    return GuardResult.hold(result.reason, result.detail, metrics=result.metrics, caveats=result.caveats)


def guard_scope_id(kind: str, version: int) -> str:
    """The STRATEGY_VERSION scope id a guard fault is attributed to (§B4 Job runner, §B14)."""
    return f"guard:{kind}:{version}"


def strategy_scope_id(kind: str, impl_version: int) -> str:
    return f"strategy:{kind}:{impl_version}"
