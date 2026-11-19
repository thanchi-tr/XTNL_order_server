"""The indicator registry, keyed `(KIND, VERSION)` (design §A6; plan S18).

A class is never removed while a spec pins it (`deprecate` only flags it: new specs are refused, existing ones keep
computing). `indicator_id = sha256(spec canonical)[:16]` (`IndicatorSpec.indicator_id`).

`plan_specs` is the registration check of §A6: every class known, every `IndicatorRef` naming a registered spec and
one of its outputs, no cycle (the result is the topological recompute order), and every input's window (and the
primary series' backfill) inside that input's retention (§A5: TICK 1m 7 days, TICK 5m / 15m / 1h 90 days, BROKER
1 year). Problems name ids and codes only.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

from src.marketdata.indicators.atr import ATR_V1
from src.marketdata.indicators.base import Anchored, Count, Indicator, IndicatorRef, IndicatorSpec, SeriesRef
from src.marketdata.types import M1, SeriesId, Source

DAY_MS: Final = 86_400_000
TICK_1M_RETENTION_MS: Final = 7 * DAY_MS
TICK_ROLLUP_RETENTION_MS: Final = 90 * DAY_MS
BROKER_RETENTION_MS: Final = 365 * DAY_MS
"""Design §A5 (the same figures as `md-writer`'s retention and `md-bars`' history reach)."""


def series_retention_ms(series: SeriesId) -> int:
    if series.source is Source.BROKER:
        return BROKER_RETENTION_MS
    return TICK_1M_RETENTION_MS if series.spec == M1 else TICK_ROLLUP_RETENTION_MS


class IndicatorRegistry:
    """Indicator classes keyed `(KIND, VERSION)`."""

    def __init__(self, classes: Iterable[Indicator] = ()) -> None:
        self._classes: dict[tuple[str, int], Indicator] = {}
        self._deprecated: set[tuple[str, int]] = set()
        for c in classes:
            self.add(c)

    def add(self, indicator: Indicator) -> None:
        key = (indicator.KIND, indicator.VERSION)
        if key in self._classes:
            raise ValueError(f"indicator {key[0]} v{key[1]} is already registered")
        if not indicator.OUTPUTS or len({o.name for o in indicator.OUTPUTS}) != len(indicator.OUTPUTS):
            raise ValueError(f"indicator {key[0]} v{key[1]} needs distinct outputs")
        self._classes[key] = indicator

    def deprecate(self, kind: str, version: int) -> None:
        if (kind, version) not in self._classes:
            raise KeyError(f"{kind} v{version}")
        self._deprecated.add((kind, version))

    def keys(self) -> tuple[tuple[str, int], ...]:
        return tuple(sorted(self._classes))

    def get(self, kind: str, version: int) -> Indicator | None:
        return self._classes.get((kind, version))

    def of(self, spec: IndicatorSpec) -> Indicator:
        ind = self._classes.get((spec.kind, spec.version))
        if ind is None:
            raise LookupError(f"unknown indicator {spec.kind} v{spec.version}")
        return ind

    def spec(self, kind: str, version: int, series: SeriesId, raw_params: Mapping[str, str],
             price_increment: Decimal) -> IndicatorSpec:
        """A new spec: the class must be registered and not deprecated; params checked by the class
        (`ParamError`)."""
        ind = self._classes.get((kind, version))
        if ind is None:
            raise LookupError(f"unknown indicator {kind} v{version}")
        if (kind, version) in self._deprecated:
            raise LookupError(f"indicator {kind} v{version} is deprecated")
        return IndicatorSpec(kind=kind, version=version, series=series, params=ind.parse_params(raw_params),
                             price_increment=price_increment)


@dataclass(frozen=True)
class SpecPlan:
    order: tuple[str, ...]
    """Indicator ids in recompute order (every input before its dependants)."""


def plan_specs(registry: IndicatorRegistry, specs: Mapping[str, IndicatorSpec]) -> SpecPlan | list[str]:
    """The registration check: classes, references, cycles, retention. A plan, or the list of problems."""
    problems: list[str] = []
    deps: dict[str, set[str]] = {}
    for iid, spec in specs.items():
        if spec.indicator_id != iid:
            problems.append(f"{iid}: id does not match its spec")
            continue
        try:
            ind = registry.of(spec)
        except LookupError:
            problems.append(f"{iid}: unknown class {spec.kind} v{spec.version}")
            continue
        clock = ind.clock(spec)
        if ind.backfill_bars(spec) * clock.timeframe_ms > series_retention_ms(spec.series):
            problems.append(f"{iid}: backfill window exceeds the retention of {spec.series.canonical}")
        deps[iid] = set()
        windows = ind.window(spec)
        for ref in ind.inputs(spec):
            w = windows.get(ref)
            if w is None:
                problems.append(f"{iid}: an input has no window")
                continue
            if isinstance(ref, IndicatorRef):
                target = specs.get(ref.indicator_id)
                if target is None:
                    problems.append(f"{iid}: input {ref.indicator_id} is not registered")
                    continue
                target_ind = registry.get(target.kind, target.version)
                if target_ind is not None and ref.output not in {o.name for o in target_ind.OUTPUTS}:
                    problems.append(f"{iid}: input {ref.indicator_id} has no output {ref.output}")
                deps[iid].add(ref.indicator_id)
                if target_ind is not None:
                    span_ms = _bars(w) * target_ind.clock(target).timeframe_ms
                    if span_ms > series_retention_ms(target.series):
                        problems.append(f"{iid}: window of input {ref.indicator_id} exceeds its retention")
            elif isinstance(ref, SeriesRef):
                if _bars(w) * ref.series.spec.timeframe_ms > series_retention_ms(ref.series):
                    problems.append(f"{iid}: window of input {ref.series.canonical} exceeds its retention")
    if problems:
        return problems
    order: list[str] = []
    remaining = {k: set(v) for k, v in deps.items()}
    while remaining:
        ready = sorted(k for k, v in remaining.items() if not v)
        if not ready:
            return [f"{iid}: dependency cycle" for iid in sorted(remaining)]
        for k in ready:
            order.append(k)
            del remaining[k]
        for v in remaining.values():
            v.difference_update(ready)
    return SpecPlan(tuple(order))


def input_series(registry: IndicatorRegistry, lookup: Callable[[str], IndicatorSpec | None],
                 indicator_id: str) -> tuple[SeriesId, ...] | None:
    """Every bar series an indicator reads, through its `IndicatorRef` inputs too (the decision gate of a value read,
    §A10). None when a spec or its class cannot be resolved (the read fails closed)."""
    out: list[SeriesId] = []
    seen: set[str] = set()
    todo = [indicator_id]
    while todo:
        iid = todo.pop()
        if iid in seen:
            continue
        seen.add(iid)
        spec = lookup(iid)
        ind = None if spec is None or spec.indicator_id != iid else registry.get(spec.kind, spec.version)
        if spec is None or ind is None:
            return None
        for ref in ind.inputs(spec):
            if isinstance(ref, SeriesRef):
                if ref.series not in out:
                    out.append(ref.series)
            else:
                todo.append(ref.indicator_id)
    return tuple(out)


def _bars(w: Count | Anchored) -> int:
    return w.n if isinstance(w, Count) else w.max_bars


REGISTRY: Final = IndicatorRegistry((ATR_V1,))
"""The engine's registry: v1 ships ATR. SMA, EMA, RSI, Bollinger, ... are each one class plus golden vectors."""

__all__ = ["IndicatorRegistry", "REGISTRY", "SpecPlan", "plan_specs", "series_retention_ms"]
