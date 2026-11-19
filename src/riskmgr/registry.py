"""The strategy and guard registry (design §B6.1 Version lifecycle, PR13; plan S19).

Strategies are keyed `(KIND, IMPL_VERSION)`, guards `(KIND, VERSION)`. The v1 registry accepts only strategies whose
`RESOURCE` is `STOP_LEVEL`. A class is never removed: `deprecate_*` only flags it, so pinned positions keep
evaluating while a new pin of it is refused. A guard KIND may not equal a strategy KIND, so the `KIND:VERSION` tokens
of `registry_tokens()` (PR13: the node's health record; `arm config accept` checks membership) are unambiguous.

`parse_params(kind, version, raw)` is the one parser of a strategy row's params (S25 validation, the arm-tracker's
pin, `arm config resolve`): the `guards` list is split off and each entry `{kind, version, params}` is parsed by its
registered guard class; the rest goes to the strategy's own `parse_params`. It refuses a missing
`REQUIRED_GUARDS` kind, a kind listed twice, an unregistered or deprecated `(kind, version)`, out-of-bound params,
unknown keys and the local-only envelope keys. Problems name keys, kinds and versions, never a value. Any other
exception a class's own `parse_params` raises refuses the row as a `ParamError` too, so a buggy class never crashes
validation or pinning. For a new pin it also refuses (`TimingRowError`) a strategy whose `triggers` name a bar spec
with no timing row (§B5.2: v1 has only the 15m row), so S25 validation and arming both refuse it.

`src/armconfig` may import this module only (design §B3: the registries, `parse_params` only), so the names a caller
of `parse_params` needs are exported here as well: `ParamError`, `TimingRowError`, `StrategyParams` and
`ENVELOPE_KEYS` (the local-only keys a config document may never set, plan S25).

`REGISTRY` is the engine's registry: kind (a) `break_even_on_risk_close` v1 and FirstBarGuard `first_bar` v1 (S20).
Every registered class has a case in the registry-wide conformance suite (`tests/riskmgr/conformance.py`).
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal
from typing import Final

from src.marketdata.indicators.registry import REGISTRY as INDICATOR_REGISTRY
from src.marketdata.indicators.registry import IndicatorRegistry
from src.marketdata.timing import timing_problem
from src.riskmgr.envelope import ENVELOPE_KEYS
from src.riskmgr.guard import GUARD_EVENTS, GUARD_KIND_RE, Guard
from src.riskmgr.guards import FIRST_BAR_V1
from src.riskmgr.outcomes import reason_map_problems
from src.riskmgr.strategy import (
    STRATEGY_KIND_RE,
    VERSION_MAX,
    GuardSpec,
    ParamError,
    ReleasePolicy,
    SlStrategy,
    StrategyParams,
    metric_problems,
    sequence_of,
)
from src.riskmgr.strategies import BREAK_EVEN_ON_RISK_CLOSE_V1
from src.riskmgr.types import ArmEvent, Resource

GUARDS_KEY: Final = "guards"
GUARD_ENTRY_KEYS: Final = frozenset({"kind", "version", "params"})
MAX_GUARDS: Final = 8


class RegistryError(ValueError):
    """A class the registry refuses to add (wrong RESOURCE, a duplicate key, malformed declarations)."""


class TimingRowError(ParamError):
    """A new pin of a strategy whose `triggers` name a bar spec with no timing row (design §B5.2)."""


def _version(value: object) -> int | None:
    """An integer version in `[1, VERSION_MAX]` (JSON int or integral Decimal; never a bool or text)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            return None
        value = int(value)
    if not isinstance(value, int) or not 1 <= value <= VERSION_MAX:
        return None
    return value


def _guarded_parse(owner: str, parse: Callable[[], tuple[tuple[str, str], ...]]) -> tuple[tuple[str, str], ...]:
    """A class's own `parse_params`: its `ParamError` passes through; any other exception (a bug in the class on an
    odd document) refuses the row as a `ParamError` naming the owner only, never the exception text or a value."""
    try:
        return parse()
    except ParamError:
        raise
    except Exception:
        raise ParamError(f"{owner}: params could not be parsed") from None


def _check_timing_rows(owner: str, strategy: SlStrategy, params: StrategyParams) -> None:
    """`TimingRowError` when a bar spec the strategy triggers on has no timing row (a raising `triggers` refuses the
    row as a `ParamError`, never crashes validation)."""
    try:
        specs = sorted({ev.bar_spec for ev in strategy.triggers(params)})
    except Exception:
        raise ParamError(f"{owner}: triggers could not be read") from None
    for spec in specs:
        if timing_problem(spec) is not None:
            raise TimingRowError(f"{owner}: bar spec {spec.name} has no timing row")


class ArmRegistry:
    """Strategies keyed `(KIND, IMPL_VERSION)` and guards keyed `(KIND, VERSION)`."""

    def __init__(self, strategies: Iterable[SlStrategy] = (), guards: Iterable[Guard] = (),
                 indicators: IndicatorRegistry = INDICATOR_REGISTRY) -> None:
        self._strategies: dict[tuple[str, int], SlStrategy] = {}
        self._guards: dict[tuple[str, int], Guard] = {}
        self._deprecated_strategies: set[tuple[str, int]] = set()
        self._deprecated_guards: set[tuple[str, int]] = set()
        self._indicators = indicators
        for g in guards:
            self.add_guard(g)
        for s in strategies:
            self.add_strategy(s)

    # ------------------------------------------------------------------------------------------------ add
    def add_strategy(self, strategy: SlStrategy) -> None:
        key = (strategy.KIND, strategy.IMPL_VERSION)
        if strategy.RESOURCE is not Resource.STOP_LEVEL:
            raise RegistryError(f"strategy {key[0]} v{key[1]}: the v1 registry accepts only RESOURCE STOP_LEVEL")
        if not isinstance(strategy.KIND, str) or not STRATEGY_KIND_RE.fullmatch(strategy.KIND):
            raise RegistryError(f"strategy KIND must match {STRATEGY_KIND_RE.pattern}")
        if _version(strategy.IMPL_VERSION) is None:
            raise RegistryError(f"strategy {strategy.KIND}: IMPL_VERSION must be 1-{VERSION_MAX}")
        if key in self._strategies:
            raise RegistryError(f"strategy {key[0]} v{key[1]} is already registered")
        if any(k == strategy.KIND for k, _ in self._guards):
            raise RegistryError(f"strategy KIND {strategy.KIND} is a guard KIND")
        owner = f"strategy {key[0]} v{key[1]}"
        if not isinstance(getattr(strategy, "RELEASE_POLICY", None), ReleasePolicy):
            raise RegistryError(f"{owner}: RELEASE_POLICY must be a ReleasePolicy (S38c)")
        problems = reason_map_problems(owner, strategy.REASONS) + metric_problems(owner, strategy.DECISION_METRICS)
        if problems:
            raise RegistryError("; ".join(problems))
        self._strategies[key] = strategy

    def add_guard(self, guard: Guard) -> None:
        key = (guard.KIND, guard.VERSION)
        if not isinstance(guard.KIND, str) or not GUARD_KIND_RE.fullmatch(guard.KIND):
            raise RegistryError(f"guard KIND must match {GUARD_KIND_RE.pattern}")
        if _version(guard.VERSION) is None:
            raise RegistryError(f"guard {guard.KIND}: VERSION must be 1-{VERSION_MAX}")
        if key in self._guards:
            raise RegistryError(f"guard {key[0]} v{key[1]} is already registered")
        if any(k == guard.KIND for k, _ in self._strategies):
            raise RegistryError(f"guard KIND {guard.KIND} is a strategy KIND")
        if ArmEvent.POST not in guard.EVENTS or not guard.EVENTS <= GUARD_EVENTS:
            raise RegistryError(f"guard {key[0]} v{key[1]}: EVENTS must include POST and hold only PRE / POST")
        problems = reason_map_problems(f"guard {key[0]} v{key[1]}", guard.REASONS)
        if problems:
            raise RegistryError("; ".join(problems))
        self._guards[key] = guard

    def deprecate_strategy(self, kind: str, impl_version: int) -> None:
        if (kind, impl_version) not in self._strategies:
            raise KeyError(f"{kind} v{impl_version}")
        self._deprecated_strategies.add((kind, impl_version))

    def deprecate_guard(self, kind: str, version: int) -> None:
        if (kind, version) not in self._guards:
            raise KeyError(f"{kind} v{version}")
        self._deprecated_guards.add((kind, version))

    # ------------------------------------------------------------------------------------------------ lookup
    def strategy_keys(self) -> tuple[tuple[str, int], ...]:
        return tuple(sorted(self._strategies))

    def guard_keys(self) -> tuple[tuple[str, int], ...]:
        return tuple(sorted(self._guards))

    def strategy(self, kind: str, impl_version: int) -> SlStrategy | None:
        """A registered strategy, deprecated or not (pinned positions keep evaluating)."""
        return self._strategies.get((kind, impl_version))

    def guard(self, kind: str, version: int) -> Guard | None:
        return self._guards.get((kind, version))

    def strategy_deprecated(self, kind: str, impl_version: int) -> bool:
        return (kind, impl_version) in self._deprecated_strategies

    def guard_deprecated(self, kind: str, version: int) -> bool:
        return (kind, version) in self._deprecated_guards

    def indicator_known(self, kind: str, version: int) -> bool:
        """The indicator class a `required_indicators` need names is loadable on this node."""
        return self._indicators.get(kind, version) is not None

    def registry_tokens(self) -> tuple[str, ...]:
        """The sorted `KIND:VERSION` tokens of every strategy, guard and indicator this node can load (PR13)."""
        keys = [*self._strategies, *self._guards, *self._indicators.keys()]
        return tuple(sorted(f"{k}:{v}" for k, v in keys))

    # ------------------------------------------------------------------------------------------------ parse
    def parse_params(self, kind: str, impl_version: int, raw: Mapping[str, object], *,
                     new_pin: bool = True) -> StrategyParams:
        """The pinned params of a strategy row, or `ParamError`. With `new_pin` (validation, arming) a deprecated
        strategy or guard version is refused; re-parsing an existing pin passes `new_pin=False`."""
        strategy = self._strategies.get((kind, impl_version))
        if strategy is None:
            raise ParamError(f"unknown strategy {kind[:40]!r} v{impl_version}")
        owner = f"strategy {kind} v{impl_version}"
        if new_pin and (kind, impl_version) in self._deprecated_strategies:
            raise ParamError(f"{owner} is deprecated")
        if not isinstance(raw, Mapping):
            raise ParamError(f"{owner}: params must be an object")
        for key in sorted(k for k in raw if isinstance(k, str)):
            if key in ENVELOPE_KEYS:
                raise ParamError(f"{owner}: {key} is a local engine setting, never a strategy parameter")
        own = {k: v for k, v in raw.items() if k != GUARDS_KEY}
        guards = self._parse_guards(owner, raw.get(GUARDS_KEY, []), new_pin=new_pin)
        values = _guarded_parse(owner, lambda: strategy.parse_params(own))
        kinds = [g.kind for g in guards]
        missing = sorted(set(strategy.REQUIRED_GUARDS) - set(kinds))
        if missing:
            raise ParamError(f"{owner}: guards must list {', '.join(missing)}")
        params = StrategyParams(kind=kind, impl_version=impl_version, values=values, guards=guards)
        if new_pin:
            _check_timing_rows(owner, strategy, params)
        return params

    def _parse_guards(self, owner: str, raw: object, *, new_pin: bool) -> tuple[GuardSpec, ...]:
        entries = sequence_of(raw)
        if entries is None:
            raise ParamError(f"{owner}: guards must be a list of {{kind, version, params}}")
        if len(entries) > MAX_GUARDS:
            raise ParamError(f"{owner}: at most {MAX_GUARDS} guards")
        out: list[GuardSpec] = []
        seen: set[str] = set()
        for n, entry in enumerate(entries):
            where = f"{owner}: guards[{n}]"
            if not isinstance(entry, Mapping) or set(entry) != GUARD_ENTRY_KEYS:
                raise ParamError(f"{where} must hold exactly kind, version and params")
            g_kind, g_version, g_params = entry["kind"], _version(entry["version"]), entry["params"]
            if not isinstance(g_kind, str) or not GUARD_KIND_RE.fullmatch(g_kind) or g_version is None:
                raise ParamError(f"{where}: kind or version out of form")
            if g_kind in seen:
                raise ParamError(f"{where}: guard {g_kind} is listed twice")
            seen.add(g_kind)
            guard = self._guards.get((g_kind, g_version))
            if guard is None:
                raise ParamError(f"{where}: guard {g_kind} v{g_version} is not registered")
            if new_pin and (g_kind, g_version) in self._deprecated_guards:
                raise ParamError(f"{where}: guard {g_kind} v{g_version} is deprecated")
            if not isinstance(g_params, Mapping):
                raise ParamError(f"{where}: params must be an object")
            parse = guard.parse_params
            out.append(GuardSpec(g_kind, g_version,
                                 _guarded_parse(f"{where}: guard {g_kind} v{g_version}", lambda: parse(g_params))))
        return tuple(out)


REGISTRY: Final = ArmRegistry(strategies=[BREAK_EVEN_ON_RISK_CLOSE_V1], guards=[FIRST_BAR_V1])
"""The engine's registry: kind (a) and FirstBarGuard (S20)."""

__all__ = ["ArmRegistry", "ENVELOPE_KEYS", "GUARDS_KEY", "ParamError", "REGISTRY", "RegistryError", "StrategyParams",
           "TimingRowError"]
