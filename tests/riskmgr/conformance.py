"""The registry-wide conformance suite (design §C2 Rules "adding a strategy", §B6.1; plan S19, R11-m).

Run over **every** registered strategy `(KIND, IMPL_VERSION)` and guard `(KIND, VERSION)`; a registered class without a
conformance case is itself a problem, so a new version cannot land without one. The suite checks:

* **pure and deterministic under replay:** each case's evaluations give equal results run forwards, run backwards
  (no state carried between calls) and run under a hostile ambient Decimal context (the class computes under its own
  pinned `DECIMAL_CONTEXT`); the case's inputs (params, contexts, states) compare equal to a deep copy taken before
  the run;
* **every REASON mapped** (`INFO` / `HOLD`, codes in form, no clash with an engine outcome) and every reason an
  evaluation or a guard verdict returns is in `REASONS`;
* **DECISION_METRICS typed** (`RMultiple` / `Ratio` only) and every returned metric is declared with its type;
* **`parse_params`** accepts the case's row, normalises it deterministically, keeps the guard list in declared order
  (`guards(params)` returns it), and rejects an unknown key, every local-only envelope key, each out-of-bound row of
  the case, a missing `REQUIRED_GUARDS` kind and a guard listed twice, with a `ParamError` that never repeats a value
  (neither the planted unknown-key value nor any value in which an out-of-bound row differs from the valid row);
* **requirements:** `required_indicators` gives registered `IndicatorNeed`s and `required_history` `HistoryNeed`s
  (strategies and guards alike);
* **`migrate_state`** round-trips the version's own state and, from every older registered version of the KIND,
  returns a state of the schema or `Refuse`;
* **`trigger_met`** means "the bar-end condition for the target holds": it equals what each case states for its
  context and a proposal names only the context's current handles; the suite calls `at_target` / `commit_state` only
  on bar-end events (as the engine does) and checks their types there;
* `trigger_side` gives a quote side for a long and a short; `RELEASE_POLICY` is a `ReleasePolicy` (S38c);
* guards: `EVENTS` hold POST, `HOLD` / `UNKNOWN` reasons are in `REASONS`, `unknown_policy` is typed, a guard not
  run at PRE has a `conditional_label`, `on_timeout` is an UNKNOWN or HOLD with a reason in `REASONS`, and
  `known_before_bar_end` is None or a HOLD with a reason in `REASONS` (S20).

`STRATEGY_CASES` / `GUARD_CASES` hold the engine's classes (S20: kind (a) `break_even_on_risk_close` v1 and
FirstBarGuard `first_bar` v1, built from `tests/riskmgr/kind_a.py`).
"""
from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Context, localcontext
from typing import Final, TypeVar

from src.dto import Side
from src.marketdata.ports import HandleRef
from src.marketdata.timing import TIMING_TABLE
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.envelope import ENVELOPE_KEYS
from src.riskmgr.guard import Guard, GuardResult, GuardStatus, UnknownPolicy
from src.riskmgr.outcomes import Outcome, reason_map_problems
from src.riskmgr.registry import GUARDS_KEY, ArmRegistry
from src.riskmgr.strategy import (
    ArmingRequirements,
    Evaluation,
    GuardSpec,
    HistoryNeed,
    IndicatorNeed,
    LoosenPolicy,
    ParamError,
    Refuse,
    ReleasePolicy,
    ReopenPolicy,
    SlStrategy,
    StrategyParams,
    StrategyState,
    metric_problems,
)
from src.riskmgr.types import BAR_END_EVENTS, ArmContext, ArmEvent, BarContext, BarEvent, Resource
from tests.riskmgr import kind_a

PLANTED_VALUE: Final = "V4LUE-731-planted"
HOSTILE: Final = Context(prec=3, rounding=ROUND_FLOOR)

R = TypeVar("R")


@dataclass(frozen=True)
class EvalCase:
    ctx: BarContext
    state: StrategyState
    event: ArmEvent
    trigger_met: bool
    """The case author's statement: does the bar-end condition for the target hold in this context?"""


@dataclass(frozen=True)
class StrategyCase:
    params: Mapping[str, object]
    """A valid strategy row's params (its own keys plus `guards`)."""
    invalid_params: tuple[Mapping[str, object], ...]
    """Rows with an out-of-bound value; each must be refused."""
    arm_ctx: ArmContext
    evaluations: tuple[EvalCase, ...]
    old_states: tuple[tuple[int, StrategyState], ...] = ()
    """`(from_impl_version, state)` for `migrate_state`: at least one per older registered version of the KIND."""
    break_even_target: bool = False
    """The kind advises break-even or beyond (kind (a)): at every bar-end evaluation, REOPEN included, the target and
    every proposed level are at or beyond `ctx.entry` (never on the loss side), and `at_target` never holds while a
    handle is on the loss side of `ctx.entry` (a release is break-even confirmed; live review)."""


@dataclass(frozen=True)
class GuardCase:
    params: Mapping[str, object]
    invalid_params: tuple[Mapping[str, object], ...]
    contexts: tuple[tuple[BarContext, StrategyState], ...]


STRATEGY_CASES: dict[tuple[str, int], StrategyCase] = {}
GUARD_CASES: dict[tuple[str, int], GuardCase] = {}


class _Raised(Exception):
    pass


def _call(problems: list[str], label: str, fn: Callable[[], R]) -> R:
    try:
        return fn()
    except Exception as e:   # a raise is a problem of the class under test, never a crash of the suite
        problems.append(f"{label} raised {type(e).__name__}")
        raise _Raised from e


def _leaves(value: object, path: tuple[str, ...] = ()) -> dict[tuple[str, ...], str]:
    """The scalar leaves of a params document, keyed by their path, as text."""
    if isinstance(value, Mapping):
        out: dict[tuple[str, ...], str] = {}
        for k, v in value.items():
            out.update(_leaves(v, (*path, str(k))))
        return out
    if isinstance(value, (list, tuple)):
        out = {}
        for n, v in enumerate(value):
            out.update(_leaves(v, (*path, str(n))))
        return out
    return {path: str(value)}


_NAMES: Final = frozenset({"kind", "version"})
"""Leaves a ParamError may name (a guard entry's kind and version name the class, not a value)."""


def _changed_values(valid: Mapping[str, object], bad: Mapping[str, object]) -> tuple[str, ...]:
    """The text of each leaf of `bad` that the valid row does not hold (the out-of-bound values), at least two
    characters long so a digit or a letter of the bound text is never mistaken for an echo."""
    before = _leaves(valid)
    return tuple(sorted({text for path, text in _leaves(bad).items()
                         if before.get(path) != text and (not path or path[-1] not in _NAMES)
                         and len(text.strip()) >= 2}))


def _echoes(message: str, values: tuple[str, ...]) -> bool:
    for v in (PLANTED_VALUE, *values):
        if re.search(r"(?<![A-Za-z0-9_.])" + re.escape(v) + r"(?![A-Za-z0-9_]|\.[0-9])", message):
            return True
    return False


def _refused(problems: list[str], label: str, fn: Callable[[], object], values: tuple[str, ...] = ()) -> None:
    """`fn` must raise a `ParamError` that repeats neither `PLANTED_VALUE` nor any of `values`."""
    try:
        fn()
    except ParamError as e:
        if _echoes(str(e), values):
            problems.append(f"{label}: the ParamError repeats a value")
        return
    except Exception as e:
        problems.append(f"{label}: raised {type(e).__name__}, not ParamError")
        return
    problems.append(f"{label}: accepted")


# ---------------------------------------------------------------------------------------------------- strategies
def check_strategy(registry: ArmRegistry, strategy: SlStrategy, case: StrategyCase) -> list[str]:
    owner = f"strategy {strategy.KIND} v{strategy.IMPL_VERSION}"
    problems: list[str] = []
    if strategy.RESOURCE is not Resource.STOP_LEVEL:
        problems.append(f"{owner}: RESOURCE is not STOP_LEVEL")
    if not isinstance(strategy.DECIMAL_CONTEXT, Context):
        problems.append(f"{owner}: DECIMAL_CONTEXT is not a decimal Context")
    if not isinstance(strategy.REOPEN_POLICY, ReopenPolicy):
        problems.append(f"{owner}: REOPEN_POLICY is not a ReopenPolicy")
    if not isinstance(getattr(strategy, "RELEASE_POLICY", None), ReleasePolicy):
        problems.append(f"{owner}: RELEASE_POLICY is not a ReleasePolicy")
    problems += reason_map_problems(owner, strategy.REASONS)
    problems += metric_problems(owner, strategy.DECISION_METRICS)
    for kind in sorted(strategy.REQUIRED_GUARDS):
        if not any(k == kind for k, _ in registry.guard_keys()):
            problems.append(f"{owner}: required guard {kind} is not registered")
    before = deepcopy(case)
    try:
        params = _params(problems, owner, registry, strategy, case)
        _declarations(problems, owner, registry, strategy, params)
        _evaluations(problems, owner, strategy, params, case)
        _migrations(problems, owner, registry, strategy, params, case)
    except _Raised:
        pass
    if case != before:
        problems.append(f"{owner}: the case's inputs changed (params, contexts or states mutated)")
    return problems


def _parse(registry: ArmRegistry, strategy: SlStrategy, raw: Mapping[str, object]) -> StrategyParams:
    return registry.parse_params(strategy.KIND, strategy.IMPL_VERSION, raw)


def _params(problems: list[str], owner: str, registry: ArmRegistry, strategy: SlStrategy,
            case: StrategyCase) -> StrategyParams:
    params = _call(problems, f"{owner}: parse_params", lambda: _parse(registry, strategy, case.params))
    again = _call(problems, f"{owner}: parse_params", lambda: _parse(registry, strategy, dict(case.params)))
    if params != again or params.params_hash != again.params_hash:
        problems.append(f"{owner}: parse_params is not deterministic")
    if tuple(strategy.guards(params)) != params.guards:
        problems.append(f"{owner}: guards(params) is not the parsed guard list in declared order")
    _refused(problems, f"{owner}: unknown key",
             lambda: _parse(registry, strategy, {**case.params, "zz_unknown_key": PLANTED_VALUE}))
    for key in sorted(ENVELOPE_KEYS):
        planted = {**case.params, key: "0"}
        _refused(problems, f"{owner}: envelope key {key}", lambda: _parse(registry, strategy, planted))
    for n, bad in enumerate(case.invalid_params):
        _refused(problems, f"{owner}: invalid row {n}", lambda: _parse(registry, strategy, bad),
                 _changed_values(case.params, bad))
    guards = case.params.get(GUARDS_KEY)
    if strategy.REQUIRED_GUARDS:
        _refused(problems, f"{owner}: no guards",
                 lambda: _parse(registry, strategy, {k: v for k, v in case.params.items() if k != GUARDS_KEY}))
        if isinstance(guards, list) and guards:
            _refused(problems, f"{owner}: a guard listed twice",
                     lambda: _parse(registry, strategy, {**case.params, GUARDS_KEY: [*guards, guards[0]]}))
    return params


def _declarations(problems: list[str], owner: str, registry: ArmRegistry, strategy: SlStrategy,
                  params: StrategyParams) -> None:
    triggers = _call(problems, f"{owner}: triggers", lambda: strategy.triggers(params))
    if not triggers or not all(isinstance(t, BarEvent) for t in triggers):
        problems.append(f"{owner}: triggers must be a non-empty tuple of BarEvent")
    else:
        if not any(t.event is ArmEvent.POST for t in triggers):
            problems.append(f"{owner}: triggers must include POST (only a bar end decides)")
        if any(t.bar_spec not in TIMING_TABLE for t in triggers):
            problems.append(f"{owner}: a trigger's bar_spec has no timing row")
    if not isinstance(_call(problems, f"{owner}: loosen_policy", lambda: strategy.loosen_policy(params)),
                      LoosenPolicy):
        problems.append(f"{owner}: loosen_policy is not a LoosenPolicy")
    if not isinstance(_call(problems, f"{owner}: latch", lambda: strategy.latch(params)), bool):
        problems.append(f"{owner}: latch is not a bool")
    if not isinstance(_call(problems, f"{owner}: arming_requirements",
                            lambda: strategy.arming_requirements(params)), ArmingRequirements):
        problems.append(f"{owner}: arming_requirements is not ArmingRequirements")
    sides: tuple[Side, ...] = ("BUY", "SELL")
    for side in sides:
        if not isinstance(_call(problems, f"{owner}: trigger_side", lambda: strategy.trigger_side(params, side)),
                          QuoteSide):
            problems.append(f"{owner}: trigger_side is not a quote side")
    _needs(problems, owner, registry, lambda: strategy.required_indicators(params),
           lambda: strategy.required_history(params))


def _needs(problems: list[str], owner: str, registry: ArmRegistry,
           indicators: Callable[[], Iterable[object]], history: Callable[[], Iterable[object]]) -> None:
    """`required_indicators` gives registered `IndicatorNeed`s and `required_history` gives `HistoryNeed`s (they are
    merged into leases and admission checks)."""
    for need in _call(problems, f"{owner}: required_indicators", indicators):
        if not isinstance(need, IndicatorNeed) or not registry.indicator_known(need.kind, need.version):
            problems.append(f"{owner}: a required indicator is not a registered IndicatorNeed")
    for h in _call(problems, f"{owner}: required_history", history):
        if not isinstance(h, HistoryNeed):
            problems.append(f"{owner}: a required history entry is not a HistoryNeed")


def _run(strategy: SlStrategy, params: StrategyParams, ev: EvalCase) -> tuple[object, ...]:
    evaluation = strategy.propose(params, ev.ctx, ev.state, ev.event)
    out: list[object] = [evaluation]
    if ev.event in BAR_END_EVENTS:
        out.append(strategy.at_target(params, ev.ctx, ev.state))
        out.append(strategy.commit_state(params, ev.state, evaluation, _final_outcome(evaluation)))
    return tuple(out)


def _final_outcome(evaluation: Evaluation) -> "Outcome":
    from src.riskmgr.outcomes import Outcome
    return Outcome.ADJUST if evaluation.proposal is not None and evaluation.proposal.targets else \
        Outcome.STRATEGY_REASON


def _evaluations(problems: list[str], owner: str, strategy: SlStrategy, params: StrategyParams,
                 case: StrategyCase) -> None:
    init = _call(problems, f"{owner}: init_state", lambda: strategy.init_state(params, case.arm_ctx))
    if not isinstance(init, StrategyState) or not init.names() <= strategy.STATE_SCHEMA:
        problems.append(f"{owner}: init_state holds a field outside STATE_SCHEMA")
    if init != _call(problems, f"{owner}: init_state", lambda: strategy.init_state(params, case.arm_ctx)):
        problems.append(f"{owner}: init_state is not deterministic")
    if not case.evaluations:
        problems.append(f"{owner}: the case has no evaluations")
    forwards = [_call(problems, f"{owner}: propose", lambda: _run(strategy, params, ev))
                for ev in case.evaluations]
    backwards = _call(problems, f"{owner}: propose (replay)",
                      lambda: [_run(strategy, params, ev) for ev in reversed(case.evaluations)][::-1])
    with localcontext(HOSTILE):
        hostile = _call(problems, f"{owner}: propose (replay)",
                        lambda: [_run(strategy, params, ev) for ev in case.evaluations])
    if forwards != backwards:
        problems.append(f"{owner}: results depend on call order (state carried between calls)")
    if forwards != hostile:
        problems.append(f"{owner}: results depend on the ambient Decimal context")
    for n, (ev, result) in enumerate(zip(case.evaluations, forwards)):
        where = f"{owner}: evaluation {n}"
        evaluation = result[0]
        if not isinstance(evaluation, Evaluation):
            problems.append(f"{where}: propose did not return an Evaluation")
            continue
        if evaluation.trigger_met is not ev.trigger_met:
            problems.append(f"{where}: trigger_met is not the bar-end condition the case states")
        if evaluation.reason and evaluation.reason not in strategy.REASONS:
            problems.append(f"{where}: reason {evaluation.reason} is not in REASONS")
        for name, value in evaluation.metrics:
            declared = strategy.DECISION_METRICS.get(name)
            if declared is None or type(value) is not declared:
                problems.append(f"{where}: metric {name} is not declared with its type")
        if evaluation.proposal is not None:
            if evaluation.proposal.resource is not Resource.STOP_LEVEL:
                problems.append(f"{where}: the proposal is not STOP_LEVEL")
            current = {HandleRef(h.kind, h.handle_id) for h in ev.ctx.handles}
            if not {h for h, _ in evaluation.proposal.targets} <= current:
                problems.append(f"{where}: the proposal names a handle the context does not hold")
        if case.break_even_target and ev.event in BAR_END_EVENTS:
            sig = 1 if ev.ctx.side == "BUY" else -1
            levels = ([evaluation.target] if evaluation.target is not None else []) + [
                t for _, t in (evaluation.proposal.targets if evaluation.proposal is not None else ())]
            if any(sig * (t - ev.ctx.entry) < 0 for t in levels):
                problems.append(f"{where}: a target on the loss side of entry")
            if result[1] is True and any(sig * (h.level - ev.ctx.entry) < 0 for h in ev.ctx.handles):
                problems.append(f"{where}: at_target with a handle on the loss side of entry")
        if ev.event in BAR_END_EVENTS:
            if not isinstance(result[1], bool):
                problems.append(f"{where}: at_target is not a bool")
            committed = result[2]
            if not isinstance(committed, StrategyState) or not committed.names() <= strategy.STATE_SCHEMA:
                problems.append(f"{where}: commit_state holds a field outside STATE_SCHEMA")


def _migrations(problems: list[str], owner: str, registry: ArmRegistry, strategy: SlStrategy,
                params: StrategyParams, case: StrategyCase) -> None:
    own = strategy.init_state(params, case.arm_ctx)
    same = _call(problems, f"{owner}: migrate_state", lambda: strategy.migrate_state(strategy.IMPL_VERSION, own))
    if same != own:
        problems.append(f"{owner}: migrate_state does not round-trip its own version's state")
    older = sorted(v for k, v in registry.strategy_keys() if k == strategy.KIND and v < strategy.IMPL_VERSION)
    covered = {v for v, _ in case.old_states}
    for v in older:
        if v not in covered:
            problems.append(f"{owner}: no old state from v{v} to migrate")
    for v, old in case.old_states:
        first = _call(problems, f"{owner}: migrate_state", lambda: strategy.migrate_state(v, old))
        if first != strategy.migrate_state(v, old):
            problems.append(f"{owner}: migrate_state from v{v} is not deterministic")
        if isinstance(first, StrategyState):
            if not first.names() <= strategy.STATE_SCHEMA:
                problems.append(f"{owner}: migrate_state from v{v} holds a field outside STATE_SCHEMA")
        elif not isinstance(first, Refuse):
            problems.append(f"{owner}: migrate_state from v{v} returned neither a state nor Refuse")


# ---------------------------------------------------------------------------------------------------- guards
def check_guard(registry: ArmRegistry, guard: Guard, case: GuardCase) -> list[str]:
    owner = f"guard {guard.KIND} v{guard.VERSION}"
    problems = reason_map_problems(owner, guard.REASONS)
    if ArmEvent.POST not in guard.EVENTS:
        problems.append(f"{owner}: EVENTS must include POST")
    before = deepcopy(case)
    try:
        _check_guard(problems, owner, registry, guard, case)
    except _Raised:
        pass
    if case != before:
        problems.append(f"{owner}: the case's inputs changed (params, contexts or states mutated)")
    return problems


def _check_guard(problems: list[str], owner: str, registry: ArmRegistry, guard: Guard, case: GuardCase) -> None:
    values = _call(problems, f"{owner}: parse_params", lambda: guard.parse_params(case.params))
    if values != _call(problems, f"{owner}: parse_params", lambda: guard.parse_params(dict(case.params))):
        problems.append(f"{owner}: parse_params is not deterministic")
    _refused(problems, f"{owner}: unknown key",
             lambda: guard.parse_params({**case.params, "zz_unknown_key": PLANTED_VALUE}))
    for n, bad in enumerate(case.invalid_params):
        _refused(problems, f"{owner}: invalid row {n}", lambda: guard.parse_params(bad),
                 _changed_values(case.params, bad))
    spec = GuardSpec(guard.KIND, guard.VERSION, values)
    _needs(problems, owner, registry, lambda: guard.required_indicators(spec),
           lambda: guard.required_history(spec))
    if not isinstance(_call(problems, f"{owner}: unknown_policy", lambda: guard.unknown_policy(spec)),
                      UnknownPolicy):
        problems.append(f"{owner}: unknown_policy is not an UnknownPolicy")
    label = _call(problems, f"{owner}: conditional_label", lambda: guard.conditional_label(spec))
    if ArmEvent.PRE not in guard.EVENTS and (not isinstance(label, str) or not label.strip()):
        problems.append(f"{owner}: a guard not run at PRE needs a conditional_label")
    timeout = _call(problems, f"{owner}: on_timeout", lambda: guard.on_timeout(spec))
    if not isinstance(timeout, GuardResult) or timeout.status not in (GuardStatus.UNKNOWN, GuardStatus.HOLD) \
            or timeout.reason not in guard.REASONS:
        problems.append(f"{owner}: on_timeout must be an UNKNOWN or HOLD with a reason in REASONS")
    for n, (c, _) in enumerate(case.contexts):
        early = _call(problems, f"{owner}: known_before_bar_end", lambda: guard.known_before_bar_end(spec, c))
        if early is not None and (not isinstance(early, GuardResult) or early.status is not GuardStatus.HOLD
                                  or early.reason not in guard.REASONS):
            problems.append(f"{owner}: context {n}: known_before_bar_end must be None or a HOLD in REASONS")
    if not case.contexts:
        problems.append(f"{owner}: the case has no contexts")

    def run(ctx: BarContext, state: StrategyState) -> tuple[bool, GuardResult]:
        return guard.applies(spec, ctx, state), guard.evaluate(spec, ctx)

    forwards = [_call(problems, f"{owner}: evaluate", lambda: run(c, s)) for c, s in case.contexts]
    backwards = _call(problems, f"{owner}: evaluate (replay)",
                      lambda: [run(c, s) for c, s in reversed(case.contexts)][::-1])
    with localcontext(HOSTILE):
        hostile = _call(problems, f"{owner}: evaluate (replay)", lambda: [run(c, s) for c, s in case.contexts])
    if forwards != backwards or forwards != hostile:
        problems.append(f"{owner}: verdicts are not deterministic under replay")
    for n, (applies, verdict) in enumerate(forwards):
        if not isinstance(applies, bool) or not isinstance(verdict, GuardResult):
            problems.append(f"{owner}: context {n}: applies / evaluate returned the wrong type")
        elif verdict.status in (GuardStatus.HOLD, GuardStatus.UNKNOWN) and verdict.reason not in guard.REASONS:
            problems.append(f"{owner}: context {n}: reason {verdict.reason} is not in REASONS")


# ---------------------------------------------------------------------------------------------------- registry-wide
def check_registry(registry: ArmRegistry, strategy_cases: Mapping[tuple[str, int], StrategyCase],
                   guard_cases: Mapping[tuple[str, int], GuardCase]) -> list[str]:
    """Every registered strategy and guard against its case; a class without a case is a problem."""
    problems: list[str] = []
    for kind, version in registry.guard_keys():
        guard = registry.guard(kind, version)
        g_case = guard_cases.get((kind, version))
        if guard is None or g_case is None:
            problems.append(f"guard {kind} v{version}: no conformance case")
            continue
        problems += check_guard(registry, guard, g_case)
    for kind, version in registry.strategy_keys():
        strategy = registry.strategy(kind, version)
        s_case = strategy_cases.get((kind, version))
        if strategy is None or s_case is None:
            problems.append(f"strategy {kind} v{version}: no conformance case")
            continue
        problems += check_strategy(registry, strategy, s_case)
    return problems


# ---------------------------------------------------------------------------------------------------- the engine's cases
def reopen_after_repin_evals() -> tuple[EvalCase, ...]:
    """REOPEN after an E re-pin (live review, finding 1), long and short: a trigger recorded at E = 1.20000 with
    offset 0 (target 1.20000), then E re-pinned 0.00010 further from the stop; the stored state is what the tracker
    writes with the re-pin (`state_after_entry_repin`: the target dropped, `latched` kept). One handle at the new S0,
    one at the old target (on the loss side of the new E): neither may get a loss-side target or count as at target."""
    from decimal import Decimal

    from src.riskmgr.tracker import state_after_entry_repin
    out: list[EvalCase] = []
    for sig, side in ((1, "BUY"), (-1, "SELL")):
        old_e = Decimal("1.20000")
        new_e = old_e + sig * Decimal("0.00010")
        s0 = new_e - sig * Decimal("0.00100")
        recorded = StrategyState.of(latched=True, target=old_e)
        kept = StrategyState.decode(state_after_entry_repin(recorded.encode(), frozenset({"latched", "target"}),
                                                            repinned=True))
        assert kept is not None
        close = str(new_e + sig * Decimal("0.00390"))
        for handle in (s0, old_e):
            ctx = kind_a.s_ctx(close, side=side, event=ArmEvent.REOPEN, entry=str(new_e), s0=str(s0),
                               stop=str(handle), handles=[str(handle)])
            out.append(EvalCase(ctx, kept, ArmEvent.REOPEN, True))
    return tuple(out)


STRATEGY_CASES[("break_even_on_risk_close", 1)] = StrategyCase(
    params=kind_a.strategy_row(),
    invalid_params=kind_a.STRATEGY_INVALID,
    arm_ctx=kind_a.s_arm_ctx(),
    evaluations=(*(EvalCase(ctx, state, event, met) for ctx, state, event, met in kind_a.strategy_evals()),
                 *reopen_after_repin_evals()),
    break_even_target=True,
)
GUARD_CASES[("first_bar", 1)] = GuardCase(
    params=kind_a.GUARD_PARAMS,
    invalid_params=kind_a.GUARD_INVALID,
    contexts=kind_a.guard_contexts(),
)
