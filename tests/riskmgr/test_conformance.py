"""The registry-wide conformance suite itself (plan S19): it runs on the empty engine registry, passes a well-formed
toy strategy and guard, and names the problem of each planted defect (non-determinism, ambient-context arithmetic,
unknown keys accepted, unmapped reasons, undeclared metrics, reordered guards, a wrong `trigger_met` meaning, a
missing migration case, a value echoed in a ParamError (planted or out of bound), a raise, mutated inputs, a guard's
unregistered indicator or malformed history need)."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from decimal import Context, Decimal
from typing import ClassVar

from src.riskmgr.guard import GuardResult
from src.riskmgr.registry import REGISTRY, ArmRegistry
from src.marketdata.types import M15, Side
from src.riskmgr.strategy import (
    Evaluation,
    GuardSpec,
    HistoryNeed,
    IndicatorNeed,
    ParamError,
    Refuse,
    StrategyParams,
    StrategyState,
    param_pairs,
)
from src.riskmgr.types import ArmEvent, BarContext, Ratio
from tests.riskmgr.conformance import (
    GUARD_CASES,
    PLANTED_VALUE,
    STRATEGY_CASES,
    EvalCase,
    GuardCase,
    StrategyCase,
    check_guard,
    check_registry,
)
from tests.riskmgr.toys import ToyGuard, ToyStrategy, arm_ctx, ctx, toy_row

KEY = (ToyStrategy.KIND, ToyStrategy.IMPL_VERSION)
GKEY = (ToyGuard.KIND, ToyGuard.VERSION)


def strategy_case(**over: object) -> StrategyCase:
    long0 = ToyStrategy().init_state(StrategyParams("toy_break_even", 1), arm_ctx())
    case = StrategyCase(
        params=toy_row(),
        invalid_params=(toy_row(offset_r="0.6"), toy_row(comparison="lt"), toy_row(offset_r=0.25)),
        arm_ctx=arm_ctx(),
        evaluations=(
            EvalCase(ctx("1.10200"), long0, ArmEvent.POST, True),
            EvalCase(ctx("1.10050"), long0, ArmEvent.POST, False),
            EvalCase(ctx("1.09950"), long0, ArmEvent.POST, False),
            EvalCase(ctx("1.12345", event=ArmEvent.PRE), long0, ArmEvent.PRE, True),
            EvalCase(ctx("1.09800", side="SELL"), long0, ArmEvent.POST, True),
            EvalCase(ctx("1.09950", side="SELL", event=ArmEvent.LATE), long0, ArmEvent.LATE, False),
            EvalCase(ctx("1.10200", stop="1.10000"), long0, ArmEvent.POST, True),
        ),
    )
    return replace(case, **over)  # type: ignore[arg-type]


def guard_case() -> GuardCase:
    s = StrategyState()
    return GuardCase(params={"limit": 4}, invalid_params=({"limit": 0}, {"limit": "x"}, {"limit": True}),
                     contexts=((ctx("1.1", bar_index=0, ticks=9), s), (ctx("1.1", bar_index=0), s),
                               (ctx("1.1", event=ArmEvent.PRE), s)))


def registry(strategy: object = None, guard: object = None) -> ArmRegistry:
    return ArmRegistry(strategies=[strategy or ToyStrategy()], guards=[guard or ToyGuard()])  # type: ignore[list-item]


def run(strategy: object = None, guard: object = None, case: StrategyCase | None = None) -> list[str]:
    return check_registry(registry(strategy, guard), {KEY: case or strategy_case()}, {GKEY: guard_case()})


# ---------------------------------------------------------------------------------------------------- the real registry
def test_suite_runs_on_the_empty_registry() -> None:
    assert check_registry(ArmRegistry(), {}, {}) == []


def test_engine_registry_conforms() -> None:
    """Every class the engine registers has a case and passes (S19: none yet; S20 adds kind (a), first_bar)."""
    assert check_registry(REGISTRY, STRATEGY_CASES, GUARD_CASES) == []


def test_well_formed_toy_passes() -> None:
    assert run() == []


def test_a_registered_class_without_a_case_is_a_problem() -> None:
    problems = check_registry(registry(), {}, {})
    assert "strategy toy_break_even v1: no conformance case" in problems
    assert "guard toy_calm v1: no conformance case" in problems


# ---------------------------------------------------------------------------------------------------- planted defects
class Flaky(ToyStrategy):
    calls: ClassVar[list[int]] = [0]

    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                event: ArmEvent) -> Evaluation:
        self.calls[0] += 1
        e = super().propose(params, ctx, state, event)
        return replace(e, caveats=(f"call{self.calls[0]}",))


class AmbientArithmetic(ToyStrategy):
    def favour(self, c: Context, sig: Decimal, close: Decimal, entry: Decimal) -> Decimal:
        return sig * (+close - entry)   # unary plus rounds to the ambient context


class LaxParams(ToyStrategy):
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        known = {k: v for k, v in raw.items() if k in self.KEYS}
        return super().parse_params(known)


class EchoingParams(ToyStrategy):
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        for k, v in raw.items():
            if k not in self.KEYS:
                raise ParamError(f"unknown {k}={v}")
        return super().parse_params(raw)


class UnmappedReason(ToyStrategy):
    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                event: ArmEvent) -> Evaluation:
        e = super().propose(params, ctx, state, event)
        return replace(e, reason="SOMETHING_ELSE") if e.reason else e


class UndeclaredMetric(ToyStrategy):
    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                event: ArmEvent) -> Evaluation:
        e = super().propose(params, ctx, state, event)
        return replace(e, metrics=(("favour_ratio", Ratio.of(Decimal(1))),))


class ReorderedGuards(ToyStrategy):
    def guards(self, params: StrategyParams) -> tuple[GuardSpec, ...]:
        return (*params.guards, GuardSpec("toy_calm", 1))


class Raising(ToyStrategy):
    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                event: ArmEvent) -> Evaluation:
        raise ZeroDivisionError


class OffSchema(ToyStrategy):
    def init_state(self, params: StrategyParams, arm_ctx: object) -> StrategyState:  # type: ignore[override]
        return StrategyState.of(last_close=Decimal("1.1"))


class ToyV2(ToyStrategy):
    IMPL_VERSION: ClassVar[int] = 2

    def migrate_state(self, from_impl_version: int, old_state: StrategyState) -> StrategyState | Refuse:
        if from_impl_version in (1, 2):
            return old_state
        return Refuse("unknown_version")


def test_non_deterministic_strategy_is_named() -> None:
    assert any("depend on call order" in p for p in run(Flaky()))


def test_ambient_decimal_context_is_named() -> None:
    assert any("ambient Decimal context" in p for p in run(AmbientArithmetic()))


def test_accepting_an_unknown_key_is_named() -> None:
    assert "strategy toy_break_even v1: unknown key: accepted" in run(LaxParams())


def test_a_param_error_must_not_echo_a_value() -> None:
    problems = run(EchoingParams())
    assert "strategy toy_break_even v1: unknown key: the ParamError repeats a value" in problems
    assert not any(PLANTED_VALUE in p for p in problems)


def test_every_reason_must_be_mapped() -> None:
    assert any("reason SOMETHING_ELSE is not in REASONS" in p for p in run(UnmappedReason()))


def test_metrics_must_be_declared_with_their_type() -> None:
    assert any("metric favour_ratio is not declared" in p for p in run(UndeclaredMetric()))


def test_guards_must_be_the_parsed_list_in_order() -> None:
    assert "strategy toy_break_even v1: guards(params) is not the parsed guard list in declared order" in run(
        ReorderedGuards())


def test_trigger_met_must_mean_the_bar_end_condition() -> None:
    case = strategy_case()
    wrong = replace(case, evaluations=(replace(case.evaluations[1], trigger_met=True),))
    assert any("trigger_met is not the bar-end condition" in p for p in run(case=wrong))


def test_a_raise_is_a_problem_not_a_crash() -> None:
    assert "strategy toy_break_even v1: propose raised ZeroDivisionError" in run(Raising())


def test_state_outside_the_schema_is_named() -> None:
    assert any("outside STATE_SCHEMA" in p for p in run(OffSchema()))


def test_an_older_version_needs_a_migration_case() -> None:
    reg = ArmRegistry(strategies=[ToyStrategy(), ToyV2()], guards=[ToyGuard()])
    problems = check_registry(reg, {KEY: strategy_case(), ("toy_break_even", 2): strategy_case()},
                              {GKEY: guard_case()})
    assert "strategy toy_break_even v2: no old state from v1 to migrate" in problems
    s1 = StrategyState.of(triggered=True)
    fixed = check_registry(reg, {KEY: strategy_case(), ("toy_break_even", 2): strategy_case(old_states=((1, s1),))},
                           {GKEY: guard_case()})
    assert fixed == []


def test_out_of_bound_rows_must_be_refused() -> None:
    lenient = strategy_case(invalid_params=(toy_row(offset_r="0.4"),))
    assert "strategy toy_break_even v1: invalid row 0: accepted" in run(case=lenient)


class EchoOutOfBound(ToyStrategy):
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        try:
            return super().parse_params(raw)
        except ParamError:
            raise ParamError(f"offset_r={raw.get('offset_r')} out of bounds") from None


def test_a_param_error_must_not_echo_an_out_of_bound_value() -> None:
    problems = run(EchoOutOfBound())
    assert "strategy toy_break_even v1: invalid row 0: the ParamError repeats a value" in problems   # '0.6'
    assert "strategy toy_break_even v1: invalid row 2: the ParamError repeats a value" in problems   # 0.25
    assert "strategy toy_break_even v1: invalid row 1: the ParamError repeats a value" not in problems  # 'lt' unechoed
    assert not any("0.6" in p or "0.25" in p for p in problems)


class MutatesItsInput(ToyStrategy):
    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState,
                event: ArmEvent) -> Evaluation:
        e = super().propose(params, ctx, state, event)
        CASE_PARAMS["offset_r"] = "0.1"   # mutates the case's own params document
        return e


CASE_PARAMS: dict[str, object] = {}


def test_mutated_inputs_are_named() -> None:
    CASE_PARAMS.clear()
    CASE_PARAMS.update(toy_row())
    problems = run(MutatesItsInput(), case=strategy_case(params=CASE_PARAMS))
    assert "strategy toy_break_even v1: the case's inputs changed (params, contexts or states mutated)" in problems


# ---------------------------------------------------------------------------------------------------- guards
class GuardUnmapped(ToyGuard):
    def evaluate(self, params: GuardSpec, ctx: BarContext) -> GuardResult:
        return GuardResult.hold("NOT_A_REASON")


class GuardNoLabel(ToyGuard):
    def conditional_label(self, params: GuardSpec) -> str:
        return " "


class GuardLax(ToyGuard):
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        return param_pairs(limit=4)


class GuardUnknownIndicator(ToyGuard):
    def required_indicators(self, params: GuardSpec) -> tuple[IndicatorNeed, ...]:
        return (IndicatorNeed("NOT_AN_INDICATOR", 1, M15, Side.BID, ()),)


class GuardBadHistory(ToyGuard):
    def required_history(self, params: GuardSpec) -> tuple[HistoryNeed, ...]:
        return ("two bars",)  # type: ignore[return-value]


class GuardRaisingNeeds(ToyGuard):
    def required_indicators(self, params: GuardSpec) -> tuple[IndicatorNeed, ...]:
        raise KeyError("length")


class GuardEchoOutOfBound(ToyGuard):
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        try:
            return super().parse_params(raw)
        except ParamError:
            raise ParamError(f"limit {raw.get('limit')} is out of bounds") from None


class GuardMutatesParams(ToyGuard):
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        out = super().parse_params(raw)
        if isinstance(raw, dict) and "limit" in raw:
            raw["limit"] = 5
        return out


def g(guard: object, case: GuardCase | None = None) -> list[str]:
    return check_guard(registry(), guard, case or guard_case())  # type: ignore[arg-type]


def test_guard_defects_are_named() -> None:
    assert g(ToyGuard()) == []
    assert any("reason NOT_A_REASON is not in REASONS" in p for p in g(GuardUnmapped()))
    assert "guard toy_calm v1: a guard not run at PRE needs a conditional_label" in g(GuardNoLabel())
    lax = g(GuardLax())
    assert "guard toy_calm v1: unknown key: accepted" in lax and "guard toy_calm v1: invalid row 0: accepted" in lax


def test_guard_requirements_are_checked() -> None:
    assert "guard toy_calm v1: a required indicator is not a registered IndicatorNeed" in g(GuardUnknownIndicator())
    assert "guard toy_calm v1: a required history entry is not a HistoryNeed" in g(GuardBadHistory())
    assert "guard toy_calm v1: required_indicators raised KeyError" in g(GuardRaisingNeeds())


def test_guard_param_error_must_not_echo_an_out_of_bound_value() -> None:
    case = replace(guard_case(), invalid_params=({"limit": 77},))
    assert "guard toy_calm v1: invalid row 0: the ParamError repeats a value" in g(GuardEchoOutOfBound(), case)
    assert g(ToyGuard(), case) == []


def test_guard_mutating_its_params_is_named() -> None:
    assert "guard toy_calm v1: the case's inputs changed (params, contexts or states mutated)" in g(
        GuardMutatesParams())
