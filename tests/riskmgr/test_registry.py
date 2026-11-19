"""The strategy and guard registry (plan S19; design §B6.1 Resources, Guard contract, Version lifecycle, PR13)."""
from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import ClassVar

import pytest

from src.riskmgr.envelope import ENVELOPE_KEYS
from src.riskmgr.outcomes import Category
from src.marketdata.types import BarSpec
from src.riskmgr.registry import REGISTRY, ArmRegistry, RegistryError, TimingRowError
from src.riskmgr.strategy import GuardSpec, ParamError, StrategyParams
from src.riskmgr.types import ArmEvent, BarEvent
from tests.riskmgr.toys import ToyGuard, ToyStrategy, toy_row


def reg() -> ArmRegistry:
    return ArmRegistry(strategies=[ToyStrategy()], guards=[ToyGuard()])


class TakeProfit(ToyStrategy):
    RESOURCE = "TAKE_PROFIT"  # type: ignore[assignment]


class ToyV2(ToyStrategy):
    IMPL_VERSION: ClassVar[int] = 2


class ActionableReason(ToyStrategy):
    REASONS: ClassVar[Mapping[str, Category]] = {"BELOW_TRIGGER": Category.ACTIONABLE}


class ClashingReason(ToyStrategy):
    REASONS: ClassVar[Mapping[str, Category]] = {"ADJUST": Category.INFO}


class FloatMetric(ToyStrategy):
    DECISION_METRICS = {"risk_r": Decimal}  # type: ignore[assignment,dict-item]


class PreOnlyGuard(ToyGuard):
    EVENTS: ClassVar[frozenset[ArmEvent]] = frozenset({ArmEvent.PRE})


class LateGuard(ToyGuard):
    EVENTS: ClassVar[frozenset[ArmEvent]] = frozenset({ArmEvent.POST, ArmEvent.LATE})


class SameKindGuard(ToyGuard):
    KIND: ClassVar[str] = "toy_break_even"


class ToyGuardV2(ToyGuard):
    VERSION: ClassVar[int] = 2


def test_engine_registry_holds_kind_a_and_first_bar() -> None:
    """S20: kind (a) and FirstBarGuard, and nothing else (a new class lands with its conformance case)."""
    assert REGISTRY.strategy_keys() == (("break_even_on_risk_close", 1),)
    assert REGISTRY.guard_keys() == (("first_bar", 1),)
    assert REGISTRY.registry_tokens() == ("ATR:1", "break_even_on_risk_close:1", "first_bar:1")
    assert not REGISTRY.strategy_deprecated("break_even_on_risk_close", 1)


def test_refuses_another_resource() -> None:
    with pytest.raises(RegistryError, match="only RESOURCE STOP_LEVEL"):
        ArmRegistry(strategies=[TakeProfit()])


@pytest.mark.parametrize("bad", [ActionableReason(), ClashingReason(), FloatMetric()])
def test_refuses_bad_declarations(bad: ToyStrategy) -> None:
    with pytest.raises(RegistryError):
        ArmRegistry(strategies=[bad])


@pytest.mark.parametrize("bad", [PreOnlyGuard(), LateGuard()])
def test_guard_events_hold_post_and_bar_events_only(bad: ToyGuard) -> None:
    with pytest.raises(RegistryError, match="EVENTS"):
        ArmRegistry(guards=[bad])


def test_duplicates_and_kind_clashes_are_refused() -> None:
    r = reg()
    with pytest.raises(RegistryError, match="already registered"):
        r.add_strategy(ToyStrategy())
    with pytest.raises(RegistryError, match="already registered"):
        r.add_guard(ToyGuard())
    with pytest.raises(RegistryError, match="is a strategy KIND"):
        r.add_guard(SameKindGuard())


def test_tokens_cover_strategies_guards_and_indicators() -> None:
    r = reg()
    r.add_strategy(ToyV2())
    assert r.registry_tokens() == ("ATR:1", "toy_break_even:1", "toy_break_even:2", "toy_calm:1")
    assert r.indicator_known("ATR", 1) and not r.indicator_known("ATR", 2)


def test_parse_params_pins_strategy_and_guard_params() -> None:
    p = reg().parse_params("toy_break_even", 1, toy_row(offset_r=Decimal("0.10"),
                                                        guards=[{"kind": "toy_calm", "version": Decimal(1),
                                                                 "params": {"limit": 7}}]))
    assert p.values == (("comparison", "gt"), ("offset_r", "0.1"))
    assert p.guards == (GuardSpec("toy_calm", 1, (("limit", "7"),)),)
    assert p.params_hash == reg().parse_params("toy_break_even", 1, toy_row(
        offset_r="0.1", guards=[{"kind": "toy_calm", "version": 1, "params": {"limit": "7"}}])).params_hash
    other = reg().parse_params("toy_break_even", 1, toy_row(
        offset_r="0.1", guards=[{"kind": "toy_calm", "version": 1, "params": {"limit": "8"}}]))
    assert other.params_hash != p.params_hash and other.guards_hash != p.guards_hash


@pytest.mark.parametrize(("raw", "match"), [
    ({"comparison": "gt"}, "guards must list toy_calm"),
    (toy_row(guards=[]), "guards must list toy_calm"),
    (toy_row(guards="toy_calm"), "guards must be a list"),
    (toy_row(guards=[{"kind": "toy_calm", "version": 1}]), "exactly kind, version and params"),
    (toy_row(guards=[{"kind": "toy_calm", "version": 1, "params": {}, "x": 1}]), "exactly kind, version"),
    (toy_row(guards=[{"kind": "toy_calm", "version": 2, "params": {}}]), "toy_calm v2 is not registered"),
    (toy_row(guards=[{"kind": "toy_calm", "version": True, "params": {}}]), "out of form"),
    (toy_row(guards=[{"kind": "toy_calm", "version": "1", "params": {}}]), "out of form"),
    (toy_row(guards=[{"kind": "toy_calm", "version": 1, "params": {}}] * 2), "listed twice"),
    (toy_row(guards=[{"kind": "toy_calm", "version": 1, "params": {"limit": 99}}]), "limit"),
    (toy_row(guards=[{"kind": "toy_calm", "version": 1, "params": []}]), "params must be an object"),
    (toy_row(**{"toy_calm.limit": 3}), "unknown parameter"),
    (toy_row(offset_r="0.51"), "offset_r"),
])
def test_parse_params_refusals(raw: dict[str, object], match: str) -> None:
    with pytest.raises(ParamError, match=match):
        reg().parse_params("toy_break_even", 1, raw)


@pytest.mark.parametrize("key", sorted(ENVELOPE_KEYS))
def test_envelope_keys_are_never_strategy_params(key: str) -> None:
    with pytest.raises(ParamError, match="local engine setting"):
        reg().parse_params("toy_break_even", 1, toy_row(**{key: "0"}))


def test_unknown_strategy_is_refused() -> None:
    with pytest.raises(ParamError, match="unknown strategy"):
        reg().parse_params("toy_break_even", 3, toy_row())


def test_deprecated_versions_stay_loadable_but_refuse_new_pins() -> None:
    r = reg()
    r.add_strategy(ToyV2())
    r.add_guard(ToyGuardV2())
    r.deprecate_strategy("toy_break_even", 1)
    assert r.strategy("toy_break_even", 1) is not None and r.strategy_deprecated("toy_break_even", 1)
    with pytest.raises(ParamError, match="deprecated"):
        r.parse_params("toy_break_even", 1, toy_row())
    assert r.parse_params("toy_break_even", 1, toy_row(), new_pin=False).kind == "toy_break_even"
    r.deprecate_guard("toy_calm", 2)
    assert r.guard("toy_calm", 2) is not None and r.guard_deprecated("toy_calm", 2)
    row2 = toy_row(guards=[{"kind": "toy_calm", "version": 2, "params": {}}])
    with pytest.raises(ParamError, match="toy_calm v2 is deprecated"):
        r.parse_params("toy_break_even", 2, row2)
    assert r.parse_params("toy_break_even", 2, row2, new_pin=False).guards[0].version == 2
    assert "toy_break_even:1" in r.registry_tokens() and "toy_calm:2" in r.registry_tokens()   # never removed
    with pytest.raises(KeyError):
        r.deprecate_strategy("toy_break_even", 9)
    with pytest.raises(KeyError):
        r.deprecate_guard("toy_calm", 9)


def test_param_errors_never_repeat_a_value() -> None:
    with pytest.raises(ParamError) as e:
        reg().parse_params("toy_break_even", 1, toy_row(offset_r="0.777777"))
    assert "0.777777" not in str(e.value)


class BuggyStrategy(ToyStrategy):
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        raise KeyError(f"offset_r={raw.get('offset_r')}")


class BuggyGuard(ToyGuard):
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        raise TypeError(f"limit={raw.get('limit')}")


def test_a_class_raising_another_exception_refuses_the_row() -> None:
    """A buggy class never crashes validation or pinning: the row is refused, the exception text never shown."""
    with pytest.raises(ParamError) as e:
        ArmRegistry(strategies=[BuggyStrategy()], guards=[ToyGuard()]).parse_params(
            "toy_break_even", 1, toy_row(offset_r="0.4321"))
    assert str(e.value) == "strategy toy_break_even v1: params could not be parsed"
    assert e.value.__cause__ is None and e.value.__suppress_context__
    with pytest.raises(ParamError) as g:
        ArmRegistry(strategies=[ToyStrategy()], guards=[BuggyGuard()]).parse_params(
            "toy_break_even", 1, toy_row(guards=[{"kind": "toy_calm", "version": 1, "params": {"limit": 37}}]))
    assert "params could not be parsed" in str(g.value) and "37" not in str(g.value)


class HourlyTriggers(ToyStrategy):
    """Triggers on a bar spec with no timing row (v1 has only the 15m row, design §B5.2)."""

    def triggers(self, params: StrategyParams) -> tuple[BarEvent, ...]:
        return (BarEvent(BarSpec.parse("1h"), ArmEvent.POST),)


class RaisingTriggers(ToyStrategy):
    def triggers(self, params: StrategyParams) -> tuple[BarEvent, ...]:
        raise RuntimeError("planted")


def test_new_pin_needs_a_timing_row_for_every_trigger_bar_spec() -> None:
    """Plan S25: validation (and arming) refuse a strategy whose bar spec has no timing row; re-parsing an existing
    pin does not."""
    registry = ArmRegistry(strategies=[HourlyTriggers()], guards=[ToyGuard()])
    with pytest.raises(TimingRowError) as e:
        registry.parse_params("toy_break_even", 1, toy_row())
    assert str(e.value) == "strategy toy_break_even v1: bar spec 1h has no timing row"
    assert isinstance(e.value, ParamError)
    assert registry.parse_params("toy_break_even", 1, toy_row(), new_pin=False).kind == "toy_break_even"
    with pytest.raises(ParamError) as r:
        ArmRegistry(strategies=[RaisingTriggers()], guards=[ToyGuard()]).parse_params("toy_break_even", 1, toy_row())
    assert str(r.value) == "strategy toy_break_even v1: triggers could not be read" and "planted" not in str(r.value)
    assert reg().parse_params("toy_break_even", 1, toy_row()).kind == "toy_break_even"


def test_names_armconfig_imports_are_exported() -> None:
    """`src/armconfig` may import only this module of the ARM (design §B3), so it exports what `parse_params` needs."""
    import src.riskmgr.registry as registry_module
    from src.riskmgr.strategy import ParamError as StrategyParamError
    from src.riskmgr.strategy import StrategyParams as StrategyParamsType

    assert {"ENVELOPE_KEYS", "ParamError", "StrategyParams", "TimingRowError"} <= set(registry_module.__all__)
    assert registry_module.ParamError is StrategyParamError
    assert registry_module.StrategyParams is StrategyParamsType
    assert registry_module.ENVELOPE_KEYS is ENVELOPE_KEYS
