"""S39b gate `conformance_probe_partial`: the `strategy_conformance` clear probe runs the pinned version's own code
against its conformance vectors, under the job runner's time cap; a hung, raising or misbehaving version fails it.

Planted defects are subclasses of the engine's classes registered in a private `ArmRegistry` under the same
`(KIND, VERSION)`, so they meet the same vectors. Synthetic levels only; no assertion message carries a value.
"""
from __future__ import annotations

import threading
from decimal import Decimal

import pytest

from src.riskmgr import probes as P
from src.riskmgr import vectors as V
from src.riskmgr.guard import GuardResult, guard_scope_id, strategy_scope_id
from src.riskmgr.guards.first_bar import FirstBarGuard
from src.riskmgr.jobs import JobRunner
from src.riskmgr.registry import REGISTRY, ArmRegistry
from src.riskmgr.strategies.break_even_on_risk_close import BREAK_EVEN_ON_RISK_CLOSE_V1, BreakEvenOnRiskClose
from src.riskmgr.strategy import Evaluation, StrategyParams, StrategyState
from src.riskmgr.types import ArmEvent, BarContext

STRATEGY = strategy_scope_id("break_even_on_risk_close", 1)
GUARD = guard_scope_id("first_bar", 1)
HANG = threading.Event()


class Hanging(BreakEvenOnRiskClose):
    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState, event: ArmEvent) -> Evaluation:
        HANG.wait(5)
        return super().propose(params, ctx, state, event)


class Raising(BreakEvenOnRiskClose):
    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState, event: ArmEvent) -> Evaluation:
        raise RuntimeError("boom")


class Inverted(BreakEvenOnRiskClose):
    """Never sees its trigger (a version that would silently stop advising)."""

    def propose(self, params: StrategyParams, ctx: BarContext, state: StrategyState, event: ArmEvent) -> Evaluation:
        ev = super().propose(params, ctx, state, event)
        return Evaluation(False, reason="BELOW_TRIGGER") if ev.trigger_met else ev


class WrongTarget(BreakEvenOnRiskClose):
    """Subtracts the buffer: advises a target on the loss side of entry (live review: the vectors must catch it)."""

    def target(self, params: StrategyParams, ctx: BarContext) -> Decimal:
        c = self.DECIMAL_CONTEXT
        offset = c.multiply(Decimal(params.param("be_offset_r")), max(ctx.r, Decimal(0)))
        return c.subtract(ctx.entry, c.multiply(Decimal(ctx.sigma), offset))


class AlwaysPass(FirstBarGuard):
    def evaluate(self, params, ctx):  # type: ignore[no-untyped-def]
        return GuardResult.passed()


def _registry(strategy: object = BREAK_EVEN_ON_RISK_CLOSE_V1, guard: object | None = None) -> ArmRegistry:
    return ArmRegistry(strategies=[strategy], guards=[guard or REGISTRY.guard("first_bar", 1)])  # type: ignore[list-item]


def test_the_engine_versions_meet_their_vectors() -> None:
    assert V.vector_problems(REGISTRY, "strategy", "break_even_on_risk_close", 1) == []
    assert V.vector_problems(REGISTRY, "guard", "first_bar", 1) == []
    assert len(V.vectors_of("strategy", "break_even_on_risk_close", 1)) >= 5
    assert len(V.vectors_of("guard", "first_bar", 1)) >= 4


async def test_the_probe_runs_the_versions_code_and_passes() -> None:
    jobs = JobRunner()
    probe = P.VersionConformanceProbe(REGISTRY, jobs)
    assert await probe.probe(STRATEGY) == P.PASSED and await probe.probe(GUARD) == P.PASSED
    jobs.close()


async def test_a_hung_version_fails_the_probe_under_the_job_cap() -> None:
    HANG.clear()
    jobs = JobRunner(cap_ms=100)
    try:
        got = await P.VersionConformanceProbe(_registry(Hanging()), jobs).probe(STRATEGY)
        assert got == P.refused_probe("job_timeout")
    finally:
        HANG.set()
        jobs.close()


async def test_a_raising_version_fails_the_probe() -> None:
    jobs = JobRunner()
    got = await P.VersionConformanceProbe(_registry(Raising()), jobs).probe(STRATEGY)
    assert got == P.refused_probe("job_raised")
    jobs.close()


@pytest.mark.parametrize("strategy,guard,scope", [
    (Inverted(), None, STRATEGY),
    (WrongTarget(), None, STRATEGY),
    (BREAK_EVEN_ON_RISK_CLOSE_V1, AlwaysPass(), GUARD),
])
async def test_a_version_answering_its_vectors_wrongly_fails_the_probe(strategy: object, guard: object | None,
                                                                       scope: str) -> None:
    jobs = JobRunner()
    reg = _registry(strategy, guard)
    head, kind, version = scope.split(":")
    assert V.vector_problems(reg, head, kind, int(version)) != []
    assert await P.VersionConformanceProbe(reg, jobs).probe(scope) == P.refused_probe(P.CONFORMANCE)
    jobs.close()


async def test_a_version_without_vectors_fails_the_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(V, "_BUILDERS", {})
    assert V.vector_problems(REGISTRY, "strategy", "break_even_on_risk_close", 1) == [
        f"strategy break_even_on_risk_close v1: {V.NO_VECTORS}"]
    jobs = JobRunner()
    assert await P.VersionConformanceProbe(REGISTRY, jobs).probe(STRATEGY) == P.refused_probe(P.CONFORMANCE)
    jobs.close()


def test_a_problem_names_the_vector_and_field_only() -> None:
    problems = V.vector_problems(_registry(Inverted()), "strategy", "break_even_on_risk_close", 1)
    assert problems and not any("1.2" in p for p in problems)
    assert all(p.rsplit(":", 1)[-1].strip() in {"trigger_met", "reason", "moves", "at_target", "target"}
               for p in problems)
