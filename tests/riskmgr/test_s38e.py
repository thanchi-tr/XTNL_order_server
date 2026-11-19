"""Plan S38e (go-live hardening, operations): the riskmgr side. The last clear probes are wired (`MdBarsSeriesProbe`
for `series_reread` / `fresh_bar`, `VersionConformanceProbe` for `strategy_conformance`), so every FAILED scope kind
can be cleared through `FailedManager`'s probe dispatch; both D7 triggers that were never produced now fire end to end
(md-bars → the fault relay → the real `ErrorBudget`: `unresolved` after `fail_unresolved_n` buckets, `refetch_failed`);
and the go-live gates refuse `XTNL_ARM_ENABLED=1` / S39 until every item is wired or accepted. Synthetic ids only."""
from __future__ import annotations

import asyncio
import threading
from dataclasses import fields
from typing import Any

import pytest

from src.marketdata.events import RefetchQueue
from src.marketdata.types import M15, SeriesId, Side, Source
from src.ops import arm as ops_arm
from src.ops.cli import main as ops_main
from src.riskmgr import golive
from src.riskmgr import probes as P
from src.riskmgr.budget import UNRESOLVED, ErrorBudget
from src.riskmgr.config import FailSettings
from src.riskmgr.jobs import JobRunner, JobSpec
from src.riskmgr.registry import REGISTRY
from src.runtime.optional_md import FaultRelay
from tests.marketdata.test_brokerbars import B, S1, TF, T
from tests.marketdata.test_s38e_md import bars_rig
from tests.riskmgr.failed_fakes import FakeCalendar, FakeHealth, FakePg

STRATEGY = "strategy:{}:{}".format(*REGISTRY.strategy_keys()[0])
GUARD = "guard:{}:{}".format(*REGISTRY.guard_keys()[0])


# ---------------------------------------------------------------------------------------------------- series probe
class Bars:
    def __init__(self, reread: str | None = None, fresh: str | None = None) -> None:
        self.reread_code, self.fresh_code = reread, fresh
        self.asked: list[tuple[str, SeriesId]] = []

    async def probe_reread(self, series: SeriesId) -> str | None:
        self.asked.append(("reread", series))
        return self.reread_code

    async def probe_fresh_bar(self, series: SeriesId) -> str | None:
        self.asked.append(("fresh", series))
        return self.fresh_code


async def test_the_series_probe_maps_md_bars_codes_and_refuses_unknown_scopes() -> None:
    bars = Bars()
    probe = P.MdBarsSeriesProbe(bars)
    assert await probe.reread(S1.canonical) == P.PASSED
    assert await probe.fresh_bar(S1.canonical) == P.PASSED
    assert bars.asked == [("reread", S1), ("fresh", S1)]
    bars.reread_code, bars.fresh_code = "invalid_bar", "not_final"
    assert await probe.reread(S1.canonical) == P.refused_probe("invalid_bar")
    assert await probe.fresh_bar(S1.canonical) == P.refused_probe("not_final")
    bars.reread_code = "Not A Code 1"
    assert await probe.reread(S1.canonical) == P.refused_probe(P.PROBE_ERROR)
    tick = SeriesId("mock", "SYN1", M15, Side.BID, Source.TICK)
    for scope_id in (tick.canonical, "mock:SYN1:ind:0123456789abcdef", "nonsense"):
        assert await probe.reread(scope_id) == P.refused_probe(P.SERIES_UNKNOWN)
        assert await probe.fresh_bar(scope_id) == P.refused_probe(P.SERIES_UNKNOWN)


async def test_the_series_probe_over_the_real_md_bars() -> None:
    rig = bars_rig(start=T + 30_000)
    probe = P.MdBarsSeriesProbe(rig.bars)
    assert await probe.reread(S1.canonical) == P.PASSED
    assert await probe.fresh_bar(S1.canonical) == P.PASSED
    rig.broker.put(B - TF, high="1.0980")
    assert await probe.reread(S1.canonical) == P.refused_probe("invalid_bar")


# ---------------------------------------------------------------------------------------------------- version probe
async def test_the_version_probe_runs_one_job_of_the_version_and_passes() -> None:
    jobs = JobRunner()
    probe = P.VersionConformanceProbe(REGISTRY, jobs)
    assert await probe.probe(STRATEGY) == P.PASSED
    assert await probe.probe(GUARD) == P.PASSED
    assert await probe.probe("strategy:NO_SUCH:1") == P.refused_probe(P.VERSION_UNKNOWN, "arm_repin")
    assert await probe.probe("bogus") == P.refused_probe(P.VERSION_UNKNOWN, "arm_repin")
    assert P.conformance_problems(REGISTRY, STRATEGY) == [] and P.conformance_problems(REGISTRY, GUARD) == []
    jobs.close()


async def test_the_version_probe_refuses_a_hung_lane_and_a_failing_conformance(
        monkeypatch: pytest.MonkeyPatch) -> None:
    jobs = JobRunner(cap_ms=20, max_abandoned=0)
    release = threading.Event()
    result = await jobs.run(JobSpec(STRATEGY), "pos", lambda: release.wait(5))   # over the cap, never retired
    assert result.reason == "job_timeout" and jobs.hung(STRATEGY)
    probe = P.VersionConformanceProbe(REGISTRY, jobs)
    assert await probe.probe(STRATEGY) == P.refused_probe(P.JOB_LANE_HUNG)
    release.set()
    for _ in range(100):
        if not jobs.hung(STRATEGY):
            break
        await asyncio.sleep(0.01)
    monkeypatch.setattr(P, "reason_map_problems", lambda owner, reasons: [f"{owner}: planted"])
    assert await probe.probe(STRATEGY) == P.refused_probe(P.CONFORMANCE)
    jobs.close()


# ---------------------------------------------------------------------------------------------------- every scope clears
def test_every_probe_kind_has_a_wired_port_in_the_clear_probes_contract() -> None:
    """Every `probe_kind` a FAILED episode can carry maps to a `ClearProbes` port the runtime now wires (S38e; the
    graph test `tests/runtime/test_optional_plan.py` checks the built object has no None port)."""
    port_of = {P.SERIES_REREAD: "series", P.FRESH_BAR: "series", P.INDICATOR_REBUILD: "indicators",
               P.SPAN_WRITE: "span", P.STRATEGY_CONFORMANCE: "versions", P.ADAPTER_SERIES: "series",
               P.ARM_GLOBAL: "trial"}
    assert set(port_of) == P.PROBE_KINDS
    assert set(port_of.values()) <= {f.name for f in fields(P.ClearProbes)}


# ---------------------------------------------------------------------------------------------------- D7 end to end
class BudgetRig:
    def __init__(self, clock: Any) -> None:
        self.budget = ErrorBudget(FailSettings(), calendar=FakeCalendar(), health=FakeHealth(),
                                  pg=FakePg(clock))
        self.relay = FaultRelay(clock)
        self.relay.bind(self.budget)


async def test_two_buckets_unresolved_at_their_caps_fail_the_series() -> None:
    """D7 'a bar UNRESOLVED at decision_cap twice in a row' (`fail_unresolved_n` 2), from md-bars through the
    relay into the real error budget: one SERIES `unresolved` fault, probe `fresh_bar`."""
    rig = bars_rig()
    br = BudgetRig(rig.clock)
    rig.bars._buckets = br.relay                       # noqa: SLF001 (the S31 wiring: md-bars' notes go to the relay)
    rig.broker.hidden.update({B, B + TF})
    await rig.run_to(T + TF + 905_000, every=5_000)
    (fault,) = br.budget.take()
    assert fault.scope.scope_id == S1.canonical and fault.reason == UNRESOLVED and fault.probe_kind == "fresh_bar"


async def test_a_clean_bar_between_unresolved_buckets_resets_the_streak() -> None:
    rig = bars_rig()
    br = BudgetRig(rig.clock)
    rig.bars._buckets = br.relay                       # noqa: SLF001
    rig.broker.hidden.update({B, B + 2 * TF, B + 3 * TF})   # B + TF is clean in between (B + 3TF: no later reset)
    await rig.run_to(T + 905_000, every=5_000)
    assert br.budget.unresolved_streak(S1) == 1        # B unresolved at its cap
    await rig.run_to(T + 2 * TF + 910_000, every=5_000)     # past B + 2TF's cap (bar end + TF + p50 + settle)
    assert rig.bars.counters.capped_unresolved == 2
    assert br.budget.take() == []                      # the clean B + TF reset the streak (note_final)
    assert br.budget.unresolved_streak(S1) == 1        # B + 2TF alone
    hidden = bars_rig()
    hb = BudgetRig(hidden.clock)
    hidden.bars._buckets = hb.relay                    # noqa: SLF001
    hidden.broker.hidden.update({B, B + TF, B + 2 * TF})   # no clean bar between: the fault fires
    await hidden.run_to(T + 2 * TF + 910_000, every=5_000)
    (fault,) = hb.budget.take()
    assert fault.reason == UNRESOLVED


async def test_refetch_failed_reaches_the_error_budget_as_a_series_fault() -> None:
    queue = RefetchQueue()
    rig = bars_rig(refetch=queue, history_reach_ms=10 * TF)
    br = BudgetRig(rig.clock)
    rig.bars._on_fault = br.relay.series                # noqa: SLF001 (the S31 wiring)
    await rig.run_to(T - 58_000)
    assert queue.request(S1, B - 30 * TF, B - 30 * TF) is None
    await rig.run_to(T - 55_000)
    (fault,) = br.budget.take()
    assert fault.reason == "refetch_failed" and fault.probe_kind == "series_reread"


# ---------------------------------------------------------------------------------------------------- go-live gates
STOP_RULE_ON = {golive.STOP_AT_ENTRY_VAR: "1"}
"""S38f: accepting `oversee_c4` counts only with the S07 Oversee stop rule switched on."""


def _open_ids() -> list[str]:
    return [g.gate_id for g in golive.GATES if g.state is golive.GateState.OPEN]


def test_open_gates_refuse_the_arm_until_each_is_accepted() -> None:
    assert len({g.gate_id for g in golive.GATES}) == len(golive.GATES)
    v = golive.verdict({})   # switch off: the table-accepted oversee_c4 is refused as well
    assert not v.allowed and [g.gate_id for g in v.refused] == _open_ids() + ["oversee_c4"]
    v = golive.verdict(dict(STOP_RULE_ON))
    assert not v.allowed and [g.gate_id for g in v.refused] == _open_ids()
    v = golive.verdict({golive.ACCEPT_VAR: ",".join(_open_ids()[:-1]), **STOP_RULE_ON})
    assert not v.allowed and [g.gate_id for g in v.refused] == _open_ids()[-1:]
    v = golive.verdict({golive.ACCEPT_VAR: " , ".join(_open_ids()) + ", clear_probes, nope", **STOP_RULE_ON})
    assert v.allowed and v.accepted == tuple(sorted(_open_ids()))
    assert len(v.problems) == 2 and all(golive.ACCEPT_VAR in p for p in v.problems)
    assert golive.refusal_text(golive.verdict(dict(STOP_RULE_ON))).count(",") == len(_open_ids()) - 1


def test_the_gates_s38e_closed_are_wired_and_the_rest_are_listed() -> None:
    wired = {g.gate_id for g in golive.GATES if g.state is golive.GateState.WIRED}
    assert {"clear_probes", "d7_triggers", "d4_corrections", "md_reset_notice", "broker_retention",
            "redis_md_grace"} <= wired
    accepted = {g.gate_id for g in golive.GATES if g.state is golive.GateState.ACCEPTED}
    # the user's decisions and the applied .claude proposals of 2026-11-18 are recorded in the table
    assert {"oversee_c4", "s21_c1", "q1_q2", "c2_c3_c5", "s25_registry", "claude_rules"} <= accepted
    # S39b wired the six engineering gates (tests/riskmgr/test_s39b.py::test_the_six_gates_are_wired)
    assert "uptime_suspend" in wired and "corrections_restart" in wired
    # S39 wired pr3_finality after the PR3 probe; its unmeasured stop-read and roll items stay open
    assert "pr3_finality" in wired and {"pr3_stop_children", "pr3_roll_prints"} <= set(_open_ids())


def test_s39_waits_for_every_gate_it_does_not_close_itself() -> None:
    """S39 (an adapter in `BAR_CAPABLE_ADAPTERS`) is refused while any gate other than `pr3_finality` is OPEN in
    the table: a user acceptance for S39 is recorded there (state ACCEPTED), never inferred."""
    blockers = {g.gate_id for g in golive.s39_blockers()}
    assert "pr3_finality" not in blockers and "pr3_stop_children" in blockers
    assert not ops_arm.BAR_CAPABLE_ADAPTERS or not blockers, \
        f"S39 enabled bars while go-live gates are open: {sorted(blockers)}"


def test_ops_arm_gates_exits_3_while_enabling_would_be_refused(capsys: pytest.CaptureFixture[str]) -> None:
    assert ops_main(["arm", "gates"], environ={}) == 3
    out = capsys.readouterr().out
    assert "outcome: refused" in out and "arm_enable_allowed: False" in out and "gate=oversee_c4" in out
    assert ops_main(["arm", "gates"], environ={golive.ACCEPT_VAR: ",".join(_open_ids()), **STOP_RULE_ON}) == 0
    assert "arm_enable_allowed: True" in capsys.readouterr().out


def test_ops_arm_gates_reads_the_engine_env_file_when_named(tmp_path: Any,
                                                             capsys: pytest.CaptureFixture[str]) -> None:
    """Review S38e: `--env-file` reads the engine's env file (the shell's own setting is ignored) and the report
    names the source it read."""
    env_file = tmp_path / "engine.env"
    env_file.write_text(f"# engine\nXTNL_ARM_ENABLED=0\n{golive.ACCEPT_VAR}={','.join(_open_ids())}\n"
                        f"{golive.STOP_AT_ENTRY_VAR}=1\n",
                        encoding="utf-8")
    shell = {golive.ACCEPT_VAR: ""}
    assert ops_main(["arm", "gates", "--env-file", str(env_file)], environ=shell) == 0
    out = capsys.readouterr().out
    assert "arm_enable_allowed: True" in out and f"settings_source: env_file:{env_file}" in out
    env_file.write_text(f"{golive.ACCEPT_VAR}=\n", encoding="utf-8")
    everything = {golive.ACCEPT_VAR: ",".join(_open_ids()), **STOP_RULE_ON}
    assert ops_main(["arm", "gates", "--env-file", str(env_file)], environ=everything) == 3
    assert ops_main(["arm", "gates"], environ=shell) == 3
    assert "settings_source: process_environment" in capsys.readouterr().out
    assert ops_main(["arm", "gates", "--env-file", str(tmp_path / "missing.env")], environ=shell) == 2
