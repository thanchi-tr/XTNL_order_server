"""S30b: the FAILED clear probes and the clear path (design §B14 Clearing, §A7 trial bootstrap, §B12; R11-M09,
R11-M12).

The record and the clear statement run through the real `PgArmFailuresStore` / `PgArmFailedClear` over the
`ArmEmulator` (the clear needs a matching `arm_clear_failed` request of this epoch); the ARM-global probe's trial
bootstrap runs through the real `MdWriterService` over fakeredis (`tests/marketdata/bootrig.py`). Synthetic ids,
symbols and times only; no price in any assertion.
"""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import pytest

from src.db.arm_budget_store import PgArmBudgetStore
from src.db.arm_failures_store import PgArmFailedClear
from src.db.arm_ports import ClearedFailure
from src.db.epoch import EngineDb, Epoch
from src.db.fence import EpochMismatch
from src.marketdata.indicators.engine import IndicatorStatus, IndState
from src.marketdata.store import keys as K
from src.marketdata.types import INDICATOR_FAULTED, INVALID_BAR, SOURCE_MISMATCH
from src.riskmgr import budget as B
from src.riskmgr import failed as F
from src.riskmgr import probes as P
from src.riskmgr.config import FailSettings
from src.riskmgr.frontier import STUCK_FRONTIER
from src.riskmgr.jobs import JobRunner
from src.riskmgr.types import ScopeKind
from tests.marketdata.bootrig import make_rig
from tests.marketdata.conftest import RunIds, md_redis, run_ids  # noqa: F401  (fixtures)
from tests.riskmgr.failed_fakes import EPOCH, T0, FailedRig, failed_rig, row, series
from tests.riskmgr.tracker_fakes import ADAPTER

S_A, S_B, S_C = series("SYN.A"), series("SYN.B"), series("SYN.C")
VERSION = "strategy:break_even_on_risk_close:1"


# ---------------------------------------------------------------------------------------------------- fakes
@dataclass
class FakeSeriesProbe:
    """md-bars' probe reads: a scripted result per series (default passed); `hold` makes a read hang; `delay` (s)
    makes it take that long."""

    results: dict[str, P.ProbeResult] = field(default_factory=dict)
    calls: list[tuple[str, str]] = field(default_factory=list)
    hold: asyncio.Event | None = None
    delay: float = 0.0

    async def _read(self, kind: str, s: str) -> P.ProbeResult:
        self.calls.append((kind, s))
        if self.hold is not None:
            await self.hold.wait()
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.results.get(s, P.PASSED)

    async def reread(self, series: str) -> P.ProbeResult:
        return await self._read("reread", series)

    async def fresh_bar(self, series: str) -> P.ProbeResult:
        return await self._read("fresh_bar", series)


@dataclass
class Scripted:
    """The span and version probe ports: one scripted result."""

    result: P.ProbeResult = P.PASSED
    calls: list[str] = field(default_factory=list)

    async def test_span_write(self, position_ids: Sequence[str]) -> P.ProbeResult:
        self.calls.append("span:" + ",".join(position_ids))
        return self.result

    async def probe(self, scope_id: str) -> P.ProbeResult:
        self.calls.append(scope_id)
        return self.result


@dataclass
class Circuits:
    open: list[str] = field(default_factory=list)

    def open_circuits(self) -> list[str]:
        return list(self.open)


@dataclass
class IndView:
    states: dict[str, IndicatorStatus] = field(default_factory=dict)

    def status(self, indicator_id: str) -> IndicatorStatus | None:
        return self.states.get(indicator_id)


@dataclass
class Hooks:
    cleared_scopes: list[B.ScopeRef] = field(default_factory=list)

    def cleared(self, scope: B.ScopeRef) -> None:
        self.cleared_scopes.append(scope)


@dataclass
class Notices:
    got: list[tuple[str, str, str, int]] = field(default_factory=list)

    def failed_cleared(self, failure_id: str, scope_kind: str, scope_id: str, at_ms: int) -> None:
        self.got.append((failure_id, scope_kind, scope_id, at_ms))


class AlwaysPassTrial:
    def __init__(self) -> None:
        self.calls = 0

    async def trial(self) -> P.ProbeResult:
        self.calls += 1
        return P.PASSED


@dataclass
class Clearing:
    """A FailedRig whose manager has the S30b ports wired."""

    rig: FailedRig
    series: FakeSeriesProbe
    span: Scripted
    versions: Scripted
    circuits: Circuits
    hooks: Hooks
    notices: Notices
    clear: Any
    indicators: Any = None
    trial: Any = None
    hung: Any = None
    next_request: int = 100
    budget_store: Any = None
    """S38f: the durable error-budget counters (None: in memory only)."""

    @property
    def m(self) -> F.FailedManager:
        return self.rig.manager

    def build(self, *, probe_timeout_ms: int = 60_000) -> F.FailedManager:
        r = self.rig
        r.manager = F.FailedManager(
            budget=r.budget, stores=F.FailedStores(arm=r.arm, engine=r.engine), positions=r.positions,
            spool=r.spool, pager=r.mirror.pager(), clock=r.clock, epoch=lambda: EPOCH, fail=r.budget.settings,
            settings=F.FailedSettings(max_positions=3, probe_timeout_ms=probe_timeout_ms), answers=r.answers,
            probes=P.ClearProbes(series=self.series, indicators=self.indicators, span=self.span,
                                 versions=self.versions, trial=self.trial, circuits=self.circuits,
                                 hung=self.hung),
            clear=self.clear, hooks=self.hooks, cleared_notice=self.notices, budget_store=self.budget_store)
        return r.manager

    def restart(self) -> F.FailedManager:
        r = self.rig
        r.budget = B.ErrorBudget(r.budget.settings, calendar=r.calendar, health=r.health, pg=r.pg)
        return self.build()

    def request(self, target: str, *, epoch: int | None = None) -> int:
        self.next_request += 1
        self.rig.emu.add_request(self.next_request, kind="arm_clear_failed", target=target, epoch=epoch)
        assert self.m.submit_clear(self.next_request, target) is True
        return self.next_request

    async def run(self, *, between: Any = None) -> None:
        """One tick (starts the probe), let the probe task run (`between` drives e.g. the md-writer step), one tick
        (finishes it)."""
        await self.m.tick()
        for _ in range(5):
            await asyncio.sleep(0)
        if between is not None:
            await between()
            for _ in range(5):
                await asyncio.sleep(0)
        await self.m.tick()

    def answer(self, request_id: int) -> tuple[str, str]:
        (got,) = [(s, why) for rid, s, why in self.rig.answers.got if rid == request_id]
        return got

    def episode(self, scope_kind: str, scope_id: str) -> dict[str, Any]:
        (f,) = [f for f in self.rig.emu.t.failures.values() if f["scope"] == (scope_kind, scope_id)
                and f["cleared_at_ms"] is None] or [None]
        return f  # type: ignore[no-any-return]

    def states(self) -> dict[str, tuple[str, str | None, str | None]]:
        return {k.position_id: (v.state, v.prior_state, v.state_reason) for k, v in self.rig.positions.rows.items()}


async def clearing(*, fail: FailSettings | None = None, rows: tuple[Any, ...] | None = None,
                   probe_timeout_ms: int = 60_000, budget_store: bool = False) -> Clearing:
    rig = failed_rig(fail=fail)
    rig.add(*(rows if rows is not None else
              (row("P1", symbol="SYN.A"), row("P2", symbol="SYN.A"), row("P3", symbol="SYN.B"))))
    db = EngineDb(rig.emu, lambda: Epoch("c", EPOCH))
    c = Clearing(rig=rig, series=FakeSeriesProbe(), span=Scripted(), versions=Scripted(), circuits=Circuits(),
                 hooks=Hooks(), notices=Notices(), clear=PgArmFailedClear(db), trial=AlwaysPassTrial(),
                 budget_store=PgArmBudgetStore(db) if budget_store else None)
    c.build(probe_timeout_ms=probe_timeout_ms)
    assert await c.m.start()
    await c.m.tick()
    return c


# ---------------------------------------------------------------------------------------------------- SERIES
async def test_a_series_clear_is_refused_while_the_cause_is_present_and_accepted_after() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now, counters=(("invalid_buckets", 3),))
    await c.m.tick()
    assert c.states()["P1"] == ("FAILED", "TRACKING", "scope_failed")
    c.series.results[S_A.canonical] = P.refused_probe("bars_invalid", "md_backfill")
    rid = c.request(f"SERIES:{S_A.canonical}")
    assert r.answers.got == []                                         # non-blocking: queued only
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_bars_invalid_first_md_backfill")
    assert c.series.calls == [("reread", S_A.canonical)]               # the recorded cause's probe
    ep = c.episode("SERIES", S_A.canonical)
    assert ep["last_probe_result"] == "refused:bars_invalid;first:md_backfill"
    assert c.m.covers(ADAPTER, [S_A.canonical], []) and c.states()["P1"][0] == "FAILED"
    assert c.hooks.cleared_scopes == [] and c.notices.got == []
    # the cause is fixed: the next clear is accepted
    del c.series.results[S_A.canonical]
    r.advance(60_000)
    rid2 = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid2) == ("applied", "")
    (f,) = r.emu.t.failures.values()
    assert f["cleared_at_ms"] == r.clock.now and f["clear_request_id"] == rid2
    assert f["last_probe_result"] == "passed"
    assert not c.m.covers(ADAPTER, [S_A.canonical], []) and c.m.summary().payload() == {"count": 0,
                                                                                         "global_failed": False,
                                                                                         "unrecorded": 0}
    assert c.states() == {"P1": ("TRACKING", None, "failed_cleared"), "P2": ("TRACKING", None, "failed_cleared"),
                          "P3": ("TRACKING", None, None)}
    assert c.hooks.cleared_scopes == [B.series_scope(S_A)]
    assert c.notices.got == [(f["failure_id"], "SERIES", S_A.canonical, r.clock.now)]
    assert c.m.counters.clears_applied == 1 and c.m.counters.clears_refused == 1
    # the counters were reset: a new fault opens a new episode with its own ping
    r.advance(60_000)
    r.budget.series_fault(S_A, SOURCE_MISMATCH, r.clock.now)
    await c.m.tick()
    assert len(r.emu.t.failures) == 2 and len(r.critical_rows()) == 2
    assert c.states()["P1"] == ("FAILED", "TRACKING", "scope_failed")


async def test_the_unresolved_streak_is_reset_by_the_clear() -> None:
    c = await clearing(fail=FailSettings(unresolved_n=2))
    r = c.rig
    for k in (1, 2):
        r.budget.note_unresolved(S_A, T0 + k * 900_000, r.clock.now - 1_000, r.clock.now)
    await c.m.tick()
    assert c.episode("SERIES", S_A.canonical)["probe_kind"] == "fresh_bar"
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("applied", "") and c.series.calls == [("fresh_bar", S_A.canonical)]
    assert r.budget.unresolved_streak(S_A) == 0


async def test_the_probe_never_blocks_the_loop_and_a_second_clear_waits_for_it() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()
    c.series.hold = asyncio.Event()                                    # md-bars' read hangs
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.m.probing() == (f"SERIES:{S_A.canonical}",)
    r.advance(1_000)
    r.budget.series_fault(S_B, INVALID_BAR, r.clock.now)               # the loop goes on meanwhile
    await c.m.tick()
    assert len(r.critical_rows()) == 2 and c.states()["P3"][0] == "FAILED"
    rid2 = c.request(f"SERIES:{S_A.canonical}")
    await c.m.tick()
    assert c.answer(rid2) == ("refused", "probe_running")
    c.series.hold.set()
    await c.run()
    assert c.answer(rid) == ("applied", "") and c.m.probing() == ()


async def test_a_probe_past_its_timeout_is_refused() -> None:
    c = await clearing(probe_timeout_ms=20)
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()
    c.series.hold = asyncio.Event()
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.m.tick()
    await asyncio.sleep(0.1)
    await c.m.tick()
    assert c.answer(rid) == ("refused", "cause_present_probe_timeout")
    assert c.m.covers(ADAPTER, [S_A.canonical], [])


async def test_a_raising_probe_is_refused_and_the_scope_stays_failed() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()

    async def boom(series: str) -> P.ProbeResult:
        raise RuntimeError("md-bars")

    c.series.reread = boom  # type: ignore[method-assign]
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_probe_error") and c.m.counters.probe_errors == 1


async def test_a_ports_own_timeout_is_a_probe_error_not_the_probe_timeout() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()

    async def read_timeout(series: str) -> P.ProbeResult:
        raise TimeoutError("broker read")

    c.series.reread = read_timeout  # type: ignore[method-assign]
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_probe_error") and c.m.counters.probe_errors == 1


async def test_a_fault_of_the_scope_during_its_probe_refuses_the_clear() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()
    c.series.hold = asyncio.Event()
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    r.advance(1_000)
    r.budget.series_fault(S_A, SOURCE_MISMATCH, r.clock.now)          # new evidence while the probe runs
    await c.m.tick()
    assert c.m.counters.duplicates == 1
    c.series.hold.set()                                                # the probe passes, but too late
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_fault_during_probe")
    assert c.episode("SERIES", S_A.canonical)["last_probe_result"] == "refused:fault_during_probe"
    assert c.m.covers(ADAPTER, [S_A.canonical], []) and c.hooks.cleared_scopes == []
    rid2 = c.request(f"SERIES:{S_A.canonical}")                       # a probe started after it may clear
    await c.run()
    assert c.answer(rid2) == ("applied", "")


async def test_a_probe_that_ignores_its_cancellation_is_abandoned_by_the_loop() -> None:
    c = await clearing(probe_timeout_ms=20)
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()
    release = asyncio.Event()

    async def stubborn(series: str) -> P.ProbeResult:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                pass                                                   # swallowed: the task's own deadline is lost
        return P.PASSED

    c.series.reread = stubborn  # type: ignore[method-assign]
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.m.tick()
    await asyncio.sleep(0.1)
    await c.m.tick()
    assert c.m.probing() == (f"SERIES:{S_A.canonical}",) and r.answers.got == []
    r.advance(20 + F.FailedSettings().probe_grace_ms + 1)
    await c.m.tick()                                                   # the loop's backstop
    assert c.answer(rid) == ("refused", "cause_present_probe_timeout") and c.m.probing() == ()
    assert c.m.covers(ADAPTER, [S_A.canonical], [])
    release.set()
    for _ in range(5):
        await asyncio.sleep(0)
    await c.m.tick()
    assert c.answer(rid) == ("refused", "cause_present_probe_timeout")  # answered once
    assert c.m.covers(ADAPTER, [S_A.canonical], [])


# ---------------------------------------------------------------------------------------------------- ADAPTER
async def test_an_adapter_clear_runs_the_probe_of_each_of_its_failed_series() -> None:
    c = await clearing(fail=FailSettings(global_series_n=2))
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    r.budget.series_fault(S_B, INDICATOR_FAULTED, r.clock.now)        # two series: the ADAPTER scope fails
    await c.m.tick()
    await c.m.tick()
    assert c.episode("ADAPTER", ADAPTER)["probe_kind"] == "adapter_series"
    c.indicators = P.IndicatorStateProbe(IndView(), lambda s: ["atr:x"])   # S_B's indicator is not tracked
    c.build()
    assert await c.m.start()
    await c.m.tick()
    rid = c.request(f"ADAPTER:{ADAPTER}")
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_indicator_not_tracked")
    assert c.series.calls == [("reread", S_A.canonical)]               # S_A's own probe ran first and passed
    c.indicators = P.IndicatorStateProbe(
        IndView({"atr:x": IndicatorStatus("atr:x", IndState.READY, True, T0, 0, "")}), lambda s: ["atr:x"])
    c.build()
    assert await c.m.start()
    await c.m.tick()
    rid2 = c.request(f"ADAPTER:{ADAPTER}")
    await c.run()
    assert c.answer(rid2) == ("applied", "")
    # the series keep their own episodes: their positions stay FAILED until each is cleared
    assert c.m.covers(ADAPTER, [S_A.canonical], []) and c.states()["P1"][0] == "FAILED"
    assert not c.m.scopes.is_open(B.ScopeRef(ScopeKind.ADAPTER, ADAPTER))
    for s in (S_A, S_B):
        rid3 = c.request(f"SERIES:{s.canonical}")
        await c.run()
        assert c.answer(rid3) == ("applied", "")
    assert {p: s[0] for p, s in c.states().items()} == {"P1": "TRACKING", "P2": "TRACKING", "P3": "TRACKING"}


async def test_an_adapter_probe_runs_its_series_probes_at_the_same_time() -> None:
    c = await clearing(fail=FailSettings(global_series_n=2, unresolved_n=2), probe_timeout_ms=400)
    r = c.rig
    for s in (S_A, S_B):
        for k in (1, 2):
            r.budget.note_unresolved(s, T0 + k * 900_000, r.clock.now - 1_000, r.clock.now)
    await c.m.tick()
    await c.m.tick()
    assert c.episode("ADAPTER", ADAPTER)["probe_kind"] == "adapter_series"
    assert c.episode("SERIES", S_A.canonical)["probe_kind"] == "fresh_bar"
    c.series.delay = 0.25                       # each fresh-bar read takes more than half the probe's timeout
    rid = c.request(f"ADAPTER:{ADAPTER}")
    for _ in range(100):
        await c.m.tick()
        if r.answers.got:
            break
        await asyncio.sleep(0.02)
    assert c.answer(rid) == ("applied", "")
    assert sorted(c.series.calls) == [("fresh_bar", S_A.canonical), ("fresh_bar", S_B.canonical)]


async def test_an_adapter_clear_keeps_a_deferred_fault_of_another_series() -> None:
    c = await clearing(fail=FailSettings(global_series_n=2))
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    r.budget.series_fault(S_B, INVALID_BAR, r.clock.now)
    await c.m.tick()
    await c.m.tick()
    r.calendar.closed.add((ADAPTER, "SYN.C"))
    r.budget.series_fault(S_C, INVALID_BAR, r.clock.now)              # deferred: SYN.C's market is closed
    assert r.budget.deferred() == (S_C.canonical,)
    rid = c.request(f"ADAPTER:{ADAPTER}")
    await c.run()
    assert c.answer(rid) == ("applied", "")
    assert r.budget.deferred() == (S_C.canonical,)                     # the clear kept it
    r.calendar.closed.clear()                                          # the pause ends: it is filed
    r.advance(1_000)
    await c.m.tick()
    assert r.budget.deferred() == () and c.m.scopes.is_open(B.series_scope(S_C))
    assert c.episode("SERIES", S_C.canonical)["reason"] == INVALID_BAR


# ---------------------------------------------------------------------------------------------------- indicator
async def test_an_indicator_faulted_clear_is_refused_until_md_rebuild() -> None:
    view = IndView({"atr:x": IndicatorStatus("atr:x", IndState.FAULTED, False, T0, 3, "raised")})
    c = await clearing()
    c.indicators = P.IndicatorStateProbe(view, lambda s: ["atr:x"] if s == S_A.canonical else [])
    c.build()
    assert await c.m.start()
    r = c.rig
    r.budget.series_fault(S_A, INDICATOR_FAULTED, r.clock.now)
    await c.m.tick()
    assert c.episode("SERIES", S_A.canonical)["probe_kind"] == "indicator_rebuild"
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_indicator_faulted_first_md_rebuild")
    view.states["atr:x"] = IndicatorStatus("atr:x", IndState.PENDING, False, T0, 0, "")   # md rebuild running
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_indicator_not_warm")
    view.states["atr:x"] = IndicatorStatus("atr:x", IndState.READY, True, T0, 0, "")      # rebuilt and warm
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("applied", "")


async def test_the_indicator_probe_needs_an_indicator_on_the_series() -> None:
    probe = P.IndicatorStateProbe(IndView(), lambda s: [])
    assert await probe.rebuilt_and_warm(S_A.canonical) == P.refused_probe("indicator_unknown")


# ---------------------------------------------------------------------------------------------------- STRATEGY / span
async def test_a_strategy_version_clear_resets_the_job_runners_circuit() -> None:
    c = await clearing()
    jobs = JobRunner()
    for p in ("mock:P1", "mock:P2", "mock:P1"):
        jobs.circuit.fault(VERSION, p)
    assert jobs.circuit.tripped(VERSION)
    c.hooks = P.ScopeClearHooks(version=jobs.reset)  # type: ignore[assignment]
    c.build()
    assert await c.m.start()
    r = c.rig
    r.budget.version_fault(VERSION, 3, ("mock:P1",), r.clock.now, edge_code="default")
    await c.m.tick()
    assert c.states()["P1"][0] == "FAILED" and c.states()["P3"][0] == "FAILED"     # both pin the version
    c.versions.result = P.refused_probe("conformance", "arm_repin")
    rid = c.request(f"STRATEGY_VERSION:{VERSION}")
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_conformance_first_arm_repin")
    assert jobs.circuit.tripped(VERSION)
    c.versions.result = P.PASSED
    rid = c.request(f"STRATEGY_VERSION:{VERSION}")
    await c.run()
    assert c.answer(rid) == ("applied", "") and not jobs.circuit.tripped(VERSION)
    assert c.versions.calls == [VERSION, VERSION]


async def test_a_stuck_frontier_clear_needs_a_test_span_write() -> None:
    c = await clearing()
    r = c.rig
    c.m.enter(B.Fault(scope=B.ARM_SCOPE, reason=STUCK_FRONTIER, first_fault_at_ms=r.clock.now,
                      counters=(("stuck_positions", 1),), position_ids=("mock:P1",)))
    await c.m.tick()
    assert c.episode("ARM", "arm")["probe_kind"] == "span_write"
    c.span.result = P.refused_probe("span_write_failed")
    rid = c.request("ARM:arm")
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_span_write_failed")
    c.span.result = P.PASSED
    rid = c.request("ARM:arm")
    await c.run()
    # the probe re-runs the span write of the recorded positions (the ARM episode lists every covered one)
    assert c.answer(rid) == ("applied", "") and c.span.calls == ["span:mock:P1,mock:P2,mock:P3"] * 2
    assert c.trial.calls == 0                                          # the span probe, not the global one


async def test_a_position_stuck_frontier_fails_only_that_position_and_its_probe_retests_it() -> None:
    """Round 12 / S38c: a stuck frontier on one position is a POSITION scope: it covers only that position, its
    clear probe re-runs the span write of exactly the recorded position, and the clear is refused while the row is
    still poisoned."""
    c = await clearing()
    r = c.rig
    c.m.enter(B.Fault(scope=B.position_scope("mock:P1"), reason=STUCK_FRONTIER, first_fault_at_ms=r.clock.now,
                      counters=(("stuck_positions", 1),), position_ids=("mock:P1",)))
    await c.m.tick()
    ep = c.episode("POSITION", "mock:P1")
    assert ep["probe_kind"] == "span_write" and not c.m.scopes.global_failed
    assert c.m.covers("mock", (), (), "mock:P1") and not c.m.covers("mock", (), (), "mock:P2")
    c.span.result = P.refused_probe("span_write_failed")             # the poisoned row still fails
    rid = c.request("POSITION:mock:P1")
    await c.run()
    assert c.answer(rid) == ("refused", "cause_present_span_write_failed")
    c.span.result = P.PASSED                                           # the row was fixed
    rid = c.request("POSITION:mock:P1")
    await c.run()
    assert c.answer(rid) == ("applied", "") and c.span.calls == ["span:mock:P1", "span:mock:P1"]
    assert not c.m.covers("mock", (), (), "mock:P1") and c.trial.calls == 0


# ---------------------------------------------------------------------------------------------------- ARM global
async def started_md(md: Any, ids: RunIds) -> Any:
    rig = await make_rig(md, ids, start=T0)
    await rig.at(T0)                                                   # the term-start bootstrap
    return rig


async def test_after_the_aof_aside_runbook_step_a_global_clear_succeeds(md_redis: Any, run_ids: RunIds) -> None:
    md = await started_md(md_redis, run_ids)
    c = await clearing()
    c.trial = P.TrialBootstrapProbe(md.svc)
    c.hooks = P.ScopeClearHooks(post_clear=md.svc.request_post_clear)  # type: ignore[assignment]
    c.build()
    assert await c.m.start()
    r = c.rig
    r.budget.store_fault("bootstrap_failed", r.clock.now)              # the store could not be loaded
    await c.m.tick()
    assert c.episode("ARM", "arm")["probe_kind"] == "arm_global"
    assert c.states()["P1"][0] == "FAILED" and c.states()["P3"][0] == "FAILED"

    async def md_step() -> None:
        md.clock.now += 1_000
        await md.svc.step()

    # a task circuit still open: refused, the request to run first named
    c.circuits.open = ["md-bars"]
    rid = c.request("ARM:arm")
    await c.run(between=md_step)
    assert c.answer(rid) == ("refused", "cause_present_circuit_open_first_reset_task")
    c.circuits.open = []
    # the ARM pool still failing: refused
    r.arm.down = True
    rid = c.request("ARM:arm")
    await c.run(between=md_step)
    assert c.answer(rid) == ("refused", "cause_present_arm_pool")
    r.arm.down = False
    # the runbook: the AOF is moved aside and redis-md restarts with an empty store
    await md_redis.flushall()
    md.restart_redis("f0e1d2c3b4")
    assert await md_redis.get(K.BOOT) is None
    resets_before = len(md.resets)
    rid = c.request("ARM:arm")
    await c.run(between=md_step)
    assert c.answer(rid) == ("applied", "")
    assert await md_redis.get(K.BOOT) is not None                      # the trial bootstrap wrote md:boot
    trial = md.resets[resets_before]
    assert trial.reason.value == "trial" and not trial.notice and not trial.counted
    assert md.svc.pending_reason is not None and md.svc.pending_reason.value == "post_clear"
    assert md.boot.budget.in_window(md.clock.now) == 0 and md.faults == []
    assert not c.m.scopes.global_failed and c.m.summary().payload() == {"count": 0, "global_failed": False,
                                                                         "unrecorded": 0}
    assert {p: s[0] for p, s in c.states().items()} == {"P1": "TRACKING", "P2": "TRACKING", "P3": "TRACKING"}
    assert c.notices.got and c.notices.got[0][1:3] == ("ARM", "arm")
    # the post-clear bootstrap runs in the writer task: never counted, no MD_RESET notice
    await md_step()
    post = md.resets[-1]
    assert post.reason.value == "post_clear" and not post.counted and not post.notice
    assert md.svc.pending_reason is None
    assert md.boot.budget.in_window(md.clock.now) == 0 and md.faults == []


async def test_a_restart_while_globally_failed_with_an_empty_redis_md_can_still_be_cleared(
        md_redis: Any, run_ids: RunIds) -> None:
    c = await clearing()
    r = c.rig
    r.budget.observe_md(usable=False, market_open=True, now_ms=r.clock.now)
    for _ in range(31):
        r.advance(30_000)
        r.budget.observe_md(usable=False, market_open=True, now_ms=r.clock.now)
    await c.m.tick()
    assert c.episode("ARM", "arm")["reason"] == "md_down"
    # the engine restarts; redis-md comes back empty (no term-start bootstrap has run yet)
    md = await make_rig(md_redis, run_ids, start=r.clock.now)
    assert await md_redis.get(K.BOOT) is None
    c.trial = P.TrialBootstrapProbe(md.svc)
    m = c.restart()
    assert m.covers(ADAPTER, [S_C.canonical], [])                     # nothing read yet: advises nothing
    assert await m.start()
    assert m.scopes.global_failed
    await m.tick()

    async def md_step() -> None:
        await md.at(r.clock.now)

    rid = c.request("ARM:arm")
    await c.run(between=md_step)
    assert c.answer(rid) == ("applied", "")
    assert await md_redis.get(K.BOOT) is not None and not m.scopes.global_failed
    assert c.states()["P1"] == ("TRACKING", None, "failed_cleared")


async def test_the_arm_probe_refuses_a_failed_trial_with_the_writer_reset_first() -> None:
    class Failing:
        def request_trial(self, done: Any) -> None:
            from src.marketdata.bootstrap import BootReason, BootResult
            from src.marketdata.types import MdGeneration
            done(BootResult(ok=False, reason=BootReason.TRIAL, generation=MdGeneration(7, 2), failure="lock_busy"))

    assert await P.TrialBootstrapProbe(Failing()).trial() == P.refused_probe("trial_bootstrap", "md_writer_reset")


# ---------------------------------------------------------------------------------------------------- targets
async def test_targets_and_requests_that_do_not_match_are_answered_without_a_clear() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()
    assert c.m.submit_clear(1, "nonsense") and c.m.submit_clear(2, "BOGUS:x") and c.m.submit_clear(3, "SERIES:")
    assert c.m.submit_clear(4, f"SERIES:{S_B.canonical}")             # not FAILED
    assert c.m.submit_clear(5, f"SERIES:{S_A.canonical}")             # no audited request row behind it
    rid = c.request(f"SERIES:{S_A.canonical}", epoch=EPOCH - 1)       # filed before a takeover
    await c.run()
    got = {rid_: (s, why) for rid_, s, why in r.answers.got}
    assert got[1] == got[2] == got[3] == ("refused", "bad_target")
    assert got[4] == ("moot", "not_failed")
    assert got[5] == ("refused", "request_mismatch")                   # probed, but the clear statement refuses
    assert got[rid] == ("refused", "probe_running")                     # one probe per scope at a time
    rid = c.request(f"SERIES:{S_A.canonical}", epoch=EPOCH - 1)
    await c.run()
    assert c.answer(rid) == ("refused", "request_mismatch")             # another term's request
    assert c.episode("SERIES", S_A.canonical) is not None and c.m.covers(ADAPTER, [S_A.canonical], [])
    assert c.hooks.cleared_scopes == [] and c.notices.got == []


async def test_a_clear_of_an_episode_not_yet_recorded_is_refused() -> None:
    c = await clearing()
    r = c.rig
    r.arm.down = r.engine.down = True
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.m.tick()
    assert c.answer(rid) == ("refused", "not_recorded")


async def test_a_lost_fence_in_the_clear_raises() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()

    class Fenced:
        async def clear(self, request_id: int, failure_id: str, at_ms: int) -> ClearedFailure | None:
            raise EpochMismatch("fenced")

    c.clear = Fenced()
    c.build()
    assert await c.m.start()
    c.request(f"SERIES:{S_A.canonical}")
    with pytest.raises(EpochMismatch):
        await c.run()


async def test_a_failing_clear_write_is_refused_and_the_scope_stays_failed() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()

    class Down:
        async def clear(self, request_id: int, failure_id: str, at_ms: int) -> ClearedFailure | None:
            raise ConnectionRefusedError("engine pool")

    c.clear = Down()
    c.build()
    assert await c.m.start()
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("refused", "clear_write_failed") and c.m.covers(ADAPTER, [S_A.canonical], [])


async def test_drain_interrupts_a_running_probe() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()
    c.series.hold = asyncio.Event()
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    queued = c.request(f"SERIES:{S_A.canonical}")
    await c.m.drain()
    assert c.answer(rid) == ("refused", "probe_interrupted") and c.answer(queued) == ("refused", "probe_interrupted")
    assert c.m.probing() == ()


# ---------------------------------------------------------------------------------------------------- restore
async def test_the_restore_is_retried_after_a_cas_miss_and_done_by_the_next_term() -> None:
    c = await clearing()
    r = c.rig
    r.budget.series_fault(S_A, INVALID_BAR, r.clock.now)
    await c.m.tick()
    r.positions.miss_next = 1                                          # a concurrent writer bumps P1 once
    rid = c.request(f"SERIES:{S_A.canonical}")
    await c.run()
    assert c.answer(rid) == ("applied", "")
    assert c.states()["P1"][0] == "FAILED" and c.states()["P2"][0] == "TRACKING"
    r.advance(6_000)
    await c.m.tick()
    assert c.states()["P1"] == ("TRACKING", None, "failed_cleared")
    # a position left FAILED with no open episode (a clear committed just before a takeover) is restored by the
    # next term once it has read the open episodes
    p3 = r.positions.row("P3")
    r.positions.rows[p3.key] = replace(p3, state="FAILED", prior_state="ARMED", state_reason="scope_failed")
    m = c.restart()
    assert await m.start()
    await m.tick()
    assert c.states()["P3"] == ("ARMED", None, "failed_cleared")


async def test_a_failed_position_without_a_valid_prior_state_stays_failed() -> None:
    c = await clearing(rows=(row("P1", symbol="SYN.A", state="FAILED"),))
    await c.m.tick()
    await c.m.tick()
    assert c.states()["P1"][0] == "FAILED" and c.m.counters.restore_refused == 1     # logged once


# ---------------------------------------------------------------------------------------------------- pure
def test_probe_kinds_match_the_budgets_table() -> None:
    assert set(B.PROBE_KIND_BY_REASON.values()) | {B.ADAPTER_PROBE, B.ARM_GLOBAL_PROBE} == P.PROBE_KINDS
    assert B.probe_kind(ScopeKind.ARM, "md_down") == P.ARM_GLOBAL
    assert B.probe_kind(ScopeKind.ADAPTER, "series_failed") == P.ADAPTER_SERIES


def test_probe_results_are_codes_only() -> None:
    assert P.refusal_reason(P.refused_probe("indicator_faulted", "md_rebuild")) == \
        "cause_present_indicator_faulted_first_md_rebuild"
    assert P.probe_note(P.PASSED) == "passed"
    assert P.probe_note(P.refused_probe("arm_pool")) == "refused:arm_pool"
    for bad in ("", "1.2345", "Bars", "has space"):
        with pytest.raises(ValueError):
            P.refused_probe(bad)
    with pytest.raises(ValueError):
        P.refused_probe("x", "rm_rf")                                  # only the known requests may be named
    with pytest.raises(ValueError):
        P.ProbeResult(True, "x")
    longest = max(P.FIRST_COMMANDS, key=len)
    assert len(P.refusal_reason(P.refused_probe("z" * 24, longest))) <= 64


def test_targets_parse() -> None:
    assert F.parse_target(f"SERIES:{S_A.canonical}") == B.series_scope(S_A)
    assert F.parse_target("ARM:arm") == B.ARM_SCOPE
    assert F.parse_target(f"ADAPTER:{ADAPTER}") == B.ScopeRef(ScopeKind.ADAPTER, ADAPTER)
    for bad in ("", "ARM", "arm:arm", "SERIES:", "SERIES:a b", f"SERIES:{S_A.canonical} ", " ARM:arm",
                "ARM:arm\n"):
        assert F.parse_target(bad) is None


def test_scope_clear_hooks_dispatch_by_kind() -> None:
    seen: list[tuple[str, str]] = []
    hooks = P.ScopeClearHooks(series=lambda s: seen.append(("series", s)), adapter=lambda a: seen.append(("ad", a)),
                              version=lambda v: seen.append(("version", v)), post_clear=lambda: seen.append(("arm", "")))
    for scope in (B.series_scope(S_A), B.ScopeRef(ScopeKind.ADAPTER, ADAPTER),
                  B.ScopeRef(ScopeKind.STRATEGY_VERSION, VERSION), B.ARM_SCOPE):
        hooks.cleared(scope)
    assert seen == [("series", S_A.canonical), ("ad", ADAPTER), ("version", VERSION), ("arm", "")]
