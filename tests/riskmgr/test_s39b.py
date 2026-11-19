"""S39b: the six engineering go-live gates (plan stage S39b; design §0b "Go-live gates").

* `uptime_suspend`: a strategy job whose run spans a PC sleep is never a timeout or a strategy fault; the bars the
  sleep covered are spanned `DOWNTIME`.
* `corrections_restart`: the BarClock rebuilds its D4 correction window from the committed decisions after a takeover,
  so a bar revised mid-window still gives a CORRECTED item (and nothing already sent is sent again).

The other four gates have their own files (`tests/marketdata/test_s39b_receipt_gaps.py`,
`tests/riskmgr/test_s39b_rerun.py`, `tests/riskmgr/test_s39b_vectors.py`, `tests/riskmgr/test_s39b_entry_time.py`).
Synthetic ids, levels and times only; no assertion message carries a value.
"""
from __future__ import annotations

import asyncio
import threading
from dataclasses import replace

import pytest

from src.db.arm_ports import DecisionRef
from src.riskmgr import barclock as barclock_mod

from src.marketdata.timing import COLD_SKEW, decision_cap_ms
from src.marketdata.types import M15
from src.riskmgr.barclock import EvalRequest, Evaluated, NotReady, Prepared
from src.riskmgr.golive import BY_ID, GateState
from src.riskmgr.jobs import JOB_SUSPENDED, JOB_TIMEOUT, JobRunner, JobSpec
from src.riskmgr.outcomes import DOWNTIME
from src.riskmgr.sm11 import ArmState, Sm11Event
from src.riskmgr.guard import strategy_scope_id
from tests.riskmgr.notice_fakes import Stored
from tests.riskmgr.barclock_fakes import T1, T2, FakeEval, bc_rig, decided
from tests.riskmgr.tracker_fakes import ADAPTER
from tests.riskmgr.test_arm_join import join_rig
from tests.riskmgr.test_round13 import _to_complete

A = strategy_scope_id("toy_a", 1)


# ---------------------------------------------------------------------------------------------------- uptime_suspend
async def _run_with(runner: JobRunner, job: object, during: object) -> object:
    task = asyncio.create_task(runner.run(JobSpec(A), "P1", job))  # type: ignore[arg-type]
    await during()  # type: ignore[operator]
    return await task


async def test_a_job_that_spans_a_sleep_and_then_finishes_is_a_normal_result() -> None:
    release = threading.Event()
    runner = JobRunner(cap_ms=200)

    def job() -> int:
        release.wait(5)
        return 7

    async def during() -> None:
        await asyncio.sleep(0.05)
        runner.note_suspended()          # the uptime tracker's signal while the job runs
        await asyncio.sleep(0.3)         # the first cap expires (across the "sleep"): a fresh awake cap starts
        release.set()

    try:
        result = await _run_with(runner, job, during)
    finally:
        release.set()
    assert result.ok and result.value == 7                                    # type: ignore[attr-defined]
    assert runner.circuit.streak(A) == 0 and runner.take_signals() == [] and runner.suspended_jobs == 1


async def test_a_job_that_spans_a_sleep_and_raises_is_never_a_fault() -> None:
    release = threading.Event()
    runner = JobRunner(cap_ms=200)

    def job() -> int:
        release.wait(5)
        raise RuntimeError("boom")

    async def during() -> None:
        await asyncio.sleep(0.05)
        runner.note_suspended()
        release.set()

    for _ in range(4):                   # more than the fault limit: never a STRATEGY_VERSION signal
        release.clear()
        try:
            result = await _run_with(runner, job, during)
        finally:
            release.set()
        assert result.reason == JOB_SUSPENDED and result.suspended and result.fault_scope == ""   # type: ignore
    assert runner.circuit.streak(A) == 0 and runner.take_signals() == [] and not runner.circuit.tripped(A)


async def test_the_runner_sees_a_sleep_itself_from_the_monotonic_jump() -> None:
    """Without the uptime tracker's signal: the cap's timer fired far later than the cap (Windows' monotonic clock
    counts through a sleep), so the job spanned a suspend."""
    release = threading.Event()
    now = [1_000.0]
    runner = JobRunner(cap_ms=200, suspend_after_ms=10_000, monotonic=lambda: now[0])

    def job() -> int:
        release.wait(5)
        raise RuntimeError("boom")

    async def during() -> None:
        await asyncio.sleep(0.05)
        now[0] += 600.0                  # ten minutes asleep
        await asyncio.sleep(0.25)
        release.set()

    try:
        result = await _run_with(runner, job, during)
    finally:
        release.set()
    assert result.reason == JOB_SUSPENDED and runner.circuit.streak(A) == 0         # type: ignore[attr-defined]


async def test_a_job_still_hung_a_full_awake_cap_after_the_resume_is_a_timeout() -> None:
    release = threading.Event()
    runner = JobRunner(cap_ms=100)

    def job() -> int:
        release.wait(5)
        return 1

    async def during() -> None:
        await asyncio.sleep(0.02)
        runner.note_suspended()

    try:
        result = await _run_with(runner, job, during)
        assert result.reason == JOB_TIMEOUT and runner.circuit.streak(A) == 1     # type: ignore[attr-defined]
    finally:
        release.set()
        runner.close()


class SleepyEval(FakeEval):
    """The job blocks until the test releases it; `fail` makes it raise once released."""

    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()
        self.fail = True

    async def prepare(self, request: EvalRequest) -> Prepared | NotReady:
        got = await super().prepare(request)
        if isinstance(got, NotReady) or request.event.value == "PRE":
            return got
        value = decided(events=(Sm11Event.FINAL_ADJUST,))

        def job() -> Evaluated:
            self.release.wait(5)
            if self.fail:
                raise RuntimeError("boom")
            return value

        return Prepared(got.spec, job)


async def test_a_sleep_during_a_job_is_a_missed_bar_spanned_downtime_never_a_fault() -> None:
    """The plan's test: sleep during a job. The BarClock's job spans the suspend (the runtime calls
    `JobRunner.note_suspended` from the session's suspend listener); it is not counted, the bar stays pending with
    `DOWNTIME` as its reason, and at its cap it is spanned `DOWNTIME`."""
    r = bc_rig()
    ev = SleepyEval()
    r.clock_._evaluator = ev                                              # noqa: SLF001
    jobs = r.clock_._jobs                                                 # noqa: SLF001

    async def sleep_then_wake() -> None:
        await asyncio.sleep(0.05)
        jobs.note_suspended(600_000)
        await asyncio.sleep(0.05)
        ev.release.set()

    waker = asyncio.create_task(sleep_then_wake())
    try:
        await r.step()
        await waker
        assert r.clock_.counters.jobs_suspended == 1 and r.clock_.counters.job_faults == 0
        assert jobs.circuit.streak("strategy:break_even_on_risk_close:1") == 0
        assert r.store.rows_of() == []                                    # nothing decided on the sleepy job
        cap = decision_cap_ms(T1, M15, COLD_SKEW)
        ev.release.clear()
        r.eval.rule = lambda _req: NotReady("broker_bar_not_final")      # the bar never decides again
        ev.rule = r.eval.rule
        await r.step(cap + 1)
        spans = [d for d in r.store.rows_of() if d.outcome == "UNRESOLVED_RANGE"]
        assert spans and spans[0].reason == DOWNTIME
    finally:
        ev.release.set()
        r.clock_.close()


async def test_a_suspend_makes_the_bars_it_covered_downtime_and_tells_the_jobs() -> None:
    """The BarClock's own suspend detection (or the latch): a bar that ended while the process slept, or whose cap
    passed while it slept, is spanned `DOWNTIME`, whatever an earlier pass held it for, and the job runner hears of
    the suspend."""
    r = bc_rig(rule=lambda _req: NotReady("broker_bar_not_final"))
    await r.step(T1 + 5_000)
    ref = f"{ADAPTER}:P1"
    assert r.clock_._reason[(ref, T1)] == "broker_bar_not_final"          # noqa: SLF001
    r.clock_._reason[(ref, T2)] = "md_unavailable"                        # noqa: SLF001  (a bar ending in the sleep)
    before = r.clock_._jobs.suspends                                      # noqa: SLF001
    r.clock_._suspended = lambda: True                                    # noqa: SLF001  (the uptime latch)
    await r.step(T2 + 600_000)
    assert r.clock_._jobs.suspends == before + 1                          # noqa: SLF001
    assert r.clock_._reason[(ref, T2)] == DOWNTIME                        # noqa: SLF001
    # T1 ended before the sleep, but its whole deciding window (its cap) passed during it: DOWNTIME as well
    assert decision_cap_ms(T1, M15, COLD_SKEW) <= T2 + 600_000
    assert r.clock_._reason[(ref, T1)] == DOWNTIME                        # noqa: SLF001
    r.clock_.close()


# ---------------------------------------------------------------------------------------------------- corrections_restart
async def test_a_takeover_mid_window_then_a_bar_revision_gives_a_corrected_item() -> None:
    """The plan's test: the release bar's decision is committed, the leader changes (a new BarClock with no memory),
    the bar is revised inside its window: the new term's BarClock rebuilt the window from the committed decisions and
    writes the CORRECTED revision; the POST notice already queued is not queued again."""
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        complete = [s for s in j.store.decisions if s.row.event == "POST" and s.row.bar_end_ms == b3][0]
        await j.at(b3 + 3_000)
        posts_before = len(j.outbox.bodies("POST"))
        j.new_term(recent=True)
        j.md.revise_bar(b3, "1.1950")
        await j.at(b3 + 10_000)
        corrected = [r for r in j.decisions("CORRECTED") if r.bar_end_ms == b3]
        assert [(r.revision, r.supersedes_decision_id) for r in corrected] == [(1, complete.ref.decision_id)]
        assert j.row().state == ArmState.TRACKING.value
        assert j.barclock.counters.corrections_rebuilt >= 1 and j.barclock.counters.corrections == 1
        assert len(j.outbox.bodies("CORRECTED")) == 1 and len(j.outbox.bodies("POST")) == posts_before
        await j.at(b3 + 20_000)                                           # the revision is the new recorded one
        assert len(j.decisions("CORRECTED")) == 1
    finally:
        j.barclock.close()


async def test_without_the_rebuild_the_new_term_misses_the_correction() -> None:
    """Control: the pre-S39b behaviour (no recent reader) leaves the revised bar unchecked after a takeover."""
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        j.new_term(recent=False)
        j.md.revise_bar(b3, "1.1950")
        await j.at(b3 + 10_000)
        assert j.decisions("CORRECTED") == []
    finally:
        j.barclock.close()


async def test_a_failed_rebuild_read_is_retried_and_a_window_ended_while_off_is_not_kept() -> None:
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        j.new_term(recent=True)
        j.store.fail_recent = 1
        await j.at(b3 + 6_000)                                            # the read fails: retried at the next cadence
        assert j.barclock.counters.correction_errors == 1 and j.barclock._recorded == {}   # noqa: SLF001
        await j.at(b3 + 12_000)
        assert j.store.recent_reads == 2 and len(j.barclock._recorded) == 1                # noqa: SLF001
        j.new_term(recent=True)                                           # a later takeover after the window ended
        await j.at(decision_cap_ms(b3, M15, COLD_SKEW) + 6_000)
        assert j.barclock._recorded == {} and j.barclock.counters.corrections_rebuilt == 0  # noqa: SLF001
    finally:
        j.barclock.close()


async def test_a_truncated_rebuild_read_keeps_the_newest_in_window_decision(monkeypatch: pytest.MonkeyPatch) -> None:
    """More committed finals than the read's limit (older bars of other positions): the read is newest first, so the
    live position's in-window decision is restored and the revision is still corrected."""
    j = join_rig()
    try:
        b3 = await _to_complete(j)
        complete = [s for s in j.store.decisions if s.row.event == "POST" and s.row.bar_end_ms == b3][0]
        for i in range(1, 6):                                             # older bars of closed positions
            row = replace(complete.row, key=replace(complete.row.key, position_id=f"GONE{i}"), bar_end_ms=b3 - i,
                          decision_seq=complete.row.decision_seq - i)
            ref = DecisionRef(decision_id=len(j.store.decisions) + 1, bar_end_ms=b3 - i)
            j.store.decisions.append(Stored(ref, row, notice_id=0))
        monkeypatch.setattr(barclock_mod, "REBUILD_LIMIT", 3)
        j.new_term(recent=True)
        j.md.revise_bar(b3, "1.1950")
        await j.at(b3 + 10_000)
        assert j.barclock.counters.corrections_rebuilt == 1
        corrected = [r for r in j.decisions("CORRECTED") if r.bar_end_ms == b3]
        assert [r.supersedes_decision_id for r in corrected] == [complete.ref.decision_id]
    finally:
        j.barclock.close()


async def test_a_bar_ended_before_a_short_sleep_with_its_cap_after_the_wake_keeps_its_reason() -> None:
    """Control: a bar that ended before the sleep and can still be decided after the wake keeps its own reason."""
    r = bc_rig(rule=lambda _req: NotReady("broker_bar_not_final"))
    await r.step(T1 + 5_000)
    ref = f"{ADAPTER}:P1"
    r.clock_._suspended = lambda: True                                    # noqa: SLF001  (the uptime latch)
    await r.step(T1 + 60_000)
    assert decision_cap_ms(T1, M15, COLD_SKEW) > T1 + 60_000
    assert r.clock_._reason[(ref, T1)] == "broker_bar_not_final"          # noqa: SLF001
    r.clock_.close()


# ---------------------------------------------------------------------------------------------------- the gate table
def test_the_six_gates_are_wired() -> None:
    for gate_id in ("uptime_suspend", "receipt_gap_feed", "replay_rerun", "conformance_probe_partial",
                    "entry_time_source", "corrections_restart"):
        assert BY_ID[gate_id].state is GateState.WIRED, gate_id
    assert all(BY_ID[g].detail.startswith("S39b") for g in ("uptime_suspend", "corrections_restart"))
