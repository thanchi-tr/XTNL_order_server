"""The per-version job runner (plan S22, R11-M17; design §B4 Job runner, §B14 STRATEGY_VERSION).

Planted defects: a hanging strategy (blocked on a `threading.Event`, released at the end of the test so no thread
outlives it), a raising guard version and a plain raise. Jobs here are small callables standing in for one position's
guards + `propose` + `at_target` (the BarClock, S27, hands in the pipeline run).
"""
from __future__ import annotations

import asyncio
import decimal
import subprocess
import sys
import threading
import time
from decimal import ROUND_HALF_EVEN, Context, Decimal
from pathlib import Path

import pytest

from src.riskmgr.guard import guard_scope_id, strategy_scope_id
from src.riskmgr.jobs import (
    EXECUTOR_HUNG,
    JOB_RAISED,
    JOB_TIMEOUT,
    JobRunner,
    JobSpec,
    ScopedFault,
    VersionCircuit,
)
from src.riskmgr.outcomes import Outcome
from src.riskmgr.pipeline import PipelineFault
from src.riskmgr.types import ScopeKind

A = strategy_scope_id("toy_a", 1)
B = strategy_scope_id("toy_b", 1)
C = strategy_scope_id("toy_c", 1)
G1 = guard_scope_id("toy_guard", 1)
G2 = guard_scope_id("toy_guard", 2)
CTX = Context(prec=40, rounding=ROUND_HALF_EVEN)


class GuardBoom(Exception):
    def __init__(self, scope_id: str) -> None:
        super().__init__(scope_id)
        self.scope_id = scope_id


def test_the_pipeline_fault_is_a_scoped_fault() -> None:
    assert isinstance(PipelineFault(G1), ScopedFault)
    assert not isinstance(ValueError("x"), ScopedFault)


def test_a_planted_hanging_strategy_leaves_other_versions_advising_and_the_arm_loop_stamping() -> None:
    release = threading.Event()
    stamps: list[float] = []

    async def scenario() -> None:
        runner = JobRunner()
        stop = asyncio.Event()

        async def arm_loop() -> None:
            while not stop.is_set():
                stamps.append(time.monotonic())
                await asyncio.sleep(0.01)

        loop_task = asyncio.create_task(arm_loop())
        try:
            hang = asyncio.create_task(runner.run(JobSpec(A), "pa1", lambda: release.wait(30)))
            others = [await runner.run(JobSpec(B), f"pb{i}", lambda: Decimal("1.5")) for i in range(5)]
            first = await hang
            assert first.reason == JOB_TIMEOUT and first.fault_scope == A
            assert first.code is not None and first.code.outcome is Outcome.STRATEGY_ERROR
            assert all(r.ok and r.value == Decimal("1.5") for r in others)
            # S38c: the hung lane is retired; the version gets a fresh executor (never queued behind the thread)
            assert not runner.hung(A) and not runner.hung(B) and runner.abandoned_threads() == 1
            t0 = time.monotonic()
            again = await runner.run(JobSpec(A), "pa2", lambda: 1)
            assert again.ok and again.value == 1 and time.monotonic() - t0 < 0.1
            hang2 = await runner.run(JobSpec(A), "pa3", lambda: release.wait(30))
            hang3 = await runner.run(JobSpec(A), "pa4", lambda: release.wait(30))
            hang4 = await runner.run(JobSpec(A), "pa5", lambda: release.wait(30))
            assert hang2.reason == hang3.reason == hang4.reason == JOB_TIMEOUT
            later = await runner.run(JobSpec(B), "pb9", lambda: 2)
            assert later.ok and later.value == 2
            signals = runner.take_signals()
            assert [(s.scope_id, s.faults, s.scope_kind) for s in signals] == [(A, 3, ScopeKind.STRATEGY_VERSION)]
            assert signals[0].position_ids == ("pa3", "pa4", "pa5")
            assert runner.take_signals() == []
            # a fourth hang (pa1 plus three) is past the bound of 3: kept, and restart_required is due once
            assert runner.abandoned_threads() == 3 and runner.hung(A) and runner.take_restart_required() == 4
        finally:
            release.set()
            stop.set()
            await loop_task
            runner.close()

    asyncio.run(scenario())
    gaps = [b - a for a, b in zip(stamps, stamps[1:], strict=False)]
    assert len(stamps) >= 10 and max(gaps) < 1.0           # the loop kept stamping through the 30 s hang


def test_a_planted_raising_guard_version_faults_only_the_positions_pinning_it() -> None:
    def job_on(guard: str) -> object:
        def run() -> str:
            if guard == G1:
                raise GuardBoom(G1)
            return "ok"
        return run

    async def scenario() -> None:
        runner = JobRunner()
        try:
            results = []
            for i in range(3):
                results.append(await runner.run(JobSpec(A, (G1,)), f"p{i}g1", job_on(G1)))  # type: ignore[arg-type]
                results.append(await runner.run(JobSpec(B, (G2,)), f"p{i}g2", job_on(G2)))  # type: ignore[arg-type]
                results.append(await runner.run(JobSpec(A, (G2,)), f"q{i}g2", job_on(G2)))  # type: ignore[arg-type]
            faulted = [r for r in results if not r.ok]
            assert len(faulted) == 3 and {r.fault_scope for r in faulted} == {G1}
            assert all(r.reason == JOB_RAISED for r in faulted)
            assert [s.scope_id for s in runner.take_signals()] == [G1]   # strategy A is not failed by its guard
            assert runner.circuit.streak(A) == 0 and runner.circuit.streak(B) == 0
        finally:
            runner.close()

    asyncio.run(scenario())


def test_a_fault_naming_a_scope_outside_the_job_counts_against_the_strategy() -> None:
    async def scenario() -> None:
        runner = JobRunner()
        try:
            def boom_foreign() -> int:
                raise GuardBoom(G2)

            def boom_plain() -> int:
                raise ValueError("an error text that is never kept 1.23456")

            r1 = await runner.run(JobSpec(A, (G1,)), "p1", boom_foreign)
            r2 = await runner.run(JobSpec(A, (G1,)), "p2", boom_plain)
            assert (r1.fault_scope, r2.fault_scope) == (A, A)
            assert "1.23456" not in repr(r2)
        finally:
            runner.close()

    asyncio.run(scenario())


def test_a_completed_job_resets_its_scopes_and_the_signal_is_once_per_episode() -> None:
    circuit = VersionCircuit(limit=3)
    assert circuit.fault(A, "p1") is None and circuit.fault(A, "p2") is None
    circuit.ok(A)
    assert circuit.streak(A) == 0
    assert circuit.fault(A, "p1") is None and circuit.fault(A, "p1") is None
    signal = circuit.fault(A, "p2")
    assert signal is not None and signal.position_ids == ("p1", "p2") and circuit.tripped(A)
    assert circuit.fault(A, "p3") is None                  # once per episode
    circuit.reset(A)
    assert not circuit.tripped(A) and circuit.streak(A) == 0
    with pytest.raises(ValueError):
        VersionCircuit(limit=0)


def test_the_job_runs_under_the_pinned_decimal_context_in_its_own_thread() -> None:
    seen: list[tuple[int, str]] = []

    def job() -> Decimal:
        c = decimal.getcontext()
        seen.append((c.prec, threading.current_thread().name))
        return Decimal(1) / Decimal(3)

    async def scenario() -> None:
        runner = JobRunner()
        try:
            with decimal.localcontext(Context(prec=5)):      # a hostile ambient context on the loop thread
                result = await runner.run(JobSpec(A, decimal_context=CTX), "p1", job)
            assert result.ok and result.value == CTX.divide(Decimal(1), Decimal(3))
            plain = await runner.run(JobSpec(B), "p2", lambda: 7)
            assert plain.value == 7
        finally:
            runner.close()

    asyncio.run(scenario())
    assert seen[0][0] == 40 and seen[0][1].startswith("arm-job-")


def test_a_cap_is_required() -> None:
    with pytest.raises(ValueError):
        JobRunner(cap_ms=0)


class GuardTimeout(TimeoutError):
    """A guard that raises the built-in TimeoutError itself (on 3.12 it is `asyncio.TimeoutError`)."""

    def __init__(self, scope_id: str) -> None:
        super().__init__(scope_id)
        self.scope_id = scope_id


def test_a_job_raising_systemexit_or_keyboardinterrupt_is_a_strategy_error_not_the_end_of_the_loop() -> None:
    def exits() -> int:
        sys.exit(3)

    def interrupts() -> int:
        raise KeyboardInterrupt

    async def scenario() -> None:
        runner = JobRunner()
        try:
            r1 = await runner.run(JobSpec(A), "p1", exits)
            r2 = await runner.run(JobSpec(A, (G1,)), "p2", interrupts)
            assert (r1.reason, r1.fault_scope) == (JOB_RAISED, A)
            assert (r2.reason, r2.fault_scope) == (JOB_RAISED, A)
            after = await runner.run(JobSpec(A), "p3", lambda: 5)      # the lane still works
            assert after.ok and after.value == 5
        finally:
            runner.close()

    asyncio.run(scenario())


def test_a_raised_timeout_error_is_job_raised_and_keeps_its_guard_attribution() -> None:
    def guard_times_out() -> int:
        raise GuardTimeout(G1)

    def plain_timeout() -> int:
        raise TimeoutError

    async def scenario() -> None:
        runner = JobRunner()
        try:
            r1 = await runner.run(JobSpec(A, (G1,)), "p1", guard_times_out)
            assert (r1.reason, r1.fault_scope) == (JOB_RAISED, G1)
            r2 = await runner.run(JobSpec(A, (G1,)), "p2", plain_timeout)
            assert (r2.reason, r2.fault_scope) == (JOB_RAISED, A)
        finally:
            runner.close()

    asyncio.run(scenario())


def test_the_callers_own_cancellation_still_propagates() -> None:
    release = threading.Event()

    async def scenario() -> None:
        runner = JobRunner(cap_ms=5_000)
        try:
            task = asyncio.create_task(runner.run(JobSpec(A), "p1", lambda: release.wait(5)))
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            runner.close()

    asyncio.run(scenario())


_HUNG_JOB_SCRIPT = """
import asyncio, os, threading
from src.riskmgr.jobs import JOB_TIMEOUT, JobRunner, JobSpec

async def main() -> bool:
    runner = JobRunner()
    never = threading.Event()
    result = await runner.run(JobSpec("strategy:toy_a:1"), "p1", never.wait)   # hangs for ever
    assert result.reason == JOB_TIMEOUT and runner.abandoned_threads() == 1
    return runner.close()

hung = asyncio.run(main())
print("closed hung=%s" % hung, flush=True)
if hung:
    os._exit(0)   # the engine's hard exit (src.runtime.exits.hard_exit): the interpreter would join the hung thread
"""


def test_a_process_with_a_planted_hung_job_exits_through_the_hard_exit_close_asks_for() -> None:
    root = Path(__file__).resolve().parents[2]
    done = subprocess.run([sys.executable, "-c", _HUNG_JOB_SCRIPT], cwd=root, capture_output=True, text=True,
                          timeout=30)
    assert done.returncode == 0, done.stderr
    assert "closed hung=True" in done.stdout


def test_close_reports_no_hung_thread_when_every_job_returned() -> None:
    async def scenario() -> bool:
        runner = JobRunner()
        assert (await runner.run(JobSpec(A), "p1", lambda: 1)).ok
        return runner.close()

    assert asyncio.run(scenario()) is False



# ---------------------------------------------------------------------------------------------------- S38c hung lanes
def test_past_the_abandoned_thread_bound_the_lane_is_kept_and_restart_required_is_signalled_once() -> None:
    """Round 12 / S38c: hung lanes are retired up to the bound; a further hang keeps its lane (`executor_hung`, no
    new thread) and signals the ARM-global `restart_required` once; the abandoned threads are bounded."""
    release = threading.Event()

    async def scenario() -> None:
        runner = JobRunner(max_abandoned=2)
        try:
            for i in range(2):
                r = await runner.run(JobSpec(A), f"pa{i}", lambda: release.wait(30))
                assert r.reason == JOB_TIMEOUT
            assert runner.abandoned_threads() == 2 and runner.take_restart_required() is None
            r = await runner.run(JobSpec(B), "pb1", lambda: release.wait(30))   # past the bound: not retired
            assert r.reason == JOB_TIMEOUT and runner.hung(B)
            assert runner.take_restart_required() == 3 and runner.take_restart_required() is None
            busy = await runner.run(JobSpec(B), "pb2", lambda: 1)
            assert busy.reason == EXECUTOR_HUNG and runner.abandoned_threads() == 2   # no new thread
            other = await runner.run(JobSpec(A), "pa9", lambda: 7)                     # A has a fresh lane
            assert other.ok and other.value == 7
            assert runner.close() is True                                              # the hard exit is needed
        finally:
            release.set()

    asyncio.run(scenario())


def test_a_finished_abandoned_thread_frees_its_slot() -> None:
    release = threading.Event()

    async def scenario() -> None:
        runner = JobRunner(cap_ms=50, max_abandoned=1)
        r = await runner.run(JobSpec(A), "pa1", lambda: release.wait(5))
        assert r.reason == JOB_TIMEOUT and runner.abandoned_threads() == 1
        release.set()
        for _ in range(100):
            if runner.abandoned_threads() == 0:
                break
            await asyncio.sleep(0.01)
        assert runner.abandoned_threads() == 0 and runner.close() is False

    asyncio.run(scenario())


def test_restart_required_is_signalled_again_after_the_hung_count_drops_under_the_bound() -> None:
    """S38c review: `restart_still_required` holds while more threads are hung than the bound; once the abandoned
    threads end, the signal is armed again, so a later hang past the bound is filed again."""
    first, second = threading.Event(), threading.Event()

    async def wait_free(runner: JobRunner) -> None:
        for _ in range(200):
            if runner.hung_threads() == 0:
                return
            await asyncio.sleep(0.01)

    async def scenario() -> None:
        runner = JobRunner(cap_ms=50, max_abandoned=1)
        try:
            assert (await runner.run(JobSpec(A), "pa1", lambda: first.wait(5))).reason == JOB_TIMEOUT
            assert runner.abandoned_threads() == 1 and not runner.restart_still_required()
            assert (await runner.run(JobSpec(B), "pb1", lambda: first.wait(5))).reason == JOB_TIMEOUT
            assert runner.hung_threads() == 2 and runner.restart_still_required()
            assert runner.take_restart_required() == 2
            first.set()
            await wait_free(runner)
            assert not runner.restart_still_required() and runner.abandoned_threads() == 0
            assert (await runner.run(JobSpec(A), "pa2", lambda: second.wait(5))).reason == JOB_TIMEOUT
            assert (await runner.run(JobSpec(C), "pc1", lambda: second.wait(5))).reason == JOB_TIMEOUT
            assert runner.take_restart_required() == 2                   # filed again
        finally:
            first.set()
            second.set()
            runner.close()

    asyncio.run(scenario())


def test_a_job_whose_cap_expired_before_it_started_never_runs() -> None:
    """S38c review: the worker marks the lane busy before it reads the ticket, so a job never starts after its cap
    on a lane the loop did not retire."""
    gate, ran = threading.Event(), []

    async def scenario() -> None:
        runner = JobRunner(cap_ms=50, max_abandoned=1)
        try:
            # the lane's single thread is held by a pre-submitted blocker, so the job queues past its cap
            lane = runner._lane(A)                                       # noqa: SLF001 (test inspects the lane)
            lane.executor.submit(gate.wait, 5)
            r = await runner.run(JobSpec(A), "pa1", lambda: ran.append(1))
            assert r.reason == JOB_TIMEOUT
            gate.set()
            await asyncio.sleep(0.2)
            assert ran == [] and runner.hung_threads() == 0
        finally:
            gate.set()
            runner.close()

    asyncio.run(scenario())


def test_a_cpu_spinning_strategy_keeps_event_loop_lag_under_the_c3_bound() -> None:
    """The accepted CPU-spin limit (S38c, design §B4): a strategy stuck in a pure-Python loop shares the GIL but the
    event loop still runs; its lag stays under the C3 100 ms bound while one spinning job thread is alive."""
    stop = threading.Event()

    def spin() -> int:
        n = 0
        while not stop.is_set():
            n += 1
        return n

    lags: list[float] = []

    async def scenario() -> None:
        runner = JobRunner(cap_ms=50)
        try:
            r = await runner.run(JobSpec(A), "pa1", spin)
            assert r.reason == JOB_TIMEOUT and runner.abandoned_threads() == 1
            for _ in range(40):
                t0 = time.perf_counter()
                await asyncio.sleep(0.01)
                lags.append(time.perf_counter() - t0 - 0.01)
        finally:
            stop.set()

    asyncio.run(scenario())
    assert max(lags) < 0.1
