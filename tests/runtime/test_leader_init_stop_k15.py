"""K15 (IMPLEMENTATION_PLAN known issues, fixed 2026-11-19): a stop (SIGTERM) taken while CANDIDATE or during
LEADER_INIT takes effect promptly instead of after LEADER_INIT ends (up to its 120 s deadline), so systemd's
TimeoutStopSec (35 s) never SIGKILLs the node mid-LEADER_INIT (operations.md §4):

* before the epoch bump commits: the lease is released and no term is started;
* after it: the LEADER_INIT step in flight is cancelled and the term ends through DRAINING with `switchover` (the
  fenced end_reason, bounded by the DRAINING budget), which is never an abnormal end for the SAFE_HOLD brake;
* after LEADER: DRAINING exactly as before.
"""
from __future__ import annotations

import asyncio
import dataclasses
import time
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.db.epoch import Epoch
from src.db.leader import SQL_COUNT_ABNORMAL
from src.runtime.etcd import LEADER_KEY
from src.runtime.leader_init import FenceOutcome, InitOutcome, LeaderInit, SodOutcome
from src.runtime.node import LEASE_LINK_TASK, NodeController
from src.runtime.ports import LeaderPorts
from src.runtime.roles import ABNORMAL_WINDOW_MS, NodeRole, TermRow, learn_backoff, safe_hold_due
from src.runtime.session import EngineSession, SessionContext, SessionTimings
from src.runtime.supervisor import TaskSupervisor
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.runtime.fakes import Exits
from tests.runtime.initfakes import InitRecorder
from tests.runtime.test_node import Session, World, roles

R = NodeRole
EPOCH = Epoch("c1", 5)


async def _until(pred: Callable[[], bool], timeout_s: float = 2.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not pred():
        assert time.monotonic() < deadline, "condition not reached"
        await asyncio.sleep(0.001)


# --- NodeController ---------------------------------------------------------------------------------------------
@pytest.mark.parametrize("how", ["sigterm", "stop"])
async def test_k15_stop_during_candidate_releases_the_won_lease_and_starts_no_term(how: str) -> None:
    w = World()
    c = w.controller()
    real = c.keeper.campaign

    def campaign(node_id: str) -> Epoch | None:
        epoch = real(node_id)
        assert c.role is R.CANDIDATE
        if how == "sigterm":
            c.request_drain("switchover", exit_after=True)
        else:
            c.stop()
        return epoch

    c.keeper.campaign = campaign  # type: ignore[method-assign]
    await asyncio.wait_for(c.run(), 2)
    assert roles(c) == [R.STANDBY, R.CANDIDATE, R.STANDBY]
    assert w.sessions == [] and w.init.log == []
    assert LEADER_KEY not in w.etcd.kv and not c.keeper.holds_lease() and c.keeper.deadline() is None
    assert w.exits.reasons == []


async def test_k15_stop_after_the_win_but_before_the_epoch_bump_starts_no_term() -> None:
    """The SIGTERM lands after the CANDIDATE check (role LEADER_INIT) but before step 1: no epoch bump, no
    leader_terms row; the session is closed, the lease released, no back-off (there was no term), and the node stops."""
    w = World()
    holder: list[NodeController] = []

    def factory(epoch: Epoch) -> Session:
        assert holder[0].role is R.LEADER_INIT
        holder[0].request_drain("switchover", exit_after=True)
        return World.factory(w, epoch)

    w.factory = factory  # type: ignore[method-assign]
    c = w.controller()
    holder.append(c)
    await asyncio.wait_for(c.run(), 2)
    assert roles(c) == [R.STANDBY, R.CANDIDATE, R.LEADER_INIT, R.STANDBY]
    assert w.init.log == []                                   # step 1 never ran: no term exists
    assert w.sessions[0].drained == [] and w.sessions[0].closed == 1 and c.session is None
    assert LEADER_KEY not in w.etcd.kv and not c.keeper.holds_lease() and c.keeper.deadline() is None
    assert c.machine.backoff is None and c._drain_request is None and c._stop.is_set()
    assert w.exits.reasons == []


async def test_k15_sigterm_mid_leader_init_cancels_the_step_and_ends_the_term_through_draining() -> None:
    """SM8 (up to 100 s in production) never returns here; before K15 the SIGTERM waited for it. Now the step is
    cancelled, the started term is drained as `switchover` (the keeper's 10 s DRAINING deadline replaces the 120 s
    LEADER_INIT one), the lease goes, and the run loop ends well inside systemd's 35 s stop timeout."""
    w = World(init=InitRecorder(hang_at="sm8"))
    c = w.controller()
    deadlines: list[str] = []
    real_begin = c.keeper.begin_draining

    def begin_draining() -> None:
        deadlines.append(f"draining (was {c.keeper.deadline()[1] if c.keeper.deadline() else None})")
        real_begin()

    c.keeper.begin_draining = begin_draining  # type: ignore[method-assign]
    task = asyncio.create_task(c.run())
    await _until(lambda: "sm8" in w.init.log)
    assert c.role is R.LEADER_INIT
    t0 = time.monotonic()
    c.request_drain("switchover", exit_after=True)
    await asyncio.wait_for(task, 3)
    assert time.monotonic() - t0 < 2.0
    assert roles(c)[-3:] == [R.LEADER_INIT, R.DRAINING, R.STANDBY] and R.LEADER not in roles(c)
    assert w.sessions[0].drained == ["switchover"] and "reached_leader" not in w.init.log
    assert deadlines == ["draining (was init_failed)"]
    assert LEADER_KEY not in w.etcd.kv and not c.keeper.holds_lease() and c.keeper.deadline() is None
    assert c.machine.backoff is not None and not c.machine.backoff.waivable     # the plain DRAINING back-off
    assert c.supervisor is not None and not c.supervisor.running(LEASE_LINK_TASK)
    assert w.exits.reasons == [] and w.flushes == 2                              # STANDBY flush after DRAINING


async def test_k15_switchover_without_exit_mid_leader_init_drains_and_keeps_running() -> None:
    w = World(init=InitRecorder(hang_at="sod"))
    c = w.controller()
    await c.startup()
    step = asyncio.create_task(c.step())
    await _until(lambda: "sod" in w.init.log)
    c.request_drain("switchover", exit_after=False)
    await asyncio.wait_for(step, 3)
    assert roles(c)[-2:] == [R.DRAINING, R.STANDBY] and w.sessions[0].drained == ["switchover"]
    assert not c._stop.is_set() and c._drain_request is None and w.exits.reasons == []


async def test_k15_switchover_without_exit_before_the_epoch_bump_sets_the_back_off() -> None:
    """A planned switchover that keeps the process up and lands before step 1: no term, the lease released, and the
    normal 60 s back-off, so this node cannot re-win the lease at once and defeat the switchover."""
    w = World()
    holder: list[NodeController] = []

    def factory(epoch: Epoch) -> Session:
        assert holder[0].role is R.LEADER_INIT
        holder[0].request_drain("switchover", exit_after=False)
        return World.factory(w, epoch)

    w.factory = factory  # type: ignore[method-assign]
    c = w.controller()
    holder.append(c)
    await c.startup()
    await asyncio.wait_for(c.step(), 2)
    assert roles(c)[-2:] == [R.LEADER_INIT, R.STANDBY] and w.init.log == []
    assert LEADER_KEY not in w.etcd.kv and not c.keeper.holds_lease()
    b = c.machine.backoff
    assert b is not None and not b.waivable and b.until_ms == w.now_ms() + 60_000
    assert c.machine.in_backoff(w.now_ms() + 1000, any_fresh_peer=False)
    assert not c._stop.is_set() and c._drain_request is None and w.exits.reasons == []


async def test_k15_stop_after_leader_reached_still_drains_as_before() -> None:
    w = World()
    c = w.controller()
    await c.startup()
    await c.step()
    assert c.role is R.LEADER and w.init.log[-1] == "reached_leader" and c.keeper.deadline() is None
    c.request_drain("switchover", exit_after=True)
    await c.step()
    assert roles(c) == [R.STANDBY, R.CANDIDATE, R.LEADER_INIT, R.LEADER, R.DRAINING, R.STANDBY]
    assert w.sessions[0].drained == ["switchover"] and c._stop.is_set()
    assert LEADER_KEY not in w.etcd.kv and w.exits.reasons == []


def test_k15_a_stopped_term_is_never_an_abnormal_end() -> None:
    """operations.md §4: abnormal = lost / init_failed. A term ended by a stop during LEADER_INIT carries
    `switchover` (written by DRAINING, kept by the successor's COALESCE), so the 3-in-15-min brake never counts it;
    it still counts as an epoch change (the cluster-wide flap budget), and a restart learns the plain back-off."""
    now = 10 * ABNORMAL_WINDOW_MS
    stopped = [TermRow("c1", i, "n1", now - 60_000 + i, None, now - 50_000 + i, "switchover") for i in range(5)]
    assert safe_hold_due(stopped, now, None) is None
    failed = [dataclasses.replace(t, end_reason="init_failed") for t in stopped[:3]]
    assert safe_hold_due(failed, now, None) is not None
    assert "'switchover'" not in SQL_COUNT_ABNORMAL
    drained_open = TermRow("c1", 9, "n1", now - 1000, None, None, "switchover")    # successor not yet seen
    b = learn_backoff([drained_open], now)
    assert b is not None and not b.waivable


# --- LeaderInit -------------------------------------------------------------------------------------------------
async def test_k15_leader_init_stopped_before_step1_bumps_nothing() -> None:
    rec, exits, stop = InitRecorder(), Exits(), asyncio.Event()
    stop.set()
    report = await LeaderInit(rec.deps(), exit_fn=exits, stop=stop).run(EPOCH, "n1")
    assert report is not None and report.outcome is InitOutcome.STOPPED and not report.term_started
    assert rec.log == [] and exits.reasons == []


async def test_k15_step1_is_never_cancelled_by_a_stop() -> None:
    """Its commit must be known: a stop during the epoch bump waits for it, then ends LEADER_INIT at the boundary."""
    rec, exits, stop = InitRecorder(), Exits(), asyncio.Event()
    finished: list[bool] = []

    async def bump(epoch: Epoch, node_id: str, now_ms: int) -> FenceOutcome:
        stop.set()
        await asyncio.sleep(0.05)
        finished.append(True)
        return FenceOutcome(False)

    rec.bump_and_fence = bump  # type: ignore[method-assign]
    report = await LeaderInit(rec.deps(), exit_fn=exits, stop=stop).run(EPOCH, "n1")
    assert finished == [True]
    assert report is not None and report.outcome is InitOutcome.STOPPED and report.term_started
    assert report.steps == ["epoch_bump"] and rec.log == [] and exits.reasons == []


async def test_k15_a_stop_cancels_the_step_in_flight() -> None:
    rec, exits, stop = InitRecorder(hang_at="sod"), Exits(), asyncio.Event()
    task = asyncio.create_task(LeaderInit(rec.deps(), exit_fn=exits, stop=stop).run(EPOCH, "n1"))
    await _until(lambda: "sod" in rec.log)
    stop.set()
    report = await asyncio.wait_for(task, 2)
    assert report is not None and report.outcome is InitOutcome.STOPPED and report.term_started
    assert report.steps == ["epoch_bump", "hlc_reseed", "flushdb", "ruleset"] and exits.reasons == []


async def test_k15_a_step_slow_to_unwind_is_bounded_by_abort_cancel_s() -> None:
    rec, exits, stop = InitRecorder(), Exits(), asyncio.Event()
    unwound = asyncio.Event()

    async def stubborn(now_ms: int) -> SodOutcome:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.3)          # e.g. a driver cleaning up a connection
            unwound.set()
            raise
        return SodOutcome()

    rec.catch_up = stubborn  # type: ignore[method-assign]
    task = asyncio.create_task(LeaderInit(rec.deps(), exit_fn=exits, stop=stop, abort_cancel_s=0.05).run(EPOCH, "n1"))
    await _until(lambda: "ruleset" in rec.log)
    await asyncio.sleep(0.01)
    t0 = time.monotonic()
    stop.set()
    report = await asyncio.wait_for(task, 2)
    assert time.monotonic() - t0 < 0.25 and not unwound.is_set()
    assert report is not None and report.outcome is InitOutcome.STOPPED and exits.reasons == []
    await asyncio.wait_for(unwound.wait(), 1)


async def test_k15_a_stop_just_before_the_deadline_is_still_stopped_not_init_failed() -> None:
    """The unwind wait is capped at the time left before the LEADER_INIT deadline: a stop that lands within
    `abort_cancel_s` of it ends STOPPED, never `init_failed: deadline`."""
    rec, exits, stop = InitRecorder(), Exits(), asyncio.Event()

    async def stubborn(now_ms: int) -> SodOutcome:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(1.0)          # slower to unwind than the deadline left
            raise
        return SodOutcome()

    rec.catch_up = stubborn  # type: ignore[method-assign]
    task = asyncio.create_task(LeaderInit(rec.deps(), exit_fn=exits, stop=stop, deadline_s=0.4,
                                          abort_cancel_s=1.0).run(EPOCH, "n1"))
    await _until(lambda: "ruleset" in rec.log)
    await asyncio.sleep(0.2)                  # ~0.2 s left before the deadline, under abort_cancel_s
    stop.set()
    report = await asyncio.wait_for(task, 2)
    assert exits.reasons == []
    assert report is not None and report.outcome is InitOutcome.STOPPED and report.term_started


async def test_k15_a_step_that_swallows_the_cancel_never_reaches_later_steps() -> None:
    """Guard: a step that (wrongly) swallows `CancelledError` and returns after the stop cannot go on to start SM4,
    the RiskSweep or mark the term LEADER."""
    rec, exits, stop = InitRecorder(), Exits(), asyncio.Event()

    async def swallowing(now_ms: int) -> SodOutcome:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)          # past abort_cancel_s, then returns as if nothing happened
        return SodOutcome()

    rec.catch_up = swallowing  # type: ignore[method-assign]
    task = asyncio.create_task(LeaderInit(rec.deps(), exit_fn=exits, stop=stop, abort_cancel_s=0.02).run(EPOCH, "n1"))
    await _until(lambda: "ruleset" in rec.log)
    await asyncio.sleep(0.01)
    stop.set()
    report = await asyncio.wait_for(task, 2)
    assert report is not None and report.outcome is InitOutcome.STOPPED
    await asyncio.sleep(0.3)                  # the swallowing step has returned by now
    assert not {"calendar", "sm4", "sm8", "risksweep", "dlq_barrier", "reached_leader"} & set(rec.log)
    assert "reached_leader" not in report.steps and exits.reasons == []


class EpochMismatch(Exception):
    """Matched by name (`is_fence_loss`), like src.db.fence.EpochMismatch."""


async def test_k15_a_fence_loss_while_stopping_is_still_the_fence_exit() -> None:
    rec, exits, stop = InitRecorder(), Exits(), asyncio.Event()

    async def fenced(now_ms: int) -> SodOutcome:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise EpochMismatch("successor bumped the epoch") from None
        return SodOutcome()

    rec.catch_up = fenced  # type: ignore[method-assign]
    task = asyncio.create_task(LeaderInit(rec.deps(), exit_fn=exits, stop=stop).run(EPOCH, "n1"))
    await _until(lambda: "ruleset" in rec.log)
    await asyncio.sleep(0.01)
    stop.set()
    assert await asyncio.wait_for(task, 2) is None
    assert exits.reasons and exits.reasons[0].startswith("fence")


async def test_k15_a_step_that_finishes_with_the_stop_still_reaches_leader() -> None:
    """No stop before the last step returned: LEADER as before (the controller then drains it on its next step)."""
    rec, exits, stop = InitRecorder(), Exits(), asyncio.Event()
    report = await LeaderInit(rec.deps(), exit_fn=exits, stop=stop).run(EPOCH, "n1")
    assert report is not None and report.outcome is InitOutcome.LEADER and report.steps[-1] == "reached_leader"


# --- EngineSession: the real DRAINING of a term stopped mid-LEADER_INIT --------------------------------------------
TIMINGS = SessionTimings(drain_close_s=0.05, drain_flush_s=0.2, drain_spill_s=0.5, drain_record_s=0.5)


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.flushall()
    await r.aclose()


async def _run_id(_: Any) -> str:
    return "run-k15"


async def test_k15_engine_session_stopped_mid_leader_init_writes_switchover_within_the_drain_budget(
        redis: Any, tmp_path: Path) -> None:
    epoch = Epoch("c", 7)
    backend = MemoryBackend(redis, _run_id, epoch.token)

    def ports(e: Epoch) -> LeaderPorts:
        return backend.ports(e)

    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [mock_adapter(fixture(wall_ms()))], lease=FakeLease(),
                         run_id_reader=_run_id, exit_fn=lambda reason: None, timings=TIMINGS)
    session = EngineSession(ctx, epoch)
    sm8_started = asyncio.Event()

    async def hung_sm8() -> None:              # SM8 waiting on a slow heal (production bound: 100 s)
        sm8_started.set()
        await asyncio.Event().wait()

    session.bootstrap = hung_sm8  # type: ignore[method-assign]
    exits: list[str] = []
    sup = TaskSupervisor([], exit_fn=exits.append, poll_interval_s=0.1)
    sup_stop = asyncio.Event()
    sup_task = asyncio.create_task(sup.run(sup_stop))
    stop = asyncio.Event()
    init = asyncio.create_task(LeaderInit(session.init_deps(sup), exit_fn=exits.append, deadline_s=60.0,
                                          clock_ms=wall_ms, stop=stop).run(epoch, NODE))
    await asyncio.wait_for(sm8_started.wait(), 5)
    stop.set()
    report = await asyncio.wait_for(init, 3)
    assert report is not None and report.outcome is InitOutcome.STOPPED and report.term_started
    t0 = time.monotonic()
    await asyncio.wait_for(session.drain("switchover"), 10)
    assert time.monotonic() - t0 < TIMINGS.drain_budget_s + 0.5
    sup_stop.set()
    await asyncio.wait_for(sup_task, 5)
    terms = backend.tables.terms
    assert terms[0][0] == "started" and ("end_reason", "switchover") in terms
    assert not any(kind == "reached_leader" for kind, _ in terms)
    assert exits == []
