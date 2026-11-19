"""IMPLEMENTATION_PLAN 4.9 review follow-ups, fencing + runtime groups (no network: fakes, fakeredis, in-memory
ports). One regression test (or more) per behaviour change."""
from __future__ import annotations

import asyncio
import dataclasses
import threading
import time
from collections.abc import AsyncIterator, Collection, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import fakeredis
import pytest

from src.adapters.base import TransientBrokerError
from src.adapters.pepperstone.tokens import InMemoryTokenStore, RefreshedTokens, TokenManager, TokenPair
from src.cache import keys as K
from src.db.epoch import Epoch
from src.db.fence import EpochMismatch
from src.oversee.sweep import EpochMismatch as SweepEpochMismatch
from src.ports import DlqRow
from src.pubsub.barrier import CommitBarrier
from src.pubsub.connection import ConnState
from src.pubsub.health import STREAMS, HealthMirror, SubscriptionState
from src.pubsub.spooler import _append_spill
from src.runtime import probes
from src.runtime.ingestion_control import ConsumerSwitches, OrchestratorReconciler
from src.runtime.leader_init import LeaderInit, is_fence_loss
from src.runtime.health import HealthBoard
from src.runtime.lease import LeaseKeeper
from src.runtime.node import NodeController
from src.runtime.pg_ports import (
    SQL_AUTH_REARMED_AT,
    SQL_OPERATOR_REQUEST_CONSUME,
    SQL_OPERATOR_REQUESTS_PENDING,
    GuardedEngineDb,
    PgAuthRearms,
    PgOperatorRequests,
    PgTermWriter,
)
from src.runtime.ports import OperatorRequest
from src.runtime.probes import NodeHealthProbe, StandbyRedisFlush
from src.runtime.session import EngineSession, SessionContext, SessionTimings, check_drain_budget
from src.runtime.spill_standby import NodeSpillDrain
from src.runtime.supervisor import TaskContext, TaskSpec, TaskSupervisor
from src.runtime.wiring import (
    TOKEN_REFRESH_MIN_LEASE_S,
    EngineHooks,
    LeaseGuardedTokenStore,
    WiringError,
    build_engine,
)
from tests.db.conftest import EPOCH as DB_EPOCH
from tests.db.conftest import FakeConn, FakeSource
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.runtime.fakes import Exits, FakeClock, FakeEtcd
from tests.runtime.initfakes import InitRecorder
from tests.runtime.test_node import Session, StandbyWorkSpy, World
from tests.runtime.test_wiring import MOCK, STONEX, cfg
from tests.runtime.test_wiring_credentials import FakeSource as FakeSecrets

EPOCH = Epoch("c1", 101)


# ============================================================================================ fencing
class FencedAtLeaderEntry(InitRecorder):
    async def mark_reached_leader(self, epoch: Epoch, now_ms: int) -> None:
        self.log.append("reached_leader")
        raise EpochMismatch("fenced write affected 0 rows (epoch 101)")


class FencedInStep6(InitRecorder):
    async def start_risksweep(self) -> None:
        self.log.append("risksweep")
        raise SweepEpochMismatch("sweep write fenced")


async def test_epoch_mismatch_from_mark_reached_leader_hard_exits_as_fence() -> None:
    """Before: a lost fence at LEADER entry exited as `init_failed`; it is a fence (exit code 75)."""
    rec, exits = FencedAtLeaderEntry(), Exits()
    report = await LeaderInit(rec.deps(), exit_fn=exits, clock_ms=lambda: 1).run(EPOCH, "n1")
    assert report is None
    assert len(exits.reasons) == 1 and exits.reasons[0].startswith("fence: EpochMismatch")


async def test_epoch_mismatch_inside_the_step6_task_group_is_a_fence_too() -> None:
    rec, exits = FencedInStep6(), Exits()
    assert await LeaderInit(rec.deps(), exit_fn=exits, clock_ms=lambda: 1).run(EPOCH, "n1") is None
    assert exits.reasons and exits.reasons[0].startswith("fence:")
    assert not is_fence_loss(RuntimeError("other")) and is_fence_loss(ExceptionGroup("g", [EpochMismatch("x")]))


async def test_pg_term_writer_lost_fence_at_leader_entry_reports_the_fence() -> None:
    """PgTermWriter.mark_reached_leader on a term a successor already closed: EpochMismatch reaches the
    runtime's on_fence_lost (hard exit) and propagates."""
    conn = FakeConn()
    conn.stored_epoch = 99
    lost: list[BaseException] = []
    writer = PgTermWriter(GuardedEngineDb(FakeSource(conn), lambda: DB_EPOCH, lost.append))
    with pytest.raises(EpochMismatch):
        await writer.mark_reached_leader(DB_EPOCH, 1_000)
    assert len(lost) == 1 and isinstance(lost[0], EpochMismatch)


class _Rt:
    def __init__(self) -> None:
        self.conn = SimpleNamespace(state=ConnState.STREAMING)
        self.subs = {s: SubscriptionState(subscribed=True, lost=False) for s in STREAMS}
        self.adapter = SimpleNamespace()


class _Orch:
    """The parts of IngestionOrchestrator the reconciler reads (CommitBarrier, HealthMirror, subscriptions)."""

    def __init__(self, keep_open: str | None = None) -> None:
        self.bus = SimpleNamespace(commit_barrier=CommitBarrier())
        self.health = HealthMirror(None)  # type: ignore[arg-type]
        self.rt = _Rt()
        self.keep_open = keep_open
        self.snapshots = 0

    def runtime(self, adapter_id: str) -> _Rt:
        return self.rt

    async def run_snapshot(self, adapter_id: str, *, refresh_instruments: bool) -> None:
        self.snapshots += 1
        if self.keep_open is not None:          # e.g. a parked REST row keeps the stream's gap open
            self.rt.subs[self.keep_open].lost = True  # type: ignore[index]
            self.health.enqueue("lost", adapter_id, self.keep_open)  # type: ignore[arg-type]


def reconciler(orch: _Orch, timeout_s: float = 0.2) -> OrchestratorReconciler:
    return OrchestratorReconciler(orch, lambda: ["mock"], ConsumerSwitches(), lambda: True,  # type: ignore[arg-type]
                                  commit_timeout_s=timeout_s, poll_s=0.01)


async def test_reconcile_all_fails_while_the_gap_it_had_to_clear_stays_open() -> None:
    """Before: the consumers going idle was enough, so SM3 went on to FLUSHDB with the account still fenced."""
    orch = _Orch(keep_open="orders")
    rc = reconciler(orch)
    assert await rc.reconcile_all(None) is False
    assert any("mock:orders gap open" in p for p in rc.last_problems), rc.last_problems


async def test_reconcile_all_waits_for_barrier_and_stream_health_write() -> None:
    orch = _Orch()
    barrier: CommitBarrier = orch.bus.commit_barrier
    p = barrier.open("mock")
    barrier.add(p, 11, "orders")
    barrier.seal(p, lambda failed: None)
    orch.health.enqueue("reconciled", "mock", "account")
    rc = reconciler(orch, timeout_s=2.0)

    async def settle() -> None:
        await asyncio.sleep(0.1)
        barrier.settle(11, True)
        await asyncio.sleep(0.1)
        orch.health._ops.clear()                 # the health mirror wrote conn_lost_at = NULL

    task = asyncio.create_task(settle())
    started = time.monotonic()
    assert await rc.reconcile_all(None) is True and rc.last_problems == []
    assert time.monotonic() - started >= 0.15
    await task
    assert rc.open_gaps(["mock"]) == []
    orch.health.enqueue("lost", "mock", "prices")
    assert rc.open_gaps(["mock"]) == ["mock:prices stream_health not written yet"]


async def test_reconcile_all_reports_unsettled_reconcile_rows() -> None:
    orch = _Orch()
    barrier: CommitBarrier = orch.bus.commit_barrier
    p = barrier.open("mock")
    barrier.add(p, 12, "account")
    barrier.seal(p, lambda failed: None)
    rc = reconciler(orch, timeout_s=0.1)
    assert await rc.reconcile_all(None) is False
    assert any("1 reconcile rows not settled" in x for x in rc.last_problems)


# ============================================================================================ STANDBY flush
async def test_standby_flush_is_flushdb_of_the_engine_database_only() -> None:
    server = fakeredis.FakeServer()
    engine_db = fakeredis.FakeAsyncRedis(server=server, db=0)
    other_db = fakeredis.FakeAsyncRedis(server=server, db=1)
    try:
        await engine_db.set(K.BOOT_ID, "run:7")
        await engine_db.set("order:x", "1")
        await other_db.set("not-ours", "keep")
        await StandbyRedisFlush(engine_db).flushdb()
        assert await engine_db.get(K.BOOT_ID) is None and await engine_db.get("order:x") is None
        assert await other_db.get("not-ours") == b"keep"
    finally:
        await engine_db.aclose()
        await other_db.aclose()
    assert not hasattr(probes, "LeaderRedisFlush")       # the dead LEADER_INIT FLUSHALL class is gone


# ============================================================================================ DRAINING budget
def test_draining_budget_fits_the_keeper_deadline() -> None:
    assert check_drain_budget(SessionTimings(), 10.0, 2.0) is None
    assert check_drain_budget(FAST_TIMINGS, 10.0, 2.0) is None
    old = SessionTimings(drain_close_s=3.0, drain_flush_s=2.0, drain_spill_s=2.0, drain_record_s=2.0,
                         drain_cluster_s=1.0, drain_aclose_s=5.0)      # the pre-4.9 steps: 15 s
    problem = check_drain_budget(old, 10.0, 2.0)
    assert problem is not None and "DRAINING deadline" in problem


def test_build_engine_refuses_a_draining_budget_over_the_deadline(tmp_path: Path) -> None:
    slow = SessionTimings(drain_close_s=3.0, drain_flush_s=2.0, drain_spill_s=2.0, drain_record_s=2.0)
    with pytest.raises(WiringError, match="DRAINING"):
        build_engine(cfg(MOCK, tmp_path), environ=MOCK,
                     hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), timings=slow, require_keeper_thread=False))


class _HangTerms:
    async def mark_reached_leader(self, epoch: Epoch, now_ms: int) -> None:
        await asyncio.Event().wait()

    async def record_end_reason(self, epoch: Epoch, reason: str) -> None:
        await asyncio.Event().wait()


async def _hang() -> None:
    await asyncio.Event().wait()


# ============================================================================================ session fixture
async def run_id(_: Any) -> str:
    return "run-49"


@dataclasses.dataclass
class Built:
    session: EngineSession
    invalidated: list[str]
    exits: list[str]


@pytest.fixture
async def built(tmp_path: Path) -> AsyncIterator[Built]:
    redis = fakeredis.FakeAsyncRedis()
    epoch = Epoch("c", 5)
    backend = MemoryBackend(redis, run_id, epoch.token)
    fx = fixture(wall_ms())
    invalidated: list[str] = []
    exits: list[str] = []
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [mock_adapter(fx)], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=exits.append, timings=FAST_TIMINGS, on_auth_failed=invalidated.append)
    s = EngineSession(ctx, epoch)
    try:
        yield Built(s, invalidated, exits)
    finally:
        await s.aclose(1.0)
        await redis.aclose()


async def test_drain_stays_inside_its_budget_when_postgres_hangs(built: Built) -> None:
    """Before: end_reason 2 s + cluster flush 1 s + pool close 5 s (+ 3 s close) could pass the 10 s deadline."""
    s = built.session
    t = dataclasses.replace(FAST_TIMINGS, drain_close_s=0.2, drain_flush_s=0.2, drain_spill_s=0.2,
                            drain_record_s=0.2, drain_cluster_s=0.1, drain_aclose_s=0.2)
    s._t = t
    s.ports = dataclasses.replace(s.ports, terms=_HangTerms(), close=_hang)
    started = time.monotonic()
    await asyncio.wait_for(s.drain("switchover"), 5.0)
    assert time.monotonic() - started < t.drain_budget_s + 0.5


class _HangCluster:
    async def flush(self, timeout_s: float) -> None:
        await asyncio.Event().wait()


async def test_drain_budget_counts_the_cluster_flush_grace(built: Built) -> None:
    """Skeptic fix: the cluster flush waits drain_cluster_s + 0.1 s and the final spill at least 0.1 s, which
    drain_budget_s left out, so check_drain_budget under-counted DRAINING by up to 0.2 s."""
    s = built.session
    t = dataclasses.replace(FAST_TIMINGS, drain_close_s=0.05, drain_flush_s=0.05, drain_spill_s=0.05,
                            drain_record_s=0.05, drain_cluster_s=0.05, drain_aclose_s=0.05)
    s._t = t
    s.ctx = dataclasses.replace(s.ctx, cluster=_HangCluster())  # type: ignore[arg-type]
    s.ports = dataclasses.replace(s.ports, terms=_HangTerms(), close=_hang)
    steps = (t.drain_close_s + t.drain_flush_s + t.drain_spill_s + t.drain_record_s + t.drain_cluster_s
             + t.drain_aclose_s)
    assert t.drain_budget_s == pytest.approx(steps + 0.2)
    started = time.monotonic()
    await asyncio.wait_for(s.drain("switchover"), 5.0)
    elapsed = time.monotonic() - started
    hung = t.drain_close_s + t.drain_record_s + t.drain_cluster_s + t.drain_aclose_s   # the steps that hang
    assert elapsed > hung + 0.08                             # the grace is real time DRAINING spends
    assert elapsed < t.drain_budget_s + 0.4


# ============================================================================================ ruleset / evidence
async def test_session_installs_the_ruleset_on_the_sweep_and_repages_at_its_cadence(built: Built) -> None:
    s = built.session
    assert s.sweep._ruleset is s.rules
    base = s.rules.current
    assert s.dispatcher.escalate_after_ms == base.sweep.escalate_after_ms
    reloaded = dataclasses.replace(base, sweep=dataclasses.replace(base.sweep, escalate_after_ms=123_456),
                                   ruleset_version=base.ruleset_version + 1)
    s.rules.install(reloaded)
    assert s.dispatcher.escalate_after_ms == 123_456       # before: fixed at the term's start


async def test_execution_events_reach_the_market_calendar(built: Built) -> None:
    s = built.session
    got: list[tuple[str, str, int]] = []
    s.calendar = SimpleNamespace(version=None, on_execution=lambda a, sym, ts: got.append((a, sym, ts)))  # type: ignore[assignment]
    rt = s.orchestrator.runtime("mock")
    s.orchestrator._execution_evidence(rt, "EURUSD", 1_000)
    assert got == []                                        # no SM10 schedule yet
    s.calendar.version = 3
    s.orchestrator._execution_evidence(rt, "EURUSD", 2_000)
    assert got == [("mock", "EURUSD", 2_000)]


# ============================================================================================ operator loop
class FakeQueue:
    """OperatorRequestQueue with the pending query's semantics (src/db/operator_requests.py SQL_PENDING_WITH_PAYLOAD):
    unconsumed rows whose kind is in `kinds` and whose id is not in `exclude`, by request id, at most 50."""

    def __init__(self, reqs: Sequence[OperatorRequest]) -> None:
        self.reqs = list(reqs)
        self.consumed: list[tuple[int, int, str]] = []
        self.kinds_asked: list[frozenset[str]] = []
        self.excluded_asked: list[frozenset[int]] = []

    async def pending(self, epoch_token: int, kinds: Collection[str],
                      exclude: Collection[int] = ()) -> Sequence[OperatorRequest]:
        self.kinds_asked.append(frozenset(kinds))
        self.excluded_asked.append(frozenset(exclude))
        done = {c[1] for c in self.consumed} | set(exclude)
        return [r for r in sorted(self.reqs, key=lambda r: r.request_id)
                if r.request_id not in done and r.kind in kinds][:50]

    async def consume(self, epoch_token: int, request_id: int, result: str) -> None:
        self.consumed.append((epoch_token, request_id, result))


class FakeRearms:
    def __init__(self) -> None:
        self.rearmed_gen: str | None = None
        self.marker = 1_000                                  # rearmed_at_ms of the operator's re-arm
        self.asked: list[tuple[str, str]] = []

    async def rearmed_at(self, adapter_id: str, credential_generation: str) -> int | None:
        self.asked.append((adapter_id, credential_generation))
        return self.marker if credential_generation == self.rearmed_gen else None


async def test_engine_consumes_reset_task_requests(built: Built) -> None:
    """Before: `python -m src.ops task reset` wrote operator_requests and nothing in the engine read them."""
    s = built.session
    audits: list[tuple[str, str, str]] = []

    async def body(ctx: TaskContext) -> None:
        await asyncio.Event().wait()

    sup = TaskSupervisor([TaskSpec("risksweep", body, max_stamp_age_s=5.0)], exit_fn=built.exits.append,
                         audit=lambda op, name, who: audits.append((op, name, who)))
    s._sup = sup
    queue = FakeQueue([OperatorRequest(1, "reset_task", "risksweep", "alice"),
                       OperatorRequest(2, "reset_task", "no-such-task", "bob"),
                       OperatorRequest(3, "something_else", "x", "carol")])
    s.operator_requests = queue
    try:
        await s.operator_pass()
        assert audits == [("reset_task", "risksweep", "alice")]
        # S10 (RT-M10): a kind with no handler is not read, so it stays pending instead of `unsupported_kind`
        assert queue.consumed == [(5, 1, "applied"), (5, 2, "unknown_task")]
        await s.operator_pass()                              # consumed: nothing is applied twice
        assert len(audits) == 1 and len(queue.consumed) == 2
    finally:
        await sup.stop_all()


async def test_engine_notices_an_operator_rearm_and_invalidates_secrets_once(built: Built) -> None:
    s = built.session
    rearms = FakeRearms()
    s.auth_rearms = rearms
    rt = s.orchestrator.runtime("mock")
    rt.conn = SimpleNamespace(state=ConnState.AUTH_FAILED)  # type: ignore[assignment]
    rt.failed_generation = "sm:v1"
    await s.operator_pass()
    assert built.invalidated == ["mock"]                    # AUTH_FAILED -> secrets cache invalidated
    assert rearms.asked == [("mock", "sm:v1")] and not rt.rearm.is_set()
    await s.operator_pass()
    assert built.invalidated == ["mock"]                    # once per failed generation
    rearms.rearmed_gen = "sm:v1"                            # `python -m src.ops adapter rearm mock sm:v1`
    await s.operator_pass()
    assert rt.rearm.is_set()
    rt.failed_generation = "sm:v2"
    await s.operator_pass()
    assert built.invalidated == ["mock", "mock"]


async def test_one_operator_rearm_rearms_the_adapter_only_once(built: Built) -> None:
    """Skeptic fix: SM4 enters AUTH_FAILED before its AUTH_FAILED row is written, so a pass can still read the
    operator's REARMED row after the re-armed attempts failed again. Before: that pass re-armed the adapter a
    second time (another round of credential attempts at the broker). Now each re-arm is acted on once."""
    s = built.session
    rearms = FakeRearms()
    rearms.rearmed_gen = "sm:v1"
    s.auth_rearms = rearms
    rt = s.orchestrator.runtime("mock")
    rt.conn = SimpleNamespace(state=ConnState.AUTH_FAILED)  # type: ignore[assignment]
    rt.failed_generation = "sm:v1"
    await s.operator_pass()
    assert rt.rearm.is_set()
    rt.rearm.clear()                                        # re-armed attempts failed: AUTH_FAILED again,
    await s.operator_pass()                                 # the row still reads REARMED (not yet rewritten)
    assert not rt.rearm.is_set()
    rearms.marker = 2_000                                   # a second `adapter rearm` by an operator
    await s.operator_pass()
    assert rt.rearm.is_set()


async def test_pg_operator_requests_and_rearm_reader() -> None:
    conn = FakeConn()
    conn.returns_all(SQL_OPERATOR_REQUESTS_PENDING, [(4, "reset_task", "risksweep", "alice")])
    from src.db.epoch import EngineDb
    db = EngineDb(FakeSource(conn), lambda: DB_EPOCH)
    q = PgOperatorRequests(db)
    assert await q.pending(7, {"reset_task", "arm_disarm"}) == [OperatorRequest(4, "reset_task", "risksweep", "alice")]
    assert conn.args_of(SQL_OPERATOR_REQUESTS_PENDING) == [(7, ["arm_disarm", "reset_task"], [])]   # $2 = the kinds
    await q.pending(7, {"reset_task"}, {9, 3})
    assert conn.args_of(SQL_OPERATOR_REQUESTS_PENDING)[-1] == (7, ["reset_task"], [3, 9])   # $3 = accepted ids
    await q.consume(7, 4, "applied")
    assert conn.args_of(SQL_OPERATOR_REQUEST_CONSUME) == [(7, 4, "applied")]   # $1 = the fencing token
    conn.stored_epoch = 99
    with pytest.raises(EpochMismatch):                       # 0 rows + a moved epoch = lost fence
        await q.consume(7, 5, "applied")

    rc = FakeConn()
    rc.returns(SQL_AUTH_REARMED_AT, ("REARMED", 5_000), ("AUTH_FAILED", None), None)
    reader = PgAuthRearms(FakeSource(rc))
    assert await reader.rearmed_at("stonex", "sm:v1") == 5_000
    assert await reader.rearmed_at("stonex", "sm:v1") is None
    assert await reader.rearmed_at("stonex", "sm:v1") is None  # no row is never a re-arm


def test_wiring_invalidates_the_secret_on_auth_failed_and_delays_the_standby_drain(tmp_path: Path) -> None:
    class CountingSecrets(FakeSecrets):
        invalidations = 0

        def invalidate(self) -> None:
            self.invalidations += 1

    secrets = CountingSecrets({})
    engine = build_engine(cfg(STONEX, tmp_path), environ=STONEX,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), secrets=secrets, require_keeper_thread=False))
    try:
        assert engine.context.on_auth_failed is not None
        engine.context.on_auth_failed("stonex")
        assert secrets.invalidations == 1
        assert engine.controller._standby_delay_after_drain_s == engine.context.timings.drain_spill_s > 0
    finally:
        asyncio.run(engine.aclose())


# ============================================================================================ token refresh lease
async def test_token_refresh_needs_eight_seconds_of_lease() -> None:
    remaining = {"s": 7.0}
    inner = InMemoryTokenStore(TokenPair("a", "r", None, 1))
    store = LeaseGuardedTokenStore(inner, lambda need: remaining["s"] > need)
    assert store.min_lease_s == TOKEN_REFRESH_MIN_LEASE_S == 8.0
    called: list[str] = []

    async def refresher(refresh_token: str) -> RefreshedTokens:
        called.append(refresh_token)
        return RefreshedTokens("a2", "r2", None)

    mgr = TokenManager(TokenPair("a", "r", None, 1), store)
    with pytest.raises(TransientBrokerError, match="8 s of lease"):
        await mgr.refresh(refresher)
    assert called == [] and inner.in_flight is None          # no 'refresh in flight' row, no broker call
    remaining["s"] = 9.0
    pair = await mgr.refresh(refresher)
    assert called == ["r"] and pair.generation == 2


def test_eight_second_bound_is_reachable_with_the_real_keeper() -> None:
    """lease_valid(30) could never hold (at most ~12 s remain after a renewal); lease_valid(8) holds while
    the keeper renews every second."""
    clk = FakeClock(1000.0)
    k = LeaseKeeper(FakeEtcd(clk), clock=clk, fence=Exits())
    assert k.campaign("n1") is not None
    assert k.lease_valid(TOKEN_REFRESH_MIN_LEASE_S) and not k.lease_valid(30.0)
    for _ in range(20):
        clk.advance(1.0)
        k.tick()
        assert k.lease_valid(TOKEN_REFRESH_MIN_LEASE_S)
    clk.advance(4.5)                                          # renewals stopped: 12 - 4.5 < 8
    assert not k.lease_valid(TOKEN_REFRESH_MIN_LEASE_S)


# ============================================================================================ standby spill drain
def _row(i: int) -> DlqRow:
    return DlqRow(event_type="CASH_FLOW", entity_id=f"t{i}", adapter_id="stonex", raw_payload=f'{{"i":{i}}}',
                  payload_hash=f"h{i}", error_reason="QueueFull")


class _Sink:
    def __init__(self) -> None:
        self.rows: list[DlqRow] = []

    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        self.rows.extend(rows)


class _Pages:
    def __init__(self) -> None:
        self.got: list[tuple[str, str, str]] = []

    def page(self, severity: str, title: str, body: str) -> None:
        self.got.append((severity, title, body))


async def test_standby_spill_drain_waits_its_delay_before_reading(tmp_path: Path) -> None:
    path = tmp_path / "spill.jsonl"
    sink = _Sink()
    drain = NodeSpillDrain(sink, path, node="n1", clock_ms=lambda: 1, idle_s=0.01, retry_s=0.01)
    _append_spill(path, [_row(1)])
    drain.start(delay_s=0.4)
    try:
        await asyncio.sleep(0.2)
        assert sink.rows == [] and drain.attempts == 0       # a DRAINING spill may still be writing
        for _ in range(100):
            if sink.rows:
                break
            await asyncio.sleep(0.02)
        assert [r.entity_id for r in sink.rows] == ["t1"]
    finally:
        await drain.stop()


async def test_torn_tail_baseline_is_reset_from_the_term(tmp_path: Path) -> None:
    """Before: the standby service kept its pre-term baseline, so a tail the term's service had already
    paged was paged again after DRAINING."""
    path = tmp_path / "spill.jsonl"
    torn = path.with_name(path.name + ".torn")
    torn.write_bytes(b"x" * 10)
    pages = _Pages()
    drain = NodeSpillDrain(_Sink(), path, node="n1", clock_ms=lambda: 1, pager=pages, idle_s=0.01, retry_s=0.01)
    await drain.drain_at_startup()                           # baseline 10 (nothing new: no page)
    assert pages.got == []
    torn.write_bytes(b"x" * 25)                              # moved aside and paged during the term
    drain.start(torn_baseline=25)
    try:
        await asyncio.sleep(0.1)
        assert pages.got == []                               # not paged a second time
        torn.write_bytes(b"x" * 40)                          # a new tail after the term: still paged
        for _ in range(100):
            if pages.got:
                break
            await asyncio.sleep(0.02)
        assert [p[1] for p in pages.got] == ["integrity:spill_torn_tail"]
    finally:
        await drain.stop()


class _TornSession(Session):
    flushing = True

    def spill_torn_bytes(self) -> int:
        return 42

    def spill_busy(self) -> bool:
        return self.flushing


async def test_node_restarts_standby_drain_after_draining_with_delay_and_term_baseline() -> None:
    w = World()
    holder: list[NodeController] = []
    spy = StandbyWorkSpy(holder)
    keeper = LeaseKeeper(w.etcd, clock=w.mono, fence=w.exits)

    def factory(epoch: Epoch) -> Session:
        s = _TornSession(w.init)
        w.sessions.append(s)
        return s

    c = NodeController("n1", keeper=keeper, board=HealthBoard(w.etcd, "n1"), probe=w, terms=w, cluster=w, local_redis=w,
                       session_factory=factory, exit_fn=w.exits, clock_ms=w.now_ms, monotonic=w.mono,
                       require_keeper_thread=False, standby_work=spy, standby_delay_after_drain_s=1.5)
    holder.append(c)
    await c.startup()
    assert spy.starts == [(0.0, None)] and w.flushes == 1  # STANDBY entry: FLUSHDB, then the drain at once
    await c.step()
    c.request_drain("abdicate", exit_after=False)
    await c.step()
    assert spy.starts[-1] == (1.5, 42) and w.flushes == 2
    busy = spy.busy[-1]                                      # the term's DRAINING flush, still running
    assert busy is not None and busy() is True
    w.sessions[-1].flushing = False  # type: ignore[attr-defined]
    assert busy() is False


async def test_standby_spill_drain_waits_for_the_terms_draining_flush(tmp_path: Path) -> None:
    """Skeptic fix: a DRAINING spill append that outlived the drain budget could land between the standby
    drain's read and its truncate (rows lost). The standby drain now waits while the term's flush runs."""
    path = tmp_path / "spill.jsonl"
    sink = _Sink()
    drain = NodeSpillDrain(sink, path, node="n1", clock_ms=lambda: 1, idle_s=0.01, retry_s=0.01)
    _append_spill(path, [_row(1)])
    flushing = {"on": True}
    drain.start(delay_s=0.05, busy=lambda: flushing["on"])
    try:
        await asyncio.sleep(0.4)
        assert sink.rows == [] and drain.attempts == 0       # still waiting on the term's flush
        _append_spill(path, [_row(2)])                       # the late append lands
        flushing["on"] = False
        for _ in range(100):
            if len(sink.rows) == 2:
                break
            await asyncio.sleep(0.02)
        assert [r.entity_id for r in sink.rows] == ["t1", "t2"]
    finally:
        await drain.stop()


async def test_session_reports_a_draining_flush_that_outlives_its_budget(built: Built) -> None:
    s = built.session
    s._t = dataclasses.replace(FAST_TIMINGS, drain_close_s=0.05, drain_flush_s=0.05, drain_spill_s=0.05,
                               drain_record_s=0.05, drain_cluster_s=0.05, drain_aclose_s=0.05)
    release = asyncio.Event()

    async def stalled_flush() -> int:                        # a spill append stuck on a stalled disk
        await release.wait()
        return 0

    s.spooler.flush = stalled_flush  # type: ignore[method-assign]
    assert s.spill_busy() is False
    await asyncio.wait_for(s.drain("switchover"), 5.0)       # DRAINING does not wait past its budget
    assert s.spill_busy() is True
    release.set()
    await asyncio.sleep(0.01)
    assert s.spill_busy() is False


# ============================================================================================ health probe
async def test_spill_size_stat_runs_off_the_event_loop(tmp_path: Path) -> None:
    redis = fakeredis.FakeAsyncRedis()

    async def chrony() -> float:
        return 0.1

    probe = NodeHealthProbe(NODE, pg=FakeSource(FakeConn()), redis=redis, chrony=chrony,
                            spill_path=tmp_path / "spill.jsonl", timeout_s=0.2)
    on_main: list[bool] = []

    def slow_stat() -> int:
        on_main.append(threading.current_thread() is threading.main_thread())
        time.sleep(0.4)                                      # a stalled disk
        return 7

    probe._stat_spill = slow_stat  # type: ignore[method-assign]
    try:
        started = time.monotonic()
        rec = await probe.probe(1)
        assert time.monotonic() - started < 0.35            # the loop was never blocked for the stat
        assert rec.spill_bytes == 0 and on_main == [False]
        await asyncio.sleep(0.4)
        probe._stat_spill = lambda: 9  # type: ignore[method-assign]
        rec = await probe.probe(2)
        assert rec.spill_bytes == 9
    finally:
        await redis.aclose()


# ============================================================================================ log redaction
@pytest.mark.parametrize(("text", "expected"), [
    ("dsn postgresql://xtnl:s3cr/et@10.0.1.10:5432,10.0.1.11:5432/xtnl?sslmode=require end",
     "dsn postgresql://10.0.1.10:5432,10.0.1.11:5432/... end"),
    ("redis://:p@ss@127.0.0.1:6379/0", "redis://127.0.0.1:6379/..."),
    ("rediss://default:pw@cache.internal:6380", "rediss://cache.internal:6380"),
    ("postgres://db.internal/xtnl?password=SECRET", "postgres://db.internal/..."),
    ("connect failed: Redis('redis://u:SECRET@h:6379/0')", "connect failed: Redis('redis://h:6379/...')"),
])
def test_redaction_covers_redis_and_postgres_dsns(text: str, expected: str) -> None:
    from src.main import redact_urls
    out = redact_urls(text)
    assert out == expected
    assert "SECRET" not in out and "s3cr" not in out and "p@ss" not in out and "pw@" not in out
