"""S32 (plan RT-2; design §A2, §A7, §B5.1, §B13, §B14, §C2, PR4, PR13): the production plan of the optional services.

A real `EngineSession` over the in-memory backend, fakeredis (with lupa) as `redis-md`, fake ARM pools and recording
ARM channels. Synthetic instruments, series and keys only; no assertion message carries a value."""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field, fields as dataclass_fields, replace as dc_replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import fakeredis
import pytest

from src.calendar.machine import SessionState, SessionTransition
from src.calendar.market import MarketCalendar
from src.db.epoch import Epoch
from src.dto.entities import PositionDTO
from src.marketdata.clockstep import SUSPEND_AFTER_MS
from src.marketdata.config import MdSettings
from src.marketdata.store.client import MdClients, MdReadClient, MdWriteClient
from src.marketdata.store.runner import ScriptRunner
from src.marketdata.types import DataUnavailable, SeriesId
from src.notifications.factory import ARM_CHANNEL_NAMES, ChannelSet
from src.observability.metrics import CONTRACT, MetricsRegistry
from src.oversee.notify import RecordingChannel
from src.pubsub.connection import ConnState
from src.db.fence import EpochMismatch
from src.riskmgr.budget import ARM_SCOPE, MD_DOWN, ErrorBudget
from src.riskmgr.config import FailSettings
from src.riskmgr.failed import FailedStores
from src.runtime.deadman import ArmFailedBeat
from src.runtime.optional import EXEMPT_COMPLETE, OptionalBootstrapRunner, OptionalDeps, make_optional_bootstrap
import src.runtime.optional_plan as plan_mod
from src.runtime.optional_plan import (
    ARM_KINDS,
    ARM_RELAY_TASK,
    ARM_STATUS_TASK,
    ARM_TASK,
    ARM_TRACKER_TASK,
    MD_BARS_TASK,
    MD_INDICATORS_TASK,
    MD_KINDS,
    MD_WRITER_TASK,
    STAMP_S,
    OptionalRuntime,
    PayloadError,
    ProductionPlan,
    default_arm_channels,
    parse_backfill,
    production_plan,
)
from src.runtime.optional_settings import MD_ENABLED_VAR, OptionalFlags
from src.runtime.ports import OperatorRequest
from src.runtime.request_handlers import Accepted, Refused
from src.runtime.sleeplog import FILE_NAME as SLEEP_LOG_FILE, SleepAwareRestarts, SleepFenceLog
from src.runtime.session import EngineSession, SessionContext
from src.runtime.supervisor import TaskSupervisor
from src.runtime.wiring import EngineHooks, build_engine
from tests.calendar.helpers import calendar, fx, ny
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import ACCOUNT, ADAPTER, FAST_TIMINGS, NODE, FakeLease, become_leader, fixture, \
    mock_adapter, wall_ms
from tests.marketdata.conftest import FakeAof, RunIds
from tests.riskmgr.failed_fakes import T0, FakeCalendar, FakeHealth, FakePg
from tests.riskmgr.tracker_fakes import FakeClock
from tests.runtime.fakes import FakeClock as EtcdClock
from tests.runtime.fakes import FakeEtcd
from tests.runtime.test_followups_4_9 import FakeQueue
from tests.runtime.test_optional_bootstrap import ARM_CHANNELS, EPOCH, MD_ENV, FakeArmPool, Pages, clean_scan, \
    md_clients_over, run_id, until
from tests.runtime.test_wiring import MOCK, cfg

#: The ARM runs only while Oversee runs the S07 stop rule (`oversee_c4`, R14 review): as `build_engine` sets it from
#: `XTNL_OVERSEE_STOP_AT_ENTRY=1` (in `ARM_CHANNELS`).
pytestmark = pytest.mark.usefixtures("stop_at_entry_on")


# ============================================================================================ helpers
def recording_channels(_session: EngineSession) -> ChannelSet:
    return ChannelSet(tuple(RecordingChannel(name) for name in ARM_CHANNEL_NAMES), (), ())


def plan() -> Any:
    return production_plan(MD_ENV, arm_channels=recording_channels)


@dataclass
class HangPools:
    """ARM pools that answer until `hang` is set; then every acquire and the graceful close hang (a query in
    flight)."""

    hang: bool = False
    built: list[FakeArmPool] = field(default_factory=list)

    def __call__(self, application_name: str) -> Any:
        outer = self

        class Pool(FakeArmPool):
            def acquire(self) -> AbstractAsyncContextManager[Any]:
                if outer.hang:
                    return _hanging_cm()
                return super().acquire()

            async def close(self) -> None:
                if outer.hang:
                    await asyncio.Event().wait()
                await super().close()

        pool = Pool(application_name)
        self.built.append(pool)
        return pool


@asynccontextmanager
async def _hanging_cm() -> AsyncIterator[Any]:
    await asyncio.Event().wait()
    yield None


def make_session(tmp_path: Path, redis: Any, *, flags: OptionalFlags, pages: Pages,
                 sup: TaskSupervisor | None = None, sleep_log: SleepFenceLog | None = None) -> EngineSession:
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [mock_adapter(fixture(wall_ms()))], lease=FakeLease(),
                         run_id_reader=run_id, exit_fn=lambda _r: None, timings=FAST_TIMINGS, page_sink=pages,
                         optional_flags=flags, sleep_log=sleep_log)
    s = EngineSession(ctx, EPOCH)
    s.init_deps(sup or TaskSupervisor([], exit_fn=lambda _r: None))
    return s


def deps(tmp_path: Path, md_redis: Any, env: dict[str, str], **kw: Any) -> OptionalDeps:
    base: dict[str, Any] = {"environ": env, "repo_root": tmp_path, "spool_dir": tmp_path / "spool",
                            "md_clients": md_clients_over(md_redis), "scan": clean_scan, "retry_base_s": 0.01,
                            "retry_max_s": 0.05, "park_stamp_s": 0.01, "plan": plan()}
    base.update(kw)
    return OptionalDeps(**base)


@asynccontextmanager
async def running(runner: OptionalBootstrapRunner) -> AsyncIterator[EngineSession]:
    s = runner.session
    sup = s._sup
    assert sup is not None
    s.add_optional_task(runner.spec())
    try:
        yield s
    finally:
        s.stop_event.set()
        await s.aclose(1.0)
        await sup.stop_all()


def runtime_of(runner: OptionalBootstrapRunner) -> OptionalRuntime:
    svc = runner.services
    assert svc is not None and isinstance(svc.runtime, OptionalRuntime)
    return svc.runtime


@pytest.fixture
async def redis() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    r = fakeredis.FakeAsyncRedis()
    try:
        yield r
    finally:
        await r.aclose()


@pytest.fixture
async def md_redis() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    r = fakeredis.FakeAsyncRedis()
    try:
        yield r
    finally:
        await r.flushall()
        await r.aclose()


# ============================================================================================ market data
async def test_md_plan_adds_the_tasks_and_handlers(tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, MD_ENV))
    async with running(runner):
        await until(lambda: runner.md_started)
        for name in (MD_WRITER_TASK, MD_BARS_TASK, MD_INDICATORS_TASK, ARM_STATUS_TASK):
            assert s.has_task(name), name
        assert not s.has_task(ARM_TASK) and not s.has_task(ARM_RELAY_TASK)
        assert MD_KINDS <= s.request_kinds() and not (ARM_KINDS & s.request_kinds())
        rt = runtime_of(runner)
        assert s.optional_view is rt and rt.graph is None
        specs = {spec.name: spec for spec in rt.md_tasks()}
        assert {n: sp.max_stamp_age_s for n, sp in specs.items()} == {n: STAMP_S[n] for n in specs}
        assert not any(sp.critical for sp in specs.values())
        assert specs[MD_INDICATORS_TASK].on_quarantine is not None
        svc = runner.services
        assert svc is not None and not svc.md.global_failed() and not svc.md.global_failed.bound
        # the writer task runs: the term-start bootstrap writes md:boot over fakeredis
        await until(lambda: svc.md.writer.boot is not None, timeout=10.0)
        # the ARM is off: the dead-man fields stay absent
        assert rt.arm_failed() is None and rt.arm_oldest_undelivered_notification_ms() is None
        assert pages.titles() == []


async def test_md_requests_are_enqueued_or_refused_without_blocking(tmp_path: Path, redis: Any,
                                                                    md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, MD_ENV))
    series = SeriesId.parse(f"{ADAPTER}:SYN1:15m:BID:BROKER").canonical
    queue = FakeQueue([
        OperatorRequest(1, "md_backfill", series, "alice", payload='{"v": 2}'),
        OperatorRequest(2, "md_backfill", "not-a-series", "alice", payload={}),
        OperatorRequest(3, "md_backfill", series, "alice", payload={"v": 1, "from_bucket_ms": 0, "to_bucket_ms": 0}),
        OperatorRequest(4, "md_rebuild", "0123456789abcdef", "alice"),
        OperatorRequest(5, "md_writer_reset", MD_WRITER_TASK, "alice"),
        OperatorRequest(6, "md_rehash", "md", "alice"),
    ])
    s.operator_requests = queue
    async with running(runner):
        await until(lambda: runner.md_started)
        await asyncio.wait_for(s.operator_pass(), FAST_TIMINGS.operator_poll_s)
        results = {rid: r for _, rid, r in queue.consumed}
        assert results[1] == "refused:bad_payload" and results[2] == "refused:bad_payload"
        assert results[3] == "refused:not_demanded" and results[4].startswith("refused:")
        assert 5 not in results and 6 not in results          # accepted by md-writer, answered later

        async def answered() -> bool:
            await s.operator_pass()
            return {5, 6} <= {rid for _, rid, _r in queue.consumed}

        end = time.monotonic() + 10.0
        while not await answered():
            assert time.monotonic() < end, "md-writer work items not answered"
            await asyncio.sleep(0.05)
        results = {rid: r for _, rid, r in queue.consumed}
        assert results[5] == "applied" and results[6] == "moot:no_retained_key"
        assert pages.titles() == []


def test_parse_backfill_payloads() -> None:
    series, a, b = parse_backfill(f"{ADAPTER}:SYN1:15m:BID:BROKER",
                                  {"v": 1, "from_bucket_ms": 900_000, "to_bucket_ms": 1_800_000})
    assert series.symbol == "SYN1" and (a, b) == (900_000, 1_800_000)
    for bad in ({"v": 1, "from_bucket_ms": -1, "to_bucket_ms": 0}, {"v": 1, "from_bucket_ms": True,
                                                                    "to_bucket_ms": 0}, [], "{", {"v": 9},
                {"from_bucket_ms": 0, "to_bucket_ms": 0},                       # no version
                {"v": True, "from_bucket_ms": 0, "to_bucket_ms": 0},            # True == 1, but not the version
                {"v": 1.0, "from_bucket_ms": 0, "to_bucket_ms": 0}, None, '{"from_bucket_ms": 0}'):
        with pytest.raises(PayloadError):
            parse_backfill(f"{ADAPTER}:SYN1:15m:BID:BROKER", bad)


async def test_tick_symbols_reach_the_orchestrator_and_the_schedule_but_decide_nothing(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True), pages=pages)
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, "XTNL_TICK_SYMBOLS": f"{ADAPTER}:SYN9"}))
    async with running(runner):
        await until(lambda: runner.md_started)
        assert s.orchestrator.md_tick_symbols(ADAPTER) == frozenset({"SYN9"})
        assert tuple(s.md_schedule_symbols(ADAPTER)) == ("SYN9",) and "SYN9" in s._schedule_symbols(ADAPTER)
        assert s.orchestrator.is_md_only(ADAPTER, "SYN9")
        # an SM10 transition of the tick-only symbol schedules no open RESYNCING and no clean-mark write
        resyncs: list[str] = []
        s.orchestrator.market_open_resync = lambda aid, open_ms: resyncs.append(aid)  # type: ignore[method-assign]
        background = len(s._background)
        s._on_transition(SessionTransition(ADAPTER, "SYN9", SessionState.CLOSED, SessionState.OPEN, wall_ms(),
                                           "schedule"))
        assert resyncs == [] and len(s._background) == background
        # a held symbol still does
        s._on_transition(SessionTransition(ADAPTER, "EURUSD", SessionState.CLOSED, SessionState.OPEN, wall_ms(),
                                           "schedule"))
        assert resyncs == [ADAPTER]
        await asyncio.sleep(0.05)                               # let the held symbol's clean-mark write run


def test_an_ignored_symbol_never_decides_the_market_the_latest_open_or_readiness() -> None:
    """S05 open point 6: a stored tick-only schedule takes no part in `deciding_sessions` (fallback included),
    `latest_open_ms` or the PRE_OPEN readiness check; without the predicate the calendar is unchanged."""
    def build(ignored: Callable[[str, str], bool] | None) -> tuple[MarketCalendar, list[tuple[str, str]]]:
        ready: list[tuple[str, str]] = []
        cal = MarketCalendar(on_readiness=lambda a, r: ready.append((a, r)), ignored=ignored)
        cal.init_from_schedule(calendar(fx(symbol="TICK/ONLY")), ny(2026, 11, 15, 16, 52))   # PRE_OPEN
        cal.tick(ny(2026, 11, 15, 17, 1))
        return cal, ready

    plain, ready = build(None)
    assert ready == [("stonex", "pre_open")] and plain.market_open("stonex")
    assert plain.latest_open_ms("stonex", ny(2026, 11, 15, 17, 1)) == ny(2026, 11, 15, 17)
    cal, ready = build(lambda a, sym: sym == "TICK/ONLY")
    assert ready == [] and not cal.market_open("stonex")
    assert cal.latest_open_ms("stonex", ny(2026, 11, 15, 17, 1)) is None
    assert cal.state("stonex", "TICK/ONLY") is SessionState.OPEN          # its session is still known

    def raising(a: str, sym: str) -> bool:
        raise RuntimeError("boom")
    cal, ready = build(raising)                                             # a raising predicate ignores nothing
    assert ready == [("stonex", "pre_open")] and cal.market_open("stonex")


# ============================================================================================ ARM
async def arm_runner(tmp_path: Path, redis: Any, md_redis: Any, pools: Any, pages: Pages,
                     sup: TaskSupervisor | None = None,
                     sleep_log: SleepFenceLog | None = None) -> OptionalBootstrapRunner:
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages, sup=sup,
                     sleep_log=sleep_log)
    return OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS}, arm_pool=pools))


async def test_arm_plan_binds_the_budget_and_reports_failed(tmp_path: Path, redis: Any, md_redis: Any) -> None:
    pages = Pages()
    runner = await arm_runner(tmp_path, redis, md_redis, HangPools(), pages)
    async with running(runner) as s:
        await until(lambda: runner.arm_started)
        for name in (ARM_TASK, ARM_RELAY_TASK, ARM_TRACKER_TASK):
            assert s.has_task(name), name
        assert ARM_KINDS <= s.request_kinds()
        assert {"arm_disarm", "arm_repin", "arm_rearm"} <= s.request_kinds()          # S36b: the tracker is wired
        rt = runtime_of(runner)
        graph = rt.graph
        svc = runner.services
        assert graph is not None and svc is not None
        # S34: the arm-relay's drain flag is the operator controls' (arm_controls, read by the arm-status loop)
        assert graph.controls_store is not None and graph.relay._drain == rt.controls.drain  # noqa: SLF001
        assert not graph.relay._drain()  # noqa: SLF001
        assert svc.md.faults.bound and svc.md.global_failed.bound and not svc.md.global_failed()
        # S38e: every clear-probe port is wired (no FAILED scope kind answers probe_not_wired), md-bars' bucket notes
        # go through the relay to the budget, and the demand book reads md-writer's measured memory
        ports = graph.failed._probe_ports  # noqa: SLF001
        assert ports is not None and all(getattr(ports, f.name) is not None for f in dataclass_fields(ports))
        assert svc.md.bars._buckets is svc.md.faults and svc.md.demand._memory_ratio is not None  # noqa: SLF001
        assert rt.arm_failed() == ArmFailedBeat(0, False)
        # the open episodes unreadable: the flag agrees with the dead-man and the metric (global FAILED, fail closed)
        scopes = graph.failed.scopes
        ready, unread = scopes.ready, scopes.unread
        scopes.ready, scopes.unread = False, True
        assert svc.md.global_failed() and rt.arm_failed() == ArmFailedBeat(1, True)
        scopes.ready, scopes.unread = ready, unread
        assert not svc.md.global_failed()
        # an ARM task circuit opening again after its half-open retry: ARM global FAILED, in memory at once
        rt.circuits.reopen(MD_BARS_TASK)
        assert graph.failed.scopes.global_failed and svc.md.global_failed()
        assert rt.arm_failed() == ArmFailedBeat(1, True)
        # md-bars and md-indicators stop writing (§B14): a pass / cycle writes nothing
        ind, bars = svc.md.indicators.counters, svc.md.bars.counters
        written, cycles, passes = ind.values_written, ind.failed_cycles, bars.failed_passes
        await svc.md.indicators.cycle()
        await svc.md.bars.step()
        assert ind.failed_cycles == cycles + 1 and ind.values_written == written
        assert bars.failed_passes == passes + 1 and bars.reads == 0 and bars.lane_reads == 0
        assert s.optional_view is rt
        from src.runtime.session import _DeadmanView
        assert _DeadmanView(s).arm_failed() == ArmFailedBeat(1, True)
        # the term's metric series: every one is in the exporter contract and renders (nothing dropped)
        samples = s.metrics_samples(leader=True)
        names = {x.name for x in samples}
        assert {"xtnl_md_series", "xtnl_md_events_total", "xtnl_arm_failed_scopes", "xtnl_arm_failed_global"} <= names
        assert [x.value for x in samples if x.name == "xtnl_arm_failed_scopes"] == [1.0]
        # S37: the rest of the design §C2 list the term holds (metrics.md "Design §C2 metric list")
        assert {"xtnl_md_shed_step", "xtnl_md_queue_depth", "xtnl_arm_paused"} <= names
        md_components = {x.labels["component"] for x in samples if x.name == "xtnl_md_events_total"}
        assert {"writer", "bars", "indicators", "service", "hook", "crosscheck"} <= md_components
        assert {x.labels["queue"] for x in samples if x.name == "xtnl_md_queue_depth"} == {"closed_bars", "pending_ticks"}
        arm_components = {x.labels["component"] for x in samples if x.name == "xtnl_arm_events_total"}
        assert graph.tracker is not None and graph.clock is not None
        # live review round 5 (§B6.3 REOPEN): the BarClock's evaluation port is the S21 join with the clean-spread
        # retry, over the session's installed clean-spread hook (the committer's and the orchestrator's)
        gate = graph.reopen_spread
        assert gate is not None and graph.clock._evaluator is gate  # noqa: SLF001
        assert gate._inner is graph.evaluator and gate._clean_spread == s.clean_spread  # noqa: SLF001
        assert s.clean_spread == s.rules.clean_spread or s.ctx.clean_spread is not None
        assert {"failed", "budget", "tracker", "barclock", "stopreads", "notices", "evaluation"} <= arm_components
        assert {x.labels["view"] for x in samples if x.name == "xtnl_arm_positions"} == {
            "armed", "unmanaged", "overdue", "unarmed"}
        # S38g (round 14): the sleep wiring and the liveness watch of the term
        assert graph.clock._cfg.suspend_after_ms == SUSPEND_AFTER_MS  # noqa: SLF001
        assert svc.md.writer._detector.suspend_after_ms == SUSPEND_AFTER_MS  # noqa: SLF001
        assert graph.clock._suspended == rt.suspend_latch.take  # noqa: SLF001
        assert isinstance(graph.failed._restarts, SleepAwareRestarts)  # noqa: SLF001
        assert graph.grid is not None and rt.arm_overdue_positions() == 0 and rt.overdue_now(0) == {}
        assert graph.jobs is not None and graph.jobs._suspend_after_s == SUSPEND_AFTER_MS / 1000  # noqa: SLF001
        jobs_suspends = graph.jobs.suspends
        s.resume_after_sleep(30_000)                                           # the uptime tracker's signal
        assert rt.suspend_latch.notes >= 1 and rt.suspend_latch.take() and not rt.suspend_latch.take()
        assert graph.jobs.suspends == jobs_suspends + 1                        # S39b: it reaches every job
        # S39b: the correction window rebuild and the history execution-time source are wired
        assert graph.clock._recent is graph.clock._store                       # noqa: SLF001
        assert graph.tracker._exec_times is not None                          # noqa: SLF001
        rt.liveness_pass(s.ctx.clock_ms())                                     # never raises
        assert rt.liveness_errors == 0
        # S37 review: the memory ratio is exported only while the writer's last memory read succeeded (never stale)
        real_status = svc.md.writer.status

        def ratio_of(ok: bool) -> list[float]:
            svc.md.writer.status = lambda: dc_replace(  # type: ignore[method-assign]
                real_status(), memory_ratio=0.75, memory_ok=ok)
            return [x.value for x in s.metrics_samples(leader=True) if x.name == "xtnl_md_redis_used_memory_ratio"]
        try:
            assert ratio_of(True) == [0.75] and ratio_of(False) == []
        finally:
            svc.md.writer.status = real_status  # type: ignore[method-assign]
        # PRE skipped by reason: a reason code is the event; a non-code key is never exported (bounded labels)
        graph.clock.counters.drop("pre_market_closed")
        graph.clock.counters.drop("Not A Code!")
        skipped = {x.labels["event"]: x.value for x in s.metrics_samples(leader=True)
                   if x.name == "xtnl_arm_events_total" and x.labels["component"] == "pre_skipped"}
        assert skipped["pre_market_closed"] >= 1.0 and "Not A Code!" not in skipped
        for x in samples:
            assert x.name in CONTRACT and set(x.labels) == set(CONTRACT[x.name][1]), x.name
        reg = MetricsRegistry("n1")
        reg.register("term", lambda: samples)
        text = reg.render()
        assert reg.dropped == 0 and "xtnl_arm_failed_scopes" in text
        # the status row carries counts and codes only
        row = rt.status_row()
        assert row.failed_scopes == 1 and row.failed_global and row.node == NODE
        assert row.running is (graph.clock is not None and graph.started)
        # the FAILED ping goes to the log mirror, never to EngineSession.page
        assert not any("FAILED" in t for t in pages.titles())
        # an unreadable FAILED state is reported fail closed: by the view, and by the session over a raising view
        real_summary = graph.failed.summary

        def boom(*_a: Any, **_k: Any) -> Any:
            raise RuntimeError("boom")

        graph.failed.summary = boom  # type: ignore[method-assign]
        try:
            assert rt.arm_failed() == ArmFailedBeat(1, True) and _DeadmanView(s).arm_failed() == ArmFailedBeat(1, True)
            assert {(x.name, x.value) for x in rt.metrics_samples() if x.name.startswith("xtnl_arm_failed")} == {
                ("xtnl_arm_failed_scopes", 1.0), ("xtnl_arm_failed_global", 1.0)}
        finally:
            graph.failed.summary = real_summary  # type: ignore[method-assign]
        for name in ("arm_failed", "metrics_samples"):
            setattr(rt, name, boom)
        try:
            assert _DeadmanView(s).arm_failed() == ArmFailedBeat(1, True)
            got = {(x.name, x.value) for x in s.metrics_samples(leader=True) if x.name.startswith("xtnl_arm_failed")}
            assert got == {("xtnl_arm_failed_scopes", 1.0), ("xtnl_arm_failed_global", 1.0)}
        finally:
            for name in ("arm_failed", "metrics_samples"):
                delattr(rt, name)


SLEEP_TITLE = "ARM: engine restarted after a PC sleep"


async def test_a_sleep_restart_is_paged_once_and_left_out_of_the_restart_budget(tmp_path: Path, redis: Any,
                                                                                md_redis: Any) -> None:
    """Round 14 review (plan S38g item 4): the node's sleep-fence record holds one unreported fence (the lease
    keeper wrote it right before the fence exit). The ARM's start pages it exactly once; a second start (the next
    term over the same file) pages nothing. The FAILED manager's restart history (`SleepAwareRestarts` bound to the
    runtime's off-loop read of the same record) leaves out the `lost` term end 40 s after that fence and keeps a crash
    10 min later."""
    path = tmp_path / SLEEP_LOG_FILE
    fence_at = wall_ms() - 120_000
    SleepFenceLog(path, clock_ms=lambda: fence_at).record(45.0)
    for term in (1, 2):
        pages = Pages()
        runner = await arm_runner(tmp_path, redis, md_redis, HangPools(), pages, sleep_log=SleepFenceLog(path))
        async with running(runner):
            await until(lambda: runner.arm_started)
            rt = runtime_of(runner)
            graph = rt.graph
            assert graph is not None
            await until(lambda: graph.started, timeout=10.0)
            await asyncio.sleep(0.05)                               # the report runs right after the start
            assert pages.titles().count(SLEEP_TITLE) == (1 if term == 1 else 0), term
            assert rt.sleep_restarts_reported == (1 if term == 1 else 0)
            restarts = graph.failed._restarts  # noqa: SLF001
            assert isinstance(restarts, SleepAwareRestarts)
            crash = fence_at + 600_000

            class History:
                async def unclean_ends_since(self, since_ms: int) -> list[int]:
                    return [fence_at + 40_000, crash]

            restarts._inner = History()  # noqa: SLF001
            assert await restarts.unclean_ends_since(0) == [crash]
        if term == 1:
            body = next(b for _, t, b in pages.items if t == SLEEP_TITLE)
            assert "45 s" in body and str(fence_at) in body
    assert [f.reported for f in SleepFenceLog(path).fences()] == [True]


async def test_a_reset_task_behind_a_hanging_clear_probe_is_applied_within_one_poll(
        tmp_path: Path, redis: Any, md_redis: Any) -> None:
    """Design §B13 / plan S32: `arm_clear_failed` only enqueues to the FAILED probe runner; its probe hangs (the
    ARM-pool test write never returns), and a `reset_task` filed behind it is applied within one
    `operator_poll_s`."""
    pages = Pages()
    runner = await arm_runner(tmp_path, redis, md_redis, HangPools(), pages)
    async with running(runner) as s:
        await until(lambda: runner.arm_started)
        graph = runtime_of(runner).graph
        assert graph is not None
        fm = graph.failed
        runtime_of(runner).circuits.reopen(MD_BARS_TASK)
        ep = fm.scopes.episodes[ARM_SCOPE]
        ep.committed = True
        fm.scopes.ready = True
        probing = asyncio.Event()

        class HangingStore:
            async def note_probe(self, failure_id: str, at_ms: int, result: str) -> bool:
                probing.set()
                await asyncio.Event().wait()
                return True

            def __getattr__(self, name: str) -> Any:
                async def down(*_a: Any, **_k: Any) -> Any:
                    raise ConnectionError("down")
                return down

        fm._stores = FailedStores(arm=HangingStore(), engine=HangingStore())  # type: ignore[arg-type]
        queue = FakeQueue([OperatorRequest(1, "arm_clear_failed", ARM_SCOPE.target, "alice"),
                           OperatorRequest(2, "reset_task", MD_BARS_TASK, "bob")])
        s.operator_requests = queue
        await asyncio.wait_for(s.operator_pass(), FAST_TIMINGS.operator_poll_s)
        assert queue.consumed == [(EPOCH.token, 2, "applied")]
        await asyncio.wait_for(probing.wait(), 5.0)                    # the arm task started the probe; it hangs
        queue.reqs.append(OperatorRequest(3, "reset_task", MD_BARS_TASK, "bob"))
        started = time.monotonic()
        await asyncio.wait_for(s.operator_pass(), FAST_TIMINGS.operator_poll_s)
        assert time.monotonic() - started < FAST_TIMINGS.operator_poll_s
        assert {rid: r for _, rid, r in queue.consumed} == {2: "applied", 3: "applied"}
        assert fm.probing() == (ARM_SCOPE.target,)


# ============================================================================================ loop bodies (units)
class Stamps:
    def __init__(self) -> None:
        self.stamps = 0
        self.exempt: list[str | None] = []

    def stamp(self) -> None:
        self.stamps += 1

    def set_exempt(self, reason: str | None) -> None:
        self.exempt.append(reason)


class SelectPool:
    """An ARM pool whose `SELECT 1` answers, raises or hangs."""

    def __init__(self) -> None:
        self.mode = "ok"

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        mode = self.mode

        @asynccontextmanager
        async def cm() -> AsyncIterator[Any]:
            if mode == "hang":
                await asyncio.Event().wait()
            if mode == "raise":
                raise ConnectionError("arm pool down")
            yield SimpleNamespace(fetchrow=_select_one)
        return cm()


async def _select_one(_sql: str) -> Any:
    return (1,)


def observe_rig(armed: tuple[str, ...] = ()) -> tuple[Any, Any, FakeClock, SelectPool]:
    """A stand-in `OptionalRuntime` for `_observe`: the session clock and calendar (every market OPEN), the writer's
    flags, the armed positions' adapters, and an ARM graph over a real `ErrorBudget`."""
    clock = FakeClock(T0)
    budget = ErrorBudget(FailSettings(md_down_s=300, pool_down_s=300), calendar=FakeCalendar(),
                         health=FakeHealth(), pg=FakePg(clock))
    pool = SelectPool()
    writer = SimpleNamespace(boot=None, bootstrap_wanted=None, counters=SimpleNamespace(flush_errors=0))
    rt = SimpleNamespace(
        session=SimpleNamespace(ctx=SimpleNamespace(clock_ms=lambda: clock.now), adapter_ids=(ADAPTER, "other"),
                                calendar=SimpleNamespace(market_open=lambda _aid: True)),
        svc=SimpleNamespace(md=SimpleNamespace(writer=writer)), armed_adapter_ids=lambda: armed)
    graph = SimpleNamespace(budget=budget, arm=SimpleNamespace(pool=pool), flush_errors_seen=0, pool_ok=None)
    return rt, graph, clock, pool


async def observe(rt: Any, graph: Any, clock: FakeClock, seconds: int, stamps: Stamps | None = None) -> None:
    for _ in range(seconds):
        clock.now += 1_000
        await OptionalRuntime._observe(rt, graph, stamps or Stamps(), asyncio.Event())  # noqa: SLF001


def md_timer_started(graph: Any) -> bool:
    return graph.budget._md.first_at_ms is not None                         # noqa: SLF001 - the md timer


async def test_redis_md_down_counts_only_for_an_armed_positions_market() -> None:
    """Design §B14: `fail_md_down_s` counts only while an armed position's market is OPEN. Nothing is armed before
    S33, so `redis-md` down past the setting with every market OPEN gives no FAILED; with an armed adapter it does."""
    rt, graph, clock, _pool = observe_rig()
    await observe(rt, graph, clock, 400)                      # unusable (no store loaded) for 400 s > 300 s
    assert graph.budget.take() == [] and graph.pool_ok is True and not md_timer_started(graph)
    rt, graph, clock, _pool = observe_rig(armed=(ADAPTER,))
    await observe(rt, graph, clock, 400)
    assert [(f.scope, f.reason) for f in graph.budget.take()] == [(ARM_SCOPE, MD_DOWN)]


async def test_observe_feeds_the_pool_and_md_usable_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    """`_observe`: an ARM-pool `SELECT 1` that raises or outlives `POOL_CHECK_S` is `observe_pool(arm_ok=False)`
    (exempt while it waits); `redis-md` is usable only with the store loaded, no bootstrap wanted and no new flush
    error since the last observation."""
    monkeypatch.setattr(plan_mod, "POOL_CHECK_S", 0.05)
    rt, graph, clock, pool = observe_rig(armed=(ADAPTER,))
    stamps = Stamps()
    for mode, ok in (("ok", True), ("raise", False), ("hang", False), ("ok", True)):
        pool.mode = mode
        await observe(rt, graph, clock, 1, stamps)
        assert graph.pool_ok is ok and graph.budget.pool_failing is (not ok), mode
    assert stamps.exempt == ["postgres", None] * 4
    writer = rt.svc.md.writer
    writer.boot = object()
    await observe(rt, graph, clock, 1)
    assert not md_timer_started(graph)                                       # usable: the clean result
    writer.counters.flush_errors = 1                                         # a new flush error: unusable once
    await observe(rt, graph, clock, 1)
    assert md_timer_started(graph) and graph.flush_errors_seen == 1
    await observe(rt, graph, clock, 1)                                       # no new error: usable again
    assert not md_timer_started(graph)
    writer.bootstrap_wanted = "store_lost"
    await observe(rt, graph, clock, 1)
    assert md_timer_started(graph)


async def test_status_loop_saves_the_record_and_the_row_and_stops_writing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The `arm-status` loop: this node's §A7 record every `STATUS_EVERY_S` (the S16 obligation), the `arm_status`
    row once the ARM graph exists (exempt while it waits), stamps between slices of the wait, nothing written after
    the stop, and a lost fence raises."""
    monkeypatch.setattr(plan_mod, "STATUS_EVERY_S", 0.05)
    monkeypatch.setattr(plan_mod, "STAMP_SLICE_S", 0.01)
    saves: list[int] = []
    rows: list[str] = []

    async def save_record() -> bool:
        saves.append(1)
        return True

    class Status:
        fenced = False

        async def upsert(self, row: str) -> None:
            if self.fenced:
                raise EpochMismatch("fenced")
            rows.append(row)

    status = Status()
    md_writer: list[Any] = [None]
    rt: Any = SimpleNamespace(
        svc=SimpleNamespace(md=SimpleNamespace(bootstrap=SimpleNamespace(save_record=save_record))), graph=None,
        record_saves=0, status_writes=0, status_errors=0, status_row=lambda: "row",
        _md_status_writer=lambda: md_writer[0], liveness_pass=lambda _now: None,
        session=SimpleNamespace(ctx=SimpleNamespace(clock_ms=lambda: 0)))
    stop = asyncio.Event()
    stamps = Stamps()
    task = asyncio.create_task(OptionalRuntime._status_loop(rt, stop, stamps))  # noqa: SLF001
    await until(lambda: len(saves) >= 2)
    assert rows == [] and rt.record_saves == len(saves)                      # no ARM graph, no engine pool: no row
    md_writer[0] = status                                                    # S36b: md only, the engine pool's row
    await until(lambda: len(rows) >= 1)
    md_writer[0] = None
    rows.clear()
    rt.status_writes = 0
    rt.graph = SimpleNamespace(arm=SimpleNamespace(status=status), controls_store=None)
    await until(lambda: len(rows) >= 2)
    assert rt.status_writes == len(rows) and "postgres" in stamps.exempt
    assert stamps.stamps >= 4 * len(saves)                                   # stamped through the waits
    stop.set()
    await asyncio.wait_for(task, 1.0)
    n_saves, n_rows = len(saves), len(rows)
    await asyncio.sleep(0.15)
    assert (len(saves), len(rows)) == (n_saves, n_rows)                       # nothing after the stop
    status.fenced = True
    with pytest.raises(EpochMismatch):
        await asyncio.wait_for(OptionalRuntime._status_loop(rt, asyncio.Event(), Stamps()), 1.0)  # noqa: SLF001


async def test_arm_loop_starts_failed_once_per_term_and_drains_on_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    """`FailedManager.start()` (spool replay, open episodes) runs once per term even when the open episodes are
    unreadable (`tick()` retries the read, the spool is never replayed again); the stop drains."""
    monkeypatch.setattr(plan_mod, "ARM_TICK_S", 0.01)
    calls = {"start": 0, "tick": 0, "drain": 0, "observe": 0}

    class Fm:
        async def start(self) -> bool:
            calls["start"] += 1
            return False                                  # open episodes unreadable

        async def tick(self) -> None:
            calls["tick"] += 1

        async def drain(self) -> None:
            calls["drain"] += 1

    async def observe_stub(*_a: Any) -> None:
        calls["observe"] += 1

    async def decide_stub(*_a: Any) -> None:
        calls["decide"] += 1

    async def notices_stub(*_a: Any) -> None:
        calls["notices"] += 1

    calls.update(decide=0, notices=0)
    graph = SimpleNamespace(failed=Fm(), started=False, tick_errors=0, clock=None)
    async def report_stub() -> int:
        return 0

    rt: Any = SimpleNamespace(arm_graph=lambda: graph, _observe=observe_stub, _after_tick=lambda _g: None,
                              _decide=decide_stub, _notice_pass=notices_stub, report_sleep_restarts=report_stub,
                              _arm_sleep_s=lambda _g, _t: 0.01, _close_clock=lambda _g: None)
    stop = asyncio.Event()
    task = asyncio.create_task(OptionalRuntime._arm_loop(rt, stop, Stamps()))  # noqa: SLF001
    await until(lambda: calls["tick"] >= 5)
    stop.set()
    await asyncio.wait_for(task, 1.0)
    assert calls["start"] == 1 and graph.started and calls["observe"] >= 5 and calls["drain"] == 1
    assert calls["decide"] >= calls["tick"] and calls["notices"] >= 1                 # S36b: BarClock and notices
    assert graph.tick_errors == 0


def test_default_arm_channels_close_their_own_client_only() -> None:
    """Without a term HTTP client the ARM channels get a client of their own, closed with the term (`arm-http`);
    with one, they share it and register nothing."""
    closers: list[str] = []

    class Client:
        async def aclose(self) -> None:
            return None

    own = Client()

    def register(name: str, *_fns: Any) -> bool:
        closers.append(name)
        return True

    def session(client: Any) -> Any:
        return SimpleNamespace(http_client=client, register_closer=register,
                               ctx=SimpleNamespace(http_client=lambda: own, smtp_factory=None))

    build = default_arm_channels({})
    assert isinstance(build(session(None)), ChannelSet) and closers == ["arm-http"]
    build(session(Client()))
    assert closers == ["arm-http"]


# ============================================================================================ isolation, DRAINING
class _Broken(fakeredis.FakeAsyncRedis):
    mode = "down"

    async def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        if self.mode == "hung":
            await asyncio.Event().wait()
        if self.mode == "raising":
            raise RuntimeError("redis-md bug")
        raise ConnectionError("redis-md refused")

    async def ping(self, **_kw: Any) -> bool:
        return bool(await self.execute_command("PING"))

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        await asyncio.Event().wait()                    # the redis-md closer hangs in every mode


def broken_clients(mode: str) -> Callable[[MdSettings], MdClients]:
    def build(_settings: MdSettings) -> MdClients:
        r = _Broken()
        r.mode = mode
        return MdClients(MdReadClient(r), MdWriteClient(r, ScriptRunner(r, run_id_reader=RunIds()), FakeAof()))
    return build


def leader_session(tmp_path: Path, redis: Any, adapter: Any, pages: Pages, *, flags: OptionalFlags,
                   d: OptionalDeps | None = None) -> EngineSession:
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=lambda _r: None, timings=FAST_TIMINGS, page_sink=pages, optional_flags=flags,
                         optional_bootstrap=make_optional_bootstrap(d) if d is not None else None)
    return EngineSession(ctx, EPOCH)


async def ingest_and_sweep(s: EngineSession, adapter: Any, n: int = 5) -> tuple[float, int]:
    """Commit `n` position updates one after the other: (the slowest commit's latency in seconds, the oldest
    RiskSweep pass age seen in ms, sampled after each commit and once more after 1.5 s)."""
    worst, oldest = 0.0, 0

    def pass_age(*, required: bool = False) -> int:
        passes = s.risksweep_pass_ms()
        assert passes or not required, "no RiskSweep pass"
        return max(0, wall_ms() - min(passes.values())) if passes else 0    # none listed yet: the first commit

    for k in range(n):
        before = s.tracking.last_commit_ms
        started = time.monotonic()
        adapter.push_order_update(PositionDTO(
            position_id="P-S32", account_id=ACCOUNT, adapter_id=ADAPTER, symbol="EURUSD", side="BUY",
            units=Decimal(1000 + k), raw_qty=Decimal(1000 + k), entry_price=Decimal("1.0850"),
            stop_loss=Decimal("1.0750"), status="OPEN", opened_by_client_order_id="P-S32",
            broker_updated_at_ms=wall_ms(), source="STREAM"))
        while s.tracking.last_commit_ms == before:
            assert time.monotonic() - started < 15.0, "ingestion stalled"
            await asyncio.sleep(0.01)
        worst = max(worst, time.monotonic() - started)
        oldest = max(oldest, pass_age())
        await asyncio.sleep(0.05)
    await asyncio.sleep(1.5)
    return worst, max(oldest, pass_age(required=True))


@pytest.mark.parametrize("mode", ["down", "hung", "raising"])
async def test_redis_md_broken_leaves_leader_ingestion_and_oversee_running_and_draining_fast(
        tmp_path: Path, redis: Any, mode: str) -> None:
    """Both flags on, `redis-md` down / hung / raising at start-up: the term reaches LEADER, ingestion commits and
    the RiskSweep keeps passing at the rate of a baseline term with both flags off (commit latency and pass age), no
    market data is served (no series LIVE, a decision read answers `DataUnavailable`), and DRAINING (with an ARM
    query in flight, and both optional closers hung: the ARM pool's and `redis-md`'s) finishes inside the 10 s
    deadline."""
    # baseline: the same term with both flags off
    (tmp_path / "base").mkdir()
    adapter = mock_adapter(fixture(wall_ms()))
    s = leader_session(tmp_path / "base", redis, adapter, Pages(), flags=OptionalFlags())
    leader = await become_leader(s, EPOCH)
    try:
        await until(lambda: s.orchestrator.runtime(ADAPTER).conn.state is ConnState.STREAMING, timeout=20.0)
        base_latency, base_age = await ingest_and_sweep(s, adapter)
    finally:
        await asyncio.wait_for(leader.shutdown(), 10.0)

    pages = Pages()
    pools = HangPools()
    d = deps(tmp_path, None, {**MD_ENV, **ARM_CHANNELS}, md_clients=broken_clients(mode), arm_pool=pools)
    adapter = mock_adapter(fixture(wall_ms()))
    s = leader_session(tmp_path, redis, adapter, pages, flags=OptionalFlags(md_enabled=True, arm_enabled=True), d=d)
    leader = await become_leader(s, EPOCH)
    try:
        s.start_optional_services()
        await until(lambda: s.has_task(MD_WRITER_TASK), timeout=10.0)
        await until(lambda: s.has_task(ARM_TASK), timeout=10.0)
        await until(lambda: s.orchestrator.runtime(ADAPTER).conn.state is ConnState.STREAMING, timeout=20.0)
        # ingestion and the RiskSweep run as in the baseline while redis-md is broken
        latency, age = await ingest_and_sweep(s, adapter)
        assert latency <= max(3 * base_latency, base_latency + 1.0), "commit latency grew"
        assert age <= base_age + 2_000 and age < 10_000, "RiskSweep pass age grew"
        # no market data is served: nothing LIVE, a decision read is unavailable
        assert all(x.labels.get("state") != "LIVE" or x.value == 0
                   for x in s.metrics_samples(leader=True) if x.name == "xtnl_md_series")
        rt = s.optional_view
        assert isinstance(rt, OptionalRuntime) and rt.svc.md.writer.boot is None
        read = await asyncio.wait_for(rt.svc.md.port.bars(SeriesId.parse(f"{ADAPTER}:SYN1:15m:BID:BROKER"), n=1),
                                      5.0)
        assert isinstance(read, DataUnavailable)
        pools.hang = True                                       # an ARM query is now in flight and never returns
        await asyncio.sleep(1.2)
    finally:
        started = time.monotonic()
        await asyncio.wait_for(leader.shutdown(), 10.0)
        elapsed = time.monotonic() - started
    assert elapsed < 10.0 and not leader.exits


# ============================================================================================ health registry
async def test_the_registry_is_published_only_with_the_md_flag(tmp_path: Path) -> None:
    on = {**MOCK, MD_ENABLED_VAR: "1"}
    engine = build_engine(cfg(on, tmp_path), environ=on,
                          hooks=EngineHooks(etcd=FakeEtcd(EtcdClock()), require_keeper_thread=False))
    try:
        tokens = engine.probe._registry()
        assert tokens and list(tokens) == sorted(set(tokens)) and all(":" in t for t in tokens)
    finally:
        await engine.aclose()
    off_dir = tmp_path / "off"
    off_dir.mkdir()
    engine = build_engine(cfg(MOCK, off_dir), environ=MOCK,
                          hooks=EngineHooks(etcd=FakeEtcd(EtcdClock()), require_keeper_thread=False))
    try:
        assert engine.probe.registry_tokens is None and engine.probe._registry() == ()
    finally:
        await engine.aclose()


async def test_build_engine_hands_the_production_plan_to_the_bootstrap(tmp_path: Path,
                                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    import src.runtime.optional as optional_mod
    import src.runtime.optional_plan as plan_mod

    seen: list[OptionalDeps] = []
    made: list[Any] = []
    real_plan, real_make = plan_mod.production_plan, optional_mod.make_optional_bootstrap

    def spy_plan(environ: Any, **kw: Any) -> Any:
        out = real_plan(environ, **kw)
        made.append(out)
        return out

    def spy_make(d: OptionalDeps) -> Any:
        seen.append(d)
        return real_make(d)

    monkeypatch.setattr(plan_mod, "production_plan", spy_plan)
    monkeypatch.setattr(optional_mod, "make_optional_bootstrap", spy_make)
    on = {**MOCK, MD_ENABLED_VAR: "1"}
    engine = build_engine(cfg(on, tmp_path), environ=on,
                          hooks=EngineHooks(etcd=FakeEtcd(EtcdClock()), require_keeper_thread=False))
    try:
        assert len(made) == 1 and len(seen) == 1 and seen[0].plan is made[0]
    finally:
        await engine.aclose()


# ============================================================================================ arm-config (S33)
PLANTED_DSN = "postgresql://xtnl_arm_reader:Pl4nted-Secret@db.planted-host.example:6543/postgres"
PLANTED = ("Pl4nted-Secret", "planted-host", PLANTED_DSN)


class _NeverReadSource:
    reads = 0

    async def read(self) -> Any:
        self.reads += 1
        raise AssertionError("not read in this test")


async def test_without_a_config_dsn_the_arm_config_is_idle_with_one_page(tmp_path: Path, redis: Any,
                                                                         md_redis: Any) -> None:
    from src.runtime.optional_plan import ARM_CONFIG_TASK

    pages = Pages()
    runner = await arm_runner(tmp_path, redis, md_redis, HangPools(), pages)
    async with running(runner) as s:
        await until(lambda: runner.arm_started)
        rt = runtime_of(runner)
        assert not s.has_task(ARM_CONFIG_TASK) and rt.accepted_config() is None
        assert rt.config_idle_reason == "no_dsn"
        assert pages.titles().count("ARM configuration idle") == 1
        row = rt.status_row()
        assert not row.paused and row.pause_reason is None and row.config_doc_hash is None


async def test_the_arm_config_task_runs_the_installer_and_feeds_the_status_row(tmp_path: Path, redis: Any,
                                                                               md_redis: Any) -> None:
    from src.armconfig.installer import PAUSE_CONFIG_HASH, ConfigInstaller, ConfigState
    from src.runtime.optional_plan import ARM_CONFIG_TASK

    pages = Pages()
    source = _NeverReadSource()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    plan_ = production_plan(MD_ENV, arm_channels=recording_channels, config_source=lambda _s: source)
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, {**MD_ENV, **ARM_CHANNELS}, arm_pool=HangPools(),
                                             plan=plan_))
    async with running(runner) as sess:
        await until(lambda: runner.arm_started)
        assert sess.has_task(ARM_CONFIG_TASK) and STAMP_S[ARM_CONFIG_TASK] == 120.0
        rt = runtime_of(runner)
        inst = rt.accepted_config()
        assert isinstance(inst, ConfigInstaller) and inst.accepted() is None
        assert "ARM configuration idle" not in pages.titles()
        inst.state = ConfigState.PAUSED
        row = rt.status_row()
        assert row.paused and row.pause_reason == PAUSE_CONFIG_HASH and row.config_doc_hash is None
        assert ("config_polls", inst.counters.polls) in row.counters
    assert source.reads == 0          # the mock adapter's instruments are not loaded in this rig: nothing checked


def test_known_ids_wait_for_every_instrument_map_and_name_routes() -> None:
    from src.runtime.optional_plan import OptionalRuntime as Rt

    runtimes = {"mock": SimpleNamespace(instruments={"SYN.A": 1}, accounts={"acct-1": 1, "acct-2": 2}),
                "mock2": SimpleNamespace(instruments={}, accounts={})}
    orch = SimpleNamespace(runtime=lambda aid: runtimes[aid])
    rt: Any = SimpleNamespace(session=SimpleNamespace(orchestrator=orch, adapter_ids=["mock", "mock2"]))
    assert Rt.known_ids(rt) is None                                        # mock2's map is not loaded yet
    runtimes["mock2"] = SimpleNamespace(instruments={"SYN.B": 1}, accounts={"acct-9": 1})
    known = Rt.known_ids(rt)
    assert known is not None and known.symbols == {"SYN.A", "SYN.B"} and known.adapters == {"mock", "mock2"}
    assert dict(known.accounts) == {"acct-1": "acct-1", "acct-2": "acct-2", "acct-9": "acct-9"}
    assert known.routes == {"*", "mock:acct-1", "mock:acct-2", "mock2:acct-9"}


def test_default_config_source_never_shows_the_dsn() -> None:
    import certifi

    from src.armconfig.client import CA_FILE_VAR, DSN_VAR, SupabaseConfigSource
    from src.runtime.optional_plan import default_config_source

    session: Any = SimpleNamespace()
    assert default_config_source({})(session) is None
    problems = default_config_source({DSN_VAR: PLANTED_DSN})(session)
    assert isinstance(problems, list) and problems and not any(p in " ".join(problems) for p in PLANTED)
    src = default_config_source({DSN_VAR: PLANTED_DSN, CA_FILE_VAR: certifi.where()})(session)
    assert isinstance(src, SupabaseConfigSource) and not any(p in repr(src) for p in PLANTED)


async def test_a_planted_dsn_reaches_no_page_or_status_row(tmp_path: Path, redis: Any, md_redis: Any,
                                                          caplog: pytest.LogCaptureFixture) -> None:
    """A bad config setting with a planted DSN: the idle page names the variable only; the status row and the log
    carry no part of the DSN."""
    import logging

    from src.armconfig.client import DSN_VAR

    caplog.set_level(logging.DEBUG)
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    env = {**MD_ENV, **ARM_CHANNELS, DSN_VAR: PLANTED_DSN}
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, env, arm_pool=HangPools(),
                                             plan=production_plan(env, arm_channels=recording_channels)))
    async with running(runner):
        await until(lambda: runner.arm_started)
        rt = runtime_of(runner)
        assert rt.config_idle_reason == "settings" and "ARM configuration idle" in pages.titles()
        texts = [f"{a} {b} {c}" for a, b, c in pages.items] + [repr(rt.status_row())]
        texts += [r.getMessage() for r in caplog.records]
        for text in texts:
            assert not any(p in text for p in PLANTED), text


async def test_round13_an_md_reset_reaches_the_arm_notices(tmp_path: Path, redis: Any, md_redis: Any) -> None:
    """Plan S38d (D4, §A13): the md bootstrap's reset event is wired to the ARM's notices: a reset that asks for a
    notice queues one MD_RESET extra keyed by the generation; one that does not, none. The indicator guard-input
    scope maps to its indicator for the clear probe and has no series side effect."""
    from src.marketdata.bootstrap import BootReason, MdReset, ResetClass
    from src.marketdata.types import MdGeneration
    from src.riskmgr.types import indicator_scope_id

    pages = Pages()
    runner = await arm_runner(tmp_path, redis, md_redis, HangPools(), pages)
    async with running(runner):
        await until(lambda: runner.arm_started)
        rt = runtime_of(runner)
        graph, svc = rt.graph, runner.services
        assert graph is not None and graph.notices is not None and svc is not None
        listeners = svc.md.bootstrap._listeners  # noqa: SLF001
        for listener in listeners:
            listener(MdReset(MdGeneration(9, 4), ResetClass.STORE_LOST, BootReason.NO_BOOT, True, True))
            listener(MdReset(MdGeneration(9, 5), ResetClass.INTACT, BootReason.TERM_START, False, False))
        extras = [e for e in graph.notices._extras if e.event == "MD_RESET"]  # noqa: SLF001
        assert [e.generation for e in extras] == ["9.4"]
        scope = indicator_scope_id(ADAPTER, "SYN1", "0123456789abcdef")
        assert list(rt._indicators_of(scope)) == ["0123456789abcdef"]  # noqa: SLF001
        rt._series_cleared(scope)  # noqa: SLF001   (no SeriesId parse warning, no SM12 change)
