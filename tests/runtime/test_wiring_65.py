"""IMPLEMENTATION_PLAN 6.5: runtime wiring of the 6.1-6.4 APIs (REST priority classes, REST bucket and token-age
metrics, the orchestrator's clean-spread thresholds, SM10 expected adapters / maintenance notice / declared windows,
the RiskSweep's AlertInhibitor and the epoch-change settle of the orchestrator's pages, shared REST buckets).
No network: fakeredis + in-memory ports. One regression test (or more) per behaviour change."""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.adapters.mock import MockBrokerAdapter
from src.adapters.ratelimit import RestBudget, RestPriority
from src.adapters.stonex.adapter import StoneXAdapter
from src.calendar.schedule import CalendarSchedule, MaintenanceWindow
from src.db.epoch import Epoch
from src.observability.metrics import MetricsRegistry
from src.oversee.inhibit import EPOCH_SETTLE_MS
from src.pubsub.connection import ConnState
from src.runtime.maintenance import (
    MAINTENANCE_FILE_VAR,
    FileMaintenanceSource,
    MaintenanceSourceError,
    parse_maintenance,
)
from src.runtime.schedule_refresh import ScheduleRefresher
from src.runtime.session import (
    EngineSession,
    SessionContext,
    SessionTimings,
    _DlqPriorityBridge,
    adapter_metric_samples,
)
from src.runtime.supervisor import TaskSupervisor
from src.runtime.wiring import EngineHooks, LeaseCheck, RestBudgets, _plan_for, build_engine, make_adapter
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.runtime.fakes import FakeClock, FakeEtcd
from tests.runtime.test_session_56 import fx_calendar, ny_ms
from tests.runtime.test_wiring import MOCK, STONEX, cfg

EPOCH = Epoch("c", 65)
T0 = 1_763_000_000_000


async def run_id(_: Any) -> str:
    return "run-65"


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


class Windows:
    """A scripted MaintenanceSource."""

    def __init__(self) -> None:
        self.windows: list[MaintenanceWindow] = []
        self.error: Exception | None = None
        self.loads = 0

    async def load(self) -> Sequence[MaintenanceWindow]:
        self.loads += 1
        if self.error is not None:
            raise self.error
        return list(self.windows)


@dataclasses.dataclass
class Built:
    session: EngineSession
    adapter: MockBrokerAdapter
    clock: Clock
    pages: list[tuple[str, str, str]]
    windows: Windows


@pytest.fixture
async def built(tmp_path: Path) -> AsyncIterator[Built]:
    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    adapter = mock_adapter(fixture(wall_ms()))
    clock = Clock(T0)
    pages: list[tuple[str, str, str]] = []
    windows = Windows()
    timings = dataclasses.replace(FAST_TIMINGS, epoch_settle_ms=EPOCH_SETTLE_MS)
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=lambda _r: None, timings=timings, transfer_sources={"mock": True},
                         clock_ms=clock, page_sink=lambda *p: pages.append(p), maintenance=windows)
    s = EngineSession(ctx, EPOCH)
    try:
        yield Built(s, adapter, clock, pages, windows)
    finally:
        await s.aclose(1.0)
        await redis.aclose()


def priorities(adapter: MockBrokerAdapter, method: str) -> list[RestPriority]:
    return [p for m, p in adapter.rest_calls if m == method]


# ============================================================================================ REST priority (6.1)
async def test_the_flat_account_poll_runs_at_poll_priority(built: Built) -> None:
    """Before 6.5 no caller tagged its class: the 60 s poll competed with reconcile reads as RECONCILE."""
    assert await built.session.orchestrator.poll_account("mock")
    assert priorities(built.adapter, "fetch_account_snapshot") == [RestPriority.POLL]


async def test_on_demand_cash_flow_fetches_run_at_cash_flows_priority(built: Built) -> None:
    o = built.session.orchestrator
    o._account_owner["ACC-65"] = "mock"
    o.runtime("mock").conn.state = ConnState.STREAMING
    assert await built.session._request_cash_flows("ACC-65") is None      # the RiskSweep's requester
    assert priorities(built.adapter, "fetch_cash_flows") == [RestPriority.CASH_FLOWS]


async def test_dlq_check_before_retry_reads_run_at_dlq_priority(built: Built) -> None:
    s = built.session
    bridge = s.dlq_worker._broker
    assert isinstance(bridge, _DlqPriorityBridge)
    await bridge.reread_entity("mock", "ORDER", "nope")
    await bridge.read_entity("mock", "POSITION", "nope")
    await bridge.reread_cash_flow("mock", "nope", 0)
    assert [p for _, p in built.adapter.rest_calls] == [RestPriority.DLQ] * 3


async def test_untagged_re_reads_stay_at_reconcile_priority(built: Built) -> None:
    """SM8 / readiness account re-reads (and every snapshot / resync) are reconcile-class: not demoted."""
    await built.session.orchestrator.reread_entity("mock", "ACCOUNT", "nope")
    assert priorities(built.adapter, "fetch_account_snapshot") == [RestPriority.RECONCILE]


async def test_the_schedule_refresh_fetch_runs_at_poll_priority(built: Built) -> None:
    adapter = built.adapter

    async def reload() -> int:
        return 0

    refresher = ScheduleRefresher(
        Any, adapter_ids=lambda: ["mock"], adapter=lambda _a: adapter, ready=lambda _a: True,  # type: ignore[arg-type]
        wanted_symbols=lambda _a: ["EURUSD"], stored_symbols=lambda _a: [], reload=reload,
        calendar_version=lambda: None, pager=lambda *_: None, clock_ms=wall_ms)
    await refresher._fetch("mock", ["EURUSD"])
    assert priorities(adapter, "fetch_market_sessions") == [RestPriority.POLL]


# ============================================================================================ metrics (6.1)
class _Creds:
    adapter_id = "stonex"

    def __init__(self, issued: int | None, expires: int | None, budget: RestBudget | None) -> None:
        self.token_issued_at_ms = issued
        self.token_expires_at_ms = expires
        self.rest_budget = budget


def test_token_age_and_rest_bucket_series_are_exported_under_the_contract() -> None:
    """Before 6.5 xtnl_token_age_seconds / xtnl_rest_bucket_tokens / xtnl_rest_requests_total were never exported."""
    budget = RestBudget("stonex", rate_per_s=5, burst=10, monotonic=lambda: 0.0)
    budget.spend(RestPriority.DLQ)
    samples = adapter_metric_samples("stonex", _Creds(T0 - 90_000, T0 + 3_600_000, budget), T0)  # type: ignore[arg-type]
    by = {(s.name, tuple(sorted(s.labels.items()))): s.value for s in samples}
    assert by[("xtnl_token_age_seconds", (("adapter", "stonex"),))] == 90.0
    assert by[("xtnl_token_expires_in_seconds", (("adapter", "stonex"),))] == 3600.0
    assert by[("xtnl_rest_bucket_tokens", (("adapter", "stonex"),))] == 9.0
    assert by[("xtnl_rest_requests_total", (("adapter", "stonex"), ("priority", "dlq")))] == 1.0
    assert by[("xtnl_rest_requests_total", (("adapter", "stonex"), ("priority", "reconcile")))] == 0.0
    reg = MetricsRegistry("n1")
    reg.register("term", lambda: samples)
    text = reg.render()
    assert reg.dropped == 0
    assert 'xtnl_token_age_seconds{node="n1",adapter="stonex"} 90' in text
    assert 'xtnl_rest_requests_total{node="n1",adapter="stonex",priority="dlq"} 1' in text


def test_unknown_token_age_and_an_unmetered_adapter_export_nothing() -> None:
    assert adapter_metric_samples("mock", _Creds(None, None, None), T0) == []  # type: ignore[arg-type]


async def test_the_leader_term_exports_the_adapters_bucket(built: Built) -> None:
    built.adapter._budget = RestBudget("mock", rate_per_s=5, burst=10)
    names = {s.name for s in built.session.metrics_samples(leader=True)}
    assert {"xtnl_rest_bucket_tokens", "xtnl_rest_requests_total"} <= names
    assert "xtnl_rest_bucket_tokens" not in {s.name for s in built.session.metrics_samples(leader=False)}


# ============================================================================================ ingestion (6.2)
async def test_the_clean_mid_watch_uses_the_committers_thresholds(built: Built) -> None:
    """Before 6.5 the orchestrator fell back to its 5-pip default, not the installed ruleset's thresholds."""
    s = built.session
    assert s.orchestrator._clean_spread == s.rules.clean_spread


# ============================================================================================ SM10 / Oversee (6.3)
async def test_every_configured_adapter_is_checked_for_calendar_sessions(built: Built) -> None:
    """Before 6.5 an adapter without sessions that sent no ticks never got the no_calendar_sessions page."""
    s = built.session
    assert s.calendar_init._adapters == ("mock",)
    s.calendar.init_from_schedule(CalendarSchedule(1, {}), built.clock.now)
    assert [p for p in built.pages if p[1] == "calendar:no_calendar_sessions"] == [("high",
                                                                                    "calendar:no_calendar_sessions",
                                                                                    "mock:*")]


async def test_a_maintenance_window_sends_one_info_notice(built: Built) -> None:
    """Before 6.5: an info page at the start AND one at the end, and nothing named the reason."""
    s = built.session
    now = built.clock.now
    fx_calendar(s, now)
    s.calendar.declare_maintenance(MaintenanceWindow("mock", now - 1_000, now + 60_000, "CIAPI upgrade"))
    s.calendar.tick(now)
    s.calendar.tick(now + 61_000)
    maint = [p for p in built.pages if "maintenance" in p[1]]
    assert maint == [("info", "maintenance: mock", "until 2025-11-13T02:14:20Z: CIAPI upgrade")]


async def test_the_sweep_holds_back_dependent_alerts_after_the_epoch_change_and_in_maintenance(built: Built) -> None:
    """Before 6.5 the RiskSweep had no AlertInhibitor: every takeover and maintenance window paged FENCE alerts."""
    s = built.session
    assert s.sweep._inhibitor is s.inhibitor
    start = built.clock.now
    assert s.inhibitor.suppressed("mock", start + 1_000) == "epoch_change"
    assert s.inhibitor.suppressed("mock", start + EPOCH_SETTLE_MS) is None
    fx_calendar(s, start)
    s.calendar.declare_maintenance(MaintenanceWindow("mock", start, start + 600_000))
    s.calendar.tick(start + EPOCH_SETTLE_MS)
    assert s.inhibitor.suppressed("mock", start + EPOCH_SETTLE_MS) == "maintenance"


async def test_the_orchestrators_liveness_and_price_pages_wait_for_the_epoch_settle(built: Built) -> None:
    """operations.md §9: stale / liveness pages are dependent alerts of an epoch change (6.3 open question)."""
    s = built.session
    o = s.orchestrator
    assert o._market_open == s._liveness_alerting and o._price_muted == s._price_alerts_muted
    fx_calendar(s, ny_ms(2026, 11, 11, 12))                  # Wednesday noon New York: OPEN
    built.clock.now += 1_000
    assert not s._liveness_alerting("mock") and s._price_alerts_muted("mock", "EURUSD")
    built.clock.now += EPOCH_SETTLE_MS
    assert s._liveness_alerting("mock") and not s._price_alerts_muted("mock", "EURUSD")


async def test_a_conn_key_absent_through_the_settle_pages_once_it_ends(built: Built) -> None:
    s = built.session
    o = s.orchestrator
    fx_calendar(s, ny_ms(2026, 11, 11, 12))
    o.runtime("mock").conn.state = ConnState.STREAMING
    built.clock.now += 90_000                                # > CONN_ABSENT_ALERT_MS, still settling
    o.check_liveness("mock")
    assert not [p for p in built.pages if p[1].startswith(("conn key absent", "stale price"))]
    built.clock.now += EPOCH_SETTLE_MS
    o.check_liveness("mock")
    assert [p for p in built.pages if p[1].startswith("conn key absent")]


# ============================================================================================ maintenance source
async def test_declared_windows_reach_the_calendar_and_a_bad_source_pages_once(built: Built) -> None:
    """Before 6.5 nothing in production declared a maintenance window, so BROKER_MAINTENANCE never happened."""
    s = built.session
    now = built.clock.now
    fx_calendar(s, now)
    w = MaintenanceWindow("mock", now, now + 600_000, "broker maintenance")
    built.windows.windows = [w]
    assert await s.refresh_maintenance()
    s.calendar.tick(now + 1)
    assert s.calendar.in_maintenance("mock")
    built.windows.error = MaintenanceSourceError("maintenance.json: not valid JSON (line 3)")
    assert not await s.refresh_maintenance()
    assert not await s.refresh_maintenance()
    bad = [p for p in built.pages if p[1] == "maintenance windows unreadable"]
    assert len(bad) == 1 and "keeping the 1 windows" in bad[0][2]
    s.calendar.tick(now + 2)
    assert s.calendar.in_maintenance("mock")                 # the last good windows stay declared
    built.windows.error = None
    built.windows.windows = []
    assert await s.refresh_maintenance()
    s.calendar.tick(now + 3)
    assert not s.calendar.in_maintenance("mock")             # removed from the file: the overlay ends


async def test_the_maintenance_task_is_supervised_when_a_source_is_configured(built: Built) -> None:
    s = built.session
    sup = TaskSupervisor([])
    s.init_deps(sup)
    await s.start_risksweep()
    try:
        assert "maintenance" in sup._rt
        for _ in range(50):
            if built.windows.loads:
                break
            await asyncio.sleep(0.01)
        assert built.windows.loads >= 1
    finally:
        await sup.stop_all()


def test_the_maintenance_file_format(tmp_path: Path) -> None:
    path = tmp_path / "maintenance.json"
    src = FileMaintenanceSource(path)
    assert asyncio.run(src.load()) == []                     # no file: no window declared
    path.write_text(json.dumps({"stonex": [{"start": "2026-11-14T22:00:00+00:00", "end": "2026-11-15T02:00:00+00:00",
                                            "reason": "weekend"}],
                                "pepperstone": [{"start_ms": 1, "end_ms": 2}]}), encoding="utf-8")
    got = asyncio.run(src.load())
    assert [(w.adapter_id, w.reason) for w in got] == [("stonex", "weekend"), ("pepperstone", "broker maintenance")]
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(MaintenanceSourceError, match="not valid JSON"):
        asyncio.run(src.load())
    for doc in ([], {"stonex": {}}, {"stonex": [{"start": "2026-11-14T22:00:00", "end": "2026-11-15T02:00:00"}]},
                {"stonex": [{"start_ms": 5, "end_ms": 5}]}):
        with pytest.raises(MaintenanceSourceError):
            parse_maintenance(doc)


# ============================================================================================ build_engine
def test_build_engine_wires_the_maintenance_file_and_shared_rest_buckets(tmp_path: Path) -> None:
    path = tmp_path / "maint.json"
    env = {**STONEX, MAINTENANCE_FILE_VAR: str(path)}
    engine = build_engine(cfg(env, tmp_path), environ=env,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        src = engine.context.maintenance
        assert isinstance(src, FileMaintenanceSource) and src.path == path
        assert engine.rest_budgets is not None
        first, second = engine.context.adapters(), engine.context.adapters()    # two leader terms
        assert isinstance(first[0], StoneXAdapter) and first[0] is not second[0]
        assert first[0].rest_budget is second[0].rest_budget is engine.rest_budgets.get("stonex")
    finally:
        asyncio.run(engine.aclose())


def test_the_mock_engine_reads_the_default_maintenance_file(tmp_path: Path) -> None:
    engine = build_engine(cfg(MOCK, tmp_path), environ=MOCK,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        src = engine.context.maintenance
        assert isinstance(src, FileMaintenanceSource) and src.path.as_posix() == "/etc/xtnl/maintenance.json"
        assert engine.context.adapters()[0].rest_budget is None              # the mock stays unmetered
    finally:
        asyncio.run(engine.aclose())


def test_a_rotated_adapter_shares_the_bucket_and_new_limits_replace_it(caplog: pytest.LogCaptureFixture) -> None:
    """One bucket per adapter (operations.md §15): before 6.5 every term and every rotation got a fresh full one."""
    budgets = RestBudgets()
    plan = _plan_for("stonex", STONEX)
    check = LeaseCheck(lambda: True)
    a, b = make_adapter(plan, check, budgets=budgets), make_adapter(plan, check, budgets=budgets)
    assert a.rest_budget is b.rest_budget is not None
    faster = _plan_for("stonex", {**STONEX, "STONE_X_REST_RATE_PER_S": "8"})
    with caplog.at_level(logging.WARNING):
        c = make_adapter(faster, check, budgets=budgets)
    assert c.rest_budget is not a.rest_budget and c.rest_budget is not None and c.rest_budget.rate_per_s == 8.0
    assert "REST limits changed" in caplog.text


def test_the_default_session_timings_settle_for_rto_plus_30_s() -> None:
    assert SessionTimings().epoch_settle_ms == EPOCH_SETTLE_MS == 120_000


# ============================================================================================ skeptic fixes (6.5)
async def test_leader_init_reads_the_declared_windows_before_sm10_is_computed(built: Built) -> None:
    """operations.md §4 step 5: SM10's state is computed with the maintenance overlay in force. Before the fix the
    file was first read by the `maintenance` task started at step 6, so a takeover inside a window computed SM10
    without the overlay (no notice, liveness / FENCE not suppressed) until that task's first read."""
    s = built.session
    now = built.clock.now
    built.windows.windows = [MaintenanceWindow("mock", now - 1_000, now + 600_000, "weekend maintenance")]
    deps = s.init_deps(TaskSupervisor([]))
    version = await deps.calendar.init_calendar(now)
    assert version == 1 and built.windows.loads == 1
    assert s.calendar.in_maintenance("mock")
    assert s.calendar.maintenance_until("mock", now) == now + 600_000


async def test_an_unreadable_source_at_leader_init_pages_and_the_calendar_still_starts(built: Built) -> None:
    s = built.session
    built.windows.error = MaintenanceSourceError("maintenance.json: not valid JSON (line 2)")
    deps = s.init_deps(TaskSupervisor([]))
    assert await deps.calendar.init_calendar(built.clock.now) == 1
    assert s.calendar.version == 1 and not s.calendar.in_maintenance("mock")
    assert [p for p in built.pages if p[1] == "maintenance windows unreadable"]


async def test_a_hung_maintenance_source_pages_instead_of_stalling(built: Built) -> None:
    """operations.md §3: blocking reads run with explicit timeouts. Before the fix a read that never returned
    (a hung mount) stalled LEADER_INIT step 5 and the maintenance task with no page."""
    s = built.session
    s._t = dataclasses.replace(s._t, maintenance_load_timeout_s=0.05)

    class Hung:
        async def load(self) -> Sequence[MaintenanceWindow]:
            await asyncio.Event().wait()
            return []

    s.ctx = dataclasses.replace(s.ctx, maintenance=Hung())
    assert not await asyncio.wait_for(s.refresh_maintenance(), 2.0)
    bad = [p for p in built.pages if p[1] == "maintenance windows unreadable"]
    assert len(bad) == 1 and "no answer within 0.05 s" in bad[0][2]


async def test_the_unreadable_source_page_never_carries_a_url_secret(built: Built) -> None:
    s = built.session
    built.windows.error = RuntimeError("GET https://ops:hunter2@config.example/maint?token=abc failed")
    assert not await s.refresh_maintenance()
    body = next(p[2] for p in built.pages if p[1] == "maintenance windows unreadable")
    assert "hunter2" not in body and "token=abc" not in body and "config.example" in body
