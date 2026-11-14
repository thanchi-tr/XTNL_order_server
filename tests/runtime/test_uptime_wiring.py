"""Uptime + downtime catch-up in the production paths: build_engine (settings, tracker, hooks to the controller),
pg_leader_ports (the catch-up store), EngineSession (fence held before SM4, catch-up task, coverage targets, resume
after sleep) and src/main.py (stop reason, metrics). No network."""
from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator, Collection, Mapping
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src import main as entry
from src.db.epoch import Epoch
from src.db.uptime_store import CatchupRecord, Coverage, PgCatchupStore, StoredRow
from src.observability.metrics import CONTRACT
from src.runtime.catchup import CatchupCoordinator
from src.runtime.pg_ports import PgLeaderPorts, pg_leader_ports
from src.runtime.roles import NodeRole
from src.runtime.session import EngineSession, SessionContext
from src.runtime.supervisor import TaskSupervisor
from src.runtime.uptime import SuspendEvent, UptimeTracker
from src.runtime.wiring import EngineHooks, WiringError, build_engine
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.runtime.fakes import FakeClock, FakeEtcd
from tests.runtime.test_wiring import MOCK, cfg

EPOCH = Epoch("c", 91)


def hooks() -> EngineHooks:
    return EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False)


# ===================================================================== build_engine
def test_build_engine_wires_the_tracker_and_the_catchup_settings(tmp_path: Path) -> None:
    env = {**MOCK, "XTNL_UPTIME_PING_S": "600", "XTNL_CATCHUP_OVERLAP_S": "1200", "XTNL_CATCHUP_FIRST_RUN_DAYS": "3"}
    engine = build_engine(cfg(env, tmp_path), environ=env, hooks=hooks())
    try:
        up = engine.uptime
        assert isinstance(up, UptimeTracker) and up.ping_s == 600 and up.node_id == engine.controller.node_id
        ctx = engine.context
        assert ctx.catchup.overlap_ms == 1_200_000 and ctx.catchup.first_run_days == 3
        assert ctx.engine_run_id() == up.run_id
        assert up.coverage is not None and up.coverage() is None         # STARTING: no leader session
        assert up.on_suspended is not None
        up.on_suspended(SuspendEvent(1, 3_600_000, 5_000, "wall_jump"))   # no session: nothing to do, no error
    finally:
        asyncio.run(engine.aclose())


@pytest.mark.parametrize("extra, match", [
    ({"XTNL_UPTIME_PING_S": "30"}, "XTNL_UPTIME_PING_S"),
    ({"XTNL_UPTIME_PING_S": "abc"}, "XTNL_UPTIME_PING_S"),
    ({"XTNL_UPTIME_PING_S": "1800", "XTNL_CATCHUP_OVERLAP_S": "900"}, "XTNL_CATCHUP_OVERLAP_S"),
    ({"XTNL_CATCHUP_FIRST_RUN_DAYS": "0"}, "XTNL_CATCHUP_FIRST_RUN_DAYS"),
])
def test_bad_uptime_settings_refuse_to_start(tmp_path: Path, extra: dict[str, str], match: str) -> None:
    env = {**MOCK, **extra}
    with pytest.raises(WiringError, match=match):
        build_engine(cfg(env, tmp_path), environ=env, hooks=hooks())


def test_production_ports_carry_the_catchup_store() -> None:
    ports = pg_leader_ports(EPOCH, control=object(), dsn="postgresql://u@127.0.0.1/db", node_id="n",  # type: ignore[arg-type]
                            redis=fakeredis.FakeAsyncRedis(), on_fence_lost=lambda e: None)
    assert isinstance(ports, PgLeaderPorts) and isinstance(ports.catchup, PgCatchupStore)


# ===================================================================== EngineSession
class NullStore:
    async def coverage(self, adapter_id: str) -> Coverage | None:
        return None

    async def stored_orders(self, adapter_id: str, ids: Collection[str]) -> Mapping[str, StoredRow]:
        return {}

    async def stored_positions(self, adapter_id: str, ids: Collection[str]) -> Mapping[str, StoredRow]:
        return {}

    async def known_cash_flows(self, adapter_id: str, ids: Collection[str]) -> set[str]:
        return set()

    async def prior_activity_ms(self, adapter_id: str, run_id: str | None, before_ms: int) -> int | None:
        return None

    async def record(self, epoch_token: int, rec: CatchupRecord) -> int:
        return 1


async def run_id(_: Any) -> str:
    return "run-91"


@pytest.fixture
async def session(tmp_path: Path) -> AsyncIterator[EngineSession]:
    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    fx = fixture(wall_ms())

    def ports(epoch: Epoch) -> Any:
        base = backend.ports(epoch)
        fields = {f.name: getattr(base, f.name) for f in dataclasses.fields(base)}
        return PgLeaderPorts(**fields, catchup=NullStore())

    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [mock_adapter(fx)], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=lambda why: None, timings=FAST_TIMINGS, transfer_sources={"mock": True},
                         engine_run_id=lambda: "uuid-run")
    s = EngineSession(ctx, EPOCH)
    try:
        yield s
    finally:
        await s.aclose()


async def test_session_holds_every_fence_before_sm4_and_supervises_the_catchup(session: EngineSession) -> None:
    sup = TaskSupervisor([])
    session.init_deps(sup)
    assert isinstance(session.catchup, CatchupCoordinator)
    await session.start_adapters()
    names = list(sup._rt)  # type: ignore[attr-defined]
    assert names.index("catchup") < names.index("ingestion")
    assert session.orchestrator.runtime("mock").fence_holds == {"catchup"}
    assert session.coverage_targets() == (EPOCH.token, [])              # not caught up yet: no coverage
    assert session._catchup_verified("mock") is False                  # nor verified for ActionGate
    assert session.resume_after_sleep(3_600_000) is True


async def test_a_session_without_a_catchup_store_runs_as_before(tmp_path: Path) -> None:
    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [mock_adapter(fixture(wall_ms()))], lease=FakeLease(),
                         run_id_reader=run_id, exit_fn=lambda why: None, timings=FAST_TIMINGS)
    s = EngineSession(ctx, EPOCH)
    try:
        assert s.catchup is None and s.coverage_targets() is None and s.resume_after_sleep() is False
        assert s._catchup_verified("mock") is True                     # no catch-up facility: gate as before
    finally:
        await s.aclose()


# ===================================================================== src/main.py
class _Ctl:
    def __init__(self, role: NodeRole) -> None:
        self.role = role


def test_stop_reason_prefers_the_signal_then_the_controller_state() -> None:
    up = UptimeTracker(NullUptime(), "n")
    assert entry.stop_reason(_Ctl(NodeRole.STANDBY), up, None) == "stopped"  # type: ignore[arg-type]
    assert entry.stop_reason(_Ctl(NodeRole.FENCED), up, None) == "fenced"  # type: ignore[arg-type]
    assert entry.stop_reason(_Ctl(NodeRole.LEADER), up, RuntimeError("x")) == "error:RuntimeError"  # type: ignore[arg-type]
    up.note_stop_reason("SIGINT")
    assert entry.stop_reason(_Ctl(NodeRole.FENCED), up, None) == "SIGINT"  # type: ignore[arg-type]


class NullUptime:
    async def start_run(self, run_id: str, node_id: str, at_ms: int) -> None: ...

    async def heartbeat(self, run_id: str, at_ms: int) -> bool:
        return True

    async def note_suspended(self, run_id: str, at_ms: int) -> None: ...

    async def stop_run(self, run_id: str, at_ms: int, reason: str) -> None: ...

    async def advance_coverage(self, epoch_token: int, adapter_id: str, at_ms: int, run_id: str | None) -> bool:
        return True


def test_engine_telemetry_exports_uptime_and_catchup_series(tmp_path: Path) -> None:
    engine = build_engine(cfg(MOCK, tmp_path), environ=MOCK,
                          hooks=dataclasses.replace(hooks(), uptime_store=NullUptime()))
    try:
        engine.context.catchup_metrics.runs.inc(adapter="mock", complete="false")
        text = entry.engine_telemetry(engine, None).registry.render()
        assert 'xtnl_engine_uptime_seconds{node="' in text and 'xtnl_suspended_gaps_total{node="' in text
        assert 'adapter="mock",complete="false"} 1' in text
        for line in text.splitlines():
            if not line.startswith("#"):
                assert line.split("{", 1)[0] in CONTRACT
    finally:
        asyncio.run(engine.aclose())


async def test_run_engine_records_start_and_a_clean_stop_with_its_reason() -> None:
    calls: list[tuple[str, ...]] = []

    class Store(NullUptime):
        async def start_run(self, run_id: str, node_id: str, at_ms: int) -> None:
            calls.append(("start", node_id))

        async def stop_run(self, run_id: str, at_ms: int, reason: str) -> None:
            calls.append(("stop", reason))

    class Keeper:
        def holds_lease(self) -> bool:
            return False

        def lease_valid(self) -> bool:
            return False

        def start(self) -> None: ...

        def stop(self) -> None: ...

    class Controller:
        keeper = Keeper()
        role = NodeRole.STANDBY

        async def run(self) -> None:
            await asyncio.sleep(0.05)

        def request_drain(self, *a: object, **k: object) -> None: ...

    class FakeEngine:
        def __init__(self) -> None:
            self.uptime = UptimeTracker(Store(), "n1", tick_s=0.01)
            self.uptime.note_stop_reason("SIGTERM")
            self.environ: dict[str, str] = {"XTNL_METRICS_ADDR": "off"}
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    eng = FakeEngine()

    async def no_spill(_: object) -> int:
        return 0

    real = entry.drain_spill_at_startup
    entry.drain_spill_at_startup = no_spill  # type: ignore[assignment]
    try:
        await entry.run_engine(Controller(), eng)  # type: ignore[arg-type]
    finally:
        entry.drain_spill_at_startup = real  # type: ignore[assignment]
    assert calls[0] == ("start", "n1") and calls[-1] == ("stop", "SIGTERM") and eng.closed


# ===================================================================== a full leader term (in-memory ports)
class RecordingStore(NullStore):
    def __init__(self) -> None:
        self.records: list[CatchupRecord] = []

    async def coverage(self, adapter_id: str) -> Coverage | None:
        return Coverage(adapter_id, wall_ms() - 3_600_000, None)

    async def record(self, epoch_token: int, rec: CatchupRecord) -> int:
        self.records.append(rec)
        return len(self.records)


async def test_a_leader_term_catches_up_releases_the_fence_and_offers_coverage(tmp_path: Path) -> None:
    from src.dto import OrderStatus
    from tests.e2e.scenario import become_leader, eventually
    from tests.pubsub.conftest import order as mk_order

    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    fx = fixture(wall_ms())
    adapter = mock_adapter(fx)
    store = RecordingStore()
    filled_offline = mk_order("OFF-1", OrderStatus.FILLED, t=wall_ms() - 1_800_000, adapter_id="mock")
    if hasattr(adapter, "_history_orders"):
        adapter._history_orders.append(filled_offline)

    def ports(epoch: Epoch) -> Any:
        base = backend.ports(epoch)
        fields = {f.name: getattr(base, f.name) for f in dataclasses.fields(base)}
        return PgLeaderPorts(**fields, catchup=store)

    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=lambda why: None, timings=FAST_TIMINGS, transfer_sources={"mock": True})
    session = EngineSession(ctx, EPOCH)
    leader = await become_leader(session, EPOCH)
    try:
        async def done() -> bool:
            return bool(store.records) and session.catchup is not None and session.catchup.finished("mock")
        await eventually(done, "catch-up finished")

        async def covered() -> bool:
            return session.coverage_targets() == (EPOCH.token, ["mock"])
        await eventually(covered, "adapter eligible for coverage")
        rt = session.orchestrator.runtime("mock")
        assert not rt.fence_holds and not any(s.lost for s in rt.subs.values())
        assert store.records[0].reason == "startup" and store.records[0].offline_from_ms is not None
        assert "mock:OFF-1" in rt.orders                         # the order finished offline went through
    finally:
        await leader.shutdown()


async def test_a_stream_gap_mid_term_stops_coverage_until_a_reconnect_catchup_re_read_the_history(
        tmp_path: Path) -> None:
    """Production path: after the start-up catch-up the adapter is offered for coverage; an order-stream gap
    withdraws it at once (an order placed and finished while frames were lost is in no snapshot); SM4 RESYNCING
    commits a pass, a `reconnect` catch-up re-reads the history, and only then is the adapter offered again.
    (A GAP, not a socket loss: the mock adapter's streams do not survive its close().)"""
    from src.adapters.base import StreamEvent, StreamEventKind
    from tests.e2e.scenario import become_leader, eventually

    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    adapter = mock_adapter(fixture(wall_ms()))
    store = RecordingStore()

    def ports(epoch: Epoch) -> Any:
        base = backend.ports(epoch)
        fields = {f.name: getattr(base, f.name) for f in dataclasses.fields(base)}
        return PgLeaderPorts(**fields, catchup=store)

    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=lambda why: None, timings=FAST_TIMINGS, transfer_sources={"mock": True})
    session = EngineSession(ctx, EPOCH)
    leader = await become_leader(session, EPOCH)
    try:
        async def covered() -> bool:
            return session.coverage_targets() == (EPOCH.token, ["mock"])
        await eventually(covered, "adapter eligible for coverage")
        session.orchestrator.handle_stream_item("mock", "orders", StreamEvent(StreamEventKind.GAP, "orders"))
        assert session.coverage_targets() == (EPOCH.token, [])

        async def reconnect_done() -> bool:
            return [r.reason for r in store.records][-1:] == ["reconnect"] and await covered()
        await eventually(reconnect_done, "reconnect catch-up finished and coverage offered again")
    finally:
        await leader.shutdown()
