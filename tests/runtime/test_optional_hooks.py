"""S09 PR-G1 runtime hooks (design §A2 / §A8; plan S09: RT-M1, RT-M2, RT-M3, RT-M6, RT-M12, RT-M13 flag part).

* `NodeController` calls `LeaderSession.start_optional_services()` right after LEADER; a raise pages and keeps the
  term (no exit, no drain, health still published).
* `TaskSupervisor.has()`, `TaskSpec.on_circuit_reopen` and `reopen_count()`.
* `EngineSession`: the enable flags (off by default: no task), the idempotent `optional-bootstrap` add, the composite
  price hook (market-data hook first, its own guard, the calendar path unchanged), `aclose` in parallel with
  `terminate()` for what is still open, DRAINING inside its budget with a hung extra closer.
* `optional_settings`: tolerant parse; `build_engine` never raises on a bad flag; `check_composition` never calls the
  bootstrap factory.

No market data appears in any assertion text, page or log checked here; instruments are synthetic.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import fakeredis
import httpx
import pytest

from src.db.epoch import Epoch
from src.db.pg import AsyncpgSource
from src.dto import PriceDTO
from src.runtime.config import RuntimeConfig
from src.runtime.etcd import LEADER_KEY
from src.runtime.health import HealthBoard
from src.runtime.lease import LeaseKeeper, LeaseTimings
from src.runtime.node import NodeController
from src.runtime.optional_settings import (
    ARM_ENABLED_VAR,
    MD_ENABLED_VAR,
    OptionalFlags,
    optional_flags_from_env,
)
from src.runtime.pg_ports import LazyPgSource, PgLeaderPorts, pg_leader_ports
from src.runtime.roles import NodeRole
from src.runtime.session import (
    MD_HOOK_TRIP_AFTER,
    OPTIONAL_BOOTSTRAP_TASK,
    EngineSession,
    MarketDataHookGuard,
    SessionContext,
    SessionTimings,
    check_drain_budget,
)
from src.runtime.supervisor import TaskContext, TaskSpec, TaskSupervisor
from src.runtime.wiring import EngineHooks, build_engine, check_composition
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import ADAPTER, FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.runtime.fakes import Exits, FakeClock, FakeEtcd
from tests.db.conftest import FakeSource
from tests.runtime.test_node import Session, World
from tests.runtime.test_wiring import MOCK, cfg
from tests.runtime.test_wiring_session import ENV as WIRED_ENV
from tests.runtime.test_wiring_session import ControlConn

EPOCH = Epoch("c", 9)
R = NodeRole


@dataclass
class Pages:
    items: list[tuple[str, str, str]] = field(default_factory=list)

    def page(self, severity: str, title: str, body: str) -> None:
        self.items.append((severity, title, body))

    def __call__(self, severity: str, title: str, body: str) -> None:
        self.page(severity, title, body)


async def run_id(_: Any) -> str:
    return "run-s09"


def tick(symbol: str = "EURUSD") -> PriceDTO:
    return PriceDTO(adapter_id=ADAPTER, symbol=symbol, bid=Decimal(1), ask=Decimal(1), spread_pips=None,
                    timestamp_ms=1)


async def forever(ctx: TaskContext) -> None:
    while True:
        ctx.stamp()
        await asyncio.sleep(0.01)


async def crash(ctx: TaskContext) -> None:
    await asyncio.sleep(0)
    raise RuntimeError("boom")


async def settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


# ============================================================================================ NodeController
class _RoleSpySession(Session):
    seen_roles: list[NodeRole]
    controller: list[NodeController]

    def start_optional_services(self) -> None:
        self.seen_roles.append(self.controller[0].role)
        super().start_optional_services()


def _controller(w: World, pager: Pages | None = None) -> NodeController:
    keeper = LeaseKeeper(w.etcd, clock=w.mono, fence=w.exits)
    return NodeController("n1", keeper=keeper, board=HealthBoard(w.etcd, "n1"), probe=w, terms=w, cluster=w,
                          local_redis=w, session_factory=w.factory, pager=pager, exit_fn=w.exits,
                          clock_ms=w.now_ms, monotonic=w.mono, step_interval_s=0.001, require_keeper_thread=False)


async def test_controller_calls_the_hook_once_right_after_leader() -> None:
    w = World()
    holder: list[NodeController] = []
    roles: list[NodeRole] = []

    def factory(epoch: Epoch) -> Session:
        s = _RoleSpySession(w.init)
        s.seen_roles, s.controller = roles, holder
        w.sessions.append(s)
        return s

    w.factory = factory  # type: ignore[method-assign]
    c = _controller(w)
    holder.append(c)
    await c.startup()
    await c.step()
    assert c.role is R.LEADER and roles == [R.LEADER]
    assert w.sessions[0].optional_started == 1
    await c.step()                                          # later steps never call it again
    assert w.sessions[0].optional_started == 1


async def test_a_raising_hook_keeps_the_node_leader_and_publishing_health() -> None:
    w = World()
    pages = Pages()
    real = w.factory

    def factory(epoch: Epoch) -> Session:
        s = real(epoch)
        s.optional_error = RuntimeError("bootstrap constructor failed")
        return s

    w.factory = factory  # type: ignore[method-assign]
    c = _controller(w, pages)
    await c.startup()
    await c.step()
    assert c.role is R.LEADER and w.exits.reasons == [] and w.sessions[0].drained == []
    assert w.etcd.kv[LEADER_KEY].value == "n1" and c.keeper.holds_lease()
    assert [(p[0], p[1]) for p in pages.items] == [("high", "optional services failed to start")]
    assert "RuntimeError" in pages.items[0][2]
    for _ in range(3):
        w.mono.advance(1)
        c.keeper.tick()
        await c.step()
    assert c.role is R.LEADER and w.exits.reasons == []
    assert c.last_record is not None and c.last_record.role == "LEADER"
    seen = HealthBoard(w.etcd, "n2").peers()
    assert [(p.node, p.role) for p in seen] == [("n1", "LEADER")] and seen[0].ts_ms == w.now_ms()


async def test_a_raising_hook_without_a_pager_only_logs(caplog: pytest.LogCaptureFixture) -> None:
    w = World()
    real = w.factory

    def factory(epoch: Epoch) -> Session:
        s = real(epoch)
        s.optional_error = ValueError("bad-setting-value")
        return s

    w.factory = factory  # type: ignore[method-assign]
    c = _controller(w)
    await c.startup()
    with caplog.at_level(logging.ERROR, logger="src.runtime.node"):
        await c.step()
    assert c.role is R.LEADER and "optional services failed to start" in caplog.text
    # The error type only (mirrors test_guard_logs_the_error_type_only): no message, no traceback.
    assert "ValueError" in caplog.text
    assert "bad-setting-value" not in caplog.text and "Traceback" not in caplog.text
    assert all(r.exc_info is None for r in caplog.records if r.name == "src.runtime.node")


# ============================================================================================ TaskSupervisor
def _sup(*specs: TaskSpec) -> tuple[TaskSupervisor, FakeClock, Pages]:
    clk, pages = FakeClock(), Pages()
    return TaskSupervisor(list(specs), clock=clk, exit_fn=Exits(), pager=pages), clk, pages


async def test_has_reports_registered_tasks_whatever_their_state() -> None:
    s, _, _ = _sup(TaskSpec("t", crash, max_stamp_age_s=5, max_deaths=0))
    assert s.has("t") and not s.has(OPTIONAL_BOOTSTRAP_TASK)
    s.start()
    await settle()
    s.poll()
    assert s.degraded() == ["t"] and s.has("t")            # circuit open: still registered
    s.add(TaskSpec(OPTIONAL_BOOTSTRAP_TASK, forever, max_stamp_age_s=60), start=False)
    assert s.has(OPTIONAL_BOOTSTRAP_TASK) and not s.running(OPTIONAL_BOOTSTRAP_TASK)
    await s.stop_all()


async def test_on_circuit_reopen_fires_on_a_half_open_death() -> None:
    """Mirrors test_half_open_death_keeps_circuit_open_for_another_10_min (RT-M3)."""
    reopened: list[str] = []
    s, clk, pages = _sup(TaskSpec("t", crash, max_stamp_age_s=5, max_deaths=0, on_circuit_reopen=reopened.append))
    s.start()
    await settle()
    s.poll()
    assert s.degraded() == ["t"] and reopened == [] and s.reopen_count("t") == 0   # the first open is not a reopen
    for n in (1, 2):
        clk.advance(600)
        s.poll()                                          # half-open: one retry
        await settle()
        s.poll()                                          # it dies: the circuit stays open
        assert s.degraded() == ["t"] and "still open" in pages.items[-1][1]
        assert reopened == ["t"] * n and s.reopen_count("t") == n
    assert s.reopen_count("nope") == 0
    await s.stop_all()


async def test_a_raising_reopen_hook_never_changes_supervision(caplog: pytest.LogCaptureFixture) -> None:
    def bad(name: str) -> None:
        raise RuntimeError("budget store down")

    s, clk, pages = _sup(TaskSpec("t", crash, max_stamp_age_s=5, max_deaths=0, on_circuit_reopen=bad))
    s.start()
    await settle()
    s.poll()
    clk.advance(600)
    s.poll()
    await settle()
    with caplog.at_level(logging.ERROR, logger="src.runtime.supervisor"):
        s.poll()
    assert s.reopen_count("t") == 1 and "still open" in pages.items[-1][1]
    assert "circuit-reopen hook of t failed" in caplog.text
    clk.advance(600)
    s.poll()
    assert s.running("t")                                  # the next half-open retry still happens
    await s.stop_all()


def _flaky() -> tuple[dict[str, bool], Any]:
    state = {"crash": True}

    async def body(ctx: TaskContext) -> None:
        await asyncio.sleep(0)
        if state["crash"]:
            raise RuntimeError("boom")
        await forever(ctx)

    return state, body


async def _reopen(s: TaskSupervisor, clk: FakeClock) -> None:
    clk.advance(600)
    s.poll()                                              # half-open: one retry
    await settle()
    s.poll()                                              # it dies: the circuit stays open


async def test_reset_task_resets_the_reopen_count() -> None:
    """reopen_count is per incident: an audited operator reset_task starts a fresh count."""
    _, body = _flaky()
    s, clk, _ = _sup(TaskSpec("t", body, max_stamp_age_s=5, max_deaths=0))
    s.start()
    await settle()
    s.poll()
    for _ in range(2):
        await _reopen(s, clk)
    assert s.reopen_count("t") == 2
    s.reset_task("t", "op1")
    assert s.reopen_count("t") == 0 and s.degraded() == []
    await settle()
    s.poll()                                              # still failing: the circuit opens again, not a reopen
    assert s.degraded() == ["t"] and s.reopen_count("t") == 0
    await _reopen(s, clk)
    assert s.reopen_count("t") == 1                       # failures from before the reset are not counted
    await s.stop_all()


async def test_a_clean_half_open_run_resets_the_reopen_count() -> None:
    state, body = _flaky()
    s, clk, _ = _sup(TaskSpec("t", body, max_stamp_age_s=1000, max_deaths=0, clean_run_s=60))
    s.start()
    await settle()
    s.poll()
    await _reopen(s, clk)
    assert s.reopen_count("t") == 1
    state["crash"] = False
    clk.advance(600)
    s.poll()                                              # half-open retry, which now runs
    await settle()
    clk.advance(60)
    s.poll()                                              # a clean run closes the circuit
    assert s.degraded() == [] and s.reopen_count("t") == 0
    await s.stop_all()


# ============================================================================================ EngineSession fixture
def _ctx(tmp_path: Path, redis: fakeredis.FakeAsyncRedis, **kw: Any) -> SessionContext:
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    return SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                          adapters=lambda: [mock_adapter(fixture(wall_ms()))], lease=FakeLease(),
                          run_id_reader=run_id, exit_fn=lambda _r: None, timings=FAST_TIMINGS, **kw)


@pytest.fixture
async def redis() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    r = fakeredis.FakeAsyncRedis()
    try:
        yield r
    finally:
        await r.aclose()


@pytest.fixture
async def session(tmp_path: Path, redis: fakeredis.FakeAsyncRedis) -> AsyncIterator[EngineSession]:
    s = EngineSession(_ctx(tmp_path, redis), EPOCH)
    try:
        yield s
    finally:
        await s.aclose(1.0)


def _session(tmp_path: Path, redis: fakeredis.FakeAsyncRedis, **kw: Any) -> EngineSession:
    return EngineSession(_ctx(tmp_path, redis, **kw), EPOCH)


class Factory:
    def __init__(self, *, name: str = OPTIONAL_BOOTSTRAP_TASK, critical: bool = False) -> None:
        self.calls: list[EngineSession] = []
        self.name = name
        self.critical = critical

    def __call__(self, s: EngineSession) -> TaskSpec:
        self.calls.append(s)
        return TaskSpec(self.name, forever, max_stamp_age_s=60.0, critical=self.critical)


# ============================================================================================ start_optional_services
async def test_flags_off_add_no_task_even_with_a_factory(tmp_path: Path, redis: fakeredis.FakeAsyncRedis) -> None:
    factory = Factory()
    s = _session(tmp_path, redis, optional_bootstrap=factory)
    sup = TaskSupervisor([])
    s.init_deps(sup)
    assert s.ctx.optional_flags == OptionalFlags()             # the default: both off
    s.start_optional_services()
    assert factory.calls == [] and not sup.has(OPTIONAL_BOOTSTRAP_TASK)
    await s.aclose(1.0)


async def test_md_flag_on_adds_the_bootstrap_once(tmp_path: Path, redis: fakeredis.FakeAsyncRedis) -> None:
    factory = Factory()
    s = _session(tmp_path, redis, optional_bootstrap=factory, optional_flags=OptionalFlags(md_enabled=True))
    sup = TaskSupervisor([])
    s.init_deps(sup)
    s.start_optional_services()
    s.start_optional_services()                                # idempotent through has()
    assert factory.calls == [s] and sup.has(OPTIONAL_BOOTSTRAP_TASK) and sup.running(OPTIONAL_BOOTSTRAP_TASK)
    await sup.stop_all()
    await s.aclose(1.0)


async def test_md_flag_on_without_a_factory_adds_nothing(tmp_path: Path, redis: fakeredis.FakeAsyncRedis) -> None:
    s = _session(tmp_path, redis, optional_flags=OptionalFlags(md_enabled=True, arm_enabled=True))
    sup = TaskSupervisor([])
    s.init_deps(sup)
    s.start_optional_services()
    assert not sup.has(OPTIONAL_BOOTSTRAP_TASK)
    await s.aclose(1.0)


async def test_no_bootstrap_in_draining_or_after_close(tmp_path: Path, redis: fakeredis.FakeAsyncRedis) -> None:
    factory = Factory()
    s = _session(tmp_path, redis, optional_bootstrap=factory, optional_flags=OptionalFlags(md_enabled=True))
    sup = TaskSupervisor([])
    s.init_deps(sup)
    s._draining = True
    s.start_optional_services()
    s._draining = False
    await s.aclose(1.0)
    s.start_optional_services()
    assert factory.calls == [] and not sup.has(OPTIONAL_BOOTSTRAP_TASK)


@pytest.mark.parametrize("factory", [Factory(name="md-writer"), Factory(critical=True)])
async def test_a_bad_bootstrap_spec_raises_for_the_controller_to_page(
        tmp_path: Path, redis: fakeredis.FakeAsyncRedis, factory: Factory) -> None:
    s = _session(tmp_path, redis, optional_bootstrap=factory, optional_flags=OptionalFlags(md_enabled=True))
    sup = TaskSupervisor([])
    s.init_deps(sup)
    with pytest.raises(ValueError):
        s.start_optional_services()
    assert not sup.has(factory.name)
    await s.aclose(1.0)


async def test_flag_problems_page_once_per_term(tmp_path: Path, redis: fakeredis.FakeAsyncRedis) -> None:
    sink: list[tuple[str, str, str]] = []
    flags = optional_flags_from_env({MD_ENABLED_VAR: "yes"})
    s = _session(tmp_path, redis, optional_flags=flags, page_sink=lambda *p: sink.append(p))
    s.init_deps(TaskSupervisor([]))
    s.start_optional_services()
    s.start_optional_services()
    assert [(p[0], p[1]) for p in sink] == [("high", "optional services settings")]
    assert MD_ENABLED_VAR in sink[0][2] and "yes" not in sink[0][2]          # the variable, never its value
    await asyncio.sleep(0.01)                                  # let the page's outbox write run
    await s.aclose(1.0)


# ============================================================================================ composite price hook
async def test_md_hook_runs_while_the_calendar_has_no_schedule(session: EngineSession) -> None:
    got: list[tuple[str, bool]] = []
    session.set_md_hook(lambda t, snap: got.append((t.symbol, snap)))
    assert session.calendar.version is None
    session._on_price_tick(tick(), True)
    assert got == [("EURUSD", True)]


async def test_md_hook_raising_on_every_tick_still_feeds_the_calendar(session: EngineSession) -> None:
    seen: list[str] = []
    sink: list[tuple[str, str, str]] = []
    session.ctx = dataclasses.replace(session.ctx, page_sink=lambda *p: sink.append(p))
    session.calendar.version = 1
    session.calendar.on_tick = (lambda a, s, b, k, ts, now, snapshot=False:  # type: ignore[method-assign]
                                seen.append(s))
    calls = {"n": 0}

    def broken(t: PriceDTO, snapshot: bool) -> None:
        calls["n"] += 1
        raise RuntimeError("ring full")

    session.set_md_hook(broken)
    n = MD_HOOK_TRIP_AFTER + 5
    for _ in range(n):
        session._on_price_tick(tick(), False)
    assert len(seen) == n                                      # every tick reached SM10
    assert calls["n"] == MD_HOOK_TRIP_AFTER                    # then skipped (60 s)
    guard = session._md_guard
    assert guard.trips == 1 and guard.skipped == 5 and guard.skipping
    assert [(p[0], p[1]) for p in sink] == [("high", "market-data hook failing")]
    await asyncio.sleep(0.01)                                  # let the page's outbox write run


async def test_tick_only_symbol_reaches_the_md_hook_but_not_sm10(session: EngineSession) -> None:
    got: list[str] = []
    seen: list[str] = []
    session.calendar.version = 1
    session.calendar.on_tick = (lambda a, s, b, k, ts, now, snapshot=False:  # type: ignore[method-assign]
                                seen.append(s))
    rt = session.orchestrator.runtime(ADAPTER)
    rt.tick_symbols = {"EURUSD"}
    session.orchestrator.set_md_tick_symbols(ADAPTER, ["BTCUSD"])
    rt.md_subscribed = {"BTCUSD"}
    session.set_md_hook(lambda t, snap: got.append(t.symbol))
    for sym in ("BTCUSD", "EURUSD"):
        session._on_price_tick(tick(sym), False)
    assert got == ["BTCUSD", "EURUSD"] and seen == ["EURUSD"]


async def test_no_md_hook_keeps_the_calendar_path_unchanged(session: EngineSession) -> None:
    seen: list[str] = []
    session.calendar.version = 1
    session.calendar.on_tick = (lambda a, s, b, k, ts, now, snapshot=False:  # type: ignore[method-assign]
                                seen.append(s))
    session._on_price_tick(tick(), False)
    assert seen == ["EURUSD"] and session._md_guard.raised == 0


def test_guard_skips_for_its_window_then_retries_and_pages_only_once() -> None:
    clk, pages = FakeClock(), Pages()
    guard = MarketDataHookGuard(pages, trip_after=3, skip_s=60.0, clock=clk)
    calls = {"n": 0}
    fail = {"on": True}

    def hook(t: PriceDTO, snapshot: bool) -> None:
        calls["n"] += 1
        if fail["on"]:
            raise ValueError("x")

    for _ in range(3):
        guard.call(hook, tick(), False)
    assert guard.trips == 1 and guard.skipping and len(pages.items) == 1
    guard.call(hook, tick(), False)
    assert calls["n"] == 3 and guard.skipped == 1
    clk.advance(60)
    for _ in range(3):
        guard.call(hook, tick(), False)                    # retried after the window, trips again
    assert calls["n"] == 6 and guard.trips == 2 and len(pages.items) == 1      # paged once
    clk.advance(60)
    fail["on"] = False
    guard.call(hook, tick(), False)
    assert guard.consecutive == 0 and not guard.skipping
    fail["on"] = True
    guard.call(hook, tick(), False)
    guard.call(hook, tick(), False)
    fail["on"] = False
    guard.call(hook, tick(), False)                        # a good call resets the streak: no trip
    assert guard.trips == 2


def test_guard_logs_the_error_type_only(caplog: pytest.LogCaptureFixture) -> None:
    guard = MarketDataHookGuard(Pages(), trip_after=2, clock=FakeClock())

    def hook(t: PriceDTO, snapshot: bool) -> None:
        raise ValueError("secret-detail-1.2345")

    with caplog.at_level(logging.DEBUG, logger="src.runtime.session"):
        guard.call(hook, tick(), False)
        guard.call(hook, tick(), False)
    assert "ValueError" in caplog.text and "secret-detail" not in caplog.text and "1.2345" not in caplog.text


# ============================================================================================ aclose / DRAINING
async def _hang() -> None:
    await asyncio.Event().wait()


class _HangTerms:
    async def mark_reached_leader(self, epoch: Epoch, now_ms: int) -> None:
        await asyncio.Event().wait()

    async def record_end_reason(self, epoch: Epoch, reason: str) -> None:
        await asyncio.Event().wait()


async def test_aclose_runs_close_steps_in_parallel(session: EngineSession) -> None:
    """Parallelism without wall time: each step finishes only once all three have started, so a sequential aclose
    would hang in the first step until the deadline and drop (terminate) all three."""
    done: list[str] = []
    dropped: list[str] = []
    started: list[str] = []
    all_started = asyncio.Event()

    def closer(name: str) -> Any:
        async def close() -> None:
            started.append(name)
            if len(started) == 3:
                all_started.set()
            await all_started.wait()
            done.append(name)
        return close

    session.ports = dataclasses.replace(session.ports, close=closer("pool"), terminate=lambda: dropped.append("pool"))
    assert session.register_closer("redis-md", closer("redis-md"), lambda: dropped.append("redis-md"))
    assert session.register_closer("arm pool", closer("arm pool"), lambda: dropped.append("arm pool"))
    await session.aclose(2.0)
    assert sorted(done) == ["arm pool", "pool", "redis-md"] and dropped == []
    assert not session.register_closer("late", closer("late"))       # after aclose: refused


async def test_aclose_cancelled_from_outside_drops_every_running_step(session: EngineSession,
                                                                      caplog: pytest.LogCaptureFixture) -> None:
    """A cancelled aclose cancels and terminates every close step still running before it re-raises: `_closed` is
    already set, so nothing else would ever close them."""
    dropped: list[str] = []
    cancelled: list[str] = []
    running = asyncio.Event()

    def hang(name: str) -> Any:
        async def close() -> None:
            running.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.append(name)
                raise
        return close

    async def fine() -> None:
        return None

    session.ports = dataclasses.replace(session.ports, close=hang("term pool"),
                                        terminate=lambda: dropped.append("term pool"))
    session.register_closer("redis-md", hang("redis-md"), lambda: dropped.append("redis-md"))
    session.register_closer("quick", fine, lambda: dropped.append("quick"))
    closing = asyncio.ensure_future(session.aclose(60.0))
    await running.wait()
    await settle()
    closing.cancel()
    with caplog.at_level(logging.WARNING, logger="src.runtime.session"), pytest.raises(asyncio.CancelledError):
        await closing
    assert sorted(dropped) == ["redis-md", "term pool"]                  # the finished step is left alone
    await settle()
    assert sorted(cancelled) == ["redis-md", "term pool"]
    assert "aclose cancelled; closing term pool dropped" in caplog.text
    await session.aclose(1.0)                                            # idempotent: a retry does nothing more
    assert sorted(dropped) == ["redis-md", "term pool"]


async def test_aclose_terminates_what_is_still_open(session: EngineSession,
                                                    caplog: pytest.LogCaptureFixture) -> None:
    dropped: list[str] = []
    session.ports = dataclasses.replace(session.ports, close=_hang, terminate=lambda: dropped.append("term pool"))
    session.register_closer("redis-md", _hang, lambda: dropped.append("redis-md"))
    session.register_closer("no fallback", _hang)

    async def fine() -> None:
        return None

    session.register_closer("quick", fine, lambda: dropped.append("quick"))
    started = time.monotonic()
    with caplog.at_level(logging.WARNING, logger="src.runtime.session"):
        await session.aclose(0.1)
    assert time.monotonic() - started < 0.3
    assert sorted(dropped) == ["redis-md", "term pool"]
    assert "closing no fallback did not finish" in caplog.text


async def test_a_raising_terminate_is_logged_not_raised(session: EngineSession,
                                                        caplog: pytest.LogCaptureFixture) -> None:
    def bad() -> None:
        raise OSError("socket gone")

    session.register_closer("redis-md", _hang, bad)
    with caplog.at_level(logging.WARNING, logger="src.runtime.session"):
        await session.aclose(0.05)
    assert "terminating redis-md failed" in caplog.text


async def test_draining_stays_inside_its_budget_with_a_hung_extra_closer(session: EngineSession) -> None:
    """Mirrors test_followups_4_9 test_drain_stays_inside_its_budget_when_postgres_hangs (RT-M6)."""
    s = session
    t = dataclasses.replace(FAST_TIMINGS, drain_close_s=0.2, drain_flush_s=0.2, drain_spill_s=0.2,
                            drain_record_s=0.2, drain_cluster_s=0.1, drain_aclose_s=0.2)
    s._t = t
    dropped: list[str] = []
    s.ports = dataclasses.replace(s.ports, terms=_HangTerms(), close=_hang,
                                  terminate=lambda: dropped.append("term pool"))
    s.register_closer("redis-md", _hang, lambda: dropped.append("redis-md"))
    s.register_closer("arm pool", _hang, lambda: dropped.append("arm pool"))
    started = time.monotonic()
    await asyncio.wait_for(s.drain("switchover"), 5.0)
    assert time.monotonic() - started < t.drain_budget_s + 0.4
    assert sorted(dropped) == ["arm pool", "redis-md", "term pool"]


def test_check_drain_budget_is_unchanged() -> None:
    t = SessionTimings()
    assert t.drain_aclose_s == 0.5 and t.drain_budget_s == pytest.approx(6.7)
    lt = LeaseTimings()
    assert check_drain_budget(t, lt.draining_deadline_s, lt.request_timeout_s) is None


# ============================================================================================ pools
class _FakePool:
    def __init__(self) -> None:
        self.terminated = 0
        self.close_started = asyncio.Event()

    async def close(self) -> None:
        self.close_started.set()
        await asyncio.Event().wait()                         # a server that never answers

    def terminate(self) -> None:
        self.terminated += 1


async def test_asyncpg_source_terminate_drops_the_pool() -> None:
    pool = _FakePool()
    AsyncpgSource(pool).terminate()
    assert pool.terminated == 1


async def test_lazy_source_terminate_reaches_a_pool_whose_close_was_cancelled() -> None:
    pool = _FakePool()

    async def factory(dsn: str, **kw: Any) -> AsyncpgSource:
        return AsyncpgSource(pool)

    src = LazyPgSource("postgresql://h/db", application_name="xtnl:9:n1", factory=factory)
    src.terminate()                                          # nothing created yet: a no-op
    assert pool.terminated == 0
    src = LazyPgSource("postgresql://h/db", application_name="xtnl:9:n1", factory=factory)
    await src._get()
    closing = asyncio.ensure_future(src.close())
    await pool.close_started.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    src.terminate()
    src.terminate()                                          # idempotent
    assert pool.terminated == 1 and src.closed
    with pytest.raises(ConnectionError):
        await src._get()


async def test_lazy_source_drops_a_pool_the_factory_finishes_after_terminate() -> None:
    """The factory is still connecting (connect_timeout 3 s > drain_aclose_s) when DRAINING calls terminate(): the
    pool it returns afterwards is terminated at once, never kept, and the acquire fails as after close()."""
    pool = _FakePool()
    entered, release = asyncio.Event(), asyncio.Event()

    async def factory(dsn: str, **kw: Any) -> AsyncpgSource:
        entered.set()
        await release.wait()
        return AsyncpgSource(pool)

    src = LazyPgSource("postgresql://h/db", application_name="xtnl:9:n1", factory=factory)
    getting = asyncio.ensure_future(src._get())
    await entered.wait()
    src.terminate()                                          # nothing reachable yet
    assert pool.terminated == 0
    release.set()
    with pytest.raises(ConnectionError):
        await getting
    assert pool.terminated == 1 and not src.created and src.closed
    with pytest.raises(ConnectionError):
        await src._get()


async def test_lazy_source_drops_a_pool_the_factory_finishes_after_close() -> None:
    pool = _FakePool()
    entered, release = asyncio.Event(), asyncio.Event()

    async def factory(dsn: str, **kw: Any) -> AsyncpgSource:
        entered.set()
        await release.wait()
        return AsyncpgSource(pool)

    src = LazyPgSource("postgresql://h/db", application_name="xtnl:9:n1", factory=factory)
    getting = asyncio.ensure_future(src._get())
    await entered.wait()
    await src.close()                                        # nothing created yet: returns at once
    release.set()
    with pytest.raises(ConnectionError):
        await getting
    assert pool.terminated == 1 and not src.created


async def test_pg_leader_ports_terminate_the_term_pool() -> None:
    ports = pg_leader_ports(EPOCH, control=LazyPgSource("postgresql://h/db", application_name="ctl"),
                            dsn="postgresql://h/db", node_id="n1", redis=fakeredis.FakeAsyncRedis(),
                            on_fence_lost=lambda e: None)
    assert isinstance(ports, PgLeaderPorts) and ports.terminate is not None
    term = getattr(ports.terminate, "__self__", None)
    assert isinstance(term, LazyPgSource) and term.application_name.startswith("xtnl:")
    ports.terminate()                                        # nothing created: no error, the pool is closed
    assert term.closed
    with pytest.raises(ConnectionError):
        async with term.acquire():
            pass


# ============================================================================================ settings / wiring
@pytest.mark.parametrize(("env", "md", "arm", "problems"), [
    ({}, False, False, 0),
    ({MD_ENABLED_VAR: "0", ARM_ENABLED_VAR: "0"}, False, False, 0),
    ({MD_ENABLED_VAR: "", ARM_ENABLED_VAR: " "}, False, False, 0),
    ({MD_ENABLED_VAR: "1"}, True, False, 0),
    ({MD_ENABLED_VAR: " 1 ", ARM_ENABLED_VAR: "1"}, True, True, 0),
    ({MD_ENABLED_VAR: "true"}, False, False, 1),
    ({MD_ENABLED_VAR: "2", ARM_ENABLED_VAR: "on"}, False, False, 2),
    ({ARM_ENABLED_VAR: "1"}, False, False, 1),                # the ARM needs the market-data layer
    ({MD_ENABLED_VAR: "yes", ARM_ENABLED_VAR: "1"}, False, False, 2),
])
def test_tolerant_flag_parse(env: dict[str, str], md: bool, arm: bool, problems: int) -> None:
    flags = optional_flags_from_env(env)
    assert (flags.md_enabled, flags.arm_enabled, len(flags.problems)) == (md, arm, problems)
    assert flags.any_enabled == (md or arm)
    for value in env.values():
        if value.strip() not in ("", "0", "1"):
            assert all(value not in p for p in flags.problems)


async def test_build_engine_parses_the_flags_and_never_refuses_a_bad_one(tmp_path: Path) -> None:
    env = {**MOCK, MD_ENABLED_VAR: "maybe", ARM_ENABLED_VAR: "1"}
    engine = build_engine(cfg(env, tmp_path), environ=env,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        flags = engine.context.optional_flags
        assert not flags.md_enabled and not flags.arm_enabled and len(flags.problems) == 2
        assert engine.context.optional_bootstrap is None              # wired in S31
    finally:
        await engine.aclose()
    on = {**MOCK, MD_ENABLED_VAR: "1"}
    engine = build_engine(cfg(on, tmp_path), environ=on,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        assert engine.context.optional_flags == OptionalFlags(md_enabled=True)
    finally:
        await engine.aclose()


async def test_build_engine_passes_the_bootstrap_hook_into_the_context(tmp_path: Path) -> None:
    factory = Factory()
    env = {**MOCK, MD_ENABLED_VAR: "1"}
    engine = build_engine(cfg(env, tmp_path), environ=env,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False,
                                            optional_bootstrap=factory))
    try:
        assert engine.context.optional_bootstrap is factory
        assert factory.calls == []                                     # only a LEADER calls it
    finally:
        await engine.aclose()


async def test_a_real_session_reaching_leader_adds_the_bootstrap_task(
        redis: fakeredis.FakeAsyncRedis, tmp_path: Path) -> None:
    """End to end through the wired NodeController and a real EngineSession (not the fake `Session`): with
    XTNL_MD_ENABLED=1 and a factory wired, LEADER adds `optional-bootstrap` to the controller's supervisor."""
    factory = Factory()
    env = {**WIRED_ENV, MD_ENABLED_VAR: "1"}
    backends: dict[int, MemoryBackend] = {}

    def ports(epoch: Epoch) -> Any:
        return backends.setdefault(epoch.token, MemoryBackend(redis, run_id, epoch.token)).ports(epoch)

    async def chrony() -> float:
        return 0.2

    def http(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503 if request.url.port == 8008 else 200)

    exits: list[str] = []
    fx = fixture(wall_ms())
    hooks = EngineHooks(etcd=FakeEtcd(FakeClock()), redis=redis, control_source=FakeSource(ControlConn()),
                        ports=ports, run_id_reader=run_id, exit_fn=exits.append, chrony=chrony,
                        http_client=lambda: httpx.AsyncClient(transport=httpx.MockTransport(http)),
                        patroni_client=httpx.AsyncClient(transport=httpx.MockTransport(http)),
                        adapters=lambda: [mock_adapter(fx)], timings=FAST_TIMINGS, require_keeper_thread=False,
                        optional_bootstrap=factory)
    config = RuntimeConfig.from_env(env, spill_path=str(tmp_path / "spill.jsonl"))
    engine = build_engine(config, environ=env, hooks=hooks)
    ctl = engine.controller
    try:
        await ctl.startup()
        await ctl.step()
        assert ctl.machine.role.value == NodeRole.LEADER.value, ctl.machine.history
        session = ctl.session
        assert isinstance(session, EngineSession)
        assert factory.calls == [session]
        assert ctl.supervisor is not None and ctl.supervisor.has(OPTIONAL_BOOTSTRAP_TASK)
        ctl.request_drain("switchover", exit_after=False)
        await ctl.step()
        assert ctl.role is NodeRole.STANDBY and not exits
    finally:
        await engine.aclose()


async def test_check_composition_never_calls_the_bootstrap(tmp_path: Path) -> None:
    factory = Factory()
    env = {**MOCK, MD_ENABLED_VAR: "1", ARM_ENABLED_VAR: "1"}
    assert await check_composition(cfg(env, tmp_path), environ=env,
                                   hooks=EngineHooks(optional_bootstrap=factory)) == []
    assert factory.calls == []
