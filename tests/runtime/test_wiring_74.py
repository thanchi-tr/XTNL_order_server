"""IMPLEMENTATION_PLAN 7.4: runtime wiring. The dead-man token follows the secrets source; the heartbeat's oldest
pass age covers every account the sweep evaluates (never-passed accounts included); the dashboard gets a copy of
each heartbeat; `frames_lost` is exported; the 7.1-7.3 APIs reach the production paths (pnl_source,
note_cash_flows on every direct fetch, account_states for broker_seq inheritance, subscribed symbols deciding
OPEN, pre-close quotes seeded at LEADER_INIT, mark_session). No network: fakeredis, in-memory ports, httpx
MockTransport."""
from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import fakeredis
import httpx
import pytest

from src.calendar.schedule import MaintenanceWindow
from src.db.epoch import Epoch
from src.db.rows import ACCOUNT_COLS
from src.dto import CashFlowDTO
from src.integrity.ports import CleanMark
from src.notifications.settings import DeadmanSettings
from src.observability.metrics import Sample
from src.oversee.sweep import AccountView
from src.pubsub.connection import ConnState
from src.runtime.deadman import (
    AUTH_REFRESH_MIN_INTERVAL_S,
    HttpsDeadmanTransport,
    build_payload,
)
from src.runtime.ingestion_control import ConsumerSwitches, OrchestratorReconciler
from src.runtime.pg_ports import SQL_ADAPTER_ACCOUNTS, PgLocalStateWithAccounts, pg_leader_ports
from src.runtime.session import (
    UNLISTED_ACCOUNTS,
    EngineSession,
    SessionContext,
    _DeadmanView,
    _DlqPriorityBridge,
)
from src.runtime.supervisor import TaskSupervisor
from src.runtime.wiring import (
    HEARTBEAT_COPY_TOKEN_VAR,
    HEARTBEAT_COPY_URL_VAR,
    EngineHooks,
    WiringError,
    build_engine,
    load_settings,
)
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.runtime.test_session_56 import fx_calendar, ny_ms
from tests.runtime.test_wiring import MOCK, SECRET_DEADMAN, cfg
from tests.runtime.test_wiring_credentials import FakeSource

EPOCH = Epoch("c", 74)
T0 = 1_763_000_000_000
DEADMAN_URL = "https://deadman.example.invalid/xtnl/heartbeat"
COPY_URL = "https://dashboard.example.invalid:8444/xtnl/heartbeat"


async def run_id(_: Any) -> str:
    return "run-74"


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


class Receiver:
    """Both Melbourne endpoints behind one httpx MockTransport: records (host, Authorization) per POST and answers
    401 to a token it does not expect."""

    def __init__(self) -> None:
        self.expect = {"deadman.example.invalid": "new-deadman-token", "dashboard.example.invalid": "copy-token"}
        self.seen: list[tuple[str, str | None]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("authorization")
        self.seen.append((request.url.host, auth))
        return httpx.Response(204 if auth == f"Bearer {self.expect[request.url.host]}" else 401)


@dataclasses.dataclass
class Built:
    session: EngineSession
    backend: MemoryBackend
    adapter: Any
    clock: Clock
    receiver: Receiver
    token: list[str]
    invalidations: list[int]


@pytest.fixture
async def built(tmp_path: Path) -> AsyncIterator[Built]:
    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    adapter = mock_adapter(fixture(wall_ms()))
    clock = Clock(T0)
    receiver = Receiver()
    token = ["old-deadman-token"]
    invalidations: list[int] = []
    ctx = SessionContext(
        node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
        adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id, exit_fn=lambda _r: None,
        timings=FAST_TIMINGS, transfer_sources={"mock": True}, clock_ms=clock,
        http_client=lambda: httpx.AsyncClient(transport=httpx.MockTransport(receiver)),
        deadman=DeadmanSettings(DEADMAN_URL, "old-deadman-token"), deadman_token=lambda: token[0],
        heartbeat_copy=DeadmanSettings(COPY_URL, None), heartbeat_copy_token=lambda: "copy-token",
        invalidate_secrets=lambda: invalidations.append(1))
    s = EngineSession(ctx, EPOCH)
    try:
        yield Built(s, backend, adapter, clock, receiver, token, invalidations)
    finally:
        await s.aclose(1.0)
        await redis.aclose()


# ============================================================================================ dead-man token
async def test_the_dead_man_token_follows_the_secrets_source_without_a_restart(built: Built) -> None:
    """Rotation: the receiver already wants the new token; the engine's cached secret still has the old one. The
    401 re-reads the secret (once, throttled); once the cache has the new token the next beat uses it."""
    s = built.session
    sup = TaskSupervisor([])
    s.init_deps(sup)
    s._start_notifications()
    try:
        sender = s.deadman
        assert sender is not None
        assert await sender.send_once() is False
        assert ("deadman.example.invalid", "Bearer old-deadman-token") in built.receiver.seen
        assert built.invalidations == [1]
        assert await sender.send_once() is False
        assert built.invalidations == [1]                       # throttled: at most once a minute
        built.token[0] = "new-deadman-token"                     # the refresher thread read the rotated secret
        assert await sender.send_once() is True
        assert built.receiver.seen[-2:].count(("deadman.example.invalid", "Bearer new-deadman-token")) == 1
        assert sender.consecutive_failures == 0
        # the dashboard's copy went out with every beat (the supervised loop's first beat included), with its own
        # token, whatever the dead-man POST did
        beats = [h for h, _ in built.receiver.seen if h == "deadman.example.invalid"]
        copies = [a for h, a in built.receiver.seen if h == "dashboard.example.invalid"]
        assert len(beats) >= 3 and copies == ["Bearer copy-token"] * len(beats) and sender.copies_sent == len(beats)
    finally:
        await sup.stop_all()


async def test_a_failing_token_source_keeps_the_last_good_token_and_a_fixed_token_is_unchanged() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(204)
    calls = [0]

    def source() -> str | None:
        calls[0] += 1
        if calls[0] > 1:
            raise RuntimeError("secret not read yet")
        return "tok-1"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        rotating = HttpsDeadmanTransport(DEADMAN_URL, token="fixed", client=client, token_source=source)
        fixed = HttpsDeadmanTransport(DEADMAN_URL, token="fixed", client=client)
        p = build_payload(_StaticSource(), T0)
        await rotating.post(p)
        await rotating.post(p)
        await fixed.post(p)
    assert seen == ["Bearer tok-1", "Bearer tok-1", "Bearer fixed"]


def test_a_rejected_token_re_reads_the_secret_at_most_once_a_minute() -> None:
    mono = [100.0]
    asks: list[int] = []
    t = HttpsDeadmanTransport(DEADMAN_URL, on_auth_error=lambda: asks.append(1), monotonic=lambda: mono[0])
    t._auth_rejected()
    t._auth_rejected()
    mono[0] += AUTH_REFRESH_MIN_INTERVAL_S
    t._auth_rejected()
    assert asks == [1, 1] and t.auth_refreshes == 2


class _StaticSource:
    def __getattr__(self, name: str) -> Any:
        values: dict[str, Any] = {"node_id": "node-a", "epoch_label": "74", "adapter_ids": (),
                                  "risksweep_pass_ms": {}, "degraded": ()}
        return lambda *a: values.get(name)


def test_build_engine_wires_the_rotating_tokens_and_the_dashboard_copy(tmp_path: Path) -> None:
    env = {**MOCK, HEARTBEAT_COPY_URL_VAR: COPY_URL, HEARTBEAT_COPY_TOKEN_VAR: "CHANGE_ME"}
    source = FakeSource({"XTNL_DEADMAN_TOKEN": "sm-deadman-1", HEARTBEAT_COPY_TOKEN_VAR: "sm-copy-1"})
    engine = build_engine(cfg(env, tmp_path), environ=env, hooks=EngineHooks(secrets=source))
    try:
        ctx = engine.context
        assert ctx.deadman_token is not None and ctx.heartbeat_copy_token is not None
        assert ctx.deadman_token() == "sm-deadman-1" and ctx.heartbeat_copy_token() == "sm-copy-1"
        source.values = {"XTNL_DEADMAN_TOKEN": "sm-deadman-2", HEARTBEAT_COPY_TOKEN_VAR: "sm-copy-2"}   # rotated
        assert ctx.deadman_token() == "sm-deadman-2" and ctx.heartbeat_copy_token() == "sm-copy-2"
        assert ctx.heartbeat_copy.url == COPY_URL
        assert ctx.invalidate_secrets == source.invalidate
        assert "sm-copy" not in repr(ctx.heartbeat_copy)
    finally:
        asyncio.run(engine.aclose())
    plain = build_engine(cfg(MOCK, tmp_path), environ=MOCK)          # env secrets never rotate
    try:
        assert plain.context.deadman_token is None and plain.context.deadman.token == SECRET_DEADMAN
        assert plain.context.heartbeat_copy.url is None
        assert [w for w in plain.settings.warnings if HEARTBEAT_COPY_URL_VAR in w]
    finally:
        asyncio.run(plain.aclose())


def test_a_bad_heartbeat_url_is_a_start_up_error_not_a_leader_init_failure() -> None:
    with pytest.raises(WiringError, match=HEARTBEAT_COPY_URL_VAR):
        load_settings(cfg(MOCK), {**MOCK, HEARTBEAT_COPY_URL_VAR: "http://dashboard.example.invalid/beat"})
    bad = {**MOCK, "XTNL_DEADMAN_URL": "ftp://deadman.example.invalid/beat"}
    with pytest.raises(WiringError, match="XTNL_DEADMAN_URL"):
        load_settings(cfg(bad), bad)


# ============================================================================================ pass ages
class _Accounts:
    def __init__(self, accounts: Sequence[str]) -> None:
        self.accounts = list(accounts)

    async def list_accounts(self) -> Sequence[str]:
        return list(self.accounts)

    async def read_account(self, account_id: str, now_ms: int) -> Any:
        raise AssertionError("not used")


async def test_the_heartbeat_pass_age_counts_accounts_that_never_passed(built: Built) -> None:
    """Before 7.4 an account without an AccountView was skipped, and with no view at all the age was null (which
    the receiver does not page on). Now: never listed -> from the sweep's start; listed but never passed -> from
    when it was first listed; the oldest wins."""
    s = built.session
    s.reader._inner = _Accounts(["A", "B"])                       # type: ignore[assignment]
    assert s.risksweep_pass_ms() == {UNLISTED_ACCOUNTS: T0}
    view = _DeadmanView(s)
    built.clock.now = T0 + 12_000
    assert build_payload(view, built.clock.now).oldest_risksweep_pass_age_ms == 12_000   # pages (> 10 s)
    await s.reader.list_accounts()                                # first listed at T0 + 12 s
    views = {"A": AccountView(T0 + 13_000, frozenset(), False)}
    s.sweep.account_view = views.get                              # type: ignore[method-assign]
    built.clock.now = T0 + 25_000
    assert s.risksweep_pass_ms() == {"A": T0 + 13_000, "B": T0 + 12_000}
    payload = build_payload(view, built.clock.now)
    assert payload.oldest_risksweep_pass_age_ms == 13_000          # B never passed: it is the oldest
    ages = [x for x in s.metrics_samples(leader=True) if x.name == "xtnl_risksweep_oldest_pass_age_seconds"]
    assert ages == [Sample("xtnl_risksweep_oldest_pass_age_seconds", 13.0)]
    views["B"] = AccountView(T0 + 24_000, frozenset(), False)
    assert build_payload(view, built.clock.now).oldest_risksweep_pass_age_ms == 12_000   # A is now the oldest
    s.reader._inner = _Accounts(["A"])                            # B gone from the list: pruned
    await s.reader.list_accounts()
    assert set(s.risksweep_pass_ms()) == {"A"} and set(s.reader.first_listed_ms) == {"A"}
    s.reader._inner = _Accounts([])
    await s.reader.list_accounts()
    assert s.risksweep_pass_ms() == {} and build_payload(view, built.clock.now).oldest_risksweep_pass_age_ms is None


async def test_the_unlisted_age_runs_from_the_sweeps_start_not_from_a_long_leader_init(built: Built) -> None:
    s = built.session
    built.clock.now = T0 + 90_000                                 # SM8 and the barrier took 90 s
    s._sweep_started_ms = built.clock.now
    assert s.risksweep_pass_ms() == {UNLISTED_ACCOUNTS: T0 + 90_000}


# ============================================================================================ frames_lost
def test_frames_lost_is_exported_on_the_leader_only(built: Built) -> None:
    s = built.session
    s.orchestrator.frames_lost.update({"orders": 3, "prices": 1})
    lost = [x for x in s.metrics_samples(leader=True) if x.name == "xtnl_frames_lost_total"]
    assert lost == [Sample("xtnl_frames_lost_total", 3.0, {"stream": "orders"}),
                    Sample("xtnl_frames_lost_total", 1.0, {"stream": "prices"})]
    assert not [x for x in s.metrics_samples(leader=False) if x.name == "xtnl_frames_lost_total"]


# ============================================================================================ 7.1 wiring
def test_the_sweep_reads_pnl_source_from_the_orchestrator(built: Built) -> None:
    inputs = built.backend.oversee.inputs
    assert inputs is not None and inputs.pnl_source == built.session.orchestrator.pnl_source
    assert inputs.pnl_source("mock") is True
    built.session.orchestrator.runtime("mock").cash_flows_partial = ("history truncated",)
    assert inputs.pnl_source("mock") is False                    # MaxDailyLossRule FLAGs pnl_unverified
    assert inputs.pnl_source("not-driven") is False               # fail closed


async def test_the_dlq_cash_flow_reread_reports_a_partial_ledger(built: Built) -> None:
    s = built.session
    built.adapter.set_cash_flows_partial("page limit reached")
    bridge = _DlqPriorityBridge(s.orchestrator, lambda: s.adapter_ids)
    assert await bridge.reread_cash_flow("mock", "no-such-tx", T0 - 3_600_000) is None
    assert s.orchestrator.pnl_source("mock") is False
    assert s.orchestrator.runtime("mock").cash_flows_partial == ("page limit reached",)


class _Rt:
    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        self.conn = type("C", (), {"state": ConnState.STREAMING})()
        self.subs: dict[str, Any] = {}


class _FakeOrch:
    """The parts of the orchestrator OrchestratorReconciler uses."""

    def __init__(self, adapter: Any) -> None:
        self.rt = _Rt(adapter)
        self.noted: list[tuple[str, Any, int]] = []
        self.resubmitted: list[CashFlowDTO] = []
        self.bus = object()
        self.health = type("H", (), {"pending": lambda self: []})()

    def runtime(self, adapter_id: str) -> _Rt:
        return self.rt

    async def run_snapshot(self, adapter_id: str, *, refresh_instruments: bool = True) -> None:
        return None

    def note_cash_flows(self, adapter_id: str, flows: Any, since_ms: int) -> tuple[str, ...]:
        self.noted.append((adapter_id, flows, since_ms))
        return ()

    def resubmit_cash_flow(self, cf: CashFlowDTO) -> None:
        self.resubmitted.append(cf)


async def test_the_sm3_reconcile_reports_its_cash_flow_fetch() -> None:
    class Adapter:
        async def fetch_cash_flows(self, since_ms: int) -> list[CashFlowDTO]:
            return []
    orch = _FakeOrch(Adapter())
    rec = OrchestratorReconciler(orch, lambda: ["mock"], ConsumerSwitches(), lambda: True)  # type: ignore[arg-type]
    assert await rec.reconcile_all(T0 - 1_000)
    assert [(a, s) for a, _, s in orch.noted] == [("mock", T0 - 1_000)]
    assert await rec.reconcile_all(None)                          # no fetch, nothing to report
    assert len(orch.noted) == 1


class _AcctConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args: object) -> list[tuple[object, ...]]:
        self.calls.append((sql, args))
        return [("ACC", "pepperstone", "USD", Decimal("1000"), None, T0, 17, "stream", 5, 99, T0)]


class _AcctSource:
    def __init__(self, conn: _AcctConn) -> None:
        self.conn = conn

    def acquire(self) -> Any:
        conn = self.conn

        class _Ctx:
            async def __aenter__(self) -> _AcctConn:
                return conn

            async def __aexit__(self, *exc: object) -> None:
                return None
        return _Ctx()


async def test_account_states_feed_broker_seq_inheritance_after_a_restart() -> None:
    conn = _AcctConn()
    db = type("Db", (), {"source": _AcctSource(conn)})()
    reader = PgLocalStateWithAccounts(db)                         # type: ignore[arg-type]
    (acct,) = await reader.account_states("pepperstone")
    assert acct.account_id == "ACC" and acct.broker_seq == 17 and acct.adapter_id == "pepperstone"
    assert conn.calls == [(SQL_ADAPTER_ACCOUNTS, ("pepperstone",))]
    assert SQL_ADAPTER_ACCOUNTS.startswith(f"SELECT {ACCOUNT_COLS} FROM account_state WHERE adapter_id = $1")
    ports = pg_leader_ports(EPOCH, control=_AcctSource(conn), dsn="postgresql://x@127.0.0.1/x",  # type: ignore[arg-type]
                            node_id=NODE, redis=fakeredis.FakeAsyncRedis(), on_fence_lost=lambda e: None)
    assert isinstance(ports.local_state, PgLocalStateWithAccounts)


# ============================================================================================ 7.2 wiring
def test_only_subscribed_symbols_decide_the_adapters_market(built: Built) -> None:
    s = built.session
    now = ny_ms(2026, 11, 14, 12)                                 # Saturday noon New York: FX closed
    fx_calendar(s, now, symbols=("EURUSD", "USDJPY"))
    rt = s.orchestrator.runtime("mock")
    rt.tick_symbols.clear()
    rt.tick_symbols.add("EURUSD")
    assert s.calendar._subscribed is not None
    assert [x.symbol for x in s.calendar.deciding_sessions("mock")] == ["EURUSD"]


async def test_leader_init_seeds_the_pre_close_quotes_from_last_clean_marks(built: Built) -> None:
    s = built.session
    marks = [CleanMark("mock", "EURUSD", Decimal("1.1"), Decimal("1.2"), T0 - 5_000),
             CleanMark("gone", "EURUSD", Decimal("1"), Decimal("2"), T0 - 5_000)]   # not an adapter of this node
    built.backend.tables.integrity.marks = marks
    seeded: list[list[tuple[str, str, Decimal, Decimal, int]]] = []

    def spy(rows: Any, now_ms: int) -> int:
        rows = list(rows)
        seeded.append(rows)
        return len(rows)
    s.calendar.seed_quotes = spy                                  # type: ignore[method-assign]
    assert await s.init_calendar(T0) is not None
    assert seeded == [[("mock", "EURUSD", Decimal("1.1"), Decimal("1.2"), T0 - 5_000)]]
    built.backend.tables.integrity.fail["last_clean_marks"] = 1   # unreadable: logged, LEADER_INIT goes on
    assert await s.seed_calendar_quotes(T0) == 0


def test_the_sweep_marks_a_symbol_in_maintenance_closed(built: Built) -> None:
    s = built.session
    now = ny_ms(2026, 11, 11, 12)                                 # Wednesday noon New York: OPEN
    fx_calendar(s, now)
    assert s._session_state("mock", "EURUSD") == "OPEN"
    s.calendar.replace_declared_maintenance([MaintenanceWindow("mock", now - 60_000, now + 3_600_000, "upgrade")])
    s.calendar.tick(now)
    assert s._session_state("mock", "EURUSD") == "CLOSED" == s.calendar.mark_session("mock", "EURUSD")


def test_without_a_calendar_every_symbol_is_marked_open(built: Built) -> None:
    assert built.session.calendar.version is None
    assert built.session._session_state("mock", "EURUSD") == "OPEN"

