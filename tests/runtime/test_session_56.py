"""IMPLEMENTATION_PLAN 5.6: the session wiring of the 5.1-5.5 open items (no network: fakeredis + in-memory ports).
One regression test (or more) per behaviour change."""
from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import fakeredis
import pytest

from src.adapters.sessions import fx_default_schedule
from src.calendar.machine import SessionState, SessionTransition
from src.calendar.readiness import readiness_check
from src.calendar.schedule import CalendarSchedule, symbol_schedule_from_json
from src.db.epoch import Epoch
from src.db.snapshot_reader import SnapshotInputs
from src.dto import OrderDTO, PriceDTO
from src.notifications.settings import NotificationSettings
from src.observability import logs as L
from src.oversee.snapshot import SnapshotReader
from src.pubsub.connection import ConnState
from src.runtime.ports import OperatorRequest
from src.runtime.session import (
    EngineSession,
    SessionContext,
    _ReadinessActions,
    scheduled_closes_before,
    token_expires_at_ms,
)
from src.runtime.supervisor import TaskSupervisor
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import FAST_TIMINGS, NODE, FakeLease, fixture, mock_adapter, wall_ms
from tests.runtime.test_followups_4_9 import FakeQueue

NY = ZoneInfo("America/New_York")
EPOCH = Epoch("c", 56)


def ny_ms(y: int, mo: int, d: int, h: int, mi: int = 0) -> int:
    return int(datetime(y, mo, d, h, mi, tzinfo=NY).timestamp() * 1000)


async def run_id(_: Any) -> str:
    return "run-56"


@dataclasses.dataclass
class Built:
    session: EngineSession
    exits: list[str]
    inputs: list[SnapshotInputs]


@pytest.fixture
async def built(tmp_path: Any) -> AsyncIterator[Built]:
    redis = fakeredis.FakeAsyncRedis()
    backend = MemoryBackend(redis, run_id, EPOCH.token)
    fx = fixture(wall_ms())
    exits: list[str] = []
    seen: list[SnapshotInputs] = []

    def ports(epoch: Epoch) -> Any:
        base = backend.ports(epoch)

        def reader(inputs: SnapshotInputs) -> SnapshotReader:
            seen.append(inputs)
            return base.snapshot_reader(inputs)
        return dataclasses.replace(base, snapshot_reader=reader)

    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [mock_adapter(fx)], lease=FakeLease(), run_id_reader=run_id,
                         exit_fn=exits.append, timings=FAST_TIMINGS, transfer_sources={"mock": True})
    s = EngineSession(ctx, EPOCH)
    try:
        yield Built(s, exits, seen)
    finally:
        await s.aclose(1.0)
        await redis.aclose()


def fx_calendar(s: EngineSession, now_ms: int, symbols: Sequence[str] = ("EURUSD",)) -> None:
    sched = {("mock", sym): symbol_schedule_from_json("mock", sym, "America/New_York", fx_default_schedule())
             for sym in symbols}
    s.calendar.init_from_schedule(CalendarSchedule(3, sched), now_ms)


# ============================================================================================ orchestrator
async def test_orchestrator_pages_and_mutes_stale_prices_by_the_calendar(built: Built) -> None:
    """Before 5.6: the orchestrator had no pager (AUTH_FAILED, stale-price and conn-key pages went nowhere)
    and no calendar hooks; a calendar-unknown cross must follow its adapter's market (5.2 correction)."""
    s = built.session
    o = s.orchestrator
    assert o._pager == s.page and o._market_open is not None and o._price_muted is not None
    for c, _ in s.consumers:
        assert c._pages._pager == s.page                 # type: ignore[attr-defined]
    fx_calendar(s, ny_ms(2026, 11, 11, 12))              # Wednesday noon New York: OPEN
    assert not s._price_muted("mock", "EURUSD")
    assert not s._price_muted("mock", "USDJPY")          # unknown to the calendar, adapter OPEN: not muted
    assert s._market_open_for_liveness("mock")
    fx_calendar(s, ny_ms(2026, 11, 14, 12))              # Saturday: CLOSED
    assert s._price_muted("mock", "EURUSD") and s._price_muted("mock", "USDJPY")
    assert not s._market_open_for_liveness("mock")


async def test_transfer_source_and_scheduled_closes_reach_the_snapshot_reader(built: Built) -> None:
    """Before 5.6: SnapshotInputs defaulted transfer_source to True (StoneX days never transfers_unverified)
    and scheduled_closes to {} (every early close marked the day partial_session)."""
    s = built.session
    (inputs,) = built.inputs
    assert inputs.transfer_source("mock") is True        # SessionContext.transfer_sources
    assert s.orchestrator.transfer_source("stonex") is False
    assert inputs.scheduled_closes == s._scheduled_closes


def test_last_scheduled_close_or_break_before_the_roll() -> None:
    index = {"sessions": [{"open": {"day": "SUN", "time": "18:00"}, "close": {"day": "FRI", "time": "16:00"}}],
             "breaks": [{"start": "16:00", "end": "18:00", "days": ["MON", "TUE", "WED", "THU"]}]}
    sched = CalendarSchedule(1, {
        ("mock", "US500"): symbol_schedule_from_json("mock", "US500", "America/New_York", index),
        ("mock", "EURUSD"): symbol_schedule_from_json("mock", "EURUSD", "America/New_York", fx_default_schedule()),
        ("other", "US500"): symbol_schedule_from_json("other", "US500", "America/New_York", index)})
    wed_roll = ny_ms(2026, 11, 11, 17)
    assert scheduled_closes_before(sched, "mock", wed_roll) == {("mock", "US500"): ny_ms(2026, 11, 11, 16)}
    fri_roll = ny_ms(2026, 11, 13, 17)
    assert scheduled_closes_before(sched, "mock", fri_roll) == {("mock", "US500"): ny_ms(2026, 11, 13, 16)}
    assert scheduled_closes_before(None, "mock", fri_roll) == {}


async def test_the_session_caches_scheduled_closes_per_roll_and_version(built: Built) -> None:
    s = built.session
    calls: list[int] = []
    s.schedule_loader.last = CalendarSchedule(1, {})
    import src.runtime.session as mod
    orig = mod.scheduled_closes_before

    def counting(schedule: Any, adapter_id: str, roll_ms: int) -> dict[Any, int]:
        calls.append(roll_ms)
        return orig(schedule, adapter_id, roll_ms)
    mod.scheduled_closes_before = counting  # type: ignore[assignment]
    try:
        s._scheduled_closes("mock", 1)
        s._scheduled_closes("mock", 1)                   # the sweep asks every pass: computed once
        s.schedule_loader.last = CalendarSchedule(2, {})
        s._scheduled_closes("mock", 1)
    finally:
        mod.scheduled_closes_before = orig  # type: ignore[assignment]
    assert calls == [1, 1]


async def test_credential_generation_follows_the_replaced_adapter(built: Built) -> None:
    """5.2: a rotated adapter must report its own generation, not the term's first adapter's."""
    s = built.session
    assert s._credential_generation("mock") == "default"
    s.orchestrator.runtime("mock").adapter = SimpleNamespace(adapter_id="mock", token_generation=7)  # type: ignore[assignment]
    assert s._credential_generation("mock") == "token:7"


async def test_a_scheduled_open_schedules_the_rest_only_resync(built: Built) -> None:
    """Before 5.6: on_market_open had no caller; no reconcile followed an open."""
    s = built.session
    rt = s.orchestrator.runtime("mock")
    at = wall_ms()
    s._on_transition(SessionTransition("mock", "EURUSD", SessionState.CLOSED, SessionState.OPEN, at, "init: open"))
    assert rt.open_resync_due_ms is None                 # LEADER_INIT initialisation: no resync
    s._on_transition(SessionTransition("mock", "EURUSD", SessionState.PRE_OPEN, SessionState.OPEN, at, "open"))
    assert rt.open_resync_due_ms is not None and at + 30_000 <= rt.open_resync_due_ms <= at + 60_000


async def test_every_new_latest_open_schedules_the_resync_evidence_included(built: Built) -> None:
    """Before the fix: OPEN_BY_EVIDENCE -> OPEN (a scheduled open while already open by evidence) and an entry into
    OPEN_BY_EVIDENCE scheduled no reconcile, although both move latest_open_ms forward and the gate then waits for
    a reconcile after it (oversee-rules.md §8)."""
    s = built.session
    rt = s.orchestrator.runtime("mock")
    at = wall_ms()
    s._on_transition(SessionTransition("mock", "EURUSD", SessionState.OPEN_BY_EVIDENCE, SessionState.OPEN, at,
                                       "scheduled open"))
    assert rt.open_resync_due_ms is not None
    rt.open_resync_due_ms = None
    s._on_transition(SessionTransition("mock", "EURUSD", SessionState.CLOSED, SessionState.OPEN_BY_EVIDENCE, at,
                                       "post-close tick with a new price"))
    assert rt.open_resync_due_ms is not None and rt.open_resync_due_ms >= at + 30_000
    rt.open_resync_due_ms = None
    s._on_transition(SessionTransition("mock", "EURUSD", SessionState.OPEN, SessionState.ROLLOVER, at, "daily break"))
    s._on_transition(SessionTransition("mock", "EURUSD", SessionState.CLOSED, SessionState.OPEN_BY_EVIDENCE, at,
                                       "init: evidence"))
    assert rt.open_resync_due_ms is None                 # a close, or a (re)initialisation: no resync


async def test_readiness_checks_the_token_validity(built: Built) -> None:
    """Before 5.6: token_valid_for_s always returned None, so a cTrader token near expiry was never paged."""
    s = built.session
    now = s.ctx.clock_ms()
    rt = s.orchestrator.runtime("mock")
    actions = _ReadinessActions(s)
    assert actions.token_valid_for_s("mock") is None     # the mock credential does not expire
    pair = SimpleNamespace(expires_at_ms=now + 3_600_000)
    rt.adapter = SimpleNamespace(adapter_id="mock", _tokens=SimpleNamespace(pair=pair))  # type: ignore[assignment]
    left = actions.token_valid_for_s("mock")
    assert left is not None and 3_590 < left <= 3_600
    pages: list[tuple[str, str, str]] = []
    s.page = lambda *p: pages.append(p)  # type: ignore[method-assign]
    report = await readiness_check(actions, "mock", "pre_open")
    assert any("token valid for only" in p for p in report.problems)
    assert pages and pages[0][0] == "critical"
    assert token_expires_at_ms(SimpleNamespace(token_expires_at_ms=5)) == 5  # type: ignore[arg-type]


# ============================================================================================ operator requests
async def test_release_account_and_refresh_cash_flows_requests_are_consumed(built: Built) -> None:
    """Before 5.6: both kinds were consumed as unsupported_kind (5.5 CLI requests had no consumer)."""
    s = built.session
    s._sup = TaskSupervisor([])
    released: list[tuple[str, str]] = []
    s.sweep.release_account = lambda op, acct: released.append((op, acct))  # type: ignore[method-assign]
    asked: list[str] = []
    answers = [True, False]

    async def request(acct: str) -> bool:
        asked.append(acct)
        return answers.pop(0)
    s.orchestrator.request_cash_flows = request  # type: ignore[method-assign]
    rt = s.orchestrator.runtime("mock")
    rt.accounts["ACC-1"] = object()  # type: ignore[assignment]
    old = 1_000
    queue = FakeQueue([
        OperatorRequest(1, "release_account", "ACC-1", "alice", '{"v":1,"account_id":"ACC-1","adapter_id":"mock"}'),
        OperatorRequest(2, "release_account", "ACC-1", "bob", '{"v":1,"account_id":"OTHER"}'),
        OperatorRequest(3, "refresh_cash_flows", "NOPE", "carol", '{"v":1,"account_id":"NOPE"}'),
        OperatorRequest(4, "refresh_cash_flows", "ACC-1", "dave", '{"v":1,"account_id":"ACC-1","since_ms":null}'),
    ])
    s.operator_requests = queue
    await s.operator_pass()
    assert released == [("alice", "ACC-1")]
    assert [c[1:] for c in queue.consumed] == [(1, "applied"), (2, "bad_payload"), (3, "unknown_account"),
                                               (4, "not_streaming")]
    rt.conn = SimpleNamespace(state=ConnState.STREAMING)  # type: ignore[assignment]
    queue.reqs += [
        OperatorRequest(5, "refresh_cash_flows", "ACC-1", "erin", '{"v":1,"account_id":"ACC-1"}'),
        OperatorRequest(6, "refresh_cash_flows", "ACC-1", "erin", {"v": 1, "account_id": "ACC-1"}),
        OperatorRequest(7, "refresh_cash_flows", "ACC-1", "erin", f'{{"v":1,"account_id":"ACC-1","since_ms":{old}}}'),
        OperatorRequest(8, "refresh_cash_flows", "ACC-1", "erin", '{"v":1,"account_id":"ACC-1","adapter_id":"x"}'),
    ]
    await s.operator_pass()
    assert [c[1:] for c in queue.consumed[4:]] == [(5, "requested"), (6, "rate_limited"), (7, "since_unsupported"),
                                                   (8, "adapter_mismatch")]
    assert asked == ["ACC-1", "ACC-1"]


# ============================================================================================ notifications
async def test_relay_follows_the_secrets_source_and_the_ruleset_cadence(built: Built) -> None:
    """Before 5.6: channels built once from the start-up settings, no channel_source / on_auth_error, and the
    relay's re-page cadence fixed at 900 s whatever the installed ruleset said."""
    s = built.session
    loads: list[int] = []
    invalidated: list[int] = []

    def load() -> NotificationSettings:
        loads.append(1)
        return NotificationSettings()
    s.ctx = dataclasses.replace(s.ctx, notification_settings=load, invalidate_secrets=lambda: invalidated.append(1))
    s.init_deps(TaskSupervisor([]))
    s._start_notifications()
    relay = s.relay
    assert relay is not None and s.channels is not None
    assert relay._source is s.channels and loads == []   # initial= : no secret read at build time
    s.channels.invalidate()
    assert invalidated == [1]
    base = s.rules.current
    s.rules.install(dataclasses.replace(base, sweep=dataclasses.replace(base.sweep, escalate_after_ms=60_000),
                                        ruleset_version=base.ruleset_version + 1))
    assert relay.escalate_after_ms == 60_000


async def test_relay_without_a_secrets_source_keeps_the_fixed_channels(built: Built) -> None:
    s = built.session
    s.init_deps(TaskSupervisor([]))
    s._start_notifications()
    assert s.channels is None and s.relay is not None and s.relay._source is None


# ============================================================================================ fence / telemetry
async def test_a_lost_fence_in_the_sweep_hard_exits(built: Built) -> None:
    """5.3 open question 1: RiskSweep was built without on_fence_lost (no engine-independent backstop)."""
    s = built.session
    assert s.sweep._on_fence_lost is not None
    s.sweep._on_fence_lost(RuntimeError("EpochMismatch"))
    assert built.exits and built.exits[0].startswith("fence:")
    seen: list[BaseException] = []
    s.ctx = dataclasses.replace(s.ctx, on_fence_lost=seen.append)
    s._on_fence_lost(KeyError("x"))
    assert len(seen) == 1 and len(built.exits) == 1


async def test_metrics_samples_leader_only_series_and_ingest_counter(built: Built) -> None:
    s = built.session
    names = {x.name for x in s.metrics_samples(leader=False)}
    assert names == {"xtnl_dlq_spool_ring_depth"}
    tick = PriceDTO(adapter_id="mock", symbol="EURUSD", bid=Decimal("1.1"), ask=Decimal("1.1001"),
                    spread_pips=Decimal("1"), timestamp_ms=wall_ms())
    s.orchestrator.handle_stream_item("mock", "prices", tick)
    got = {(x.name, tuple(sorted(x.labels.items()))): x.value for x in s.metrics_samples(leader=True)}
    assert got[("xtnl_ingest_events_total", (("adapter", "mock"), ("stream", "prices")))] == 1
    assert got[("xtnl_adapter_state", (("adapter", "mock"), ("state", s.orchestrator.runtime("mock").conn.state.value)))] == 1
    assert {k[1][0][1] for k in got if k[0] == "xtnl_bus_queue_depth"} == {
        "orders.internal.dto", "position.state.dto", "market.price.dto", "account.state.dto"}
    assert ("xtnl_risksweep_pass_duration_seconds", ()) not in got      # no full pass yet: absent


async def test_stream_items_are_logged_with_their_entity_fields(built: Built) -> None:
    s = built.session
    seen: list[dict[str, object]] = []
    s.orchestrator._accept_entity = lambda *a, **k: seen.append(dict(L.current_context()))  # type: ignore[method-assign]
    order = OrderDTO(client_order_id="C-1", broker_order_id="B-1", account_id="ACC", adapter_id="mock",
                     symbol="EURUSD", side="BUY", status="WORKING", entry_price=Decimal("1.1"),
                     ordered_units=Decimal(1), remaining_units=Decimal(1), broker_updated_at_ms=wall_ms())
    s.orchestrator.handle_stream_item("mock", "orders", order)
    assert seen == [{"client_order_id": "C-1", "account_id": "ACC"}]
    assert L.current_context() == {}


async def test_the_term_sets_and_clears_the_log_epoch(built: Built) -> None:
    s = built.session
    assert L.process_fields()["epoch"] == EPOCH.label
    await s.aclose(1.0)
    assert L.process_fields()["epoch"] is None
