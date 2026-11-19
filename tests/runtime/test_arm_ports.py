"""S36b (plan RT-3): the engine-side ports of the ARM runtime join (`src/runtime/arm_ports.py`) and the plan's
tracker request handlers, `notify-test`, status row and md-only heartbeat (`src/runtime/optional_plan.py`).

Synthetic adapters, accounts, symbols, levels and times only; no assertion message carries a value."""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from src.calendar.sessions import Phase, phase_at
from src.db.arm_ports import ControlsRow
from src.marketdata.types import M15
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.config import ArmSettings
from src.riskmgr.notices import TEST, ArmNotices, NoticeSettings, RouteBook
from src.riskmgr.ports import Pager
from src.riskmgr.sinks import Decided, DecisionSink, SinkChain
from src.riskmgr.tracker import TrackerRequest
from src.riskmgr.types import ArmEvent
from src.runtime.arm_ports import (
    CATCHUP_UNVERIFIED,
    INTEGRITY_INCIDENT,
    STOP_READS_PER_WINDOW,
    SQL_ARM_ACCOUNT_ORDERS,
    SQL_ARM_ACCOUNT_POSITIONS,
    SQL_ARM_INSTRUMENTS,
    SQL_ARM_OPEN_ACCOUNTS,
    SQL_ARM_STREAM_FENCES,
    CalendarGrid,
    LateNoticeSink,
    PgTrackerSnapshot,
    stop_read_cost,
)
from src.runtime.optional_plan import (
    ARM_KINDS,
    ARM_TRACKER_TASK,
    NOTIFY_TEST_WINDOW_MS,
    STAMP_S,
    ControlsView,
    OptionalRuntime,
    PayloadError,
    parse_tracker_request,
)
from src.runtime.ports import OperatorRequest
from src.runtime.request_handlers import Accepted, Refused
from tests.calendar.helpers import fx, ny
from tests.riskmgr.notice_fakes import FakeOutbox, Grid, NStore, Policies, decision, row
from tests.riskmgr.tracker_fakes import ADAPTER, FakeClock, Recorder
from tests.riskmgr.tracker_fakes import rig as tracker_rig

D = Decimal
TF = M15.timeframe_ms


# ============================================================================================ the grid
def grid() -> CalendarGrid:
    sched = fx(ADAPTER, "SYN.A")
    return CalendarGrid(schedule_of=lambda a, s: sched if (a, s) == (ADAPTER, "SYN.A") else None,
                        bars_capable=lambda a: a == ADAPTER)


def test_the_grid_counts_open_buckets_only_and_names_the_next_open() -> None:
    g = grid()
    fri_close = ny(2026, 11, 13, 17, 0)                      # Friday 17:00 New York: the weekly close
    sun_open = ny(2026, 11, 15, 17, 0)
    ends = g.open_bar_ends(ADAPTER, "SYN.A", M15, fri_close - 2 * TF, sun_open + 2 * TF)
    assert ends == [fri_close - TF, fri_close, sun_open + TF, sun_open + 2 * TF]   # nothing in the weekend
    assert g.next_open_ms(ADAPTER, "SYN.A", fri_close) == sun_open                 # a pre-close bar
    assert g.next_open_ms(ADAPTER, "SYN.A", fri_close - TF) is None                # an ordinary bar
    mon_break = ny(2026, 11, 16, 17, 0)                       # the 5-minute daily break is inside an open bucket
    assert mon_break + TF in g.open_bar_ends(ADAPTER, "SYN.A", M15, mon_break - TF, mon_break + TF)
    assert phase_at(fx(ADAPTER, "SYN.A"), mon_break) is Phase.ROLLOVER
    assert g.next_open_ms(ADAPTER, "SYN.A", mon_break) == ny(2026, 11, 16, 17, 5)
    # no schedule: no bar (fail closed); bar sides from the adapter's capability
    assert g.open_bar_ends(ADAPTER, "OTHER", M15, fri_close - 4 * TF, fri_close) == []
    assert g.bar_sides(ADAPTER) == frozenset({QuoteSide.BID, QuoteSide.ASK}) and g.bar_sides("x") == frozenset()
    assert g.skew(ADAPTER).p50_ms == 0                                             # cold skew without an estimate


def test_a_raising_schedule_or_capability_is_no_bar() -> None:
    def boom(*_a: Any) -> Any:
        raise RuntimeError("boom")

    g = CalendarGrid(schedule_of=boom, skew_of=boom, bars_capable=boom)
    assert g.open_bar_ends(ADAPTER, "SYN.A", M15, 0, 10 * TF) == [] and g.bar_sides(ADAPTER) == frozenset()
    assert g.next_open_ms(ADAPTER, "SYN.A", TF) is None and g.skew(ADAPTER).settle_ms > 0


# ============================================================================================ the sink slot
async def test_the_late_notice_slot_passes_through_until_bound() -> None:
    store = NStore()
    outbox = FakeOutbox()
    store.outbox = outbox
    clock = FakeClock(0)
    rec = Recorder()
    notices = ArmNotices(store=store, outbox=outbox, routes=RouteBook({}), policies=Policies(), grid=Grid(),
                         pager=Pager(rec.page, lambda t, b: None), clock=clock,
                         settings=ArmSettings(),
                         notices=NoticeSettings(test_at_ms_of_day=86_399_999))
    late = LateNoticeSink()
    chain = SinkChain((DecisionSink(), late))
    assert chain.names == ("decision", "notice") and not late.bound
    r = row("P1")
    store.rows[r.key] = r
    d = decision(r, seq=1)
    clock.now = d.decided_at_ms
    async with store.transaction() as tx:
        out = await chain.run(tx, ArmEvent.POST, d.bar_end_ms, [Decided(ref=r.ref, decision=d, position=None)])
    chain.settle(tx, committed=True)
    assert [w.status.value for w in out if w.status is not None] == ["INSERTED"] and outbox.rows == []
    late.bind(notices.sink())
    notices.learn(r)                                          # the route (the BarClock's CAS row carries it)
    d2 = decision(r, seq=2, bar_end=d.bar_end_ms + TF, at=d.decided_at_ms + TF)
    clock.now = d2.decided_at_ms
    async with store.transaction() as tx:
        await chain.run(tx, ArmEvent.POST, d2.bar_end_ms, [Decided(ref=r.ref, decision=d2, position=None)])
    chain.settle(tx, committed=True)
    assert outbox.rows and all(s.notice_id is not None for s in store.decisions)   # both noticed in the bound pass


# ============================================================================================ the snapshot
class _Conn:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data
        self.sql: list[str] = []

    async def fetch(self, sql: str, *args: object) -> list[tuple[Any, ...]]:
        self.sql.append(sql)
        got = self.data[sql]
        return list(got(*args) if callable(got) else got)


class _Db:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn
        self.snapshots = 0

    @asynccontextmanager
    async def snapshot(self) -> AsyncIterator[_Conn]:
        self.snapshots += 1
        yield self.conn


def _position_row(pid: str, account: str, status: str = "OPEN") -> tuple[Any, ...]:
    return (pid, account, ADAPTER, "SYN.A", "BUY", "1000", "1.2", status, False, None, "1.19", None, "E1", 0, 0,
            100, None, "STREAM", 1, 1)


def _order_row(oid: str, account: str) -> tuple[Any, ...]:
    return (None, oid, account, ADAPTER, "SYN.A", "BUY", "FILLED", "1.2", None, None, "1000", "0", False, None,
            False, None, 90, None, "STREAM", 1, 1, "MARKET")


def _instrument_row() -> tuple[Any, ...]:
    return (ADAPTER, "SYN.A", "AAA", "BBB", "BBB", "1", "UNITS", "0.0001", "1", False, "SYN.A", "0.0001", None, "fx")


async def test_the_snapshot_reads_each_account_once_in_one_snapshot_with_its_freshness() -> None:
    conn = _Conn({
        SQL_ARM_OPEN_ACCOUNTS: [(ADAPTER, "a1")],
        SQL_ARM_INSTRUMENTS: [_instrument_row()],
        SQL_ARM_STREAM_FENCES: [("orders",)],
        SQL_ARM_ACCOUNT_POSITIONS: lambda _a, acct: [_position_row("P1" if acct == "a1" else "P2", acct)],
        SQL_ARM_ACCOUNT_ORDERS: lambda _a, acct: [_order_row("E1", acct)],
    })
    db = _Db(conn)
    incident = [False]
    snap = PgTrackerSnapshot(db, integrity_incident=lambda: incident[0],  # type: ignore[arg-type]
                             catchup_verified=lambda a: True)
    images = await snap.read([(ADAPTER, "a2")])                    # a live ARM row's account with no OPEN position
    assert db.snapshots == 1 and [(i.account_id, i.fresh, i.reasons) for i in images] == [
        ("a1", False, ("orders_fence",)), ("a2", False, ("orders_fence",))]
    assert conn.sql.count(SQL_ARM_INSTRUMENTS) == 1                # instruments and fences once per adapter
    assert [p.position_id for p in images[0].positions] == ["P1"] and images[0].orders[0].broker_order_id == "E1"
    assert images[0].instrument("SYN.A") is not None
    conn.data[SQL_ARM_STREAM_FENCES] = []
    incident[0] = True
    snap2 = PgTrackerSnapshot(db, integrity_incident=lambda: incident[0],  # type: ignore[arg-type]
                              catchup_verified=lambda a: False)
    got = await snap2.read([])
    assert [(i.fresh, i.reasons) for i in got] == [(False, (INTEGRITY_INCIDENT, CATCHUP_UNVERIFIED))]

    def boom() -> bool:
        raise RuntimeError("boom")

    snap3 = PgTrackerSnapshot(db, integrity_incident=boom, catchup_verified=lambda a: True)  # type: ignore[arg-type]
    assert [i.reasons for i in await snap3.read([])] == [(INTEGRITY_INCIDENT,)]   # a raising input: not fresh
    assert "arm_positions" in SQL_ARM_ACCOUNT_POSITIONS and "DISARMED" in SQL_ARM_ACCOUNT_POSITIONS
    for sql in (SQL_ARM_OPEN_ACCOUNTS, SQL_ARM_ACCOUNT_POSITIONS, SQL_ARM_ACCOUNT_ORDERS, SQL_ARM_INSTRUMENTS,
                SQL_ARM_STREAM_FENCES):
        assert sql.lstrip().upper().startswith("SELECT") and not any(
            w in sql.upper() for w in ("INSERT", "UPDATE ", "DELETE", "BID", "ASK"))     # reads, no market data


# ============================================================================================ tracker requests
def _req(kind: str, target: str = f"{ADAPTER}:P1", payload: object = "{}", rid: int = 7) -> OperatorRequest:
    return OperatorRequest(request_id=rid, kind=kind, target=target, operator="op", payload=payload)


def test_tracker_request_payloads() -> None:
    assert parse_tracker_request(_req("arm_disarm")) == TrackerRequest.of(7, "arm_disarm", f"{ADAPTER}:P1")
    got = parse_tracker_request(_req("arm_repin", payload=json.dumps(
        {"v": 1, "strategy_code": "be", "strategy_version": 2})))
    assert (got.strategy_code, got.strategy_version) == ("be", 2)
    assert parse_tracker_request(_req("arm_repin", payload={})).strategy_code is None
    for kind, target, payload in (("arm_disarm", f"{ADAPTER}:P1", '{"v": 1, "strategy_code": "be", '
                                                                   '"strategy_version": 2}'),
                                  ("arm_repin", f"{ADAPTER}:P1", '{"v": 1, "strategy_code": "be"}'),
                                  ("arm_repin", f"{ADAPTER}:P1", '{"v": 1, "strategy_code": "be", '
                                                                 '"strategy_version": true}'),
                                  ("arm_repin", f"{ADAPTER}:P1", "not json"),
                                  ("arm_rearm", "no-colon", "{}")):
        try:
            parse_tracker_request(_req(kind, target, payload))
        except PayloadError:
            continue
        raise AssertionError(f"{kind} accepted a bad request")


def test_tracker_handlers_enqueue_or_refuse_at_once_and_the_tracker_answers() -> None:
    import asyncio

    assert {"arm_disarm", "arm_repin", "arm_rearm"} <= ARM_KINDS and STAMP_S[ARM_TRACKER_TASK] == 10.0
    r = tracker_rig()
    rt: Any = SimpleNamespace(graph=None)
    assert OptionalRuntime._tracker_request(rt, _req("arm_disarm")) == Refused("arm_not_running")  # noqa: SLF001
    rt.graph = SimpleNamespace(tracker=r.tracker)
    assert OptionalRuntime._tracker_request(rt, _req("arm_repin", payload="[]")) == Refused("bad_payload")  # noqa: SLF001
    from tests.riskmgr.tracker_fakes import image, position

    r.snapshot.images = [image(position("P1"))]
    asyncio.run(r.tracker.step())
    assert OptionalRuntime._tracker_request(rt, _req("arm_disarm", rid=11)) == Accepted(ARM_TRACKER_TASK)  # noqa: SLF001
    assert OptionalRuntime._tracker_request(rt, _req("arm_rearm", rid=12)) == Accepted(ARM_TRACKER_TASK)  # noqa: SLF001
    assert OptionalRuntime._tracker_request(  # noqa: SLF001
        rt, _req("arm_repin", rid=13, payload='{"v": 1, "strategy_code": "zz", "strategy_version": 9}')) == \
        Accepted(ARM_TRACKER_TASK)
    asyncio.run(r.tracker.step())
    answers = {i: (a.status.value, a.reason) for i, a in r.rec.answers}
    assert answers[11] == ("applied", "") and answers[12][0] == "moot" and answers[13][0] in ("refused", "moot")


# ============================================================================================ notify-test
async def test_notify_test_queues_one_test_notice_per_channel_once() -> None:
    store = NStore()
    outbox = FakeOutbox()
    store.outbox = outbox
    clock = FakeClock(10 * 86_400_000 + 3_600_000)
    rec = Recorder()
    notices = ArmNotices(store=store, outbox=outbox, routes=RouteBook({}), policies=Policies(), grid=Grid(),
                         pager=Pager(rec.page, lambda t, b: None), clock=clock, settings=ArmSettings(),
                         notices=NoticeSettings(test_at_ms_of_day=86_399_999))
    at = clock.now - 1_000
    view = ControlsView()
    view.row = ControlsRow(paused=False, pause_reason=None, paused_at_ms=None, drain_notices=False,
                           notify_test_at_ms=at, updated_at_ms=at)
    graph: Any = SimpleNamespace(jobs=None, clock=None, notices=notices, notify_test_seen=None, evaluator=None,
                                 budget=None)
    rt: Any = SimpleNamespace(session=SimpleNamespace(ctx=SimpleNamespace(clock_ms=lambda: clock.now)),
                              svc=SimpleNamespace(settings=SimpleNamespace(arm=ArmSettings())), controls=view)
    OptionalRuntime._after_tick(rt, graph)  # noqa: SLF001
    OptionalRuntime._after_tick(rt, graph)  # noqa: SLF001          (seen: queued once)
    await notices.step()
    await notices.step()
    bodies = outbox.bodies(TEST)
    assert sorted(b.channel or "" for b in bodies) == ["arm_email", "arm_push"] and all(b.bar_end_ms == at
                                                                                          for b in bodies)
    notices.request_test(at)                                   # the same request again (another term): key conflict
    await notices.step()
    assert len(outbox.bodies(TEST)) == 2
    old = clock.now - NOTIFY_TEST_WINDOW_MS - 1                 # an earlier term's request: nothing sent
    view.row = ControlsRow(paused=False, pause_reason=None, paused_at_ms=None, drain_notices=False,
                           notify_test_at_ms=old, updated_at_ms=old)
    OptionalRuntime._after_tick(rt, graph)  # noqa: SLF001
    await notices.step()
    assert len(outbox.bodies(TEST)) == 2 and graph.notify_test_seen == old


# ============================================================================================ cost, heartbeat
def test_stop_read_cost_is_per_adapter_with_armed_handles() -> None:
    class T:
        def handle_count(self, adapter_id: str) -> int:
            return 2 if adapter_id == ADAPTER else 0

    assert stop_read_cost(T(), ADAPTER) == STOP_READS_PER_WINDOW  # type: ignore[arg-type]
    assert stop_read_cost(T(), "other") == 0 and stop_read_cost(None, ADAPTER) == 0  # type: ignore[arg-type]


def test_the_md_only_heartbeat_uses_the_engine_pool_or_nothing() -> None:
    rt: Any = SimpleNamespace(session=SimpleNamespace(ports=SimpleNamespace()), _md_status=None,
                              _md_status_checked=False)
    assert OptionalRuntime._md_status_writer(rt) is None       # noqa: SLF001  in-memory ports: no engine pool
    pool = object()
    rt2: Any = SimpleNamespace(session=SimpleNamespace(ports=SimpleNamespace(engine_db=SimpleNamespace(source=pool))),
                               _md_status=None, _md_status_checked=False)
    writer = OptionalRuntime._md_status_writer(rt2)            # noqa: SLF001
    assert writer is not None and OptionalRuntime._md_status_writer(rt2) is writer  # noqa: SLF001
