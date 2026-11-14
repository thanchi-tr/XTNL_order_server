"""Downtime catch-up end to end (single-PC mode): the engine is not on 24/7, so it records when it was on and, on
the next start-up or wake from sleep, reads back from the broker what changed while it was off.

Shared by the in-memory test (tests/e2e/test_downtime_memory.py, always runs) and the PostgreSQL + Redis test
(tests/e2e/test_downtime_pg.py, ORDER_SERVER_TEST_DSN / ORDER_SERVER_TEST_REDIS). Both run the real composition:
UptimeTracker (src/runtime/uptime.py) -> EngineSession through LEADER_INIT -> CatchupCoordinator
(src/runtime/catchup.py) -> MockBrokerAdapter.fetch_history -> orchestrator -> consumers -> storage ports. Only the
storage differs. No network.

`run_restart_scenario`:
1. run 1 (its clock three hours in the past): the first-run catch-up finishes, the heartbeat records
   `adapter_coverage`, the engine stops cleanly (`SIGINT`);
2. while 'off' the broker gains: an order placed and FILLED (its position still OPEN), a position opened and
   CLOSED by a reduce-only closing trade, a stop entry placed and CANCELLED (the offline history fixture), a
   working stop that moved, a stored OPEN position closed whose history row has no entry price (the opening
   trade is older than the window), and an older history row of a position the stream has since updated;
3. run 2 (a new epoch) catches up from the coverage minus the overlap: every change is committed, the stored
   entry of the closed position is kept, the older row overwrites nothing, `catchup_runs` says complete with the
   counts of what the catch-up itself added on top of the start-up snapshot, and the adapter is verified for
   ActionGate. With `incomplete=True` the broker's history says it is incomplete: a high page names the
   uncovered window while the fence is still held, and ActionGate stays unverified for the adapter.
"""
from __future__ import annotations

import dataclasses
import re
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from src.adapters.mock import MockBrokerAdapter, MockFixture, build_fx_fixture, build_offline_history_fixture
from src.cache import keys as K
from src.db.epoch import Epoch
from src.db.fence import EpochMismatch
from src.db.uptime_store import CatchupRecord, Coverage, RunRow, StoredRow
from src.dto import OrderDTO, OrderStatus, PositionDTO
from src.oversee.gate import Action
from src.runtime.catchup import CatchupSettings
from src.runtime.pg_ports import PgLeaderPorts
from src.runtime.session import EngineSession, SessionContext
from src.runtime.uptime import SuspendEvent, UptimeStore, UptimeTracker
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import ACCOUNT, ADAPTER, FAST_TIMINGS, NODE, FakeLease, Leader, become_leader, eventually
from tests.e2e.scenario import row as committed_row
from tests.e2e.scenario import wall_ms

HOUR_MS = 3_600_000
MINUTE_MS = 60_000
OVERLAP_MS = 900_000
OFFLINE_FOR_MS = 3 * HOUR_MS
#: A decimal number in a page body would be an amount or a price: pages carry counts, ids and times only.
DECIMAL = re.compile(r"\d+\.\d+")


# ===================================================================== storage
class Views(Protocol):
    async def runs(self) -> list[RunRow]: ...                     # oldest first

    async def coverage(self, adapter_id: str) -> Coverage | None: ...

    async def catchups(self) -> list[CatchupRecord]: ...          # oldest first


@dataclass
class Backend:
    """One storage backend for every run of a scenario."""

    uptime_store: UptimeStore
    ports: Callable[[Epoch], Any]
    views: Views
    redis: Any
    epoch_changed: Callable[[Epoch], None] = lambda epoch: None
    ctx_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class _Run:
    run_id: str
    node_id: str
    started: int
    last_seen: int
    stopped: int | None = None
    reason: str | None = None
    gaps: int = 0


class MemDowntimeStore:
    """`UptimeStore` + `CatchupStore` + `Views` over the in-memory tables, with the PostgreSQL stores' semantics:
    coverage only forward and only under the current epoch, a fenced catchup_runs insert, a stop written once."""

    def __init__(self, backend: MemoryBackend) -> None:
        self.t = backend.tables
        self._runs: dict[str, _Run] = {}
        self._coverage: dict[str, Coverage] = {}
        self.records: list[CatchupRecord] = []

    # --- UptimeStore
    async def start_run(self, run_id: str, node_id: str, at_ms: int) -> None:
        self._runs.setdefault(run_id, _Run(run_id, node_id, at_ms, at_ms))

    async def heartbeat(self, run_id: str, at_ms: int) -> bool:
        r = self._runs.get(run_id)
        if r is None:
            return False
        r.last_seen = max(r.last_seen, at_ms)
        return True

    async def note_suspended(self, run_id: str, at_ms: int) -> None:
        r = self._runs[run_id]
        r.gaps += 1
        r.last_seen = max(r.last_seen, at_ms)

    async def stop_run(self, run_id: str, at_ms: int, reason: str) -> None:
        r = self._runs[run_id]
        if r.stopped is None:
            r.stopped, r.reason, r.last_seen = at_ms, reason, max(r.last_seen, at_ms)

    async def advance_coverage(self, epoch_token: int, adapter_id: str, at_ms: int, run_id: str | None) -> bool:
        if epoch_token != self.t.epoch:
            return False
        cur = self._coverage.get(adapter_id)
        if cur is None or at_ms >= cur.last_streaming_ms:
            self._coverage[adapter_id] = Coverage(adapter_id, at_ms, run_id)
        return True

    # --- CatchupStore
    async def coverage(self, adapter_id: str) -> Coverage | None:
        return self._coverage.get(adapter_id)

    async def stored_orders(self, adapter_id: str, ids: Collection[str]) -> Mapping[str, StoredRow]:
        return {e.broker_order_id: StoredRow(e.checksum, str(e.status.value)) for e in self.t.entities.values()
                if isinstance(e, OrderDTO) and e.adapter_id == adapter_id and e.broker_order_id in ids}

    async def stored_positions(self, adapter_id: str, ids: Collection[str]) -> Mapping[str, StoredRow]:
        return {e.position_id: StoredRow(e.checksum, str(e.status)) for e in self.t.entities.values()
                if isinstance(e, PositionDTO) and e.adapter_id == adapter_id and e.position_id in ids}

    async def known_cash_flows(self, adapter_id: str, ids: Collection[str]) -> set[str]:
        return {tid for (aid, tid) in self.t.cash_flows if aid == adapter_id and tid in ids}

    async def prior_activity_ms(self, adapter_id: str, run_id: str | None, before_ms: int) -> int | None:
        """As PgCatchupStore over engine_runs (the in-memory tables keep no event_log history)."""
        ends = [max(r.last_seen, r.stopped or r.last_seen) for r in self._runs.values()
                if r.run_id != run_id and r.started < before_ms]
        return min(ends) if ends else None

    async def record(self, epoch_token: int, rec: CatchupRecord) -> int:
        if epoch_token != self.t.epoch:
            raise EpochMismatch(f"{epoch_token} != {self.t.epoch}")
        self.records.append(rec)
        return len(self.records)

    # --- Views
    async def runs(self) -> list[RunRow]:
        return [RunRow(r.run_id, r.started, r.last_seen, r.stopped, r.reason, r.gaps)
                for r in sorted(self._runs.values(), key=lambda r: r.started)]

    async def catchups(self) -> list[CatchupRecord]:
        return list(self.records)


def memory_backend(redis: Any, run_id_reader: Any, epoch: Epoch) -> tuple[Backend, MemoryBackend, MemDowntimeStore]:
    mem = MemoryBackend(redis, run_id_reader, epoch.token)
    store = MemDowntimeStore(mem)

    def ports(e: Epoch) -> Any:
        base = mem.ports(e)
        fields = {f.name: getattr(base, f.name) for f in dataclasses.fields(base)}
        return PgLeaderPorts(**fields, catchup=store)

    def epoch_changed(e: Epoch) -> None:          # what LEADER_INIT's epoch bump does to engine_epoch
        mem.tables.epoch = e.token
        mem.oversee.engine_epoch = e.token

    return (Backend(store, ports, store, redis, epoch_changed, {"run_id_reader": run_id_reader}), mem, store)


# ===================================================================== one engine process
@dataclass
class Process:
    """One engine process: its uptime tracker, its leader term (session) and the pages it sent."""

    tracker: UptimeTracker
    session: EngineSession
    leader: Leader
    pages: list[tuple[str, str, str, bool]]       # (severity, title, body, adapter fence held when sent)
    exits: list[str]

    async def stop(self, reason: str = "SIGINT") -> None:
        """A clean stop, as src/main.py does it: the signal's reason, DRAINING, then the run's stop row."""
        self.tracker.note_stop_reason(reason)
        await self.leader.shutdown()
        await self.session.aclose()
        await self.tracker.stop()

    def titles(self, severity: str) -> list[str]:
        return [t for s, t, _, _ in self.pages if s == severity]


async def start_process(b: Backend, adapter: MockBrokerAdapter, epoch: Epoch, tmp_path: Path, *,
                        clock_ms: Callable[[], int] = wall_ms, monotonic: Callable[[], float] | None = None,
                        settings: CatchupSettings | None = None) -> Process:
    """Start an engine process and make it the leader. The tracker is ticked by the test (no background loop)."""
    pages: list[tuple[str, str, str, bool]] = []
    exits: list[str] = []
    holder: list[EngineSession] = []
    tracker_kw: dict[str, Any] = {} if monotonic is None else {"monotonic": monotonic}
    tracker = UptimeTracker(b.uptime_store, NODE, ping_s=60, tick_s=5.0, clock_ms=clock_ms, **tracker_kw)

    def sink(sev: str, title: str, body: str) -> None:
        held = bool(holder) and holder[0].orchestrator.fence_held(ADAPTER)
        pages.append((sev, title, body, held))

    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / f"spill-{epoch.revision}.jsonl", redis=b.redis,
                         ports=b.ports, adapters=lambda: [adapter], lease=FakeLease(), exit_fn=exits.append,
                         timings=FAST_TIMINGS, transfer_sources={ADAPTER: True}, page_sink=sink,
                         engine_run_id=lambda: tracker.run_id,
                         catchup=settings or CatchupSettings(overlap_ms=OVERLAP_MS), **b.ctx_kwargs)
    session = EngineSession(ctx, epoch)
    holder.append(session)
    tracker.coverage = session.coverage_targets

    def on_suspended(ev: SuspendEvent) -> None:
        session.resume_after_sleep(ev.wall_gap_ms)
    tracker.on_suspended = on_suspended
    b.epoch_changed(epoch)
    await tracker.tick()                                   # the engine_runs start row
    leader = await become_leader(session, epoch)
    return Process(tracker, session, leader, pages, exits)


async def caught_up(p: Process, epoch: Epoch) -> None:
    async def eligible() -> bool:
        return p.session.coverage_targets() == (epoch.token, [ADAPTER])
    await eventually(eligible, "catch-up finished and the adapter offered for coverage")


async def entity(p: Process, key: str) -> Any:
    r = await committed_row(p.session.ports.integrity_db, key)
    return None if r is None else r.entity


async def gate_ctx_verified(p: Process) -> bool:
    """What the session's ActionGate reader says about the adapter's catch-up (risk-increasing check input)."""
    reader = p.session.gate._reader  # noqa: SLF001 - the gate's own reader, as wired by the session
    assert reader is not None
    ctx = await reader.read(Action(ACCOUNT, ADAPTER, risk_increasing=True, symbol="EURUSD"))
    return ctx.catchup_verified


def offline_fixture(now_ms: int) -> MockFixture:
    return build_fx_fixture(adapter_id=ADAPTER, account_id=ACCOUNT, ts_ms=now_ms - 5_000)


@dataclass(frozen=True)
class OfflineChanges:
    coverage_ms: int
    orders_new: tuple[str, ...]          # placed and finished while off: only the history knows them
    positions_closed: tuple[str, ...]    # opened and closed while off
    amended_stop: OrderDTO               # a working stop that moved while off (still WORKING)
    closed_known: PositionDTO            # a stored OPEN position closed while off (broker book, with entry)


def script_offline_changes(adapter: MockBrokerAdapter, fx: MockFixture, coverage_ms: int,
                           until_ms: int) -> OfflineChanges:
    """Broker changes made while the engine was off, between the recorded coverage and `until_ms`."""
    off = build_offline_history_fixture(ADAPTER, ACCOUNT, since_ms=coverage_ms, until_ms=until_ms)
    adapter.change_while_off(*off.history_orders, *off.history_positions)
    span = until_ms - coverage_ms
    eu_stop = next(o for o in fx.orders if o.broker_order_id == "P-EU-1-SL")
    amended = dataclasses.replace(eu_stop, entry_price=Decimal("1.0820"),
                                  broker_updated_at_ms=coverage_ms + span // 5)
    adapter.change_while_off(amended)
    pf = next(p for p in fx.positions if p.position_id == "PF-1")
    closed = dataclasses.replace(pf, units=Decimal(0), raw_qty=Decimal(0), status="CLOSED",
                                 broker_updated_at_ms=coverage_ms + span * 2 // 5)
    adapter.set_position(closed)                                      # the by-id read knows the entry ...
    adapter.list_in_history(dataclasses.replace(closed, entry_price=None, side="SELL"))   # ... the history not
    stale = next(p for p in fx.positions if p.position_id == "P-EU-1")   # REST row 1 s older than the stream's
    adapter.list_in_history(stale)
    return OfflineChanges(coverage_ms, ("H-MKT-1", "H-RT-1", "H-RT-1-X", "H-STP-1"), ("H-RT-1",), amended, closed)


async def assert_offline_changes_committed(p: Process, ch: OfflineChanges) -> None:
    want = {"H-MKT-1": OrderStatus.FILLED, "H-RT-1": OrderStatus.FILLED, "H-RT-1-X": OrderStatus.FILLED,
            "H-STP-1": OrderStatus.CANCELLED}
    for oid, status in want.items():
        o = await entity(p, K.order_key(ADAPTER, oid))
        assert isinstance(o, OrderDTO) and o.status is status, (oid, o)
    stop = await entity(p, K.order_key(ADAPTER, "P-EU-1-SL"))
    assert isinstance(stop, OrderDTO) and stop.status is OrderStatus.WORKING
    assert stop.entry_price == ch.amended_stop.entry_price                   # the working order that moved
    mkt = await entity(p, K.position_key(ADAPTER, "H-MKT-1"))
    assert isinstance(mkt, PositionDTO) and mkt.status == "OPEN"
    rt = await entity(p, K.position_key(ADAPTER, "H-RT-1"))
    assert isinstance(rt, PositionDTO) and rt.status == "CLOSED"            # opened and closed while off


async def assert_nothing_overwritten_by_older_rows(p: Process, ch: OfflineChanges) -> None:
    eu = await entity(p, K.position_key(ADAPTER, "P-EU-1"))
    assert isinstance(eu, PositionDTO) and eu.units == Decimal(10000), eu   # the stream's newer row stands
    pf = await entity(p, K.position_key(ADAPTER, "PF-1"))
    assert isinstance(pf, PositionDTO) and pf.status == "CLOSED"
    assert pf.entry_price == ch.closed_known.entry_price and pf.side == ch.closed_known.side, pf


def assert_pages_carry_no_amounts(p: Process) -> None:
    for _, title, body, _ in p.pages:
        if "catch-up" in title:
            assert not DECIMAL.search(body), body


# ===================================================================== the restart scenario
async def run_restart_scenario(b: Backend, tmp_path: Path, epochs: tuple[Epoch, Epoch], *,
                               incomplete: bool = False) -> tuple[Process, OfflineChanges]:
    """Run 1, offline changes, run 2. Returns run 2 (still leader; the caller stops it) and the changes."""
    e1, e2 = epochs
    then = wall_ms() - OFFLINE_FOR_MS
    fx = offline_fixture(then)
    adapter = MockBrokerAdapter.from_fixture(fx, finite_streams=False)

    # --- run 1: on three hours ago, caught up, coverage recorded, clean stop -------------------------------
    p1 = await start_process(b, adapter, e1, tmp_path, clock_ms=lambda: wall_ms() - OFFLINE_FOR_MS)
    try:
        await caught_up(p1, e1)

        async def stream_row() -> bool:
            eu = await entity(p1, K.position_key(ADAPTER, "P-EU-1"))
            return isinstance(eu, PositionDTO) and eu.units == Decimal(10000)
        await eventually(stream_row, "the stream's P-EU-1 update committed")
        await p1.tracker.tick()                            # heartbeat: adapter_coverage for the caught-up adapter
        await p1.tracker.heartbeat()
    finally:
        await p1.stop("SIGINT")
    cov = await b.views.coverage(ADAPTER)
    assert cov is not None and cov.run_id == p1.tracker.run_id
    assert then <= cov.last_streaming_ms <= then + 5 * MINUTE_MS
    assert [c.reason for c in await b.views.catchups()] == ["startup"]
    assert (await b.views.catchups())[0].complete and (await b.views.catchups())[0].offline_from_ms is None

    # --- while off --------------------------------------------------------------------------------------
    changes = script_offline_changes(adapter, fx, cov.last_streaming_ms, wall_ms() - MINUTE_MS)
    if incomplete:
        adapter.set_history_incomplete("trade history paging limit reached")

    # --- run 2: a new process and epoch catches up from the coverage minus the overlap ---------------------
    p2 = await start_process(b, adapter, e2, tmp_path)

    async def recorded() -> bool:
        return len(await b.views.catchups()) == 2 and p2.session.catchup is not None \
            and p2.session.catchup.finished(ADAPTER)
    await eventually(recorded, "start-up catch-up recorded")
    rec = (await b.views.catchups())[-1]
    assert rec.reason == "startup" and rec.run_id == p2.tracker.run_id
    assert rec.offline_from_ms == cov.last_streaming_ms
    assert rec.gap_start_ms == cov.last_streaming_ms - OVERLAP_MS and rec.gap_end_ms >= rec.started_at_ms
    # counts: what the catch-up itself added on top of the start-up snapshot (which already holds every open
    # order / position, the moved stop and the by-id re-read close of PF-1)
    assert (rec.orders_new, rec.orders_updated, rec.positions_new, rec.positions_closed, rec.anomalies) == \
        (4, 0, 1, 1, 0), rec
    assert (rec.orders_seen, rec.positions_seen) == (5, 4), rec
    assert adapter.history_requests[-1][0] == rec.gap_start_ms
    await assert_offline_changes_committed(p2, changes)
    await assert_nothing_overwritten_by_older_rows(p2, changes)
    assert_pages_carry_no_amounts(p2)
    assert any(t == f"downtime catch-up: {ADAPTER}" and "engine was offline from" in body
               for s, t, body, _ in p2.pages if s == "info"), p2.pages

    runs = await b.views.runs()
    assert [r.run_id for r in runs] == [p1.tracker.run_id, p2.tracker.run_id]
    assert runs[0].stopped_at_ms is not None and runs[0].end_reason == "SIGINT" and runs[1].stopped_at_ms is None

    catchup = p2.session.catchup
    assert catchup is not None
    if not incomplete:
        assert rec.complete and rec.covered_from_ms == rec.gap_start_ms and not rec.notes, rec
        assert not [t for t in p2.titles("high") if "catch-up" in t], p2.pages
        assert catchup.verified(ADAPTER) and await gate_ctx_verified(p2)
    else:
        assert not rec.complete and "history incomplete" in rec.notes and "paging limit" in rec.notes, rec
        high = [(body, held) for s, t, body, held in p2.pages
                if s == "high" and t == f"downtime catch-up incomplete: {ADAPTER}"]
        assert len(high) == 1, p2.pages
        body, held = high[0]
        assert held, "the fence must still be held when the incomplete catch-up pages"
        assert "uncovered window" in body
        # fail closed: risk-increasing actions stay denied for the adapter for the rest of the term
        assert not catchup.verified(ADAPTER) and not await gate_ctx_verified(p2)
    assert not p1.exits and not p1.leader.exits and not p2.exits, (p1.exits, p2.exits)
    return p2, changes
