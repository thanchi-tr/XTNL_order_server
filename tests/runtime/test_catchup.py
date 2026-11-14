"""Downtime catch-up (src/runtime/catchup.py): window, history submit, completeness re-reads, counts, pages, the
fence held until the catch-up finished, retries, resume after sleep. No network: a scripted adapter, the real
IngestionOrchestrator, and an in-memory store the 'consumers' commit into."""
from __future__ import annotations

import asyncio
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from src.adapters.base import BrokerError, BrokerHistory, new_unmapped_rows, note_unmapped
from src.db.fence import EpochMismatch
from src.db.uptime_store import CatchupRecord, Coverage, StoredRow
from src.dto import CashFlowDTO, OrderDTO, OrderStatus, PositionDTO
from src.observability.metrics import MetricsRegistry
from src.runtime.catchup import (
    DAY_MS,
    CatchupCoordinator,
    CatchupMetrics,
    CatchupSettings,
    duration_text,
    iso_utc,
)
from tests.pubsub.conftest import ManualClock, cash_flow, ev, order, position
from tests.pubsub.test_catchup_hooks import HistoryAdapter, history
from tests.pubsub.test_orchestrator import Env, K, make, to_streaming, wait_for

NOW = 1_700_000_000_000
FAST = CatchupSettings(overlap_ms=900_000, first_run_days=7, commit_timeout_s=2.0, max_attempts=3, retry_s=0.01,
                       poll_s=0.005)
AMOUNT = "98765.43"          # a cash-flow amount that must never appear in a page


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(NOW)


@dataclass
class Db:
    """In-memory CatchupStore + LocalStateReader: the 'PostgreSQL' the committer writes into."""

    orders: dict[str, OrderDTO] = field(default_factory=dict)
    positions: dict[str, PositionDTO] = field(default_factory=dict)
    flows: set[str] = field(default_factory=set)
    cov: Coverage | None = None
    records: list[tuple[int, CatchupRecord]] = field(default_factory=list)
    coverage_errors: int = 0
    record_error: BaseException | None = None
    prior: int | None = None                      # the engine's last known active time before this term
    prior_calls: list[tuple[str | None, int]] = field(default_factory=list)

    def apply(self, item: object) -> None:
        if isinstance(item, OrderDTO):
            cur = self.orders.get(item.broker_order_id)
            if cur is None or item.broker_updated_at_ms >= cur.broker_updated_at_ms:
                self.orders[item.broker_order_id] = item
        elif isinstance(item, PositionDTO):
            cur_p = self.positions.get(item.position_id)
            if cur_p is None or item.broker_updated_at_ms >= cur_p.broker_updated_at_ms:
                self.positions[item.position_id] = item
        elif isinstance(item, CashFlowDTO):
            self.flows.add(item.transaction_id)

    async def coverage(self, adapter_id: str) -> Coverage | None:
        if self.coverage_errors:
            self.coverage_errors -= 1
            raise ConnectionError("postgres down")
        return self.cov

    async def stored_orders(self, adapter_id: str, ids: Collection[str]) -> Mapping[str, StoredRow]:
        return {i: StoredRow(o.checksum, o.status.value) for i, o in self.orders.items() if i in ids}

    async def stored_positions(self, adapter_id: str, ids: Collection[str]) -> Mapping[str, StoredRow]:
        return {i: StoredRow(p.checksum, p.status) for i, p in self.positions.items() if i in ids}

    async def known_cash_flows(self, adapter_id: str, ids: Collection[str]) -> set[str]:
        return {i for i in self.flows if i in ids}

    async def prior_activity_ms(self, adapter_id: str, run_id: str | None, before_ms: int) -> int | None:
        self.prior_calls.append((run_id, before_ms))
        return self.prior

    async def record(self, epoch_token: int, rec: CatchupRecord) -> int:
        if self.record_error is not None:
            raise self.record_error
        self.records.append((epoch_token, rec))
        return len(self.records)

    async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
        return ([o for o in self.orders.values() if not o.status.terminal],
                [p for p in self.positions.values() if p.status == "OPEN"])


@dataclass
class Rig:
    env: Env
    ad: HistoryAdapter
    db: Db
    coord: CatchupCoordinator
    pages: list[tuple[str, str, str]]
    stop: asyncio.Event
    tasks: list[asyncio.Task[None]]

    @property
    def rt(self) -> object:
        return self.env.orch.runtime("mock")

    async def snapshot(self) -> None:
        """SM4: streams SUBSCRIBED, a SNAPSHOTTING pass (committed by the consumers), STREAMING."""
        for s in ("orders", "account", "prices"):
            self.env.item(s, ev(K.SUBSCRIBED, s))
        await self.env.orch.run_snapshot("mock")
        conn = self.env.orch.runtime("mock").conn
        if conn.state.value != "STREAMING":
            to_streaming(self.env)

    async def close(self) -> None:
        self.stop.set()
        await asyncio.gather(*self.tasks, return_exceptions=True)


async def rig(tmp_path: Path, clock: ManualClock, *, db: Db | None = None,
              settings: CatchupSettings = FAST) -> Rig:
    ad = HistoryAdapter()
    db = db or Db()
    env = make(tmp_path, clock, adapter=ad, local_state=db)
    pages: list[tuple[str, str, str]] = []
    coord = CatchupCoordinator(env.orch, db, db, adapter_ids=lambda: ["mock"], epoch_token=lambda: 7,
                               run_id=lambda: "run-1", pager=lambda s, t, b: pages.append((s, t, b)),
                               settings=settings, metrics=CatchupMetrics(), clock_ms=clock)
    stop = asyncio.Event()

    async def committer() -> None:
        while not stop.is_set():
            for item in env.orders() + env.positions() + env.account_items():
                db.apply(item)
            await asyncio.sleep(0.001)

    tasks = [asyncio.create_task(committer())]
    return Rig(env, ad, db, coord, pages, stop, tasks)


def start(r: Rig) -> None:
    r.tasks.append(asyncio.create_task(r.coord.run(r.stop)))


# ===================================================================== happy path
async def test_startup_catchup_reads_from_coverage_minus_overlap_and_counts_what_changed(
        tmp_path: Path, clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 3_600_000, "run-0"))
    db.orders["W1"] = order("W1", OrderStatus.WORKING, t=NOW - 7_200_000)          # stored working, then filled
    db.positions["P1"] = position("P1", "OPEN", t=NOW - 7_200_000)                 # stored open, then closed
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history([order("W1", OrderStatus.FILLED, t=NOW - 1_800_000),
                            order("N1", OrderStatus.CANCELLED, t=NOW - 1_700_000)],     # placed + cancelled offline
                           [position("P1", "CLOSED", t=NOW - 1_600_000),
                            position("P2", "CLOSED", t=NOW - 1_500_000)],               # opened + closed offline
                           since=NOW - 4_500_000, until=NOW)
    r.ad.flows = [cash_flow("T1", AMOUNT)]
    r.ad.flows_only_since = NOW - 4_500_000                  # booked offline: only the catch-up window reads it
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert r.ad.called("fetch_history") == [(NOW - 3_600_000 - 900_000, NOW)]
    assert r.ad.called("fetch_cash_flows")[-1] == NOW - 4_500_000
    (epoch, rec), = r.db.records
    assert epoch == 7 and rec.complete and rec.reason == "startup" and rec.run_id == "run-1"
    assert (rec.orders_seen, rec.orders_new, rec.orders_updated) == (2, 1, 1)
    assert (rec.positions_seen, rec.positions_new, rec.positions_closed) == (2, 1, 2)
    assert rec.cash_flows_new == 1 and rec.anomalies == 0 and rec.offline_from_ms == NOW - 3_600_000
    assert r.db.orders["W1"].status is OrderStatus.FILLED and r.db.orders["N1"].status is OrderStatus.CANCELLED
    (sev, title, body), = r.pages
    assert sev == "info" and title == "downtime catch-up: mock"
    assert "engine was offline from 2023-11-14T21:13:20Z to 2023-11-14T22:13:20Z (1h 0m)" in body
    assert "1 orders new, 1 updated, 2 positions closed while offline" in body
    assert AMOUNT not in body
    rt = r.env.orch.runtime("mock")
    assert not rt.fence_holds and not any(s.lost for s in rt.subs.values())     # fence released, gaps cleared
    reg = MetricsRegistry("n")
    reg.register("c", r.coord.metrics.samples)
    text = reg.render()
    assert 'xtnl_catchup_runs_total{node="n",adapter="mock",complete="true"} 1' in text
    assert 'xtnl_catchup_rows_total{node="n",adapter="mock",kind="position_closed"} 2' in text
    assert 'xtnl_offline_gap_seconds{node="n"} 3600' in text


async def test_the_fence_stays_held_until_the_catchup_has_finished(tmp_path: Path, clock: ManualClock) -> None:
    r = await rig(tmp_path, clock)
    gate = asyncio.Event()
    real = r.ad.fetch_history

    async def slow(since_ms: int, until_ms: int) -> BrokerHistory:
        await gate.wait()
        return await real(since_ms, until_ms)

    r.ad.fetch_history = slow  # type: ignore[method-assign]
    r.ad.history = history(since=0, until=NOW)
    r.coord.begin_startup()
    start(r)
    rt = r.env.orch.runtime("mock")
    try:
        await r.snapshot()
        await wait_for(lambda: rt.passes_committed == 1)
        await asyncio.sleep(0.05)
        assert rt.fence_holds == {"catchup"} and all(s.lost for s in rt.subs.values())   # snapshot did not clear
        assert not r.coord.finished("mock")
        gate.set()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert not rt.fence_holds and not any(s.lost for s in rt.subs.values())


async def test_first_run_window_is_the_older_of_seven_days_and_the_oldest_working_entity(
        tmp_path: Path, clock: ManualClock) -> None:
    db = Db()
    db.orders["OLD"] = order("OLD", OrderStatus.WORKING, t=NOW - 20 * DAY_MS)
    r = await rig(tmp_path, clock, db=db)
    r.ad.orders_by_id["OLD"] = order("OLD", OrderStatus.WORKING, t=NOW - 20 * DAY_MS)
    r.ad.history = history(since=NOW - 20 * DAY_MS, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert r.ad.called("fetch_history") == [(NOW - 20 * DAY_MS, NOW)]
    (_, rec), = r.db.records
    assert rec.offline_from_ms is None and rec.complete
    assert "no earlier coverage recorded (first run)" in r.pages[0][2]


async def test_first_run_without_stored_entities_reads_seven_days(tmp_path: Path, clock: ManualClock) -> None:
    r = await rig(tmp_path, clock)
    r.ad.history = history(since=NOW - 7 * DAY_MS, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert r.ad.called("fetch_history") == [(NOW - 7 * DAY_MS, NOW)]


# ===================================================================== incomplete
async def test_retention_shorter_than_the_downtime_pages_the_uncovered_window(tmp_path: Path,
                                                                              clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 10 * DAY_MS, None))
    r = await rig(tmp_path, clock, db=db)
    h = history(since=NOW - 10 * DAY_MS - 900_000, until=NOW)
    r.ad.history = BrokerHistory("mock", h.since_ms, h.until_ms, covered_from_ms=NOW - 2 * DAY_MS, orders=(),
                                 positions=(), complete=True, notes=())
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert not rec.complete and rec.covered_from_ms == NOW - 2 * DAY_MS
    high = [p for p in r.pages if p[0] == "high"]
    assert len(high) == 1 and high[0][1] == "downtime catch-up incomplete: mock"
    assert "uncovered window 2023-11-04T21:58:20Z .. 2023-11-12T22:13:20Z" in high[0][2]
    assert "retention" in high[0][2]
    assert not r.env.orch.runtime("mock").fence_holds              # paged: the hold is released


async def test_an_adapter_without_history_is_incomplete_and_paged(tmp_path: Path, clock: ManualClock) -> None:
    r = await rig(tmp_path, clock)                                 # history None: the contract's default answer
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert not rec.complete and "history not supported" in rec.notes
    assert any(p[0] == "high" for p in r.pages)


async def test_a_stored_working_order_the_broker_no_longer_knows_is_an_anomaly(tmp_path: Path,
                                                                               clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    db.orders["LOST"] = order("LOST", OrderStatus.WORKING, t=NOW - DAY_MS)
    db.orders["SNAP"] = order("SNAP", OrderStatus.WORKING, t=NOW - DAY_MS)
    r = await rig(tmp_path, clock, db=db)
    r.ad.open_orders = [order("SNAP", OrderStatus.WORKING, t=NOW - DAY_MS)]   # still open: in the snapshot
    r.ad.orders_by_id["LOST"] = None                                          # broker: not found
    r.ad.history = history(since=NOW - 960_000, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert not rec.complete and rec.anomalies == 1
    assert r.db.orders["LOST"].status is OrderStatus.WORKING        # never closed on absence
    high = [p for p in r.pages if p[0] == "high"][0]
    assert "anomalies: order LOST" in high[2]
    # by-id re-reads: the snapshot pass read LOST (first pass), the catch-up read it once more; SNAP is in the snapshot
    assert r.ad.called("fetch_order_status").count("LOST") == 2 and "SNAP" not in r.ad.called("fetch_order_status")


async def test_a_failing_read_is_retried_then_recorded_incomplete_and_released(tmp_path: Path,
                                                                               clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    r = await rig(tmp_path, clock, db=db)

    async def broken(since_ms: int, until_ms: int) -> BrokerHistory:
        raise BrokerError("history endpoint down")

    r.ad.fetch_history = broken  # type: ignore[method-assign]
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert not rec.complete and "BrokerError after 3 attempts" in rec.notes
    assert [p[0] for p in r.pages] == ["info", "high"]
    assert not r.env.orch.runtime("mock").fence_holds


async def test_a_transient_error_is_retried_and_then_succeeds(tmp_path: Path, clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 60_000, None), coverage_errors=1)
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(since=NOW - 960_000, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert rec.complete


async def test_a_lost_fence_propagates_out_of_the_task(tmp_path: Path, clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 60_000, None), record_error=EpochMismatch("engine_epoch moved"))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(since=NOW - 960_000, until=NOW)
    r.coord.begin_startup()
    task = asyncio.create_task(r.coord.run(r.stop))
    try:
        await r.snapshot()
        with pytest.raises(EpochMismatch):
            await asyncio.wait_for(task, 3)
    finally:
        await r.close()
    assert r.env.orch.runtime("mock").fence_holds == {"catchup"}      # never released on a lost fence


# ===================================================================== resume after sleep / restart
async def test_resume_after_sleep_forces_resyncing_and_catches_up_after_a_newer_pass(
        tmp_path: Path, clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(since=0, until=NOW)
    r.coord.begin_startup()
    start(r)
    rt = r.env.orch.runtime("mock")
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
        db.cov = Coverage("mock", NOW, None)
        clock.ms += 3_600_000                                       # the PC slept for an hour
        r.coord.resume_after_sleep()
        assert rt.fence_holds == {"catchup"} and rt.resync_event.is_set()
        assert all(not s.subscribed for s in rt.subs.values())
        await asyncio.sleep(0.05)
        assert len(r.db.records) == 1                               # waits for a pass newer than the request
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert [rec.reason for _, rec in r.db.records] == ["startup", "resume_after_sleep"]
    assert r.ad.called("fetch_history")[-1] == (NOW - 900_000, NOW + 3_600_000)
    assert not rt.fence_holds


async def test_a_restarted_task_requeues_the_catchup_it_was_running(tmp_path: Path, clock: ManualClock) -> None:
    r = await rig(tmp_path, clock)
    r.ad.history = history(since=0, until=NOW)
    r.coord.begin_startup()
    first_stop = asyncio.Event()
    t1 = asyncio.create_task(r.coord.run(first_stop))
    await asyncio.sleep(0.05)                                     # the job waits for the first pass
    t1.cancel()                                                   # the supervisor restarts the task
    await asyncio.gather(t1, return_exceptions=True)
    assert r.coord.busy() and not r.coord.finished("mock")
    assert r.env.orch.runtime("mock").fence_holds == {"catchup"}
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert len(r.db.records) == 1


# ===================================================================== settings + helpers
def test_settings_from_env_validates_and_covers_the_heartbeat_interval() -> None:
    s = CatchupSettings.from_env({})
    assert (s.overlap_ms, s.first_run_days) == (900_000, 7)
    s = CatchupSettings.from_env({"XTNL_CATCHUP_OVERLAP_S": "1800", "XTNL_CATCHUP_FIRST_RUN_DAYS": "30"}, ping_s=1800)
    assert (s.overlap_ms, s.first_run_days) == (1_800_000, 30)
    for env, ping in (({"XTNL_CATCHUP_OVERLAP_S": "600"}, 900), ({"XTNL_CATCHUP_OVERLAP_S": "x"}, 60),
                      ({"XTNL_CATCHUP_FIRST_RUN_DAYS": "0"}, 60), ({"XTNL_CATCHUP_FIRST_RUN_DAYS": "400"}, 60),
                      ({"XTNL_CATCHUP_OVERLAP_S": "30"}, None),
                      ({"XTNL_CATCHUP_OVERLAP_S": "120"}, 60)):      # below the 300 s floor (short sleep)
        with pytest.raises(ValueError, match="XTNL_CATCHUP_"):
            CatchupSettings.from_env(env, ping_s=ping)


def test_duration_text() -> None:
    assert duration_text(65_000) == "1m 5s"
    assert duration_text(3_720_000) == "1h 2m"
    assert duration_text(DAY_MS + 3_660_000) == "1d 1h 1m"
    assert duration_text(-5) == "0m 0s"


def test_pages_and_records_carry_no_amounts() -> None:
    """The record type has no amount field at all (counts, ids, times)."""
    fields = set(CatchupRecord.__dataclass_fields__)
    assert not {f for f in fields if re.search(r"amount|balance|equity|margin|pnl|price", f)}


# ===================================================================== skeptic review: coverage only for what was read
async def test_a_failed_catchup_holds_coverage_back_and_retries_until_it_reads_the_window(
        tmp_path: Path, clock: ManualClock) -> None:
    """Every read failed: paged and released (as before), but adapter_coverage must not advance past the window
    nobody read (the next start-up would skip it for good); a `retry` catch-up re-reads it later."""
    db = Db(cov=Coverage("mock", NOW - 3_600_000, None))
    settings = CatchupSettings(overlap_ms=900_000, first_run_days=7, commit_timeout_s=2.0, max_attempts=2,
                               retry_s=0.01, poll_s=0.005, retry_after_s=300.0)
    r = await rig(tmp_path, clock, db=db, settings=settings)
    real = r.ad.fetch_history
    failing = [True]

    async def flaky(since_ms: int, until_ms: int) -> BrokerHistory:
        if failing[0]:
            raise BrokerError("history endpoint down")
        return await real(since_ms, until_ms)

    r.ad.fetch_history = flaky  # type: ignore[method-assign]
    r.ad.history = history([order("OFF", OrderStatus.FILLED, t=NOW - 1_000_000)], since=0, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: len(r.db.records) == 1 and r.coord.finished("mock"))
        assert not r.db.records[0][1].complete
        assert not r.coord.vouches("mock")                         # coverage held back
        assert not r.env.orch.runtime("mock").fence_holds          # paged: the hold is released meanwhile
        high = [p for p in r.pages if p[0] == "high"][0]
        assert "adapter_coverage held back" in high[2] and "retry in 5m 0s" in high[2]
        await asyncio.sleep(0.05)
        assert len(r.db.records) == 1                              # not before the retry is due
        failing[0] = False
        clock.ms += 300_000
        await wait_for(lambda: len(r.db.records) == 2 and r.coord.finished("mock"))
    finally:
        await r.close()
    rec = r.db.records[1][1]
    assert rec.reason == "retry" and rec.complete and rec.gap_start_ms == NOW - 3_600_000 - 900_000
    assert r.db.orders["OFF"].status is OrderStatus.FILLED
    assert r.coord.vouches("mock")


async def test_a_permanent_gap_is_paged_once_and_does_not_hold_coverage(tmp_path: Path, clock: ManualClock) -> None:
    """What the broker cannot answer (here: no history facility) would give the same answer again: no retry."""
    r = await rig(tmp_path, clock)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
        clock.ms += 7_200_000
        await asyncio.sleep(0.05)
    finally:
        await r.close()
    assert len(r.db.records) == 1 and r.coord.vouches("mock")


async def test_an_interruption_after_the_read_stops_coverage_and_runs_a_reconnect_catchup(
        tmp_path: Path, clock: ManualClock) -> None:
    """A socket loss after the catch-up read: an order placed and filled while it lasted is in no reconcile
    snapshot. Coverage stops at once; once the reconnect's pass has committed, the history is re-read from the
    unchanged coverage."""
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(since=0, until=NOW)
    r.coord.begin_startup()
    start(r)
    rt = r.env.orch.runtime("mock")
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
        assert r.coord.vouches("mock")
        n_pages = len(r.pages)
        clock.ms += 600_000
        r.env.item("orders", ev(K.DISCONNECTED, "orders"))        # network drop
        assert not r.coord.vouches("mock")
        await wait_for(lambda: r.coord.busy())                     # a reconnect catch-up is queued, fence held
        assert rt.fence_holds == {"catchup"}
        await asyncio.sleep(0.05)
        assert len(r.db.records) == 1                              # waits for a pass sent after the interruption
        r.ad.history = history([order("BLIP", OrderStatus.FILLED, t=NOW + 300_000)], since=0, until=NOW)
        await r.snapshot()                                         # reconnected: SNAPSHOTTING committed
        await wait_for(lambda: len(r.db.records) == 2 and r.coord.finished("mock"))
    finally:
        await r.close()
    rec = r.db.records[1][1]
    assert rec.reason == "reconnect" and rec.complete and rec.orders_new == 1
    assert r.ad.called("fetch_history")[-1][0] == NOW - 60_000 - 900_000      # from the unchanged coverage
    assert r.db.orders["BLIP"].status is OrderStatus.FILLED
    (sev, _, body), = r.pages[n_pages:]
    assert sev == "info" and "stream interrupted" in body and "engine was offline" not in body
    assert r.coord.vouches("mock") and not rt.fence_holds


async def test_a_reconnect_catchup_that_finds_nothing_sends_no_page(tmp_path: Path, clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(since=0, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
        n_pages = len(r.pages)
        r.env.item("orders", ev(K.GAP, "orders"))
        await wait_for(lambda: r.coord.busy())
        await r.snapshot()
        await wait_for(lambda: len(r.db.records) == 2 and r.coord.finished("mock"))
    finally:
        await r.close()
    assert r.db.records[1][1].reason == "reconnect" and len(r.pages) == n_pages


async def test_resume_ignores_a_pass_sent_before_the_sleep(tmp_path: Path, clock: ManualClock) -> None:
    """A reconcile pass in flight when the PC went to sleep commits after the wake: it was sent before the forced
    RESYNCING, so it proves nothing about the sleep; the resume catch-up waits for a newer one."""
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(since=0, until=NOW)
    r.coord.begin_startup()
    start(r)
    rt = r.env.orch.runtime("mock")
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
        stale_send_seq = r.env.orch.hlc.next()                     # sent before the sleep
        clock.ms += 3_600_000
        r.coord.resume_after_sleep()
        r.env.orch._pass_committed(rt, stale_send_seq)             # ... and its rows commit after the wake
        assert rt.conn.state.value == "STREAMING"                  # (no driver here: SM4 stays STREAMING)
        await asyncio.sleep(0.05)
        assert len(r.db.records) == 1
        await r.snapshot()
        await wait_for(lambda: len(r.db.records) == 2 and r.coord.finished("mock"))
    finally:
        await r.close()
    assert r.db.records[1][1].reason == "resume_after_sleep"


async def test_a_hung_history_read_is_a_failed_attempt(tmp_path: Path, clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    settings = CatchupSettings(overlap_ms=900_000, first_run_days=7, commit_timeout_s=2.0, max_attempts=2,
                               retry_s=0.01, poll_s=0.005, fetch_timeout_s=0.05)
    r = await rig(tmp_path, clock, db=db, settings=settings)
    hang = asyncio.Event()

    async def stuck(since_ms: int, until_ms: int) -> BrokerHistory:
        await hang.wait()
        raise AssertionError("unreachable")

    r.ad.fetch_history = stuck  # type: ignore[method-assign]
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert not rec.complete and "TimeoutError after 2 attempts" in rec.notes
    assert not r.coord.vouches("mock")


async def test_unmappable_history_rows_never_count_as_an_interruption_even_with_the_driver_running(
        tmp_path: Path, clock: ManualClock) -> None:
    """Skeptic review (reconnect loop): history rows the adapter cannot map used to land on the live-stream
    `unmapped_rows` counter. The SM4 driver's ~1 s `check_unmapped` step took that count while the history read
    was still awaiting, opened the orders gap and bumped `last_interruption_seq` past the catch-up's mark, so a
    `reconnect` catch-up re-read the same row, for ever. They now count on the read's own channel: the catch-up
    is incomplete and retried with back-off, and the stream is never interrupted."""
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.unmapped_rows = new_unmapped_rows()  # type: ignore[attr-defined]
    real = r.ad.fetch_history

    async def skips_a_row(since_ms: int, until_ms: int) -> BrokerHistory:
        note_unmapped(r.ad.unmapped_rows, "mock", "orders", "history row without an id")  # type: ignore[attr-defined]
        await asyncio.sleep(0.03)                                   # more history pages: the driver steps meanwhile
        return await real(since_ms, until_ms)

    async def driver() -> None:                                     # the SM4 STREAMING step's check
        while not r.stop.is_set():
            r.env.orch.check_unmapped("mock")
            await asyncio.sleep(0.001)

    r.ad.fetch_history = skips_a_row  # type: ignore[method-assign]
    r.ad.history = history(since=0, until=NOW)
    r.coord.begin_startup()
    start(r)
    r.tasks.append(asyncio.create_task(driver()))
    rt = r.env.orch.runtime("mock")
    try:
        await r.snapshot()
        interruption = rt.last_interruption_seq
        await wait_for(lambda: r.coord.finished("mock"))
        await asyncio.sleep(0.05)
        assert rt.last_interruption_seq == interruption and not rt.subs["orders"].lost
        assert not r.coord.busy()                                   # no reconnect catch-up queued
    finally:
        await r.close()
    assert [rec.reason for _, rec in r.db.records] == ["startup"]
    rec = r.db.records[0][1]
    assert not rec.complete and "1 rows the adapter could not map during the history read" in rec.notes
    assert not r.coord.vouches("mock")                              # retryable: coverage held back, retried later
    high = [p for p in r.pages if p[0] == "high"]
    assert len(high) == 1 and "retry in 5m 0s" in high[0][2]


async def test_a_live_row_skipped_during_the_catchup_survives_and_stops_coverage(
        tmp_path: Path, clock: ManualClock) -> None:
    """Skeptic review: `catchup_submit` used to put `last_interruption_seq` back after its own `check_unmapped`,
    erasing a live-stream row the adapter skipped while the catch-up ran (an order placed and filled meanwhile).
    The interruption must survive: no vouching, and a `reconnect` catch-up re-reads the history."""
    db = Db(cov=Coverage("mock", NOW - 60_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.unmapped_rows = new_unmapped_rows()  # type: ignore[attr-defined]
    real = r.ad.fetch_history

    async def live_skip_meanwhile(since_ms: int, until_ms: int) -> BrokerHistory:
        async def stream_pump() -> None:                            # the orders stream task, not the catch-up
            note_unmapped(r.ad.unmapped_rows, "mock", "orders", "live order event unmappable")  # type: ignore[attr-defined]

        await asyncio.create_task(stream_pump())
        return await real(since_ms, until_ms)

    r.ad.fetch_history = live_skip_meanwhile  # type: ignore[method-assign]
    r.ad.history = history(since=0, until=NOW)
    r.coord.begin_startup()
    start(r)
    rt = r.env.orch.runtime("mock")
    try:
        await r.snapshot()
        await wait_for(lambda: len(r.db.records) == 1)
        assert not r.coord.vouches("mock")
        await asyncio.sleep(0.05)                                   # the job is reaped, follow-ups run
        assert r.coord.busy() and rt.fence_holds == {"catchup"}    # a reconnect catch-up is queued, fence held
        assert len(r.db.records) == 1                               # waits for a pass sent after the interruption
        r.ad.fetch_history = real  # type: ignore[method-assign]
        await r.snapshot()
        await wait_for(lambda: len(r.db.records) == 2 and r.coord.finished("mock"))
    finally:
        await r.close()
    assert [rec.reason for _, rec in r.db.records] == ["startup", "reconnect"]
    assert r.db.records[0][1].complete                              # the history itself was fine
    assert r.coord.vouches("mock")


async def test_a_history_incomplete_for_a_failed_page_holds_coverage_and_retries(
        tmp_path: Path, clock: ManualClock) -> None:
    """Skeptic review: a failed / timed-out page made the answer complete=False, which was always classed as a
    broker limit: coverage moved past a window nobody read, and the next term reported verified. An incomplete
    answer is retryable unless the adapter says every cause is a broker limit (unknown = retryable)."""
    db = Db(cov=Coverage("mock", NOW - 3_600_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = BrokerHistory("mock", 0, NOW, covered_from_ms=0, orders=(), positions=(), complete=False,
                                 notes=("order list page [1, 2] failed: TimeoutError",))
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
        assert not r.db.records[0][1].complete
        assert not r.coord.vouches("mock") and not r.coord.verified("mock")    # coverage held back
        high = [p for p in r.pages if p[0] == "high"][0]
        assert "adapter_coverage held back" in high[2] and "retry in 5m 0s" in high[2]
        assert "failed: TimeoutError" in high[2]
        r.ad.history = history([order("OFF", OrderStatus.FILLED, t=NOW - 1_000_000)], since=0, until=NOW)
        clock.ms += 300_000
        await wait_for(lambda: len(r.db.records) == 2 and r.coord.finished("mock"))
    finally:
        await r.close()
    rec = r.db.records[1][1]
    assert rec.reason == "retry" and rec.complete and rec.gap_start_ms == NOW - 3_600_000 - 900_000
    assert r.db.orders["OFF"].status is OrderStatus.FILLED
    assert r.coord.vouches("mock") and r.coord.verified("mock")


async def test_a_history_incomplete_only_for_a_broker_limit_is_paged_once_and_moves_on(
        tmp_path: Path, clock: ManualClock) -> None:
    db = Db(cov=Coverage("mock", NOW - 3_600_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = BrokerHistory("mock", 0, NOW, covered_from_ms=0, orders=(), positions=(), complete=False,
                                 notes=("deal list still truncated for [1, 2]",), retryable=False)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
        clock.ms += 7_200_000
        await asyncio.sleep(0.05)
    finally:
        await r.close()
    assert len(r.db.records) == 1 and r.coord.vouches("mock") and not r.coord.verified("mock")
    assert "history incomplete (broker limit)" in r.db.records[0][1].notes


async def test_first_run_reaches_back_to_the_engines_last_known_active_time(tmp_path: Path,
                                                                            clock: ManualClock) -> None:
    """Skeptic review: without a coverage row the window ignored the engine's earlier runs and reached back only
    7 days, so an order placed and finished 9 days ago, after the engine was last active 10 days ago, was missed
    while the catch-up reported complete."""
    db = Db(prior=NOW - 10 * DAY_MS)
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(since=NOW - 10 * DAY_MS, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert r.ad.called("fetch_history") == [(NOW - 10 * DAY_MS, NOW)]
    assert r.db.prior_calls == [("run-1", NOW)]                     # earlier runs only, before this term
    (_, rec), = r.db.records
    assert rec.complete and rec.gap_start_ms == NOW - 10 * DAY_MS


async def test_a_first_run_window_that_must_be_cut_is_incomplete_and_paged(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    db = Db(prior=NOW - 400 * DAY_MS)
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history(since=NOW - 365 * DAY_MS, until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert r.ad.called("fetch_history") == [(NOW - 365 * DAY_MS, NOW)]
    (_, rec), = r.db.records
    assert not rec.complete and "cut at 365 days" in rec.notes
    high = [p for p in r.pages if p[0] == "high"]
    assert len(high) == 1 and "uncovered window 2022-10-10T22:13:20Z .. 2022-11-14T22:13:20Z" in high[0][2]
    assert r.coord.vouches("mock") and not r.coord.verified("mock")    # a fixed limit: paged once, no retry


async def test_coverage_ahead_of_the_wall_clock_never_gives_an_empty_window(tmp_path: Path,
                                                                           clock: ManualClock) -> None:
    """Skeptic review: coverage written while the PC clock ran fast made min(now, cov - overlap) = now, an empty
    window recorded as complete and verified. Future coverage proves nothing about when the engine stopped: the
    window is read as without coverage (the first-run window, so an order finished while the engine was off is
    still ingested), recorded incomplete and paged."""
    db = Db(cov=Coverage("mock", NOW + 7_200_000, None))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history([order("OFF", OrderStatus.FILLED, t=NOW - 3 * 3_600_000)], since=NOW - 7 * DAY_MS,
                           until=NOW)
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    assert r.ad.called("fetch_history") == [(NOW - 7 * DAY_MS, NOW)]
    assert r.db.orders["OFF"].status is OrderStatus.FILLED          # the offline span was read
    (_, rec), = r.db.records
    assert not rec.complete and rec.gap_start_ms == NOW - 7 * DAY_MS and "clock moved back" in rec.notes
    clock_pages = [p for p in r.pages if p[1] == "clock moved back: mock"]
    assert len(clock_pages) == 1 and clock_pages[0][0] == "high" and "clock_moved_back" in clock_pages[0][2]
    assert not r.coord.verified("mock")


async def test_a_first_run_catchup_that_gives_up_pages_the_whole_wanted_window(tmp_path: Path,
                                                                            clock: ManualClock) -> None:
    """Second skeptic: every read failed on a first run whose window reaches back to the engine's last known active
    time (10 days). The give-up page named only the 7-day floor, so the operator checking the broker history for
    the uncovered window would skip days 7-10."""
    db = Db(prior=NOW - 10 * DAY_MS)
    r = await rig(tmp_path, clock, db=db)

    async def broken(since_ms: int, until_ms: int) -> BrokerHistory:
        raise BrokerError("history endpoint down")

    r.ad.fetch_history = broken  # type: ignore[method-assign]
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_, rec), = r.db.records
    assert not rec.complete and rec.gap_start_ms == NOW - 10 * DAY_MS
    high = [b for s, _, b in r.pages if s == "high"]
    assert high and iso_utc(NOW - 10 * DAY_MS) in high[0]
    assert r.coord.results[-1].retryable and not r.coord.vouches("mock")


async def test_a_partial_cash_flow_ledger_alone_leaves_the_order_catchup_complete(
        tmp_path: Path, clock: ManualClock) -> None:
    """Live finding (2026-11-14): StoneX closing trades often carry no RealisedPnl, so the cash-flow ledger is partial
    on every StoneX catch-up. That must not mark the ORDER catch-up incomplete (no order is missing): `complete`
    stays true and coverage moves on; the partial ledger is still noted (and pages on its own as
    'cash-flow ledger incomplete', keeping P&L unverified)."""
    from src.adapters.base import CashFlowBatch
    db = Db(cov=Coverage("mock", NOW - 3_600_000, "run-0"))
    r = await rig(tmp_path, clock, db=db)
    r.ad.history = history([order("N1", OrderStatus.CANCELLED, t=NOW - 1_700_000)], [],
                           since=NOW - 4_500_000, until=NOW)
    r.ad.flow_batch = CashFlowBatch([], partial_reasons=("closing trade 1 without RealisedPnl",))
    r.coord.begin_startup()
    start(r)
    try:
        await r.snapshot()
        await wait_for(lambda: r.coord.finished("mock"))
    finally:
        await r.close()
    (_epoch, rec), = r.db.records
    assert rec.complete and "cash-flow ledger partial" in rec.notes
    assert all(sev != "high" for sev, _t, _b in r.pages)
