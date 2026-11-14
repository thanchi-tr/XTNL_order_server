"""4.5 / 4.8 stores: rulesets reader + audit, live account sampler, data lifecycle, sod verification flags."""
from __future__ import annotations

import gzip
import hashlib
import os
import re
import threading
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from src.db import MIGRATIONS_DIR
from src.db import account_sampler as AS
from src.db import lifecycle as LC
from src.db import ruleset_store as RS
from src.db import snapshot_reader as S
from src.db.epoch import EngineDb
from src.db.fence import FENCE, EpochMismatch
from src.dto import AccountLive
from src.oversee.config import OverseeConfig
from src.oversee.rules import EvalContext, MaxDailyLossRule
from tests.db.conftest import EPOCH, FakeConn, FakeSource
from tests.db.test_snapshot_reader import NOW, ROLL, script_pg

D = Decimal


def placeholders(sql: str) -> set[int]:
    return {int(n) for n in re.findall(r"\$(\d+)", sql)}


@pytest.mark.parametrize("sql", [RS.SQL_AUDIT, AS.SQL_LIVE_SAMPLE, LC.SQL_ENSURE_PARTITION, LC.SQL_DETACH_ARCHIVED,
                                 LC.SQL_OUTBOX_RETENTION, LC.SQL_ARCHIVE_RECORD, LC.SQL_DLQ_HISTORY_MOVE])
def test_every_write_is_fenced_in_the_statement(sql: str) -> None:
    assert FENCE in sql
    ph = placeholders(sql)
    assert ph == set(range(1, max(ph) + 1))


def test_migration_0008_objects() -> None:
    sql = (MIGRATIONS_DIR / "0008_ruleset_audit_lifecycle.sql").read_text(encoding="utf-8")
    for obj in ("CREATE TABLE IF NOT EXISTS ruleset_audit", "CREATE TABLE IF NOT EXISTS partition_archive",
                "CREATE OR REPLACE FUNCTION xtnl_detach_archived_partition"):
        assert obj in sql
    assert "DROP TABLE" not in sql.upper().replace("DROPPING", "")    # detach only; drop is an operator action
    assert "count_mismatch" in sql and "DETACH PARTITION" in sql


# ============================================================================================ rulesets
async def test_ruleset_store_reads_and_fenced_audit(conn: FakeConn, db: EngineDb) -> None:
    store = RS.PgRulesetStore(db, "n1")
    assert await store.active_version() is None and await store.active() is None
    conn.returns(RS.SQL_ACTIVE_VERSION, (4,))
    conn.returns(RS.SQL_ACTIVE, (4, {"marks": {"max_clean_spread_pips": "3"}}, "ops", "tighter", 99))
    assert await store.active_version() == 4
    row = await store.active()
    assert row is not None and (row.version, row.created_by, row.reason) == (4, "ops", "tighter")
    conn.returns_all(RS.SQL_EARLIER, [(3, {}, "ops", "r", 1), (1, {}, "ops", "r", 0)])
    assert [r.version for r in await store.earlier(4, 20)] == [3, 1]
    assert conn.args_of(RS.SQL_EARLIER) == [(4, 20)]
    await store.audit(4, "installed", "by ops")
    assert conn.args_of(RS.SQL_AUDIT) == [(7, 4, "installed", "by ops", "n1")]
    conn.stored_epoch = 8
    with pytest.raises(EpochMismatch):
        await store.audit(5, "rejected", "x")


# ============================================================================================ sampler
def live(acct: str, ts: int, margin: str = "100", sampled: bool = False) -> AccountLive:
    return AccountLive(account_id=acct, adapter_id="stonex", currency="USD", equity=D("1000"),
                       margin_used=D(margin), free_margin=D("900"), unrealised_pnl=D("5"), timestamp_ms=ts,
                       sampled=sampled)


class Clock:
    def __init__(self) -> None:
        self.t = 1_000_000

    def __call__(self) -> int:
        return self.t


class FakeSampleStore:
    def __init__(self) -> None:
        self.batches: list[list[AS.LiveSample]] = []
        self.error: Exception | None = None

    async def write_samples(self, samples: Sequence[AS.LiveSample]) -> None:
        if self.error is not None:
            raise self.error
        self.batches.append(list(samples))


async def test_sampler_is_rate_limited_per_account_and_keeps_the_newest() -> None:
    clock, store = Clock(), FakeSampleStore()
    seq = iter(range(100, 200))
    sampler = AS.AccountLiveSampler(store, lambda: next(seq), clock_ms=clock, interval_ms=60_000)
    assert not sampler.offer(live("A", 5, sampled=True))        # a seeded value is never re-sampled
    assert sampler.offer(live("A", 10)) and sampler.offer(live("A", 20, margin="150"))
    assert not sampler.offer(live("A", 15))                     # older than what we hold
    sampler.offer(live("B", 30))
    assert await sampler.flush() == 2
    (batch,) = store.batches
    assert [(s.live.account_id, s.live.timestamp_ms, s.live.margin_used, s.seq_id) for s in batch] == [
        ("A", 20, D("150"), 100), ("B", 30, D("100"), 101)]
    sampler.offer(live("A", 40))
    clock.t += 59_999
    assert await sampler.flush() == 0                           # within the interval: no write
    clock.t += 1
    assert await sampler.flush() == 1 and store.batches[-1][0].live.timestamp_ms == 40
    clock.t += 60_000
    assert await sampler.flush() == 0                           # nothing newer than the last sample


async def test_sampler_retries_after_a_failure_and_propagates_a_lost_fence() -> None:
    clock, store = Clock(), FakeSampleStore()
    sampler = AS.AccountLiveSampler(store, lambda: 1, clock_ms=clock)
    sampler.offer(live("A", 10))
    store.error = ConnectionError("down")
    assert await sampler.flush() == 0
    store.error = None
    assert await sampler.flush() == 1
    sampler.offer(live("A", 99))
    clock.t += 60_000
    store.error = EpochMismatch("fenced")
    with pytest.raises(EpochMismatch):
        await sampler.flush()


async def test_pg_sample_store_writes_live_ts_ms(conn: FakeConn, db: EngineDb) -> None:
    await AS.PgLiveSampleStore(db).write_samples([AS.LiveSample(live("A", 77), 555)])
    (args,) = conn.args_of(AS.SQL_LIVE_SAMPLE)
    assert args == (7, "stonex", "A", 555, "USD", D("1000"), D("100"), D("900"), D("5"), 77)
    assert "live_ts_ms" in AS.SQL_LIVE_SAMPLE and "sampled) SELECT" in AS.SQL_LIVE_SAMPLE


# ============================================================================================ lifecycle
def ms(y: int, m: int, d: int) -> int:
    return int(datetime(y, m, d, tzinfo=UTC).timestamp() * 1000)


def test_month_helpers() -> None:
    assert LC.months_to_ensure(ms(2026, 11, 13), 2) == [date(2026, 11, 1), date(2026, 12, 1), date(2027, 1, 1)]
    assert LC.partition_month("event_log_p202608", "event_log") == date(2026, 8, 1)
    assert LC.partition_month("event_log_default", "event_log") is None
    assert LC.partition_month("event_log_p202613", "event_log") is None
    assert LC.partition_month("oversee_events_p202608", "event_log") is None
    assert LC.past_hot_window(date(2026, 7, 1), ms(2026, 10, 30), 90)      # July ended 2026-08-01
    assert not LC.past_hot_window(date(2026, 8, 1), ms(2026, 10, 30), 90)


async def test_run_once_creates_ahead_detaches_archived_and_trims_the_outbox(conn: FakeConn, db: EngineDb) -> None:
    now = ms(2026, 11, 13)
    conn.returns_all(LC.SQL_LIST_PARTITIONS, [
        ("event_log", "event_log_default"), ("event_log", "event_log_p202607"),
        ("account_snapshots", "account_snapshots_p202607"), ("notification_outbox", "notification_outbox_p202606"),
        ("event_log", "event_log_p202611")])
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 15)
    conn.returns(LC.SQL_DETACH_ARCHIVED, ("detached",), ("not_archived",), ("count_mismatch",))
    conn.status_for(LC.SQL_OUTBOX_RETENTION, "DELETE 3", "DELETE 1")
    job = LC.PgPartitionMaintenance(db, LC.LifecycleConfig(outbox_batch=3))
    rep = await job.run_once(now)
    ensured = conn.args_of(LC.SQL_ENSURE_PARTITION)
    assert len(ensured) == 3 * 5 and (7, "oversee_events", date(2027, 1, 1), 20) in ensured
    assert (7, "events_dlq_history", date(2026, 11, 1), 20) in ensured
    assert (7, "notification_outbox", date(2026, 11, 1), 0) in ensured
    (listed,) = conn.args_of(LC.SQL_LIST_PARTITIONS)
    assert "oversee_events" not in listed[0]                     # audit table: never detached
    assert [a[1:3] for a in conn.args_of(LC.SQL_DETACH_ARCHIVED)] == [
        ("event_log", "event_log_p202607"), ("account_snapshots", "account_snapshots_p202607"),
        ("notification_outbox", "notification_outbox_p202606")]     # the hot November one is untouched
    assert rep.detached == ["event_log_p202607"] and rep.awaiting_archive == ["account_snapshots_p202607"]
    assert rep.count_mismatch == ["notification_outbox_p202606"]
    assert rep.outbox_deleted == 4 and conn.args_of(LC.SQL_OUTBOX_RETENTION)[0][1:] == (now - 90 * LC.DAY_MS, 3)
    assert all(s == LC.SQL_LOCK_TIMEOUT for s in conn.sql if "lock_timeout" in s)


async def test_a_failing_statement_is_reported_and_a_lost_fence_propagates(conn: FakeConn, db: EngineDb) -> None:
    conn.fail(LC.SQL_ENSURE_PARTITION, RuntimeError("rows in default partition"))
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 14)
    rep = await LC.PgPartitionMaintenance(db).run_once(ms(2026, 11, 13))
    assert len(rep.ensured) == 14 and rep.errors and "rows in default partition" in rep.errors[0]
    conn.stored_epoch = 99
    with pytest.raises(EpochMismatch):
        await LC.PgPartitionMaintenance(db).run_once(ms(2026, 11, 13))


def test_lifecycle_runner_schedules_and_pages_each_condition_once() -> None:
    runner = LC.LifecycleRunner(every_ms=1000)
    assert runner.due(0)
    runner.ran(0)
    assert not runner.due(999) and runner.due(1000)
    rep = LC.LifecycleReport(awaiting_archive=["event_log_p202607"], errors=["x"])
    assert [p[1] for p in runner.pages(rep)] == ["lifecycle: maintenance errors",
                                                 "lifecycle: partitions past 90 days await export"]
    assert runner.pages(rep) == []
    assert runner.pages(LC.LifecycleReport()) == []
    assert len(runner.pages(rep)) == 2                           # cleared, then back: pages again


# ============================================================================================ export (4.9)
EXPORT_LISTING = [("event_log", "event_log_p202607"), ("event_log", "event_log_p202611"),
                  ("account_snapshots", "account_snapshots_p202607")]


def exporter(db: EngineDb, tmp_path: Path, batch: int = 2) -> LC.PartitionExporter:
    return LC.PartitionExporter(db, LC.ExportConfig(tmp_path / "archive", verified_by="engine:n1", batch_rows=batch))


async def test_export_writes_verified_gzip_jsonl_records_it_then_detaches(conn: FakeConn, db: EngineDb,
                                                                         tmp_path: Path) -> None:
    now = ms(2026, 11, 13)
    conn.returns_all(LC.SQL_LIST_PARTITIONS, EXPORT_LISTING, EXPORT_LISTING)
    conn.returns_all(LC.SQL_ARCHIVED_NAMES, [("account_snapshots_p202607",)])   # exported earlier
    page = LC.SQL_EXPORT_FETCH.format(n=2)
    declare = LC.SQL_EXPORT_DECLARE.format(name="event_log_p202607")
    conn.returns_all(page, [('{"seq_id": 1}',), ('{"seq_id": 2}',)], [('{"seq_id": 3}',)])
    conn.status_for(LC.SQL_ARCHIVE_RECORD, "INSERT 0 1")
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 15)
    conn.returns(LC.SQL_DETACH_ARCHIVED, ("detached",), ("detached",))
    job = LC.PgPartitionMaintenance(db, exporter=exporter(db, tmp_path))
    rep = await job.run_once(now)

    assert conn.args_of(LC.SQL_ARCHIVED_NAMES) == [(["event_log_p202607", "account_snapshots_p202607"],)]
    assert conn.args_of(page) == [(), (), ()]                         # cursor pages until an empty FETCH
    f = tmp_path / "archive" / "event_log" / "event_log_p202607.jsonl.gz"
    data = f.read_bytes()
    assert gzip.decompress(data) == b'{"seq_id": 1}\n{"seq_id": 2}\n{"seq_id": 3}\n'
    digest = hashlib.sha256(data).hexdigest()
    assert (f.parent / (f.name + ".sha256")).read_text(encoding="ascii") == f"{digest}  {f.name}\n"
    assert not list(f.parent.glob("*.part"))
    (rec,) = conn.args_of(LC.SQL_ARCHIVE_RECORD)
    assert rec == (7, "event_log_p202607", "event_log", 3, f"sha256:{digest}", f.resolve().as_uri(), now,
                   "engine:n1")
    assert rep.exported == ["event_log_p202607"] and rep.errors == []
    assert rep.detached == ["event_log_p202607", "account_snapshots_p202607"]
    sql = conn.sql
    assert sql.index(LC.SQL_ARCHIVE_RECORD) < sql.index(LC.SQL_DETACH_ARCHIVED)   # export, then detach
    i = sql.index(declare)
    assert sql[i - 2:i] == ["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", LC.SQL_EXPORT_SYNC_SCANS_OFF]


async def test_export_is_deterministic_and_skips_hot_and_audit_partitions(conn: FakeConn, db: EngineDb,
                                                                        tmp_path: Path) -> None:
    job = exporter(db, tmp_path, batch=10)
    listing = EXPORT_LISTING + [("oversee_events", "oversee_events_p202601"), ("event_log", "event_log_default")]
    conn.returns_all(LC.SQL_ARCHIVED_NAMES, [])
    todo = await job.candidates(ms(2026, 11, 13), listing)
    assert todo == [("event_log", "event_log_p202607"), ("account_snapshots", "account_snapshots_p202607")]
    page = LC.SQL_EXPORT_FETCH.format(n=10)
    rows = [('{"a": "x"}',)]
    conn.returns_all(page, rows, [], rows, [])
    first = await job.export_partition("event_log", "event_log_p202607")
    again = await job.export_partition("event_log", "event_log_p202607")
    assert first.sha256 == again.sha256 and first.rows == 1             # same rows -> same bytes
    with pytest.raises(ValueError):
        await job.export_partition("oversee_events", "oversee_events_p202601")   # audit table: never exported


async def test_failed_export_records_nothing_so_nothing_is_detached(conn: FakeConn, db: EngineDb,
                                                                   tmp_path: Path) -> None:
    conn.returns_all(LC.SQL_LIST_PARTITIONS, EXPORT_LISTING, EXPORT_LISTING)
    conn.returns_all(LC.SQL_ARCHIVED_NAMES, [("account_snapshots_p202607",)])
    page = LC.SQL_EXPORT_FETCH.format(n=2)
    conn.fail(page, RuntimeError("statement timeout"))
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 15)
    conn.returns(LC.SQL_DETACH_ARCHIVED, ("not_archived",), ("detached",))
    rep = await LC.PgPartitionMaintenance(db, exporter=exporter(db, tmp_path)).run_once(ms(2026, 11, 13))
    assert conn.args_of(LC.SQL_ARCHIVE_RECORD) == []
    assert rep.exported == [] and any("export event_log_p202607" in e for e in rep.errors)
    assert rep.awaiting_archive == ["event_log_p202607"]
    d = tmp_path / "archive" / "event_log"
    assert not list(d.glob("*")) if d.exists() else True                 # no partial file left behind


async def test_record_conflict_is_false_and_a_lost_fence_raises(conn: FakeConn, db: EngineDb, tmp_path: Path) -> None:
    job = exporter(db, tmp_path)
    f = LC.ArchiveFile(tmp_path / "x.jsonl.gz", 3, "ab")
    conn.status_for(LC.SQL_ARCHIVE_RECORD, "INSERT 0 0")
    assert await job.record("event_log", "event_log_p202607", f, 1) is False   # already recorded
    conn.stored_epoch = 99
    with pytest.raises(EpochMismatch):
        await job.record("event_log", "event_log_p202607", f, 1)


def test_verify_archive_rejects_a_short_or_corrupt_file(tmp_path: Path) -> None:
    good = tmp_path / "a.jsonl.gz"
    good.write_bytes(gzip.compress(b'{"a": 1}\n{"a": 2}\n'))
    assert LC.verify_archive(good, 2) == hashlib.sha256(good.read_bytes()).hexdigest()
    with pytest.raises(LC.ArchiveVerifyError):
        LC.verify_archive(good, 3)
    bad = tmp_path / "b.jsonl.gz"
    bad.write_bytes(good.read_bytes()[:-6])                                # truncated: CRC / EOF error
    with pytest.raises(LC.ArchiveVerifyError):
        LC.verify_archive(bad, 2)


def test_exporter_from_env(db: EngineDb, tmp_path: Path) -> None:
    assert LC.PartitionExporter.from_env(db, "node-a", {}) is None
    job = LC.PartitionExporter.from_env(db, "node-a", {LC.ARCHIVE_DIR_ENV: str(tmp_path)})
    assert job is not None and job.cfg.archive_dir == tmp_path and job.cfg.verified_by == "engine:node-a"


# ============================================================================================ sod flags
@pytest.mark.parametrize("flags,transfer,pnl", [(None, True, True), ((True, False), False, True),
                                                ((False, True), True, False)])
async def test_sod_verification_flags_reach_the_snapshot(conn: FakeConn, flags: Any, transfer: bool,
                                                         pnl: bool) -> None:
    script_pg(conn)
    if flags is not None:
        conn.returns(S.SQL_SOD_FLAGS, flags)
    reader = S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None)
    snap = await reader.read_account("ACC", NOW)
    assert (snap.transfer_source, snap.pnl_source) == (transfer, pnl)
    (args,) = conn.args_of(S.SQL_SOD_FLAGS)
    assert args[:2] == ("stonex", "ACC")


async def test_transfers_unverified_day_flags_max_daily_loss(conn: FakeConn) -> None:
    script_pg(conn)
    conn.returns(S.SQL_SOD_FLAGS, (True, False))
    snap = await S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None).read_account("ACC", NOW)
    cfg = OverseeConfig()
    rule = MaxDailyLossRule(cfg.max_daily_loss, cfg.sweep.release_margin, cfg.sweep.skew_ms)
    ctx = EvalContext(snap, cfg, NOW)
    flags = rule._flags(ctx, ctx.equity())
    assert "transfers_unverified" in flags
    assert ROLL < NOW


# ============================================================================================ export, live PostgreSQL
_PG_DSN = os.environ.get("ORDER_SERVER_TEST_DSN")


def test_export_sql_binds_no_tid_parameter() -> None:
    """asyncpg encodes a `$n::tid` parameter with its binary tid codec, which rejects text such as '(0,0)';
    the export reads through a parameterless cursor instead (4.9 skeptic fix)."""
    for sql in (LC.SQL_EXPORT_DECLARE, LC.SQL_EXPORT_FETCH, LC.SQL_EXPORT_SYNC_SCANS_OFF):
        assert "tid" not in sql.replace("xtnl_export", "") and "$" not in sql
    assert "ORDER BY" not in LC.SQL_EXPORT_DECLARE                     # one sequential scan, no per-page sort


def test_abort_waits_for_an_in_flight_write(tmp_path: Path) -> None:
    """A cancelled export leaves its `to_thread(write)` running; abort must not close the GzipFile under it."""
    w = LC._GzipJsonlWriter(tmp_path / "p" / "x.jsonl.gz")
    w.open()
    w.write(['{"a": 1}'])
    done = threading.Event()
    with w._lock:                                                       # a write is in progress
        t = threading.Thread(target=lambda: (w.abort(), done.set()))
        t.start()
        assert not done.wait(0.2) and w.tmp.exists()                    # abort waits for it
    t.join(5)
    assert done.is_set() and not w.tmp.exists()
    with pytest.raises(RuntimeError):
        w.write(['{"a": 2}'])                                           # nothing writes after an abort
    with pytest.raises(RuntimeError):
        w.close()


@pytest.mark.pg
@pytest.mark.skipif(not _PG_DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_export_then_detach_on_a_real_postgresql(tmp_path: Path) -> None:
    """The exporter through asyncpg (binary protocol): export, verify, record, then detach. DISPOSABLE database."""
    from src.db.epoch import Epoch, bump_epoch
    from src.db.migrate import migrate
    from src.db.pg import create_asyncpg_source

    assert _PG_DSN is not None
    src = await create_asyncpg_source(_PG_DSN, application_name="xtnl:31:it")
    epoch = Epoch("it-lifecycle", 31)
    part = "event_log_p202606"
    try:
        async with src.acquire() as c:
            await migrate(c)
            await c.execute(f'DROP TABLE IF EXISTS "{part}"')
            await c.execute("DELETE FROM partition_archive WHERE partition_name = $1", part)
            await c.execute("TRUNCATE engine_epoch")
            await bump_epoch(c, epoch, "it-node", terminate_older=False)
            await c.execute("SELECT xtnl_ensure_month_partition('event_log', '2026-06-01'::date, 20)")
            await c.execute(
                "INSERT INTO event_log (seq_id, entity_type, entity_key, adapter_id, action, payload, epoch_revision) "
                "SELECT $1::bigint + g, 'ORDER', 'k' || g, 'it', 'APPLIED', "
                "jsonb_build_object('n', g, 's', E'line\nbreak'), 31 FROM generate_series(1, 2501) g",
                ms(2026, 6, 10) << 20)
        db = EngineDb(src, lambda: epoch)
        exporter = LC.PartitionExporter(db, LC.ExportConfig(tmp_path / "archive", verified_by="engine:it",
                                                            batch_rows=1000))
        listed = [("event_log", part)]
        f = await exporter.export_partition("event_log", part)
        assert f.rows == 2501 and LC.verify_archive(f.path, 2501) == f.sha256
        again = await exporter.export_partition("event_log", part)
        assert again.sha256 == f.sha256                                  # same rows, same bytes
        assert await exporter.candidates(ms(2026, 11, 13), listed) == listed
        assert await exporter.record("event_log", part, f, ms(2026, 11, 13)) is True
        assert await exporter.candidates(ms(2026, 11, 13), listed) == []
        rep = await LC.PgPartitionMaintenance(db).run_once(ms(2026, 11, 13))
        assert part in rep.detached, rep.errors
        async with src.acquire() as c:
            assert await c.fetchrow("SELECT row_count, checksum, archive_uri, detached_epoch FROM partition_archive "
                                    "WHERE partition_name = $1", part) == (2501, f.checksum, f.path.as_uri(), 31)
    finally:
        await src.close()


# ============================================================================================ DLQ history (5.5)
async def test_dlq_history_creates_the_months_it_needs_then_moves_resolved_rows_in_batches(
        conn: FakeConn, db: EngineDb) -> None:
    """operations.md s11: RESOLVED events_dlq rows past the keep window move to events_dlq_history (partitioned
    by seq_id range, exported / detached like event_log); their month partitions are created first."""
    now = ms(2026, 11, 13)
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 16)
    conn.returns_all(LC.SQL_DLQ_HISTORY_MONTHS, [(date(2026, 8, 1),)])
    conn.status_for(LC.SQL_DLQ_HISTORY_MOVE, "INSERT 0 2", "INSERT 0 1")
    job = LC.PgPartitionMaintenance(db, LC.LifecycleConfig(dlq_batch=2))
    rep = await job.run_once(now)
    cutoff = now - 7 * LC.DAY_MS
    assert conn.args_of(LC.SQL_DLQ_HISTORY_MONTHS) == [(cutoff,)]
    ensured = conn.args_of(LC.SQL_ENSURE_PARTITION)
    assert (7, "events_dlq_history", date(2026, 8, 1), 20) in ensured          # the old month, before the move
    first_move = conn.sql.index(LC.SQL_DLQ_HISTORY_MOVE)
    assert max(i for i, s in enumerate(conn.sql) if s == LC.SQL_ENSURE_PARTITION) < first_move
    assert conn.args_of(LC.SQL_DLQ_HISTORY_MOVE) == [(7, cutoff, 2), (7, cutoff, 2)]
    assert rep.dlq_moved == 3 and "events_dlq_history_p202608" in rep.ensured


def test_dlq_history_sql_shape() -> None:
    sql = LC.SQL_DLQ_HISTORY_MOVE
    assert sql.startswith("WITH moved AS (DELETE FROM events_dlq d") and "INSERT INTO events_dlq_history" in sql
    assert "o.status = 'RESOLVED'" in sql and "FOR UPDATE SKIP LOCKED" in sql
    assert "NOT EXISTS (SELECT 1 FROM events_dlq c WHERE c.origin_dlq_id = o.dlq_id)" in sql   # self-FK safe
    assert "(m.updated_at_ms << 20) | (m.dlq_id & 1048575)" in sql                         # HLC-shaped seq_id
    assert LC.detachable("events_dlq_history") and LC.PARTITIONED["events_dlq_history"] == 20
    assert LC.partition_month("events_dlq_history_p202608", "events_dlq_history") == date(2026, 8, 1)


async def test_dlq_history_stops_when_its_month_cannot_be_created(conn: FakeConn, db: EngineDb) -> None:
    """A month partition that cannot be created (rows already in the default partition) stops the move, so no
    further row lands in events_dlq_history_default."""
    conn.returns_all(LC.SQL_DLQ_HISTORY_MONTHS, [(date(2026, 8, 1),)])
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 15)
    job = LC.PgPartitionMaintenance(db)
    real = job._ensure

    async def ensure(parent: str, month: date, shift: int, rep: LC.LifecycleReport) -> bool:
        if parent == "events_dlq_history" and month == date(2026, 8, 1):
            rep.errors.append("create events_dlq_history_p202608: rows in default partition")
            return False
        return await real(parent, month, shift, rep)

    job._ensure = ensure  # type: ignore[method-assign]
    rep = await job.run_once(ms(2026, 11, 13))
    assert rep.errors and rep.dlq_moved == 0 and LC.SQL_DLQ_HISTORY_MOVE not in conn.sql


def test_migration_0009_objects() -> None:
    sql = (MIGRATIONS_DIR / "0009_operator_requests_dlq_history.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS events_dlq_history" in sql and "PARTITION BY RANGE (seq_id)" in sql
    assert "PRIMARY KEY (seq_id, dlq_id)" in sql and "events_dlq_history_hash_idx" in sql
    assert "xtnl_ensure_month_partition('events_dlq_history'" in sql
    assert "CREATE TABLE IF NOT EXISTS dlq_lineage_bumps" in sql and "payload_hash    char(64) PRIMARY KEY" in sql
    assert "CHECK (kind IN ('reset_task', 'release_account', 'refresh_cash_flows'))" in sql
    assert "DROP CONSTRAINT IF EXISTS operator_requests_kind_check" in sql
    assert "ADD COLUMN IF NOT EXISTS payload jsonb NOT NULL DEFAULT '{}'::jsonb" in sql
    assert "COMMENT ON TABLE partition_archive" in sql and "Parquet" not in sql.split("COMMENT ON TABLE partition_archive")[1]


def test_dlq_history_move_keeps_last_error() -> None:
    """The history move must not drop events_dlq.last_error (0003): it moves with the row (0010 column)."""
    sql = LC.SQL_DLQ_HISTORY_MOVE
    assert "d.last_error)" in sql and "resolved_at_ms, last_error)" in sql and "m.last_error FROM moved m" in sql


async def test_dlq_lineage_bumps_of_moved_origins_are_pruned_after_the_move(conn: FakeConn, db: EngineDb) -> None:
    """dlq_lineage_bumps (0009) gets one row per deterministic re-park; once the origin has left events_dlq and
    the keep window passed, the dedupe row is deleted (fenced, batched), so the table stays bounded."""
    now = ms(2026, 11, 13)
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 15)
    conn.status_for(LC.SQL_DLQ_HISTORY_MOVE, "INSERT 0 0")
    conn.status_for(LC.SQL_DLQ_BUMPS_PRUNE, "DELETE 2", "DELETE 1")
    rep = await LC.PgPartitionMaintenance(db, LC.LifecycleConfig(dlq_batch=2)).run_once(now)
    cutoff = now - 7 * LC.DAY_MS
    assert conn.args_of(LC.SQL_DLQ_BUMPS_PRUNE) == [(7, cutoff, 2), (7, cutoff, 2)]
    assert conn.sql.index(LC.SQL_DLQ_HISTORY_MOVE) < conn.sql.index(LC.SQL_DLQ_BUMPS_PRUNE)
    assert rep.dlq_bumps_pruned == 3
    sql = LC.SQL_DLQ_BUMPS_PRUNE
    assert sql.startswith("DELETE FROM dlq_lineage_bumps") and FENCE in sql and "FOR UPDATE SKIP LOCKED" in sql
    assert "NOT EXISTS (SELECT 1 FROM events_dlq d WHERE d.dlq_id = b.origin_dlq_id)" in sql
    assert "b.bumped_at_ms < $2::bigint" in sql
