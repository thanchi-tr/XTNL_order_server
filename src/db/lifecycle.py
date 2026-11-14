"""Data-lifecycle housekeeping: monthly partitions and outbox retention (IMPLEMENTATION_PLAN 4.8;
operations.md §11, tier-persistence.md §4).

One `run_once()` (the runtime calls it from a supervised task, every few hours):

1. **Create partitions ahead**: the current month and the next `months_ahead` months of `event_log`,
   `account_snapshots`, `oversee_events`, `events_dlq_history` (HLC ranges, shift 20) and
   `notification_outbox` (ms ranges), through `xtnl_ensure_month_partition` (0001), so rows never land in
   the default partition.
1b. **DLQ history** (operations.md §11, "DLQ history by seq_id range"): `RESOLVED` `events_dlq` rows whose
   resolution is older than `dlq_keep_days` and that no active row names as its lineage origin are moved,
   in batches, to `events_dlq_history` (0009) in one fenced statement (`DELETE ... RETURNING` feeding the
   `INSERT`), with `seq_id = resolved_at_ms << 20 | (dlq_id & 0xFFFFF)`. The month partitions those rows
   need are created first, so nothing lands in the default partition. From there the history partitions
   are exported, verified and detached like `event_log` (steps 2-3). `QUARANTINED` rows stay (an operator
   may release them). `last_error` moves with the row.
1c. **Lineage-bump dedupe rows**: `dlq_lineage_bumps` rows older than the keep window whose origin is no
   longer in `events_dlq` (moved in 1b) are deleted in batches, so that table stays bounded.
2. **Export** (only with a `PartitionExporter`, i.e. `XTNL_ARCHIVE_DIR` set): every attached partition past
   the hot window without a `partition_archive` row is written, from one READ ONLY REPEATABLE READ
   snapshot through a cursor, as gzip JSONL (`to_jsonb` of each row, physical order) to
   `$XTNL_ARCHIVE_DIR/<parent>/<partition>.jsonl.gz` plus a `.sha256` sidecar. The file is re-read and
   verified (gzip CRC, line count = rows read, sha256 of the file) before the fenced `partition_archive`
   insert records it. A failed export leaves no row, so nothing is detached.
3. **Retention** (90 days hot): a monthly partition whose whole month ended more than `hot_days` ago is
   detached only after it has been exported and verified (a `partition_archive` row whose
   `row_count` still matches; `xtnl_detach_archived_partition`, 0008). Without that row it is reported
   as awaiting archive (the runtime pages). Audit tables are never detached: `oversee_events` is only
   created ahead. Dropping a detached table is an operator action.
4. **Outbox retention**: `outbox_events` rows older than `hot_days` are deleted in batches (nothing relays
   them in Phase 1, so "deleted after relay" becomes "deleted after the hot window").

Every statement is fenced in the statement itself (`SELECT f(...) WHERE <fence>`, `DELETE ... AND <fence>`)
in its own READ COMMITTED transaction with `lock_timeout` 2 s. A failing statement is reported and the
rest still run; a lost fence (`EpochMismatch`) propagates (the runtime hard-exits).
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import logging
import os
import re
import threading
import zlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import IO, Final, Protocol

from src.db.epoch import EngineDb
from src.db.fence import FENCE, EpochMismatch

log = logging.getLogger(__name__)

#: parent table -> range-key shift (20: HLC seq_id / sweep_id; 0: plain ms).
PARTITIONED: Final[Mapping[str, int]] = MappingProxyType({
    "event_log": 20, "account_snapshots": 20, "oversee_events": 20, "notification_outbox": 0,
    "events_dlq_history": 20})
#: Audit tables kept per compliance (operations.md §11): created ahead, never detached by the engine.
AUDIT_TABLES: Final = frozenset({"oversee_events"})
_PART_NAME: Final = re.compile(r"^(?P<parent>[a-z_]+)_p(?P<y>\d{4})(?P<m>\d{2})$")

SQL_LOCK_TIMEOUT: Final = "SET LOCAL lock_timeout = '2s'"
SQL_ENSURE_PARTITION: Final = f"SELECT xtnl_ensure_month_partition($2::text, $3::date, $4::int) WHERE {FENCE}"
SQL_LIST_PARTITIONS: Final = (
    "SELECT p.relname, c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
    "JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = ANY($1::text[]) ORDER BY 1, 2"
)
SQL_DETACH_ARCHIVED: Final = (
    f"SELECT xtnl_detach_archived_partition($2::text, $3::text, $1::bigint) WHERE {FENCE}"
)
SQL_OUTBOX_RETENTION: Final = (
    "DELETE FROM outbox_events WHERE outbox_id IN (SELECT outbox_id FROM outbox_events "
    f"WHERE created_at_ms < $2::bigint ORDER BY outbox_id LIMIT $3::int) AND {FENCE}"
)
#: Months (UTC) of the RESOLVED rows due for the history move, so their partitions exist before the move.
SQL_DLQ_HISTORY_MONTHS: Final = (
    "SELECT DISTINCT date_trunc('month', to_timestamp(updated_at_ms / 1000.0) AT TIME ZONE 'UTC')::date "
    "FROM events_dlq WHERE status = 'RESOLVED' AND updated_at_ms < $1::bigint ORDER BY 1"
)
#: One batch of the history move: fenced DELETE ... RETURNING feeding the INSERT (one statement, one
#: transaction), skipping rows another session holds and rows an active row still names as its origin
#: (the self-FK of events_dlq.origin_dlq_id).
SQL_DLQ_HISTORY_MOVE: Final = (
    "WITH moved AS (DELETE FROM events_dlq d WHERE d.dlq_id IN (SELECT o.dlq_id FROM events_dlq o "
    "WHERE o.status = 'RESOLVED' AND o.updated_at_ms < $2::bigint "
    "AND NOT EXISTS (SELECT 1 FROM events_dlq c WHERE c.origin_dlq_id = o.dlq_id) "
    f"ORDER BY o.dlq_id LIMIT $3::int FOR UPDATE SKIP LOCKED) AND {FENCE} "
    "RETURNING d.dlq_id, d.origin_dlq_id, d.event_type, d.entity_id, d.adapter_id, d.raw_payload, d.payload_hash, "
    "d.error_reason, d.retry_count, d.status, d.parked_at_ms, d.updated_at_ms, d.last_error) "
    "INSERT INTO events_dlq_history (seq_id, dlq_id, origin_dlq_id, event_type, entity_id, adapter_id, raw_payload, "
    "payload_hash, error_reason, retry_count, status, parked_at_ms, resolved_at_ms, last_error) "
    "SELECT (m.updated_at_ms << 20) | (m.dlq_id & 1048575), m.dlq_id, m.origin_dlq_id, m.event_type, m.entity_id, "
    "m.adapter_id, m.raw_payload, m.payload_hash, m.error_reason, m.retry_count, m.status, m.parked_at_ms, "
    "m.updated_at_ms, m.last_error FROM moved m"
)
#: dlq_lineage_bumps (0009) rows whose origin has left events_dlq (moved to history) and that are older than the
#: keep window: a re-delivery of that payload can no longer be counted on the origin anyway, so the dedupe row
#: has done its job (otherwise the table grows by one row per deterministic re-park forever).
SQL_DLQ_BUMPS_PRUNE: Final = (
    "DELETE FROM dlq_lineage_bumps WHERE payload_hash IN (SELECT b.payload_hash FROM dlq_lineage_bumps b "
    "WHERE b.bumped_at_ms < $2::bigint AND NOT EXISTS (SELECT 1 FROM events_dlq d WHERE d.dlq_id = b.origin_dlq_id) "
    f"ORDER BY b.payload_hash LIMIT $3::int FOR UPDATE SKIP LOCKED) AND {FENCE}"
)
SQL_ARCHIVED_NAMES: Final = "SELECT partition_name FROM partition_archive WHERE partition_name = ANY($1::text[])"
SQL_ARCHIVE_RECORD: Final = (
    "INSERT INTO partition_archive (partition_name, parent_table, row_count, checksum, archive_uri, verified_at_ms, "
    "verified_by) SELECT $2::text, $3::text, $4::bigint, $5::text, $6::text, $7::bigint, $8::text "
    f"WHERE {FENCE} ON CONFLICT (partition_name) DO NOTHING"
)
#: One partition is read through a cursor inside the READ ONLY REPEATABLE READ snapshot: a single sequential
#: scan for the whole partition (each FETCH is its own short statement, inside statement_timeout), with no
#: bound parameters. (A `ctid > $1::tid` keyset could not be bound: asyncpg's binary tid codec rejects text,
#: and `ORDER BY ctid LIMIT n` re-sorts the rest of the partition on every page.) With synchronized scans
#: off, the scan starts at block 0, so the same rows always come out in the same order.
SQL_EXPORT_SYNC_SCANS_OFF: Final = "SET LOCAL synchronize_seqscans = off"
SQL_EXPORT_DECLARE: Final = 'DECLARE xtnl_export NO SCROLL CURSOR FOR SELECT to_jsonb(t)::text FROM "{name}" t'
SQL_EXPORT_FETCH: Final = "FETCH FORWARD {n} FROM xtnl_export"
ARCHIVE_DIR_ENV: Final = "XTNL_ARCHIVE_DIR"
DAY_MS: Final = 86_400_000


@dataclass(frozen=True)
class LifecycleConfig:
    months_ahead: int = 2
    hot_days: int = 90
    outbox_batch: int = 5_000
    outbox_max_batches: int = 20
    dlq_keep_days: int = 7          # RESOLVED rows stay in events_dlq this long (payload_hash dedupe, triage)
    dlq_batch: int = 2_000
    dlq_max_batches: int = 20


@dataclass
class LifecycleReport:
    ensured: list[str] = field(default_factory=list)          # partitions created or already present
    exported: list[str] = field(default_factory=list)         # written, verified and recorded this run
    detached: list[str] = field(default_factory=list)
    awaiting_archive: list[str] = field(default_factory=list)  # past the hot window, no verified export
    count_mismatch: list[str] = field(default_factory=list)    # export row count != partition row count
    outbox_deleted: int = 0
    dlq_moved: int = 0                                         # events_dlq rows moved to events_dlq_history
    dlq_bumps_pruned: int = 0                                  # dlq_lineage_bumps rows of moved origins deleted
    errors: list[str] = field(default_factory=list)


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def add_months(d: date, n: int) -> date:
    y, m = divmod(d.month - 1 + n, 12)
    return date(d.year + y, m + 1, 1)


def months_to_ensure(now_ms: int, months_ahead: int) -> list[date]:
    """The current UTC month and the next `months_ahead` months (first days)."""
    first = month_start(datetime.fromtimestamp(now_ms / 1000, UTC).date())
    return [add_months(first, k) for k in range(months_ahead + 1)]


def partition_month(name: str, parent: str) -> date | None:
    """`event_log_p202608` -> 2026-08-01; None for the default partition or a foreign name."""
    m = _PART_NAME.match(name)
    if m is None or m.group("parent") != parent:
        return None
    try:
        return date(int(m.group("y")), int(m.group("m")), 1)
    except ValueError:
        return None


def past_hot_window(month: date, now_ms: int, hot_days: int) -> bool:
    """True when the whole month ended more than `hot_days` ago."""
    end = add_months(month, 1)
    end_ms = int(datetime(end.year, end.month, end.day, tzinfo=UTC).timestamp() * 1000)
    return end_ms <= now_ms - hot_days * DAY_MS


def detachable(parent: str) -> bool:
    return parent in PARTITIONED and parent not in AUDIT_TABLES


# ============================================================================================ export
@dataclass(frozen=True)
class ArchiveFile:
    path: Path
    rows: int
    sha256: str

    @property
    def checksum(self) -> str:
        return f"sha256:{self.sha256}"


class ArchiveVerifyError(RuntimeError):
    """The written archive does not read back as written (no partition_archive row is recorded)."""


class _GzipJsonlWriter:
    """Blocking writer (always called through asyncio.to_thread): `<final>.part`, fsync, then rename.
    gzip with mtime 0 and no file name, so the same rows always give the same bytes (and sha256)."""

    def __init__(self, final: Path) -> None:
        self.final = final
        self.tmp = final.with_name(final.name + ".part")
        self.rows = 0
        self._raw: IO[bytes] | None = None
        self._gz: gzip.GzipFile | None = None
        # A cancelled `to_thread(write)` keeps running in its worker thread. `abort()` takes this lock, so it
        # waits for that write instead of closing the (not thread-safe) GzipFile under it, and a write or
        # close that starts after an abort refuses instead of touching closed files.
        self._lock = threading.Lock()
        self._aborted = False

    def open(self) -> None:
        with self._lock:
            if self._aborted:
                raise RuntimeError("archive writer aborted")
            # Absolute path (for the file:// archive_uri), resolved here off the event loop.
            self.final = self.final.resolve()
            self.tmp = self.final.with_name(self.final.name + ".part")
            self.final.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0), 0o640)
            self._raw = os.fdopen(fd, "wb")
            self._gz = gzip.GzipFile(filename="", mode="wb", fileobj=self._raw, mtime=0)

    def write(self, lines: Sequence[str]) -> None:
        with self._lock:
            if self._aborted or self._gz is None:
                raise RuntimeError("archive writer is not open")
            for line in lines:
                if "\n" in line:    # to_jsonb text never holds a raw newline; refuse rather than corrupt
                    raise ValueError("JSON row contains a raw newline")
                self._gz.write(line.encode("utf-8") + b"\n")
            self.rows += len(lines)

    def close(self) -> None:
        with self._lock:
            if self._aborted or self._gz is None or self._raw is None:
                raise RuntimeError("archive writer is not open")
            self._gz.close()
            self._raw.flush()
            os.fsync(self._raw.fileno())
            self._raw.close()
            os.replace(self.tmp, self.final)
        _fsync_dir(self.final.parent)

    def abort(self) -> None:
        with self._lock:
            self._aborted = True
            for closer in (self._gz, self._raw):
                try:
                    if closer is not None:
                        closer.close()
                except Exception:
                    pass
            try:
                self.tmp.unlink(missing_ok=True)
            except OSError:
                pass


def _fsync_dir(d: Path) -> None:
    if os.name != "posix":
        return
    try:
        fd = os.open(d, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def verify_archive(path: Path, expected_rows: int) -> str:
    """Read the file back: full gzip decode (CRC check), line count, sha256 of the file bytes. Returns the
    hex digest; raises ArchiveVerifyError on any difference."""
    digest = hashlib.sha256()
    lines = 0
    try:
        with open(path, "rb") as raw:
            for chunk in iter(lambda: raw.read(1 << 20), b""):
                digest.update(chunk)
        with gzip.open(path, "rb") as gz:
            for _ in gz:
                lines += 1
    except (OSError, EOFError, zlib.error) as exc:
        raise ArchiveVerifyError(f"{path.name}: unreadable ({type(exc).__name__})") from exc
    if lines != expected_rows:
        raise ArchiveVerifyError(f"{path.name}: {lines} lines read back, {expected_rows} rows exported")
    return digest.hexdigest()


def _write_sidecar(path: Path, hexdigest: str) -> Path:
    """`sha256sum -c` format: `<hex>  <file name>`."""
    side = path.with_name(path.name + ".sha256")
    tmp = side.with_name(side.name + ".part")
    with open(tmp, "w", encoding="ascii", newline="\n") as f:
        f.write(f"{hexdigest}  {path.name}\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, side)
    return side


@dataclass(frozen=True)
class ExportConfig:
    archive_dir: Path
    verified_by: str = "engine"
    batch_rows: int = 5_000
    max_partitions_per_run: int = 4


class PartitionExporter:
    """Exports partitions past the hot window to `archive_dir` and records them in `partition_archive`
    (module docstring, step 2). Used by `PgPartitionMaintenance(exporter=...)`, so it runs inside the
    supervised lifecycle task, before the detach step of the same run."""

    def __init__(self, db: EngineDb, cfg: ExportConfig, hot_days: int = 90) -> None:
        self._db = db
        self.cfg = cfg
        self.hot_days = hot_days

    @classmethod
    def from_env(cls, db: EngineDb, node_id: str, environ: Mapping[str, str] | None = None,
                 hot_days: int = 90) -> "PartitionExporter | None":
        """None when XTNL_ARCHIVE_DIR is unset (partitions past 90 days then page as awaiting export)."""
        env = os.environ if environ is None else environ
        raw = (env.get(ARCHIVE_DIR_ENV) or "").strip()
        if not raw:
            return None
        return cls(db, ExportConfig(Path(raw), verified_by=f"engine:{node_id}"[:64]), hot_days)

    def target(self, parent: str, name: str) -> Path:
        return self.cfg.archive_dir / parent / f"{name}.jsonl.gz"

    async def candidates(self, now_ms: int, listed: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
        """(parent, partition) pairs past the hot window, detachable, attached and not yet archived."""
        due: list[tuple[str, str]] = []
        for parent, name in listed:
            month = partition_month(name, parent)
            if detachable(parent) and month is not None and past_hot_window(month, now_ms, self.hot_days):
                due.append((parent, name))
        if not due:
            return []
        async with self._db.source.acquire() as conn:
            rows = await conn.fetch(SQL_ARCHIVED_NAMES, [name for _, name in due])
        done = {str(r[0]) for r in rows}
        return [(p, n) for p, n in due if n not in done][: self.cfg.max_partitions_per_run]

    async def export_partition(self, parent: str, name: str) -> ArchiveFile:
        """Write and verify one partition's archive file (reads only; no database write)."""
        if not detachable(parent) or partition_month(name, parent) is None:
            raise ValueError(f"not an exportable partition: {parent}/{name}")
        declare = SQL_EXPORT_DECLARE.format(name=name)   # name matched _PART_NAME: [a-z_]+_pYYYYMM
        fetch = SQL_EXPORT_FETCH.format(n=max(1, int(self.cfg.batch_rows)))
        writer = _GzipJsonlWriter(self.target(parent, name))
        try:
            await asyncio.to_thread(writer.open)
            async with self._db.snapshot() as conn:
                await conn.execute(SQL_EXPORT_SYNC_SCANS_OFF)
                await conn.execute(declare)
                while True:
                    page = await conn.fetch(fetch)
                    if not page:
                        break
                    await asyncio.to_thread(writer.write, [str(r[0]) for r in page])
            await asyncio.to_thread(writer.close)
        except BaseException:
            await asyncio.to_thread(writer.abort)
            raise
        hexdigest = await asyncio.to_thread(verify_archive, writer.final, writer.rows)
        await asyncio.to_thread(_write_sidecar, writer.final, hexdigest)
        return ArchiveFile(writer.final, writer.rows, hexdigest)

    async def record(self, parent: str, name: str, f: ArchiveFile, now_ms: int) -> bool:
        """Fenced insert of the verified export; False when a row already exists (a lost fence raises)."""
        async with self._db.transaction() as tx:
            await tx.execute(SQL_LOCK_TIMEOUT)
            n = await tx.write(SQL_ARCHIVE_RECORD, name, parent, f.rows, f.checksum, f.path.as_uri(),
                               now_ms, self.cfg.verified_by)
        return n > 0

    async def run_once(self, now_ms: int, listed: Sequence[tuple[str, str]], rep: LifecycleReport) -> None:
        try:
            todo = await self.candidates(now_ms, listed)
        except Exception as exc:
            rep.errors.append(f"export candidates: {type(exc).__name__}: {exc}")
            return
        for parent, name in todo:
            try:
                f = await self.export_partition(parent, name)
                if await self.record(parent, name, f, now_ms):
                    rep.exported.append(name)
                    log.info("lifecycle: exported %s (%d rows, sha256 %s)", name, f.rows, f.sha256)
            except EpochMismatch:
                raise
            except Exception as exc:
                rep.errors.append(f"export {name}: {type(exc).__name__}: {exc}")


class PgPartitionMaintenance:
    """The lifecycle job over PostgreSQL (see the module docstring)."""

    def __init__(self, db: EngineDb, cfg: LifecycleConfig = LifecycleConfig(), *,
                 exporter: PartitionExporter | None = None) -> None:
        self._db = db
        self.cfg = cfg
        self.exporter = exporter

    async def run_once(self, now_ms: int) -> LifecycleReport:
        rep = LifecycleReport()
        await self._ensure_ahead(now_ms, rep)
        await self._dlq_history(now_ms, rep)
        if self.exporter is not None:
            listed = await self._list(rep)
            if listed is not None:
                await self.exporter.run_once(now_ms, listed, rep)
        await self._retention(now_ms, rep)
        await self._outbox(now_ms, rep)
        return rep

    async def _list(self, rep: LifecycleReport) -> list[tuple[str, str]] | None:
        candidates = [p for p in PARTITIONED if p not in AUDIT_TABLES]
        try:
            async with self._db.source.acquire() as conn:
                rows = await conn.fetch(SQL_LIST_PARTITIONS, candidates)
        except Exception as exc:
            rep.errors.append(f"list partitions: {type(exc).__name__}: {exc}")
            return None
        return [(str(r[0]), str(r[1])) for r in rows]

    async def _ensure_ahead(self, now_ms: int, rep: LifecycleReport) -> None:
        for month in months_to_ensure(now_ms, self.cfg.months_ahead):
            for parent, shift in PARTITIONED.items():
                await self._ensure(parent, month, shift, rep)

    async def _ensure(self, parent: str, month: date, shift: int, rep: LifecycleReport) -> bool:
        name = f"{parent}_p{month:%Y%m}"
        try:
            async with self._db.transaction() as tx:
                await tx.execute(SQL_LOCK_TIMEOUT)
                await tx.write_returning(SQL_ENSURE_PARTITION, parent, month, shift, must=True)
        except EpochMismatch:
            raise
        except Exception as exc:   # e.g. rows of that month already in the default partition
            rep.errors.append(f"create {name}: {type(exc).__name__}: {exc}")
            return False
        if name not in rep.ensured:
            rep.ensured.append(name)
        return True

    async def _dlq_history(self, now_ms: int, rep: LifecycleReport) -> None:
        """Step 1b: move RESOLVED rows past `dlq_keep_days` to events_dlq_history, after creating the month
        partitions they need (a month that cannot be created stops the move: nothing lands in default)."""
        cutoff = now_ms - self.cfg.dlq_keep_days * DAY_MS
        try:
            async with self._db.source.acquire() as conn:
                rows = await conn.fetch(SQL_DLQ_HISTORY_MONTHS, cutoff)
        except Exception as exc:
            rep.errors.append(f"dlq history months: {type(exc).__name__}: {exc}")
            return
        for r in rows:
            month = r[0]
            if not isinstance(month, date):
                month = date.fromisoformat(str(month)[:10])
            if not await self._ensure("events_dlq_history", month_start(month), PARTITIONED["events_dlq_history"],
                                      rep):
                return
        for _ in range(self.cfg.dlq_max_batches):
            try:
                async with self._db.transaction() as tx:
                    await tx.execute(SQL_LOCK_TIMEOUT)
                    n = await tx.write(SQL_DLQ_HISTORY_MOVE, cutoff, self.cfg.dlq_batch)
            except EpochMismatch:
                raise
            except Exception as exc:
                rep.errors.append(f"dlq history: {type(exc).__name__}: {exc}")
                return
            rep.dlq_moved += n
            if n < self.cfg.dlq_batch:
                break
        await self._dlq_bumps(cutoff, rep)

    async def _dlq_bumps(self, cutoff: int, rep: LifecycleReport) -> None:
        """Step 1c: drop the lineage-bump dedupe rows of origins that left events_dlq (bounded table)."""
        for _ in range(self.cfg.dlq_max_batches):
            try:
                async with self._db.transaction() as tx:
                    await tx.execute(SQL_LOCK_TIMEOUT)
                    n = await tx.write(SQL_DLQ_BUMPS_PRUNE, cutoff, self.cfg.dlq_batch)
            except EpochMismatch:
                raise
            except Exception as exc:
                rep.errors.append(f"dlq lineage bumps: {type(exc).__name__}: {exc}")
                return
            rep.dlq_bumps_pruned += n
            if n < self.cfg.dlq_batch:
                return

    async def _retention(self, now_ms: int, rep: LifecycleReport) -> None:
        listed = await self._list(rep)
        if listed is None:
            return
        for parent, name in listed:
            month = partition_month(name, parent)
            if month is None or not past_hot_window(month, now_ms, self.cfg.hot_days):
                continue
            try:
                async with self._db.transaction() as tx:
                    await tx.execute(SQL_LOCK_TIMEOUT)
                    row = await tx.write_returning(SQL_DETACH_ARCHIVED, parent, name, must=True)
                outcome = "" if row is None else str(row[0])
            except EpochMismatch:
                raise
            except Exception as exc:
                rep.errors.append(f"detach {name}: {type(exc).__name__}: {exc}")
                continue
            if outcome == "detached":
                rep.detached.append(name)
            elif outcome == "not_archived":
                rep.awaiting_archive.append(name)
            elif outcome == "count_mismatch":
                rep.count_mismatch.append(name)

    async def _outbox(self, now_ms: int, rep: LifecycleReport) -> None:
        cutoff = now_ms - self.cfg.hot_days * DAY_MS
        for _ in range(self.cfg.outbox_max_batches):
            try:
                async with self._db.transaction() as tx:
                    n = await tx.write(SQL_OUTBOX_RETENTION, cutoff, self.cfg.outbox_batch)
            except EpochMismatch:
                raise
            except Exception as exc:
                rep.errors.append(f"outbox retention: {type(exc).__name__}: {exc}")
                return
            rep.outbox_deleted += n
            if n < self.cfg.outbox_batch:
                return


class PartitionMaintenance(Protocol):
    async def run_once(self, now_ms: int) -> LifecycleReport: ...


@dataclass
class LifecycleRunner:
    """When to run and what to page (pure; the runtime's supervised task drives it)."""

    every_ms: int = 6 * 3_600_000
    next_run_ms: int = 0
    paged: set[str] = field(default_factory=set)

    def due(self, now_ms: int) -> bool:
        return now_ms >= self.next_run_ms

    def ran(self, now_ms: int) -> None:
        self.next_run_ms = now_ms + self.every_ms

    def pages(self, rep: LifecycleReport) -> list[tuple[str, str, str]]:
        """(severity, title, body) for conditions not yet paged; a cleared condition pages again later."""
        out: list[tuple[str, str, str]] = []
        current: dict[str, tuple[str, str, str]] = {}
        if rep.errors:
            current["errors"] = ("high", "lifecycle: maintenance errors", "; ".join(rep.errors)[:2000])
        if rep.awaiting_archive:
            current["archive"] = ("info", "lifecycle: partitions past 90 days await export",
                                  ", ".join(rep.awaiting_archive))
        if rep.count_mismatch:
            current["mismatch"] = ("high", "lifecycle: export row count mismatch (not detached)",
                                   ", ".join(rep.count_mismatch))
        for key, page in current.items():
            if key not in self.paged:
                out.append(page)
        self.paged = set(current)
        return out


__all__ = ["ARCHIVE_DIR_ENV", "AUDIT_TABLES", "ArchiveFile", "ArchiveVerifyError", "ExportConfig", "LifecycleConfig",
           "LifecycleReport", "LifecycleRunner", "PARTITIONED", "PartitionExporter", "PartitionMaintenance",
           "PgPartitionMaintenance", "add_months", "detachable", "month_start", "months_to_ensure", "partition_month",
           "past_hot_window", "verify_archive"]
