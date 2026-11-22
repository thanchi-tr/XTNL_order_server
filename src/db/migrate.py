"""Schema migrations runner (operations.md §12: an operator job, never run at engine start-up).

Applies `src/db/migrations/NNNN_name.sql` in numeric order, each in its own transaction with
`lock_timeout = 2s`, and records it in `schema_migrations(version, name, checksum, applied_at_ms)`.
A migration that loses a lock wait (SQLSTATE 55P03 lock_not_available, 40P01 deadlock_detected) is rolled
back and retried with back-off, up to `retries` times (operations.md §12: "lock_timeout 2s and retries"),
so a busy table never fails the operator job on the first wait. Any other error is raised at once.
Idempotent: applied versions are skipped; an applied file whose SHA-256 changed raises `MigrationDrift`
(fix forward with a new file). The one exception is a reviewed **comment-only** edit listed in
`ACCEPTED_REVISIONS`: the applied checksum must be the listed old one and the file, with `--` comments
and blank lines removed, must still hash to the listed value, so any statement change is still drift.
A session advisory lock serialises concurrent runners.

Post step (no transaction, after every file; design §C1 "Locks on partitioned tables", plan S08): a migration
creates an index on a partitioned table `ON ONLY` the parent (it builds nothing) and adds a CHECK `NOT VALID`
(no scan under the file's lock). `post_step` then, for each `POST_STEP` item whose migration is in the set:
  * `PartitionIndex`: on every partition (the DEFAULT one included, listed from `pg_inherits`) whose index is not
    yet attached, `CREATE INDEX CONCURRENTLY IF NOT EXISTS` (an invalid leftover of an interrupted build is dropped
    first) and `ALTER INDEX ... ATTACH PARTITION`; once all are attached the parent index is valid and partitions
    created later get it automatically;
  * `ValidateCheck`: `ALTER TABLE ... VALIDATE CONSTRAINT` (SHARE UPDATE EXCLUSIVE: writes continue) unless
    `pg_constraint.convalidated` already says so;
  * `TableIndex`: on a plain (not partitioned) table, `CREATE INDEX CONCURRENTLY IF NOT EXISTS` unless a valid
    index of that name exists (an invalid leftover of an interrupted build is dropped first), so a busy table's
    writes are never blocked (0019, the S_at_T linked-children index on `orders_state`).
It is idempotent, runs with a session `lock_timeout` of 2 s and the same retries as a file, and never runs inside
a transaction (CREATE INDEX CONCURRENTLY cannot). No query needs the index or the validated CHECK to be correct.
The post step runs only for migrations that are applied (never for one held back below).

Accepted level (plan S38f, design §0b): the operator entry point applies a migration above
`XTNL_MIGRATIONS_ACCEPTED_UPTO` (a local setting, an integer, default 14) only when the setting allows it. Every
pending migration above the level is held back: one line names them and the job exits 0, so a start script that runs
it (`deploy/local/start-xtnl.ps1`, `xtnl-migrate.service`) carries on with the schema as it is. A value that is not a
whole number in range is reported and taken as the default (nothing unreviewed is applied). `migrate()` itself applies
everything unless its caller passes `accepted_upto`.

    python -m src.db.migrate            # uses ORDER_SERVER_DSN; prints the redacted DSN only
"""
from __future__ import annotations

import asyncio
import hashlib
import importlib
import os
import re
import sys
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Final

from src.db.pg import PgConnection, sqlstate

MIGRATIONS_DIR: Final = Path(__file__).with_name("migrations")
MIGRATE_LOCK: Final = 0x58544E4C4D494752   # "XTNLMIGR"
_NAME: Final = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")

SQL_CREATE_TABLE: Final = (
    "CREATE TABLE IF NOT EXISTS schema_migrations (version int PRIMARY KEY, name text NOT NULL, "
    "checksum char(64) NOT NULL, applied_at_ms bigint NOT NULL "
    "DEFAULT (extract(epoch FROM clock_timestamp()) * 1000)::bigint)"
)
SQL_LOCK: Final = "SELECT pg_advisory_lock($1::bigint)"
SQL_UNLOCK: Final = "SELECT pg_advisory_unlock($1::bigint)"
SQL_APPLIED: Final = "SELECT version, checksum FROM schema_migrations ORDER BY version"
SQL_BEGIN: Final = "BEGIN"
SQL_LOCK_TIMEOUT: Final = "SET LOCAL lock_timeout = '2s'"
SQL_RECORD: Final = "INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)"
SQL_COMMIT: Final = "COMMIT"
SQL_ROLLBACK: Final = "ROLLBACK"

#: SQLSTATEs retried: lock_not_available (lock_timeout expired) and deadlock_detected.
RETRY_SQLSTATES: Final = frozenset({"55P03", "40P01"})
DEFAULT_RETRIES: Final = 5
DEFAULT_BACKOFF_S: Final = 2.0
#: Reviewed comment-only edits of applied files: version -> (applied checksum, sha256 of the comment-free text).
ACCEPTED_REVISIONS: Final[Mapping[int, tuple[str, str]]] = {
    # 0008: its partition_archive comments described an external Parquet / S3 exporter (IMPLEMENTATION_PLAN 5.5).
    8: ("a90633153f1cb47e641d3e51629d68eee1f8a220462dc69437363511c95e288f",
        "c8a4cf90525fc05a8079c47cd1af5b7702564858c466a1532e073e79981d56e3"),
}

Sleep = Callable[[float], Awaitable[None]]

ACCEPTED_UPTO_VAR: Final = "XTNL_MIGRATIONS_ACCEPTED_UPTO"
#: The schema level reviewed and accepted before the ARM work: 0015 and later (the market-data / ARM prerequisites,
#: the ARM schema and its hardening) are applied only once the user raises the setting (plan S38f).
DEFAULT_ACCEPTED_UPTO: Final = 14
_MAX_VERSION: Final = 9999


def accepted_upto_from_env(environ: Mapping[str, str]) -> tuple[int, str | None]:
    """`XTNL_MIGRATIONS_ACCEPTED_UPTO` as an integer (default 14) and the problem with it, if any (a bad value is
    taken as the default: fail closed, nothing unreviewed is applied). Never raises."""
    raw = (environ.get(ACCEPTED_UPTO_VAR) or "").strip()
    if not raw:
        return DEFAULT_ACCEPTED_UPTO, None
    if not raw.isascii() or not raw.isdigit() or int(raw) > _MAX_VERSION:
        return DEFAULT_ACCEPTED_UPTO, (f"{ACCEPTED_UPTO_VAR} is not a whole number in [0, {_MAX_VERSION}]; "
                                       f"taken as {DEFAULT_ACCEPTED_UPTO}")
    return int(raw), None


def held_line(held: Sequence[Migration], accepted_upto: int) -> str:
    """The one line naming the migrations held back above the accepted level (names only)."""
    names = ", ".join(f"{m.version:04d}_{m.name}" for m in held)
    return (f"held back above {ACCEPTED_UPTO_VAR}={accepted_upto} (not applied; raise the setting once reviewed): "
            f"{names}")

# ---- post step (no transaction) ---------------------------------------------------------------------------
SQL_POST_LOCK_TIMEOUT: Final = "SET lock_timeout = '2s'"
SQL_POST_RESET_LOCK_TIMEOUT: Final = "RESET lock_timeout"
#: Partitions of a table: (relname, relkind). to_regclass: a missing table yields no rows.
SQL_PARTITIONS: Final = (
    "SELECT c.relname::text, c.relkind::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
    "WHERE i.inhparent = to_regclass($1::text) ORDER BY c.relname"
)
#: The index of partition $2 already attached to parent index $1 (one row when attached).
SQL_ATTACHED_INDEX: Final = (
    "SELECT x.indexrelid::regclass::text FROM pg_inherits i JOIN pg_index x ON x.indexrelid = i.inhrelid "
    "WHERE i.inhparent = to_regclass($1::text) AND x.indrelid = to_regclass($2::text)"
)
#: indisvalid of an index (no row when it does not exist).
SQL_INDEX_VALID: Final = "SELECT x.indisvalid FROM pg_index x WHERE x.indexrelid = to_regclass($1::text)"
SQL_CHECK_VALIDATED: Final = (
    "SELECT convalidated FROM pg_constraint WHERE conrelid = to_regclass($1::text) AND conname = $2::text"
)
_IDENT: Final = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_MAX_IDENT: Final = 63


def _ident(name: str) -> str:
    """A plain lower-case identifier, safe to format into DDL (catalog names only; anything else is refused)."""
    if not _IDENT.match(name):
        raise MigrationError(f"post step: not a plain identifier: {name!r}")
    return name


@dataclass(frozen=True)
class PartitionIndex:
    """An index created `ON ONLY` a partitioned parent by migration `version`; built per partition here."""

    version: int
    table: str
    index: str
    suffix: str          # the partition's index is named <partition>_<suffix>
    columns: str         # e.g. "kind, next_attempt_at_ms, outbox_id"
    where: str | None = None


@dataclass(frozen=True)
class ValidateCheck:
    """A CHECK added `NOT VALID` on a partitioned table by migration `version`; validated here."""

    version: int
    table: str
    constraint: str


@dataclass(frozen=True)
class TableIndex:
    """An index on a plain table added by migration `version`; built here with `CREATE INDEX CONCURRENTLY`."""

    version: int
    table: str
    index: str
    columns: str
    where: str | None = None


PostItem = PartitionIndex | ValidateCheck | TableIndex

#: Registered post-step work, in order: the 0015 / 0016 CHECKs and outbox index (S08), and the 0017 event_log index
#: serving entry time and S_at_T (`(entity_type, entity_key, broker_time_ms)`, design PR8, S23), and the 0019
#: `orders_state` index of the S_at_T linked-children read (`SQL_LINKED_STOP_CHILDREN`, plan S38d review), and the 0021
#: event_log `log_id` index of the live view's id cursor (docs/design/live-view.md s3.1).
POST_STEP: Final[tuple[PostItem, ...]] = (
    ValidateCheck(15, "event_log", "event_log_origin_check"),
    PartitionIndex(16, "notification_outbox", "notification_outbox_arm_due_idx", "arm_due_idx",
                   "kind, next_attempt_at_ms, outbox_id", "sent_at_ms IS NULL"),
    ValidateCheck(16, "notification_outbox", "notification_outbox_arm_channels_check"),
    PartitionIndex(17, "event_log", "event_log_broker_time_idx", "broker_time_idx",
                   "entity_type, entity_key, broker_time_ms"),
    TableIndex(19, "orders_state", "orders_state_linked_stop_idx", "adapter_id, linked_position_id",
               "is_reduce_only"),
    PartitionIndex(21, "event_log", "event_log_log_id_idx", "log_id_idx", "log_id"),
)


def partition_index_name(partition: str, suffix: str) -> str:
    """`<partition>_<suffix>`, or a shortened form with a stable hash when that exceeds 63 bytes."""
    name = f"{partition}_{suffix}"
    if len(name) <= _MAX_IDENT:
        return name
    digest = hashlib.sha256(partition.encode("utf-8")).hexdigest()[:8]
    head = partition[:_MAX_IDENT - len(suffix) - len(digest) - 2]
    return f"{head}_{digest}_{suffix}"


class MigrationError(RuntimeError):
    """Bad migration set (duplicate version, bad file name)."""


class MigrationDrift(MigrationError):
    """An applied migration file changed after it was applied."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    """Every NNNN_name.sql in order; anything else ending in .sql is an error (never silently skipped)."""
    out: dict[int, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        m = _NAME.match(path.name)
        if m is None:
            raise MigrationError(f"bad migration file name: {path.name}")
        version = int(m.group(1))
        if version in out:
            raise MigrationError(f"duplicate migration version {version:04d}")
        out[version] = Migration(version, m.group(2), path.read_text(encoding="utf-8"))
    return [out[v] for v in sorted(out)]


def strip_comments(sql: str) -> str:
    """The SQL without `--` comments (outside single-quoted literals), trailing blanks and blank lines:
    the text a comment-only edit must leave unchanged."""
    out: list[str] = []
    for line in sql.splitlines():
        quoted = False
        cut = len(line)
        for i, ch in enumerate(line):
            if ch == "'":
                quoted = not quoted
            elif not quoted and line.startswith("--", i):
                cut = i
                break
        kept = line[:cut].rstrip()
        if kept:
            out.append(kept)
    return "\n".join(out)


def stripped_checksum(sql: str) -> str:
    return hashlib.sha256(strip_comments(sql).encode("utf-8")).hexdigest()


def _accepted_revision(m: Migration, stored: str, accepted: Mapping[int, tuple[str, str]]) -> bool:
    entry = accepted.get(m.version)
    return entry is not None and stored == entry[0] and stripped_checksum(m.sql) == entry[1]


def pending(migrations: Sequence[Migration], applied: dict[int, str],
            accepted: Mapping[int, tuple[str, str]] = ACCEPTED_REVISIONS) -> list[Migration]:
    """Migrations still to apply; raises MigrationDrift when an applied file changed (other than a listed,
    comment-only revision)."""
    todo: list[Migration] = []
    for m in migrations:
        stored = applied.get(m.version)
        if stored is None:
            todo.append(m)
        elif stored.strip() != m.checksum and not _accepted_revision(m, stored.strip(), accepted):
            raise MigrationDrift(f"migration {m.version:04d}_{m.name} changed after it was applied")
    return todo


def retryable(exc: BaseException) -> bool:
    """A lost lock wait or a deadlock: the migration is rolled back and retried."""
    return sqlstate(exc) in RETRY_SQLSTATES


async def _apply(conn: PgConnection, m: Migration) -> None:
    await conn.execute(SQL_BEGIN)
    try:
        await conn.execute(SQL_LOCK_TIMEOUT)
        await conn.execute(m.sql)
        await conn.execute(SQL_RECORD, m.version, m.name, m.checksum)
    except BaseException:
        await conn.execute(SQL_ROLLBACK)
        raise
    await conn.execute(SQL_COMMIT)


async def _retrying(label: str, step: Callable[[], Awaitable[None]], *, retries: int, backoff_s: float,
                    sleep: Sleep) -> None:
    attempt = 0
    while True:
        try:
            await step()
            return
        except Exception as exc:
            if not retryable(exc) or attempt >= retries:
                raise
            attempt += 1
            print(f"post step {label}: lock wait lost (SQLSTATE {sqlstate(exc)}), retry {attempt}/{retries}",
                  file=sys.stderr)
            await sleep(backoff_s * attempt)


async def _index_partition(conn: PgConnection, item: PartitionIndex, partition: str, done: list[str]) -> None:
    """Build and attach the index of one partition (nothing when it is already attached)."""
    if await conn.fetchrow(SQL_ATTACHED_INDEX, item.index, partition) is not None:
        return
    child = _ident(partition_index_name(partition, item.suffix))
    state = await conn.fetchrow(SQL_INDEX_VALID, child)
    if state is not None and not bool(state[0]):
        # an interrupted CONCURRENTLY build leaves an INVALID index: drop it and build it again
        await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {child}")
        done.append(f"dropped invalid index {child}")
    where = f" WHERE {item.where}" if item.where else ""
    await conn.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {child} ON {_ident(partition)} "
                       f"({item.columns}){where}")
    await conn.execute(f"ALTER INDEX {_ident(item.index)} ATTACH PARTITION {child}")
    done.append(f"attached {child} to {item.index}")


async def _table_index(conn: PgConnection, item: TableIndex, done: list[str]) -> None:
    """Build one plain-table index concurrently (nothing when a valid one exists)."""
    index = _ident(item.index)
    state = await conn.fetchrow(SQL_INDEX_VALID, index)
    if state is not None and bool(state[0]):
        return
    if state is not None:
        # an interrupted CONCURRENTLY build leaves an INVALID index: drop it and build it again
        await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index}")
        done.append(f"dropped invalid index {index}")
    where = f" WHERE {item.where}" if item.where else ""
    await conn.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} ON {_ident(item.table)} "
                       f"({item.columns}){where}")
    done.append(f"built {index}")


async def _validate(conn: PgConnection, item: ValidateCheck, done: list[str]) -> None:
    row = await conn.fetchrow(SQL_CHECK_VALIDATED, _ident(item.table), _ident(item.constraint))
    if row is None:
        raise MigrationError(f"post step: constraint {item.table}.{item.constraint} is missing")
    if bool(row[0]):
        return
    await conn.execute(f"ALTER TABLE {item.table} VALIDATE CONSTRAINT {item.constraint}")
    done.append(f"validated {item.table}.{item.constraint}")


async def post_step(conn: PgConnection, versions: Collection[int], *, items: Sequence[PostItem] = POST_STEP,
                    retries: int = DEFAULT_RETRIES, backoff_s: float = DEFAULT_BACKOFF_S,
                    sleep: Sleep = asyncio.sleep) -> list[str]:
    """Build the partition indexes and validate the CHECKs registered for the migrations in `versions`
    (outside any transaction; idempotent). Returns what it did (empty when everything was already done)."""
    todo = [it for it in items if it.version in versions]
    if not todo:
        return []
    done: list[str] = []
    await conn.execute(SQL_POST_LOCK_TIMEOUT)
    try:
        for it in todo:
            if isinstance(it, ValidateCheck):
                await _retrying(f"{it.table}.{it.constraint}", partial(_validate, conn, it, done),
                                retries=retries, backoff_s=backoff_s, sleep=sleep)
                continue
            if isinstance(it, TableIndex):
                await _retrying(it.index, partial(_table_index, conn, it, done),
                                retries=retries, backoff_s=backoff_s, sleep=sleep)
                continue
            for row in await conn.fetch(SQL_PARTITIONS, _ident(it.table)):
                partition, relkind = str(row[0]), str(row[1])
                if relkind != "r":
                    raise MigrationError(f"post step: partition {partition} of {it.table} is not a plain table")
                await _retrying(f"{it.index} on {partition}", partial(_index_partition, conn, it, partition, done),
                                retries=retries, backoff_s=backoff_s, sleep=sleep)
    finally:
        await conn.execute(SQL_POST_RESET_LOCK_TIMEOUT)
    return done


async def migrate(conn: PgConnection, directory: Path = MIGRATIONS_DIR, *, retries: int = DEFAULT_RETRIES,
                  backoff_s: float = DEFAULT_BACKOFF_S, sleep: Sleep = asyncio.sleep,
                  post: Sequence[PostItem] = POST_STEP, accepted_upto: int | None = None,
                  held: list[Migration] | None = None) -> list[int]:
    """Apply pending migrations; returns the versions applied (empty when up to date). A migration whose
    lock wait expires (or that deadlocks) is retried up to `retries` times, waiting backoff_s, 2 x backoff_s, ...
    Then, still under the runner lock and outside any transaction, the post step runs for the `post` items whose
    migration is in `directory` and applied (on every run, so an interrupted post step is finished by the next one).

    `accepted_upto` (plan S38f): pending migrations above it are not applied; they are appended to `held` (when
    given) for the caller's one line. None applies everything."""
    migrations = discover(directory)
    await conn.execute(SQL_CREATE_TABLE)
    await conn.fetchrow(SQL_LOCK, MIGRATE_LOCK)
    try:
        applied = {int(r[0]): str(r[1]) for r in await conn.fetch(SQL_APPLIED)}
        done: list[int] = []
        todo = pending(migrations, applied)
        if accepted_upto is not None:
            if held is not None:
                held.extend(m for m in todo if m.version > accepted_upto)
            todo = [m for m in todo if m.version <= accepted_upto]
        for m in todo:
            attempt = 0
            while True:
                try:
                    await _apply(conn, m)
                    break
                except Exception as exc:
                    if not retryable(exc) or attempt >= retries:
                        raise
                    attempt += 1
                    print(f"migration {m.version:04d}_{m.name}: lock wait lost (SQLSTATE {sqlstate(exc)}), "
                          f"retry {attempt}/{retries}", file=sys.stderr)
                    await sleep(backoff_s * attempt)
            done.append(m.version)
        live = (set(applied) | set(done)) & {m.version for m in migrations}
        for action in await post_step(conn, live, items=post, retries=retries,
                                      backoff_s=backoff_s, sleep=sleep):
            print(f"post step: {action}", file=sys.stderr)
        return done
    finally:
        await conn.fetchrow(SQL_UNLOCK, MIGRATE_LOCK)


async def _main() -> int:   # pragma: no cover - operator entry point (needs a server)
    from src.db.pg import AsyncpgConnection
    from src.db.settings import StorageSettings, redact

    settings = StorageSettings.from_env()
    upto, problem = accepted_upto_from_env(os.environ)
    if problem is not None:
        print(f"warning: {problem}", file=sys.stderr)
    asyncpg = importlib.import_module("asyncpg")
    raw = await asyncpg.connect(settings.dsn, timeout=5)
    try:
        print(f"migrating {redact(settings.dsn)} (accepted up to {upto:04d})")
        held: list[Migration] = []
        versions = await migrate(AsyncpgConnection(raw), accepted_upto=upto, held=held)
        print("applied: " + (", ".join(f"{v:04d}" for v in versions) or "none (up to date)"))
        if held:
            print(held_line(held, upto))
    finally:
        await raw.close()
    return 0


if __name__ == "__main__":   # pragma: no cover
    sys.exit(asyncio.run(_main()))
