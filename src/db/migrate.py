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

    python -m src.db.migrate            # uses ORDER_SERVER_DSN; prints the redacted DSN only
"""
from __future__ import annotations

import asyncio
import hashlib
import importlib
import re
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
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


async def migrate(conn: PgConnection, directory: Path = MIGRATIONS_DIR, *, retries: int = DEFAULT_RETRIES,
                  backoff_s: float = DEFAULT_BACKOFF_S, sleep: Sleep = asyncio.sleep) -> list[int]:
    """Apply pending migrations; returns the versions applied (empty when up to date). A migration whose
    lock wait expires (or that deadlocks) is retried up to `retries` times, waiting backoff_s, 2 x backoff_s, ..."""
    migrations = discover(directory)
    await conn.execute(SQL_CREATE_TABLE)
    await conn.fetchrow(SQL_LOCK, MIGRATE_LOCK)
    try:
        applied = {int(r[0]): str(r[1]) for r in await conn.fetch(SQL_APPLIED)}
        done: list[int] = []
        for m in pending(migrations, applied):
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
        return done
    finally:
        await conn.fetchrow(SQL_UNLOCK, MIGRATE_LOCK)


async def _main() -> int:   # pragma: no cover - operator entry point (needs a server)
    from src.db.pg import AsyncpgConnection
    from src.db.settings import StorageSettings, redact

    settings = StorageSettings.from_env()
    asyncpg = importlib.import_module("asyncpg")
    raw = await asyncpg.connect(settings.dsn, timeout=5)
    try:
        print(f"migrating {redact(settings.dsn)}")
        versions = await migrate(AsyncpgConnection(raw))
        print("applied: " + (", ".join(f"{v:04d}" for v in versions) or "none (up to date)"))
    finally:
        await raw.close()
    return 0


if __name__ == "__main__":   # pragma: no cover
    sys.exit(asyncio.run(_main()))
