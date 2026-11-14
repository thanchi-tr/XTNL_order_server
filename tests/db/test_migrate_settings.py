"""Migrations runner (ordered, recorded, idempotent, drift-checked) and the storage settings loader."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.db import MIGRATIONS_DIR
from src.db import migrate as M
from src.db.settings import DEFAULT_REDIS_URL, StorageSettings, redact

from tests.db.conftest import FakeConn


def write(d: Path, name: str, sql: str) -> None:
    (d / name).write_text(sql, encoding="utf-8")


def test_discover_orders_and_validates(tmp_path: Path) -> None:
    write(tmp_path, "0002_b.sql", "SELECT 2;")
    write(tmp_path, "0001_a.sql", "SELECT 1;")
    ms = M.discover(tmp_path)
    assert [(m.version, m.name) for m in ms] == [(1, "a"), (2, "b")] and len(ms[0].checksum) == 64
    write(tmp_path, "0002_c.sql", "SELECT 3;")
    with pytest.raises(M.MigrationError, match="duplicate"):
        M.discover(tmp_path)


def test_bad_file_name_is_an_error(tmp_path: Path) -> None:
    write(tmp_path, "init.sql", "SELECT 1;")
    with pytest.raises(M.MigrationError, match="bad migration file name"):
        M.discover(tmp_path)


def test_shipped_migrations_are_discoverable() -> None:
    ms = M.discover(MIGRATIONS_DIR)
    assert [m.version for m in ms][:2] == [1, 2]
    assert "epoch bigint GENERATED ALWAYS AS (revision) STORED" in ms[1].sql


async def test_migrate_applies_pending_in_order(tmp_path: Path, conn: FakeConn) -> None:
    write(tmp_path, "0001_a.sql", "CREATE TABLE a ();")
    write(tmp_path, "0002_b.sql", "CREATE TABLE b ();")
    a = M.discover(tmp_path)[0]
    conn.returns_all(M.SQL_APPLIED, [(1, a.checksum)])
    assert await M.migrate(conn, tmp_path) == [2]
    assert conn.sql == [M.SQL_CREATE_TABLE, M.SQL_LOCK, M.SQL_APPLIED, M.SQL_BEGIN, M.SQL_LOCK_TIMEOUT,
                        "CREATE TABLE b ();", M.SQL_RECORD, M.SQL_COMMIT, M.SQL_UNLOCK]
    assert conn.args_of(M.SQL_RECORD)[0][:2] == (2, "b")


async def test_migrate_is_idempotent(tmp_path: Path, conn: FakeConn) -> None:
    write(tmp_path, "0001_a.sql", "CREATE TABLE a ();")
    conn.returns_all(M.SQL_APPLIED, [(1, M.discover(tmp_path)[0].checksum)])
    assert await M.migrate(conn, tmp_path) == []
    assert M.SQL_BEGIN not in conn.sql and conn.sql[-1] == M.SQL_UNLOCK


async def test_drift_is_refused(tmp_path: Path, conn: FakeConn) -> None:
    write(tmp_path, "0001_a.sql", "CREATE TABLE a ();")
    conn.returns_all(M.SQL_APPLIED, [(1, "0" * 64)])
    with pytest.raises(M.MigrationDrift):
        await M.migrate(conn, tmp_path)
    assert conn.sql[-1] == M.SQL_UNLOCK   # the runner lock is always released


async def test_failed_migration_rolls_back(tmp_path: Path, conn: FakeConn) -> None:
    write(tmp_path, "0001_a.sql", "BROKEN;")
    conn.fail("BROKEN;", RuntimeError("syntax error"))
    with pytest.raises(RuntimeError):
        await M.migrate(conn, tmp_path)
    assert M.SQL_ROLLBACK in conn.sql and M.SQL_RECORD not in conn.sql and conn.sql[-1] == M.SQL_UNLOCK


def test_settings_from_env_and_redaction() -> None:
    s = StorageSettings.from_env({"ORDER_SERVER_DSN": "postgresql://xtnl:hunter2@db.internal:5432/xtnl?sslmode=x",
                                  "REDIS_URL": "redis://:redispw@cache:6379/0"})
    assert s.dsn.endswith("sslmode=x") and s.redis_url.startswith("redis://")
    text = repr(s) + str(s)
    assert "hunter2" not in text and "redispw" not in text and "sslmode" not in text
    assert "xtnl:***@db.internal:5432/xtnl" in text
    assert StorageSettings.from_env({"ORDER_SERVER_DSN": "postgres://u@h/d"}).redis_url == DEFAULT_REDIS_URL
    assert redact("postgresql://h/d") == "postgresql://h/d"


@pytest.mark.parametrize("env,exc", [
    ({}, KeyError), ({"ORDER_SERVER_DSN": "mysql://x"}, ValueError),
    ({"ORDER_SERVER_DSN": "postgresql://h/d", "REDIS_URL": "http://x"}, ValueError),
])
def test_settings_refuse_bad_config(env: dict[str, str], exc: type[Exception]) -> None:
    with pytest.raises(exc) as info:
        StorageSettings.from_env(env)
    assert "mysql://x" not in str(info.value)


def test_redact_multi_host_dsn_never_raises() -> None:
    from src.db.settings import StorageSettings, redact

    dsn = "postgresql://xtnl_engine:s3cret@h1:5432,h2:5432/xtnl?target_session_attrs=read-write"
    out = redact(dsn)
    assert "s3cret" not in out and "h1:5432,h2:5432" in out and out.startswith("postgresql://xtnl_engine:***@")
    assert "s3cret" not in repr(StorageSettings(dsn, "redis://:pw@r:6379/0"))
    assert redact("redis://:pw@r:6379/0") == "redis://:***@r:6379/0"
    assert redact("postgresql://h/db") == "postgresql://h/db"


# ---- 5.5: lock_timeout retries, accepted comment-only revisions ----------------------------------------
class LockNotAvailable(Exception):
    sqlstate = "55P03"


class SyntaxProblem(Exception):
    sqlstate = "42601"


async def test_lost_lock_wait_is_rolled_back_and_retried(tmp_path: Path, conn: FakeConn) -> None:
    """operations.md s12: migrations run with lock_timeout 2s AND retries (a busy table never fails the job)."""
    write(tmp_path, "0001_a.sql", "ALTER TABLE busy ADD COLUMN x int;")
    conn.fail("ALTER TABLE busy ADD COLUMN x int;", LockNotAvailable("canceling statement due to lock timeout"))
    conn.fail("ALTER TABLE busy ADD COLUMN x int;", LockNotAvailable("again"))
    waits: list[float] = []

    async def sleep(s: float) -> None:
        waits.append(s)

    assert await M.migrate(conn, tmp_path, retries=5, backoff_s=2.0, sleep=sleep) == [1]
    assert waits == [2.0, 4.0]
    assert conn.sql.count(M.SQL_ROLLBACK) == 2 and conn.sql.count(M.SQL_LOCK_TIMEOUT) == 3
    assert conn.sql.count(M.SQL_RECORD) == 1 and conn.sql[-2:] == [M.SQL_COMMIT, M.SQL_UNLOCK]


async def test_retries_are_bounded_and_other_errors_are_not_retried(tmp_path: Path, conn: FakeConn) -> None:
    write(tmp_path, "0001_a.sql", "ALTER TABLE busy ADD COLUMN x int;")
    for _ in range(3):
        conn.fail("ALTER TABLE busy ADD COLUMN x int;", LockNotAvailable("lock timeout"))

    async def sleep(s: float) -> None:
        return None

    with pytest.raises(LockNotAvailable):
        await M.migrate(conn, tmp_path, retries=2, sleep=sleep)
    assert conn.sql.count(M.SQL_ROLLBACK) == 3 and conn.sql[-1] == M.SQL_UNLOCK

    conn2 = FakeConn()
    conn2.fail("ALTER TABLE busy ADD COLUMN x int;", SyntaxProblem("syntax"))
    with pytest.raises(SyntaxProblem):
        await M.migrate(conn2, tmp_path, retries=5, sleep=sleep)
    assert conn2.sql.count(M.SQL_ROLLBACK) == 1


def test_comment_only_revision_is_accepted_and_a_statement_change_is_drift(tmp_path: Path) -> None:
    old = "-- old wording\nCREATE TABLE a (x int);  -- trailing old\n"
    new = "-- new wording, longer\n-- second line\nCREATE TABLE a (x int);  -- trailing new\n\n"
    changed = "-- new wording\nCREATE TABLE a (x bigint);\n"
    write(tmp_path, "0001_a.sql", new)
    (m,) = M.discover(tmp_path)
    old_sum = M.Migration(1, "a", old).checksum
    accepted = {1: (old_sum, M.stripped_checksum(old))}
    assert M.pending([m], {1: old_sum}, accepted) == []
    with pytest.raises(M.MigrationDrift):
        M.pending([M.Migration(1, "a", changed)], {1: old_sum}, accepted)
    with pytest.raises(M.MigrationDrift):                       # only the listed applied checksum is accepted
        M.pending([m], {1: "0" * 64}, accepted)
    assert M.strip_comments("SELECT '--not a comment' -- comment") == "SELECT '--not a comment'"


def test_shipped_0008_revision_is_comment_only() -> None:
    """The 0008 header no longer describes an external Parquet exporter; the statements are unchanged."""
    (m,) = [x for x in M.discover(MIGRATIONS_DIR) if x.version == 8]
    applied_sum, stripped = M.ACCEPTED_REVISIONS[8]
    assert M.stripped_checksum(m.sql) == stripped
    assert M.pending([m], {8: applied_sum}) == [] and M.pending([m], {8: m.checksum}) == []
    assert "Parquet" not in m.sql and "s3://" not in m.sql
