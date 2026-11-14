"""Lock-free single-leader fencing (operations.md §2, skill §1 P12) and the named exceptions."""
from __future__ import annotations

import pytest

from src.db import epoch as E
from src.db.epoch import (
    HALT_LOCK,
    CommitUnknownError,
    EngineDb,
    Epoch,
    EpochFenced,
    application_name,
    bump_epoch,
    epoch_transaction,
    read_epoch,
)
from src.db.fence import FENCE, EpochMismatch
from src.ports import DlqRow

from tests.db.conftest import EPOCH, FakeConn, FakeSource

FENCED_SQL = f"UPDATE x SET y = 1 WHERE id = $2 AND {FENCE}"


async def test_transaction_takes_no_lock_and_commits(conn: FakeConn) -> None:
    async with epoch_transaction(conn, EPOCH) as tx:
        assert tx.epoch == 7
        await tx.write(FENCED_SQL, 1, must=True)
    assert conn.sql == [E.SQL_BEGIN_RC, FENCED_SQL, E.SQL_COMMIT_TIMEOUT_OFF, E.SQL_COMMIT]
    assert conn.args_of(FENCED_SQL) == [(7, 1)]  # $1 = epoch, prepended
    assert not any("advisory" in s for s in conn.sql)


async def test_transaction_accepts_bare_token(conn: FakeConn) -> None:
    async with epoch_transaction(conn, 7) as tx:
        await tx.write(FENCED_SQL, 1, must=True)
    assert conn.sql[-1] == E.SQL_COMMIT


async def test_lost_fence_rolls_back(conn: FakeConn) -> None:
    conn.stored_epoch = 8   # a newer leader bumped the epoch
    with pytest.raises(EpochMismatch):
        async with epoch_transaction(conn, EPOCH) as tx:
            await tx.write(FENCED_SQL, 1, must=True)
            await tx.execute("never reached")
    assert conn.sql == [E.SQL_BEGIN_RC, FENCED_SQL, E.SQL_ROLLBACK]


async def test_body_error_rolls_back(conn: FakeConn) -> None:
    with pytest.raises(ValueError):
        async with epoch_transaction(conn, EPOCH):
            raise ValueError("boom")
    assert conn.sql[-1] == E.SQL_ROLLBACK and E.SQL_COMMIT not in conn.sql


async def test_commit_failure_is_unknown_outcome(conn: FakeConn) -> None:
    conn.fail(E.SQL_COMMIT, ConnectionResetError("gone"))
    with pytest.raises(CommitUnknownError):
        async with epoch_transaction(conn, EPOCH):
            pass


async def test_snapshot_is_read_only_and_unfenced(db: EngineDb, conn: FakeConn) -> None:
    async with db.snapshot() as tx:
        await tx.fetch("SELECT 1")
    assert conn.sql == [E.SQL_BEGIN_SNAPSHOT, "SELECT 1", E.SQL_ROLLBACK]


async def test_spill_drain_is_unfenced_and_idempotent(db: EngineDb, conn: FakeConn) -> None:
    conn.stored_epoch = 99   # even a fenced node drains its spill file
    rows = [DlqRow("ORDER", "c1", "a", "{}", "h1", "QueueFull"), DlqRow("ORDER", "c2", "a", "{}", "h2", "x", 4)]
    conn.status_for(E.SQL_SPILL_DRAIN_INSERT, "INSERT 0 2")
    assert await db.spill_drain_insert(rows) == 2
    assert conn.sql == [E.SQL_SPILL_DRAIN_INSERT]
    assert "ON CONFLICT (payload_hash) DO NOTHING" in E.SQL_SPILL_DRAIN_INSERT and FENCE not in E.SQL_SPILL_DRAIN_INSERT
    assert conn.args_of(E.SQL_SPILL_DRAIN_INSERT)[0][6] == [None, 4]
    assert await db.spill_drain_insert([]) == 0


async def test_bump_first_epoch(conn: FakeConn) -> None:
    conn.epoch_row = None
    await bump_epoch(conn, EPOCH, "node-a")
    assert conn.sql == [E.SQL_BEGIN_RC, E.SQL_LOCK_TIMEOUT_2S, E.SQL_READ_EPOCH_FOR_UPDATE, E.SQL_INSERT_FIRST_EPOCH,
                        E.SQL_COMMIT_TIMEOUT_OFF, E.SQL_COMMIT, E.SQL_TERMINATE_OLDER_EPOCHS]
    assert conn.args_of(E.SQL_INSERT_FIRST_EPOCH) == [("cluster-a", 7, "node-a")]


async def test_bump_only_increases(conn: FakeConn) -> None:
    conn.epoch_row = ("cluster-a", 6)
    work_calls: list[int] = []

    async def work(tx: object) -> str:
        work_calls.append(1)
        return "done"

    assert await bump_epoch(conn, EPOCH, "node-a", in_txn=work) == "done"
    assert conn.args_of(E.SQL_BUMP_EPOCH) == [("cluster-a", 7, "node-a")]
    assert "revision < $2" in E.SQL_BUMP_EPOCH and work_calls == [1]
    assert conn.args_of(E.SQL_TERMINATE_OLDER_EPOCHS) == [(7,)]
    assert not any("advisory" in s for s in conn.sql)


async def test_zero_row_bump_is_fenced(conn: FakeConn) -> None:
    conn.epoch_row = ("cluster-a", 9)
    conn.status_for(E.SQL_BUMP_EPOCH, "UPDATE 0")
    with pytest.raises(EpochFenced):
        await bump_epoch(conn, EPOCH, "node-a")
    assert conn.sql[-1] == E.SQL_ROLLBACK and E.SQL_TERMINATE_OLDER_EPOCHS not in conn.sql


async def test_other_cluster_is_fenced(conn: FakeConn) -> None:
    conn.epoch_row = ("cluster-b", 1)
    with pytest.raises(EpochFenced, match="sign-off"):
        await bump_epoch(conn, EPOCH, "node-a")


async def test_concurrent_first_bump_is_fenced(conn: FakeConn) -> None:
    conn.epoch_row = None
    conn.status_for(E.SQL_INSERT_FIRST_EPOCH, "INSERT 0 0")
    with pytest.raises(EpochFenced):
        await bump_epoch(conn, EPOCH, "node-a")


async def test_terminate_failure_does_not_fail_the_bump(conn: FakeConn) -> None:
    class NoPrivilege(Exception):
        sqlstate = "42501"

    conn.fail(E.SQL_TERMINATE_OLDER_EPOCHS, NoPrivilege())
    await bump_epoch(conn, EPOCH, "node-a")
    assert E.SQL_COMMIT in conn.sql


async def test_bump_work_failure_rolls_back(conn: FakeConn) -> None:
    async def work(tx: object) -> None:
        raise RuntimeError("safe hold check failed")

    with pytest.raises(RuntimeError):
        await bump_epoch(conn, EPOCH, "node-a", in_txn=work)
    assert conn.sql[-1] == E.SQL_ROLLBACK


async def test_read_epoch(conn: FakeConn) -> None:
    assert await read_epoch(conn) == EPOCH
    conn.epoch_row = None
    assert await read_epoch(conn) is None


async def test_engine_db_explicit_token(conn: FakeConn) -> None:
    db = EngineDb(FakeSource(conn), lambda: EPOCH)
    conn.stored_epoch = 3
    async with db.transaction(3) as tx:
        await tx.write(FENCED_SQL, 1, must=True)
    with pytest.raises(EpochMismatch):
        async with db.transaction() as tx:   # this node's epoch (7) no longer matches
            await tx.write(FENCED_SQL, 1, must=True)


def test_epoch_label_and_application_name() -> None:
    e = Epoch("abc", 42)
    assert e.label == "42" and e.token == 42   # boot id = run_id:42, as ActionGate compares it
    assert application_name(e, "node-a") == "xtnl:42:node-a"
    assert len(application_name(e, "n" * 200)) == 63
    assert 0 < HALT_LOCK < 2**63
    assert not hasattr(E, "EPOCH_LOCK")


def test_spill_drain_skips_archived_payloads_and_archived_origins() -> None:
    """5.5: a late re-drain never re-opens a row already moved to events_dlq_history, and a lineage origin that
    was archived is dropped from origin_dlq_id instead of failing the whole batch on the self-FK."""
    sql = E.SQL_SPILL_DRAIN_INSERT
    assert "NOT EXISTS (SELECT 1 FROM events_dlq_history x WHERE x.payload_hash = u.h::bpchar)" in sql
    # char(64) = text would cast the column and skip the payload_hash index (sequential scan per row)
    assert "x.payload_hash = u.h)" not in sql
    assert "(SELECT o.dlq_id FROM events_dlq o WHERE o.dlq_id = u.o)" in sql
    assert sql.endswith("ON CONFLICT (payload_hash) DO NOTHING") and FENCE not in sql
