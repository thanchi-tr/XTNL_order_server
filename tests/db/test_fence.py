"""FencedTx semantics: 0 rows -> EpochMismatch when a row was expected; re-check when 0 is legitimate."""
from __future__ import annotations

import pytest

from src.db.fence import FENCE, SQL_CHECK_EPOCH, EpochMismatch, FencedTx, assert_epoch, stored_epoch

from tests.db.conftest import FakeConn

INS = f"INSERT INTO t (a) SELECT $2::text WHERE {FENCE} ON CONFLICT DO NOTHING RETURNING 1"
UPD = f"UPDATE t SET a = $2 WHERE {FENCE}"


def test_fence_predicate_shape() -> None:
    assert FENCE == "(SELECT epoch FROM engine_epoch WHERE id = 1) = $1::bigint"


async def test_write_counts_rows(conn: FakeConn) -> None:
    tx = FencedTx(conn, 7)
    conn.status_for(UPD, "UPDATE 3")
    assert await tx.write(UPD, "x") == 3
    assert conn.args_of(UPD) == [(7, "x")]
    assert SQL_CHECK_EPOCH not in conn.sql   # a non-zero result needs no re-check


async def test_must_write_zero_rows_is_lost_fence(conn: FakeConn) -> None:
    conn.status_for(UPD, "UPDATE 0")   # even with a matching epoch, `must` means 0 rows = fence lost
    with pytest.raises(EpochMismatch):
        await FencedTx(conn, 7).write(UPD, "x", must=True)
    assert SQL_CHECK_EPOCH not in conn.sql


async def test_optional_zero_rows_rechecks_fence(conn: FakeConn) -> None:
    conn.status_for(UPD, "UPDATE 0")
    assert await FencedTx(conn, 7).write(UPD, "x") == 0       # CAS miss / conflict, fence intact
    assert conn.sql == [UPD, SQL_CHECK_EPOCH]
    conn.stored_epoch = 8
    with pytest.raises(EpochMismatch):
        await FencedTx(conn, 7).write(UPD, "x")


async def test_returning(conn: FakeConn) -> None:
    tx = FencedTx(conn, 7)
    conn.returns(INS, (1,))
    assert await tx.write_returning(INS, "a") == (1,)
    assert await tx.write_returning(INS, "a") is None          # conflict, fence intact
    with pytest.raises(EpochMismatch):
        await tx.write_returning(INS, "a", must=True)
    conn.stored_epoch = None                                  # engine_epoch row missing
    with pytest.raises(EpochMismatch):
        await tx.write_returning(INS, "a")


async def test_returning_all(conn: FakeConn) -> None:
    sql = f"DELETE FROM t WHERE {FENCE} RETURNING a"
    conn.returns_all(sql, [(1,), (2,)])
    tx = FencedTx(conn, 7)
    assert await tx.write_returning_all(sql) == [(1,), (2,)]
    assert await tx.write_returning_all(sql) == []
    conn.stored_epoch = 9
    with pytest.raises(EpochMismatch):
        await tx.write_returning_all(sql)


async def test_assert_and_read_epoch(conn: FakeConn) -> None:
    assert await stored_epoch(conn) == 7
    await assert_epoch(conn, 7)
    with pytest.raises(EpochMismatch):
        await assert_epoch(conn, 6)


async def test_unfenced_passthrough(conn: FakeConn) -> None:
    tx = FencedTx(conn, 7)
    await tx.execute("SET x = 1")
    await tx.fetch("SELECT 1")
    await tx.fetchrow("SELECT 2", 5)
    assert conn.calls == [("SET x = 1", ()), ("SELECT 1", ()), ("SELECT 2", (5,))]
