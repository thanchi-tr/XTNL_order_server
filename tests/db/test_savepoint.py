"""FencedTx.savepoint() (S08, ST-2): SAVEPOINT / RELEASE, or ROLLBACK TO + RELEASE and the error re-raised; the
outer transaction keeps its own writes and commits."""
from __future__ import annotations

import pytest

from src.db import epoch as E
from src.db.epoch import EngineDb, Epoch
from src.db.fence import (SQL_RELEASE_SAVEPOINT, SQL_ROLLBACK_TO_SAVEPOINT, SQL_SAVEPOINT, EpochMismatch, FencedTx)
from src.db.notify_store import insert_outbox
from src.oversee.notify import OutboxRow

from tests.db.conftest import FakeConn
from tests.db.pg_emulator import PgEmulator

ROW = OutboxRow("info", "info", "outer", "b", 100)


class Boom(Exception):
    pass


async def test_a_clean_block_is_released(conn: FakeConn) -> None:
    tx = FencedTx(conn, 7)
    async with tx.savepoint() as sp:
        assert sp is tx
        await sp.execute("SELECT 1")
    assert conn.sql == [SQL_SAVEPOINT, "SELECT 1", SQL_RELEASE_SAVEPOINT]


async def test_a_raising_block_is_rolled_back_to_the_savepoint_and_re_raised(conn: FakeConn) -> None:
    tx = FencedTx(conn, 7)
    with pytest.raises(Boom):
        async with tx.savepoint():
            await tx.execute("SELECT 1")
            raise Boom
    assert conn.sql == [SQL_SAVEPOINT, "SELECT 1", SQL_ROLLBACK_TO_SAVEPOINT, SQL_RELEASE_SAVEPOINT]


async def test_a_failing_rollback_never_hides_the_block_error(conn: FakeConn) -> None:
    conn.fail(SQL_ROLLBACK_TO_SAVEPOINT, ConnectionResetError("gone"))
    with pytest.raises(EpochMismatch):
        async with FencedTx(conn, 7).savepoint():
            raise EpochMismatch("fenced")


async def test_nested_savepoints_unwind_in_order(conn: FakeConn) -> None:
    tx = FencedTx(conn, 7)
    async with tx.savepoint():
        with pytest.raises(Boom):
            async with tx.savepoint():
                raise Boom
        await tx.execute("SELECT 2")
    assert conn.sql == [SQL_SAVEPOINT, SQL_SAVEPOINT, SQL_ROLLBACK_TO_SAVEPOINT, SQL_RELEASE_SAVEPOINT,
                        "SELECT 2", SQL_RELEASE_SAVEPOINT]


def test_savepoint_statements() -> None:
    assert (SQL_SAVEPOINT, SQL_RELEASE_SAVEPOINT, SQL_ROLLBACK_TO_SAVEPOINT) == (
        "SAVEPOINT xtnl_sp", "RELEASE SAVEPOINT xtnl_sp", "ROLLBACK TO SAVEPOINT xtnl_sp")


async def test_savepoint_rollback_keeps_the_outer_transaction() -> None:
    emu = PgEmulator(epoch=7)
    db = EngineDb(emu, lambda: Epoch("c", 7))
    async with db.transaction() as tx:
        await insert_outbox(tx, [ROW])
        with pytest.raises(Boom):
            async with tx.savepoint():
                await insert_outbox(tx, [OutboxRow("info", "info", "inner, undone", "b", 101)])
                raise Boom
        async with tx.savepoint():
            await insert_outbox(tx, [OutboxRow("info", "info", "inner, kept", "b", 102)])
    assert emu.outbox_titles() == ["outer", "inner, kept"]
    assert emu.sql[0] == E.SQL_BEGIN_RC and E.SQL_COMMIT in emu.sql and E.SQL_ROLLBACK not in emu.sql


async def test_an_outer_rollback_also_undoes_released_savepoints() -> None:
    emu = PgEmulator(epoch=7)
    db = EngineDb(emu, lambda: Epoch("c", 7))
    with pytest.raises(Boom):
        async with db.transaction() as tx:
            async with tx.savepoint():
                await insert_outbox(tx, [ROW])
            raise Boom
    assert emu.outbox_titles() == []
