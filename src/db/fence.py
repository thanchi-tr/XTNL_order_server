"""Single-leader lock-free fencing (skill §1 P12; operations.md §2).

Every engine **state write** is conditional on the stored fencing token, in the same statement:

    INSERT INTO t (...) SELECT $2::..., ... WHERE (SELECT epoch FROM engine_epoch WHERE id = 1) = $1
    UPDATE t SET ... WHERE ... AND (SELECT epoch FROM engine_epoch WHERE id = 1) = $1

By convention the epoch is always parameter `$1` of a fenced statement. No advisory lock is taken: the
statement reads `engine_epoch` under MVCC, so a bump never waits for the old leader and the old leader
never waits for the bump. A fenced statement that affects 0 rows where a row was expected means the
fence is lost: `EpochMismatch` is raised, the transaction rolls back and the caller hard-exits
(operations.md §3). Where 0 rows is also a legitimate answer (ON CONFLICT DO NOTHING, a CAS miss), the
fence is re-checked with one read (`SQL_CHECK_EPOCH`) to tell the two apart.

Residual window (accepted by the single-leader design): a transaction whose last fenced statement ran
before the bump committed may still COMMIT after it. Lease timing bounds it (fence 12 s < TTL 15 s,
operations.md §3), and the bump terminates older-epoch backends after it commits (epoch.bump_epoch).
"""
from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Final

from src.db.pg import PgConnection, Row, rowcount

#: The fence predicate. Append with AND / WHERE; `$1` is the caller's epoch.
FENCE: Final = "(SELECT epoch FROM engine_epoch WHERE id = 1) = $1::bigint"
SQL_CHECK_EPOCH: Final = "SELECT (SELECT epoch FROM engine_epoch WHERE id = 1)"
#: Savepoints (ST-2). One fixed name: PostgreSQL resolves a repeated name to the most recent savepoint, so nested
#: `FencedTx.savepoint()` blocks release and roll back in LIFO order.
SQL_SAVEPOINT: Final = "SAVEPOINT xtnl_sp"
SQL_RELEASE_SAVEPOINT: Final = "RELEASE SAVEPOINT xtnl_sp"
SQL_ROLLBACK_TO_SAVEPOINT: Final = "ROLLBACK TO SAVEPOINT xtnl_sp"


class EpochMismatch(RuntimeError):
    """engine_epoch no longer holds this node's epoch: the node is fenced. The transaction is rolled back;
    the caller must hard-exit (operations.md §3)."""


async def stored_epoch(conn: PgConnection) -> int | None:
    row = await conn.fetchrow(SQL_CHECK_EPOCH)
    return None if row is None or row[0] is None else int(row[0])


async def assert_epoch(conn: PgConnection, epoch: int) -> None:
    """Raise EpochMismatch unless engine_epoch still holds `epoch` (one read, no lock)."""
    stored = await stored_epoch(conn)
    if stored != epoch:
        raise EpochMismatch(f"engine_epoch is {stored}, this node holds {epoch}")


class FencedTx:
    """A connection inside an engine READ COMMITTED transaction, bound to this node's epoch.

    * `write(sql, *args)`: a fenced statement (`$1` = epoch is prepended). Returns rows affected.
      `must=True`: 0 rows can only mean a lost fence -> EpochMismatch. `must=False`: 0 rows is
      re-checked; a lost fence raises, otherwise 0 is returned to the caller.
    * `write_returning(sql, *args)`: the same for `... RETURNING` statements (one row or None).
    * `savepoint()`: a nested block whose writes are undone on error while the transaction carries on.
    * `execute` / `fetchrow` / `fetch`: unfenced reads (and the named exceptions) on the same connection.
    """

    def __init__(self, conn: PgConnection, epoch: int) -> None:
        self.conn = conn
        self.epoch = epoch

    async def write(self, sql: str, *args: object, must: bool = False) -> int:
        n = rowcount(await self.conn.execute(sql, self.epoch, *args))
        if n == 0:
            await self._zero(sql, must)
        return n

    async def write_returning(self, sql: str, *args: object, must: bool = False) -> Row | None:
        row = await self.conn.fetchrow(sql, self.epoch, *args)
        if row is None:
            await self._zero(sql, must)
        return row

    async def write_returning_all(self, sql: str, *args: object) -> list[Row]:
        rows = await self.conn.fetch(sql, self.epoch, *args)
        if not rows:
            await assert_epoch(self.conn, self.epoch)
        return rows

    async def _zero(self, sql: str, must: bool) -> None:
        if must:
            raise EpochMismatch(f"fenced write affected 0 rows (epoch {self.epoch}): {sql[:60]}")
        await assert_epoch(self.conn, self.epoch)

    @asynccontextmanager
    async def _savepoint(self) -> AsyncIterator[FencedTx]:
        await self.conn.execute(SQL_SAVEPOINT)
        try:
            yield self
        except BaseException:
            # undo only the block's statements; the outer transaction stays usable. A failure here (connection
            # lost) never hides the block's own error: the outer transaction rolls back anyway.
            with contextlib.suppress(Exception):
                await self.conn.execute(SQL_ROLLBACK_TO_SAVEPOINT)
                await self.conn.execute(SQL_RELEASE_SAVEPOINT)
            raise
        await self.conn.execute(SQL_RELEASE_SAVEPOINT)

    def savepoint(self) -> AbstractAsyncContextManager[FencedTx]:
        """A nested unit inside this transaction (ST-2): `SAVEPOINT`, then `RELEASE` when the block ends normally,
        or `ROLLBACK TO` + `RELEASE` and the error re-raised when it raises, so the block's writes are undone and
        the outer transaction carries on. Savepoints nest (the same name refers to the most recent one)."""
        return self._savepoint()

    async def execute(self, sql: str, *args: object) -> str:
        return await self.conn.execute(sql, *args)

    async def fetchrow(self, sql: str, *args: object) -> Row | None:
        return await self.conn.fetchrow(sql, *args)

    async def fetch(self, sql: str, *args: object) -> list[Row]:
        return await self.conn.fetch(sql, *args)
