"""Fake driver connection that records every statement in order (no PostgreSQL needed).

Fencing is simulated: a statement containing the fence predicate whose `$1` differs from
`stored_epoch` affects 0 rows / returns no row, exactly as PostgreSQL would.
"""
from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest

from src.db.epoch import EngineDb, Epoch
from src.db.fence import FENCE, SQL_CHECK_EPOCH

EPOCH = Epoch("cluster-a", 7)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "pg: integration test against a real PostgreSQL (ORDER_SERVER_TEST_DSN)")


class FakeConn:
    """Scripted responses per exact SQL text; everything else returns a neutral default."""

    def __init__(self, epoch: Epoch = EPOCH) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.rows: dict[str, deque[Any]] = defaultdict(deque)
        self.lists: dict[str, deque[list[Any]]] = defaultdict(deque)
        self.status: dict[str, deque[str]] = defaultdict(deque)
        self.errors: dict[str, deque[BaseException]] = defaultdict(deque)
        self.epoch_row: tuple[Any, ...] | None = (epoch.cluster_id, epoch.revision)
        self.stored_epoch: int | None = epoch.revision

    # scripting -----------------------------------------------------------------------------------
    def returns(self, sql: str, *rows: Any) -> None:
        """Queue fetchrow results for `sql` (None = no row)."""
        self.rows[sql].extend(rows)

    def returns_all(self, sql: str, *row_lists: list[Any]) -> None:
        """Queue fetch() results for `sql` (one list per call)."""
        self.lists[sql].extend(row_lists)

    def status_for(self, sql: str, *tags: str) -> None:
        self.status[sql].extend(tags)

    def fail(self, sql: str, exc: BaseException) -> None:
        self.errors[sql].append(exc)

    # driver protocol -----------------------------------------------------------------------------
    def _record(self, sql: str, args: tuple[Any, ...]) -> None:
        self.calls.append((sql, args))
        if self.errors.get(sql):
            raise self.errors[sql].popleft()

    def _fenced_out(self, sql: str, args: tuple[Any, ...]) -> bool:
        return FENCE in sql and (not args or args[0] != self.stored_epoch)

    async def execute(self, sql: str, *args: object) -> str:
        self._record(sql, args)
        if self._fenced_out(sql, args):
            return "UPDATE 0"
        if self.status.get(sql):
            return self.status[sql].popleft()
        return "OK 1"

    async def fetchrow(self, sql: str, *args: object) -> tuple[Any, ...] | None:
        self._record(sql, args)
        if self._fenced_out(sql, args):
            return None
        if self.rows.get(sql):
            return self.rows[sql].popleft()  # type: ignore[no-any-return]
        if sql == SQL_CHECK_EPOCH:
            return (self.stored_epoch,)
        if sql.startswith("SELECT cluster_id, revision FROM engine_epoch"):
            return self.epoch_row
        return None

    async def fetch(self, sql: str, *args: object) -> list[tuple[Any, ...]]:
        self._record(sql, args)
        if self._fenced_out(sql, args):
            return []
        if self.lists.get(sql):
            return list(self.lists[sql].popleft())
        return list(self.rows.pop(sql, []))

    # helpers -------------------------------------------------------------------------------------
    @property
    def sql(self) -> list[str]:
        return [s for s, _ in self.calls]

    def args_of(self, sql: str) -> list[tuple[Any, ...]]:
        return [a for s, a in self.calls if s == sql]


class FakeSource:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[Any]:
        yield self.conn

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        return self._acq()


@pytest.fixture
def conn() -> FakeConn:
    return FakeConn()


@pytest.fixture
def db(conn: FakeConn) -> EngineDb:
    return EngineDb(FakeSource(conn), lambda: EPOCH)
