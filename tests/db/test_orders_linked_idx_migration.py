"""0019_orders_linked_idx.sql (plan S38d review; design §B6.4): the `S_at_T` linked-children read gets an index.

`SQL_LINKED_STOP_CHILDREN` filters `orders_state` on `(adapter_id, linked_position_id)` and `is_reduce_only`; the
table keeps every terminal order, so 0019 registers a partial index built by the migrate post step with
`CREATE INDEX CONCURRENTLY` (never inside the file's transaction), and the statement's filter is the index's columns
and predicate, so the planner can use it."""
from __future__ import annotations

from src.db import arm_versions as V
from src.db import migrate as M
from src.db.migrate import discover

from tests.db.conftest import FakeConn

MIG = next(m for m in discover() if m.version == 19)
CODE = "\n".join(line.split("--", 1)[0] for line in MIG.sql.splitlines())
IDX = next(it for it in M.POST_STEP if isinstance(it, M.TableIndex) and it.version == 19)
CREATE = ("CREATE INDEX CONCURRENTLY IF NOT EXISTS orders_state_linked_stop_idx ON orders_state "
          "(adapter_id, linked_position_id) WHERE is_reduce_only")


def ddl(conn: FakeConn) -> list[str]:
    return [s for s in conn.sql if s.startswith(("CREATE", "ALTER", "DROP"))]


def test_the_migration_is_expand_only_and_builds_nothing_in_its_transaction() -> None:
    assert MIG.name == "orders_linked_idx" and MIG.sql.startswith("-- 0019_orders_linked_idx.sql")
    upper = CODE.upper()
    for word in ("CREATE", "ALTER", "DROP", "DELETE", "UPDATE", "INSERT", "CONCURRENTLY"):
        assert word not in upper                       # the index is the post step's work, outside a transaction
    assert (IDX.table, IDX.index, IDX.columns, IDX.where) == (
        "orders_state", "orders_state_linked_stop_idx", "adapter_id, linked_position_id", "is_reduce_only")


def test_the_linked_children_statement_matches_the_index() -> None:
    sql = V.SQL_LINKED_STOP_CHILDREN
    assert "FROM orders_state WHERE adapter_id = $1::text AND linked_position_id = $2::text AND is_reduce_only" in sql
    for column in (c.strip() for c in IDX.columns.split(",")):
        assert f"{column} = $" in sql                  # equality on every index column
    assert IDX.where is not None and f"AND {IDX.where} " in sql   # the partial index's predicate is implied


async def test_the_post_step_builds_it_concurrently_once(conn: FakeConn) -> None:
    done = await M.post_step(conn, {19}, items=(IDX,))
    assert ddl(conn) == [CREATE] and done == ["built orders_state_linked_stop_idx"]
    assert not any(s.startswith(("BEGIN", "COMMIT", "SET LOCAL")) for s in conn.sql)
    again = FakeConn()
    again.returns(M.SQL_INDEX_VALID, (True,))
    assert await M.post_step(again, {19}, items=(IDX,)) == [] and ddl(again) == []


async def test_an_invalid_leftover_is_dropped_and_rebuilt(conn: FakeConn) -> None:
    conn.returns(M.SQL_INDEX_VALID, (False,))
    done = await M.post_step(conn, {19}, items=(IDX,))
    assert ddl(conn) == ["DROP INDEX CONCURRENTLY IF EXISTS orders_state_linked_stop_idx", CREATE]
    assert done == ["dropped invalid index orders_state_linked_stop_idx", "built orders_state_linked_stop_idx"]
