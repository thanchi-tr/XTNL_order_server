"""S10 PR-G2, storage side (design §B13; plan S10: RT-M9 method, RT-M10).

* The pending query filters on the kinds the leader can act on (`AND r.kind = ANY($2::text[])`, before the
  `LIMIT 50`), so pending requests of a kind without a handler never hide a core one, and leaves out the ids the
  session accepted and has not answered (`$3`), so requests waiting on a hung owner task never hide one either. A
  real-PostgreSQL check (marker `pg`, skipped without ORDER_SERVER_TEST_DSN, and unless that DSN names a disposable
  database whose name ends in `_test`: `migrate()` commits on it before the rolled-back transaction starts)
  inserts 60 `arm_disarm` rows under a test-only kind list (the CHECK is widened inside the rolled-back
  transaction) and one `reset_task`, and reads them through `PgOperatorRequests` on the same connection.
* `PgPartitionMaintenance.ensure_months(parent, months)`: the public, fenced form of the lifecycle's ensure step.
"""
from __future__ import annotations

import os
from datetime import date
from urllib.parse import urlparse

import pytest

from src.db import lifecycle as LC
from src.db.epoch import EngineDb
from src.db.fence import EpochMismatch
from src.db.operator_requests import CORE_REQUEST_KINDS, REQUEST_KINDS, SQL_PENDING_WITH_PAYLOAD
from tests.db.conftest import EPOCH, FakeConn, FakeSource

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")


def _database_name(dsn: str) -> str:
    """The database a DSN names (URL form `postgresql://.../name` or key/value form `dbname=name`)."""
    if "://" in dsn:
        return urlparse(dsn).path.lstrip("/")
    for part in dsn.split():
        key, _, value = part.partition("=")
        if key == "dbname":
            return value.strip("'\"")
    return ""


#: `migrate()` commits permanently, so the real-PostgreSQL test runs only on a disposable `*_test` database.
DISPOSABLE_DSN = bool(DSN) and _database_name(DSN or "").endswith("_test")


# ============================================================================================ kind filter
def test_pending_query_filters_on_the_kind_list_before_the_limit() -> None:
    sql = SQL_PENDING_WITH_PAYLOAD
    assert "AND r.kind = ANY($2::text[])" in sql
    assert "AND NOT (r.request_id = ANY($3::bigint[]))" in sql
    assert sql.index("ANY($2::text[])") < sql.index("ORDER BY r.request_id") < sql.index("LIMIT 50")
    assert sql.index("ANY($3::bigint[])") < sql.index("ORDER BY r.request_id")
    assert "r.requested_epoch = $1::bigint" in sql and "r.consumed_at_ms IS NULL" in sql


def test_core_kinds_are_the_session_consumed_ones() -> None:
    assert CORE_REQUEST_KINDS == {"reset_task", "release_account", "refresh_cash_flows"}
    assert CORE_REQUEST_KINDS <= REQUEST_KINDS


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
@pytest.mark.skipif(not DISPOSABLE_DSN, reason="ORDER_SERVER_TEST_DSN must name a disposable *_test database")
async def test_sixty_unhandled_requests_never_hide_a_reset_task_on_postgres() -> None:
    """Needs a disposable database: `migrate(conn)` commits on it before the rolled-back transaction starts."""
    from src.db.migrate import migrate
    from src.db.pg import create_asyncpg_source
    from src.runtime.pg_ports import PgOperatorRequests

    assert DSN is not None
    source = await create_asyncpg_source(DSN, application_name="xtnl:0:s10-it")
    try:
        async with source.acquire() as conn:
            await migrate(conn)
            tr = conn.transaction()
            await tr.start()
            try:
                # test-only kind list: widen the CHECK inside this transaction (rolled back below)
                await conn.execute("ALTER TABLE operator_requests DROP CONSTRAINT operator_requests_kind_check")
                await conn.execute(
                    "ALTER TABLE operator_requests ADD CONSTRAINT operator_requests_kind_check CHECK (kind IN "
                    "('reset_task', 'release_account', 'refresh_cash_flows', 'arm_disarm'))")
                audit = await conn.fetchval(
                    "INSERT INTO operator_audit (command, target, operator, reason, outcome, epoch) "
                    "VALUES ('test.s10', 'x', 'tester', 'S10 kind filter test', 'applied', 424242) RETURNING audit_id")
                for i in range(60):
                    await conn.execute(
                        "INSERT INTO operator_requests (kind, target, audit_id, requested_epoch) "
                        "VALUES ('arm_disarm', $1, $2, 424242)", f"P{i}", audit)
                await conn.execute(
                    "INSERT INTO operator_requests (kind, target, audit_id, requested_epoch) "
                    "VALUES ('reset_task', 'risksweep', $1, 424242)", audit)
                # the session's own port on this connection (inside the transaction): the real parameter passing
                queue = PgOperatorRequests(EngineDb(FakeSource(conn), lambda: EPOCH))  # type: ignore[arg-type]
                core = await queue.pending(424242, CORE_REQUEST_KINDS)
                assert [(r.kind, r.target) for r in core] == [("reset_task", "risksweep")]
                both = await queue.pending(424242, CORE_REQUEST_KINDS | {"arm_disarm"})
                assert len(both) == 50 and all(r.kind == "arm_disarm" for r in both)   # LIMIT 50, oldest first
                # the first 50 accepted by a handler whose owner hangs: they take no slot, the reset_task is read
                accepted = [r.request_id for r in both]
                rest = await queue.pending(424242, CORE_REQUEST_KINDS | {"arm_disarm"}, accepted)
                assert len(rest) == 11 and rest[-1].kind == "reset_task"
                assert not set(accepted) & {r.request_id for r in rest}
            finally:
                await tr.rollback()
    finally:
        await source.close()


# ============================================================================================ ensure_months
async def test_ensure_months_creates_each_month_once_fenced(conn: FakeConn, db: EngineDb) -> None:
    conn.returns(LC.SQL_ENSURE_PARTITION, *[("p",)] * 4)
    job = LC.PgPartitionMaintenance(db)
    rep = await job.ensure_months("event_log", [date(2026, 12, 15), date(2026, 10, 1), date(2026, 12, 1),
                                                date(2027, 1, 31)])
    args = conn.args_of(LC.SQL_ENSURE_PARTITION)
    assert [a[1:] for a in args] == [("event_log", date(2026, 10, 1), 20), ("event_log", date(2026, 12, 1), 20),
                                     ("event_log", date(2027, 1, 1), 20)]
    assert all(a[0] == 7 for a in args)                       # $1 = the fencing token
    assert rep.ensured == ["event_log_p202610", "event_log_p202612", "event_log_p202701"] and rep.errors == []
    assert all(s == LC.SQL_LOCK_TIMEOUT for s in conn.sql if "lock_timeout" in s)


async def test_ensure_months_reports_a_failed_month_and_tries_the_rest(conn: FakeConn, db: EngineDb) -> None:
    conn.fail(LC.SQL_ENSURE_PARTITION, RuntimeError("rows in default partition"))
    conn.returns(LC.SQL_ENSURE_PARTITION, ("p",))
    rep = await LC.PgPartitionMaintenance(db).ensure_months("notification_outbox",
                                                            [date(2026, 11, 1), date(2026, 12, 1)])
    assert rep.ensured == ["notification_outbox_p202612"]
    assert len(rep.errors) == 1 and "notification_outbox_p202611" in rep.errors[0]
    assert [a[3] for a in conn.args_of(LC.SQL_ENSURE_PARTITION)] == [0, 0]      # ms-range shift


async def test_ensure_months_refuses_an_unknown_parent_and_propagates_a_lost_fence(conn: FakeConn,
                                                                                 db: EngineDb) -> None:
    job = LC.PgPartitionMaintenance(db)
    with pytest.raises(ValueError):
        await job.ensure_months("orders_state", [date(2026, 11, 1)])
    assert conn.args_of(LC.SQL_ENSURE_PARTITION) == []
    conn.stored_epoch = 99
    with pytest.raises(EpochMismatch):
        await job.ensure_months("event_log", [date(2026, 11, 1)])
    assert (await job.ensure_months("event_log", [])).ensured == []
