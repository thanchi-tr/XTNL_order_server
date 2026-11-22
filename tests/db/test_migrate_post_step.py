"""The migrate post step (S08, ST-3; design §C1 "Locks on partitioned tables"): no transaction, per-partition
CREATE INDEX CONCURRENTLY + ATTACH PARTITION for each registered parent index, VALIDATE CONSTRAINT for each
registered NOT VALID CHECK; idempotent, lock_timeout 2 s, retried on a lost lock wait."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.db import migrate as M

from tests.db.conftest import FakeConn

IDX = M.PartitionIndex(16, "notification_outbox", "notification_outbox_arm_due_idx", "arm_due_idx",
                       "kind, next_attempt_at_ms, outbox_id", "sent_at_ms IS NULL")
CHK = M.ValidateCheck(16, "notification_outbox", "notification_outbox_arm_channels_check")
PARTS = [("notification_outbox_default", "r"), ("notification_outbox_p202611", "r")]


def create(p: str) -> str:
    return (f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {p}_arm_due_idx ON {p} (kind, next_attempt_at_ms, outbox_id) "
            "WHERE sent_at_ms IS NULL")


def attach(p: str) -> str:
    return f"ALTER INDEX notification_outbox_arm_due_idx ATTACH PARTITION {p}_arm_due_idx"


VALIDATE = "ALTER TABLE notification_outbox VALIDATE CONSTRAINT notification_outbox_arm_channels_check"


class LockNotAvailable(Exception):
    sqlstate = "55P03"


async def nosleep(s: float) -> None:
    return None


def ddl(conn: FakeConn) -> list[str]:
    return [s for s in conn.sql if s.startswith(("CREATE", "ALTER", "DROP"))]


async def test_first_run_builds_attaches_and_validates_outside_a_transaction(conn: FakeConn) -> None:
    conn.returns_all(M.SQL_PARTITIONS, PARTS)
    conn.returns(M.SQL_CHECK_VALIDATED, (False,))
    done = await M.post_step(conn, {15, 16}, items=(IDX, CHK))
    a, b = (p for p, _ in PARTS)
    assert ddl(conn) == [create(a), attach(a), create(b), attach(b), VALIDATE]
    assert done == [f"attached {a}_arm_due_idx to notification_outbox_arm_due_idx",
                    f"attached {b}_arm_due_idx to notification_outbox_arm_due_idx",
                    "validated notification_outbox.notification_outbox_arm_channels_check"]
    assert conn.sql[0] == M.SQL_POST_LOCK_TIMEOUT and conn.sql[-1] == M.SQL_POST_RESET_LOCK_TIMEOUT
    assert not any(s.startswith(("BEGIN", "COMMIT", "SET LOCAL")) for s in conn.sql)   # never in a transaction
    assert conn.args_of(M.SQL_PARTITIONS) == [("notification_outbox",)]
    assert conn.args_of(M.SQL_ATTACHED_INDEX) == [("notification_outbox_arm_due_idx", a),
                                                  ("notification_outbox_arm_due_idx", b)]
    assert conn.args_of(M.SQL_CHECK_VALIDATED) == [("notification_outbox", "notification_outbox_arm_channels_check")]


async def test_second_run_is_a_no_op(conn: FakeConn) -> None:
    """Idempotent: attached indexes and a validated CHECK need no DDL."""
    conn.returns_all(M.SQL_PARTITIONS, PARTS)
    conn.returns(M.SQL_ATTACHED_INDEX, ("notification_outbox_default_arm_due_idx",),
                 ("notification_outbox_p202611_arm_due_idx",))
    conn.returns(M.SQL_CHECK_VALIDATED, (True,))
    assert await M.post_step(conn, {16}, items=(IDX, CHK)) == []
    assert ddl(conn) == [] and M.SQL_INDEX_VALID not in conn.sql


async def test_a_partition_created_later_is_built_on_the_next_run(conn: FakeConn) -> None:
    conn.returns_all(M.SQL_PARTITIONS, [*PARTS, ("notification_outbox_p202612", "r")])
    conn.returns(M.SQL_ATTACHED_INDEX, ("x",), ("y",), None)
    assert await M.post_step(conn, {16}, items=(IDX,)) == [
        "attached notification_outbox_p202612_arm_due_idx to notification_outbox_arm_due_idx"]
    assert ddl(conn) == [create("notification_outbox_p202612"), attach("notification_outbox_p202612")]


async def test_an_invalid_leftover_is_dropped_and_rebuilt(conn: FakeConn) -> None:
    """An interrupted CONCURRENTLY build leaves an INVALID index of the same name."""
    p = "notification_outbox_default"
    conn.returns_all(M.SQL_PARTITIONS, [(p, "r")])
    conn.returns(M.SQL_INDEX_VALID, (False,))
    done = await M.post_step(conn, {16}, items=(IDX,))
    assert ddl(conn) == [f"DROP INDEX CONCURRENTLY IF EXISTS {p}_arm_due_idx", create(p), attach(p)]
    assert done[0] == f"dropped invalid index {p}_arm_due_idx"
    assert conn.args_of(M.SQL_INDEX_VALID) == [(f"{p}_arm_due_idx",)]


async def test_a_valid_unattached_index_is_only_attached(conn: FakeConn) -> None:
    p = "notification_outbox_default"
    conn.returns_all(M.SQL_PARTITIONS, [(p, "r")])
    conn.returns(M.SQL_INDEX_VALID, (True,))
    await M.post_step(conn, {16}, items=(IDX,))
    assert ddl(conn) == [create(p), attach(p)]          # IF NOT EXISTS keeps the valid one


async def test_lost_lock_waits_are_retried_with_back_off(conn: FakeConn) -> None:
    p = "notification_outbox_default"
    conn.returns_all(M.SQL_PARTITIONS, [(p, "r")])
    conn.fail(attach(p), LockNotAvailable("lock timeout"))
    conn.fail(VALIDATE, LockNotAvailable("lock timeout"))
    conn.fail(VALIDATE, LockNotAvailable("lock timeout"))
    conn.returns(M.SQL_CHECK_VALIDATED, (False,), (False,), (False,))
    waits: list[float] = []

    async def sleep(s: float) -> None:
        waits.append(s)

    await M.post_step(conn, {16}, items=(IDX, CHK), backoff_s=2.0, sleep=sleep)
    assert waits == [2.0, 2.0, 4.0]
    assert conn.sql.count(attach(p)) == 2 and conn.sql.count(VALIDATE) == 3


async def test_retries_are_bounded_and_the_lock_timeout_is_reset(conn: FakeConn) -> None:
    conn.returns(M.SQL_CHECK_VALIDATED, (False,), (False,))
    conn.fail(VALIDATE, LockNotAvailable("a"))
    conn.fail(VALIDATE, LockNotAvailable("b"))
    with pytest.raises(LockNotAvailable):
        await M.post_step(conn, {16}, items=(CHK,), retries=1, sleep=nosleep)
    assert conn.sql[-1] == M.SQL_POST_RESET_LOCK_TIMEOUT


async def test_a_missing_constraint_or_a_sub_partitioned_table_is_an_error(conn: FakeConn) -> None:
    with pytest.raises(M.MigrationError, match="missing"):
        await M.post_step(conn, {16}, items=(CHK,))
    conn2 = FakeConn()
    conn2.returns_all(M.SQL_PARTITIONS, [("notification_outbox_sub", "p")])
    with pytest.raises(M.MigrationError, match="not a plain table"):
        await M.post_step(conn2, {16}, items=(IDX,))


async def test_unsafe_identifiers_are_refused(conn: FakeConn) -> None:
    conn.returns_all(M.SQL_PARTITIONS, [('x"; DROP TABLE t; --', "r")])
    with pytest.raises(M.MigrationError, match="identifier"):
        await M.post_step(conn, {16}, items=(IDX,))
    assert ddl(conn) == []


async def test_items_of_migrations_not_in_the_set_are_skipped(conn: FakeConn) -> None:
    assert await M.post_step(conn, {1, 2}) == [] and conn.sql == []


def test_partition_index_names_fit_63_bytes() -> None:
    assert M.partition_index_name("notification_outbox_p202611", "arm_due_idx") == \
        "notification_outbox_p202611_arm_due_idx"
    long = "p" * 60
    name = M.partition_index_name(long, "arm_due_idx")
    assert len(name) == 63 and name.endswith("_arm_due_idx") and name == M.partition_index_name(long, "arm_due_idx")
    assert name != M.partition_index_name("q" * 60, "arm_due_idx")


#: 0017 (S23): the event_log index serving entry time and S_at_T, built per partition by the post step.
EVENT_LOG_IDX = M.PartitionIndex(17, "event_log", "event_log_broker_time_idx", "broker_time_idx",
                                 "entity_type, entity_key, broker_time_ms")


#: 0019 (S38d review): the orders_state index of the S_at_T linked-children read, built concurrently.
LINKED_IDX = M.TableIndex(19, "orders_state", "orders_state_linked_stop_idx", "adapter_id, linked_position_id",
                          "is_reduce_only")
LOG_ID_IDX = M.PartitionIndex(21, "event_log", "event_log_log_id_idx", "log_id_idx", "log_id")


def test_registered_items() -> None:
    assert M.POST_STEP == (M.ValidateCheck(15, "event_log", "event_log_origin_check"), IDX, CHK, EVENT_LOG_IDX,
                           LINKED_IDX, LOG_ID_IDX)


async def test_the_0017_event_log_index_is_built_and_attached_per_partition(conn: FakeConn) -> None:
    conn.returns_all(M.SQL_PARTITIONS, [("event_log_default", "r"), ("event_log_p202611", "r")])
    done = await M.post_step(conn, {17}, retries=0, sleep=nosleep)
    assert done == ["attached event_log_default_broker_time_idx to event_log_broker_time_idx",
                    "attached event_log_p202611_broker_time_idx to event_log_broker_time_idx"]
    assert ("CREATE INDEX CONCURRENTLY IF NOT EXISTS event_log_p202611_broker_time_idx ON event_log_p202611 "
            "(entity_type, entity_key, broker_time_ms)") in conn.sql
    assert "ALTER INDEX event_log_broker_time_idx ATTACH PARTITION event_log_p202611_broker_time_idx" in conn.sql


async def test_the_0021_log_id_index_is_built_and_attached_per_partition(conn: FakeConn) -> None:
    """The live view's id cursor index (docs/design/live-view.md s3.1): built concurrently on every partition, the
    DEFAULT one included, then attached to the ON ONLY parent index that 0021 creates."""
    conn.returns_all(M.SQL_PARTITIONS, [("event_log_default", "r"), ("event_log_p202611", "r")])
    done = await M.post_step(conn, {21}, retries=0, sleep=nosleep)
    assert done == ["attached event_log_default_log_id_idx to event_log_log_id_idx",
                    "attached event_log_p202611_log_id_idx to event_log_log_id_idx"]
    for part in ("event_log_default", "event_log_p202611"):
        assert f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {part}_log_id_idx ON {part} (log_id)" in conn.sql
        assert f"ALTER INDEX event_log_log_id_idx ATTACH PARTITION {part}_log_id_idx" in conn.sql


async def test_migrate_runs_the_post_step_after_every_file_and_on_every_run(tmp_path: Path, conn: FakeConn) -> None:
    (tmp_path / "0016_x.sql").write_text("SELECT 16;", encoding="utf-8")
    conn.returns(M.SQL_CHECK_VALIDATED, (False,))
    assert await M.migrate(conn, tmp_path, post=(CHK,)) == [16]
    assert conn.sql[conn.sql.index(M.SQL_COMMIT) + 1] == M.SQL_POST_LOCK_TIMEOUT   # after the file's COMMIT
    assert VALIDATE in conn.sql and conn.sql[-2:] == [M.SQL_POST_RESET_LOCK_TIMEOUT, M.SQL_UNLOCK]

    again = FakeConn()      # up to date: no file applied, the post step still runs (finishes an interrupted one)
    again.returns_all(M.SQL_APPLIED, [(16, M.discover(tmp_path)[0].checksum)])
    again.returns(M.SQL_CHECK_VALIDATED, (True,))
    assert await M.migrate(again, tmp_path, post=(CHK,)) == []
    assert M.SQL_BEGIN not in again.sql and M.SQL_CHECK_VALIDATED in again.sql and VALIDATE not in again.sql


async def test_the_shipped_set_registers_the_post_step(conn: FakeConn) -> None:
    """With the real migrations every registered item runs (0015 and 0016 are in the set)."""
    versions = {m.version for m in M.discover()}
    assert {it.version for it in M.POST_STEP} <= versions
