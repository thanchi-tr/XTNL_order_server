"""PgOutboxStore relay bookkeeping (due / reschedule / finish / oldest) over the recording fake driver."""
from __future__ import annotations

import pytest

from src.db import notify_store as N
from src.db.epoch import EngineDb
from src.db.oversee_errors import SweepFenced
from src.notifications.relay import RelayOutboxStore
from src.oversee.notify import OutboxRow, episode_key
from src.oversee.types import Level
from tests.db.conftest import EPOCH, FakeConn

KEY = episode_key("agg", "account", "A1", 5, Level.SOFT_HALT)


def test_pg_outbox_store_is_a_relay_store(db: EngineDb) -> None:
    assert isinstance(N.PgOutboxStore(db), RelayOutboxStore)


async def test_due_maps_channels_and_attempts(db: EngineDb, conn: FakeConn) -> None:
    conn.returns_all(N.SQL_OUTBOX_DUE, [
        (3, "episode", "HIGH", "SOFT r", "b", 100, '["agg","account","A1",5,2]', "A1", ["mobile_push"], 2),
        (4, "info", "INFO", "t", "b", 101, None, None, ["email"], 0),
    ])
    rows = await N.PgOutboxStore(db).due(5_000, 10)
    assert conn.args_of(N.SQL_OUTBOX_DUE) == [(5_000, 10)]
    assert rows[0] == OutboxRow("episode", "high", "SOFT r", "b", 100, KEY, "A1", 3, ("mobile_push",), 2)
    assert rows[0].route == ("mobile_push",)
    assert rows[1].route == ("email",) and rows[1].attempts == 0


async def test_reschedule_and_finish_are_fenced_writes(db: EngineDb, conn: FakeConn) -> None:
    store = N.PgOutboxStore(db)
    await store.reschedule(3, remaining=["mobile_push"], next_attempt_ms=9_000, error="mobile_push: down",
                           attempted=True)
    await store.finish(3, 12_000, delivered=True, error=None)
    assert conn.args_of(N.SQL_OUTBOX_RESCHEDULE) == [(EPOCH.revision, 3, ["mobile_push"], 9_000,
                                                      "mobile_push: down", 1)]
    assert conn.args_of(N.SQL_OUTBOX_FINISH) == [(EPOCH.revision, 3, 12_000, True, None)]
    for sql in (N.SQL_OUTBOX_RESCHEDULE, N.SQL_OUTBOX_FINISH):
        assert "sent_at_ms IS NULL" in sql and "engine_epoch" in sql


async def test_finish_already_finished_row_is_not_an_error(db: EngineDb, conn: FakeConn) -> None:
    conn.status_for(N.SQL_OUTBOX_FINISH, "UPDATE 0")      # sent meanwhile; fence still ours
    await N.PgOutboxStore(db).finish(3, 1, delivered=False, error="deduplicated")


async def test_lost_fence_raises(db: EngineDb, conn: FakeConn) -> None:
    conn.stored_epoch = EPOCH.revision + 1
    with pytest.raises(SweepFenced):
        await N.PgOutboxStore(db).reschedule(3, remaining=["email"], next_attempt_ms=1, error=None, attempted=False)


async def test_oldest_undelivered(db: EngineDb, conn: FakeConn) -> None:
    store = N.PgOutboxStore(db)
    conn.returns(N.SQL_OUTBOX_OLDEST_UNSENT, (1_234,), (None,))
    assert await store.oldest_undelivered_ms() == 1_234
    assert await store.oldest_undelivered_ms() is None


def test_existing_statements_unchanged_for_the_dispatcher() -> None:
    assert "sent_at_ms IS NULL" in N.SQL_OUTBOX_PENDING
    assert "ARRAY['email', 'mobile_push']" in N.SQL_OUTBOX_INSERT


async def test_due_decodes_sm6_retry_state(db: EngineDb, conn: FakeConn) -> None:
    """5.4: per-channel failures and the escalation marker live in the channels column."""
    conn.returns_all(N.SQL_OUTBOX_DUE, [
        (5, "info", "INFO", "t", "b", 100, None, "A1", ["email:5", "mobile_push", "+escalated"], 5),
    ])
    (row,) = await N.PgOutboxStore(db).due(5_000, 10)
    assert row.route == ("email", "mobile_push") and row.escalated is True
    assert row.channel_failures == (("email", 5),) and row.failures_of("email") == 5 and row.attempts == 5


async def test_reschedule_encodes_sm6_retry_state(db: EngineDb, conn: FakeConn) -> None:
    await N.PgOutboxStore(db).reschedule(5, remaining=["email", "mobile_push"], next_attempt_ms=9_000,
                                         error="email: down; escalated", attempted=True,
                                         failures={"email": 6, "mobile_push": 0}, escalated=True)
    assert conn.args_of(N.SQL_OUTBOX_RESCHEDULE) == [
        (EPOCH.revision, 5, ["email:6", "mobile_push", "+escalated"], 9_000, "email: down; escalated", 1)]


async def test_escalated_since_lists_rows_for_the_dashboard(db: EngineDb, conn: FakeConn) -> None:
    conn.returns_all(N.SQL_OUTBOX_ESCALATED, [
        (7, "info", "INFO", "t", "b", 100, None, None, ["email:9", "+escalated"], 9, None, None, "email: down"),
        (6, "trip", "CRITICAL", "HALT", "b", 90, None, None, ["+escalated"], 3, 2_000, True, "escalated"),
    ])
    out = await N.PgOutboxStore(db).escalated_since(50, 20)
    assert conn.args_of(N.SQL_OUTBOX_ESCALATED) == [("+escalated", 50, 20)]
    assert out[0].row.outbox_id == 7 and out[0].sent_at_ms is None and out[0].row.failures_of("email") == 9
    assert out[1].delivered is True and out[1].row.channels == () and out[1].row.escalated
