"""Terminal-state guard against a real PostgreSQL (skipped unless ORDER_SERVER_TEST_DSN points at a DISPOSABLE
database). The fake-connection tests in test_terminal_guard.py prove the logic; this one proves the SQL: the
status column read under FOR UPDATE, SQL_PARK_ILLEGAL against the real events_dlq (payload_hash unique, history
check), and that the current row, event_log and the outbox stay untouched in a transaction that still commits.
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal

import pytest

from src.db.epoch import Epoch, EngineDb, bump_epoch
from src.db.migrate import migrate
from src.db.pg import AsyncpgSource, create_asyncpg_source
from src.db.repository import IllegalTransitionParked, PgStateStore
from src.dto import Decision, OrderDTO, OrderStatus, PositionDTO
from src.pubsub.dlq_rows import ILLEGAL_TRANSITION, dlq_row

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
pytestmark = [pytest.mark.pg, pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")]

EPOCH = Epoch("it-cluster", 5)
TABLES = "engine_epoch, leader_terms, orders_state, positions_state, event_log, outbox_events, events_dlq"


@pytest.fixture
async def source() -> AsyncIterator[AsyncpgSource]:
    assert DSN is not None
    src = await create_asyncpg_source(DSN, application_name="xtnl:5:it")
    async with src.acquire() as conn:
        await migrate(conn)
        await conn.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
        await bump_epoch(conn, EPOCH, "it-node", terminate_older=False)
    try:
        yield src
    finally:
        await src.close()


def order(**kw: object) -> OrderDTO:
    base: dict[str, object] = dict(client_order_id=None, broker_order_id="TG1", account_id="ACC", adapter_id="it",
                                   symbol="EURUSD", side="BUY", status=OrderStatus.FILLED, entry_price=Decimal("1.1"),
                                   ordered_units=Decimal(1), remaining_units=Decimal(0), broker_updated_at_ms=1,
                                   observed_seq=1, seq_id=(1 << 40))
    base.update(kw)
    return OrderDTO(**base)  # type: ignore[arg-type]


async def _counts(source: AsyncpgSource) -> tuple[int, int, int]:
    async with source.acquire() as conn:
        ev = await conn.fetchrow("SELECT count(*) FROM event_log")
        ob = await conn.fetchrow("SELECT count(*) FROM outbox_events")
        dl = await conn.fetchrow("SELECT count(*) FROM events_dlq")
    assert ev is not None and ob is not None and dl is not None
    return int(ev[0]), int(ob[0]), int(dl[0])


async def test_filled_order_is_never_reopened_by_a_newer_row(source: AsyncpgSource) -> None:
    store = PgStateStore(EngineDb(source, lambda: EPOCH))
    filled = order()
    assert (await store.commit_entity(filled)).decision is Decision.APPLY
    before = await _counts(source)
    reopened = order(status=OrderStatus.WORKING, remaining_units=Decimal(1), observed_seq=2, seq_id=(1 << 40) + 1)
    with pytest.raises(IllegalTransitionParked) as err:
        await store.commit_entity(reopened)
    assert (err.value.stored_status, err.value.new_status) == ("FILLED", "WORKING")
    async with source.acquire() as conn:
        cur = await conn.fetchrow("SELECT status, seq_id FROM orders_state WHERE adapter_id = 'it' "
                                  "AND broker_order_id = 'TG1'")
        park = await conn.fetchrow("SELECT error_reason, payload_hash, event_type, entity_id FROM events_dlq")
    assert cur is not None and (str(cur[0]), int(cur[1])) == ("FILLED", 1 << 40)
    expected = dlq_row(reopened, ILLEGAL_TRANSITION)
    assert park is not None
    assert (park[0], str(park[1]).strip(), park[2], park[3]) == (ILLEGAL_TRANSITION, expected.payload_hash,
                                                                 "ORDER", "TG1")
    ev, ob, dl = await _counts(source)
    assert (ev, ob, dl) == (before[0], before[1], before[2] + 1)
    # the same refused row again: no second DLQ row (payload_hash), still raises
    with pytest.raises(IllegalTransitionParked):
        await store.commit_entity(reopened)
    assert (await _counts(source))[2] == dl
    # a legal same-status newer row still commits
    amended = replace(filled, observed_seq=3, seq_id=(1 << 40) + 2, entry_price=Decimal("1.2"))
    assert (await store.commit_entity(amended)).decision is Decision.APPLY


async def test_closed_position_is_never_reopened(source: AsyncpgSource) -> None:
    store = PgStateStore(EngineDb(source, lambda: EPOCH))
    closed = PositionDTO(position_id="TP1", account_id="ACC", adapter_id="it", symbol="EURUSD", side="BUY",
                         units=Decimal(0), entry_price=Decimal("1.1"), status="CLOSED", broker_updated_at_ms=1,
                         observed_seq=1, seq_id=1 << 40)
    assert (await store.commit_entity(closed)).decision is Decision.APPLY
    with pytest.raises(IllegalTransitionParked):
        await store.commit_entity(replace(closed, status="OPEN", units=Decimal(1), observed_seq=2,
                                          seq_id=(1 << 40) + 1))
    async with source.acquire() as conn:
        cur = await conn.fetchrow("SELECT status FROM positions_state WHERE position_id = 'TP1'")
    assert cur is not None and str(cur[0]) == "CLOSED"


async def test_dashboard_role_reads_every_view_and_writes_nothing(source: AsyncpgSource) -> None:
    """Migration 0011 against the real catalog: as `xtnl_dashboard` every dashboard statement runs, the
    dashboard's own role check counts 0 writable tables, and a write is refused by privilege."""
    from src.dashboard import views
    from src.dashboard.server import SQL_ROLE_CHECK

    async with source.acquire() as conn:
        await conn.execute("SET ROLE xtnl_dashboard")
        try:
            for sql in views.ALL_SQL:
                await conn.fetch(sql)
            row = await conn.fetchrow(SQL_ROLE_CHECK)
            assert row is not None and row[0] == "xtnl_dashboard" and row[1] is False and int(row[3]) == 0
            with pytest.raises(Exception, match="permission denied"):
                await conn.execute("UPDATE engine_epoch SET node = node")
        finally:
            await conn.execute("RESET ROLE")
        attrs = await conn.fetchrow("SELECT rolcanlogin, rolsuper, rolcreaterole, rolreplication, rolconnlimit, "
                                    "rolconfig::text FROM pg_roles WHERE rolname = 'xtnl_dashboard'")
    # rolcanlogin is not asserted: the role is cluster-wide and an operator may have enabled login already
    assert attrs is not None and attrs[1:5] == (False, False, False, 5)
    assert "default_transaction_read_only=on" in str(attrs[5]) and "statement_timeout=5s" in str(attrs[5])
