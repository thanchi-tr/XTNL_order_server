"""Real PostgreSQL integration (skipped unless ORDER_SERVER_TEST_DSN points at a DISPOSABLE database: the
tests truncate the engine tables)."""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal

import pytest

from src.db.epoch import EngineDb, Epoch, EpochFenced, bump_epoch
from src.db.fence import EpochMismatch
from src.db.halt_store import PgHaltStore
from src.db.migrate import migrate
from src.db.notify_store import PgDedupeStore, PgOutboxStore
from src.db.pg import AsyncpgSource, create_asyncpg_source
from src.db.repository import PgStateStore, PgStreamHealth
from src.db.snapshot_reader import PgSnapshotReader
from src.db.sweep_writer import PgSweepWriter
from src.dto import AccountDTO, Decision, OrderDTO, OrderStatus
from src.oversee.halt import HaltSwitch
from src.oversee.notify import OutboxRow, episode_key
from src.oversee.sweep import AccountWrite, CasConflict, StateWrite
from src.oversee.types import BreachState, HaltLevel, Level

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
pytestmark = [pytest.mark.pg, pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")]

TABLES = ("engine_epoch, leader_terms, orders_state, positions_state, account_state, account_snapshots, event_log, "
          "outbox_events, halt_breaches, breach_acks, halt_events, oversee_events, notification_outbox, "
          "notification_dedupe, stop_watch, stream_health, sod_marks, sweep_quarantine, cash_flows, instruments")
EPOCH = Epoch("it-cluster", 5)


@pytest.fixture
async def source() -> AsyncIterator[AsyncpgSource]:
    assert DSN is not None
    src = await create_asyncpg_source(DSN, application_name="xtnl:5:it")
    async with src.acquire() as conn:
        await migrate(conn)
        assert await migrate(conn) == []   # idempotent
        await conn.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
        await bump_epoch(conn, EPOCH, "it-node", terminate_older=False)
    try:
        yield src
    finally:
        await src.close()


def order(**kw: object) -> OrderDTO:
    base: dict[str, object] = dict(client_order_id=None, broker_order_id="IT1", account_id="ACC", adapter_id="it",
                                   symbol="EURUSD", side="BUY", status=OrderStatus.WORKING, entry_price=Decimal("1.1"),
                                   ordered_units=Decimal(1), remaining_units=Decimal(1), broker_updated_at_ms=1,
                                   observed_seq=1, seq_id=(1 << 40))
    base.update(kw)
    return OrderDTO(**base)  # type: ignore[arg-type]


async def test_bump_only_increases(source: AsyncpgSource) -> None:
    async with source.acquire() as conn:
        with pytest.raises(EpochFenced):
            await bump_epoch(conn, Epoch("it-cluster", 4), "old", terminate_older=False)
        with pytest.raises(EpochFenced):
            await bump_epoch(conn, Epoch("other", 9), "dr", terminate_older=False)
        row = await conn.fetchrow("SELECT epoch, revision FROM engine_epoch WHERE id = 1")
        assert row == (5, 5)


async def test_entity_cas_and_fencing(source: AsyncpgSource) -> None:
    store = PgStateStore(EngineDb(source, lambda: EPOCH))
    o = order()
    assert (await store.commit_entity(o)).decision is Decision.APPLY
    assert (await store.commit_entity(o)).decision is Decision.NOOP
    assert (await store.commit_entity(replace(o, observed_seq=0, stop_loss=Decimal(1)))).decision is Decision.DROP_STALE
    newer = order(status=OrderStatus.PARTIALLY_FILLED, remaining_units=Decimal("0.5"), observed_seq=2, seq_id=(1 << 40) + 1)
    assert (await store.commit_entity(newer)).decision is Decision.APPLY
    acct = AccountDTO(account_id="ACC", adapter_id="it", currency="USD", balance=Decimal("100.25"), seq_id=(1 << 40) + 2)
    assert (await store.commit_entity(acct)).decision is Decision.APPLY
    fenced = PgStateStore(EngineDb(source, lambda: Epoch("it-cluster", 4)))
    with pytest.raises(EpochMismatch):
        await fenced.commit_entity(replace(o, observed_seq=9, seq_id=(1 << 40) + 9, stop_loss=Decimal("1.0")))
    async with source.acquire() as conn:
        n = await conn.fetchrow("SELECT count(*) FROM event_log")
        assert n is not None and n[0] == 4   # 3 applied + 1 dropped-stale, nothing from the fenced node
    await PgStreamHealth(EngineDb(source, lambda: EPOCH)).mark_lost("it", "orders")


async def test_oversee_stores_round_trip(source: AsyncpgSource) -> None:
    db = EngineDb(source, lambda: EPOCH)
    await PgStateStore(db).commit_entity(AccountDTO(account_id="ACC", adapter_id="it", currency="USD",
                                                    balance=Decimal(100), seq_id=1 << 40))
    writer = PgSweepWriter(db)
    s = BreachState("max_position_risk", "order", "it:IT1", "ACC", Level.SOFT_HALT, reason="r", active_since=1,
                    activation_id=10, last_sweep_id=10)
    await writer.write_account(AccountWrite("ACC", 5, 10, states=(StateWrite(s, None, None),),
                                            stop_watch_set=((("order", "it:IT1"), 100),)))
    with pytest.raises(CasConflict):
        await writer.write_account(AccountWrite("ACC", 5, 11, states=(StateWrite(s, 9, 0),)))
    snap = await PgSnapshotReader(db, None).read_account("ACC", 1_794_600_000_000)
    assert [b.key for b in snap.breaches] == [s.key] and dict(snap.stop_watch) == {("order", "it:IT1"): 100}
    hs = HaltSwitch(PgHaltStore(db))
    ev = await hs.trip(HaltLevel.SOFT, rule_id="r", reason="soft", now_ms=1_000)
    assert ev is not None and ev.id == 1 and (await hs.status()).effective_level is HaltLevel.SOFT
    outbox, dedupe = PgOutboxStore(db), PgDedupeStore(db)
    key = episode_key("r", "order", "it:IT1", 10, Level.SOFT_HALT)
    await outbox.add([OutboxRow("episode", "high", "t", "b", 5, key, "ACC")])
    pending = await outbox.pending(10)
    assert any(r.episode == key for r in pending)
    await dedupe.record(key, 7)
    assert await dedupe.last_sent(key) == 7
