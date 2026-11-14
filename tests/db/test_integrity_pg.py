"""PgIntegrityDb / PgDlqStore against a real PostgreSQL (skipped unless ORDER_SERVER_TEST_DSN points at a
DISPOSABLE database: the tests truncate the engine tables). The role needs EXECUTE on pg_control_checkpoint()
(superuser or pg_monitor) for high_water()."""
from __future__ import annotations

import hashlib
import os
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest

from src.cache.digest import fold
from src.db.epoch import EngineDb, Epoch, bump_epoch
from src.db.fence import EpochMismatch
from src.db.integrity_store import PgDlqStore, PgIntegrityDb
from src.db.migrate import migrate
from src.db.pg import AsyncpgSource, create_asyncpg_source
from src.db.repository import PgStateStore
from src.dto import AccountDTO, InstrumentSpec, OrderDTO, OrderStatus, PositionDTO
from src.integrity.dbcheck import check_row
from src.integrity.ports import DlqStatus
from src.oversee.dayroll import trading_day
from src.ports import DlqRow

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
pytestmark = [pytest.mark.pg, pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")]

TABLES = ("engine_epoch, orders_state, positions_state, account_state, account_snapshots, instruments, event_log, "
          "outbox_events, halt_events, cash_flows, events_dlq, events_quarantine, sod_marks, last_clean_marks")
EPOCH = Epoch("it-cluster", 5)
SEQ = 1 << 40
T = 1_794_600_000_000


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


def dlq(entity_id: str, reason: str, payload: str = "{}") -> DlqRow:
    raw = f"{entity_id}:{reason}:{payload}"
    return DlqRow("ORDER" if not payload.startswith('{"broker_ts"') else "CASH_FLOW", entity_id, "it", payload,
                  hashlib.sha256(raw.encode()).hexdigest(), reason)


async def test_integrity_reads_rebuild_committed_rows(source: AsyncpgSource) -> None:
    db = EngineDb(source, lambda: EPOCH)
    store = PgStateStore(db)
    o = OrderDTO(client_order_id=None, broker_order_id="IT1", account_id="ACC", adapter_id="it", symbol="EURUSD",
                 side="BUY", status=OrderStatus.WORKING, entry_price=Decimal("1.1000"), ordered_units=Decimal(1),
                 remaining_units=Decimal(1), broker_updated_at_ms=1, observed_seq=1, seq_id=SEQ)
    p = PositionDTO(position_id="P1", account_id="ACC", adapter_id="it", symbol="EURUSD", side="SELL",
                    units=Decimal("2.50"), entry_price=Decimal("1.2"), status="OPEN", observed_seq=1, seq_id=SEQ + 1)
    a = AccountDTO(account_id="ACC", adapter_id="it", currency="USD", balance=Decimal("100.25"), seq_id=SEQ + 2)
    for item in (o, p, a):
        await store.commit_entity(item)
    await store.commit_instruments([InstrumentSpec(
        adapter_id="it", symbol="EURUSD", base_ccy="EUR", quote_ccy="USD", pnl_ccy="USD", contract_size=Decimal(1),
        unit_kind="UNITS", point_size=Decimal("0.0001"), min_qty=Decimal(1), swap_booked_to_balance=True)], SEQ + 3)

    idb = PgIntegrityDb(source)
    rows = await idb.rows_after_key("", 100)
    assert [r.key for r in rows] == ["account:it:ACC", "instrument:it:EURUSD", "order:it:IT1", "position:it:P1"]
    assert all(check_row(r) == [] for r in rows)
    assert await idb.digest() == fold(r.digest_hash for r in rows)
    latest = await idb.latest_commit_ms()
    assert latest is not None
    window = await idb.committed_rows(0, latest, 100)
    assert sorted(r.key for r in window) == [r.key for r in rows]
    by_key = await idb.rows_by_keys(["order:it:IT1", "position:it:P1", "order:it:NOPE"])
    assert set(by_key) == {"order:it:IT1", "position:it:P1"} and by_key["order:it:IT1"].entity == o
    hw = await idb.high_water()
    assert hw.epoch == 5 and hw.max_seq_id >= SEQ + 2 and hw.timeline >= 1
    assert await idb.latest_halt_event() is None


async def test_dlq_store_round_trip(source: AsyncpgSource) -> None:
    db = EngineDb(source, lambda: EPOCH)
    await db.spill_drain_insert([dlq("B1", "checksum"), dlq("B2", "QueueFull"),
                                 dlq("T1", "QueueFull", '{"broker_ts": 123, "account_id": "ACC"}')])
    s = PgDlqStore(db)
    async with source.acquire() as conn:
        row = await conn.fetchrow("SELECT now_ms()")
    assert row is not None
    now = int(row[0]) + 1_000
    leased = await s.lease("w1", now, 30_000, {"it"}, 10)
    assert [r.entity_id for r in leased] == ["B1", "B2", "T1"] and all(r.status is DlqStatus.LEASED for r in leased)
    b1, b2, t1 = (r.dlq_id for r in leased)
    assert not await s.resolve(b1, "w2") and await s.resolve(b1, "w1")
    assert await s.backoff(b2, "w1", 1, now + 2_000, "read failed")
    assert await s.cash_flow_since_ms("it") == 123
    assert await s.quarantine(t1, "w1", "not found x5")
    origin = await s.find_lineage_origin("ORDER", "B1", "checksum", b1 + 100, now - 60_000, adapter_id="it")
    assert origin is not None and origin.dlq_id == b1
    await db.spill_drain_insert([dlq("B1", "checksum", '{"again": 1}')])
    child = (await s.lease("w1", now + 5_000, 30_000, {"it"}, 10))
    child_id = next(r.dlq_id for r in child if r.entity_id == "B1")
    assert await s.merge_into_origin(child_id, "w1", b1, 1, now + 9_000)
    async with source.acquire() as conn:
        o = await conn.fetchrow("SELECT status, retry_count FROM events_dlq WHERE dlq_id = $1", b1)
        q = await conn.fetchrow("SELECT account_id, error_reason FROM events_quarantine WHERE dlq_id = $1", t1)
    assert o == ("BACKOFF", 1) and q == ("ACC", "QueueFull: not found x5")
    assert await s.release_expired(now + 10_000_000) >= 1
    await s.mark_transfers_unverified("it", "ACC", T)
    async with source.acquire() as conn:
        f = await conn.fetchrow("SELECT transfers_unverified, sod_equity FROM sod_marks WHERE adapter_id = 'it' "
                                "AND account_id = 'ACC' AND trading_day = $1", trading_day(T))
    assert f == (True, None)
    fenced = PgDlqStore(EngineDb(source, lambda: Epoch("it-cluster", 4)))
    with pytest.raises(EpochMismatch):
        await fenced.release_expired(now)
