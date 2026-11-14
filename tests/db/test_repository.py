"""PgStateStore / TieredStateCommitter / PgDlqSink / stream health with a fake connection."""
from __future__ import annotations

import re
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

import fakeredis
import pytest

from src.cache import keys as K
from src.cache.scripts import RedisState, RedisStreamHealth, entity_write
from src.db import epoch as E
from src.db import repository as R
from src.db.entity_sql import ACCOUNTS, ORDERS, POSITIONS, EntityTable
from src.db.epoch import EngineDb
from src.db.fence import FENCE, SQL_CHECK_EPOCH, EpochMismatch
from src.dto import AccountDTO, CashFlowDTO, Decision, InstrumentSpec, OrderDTO, OrderStatus, PriceDTO
from src.ports import CommitOutcome, DlqRow

from tests.db.conftest import FakeConn

BOOT = "run:cluster-a:7"


def order(**kw: Any) -> OrderDTO:
    base: dict[str, Any] = dict(
        client_order_id=None, broker_order_id="B1", account_id="ACC", adapter_id="stonex", symbol="EURUSD",
        side="BUY", status=OrderStatus.WORKING, entry_price=Decimal("1.1"), stop_loss=Decimal("1.09"),
        ordered_units=Decimal(1000), remaining_units=Decimal(1000), broker_updated_at_ms=1_000, observed_seq=10,
        seq_id=100,
    )
    base.update(kw)
    return OrderDTO(**base)


def stored(dto: OrderDTO) -> tuple[Any, ...]:
    return (dto.broker_seq, dto.broker_updated_at_ms, dto.observed_seq, dto.seq_id, dto.checksum)


def tx_frame(conn: FakeConn) -> None:
    assert conn.sql[:1] == [E.SQL_BEGIN_RC]
    assert conn.sql[-2:] == [E.SQL_COMMIT_TIMEOUT_OFF, E.SQL_COMMIT]


def placeholders(sql: str) -> set[int]:
    return {int(n) for n in re.findall(r"\$(\d+)", sql)}


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    await r.set(K.BOOT_ID, BOOT)
    yield r
    await r.aclose()


# ---- generated SQL ------------------------------------------------------------------------------

@pytest.mark.parametrize("t", [ORDERS, POSITIONS, ACCOUNTS])
def test_generated_sql_placeholders_match_params(t: EntityTable) -> None:
    dto: Any = {
        "orders_state": order(),
        "positions_state": __import__("src.dto", fromlist=["PositionDTO"]).PositionDTO(
            position_id="P", account_id="A", adapter_id="x", symbol="S", side="BUY", units=Decimal(1),
            entry_price=Decimal(1), status="OPEN"),
        "account_state": AccountDTO(account_id="A", adapter_id="x", currency="USD", balance=Decimal(1)),
    }[t.table]
    w = entity_write(dto)
    for sql, params in ((t.insert_new, t.insert_params(dto, w)), (t.update_apply, t.apply_params(dto, w)),
                        (t.update_advance, t.advance_params(dto, w))):
        assert placeholders(sql) == set(range(1, len(params) + 2)), sql   # $1 = epoch
        assert FENCE in sql
    assert placeholders(t.select_for_update) == set(range(1, len(t.key_cols) + 1))
    assert t.select_for_update.endswith("FOR UPDATE") and FENCE not in t.select_for_update
    assert "checksum =" not in t.update_advance.split("WHERE")[0]
    assert ("status_units_changed_at_ms" in t.update_apply) == (t is not ACCOUNTS)


def test_payload_values_are_plain() -> None:
    vals = ORDERS.payload_values(order())
    assert "WORKING" in vals and None in vals  # enum -> value, nullable client_order_id


# ---- entity commits -----------------------------------------------------------------------------

async def test_new_entity_inserts_logs_and_outboxes(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(ORDERS.insert_new, (1,))
    res = await R.PgStateStore(db).commit_entity(order())
    assert res.decision is Decision.APPLY
    tx_frame(conn)
    body = conn.sql[1:-2]
    assert body == [ORDERS.select_for_update, ORDERS.insert_new, R.SQL_EVENT_LOG, R.SQL_OUTBOX]
    (log_args,) = conn.args_of(R.SQL_EVENT_LOG)
    assert log_args[0] == 7  # $1 = epoch, also written as epoch_revision
    assert log_args[2] == "ORDER" and log_args[3] == "order:stonex:B1" and log_args[6] == "APPLIED"
    (ins,) = conn.args_of(ORDERS.insert_new)
    assert ins[1:3] == ("stonex", "B1") and ins[-2] == order().checksum and ins[-1] == res.write.digest_hash


async def test_newer_version_updates_current_row(db: EngineDb, conn: FakeConn) -> None:
    old = order()
    conn.returns(ORDERS.select_for_update, stored(old))
    new = order(stop_loss=Decimal("1.08"), observed_seq=11, seq_id=101)
    res = await R.PgStateStore(db).commit_entity(new)
    assert res.decision is Decision.APPLY
    assert conn.sql[1:-2] == [ORDERS.select_for_update, ORDERS.update_apply, R.SQL_EVENT_LOG, R.SQL_OUTBOX]


async def test_same_checksum_only_advances_version(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(ORDERS.select_for_update, stored(order()))
    newer_same = order(observed_seq=50, seq_id=500)
    res = await R.PgStateStore(db).commit_entity(newer_same)
    assert res.decision is Decision.NOOP_ADVANCE
    assert conn.sql[1:-2] == [ORDERS.select_for_update, ORDERS.update_advance]
    (args,) = conn.args_of(ORDERS.update_advance)
    assert args[:3] == (7, "stonex", "B1") and res.write.version in args and 500 in args


async def test_lower_version_dropped_and_logged_only(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(ORDERS.select_for_update, stored(order(observed_seq=99)))
    res = await R.PgStateStore(db).commit_entity(order(observed_seq=5, stop_loss=Decimal(1)))
    assert res.decision is Decision.DROP_STALE
    assert conn.sql[1:-2] == [ORDERS.select_for_update, R.SQL_EVENT_LOG]
    assert conn.args_of(R.SQL_EVENT_LOG)[0][6] == "DROPPED_STALE"


async def test_identical_write_touches_nothing(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(ORDERS.select_for_update, stored(order()))
    res = await R.PgStateStore(db).commit_entity(order())
    assert res.decision is Decision.NOOP
    assert conn.sql[1:-2] == [ORDERS.select_for_update]


async def test_insert_race_recompares_with_winner(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(ORDERS.select_for_update, None, stored(order(observed_seq=99)))
    res = await R.PgStateStore(db).commit_entity(order())
    assert res.decision is Decision.DROP_STALE
    assert conn.sql[1:5] == [ORDERS.select_for_update, ORDERS.insert_new, SQL_CHECK_EPOCH, ORDERS.select_for_update]


async def test_account_apply_writes_snapshot_history(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(ACCOUNTS.insert_new, (1,))
    acct = AccountDTO(account_id="ACC", adapter_id="stonex", currency="USD", balance=Decimal("100.50"), seq_id=9)
    await R.PgStateStore(db).commit_entity(acct)
    (snap,) = conn.args_of(R.SQL_ACCOUNT_SNAPSHOT)
    assert snap[:6] == (7, "stonex", "ACC", 9, "USD", Decimal("100.50"))


async def test_epoch_mismatch_writes_nothing(db: EngineDb, conn: FakeConn) -> None:
    conn.stored_epoch = 8   # a newer leader: the fenced insert affects 0 rows, the re-check fails
    with pytest.raises(EpochMismatch):
        await R.PgStateStore(db).commit_entity(order())
    assert R.SQL_EVENT_LOG not in conn.sql and conn.sql[-1] == E.SQL_ROLLBACK and E.SQL_COMMIT not in conn.sql


async def test_epoch_mismatch_on_update_is_immediate(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(ORDERS.select_for_update, stored(order()))
    conn.stored_epoch = 8
    with pytest.raises(EpochMismatch):
        await R.PgStateStore(db).commit_entity(order(stop_loss=Decimal("1.05"), observed_seq=11))
    assert conn.sql[-2:] == [ORDERS.update_apply, E.SQL_ROLLBACK]   # `must`: no extra re-check read


async def test_cash_flow_on_fenced_node_raises(db: EngineDb, conn: FakeConn) -> None:
    conn.stored_epoch = 8
    cf = CashFlowDTO(transaction_id="T1", account_id="ACC", adapter_id="stonex", kind="DEPOSIT",
                     amount=Decimal("50"), currency="USD", broker_ts=5, seq_id=200)
    with pytest.raises(EpochMismatch):
        await R.PgStateStore(db).commit_cash_flow(cf)


async def test_cash_flow_new_and_duplicate(db: EngineDb, conn: FakeConn) -> None:
    cf = CashFlowDTO(transaction_id="T1", account_id="ACC", adapter_id="stonex", kind="DEPOSIT",
                     amount=Decimal("50"), currency="USD", broker_ts=5, seq_id=200)
    conn.returns(R.SQL_CASH_FLOW, (1,))
    store = R.PgStateStore(db)
    assert await store.commit_cash_flow(cf) is True
    assert R.SQL_EVENT_LOG in conn.sql and R.SQL_OUTBOX in conn.sql
    conn.calls.clear()
    assert await store.commit_cash_flow(cf) is False
    assert R.SQL_EVENT_LOG not in conn.sql


async def test_instrument_commit_only_when_changed(db: EngineDb, conn: FakeConn) -> None:
    spec = InstrumentSpec(adapter_id="stonex", symbol="EURUSD", base_ccy="EUR", quote_ccy="USD", pnl_ccy="USD",
                          contract_size=Decimal(1), unit_kind="UNITS", point_size=Decimal("0.0001"),
                          min_qty=Decimal(1), swap_booked_to_balance=True)
    store = R.PgStateStore(db)
    conn.returns(R.SQL_INSTRUMENT_UPSERT, (1,))
    assert (await store.commit_instrument(spec, 10)) is not None
    assert await store.commit_instrument(spec, 11) is None


async def test_max_seq_id(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(R.SQL_MAX_SEQ_ID, (12345,))
    assert await R.max_seq_id(db.source) == 12345
    assert await R.max_seq_id(db.source) == 0


# ---- tiered commit (PG fake + fakeredis Lua) ----------------------------------------------------

def tiered(db: EngineDb, redis: Any, boot: str = BOOT, clock: list[int] | None = None) -> R.TieredStateCommitter:
    cache = RedisState(redis, lambda: boot)
    clk = clock if clock is not None else [0]
    return R.TieredStateCommitter(R.PgStateStore(db), cache, lambda a, s: Decimal("1.0"), clock_ms=lambda: clk[0])


async def test_tiered_apply_copies_to_redis_after_commit(db: EngineDb, conn: FakeConn, redis: Any) -> None:
    conn.returns(ORDERS.insert_new, (1,))
    assert await tiered(db, redis).commit_entity(order()) is CommitOutcome.APPLIED
    assert conn.sql[-1] == E.SQL_COMMIT
    assert await redis.exists("order:stonex:B1") and await redis.xlen(K.FEED) == 1


async def test_tiered_advance_and_stale(db: EngineDb, conn: FakeConn, redis: Any) -> None:
    c = tiered(db, redis)
    conn.returns(ORDERS.insert_new, (1,))
    await c.commit_entity(order())
    conn.returns(ORDERS.select_for_update, stored(order()))
    assert await c.commit_entity(order(observed_seq=20, seq_id=200)) is CommitOutcome.NOOP_ADVANCED
    assert await redis.xlen(K.FEED) == 1
    conn.returns(ORDERS.select_for_update, stored(order(observed_seq=20)))
    assert await c.commit_entity(order(observed_seq=1, stop_loss=Decimal(1))) is CommitOutcome.STALE


async def test_tiered_no_boot_is_queued(db: EngineDb, conn: FakeConn, redis: Any) -> None:
    await redis.delete(K.BOOT_ID)
    c = tiered(db, redis)
    conn.returns(ORDERS.insert_new, (1,))
    assert await c.commit_entity(order()) is CommitOutcome.NO_BOOT
    assert len(c._cache.no_boot) == 1
    assert not await redis.exists("order:stonex:B1")


async def test_tiered_redis_down_after_commit_is_queued(db: EngineDb, conn: FakeConn) -> None:
    from redis.exceptions import ConnectionError as RedisConnError

    class DeadRedis:
        async def script_load(self, *_: Any) -> str:
            raise RedisConnError("down")

    cache = RedisState(DeadRedis(), lambda: BOOT)  # type: ignore[arg-type]
    c = R.TieredStateCommitter(R.PgStateStore(db), cache, lambda a, s: None)
    conn.returns(ORDERS.insert_new, (1,))
    assert await c.commit_entity(order()) is CommitOutcome.NO_BOOT
    assert len(cache.no_boot) == 1


async def test_tiered_cash_flow_feeds_account_entry(db: EngineDb, conn: FakeConn, redis: Any) -> None:
    cf = CashFlowDTO(transaction_id="T1", account_id="ACC", adapter_id="stonex", kind="WITHDRAWAL",
                     amount=Decimal("-5"), currency="USD", broker_ts=5, seq_id=3)
    conn.returns(R.SQL_CASH_FLOW, (1,))
    c = tiered(db, redis)
    assert await c.commit_cash_flow(cf) is True
    assert await c.commit_cash_flow(cf) is False
    entries = await redis.xrange(K.FEED)
    assert len(entries) == 1 and entries[0][1][b"entity_type"] == b"ACCOUNT"


async def test_write_live_price_persists_clean_mark_every_10s(db: EngineDb, conn: FakeConn, redis: Any) -> None:
    clock = [100_000]
    c = tiered(db, redis, clock=clock)

    def px(ts: int, spread: str = "0.5") -> PriceDTO:
        return PriceDTO(adapter_id="stonex", symbol="EURUSD", bid=Decimal("1.1"), ask=Decimal("1.1001"),
                        spread_pips=Decimal(spread), timestamp_ms=ts)

    await c.write_live(px(1))
    await c.write_live(px(2))
    assert len(conn.args_of(R.SQL_LAST_CLEAN_MARK)) == 1
    await c.write_live(px(3, spread="9"))      # wide: price only, no mark
    clock[0] += 10_000
    await c.write_live(px(4))
    assert [a[-1] for a in conn.args_of(R.SQL_LAST_CLEAN_MARK)] == [1, 4]
    assert await c.force_clean_marks("stonex") == 1
    assert len(conn.args_of(R.SQL_LAST_CLEAN_MARK)) == 3
    assert await redis.exists(K.price_key("stonex", "EURUSD"))


# ---- DLQ ----------------------------------------------------------------------------------------

def dlq(reason: str = "QueueFull", origin: int | None = None, h: str = "h1") -> DlqRow:
    return DlqRow("ORDER", "c1", "stonex", "{}", h, reason, origin_dlq_id=origin)


async def test_dlq_plain_rows_use_spill_drain_exception(db: EngineDb, conn: FakeConn) -> None:
    await R.PgDlqSink(db).insert_batch([dlq(), dlq("timeout", origin=3, h="h2")])
    assert conn.sql == [E.SQL_SPILL_DRAIN_INSERT]  # no epoch lock, transient re-park gets a fresh row


async def test_dlq_deterministic_reparks_bump_origin(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(R.SQL_DLQ_LINEAGE_BUMP, (1, False))
    await R.PgDlqSink(db).insert_batch([dlq("schema_invalid", origin=3)])
    assert conn.args_of(R.SQL_DLQ_LINEAGE_BUMP) == [(7, 3, "c1", "schema_invalid", "stonex", "h1")]
    assert E.SQL_SPILL_DRAIN_INSERT not in conn.sql
    assert "AND d.adapter_id = $5::text" in R.SQL_DLQ_LINEAGE_BUMP   # entity ids are per adapter


async def test_dlq_missing_client_order_id_is_deterministic(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(R.SQL_DLQ_LINEAGE_BUMP, (1, False))
    await R.PgDlqSink(db).insert_batch([dlq("missing_client_order_id", origin=4)])
    assert conn.args_of(R.SQL_DLQ_LINEAGE_BUMP) == [(7, 4, "c1", "missing_client_order_id", "stonex", "h1")]


async def test_dlq_lineage_without_origin_match_inserts(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(R.SQL_DLQ_LINEAGE_BUMP, (0, False))
    await R.PgDlqSink(db).insert_batch([dlq("checksum", origin=3)])
    assert conn.sql[-1] == E.SQL_SPILL_DRAIN_INSERT


async def test_dlq_lineage_bump_is_idempotent_per_payload_hash(db: EngineDb, conn: FakeConn) -> None:
    """5.5: a re-delivered batch (spooler retry after an unknown COMMIT) whose bump was already counted is
    neither counted again nor inserted as a fresh row."""
    conn.returns(R.SQL_DLQ_LINEAGE_BUMP, (1, False), (0, True))
    sink = R.PgDlqSink(db)
    await sink.insert_batch([dlq("checksum", origin=3)])
    await sink.insert_batch([dlq("checksum", origin=3)])      # the same payload again
    assert [a[-1] for a in conn.args_of(R.SQL_DLQ_LINEAGE_BUMP)] == ["h1", "h1"]
    assert E.SQL_SPILL_DRAIN_INSERT not in conn.sql
    sql = R.SQL_DLQ_LINEAGE_BUMP
    assert "INSERT INTO dlq_lineage_bumps" in sql and "ON CONFLICT (payload_hash) DO NOTHING" in sql
    assert "NOT EXISTS (SELECT 1 FROM seen)" in sql and FENCE in sql
    # payload_hash is char(64): the lookup casts the text parameter so the primary-key index is used
    assert "WHERE payload_hash = $6::text::bpchar)" in sql
    assert sql.index("ins AS") < sql.index("upd AS") and "WHERE dlq_id IN (SELECT origin_dlq_id FROM ins)" in sql


async def test_dlq_lineage_on_fenced_node_still_inserts(db: EngineDb, conn: FakeConn) -> None:
    conn.stored_epoch = 99
    await R.PgDlqSink(db).insert_batch([dlq("checksum", origin=3)])
    assert conn.sql[-1] == E.SQL_SPILL_DRAIN_INSERT


class PgDown(Exception):
    sqlstate = "08006"


@pytest.mark.parametrize("exc", [OSError("refused"), TimeoutError(), PgDown()])
async def test_dlq_connection_failure_raises_connection_error(db: EngineDb, conn: FakeConn, exc: Exception) -> None:
    conn.fail(E.SQL_SPILL_DRAIN_INSERT, exc)
    with pytest.raises(ConnectionError):
        await R.PgDlqSink(db).insert_batch([dlq()])


async def test_dlq_statement_error_is_not_masked(db: EngineDb, conn: FakeConn) -> None:
    class Bad(Exception):
        sqlstate = "22P02"

    conn.fail(E.SQL_SPILL_DRAIN_INSERT, Bad())
    with pytest.raises(Bad):
        await R.PgDlqSink(db).insert_batch([dlq()])


# ---- stream health ------------------------------------------------------------------------------

async def test_pg_stream_health(db: EngineDb, conn: FakeConn) -> None:
    pg = R.PgStreamHealth(db)
    await pg.mark_lost("stonex", "orders")
    tx_frame(conn)
    assert conn.args_of(R.SQL_STREAM_LOST) == [(7, "stonex", "orders")]
    conn.calls.clear()
    await pg.mark_reconciled("stonex", "orders")
    assert conn.args_of(R.SQL_STREAM_RECONCILED) == [(7, "stonex", "orders")]
    assert "reconciled_epoch" in R.SQL_STREAM_RECONCILED and "$1::bigint, now_ms()" in R.SQL_STREAM_RECONCILED
    conn.calls.clear()
    await pg.heartbeat("stonex", "orders")
    assert conn.calls == []
    assert "COALESCE(stream_health.conn_lost_at" in R.SQL_STREAM_LOST


async def test_tiered_stream_health(db: EngineDb, conn: FakeConn, redis: Any) -> None:
    sh = R.TieredStreamHealth(R.PgStreamHealth(db), RedisStreamHealth(RedisState(redis, lambda: BOOT), lambda: 5))
    await sh.heartbeat("stonex", "prices")
    assert await redis.exists(K.conn_key("stonex", "prices"))
    await sh.mark_lost("stonex", "prices")
    assert not await redis.exists(K.conn_key("stonex", "prices"))
    assert R.SQL_STREAM_LOST in conn.sql
    await sh.mark_reconciled("stonex", "prices")
    assert await redis.exists(K.conn_key("stonex", "prices"))



async def test_stream_health_on_fenced_node_raises(db: EngineDb, conn: FakeConn) -> None:
    conn.stored_epoch = 8
    with pytest.raises(EpochMismatch):
        await R.PgStreamHealth(db).mark_lost("stonex", "orders")


def spec(symbol: str = "EURUSD") -> InstrumentSpec:
    return InstrumentSpec(adapter_id="stonex", symbol=symbol, base_ccy="EUR", quote_ccy="USD", pnl_ccy="USD",
                          contract_size=Decimal(1), unit_kind="UNITS", point_size=Decimal("0.0001"),
                          min_qty=Decimal(1), swap_booked_to_balance=True)


async def test_instrument_batch_is_one_transaction(db: EngineDb, conn: FakeConn) -> None:
    conn.returns(R.SQL_INSTRUMENT_UPSERT, (1,), None)
    writes = await R.PgStateStore(db).commit_instruments([spec("EURUSD"), spec("EURGBP")], 77)
    assert [w is not None for w in writes] == [True, False]
    assert conn.sql.count(E.SQL_BEGIN_RC) == 1 and conn.sql.count(R.SQL_EVENT_LOG) == 1
    assert await R.PgStateStore(db).commit_instruments([], 1) == []


async def test_tiered_upsert_instruments_copies_changed(db: EngineDb, conn: FakeConn, redis: Any) -> None:
    c = tiered(db, redis)
    conn.returns(R.SQL_INSTRUMENT_UPSERT, (1,))
    assert await c.upsert_instruments([spec()], 5) == 1
    assert await redis.exists(K.instrument_key("stonex", "EURUSD"))
    assert await redis.xlen(K.FEED) == 0          # instrument: keys are never fed
    assert await c.commit_instrument(spec(), 6) is False
    assert len(c.no_boot) == 0


async def test_clean_mark_fence_loss_propagates(db: EngineDb, conn: FakeConn, redis: Any) -> None:
    conn.stored_epoch = 8
    c = tiered(db, redis)
    px = PriceDTO(adapter_id="stonex", symbol="EURUSD", bid=Decimal("1.1"), ask=Decimal("1.1001"),
                  spread_pips=Decimal("0.5"), timestamp_ms=1)
    with pytest.raises(EpochMismatch):
        await c.write_live(px)


def test_every_write_statement_is_fenced() -> None:
    for name in dir(R):
        sql = getattr(R, name)
        if name.startswith("SQL_") and isinstance(sql, str) and sql.split()[0] in ("INSERT", "UPDATE", "DELETE"):
            assert FENCE in sql, name
            assert 1 in placeholders(sql), name
