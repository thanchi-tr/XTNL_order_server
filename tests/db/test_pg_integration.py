"""Real PostgreSQL integration (skipped unless ORDER_SERVER_TEST_DSN points at a DISPOSABLE database: the
tests truncate the engine tables)."""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from typing import Any

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


async def test_0015_arm_prereq_columns_round_trip(source: AsyncpgSource) -> None:
    """S01: order_kind and the instrument metadata persist and decode; event_log origin / broker_time_ms stay NULL;
    the CHECKs refuse unknown values; sod_marks.position_marks is gone."""
    from src.db.rows import INSTRUMENT_COLS, ORDER_COLS, instrument_from_row, order_from_row
    from src.dto import InstrumentSpec

    store = PgStateStore(EngineDb(source, lambda: EPOCH))
    o = order(order_kind="STOP")
    assert (await store.commit_entity(o)).decision is Decision.APPLY
    spec = InstrumentSpec(adapter_id="it", symbol="SYN.A", base_ccy="AAA", quote_ccy="BBB", pnl_ccy="BBB",
                          contract_size=Decimal(1), unit_kind="UNITS", point_size=Decimal("0.5"), min_qty=Decimal(1),
                          swap_booked_to_balance=False, price_increment=Decimal("0.25"), asset_class="SYNTHETIC")
    assert await store.commit_instrument(spec, (1 << 40) + 5) is not None
    async with source.acquire() as conn:
        row = await conn.fetchrow(f"SELECT {ORDER_COLS} FROM orders_state WHERE broker_order_id = 'IT1'")
        assert row is not None and order_from_row(row) == o and order_from_row(row).checksum == o.checksum
        irow = await conn.fetchrow(f"SELECT {INSTRUMENT_COLS} FROM instruments WHERE symbol = 'SYN.A'")
        assert irow is not None and instrument_from_row(irow) == spec
        logs = await conn.fetch("SELECT origin, broker_time_ms FROM event_log")
        assert len(logs) == 2 and all(tuple(r) == (None, None) for r in logs)
        with pytest.raises(Exception, match="orders_state_order_kind_check"):
            await conn.execute("UPDATE orders_state SET order_kind = 'MARKET'")
        with pytest.raises(Exception, match="event_log_origin_check"):
            await conn.execute("INSERT INTO event_log (seq_id, entity_type, entity_key, adapter_id, action, payload, "
                               "epoch_revision, origin) VALUES (1, 'ORDER', 'k', 'it', 'APPLIED', '{}', 5, 'bogus')")
        gone = await conn.fetchrow("SELECT count(*) FROM information_schema.columns "
                                   "WHERE table_name = 'sod_marks' AND column_name = 'position_marks'")
        assert gone is not None and gone[0] == 0


async def test_0016_outbox_checks_index_and_post_step(source: AsyncpgSource) -> None:
    """S08: PG17 accepts the NOT VALID CHECKs on both partitioned tables (event_log 0015, notification_outbox 0016);
    the migrate post step (run by the fixture's migrate) validated them and built + attached the ARM due index on
    every partition, so the parent index is valid; a partition created later gets it automatically; a second post
    step does nothing; the CHECK table holds on the real server."""
    from src.db.migrate import POST_STEP, post_step
    from tests.db.test_outbox_arm_migration import CHECK_TABLE

    async with source.acquire() as conn:
        for table, name in (("event_log", "event_log_origin_check"),
                            ("notification_outbox", "notification_outbox_arm_channels_check")):
            row = await conn.fetchrow("SELECT convalidated FROM pg_constraint WHERE conrelid = $1::regclass "
                                      "AND conname = $2", table, name)
            assert row is not None and row[0] is True, name
        parent = await conn.fetchrow("SELECT indisvalid FROM pg_index "
                                     "WHERE indexrelid = 'notification_outbox_arm_due_idx'::regclass")
        assert parent is not None and parent[0] is True
        await conn.execute("SELECT xtnl_ensure_month_partition('notification_outbox', '2031-01-01'::date, 0)")
        parts = await conn.fetch("SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                                 "WHERE i.inhparent = 'notification_outbox'::regclass")
        attached = await conn.fetch("SELECT x.indrelid::regclass::text FROM pg_inherits i JOIN pg_index x "
                                    "ON x.indexrelid = i.inhrelid "
                                    "WHERE i.inhparent = 'notification_outbox_arm_due_idx'::regclass")
        assert {str(r[0]) for r in parts} == {str(r[0]) for r in attached}
        assert "notification_outbox_default" in {str(r[0]) for r in parts}
        assert await post_step(conn, {15, 16}, items=POST_STEP) == []      # idempotent
        for kind, channels, accepted in CHECK_TABLE:
            if channels is None:
                continue                                   # channels is NOT NULL: a NULL array never reaches the CHECK
            await conn.execute("BEGIN")
            try:
                ok = True
                try:
                    await conn.execute("INSERT INTO notification_outbox (severity, subject, body, kind, channels) "
                                       "VALUES ('INFO', 's', 'b', $1, $2::text[])", kind, channels)
                except Exception as exc:
                    assert "notification_outbox_arm_channels_check" in str(exc), exc
                    ok = False
                assert ok is accepted, (kind, channels)
            finally:
                await conn.execute("ROLLBACK")


async def test_savepoint_rollback_keeps_the_outer_transaction_on_postgres(source: AsyncpgSource) -> None:
    db = EngineDb(source, lambda: EPOCH)
    async with db.transaction() as tx:
        await tx.execute("INSERT INTO notification_outbox (severity, subject, body) VALUES ('INFO', 'outer', 'b')")
        with pytest.raises(RuntimeError):
            async with tx.savepoint():
                await tx.execute("INSERT INTO notification_outbox (severity, subject, body) "
                                 "VALUES ('INFO', 'inner', 'b')")
                raise RuntimeError("inner fails")
        with pytest.raises(Exception, match="notification_outbox_arm_channels_check"):
            async with tx.savepoint():           # a failed statement inside the savepoint does not abort the outer
                await tx.execute("INSERT INTO notification_outbox (severity, subject, body, kind, channels) "
                                 "VALUES ('INFO', 'bad', 'b', 'arm', '{}')")
    async with source.acquire() as conn:
        rows = await conn.fetch("SELECT subject FROM notification_outbox ORDER BY outbox_id")
    assert [str(r[0]) for r in rows] == ["outer"]


async def test_post_step_validates_the_outbox_check_over_existing_oversee_rows(source: AsyncpgSource) -> None:
    """Rows the Oversee writers produced before 0016 (plain, retried and escalated channels) pass the validation."""
    import re as _re

    from src.db.migrate import POST_STEP, discover, post_step

    sql = next(m for m in discover() if m.version == 16).sql
    add = _re.search(r"(ALTER TABLE notification_outbox ADD CONSTRAINT notification_outbox_arm_channels_check.*?"
                     r"NOT VALID);", sql, _re.S)
    assert add is not None
    db = EngineDb(source, lambda: EPOCH)
    await PgOutboxStore(db).add([OutboxRow("info", "info", "a", "b", 5), OutboxRow("trip", "critical", "c", "d", 6)])
    async with source.acquire() as conn:
        await conn.execute("UPDATE notification_outbox SET channels = ARRAY['email:6', 'mobile_push', '+escalated'] "
                           "WHERE subject = 'c'")
        await conn.execute("ALTER TABLE notification_outbox DROP CONSTRAINT notification_outbox_arm_channels_check")
        await conn.execute(add.group(1))
        assert await post_step(conn, {16}, items=POST_STEP) == [
            "validated notification_outbox.notification_outbox_arm_channels_check"]
        assert await post_step(conn, {16}, items=POST_STEP) == []


async def _denied(conn: object, sql: str, *args: object) -> bool:
    """Run `sql` inside a savepoint; True when PostgreSQL refuses it with insufficient_privilege (42501)."""
    from src.db.pg import sqlstate

    c: Any = conn
    await c.execute("SAVEPOINT s23")
    try:
        await c.execute(sql, *args)
    except Exception as exc:
        await c.execute("ROLLBACK TO SAVEPOINT s23")
        assert sqlstate(exc) == "42501", exc
        return True
    await c.execute("RELEASE SAVEPOINT s23")
    return False


async def test_0017_arm_schema_role_grants_and_post_step(source: AsyncpgSource) -> None:
    """S23: 0017 applied (twice, by the fixture) and its post step run: the ARM tables with their month partitions
    -1..2 and DEFAULT, the event_log index valid and attached on every partition, the kind CHECK equal to
    REQUEST_KINDS, and the xtnl_arm privileges read from the catalog equal to the §B3 list; as xtnl_arm an insert of
    a failure works while setting cleared_at_ms, updating the outbox or reading operator_requests is refused."""
    from datetime import UTC, datetime

    from src.db import arm_schema as A
    from src.db.lifecycle import add_months, month_start
    from src.db.migrate import POST_STEP, post_step
    from src.db.operator_requests import REQUEST_KINDS

    async with source.acquire() as conn:
        c: Any = conn
        base = month_start(datetime.now(UTC).date())
        for table in A.ARM_PARTITIONED:
            rows = await c.fetch("SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                                 "WHERE i.inhparent = $1::regclass", table)
            names = {str(r[0]) for r in rows}
            want = {f"{table}_p{add_months(base, m):%Y%m}" for m in A.ARM_PARTITION_MONTHS} | {f"{table}_default"}
            assert want <= names, (table, want - names)
        idx = await c.fetchrow("SELECT indisvalid FROM pg_index WHERE indexrelid = 'event_log_broker_time_idx'::regclass")
        assert idx is not None and idx[0] is True
        parts = await c.fetch("SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                              "WHERE i.inhparent = 'event_log'::regclass")
        attached = await c.fetch("SELECT x.indrelid::regclass::text FROM pg_inherits i JOIN pg_index x "
                                 "ON x.indexrelid = i.inhrelid WHERE i.inhparent = 'event_log_broker_time_idx'::regclass")
        assert {str(r[0]) for r in parts} == {str(r[0]) for r in attached}
        assert await post_step(c, {15, 16, 17}, items=POST_STEP) == []          # idempotent
        kind = await c.fetchrow("SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                                "WHERE conname = 'operator_requests_kind_check'")
        assert kind is not None
        import re as _re
        assert set(_re.findall(r"'([a-z_]+)'::text", str(kind[0]))) == set(REQUEST_KINDS)

        tables = [(str(r[0]), str(r[1])) for r in await c.fetch(A.SQL_ROLE_TABLE_PRIVILEGES, A.ARM_ROLE)]
        columns = [(str(r[0]), str(r[1]), str(r[2])) for r in await c.fetch(A.SQL_ROLE_COLUMN_PRIVILEGES, A.ARM_ROLE)]
        sequences = [(str(r[0]), str(r[1])) for r in await c.fetch(A.SQL_ROLE_SEQUENCE_PRIVILEGES, A.ARM_ROLE)]
        assert A.privilege_problems(tables, columns, sequences) == []
        attrs = await c.fetchrow(A.SQL_ROLE_ATTRIBUTES, A.ARM_ROLE)
        assert attrs is not None and tuple(attrs) == (False, False, False, False, False, False)
        dash = [(str(r[0]), str(r[1])) for r in await c.fetch(A.SQL_ROLE_TABLE_PRIVILEGES, A.DASHBOARD_ROLE)]
        assert {t for t, _ in dash if t.startswith("arm_")} == set(A.ARM_DASHBOARD_VIEWS)
        assert {p for t, p in dash if t.startswith("arm_")} == {"SELECT"}
        api = await c.fetch(A.SQL_ROLE_TABLE_PRIVILEGES, "xtnl_api")
        assert not [r for r in api if str(r[0]).startswith("arm_")]

        await c.execute("BEGIN")
        try:
            await c.execute("SET LOCAL ROLE xtnl_arm")
            await c.execute(
                "INSERT INTO arm_failures (failure_id, scope_kind, scope_id, reason, probe_kind, ping_id, "
                "first_fault_at_ms, entered_at_ms, epoch) "
                "SELECT 'f1', 'ARM', 'arm', 'stuck_frontier', 'span_write', 'p1', 1, 2, $1::bigint "
                "WHERE (SELECT epoch FROM engine_epoch WHERE id = 1) = $1::bigint", 5)
            await c.execute("UPDATE arm_failures SET ping_count = 2, last_ping_at_ms = 3 WHERE failure_id = 'f1'")
            assert await _denied(c, "UPDATE arm_failures SET cleared_at_ms = 4, clear_request_id = 1 "
                                    "WHERE failure_id = 'f1'")
            assert await _denied(c, "INSERT INTO arm_failures (failure_id, scope_kind, scope_id, reason, probe_kind, "
                                    "ping_id, first_fault_at_ms, entered_at_ms, epoch, cleared_at_ms, "
                                    "clear_request_id) VALUES ('f9', 'ARM', 'arm9', 'stuck_frontier', 'span_write', "
                                    "'p9', 1, 2, 5, 3, 1)")
            await c.execute("INSERT INTO notification_outbox (severity, subject, body) VALUES ('CRITICAL', 's', 'b')")
            assert await _denied(c, "UPDATE notification_outbox SET attempts = 1")
            assert await _denied(c, "SELECT 1 FROM operator_requests")
            assert await _denied(c, "DELETE FROM arm_failures")
            assert await _denied(c, "INSERT INTO arm_controls (id) VALUES (1) ON CONFLICT DO NOTHING")
            now_ms = int(datetime.now(UTC).timestamp() * 1000)
            row = await c.fetchrow(
                "INSERT INTO arm_decisions (bar_end_ms, adapter_id, position_id, resource, owner, bar_spec, event, "
                "decision_seq, decided_at_ms, epoch, edge_code, outcome, category, advice, envelope_version, "
                "envelope_params_hash, strategy_kind, strategy_impl_version, guards_hash) "
                "VALUES ($1, 'syn', 'P1', 'STOP_LEVEL', 'position', '15m', 'POST', 1, $1, 5, 'edge_a', 'ADJUST', "
                "'ACTIONABLE', true, 1, 'abcdef0123456789', 'kind_a', 1, '0123456789abcdef') "
                "ON CONFLICT DO NOTHING RETURNING tableoid::regclass::text", now_ms)
            assert row is not None and str(row[0]) == f"arm_decisions_p{base:%Y%m}"   # never the DEFAULT partition
            assert await _denied(c, "DELETE FROM arm_decisions")
        finally:
            await c.execute("ROLLBACK")

        for bad in ("INSERT INTO arm_notices (route, event, bar_end_ms, part, created_at_ms, epoch) "
                    "VALUES ('r', 'POST', 1, 6, 1, 5)",
                    "INSERT INTO arm_notices (route, event, bar_end_ms, part, created_at_ms, epoch) "
                    "VALUES ('r', 'LATE', 1, 1, 1, 5)"):
            with pytest.raises(Exception, match="arm_notices_part"):
                await c.execute(bad)


async def test_0018_round12_grants_and_checks_on_postgres(source: AsyncpgSource) -> None:
    """S38c review: 0017 then 0018 on a real server (the fixture migrates all, twice). The catalog's column grant
    reads back through `SQL_ROLE_COLUMN_PRIVILEGES`; as `xtnl_arm` a decision row inserts with the new
    `NO_BROKER_BAR` outcome into its month partition, `SQL_DECISIONS_ATTACH` (the `notice_id` update) succeeds, and
    an UPDATE of any other `arm_decisions` column, of `arm_decision_handles`, `arm_correction_audit` or
    `arm_config_audit` is refused; a POSITION scope inserts into `arm_failures`."""
    from datetime import UTC, datetime

    from src.db import arm_schema as A
    from src.db.arm_store import SQL_DECISIONS_ATTACH

    async with source.acquire() as conn:
        c: Any = conn
        columns = [(str(r[0]), str(r[1]), str(r[2])) for r in await c.fetch(A.SQL_ROLE_COLUMN_PRIVILEGES, A.ARM_ROLE)]
        assert ("arm_decisions", "notice_id", "UPDATE") in columns
        assert not [x for x in columns if x[0] == "arm_decisions" and x[1] != "notice_id"]
        tables = [(str(r[0]), str(r[1])) for r in await c.fetch(A.SQL_ROLE_TABLE_PRIVILEGES, A.ARM_ROLE)]
        for table in A.ARM_IMMUTABLE_TABLES:
            assert (table, "UPDATE") not in tables, table
        sequences = [(str(r[0]), str(r[1])) for r in await c.fetch(A.SQL_ROLE_SEQUENCE_PRIVILEGES, A.ARM_ROLE)]
        assert A.privilege_problems(tables, columns, sequences) == []
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        await c.execute("BEGIN")
        try:
            await c.execute("SET LOCAL ROLE xtnl_arm")
            row = await c.fetchrow(
                "INSERT INTO arm_decisions (bar_end_ms, adapter_id, position_id, resource, owner, bar_spec, event, "
                "decision_seq, decided_at_ms, epoch, edge_code, outcome, category, advice, envelope_version, "
                "envelope_params_hash, strategy_kind, strategy_impl_version, guards_hash) "
                "VALUES ($1, 'syn', 'P1', 'STOP_LEVEL', 'position', '15m', 'POST', 1, $1, 5, 'edge_a', "
                "'NO_BROKER_BAR', 'INFO', true, 1, 'abcdef0123456789', 'kind_a', 1, '0123456789abcdef') "
                "RETURNING decision_id, bar_end_ms", now_ms)
            assert row is not None
            await c.execute(SQL_DECISIONS_ATTACH, EPOCH.revision, 0, [int(row[0])], [int(row[1])])
            got = await c.fetchrow("SELECT notice_id FROM arm_decisions WHERE decision_id = $1", int(row[0]))
            assert got is not None and got[0] == 0
            assert await _denied(c, "UPDATE arm_decisions SET outcome = 'ADJUST'")
            assert await _denied(c, "UPDATE arm_decisions SET decided_at_ms = 1")
            assert await _denied(c, "UPDATE arm_decision_handles SET handle_role = 'x'")
            assert await _denied(c, "UPDATE arm_correction_audit SET result = 'x'")
            assert await _denied(c, "UPDATE arm_config_audit SET action = 'x'")
            await c.execute(
                "INSERT INTO arm_failures (failure_id, scope_kind, scope_id, reason, probe_kind, ping_id, "
                "first_fault_at_ms, entered_at_ms, epoch) "
                "SELECT 'f18', 'POSITION', 'syn:P1', 'stuck_frontier', 'span_write', 'p18', 1, 2, $1::bigint "
                "WHERE (SELECT epoch FROM engine_epoch WHERE id = 1) = $1::bigint", EPOCH.revision)
        finally:
            await c.execute("ROLLBACK")


ARM_TABLES = ("arm_positions, arm_decisions, arm_decision_handles, arm_correction_audit, arm_notices, arm_status, "
              "arm_failures, arm_config_cache, arm_config_audit")


class _ArmRoleSource:
    """The ARM pool's shape (S31): every connection runs as `xtnl_arm` (SET ROLE), reset on release."""

    def __init__(self, inner: AsyncpgSource) -> None:
        self._inner = inner

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[Any]:
        async with self._inner.acquire() as conn:
            await conn.execute("SET ROLE xtnl_arm")
            try:
                yield conn
            finally:
                await conn.execute("RESET ROLE")

    def acquire(self) -> Any:
        return self._acq()


async def test_s24_arm_stores_as_xtnl_arm_on_postgres(source: AsyncpgSource) -> None:
    """S24: every ARM store statement runs on a real server as `xtnl_arm` (the §B3 grants are enough), except the
    clear, which only the engine (owner) pool may run; the S_at_T reads never give a history or DROPPED_STALE level."""
    from datetime import UTC, datetime

    from src.db.arm_config_store import PgArmConfigStore
    from src.db.arm_failures_store import PgArmFailedClear, PgArmFailuresStore
    from src.db.arm_ports import (
        ArmStatusRow,
        ConfigAuditRow,
        ConfigCacheRow,
        CorrectionAuditRow,
        NodeMdRecordRow,
        NoticeRow,
    )
    from src.db.arm_ports import DecisionWriteStatus as W
    from src.db.arm_ports import PositionCas
    from src.db.arm_status_store import PgArmStatusStore
    from src.db.arm_store import PgArmStateStore
    from src.db.arm_versions import PgArmVersionReader
    from src.db.pg import sqlstate
    from src.riskmgr.stopbasis import StopAtT, stop_at_bar_end
    from src.riskmgr.types import StopBasis
    from tests.db.arm_rows import KEY, decision_row, failure, handle_row, ping, position_row, to_stop_version

    async with source.acquire() as conn:
        await conn.execute(f"TRUNCATE {ARM_TABLES} RESTART IDENTITY")
    arm = EngineDb(_ArmRoleSource(source), lambda: EPOCH)
    engine = EngineDb(source, lambda: EPOCH)

    # ---- positions, decisions, handles, notices, audit ---------------------------------------------------------
    store = PgArmStateStore(arm)
    pos = position_row()
    async with store.transaction() as txn:
        assert await txn.insert_position(pos) is True
        assert await txn.insert_position(pos) is False
    stored = await store.position(KEY)
    assert stored == replace(pos, epoch=5)
    nxt = replace(pos, state="TRACKING", decision_seq=1, last_decided_bar=900_000, last_decided_hash="01" + "d" * 32)
    cas = PositionCas(row=nxt, expected_row_version=0)
    async with store.transaction() as txn:
        first = await txn.record_decision(decision_row(), [handle_row()], position=cas)
        assert first.status is W.INSERTED and first.ref is not None
        again = await txn.record_decision(decision_row(), [handle_row()], position=cas)
        assert again.status is W.ALREADY_DECIDED
        stale = await txn.record_decision(decision_row(bar_end_ms=1_800_000, decision_seq=2), [handle_row()],
                                          position=cas)
        assert stale.status is W.CAS_MISS      # only its savepoint rolled back; the transaction carries on
        nid = await txn.insert_notice(NoticeRow(route="r1", bar_spec="15m", event="POST", bar_end_ms=900_000,
                                                decision_count=1, created_at_ms=902_000))
        assert nid is not None
        assert await txn.insert_notice(NoticeRow(route="r1", bar_spec="15m", event="POST", bar_end_ms=900_000,
                                                 created_at_ms=903_000)) is None
        unnoticed = await txn.unnoticed_decisions([("syn", "ACC1")], "15m", since_bar_end_ms=0)
        assert [d.ref for d in unnoticed.decisions] == [first.ref] and unnoticed.truncated is False
        assert unnoticed.decisions[0].row == replace(decision_row(), epoch=5)
        assert await txn.attach_notice(nid, [first.ref]) == 1
        assert await txn.attach_notice(nid, [first.ref]) == 0
        assert (await txn.unnoticed_decisions([("syn", "ACC1")], "15m", since_bar_end_ms=0)).decisions == ()
        assert await txn.insert_correction_audit(CorrectionAuditRow(ref=first.ref, adapter_id="syn",
                                                                    position_id="P1", reason="bars_missing",
                                                                    recorded_at_ms=1)) is True
    after = await store.position(KEY)
    assert after is not None and after.row_version == 1 and after.state == "TRACKING"
    memory = await store.decision_memory(KEY, "15m", 900_000)      # S36b review: the committed-decision read
    assert memory.pre_levels == () and (memory.last_outcome, memory.last_reason) == ("ADJUST", None)
    async with source.acquire() as conn:
        n = await conn.fetchrow("SELECT count(*) FROM arm_decisions")
        h = await conn.fetchrow("SELECT count(*) FROM arm_decision_handles")
    assert n == (1,) and h == (1,)
    assert [p.key for p in await store.live_positions()] == [KEY]

    # ---- event_log reads: entry time and S_at_T (golden: amended, then a history version) --------------------------
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    seq = now_ms << 20
    t = 1_000_000

    async def log(i: int, et: str, key: str, payload: str, *, origin: str | None, bt: int | None,
                  action: str = "APPLIED") -> None:
        async with source.acquire() as c:
            await c.execute(
                "INSERT INTO event_log (seq_id, entity_type, entity_key, adapter_id, account_id, action, payload, "
                "epoch_revision, origin, broker_time_ms) VALUES ($1, $2, $3, 'syn', 'ACC1', $4, $5::text::jsonb, 5, "
                "$6, $7)", seq + i, et, key, action, payload, origin, bt)

    pk, ok_ = "position:syn:P2", "order:syn:O1"
    await log(1, "POSITION", pk, '{"status":"OPEN","units":"2"}', origin="stream", bt=t - 50_000)
    await log(2, "ORDER", ok_, '{"status":"WORKING","entry_price":"9.0","remaining_units":"2","order_kind":"STOP"}',
              origin="stream", bt=t - 40_000)
    # the StoneX history mapping of the amended-then-cancelled stop: the terminal CANCELLED version at the last
    # change and the placement version (working status) at CreatedDateTimeUtc, both with the last level
    await log(3, "ORDER", ok_, '{"status":"CANCELLED","entry_price":"9.5","remaining_units":"2","order_kind":"STOP"}',
              origin="history", bt=t - 10_000)
    await log(5, "ORDER", ok_, '{"status":"WORKING","entry_price":"9.5","remaining_units":"2","order_kind":"STOP"}',
              origin="history", bt=t - 90_000)
    await log(4, "ORDER", ok_, '{"status":"WORKING","entry_price":"9.7","remaining_units":"2","order_kind":"STOP"}',
              origin="stream", bt=t - 5_000, action="DROPPED_STALE")
    reader = PgArmVersionReader(arm)
    entry = await reader.entry_time("syn", "P2", None)
    assert entry is not None and entry.source == "STREAM" and entry.broker_time_ms == t - 50_000
    versions = await reader.stop_versions("syn", "P2", ["O1"], t)
    assert {v.level for v in versions if v.level is not None and v.counts} == {Decimal("9.0")}   # never 9.7
    assert Decimal("9.7") not in {v.level for v in versions}                 # DROPPED_STALE is never read
    got = stop_at_bar_end([to_stop_version(v) for v in versions], side="BUY", entry=Decimal("10"),
                          units_to_cover=Decimal("2"), bar_end_ms=t)
    assert isinstance(got, StopAtT) and got.level == Decimal("9.0") and got.basis is StopBasis.PRIOR_VERSION

    # ---- FAILED: record + both outbox rows as xtnl_arm; the clear only on the engine pool --------------------------
    failures = PgArmFailuresStore(arm)
    assert await failures.record(failure(), ping()) is True
    assert await failures.record(failure(), ping()) is False                       # replay: no second ping
    assert await failures.reping("f-1", "ping-1", ping(created_ms=3_000)) is True
    assert await failures.note_probe("f-1", 3_500, "refused:bars_invalid") is True
    [open_] = await failures.open_episodes()
    assert open_.record == failure() and open_.ping_count == 2 and open_.last_probe_result == "refused:bars_invalid"
    async with source.acquire() as conn:
        kinds = await conn.fetch("SELECT kind, severity, channels FROM notification_outbox ORDER BY outbox_id")
    assert [(str(k), str(s)) for k, s, _ in kinds] == [("info", "CRITICAL"), ("arm", "CRITICAL")] * 2
    assert [list(c) for k, _, c in kinds if k == "arm"] == [["arm_email", "arm_push"]] * 2
    async with source.acquire() as conn:
        audit = await conn.fetchrow(
            "INSERT INTO operator_audit (command, target, operator, reason, outcome, epoch) VALUES "
            "('arm.failed.clear', 'SERIES:syn:SYN1:15m:bid', 'op.one', 'cause fixed by rebuild', 'applied', 5) "
            "RETURNING audit_id")
        assert audit is not None
        req = await conn.fetchrow(
            "INSERT INTO operator_requests (kind, target, audit_id, requested_epoch) VALUES "
            "('arm_clear_failed', 'SERIES:syn:SYN1:15m:bid', $1, 5) RETURNING request_id", audit[0])
        assert req is not None
    with pytest.raises(Exception) as denied:
        await PgArmFailedClear(arm).clear(int(req[0]), "f-1", 4_000)
    assert sqlstate(denied.value) == "42501"
    cleared = await PgArmFailedClear(engine).clear(int(req[0]), "f-1", 4_000)
    assert cleared is not None and cleared.scope_kind == "SERIES"
    assert await failures.open_episodes() == []
    assert await failures.record(failure(failure_id="f-2"), ping()) is True        # a new episode of the scope
    assert await PgArmFailedClear(engine).clear(int(req[0]), "f-2", 5_000) is None  # one request, one episode

    # ---- status (unfenced) and config ------------------------------------------------------------------------------
    status = PgArmStatusStore(_ArmRoleSource(source))
    assert await status.upsert(ArmStatusRow(node="n1", epoch=5, updated_at_ms=10, counters=(("armed", 1),))) is True
    assert await status.upsert(ArmStatusRow(node="n1", epoch=5, updated_at_ms=9)) is False
    assert [s.counters for s in await status.all()] == [(("armed", 1),)]
    md = NodeMdRecordRow(node="n1", sentinel="abc123:c1:n1:5.2", last_write_ms=1_000)
    assert await status.save_md_record(md) is True
    assert await status.save_md_record(replace(md, last_write_ms=999)) is False
    assert await status.upsert(ArmStatusRow(node="n1", epoch=5, updated_at_ms=11)) is True   # md columns untouched
    assert await status.load_md_record("n1") == md
    assert await status.save_md_record(replace(md, node="n2")) is True                      # creates n2's row
    assert [(s.node, s.updated_at_ms) for s in await status.all()] == [("n1", 11), ("n2", 0)]
    config = PgArmConfigStore(arm)
    cache = ConfigCacheRow(doc_hash="b" * 64, schema_version=1, document_json='{"assignments":[]}',
                           row_hashes=(("assignments:1", "c" * 64),), chain_head="d" * 64, chain_rows=1,
                           updated_at_ms=10)
    assert await config.save_cache(cache) is True
    assert await config.cached() == replace(cache, document_json='{"assignments": []}')
    assert await config.audit(ConfigAuditRow(at_ms=10, node="n1", action="installed", doc_hash="b" * 64,
                                             row_refs=("assignments:1:" + "c" * 64,))) is True
    assert await config.latest_accepted() is None
    controls = await config.controls()
    assert controls is not None and controls.paused is False
