"""Sanity checks on the migrations (no server needed): 0001_init.sql, and every file for the column checks (ST-5:
a column added by a later ALTER TABLE counts, a dropped one does not)."""
from __future__ import annotations

import re

from src.db import MIGRATIONS_DIR

SQL = (MIGRATIONS_DIR / "0001_init.sql").read_text(encoding="utf-8")
ALL_SQL = {p.name: p.read_text(encoding="utf-8") for p in sorted(MIGRATIONS_DIR.glob("*.sql"))}

REQUIRED = {
    "engine_epoch", "leader_terms", "cluster_state", "orders_state", "positions_state", "account_state",
    "account_snapshots", "cash_flows", "sod_marks", "instruments", "last_clean_marks", "stop_watch",
    "stream_health", "breach_acks", "sweep_quarantine", "halt_breaches", "halt_events", "oversee_events",
    "notification_outbox", "notification_dedupe", "event_log", "outbox_events", "events_dlq", "events_quarantine",
    "adapter_auth_state", "outbound_intent", "rulesets", "market_sessions", "ctrader_tokens",
}


def _tables() -> set[str]:
    return set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)\s*\(", SQL))


def _body(table: str) -> str:
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {table}\s*\((.*?)\n\)", SQL, re.S)
    assert m, table
    return m.group(1)


def _code(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _columns(table: str) -> set[str]:
    """The table's columns after every migration in order: its CREATE TABLE body, plus ALTER TABLE ... ADD COLUMN,
    minus ALTER TABLE ... DROP COLUMN (guarded DO blocks included)."""
    cols: set[str] = set()
    for sql in ALL_SQL.values():
        code = _code(sql)
        m = re.search(rf"CREATE TABLE IF NOT EXISTS {table}\s*\((.*?)\n\)", code, re.S)
        if m:
            for line in m.group(1).splitlines():
                c = re.match(r"\s+(\w+)\s+\w", line)
                if c and c.group(1).upper() not in ("PRIMARY", "UNIQUE", "CHECK", "CONSTRAINT", "FOREIGN"):
                    cols.add(c.group(1))
        for stmt in re.findall(rf"ALTER TABLE (?:IF EXISTS )?(?:ONLY )?{table}\s(.*?);", code, re.S):
            cols |= set(re.findall(r"ADD COLUMN (?:IF NOT EXISTS )?(\w+)", stmt))
            cols -= set(re.findall(r"DROP COLUMN (?:IF EXISTS )?(\w+)", stmt))
    return cols


def _col_list(cols: str) -> list[str]:
    return [c.strip() for c in cols.split(",")]


def test_every_required_table_exists() -> None:
    assert REQUIRED <= _tables(), REQUIRED - _tables()


def test_idempotent_ddl() -> None:
    for name, sql in ALL_SQL.items():
        code = _code(sql)
        assert not re.search(r"CREATE TABLE (?!IF NOT EXISTS)", code), name
        assert not re.search(r"CREATE (UNIQUE )?INDEX (?!(CONCURRENTLY )?IF NOT EXISTS)", code), name
        assert not re.search(r"CREATE FUNCTION", code), name  # always CREATE OR REPLACE


def test_partitioning() -> None:
    for t, key in (("event_log", "seq_id"), ("account_snapshots", "seq_id"), ("oversee_events", "sweep_id"),
                   ("notification_outbox", "created_at_ms")):
        assert re.search(rf"CREATE TABLE IF NOT EXISTS {t} \(.*?\) PARTITION BY RANGE \({key}\);", SQL, re.S), t
        assert f"xtnl_ensure_month_partition('{t}'" in SQL
    assert "UNIQUE (rule_id, subject_type, subject_id, transition, sweep_id)" in _body("oversee_events")


def test_halt_breaches_columns() -> None:
    body = _body("halt_breaches")
    for col in ("level", "cap", "reason", "pending_since", "active_since", "release_pending_since", "unknown_since",
                "uncapped_ms", "below_soft_since", "resolved_at", "resolved_by", "last_sweep_id", "clear_generation"):
        assert re.search(rf"^\s+{col}\s", body, re.M), col
    assert "UNIQUE (rule_id, subject_type, subject_id)" in body


def test_key_columns_and_types() -> None:
    assert "PRIMARY KEY (adapter_id, broker_order_id)" in _body("orders_state")
    assert re.search(r"client_order_id\s+text,", _body("orders_state"))  # nullable
    assert "payload_hash        char(64) NOT NULL UNIQUE" in _body("events_dlq")
    for col in ("origin_dlq_id", "status", "retry_count", "next_retry_at_ms"):
        assert col in _body("events_dlq")
    assert "hard_latch" in _body("halt_events")
    assert "token_generation" in _body("ctrader_tokens")
    for col in ("unexplained_increase_at_roll", "partial_session"):
        assert col in _body("sod_marks")
    for t in ("orders_state", "positions_state", "account_state"):
        b = _body(t)
        for col in ("broker_seq", "broker_updated_at_ms", "observed_seq", "version", "seq_id", "checksum",
                    "digest_hash"):
            assert re.search(rf"^\s+{col}\s", b, re.M), (t, col)
        assert "version               char(56)" in b or "version                    char(56)" in b
    # money is NUMERIC, never floating point (every migration)
    for name, sql in ALL_SQL.items():
        assert not re.search(r"\b(real|double precision|float\d?)\b", _code(sql), re.I), name


def test_entity_sql_columns_exist_in_schema() -> None:
    from src.db.entity_sql import ACCOUNTS, ORDERS, POSITIONS
    for t in (ORDERS, POSITIONS, ACCOUNTS):
        cols = _columns(t.table)
        for col in t.all_cols:
            assert col in cols, (t.table, col)


def test_row_decoder_columns_exist_in_schema() -> None:
    from src.db import rows as R
    for table, cols in (("orders_state", R.ORDER_COLS), ("positions_state", R.POSITION_COLS),
                        ("account_state", R.ACCOUNT_COLS), ("instruments", R.INSTRUMENT_COLS),
                        ("sod_marks", R.SOD_COLS)):
        have = _columns(table)
        for col in _col_list(cols):
            assert col in have, (table, col)


def test_store_insert_columns_exist_in_schema() -> None:
    from src.db import repository as R
    for sql in (R.SQL_EVENT_LOG, R.SQL_INSTRUMENT_UPSERT):
        m = re.match(r"INSERT INTO (\w+) \((.*?)\)", sql, re.S)
        assert m, sql
        have = _columns(m.group(1))
        for col in _col_list(m.group(2)):
            assert col in have, (m.group(1), col)


def test_column_scan_follows_later_migrations() -> None:
    assert "balance_changed_at_ms" in _columns("account_state")      # 0005, inside a guarded DO block
    assert {"order_kind"} <= _columns("orders_state")                # 0015
    assert {"origin", "broker_time_ms"} <= _columns("event_log")     # 0015
    assert "position_marks" not in _columns("sod_marks")             # created by 0001, dropped by 0015


SQL2 = (MIGRATIONS_DIR / "0002_lockfree_fencing.sql").read_text(encoding="utf-8")


def test_0002_adds_fencing_token_and_store_columns() -> None:
    assert "ALTER TABLE engine_epoch ADD COLUMN IF NOT EXISTS epoch bigint GENERATED ALWAYS AS (revision) STORED" in SQL2
    for col in ("status_units_changed_at_ms", "unknown_origin", "activation_id", "sent_at_ms", "delivered", "episode"):
        assert f"ADD COLUMN IF NOT EXISTS {col}" in SQL2, col
    assert "'HIGH'" in SQL2 and "PRIMARY KEY (account_id)" in SQL2


def test_0002_is_idempotent() -> None:
    assert not re.search(r"ADD COLUMN (?!IF NOT EXISTS)", SQL2)
    assert not re.search(r"CREATE (UNIQUE )?INDEX (?!IF NOT EXISTS)", SQL2)
    # constraint swaps are guarded by catalog checks inside DO blocks
    assert SQL2.count("DO $$") == 2 and "IF EXISTS (SELECT 1 FROM pg_constraint" in SQL2
