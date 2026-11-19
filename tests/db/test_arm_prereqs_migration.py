"""0015_arm_prereqs.sql (S01): idempotent, nullable columns only, the event_log CHECK added NOT VALID, and no
column that could hold market data (arm-implementation-plan.md §1, design §C1 0015)."""
from __future__ import annotations

import re

from src.db.migrate import discover

MIG = next(m for m in discover() if m.version == 15)
SQL = MIG.sql
CODE = "\n".join(line.split("--", 1)[0] for line in SQL.splitlines())

#: Every column 0015 may add, by table (an allow-list: a new column needs a reviewed change here).
ALLOWED_COLUMNS = {
    "orders_state": {"order_kind"},
    "instruments": {"price_increment", "min_stop_distance", "asset_class"},
    "event_log": {"origin", "broker_time_ms"},
}
MARKET_DATA_WORDS = ("bid", "ask", "mid", "quote", "spread", "tick", "ohlc", "open_price", "high", "low", "close",
                     "atr", "indicator", "favour", "price")


def _added() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for table, col in re.findall(r"ALTER TABLE (\w+) ADD COLUMN IF NOT EXISTS (\w+)", CODE):
        out.setdefault(table, set()).add(col)
    return out


def test_migration_is_0015_arm_prereqs() -> None:
    assert MIG.name == "arm_prereqs"
    assert SQL.startswith("-- 0015_arm_prereqs.sql")
    assert "SET client_min_messages = warning;" in CODE


def test_idempotent() -> None:
    assert not re.search(r"ADD COLUMN (?!IF NOT EXISTS)", CODE)
    assert not re.search(r"DROP COLUMN (?!IF EXISTS)", CODE)
    assert not re.search(r"CREATE (UNIQUE )?INDEX (?!IF NOT EXISTS)", CODE)
    assert not re.search(r"CREATE TABLE (?!IF NOT EXISTS)", CODE)
    # every ADD CONSTRAINT sits in a DO block guarded by a pg_constraint lookup on its own table and name
    adds = re.findall(r"ALTER TABLE (\w+) ADD CONSTRAINT (\w+)", CODE)
    assert sorted(adds) == [("event_log", "event_log_origin_check"), ("orders_state", "orders_state_order_kind_check")]
    for table, name in adds:
        assert re.search(rf"IF NOT EXISTS \(SELECT 1 FROM pg_constraint\s+WHERE conrelid = '{table}'::regclass "
                         rf"AND conname = '{name}'\) THEN\s+ALTER TABLE {table} ADD CONSTRAINT {name}", CODE), name
    assert CODE.count("DO $$") == 2 and CODE.count("$$;") == 2


def test_columns_are_exactly_the_allow_list_and_nullable_without_default() -> None:
    assert _added() == ALLOWED_COLUMNS
    for stmt in re.findall(r"ADD COLUMN IF NOT EXISTS [^;]*;", CODE):
        assert "NOT NULL" not in stmt and "DEFAULT" not in stmt, stmt
    assert re.search(r"ADD COLUMN IF NOT EXISTS order_kind text;", CODE)
    assert re.search(r"ADD COLUMN IF NOT EXISTS price_increment numeric;", CODE)
    assert re.search(r"ADD COLUMN IF NOT EXISTS min_stop_distance numeric;", CODE)
    assert re.search(r"ADD COLUMN IF NOT EXISTS asset_class text;", CODE)
    assert re.search(r"ADD COLUMN IF NOT EXISTS origin text;", CODE)
    assert re.search(r"ADD COLUMN IF NOT EXISTS broker_time_ms bigint;", CODE)


def test_checks() -> None:
    assert "CHECK (order_kind IN ('STOP', 'LIMIT'));" in CODE        # orders_state is not partitioned
    assert "CHECK (origin IN ('stream', 'snapshot', 'history', 'dlq', 'stop_read')) NOT VALID;" in CODE
    assert "VALIDATE CONSTRAINT" not in CODE                           # the S08 migrate post step validates it


def test_drops_only_the_unused_sod_marks_column() -> None:
    assert re.findall(r"ALTER TABLE (\w+) DROP COLUMN IF EXISTS (\w+)", CODE) == [("sod_marks", "position_marks")]
    assert "DROP TABLE" not in CODE and "DROP CONSTRAINT" not in CODE
    # the existing price tables of Q15 are left exactly as they are
    assert "last_clean_marks" not in CODE and "clean_mids" not in CODE


def test_no_column_can_hold_market_data() -> None:
    for table, cols in _added().items():
        for col in cols:
            if (table, col) == ("instruments", "price_increment"):
                continue   # instrument metadata (PR7), like point_size; never a price
            assert not any(w in col for w in MARKET_DATA_WORDS), (table, col)


def test_no_index_here() -> None:
    # the event_log (entity_type, entity_key, broker_time_ms) index is built ON ONLY by 0017 plus a post step
    assert "INDEX" not in CODE.upper()
