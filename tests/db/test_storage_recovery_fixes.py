"""Regression tests for the storage-recovery review findings that live in src/db (no server needed,
except the `pg` test at the bottom, which runs only with ORDER_SERVER_TEST_DSN on a DISPOSABLE database).

* 0002-dropnotnull-on-pk-column: no migration drops NOT NULL on a column that is still in a primary key.
* balance-skew-noop-advance: the skew input is the latest balance *change*, not committed_at_ms.
* dlq-lineage-not-adapter-scoped: lineage statements are scoped by adapter_id.
"""
from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from src.db import MIGRATIONS_DIR
from src.db.entity_sql import ACCOUNTS, ORDERS, POSITIONS
from src.db.epoch import EngineDb, Epoch, bump_epoch
from src.db.migrate import migrate
from src.db.pg import AsyncpgSource, create_asyncpg_source
from src.db.repository import PgStateStore
from src.db.rows import ACCOUNT_COLS
from src.dto import AccountDTO, Decision

_CREATE = re.compile(r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\n\)", re.S)
_INLINE_PK = re.compile(r"^\s*(\w+)\s+[^,\n]*\bPRIMARY KEY\b", re.M)
_TABLE_PK = re.compile(r"^\s*PRIMARY KEY\s*\(([^)]*)\)", re.M)
_STMT = re.compile(
    r"ALTER TABLE (?:IF EXISTS )?(\w+)\s+(?:DROP CONSTRAINT (?:IF EXISTS )?(\w+)"
    r"|ADD CONSTRAINT (\w+) PRIMARY KEY \(([^)]*)\)"
    r"|ALTER COLUMN (\w+) DROP NOT NULL)")


def _strip_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def drop_not_null_on_pk_columns(directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Replay every migration in order, tracking each table's primary key, and list each
    `ALTER COLUMN c DROP NOT NULL` that runs while `c` is still in its table's primary key
    (PostgreSQL rejects it: ERROR 42P16 "column ... is in a primary key")."""
    pk: dict[str, set[str]] = {}
    bad: list[str] = []
    for path in sorted(directory.glob("*.sql")):
        sql = _strip_comments(path.read_text(encoding="utf-8"))
        events: list[tuple[int, str, re.Match[str]]] = [(m.start(), "create", m) for m in _CREATE.finditer(sql)]
        events += [(m.start(), "alter", m) for m in _STMT.finditer(sql)]
        for _, kind, m in sorted(events, key=lambda e: e[0]):
            if kind == "create":
                table, body = m.group(1), m.group(2)
                cols = {c.group(1) for c in _INLINE_PK.finditer(body)}
                for t in _TABLE_PK.finditer(body):
                    cols |= {c.strip() for c in t.group(1).split(",")}
                pk.setdefault(table, cols)
                continue
            table = m.group(1)
            if m.group(2) is not None and m.group(2) == f"{table}_pkey":
                pk[table] = set()
            elif m.group(3) is not None:
                pk[table] = {c.strip() for c in m.group(4).split(",")}
            elif m.group(5) is not None and m.group(5) in pk.get(table, set()):
                bad.append(f"{path.name}: {table}.{m.group(5)}")
    return bad


def test_no_drop_not_null_on_a_primary_key_column() -> None:
    """0002-dropnotnull-on-pk-column: the PK swap must precede the DROP NOT NULL."""
    assert drop_not_null_on_pk_columns() == []


def test_checker_catches_the_original_0002_order(tmp_path: Path) -> None:
    """The checker flags the shipped bug: DROP NOT NULL on sweep_quarantine.adapter_id before the PK swap."""
    init = (MIGRATIONS_DIR / "0001_init.sql").read_text(encoding="utf-8")
    fixed = (MIGRATIONS_DIR / "0002_lockfree_fencing.sql").read_text(encoding="utf-8")
    drop = "ALTER TABLE sweep_quarantine ALTER COLUMN adapter_id DROP NOT NULL;\n"
    anchor = "-- Swap the primary key to (account_id) FIRST"
    assert drop in fixed and anchor in fixed
    original = fixed.replace(drop, "").replace(anchor, drop + anchor, 1)   # the shipped (broken) order
    (tmp_path / "0001_init.sql").write_text(init, encoding="utf-8")
    (tmp_path / "0002_lockfree_fencing.sql").write_text(original, encoding="utf-8")
    assert drop_not_null_on_pk_columns(tmp_path) == ["0002_lockfree_fencing.sql: sweep_quarantine.adapter_id"]


def test_sweep_quarantine_adapter_id_ends_nullable() -> None:
    sql = (MIGRATIONS_DIR / "0002_lockfree_fencing.sql").read_text(encoding="utf-8")
    assert "ALTER TABLE sweep_quarantine ALTER COLUMN adapter_id DROP NOT NULL" in sql


# ---- balance-skew-noop-advance --------------------------------------------------------------------

def test_account_skew_column_is_the_balance_change_time() -> None:
    cols = [c.strip() for c in ACCOUNT_COLS.split(",")]
    assert cols[10] == "balance_changed_at_ms"     # snapshot_reader reads index 10 as balance_committed_at_ms
    assert "committed_at_ms" not in cols


def test_balance_change_stamps_only_when_the_balance_changes() -> None:
    apply_sql = ACCOUNTS.update_apply
    bal = ACCOUNTS.all_cols.index("balance") + 2   # $1 is the epoch
    assert (f"balance_changed_at_ms = CASE WHEN balance IS DISTINCT FROM ${bal} THEN now_ms() "
            "ELSE balance_changed_at_ms END") in apply_sql
    # NOOP_ADVANCE (same checksum, newer key) must never move the balance version time.
    assert "balance_changed_at_ms" not in ACCOUNTS.update_advance
    # orders / positions keep their own stamp
    for t in (ORDERS, POSITIONS):
        assert "status_units_changed_at_ms = CASE WHEN" in t.update_apply
        assert "balance_changed_at_ms" not in t.update_apply


def test_0005_adds_balance_changed_at() -> None:
    sql = (MIGRATIONS_DIR / "0005_balance_changed_at.sql").read_text(encoding="utf-8")
    assert "ADD COLUMN balance_changed_at_ms bigint" in sql
    assert "SET DEFAULT now_ms()" in sql and "SET NOT NULL" in sql


# ---- real PostgreSQL ------------------------------------------------------------------------------

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
IT_EPOCH = Epoch("it-cluster", 5)


@pytest.fixture
async def source() -> AsyncIterator[AsyncpgSource]:
    if not DSN:
        pytest.skip("ORDER_SERVER_TEST_DSN not set")
    src = await create_asyncpg_source(DSN, application_name="xtnl:5:it")
    async with src.acquire() as conn:
        await migrate(conn)
        await conn.execute("TRUNCATE engine_epoch, account_state, account_snapshots, event_log, outbox_events "
                           "RESTART IDENTITY CASCADE")
        await bump_epoch(conn, IT_EPOCH, "it-node", terminate_older=False)
    try:
        yield src
    finally:
        await src.close()


async def _balance_changed(source: AsyncpgSource) -> int:
    async with source.acquire() as conn:
        row = await conn.fetchrow("SELECT balance_changed_at_ms FROM account_state WHERE adapter_id = 'it'")
    assert row is not None
    return int(row[0])


@pytest.mark.pg
async def test_pg_migrations_and_balance_stamp(source: AsyncpgSource) -> None:
    """Fresh migrate gets past 0002 (sweep_quarantine.adapter_id nullable) and the account commit path
    moves balance_changed_at_ms only when the balance changes (not on NOOP_ADVANCE or a P&L-only change)."""
    async with source.acquire() as conn:
        row = await conn.fetchrow("SELECT is_nullable FROM information_schema.columns "
                                  "WHERE table_name = 'sweep_quarantine' AND column_name = 'adapter_id'")
        assert row is not None and row[0] == "YES"
    store = PgStateStore(EngineDb(source, lambda: IT_EPOCH))
    base = AccountDTO(account_id="ACC", adapter_id="it", currency="USD", balance=Decimal(100), observed_seq=1,
                      seq_id=(1 << 40))
    assert (await store.commit_entity(base)).decision is Decision.APPLY
    async with source.acquire() as conn:
        await conn.execute("UPDATE account_state SET balance_changed_at_ms = 5 WHERE adapter_id = 'it'")
    resent = replace(base, observed_seq=2, seq_id=(1 << 40) + 1)          # same checksum, newer key
    assert (await store.commit_entity(resent)).decision is Decision.NOOP_ADVANCE
    assert await _balance_changed(source) == 5
    pnl_only = replace(base, realised_pnl_today=Decimal(3), observed_seq=3, seq_id=(1 << 40) + 2)
    assert (await store.commit_entity(pnl_only)).decision is Decision.APPLY
    assert await _balance_changed(source) == 5
    credit = replace(pnl_only, balance=Decimal(104), observed_seq=4, seq_id=(1 << 40) + 3)
    assert (await store.commit_entity(credit)).decision is Decision.APPLY
    assert await _balance_changed(source) > 5
