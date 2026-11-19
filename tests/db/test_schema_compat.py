"""Plan S38f (round 14, fix 1): with every flag off and the schema at 0014 (migrations 0015-0020 held back by
`XTNL_MIGRATIONS_ACCEPTED_UPTO`), the engine starts and behaves as before.

* Without a database: a fake 0014 connection refuses (as PostgreSQL would, SQLSTATE 42703) every statement that names
  a column 0015-0020 added; the core write and read paths run through `LegacyConnection` against it, and a scan of
  every SQL constant of the core modules finds none the rewriter would leave naming such a column.
* `pg` marker: a scratch schema migrated to 0014 only, the real statements through the real asyncpg driver.
"""
from __future__ import annotations

import importlib
import os
import re
from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from src.db import schema_compat as C
from src.db.entity_sql import ORDERS
from src.db.epoch import EngineDb, Epoch
from src.db.integrity_store import CURRENT_TABLES
from src.db.local_state import PgLocalStateReader
from src.db.repository import SQL_EVENT_LOG, SQL_INSTRUMENT_UPSERT, PgStateStore
from src.db.rows import INSTRUMENT_COLS, ORDER_COLS, instrument_from_row, order_from_row
from src.dto import InstrumentSpec, OrderDTO, OrderStatus, without_optional_payload

from tests.db.conftest import EPOCH, FakeConn

ROOT = Path(__file__).resolve().parents[2]
_NEW_0015 = re.compile(r"\b(order_kind|price_increment|min_stop_distance|asset_class|broker_time_ms|origin)\b")
_NEW_0016 = re.compile(r"\b(expires_at_ms|superseded_by)\b")    # notification_outbox only (ctrader_tokens has its own)


class _NewColumn:
    """A 0015-0020 column named by a statement (0016's outbox columns only where the outbox is named)."""

    @staticmethod
    def search(sql: str) -> re.Match[str] | None:
        m = _NEW_0015.search(sql)
        if m is None and "notification_outbox" in sql:
            m = _NEW_0016.search(sql)
        return m


NEW_COLUMN = _NewColumn()
#: Core modules whose SQL runs with both flags off (every src/db module but the ARM stores and the runner itself).
ARM_ONLY = ("src.db.arm_", "src.db.migrate", "src.db.schema_compat")
#: The modules the old-schema scan leaves out: the market-data / ARM-only packages (they run only with a flag on,
#: after the migrations they need) and the migration runner. Every other `src` module's SQL runs with both flags off.
CORE_EXCLUDED = (*ARM_ONLY, "src.riskmgr", "src.marketdata", "src.armconfig", "src.runtime.optional",
                 "src.runtime.arm_ports", "src.runtime.md_ports")
#: Statements that run only when 0017 is applied (the lifecycle step 5 is skipped while its tables are absent).
SKIPPED_WITHOUT_0017 = ("SQL_ARM_OUTBOX_",)


@pytest.fixture(autouse=True)
def _reset_process_state() -> Iterator[None]:
    C.reset_for_tests()
    yield
    C.reset_for_tests()


def placeholders(sql: str) -> set[int]:
    return {int(n) for n in re.findall(r"\$(\d+)", sql)}


class UndefinedColumn(Exception):
    sqlstate = "42703"


class Schema0014Conn(FakeConn):
    """FakeConn that refuses, like PostgreSQL on a 0014 schema, any statement naming a 0015-0020 column."""

    def _record(self, sql: str, args: tuple[Any, ...]) -> None:
        m = NEW_COLUMN.search(sql)
        if m is not None:
            raise UndefinedColumn(f'column "{m.group(1)}" does not exist')
        if len(placeholders(sql)) and max(placeholders(sql)) != len(args):
            raise AssertionError(f"{len(args)} arguments for {max(placeholders(sql))} parameters: {sql[:80]}")
        super()._record(sql, args)


class Source:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[Any]:
        yield self.conn

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        return self._acq()


def order(**kw: Any) -> OrderDTO:
    base: dict[str, Any] = dict(
        client_order_id=None, broker_order_id="B1", account_id="ACC", adapter_id="stonex", symbol="EURUSD",
        side="SELL", status=OrderStatus.WORKING, entry_price=Decimal("1.09"), ordered_units=Decimal(1000),
        remaining_units=Decimal(1000), is_reduce_only=True, linked_position_id="P1", order_kind="STOP",
        broker_updated_at_ms=1_000, observed_seq=10, seq_id=100, origin="stream", broker_time_source="BROKER")
    base.update(kw)
    return OrderDTO(**base)


def instrument(**kw: Any) -> InstrumentSpec:
    base: dict[str, Any] = dict(adapter_id="stonex", symbol="EURUSD", base_ccy="EUR", quote_ccy="USD", pnl_ccy="USD",
                                contract_size=Decimal(1), unit_kind="UNITS", point_size=Decimal("0.0001"),
                                min_qty=Decimal(1), swap_booked_to_balance=True, broker_symbol="401",
                                price_increment=Decimal("0.00001"), min_stop_distance=Decimal("0.0005"),
                                asset_class="FX")
    base.update(kw)
    return InstrumentSpec(**base)


# ---------------------------------------------------------------------------------------------------- rewrites
def test_the_write_rewrites_drop_each_new_column_and_its_argument() -> None:
    rw = C.LegacyRewriter()
    o = order()
    from src.cache.scripts import entity_write
    w = entity_write(o)
    for sql, params in ((ORDERS.insert_new, ORDERS.insert_params(o, w)), (ORDERS.update_apply, ORDERS.apply_params(o, w))):
        args = (7, *params)
        new_sql, new_args = rw.rewrite(sql, args)
        assert not NEW_COLUMN.search(new_sql) and placeholders(new_sql) == set(range(1, len(new_args) + 1))
        assert len(new_args) == len(args) - 1 and "STOP" not in new_args and new_args[0] == 7
    # ORDERS.update_advance names no payload column: unchanged
    assert rw.rewrite(ORDERS.update_advance, (1,)) == (ORDERS.update_advance, (1,))
    ev_args = (7, *range(2, 11), "stream", 12345)
    sql, args = rw.rewrite(SQL_EVENT_LOG, ev_args)
    assert not NEW_COLUMN.search(sql) and args == ev_args[:10] and placeholders(sql) == set(range(1, 11))
    ins_args = (7, *range(2, 17), Decimal("0.00001"), Decimal("0.0005"), "FX")
    sql, args = rw.rewrite(SQL_INSTRUMENT_UPSERT, ins_args)
    assert not NEW_COLUMN.search(sql) and args == ins_args[:16] and placeholders(sql) == set(range(1, 17))
    assert "ON CONFLICT (adapter_id, symbol) DO UPDATE" in sql and "RETURNING 1" in sql


def test_the_read_fragments_select_null_in_place_of_the_new_columns() -> None:
    rw = C.LegacyRewriter()
    for t in CURRENT_TABLES:
        for sql in (t.sql_window, t.sql_after_key, t.sql_by_ids):
            new, _ = rw.rewrite(sql, ())
            assert not NEW_COLUMN.search(new)
            assert new.count(",") == sql.count(",")          # the same column count: every decoder unchanged
    order_sql, _ = rw.rewrite(f"SELECT {ORDER_COLS} FROM orders_state", ())
    assert order_sql.endswith("seq_id, NULL::text FROM orders_state")
    instr_sql, _ = rw.rewrite(f"SELECT {INSTRUMENT_COLS} FROM instruments", ())
    assert "broker_symbol, NULL::numeric, NULL::numeric, NULL::text FROM" in instr_sql


def _core_modules() -> list[str]:
    """Every `src/**/*.py` module but the market-data / ARM-only ones (`CORE_EXCLUDED`), R14 review: SQL in
    `src/runtime`, `src/oversee`, `src/integrity`, `src/ops`, `src/api`, ... is scanned too, not only `src/db`."""
    names: list[str] = []
    for p in sorted((ROOT / "src").rglob("*.py")):
        parts = p.relative_to(ROOT).with_suffix("").parts
        if parts[-1] == "__main__":
            continue
        name = ".".join(parts[:-1] if parts[-1] == "__init__" else parts)
        if not name.startswith(CORE_EXCLUDED):
            names.append(name)
    return names


def _sql_constants() -> Iterator[tuple[str, str]]:
    mods: list[ModuleType] = [importlib.import_module(n) for n in _core_modules()]
    for mod in mods:
        for name, value in vars(mod).items():
            if (name.startswith("SQL") and isinstance(value, str)
                    and not name.startswith(SKIPPED_WITHOUT_0017)):
                yield f"{mod.__name__}.{name}", value
    for t in CURRENT_TABLES:
        yield f"CurrentTable {t.table}", t.sql_window
    from src.db.entity_sql import ACCOUNTS, POSITIONS
    for t in (ORDERS, POSITIONS, ACCOUNTS):
        for prop in ("insert_new", "update_apply", "update_advance", "select_for_update"):
            yield f"{t.table}.{prop}", getattr(t, prop)


def test_no_core_statement_names_a_new_column_once_rewritten() -> None:
    rw = C.LegacyRewriter()
    scanned = 0
    for label, sql in _sql_constants():
        new, _ = rw.rewrite(sql, ())
        assert not NEW_COLUMN.search(re.sub(r"\bAS \w+", "", new)), label   # an output alias names no column
        scanned += 1
    assert scanned > 250
    core = _core_modules()
    for pkg in ("src.runtime.pg_ports", "src.runtime.session", "src.oversee", "src.integrity", "src.ops", "src.api",
                "src.dashboard.views"):
        assert any(m == pkg or m.startswith(pkg + ".") for m in core), pkg


def test_the_lifecycle_skips_the_arm_notice_expiry_without_0017() -> None:
    text = (ROOT / "src/db/lifecycle.py").read_text(encoding="utf-8")
    body = text[text.index("async def _arm_expiry"):]
    assert body.index("if rep.absent:") < body.index("SQL_ARM_OUTBOX_EXPIRED")


# ---------------------------------------------------------------------------------------------------- 0014, faked
async def test_core_paths_run_on_a_0014_schema_through_the_legacy_connection() -> None:
    conn = Schema0014Conn()
    rw = C.LegacyRewriter()
    conn.returns(rw.rewrite(ORDERS.insert_new, ())[0], (1,))                 # the first commit inserts
    conn.returns(ORDERS.select_for_update, None, (None, 500, 5, 50, "0" * 64, "WORKING"))   # the second updates
    legacy = C.LegacyConnection(conn, rw)
    db = EngineDb(Source(legacy), lambda: EPOCH)
    store = PgStateStore(db)
    stripped = without_optional_payload(order())
    assert stripped.order_kind is None and stripped.checksum == replace(order(), order_kind=None).checksum
    await store.commit_entity(order())                       # the rewriter keeps the statements valid either way
    await store.commit_entity(stripped)
    assert any(s.startswith("UPDATE orders_state") for s in conn.sql)
    await store.commit_instruments([instrument(), without_optional_payload(instrument())], 1 << 40)
    await PgLocalStateReader(db).open_entities("stonex")
    assert any("INSERT INTO orders_state" in s for s in conn.sql)
    assert any("INSERT INTO event_log" in s for s in conn.sql)
    assert any("INSERT INTO instruments" in s for s in conn.sql)
    with pytest.raises(UndefinedColumn):                     # and the plain connection would have failed
        await PgStateStore(EngineDb(Source(Schema0014Conn()), lambda: EPOCH)).commit_entity(order())


def test_rows_read_back_from_a_0014_schema_decode_with_none() -> None:
    o = order(order_kind=None, origin=None, broker_time_source=None)
    row = (o.client_order_id, o.broker_order_id, o.account_id, o.adapter_id, o.symbol, o.side, o.status.value,
           o.entry_price, o.stop_loss, o.take_profit, o.ordered_units, o.remaining_units, o.instrument_unknown,
           o.raw_qty, o.is_reduce_only, o.linked_position_id, o.broker_updated_at_ms, o.broker_seq, o.source,
           o.observed_seq, o.seq_id, None)
    back = order_from_row(row)
    assert back.order_kind is None and back.checksum == without_optional_payload(order()).checksum
    i = without_optional_payload(instrument())
    irow = (i.adapter_id, i.symbol, i.base_ccy, i.quote_ccy, i.pnl_ccy, i.contract_size, i.unit_kind, i.point_size,
            i.min_qty, i.swap_booked_to_balance, i.broker_symbol, None, None, None)
    assert instrument_from_row(irow) == i


async def test_detection_reads_the_catalog_and_notes_the_process() -> None:
    conn = FakeConn()
    conn.returns(C.SQL_SCHEMA_0015, (True, 0))
    assert isinstance(await C.rewriter_for(conn), C.LegacyRewriter)
    assert C.legacy_schema_seen() and not C.optional_payload_stored()
    C.reset_for_tests()
    conn.returns(C.SQL_SCHEMA_0015, (True, len(C.COLUMNS_0015)))
    assert await C.rewriter_for(conn) is None and C.optional_payload_stored()
    conn.returns(C.SQL_SCHEMA_0015, (False, 0))              # nothing migrated yet: nothing to be compatible with
    assert await C.rewriter_for(conn) is None and C.optional_payload_stored()


def test_the_sources_detect_the_schema() -> None:
    pg = (ROOT / "src/db/pg.py").read_text(encoding="utf-8")
    assert "schema_check: bool = True" in pg and "await with_schema_compat(pool)" in pg
    dash = (ROOT / "src/dashboard/server.py").read_text(encoding="utf-8")
    assert "await with_schema_compat(pool)" in dash
    session = (ROOT / "src/runtime/session.py").read_text(encoding="utf-8")
    assert "optional_payload=optional_payload_stored" in session


# ---------------------------------------------------------------------------------------------------- orchestrator
@pytest.mark.parametrize("stored", [True, False])
async def test_the_orchestrator_publishes_the_pre_0015_shape_on_a_0014_schema(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stored: bool) -> None:
    from tests.pubsub.conftest import ManualClock
    from tests.pubsub.conftest import order as stream_order
    from tests.pubsub.test_orchestrator import make
    monkeypatch.setenv("BROKER_ENV", "sandbox")
    env = make(tmp_path, ManualClock(), optional_payload=lambda: stored)
    row = replace(stream_order("O1", t=1), order_kind="STOP")
    env.item("orders", row)
    (o,) = env.orders()
    assert o.order_kind == ("STOP" if stored else None)
    assert o.checksum == (row.checksum if stored else replace(row, order_kind=None).checksum)
    assert env.orch.runtime("mock").order_kind == ({"mock:O1": "STOP"} if stored else {})


def test_the_orchestrator_strips_before_validation_and_for_instruments() -> None:
    text = (ROOT / "src/pubsub/orchestrator.py").read_text(encoding="utf-8")
    accept = text[text.index("    def _accept_detail"):]
    assert accept.index("without_optional_payload(dto)") < accept.index("self._invalid(rt, dto)")
    refresh = text[text.index("    async def _refresh_instruments"):]
    assert refresh.index("without_optional_payload(s) for s in specs") < refresh.index("rt.instruments =")


# ---------------------------------------------------------------------------------------------------- pg marker
DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
SCRATCH = "xtnl_s38f_0014"


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_the_core_paths_run_on_a_schema_held_at_0014() -> None:
    asyncpg = importlib.import_module("asyncpg")
    from src.db.epoch import bump_epoch
    from src.db.migrate import migrate
    from src.db.pg import AsyncpgConnection, init_connection
    admin = await asyncpg.connect(DSN)
    try:
        await admin.execute(f"DROP SCHEMA IF EXISTS {SCRATCH} CASCADE")
        await admin.execute(f"CREATE SCHEMA {SCRATCH}")
    finally:
        await admin.close()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2, init=init_connection,
                                     server_settings={"search_path": SCRATCH})
    try:
        async with pool.acquire() as raw:
            held: list[Any] = []
            assert await migrate(AsyncpgConnection(raw), accepted_upto=14, held=held) == list(range(1, 15))
            assert [m.version for m in held][:1] == [15]
            await bump_epoch(AsyncpgConnection(raw), Epoch("s38f", 3), "it", terminate_older=False)
        from src.db.pg import with_schema_compat
        source = await with_schema_compat(pool)
        assert source.legacy_schema and C.legacy_schema_seen()
        db = EngineDb(source, lambda: Epoch("s38f", 3))
        store = PgStateStore(db)
        assert (await store.commit_entity(without_optional_payload(order()))).decision.name == "APPLY"
        await store.commit_instruments([without_optional_payload(instrument())], (1 << 40) + 1)
        orders, _ = await PgLocalStateReader(db).open_entities("stonex")
        assert [o.broker_order_id for o in orders] == ["B1"] and orders[0].order_kind is None
        assert orders[0].checksum == without_optional_payload(order()).checksum
    finally:
        await pool.close()
        admin = await asyncpg.connect(DSN)
        try:
            await admin.execute(f"DROP SCHEMA IF EXISTS {SCRATCH} CASCADE")
        finally:
            await admin.close()
