"""SM10 calendar persistence (operations.md s10, tier-persistence.md s4; IMPLEMENTATION_PLAN 5.5 for 5.1):
versioned market_sessions rows, a new version only when the content changes, other adapters carried over."""
from __future__ import annotations

import json
from typing import Any

import pytest

from src.calendar.schedule import ScheduleError
from src.db import market_sessions as M
from src.db.epoch import EngineDb
from src.db.fence import FENCE, EpochMismatch
from src.runtime.pg_ports import SQL_LATEST_CALENDAR

from tests.db.conftest import FakeConn

FX = {"sessions": [{"open": {"day": "SUN", "time": "17:00"}, "close": {"day": "FRI", "time": "17:00"}}],
      "breaks": [{"start": "17:00", "end": "17:05", "days": ["MON", "TUE", "WED", "THU"]}]}
XMAS = [{"date": "2026-12-25"}]


def row(adapter: str = "pepperstone", symbol: str = "EURUSD", schedule: dict[str, Any] | None = None,
        holidays: list[dict[str, str]] | None = None, source: M.SessionSource = "ctrader") -> M.MarketSessionRow:
    return M.MarketSessionRow(adapter, symbol, "America/New_York", schedule or FX, tuple(holidays or XMAS), source)


def db_row(version: int, r: M.MarketSessionRow) -> tuple[Any, ...]:
    return (version, r.adapter_id, r.symbol, r.timezone, json.dumps(dict(r.schedule)), list(r.holidays), r.source)


def test_insert_is_fenced_and_matches_the_loader_columns() -> None:
    assert FENCE in M.SQL_INSERT_VERSION and "$1::bigint" in M.SQL_INSERT_VERSION
    # the loader reads exactly the latest version, the store writes whole versions
    assert "max(calendar_version)" in M.SQL_LATEST_ROWS and "max(calendar_version)" in SQL_LATEST_CALENDAR


async def test_first_save_writes_version_1(db: EngineDb, conn: FakeConn) -> None:
    res = await M.PgMarketSessionStore(db).save([row(), row(symbol="GBPUSD")], adapters=["pepperstone"])
    assert res == M.SaveResult(1, True)
    (args,) = conn.args_of(M.SQL_INSERT_VERSION)
    assert args[0] == 7 and args[1] == 1
    assert args[2] == ["pepperstone", "pepperstone"] and args[3] == ["EURUSD", "GBPUSD"]
    assert json.loads(args[5][0]) == FX and json.loads(args[6][0]) == XMAS and args[7] == ["ctrader", "ctrader"]
    assert conn.sql.index(M.SQL_LOCK) < conn.sql.index(M.SQL_LATEST_ROWS) < conn.sql.index(M.SQL_INSERT_VERSION)


async def test_unchanged_content_writes_nothing(db: EngineDb, conn: FakeConn) -> None:
    conn.returns_all(M.SQL_LATEST_ROWS, [db_row(4, row()), db_row(4, row("stonex", "EURUSD", source="stonex"))])
    res = await M.PgMarketSessionStore(db).save([row()], adapters=["pepperstone"])
    assert res == M.SaveResult(4, False) and M.SQL_INSERT_VERSION not in conn.sql


async def test_changed_adapter_replaces_its_rows_and_carries_the_others_over(db: EngineDb, conn: FakeConn) -> None:
    stonex = row("stonex", "EURUSD", source="stonex")
    conn.returns_all(M.SQL_LATEST_ROWS, [db_row(4, row()), db_row(4, row(symbol="USDJPY")), db_row(4, stonex)])
    new_holidays = XMAS + [{"date": "2027-01-01"}]
    res = await M.PgMarketSessionStore(db).save([row(holidays=new_holidays)], adapters=["pepperstone"])
    assert res == M.SaveResult(5, True)
    (args,) = conn.args_of(M.SQL_INSERT_VERSION)
    assert args[1] == 5
    assert list(zip(args[2], args[3])) == [("pepperstone", "EURUSD"), ("stonex", "EURUSD")]   # USDJPY dropped
    assert json.loads(args[6][0]) == new_holidays


async def test_invalid_rows_are_refused_before_any_write(db: EngineDb, conn: FakeConn) -> None:
    store = M.PgMarketSessionStore(db)
    with pytest.raises(ScheduleError):
        await store.save([row(schedule={"sessions": []})], adapters=["pepperstone"])
    with pytest.raises(ScheduleError):
        await store.save([M.MarketSessionRow("pepperstone", "X", "Mars/Olympus", FX)], adapters=["pepperstone"])
    with pytest.raises(ScheduleError):
        await store.save([row("stonex")], adapters=["pepperstone"])           # not one of the named adapters
    with pytest.raises(ScheduleError):
        await store.save([row(), row()], adapters=["pepperstone"])            # duplicate
    with pytest.raises(ScheduleError):
        await store.save([row(source="guess")], adapters=["pepperstone"])  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        await store.save([row()], adapters=[])
    assert conn.sql == []


async def test_lost_fence_raises(db: EngineDb, conn: FakeConn) -> None:
    conn.stored_epoch = 99
    with pytest.raises(EpochMismatch):
        await M.PgMarketSessionStore(db).save([row()], adapters=["pepperstone"])


async def test_latest_reads_back_rows(db: EngineDb, conn: FakeConn) -> None:
    conn.returns_all(M.SQL_LATEST_ROWS, [db_row(3, row())])
    version, rows = await M.PgMarketSessionStore(db).latest()
    assert version == 3 and [r.content() for r in rows] == [row().content()]


# ---- 5.1 contract (src/adapters/base.py MarketSessions) -> row, without loss --------------------------------
def test_from_sessions_maps_the_5_1_contract_without_loss() -> None:
    """What 5.1's fetch_market_sessions() returns is stored as is: the mock source is accepted, and the review
    flags (is_default, holidays_known, note) are kept (0010 columns) instead of being dropped."""
    from src.adapters.base import MarketSessions

    ms = MarketSessions("mock", "EURUSD", "America/New_York", dict(FX), list(XMAS), source="mock", is_default=True,
                        holidays_known=False, note="fx default: no broker hours")
    r = M.MarketSessionRow.from_sessions(ms)
    r.validate()
    assert (r.adapter_id, r.symbol, r.timezone, r.source) == ("mock", "EURUSD", "America/New_York", "mock")
    assert dict(r.schedule) == FX and list(r.holidays) == XMAS
    assert (r.is_default, r.holidays_known, r.note) == (True, False, "fx default: no broker hours")
    for src in ("ctrader", "stonex"):
        M.MarketSessionRow.from_sessions(MarketSessions("a", "EURUSD", "UTC", dict(FX), source=src)).validate()


async def test_review_flags_are_written_read_back_and_versioned(db: EngineDb, conn: FakeConn) -> None:
    flagged = M.MarketSessionRow("stonex", "EURUSD", "America/New_York", FX, (), "stonex", is_default=True,
                                 holidays_known=False, note="default hours")
    res = await M.PgMarketSessionStore(db).save([flagged], adapters=["stonex"])
    assert res == M.SaveResult(1, True)
    (args,) = conn.args_of(M.SQL_INSERT_VERSION)
    assert args[8:] == (["true"], ["false"], ["default hours"])
    assert "is_default, holidays_known, note" in M.SQL_INSERT_VERSION and "note FROM market_sessions" in M.SQL_LATEST_ROWS
    # read back with the flags; the same schedule now from the broker (not a default) is a new version
    stored = (2, "stonex", "EURUSD", "America/New_York", json.dumps(FX), [], "stonex", True, False, "default hours")
    assert M.row_from_db(stored) == flagged
    conn.returns_all(M.SQL_LATEST_ROWS, [stored])
    broker = M.MarketSessionRow("stonex", "EURUSD", "America/New_York", FX, (), "stonex")
    assert (await M.PgMarketSessionStore(db).save([broker], adapters=["stonex"])).changed
    conn.returns_all(M.SQL_LATEST_ROWS, [stored])
    assert not (await M.PgMarketSessionStore(db).save([flagged], adapters=["stonex"])).changed


def test_migration_0010_adds_the_review_columns_and_dlq_last_error() -> None:
    from src.db.migrate import MIGRATIONS_DIR

    sql = (MIGRATIONS_DIR / "0010_dlq_history_last_error_session_review.sql").read_text(encoding="utf-8")
    for col in ("is_default boolean NOT NULL DEFAULT false", "holidays_known boolean NOT NULL DEFAULT true",
                "note text NOT NULL DEFAULT ''"):
        assert f"ALTER TABLE market_sessions ADD COLUMN IF NOT EXISTS {col}" in sql
    assert "ALTER TABLE events_dlq_history ADD COLUMN IF NOT EXISTS last_error text" in sql
