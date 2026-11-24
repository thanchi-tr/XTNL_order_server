"""Live review round 3: an operator's `market-sessions accept` is never lost to the engine's concurrent schedule save
(operations.md s10 / SM10 Review and Accept: the accept writes a new calendar_version under the engine writer's lock;
the leader loads it at its next schedule save). The saver builds its rows from `latest()` before the lock, so `save`
re-merges under the lock and keeps every `reviewed_config` row committed after that read."""
from __future__ import annotations

import json
from typing import Any

from src.db import market_sessions as M
from src.db.epoch import EngineDb

from tests.db.conftest import FakeConn

FX = {"sessions": [{"open": {"day": "SUN", "time": "17:00"}, "close": {"day": "FRI", "time": "17:00"}}],
      "breaks": [{"start": "17:00", "end": "17:05", "days": ["MON", "TUE", "WED", "THU"]}]}
FX2 = {"sessions": [{"open": {"day": "SUN", "time": "17:05"}, "close": {"day": "FRI", "time": "16:55"}}]}
XMAS = [{"date": "2026-12-25"}]


def default(symbol: str, schedule: dict[str, Any] | None = None) -> M.MarketSessionRow:
    """StoneX's flagged default FX hours (no trading hours, no holiday source)."""
    return M.MarketSessionRow("stonex", symbol, "America/New_York", schedule or FX, (), "stonex", is_default=True,
                              holidays_known=False, note="fx default")


def broker(symbol: str, schedule: dict[str, Any] | None = None) -> M.MarketSessionRow:
    return M.MarketSessionRow("stonex", symbol, "America/New_York", schedule or FX, (), "stonex")


def reviewed(symbol: str, adapter: str = "stonex", holidays: list[dict[str, str]] | None = None) -> M.MarketSessionRow:
    return M.MarketSessionRow(adapter, symbol, "America/New_York", FX, tuple(holidays or ()), "reviewed_config",
                              is_default=False, holidays_known=holidays is not None, note="")


def db_row(version: int, r: M.MarketSessionRow) -> tuple[Any, ...]:
    return (version, r.adapter_id, r.symbol, r.timezone, json.dumps(dict(r.schedule)), list(r.holidays), r.source,
            r.is_default, r.holidays_known, r.note)


def version(v: int, *rows: M.MarketSessionRow) -> list[tuple[Any, ...]]:
    return [db_row(v, r) for r in sorted(rows, key=lambda r: r.key)]


def inserted(conn: FakeConn) -> dict[tuple[str, str], tuple[str, ...]]:
    (args,) = conn.args_of(M.SQL_INSERT_VERSION)
    cols = [list(c) for c in args[2:]]
    return {(row[0], row[1]): tuple(row) for row in zip(*cols)}


async def test_accept_committed_after_the_saver_read_is_not_overwritten(db: EngineDb, conn: FakeConn) -> None:
    """Saver reads v4 (EURUSD a flagged default), the operator accepts EURUSD (v5), the saver then saves the same
    flagged default it built from v4: v5's reviewed row stays, nothing is written (before: v6 dropped it)."""
    v4 = version(4, default("EURUSD"))
    conn.returns_all(M.SQL_LATEST_ROWS, v4, version(5, reviewed("EURUSD")))
    conn.returns_all(M.SQL_VERSION_ROWS, v4)
    store = M.PgMarketSessionStore(db)
    read_version, _ = await store.latest()
    assert read_version == 4
    res = await store.save([default("EURUSD")], adapters=["stonex"])
    assert res == M.SaveResult(5, False) and M.SQL_INSERT_VERSION not in conn.sql
    assert conn.args_of(M.SQL_VERSION_ROWS) == [(4,)]
    assert conn.sql.index(M.SQL_LOCK) < conn.sql.index(M.SQL_VERSION_ROWS)       # re-merged under the lock


async def test_other_changes_are_saved_with_the_accepted_rows_kept(db: EngineDb, conn: FakeConn) -> None:
    v4 = version(4, default("AUDUSD"), default("EURUSD"))
    v5 = version(5, reviewed("AUDUSD", holidays=XMAS), reviewed("EURUSD"))
    conn.returns_all(M.SQL_LATEST_ROWS, v4, v5)
    conn.returns_all(M.SQL_VERSION_ROWS, v4)
    store = M.PgMarketSessionStore(db)
    await store.latest()
    res = await store.save([default("AUDUSD"), default("EURUSD"), broker("USDJPY")], adapters=["stonex"])
    assert res == M.SaveResult(6, True)
    rows = inserted(conn)
    assert sorted(rows) == [("stonex", "AUDUSD"), ("stonex", "EURUSD"), ("stonex", "USDJPY")]
    assert rows[("stonex", "AUDUSD")] == reviewed("AUDUSD", holidays=XMAS).content()
    assert rows[("stonex", "EURUSD")] == reviewed("EURUSD").content()
    assert rows[("stonex", "USDJPY")] == broker("USDJPY").content()


async def test_a_reviewed_row_the_saver_read_still_follows_the_saver(db: EngineDb, conn: FakeConn) -> None:
    """A reviewed row already in the saver's read is the saver's decision (its merge replaces it with real broker
    hours): only rows committed after the read are re-merged."""
    v4 = version(4, reviewed("EURUSD"), default("AUDUSD"))
    v5 = version(5, reviewed("EURUSD"), reviewed("AUDUSD"))
    conn.returns_all(M.SQL_LATEST_ROWS, v4, v5)
    conn.returns_all(M.SQL_VERSION_ROWS, v4)
    store = M.PgMarketSessionStore(db)
    await store.latest()
    res = await store.save([broker("EURUSD", FX2), default("AUDUSD")], adapters=["stonex"])
    assert res == M.SaveResult(6, True)
    rows = inserted(conn)
    assert rows[("stonex", "EURUSD")] == broker("EURUSD", FX2).content()           # saver's decision stands
    assert rows[("stonex", "AUDUSD")] == reviewed("AUDUSD").content()              # the new accept is kept


async def test_a_re_accept_with_holidays_after_the_read_is_kept(db: EngineDb, conn: FakeConn) -> None:
    v4 = version(4, reviewed("EURUSD"))
    conn.returns_all(M.SQL_LATEST_ROWS, v4, version(5, reviewed("EURUSD", holidays=XMAS)))
    conn.returns_all(M.SQL_VERSION_ROWS, v4)
    store = M.PgMarketSessionStore(db)
    await store.latest()
    # the saver kept the reviewed row it read (the broker gave a flagged default)
    res = await store.save([reviewed("EURUSD")], adapters=["stonex"])
    assert res == M.SaveResult(5, False) and M.SQL_INSERT_VERSION not in conn.sql


async def test_explicit_base_version_and_a_first_accept(db: EngineDb, conn: FakeConn) -> None:
    """`base_version=0`: nothing was stored when the saver read, so every reviewed row now stored is newer."""
    conn.returns_all(M.SQL_LATEST_ROWS, version(1, reviewed("EURUSD"), reviewed("GBPUSD", adapter="pepperstone")))
    res = await M.PgMarketSessionStore(db).save([default("EURUSD"), broker("AUDUSD")], adapters=["stonex"],
                                                base_version=0)
    assert res == M.SaveResult(2, True)
    rows = inserted(conn)
    assert rows[("stonex", "EURUSD")] == reviewed("EURUSD").content()
    assert rows[("pepperstone", "GBPUSD")] == reviewed("GBPUSD", adapter="pepperstone").content()
    assert M.SQL_VERSION_ROWS not in conn.sql                                       # version 0 has no rows


async def test_no_newer_version_means_no_re_merge(db: EngineDb, conn: FakeConn) -> None:
    v4 = version(4, reviewed("EURUSD"))
    conn.returns_all(M.SQL_LATEST_ROWS, v4, v4)
    store = M.PgMarketSessionStore(db)
    await store.latest()
    res = await store.save([broker("EURUSD", FX2)], adapters=["stonex"])
    assert res == M.SaveResult(5, True) and M.SQL_VERSION_ROWS not in conn.sql
    assert inserted(conn)[("stonex", "EURUSD")] == broker("EURUSD", FX2).content()


async def test_one_read_serves_one_save(db: EngineDb, conn: FakeConn) -> None:
    """A save without a fresh read has no base to re-merge against (the earlier read is consumed)."""
    conn.returns_all(M.SQL_LATEST_ROWS, version(4, default("EURUSD")), version(4, default("EURUSD")),
                     version(5, reviewed("EURUSD")))
    store = M.PgMarketSessionStore(db)
    await store.latest()
    assert not (await store.save([default("EURUSD")], adapters=["stonex"])).changed
    res = await store.save([default("EURUSD")], adapters=["stonex"])
    assert res == M.SaveResult(6, True) and M.SQL_VERSION_ROWS not in conn.sql


def test_version_rows_read_matches_the_latest_columns() -> None:
    cols = M.SQL_LATEST_ROWS.split(" FROM ")[0]
    assert M.SQL_VERSION_ROWS.startswith(cols) and "calendar_version = $1::bigint" in M.SQL_VERSION_ROWS
