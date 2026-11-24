"""Live review 2026-11-25: there was no supported way to accept a reviewed market-session schedule, so StoneX
EURUSD / AUDUSD (no trading hours, no holiday source) paged 'market sessions need review' on every start.
`market-sessions accept` writes `reviewed_config` rows as a new calendar version, fenced on the stored epoch,
under the engine writer's advisory lock, with its operator_audit row in the same transaction."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.adapters.sessions import fx_default_schedule
from src.adapters.stonex.sessions import BROKER_NOTE
from src.db.market_sessions import SQL_INSERT_VERSION, SQL_LATEST_ROWS, SQL_LOCK, MarketSessionRow, row_from_db
from src.ops import cli
from src.ops.market_sessions import parse_symbols, sessions_accept
from src.ops.model import Actor, OpsUsageError
from src.ops.service import OpsService

from tests.ops.conftest import outcomes
from tests.ops.emulator import OpsPg
from tests.ops.test_cli import Harness

ALICE = Actor("alice", "INC-90 StoneX FX hours checked against the platform")


class SessionsPg(OpsPg):
    """OpsPg plus the three market_sessions statements (rows live in `self.t`, so ROLLBACK restores them)."""

    def __init__(self) -> None:
        super().__init__()
        self.t.ms = []                     # type: ignore[attr-defined]  # (version, adapter, symbol, ...) tuples
        self._h.update({SQL_LOCK: lambda a: (None,), SQL_LATEST_ROWS: self._latest, SQL_INSERT_VERSION: self._insert})

    @property
    def ms(self) -> list[tuple[Any, ...]]:
        return self.t.ms                   # type: ignore[attr-defined,no-any-return]

    def seed(self, version: int, rows: list[MarketSessionRow]) -> None:
        for r in rows:
            c = r.content()
            self.ms.append((version, c[0], c[1], c[2], c[3], c[4], c[5], c[6] == "true", c[7] == "true", c[8]))

    def _latest(self, a: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        top = max((r[0] for r in self.ms), default=None)
        return sorted((r for r in self.ms if r[0] == top), key=lambda r: (r[1], r[2]))

    def _insert(self, a: tuple[Any, ...]) -> str:
        if not self._fenced(a):
            return "INSERT 0 0"
        version, cols = a[1], a[2:]
        for vals in zip(*cols):
            self.ms.append((version, vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6] == "true",
                            vals[7] == "true", vals[8]))
        return f"INSERT 0 {len(cols[0])}"

    def latest_rows(self) -> dict[tuple[str, str], MarketSessionRow]:
        return {(r.adapter_id, r.symbol): r for r in (row_from_db(x) for x in self._latest(()))}


def stonex_default(symbol: str) -> MarketSessionRow:
    return MarketSessionRow("stonex", symbol, "America/New_York", fx_default_schedule(), source="stonex",
                            is_default=True, holidays_known=False, note=BROKER_NOTE)


def seeded() -> SessionsPg:
    pg = SessionsPg()
    pg.seed(3, [stonex_default("AUDUSD"), stonex_default("EURUSD"),
                MarketSessionRow("pepperstone", "EURUSD", "UTC", fx_default_schedule(), source="ctrader")])
    return pg


async def test_accept_writes_reviewed_rows_as_a_new_fenced_audited_version() -> None:
    pg = seeded()
    rep = await sessions_accept(OpsService(pg, None), "stonex", ["AUDUSD", "EURUSD"], None, ALICE)
    assert rep.outcome == "applied" and rep.fields["calendar_version"] == 4 and rep.fields["previous_version"] == 3
    rows = pg.latest_rows()
    for s in ("AUDUSD", "EURUSD"):
        r = rows[("stonex", s)]
        assert (r.source, r.is_default, r.holidays_known, r.note) == ("reviewed_config", False, False, "")
        assert r.schedule == fx_default_schedule() and r.timezone == "America/New_York"   # the stored hours
    assert rows[("pepperstone", "EURUSD")].source == "ctrader"     # other adapters carried over unchanged
    assert {r[0] for r in pg.ms} == {3, 4}
    assert outcomes(pg) == [("market_sessions.accept", "applied")]
    assert pg.o.audit[0]["target"] == "stonex:AUDUSD,EURUSD"
    assert pg.statements(SQL_LOCK) == 1                            # the engine writer's advisory lock


async def test_accept_with_a_holiday_list_marks_holidays_known() -> None:
    pg = seeded()
    hol = [{"date": "2026-12-25"}, {"date": "2027-01-01"}]
    rep = await sessions_accept(OpsService(pg, None), "stonex", ["EURUSD"], hol, ALICE, holidays_source="h.json")
    assert rep.outcome == "applied"
    r = pg.latest_rows()[("stonex", "EURUSD")]
    assert r.holidays_known is True and [dict(h) for h in r.holidays] == hol
    assert pg.latest_rows()[("stonex", "AUDUSD")].source == "stonex"   # only the named symbol


async def test_an_unchanged_accept_is_a_noop_and_writes_no_version() -> None:
    pg = seeded()
    svc = OpsService(pg, None)
    await sessions_accept(svc, "stonex", ["AUDUSD", "EURUSD"], None, ALICE)
    rep = await sessions_accept(svc, "stonex", ["AUDUSD", "EURUSD"], None, ALICE)
    assert rep.outcome == "noop" and rep.fields["changed"] is False
    assert {r[0] for r in pg.ms} == {3, 4}
    assert outcomes(pg) == [("market_sessions.accept", "applied"), ("market_sessions.accept", "noop")]


async def test_accept_refuses_a_symbol_without_a_stored_row_and_writes_nothing() -> None:
    pg = seeded()
    rep = await sessions_accept(OpsService(pg, None), "stonex", ["EURUSD", "USDJPY"], None, ALICE)
    assert rep.outcome == "refused" and rep.fields["refused"] == "not_found" and "USDJPY" in rep.fields["detail"]
    assert {r[0] for r in pg.ms} == {3}
    assert outcomes(pg) == [("market_sessions.accept", "refused")]


async def test_accept_refuses_a_holiday_the_engine_would_reject() -> None:
    pg = seeded()
    rep = await sessions_accept(OpsService(pg, None), "stonex", ["EURUSD"], [{"date": "not-a-date"}], ALICE)
    assert rep.outcome == "refused" and rep.fields["refused"] == "invalid_schedule"
    assert {r[0] for r in pg.ms} == {3}


async def test_accept_is_refused_when_the_epoch_moves() -> None:
    pg = seeded()
    pg.before[SQL_INSERT_VERSION] = lambda: pg.bump(8)            # a LEADER_INIT bump commits meanwhile
    rep = await sessions_accept(OpsService(pg, None), "stonex", ["EURUSD"], None, ALICE)
    assert rep.outcome == "refused" and rep.fields["refused"] == "epoch_moved"
    assert {r[0] for r in pg.ms} == {3}                            # rolled back: no half-written version


def test_symbol_parsing() -> None:
    assert parse_symbols(" EURUSD, AUDUSD,EURUSD ") == ["AUDUSD", "EURUSD"]
    for bad in ("", " , ", "EUR USD"):
        with pytest.raises(OpsUsageError):
            parse_symbols(bad)


def test_cli_accept_and_show_end_to_end(tmp_path: Path) -> None:
    h = Harness()
    h.pg = seeded()
    hol = tmp_path / "hol.json"
    hol.write_text(json.dumps([{"date": "2026-12-25"}]), encoding="utf-8")
    code, out, err = h.run("market-sessions", "accept", "--adapter", "stonex", "--symbols", "EURUSD,AUDUSD",
                           "--holidays", str(hol), "--operator", "alice", "--reason", ALICE.reason)
    assert code == cli.EXIT_OK, err
    assert "outcome: applied" in out and "calendar_version: 4" in out
    code, out, err = h.run("market-sessions", "show", "--adapter", "stonex")
    assert code == cli.EXIT_OK, err
    assert out.count("source=reviewed_config") == 2 and "holidays_known=True" in out
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    code, _, err = h.run("market-sessions", "accept", "--adapter", "stonex", "--symbols", "EURUSD",
                         "--holidays", str(bad), "--operator", "alice", "--reason", ALICE.reason)
    assert code == cli.EXIT_USAGE and "not JSON" in err
    code, _, err = h.run("market-sessions", "accept", "--adapter", "stonex", "--symbols", "EURUSD",
                         "--holidays", str(tmp_path / "missing.json"), "--operator", "alice", "--reason",
                         ALICE.reason)
    assert code == cli.EXIT_USAGE and "cannot read" in err


def test_show_flags_unreviewed_rows() -> None:
    h = Harness()
    h.pg = seeded()
    code, out, err = h.run("market-sessions", "show")
    assert code == cli.EXIT_OK, err
    assert "calendar_version: 3" in out and "2 row(s) flagged for review" in out
    assert h.pg.o.audit == []                                      # read-only, not audited
