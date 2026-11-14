"""Broker schedule fetch -> market_sessions -> calendar (src/runtime/schedule_refresh.py; operations.md §10;
IMPLEMENTATION_PLAN 5.1 / 5.5 / 5.6). No network: scripted adapters and an in-memory store."""
from __future__ import annotations

from collections.abc import Collection, Sequence
from typing import Any

import pytest

from src.adapters.base import MarketSessions
from src.adapters.sessions import fx_default_schedule, fx_default_sessions
from src.db.market_sessions import MarketSessionRow, SaveResult, merge_rows, same_content
from src.runtime.schedule_refresh import ScheduleRefresher


def real(adapter_id: str, symbol: str, **kw: Any) -> MarketSessions:
    return MarketSessions(adapter_id=adapter_id, symbol=symbol, timezone="America/New_York",
                          schedule=fx_default_schedule(), holidays=[], source="ctrader", **kw)


class Adapter:
    def __init__(self, adapter_id: str) -> None:
        self.adapter_id = adapter_id
        self.asked: list[list[str]] = []
        self.answer: dict[str, MarketSessions] | Exception = {}

    async def fetch_market_sessions(self, symbols: Sequence[str]) -> dict[str, MarketSessions]:
        self.asked.append(list(symbols))
        if isinstance(self.answer, Exception):
            raise self.answer
        return {k: v for k, v in self.answer.items() if k in symbols}


class Store:
    def __init__(self, rows: Sequence[MarketSessionRow] = ()) -> None:
        self.version = 1 if rows else 0
        self.rows = list(rows)
        self.saves: list[tuple[list[MarketSessionRow], list[str]]] = []

    async def latest(self) -> tuple[int, list[MarketSessionRow]]:
        return self.version, list(self.rows)

    async def save(self, rows: Sequence[MarketSessionRow], *, adapters: Collection[str]) -> SaveResult:
        self.saves.append((list(rows), sorted(adapters)))
        merged = merge_rows(self.rows, rows, set(adapters))
        if same_content(merged, self.rows):
            return SaveResult(self.version, False)
        self.version += 1
        self.rows = merged
        return SaveResult(self.version, True)


class World:
    def __init__(self, store: Store | None = None) -> None:
        self.now = 1_000_000
        self.adapters = {"pepperstone": Adapter("pepperstone")}
        self.streaming = {"pepperstone": False}
        self.wanted: dict[str, list[str]] = {"pepperstone": ["EURUSD"]}
        self.store = store or Store()
        self.calendar_version: int | None = 0
        self.reloads = 0
        self.pages: list[tuple[str, str, str]] = []
        self.r = ScheduleRefresher(
            self.store, adapter_ids=lambda: list(self.adapters), adapter=lambda a: self.adapters[a],  # type: ignore[arg-type,return-value]
            ready=lambda a: self.streaming[a], wanted_symbols=lambda a: self.wanted[a],
            stored_symbols=lambda a: sorted(r.symbol for r in self.store.rows if r.adapter_id == a),
            reload=self.reload, calendar_version=lambda: self.calendar_version,
            pager=lambda *p: self.pages.append(p), clock_ms=lambda: self.now, refresh_ms=6 * 3_600_000,
            retry_ms=300_000)

    async def reload(self) -> int:
        self.reloads += 1
        self.calendar_version = self.store.version
        return self.store.version


async def test_fetch_waits_for_streaming_then_saves_and_reloads_the_calendar() -> None:
    """Before 5.6: nothing ever wrote market_sessions; the calendar stayed at version 0."""
    w = World()
    w.adapters["pepperstone"].answer = {"EURUSD": real("pepperstone", "EURUSD")}
    out = await w.r.pass_once()
    assert out.fetched == [] and w.adapters["pepperstone"].asked == []     # not connected yet
    w.streaming["pepperstone"] = True
    out = await w.r.pass_once()
    assert out.fetched == ["pepperstone"] and out.changed and out.reloaded
    assert w.store.version == 1 and w.calendar_version == 1 and w.reloads == 1
    assert [r.symbol for r in w.store.rows] == ["EURUSD"]
    out = await w.r.pass_once()                                             # not due again yet
    assert out.fetched == [] and len(w.adapters["pepperstone"].asked) == 1


async def test_refetch_after_the_interval_after_a_roll_and_for_a_new_symbol() -> None:
    w = World()
    w.streaming["pepperstone"] = True
    a = w.adapters["pepperstone"]
    a.answer = {s: real("pepperstone", s) for s in ("EURUSD", "USDJPY")}
    await w.r.pass_once()
    w.wanted["pepperstone"] = ["EURUSD", "USDJPY"]                         # a new position's symbol
    await w.r.pass_once()
    assert a.asked[-1] == ["EURUSD", "USDJPY"]                              # stored + wanted
    w.r.request_all()                                                       # the 17:00 New York roll
    await w.r.pass_once()
    assert len(a.asked) == 3
    w.now += 6 * 3_600_000                                                  # DST-safe periodic re-fetch
    out = await w.r.pass_once()
    assert len(a.asked) == 4 and not out.changed and not out.reloaded       # unchanged: no new version


async def test_a_failed_fetch_keeps_the_version_and_retries_later() -> None:
    w = World()
    w.streaming["pepperstone"] = True
    a = w.adapters["pepperstone"]
    a.answer = ConnectionError("broker down")
    out = await w.r.pass_once()
    assert out.failed == ["pepperstone"] and w.store.saves == [] and w.reloads == 0
    a.answer = {"EURUSD": real("pepperstone", "EURUSD")}
    await w.r.pass_once()
    assert len(a.asked) == 1                                                # retry_ms not over
    w.now += 300_000
    out = await w.r.pass_once()
    assert out.fetched == ["pepperstone"]


async def test_review_flags_page_once_and_reviewed_rows_survive_a_default() -> None:
    reviewed = MarketSessionRow("pepperstone", "US500", "America/New_York", fx_default_schedule(),
                                source="reviewed_config")
    w = World(Store([reviewed]))
    w.streaming["pepperstone"] = True
    w.wanted["pepperstone"] = ["EURUSD", "GBPUSD"]
    w.adapters["pepperstone"].answer = {
        "EURUSD": real("pepperstone", "EURUSD", note="no scheduleTimeZone; UTC assumed"),
        "US500": fx_default_sessions("pepperstone", "US500", source="ctrader")}
    out = await w.r.pass_once()
    assert out.changed
    saved = {r.symbol: r for r in w.store.rows}
    assert saved["US500"].source == "reviewed_config"                       # not replaced by a flagged default
    assert [p[1] for p in w.pages] == ["market sessions need review: pepperstone"]
    body = w.pages[0][2]
    assert "GBPUSD" in body and "US500" not in body.split("no sessions for")[1].split(";")[0]
    assert "UTC assumed" in body
    w.r.request_all()
    await w.r.pass_once()
    assert len(w.pages) == 1                                                # same findings: paged once per term


async def test_an_adapter_that_returns_nothing_is_not_saved_and_an_unusable_row_is_dropped() -> None:
    stored = MarketSessionRow("pepperstone", "EURUSD", "America/New_York", fx_default_schedule(), source="ctrader")
    w = World(Store([stored]))
    w.streaming["pepperstone"] = True
    out = await w.r.pass_once()                                             # {}: nothing usable
    assert out.failed == ["pepperstone"] and w.store.saves == []
    assert w.store.rows == [stored]                                         # rows never wiped
    w.now += 300_000
    bad = MarketSessions("pepperstone", "EURUSD", "Not/AZone", fx_default_schedule(), [], "ctrader")
    w.adapters["pepperstone"].answer = {"EURUSD": bad}
    out = await w.r.pass_once()
    assert out.failed == ["pepperstone"] and any("unusable" in p[1] for p in w.pages)


async def test_a_lost_fence_propagates() -> None:
    class EpochMismatch(Exception):
        pass

    class FencedStore(Store):
        async def save(self, rows: Sequence[MarketSessionRow], *, adapters: Collection[str]) -> SaveResult:
            raise EpochMismatch("fence")
    w = World(FencedStore())
    w.streaming["pepperstone"] = True
    w.adapters["pepperstone"].answer = {"EURUSD": real("pepperstone", "EURUSD")}
    with pytest.raises(EpochMismatch):
        await w.r.pass_once()


async def test_a_stored_symbol_missing_from_a_partial_answer_keeps_its_stored_row() -> None:
    """Before the fix: the save replaced all of the adapter's rows with the answer, so a symbol the broker left
    out once (a partial or flaky answer) vanished from the calendar version (unknown state reads as OPEN)."""
    eur = MarketSessionRow("pepperstone", "EURUSD", "America/New_York", fx_default_schedule(), source="ctrader")
    jpy = MarketSessionRow("pepperstone", "USDJPY", "America/New_York", fx_default_schedule(), source="ctrader")
    w = World(Store([eur, jpy]))
    w.streaming["pepperstone"] = True
    w.adapters["pepperstone"].answer = {"EURUSD": real("pepperstone", "EURUSD")}     # USDJPY left out
    out = await w.r.pass_once()
    assert out.fetched == ["pepperstone"]
    assert sorted(w.adapters["pepperstone"].asked[-1]) == ["EURUSD", "USDJPY"]
    saved_rows, saved_adapters = w.store.saves[-1]
    assert sorted(r.symbol for r in saved_rows) == ["EURUSD", "USDJPY"] and saved_adapters == ["pepperstone"]
    assert sorted(r.symbol for r in w.store.rows) == ["EURUSD", "USDJPY"]
    assert any("no sessions for USDJPY" in p[2] for p in w.pages)          # still paged for review
