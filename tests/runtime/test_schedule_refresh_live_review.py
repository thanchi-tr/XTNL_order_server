"""Live review 2026-11-25: 'market sessions need review: stonex' went out on every engine start (6 pages
2026-11-21..24) for StoneX EURUSD / AUDUSD, which have no trading hours and no holiday source. Before the fix
`_review` looked at the raw broker rows only (an operator's `reviewed_config` row could never silence it) and the
paged set lived in memory (a new term paged it again). No network: scripted adapters, in-memory store and dedupe."""
from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest

from src.adapters.base import MarketSessions
from src.adapters.sessions import fx_default_schedule, fx_default_sessions
from src.adapters.stonex.sessions import BROKER_NOTE
from src.db.market_sessions import MarketSessionRow
from src.runtime.schedule_refresh import ReviewKey, ScheduleRefresher, review_episode_keys

from tests.runtime.test_schedule_refresh import Adapter, Store, World

TITLE = "market sessions need review: stonex"


class Dedupe:
    """notification_dedupe in memory (PgDedupeStore's last_sent_of / record)."""

    def __init__(self) -> None:
        self.rows: dict[ReviewKey, int] = {}
        self.fail_read = False

    async def last_sent_of(self, keys: Sequence[ReviewKey]) -> Mapping[ReviewKey, int]:
        if self.fail_read:
            raise ConnectionError("postgres down")
        return {k: self.rows[k] for k in keys if k in self.rows}

    async def record(self, key: ReviewKey, at_ms: int) -> None:
        self.rows[key] = at_ms


def stonex_default(symbol: str) -> MarketSessions:
    """What the StoneX adapter returns for an FX pair without trading hours (src/adapters/stonex/sessions.py)."""
    return fx_default_sessions("stonex", symbol, source="stonex", note=BROKER_NOTE)


def reviewed(symbol: str, *, holidays_known: bool = False) -> MarketSessionRow:
    return MarketSessionRow("stonex", symbol, "America/New_York", fx_default_schedule(), source="reviewed_config",
                            holidays_known=holidays_known)


def stonex_world(store: Store, dedupe: Dedupe | None, symbols: Sequence[str] = ("AUDUSD", "EURUSD")) -> World:
    w = World(store)
    w.adapters = {"stonex": Adapter("stonex")}
    w.streaming = {"stonex": True}
    w.wanted = {"stonex": list(symbols)}
    w.adapters["stonex"].answer = {s: stonex_default(s) for s in symbols}
    new_term(w, dedupe)
    return w


def new_term(w: World, dedupe: Dedupe | None) -> None:
    """A fresh ScheduleRefresher over the same store: what a new leader term (an engine start) builds."""
    w.r = ScheduleRefresher(
        w.store, adapter_ids=lambda: list(w.adapters), adapter=lambda a: w.adapters[a],  # type: ignore[arg-type,return-value]
        ready=lambda a: w.streaming[a], wanted_symbols=lambda a: w.wanted[a],
        stored_symbols=lambda a: sorted(r.symbol for r in w.store.rows if r.adapter_id == a),
        reload=w.reload, calendar_version=lambda: w.calendar_version,
        pager=lambda *p: w.pages.append(p), clock_ms=lambda: w.now, refresh_ms=6 * 3_600_000, retry_ms=300_000,
        review_dedupe=dedupe)


def review_pages(w: World) -> list[tuple[str, str, str]]:
    return [p for p in w.pages if p[1] == TITLE]


async def test_reviewed_config_rows_silence_the_stonex_default_review() -> None:
    """Before: the page listed 'documented default hours (review) for AUDUSD, EURUSD' although both had a stored
    reviewed_config row (which the merge keeps over a flagged default)."""
    w = stonex_world(Store([reviewed("AUDUSD"), reviewed("EURUSD")]), Dedupe())
    out = await w.r.pass_once()
    assert out.fetched == ["stonex"]
    assert review_pages(w) == []
    assert {r.symbol: r.source for r in w.store.rows} == {"AUDUSD": "reviewed_config", "EURUSD": "reviewed_config"}


async def test_an_unreviewed_symbol_and_a_missing_one_are_still_paged() -> None:
    w = stonex_world(Store([reviewed("EURUSD"), reviewed("GBPUSD")]), Dedupe(),
                     symbols=("AUDUSD", "EURUSD", "GBPUSD"))
    w.adapters["stonex"].answer = {s: stonex_default(s) for s in ("AUDUSD", "EURUSD")}     # GBPUSD left out
    await w.r.pass_once()
    pages = review_pages(w)
    assert len(pages) == 1
    body = pages[0][2]
    assert "no sessions for GBPUSD" in body                       # missing: reported even with a reviewed row
    assert "documented default hours (review) for AUDUSD" in body
    assert "EURUSD" not in body                                   # accepted by the operator
    assert "holidays unknown for 1 symbol(s)" in body


async def test_a_real_broker_schedule_replaces_the_reviewed_row_and_is_reviewed_again() -> None:
    """The merge keeps a reviewed row only over nothing or a flagged default; the review follows the merge."""
    w = stonex_world(Store([reviewed("EURUSD")]), Dedupe(), symbols=("EURUSD",))
    w.adapters["stonex"].answer = {"EURUSD": MarketSessions("stonex", "EURUSD", "UTC", fx_default_schedule(), [],
                                                            "stonex", is_default=False, holidays_known=False,
                                                            note=BROKER_NOTE)}
    await w.r.pass_once()
    assert w.store.rows[0].source == "stonex"
    assert len(review_pages(w)) == 1 and "holidays unknown" in review_pages(w)[0][2]


async def test_an_unchanged_review_set_is_not_paged_again_by_a_new_term() -> None:
    """Before: `_review_paged` lived in memory, so every engine start paged the same set again."""
    dedupe = Dedupe()
    w = stonex_world(Store(), dedupe)
    await w.r.pass_once()
    assert len(review_pages(w)) == 1
    adapter_key = review_episode_keys("stonex", ())[0]
    assert adapter_key in dedupe.rows and len(dedupe.rows) == 2   # the adapter's last page and the set's own row
    for _ in range(3):                                            # three engine restarts, same answer
        w.now += 60_000
        new_term(w, dedupe)
        await w.r.pass_once()
    assert len(review_pages(w)) == 1


async def test_a_changed_set_is_paged_and_a_set_seen_again_after_another_is_paged_again() -> None:
    dedupe = Dedupe()
    w = stonex_world(Store(), dedupe, symbols=("EURUSD",))
    await w.r.pass_once()
    assert len(review_pages(w)) == 1
    w.now += 60_000
    w.wanted["stonex"] = ["AUDUSD", "EURUSD"]                     # a new symbol: a different set
    w.adapters["stonex"].answer = {s: stonex_default(s) for s in ("AUDUSD", "EURUSD")}
    new_term(w, dedupe)
    await w.r.pass_once()
    assert len(review_pages(w)) == 2 and "AUDUSD" in review_pages(w)[1][2]
    w.now += 60_000                                               # back to the first set (AUDUSD dropped)
    w.store.rows = [r for r in w.store.rows if r.symbol == "EURUSD"]
    w.wanted["stonex"] = ["EURUSD"]
    w.adapters["stonex"].answer = {"EURUSD": stonex_default("EURUSD")}
    new_term(w, dedupe)
    await w.r.pass_once()
    assert len(review_pages(w)) == 3


async def test_a_clear_then_the_same_set_pages_again() -> None:
    dedupe = Dedupe()
    w = stonex_world(Store(), dedupe, symbols=("EURUSD",))
    await w.r.pass_once()
    w.now += 60_000
    w.store.rows = [reviewed("EURUSD")]                           # the operator accepted it: nothing to review
    new_term(w, dedupe)
    await w.r.pass_once()
    assert len(review_pages(w)) == 1
    w.now += 60_000
    w.store.rows = [MarketSessionRow.from_sessions(stonex_default("EURUSD"))]   # the reviewed row is gone again
    new_term(w, dedupe)
    await w.r.pass_once()
    assert len(review_pages(w)) == 2


async def test_an_unreadable_dedupe_store_still_pages_once_per_term() -> None:
    dedupe = Dedupe()
    w = stonex_world(Store(), dedupe)
    await w.r.pass_once()
    dedupe.fail_read = True
    new_term(w, dedupe)
    await w.r.pass_once()
    assert len(review_pages(w)) == 2                              # fail open: an info page twice beats none
    w.r.request_all()
    await w.r.pass_once()
    assert len(review_pages(w)) == 2                              # but once per term


async def test_without_a_dedupe_store_the_set_is_paged_once_per_term() -> None:
    w = stonex_world(Store(), None)
    await w.r.pass_once()
    w.r.request_all()
    await w.r.pass_once()
    assert len(review_pages(w)) == 1


async def test_an_unreadable_stored_calendar_skips_the_review_and_retries() -> None:
    class DownStore(Store):
        async def latest(self) -> tuple[int, list[MarketSessionRow]]:
            raise ConnectionError("postgres down")

    w = stonex_world(DownStore([reviewed("EURUSD")]), Dedupe())
    out = await w.r.pass_once()
    assert out.failed == ["stonex"] and out.fetched == [] and w.store.saves == []
    assert review_pages(w) == []                                  # never paged without the reviewed rows
    assert [p[1] for p in w.pages] == ["market sessions not saved"]


async def test_a_lost_fence_while_recording_the_review_propagates() -> None:
    class EpochMismatch(Exception):
        pass

    class FencedDedupe(Dedupe):
        async def record(self, key: ReviewKey, at_ms: int) -> None:
            raise EpochMismatch("fence")

    w = stonex_world(Store(), FencedDedupe())
    with pytest.raises(EpochMismatch):
        await w.r.pass_once()
