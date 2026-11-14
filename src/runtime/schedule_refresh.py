"""Broker market-session fetch -> `market_sessions` -> SM10 calendar (operations.md §10; IMPLEMENTATION_PLAN 5.1 /
5.5 / 5.6).

LEADER_INIT step 5 initialises SM10 from the latest stored calendar version (the adapters are not connected
yet). This supervised loop then keeps the stored schedule current from the brokers:

* An adapter is fetched once it is STREAMING in this term (connected, instruments known), then every
  `refresh_ms` (6 h, so a StoneX schedule written in UTC at the current DST offset never stays an hour wrong
  for long), after each 17:00 New York roll (`request_all()`), and when it needs a symbol the stored version
  does not cover yet. A failed fetch keeps the previous version and is retried after `retry_ms`.
* The symbols asked for are the adapter's subscribed / wanted tick symbols plus every symbol the stored
  version already has for it (a save replaces all of an adapter's rows).
* `MarketSessionRow.from_sessions` maps each value; a row that would not parse is dropped and paged (the rest
  are saved). An operator's `reviewed_config` row is kept where the broker returned nothing or only a flagged
  default for that symbol. An adapter that returned no rows at all is not saved (its stored rows stay), and a
  stored symbol the broker left out of an answer keeps its stored row (paged as missing for review).
* A new version (or a stored version the calendar is not on) re-initialises the calendar (`reload`).
* Review: requested symbols missing from the answer, `is_default`, `holidays_known=False` and a non-empty
  `note` are logged, and paged once per adapter per term when that set changes (info).

A lost fence (`EpochMismatch`) propagates (the GuardedEngineDb has already hard-exited); any other error is
logged and retried. Nothing here writes to a broker: `fetch_market_sessions` is a read, made at
RestPriority.POLL on the adapter's shared REST bucket (operations.md §15).
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Final

from src.adapters.base import BrokerAdapter
from src.adapters.ratelimit import RestPriority, rest_priority
from src.calendar.schedule import ScheduleError
from src.db.market_sessions import MarketSessionRow
from src.runtime.leader_init import is_fence_loss
from src.runtime.ports import MarketSessionStore

log = logging.getLogger(__name__)

SCHEDULE_REFRESH_MS: Final = 6 * 3_600_000
SCHEDULE_RETRY_MS: Final = 5 * 60_000
SCHEDULE_FETCH_TIMEOUT_S: Final = 30.0


@dataclass
class RefreshOutcome:
    fetched: list[str]
    failed: list[str]
    version: int | None = None
    changed: bool = False
    reloaded: bool = False


class ScheduleRefresher:
    def __init__(self, store: MarketSessionStore, *, adapter_ids: Callable[[], Sequence[str]],
                 adapter: Callable[[str], BrokerAdapter], ready: Callable[[str], bool],
                 wanted_symbols: Callable[[str], Sequence[str]], stored_symbols: Callable[[str], Sequence[str]],
                 reload: Callable[[], Awaitable[int]], calendar_version: Callable[[], int | None],
                 pager: Callable[[str, str, str], None], clock_ms: Callable[[], int],
                 refresh_ms: int = SCHEDULE_REFRESH_MS, retry_ms: int = SCHEDULE_RETRY_MS,
                 fetch_timeout_s: float = SCHEDULE_FETCH_TIMEOUT_S) -> None:
        self._store = store
        self._ids = adapter_ids
        self._adapter = adapter
        self._ready = ready
        self._wanted = wanted_symbols
        self._stored = stored_symbols
        self._reload = reload
        self._version = calendar_version
        self._page = pager
        self._clock = clock_ms
        self.refresh_ms = refresh_ms
        self.retry_ms = retry_ms
        self.fetch_timeout_s = fetch_timeout_s
        self._last_ok: dict[str, int] = {}
        self._next_try: dict[str, int] = {}
        self._fetched_symbols: dict[str, frozenset[str]] = {}
        self._forced: set[str] = set()
        self._review_paged: dict[str, tuple[str, ...]] = {}
        self.liveness_ms: int | None = None

    def request_all(self) -> None:
        """After a roll: every adapter is due again (the next pass fetches each STREAMING one)."""
        self._forced = set(self._ids())

    def _due(self, aid: str, now: int) -> bool:
        if not self._ready(aid) or now < self._next_try.get(aid, 0):
            return False
        last = self._last_ok.get(aid)
        if last is None or aid in self._forced or now - last >= self.refresh_ms:
            return True
        return bool(set(self._wanted(aid)) - self._fetched_symbols.get(aid, frozenset()))

    def _review(self, aid: str, requested: Sequence[str], rows: Sequence[MarketSessionRow]) -> None:
        got = {r.symbol for r in rows}
        notes: list[str] = []
        missing = sorted(set(requested) - got)
        if missing:
            notes.append(f"no sessions for {', '.join(missing[:20])}" + (" ..." if len(missing) > 20 else ""))
        defaults = sorted(r.symbol for r in rows if r.is_default)
        if defaults:
            notes.append(f"documented default hours (review) for {', '.join(defaults[:20])}")
        unknown = sorted(r.symbol for r in rows if not r.holidays_known)
        if unknown:
            notes.append(f"holidays unknown for {len(unknown)} symbol(s)")
        noted = sorted(f"{r.symbol}: {r.note}" for r in rows if r.note)
        for n in noted[:20]:
            notes.append(n)
        if not notes:
            self._review_paged.pop(aid, None)
            return
        log.warning("%s market sessions need review: %s", aid, "; ".join(notes))
        key = tuple(notes)
        if self._review_paged.get(aid) != key:
            self._review_paged[aid] = key
            self._page("info", f"market sessions need review: {aid}", "; ".join(notes)[:1800])

    async def _fetch(self, aid: str, symbols: Sequence[str]) -> list[MarketSessionRow]:
        adapter = self._adapter(aid)
        with rest_priority(RestPriority.POLL):      # a periodic read: the lowest REST class (operations.md §15)
            got = await asyncio.wait_for(adapter.fetch_market_sessions(list(symbols)), self.fetch_timeout_s)
        rows: list[MarketSessionRow] = []
        for ms in got.values():
            if ms.adapter_id != aid:
                log.error("%s: fetch_market_sessions returned a row of %s; ignored", aid, ms.adapter_id)
                continue
            row = MarketSessionRow.from_sessions(ms)
            try:
                row.validate()
            except ScheduleError as exc:
                self._page("high", f"market session unusable: {aid}:{ms.symbol}", str(exc)[:500])
                continue
            rows.append(row)
        return rows

    async def pass_once(self) -> RefreshOutcome:
        now = self._clock()
        out = RefreshOutcome([], [])
        fresh: list[MarketSessionRow] = []
        asked: dict[str, frozenset[str]] = {}
        for aid in self._ids():
            if not self._due(aid, now):
                continue
            symbols = sorted(set(self._wanted(aid)) | set(self._stored(aid)))
            if not symbols:
                continue                                  # nothing subscribed yet: try again next pass
            try:
                rows = await self._fetch(aid, symbols)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_fence_loss(exc):
                    raise
                out.failed.append(aid)
                self._next_try[aid] = now + self.retry_ms
                log.warning("%s: market sessions not fetched (keeping calendar v%s): %s", aid, self._version(),
                            type(exc).__name__)
                continue
            self._review(aid, symbols, rows)
            if not rows:
                out.failed.append(aid)
                self._next_try[aid] = now + self.retry_ms
                continue
            fresh.extend(rows)
            asked[aid] = frozenset(symbols)
            out.fetched.append(aid)
        if not out.fetched:
            return out
        try:
            _, latest = await self._store.latest()
            by_key = {r.key: r for r in fresh}
            for r in latest:
                if r.adapter_id not in asked:
                    continue
                cand = by_key.get(r.key)
                if r.source == "reviewed_config":
                    # keep an operator's reviewed row where the broker gave no real data
                    if cand is None or cand.is_default:
                        by_key[r.key] = r
                elif cand is None:
                    # the broker left this stored symbol out of this answer (paged by _review): keep its stored
                    # sessions, or the save would drop the symbol from the calendar (its state would then be
                    # unknown and read as OPEN: weekend stale-price pages, a wrong sod reference)
                    by_key[r.key] = r
            result = await self._store.save(sorted(by_key.values(), key=lambda r: r.key), adapters=out.fetched)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if is_fence_loss(exc):
                raise
            for aid in out.fetched:
                self._next_try[aid] = now + self.retry_ms
            self._page("high", "market sessions not saved", f"{type(exc).__name__}: {str(exc)[:300]}")
            out.failed.extend(out.fetched)
            out.fetched = []
            return out
        out.version, out.changed = result.version, result.changed
        for aid in out.fetched:
            self._last_ok[aid] = now
            self._fetched_symbols[aid] = asked[aid]
            self._forced.discard(aid)
            self._next_try.pop(aid, None)
        if result.changed or (result.version and self._version() != result.version):
            await self._reload()
            out.reloaded = True
            log.warning("market sessions: calendar version %d %s (%s)", result.version,
                        "saved and loaded" if result.changed else "loaded", ", ".join(out.fetched))
        return out

    async def run(self, stop: asyncio.Event, *, poll_s: float = 30.0, stamp: Callable[[], None] | None = None) -> None:
        while not stop.is_set():
            try:
                await self.pass_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_fence_loss(exc):
                    raise
                log.warning("market session refresh pass failed: %r", exc)
            self.liveness_ms = self._clock()
            if stamp is not None:
                stamp()
            try:
                await asyncio.wait_for(stop.wait(), poll_s)
            except TimeoutError:
                pass


__all__ = ["RefreshOutcome", "SCHEDULE_FETCH_TIMEOUT_S", "SCHEDULE_REFRESH_MS", "SCHEDULE_RETRY_MS",
           "ScheduleRefresher"]
