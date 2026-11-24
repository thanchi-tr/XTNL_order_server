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
  `note` are logged, and paged (info) when that set changes. A symbol whose stored row is an operator's
  `reviewed_config` row that this pass keeps (the broker gave nothing or only a flagged default for it) is left out
  of the default / holidays / note entries: the operator accepted that schedule (`python -m src.ops
  market-sessions accept`), so StoneX's standing facts (no trading hours, no holiday source) stop paging. A missing
  symbol is still reported. The last paged set is kept per adapter in `notification_dedupe` (two episode keys:
  the adapter's last page-or-clear time and the time each set was paged), so an unchanged set is paged once per
  change, not once per term or engine start; a set seen again after a different set or after a clear is paged
  again. Without that store (or when it cannot be read) the set is paged once per term, as before.

A lost fence (`EpochMismatch`) propagates (the GuardedEngineDb has already hard-exited); any other error is
logged and retried. Nothing here writes to a broker: `fetch_market_sessions` is a read, made at
RestPriority.POLL on the adapter's shared REST bucket (operations.md §15).
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Protocol

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
REVIEW_DEDUPE_TIMEOUT_S: Final = 5.0
#: notification_dedupe episode keys of the review page: (rule, "adapter", adapter_id, 0, level). Level 0 is the
#: adapter's last page-or-clear time, any other level the time one review set (its digest) was paged. Not a rule
#: id, so no Oversee episode can collide (like `deadman_ack`).
REVIEW_EPISODE_RULE: Final = "market_sessions_review"

ReviewKey = tuple[str, str, str, int, int]


class ReviewDedupe(Protocol):
    """The two `DedupeStore` calls the review page uses (src/db/notify_store.py PgDedupeStore)."""

    async def last_sent_of(self, keys: Sequence[ReviewKey]) -> Mapping[ReviewKey, int]: ...

    async def record(self, key: ReviewKey, at_ms: int) -> None: ...


def review_episode_keys(adapter_id: str, notes: Sequence[str]) -> tuple[ReviewKey, ReviewKey]:
    """(adapter's last page-or-clear key, this review set's key). The set's level is a 31-bit digest, never 0."""
    digest = hashlib.sha256("\n".join(notes).encode("utf-8")).digest()
    level = (int.from_bytes(digest[:4], "big") & 0x7FFFFFFF) or 1
    return ((REVIEW_EPISODE_RULE, "adapter", adapter_id, 0, 0),
            (REVIEW_EPISODE_RULE, "adapter", adapter_id, 0, level))


def _keeps_reviewed(answer: MarketSessionRow | None) -> bool:
    """An operator's stored `reviewed_config` row is kept over the broker's answer for its symbol when the broker
    gave nothing for it or only a flagged default (the merge in `pass_once`, and so the review)."""
    return answer is None or answer.is_default


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
                 fetch_timeout_s: float = SCHEDULE_FETCH_TIMEOUT_S,
                 review_dedupe: ReviewDedupe | None = None) -> None:
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
        #: per adapter: the review set paged (or found paged) in this term; () once a pass found nothing to review
        self._review_paged: dict[str, tuple[str, ...]] = {}
        self._review_dedupe = review_dedupe
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

    async def _review(self, aid: str, requested: Sequence[str], rows: Sequence[MarketSessionRow],
                      reviewed: frozenset[str] = frozenset()) -> None:
        """Log and page the review findings of one answer. `reviewed`: the symbols whose stored `reviewed_config`
        row this pass keeps (the operator accepted that schedule): no default / holidays / note entry for them."""
        got = {r.symbol for r in rows}
        notes: list[str] = []
        missing = sorted(set(requested) - got)
        if missing:
            notes.append(f"no sessions for {', '.join(missing[:20])}" + (" ..." if len(missing) > 20 else ""))
        flagged = [r for r in rows if r.symbol not in reviewed]
        defaults = sorted(r.symbol for r in flagged if r.is_default)
        if defaults:
            notes.append(f"documented default hours (review) for {', '.join(defaults[:20])}")
        unknown = sorted(r.symbol for r in flagged if not r.holidays_known)
        if unknown:
            notes.append(f"holidays unknown for {len(unknown)} symbol(s)")
        noted = sorted(f"{r.symbol}: {r.note}" for r in flagged if r.note)
        for n in noted[:20]:
            notes.append(n)
        if not notes:
            if self._review_paged.get(aid) != ():
                self._review_paged[aid] = ()
                # a clear: a later set is paged again even when it equals one paged before the clear
                await self._record_review(review_episode_keys(aid, ())[0], self._clock())
            return
        log.warning("%s market sessions need review: %s", aid, "; ".join(notes))
        key = tuple(notes)
        if self._review_paged.get(aid) == key:
            return
        latest_key, set_key = review_episode_keys(aid, key)
        self._review_paged[aid] = key
        if await self._review_already_paged(latest_key, set_key):
            log.info("%s market sessions review: this set was paged before and has not changed (not paged again)",
                     aid)
            return
        self._page("info", f"market sessions need review: {aid}", "; ".join(notes)[:1800])
        at = self._clock()
        if await self._record_review(set_key, at):
            await self._record_review(latest_key, at)

    async def _review_already_paged(self, latest_key: ReviewKey, set_key: ReviewKey) -> bool:
        """True when `set_key` is the set its adapter paged last (no other set and no clear since). A store that
        cannot be read answers False (an info page twice beats none). A lost fence propagates."""
        if self._review_dedupe is None:
            return False
        try:
            got = await asyncio.wait_for(self._review_dedupe.last_sent_of([latest_key, set_key]),
                                         REVIEW_DEDUPE_TIMEOUT_S)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if is_fence_loss(exc):
                raise
            log.warning("market sessions review dedupe not read: %s", type(exc).__name__)
            return False
        paged_at = got.get(set_key)
        last = got.get(latest_key)
        return paged_at is not None and (last is None or paged_at >= last)

    async def _record_review(self, key: ReviewKey, at_ms: int) -> bool:
        """True when recorded (or there is no store). A lost fence propagates; any other error only logs."""
        if self._review_dedupe is None:
            return True
        try:
            await asyncio.wait_for(self._review_dedupe.record(key, at_ms), REVIEW_DEDUPE_TIMEOUT_S)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if is_fence_loss(exc):
                raise
            log.warning("market sessions review dedupe not recorded: %s", type(exc).__name__)
            return False
        return True

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
        answers: list[tuple[str, list[str], list[MarketSessionRow]]] = []
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
            answers.append((aid, symbols, rows))
        if not answers:
            return out
        try:
            # one read of the stored version: the review (which symbols an operator accepted) and the merge
            _, latest = await self._store.latest()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if is_fence_loss(exc):
                raise
            for aid, _symbols, rows in answers:
                out.failed.append(aid)
                self._next_try[aid] = now + self.retry_ms
            log.warning("market sessions: stored calendar not read (review and save retried): %s",
                        type(exc).__name__)
            if any(rows for _aid, _symbols, rows in answers):
                self._page("high", "market sessions not saved", f"{type(exc).__name__}: {str(exc)[:300]}")
            return out
        for aid, symbols, rows in answers:
            answer = {r.symbol: r for r in rows}
            reviewed = frozenset(r.symbol for r in latest
                                 if r.adapter_id == aid and r.source == "reviewed_config"
                                 and _keeps_reviewed(answer.get(r.symbol)))
            await self._review(aid, symbols, rows, reviewed)
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
            by_key = {r.key: r for r in fresh}
            for r in latest:
                if r.adapter_id not in asked:
                    continue
                cand = by_key.get(r.key)
                if r.source == "reviewed_config":
                    # keep an operator's reviewed row where the broker gave no real data
                    if _keeps_reviewed(cand):
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


__all__ = ["REVIEW_EPISODE_RULE", "RefreshOutcome", "ReviewDedupe", "SCHEDULE_FETCH_TIMEOUT_S",
           "SCHEDULE_REFRESH_MS", "SCHEDULE_RETRY_MS", "ScheduleRefresher", "review_episode_keys"]
