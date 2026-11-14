"""Heal Redis entity keys from PostgreSQL's current rows (tier-persistence.md §2 step 4, §3 step 1).

Redis behind  -> re-apply through apply_state.lua (version CAS; the XADD wakes the RiskSweep).
Redis ahead   -> page, DEL (digest-preserving), re-apply: a version CAS can never correct a key that is ahead.
Redis orphan  -> page, DEL (a key PostgreSQL does not have).
Every heal reads Redis FIRST and PostgreSQL SECOND: Redis is written only after the PostgreSQL commit, so a
key that still looks ahead after that order really is ahead and never a copy in flight.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Final

import redis.asyncio as aioredis

from src.cache.scripts import ApplyResult, RedisState
from src.integrity.ports import CommittedRow, IntegrityAlert, IntegrityDb, Pager, null_pager
from src.integrity.redis_ops import Drift, RedisMeta, classify, drop_entity_key, read_meta, scan_entity_keys

log = logging.getLogger(__name__)

APPLY_CHUNK: Final = 64


class NoBootError(RuntimeError):
    """apply_state.lua answered NO_BOOT: redis:boot_id is missing / foreign and the lock is not held."""


class HealOutcome(str, Enum):
    MATCH = "MATCH"
    HEALED = "HEALED"          # behind -> CAS re-apply
    REPLACED = "REPLACED"      # ahead -> DEL + re-apply
    DROPPED = "DROPPED"        # orphan -> DEL
    RACED = "RACED"            # a newer copy landed meanwhile (STALE): nothing to do


@dataclass
class HealReport:
    rows: int = 0
    outcomes: dict[HealOutcome, int] = field(default_factory=lambda: {o: 0 for o in HealOutcome})

    def add(self, outcome: HealOutcome) -> None:
        self.outcomes[outcome] += 1

    @property
    def changed(self) -> int:
        o = self.outcomes
        return o[HealOutcome.HEALED] + o[HealOutcome.REPLACED] + o[HealOutcome.DROPPED]


class Healer:
    def __init__(self, redis: aioredis.Redis, cache: RedisState, db: IntegrityDb, *,
                 clock_ms: Callable[[], int], pager: Pager = null_pager) -> None:
        self._redis = redis
        self._cache = cache
        self._db = db
        self._clock_ms = clock_ms
        self._pager = pager

    async def _apply(self, row: CommittedRow, lock_token: str | None) -> HealOutcome:
        res = await self._cache.apply(row.write(), lock_token=lock_token)
        if res is ApplyResult.NO_BOOT:
            raise NoBootError(row.key)
        if res is ApplyResult.STALE:
            return HealOutcome.RACED
        return HealOutcome.HEALED

    async def act(self, key: str, pg: CommittedRow | None, meta: RedisMeta | None, *,
                  lock_token: str | None = None, page_each: bool = True) -> HealOutcome:
        """Apply the verdict for one key. Raises NoBootError (the caller signals SM8)."""
        drift = classify(pg, meta)
        if drift is Drift.MATCH:
            return HealOutcome.MATCH
        if drift is Drift.ORPHAN:
            await drop_entity_key(self._redis, key)
            if page_each:
                self._page("redis_orphan", f"{key} is in Redis but not in PostgreSQL: deleted")
            return HealOutcome.DROPPED
        assert pg is not None
        if drift is Drift.AHEAD:
            if page_each:
                self._page("redis_ahead", f"{key} Redis v={meta.version if meta else '-'} ahead of PostgreSQL "
                                          f"v={pg.version}: deleted and re-applied")
            await drop_entity_key(self._redis, key)
            out = await self._apply(pg, lock_token)
            return HealOutcome.REPLACED if out is HealOutcome.HEALED else out
        return await self._apply(pg, lock_token)

    async def heal_keys(self, keys: Collection[str], *, lock_token: str | None = None) -> dict[str, HealOutcome]:
        """Confirm-then-heal: read Redis, then PostgreSQL, then act on whatever still differs."""
        ordered = sorted(keys)
        metas = await read_meta(self._redis, ordered)
        rows = await self._db.rows_by_keys(ordered)
        out: dict[str, HealOutcome] = {}
        for k in ordered:
            out[k] = await self.act(k, rows.get(k), metas.get(k), lock_token=lock_token)
        return out

    async def heal_all(self, *, lock_token: str | None = None, page_size: int = 500) -> HealReport:
        """Re-apply every PostgreSQL current row (SM8 step 4, SM3 full reconcile), then delete orphans.

        The PostgreSQL page only drives the key order; each page is then classified Redis FIRST and
        PostgreSQL SECOND (`_apply_page`), so a live commit landing between the page read and the Redis
        read is never mistaken for a key that is ahead (heal-all-reads-pg-before-redis)."""
        report = HealReport()
        after = ""
        while True:
            rows = await self._db.rows_after_key(after, page_size)
            if not rows:
                break
            await self._apply_page(rows, report, lock_token)
            after = rows[-1].key
            if len(rows) < page_size:
                break
        await self._drop_orphans(report)
        o = report.outcomes
        if o[HealOutcome.REPLACED] or o[HealOutcome.DROPPED]:
            self._page("redis_ahead", f"full heal: {o[HealOutcome.REPLACED]} keys ahead re-applied, "
                                      f"{o[HealOutcome.DROPPED]} orphan keys deleted")
        return report

    async def _apply_page(self, rows: Sequence[CommittedRow], report: HealReport, lock_token: str | None) -> None:
        keys = [r.key for r in rows]
        metas = await read_meta(self._redis, keys)       # Redis first ...
        fresh = await self._db.rows_by_keys(keys)        # ... then a fresh PostgreSQL read of the same keys
        for i in range(0, len(keys), APPLY_CHUNK):
            chunk = keys[i:i + APPLY_CHUNK]
            outs = await asyncio.gather(*(self.act(k, fresh.get(k), metas.get(k), lock_token=lock_token,
                                                   page_each=False) for k in chunk))
            for o in outs:
                report.add(o)
            report.rows += len(chunk)

    async def _drop_orphans(self, report: HealReport) -> None:
        async for batch in scan_entity_keys(self._redis):
            present = await self._db.rows_by_keys(batch)
            for k in batch:
                if k not in present:
                    await drop_entity_key(self._redis, k)
                    report.add(HealOutcome.DROPPED)

    def _page(self, code: str, detail: str) -> None:
        log.warning("integrity %s: %s", code, detail)
        self._pager(IntegrityAlert("high", code, detail, self._clock_ms()))
