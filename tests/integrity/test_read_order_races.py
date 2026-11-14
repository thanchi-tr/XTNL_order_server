"""Redis-first read order (tier-persistence.md §2-§3): a live commit landing between the two reads must
never be classified as 'Redis ahead' (heal-all-reads-pg-before-redis, sm8-restore-halt-race)."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from src.cache.scripts import NoBootQueue, RedisState
from src.integrity.bootstrap import RedisBootstrap
from src.integrity.heal import HealOutcome, Healer
from src.integrity.ports import CommittedRow, HaltRow
from src.integrity.redis_ops import BootIdentity, read_meta
from tests.integrity.conftest import boot
from tests.integrity.fakes import T0, FakeIntegrityDb, ManualClock, Pages, order


class CommitDuringPageRead(FakeIntegrityDb):
    """The first rows_after_key returns the v1 page, then (before the caller's Redis read) a live consumer
    commits v2 to PostgreSQL and applies it to Redis, as the integrity worker never pauses consumers."""

    def __init__(self, cache: RedisState) -> None:
        super().__init__()
        self._cache = cache
        self.fired = False

    async def rows_after_key(self, after_key: str, limit: int) -> Sequence[CommittedRow]:
        page = await super().rows_after_key(after_key, limit)
        if not self.fired and page:
            self.fired = True
            v2 = self.put(order("K", observed=200, seq=2000), T0 + 1)
            await self._cache.apply(v2.write())
        return page


async def test_heal_all_does_not_roll_back_a_concurrent_commit(redis: Any, cache: RedisState,
                                                               identity: BootIdentity, clock: ManualClock) -> None:
    await boot(redis, identity)
    db = CommitDuringPageRead(cache)
    v1 = db.put(order("K", observed=100, seq=1000))
    await cache.apply(v1.write())
    pages = Pages()
    report = await Healer(redis, cache, db, clock_ms=clock, pager=pages).heal_all()
    meta = (await read_meta(redis, [v1.key]))[v1.key]
    assert meta is not None and meta.version == db.rows[v1.key].version      # still v2, not rolled back
    assert meta.version != v1.version
    assert report.outcomes[HealOutcome.REPLACED] == 0
    assert "redis_ahead" not in pages.codes()


class TripDuringHaltRead(FakeIntegrityDb):
    """The first latest_halt_event answers id N, then the RiskSweep trips: halt_events N+1 commits and its
    CAS writes halt:state = (HARD, N+1) before the bootstrap's next read."""

    def __init__(self, cache: RedisState) -> None:
        super().__init__()
        self._cache = cache
        self.fired = False

    async def latest_halt_event(self) -> HaltRow | None:
        row = await super().latest_halt_event()
        if not self.fired:
            self.fired = True
            self.halt(6, "HARD")
            await self._cache.set_halt_state("HARD", 6)
        return row


async def test_sm8_restore_halt_keeps_a_concurrent_trip(redis: Any, cache: RedisState, identity: BootIdentity,
                                                        no_boot: NoBootQueue, clock: ManualClock) -> None:
    db = TripDuringHaltRead(cache)
    db.halt(5, "NONE")
    sm8 = RedisBootstrap(redis, cache, db, identity, no_boot, clock_ms=clock, pager=Pages(),
                         step_retry_s=0.0, lock_wait_s=0.0, price_poll_s=0.0)
    assert await sm8.run_once(in_place=True)
    assert await cache.read_halt_state() == ("HARD", 6)


async def test_sm8_restore_halt_foreign_ahead_still_replaced_and_rechecked(
        redis: Any, cache: RedisState, identity: BootIdentity, no_boot: NoBootQueue, clock: ManualClock) -> None:
    """Genuinely foreign data ahead is still DEL + re-applied; a trip landing in that window is re-applied."""
    db = TripDuringHaltRead(cache)
    db.halt(5, "NONE")
    db.fired = True                       # no trip during the first read
    await cache.set_halt_state("SOFT", 9)  # foreign / restored Redis, ahead of PostgreSQL
    sm8 = RedisBootstrap(redis, cache, db, identity, no_boot, clock_ms=clock, pager=Pages(),
                         step_retry_s=0.0, lock_wait_s=0.0, price_poll_s=0.0)
    assert await sm8.run_once(in_place=True)
    assert await cache.read_halt_state() == ("NONE", 5)
