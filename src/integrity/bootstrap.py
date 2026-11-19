"""SM8 RedisBootstrap: rebuild Redis from PostgreSQL after data loss, fail-closed (tier-persistence.md §2).

Single-flight under `SET redis:boot_lock <owner> NX PX <ttl>` (renewed while running); every other signal
just waits. Steps, in order, each retried (the gate stays closed throughout):
  1 RESTORE_HALT    halt:state from the latest halt_events row through the CAS (no new row, no alert);
  2 CREATE_GROUP    XGROUP CREATE oversee:feed <group> 0 MKSTREAM (BUSYGROUP = success);
  3 PRICE_BARRIER   seed price:/clean_mid: from last_clean_marks and account_live: from the latest sampled
                    account_snapshots row, only where absent or older (light.lua compares ts); then wait,
                    bounded, for conn:{adapter}:prices;
  4 HEAL            re-apply every PostgreSQL entity through apply_state.lua as lock holder;
  5 REPLAY_NO_BOOT  re-apply the queued NO_BOOT writes as lock holder;
  6 READY           write redis:boot_id LAST and release the lock (one script); for an in-place run, request
                    fetch_account_snapshot() from each STREAMING adapter.
Retries are bounded by the LEADER_INIT deadline (operations.md §4, 120 s) when one is given; in place, a
step failing for 60 s pages (and keeps retrying).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Collection
from typing import Final, TypeVar

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from src.cache import keys as K
from src.cache.scripts import ApplyResult, RedisState, read_boot_id
from src.integrity.heal import Healer, NoBootError
from src.integrity.ports import BootState, IntegrityAlert, IntegrityDb, NoBootReplay, Pager, null_pager
from src.integrity.redis_ops import BootIdentity, finish_boot, release_lock, renew_lock, try_lock
from src.oversee.feed import FEED_GROUP

log = logging.getLogger(__name__)
T = TypeVar("T")

LEADER_INIT_DEADLINE_MS: Final = 120_000
IN_PLACE_PAGE_AFTER_MS: Final = 60_000


class BootstrapDeadline(RuntimeError):
    """A step could not complete before the LEADER_INIT deadline (LEADER_INIT then hard-exits)."""


class LockLost(RuntimeError):
    """redis:boot_lock expired or was taken over while SM8 was running."""


class RedisBootstrap:
    """SM8. `run_once()` performs one bootstrap; `run()` serves signals (NO_BOOT, boot_id mismatch, FLUSHDB)."""

    def __init__(
        self,
        redis: aioredis.Redis,
        cache: RedisState,
        db: IntegrityDb,
        identity: BootIdentity,
        no_boot: NoBootReplay,
        *,
        clock_ms: Callable[[], int],
        price_adapters: Callable[[], Collection[str]] = lambda: (),
        request_account_snapshots: Callable[[], Awaitable[None]] | None = None,
        pager: Pager = null_pager,
        feed_group: str = FEED_GROUP,
        owner: str | None = None,
        lock_ttl_ms: int = 15_000,
        price_wait_ms: int = 10_000,
        price_poll_s: float = 0.2,
        step_retry_s: float = 0.5,
        lock_wait_s: float = 0.5,
    ) -> None:
        self._redis = redis
        self._cache = cache
        self._db = db
        self.identity = identity
        self._no_boot = no_boot
        self._clock_ms = clock_ms
        self._price_adapters = price_adapters
        self._request_snapshots = request_account_snapshots
        self._pager = pager
        self._group = feed_group
        self.owner = owner or f"sm8:{uuid.uuid4().hex}"
        self._ttl_ms = lock_ttl_ms
        self._price_wait_ms = price_wait_ms
        self._price_poll_s = price_poll_s
        self._retry_s = step_retry_s
        self._lock_wait_s = lock_wait_s
        self._healer = Healer(redis, cache, db, clock_ms=clock_ms, pager=pager)
        self._wake = asyncio.Event()
        self._ready_event = asyncio.Event()
        self._lock_lost = False
        self.state = BootState.NOT_BOOTED
        self.history: deque[tuple[BootState, BootState, str]] = deque(maxlen=500)  # bounded for 24/7 runs
        self.signals: deque[str] = deque(maxlen=500)   # bounded: NO_BOOT enqueues signal too (K3)
        self.runs = 0
        self.last_ready_ms: int | None = None

    # ---- public --------------------------------------------------------------------------------
    def signal(self, reason: str) -> None:
        """NO_BOOT, a boot_id mismatch or a reconcile FLUSHDB: (re)start SM8. Never blocks."""
        self.signals.append(reason)
        self._wake.set()

    @property
    def ready(self) -> bool:
        return self.state is BootState.READY

    async def wait_ready(self, timeout_s: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout_s)
        except TimeoutError:
            return False
        return True

    async def is_booted(self) -> bool:
        """redis:boot_id = run_id:epoch right now (refreshes INFO run_id)."""
        expected = await self.identity.refresh()
        return await read_boot_id(self._redis) == expected

    async def run(self, stop: asyncio.Event) -> None:
        """Serve signals until `stop`. An already-booted Redis only gets its NO_BOOT queue replayed."""
        while not stop.is_set():
            self._wake.clear()
            try:
                if await self.is_booted():
                    self._set(BootState.READY, "boot_id current")
                    self._ready_event.set()
                    await self._replay_no_boot(lock_token=None)
                else:
                    await self.run_once(in_place=True)
            except Exception:
                log.exception("SM8 run failed; waiting for the next signal")
            if self._wake.is_set() and not stop.is_set():
                continue
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), self._lock_wait_s * 10)

    async def run_once(self, *, in_place: bool, deadline_ms: int | None = None) -> bool:
        """One bootstrap. False if another owner holds the lock (this signal just waits).
        Raises BootstrapDeadline when `deadline_ms` passes (LEADER_INIT)."""
        self._mark_not_ready("bootstrap requested")
        expected = await self._retry("refresh run_id", self.identity.refresh, deadline_ms, in_place)
        if not await self._acquire(deadline_ms):
            return False
        self.runs += 1
        self._lock_lost = False
        self._set(BootState.DETECTED, "boot_lock acquired")
        renewer = asyncio.create_task(self._renew_loop())
        try:
            self._set(BootState.RESTORE_HALT, "step 1")
            await self._retry("restore halt:state", self._restore_halt, deadline_ms, in_place)
            self._set(BootState.CREATE_GROUP, "step 2")
            await self._retry("create feed group", self._create_group, deadline_ms, in_place)
            self._set(BootState.PRICE_BARRIER, "step 3")
            await self._retry("seed live keys", self._seed_live, deadline_ms, in_place)
            await self._wait_prices(deadline_ms)
            self._set(BootState.HEAL, "step 4")
            await self._retry("heal entity keys", self._heal, deadline_ms, in_place)
            self._set(BootState.REPLAY_NO_BOOT, "step 5")
            await self._retry("replay NO_BOOT", self._replay_locked, deadline_ms, in_place)
            await self._retry("write boot_id", lambda: self._finish(expected), deadline_ms, in_place)
        except BaseException:
            renewer.cancel()
            with contextlib.suppress(Exception):
                await release_lock(self._redis, self.owner)
            self._set(BootState.NOT_BOOTED, "aborted")
            raise
        renewer.cancel()
        self.last_ready_ms = self._clock_ms()
        self._set(BootState.READY, "boot_id written")
        self._ready_event.set()
        # Writes refused while boot_id was being written are replayed now (boot_id exists: no lock needed).
        with contextlib.suppress(Exception):
            await self._replay_no_boot(lock_token=None)
        if in_place and self._request_snapshots is not None:
            try:
                await self._request_snapshots()
            except Exception:
                log.exception("SM8: post-READY account snapshot request failed")
        return True

    # ---- steps ---------------------------------------------------------------------------------
    async def _restore_halt(self) -> None:
        """Redis FIRST, PostgreSQL SECOND (as IntegrityWorker._check_halt): halt:state is CAS-written only
        after its halt_events row commits, so a value read before the PostgreSQL read can never be ahead
        of it unless it really is foreign. Reading PostgreSQL first would let a RiskSweep trip landing
        between the reads (the CAS has no boot check) look 'ahead' and be replaced by the older row."""
        current = await self._cache.read_halt_state()
        latest = await self._db.latest_halt_event()
        if latest is None:
            return
        if current is not None and (current[1] > latest.id or
                                    (current[1] == latest.id and current[0] != latest.effective_level)):
            # Foreign / restored data ahead of PostgreSQL: DEL then re-apply (never a CAS fix).
            await self._cache.delete_halt_state()
            await self._cache.set_halt_state(latest.effective_level, latest.id)
            # A trip that committed and CAS-ed in the DEL window was just removed: re-read and re-apply
            # the newest row (the CAS only ever moves halt:state forward).
            again = await self._db.latest_halt_event()
            if again is not None and again.id > latest.id:
                await self._cache.set_halt_state(again.effective_level, again.id)
            return
        await self._cache.set_halt_state(latest.effective_level, latest.id)

    async def _create_group(self) -> None:
        try:
            await self._redis.xgroup_create(K.FEED, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _seed_live(self) -> None:
        for m in await self._db.last_clean_marks():
            await self._cache.seed_clean_mark(m.adapter_id, m.symbol, m.bid, m.ask, m.ts, m.spread_pips)
        for live in await self._db.latest_live_samples():
            await self._cache.write_account_live(live, seed=True)

    async def _wait_prices(self, deadline_ms: int | None) -> None:
        """Bounded wait for conn:{adapter}:prices; on timeout continue (stale prices are capped by the sweep)."""
        adapters = list(self._price_adapters())
        if not adapters:
            return
        limit = self._clock_ms() + self._price_wait_ms
        if deadline_ms is not None:
            limit = min(limit, deadline_ms)
        while True:
            missing = [a for a in adapters if not await self._redis.exists(K.conn_key(a, "prices"))]
            if not missing:
                return
            if self._clock_ms() >= limit:
                log.warning("SM8: prices not live for %s; continuing", missing)
                return
            await asyncio.sleep(self._price_poll_s)

    async def _heal(self) -> None:
        self._check_lock()
        try:
            report = await self._healer.heal_all(lock_token=self.owner)
        except NoBootError as exc:
            self._lock_lost = True
            raise LockLost(f"apply_state refused the lock holder at {exc}") from exc
        log.info("SM8 heal: %d rows, %s", report.rows, {k.value: v for k, v in report.outcomes.items() if v})

    async def _replay_locked(self) -> None:
        self._check_lock()
        await self._replay_no_boot(lock_token=self.owner)

    async def _replay_no_boot(self, lock_token: str | None) -> int:
        done = 0
        for w in self._no_boot.snapshot():
            res = await self._cache.apply(w, lock_token=lock_token)
            if res is ApplyResult.NO_BOOT:
                if lock_token is not None:
                    self._lock_lost = True
                    raise LockLost("boot lock not held during NO_BOOT replay")
                return done                      # boot_id gone again: a new SM8 run will replay it
            self._no_boot.discard(w)
            done += 1
        return done

    async def _finish(self, boot_id: str) -> None:
        if not await finish_boot(self._redis, self.owner, boot_id):
            self._lock_lost = True
            raise LockLost("boot lock lost before boot_id was written")

    # ---- lock & retry machinery --------------------------------------------------------------------
    async def _acquire(self, deadline_ms: int | None) -> bool:
        while True:
            if await try_lock(self._redis, self.owner, self._ttl_ms):
                return True
            holder = await self._redis.get(K.BOOT_LOCK)
            if deadline_ms is None:
                log.info("SM8: boot_lock held by %r; waiting", holder)
                return False
            if self._clock_ms() >= deadline_ms:
                raise BootstrapDeadline("boot_lock not acquired before the LEADER_INIT deadline")
            await asyncio.sleep(self._lock_wait_s)

    async def _renew_loop(self) -> None:
        interval = max(self._ttl_ms / 3000.0, 0.01)
        while True:
            await asyncio.sleep(interval)
            try:
                if not await renew_lock(self._redis, self.owner, self._ttl_ms):
                    self._lock_lost = True
                    log.error("SM8: boot_lock lost")
                    return
            except Exception:
                log.warning("SM8: boot_lock renew failed", exc_info=True)

    def _check_lock(self) -> None:
        if self._lock_lost:
            raise LockLost("boot lock lost")

    async def _retry(self, name: str, step: Callable[[], Awaitable[T]], deadline_ms: int | None,
                     in_place: bool) -> T:
        started = self._clock_ms()
        paged = False
        while True:
            try:
                return await step()
            except LockLost:
                raise
            except Exception as exc:
                now = self._clock_ms()
                log.warning("SM8 step %r failed: %s", name, exc)
                if deadline_ms is not None and now >= deadline_ms:
                    raise BootstrapDeadline(f"SM8 step {name!r} still failing at the deadline") from exc
                if in_place and not paged and now - started >= IN_PLACE_PAGE_AFTER_MS:
                    paged = True
                    self._pager(IntegrityAlert("critical", "sm8_step_failing",
                                               f"SM8 step {name!r} failing for {now - started} ms: {exc}", now))
            await asyncio.sleep(self._retry_s)

    def _mark_not_ready(self, why: str) -> None:
        self._ready_event.clear()
        if self.state is BootState.READY:
            self._set(BootState.NOT_BOOTED, why)

    def _set(self, new: BootState, why: str) -> None:
        if new is self.state:
            return
        self.history.append((self.state, new, why))
        log.info("SM8 %s -> %s (%s)", self.state.value, new.value, why)
        self.state = new
