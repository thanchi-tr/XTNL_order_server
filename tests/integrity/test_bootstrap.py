"""SM8 RedisBootstrap (tier-persistence.md §2): step order, single flight, seeding, heal, NO_BOOT replay,
boot_id last, deadlines and paging."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import pytest

from src.cache import keys as K
from src.cache.scripts import ApplyResult, NoBootQueue, RedisState, read_boot_id
from src.dto import AccountLive, PriceDTO
from src.integrity.bootstrap import BootstrapDeadline, LockLost, RedisBootstrap
from src.integrity.ports import BootState, CleanMark, CommittedRow, committed_row
from src.integrity.redis_ops import BootIdentity, read_meta, try_lock
from tests.integrity.conftest import EPOCH, RunId, boot
from tests.integrity.fakes import T0, FakeIntegrityDb, ManualClock, Pages, account, order, position


class TickingClock(ManualClock):
    """Advances by `step` ms on every read (drives deadline / bounded-wait loops)."""

    def __init__(self, step: int = 1_000) -> None:
        super().__init__()
        self.step = step

    def __call__(self) -> int:
        self.ms += self.step
        return self.ms


def make(redis: Any, cache: RedisState, db: FakeIntegrityDb, identity: BootIdentity, no_boot: NoBootQueue,
         clock: ManualClock, pages: Pages | None = None, **kw: Any) -> RedisBootstrap:
    return RedisBootstrap(redis, cache, db, identity, no_boot, clock_ms=clock, pager=pages or Pages(),
                          step_retry_s=0.0, lock_wait_s=0.0, price_poll_s=0.0, **kw)


def seed_db(db: FakeIntegrityDb) -> list[CommittedRow]:
    db.halt(3, "SOFT")
    db.halt(5, "HARD")
    db.marks.append(CleanMark("stonex", "EURUSD", Decimal("1.1000"), Decimal("1.1002"), T0 - 60_000, Decimal("2")))
    db.lives.append(AccountLive(account_id="ACC", adapter_id="stonex", currency="USD", equity=Decimal("100"),
                                margin_used=Decimal("1"), free_margin=Decimal("99"), unrealised_pnl=Decimal("0"),
                                timestamp_ms=T0 - 30_000, sampled=True))
    return [db.put(order("A")), db.put(position("P1")), db.put(account())]


async def test_full_bootstrap_in_order(redis: Any, cache: RedisState, db: FakeIntegrityDb, identity: BootIdentity,
                                       no_boot: NoBootQueue, clock: ManualClock) -> None:
    rows = seed_db(db)
    snapshots: list[int] = []

    async def request() -> None:
        snapshots.append(1)

    sm8 = make(redis, cache, db, identity, no_boot, clock, request_account_snapshots=request)
    assert await sm8.run_once(in_place=True)
    assert sm8.state is BootState.READY and sm8.ready
    assert [h[1] for h in sm8.history] == [BootState.DETECTED, BootState.RESTORE_HALT, BootState.CREATE_GROUP,
                                           BootState.PRICE_BARRIER, BootState.HEAL, BootState.REPLAY_NO_BOOT,
                                           BootState.READY]
    assert await read_boot_id(redis) == f"run-1:{EPOCH}"
    assert await redis.get(K.BOOT_LOCK) is None
    assert await cache.read_halt_state() == ("HARD", 5)
    groups = await redis.xinfo_groups(K.FEED)
    assert [g["name"] for g in groups] == [b"oversee"]
    assert await redis.hget(K.price_key("stonex", "EURUSD"), "seeded") == b"1"
    assert await redis.hget(K.clean_mid_key("stonex", "EURUSD"), "mid") == b"1.1001"
    assert await redis.hget(K.account_live_key("ACC"), "sampled") == b"1"
    metas = await read_meta(redis, [r.key for r in rows])
    for r in rows:
        m = metas[r.key]
        assert m is not None and m.version == r.version
    assert await redis.xlen(K.FEED) == 3      # heals XADD, waking the RiskSweep
    assert snapshots == [1]
    assert await sm8.wait_ready(0.01)


async def test_leader_init_run_requests_no_snapshot(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                    identity: BootIdentity, no_boot: NoBootQueue,
                                                    clock: ManualClock) -> None:
    calls: list[int] = []

    async def request() -> None:
        calls.append(1)

    sm8 = make(redis, cache, db, identity, no_boot, clock, request_account_snapshots=request)
    assert await sm8.run_once(in_place=False, deadline_ms=clock() + 120_000)
    assert calls == []          # SNAPSHOTTING follows a leader change anyway


async def test_boot_id_is_written_last(redis: Any, cache: RedisState, identity: BootIdentity, no_boot: NoBootQueue,
                                       clock: ManualClock) -> None:
    seen: dict[str, Any] = {}

    class Probe(FakeIntegrityDb):
        async def rows_after_key(self, after_key: str, limit: int) -> Sequence[CommittedRow]:
            seen["boot_id"] = await redis.get(K.BOOT_ID)
            seen["halt"] = await redis.hgetall(K.HALT_STATE)
            seen["groups"] = len(await redis.xinfo_groups(K.FEED))
            return await super().rows_after_key(after_key, limit)

    db = Probe()
    db.halt(1, "NONE")
    db.put(order("A"))
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    assert await sm8.run_once(in_place=False)
    assert seen["boot_id"] is None and seen["halt"] and seen["groups"] == 1
    assert await read_boot_id(redis) is not None


async def test_no_boot_writes_replayed_before_boot_id(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                      identity: BootIdentity, no_boot: NoBootQueue,
                                                      clock: ManualClock) -> None:
    await identity.refresh()
    late = committed_row(order("LATE", observed=500, seq=5000), T0)
    assert await cache.apply_after_commit(late.write()) is ApplyResult.NO_BOOT
    assert len(no_boot) == 1
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    assert await sm8.run_once(in_place=True)
    assert len(no_boot) == 0
    assert (await read_meta(redis, [late.key]))[late.key] is not None


async def test_seed_never_overwrites_fresher_live_values(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                         identity: BootIdentity, no_boot: NoBootQueue,
                                                         clock: ManualClock) -> None:
    seed_db(db)
    fresh = PriceDTO(adapter_id="stonex", symbol="EURUSD", bid=Decimal("1.2"), ask=Decimal("1.2001"),
                     spread_pips=Decimal("1"), timestamp_ms=T0, seq_id=1)
    await cache.write_price(fresh, Decimal("3"))
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    assert await sm8.run_once(in_place=True)
    assert await redis.hget(K.price_key("stonex", "EURUSD"), "bid") == b"1.2"
    assert await redis.hget(K.clean_mid_key("stonex", "EURUSD"), "seeded") == b"0"
    assert await redis.hget(K.account_live_key("ACC"), "equity") == b"100"   # absent, so seeded


async def test_busygroup_counts_as_success(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                           identity: BootIdentity, no_boot: NoBootQueue, clock: ManualClock) -> None:
    await redis.xgroup_create(K.FEED, "oversee", id="0", mkstream=True)
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    assert await sm8.run_once(in_place=True)


async def test_halt_state_ahead_is_replaced(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                            identity: BootIdentity, no_boot: NoBootQueue, clock: ManualClock) -> None:
    db.halt(5, "SOFT")
    await cache.set_halt_state("HARD", 9)
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    assert await sm8.run_once(in_place=True)
    assert await cache.read_halt_state() == ("SOFT", 5)


async def test_foreign_redis_data_is_healed(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                            identity: BootIdentity, no_boot: NoBootQueue, clock: ManualClock,
                                            run_id: RunId) -> None:
    await boot(redis, identity)
    stale_future = committed_row(order("A", observed=900, seq=9000), T0)
    orphan = committed_row(order("GHOST"), T0)
    await cache.apply(stale_future.write())
    await cache.apply(orphan.write())
    good = db.put(order("A", observed=100, seq=1000))
    run_id.value = "run-2"              # Redis restarted with its old data: boot_id is now foreign
    pages = Pages()
    sm8 = make(redis, cache, db, identity, no_boot, clock, pages)
    assert await sm8.run_once(in_place=True)
    metas = await read_meta(redis, [good.key, orphan.key])
    assert metas[orphan.key] is None
    m = metas[good.key]
    assert m is not None and m.version == good.version
    assert await redis.get(K.STATE_DIGEST) == good.digest_hash.encode()
    assert "redis_ahead" in pages.codes()
    assert await read_boot_id(redis) == f"run-2:{EPOCH}"


async def test_single_flight_other_owner_waits(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                               identity: BootIdentity, no_boot: NoBootQueue, clock: ManualClock) -> None:
    assert await try_lock(redis, "someone-else", 60_000)
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    assert await sm8.run_once(in_place=True) is False
    assert sm8.state is BootState.NOT_BOOTED
    with pytest.raises(BootstrapDeadline):
        await sm8.run_once(in_place=False, deadline_ms=clock())
    assert await read_boot_id(redis) is None


async def test_failing_step_retries(redis: Any, cache: RedisState, db: FakeIntegrityDb, identity: BootIdentity,
                                    no_boot: NoBootQueue, clock: ManualClock) -> None:
    db.halt(1, "SOFT")
    db.fail["latest_halt_event"] = 2
    db.fail["rows_after_key"] = 1
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    assert await sm8.run_once(in_place=True)
    assert db.calls.count("latest_halt_event") == 3
    assert await cache.read_halt_state() == ("SOFT", 1)


async def test_deadline_aborts_and_releases_lock(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                 identity: BootIdentity, no_boot: NoBootQueue) -> None:
    clock = TickingClock(10_000)
    db.fail["last_clean_marks"] = 1_000
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    with pytest.raises(BootstrapDeadline):
        await sm8.run_once(in_place=False, deadline_ms=clock.ms + 50_000)
    assert sm8.state is BootState.NOT_BOOTED
    assert await redis.get(K.BOOT_LOCK) is None
    assert await read_boot_id(redis) is None


async def test_in_place_failing_step_pages_after_60s(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                     identity: BootIdentity, no_boot: NoBootQueue) -> None:
    clock = TickingClock(25_000)
    db.fail["last_clean_marks"] = 4
    pages = Pages()
    sm8 = make(redis, cache, db, identity, no_boot, clock, pages)
    assert await sm8.run_once(in_place=True)
    assert pages.codes() == ["sm8_step_failing"]
    assert pages.alerts[0].severity == "critical"


async def test_price_barrier_waits_bounded(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                           identity: BootIdentity, no_boot: NoBootQueue) -> None:
    clock = TickingClock(1_000)
    sm8 = make(redis, cache, db, identity, no_boot, clock, price_adapters=lambda: ["stonex"], price_wait_ms=5_000)
    assert await sm8.run_once(in_place=True)          # no conn:stonex:prices: continues after the timeout
    await cache.write_conn("stonex", "prices", T0)
    await redis.delete(K.BOOT_ID)
    sm8b = make(redis, cache, db, identity, no_boot, ManualClock(), price_adapters=lambda: ["stonex"])
    assert await sm8b.run_once(in_place=True)         # live at once (a frozen clock would otherwise hang)


async def test_lock_lost_during_heal(redis: Any, cache: RedisState, identity: BootIdentity, no_boot: NoBootQueue,
                                     clock: ManualClock) -> None:
    class Thief(FakeIntegrityDb):
        async def rows_after_key(self, after_key: str, limit: int) -> Sequence[CommittedRow]:
            await redis.set(K.BOOT_LOCK, "thief")
            return await super().rows_after_key(after_key, limit)

    db = Thief()
    db.put(order("A"))
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    with pytest.raises(LockLost):
        await sm8.run_once(in_place=True)
    assert await read_boot_id(redis) is None
    assert await redis.get(K.BOOT_LOCK) == b"thief"     # never released someone else's lock


async def test_run_serves_signals(redis: Any, cache: RedisState, db: FakeIntegrityDb, identity: BootIdentity,
                                  no_boot: NoBootQueue, clock: ManualClock) -> None:
    db.put(order("A"))
    sm8 = make(redis, cache, db, identity, no_boot, clock)
    stop = asyncio.Event()
    task = asyncio.create_task(sm8.run(stop))
    sm8.signal("NO_BOOT")
    assert await sm8.wait_ready(2.0)
    assert sm8.runs == 1
    # Already booted: a later NO_BOOT signal only replays the queue (no second bootstrap).
    no_boot.add(committed_row(order("B"), T0).write())
    sm8.signal("NO_BOOT again")
    for _ in range(200):
        if not len(no_boot):
            break
        await asyncio.sleep(0.01)
    assert len(no_boot) == 0 and sm8.runs == 1
    stop.set()
    sm8.signal("stop")
    await asyncio.wait_for(task, 2.0)
