"""SM3 IntegrityWorker (tier-persistence.md §3): boot / halt checks, two-tick heals, quiet digest, Step 2."""
from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from dataclasses import replace
from decimal import Decimal
from typing import Any

from src.cache import keys as K
from src.cache.scripts import NoBootQueue, RedisState
from src.integrity.heal import HealOutcome
from src.integrity.incident import IncidentMachine
from src.integrity.ports import CommittedRow, DbHighWater, IntegrityState, committed_row
from src.integrity.redis_ops import BootIdentity, read_digest, read_meta
from src.integrity.worker import IntegrityConfig, IntegrityWorker
from tests.integrity.conftest import boot
from tests.integrity.fakes import (
    T0,
    FakeBoot,
    FakeIngestion,
    FakeIntegrityDb,
    FakePatroni,
    FakeReconciler,
    ManualClock,
    Pages,
    order,
)

NOISY = IntegrityConfig(quiet_ms=10**9)   # digest never quiet: isolates Step 1 key checks


class Env:
    def __init__(self, redis: Any, cache: RedisState, db: FakeIntegrityDb, identity: BootIdentity,
                 clock: ManualClock, config: IntegrityConfig = NOISY) -> None:
        self.redis, self.cache, self.db, self.clock = redis, cache, db, clock
        self.boot = FakeBoot()
        self.ingestion = FakeIngestion()
        self.patroni = FakePatroni()
        self.reconciler = FakeReconciler()
        self.pages = Pages()

        async def ready() -> bool:
            return True

        self.incident = IncidentMachine(redis=redis, ingestion=self.ingestion, patroni=self.patroni,
                                        reconciler=self.reconciler, boot=self.boot, boot_ready=ready,
                                        clock_ms=clock, pager=self.pages)
        self.worker = IntegrityWorker(redis, cache, db, identity, self.boot, self.incident, clock_ms=clock,
                                      pager=self.pages, config=config)


async def env_for(redis: Any, cache: RedisState, db: FakeIntegrityDb, identity: BootIdentity, clock: ManualClock,
                  config: IntegrityConfig = NOISY) -> Env:
    await boot(redis, identity)
    clock.ms = T0 + 1_000
    return Env(redis, cache, db, identity, clock, config)


async def test_boot_mismatch_signals_sm8_and_skips(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                   identity: BootIdentity, clock: ManualClock) -> None:
    e = Env(redis, cache, db, identity, clock)
    db.put(order("A"))
    rep = await e.worker.tick()
    assert not rep.boot_ok and rep.compared == 0
    assert e.boot.signals and "boot_id" in e.boot.signals[0]
    assert e.worker.state is IntegrityState.IDLE
    await redis.set(K.BOOT_ID, "other-run:cluster-a:7")       # foreign boot id
    await e.worker.tick()
    assert len(e.boot.signals) == 2


async def test_halt_state_checks(redis: Any, cache: RedisState, db: FakeIntegrityDb, identity: BootIdentity,
                                 clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    assert (await e.worker.tick()).halt == "none"
    db.halt(4, "SOFT")
    assert (await e.worker.tick()).halt == "behind"
    assert await cache.read_halt_state() == ("SOFT", 4)
    assert (await e.worker.tick()).halt == "match"
    await cache.set_halt_state("HARD", 9)                     # ahead (only possible after a restore)
    assert (await e.worker.tick()).halt == "ahead"
    assert await cache.read_halt_state() == ("SOFT", 4)
    assert "halt_state_ahead" in e.pages.codes()
    await redis.delete(K.HALT_STATE)
    await redis.hset(K.HALT_STATE, mapping={"junk": "1"})     # malformed: replaced
    assert (await e.worker.tick()).halt == "ahead"
    assert await cache.read_halt_state() == ("SOFT", 4)


async def test_halt_state_without_events_is_deleted(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                    identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    await cache.set_halt_state("SOFT", 2)
    assert (await e.worker.tick()).halt == "ahead"
    assert await cache.read_halt_state() is None


async def test_redis_behind_heals_on_second_tick(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                 identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    row = db.put(order("A"))
    rep1 = await e.worker.tick()
    assert rep1.compared == 1 and rep1.suspects == 1 and not rep1.healed
    assert (await read_meta(redis, [row.key]))[row.key] is None
    clock.advance(5_000)
    rep2 = await e.worker.tick()
    assert rep2.healed == {row.key: HealOutcome.HEALED}
    assert (await read_meta(redis, [row.key]))[row.key] is not None
    assert await redis.xlen(K.FEED) == 1                      # the heal wakes the RiskSweep
    assert (IntegrityState.CHECK_REDIS_VS_DB, IntegrityState.HEALING_REDIS, "1 keys mismatched on two ticks") \
        in list(e.worker.history)
    assert e.ingestion.calls == []                            # never pauses anything


async def test_copy_in_flight_is_not_healed(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                            identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    row = db.put(order("A"))
    assert (await e.worker.tick()).suspects == 1
    await cache.apply(row.write())                             # the consumer's post-commit copy lands
    clock.advance(5_000)
    rep = await e.worker.tick()
    assert rep.healed == {} and rep.suspects == 0


async def test_redis_ahead_is_deleted_and_reapplied(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                    identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    row = db.put(order("A", observed=100, seq=1000))
    ahead = committed_row(order("A", observed=900, seq=9000, stop="1.0"), T0)
    await cache.apply(ahead.write())
    await e.worker.tick()
    clock.advance(5_000)
    rep = await e.worker.tick()
    assert rep.healed == {row.key: HealOutcome.REPLACED}
    m = (await read_meta(redis, [row.key]))[row.key]
    assert m is not None and m.version == row.version and m.checksum == row.checksum
    assert await read_digest(redis) == row.digest_hash      # digest equals PostgreSQL's again
    assert "redis_ahead" in e.pages.codes()


async def test_rotating_sweep_reaches_old_rows(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                               identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock, replace(NOISY, sweep_keys=2))
    rows = [db.put(order(oid), T0 - 3_600_000) for oid in ("A", "B", "C")]   # outside every commit window
    for r in rows[:2]:
        await cache.apply(r.write())
    seen = 0
    for _ in range(6):
        rep = await e.worker.tick()
        seen += len(rep.healed)
        clock.advance(5_000)
    assert seen == 1
    metas = await read_meta(redis, [r.key for r in rows])
    assert all(metas[r.key] is not None for r in rows)


async def test_no_boot_during_heal_signals_sm8(redis: Any, cache: RedisState, identity: BootIdentity,
                                               clock: ManualClock) -> None:
    class Vanishing(FakeIntegrityDb):
        armed = False

        async def rows_by_keys(self, keys: Collection[str]) -> Mapping[str, CommittedRow]:
            if self.armed:
                await redis.delete(K.BOOT_ID)
            return await super().rows_by_keys(keys)

    db = Vanishing()
    e = await env_for(redis, cache, db, identity, clock)
    db.put(order("A"))
    await e.worker.tick()
    db.armed = True
    clock.advance(5_000)
    await e.worker.tick()
    assert any("NO_BOOT" in s for s in e.boot.signals)


async def test_digest_compared_only_when_quiet(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                               identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock, IntegrityConfig(quiet_ms=0))
    row = db.put(order("A"), T0 + 900)
    await cache.apply(row.write())
    assert (await e.worker.tick()).digest == "match"
    e2 = Env(redis, cache, db, identity, clock, IntegrityConfig(quiet_ms=500))
    assert (await e2.worker.tick()).digest == "not_quiet"     # a PostgreSQL commit 100 ms ago


async def test_digest_mismatch_on_two_quiet_ticks_runs_full_heal(redis: Any, cache: RedisState,
                                                                 db: FakeIntegrityDb, identity: BootIdentity,
                                                                 clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock, IntegrityConfig(quiet_ms=0, sweep_keys=1))
    row = db.put(order("A"), T0 - 3_600_000)
    await cache.apply(row.write())
    ghost = committed_row(order("GHOST"), T0)
    await cache.apply(ghost.write())                           # orphan: only the digest can see it
    assert (await e.worker.tick()).digest == "mismatch"
    clock.advance(5_000)
    assert (await e.worker.tick()).digest == "repaired"
    assert (await read_meta(redis, [ghost.key]))[ghost.key] is None
    assert await read_digest(redis) == row.digest_hash


async def test_drifted_digest_value_is_reset(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                             identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock, IntegrityConfig(quiet_ms=0))
    row = db.put(order("A"), T0 - 3_600_000)
    await cache.apply(row.write())
    await redis.set(K.STATE_DIGEST, "f" * 64)
    assert (await e.worker.tick()).digest == "mismatch"
    assert (await e.worker.tick()).digest == "repaired"
    assert await read_digest(redis) == row.digest_hash
    assert "digest_repaired" in e.pages.codes()


async def test_checksum_corruption_halts(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                         identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    row = db.put(order("A"))
    await cache.apply(row.write())
    db.corrupt(row.key, entity=order("A", stop="0.5"))         # stored checksum no longer matches the row
    rep = await e.worker.tick()
    assert [f.kind for f in rep.findings] == ["checksum"]
    assert rep.incident is IntegrityState.HALTED
    assert e.worker.state is IntegrityState.HALTED and e.worker.integrity_incident
    assert e.ingestion.calls == ["halt"]
    assert e.pages.alerts[-1].severity == "critical"
    await e.worker.tick()                                      # standby passes -> switchover
    assert e.worker.state is IntegrityState.SWITCHING_OVER


async def test_digest_hash_and_version_label_damage(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                    identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    row = db.put(order("A"))
    await cache.apply(row.write())
    db.corrupt(row.key, digest_hash="0" * 64, entity=order("A", observed=101))
    rep = await e.worker.tick()
    assert sorted(f.kind for f in rep.findings) == ["digest_hash", "version_label"]


async def test_seq_regression_same_timeline_halts(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                  identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    db.hw = DbHighWater(1, 7, 5_000)
    assert not (await e.worker.tick()).findings
    db.hw = DbHighWater(1, 8, 4_000)                           # a new epoch never excuses a decrease
    rep = await e.worker.tick()
    assert [f.kind for f in rep.findings] == ["seq_regression"]
    assert e.worker.state is IntegrityState.HALTED


async def test_failover_regression_goes_to_reconciling(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                       identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    db.hw = DbHighWater(1, 7, 5_000)
    await e.worker.tick()
    db.hw = DbHighWater(2, 7, 4_000)
    rep = await e.worker.tick()
    assert [f.kind for f in rep.findings] == ["failover_regression"]
    assert e.worker.state is IntegrityState.HALTED
    await e.worker.tick()
    assert e.worker.state is IntegrityState.RECONCILING
    await e.worker.tick()
    # no switchover for data lost by a failover; reconcile, FLUSHDB, SM8 (ready), resume -> IDLE
    assert e.patroni.calls == []
    assert e.reconciler.calls == [None] and e.boot.signals
    assert e.ingestion.calls == ["halt", "resume_prices", "resume_all"]
    assert e.worker.state is IntegrityState.IDLE and not e.worker.integrity_incident


async def test_version_regression(redis: Any, cache: RedisState, db: FakeIntegrityDb, identity: BootIdentity,
                                  clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock)
    row = db.put(order("A", observed=500, seq=5000))
    await cache.apply(row.write())
    await e.worker.tick()
    db.put(order("A", observed=100, seq=1000))
    clock.advance(5_000)
    rep = await e.worker.tick()
    assert "version_regression" in [f.kind for f in rep.findings]


async def test_run_loop_ticks_and_stamps_liveness(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                                  identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock, replace(NOISY, interval_s=0.01))
    db.fail["latest_halt_event"] = 1                           # one failing tick never kills the task
    stop = asyncio.Event()
    task = asyncio.create_task(e.worker.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1.0)
    assert e.worker.liveness_ms == clock.ms
    assert db.calls.count("latest_halt_event") >= 2
    assert e.worker.state is IntegrityState.IDLE


async def test_window_truncation_carries_over(redis: Any, cache: RedisState, db: FakeIntegrityDb,
                                              identity: BootIdentity, clock: ManualClock) -> None:
    e = await env_for(redis, cache, db, identity, clock, replace(NOISY, window_limit=2, sweep_keys=1))
    for i, oid in enumerate(("A", "B", "C")):
        r = db.put(order(oid), T0 + i)
        await cache.apply(r.write())
    rep = await e.worker.tick()
    assert rep.compared >= 2 and not rep.findings


def test_instrument_rows_are_checked() -> None:
    from src.adapters.mock import eurusd_spec
    from src.integrity.dbcheck import check_row
    spec = eurusd_spec("stonex")
    row = committed_row(spec, T0, seq_id=42)
    assert check_row(row) == []
    bad = replace(row, entity=replace(spec, contract_size=Decimal(2)))
    assert [f.kind for f in check_row(bad)] == ["checksum"]


async def test_no_boot_queue_is_independent(no_boot: NoBootQueue) -> None:
    assert len(no_boot) == 0
