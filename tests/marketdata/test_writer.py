"""`md-writer`: closed-bar queue, AOF release rule, Redis errors, NO_BOOT, retention, memory shedding, stop flag
(plan S15; design §A8, §A5, §A7, §A13). fakeredis + lupa with a fake `AofWaiter`; synthetic instruments and prices
only, and no price appears in an assertion message."""
from __future__ import annotations

import ast
import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from src.dto.entities import PriceDTO
from src.marketdata.codec import decode_member, DecodedMember
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.client import MdWriteClient, WriteResult
from src.marketdata.store.runner import (ScriptFailed, ScriptOk, ScriptRefused, ScriptReply, ScriptRunner,
                                         StoreUnavailable)
from src.marketdata.tickbars import TickBar
from src.marketdata.ticks import MdHook, TickDrain
from src.marketdata.types import M1, M5, M15, H1, Bar, BarEndEvent, MdGeneration, Quality, SeriesId, Side, Source
from src.marketdata import writer as W
from src.marketdata.writer import MdWriter

from .conftest import KEYRING, RUN_ID, SENTINEL, T0, TOKEN, FakeAof, RunIds, bar, hash_of

H0 = 1_699_999_200_000          # on the hour
MIN = 60_000
AD, SYM = "mock", "SYN1"
BID_1M = SeriesId(AD, SYM, M1, Side.BID, Source.TICK)
ASK_1M = SeriesId(AD, SYM, M1, Side.ASK, Source.TICK)
BROKER_15M = SeriesId(AD, SYM, M15, Side.BID, Source.BROKER)
PRICES = ("1.10003", "1.10010", "1.10017", "1.10024")


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


@dataclass
class Pages:
    sent: list[tuple[str, str, str]] = field(default_factory=list)

    def page(self, severity: str, title: str, body: str) -> None:
        self.sent.append((severity, title, body))


@dataclass
class Memory:
    ratio: float | None = 0.10
    error: bool = False
    reads: int = 0

    async def memory_ratio(self) -> float | None:
        self.reads += 1
        if self.error:
            raise StoreUnavailable("timeout", "TimeoutError")
        return self.ratio


class Stamp:
    def __init__(self) -> None:
        self.stamps = 0

    def stamp(self) -> None:
        self.stamps += 1

    def set_exempt(self, reason: str | None) -> None:
        pass


class DownWrite:
    """A writer connection whose every call fails like an unreachable `redis-md`."""

    def __init__(self) -> None:
        self.calls = 0

    async def run(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]:
        self.calls += 1
        raise StoreUnavailable("connection", "ConnectionError")

    async def write_and_wait(self, calls: Sequence[S.ScriptCall]) -> WriteResult:
        self.calls += 1
        raise StoreUnavailable("connection", "ConnectionError")


class Switch:
    """Delegates to the real client, or fails while `down`."""

    def __init__(self, real: MdWriteClient) -> None:
        self.real = real
        self.down = False
        self.down_write = DownWrite()

    async def run(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]:
        return await (self.down_write.run(calls) if self.down else self.real.run(calls))

    async def write_and_wait(self, calls: Sequence[S.ScriptCall]) -> WriteResult:
        return await (self.down_write.write_and_wait(calls) if self.down else self.real.write_and_wait(calls))


def dto(bid: str, broker_ms: int, *, received_ms: int | None = None) -> PriceDTO:
    b = Decimal(bid)
    return PriceDTO(adapter_id=AD, symbol=SYM, bid=b, ask=b + Decimal("0.0002"), spread_pips=None,
                    timestamp_ms=broker_ms, tradeable=True, ts_source="BROKER",
                    received_ms=broker_ms + 100 if received_ms is None else received_ms)


def feed(hook: MdHook, start: int, n: int, step: int = 10_000) -> list[int]:
    """`n` ticks from `start`, `step` apart; returns their receipt times."""
    out = []
    for i in range(n):
        ts = start + i * step + 7
        hook.on_price(dto(PRICES[i % len(PRICES)], ts))
        out.append(ts + 100)
    return out


@dataclass
class Rig:
    writer: MdWriter
    hook: MdHook
    clock: Clock
    redis: Any
    aof: FakeAof
    pages: Pages
    released: list[BarEndEvent]
    boots: list[str]
    refetches: list[tuple[SeriesId, int, int]]

    async def at(self, now: int, stop: asyncio.Event | None = None) -> None:
        self.clock.now = now
        await self.writer.cycle(stop)


async def rig(md_redis: Any, run_ids: RunIds, *, aof: FakeAof | None = None, aof_off: bool = False,
              write: Any = None, memory: Memory | None = None, start: int = H0 - MIN,
              hook: MdHook | None = None, **kw: Any) -> Rig:
    await md_redis.set(K.WRITER, TOKEN.text)
    await md_redis.set(K.BOOT, SENTINEL.text)
    aof = aof or FakeAof()
    client = MdWriteClient(md_redis, ScriptRunner(md_redis, run_id_reader=run_ids), None if aof_off else aof)
    clock = Clock(start)
    h = hook or MdHook(clock_ms=clock, unfiltered=(AD,))
    h.seed_subscriptions(AD, {SYM: start})
    pages, released, boots, refetches = Pages(), [], [], []   # type: ignore[var-annotated]
    w = MdWriter(h, write if write is not None else client, token=TOKEN, keyring=KEYRING, boot=SENTINEL,
                 memory=memory, pager=pages, request_bootstrap=boots.append,
                 refetch=lambda s, a, b: refetches.append((s, a, b)), clock_ms=clock, mono_ms=clock, **kw)
    w.on_released(released.append)
    return Rig(w, h, clock, md_redis, aof, pages, released, boots, refetches)


async def stored_bars(r: Any, series: SeriesId) -> list[Bar]:
    out = []
    for member in await r.zrange(K.bars(series), 0, -1):
        decoded = decode_member(series, member)
        assert isinstance(decoded, DecodedMember)
        out.append(decoded.bar)
    return out


async def event_count(r: Any) -> int:
    return len(await r.xrange(K.EVENTS))


# ---------------------------------------------------------------------------------------------------- release rule
async def test_closed_bars_are_released_on_waitaof_and_handed_to_the_listeners(md_redis: Any,
                                                                             run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids)
    feed(r.hook, H0, 12)                                     # two minutes of ticks
    await r.at(H0 + 2 * MIN + 6_000)                         # cold skew: T + 5 s finalises both minutes
    assert r.writer.queue_depth() == 0 and r.aof.calls == [1]
    assert [b.bucket_start_ms for b in await stored_bars(md_redis, BID_1M)] == [H0, H0 + MIN]
    assert {(e.series, e.bar_end_ms) for e in r.released} == {
        (s, H0 + k * MIN) for s in (BID_1M, ASK_1M) for k in (1, 2)}
    assert all(e.write_seq >= 1 and e.content_hash for e in r.released)
    assert await md_redis.xlen(K.ticks(AD, SYM)) == 12 and r.writer.counters.ticks_written == 12
    assert r.writer.gapped == frozenset() and r.writer.counters.aof_wait_timeouts == 0


async def test_an_aof_wait_timeout_keeps_bars_queued_and_rewrites_them_idempotently(md_redis: Any,
                                                                                  run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids, aof=FakeAof(local=0))
    feed(r.hook, H0, 12)
    await r.at(H0 + 2 * MIN + 6_000)
    assert r.writer.queue_depth() == 4 and r.released == []           # written, not released
    assert r.writer.counters.aof_wait_timeouts == 1 and r.writer.counters.bars_requeued == 4
    assert await md_redis.xlen(K.ticks(AD, SYM)) == 12                  # ticks leave on the script reply
    events, members = await event_count(md_redis), await md_redis.zrange(K.bars(BID_1M), 0, -1)
    r.aof.local = 0
    r.aof.error = True                                                  # a WAITAOF error keeps them queued too
    await r.at(H0 + 2 * MIN + 6_250)
    assert r.writer.queue_depth() == 4 and r.writer.counters.aof_wait_timeouts == 2
    r.aof.error, r.aof.local = False, 1
    await r.at(H0 + 2 * MIN + 6_500)
    assert r.writer.queue_depth() == 0
    assert await event_count(md_redis) == events                        # identical re-write: no new event
    assert await md_redis.zrange(K.bars(BID_1M), 0, -1) == members      # nor a new member or write_seq
    assert await md_redis.xlen(K.ticks(AD, SYM)) == 12                  # ticks never re-sent
    seqs = sorted(e.write_seq for e in r.released if e.series == BID_1M)
    assert len(r.released) == 4 and seqs == [1, 2]                      # the first write's event ids


async def test_aof_off_releases_on_the_script_reply(md_redis: Any, run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids, aof_off=True)
    feed(r.hook, H0, 12)
    await r.at(H0 + 2 * MIN + 6_000)
    assert r.writer.queue_depth() == 0 and len(r.released) == 4 and r.aof.calls == []


async def test_a_revision_queued_while_a_write_is_in_flight_stays_queued(md_redis: Any, run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids)
    first = bar(T0, series=BROKER_15M)
    newer = bar(T0, "1.2399", revision=1, series=BROKER_15M)
    r.aof.on_wait = lambda: r.writer.queue_bar(newer)                    # md-bars hands a revision mid-flush
    r.writer.queue_bar(first)
    await r.at(T0 + 960_000)
    assert r.writer.queue_depth(BROKER_15M) == 1
    r.aof.on_wait = None
    await r.at(T0 + 960_250)
    assert r.writer.queue_depth(BROKER_15M) == 0
    assert [b.revision for b in await stored_bars(md_redis, BROKER_15M)] == [1]


# ---------------------------------------------------------------------------------------------------- redis errors
async def test_redis_down_keeps_the_newest_ticks_and_every_bar_and_marks_series_gapped(md_redis: Any,
                                                                                    run_ids: RunIds) -> None:
    probe = await rig(md_redis, run_ids)
    switch = Switch(probe.writer._write)   # type: ignore[arg-type]  # the real client of the probe rig
    r = await rig(md_redis, run_ids, write=switch, pending_ticks=5)
    switch.down = True
    received = feed(r.hook, H0, 12)
    await r.at(H0 + 2 * MIN + 6_000)
    assert r.writer.pending_ticks((AD, SYM)) == 5 and r.writer.counters.ticks_dropped == 7
    assert r.writer.queue_depth() == 4 and r.writer.counters.flush_errors == 1
    assert {BID_1M, ASK_1M} <= r.writer.gapped
    await r.at(H0 + 2 * MIN + 6_250)                         # still down: nothing lost, nothing grows
    assert r.writer.pending_ticks((AD, SYM)) == 5 and r.writer.queue_depth() == 4
    switch.down = False
    await r.at(H0 + 2 * MIN + 6_500)
    ids = [int(i.decode().split("-")[0]) for i, _ in await md_redis.xrange(K.ticks(AD, SYM))]
    assert ids == received[-5:]                               # the newest ticks survived the outage
    assert r.writer.queue_depth() == 0 and len(r.released) == 4 and r.writer.gapped == frozenset()


async def test_a_flush_that_overruns_its_budget_counts_as_a_store_error(md_redis: Any, run_ids: RunIds) -> None:
    class Hung(DownWrite):
        async def write_and_wait(self, calls: Sequence[S.ScriptCall]) -> WriteResult:
            await asyncio.sleep(10)
            raise AssertionError("unreachable")

    r = await rig(md_redis, run_ids, write=Hung(), flush_budget_s=0.05)
    feed(r.hook, H0, 12)
    await asyncio.wait_for(r.at(H0 + 2 * MIN + 6_000), 2.0)
    assert r.writer.counters.flush_errors == 1 and r.writer.queue_depth() == 4 and r.writer.gapped


async def test_no_boot_pauses_writes_requests_one_bootstrap_and_resumes_on_install(md_redis: Any,
                                                                                  run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids)
    await md_redis.delete(K.BOOT)                             # a FLUSHALL / older AOF: the sentinel is gone
    feed(r.hook, H0, 12)
    await r.at(H0 + 2 * MIN + 6_000)
    assert r.boots == ["no_boot"] and r.writer.boot is None and r.writer.bootstrap_wanted == "no_boot"
    assert r.writer.queue_depth() == 4 and r.writer.pending_ticks((AD, SYM)) == 12 and r.writer.gapped
    await r.at(H0 + 2 * MIN + 6_250)
    assert r.boots == ["no_boot"]                             # asked once; nothing sent while waiting
    newer = K.BootSentinel(RUN_ID, SENTINEL.cluster_id, SENTINEL.node, MdGeneration(7, 2))
    await md_redis.set(K.BOOT, newer.text)
    r.writer.install_boot(newer)
    await r.at(H0 + 2 * MIN + 6_500)
    assert r.writer.queue_depth() == 0 and await md_redis.xlen(K.ticks(AD, SYM)) == 12
    assert r.writer.bootstrap_wanted is None and r.writer.counters.no_boot == 1


async def test_a_fenced_token_is_no_writer_and_requests_a_bootstrap(md_redis: Any, run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids)
    await md_redis.set(K.WRITER, "1234567890:8:node-b:other")
    r.writer.queue_bar(bar(T0, series=BROKER_15M))
    await r.at(T0 + 960_000)
    assert r.boots == ["no_writer"] and r.writer.queue_depth() == 1 and BROKER_15M in r.writer.gapped


async def test_a_writer_without_a_sentinel_waits_for_the_term_start_bootstrap(md_redis: Any,
                                                                             run_ids: RunIds) -> None:
    write = DownWrite()
    r = await rig(md_redis, run_ids, write=write)
    w = MdWriter(r.hook, write, token=TOKEN, keyring=KEYRING, clock_ms=r.clock, mono_ms=r.clock)
    feed(r.hook, H0, 12)
    r.clock.now = H0 + 2 * MIN + 6_000
    await w.cycle()
    assert w.bootstrap_wanted == "start" and write.calls == 0 and w.queue_depth() == 4


# ---------------------------------------------------------------------------------------------------- queue bound
async def test_queue_bound_drops_the_oldest_bar_and_refetches_a_broker_bucket(md_redis: Any,
                                                                             run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids, write=DownWrite(), queue_bars=3)
    tick_series = SeriesId(AD, SYM, M15, Side.BID, Source.TICK)
    for k in range(5):
        r.writer.queue_bar(bar(T0 + k * 900_000, series=BROKER_15M))
        r.writer.queue_bar(bar(T0 + k * 900_000, series=tick_series))
    r.writer.queue_bar(bar(T0 + 4 * 900_000, "1.2399", revision=1, series=BROKER_15M))   # same bucket: replaces
    assert r.writer.queue_depth(BROKER_15M) == 3 and r.writer.queue_depth(tick_series) == 3
    assert r.writer.counters.queue_dropped == 4
    assert r.refetches == [(BROKER_15M, T0, T0), (BROKER_15M, T0 + 900_000, T0 + 900_000)]   # TICK: blind only
    await r.at(T0 + 5 * 900_000)                                      # never dropped by a failing flush
    assert r.writer.queue_depth() == 6


async def test_default_queue_bound_is_six_hours_of_one_minute_bars() -> None:
    assert W.QUEUE_BARS_PER_SERIES == 6 * 60
    with pytest.raises(ValueError):
        MdWriter(MdHook(clock_ms=lambda: H0), DownWrite(), token=TOKEN, keyring=KEYRING, queue_bars=0)


# ---------------------------------------------------------------------------------------------------- forming, quarantine
async def test_forming_bars_are_written_and_cleared_once_final(md_redis: Any, run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids)
    feed(r.hook, H0, 3)
    await r.at(H0 + 30_000)
    assert (await md_redis.hget(K.forming(BID_1M), "start")).decode() == str(H0)
    assert await md_redis.exists(K.forming(SeriesId(AD, SYM, H1, Side.ASK, Source.TICK)))
    calls_before = r.aof.calls[:]
    await r.at(H0 + 30_250)                                   # unchanged: nothing to write, no call
    assert r.aof.calls == calls_before
    await r.at(H0 + MIN + 6_000)                              # the minute is final: forming cleared
    assert not await md_redis.exists(K.forming(BID_1M))
    assert [b.bucket_start_ms for b in await stored_bars(md_redis, BID_1M)] == [H0]


class BadBarHook(MdHook):
    """Hands the writer one closed TICK bar that fails the price check (high below the open)."""

    def __init__(self, *, clock_ms: Any, bad: Bar) -> None:
        super().__init__(clock_ms=clock_ms, unfiltered=(AD,))
        self.bad: list[Bar] = [bad]

    def drain(self, adapter_id: str, symbol: str) -> TickDrain:
        out = super().drain(adapter_id, symbol)
        extra = tuple(TickBar(b, True, False) for b in self.bad)
        self.bad = []
        return replace(out, closed=out.closed + extra)


async def test_a_tick_bar_failing_its_checks_is_quarantined_not_stored(md_redis: Any, run_ids: RunIds) -> None:
    clock = Clock(H0)
    bad = Bar(series=BID_1M, bucket_start_ms=H0, open=Decimal("1.3"), high=Decimal("1.2"), low=Decimal("1.1"),
              close=Decimal("1.15"), tick_count=3, quality=Quality.COMPLETE, final_at_ms=H0 + MIN)
    h = BadBarHook(clock_ms=clock, bad=bad)
    h.on_price(dto("1.10000", H0 - 5_000))                    # makes the market known to the hook
    r = await rig(md_redis, run_ids, hook=h, start=H0)
    await r.at(H0 + 2 * MIN)
    assert r.writer.counters.quarantined == 1 and r.writer.queue_depth(BID_1M) == 0
    assert H0 not in [b.bucket_start_ms for b in await stored_bars(md_redis, BID_1M)]   # its bucket stays blind
    (entry,) = await md_redis.zrange(K.quarantine(BID_1M), 0, -1)
    assert b"|prices|" in entry and await md_redis.hget(K.meta(BID_1M), "quarantined") == b"1"


# ---------------------------------------------------------------------------------------------------- retention, shedding
async def test_retention_trims_ticks_by_minid_and_tick_bars_by_floor_never_broker(md_redis: Any,
                                                                                run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids)
    feed(r.hook, H0, 12)
    r.writer.queue_bar(bar(H0 - 900_000, series=BROKER_15M))
    await r.at(H0 + 2 * MIN + 6_000)
    assert await md_redis.xlen(K.ticks(AD, SYM)) == 12 and r.writer.counters.retain_runs == 1
    later = H0 + 2 * 3_600_000 + MIN + 30_000                 # 2 h later: the first minute's ticks expire
    await r.at(later)
    left = [int(i.decode().split("-")[0]) for i, _ in await md_redis.xrange(K.ticks(AD, SYM))]
    assert left and min(left) >= later - W.TICK_RETENTION_MS and len(left) < 12
    minid, floors = r.writer.retention_floors(later)
    assert minid == later - W.TICK_RETENTION_MS
    assert floors[BID_1M] == M1.bucket_start_ms(later - W.TICK_1M_RETENTION_MS)
    assert BROKER_15M not in floors and all(s.source is Source.TICK for s in floors)
    eight_days = later + 8 * W.DAY_MS
    await r.at(eight_days)                                    # TICK 1m beyond 7 days goes; BROKER stays
    assert not await md_redis.zcard(K.bars(BID_1M)) and await md_redis.zcard(K.bars(BROKER_15M)) == 1
    assert await md_redis.hget(K.meta(BID_1M), "first_bucket") is None


async def test_shedding_steps_page_in_order_and_change_retention_and_demand(md_redis: Any,
                                                                           run_ids: RunIds) -> None:
    mem = Memory(0.50)
    demanded = SeriesId(AD, SYM, M15, Side.BID, Source.TICK)
    r = await rig(md_redis, run_ids, memory=mem, is_demanded=lambda s: s == demanded)
    feed(r.hook, H0, 36)
    now = H0 + 6 * MIN + 6_000
    await r.at(now)                                           # TICK 1m and 5m series written at level 0
    minid, floors = r.writer.retention_floors(now)
    assert minid == now - W.TICK_RETENTION_MS and floors[BID_1M] == M1.bucket_start_ms(now - W.TICK_1M_RETENTION_MS)
    expected_1m = {0: W.TICK_1M_RETENTION_MS, 1: W.TICK_1M_RETENTION_MS, 2: W.TICK_1M_SHED_RETENTION_MS,
                   3: W.TICK_1M_SHED_RETENTION_MS, 4: W.TICK_1M_SHED_RETENTION_MS}
    for ratio, level in ((0.72, 0), (0.76, 1), (0.81, 2), (0.86, 3), (0.91, 4)):
        mem.ratio = ratio
        now += W.MEMORY_EVERY_MS
        await r.at(now)
        assert r.writer.shed_level == level
        minid, floors = r.writer.retention_floors(now)
        assert minid == now - (W.TICK_SHED_RETENTION_MS if level >= 1 else W.TICK_RETENTION_MS)
        assert floors[BID_1M] == M1.bucket_start_ms(now - expected_1m[level])
        keep_5m = W.TICK_5M_SHED_RETENTION_MS if level >= 3 else W.TICK_ROLLUP_RETENTION_MS
        assert floors[SeriesId(AD, SYM, M5, Side.ASK, Source.TICK)] == M5.bucket_start_ms(now - keep_5m)
        assert all(s.source is Source.TICK for s in floors)
    titles = [t for _, t, _ in r.pages.sent]
    assert titles == ["redis-md memory high"] + [f"redis-md memory shedding step {k}" for k in (1, 2, 3, 4)]
    assert all("%" in body for _, _, body in r.pages.sent)
    r.writer.queue_bar(bar(T0, series=BROKER_15M))           # BROKER is never shed
    base = M1.bucket_start_ms(now) + MIN
    feed(r.hook, base, 12)
    now = base + 2 * MIN + 6_000
    await r.at(now)
    assert r.writer.counters.shed_bars > 0                    # undemanded TICK specs are no longer stored
    assert [b.bucket_start_ms for b in await stored_bars(md_redis, BID_1M)] == [H0 + k * MIN for k in range(6)]
    assert await md_redis.zcard(K.bars(BROKER_15M)) == 1
    mem.ratio = 0.72                                          # hysteresis: back to step 1, no new page
    now += W.MEMORY_EVERY_MS
    await r.at(now)
    assert r.writer.shed_level == 1 and len(r.pages.sent) == 5


def test_shedding_order_and_thresholds_follow_the_design() -> None:
    assert W.SHED_THRESHOLDS == (0.75, 0.80, 0.85, 0.90) and W.MEMORY_PAGE_RATIO == 0.70
    assert [s.split()[0] for s in W.SHED_STEPS] == ["ticks", "TICK", "TICK", "undemanded"]
    assert W.TICK_SHED_RETENTION_MS == 30 * MIN and W.TICK_1M_SHED_RETENTION_MS == 2 * W.DAY_MS
    assert W.TICK_5M_SHED_RETENTION_MS == 30 * W.DAY_MS


async def test_a_memory_refusal_after_the_last_step_pages_once(md_redis: Any, run_ids: RunIds) -> None:
    class Oom(DownWrite):
        async def write_and_wait(self, calls: Sequence[S.ScriptCall]) -> WriteResult:
            from src.marketdata.store.runner import ScriptFailed
            return WriteResult(tuple(ScriptFailed("ResponseError(OOM)") for _ in calls), 1)

    mem = Memory(0.95)
    r = await rig(md_redis, run_ids, write=Oom(), memory=mem)
    await r.at(H0)                                            # reaches step 4
    r.writer.queue_bar(bar(T0, series=BROKER_15M))
    await r.at(H0 + 250)
    r.writer.queue_bar(bar(T0 + 900_000, series=BROKER_15M))
    await r.at(H0 + 500)
    oom = [t for _, t, _ in r.pages.sent if "refuses writes" in t]
    assert oom == ["redis-md refuses writes for memory"] and r.writer.counters.oom_refusals == 2
    assert r.writer.queue_depth() == 2


async def test_a_failing_memory_read_changes_nothing(md_redis: Any, run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids, memory=Memory(error=True))
    await r.at(H0)
    assert r.writer.counters.memory_errors == 1 and r.writer.shed_level == 0 and r.pages.sent == []
    assert not r.writer.status().memory_ok


async def test_status_marks_the_memory_ratio_stale_after_a_failed_read(md_redis: Any, run_ids: RunIds) -> None:
    mem = Memory(0.50)
    r = await rig(md_redis, run_ids, memory=mem)
    await r.at(H0)
    st = r.writer.status()
    assert st.memory_ratio == 0.50 and st.memory_ok
    mem.error = True                                          # redis-md stops answering: the old ratio is stale
    await r.at(H0 + W.MEMORY_EVERY_MS)
    st = r.writer.status()
    assert mem.reads == 2 and r.writer.counters.memory_errors == 1 and not st.memory_ok
    mem.error, mem.ratio = False, 0.40
    await r.at(H0 + 2 * W.MEMORY_EVERY_MS)
    st = r.writer.status()
    assert st.memory_ratio == 0.40 and st.memory_ok


# ---------------------------------------------------------------------------------------------------- task body
async def test_the_stop_flag_starts_no_new_write_and_run_returns(md_redis: Any, run_ids: RunIds) -> None:
    write = DownWrite()
    r = await rig(md_redis, run_ids, write=write)
    feed(r.hook, H0, 12)
    stop = asyncio.Event()
    stop.set()
    await r.at(H0 + 2 * MIN + 6_000, stop)
    assert write.calls == 0 and r.writer.queue_depth() == 4  # drained, not written
    stamp = Stamp()
    await asyncio.wait_for(r.writer.run(stop, stamp), 1.0)
    assert stamp.stamps == 0
    live = asyncio.Event()
    task = asyncio.create_task(r.writer.run(live, stamp))
    await asyncio.sleep(0.05)
    live.set()
    await asyncio.wait_for(task, 1.0)
    assert stamp.stamps >= 2


async def test_the_writer_runs_its_own_clock_step_detector(md_redis: Any, run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids, aof_off=True)
    mono = Clock(H0)
    r.writer = MdWriter(r.hook, r.writer._write, token=TOKEN, keyring=KEYRING,   # type: ignore[arg-type]
                        boot=SENTINEL, clock_ms=r.clock, mono_ms=mono)
    await r.at(H0)
    mono.now = H0 + 250
    await r.at(H0 + 10_250)                                   # the wall jumped 10 s ahead of the monotonic clock
    assert r.writer.counters.clock_steps == 1 and r.hook.counters.clock_steps == 1


async def test_the_writer_never_awaits_broker_io(md_redis: Any, run_ids: RunIds) -> None:
    source = Path(W.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    imported |= {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert not {m for m in imported if m.startswith(("src.adapters", "src.runtime", "src.pubsub"))}
    assert "BrokerBarsPort" not in source and "fetch_price_bars" not in source


async def test_no_price_reaches_a_log_a_page_or_the_status(md_redis: Any, run_ids: RunIds,
                                                           caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    mem = Memory(0.95)
    r = await rig(md_redis, run_ids, memory=mem, aof=FakeAof(local=0), queue_bars=1)
    feed(r.hook, H0, 12)
    await r.at(H0 + 2 * MIN + 6_000)
    await md_redis.delete(K.BOOT)
    await r.at(H0 + 2 * MIN + 6_250)
    text = " ".join(rec.getMessage() for rec in caplog.records) + repr(r.pages.sent) + repr(r.writer.status())
    text += repr(r.writer.counters)
    for p in PRICES:
        assert p not in text and p.rstrip("0") not in text


# ---------------------------------------------------------------------------------------------------- RETAIN script
async def test_retain_script_trims_ticks_and_tick_bars_and_moves_first_bucket(md_redis: Any,
                                                                            runner: ScriptRunner) -> None:
    tick_15m = SeriesId(AD, SYM, M15, Side.BID, Source.TICK)
    ticks = [S.TickWrite(H0 + k * 1_000, "1.1", "1.2", H0 + k * 1_000, "BROKER", "tradeable") for k in range(6)]
    closed = tuple(S.ClosedBarWrite(b, hash_of(b)) for b in (bar(H0 + k * 900_000, series=tick_15m)
                                                             for k in range(3)))
    (ok,) = await runner.run([S.flush_call(TOKEN, SENTINEL, AD, SYM, H0 + 3_600_000, ticks,
                                           [S.SeriesFlush(tick_15m, closed=closed)], rollback_n=8)])
    assert isinstance(ok, ScriptOk)
    rb_before = await md_redis.zrange(K.rollback(tick_15m), 0, -1)
    call = S.retain_call(TOKEN, SENTINEL, AD, SYM, tick_minid_ms=H0 + 3_000, floors=[(tick_15m, H0 + 900_000)],
                         tick_maxlen=2)
    (reply,) = await runner.run([call])
    assert isinstance(reply, ScriptOk)
    parsed = S.parse_retain_reply(reply.value)
    assert parsed.ticks_trimmed == 4 and parsed.bars_removed == (1,)
    assert [i.decode() for i, _ in await md_redis.xrange(K.ticks(AD, SYM))] == [f"{H0 + 4_000}-0", f"{H0 + 5_000}-0"]
    assert await md_redis.hget(K.meta(tick_15m), "first_bucket") == str(H0 + 900_000).encode()
    assert await md_redis.zrange(K.rollback(tick_15m), 0, -1) == rb_before   # rollback set never shed
    with pytest.raises(ValueError):
        S.retain_call(TOKEN, SENTINEL, AD, SYM, tick_minid_ms=0, floors=[(BROKER_15M, 0)])
    with pytest.raises(ValueError):
        S.retain_call(TOKEN, SENTINEL, AD, SYM, tick_minid_ms=0,
                      floors=[(SeriesId(AD, "SYN2", M15, Side.BID, Source.TICK), 0)])


async def test_retain_script_is_guarded_and_checks_before_writing(md_redis: Any, runner: ScriptRunner) -> None:
    tick_15m = SeriesId(AD, SYM, M15, Side.BID, Source.TICK)
    await md_redis.xadd(K.ticks(AD, SYM), {"b": "1"}, id=f"{H0}-0")
    stale = K.BootSentinel(RUN_ID, SENTINEL.cluster_id, SENTINEL.node, MdGeneration(7, 0))
    call = S.retain_call(TOKEN, stale, AD, SYM, tick_minid_ms=H0 + 1, floors=[])
    assert await runner.run([call]) == [ScriptRefused("NO_BOOT")]
    await md_redis.hset(K.meta(tick_15m), "first_bucket", "corrupt")
    call = S.retain_call(TOKEN, SENTINEL, AD, SYM, tick_minid_ms=H0 + 1, floors=[(tick_15m, H0)])
    assert await runner.run([call]) == [ScriptRefused("BAD_ARGS")]
    assert await md_redis.xlen(K.ticks(AD, SYM)) == 1      # nothing trimmed before the refusal


# ---------------------------------------------------------------------------------------------------- client memory
async def test_memory_ratio_reads_info_memory() -> None:
    class FakeInfo:
        def __init__(self, reply: Any) -> None:
            self.reply = reply

        async def info(self, section: str) -> Any:
            assert section == "memory"
            if isinstance(self.reply, Exception):
                raise self.reply
            return self.reply

    def client(reply: Any) -> MdWriteClient:
        fake: Any = FakeInfo(reply)
        return MdWriteClient(fake, ScriptRunner(fake, run_id_reader=RunIds()), None)

    assert await client({"used_memory": 750, "maxmemory": 1000}).memory_ratio() == 0.75
    assert await client({"used_memory": 750, "maxmemory": 0}).memory_ratio() is None
    assert await client({"used_memory": "x"}).memory_ratio() is None
    from redis.exceptions import ConnectionError as RedisConnectionError
    with pytest.raises(StoreUnavailable):
        await client(RedisConnectionError("down")).memory_ratio()


# ---------------------------------------------------------------------------------------------------- review fixes
class HungWrite:
    """A writer connection that never answers (scripts and `WAITAOF`)."""

    def __init__(self) -> None:
        self.runs = 0
        self.waits = 0

    async def run(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]:
        self.runs += 1
        await asyncio.sleep(30)
        raise AssertionError("unreachable")

    async def write_and_wait(self, calls: Sequence[S.ScriptCall]) -> WriteResult:
        self.waits += 1
        await asyncio.sleep(30)
        raise AssertionError("unreachable")


@dataclass
class HungMemory:
    reads: int = 0

    async def memory_ratio(self) -> float | None:
        self.reads += 1
        await asyncio.sleep(30)
        raise AssertionError("unreachable")


async def timed(coro: Any) -> float:
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await asyncio.wait_for(coro, 5.0)
    return loop.time() - t0


async def test_one_cycle_stays_inside_its_deadline_when_every_await_hangs(md_redis: Any, run_ids: RunIds) -> None:
    assert W.FLUSH_BUDGET_S <= W.CYCLE_BUDGET_S < W.STAMP_BUDGET_S
    budget, slack = 0.5, 0.25
    write, mem = HungWrite(), HungMemory()
    r = await rig(md_redis, run_ids, write=write, memory=mem, flush_budget_s=0.4, cycle_budget_s=budget)
    feed(r.hook, H0, 12)
    r.clock.now = H0 + 2 * MIN + 6_000
    elapsed = await timed(r.writer.cycle())                  # the flush hangs: no memory read nor RETAIN after it
    assert elapsed < budget + slack and write.waits == 1 and mem.reads == 0
    assert r.writer.queue_depth() == 4 and r.writer.pending_ticks((AD, SYM)) == 12 and r.writer.gapped

    idle_write, idle_mem = HungWrite(), HungMemory()          # nothing to flush: memory read and RETAIN both hang
    idle = await rig(md_redis, run_ids, write=idle_write, memory=idle_mem, flush_budget_s=0.4, cycle_budget_s=budget)
    idle.writer.track_retention(series=[BID_1M])
    idle.clock.now = H0
    elapsed = await timed(idle.writer.cycle())
    assert elapsed < budget + slack and idle_mem.reads == 1 and idle_write.runs + idle_write.waits <= 1
    c = idle.writer.counters
    assert c.memory_errors == 1 and c.retain_errors + c.deadline_skips == 1


async def test_run_stamps_inside_the_budget_while_the_store_hangs(md_redis: Any, run_ids: RunIds) -> None:
    budget = 0.3
    r = await rig(md_redis, run_ids, write=HungWrite(), memory=HungMemory(), flush_budget_s=0.25,
                  cycle_budget_s=budget, interval_s=0.05)
    feed(r.hook, H0, 12)
    r.clock.now = H0 + 2 * MIN + 6_000
    loop = asyncio.get_running_loop()
    times: list[float] = []

    class TimedStamp(Stamp):
        def stamp(self) -> None:
            times.append(loop.time())

    stop = asyncio.Event()
    task = asyncio.create_task(r.writer.run(stop, TimedStamp()))
    await asyncio.sleep(1.0)
    stop.set()
    await asyncio.wait_for(task, 2.0)
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert len(times) >= 4 and max(gaps) < budget + 0.25


async def test_a_flush_of_ticks_or_forming_bars_only_sends_no_waitaof(md_redis: Any, run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids, aof=FakeAof(local=0))
    feed(r.hook, H0, 3)
    await r.at(H0 + 30_000)                                   # ticks and forming bars only
    assert await md_redis.xlen(K.ticks(AD, SYM)) == 3 and await md_redis.exists(K.forming(BID_1M))
    assert r.aof.calls == [] and r.writer.counters.aof_wait_timeouts == 0
    feed(r.hook, H0 + 35_000, 1)
    await r.at(H0 + 40_000)                                   # one tick plus a changed forming bar
    assert r.aof.calls == [] and r.writer.counters.aof_wait_timeouts == 0
    await r.at(H0 + MIN + 6_000)                              # the minute closes: closed bars wait for the disk
    assert r.aof.calls == [0] and r.writer.counters.aof_wait_timeouts == 1 and r.writer.queue_depth() == 2


async def test_a_price_the_codec_cannot_encode_never_stops_the_cycle(md_redis: Any, run_ids: RunIds,
                                                                     caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    bad = "1.1000000000001"                                   # 13 decimal places: valid quote, record cannot hold it
    r = await rig(md_redis, run_ids)
    r.hook.on_price(dto(bad, H0 + 7))                         # the open of every bucket starting at H0
    feed(r.hook, H0 + 10_000, 5)
    await r.at(H0 + 55_000)                                   # forming bars of H0 cannot be encoded
    assert await md_redis.xlen(K.ticks(AD, SYM)) == 6         # the symbol's ticks are still written
    assert not await md_redis.exists(K.forming(BID_1M)) and r.writer.counters.unencodable > 0
    counted = r.writer.counters.unencodable
    await r.at(H0 + 55_250)                                   # counted once per bucket, not every cycle
    assert r.writer.counters.unencodable == counted
    feed(r.hook, H0 + MIN, 6)
    await r.at(H0 + 2 * MIN + 6_000)                          # H0 closes (dropped: blind), H0 + 1 min is stored
    assert [b.bucket_start_ms for b in await stored_bars(md_redis, BID_1M)] == [H0 + MIN]
    assert r.writer.queue_depth() == 0 and r.writer.gapped == frozenset()
    assert await md_redis.xlen(K.ticks(AD, SYM)) == 12
    before = r.writer.counters.unencodable
    r.writer.queue_bar(bar(T0, bad, series=BROKER_15M))       # a BROKER bar is checked when it is queued
    assert r.writer.queue_depth(BROKER_15M) == 0 and r.writer.counters.unencodable == before + 1
    text = " ".join(rec.getMessage() for rec in caplog.records)
    assert bad not in text and "1.1000" not in text


@dataclass
class Replying:
    """A writer connection that answers every script with `reply`."""

    reply: ScriptReply
    aof: list[int] = field(default_factory=list)

    async def run(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]:
        return [self.reply for _ in calls]

    async def write_and_wait(self, calls: Sequence[S.ScriptCall]) -> WriteResult:
        self.aof.append(1)
        return WriteResult(tuple(self.reply for _ in calls), 1)


@pytest.mark.parametrize(("reply", "requeued"), [
    (ScriptFailed("ResponseError(ERR)"), False),             # a Lua error: unknown, possibly partial
    (ScriptOk([b"x"]), False),                               # the script ran but its reply is unusable
    (ScriptFailed("ResponseError(OOM)"), True),              # refused at the first write: nothing written
    (ScriptRefused("BAD_ARGS"), True),                       # a refusal writes nothing
])
async def test_ticks_are_resent_only_when_nothing_was_written(md_redis: Any, run_ids: RunIds, reply: ScriptReply,
                                                              requeued: bool) -> None:
    r = await rig(md_redis, run_ids, write=Replying(reply))
    feed(r.hook, H0, 12)
    await r.at(H0 + 2 * MIN + 6_000)
    assert r.writer.queue_depth() == 4 and {BID_1M, ASK_1M} <= r.writer.gapped     # bars always stay queued
    assert r.writer.pending_ticks((AD, SYM)) == (12 if requeued else 0)
    assert r.writer.counters.ticks_unconfirmed == (0 if requeued else 12)


class Flaky:
    """Delegates to the real client, or answers every script with an `OOM` refusal while `oom`."""

    def __init__(self, real: MdWriteClient) -> None:
        self.real = real
        self.oom = False

    async def run(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]:
        if self.oom:
            return [ScriptFailed("ResponseError(OOM)") for _ in calls]
        return await self.real.run(calls)

    async def write_and_wait(self, calls: Sequence[S.ScriptCall]) -> WriteResult:
        if self.oom:
            return WriteResult(tuple(ScriptFailed("ResponseError(OOM)") for _ in calls), 1)
        return await self.real.write_and_wait(calls)


async def test_the_oom_page_is_not_rearmed_by_alternating_success(md_redis: Any, run_ids: RunIds,
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    probe = await rig(md_redis, run_ids)
    flaky = Flaky(probe.writer._write)   # type: ignore[arg-type]  # the real client of the probe rig
    r = await rig(md_redis, run_ids, write=flaky, memory=Memory(0.95))
    await r.at(H0)                                            # reaches step 4
    assert r.writer.shed_level == 4
    now = H0
    for k in range(6):                                        # OOM, success, OOM, success, ...
        flaky.oom = k % 2 == 0
        r.writer.queue_bar(bar(T0 + k * 900_000, series=BROKER_15M))
        now += 250
        await r.at(now)
    oom_pages = [t for _, t, _ in r.pages.sent if "refuses writes" in t]
    assert oom_pages == ["redis-md refuses writes for memory"] and r.writer.counters.oom_refusals == 3
    monkeypatch.setattr(W, "OOM_REARM_FLUSHES", 2)
    flaky.oom = False
    for k in range(2):                                        # two clean flushes re-arm the page
        r.writer.queue_bar(bar(T0 + (10 + k) * 900_000, series=BROKER_15M))
        now += 250
        await r.at(now)
    flaky.oom = True
    r.writer.queue_bar(bar(T0 + 20 * 900_000, series=BROKER_15M))
    await r.at(now + 250)
    assert len([t for _, t, _ in r.pages.sent if "refuses writes" in t]) == 2


async def test_an_oom_below_the_last_step_reads_memory_at_once_and_pages_when_memory_is_unknown(
        md_redis: Any, run_ids: RunIds) -> None:
    oom = Replying(ScriptFailed("ResponseError(OOM)"))
    mem = Memory(0.50)
    r = await rig(md_redis, run_ids, write=oom, memory=mem)
    await r.at(H0)
    assert mem.reads == 1
    r.writer.queue_bar(bar(T0, series=BROKER_15M))
    await r.at(H0 + 250)                                      # OOM at step 0: the memory read runs in this cycle
    assert mem.reads == 2 and r.writer.counters.oom_refusals == 1
    assert not [t for _, t, _ in r.pages.sent if "refuses writes" in t]   # memory known and below step 4
    blind = await rig(md_redis, run_ids, write=oom)           # no memory port: the level is unknown
    blind.writer.queue_bar(bar(T0, series=BROKER_15M))
    await blind.at(H0)
    await blind.at(H0 + 250)
    pages = [b for _, t, b in blind.pages.sent if "refuses writes" in t]
    assert len(pages) == 1 and "unknown" in pages[0]


async def test_a_backward_wall_step_does_not_pause_memory_reads_or_retention(md_redis: Any,
                                                                            run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids, aof_off=True)
    mono = Clock(H0)
    mem = Memory(0.10)
    w = MdWriter(r.hook, r.writer._write, token=TOKEN, keyring=KEYRING,   # type: ignore[arg-type]
                 boot=SENTINEL, memory=mem, clock_ms=r.clock, mono_ms=mono)
    w.track_retention(series=[BID_1M])
    r.clock.now = H0
    await w.cycle()
    assert mem.reads == 1 and w.counters.retain_runs == 1
    r.clock.now, mono.now = H0 - 3_600_000, H0 + W.MEMORY_EVERY_MS          # the wall steps back an hour
    await w.cycle()
    assert w.counters.clock_steps == 1 and mem.reads == 2
    r.clock.now, mono.now = H0 - 3_600_000 + W.RETAIN_EVERY_MS, H0 + W.RETAIN_EVERY_MS + 10
    await w.cycle()
    assert w.counters.retain_runs == 2 and mem.reads == 3


async def test_retention_covers_series_stored_by_an_earlier_term(md_redis: Any, run_ids: RunIds,
                                                                 runner: ScriptRunner) -> None:
    tick_15m = SeriesId(AD, SYM, M15, Side.BID, Source.TICK)
    ticks = [S.TickWrite(H0 + k * 1_000, "1.1", "1.2", H0 + k * 1_000, "BROKER", "tradeable") for k in range(3)]
    closed = tuple(S.ClosedBarWrite(b, hash_of(b)) for b in (bar(H0 + k * 900_000, series=tick_15m)
                                                             for k in range(2)))
    (ok,) = await runner.run([S.flush_call(TOKEN, SENTINEL, AD, SYM, H0 + 3_600_000, ticks,
                                           [S.SeriesFlush(tick_15m, closed=closed)], rollback_n=8)])
    assert isinstance(ok, ScriptOk)
    r = await rig(md_redis, run_ids)
    r.writer.track_retention(series=[tick_15m, BROKER_15M], markets=[(AD, SYM)])
    later = H0 + 91 * W.DAY_MS                                # this term never wrote the symbol
    await r.at(later)
    assert r.writer.counters.retain_runs == 1 and await md_redis.xlen(K.ticks(AD, SYM)) == 0
    assert await md_redis.zcard(K.bars(tick_15m)) == 0
    _, floors = r.writer.retention_floors(later)
    assert BROKER_15M not in floors and tick_15m in floors


# ---------------------------------------------------------------------------------------------------- tick tap (S17b)
async def test_closed_tick_listeners_see_every_drained_tick_bar_and_a_raising_one_is_contained(
        md_redis: Any, run_ids: RunIds) -> None:
    r = await rig(md_redis, run_ids)
    seen: list[TickBar] = []

    def boom(tb: TickBar) -> None:
        raise RuntimeError("listener fault")

    r.writer.on_closed_tick(boom)
    r.writer.on_closed_tick(seen.append)
    feed(r.hook, H0, 12)
    await r.at(H0 + 2 * MIN + 6_000)
    one_minute = sorted((t.bar.series.side.value, t.bar.bucket_start_ms) for t in seen if t.bar.series.spec == M1)
    assert one_minute == [("ASK", H0), ("ASK", H0 + MIN), ("BID", H0), ("BID", H0 + MIN)]
    assert all(t.median_spread == Decimal("0.0002") for t in seen)
    assert r.writer.counters.tick_listener_errors == len(seen)
    assert [b.bucket_start_ms for b in await stored_bars(md_redis, BID_1M)] == [H0, H0 + MIN]   # capture went on


@pytest.mark.parametrize("failing", ["prices", "future"])
async def test_a_closed_tick_bar_failing_its_checks_never_reaches_a_tick_listener(md_redis: Any, run_ids: RunIds,
                                                                                  failing: str) -> None:
    """Zero trust (D6): the TICK bar that judges a BROKER bar must itself pass the hard checks."""
    clock = Clock(H0)
    if failing == "prices":                                  # high below the open
        bad = Bar(series=BID_1M, bucket_start_ms=H0, open=Decimal("1.3"), high=Decimal("1.2"), low=Decimal("1.1"),
                  close=Decimal("1.15"), tick_count=3, quality=Quality.COMPLETE, final_at_ms=H0 + MIN)
    else:                                                    # a bucket that has not ended yet
        bad = Bar(series=BID_1M, bucket_start_ms=H0 + 60 * MIN, open=Decimal("1.1"), high=Decimal("1.2"),
                  low=Decimal("1.0"), close=Decimal("1.15"), tick_count=3, quality=Quality.COMPLETE,
                  final_at_ms=H0 + 61 * MIN)
    h = BadBarHook(clock_ms=clock, bad=bad)
    h.on_price(dto("1.10000", H0 - 5_000))
    r = await rig(md_redis, run_ids, hook=h, start=H0)
    seen: list[TickBar] = []
    r.writer.on_closed_tick(seen.append)
    feed(r.hook, H0, 6)
    await r.at(H0 + 2 * MIN)
    assert r.writer.counters.quarantined == 1
    assert seen and bad not in [t.bar for t in seen]         # the valid closed bars still reach the book
