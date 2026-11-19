"""The `redis-md` load test (plan S38; design §A5 sizing, §C3 "a sizing load test" and "recompute with 8 symbols
keeps event-loop lag under 100 ms"). Marker `md`: skipped unless ORDER_SERVER_TEST_REDIS_MD names the DISPOSABLE
dev `redis-md` of `deploy/docker-compose.dev.yml` (profile `md`, e.g. `redis://127.0.0.1:56383/0`, no user in the
URL); the writer connects as `md_writer` with the throwaway dev-only password read from that compose file, as
`tests/marketdata/test_acl_live.py` does.

* `test_full_retention_sizing_of_one_symbol`: one synthetic symbol filled to the v1 retention of §A5 through the
  real scripts (`FLUSH` for bars and ticks, `IND` for indicator values, both pipelined): BROKER 15m bid + ask for a
  year, TICK 15m bid + ask for 90 days, TICK 1m bid + ask for 7 days, ATR(14) on both BROKER sides for the year and
  the tick stream at its cap. `INFO memory` before and after gives the bytes per symbol; the projection for 8
  symbols plus `md:events` must sit near half of the 1 GB `maxmemory` (design: about 500 MB). The figures are
  printed (`-s`) and recorded in design §A5.
* `test_recompute_for_8_symbols_keeps_event_loop_lag_under_100_ms`: 8 symbols × bid / ask with 7 days of BROKER
  15m bars; the real `md-indicators` (`MdIndicators`) registers ATR(14) on all 16 series, then 96 bar closes (one
  trading day) are driven on all 16 series, every 8th close also revising the bar 10 buckets back on every series,
  each followed by the engine's cycles, reading through `MdPort` and writing through `IND`, while a probe on the
  same event loop measures how late a 5 ms sleep wakes: the worst lag stays under 100 ms.

Every key the tests write is deleted afterwards, the store-wide ones too (`md:writer`, `md:boot`, `md:events`, the
registries: `STORE_GLOBAL_KEYS`), so the `md` marker tests can run in any order on one dev `redis-md` (`md_writer`
may `DEL` `md:*`; it may not `SCAN`, `KEYS` or `FLUSHDB`). Synthetic instruments and prices only; no price appears in
an assertion message.
"""
from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

import pytest
from redis.asyncio import Redis

from src.marketdata.bootstrap import Sm12Event
from src.marketdata.hashing import content_hash
from src.marketdata.indicators.base import IndicatorValue, Lineage, encode_value
from src.marketdata.indicators.engine import IndicatorSettings, MdIndicators
from src.marketdata.indicators.registry import REGISTRY
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.runner import ScriptOk
from src.marketdata.types import M1, M15, Bar, BarEndEvent, Quality, SeriesId, Side, Source

from .bootrig import Rig, make_rig
from .conftest import KEYRING, STORE_GLOBAL_KEYS, RunIds
from .indicators.helpers import walk

URL = os.environ.get("ORDER_SERVER_TEST_REDIS_MD")
pytestmark = [pytest.mark.md, pytest.mark.skipif(not URL, reason="ORDER_SERVER_TEST_REDIS_MD not set")]

ADAPTER = "mock"
DAY = 24 * 3600 * 1000
H2 = 2 * 3600 * 1000
TOKEN = K.WriterToken("4242424242", 9, "loadnode", "l0ad")
INCREMENT = Decimal("0.00001")
BATCH = 200                         # closed bars per FLUSH (one series)
IND_BATCH = 500                     # IND calls per pipeline
TICK_BATCH = 2_000                  # ticks per FLUSH
TICKS_CAP = 200_000                 # the stream's MAXLEN (design §A5)
LAG_LIMIT_MS = 100
DAYS_LIVE = 1                        # live bar closes driven through the engine (96 per series per day)
MB = 1024 * 1024
# The epoch end of the fill: a 15m bucket boundary aligned to a day (bars end at or before it).
END = 1_700_006_400_000 - 1_700_006_400_000 % DAY


def _md_url(user: str) -> str:
    from .test_acl_live import _url
    return _url(user)


@dataclass
class Store:
    """The real `redis-md` as `md_writer` with the S16 rig's writer and bootstrap on it, and every key written."""

    redis: Redis
    rig: Rig
    written: set[str] = field(default_factory=set)

    async def used_memory(self) -> int:
        info = await self.redis.info("memory")
        return int(info["used_memory"])

    async def run(self, calls: Sequence[S.ScriptCall]) -> None:
        for call in calls:
            self.written.update(call.keys)
        for i in range(0, len(calls), IND_BATCH):
            replies = await self.rig.client.run(calls[i:i + IND_BATCH])
            refused = [getattr(r, "code", type(r).__name__) for r in replies if not isinstance(r, ScriptOk)]
            assert not refused, f"a load write was refused: {sorted(set(map(str, refused)))}"

    async def members(self, key: str) -> int:
        return int(await self.redis.zcard(key))


@pytest.fixture
async def store() -> AsyncIterator[Store]:
    redis = Redis.from_url(_md_url("md_writer"))
    info = await redis.info("server")
    rig = await make_rig(redis, RunIds(str(info["run_id"])), start=END, token=TOKEN)
    await rig.at(END)                                         # bootstrap: token, md:boot
    assert rig.writer.boot is not None, "the load rig did not boot"
    s = Store(redis, rig)
    try:
        yield s
    finally:
        # every key written, the store-wide ones too (md:writer at epoch 9, md:boot, md:events, the registries):
        # the next `md` test (the e2e one boots with an epoch-7 token) finds an unbooted store, whatever the order
        keys = sorted({k for k in s.written if k.startswith(K.PREFIX)} | set(STORE_GLOBAL_KEYS))
        for i in range(0, len(keys), 500):
            await redis.delete(*keys[i:i + 500])
        await redis.aclose()


def series(symbol: str, spec: Any, side: Side, source: Source) -> SeriesId:
    return SeriesId(ADAPTER, symbol, spec, side, source)


def bars_for(s: SeriesId, days: int, *, seed: int) -> list[Bar]:
    tf = s.spec.timeframe_ms
    n = days * DAY // tf
    out = walk(n, seed=seed, series=s, start=END - n * tf)
    return [replace(b, final_at_ms=b.bucket_end_ms + 1_000) for b in out]


def closed(b: Bar) -> S.ClosedBarWrite:
    return S.ClosedBarWrite(b, content_hash(KEYRING, b.series.canonical, b.bucket_end_ms, b.open, b.high, b.low,
                                            b.close))


def bar_calls(st: Store, bars: Sequence[Bar]) -> list[S.ScriptCall]:
    boot = st.rig.writer.boot
    assert boot is not None
    s = bars[0].series
    return [S.flush_call(TOKEN, boot, s.adapter_id, s.symbol, END, (),
                         [S.SeriesFlush(s, closed=tuple(closed(b) for b in bars[i:i + BATCH]))], rollback_n=8)
            for i in range(0, len(bars), BATCH)]


def tick_calls(st: Store, symbol: str, n: int) -> list[S.ScriptCall]:
    boot = st.rig.writer.boot
    assert boot is not None
    step = H2 // n
    ticks = [S.TickWrite(END - H2 + (i + 1) * step, "1.10010", "1.10020", END - H2 + (i + 1) * step - 30, "BROKER",
                         "TRADEABLE") for i in range(n)]
    return [S.flush_call(TOKEN, boot, ADAPTER, symbol, END, ticks[i:i + TICK_BATCH], [], rollback_n=8)
            for i in range(0, n, TICK_BATCH)]


def atr_calls(st: Store, bars: Sequence[Bar]) -> tuple[str, list[S.ScriptCall]]:
    """One `IND` call per bucket of ATR(14) over `bars` (the engine's member encoding; synthetic values)."""
    boot = st.rig.writer.boot
    assert boot is not None
    spec = REGISTRY.spec("ATR", 1, bars[0].series, {"length": "14"}, INCREMENT)
    iid = spec.indicator_id
    calls = []
    for k, b in enumerate(bars):
        value = IndicatorValue(indicator_id=iid, output="value", bucket_start_ms=b.bucket_start_ms,
                               value=Decimal("0.00123") + INCREMENT * (k % 50), quality=Quality.COMPLETE, warm=k >= 14,
                               final_at_ms=b.final_at_ms, lineage=Lineage(iid, b.bucket_start_ms - 13 * M15.timeframe_ms,
                                                                       "0123456789abcdef", 0))
        w = S.IndicatorWrite(iid, b.bucket_start_ms, b.bucket_end_ms, (("value", encode_value(value)),),
                             definition=spec.canonical if k == 0 else "", emit=False)
        calls.append(S.indicator_call(TOKEN, boot, w, value_floor_ms=0, checkpoint_floor_ms=0))
    return iid, calls


# ---------------------------------------------------------------------------------------------------- sizing
async def test_full_retention_sizing_of_one_symbol(store: Store) -> None:
    symbol = "LOADSIZE"
    before = await store.used_memory()
    started = time.monotonic()
    plan = [(series(symbol, M15, side, Source.BROKER), 365) for side in (Side.BID, Side.ASK)]
    plan += [(series(symbol, M15, side, Source.TICK), 90) for side in (Side.BID, Side.ASK)]
    plan += [(series(symbol, M1, side, Source.TICK), 7) for side in (Side.BID, Side.ASK)]
    counts: dict[str, int] = {}
    ind_ids: list[str] = []
    for i, (s, days) in enumerate(plan):
        bars = bars_for(s, days, seed=11 + i)
        await store.run(bar_calls(store, bars))
        counts[s.canonical] = await store.members(K.bars(s))
        assert counts[s.canonical] == len(bars), "a series was not filled to its retention"
        if s.source is Source.BROKER:
            iid, calls = atr_calls(store, bars)
            await store.run(calls)
            ind_ids.append(iid)
    await store.run(tick_calls(store, symbol, TICKS_CAP))
    ticks = int(await store.redis.xlen(K.ticks(ADAPTER, symbol)))
    after = await store.used_memory()
    ind_members = sum([await store.members(K.indicator(iid, "value")) for iid in ind_ids])
    bar_members = sum(counts.values())
    per_symbol = after - before
    projected = 8 * per_symbol + 15 * MB          # + md:events at its cap (design §A5)
    print(f"\nS38 sizing: {bar_members} bar members, {ind_members} indicator members, {ticks} ticks; "
          f"{per_symbol / MB:.1f} MB per symbol ({per_symbol / (bar_members + ind_members):.0f} B per member incl. "
          f"the tick stream); 8 symbols + md:events about {projected / MB:.0f} MB; fill took "
          f"{time.monotonic() - started:.0f} s")
    assert bar_members == 2 * (35_040 + 8_640 + 10_080) and ind_members == 2 * 35_040
    assert ticks >= TICKS_CAP * 0.9
    assert per_symbol <= 75 * MB, "one symbol at full retention is larger than the design's sizing allows"
    assert projected <= 640 * MB, "8 symbols at full retention would not sit near half of maxmemory 1gb"


# ---------------------------------------------------------------------------------------------------- recompute lag
class LagProbe:
    """How late a 5 ms sleep wakes on this event loop (the worst of every sample while it runs)."""

    def __init__(self) -> None:
        self.worst_ms = 0.0
        self.samples = 0
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            t = time.perf_counter()
            await asyncio.sleep(0.005)
            self.worst_ms = max(self.worst_ms, (time.perf_counter() - t) * 1000 - 5)
            self.samples += 1

    def stop(self) -> None:
        self._stop.set()


async def test_recompute_for_8_symbols_keeps_event_loop_lag_under_100_ms(store: Store) -> None:
    rig = store.rig
    symbols = [f"LOAD{i}" for i in range(1, 9)]
    all_series = [series(sym, M15, side, Source.BROKER) for sym in symbols for side in (Side.BID, Side.ASK)]
    history: dict[SeriesId, list[Bar]] = {}
    for i, s in enumerate(all_series):
        history[s] = bars_for(s, 7, seed=101 + i)
        await store.run(bar_calls(store, history[s]))
        rig.book.apply(s, Sm12Event.LIVE)
    rig.clock.now = END + 60_000
    engine = MdIndicators(rig.port, write=rig.client, token=lambda: rig.writer.token, boot=lambda: rig.writer.boot,
                          settings=IndicatorSettings(retry_ms=0), clock_ms=rig.clock)
    specs = [REGISTRY.spec("ATR", 1, s, {"length": "14"}, INCREMENT) for s in all_series]
    for spec in specs:
        for o in ("value",):
            store.written.add(K.indicator(spec.indicator_id, o))
        store.written.add(K.indicator_checkpoints(spec.indicator_id))
    probe = LagProbe()
    probe_task = asyncio.create_task(probe.run())
    started = time.monotonic()
    tf = M15.timeframe_ms
    try:
        for spec in specs:
            status = await engine.register(spec)
            assert status is not None, "ATR(14) was refused"
        written_at_register = engine.counters.values_written
        # a trading day of bar closes on all 16 series at once, each followed by the engine's cycle; every 8th
        # close also revises a bar 10 buckets back on every series (a recompute of that range)
        for k in range(1, DAYS_LIVE * 96 + 1):
            end = END + k * tf
            rig.clock.now = end + 2_000
            for s in all_series:
                nxt = replace(history[s][-1], bucket_start_ms=end - tf, final_at_ms=end + 1_000)
                history[s].append(nxt)
                await store.run(bar_calls(store, [nxt]))
                engine.on_released(BarEndEvent(s, end, "h", 1, 0, False))
                if k % 8 == 0:
                    old = history[s][-11]
                    revised = replace(old, close=old.close + INCREMENT, high=max(old.high, old.close + INCREMENT),
                                      revision=1)
                    await store.run(bar_calls(store, [revised]))
                    engine.on_released(BarEndEvent(s, old.bucket_end_ms, "h", 1, 1, True))
            for _ in range(20):
                await engine.cycle()
                if all(engine._inds[sp.indicator_id].job is None for sp in specs):
                    break
    finally:
        probe.stop()
        await probe_task
    recomputed = engine.counters.values_written
    print(f"\nS38 recompute: 16 ATR(14) series, 7 days stored then {DAYS_LIVE * 96} closes each (every 8th with a "
          f"revision 10 buckets back); {written_at_register} values at registration, {recomputed} written in all, "
          f"{engine.counters.values_unchanged} unchanged; worst event-loop lag {probe.worst_ms:.1f} ms over "
          f"{probe.samples} samples; {time.monotonic() - started:.1f} s")
    assert probe.samples > 10
    assert all(engine._inds[sp.indicator_id].job is None for sp in specs), "a recompute did not finish"
    assert engine.counters.write_errors == 0 and recomputed >= 16 * DAYS_LIVE * 96
    assert probe.worst_ms < LAG_LIMIT_MS, "the recompute blocked the event loop"
