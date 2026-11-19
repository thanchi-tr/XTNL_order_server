"""Plan S38e (go-live hardening, operations): the market-data side. BROKER retention (`RETAIN_BROKER`, every stored
series, demanded or not), the measured memory ceiling of the demand book, md-bars' D7 feeds (`note_unresolved` /
`note_final` / `note_no_trade` and `refetch_failed`), the series clear probes (`probe_reread`, `probe_fresh_bar`), the
cross-check's record-nothing `check_only`, the fault relay's bucket notes, and the indicator engine refusing
multi-input (DAG) indicators. Synthetic instruments and prices only; no price appears in an assertion message."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest

from src.calendar.machine import SessionState
from src.marketdata.bootstrap import SeriesStateBook
from src.marketdata.brokerbars import BrokerBarsSettings, Coverage, MdBars, demanded
from src.marketdata.crosscheck import CrossChecker, TickRef, Verdict
from src.marketdata.demand import DemandBook, DemandSettings
from src.marketdata.events import RefetchQueue
from src.marketdata.indicators.atr import ATR_V1
from src.marketdata.indicators.engine import IndicatorSettings, MdIndicators
from src.marketdata.indicators.registry import IndicatorRegistry
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.runner import ScriptOk, ScriptRefused, ScriptRunner
from src.marketdata.types import M15, Bar, LeaseClass, MdGeneration, Quality, SeriesId, Side, Source
from src.marketdata import writer as W
from src.runtime.optional_md import FaultRelay

from .conftest import KEYRING, RUN_ID, SENTINEL, SERIES, TOKEN, RunIds, bar, hash_of
from .indicators.helpers import INC, FakeBars
from .indicators.test_indicator_types import Smooth
from .test_brokerbars import AD, B, S1, TF, T, Broker, Calendar, Clock, Health, Pages, Sink, Stored
from .test_writer import H0, rig

DAY = 86_400_000


# ---------------------------------------------------------------------------------------------------- RETAIN_BROKER
async def _seed_broker(runner: ScriptRunner, starts: list[int]) -> None:
    closed = tuple(S.ClosedBarWrite(b, hash_of(b)) for b in (bar(s) for s in starts))
    (ok,) = await runner.run([S.flush_call(TOKEN, SENTINEL, SERIES.adapter_id, SERIES.symbol, max(starts) + TF, [],
                                           [S.SeriesFlush(SERIES, closed=closed)], rollback_n=8)])
    assert isinstance(ok, ScriptOk)


async def test_retain_broker_trims_bars_revisions_and_quarantine_and_moves_first_bucket(
        md_redis: Any, runner: ScriptRunner) -> None:
    starts = [H0, H0 + TF, H0 + 2 * TF]
    await _seed_broker(runner, starts)
    await md_redis.zadd(K.bars_rev(SERIES), {"old-rev": H0, "new-rev": H0 + 9 * DAY})
    await md_redis.zadd(K.quarantine(SERIES), {"old-q": H0, "new-q": H0 + 9 * DAY})
    rb_before = await md_redis.zrange(K.rollback(SERIES), 0, -1)
    events_before = await md_redis.xlen(K.EVENTS)
    call = S.retain_broker_call(TOKEN, SENTINEL, H0 + 10 * DAY, [(SERIES, H0 + TF)])
    (reply,) = await runner.run([call])
    assert isinstance(reply, ScriptOk)
    assert S.parse_retain_broker_reply(reply.value).removed == ((1, 1, 1),)
    assert [s for _, s in await md_redis.zrange(K.bars(SERIES), 0, -1, withscores=True)] == [H0 + TF, H0 + 2 * TF]
    assert await md_redis.hget(K.meta(SERIES), "first_bucket") == str(H0 + TF).encode()
    assert await md_redis.zrange(K.bars_rev(SERIES), 0, -1) == [b"new-rev"]
    assert await md_redis.zrange(K.quarantine(SERIES), 0, -1) == [b"new-q"]
    assert await md_redis.zrange(K.rollback(SERIES), 0, -1) == rb_before          # never the rollback set
    assert await md_redis.xlen(K.EVENTS) == events_before                       # no event


async def test_retain_broker_is_guarded_checks_before_writing_and_refuses_tick_series(
        md_redis: Any, runner: ScriptRunner) -> None:
    await _seed_broker(runner, [H0])
    stale = K.BootSentinel(RUN_ID, SENTINEL.cluster_id, SENTINEL.node, MdGeneration(7, 0))
    assert await runner.run([S.retain_broker_call(TOKEN, stale, H0 + DAY, [(SERIES, H0 + TF)])]) == \
        [ScriptRefused("NO_BOOT")]
    await md_redis.hset(K.meta(SERIES), "first_bucket", "corrupt")
    assert await runner.run([S.retain_broker_call(TOKEN, SENTINEL, H0 + DAY, [(SERIES, H0 + TF)])]) == \
        [ScriptRefused("BAD_ARGS")]
    assert await md_redis.zcard(K.bars(SERIES)) == 1                            # nothing trimmed before it
    with pytest.raises(ValueError):
        S.retain_broker_call(TOKEN, SENTINEL, H0, [(SeriesId(AD, "SYN1", M15, Side.BID, Source.TICK), 0)])
    with pytest.raises(ValueError):
        S.retain_broker_call(TOKEN, SENTINEL, H0, [])


async def test_writer_trims_every_stored_broker_series_to_one_year_demanded_or_not(
        md_redis: Any, run_ids: RunIds, runner: ScriptRunner) -> None:
    """A BROKER series an earlier term stored (handed over by the bootstrap's `track_retention`) is trimmed below
    1 year, though this term never writes or demands it: the §A5 sizing is a bound, not an estimate."""
    await _seed_broker(runner, [H0, H0 + 2 * DAY])
    r = await rig(md_redis, run_ids, aof_off=True)
    r.writer.track_retention(series=[SERIES])
    assert SERIES in r.writer.broker_series() and (SERIES.adapter_id, SERIES.symbol) in r.writer.stored_broker_markets()
    later = H0 + W.BROKER_RETENTION_MS + DAY                                    # floor: H0 + 1 day
    assert r.writer.broker_floor(SERIES, later) == SERIES.spec.bucket_start_ms(later - W.BROKER_RETENTION_MS)
    await r.at(later)
    assert r.writer.counters.broker_retain_runs == 1 and r.writer.counters.broker_bars_trimmed == 1
    assert [s for _, s in await md_redis.zrange(K.bars(SERIES), 0, -1, withscores=True)] == [H0 + 2 * DAY]


async def test_a_wider_declared_backfill_keeps_more_than_a_year(md_redis: Any, run_ids: RunIds,
                                                                runner: ScriptRunner) -> None:
    await _seed_broker(runner, [H0])
    r = await rig(md_redis, run_ids, aof_off=True, broker_keep_ms=lambda s: 400 * DAY)
    r.writer.track_retention(series=[SERIES])
    await r.at(H0 + 380 * DAY)
    assert r.writer.counters.broker_retain_runs == 1 and r.writer.counters.broker_bars_trimmed == 0
    assert await md_redis.zcard(K.bars(SERIES)) == 1
    raising = await rig(md_redis, run_ids, aof_off=True, broker_keep_ms=lambda s: 1 // 0)
    assert raising.writer.broker_floor(SERIES, H0 + 380 * DAY) == \
        SERIES.spec.bucket_start_ms(H0 + 380 * DAY - W.BROKER_RETENTION_MS)     # a failing source keeps 1 year


# ---------------------------------------------------------------------------------------------------- demand ceiling
def test_the_memory_ceiling_counts_what_redis_md_actually_holds() -> None:
    ratio: list[float | None] = [None]
    b = DemandBook(settings=DemandSettings(), memory_ratio=lambda: ratio[0])
    assert b.memory_symbols(3) == 8                       # unknown: the §A5 formula alone
    ratio[0] = 0.10
    assert b.memory_symbols(3) == 8                       # room for 6 more, the formula binds
    ratio[0] = 0.45
    assert b.memory_symbols(3) == 3                       # history piled up: no new symbol
    ratio[0] = 0.60
    assert b.memory_symbols(3) == 3                       # above the target: never below what is admitted
    full = DemandBook(settings=DemandSettings(decision_symbols_per_adapter=8), memory_ratio=lambda: 0.49)
    sid = full.define(SeriesId(AD, "SYN1", M15, Side.BID, Source.BROKER))
    got = full.admit(sid, "arm", LeaseClass.ARMED)
    assert not got.admitted and got.reason == "ceiling"


# ---------------------------------------------------------------------------------------------------- md-bars D7 feeds
@dataclass
class Notes:
    seen: list[tuple[Any, ...]] = field(default_factory=list)

    def note_unresolved(self, series: SeriesId, bucket_ms: int, cap_ms: int, now_ms: int) -> None:
        self.seen.append(("unresolved", series, bucket_ms, cap_ms, now_ms))

    def note_final(self, series: SeriesId, bucket_ms: int) -> None:
        self.seen.append(("final", series, bucket_ms))

    def note_no_trade(self, series: SeriesId, bucket_ms: int) -> None:
        self.seen.append(("no_trade", series, bucket_ms))


@dataclass
class BarsRig:
    bars: MdBars
    broker: Broker
    clock: Clock
    notes: Notes
    faults: list[tuple[SeriesId, str]]

    async def run_to(self, until: int, every: int = 1_000) -> None:
        while self.clock.now <= until:
            await self.bars.step()
            self.clock.now += every


def bars_rig(*, start: int = T - 60_000, refetch: RefetchQueue | None = None, health: Health | None = None,
             calendar: Calendar | None = None, crosscheck: Any = None, trusts: bool = False,
             **cfg: Any) -> BarsRig:
    clock = Clock(start)
    broker = Broker(clock)
    broker.fill(T - 40 * TF, T + 20 * TF)
    sink = Sink()
    notes = Notes()
    faults: list[tuple[SeriesId, str]] = []
    if health is not None:
        health.clock = clock

    async def sleep(s: float) -> None:
        clock.now += int(s * 1000)

    settings = BrokerBarsSettings(**({"lane_retry_min_ms": 1_000, "lane_retry_max_ms": 4_000} | cfg))
    bars = MdBars(broker, sink, SeriesStateBook(), keyring=KEYRING, demand=demanded([(S1, 4)]), settings=settings,
                  stored=Stored(sink), health=health, calendar=calendar, pager=Pages(), refetch=refetch,
                  trusts_partial_flag=lambda a: trusts, trusts_next_bucket=lambda a: True,
                  on_fault=lambda s, why: faults.append((s, why)), crosscheck=crosscheck, buckets=notes,
                  sleep=sleep, clock_ms=clock)
    return BarsRig(bars, broker, clock, notes, faults)


async def test_a_bucket_unresolved_at_its_cap_is_noted_to_the_error_budget() -> None:
    rig_ = bars_rig()
    rig_.broker.hidden.add(B)
    await rig_.run_to(T + 905_000, every=5_000)
    unresolved = [n for n in rig_.notes.seen if n[0] == "unresolved"]
    assert len(unresolved) == 1 and unresolved[0][1:3] == (S1, B)
    assert unresolved[0][3] <= unresolved[0][4]                       # noted at (after) its decision_cap


async def test_a_clean_final_bar_and_a_no_trade_bucket_are_noted() -> None:
    rig_ = bars_rig()
    await rig_.run_to(T + 5_000)
    assert ("final", S1, B) in rig_.notes.seen
    empty = bars_rig()
    empty.broker.bars.pop(("SYN1", B))
    await empty.run_to(T + 905_000, every=5_000)                       # still empty at its cap: no_trade
    assert ("no_trade", S1, B) in empty.notes.seen
    assert not any(n[0] == "unresolved" for n in empty.notes.seen)


async def test_a_refetch_beyond_broker_reach_is_refetch_failed_once() -> None:
    """D7 (S38e): a rollback needed beyond the rollback set whose StoneX re-fetch fails is a SERIES fault."""
    queue = RefetchQueue()
    rig_ = bars_rig(refetch=queue, history_reach_ms=10 * TF)
    await rig_.run_to(T - 58_000)
    assert queue.request(S1, B - 30 * TF, B - 29 * TF) is None
    await rig_.run_to(T - 55_000)
    assert rig_.faults == [(S1, "refetch_failed")]
    assert rig_.bars.counters.refetch_failed == 2                     # two buckets, one fault per series episode


async def test_a_refetch_that_ends_unfillable_in_the_lane_is_refetch_failed() -> None:
    queue = RefetchQueue()
    rig_ = bars_rig(refetch=queue, lane_max_misses=2)
    await rig_.run_to(T - 58_000)
    lost = B - 25 * TF
    rig_.broker.bars.pop(("SYN1", lost))
    rig_.broker.hidden.add(lost)
    assert queue.request(S1, lost, lost) is None
    await rig_.run_to(T - 40_000)
    assert rig_.bars.coverage(S1).get(lost) is Coverage.UNFILLABLE
    assert rig_.faults == [(S1, "refetch_failed")]


async def test_a_lane_bucket_unfillable_without_a_refetch_is_no_fault() -> None:
    rig_ = bars_rig(lane_max_misses=2)
    rig_.broker.hidden.add(B)
    await rig_.run_to(T + 960_000, every=5_000)
    assert rig_.bars.coverage(S1).get(B) is Coverage.UNFILLABLE
    assert all(why != "refetch_failed" for _, why in rig_.faults)


# ---------------------------------------------------------------------------------------------------- series probes
async def test_probe_reread_passes_on_clean_bars_and_names_the_failing_check() -> None:
    rig_ = bars_rig(start=T + 30_000)
    assert await rig_.bars.probe_reread(S1) is None
    reads = rig_.bars.counters.probe_reads
    assert reads == 1 and rig_.bars.counters.reads == 0               # never the live path's counters
    rig_.broker.put(B - TF, high="1.0980")                            # high below low: fails a hard check
    assert await rig_.bars.probe_reread(S1) == "invalid_bar"
    rig_.broker.fail = lambda sym, lo, hi: TimeoutError()
    assert await rig_.bars.probe_reread(S1) == "read_failed"
    rig_.broker.fail = None
    for start in range(T - 40 * TF, T + 20 * TF, TF):
        rig_.broker.bars.pop(("SYN1", start), None)
    assert await rig_.bars.probe_reread(S1) == "no_bars"
    assert await rig_.bars.probe_reread(SeriesId(AD, "SYN1", M15, Side.BID, Source.TICK)) == "not_broker"
    down = bars_rig(start=T + 30_000, health=Health(streaming=False))
    assert await down.bars.probe_reread(S1) == "broker_unavailable"
    assert rig_.faults == [] and rig_.notes.seen == []                # a probe never counts or faults


@dataclass
class Mismatch:
    checked: list[int] = field(default_factory=list)

    def check_only(self, bar_: Bar) -> Any:
        self.checked.append(bar_.bucket_start_ms)
        return type("R", (), {"verdict": Verdict.MISMATCH})()


async def test_probe_reread_refuses_a_tick_cross_check_mismatch() -> None:
    xc = Mismatch()
    rig_ = bars_rig(start=T + 30_000, crosscheck=xc)
    assert await rig_.bars.probe_reread(S1) == "source_mismatch" and xc.checked


async def test_probe_reread_reads_the_configured_rollback_set() -> None:
    rig_ = bars_rig(start=T + 30_000, rollback_n=3)
    assert await rig_.bars.probe_reread(S1) is None
    (_, _, lo, hi), = rig_.broker.calls
    assert hi - lo == 3 * TF                                          # 3 closed buckets plus the one in progress
    wide = bars_rig(start=T + 30_000, rollback_n=20)
    assert await wide.bars.probe_reread(S1) is None
    (_, _, lo, hi), = wide.broker.calls
    assert hi - lo == 20 * TF


FRESH_END = T + TF
"""The next bar end after a probe started at `T + 30 s`: the probe reads bucket `T` (bar end `T + TF`)."""


async def test_probe_fresh_bar_needs_two_equal_reads_settle_apart_within_post_grace() -> None:
    rig_ = bars_rig(start=T + 30_000)
    assert await rig_.bars.probe_fresh_bar(S1) is None
    first_read = FRESH_END + 5_000                                    # T' + p50 + settle (cold skew)
    assert rig_.clock.now == first_read + rig_.bars._cfg.settle_ms    # two reads, one settle apart
    assert {c[2] for c in rig_.broker.calls} == {T}                   # only the fresh bucket, read after T'
    assert min(c[0] for c in rig_.broker.calls) == first_read
    flip = bars_rig(start=T + 30_000)
    n = [0]

    def changing(sym: str, lo: int, hi: int) -> object:
        n[0] += 1
        flip.broker.put(lo, close="1.1007" if n[0] % 2 else "1.1005")
        return None

    flip.broker.fail = changing
    assert await flip.bars.probe_fresh_bar(S1) == "not_final"
    assert flip.clock.now - FRESH_END <= 60_000                       # bounded by T' + post_grace
    empty = bars_rig(start=T + 30_000)
    empty.broker.hidden.add(T)
    assert await empty.bars.probe_fresh_bar(S1) == "no_bar"
    closed = bars_rig(start=T + 30_000, calendar=Calendar(SessionState.CLOSED))
    assert await closed.bars.probe_fresh_bar(S1) == "market_closed"
    trusted = bars_rig(start=T + 30_000, trusts=True)
    assert await trusted.bars.probe_fresh_bar(S1) is None and trusted.clock.now == FRESH_END + 5_000


async def test_probe_fresh_bar_refuses_when_only_an_older_closed_bar_is_final() -> None:
    """The recorded cause is `unresolved` (bars not final by their cap): a bucket already final when the probe
    starts proves nothing. The newest closed bar (B) is final; the next bar (T) does not settle within post_grace."""
    rig_ = bars_rig(start=T + 30_000)
    n = [0]

    def unsettled(sym: str, lo: int, hi: int) -> object:
        if lo == T:
            n[0] += 1
            rig_.broker.put(T, close="1.1007" if n[0] % 2 else "1.1005")
        return None

    rig_.broker.fail = unsettled
    assert await rig_.bars.probe_fresh_bar(S1) == "not_final"
    assert all(c[2] == T for c in rig_.broker.calls)                  # B, final long ago, is never the evidence
    late = bars_rig(start=T + 30_000)

    def settles_late(sym: str, lo: int, hi: int) -> object:
        if late.clock.now < FRESH_END + 50_000:                       # final only after T' + 50 s
            late.broker.put(T, close="1.1007" if (late.clock.now // 10_000) % 2 else "1.1005")
        else:
            late.broker.put(T, close="1.1001")
        return None

    late.broker.fail = settles_late
    assert await late.bars.probe_fresh_bar(S1) == "not_final"         # the second equal read would be past post_grace


def test_check_only_records_nothing() -> None:
    tick = SeriesId(AD, "SYN1", M15, Side.BID, Source.TICK)
    ref_bar = Bar(series=tick, bucket_start_ms=B, open=Decimal("1.2000"), high=Decimal("1.2100"),
                  low=Decimal("1.1900"), close=Decimal("1.2050"), tick_count=40, quality=Quality.COMPLETE,
                  final_at_ms=T + 1_000)
    xc = CrossChecker(type("Ticks", (), {"get": lambda self, s, b: TickRef(ref_bar, True, Decimal("0.0002"))
                                         if b == B else None})(), mismatch_k=2)
    broker_bar = Bar(series=S1, bucket_start_ms=B, open=Decimal("1.1000"), high=Decimal("1.1010"),
                     low=Decimal("1.0990"), close=Decimal("1.1005"), quality=Quality.COMPLETE, final_at_ms=T + 1_000)
    assert xc.check_only(broker_bar).verdict is Verdict.MISMATCH
    assert xc.result(S1, B) is None and xc.mismatch_streak(S1) == 0 and xc.counters.checks == 0

    def broken(self: object, s: SeriesId, b: int) -> TickRef:
        raise ValueError("malformed reference")

    raising = CrossChecker(type("Ticks", (), {"get": broken})(), mismatch_k=2)
    assert raising.check_only(broker_bar).verdict is Verdict.SKIPPED
    assert raising.counters.errors == 0 and raising.counters.checks == 0          # the exception path records nothing


# ---------------------------------------------------------------------------------------------------- the relay
@dataclass
class Budget:
    seen: list[tuple[Any, ...]] = field(default_factory=list)

    def series_fault(self, series: SeriesId, reason: str, now_ms: int) -> object:
        self.seen.append(("fault", reason))
        return None

    def store_fault(self, reason: str, now_ms: int) -> None:
        self.seen.append(("store", reason))

    def indicator_fault(self, indicator_id: str, series: SeriesId, reason: str, now_ms: int) -> None:
        self.seen.append(("indicator", reason))

    def note_unresolved(self, series: SeriesId, bucket_ms: int, cap_ms: int, now_ms: int) -> None:
        self.seen.append(("unresolved", bucket_ms))

    def note_final(self, series: SeriesId, bucket_ms: int) -> None:
        self.seen.append(("final", bucket_ms))

    def note_no_trade(self, series: SeriesId, bucket_ms: int) -> None:
        self.seen.append(("no_trade", bucket_ms))


def test_the_fault_relay_forwards_bucket_notes_once_bound_and_counts_them_before() -> None:
    relay = FaultRelay(lambda: T)
    relay.note_unresolved(S1, B, T, T)
    assert relay.counts == {"bucket_notes_dropped": 1}
    budget = Budget()
    relay.bind(budget)
    relay.note_unresolved(S1, B, T, T)
    relay.note_final(S1, B)
    relay.note_no_trade(S1, B)
    relay.series(S1, "refetch_failed")
    assert budget.seen == [("unresolved", B), ("final", B), ("no_trade", B), ("fault", "refetch_failed")]


# ---------------------------------------------------------------------------------------------------- indicators
def test_multi_input_indicators_are_refused_until_dag_inputs_are_canonical() -> None:
    reg = IndicatorRegistry((ATR_V1, Smooth()))
    atr = reg.spec("ATR", 1, S1, {}, INC)
    smooth = reg.spec("TEST_SMOOTH", 1, S1, {"src": atr.indicator_id}, INC)
    engine = MdIndicators(FakeBars(), registry=reg, settings=IndicatorSettings(retry_ms=0))
    assert engine.add(atr) is None
    assert engine.add(smooth) == "registration_refused" and smooth.indicator_id not in engine.tracked()
    opted = MdIndicators(FakeBars(), registry=reg, settings=IndicatorSettings(retry_ms=0, indicator_inputs=True))
    assert opted.add(atr) is None and opted.add(smooth) is None       # the DAG planner itself is kept
