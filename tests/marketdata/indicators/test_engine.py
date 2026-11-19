"""`md-indicators` (plan S18; design §A6, §A8, R11-M03, R11-m): fault containment (a planted raising indicator
becomes FAULTED while every other series keeps ATR; CPU cap; hung job; out-of-range value; task quarantine), recompute
after a revision, a blind bucket resolved by the backfill lane makes ATR warm again at once, incremental equals
canonical at every checkpoint, replay from re-fetched bars gives the identical quantised value, `md_rebuild` clears
FAULTED, `as_of` across a revision and past `md:bars_rev` retention, and the store path over fakeredis (event order,
the `IND` script, the port's indicator reads). Synthetic instruments only; no price in an assertion message."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from src.marketdata.brokerbars import Resolution
from src.marketdata.indicators.atr import ATR_CONTEXT, ATR_V1, LENGTH_MAX, atr_exact, window_bars
from src.marketdata.indicators.base import (
    IndicatorUpdate,
    SeriesRef,
    decode_value,
    quantise,
    quantum,
)
from src.marketdata.indicators.checkpoint import decode_checkpoint, due
from src.marketdata.indicators.engine import (
    IndicatorFault,
    IndicatorSettings,
    IndState,
    MdIndicators,
    compute_batch,
    compute_buckets,
)
from src.marketdata.indicators.registry import IndicatorRegistry
from src.marketdata.port_impl import MdPort
from src.marketdata.ports import IndicatorRead
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.reads import StoreReader
from src.marketdata.store.runner import ScriptOk, ScriptRefused, StoreUnavailable
from src.marketdata.bootstrap import Sm12Event
from src.marketdata.types import BarEndEvent, DataUnavailable, SeriesId, Sm12State

from ..bootrig import make_rig
from ..conftest import KEYRING, RUN_ID, SENTINEL, SERIES, TOKEN, RunIds, bar
from .helpers import INC, Q, SYN, SYN2, T0, Bands, FakeBars, Hang, Negative, Raising, Spin, walk

REG = IndicatorRegistry((ATR_V1, Bands(), Raising(), Negative(), Spin(), Hang()))
FAST = IndicatorSettings(retry_ms=0)


class FakeWrite:
    """The writer connection for `IND` calls: records the calls and answers `ScriptOk` (or raises)."""

    def __init__(self) -> None:
        self.calls: list[S.ScriptCall] = []
        self.fail: bool = False

    async def run(self, calls: Sequence[S.ScriptCall]) -> list[Any]:
        if self.fail:
            raise StoreUnavailable("timeout", "TimeoutError")
        self.calls.extend(calls)
        return [ScriptOk(len(c.keys) - 5) for c in calls]

    def values(self, iid: str, output: str) -> dict[int, str]:
        out: dict[int, str] = {}
        key = K.indicator(iid, output)
        for c in self.calls:
            if c.script.name == "ind" and key in c.keys:
                out[int(c.args[5])] = c.args[12 + c.keys.index(key) - 5]
        return out

    def checkpoints(self, iid: str) -> dict[int, str]:
        return {int(c.args[5]): c.args[9] for c in self.calls if c.script.name == "ind" and c.args[4] == iid
                and c.args[9]}


def atr_spec(series: Any = SYN, length: int = 14) -> Any:
    return REG.spec("ATR", 1, series, {"length": str(length)}, INC)


def make(fake: FakeBars, **kw: Any) -> MdIndicators:
    kw.setdefault("settings", FAST)
    return MdIndicators(fake, registry=REG, **kw)


def released(b: Any, *, revised: bool = False) -> BarEndEvent:
    return BarEndEvent(b.series, b.bucket_end_ms, "h", 1, b.revision, revised)


def canonical(bars: Sequence[Any], length: int = 14) -> Decimal:
    w = window_bars(length)
    prev = bars[-w - 1].close if len(bars) > w else None
    return atr_exact(bars[-w:], length, prev_close=prev)


# ---------------------------------------------------------------------------------------------------- faults
async def test_planted_raising_indicator_faults_while_every_other_series_keeps_atr() -> None:
    fake = FakeBars()
    fake.put(walk(200, seed=1) + walk(200, seed=2, series=SYN2))
    faults: list[tuple[str, str]] = []
    pages: list[str] = []

    class Pager:
        def page(self, severity: str, title: str, body: str) -> None:
            pages.append(f"{title}: {body}")

    eng = make(fake, on_fault=lambda i, r: faults.append((i, r)), pager=Pager())
    a1, a2 = atr_spec(SYN), atr_spec(SYN2)
    bad = REG.spec("TEST_RAISE", 1, SYN, {}, INC)
    Raising.fail = True
    try:
        assert await eng.register(a1) is not None and await eng.register(a2) is not None
        st = await eng.register(bad)
        assert st is not None and st.state is IndState.PENDING and st.fault_streak == 1
        await eng.cycle()
        await eng.cycle()
        st = eng.status(bad.indicator_id)
        assert st is not None and st.state is IndState.FAULTED and st.reason == "raised" and not st.warm
        assert faults == [(bad.indicator_id, "raised")] and len(pages) == 1
        assert eng.latest(bad.indicator_id, "value") is None
        more = walk(201, seed=1)[-1:] + walk(201, seed=2, series=SYN2)[-1:]
        fake.put(more)
        for b in more:
            eng.on_released(released(b))
        await eng.cycle()
        assert eng.counters.dropped >= 1                      # the faulted indicator's job is dropped
        for spec, b in ((a1, more[0]), (a2, more[1])):
            status = eng.status(spec.indicator_id)
            assert status is not None and status.last_bucket_ms == b.bucket_start_ms and status.warm
            assert eng.latest(spec.indicator_id, "value") == canonical(sorted(fake.current[b.series].values(),
                                                                              key=lambda x: x.bucket_start_ms))
    finally:
        Raising.fail = True


async def test_md_rebuild_clears_faulted_and_completes_its_request() -> None:
    fake = FakeBars()
    fake.put(walk(30))
    done: list[tuple[int, str, str]] = []
    eng = make(fake, complete=lambda r, o, why: done.append((r, o, why)))
    bad = REG.spec("TEST_RAISE", 1, SYN, {}, INC)
    Raising.fail = True
    await eng.register(bad)
    await eng.cycle()
    await eng.cycle()
    assert eng.status(bad.indicator_id).state is IndState.FAULTED     # type: ignore[union-attr]
    assert eng.md_rebuild(7, "f" * 16) == "not_registered"
    Raising.fail = False
    try:
        assert eng.md_rebuild(42, bad.indicator_id) is None
        await eng.cycle()
    finally:
        Raising.fail = True
    st = eng.status(bad.indicator_id)
    assert st is not None and st.state is IndState.READY and st.fault_streak == 0 and st.reason == ""
    assert done == [(42, "applied", "warm")]
    assert eng.latest(bad.indicator_id, "value") == fake.newest(SYN).close


async def test_cpu_cap_and_out_of_range_values_are_faults() -> None:
    fake = FakeBars()
    fake.put(walk(10))
    eng = make(fake, settings=IndicatorSettings(retry_ms=0, cpu_cap_ms=5))
    spin = REG.spec("TEST_SPIN", 1, SYN, {}, INC)
    neg = REG.spec("TEST_NEG", 1, SYN, {}, INC)
    Spin.spin_ms = 30
    await eng.register(spin)
    await eng.register(neg)
    await eng.cycle()
    await eng.cycle()
    assert eng.status(spin.indicator_id).reason == "cpu_cap"            # type: ignore[union-attr]
    assert eng.status(neg.indicator_id).reason == "out_of_range"        # type: ignore[union-attr]
    assert eng.counters.cpu_capped == 3


async def test_hung_job_is_confined_to_its_indicator() -> None:
    fake = FakeBars()
    fake.put(walk(200))
    eng = make(fake, settings=IndicatorSettings(retry_ms=0, job_timeout_s=0.2))
    hang = REG.spec("TEST_HANG", 1, SYN, {}, INC)
    atr = atr_spec(SYN)
    Hang.gate.clear()
    try:
        st = await eng.register(hang)
        assert st is not None and st.state is IndState.FAULTED and st.reason == "job_hung"
        assert eng.md_rebuild(1, hang.indicator_id) == "busy"            # its thread is still running
        assert (await eng.register(atr)) is not None and eng.is_warm(atr.indicator_id)
    finally:
        Hang.gate.set()
    await asyncio.sleep(0.05)
    assert eng.md_rebuild(1, hang.indicator_id) is None


async def test_register_again_never_runs_a_second_job_on_a_hung_or_faulted_indicator() -> None:
    fake = FakeBars()
    fake.put(walk(30))
    write = FakeWrite()
    eng = make(fake, write=write, token=lambda: TOKEN, boot=lambda: SENTINEL,
               settings=IndicatorSettings(retry_ms=0, job_timeout_s=0.2))
    hang = REG.spec("TEST_HANG", 1, SYN, {}, INC)
    Hang.gate.clear()
    Hang.calls = 0
    try:
        st = await eng.register(hang)
        assert st is not None and st.state is IndState.FAULTED and st.reason == "job_hung"
        again = await eng.register(hang)                          # a second consumer's lease
        assert again is not None and again.state is IndState.FAULTED
        await asyncio.sleep(0.05)
        assert Hang.calls == 1 and eng.counters.hung == 1           # one thread, never a second one
    finally:
        Hang.gate.set()
    await asyncio.sleep(0.05)
    bad = REG.spec("TEST_RAISE", 1, SYN, {}, INC)
    Raising.fail = True
    await eng.register(bad)
    await eng.cycle()
    await eng.cycle()
    assert eng.status(bad.indicator_id).state is IndState.FAULTED      # type: ignore[union-attr]
    calls = len(write.calls)
    Raising.fail = False
    try:
        again = await eng.register(bad)                           # demanded by another consumer
        await eng.cycle()
    finally:
        Raising.fail = True
    assert again is not None and again.state is IndState.FAULTED
    assert len(write.calls) == calls                              # no IND write over the fault marker


async def test_faulted_survives_every_lease_lapsing_until_md_rebuild() -> None:
    fake = FakeBars()
    fake.put(walk(30))
    done: list[tuple[int, str, str]] = []
    eng = make(fake, complete=lambda r, o, why: done.append((r, o, why)))
    bad = REG.spec("TEST_RAISE", 1, SYN, {}, INC)
    iid = bad.indicator_id
    Raising.fail = True
    await eng.register(bad)
    await eng.cycle()
    await eng.cycle()
    assert eng.remove(iid) and eng.status(iid) is None              # every lease released or lapsed
    Raising.fail = False
    try:
        st = await eng.register(bad)                              # demanded again
        assert st is not None and st.state is IndState.FAULTED and st.reason == "raised"
        await eng.cycle()
        assert eng.status(iid).state is IndState.FAULTED and eng.latest(iid, "value") is None  # type: ignore[union-attr]
        assert eng.md_rebuild(3, iid) is None
        await eng.cycle()
        assert eng.status(iid).state is IndState.READY             # type: ignore[union-attr]
        assert done == [(3, "applied", "warm")]
        # a tombstone of an indicator nobody demands: md_rebuild clears it, the next demand re-seeds
        Raising.fail = True
        eng.md_rebuild(4, iid)
        await eng.cycle()
        await eng.cycle()
        await eng.cycle()
        assert eng.status(iid).state is IndState.FAULTED           # type: ignore[union-attr]
        assert eng.remove(iid)
        assert eng.md_rebuild(5, iid) is None and done[-1] == (5, "applied", "not_tracked")
        assert eng.md_rebuild(6, "f" * 16) == "not_registered"
        Raising.fail = False
        st = await eng.register(bad)
        assert st is not None and st.state is IndState.READY
    finally:
        Raising.fail = True


async def test_hung_threads_are_bounded_and_a_never_started_call_clears_busy() -> None:
    """A worker call still queued behind the shared executor when the job times out is never cancelled (its id stays
    busy until it has run); at `max_hung_threads` no new job is dispatched (one page) until the threads return."""
    fake = FakeBars()
    fake.put(walk(200) + walk(200, seed=2, series=SYN2))
    gate = asyncio.Event()
    started: list[int] = []

    async def queued(fn: Any) -> Any:            # a worker that has not started yet: the executor is busy
        await gate.wait()
        started.append(1)
        return fn()

    pages: list[str] = []

    class Pager:
        def page(self, severity: str, title: str, body: str) -> None:
            pages.append(title)

    eng = make(fake, run_in_thread=queued, pager=Pager(),
               settings=IndicatorSettings(retry_ms=0, job_timeout_s=0.05, max_hung_threads=1))
    a, b = atr_spec(SYN), atr_spec(SYN2)
    st = await eng.register(a)
    assert st is not None and st.state is IndState.FAULTED and st.reason == "job_hung"
    assert eng.md_rebuild(1, a.indicator_id) == "busy"
    st = await eng.register(b)                    # the hung limit is reached: queued, not dispatched
    assert st is not None and st.state is IndState.PENDING and st.last_bucket_ms is None
    await eng.cycle()
    await eng.cycle()
    assert pages.count("indicator threads hung") == 1 and eng.status(b.indicator_id).last_bucket_ms is None  # type: ignore[union-attr]
    assert await eng.value_as_of(b.indicator_id, T0, T0) == DataUnavailable("indicators_saturated")
    gate.set()
    await asyncio.sleep(0.05)
    assert started == [1]                         # the queued call ran once it could; nothing was lost
    assert eng.md_rebuild(2, a.indicator_id) is None
    await eng.cycle()
    assert eng.is_warm(a.indicator_id) and eng.is_warm(b.indicator_id)


async def test_cycle_stamps_between_bounded_steps() -> None:
    fake = FakeBars()
    fake.put(walk(200))
    eng = make(fake, write=FakeWrite(), token=lambda: TOKEN, boot=lambda: SENTINEL)

    class Stamp:
        n = 0

        def stamp(self) -> None:
            self.n += 1

        def set_exempt(self, reason: str | None) -> None:
            return None

    assert eng.add(atr_spec()) is None
    sp = Stamp()
    await eng.cycle(stamp=sp)
    assert sp.n >= 5                              # before the job, newest read, input read, thread, write, after
    with pytest.raises(ValueError):
        IndicatorSettings(job_timeout_s=8.0, io_timeout_s=2.0)


async def test_is_warm_needs_the_newest_closed_bucket_read_from_the_store() -> None:
    fake = FakeBars()
    fake.put(walk(200))
    eng = make(fake)
    a, b = atr_spec(), atr_spec(length=10)
    await eng.register(a)
    assert eng.is_warm(a.indicator_id)
    fake.put(walk(201)[-1:])                      # a newer bar this process never saw released (a takeover)
    await eng.register(b)
    assert eng.is_warm(b.indicator_id) and not eng.is_warm(a.indicator_id)


async def test_a_long_market_closure_reads_further_back_for_the_window() -> None:
    before = walk(200, seed=41)
    after = walk(10, seed=42, start=T0 + (200 + 480) * Q)        # a 5-day closure between them
    fake = FakeBars()
    fake.put(before + after)
    eng = make(fake, clock_ms=lambda: after[-1].bucket_end_ms + 60_000)
    spec = atr_spec()
    st = await eng.register(spec)
    assert st is not None and st.warm and fake.reads >= 3
    assert eng.latest(spec.indicator_id, "value") == canonical(before + after)


def test_a_batch_stops_at_the_cpu_budget_and_the_cap_is_per_target() -> None:
    bars = walk(10)
    spin = REG.spec("TEST_SPIN", 1, SYN, {}, INC)
    targets = [b.bucket_start_ms for b in bars]
    Spin.spin_ms = 20                                           # thread CPU ticks are ~16 ms on Windows
    out, done = compute_batch(Spin(), spin, {SeriesRef(SYN): bars}, targets, cpu_cap_ms=100, budget_ms=50)
    assert 2 <= done <= 4 and len(out) == done                  # 200 ms over 10 targets, no target over the cap
    out, done = compute_batch(Spin(), spin, {SeriesRef(SYN): bars}, targets, cpu_cap_ms=100)
    assert done == 10
    Spin.spin_ms = 60
    try:
        with pytest.raises(IndicatorFault):
            compute_batch(Spin(), spin, {SeriesRef(SYN): bars}, targets[:1], cpu_cap_ms=50)
    finally:
        Spin.spin_ms = 40


async def test_a_long_atr_revision_is_split_by_the_cpu_budget_never_faulted() -> None:
    w = window_bars(LENGTH_MAX)
    bars = walk(w + 1 + 96, seed=31)
    fake = FakeBars()
    fake.put(bars[:w + 1])
    write = FakeWrite()
    eng = make(fake, write=write, token=lambda: TOKEN, boot=lambda: SENTINEL)      # the real 250 ms cap
    spec = atr_spec(length=LENGTH_MAX)
    await eng.register(spec)
    for b in bars[w + 1:]:
        fake.put([b])
        eng.on_released(released(b))
        await eng.cycle()
    jobs = eng.counters.jobs
    target = bars[w + 1]                                         # every held value after it is recomputed
    fake.revise(replace(target, high=target.high + 300 * INC), at_ms=bars[-1].bucket_end_ms + 60_000)
    eng.on_released(released(fake.current[SYN][target.bucket_start_ms], revised=True))
    for _ in range(200):
        await eng.cycle()
    assert eng.counters.faults == 0 and eng.counters.jobs > jobs
    current = [fake.current[SYN][k] for k in sorted(fake.current[SYN])]
    stored = write.values(spec.indicator_id, "value")
    q = quantum(ATR_V1.OUTPUTS[0].unit, INC)
    for b in current[w + 1:]:
        upto = [x for x in current if x.bucket_start_ms <= b.bucket_start_ms]
        v = decode_value(spec.indicator_id, "value", stored[b.bucket_start_ms])
        assert v is not None and v.value == quantise(canonical(upto, LENGTH_MAX), q, ATR_CONTEXT)


async def test_on_quarantine_drops_the_head_job_and_faults_its_indicator() -> None:
    fake = FakeBars()
    fake.put(walk(200))
    faults: list[tuple[str, str]] = []
    eng = make(fake, on_fault=lambda i, r: faults.append((i, r)))
    spec = atr_spec()
    assert eng.add(spec) is None
    eng.on_quarantine()
    st = eng.status(spec.indicator_id)
    assert st is not None and st.state is IndState.FAULTED and faults == [(spec.indicator_id, "task_quarantine")]
    await eng.cycle()
    assert eng.latest(spec.indicator_id, "value") is None


async def test_unreadable_store_defers_and_is_never_a_fault() -> None:
    fake = FakeBars()
    fake.put(walk(200))
    fake.unavailable = "not_live"
    eng = make(fake)
    spec = atr_spec()
    st = await eng.register(spec)
    assert st is not None and st.state is IndState.PENDING and st.fault_streak == 0
    assert eng.counters.deferred == 1 and eng.counters.faults == 0
    fake.unavailable = None
    await eng.cycle()
    assert eng.is_warm(spec.indicator_id)


# ---------------------------------------------------------------------------------------------------- recompute
async def test_recompute_after_a_revision() -> None:
    fake = FakeBars()
    bars = walk(203, seed=5)
    fake.put(bars[:200])
    eng = make(fake)
    spec = atr_spec()
    await eng.register(spec)
    for b in bars[200:]:
        fake.put([b])
        eng.on_released(released(b))
        await eng.cycle()
    before = eng.latest(spec.indicator_id, "value")
    target = bars[201]
    fake.revise(replace(target, high=target.high + 200 * INC), at_ms=target.bucket_end_ms + 120_000)
    eng.on_released(released(fake.current[SYN][target.bucket_start_ms], revised=True))
    await eng.cycle()
    current = [fake.current[SYN][k] for k in sorted(fake.current[SYN])]
    after = eng.latest(spec.indicator_id, "value")
    assert after != before and after == canonical(current)
    st = eng.status(spec.indicator_id)
    assert st is not None and st.last_bucket_ms == bars[-1].bucket_start_ms
    # the revised bucket's own value was recomputed too
    upto = [b for b in current if b.bucket_start_ms <= target.bucket_start_ms]
    computed = compute_buckets(ATR_V1, spec, {SeriesRef(SYN): current}, [target.bucket_start_ms])[0]
    assert computed.outputs["value"] == canonical(upto)


async def test_blind_bucket_resolved_in_the_lane_makes_atr_warm_at_once() -> None:
    """R11-M03: a blind BROKER bucket resolved by the S17 backfill lane: ATR is warm again without waiting
    3 × length new bars."""
    fake = FakeBars()
    bars = walk(300, seed=9)
    hole = bars[-5]
    fake.put([b for b in bars if b is not hole])
    lane = {hole.bucket_start_ms}
    eng = make(fake, blind=lambda s: frozenset(lane) if s == SYN else frozenset())
    spec = atr_spec()
    await eng.register(spec)
    assert not eng.is_warm(spec.indicator_id)                 # a blind bucket inside the last 3 × length
    await eng.cycle()
    assert not eng.is_warm(spec.indicator_id)
    fake.put([hole])                                          # the lane re-fetched it: md-bars wrote the bar
    lane.clear()
    eng.on_resolved(SYN, hole.bucket_start_ms, Resolution.BAR)
    eng.on_released(released(hole))
    await eng.cycle()
    assert eng.is_warm(spec.indicator_id)
    assert eng.latest(spec.indicator_id, "value") == canonical(bars)


async def test_checkpoint_state_equals_canonical_and_step_tracks_it() -> None:
    """Every checkpoint's state is the canonical value of its bucket (written from the canonical recompute), and the
    RMA `step` path from each checkpoint over the following bars up to the next one tracks the canonical value of
    those buckets within C3's relative 1e-3 (the two differ only by the window seed's remaining weight, so the
    incremental path is never the stored value; see deviation 3)."""
    fake = FakeBars()
    w = window_bars(14)
    bars = walk(w + 1 + 200, seed=13)
    fake.put(bars[:w + 1])
    write = FakeWrite()
    eng = make(fake, write=write, token=lambda: TOKEN, boot=lambda: SENTINEL)
    spec = atr_spec()
    await eng.register(spec)
    for b in bars[w + 1:]:
        fake.put([b])
        eng.on_released(released(b))
        await eng.cycle()
    q = quantum(ATR_V1.OUTPUTS[0].unit, INC)
    ckpts = write.checkpoints(spec.indicator_id)
    stored = write.values(spec.indicator_id, "value")
    due_buckets = [b.bucket_start_ms for b in bars[w + 1:] if due(b.bucket_start_ms, SYN.spec)]
    assert len(due_buckets) >= 2 and sorted(ckpts) == due_buckets
    for t in due_buckets:
        upto = [b for b in bars if b.bucket_start_ms <= t]
        exact = canonical(upto)
        value = decode_value(spec.indicator_id, "value", stored[t])
        assert value is not None and value.value == quantise(exact, q, ATR_CONTEXT) and value.warm
        ck = decode_checkpoint(ckpts[t])
        assert ck is not None and ck.state.dec("atr") == exact
    stepped = 0
    for t, nxt in zip(due_buckets, due_buckets[1:] + [bars[-1].bucket_start_ms + Q]):
        ck = decode_checkpoint(ckpts[t])
        assert ck is not None
        state = ck.state
        for b in (b for b in bars if t < b.bucket_start_ms < nxt):
            state, outs = ATR_V1.step(spec, state, {SeriesRef(SYN): b})
            upto = [x for x in bars if x.bucket_start_ms <= b.bucket_start_ms]
            exact = canonical(upto)
            assert abs(outs["value"] - exact) <= exact * Decimal("1e-3")
            stepped += 1
    assert stepped >= 100


async def test_replay_from_refetched_broker_bars_gives_the_identical_quantised_value() -> None:
    bars = walk(240, seed=21)
    first, second = FakeBars(), FakeBars()
    first.put(bars)
    second.put([replace(b, revision=0, final_at_ms=(b.final_at_ms or 0) + 7_200_000) for b in bars])   # a new term
    w1, w2 = FakeWrite(), FakeWrite()
    spec = atr_spec()
    for fake, write in ((first, w1), (second, w2)):
        eng = make(fake, write=write, token=lambda: TOKEN, boot=lambda: SENTINEL)
        await eng.register(spec)
    t = bars[-1].bucket_start_ms
    v1 = decode_value(spec.indicator_id, "value", w1.values(spec.indicator_id, "value")[t])
    v2 = decode_value(spec.indicator_id, "value", w2.values(spec.indicator_id, "value")[t])
    assert v1 is not None and v2 is not None and v1.value == v2.value and v1.lineage.input_digest == \
        v2.lineage.input_digest


async def test_as_of_across_a_revision_and_past_bars_rev_retention() -> None:
    fake = FakeBars()
    bars = walk(200, seed=17)
    fake.put(bars)
    eng = make(fake)
    spec = atr_spec()
    await eng.register(spec)
    t = bars[-1].bucket_start_ms
    old = eng.latest(spec.indicator_id, "value")
    revised_at = bars[-1].bucket_end_ms + 600_000
    target = bars[-3]
    fake.revise(replace(target, low=target.low - 150 * INC), at_ms=revised_at)
    eng.on_released(released(fake.current[SYN][target.bucket_start_ms], revised=True))
    await eng.cycle()
    new = eng.latest(spec.indicator_id, "value")
    assert new != old
    q = quantum(ATR_V1.OUTPUTS[0].unit, INC)
    before = await eng.value_as_of(spec.indicator_id, t, revised_at - 1)
    after = await eng.value_as_of(spec.indicator_id, t, revised_at)
    assert not isinstance(before, DataUnavailable) and not isinstance(after, DataUnavailable)
    assert before["value"].value == quantise(old or Decimal(0), q, ATR_CONTEXT)
    assert after["value"].value == quantise(new or Decimal(0), q, ATR_CONTEXT)
    fake.superseded.clear()                                     # the old revision aged out of md:bars_rev (7 days)
    assert await eng.value_as_of(spec.indicator_id, t, revised_at - 1) == DataUnavailable("superseded_expired")
    assert await eng.value_as_of("f" * 16, t, revised_at) == DataUnavailable("not_registered")


async def test_write_failure_defers_the_value_and_retries() -> None:
    fake = FakeBars()
    fake.put(walk(200))
    write = FakeWrite()
    write.fail = True
    updates: list[IndicatorUpdate] = []
    eng = make(fake, write=write, token=lambda: TOKEN, boot=lambda: SENTINEL)
    eng.on_updated(updates.append)
    spec = atr_spec()
    st = await eng.register(spec)
    assert st is not None and st.last_bucket_ms is None and not updates and eng.counters.write_errors == 1
    write.fail = False
    await eng.cycle()
    assert [u.bucket_start_ms for u in updates] == [fake.newest(SYN).bucket_start_ms] and eng.is_warm(spec.indicator_id)
    await eng.cycle()                                           # nothing queued: nothing rewritten
    assert len(write.calls) == 1


async def test_global_failed_writes_only_md_rebuild_and_keeps_recomputes_queued() -> None:
    """S32 (design §B13, §B14): while the ARM is globally FAILED, md-indicators writes no recompute and a new
    registration computes nothing (the jobs stay queued); an `md_rebuild` repair still runs and completes; after the
    clear the queued work runs."""
    fake = FakeBars()
    fake.put(walk(200) + walk(200, seed=2, series=SYN2))
    write = FakeWrite()
    failed = [False]
    done: list[tuple[int, str, str]] = []
    eng = make(fake, write=write, token=lambda: TOKEN, boot=lambda: SENTINEL, global_failed=lambda: failed[0],
               complete=lambda r, o, why: done.append((r, o, why)))
    a1, a2 = atr_spec(SYN), atr_spec(SYN2)
    assert await eng.register(a1) is not None and write.calls
    failed[0] = True
    n = len(write.calls)
    more = walk(201)[-1:]
    fake.put(more)
    eng.on_released(released(more[0]))                          # a recompute is queued
    st = await eng.register(a2)                                 # a registration computes nothing
    assert st is not None and not st.warm
    await eng.cycle()
    await eng.cycle()
    assert len(write.calls) == n and eng.counters.failed_cycles == 2
    assert not any(c.args[4] == a2.indicator_id for c in write.calls if c.script.name == "ind")
    # the md_rebuild repair is applied during FAILED (it runs with the queued recompute of the same indicator)
    assert eng.md_rebuild(5, a1.indicator_id) is None
    await eng.cycle()
    assert len(write.calls) > n and done == [(5, "applied", "warm")]
    assert not any(c.args[4] == a2.indicator_id for c in write.calls if c.script.name == "ind")
    # cleared: the queued work runs
    failed[0] = False
    await eng.cycle()
    assert any(c.args[4] == a2.indicator_id for c in write.calls if c.script.name == "ind")
    assert eng.is_warm(a2.indicator_id)


# ---------------------------------------------------------------------------------------------------- store path
async def live_rig(md_redis: Any, run_ids: RunIds, n: int) -> Any:
    rig = await make_rig(md_redis, run_ids, start=T0)
    await rig.at(T0)
    for k in range(n):
        rig.writer.queue_bar(bar(T0 + k * Q, f"1.23{k % 10}0"))
        await rig.at(T0 + (k + 1) * Q + 60_000)
    rig.book.apply(SERIES, Sm12Event.LIVE)
    return rig


def reg_port(rig: Any, reg: IndicatorRegistry, **kw: Any) -> MdPort:
    """A read port over the rig's store whose registry knows the test classes."""
    kw.setdefault("book", rig.book)
    return MdPort(StoreReader(rig.redis), keyring=KEYRING, leader_epoch=lambda: rig.writer.token.epoch_rev,
                  clock_ms=rig.clock, registry=reg, **kw)


async def test_store_event_order_bar_closed_then_indicator_updated(md_redis: Any, run_ids: RunIds,
                                                                   caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    rig = await live_rig(md_redis, run_ids, 22)
    reg = IndicatorRegistry((ATR_V1, Bands(), Raising()))
    eng = MdIndicators(rig.port, registry=reg, write=rig.client, token=lambda: rig.writer.token,
                       boot=lambda: rig.writer.boot, settings=FAST, clock_ms=rig.clock)
    order: list[str] = []
    rig.writer.on_released(eng.on_released)
    rig.writer.on_released(lambda e: order.append(f"bar:{e.bar_end_ms}"))
    eng.on_updated(rig.port.wake_indicator)
    rig.port.on_indicator_updated(lambda u: order.append(f"ind:{u.bar_end_ms}"))
    spec = reg.spec("ATR", 1, SERIES, {"length": "2"}, Decimal("0.0001"))
    bands = reg.spec("TEST_BANDS", 1, SERIES, {"n": "3"}, Decimal("0.0001"))
    assert await eng.register(spec) is not None and await eng.register(bands) is not None
    assert eng.is_warm(spec.indicator_id)
    order.clear()
    rig.writer.queue_bar(bar(T0 + 22 * Q, "1.2377"))
    end = T0 + 23 * Q
    await rig.at(end + 60_000)
    await eng.cycle()
    assert order == [f"bar:{end}", f"ind:{end}", f"ind:{end}"]
    events = await md_redis.xrange(K.EVENTS)
    tail = [(f[b"k"].decode(), int(f[b"e"])) for _, f in events if b"e" in f][-3:]
    assert tail == [("bar_closed", end), ("indicator_updated", end), ("indicator_updated", end)]
    assert all(b"i" not in f or len(f) == 3 for _, f in events)        # ids and bar ends only, never a value
    read = await rig.port.indicator(spec.indicator_id, "value", n=2)
    assert isinstance(read, IndicatorRead) and [v.bucket_start_ms for v in read.values] == [end - 2 * Q, end - Q]
    assert read.values[-1].warm and read.values[-1].value is not None
    port = reg_port(rig, reg)                                       # resolves the test classes from md:registry:ind
    for out in ("upper", "mid", "lower"):                           # a new class: three keys, no store change
        r = await port.indicator(bands.indicator_id, out, at_bucket_ms=end - Q)
        assert isinstance(r, IndicatorRead) and len(r.values) == 1
    assert (await md_redis.hget(K.REGISTRY_IND, spec.indicator_id)).decode() == spec.canonical
    assert await rig.port.indicator(spec.indicator_id, "value") == DataUnavailable("bad_request")
    for rec in caplog.records:
        assert "1.23" not in rec.getMessage()
    # a planted fault: the marker reads as indicator_faulted
    bad = reg.spec("TEST_RAISE", 1, SERIES, {}, Decimal("0.0001"))
    Raising.fail = True
    await eng.register(bad)
    await eng.cycle()
    await eng.cycle()
    assert await port.indicator(bad.indicator_id, "value", n=1) == DataUnavailable("indicator_faulted")
    Raising.fail = False                    # the marker sits on the newest closed bucket: md_rebuild's value replaces it
    try:
        assert eng.md_rebuild(1, bad.indicator_id) is None
        await eng.cycle()
    finally:
        Raising.fail = True
    healed = await port.indicator(bad.indicator_id, "value", n=1)
    assert isinstance(healed, IndicatorRead) and healed.values[0].value is not None


class Gate:
    """A settable SM12 gate (`StateGate`)."""

    def __init__(self) -> None:
        self.sm12 = Sm12State.LIVE
        self.ok = True

    def state(self, series: SeriesId) -> Sm12State:
        return self.sm12

    def readable(self, series: SeriesId, from_ms: int | None, to_ms: int | None) -> bool:
        return self.ok

    def gaps(self, series: SeriesId) -> int:
        return 0


async def test_port_indicator_read_passes_the_decision_gate(md_redis: Any, run_ids: RunIds) -> None:
    """§A10: an indicator read is a decision read; FAILED, global FAILED and not LIVE give `DataUnavailable` like
    `bars`, and a spec neither `md-indicators` nor `md:registry:ind` resolves is `not_registered`."""
    rig = await live_rig(md_redis, run_ids, 22)
    reg = IndicatorRegistry((ATR_V1, Raising()))
    eng = MdIndicators(rig.port, registry=reg, write=rig.client, token=lambda: rig.writer.token,
                       boot=lambda: rig.writer.boot, settings=FAST, clock_ms=rig.clock)
    spec = reg.spec("ATR", 1, SERIES, {"length": "2"}, Decimal("0.0001"))
    iid = spec.indicator_id
    assert await eng.register(spec) is not None
    gate, failed = Gate(), [False]
    port = reg_port(rig, reg, book=gate, global_failed=lambda: failed[0])
    wired = reg_port(rig, IndicatorRegistry(), book=gate, indicators=eng)     # resolved through md-indicators
    for p in (port, wired):
        assert isinstance(await p.indicator(iid, "value", n=1), IndicatorRead)
    gate.ok = False
    assert await port.indicator(iid, "value", n=1) == DataUnavailable("not_live")
    assert await wired.indicator(iid, "value", at_bucket_ms=T0) == DataUnavailable("not_live")
    gate.ok, gate.sm12 = True, Sm12State.FAILED
    assert await port.indicator(iid, "value", n=1) == DataUnavailable("failed")
    gate.sm12, failed[0] = Sm12State.LIVE, True
    assert await port.indicator(iid, "value", n=1) == DataUnavailable("failed")
    failed[0] = False
    assert await port.indicator("f" * 16, "value", n=1) == DataUnavailable("not_registered")
    unknown_class = reg_port(rig, IndicatorRegistry(), book=gate)
    assert await unknown_class.indicator(iid, "value", n=1) == DataUnavailable("not_registered")


async def test_older_fault_markers_are_dropped_from_n_reads(md_redis: Any, run_ids: RunIds) -> None:
    rig = await live_rig(md_redis, run_ids, 22)
    reg = IndicatorRegistry((ATR_V1, Raising()))
    eng = MdIndicators(rig.port, registry=reg, write=rig.client, token=lambda: rig.writer.token,
                       boot=lambda: rig.writer.boot, settings=FAST, clock_ms=rig.clock)
    rig.writer.on_released(eng.on_released)
    bad = reg.spec("TEST_RAISE", 1, SERIES, {}, Decimal("0.0001"))
    iid = bad.indicator_id
    port = reg_port(rig, reg)
    end = T0 + 23 * Q
    Raising.fail = False
    try:
        assert await eng.register(bad) is not None
        Raising.fail = True
        rig.writer.queue_bar(bar(T0 + 22 * Q, "1.2377"))
        await rig.at(end + 60_000)
        for _ in range(3):
            await eng.cycle()
        assert eng.status(iid).state is IndState.FAULTED                # type: ignore[union-attr]
        assert await port.indicator(iid, "value", n=2) == DataUnavailable("indicator_faulted")
        Raising.fail = False
        assert eng.md_rebuild(9, iid) is None
        await eng.cycle()
    finally:
        Raising.fail = True
    read = await port.indicator(iid, "value", n=3)
    assert isinstance(read, IndicatorRead) and read.values and all(v.value is not None for v in read.values)
    assert read.values[-1].bucket_start_ms == end - Q
    assert len(await md_redis.zrange(K.indicator(iid, "value"), 0, -1)) == 2     # the marker is still stored


async def test_ind_script_guards_trims_and_emits(md_redis: Any, run_ids: RunIds) -> None:
    from src.marketdata.store.runner import ScriptRunner
    await md_redis.set(K.WRITER, TOKEN.text)
    await md_redis.set(K.BOOT, SENTINEL.text)
    runner = ScriptRunner(md_redis, run_id_reader=run_ids)
    iid = "0123456789abcdef"
    m_old = f"{T0 - 400 * 86_400_000:013d}|0.0001|C|1|{0:013d}|{0:013d}|{'0' * 16}|0000|-"
    old = S.IndicatorWrite(iid, T0 - 400 * 86_400_000, T0 - 400 * 86_400_000 + Q, (("value", m_old),), emit=False)
    reply = await runner.run([S.indicator_call(TOKEN, SENTINEL, old, value_floor_ms=0, checkpoint_floor_ms=0)])
    assert isinstance(reply[0], ScriptOk)
    m = f"{T0:013d}|0.000123|C|1|{T0 + Q:013d}|{T0 - 140 * Q:013d}|{'a' * 16}|0000|-"
    w = S.IndicatorWrite(iid, T0, T0 + Q, (("value", m),), checkpoint=f"{T0:013d}|{{\"atr\":\"1\"}}",
                         definition='{"k":"ATR"}')
    floor = T0 - 365 * 86_400_000
    reply = await runner.run([S.indicator_call(TOKEN, SENTINEL, w, value_floor_ms=floor, checkpoint_floor_ms=floor)])
    assert reply == [ScriptOk(1)]
    assert [x.decode() for x in await md_redis.zrange(K.indicator(iid, "value"), 0, -1)] == [m]   # old trimmed
    assert await md_redis.zcard(K.indicator_checkpoints(iid)) == 1
    assert (await md_redis.hget(K.REGISTRY_IND, iid)).decode() == '{"k":"ATR"}'
    kinds = [f[b"k"] for _, f in await md_redis.xrange(K.EVENTS)]
    assert kinds == [b"indicator_updated"]                       # the emit-less write added no event
    bad_member = S.IndicatorWrite(iid, T0, T0 + Q, (("value", "x" * 20),))
    assert await runner.run([S.indicator_call(TOKEN, SENTINEL, bad_member, value_floor_ms=0,
                                              checkpoint_floor_ms=0)]) == [ScriptRefused("BAD_ARGS")]
    other = K.BootSentinel(RUN_ID, "1234567890", "node-a", replace(SENTINEL.generation, n=9))
    assert await runner.run([S.indicator_call(TOKEN, other, w, value_floor_ms=0,
                                              checkpoint_floor_ms=0)]) == [ScriptRefused("NO_BOOT")]
    with pytest.raises(ValueError):
        S.indicator_call(TOKEN, SENTINEL, replace(w, indicator_id="nothex"), value_floor_ms=0, checkpoint_floor_ms=0)
