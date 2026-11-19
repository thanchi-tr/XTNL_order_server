"""R11-M03 end to end on the in-memory market-data fakes (plan S20; design §A4, §A6, §B6.5).

One BROKER bucket is blind at its cap (it sits in the `md-bars` backfill lane), so the ATR over it is not `warm`. A
fill one hour later: without the lane resolving the bucket, FirstBarGuard can only wait (PENDING) and becomes
`FIRST_BAR_UNKNOWN(timeout)` at bar 0's cap; once the lane re-fetches the bucket and `md-indicators` recomputes, the
same fill gets a real first-bar verdict. The guard reads the values exactly as the ARM will: bars and the indicator
value through the read side of `md-indicators`, then a pure `BarContext`. Synthetic instruments only.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from decimal import ROUND_HALF_EVEN, Decimal
from fractions import Fraction

import pytest

from src.marketdata.brokerbars import Resolution
from src.marketdata.indicators.atr import window_bars
from src.marketdata.indicators.base import IndicatorValue
from src.marketdata.indicators.engine import IndicatorSettings, MdIndicators
from src.marketdata.ports import BarsRead
from src.marketdata.types import Bar, BarEndEvent, Quality
from src.riskmgr.guard import GuardResult, GuardStatus, final_verdict
from src.riskmgr.guards.first_bar import (
    FIRST_BAR_UNKNOWN,
    FIRST_BAR_V1,
    FIRST_BAR_VOLATILE,
    TIMEOUT,
    canonical_atr,
    evaluable_bars,
)
from src.riskmgr.strategy import GuardSpec
from src.riskmgr.types import ArmEvent, BarContext, EntryTimeSource, HandleView, S0Source
from tests.marketdata.indicators.helpers import INC, SYN, FakeBars, walk

G = FIRST_BAR_V1
SPEC = GuardSpec(G.KIND, G.VERSION, G.parse_params({}))
Q = SYN.spec.timeframe_ms
FAR = 10 ** 13


PRICE_Q = INC.scaleb(-4)
"""The PRICE quantum `md-indicators` stores the ATR at (`price_increment × 10^-4`)."""
M = Fraction(Decimal("1.318"))


async def _history(fake: FakeBars, spec: GuardSpec, upto_end_ms: int) -> tuple[Bar, ...]:
    """S21's reader under the `HistoryNeed` contract: the newest `bars` evaluable bars ending at the decided bar,
    reading further back past `no_trade` / `blind` buckets (the non-evaluable bars in the span come along)."""
    need = G.required_history(spec)[0]
    n = need.bars
    while True:
        read = await fake.bars(SYN, n=n)
        assert isinstance(read, BarsRead)
        bars = tuple(b for b in read.bars if b.bucket_end_ms <= upto_end_ms)
        if need.held_by(bars, upto_end_ms) or len(read.bars) < n:
            return bars
        n += 1


async def _context(eng: MdIndicators, fake: FakeBars, bar0_bucket: int, fill_ms: int, cap: int,
                   spec: GuardSpec = SPEC, atr_bucket: int | None = None) -> BarContext:
    """Bars and the ATR value as the ARM reads them for bar 0's POST."""
    bars = await _history(fake, spec, bar0_bucket + Q)
    iid = G.atr_need(spec).resolve(SYN.adapter_id, SYN.symbol, INC).indicator_id
    values: tuple[IndicatorValue, ...] = ()
    got = await eng.value_as_of(iid, bar0_bucket if atr_bucket is None else atr_bucket, FAR)
    if isinstance(got, dict):
        values = (got["value"],)
    last = bars[-1]
    stop = last.close - 200 * INC
    return BarContext(
        position_id="P1", adapter_id=SYN.adapter_id, symbol=SYN.symbol, side="BUY", bar_spec=SYN.spec,
        event=ArmEvent.POST, bar_end_ms=bar0_bucket + Q, bar_index=0, entry=last.open, entry_time_ms=fill_ms,
        entry_time_source=EntryTimeSource.STREAM, decision_cap_ms=cap, s0=stop, s0_source=S0Source.AT_FILL,
        handles=(HandleView(kind="CHILD_ORDER", handle_id="O1", level=stop),), stop=stop, stop_at_t=stop,
        price_increment=INC, history=((SYN, bars),), indicators=(((iid, "value"), values),),
    )


def ref_atr(window: Sequence[Bar], length: int = 14) -> Fraction:
    """Independent exact reference (§A6): the newest `W` bars of `window`, the bar before them lending its close,
    seed = mean of the first `length` true ranges, then Wilder's RMA, all in `Fraction`."""
    w = window_bars(length)
    src = list(window)[-(w + 1):]
    prev = Fraction(src[0].close) if len(src) > w else None
    trs: list[Fraction] = []
    for b in src[-w:]:
        h, lo = Fraction(b.high), Fraction(b.low)
        trs.append(h - lo if prev is None else max(h - lo, abs(h - prev), abs(lo - prev)))
        prev = Fraction(b.close)
    atr = sum(trs[:length], Fraction(0)) / length
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
    return atr


def assert_canonical_verdict(ctx: BarContext, result: GuardResult, atr_bar: str = "current") -> None:
    """The guard's ATR, quantised to the PRICE unit, is the value `md-indicators` stored for the ATR bucket, and the
    status is the one the `Fraction` reference gives for bar 0's high − low against 1.318 × ATR."""
    evaluable = evaluable_bars(ctx.history[0][1], ctx.bar_end_ms)
    bar0 = evaluable[-1]
    assert bar0.bucket_start_ms == ctx.bar_end_ms - Q
    window = evaluable if atr_bar == "current" else evaluable[:-1]
    assert len(window) >= window_bars(14) + 1                       # the oldest bar lends its close
    (stored,) = ctx.indicators[0][1]
    assert stored.bucket_start_ms == window[-1].bucket_start_ms and stored.warm and not stored.faulted
    exact = canonical_atr(window, 14)
    assert exact is not None
    ref = ref_atr(window)
    assert abs(Fraction(exact) - ref) < Fraction(1, 10 ** 30)
    assert exact.quantize(PRICE_Q, rounding=ROUND_HALF_EVEN) == stored.value == PRICE_Q * round(ref / Fraction(PRICE_Q))
    volatile = Fraction(bar0.high - bar0.low) > M * ref
    assert result.status is (GuardStatus.HOLD if volatile else GuardStatus.PASS)
    if volatile:
        assert result.reason == FIRST_BAR_VOLATILE


async def test_a_blind_bucket_resolved_by_the_lane_gives_a_real_first_bar_verdict() -> None:
    fake = FakeBars()
    bars = walk(300, seed=11)
    hole = bars[-5]                                   # blind at its cap: one hour before bar 0
    bar0 = bars[-1]
    fake.put([b for b in bars if b is not hole])
    lane = {hole.bucket_start_ms}
    eng = MdIndicators(fake, settings=IndicatorSettings(retry_ms=0),
                       blind=lambda s: frozenset(lane) if s == SYN else frozenset())
    spec = G.atr_need(SPEC).resolve(SYN.adapter_id, SYN.symbol, INC)
    await eng.register(spec)
    await eng.cycle()
    fill = bar0.bucket_start_ms + 2 * 60_000
    cap = bar0.bucket_start_ms + 2 * Q + 5_000
    assert bar0.bucket_start_ms - hole.bucket_start_ms == 4 * Q

    before = await _context(eng, fake, bar0.bucket_start_ms, fill, cap)
    (value,) = before.indicators[0][1]
    assert value.bucket_start_ms == bar0.bucket_start_ms and not value.warm and not value.faulted
    blind = G.evaluate(SPEC, before)
    assert blind == GuardResult.pending(cap)          # ATR not warm: a blind bucket in the last 3 × length
    assert final_verdict(G, SPEC, blind, cap) == GuardResult.hold(FIRST_BAR_UNKNOWN, TIMEOUT)

    fake.put([hole])                                  # the backfill lane re-fetched it inside the BARS budget
    lane.clear()
    eng.on_resolved(SYN, hole.bucket_start_ms, Resolution.BAR)
    eng.on_released(BarEndEvent(SYN, hole.bucket_end_ms, "h", 1, hole.revision))
    await eng.cycle()
    assert eng.is_warm(spec.indicator_id)

    ctx = await _context(eng, fake, bar0.bucket_start_ms, fill, cap)
    real = G.evaluate(SPEC, ctx)
    assert real.status is GuardStatus.PASS                      # the Fraction reference below states why
    assert final_verdict(G, SPEC, real, cap + 60_000) == real   # a real verdict, never the timeout
    # the guard's ATR is the canonical value md-indicators computed (same bars, same rule)
    assert len(ctx.history[0][1]) == window_bars(14) + 2
    assert_canonical_verdict(ctx, real)

    # a bar 0 widened far past the ATR turns the same real verdict into FIRST_BAR_VOLATILE
    wide = replace(bar0, high=bar0.high + 400 * INC, low=bar0.low - 400 * INC, quality=Quality.COMPLETE)
    fake.put([wide])
    eng.on_released(BarEndEvent(SYN, wide.bucket_end_ms, "h2", 2, wide.revision + 1, True))
    await eng.cycle()
    wide_ctx = await _context(eng, fake, bar0.bucket_start_ms, fill, cap)
    volatile = G.evaluate(SPEC, wide_ctx)
    assert volatile.reason == FIRST_BAR_VOLATILE
    assert_canonical_verdict(wide_ctx, volatile)


@pytest.mark.parametrize("atr_bar", ["current", "previous"])
async def test_no_trade_buckets_in_the_span_still_give_the_canonical_verdict(atr_bar: str) -> None:
    """Two `no_trade` buckets inside the newest `W + 2` buckets: `md-indicators` stays warm (its `Count` window
    skips them), and the history read under the `HistoryNeed` contract (`W + 2` evaluable bars, read back past
    them) gives a real verdict whose ATR is the stored canonical one, for both `atr_bar` modes. Reading `W + 2` raw
    buckets instead would hold only `W` evaluable bars: a window without the lent close, or a PENDING."""
    spec = GuardSpec(G.KIND, G.VERSION, G.parse_params({"atr_bar": atr_bar}))
    bars = walk(300, seed=11)
    quiet = {bars[-3].bucket_start_ms, bars[-60].bucket_start_ms}
    bars = [replace(b, quality=Quality.NO_TRADE, tick_count=0) if b.bucket_start_ms in quiet else b for b in bars]
    bar0 = bars[-1]
    fake = FakeBars()
    fake.put(bars)
    eng = MdIndicators(fake, settings=IndicatorSettings(retry_ms=0))
    ind = G.atr_need(spec).resolve(SYN.adapter_id, SYN.symbol, INC)
    await eng.register(ind)
    await eng.cycle()
    assert eng.is_warm(ind.indicator_id)
    w = window_bars(14)

    raw = await fake.bars(SYN, n=G.required_history(spec)[0].bars)
    assert isinstance(raw, BarsRead)
    assert len(evaluable_bars(raw.bars, bar0.bucket_end_ms)) == w    # the raw-count read falls two short

    atr_bucket = bar0.bucket_start_ms if atr_bar == "current" else bars[-2].bucket_start_ms
    fill = bar0.bucket_start_ms + 2 * 60_000
    cap = bar0.bucket_start_ms + 2 * Q + 5_000
    ctx = await _context(eng, fake, bar0.bucket_start_ms, fill, cap, spec, atr_bucket)
    assert len(ctx.history[0][1]) == w + 4                           # W + 2 evaluable plus the two quiet buckets
    assert G.required_history(spec)[0].held_by(ctx.history[0][1], ctx.bar_end_ms)
    result = G.evaluate(spec, ctx)
    assert result.status in (GuardStatus.PASS, GuardStatus.HOLD)
    assert final_verdict(G, spec, result, cap + 60_000) == result
    assert_canonical_verdict(ctx, result, atr_bar)
