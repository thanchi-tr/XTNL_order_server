"""Shared fixtures of the indicator tests (plan S18): synthetic bars, an in-memory bar read port with `as_of`, two
independent float references of the ATR (window-seeded, and plain `ta.atr` seeded at the chart's first bar) and the
test indicators that show a new class needs no store or port change. Synthetic instruments only."""
from __future__ import annotations

import random
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import replace
from decimal import ROUND_HALF_EVEN, Context, Decimal
from typing import ClassVar

from src.marketdata.indicators.base import (
    Aligned,
    Anchored,
    Count,
    History,
    IndicatorSpec,
    InputRef,
    Output,
    ParamError,
    Point,
    SeriesRef,
    State,
    Unit,
    Window,
)
from src.marketdata.ports import BarsRead
from src.marketdata.types import H1, M1, M15, Bar, BarSpec, DataUnavailable, MdGeneration, Quality, SeriesId, Side, Source

Q = 900_000
T0 = 1_700_000_100_000 - 1_700_000_100_000 % (96 * Q)
"""A checkpoint-aligned bucket start (index on the UTC grid a multiple of 96)."""
INC = Decimal("0.00001")
GEN = MdGeneration(7, 1)
SYN = SeriesId("mock", "SYN1", M15, Side.BID, Source.BROKER)
SYN2 = SeriesId("mock", "SYN2", M15, Side.BID, Source.BROKER)
SYN_ASK = replace(SYN, side=Side.ASK)
SYN_1M = SeriesId("mock", "SYN1", M1, Side.BID, Source.TICK)


def walk(n: int, *, seed: int = 7, series: SeriesId = SYN, start: int = T0, base: str = "1.10000",
         step: int = 40) -> list[Bar]:
    """A deterministic synthetic random walk (the next bar opens at the previous close, some gaps)."""
    rng = random.Random(seed)
    tf = series.spec.timeframe_ms
    price = Decimal(base)
    out: list[Bar] = []
    for k in range(n):
        o = price + (Decimal(rng.randint(-5, 5)) * INC if k % 7 == 0 else Decimal(0))
        path = [o]
        for _ in range(4):
            path.append(path[-1] + Decimal(rng.randint(-step, step)) * INC)
        c = path[-1]
        h = max(path) + Decimal(rng.randint(0, 10)) * INC
        low = min(path) - Decimal(rng.randint(0, 10)) * INC
        out.append(Bar(series=series, bucket_start_ms=start + k * tf, open=o, high=h, low=low, close=c, tick_count=10,
                       quality=Quality.COMPLETE, final_at_ms=start + (k + 1) * tf + 30_000))
        price = c
    return out


class FakeBars:
    """`BarsReadPort` over memory: current bars plus superseded revisions (`md:bars_rev`) for `as_of` reads."""

    def __init__(self) -> None:
        self.current: dict[SeriesId, dict[int, Bar]] = {}
        self.superseded: dict[SeriesId, list[tuple[Bar, int]]] = {}
        self.unavailable: str | None = None
        self.reads = 0

    def put(self, bars: Sequence[Bar]) -> None:
        for b in bars:
            self.current.setdefault(b.series, {})[b.bucket_start_ms] = b

    def revise(self, bar: Bar, at_ms: int) -> Bar:
        """Replace the stored bar with `revision + 1`, final at `at_ms`; the old one superseded at `at_ms`."""
        old = self.current[bar.series][bar.bucket_start_ms]
        new = replace(bar, revision=old.revision + 1, final_at_ms=at_ms)
        self.superseded.setdefault(bar.series, []).append((old, at_ms))
        self.current[bar.series][bar.bucket_start_ms] = new
        return new

    def newest(self, series: SeriesId) -> Bar:
        cur = self.current[series]
        return cur[max(cur)]

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable:
        self.reads += 1
        if self.unavailable is not None:
            return DataUnavailable(self.unavailable)
        cur = self.current.get(series, {})
        if n is not None:
            return BarsRead(tuple(cur[k] for k in sorted(cur)[-n:]), GEN)
        lo = at_bucket_ms if at_bucket_ms is not None else from_bucket_ms
        hi = at_bucket_ms if at_bucket_ms is not None else to_bucket_ms
        assert lo is not None and hi is not None
        out: list[Bar] = []
        olds = self.superseded.get(series, [])
        buckets = sorted({k for k in cur if lo <= k <= hi} | {b.bucket_start_ms for b, _ in olds
                                                                if lo <= b.bucket_start_ms <= hi})
        for k in buckets:
            if as_of_ms is None:
                if k in cur:
                    out.append(cur[k])
                continue
            cands = [b for b in [cur.get(k)] if b is not None and b.final_at_ms is not None
                     and b.final_at_ms <= as_of_ms]
            cands += [b for b, sup in olds if b.bucket_start_ms == k and b.final_at_ms is not None
                      and b.final_at_ms <= as_of_ms < sup]
            if cands:
                out.append(max(cands, key=lambda b: b.revision))
        return BarsRead(tuple(out), GEN)


# ---------------------------------------------------------------------------------------------------- references
def _tr(h: float, low: float, prev: float | None) -> float:
    return h - low if prev is None else max(h - low, abs(h - prev), abs(low - prev))


def ref_window_atr(bars: Sequence[Bar], length: int) -> float:
    """Independent float reference: Pine's RMA seeded at the window's first bar (a `bar_index` offset)."""
    w = 10 * length
    window = list(bars[-w:])
    prev = float(bars[-w - 1].close) if len(bars) > w else None
    trs: list[float] = []
    for b in window:
        trs.append(_tr(float(b.high), float(b.low), prev))
        prev = float(b.close)
    atr = sum(trs[:length]) / length
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
    return atr


def ref_plain_atr(bars: Sequence[Bar], length: int) -> float:
    """Plain `ta.atr(length)`: `ta.rma(ta.tr(true), length)` seeded at the chart's first bar."""
    prev: float | None = None
    trs: list[float] = []
    for b in bars:
        trs.append(_tr(float(b.high), float(b.low), prev))
        prev = float(b.close)
    atr = sum(trs[:length]) / length
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
    return atr


# ---------------------------------------------------------------------------------------------------- test classes
_CTX = Context(prec=40, rounding=ROUND_HALF_EVEN)


class _Base:
    VERSION: ClassVar[int] = 1
    DECIMAL_CONTEXT: ClassVar[Context] = _CTX
    LOWER_BOUND: ClassVar[Decimal | None] = None
    EXACT_SLIDING: ClassVar[bool] = False

    def parse_params(self, raw: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        unknown = sorted(set(raw) - {"n"})
        if unknown:
            raise ParamError(f"unknown parameter {unknown[0]!r}")
        n = int(raw.get("n", "4"))
        if not 1 <= n <= 100:
            raise ParamError("n must be in [1, 100]")
        return (("n", str(n)),)

    def clock(self, spec: IndicatorSpec) -> BarSpec:
        return spec.series.spec

    def inputs(self, spec: IndicatorSpec) -> tuple[InputRef, ...]:
        return (SeriesRef(spec.series),)

    def window(self, spec: IndicatorSpec) -> Mapping[InputRef, Window]:
        return {SeriesRef(spec.series): Count(int(spec.param("n")))}

    def warm_counts(self, spec: IndicatorSpec) -> Mapping[InputRef, int]:
        return dict(((r, w.n if isinstance(w, Count) else 1) for r, w in self.window(spec).items()))

    def warm_span(self, spec: IndicatorSpec) -> int:
        return 0

    def backfill_bars(self, spec: IndicatorSpec) -> int:
        return 16

    def step(self, spec: IndicatorSpec, state: State,
             aligned: Mapping[InputRef, Aligned]) -> tuple[State, Mapping[str, Decimal]]:
        return state, {}

    def provisional(self, spec: IndicatorSpec, state: State,
                    forming: Mapping[InputRef, Bar | Point]) -> Mapping[str, Decimal]:
        return {}

    def _bars(self, spec: IndicatorSpec, history: History) -> list[Bar]:
        return [b for b in history.get(SeriesRef(spec.series), ()) if isinstance(b, Bar)]


class Bands(_Base):
    """Multi-output: highest high, midpoint and lowest low of the last `n` bars."""

    KIND: ClassVar[str] = "TEST_BANDS"
    OUTPUTS: ClassVar[tuple[Output, ...]] = (Output("upper", Unit.PRICE), Output("mid", Unit.PRICE),
                                             Output("lower", Unit.PRICE))

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        bars = self._bars(spec, history)
        if not bars:
            return State.of(n=0)
        u, lo = max(b.high for b in bars), min(b.low for b in bars)
        return State.of(upper=u, lower=lo, mid=(u + lo) / 2, n=len(bars))

    def outputs(self, spec: IndicatorSpec, state: State) -> Mapping[str, Decimal] | None:
        u, m, lo = state.dec("upper"), state.dec("mid"), state.dec("lower")
        return None if u is None or m is None or lo is None else {"upper": u, "mid": m, "lower": lo}


class MidClose(_Base):
    """Multi-input: the mean of the bid and ask closes of the same bucket (RATIO output: the ask / bid ratio)."""

    KIND: ClassVar[str] = "TEST_MID"
    OUTPUTS: ClassVar[tuple[Output, ...]] = (Output("value", Unit.PRICE), Output("ratio", Unit.RATIO))

    def inputs(self, spec: IndicatorSpec) -> tuple[InputRef, ...]:
        return (SeriesRef(spec.series), SeriesRef(replace(spec.series, side=Side.ASK)))

    def window(self, spec: IndicatorSpec) -> Mapping[InputRef, Window]:
        return {r: Count(1) for r in self.inputs(spec)}

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        bid = [b for b in history.get(SeriesRef(spec.series), ()) if isinstance(b, Bar)]
        ask = [b for b in history.get(SeriesRef(replace(spec.series, side=Side.ASK)), ()) if isinstance(b, Bar)]
        if not bid or not ask or bid[-1].bucket_start_ms != ask[-1].bucket_start_ms:
            return State.of(n=0)
        return State.of(mid=(bid[-1].close + ask[-1].close) / 2, ratio=ask[-1].close / bid[-1].close)

    def outputs(self, spec: IndicatorSpec, state: State) -> Mapping[str, Decimal] | None:
        m, r = state.dec("mid"), state.dec("ratio")
        return None if m is None or r is None else {"value": m, "ratio": r}


class ChildRange(_Base):
    """Lower timeframe: a 15m clock over 1m bars: the children closed inside the bucket and their range."""

    KIND: ClassVar[str] = "TEST_CHILDREN"
    OUTPUTS: ClassVar[tuple[Output, ...]] = (Output("count", Unit.COUNT), Output("range", Unit.PRICE))

    def clock(self, spec: IndicatorSpec) -> BarSpec:
        return M15

    def window(self, spec: IndicatorSpec) -> Mapping[InputRef, Window]:
        return {SeriesRef(spec.series): Count(15)}

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        bars = self._bars(spec, history)
        if not bars:
            return State.of(n=0)
        t = M15.bucket_start_ms(bars[-1].bucket_start_ms)
        inside = [b for b in bars if b.bucket_start_ms >= t]
        return State.of(count=len(inside), range=max(b.high for b in inside) - min(b.low for b in inside))

    def outputs(self, spec: IndicatorSpec, state: State) -> Mapping[str, Decimal] | None:
        c, r = state.dec("count"), state.dec("range")
        return None if c is None or r is None else {"count": c, "range": r}


class HourHigh(_Base):
    """Anchored: the highest high since the start of the hour holding the clock bucket (at most 4 bars)."""

    KIND: ClassVar[str] = "TEST_ANCHOR"
    OUTPUTS: ClassVar[tuple[Output, ...]] = (Output("high", Unit.PRICE), Output("bars", Unit.COUNT))

    def window(self, spec: IndicatorSpec) -> Mapping[InputRef, Window]:
        return {SeriesRef(spec.series): Anchored(H1, 4)}

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        bars = self._bars(spec, history)
        if not bars:
            return State.of(n=0)
        return State.of(high=max(b.high for b in bars), bars=len(bars))

    def outputs(self, spec: IndicatorSpec, state: State) -> Mapping[str, Decimal] | None:
        h, n = state.dec("high"), state.dec("bars")
        return None if h is None or n is None else {"high": h, "bars": n}


class Raising(_Base):
    """A planted fault: `seed` raises while `fail` is set."""

    KIND: ClassVar[str] = "TEST_RAISE"
    OUTPUTS: ClassVar[tuple[Output, ...]] = (Output("value", Unit.PRICE),)
    fail: ClassVar[bool] = True

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        if Raising.fail:
            raise RuntimeError("planted")
        bars = self._bars(spec, history)
        return State.of(value=bars[-1].close) if bars else State.of(n=0)

    def outputs(self, spec: IndicatorSpec, state: State) -> Mapping[str, Decimal] | None:
        v = state.dec("value")
        return None if v is None else {"value": v}


class Negative(Raising):
    """A value below the class's declared bound (0)."""

    KIND: ClassVar[str] = "TEST_NEG"
    LOWER_BOUND: ClassVar[Decimal | None] = Decimal(0)

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        return State.of(value=Decimal(-1))


class Spin(Raising):
    """Burns thread CPU for `spin_ms` per seed (the 250 ms CPU cap)."""

    KIND: ClassVar[str] = "TEST_SPIN"
    spin_ms: ClassVar[int] = 40

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        t0 = time.thread_time_ns()
        x = Decimal(1)
        while (time.thread_time_ns() - t0) < Spin.spin_ms * 1_000_000:
            x = (x * Decimal("1.0001")) % Decimal(7)
        return State.of(value=Decimal(1))


class Hang(Raising):
    """Blocks its worker thread until released (a hung job)."""

    KIND: ClassVar[str] = "TEST_HANG"
    gate: ClassVar[threading.Event] = threading.Event()
    calls: ClassVar[int] = 0

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        Hang.calls += 1
        Hang.gate.wait(timeout=5.0)
        return State.of(value=Decimal(1))
