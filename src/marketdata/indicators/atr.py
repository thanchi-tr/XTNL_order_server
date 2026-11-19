"""The canonical ATR (design §A6; plan S18). v1 ships `ATR` version 1.

**Canonical value** (reproducible from bars alone): the value at bucket `t` is computed over exactly the
`W = 10 × length` evaluable bars ending at `t`: the first true range uses the previous bar's close when that bar
exists (the window is `Count(W + 1)`: the extra, oldest bar only lends its close), the seed is the simple average of
the first `length` true ranges, then Wilder's RMA steps to `t` (`atr = (atr × (length − 1) + TR) / length`). It is
exact `Decimal` under the pinned context `ATR_CONTEXT` (`ROUND_HALF_EVEN`, 40 digits) and quantised only for storage
(PRICE unit: `price_increment × 10^-4`). The windowed seed is not an exact sliding form, so the canonical value is
computed on every update (at most `W + 1` Decimal steps per bar; `EXACT_SLIDING` is False); `step` is the RMA
recurrence for `provisional` and for the incremental cross-check of a checkpoint.

`atr_exact(bars, length, prev_close=)` is pure and shared with the ARM's FirstBarGuard (S20).

TradingView's `ta.atr(length)` seeds its RMA at the chart's first bar, not the window's, so it differs from this value
by the seed's remaining weight (about e^-9 at `W = 10 × length`): golden tests compare against a window-seeded
reference within the quantum and against plain `ta.atr` at a relative 1e-3 (C3).
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import ROUND_HALF_EVEN, Context, Decimal, DivisionByZero, InvalidOperation, Overflow, localcontext
from typing import ClassVar, Final, Protocol

from src.marketdata.indicators.base import (
    Aligned,
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
from src.marketdata.types import Bar, BarSpec

ATR_CONTEXT: Final = Context(prec=40, rounding=ROUND_HALF_EVEN, traps=[InvalidOperation, DivisionByZero, Overflow])
"""Pinned for ATR version 1: never changed in place (a change is a new VERSION)."""
W_FACTOR: Final = 10
LENGTH_MIN: Final = 1
LENGTH_MAX: Final = 200
LENGTH_DEFAULT: Final = 14
_ZERO: Final = Decimal(0)


class OHLC(Protocol):
    @property
    def high(self) -> Decimal: ...

    @property
    def low(self) -> Decimal: ...

    @property
    def close(self) -> Decimal: ...


def window_bars(length: int) -> int:
    """`W(length) = 10 × length` evaluable bars define one value."""
    return W_FACTOR * length


def true_range(bar: OHLC, prev_close: Decimal | None) -> Decimal:
    """`max(h − l, |h − prev|, |l − prev|)`; `h − l` without a previous close."""
    hl = bar.high - bar.low
    if prev_close is None:
        return hl
    return max(hl, abs(bar.high - prev_close), abs(bar.low - prev_close))


def atr_exact(bars: Sequence[OHLC], length: int, *, prev_close: Decimal | None = None) -> Decimal:
    """The exact ATR over `bars` (oldest first): seed = mean of the first `length` true ranges (the first one using
    `prev_close` when given), then RMA through the rest, under `ATR_CONTEXT`. Raises ValueError for fewer than
    `length` bars or a length out of bounds."""
    if not LENGTH_MIN <= length <= LENGTH_MAX:
        raise ValueError(f"ATR length must be {LENGTH_MIN}..{LENGTH_MAX}")
    if len(bars) < length:
        raise ValueError("ATR needs at least `length` bars")
    with localcontext(ATR_CONTEXT):
        prev = prev_close
        trs: list[Decimal] = []
        for b in bars:
            trs.append(true_range(b, prev))
            prev = b.close
        n = Decimal(length)
        atr = sum(trs[:length], _ZERO) / n
        for tr in trs[length:]:
            atr = (atr * (n - 1) + tr) / n
        return +atr


def _length(spec: IndicatorSpec) -> int:
    return int(spec.param("length"))


class Atr:
    """ATR version 1: one input (its own series and clock), one PRICE output `value`."""

    KIND: ClassVar[str] = "ATR"
    VERSION: ClassVar[int] = 1
    OUTPUTS: ClassVar[tuple[Output, ...]] = (Output("value", Unit.PRICE),)
    DECIMAL_CONTEXT: ClassVar[Context] = ATR_CONTEXT
    LOWER_BOUND: ClassVar[Decimal | None] = _ZERO
    EXACT_SLIDING: ClassVar[bool] = False

    def parse_params(self, raw: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        unknown = sorted(set(raw) - {"length"})
        if unknown:
            raise ParamError(f"unknown ATR parameter {unknown[0]!r}")
        text = raw.get("length", str(LENGTH_DEFAULT)).strip()
        if not text.isascii() or not text.isdigit() or not LENGTH_MIN <= int(text) <= LENGTH_MAX:
            raise ParamError(f"ATR length must be an integer in [{LENGTH_MIN}, {LENGTH_MAX}]")
        return (("length", str(int(text))),)

    def clock(self, spec: IndicatorSpec) -> BarSpec:
        return spec.series.spec

    def inputs(self, spec: IndicatorSpec) -> tuple[InputRef, ...]:
        return (SeriesRef(spec.series),)

    def window(self, spec: IndicatorSpec) -> Mapping[InputRef, Window]:
        return {SeriesRef(spec.series): Count(window_bars(_length(spec)) + 1)}

    def warm_counts(self, spec: IndicatorSpec) -> Mapping[InputRef, int]:
        return {SeriesRef(spec.series): window_bars(_length(spec))}

    def warm_span(self, spec: IndicatorSpec) -> int:
        return 3 * _length(spec)

    def backfill_bars(self, spec: IndicatorSpec) -> int:
        length = _length(spec)
        return window_bars(length) + 3 * length

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        length = _length(spec)
        w = window_bars(length)
        bars = [b for b in history.get(SeriesRef(spec.series), ()) if isinstance(b, Bar)]
        prev: Decimal | None = None
        if len(bars) > w:
            prev = bars[-w - 1].close
            bars = bars[-w:]
        if not bars:
            return State.of(n=0)
        if len(bars) < length:
            return State.of(n=len(bars), close=bars[-1].close, bucket=bars[-1].bucket_start_ms)
        atr = atr_exact(bars, length, prev_close=prev)
        return State.of(atr=atr, close=bars[-1].close, n=len(bars), bucket=bars[-1].bucket_start_ms)

    def step(self, spec: IndicatorSpec, state: State,
             aligned: Mapping[InputRef, Aligned]) -> tuple[State, Mapping[str, Decimal]]:
        bar = aligned.get(SeriesRef(spec.series))
        if not isinstance(bar, Bar):          # NoTrade / Closed / nothing: the state is unchanged
            out = self.outputs(spec, state)
            return state, dict(out or {})
        atr = state.dec("atr")
        n = state.num("n")
        if atr is None:
            new = State.of(n=n + 1, close=bar.close, bucket=bar.bucket_start_ms)
            return new, {}
        length = Decimal(_length(spec))
        with localcontext(ATR_CONTEXT):
            nxt = +((atr * (length - 1) + true_range(bar, state.dec("close"))) / length)
        new = State.of(atr=nxt, close=bar.close, n=n + 1, bucket=bar.bucket_start_ms)
        return new, {"value": nxt}

    def outputs(self, spec: IndicatorSpec, state: State) -> Mapping[str, Decimal] | None:
        atr = state.dec("atr")
        return None if atr is None else {"value": atr}

    def provisional(self, spec: IndicatorSpec, state: State,
                    forming: Mapping[InputRef, Bar | Point]) -> Mapping[str, Decimal]:
        bar = forming.get(SeriesRef(spec.series))
        atr = state.dec("atr")
        if not isinstance(bar, Bar) or atr is None:
            return {}
        length = Decimal(_length(spec))
        with localcontext(ATR_CONTEXT):
            return {"value": +((atr * (length - 1) + true_range(bar, state.dec("close"))) / length)}


ATR_V1: Final = Atr()

__all__ = ["Atr", "ATR_V1", "ATR_CONTEXT", "atr_exact", "true_range", "window_bars", "W_FACTOR", "LENGTH_DEFAULT"]
