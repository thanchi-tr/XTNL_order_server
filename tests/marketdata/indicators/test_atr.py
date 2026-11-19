"""The canonical ATR (plan S18; design §A6): golden vectors at lengths 5, 14 and 50 against an independent
window-seeded reference (within the quantum) and plain `ta.atr` (relative 1e-3), hand-checked small vectors, params,
`step` / `provisional`. Synthetic instruments only."""
from __future__ import annotations

from decimal import Decimal, localcontext

import pytest

from src.marketdata.indicators.atr import ATR_CONTEXT, ATR_V1, atr_exact, true_range, window_bars
from src.marketdata.indicators.base import (
    IndicatorSpec,
    ParamError,
    SeriesRef,
    State,
    quantise,
    quantum,
)
from src.marketdata.indicators.engine import compute_buckets
from src.marketdata.indicators.registry import REGISTRY
from src.marketdata.types import Bar, Quality

from .helpers import INC, SYN, T0, ref_plain_atr, ref_window_atr, walk


def spec(length: int) -> IndicatorSpec:
    return REGISTRY.spec("ATR", 1, SYN, {"length": str(length)}, INC)


def hbar(k: int, h: str, low: str, c: str) -> Bar:
    return Bar(series=SYN, bucket_start_ms=T0 + k * 900_000, open=Decimal(c), high=Decimal(h), low=Decimal(low),
               close=Decimal(c), tick_count=1, quality=Quality.COMPLETE, final_at_ms=T0 + (k + 1) * 900_000)


def test_hand_checked_small_vector() -> None:
    bars = [hbar(0, "10", "8", "9"), hbar(1, "12", "9", "11"), hbar(2, "11", "10", "10.5"), hbar(3, "13", "10.5", "12")]
    # TR = 2, 3, 1, 2.5; seed (2 + 3) / 2 = 2.5; RMA: (2.5 + 1) / 2 = 1.75; (1.75 + 2.5) / 2 = 2.125
    assert atr_exact(bars, 2) == Decimal("2.125")
    # a previous close of 7 makes the first TR max(2, |10 - 7|, |8 - 7|) = 3: seed 3, then 2, then 2.25
    assert atr_exact(bars, 2, prev_close=Decimal(7)) == Decimal("2.25")
    assert true_range(bars[1], Decimal(9)) == Decimal(3)
    assert atr_exact(bars[:1], 1) == Decimal(2)


def test_atr_exact_bounds() -> None:
    bars = walk(3)
    with pytest.raises(ValueError):
        atr_exact(bars, 5)
    with pytest.raises(ValueError):
        atr_exact(bars, 0)


@pytest.mark.parametrize("length", [5, 14, 50])
def test_golden_vectors_against_window_seeded_reference_and_plain_ta_atr(length: int) -> None:
    w = window_bars(length)
    bars = walk(w + 600, seed=length)
    s = spec(length)
    q = quantum(ATR_V1.OUTPUTS[0].unit, INC)
    targets = [b.bucket_start_ms for b in bars[w + 1::37]]
    computed = compute_buckets(ATR_V1, s, {SeriesRef(SYN): bars}, targets)
    assert [c.bucket_start_ms for c in computed] == targets
    for c in computed:
        k = (c.bucket_start_ms - T0) // 900_000
        upto = bars[:k + 1]
        stored = quantise(c.outputs["value"], q, ATR_CONTEXT)
        ref = Decimal(repr(ref_window_atr(upto, length))).quantize(q)
        assert abs(stored - ref) <= q, f"window-seeded reference differs at bucket index {k}"
        plain = ref_plain_atr(upto, length)
        assert abs(float(c.outputs["value"]) - plain) / plain < 1e-3, f"plain ta.atr differs at bucket index {k}"
        assert c.warm


def test_canonical_value_is_the_window_only() -> None:
    """Bars older than the window plus its previous bar never change the value (reproducible from W + 1 bars)."""
    s = spec(14)
    w = window_bars(14)
    bars = walk(w + 300, seed=3)
    t = bars[-1].bucket_start_ms
    full = compute_buckets(ATR_V1, s, {SeriesRef(SYN): bars}, [t])[0]
    tail = compute_buckets(ATR_V1, s, {SeriesRef(SYN): bars[-(w + 1):]}, [t])[0]
    assert full.outputs == tail.outputs
    short = compute_buckets(ATR_V1, s, {SeriesRef(SYN): bars[-w:]}, [t])[0]
    assert short.warm and short.outputs["value"] == atr_exact(bars[-w:], 14)    # no previous bar: its first TR is h - l
    not_warm = compute_buckets(ATR_V1, s, {SeriesRef(SYN): bars[-(w - 1):]}, [t])[0]
    assert not not_warm.warm


def test_params_bounds_and_unknown_keys() -> None:
    assert ATR_V1.parse_params({}) == (("length", "14"),)
    assert ATR_V1.parse_params({"length": " 5 "}) == (("length", "5"),)
    for bad in ({"length": "0"}, {"length": "201"}, {"length": "x"}, {"length": "-3"}, {"length": "14", "src": "c"}):
        with pytest.raises(ParamError) as e:
            ATR_V1.parse_params(bad)
        assert "201" not in str(e.value) and "'x'" not in str(e.value)   # bounds and key names, never the value
    s = spec(14)
    assert ATR_V1.backfill_bars(s) == 182 and ATR_V1.warm_span(s) == 42
    assert ATR_V1.warm_counts(s) == {SeriesRef(SYN): 140}


def test_step_is_the_rma_recurrence_and_provisional_does_not_store() -> None:
    s = spec(5)
    bars = walk(80, seed=11)
    state = ATR_V1.seed(s, {SeriesRef(SYN): bars[:-1]})
    nxt, out = ATR_V1.step(s, state, {SeriesRef(SYN): bars[-1]})
    prev_atr = state.dec("atr")
    assert prev_atr is not None
    tr = true_range(bars[-1], bars[-2].close)
    with localcontext(ATR_CONTEXT):
        assert out["value"] == (prev_atr * 4 + tr) / 5
    assert ATR_V1.provisional(s, state, {SeriesRef(SYN): bars[-1]}) == out
    assert ATR_V1.step(s, nxt, {})[0] == nxt                    # nothing aligned: the state is unchanged
    assert ATR_V1.outputs(s, State.of(n=0)) is None
