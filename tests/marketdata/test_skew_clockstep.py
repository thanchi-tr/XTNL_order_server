"""ClockSkew (settle clamp, fresh samples, re-baseline, settled rule, page signals) and the clock-step / receipt-gap
detectors (plan S14; design §A9, §A7)."""
from __future__ import annotations

from decimal import Decimal

from src.marketdata.clockstep import STEP_THRESHOLD_MS, ReceiptGap, ReceiptGapDetector, WallMonoDetector
from src.marketdata.skew import (
    MIN_SAMPLES,
    NOT_SETTLED_AFTER_OPEN_MS,
    P50_CLEAR_AFTER_MS,
    ClockSkew,
    SkewSignal,
    fresh_sample,
)
from src.marketdata.timing import COLD_SKEW, SETTLE_MAX_MS, SETTLE_MIN_MS

T0 = 1_700_000_000_000
B, A = Decimal("1.1"), Decimal("1.2")


def fill(skew: ClockSkew, lags: list[int], start: int = T0) -> None:
    for i, lag in enumerate(lags):
        broker = start + i * 1_000
        skew.add(broker + lag, broker)


# ---------------------------------------------------------------------------------------------------- skew
def test_cold_until_min_samples() -> None:
    s = ClockSkew("mock")
    assert s.estimate() == COLD_SKEW and not s.settled
    fill(s, [120] * (MIN_SAMPLES - 1))
    assert s.estimate() == COLD_SKEW
    s.add(T0 + 99_000 + 120, T0 + 99_000)
    est = s.estimate()
    assert est.p50_ms == 120 and est.settle_ms == SETTLE_MIN_MS and s.settled


def test_settle_clamp_low_mid_high() -> None:
    low = ClockSkew("mock")
    fill(low, [100] * 50)
    assert low.estimate().settle_ms == SETTLE_MIN_MS             # spread 0 + 250 -> 250
    mid = ClockSkew("mock")
    fill(mid, [100] * 98 + [1_100] * 2)
    est = mid.estimate()
    assert est.p50_ms == 100 and est.settle_ms == 1_000 + 250    # p99 - p50 + 250
    assert not mid.settled                                       # spread above 250 ms
    high = ClockSkew("mock")
    fill(high, [0] * 90 + [20_000] * 10)
    assert high.estimate().settle_ms == SETTLE_MAX_MS
    res = ClockSkew("mock", resolution_ms=1_000)
    fill(res, [100] * 50)
    assert res.estimate().settle_ms == SETTLE_MIN_MS + 1_000      # plus the TickDateRaw resolution
    assert ClockSkew("mock", resolution_ms=1_000).estimate().settle_ms == COLD_SKEW.settle_ms + 1_000


def test_negative_p50_is_kept() -> None:
    s = ClockSkew("mock")
    fill(s, [-300] * 30)
    assert s.estimate().p50_ms == -300


def test_rebaseline_drops_earlier_samples_and_needs_twenty_new_ones() -> None:
    s = ClockSkew("mock")
    fill(s, [100] * 100)
    assert s.settled and s.estimate().p50_ms == 100
    s.rebaseline(T0 + 200_000)
    assert s.samples == 0 and not s.settled and s.estimate() == COLD_SKEW
    fill(s, [2_500] * (MIN_SAMPLES - 1), start=T0 + 201_000)
    assert not s.settled and s.estimate() == COLD_SKEW
    s.add(T0 + 300_000 + 2_500, T0 + 300_000)
    assert s.settled and s.estimate().p50_ms == 2_500             # never compared with the pre-step value


def test_settled_needs_a_tight_spread() -> None:
    s = ClockSkew("mock")
    fill(s, [100, 600] * 20)
    assert not s.settled
    fill(s, [100] * 600, start=T0 + 100_000)                      # the window forgets the wide samples
    assert s.settled


def test_p50_page_with_hysteresis() -> None:
    s = ClockSkew("mock")
    fill(s, [1_500] * 30)
    assert s.evaluate(T0, market_open=False) == (SkewSignal.SKEW_P50_HIGH,)
    assert s.evaluate(T0 + 1_000, market_open=False) == ()        # once
    fill(s, [100] * 512, start=T0 + 10_000)
    assert s.evaluate(T0 + 20_000, market_open=False) == ()       # ok again, hysteresis starts
    assert s.evaluate(T0 + 20_000 + P50_CLEAR_AFTER_MS - 1, market_open=False) == ()
    assert s.evaluate(T0 + 20_000 + P50_CLEAR_AFTER_MS, market_open=False) == (SkewSignal.SKEW_P50_CLEARED,)
    res = ClockSkew("mock", resolution_ms=1_000)
    fill(res, [1_500] * 30)
    assert res.evaluate(T0, market_open=False) == ()              # inside resolution + 1 s


def test_not_settled_ten_minutes_after_open_pages_once_per_baseline() -> None:
    s = ClockSkew("mock")
    assert s.evaluate(T0, market_open=True) == ()
    assert s.evaluate(T0 + NOT_SETTLED_AFTER_OPEN_MS - 1, market_open=True) == ()
    assert s.evaluate(T0 + NOT_SETTLED_AFTER_OPEN_MS, market_open=True) == (SkewSignal.SKEW_NOT_SETTLED,)
    assert s.evaluate(T0 + NOT_SETTLED_AFTER_OPEN_MS + 1, market_open=True) == ()
    s.rebaseline(T0 + 2 * NOT_SETTLED_AFTER_OPEN_MS)
    assert s.evaluate(T0 + 2 * NOT_SETTLED_AFTER_OPEN_MS + 1, market_open=True) == ()
    assert s.evaluate(T0 + 3 * NOT_SETTLED_AFTER_OPEN_MS, market_open=True) == (SkewSignal.SKEW_NOT_SETTLED,)
    settled = ClockSkew("mock")
    fill(settled, [100] * 30)
    assert settled.evaluate(T0, market_open=True) == ()
    assert settled.evaluate(T0 + NOT_SETTLED_AFTER_OPEN_MS, market_open=True) == ()


def test_fresh_sample_filter() -> None:
    base = dict(broker_timed=True, tradeable=True, snapshot=False, received_ms=T0 + 20_000,
                subscribed_since_ms=T0, bid=B, ask=A, prev=(B, Decimal("1.3")))
    assert fresh_sample(**base)  # type: ignore[arg-type]
    for key, value in (("broker_timed", False), ("tradeable", False), ("snapshot", True), ("received_ms", None),
                       ("subscribed_since_ms", T0 + 15_000), ("prev", (B, A))):
        assert not fresh_sample(**{**base, key: value}), key  # type: ignore[arg-type]
    assert fresh_sample(**{**base, "prev": None, "subscribed_since_ms": None})  # type: ignore[arg-type]


def test_snapshot_has_timing_values_only() -> None:
    s = ClockSkew("mock")
    fill(s, [100] * 30)
    snap = s.snapshot()
    assert (snap.p50_ms, snap.p99_ms, snap.settle_ms, snap.samples, snap.settled) == (100, 100, 250, 30, True)


# ---------------------------------------------------------------------------------------------------- clock steps
def test_wall_mono_step_both_directions() -> None:
    d = WallMonoDetector()
    assert d.observe(T0, 1_000) is None
    assert d.observe(T0 + 1_000 + STEP_THRESHOLD_MS, 2_000) is None           # exactly 2 s: not a step
    fwd = d.observe(T0 + 10_000 + 2 * STEP_THRESHOLD_MS + 10_000, 3_000)
    assert fwd is not None and fwd.delta_ms > STEP_THRESHOLD_MS
    assert fwd.from_ms == T0 + 1_000 + STEP_THRESHOLD_MS and fwd.to_ms == T0 + 10_000 + 2 * STEP_THRESHOLD_MS + 10_000
    assert fwd.last_wall_ms == T0 + 1_000 + STEP_THRESHOLD_MS and fwd.mono_delta_ms == 1_000
    back = d.observe(T0, 4_000)
    assert back is not None and back.delta_ms < -STEP_THRESHOLD_MS
    assert back.from_ms == T0 and back.to_ms > T0 + 10_000
    assert d.steps == 2


def test_receipt_gap_found_extended_and_ended() -> None:
    g = ReceiptGapDetector(keepalive_ms={"mock": 5_000})
    assert g.on_subscription("mock", "SYN1", True, T0) is None
    assert g.check(T0 + 5_000) == ()
    assert g.check(T0 + 5_001) == (ReceiptGap("mock", "SYN1", T0, T0 + 5_001),)
    assert g.in_gap("mock", "SYN1")
    assert g.check(T0 + 6_000) == (ReceiptGap("mock", "SYN1", T0 + 5_001, T0 + 6_000),)
    assert g.on_frame("mock", "SYN1", T0 + 7_000) == (ReceiptGap("mock", "SYN1", T0 + 6_000, T0 + 7_000, ended=True),)
    assert not g.in_gap("mock", "SYN1") and g.gaps == 1
    assert g.check(T0 + 11_000) == ()


def test_receipt_gap_heartbeat_and_unwatched_adapters() -> None:
    g = ReceiptGapDetector(keepalive_ms={"mock": 5_000})
    g.on_subscription("mock", "SYN1", True, T0)
    g.on_subscription("mock", "SYN2", True, T0)
    g.on_frame("mock", None, T0 + 4_000)                  # an adapter-wide heartbeat
    assert g.check(T0 + 8_000) == ()
    g.on_subscription("other", "SYN1", True, T0)          # no keepalive stated: not watched
    assert g.check(T0 + 60_000) and all(gap.adapter_id == "mock" for gap in g.check(T0 + 61_000))
    end = g.on_subscription("mock", "SYN1", False, T0 + 62_000)
    assert end is not None and end.ended
    assert all(gap.market == "SYN2" for gap in g.check(T0 + 63_000))


def test_receipt_gap_rebase_moves_watches_into_the_new_wall_frame() -> None:
    g = ReceiptGapDetector(keepalive_ms={"mock": 5_000})
    g.on_subscription("mock", "SYN1", True, T0 + 60_000)
    g.rebase(-60_000, T0)                                 # wall stepped 60 s back
    assert g.check(T0 + 5_001) == (ReceiptGap("mock", "SYN1", T0, T0 + 5_001),)   # not silenced for 60 s
    g.on_frame("mock", "SYN1", T0 + 6_000)
    g.rebase(600_000, T0 + 6_000 + 600_000)               # wall stepped 10 min ahead
    assert g.check(T0 + 606_500) == ()                    # the step itself is not a gap
