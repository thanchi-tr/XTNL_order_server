"""FirstBarGuard `first_bar` v1 (plan S20; design §B6.5, §B6.7, Q2, Q3, Q7, Q14, R11-m).

Golden vectors at `multiplier × ATR ± 1 quantum` for a 5-digit and a 3-digit synthetic pair (an independent `Fraction`
reference states the verdict), `gt` / `ge` at the exact boundary, the three measures, `atr_bar` current / previous,
`borderline`, a fill at 17:06 New York (bar 0 just after the daily break, with and without break prints), the entry
bucket `PRE_ANCHOR` (also known at PRE), PENDING until bar 0's `decision_cap` then `FIRST_BAR_UNKNOWN(timeout)` under
the `unknown` policy, `atr_degenerate`, and the entry-time cases. Synthetic instruments only.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from fractions import Fraction
from zoneinfo import ZoneInfo

import pytest

from src.marketdata.indicators.atr import window_bars
from src.marketdata.timing import COLD_SKEW, decision_cap_ms, overlaps_roll
from src.marketdata.types import M15, Quality, Side
from src.riskmgr.guard import GuardResult, GuardStatus, UnknownPolicy, final_verdict
from src.riskmgr.guards.first_bar import (
    ATR_DEGENERATE,
    BORDERLINE,
    CONDITIONAL_LABEL,
    ENTRY_AMBIGUOUS,
    ENTRY_TIME_UNKNOWN,
    FIRST_BAR_RATIO,
    FIRST_BAR_UNKNOWN,
    FIRST_BAR_V1,
    FIRST_BAR_VOLATILE,
    INCREMENT_UNKNOWN,
    PRE_ANCHOR,
    ROLL_AFFECTED,
    TIMEOUT,
    bar0_bucket_ms,
    measure_of,
    pre_label,
)
from src.riskmgr.outcomes import Category, Outcome, OutcomeCode
from src.riskmgr.strategy import GuardSpec, HistoryNeed, IndicatorNeed, ParamError, StrategyState
from src.riskmgr.types import ArmEvent, EntryTimeSource, Ratio
from tests.riskmgr.kind_a import (
    B0,
    BASE,
    FILL,
    FIVE,
    INC,
    Q,
    THREE,
    atr_value,
    bar,
    bar0_with_range,
    flat_history,
    guard_ctx,
    utc_ms,
)

D = Decimal
G = FIRST_BAR_V1
STATE = StrategyState()
A = {FIVE: D("0.00500"), THREE: D("0.500")}
"""A flat ATR history's true range per pair: 500 increments."""


def spec(**params: object) -> GuardSpec:
    return GuardSpec(G.KIND, G.VERSION, G.parse_params(params))


def verdict(symbol: str, x: Decimal, *, a: Decimal | None = None, atr_bar: str = "current", **params: object
            ) -> GuardResult:
    """Bar 0 with high − low = true range = `x` after a flat history of true range `a`."""
    a = A[symbol] if a is None else a
    bars = [*flat_history(symbol, a), bar0_with_range(symbol, x)]
    atr_bucket = B0 if atr_bar == "current" else B0 - Q
    return G.evaluate(spec(atr_bar=atr_bar, **params), guard_ctx(symbol, bars, [atr_value(symbol, atr_bucket)]))


def ref_volatile_current(x: Decimal, a: Decimal, m: Decimal, ge: bool) -> bool:
    """Independent reference: ATR including bar 0 after a flat window of true range `a` is `(13a + x) / 14`."""
    fx, fa, fm = Fraction(x), Fraction(a), Fraction(m)
    atr = (13 * fa + fx) / 14
    return fx >= fm * atr if ge else fx > fm * atr


# ---------------------------------------------------------------------------------------------------- params
def test_params_defaults_bounds_and_requirements() -> None:
    p = spec()
    assert dict(p.params) == {"anchor": "fill_bucket", "measure": "high_low", "comparison": "gt",
                              "atr_bar": "current", "multiplier": "1.318", "atr_length": "14", "unknown": "hold_bar"}
    assert G.required_indicators(p) == (IndicatorNeed("ATR", 1, M15, Side.BID, (("length", "14"),)),)
    assert G.required_history(p) == (HistoryNeed(M15, Side.BID, window_bars(14) + 2),)
    assert G.required_history(spec(atr_length=5)) == (HistoryNeed(M15, Side.BID, 52),)
    assert G.unknown_policy(p) is UnknownPolicy.HOLD_BAR
    assert G.unknown_policy(spec(unknown="skip_test")) is UnknownPolicy.SKIP
    for bad in ({"multiplier": "5.5"}, {"atr_length": 4}, {"measure": "range"}, {"first_bar.anchor": "x"}):
        with pytest.raises(ParamError):
            G.parse_params(bad)


def test_events_reasons_and_labels() -> None:
    assert G.EVENTS == frozenset({ArmEvent.POST})
    assert all(c is Category.HOLD for c in G.REASONS.values())
    assert set(G.REASONS) == {FIRST_BAR_VOLATILE, FIRST_BAR_UNKNOWN, PRE_ANCHOR}
    assert G.conditional_label(spec()) == CONDITIONAL_LABEL == "first-bar check pending at bar end"
    assert pre_label(True).endswith("if this bar closes calm, the trigger is met")
    assert pre_label(False).endswith("the trigger is not met")
    assert G.on_timeout(spec()) == GuardResult.unknown(FIRST_BAR_UNKNOWN, TIMEOUT)


# ---------------------------------------------------------------------------------------------------- golden vectors
@pytest.mark.parametrize("symbol", [FIVE, THREE])
@pytest.mark.parametrize("comparison", ["gt", "ge"])
def test_golden_one_quantum_either_side_of_the_threshold(symbol: str, comparison: str) -> None:
    """Default multiplier 1.318, ATR including bar 0: the boundary `x* = 13·m·a / (14 − m)` is off the grid, so the
    grid points just below and just above it are one quantum apart and decide calm / volatile."""
    inc, a, m = INC[symbol], A[symbol], D("1.318")
    boundary = 13 * Fraction(m) * Fraction(a) / (14 - Fraction(m))
    below = (boundary / Fraction(inc)).__floor__() * inc
    above = below + inc
    assert Fraction(below) < boundary < Fraction(above)
    ge = comparison == "ge"
    for x, volatile in ((below, False), (above, True)):
        assert ref_volatile_current(x, a, m, ge) is volatile
        result = verdict(symbol, x, comparison=comparison)
        expected = GuardStatus.HOLD if volatile else GuardStatus.PASS
        assert result.status is expected, (symbol, comparison, volatile)
        if volatile:
            assert result.reason == FIRST_BAR_VOLATILE
        assert BORDERLINE in result.caveats                      # within 3 % of the threshold


@pytest.mark.parametrize("symbol", [FIVE, THREE])
def test_exact_boundary_previous_atr_gt_and_ge(symbol: str) -> None:
    """`atr_bar = previous`: ATR = a exactly, so `x = 1.318 × a` sits on the grid; `gt` is calm there, `ge` volatile,
    and one quantum either side agrees for both."""
    inc, a = INC[symbol], A[symbol]
    x = D("1.318") * a
    assert x == x.quantize(inc)
    assert verdict(symbol, x, atr_bar="previous", comparison="gt").status is GuardStatus.PASS
    assert verdict(symbol, x, atr_bar="previous", comparison="ge").status is GuardStatus.HOLD
    for comparison in ("gt", "ge"):
        assert verdict(symbol, x - inc, atr_bar="previous", comparison=comparison).status is GuardStatus.PASS
        assert verdict(symbol, x + inc, atr_bar="previous", comparison=comparison).status is GuardStatus.HOLD
    on = verdict(symbol, x, atr_bar="previous")
    assert on.metric(FIRST_BAR_RATIO) == Ratio(D("1.318"))      # quantised for the notice only


@pytest.mark.parametrize("symbol", [FIVE, THREE])
def test_exact_boundary_current_atr_multiplier_two(symbol: str) -> None:
    """ATR including bar 0 with multiplier 2: `a = 600 quanta`, `x = 1300 quanta` gives `2 × (13a + x) / 14 = x`
    exactly (the exact Decimal verdict, no quantised ATR)."""
    inc = INC[symbol]
    a, x = 600 * inc, 1300 * inc
    assert verdict(symbol, x, a=a, multiplier="2", comparison="gt").status is GuardStatus.PASS
    assert verdict(symbol, x, a=a, multiplier="2", comparison="ge").status is GuardStatus.HOLD
    assert verdict(symbol, x + inc, a=a, multiplier="2").status is GuardStatus.HOLD
    assert verdict(symbol, x - inc, a=a, multiplier="2", comparison="ge").status is GuardStatus.PASS


def test_side_does_not_matter_the_test_reads_bid_bars() -> None:
    bars = [*flat_history(FIVE, A[FIVE]), bar0_with_range(FIVE, D("0.00900"))]
    values = [atr_value(FIVE, B0)]
    long = G.evaluate(spec(), guard_ctx(FIVE, bars, values, side="BUY"))
    short = G.evaluate(spec(), guard_ctx(FIVE, bars, values, side="SELL"))
    assert long == short and long.reason == FIRST_BAR_VOLATILE


def test_measures_high_low_body_and_true_range() -> None:
    """A bar 0 that gaps away from the previous close: small range and body, large true range."""
    base = BASE[FIVE]
    b0 = bar(FIVE, B0, base + D("0.00600"), base + D("0.00700"), base + D("0.00600"), base + D("0.00650"))
    bars = [*flat_history(FIVE, A[FIVE]), b0]
    ctx = guard_ctx(FIVE, bars, [atr_value(FIVE, B0)])
    assert G.evaluate(spec(measure="high_low"), ctx).status is GuardStatus.PASS
    assert G.evaluate(spec(measure="body"), ctx).status is GuardStatus.PASS
    tr = G.evaluate(spec(measure="true_range"), ctx)
    assert tr.reason == FIRST_BAR_VOLATILE
    # ATR including bar 0 = (13 × 0.005 + 0.007) / 14; ratio = 0.007 / that = 1.361 (0.001 grid)
    assert tr.metric(FIRST_BAR_RATIO) == Ratio(D("1.361"))


def test_borderline_is_shown_within_three_percent_only() -> None:
    near = verdict(FIVE, D("0.00650"), atr_bar="previous")       # threshold 0.00659: 1.4 % below
    far = verdict(FIVE, D("0.00600"), atr_bar="previous")        # 9 % below
    assert near.status is far.status is GuardStatus.PASS
    assert BORDERLINE in near.caveats and BORDERLINE not in far.caveats


# ---------------------------------------------------------------------------------------------------- 17:06 New York
NY = ZoneInfo("America/New_York")


def ny_ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=NY).timestamp() * 1000)    # type: ignore[misc]


def ratio_ref(x: Fraction, atr: Fraction) -> Ratio:
    """`first_bar_ratio` from the `Fraction` reference, on the 0.001 grid (half even)."""
    return Ratio(D(round(x / atr * 1000)) / 1000)


@pytest.mark.parametrize("shape", ["reopen_only", "break_print_low"])
def test_fill_at_1706_new_york_bar0_just_after_the_daily_break(shape: str) -> None:
    """Q14: a fill at 17:06 New York puts bar 0 in the 17:00-17:15 bucket, which holds the 17:00-17:05 break.

    * `reopen_only`: no break prints; bar 0 starts at the 17:05 reopen print, which is its open and its low.
    * `break_print_low`: a print at 17:02, inside the break, is the bucket's first print and sets its low far below
      the reopen; the rest of the bucket is the same as `reopen_only`.

    The measure follows the bars as they are (high − low = true range here, since the previous close lies inside
    the bar), the verdict and the ratio match the `Fraction` reference, and both carry `roll_affected`."""
    fill = ny_ms(2026, 3, 10, 17, 6)
    bucket = ny_ms(2026, 3, 10, 17, 0)
    assert bar0_bucket_ms("fill_bucket", fill) == bucket == utc_ms(2026, 3, 10, 21, 0)
    base = BASE[FIVE]
    reopen = base                                                # the 17:05 reopen print, at the previous close
    high, close = reopen + D("0.00400"), reopen + D("0.00100")
    low = reopen if shape == "reopen_only" else reopen - D("0.00800")
    b0 = bar(FIVE, bucket, low, high, low, close)                # the first print is the low in both shapes
    history = flat_history(FIVE, A[FIVE], end_bucket=bucket)
    x = high - low
    assert measure_of("high_low", b0, base) == measure_of("true_range", b0, base) == x
    assert x == (D("0.00400") if shape == "reopen_only" else D("0.01200"))

    ctx = guard_ctx(FIVE, [*history, b0], [atr_value(FIVE, bucket)], bucket=bucket, entry_time_ms=fill)
    assert G.applies(spec(), ctx, STATE)
    result = G.evaluate(spec(), ctx)
    atr = (13 * Fraction(A[FIVE]) + Fraction(x)) / 14            # ATR including bar 0 after a flat window
    volatile = ref_volatile_current(x, A[FIVE], D("1.318"), ge=False)
    assert volatile is (shape == "break_print_low")
    assert result.status is (GuardStatus.HOLD if volatile else GuardStatus.PASS)
    assert result.metric(FIRST_BAR_RATIO) == ratio_ref(Fraction(x), atr)
    assert result.metric(FIRST_BAR_RATIO) == Ratio(D("2.182") if volatile else D("0.812"))
    assert ROLL_AFFECTED in result.caveats
    # under next_full_bucket the 17:00 bucket is the entry bucket and bar 0 is 17:15
    assert G.evaluate(spec(anchor="next_full_bucket"), ctx) == GuardResult.hold(PRE_ANCHOR)
    assert bar0_bucket_ms("next_full_bucket", fill) == bucket + Q


def test_fill_at_1659_new_york_bar0_ends_at_the_roll_without_the_caveat() -> None:
    """A fill at 16:59 puts bar 0 in 16:45-17:00. `overlaps_roll` is half-open (`[start, end)`), so a bucket that
    ends exactly at the 17:00 roll is not roll-affected; the 17:00 bucket after it is."""
    fill = ny_ms(2026, 3, 10, 16, 59)
    bucket = ny_ms(2026, 3, 10, 16, 45)
    assert bar0_bucket_ms("fill_bucket", fill) == bucket
    assert not overlaps_roll(bucket, bucket + Q)
    assert overlaps_roll(bucket + Q, bucket + 2 * Q)
    x = D("0.00400")
    bars = [*flat_history(FIVE, A[FIVE], end_bucket=bucket), bar0_with_range(FIVE, x, bucket=bucket)]
    result = G.evaluate(spec(), guard_ctx(FIVE, bars, [atr_value(FIVE, bucket)], bucket=bucket, entry_time_ms=fill))
    assert result.status is GuardStatus.PASS
    assert result.metric(FIRST_BAR_RATIO) == ratio_ref(Fraction(x), (13 * Fraction(A[FIVE]) + Fraction(x)) / 14)
    assert ROLL_AFFECTED not in result.caveats


def test_a_bar_far_from_the_roll_has_no_roll_caveat() -> None:
    assert ROLL_AFFECTED not in verdict(FIVE, D("0.00400")).caveats


# ---------------------------------------------------------------------------------------------------- applicability
def test_applies_on_bar0_only_and_the_next_bar_is_normal() -> None:
    bars = [*flat_history(FIVE, A[FIVE]), bar0_with_range(FIVE, D("0.00900"))]
    p = spec()
    assert G.applies(p, guard_ctx(FIVE, bars), STATE)
    assert not G.applies(p, guard_ctx(FIVE, bars, bucket=B0 + Q, bar_index=1), STATE)
    assert not G.applies(p, guard_ctx(FIVE, bars, bucket=B0 - Q, bar_index=None), STATE)
    # unknown entry time: the engine's bar index is the only hint
    assert G.applies(p, guard_ctx(FIVE, bars, entry_time_ms=None, bar_index=0), STATE)
    assert not G.applies(p, guard_ctx(FIVE, bars, entry_time_ms=None, bar_index=2), STATE)
    assert G.evaluate(p, guard_ctx(FIVE, bars, bucket=B0 + Q)) == GuardResult.passed()


def test_entry_bucket_is_pre_anchor_at_post_and_known_at_pre() -> None:
    """R11-m: under `next_full_bucket` the fill bucket's PRE says "no advice this bar (entry bucket)"."""
    p = spec(anchor="next_full_bucket")
    hist = flat_history(FIVE, A[FIVE])
    pre = guard_ctx(FIVE, hist, event=ArmEvent.PRE)
    post = guard_ctx(FIVE, hist)
    assert G.applies(p, pre, STATE) and G.applies(p, post, STATE)
    assert G.known_before_bar_end(p, pre) == GuardResult.hold(PRE_ANCHOR)
    assert G.evaluate(p, post) == GuardResult.hold(PRE_ANCHOR)
    # bar 0 is the next bucket: nothing is known before its bar end, and it is tested normally
    bars = [*flat_history(FIVE, A[FIVE], end_bucket=B0 + Q), bar0_with_range(FIVE, D("0.00900"), bucket=B0 + Q)]
    nxt = guard_ctx(FIVE, bars, [atr_value(FIVE, B0 + Q)], bucket=B0 + Q, event=ArmEvent.PRE)
    assert G.known_before_bar_end(p, nxt) is None
    assert G.evaluate(p, guard_ctx(FIVE, bars, [atr_value(FIVE, B0 + Q)], bucket=B0 + Q)).reason == FIRST_BAR_VOLATILE
    # under fill_bucket nothing is known before the bar end
    assert G.known_before_bar_end(spec(), pre) is None


# ---------------------------------------------------------------------------------------------------- not decidable
CAP = B0 + 2 * Q + 4_200


@pytest.mark.parametrize("case", ["estimated", "no_value", "not_warm", "faulted", "other_bucket", "bar0_not_final",
                                  "bar0_missing", "short_window"])
def test_pending_until_bar0_decision_cap(case: str) -> None:
    hist = flat_history(FIVE, A[FIVE])
    b0 = bar0_with_range(FIVE, D("0.00400"))
    bars = [*hist, b0]
    values = [atr_value(FIVE, B0)]
    source = EntryTimeSource.STREAM
    if case == "estimated":
        source = EntryTimeSource.ESTIMATED          # a BROKER upgrade is still possible until bar 0's cap
    elif case == "no_value":
        values = []
    elif case == "not_warm":
        values = [atr_value(FIVE, B0, warm=False)]
    elif case == "faulted":
        values = [atr_value(FIVE, B0, faulted=True)]
    elif case == "other_bucket":
        values = [atr_value(FIVE, B0 - Q)]          # md-indicators has not updated for bar 0 yet
    elif case == "bar0_not_final":
        bars = [*hist, bar(FIVE, B0, b0.open, b0.high, b0.low, b0.close, final=False)]
    elif case == "bar0_missing":
        bars = hist
    elif case == "short_window":
        bars = [*hist[-(window_bars(14) - 2):], b0]
    result = G.evaluate(spec(), guard_ctx(FIVE, bars, values, source=source, cap=CAP))
    assert result == GuardResult.pending(CAP)


def test_pending_without_a_cap_uses_the_cold_clock() -> None:
    ctx = guard_ctx(FIVE, flat_history(FIVE, A[FIVE]))
    assert G.evaluate(spec(), ctx) == GuardResult.pending(decision_cap_ms(B0 + Q, M15, COLD_SKEW))


def test_blind_and_no_trade_bars_are_not_evaluable() -> None:
    """A blind or no-trade bucket is skipped in the window (the indicator's warm flag decides whether that is
    acceptable); the verdict still uses exact bars."""
    hist = flat_history(FIVE, A[FIVE], n=window_bars(14) + 3)
    hist[50] = bar(FIVE, hist[50].bucket_start_ms, BASE[FIVE], BASE[FIVE], BASE[FIVE], BASE[FIVE],
                   quality=Quality.NO_TRADE)
    hist[60] = bar(FIVE, hist[60].bucket_start_ms, BASE[FIVE], BASE[FIVE] + 1, BASE[FIVE], BASE[FIVE],
                   quality=Quality.BLIND)
    bars = [*hist, bar0_with_range(FIVE, D("0.00659"))]
    result = G.evaluate(spec(atr_bar="previous"), guard_ctx(FIVE, bars, [atr_value(FIVE, B0 - Q)]))
    assert result.status is GuardStatus.PASS and result.metric(FIRST_BAR_RATIO) == Ratio(D("1.318"))


def test_atr_below_one_increment_is_degenerate() -> None:
    zero = verdict(FIVE, D(0), a=D(0))
    assert zero == GuardResult.unknown(FIRST_BAR_UNKNOWN, ATR_DEGENERATE)
    tiny = verdict(FIVE, INC[FIVE], a=D(0))              # ATR = 1 quantum / 14
    assert tiny == GuardResult.unknown(FIRST_BAR_UNKNOWN, ATR_DEGENERATE)
    assert verdict(FIVE, D(0), a=INC[FIVE], atr_bar="previous").status is GuardStatus.PASS   # ATR = 1 quantum


def test_entry_time_cases() -> None:
    bars = [*flat_history(FIVE, A[FIVE]), bar0_with_range(FIVE, D("0.00400"))]
    values = [atr_value(FIVE, B0)]
    assert G.evaluate(spec(), guard_ctx(FIVE, bars, values, ambiguous=True)) == \
        GuardResult.unknown(FIRST_BAR_UNKNOWN, ENTRY_AMBIGUOUS)
    assert G.evaluate(spec(), guard_ctx(FIVE, bars, values, entry_time_ms=None)) == \
        GuardResult.unknown(FIRST_BAR_UNKNOWN, ENTRY_TIME_UNKNOWN)
    assert G.evaluate(spec(), guard_ctx(FIVE, bars, values, source=EntryTimeSource.BROKER)).status \
        is GuardStatus.PASS
    assert G.evaluate(spec(), guard_ctx(FIVE, bars, values, increment=D(0))) == \
        GuardResult.unknown(FIRST_BAR_UNKNOWN, INCREMENT_UNKNOWN)
    # an ambiguous entry time is never an entry bucket either
    assert G.known_before_bar_end(spec(anchor="next_full_bucket"), guard_ctx(FIVE, bars, ambiguous=True)) is None


# ---------------------------------------------------------------------------------------------------- the unknown policy
def test_final_verdict_pending_then_timeout_under_each_policy() -> None:
    pending = GuardResult.pending(CAP)
    hold, skip = spec(), spec(unknown="skip_test")
    assert final_verdict(G, hold, pending, CAP - 1) == pending
    assert final_verdict(G, hold, pending, CAP) == GuardResult.hold(FIRST_BAR_UNKNOWN, TIMEOUT)
    assert final_verdict(G, skip, pending, CAP) == GuardResult.passed(caveats=("first_bar_skipped",))
    degenerate = GuardResult.unknown(FIRST_BAR_UNKNOWN, ATR_DEGENERATE)
    assert final_verdict(G, hold, degenerate, 0) == GuardResult.hold(FIRST_BAR_UNKNOWN, ATR_DEGENERATE)
    volatile = verdict(FIVE, D("0.00900"))
    assert final_verdict(G, skip, volatile, CAP + 1) == volatile
    calm = verdict(FIVE, D("0.00400"))
    assert final_verdict(G, hold, calm, CAP + 1) == calm


def test_outcome_codes_for_guard_reasons() -> None:
    """A guard reason is stored with its guard KIND and takes HOLD; `PRE_NOT_CONFIRMED(FIRST_BAR_VOLATILE)`."""
    code = OutcomeCode.kind_reason(Outcome.GUARD_REASON, G.KIND, FIRST_BAR_UNKNOWN, G.REASONS, detail=TIMEOUT)
    assert code.category is Category.HOLD and code.owner == "first_bar" and code.detail == "timeout"
    pnc = OutcomeCode.pre_not_confirmed(G.KIND, FIRST_BAR_VOLATILE)
    assert pnc.category is Category.INFO and pnc.reason == FIRST_BAR_VOLATILE


def test_a_fill_near_the_end_of_the_bucket_still_places_bar0_in_it() -> None:
    late = B0 + Q - 10_000
    assert bar0_bucket_ms("fill_bucket", late) == B0
    assert bar0_bucket_ms("next_full_bucket", late) == B0 + Q
    assert bar0_bucket_ms("fill_bucket", FILL) == B0
