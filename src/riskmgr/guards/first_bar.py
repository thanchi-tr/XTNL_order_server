"""FirstBarGuard: the first-bar test, `first_bar` version 1 (design §B6.5, §B6.7, §B6.2; plan S20, R11-M03, R11-m).

**Bar 0** (Q7, param `anchor`): the bucket holding the fill (`fill_bucket`, the default) or the first full bucket after
it (`next_full_bucket`). Under `next_full_bucket` the fill bucket is the **entry bucket**: its bar end is a guard HOLD
`PRE_ANCHOR` ("no advice this bar (entry bucket)"), and because that needs no market data it is also known at PRE
(`known_before_bar_end`). Bar 0 is derived here from the entry time and the anchor, never from the engine's bar index
(which is only the fallback when the entry time is unknown).

**When it runs:** `EVENTS = {POST}`, i.e. bar-end decisions only (POST and its LATE / REOPEN / CORRECTED re-runs);
PRE never runs it. At bar 0's PRE the engine shows `conditional_label` ("first-bar check pending at bar end").

**The verdict, in exact Decimal:** `FIRST_BAR_VOLATILE` iff `measure(BROKER bar 0, bid) > multiplier × ATR_exact`
(`≥` for `comparison = ge`), where `ATR_exact` is the unquantised canonical ATR(`atr_length`) (`atr_exact`, the same
rule as `md-indicators`: the `W = 10 × length` evaluable bars ending at the ATR bucket, the previous evaluable bar
lending its close) at bar 0 (`atr_bar = current`, Q2 default: including bar 0) or at the bar before it
(`previous`). `measure` is `high_low` (default), `body` or `true_range`. The ratio `measure / ATR_exact` is quantised
(0.001) only as the metric `first_bar_ratio` for the notice; `borderline` (`|ratio / multiplier − 1| ≤ 0.03`, shown,
never hidden) and `roll_affected` (bar 0 overlaps the 17:00 New York roll, Q14) are caveats.

**Not yet decidable:** bar 0 not final, fewer than `W` evaluable bars, the ATR value for the ATR bucket missing,
FAULTED or not `warm` in `md-indicators` (the window rule, which knows the backfill lane's blind buckets, R11-M03), or
an entry time still `ESTIMATED` (an upgrade to BROKER is allowed until bar 0's window ends, §B7) → `PENDING` until bar
0's `decision_cap`; `final_verdict` then gives `on_timeout` = `FIRST_BAR_UNKNOWN(timeout)` under `unknown_policy`
(param `unknown`, Q3: `hold_bar` default, or `skip_test`). An `ATR_exact` below one `price_increment` (zero included)
is `FIRST_BAR_UNKNOWN(atr_degenerate)`; an entry time that is permanently ambiguous is `FIRST_BAR_UNKNOWN
(entry_ambiguous)` at once. A replay span that swallowed bar 0 records `FIRST_BAR_UNKNOWN(stale)` (S22; the guard
never applies after it, since the current bucket is then past bar 0).

**History** (`required_history`): the newest `W + 2` evaluable bid BROKER bars ending at bar 0, counted like
`md-indicators`' `Count` window (S21's reader reads back past `no_trade` / `blind` buckets, `HistoryNeed`), so a
quiet bucket in the span neither shortens the window into a PENDING nor drops the close the oldest bar lends.

Pure: every input comes from the params and the `BarContext`; arithmetic runs under `GUARD_CONTEXT` (never the
ambient context). Reasons and details are codes; no price is logged, stored or shown.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import ROUND_HALF_EVEN, Context, Decimal
from typing import ClassVar, Final

from src.marketdata.indicators.atr import atr_exact, window_bars
from src.marketdata.timing import COLD_SKEW, decision_cap_ms, overlaps_roll
from src.marketdata.types import M15, Bar, BarSpec, Quality, SeriesId, Side, Source
from src.riskmgr.guard import GuardResult, UnknownPolicy
from src.riskmgr.outcomes import Category
from src.riskmgr.strategy import (
    GuardSpec,
    HistoryNeed,
    IndicatorNeed,
    StrategyState,
    param_pairs,
    read_choice,
    read_decimal,
    read_int,
    reject_unknown,
)
from src.riskmgr.types import ArmEvent, BarContext, EntryTimeSource, Ratio

# ---------------------------------------------------------------------------------------------------- codes
FIRST_BAR_VOLATILE: Final = "FIRST_BAR_VOLATILE"
FIRST_BAR_UNKNOWN: Final = "FIRST_BAR_UNKNOWN"
PRE_ANCHOR: Final = "PRE_ANCHOR"

ATR_DEGENERATE: Final = "atr_degenerate"
TIMEOUT: Final = "timeout"
STALE: Final = "stale"
ENTRY_AMBIGUOUS: Final = "entry_ambiguous"
ENTRY_TIME_UNKNOWN: Final = "entry_time_unknown"
INCREMENT_UNKNOWN: Final = "increment_unknown"
"""Details of `FIRST_BAR_UNKNOWN` (engine-form codes)."""

BORDERLINE: Final = "borderline"
ROLL_AFFECTED: Final = "roll_affected"
FIRST_BAR_RATIO: Final = "first_bar_ratio"

CONDITIONAL_LABEL: Final = "first-bar check pending at bar end"
PRE_ANCHOR_TEXT: Final = "no advice this bar (entry bucket)"

# ---------------------------------------------------------------------------------------------------- params
ANCHORS: Final = ("fill_bucket", "next_full_bucket")
MEASURES: Final = ("high_low", "body", "true_range")
COMPARISONS: Final = ("gt", "ge")
ATR_BARS: Final = ("current", "previous")
UNKNOWN_CHOICES: Final = ("hold_bar", "skip_test")
MULTIPLIER_DEFAULT: Final = Decimal("1.318")
MULTIPLIER_MIN: Final = Decimal("0.5")
MULTIPLIER_MAX: Final = Decimal("5.0")
ATR_LENGTH_DEFAULT: Final = 14
ATR_LENGTH_MIN: Final = 5
ATR_LENGTH_MAX: Final = 50
PARAM_KEYS: Final = frozenset({"anchor", "measure", "comparison", "atr_bar", "multiplier", "atr_length", "unknown"})

BAR_SPEC: Final[BarSpec] = M15
"""v1: the 15m bar spec only (the one timing row, §B5.2); the strategy's `bar_spec` admits nothing else."""
BORDERLINE_BAND: Final = Decimal("0.03")
GUARD_CONTEXT: Final = Context(prec=40, rounding=ROUND_HALF_EVEN)
"""Pinned for `first_bar` version 1: never changed in place (a change is a new VERSION)."""
ATR_OUTPUT: Final = "value"


# ---------------------------------------------------------------------------------------------------- pure helpers
def bar0_bucket_ms(anchor: str, entry_time_ms: int, spec: BarSpec = BAR_SPEC) -> int:
    """Bar 0's bucket start: the fill bucket, or (`next_full_bucket`) the bucket after it."""
    fill = spec.bucket_start_ms(entry_time_ms)
    return fill + spec.timeframe_ms if anchor == "next_full_bucket" else fill


def evaluable_bars(bars: Sequence[Bar], upto_end_ms: int) -> list[Bar]:
    """The §A4 evaluable bars (not `blind` / `no_trade`, final) ending at or before `upto_end_ms`, oldest first, one
    per bucket (the same input rule as `md-indicators`)."""
    by_bucket: dict[int, Bar] = {}
    for b in bars:
        if b.quality in (Quality.BLIND, Quality.NO_TRADE) or b.final_at_ms is None or b.bucket_end_ms > upto_end_ms:
            continue
        by_bucket[b.bucket_start_ms] = b
    return [by_bucket[k] for k in sorted(by_bucket)]


def measure_of(measure: str, bar: Bar, prev_close: Decimal | None, c: Context = GUARD_CONTEXT) -> Decimal:
    """`high_low` = h − l; `body` = |c − o|; `true_range` = max(h − l, |h − prev|, |l − prev|) (h − l without a
    previous close). Exact under `c`."""
    hl = c.subtract(bar.high, bar.low)
    if measure == "body":
        return c.subtract(bar.close, bar.open).copy_abs()
    if measure == "true_range" and prev_close is not None:
        return max(hl, c.subtract(bar.high, prev_close).copy_abs(), c.subtract(bar.low, prev_close).copy_abs())
    return hl


def canonical_atr(window_source: Sequence[Bar], length: int) -> Decimal | None:
    """`ATR_exact` over the newest `W + 1` evaluable bars of `window_source` (oldest first): the oldest lends only its
    close when there are more than `W`; None with fewer than `W` bars (not warm)."""
    w = window_bars(length)
    src = list(window_source[-(w + 1):])
    if len(src) < w:
        return None
    prev = src[0].close if len(src) > w else None
    return atr_exact(src[-w:], length, prev_close=prev)


def pre_label(trigger_met: bool) -> str:
    """Bar 0's PRE text (§B6.5): the trigger only, conditional on the first-bar check."""
    return f"{CONDITIONAL_LABEL}; if this bar closes calm, the trigger is {'met' if trigger_met else 'not met'}"


def _bid_series(ctx: BarContext) -> SeriesId:
    return SeriesId(ctx.adapter_id, ctx.symbol, BAR_SPEC, Side.BID, Source.BROKER)


# ---------------------------------------------------------------------------------------------------- the guard
class FirstBarGuard:
    """`first_bar` v1. See the module docstring."""

    KIND: ClassVar[str] = "first_bar"
    VERSION: ClassVar[int] = 1
    EVENTS: ClassVar[frozenset[ArmEvent]] = frozenset({ArmEvent.POST})
    REASONS: ClassVar[Mapping[str, Category]] = {
        FIRST_BAR_VOLATILE: Category.HOLD,
        FIRST_BAR_UNKNOWN: Category.HOLD,
        PRE_ANCHOR: Category.HOLD,
    }

    # ------------------------------------------------------------------------------------------------ params
    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        owner = self.KIND
        reject_unknown(raw, PARAM_KEYS, owner)
        return param_pairs(
            anchor=read_choice(raw, "anchor", ANCHORS, "fill_bucket", owner),
            measure=read_choice(raw, "measure", MEASURES, "high_low", owner),
            comparison=read_choice(raw, "comparison", COMPARISONS, "gt", owner),
            atr_bar=read_choice(raw, "atr_bar", ATR_BARS, "current", owner),
            multiplier=read_decimal(raw, "multiplier", MULTIPLIER_MIN, MULTIPLIER_MAX, MULTIPLIER_DEFAULT, owner),
            atr_length=read_int(raw, "atr_length", ATR_LENGTH_MIN, ATR_LENGTH_MAX, ATR_LENGTH_DEFAULT, owner),
            unknown=read_choice(raw, "unknown", UNKNOWN_CHOICES, "hold_bar", owner),
        )

    @staticmethod
    def _length(params: GuardSpec) -> int:
        return int(params.param("atr_length"))

    def atr_need(self, params: GuardSpec) -> IndicatorNeed:
        """The BROKER bid ATR whose `warm` flag gates the verdict (its id: `atr_need(...).resolve(...)`)."""
        return IndicatorNeed("ATR", 1, BAR_SPEC, Side.BID, (("length", str(self._length(params))),))

    def required_indicators(self, params: GuardSpec) -> tuple[IndicatorNeed, ...]:
        return (self.atr_need(params),)

    def required_history(self, params: GuardSpec) -> tuple[HistoryNeed, ...]:
        """The newest `W + 2` **evaluable** bid BROKER bars ending at the decided bar (the `HistoryNeed` `Count` rule,
        the same as `md-indicators`): bar 0 plus the `W + 1` of the ATR window ending at the bar before it, which
        also covers `W + 1` ending at bar 0. A `no_trade` or `blind` bucket inside the span is read past, never
        counted, so the window and the lent close are the ones `md-indicators` used."""
        return (HistoryNeed(BAR_SPEC, Side.BID, window_bars(self._length(params)) + 2),)

    def unknown_policy(self, params: GuardSpec) -> UnknownPolicy:
        return UnknownPolicy.SKIP if params.param("unknown") == "skip_test" else UnknownPolicy.HOLD_BAR

    def conditional_label(self, params: GuardSpec) -> str:
        return CONDITIONAL_LABEL

    def on_timeout(self, params: GuardSpec) -> GuardResult:
        return GuardResult.unknown(FIRST_BAR_UNKNOWN, TIMEOUT)

    # ------------------------------------------------------------------------------------------------ applicability
    @staticmethod
    def _bucket(ctx: BarContext) -> int:
        return ctx.bar_end_ms - BAR_SPEC.timeframe_ms

    def _entry_bucket(self, params: GuardSpec, ctx: BarContext) -> bool:
        """The decided bucket is the fill bucket under `next_full_bucket` (the entry bucket, `PRE_ANCHOR`)."""
        return (ctx.entry_time_ms is not None and not ctx.entry_time_ambiguous
                and params.param("anchor") == "next_full_bucket"
                and self._bucket(ctx) == BAR_SPEC.bucket_start_ms(ctx.entry_time_ms))

    def applies(self, params: GuardSpec, ctx: BarContext, state: StrategyState) -> bool:
        """Bar 0, or the entry bucket under `next_full_bucket`. Without an entry time, the engine's bar index 0."""
        if ctx.entry_time_ms is None:
            return ctx.bar_index == 0
        bucket = self._bucket(ctx)
        return bucket == bar0_bucket_ms(params.param("anchor"), ctx.entry_time_ms) or self._entry_bucket(params, ctx)

    def known_before_bar_end(self, params: GuardSpec, ctx: BarContext) -> GuardResult | None:
        return GuardResult.hold(PRE_ANCHOR) if self._entry_bucket(params, ctx) else None

    # ------------------------------------------------------------------------------------------------ verdict
    def _warm(self, params: GuardSpec, ctx: BarContext, increment: Decimal, atr_bucket_ms: int) -> bool:
        """`md-indicators` holds a warm, non-FAULTED value for exactly the ATR bucket (the window rule plus the
        consumer's bucket check, plan S18 deviation 5)."""
        iid = self.atr_need(params).resolve(ctx.adapter_id, ctx.symbol, increment).indicator_id
        return any(v.bucket_start_ms == atr_bucket_ms and v.warm and not v.faulted
                   for v in ctx.indicator_values(iid, ATR_OUTPUT))

    def evaluate(self, params: GuardSpec, ctx: BarContext) -> GuardResult:
        c = GUARD_CONTEXT
        if ctx.entry_time_ms is None:
            return GuardResult.unknown(FIRST_BAR_UNKNOWN, ENTRY_TIME_UNKNOWN)
        if ctx.entry_time_ambiguous:
            return GuardResult.unknown(FIRST_BAR_UNKNOWN, ENTRY_AMBIGUOUS)
        if self._entry_bucket(params, ctx):
            return GuardResult.hold(PRE_ANCHOR)
        bar0 = bar0_bucket_ms(params.param("anchor"), ctx.entry_time_ms)
        if self._bucket(ctx) != bar0:
            return GuardResult.passed()          # not bar 0: the test does not apply
        cap = ctx.decision_cap_ms if ctx.decision_cap_ms is not None else \
            decision_cap_ms(ctx.bar_end_ms, BAR_SPEC, COLD_SKEW)
        pending = GuardResult.pending(cap)
        increment = ctx.price_increment
        if increment is None or not increment.is_finite() or increment <= 0:
            return GuardResult.unknown(FIRST_BAR_UNKNOWN, INCREMENT_UNKNOWN)
        if ctx.entry_time_source is EntryTimeSource.ESTIMATED:
            return pending                       # an upgrade to BROKER is still possible until bar 0's cap
        bars = evaluable_bars(ctx.history_of(_bid_series(ctx)), BAR_SPEC.bucket_end_ms(bar0))
        if not bars or bars[-1].bucket_start_ms != bar0:
            return pending                       # bar 0 not final (or not read yet)
        bar = bars[-1]
        before = bars[:-1]
        prev_close = before[-1].close if before else None
        if params.param("atr_bar") == "previous":
            if not before:
                return pending
            atr_bucket = before[-1].bucket_start_ms
            window = before
        else:
            atr_bucket = bar0
            window = bars
        length = self._length(params)
        if not self._warm(params, ctx, increment, atr_bucket):
            return pending
        atr = canonical_atr(window, length)
        if atr is None:
            return pending
        if atr < increment:
            return GuardResult.unknown(FIRST_BAR_UNKNOWN, ATR_DEGENERATE)
        size = measure_of(params.param("measure"), bar, prev_close, c)
        threshold = c.multiply(Decimal(params.param("multiplier")), atr)
        volatile = size >= threshold if params.param("comparison") == "ge" else size > threshold
        caveats: list[str] = []
        if c.subtract(size, threshold).copy_abs() <= c.multiply(BORDERLINE_BAND, threshold):
            caveats.append(BORDERLINE)
        if overlaps_roll(bar0, BAR_SPEC.bucket_end_ms(bar0)):
            caveats.append(ROLL_AFFECTED)
        metrics = ((FIRST_BAR_RATIO, Ratio.of(c.divide(size, atr))),)
        if volatile:
            return GuardResult.hold(FIRST_BAR_VOLATILE, metrics=metrics, caveats=tuple(caveats))
        return GuardResult.passed(metrics=metrics, caveats=tuple(caveats))


FIRST_BAR_V1: Final = FirstBarGuard()

__all__ = [
    "FirstBarGuard", "FIRST_BAR_V1", "FIRST_BAR_VOLATILE", "FIRST_BAR_UNKNOWN", "PRE_ANCHOR", "ATR_DEGENERATE",
    "TIMEOUT", "STALE", "ENTRY_AMBIGUOUS", "ENTRY_TIME_UNKNOWN", "INCREMENT_UNKNOWN", "BORDERLINE", "ROLL_AFFECTED",
    "FIRST_BAR_RATIO", "CONDITIONAL_LABEL", "PRE_ANCHOR_TEXT", "bar0_bucket_ms", "evaluable_bars", "measure_of",
    "canonical_atr", "pre_label", "BAR_SPEC", "GUARD_CONTEXT",
]
