"""The D6 cross-check: each final BROKER bar against the TICK bar of the same bucket and side (design §A4 Cross-check,
§B14 SERIES; plan S17b, R11-M01, R11-M02).

Pure and synchronous: no I/O, no clock of its own (callers pass `now_ms`), no store. `md-bars` (S17a) hands every
final BROKER bar it commits to `CrossChecker.submit` and calls `poll` once per pass; the TICK side comes from a
`TickRefBook`, an in-memory book of the closed TICK bars `md-writer` drains from the hook (`MdWriter.on_closed_tick`),
each with its `close_eligible` flag and its median clean spread (`tickbars.TickBar.median_spread`, memory only).

* **Per-field eligibility (R11-M01):** the close only when the TICK bucket is `close_eligible` (coverage complete,
  possible under `filtered` delivery too); high and low only when the TICK bar is `complete`. The eligible fields
  form the bucket's eligible vector; with none eligible the check is skipped (`not_eligible`).
* **Tolerance:** `k` × the bucket's median clean spread (`XTNL_MD_CROSSCHECK_K`, default 1.0). A field agrees when
  it differs from the TICK bar's by **less** than the tolerance (a bar exactly one tolerance away does not agree, so a
  bid / ask swap at a constant spread is still seen at `k` = 1). A bar within tolerance of its own TICK bar on the
  whole eligible vector is always a **match**.
* **Mismatch and its kind (R11-M02):** a bar that fails the tolerance is a mismatch. It is labelled a time shift
  (`shift_prev` / `shift_next`: the TICK bar of bucket T − 1 / T + 1) or a side swap (`swap`: the other side's TICK
  bar of bucket T) only when bucket T fails **and** that alternative passes the tolerance by a margin of at least
  0.5 × tolerance on the same eligible vector; otherwise it is a `value` mismatch. The shift test is skipped when the
  TICK bucket's range (high − low) is below 3 × its median spread, so a quiet, flat series is never read as shifted.
  The kind is a metric and a log code; every mismatch counts the same.
* **Flags and K:** one mismatch flags the bucket (`caveat`: decisions that use the bar carry `source_mismatch`) and a
  metric; no page. `K` consecutive mismatches (`XTNL_MD_MISMATCH_K`, default 3) block the series (`blocked`) and call
  `on_fault(series, "source_mismatch")` once (the §B14 SERIES fault; S30 records FAILED and pings). "Consecutive" is
  in bucket order over the checked buckets: a skipped bucket neither flags nor resets, and a match resets.
* **Operator clear:** `clear(series)` (S30 / S31 call it when the operator clears the series' FAILED) resets the
  streak and re-arms the fault, keeping the verdicts and caveats: only buckets after the newest one already judged
  count toward the next streak, so a feed that is still wrong reaches `K` (and FAILED) again.
* **Demand:** `forget` (the series left demand) keeps the state for one timeframe of the series, so a brief lapse of
  demand (an S18 lease renewed late) never resets a partial streak; `resume` (or the next bar) keeps it, and `poll`
  drops it once the grace is over (a later demand then starts afresh).
* **Waiting for the TICK bar:** a live BROKER bar can be final before the writer has drained its TICK bar; it waits
  (`tick_wait_ms`, re-tried by `poll`) and is skipped (`no_tick_bar`) when its TICK bar never comes. A historical bar
  (backfill, lane) is checked only when its TICK bar is in the book, and never counts toward the skipped ratio.
* **Skipped ratio and D6 status:** over the newest 16 live (demanded) buckets per series: the skipped count is a
  metric, and more than half skipped pages once per episode (`info`); the episode ends (the page re-arms) only when
  a quarter or fewer are skipped, so a count swinging around half never pages again. The D6 status is `ACTIVE` when high and low were
  checked in that window, `CLOSE_ONLY` when only the close was, `UNAVAILABLE` when nothing could be checked (only the
  hard checks guard the series). Both reach `md:meta` through `meta_pairs` (`d6`, `xc_*` counts; zero counts for a
  series without state, so a restart or a re-demand never leaves an earlier streak in `md:meta`) and `md show`.

Nothing here logs, pages or formats a price, a spread or a tolerance: series ids, bucket starts, field names, kinds,
reason codes and counts only, and none of it goes to PostgreSQL.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Final, Protocol

from src.marketdata.errors import safe_redis_error
from src.marketdata.ports import PagePort
from src.marketdata.tickbars import TickBar
from src.marketdata.timing import TIMING_TABLE, timing_for
from src.marketdata.types import SOURCE_MISMATCH, Bar, BarSpec, Quality, SeriesId, Side, Source

log = logging.getLogger(__name__)

WINDOW: Final = 16
"""Demanded (live) buckets per series the skipped ratio and the D6 status look at."""
MARGIN: Final = Decimal("0.5")
"""An alternative (neighbour, other side) passes by at least this × tolerance to label a shift or a swap."""
QUIET_RANGE_SPREADS: Final = 3
"""The shift test is skipped when the TICK bucket's range is below this many median spreads."""
TICK_WAIT_MS: Final = 120_000
"""How long a live BROKER bar waits for its TICK bar (the TICK bar is final by `T + p50 + settle`)."""
VERDICT_KEEP: Final = 64
PENDING_KEEP: Final = 64
BOOK_KEEP: Final = 96
BOOK_MAX_SERIES: Final = 1_024
DEFAULT_TICK_SPECS: Final = frozenset(row.cross_check_tick_spec for row in TIMING_TABLE.values())


# ---------------------------------------------------------------------------------------------------- values
class Field(str, Enum):
    CLOSE = "close"
    HIGH = "high"
    LOW = "low"


class Verdict(str, Enum):
    MATCH = "match"
    MISMATCH = "mismatch"
    SKIPPED = "skipped"


class Kind(str, Enum):
    """What a mismatch looks like (a metric and a log code; every kind counts the same)."""

    VALUE = "value"
    SHIFT_PREV = "shift_prev"
    SHIFT_NEXT = "shift_next"
    SWAP = "swap"


class SkipReason(str, Enum):
    NO_TICK_BAR = "no_tick_bar"        # the TICK bar of the bucket is not in the book (in time)
    NOT_ELIGIBLE = "not_eligible"      # neither the close nor high / low is eligible
    NO_SPREAD = "no_spread"            # the TICK bucket has no usable median spread
    NO_TICK_SPEC = "no_tick_spec"      # the BROKER spec has no cross-check TICK spec


class D6Status(str, Enum):
    """Design §A4: per series, in `md:meta` and `md show`."""

    ACTIVE = "ACTIVE"
    CLOSE_ONLY = "CLOSE_ONLY"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class TickRef:
    """A closed TICK bar as the cross-check sees it: the bar (prices out of `repr`), `close_eligible` and the
    bucket's median clean spread (None: unknown; out of `repr`)."""

    bar: Bar
    close_eligible: bool
    median_spread: Decimal | None = field(default=None, repr=False)

    @property
    def complete(self) -> bool:
        return self.bar.quality is Quality.COMPLETE

    @classmethod
    def of(cls, tb: TickBar) -> TickRef:
        return cls(tb.bar, tb.close_eligible, tb.median_spread)


@dataclass(frozen=True)
class CheckResult:
    """One bucket's cross-check: the verdict, the eligible vector it ran on, a mismatch's kind or a skip reason, and
    whether the shift test ran. No price."""

    verdict: Verdict
    fields: tuple[Field, ...] = ()
    kind: Kind | None = None
    skip: SkipReason | None = None
    shift_tested: bool = False


def eligible_fields(ref: TickRef) -> tuple[Field, ...]:
    """The eligible vector (R11-M01): the close when `close_eligible`; high and low when the TICK bar is complete."""
    out: list[Field] = []
    if ref.close_eligible:
        out.append(Field.CLOSE)
    if ref.complete:
        out += [Field.HIGH, Field.LOW]
    return tuple(out)


def _value(bar: Bar, f: Field) -> Decimal:
    if f is Field.CLOSE:
        return bar.close
    return bar.high if f is Field.HIGH else bar.low


def deviation(broker: Bar, tick: Bar, fields: Iterable[Field]) -> Decimal:
    """The largest absolute difference over `fields` (memory only; never logged)."""
    return max((abs(_value(broker, f) - _value(tick, f)) for f in fields), default=Decimal(0))


def evaluate(broker: Bar, own: TickRef | None, *, k: Decimal, prev: TickRef | None = None,
             nxt: TickRef | None = None, other: TickRef | None = None) -> CheckResult:
    """The cross-check of one final BROKER bar (pure; design §A4, R11-M01, R11-M02). `own` is the TICK bar of the
    same bucket and side, `prev` / `nxt` the TICK bars of the neighbouring buckets, `other` the other side's."""
    if own is None:
        return CheckResult(Verdict.SKIPPED, skip=SkipReason.NO_TICK_BAR)
    fields = eligible_fields(own)
    if not fields:
        return CheckResult(Verdict.SKIPPED, skip=SkipReason.NOT_ELIGIBLE)
    spread = own.median_spread
    if spread is None or not spread.is_finite() or spread <= 0:
        return CheckResult(Verdict.SKIPPED, fields, skip=SkipReason.NO_SPREAD)
    tol = k * spread
    if deviation(broker, own.bar, fields) < tol:
        return CheckResult(Verdict.MATCH, fields)
    shift_tested = own.bar.high - own.bar.low >= QUIET_RANGE_SPREADS * spread
    candidates: list[tuple[Kind, TickRef | None]] = [(Kind.SWAP, other)]
    if shift_tested:
        candidates += [(Kind.SHIFT_PREV, prev), (Kind.SHIFT_NEXT, nxt)]
    passes = tol - MARGIN * tol
    best: tuple[Kind, Decimal] | None = None
    for kind, alt in candidates:
        if alt is None:
            continue
        d = deviation(broker, alt.bar, fields)
        if d <= passes and (best is None or d < best[1]):
            best = (kind, d)
    return CheckResult(Verdict.MISMATCH, fields, kind=Kind.VALUE if best is None else best[0],
                       shift_tested=shift_tested)


# ---------------------------------------------------------------------------------------------------- TICK book
class TickSource(Protocol):
    def get(self, series: SeriesId, bucket_start_ms: int) -> TickRef | None: ...


class TickRefBook:
    """The closed TICK bars of the cross-check specs, in memory (`note` is the `MdWriter.on_closed_tick` listener).
    Newest revision per bucket; `keep` buckets per series and `max_series` series (bounded; a series past the bound is
    counted and not kept)."""

    def __init__(self, *, specs: Iterable[BarSpec] = DEFAULT_TICK_SPECS, keep: int = BOOK_KEEP,
                 max_series: int = BOOK_MAX_SERIES) -> None:
        if keep < 4 or max_series < 1:
            raise ValueError("the TICK book bounds must be positive (keep at least 4 buckets)")
        self._specs = frozenset(specs)
        self._keep = keep
        self._max_series = max_series
        self._bars: dict[SeriesId, dict[int, TickRef]] = {}
        self.noted = 0
        self.refused_series = 0

    def note(self, tb: TickBar) -> None:
        series = tb.bar.series
        if series.source is not Source.TICK or series.spec not in self._specs or tb.bar.final_at_ms is None:
            return
        held = self._bars.get(series)
        if held is None:
            if len(self._bars) >= self._max_series:
                self.refused_series += 1
                return
            held = self._bars[series] = {}
        start = tb.bar.bucket_start_ms
        old = held.get(start)
        if old is not None and old.bar.revision > tb.bar.revision:
            return                                  # an older revision arriving late
        held[start] = TickRef.of(tb)
        self.noted += 1
        if len(held) > self._keep + self._keep // 4:
            for gone in sorted(held)[:len(held) - self._keep]:
                del held[gone]

    def get(self, series: SeriesId, bucket_start_ms: int) -> TickRef | None:
        held = self._bars.get(series)
        return None if held is None else held.get(bucket_start_ms)

    def series(self) -> tuple[SeriesId, ...]:
        return tuple(sorted(self._bars))


# ---------------------------------------------------------------------------------------------------- checker
FaultSink = Callable[[SeriesId, str], None]


@dataclass
class CrossCheckCounters:
    checks: int = 0
    matches: int = 0
    mismatches: int = 0
    skipped: int = 0
    value: int = 0
    shift_prev: int = 0
    shift_next: int = 0
    swap: int = 0
    shift_untested: int = 0          # mismatches whose shift test was skipped (quiet bucket)
    no_tick_bar: int = 0
    not_eligible: int = 0
    no_spread: int = 0
    no_tick_spec: int = 0
    waited: int = 0                  # live bars that waited for their TICK bar
    pending_dropped: int = 0
    history_unchecked: int = 0       # backfill / lane bars with no TICK bar in the book (not counted as skipped)
    skip_pages: int = 0
    faults: int = 0
    errors: int = 0


@dataclass(frozen=True)
class CrossCheckStatus:
    """What `md show` / `arm_status` report for one BROKER series: states and counts only."""

    series: str
    d6: D6Status
    checked: int
    mismatched: int
    skipped: int
    mismatch_streak: int
    skipped_recent: int
    window: int
    pending: int
    blocked: bool


@dataclass
class _Pending:
    bar: Bar = field(repr=False)
    live: bool
    until_ms: int


@dataclass
class _State:
    verdicts: dict[int, CheckResult] = field(default_factory=dict)
    window: dict[int, CheckResult] = field(default_factory=dict)
    pending: dict[int, _Pending] = field(default_factory=dict)
    checked: int = 0
    mismatched: int = 0
    skipped: int = 0
    streak: int = 0
    fault_sent: bool = False
    skip_paged: bool = False
    floor_ms: int | None = None          # after an operator clear: only later buckets count toward the streak
    forgotten_at_ms: int | None = None   # left demand at this time (dropped by `poll` after one timeframe)


class CrossChecker:
    """Per BROKER series: verdicts, the mismatch streak, the skipped ratio and the D6 status (design §A4)."""

    def __init__(self, ticks: TickSource, *, k: Decimal = Decimal("1.0"), mismatch_k: int = 3,
                 window: int = WINDOW, tick_wait_ms: int = TICK_WAIT_MS, pager: PagePort | None = None,
                 on_fault: FaultSink | None = None) -> None:
        if not k.is_finite() or k <= 0 or mismatch_k < 2 or window < 2 or tick_wait_ms < 0:
            raise ValueError("cross-check settings must be positive (mismatch_k at least 2: one mismatch only flags, D6)")
        self._ticks = ticks
        self._k = k
        self._mismatch_k = mismatch_k
        self._window = window
        self._tick_wait_ms = tick_wait_ms
        self._pager = pager
        self._on_fault = on_fault
        self._states: dict[SeriesId, _State] = {}
        self.counters = CrossCheckCounters()

    # -------------------------------------------------------------------------------------------- input
    def submit(self, bar: Bar, *, live: bool, now_ms: int) -> CheckResult | None:
        """A final BROKER bar `md-bars` committed (queued, or identical to the stored one). `live`: from the live
        schedule (a demanded bucket for the skipped ratio). Returns the result, or None while it waits for its TICK
        bar (live) or when a historical bar has none (not checked)."""
        series = bar.series
        if series.source is not Source.BROKER:
            return None
        st = self._states.setdefault(series, _State())
        st.forgotten_at_ms = None
        start = bar.bucket_start_ms
        st.pending.pop(start, None)
        tick = self._tick_series(series, series.side)
        if tick is not None and self._ticks.get(tick, start) is None:
            if not live:
                self.counters.history_unchecked += 1
                return None
            if len(st.pending) >= PENDING_KEEP:
                del st.pending[min(st.pending)]
                self.counters.pending_dropped += 1
            st.pending[start] = _Pending(bar, live, now_ms + self._tick_wait_ms)
            self.counters.waited += 1
            return None
        return self._check(series, st, bar, live)

    def poll(self, now_ms: int) -> None:
        """Check every waiting bar whose TICK bar has arrived; skip (`no_tick_bar`) those whose wait is over; drop the
        state of a series undemanded for longer than its grace. A failing TICK lookup is that bar's skipped check
        (`no_tick_bar`), never another bar's or another series'."""
        for series, st in list(self._states.items()):
            if st.forgotten_at_ms is not None and now_ms - st.forgotten_at_ms >= series.spec.timeframe_ms:
                del self._states[series]
                continue
            if not st.pending:
                continue
            tick = self._tick_series(series, series.side)
            for start in sorted(st.pending):
                p = st.pending[start]
                try:
                    if tick is not None and self._ticks.get(tick, start) is None and now_ms < p.until_ms:
                        continue
                except Exception as e:      # a failing TICK source: this bar is skipped, the rest go on
                    del st.pending[start]
                    self.counters.errors += 1
                    log.warning("md-bars: cross-check of %s failed (%s)", series.canonical, safe_redis_error(e))
                    self._record(series, st, start, CheckResult(Verdict.SKIPPED, skip=SkipReason.NO_TICK_BAR),
                                 p.live)
                    continue
                del st.pending[start]
                self._check(series, st, p.bar, p.live)

    def forget(self, series: SeriesId, *, now_ms: int) -> None:
        """A series no longer demanded: its waiting bars go, and its state is kept for one timeframe of the series
        (a brief lapse of demand never resets a partial streak); `poll` drops it after that, and a later demand then
        starts afresh."""
        st = self._states.get(series)
        if st is None:
            return
        st.pending.clear()
        if st.forgotten_at_ms is None:
            st.forgotten_at_ms = now_ms

    def resume(self, series: SeriesId) -> None:
        """The series is demanded again: a state kept through `forget`'s grace stays."""
        st = self._states.get(series)
        if st is not None:
            st.forgotten_at_ms = None

    def clear(self, series: SeriesId) -> None:
        """The operator cleared the series' FAILED (S30 / S31): the streak restarts and the fault re-arms. Verdicts
        and caveats stay; only buckets after the newest one already judged count toward the next streak."""
        st = self._states.get(series)
        if st is None:
            return
        if st.verdicts:
            newest = max(st.verdicts)
            st.floor_ms = newest if st.floor_ms is None else max(st.floor_ms, newest)
        st.streak = 0
        st.fault_sent = False

    # -------------------------------------------------------------------------------------------- views
    def caveat(self, series: SeriesId, bucket_start_ms: int) -> bool:
        """True when the bucket's bar mismatched its TICK bar (decisions that use it carry `source_mismatch`)."""
        st = self._states.get(series)
        r = None if st is None else st.verdicts.get(bucket_start_ms)
        return r is not None and r.verdict is Verdict.MISMATCH

    def result(self, series: SeriesId, bucket_start_ms: int) -> CheckResult | None:
        st = self._states.get(series)
        return None if st is None else st.verdicts.get(bucket_start_ms)

    def mismatch_streak(self, series: SeriesId) -> int:
        st = self._states.get(series)
        return 0 if st is None else st.streak

    def blocked(self, series: SeriesId) -> bool:
        """`K` consecutive mismatches: decision reads return `DATA_UNAVAILABLE(source_mismatch)` (S30 FAILED)."""
        return self.mismatch_streak(series) >= self._mismatch_k

    def d6(self, series: SeriesId) -> D6Status:
        st = self._states.get(series)
        if st is None:
            return D6Status.UNAVAILABLE
        ran = [r.fields for r in st.window.values() if r.verdict is not Verdict.SKIPPED]
        if any(Field.HIGH in f for f in ran):
            return D6Status.ACTIVE
        return D6Status.CLOSE_ONLY if any(Field.CLOSE in f for f in ran) else D6Status.UNAVAILABLE

    def skipped_recent(self, series: SeriesId) -> int:
        st = self._states.get(series)
        return 0 if st is None else sum(1 for r in st.window.values() if r.verdict is Verdict.SKIPPED)

    def status(self, series: SeriesId) -> CrossCheckStatus | None:
        st = self._states.get(series)
        if st is None:
            return None
        return CrossCheckStatus(series.canonical, self.d6(series), st.checked, st.mismatched, st.skipped, st.streak,
                                self.skipped_recent(series), len(st.window), len(st.pending), self.blocked(series))

    def meta_pairs(self, series: SeriesId) -> tuple[tuple[str, str], ...]:
        """`md:meta` fields of the series (states and counts only)."""
        st = self._states.get(series)
        if st is None:                              # zero counts: an earlier value never stays in `md:meta`
            return (("d6", D6Status.UNAVAILABLE.value), ("xc_checked", "0"), ("xc_k", "0"), ("xc_mismatched", "0"),
                    ("xc_skip_recent", "0"), ("xc_skipped", "0"))
        return (
            ("d6", self.d6(series).value),
            ("xc_checked", str(st.checked)),
            ("xc_k", str(st.streak)),
            ("xc_mismatched", str(st.mismatched)),
            ("xc_skip_recent", str(self.skipped_recent(series))),
            ("xc_skipped", str(st.skipped)),
        )

    # -------------------------------------------------------------------------------------------- internals
    @staticmethod
    def _tick_series(series: SeriesId, side: Side) -> SeriesId | None:
        row = timing_for(series.spec)
        if row is None or row.cross_check_tick_spec != series.spec:
            return None                             # v1 compares like with like (15m with 15m)
        return SeriesId(series.adapter_id, series.symbol, row.cross_check_tick_spec, side, Source.TICK)

    def check_only(self, bar: Bar) -> CheckResult:
        """The cross-check of one BROKER bar against the TICK book, **recorded nowhere** (no verdict, streak, counter
        or fault): the `series_reread` clear probe (plan S38e). A bar with no TICK bar in the book is SKIPPED; so is
        a malformed TICK reference (logged, not counted in `counters.errors`)."""
        return self._evaluate(bar.series, bar, count_errors=False)

    def _check(self, series: SeriesId, st: _State, bar: Bar, live: bool) -> CheckResult:
        result = self._evaluate(series, bar)
        self._record(series, st, bar.bucket_start_ms, result, live)
        return result

    def _evaluate(self, series: SeriesId, bar: Bar, *, count_errors: bool = True) -> CheckResult:
        own_series = self._tick_series(series, series.side)
        if own_series is None:
            result = CheckResult(Verdict.SKIPPED, skip=SkipReason.NO_TICK_SPEC)
        else:
            other_side = Side.ASK if series.side is Side.BID else Side.BID
            other_series = self._tick_series(series, other_side)
            start, tf = bar.bucket_start_ms, series.spec.timeframe_ms
            try:
                result = evaluate(bar, self._ticks.get(own_series, start), k=self._k,
                                  prev=self._ticks.get(own_series, start - tf),
                                  nxt=self._ticks.get(own_series, start + tf),
                                  other=None if other_series is None else self._ticks.get(other_series, start))
            except Exception as e:      # a malformed TICK reference is a skipped check, never a fault
                if count_errors:
                    self.counters.errors += 1
                log.warning("md-bars: cross-check of %s failed (%s)", series.canonical, safe_redis_error(e))
                result = CheckResult(Verdict.SKIPPED, skip=SkipReason.NO_TICK_BAR)
        return result

    def _record(self, series: SeriesId, st: _State, start: int, result: CheckResult, live: bool) -> None:
        c = self.counters
        c.checks += 1
        if result.verdict is Verdict.MATCH:
            c.matches += 1
            st.checked += 1
        elif result.verdict is Verdict.MISMATCH:
            c.mismatches += 1
            st.checked += 1
            st.mismatched += 1
            kind = result.kind or Kind.VALUE
            if kind is Kind.VALUE:
                c.value += 1
            elif kind is Kind.SWAP:
                c.swap += 1
            elif kind is Kind.SHIFT_PREV:
                c.shift_prev += 1
            else:
                c.shift_next += 1
            if not result.shift_tested:
                c.shift_untested += 1
            log.info("md-bars: %s bucket %d mismatches its TICK bar (%s; fields %s)", series.canonical, start,
                     kind.value, ",".join(f.value for f in result.fields))
        else:
            c.skipped += 1
            st.skipped += 1
            reason = result.skip or SkipReason.NO_TICK_BAR
            if reason is SkipReason.NO_TICK_BAR:
                c.no_tick_bar += 1
            elif reason is SkipReason.NOT_ELIGIBLE:
                c.not_eligible += 1
            elif reason is SkipReason.NO_SPREAD:
                c.no_spread += 1
            else:
                c.no_tick_spec += 1
        st.verdicts[start] = result
        if len(st.verdicts) > VERDICT_KEEP:
            for gone in sorted(st.verdicts)[:len(st.verdicts) - VERDICT_KEEP]:
                del st.verdicts[gone]
        if live:
            st.window[start] = result
            if len(st.window) > self._window:
                for gone in sorted(st.window)[:len(st.window) - self._window]:
                    del st.window[gone]
            self._skipped_ratio(series, st)
        self._streak(series, st)

    def _streak(self, series: SeriesId, st: _State) -> None:
        """Consecutive mismatches back from the newest checked bucket (skipped buckets neither count nor reset), over
        the buckets after the operator-clear floor."""
        n = 0
        for start in sorted(st.verdicts, reverse=True):
            if st.floor_ms is not None and start <= st.floor_ms:
                break
            v = st.verdicts[start].verdict
            if v is Verdict.SKIPPED:
                continue
            if v is Verdict.MATCH:
                break
            n += 1
        st.streak = n
        if n == 0:
            st.fault_sent = False
        if n < self._mismatch_k or st.fault_sent:
            return
        st.fault_sent = True
        self.counters.faults += 1
        log.warning("md-bars: %s has %d tick cross-check mismatches in a row (%s)", series.canonical, n,
                    SOURCE_MISMATCH)
        if self._on_fault is not None:
            try:
                self._on_fault(series, SOURCE_MISMATCH)
            except Exception as e:
                log.warning("md-bars: fault sink failed (%s)", safe_redis_error(e))

    def _skipped_ratio(self, series: SeriesId, st: _State) -> None:
        skipped = sum(1 for r in st.window.values() if r.verdict is Verdict.SKIPPED)
        if skipped <= self._window // 2:
            if skipped <= self._window // 4:        # hysteresis: the episode ends only well below half
                st.skip_paged = False
            return
        if st.skip_paged:
            return
        st.skip_paged = True
        self.counters.skip_pages += 1
        title = "tick cross-check skipped"
        body = (f"{series.canonical}: the D6 cross-check was skipped for {skipped} of the last {len(st.window)} "
                f"demanded buckets (D6 {self.d6(series).value}); only the hard checks guard this series")
        log.warning("md-bars: %s: %s", title, body)
        if self._pager is None:
            return
        try:
            self._pager.page("info", title, body)
        except Exception as e:
            log.warning("md-bars: page failed (%s)", safe_redis_error(e))


__all__ = [
    "CrossChecker", "CrossCheckCounters", "CrossCheckStatus", "CheckResult", "D6Status", "Field", "Kind",
    "SkipReason", "TickRef", "TickRefBook", "TickSource", "Verdict", "deviation", "eligible_fields", "evaluate",
    "WINDOW", "MARGIN", "QUIET_RANGE_SPREADS", "TICK_WAIT_MS",
]
