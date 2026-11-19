"""TICK bars: the 1-minute builder and the exact roll-ups (design §A4 TICK base, Finality, Revisions, Quality,
Cross-check eligibility; plan S14, R11-M01).

Pure and synchronous: the tick hook (`src/marketdata/ticks.py`) owns one `TickBarBuilder` per `(adapter, symbol)`
and feeds it, on the price path, only **tradeable, broker-timed, non-snapshot** ticks that passed the quote checks;
everything else reaches the builder as a flag (`mark`, `note_receipt`). Times given to the builder are broker times
(the hook converts local readings with the adapter's ClockSkew p50); `now_ms` arguments are the local clock and only
stamp `final_at_ms`.

* **Buckets** `[start, start + 60 s)` in broker time; a tick within the adapter's timestamp resolution of a minute
  boundary is assigned by its broker time and flags the bucket (`boundary`).
* **Both sides from one tick:** each minute keeps a bid and an ask OHLC and one tick count; it yields two `TickBar`s
  (series `adapter:symbol:1m:BID:TICK` and `…:ASK:TICK`).
* **Finality** (`finalize_until`): a minute is final at the first tick of a later bucket or when the hook's
  `advance(now)` passes `T + p50 + settle`. Roll-ups (`rollup_specs`, default 5m / 15m / 1h) are final with their last
  minute; they are **exact**: open of the first child with ticks, close of the last, high / low the extremes, the
  tick counts summed.
* **Revisions:** a late tick, or a flag that lands on a final minute (drops observed late, a clock step), re-emits
  the minute with `revision + 1` when its content or flags changed, and with it every final roll-up containing it.
  A tick older than the retained window (`retain_ms`, aligned to the largest roll-up so a roll-up is either fully
  kept or fully gone) is refused (`TOO_LATE`).
* **Quality** of a minute: `blind` (a clock step or receipt gap overlapped it) > `degraded` (drops or lost updates in
  it, a rejected tick, a receipt-timed tick) > `partial` (not subscribed throughout per the PR6 intervals, or the
  hook started inside it) > `complete`. A roll-up takes the worst child; a minute with no ticks counts as `no_trade`
  (no harm) when covered and unflagged, else by its flags or as `partial`. Under `filtered` delivery the quality is
  capped at `partial` (high / low may miss extremes; design §A4).
* **`close_eligible`** (R11-M01), separate from quality and possible under `filtered` delivery: the bucket's coverage
  is complete: subscribed throughout, no drops, lost updates or rejected ticks in it, not blind, and its last tick
  broker-timed (no receipt-timed tick after the last broker-timed one).
* **Median clean spread** (S17b, the cross-check tolerance unit, design §A4): every tick the builder takes is clean
  (tradeable, broker-timed, non-snapshot, validated), so each minute keeps a small histogram of its spreads
  (`ask - bid` → count, at most `SPREAD_KINDS_MAX` distinct values; past that the minute's spread is unknown). A
  bucket's `median_spread` is the lower median of its ticks' spreads (roll-ups merge their children's histograms;
  only the distinct values are sorted, at finalisation). It stays in memory (`TickBar.median_spread`, never stored,
  logged or shown).
* Writer-side ring overflow never reaches this module, so it never degrades a bar.

* `final_at_ms` is the local time the bucket became final; a revision keeps it.
* **Bounded, no sort on the price path:** the non-final minutes and the open roll-ups are kept in start order
  (`deque`s; a bucket created out of order is inserted in place), so finalisation pops from the left; the retained
  window is pruned only when its floor moves to the next largest-roll-up bucket, and only the buckets it passes.

Closed bars wait in a short hand-off buffer (`take_closed`) that `md-writer` drains every 250 ms into its own
closed-bar queue (S15). Prices never appear in a `repr`, a log or an error here.
"""
from __future__ import annotations

from bisect import insort
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Final

from src.marketdata.types import H1, M1, M5, M15, Bar, BarSpec, Quality, SeriesId, Side, Source

MINUTE_MS: Final = M1.timeframe_ms
DEFAULT_ROLLUPS: Final = (M5, M15, H1)
DEFAULT_RETAIN_MS: Final = 2 * 3_600_000
DEFAULT_MAX_PENDING: Final = 2_048
SPREAD_KINDS_MAX: Final = 64
"""Distinct spread values one minute keeps; a minute with more has no known median spread (bounded memory)."""
QUALITY_RANK: Final = {Quality.COMPLETE: 0, Quality.NO_TRADE: 0, Quality.PARTIAL: 1, Quality.DEGRADED: 2,
                       Quality.BLIND: 3}

Covered = Callable[[int, int], bool]
"""`covered(from_ms, to_ms)`: subscribed throughout the broker-time range (the hook's PR6 intervals)."""


class TickOutcome(str, Enum):
    BUILT = "built"          # into a forming (non-final) minute
    REVISED = "revised"      # a late tick into a final minute
    TOO_LATE = "too_late"    # older than the retained window: not used


def worst(a: Quality, b: Quality) -> Quality:
    return a if QUALITY_RANK[a] >= QUALITY_RANK[b] else b


@dataclass(frozen=True)
class TickBar:
    """One closed TICK bar plus its cross-check inputs (S17b): `close_eligible`, `boundary` and the bucket's median
    clean spread (None: unknown; kept out of `repr`)."""

    bar: Bar
    close_eligible: bool
    boundary: bool
    median_spread: Decimal | None = field(default=None, repr=False)


def median_of(hist: dict[Decimal, int]) -> Decimal | None:
    """The lower median of a `value → count` histogram (None when empty). Sorts only the distinct values."""
    total = sum(hist.values())
    if total <= 0:
        return None
    rank = (total + 1) // 2
    seen = 0
    for value in sorted(hist):
        seen += hist[value]
        if seen >= rank:
            return value
    return None


class _Ohlc:
    __slots__ = ("open", "high", "low", "close", "first_ms", "last_ms")

    def __init__(self, price: Decimal, ts_ms: int) -> None:
        self.open = self.high = self.low = self.close = price
        self.first_ms = self.last_ms = ts_ms

    def add(self, price: Decimal, ts_ms: int) -> None:
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        if ts_ms < self.first_ms:
            self.open, self.first_ms = price, ts_ms
        if ts_ms >= self.last_ms:
            self.close, self.last_ms = price, ts_ms

    def values(self) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        return (self.open, self.high, self.low, self.close)


class _Minute:
    """One 1-minute bucket (both sides). Internal and mutable; only the builder touches it."""

    __slots__ = ("start", "bid", "ask", "ticks", "dropped", "receipt", "last_receipt_ms", "blind", "boundary",
                 "final", "final_at_ms", "revision", "signature", "spreads")

    def __init__(self, start: int) -> None:
        self.start = start
        self.bid: _Ohlc | None = None
        self.ask: _Ohlc | None = None
        self.ticks = 0
        self.dropped = False
        self.receipt = False
        self.last_receipt_ms: int | None = None
        self.blind = False
        self.boundary = False
        self.final = False
        self.final_at_ms: int | None = None
        self.revision = -1            # -1: never emitted
        self.signature: tuple[object, ...] | None = None
        self.spreads: dict[Decimal, int] | None = {}   # None: more than SPREAD_KINDS_MAX distinct values

    def add_spread(self, spread: Decimal) -> None:
        hist = self.spreads
        if hist is None:
            return
        if spread in hist:
            hist[spread] += 1
        elif len(hist) < SPREAD_KINDS_MAX:
            hist[spread] = 1
        else:
            self.spreads = None


@dataclass
class _Rollup:
    final_at_ms: int
    revision: int = 0
    signature: tuple[object, ...] | None = None


def _insert(q: deque[int], start: int) -> None:
    """Keep `q` in start order: append in the usual case (a newer bucket), insert in place otherwise."""
    if not q or start > q[-1]:
        q.append(start)
    else:
        insort(q, start)


@dataclass(frozen=True)
class _Agg:
    """An aggregate over one or more minutes (prices kept out of `repr`)."""

    bid: tuple[Decimal, Decimal, Decimal, Decimal]
    ask: tuple[Decimal, Decimal, Decimal, Decimal]
    ticks: int
    quality: Quality
    close_eligible: bool
    boundary: bool
    median_spread: Decimal | None = None

    def signature(self) -> tuple[object, ...]:
        return (self.bid, self.ask, self.ticks, self.quality, self.close_eligible, self.boundary, self.median_spread)

    def __repr__(self) -> str:
        return f"_Agg(ticks={self.ticks}, quality={self.quality.value})"


class TickBarBuilder:
    """The TICK bars of one `(adapter, symbol)`."""

    def __init__(self, adapter_id: str, symbol: str, *, covered: Covered,
                 rollup_specs: Iterable[BarSpec] = DEFAULT_ROLLUPS, filtered: bool = True, resolution_ms: int = 0,
                 retain_ms: int = DEFAULT_RETAIN_MS, max_pending: int = DEFAULT_MAX_PENDING) -> None:
        self.adapter_id = adapter_id
        self.symbol = symbol
        self.filtered = filtered
        self.resolution_ms = max(resolution_ms, 0)
        self.retain_ms = retain_ms
        self._covered = covered
        self._specs = tuple(sorted({s for s in rollup_specs if s.timeframe_ms > MINUTE_MS}))
        self._largest = self._specs[-1] if self._specs else M1
        self._series = {(spec, side): SeriesId(adapter_id, symbol, spec, side, Source.TICK)
                        for spec in (M1, *self._specs) for side in Side}
        self._minutes: dict[int, _Minute] = {}
        self._forming: deque[int] = deque()           # non-final minute starts, in order
        self._final_through: int | None = None
        self._floor: int | None = None
        self._newest: int | None = None
        self._open_rollups: dict[BarSpec, deque[int]] = {s: deque() for s in self._specs}   # in start order
        self._open_keys: set[tuple[BarSpec, int]] = set()
        self._rollups: dict[tuple[BarSpec, int], _Rollup] = {}
        self._pending: deque[TickBar] = deque(maxlen=max_pending)
        self.pending_dropped = 0
        self.revisions = 0

    @property
    def rollup_specs(self) -> tuple[BarSpec, ...]:
        return self._specs

    @property
    def final_through_ms(self) -> int | None:
        return self._final_through

    # -------------------------------------------------------------------------------------------- input
    def add_tick(self, bid: Decimal, ask: Decimal, broker_ms: int, now_ms: int) -> TickOutcome:
        """One tradeable, broker-timed, non-snapshot, validated tick. Bounded work."""
        start = M1.bucket_start_ms(broker_ms)
        if self._floor is not None and start < self._floor:
            return TickOutcome.TOO_LATE
        self.finalize_until(start, now_ms)            # the first tick of a later bucket closes the earlier ones
        m = self._minute(start, now_ms)
        if m.bid is None or m.ask is None:
            m.bid, m.ask = _Ohlc(bid, broker_ms), _Ohlc(ask, broker_ms)
        else:
            m.bid.add(bid, broker_ms)
            m.ask.add(ask, broker_ms)
        m.ticks += 1
        m.add_spread(ask - bid)
        if self.resolution_ms and (broker_ms - start < self.resolution_ms
                                   or start + MINUTE_MS - broker_ms <= self.resolution_ms):
            m.boundary = True
        if m.final:
            self._revise(m, now_ms)
            return TickOutcome.REVISED
        return TickOutcome.BUILT

    def note_receipt(self, at_ms: int, now_ms: int) -> None:
        """A receipt-timed tick at (broker-equivalent) `at_ms`: its bucket is `degraded`, and its close is not
        broker-timed if no broker-timed tick follows."""
        start = M1.bucket_start_ms(at_ms)
        if self._floor is not None and start < self._floor:
            return
        m = self._minute(start, now_ms)
        m.receipt = True
        m.last_receipt_ms = at_ms if m.last_receipt_ms is None else max(m.last_receipt_ms, at_ms)
        if m.final:
            self._revise(m, now_ms)

    def mark(self, from_ms: int, to_ms: int, now_ms: int, *, dropped: bool = False, blind: bool = False) -> None:
        """Flag every minute overlapping the broker-time range `[from_ms, to_ms]` (clamped to the retained window)."""
        if to_ms < from_ms:
            from_ms, to_ms = to_ms, from_ms
        from_ms = max(from_ms, to_ms - self.retain_ms)
        if self._floor is not None:
            from_ms = max(from_ms, self._floor)
        if to_ms < from_ms:
            return
        start = M1.bucket_start_ms(from_ms)
        while start <= to_ms:
            m = self._minute(start, now_ms)
            changed = (dropped and not m.dropped) or (blind and not m.blind)
            m.dropped = m.dropped or dropped
            m.blind = m.blind or blind
            if changed and m.final:
                self._revise(m, now_ms)
            start += MINUTE_MS

    def reset_forming(self) -> None:
        """A clock step: every non-final minute loses its prices and stays `blind`."""
        for start in self._forming:
            m = self._minutes[start]
            m.bid = m.ask = None
            m.ticks = 0
            m.spreads = {}
            m.blind = True

    # -------------------------------------------------------------------------------------------- finality
    def finalize_until(self, cutoff_ms: int, now_ms: int) -> None:
        """Every bucket (minute and roll-up) whose end is at or before `cutoff_ms` becomes final."""
        if self._final_through is not None and cutoff_ms <= self._final_through:
            return
        self._final_through = cutoff_ms
        forming = self._forming
        while forming and forming[0] + MINUTE_MS <= cutoff_ms:
            m = self._minutes[forming.popleft()]
            m.final = True
            m.final_at_ms = now_ms
            self._emit_minute(m, now_ms)
        for spec in self._specs:
            due = self._open_rollups[spec]
            while due and spec.bucket_end_ms(due[0]) <= cutoff_ms:
                start = due.popleft()
                self._open_keys.discard((spec, start))
                state = _Rollup(final_at_ms=now_ms)
                self._rollups[(spec, start)] = state
                agg = self._aggregate(spec, start)
                if agg is not None:
                    state.signature = agg.signature()
                    self._emit(spec, start, agg, state.revision, state.final_at_ms)
        self._prune()

    # -------------------------------------------------------------------------------------------- output
    def take_closed(self) -> tuple[TickBar, ...]:
        out = tuple(self._pending)
        self._pending.clear()
        return out

    def forming(self) -> tuple[Bar, ...]:
        """The forming bars (both sides): the newest non-final minute with ticks and, for each roll-up spec, the
        bucket holding it so far. Provisional quality; `final_at_ms` None."""
        newest = next((s for s in reversed(self._forming) if self._minutes[s].ticks), None)
        if newest is None:
            return ()
        out: list[Bar] = []
        agg = self._minute_agg(self._minutes[newest])
        if agg is not None:
            out += self._bars(M1, newest, agg, 0, None)
        for spec in self._specs:
            start = spec.bucket_start_ms(newest)
            ragg = self._aggregate(spec, start)
            if ragg is not None:
                out += self._bars(spec, start, ragg, 0, None)
        return tuple(out)

    # -------------------------------------------------------------------------------------------- internals
    def _minute(self, start: int, now_ms: int) -> _Minute:
        m = self._minutes.get(start)
        if m is not None:
            return m
        m = _Minute(start)
        self._minutes[start] = m
        if self._newest is None or start > self._newest:
            self._newest = start
        if self._final_through is not None and start + MINUTE_MS <= self._final_through:
            m.final = True                          # created after its finality (a late flag or tick)
            m.final_at_ms = now_ms
        else:
            _insert(self._forming, start)
        for spec in self._specs:
            key = (spec, spec.bucket_start_ms(start))
            if key not in self._rollups and key not in self._open_keys:
                self._open_keys.add(key)
                _insert(self._open_rollups[spec], key[1])
        return m

    def _minute_quality(self, m: _Minute, covered: bool) -> Quality:
        if m.blind:
            return Quality.BLIND
        if m.dropped or m.receipt:
            return Quality.DEGRADED
        return Quality.COMPLETE if covered else Quality.PARTIAL

    def _cap(self, q: Quality) -> Quality:
        return Quality.PARTIAL if self.filtered and q is Quality.COMPLETE else q

    def _minute_agg(self, m: _Minute) -> _Agg | None:
        if m.bid is None or m.ask is None or not m.ticks:
            return None
        covered = self._covered(m.start, m.start + MINUTE_MS)
        last = m.bid.last_ms
        eligible = (covered and not m.dropped and not m.blind
                    and (m.last_receipt_ms is None or m.last_receipt_ms < last))
        median = median_of(m.spreads) if m.spreads is not None else None
        return _Agg(m.bid.values(), m.ask.values(), m.ticks, self._cap(self._minute_quality(m, covered)), eligible,
                    m.boundary, median)

    def _children(self, spec: BarSpec, start: int) -> Iterator[tuple[int, _Minute | None]]:
        for s in range(start, spec.bucket_end_ms(start), MINUTE_MS):
            yield s, self._minutes.get(s)

    def _aggregate(self, spec: BarSpec, start: int) -> _Agg | None:
        end = spec.bucket_end_ms(start)
        all_covered = self._covered(start, end)
        quality = Quality.COMPLETE
        clean = all_covered
        bid: list[Decimal] | None = None
        ask: list[Decimal] | None = None
        ticks = 0
        last_ms: int | None = None
        last_receipt: int | None = None
        boundary = False
        spreads: dict[Decimal, int] | None = {}
        for s, m in self._children(spec, start):
            covered = all_covered or self._covered(s, s + MINUTE_MS)
            if m is None:
                if not covered:
                    quality = worst(quality, Quality.PARTIAL)
                continue
            quality = worst(quality, self._minute_quality(m, covered))
            clean = clean and not m.dropped and not m.blind
            if m.last_receipt_ms is not None:
                last_receipt = m.last_receipt_ms if last_receipt is None else max(last_receipt, m.last_receipt_ms)
            if (s == start or s + MINUTE_MS == end) and m.boundary:
                boundary = True
            if m.bid is None or m.ask is None or not m.ticks:
                continue
            ticks += m.ticks
            last_ms = m.bid.last_ms
            if m.spreads is None:
                spreads = None
            elif spreads is not None:
                for value, n in m.spreads.items():
                    spreads[value] = spreads.get(value, 0) + n
            if bid is None or ask is None:
                bid, ask = list(m.bid.values()), list(m.ask.values())
                continue
            for acc, o in ((bid, m.bid), (ask, m.ask)):
                acc[1] = max(acc[1], o.high)
                acc[2] = min(acc[2], o.low)
                acc[3] = o.close
        if bid is None or ask is None or last_ms is None:
            return None
        eligible = clean and (last_receipt is None or last_receipt < last_ms)
        median = median_of(spreads) if spreads is not None else None
        return _Agg((bid[0], bid[1], bid[2], bid[3]), (ask[0], ask[1], ask[2], ask[3]), ticks, self._cap(quality),
                    eligible, boundary, median)

    def _bars(self, spec: BarSpec, start: int, agg: _Agg, revision: int, final_at_ms: int | None) -> list[Bar]:
        out = []
        for side, (o, h, low, c) in ((Side.BID, agg.bid), (Side.ASK, agg.ask)):
            out.append(Bar(series=self._series[(spec, side)], bucket_start_ms=start, open=o, high=h, low=low, close=c,
                           tick_count=agg.ticks, quality=agg.quality, revision=revision, final_at_ms=final_at_ms))
        return out

    def _emit(self, spec: BarSpec, start: int, agg: _Agg, revision: int, final_at_ms: int) -> None:
        for bar in self._bars(spec, start, agg, revision, final_at_ms):
            if len(self._pending) == self._pending.maxlen:
                self.pending_dropped += 1
            self._pending.append(TickBar(bar, agg.close_eligible, agg.boundary, agg.median_spread))

    def _emit_minute(self, m: _Minute, now_ms: int) -> None:
        agg = self._minute_agg(m)
        if agg is None:
            return
        sig = agg.signature()
        if m.signature == sig:
            return
        m.revision += 1
        m.signature = sig
        if m.final_at_ms is None:                   # a revision keeps the time the minute became final
            m.final_at_ms = now_ms
        self._emit(M1, m.start, agg, m.revision, m.final_at_ms)

    def _revise(self, m: _Minute, now_ms: int) -> None:
        """A change on a final minute: re-emit it and every final roll-up holding it when their content changed."""
        before = m.revision
        self._emit_minute(m, now_ms)
        if m.revision != before and before >= 0:
            self.revisions += 1
        for spec in self._specs:
            rstart = spec.bucket_start_ms(m.start)
            state = self._rollups.get((spec, rstart))
            if state is None:
                continue
            agg = self._aggregate(spec, rstart)
            if agg is None:
                continue
            sig = agg.signature()
            if sig == state.signature:
                continue
            if state.signature is not None:
                state.revision += 1
                self.revisions += 1
            state.signature = sig
            self._emit(spec, rstart, agg, state.revision, state.final_at_ms)

    def _prune(self) -> None:
        """Drop final minutes and roll-up states older than the retained window, aligned to the largest roll-up.
        Runs only when the floor moves (once per largest-roll-up bucket) and visits only the buckets it passes (a
        full scan when that is shorter: the first move, a long silence). A non-final minute is never below the floor
        (the floor is at most the start of the largest bucket holding `final_through`)."""
        if self._final_through is None:
            return
        ref = self._final_through if self._newest is None else max(self._newest, self._final_through)
        floor = self._largest.bucket_start_ms(ref - self.retain_ms)
        floor = min(floor, self._largest.bucket_start_ms(self._final_through))
        old = self._floor
        if old is not None and floor <= old:
            return
        self._floor = floor
        if old is None or (floor - old) // MINUTE_MS > len(self._minutes):
            gone = [s for s, m in self._minutes.items() if s < floor and m.final]
        else:
            gone = [s for s in range(old, floor, MINUTE_MS) if s in self._minutes and self._minutes[s].final]
        for s in gone:
            del self._minutes[s]
        if old is None or len(self._specs) * ((floor - old) // MINUTE_MS) > len(self._rollups):
            keys = [k for k in self._rollups if k[1] < floor]
        else:
            keys = [(spec, s) for spec in self._specs
                    for s in range(spec.bucket_start_ms(old), floor, spec.timeframe_ms)]
        for key in keys:
            self._rollups.pop(key, None)
