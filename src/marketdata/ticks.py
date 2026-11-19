"""The market-data tick hook (design §A8 Hook, §A4 TICK base, §A9, §A7 clock steps; plan S14).

`MdHook.on_price(dto, snapshot)` has the session's `MdHook` signature (`src/runtime/session.py`) and runs on the
parser path behind `MarketDataHookGuard` (S09; wired by S31 through `set_md_hook`). It is pure and does bounded work
per tick: no I/O, no await, no sort; it never raises on a bad tick (a bad tick is counted and flags its bucket).

Per `(adapter, symbol)`:
* a **bounded tick ring** (`ring_size`, oldest dropped and counted on overflow) that `md-writer` drains every 250 ms
  (`drain`, S15) into `md:ticks:*`. Ring overflow is writer-side shedding and never degrades a bar;
* the **validated latest quote** (tradeable ticks only; bid > 0, ask ≥ bid, broker time at most 5 s after receipt);
  a failing tick clears it, so `quote()` is None and the read port reports `STALE`;
* a short **quote ring** (round 13, plan S38d): every change of the latest quote (a new validated quote, or a clear by a
  failing tick) with its receipt time, kept `QUOTE_KEEP_MS` (at most `QUOTE_RING_SIZE` entries), so `quote_at(at_ms)`
  answers "the latest quote as it was at `at_ms`" and the PRE heads-up is frozen at its fire time however many ticks
  arrive between fire and the BarClock's read. Memory only; never stored, logged or paged;
* the **TICK bars** (`src/marketdata/tickbars.py`): tradeable, broker-timed, non-snapshot ticks build the forming
  1-minute bars per side; a **receipt-timed** tick makes its bucket `degraded`; a rejected tick (bad prices,
  future-dated) counts as a lost update of its bucket;
* the PR6 **subscription intervals** (`on_price_subscription`, the orchestrator's constructor hook; `seed_subscriptions`
  for the intervals already open when the hook is bound): a bucket is covered only when one interval spans it and the
  hook was already running at its start;
* **drop counters**: `note_drop_totals` takes the adapter's own cumulative `dropped_price_frames` (frames it dropped
  for a full channel and the lost updates, per market: the drops **before** this hook) and flags the buckets between
  two readings. S31 must not pass the orchestrator's `dropped_prices_by_market`: that adds the drops after the hook
  (a full price topic, `backpressure_overflow`), whose ticks this hook did see, and would needlessly degrade bars and
  turn off the D6 close check while the topic is full. The first reading per adapter is the baseline for every market
  it has; after it a market missing from a reading reads as 0, and a market first seen later starts at 0 from the
  adapter's last reading, so its first drops are flagged. `note_missed` flags a range in which the hook itself was
  skipped (its guard tripped).

Per adapter: a `ClockSkew` (fresh samples only) and a receipt-gap watch (on since plan S39b: `watch_receipt_gaps`,
fed by every tick and, through `note_frame`, every prices-stream heartbeat; a silence longer than the keepalive while
SUBSCRIBED blinds the TICK buckets it overlaps). `advance(now)` (the writer's loop) refreshes
the cached p50 / settle that convert local readings to broker time, finalises every bucket whose `T + p50 + settle`
has passed and blinds the buckets of a silent subscription; `clock_step(step)` (the writer's `WallMonoDetector`)
blinds the TICK buckets of the stretch at risk (the broker-time range from the last wall reading to the same
instant on the monotonic clock, mapped with the pre-step p50) plus the forming buckets, resets the forming bars,
re-anchors the receipt-gap watch and re-baselines every ClockSkew; the new wall frame is then mapped by the
re-baselined skew, so future broker minutes and past final minutes are never blinded by the step's size. A receipt
gap blinds its range; while it is open, `advance` also resets that market's forming bars (§A7).

A refused symbol (over `max_symbols_per_adapter`, or not key-safe) is remembered, so it costs O(1) per tick and counts
once in `symbols_refused`.

Nothing here logs or formats a price; counters, series ids and bucket times only.
"""
from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Final

from src.dto.entities import PriceDTO
from src.marketdata.clockstep import ClockStep, ReceiptGap, ReceiptGapDetector
from src.marketdata.skew import ClockSkew, fresh_sample
from src.marketdata.tickbars import (
    DEFAULT_RETAIN_MS,
    DEFAULT_ROLLUPS,
    TickBar,
    TickBarBuilder,
    TickOutcome,
)
from src.marketdata.timing import COLD_SKEW, SkewEstimate
from src.marketdata.types import Bar, BarSpec, Quote, SeriesId
from src.marketdata.validate import check_quote

DEFAULT_RING_SIZE: Final = 4_096
DEFAULT_MAX_SYMBOLS: Final = 64
MAX_REFUSED: Final = 1_024
MAX_CLOSED_INTERVALS: Final = 32

STATUS_TRADEABLE: Final = "tradeable"
STATUS_INDICATIVE: Final = "indicative"
STATUS_SNAPSHOT: Final = "snapshot"


@dataclass(frozen=True)
class RingTick:
    """One captured tick for `md:ticks:{adapter}:{symbol}` (prices hidden from `repr`)."""

    received_ms: int
    bid: Decimal = field(repr=False)
    ask: Decimal = field(repr=False)
    broker_ms: int
    ts_source: str          # BROKER | RECEIPT
    status: str             # tradeable | indicative | snapshot


@dataclass(frozen=True)
class TickDrain:
    """What one `drain` hands the writer for one symbol."""

    adapter_id: str
    symbol: str
    ticks: tuple[RingTick, ...]
    closed: tuple[TickBar, ...]
    ring_dropped: int = 0


@dataclass
class HookCounters:
    ticks: int = 0
    bar_ticks: int = 0
    snapshots: int = 0
    indicative: int = 0
    receipt_timed: int = 0
    rejected: int = 0
    too_late: int = 0
    late_ticks: int = 0
    ring_dropped: int = 0
    symbols_refused: int = 0
    clock_steps: int = 0


@dataclass
class AdapterClock:
    """An adapter's ClockSkew plus the p50 / settle cached by `advance` (the price path never sorts)."""

    skew: ClockSkew
    estimate: SkewEstimate = COLD_SKEW


QUOTE_KEEP_MS: Final = 15_000
"""How long a superseded quote stays answerable by `quote_at` (the PRE window is 10 s after fire, plus slack)."""
QUOTE_RING_SIZE: Final = 2_048
"""Hard bound on the quote ring per market (bounded work per tick); an `at_ms` older than the ring is unknown."""


@dataclass
class MarketState:
    """Everything the hook keeps for one `(adapter, symbol)`."""

    adapter_id: str
    symbol: str
    clock: AdapterClock
    started_ms: int
    ring: deque[RingTick]
    builder: TickBarBuilder | None = None
    quote: Quote | None = None
    quotes: deque[tuple[int, Quote | None]] = field(default_factory=lambda: deque(maxlen=QUOTE_RING_SIZE))
    """(receipt ms, the latest quote from then on: None after a failing tick), oldest first (`quote_at`)."""
    prev: tuple[Decimal, Decimal] | None = None
    sub_since_ms: int | None = None
    intervals: deque[tuple[int, int]] = field(default_factory=lambda: deque(maxlen=MAX_CLOSED_INTERVALS))
    drop_total: int | None = None
    drop_seen_ms: int | None = None
    ring_dropped: int = 0

    def covered(self, from_ms: int, to_ms: int) -> bool:
        """Subscribed throughout the broker-time range `[from_ms, to_ms)` (local = broker + p50), and the hook was
        already running at its start."""
        p50 = self.clock.estimate.p50_ms
        lo, hi = from_ms + p50, to_ms + p50
        if lo < self.started_ms:
            return False
        if self.sub_since_ms is not None and self.sub_since_ms <= lo:
            return True
        return any(since <= lo and until >= hi for since, until in self.intervals)

    def to_broker(self, local_ms: int) -> int:
        return local_ms - self.clock.estimate.p50_ms


def _remember(m: MarketState, now_ms: int, q: Quote | None) -> None:
    """Append a change of the latest quote to the ring and drop entries older than `QUOTE_KEEP_MS` (keeping the
    newest older one, which still answers `quote_at` inside the horizon). Amortised O(1) per tick."""
    ring = m.quotes
    ring.append((now_ms, q))
    horizon = now_ms - QUOTE_KEEP_MS
    while len(ring) >= 2 and ring[1][0] <= horizon:
        ring.popleft()


class MdHook:
    """The market-data hook: one instance per term (S31), shared by the price path and `md-writer`."""

    def __init__(self, *, clock_ms: Callable[[], int], ring_size: int = DEFAULT_RING_SIZE,
                 rollup_specs: Iterable[BarSpec] = DEFAULT_ROLLUPS, unfiltered: Iterable[str] = (),
                 resolution_ms: Mapping[str, int] | None = None, keepalive_ms: Mapping[str, int] | None = None,
                 retain_ms: int = DEFAULT_RETAIN_MS, max_symbols_per_adapter: int = DEFAULT_MAX_SYMBOLS) -> None:
        self._clock_ms = clock_ms
        self._ring_size = ring_size
        self._specs = tuple(rollup_specs)
        self._unfiltered = set(unfiltered)
        self._resolution = dict(resolution_ms or {})
        self._retain_ms = retain_ms
        self._max_symbols = max_symbols_per_adapter
        self._started_ms = clock_ms()
        self._markets: dict[tuple[str, str], MarketState] = {}
        self._per_adapter: dict[str, int] = {}
        self._refused: set[tuple[str, str]] = set()
        self._drop_seen: dict[str, int] = {}          # adapter -> time of its last drop reading (set = baselined)
        self._clocks: dict[str, AdapterClock] = {}
        self._gaps = ReceiptGapDetector(keepalive_ms=dict(keepalive_ms or {}))
        self.counters = HookCounters()

    # -------------------------------------------------------------------------------------------- price path
    def on_price(self, dto: PriceDTO, snapshot: bool = False) -> None:
        """One tick from the parser path. Bounded work; never raises on a bad tick."""
        now = dto.received_ms if dto.received_ms is not None else self._clock_ms()
        m = self._market(dto.adapter_id, dto.symbol, now)
        if m is None:
            return
        c = self.counters
        c.ticks += 1
        for gap in self._gaps.on_frame(dto.adapter_id, dto.symbol, now):
            self._apply_gap(gap, now)
        bid, ask = dto.bid, dto.ask
        if not check_quote(bid, ask, dto.timestamp_ms, now).ok:
            c.rejected += 1
            m.quote = None
            _remember(m, now, None)
            if m.builder is not None:
                at = m.to_broker(now)
                m.builder.mark(at, at, now, dropped=True)
            return
        broker_timed = dto.ts_source == "BROKER"
        status = STATUS_SNAPSHOT if snapshot else STATUS_TRADEABLE if dto.tradeable else STATUS_INDICATIVE
        if len(m.ring) == m.ring.maxlen:
            m.ring_dropped += 1
            c.ring_dropped += 1
        m.ring.append(RingTick(now, bid, ask, dto.timestamp_ms, "BROKER" if broker_timed else "RECEIPT", status))
        prev, m.prev = m.prev, (bid, ask)
        if dto.tradeable:
            m.quote = Quote(adapter_id=dto.adapter_id, symbol=dto.symbol, bid=bid, ask=ask,
                            broker_ms=dto.timestamp_ms, received_ms=now, broker_timed=broker_timed)
            _remember(m, now, m.quote)
        if snapshot:
            c.snapshots += 1
            return
        if not dto.tradeable:
            c.indicative += 1
            return
        if fresh_sample(broker_timed=broker_timed, tradeable=True, snapshot=False, received_ms=dto.received_ms,
                        subscribed_since_ms=m.sub_since_ms, bid=bid, ask=ask, prev=prev):
            m.clock.skew.add(now, dto.timestamp_ms)
        if m.builder is None:
            return
        if not broker_timed:
            c.receipt_timed += 1
            m.builder.note_receipt(m.to_broker(now), now)
            return
        outcome = m.builder.add_tick(bid, ask, dto.timestamp_ms, now)
        if outcome is TickOutcome.TOO_LATE:
            c.too_late += 1
            return
        c.bar_ticks += 1
        if outcome is TickOutcome.REVISED:
            c.late_ticks += 1

    # -------------------------------------------------------------------------------------------- PR6 intervals, drops
    def on_price_subscription(self, adapter_id: str, market: str, subscribed: bool, ts_ms: int) -> None:
        """The orchestrator's `on_price_subscription(adapter_id, market, subscribed, ts_ms)` (PR6). A repeated
        SUBSCRIBED restarts the interval."""
        m = self._market(adapter_id, market, ts_ms)
        if m is None:
            return
        if m.sub_since_ms is not None:
            m.intervals.append((m.sub_since_ms, ts_ms))
            m.sub_since_ms = None
        if subscribed:
            m.sub_since_ms = ts_ms
        gap = self._gaps.on_subscription(adapter_id, market, subscribed, ts_ms)
        if gap is not None:
            self._apply_gap(gap, ts_ms)

    def seed_subscriptions(self, adapter_id: str, open_since: Mapping[str, int]) -> None:
        """The intervals already open when the hook is bound (`orchestrator.price_subscriptions(adapter_id)`). The
        receipt-gap watch of a seeded market starts now, never at the interval's start (the hook saw nothing before
        it was bound, which TICK coverage already accounts for)."""
        for market, since in open_since.items():
            self.on_price_subscription(adapter_id, market, True, since)
        if open_since:
            self._gaps.on_frame(adapter_id, None, self._clock_ms())

    def watch_receipt_gaps(self, adapter_ids: Iterable[str], keepalive_ms: int) -> None:
        """Turn the receipt-gap watch on for these adapters (plan S39b, go-live gate `receipt_gap_feed`): call it
        before `seed_subscriptions` and bind `note_frame` to the orchestrator's frame hook at the same time."""
        for adapter_id in adapter_ids:
            self._gaps.set_keepalive(adapter_id, keepalive_ms)

    def note_frame(self, adapter_id: str, market: str | None, ts_ms: int) -> None:
        """An inbound frame that is not a tick (a heartbeat; market None = the whole adapter): feeds the receipt-gap
        watch only."""
        for gap in self._gaps.on_frame(adapter_id, market, ts_ms):
            self._apply_gap(gap, ts_ms)

    def note_drop_totals(self, adapter_id: str, totals: Mapping[str, int], now_ms: int) -> None:
        """The adapter's cumulative per-market drop counts (`dropped_price_frames`, the drops before this hook): an
        increase, or a counter that went back (a replaced adapter), flags the buckets since the previous reading.

        The adapter's first reading is the baseline of every market it has (a market missing from it is at 0). After
        it a market missing from a reading reads as 0, and a market created later starts at 0 from the adapter's last
        reading (`_market`), so its first non-zero total flags `[that reading, now]`."""
        first = adapter_id not in self._drop_seen
        self._drop_seen[adapter_id] = now_ms
        for m in self._markets.values():
            if m.adapter_id != adapter_id:
                continue
            total = totals.get(m.symbol, 0)
            last, seen = m.drop_total, m.drop_seen_ms
            m.drop_total, m.drop_seen_ms = total, now_ms
            if first or seen is None or total == (last or 0) or m.builder is None:
                continue
            m.builder.mark(m.to_broker(seen), m.to_broker(now_ms), now_ms, dropped=True)

    def note_missed(self, from_ms: int, to_ms: int, adapter_id: str | None = None) -> None:
        """Ticks the hook did not see in the local range (its guard skipped it): every bucket there is `degraded`."""
        now = self._clock_ms()
        for m in self._markets.values():
            if m.builder is not None and (adapter_id is None or m.adapter_id == adapter_id):
                m.builder.mark(m.to_broker(from_ms), m.to_broker(to_ms), now, dropped=True)

    # -------------------------------------------------------------------------------------------- writer loop
    def advance(self, now_ms: int) -> None:
        """The writer's periodic call: refresh the cached skew, blind silent subscriptions, finalise every bucket
        whose `T + p50 + settle` has passed."""
        for ac in self._clocks.values():
            ac.estimate = ac.skew.estimate()
        gaps = self._gaps.check(now_ms)
        for gap in gaps:
            self._apply_gap(gap, now_ms)
        for m in self._markets.values():
            if m.builder is not None:
                est = m.clock.estimate
                m.builder.finalize_until(now_ms - est.p50_ms - est.settle_ms, now_ms)
        for gap in gaps:                    # after finality: only the buckets still forming lose their prices
            if not gap.ended:
                self._reset_forming(gap.adapter_id, gap.market)

    def clock_step(self, step: ClockStep) -> None:
        """A wall-versus-monotonic step: the TICK buckets of the stretch at risk, `[last wall, last wall + monotonic
        delta]` mapped to broker time with the pre-step p50, are `blind`, the forming bars reset (and stay `blind`),
        the receipt-gap watch moves into the new wall frame, and every adapter's ClockSkew drops its samples and
        re-baselines. The wider `from_ms..to_ms` span is not blinded: in broker time it would reach future minutes
        (a forward step) or past final minutes (a backward one)."""
        self.counters.clock_steps += 1
        now = self._clock_ms()
        lo, hi = step.last_wall_ms, step.last_wall_ms + max(step.mono_delta_ms, 0)
        for m in self._markets.values():
            if m.builder is not None:
                m.builder.mark(m.to_broker(lo), m.to_broker(hi), now, blind=True)
                m.builder.reset_forming()
        self._gaps.rebase(step.delta_ms, now)
        for ac in self._clocks.values():
            ac.skew.rebaseline(step.to_ms)
            ac.estimate = ac.skew.estimate()

    def drain(self, adapter_id: str, symbol: str) -> TickDrain:
        """Take the ring's ticks and the closed bars of one symbol (the writer, every 250 ms)."""
        m = self._markets.get((adapter_id, symbol))
        if m is None:
            return TickDrain(adapter_id, symbol, (), ())
        ticks = tuple(m.ring)
        m.ring.clear()
        dropped, m.ring_dropped = m.ring_dropped, 0
        closed = m.builder.take_closed() if m.builder is not None else ()
        return TickDrain(adapter_id, symbol, ticks, closed, dropped)

    # -------------------------------------------------------------------------------------------- reads
    def markets(self) -> list[tuple[str, str]]:
        return sorted(self._markets)

    def quote(self, adapter_id: str, symbol: str) -> Quote | None:
        m = self._markets.get((adapter_id, symbol))
        return None if m is None else m.quote

    def quote_at(self, adapter_id: str, symbol: str, at_ms: int) -> Quote | None:
        """The latest validated quote **as it was at** local time `at_ms` (round 13: the PRE quote frozen at fire):
        the newest ring entry received at or before `at_ms`. None when that entry is a clear (a failing tick), when
        nothing was received by then, or when `at_ms` is older than the ring (unknown, never a later quote)."""
        m = self._markets.get((adapter_id, symbol))
        if m is None or not m.quotes:
            return None
        for received_ms, q in reversed(m.quotes):
            if received_ms <= at_ms:
                return q
        return None

    def forming(self, adapter_id: str, symbol: str) -> tuple[Bar, ...]:
        m = self._markets.get((adapter_id, symbol))
        return () if m is None or m.builder is None else m.builder.forming()

    def skew(self, adapter_id: str) -> ClockSkew:
        return self._clock(adapter_id).skew

    def skew_estimate(self, adapter_id: str) -> SkewEstimate:
        return self._clock(adapter_id).estimate

    def set_unfiltered(self, adapter_id: str, unfiltered: bool) -> None:
        """The adapter's delivery (PR3; StoneX `XTNL_STONEX_PRICE_UNFILTERED`). Filtered is the default."""
        if unfiltered:
            self._unfiltered.add(adapter_id)
        else:
            self._unfiltered.discard(adapter_id)
        for m in self._markets.values():
            if m.adapter_id == adapter_id and m.builder is not None:
                m.builder.filtered = not unfiltered

    def in_receipt_gap(self, adapter_id: str, market: str) -> bool:
        return self._gaps.in_gap(adapter_id, market)

    @property
    def receipt_gaps(self) -> int:
        """Silences found so far (each counted once, however long it lasts)."""
        return self._gaps.gaps

    # -------------------------------------------------------------------------------------------- internals
    def _clock(self, adapter_id: str) -> AdapterClock:
        ac = self._clocks.get(adapter_id)
        if ac is None:
            ac = AdapterClock(ClockSkew(adapter_id, resolution_ms=self._resolution.get(adapter_id, 0)))
            ac.estimate = ac.skew.estimate()
            self._clocks[adapter_id] = ac
        return ac

    def _market(self, adapter_id: str, symbol: str, now_ms: int) -> MarketState | None:
        key = (adapter_id, symbol)
        m = self._markets.get(key)
        if m is not None:
            return m
        if key in self._refused:
            return None
        if self._per_adapter.get(adapter_id, 0) >= self._max_symbols:
            self._refuse(key)
            return None
        try:
            SeriesId.parse(f"{adapter_id}:{symbol}:1m:BID:TICK")
        except ValueError:
            self._refuse(key)                       # not key-safe: never stored, never built
            return None
        m = MarketState(adapter_id, symbol, self._clock(adapter_id), self._started_ms,
                        deque(maxlen=self._ring_size))
        seen = self._drop_seen.get(adapter_id)
        if seen is not None:                        # created after the drop baseline: its drops count from 0
            m.drop_total, m.drop_seen_ms = 0, min(seen, now_ms)
        self._per_adapter[adapter_id] = self._per_adapter.get(adapter_id, 0) + 1
        m.builder = TickBarBuilder(adapter_id, symbol, covered=m.covered, rollup_specs=self._specs,
                                   filtered=adapter_id not in self._unfiltered,
                                   resolution_ms=self._resolution.get(adapter_id, 0), retain_ms=self._retain_ms)
        self._markets[key] = m
        return m

    def _refuse(self, key: tuple[str, str]) -> None:
        """Remember a refused symbol (bounded) and count it once."""
        if len(self._refused) < MAX_REFUSED:
            self._refused.add(key)
            self.counters.symbols_refused += 1

    def _apply_gap(self, gap: ReceiptGap, now_ms: int) -> None:
        """A receipt gap's range is `blind`. An open gap (found or extended by `advance`) also resets the market's
        forming bars (§A7), once `advance` has finalised what was due, so a bucket that only waited for the cap keeps
        its prices (and reads `blind`)."""
        m = self._markets.get((gap.adapter_id, gap.market))
        if m is not None and m.builder is not None:
            m.builder.mark(m.to_broker(gap.from_ms), m.to_broker(gap.to_ms), now_ms, blind=True)

    def _reset_forming(self, adapter_id: str, market: str) -> None:
        m = self._markets.get((adapter_id, market))
        if m is not None and m.builder is not None:
            m.builder.reset_forming()
