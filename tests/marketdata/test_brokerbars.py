"""`md-bars`: BROKER bar schedule, finality, verification re-read, error unit, backfill lane, gap heal, broker-outage
pause and the `md_backfill` work item (plan S17a; design §A4, §A8, §A7 SM12, §B5.2, §B14). A fake broker (synthetic
instruments `mock:SYN1` / `mock:SYN2`, synthetic prices) behind `BrokerBarsPort`; the real `SeriesStateBook`; and one
integration test over fakeredis with the real `MdWriter`. No price appears in an assertion message."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from src.calendar.machine import SessionState
from src.dto.bars import BrokerBar, PriceBarsResponse
from src.marketdata.bootstrap import SeriesStateBook, Sm12Event
from src.marketdata.brokerbars import (
    BrokerBarsSettings,
    Coverage,
    MdBars,
    Phase,
    Resolution,
    StoreBarsReader,
    StoredBar,
    demanded,
)
from src.marketdata.config import parse_md_settings
from src.marketdata.events import RefetchQueue
from src.marketdata.hashing import content_hash
from src.marketdata.store.reads import StoreReader
from src.marketdata.timing import COLD_SKEW, SkewEstimate
from src.marketdata.types import M15, Bar, SeriesId, Side, Sm12State, Source, Transient, Unsupported

from .bootrig import make_rig
from .conftest import KEYRING, RunIds

TF = 900_000
AD = "mock"
S1 = SeriesId(AD, "SYN1", M15, Side.BID, Source.BROKER)
S2 = SeriesId(AD, "SYN2", M15, Side.BID, Source.BROKER)
T = 1_700_000_100_000 - 1_700_000_100_000 % TF + 10 * TF      # a bar end (broker time), a weekday
B = T - TF                                                       # the bucket of bar end T
PRICES = ("1.1000", "1.1010", "1.0990", "1.1005", "1.1007")


def _utc_ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=timezone.utc).timestamp() * 1000)


FRIDAY_CLOSE = _utc_ms(2023, 11, 17, 22, 0)      # Friday 17:00 New York (EST)
DAILY_BREAK = _utc_ms(2023, 11, 15, 22, 0)       # Wednesday 17:00 New York (EST)


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


def bb(start: int, close: str = "1.1005", *, high: str = "1.1010", partial: bool = False) -> BrokerBar:
    return BrokerBar(bucket_start_ms=start, open=Decimal("1.1000"), high=Decimal(high), low=Decimal("1.0990"),
                     close=Decimal(close), partial=partial)


@dataclass
class Broker:
    """A fake broker: a closed bar is visible from its bucket end + `lag_ms`; the bucket in progress is returned as a
    forming bar when `forming`; a `hidden` bucket is left out and the answer marked truncated."""

    clock: Clock
    bars: dict[tuple[str, int], BrokerBar] = field(default_factory=dict)
    lag_ms: int = 0
    forming: bool = True
    hidden: set[int] = field(default_factory=set)
    fail: Callable[[str, int, int], object] | None = None
    calls: list[tuple[int, str, int, int]] = field(default_factory=list)

    def put(self, start: int, symbol: str = "SYN1", **kw: Any) -> None:
        self.bars[(symbol, start)] = bb(start, **kw)

    def fill(self, lo: int, hi: int, symbol: str = "SYN1") -> None:
        for start in range(lo, hi + 1, TF):
            self.put(start, symbol)

    async def fetch_price_bars(self, adapter_id: str, symbol: str, side: Side, timeframe_s: int, *,
                               count: int | None = None, from_ms: int | None = None,
                               to_ms: int | None = None) -> Any:
        assert from_ms is not None and to_ms is not None and count is None
        now = self.clock.now
        self.calls.append((now, symbol, from_ms, to_ms))
        if self.fail is not None:
            out = self.fail(symbol, from_ms, to_ms)
            if isinstance(out, Exception):
                raise out
            if out is not None:
                return out
        tf = timeframe_s * 1000
        bars: list[BrokerBar] = []
        truncated = False
        for start in range(from_ms, to_ms + 1, tf):
            if start in self.hidden:
                truncated = True
                continue
            end = start + tf
            bar = self.bars.get((symbol, start))
            if bar is not None and now >= end + self.lag_ms:
                bars.append(bar)
            elif start <= now < end and self.forming:
                bars.append(bb(start, partial=True))
        return PriceBarsResponse(adapter_id=adapter_id, symbol=symbol, side=side.value, timeframe_s=timeframe_s,
                                 bars=tuple(bars), truncated=truncated, received_ms=now, from_ms=from_ms,
                                 to_ms=to_ms)


@dataclass
class Sink:
    queued: list[Bar] = field(default_factory=list)
    quarantined: list[tuple[Bar, str]] = field(default_factory=list)
    meta: dict[SeriesId, dict[str, str]] = field(default_factory=dict)

    def queue_bar(self, bar: Bar) -> None:
        self.queued.append(bar)

    def quarantine_stored(self, bar: Bar, check: str, member: str = "") -> bool:
        self.quarantined.append((bar, check))
        return True

    def set_meta(self, series: SeriesId, pairs: Sequence[tuple[str, str]]) -> bool:
        self.meta.setdefault(series, {}).update(pairs)
        return True

    def is_queued(self, series: SeriesId, bucket_start_ms: int) -> bool:
        return False

    def of(self, series: SeriesId, start: int) -> list[Bar]:
        return [b for b in self.queued if b.series == series and b.bucket_start_ms == start]


@dataclass
class Stored:
    """`StoredBarsPort` fed from what the sink queued (an instant writer); `drop` hides stored buckets."""

    sink: Sink
    down: bool = False
    drop: set[int] = field(default_factory=set)

    def _latest(self, series: SeriesId) -> dict[int, Bar]:
        out: dict[int, Bar] = {}
        for b in self.sink.queued:
            if b.series == series and b.bucket_start_ms not in self.drop:
                out[b.bucket_start_ms] = b
        return out

    async def stored(self, series: SeriesId, from_ms: int, to_ms: int) -> Mapping[int, StoredBar] | None:
        if self.down:
            return None
        return {s: StoredBar(content_hash(KEYRING, series.canonical, b.bucket_end_ms, b.open, b.high, b.low, b.close),
                             b.revision)
                for s, b in self._latest(series).items() if from_ms <= s <= to_ms}

    async def span(self, series: SeriesId) -> tuple[int, int] | None:
        held = self._latest(series)
        return (min(held), max(held)) if held else None


@dataclass
class Health:
    streaming: bool = True
    failing: Callable[[int], bool | None] = lambda now: False
    clock: Clock | None = None

    def is_streaming(self, adapter_id: str) -> bool:
        return self.streaming

    def rest_failing_everywhere(self, adapter_id: str) -> bool | None:
        assert self.clock is not None
        return self.failing(self.clock.now)


@dataclass
class Pages:
    sent: list[tuple[str, str, str]] = field(default_factory=list)

    def page(self, severity: str, title: str, body: str) -> None:
        self.sent.append((severity, title, body))

    def titled(self, title: str) -> int:
        return sum(1 for _, t, _ in self.sent if t == title)


@dataclass
class Calendar:
    state_: SessionState | None = SessionState.OPEN

    def state(self, adapter_id: str, symbol: str) -> SessionState | None:
        return self.state_


@dataclass
class Rig:
    bars: MdBars
    broker: Broker
    sink: Sink
    book: SeriesStateBook
    clock: Clock
    pages: Pages
    faults: list[tuple[SeriesId, str]]
    resolved: list[tuple[SeriesId, int, Resolution]]
    completed: list[tuple[int, str, str]]
    stored: Stored | None

    async def run_to(self, until: int, every: int = 1_000) -> None:
        while self.clock.now <= until:
            await self.bars.step()
            self.clock.now += every


def make(*, start: int = T - 60_000, need: int = 4, series: Sequence[SeriesId] = (S1,), stored: bool = True,
         history: bool = True, health: Health | None = None, calendar: Calendar | None = None,
         refetch: RefetchQueue | None = None, trusts: bool = False, next_bucket: bool = True, **cfg: Any) -> Rig:
    """`next_bucket`: the PR3-trusted path (`T + 1` in the same answer makes `T` final); production keeps it off
    until the PR3 probe (round 13), `test_round13_md.py` covers the default."""
    clock = Clock(start)
    broker = Broker(clock)
    if history:
        for s in series:
            broker.fill(T - 40 * TF, T + 20 * TF, s.symbol)
    sink = Sink()
    book = SeriesStateBook()
    pages = Pages()
    faults: list[tuple[SeriesId, str]] = []
    resolved: list[tuple[SeriesId, int, Resolution]] = []
    completed: list[tuple[int, str, str]] = []
    store = Stored(sink) if stored else None
    if health is not None:
        health.clock = clock
    settings = BrokerBarsSettings(**({"lane_retry_min_ms": 1_000, "lane_retry_max_ms": 4_000} | cfg))
    bars = MdBars(broker, sink, book, keyring=KEYRING, demand=demanded((s, need) for s in series), settings=settings,
                  stored=store, health=health, calendar=calendar, pager=pages, refetch=refetch,
                  trusts_partial_flag=(lambda a: trusts), trusts_next_bucket=(lambda a: next_bucket),
                  on_fault=lambda s, why: faults.append((s, why)),
                  complete=lambda r, o, why: completed.append((r, o, why)), clock_ms=clock)
    bars.on_resolved(lambda s, b, how: resolved.append((s, b, how)))
    return Rig(bars, broker, sink, book, clock, pages, faults, resolved, completed, store)


def live_calls(rig: Rig, bucket: int, symbol: str = "SYN1") -> list[int]:
    return [at for at, sym, lo, _ in rig.broker.calls if sym == symbol and lo == bucket]


# ---------------------------------------------------------------------------------------------------- schedule
async def test_first_read_at_bar_end_plus_p50_plus_settle() -> None:
    rig = make()
    await rig.run_to(T + 4_000)
    assert live_calls(rig, B) == []                      # never before T + p50 + settle (cold: 0 + 5 s)
    await rig.run_to(T + 5_000)
    assert live_calls(rig, B) == [T + 5_000]


async def test_reread_schedule_until_the_cap_then_lane() -> None:
    rig = make()
    rig.broker.hidden.add(B)                             # never answered: blind until the cap
    await rig.run_to(T + 905_000)
    reads = live_calls(rig, B)
    assert reads[:12] == [T + k * 5_000 for k in range(1, 13)]                  # every 5 s until T + post_grace
    assert [r for r in reads if T + 60_000 < r <= T + 300_000] == [T + 60_000 + k * 30_000 for k in range(1, 9)]
    assert [r for r in reads if T + 300_000 < r < T + 905_000] == [T + 300_000 + k * 60_000 for k in range(1, 11)]
    assert B in rig.bars.lane(S1)                        # at the cap the blind bucket moved to the lane
    assert rig.bars.counters.capped_unresolved == 1 and rig.bars.counters.capped_invalid == 0


async def test_lag_setting_only_starts_the_back_off() -> None:
    rig = make(lag_ms=90_000)
    rig.broker.hidden.add(B)
    await rig.run_to(T + 100_000)
    reads = live_calls(rig, B)
    assert reads[0] == T + 5_000                        # the first read is still at T + p50 + settle
    assert T + 90_000 in reads                           # the 5 s cadence lasts until T + max(post_grace, lag)


# ---------------------------------------------------------------------------------------------------- finality
async def test_final_on_a_later_bucket_in_the_same_answer() -> None:
    rig = make()
    await rig.run_to(T + 5_000)
    [bar] = rig.sink.of(S1, B)
    assert bar.revision == 0 and bar.final_at_ms == T + 5_000 and bar.tick_count == 0
    assert rig.book.state(S1) is Sm12State.LIVE


async def test_forming_bar_is_never_stored_or_counted() -> None:
    rig = make()
    await rig.run_to(T + 10_000)
    assert rig.sink.of(S1, T) == []                      # the bucket in progress was evidence only
    assert rig.bars.counters.forming_seen > 0
    assert rig.bars.counters.invalid_bars == 0 and rig.sink.quarantined == []


@pytest.mark.parametrize("bar_end", [DAILY_BREAK, FRIDAY_CLOSE], ids=["daily_break", "friday_close"])
async def test_finality_at_break_and_close_by_two_agreeing_reads(bar_end: int) -> None:
    rig = make(start=bar_end - 60_000)
    rig.broker.forming = False                           # market closed after T: no later bucket in any answer
    rig.broker.fill(bar_end - 40 * TF, bar_end - TF)
    for start in range(bar_end, bar_end + 10 * TF, TF):
        rig.broker.bars.pop(("SYN1", start), None)
    await rig.run_to(bar_end + 14_000)
    assert rig.sink.of(S1, bar_end - TF) == []           # two reads only 5 s apart: settle is 10 s
    await rig.run_to(bar_end + 15_000)
    [bar] = rig.sink.of(S1, bar_end - TF)
    assert bar.final_at_ms == bar_end + 15_000           # reads at +5 s and +15 s agreed


async def test_changed_second_read_restarts_the_agreement() -> None:
    rig = make()
    rig.broker.forming = False
    for start in range(T, T + 5 * TF, TF):
        rig.broker.bars.pop(("SYN1", start), None)
    await rig.run_to(T + 5_000)
    rig.broker.put(B, close="1.1007")                    # the broker bar changed between reads
    await rig.run_to(T + 15_000)
    assert rig.sink.of(S1, B) == []
    await rig.run_to(T + 20_000)
    [bar] = rig.sink.of(S1, B)
    assert bar.final_at_ms == T + 20_000 and bar.close == Decimal(PRICES[4])


async def test_trusted_partial_flag_clear_is_final_at_once() -> None:
    rig = make(trusts=True)
    rig.broker.forming = False
    await rig.run_to(T + 5_000)
    assert len(rig.sink.of(S1, B)) == 1


async def test_empty_bucket_no_trade_needs_a_later_closed_bar_or_the_cap() -> None:
    rig = make()
    rig.broker.bars.pop(("SYN1", B))
    await rig.run_to(T + 5_000)
    assert B not in rig.bars.coverage(S1)                # a forming bar alone may precede a lagging bar
    await rig.run_to(T + 905_000, every=5_000)
    assert rig.bars.coverage(S1).get(B) is Coverage.NO_TRADE and rig.sink.of(S1, B) == []
    assert rig.bars.counters.capped_unresolved == 0 and B not in rig.bars.lane(S1)
    rig3 = make()
    rig3.broker.bars.pop(("SYN1", B))
    rig3.broker.hidden.add(B)                            # truncated answer: nothing learned (blind)
    await rig3.run_to(T + 905_000, every=5_000)
    assert B not in rig3.bars.coverage(S1) and B in rig3.bars.lane(S1)


async def test_empty_bucket_before_a_closed_bar_is_no_trade_at_once() -> None:
    rig = make(start=T + 5_000)
    rig.broker.bars.pop(("SYN1", B - TF))                # bar end T - 15 min empty, bar end T closed
    await rig.run_to(T + 5_000)
    assert rig.bars.coverage(S1).get(B - TF) is Coverage.NO_TRADE


async def test_closed_calendar_and_empty_complete_answer_is_no_trade() -> None:
    rig = make(calendar=Calendar(SessionState.CLOSED))
    rig.broker.forming = False
    rig.broker.bars.pop(("SYN1", B))
    await rig.run_to(T + 5_000)
    assert rig.bars.coverage(S1).get(B) is Coverage.NO_TRADE


# ---------------------------------------------------------------------------------------------------- verification
async def test_verification_reread_finds_a_change_and_writes_revision_one() -> None:
    rig = make()
    await rig.run_to(T + 30_000)
    rig.broker.put(B, close="1.1007")
    await rig.run_to(T + 70_000)
    bars = rig.sink.of(S1, B)
    assert [b.revision for b in bars] == [0, 1]
    assert bars[1].final_at_ms == T + 65_000             # min(final + 60 s, cap - 30 s)
    assert rig.bars.counters.revisions == 1
    assert live_calls(rig, B) == [T + 5_000, T + 65_000]


async def test_verification_reread_unchanged_writes_nothing() -> None:
    rig = make()
    await rig.run_to(T + 70_000)
    assert len(rig.sink.of(S1, B)) == 1 and rig.bars.counters.bars_unchanged >= 1
    assert T + 65_000 in live_calls(rig, B)


async def test_unverified_final_when_no_verification_time_remains() -> None:
    rig = make(start=T - 5_000)
    rig.broker.lag_ms = 880_000                          # the bar appears 20 s before its cap
    await rig.run_to(T + 905_000, every=5_000)
    [bar] = rig.sink.of(S1, B)
    assert bar.final_at_ms == T + 900_000
    assert B in rig.bars.unverified(S1)


# ---------------------------------------------------------------------------------------------------- error unit
async def test_three_undecodable_answers_then_a_valid_bar_leave_k_zero() -> None:
    rig = make()
    left = [3]

    def fail(symbol: str, lo: int, hi: int) -> object:
        if lo == B and left[0] > 0:
            left[0] -= 1
            return Transient("decode_error")
        return None

    rig.broker.fail = fail
    await rig.run_to(T + 20_000)
    assert len(rig.sink.of(S1, B)) == 1                  # 4th read at T + 20 s is final
    status = rig.bars.status(S1)
    assert status is not None and status.invalid_streak == 0 and status.unresolved_streak == 0
    assert rig.book.state(S1) is Sm12State.LIVE
    assert rig.bars.counters.decode_errors == 3
    assert rig.pages.titled("broker bar reads failing") == 1          # one page per episode
    assert rig.faults == []


async def test_corrupt_bar_quarantined_at_once_and_k_buckets_fault() -> None:
    rig = make()
    for k in range(3):
        rig.broker.put(B + k * TF, high="1.0995")        # high below close: check 1 fails
    await rig.run_to(T + 5_000)
    assert [(b.bucket_start_ms, check) for b, check in rig.sink.quarantined] == [(B, "prices")]
    assert rig.sink.of(S1, B) == []                      # blind, never stored
    await rig.run_to(T + 906_000, every=5_000)
    assert len(rig.sink.quarantined) == 2                # once per bucket, however many re-reads
    assert B in rig.bars.lane(S1)
    status = rig.bars.status(S1)
    assert status is not None and status.invalid_streak == 1 and rig.faults == []
    await rig.run_to(T + 2 * TF + 906_000, every=5_000)
    assert rig.faults == [(S1, "invalid_bar")]          # once, at the third invalid bucket's cap
    assert rig.bars.counters.capped_invalid == 3 and rig.bars.counters.faults == 1


async def test_a_clean_bucket_resets_the_streaks() -> None:
    rig = make()
    for k in (0, 2, 3):                                  # invalid, clean, invalid, invalid: never 3 in a row
        rig.broker.put(B + k * TF, high="1.0995")
    await rig.run_to(T + 3 * TF + 900_000, every=5_000)
    status = rig.bars.status(S1)
    assert rig.bars.counters.capped_invalid == 2 and status is not None and status.invalid_streak == 1
    await rig.run_to(T + 3 * TF + 905_000, every=5_000)
    assert rig.bars.counters.capped_invalid == 3 and rig.faults == []


@pytest.mark.parametrize("failing, counted", [(False, True), (None, False)], ids=["poll_ok", "unknown"])
async def test_bar_reads_failing_count_only_while_other_priorities_succeed(failing: bool | None,
                                                                            counted: bool) -> None:
    rig = make(health=Health(failing=lambda now: failing))
    rig.broker.fail = lambda symbol, lo, hi: Transient("timeout") if lo == B else None
    await rig.run_to(T + 900_000, every=2_000)
    assert rig.pages.titled("broker bar reads failing") == 1          # one page for the episode
    await rig.run_to(T + 906_000, every=2_000)
    assert rig.bars.counters.capped_unresolved == (1 if counted else 0)
    assert rig.bars.counters.capped_uncounted == (0 if counted else 1)
    assert rig.pages.titled("broker bars unavailable") == 0
    assert rig.faults == []


async def test_thirty_minute_rest_outage_pauses_and_never_fails() -> None:
    down = (T - 30_000, T + 30 * 60_000)
    health = Health(failing=lambda now: down[0] <= now < down[1])
    rig = make(health=health)
    await rig.run_to(T + 15 * 60_000, every=5_000)
    assert rig.pages.titled("broker bars unavailable") == 1
    assert rig.book.state(S1) is Sm12State.GAPPED
    status = rig.bars.status(S1)
    assert status is not None and status.phase is Phase.PAUSED
    await rig.run_to(T + 45 * 60_000, every=5_000)
    assert rig.pages.titled("broker bars unavailable") == 1          # one per episode
    assert rig.faults == []
    status = rig.bars.status(S1)
    assert status is not None and status.invalid_streak == 0 and status.unresolved_streak == 0
    assert rig.bars.counters.capped_uncounted >= 1
    assert rig.book.state(S1) is Sm12State.LIVE                       # backfilled again after the outage
    assert rig.bars.lane(S1) == frozenset()                           # outage buckets re-fetched in the lane
    assert rig.sink.of(S1, B)                                         # the bar of the outage is stored


async def test_sleep_longer_than_the_cap_counts_nothing() -> None:
    rig = make()
    rig.broker.hidden.update(B + k * TF for k in range(12))
    await rig.run_to(T + 5_000)
    rig.clock.now = T + 2 * 3_600_000                    # the PC slept for two hours
    await rig.run_to(T + 2 * 3_600_000 + 2_000)
    assert rig.bars.counters.capped_unresolved == 0 and rig.bars.counters.capped_invalid == 0
    assert rig.faults == []
    assert B in rig.bars.lane(S1)                        # the missed bucket is re-fetched, not counted


async def test_adapter_not_streaming_is_an_outage() -> None:
    rig = make(health=Health(streaming=False))
    await rig.run_to(T + 10_000)
    assert rig.pages.titled("broker bars unavailable") == 1 and rig.sink.queued == []


# ---------------------------------------------------------------------------------------------------- containment
@pytest.mark.parametrize("bad", ["raise", "garbage", "nan", "wrong_symbol"])
async def test_one_series_malformed_payload_leaves_others_fetching(bad: str) -> None:
    rig = make(series=(S1, S2))

    def fail(symbol: str, lo: int, hi: int) -> object:
        if symbol != "SYN1":
            return None
        if bad == "raise":
            return RuntimeError("boom")
        if bad == "garbage":
            return "not a response"
        nan = BrokerBar(bucket_start_ms=lo, open=Decimal("NaN"), high=Decimal("NaN"), low=Decimal("NaN"),
                        close=Decimal("NaN"))
        symbol_out = "OTHER" if bad == "wrong_symbol" else "SYN1"
        bars = (bb(lo),) if bad == "wrong_symbol" else (nan,)
        return PriceBarsResponse(adapter_id=AD, symbol=symbol_out, side="BID", timeframe_s=900, bars=bars,
                                 truncated=False, received_ms=rig.clock.now, from_ms=lo, to_ms=hi)

    rig.broker.fail = fail
    await rig.run_to(T + 10_000)
    assert len(rig.sink.of(S2, B)) == 1 and rig.book.state(S2) is Sm12State.LIVE
    assert rig.sink.of(S1, B) == []


async def test_unsupported_adapter_stops_reading() -> None:
    rig = make()
    rig.broker.fail = lambda symbol, lo, hi: Unsupported("no bars")
    await rig.run_to(T + 10_000)
    n = len(rig.broker.calls)
    await rig.run_to(T + 60_000)
    assert len(rig.broker.calls) == n
    status = rig.bars.status(S1)
    assert status is not None and status.phase is Phase.UNSUPPORTED


async def test_global_failed_stops_md_bars() -> None:
    rig = make()
    rig.bars._global_failed = lambda: True               # noqa: SLF001 - the S30 wiring
    await rig.run_to(T + 10_000)
    assert rig.broker.calls == [] and rig.sink.queued == []


async def test_md_backfill_is_applied_while_globally_failed_and_nothing_else_is_read() -> None:
    """S32 (design §B13, §B14): while the ARM is globally FAILED md-bars makes no live, re-fetch or gap-heal read
    and writes nothing, except an accepted `md_backfill` (a repair), which is read and written in the lane and
    completed."""
    rig = make()
    await rig.run_to(T - 58_000)
    failed = [True]
    rig.bars._global_failed = lambda: failed[0]          # noqa: SLF001 - the S32 wiring
    calls, n = len(rig.broker.calls), len(rig.sink.queued)
    await rig.run_to(T + 10_000)                         # the live read of B would be due at T + 5 s
    assert len(rig.broker.calls) == calls and len(rig.sink.queued) == n and live_calls(rig, B) == []
    assert rig.bars.counters.failed_passes > 0
    rig.broker.put(B - 20 * TF, close="1.1007")          # history the store never had
    assert rig.bars.request_backfill(7, S1, B - 21 * TF, B - 19 * TF) is None
    await rig.run_to(T + 20_000)
    assert rig.completed == [(7, "applied", "resolved")] and len(rig.sink.queued) == n + 3
    assert live_calls(rig, B) == []                      # still no live read
    failed[0] = False                                    # cleared: the live schedule resumes
    await rig.run_to(T + 30_000)
    assert live_calls(rig, B) != []


# ---------------------------------------------------------------------------------------------------- store compare
async def test_identical_bar_already_stored_is_not_written_again() -> None:
    rig = make()
    await rig.run_to(T + 5_000)
    n = len(rig.sink.queued)
    rig2 = make(need=4)                                  # a new term: backfill re-fetches what is stored
    rig2.sink.queued = list(rig.sink.queued)
    await rig2.run_to(T + 5_000)
    assert len(rig2.sink.queued) == n
    assert rig2.bars.counters.bars_unchanged >= n


async def test_changed_stored_bar_is_written_as_the_next_revision() -> None:
    rig = make()
    old = Bar(series=S1, bucket_start_ms=B - 2 * TF, open=Decimal("1.1000"), high=Decimal("1.1010"),
              low=Decimal("1.0990"), close=Decimal("1.1007"), revision=2, final_at_ms=T)
    rig.sink.queued.append(old)
    await rig.run_to(T - 59_000)
    assert [b.revision for b in rig.sink.of(S1, B - 2 * TF)] == [2, 3]


async def test_store_unreadable_defers_the_write() -> None:
    rig = make()
    assert rig.stored is not None
    rig.stored.down = True
    await rig.run_to(T + 5_000)
    assert rig.sink.of(S1, B) == [] and rig.bars.counters.deferred >= 1
    rig.stored.down = False
    await rig.run_to(T + 15_000)
    assert len(rig.sink.of(S1, B)) == 1


# ---------------------------------------------------------------------------------------------------- backfill
async def test_backfill_reads_newest_first_and_goes_live() -> None:
    rig = make(need=10, page_bars=4)
    assert rig.book.state(S1) is Sm12State.EMPTY
    await rig.run_to(T - 58_000)
    pages = [lo for at, _, lo, hi in rig.broker.calls if lo < B - TF]
    assert pages == sorted(pages, reverse=True) and len(pages) == 3
    hi = B - 2 * TF                                      # the window ends before the live bucket
    for k in range(10):
        assert len(rig.sink.of(S1, hi - k * TF)) == 1
    assert rig.book.state(S1) is Sm12State.LIVE
    status = rig.bars.status(S1)
    assert status is not None and status.backfilled


async def test_backfill_progress_is_readable_while_backfilling() -> None:
    rig = make(need=10, page_bars=4)
    rig.broker.fail = lambda symbol, lo, hi: Transient("timeout") if lo < B - 6 * TF else None
    await rig.run_to(T - 59_000)
    assert rig.book.state(S1) is Sm12State.BACKFILLING
    assert rig.book.readable(S1, B - 5 * TF, B - 2 * TF)
    assert not rig.book.readable(S1, B - 9 * TF, B - 2 * TF)


async def test_gapped_series_backfills_again() -> None:
    rig = make()
    await rig.run_to(T + 5_000)
    assert rig.book.state(S1) is Sm12State.LIVE
    rig.book.apply(S1, Sm12Event.GAP)
    await rig.run_to(T + 7_000)
    assert rig.book.state(S1) is Sm12State.LIVE
    assert rig.bars.counters.backfill_done == 2


async def test_failed_series_is_not_read() -> None:
    rig = make()
    await rig.run_to(T - 58_000)
    rig.book.apply(S1, Sm12Event.FAIL)
    n = len(rig.broker.calls)
    await rig.run_to(T + 10_000)
    assert len(rig.broker.calls) == n


# ---------------------------------------------------------------------------------------------------- lane
async def test_blind_bucket_at_cap_is_resolved_in_the_lane() -> None:
    rig = make()
    rig.broker.hidden.add(B)
    await rig.run_to(T + 905_000, every=5_000)
    assert B in rig.bars.lane(S1) and rig.sink.of(S1, B) == []
    rig.broker.hidden.discard(B)
    await rig.run_to(T + 912_000)
    assert (S1, B, Resolution.BAR) in rig.resolved      # bucket_resolved (S18 recomputes ATR on it)
    assert len(rig.sink.of(S1, B)) == 1 and B not in rig.bars.lane(S1)


async def test_lane_bucket_given_up_as_unfillable() -> None:
    rig = make(lane_max_misses=3)
    rig.broker.hidden.add(B)
    await rig.run_to(T + 905_000, every=5_000)
    await rig.run_to(T + 960_000)
    assert rig.bars.coverage(S1).get(B) is Coverage.UNFILLABLE
    assert (S1, B, Resolution.UNFILLABLE) in rig.resolved
    assert B not in rig.bars.lane(S1)


async def test_bucket_beyond_broker_reach_is_unfillable_at_once() -> None:
    rig = make(history_reach_ms=10 * TF)
    await rig.run_to(T - 58_000)
    assert rig.bars.request_backfill(1, S1, B - 30 * TF, B - 30 * TF) is None
    assert rig.bars.coverage(S1).get(B - 30 * TF) is Coverage.UNFILLABLE
    assert rig.completed == [(1, "applied", "unfillable_1")]


async def test_md_backfill_request_refetches_in_the_lane_and_completes() -> None:
    rig = make()
    await rig.run_to(T - 58_000)
    n = len(rig.sink.queued)
    rig.broker.put(B - 20 * TF, close="1.1007")          # history the store never had
    assert rig.bars.request_backfill(7, S1, B - 21 * TF, B - 19 * TF) is None
    await rig.run_to(T - 50_000)
    assert rig.completed == [(7, "applied", "resolved")]
    assert len(rig.sink.queued) == n + 3


async def test_md_backfill_refusals() -> None:
    rig = make(md_backfill_max=10)
    await rig.run_to(T - 58_000)
    tick = SeriesId(AD, "SYN1", M15, Side.BID, Source.TICK)
    assert rig.bars.request_backfill(1, tick, B, B) == "tick_series"
    assert rig.bars.request_backfill(2, S2, B, B) == "not_demanded"
    assert rig.bars.request_backfill(3, S1, B + 1, B + TF) == "bucket_range"
    assert rig.bars.request_backfill(4, S1, B - 20 * TF, B) == "range_too_large"
    assert rig.completed == []


async def test_consumer_refetch_is_drained_into_the_lane() -> None:
    queue = RefetchQueue()
    rig = make(refetch=queue)
    await rig.run_to(T - 58_000)
    assert queue.request(S1, B - 25 * TF, B - 24 * TF) is None
    await rig.run_to(T - 55_000)
    assert len(queue) == 0 and rig.bars.counters.refetch_drained == 1
    assert {(s, b) for s, b, how in rig.resolved if how is Resolution.BAR} >= {(S1, B - 25 * TF), (S1, B - 24 * TF)}


async def test_gap_heal_walks_stored_holes_oldest_first() -> None:
    rig = make(need=12, heal_every_ms=1_000)
    await rig.run_to(T - 58_000)
    hole = B - 8 * TF
    rig.sink.queued = [b for b in rig.sink.queued if b.bucket_start_ms != hole]     # a member lost from md:bars
    await rig.run_to(T - 50_000)
    assert rig.bars.counters.heal_holes == 1
    assert (S1, hole, Resolution.BAR) in rig.resolved
    [bar] = rig.sink.of(S1, hole)
    assert bar.revision == 0


# ---------------------------------------------------------------------------------------------------- mirror, logs
async def test_meta_mirror_holds_states_and_counts_only() -> None:
    rig = make()
    rig.broker.bars.pop(("SYN1", B))
    await rig.run_to(T + 905_000, every=5_000)
    meta = rig.sink.meta[S1]
    assert meta["sm12"] == "LIVE" and meta["bars_phase"] == "live"
    assert meta["cov_no_trade"] == "1" and meta["cov_blind"] == "0" and meta["cov_unfillable"] == "0"
    assert all(v.isdigit() or v.isalpha() for v in meta.values())


async def test_no_price_in_logs_or_pages(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    down = (T + 2 * TF, T + 3 * TF)
    rig = make(health=Health(failing=lambda now: down[0] <= now < down[1]))
    rig.broker.put(B, high="1.0995")
    rig.broker.fail = lambda symbol, lo, hi: Transient("decode_error 1.1005") if lo == B + TF else None
    await rig.run_to(T + 4 * TF, every=5_000)
    text = caplog.text + " ".join(" ".join(p) for p in rig.pages.sent)
    assert rig.pages.sent
    for price in PRICES + ("1.0995",):
        assert price not in text
        assert all(price not in repr(b) for b in rig.sink.queued)


def test_settings_from_md_and_bounds() -> None:
    md = parse_md_settings({"XTNL_MD_REDIS_URL": "redis://h:1", "XTNL_MD_HMAC_KEY": "ab" * 32,
                            "XTNL_MD_HMAC_KEY_ID": "3", "XTNL_MD_BROKER_BAR_SETTLE_S": "20",
                            "XTNL_MD_MISMATCH_K": "4"})
    assert not isinstance(md, list)
    s = BrokerBarsSettings.from_md(md, page_bars=50)
    assert (s.settle_ms, s.mismatch_k, s.lag_ms, s.page_bars) == (20_000, 4, 5_000, 50)
    with pytest.raises(ValueError):
        BrokerBarsSettings(read_timeout_s=25.0)
    with pytest.raises(ValueError):
        BrokerBarsSettings(page_bars=0)


# ---------------------------------------------------------------------------------------------------- review edges
@pytest.mark.parametrize("failing, counted", [(False, True), (None, False)], ids=["poll_ok", "unknown"])
async def test_empty_read_then_failed_reads_to_the_cap_is_not_no_trade(failing: bool | None, counted: bool) -> None:
    rig = make(health=Health(failing=lambda now: failing))
    rig.broker.bars.pop(("SYN1", B))
    seen: list[int] = []

    def fail(symbol: str, lo: int, hi: int) -> object:
        if lo == B and not seen:                         # the first read of B: complete, forming bar, no bar for B
            seen.append(lo)
            return None
        return Transient("timeout") if seen and lo <= B <= hi else None

    rig.broker.fail = fail
    await rig.run_to(T + 906_000, every=2_000)
    assert B not in rig.bars.coverage(S1) and B in rig.bars.lane(S1)
    assert rig.bars.counters.capped_unresolved == (1 if counted else 0)
    assert rig.bars.counters.capped_uncounted == (0 if counted else 1)


async def test_store_unreadable_across_the_cap_never_counts() -> None:
    rig = make()
    assert rig.stored is not None
    rig.stored.down = True
    await rig.run_to(T + 906_000, every=2_000)
    status = rig.bars.status(S1)
    assert status is not None and status.unresolved_streak == 0 and status.invalid_streak == 0
    assert rig.bars.counters.capped_unresolved == 0 and rig.bars.counters.capped_uncounted >= 1
    assert len(live_calls(rig, B)) == 1                  # deferral re-checks the store, never re-reads the broker
    assert B in rig.bars.lane(S1) and rig.sink.of(S1, B) == []
    rig.stored.down = False
    await rig.run_to(T + 930_000)
    assert len(rig.sink.of(S1, B)) == 1 and B not in rig.bars.lane(S1)
    assert (S1, B, Resolution.BAR) in rig.resolved


async def test_md_backfill_on_a_failed_series_is_refused() -> None:
    rig = make()
    await rig.run_to(T - 58_000)
    rig.broker.hidden.add(B - 20 * TF)                   # keeps the request pending
    assert rig.bars.request_backfill(7, S1, B - 20 * TF, B - 20 * TF) is None
    rig.book.apply(S1, Sm12Event.FAIL)
    await rig.run_to(T - 56_000)
    assert rig.completed == [(7, "refused", "series_failed")]       # a pending request is completed
    assert rig.bars.request_backfill(8, S1, B - 21 * TF, B - 21 * TF) == "series_failed"
    assert rig.completed == [(7, "refused", "series_failed")]


async def test_raising_health_port_is_not_a_broker_outage(caplog: pytest.LogCaptureFixture) -> None:
    @dataclass
    class Broken(Health):
        def is_streaming(self, adapter_id: str) -> bool:
            raise RuntimeError("health wiring fault")

    caplog.set_level(logging.INFO)
    rig = make(health=Broken())
    rig.broker.hidden.add(B)
    await rig.run_to(T + 906_000, every=5_000)
    assert rig.pages.titled("broker bars unavailable") == 0 and rig.bars.counters.outages == 0
    assert rig.book.state(S1) is Sm12State.LIVE                      # never GAPPED
    assert len(rig.sink.of(S1, B - TF)) == 1                         # reads go on
    assert rig.bars.counters.capped_unresolved == 0 and rig.bars.counters.capped_uncounted == 1
    assert sum("health unknown" in r.getMessage() for r in caplog.records) == 1     # logged once


async def test_clean_later_bucket_before_an_older_cap_keeps_k_strict() -> None:
    skew = [SkewEstimate(p50_ms=60_000, settle_ms=5_000)]
    rig = make()
    rig.bars._skew = lambda adapter_id: skew[0]          # noqa: SLF001 - the adapter's ClockSkew
    rig.broker.put(B, high="1.0995")                     # B invalid; its cap is fixed under the large p50
    await rig.run_to(T + 70_000, every=5_000)
    skew[0] = COLD_SKEW                                  # p50 drops: the next bucket is final before B caps
    await rig.run_to(T + TF + 70_000, every=5_000)
    assert len(rig.sink.of(S1, T)) == 1 and B in rig.bars.lane(S1)
    status = rig.bars.status(S1)
    assert status is not None and status.invalid_streak == 0
    assert rig.bars.counters.capped_invalid == 0 and rig.bars.counters.capped_uncounted == 1


async def test_capped_empty_lane_bucket_beyond_real_reach_is_not_no_trade() -> None:
    rig = make(lane_max_misses=3)
    await rig.run_to(T - 58_000)
    for start in range(B - 39 * TF, B - 29 * TF, TF):     # the broker's real history ends here (reach default 1 y)
        rig.broker.bars.pop(("SYN1", start), None)
    old = B - 35 * TF
    assert rig.bars.request_backfill(3, S1, old, old) is None
    await rig.run_to(T - 30_000)
    assert rig.bars.coverage(S1).get(old) is Coverage.UNFILLABLE    # empty without evidence: a miss, never no_trade
    assert rig.completed == [(3, "applied", "unfillable_1")]
    gap = B - 25 * TF                                    # an empty bucket with history on both sides is no_trade
    rig.broker.bars.pop(("SYN1", gap))
    assert rig.bars.request_backfill(4, S1, gap, gap) is None
    await rig.run_to(T - 25_000)
    assert rig.bars.coverage(S1).get(gap) is Coverage.NO_TRADE


async def test_slow_series_skips_its_backfill_page_without_cutting_the_pass() -> None:
    rig = make(series=(S1, S2), read_timeout_s=0.9, pass_budget_s=1.0)
    inner = rig.broker

    class Slow:
        async def fetch_price_bars(self, adapter_id: str, symbol: str, side: Side, timeframe_s: int,
                                   **kw: Any) -> Any:
            if symbol == "SYN1":
                await asyncio.sleep(0.6)                 # more than half the pass budget
            return await inner.fetch_price_bars(adapter_id, symbol, side, timeframe_s, **kw)

    rig.bars._port = Slow()                              # noqa: SLF001 - a slow broker for one series
    await rig.bars.step()
    c = rig.bars.counters
    assert c.backfill_skipped == 1 and c.pass_timeouts == 0 and c.series_timeouts == 0
    assert rig.sink.of(S2, B - 2 * TF) and rig.sink.of(S1, B - 2 * TF) == []


# ---------------------------------------------------------------------------------------------------- integration
async def test_with_the_real_writer_and_store(md_redis: Any, run_ids: RunIds) -> None:
    rig = await make_rig(md_redis, run_ids, start=T - 60_000)
    await rig.at(T - 60_000)                             # term-start bootstrap writes md:boot
    assert rig.writer.boot is not None
    broker = Broker(rig.clock)
    broker.fill(T - 40 * TF, T + 20 * TF)
    reader = StoreReader(md_redis)
    bars = MdBars(broker, rig.writer, rig.book, keyring=KEYRING, demand=demanded([(S1, 4)]),
                  stored=StoreBarsReader(reader), trusts_next_bucket=lambda a: True, clock_ms=rig.clock)
    for now in range(T - 59_000, T + 8_000, 1_000):
        rig.clock.now = now
        await bars.step()
        await rig.svc.step()
    stored = await reader.bars_range(S1, B - 6 * TF, B)
    assert [s for s, _ in stored] == [B - 5 * TF + k * TF for k in range(6)]
    meta = await reader.meta(S1)
    assert meta.get("sm12") == "LIVE" and meta.get("cov_blind") == "0" and meta.get("bars_phase") == "live"
    seq = meta.get("write_seq")
    assert not rig.writer.set_meta(S1, [("write_seq", "1")])     # a field the script writes itself is refused
    assert not rig.writer.set_meta(S1, [("bad field", "x")])
    # a new term re-fetches the same history: nothing is written again (no revision, no write_seq)
    again = MdBars(broker, rig.writer, rig.book, keyring=KEYRING, demand=demanded([(S1, 4)]),
                   stored=StoreBarsReader(reader), trusts_next_bucket=lambda a: True, clock_ms=rig.clock)
    rig.book.apply(S1, Sm12Event.GAP)
    for now in range(T + 8_000, T + 12_000, 1_000):
        rig.clock.now = now
        await again.step()
        await rig.svc.step()
    assert again.counters.bars_queued == 0 and again.counters.bars_unchanged >= 4
    assert (await reader.meta(S1)).get("write_seq") == seq
