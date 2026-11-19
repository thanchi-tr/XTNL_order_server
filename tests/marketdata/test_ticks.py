"""The market-data tick hook: ring, latest quote, drop counters, coverage, receipt-timed ticks, finality by the cap,
clock steps and receipt gaps (plan S14; design §A8, §A4, §A9, §A7). Synthetic instruments and prices only."""
from __future__ import annotations

from decimal import Decimal

from src.dto.entities import PriceDTO
from src.marketdata.clockstep import ClockStep, WallMonoDetector
from src.marketdata.skew import MIN_SAMPLES
from src.marketdata.tickbars import TickBar
from src.marketdata.ticks import STATUS_INDICATIVE, STATUS_SNAPSHOT, STATUS_TRADEABLE, MdHook
from src.marketdata.timing import COLD_SKEW
from src.marketdata.types import M1, M15, Quality, Side
from src.runtime.session import MarketDataHookGuard, MdHook as SessionMdHook

H0 = 1_699_999_200_000
MIN = 60_000
AD, SYM = "mock", "SYN1"


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


def hook(*, start: int = H0 - MIN, unfiltered: bool = True, ring_size: int = 4_096,
         keepalive_ms: dict[str, int] | None = None) -> tuple[MdHook, Clock]:
    clock = Clock(start)
    h = MdHook(clock_ms=clock, ring_size=ring_size, unfiltered=(AD,) if unfiltered else (),
               keepalive_ms=keepalive_ms)
    h.seed_subscriptions(AD, {SYM: start})
    return h, clock


def dto(bid: str, broker_ms: int, *, received_ms: int | None = None, ts_source: str | None = "BROKER",
        tradeable: bool = True, ask: str | None = None, symbol: str = SYM) -> PriceDTO:
    b = Decimal(bid)
    a = Decimal(ask) if ask is not None else b + Decimal("0.0002")
    return PriceDTO(adapter_id=AD, symbol=symbol, bid=b, ask=a, spread_pips=None, timestamp_ms=broker_ms,
                    tradeable=tradeable, ts_source=ts_source,  # type: ignore[arg-type]
                    received_ms=broker_ms + 100 if received_ms is None else received_ms)


def minutes(h: MdHook, n: int, start: int = H0, per_minute: int = 4) -> None:
    for i in range(n * per_minute):
        ts = start + i * (MIN // per_minute) + 7
        h.on_price(dto(f"1.{10_000 + (i * 7) % 13:05d}", ts))


def closed(h: MdHook) -> list[TickBar]:
    return list(h.drain(AD, SYM).closed)


def one(bars: list[TickBar], spec: object, start: int, side: Side = Side.BID) -> TickBar:
    found = [t for t in bars if t.bar.series.spec == spec and t.bar.series.side is side
             and t.bar.bucket_start_ms == start]
    assert len(found) == 1, found
    return found[0]


def test_signature_matches_the_session_hook_and_its_guard() -> None:
    h, _ = hook()
    fn: SessionMdHook = h.on_price
    pages: list[tuple[str, str, str]] = []
    guard = MarketDataHookGuard(lambda *a: pages.append(a))
    guard.call(fn, dto("1.10000", H0 + 1_000), False)
    assert guard.raised == 0 and h.counters.bar_ticks == 1 and not pages


def test_bars_build_and_finalise_at_the_next_bucket_and_at_the_cap() -> None:
    h, clock = hook()
    minutes(h, 2)
    out = closed(h)
    first = one(out, M1, H0)
    assert first.bar.quality is Quality.COMPLETE and first.close_eligible
    assert not [t for t in out if t.bar.bucket_start_ms == H0 + MIN]
    clock.now = H0 + 2 * MIN + COLD_SKEW.settle_ms - 1
    h.advance(clock.now)                                    # cold skew: T + 0 + 5 s not yet reached
    assert closed(h) == []
    clock.now += 1
    h.advance(clock.now)
    assert one(closed(h), M1, H0 + MIN).bar.final_at_ms == clock.now


def test_ring_overflow_never_degrades_a_closed_bar() -> None:
    h, clock = hook(ring_size=3)
    minutes(h, 15, per_minute=6)
    drained = h.drain(AD, SYM)
    assert len(drained.ticks) == 3 and drained.ring_dropped == 15 * 6 - 3
    assert h.counters.ring_dropped == 15 * 6 - 3
    clock.now = H0 + 16 * MIN
    h.advance(clock.now)
    out = list(drained.closed) + closed(h)
    assert out and all(t.bar.quality is Quality.COMPLETE and t.close_eligible for t in out)
    assert one(out, M15, H0).bar.tick_count == 15 * 6


def test_close_eligible_under_filtered_delivery_with_complete_coverage() -> None:
    h, clock = hook(unfiltered=False)
    minutes(h, 15)
    clock.now = H0 + 16 * MIN
    h.advance(clock.now)
    q15 = one(closed(h), M15, H0)
    assert q15.bar.quality is Quality.PARTIAL and q15.close_eligible
    h.set_unfiltered(AD, True)
    minutes(h, 15, start=H0 + 15 * MIN)
    clock.now = H0 + 31 * MIN
    h.advance(clock.now)
    q15b = one(closed(h), M15, H0 + 15 * MIN)
    assert q15b.bar.quality is Quality.COMPLETE and q15b.close_eligible


def test_hook_started_inside_a_bucket_makes_it_partial() -> None:
    h, clock = hook(start=H0 + 10_000)
    minutes(h, 2)
    clock.now = H0 + 3 * MIN
    h.advance(clock.now)
    out = closed(h)
    assert one(out, M1, H0).bar.quality is Quality.PARTIAL and not one(out, M1, H0).close_eligible
    assert one(out, M1, H0 + MIN).bar.quality is Quality.COMPLETE


def test_unsubscribed_stretch_makes_partial_and_not_close_eligible() -> None:
    h, clock = hook()
    minutes(h, 1)
    h.on_price_subscription(AD, SYM, False, H0 + MIN + 10_000)
    h.on_price_subscription(AD, SYM, True, H0 + MIN + 20_000)
    minutes(h, 2, start=H0 + MIN + 25_000, per_minute=2)
    clock.now = H0 + 5 * MIN
    h.advance(clock.now)
    out = closed(h)
    assert one(out, M1, H0).bar.quality is Quality.COMPLETE
    assert one(out, M1, H0 + MIN).bar.quality is Quality.PARTIAL and not one(out, M1, H0 + MIN).close_eligible
    assert one(out, M1, H0 + 2 * MIN).bar.quality is Quality.COMPLETE


def test_drop_totals_flag_the_buckets_between_readings() -> None:
    h, clock = hook()
    h.note_drop_totals(AD, {SYM: 5}, H0)                    # baseline: earlier drops are not this hook's
    minutes(h, 1)
    h.note_drop_totals(AD, {SYM: 5}, H0 + 30_000)           # unchanged
    h.on_price(dto("1.10000", H0 + MIN + 1_000))
    h.note_drop_totals(AD, {SYM: 5}, H0 + MIN + 29_750)     # unchanged (readings come every 250 ms)
    h.note_drop_totals(AD, {SYM: 6}, H0 + MIN + 30_000)     # a lost update inside minute 2
    h.on_price(dto("1.10000", H0 + 2 * MIN + 1_000))
    clock.now = H0 + 4 * MIN
    h.advance(clock.now)
    out = closed(h)
    assert one(out, M1, H0).bar.quality is Quality.COMPLETE and one(out, M1, H0).close_eligible
    m2 = one(out, M1, H0 + MIN)
    assert m2.bar.quality is Quality.DEGRADED and not m2.close_eligible


def test_first_drops_of_a_market_after_an_empty_first_reading_are_flagged() -> None:
    h, clock = hook()
    h.note_drop_totals(AD, {}, H0)                          # baseline: the market has no drop entry yet (0)
    minutes(h, 1)
    h.note_drop_totals(AD, {SYM: 3}, H0 + 30_000)           # its first drops
    h.note_drop_totals(AD, {SYM: 3}, H0 + MIN + 30_000)
    h.on_price(dto("1.10000", H0 + MIN + 40_000))
    h.note_drop_totals(AD, {}, H0 + MIN + 50_000)           # missing again: reads as 0 (a counter that went back)
    h.on_price(dto("1.10000", H0 + 2 * MIN + 1_000))
    clock.now = H0 + 4 * MIN
    h.advance(clock.now)
    out = closed(h)
    m1 = one(out, M1, H0)
    assert m1.bar.quality is Quality.DEGRADED and not m1.close_eligible
    assert one(out, M1, H0 + MIN).bar.quality is Quality.DEGRADED


def test_drops_of_a_market_first_seen_after_the_baseline_are_flagged() -> None:
    h, clock = hook()
    h.note_drop_totals(AD, {SYM: 4}, H0 - 1_000)            # baseline before SYN2 exists
    h.on_price_subscription(AD, "SYN2", True, H0 - 500)
    for i in range(4):
        h.on_price(dto(f"1.{10_000 + i:05d}", H0 + i * 10_000 + 7, symbol="SYN2"))
    h.note_drop_totals(AD, {SYM: 4, "SYN2": 2}, H0 + 45_000)
    h.on_price(dto("1.10000", H0 + MIN + 1_000, symbol="SYN2"))
    clock.now = H0 + 3 * MIN
    h.advance(clock.now)
    bar = one(list(h.drain(AD, "SYN2").closed), M1, H0)
    assert bar.bar.quality is Quality.DEGRADED and not bar.close_eligible


def test_late_drop_reading_revises_a_final_bucket() -> None:
    h, clock = hook()
    h.note_drop_totals(AD, {SYM: 0}, H0 + 50_000)
    minutes(h, 1)
    h.on_price(dto("1.10000", H0 + MIN + 100))              # minute 1 final
    assert one(closed(h), M1, H0).bar.revision == 0
    h.note_drop_totals(AD, {SYM: 1}, H0 + MIN + 150)        # the drop happened in [50 s, 60.15 s]
    revised = one(closed(h), M1, H0)
    assert revised.bar.revision == 1 and revised.bar.quality is Quality.DEGRADED and not revised.close_eligible


def test_receipt_timed_tick_degrades_its_bucket_and_never_builds() -> None:
    h, clock = hook()
    h.on_price(dto("1.10000", H0 + 5_000))
    h.on_price(dto("1.50000", H0 + 30_000, received_ms=H0 + 30_000, ts_source="RECEIPT"))
    h.on_price(dto("1.10010", H0 + 40_000))
    h.on_price(dto("1.10020", H0 + 50_000, ts_source=None))   # not stated: treated as receipt-timed
    clock.now = H0 + 2 * MIN
    h.advance(clock.now)
    bar = one(closed(h), M1, H0)
    assert bar.bar.quality is Quality.DEGRADED and bar.bar.high == Decimal("1.10010") and bar.bar.tick_count == 2
    assert not bar.close_eligible                              # the last tick was not broker-timed
    assert h.counters.receipt_timed == 2
    q = h.quote(AD, SYM)
    assert q is not None and not q.broker_timed


def test_snapshot_and_indicative_ticks_are_captured_but_never_build_bars() -> None:
    h, clock = hook()
    h.on_price(dto("1.40000", H0 + 1_000), True)
    h.on_price(dto("1.60000", H0 + 2_000, tradeable=False))
    h.on_price(dto("1.10000", H0 + 3_000))
    ticks = h.drain(AD, SYM).ticks
    assert [t.status for t in ticks] == [STATUS_SNAPSHOT, STATUS_INDICATIVE, STATUS_TRADEABLE]
    clock.now = H0 + 2 * MIN
    h.advance(clock.now)
    bar = one(closed(h), M1, H0)
    assert bar.bar.tick_count == 1 and bar.bar.high == Decimal("1.10000")
    assert h.counters.snapshots == 1 and h.counters.indicative == 1


def test_latest_quote_is_validated() -> None:
    h, _ = hook()
    h.on_price(dto("1.10000", H0 + 1_000))
    q = h.quote(AD, SYM)
    assert q is not None and q.broker_timed and q.bid == Decimal("1.10000")
    h.on_price(dto("1.10000", H0 + 2_000, ask="1.09990"))           # ask below bid
    assert h.quote(AD, SYM) is None and h.counters.rejected == 1
    h.on_price(dto("1.10000", H0 + 3_000))
    assert h.quote(AD, SYM) is not None
    h.on_price(dto("1.10000", H0 + 60_000, received_ms=H0 + 4_000))  # broker time 56 s ahead of receipt
    assert h.quote(AD, SYM) is None and h.counters.rejected == 2
    h.on_price(dto("NaN", H0 + 5_000, ask="1.1"))
    assert h.counters.rejected == 3
    h.on_price(dto("1.20000", H0 + 6_000, tradeable=False))
    assert h.quote(AD, SYM) is None                                  # indicative prices never become the quote


def test_rejected_tick_counts_as_a_lost_update() -> None:
    h, clock = hook()
    minutes(h, 1)
    h.on_price(dto("0", H0 + 30_500, received_ms=H0 + 30_600))
    clock.now = H0 + 2 * MIN
    h.advance(clock.now)
    bar = one(closed(h), M1, H0)
    assert bar.bar.quality is Quality.DEGRADED and not bar.close_eligible


def test_late_tick_revision_through_the_hook() -> None:
    h, _ = hook()
    minutes(h, 2)
    assert one(closed(h), M1, H0).bar.revision == 0
    h.on_price(dto("1.90000", H0 + 20_000, received_ms=H0 + MIN + 30_000))
    rev = one(closed(h), M1, H0)
    assert rev.bar.revision == 1 and rev.bar.high == Decimal("1.90000") and h.counters.late_ticks == 1


def test_clock_step_blinds_overlapping_buckets_resets_forming_and_rebaselines() -> None:
    h, clock = hook()
    for i in range(40):                                      # settle the skew first
        ts = H0 + i * 1_000
        h.on_price(dto(f"1.{10_000 + i:05d}", ts, received_ms=ts + 150))
    h.on_price(dto("1.20000", H0 + MIN + 1_000, received_ms=H0 + MIN + 1_150))
    h.on_price(dto("1.20010", H0 + 2 * MIN + 1_000, received_ms=H0 + 2 * MIN + 1_150))
    clock.now = H0 + 2 * MIN + 2_000
    h.advance(clock.now)
    assert h.skew(AD).settled and h.skew_estimate(AD).p50_ms == 150
    before = closed(h)
    assert one(before, M1, H0).bar.quality is Quality.COMPLETE
    assert h.forming(AD, SYM)
    h.clock_step(ClockStep(H0 + MIN + 30_000, H0 + 2 * MIN + 2_000, 30_000, H0 + MIN + 30_000, 2_000))
    after = closed(h)
    m2 = one(after, M1, H0 + MIN)                            # final minute overlapping the step: revised blind
    assert m2.bar.quality is Quality.BLIND and m2.bar.revision == 1 and not m2.close_eligible
    assert not [t for t in after if t.bar.bucket_start_ms == H0 and t.bar.series.spec == M1]
    assert h.forming(AD, SYM) == ()                          # forming bars reset
    assert not h.skew(AD).settled and h.skew(AD).samples == 0 and h.skew_estimate(AD) == COLD_SKEW
    h.on_price(dto("1.30000", H0 + 2 * MIN + 40_000))
    clock.now = H0 + 4 * MIN
    h.advance(clock.now)
    m3 = one(closed(h), M1, H0 + 2 * MIN)
    assert m3.bar.quality is Quality.BLIND and m3.bar.tick_count == 1
    assert h.counters.clock_steps == 1


def test_forward_wall_step_blinds_only_the_stretch_at_risk() -> None:
    h, clock = hook()
    det = WallMonoDetector()
    det.observe(H0 + 29_000, 1_000)
    minutes(h, 1, per_minute=2)                              # ticks at 7 ms and 30.007 s of minute 0
    step = det.observe(H0 + 30_000 + 10 * MIN, 2_000)        # wall jumps 10 min ahead; broker clock unchanged
    assert step is not None
    clock.now = H0 + 30_000 + 10 * MIN
    h.clock_step(step)
    for k in (2, 3):                                         # later broker minutes, receipt in the new wall frame
        ts = H0 + k * MIN + 1_000
        h.on_price(dto("1.10000", ts, received_ms=ts + 10 * MIN + 100))
    clock.now = H0 + 15 * MIN
    h.advance(clock.now)
    out = closed(h)
    assert one(out, M1, H0 + 2 * MIN).bar.quality is Quality.COMPLETE   # not blinded by the step's size
    assert not [t for t in out if t.bar.series.spec == M1 and t.bar.bucket_start_ms == H0]  # forming: reset


def test_backward_wall_step_never_reissues_past_final_minutes() -> None:
    h, clock = hook()
    minutes(h, 5)                                            # minutes 0-3 final, minute 4 forming
    before = closed(h)
    assert {t.bar.bucket_start_ms for t in before if t.bar.series.spec == M1} == {H0 + k * MIN for k in range(4)}
    det = WallMonoDetector()
    det.observe(H0 + 5 * MIN, 1_000)
    step = det.observe(H0 + 5 * MIN - 4 * MIN, 2_000)        # wall steps 4 min back
    assert step is not None and step.delta_ms < 0
    clock.now = H0 + MIN
    h.clock_step(step)
    assert closed(h) == []                                   # no final minute re-issued as blind
    assert h.forming(AD, SYM) == ()


def test_receipt_gap_resets_the_forming_bars() -> None:
    h, clock = hook(keepalive_ms={AD: 10_000})
    h.on_price(dto("1.50000", H0 + 1_000, received_ms=H0 + 1_000))
    clock.now = H0 + 20_000
    h.advance(clock.now)                                     # gap found; minute 0 still forming
    assert h.in_receipt_gap(AD, SYM) and h.forming(AD, SYM) == ()
    h.on_price(dto("1.10000", H0 + 40_000, received_ms=H0 + 40_000))
    clock.now = H0 + 2 * MIN
    h.advance(clock.now)
    bar = one(closed(h), M1, H0)
    assert bar.bar.quality is Quality.BLIND and bar.bar.tick_count == 1 and bar.bar.high == Decimal("1.10000")


def test_receipt_gap_watch_survives_a_backward_wall_step() -> None:
    h, clock = hook(keepalive_ms={AD: 10_000})
    h.on_price(dto("1.10000", H0 + 5 * MIN, received_ms=H0 + 5 * MIN))
    det = WallMonoDetector()
    det.observe(H0 + 5 * MIN, 1_000)
    step = det.observe(H0 + 5 * MIN - 3 * MIN, 1_500)        # wall steps 3 min back
    assert step is not None
    clock.now = H0 + 2 * MIN
    h.clock_step(step)
    clock.now = H0 + 2 * MIN + 15_000
    h.advance(clock.now)                                     # silent for 15 s on the monotonic clock: a gap
    assert h.in_receipt_gap(AD, SYM)


def test_receipt_gap_blinds_the_silent_buckets() -> None:
    h, clock = hook(keepalive_ms={AD: 10_000})
    h.on_price(dto("1.10000", H0 + 1_000, received_ms=H0 + 1_000))
    clock.now = H0 + 3 * MIN
    h.advance(clock.now)                                     # silent since 1 s: gap found
    assert h.in_receipt_gap(AD, SYM) and h.receipt_gaps == 1
    h.on_price(dto("1.10010", H0 + 3 * MIN + 30_000, received_ms=H0 + 3 * MIN + 30_000))
    assert not h.in_receipt_gap(AD, SYM)
    clock.now = H0 + 5 * MIN
    h.advance(clock.now)
    out = closed(h)
    assert one(out, M1, H0).bar.quality is Quality.BLIND
    assert one(out, M1, H0 + 3 * MIN).bar.quality is Quality.BLIND


def test_skew_samples_only_from_fresh_ticks() -> None:
    h, _ = hook(start=H0)
    h.on_price(dto("1.10000", H0 + 1_000, received_ms=H0 + 1_200))      # within 10 s of the subscribe
    for i in range(5):
        h.on_price(dto("1.10000", H0 + 20_000 + i, received_ms=H0 + 20_200 + i))   # unchanged values
    h.on_price(dto("1.10050", H0 + 30_000, received_ms=H0 + 30_200), True)          # snapshot
    h.on_price(dto("1.10060", H0 + 31_000, received_ms=H0 + 31_200, ts_source="RECEIPT"))
    assert h.skew(AD).samples == 0
    for i in range(MIN_SAMPLES):
        h.on_price(dto(f"1.{11_000 + i:05d}", H0 + 40_000 + i * 10, received_ms=H0 + 40_250 + i * 10))
    h.advance(H0 + 50_000)
    assert h.skew(AD).samples == MIN_SAMPLES and h.skew_estimate(AD).p50_ms == 250


def test_symbols_refused_when_not_key_safe_or_over_the_limit() -> None:
    clock = Clock(H0)
    h = MdHook(clock_ms=clock, max_symbols_per_adapter=2)
    h.on_price(dto("1.1", H0, symbol="BAD SYM"))
    h.on_price(dto("1.1", H0, symbol="A1"))
    h.on_price(dto("1.1", H0, symbol="A2"))
    h.on_price(dto("1.1", H0, symbol="A3"))
    h.on_price(dto("1.1", H0 + 1, symbol="A3"))              # remembered: counted once per symbol
    h.on_price(dto("1.1", H0 + 1, symbol="BAD SYM"))
    assert h.markets() == [(AD, "A1"), (AD, "A2")] and h.counters.symbols_refused == 2


def test_drain_of_an_unknown_symbol_is_empty_and_nothing_shows_prices() -> None:
    h, _ = hook()
    assert h.drain(AD, "NOPE").ticks == ()
    h.on_price(dto("1.23456", H0 + 1_000))
    d = h.drain(AD, SYM)
    assert "1.23456" not in repr(d) and "1.23456" not in repr(h.quote(AD, SYM))
    assert h.drain(AD, SYM).ticks == ()


def test_note_missed_degrades_every_market() -> None:
    h, clock = hook()
    minutes(h, 1)
    h.note_missed(H0 + 20_000, H0 + 21_000)
    clock.now = H0 + 2 * MIN
    h.advance(clock.now)
    assert one(closed(h), M1, H0).bar.quality is Quality.DEGRADED
