"""TICK bars: 1-minute builder, exact roll-ups, finality, revisions, quality, `close_eligible` (plan S14; design §A4,
R11-M01). Synthetic instruments and prices only."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.tickbars import TickBar, TickBarBuilder, TickOutcome, worst
from src.marketdata.types import H1, M1, M5, M15, Quality, Side, Source

H0 = 1_699_999_200_000          # an hour boundary (UTC grid)
MIN = 60_000


def always(_a: int, _b: int) -> bool:
    return True


def never(_a: int, _b: int) -> bool:
    return False


def builder(*, filtered: bool = False, covered: object = always, resolution_ms: int = 0) -> TickBarBuilder:
    return TickBarBuilder("mock", "SYN1", covered=covered, filtered=filtered,  # type: ignore[arg-type]
                          resolution_ms=resolution_ms)


def p(x: str) -> Decimal:
    return Decimal(x)


def bars_of(out: tuple[TickBar, ...], spec: object, side: Side = Side.BID) -> list[TickBar]:
    return [t for t in out if t.bar.series.spec == spec and t.bar.series.side is side]


def feed(b: TickBarBuilder, ticks: list[tuple[int, str]]) -> None:
    for ts, bid in ticks:
        b.add_tick(p(bid), p(bid) + p("0.0002"), ts, ts + 50)


def synthetic(n_minutes: int, per_minute: int = 7, start: int = H0) -> list[tuple[int, str]]:
    out = []
    for i in range(n_minutes * per_minute):
        ts = start + i * (MIN // per_minute) + (i % 3) * 11
        level = 10_000 + ((i * 37) % 23) - ((i * 13) % 17) + i // 5
        out.append((ts, f"1.{level:05d}"))
    return out


def test_one_minute_bar_both_sides_final_at_next_bucket_first_tick() -> None:
    b = builder()
    feed(b, [(H0 + 1_000, "1.10000"), (H0 + 20_000, "1.10050"), (H0 + 40_000, "1.09950"), (H0 + 59_000, "1.10010")])
    assert b.take_closed() == ()                     # still forming
    forming = b.forming()
    assert {(f.series.spec, f.series.side) for f in forming} >= {(M1, Side.BID), (M1, Side.ASK)}
    assert all(f.final_at_ms is None for f in forming)
    b.add_tick(p("1.10020"), p("1.10040"), H0 + MIN + 500, H0 + MIN + 550)
    out = b.take_closed()
    bid, = bars_of(out, M1, Side.BID)
    ask, = bars_of(out, M1, Side.ASK)
    assert (bid.bar.open, bid.bar.high, bid.bar.low, bid.bar.close) == (
        p("1.10000"), p("1.10050"), p("1.09950"), p("1.10010"))
    assert ask.bar.high == p("1.10070") and ask.bar.close == p("1.10030")
    assert bid.bar.tick_count == 4 and bid.bar.revision == 0 and bid.bar.final_at_ms == H0 + MIN + 550
    assert bid.bar.series.source is Source.TICK and bid.bar.series.canonical == "mock:SYN1:1m:BID:TICK"
    assert bid.bar.quality is Quality.COMPLETE and bid.close_eligible


def test_final_at_the_cap_without_a_next_tick() -> None:
    b = builder()
    feed(b, [(H0 + 1_000, "1.20000")])
    b.finalize_until(H0 + MIN - 1, H0 + MIN + 4_000)
    assert b.take_closed() == ()
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    bid, = bars_of(b.take_closed(), M1)
    assert bid.bar.final_at_ms == H0 + MIN + 5_000


def test_roll_ups_are_exact_and_equal_the_one_minute_children() -> None:
    b = builder()
    ticks = synthetic(60)
    feed(b, ticks)
    b.finalize_until(H0 + 60 * MIN, H0 + 61 * MIN)
    out = b.take_closed()
    minutes = sorted(bars_of(out, M1), key=lambda t: t.bar.bucket_start_ms)
    assert len(minutes) == 60
    for spec in (M5, M15, H1):
        rolled = sorted(bars_of(out, spec), key=lambda t: t.bar.bucket_start_ms)
        assert len(rolled) == 60 * MIN // spec.timeframe_ms
        for r in rolled:
            s, e = r.bar.bucket_start_ms, r.bar.bucket_end_ms
            kids = [m.bar for m in minutes if s <= m.bar.bucket_start_ms < e]
            raw = [(ts, p(x)) for ts, x in ticks if s <= ts < e]
            assert r.bar.open == kids[0].open == raw[0][1]
            assert r.bar.close == kids[-1].close == raw[-1][1]
            assert r.bar.high == max(k.high for k in kids) == max(v for _, v in raw)
            assert r.bar.low == min(k.low for k in kids) == min(v for _, v in raw)
            assert r.bar.tick_count == sum(k.tick_count for k in kids) == len(raw)
            assert r.bar.quality is Quality.COMPLETE and r.close_eligible


def test_roll_up_quality_is_the_worst_child() -> None:
    b = builder()
    feed(b, synthetic(15))
    b.mark(H0 + 7 * MIN + 10, H0 + 7 * MIN + 20, H0, dropped=True)     # one degraded minute
    b.finalize_until(H0 + 15 * MIN, H0 + 16 * MIN)
    out = b.take_closed()
    q15, = bars_of(out, M15)
    assert q15.bar.quality is Quality.DEGRADED and not q15.close_eligible
    five = {t.bar.bucket_start_ms: t for t in bars_of(out, M5)}
    assert five[H0].bar.quality is Quality.COMPLETE
    assert five[H0 + 5 * MIN].bar.quality is Quality.DEGRADED
    assert five[H0 + 10 * MIN].bar.quality is Quality.COMPLETE


def test_quality_classes_of_one_minute() -> None:
    cases = {
        "complete": (always, {}, Quality.COMPLETE, True),
        "partial": (never, {}, Quality.PARTIAL, False),
        "degraded_drop": (always, {"dropped": True}, Quality.DEGRADED, False),
        "blind": (always, {"blind": True}, Quality.BLIND, False),
    }
    for name, (cov, flags, quality, eligible) in cases.items():
        b = builder(covered=cov)
        feed(b, [(H0 + 5_000, "1.30000"), (H0 + 30_000, "1.30010")])
        if flags:
            b.mark(H0 + 10_000, H0 + 10_000, H0, **flags)
        b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
        bid, = bars_of(b.take_closed(), M1)
        assert (bid.bar.quality, bid.close_eligible) == (quality, eligible), name


def test_receipt_timed_tick_degrades_and_a_later_one_kills_close_eligibility() -> None:
    b = builder()
    feed(b, [(H0 + 5_000, "1.30000"), (H0 + 30_000, "1.30010")])
    b.note_receipt(H0 + 10_000, H0 + 10_050)          # before the last broker tick: close still broker-timed
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    bid, = bars_of(b.take_closed(), M1)
    assert bid.bar.quality is Quality.DEGRADED and bid.close_eligible
    b2 = builder()
    feed(b2, [(H0 + 5_000, "1.30000"), (H0 + 30_000, "1.30010")])
    b2.note_receipt(H0 + 45_000, H0 + 45_050)         # after it: the close is unknown
    b2.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    bid2, = bars_of(b2.take_closed(), M1)
    assert bid2.bar.quality is Quality.DEGRADED and not bid2.close_eligible


def test_filtered_delivery_caps_quality_but_close_eligible_holds_when_coverage_is_complete() -> None:
    b = builder(filtered=True)
    feed(b, synthetic(15))
    b.finalize_until(H0 + 15 * MIN, H0 + 16 * MIN)
    out = b.take_closed()
    for t in out:
        assert t.bar.quality is Quality.PARTIAL       # high / low may miss extremes under filtered delivery
        assert t.close_eligible                       # R11-M01: the close is still checkable
    unfiltered = builder(filtered=False)
    feed(unfiltered, synthetic(15))
    unfiltered.finalize_until(H0 + 15 * MIN, H0 + 16 * MIN)
    assert all(t.bar.quality is Quality.COMPLETE for t in unfiltered.take_closed())


def test_filtered_close_not_eligible_with_a_drop_or_an_uncovered_stretch() -> None:
    b = builder(filtered=True)
    feed(b, synthetic(15))
    b.mark(H0 + 14 * MIN + 1, H0 + 14 * MIN + 2, H0, dropped=True)
    b.finalize_until(H0 + 15 * MIN, H0 + 16 * MIN)
    q15, = bars_of(b.take_closed(), M15)
    assert not q15.close_eligible
    gap = (H0 + 3 * MIN, H0 + 4 * MIN)

    def covered(a: int, z: int) -> bool:
        return z <= gap[0] or a >= gap[1]

    c = builder(filtered=True, covered=covered)
    feed(c, synthetic(15))
    c.finalize_until(H0 + 15 * MIN, H0 + 16 * MIN)
    out = c.take_closed()
    q15c, = bars_of(out, M15)
    assert not q15c.close_eligible and q15c.bar.quality is Quality.PARTIAL
    minutes = {t.bar.bucket_start_ms: t for t in bars_of(out, M1)}
    assert minutes[H0 + 2 * MIN].close_eligible and not minutes[H0 + 3 * MIN].close_eligible


def test_late_tick_makes_a_revision_of_the_minute_and_its_roll_ups() -> None:
    b = builder()
    feed(b, synthetic(15))
    b.add_tick(p("1.10000"), p("1.10020"), H0 + 15 * MIN + 100, H0 + 15 * MIN + 150)   # finalises up to 15 min
    first = b.take_closed()
    m3, = [t for t in bars_of(first, M1) if t.bar.bucket_start_ms == H0 + 3 * MIN]
    q15, = bars_of(first, M15)
    assert m3.bar.revision == 0 and q15.bar.revision == 0
    outcome = b.add_tick(p("1.90000"), p("1.90020"), H0 + 3 * MIN + 30_000, H0 + 15 * MIN + 900)
    assert outcome is TickOutcome.REVISED
    again = b.take_closed()
    m3r, = [t for t in bars_of(again, M1) if t.bar.bucket_start_ms == H0 + 3 * MIN]
    q15r, = bars_of(again, M15)
    five, = bars_of(again, M5)
    assert m3r.bar.final_at_ms == m3.bar.final_at_ms                 # a revision keeps the first final time
    assert m3r.bar.revision == 1 and m3r.bar.tick_count == m3.bar.tick_count + 1 and m3r.bar.high == p("1.90000")
    assert q15r.bar.revision == 1 and q15r.bar.high == p("1.90000") and q15r.bar.final_at_ms == H0 + 15 * MIN + 150
    assert five.bar.bucket_start_ms == H0 and five.bar.revision == 1
    assert not bars_of(again, H1)                     # the hour is still forming
    assert b.revisions >= 3


def test_late_tick_order_sets_open_and_close_by_broker_time() -> None:
    b = builder()
    feed(b, [(H0 + 20_000, "1.20000"), (H0 + 40_000, "1.20010")])
    b.add_tick(p("1.20100"), p("1.20120"), H0 + 10_000, H0 + 41_000)      # out of order, earlier: new open
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    bid, = bars_of(b.take_closed(), M1)
    assert bid.bar.open == p("1.20100") and bid.bar.close == p("1.20010")


def test_flag_on_a_final_minute_revises_only_when_it_changes_something() -> None:
    b = builder()
    feed(b, [(H0 + 5_000, "1.30000")])
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    b.take_closed()
    b.mark(H0 + 10_000, H0 + 10_000, H0 + MIN + 6_000, dropped=True)
    rev, = bars_of(b.take_closed(), M1)
    assert rev.bar.revision == 1 and rev.bar.quality is Quality.DEGRADED
    b.mark(H0 + 20_000, H0 + 20_000, H0 + MIN + 7_000, dropped=True)        # already dropped: no new revision
    assert b.take_closed() == ()


def test_too_late_ticks_are_refused_after_the_window() -> None:
    b = TickBarBuilder("mock", "SYN1", covered=always, filtered=False, retain_ms=2 * 3_600_000)
    feed(b, [(H0 + 1_000, "1.10000")])
    far = H0 + 4 * 3_600_000
    b.add_tick(p("1.10000"), p("1.10020"), far, far + 50)
    b.finalize_until(far, far + 100)
    assert b.add_tick(p("1.1"), p("1.2"), H0 + 2_000, far + 200) is TickOutcome.TOO_LATE


def test_boundary_flag_within_the_timestamp_resolution() -> None:
    b = builder(resolution_ms=500)
    feed(b, [(H0 + 30_000, "1.10000"), (H0 + MIN - 200, "1.10010")])
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    bid, = bars_of(b.take_closed(), M1)
    assert bid.boundary
    c = builder(resolution_ms=500)
    feed(c, [(H0 + 30_000, "1.10000")])
    c.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    assert not bars_of(c.take_closed(), M1)[0].boundary


def test_reset_forming_blinds_and_drops_prices() -> None:
    b = builder()
    feed(b, [(H0 + 5_000, "1.10000")])
    b.mark(H0, H0 + 10_000, H0, blind=True)
    b.reset_forming()
    assert b.forming() == ()
    feed(b, [(H0 + 40_000, "1.20000")])
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    bid, = bars_of(b.take_closed(), M1)
    assert bid.bar.quality is Quality.BLIND and bid.bar.open == p("1.20000") and bid.bar.tick_count == 1


def test_minute_without_ticks_gives_no_bar_and_no_harm_to_the_roll_up() -> None:
    b = builder()
    feed(b, [(H0 + 5_000, "1.10000"), (H0 + 4 * MIN + 5_000, "1.10010")])
    b.finalize_until(H0 + 5 * MIN, H0 + 5 * MIN + 5_000)
    out = b.take_closed()
    assert len(bars_of(out, M1)) == 2
    five, = bars_of(out, M5)
    assert five.bar.quality is Quality.COMPLETE and five.bar.tick_count == 2 and five.close_eligible


def test_pending_buffer_is_bounded() -> None:
    b = TickBarBuilder("mock", "SYN1", covered=always, filtered=False, max_pending=4)
    feed(b, synthetic(5, per_minute=1))
    b.finalize_until(H0 + 5 * MIN, H0 + 6 * MIN)
    assert len(b.take_closed()) == 4 and b.pending_dropped > 0


def test_bars_never_show_prices_in_repr() -> None:
    b = builder()
    feed(b, [(H0 + 5_000, "1.23456")])
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    for t in b.take_closed():
        assert "1.23456" not in repr(t) and "1.23476" not in repr(t)


@pytest.mark.parametrize(("a", "b", "w"), [
    (Quality.COMPLETE, Quality.PARTIAL, Quality.PARTIAL),
    (Quality.DEGRADED, Quality.PARTIAL, Quality.DEGRADED),
    (Quality.BLIND, Quality.DEGRADED, Quality.BLIND),
    (Quality.NO_TRADE, Quality.COMPLETE, Quality.NO_TRADE),
])
def test_worst(a: Quality, b: Quality, w: Quality) -> None:
    assert worst(a, b) is w


# ---------------------------------------------------------------------------------------------------- median spread (S17b)
def test_median_spread_is_the_lower_median_of_the_bucket_ticks() -> None:
    b = builder()
    for i, spread in enumerate(("0.0002", "0.0001", "0.0003", "0.0001")):
        b.add_tick(p("1.10000"), p("1.10000") + p(spread), H0 + 1_000 + i * 10_000, H0 + 2_000 + i * 10_000)
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    out = b.take_closed()
    bid, = bars_of(out, M1, Side.BID)
    ask, = bars_of(out, M1, Side.ASK)
    assert bid.median_spread == p("0.0001") and ask.median_spread == p("0.0001")    # lower median of 1, 1, 2, 3


def test_rollup_median_spread_merges_its_children() -> None:
    b = builder()
    spreads = {0: ("0.0001", "0.0001", "0.0001"), 1: ("0.0004", "0.0004"), 2: ("0.0004", "0.0004")}
    for minute, values in spreads.items():
        for i, spread in enumerate(values):
            ts = H0 + minute * MIN + 1_000 + i * 5_000
            b.add_tick(p("1.10000"), p("1.10000") + p(spread), ts, ts + 50)
    b.finalize_until(H0 + 15 * MIN, H0 + 15 * MIN + 5_000)
    out = b.take_closed()
    q15, = bars_of(out, M15)
    assert q15.median_spread == p("0.0004")              # 3 × 1, 4 × 4: rank 4 of 7
    assert [t.median_spread for t in bars_of(out, M1)] == [p("0.0001"), p("0.0004"), p("0.0004")]


def test_median_spread_unknown_past_the_distinct_value_bound() -> None:
    from src.marketdata.tickbars import SPREAD_KINDS_MAX
    b = builder()
    for i in range(SPREAD_KINDS_MAX + 1):
        b.add_tick(p("1.10000"), p("1.10000") + p("0.00001") * (i + 1), H0 + 100 + i * 500, H0 + 200 + i * 500)
    b.finalize_until(H0 + 15 * MIN, H0 + 15 * MIN + 5_000)
    out = b.take_closed()
    assert all(t.median_spread is None for t in out)     # the minute and every roll-up holding it


def test_median_spread_never_in_repr() -> None:
    b = builder()
    b.add_tick(p("1.20000"), p("1.20037"), H0 + 5_000, H0 + 5_050)
    b.finalize_until(H0 + MIN, H0 + MIN + 5_000)
    for t in b.take_closed():
        assert t.median_spread == p("0.00037") and "0.00037" not in repr(t)
