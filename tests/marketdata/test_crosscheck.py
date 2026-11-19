"""The D6 cross-check: per-field eligibility, tolerance, shift / swap rule with the margin and the quiet-range skip,
the mismatch streak and its fault, the skipped ratio and its page, the D6 status, the TICK book, and the golden
runs through `md-bars` (plan S17b; design §A4 Cross-check, §B14; R11-M01, R11-M02). Synthetic instruments
(`mock:SYN1`) and synthetic prices only; no price appears in an assertion message."""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest

from src.dto.bars import BrokerBar
from src.marketdata.bootstrap import SeriesStateBook
from src.marketdata.brokerbars import BrokerBarsSettings, MdBars, demanded
from src.marketdata.crosscheck import (
    MARGIN,
    CrossChecker,
    D6Status,
    Field,
    Kind,
    SkipReason,
    TickRef,
    TickRefBook,
    TickSource,
    Verdict,
    eligible_fields,
    evaluate,
)
from src.marketdata.tickbars import TickBar, TickBarBuilder
from src.marketdata.types import M1, M15, Bar, Quality, SeriesId, Side, Source

from .conftest import KEYRING, RunIds
from .test_brokerbars import TF, Broker, Clock, Pages, Sink, Stored, T
from .test_writer import dto as writer_dto
from .test_writer import rig as writer_rig

AD, SYM = "mock", "SYN1"
BROKER_BID = SeriesId(AD, SYM, M15, Side.BID, Source.BROKER)
BROKER_ASK = SeriesId(AD, SYM, M15, Side.ASK, Source.BROKER)
TICK_BID = SeriesId(AD, SYM, M15, Side.BID, Source.TICK)
TICK_ASK = SeriesId(AD, SYM, M15, Side.ASK, Source.TICK)
S0 = T - 20 * TF                     # a bucket start on the 15m grid
SPREAD = Decimal("0.0002")
K1 = Decimal("1.0")


def d(x: str) -> Decimal:
    return Decimal(x)


def tbar(series: SeriesId, start: int, o: str, h: str, low: str, c: str, *, quality: Quality = Quality.COMPLETE,
         revision: int = 0, final: bool = True) -> Bar:
    return Bar(series=series, bucket_start_ms=start, open=d(o), high=d(h), low=d(low), close=d(c), tick_count=9,
               quality=quality, revision=revision, final_at_ms=start + TF + 5_000 if final else None)


def tref(start: int, o: str, h: str, low: str, c: str, *, side: Side = Side.BID, eligible: bool = True,
         quality: Quality = Quality.COMPLETE, spread: Decimal | None = SPREAD) -> TickRef:
    return TickRef(tbar(TICK_BID if side is Side.BID else TICK_ASK, start, o, h, low, c, quality=quality), eligible,
                   spread)


def bbar(start: int, o: str, h: str, low: str, c: str, series: SeriesId = BROKER_BID) -> Bar:
    return tbar(series, start, o, h, low, c)


# ---------------------------------------------------------------------------------------------------- eligibility
@pytest.mark.parametrize(("eligible", "quality", "fields"), [
    (True, Quality.PARTIAL, (Field.CLOSE,)),
    (False, Quality.COMPLETE, (Field.HIGH, Field.LOW)),
    (True, Quality.COMPLETE, (Field.CLOSE, Field.HIGH, Field.LOW)),
    (False, Quality.PARTIAL, ()),
    (True, Quality.DEGRADED, (Field.CLOSE,)),
])
def test_close_only_when_close_eligible_and_high_low_only_when_complete(eligible: bool, quality: Quality,
                                                                      fields: tuple[Field, ...]) -> None:
    own = tref(S0, "1.10000", "1.10100", "1.09900", "1.10050", eligible=eligible, quality=quality)
    assert eligible_fields(own) == fields
    result = evaluate(bbar(S0, "1.10000", "1.10100", "1.09900", "1.10050"), own, k=K1)
    assert result.verdict is (Verdict.MATCH if fields else Verdict.SKIPPED)
    if not fields:
        assert result.skip is SkipReason.NOT_ELIGIBLE


def test_an_ineligible_field_never_decides() -> None:
    own = tref(S0, "1.10000", "1.10100", "1.09900", "1.10050", quality=Quality.PARTIAL)   # filtered: close only
    far_high = bbar(S0, "1.10000", "1.10900", "1.09900", "1.10050")
    assert evaluate(far_high, own, k=K1).verdict is Verdict.MATCH


def test_no_tick_bar_or_no_spread_is_skipped() -> None:
    broker = bbar(S0, "1.10000", "1.10100", "1.09900", "1.10050")
    assert evaluate(broker, None, k=K1).skip is SkipReason.NO_TICK_BAR
    for spread in (None, Decimal(0), Decimal("NaN")):
        own = tref(S0, "1.10000", "1.10100", "1.09900", "1.10050", spread=spread)
        assert evaluate(broker, own, k=K1).skip is SkipReason.NO_SPREAD


# ---------------------------------------------------------------------------------------------------- tolerance
def test_within_tolerance_matches_and_one_full_tolerance_away_does_not() -> None:
    own = tref(S0, "1.10000", "1.10100", "1.09900", "1.10050")
    assert evaluate(bbar(S0, "1.10000", "1.10110", "1.09890", "1.10060"), own, k=K1).verdict is Verdict.MATCH
    r = evaluate(bbar(S0, "1.10000", "1.10100", "1.09900", "1.10070"), own, k=K1)
    assert r.verdict is Verdict.MISMATCH and r.kind is Kind.VALUE
    assert evaluate(bbar(S0, "1.10000", "1.10100", "1.09900", "1.10070"), own, k=Decimal("1.5")).verdict is \
        Verdict.MATCH


def test_a_bar_within_tolerance_of_its_own_tick_bar_is_always_a_match() -> None:
    own = tref(S0, "1.10000", "1.10100", "1.09900", "1.10050")
    prev = tref(S0 - TF, "1.09900", "1.10100", "1.09800", "1.10060")
    broker = bbar(S0, "1.10000", "1.10100", "1.09900", "1.10060")          # prev fits exactly, own within tol
    assert evaluate(broker, own, k=K1, prev=prev).verdict is Verdict.MATCH


# ---------------------------------------------------------------------------------------------------- shift / swap
def test_shift_counts_only_when_the_neighbour_passes_by_the_margin() -> None:
    own = tref(S0, "1.10000", "1.10100", "1.09900", "1.10050")             # range 5 spreads: shift test runs
    prev = tref(S0 - TF, "1.09900", "1.10000", "1.09800", "1.09950")
    nxt = tref(S0 + TF, "1.10050", "1.10200", "1.10000", "1.10150")
    shifted_late = bbar(S0, "1.09900", "1.10000", "1.09800", "1.09950")    # bucket T carries T − 1
    r = evaluate(shifted_late, own, k=K1, prev=prev, nxt=nxt)
    assert (r.verdict, r.kind, r.shift_tested) == (Verdict.MISMATCH, Kind.SHIFT_PREV, True)
    shifted_early = bbar(S0, "1.10050", "1.10200", "1.10000", "1.10150")
    assert evaluate(shifted_early, own, k=K1, prev=prev, nxt=nxt).kind is Kind.SHIFT_NEXT
    # the neighbour passes the tolerance, but with less than 0.5 × tolerance to spare: a plain value mismatch
    near = tref(S0 - TF, "1.09900", "1.10000", "1.09800", "1.09965")
    assert MARGIN == Decimal("0.5")
    r = evaluate(shifted_late, own, k=K1, prev=near)
    assert (r.verdict, r.kind) == (Verdict.MISMATCH, Kind.VALUE)


def test_the_shift_test_is_skipped_on_a_quiet_bucket() -> None:
    own = tref(S0, "1.10000", "1.10040", "1.09990", "1.10000")             # range 2.5 spreads (< 3)
    prev = tref(S0 - TF, "1.10030", "1.10040", "1.09990", "1.10030")
    r = evaluate(bbar(S0, "1.10030", "1.10040", "1.09990", "1.10030"), own, k=K1, prev=prev)
    assert (r.verdict, r.kind, r.shift_tested) == (Verdict.MISMATCH, Kind.VALUE, False)


def test_a_bid_ask_swap_is_detected_even_on_a_quiet_bucket() -> None:
    own = tref(S0, "1.10000", "1.10010", "1.09990", "1.10000")
    other = tref(S0, "1.10020", "1.10030", "1.10010", "1.10020", side=Side.ASK)
    r = evaluate(bbar(S0, "1.10020", "1.10030", "1.10010", "1.10020"), own, k=K1, other=other)
    assert (r.verdict, r.kind) == (Verdict.MISMATCH, Kind.SWAP)


# ---------------------------------------------------------------------------------------------------- checker
@dataclass
class Rig:
    book: TickRefBook
    xc: CrossChecker
    pages: Pages
    faults: list[tuple[SeriesId, str]] = field(default_factory=list)


def rig(*, k: int = 3, window: int = 16, wait_ms: int = 60_000,
        fault: Callable[[SeriesId, str], None] | None = None) -> Rig:
    book = TickRefBook()
    pages = Pages()
    faults: list[tuple[SeriesId, str]] = []
    xc = CrossChecker(book, k=K1, mismatch_k=k, window=window, tick_wait_ms=wait_ms, pager=pages,
                      on_fault=fault or (lambda s, why: faults.append((s, why))))
    return Rig(book, xc, pages, faults)


def note(book: TickRefBook, start: int, *, side: Side = Side.BID, eligible: bool = True,
         quality: Quality = Quality.COMPLETE, revision: int = 0, close: str = "1.10050") -> None:
    series = TICK_BID if side is Side.BID else TICK_ASK
    book.note(TickBar(tbar(series, start, "1.10000", "1.10100", "1.09900", close, quality=quality,
                           revision=revision), eligible, False, SPREAD))


MATCH_BAR = ("1.10000", "1.10100", "1.09900", "1.10050")
MISS_BAR = ("1.10000", "1.10100", "1.09900", "1.10450")


def submit(r: Rig, i: int, bar: tuple[str, str, str, str], *, live: bool = True,
           eligible: bool = True) -> object:
    start = S0 + i * TF
    note(r.book, start, eligible=eligible, quality=Quality.COMPLETE if eligible else Quality.PARTIAL)
    return r.xc.submit(bbar(start, *bar), live=live, now_ms=start + TF + 6_000)


def test_k_consecutive_mismatches_fault_once_skips_neither_flag_nor_reset_and_a_match_rearms() -> None:
    r = rig()
    submit(r, 0, MISS_BAR)
    submit(r, 1, MISS_BAR)
    submit(r, 2, MISS_BAR, eligible=False)               # skipped: neither flags nor resets
    assert r.xc.mismatch_streak(BROKER_BID) == 2 and r.faults == []
    submit(r, 3, MISS_BAR)
    assert r.faults == [(BROKER_BID, "source_mismatch")] and r.xc.blocked(BROKER_BID)
    submit(r, 4, MISS_BAR)
    assert len(r.faults) == 1                            # once per streak
    submit(r, 5, MATCH_BAR)
    assert r.xc.mismatch_streak(BROKER_BID) == 0 and not r.xc.blocked(BROKER_BID)
    for i in (6, 7, 8):
        submit(r, i, MISS_BAR)
    assert len(r.faults) == 2 and r.xc.counters.faults == 2
    assert r.xc.caveat(BROKER_BID, S0 + 6 * TF) and not r.xc.caveat(BROKER_BID, S0 + 5 * TF)
    assert r.xc.caveat(BROKER_BID, S0 + 2 * TF) is False


def test_streak_follows_bucket_order_not_arrival_order() -> None:
    r = rig()
    submit(r, 5, MATCH_BAR)
    for i in (2, 3, 4):                                  # older buckets resolved later (the lane)
        submit(r, i, MISS_BAR, live=False)
    assert r.xc.mismatch_streak(BROKER_BID) == 0 and r.faults == []


def test_a_live_bar_waits_for_its_tick_bar_and_is_skipped_when_it_never_comes() -> None:
    r = rig(wait_ms=60_000)
    now = S0 + TF + 6_000
    assert r.xc.submit(bbar(S0, *MATCH_BAR), live=True, now_ms=now) is None
    assert r.xc.submit(bbar(S0 + TF, *MATCH_BAR), live=True, now_ms=now) is None
    note(r.book, S0)
    r.xc.poll(now + 1_000)
    res = r.xc.result(BROKER_BID, S0)
    assert res is not None and res.verdict is Verdict.MATCH
    assert r.xc.result(BROKER_BID, S0 + TF) is None      # still waiting
    r.xc.poll(now + 60_000)
    res = r.xc.result(BROKER_BID, S0 + TF)
    assert res is not None and res.skip is SkipReason.NO_TICK_BAR
    assert r.xc.counters.waited == 2 and r.xc.skipped_recent(BROKER_BID) == 1


def test_a_history_bar_without_its_tick_bar_is_not_checked_or_counted() -> None:
    r = rig()
    assert r.xc.submit(bbar(S0, *MISS_BAR), live=False, now_ms=S0 + 10 * TF) is None
    assert r.xc.counters.history_unchecked == 1 and r.xc.skipped_recent(BROKER_BID) == 0
    st = r.xc.status(BROKER_BID)
    assert st is not None and st.skipped == 0 and st.pending == 0


def test_a_broker_spec_without_a_tick_spec_is_skipped() -> None:
    r = rig()
    series = SeriesId(AD, SYM, M1, Side.BID, Source.BROKER)
    res = r.xc.submit(tbar(series, S0, *MATCH_BAR), live=True, now_ms=S0 + TF)
    assert res is not None and res.skip is SkipReason.NO_TICK_SPEC


def test_skipped_ratio_pages_once_above_half_of_the_last_16_and_rearms() -> None:
    r = rig()
    for i in range(16):
        submit(r, i, MATCH_BAR)
    for i in range(16, 24):
        submit(r, i, MATCH_BAR, eligible=False)          # 8 of the last 16: exactly half, no page
    assert r.pages.sent == [] and r.xc.skipped_recent(BROKER_BID) == 8
    submit(r, 24, MATCH_BAR, eligible=False)             # 9 of the last 16
    assert r.xc.skipped_recent(BROKER_BID) == 9
    assert r.pages.titled("tick cross-check skipped") == 1
    for i in range(25, 28):
        submit(r, i, MATCH_BAR, eligible=False)
    assert r.pages.titled("tick cross-check skipped") == 1    # once per episode
    for i in range(28, 44):
        submit(r, i, MATCH_BAR)
    assert r.xc.skipped_recent(BROKER_BID) == 0
    for i in range(44, 53):
        submit(r, i, MATCH_BAR, eligible=False)
    assert r.pages.titled("tick cross-check skipped") == 2 and r.xc.counters.skip_pages == 2


def test_d6_status_follows_what_the_window_could_check() -> None:
    r = rig(window=4)
    assert r.xc.d6(BROKER_BID) is D6Status.UNAVAILABLE
    for i in range(4):                                   # filtered delivery: close only
        note(r.book, S0 + i * TF, quality=Quality.PARTIAL)
        r.xc.submit(bbar(S0 + i * TF, *MATCH_BAR), live=True, now_ms=S0 + 30 * TF)
    assert r.xc.d6(BROKER_BID) is D6Status.CLOSE_ONLY
    note(r.book, S0 + 4 * TF)
    r.xc.submit(bbar(S0 + 4 * TF, *MATCH_BAR), live=True, now_ms=S0 + 30 * TF)
    assert r.xc.d6(BROKER_BID) is D6Status.ACTIVE
    for i in range(5, 9):
        note(r.book, S0 + i * TF, eligible=False, quality=Quality.DEGRADED)
        r.xc.submit(bbar(S0 + i * TF, *MATCH_BAR), live=True, now_ms=S0 + 30 * TF)
    assert r.xc.d6(BROKER_BID) is D6Status.UNAVAILABLE


def test_meta_pairs_and_status_carry_states_and_counts_only() -> None:
    r = rig()
    zero = {"d6": "UNAVAILABLE", "xc_checked": "0", "xc_k": "0", "xc_mismatched": "0", "xc_skip_recent": "0",
            "xc_skipped": "0"}
    assert dict(r.xc.meta_pairs(BROKER_BID)) == zero        # every field: no stale count stays in `md:meta`
    submit(r, 0, MISS_BAR)
    submit(r, 1, MATCH_BAR, eligible=False)
    pairs = dict(r.xc.meta_pairs(BROKER_BID))
    assert pairs == {"d6": "ACTIVE", "xc_checked": "1", "xc_k": "1", "xc_mismatched": "1", "xc_skip_recent": "1",
                     "xc_skipped": "1"}
    st = r.xc.status(BROKER_BID)
    assert st is not None and (st.d6, st.mismatch_streak, st.blocked) == (D6Status.ACTIVE, 1, False)
    now = S0 + 3 * TF
    r.xc.forget(BROKER_BID, now_ms=now)
    r.xc.poll(now + TF)                                      # undemanded for one timeframe: the state goes
    assert r.xc.status(BROKER_BID) is None and dict(r.xc.meta_pairs(BROKER_BID)) == zero


def test_a_brief_lapse_of_demand_keeps_a_partial_streak() -> None:
    r = rig()
    submit(r, 0, MISS_BAR)
    submit(r, 1, MISS_BAR)
    now = S0 + 2 * TF + 6_000
    r.xc.forget(BROKER_BID, now_ms=now)                       # an S18 lease renewed late
    r.xc.poll(now + 60_000)
    r.xc.resume(BROKER_BID)
    r.xc.poll(now + TF + 1)                                   # demanded again: no grace runs out
    assert r.xc.mismatch_streak(BROKER_BID) == 2
    submit(r, 2, MISS_BAR)
    assert r.faults == [(BROKER_BID, "source_mismatch")]
    r.xc.forget(BROKER_BID, now_ms=now + 2 * TF)
    submit(r, 3, MISS_BAR)                                    # a bar of the series is a demand too
    r.xc.poll(now + 4 * TF)
    assert r.xc.mismatch_streak(BROKER_BID) == 4


def test_an_operator_clear_restarts_the_streak_and_rearms_the_fault() -> None:
    r = rig()
    for i in range(3):
        submit(r, i, MISS_BAR)
    assert r.faults == [(BROKER_BID, "source_mismatch")] and r.xc.blocked(BROKER_BID)
    r.xc.clear(BROKER_BID)                                    # S30: the operator cleared FAILED
    assert r.xc.mismatch_streak(BROKER_BID) == 0 and not r.xc.blocked(BROKER_BID)
    assert r.xc.caveat(BROKER_BID, S0 + 2 * TF)               # verdicts and caveats stay
    submit(r, 1, MISS_BAR, live=False)                        # an older bucket re-judged: below the floor
    submit(r, 3, MISS_BAR)
    submit(r, 4, MISS_BAR)
    assert r.xc.mismatch_streak(BROKER_BID) == 2 and len(r.faults) == 1
    submit(r, 5, MISS_BAR)                                    # the feed is still wrong: FAILED again
    assert len(r.faults) == 2 and r.xc.blocked(BROKER_BID)
    r.xc.clear(SeriesId(AD, "SYN9", M15, Side.BID, Source.BROKER))     # no state: nothing to clear


def test_poll_checks_every_series_when_one_tick_lookup_raises() -> None:
    other = SeriesId(AD, "SYN2", M15, Side.BID, Source.BROKER)
    book = TickRefBook()

    class Flaky:
        def get(self, series: SeriesId, bucket_start_ms: int) -> TickRef | None:
            if series.symbol == "SYN2" and flaky:
                raise RuntimeError("tick book fault")
            return book.get(series, bucket_start_ms)

    flaky = False
    xc = CrossChecker(Flaky(), k=K1, mismatch_k=3, tick_wait_ms=60_000)
    now = S0 + TF + 6_000
    assert xc.submit(tbar(other, S0, *MATCH_BAR), live=True, now_ms=now) is None        # both wait
    assert xc.submit(bbar(S0, *MATCH_BAR), live=True, now_ms=now) is None
    note(book, S0)
    flaky = True
    xc.poll(now + 1_000)                                      # never raises
    res = xc.result(BROKER_BID, S0)
    assert res is not None and res.verdict is Verdict.MATCH   # the other series' waiting bar was checked
    res = xc.result(other, S0)
    assert res is not None and res.skip is SkipReason.NO_TICK_BAR
    assert xc.counters.errors == 1 and xc.skipped_recent(other) == 1
    st = xc.status(other)
    assert st is not None and st.pending == 0


def test_a_skipped_count_swinging_around_half_pages_once() -> None:
    r = rig()
    counts: list[int] = []
    i = 0
    for block in range(4):                                    # M = matched, S = skipped; the window slides over
        for c in ("MS" if block % 2 == 0 else "SM") * 8:      # the halves so the count swings 9, 8, 9, 8, ...
            submit(r, i, MATCH_BAR, eligible=c == "M")
            counts.append(r.xc.skipped_recent(BROKER_BID))
            i += 1
    assert max(counts) == 9 and sum(1 for a, b in zip(counts, counts[1:]) if (a, b) == (8, 9)) >= 4
    assert r.pages.titled("tick cross-check skipped") == 1    # one page per episode
    for j in range(i, i + 16):
        submit(r, j, MATCH_BAR)
    for j in range(i + 16, i + 25):
        submit(r, j, MATCH_BAR, eligible=False)
    assert r.pages.titled("tick cross-check skipped") == 2    # re-armed only once a quarter or fewer were skipped


def test_a_raising_fault_sink_is_contained() -> None:
    def boom(series: SeriesId, why: str) -> None:
        raise RuntimeError("sink down")

    r = rig(fault=boom)
    for i in range(3):
        submit(r, i, MISS_BAR)
    assert r.xc.counters.faults == 1 and r.xc.blocked(BROKER_BID)


def test_no_price_spread_or_tolerance_in_logs_or_pages(caplog: pytest.LogCaptureFixture) -> None:
    r = rig()
    with caplog.at_level(logging.INFO):
        for i in range(3):
            submit(r, i, MISS_BAR)
        for i in range(3, 12):
            submit(r, i, MATCH_BAR, eligible=False)
    text = " ".join(rec.getMessage() for rec in caplog.records) + " ".join(" ".join(p) for p in r.pages.sent)
    assert r.pages.sent and caplog.records
    for value in ("1.10", "0.0002", "0.0004", "0.0001"):
        assert value not in text


# ---------------------------------------------------------------------------------------------------- TICK book
def test_tick_book_keeps_the_newest_final_revision_of_the_cross_check_specs_bounded() -> None:
    book = TickRefBook(keep=4, max_series=1)
    note(book, S0, revision=1, close="1.10060")
    note(book, S0, revision=0, close="1.10050")          # an older revision arriving late
    got = book.get(TICK_BID, S0)
    assert got is not None and got.bar.revision == 1
    note(book, S0, revision=2, close="1.10070")
    got = book.get(TICK_BID, S0)
    assert got is not None and got.bar.revision == 2
    book.note(TickBar(tbar(TICK_BID, S0 + TF, *MATCH_BAR, final=False), True, False, SPREAD))     # not final
    book.note(TickBar(tbar(SeriesId(AD, SYM, M1, Side.BID, Source.TICK), S0, *MATCH_BAR), True, False, SPREAD))
    book.note(TickBar(tbar(BROKER_BID, S0, *MATCH_BAR), True, False, SPREAD))
    assert book.get(TICK_BID, S0 + TF) is None and book.series() == (TICK_BID,)
    note(book, S0, side=Side.ASK)                        # a second series past `max_series`
    assert book.get(TICK_ASK, S0) is None and book.refused_series == 1
    for i in range(1, 10):
        note(book, S0 + i * TF)
    assert book.get(TICK_BID, S0) is None and book.get(TICK_BID, S0 + 9 * TF) is not None
    with pytest.raises(ValueError):
        TickRefBook(keep=2)


# ---------------------------------------------------------------------------------------------------- golden: md-bars
def build_ticks(lo: int, hi: int, level: Callable[[int, int], Decimal], *, filtered: bool) -> dict[
        tuple[Side, int], TickBar]:
    """15m TICK bars of `[lo, hi]` from the real builder: two ticks a minute, bid `level(bucket, minute)`, ask one
    synthetic spread above."""
    b = TickBarBuilder(AD, SYM, covered=lambda a, z: True, filtered=filtered)
    for start in range(lo, hi + 1, TF):
        for minute in range(15):
            for second in (10, 50):
                ts = start + minute * 60_000 + second * 1_000
                bid = level(start, minute)
                b.add_tick(bid, bid + SPREAD, ts, ts + 40)
    b.finalize_until(hi + TF, hi + TF + 5_000)
    return {(t.bar.series.side, t.bar.bucket_start_ms): t for t in b.take_closed() if t.bar.series.spec == M15}


def as_broker(tb: TickBar, start: int, *, close_shift: Decimal = Decimal(0)) -> BrokerBar:
    bar = tb.bar
    return BrokerBar(bucket_start_ms=start, open=bar.open, high=max(bar.high, bar.close + close_shift),
                     low=min(bar.low, bar.close + close_shift), close=bar.close + close_shift)


@dataclass
class Golden:
    bars: MdBars
    xc: CrossChecker
    clock: Clock
    sink: Sink
    pages: Pages
    faults: list[tuple[SeriesId, str]]

    async def run_to(self, until: int) -> None:
        while self.clock.now <= until:
            await self.bars.step()
            self.clock.now += 1_000


def golden(ticks: dict[tuple[Side, int], TickBar], broker_of: Callable[[int], BrokerBar | None], *,
           noted_from: int, series: SeriesId = BROKER_BID, source: TickSource | None = None) -> Golden:
    clock = Clock(T - 60_000)
    broker = Broker(clock)
    for start in range(T - 41 * TF, T + 20 * TF, TF):
        got = broker_of(start)
        if got is not None:
            broker.bars[(SYM, start)] = got
    book = TickRefBook()
    for (_, start), tb in sorted(ticks.items(), key=lambda kv: kv[0][1]):
        if start >= noted_from:                          # the hook started at `noted_from`
            book.note(tb)
    sink, pages = Sink(), Pages()
    faults: list[tuple[SeriesId, str]] = []
    xc = CrossChecker(source or book, k=K1, mismatch_k=3, pager=pages, on_fault=lambda s, why: faults.append((s, why)))
    bars = MdBars(broker, sink, SeriesStateBook(), keyring=KEYRING, demand=demanded([(series, 4)]),
                  settings=BrokerBarsSettings(), stored=Stored(sink), pager=pages,
                  on_fault=lambda s, why: faults.append((s, why)), crosscheck=xc, trusts_next_bucket=lambda a: True,
                  clock_ms=clock)
    return Golden(bars, xc, clock, sink, pages, faults)


B0 = T - TF                                              # the first live bucket (bar end T)


def trending(start: int, minute: int) -> Decimal:
    return Decimal("1.10000") + Decimal("0.0001") * ((start - B0) // 60_000 + minute)


async def test_golden_filtered_feed_shifted_one_bucket_reaches_source_mismatch_within_3_buckets() -> None:
    ticks = build_ticks(B0 - 6 * TF, B0 + 8 * TF, trending, filtered=True)
    g = golden(ticks, lambda s: as_broker(ticks[(Side.BID, s - TF)], s) if (Side.BID, s - TF) in ticks else None,
               noted_from=B0)
    await g.run_to(T + TF + 10_000)                      # buckets B0, B0 + TF final: two mismatches
    assert g.faults == [] and g.xc.mismatch_streak(BROKER_BID) == 2
    await g.run_to(T + 2 * TF + 10_000)                  # the third
    assert g.faults == [(BROKER_BID, "source_mismatch")]
    assert g.xc.d6(BROKER_BID) is D6Status.CLOSE_ONLY    # filtered: only the close is ever eligible
    assert g.xc.counters.shift_prev >= 1 and g.xc.counters.history_unchecked >= 1   # backfill: no TICK bar
    meta = g.sink.meta[BROKER_BID]
    assert meta["d6"] == "CLOSE_ONLY" and meta["xc_k"] == "3"
    status = g.bars.status(BROKER_BID)
    assert status is not None and (status.d6, status.mismatch_streak) == ("CLOSE_ONLY", 3)


async def test_golden_flat_asian_session_with_equal_closes_never_flags() -> None:
    def flat(start: int, minute: int) -> Decimal:
        return Decimal("1.10005") if minute % 2 else Decimal("1.10000")     # equal closes every bucket

    ticks = build_ticks(B0 - 6 * TF, B0 + 10 * TF, flat, filtered=False)
    g = golden(ticks, lambda s: as_broker(ticks[(Side.BID, s)], s, close_shift=Decimal("0.0001"))
               if (Side.BID, s) in ticks else None, noted_from=B0 - 6 * TF)
    await g.run_to(T + 8 * TF + 10_000)
    c = g.xc.counters
    assert c.mismatches == 0 and c.matches >= 8 and g.faults == []
    assert g.xc.d6(BROKER_BID) is D6Status.ACTIVE and g.pages.titled("tick cross-check skipped") == 0


async def test_golden_filtered_bid_ask_swap_is_detected() -> None:
    ticks = build_ticks(B0 - 6 * TF, B0 + 8 * TF, trending, filtered=True)
    g = golden(ticks, lambda s: as_broker(ticks[(Side.ASK, s)], s) if (Side.ASK, s) in ticks else None,
               noted_from=B0)
    await g.run_to(T + 2 * TF + 10_000)
    assert g.xc.counters.swap >= 3 and g.faults == [(BROKER_BID, "source_mismatch")]


async def test_a_failing_tick_source_never_stops_md_bars() -> None:
    class Broken:
        def get(self, series: SeriesId, bucket_start_ms: int) -> TickRef | None:
            raise RuntimeError("tick book fault")

    ticks = build_ticks(B0 - 6 * TF, B0 + 2 * TF, trending, filtered=True)
    g = golden(ticks, lambda s: as_broker(ticks[(Side.BID, s)], s) if (Side.BID, s) in ticks else None,
               noted_from=B0, source=Broken())
    await g.run_to(T + 10_000)
    assert g.sink.of(BROKER_BID, B0) and g.faults == []      # the bar was queued all the same
    assert g.xc.counters.checks == 0


# ---------------------------------------------------------------------------------------------------- golden: real tap
async def test_golden_real_writer_tap_shifted_feed_reaches_source_mismatch_within_3_buckets(
        md_redis: Any, run_ids: RunIds) -> None:
    """The path S31 wires: ticks reach the real `MdHook`, `md-writer` (fakeredis) drains each closed TICK bar into
    the book through `on_closed_tick` only once the hook finalises it, and `md-bars` checks its BROKER bars over
    simulated time. The T + 1 neighbour is never in the book when bucket T is checked (it is still forming), so a
    feed one bucket late reads as `shift_prev`, never `shift_next`."""
    truth = build_ticks(B0 - 6 * TF, B0 + 8 * TF, trending, filtered=True)
    start = B0 - 10_000
    w = await writer_rig(md_redis, run_ids, start=start)
    w.hook.set_unfiltered(AD, False)                         # filtered delivery: the close is the eligible field
    clock = w.clock
    book = TickRefBook()
    w.writer.on_closed_tick(book.note)
    broker = Broker(clock)                                   # type: ignore[arg-type]
    for s in range(T - 41 * TF, T + 20 * TF, TF):
        if (Side.BID, s - TF) in truth:                       # the feed carries bucket T - 1 at T
            broker.bars[(SYM, s)] = as_broker(truth[(Side.BID, s - TF)], s)
    sink, pages = Sink(), Pages()
    faults: list[tuple[SeriesId, str, int]] = []
    xc = CrossChecker(book, k=K1, mismatch_k=3, pager=pages,
                      on_fault=lambda s, why: faults.append((s, why, clock.now)))
    bars = MdBars(broker, sink, SeriesStateBook(), keyring=KEYRING, demand=demanded([(BROKER_BID, 4)]),
                  settings=BrokerBarsSettings(), stored=Stored(sink), pager=pages,
                  on_fault=lambda s, why: None, crosscheck=xc, trusts_next_bucket=lambda a: True,
                  clock_ms=clock)
    feed_ticks = sorted((start_ms + minute * 60_000 + second * 1_000, trending(start_ms, minute))
                        for start_ms in range(B0, B0 + 4 * TF, TF) for minute in range(15) for second in (10, 50))
    nxt_at_check: dict[int, bool] = {}
    end = T + 2 * TF + 130_000
    now = start
    while now <= end:
        while feed_ticks and feed_ticks[0][0] + 100 <= now:
            ts, bid = feed_ticks.pop(0)
            w.hook.on_price(writer_dto(str(bid), ts))
        await w.at(now)
        if now >= T - 60_000:
            await bars.step()
            for b in range(B0, B0 + 3 * TF, TF):
                if b not in nxt_at_check and xc.result(BROKER_BID, b) is not None:
                    nxt_at_check[b] = book.get(TICK_BID, b + TF) is not None
        now += 5_000
    assert book.noted > 0 and w.writer.counters.tick_listener_errors == 0
    for b in range(B0, B0 + 3 * TF, TF):
        res = xc.result(BROKER_BID, b)
        assert res is not None and res.verdict is Verdict.MISMATCH, f"bucket {(b - B0) // TF}"
    assert nxt_at_check == {B0: False, B0 + TF: False, B0 + 2 * TF: False}     # T + 1 was still forming
    assert [(s, why) for s, why, _ in faults] == [(BROKER_BID, "source_mismatch")]
    assert T + 2 * TF <= faults[0][2] < T + 3 * TF                               # the third live bucket
    c = xc.counters
    assert c.shift_next == 0 and c.shift_prev >= 2                              # B0 has no T - 1 in the book
    assert xc.d6(BROKER_BID) is D6Status.CLOSE_ONLY
