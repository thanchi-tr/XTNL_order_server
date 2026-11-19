"""Replay, frontier spans, the Friday-close REOPEN rule and the pull cursor (plan S22; design §B6.6, §B5.2, §A13).

Times are synthetic bar ends on the 15m grid (open around the clock), or New York times on an FX-style calendar
(`fx_open_ends`: open Sunday 17:00 to Friday 17:00, a 17:00-17:05 daily break, so the 17:00-17:15 bucket is still a
bar); the skew is the cold-start one (p50 0, settle 5 s). A tiny BarClock driver (`drive`) applies `next_step` /
`advance` as S27 will: spans commit, a `Decide` calls the fake decider, a POST / LATE that did not decide, a `Wait`,
`Hold` or None ends the pass, and a REOPEN that did not decide is marked not due for the rest of the pass.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.ports import EventsRead
from src.marketdata.timing import COLD_SKEW, SkewEstimate, decision_cap_ms
from src.marketdata.types import (
    M15,
    BarEndEvent,
    CursorExpired,
    DataUnavailable,
    EventCursor,
    MdGeneration,
    SeriesId,
    Source,
)
from src.marketdata.types import Side as MdSide
from src.riskmgr.outcomes import ARMED_LATE, BROKER_BAR_NOT_FINAL, DOWNTIME, FAILED, PAUSED, Category
from src.riskmgr.replay import (
    AWAITING_INPUTS,
    FIRST_BAR_STALE,
    REOPEN_PENDING,
    BarSlot,
    Decide,
    DropRecheck,
    EventAction,
    Frontier,
    Hold,
    ReopenCheck,
    SeriesCursor,
    Span,
    Step,
    Wait,
    advance,
    arming_frontier,
    classify_event,
    in_bar_order,
    newest_expired_bar_end,
    next_step,
    reopen_after_closed_post,
)
from src.riskmgr.types import ArmEvent

Q = M15.timeframe_ms
SETTLE = COLD_SKEW.settle_ms
NY = ZoneInfo("America/New_York")
C0 = 1_700_000_100_000 - 1_700_000_100_000 % Q          # a bar end on the grid
SERIES = SeriesId("mock", "SYN.A", M15, MdSide.BID, Source.BROKER)
KEY1, KEY2 = b"\x01" * 32, b"\x02" * 32
RING1 = Keyring.of(1, KEY1)
RING2 = Keyring.of(2, KEY2, [(1, KEY1)])
GEN1, GEN2 = MdGeneration(1, 1), MdGeneration(1, 2)


def h(bar_end: int, close: str = "1.2", ring: Keyring = RING1) -> str:
    from decimal import Decimal
    c = Decimal(close)
    return content_hash(ring, SERIES.canonical, bar_end, c, c, c, c)


def ny_ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=NY).timestamp() * 1000)


def grid(after: int, upto: int) -> list[int]:
    """Every bar end in `(after, upto]` on a calendar that is always open."""
    return list(range(after + Q, upto + 1, Q))


def fx_open_ends(after: int, upto: int) -> list[int]:
    """The open bar ends in `(after, upto]` on the FX-style calendar: a bucket is a bar when any instant in it is
    open (Sunday 17:00 to Friday 17:00 New York; the 17:00-17:05 daily break leaves the 17:00-17:15 bucket open)."""
    out = []
    for end in grid(after, upto):
        start = datetime.fromtimestamp((end - Q) / 1000, tz=NY)
        minutes = start.hour * 60 + start.minute
        closed = (start.weekday() == 4 and minutes >= 17 * 60) or start.weekday() == 5 or (
            start.weekday() == 6 and minutes < 17 * 60)
        if not closed:
            out.append(end)
    return out


Decider = Callable[[Decide], tuple[bool, str | None]]


def drive(frontier: Frontier, slots: Sequence[BarSlot], now: int, decide: Decider, *,
          rechecks: Sequence[tuple[ReopenCheck, bool]] = (), hold: str | None = None,
          bar0: int | None = None, skew: SkewEstimate = COLD_SKEW) -> tuple[Frontier, list[Step]]:
    """One BarClock pass for one position: returns the frontier afterwards and every step taken."""
    steps: list[Step] = []
    pending = list(rechecks)
    for _ in range(1_000):
        live = [s for s in slots if s.bar_end_ms > frontier.cursor_ms]
        step = next_step(frontier, live, now, spec=M15, skew=skew, hold=hold, bar0_end_ms=bar0, rechecks=pending)
        if step is None:
            break
        steps.append(step)
        if isinstance(step, Span):
            frontier = advance(frontier, step)
            continue
        if isinstance(step, DropRecheck):
            pending = [(c, f) for c, f in pending if c.bar_end_ms != step.bar_end_ms]
            continue
        if isinstance(step, Decide):
            final, hashed = decide(step)
            if not final and step.event is ArmEvent.REOPEN:   # retryable: tried again on its own 5 s cadence
                pending = [(c, False if c.bar_end_ms == step.bar_end_ms else f) for c, f in pending]
                continue
            if not final:
                break
            frontier = advance(frontier, step, decided_hash=hashed)
            pending = [(c, f) for c, f in pending if c.bar_end_ms != step.bar_end_ms]
            continue
        break
    return frontier, steps


def always_final(step: Decide) -> tuple[bool, str | None]:
    return True, h(step.bar_end_ms)


# ---------------------------------------------------------------------------------------------------- arming
def test_arming_three_weeks_after_the_fill_writes_one_armed_late_span_and_decides_only_the_current_bar() -> None:
    fill_bucket = C0
    armed_at = C0 + 21 * 24 * 3_600_000 + 7 * 60_000
    armed = arming_frontier(fill_bucket_ms=fill_bucket, armed_at_ms=armed_at, spec=M15, skew=COLD_SKEW,
                            bar_ends=grid(fill_bucket, armed_at), bar0_end_ms=fill_bucket + Q)
    span = armed.span
    assert span is not None and span.reason == ARMED_LATE and span.code.category is Category.INFO and not span.pages
    assert span.from_bar_ms == fill_bucket + Q and span.to_bar_ms == newest_expired_bar_end(armed_at, M15, COLD_SKEW)
    assert span.first_bar_stale                       # FIRST_BAR_UNKNOWN(stale): FirstBarGuard never applies after
    assert span.first_bar_detail == FIRST_BAR_STALE
    assert span.bars == (span.to_bar_ms - fill_bucket) // Q
    assert armed.frontier == Frontier(span.to_bar_ms, None)
    # every bar ending after the cursor up to now: only the current one (its cap has not passed)
    slots = [BarSlot(e, ready=True) for e in range(armed.frontier.cursor_ms + Q, armed_at + 1, Q)]
    assert len(slots) == 1
    decided: list[Decide] = []
    after, steps = drive(armed.frontier, slots, armed_at, lambda d: (decided.append(d), always_final(d))[1])
    assert decided == [Decide(slots[0].bar_end_ms, ArmEvent.POST)]
    assert not any(isinstance(s, Span) for s in steps) and after.cursor_ms == slots[0].bar_end_ms


def test_arming_inside_the_fill_bar_starts_at_the_fill_bucket() -> None:
    armed = arming_frontier(fill_bucket_ms=C0, armed_at_ms=C0 + 60_000, spec=M15, skew=COLD_SKEW,
                            bar_ends=grid(C0 - 4 * Q, C0 + 60_000))
    assert armed.span is None and armed.frontier == Frontier(C0)


def test_arming_with_no_entry_time_starts_at_the_newest_expired_bar() -> None:
    armed = arming_frontier(fill_bucket_ms=None, armed_at_ms=C0 + 3 * Q, spec=M15, skew=COLD_SKEW,
                            bar_ends=grid(C0 - 4 * Q, C0 + 3 * Q))
    assert armed.span is None and armed.frontier.cursor_ms == newest_expired_bar_end(C0 + 3 * Q, M15, COLD_SKEW)


def test_rearming_keeps_the_stored_cursor_and_spans_the_suspension() -> None:
    stored = Frontier(C0, h(C0))
    armed = arming_frontier(fill_bucket_ms=C0 - 40 * Q, armed_at_ms=C0 + 10 * Q, spec=M15, skew=COLD_SKEW,
                            bar_ends=grid(C0 - 40 * Q, C0 + 10 * Q), previous=stored)
    assert armed.span is not None and armed.span.from_bar_ms == C0 + Q and armed.span.bars == 8
    assert armed.frontier.cursor_ms == C0 + 8 * Q and armed.frontier.last_hash is None
    same = arming_frontier(fill_bucket_ms=None, armed_at_ms=C0 + Q, spec=M15, skew=COLD_SKEW,
                           bar_ends=grid(C0, C0 + Q), previous=stored)
    assert same.span is None and same.frontier == stored


def test_arming_on_a_saturday_counts_only_open_bars_and_ends_on_fridays_last_bar() -> None:
    fill_bucket = ny_ms(2026, 11, 13, 16, 0)              # Friday 16:00-16:15 New York
    friday_last = ny_ms(2026, 11, 13, 17, 0)
    armed_at = ny_ms(2026, 11, 14, 12, 0)                 # Saturday noon: the market is closed
    armed = arming_frontier(fill_bucket_ms=fill_bucket, armed_at_ms=armed_at, spec=M15, skew=COLD_SKEW,
                            bar_ends=fx_open_ends(fill_bucket - 8 * Q, armed_at), bar0_end_ms=fill_bucket + Q)
    span = armed.span
    assert span is not None and span.bars == 4             # 16:15, 16:30, 16:45, 17:00 (not 78 grid buckets)
    assert (span.from_bar_ms, span.to_bar_ms) == (fill_bucket + Q, friday_last) and span.first_bar_stale
    assert armed.frontier == Frontier(friday_last, None)  # never a bucket of the weekend
    # re-armed later on Saturday from that cursor: no open bar in the range, so no span
    again = arming_frontier(fill_bucket_ms=fill_bucket, armed_at_ms=ny_ms(2026, 11, 14, 20, 0), spec=M15,
                            skew=COLD_SKEW, bar_ends=fx_open_ends(friday_last, ny_ms(2026, 11, 14, 20, 0)),
                            previous=armed.frontier)
    assert again.span is None and again.frontier == armed.frontier


def test_arming_at_the_sunday_open_spans_fridays_bars_and_decides_the_first_open_bar() -> None:
    fill_bucket = ny_ms(2026, 11, 13, 16, 0)
    friday_last = ny_ms(2026, 11, 13, 17, 0)
    armed_at = ny_ms(2026, 11, 15, 17, 25)                # Sunday 17:25 New York, 25 minutes after the open
    armed = arming_frontier(fill_bucket_ms=fill_bucket, armed_at_ms=armed_at, spec=M15, skew=COLD_SKEW,
                            bar_ends=fx_open_ends(fill_bucket - 8 * Q, armed_at))
    span = armed.span
    assert span is not None and span.bars == 4 and span.to_bar_ms == friday_last   # not Sunday 17:00 (closed)
    assert armed.frontier.cursor_ms == friday_last
    first_open_bar = ny_ms(2026, 11, 15, 17, 15)
    slots = [BarSlot(e, ready=True) for e in fx_open_ends(armed.frontier.cursor_ms, armed_at)]
    assert [s.bar_end_ms for s in slots] == [first_open_bar]
    after, steps = drive(armed.frontier, slots, armed_at, always_final)
    assert steps == [Decide(first_open_bar, ArmEvent.POST)] and after.cursor_ms == first_open_bar
    # re-armed at the open from Friday's last bar: the closed Sunday buckets are not bars, so no span
    stored = Frontier(friday_last, h(friday_last))
    rearmed = arming_frontier(fill_bucket_ms=None, armed_at_ms=armed_at, spec=M15, skew=COLD_SKEW,
                              bar_ends=fx_open_ends(friday_last, armed_at), previous=stored)
    assert rearmed.span is None and rearmed.frontier == stored


def test_arming_bar_ends_must_increase() -> None:
    with pytest.raises(ValueError):
        arming_frontier(fill_bucket_ms=C0, armed_at_ms=C0 + 4 * Q, spec=M15, skew=COLD_SKEW,
                        bar_ends=[C0 + 2 * Q, C0 + Q])


def test_newest_expired_bar_end_is_exactly_at_the_cap() -> None:
    cap = decision_cap_ms(C0, M15, COLD_SKEW)
    assert newest_expired_bar_end(cap, M15, COLD_SKEW) == C0
    assert newest_expired_bar_end(cap - 1, M15, COLD_SKEW) == C0 - Q


# ---------------------------------------------------------------------------------------------------- passes
def test_a_nine_hour_shutdown_gives_one_line_per_position_and_no_page() -> None:
    restart = C0 + 9 * 3_600_000 + 7 * 60_000
    for _ in range(3):   # three positions, one span each
        slots = [BarSlot(e, ready=True) for e in range(C0 + Q, restart + 1, Q)]
        after, steps = drive(Frontier(C0, h(C0)), slots, restart, always_final)
        spans = [s for s in steps if isinstance(s, Span)]
        assert len(spans) == 1 and spans[0].reason == DOWNTIME and not spans[0].pages
        assert spans[0].bars == len(slots) - 1
        assert [s for s in steps if isinstance(s, Decide)] == [Decide(slots[-1].bar_end_ms, ArmEvent.POST)]
        assert after.cursor_ms == slots[-1].bar_end_ms


def test_a_bar_never_final_gives_a_span_at_its_cap_and_the_next_bar_is_decided_on_time() -> None:
    n, n1 = C0 + Q, C0 + 2 * Q
    slots = [BarSlot(n, reason=BROKER_BAR_NOT_FINAL), BarSlot(n1, ready=True)]
    before = decision_cap_ms(n, M15, COLD_SKEW) - 1
    _, steps = drive(Frontier(C0), slots, before, always_final)
    assert steps == [Wait(n, BROKER_BAR_NOT_FINAL)]          # bar n + 1 is never decided before bar n
    cap = decision_cap_ms(n, M15, COLD_SKEW)
    assert cap == n1 + SETTLE                                 # bar n's cap is bar n + 1's first read
    after, steps = drive(Frontier(C0), slots, cap, always_final)
    assert isinstance(steps[0], Span) and steps[0].reason == BROKER_BAR_NOT_FINAL and steps[0].pages
    assert steps[0].bars == 1 and steps[0].reopen is None
    assert steps[1] == Decide(n1, ArmEvent.POST) and after == Frontier(n1, h(n1))


def test_spans_split_where_the_reason_changes() -> None:
    slots = [BarSlot(C0 + Q), BarSlot(C0 + 2 * Q), BarSlot(C0 + 3 * Q, reason=BROKER_BAR_NOT_FINAL),
             BarSlot(C0 + 4 * Q, ready=True)]
    _, steps = drive(Frontier(C0), slots, decision_cap_ms(C0 + 3 * Q, M15, COLD_SKEW), always_final)
    spans = [s for s in steps if isinstance(s, Span)]
    assert [(s.from_bar_ms, s.to_bar_ms, s.reason) for s in spans] == [
        (C0 + Q, C0 + 2 * Q, DOWNTIME), (C0 + 3 * Q, C0 + 3 * Q, BROKER_BAR_NOT_FINAL)]


def test_bars_decide_in_order_and_a_late_bar_is_late() -> None:
    slots = [BarSlot(C0 + Q, ready=True, post_sent=True), BarSlot(C0 + 2 * Q, ready=True)]
    decided: list[Decide] = []
    after, _ = drive(Frontier(C0), slots, C0 + 2 * Q + 1_000,
                     lambda d: (decided.append(d), always_final(d))[1])
    assert decided == [Decide(C0 + Q, ArmEvent.LATE), Decide(C0 + 2 * Q, ArmEvent.POST)]
    assert after.cursor_ms == C0 + 2 * Q


def test_a_post_time_data_unavailable_does_not_advance_the_cursor() -> None:
    slots = [BarSlot(C0 + Q, ready=True), BarSlot(C0 + 2 * Q, ready=True)]
    after, steps = drive(Frontier(C0), slots, C0 + 2 * Q + 1_000, lambda d: (False, None))
    assert steps == [Decide(C0 + Q, ArmEvent.POST)] and after == Frontier(C0)


def test_a_not_ready_bar_waits_with_a_code() -> None:
    assert next_step(Frontier(C0), [BarSlot(C0 + Q)], C0 + Q + SETTLE, spec=M15,
                     skew=COLD_SKEW) == Wait(C0 + Q, AWAITING_INPUTS)


@pytest.mark.parametrize("hold", [PAUSED, FAILED])
def test_a_failed_or_paused_position_keeps_its_cursor(hold: str) -> None:
    slots = [BarSlot(C0 + Q), BarSlot(C0 + 2 * Q, ready=True)]
    after, steps = drive(Frontier(C0), slots, C0 + 10 * Q, always_final, hold=hold)
    assert steps == [Hold(hold)] and after == Frontier(C0)
    with pytest.raises(ValueError):
        next_step(Frontier(C0), slots, C0, spec=M15, skew=COLD_SKEW, hold=DOWNTIME)


def test_a_span_with_bar_zero_records_first_bar_stale() -> None:
    slots = [BarSlot(C0 + Q), BarSlot(C0 + 2 * Q)]
    step = next_step(Frontier(C0), slots, C0 + 10 * Q, spec=M15, skew=COLD_SKEW, bar0_end_ms=C0 + Q)
    assert isinstance(step, Span) and step.first_bar_stale and step.first_bar_detail == FIRST_BAR_STALE
    step = next_step(Frontier(C0), slots, C0 + 10 * Q, spec=M15, skew=COLD_SKEW, bar0_end_ms=C0 - Q)
    assert isinstance(step, Span) and not step.first_bar_stale and step.first_bar_detail is None


def test_slots_must_follow_the_cursor_in_order() -> None:
    with pytest.raises(ValueError):
        next_step(Frontier(C0), [BarSlot(C0)], C0 + Q, spec=M15, skew=COLD_SKEW)
    with pytest.raises(ValueError):
        next_step(Frontier(C0), [BarSlot(C0 + 2 * Q), BarSlot(C0 + Q)], C0 + Q, spec=M15, skew=COLD_SKEW)


def test_the_cap_follows_the_skew() -> None:
    skew = SkewEstimate(p50_ms=2_000, settle_ms=1_000)
    slots = [BarSlot(C0 + Q, reason=BROKER_BAR_NOT_FINAL)]
    at_cold_cap = decision_cap_ms(C0 + Q, M15, COLD_SKEW)
    assert isinstance(next_step(Frontier(C0), slots, at_cold_cap, spec=M15, skew=COLD_SKEW), Span)
    assert next_step(Frontier(C0), slots, decision_cap_ms(C0 + Q, M15, skew) - 1, spec=M15,
                     skew=skew) == Wait(C0 + Q, BROKER_BAR_NOT_FINAL)


# ---------------------------------------------------------------------------------------------------- market close
def test_fridays_last_bar_final_after_its_cap_is_decided_as_a_reopen_item_at_the_sunday_open() -> None:
    t = ny_ms(2026, 11, 13, 17, 0)                        # Friday 16:45-17:00 New York bar
    sunday_open = ny_ms(2026, 11, 15, 17, 0)
    first_open_bar = sunday_open + Q
    cursor = Frontier(t - Q, h(t - Q))
    slot = BarSlot(t, reason=BROKER_BAR_NOT_FINAL, next_open_ms=sunday_open)
    cap = decision_cap_ms(t, M15, COLD_SKEW)
    assert cap < t + 20 * 60_000                          # final only at T + 20 min: after its cap
    after, steps = drive(cursor, [slot], cap, always_final)
    assert len(steps) == 1 and isinstance(steps[0], Span)
    span = steps[0]
    assert span.reason == BROKER_BAR_NOT_FINAL and span.to_bar_ms == t and after == Frontier(t, None)
    # the REOPEN schedule: from the open + settle until the first post-open bar's decision_cap
    assert span.reopen == ReopenCheck(t, sunday_open + SETTLE, decision_cap_ms(first_open_bar, M15, COLD_SKEW),
                                      first_open_bar)
    check = span.reopen
    # final at T + 20 min, but the market is closed: nothing before the open
    _, steps = drive(after, [], t + 20 * 60_000, always_final, rechecks=[(check, True)])
    assert steps == []
    _, steps = drive(after, [], sunday_open + SETTLE - 1, always_final, rechecks=[(check, True)])
    assert steps == []
    # re-read at the open: final, decided as REOPEN on the original bar end; the cursor does not move
    decided: list[Decide] = []
    slots = [BarSlot(first_open_bar, ready=True)]
    final_after, steps = drive(after, slots, first_open_bar + SETTLE,
                               lambda d: (decided.append(d), always_final(d))[1], rechecks=[(check, True)])
    assert decided == [Decide(t, ArmEvent.REOPEN), Decide(first_open_bar, ArmEvent.POST)]
    assert final_after.cursor_ms == first_open_bar


def test_a_bar_final_between_the_first_open_bars_first_read_and_its_post_is_still_a_reopen() -> None:
    t = ny_ms(2026, 11, 13, 17, 0)
    sunday_open = ny_ms(2026, 11, 15, 17, 0)
    first_open_bar = sunday_open + Q
    check = reopen_after_closed_post(t, sunday_open, M15, COLD_SKEW)
    assert check.deadline_ms == decision_cap_ms(first_open_bar, M15, COLD_SKEW)
    # at the open, not final yet and no post-open bar to decide: the re-check waits
    _, steps = drive(Frontier(t), [], sunday_open + 60_000, always_final, rechecks=[(check, False)])
    assert steps == [Wait(t, REOPEN_PENDING)]
    # the first post-open bar's first read: the broker's bar is not in yet, still not final
    waiting = [BarSlot(first_open_bar, reason=BROKER_BAR_NOT_FINAL)]
    _, steps = drive(Frontier(t), waiting, first_open_bar + SETTLE, always_final, rechecks=[(check, False)])
    assert steps == [Wait(first_open_bar, BROKER_BAR_NOT_FINAL)]
    # final two minutes later, before the first post-open POST: decided as REOPEN (the old window dropped it)
    after, steps = drive(Frontier(t), waiting, first_open_bar + 120_000, always_final, rechecks=[(check, True)])
    assert steps == [Decide(t, ArmEvent.REOPEN), Wait(first_open_bar, BROKER_BAR_NOT_FINAL)]
    assert after == Frontier(t)
    after, steps = drive(after, [BarSlot(first_open_bar, ready=True)], first_open_bar + 180_000, always_final)
    assert steps == [Decide(first_open_bar, ArmEvent.POST)] and after.cursor_ms == first_open_bar


def test_a_recheck_never_blocks_the_first_post_open_post_and_ends_with_it() -> None:
    t = ny_ms(2026, 11, 13, 17, 0)
    sunday_open = ny_ms(2026, 11, 15, 17, 0)
    first_open_bar = sunday_open + Q
    check = reopen_after_closed_post(t, sunday_open, M15, COLD_SKEW)
    slots = [BarSlot(first_open_bar, ready=True)]
    after, steps = drive(Frontier(t), slots, first_open_bar + SETTLE, always_final, rechecks=[(check, False)])
    assert steps == [Decide(first_open_bar, ArmEvent.POST), DropRecheck(t)]   # the POST ends the REOPEN schedule
    assert after.cursor_ms == first_open_bar
    # a retryable REOPEN (price through the level, a wide spread at the open) never holds the POST back either
    tried: list[Decide] = []

    def retryable(d: Decide) -> tuple[bool, str | None]:
        tried.append(d)
        return (False, None) if d.event is ArmEvent.REOPEN else always_final(d)

    after, steps = drive(Frontier(t), slots, first_open_bar + SETTLE, retryable, rechecks=[(check, True)])
    assert tried == [Decide(t, ArmEvent.REOPEN), Decide(first_open_bar, ArmEvent.POST)]
    assert steps[-1] == DropRecheck(t) and after.cursor_ms == first_open_bar


def test_a_recheck_still_open_at_the_first_open_bars_cap_is_dropped() -> None:
    t = ny_ms(2026, 11, 13, 17, 0)
    sunday_open = ny_ms(2026, 11, 15, 17, 0)
    first_open_bar = sunday_open + Q
    check = reopen_after_closed_post(t, sunday_open, M15, COLD_SKEW)
    slots = [BarSlot(first_open_bar, reason=BROKER_BAR_NOT_FINAL)]
    after, steps = drive(Frontier(t), slots, check.deadline_ms, always_final, rechecks=[(check, False)])
    assert steps[0] == DropRecheck(t)
    assert isinstance(steps[1], Span) and steps[1].to_bar_ms == first_open_bar and after.cursor_ms == first_open_bar


def test_a_trigger_on_the_1645_1700_new_york_bar_gives_market_closed_then_reopen_at_1705() -> None:
    t = ny_ms(2026, 11, 11, 17, 0)                        # Wednesday 16:45-17:00 New York, before the daily break
    reopen = ny_ms(2026, 11, 11, 17, 5)
    first_open_bar = ny_ms(2026, 11, 11, 17, 15)          # the 17:00-17:15 bucket is open from 17:05
    assert fx_open_ends(t - Q, first_open_bar) == [t, first_open_bar]
    slot = BarSlot(t, ready=True, next_open_ms=reopen)
    post_at = t + SETTLE                                  # 17:00:05: inside the 17:00-17:05 break
    assert t <= post_at < reopen
    outcomes: list[tuple[int, ArmEvent, str]] = []

    def decide(d: Decide) -> tuple[bool, str | None]:
        # the pipeline finds the trigger met; a POST in the break is final as MARKET_CLOSED (a HOLD)
        outcomes.append((d.bar_end_ms, d.event, "MARKET_CLOSED" if d.event is ArmEvent.POST else "ADJUST"))
        return True, h(d.bar_end_ms, "1.3")

    after, steps = drive(Frontier(t - Q), [slot], post_at, decide)
    assert steps == [Decide(t, ArmEvent.POST)] and outcomes == [(t, ArmEvent.POST, "MARKET_CLOSED")]
    assert after == Frontier(t, h(t, "1.3"))               # the cursor advances
    check = reopen_after_closed_post(t, reopen, M15, COLD_SKEW)   # S27 stores the pending re-check
    assert check == ReopenCheck(t, reopen + SETTLE, decision_cap_ms(first_open_bar, M15, COLD_SKEW), first_open_bar)
    # nothing is decided while closed
    for now in (post_at + 1_000, reopen, reopen + SETTLE - 1):
        _, steps = drive(after, [], now, decide, rechecks=[(check, True)])
        assert steps == []
    # REOPEN at 17:05 + settle, before the first post-open POST (17:15 + settle)
    reopened, steps = drive(after, [], reopen + SETTLE, decide, rechecks=[(check, True)])
    assert steps == [Decide(t, ArmEvent.REOPEN)] and reopened == after
    assert reopen + SETTLE < first_open_bar + SETTLE
    final_after, steps = drive(reopened, [BarSlot(first_open_bar, ready=True)], first_open_bar + SETTLE, decide)
    assert steps == [Decide(first_open_bar, ArmEvent.POST)] and final_after.cursor_ms == first_open_bar
    assert [o[:2] for o in outcomes] == [(t, ArmEvent.POST), (t, ArmEvent.REOPEN), (first_open_bar, ArmEvent.POST)]
    with pytest.raises(ValueError):
        reopen_after_closed_post(t, t, M15, COLD_SKEW)


def test_a_pre_close_bar_spanned_for_downtime_gets_no_recheck() -> None:
    t = ny_ms(2026, 11, 13, 17, 0)
    slot = BarSlot(t, next_open_ms=ny_ms(2026, 11, 15, 17, 0))
    step = next_step(Frontier(t - Q), [slot], t + 3 * Q, spec=M15, skew=COLD_SKEW)
    assert isinstance(step, Span) and step.reopen is None


def test_a_reopen_item_never_moves_the_cursor() -> None:
    f = Frontier(C0 + 4 * Q, h(C0 + 4 * Q))
    assert advance(f, Decide(C0, ArmEvent.REOPEN), decided_hash=h(C0)) == f
    assert advance(f, Wait(C0, "x")) == f and advance(f, Hold(PAUSED)) == f and advance(f, DropRecheck(C0)) == f


# ---------------------------------------------------------------------------------------------------- pull cursor
def ev(bar_end: int, seq: int, close: str = "1.2", ring: Keyring = RING1) -> BarEndEvent:
    return BarEndEvent(SERIES, bar_end, h(bar_end, close, ring), seq, 0)


def test_events_are_applied_in_bar_order_with_the_current_revision() -> None:
    events = [ev(C0 + 2 * Q, 5), ev(C0 + Q, 3), ev(C0 + Q, 7, "1.3")]
    ordered = in_bar_order(events)
    assert [(e.bar_end_ms, e.write_seq) for e in ordered] == [(C0 + Q, 7), (C0 + 2 * Q, 5)]


def test_classify_pulled_events() -> None:
    f = Frontier(C0, h(C0))
    assert classify_event(ev(C0 + Q, 1), f) is EventAction.WAKE
    assert classify_event(ev(C0, 1), f) is EventAction.NOOP                     # same id, same hash
    assert classify_event(ev(C0, 1, "1.3"), f) is EventAction.CORRECTION_CHECK  # same id, different hash
    assert classify_event(ev(C0, 1, ring=RING2), f) is EventAction.INCOMPARABLE  # another key id: never a change
    assert classify_event(ev(C0 - Q, 1), f) is EventAction.CORRECTION_CHECK     # older id: compare the inputs
    assert classify_event(ev(C0, 1), Frontier(C0, None)) is EventAction.NOOP     # after a span


def test_the_write_seq_cursor_restarts_from_the_durable_acknowledgement_after_a_generation_change() -> None:
    cur = SeriesCursor()
    first = cur.for_pull(C0)
    assert first == EventCursor(C0, 0)
    out = cur.on_result(first, EventsRead((ev(C0, 10), ev(C0 + Q, 11)), GEN1))
    assert [e.bar_end_ms for e in out.events] == [C0, C0 + Q]
    held = cur.cursor
    assert held is not None and (held.bar_end_ms, held.write_seq, held.generation) == (C0 + Q, 11, GEN1)
    # a new md generation: the port answers CURSOR_EXPIRED; the next pull is the restart form from PostgreSQL
    out = cur.on_result(held, CursorExpired(GEN2))
    assert out.restarted and out.read_by_bucket_from_ms is None and cur.cursor is None
    restart = cur.for_pull(C0 + Q)
    assert restart == EventCursor(C0 + Q, 0)
    out = cur.on_result(restart, EventsRead((ev(C0 + Q, 2), ev(C0 + 2 * Q, 3)), GEN2))
    assert [e.bar_end_ms for e in out.events] == [C0 + Q, C0 + 2 * Q]
    held = cur.cursor
    assert held is not None and held.generation == GEN2 and held.write_seq == 3


def test_an_expired_restart_form_reads_by_bucket() -> None:
    cur = SeriesCursor()
    restart = cur.for_pull(C0)
    out = cur.on_result(restart, CursorExpired(GEN1))
    assert out.read_by_bucket_from_ms == C0 and out.events == ()


def test_unavailable_keeps_the_cursor_and_a_foreign_generation_read_is_dropped() -> None:
    cur = SeriesCursor()
    first = cur.for_pull(C0)
    cur.on_result(first, EventsRead((ev(C0, 4),), GEN1))
    held = cur.cursor
    assert held is not None
    assert cur.on_result(held, DataUnavailable("not_live")) == cur.on_result(held, DataUnavailable("not_live"))
    assert cur.cursor == held
    out = cur.on_result(held, EventsRead((ev(C0 + Q, 1),), GEN2))
    assert out.restarted and out.events == () and cur.cursor is None
    cur.on_result(cur.for_pull(C0), EventsRead((), GEN2))
    assert cur.cursor is not None and cur.cursor.generation == GEN2
    cur.reset()
    assert cur.cursor is None
