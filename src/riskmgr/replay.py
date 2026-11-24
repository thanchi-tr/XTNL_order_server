"""Replay: the per-position frontier, `UNRESOLVED_RANGE` spans, in-order decisions, the Friday-close REOPEN rule and
the in-memory pull cursor (design §B6.6, §B5.2, §A13; plan S22, R11-M16, R11-m).

**Frontier.** `arm_positions.last_decided_bar` is the ARM's durable cursor and acknowledgement (with
`last_decided_hash`, NULL after a span). At arming and re-arming (`arming_frontier`) the cursor is the later of the
start of the fill bucket (the bar end before the fill bucket's) and the newest **open** bar end whose `decision_cap`
has passed (arming time − replay horizon, the horizon being `decision_cap`, §B5.2). The BarClock hands in the open bar
ends (`bar_ends`: the ends of open-calendar buckets, as it lists them for a pass), so a span across a closed period
counts only real bars and never ends on a bucket of the closed period; the open bars skipped that way become one
`armed_late` span, so a position armed long after its fill decides at most the current bar.

**One pass** (`next_step`, applied with `advance` until it returns a POST / LATE `Decide` that did not decide the
bar, a `Wait`, a `Hold` or None; a REOPEN attempt that did not decide is marked not due for the rest of the pass):
the BarClock lists, as `BarSlot`s, every bar end of an open bucket after the cursor up to now (a bucket of a closed
period is not a bar and is not listed). Bars whose cap has passed undecided become **one**
terminal `UNRESOLVED_RANGE(from, to, reason)` span per contiguous run with one reason (`bar_end_ms` = `to`, no
`commit_state`, hash NULL), which advances the cursor; a bar still undecided at its cap is never decided
(except a guard's wait, below). The reason
is the one the slot carries: the reason the bar was last pending with while the ARM ran (FAULT: pages once per
position episode and counts toward D7), or `downtime` / `paused` / `failed` (INFO) for a bar the ARM did not look at
while running (the BarClock's default is `downtime`). A span containing bar 0 records `FIRST_BAR_UNKNOWN(stale)`.
**A guard's wait is not spanned** (§B6.1, §B6.5; live review round 2): a slot whose last pending reason is a guard's
`<kind>_pending` (`guard_timeout`: its broker bar was final and the pipeline reached the guards) is decided once at
its cap instead (`POST`, or `LATE` once its POST notice has gone): the pipeline then runs at or after the guard's
`until`, so `final_verdict` gives `on_timeout` under `unknown_policy` (bar 0: `FIRST_BAR_UNKNOWN(timeout)`, Q3) and
the cursor advances. If that one decision is not final either, the BarClock clears the flag and the bar is spanned on
the next pass, so the frontier never sticks.
Then the first bar whose cap has not passed is decided when its inputs exist (`POST`, or `LATE` once its POST notice
has gone), else the pass waits: bar n + 1 is never decided before bar n is decided or expired, and since bar n's cap is
bar n + 1's first read, that never delays it. While the position's scope is FAILED or the ARM is paused the cursor
stays put (`Hold`).

**Market close** (§B5.2, §B6.3 REOPEN, round-11 confirmation C3): a pre-close bar (its end is a daily break or the
Friday close) still not final at its cap is spanned like any other, but the span carries a `ReopenCheck`; a final POST
of a pre-close bar that came out `MARKET_CLOSED` with `trigger_met` (its POST fell in the closed period) gets one from
`reopen_after_closed_post` (S27 calls it). From the first open instant + settle the bar is re-read and, when final and
a REOPEN attempt is due (the caller's 5 s retry cadence), decided as a `REOPEN` item (on the original bar end; the
cursor does not move). The REOPEN schedule ends at the first post-open bar's POST decision, at the latest its
`decision_cap` (`deadline_ms`): a re-check is dropped (`DropRecheck`, the bar stays UNRESOLVED or its HOLD stands)
once the cursor reaches the first post-open bar or at the deadline. A re-check never blocks that POST: when no REOPEN
attempt is due, the pass goes on to the first post-open bar, and it waits (`REOPEN_PENDING`) only when nothing else is
decidable.

**Pull cursor** (R11-M16): the `write_seq` cursor of `events_since` is in memory per series and md generation
(`SeriesCursor`). After a generation change or `CURSOR_EXPIRED` it restarts from the durable acknowledgement
(`EventCursor(oldest last_decided_bar of the positions on the series, 0)`); when the restart form itself expires the
consumer reads by bucket. A pulled event is only a wake-up (`classify_event`): after the cursor it wakes a decision; at
the cursor with the same hash, or a hash under another key id, it is a no-op; with a different hash, or before the
cursor, it goes to the correction check (`src.riskmgr.corrections`).

Pure values and in-memory state only: ids, bar ends, codes and keyed hashes, never a price; nothing here logs.
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final

from src.marketdata.hashing import HashComparison, compare_hashes
from src.marketdata.ports import EventsRead
from src.marketdata.timing import SkewEstimate, decision_cap_ms
from src.marketdata.types import BarEndEvent, BarSpec, CursorExpired, DataUnavailable, EventCursor
from src.riskmgr.outcomes import (
    ARMED_LATE,
    BROKER_BAR_NOT_FINAL,
    DOWNTIME,
    FAILED,
    PAUSED,
    UNRESOLVED_INFO_REASONS,
    Category,
    OutcomeCode,
)
from src.riskmgr.types import ArmEvent

FIRST_BAR_STALE: Final = "stale"
"""The `FIRST_BAR_UNKNOWN` detail recorded on a span that contains bar 0 (§B6.6)."""
HOLD_REASONS: Final = frozenset({PAUSED, FAILED})
AWAITING_INPUTS: Final = "awaiting_inputs"
"""`Wait` reason for a bar not decidable yet whose slot names no pending reason."""
REOPEN_PENDING: Final = "reopen_pending"
"""`Wait` reason while a pre-close re-check is open and no REOPEN attempt or newer bar is decidable."""


# ---------------------------------------------------------------------------------------------------- the frontier
@dataclass(frozen=True)
class Frontier:
    """`(last_decided_bar, last_decided_hash)` of one position (hash NULL after a span or before the first row)."""

    cursor_ms: int
    last_hash: str | None = None


def newest_expired_bar_end(now_ms: int, spec: BarSpec, skew: SkewEstimate) -> int:
    """The newest bar end on the grid whose `decision_cap` is at or before `now_ms` (arming time − replay horizon)."""
    latest = now_ms - spec.timeframe_ms - skew.p50_ms - skew.settle_ms
    return spec.bucket_start_ms(latest)


def _sorted_ends(bar_ends: Sequence[int]) -> None:
    for a, b in zip(bar_ends, bar_ends[1:], strict=False):
        if b <= a:
            raise ValueError("bar ends are increasing")


@dataclass(frozen=True)
class ReopenCheck:
    """A pre-close bar re-read at the open: `open_ms` (the first open instant + settle) and `deadline_ms` (the
    `decision_cap` of `first_open_bar_ms`, the first post-open bar) on the local clock."""

    bar_end_ms: int
    open_ms: int
    deadline_ms: int
    first_open_bar_ms: int


def first_open_bar_end_ms(next_open_ms: int, spec: BarSpec, skew: SkewEstimate) -> int:
    """The end of the first bar after a local open instant (the bucket holding the open, on the broker grid)."""
    return spec.bucket_start_ms(next_open_ms - skew.p50_ms) + spec.timeframe_ms


def reopen_check(bar_end_ms: int, next_open_ms: int, spec: BarSpec, skew: SkewEstimate) -> ReopenCheck:
    """The re-check of pre-close bar `bar_end_ms`: from the open + settle until the first post-open bar's cap."""
    if next_open_ms <= bar_end_ms:
        raise ValueError("the next open follows the bar end")
    first = first_open_bar_end_ms(next_open_ms, spec, skew)
    return ReopenCheck(bar_end_ms, next_open_ms + skew.settle_ms, decision_cap_ms(first, spec, skew), first)


def reopen_after_closed_post(bar_end_ms: int, next_open_ms: int, spec: BarSpec, skew: SkewEstimate) -> ReopenCheck:
    """The re-check S27 stores when a final POST of a pre-close bar came out `MARKET_CLOSED` with `trigger_met`
    (§B6.3 REOPEN): retried from the open + settle until the first post-open bar's POST decision."""
    return reopen_check(bar_end_ms, next_open_ms, spec, skew)


@dataclass(frozen=True)
class Span:
    """One terminal `UNRESOLVED_RANGE(from_bar, to_bar, reason)` row (`bar_end_ms` = `to_bar_ms`)."""

    from_bar_ms: int
    to_bar_ms: int
    reason: str
    bars: int
    first_bar_stale: bool = False
    reopen: ReopenCheck | None = None

    @property
    def code(self) -> OutcomeCode:
        return OutcomeCode.unresolved(self.reason)

    @property
    def first_bar_detail(self) -> str | None:
        """`stale` when the span holds bar 0: S27 records `FIRST_BAR_UNKNOWN(stale)`; else None."""
        return FIRST_BAR_STALE if self.first_bar_stale else None

    @property
    def bar_end_ms(self) -> int:
        return self.to_bar_ms

    @property
    def pages(self) -> bool:
        """FAULT reasons page once per position episode and count toward D7; the INFO four never do."""
        return self.code.category is Category.FAULT


@dataclass(frozen=True)
class ArmingFrontier:
    frontier: Frontier
    span: Span | None


def arming_frontier(*, fill_bucket_ms: int | None, armed_at_ms: int, spec: BarSpec, skew: SkewEstimate,
                    bar_ends: Sequence[int], previous: Frontier | None = None, reason: str = ARMED_LATE,
                    bar0_end_ms: int | None = None) -> ArmingFrontier:
    """The cursor at arming (`previous` None) or re-arming: the later of the fill bucket's start (or the stored
    cursor) and the newest open bar end at or before the newest expired grid bar end; the open bars between become
    one span (`armed_late` by default). `bar_ends`: the BarClock's open bar ends (increasing), covering at least the
    range from the start to arming time; a bucket of a closed period is not a bar, so it is never counted, never the
    span's last bar and never the cursor. No open bar in the range means no span (the cursor stays at the start)."""
    _sorted_ends(bar_ends)
    start = previous.cursor_ms if previous is not None else fill_bucket_ms
    expired = newest_expired_bar_end(armed_at_ms, spec, skew)
    if start is None:
        known = [e for e in bar_ends if e <= expired]
        return ArmingFrontier(Frontier(known[-1] if known else expired), None)
    inside = [e for e in bar_ends if start < e <= expired]
    if not inside:
        return ArmingFrontier(previous if previous is not None else Frontier(start), None)
    first, last = inside[0], inside[-1]
    stale = bar0_end_ms is not None and first <= bar0_end_ms <= last
    span = Span(first, last, reason, len(inside), stale)
    return ArmingFrontier(Frontier(last), span)


# ---------------------------------------------------------------------------------------------------- one pass
@dataclass(frozen=True)
class BarSlot:
    """One bar end of an open bucket after the cursor, as the BarClock sees it now.

    `ready`: the bar's inputs exist (the broker's final bar is stored and its indicators updated), so the whole
    pipeline can run; `reason`: what an expiry of this bar records (the reason it was last pending with while the ARM
    ran, `broker_bar_not_final` while it awaits the broker's final bar, or `downtime` / `paused` / `failed`);
    `post_sent`: its POST notice has gone (a decision now is `LATE`);
    `next_open_ms`: set on a pre-close bar, the local time of the next open (a span of it with
    `broker_bar_not_final` carries a `ReopenCheck`);
    `guard_timeout`: `reason` is a guard's `<kind>_pending` (the broker bar was final) and the bar has not yet been
    tried at its cap: once expired it is decided (`Decide`), never joined to a span (§B6.1, §B6.5)."""

    bar_end_ms: int
    ready: bool = False
    reason: str = DOWNTIME
    post_sent: bool = False
    next_open_ms: int | None = None
    guard_timeout: bool = False


@dataclass(frozen=True)
class Decide:
    bar_end_ms: int
    event: ArmEvent


@dataclass(frozen=True)
class Wait:
    """The first undecided bar is not decidable yet; the bars after it stay pending."""

    bar_end_ms: int
    reason: str


@dataclass(frozen=True)
class Hold:
    """The position's scope is FAILED or the ARM is paused: the cursor stays put."""

    reason: str


@dataclass(frozen=True)
class DropRecheck:
    """A pre-close re-check ended undecided: its deadline passed or the first post-open bar was decided (the cursor
    reached it). The bar stays UNRESOLVED (or its MARKET_CLOSED HOLD stands)."""

    bar_end_ms: int


Step = Span | Decide | Wait | Hold | DropRecheck


def _reopen(slot: BarSlot, spec: BarSpec, skew: SkewEstimate) -> ReopenCheck | None:
    if slot.next_open_ms is None or slot.reason != BROKER_BAR_NOT_FINAL:
        return None
    return reopen_check(slot.bar_end_ms, slot.next_open_ms, spec, skew)


def _check_slots(frontier: Frontier, slots: Sequence[BarSlot]) -> None:
    prev = frontier.cursor_ms
    for s in slots:
        if s.bar_end_ms <= prev:
            raise ValueError("slots are bar ends after the cursor, in increasing order")
        prev = s.bar_end_ms


def next_step(frontier: Frontier, slots: Sequence[BarSlot], now_ms: int, *, spec: BarSpec, skew: SkewEstimate,
              hold: str | None = None, bar0_end_ms: int | None = None,
              rechecks: Sequence[tuple[ReopenCheck, bool]] = ()) -> Step | None:
    """The next thing to do for one position in this BarClock pass, or None when nothing is due.

    `rechecks`: the pending pre-close re-checks (oldest first), each with whether a REOPEN attempt is due now: the bar
    is final and the caller's retry cadence allows an attempt (after a retryable REOPEN result the caller passes
    False for the rest of the pass, so the first post-open POST still goes ahead)."""
    if hold is not None:
        if hold not in HOLD_REASONS:
            raise ValueError("a hold is paused or failed")
        return Hold(hold)
    _check_slots(frontier, slots)
    open_check: ReopenCheck | None = None
    for check, due in rechecks:
        if frontier.cursor_ms >= check.first_open_bar_ms:
            return DropRecheck(check.bar_end_ms)     # the first post-open bar's POST was decided: the schedule ends
        if now_ms < check.open_ms:
            continue
        if due and now_ms <= check.deadline_ms:
            return Decide(check.bar_end_ms, ArmEvent.REOPEN)
        if now_ms >= check.deadline_ms:
            return DropRecheck(check.bar_end_ms)
        if open_check is None:
            open_check = check
    step = _slot_step(slots, now_ms, spec=spec, skew=skew, bar0_end_ms=bar0_end_ms)
    if step is None and open_check is not None:
        return Wait(open_check.bar_end_ms, REOPEN_PENDING)
    return step


def _slot_step(slots: Sequence[BarSlot], now_ms: int, *, spec: BarSpec, skew: SkewEstimate,
               bar0_end_ms: int | None) -> Step | None:
    run: list[BarSlot] = []
    for slot in slots:
        expired = decision_cap_ms(slot.bar_end_ms, spec, skew) <= now_ms
        if expired and slot.guard_timeout:
            if run:
                break          # the run before it is spanned first; this bar is decided on its own
            return Decide(slot.bar_end_ms, ArmEvent.LATE if slot.post_sent else ArmEvent.POST)
        if expired and (not run or run[-1].reason == slot.reason):
            run.append(slot)
            if _reopen(slot, spec, skew) is not None:
                break          # a re-check is tied to its own bar: the span ends there
            continue
        if run:
            break
        if slot.ready:
            return Decide(slot.bar_end_ms, ArmEvent.LATE if slot.post_sent else ArmEvent.POST)
        return Wait(slot.bar_end_ms, AWAITING_INPUTS if slot.reason in UNRESOLVED_INFO_REASONS else slot.reason)
    if not run:
        return None
    first, last = run[0].bar_end_ms, run[-1].bar_end_ms
    stale = bar0_end_ms is not None and first <= bar0_end_ms <= last
    return Span(first, last, run[0].reason, len(run), stale, _reopen(run[-1], spec, skew))


def advance(frontier: Frontier, step: Step, *, decided_hash: str | None = None) -> Frontier:
    """The frontier after `step` committed: a span moves the cursor to its last bar (hash NULL); a final POST or
    LATE moves it to its bar with the trigger bar's keyed hash; a REOPEN item, a wait, a hold or a dropped re-check
    leave it where it is."""
    if isinstance(step, Span):
        return Frontier(step.to_bar_ms, None)
    if isinstance(step, Decide) and step.event in (ArmEvent.POST, ArmEvent.LATE):
        return Frontier(step.bar_end_ms, decided_hash)
    return frontier


# ---------------------------------------------------------------------------------------------------- pull cursor
class EventAction(str, Enum):
    WAKE = "WAKE"                         # a bar after the cursor: decide (re-reading every input)
    NOOP = "NOOP"                         # at the cursor, same hash
    INCOMPARABLE = "INCOMPARABLE"         # at the cursor, hash under another key id: never a change
    CORRECTION_CHECK = "CORRECTION_CHECK"  # at the cursor with another hash, or an older id: compare the inputs


def classify_event(event: BarEndEvent, frontier: Frontier) -> EventAction:
    if event.bar_end_ms > frontier.cursor_ms:
        return EventAction.WAKE
    if event.bar_end_ms < frontier.cursor_ms:
        return EventAction.CORRECTION_CHECK
    if frontier.last_hash is None:
        return EventAction.NOOP           # a span is terminal: nothing was acknowledged by hash
    result = compare_hashes(frontier.last_hash, event.content_hash)
    if result is HashComparison.SAME:
        return EventAction.NOOP
    if result is HashComparison.INCOMPARABLE:
        return EventAction.INCOMPARABLE
    return EventAction.CORRECTION_CHECK


def in_bar_order(events: Iterable[BarEndEvent]) -> list[BarEndEvent]:
    """Pulled events oldest bar first; of one id only the newest `write_seq` (the current revision) is kept."""
    newest: dict[tuple[str, int], BarEndEvent] = {}
    for e in events:
        held = newest.get(e.event_id)
        if held is None or e.write_seq > held.write_seq:
            newest[e.event_id] = e
    return sorted(newest.values(), key=lambda e: (e.bar_end_ms, e.write_seq))


@dataclass(frozen=True)
class PullOutcome:
    """The events to apply (bar order), or `read_by_bucket_from_ms` set when the consumer must read by bucket."""

    events: tuple[BarEndEvent, ...] = ()
    read_by_bucket_from_ms: int | None = None
    restarted: bool = False


class SeriesCursor:
    """The in-memory `write_seq` cursor of one series (valid only within its md generation)."""

    def __init__(self) -> None:
        self._cursor: EventCursor | None = None

    @property
    def cursor(self) -> EventCursor | None:
        return self._cursor

    def for_pull(self, durable_floor_ms: int) -> EventCursor:
        """The cursor to pull with: the in-memory one, else the restart form from the durable acknowledgement (the
        oldest `last_decided_bar` of the positions on the series)."""
        return self._cursor if self._cursor is not None else EventCursor(durable_floor_ms, 0)

    def on_result(self, used: EventCursor, result: EventsRead | CursorExpired | DataUnavailable) -> PullOutcome:
        if isinstance(result, DataUnavailable):
            return PullOutcome()
        if isinstance(result, CursorExpired):
            self._cursor = None
            if used.write_seq == 0:
                return PullOutcome(read_by_bucket_from_ms=used.bar_end_ms, restarted=True)
            return PullOutcome(restarted=True)
        if self._cursor is not None and self._cursor.generation != result.generation:
            self._cursor = None   # a new generation: the write_seq part is void; restart from the durable cursor
            return PullOutcome(restarted=True)
        self._cursor = result.next_cursor(used)
        return PullOutcome(tuple(in_bar_order(result.events)))

    def reset(self) -> None:
        self._cursor = None
