"""Per-bar-spec timing table and the one cap clock (design §A4, §A9, §B5.2; plan S12, R11-m).

Every timing value of the market-data layer and the ARM's validity windows lives in one row per bar spec
(`TIMING_TABLE`): the `pre_offset` bounds, `post_grace`, the broker re-read schedule, the UNSUPPORTED lag limit, the
rollback-set size N and the TICK spec the BROKER bars are cross-checked against. v1 has only the 15m row; a strategy
whose `bar_spec` has no row is refused (`timing_problem`).

**The one clock** of every cap-based timer (the POST outer cap, the end-of-window notice part, the replay horizon and
the correction window) is `decision_cap(T) = local T + timeframe + p50 + settle`: `T` is the bar end in broker time,
`p50` / `settle` the adapter's ClockSkew (§A9; `p50` = received − broker, so it may be negative). Nothing else uses
another number. The same skew shifts the first broker read (`T + p50 + settle`), PRE (`T + p50 − pre_offset`) and the
re-read schedule onto the local clock.

`last_roll_ms` / `overlaps_roll` are a `zoneinfo` copy of the 17:00 New York roll (`src/pubsub/tradingday.py`), which
this package may not import (IN-14); a decision on a bucket overlapping the roll can carry `roll_affected` (Q14).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Final
from zoneinfo import ZoneInfo

from src.marketdata.types import M15, BarSpec

# ---------------------------------------------------------------------------------------------------- skew
SETTLE_MIN_MS: Final = 250
SETTLE_MAX_MS: Final = 5_000
SETTLE_MARGIN_MS: Final = 250
COLD_SETTLE_MS: Final = 5_000


@dataclass(frozen=True)
class SkewEstimate:
    """One adapter's ClockSkew summary (design §A9): `p50_ms` of (received − broker time), possibly negative, and
    `settle_ms`. Cold start: p50 0, settle 5 s."""

    p50_ms: int = 0
    settle_ms: int = COLD_SETTLE_MS


COLD_SKEW: Final = SkewEstimate()


def settle_ms(p50_ms: int, p99_ms: int, resolution_ms: int = 0) -> int:
    """`clamp(p99 − p50 + 250 ms, 250 ms, 5 s)` plus the adapter's `TickDateRaw` resolution (§A9)."""
    spread = p99_ms - p50_ms + SETTLE_MARGIN_MS
    return min(max(spread, SETTLE_MIN_MS), SETTLE_MAX_MS) + max(resolution_ms, 0)


# ---------------------------------------------------------------------------------------------------- table
@dataclass(frozen=True)
class RereadStep:
    """Re-read every `every_ms` while the read time is before local `T + until_ms` (None: until the cap)."""

    until_ms: int | None
    every_ms: int


@dataclass(frozen=True)
class TimingRow:
    spec: BarSpec
    cross_check_tick_spec: BarSpec
    pre_offset_default_s: int = 40
    pre_offset_min_s: int = 30             # design D1: 30-45 s
    pre_offset_max_s: int = 45
    pre_valid_for_ms: int = 10_000          # PRE valid until fire + 10 s (a 10 s window at every offset, S38c)
    post_grace_ms: int = 60_000
    reread: tuple[RereadStep, ...] = (
        RereadStep(60_000, 5_000),           # every 5 s until T + post_grace
        RereadStep(300_000, 30_000),         # every 30 s until T + 5 min
        RereadStep(None, 60_000),            # every 60 s until the cap
    )
    verify_after_final_ms: int = 60_000     # verification re-read at min(final + 60 s, cap − 30 s)
    verify_before_cap_ms: int = 30_000
    unsupported_lag_ms: int = 300_000       # a PR3 p99 finality lag above this makes the adapter / side UNSUPPORTED
    rollback_n_default: int = 8
    rollback_n_min: int = 2


TIMING_TABLE: Final[dict[BarSpec, TimingRow]] = {
    M15: TimingRow(spec=M15, cross_check_tick_spec=M15),
}
"""v1: the 15m row only (design §B5.2)."""


def timing_for(spec: BarSpec) -> TimingRow | None:
    return TIMING_TABLE.get(spec)


def timing_problem(spec: BarSpec) -> str | None:
    """Why a strategy (or decision-grade demand) on `spec` is refused, or None."""
    if spec in TIMING_TABLE:
        return None
    return f"bar spec {spec.name} has no timing row (v1 supports {sorted(s.name for s in TIMING_TABLE)})"


def pre_offset_problem(spec: BarSpec, pre_offset_s: int) -> str | None:
    row = timing_for(spec)
    if row is None:
        return timing_problem(spec)
    if not row.pre_offset_min_s <= pre_offset_s <= row.pre_offset_max_s:
        return f"pre_offset must be {row.pre_offset_min_s}-{row.pre_offset_max_s} s for {spec.name}"
    if pre_offset_s * 1000 <= row.pre_valid_for_ms:   # the window would reach the bar end
        return f"pre_offset must end the PRE window before the bar end for {spec.name}"
    return None


def rollback_n_problem(spec: BarSpec, n: int) -> str | None:
    row = timing_for(spec)
    if row is None:
        return timing_problem(spec)
    return None if n >= row.rollback_n_min else f"rollback N must be at least {row.rollback_n_min}"


# ---------------------------------------------------------------------------------------------------- the clock
def local_bar_end_ms(bar_end_ms: int, skew: SkewEstimate) -> int:
    """The bar end `T` (broker time) on the local clock: `T + p50`."""
    return bar_end_ms + skew.p50_ms


def decision_cap_ms(bar_end_ms: int, spec: BarSpec, skew: SkewEstimate) -> int:
    """`local T + timeframe + p50 + settle` (§B5.2): the one clock of every cap-based timer; never a setting."""
    return bar_end_ms + spec.timeframe_ms + skew.p50_ms + skew.settle_ms


def first_read_ms(bar_end_ms: int, skew: SkewEstimate) -> int:
    """The first broker read of bar `T`: `T + p50 + settle` (§A4, the one rule)."""
    return bar_end_ms + skew.p50_ms + skew.settle_ms


def next_read_ms(bar_end_ms: int, last_read_ms: int, row: TimingRow, skew: SkewEstimate,
                 lag_ms: int = 0) -> int | None:
    """The next re-read of a missing or non-final bar after a read at `last_read_ms` (§B5.2 schedule), or None when
    it would be at or after `decision_cap` (live re-reading stops; the bucket moves to the backfill lane).

    `lag_ms` is `XTNL_MD_BROKER_BAR_LAG_MS` (the PR3 p99 finality lag, §A4): it starts the back-off, so the fastest
    cadence lasts until `T + max(post_grace, lag)` and no later step ends before that."""
    offset = last_read_ms - local_bar_end_ms(bar_end_ms, skew)
    every = row.reread[-1].every_ms
    for step in row.reread:
        if step.until_ms is None or offset < max(step.until_ms, lag_ms):
            every = step.every_ms
            break
    due = last_read_ms + every
    return due if due < decision_cap_ms(bar_end_ms, row.spec, skew) else None


def verification_read_ms(final_at_ms: int, cap_ms: int, row: TimingRow) -> int | None:
    """The one verification re-read of a final BROKER bar at `min(final + 60 s, cap − 30 s)`; None when no such time
    remains after `final_at_ms` (the bar then carries the caveat `unverified_final`, §A4)."""
    at = min(final_at_ms + row.verify_after_final_ms, cap_ms - row.verify_before_cap_ms)
    return at if at > final_at_ms else None


def pre_fire_ms(bar_end_ms: int, skew: SkewEstimate, pre_offset_s: int) -> int:
    """PRE fires at local `T + p50 − pre_offset` (§B5.2)."""
    return local_bar_end_ms(bar_end_ms, skew) - pre_offset_s * 1000


def pre_valid_until_ms(fire_ms: int, bar_end_ms: int, row: TimingRow, skew: SkewEstimate) -> int:
    """`fire + 10 s` (§B5.2, round 12 / S38c): the window is 10 s at every allowed offset (30 s: it ends at local
    `T + p50 − 20 s`; 45 s: at `T + p50 − 35 s`), never empty. `bar_end_ms` and `skew` are kept for the callers'
    one signature (the window no longer depends on them)."""
    del bar_end_ms, skew
    return fire_ms + row.pre_valid_for_ms


def post_grace_end_ms(bar_end_ms: int, row: TimingRow, skew: SkewEstimate) -> int:
    """`T + post_grace` on the local clock: a POST decided later is `DELAYED`; a series still waiting pages once."""
    return local_bar_end_ms(bar_end_ms, skew) + row.post_grace_ms


# ---------------------------------------------------------------------------------------------------- 17:00 New York roll
NEW_YORK: Final = ZoneInfo("America/New_York")
ROLL_TIME: Final = time(17, 0)


def last_roll_ms(now_ms: int) -> int:
    """Epoch ms of the most recent weekday 17:00 New York roll at or before `now_ms` (DST-aware; Monday-Friday only,
    the Sunday open is not a roll). A copy of `src.pubsub.tradingday.last_roll_ms`, which this package may not
    import; `tests/marketdata/test_timing.py` keeps the two equal."""
    now = datetime.fromtimestamp(now_ms / 1000, tz=NEW_YORK)
    day = now.date()
    if now.time() < ROLL_TIME:
        day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return int(datetime.combine(day, ROLL_TIME, tzinfo=NEW_YORK).timestamp() * 1000)


def overlaps_roll(bucket_start_ms: int, bucket_end_ms: int) -> bool:
    """True when a weekday 17:00 New York roll lies inside `[bucket_start, bucket_end)` (the `roll_affected` caveat
    applies to such a bucket if PR3 shows break prints in broker bars, Q14)."""
    if bucket_end_ms <= bucket_start_ms:
        return False
    return last_roll_ms(bucket_end_ms - 1) >= bucket_start_ms
