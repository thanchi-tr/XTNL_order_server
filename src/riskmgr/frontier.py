"""Stuck-frontier detection (design §B6.6, §B14; plan S22, R11-M11).

A **stuck replay frontier** is an ARM-scope fault: while the ARM runs, a bar of a position whose scope is neither
FAILED nor paused is still neither decided nor `UNRESOLVED` 120 s after its `decision_cap`, because the
`UNRESOLVED_RANGE` span write itself fails. Only counting time adds to the 120 s: time with no ARM running, time while
engine PostgreSQL is down (the term pool's last fenced commit older than 30 s, or the RiskSweep's unreachable alert,
`src.riskmgr.ports.engine_pg_down`) and the other §B14 counting pauses never count, and a position on a FAILED scope
or a paused one never counts at all (its cursor stays put by design), so a scoped FAILED never becomes a global one.

**Keyed by cause.** `ARM_WRITE`: the ARM pool cannot be built or its transactions (the span write) fail; the
frontier never files this itself: it defers to the `fail_pool_down_s` row (S30: the pool failing that long while the
engine's main pool works, measured there), so a pool outage is one fault, not two. `DATA`: no ARM write is failing,
yet the frontier did not move: at once after the grace. `FrontierWatch.observe` returns a `FrontierFault` when a DATA
fault becomes due, once per position and episode (a position's episode ends when it is no longer over the grace:
its cursor moved, or its scope is held). **Scoped** (round 12, plan S38c): the budget files one POSITION scope per
newly stuck position while fewer than `fail_frontier_positions_n` (default 2) positions are over the grace in the
pass, so one poisoned row never fails every position; at that count (a shared cause) the ARM fails globally. Either
way the reason is `stuck_frontier` and the probe kind `span_write`, which re-runs the span write of the recorded
positions (rolled back). It clears a position's clock as soon as its
cursor moves past the overdue bar or its scope is held.

In-memory state over ids and times only; nothing here logs.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final

STUCK_GRACE_MS: Final = 120_000
STUCK_FRONTIER: Final = "stuck_frontier"
"""The ARM-scope `arm_failures.reason` (its probe kind is `span_write`, `src.db.arm_schema.REASON_PROBE_KIND`:
a test fenced span write on the ARM pool succeeds, §B14 Clearing)."""


class StuckCause(str, Enum):
    ARM_WRITE = "arm_write"   # the ARM pool or its transactions (the span write) fail: filed by fail_pool_down_s
    DATA = "data"             # no ARM write failing: the frontier is stuck for another cause, at once after the grace


@dataclass(frozen=True, kw_only=True)
class FrontierView:
    """One position as the BarClock sees it this pass: `overdue_bar_ms` is the oldest bar after its cursor whose
    `decision_cap` has passed (None when none); `held`: its scope is FAILED or it is paused."""

    position_id: str
    overdue_bar_ms: int | None
    held: bool = False


@dataclass(frozen=True)
class FrontierFault:
    """A due stuck-frontier fault: the cause, the positions newly over the grace (not filed before in their
    episode), the oldest of their overdue bars and every position over the grace in this pass (`stuck`, which
    decides the scope: one POSITION scope each, or ARM-global at `fail_frontier_positions_n`, S38c)."""

    cause: StuckCause
    position_ids: tuple[str, ...]
    oldest_bar_ms: int
    stuck: tuple[str, ...] = ()

    @property
    def stuck_count(self) -> int:
        return len(self.stuck) or len(self.position_ids)

    @property
    def key(self) -> str:
        """The in-memory once-per-(cause, episode) key; never `arm_failures.reason` (that is `STUCK_FRONTIER`)."""
        return f"{STUCK_FRONTIER}:{self.cause.value}"


def classify(*, pool_failing: bool) -> StuckCause:
    """The cause a stuck frontier is filed under: ARM-side while the ARM pool cannot be built or its transactions
    (the span write among them) fail, else DATA."""
    return StuckCause.ARM_WRITE if pool_failing else StuckCause.DATA


class FrontierWatch:
    """Counting time per (position, overdue bar) and the due DATA fault (once per episode)."""

    def __init__(self, *, grace_ms: int = STUCK_GRACE_MS) -> None:
        if grace_ms <= 0:
            raise ValueError("the grace is positive")
        self._grace_ms = grace_ms
        self._elapsed: dict[str, tuple[int, int]] = {}
        self._last_ms: int | None = None
        self._filed: set[str] = set()

    def elapsed_ms(self, position_id: str) -> int:
        held = self._elapsed.get(position_id)
        return 0 if held is None else held[1]

    def observe(self, views: Sequence[FrontierView], now_ms: int, *, counting: bool,
                pool_failing: bool = False) -> FrontierFault | None:
        """One pass. `counting`: the ARM runs and no §B14 counting pause holds (engine PostgreSQL up, market open,
        no maintenance window); `pool_failing`: the ARM pool cannot be built or its transactions fail while the main
        pool works. Time adds only between two counting passes. Returns the due fault once per (cause, episode), or
        None; an ARM_WRITE stuck frontier is never returned (the `fail_pool_down_s` row files the pool outage)."""
        step = 0 if self._last_ms is None or not counting else max(now_ms - self._last_ms, 0)
        self._last_ms = now_ms if counting else None
        kept: dict[str, tuple[int, int]] = {}
        over: list[tuple[str, int]] = []
        for v in views:
            if v.held or v.overdue_bar_ms is None:
                continue
            held = self._elapsed.get(v.position_id)
            total = held[1] + step if held is not None and held[0] == v.overdue_bar_ms else 0
            kept[v.position_id] = (v.overdue_bar_ms, total)
            if total >= self._grace_ms:
                over.append((v.position_id, v.overdue_bar_ms))
        self._elapsed = kept
        self._filed &= {p for p, _ in over}   # a position no longer over the grace (moved or held) ends its episode
        if not over:
            return None
        cause = classify(pool_failing=pool_failing)
        if cause is StuckCause.ARM_WRITE:
            return None    # the fail_pool_down_s row files the pool outage
        new = [(p, b) for p, b in over if p not in self._filed]
        if not new:
            return None    # each position once per episode
        self._filed |= {p for p, _ in new}
        return FrontierFault(cause, tuple(sorted(p for p, _ in new)), min(b for _, b in new),
                             tuple(sorted(p for p, _ in over)))
