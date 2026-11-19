"""ARM liveness pages (design §B8 "Liveness", §C2 Liveness; plan S38g, round 14).

Three ways the ARM can go quiet without any error, each watched here and paged once per episode:

1. **An armed position no longer decided** (`arm_overdue_positions`). For every live row in a deciding state
   (ARMED, TRACKING, ADVICE_OUTSTANDING: not released, not SUSPENDED or FAILED), not covered by a FAILED scope and
   while neither pause holds, `undecided_open_bars` counts the OPEN bar ends after its cursor (`last_decided_bar`)
   whose `decision_cap` has passed, from the SM10 schedule (the `CalendarGrid`, never the BarClock's own pass). Every
   such bar is normally decided or spanned at its cap, so more than `overdue_open_bars` (2) of them means the
   position silently stopped being evaluated (a BarClock pass that keeps returning early, a symbol whose schedule
   never loaded, a frontier that never moves). A row whose symbol has **no schedule** counts as overdue, never as
   zero. The count is the dead-man field `arm_overdue_positions` and the `arm_status` / metric `overdue` view; the
   local page is "ARM: armed position not decided for 2 OPEN bars".
   **Confirmation** (round 14 review): the rows are the arm-tracker's (read every 10 s), so after a restart that
   follows a long downtime they still show the old cursor for up to 10 s after the BarClock wrote the DOWNTIME span
   (within about 250 ms of being allowed to decide). A position first seen overdue is therefore counted and paged
   only once (a) a BarClock pass that **started after** that first sighting has completed and the condition still
   holds on tracker rows **read after** the watch saw that pass, or (b) it has stayed overdue for
   `overdue_confirm_ms` (120 s; a BarClock that makes no such pass is also paged as stalled). `LivenessWatch.confirm`.
2. **A stalled loop**: the BarClock's or the arm-tracker's last completed pass (`last_pass_ms`) older than
   `barclock_stale_ms` (60 s; the pass runs every 250 ms) or `tracker_stale_ms` (120 s; every 10 s), or none at all
   that long after the loop was last seen allowed to run (`running` turning True: the BarClock runs only once the
   FAILED episodes are read and no ARM-global FAILED holds, so a pass older than that start, or none, is measured
   from the start; round 14 review). Pages "ARM: decision loop stalled" / "ARM: arm-tracker stalled" (a task
   whose stamp goes stale is restarted by the supervisor; this page also covers a loop that stamps but never gets
   through a pass).
3. **The arm-relay backlog**: the oldest unsent ARM notice older than `relay_backlog_ms` (15 min). Pages
   "ARM: notice relay backlog" (the dead-man's `arm_oldest_undelivered_notification_age_ms` pages off-node too).

Each page fires once when its condition starts and again only after it has cleared. Pure apart from the pager; the
caller (`src/runtime/optional_plan.py`, the `arm-status` loop every 5 s, independent of the `arm` loop) passes
every input in. Pages carry ids, counts and ages only.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from src.db.arm_ports import ArmPositionRow
from src.marketdata.timing import decision_cap_ms, timing_for
from src.marketdata.types import BarSpec
from src.riskmgr.ports import Pager
from src.riskmgr.sm11 import ArmState
from src.riskmgr.tracker import BarGridPort

WATCHED_STATES: Final = frozenset({ArmState.ARMED.value, ArmState.TRACKING.value,
                                   ArmState.ADVICE_OUTSTANDING.value})
"""Rows whose bars must be decided: not released (STRATEGY_COMPLETE), SUSPENDED, FAILED or ended."""
NO_SCHEDULE: Final = -1
"""`undecided_open_bars` for a row whose symbol has no loaded schedule: overdue, never zero."""

OVERDUE_TITLE: Final = "ARM: armed position not decided for 2 OPEN bars"
BARCLOCK_TITLE: Final = "ARM: decision loop stalled"
TRACKER_TITLE: Final = "ARM: arm-tracker stalled"
RELAY_TITLE: Final = "ARM: notice relay backlog"


@dataclass(frozen=True, kw_only=True)
class LivenessSettings:
    overdue_open_bars: int = 2
    """More undecided OPEN bars past their cap than this is overdue (design §B8: "more than 2 OPEN bars")."""
    barclock_stale_ms: int = 60_000
    tracker_stale_ms: int = 120_000
    relay_backlog_ms: int = 900_000
    overdue_confirm_ms: int = 120_000
    """A position overdue this long counts even without a confirming BarClock pass (module docstring)."""

    def __post_init__(self) -> None:
        if self.overdue_open_bars < 1 or min(self.barclock_stale_ms, self.tracker_stale_ms,
                                             self.relay_backlog_ms, self.overdue_confirm_ms) <= 0:
            raise ValueError("liveness thresholds must be positive")


def undecided_open_bars(row: ArmPositionRow, grid: BarGridPort, now_ms: int,
                        schedule_known: Callable[[str, str], bool]) -> int:
    """The OPEN bar ends after the row's cursor whose `decision_cap` has passed (`NO_SCHEDULE` when the symbol has
    no loaded schedule; 0 for a bar spec without a timing row, which arming refuses)."""
    try:
        spec = BarSpec.parse(row.bar_spec)
    except ValueError:
        return 0
    if timing_for(spec) is None:
        return 0
    adapter_id = row.key.adapter_id
    try:
        known = bool(schedule_known(adapter_id, row.symbol))
    except Exception:
        known = False
    if not known:
        return NO_SCHEDULE
    skew = grid.skew(adapter_id)
    cursor = row.last_decided_bar if row.last_decided_bar is not None else row.armed_at_ms
    ends = grid.open_bar_ends(adapter_id, row.symbol, spec, cursor, now_ms - skew.p50_ms)
    return sum(1 for e in ends if decision_cap_ms(e, spec, skew) < now_ms)


def overdue_positions(rows: Sequence[ArmPositionRow], grid: BarGridPort, now_ms: int, *,
                      schedule_known: Callable[[str, str], bool], held: Callable[[ArmPositionRow], bool],
                      paused: bool, settings: LivenessSettings | None = None) -> dict[str, int]:
    """`{position ref: undecided OPEN bars (NO_SCHEDULE for none)}` of the overdue rows (see the module docstring).
    Nothing is overdue while a pause holds; a row covered by a FAILED scope (`held`) is not counted (its episode has
    pinged); a raising `held` counts the row (fail toward the page)."""
    cfg = settings or LivenessSettings()
    if paused:
        return {}
    out: dict[str, int] = {}
    for row in rows:
        if row.state not in WATCHED_STATES:
            continue
        try:
            if held(row):
                continue
        except Exception:
            pass
        n = undecided_open_bars(row, grid, now_ms, schedule_known)
        if n == NO_SCHEDULE or n > cfg.overdue_open_bars:
            out[row.ref] = n
    return out


@dataclass
class _Suspect:
    """A position seen overdue, not confirmed yet: when the watch first saw it and when the watch first saw a
    completed BarClock pass that started after that (None: not yet)."""

    first_seen_ms: int
    clock_seen_ms: int | None = None


class LivenessWatch:
    """The three liveness pages (module docstring), once per episode each."""

    def __init__(self, pager: Pager, settings: LivenessSettings | None = None) -> None:
        self._pager = pager
        self.settings = settings or LivenessSettings()
        self._overdue_paged: set[str] = set()
        self._stalled: set[str] = set()
        self._relay_paged = False
        self._running_since: dict[str, int] = {}
        self._suspects: dict[str, _Suspect] = {}
        self.pages = 0

    def confirm(self, overdue: Mapping[str, int], now_ms: int, *, rows_read_ms: int | None,
                barclock_last_pass_ms: int | None) -> dict[str, int]:
        """The confirmed subset of `overdue` (computed from the tracker rows read at `rows_read_ms`): a position
        counts once a BarClock pass that started after it was first seen overdue has completed and tracker rows read
        after the watch saw that pass still show it overdue, or once it has been overdue for `overdue_confirm_ms`
        (module docstring). A position no longer overdue ends its episode. No I/O."""
        for ref in [r for r in self._suspects if r not in overdue]:
            del self._suspects[ref]
        out: dict[str, int] = {}
        for ref, n in overdue.items():
            sus = self._suspects.get(ref)
            if sus is None:
                sus = self._suspects[ref] = _Suspect(first_seen_ms=now_ms)
            if (sus.clock_seen_ms is None and barclock_last_pass_ms is not None
                    and barclock_last_pass_ms > sus.first_seen_ms):
                sus.clock_seen_ms = now_ms         # a pass started after the first sighting and has completed
            seen_after = (sus.clock_seen_ms is not None and rows_read_ms is not None
                          and rows_read_ms > sus.clock_seen_ms)
            if seen_after or now_ms - sus.first_seen_ms >= self.settings.overdue_confirm_ms:
                out[ref] = n
        return out

    def positions(self, overdue: Mapping[str, int]) -> None:
        """Page each newly overdue position once; a position that is decided again may page again later."""
        for ref in sorted(set(overdue) - self._overdue_paged):
            n = overdue[ref]
            detail = "its symbol has no loaded market schedule" if n == NO_SCHEDULE else \
                f"{n} OPEN bars past their decision cap are undecided"
            self.pages += 1
            self._pager.page("warning", "ARM: armed position not decided for 2 OPEN bars",
                             f"{ref}: {detail}; check the arm loop, the calendar and `arm show`")
        self._overdue_paged = set(overdue)

    def loops(self, now_ms: int, *, barclock_last_pass_ms: int | None, tracker_last_pass_ms: int | None,
              barclock_running: bool = True, tracker_running: bool = True) -> None:
        """Page a loop whose last completed pass is too old, measured from no earlier than the time the watch first
        saw the loop allowed to run (`running` turning True; a loop not running is never stalled and its start is
        forgotten), so a loop that starts or resumes after a long hold is never paged before it could pass."""
        checks = (("barclock", barclock_running, barclock_last_pass_ms, self.settings.barclock_stale_ms),
                  ("tracker", tracker_running, tracker_last_pass_ms, self.settings.tracker_stale_ms))
        for name, running, last, limit in checks:
            if not running:
                self._running_since.pop(name, None)
                self._stalled.discard(name)
                continue
            started = self._running_since.setdefault(name, now_ms)
            since = started if last is None else max(last, started)
            stale = now_ms - since > limit
            if stale and name not in self._stalled:
                self._stalled.add(name)
                age = "no completed pass" if last is None else f"last completed pass {(now_ms - last) // 1000} s ago"
                body = f"{name}: {age} (limit {limit // 1000} s)"
                self.pages += 1
                if name == "barclock":
                    self._pager.page("warning", "ARM: decision loop stalled", body)
                else:
                    self._pager.page("warning", "ARM: arm-tracker stalled", body)
            elif not stale:
                self._stalled.discard(name)

    def relay(self, now_ms: int, oldest_undelivered_ms: int | None) -> None:
        """Page once while the oldest unsent ARM notice is older than `relay_backlog_ms`."""
        age = None if oldest_undelivered_ms is None else now_ms - oldest_undelivered_ms
        backlog = age is not None and age > self.settings.relay_backlog_ms
        if backlog and not self._relay_paged:
            self._relay_paged = True
            assert age is not None
            self.pages += 1
            self._pager.page("warning", "ARM: notice relay backlog",
                             f"oldest unsent ARM notice is {age // 1000} s old "
                             f"(limit {self.settings.relay_backlog_ms // 1000} s); check the ARM channels")
        elif not backlog:
            self._relay_paged = False


__all__ = ["BARCLOCK_TITLE", "LivenessSettings", "LivenessWatch", "NO_SCHEDULE", "OVERDUE_TITLE", "RELAY_TITLE",
           "TRACKER_TITLE", "WATCHED_STATES", "overdue_positions", "undecided_open_bars"]
