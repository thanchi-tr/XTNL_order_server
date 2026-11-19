"""MarketCalendar: every SM10 MarketSession plus the per-adapter BROKER_MAINTENANCE overlay
(operations.md §10).

* Initialised from the versioned schedule at LEADER_INIT (§4 step 5): `init_from_schedule()`. Called again on
  a loaded calendar (a schedule reload) it keeps the machine state: OPEN_BY_EVIDENCE (with its evidence time),
  every symbol's state and entry time (only symbols the new version moves emit a `reload:` transition; new
  symbols emit `init:`), the active maintenance overlays (no second notice), the declared maintenance windows and
  the PRE_OPEN readiness checks already run. Adapters without sessions page again after each reload.
* `tick(now_ms)` (1 Hz) re-evaluates the level guards and emits callbacks:
  - `on_transition(SessionTransition)` for every session change (open / closed / roll transitions
    force a `last_clean_marks` upsert, §11);
  - `on_readiness(adapter_id, reason)` for the PRE_OPEN readiness check: once per scheduled open
    when open - 10 min is reached (also at init), after each 17:00 New York roll, and when a
    maintenance window ends after an open passed during it. Never for an adapter inside a window (normal
    back-off, no liveness page): its pre-open and roll checks are re-run when the window ends ("maintenance ended
    after an open" / "... after a roll");
  - `on_roll(roll_ms)` at each 17:00 America/New_York roll (roll marks);
  - `on_flag(adapter_id, symbol, "calendar_mismatch")` when evidence beats the calendar;
  - `on_maintenance(adapter_id, active, until_ms)` entering / leaving the overlay (state only: liveness / FENCE
    alerts are suppressed meanwhile);
  - `on_maintenance_notice(adapter_id, until_ms, reason)` once per overlay entry: the one pre-acknowledged
    notice (an `info` row: never re-paged, nothing to acknowledge). A window that extends or overlaps an active
    overlay, its end and a schedule reload send none;
  - `on_no_sessions(adapter_id)` (default: `on_flag(adapter_id, "*", "no_calendar_sessions")`) for an
    adapter that has no session in the loaded schedule: at init, again after each roll while it lasts, and
    the first time a tick or execution arrives from it. Such an adapter has no latest open, so the gate
    denies its risk-increasing actions (`latest_open_unknown`) until the schedule covers it. The adapters
    checked are `expected_adapters()`, those passed to `init_from_schedule`, and those seen on ticks.
* `market_open(adapter)` (dead-man payload, liveness, broker-grounds abdication): any **currently subscribed**
  symbol (`subscribed_symbols(adapter)`) OPEN or OPEN_BY_EVIDENCE; every scheduled symbol when none is known.
  `latest_open_ms` (the gate) still looks at every scheduled symbol: the latest open of any of them is the
  stricter reconcile bound.
* `mark_session(adapter, symbol)`: the session the RiskSweep marks under (BROKER_MAINTENANCE -> CLOSED).
* `ignored(adapter, symbol)` (plan S32, S05 open point 6): a symbol the market-data layer alone subscribes (a
  `XTNL_TICK_SYMBOLS` tick-only symbol) has a stored schedule so the market-data layer can read its session, but it
  never decides anything here: it is left out of `deciding_sessions` (its fallback included), `latest_open_ms`, the
  PRE_OPEN readiness check and the maintenance-ended readiness check. None (the default) ignores nothing, so with
  the optional services off the calendar behaves exactly as before.
* `seed_quotes(marks, now_ms)`: pre-close reference quotes from `last_clean_marks` at LEADER_INIT.
* Maintenance windows come from the schedule version (`CalendarSchedule.maintenance`), from
  `declare_maintenance(window)` and from `replace_declared_maintenance(windows)` (a durable source re-read
  periodically; idempotent). Declared windows survive schedule reloads.

Not thread-safe; owned by the event loop. Nothing here performs I/O.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Iterable
from decimal import Decimal
from typing import Literal

from src.calendar.machine import OPEN_STATES, MarketSession, SessionState, SessionTransition
from src.calendar.schedule import CalendarSchedule, MaintenanceWindow, ScheduleLoader, SymbolKey, SymbolSchedule
from src.calendar.sessions import opens_between
from src.pubsub.tradingday import last_roll_ms

log = logging.getLogger(__name__)

TransitionHook = Callable[[SessionTransition], None]
ReadinessHook = Callable[[str, str], None]          # (adapter_id, reason)
RollHook = Callable[[int], None]                    # roll instant (ms)
FlagHook = Callable[[str, str, str], None]          # (adapter_id, symbol, flag)
MaintenanceHook = Callable[[str, bool, int | None], None]
MaintenanceNoticeHook = Callable[[str, int, str], None]   # (adapter_id, until_ms, reason)
NoSessionsHook = Callable[[str], None]              # adapter_id
NO_SESSIONS_FLAG = "no_calendar_sessions"


def _noop(*_: object) -> None:
    return None


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


class ScheduledCalendarInit:
    """LEADER_INIT step 5 adapter (`src.runtime.leader_init.CalendarInit`): load the latest schedule
    version and initialise the calendar from it."""

    def __init__(self, loader: ScheduleLoader, calendar: "MarketCalendar",
                 adapters: Iterable[str] = ()) -> None:
        self._loader = loader
        self._calendar = calendar
        self._adapters = tuple(adapters)

    async def init_calendar(self, now_ms: int) -> int:
        schedule = await self._loader.load()
        self._calendar.init_from_schedule(schedule, now_ms, adapters=self._adapters)
        return schedule.version


class MarketCalendar:
    def __init__(self, *, on_transition: TransitionHook | None = None, on_readiness: ReadinessHook | None = None,
                 on_roll: RollHook | None = None, on_flag: FlagHook | None = None,
                 on_maintenance: MaintenanceHook | None = None, on_no_sessions: NoSessionsHook | None = None,
                 expected_adapters: Callable[[], Iterable[str]] | None = None,
                 on_maintenance_notice: MaintenanceNoticeHook | None = None,
                 subscribed_symbols: Callable[[str], Iterable[str]] | None = None,
                 ignored: Callable[[str, str], bool] | None = None) -> None:
        self._on_transition: TransitionHook = on_transition or _noop
        self._on_readiness: ReadinessHook = on_readiness or _noop
        self._on_roll: RollHook = on_roll or _noop
        self._on_flag: FlagHook = on_flag or _noop
        self._on_maintenance: MaintenanceHook = on_maintenance or _noop
        self._on_maintenance_notice: MaintenanceNoticeHook = on_maintenance_notice or _noop
        self._on_no_sessions: NoSessionsHook = on_no_sessions or self._flag_no_sessions
        self._expected_adapters = expected_adapters
        self._known_adapters: set[str] = set()            # adapters to check for sessions (init + ticks)
        self._no_sessions_paged: set[str] = set()         # paged since the last init / roll
        self.version: int | None = None
        self._sessions: dict[SymbolKey, MarketSession] = {}
        self._windows: list[MaintenanceWindow] = []         # schedule version's + declared
        self._declared: list[MaintenanceWindow] = []        # declared after the load (kept across reloads)
        self._schedule_windows: list[MaintenanceWindow] = []
        self._maint_since: dict[str, int] = {}             # adapter -> overlay entry time
        self._readiness_done: set[tuple[str, int]] = set()  # (adapter, open_ms) already checked
        self._readiness_deferred: set[str] = set()          # adapters whose roll check waits for a window's end
        self._last_roll: int | None = None
        self._subscribed = subscribed_symbols
        self._ignored = ignored

    # --- lifecycle ---------------------------------------------------------------------------------
    def init_from_schedule(self, schedule: CalendarSchedule, now_ms: int,
                           adapters: Iterable[str] = ()) -> list[SessionTransition]:
        """LEADER_INIT step 5: compute every (adapter, symbol)'s state from the schedule. `adapters` (and
        `expected_adapters()`) are checked for sessions: one without any pages (`on_no_sessions`).

        On a calendar already loaded (a schedule reload) the machine state is kept (`MarketSession.adopt`):
        returns only the transitions the new version causes (`reload:` for a moved symbol, `init:` for a new
        one). Maintenance overlays in force, declared windows and readiness checks already run are kept; the
        no-sessions page fires again for every adapter still without sessions."""
        reload = self.version is not None
        old = self._sessions
        self.version = schedule.version
        self._sessions = {k: MarketSession(s) for k, s in schedule.symbols.items()}
        self._schedule_windows = list(schedule.maintenance)
        self._rebuild_windows()
        if not reload:
            self._maint_since = {}
            self._readiness_done = set()
            self._readiness_deferred = set()
            self._last_roll = last_roll_ms(now_ms)
        elif self._last_roll is None:
            self._last_roll = last_roll_ms(now_ms)
        self._known_adapters.update(adapters)
        self._no_sessions_paged = set()
        out: list[SessionTransition] = []
        for key, s in self._sessions.items():
            prev = old.get(key) if reload else None
            t = s.adopt(prev, now_ms) if prev is not None else s.initialise(now_ms)
            if t is not None:
                out.append(t)
        for t in out:
            self._on_transition(t)
        self._update_maintenance(now_ms)
        self._check_pre_open(now_ms)
        self._check_sessions()
        return out

    def declare_maintenance(self, window: MaintenanceWindow) -> None:
        """Add a maintenance window announced after the schedule was loaded (kept across schedule reloads;
        the overlay starts at the next `tick`)."""
        if window not in self._declared:
            self._declared.append(window)
        self._rebuild_windows()

    def replace_declared_maintenance(self, windows: Iterable[MaintenanceWindow]) -> None:
        """Replace the declared windows with a durable source's current list (idempotent, so the source can be
        re-read every few seconds). A window removed while active ends the overlay at the next `tick`."""
        self._declared = list(dict.fromkeys(windows))
        self._rebuild_windows()

    def _rebuild_windows(self) -> None:
        self._windows = list(dict.fromkeys([*self._schedule_windows, *self._declared]))

    def tick(self, now_ms: int) -> list[SessionTransition]:
        if self.version is None:
            return []
        out: list[SessionTransition] = []
        for s in self._sessions.values():
            t = s.evaluate(now_ms)
            if t is not None:
                out.append(t)
                self._on_transition(t)
        self._update_maintenance(now_ms)     # first: a window starting with this tick already defers the roll check
        roll = last_roll_ms(now_ms)
        if self._last_roll is not None and roll != self._last_roll:
            self._on_roll(roll)
            for adapter in self.adapters():
                if adapter in self._maint_since:
                    # §10: normal back-off and no liveness pages during a window; the check runs when it ends.
                    self._readiness_deferred.add(adapter)
                    continue
                self._on_readiness(adapter, "roll")
            self._no_sessions_paged = set()   # still without sessions: page again once a day
            self._check_sessions()
            # Only future opens are looked up again, and reloads keep the set: drop the passed ones (bounded).
            self._readiness_done = {k for k in self._readiness_done if k[1] > now_ms}
        self._last_roll = roll
        self._check_pre_open(now_ms)
        return out

    async def run(self, stop: asyncio.Event, clock_ms: Callable[[], int] = _wall_ms,
                  interval_s: float = 1.0, stamp: Callable[[], None] | None = None) -> None:
        """1 Hz level evaluation (supervised task; `stamp` feeds its liveness)."""
        while not stop.is_set():
            self.tick(clock_ms())
            if stamp is not None:
                stamp()
            try:
                await asyncio.wait_for(stop.wait(), interval_s)
            except TimeoutError:
                pass

    # --- evidence ----------------------------------------------------------------------------------
    def on_tick(self, adapter_id: str, symbol: str, bid: Decimal, ask: Decimal, broker_ts_ms: int,
                now_ms: int, *, snapshot: bool = False) -> None:
        s = self._sessions.get((adapter_id, symbol))
        if s is None:
            self._unscheduled(adapter_id)
            return
        self._emit_evidence(s.on_tick(bid, ask, broker_ts_ms, now_ms, snapshot=snapshot))

    def on_execution(self, adapter_id: str, symbol: str, now_ms: int) -> None:
        s = self._sessions.get((adapter_id, symbol))
        if s is not None:
            self._emit_evidence(s.on_execution(now_ms))
        else:
            self._unscheduled(adapter_id)

    def seed_quote(self, adapter_id: str, symbol: str, bid: Decimal, ask: Decimal, ts_ms: int | None = None,
                   now_ms: int | None = None) -> None:
        """Seed one symbol's last pre-close quote (see `MarketSession.seed_quote`)."""
        s = self._sessions.get((adapter_id, symbol))
        if s is not None:
            s.seed_quote(bid, ask, ts_ms, now_ms)

    def seed_quotes(self, marks: Iterable[tuple[str, str, Decimal, Decimal, int]], now_ms: int) -> int:
        """LEADER_INIT (after `init_from_schedule`): seed the pre-close reference quotes from `last_clean_marks`
        rows (adapter_id, symbol, bid, ask, ts), so a takeover while CLOSED compares the first post-close tick with
        what the market closed at. A mark stamped after the scheduled close, or a symbol that already has a live
        reference, is skipped. Returns the number of symbols seeded."""
        n = 0
        for adapter_id, symbol, bid, ask, ts in marks:
            s = self._sessions.get((adapter_id, symbol))
            if s is None or s.preclose_quote is not None:
                continue
            s.seed_quote(bid, ask, ts, now_ms)
            n += s.preclose_quote is not None
        return n

    def _emit_evidence(self, t: SessionTransition | None) -> None:
        if t is None:
            return
        log.warning("calendar_mismatch %s:%s -> OPEN_BY_EVIDENCE (%s)", t.adapter_id, t.symbol, t.reason)
        self._on_transition(t)
        self._on_flag(t.adapter_id, t.symbol, "calendar_mismatch")

    # --- adapters without sessions ----------------------------------------------------------------
    def _flag_no_sessions(self, adapter_id: str) -> None:
        self._on_flag(adapter_id, "*", NO_SESSIONS_FLAG)

    def _unscheduled(self, adapter_id: str) -> None:
        """Evidence from a symbol without a schedule: page if its whole adapter has none (O(1) once known)."""
        if self.version is None or adapter_id in self._known_adapters:
            return
        self._known_adapters.add(adapter_id)
        self._check_sessions()

    def adapters_without_sessions(self, adapter_ids: Iterable[str]) -> list[str]:
        """The given adapters that have no session in the loaded schedule."""
        scheduled = {k[0] for k in self._sessions}
        return sorted(set(adapter_ids) - scheduled)

    def _check_sessions(self) -> None:
        if self.version is None:
            return
        wanted = set(self._known_adapters)
        if self._expected_adapters is not None:
            try:
                wanted.update(self._expected_adapters())
            except Exception:
                log.exception("expected_adapters failed")
        for adapter in self.adapters_without_sessions(wanted):
            if adapter in self._no_sessions_paged:
                continue
            self._no_sessions_paged.add(adapter)
            log.error("calendar v%s has no sessions for adapter %s: its latest open is unknown, so the gate "
                      "denies its risk-increasing actions", self.version, adapter)
            self._on_no_sessions(adapter)

    # --- maintenance overlay -----------------------------------------------------------------------
    def _active_window_end(self, adapter_id: str, now_ms: int) -> int | None:
        ends = [w.end_ms for w in self._windows if w.adapter_id == adapter_id and w.active(now_ms)]
        return max(ends) if ends else None

    def _update_maintenance(self, now_ms: int) -> None:
        horizon = now_ms - 86_400_000
        self._schedule_windows = [w for w in self._schedule_windows if w.end_ms > horizon]
        self._declared = [w for w in self._declared if w.end_ms > horizon]
        self._rebuild_windows()
        for adapter in sorted(set(self.adapters()) | {w.adapter_id for w in self._windows} | set(self._maint_since)):
            until = self._active_window_end(adapter, now_ms)
            since = self._maint_since.get(adapter)
            if until is not None and since is None:
                self._maint_since[adapter] = now_ms
                self._on_maintenance(adapter, True, until)
                reason = next((w.reason for w in self._windows
                               if w.adapter_id == adapter and w.active(now_ms) and w.end_ms == until),
                              "broker maintenance")
                try:
                    self._on_maintenance_notice(adapter, until, reason)
                except Exception:
                    log.exception("maintenance notice for %s failed", adapter)
            elif until is None and since is not None:
                del self._maint_since[adapter]
                self._on_maintenance(adapter, False, None)
                rolled = adapter in self._readiness_deferred
                self._readiness_deferred.discard(adapter)
                if any(opens_between(s.schedule, since, now_ms) for s in self._deciding(adapter)):
                    self._on_readiness(adapter, "maintenance ended after an open")
                elif rolled:
                    self._on_readiness(adapter, "maintenance ended after a roll")

    def _check_pre_open(self, now_ms: int) -> None:
        for s in self._sessions.values():
            if s.state in (SessionState.OPEN, SessionState.ROLLOVER) or not s.in_pre_open_window(now_ms):
                continue
            if self._is_ignored(s.adapter_id, s.symbol):
                continue
            nxt = s.next_open_ms(now_ms)
            if nxt is None:
                continue
            key = (s.adapter_id, nxt)
            if key not in self._readiness_done and s.adapter_id not in self._maint_since:
                self._readiness_done.add(key)
                self._on_readiness(s.adapter_id, "pre_open")

    # --- queries -----------------------------------------------------------------------------------
    def adapters(self) -> list[str]:
        return sorted({k[0] for k in self._sessions})

    def _for_adapter(self, adapter_id: str) -> Iterable[MarketSession]:
        return (s for k, s in self._sessions.items() if k[0] == adapter_id)

    def _is_ignored(self, adapter_id: str, symbol: str) -> bool:
        """A symbol only the market-data layer subscribes (`ignored`). A raising predicate ignores nothing (logged):
        the symbol then decides like any scheduled one, as without the market-data layer."""
        if self._ignored is None:
            return False
        try:
            return bool(self._ignored(adapter_id, symbol))
        except Exception:
            log.exception("ignored(%s, %s) failed: the symbol decides like any scheduled one", adapter_id, symbol)
            return False

    def _deciding(self, adapter_id: str) -> list[MarketSession]:
        """Every scheduled session of the adapter that may decide something (not `ignored`)."""
        return [s for s in self._for_adapter(adapter_id) if not self._is_ignored(s.adapter_id, s.symbol)]

    def schedule_of(self, adapter_id: str, symbol: str) -> SymbolSchedule | None:
        """The versioned schedule of one (adapter, symbol), or None when it is not scheduled (the ARM's bar grid,
        plan S36b: which 15m buckets are open and when a closed period ends)."""
        s = self._sessions.get((adapter_id, symbol))
        return None if s is None else s.schedule

    def session_state(self, adapter_id: str, symbol: str) -> SessionState | None:
        s = self._sessions.get((adapter_id, symbol))
        return s.state if s is not None else None

    def state(self, adapter_id: str, symbol: str) -> SessionState | None:
        """Effective state: BROKER_MAINTENANCE overlays the session state while a window is active."""
        if adapter_id in self._maint_since:
            return SessionState.BROKER_MAINTENANCE
        return self.session_state(adapter_id, symbol)

    def in_maintenance(self, adapter_id: str) -> bool:
        return adapter_id in self._maint_since

    def maintenance_until(self, adapter_id: str, now_ms: int) -> int | None:
        return self._active_window_end(adapter_id, now_ms)

    def deciding_sessions(self, adapter_id: str) -> list[MarketSession]:
        """The sessions that decide the adapter's market (operations.md §10 "any of its subscribed symbols"): its
        currently subscribed symbols (`subscribed_symbols(adapter)`) that have a schedule. Fail-loud fallback to
        every scheduled symbol of the adapter when no provider is wired, the provider raises, or none of the
        subscribed symbols is scheduled (an adapter that never connected subscribes nothing, yet its liveness and
        the dead-man's 'not STREAMING while market open' must still be judged)."""
        every = self._deciding(adapter_id)
        if self._subscribed is None:
            return every
        try:
            wanted = set(self._subscribed(adapter_id))
        except Exception:
            log.exception("subscribed_symbols(%s) failed: every scheduled symbol decides its market", adapter_id)
            return every
        chosen = [s for s in every if s.symbol in wanted]
        return chosen if chosen else every

    def market_open(self, adapter_id: str) -> bool:
        """An adapter's market is OPEN when any of its subscribed symbols is OPEN for it (`deciding_sessions`), so a
        symbol held or subscribed once with wider hours (a 24/7 CFD) no longer keeps the market open for ever."""
        return any(s.state in OPEN_STATES for s in self.deciding_sessions(adapter_id))

    def mark_session(self, adapter_id: str, symbol: str) -> Literal["OPEN", "ROLLOVER", "CLOSED"]:
        """The SM10 session the RiskSweep marks a symbol under (`SymbolQuotes.session`, oversee-rules.md §5).

        BROKER_MAINTENANCE maps to CLOSED (settled in 7.2, fail-closed): during a window the adapter's feed is down
        or unreliable, so the symbol is marked from its last clean mid with `FLAG stale_mark` (capped at SOFT) at
        once, instead of being trusted as a fresh clean mid for up to `stale_mark_ms`. PRE_OPEN is CLOSED;
        OPEN_BY_EVIDENCE is OPEN. A symbol the calendar does not know, or no calendar loaded, is OPEN (the stale
        cases of an open market then apply, from the quotes' own timestamps)."""
        if self.version is None:
            return "OPEN"
        st = self.state(adapter_id, symbol)
        if st is None or st in OPEN_STATES:
            return "OPEN"
        if st is SessionState.ROLLOVER:
            return "ROLLOVER"
        return "CLOSED"

    def price_staleness_suppressed(self, adapter_id: str, symbol: str) -> bool:
        """While CLOSED or in ROLLOVER (or in maintenance) only price staleness is suppressed."""
        st = self.state(adapter_id, symbol)
        return st in (SessionState.CLOSED, SessionState.ROLLOVER, SessionState.PRE_OPEN,
                      SessionState.BROKER_MAINTENANCE)

    def latest_open_ms(self, adapter_id: str, now_ms: int) -> int | None:
        """The latest market open for the adapter (scheduled, or entry into OPEN_BY_EVIDENCE); feeds
        GateContext.latest_open_ms (oversee-rules.md §8). An evidence open still counts after the symbol has left
        OPEN_BY_EVIDENCE (PRE_OPEN at open - 10 min, m11, or CLOSED after 30 min quiet), so the bound never moves
        back to an earlier scheduled open (fail closed: the account must be reconciled after it)."""
        best: int | None = None
        for s in self._deciding(adapter_id):
            cand = s.last_open_ms(now_ms)
            if s.state is SessionState.OPEN_BY_EVIDENCE and s.entered_ms is not None:
                cand = max(cand or 0, s.entered_ms)
            if s.evidence_open_ms is not None and s.evidence_open_ms <= now_ms:
                cand = max(cand or 0, s.evidence_open_ms)
            if cand is not None and (best is None or cand > best):
                best = cand
        return best
