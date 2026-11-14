"""SM10 MarketSession: one state machine per (adapter_id, symbol) (operations.md §10).

Guards are **level conditions** evaluated against the versioned schedule on every tick, so a
machine initialised at any instant (LEADER_INIT §4 step 5) lands in the right state, and a takeover
just before an open still runs PRE_OPEN:

    scheduled OPEN                  -> OPEN
    scheduled daily break           -> ROLLOVER
    closed, now >= open - 10 min    -> PRE_OPEN           (also out of OPEN_BY_EVIDENCE: SM10 edge m11)
    closed, evidence < 30 min old   -> OPEN_BY_EVIDENCE   (evidence beats the calendar)
    otherwise (weekly close, holiday) -> CLOSED

Evidence: while CLOSED, a tick on *this* symbol with a broker timestamp after the scheduled close and
a price that differs from the **last pre-close quote**, or an execution on this symbol, moves it to
OPEN_BY_EVIDENCE (FLAG calendar_mismatch). Subscription snapshot items never count. A scheduled-OPEN
symbol with no ticks is never moved to CLOSED (the stale-price page fires instead).

The pre-close reference quote is the last quote seen while the symbol was OPEN or in ROLLOVER, any quote whose
broker timestamp is at or before the scheduled close, or `seed_quote()` (last_clean_marks at LEADER_INIT); of those,
the newest by broker time wins (an out-of-order older quote never replaces it). Ticks after the close never move
it. With no reference at all (a takeover while closed, nothing seeded), the first
post-close quote becomes the reference and is not evidence itself: a price is evidence only once it can be shown
to differ from what the market closed at (7.2), so a re-sent closing quote never opens a closed symbol.
BROKER_MAINTENANCE is a per-adapter overlay held by MarketCalendar, not a state of this machine.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Final

from src.calendar.schedule import SymbolSchedule
from src.calendar.sessions import Phase, last_close_ms, last_open_ms, next_session_open_ms, phase_at

PRE_OPEN_LEAD_MS: Final = 10 * 60_000
EVIDENCE_QUIET_MS: Final = 30 * 60_000


class SessionState(str, Enum):
    CLOSED = "CLOSED"
    PRE_OPEN = "PRE_OPEN"
    OPEN = "OPEN"
    ROLLOVER = "ROLLOVER"
    OPEN_BY_EVIDENCE = "OPEN_BY_EVIDENCE"
    BROKER_MAINTENANCE = "BROKER_MAINTENANCE"   # reported overlay only (MarketCalendar.state)


OPEN_STATES: Final = frozenset({SessionState.OPEN, SessionState.OPEN_BY_EVIDENCE})


@dataclass(frozen=True)
class SessionTransition:
    adapter_id: str
    symbol: str
    old: SessionState
    new: SessionState
    at_ms: int
    reason: str


class MarketSession:
    """SM10 for one (adapter_id, symbol). Pure: callers pass `now_ms`; nothing here reads a clock."""

    def __init__(self, schedule: SymbolSchedule) -> None:
        self.schedule = schedule
        self.state = SessionState.CLOSED
        self.initialised = False
        self.last_evidence_ms: int | None = None
        self.entered_ms: int | None = None
        # The latest entry into OPEN_BY_EVIDENCE (an evidence open). Kept after the symbol leaves that state (to
        # PRE_OPEN at open - 10 min, m11, or to CLOSED after 30 min without evidence), so the gate's latest open
        # (`MarketCalendar.latest_open_ms`) never moves back to an earlier scheduled open (fail closed).
        self.evidence_open_ms: int | None = None
        self._last_quote: tuple[Decimal, Decimal] | None = None
        self._last_quote_ts: int | None = None          # broker timestamp of `_last_quote` (None: unknown)
        self._next_open_cache: tuple[int, int | None] | None = None   # (valid_until_ms, next open)

    @property
    def adapter_id(self) -> str:
        return self.schedule.adapter_id

    @property
    def symbol(self) -> str:
        return self.schedule.symbol

    # --- schedule queries --------------------------------------------------------------------------
    def next_open_ms(self, now_ms: int) -> int | None:
        """Next scheduled open out of CLOSED (cached until it passes)."""
        cached = self._next_open_cache
        if cached is not None and now_ms < cached[0]:
            return cached[1]
        nxt = next_session_open_ms(self.schedule, now_ms)
        self._next_open_cache = (nxt if nxt is not None else now_ms + 3_600_000, nxt)
        return nxt

    def last_open_ms(self, now_ms: int) -> int | None:
        return last_open_ms(self.schedule, now_ms)

    def in_pre_open_window(self, now_ms: int) -> bool:
        nxt = self.next_open_ms(now_ms)
        return nxt is not None and now_ms >= nxt - PRE_OPEN_LEAD_MS

    # --- guards ------------------------------------------------------------------------------------
    def _target(self, now_ms: int) -> tuple[SessionState, str]:
        ph = phase_at(self.schedule, now_ms)
        if ph is Phase.OPEN:
            return SessionState.OPEN, "scheduled open"
        if ph is Phase.ROLLOVER:
            return SessionState.ROLLOVER, "daily break"
        if self.in_pre_open_window(now_ms):          # also out of OPEN_BY_EVIDENCE (SM10 m11, 7.2)
            return SessionState.PRE_OPEN, "open - 10 min"
        if (self.state is SessionState.OPEN_BY_EVIDENCE and self.last_evidence_ms is not None
                and now_ms - self.last_evidence_ms < EVIDENCE_QUIET_MS):
            return SessionState.OPEN_BY_EVIDENCE, "evidence"
        if self.state is SessionState.OPEN_BY_EVIDENCE:
            return SessionState.CLOSED, "30 min without evidence"
        return SessionState.CLOSED, "scheduled close / holiday"

    def _go(self, new: SessionState, now_ms: int, reason: str) -> SessionTransition | None:
        old = self.state
        if new is old and self.initialised:
            return None
        self.state = new
        self.entered_ms = now_ms
        self.initialised = True
        if new is not SessionState.OPEN_BY_EVIDENCE:
            self.last_evidence_ms = None
        return SessionTransition(self.adapter_id, self.symbol, old, new, now_ms, reason)

    def initialise(self, now_ms: int) -> SessionTransition:
        """Initial state from the schedule (LEADER_INIT step 5). Always returns the entry transition."""
        self.initialised = False
        self.state = SessionState.CLOSED
        new, reason = self._target(now_ms)
        t = self._go(new, now_ms, f"init: {reason}")
        assert t is not None
        return t

    def adopt(self, old: "MarketSession", now_ms: int) -> SessionTransition | None:
        """A schedule reload: carry the previous version's machine state (state, entry time, evidence and the
        last quote) over to this schedule, then re-evaluate the level guards against the new schedule. Returns
        a normal (not `init:`) transition only when the new schedule moves the symbol, so OPEN_BY_EVIDENCE
        survives a reload while it still has fresh evidence and the schedule still says closed."""
        self.state = old.state
        self.initialised = old.initialised
        self.last_evidence_ms = old.last_evidence_ms
        self.entered_ms = old.entered_ms
        self.evidence_open_ms = old.evidence_open_ms
        self._last_quote = old._last_quote
        self._last_quote_ts = old._last_quote_ts
        if not self.initialised:
            return self.initialise(now_ms)
        t = self.evaluate(now_ms)
        if t is not None:
            return SessionTransition(t.adapter_id, t.symbol, t.old, t.new, t.at_ms, f"reload: {t.reason}")
        return None

    def evaluate(self, now_ms: int) -> SessionTransition | None:
        """Re-evaluate the level guards; returns the transition if the state changed."""
        new, reason = self._target(now_ms)
        return self._go(new, now_ms, reason)

    # --- evidence ----------------------------------------------------------------------------------
    @property
    def preclose_quote(self) -> tuple[Decimal, Decimal] | None:
        """The reference quote post-close ticks are compared with (None: none known yet)."""
        return self._last_quote

    def seed_quote(self, bid: Decimal, ask: Decimal, ts_ms: int | None = None, now_ms: int | None = None) -> None:
        """Seed the last pre-close quote (from last_clean_marks at LEADER_INIT) so the first post-close tick can be
        compared with it. With `ts_ms` and `now_ms`, a quote stamped after the scheduled close is not a pre-close
        quote and is ignored; a reference already observed live is never replaced."""
        if self._last_quote is not None:
            return
        if ts_ms is not None and now_ms is not None:
            close = last_close_ms(self.schedule, now_ms)
            if self.state in (SessionState.CLOSED, SessionState.OPEN_BY_EVIDENCE, SessionState.PRE_OPEN) \
                    and close is not None and ts_ms > close:
                return
        self._last_quote = (bid, ask)
        self._last_quote_ts = ts_ms

    def _take_reference(self, quote: tuple[Decimal, Decimal], broker_ts_ms: int) -> None:
        """A pre-close quote becomes the reference unless it is older (by broker time) than the one held: a late,
        out-of-order quote (a re-sent snapshot item stamped earlier) never replaces the newer closing quote."""
        if self._last_quote is None or self._last_quote_ts is None or broker_ts_ms >= self._last_quote_ts:
            self._last_quote = quote
            self._last_quote_ts = broker_ts_ms

    def on_tick(self, bid: Decimal, ask: Decimal, broker_ts_ms: int, now_ms: int, *,
                snapshot: bool = False) -> SessionTransition | None:
        """A tick on this symbol. Returns the OPEN_BY_EVIDENCE transition when it is evidence."""
        quote = (bid, ask)
        if self.state in (SessionState.OPEN, SessionState.ROLLOVER):
            self._take_reference(quote, broker_ts_ms)   # the market is (scheduled) open: a pre-close quote
            return None
        close = last_close_ms(self.schedule, now_ms)
        if close is None or broker_ts_ms <= close:
            self._take_reference(quote, broker_ts_ms)   # stamped before the close: a (late) pre-close quote
            return None
        reference = self._last_quote
        if reference is None:
            self._take_reference(quote, broker_ts_ms)   # nothing to compare with: the reference, never evidence
            return None
        if snapshot or self.state not in (SessionState.CLOSED, SessionState.OPEN_BY_EVIDENCE):
            return None
        if reference == quote:
            return None
        return self._evidence(now_ms, "post-close tick with a new price")

    def on_execution(self, now_ms: int) -> SessionTransition | None:
        """An execution event on this symbol is evidence while CLOSED."""
        if self.state not in (SessionState.CLOSED, SessionState.OPEN_BY_EVIDENCE):
            return None
        return self._evidence(now_ms, "execution on symbol")

    def _evidence(self, now_ms: int, reason: str) -> SessionTransition | None:
        if self.state is SessionState.OPEN_BY_EVIDENCE:
            self.last_evidence_ms = now_ms
            return None
        t = self._go(SessionState.OPEN_BY_EVIDENCE, now_ms, reason)
        self.last_evidence_ms = now_ms
        self.evidence_open_ms = now_ms
        return t
