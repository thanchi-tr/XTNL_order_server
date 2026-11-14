"""Pure schedule arithmetic for SM10 (operations.md §10, §14).

`phase_at()` is a level function of one instant (OPEN / ROLLOVER / CLOSED from sessions, breaks and
holidays in local wall time). Transitions can only happen at a local boundary (session open/close,
break start/end, closure start/end) or at a DST transition of the zone, so `next_open_ms()` and
`last_close_ms()` evaluate `phase_at()` only at those candidate instants. Everything is DST-safe:
local times come from zoneinfo, never from a fixed UTC offset.
"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from zoneinfo import ZoneInfo

from src.calendar.schedule import SymbolSchedule

DAY_MS = 86_400_000
DEFAULT_HORIZON_DAYS = 15
ALWAYS_OPEN_SINCE_MS = 0   # last_open_ms of a symbol open since before any closure its schedule lists (24/7)


class Phase(str, Enum):
    OPEN = "OPEN"
    ROLLOVER = "ROLLOVER"   # inside a daily break
    CLOSED = "CLOSED"       # outside every weekly session, or inside a holiday closure


def to_local(tz: ZoneInfo, ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(tz)


def local_to_ms(tz: ZoneInfo, naive: datetime) -> int:
    """A naive local wall time -> UTC ms (fold=0: an ambiguous time takes its first occurrence)."""
    return int(naive.replace(tzinfo=tz).timestamp() * 1000)


def phase_at(s: SymbolSchedule, ms: int) -> Phase:
    local = to_local(s.tz, ms)
    naive = local.replace(tzinfo=None)
    if any(c.contains(naive) for c in s.closures):
        return Phase.CLOSED
    if not any(sess.contains(local) for sess in s.sessions):
        return Phase.CLOSED
    if any(b.contains(local) for b in s.breaks):
        return Phase.ROLLOVER
    return Phase.OPEN


def _dst_transitions(tz: ZoneInfo, day: date) -> Iterator[int]:
    """UTC ms of an offset change between local midnight of `day` and the next one (bisection to 1 s)."""
    lo = local_to_ms(tz, datetime.combine(day, time(0)))
    hi = local_to_ms(tz, datetime.combine(day + timedelta(days=1), time(0)))

    def offset(ms: int) -> timedelta | None:
        return to_local(tz, ms).utcoffset()

    if offset(lo) == offset(hi):
        return
    first = offset(lo)
    while hi - lo > 1000:
        mid = (lo + hi) // 2
        if offset(mid) == first:
            lo = mid
        else:
            hi = mid
    yield hi


def boundaries(s: SymbolSchedule, from_ms: int, to_ms: int) -> list[int]:
    """Every candidate transition instant in [from_ms, to_ms], sorted and unique."""
    out: set[int] = set()
    start_day = to_local(s.tz, from_ms).date() - timedelta(days=1)
    end_day = to_local(s.tz, to_ms).date() + timedelta(days=1)
    day = start_day
    while day <= end_day:
        wd = day.weekday()
        times: list[time] = []
        for sess in s.sessions:
            if sess.open_day == wd:
                times.append(sess.open_time)
            if sess.close_day == wd:
                times.append(sess.close_time)
        for b in s.breaks:
            if wd in b.days or (b.start > b.end and (wd - 1) % 7 in b.days):
                times.extend((b.start, b.end))
        for t in times:
            out.add(local_to_ms(s.tz, datetime.combine(day, t)))
        out.update(_dst_transitions(s.tz, day))
        day += timedelta(days=1)
    for c in s.closures:
        out.add(local_to_ms(s.tz, c.start))
        out.add(local_to_ms(s.tz, c.end))
    return sorted(b for b in out if from_ms <= b <= to_ms)


def _is_open_edge(s: SymbolSchedule, b: int) -> bool:
    return phase_at(s, b) is Phase.OPEN and phase_at(s, b - 1) is not Phase.OPEN


def next_open_ms(s: SymbolSchedule, now_ms: int, horizon_days: int = DEFAULT_HORIZON_DAYS) -> int | None:
    """The first instant > now_ms at which the symbol becomes OPEN (from CLOSED or ROLLOVER)."""
    for b in boundaries(s, now_ms + 1, now_ms + horizon_days * DAY_MS):
        if _is_open_edge(s, b):
            return b
    return None


def next_session_open_ms(s: SymbolSchedule, now_ms: int,
                         horizon_days: int = DEFAULT_HORIZON_DAYS) -> int | None:
    """Like next_open_ms but only opens out of CLOSED (the PRE_OPEN target; a break end is not one)."""
    for b in boundaries(s, now_ms + 1, now_ms + horizon_days * DAY_MS):
        if phase_at(s, b) is Phase.OPEN and phase_at(s, b - 1) is Phase.CLOSED:
            return b
    return None


def last_open_ms(s: SymbolSchedule, now_ms: int, horizon_days: int = DEFAULT_HORIZON_DAYS) -> int | None:
    """The most recent instant <= now_ms at which the symbol became OPEN.

    A symbol that is OPEN now without an open edge in the horizon has been open all along (a 24/7 session:
    weekly sessions and daily breaks repeat within a week, so only a holiday closure can still have opened it):
    it reports the end of its latest closure before now, else `ALWAYS_OPEN_SINCE_MS`. Never None for an open
    symbol, so the gate's "reconciled after the latest open" check needs only a reconcile in this epoch."""
    for b in reversed(boundaries(s, now_ms - horizon_days * DAY_MS, now_ms)):
        if _is_open_edge(s, b):
            return b
    if phase_at(s, now_ms) is not Phase.OPEN:
        return None
    ends = [e for e in (local_to_ms(s.tz, c.end) for c in s.closures) if e <= now_ms]
    return max(ends) if ends else ALWAYS_OPEN_SINCE_MS


def last_close_ms(s: SymbolSchedule, now_ms: int, horizon_days: int = DEFAULT_HORIZON_DAYS) -> int | None:
    """The most recent instant <= now_ms at which the symbol became CLOSED (scheduled close)."""
    for b in reversed(boundaries(s, now_ms - horizon_days * DAY_MS, now_ms)):
        if phase_at(s, b) is Phase.CLOSED and phase_at(s, b - 1) is not Phase.CLOSED:
            return b
    return None


def opens_between(s: SymbolSchedule, after_ms: int, until_ms: int) -> bool:
    """True when a scheduled open (out of CLOSED or ROLLOVER) falls in (after_ms, until_ms]."""
    if until_ms <= after_ms:
        return False
    for b in boundaries(s, after_ms + 1, until_ms):
        if _is_open_edge(s, b):
            return True
    return False
