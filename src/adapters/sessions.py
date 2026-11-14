"""Broker market hours -> the `market_sessions` JSON layout (SM10, operations.md §10). Pure, no I/O.

Brokers report weekly trading intervals as seconds since the start of the week (cTrader: Sunday
00:00 in the symbol's schedule zone; StoneX pricing times: day of week + UTC time of day). This
module merges them and splits the gaps:
- a gap of at most ``roll_break_max_s`` (default 2 h: the daily roll / maintenance break) becomes a
  daily break (SM10 ROLLOVER: no PRE_OPEN, staleness muted);
- a longer gap (the weekend) closes the session (SM10 CLOSED).
A week with no long gap is one 24/7 session (open == close) plus its breaks, which is how
src/calendar/schedule.py spells an always-open symbol.

The output parses with src/calendar/schedule.py:symbol_schedule_from_json (the adapters cannot
import src.calendar, which pulls in src.pubsub; the adapter tests check the round trip).
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.adapters.base import MarketSessions

DAY_S = 86_400
WEEK_S = 7 * DAY_S
SUNDAY_FIRST = ("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT")
ROLL_BREAK_MAX_S = 2 * 3_600

Interval = tuple[int, int]   # [start, end) seconds of the week, Sunday 00:00 = 0

#: Documented FX default (used only when the broker reports no hours for an FX symbol): the FX week
#: runs from Sunday 17:00 to Friday 17:00 New York time, with the 17:00-17:05 daily roll Mon-Thu.
#: Wall-clock New York times, so it follows US DST (operations.md §14).
FX_DEFAULT_TIMEZONE = "America/New_York"
FX_DEFAULT_NOTE = ("default FX hours (Sunday 17:00 - Friday 17:00 New York, 17:00-17:05 roll Mon-Thu): "
                   "the broker reported no trading hours for this symbol; no holidays; review before relying on it")


_METALS = frozenset({"XAU", "XAG", "XPT", "XPD"})   # metals quoted as pairs do not keep FX hours


def canonical(symbol: str) -> str:
    """'eur/usd' -> 'EURUSD' (the canonical InstrumentSpec.symbol spelling: upper-case letters and digits)."""
    return "".join(ch for ch in symbol.upper() if ch.isascii() and ch.isalnum())


def looks_like_fx_pair(symbol: str) -> bool:
    """Six letters, neither leg a metal: the only symbols the FX default may ever be applied to."""
    s = canonical(symbol)
    return len(s) == 6 and s.isalpha() and s[:3] not in _METALS and s[3:] not in _METALS


def fx_default_schedule() -> dict[str, Any]:
    return {
        "sessions": [{"open": {"day": "SUN", "time": "17:00"}, "close": {"day": "FRI", "time": "17:00"}}],
        "breaks": [{"start": "17:00", "end": "17:05", "days": ["MON", "TUE", "WED", "THU"]}],
    }


def fx_default_sessions(adapter_id: str, symbol: str, *, source: str, note: str = FX_DEFAULT_NOTE) -> MarketSessions:
    """The documented FX default, flagged ``is_default`` and ``holidays_known=False``."""
    return MarketSessions(adapter_id=adapter_id, symbol=symbol, timezone=FX_DEFAULT_TIMEZONE,
                          schedule=fx_default_schedule(), holidays=[], source=source, is_default=True,
                          holidays_known=False, note=note)


def valid_timezone(name: object) -> str | None:
    """The IANA zone name when zoneinfo knows it, else None."""
    if not isinstance(name, str) or not name.strip():
        return None
    try:
        ZoneInfo(name.strip())
    except (ZoneInfoNotFoundError, ValueError):
        return None
    return name.strip()


def clock(second_of_week: int) -> tuple[str, str]:
    """Seconds since Sunday 00:00 -> ("MON", "17:05" or "17:05:30")."""
    sec = second_of_week % WEEK_S
    day, rest = divmod(sec, DAY_S)
    h, rest = divmod(rest, 3_600)
    m, s = divmod(rest, 60)
    return SUNDAY_FIRST[day], f"{h:02d}:{m:02d}" if s == 0 else f"{h:02d}:{m:02d}:{s:02d}"


def normalise(intervals: Iterable[Interval]) -> list[Interval]:
    """Clip to one week, split intervals that wrap past Saturday 24:00, sort and merge."""
    parts: list[Interval] = []
    for start, end in intervals:
        length = end - start
        if length <= 0:
            continue
        if length >= WEEK_S:
            return [(0, WEEK_S)]
        s0 = start % WEEK_S
        e0 = s0 + length
        if e0 <= WEEK_S:
            parts.append((s0, e0))
        else:
            parts.extend(((s0, WEEK_S), (0, e0 - WEEK_S)))
    parts.sort()
    merged: list[Interval] = []
    for s, e in parts:
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def subtract(intervals: Iterable[Interval], cuts: Iterable[Interval]) -> list[Interval]:
    """``intervals`` minus ``cuts`` (both in seconds of the week), normalised."""
    out = normalise(intervals)
    for cs, ce in normalise(cuts):
        nxt: list[Interval] = []
        for s, e in out:
            if ce <= s or cs >= e:
                nxt.append((s, e))
                continue
            if s < cs:
                nxt.append((s, cs))
            if ce < e:
                nxt.append((ce, e))
        out = nxt
    return normalise(out)


def weekly_schedule(intervals: Iterable[Interval], *, roll_break_max_s: int = ROLL_BREAK_MAX_S) -> dict[str, Any] | None:
    """Trading intervals -> {"sessions": [...], "breaks": [...]}; None when there is no interval."""
    iv = normalise(intervals)
    n = len(iv)
    if n == 0:
        return None
    gaps = [(iv[(i + 1) % n][0] - iv[i][1]) % WEEK_S for i in range(n)]
    big = [i for i, g in enumerate(gaps) if g > roll_break_max_s]

    grouped: dict[tuple[str, str], list[str]] = {}
    for i, g in enumerate(gaps):
        if 0 < g <= roll_break_max_s:
            day, start = clock(iv[i][1])
            _, end = clock(iv[i][1] + g)
            days = grouped.setdefault((start, end), [])
            if day not in days:
                days.append(day)
    breaks = [{"start": s, "end": e, "days": days} for (s, e), days in grouped.items()]

    sessions: list[dict[str, Any]] = []
    if not big:
        day, t = clock(iv[0][0])                       # open == close: 24/7 (breaks still apply)
        sessions.append({"open": {"day": day, "time": t}, "close": {"day": day, "time": t}})
    else:
        runs = sorted((iv[(gap_index + 1) % n][0], iv[big[(k + 1) % len(big)]][1])
                      for k, gap_index in enumerate(big))
        for open_s, close_s in runs:
            open_day, open_t = clock(open_s)
            close_day, close_t = clock(close_s)
            sessions.append({"open": {"day": open_day, "time": open_t},
                             "close": {"day": close_day, "time": close_t}})
    out: dict[str, Any] = {"sessions": sessions}
    if breaks:
        out["breaks"] = breaks
    return out
