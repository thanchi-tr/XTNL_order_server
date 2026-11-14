"""SM10 schedule model: versioned per-(adapter, symbol) sessions, daily breaks, holidays and
per-adapter maintenance windows (operations.md §10, tier-persistence.md §4 `market_sessions`).

All session times are local wall-clock times in the schedule's IANA zone (usually
America/New_York), so "17:00 New York" stays 17:00 across DST (operations.md §14). Instants are
UTC epoch milliseconds. The JSON layout matches the `market_sessions.schedule` / `holidays` jsonb:

    schedule = {"sessions": [{"open": {"day": "SUN", "time": "17:00"},
                              "close": {"day": "FRI", "time": "17:00"}}],
                "breaks":   [{"start": "17:00", "end": "17:05", "days": ["MON", "TUE", "WED", "THU"]}]}
    holidays = [{"date": "2026-12-25"},                                   # whole local day closed
                {"start": "2026-12-24T13:00", "end": "2026-12-24T17:00", "reason": "early close"}]
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any, Protocol
from zoneinfo import ZoneInfo

WEEKDAYS: Mapping[str, int] = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
_WEEK_S = 7 * 86_400

SymbolKey = tuple[str, str]   # (adapter_id, symbol)


class ScheduleError(ValueError):
    """A schedule row could not be parsed; the calendar version is rejected as a whole."""


def _sow(day: int, t: time) -> int:
    """Seconds of the local week (Monday 00:00 = 0)."""
    return day * 86_400 + t.hour * 3_600 + t.minute * 60 + t.second


@dataclass(frozen=True)
class WeeklySession:
    """One weekly trading session [open, close) in local time; may wrap the week (Sun -> Fri)."""

    open_day: int
    open_time: time
    close_day: int
    close_time: time

    def contains(self, local: datetime) -> bool:
        m = _sow(local.weekday(), local.time())
        start, end = _sow(self.open_day, self.open_time), _sow(self.close_day, self.close_time)
        if start == end:
            return True                      # 24x7 session
        if start < end:
            return start <= m < end
        return m >= start or m < end         # wraps past Sunday midnight


@dataclass(frozen=True)
class DailyBreak:
    """Daily break (ROLLOVER) [start, end) local, starting on the listed weekdays; may cross midnight."""

    start: time
    end: time
    days: frozenset[int] = frozenset(range(7))

    def contains(self, local: datetime) -> bool:
        t, wd = local.time(), local.weekday()
        if self.start < self.end:
            return wd in self.days and self.start <= t < self.end
        # crosses midnight: the part after midnight belongs to the previous day's break
        return (wd in self.days and t >= self.start) or ((wd - 1) % 7 in self.days and t < self.end)


@dataclass(frozen=True)
class Closure:
    """A holiday or early close: closed for [start, end) in naive local time."""

    start: datetime
    end: datetime
    reason: str = "holiday"

    @staticmethod
    def full_day(day: date, reason: str = "holiday") -> "Closure":
        start = datetime.combine(day, time(0, 0))
        return Closure(start, start + timedelta(days=1), reason)

    def contains(self, naive_local: datetime) -> bool:
        return self.start <= naive_local < self.end


@dataclass(frozen=True)
class SymbolSchedule:
    """The schedule of one (adapter, symbol) in one calendar version."""

    adapter_id: str
    symbol: str
    tz: ZoneInfo
    sessions: tuple[WeeklySession, ...]
    breaks: tuple[DailyBreak, ...] = ()
    closures: tuple[Closure, ...] = ()

    @property
    def key(self) -> SymbolKey:
        return (self.adapter_id, self.symbol)


@dataclass(frozen=True)
class MaintenanceWindow:
    """A declared broker maintenance window for one adapter, [start_ms, end_ms) (operations.md §10)."""

    adapter_id: str
    start_ms: int
    end_ms: int
    reason: str = "broker maintenance"

    def active(self, now_ms: int) -> bool:
        return self.start_ms <= now_ms < self.end_ms


@dataclass(frozen=True)
class CalendarSchedule:
    """One calendar version: every (adapter, symbol) schedule plus the maintenance windows."""

    version: int
    symbols: Mapping[SymbolKey, SymbolSchedule]
    maintenance: tuple[MaintenanceWindow, ...] = field(default=())


def maintenance_window_from_json(adapter_id: str, obj: Mapping[str, Any]) -> MaintenanceWindow:
    """One declared window: {"start": ISO-8601 instant with an offset, "end": ..., "reason": "..."} or epoch
    milliseconds {"start_ms": ..., "end_ms": ...}. Naive times are refused (a window is an instant range)."""
    try:
        if "start_ms" in obj:
            start, end = int(obj["start_ms"]), int(obj["end_ms"])
        else:
            s_dt, e_dt = datetime.fromisoformat(str(obj["start"])), datetime.fromisoformat(str(obj["end"]))
            if s_dt.tzinfo is None or e_dt.tzinfo is None:
                raise ScheduleError(f"maintenance window needs an offset (UTC instants): {obj!r}")
            start, end = int(s_dt.timestamp() * 1000), int(e_dt.timestamp() * 1000)
    except (KeyError, TypeError, ValueError) as exc:
        raise ScheduleError(f"bad maintenance window {obj!r}") from exc
    if end <= start:
        raise ScheduleError(f"maintenance window must end after it starts: {obj!r}")
    return MaintenanceWindow(adapter_id, start, end, str(obj.get("reason", "broker maintenance")))


class ScheduleLoader(Protocol):
    """Loads the latest calendar version (PostgreSQL `market_sessions`; built from the cTrader symbol
    schedule / StoneX market information or a reviewed config, operations.md §10)."""

    async def load(self) -> CalendarSchedule: ...


# --- JSON parsing ---------------------------------------------------------------------------------
def _day(v: Any) -> int:
    if isinstance(v, str) and v.upper()[:3] in WEEKDAYS:
        return WEEKDAYS[v.upper()[:3]]
    raise ScheduleError(f"bad weekday {v!r}")


def _time(v: Any) -> time:
    try:
        return time.fromisoformat(str(v))
    except ValueError as exc:
        raise ScheduleError(f"bad time {v!r}") from exc


def _closure(obj: Mapping[str, Any]) -> Closure:
    reason = str(obj.get("reason", "holiday"))
    try:
        if "date" in obj:
            return Closure.full_day(date.fromisoformat(str(obj["date"])), reason)
        start, end = datetime.fromisoformat(str(obj["start"])), datetime.fromisoformat(str(obj["end"]))
    except (KeyError, ValueError) as exc:
        raise ScheduleError(f"bad holiday {obj!r}") from exc
    if start.tzinfo is not None or end.tzinfo is not None or end <= start:
        raise ScheduleError(f"holiday must be a naive local interval with end > start: {obj!r}")
    return Closure(start, end, reason)


def symbol_schedule_from_json(adapter_id: str, symbol: str, timezone: str, schedule: Mapping[str, Any],
                              holidays: Sequence[Mapping[str, Any]] = ()) -> SymbolSchedule:
    """Parse one `market_sessions` row."""
    try:
        tz = ZoneInfo(timezone)
    except Exception as exc:
        raise ScheduleError(f"unknown timezone {timezone!r}") from exc
    sessions = tuple(
        WeeklySession(_day(s["open"]["day"]), _time(s["open"]["time"]),
                      _day(s["close"]["day"]), _time(s["close"]["time"]))
        for s in schedule.get("sessions", ()))
    if not sessions:
        raise ScheduleError(f"{adapter_id}:{symbol} has no sessions")
    breaks = tuple(
        DailyBreak(_time(b["start"]), _time(b["end"]),
                   frozenset(_day(d) for d in b.get("days", list(WEEKDAYS))))
        for b in schedule.get("breaks", ()))
    return SymbolSchedule(adapter_id, symbol, tz, sessions, breaks, tuple(_closure(h) for h in holidays))
