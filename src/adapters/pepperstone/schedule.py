"""cTrader symbol schedule + holidays -> MarketSessions (SM10, operations.md §10). Pure, no I/O.

ProtoOASymbol (the ProtoOASymbolByIdRes rows, already on the read-only allow-list) carries:
- ``schedule``: ProtoOAInterval rows {startSecond, endSecond}, seconds since Sunday 00:00 in
  ``scheduleTimeZone`` (to confirm against the API version);
- ``holiday``: ProtoOAHoliday rows {name, scheduleTimeZone, holidayDate (days since 1970-01-01),
  isRecurring, startSecond / endSecond (seconds since the holiday's local midnight; both absent =
  the whole day)}.
cTrader has no separate holiday request: holidays travel inside the symbol, so no payloadType is
added to the allow-list for them.

Holidays are written as naive local intervals in the symbol's schedule zone (the calendar's layout);
a holiday in another zone is converted. Recurring holidays are expanded for last, this and next
year; holidays that ended more than ``HOLIDAY_LOOKBACK_DAYS`` ago are dropped.
"""
from __future__ import annotations

import logging
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from src.adapters.base import MarketSessions
from src.adapters.pepperstone.mapping import objs
from src.adapters.pepperstone.protocol import JsonObj
from src.adapters.sessions import ROLL_BREAK_MAX_S, Interval, valid_timezone, weekly_schedule
from src.adapters.stonex.wire import to_bool, to_int

log = logging.getLogger(__name__)

SOURCE = "ctrader"
DEFAULT_SCHEDULE_TZ = "UTC"
HOLIDAY_LOOKBACK_DAYS = 31
_EPOCH = date(1970, 1, 1)


def schedule_intervals(full: JsonObj) -> list[Interval]:
    out: list[Interval] = []
    for row in objs(full, "schedule"):
        start, end = to_int(row.get("startSecond")), to_int(row.get("endSecond"))
        if start is None or end is None or start < 0 or end <= start:
            continue
        out.append((start, end))
    return out


def _holiday_dates(base: date, recurring: bool, now_year: int) -> list[date]:
    if not recurring:
        return [base]
    out: list[date] = []
    for year in (now_year - 1, now_year, now_year + 1):
        try:
            out.append(base.replace(year=year))
        except ValueError:          # 29 February in a non-leap year
            continue
    return out


def holidays_from_symbol(full: JsonObj, tz_name: str, now_ms: int,
                         lookback_days: int = HOLIDAY_LOOKBACK_DAYS) -> list[dict[str, Any]]:
    """ProtoOAHoliday rows -> market_sessions.holidays entries in ``tz_name`` local time."""
    tz = ZoneInfo(tz_name)
    now_year = datetime.fromtimestamp(now_ms / 1000, tz=UTC).year
    cutoff_ms = now_ms - lookback_days * 86_400_000
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in objs(full, "holiday"):
        days = to_int(row.get("holidayDate"))
        if days is None:
            continue
        h_tz_name = valid_timezone(row.get("scheduleTimeZone")) or tz_name
        h_tz = ZoneInfo(h_tz_name)
        start_s, end_s = to_int(row.get("startSecond")), to_int(row.get("endSecond"))
        whole_day = start_s is None and end_s is None
        start_s = 0 if start_s is None else start_s
        end_s = 86_400 if end_s is None else end_s
        if start_s < 0 or end_s <= start_s:
            log.warning("ctrader: holiday %r has an empty interval, skipped", row.get("name"))
            continue
        reason = str(row.get("name") or "holiday")
        try:
            base = _EPOCH + timedelta(days=days)
        except OverflowError:
            continue
        for day in _holiday_dates(base, to_bool(row.get("isRecurring")) is True, now_year):
            midnight = datetime.combine(day, time(0), tzinfo=h_tz).astimezone(UTC)
            start_utc, end_utc = midnight + timedelta(seconds=start_s), midnight + timedelta(seconds=end_s)
            if end_utc.timestamp() * 1000 < cutoff_ms:
                continue
            entry: dict[str, Any]
            if whole_day and h_tz_name == tz_name:
                entry = {"date": day.isoformat(), "reason": reason}
            else:
                start = start_utc.astimezone(tz).replace(tzinfo=None)
                end = end_utc.astimezone(tz).replace(tzinfo=None)
                if end <= start:
                    continue
                entry = {"start": start.isoformat(timespec="seconds"), "end": end.isoformat(timespec="seconds"),
                         "reason": reason}
            key = (str(entry.get("date") or entry["start"]), str(entry.get("end", "")))
            if key not in seen:
                seen.add(key)
                out.append(entry)
    out.sort(key=lambda e: str(e.get("date") or e["start"]))
    return out


def sessions_from_symbol(full: JsonObj, *, adapter_id: str, symbol: str, now_ms: int,
                         roll_break_max_s: int = ROLL_BREAK_MAX_S) -> MarketSessions | None:
    """One ProtoOASymbol -> MarketSessions; None when it has no usable schedule or zone."""
    raw_tz = full.get("scheduleTimeZone")
    notes: list[str] = []
    if raw_tz is None or (isinstance(raw_tz, str) and not raw_tz.strip()):
        tz_name = DEFAULT_SCHEDULE_TZ
        notes.append("scheduleTimeZone absent: UTC assumed (to confirm)")
    else:
        found = valid_timezone(raw_tz)
        if found is None:
            log.warning("ctrader: %s has an unknown scheduleTimeZone %r (no sessions)", symbol, raw_tz)
            return None
        tz_name = found
    schedule = weekly_schedule(schedule_intervals(full), roll_break_max_s=roll_break_max_s)
    if schedule is None:
        log.warning("ctrader: %s reports no trading schedule (no sessions)", symbol)
        return None
    return MarketSessions(adapter_id=adapter_id, symbol=symbol, timezone=tz_name, schedule=schedule,
                          holidays=holidays_from_symbol(full, tz_name, now_ms), source=SOURCE,
                          is_default=False, holidays_known=True, note="; ".join(notes))
