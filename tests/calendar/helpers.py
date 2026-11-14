"""Calendar test helpers: an FX-like schedule (Sun 17:00 -> Fri 17:00 New York, 5 min daily break)."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.calendar.schedule import CalendarSchedule, MaintenanceWindow, SymbolSchedule, symbol_schedule_from_json

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

FX_SCHEDULE: Mapping[str, Any] = {
    "sessions": [{"open": {"day": "SUN", "time": "17:00"}, "close": {"day": "FRI", "time": "17:00"}}],
    "breaks": [{"start": "17:00", "end": "17:05", "days": ["MON", "TUE", "WED", "THU"]}],
}


def ny(y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0) -> int:
    return int(datetime(y, mo, d, h, mi, s, tzinfo=NY).timestamp() * 1000)


def utc(y: int, mo: int, d: int, h: int = 0, mi: int = 0) -> int:
    return int(datetime(y, mo, d, h, mi, tzinfo=UTC).timestamp() * 1000)


def fx(adapter: str = "stonex", symbol: str = "EUR/USD",
       holidays: Sequence[Mapping[str, Any]] = ()) -> SymbolSchedule:
    return symbol_schedule_from_json(adapter, symbol, "America/New_York", FX_SCHEDULE, holidays)


def calendar(*schedules: SymbolSchedule, version: int = 7,
             maintenance: Sequence[MaintenanceWindow] = ()) -> CalendarSchedule:
    return CalendarSchedule(version, {s.key: s for s in schedules}, tuple(maintenance))
