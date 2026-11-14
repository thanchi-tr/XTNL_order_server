"""Trading-day roll: 17:00 America/New_York, DST-aware (tier-persistence.md §4 sod_marks).

Rolls happen Monday-Friday; the day that starts at Friday's roll covers the weekend
(the Sunday open is not a roll). Holidays are SM10's concern, not this helper's.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")
ROLL_TIME = time(17, 0)


def last_roll_ms(now_ms: int) -> int:
    """Epoch ms of the most recent weekday 17:00 New York roll at or before now_ms."""
    now = datetime.fromtimestamp(now_ms / 1000, tz=NEW_YORK)
    day = now.date()
    if now.time() < ROLL_TIME:
        day -= timedelta(days=1)
    while day.weekday() >= 5:   # Saturday / Sunday: no roll
        day -= timedelta(days=1)
    roll = datetime.combine(day, ROLL_TIME, tzinfo=NEW_YORK)
    return int(roll.timestamp() * 1000)
