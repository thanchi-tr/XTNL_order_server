from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from src.pubsub.tradingday import last_roll_ms

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def ms(y: int, mo: int, d: int, h: int, mi: int = 0, tz: ZoneInfo = NY) -> int:
    return int(datetime(y, mo, d, h, mi, tzinfo=tz).timestamp() * 1000)


def test_after_and_before_roll_on_a_weekday() -> None:
    assert last_roll_ms(ms(2026, 11, 11, 18)) == ms(2026, 11, 11, 17)   # Wednesday
    assert last_roll_ms(ms(2026, 11, 11, 16, 59)) == ms(2026, 11, 10, 17)
    assert last_roll_ms(ms(2026, 11, 11, 17)) == ms(2026, 11, 11, 17)


def test_weekend_and_monday_morning_use_fridays_roll() -> None:
    friday = ms(2026, 11, 13, 17)
    assert last_roll_ms(ms(2026, 11, 14, 12)) == friday   # Saturday
    assert last_roll_ms(ms(2026, 11, 15, 18)) == friday   # Sunday open is not a roll
    assert last_roll_ms(ms(2026, 11, 16, 9)) == friday    # Monday before the roll


def test_dst_aware() -> None:
    assert last_roll_ms(ms(2026, 7, 15, 23, tz=UTC)) == ms(2026, 7, 15, 21, tz=UTC)    # EDT
    assert last_roll_ms(ms(2026, 1, 14, 23, tz=UTC)) == ms(2026, 1, 14, 22, tz=UTC)    # EST
