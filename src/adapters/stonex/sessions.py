"""StoneX market information trading hours -> MarketSessions (SM10, operations.md §10). Pure, no I/O.

ApiMarketInformationDTO (POST /v2/market/information, the read-only call fetch_instruments already
makes) may carry ``MarketPricingTimes`` and ``MarketBreakTimes``: rows {DayOfWeek, StartTimeUtc,
EndTimeUtc} (to confirm against the live API). DayOfWeek is .NET's (0 = Sunday) or a day name; a time
is "HH:MM[:SS]", a WCF ``/Date(ms)/`` or an ISO date-time (its UTC time of day is used). An end at or
before the start runs into the next day. Break rows are cut out of the pricing times.
All-or-nothing: one unreadable row and the market counts as having no hours. Sessions are in UTC.

StoneX exposes no holiday list, so ``holidays_known`` is always False. A market with no hours gets
the documented FX default (sessions.FX_DEFAULT_*) only when it is an FX pair, flagged ``is_default``;
any other market is left without sessions (the calendar pages for it, or a reviewed config fills it).
"""
from __future__ import annotations

import re
from typing import Any

from src.adapters.base import MarketSessions
from src.adapters.sessions import (
    DAY_S,
    FX_DEFAULT_NOTE,
    ROLL_BREAK_MAX_S,
    SUNDAY_FIRST,
    Interval,
    fx_default_sessions,
    looks_like_fx_pair,
    subtract,
    weekly_schedule,
)
from src.adapters.stonex.instruments import FX_UNDERLYING_TYPE_ID
from src.adapters.stonex.wire import JsonObj, get_ci, get_list, parse_broker_time_ms, to_int

SOURCE = "stonex"
_TIME_OF_DAY = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?$")
_WCF_DATE = re.compile(r"/Date\((-?\d+)(?:[+-]\d{4})?\)/")

BROKER_NOTE = "StoneX MarketPricingTimes in UTC (to confirm against the live API); StoneX has no holiday source"
DEFAULT_NOTE_SUFFIX = "; StoneX market information has no trading hours for this market"


def _day(raw: object) -> int | None:
    if isinstance(raw, str) and raw.strip()[:3].upper() in SUNDAY_FIRST:
        return SUNDAY_FIRST.index(raw.strip()[:3].upper())
    value = to_int(raw)
    return value if value is not None and 0 <= value <= 6 else None


def _time(raw: object) -> tuple[int, int | None] | None:
    """(seconds of the UTC day, Sunday-first weekday when the value is a full date-time)."""
    if isinstance(raw, dict):            # ApiDateTimeOffsetDTO-shaped {UtcDateTime, OffsetMinutes}
        raw = get_ci(raw, "UtcDateTime", "DateTime")
    if raw is None or isinstance(raw, (bool, dict, list)):
        return None
    m = _TIME_OF_DAY.match(str(raw).strip())
    if m:
        h, mi, s = int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)
        if h > 24 or mi > 59 or s > 59 or (h == 24 and (mi or s)):
            return None
        return h * 3_600 + mi * 60 + s, None
    wcf = _WCF_DATE.search(str(raw).replace("\\", ""))
    # A time of day sent as a WCF date on 1970-01-01: /Date(0)/ is midnight, which
    # parse_broker_time_ms refuses as "no value" (it never invents a timestamp).
    ms = int(wcf.group(1)) if wcf else parse_broker_time_ms(raw)
    if ms is None:
        return None
    day_index = ms // 86_400_000
    return (ms % 86_400_000) // 1000, (day_index + 4) % 7   # 1970-01-01 was a Thursday


def _intervals(rows: list[JsonObj]) -> list[Interval] | None:
    out: list[Interval] = []
    for row in rows:
        start = _time(get_ci(row, "StartTimeUtc", "StartTime", "Start"))
        end = _time(get_ci(row, "EndTimeUtc", "EndTime", "End"))
        if start is None or end is None:
            return None
        day = _day(get_ci(row, "DayOfWeek", "Day"))
        if day is None:
            day = start[1]
        if day is None:
            return None
        s, e = day * DAY_S + start[0], day * DAY_S + end[0]
        if e <= s:
            e += DAY_S
        out.append((s, e))
    return out


def broker_schedule(info: JsonObj, roll_break_max_s: int = ROLL_BREAK_MAX_S) -> dict[str, Any] | None:
    """MarketPricingTimes minus MarketBreakTimes as a weekly UTC schedule; None when absent or unreadable."""
    pricing = _intervals(get_list(info, "MarketPricingTimes", "PricingTimes"))
    if not pricing:
        return None
    breaks = _intervals(get_list(info, "MarketBreakTimes", "BreakTimes"))
    if breaks is None:
        return None
    return weekly_schedule(subtract(pricing, breaks), roll_break_max_s=roll_break_max_s)


def is_fx_market(symbol: str, info: JsonObj | None) -> bool:
    """A currency pair: six letters, neither leg a metal, and not a non-FX underlying type."""
    if not looks_like_fx_pair(symbol):
        return False
    underlying = to_int(get_ci(info, "MarketUnderlyingTypeId", "MarketUnderlyingType")) if info else None
    return underlying in (None, FX_UNDERLYING_TYPE_ID)


def sessions_from_market_info(symbol: str, info: JsonObj | None, *, adapter_id: str) -> MarketSessions | None:
    """Broker hours when readable, else the flagged FX default for an FX pair, else None."""
    schedule = broker_schedule(info) if info is not None else None
    if schedule is not None:
        return MarketSessions(adapter_id=adapter_id, symbol=symbol, timezone="UTC", schedule=schedule,
                              holidays=[], source=SOURCE, is_default=False, holidays_known=False, note=BROKER_NOTE)
    if is_fx_market(symbol, info):
        return fx_default_sessions(adapter_id, symbol, source=SOURCE, note=FX_DEFAULT_NOTE + DEFAULT_NOTE_SUFFIX)
    return None
