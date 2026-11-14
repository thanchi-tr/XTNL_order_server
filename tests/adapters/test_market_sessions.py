"""Item 5.1 market sessions: fetch_market_sessions() on every adapter and the pure converters.

Every result must parse with src/calendar/schedule.py:symbol_schedule_from_json (the market_sessions
row format) and give the expected SM10 phases. No network I/O.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.adapters.base import BrokerAdapter, MarketSessions, TransientBrokerError, assert_read_only
from src.adapters.mock import MockBrokerAdapter, build_fx_fixture, eurusd_spec
from src.adapters.pepperstone.adapter import PepperstoneAdapter
from src.adapters.pepperstone.protocol import OUTBOUND_ALLOW_LIST, PT_SYMBOL_BY_ID_REQ
from src.adapters.pepperstone.schedule import holidays_from_symbol, sessions_from_symbol
from src.adapters.sessions import (
    DAY_S,
    FX_DEFAULT_TIMEZONE,
    WEEK_S,
    fx_default_sessions,
    normalise,
    subtract,
    weekly_schedule,
)
from src.adapters.stonex.adapter import StoneXAdapter
from src.adapters.stonex.sessions import broker_schedule, is_fx_market, sessions_from_market_info
from src.adapters.transport import is_denied_path
from src.calendar.schedule import SymbolSchedule, symbol_schedule_from_json
from src.calendar.sessions import Phase, phase_at

from . import conftest as sx
from .pepperstone import conftest as ct

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
NOW_MS = int(datetime(2026, 11, 13, 12, 0, tzinfo=UTC).timestamp() * 1000)   # a Friday

# cTrader-style FX week in UTC: Sun 22:00 -> Fri 21:00 with a 21:00-21:05 roll Mon-Thu.
FX_INTERVALS = [(79_200, DAY_S + 75_600)] + [(d * DAY_S + 75_900, (d + 1) * DAY_S + 75_600) for d in range(1, 5)]
FX_EXPECTED = {
    "sessions": [{"open": {"day": "SUN", "time": "22:00"}, "close": {"day": "FRI", "time": "21:00"}}],
    "breaks": [{"start": "21:00", "end": "21:05", "days": ["MON", "TUE", "WED", "THU"]}],
}


def ms(tz: ZoneInfo, *args: int) -> int:
    return int(datetime(*args, tzinfo=tz).timestamp() * 1000)   # type: ignore[misc]


def epoch_days(d: date) -> int:
    return (d - date(1970, 1, 1)).days


def parsed(m: MarketSessions) -> SymbolSchedule:
    return symbol_schedule_from_json(m.adapter_id, m.symbol, m.timezone, m.schedule, m.holidays)


# --- pure weekly conversion --------------------------------------------------------------------------
def test_weekly_schedule_turns_short_gaps_into_roll_breaks_and_the_weekend_into_a_close() -> None:
    schedule = weekly_schedule(FX_INTERVALS)
    assert schedule == FX_EXPECTED
    s = symbol_schedule_from_json("pepperstone", "EURUSD", "UTC", schedule or {})
    assert phase_at(s, ms(UTC, 2026, 11, 16, 12, 0)) is Phase.OPEN          # Monday
    assert phase_at(s, ms(UTC, 2026, 11, 17, 21, 2)) is Phase.ROLLOVER      # Tuesday roll
    assert phase_at(s, ms(UTC, 2026, 11, 20, 21, 2)) is Phase.CLOSED        # Friday after the close
    assert phase_at(s, ms(UTC, 2026, 11, 21, 12, 0)) is Phase.CLOSED        # Saturday
    assert phase_at(s, ms(UTC, 2026, 11, 22, 22, 0)) is Phase.OPEN          # Sunday open


def test_full_week_is_one_24x7_session_with_its_breaks() -> None:
    assert weekly_schedule([(0, WEEK_S)]) == {
        "sessions": [{"open": {"day": "SUN", "time": "00:00"}, "close": {"day": "SUN", "time": "00:00"}}]}
    daily = [(d * DAY_S, d * DAY_S + 86_100) for d in range(7)]          # 23:55-24:00 break every day
    schedule = weekly_schedule(daily)
    assert schedule is not None and schedule["sessions"][0]["open"] == schedule["sessions"][0]["close"]
    assert schedule["breaks"] == [{"start": "23:55", "end": "00:00",
                                   "days": ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]}]
    s = symbol_schedule_from_json("pepperstone", "BTCUSD", "UTC", schedule)
    assert phase_at(s, ms(UTC, 2026, 11, 21, 12, 0)) is Phase.OPEN
    assert phase_at(s, ms(UTC, 2026, 11, 21, 23, 57)) is Phase.ROLLOVER


def test_intervals_wrapping_the_week_and_cut_out_breaks() -> None:
    assert normalise([(6 * DAY_S + 79_200, 7 * DAY_S + 3_600)]) == [(0, 3_600), (6 * DAY_S + 79_200, WEEK_S)]
    schedule = weekly_schedule([(6 * DAY_S + 79_200, 7 * DAY_S + 3_600)])
    assert schedule == {"sessions": [{"open": {"day": "SAT", "time": "22:00"},
                                      "close": {"day": "SUN", "time": "01:00"}}]}
    assert subtract([(0, 100)], [(10, 20), (90, 200)]) == [(0, 10), (20, 90)]
    assert weekly_schedule([]) is None and weekly_schedule([(5, 5)]) is None


def test_two_weekly_sessions_split_by_long_gaps() -> None:
    schedule = weekly_schedule([(DAY_S + 3_600, DAY_S + 7_200), (3 * DAY_S, 3 * DAY_S + 30)])
    assert schedule is not None
    assert schedule["sessions"] == [
        {"open": {"day": "MON", "time": "01:00"}, "close": {"day": "MON", "time": "02:00"}},
        {"open": {"day": "WED", "time": "00:00"}, "close": {"day": "WED", "time": "00:00:30"}},
    ]
    symbol_schedule_from_json("x", "Y", "UTC", schedule)


# --- cTrader -------------------------------------------------------------------------------------------
def ctrader_symbol(**extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"symbolId": 1, "scheduleTimeZone": "UTC",
                           "schedule": [{"startSecond": s, "endSecond": e} for s, e in FX_INTERVALS]}
    row.update(extra)
    return row


def test_ctrader_holidays_in_the_symbol_zone_recurring_and_trimmed() -> None:
    full = {"holiday": [
        {"name": "Christmas", "holidayDate": epoch_days(date(2026, 12, 25))},
        {"name": "New Year", "holidayDate": epoch_days(date(2020, 1, 1)), "isRecurring": True},
        {"name": "Early close", "holidayDate": epoch_days(date(2026, 12, 24)), "scheduleTimeZone": "UTC",
         "startSecond": 18 * 3_600, "endSecond": 22 * 3_600},
        {"name": "Old", "holidayDate": epoch_days(date(2025, 12, 25))},
        {"name": "Empty", "holidayDate": epoch_days(date(2026, 12, 26)), "startSecond": 10, "endSecond": 10},
    ]}
    assert holidays_from_symbol(full, "America/New_York", NOW_MS) == [
        {"start": "2026-12-24T13:00:00", "end": "2026-12-24T17:00:00", "reason": "Early close"},
        {"date": "2026-12-25", "reason": "Christmas"},
        {"date": "2027-01-01", "reason": "New Year"},
    ]


def test_ctrader_symbol_to_market_sessions_parses_and_closes_on_holidays() -> None:
    full = ctrader_symbol(scheduleTimeZone="America/New_York",
                          holiday=[{"name": "Thanksgiving", "holidayDate": epoch_days(date(2026, 11, 26))}])
    m = sessions_from_symbol(full, adapter_id="pepperstone", symbol="EURUSD", now_ms=NOW_MS)
    assert m is not None
    assert (m.timezone, m.source, m.is_default, m.holidays_known) == ("America/New_York", "ctrader", False, True)
    s = parsed(m)
    assert phase_at(s, ms(NY, 2026, 11, 26, 12, 0)) is Phase.CLOSED     # holiday Thursday
    assert phase_at(s, ms(NY, 2026, 11, 25, 12, 0)) is Phase.OPEN


def test_ctrader_symbol_zone_and_schedule_validation() -> None:
    assert sessions_from_symbol(ctrader_symbol(scheduleTimeZone="Mars/Olympus"), adapter_id="p", symbol="X",
                                now_ms=NOW_MS) is None
    assert sessions_from_symbol(ctrader_symbol(schedule=[]), adapter_id="p", symbol="X", now_ms=NOW_MS) is None
    no_tz = ctrader_symbol()
    del no_tz["scheduleTimeZone"]
    m = sessions_from_symbol(no_tz, adapter_id="p", symbol="X", now_ms=NOW_MS)
    assert m is not None and m.timezone == "UTC" and "UTC assumed" in m.note


async def test_pepperstone_fetch_market_sessions_uses_only_allow_listed_symbol_by_id() -> None:
    fake = ct.FakeCTrader()
    fake.full[ct.EURUSD].update(ctrader_symbol(
        symbolId=ct.EURUSD, holiday=[{"name": "Christmas", "holidayDate": epoch_days(date(2026, 12, 25))}]))
    adapter = ct.make_adapter(fake, wall_ms=lambda: NOW_MS)
    await adapter.connect()
    try:
        before = len(fake.sent)
        result = await adapter.fetch_market_sessions(["EUR/USD", "USDJPY", "NOPE"])
        sent = fake.types_sent()[before:]
    finally:
        await adapter.close()
    assert set(result) == {"EURUSD"}                                     # USDJPY: no schedule; NOPE unknown
    m = result["EURUSD"]
    assert m.schedule == FX_EXPECTED and m.holidays == [{"date": "2026-12-25", "reason": "Christmas"}]
    assert PT_SYMBOL_BY_ID_REQ in sent and all(pt in OUTBOUND_ALLOW_LIST for pt in sent)
    parsed(m)


# --- StoneX --------------------------------------------------------------------------------------------
STONEX_PRICING = (
    [{"DayOfWeek": 0, "StartTimeUtc": "22:00", "EndTimeUtc": "00:00"}]
    + [{"DayOfWeek": d, "StartTimeUtc": "00:00:00", "EndTimeUtc": "00:00:00"} for d in range(1, 5)]
    + [{"DayOfWeek": "Friday", "StartTimeUtc": "00:00", "EndTimeUtc": "21:00"}]
)
STONEX_BREAKS = [{"DayOfWeek": d, "StartTimeUtc": "21:00", "EndTimeUtc": "21:05"} for d in range(1, 5)]


def test_stonex_pricing_times_minus_break_times() -> None:
    assert broker_schedule({"MarketPricingTimes": STONEX_PRICING, "MarketBreakTimes": STONEX_BREAKS}) == FX_EXPECTED
    # WCF date-times give their UTC time of day (and weekday when DayOfWeek is absent)
    monday_9 = ms(UTC, 2026, 11, 16, 9, 0)
    rows = [{"StartTimeUtc": f"/Date({monday_9})/", "EndTimeUtc": f"/Date({monday_9 + 3_600_000})/"}]
    assert broker_schedule({"MarketPricingTimes": rows}) == {
        "sessions": [{"open": {"day": "MON", "time": "09:00"}, "close": {"day": "MON", "time": "10:00"}}]}
    # one unreadable row -> no broker hours at all
    assert broker_schedule({"MarketPricingTimes": [*STONEX_PRICING, {"DayOfWeek": 9, "StartTimeUtc": "1:00",
                                                                     "EndTimeUtc": "2:00"}]}) is None
    assert broker_schedule({"MarketPricingTimes": STONEX_PRICING, "MarketBreakTimes": [{"DayOfWeek": 1}]}) is None
    assert broker_schedule({}) is None


def test_stonex_default_only_for_fx_pairs_and_flagged() -> None:
    m = sessions_from_market_info("USDJPY", {"MarketUnderlyingTypeId": 4}, adapter_id="stonex")
    assert m is not None and m.is_default and not m.holidays_known and m.timezone == FX_DEFAULT_TIMEZONE
    assert "default FX hours" in m.note
    s = parsed(m)
    assert phase_at(s, ms(NY, 2026, 11, 16, 12, 0)) is Phase.OPEN
    assert phase_at(s, ms(NY, 2026, 11, 17, 17, 2)) is Phase.ROLLOVER
    assert phase_at(s, ms(NY, 2026, 11, 20, 17, 0)) is Phase.CLOSED
    assert phase_at(s, ms(NY, 2026, 11, 22, 16, 59)) is Phase.CLOSED
    assert phase_at(s, ms(NY, 2026, 11, 22, 17, 0)) is Phase.OPEN
    assert phase_at(s, ms(NY, 2026, 7, 13, 12, 0)) is Phase.OPEN         # summer time follows New York
    assert sessions_from_market_info("UK100", {"MarketUnderlyingTypeId": 2}, adapter_id="stonex") is None
    assert not is_fx_market("XAUUSD", None) and not is_fx_market("EURUSD", {"MarketUnderlyingTypeId": 2})
    assert sessions_from_market_info("EURUSD", None, adapter_id="stonex") is not None


async def test_stonex_fetch_market_sessions_reads_market_information(fake_broker: sx.FakeStoneX) -> None:
    fake_broker.markets[sx.EURUSD_ID] = {**sx.MARKETS[sx.EURUSD_ID], "MarketPricingTimes": STONEX_PRICING,
                                         "MarketBreakTimes": STONEX_BREAKS}
    fake_broker.markets["9999"] = {"MarketId": 9999, "Name": "UK 100 CFD", "PriceDecimalPlaces": 1,
                                   "MarketCurrencyIsoCode": "GBP", "MarketUnderlyingTypeId": 2}
    adapter = sx.make_adapter(fake_broker, symbol_market_ids={"UK100": "9999"})
    await adapter.connect()
    result = await adapter.fetch_market_sessions(["eurusd", "USDJPY", "UK100", "NOPE00"])
    assert set(result) == {"EURUSD", "USDJPY"}
    eur, jpy = result["EURUSD"], result["USDJPY"]
    assert (eur.timezone, eur.schedule, eur.is_default, eur.holidays_known) == ("UTC", FX_EXPECTED, False, False)
    assert jpy.is_default and jpy.source == "stonex"
    parsed(eur), parsed(jpy)
    assert "/v2/market/information" in fake_broker.paths()
    assert not any(is_denied_path(p) for p in fake_broker.paths())


# --- mock and the default ------------------------------------------------------------------------------
async def test_mock_fetch_market_sessions_scripted_default_and_failure() -> None:
    fixture = build_fx_fixture()
    scripted = MarketSessions(adapter_id="mock", symbol="EURUSD", timezone="UTC", schedule=FX_EXPECTED,
                              holidays=[{"date": "2026-12-25"}])
    fixture.market_sessions.append(scripted)
    adapter = MockBrokerAdapter.from_fixture(fixture)
    result = await adapter.fetch_market_sessions(["EURUSD", "AUDUSD", "GBPUSD"])
    assert result["EURUSD"] is scripted and set(result) == {"EURUSD", "AUDUSD"}
    assert result["AUDUSD"].is_default and result["AUDUSD"].source == "mock"
    parsed(result["AUDUSD"]), parsed(scripted)
    assert await MockBrokerAdapter(instruments=[eurusd_spec()], default_fx_sessions=False).fetch_market_sessions(
        ["EURUSD"]) == {}
    adapter.fail_next("fetch_market_sessions", TransientBrokerError("down"))
    with pytest.raises(TransientBrokerError):
        await adapter.fetch_market_sessions(["EURUSD"])


async def test_base_default_has_no_broker_schedule_and_adapters_stay_read_only() -> None:
    assert await BrokerAdapter.fetch_market_sessions(MockBrokerAdapter(), ["EURUSD"]) == {}
    for cls in (StoneXAdapter, PepperstoneAdapter, MockBrokerAdapter):
        assert_read_only(cls)
    m = fx_default_sessions("x", "EURUSD", source="mock")
    assert m.schedule is not fx_default_sessions("x", "EURUSD", source="mock").schedule   # no shared dict


# --- skeptic review regressions ------------------------------------------------------------------------
def test_ctrader_whole_day_holiday_in_another_zone_becomes_a_local_interval() -> None:
    # A whole London day is not the whole New York day: it must be written as a converted interval.
    full = {"holiday": [{"name": "Boxing Day", "holidayDate": epoch_days(date(2026, 12, 28)),
                         "scheduleTimeZone": "Europe/London"}]}
    assert holidays_from_symbol(full, "America/New_York", NOW_MS) == [
        {"start": "2026-12-27T19:00:00", "end": "2026-12-28T19:00:00", "reason": "Boxing Day"}]


def test_stonex_time_values_midnight_wcf_and_datetime_offset_objects() -> None:
    from src.adapters.stonex.sessions import _time
    assert _time("/Date(0)/") == (0, 4)                              # midnight, 1970-01-01 (a Thursday)
    assert _time(r"\/Date(61200000+0000)\/") == (61_200, 4)
    assert _time({"UtcDateTime": "/Date(61200000)/", "OffsetMinutes": 0}) == (61_200, 4)
    assert _time({"UtcDateTime": "2026-11-16T09:00:00Z"}) == (32_400, 1)
    assert _time({"OffsetMinutes": 0}) is None
    rows = [{"DayOfWeek": d, "StartTimeUtc": "/Date(0)/", "EndTimeUtc": "/Date(0)/"} for d in range(7)]
    assert broker_schedule({"MarketPricingTimes": rows}) == {
        "sessions": [{"open": {"day": "SUN", "time": "00:00"}, "close": {"day": "SUN", "time": "00:00"}}]}


async def test_stonex_fetch_market_sessions_keys_canonical_symbols(fake_broker: sx.FakeStoneX) -> None:
    adapter = sx.make_adapter(fake_broker)
    await adapter.connect()
    result = await adapter.fetch_market_sessions(["EUR/USD", "usd-jpy", ""])
    assert set(result) == {"EURUSD", "USDJPY"} and all(m.symbol == k for k, m in result.items())


async def test_mock_fx_default_never_applies_to_non_fx_instruments() -> None:
    from src.adapters.mock import fx_spec
    from src.adapters.sessions import looks_like_fx_pair
    gold, index = fx_spec("XAUUSD"), fx_spec("US500")
    adapter = MockBrokerAdapter(instruments=[eurusd_spec(), gold, index])
    assert set(await adapter.fetch_market_sessions(["EURUSD", "XAUUSD", "US500"])) == {"EURUSD"}
    assert looks_like_fx_pair("eur/usd") and not looks_like_fx_pair("USDXAG") and not looks_like_fx_pair("UK100")
