"""Zero-trust bar and quote checks (plan S12; design §A4 Validation, R11-M15). Synthetic instruments only."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.dto.bars import BrokerBar, PriceBarsResponse
from src.marketdata.hashing import HashStatus, Keyring, content_hash
from src.marketdata.types import M1, M15, Bar, SeriesId, Side, Source
from src.marketdata.validate import (
    Check,
    check_broker_bar,
    check_quote,
    check_stored,
    check_tick_bar,
    grid_ok,
    is_forming,
    prices_ok,
    split_response,
)

D = Decimal
SERIES = SeriesId("mock", "SYN1", M15, Side.BID, Source.BROKER)
T0 = 1_700_000_100_000
RECEIVED = T0 + 900_000 + 2_000   # bucket T0 has closed 2 s before receipt
KEY = bytes(range(32))
OTHER_KEY = bytes(range(32, 64))


def bb(start: int = T0, o: str = "1.1", h: str = "1.2", low: str = "1.0", c: str = "1.15",
       side: str | None = None, partial: bool = False) -> BrokerBar:
    return BrokerBar(bucket_start_ms=start, open=D(o), high=D(h), low=D(low), close=D(c), side=side, partial=partial)


# ---------------------------------------------------------------------------------------------------- check table
@pytest.mark.parametrize(("bar", "kw", "failed"), [
    (bb(), {}, ()),
    (bb(side="BID"), {}, ()),
    (bb(side="bid"), {}, ()),
    (bb(o="0"), {}, (Check.PRICES,)),
    (bb(low="-1"), {}, (Check.PRICES,)),
    (bb(h="1.14"), {}, (Check.PRICES,)),                     # high below the close
    (bb(low="1.12"), {}, (Check.PRICES,)),                   # low above the open
    (bb(c="NaN"), {}, (Check.PRICES,)),
    (bb(h="sNaN"), {}, (Check.PRICES,)),
    (bb(h="Infinity"), {}, (Check.PRICES,)),
    (bb(start=T0 - 840_000), {}, (Check.GRID_RANGE,)),       # off the 15m grid
    (bb(), {"from_ms": T0 + 900_000, "to_ms": T0 + 1_800_000}, (Check.GRID_RANGE,)),   # not requested
    (bb(), {"from_ms": T0 - 900_000, "to_ms": T0 - 900_000}, (Check.GRID_RANGE,)),
    (bb(), {"from_ms": T0, "to_ms": T0}, ()),
    (bb(side="ASK"), {}, (Check.SIDE,)),
    (bb(start=T0 + 900_000), {}, (Check.FUTURE,)),           # ends 898 s after receipt
    (bb(start=T0 - 840_000, o="0", side="ASK"), {}, (Check.PRICES, Check.GRID_RANGE, Check.SIDE)),
])
def test_broker_bar_checks(bar: BrokerBar, kw: dict[str, int], failed: tuple[Check, ...]) -> None:
    result = check_broker_bar(bar, SERIES, received_ms=RECEIVED, **kw)
    assert result.failed == failed
    assert result.ok is (not failed)


def test_future_check_has_the_five_second_tolerance() -> None:
    end = T0 + 900_000
    assert check_broker_bar(bb(), SERIES, received_ms=end - 5_000).ok
    assert check_broker_bar(bb(), SERIES, received_ms=end - 5_001).failed == (Check.FUTURE,)


def test_checks_never_raise_on_signalling_nan() -> None:
    assert prices_ok(D("sNaN"), D("1"), D("1"), D("1")) is False
    assert check_quote(D("sNaN"), D("1"), T0, T0).failed == (Check.QUOTE_PRICES,)
    assert check_quote(D("1"), D("sNaN"), T0, T0).failed == (Check.QUOTE_PRICES,)


def test_grid_ok_without_range() -> None:
    assert grid_ok(M15, T0) and not grid_ok(M15, T0 + 1)
    assert grid_ok(M1, T0 + 60_000)


# ---------------------------------------------------------------------------------------------------- responses
def response(*bars: BrokerBar, received: int = RECEIVED, side: str = "BID", timeframe_s: int = 900,
             from_ms: int | None = None, to_ms: int | None = None, adapter_id: str = "mock",
             symbol: str = "SYN1") -> PriceBarsResponse:
    count = None if from_ms is not None else len(bars)
    return PriceBarsResponse(adapter_id=adapter_id, symbol=symbol, side=side,  # type: ignore[arg-type]
                             timeframe_s=timeframe_s, bars=bars, truncated=False, received_ms=received, count=count,
                             from_ms=from_ms, to_ms=to_ms)


def test_forming_bar_is_evidence_only_never_validated_stored_or_counted() -> None:
    closed_prev, closed, forming = bb(T0 - 900_000), bb(T0), bb(T0 + 900_000, o="0", partial=True)
    split = split_response(response(closed_prev, closed, forming), SERIES)
    assert split.valid == (closed_prev, closed)
    assert split.invalid == ()          # the corrupt forming bar is not a check failure
    assert split.forming == (forming,)
    assert is_forming(M15, T0 + 900_000, RECEIVED) and not is_forming(M15, T0, RECEIVED)


def test_future_dated_bars_are_check_4_failures_not_forming_evidence() -> None:
    forming = bb(T0 + 900_000)                      # the bucket already started at receipt
    next_bucket = bb(T0 + 2 * 900_000)              # starts 898 s after receipt
    a_day_ahead = bb(T0 + 96 * 900_000)
    split = split_response(response(bb(T0), forming, next_bucket, a_day_ahead), SERIES)
    assert split.valid == (bb(T0),)
    assert split.forming == (forming,)
    assert [(b, r.failed) for b, r in split.invalid] == [(next_bucket, (Check.FUTURE,)),
                                                          (a_day_ahead, (Check.FUTURE,))]
    assert not is_forming(M15, T0 + 2 * 900_000, RECEIVED)
    # The bucket starting within the 5 s tolerance of receipt is still the forming bar.
    assert is_forming(M15, T0 + 900_000, T0 + 900_000 - 5_000)
    assert not is_forming(M15, T0 + 900_000, T0 + 900_000 - 5_001)


def test_split_response_quarantines_corrupt_closed_bars() -> None:
    bad = bb(T0 - 900_000, h="0.5")
    split = split_response(response(bad, bb(T0)), SERIES)
    assert [b for b, _ in split.invalid] == [bad]
    assert split.invalid[0][1].failed == (Check.PRICES,)
    assert split.valid == (bb(T0),)


def test_split_response_range_form_bounds_the_bucket() -> None:
    outside = bb(T0 - 1_800_000)
    split = split_response(response(outside, bb(T0 - 900_000), bb(T0), from_ms=T0 - 900_000, to_ms=T0), SERIES)
    assert [b for b, _ in split.invalid] == [outside]
    assert split.invalid[0][1].failed == (Check.GRID_RANGE,)
    assert len(split.valid) == 2


def test_split_response_for_another_side_or_timeframe_fails_every_closed_bar() -> None:
    wrong_side = split_response(response(bb(T0), side="ASK"), SERIES)
    assert wrong_side.valid == () and wrong_side.invalid[0][1].failed == (Check.SIDE,)
    wrong_tf = split_response(response(bb(T0), timeframe_s=60), SERIES)
    assert wrong_tf.valid == () and wrong_tf.invalid[0][1].failed == (Check.GRID_RANGE,)


@pytest.mark.parametrize(("adapter_id", "symbol"), [("other", "SYN1"), ("mock", "OTHER"), ("other", "OTHER")])
def test_split_response_for_another_instrument_fails_every_closed_bar(adapter_id: str, symbol: str) -> None:
    split = split_response(response(bb(T0 - 900_000), bb(T0), adapter_id=adapter_id, symbol=symbol), SERIES)
    assert split.valid == ()
    assert [r.failed for _, r in split.invalid] == [(Check.INSTRUMENT,)] * 2


# ---------------------------------------------------------------------------------------------------- tick and stored bars
TICK = SeriesId("mock", "SYN1", M1, Side.ASK, Source.TICK)


def tick_bar(start: int, series: SeriesId = TICK, h: str = "1.2") -> Bar:
    return Bar(series=series, bucket_start_ms=start, open=D("1.1"), high=D(h), low=D("1.0"), close=D("1.15"),
               tick_count=12)


def test_tick_bar_checks() -> None:
    start = T0 + 60_000
    received = start + 61_000
    assert check_tick_bar(tick_bar(start), built_bucket_start_ms=start, built_side=Side.ASK, received_ms=received).ok
    assert check_tick_bar(tick_bar(start), built_bucket_start_ms=start + 60_000, built_side=Side.ASK,
                          received_ms=received).failed == (Check.GRID_RANGE,)
    assert check_tick_bar(tick_bar(start), built_bucket_start_ms=start, built_side=Side.BID,
                          received_ms=received).failed == (Check.SIDE,)
    assert check_tick_bar(tick_bar(start, h="1"), built_bucket_start_ms=start, built_side=Side.ASK,
                          received_ms=start).failed == (Check.PRICES, Check.FUTURE)


def stored(bar: Bar, ring: Keyring) -> str:
    return content_hash(ring, bar.series.canonical, bar.bucket_end_ms, bar.open, bar.high, bar.low, bar.close)


def test_stored_bar_hash_check() -> None:
    ring = Keyring.of(1, KEY)
    bar = tick_bar(T0, SERIES)
    good = check_stored(bar, stored(bar, ring), ring, now_ms=RECEIVED)
    assert good.usable and good.hash_status is HashStatus.VALID
    tampered = Bar(series=SERIES, bucket_start_ms=T0, open=D("1.1"), high=D("1.2"), low=D("1.0"), close=D("1.16"))
    bad = check_stored(tampered, stored(bar, ring), ring, now_ms=RECEIVED)
    assert bad.result.failed == (Check.CONTENT_HASH,) and bad.hash_status is HashStatus.MISMATCH


def test_stored_bar_under_an_unheld_key_is_missing_not_a_failure() -> None:
    bar = tick_bar(T0, SERIES)
    old = stored(bar, Keyring.of(1, OTHER_KEY))
    result = check_stored(bar, old, Keyring.of(2, KEY), now_ms=RECEIVED)
    assert result.result.ok and result.hash_status is HashStatus.MISSING and not result.usable


def test_check_stored_never_raises_on_a_huge_finite_price() -> None:
    ring = Keyring.of(1, KEY)
    bar = tick_bar(T0, SERIES)
    huge = Bar(series=SERIES, bucket_start_ms=T0, open=D("1.1"), high=D("1E+1000000"), low=D("1.0"), close=D("1.15"))
    result = check_stored(huge, stored(bar, ring), ring, now_ms=RECEIVED)
    assert result.hash_status is HashStatus.MISMATCH and Check.CONTENT_HASH in result.result.failed


# ---------------------------------------------------------------------------------------------------- quotes
@pytest.mark.parametrize(("bid", "ask", "broker_ms", "failed"), [
    ("1.1", "1.2", T0, ()),
    ("1.1", "1.1", T0, ()),
    ("0", "1.2", T0, (Check.QUOTE_PRICES,)),
    ("1.2", "1.1", T0, (Check.QUOTE_PRICES,)),
    ("1.1", "Infinity", T0, (Check.QUOTE_PRICES,)),
    ("1.1", "1.2", T0 + 5_001, (Check.QUOTE_FUTURE,)),
    ("1.1", "1.2", T0 + 5_000, ()),
])
def test_quote_checks(bid: str, ask: str, broker_ms: int, failed: tuple[Check, ...]) -> None:
    assert check_quote(D(bid), D(ask), broker_ms, T0).failed == failed
