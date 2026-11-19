"""StoneX price-bar answers -> ``BrokerBar`` (design PR3): pure decoding for ``StoneXAdapter.fetch_price_bars``.

The read-only endpoints were confirmed by the PR3 probe (live_readonly, 2026-11-17; plan S39): ``GET
market/{marketId}/barhistory`` (count form: ``interval``, ``span``, ``PriceBars``, ``priceType``) and ``GET
market/{marketId}/barhistorybetween`` (range form: ``fromTimeStampUTC`` / ``toTimeStampUTC`` in seconds), 1m and 15m,
bid and ask. The answer is ``{"PriceBars": [{"BarDate", "Open", "High", "Low", "Close", "Volume"}],
"PartialPriceBar": {...} | null}``. ``BarDate`` is the bucket **start** (every probe vote; the forming bar is dated on
the grid). The bars carry **no side tag**: ``BrokerBar.side`` stays None and the side is the request's
(``PriceBarsResponse.side``); a tag, should the broker ever send one, is still read as evidence for the §A4 side
check. ``Volume`` is not read. The broker's cap on a range answer is unconfirmed (no probe answer was truncated).

Errors name fields, never values: a bar is market data and never reaches a log line.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.adapters.base import PriceBarsDecodeError
from src.adapters.stonex.wire import get_ci, parse_broker_time_ms, to_decimal
from src.dto import BrokerBar

#: timeframe_s -> (CIAPI ``interval``, ``span``). Only the specs the design names (1m, 5m, 15m, 1h).
INTERVALS: dict[int, tuple[str, int]] = {60: ("MINUTE", 1), 300: ("MINUTE", 5), 900: ("MINUTE", 15),
                                         3600: ("HOUR", 1)}
#: Bars at or above which a range answer is treated as capped by the broker (truncated). Unconfirmed: PR3 saw
#: no truncated range answer.
RANGE_CAP = 4000
_OHLC = ("Open", "High", "Low", "Close")
_SIDE_FIELDS = ("PriceType", "PriceTypeId", "Side")


def interval_for(timeframe_s: int) -> tuple[str, int] | None:
    return INTERVALS.get(timeframe_s)


def _side_tag(row: Mapping[str, Any], body: Mapping[str, Any]) -> str | None:
    value = get_ci(row, *_SIDE_FIELDS)
    if value is None:
        value = get_ci(body, *_SIDE_FIELDS)
    return None if value is None or value == "" else str(value).strip().upper()


def _bar(row: object, body: Mapping[str, Any], *, partial: bool, where: str) -> BrokerBar:
    if not isinstance(row, Mapping):
        raise PriceBarsDecodeError(f"stonex bars: {where} is not an object")
    start = parse_broker_time_ms(get_ci(row, "BarDate"))
    if start is None:
        raise PriceBarsDecodeError(f"stonex bars: {where} without a readable BarDate")
    values = [to_decimal(get_ci(row, name)) for name in _OHLC]
    missing = [name for name, v in zip(_OHLC, values, strict=True) if v is None]
    if missing:
        raise PriceBarsDecodeError(f"stonex bars: {where} without a readable {', '.join(missing)}")
    o, h, low, c = (v for v in values if v is not None)
    return BrokerBar(bucket_start_ms=start, open=o, high=h, low=low, close=c, side=_side_tag(row, body),
                     partial=partial)


def field_names(body: Mapping[str, Any]) -> tuple[str, ...]:
    """The names (never the values) of the answer's fields: top-level keys, and ``bar.<key>`` for the keys of its
    bar objects (complete and partial)."""
    names = {str(k) for k in body}
    rows: list[object] = list(get_ci(body, "PriceBars") or [])
    rows.append(get_ci(body, "PartialPriceBar"))
    for row in rows:
        if isinstance(row, Mapping):
            names.update(f"bar.{k}" for k in row)
    return tuple(sorted(names))


def decode_bar_history(body: object) -> tuple[list[BrokerBar], tuple[str, ...]]:
    """A barhistory / barhistorybetween answer -> (bars oldest first, the partial bar last when returned; the
    answer's field names). Raises ``PriceBarsDecodeError`` for an answer that is not the documented shape."""
    if not isinstance(body, Mapping):
        raise PriceBarsDecodeError("stonex bars: the answer is not an object")
    rows = get_ci(body, "PriceBars")
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise PriceBarsDecodeError("stonex bars: PriceBars is not a list")
    bars = [_bar(row, body, partial=False, where=f"PriceBars[{i}]") for i, row in enumerate(rows)]
    bars.sort(key=lambda b: b.bucket_start_ms)
    partial = get_ci(body, "PartialPriceBar")
    if partial is not None:
        bars.append(_bar(partial, body, partial=True, where="PartialPriceBar"))
    return bars, field_names(body)
