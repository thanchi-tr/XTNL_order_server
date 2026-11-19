"""Broker price bars as an adapter returns them (design PR3, §A4): transport types only.

``BrokerBar`` and ``PriceBarsResponse`` carry one ``fetch_price_bars`` answer from an adapter to its caller (the
market-data layer's ``md-bars`` from S17, the PR3 probe). They are never persisted: no PostgreSQL table, payload,
outbox body, log line or page ever holds a bar. The OHLC fields are left out of ``repr`` so an accidental log of a
bar or a response shows its bucket and flags, never a price.

Validation (the §A4 zero-trust checks: prices > 0, high / low bounds, the bucket on the spec grid and inside the
requested range, the side, the bucket end not in the future) belongs to the consumer, not to these types: an
adapter returns what the broker said, including a forming bar (``partial``), and the consumer decides.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

#: The quote side a bar series is built from (the request side; the ARM reads bid bars for longs, ask for shorts).
BarSide = Literal["BID", "ASK"]
BAR_SIDES: frozenset[str] = frozenset({"BID", "ASK"})

#: ``PriceBarsResponse.note`` of an adapter without the capability (the ``BrokerAdapter`` default).
PRICE_BARS_NOT_SUPPORTED = "price bars not supported"


@dataclass(frozen=True, kw_only=True)
class BrokerBar:
    """One bar exactly as the broker returned it.

    ``bucket_start_ms`` is the bucket start in broker time (UTC ms) under the adapter's stated time convention
    (StoneX: ``BarDate`` is the bucket start, PR3 probe 2026-11-17). ``side`` is the side tag the response itself
    states for this bar (e.g. a price-type field), or None when the response names none (StoneX bars name none:
    the side is then the request's, ``PriceBarsResponse.side``); it is evidence for the side check, never the
    requested side copied in. ``partial`` is the broker's own forming / partial flag."""

    bucket_start_ms: int
    open: Decimal = field(repr=False)
    high: Decimal = field(repr=False)
    low: Decimal = field(repr=False)
    close: Decimal = field(repr=False)
    side: str | None = None
    partial: bool = False


@dataclass(frozen=True, kw_only=True)
class PriceBarsResponse:
    """One ``fetch_price_bars`` answer (count form: ``count``; range form: ``from_ms`` / ``to_ms``).

    ``bars`` are oldest first. ``truncated`` is True when the broker capped the answer, so a bucket of the request
    with no bar may still have traded (blind, never ``no_trade``). ``received_ms`` is the local time the answer was
    received. ``supported`` is False for an adapter (or side) without the capability; ``note`` then says why.
    ``field_names`` lists the names (never the values) of the fields the broker answer carried, for the PR3 probe."""

    adapter_id: str
    symbol: str
    side: BarSide
    timeframe_s: int
    bars: tuple[BrokerBar, ...]
    truncated: bool
    received_ms: int
    count: int | None = None
    from_ms: int | None = None
    to_ms: int | None = None
    supported: bool = True
    note: str = ""
    field_names: tuple[str, ...] = ()

    @classmethod
    def unsupported(cls, adapter_id: str, symbol: str, side: BarSide, timeframe_s: int, *, received_ms: int = 0,
                    count: int | None = None, from_ms: int | None = None, to_ms: int | None = None,
                    note: str = PRICE_BARS_NOT_SUPPORTED) -> PriceBarsResponse:
        """An empty answer of an adapter without the capability: covers nothing (fail closed)."""
        return cls(adapter_id=adapter_id, symbol=symbol, side=side, timeframe_s=timeframe_s, bars=(),
                   truncated=False, received_ms=received_ms, count=count, from_ms=from_ms, to_ms=to_ms,
                   supported=False, note=note)


def bar_request_problem(side: str, timeframe_s: int, count: int | None, from_ms: int | None,
                        to_ms: int | None) -> str | None:
    """Why a ``fetch_price_bars`` request is malformed, or None. Exactly one form: ``count`` (> 0) alone, or both
    ``from_ms`` and ``to_ms`` with ``from_ms <= to_ms`` (R11-M15)."""
    if side not in BAR_SIDES:
        return f"side must be one of {sorted(BAR_SIDES)}"
    if timeframe_s <= 0:
        return "timeframe_s must be positive"
    ranged = from_ms is not None or to_ms is not None
    if count is not None and ranged:
        return "give either count or from_ms / to_ms, not both"
    if count is not None:
        return None if count > 0 else "count must be positive"
    if from_ms is None or to_ms is None:
        return "give count, or both from_ms and to_ms"
    return None if from_ms <= to_ms else "from_ms must not be after to_ms"
