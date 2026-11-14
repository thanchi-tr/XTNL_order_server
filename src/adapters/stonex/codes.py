"""StoneX / GAIN Capital wire vocabulary (docs.labs.gaincapital.com, APICodesList).

The wire carries numeric codes as strings ("2", "9"); REST sometimes uses words
("Buy", "Stop"). Every parser accepts both and returns None when unrecognised.
Canonical status mapping: adapter-isolation.md §2 (StoneX).
"""
from __future__ import annotations

from enum import IntEnum

from src.dto import OrderStatus, Side


class StoneXOrderStatus(IntEnum):
    PENDING = 1        # bracket attached to an order that is not live yet
    ACCEPTED = 2       # working
    OPEN = 3           # filled: the trade is an open position
    CANCELLED = 4
    REJECTED = 5
    SUSPENDED = 6      # OCO sibling parked
    YELLOW_CARD = 8    # awaiting manual approval
    CLOSED = 9         # position closed
    RED_CARD = 10      # awaiting approval (risk)
    TRIGGERED = 11     # level crossed, fill in flight


class StoneXOrderType(IntEnum):
    TRADE = 1
    STOP = 2
    LIMIT = 3


class PriceStatus(IntEnum):
    NORMAL = 0
    INDICATIVE = 1
    PHONE_ONLY = 2
    SUSPENDED = 3
    CLOSED = 4


CANONICAL_STATUS: dict[StoneXOrderStatus, OrderStatus] = {
    StoneXOrderStatus.PENDING: OrderStatus.PENDING,
    StoneXOrderStatus.ACCEPTED: OrderStatus.WORKING,
    StoneXOrderStatus.OPEN: OrderStatus.FILLED,
    StoneXOrderStatus.CANCELLED: OrderStatus.CANCELLED,
    StoneXOrderStatus.REJECTED: OrderStatus.REJECTED,
    StoneXOrderStatus.SUSPENDED: OrderStatus.WORKING,
    StoneXOrderStatus.YELLOW_CARD: OrderStatus.PENDING,
    StoneXOrderStatus.CLOSED: OrderStatus.FILLED,
    StoneXOrderStatus.RED_CARD: OrderStatus.PENDING,
    StoneXOrderStatus.TRIGGERED: OrderStatus.WORKING,
}

CHILD_TYPES = frozenset({StoneXOrderType.STOP, StoneXOrderType.LIMIT})


def _clean(raw: object) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text.upper().replace(" ", "_") if text else None


def _code(text: str) -> int | None:
    try:
        number = float(text)
    except ValueError:
        return None
    return int(number) if number == int(number) else None


def parse_status(raw: object) -> StoneXOrderStatus | None:
    text = _clean(raw)
    if text is None:
        return None
    code = _code(text)
    if code is not None:
        try:
            return StoneXOrderStatus(code)
        except ValueError:
            return None
    return StoneXOrderStatus.__members__.get(text)


def parse_order_type(raw: object) -> StoneXOrderType | None:
    text = _clean(raw)
    if text is None:
        return None
    code = _code(text)
    if code is not None:
        try:
            return StoneXOrderType(code)
        except ValueError:
            return None
    return StoneXOrderType.__members__.get(text)


def parse_side(raw: object) -> Side | None:
    """Streams send 1 = buy, 0 = sell; REST sends "Buy" / "Sell"."""
    text = _clean(raw)
    if text is None:
        return None
    if text in ("BUY", "B", "1"):
        return "BUY"
    if text in ("SELL", "S", "0"):
        return "SELL"
    code = _code(text)
    if code == 1:
        return "BUY"
    if code == 0:
        return "SELL"
    return None


def opposite(side: Side) -> Side:
    return "SELL" if side == "BUY" else "BUY"


def canonical_status(code: StoneXOrderStatus) -> OrderStatus:
    return CANONICAL_STATUS[code]


def parse_price_status(raw: object) -> PriceStatus | None:
    text = _clean(raw)
    code = None if text is None else _code(text)
    if code is None:
        return None
    try:
        return PriceStatus(code)
    except ValueError:
        return None


def is_tradeable(raw: object) -> bool:
    """Only StatusSummary 0 (Normal) is tradeable; unknown or missing is not."""
    return parse_price_status(raw) == PriceStatus.NORMAL
