"""Wire-value parsing for StoneX (CIAPI) payloads (adapter-isolation.md §1).

Lightstreamer renders every field as text and REST payloads vary in key casing, so
every reader here is total: absent or malformed values become ``None``, never ``0``.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

JsonObj = Mapping[str, Any]

_WCF_DATE = re.compile(r"Date\((-?\d+)(?:[+-]\d{4})?\)")


def get_ci(obj: Mapping[str, Any] | None, *names: str) -> Any:
    """First non-None value among ``names``, matched exactly, then case-insensitively."""
    if not obj:
        return None
    for name in names:
        value = obj.get(name)
        if value is not None:
            return value
    lowered = {k.lower(): v for k, v in obj.items() if isinstance(k, str)}
    for name in names:
        value = lowered.get(name.lower())
        if value is not None:
            return value
    return None


def get_obj(obj: Mapping[str, Any] | None, *names: str) -> JsonObj | None:
    value = get_ci(obj, *names)
    return value if isinstance(value, Mapping) else None


def get_list(obj: Mapping[str, Any] | None, *names: str) -> list[JsonObj]:
    value = get_ci(obj, *names)
    if not isinstance(value, list):
        return []
    return [v for v in value if isinstance(v, Mapping)]


def to_decimal(raw: object) -> Decimal | None:
    """Parse a decimal wire value; blanks, booleans, NaN and infinities become None."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, Decimal):
        value = raw
    else:
        text = str(raw).strip()
        if not text:
            return None
        try:
            value = Decimal(text)
        except (InvalidOperation, ValueError):
            return None
    return value if value.is_finite() else None


def to_int(raw: object) -> int | None:
    """Parse an integral wire value ("3", "3.0", 3); anything fractional becomes None."""
    value = to_decimal(raw)
    if value is None or value != value.to_integral_value():
        return None
    return int(value)


def to_id(raw: object) -> str | None:
    """Broker ids are integers on the wire; return their canonical string form."""
    value = to_int(raw)
    return None if value is None else str(value)


def to_bool(raw: object) -> bool | None:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in ("true", "1", "yes"):
        return True
    if text in ("false", "0", "no"):
        return False
    return None


def epoch_to_ms(value: Decimal) -> int | None:
    """Epoch of unknown unit (s / ms / us / ns) -> ms, detected by magnitude."""
    if value <= 0:
        return None
    if value < Decimal(10) ** 11:        # seconds (until year ~5138)
        return int(value * 1000)
    if value < Decimal(10) ** 14:        # milliseconds
        return int(value)
    if value < Decimal(10) ** 17:        # microseconds
        return int(value / 1000)
    return int(value / 1_000_000)        # nanoseconds


def parse_broker_time_ms(raw: object) -> int | None:
    r"""Broker time in UTC ms from ``/Date(1289231327280)/`` (escaped ``\/Date(..)\/`` too),
    an ISO-8601 string, or a bare epoch number of any unit. Never invents a value."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, Decimal)):
        return epoch_to_ms(Decimal(raw))
    text = str(raw).strip().replace("\\", "")
    if not text:
        return None
    match = _WCF_DATE.search(text)
    if match:
        ms = int(match.group(1))   # WCF dates are UTC ms; the offset only names the zone
        return ms if ms > 0 else None
    number = to_decimal(text)
    if number is not None:
        return epoch_to_ms(number)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    ms = int(dt.timestamp() * 1000)
    return ms if ms > 0 else None
