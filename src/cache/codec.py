"""JSON encoding of DTOs for Redis values and PostgreSQL jsonb columns (Decimal kept exact as strings)."""
from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from decimal import Decimal
from enum import Enum
from typing import Any


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (tuple, list)):
        return [_plain(v) for v in value]
    return value


def dto_dict(obj: object) -> dict[str, Any]:
    if not is_dataclass(obj) or isinstance(obj, type):
        raise TypeError("dto_dict expects a dataclass instance")
    return {f.name: _plain(getattr(obj, f.name)) for f in fields(obj)}


def dto_json(obj: object) -> str:
    """Stable JSON (sorted keys, compact) of a DTO dataclass."""
    return json.dumps(dto_dict(obj), sort_keys=True, separators=(",", ":"))


def decimal_str(value: Decimal | None) -> str:
    return "" if value is None else format(value, "f")
