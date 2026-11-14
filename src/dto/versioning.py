"""Version labels, checksums and the two-step version compare (dto-contracts.md §2).

Key = (broker_seq, broker_updated_at_ms, observed_seq). At equal keys the checksum decides:
same content is a no-op; different content is ordered by seq_id (the last tie-breaker).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from decimal import Decimal
from enum import Enum
from typing import Any, Literal, Protocol

Source = Literal["STREAM", "REST"]

# Fields that are never part of a payload checksum.
NON_PAYLOAD_FIELDS = frozenset(
    {"broker_updated_at_ms", "broker_seq", "source", "observed_seq", "seq_id", "checksum"}
)

FUTURE_TIMESTAMP_TOLERANCE_MS = 5_000


def _canonical(value: Any) -> Any:
    if isinstance(value, Decimal):
        # normalise so 1.10 and 1.1 hash the same; keep a plain (non-exponent) form
        return format(value.normalize(), "f") if value == value else "NaN"
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (tuple, list)):
        return [_canonical(v) for v in value]
    return value


def payload_checksum(obj: Any) -> str:
    """SHA-256 over the broker payload fields of a DTO dataclass (sorted keys, canonical decimals)."""
    payload = {
        f.name: _canonical(getattr(obj, f.name))
        for f in fields(obj)
        if f.name not in NON_PAYLOAD_FIELDS
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class Versioned(Protocol):
    @property
    def broker_updated_at_ms(self) -> int: ...
    @property
    def broker_seq(self) -> int | None: ...
    @property
    def observed_seq(self) -> int: ...
    @property
    def seq_id(self) -> int: ...
    @property
    def checksum(self) -> str: ...


def version_key(v: Versioned) -> tuple[int, int, int]:
    """Comparable key. A missing broker_seq sorts below any real one (the orchestrator fills
    in the entity's last seen broker_seq before comparing, per dto-contracts.md §2)."""
    return (-1 if v.broker_seq is None else v.broker_seq, v.broker_updated_at_ms, v.observed_seq)


def version_str(v: Versioned) -> str:
    """Fixed-width string form for Lua compare (never tonumber()). Lexical order == key order."""
    bseq = 0 if v.broker_seq is None else v.broker_seq + 1
    return f"{bseq:020d}{v.broker_updated_at_ms:016d}{v.observed_seq:020d}"


class Decision(str, Enum):
    APPLY = "APPLY"                 # newer, different content
    NOOP_ADVANCE = "NOOP_ADVANCE"   # newer key, same checksum: advance stored version only
    NOOP = "NOOP"                   # identical key and content
    DROP_STALE = "DROP_STALE"       # lower version: drop, record in event_log only


def decide(stored: Versioned | None, incoming: Versioned) -> Decision:
    if stored is None:
        return Decision.APPLY
    ki, ks = version_key(incoming), version_key(stored)
    if ki > ks:
        return Decision.NOOP_ADVANCE if incoming.checksum == stored.checksum else Decision.APPLY
    if ki < ks:
        return Decision.DROP_STALE
    if incoming.checksum == stored.checksum:
        return Decision.NOOP
    return Decision.APPLY if incoming.seq_id > stored.seq_id else Decision.DROP_STALE


def is_future_timestamp(broker_updated_at_ms: int, received_at_ms: int) -> bool:
    return broker_updated_at_ms - received_at_ms > FUTURE_TIMESTAMP_TOLERANCE_MS
