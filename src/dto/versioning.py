"""Version labels, checksums and the two-step version compare (dto-contracts.md §2).

Key = (broker_seq, broker_updated_at_ms, observed_seq). At equal keys the checksum decides:
same content is a no-op; different content is ordered by seq_id (the last tie-breaker).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import fields, replace
from decimal import Decimal
from enum import Enum
from typing import Any, Literal, Protocol, TypeVar

Source = Literal["STREAM", "REST"]
#: The path that produced a version (design PR8): `stream`, a `snapshot` / resync read, a `history` read, the DLQ
#: check-before-retry re-read (`dlq`) or a PR9 stop-order list read (`stop_read`). A version label, like `source`.
Origin = Literal["stream", "snapshot", "history", "dlq", "stop_read"]
#: Whether `broker_updated_at_ms` is a real broker time (`BROKER`) or not (`WATERMARK`: a REST row labelled with
#: the stream watermark, dto-contracts.md §2, or an entity type kept in the local-time domain). Only a `BROKER`
#: time reaches `event_log.broker_time_ms` (PR8).
BrokerTimeSource = Literal["BROKER", "WATERMARK"]

# Fields that are never part of a payload checksum.
NON_PAYLOAD_FIELDS = frozenset(
    {"broker_updated_at_ms", "broker_seq", "source", "observed_seq", "seq_id", "checksum", "origin",
     "broker_time_source"}
)
#: Broker payload fields added after the baseline (design PR1, PR7): part of the checksum only when set, so a
#: row without them keeps its checksum byte-identical (no integrity heal of the stored rows).
OPTIONAL_PAYLOAD_FIELDS = frozenset({"order_kind", "price_increment", "min_stop_distance", "asset_class"})
#: Fields the JSON encodings (src/cache/codec.py, src/pubsub/dlq_rows.py) leave out while None: the optional
#: payload fields, the PR8 version labels and the PR2 price timing fields. Rows that lack them encode exactly as
#: before, so Redis / jsonb payloads and the DLQ `payload_hash` stay byte-identical too.
OMITTED_WHEN_NONE = OPTIONAL_PAYLOAD_FIELDS | frozenset({"origin", "broker_time_source", "ts_source", "received_ms"})

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


def encoded(name: str, value: object) -> bool:
    """False for a field the encodings leave out: one of OMITTED_WHEN_NONE whose value is None."""
    return value is not None or name not in OMITTED_WHEN_NONE


_D = TypeVar("_D")


def without_optional_payload(obj: _D) -> _D:
    """`obj` with every OPTIONAL_PAYLOAD_FIELDS field None (the pre-0015 shape, plan S38f): the same object when none
    is set, else a copy whose checksum is re-sealed (`dataclasses.replace` re-runs `__post_init__`). Used by the
    ingestion orchestrator while the schema has no 0015 columns (`src/db/schema_compat.py`)."""
    changes = {f.name: None for f in fields(obj)  # type: ignore[arg-type]
               if f.name in OPTIONAL_PAYLOAD_FIELDS and getattr(obj, f.name) is not None}
    return replace(obj, **changes) if changes else obj  # type: ignore[type-var]


def payload_checksum(obj: Any) -> str:
    """SHA-256 over the broker payload fields of a DTO dataclass (sorted keys, canonical decimals). An optional
    payload field (OPTIONAL_PAYLOAD_FIELDS) counts only when it is not None."""
    payload = {
        f.name: _canonical(value)
        for f in fields(obj)
        if f.name not in NON_PAYLOAD_FIELDS and encoded(f.name, value := getattr(obj, f.name))
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
