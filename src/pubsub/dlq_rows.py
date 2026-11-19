"""Build events_dlq rows from DTOs (tier-persistence.md §5).

entity_id is the id the DLQ worker re-reads from the broker: broker_order_id for orders
(BrokerAdapter.fetch_order_status takes the broker id), position_id, account_id or transaction_id.
Prices and AccountLive are never parked.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from decimal import Decimal
from enum import Enum
from typing import Any, Final

from src.dto import AccountDTO, CashFlowDTO, OrderDTO, PositionDTO, encoded
from src.ports import DlqEventType, DlqRow

Parkable = OrderDTO | PositionDTO | AccountDTO | CashFlowDTO

# error_reason values (tier-persistence.md §5, dto-contracts.md §5).
QUEUE_FULL: Final = "QueueFull"
SCHEMA_INVALID: Final = "schema_invalid"
CHECKSUM: Final = "checksum"
FUTURE_TIMESTAMP: Final = "future_timestamp"
ILLEGAL_TRANSITION: Final = "illegal_transition"
MISSING_CLIENT_ORDER_ID: Final = "missing_client_order_id"

#: Reasons that fail the same way every time the same event is re-submitted: a re-park with the same
#: (entity_id, error_reason) increments the origin row's retry_count instead of starting a fresh count.
#: The single definition, used by the orchestrator, src.db.repository (PgDlqSink) and src.integrity.
DETERMINISTIC_REASONS: Final = frozenset({SCHEMA_INVALID, CHECKSUM, FUTURE_TIMESTAMP, ILLEGAL_TRANSITION,
                                          MISSING_CLIENT_ORDER_ID})


def is_deterministic(reason: str) -> bool:
    return reason in DETERMINISTIC_REASONS


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value


def dto_json(dto: Any) -> str:
    """Deterministic JSON of every dataclass field (version fields and checksum included), except a None field
    of OMITTED_WHEN_NONE, so the payload_hash of a row without the ARM prerequisite fields is unchanged."""
    payload = {f.name: _plain(v) for f in fields(dto) if encoded(f.name, v := getattr(dto, f.name))}
    payload["_type"] = type(dto).__name__
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def entity_ref(dto: Parkable) -> tuple[DlqEventType, str]:
    if isinstance(dto, OrderDTO):
        return "ORDER", dto.broker_order_id
    if isinstance(dto, PositionDTO):
        return "POSITION", dto.position_id
    if isinstance(dto, AccountDTO):
        return "ACCOUNT", dto.account_id
    return "CASH_FLOW", dto.transaction_id


def dlq_row(dto: Parkable, error_reason: str) -> DlqRow:
    event_type, entity_id = entity_ref(dto)
    raw = dto_json(dto)
    return DlqRow(
        event_type=event_type,
        entity_id=entity_id,
        adapter_id=dto.adapter_id,
        raw_payload=raw,
        payload_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        error_reason=error_reason,
    )
