"""Shared ports between the ingestion pipeline (src/pubsub) and storage (src/db, src/cache).

src/pubsub depends only on these protocols; src/db + src/cache provide the implementations.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Literal, Protocol

from src.dto import AccountDTO, AccountLive, CashFlowDTO, InstrumentSpec, OrderDTO, PositionDTO, PriceDTO

DlqEventType = Literal["ORDER", "POSITION", "ACCOUNT", "CASH_FLOW"]
StreamKind = Literal["prices", "orders", "account"]


class CommitOutcome(str, Enum):
    APPLIED = "APPLIED"            # newer version, committed to PostgreSQL and copied to Redis (+ feed entry)
    NOOP_ADVANCED = "NOOP_ADVANCED"  # same checksum, stored version advanced only (no feed entry)
    STALE = "STALE"                # lower version: dropped, recorded in event_log only
    NO_BOOT = "NO_BOOT"            # committed to PostgreSQL; Redis copy deferred until SM8 READY


@dataclass(frozen=True)
class DlqRow:
    """One events_dlq row (tier-persistence.md §5)."""

    event_type: DlqEventType
    entity_id: str
    adapter_id: str
    raw_payload: str          # JSON of the DTO / broker frame
    payload_hash: str         # sha256 of raw_payload (idempotent insert key)
    error_reason: str         # QueueFull | schema_invalid | checksum | future_timestamp | illegal_transition | timeout
    origin_dlq_id: int | None = None


class StateCommitter(Protocol):
    """Unit of work per entity: PostgreSQL current row (version CAS) + event_log + outbox in one
    epoch-checked transaction, then Redis apply_state.lua after commit."""

    async def commit_entity(self, dto: OrderDTO | PositionDTO | AccountDTO) -> CommitOutcome: ...

    async def commit_cash_flow(self, cf: CashFlowDTO) -> bool:
        """Insert idempotently on transaction_id; XADD an ACCOUNT feed entry. True if new."""
        ...

    async def write_live(self, item: AccountLive | PriceDTO) -> None:
        """Light script: price:/clean_mid:/account_live: (latest value wins, no feed entry)."""
        ...


class DlqSink(Protocol):
    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        """INSERT ... ON CONFLICT (payload_hash) DO NOTHING. Raises ConnectionError when PostgreSQL is down."""
        ...


class StreamHealthSink(Protocol):
    """Mirrors subscription health into PostgreSQL stream_health and Redis conn: keys."""

    async def mark_lost(self, adapter_id: str, stream: StreamKind) -> None: ...

    async def mark_reconciled(self, adapter_id: str, stream: StreamKind) -> None: ...

    async def heartbeat(self, adapter_id: str, stream: StreamKind) -> None: ...


class InstrumentSink(Protocol):
    """Persists an instrument refresh (SM4 SNAPSHOTTING) into PostgreSQL `instruments`, then the Redis
    instrument: keys (tier-persistence.md §1, §4). Only changed rows are written."""

    async def upsert_instruments(self, specs: Sequence[InstrumentSpec], seq_id: int) -> int:
        """Returns how many rows changed. `seq_id` is the HLC value of the refresh (instrument version)."""
        ...


class AdapterAuthStore(Protocol):
    """`adapter_auth_state`: AUTH_FAILED per (adapter, credential generation), a cluster-wide fact
    (operations.md §4, §16). A leader does not retry until the generation changes or an operator re-arms."""

    async def known_bad(self, adapter_id: str, credential_generation: str) -> bool: ...

    async def record_auth_failed(self, adapter_id: str, credential_generation: str, detail: str) -> None: ...

    async def rearm(self, adapter_id: str, credential_generation: str, operator: str) -> bool:
        """Audited operator re-arm; True when a failed generation was re-armed."""
        ...


class LocalStateReader(Protocol):
    """The locally stored non-terminal orders / OPEN positions of an adapter, read before its first
    snapshot so absent ids are re-read by id (adapter-isolation.md §5). Same shape as the protocol in
    src/pubsub/orchestrator.py."""

    async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]: ...
