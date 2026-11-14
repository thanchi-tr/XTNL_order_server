"""Integrity-side ports: the records and protocols src/integrity builds against (tier-persistence.md §2, §3, §5).

src/db provides the PostgreSQL implementations (IntegrityDb, DlqStore); src/cache provides the Redis half
(RedisState, NoBootQueue); the orchestrator / runtime provide IngestionControl, BrokerReconciler,
DlqBroker and PatroniControl. Nothing here does I/O.
"""
from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Final, Literal, Protocol

from src.cache import keys as K
from src.cache.digest import entity_hash
from src.cache.scripts import EntityWrite, entity_write, instrument_write
from src.dto import AccountDTO, AccountLive, CashFlowDTO, InstrumentSpec, OrderDTO, PositionDTO
from src.ports import DlqEventType

StatefulDTO = OrderDTO | PositionDTO | AccountDTO
EntityPayload = OrderDTO | PositionDTO | AccountDTO | InstrumentSpec
HaltLevelName = Literal["NONE", "SOFT", "HARD"]
Severity = Literal["info", "high", "critical"]

INTEGRITY_CHECK_INTERVAL_SEC: Final = 5.0
SETTLE_MS: Final = 500          # rows younger than this are not compared yet (§3 step 1)
MAX_DLQ_RETRIES: Final = 5      # §5


# ---- SM3 / SM8 / SM2 state names (docs/architecture page 3) --------------------------------------

class IntegrityState(str, Enum):
    """SM3 (tier-persistence.md §3)."""

    IDLE = "IDLE"
    CHECK_REDIS_VS_DB = "CHECK_REDIS_VS_DB"
    HEALING_REDIS = "HEALING_REDIS"
    CHECK_DB_INTEGRITY = "CHECK_DB_INTEGRITY"
    HALTED = "HALTED"
    SWITCHING_OVER = "SWITCHING_OVER"
    MANUAL_INTERVENTION = "MANUAL_INTERVENTION"
    RECONCILING = "RECONCILING"


INCIDENT_STATES: Final = frozenset({IntegrityState.HALTED, IntegrityState.SWITCHING_OVER,
                                    IntegrityState.MANUAL_INTERVENTION, IntegrityState.RECONCILING})


class BootState(str, Enum):
    """SM8 (tier-persistence.md §2). NOT_BOOTED = never booted or boot_id lost, not yet started."""

    NOT_BOOTED = "NOT_BOOTED"
    DETECTED = "DETECTED"
    RESTORE_HALT = "RESTORE_HALT"
    CREATE_GROUP = "CREATE_GROUP"
    PRICE_BARRIER = "PRICE_BARRIER"
    HEAL = "HEAL"
    REPLAY_NO_BOOT = "REPLAY_NO_BOOT"
    READY = "READY"


class DlqStatus(str, Enum):
    """events_dlq.status values (0001_init.sql)."""

    QUEUED = "QUEUED"
    LEASED = "LEASED"
    BACKOFF = "BACKOFF"
    RESOLVED = "RESOLVED"
    QUARANTINED = "QUARANTINED"


# ---- alerts ----------------------------------------------------------------------------------------

@dataclass(frozen=True)
class IntegrityAlert:
    """An operational page raised by src/integrity (relayed to the notification outbox by the runtime)."""

    severity: Severity
    code: str
    detail: str
    at_ms: int
    extra: Mapping[str, str] = field(default_factory=dict)


class Pager(Protocol):
    def __call__(self, alert: IntegrityAlert) -> None: ...


def null_pager(alert: IntegrityAlert) -> None:
    """Default pager: alerts are only logged by the caller."""


# ---- PostgreSQL rows -------------------------------------------------------------------------------

@dataclass(frozen=True)
class CommittedRow:
    """One PostgreSQL current row (orders_state / positions_state / account_state / instruments).

    `key` is the Redis entity key (src.cache.keys), `version` the 56-digit src.dto.version_str,
    `digest_hash` the stored sha256(key || version || checksum). `entity` is the row rebuilt as a DTO
    (its own checksum is recomputed on construction), used by Step 2 to verify the stored checksum.
    """

    key: str
    entity_type: K.EntityType
    entity_id: str
    account_id: str
    version: str
    seq_id: int
    checksum: str
    digest_hash: str
    payload: str
    committed_at_ms: int
    entity: EntityPayload | None = None

    def write(self) -> EntityWrite:
        """The apply_state.lua call that copies exactly this stored row to Redis."""
        return EntityWrite(key=self.key, version=self.version, seq_id=self.seq_id, checksum=self.checksum,
                           payload=self.payload, entity_type=self.entity_type, entity_id=self.entity_id,
                           account_id=self.account_id)


def committed_row(item: StatefulDTO | InstrumentSpec, committed_at_ms: int, *, seq_id: int | None = None,
                  with_entity: bool = True) -> CommittedRow:
    """CommittedRow for a DTO as PgStateStore would store it (tests and the db implementation)."""
    if isinstance(item, InstrumentSpec):
        w = instrument_write(item, seq_id if seq_id is not None else 0)
    else:
        w = entity_write(item)
    return CommittedRow(key=w.key, entity_type=w.entity_type, entity_id=w.entity_id, account_id=w.account_id,
                        version=w.version, seq_id=w.seq_id, checksum=w.checksum,
                        digest_hash=entity_hash(w.key, w.version, w.checksum), payload=w.payload,
                        committed_at_ms=committed_at_ms, entity=item if with_entity else None)


@dataclass(frozen=True)
class HaltRow:
    """The latest halt_events row (MAX(id)): the truth for halt:state."""

    id: int
    effective_level: HaltLevelName


@dataclass(frozen=True)
class CleanMark:
    """last_clean_marks(adapter_id, symbol, bid, ask, spread_pips, ts)."""

    adapter_id: str
    symbol: str
    bid: Decimal
    ask: Decimal
    ts: int
    spread_pips: Decimal | None = None


@dataclass(frozen=True)
class DbHighWater:
    """Step 2 monotonicity inputs: Patroni timeline, engine epoch and MAX(seq_id) over the HLC tables."""

    timeline: int
    epoch: int
    max_seq_id: int


class IntegrityDb(Protocol):
    """Reads used by SM3 and SM8. All reads; no epoch-checked writes (Redis heals are post-commit copies)."""

    async def committed_rows(self, from_ms: int, to_ms: int, limit: int) -> Sequence[CommittedRow]:
        """Current rows whose committed_at_ms is in [from_ms, to_ms], oldest first, at most `limit`."""
        ...

    async def rows_by_keys(self, keys: Collection[str]) -> Mapping[str, CommittedRow]:
        """Current rows for these Redis keys (absent keys are missing from the result)."""
        ...

    async def rows_after_key(self, after_key: str, limit: int) -> Sequence[CommittedRow]:
        """Current rows of all four tables ordered by Redis key (COLLATE "C"), key > after_key."""
        ...

    async def digest(self) -> str:
        """bit_xor of digest_hash over all current rows (64 lower-case hex; zeros when empty)."""
        ...

    async def latest_commit_ms(self) -> int | None:
        """MAX(committed_at_ms) over the current-row tables (None when empty)."""
        ...

    async def latest_halt_event(self) -> HaltRow | None:
        """The halt_events row with MAX(id), or None."""
        ...

    async def high_water(self) -> DbHighWater:
        """Timeline (pg_control_checkpoint().timeline_id), current epoch revision, MAX(seq_id)."""
        ...

    async def last_clean_marks(self) -> Sequence[CleanMark]: ...

    async def latest_live_samples(self) -> Sequence[AccountLive]:
        """The newest account_snapshots row with live fields per account, as AccountLive(sampled=True)."""
        ...


# ---- events_dlq ------------------------------------------------------------------------------------

@dataclass(frozen=True)
class DlqRecord:
    dlq_id: int
    event_type: DlqEventType
    entity_id: str
    adapter_id: str
    raw_payload: str
    payload_hash: str
    error_reason: str
    retry_count: int
    status: DlqStatus
    next_retry_at_ms: int
    parked_at_ms: int
    origin_dlq_id: int | None = None
    lease_owner: str | None = None
    lease_until_ms: int | None = None
    updated_at_ms: int = 0


class DlqStore(Protocol):
    """events_dlq / events_quarantine access for SM2 (epoch-checked transactions, owner-checked updates)."""

    async def lease(self, owner: str, now_ms: int, lease_ms: int, adapters: Collection[str],
                    limit: int) -> Sequence[DlqRecord]:
        """QUEUED / BACKOFF rows due (next_retry_at_ms <= now) of these adapters -> LEASED."""
        ...

    async def release_expired(self, now_ms: int) -> int:
        """LEASED rows whose lease_until_ms < now -> QUEUED, retry_count unchanged."""
        ...

    async def requeue(self, dlq_id: int, owner: str) -> bool:
        """Adapter left STREAMING: LEASED (by owner) -> QUEUED, retry_count unchanged."""
        ...

    async def resolve(self, dlq_id: int, owner: str) -> bool:
        """Converged and re-submitted (or nothing to re-submit): -> RESOLVED, parked payload dropped."""
        ...

    async def backoff(self, dlq_id: int, owner: str, retry_count: int, next_retry_at_ms: int,
                      last_error: str) -> bool:
        """-> BACKOFF with the new retry_count and due time."""
        ...

    async def quarantine(self, dlq_id: int, owner: str | None, reason: str) -> bool:
        """Insert into events_quarantine (source 'dlq') and set QUARANTINED, in one transaction.
        owner None = no lease check (an origin row quarantined through lineage)."""
        ...

    async def find_lineage_origin(self, event_type: DlqEventType, entity_id: str, error_reason: str,
                                  before_dlq_id: int, since_ms: int, *, adapter_id: str) -> DlqRecord | None:
        """The row this worker re-submitted for the same (adapter_id, event_type, entity_id, error_reason):
        status RESOLVED, origin_dlq_id NULL, dlq_id < before_dlq_id, updated_at_ms >= since_ms, newest
        first. Entity ids are unique only per adapter, so the lookup is always adapter-scoped."""
        ...

    async def merge_into_origin(self, dlq_id: int, owner: str, origin_dlq_id: int, retry_count: int,
                                next_retry_at_ms: int) -> bool:
        """Deterministic re-park: this row -> RESOLVED with origin_dlq_id set; the origin -> BACKOFF with
        retry_count and next_retry_at_ms (one transaction). Refused (False, nothing written) unless the
        origin is the same (adapter_id, event_type, entity_id) as this row."""
        ...

    async def cash_flow_since_ms(self, adapter_id: str) -> int | None:
        """min(last committed cash_flows.broker_ts, oldest parked CASH_FLOW broker_ts) for the adapter."""
        ...

    async def mark_transfers_unverified(self, adapter_id: str, account_id: str, at_ms: int) -> None:
        """Mark the current trading day transfers_unverified for the account (§4, §5)."""
        ...


# ---- collaborators outside src/integrity -----------------------------------------------------------

class DlqBroker(Protocol):
    """Check-before-retry reads through the orchestrator, the sole publisher (dto-contracts.md §6)."""

    def streaming_adapters(self) -> Collection[str]:
        """Adapters whose SM4 state is STREAMING right now."""
        ...

    async def reread_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                            entity_id: str) -> StatefulDTO | None:
        """Read the broker's row and re-submit it versioned at its own read (IngestionOrchestrator). None is
        absence only when the adapter recorded an explicit "not found" for this id (src.adapters.base.note_absent,
        read through ``absence_probe``); the worker treats every other None as not converged."""
        ...

    async def read_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                          entity_id: str) -> StatefulDTO | None:
        """Read only (illegal-transition rows are quarantined, not re-submitted)."""
        ...

    async def reread_cash_flow(self, adapter_id: str, transaction_id: str, since_ms: int) -> CashFlowDTO | None:
        """fetch_cash_flows(since_ms) matched on transaction_id; re-submit it when found."""
        ...


class IngestionControl(Protocol):
    """SM3 incident hooks into the orchestrator (sockets keep draining throughout)."""

    async def halt_ingestion(self, reason: str) -> None: ...

    async def resume_prices(self) -> None: ...

    async def resume_all(self) -> None: ...


class BrokerReconciler(Protocol):
    """RECONCILING: fetch_open_orders / fetch_positions / fetch_account_snapshot / fetch_cash_flows(since) plus
    watermark re-reads by id (never close on absence), committed. True when the reconcile committed."""

    async def reconcile_all(self, cash_flows_since_ms: int | None) -> bool: ...


class PatroniControl(Protocol):
    """operations.md §7: the engine never restores PostgreSQL; it can only switch over to a checked standby."""

    async def primary_member(self) -> str: ...

    async def standby_passes_check(self) -> bool:
        """Run the same Step 2 checks against the sync standby."""
        ...

    async def switchover(self) -> str:
        """Patroni switchover to the sync standby; returns the new primary. Raises on failure."""
        ...

    async def reinit(self, member: str) -> None:
        """patronictl reinit of the damaged member."""
        ...


class NoBootReplay(Protocol):
    """The queue of writes refused with NO_BOOT (src.cache.NoBootQueue satisfies it)."""

    def snapshot(self) -> list[EntityWrite]: ...

    def discard(self, w: EntityWrite) -> None: ...

    def __len__(self) -> int: ...


class BootSignal(Protocol):
    """What SM3 needs from SM8 (RedisBootstrap satisfies it)."""

    def signal(self, reason: str) -> None: ...

    @property
    def state(self) -> BootState: ...
