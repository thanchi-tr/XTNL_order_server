"""Storage ports of one leader term (IMPLEMENTATION_PLAN 3.6).

`EngineSession` (src/runtime/session.py) is composed against these ports, so the same composition
runs on PostgreSQL (`src/runtime/pg_ports.py`, production) and on in-memory fakes (the no-network
end-to-end smoke test). Redis is not a port: both run the real `src/cache` scripts (fakeredis + lupa
in tests).

Concrete types are used where the consuming class is typed concretely (`PgStateStore` for
`TieredStateCommitter`, `PgStreamHealth` for `TieredStreamHealth`).
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable, Collection, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Protocol

from src.calendar.schedule import ScheduleLoader
from src.db.epoch import Epoch
from src.db.gate_reader import GateInputs
from src.db.market_sessions import MarketSessionRow, SaveResult
from src.db.repository import PgStateStore, PgStreamHealth
from src.db.snapshot_reader import SnapshotInputs
from src.integrity.ports import DlqStore, IntegrityDb
from src.oversee.feed import QuarantineSink
from src.oversee.gate import GateContextReader
from src.oversee.halt import HaltStore
from src.oversee.notify import DedupeStore, OutboxStore
from src.oversee.snapshot import SnapshotReader
from src.oversee.sweep import SweepWriter
from src.ports import AdapterAuthStore, DlqSink, LocalStateReader
from src.runtime.leader_init import EpochFencer, SeqSource
from src.runtime.roles import EndReason


class TermWriter(Protocol):
    """leader_terms writes of this term: LEADER entry and DRAINING (fenced)."""

    async def mark_reached_leader(self, epoch: Epoch, now_ms: int) -> None: ...

    async def record_end_reason(self, epoch: Epoch, reason: EndReason) -> None: ...


@dataclass(frozen=True)
class SodStatus:
    rows: int
    partial: bool


class SodLedger(Protocol):
    """LEADER_INIT step 4 (sod catch-up) reads."""

    async def missing_accounts(self, trading_day: date) -> Sequence[str]:
        """Accounts without a filled `sod_marks` row for `trading_day`."""
        ...

    async def status(self, trading_day: date, accounts: Sequence[str]) -> SodStatus:
        """How many of `accounts` now have a filled row, and whether any is partial_session."""
        ...


@dataclass(frozen=True)
class OperatorRequest:
    """One pending `operator_requests` row for this leader's epoch (IMPLEMENTATION_PLAN 4.6 / 4.9)."""

    request_id: int
    kind: str           # a core kind (src/db/operator_requests.py CORE_REQUEST_KINDS) or a registered one
    target: str         # the task name, or the account id
    operator: str       # operator_audit.operator of the request
    payload: object = None   # operator_requests.payload (jsonb as text or a mapping); parsed per kind


class OperatorRequestQueue(Protocol):
    """In-process operator requests (`python -m src.ops task reset`), scoped to the epoch they were made in."""

    async def pending(self, epoch_token: int, kinds: Collection[str],
                      exclude: Collection[int] = ()) -> Sequence[OperatorRequest]:
        """This epoch's unconsumed requests whose kind is in `kinds` (the core kinds plus the registered ones;
        design §B13, RT-M10) and whose id is not in `exclude` (the session's accepted, unanswered requests, so
        they take no slot of the limit), oldest first, at most 50."""
        ...

    async def consume(self, epoch_token: int, request_id: int, result: str) -> None:
        """Mark the request consumed by this epoch (fenced; a request already consumed is left alone)."""
        ...


class MarketSessionStore(Protocol):
    """Write side of `market_sessions` (src/db/market_sessions.py PgMarketSessionStore, fenced)."""

    async def latest(self) -> tuple[int, list[MarketSessionRow]]: ...

    async def save(self, rows: Sequence[MarketSessionRow], *, adapters: Collection[str]) -> SaveResult: ...


class AuthRearmReader(Protocol):
    """Whether an operator re-armed (adapter, credential generation) in `adapter_auth_state` (state REARMED)."""

    async def rearmed_at(self, adapter_id: str, credential_generation: str) -> int | None:
        """The re-arm's `rearmed_at_ms` while the row is REARMED (it identifies one operator action), else None
        (no row, or AUTH_FAILED). The engine acts on each re-arm once, so a REARMED row read just before the
        adapter's next AUTH_FAILED is recorded never re-arms it a second time."""
        ...


@dataclass(frozen=True)
class LeaderPorts:
    fencer: EpochFencer
    seq: SeqSource
    terms: TermWriter
    state_store: PgStateStore
    stream_health: PgStreamHealth
    dlq_sink: DlqSink
    local_state: LocalStateReader
    auth_store: AdapterAuthStore
    snapshot_reader: Callable[[SnapshotInputs], SnapshotReader]
    sweep_writer: SweepWriter
    halt_store: HaltStore
    outbox: OutboxStore
    dedupe: DedupeStore
    feed_quarantine: QuarantineSink
    integrity_db: IntegrityDb
    dlq_store: DlqStore
    schedule: ScheduleLoader
    gate_reader: Callable[[GateInputs], GateContextReader]
    sod: SodLedger
    close: Callable[[], Awaitable[None]]
    # Drops the term pool's connections without waiting (never awaits): what EngineSession.aclose calls when
    # `close` has not finished inside drain_aclose_s (design RT-M6). None: nothing to drop (in-memory ports).
    terminate: Callable[[], None] | None = None
    # S31 (design §B14, R11-M11): wall time of the term pool's last successful fenced commit, the "engine PostgreSQL
    # down" input of the ARM's error budget (`PgHealthPortImpl`). None: not tracked (in-memory ports).
    last_fenced_commit_ms: Callable[[], int | None] | None = None


PortsFactory = Callable[[Epoch], LeaderPorts]
