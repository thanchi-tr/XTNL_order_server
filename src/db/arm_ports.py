"""The ARM's PostgreSQL store Protocols and row types (design §B3, §B5.3, §B6.4, §B7, §B8, §B14, §C1; plan S24).

This is the one `src.db` module the scoped packages may import (the lockout allow-list entry "the `src.db` store
Protocols", `src/riskmgr/lockout.py` `INTERNAL_ALLOWED`). It imports only the standard library and `src.dto`, never
`src.riskmgr`, `src.marketdata` or a driver, so `src/db` does not depend on the ARM: the riskmgr maps its own types
(`SM11` states, outcomes, `StopVersion`, `FailedSpoolRecord`) to these rows. `is_lost_fence` lets the scoped packages
recognise a lost fence without importing `src.db.fence` / `src.db.epoch` (it matches the error classes by their
qualified names; `tests/db/test_arm_store.py` keeps the names equal to the real classes). The implementations live in
`src/db/arm_store.py`, `arm_versions.py`, `arm_failures_store.py`, `arm_status_store.py` and `arm_config_store.py`.

Every row here holds ids, times, codes, counts, order-state levels (entry, stops, the proposed stop, `pre_basis`),
dimensionless R values and keyed hashes only: never a tick, bar, quote, spread, indicator value or a figure the close
can be recomputed from (favour R, favour / risk; design §B8). Levels and parameters are kept out of every `repr`.
jsonb columns travel as JSON text (`*_json`), so the rows stay hashable and driver-neutral; a read returns
PostgreSQL's jsonb rendering of the document (compare parsed values, never the text).

Position references outside `arm_positions` / `arm_decisions` (the `arm_failures.position_ids` elements, the §B8
item keys) are written `adapter_id:position_id` (S23 deviation 1): two brokers may reuse a position id.
"""
from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Final, Protocol

from src.dto import OrderKind, Origin, Side

RESOURCE_STOP_LEVEL: Final = "STOP_LEVEL"
OWNER_POSITION: Final = "position"
TERMINAL_ARM_STATES: Final = frozenset({"CLOSED", "DISARMED"})
"""A position row in one of these states is never changed again by the `row_version` CAS (§B5.3)."""


def position_ref(adapter_id: str, position_id: str) -> str:
    """`adapter_id:position_id`: the position reference outside the keyed tables (S23 deviation 1)."""
    return f"{adapter_id}:{position_id}"


LOST_FENCE_ERRORS: Final = frozenset({"src.db.fence.EpochMismatch", "src.db.epoch.EpochFenced"})
"""The qualified names of the errors that mean this node's fence is lost (the caller must hard-exit)."""


def is_lost_fence(error: BaseException) -> bool:
    """True for a lost fence (`EpochMismatch`, `EpochFenced` or a subclass): the ARM's default `fatal` predicate, so a
    caller that forgets to pass one still hard-exits instead of retrying a fenced write (operations.md §3)."""
    return any(f"{c.__module__}.{c.__qualname__}" in LOST_FENCE_ERRORS for c in type(error).__mro__)


# ---------------------------------------------------------------------------------------------------- arm_positions
@dataclass(frozen=True, kw_only=True)
class PositionKey:
    adapter_id: str
    position_id: str
    resource: str = RESOURCE_STOP_LEVEL
    owner: str = OWNER_POSITION


@dataclass(frozen=True, kw_only=True)
class ArmPositionRow:
    """One `arm_positions` row (0017). `row_version` is the stored version: the CAS writes it + 1."""

    key: PositionKey
    account_id: str
    symbol: str
    side: Side
    state: str
    prior_state: str | None = None
    state_reason: str | None = None
    row_version: int = 0
    edge_code: str
    attribution_source: str
    opened_by_client_order_id: str | None = None
    strategy_code: str
    strategy_version: int
    strategy_kind: str
    strategy_impl_version: int
    strategy_params_json: str = field(repr=False)
    guards_json: str = field(default="[]", repr=False)
    guards_hash: str
    indicator_specs_json: str = field(default="[]", repr=False)
    required_history_json: str = field(default="[]", repr=False)
    bar_spec: str
    config_doc_hash: str
    entry_level: Decimal = field(repr=False)
    entry_time_ms: int
    entry_time_source: str
    s0_level: Decimal = field(repr=False)
    s0_source: str
    pinned_units: Decimal = field(repr=False)
    strategy_state_json: str = field(default="{}", repr=False)
    last_decided_bar: int | None = None
    last_decided_hash: str | None = None
    pending_event: str | None = None
    pending_bar_end_ms: int | None = None
    reopen_bar_end_ms: int | None = None
    decision_seq: int = 0
    pre_basis_json: str | None = field(default=None, repr=False)
    pre_basis_bar: int | None = None
    armed_at_ms: int
    updated_at_ms: int
    ended_at_ms: int | None = None
    epoch: int = 0
    """The epoch of the last writer as stored; on a write the store always writes its own fencing token."""

    @property
    def ref(self) -> str:
        return position_ref(self.key.adapter_id, self.key.position_id)


# ---------------------------------------------------------------------------------------------------- arm_decisions
@dataclass(frozen=True, kw_only=True)
class DecisionRow:
    """One `arm_decisions` row without its `decision_id` (assigned by the INSERT). Input references are
    `series_id|bar_end_ms|key_id||hash` and `indicator_id|output|bar_end_ms|window_start|hash` texts (§B8)."""

    key: PositionKey
    bar_end_ms: int
    bar_spec: str
    event: str
    revision: int = 0
    decision_seq: int
    decided_at_ms: int
    edge_code: str
    outcome: str
    category: str
    reason: str | None = None
    reason_kind: str | None = None
    trigger_met: bool | None = None
    borderline: bool = False
    advice: bool
    proposed_stop: Decimal | None = field(default=None, repr=False)
    current_risk_r: Decimal | None = None
    first_bar_ratio: Decimal | None = None
    quality_flags: tuple[str, ...] = ()
    caveats: tuple[str, ...] = ()
    close_source: str | None = None
    stop_basis: str | None = None
    series_inputs: tuple[str, ...] = ()
    indicator_inputs: tuple[str, ...] = ()
    inputs_digest: str | None = None
    envelope_version: int
    envelope_params_hash: str
    strategy_kind: str
    strategy_impl_version: int
    guards_hash: str
    supersedes_decision_id: int | None = None
    supersedes_bar_end_ms: int | None = None
    range_from_bucket: int | None = None
    notice_id: int | None = None
    epoch: int = 0


@dataclass(frozen=True, kw_only=True)
class DecisionRef:
    """The composite reference of a decision (`arm_decisions` is partitioned on `bar_end_ms`)."""

    decision_id: int
    bar_end_ms: int


@dataclass(frozen=True, kw_only=True)
class StoredDecision:
    ref: DecisionRef
    row: DecisionRow


@dataclass(frozen=True, kw_only=True)
class UnnoticedPage:
    """One read of the decisions still without a notice (§B8), oldest first. `truncated`: more such decisions exist
    than were returned; the notice transaction attaches or suppresses these and reads again until it is False, so no
    committed decision is left out."""

    decisions: tuple[StoredDecision, ...]
    truncated: bool


@dataclass(frozen=True, kw_only=True)
class DecisionHandleRow:
    """One `arm_decision_handles` row (§B10): every level is order state."""

    handle_id: str
    handle_kind: str
    handle_role: str
    level_at_bar_end: Decimal | None = field(default=None, repr=False)
    broker_seq_at_bar_end: int | None = None
    broker_updated_at_bar_end: int | None = None
    level_read: Decimal | None = field(default=None, repr=False)
    broker_seq_read: int | None = None
    broker_updated_read: int | None = None
    read_at_ms: int | None = None
    level_at_pre: Decimal | None = field(default=None, repr=False)
    target_level: Decimal | None = field(default=None, repr=False)


@dataclass(frozen=True, kw_only=True)
class PositionCas:
    """The position update a decision commits with (cursor, its hash, `decision_seq`, ...): `row` holds the new
    values, `expected_row_version` the version read before the evaluation (§B5.3)."""

    row: ArmPositionRow
    expected_row_version: int


class DecisionWriteStatus(str, Enum):
    INSERTED = "INSERTED"
    ALREADY_DECIDED = "ALREADY_DECIDED"
    """`ON CONFLICT DO NOTHING`: this (position, bar_spec, event, bar_end, revision) is decided already."""
    CAS_MISS = "CAS_MISS"
    """The position's `row_version` moved (or it ended): the decision's savepoint was rolled back; re-read."""


@dataclass(frozen=True)
class DecisionWrite:
    status: DecisionWriteStatus
    ref: DecisionRef | None = None


@dataclass(frozen=True, kw_only=True)
class NoticeRow:
    """One `arm_notices` row (§B8 key and parts). `item_key` is empty for bar notices."""

    route: str
    bar_spec: str = ""
    event: str
    bar_end_ms: int
    part: int = 0
    item_key: str = ""
    decision_count: int = 0
    created_at_ms: int


@dataclass(frozen=True, kw_only=True)
class CorrectionAuditRow:
    """An `UNVERIFIABLE` correction result (§B6.6), audit only."""

    ref: DecisionRef
    adapter_id: str
    position_id: str
    reason: str
    recorded_at_ms: int
    result: str = "UNVERIFIABLE"


# ---------------------------------------------------------------------------------------------------- event_log reads
COUNTING_ORIGINS: Final = frozenset({"stream", "stop_read", "snapshot", "dlq"})
"""PR8 origins whose versions may give `S_at_T` (design §B6.4); `history` never counts."""


@dataclass(frozen=True, kw_only=True)
class EntryTime:
    """The fill time (§B7): `STREAM` (first stream POSITION version with OPEN) or `BROKER` (the entry order's
    FILLED / PARTIALLY_FILLED version with a real broker time). None from the reader means `ESTIMATED`."""

    source: str
    broker_time_ms: int
    seq_id: int
    origin: Origin | None = None


@dataclass(frozen=True, kw_only=True)
class HandleVersionRow:
    """One `event_log` version of one stop handle, as the `S_at_T` reads return it (all `APPLIED`).

    * `level` is set on a **counting** version (origin `stream`, `stop_read`, `snapshot` or `dlq` with a real
      broker time) and, since round 14 (plan S38g), on the newest non-counting versions (a `history` or
      watermark-labelled one): the pure rule takes the farther from entry of the prior counting level and theirs,
      so a non-counting level can only make the trigger test harder, never easier (R11-M06); the `stop_read`
      version after `T` is a marker with `level` None. `DROPPED_STALE` versions are never returned.
    * `live`: the handle still holds a stop in this version (a non-terminal child, an OPEN position).
    """

    handle_kind: str
    handle_id: str
    seq_id: int
    broker_time_ms: int | None
    origin: Origin | None
    status: str | None
    level: Decimal | None = field(default=None, repr=False)
    units: Decimal | None = field(default=None, repr=False)
    order_kind: OrderKind | None = None
    live: bool = True

    @property
    def counts(self) -> bool:
        """A counting origin with a real broker time (design §B6.4, R11-M06)."""
        return self.origin in COUNTING_ORIGINS and self.broker_time_ms is not None


# ---------------------------------------------------------------------------------------------------- arm_failures
@dataclass(frozen=True, kw_only=True)
class FailureRecord:
    """One FAILED episode (§B14 Record): ids, codes, counts and times only. `counters` maps a code to a count or a
    bucket time; `position_ids` are `adapter_id:position_id`."""

    failure_id: str
    scope_kind: str
    scope_id: str
    reason: str
    probe_kind: str
    ping_id: str
    first_fault_at_ms: int
    entered_at_ms: int
    edge_code: str | None = None
    counters: tuple[tuple[str, int], ...] = ()
    position_ids: tuple[str, ...] = ()


@dataclass(frozen=True, kw_only=True)
class FailedPing:
    """The immediate ping (§B8, §B14): the same title (carrying the ping id) and bounded body go out as the
    critical `info` row through the Oversee relay and as the kind `arm` row on `arm_email` and `arm_push`."""

    title: str
    body: str
    created_ms: int


@dataclass(frozen=True, kw_only=True)
class OpenFailure:
    record: FailureRecord
    epoch: int
    ping_count: int
    last_ping_at_ms: int | None = None
    last_probe_at_ms: int | None = None
    last_probe_result: str | None = None


@dataclass(frozen=True, kw_only=True)
class ClearedFailure:
    failure_id: str
    scope_kind: str
    scope_id: str
    cleared_at_ms: int
    request_id: int


# ---------------------------------------------------------------------------------------------------- arm_budget_counters
@dataclass(frozen=True, kw_only=True)
class BudgetCounterRow:
    """One durable D7 error-budget counter (0020 `arm_budget_counters`, plan S38f): a scope, a counter code, its count
    (consecutive buckets, or counted milliseconds for a time counter), when the streak started and the last counted
    event (a bucket time, an observation time, or the restart watermark). Codes, counts and times only."""

    scope_kind: str
    scope_id: str
    counter: str
    count: int
    streak_start_ms: int | None = None
    last_event_ms: int | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.scope_kind, self.scope_id, self.counter)


class ArmBudgetStore(Protocol):
    """The durable error-budget counters (plan S38f): restored at the FAILED manager's start, written fenced and only
    on change (a reset counter is written with count 0)."""

    async def load(self) -> list[BudgetCounterRow]: ...

    async def save(self, rows: Sequence[BudgetCounterRow], now_ms: int) -> None: ...


class RestartHistoryPort(Protocol):
    """Unclean engine restarts and takeovers (plan S38f): the end times of the leader terms that ended `lost` or
    `init_failed` at or after `since_ms` (`leader_terms`, read on the engine pool)."""

    async def unclean_ends_since(self, since_ms: int) -> list[int]: ...


# ---------------------------------------------------------------------------------------------------- status, config
@dataclass(frozen=True, kw_only=True)
class ArmStatusRow:
    """The per-node `arm_status` row (unfenced telemetry, §B5.1). `counters`: codes and counts only."""

    node: str
    epoch: int | None
    updated_at_ms: int
    heartbeat_at_ms: int | None = None
    leader: bool = False
    running: bool = False
    paused: bool = False
    pause_reason: str | None = None
    config_doc_hash: str | None = None
    armed_positions: int = 0
    unmanaged_positions: int = 0
    overdue_positions: int = 0
    failed_scopes: int = 0
    failed_global: bool = False
    counters: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, kw_only=True)
class NodeMdRecordRow:
    """This node's market-data record (§A7 `NodeMdRecord`, kept in its own `arm_status` row): the `md:boot` sentinel
    text it wrote (ids and a generation only) and the newest `last_write_ms` its writer wrote. S31's
    `NodeRecordPort` maps `src.marketdata.ports.NodeMdRecord` to this row."""

    node: str
    sentinel: str
    last_write_ms: int


@dataclass(frozen=True, kw_only=True)
class ConfigCacheRow:
    """The cached accepted document and the seen-row chain head (§B9). `row_hashes`: (row ref, row hash)."""

    doc_hash: str
    schema_version: int
    document_json: str = field(repr=False)
    row_hashes: tuple[tuple[str, str], ...] = ()
    chain_head: str
    chain_rows: int = 0
    updated_at_ms: int


@dataclass(frozen=True, kw_only=True)
class ConfigAuditRow:
    at_ms: int
    node: str
    action: str
    doc_hash: str | None = None
    row_refs: tuple[str, ...] = ()


@dataclass(frozen=True, kw_only=True)
class AcceptedRow:
    accepted_id: int
    doc_hash: str
    schema_version: int
    mode: str
    accepted_at_ms: int
    accepted_by: str


@dataclass(frozen=True, kw_only=True)
class ControlsRow:
    paused: bool
    pause_reason: str | None
    paused_at_ms: int | None
    drain_notices: bool
    notify_test_at_ms: int | None
    updated_at_ms: int


# ---------------------------------------------------------------------------------------------------- Protocols
class ArmStateTxn(Protocol):
    """One fenced transaction of the ARM pool (§B5.3): a lost fence raises (the caller hard-exits)."""

    async def insert_position(self, row: ArmPositionRow) -> bool:
        """`INSERT ... ON CONFLICT DO NOTHING`: False when the row exists already."""
        ...

    async def update_position(self, cas: PositionCas) -> bool:
        """The `row_version` CAS (`must=False`) in its own savepoint: False on a miss (version moved, or the row
        ended); an error rolls back only this savepoint and is re-raised, so the transaction stays usable."""
        ...

    async def record_decision(self, decision: DecisionRow, handles: Sequence[DecisionHandleRow] = (), *,
                              position: PositionCas | None = None) -> DecisionWrite:
        """In its own savepoint: the decision, its handle rows and the position CAS; a CAS miss rolls back only
        this savepoint."""
        ...

    async def insert_notice(self, notice: NoticeRow) -> int | None:
        """`ON CONFLICT DO NOTHING RETURNING notice_id`: None when the key exists (send no outbox row)."""
        ...

    async def attach_notice(self, notice_id: int, refs: Sequence[DecisionRef]) -> int:
        """Set `notice_id` on decisions still without one; returns how many were set."""
        ...

    async def suppress(self, refs: Sequence[DecisionRef]) -> int:
        """Mark decisions not shown with the `suppressed` sentinel (`notice_id = 0`)."""
        ...

    async def insert_correction_audit(self, row: CorrectionAuditRow) -> bool: ...

    async def unnoticed_decisions(self, route: Sequence[tuple[str, str]], bar_spec: str, *, since_bar_end_ms: int,
                                  limit: int = 500) -> UnnoticedPage:
        """Decisions of this route (its (adapter_id, account_id) pairs) with `notice_id` IS NULL (§B8): at most
        `limit` (>= 1), with `truncated` set when more exist."""
        ...


class ArmStateStore(Protocol):
    def transaction(self) -> AbstractAsyncContextManager[ArmStateTxn]: ...

    async def live_positions(self) -> list[ArmPositionRow]: ...

    async def position(self, key: PositionKey) -> ArmPositionRow | None: ...


class RecentDecisionReader(Protocol):
    """The committed final bar-end decisions the BarClock re-checks after a restart or a takeover (go-live gate
    `corrections_restart`, plan S39b). Read-only."""

    async def recent_final_decisions(self, since_bar_end_ms: int, limit: int) -> list[StoredDecision]:
        """Every committed `POST` / `LATE` / `CORRECTED` decision that is not an `UNRESOLVED_RANGE` span, with a
        bar end at or after `since_bar_end_ms`, newest first (bar end, then `decision_seq`, descending), so a read cut
        at `limit` rows drops the oldest bars, whose windows end first; the caller re-sorts them oldest first."""
        ...


class ArmVersionReader(Protocol):
    async def entry_time(self, adapter_id: str, position_id: str, entry_order_id: str | None) -> EntryTime | None: ...

    async def stop_versions(self, adapter_id: str, position_id: str, children: Sequence[str],
                            bar_end_ms: int) -> list[HandleVersionRow]:
        """The `S_at_T` versions of the `stop_loss` field, `children` and every child ever linked to the position
        that may have been live at `bar_end_ms` (round 13)."""
        ...


@dataclass(frozen=True, kw_only=True)
class DecisionMemory:
    """What a bar-end evaluation reads back from a position's **committed** decisions (plan S36b review), so it
    survives a restart or a leader change: `pre_levels` the `(handle_role, level_at_pre)` rows of this bar's committed
    PRE `ADJUST` (empty: no PRE proposed a move this bar; order state); `last_outcome` / `last_reason` /
    `last_reason_kind` the newest committed bar-end decision (POST, LATE, REOPEN, CORRECTED or a span; never a
    `NO_BROKER_BAR`, which changes no state) by `decision_seq`, None when there is none. `final_advice_target`
    (round 14, plan S38g): the proposed stop of the newest committed final `ADJUST` (event POST, LATE, REOPEN or
    CORRECTED) of an **earlier** bar, by `decision_seq` (order state); the evaluation uses it for a POST or LATE
    only while the position is ADVICE_OUTSTANDING (the advice was not withdrawn or superseded since).
    `prior_final_adjust` (round 14 review): the proposed stop of the newest final bar-end decision of an earlier bar
    (by bar end, then `decision_seq`; any outcome but `NO_BROKER_BAR`) **when that decision is an ADJUST**, else None.
    A REOPEN or CORRECTED re-run of a bar uses it instead: the row's SM11 state already includes this bar's own (or a
    later) decision, so only a final ADJUST directly before this bar shows advice outstanding when it began (a later
    withdrawal or a latched HOLD reads as none: the AT_PRE discount then applies, the conservative side)."""

    pre_levels: tuple[tuple[str, Decimal], ...] = field(default=(), repr=False)
    last_outcome: str | None = None
    last_reason: str | None = None
    last_reason_kind: str | None = None
    final_advice_target: Decimal | None = field(default=None, repr=False)
    prior_final_adjust: Decimal | None = field(default=None, repr=False)


class ArmDecisionReader(Protocol):
    async def decision_memory(self, key: PositionKey, bar_spec: str, bar_end_ms: int) -> DecisionMemory:
        """Read-only: this bar's committed PRE `ADJUST` levels and the newest committed bar-end decision."""
        ...


class PastDecisionReader(Protocol):
    async def decision_memory_before(self, key: PositionKey, bar_spec: str, bar_end_ms: int,
                                     decision_seq: int) -> DecisionMemory:
        """Read-only (`arm replay --rerun`, plan S39b): `decision_memory` as it stood before the decision with
        `decision_seq` was committed."""
        ...


class ArmFailuresStore(Protocol):
    async def record(self, failure: FailureRecord, ping: FailedPing, *, queue_ping: bool = True) -> bool:
        """One fenced transaction: the row, the critical copy and the kind `arm` row, or none of them. False when
        the episode (or another open one of the scope) exists already: no second ping. `queue_ping=False`: the row
        only (an episode folded into a collapsed ping, §B14)."""
        ...

    async def reping(self, failure_id: str, ping_id: str, ping: FailedPing) -> bool: ...

    async def note_probe(self, failure_id: str, at_ms: int, result: str) -> bool: ...

    async def open_episodes(self) -> list[OpenFailure]: ...


class ArmFailedClearPort(Protocol):
    """The `arm_clear_failed` clear statement (S30b; `src.db.arm_failures_store.PgArmFailedClear`): always on the
    engine (owner) pool, fenced, under the FAILED-clear advisory lock, linked to the request's audit row."""

    async def clear(self, request_id: int, failure_id: str, at_ms: int, *,
                    reset: Sequence[BudgetCounterRow] = ()) -> ClearedFailure | None:
        """Set `cleared_at_ms` / `clear_request_id`; None when the episode is not open or the request (kind, epoch,
        target, one episode per request) does not match it. `reset` (S38f): the error-budget counters the clear
        resets, upserted fenced into `arm_budget_counters` in the same transaction, only when the clear applied."""
        ...


class ArmStatusWriter(Protocol):
    async def upsert(self, row: ArmStatusRow) -> bool: ...


class ArmNodeRecordStore(Protocol):
    """This node's market-data record in its `arm_status` row (unfenced, forward-only on `last_write_ms`; the
    status upsert never touches it)."""

    async def save_md_record(self, row: NodeMdRecordRow) -> bool: ...

    async def load_md_record(self, node: str) -> NodeMdRecordRow | None: ...


class ArmConfigStore(Protocol):
    async def cached(self) -> ConfigCacheRow | None: ...

    async def save_cache(self, row: ConfigCacheRow) -> bool: ...

    async def audit(self, row: ConfigAuditRow) -> bool: ...

    async def latest_accepted(self) -> AcceptedRow | None: ...

    async def controls(self) -> ControlsRow | None: ...


__all__ = [
    "AcceptedRow", "ArmBudgetStore", "BudgetCounterRow", "RestartHistoryPort", "ArmConfigStore", "RecentDecisionReader", "PastDecisionReader", "ArmDecisionReader", "ArmFailedClearPort", "ArmFailuresStore", "ArmNodeRecordStore", "ArmPositionRow", "ArmStateStore",
    "ArmStateTxn", "ArmStatusRow", "ArmStatusWriter", "ArmVersionReader", "COUNTING_ORIGINS", "ClearedFailure",
    "ConfigAuditRow", "ConfigCacheRow", "ControlsRow", "CorrectionAuditRow", "DecisionHandleRow", "DecisionMemory",
    "DecisionRef", "DecisionRow", "DecisionWrite", "DecisionWriteStatus", "EntryTime", "FailedPing", "FailureRecord",
    "HandleVersionRow", "LOST_FENCE_ERRORS", "NodeMdRecordRow", "NoticeRow", "OWNER_POSITION", "OpenFailure",
    "PositionCas", "PositionKey", "RESOURCE_STOP_LEVEL", "StoredDecision", "TERMINAL_ARM_STATES", "UnnoticedPage",
    "is_lost_fence", "position_ref",
]
