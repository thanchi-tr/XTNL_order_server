"""Per-entity current-row SQL (tier-persistence.md §4): orders_state, positions_state, account_state.

Statements are generated once at import from fixed column lists; every value is a `$n` parameter. The
writes are fenced (src/db/fence.py): `$1` is the epoch, prepended by `FencedTx`, so the `*_params`
tuples below start at `$2`. INSERT ... SELECT parameters carry explicit casts (asyncpg binary protocol
infers nothing from a bare SELECT list).
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Final

from src.cache.scripts import EntityWrite
from src.db.fence import FENCE
from src.dto import AccountDTO, OrderDTO, PositionDTO

StatefulDTO = OrderDTO | PositionDTO | AccountDTO

VERSION_COLS: Final = ("broker_seq", "broker_updated_at_ms", "observed_seq", "source", "version", "seq_id",
                       "checksum", "digest_hash")

COL_TYPES: Final[dict[str, str]] = {
    "adapter_id": "text", "broker_order_id": "text", "client_order_id": "text", "account_id": "text",
    "symbol": "text", "side": "text", "status": "text", "entry_price": "numeric", "stop_loss": "numeric",
    "take_profit": "numeric", "ordered_units": "numeric", "remaining_units": "numeric",
    "instrument_unknown": "boolean", "raw_qty": "numeric", "is_reduce_only": "boolean",
    "linked_position_id": "text", "order_kind": "text", "position_id": "text", "units": "numeric",
    "opened_by_client_order_id": "text", "accrued_swap": "numeric", "commission": "numeric",
    "currency": "text", "balance": "numeric", "realised_pnl_today": "numeric",
    "broker_seq": "bigint", "broker_updated_at_ms": "bigint", "observed_seq": "bigint", "source": "text",
    "version": "text", "seq_id": "bigint", "checksum": "text", "digest_hash": "text",
}

FIRST_PARAM: Final = 2   # $1 is the epoch


def _value(v: object) -> object:
    return v.value if isinstance(v, Enum) else v


@dataclass(frozen=True)
class EntityTable:
    table: str
    entity_type: str
    topic: str
    key_cols: tuple[str, ...]
    payload_cols: tuple[str, ...]
    change_cols: tuple[str, ...] = ()   # a change of one of these columns stamps `stamp_col`
    #: Orders / positions: status or units changed (oversee-rules.md §2 skew input). Accounts: the balance
    #: changed (`balance_changed_at_ms`, the "latest balance version" time); never `committed_at_ms`, which
    #: every UPDATE (including a same-checksum NOOP_ADVANCE) moves.
    stamp_col: str = "status_units_changed_at_ms"
    #: The lifecycle column read back with the current row, so the store can refuse an illegal transition
    #: (dto-contracts.md §5) even when the entity is not in the orchestrator's memory. None: no lifecycle.
    status_col: str | None = "status"

    @property
    def all_cols(self) -> tuple[str, ...]:
        return self.key_cols + self.payload_cols + VERSION_COLS

    def _where(self, first: int) -> str:
        return " AND ".join(f"{c} = ${i}" for i, c in enumerate(self.key_cols, first))

    @property
    def select_for_update(self) -> str:
        """Unfenced read: $1.. are the key columns. Columns: the version (StoredVersion) and, for a table with
        a lifecycle, its status as a sixth column."""
        status = f", {self.status_col}" if self.status_col else ""
        return (f"SELECT broker_seq, broker_updated_at_ms, observed_seq, seq_id, checksum{status} FROM {self.table} "
                f"WHERE {self._where(1)} FOR UPDATE")

    @property
    def insert_new(self) -> str:
        cols = self.all_cols
        sel = ", ".join(f"${i}::{COL_TYPES[c]}" for i, c in enumerate(cols, FIRST_PARAM))
        return (f"INSERT INTO {self.table} ({', '.join(cols)}) SELECT {sel} WHERE {FENCE} "
                f"ON CONFLICT ({', '.join(self.key_cols)}) DO NOTHING RETURNING 1")

    def _change_stamp(self, assigned: dict[str, int]) -> str:
        if not self.change_cols:
            return ""
        cond = " OR ".join(f"{c} IS DISTINCT FROM ${assigned[c]}" for c in self.change_cols if c in assigned)
        if not cond:
            return ""
        return f", {self.stamp_col} = CASE WHEN {cond} THEN now_ms() ELSE {self.stamp_col} END"

    def _update(self, cols: tuple[str, ...]) -> str:
        first = FIRST_PARAM + len(self.key_cols)
        assigned = {c: i for i, c in enumerate(cols, first)}
        sets = ", ".join(f"{c} = ${i}" for c, i in assigned.items())
        # The change stamp reads the old column values (SET right-hand sides see the pre-update row).
        return (f"UPDATE {self.table} SET {sets}, committed_at_ms = now_ms(){self._change_stamp(assigned)} "
                f"WHERE {self._where(FIRST_PARAM)} AND {FENCE}")

    @property
    def update_apply(self) -> str:
        return self._update(self.payload_cols + VERSION_COLS)

    @property
    def update_advance(self) -> str:
        """Same checksum, newer key: advance only the stored version (and its digest hash)."""
        return self._update(tuple(c for c in VERSION_COLS if c != "checksum"))

    def key_values(self, dto: StatefulDTO) -> tuple[object, ...]:
        return tuple(getattr(dto, c) for c in self.key_cols)

    def payload_values(self, dto: StatefulDTO) -> tuple[object, ...]:
        return tuple(_value(getattr(dto, c)) for c in self.payload_cols)

    @staticmethod
    def version_values(dto: StatefulDTO, w: EntityWrite) -> tuple[object, ...]:
        return (dto.broker_seq, dto.broker_updated_at_ms, dto.observed_seq, dto.source, w.version, dto.seq_id,
                dto.checksum, w.digest_hash)

    def insert_params(self, dto: StatefulDTO, w: EntityWrite) -> tuple[object, ...]:
        return self.key_values(dto) + self.payload_values(dto) + self.version_values(dto, w)

    def apply_params(self, dto: StatefulDTO, w: EntityWrite) -> tuple[object, ...]:
        return self.insert_params(dto, w)

    def advance_params(self, dto: StatefulDTO, w: EntityWrite) -> tuple[object, ...]:
        v = self.version_values(dto, w)
        return self.key_values(dto) + v[:6] + v[7:]


ORDERS: Final = EntityTable(
    table="orders_state", entity_type="ORDER", topic="orders.internal.dto",
    key_cols=("adapter_id", "broker_order_id"),
    payload_cols=("client_order_id", "account_id", "symbol", "side", "status", "entry_price", "stop_loss",
                  "take_profit", "ordered_units", "remaining_units", "instrument_unknown", "raw_qty",
                  "is_reduce_only", "linked_position_id", "order_kind"),   # order_kind: 0015 (PR1)
    change_cols=("status", "ordered_units", "remaining_units"),
)
POSITIONS: Final = EntityTable(
    table="positions_state", entity_type="POSITION", topic="position.state.dto",
    key_cols=("adapter_id", "position_id"),
    payload_cols=("account_id", "symbol", "side", "units", "entry_price", "status", "instrument_unknown",
                  "raw_qty", "stop_loss", "take_profit", "opened_by_client_order_id", "accrued_swap",
                  "commission"),
    change_cols=("status", "units"),
)
ACCOUNTS: Final = EntityTable(
    table="account_state", entity_type="ACCOUNT", topic="account.state.dto",
    key_cols=("adapter_id", "account_id"),
    payload_cols=("currency", "balance", "realised_pnl_today"),
    change_cols=("balance",),
    stamp_col="balance_changed_at_ms",   # 0005_balance_changed_at.sql; an INSERT stamps it by default
    status_col=None,                     # accounts have no lifecycle
)


def table_for(dto: StatefulDTO) -> EntityTable:
    if isinstance(dto, OrderDTO):
        return ORDERS
    if isinstance(dto, PositionDTO):
        return POSITIONS
    return ACCOUNTS


TableFor = Callable[[StatefulDTO], EntityTable]
