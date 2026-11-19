"""The ARM state store: positions, decisions, handles, notices, correction audit (design §B5.3, §B6.6, §B8; plan S24).

Runs on the ARM pool (role `xtnl_arm`, §B3) inside one fenced READ COMMITTED transaction per (event, bar end); the
statements and their modes are the §B5.3 table:

| Statement | Mode |
|---|---|
| `INSERT arm_decisions ... ON CONFLICT DO NOTHING RETURNING` | `must=False`; conflict = already decided |
| `UPDATE arm_positions ... WHERE row_version = $n AND state NOT IN ('CLOSED','DISARMED')` | `must=False`; a miss rolls back the decision's savepoint |
| `INSERT arm_positions ... ON CONFLICT DO NOTHING` | `must=False` |
| `INSERT arm_notices ... ON CONFLICT DO NOTHING RETURNING notice_id` | `must=False`; outbox only if returned |

Every decision is written inside `FencedTx.savepoint()` (`record_decision`): the decision row, its handle rows and the
position CAS commit together or not at all, and a CAS miss or an error undoes only that savepoint while the event's
transaction carries on with the next position. A position CAS alone (`update_position`, the `pending_event`
bookkeeping) has its own savepoint too, so its error never aborts the other positions' decisions. A fenced statement that affects 0 rows is re-checked against
`engine_epoch` (`FencedTx`), so a lost fence always raises `EpochMismatch`.

"Decisions of this route with `notice_id` IS NULL" (`unnoticed_decisions`) is how each notice transaction makes sure
no committed decision is left out (§B8); `attach_notice` sets `notice_id` only where it is still NULL, and `suppress`
writes the `suppressed` sentinel (`SUPPRESSED_NOTICE_ID`, 0).

Values are ids, codes, counts, order-state levels and keyed hashes only (`src/db/arm_ports.py`); jsonb columns are
written from JSON text (`$n::text::jsonb`) and read back as text.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal
from typing import Any, Final, cast

from src.db.arm_ports import (
    ArmPositionRow,
    ArmStateTxn,
    CorrectionAuditRow,
    DecisionHandleRow,
    DecisionMemory,
    DecisionRef,
    DecisionRow,
    DecisionWrite,
    DecisionWriteStatus,
    NoticeRow,
    PositionCas,
    PositionKey,
    StoredDecision,
    UnnoticedPage,
)
from src.db.arm_schema import SUPPRESSED_NOTICE_ID
from src.db.epoch import EngineDb
from src.db.fence import FENCE, FencedTx
from src.db.pg import Row
from src.dto import Side

JSON: Final = "text::jsonb"

# ---------------------------------------------------------------------------------------------------- columns
#: `arm_positions` columns written by the insert (table order), with their parameter casts; `epoch` is `$1`.
POSITION_KEY_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("adapter_id", "text"), ("position_id", "text"), ("resource", "text"), ("owner", "text"))
POSITION_FIXED_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("account_id", "text"), ("symbol", "text"), ("side", "text"), ("armed_at_ms", "bigint"))
#: The columns the `row_version` CAS may change (everything but the key, the fixed columns, row_version and epoch).
POSITION_MUTABLE_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("state", "text"), ("prior_state", "text"), ("state_reason", "text"), ("edge_code", "text"),
    ("attribution_source", "text"), ("opened_by_client_order_id", "text"), ("strategy_code", "text"),
    ("strategy_version", "int"), ("strategy_kind", "text"), ("strategy_impl_version", "int"),
    ("strategy_params", JSON), ("guards", JSON), ("guards_hash", "text"), ("indicator_specs", JSON),
    ("required_history", JSON), ("bar_spec", "text"), ("config_doc_hash", "text"), ("entry_level", "numeric"),
    ("entry_time_ms", "bigint"), ("entry_time_source", "text"), ("s0_level", "numeric"), ("s0_source", "text"),
    ("pinned_units", "numeric"), ("strategy_state", JSON), ("last_decided_bar", "bigint"),
    ("last_decided_hash", "text"), ("pending_event", "text"), ("pending_bar_end_ms", "bigint"),
    ("reopen_bar_end_ms", "bigint"), ("decision_seq", "bigint"), ("pre_basis", JSON), ("pre_basis_bar", "bigint"),
    ("updated_at_ms", "bigint"), ("ended_at_ms", "bigint"))
POSITION_INSERT_COLUMNS: Final = (POSITION_KEY_COLUMNS + POSITION_FIXED_COLUMNS + (("row_version", "bigint"),)
                                  + POSITION_MUTABLE_COLUMNS)
_POSITION_JSON: Final = frozenset(c for c, t in POSITION_INSERT_COLUMNS if t == JSON)

DECISION_KEY: Final = "adapter_id, position_id, resource, owner, bar_spec, event, bar_end_ms, revision"
#: `arm_decisions` columns written by the insert (everything but `decision_id`); `epoch` is `$1`.
DECISION_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("bar_end_ms", "bigint"), ("adapter_id", "text"), ("position_id", "text"), ("resource", "text"),
    ("owner", "text"), ("bar_spec", "text"), ("event", "text"), ("revision", "int"), ("decision_seq", "bigint"),
    ("decided_at_ms", "bigint"), ("edge_code", "text"), ("outcome", "text"), ("category", "text"),
    ("reason", "text"), ("reason_kind", "text"), ("trigger_met", "boolean"), ("borderline", "boolean"),
    ("advice", "boolean"), ("proposed_stop", "numeric"), ("current_risk_r", "numeric"),
    ("first_bar_ratio", "numeric"), ("quality_flags", "text[]"), ("caveats", "text[]"), ("close_source", "text"),
    ("stop_basis", "text"), ("series_inputs", "text[]"), ("indicator_inputs", "text[]"), ("inputs_digest", "text"),
    ("envelope_version", "int"), ("envelope_params_hash", "text"), ("strategy_kind", "text"),
    ("strategy_impl_version", "int"), ("guards_hash", "text"), ("supersedes_decision_id", "bigint"),
    ("supersedes_bar_end_ms", "bigint"), ("range_from_bucket", "bigint"), ("notice_id", "bigint"))
HANDLE_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("handle_id", "text"), ("handle_kind", "text"), ("handle_role", "text"), ("level_at_bar_end", "numeric"),
    ("broker_seq_at_bar_end", "bigint"), ("broker_updated_at_bar_end", "bigint"), ("level_read", "numeric"),
    ("broker_seq_read", "bigint"), ("broker_updated_read", "bigint"), ("read_at_ms", "bigint"),
    ("level_at_pre", "numeric"), ("target_level", "numeric"))


def _casts(cols: Sequence[tuple[str, str]], start: int) -> str:
    return ", ".join(f"${i}::{t}" for i, (_, t) in enumerate(cols, start))


def _names(cols: Sequence[tuple[str, str]]) -> str:
    return ", ".join(c for c, _ in cols)


def _select_list(cols: Sequence[tuple[str, str]], json_cols: frozenset[str], prefix: str = "") -> str:
    return ", ".join(f"{prefix}{c}::text" if c in json_cols else f"{prefix}{c}" for c, _ in cols)


# ---------------------------------------------------------------------------------------------------- positions SQL
SQL_POSITION_INSERT: Final = (
    f"INSERT INTO arm_positions ({_names(POSITION_INSERT_COLUMNS)}, epoch) "
    f"SELECT {_casts(POSITION_INSERT_COLUMNS, 2)}, $1::bigint WHERE {FENCE} "
    "ON CONFLICT (adapter_id, position_id, resource, owner) DO NOTHING"
)
_CAS_SET: Final = ", ".join(f"{c} = ${i}::{t}" for i, (c, t) in enumerate(POSITION_MUTABLE_COLUMNS, 2))
_CAS_KEY: Final = len(POSITION_MUTABLE_COLUMNS) + 2
SQL_POSITION_CAS: Final = (
    f"UPDATE arm_positions SET {_CAS_SET}, row_version = row_version + 1, epoch = $1::bigint "
    f"WHERE adapter_id = ${_CAS_KEY}::text AND position_id = ${_CAS_KEY + 1}::text "
    f"AND resource = ${_CAS_KEY + 2}::text AND owner = ${_CAS_KEY + 3}::text "
    f"AND row_version = ${_CAS_KEY + 4}::bigint AND state NOT IN ('CLOSED', 'DISARMED') AND {FENCE}"
)
POSITION_SELECT_COLUMNS: Final = (POSITION_INSERT_COLUMNS + (("epoch", "bigint"),))
POSITION_COLS: Final = _select_list(POSITION_SELECT_COLUMNS, _POSITION_JSON)
SQL_POSITIONS_LIVE: Final = (
    f"SELECT {POSITION_COLS} FROM arm_positions WHERE state NOT IN ('CLOSED', 'DISARMED') "
    "ORDER BY adapter_id, position_id, resource, owner"
)
SQL_POSITION_GET: Final = (
    f"SELECT {POSITION_COLS} FROM arm_positions WHERE adapter_id = $1::text AND position_id = $2::text "
    "AND resource = $3::text AND owner = $4::text"
)

# ---------------------------------------------------------------------------------------------------- decisions SQL
SQL_DECISION_INSERT: Final = (
    f"INSERT INTO arm_decisions ({_names(DECISION_COLUMNS)}, epoch) "
    f"SELECT {_casts(DECISION_COLUMNS, 2)}, $1::bigint WHERE {FENCE} "
    f"ON CONFLICT ({DECISION_KEY}) DO NOTHING RETURNING decision_id, bar_end_ms"
)
SQL_HANDLES_INSERT: Final = (
    f"INSERT INTO arm_decision_handles (decision_id, bar_end_ms, {_names(HANDLE_COLUMNS)}) "
    f"SELECT $2::bigint, $3::bigint, {', '.join('u.' + c for c, _ in HANDLE_COLUMNS)} FROM unnest("
    + ", ".join(f"${i}::{t}[]" for i, (_, t) in enumerate(HANDLE_COLUMNS, 4))
    + f") AS u({_names(HANDLE_COLUMNS)}) WHERE {FENCE}"
)
#: Set `notice_id` ($2; 0 = the suppressed sentinel) on the referenced decisions that still have none.
SQL_DECISIONS_ATTACH: Final = (
    "UPDATE arm_decisions d SET notice_id = $2::bigint FROM unnest($3::bigint[], $4::bigint[]) AS r(id, bar) "
    f"WHERE d.decision_id = r.id AND d.bar_end_ms = r.bar AND d.notice_id IS NULL AND {FENCE}"
)
DECISION_COLS: Final = "d.decision_id, " + ", ".join(f"d.{c}" for c, _ in DECISION_COLUMNS) + ", d.epoch"
#: The committed final bar-end decisions from bar end `$1` on (go-live gate `corrections_restart`, plan S39b): the
#: BarClock's correction window rebuilt after a restart or a takeover. Ids, codes and keyed input hashes are what
#: it uses; at most `$2` rows, NEWEST first (a truncated read drops the oldest bars, whose windows end first; the
#: caller re-sorts them oldest first).
SQL_RECENT_FINAL_DECISIONS: Final = (
    f"SELECT {DECISION_COLS} FROM arm_decisions d WHERE d.bar_end_ms >= $1::bigint "
    "AND d.event IN ('POST', 'LATE', 'CORRECTED') AND d.outcome <> 'UNRESOLVED_RANGE' "
    "ORDER BY d.bar_end_ms DESC, d.decision_seq DESC LIMIT $2::int"
)
#: Decisions of one route (its (adapter_id, account_id) pairs, `$1` / `$2`) and bar spec (`$3`) with no notice yet,
#: from bar end `$4` on (bounds the partitions read), oldest first; `$5` is the caller's limit + 1, so a page can
#: tell that more exist (`UnnoticedPage.truncated`).
SQL_DECISIONS_UNNOTICED: Final = (
    f"SELECT {DECISION_COLS} FROM arm_decisions d "
    "JOIN arm_positions p ON p.adapter_id = d.adapter_id AND p.position_id = d.position_id "
    "AND p.resource = d.resource AND p.owner = d.owner "
    "JOIN unnest($1::text[], $2::text[]) AS r(adapter_id, account_id) "
    "ON r.adapter_id = p.adapter_id AND r.account_id = p.account_id "
    "WHERE d.notice_id IS NULL AND d.bar_spec = $3::text AND d.bar_end_ms >= $4::bigint "
    "ORDER BY d.bar_end_ms, d.decided_at_ms, d.decision_id LIMIT $5::int"
)

#: This bar's committed PRE `ADJUST` (`$5` bar spec, `$6` bar end; both tables pruned to its partition): the handle
#: roles and their levels at PRE (order state), what the bar-end test re-reads as `this_bar_pre` (S36b review).
SQL_PRE_ADJUST_LEVELS: Final = (
    "SELECT h.handle_role, h.level_at_pre FROM arm_decisions d JOIN arm_decision_handles h "
    "ON h.decision_id = d.decision_id AND h.bar_end_ms = d.bar_end_ms "
    "WHERE d.adapter_id = $1::text AND d.position_id = $2::text AND d.resource = $3::text AND d.owner = $4::text "
    "AND d.bar_spec = $5::text AND d.event = 'PRE' AND d.bar_end_ms = $6::bigint AND h.bar_end_ms = $6::bigint "
    "AND d.outcome = 'ADJUST' AND h.level_at_pre IS NOT NULL ORDER BY h.handle_role"
)
#: The position's newest committed bar-end decision (every event but PRE, spans included) by `decision_seq`
#: (`arm_decisions_position_idx`), skipping `NO_BROKER_BAR` rows: a bucket the broker did not trade changes no state,
#: so it never breaks an at-target run (S38c review).
SQL_LAST_BAR_END_DECISION: Final = (
    "SELECT outcome, reason, reason_kind FROM arm_decisions WHERE adapter_id = $1::text AND position_id = $2::text "
    "AND resource = $3::text AND owner = $4::text AND bar_spec = $5::text AND event <> 'PRE' "
    "AND outcome <> 'NO_BROKER_BAR' ORDER BY decision_seq DESC LIMIT 1"
)

#: The proposed stop of the position's newest committed final `ADJUST` of a bar before `$6` (round 14, plan S38g):
#: the target of the outstanding final advice a stop move may answer, so a move made on it is never discounted by
#: AT_PRE. Order state only (`proposed_stop`); `arm_decisions_position_idx`.
SQL_FINAL_ADVICE_TARGET: Final = (
    "SELECT proposed_stop FROM arm_decisions WHERE adapter_id = $1::text AND position_id = $2::text "
    "AND resource = $3::text AND owner = $4::text AND bar_spec = $5::text "
    "AND event IN ('POST', 'LATE', 'REOPEN', 'CORRECTED') AND outcome = 'ADJUST' AND bar_end_ms < $6::bigint "
    "AND proposed_stop IS NOT NULL ORDER BY decision_seq DESC LIMIT 1"
)
#: The newest final bar-end decision of a bar before `$6` (any outcome but `NO_BROKER_BAR`; by bar end, then
#: `decision_seq`, so a revision of a bar outranks its POST): its outcome and proposed stop (round 14 review: a REOPEN
#: or CORRECTED re-run takes the final advice target only from an ADJUST here). Order state only.
SQL_PRIOR_FINAL_DECISION: Final = (
    "SELECT outcome, proposed_stop FROM arm_decisions WHERE adapter_id = $1::text AND position_id = $2::text "
    "AND resource = $3::text AND owner = $4::text AND bar_spec = $5::text AND event <> 'PRE' "
    "AND outcome <> 'NO_BROKER_BAR' AND bar_end_ms < $6::bigint ORDER BY bar_end_ms DESC, decision_seq DESC LIMIT 1"
)

#: `arm replay --rerun` (plan S39b, go-live gate `replay_rerun`): the same four memory reads as they stood just before
#: a stored decision (`decision_seq < $7`), so a past bar is re-run on the memory it was decided on. Read-only.
SQL_PRE_ADJUST_LEVELS_BEFORE: Final = SQL_PRE_ADJUST_LEVELS.replace(
    "AND d.outcome = 'ADJUST'", "AND d.outcome = 'ADJUST' AND d.decision_seq < $7::bigint")
SQL_LAST_BAR_END_DECISION_BEFORE: Final = SQL_LAST_BAR_END_DECISION.replace(
    "AND outcome <> 'NO_BROKER_BAR'", "AND outcome <> 'NO_BROKER_BAR' AND decision_seq < $6::bigint")
SQL_FINAL_ADVICE_TARGET_BEFORE: Final = SQL_FINAL_ADVICE_TARGET.replace(
    "AND proposed_stop IS NOT NULL", "AND proposed_stop IS NOT NULL AND decision_seq < $7::bigint")
SQL_PRIOR_FINAL_DECISION_BEFORE: Final = SQL_PRIOR_FINAL_DECISION.replace(
    "AND bar_end_ms < $6::bigint", "AND bar_end_ms < $6::bigint AND decision_seq < $7::bigint")

#: The handles one decision read (`arm replay --rerun`, plan S39b): `(handle_id, level_read)`, order state only.
SQL_DECISION_HANDLES_READ: Final = (
    "SELECT handle_id, level_read FROM arm_decision_handles WHERE decision_id = $1::bigint "
    "AND bar_end_ms = $2::bigint ORDER BY handle_id"
)

# ---------------------------------------------------------------------------------------------------- notices, audit
SQL_NOTICE_INSERT: Final = (
    "INSERT INTO arm_notices (route, bar_spec, event, bar_end_ms, part, item_key, decision_count, created_at_ms, "
    "epoch) SELECT $2::text, $3::text, $4::text, $5::bigint, $6::smallint, $7::text, $8::int, $9::bigint, "
    f"$1::bigint WHERE {FENCE} ON CONFLICT (route, bar_spec, event, bar_end_ms, part, item_key) DO NOTHING "
    "RETURNING notice_id"
)
SQL_CORRECTION_AUDIT_INSERT: Final = (
    "INSERT INTO arm_correction_audit (decision_id, bar_end_ms, adapter_id, position_id, result, reason, "
    "recorded_at_ms, epoch) SELECT $2::bigint, $3::bigint, $4::text, $5::text, $6::text, $7::text, $8::bigint, "
    f"$1::bigint WHERE {FENCE} ON CONFLICT (decision_id, bar_end_ms) DO NOTHING"
)


# ---------------------------------------------------------------------------------------------------- mapping
def _key_values(k: PositionKey) -> list[object]:
    return [k.adapter_id, k.position_id, k.resource, k.owner]


def _position_values(row: ArmPositionRow) -> dict[str, object]:
    return {
        "state": row.state, "prior_state": row.prior_state, "state_reason": row.state_reason,
        "edge_code": row.edge_code, "attribution_source": row.attribution_source,
        "opened_by_client_order_id": row.opened_by_client_order_id, "strategy_code": row.strategy_code,
        "strategy_version": row.strategy_version, "strategy_kind": row.strategy_kind,
        "strategy_impl_version": row.strategy_impl_version, "strategy_params": row.strategy_params_json,
        "guards": row.guards_json, "guards_hash": row.guards_hash, "indicator_specs": row.indicator_specs_json,
        "required_history": row.required_history_json, "bar_spec": row.bar_spec,
        "config_doc_hash": row.config_doc_hash, "entry_level": row.entry_level, "entry_time_ms": row.entry_time_ms,
        "entry_time_source": row.entry_time_source, "s0_level": row.s0_level, "s0_source": row.s0_source,
        "pinned_units": row.pinned_units, "strategy_state": row.strategy_state_json,
        "last_decided_bar": row.last_decided_bar, "last_decided_hash": row.last_decided_hash,
        "pending_event": row.pending_event, "pending_bar_end_ms": row.pending_bar_end_ms,
        "reopen_bar_end_ms": row.reopen_bar_end_ms, "decision_seq": row.decision_seq,
        "pre_basis": row.pre_basis_json, "pre_basis_bar": row.pre_basis_bar, "updated_at_ms": row.updated_at_ms,
        "ended_at_ms": row.ended_at_ms,
    }


def position_insert_args(row: ArmPositionRow) -> list[object]:
    """The parameters `$2...` of `SQL_POSITION_INSERT`."""
    values = _position_values(row)
    fixed = [row.account_id, row.symbol, row.side, row.armed_at_ms]
    return [*_key_values(row.key), *fixed, row.row_version, *(values[c] for c, _ in POSITION_MUTABLE_COLUMNS)]


def position_cas_args(cas: PositionCas) -> list[object]:
    """The parameters `$2...` of `SQL_POSITION_CAS`."""
    values = _position_values(cas.row)
    return [*(values[c] for c, _ in POSITION_MUTABLE_COLUMNS), *_key_values(cas.row.key), cas.expected_row_version]


def _dec(v: object) -> Decimal:
    return v if isinstance(v, Decimal) else Decimal(str(v))


def _opt_dec(v: object) -> Decimal | None:
    return None if v is None else _dec(v)


def _opt_int(v: object) -> int | None:
    return None if v is None else int(cast(Any, v))


def _opt_str(v: object) -> str | None:
    return None if v is None else str(v).strip()


def _texts(v: object) -> tuple[str, ...]:
    return tuple(str(x) for x in cast(Sequence[object], v or ()))


def position_from_row(r: Row) -> ArmPositionRow:
    """Decode one `POSITION_COLS` row."""
    d: Mapping[str, Any] = dict(zip((c for c, _ in POSITION_SELECT_COLUMNS), r))
    return ArmPositionRow(
        key=PositionKey(adapter_id=str(d["adapter_id"]), position_id=str(d["position_id"]),
                        resource=str(d["resource"]), owner=str(d["owner"])),
        account_id=str(d["account_id"]), symbol=str(d["symbol"]), side=cast(Side, str(d["side"])),
        armed_at_ms=int(d["armed_at_ms"]), row_version=int(d["row_version"]), state=str(d["state"]),
        prior_state=_opt_str(d["prior_state"]), state_reason=_opt_str(d["state_reason"]),
        edge_code=str(d["edge_code"]), attribution_source=str(d["attribution_source"]),
        opened_by_client_order_id=_opt_str(d["opened_by_client_order_id"]), strategy_code=str(d["strategy_code"]),
        strategy_version=int(d["strategy_version"]), strategy_kind=str(d["strategy_kind"]),
        strategy_impl_version=int(d["strategy_impl_version"]), strategy_params_json=str(d["strategy_params"]),
        guards_json=str(d["guards"]), guards_hash=str(d["guards_hash"]),
        indicator_specs_json=str(d["indicator_specs"]), required_history_json=str(d["required_history"]),
        bar_spec=str(d["bar_spec"]), config_doc_hash=str(d["config_doc_hash"]).strip(),
        entry_level=_dec(d["entry_level"]), entry_time_ms=int(d["entry_time_ms"]),
        entry_time_source=str(d["entry_time_source"]), s0_level=_dec(d["s0_level"]), s0_source=str(d["s0_source"]),
        pinned_units=_dec(d["pinned_units"]), strategy_state_json=str(d["strategy_state"]),
        last_decided_bar=_opt_int(d["last_decided_bar"]), last_decided_hash=_opt_str(d["last_decided_hash"]),
        pending_event=_opt_str(d["pending_event"]), pending_bar_end_ms=_opt_int(d["pending_bar_end_ms"]),
        reopen_bar_end_ms=_opt_int(d["reopen_bar_end_ms"]), decision_seq=int(d["decision_seq"]),
        pre_basis_json=_opt_str(d["pre_basis"]), pre_basis_bar=_opt_int(d["pre_basis_bar"]),
        updated_at_ms=int(d["updated_at_ms"]), ended_at_ms=_opt_int(d["ended_at_ms"]), epoch=int(d["epoch"]),
    )


def decision_args(d: DecisionRow) -> list[object]:
    """The parameters `$2...` of `SQL_DECISION_INSERT`."""
    k = d.key
    return [d.bar_end_ms, k.adapter_id, k.position_id, k.resource, k.owner, d.bar_spec, d.event, d.revision,
            d.decision_seq, d.decided_at_ms, d.edge_code, d.outcome, d.category, d.reason, d.reason_kind,
            d.trigger_met, d.borderline, d.advice, d.proposed_stop, d.current_risk_r, d.first_bar_ratio,
            list(d.quality_flags), list(d.caveats), d.close_source, d.stop_basis, list(d.series_inputs),
            list(d.indicator_inputs), d.inputs_digest, d.envelope_version, d.envelope_params_hash, d.strategy_kind,
            d.strategy_impl_version, d.guards_hash, d.supersedes_decision_id, d.supersedes_bar_end_ms,
            d.range_from_bucket, d.notice_id]


def decision_from_row(r: Row) -> StoredDecision:
    """Decode one `DECISION_COLS` row."""
    d: Mapping[str, Any] = dict(zip(("decision_id", *(c for c, _ in DECISION_COLUMNS), "epoch"), r))
    row = DecisionRow(
        key=PositionKey(adapter_id=str(d["adapter_id"]), position_id=str(d["position_id"]),
                        resource=str(d["resource"]), owner=str(d["owner"])),
        bar_end_ms=int(d["bar_end_ms"]), bar_spec=str(d["bar_spec"]), event=str(d["event"]),
        revision=int(d["revision"]), decision_seq=int(d["decision_seq"]), decided_at_ms=int(d["decided_at_ms"]),
        edge_code=str(d["edge_code"]), outcome=str(d["outcome"]), category=str(d["category"]),
        reason=_opt_str(d["reason"]), reason_kind=_opt_str(d["reason_kind"]),
        trigger_met=None if d["trigger_met"] is None else bool(d["trigger_met"]), borderline=bool(d["borderline"]),
        advice=bool(d["advice"]), proposed_stop=_opt_dec(d["proposed_stop"]),
        current_risk_r=_opt_dec(d["current_risk_r"]), first_bar_ratio=_opt_dec(d["first_bar_ratio"]),
        quality_flags=_texts(d["quality_flags"]), caveats=_texts(d["caveats"]),
        close_source=_opt_str(d["close_source"]), stop_basis=_opt_str(d["stop_basis"]),
        series_inputs=_texts(d["series_inputs"]), indicator_inputs=_texts(d["indicator_inputs"]),
        inputs_digest=_opt_str(d["inputs_digest"]), envelope_version=int(d["envelope_version"]),
        envelope_params_hash=str(d["envelope_params_hash"]), strategy_kind=str(d["strategy_kind"]),
        strategy_impl_version=int(d["strategy_impl_version"]), guards_hash=str(d["guards_hash"]),
        supersedes_decision_id=_opt_int(d["supersedes_decision_id"]),
        supersedes_bar_end_ms=_opt_int(d["supersedes_bar_end_ms"]),
        range_from_bucket=_opt_int(d["range_from_bucket"]), notice_id=_opt_int(d["notice_id"]), epoch=int(d["epoch"]),
    )
    return StoredDecision(ref=DecisionRef(decision_id=int(d["decision_id"]), bar_end_ms=row.bar_end_ms), row=row)


def handle_arrays(handles: Sequence[DecisionHandleRow]) -> list[list[object]]:
    """Column arrays (`HANDLE_COLUMNS` order) for the unnest insert."""
    return [[getattr(h, c) for h in handles] for c, _ in HANDLE_COLUMNS]


def _ref_arrays(refs: Sequence[DecisionRef]) -> tuple[list[int], list[int]]:
    return [r.decision_id for r in refs], [r.bar_end_ms for r in refs]


class _CasMiss(Exception):
    """Raised inside a decision's savepoint so `FencedTx.savepoint()` rolls back only that savepoint."""


# ---------------------------------------------------------------------------------------------------- the store
class PgArmTxn(ArmStateTxn):
    """`ArmStateTxn` over one `FencedTx` (the caller's transaction)."""

    def __init__(self, tx: FencedTx) -> None:
        self.tx = tx

    async def insert_position(self, row: ArmPositionRow) -> bool:
        return await self.tx.write(SQL_POSITION_INSERT, *position_insert_args(row)) > 0

    async def update_position(self, cas: PositionCas) -> bool:
        # its own savepoint (§B5.3): an error here (a statement timeout, a CHECK violation) would otherwise abort the
        # event's whole transaction and lose every other position's decision in it
        async with self.tx.savepoint() as sp:
            return await sp.write(SQL_POSITION_CAS, *position_cas_args(cas)) > 0

    async def record_decision(self, decision: DecisionRow, handles: Sequence[DecisionHandleRow] = (), *,
                              position: PositionCas | None = None) -> DecisionWrite:
        if position is not None and position.row.key != decision.key:
            raise ValueError("the position CAS must be the decided position's row")
        try:
            async with self.tx.savepoint() as sp:
                got = await sp.write_returning(SQL_DECISION_INSERT, *decision_args(decision))
                if got is None:
                    return DecisionWrite(DecisionWriteStatus.ALREADY_DECIDED)
                ref = DecisionRef(decision_id=int(got[0]), bar_end_ms=int(got[1]))
                if handles:
                    await sp.write(SQL_HANDLES_INSERT, ref.decision_id, ref.bar_end_ms, *handle_arrays(handles),
                                   must=True)
                if position is not None and await sp.write(SQL_POSITION_CAS, *position_cas_args(position)) == 0:
                    raise _CasMiss()
        except _CasMiss:
            return DecisionWrite(DecisionWriteStatus.CAS_MISS)
        return DecisionWrite(DecisionWriteStatus.INSERTED, ref)

    async def insert_notice(self, notice: NoticeRow) -> int | None:
        got = await self.tx.write_returning(
            SQL_NOTICE_INSERT, notice.route, notice.bar_spec, notice.event, notice.bar_end_ms, notice.part,
            notice.item_key, notice.decision_count, notice.created_at_ms)
        return None if got is None else int(got[0])

    async def _attach(self, notice_id: int, refs: Sequence[DecisionRef]) -> int:
        if not refs:
            return 0
        return await self.tx.write(SQL_DECISIONS_ATTACH, notice_id, *_ref_arrays(refs))

    async def attach_notice(self, notice_id: int, refs: Sequence[DecisionRef]) -> int:
        if notice_id <= SUPPRESSED_NOTICE_ID:
            raise ValueError("a real notice id starts at 1 (0 is the suppressed sentinel)")
        return await self._attach(notice_id, refs)

    async def suppress(self, refs: Sequence[DecisionRef]) -> int:
        return await self._attach(SUPPRESSED_NOTICE_ID, refs)

    async def insert_correction_audit(self, row: CorrectionAuditRow) -> bool:
        return await self.tx.write(
            SQL_CORRECTION_AUDIT_INSERT, row.ref.decision_id, row.ref.bar_end_ms, row.adapter_id, row.position_id,
            row.result, row.reason, row.recorded_at_ms) > 0

    async def unnoticed_decisions(self, route: Sequence[tuple[str, str]], bar_spec: str, *, since_bar_end_ms: int,
                                  limit: int = 500) -> UnnoticedPage:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        if not route:
            return UnnoticedPage(decisions=(), truncated=False)
        rows = await self.tx.fetch(SQL_DECISIONS_UNNOTICED, [a for a, _ in route], [b for _, b in route], bar_spec,
                                   since_bar_end_ms, limit + 1)
        return UnnoticedPage(decisions=tuple(decision_from_row(r) for r in rows[:limit]), truncated=len(rows) > limit)


class PgArmStateStore:
    """`ArmStateStore` over the ARM pool's `EngineDb` (role `xtnl_arm`; S31 builds the pool)."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    @asynccontextmanager
    async def _txn(self) -> AsyncIterator[ArmStateTxn]:
        async with self._db.transaction() as tx:
            yield PgArmTxn(tx)

    def transaction(self) -> AbstractAsyncContextManager[ArmStateTxn]:
        """One fenced transaction (an event, a tracker change): commits when the block ends normally."""
        return self._txn()

    async def live_positions(self) -> list[ArmPositionRow]:
        async with self._db.source.acquire() as conn:
            return [position_from_row(r) for r in await conn.fetch(SQL_POSITIONS_LIVE)]

    async def recent_final_decisions(self, since_bar_end_ms: int, limit: int) -> list[StoredDecision]:
        """`RecentDecisionReader` (plan S39b): read-only, no fence."""
        async with self._db.source.acquire() as conn:
            return [decision_from_row(r) for r in await conn.fetch(SQL_RECENT_FINAL_DECISIONS, since_bar_end_ms,
                                                                   limit)]

    async def position(self, key: PositionKey) -> ArmPositionRow | None:
        async with self._db.source.acquire() as conn:
            r = await conn.fetchrow(SQL_POSITION_GET, *_key_values(key))
        return None if r is None else position_from_row(r)

    async def decision_memory(self, key: PositionKey, bar_spec: str, bar_end_ms: int) -> DecisionMemory:
        """`ArmDecisionReader`: this bar's committed PRE `ADJUST` levels, the newest committed bar-end decision and
        the target of the newest final `ADJUST` of an earlier bar and the newest final decision of an earlier bar
        (four reads, no write)."""
        async with self._db.source.acquire() as conn:
            levels = await conn.fetch(SQL_PRE_ADJUST_LEVELS, *_key_values(key), bar_spec, bar_end_ms)
            last = await conn.fetchrow(SQL_LAST_BAR_END_DECISION, *_key_values(key), bar_spec)
            advice = await conn.fetchrow(SQL_FINAL_ADVICE_TARGET, *_key_values(key), bar_spec, bar_end_ms)
            prior = await conn.fetchrow(SQL_PRIOR_FINAL_DECISION, *_key_values(key), bar_spec, bar_end_ms)
        return _memory(levels, last, advice, prior)

    async def decision_handles(self, ref: DecisionRef) -> tuple[tuple[str, Decimal | None], ...]:
        """The handles a stored decision read, `(handle_id, level_read)` (plan S39b; read-only)."""
        async with self._db.source.acquire() as conn:
            rows = await conn.fetch(SQL_DECISION_HANDLES_READ, ref.decision_id, ref.bar_end_ms)
        return tuple((str(r[0]), None if r[1] is None else _dec(r[1])) for r in rows)

    async def decision_memory_before(self, key: PositionKey, bar_spec: str, bar_end_ms: int,
                                     decision_seq: int) -> DecisionMemory:
        """`PastDecisionReader` (plan S39b): `decision_memory` as it stood before the decision with `decision_seq`
        (the same four reads, each bounded by `decision_seq`; no write)."""
        async with self._db.source.acquire() as conn:
            levels = await conn.fetch(SQL_PRE_ADJUST_LEVELS_BEFORE, *_key_values(key), bar_spec, bar_end_ms,
                                      decision_seq)
            last = await conn.fetchrow(SQL_LAST_BAR_END_DECISION_BEFORE, *_key_values(key), bar_spec, decision_seq)
            advice = await conn.fetchrow(SQL_FINAL_ADVICE_TARGET_BEFORE, *_key_values(key), bar_spec, bar_end_ms,
                                         decision_seq)
            prior = await conn.fetchrow(SQL_PRIOR_FINAL_DECISION_BEFORE, *_key_values(key), bar_spec, bar_end_ms,
                                        decision_seq)
        return _memory(levels, last, advice, prior)


def _memory(levels: Sequence[Row], last: Row | None, advice: Row | None, prior: Row | None) -> DecisionMemory:
    """The `DecisionMemory` of the four memory reads."""
    return DecisionMemory(
        pre_levels=tuple((str(r[0]), _dec(r[1])) for r in levels),
        last_outcome=None if last is None else str(last[0]),
        last_reason=None if last is None else _opt_str(last[1]),
        last_reason_kind=None if last is None else _opt_str(last[2]),
        final_advice_target=None if advice is None or advice[0] is None else _dec(advice[0]),
        prior_final_adjust=None if prior is None or str(prior[0]) != "ADJUST" or prior[1] is None
        else _dec(prior[1]))


__all__ = [
    "DECISION_COLS", "DECISION_COLUMNS", "HANDLE_COLUMNS", "POSITION_COLS", "POSITION_INSERT_COLUMNS",
    "POSITION_MUTABLE_COLUMNS", "PgArmStateStore", "PgArmTxn", "SQL_CORRECTION_AUDIT_INSERT", "SQL_DECISIONS_ATTACH",
    "SQL_DECISIONS_UNNOTICED", "SQL_DECISION_INSERT", "SQL_FINAL_ADVICE_TARGET", "SQL_HANDLES_INSERT",
    "SQL_LAST_BAR_END_DECISION", "SQL_PRIOR_FINAL_DECISION", "SQL_PRE_ADJUST_LEVELS_BEFORE",
    "SQL_LAST_BAR_END_DECISION_BEFORE", "SQL_FINAL_ADVICE_TARGET_BEFORE", "SQL_PRIOR_FINAL_DECISION_BEFORE", "SQL_DECISION_HANDLES_READ",
    "SQL_NOTICE_INSERT", "SQL_POSITIONS_LIVE", "SQL_PRE_ADJUST_LEVELS",
    "SQL_POSITION_CAS", "SQL_POSITION_GET", "SQL_POSITION_INSERT", "SQL_RECENT_FINAL_DECISIONS", "decision_args", "decision_from_row",
    "handle_arrays", "position_cas_args", "position_from_row", "position_insert_args",
]
