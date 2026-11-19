"""`cluster_state` and `leader_terms` access for the node runtime (operations.md §3, §4).

* `cluster_state` is the truth for SAFE_HOLD (mirrored to the etcd key /xtnl/safe_hold by the leader),
  for the RiskSweep "at most one abdication per incident" record and for the durable broker-abdication
  memory (`broker_abdications`, migration 0004).
* Reads are plain (any node, the control pool). Every write is fenced in the same statement
  (`(SELECT epoch FROM engine_epoch WHERE id = 1) = $1`), so only the current leader records an
  abdication; the row always exists (inserted by migration 0001), so 0 rows means a lost fence.
* `leader_terms` rows of one node feed the STARTING back-off (`src/runtime/roles.py learn_backoff`).

This module returns plain rows; `src/runtime/cluster.py` adapts them to the runtime protocols.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final

from src.db.fence import FENCE, FencedTx
from src.db.pg import PgConnection, Row

SQL_READ_CLUSTER: Final = (
    "SELECT safe_hold, safe_hold_since, safe_hold_reason, last_resume_at, risksweep_incident_id, "
    "risksweep_abdicated_at, broker_abdications FROM cluster_state WHERE id = 1"
)
SQL_RISKSWEEP_ABDICATED: Final = (
    "UPDATE cluster_state SET risksweep_incident_id = $2::text, risksweep_abdicated_at = $3::bigint, "
    f"updated_at_ms = $3::bigint WHERE id = 1 AND {FENCE}"
)
SQL_RISKSWEEP_RECOVERED: Final = (
    "UPDATE cluster_state SET risksweep_incident_id = NULL, risksweep_abdicated_at = NULL, "
    f"updated_at_ms = now_ms() WHERE id = 1 AND {FENCE}"
)
SQL_BROKER_ABDICATED: Final = (
    "UPDATE cluster_state SET broker_abdications = broker_abdications || jsonb_build_object($2::text, $3::bigint), "
    f"updated_at_ms = $3::bigint WHERE id = 1 AND {FENCE}"
)
SQL_BROKER_STREAMING: Final = (
    "UPDATE cluster_state SET broker_abdications = broker_abdications - $2::text, updated_at_ms = now_ms() "
    f"WHERE id = 1 AND {FENCE}"
)
SQL_OWN_TERMS: Final = (
    "SELECT cluster_id, epoch_revision, node, started_at, reached_leader_at, ended_at, end_reason "
    "FROM leader_terms WHERE node = $1 ORDER BY started_at DESC LIMIT 50"
)

# --- SAFE_HOLD resume (operations.md §4) ---------------------------------------------------------------------
# The statement the operator CLI runs (src/ops/sql.py SQL_SAFE_HOLD_RESUME, kept identical by a test). Fenced: $1 =
# the stored epoch.
SQL_SAFE_HOLD_RESUME: Final = (
    "UPDATE cluster_state SET safe_hold = false, last_resume_at = now_ms(), resumed_by = $2::text, "
    f"updated_at_ms = now_ms() WHERE id = 1 AND safe_hold AND {FENCE} "
    "RETURNING safe_hold_since, safe_hold_reason, last_resume_at"
)
# The single-PC auto-resume's form of it (src/runtime/single_pc.py, XTNL_SINGLE_PC=1 only): it clears only the hold
# its cool-down and budget were judged for ($3 = that hold's safe_hold_since, $4 = its reason). A newer hold set
# between the decision and this UPDATE matches no row (the caller then re-judges it), never cleared unjudged.
SQL_SAFE_HOLD_AUTO_RESUME: Final = (
    "UPDATE cluster_state SET safe_hold = false, last_resume_at = now_ms(), resumed_by = $2::text, "
    "updated_at_ms = now_ms() WHERE id = 1 AND safe_hold AND safe_hold_since IS NOT DISTINCT FROM $3::bigint "
    f"AND safe_hold_reason IS NOT DISTINCT FROM $4::text AND {FENCE} "
    "RETURNING safe_hold_since, safe_hold_reason, last_resume_at"
)
# operator_audit (migration 0007, append-only): the same columns the operator CLI writes (src/ops/sql.py).
SQL_AUDIT_INSERT: Final = (
    "INSERT INTO operator_audit (command, target, operator, approver, reason, outcome, detail, epoch, os_user) "
    "VALUES ($1::text, $2::text, $3::text, $4::text, $5::text, $6::text, $7::text::jsonb, $8::bigint, $9::text) "
    "RETURNING audit_id, at_ms"
)
# The single-PC auto-resume budget: applied SAFE_HOLD resumes written by the auto-resume actor ($1) at or after a time
# ($2) and after the last applied resume by anyone else (a person's resume starts a fresh budget: the operator has
# judged the cause, so the next hold is not refused for auto-resumes made before that judgement). Ordered by
# audit_id (the append-only sequence), never by clock.
SQL_COUNT_AUTO_RESUMES: Final = (
    "SELECT count(*) FROM operator_audit a WHERE a.command = 'safe_hold.resume' AND a.operator = $1::text "
    "AND a.outcome = 'applied' AND a.at_ms >= $2::bigint AND a.audit_id > COALESCE((SELECT max(h.audit_id) "
    "FROM operator_audit h WHERE h.command = 'safe_hold.resume' AND h.outcome = 'applied' "
    "AND h.operator <> $1::text), 0)"
)


@dataclass(frozen=True)
class ClusterStateRow:
    safe_hold: bool = False
    safe_hold_since: int | None = None
    safe_hold_reason: str | None = None
    last_resume_at: int | None = None
    risksweep_incident_id: str | None = None
    risksweep_abdicated_at: int | None = None
    broker_abdications: Mapping[str, int] = field(default_factory=lambda: MappingProxyType({}))


def _opt_int(v: object) -> int | None:
    return None if v is None else int(str(v))


def _abdications(v: object) -> Mapping[str, int]:
    if v is None:
        return MappingProxyType({})
    if isinstance(v, (str, bytes)):
        v = json.loads(v)
    if not isinstance(v, Mapping):
        raise ValueError("cluster_state.broker_abdications is not a JSON object")
    return MappingProxyType({str(k): int(str(x)) for k, x in v.items()})


def cluster_state_from_row(r: Row | None) -> ClusterStateRow:
    """A missing row reads as the defaults (no SAFE_HOLD, nothing recorded)."""
    if r is None:
        return ClusterStateRow()
    return ClusterStateRow(
        safe_hold=bool(r[0]), safe_hold_since=_opt_int(r[1]),
        safe_hold_reason=None if r[2] is None else str(r[2]), last_resume_at=_opt_int(r[3]),
        risksweep_incident_id=None if r[4] is None else str(r[4]), risksweep_abdicated_at=_opt_int(r[5]),
        broker_abdications=_abdications(r[6]))


async def read_cluster_state(conn: PgConnection) -> ClusterStateRow:
    return cluster_state_from_row(await conn.fetchrow(SQL_READ_CLUSTER))


async def own_term_rows(conn: PgConnection, node: str) -> list[Row]:
    """This node's latest leader_terms rows (cluster_id, epoch_revision, node, started_at,
    reached_leader_at, ended_at, end_reason), newest first."""
    return await conn.fetch(SQL_OWN_TERMS, node)


# --- fenced writes (leader only; the caller opens EngineDb.transaction()) -------------------------------
async def record_risksweep_abdication(tx: FencedTx, incident_id: str, at_ms: int) -> None:
    await tx.write(SQL_RISKSWEEP_ABDICATED, incident_id, at_ms, must=True)


async def clear_risksweep_abdication(tx: FencedTx) -> None:
    await tx.write(SQL_RISKSWEEP_RECOVERED, must=True)


async def record_broker_abdication(tx: FencedTx, adapter_id: str, at_ms: int) -> None:
    await tx.write(SQL_BROKER_ABDICATED, adapter_id, at_ms, must=True)


async def clear_broker_abdication(tx: FencedTx, adapter_id: str) -> None:
    await tx.write(SQL_BROKER_STREAMING, adapter_id, must=True)
