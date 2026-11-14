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
