"""LEADER_INIT step 1 work inside the epoch-bump transaction, and leader_terms bookkeeping (operations.md §4).

Use: `await bump_epoch(conn, epoch, node, in_txn=lambda tx: leader_init_fence(tx, epoch, node, now_ms))`.
These statements carry no fence predicate on purpose: they run inside the bump transaction, which holds
the engine_epoch row lock and commits atomically with the new epoch, so they are fenced by construction.
DRAINING's `record_end_reason` / LEADER entry's `mark_reached_leader` run later, in an ordinary engine
transaction (`EngineDb.transaction()`), so they are ordinary engine state writes: each carries the fence
predicate in the same statement (tier-persistence.md §4 names no exception for leader_terms) and goes
through `FencedTx.write`. A stale leader that resumes after its successor bumped the epoch therefore
gets `EpochMismatch` instead of stamping `reached_leader_at` on a term already closed `init_failed`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

from src.db.epoch import Epoch
from src.db.fence import FENCE, FencedTx
from src.db.pg import PgConnection, rowcount

EndReason = Literal["switchover", "abdicate", "lost", "init_failed"]

ABNORMAL_WINDOW_MS: Final = 15 * 60_000
ABNORMAL_LIMIT: Final = 3          # 3 abnormal ends within 15 min
EPOCH_WINDOW_MS: Final = 30 * 60_000
EPOCH_CHANGE_LIMIT: Final = 6      # more than 6 epoch changes within 30 min

SQL_FENCE_ALL_STREAMS: Final = (
    "UPDATE stream_health SET conn_lost_at = COALESCE(conn_lost_at, $1::bigint), updated_at_ms = $1::bigint"
)
SQL_CLOSE_PREDECESSORS: Final = (
    "UPDATE leader_terms SET ended_at = COALESCE(ended_at, $1::bigint), "
    "end_reason = COALESCE(end_reason, CASE WHEN reached_leader_at IS NULL THEN 'init_failed' ELSE 'lost' END) "
    "WHERE (ended_at IS NULL OR end_reason IS NULL) AND NOT (cluster_id = $2 AND epoch_revision = $3)"
)
SQL_INSERT_TERM: Final = (
    "INSERT INTO leader_terms (cluster_id, epoch_revision, node, started_at) VALUES ($1, $2, $3, $4)"
)
SQL_READ_CLUSTER_STATE: Final = "SELECT safe_hold, last_resume_at FROM cluster_state WHERE id = 1 FOR UPDATE"
SQL_COUNT_ABNORMAL: Final = (
    "SELECT count(*) FROM leader_terms WHERE end_reason IN ('lost', 'init_failed') "
    "AND ended_at >= $1::bigint AND started_at >= $1::bigint AND ended_at > COALESCE($2::bigint, 0)"
)
"""A flap is a term that both started and ended abnormally inside the window. A term left open by a kill (a PC
shutdown or sleep, a crash hours ago) is closed by its successor's step 1 with `ended_at` = now; counting it by
that closing time alone made several old kills look like 3 crashes within 15 min (false SAFE_HOLD on a single PC
after a shutdown, 2026-11-18). Its start time shows it was not part of a restart loop."""
SQL_COUNT_EPOCH_CHANGES: Final = (
    "SELECT count(*) FROM leader_terms WHERE started_at >= $1::bigint AND started_at > COALESCE($2::bigint, 0)"
)
SQL_SET_SAFE_HOLD: Final = (
    "INSERT INTO cluster_state (id, safe_hold, safe_hold_since, safe_hold_reason, updated_at_ms) "
    "VALUES (1, true, $1, $2, $1) ON CONFLICT (id) DO UPDATE SET safe_hold = true, "
    "safe_hold_since = EXCLUDED.safe_hold_since, safe_hold_reason = EXCLUDED.safe_hold_reason, "
    "updated_at_ms = EXCLUDED.updated_at_ms"
)
SQL_END_TERM: Final = (
    "UPDATE leader_terms SET ended_at = $3, end_reason = $4 WHERE cluster_id = $1 AND epoch_revision = $2"
)
# Fenced ($1 = the epoch, prepended by FencedTx.write).
SQL_RECORD_END_REASON: Final = (
    "UPDATE leader_terms SET end_reason = $4::text WHERE cluster_id = $2::text AND epoch_revision = $3::bigint "
    f"AND end_reason IS NULL AND {FENCE}"
)
SQL_REACHED_LEADER: Final = (
    "UPDATE leader_terms SET reached_leader_at = $4::bigint WHERE cluster_id = $2::text "
    f"AND epoch_revision = $3::bigint AND {FENCE}"
)


@dataclass(frozen=True)
class LeaderInitFence:
    streams_fenced: int
    predecessors_closed: int
    safe_hold: bool
    safe_hold_reason: str | None = None


async def leader_init_fence(tx: PgConnection, epoch: Epoch, node: str, now_ms: int) -> LeaderInitFence:
    """Fence every stream, close predecessor terms, open this term, and set SAFE_HOLD if due.

    When SAFE_HOLD is (or becomes) set, this term is ended `init_failed` in the same transaction; the
    caller commits and then exits LEADER_INIT.
    """
    fenced = rowcount(await tx.execute(SQL_FENCE_ALL_STREAMS, now_ms))
    closed = rowcount(await tx.execute(SQL_CLOSE_PREDECESSORS, now_ms, epoch.cluster_id, epoch.revision))
    await tx.execute(SQL_INSERT_TERM, epoch.cluster_id, epoch.revision, node, now_ms)

    cs = await tx.fetchrow(SQL_READ_CLUSTER_STATE)
    already, last_resume = (bool(cs[0]), cs[1]) if cs is not None else (False, None)
    reason: str | None = "safe_hold already set" if already else None
    if reason is None:
        abn = await tx.fetchrow(SQL_COUNT_ABNORMAL, now_ms - ABNORMAL_WINDOW_MS, last_resume)
        if abn is not None and int(abn[0]) >= ABNORMAL_LIMIT:
            reason = f"{int(abn[0])} abnormal term ends within 15 min"
    if reason is None:
        chg = await tx.fetchrow(SQL_COUNT_EPOCH_CHANGES, now_ms - EPOCH_WINDOW_MS, last_resume)
        if chg is not None and int(chg[0]) > EPOCH_CHANGE_LIMIT:
            reason = f"{int(chg[0])} epoch changes within 30 min"
    if reason is not None:
        if not already:
            await tx.execute(SQL_SET_SAFE_HOLD, now_ms, reason)
        await tx.execute(SQL_END_TERM, epoch.cluster_id, epoch.revision, now_ms, "init_failed")
    return LeaderInitFence(fenced, closed, reason is not None, reason)


async def mark_reached_leader(tx: FencedTx, epoch: Epoch, now_ms: int) -> None:
    """LEADER entry: this term's row must exist, so 0 rows can only mean a lost fence (EpochMismatch)."""
    await tx.write(SQL_REACHED_LEADER, epoch.cluster_id, epoch.revision, now_ms, must=True)


async def record_end_reason(tx: FencedTx, epoch: Epoch, reason: EndReason) -> None:
    """DRAINING writes its own end_reason (switchover / abdicate); the successor closes the term.

    0 rows is legitimate (end_reason already set), so the fence is re-checked: EpochMismatch if lost."""
    await tx.write(SQL_RECORD_END_REASON, epoch.cluster_id, epoch.revision, reason)
