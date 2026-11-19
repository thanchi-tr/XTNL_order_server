"""The durable D7 error-budget counters and the unclean-restart history (design §B14; plan S38f; migration 0020).

* `PgArmBudgetStore` (ARM pool, role `xtnl_arm`): `load()` reads every `arm_budget_counters` row; `save(rows)` upserts
  the changed ones in **one fenced transaction** (each statement `WHERE (SELECT epoch FROM engine_epoch) = $1`; a
  0-row upsert means the fence is lost: `EpochMismatch`). The FAILED manager calls it only on change, and writes a
  reset counter with count 0 instead of deleting it (the role has no DELETE).
* `PgRestartHistory` (engine pool): the end times of the leader terms that ended `lost` or `init_failed` (an engine
  that crashed, was killed or fenced, or a takeover after one), the input of the unclean-restart counter. A clean stop
  or a planned switchover ends its term `switchover` / `abdicate` and never counts. Nor does a term the SAFE_HOLD
  brake aborted (R14 review): `leader_init_fence` ends its own new term `init_failed` in the bump transaction that
  finds SAFE_HOLD due (or already set), so that term never reached LEADER and has `ended_at = started_at`; it is a
  deliberate stop, not a crash. The crashes that tripped SAFE_HOLD still count: SAFE_HOLD preempts (no node leads,
  no ARM runs) and, after the operator's resume, the ARM goes FAILED `unclean_restarts` if they still reach the limit
  within its window (design §B14; `resume()` clears SAFE_HOLD, never the ARM's error budget).

Rows hold scope ids, counter codes, counts and times only: never a tick, bar, quote, spread or indicator value.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Final

from src.db.arm_ports import BudgetCounterRow
from src.db.epoch import EngineDb
from src.db.fence import FENCE
from src.db.pg import ConnectionSource

SQL_BUDGET_LOAD: Final = (
    "SELECT scope_kind, scope_id, counter, count, streak_start_ms, last_event_ms FROM arm_budget_counters "
    "ORDER BY scope_kind, scope_id, counter"
)
SQL_BUDGET_UPSERT: Final = (
    "INSERT INTO arm_budget_counters (scope_kind, scope_id, counter, count, streak_start_ms, last_event_ms, epoch, "
    "updated_at_ms) SELECT $2::text, $3::text, $4::text, $5::bigint, $6::bigint, $7::bigint, $1::bigint, $8::bigint "
    f"WHERE {FENCE} ON CONFLICT (scope_kind, scope_id, counter) DO UPDATE SET count = EXCLUDED.count, "
    "streak_start_ms = EXCLUDED.streak_start_ms, last_event_ms = EXCLUDED.last_event_ms, epoch = EXCLUDED.epoch, "
    "updated_at_ms = EXCLUDED.updated_at_ms"
)
SQL_UNCLEAN_ENDS: Final = (
    "SELECT ended_at FROM leader_terms WHERE end_reason IN ('lost', 'init_failed') AND ended_at >= $1::bigint "
    "AND started_at >= $1::bigint "
    "AND NOT (end_reason = 'init_failed' AND reached_leader_at IS NULL AND ended_at = started_at) "
    "ORDER BY ended_at"
)
"""The unclean ends since `$1`, without a term the SAFE_HOLD brake aborted in its own LEADER_INIT step 1
(`src.db.leader.leader_init_fence`: `SQL_INSERT_TERM` and `SQL_END_TERM` with the same `now_ms`)."""


def _opt(v: object) -> int | None:
    return None if v is None else int(str(v))


class PgArmBudgetStore:
    """`ArmBudgetStore` over the ARM pool's fenced `EngineDb`."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def load(self) -> list[BudgetCounterRow]:
        async with self._db.source.acquire() as conn:
            rows = await conn.fetch(SQL_BUDGET_LOAD)
        return [BudgetCounterRow(scope_kind=str(r[0]), scope_id=str(r[1]), counter=str(r[2]), count=int(r[3]),
                                 streak_start_ms=_opt(r[4]), last_event_ms=_opt(r[5])) for r in rows]

    async def save(self, rows: Sequence[BudgetCounterRow], now_ms: int) -> None:
        if not rows:
            return
        async with self._db.transaction() as tx:
            for row in rows:
                if row.count < 0:
                    raise ValueError("a budget counter is never negative")
                await tx.write(SQL_BUDGET_UPSERT, row.scope_kind, row.scope_id, row.counter, row.count,
                               row.streak_start_ms, row.last_event_ms, now_ms, must=True)


class PgRestartHistory:
    """`RestartHistoryPort` over the engine pool (a read; `leader_terms` is not granted to `xtnl_arm`)."""

    def __init__(self, source: ConnectionSource) -> None:
        self._source = source

    async def unclean_ends_since(self, since_ms: int) -> list[int]:
        async with self._source.acquire() as conn:
            rows = await conn.fetch(SQL_UNCLEAN_ENDS, since_ms)
        return [int(r[0]) for r in rows if r[0] is not None]


if TYPE_CHECKING:
    from src.db.arm_ports import ArmBudgetStore, RestartHistoryPort

    def _ports(b: PgArmBudgetStore, h: PgRestartHistory) -> tuple[ArmBudgetStore, RestartHistoryPort]:
        return b, h


__all__ = ["PgArmBudgetStore", "PgRestartHistory", "SQL_BUDGET_LOAD", "SQL_BUDGET_UPSERT", "SQL_UNCLEAN_ENDS"]
