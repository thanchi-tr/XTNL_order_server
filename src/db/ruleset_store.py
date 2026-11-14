"""`rulesets` reads and the fenced `ruleset_audit` insert (IMPLEMENTATION_PLAN 4.5; `RulesetSource` in
src/oversee/ruleset.py).

Reads are plain (no fence: they write nothing). The audit row is a state write of this term, fenced in the
same statement (operations.md §2); 0 rows can only mean a lost fence (`EpochMismatch`).
The versioned operator write path (new row + activation) is the operator CLI (IMPLEMENTATION_PLAN 4.6).
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from src.db.epoch import EngineDb
from src.db.fence import FENCE
from src.db.pg import Row
from src.oversee.ruleset import AuditOutcome, RulesetRow

_COLS: Final = "ruleset_version, config, created_by, reason, created_at_ms"
SQL_ACTIVE_VERSION: Final = "SELECT max(ruleset_version) FROM rulesets WHERE active"
SQL_ACTIVE: Final = f"SELECT {_COLS} FROM rulesets WHERE active ORDER BY ruleset_version DESC LIMIT 1"
SQL_EARLIER: Final = f"SELECT {_COLS} FROM rulesets WHERE ruleset_version < $1 ORDER BY ruleset_version DESC LIMIT $2"
SQL_AUDIT: Final = (
    "INSERT INTO ruleset_audit (ruleset_version, outcome, detail, epoch_revision, node) "
    f"SELECT $2::bigint, $3::text, $4::text, $1::bigint, $5::text WHERE {FENCE}"
)


def ruleset_from_row(r: Row) -> RulesetRow:
    return RulesetRow(int(r[0]), r[1], str(r[2] or ""), str(r[3] or ""), int(r[4] or 0))


class PgRulesetStore:
    """RulesetSource over PostgreSQL."""

    def __init__(self, db: EngineDb, node_id: str) -> None:
        self._db = db
        self._node = node_id

    async def active_version(self) -> int | None:
        async with self._db.source.acquire() as conn:
            row = await conn.fetchrow(SQL_ACTIVE_VERSION)
        return None if row is None or row[0] is None else int(row[0])

    async def active(self) -> RulesetRow | None:
        async with self._db.source.acquire() as conn:
            row = await conn.fetchrow(SQL_ACTIVE)
        return None if row is None else ruleset_from_row(row)

    async def earlier(self, below_version: int, limit: int) -> Sequence[RulesetRow]:
        async with self._db.source.acquire() as conn:
            rows = await conn.fetch(SQL_EARLIER, below_version, limit)
        return [ruleset_from_row(r) for r in rows]

    async def audit(self, version: int | None, outcome: AuditOutcome, detail: str) -> None:
        async with self._db.transaction() as tx:
            await tx.write(SQL_AUDIT, version, outcome, detail, self._node, must=True)


__all__ = ["PgRulesetStore", "ruleset_from_row"]
