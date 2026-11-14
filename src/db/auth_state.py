"""AdapterAuthStore (src/ports.py) over PostgreSQL `adapter_auth_state` (tier-persistence.md §4,
operations.md §4, §16).

AUTH_FAILED is a cluster-wide fact per (adapter, credential generation): a leader does not retry that
adapter until the generation changes (Secrets Manager version id or cTrader `token_generation`) or an
operator re-arms it. Writes are fenced (only the leader runs adapters; the operator re-arm goes
through the leader).
"""
from __future__ import annotations

from typing import Final

from src.db.epoch import EngineDb
from src.db.fence import FENCE

SQL_AUTH_GET: Final = (
    "SELECT state FROM adapter_auth_state WHERE adapter_id = $1 AND credential_generation = $2"
)
SQL_AUTH_FAILED: Final = (
    "INSERT INTO adapter_auth_state (adapter_id, credential_generation, state, attempts, detail, failed_at_ms) "
    f"SELECT $2::text, $3::text, 'AUTH_FAILED', 1, $4::text, now_ms() WHERE {FENCE} "
    "ON CONFLICT (adapter_id, credential_generation) DO UPDATE SET state = 'AUTH_FAILED', "
    "attempts = adapter_auth_state.attempts + 1, detail = EXCLUDED.detail, failed_at_ms = EXCLUDED.failed_at_ms, "
    "rearmed_by = NULL, rearmed_at_ms = NULL"
)
SQL_AUTH_REARM: Final = (
    "UPDATE adapter_auth_state SET state = 'REARMED', rearmed_by = $4, rearmed_at_ms = now_ms() "
    f"WHERE adapter_id = $2 AND credential_generation = $3 AND state = 'AUTH_FAILED' AND {FENCE}"
)


class PgAdapterAuthStore:
    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def known_bad(self, adapter_id: str, credential_generation: str) -> bool:
        async with self._db.source.acquire() as conn:
            row = await conn.fetchrow(SQL_AUTH_GET, adapter_id, credential_generation)
        return row is not None and str(row[0]) == "AUTH_FAILED"

    async def record_auth_failed(self, adapter_id: str, credential_generation: str, detail: str) -> None:
        async with self._db.transaction() as tx:
            await tx.write(SQL_AUTH_FAILED, adapter_id, credential_generation, detail[:2000], must=True)

    async def rearm(self, adapter_id: str, credential_generation: str, operator: str) -> bool:
        """Audited operator re-arm; True when a failed generation was re-armed."""
        async with self._db.transaction() as tx:
            return await tx.write(SQL_AUTH_REARM, adapter_id, credential_generation, operator) > 0
