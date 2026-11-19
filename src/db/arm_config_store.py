"""The ARM configuration cache and audit, and the read-only operator rows (design §B9, §B13; plan S24).

* `arm_config_cache` (one row): the accepted document, the hash of every Supabase row the engine has seen and the
  sha256 chain head over them (pending = a row not seen before; tampered = a seen row changed or vanished). Fenced
  upsert, `must=False` (§B5.3).
* `arm_config_audit`: what `arm-config` did (`action` code, document hash, rows named `table:row_id:row_hash`, never
  by their text). Fenced insert, `must=False`.
* `arm_accepted` and `arm_controls` are **read only** here: they are written only by the ops CLI with an
  `operator_audit` row (§B3 static check; `tests/db/test_arm_stores_static.py`).

Configuration only: a document holds strategy codes, parameters and routes, never market data.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final, cast

from src.db.arm_ports import AcceptedRow, ArmConfigStore, ConfigAuditRow, ConfigCacheRow, ControlsRow
from src.db.epoch import EngineDb
from src.db.fence import FENCE
from src.db.pg import Row, json_dumps, json_loads

SQL_CONFIG_CACHE_UPSERT: Final = (
    "INSERT INTO arm_config_cache (id, doc_hash, schema_version, document, row_hashes, chain_head, chain_rows, "
    "updated_at_ms, epoch) SELECT 1, $2::text, $3::int, $4::text::jsonb, $5::text::jsonb, $6::text, $7::int, "
    f"$8::bigint, $1::bigint WHERE {FENCE} "
    "ON CONFLICT (id) DO UPDATE SET doc_hash = EXCLUDED.doc_hash, schema_version = EXCLUDED.schema_version, "
    "document = EXCLUDED.document, row_hashes = EXCLUDED.row_hashes, chain_head = EXCLUDED.chain_head, "
    "chain_rows = EXCLUDED.chain_rows, updated_at_ms = EXCLUDED.updated_at_ms, epoch = EXCLUDED.epoch"
)
SQL_CONFIG_CACHE_GET: Final = (
    "SELECT doc_hash, schema_version, document::text, row_hashes::text, chain_head, chain_rows, updated_at_ms "
    "FROM arm_config_cache WHERE id = 1"
)
SQL_CONFIG_AUDIT_INSERT: Final = (
    "INSERT INTO arm_config_audit (at_ms, epoch, node, action, doc_hash, row_refs) "
    f"SELECT $2::bigint, $1::bigint, $3::text, $4::text, $5::text, $6::text[] WHERE {FENCE}"
)
SQL_ACCEPTED_LATEST: Final = (
    "SELECT accepted_id, doc_hash, schema_version, mode, accepted_at_ms, accepted_by FROM arm_accepted "
    "ORDER BY accepted_at_ms DESC, accepted_id DESC LIMIT 1"
)
SQL_CONTROLS_GET: Final = (
    "SELECT paused, pause_reason, paused_at_ms, drain_notices, notify_test_at_ms, updated_at_ms FROM arm_controls "
    "WHERE id = 1"
)


def _obj(raw: object) -> Mapping[str, Any]:
    v = json_loads(raw) if isinstance(raw, (str, bytes)) else (raw or {})
    return cast(Mapping[str, Any], v)


def cache_from_row(r: Row) -> ConfigCacheRow:
    doc = r[2]
    return ConfigCacheRow(
        doc_hash=str(r[0]).strip(), schema_version=int(r[1]),
        document_json=doc if isinstance(doc, str) else json_dumps(doc),
        row_hashes=tuple(sorted((str(k), str(v)) for k, v in _obj(r[3]).items())), chain_head=str(r[4]).strip(),
        chain_rows=int(r[5]), updated_at_ms=int(r[6]))


class PgArmConfigStore(ArmConfigStore):
    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def cached(self) -> ConfigCacheRow | None:
        async with self._db.source.acquire() as conn:
            r = await conn.fetchrow(SQL_CONFIG_CACHE_GET)
        return None if r is None else cache_from_row(r)

    async def save_cache(self, row: ConfigCacheRow) -> bool:
        async with self._db.transaction() as tx:
            return await tx.write(SQL_CONFIG_CACHE_UPSERT, row.doc_hash, row.schema_version, row.document_json,
                                  json_dumps(dict(row.row_hashes)), row.chain_head, row.chain_rows,
                                  row.updated_at_ms) > 0

    async def audit(self, row: ConfigAuditRow) -> bool:
        async with self._db.transaction() as tx:
            return await tx.write(SQL_CONFIG_AUDIT_INSERT, row.at_ms, row.node, row.action, row.doc_hash,
                                  list(row.row_refs)) > 0

    async def latest_accepted(self) -> AcceptedRow | None:
        async with self._db.source.acquire() as conn:
            r = await conn.fetchrow(SQL_ACCEPTED_LATEST)
        if r is None:
            return None
        return AcceptedRow(accepted_id=int(r[0]), doc_hash=str(r[1]).strip(), schema_version=int(r[2]),
                           mode=str(r[3]), accepted_at_ms=int(r[4]), accepted_by=str(r[5]))

    async def controls(self) -> ControlsRow | None:
        """None when the singleton row is missing (the caller treats controls as unreadable: hold, never a pause)."""
        async with self._db.source.acquire() as conn:
            r = await conn.fetchrow(SQL_CONTROLS_GET)
        if r is None:
            return None
        return ControlsRow(paused=bool(r[0]), pause_reason=None if r[1] is None else str(r[1]),
                           paused_at_ms=None if r[2] is None else int(r[2]), drain_notices=bool(r[3]),
                           notify_test_at_ms=None if r[4] is None else int(r[4]), updated_at_ms=int(r[5]))


__all__ = ["PgArmConfigStore", "SQL_ACCEPTED_LATEST", "SQL_CONFIG_AUDIT_INSERT", "SQL_CONFIG_CACHE_GET",
           "SQL_CONFIG_CACHE_UPSERT", "SQL_CONTROLS_GET", "cache_from_row"]
