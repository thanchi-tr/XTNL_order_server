"""`arm_status`: the per-node ARM telemetry row (design §B5.1, §B14; plan S24).

The leader writes its row every 5 s (heartbeat, epoch, running / paused, counts of armed, unmanaged and overdue
positions, FAILED scopes, error-budget counters); a standby may write its own row with `leader = false`. Node
telemetry, not engine state: written **without the epoch fence**, like `engine_runs` (`src/db/uptime_store.py`), so a
fenced node can still report itself. The upsert only moves forward (`updated_at_ms`), so a delayed write never
replaces a newer one. `counters` are codes with integer counts only, never a value.

The same row keeps this node's market-data record (§A7 `NodeMdRecord`: the `md:boot` sentinel it wrote and the
newest `last_write_ms` its writer wrote; `md_sentinel` / `md_last_write_ms`, plan S16 / S31). Its own unfenced
statements save and load it; the save only moves forward on `last_write_ms` (an equal value may carry a new
sentinel, as after a bootstrap that wrote nothing yet), and the 5 s status upsert never names those columns, so
neither write overwrites the other. A record saved before the first status write creates the row with
`updated_at_ms = 0`, which any status write then replaces. Ids and times only.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Final, cast

from src.db.arm_ports import ArmNodeRecordStore, ArmStatusRow, ArmStatusWriter, NodeMdRecordRow
from src.db.arm_schema import REASON_PATTERN
from src.db.pg import ConnectionSource, Row, json_dumps, json_loads, rowcount

CODE_PATTERN: Final = re.compile(REASON_PATTERN.strip("^$"))
MAX_COUNTERS: Final = 64

SQL_STATUS_UPSERT: Final = (
    "INSERT INTO arm_status (node, epoch, updated_at_ms, heartbeat_at_ms, leader, running, paused, pause_reason, "
    "config_doc_hash, armed_positions, unmanaged_positions, overdue_positions, failed_scopes, failed_global, counters) "
    "VALUES ($1::text, $2::bigint, $3::bigint, $4::bigint, $5::boolean, $6::boolean, $7::boolean, $8::text, $9::text, "
    "$10::int, $11::int, $12::int, $13::int, $14::boolean, $15::text::jsonb) "
    "ON CONFLICT (node) DO UPDATE SET epoch = EXCLUDED.epoch, updated_at_ms = EXCLUDED.updated_at_ms, "
    "heartbeat_at_ms = EXCLUDED.heartbeat_at_ms, leader = EXCLUDED.leader, running = EXCLUDED.running, "
    "paused = EXCLUDED.paused, pause_reason = EXCLUDED.pause_reason, config_doc_hash = EXCLUDED.config_doc_hash, "
    "armed_positions = EXCLUDED.armed_positions, unmanaged_positions = EXCLUDED.unmanaged_positions, "
    "overdue_positions = EXCLUDED.overdue_positions, failed_scopes = EXCLUDED.failed_scopes, "
    "failed_global = EXCLUDED.failed_global, counters = EXCLUDED.counters "
    "WHERE arm_status.updated_at_ms <= EXCLUDED.updated_at_ms"
)
STATUS_COLS: Final = (
    "node, epoch, updated_at_ms, heartbeat_at_ms, leader, running, paused, pause_reason, config_doc_hash, "
    "armed_positions, unmanaged_positions, overdue_positions, failed_scopes, failed_global, counters::text"
)
SQL_STATUS_ALL: Final = f"SELECT {STATUS_COLS} FROM arm_status ORDER BY node"
#: `md:boot` sentinel text (`run_id:cluster_id:node:epoch_rev.n`; the 0017 CHECK on `md_sentinel`).
MD_SENTINEL_PATTERN: Final = re.compile(
    r"[A-Za-z0-9]{1,64}:[A-Za-z0-9._-]{1,64}:[A-Za-z0-9._-]{1,64}:[0-9]{1,18}\.[0-9]{1,18}")
#: This node's market-data record: never the status columns, forward-only on `md_last_write_ms`.
SQL_MD_RECORD_SAVE: Final = (
    "INSERT INTO arm_status (node, updated_at_ms, md_sentinel, md_last_write_ms) "
    "VALUES ($1::text, 0, $2::text, $3::bigint) "
    "ON CONFLICT (node) DO UPDATE SET md_sentinel = EXCLUDED.md_sentinel, "
    "md_last_write_ms = EXCLUDED.md_last_write_ms "
    "WHERE arm_status.md_last_write_ms IS NULL OR arm_status.md_last_write_ms <= EXCLUDED.md_last_write_ms"
)
SQL_MD_RECORD_GET: Final = (
    "SELECT md_sentinel, md_last_write_ms FROM arm_status WHERE node = $1::text AND md_sentinel IS NOT NULL"
)


def validate_counters(counters: tuple[tuple[str, int], ...]) -> None:
    if len(counters) > MAX_COUNTERS:
        raise ValueError("too many counters")
    for name, value in counters:
        if not CODE_PATTERN.fullmatch(name) or isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("counters are codes with integer counts")


def status_from_row(r: Row) -> ArmStatusRow:
    raw = r[14]
    obj = json_loads(raw) if isinstance(raw, (str, bytes)) else (raw or {})
    return ArmStatusRow(
        node=str(r[0]), epoch=None if r[1] is None else int(r[1]), updated_at_ms=int(r[2]),
        heartbeat_at_ms=None if r[3] is None else int(r[3]), leader=bool(r[4]), running=bool(r[5]),
        paused=bool(r[6]), pause_reason=None if r[7] is None else str(r[7]),
        config_doc_hash=None if r[8] is None else str(r[8]).strip(), armed_positions=int(r[9]),
        unmanaged_positions=int(r[10]), overdue_positions=int(r[11]), failed_scopes=int(r[12]),
        failed_global=bool(r[13]),
        counters=tuple(sorted((str(k), int(v)) for k, v in cast(Mapping[str, Any], obj).items())))


def validate_md_record(row: NodeMdRecordRow) -> None:
    if not MD_SENTINEL_PATTERN.fullmatch(row.sentinel):
        raise ValueError("md sentinel is run_id:cluster_id:node:generation")
    if isinstance(row.last_write_ms, bool) or not isinstance(row.last_write_ms, int) or row.last_write_ms < 0:
        raise ValueError("last_write_ms is a non-negative integer")


class PgArmStatusStore(ArmStatusWriter, ArmNodeRecordStore):
    """The unfenced `arm_status` upsert and read, and this node's market-data record, over any node's connection
    source."""

    def __init__(self, source: ConnectionSource) -> None:
        self._source = source

    async def upsert(self, row: ArmStatusRow) -> bool:
        """False when a newer row of this node is stored already."""
        validate_counters(row.counters)
        async with self._source.acquire() as conn:
            status = await conn.execute(
                SQL_STATUS_UPSERT, row.node, row.epoch, row.updated_at_ms, row.heartbeat_at_ms, row.leader,
                row.running, row.paused, row.pause_reason, row.config_doc_hash, row.armed_positions,
                row.unmanaged_positions, row.overdue_positions, row.failed_scopes, row.failed_global,
                json_dumps(dict(row.counters)))
        return rowcount(status) > 0

    async def all(self) -> list[ArmStatusRow]:
        async with self._source.acquire() as conn:
            return [status_from_row(r) for r in await conn.fetch(SQL_STATUS_ALL)]

    async def save_md_record(self, row: NodeMdRecordRow) -> bool:
        """False when a record with a newer `last_write_ms` is stored already."""
        validate_md_record(row)
        async with self._source.acquire() as conn:
            status = await conn.execute(SQL_MD_RECORD_SAVE, row.node, row.sentinel, row.last_write_ms)
        return rowcount(status) > 0

    async def load_md_record(self, node: str) -> NodeMdRecordRow | None:
        async with self._source.acquire() as conn:
            r = await conn.fetchrow(SQL_MD_RECORD_GET, node)
        if r is None or r[0] is None or r[1] is None:
            return None
        return NodeMdRecordRow(node=node, sentinel=str(r[0]), last_write_ms=int(r[1]))


__all__ = ["MD_SENTINEL_PATTERN", "PgArmStatusStore", "SQL_MD_RECORD_GET", "SQL_MD_RECORD_SAVE", "SQL_STATUS_ALL",
           "SQL_STATUS_UPSERT", "STATUS_COLS", "status_from_row", "validate_counters", "validate_md_record"]
