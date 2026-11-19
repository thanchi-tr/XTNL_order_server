"""Running on a schema older than 0015 (plan S38f; design §0b "What turns each on").

Migrations above `XTNL_MIGRATIONS_ACCEPTED_UPTO` (default 14, `src/db/migrate.py`) are held back until the user has
reviewed them, so the engine must start and behave exactly as before on a 0014-level schema with every flag off. The
core paths (ingestion's entity, `event_log` and instrument writes, the RiskSweep snapshot, the integrity worker, the
orchestrator's local-state read, the dashboard) were extended by 0015 (`orders_state.order_kind`,
`instruments.price_increment` / `min_stop_distance` / `asset_class`, `event_log.origin` / `broker_time_ms`). Every
other column or table added by 0016-0020 is read or written only by the market-data / ARM code, which stays unused
while `XTNL_MD_ENABLED` / `XTNL_ARM_ENABLED` are off (the lifecycle's ARM-notice expiry step is skipped while 0017's
tables are absent, `src/db/lifecycle.py`).

Detection, once per pool at creation (`detect_schema`): a catalog read of the 0015 columns (`pg_attribute`, so it
works for every role, the read-only dashboard role included). A database without `orders_state` (nothing migrated
yet) counts as current: there is no old schema to be compatible with. A pool created on an older schema gets a
`LegacyRewriter`; the process remembers it (`legacy_schema_seen()`), and the ingestion orchestrator then publishes
every DTO in its pre-0015 shape (`src.dto.versioning.without_optional_payload`: `order_kind` and the PR7 instrument
fields None), so the stored checksums are the ones the pre-0015 code wrote and the integrity worker's recomputed
checksums agree with them.

`LegacyRewriter` maps a statement to its 0014 form, by exact text (memoised):
* the writes that name a 0015 column (`REWRITES`): the column and its parameter are removed, and the argument at that
  position is dropped (a statement's `$n` are renumbered by building it from a table definition without the column);
* the reads (`READ_FRAGMENTS`): the decoder column lists `ORDER_COLS` / `INSTRUMENT_COLS` and the dashboard's order
  list select `NULL` for the absent columns, so every row decoder is unchanged.
A statement with no 0015 column passes through untouched; `tests/db/test_schema_compat.py` scans every SQL constant of
the core modules for one the rewriter would miss.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Final

from src.db.pg import PgConnection, Row

log = logging.getLogger(__name__)

#: The 0015 columns on the core paths: (table, column).
COLUMNS_0015: Final[tuple[tuple[str, str], ...]] = (
    ("orders_state", "order_kind"),
    ("instruments", "price_increment"), ("instruments", "min_stop_distance"), ("instruments", "asset_class"),
    ("event_log", "origin"), ("event_log", "broker_time_ms"),
)
#: Identifiers added by 0015-0020 that a core-path statement must not name on an older schema (the scan's list).
NEW_IDENTIFIERS: Final = frozenset({
    "order_kind", "price_increment", "min_stop_distance", "asset_class", "broker_time_ms", "expires_at_ms",
    "superseded_by"})

#: One row: (orders_state exists, it has every 0015 column checked). to_regclass: a missing table is NULL.
SQL_SCHEMA_0015: Final = (
    "SELECT to_regclass('orders_state') IS NOT NULL, "
    "(SELECT count(*) FROM pg_attribute a WHERE NOT a.attisdropped AND a.attnum > 0 AND "
    "((a.attrelid = to_regclass('orders_state') AND a.attname = 'order_kind') OR "
    "(a.attrelid = to_regclass('instruments') AND a.attname IN ('price_increment', 'min_stop_distance', "
    "'asset_class')) OR "
    "(a.attrelid = to_regclass('event_log') AND a.attname IN ('origin', 'broker_time_ms'))))::int"
)


@dataclass(frozen=True)
class SchemaLevel:
    """What the catalog showed: `migrated` (orders_state exists) and `prereqs` (every 0015 column is there)."""

    migrated: bool
    prereqs: bool

    @property
    def legacy(self) -> bool:
        """A migrated schema without the 0015 columns: the 0014 compatibility mode."""
        return self.migrated and not self.prereqs


CURRENT: Final = SchemaLevel(migrated=True, prereqs=True)


@dataclass(frozen=True)
class Rewrite:
    """A statement's 0014 form and the argument positions (0-based, `$1` is position 0) it no longer takes."""

    sql: str
    drop: tuple[int, ...] = ()


def _legacy_writes() -> dict[str, Rewrite]:
    """The core writes that name a 0015 column, mapped to their 0014 form (built from the real constants)."""
    from src.db import repository as R
    from src.db.entity_sql import ORDERS

    out: dict[str, Rewrite] = {}
    # orders_state.order_kind: the same table definition without the column; its argument follows the key columns
    # and the payload columns before it ($1 is the epoch).
    orders_0014 = replace(ORDERS, payload_cols=tuple(c for c in ORDERS.payload_cols if c != "order_kind"))
    pos = 1 + len(ORDERS.key_cols) + ORDERS.payload_cols.index("order_kind")
    out[ORDERS.insert_new] = Rewrite(orders_0014.insert_new, (pos,))
    out[ORDERS.update_apply] = Rewrite(orders_0014.update_apply, (pos,))
    # event_log.origin / broker_time_ms: $11 and $12, the last two arguments.
    ev = R.SQL_EVENT_LOG
    ev_0014 = ev.replace(", origin, broker_time_ms)", ")", 1).replace(", $11::text, $12::bigint WHERE", " WHERE", 1)
    out[ev] = Rewrite(ev_0014, (10, 11))
    # instruments.price_increment / min_stop_distance / asset_class: $17-$19, the last three arguments.
    ins = R.SQL_INSTRUMENT_UPSERT
    ins_0014 = (ins.replace(", price_increment, min_stop_distance, asset_class) ", ") ", 1)
                .replace(", $17::numeric, $18::numeric, $19::text ", " ", 1)
                .replace("price_increment = EXCLUDED.price_increment, min_stop_distance = EXCLUDED.min_stop_distance, "
                         "asset_class = EXCLUDED.asset_class, ", "", 1))
    out[ins] = Rewrite(ins_0014, (16, 17, 18))
    for full, rw in out.items():
        if rw.sql == full:
            raise RuntimeError("a 0014 rewrite did not change its statement")
    return out


def _read_fragments() -> tuple[tuple[str, str], ...]:
    """Decoder column lists that name a 0015 column -> the same list selecting NULL in its place."""
    from src.db.rows import INSTRUMENT_COLS, ORDER_COLS

    order_0014 = ORDER_COLS.replace(", order_kind", ", NULL::text", 1)
    instr_0014 = INSTRUMENT_COLS.replace(", price_increment, min_stop_distance, asset_class",
                                         ", NULL::numeric, NULL::numeric, NULL::text", 1)
    # The dashboard's order list (src/dashboard/views.py SQL_ORDERS) names the column in its own list.
    dash = ("linked_position_id, order_kind, seq_id", "linked_position_id, NULL::text AS order_kind, seq_id")
    out = ((ORDER_COLS, order_0014), (INSTRUMENT_COLS, instr_0014), dash)
    if any(a == b for a, b in out):
        raise RuntimeError("a 0014 read fragment did not change")
    return out


class LegacyRewriter:
    """Statement text -> its 0014 form (exact text, memoised; anything else passes through)."""

    def __init__(self) -> None:
        self._writes: Mapping[str, Rewrite] = _legacy_writes()
        self._reads = _read_fragments()
        self._memo: dict[str, Rewrite | None] = {}

    def _lookup(self, sql: str) -> Rewrite | None:
        if sql in self._memo:
            return self._memo[sql]
        rw = self._writes.get(sql)
        if rw is None:
            text = sql
            for full, old in self._reads:
                if full in text:
                    text = text.replace(full, old)
            rw = None if text == sql else Rewrite(text)
        if len(self._memo) < 4096:
            self._memo[sql] = rw
        return rw

    def rewrite(self, sql: str, args: tuple[object, ...]) -> tuple[str, tuple[object, ...]]:
        rw = self._lookup(sql)
        if rw is None:
            return sql, args
        if not rw.drop:
            return rw.sql, args
        return rw.sql, tuple(a for i, a in enumerate(args) if i not in rw.drop)


class LegacyConnection:
    """A PgConnection that runs every statement in its 0014 form."""

    def __init__(self, inner: PgConnection, rewriter: LegacyRewriter) -> None:
        self._inner = inner
        self._rw = rewriter

    async def execute(self, sql: str, *args: object) -> str:
        sql, a = self._rw.rewrite(sql, args)
        return await self._inner.execute(sql, *a)

    async def fetchrow(self, sql: str, *args: object) -> Row | None:
        sql, a = self._rw.rewrite(sql, args)
        return await self._inner.fetchrow(sql, *a)

    async def fetch(self, sql: str, *args: object) -> list[Row]:
        sql, a = self._rw.rewrite(sql, args)
        return await self._inner.fetch(sql, *a)


# ---- process state ---------------------------------------------------------------------------------------------
_legacy_seen: bool = False


def note_level(level: SchemaLevel) -> None:
    """Remember an older schema for the process (sticky until restart: a migration applied while the engine runs takes
    effect at the next start, so one process never mixes the two DTO shapes)."""
    global _legacy_seen
    if level.legacy and not _legacy_seen:
        _legacy_seen = True
        log.warning("schema: the 0015 columns are absent (migrations held back by XTNL_MIGRATIONS_ACCEPTED_UPTO); "
                    "running in 0014 compatibility mode")


def legacy_schema_seen() -> bool:
    return _legacy_seen


def optional_payload_stored() -> bool:
    """False once a pool saw a pre-0015 schema: the orchestrator then strips the optional payload fields."""
    return not _legacy_seen


def reset_for_tests() -> None:
    global _legacy_seen
    _legacy_seen = False


async def detect_schema(conn: PgConnection) -> SchemaLevel:
    """The catalog's answer for the 0015 columns (one read)."""
    row = await conn.fetchrow(SQL_SCHEMA_0015)
    if row is None:
        return CURRENT
    migrated = bool(row[0])
    return SchemaLevel(migrated=migrated, prereqs=not migrated or int(row[1]) >= len(COLUMNS_0015))


async def rewriter_for(conn: PgConnection) -> LegacyRewriter | None:
    """Detect the schema over `conn`, note it for the process, and return the rewriter an older schema needs."""
    level = await detect_schema(conn)
    note_level(level)
    return LegacyRewriter() if level.legacy else None


__all__ = ["COLUMNS_0015", "CURRENT", "LegacyConnection", "LegacyRewriter", "NEW_IDENTIFIERS", "Rewrite",
           "SQL_SCHEMA_0015", "SchemaLevel", "detect_schema", "legacy_schema_seen", "note_level",
           "optional_payload_stored", "reset_for_tests", "rewriter_for"]
