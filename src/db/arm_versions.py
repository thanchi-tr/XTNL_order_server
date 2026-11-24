"""`event_log` reads for the ARM: entry time and the `S_at_T` candidates (design §B6.4, §B7, PR8; plan S24, R11-M06).

All reads go through `event_log_broker_time_idx` `(entity_type, entity_key, broker_time_ms)` (0017, built per
partition by the migrate post step). `event_log.broker_time_ms` holds a real broker time only (PR8: a
watermark-labelled version has NULL), so "a real broker time" is `broker_time_ms IS NOT NULL`. Entity keys are the
Redis keys `position:{adapter_id}:{position_id}` / `order:{adapter_id}:{broker_order_id}` (`src/cache/keys.py`).

* **Entry time** (§B7, the fill, never the placement): the broker time of the first `APPLIED` POSITION version with
  origin `stream` and status OPEN (`STREAM`), else the earliest `APPLIED` version of the entry order with status
  FILLED or PARTIALLY_FILLED and a real broker time (`BROKER`; StoneX history's `ExecutedDateTimeUtc`). PENDING /
  WORKING versions (a stop or limit entry's placement) and watermark-labelled rows are never used. None means the
  caller records `ESTIMATED`.
* **Every stop child ever linked** (round 13, plan S38d): the children read are the caller's current children plus
  every reduce-only order `orders_state` links to the position (`linked_position_id`) that may have been live at `T`:
  still non-terminal, or whose last version is after `T` (a child cancelled / replaced after `T`, e.g. a StoneX
  cancel/replace while a delayed POST waits, over a weekend before a REOPEN, or before a LATE replay). Such a child
  counts as live at `T` through its own versions at or before `T`; a child created after `T` has no candidate and
  takes no part; a child that ended at or before `T` is not read at all (it held no stop at `T`).
* **`S_at_T` candidates** (§B6.4): per handle (the position's `stop_loss` field and each stop child), the newest
  **counting** version at or before `T`: `action = 'APPLIED'` (never `DROPPED_STALE`), origin `stream`, `stop_read`,
  `snapshot` or `dlq` (never `history`) and `broker_time_ms <= T`. Its level is the handle's level. The
  non-counting read returns **every** version newer than that counting one that does not count (round 14 review: a
  `history` or unlabelled version with a broker time at or before `T`, newer by (broker time, seq); a version with no
  broker time (watermark), newer by seq and older by seq than every counting version timed after `T`: one newer than
  a post-`T` counting version was read after it and shows state from after `T`, so it takes no part, as in
  `stopbasis._untimed_newer`, live review 2026-11-25) **with their order-state fields** (level, status, units, kind; plan S38g):
  a newer non-counting version gives `PRIOR_VERSION`, and the pure rule (`src/riskmgr/stopbasis.py`) tests the
  farthest from entry of the prior counting level and every one of theirs, so a loosen followed by a re-tighten, or a
  loosened amend seen only through an untimed REST row, is never tested at a smaller risk. Their levels alone never
  become `S_at_T` against a farther counting one. The read is bounded to `NONCOUNTING_LIMIT` (32) versions per handle,
  newest first; a handle with more gets one level-less live version in place of the rest, so it is unresolved
  (`StopUnavailable`, fail closed), never tested without its oldest levels. One marker read carries no level: the newest counting
  `stop_read` version after `T` (the caveat `stop_changed_after_bar_end`).

Read statements only (the ARM role has SELECT on `event_log`, §B3). Each method runs all its statements in one
READ ONLY REPEATABLE READ transaction (`EngineDb.snapshot()`: no fence, writes nothing; an ARM read snapshot like
the RiskSweep's named exception 1, listed as an operations §2 exception in the pending proposal
`docs/design/proposed-claude-changes/S38e-rules-arm-exceptions.md`), so the
counting read and the marker reads of every handle, POSITION and ORDER alike, see one committed state of
`event_log`. The payload fields read are order state
(`stop_loss`, `entry_price` of a stop child, `status`, units, `order_kind`); nothing else leaves the payload.
"""
from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal, InvalidOperation
from typing import Final, cast

from src.cache.keys import order_key, position_key
from src.db.arm_ports import ArmVersionReader, EntryTime, HandleVersionRow
from src.db.epoch import EngineDb
from src.db.pg import Row
from src.dto import OrderKind, Origin

COUNTING_SQL: Final = "('stream', 'stop_read', 'snapshot', 'dlq')"
FILL_STATUSES_SQL: Final = "('FILLED', 'PARTIALLY_FILLED')"
LIVE_ORDER_STATUSES: Final = frozenset({"PENDING", "WORKING", "PARTIALLY_FILLED"})
TERMINAL_ORDER_STATUSES_SQL: Final = "('FILLED', 'CANCELLED', 'REJECTED')"
POSITION_FIELD: Final = "POSITION_FIELD"
CHILD_ORDER: Final = "CHILD_ORDER"

SQL_ENTRY_STREAM: Final = (
    "SELECT broker_time_ms, seq_id, origin FROM event_log WHERE entity_type = 'POSITION' AND entity_key = $1::text "
    "AND action = 'APPLIED' AND origin = 'stream' AND broker_time_ms IS NOT NULL AND payload->>'status' = 'OPEN' "
    "ORDER BY broker_time_ms, seq_id LIMIT 1"
)
SQL_ENTRY_ORDER_FILL: Final = (
    "SELECT broker_time_ms, seq_id, origin FROM event_log WHERE entity_type = 'ORDER' AND entity_key = $1::text "
    f"AND action = 'APPLIED' AND broker_time_ms IS NOT NULL AND payload->>'status' IN {FILL_STATUSES_SQL} "
    "ORDER BY broker_time_ms, seq_id LIMIT 1"
)
#: Every reduce-only order linked to the position that may have held a stop at `$3` (= T): not terminal now, or
#: whose last version is after T (round 13). `orders_state` keeps terminal rows (one row per order, never deleted).
SQL_LINKED_STOP_CHILDREN: Final = (
    "SELECT broker_order_id FROM orders_state WHERE adapter_id = $1::text AND linked_position_id = $2::text "
    f"AND is_reduce_only AND (status NOT IN {TERMINAL_ORDER_STATUSES_SQL} OR broker_updated_at_ms > $3::bigint) "
    "ORDER BY broker_order_id"
)
#: The order-state fields of one version: level, status, units (position) / remaining units (order), order kind.
_FIELDS: Final = (
    "CASE WHEN entity_type = 'POSITION' THEN payload->>'stop_loss' ELSE payload->>'entry_price' END, "
    "payload->>'status', CASE WHEN entity_type = 'POSITION' THEN payload->>'units' "
    "ELSE payload->>'remaining_units' END, payload->>'order_kind'"
)
_HANDLES: Final = "entity_type = $1::text AND entity_key = ANY($2::text[]) AND action = 'APPLIED'"
#: The newest counting version per handle at or before `$3` (= T): `history` and `DROPPED_STALE` excluded.
SQL_STOP_COUNTING: Final = (
    f"SELECT DISTINCT ON (entity_key) entity_key, seq_id, broker_time_ms, origin, {_FIELDS} FROM event_log "
    f"WHERE {_HANDLES} AND origin IN {COUNTING_SQL} AND broker_time_ms IS NOT NULL AND broker_time_ms <= $3::bigint "
    "ORDER BY entity_key, broker_time_ms DESC, seq_id DESC"
)
NONCOUNTING_LIMIT: Final = 32
"""Newer non-counting versions read per handle; one more means the handle is unresolved (module docstring)."""
_FIELDS_NAMED: Final = (
    "CASE WHEN entity_type = 'POSITION' THEN payload->>'stop_loss' ELSE payload->>'entry_price' END AS level, "
    "payload->>'status' AS status, CASE WHEN entity_type = 'POSITION' THEN payload->>'units' "
    "ELSE payload->>'remaining_units' END AS units, payload->>'order_kind' AS order_kind"
)
#: Every version of each handle newer than its newest counting version at or before `$3` (= T) that does not count,
#: with their order-state fields (round 14 review: the direction rule of `stopbasis.handle_at_bar_end` takes the
#: farthest of them): timed non-counting ones at or before T newer by (broker time, seq) than every counting one, and
#: untimed ones newer by `seq_id` and older by `seq_id` than every counting version timed after T (§B6.4: a later
#: untimed row was read after that post-T version and shows state from after T; `stopbasis._untimed_newer`). At most
#: `$4` per handle, newest by `seq_id` first (the reader asks `NONCOUNTING_LIMIT + 1`, so it can tell that more exist).
SQL_STOP_NONCOUNTING: Final = (
    "SELECT entity_key, seq_id, broker_time_ms, origin, level, status, units, order_kind FROM ("
    f"SELECT e.entity_key, e.seq_id, e.broker_time_ms, e.origin, {_FIELDS_NAMED}, "
    "row_number() OVER (PARTITION BY e.entity_key ORDER BY e.seq_id DESC) AS rn FROM event_log e "
    f"WHERE {_HANDLES} AND ((broker_time_ms IS NOT NULL AND broker_time_ms <= $3::bigint "
    f"AND (origin IS NULL OR origin NOT IN {COUNTING_SQL})) OR broker_time_ms IS NULL) "
    "AND NOT EXISTS (SELECT 1 FROM event_log c WHERE c.entity_type = e.entity_type AND c.entity_key = e.entity_key "
    f"AND c.action = 'APPLIED' AND c.origin IN {COUNTING_SQL} AND c.broker_time_ms IS NOT NULL "
    "AND c.broker_time_ms <= $3::bigint AND CASE WHEN e.broker_time_ms IS NULL THEN c.seq_id > e.seq_id "
    "ELSE (c.broker_time_ms, c.seq_id) > (e.broker_time_ms, e.seq_id) END) "
    "AND (e.broker_time_ms IS NOT NULL OR NOT EXISTS (SELECT 1 FROM event_log a WHERE a.entity_type = e.entity_type "
    f"AND a.entity_key = e.entity_key AND a.action = 'APPLIED' AND a.origin IN {COUNTING_SQL} "
    "AND a.broker_time_ms > $3::bigint AND a.seq_id < e.seq_id))"
    ") AS v WHERE rn <= $4::int ORDER BY entity_key, seq_id"
)
#: Marker (no level): the newest counting `stop_read` version after `$3` per handle (stop changed after bar end).
SQL_STOP_READ_AFTER: Final = (
    "SELECT DISTINCT ON (entity_key) entity_key, seq_id, broker_time_ms, origin, payload->>'order_kind' "
    f"FROM event_log WHERE {_HANDLES} AND origin = 'stop_read' AND broker_time_ms > $3::bigint "
    "ORDER BY entity_key, broker_time_ms DESC, seq_id DESC"
)


def _num(v: object) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        d = Decimal(str(v))
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


def _kind(v: object) -> OrderKind | None:
    return cast(OrderKind, str(v)) if v in ("STOP", "LIMIT") else None


def _origin(v: object) -> Origin | None:
    return None if v is None else cast(Origin, str(v))


def _live(handle_kind: str, status: str | None) -> bool:
    if handle_kind == POSITION_FIELD:
        return status == "OPEN"
    return status in LIVE_ORDER_STATUSES


def _entry(source: str, r: Row | None) -> EntryTime | None:
    if r is None or r[0] is None:
        return None
    return EntryTime(source=source, broker_time_ms=int(r[0]), seq_id=int(r[1]), origin=_origin(r[2]))


def _bounded(rows: list[HandleVersionRow]) -> list[HandleVersionRow]:
    """One handle's newer non-counting versions as read (at most `NONCOUNTING_LIMIT + 1`): more than the limit means
    older ones were not read, so the oldest returned is replaced by a level-less live untimed version (the pure rule then
    finds the handle held at an unknown level: `StopUnavailable`, never a test without its oldest levels)."""
    if len(rows) <= NONCOUNTING_LIMIT:
        return rows
    rows = sorted(rows, key=lambda v: v.seq_id)
    oldest = rows[0]
    marker = HandleVersionRow(handle_kind=oldest.handle_kind, handle_id=oldest.handle_id, seq_id=rows[-1].seq_id,
                              broker_time_ms=None, origin=None, status=None, level=None, live=True)
    return [*rows[1:], marker]


class PgArmVersionReader(ArmVersionReader):
    """Entry time and the `S_at_T` candidate reads over any pool that may SELECT `event_log`."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def entry_time(self, adapter_id: str, position_id: str, entry_order_id: str | None) -> EntryTime | None:
        async with self._db.snapshot() as conn:
            got = _entry("STREAM", await conn.fetchrow(SQL_ENTRY_STREAM, position_key(adapter_id, position_id)))
            if got is None and entry_order_id:
                got = _entry("BROKER", await conn.fetchrow(SQL_ENTRY_ORDER_FILL, order_key(adapter_id, entry_order_id)))
        return got

    async def stop_versions(self, adapter_id: str, position_id: str, children: Sequence[str],
                            bar_end_ms: int) -> list[HandleVersionRow]:
        """Every version the `S_at_T` rule needs for the position's handles at `T = bar_end_ms`: one counting
        version (with its level) per handle, the newer non-counting versions (with their levels, round 14) and the
        level-less `stop_read`-after-`T` marker (module docstring). The handles are the
        `stop_loss` field, `children` (the current ones) and every child ever linked to the position that may have
        been live at `T` (`SQL_LINKED_STOP_CHILDREN`, round 13). Order: by handle, then `seq_id`."""
        out: list[HandleVersionRow] = []
        async with self._db.snapshot() as conn:                 # one snapshot for every statement below
            linked = [str(r[0]) for r in await conn.fetch(SQL_LINKED_STOP_CHILDREN, adapter_id, position_id,
                                                           bar_end_ms)]
            every = sorted(set(children) | set(linked))
            groups: list[tuple[str, str, dict[str, str]]] = [
                ("POSITION", POSITION_FIELD, {position_key(adapter_id, position_id): position_id})]
            if every:
                groups.append(("ORDER", CHILD_ORDER, {order_key(adapter_id, c): c for c in every}))
            for entity_type, handle_kind, ids in groups:
                keys = sorted(ids)
                for r in await conn.fetch(SQL_STOP_COUNTING, entity_type, keys, bar_end_ms):
                    status = None if r[5] is None else str(r[5])
                    out.append(HandleVersionRow(
                        handle_kind=handle_kind, handle_id=ids[str(r[0])], seq_id=int(r[1]),
                        broker_time_ms=int(r[2]), origin=_origin(r[3]), status=status, level=_num(r[4]),
                        units=_num(r[6]), order_kind=_kind(r[7]), live=_live(handle_kind, status)))
                newer: dict[str, list[HandleVersionRow]] = {}
                for r in await conn.fetch(SQL_STOP_NONCOUNTING, entity_type, keys, bar_end_ms,
                                          NONCOUNTING_LIMIT + 1):
                    status = None if r[5] is None else str(r[5])
                    newer.setdefault(str(r[0]), []).append(HandleVersionRow(
                        handle_kind=handle_kind, handle_id=ids[str(r[0])], seq_id=int(r[1]),
                        broker_time_ms=None if r[2] is None else int(r[2]), origin=_origin(r[3]), status=status,
                        level=_num(r[4]), units=_num(r[6]), order_kind=_kind(r[7]),
                        live=_live(handle_kind, status)))
                for rows in newer.values():
                    out.extend(_bounded(rows))
                for r in await conn.fetch(SQL_STOP_READ_AFTER, entity_type, keys, bar_end_ms):
                    out.append(HandleVersionRow(
                        handle_kind=handle_kind, handle_id=ids[str(r[0])], seq_id=int(r[1]),
                        broker_time_ms=None if r[2] is None else int(r[2]), origin=_origin(r[3]), status=None,
                        order_kind=_kind(r[4])))
        out.sort(key=lambda v: (v.handle_kind, v.handle_id, v.seq_id))
        return out


__all__ = ["CHILD_ORDER", "LIVE_ORDER_STATUSES", "NONCOUNTING_LIMIT", "POSITION_FIELD", "PgArmVersionReader", "SQL_ENTRY_ORDER_FILL",
           "SQL_ENTRY_STREAM", "SQL_LINKED_STOP_CHILDREN", "SQL_STOP_COUNTING", "SQL_STOP_NONCOUNTING",
           "SQL_STOP_READ_AFTER", "TERMINAL_ORDER_STATUSES_SQL"]
