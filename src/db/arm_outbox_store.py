"""The ARM outbox store: kind `arm` rows of `notification_outbox` (design PR4, §B3, §B8; plan S29, RT-M4, R11-M14).

ARM notices share `notification_outbox` with the Oversee alerts but never meet the Oversee relay: every read in
`src/db/notify_store.py` skips `kind = 'arm'`, and every statement here names `kind = 'arm'`.

* **Insert** (`PgArmOutboxInserter.enqueue`, the `NoticeOutboxPort` of `src.riskmgr.notices`): inside the notice's
  own fenced transaction on the ARM pool (role `xtnl_arm`: INSERT only on the outbox, so no RETURNING; the
  `arm_notices.outbox_id` column stays unset, S24 deviation 6). Kind `arm`, severity `INFO`, the route's channels
  (`arm_email` / `arm_push`, non-empty: the 0016 CHECK), the subject (ids, event, part, bar end) and the ids-only body
  (`src.riskmgr.render.encode_body`), `next_attempt_at_ms` = the creation time and `expires_at_ms`.
* **Relay reads and writes** (`PgArmOutboxStore`, the arm-relay's store): on the **engine (owner) pool**, like the
  Oversee relay. `due` (unsent rows whose next attempt is due or whose expiry has passed), `recent` (the kind `arm`
  rows created since a time, newest first: the supersede check), `pending`, `oldest_undelivered_ms` (the dead-man
  field `arm_oldest_undelivered_notification_age_ms`), `reschedule` (remaining channels in the encoded `name:n`
  form, never `+escalated`, so a rescheduled row still passes the 0016 CHECK; `delivered_at_ms` records the first
  time a channel accepted the row) and `finish` (sent, expired, superseded, suppressed: sets `sent_at_ms`,
  `delivered`, `superseded_by` and **replaces the body**, `finished_body`).
* **What a finished body keeps** (R11-M14): `finished_body(body)` keeps the notice id, the notice key (route, bar
  spec, event, bar end, part, item key), the decision ids, the outcome codes and the finish status, nothing else: the
  order-state levels of the unsent body (proposed stop, entry, S0, R values) are dropped. An immediate-ping body
  (§B14; plain text with scope, reason code and position ids, written by `src/db/arm_failures_store.py`) is not a
  notice body and is kept as written. Housekeeping (`src/db/lifecycle.py`) finishes expired rows with the same
  function when the relay is not running.

Nothing here reads or writes a price: rows hold ids, codes, order state and the keyed hash of an input bar only.
"""
from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Protocol

from src.db.arm_store import PgArmTxn
from src.db.epoch import EngineDb
from src.db.fence import FENCE
from src.db.pg import Row
from src.oversee.notify import ESCALATED_TOKEN, decode_channels

ARM_KIND: Final = "arm"
ARM_CHANNELS: Final = frozenset({"arm_email", "arm_push"})
NOTICE_SEVERITY: Final = "INFO"
PING_SEVERITY: Final = "CRITICAL"
"""The kind `arm` copy of the immediate ping (`SQL_ARM_PING_INSERT`) is the only CRITICAL kind `arm` row."""
FINISHED_VERSION: Final = 1
FINISH_STATUSES: Final = frozenset({"sent", "expired", "superseded", "suppressed", "undeliverable", "bad_body"})
_CODE: Final = re.compile(r"[A-Za-z0-9_.:*@+-]{0,200}")
_CHANNEL: Final = re.compile(r"(arm_email|arm_push)(:[0-9]+)?")

SQL_ARM_OUTBOX_INSERT: Final = (
    "INSERT INTO notification_outbox (created_at_ms, severity, channels, subject, body, kind, next_attempt_at_ms, "
    "expires_at_ms) SELECT $2::bigint, 'INFO', $3::text[], $4::text, $5::text, 'arm', $2::bigint, $6::bigint "
    f"WHERE {FENCE}"
)
ARM_ROW_COLS: Final = (
    "outbox_id, created_at_ms, severity, subject, body, channels, attempts, expires_at_ms, delivered_at_ms"
)
SQL_ARM_OUTBOX_DUE: Final = (
    f"SELECT {ARM_ROW_COLS} FROM notification_outbox WHERE kind = 'arm' AND sent_at_ms IS NULL "
    "AND (next_attempt_at_ms <= $1::bigint OR expires_at_ms <= $1::bigint) "
    "ORDER BY next_attempt_at_ms, outbox_id LIMIT $2::int"
)
SQL_ARM_OUTBOX_PENDING: Final = (
    f"SELECT {ARM_ROW_COLS} FROM notification_outbox WHERE kind = 'arm' AND sent_at_ms IS NULL "
    "ORDER BY outbox_id LIMIT $1::int"
)
#: The kind `arm` rows created at or after `$1`, newest first (sent or not): what a due row may be superseded by.
SQL_ARM_OUTBOX_RECENT: Final = (
    "SELECT outbox_id, created_at_ms, severity, body FROM notification_outbox WHERE kind = 'arm' "
    "AND created_at_ms >= $1::bigint ORDER BY outbox_id DESC LIMIT $2::int"
)
SQL_ARM_OUTBOX_OLDEST_UNSENT: Final = (
    "SELECT MIN(created_at_ms) FROM notification_outbox WHERE kind = 'arm' AND sent_at_ms IS NULL"
)
SQL_ARM_OUTBOX_RESCHEDULE: Final = (
    "UPDATE notification_outbox SET channels = $4::text[], next_attempt_at_ms = $5::bigint, last_error = $6::text, "
    "attempts = attempts + $7::int, delivered_at_ms = COALESCE(delivered_at_ms, $8::bigint) "
    "WHERE outbox_id = $2::bigint AND created_at_ms = $3::bigint AND kind = 'arm' AND sent_at_ms IS NULL "
    f"AND {FENCE}"
)
SQL_ARM_OUTBOX_FINISH: Final = (
    "UPDATE notification_outbox SET sent_at_ms = $4::bigint, delivered = $5::boolean, "
    "delivered_at_ms = CASE WHEN $5::boolean THEN COALESCE(delivered_at_ms, $4::bigint) ELSE delivered_at_ms END, "
    "attempts = attempts + $6::int, last_error = $7::text, body = $8::text, superseded_by = $9::bigint "
    "WHERE outbox_id = $2::bigint AND created_at_ms = $3::bigint AND kind = 'arm' AND sent_at_ms IS NULL "
    f"AND {FENCE}"
)
#: Housekeeping: unsent kind `arm` rows whose expiry is at or before `$1` (pings have none and never expire), oldest
#: first. Housekeeping passes `now - LifecycleConfig.arm_expiry_grace_ms`, so the relay always gets the first chance
#: (it pages an expired POST / LATE `ADJUST` notice, §B8).
SQL_ARM_OUTBOX_EXPIRED: Final = (
    "SELECT outbox_id, created_at_ms, body FROM notification_outbox WHERE kind = 'arm' AND sent_at_ms IS NULL "
    "AND expires_at_ms <= $1::bigint ORDER BY outbox_id LIMIT $2::int"
)
#: Housekeeping: finish a batch of expired rows (`$2` = now) with their scrubbed bodies.
SQL_ARM_OUTBOX_EXPIRE: Final = (
    "UPDATE notification_outbox o SET sent_at_ms = $2::bigint, delivered = false, last_error = 'expired', "
    "body = u.body FROM unnest($3::bigint[], $4::bigint[], $5::text[]) AS u(id, created, body) "
    "WHERE o.outbox_id = u.id AND o.created_at_ms = u.created AND o.kind = 'arm' AND o.sent_at_ms IS NULL "
    f"AND o.expires_at_ms <= $2::bigint AND {FENCE}"
)


# ---------------------------------------------------------------------------------------------------- bodies
@dataclass(frozen=True, kw_only=True)
class NoticeKey:
    """A notice's key, read from its body (unsent or finished): what `supersedes` compares."""

    notice_id: int
    route: str
    bar_spec: str
    event: str
    bar_end_ms: int
    part: int = 0
    item_key: str = ""


def _json_obj(text: str) -> Mapping[str, Any] | None:
    try:
        obj = json.loads(text)
    except (ValueError, TypeError):
        return None
    return obj if isinstance(obj, dict) else None


def _int(v: object) -> int | None:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _code(v: object) -> str | None:
    return v if isinstance(v, str) and _CODE.fullmatch(v) else None


def notice_key(body: str) -> NoticeKey | None:
    """The key of a notice body (the full body or a finished one); None for a ping or anything else."""
    obj = _json_obj(body)
    if obj is None:
        return None
    nid, end, part = _int(obj.get("notice_id")), _int(obj.get("bar_end_ms")), _int(obj.get("part"))
    route, spec, event = obj.get("route"), obj.get("bar_spec"), obj.get("event")
    item = obj.get("item_key", "")
    if nid is None or end is None or part is None or not isinstance(route, str) or not isinstance(spec, str) \
            or not isinstance(event, str) or not isinstance(item, str):
        return None
    return NoticeKey(notice_id=nid, route=route, bar_spec=spec, event=event, bar_end_ms=end, part=part,
                     item_key=item)


def finished_body(body: str, status: str) -> str:
    """The body a finished row keeps (R11-M14): the notice id and key, decision ids, outcome codes and `status`; a
    body that is not a notice (the immediate ping's text: scope, reason code, position ids) is kept as written."""
    if status not in FINISH_STATUSES:
        raise ValueError(f"unknown finish status {status!r}")
    try:
        parsed = json.loads(body)
    except (ValueError, TypeError):
        return body                          # the ping's plain text
    obj: Mapping[str, Any] = parsed if isinstance(parsed, dict) else {}
    key = notice_key(body)
    out: dict[str, object] = {"v": FINISHED_VERSION, "status": status}
    if key is not None:
        out.update({"notice_id": key.notice_id, "route": key.route, "bar_spec": key.bar_spec, "event": key.event,
                    "bar_end_ms": key.bar_end_ms, "part": key.part, "item_key": key.item_key})
    else:
        nid = _int(obj.get("notice_id"))
        if nid is not None:
            out["notice_id"] = nid
    decisions: list[int] = []
    outcomes: list[str] = []
    items = obj.get("items")
    for item in items if isinstance(items, list) else ():
        if not isinstance(item, dict):
            continue
        did, outcome = _int(item.get("decision_id")), _code(item.get("outcome"))
        if did is not None:
            decisions.append(did)
        if outcome is not None:
            outcomes.append(outcome)
    out["decision_ids"] = decisions
    out["outcomes"] = outcomes
    return json.dumps(out, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def pages_when_expired(body: str) -> bool:
    """§B8 Pages, read from a stored body (unsent or finished): a POST or LATE notice with an `ADJUST` item pages when
    it expires undelivered. The same rule as `src.notifications.arm_relay.expired_adjust` over a decoded body (a test keeps
    them equal); here on the JSON so housekeeping needs no ARM import. A ping or anything else: False."""
    obj = _json_obj(body)
    if obj is None:
        return False
    items = obj.get("items")
    rows = [i for i in items if isinstance(i, dict)] if isinstance(items, list) else []
    late = obj.get("event") in ("POST", "LATE") or any(i.get("event") == "LATE" for i in rows)
    return late and any(i.get("outcome") == "ADJUST" for i in rows)


def check_channels(channels: Sequence[str]) -> tuple[str, ...]:
    """The plain `arm_email` / `arm_push` route of a new row: non-empty, no repeats (the 0016 CHECK)."""
    out = tuple(channels)
    if not out or len(set(out)) != len(out) or not set(out) <= ARM_CHANNELS:
        raise ValueError("an ARM notice goes to a non-empty subset of arm_email, arm_push")
    return out


def encode_arm_channels(remaining: Sequence[str], failures: Mapping[str, int]) -> list[str]:
    """The `channels` value of a rescheduled ARM row: each remaining channel as `name` or `name:n` (n failures).
    Never empty and never `+escalated` (the ARM relay does not escalate), so it always passes the 0016 CHECK."""
    out = [f"{c}:{failures[c]}" if failures.get(c, 0) > 0 else c for c in remaining]
    if not out or any(not _CHANNEL.fullmatch(c) for c in out) or ESCALATED_TOKEN in out:
        raise ValueError("a rescheduled ARM row keeps at least one arm_email / arm_push channel")
    return out


# ---------------------------------------------------------------------------------------------------- rows
@dataclass(frozen=True, kw_only=True)
class ArmOutboxRow:
    """One unsent kind `arm` row as the relay reads it."""

    outbox_id: int
    created_ms: int
    severity: str
    subject: str
    body: str = field(repr=False)
    channels: tuple[str, ...]
    """Channels still to deliver."""
    failures: tuple[tuple[str, int], ...] = ()
    attempts: int = 0
    expires_at_ms: int | None = None
    delivered_at_ms: int | None = None
    """The first time one channel accepted the row (None: never sent to anyone yet)."""

    @property
    def is_ping(self) -> bool:
        return self.severity.upper() == PING_SEVERITY


@dataclass(frozen=True, kw_only=True)
class ArmRecentRow:
    outbox_id: int
    created_ms: int
    is_ping: bool
    key: NoticeKey | None


def arm_row_from(r: Row) -> ArmOutboxRow:
    names, failures, _ = decode_channels(str(c) for c in (r[5] or ()))
    return ArmOutboxRow(outbox_id=int(r[0]), created_ms=int(r[1]), severity=str(r[2]), subject=str(r[3]),
                        body=str(r[4]), channels=names, failures=failures, attempts=int(r[6] or 0),
                        expires_at_ms=None if r[7] is None else int(r[7]),
                        delivered_at_ms=None if r[8] is None else int(r[8]))


# ---------------------------------------------------------------------------------------------------- insert
class OutboxNoticeLike(Protocol):
    """`src.riskmgr.notices.OutboxNotice` (structural, so `src/db` does not import the ARM package)."""

    @property
    def notice_id(self) -> int: ...

    @property
    def channels(self) -> tuple[str, ...]: ...

    @property
    def subject(self) -> str: ...

    @property
    def body(self) -> str: ...

    @property
    def created_ms(self) -> int: ...

    @property
    def expires_at_ms(self) -> int: ...


class PgArmOutboxInserter:
    """`NoticeOutboxPort` over the notice's own `PgArmTxn` (the ARM pool's fenced transaction)."""

    async def enqueue(self, tx: object, notice: OutboxNoticeLike) -> None:
        if not isinstance(tx, PgArmTxn):
            raise TypeError("the ARM outbox insert runs in the notice's PgArmTxn")
        channels = check_channels(notice.channels)
        obj = _json_obj(notice.body)
        if obj is None or obj.get("notice_id") != notice.notice_id:
            raise ValueError("an ARM outbox body is the notice's encoded body")
        await tx.tx.write(SQL_ARM_OUTBOX_INSERT, notice.created_ms, list(channels), notice.subject, notice.body,
                          notice.expires_at_ms, must=True)


# ---------------------------------------------------------------------------------------------------- relay store
class PgArmOutboxStore:
    """The arm-relay's reads and writes, on the engine (owner) pool (§B3)."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def due(self, now_ms: int, limit: int) -> list[ArmOutboxRow]:
        async with self._db.source.acquire() as conn:
            return [arm_row_from(r) for r in await conn.fetch(SQL_ARM_OUTBOX_DUE, now_ms, limit)]

    async def pending(self, limit: int) -> list[ArmOutboxRow]:
        async with self._db.source.acquire() as conn:
            return [arm_row_from(r) for r in await conn.fetch(SQL_ARM_OUTBOX_PENDING, limit)]

    async def recent(self, since_ms: int, limit: int) -> list[ArmRecentRow]:
        async with self._db.source.acquire() as conn:
            rows = await conn.fetch(SQL_ARM_OUTBOX_RECENT, since_ms, limit)
        return [ArmRecentRow(outbox_id=int(r[0]), created_ms=int(r[1]),
                             is_ping=str(r[2]).upper() == PING_SEVERITY, key=notice_key(str(r[3]))) for r in rows]

    async def oldest_undelivered_ms(self) -> int | None:
        async with self._db.source.acquire() as conn:
            row = await conn.fetchrow(SQL_ARM_OUTBOX_OLDEST_UNSENT)
        return None if row is None or row[0] is None else int(row[0])

    async def reschedule(self, row: ArmOutboxRow, *, remaining: Sequence[str], failures: Mapping[str, int],
                         next_attempt_ms: int, error: str | None, attempted: bool,
                         delivered_at_ms: int | None) -> bool:
        channels = encode_arm_channels(remaining, failures)
        async with self._db.transaction() as tx:
            return await tx.write(SQL_ARM_OUTBOX_RESCHEDULE, row.outbox_id, row.created_ms, channels,
                                  next_attempt_ms, error, 1 if attempted else 0, delivered_at_ms) > 0

    async def finish(self, row: ArmOutboxRow, at_ms: int, *, delivered: bool, status: str, error: str | None,
                     attempted: bool, superseded_by: int | None = None) -> bool:
        async with self._db.transaction() as tx:
            return await tx.write(SQL_ARM_OUTBOX_FINISH, row.outbox_id, row.created_ms, at_ms, delivered,
                                  1 if attempted else 0, error, finished_body(row.body, status), superseded_by) > 0


__all__ = [
    "ARM_CHANNELS", "ARM_KIND", "ARM_ROW_COLS", "ArmOutboxRow", "ArmRecentRow", "FINISH_STATUSES", "NOTICE_SEVERITY",
    "NoticeKey", "OutboxNoticeLike", "PING_SEVERITY", "PgArmOutboxInserter", "PgArmOutboxStore",
    "SQL_ARM_OUTBOX_DUE", "SQL_ARM_OUTBOX_EXPIRE", "SQL_ARM_OUTBOX_EXPIRED", "SQL_ARM_OUTBOX_FINISH",
    "SQL_ARM_OUTBOX_INSERT", "SQL_ARM_OUTBOX_OLDEST_UNSENT", "SQL_ARM_OUTBOX_PENDING", "SQL_ARM_OUTBOX_RECENT",
    "SQL_ARM_OUTBOX_RESCHEDULE", "arm_row_from", "check_channels", "encode_arm_channels", "finished_body",
    "notice_key", "pages_when_expired",
]
