"""OutboxStore and DedupeStore (src/oversee/notify.py) over PostgreSQL (oversee-rules.md §10).

* `notification_outbox`: rows are written in the same fenced transaction as the transition / halt row
  (`insert_outbox`, shared with the sweep writer and the halt store); `pending` / `mark_sent` drive the
  dispatcher. Channels: info -> email; high / critical (SOFT / HARD) -> email + mobile push.
* `notification_dedupe`: the per-episode last-sent time lives in PostgreSQL so a new leader does not
  re-send. Keys are the episode tuple as a compact JSON array.
* Relay bookkeeping (`src.notifications.relay.RelayOutboxStore`): `due` (unsent rows whose
  `next_attempt_at_ms` has passed, with their remaining `channels` and `attempts`), `reschedule`
  (remaining channels, back-off, `last_error`), `finish` (`sent_at_ms`, `delivered`, `last_error`) and
  `oldest_undelivered_ms` (the dead-man payload field). A new leader resumes the retries where the old
  one stopped, sending only the channels that had not yet accepted the alert.
* SM6 retry state lives in the `channels` column (no schema change): each remaining channel as `name` or
  `name:n` (n = its failed attempts for this row, the back-off exponent and the escalation counter) and
  the `+escalated` marker once the alert was escalated to the other channel (`encode_channels` /
  `decode_channels` in src/oversee/notify.py). `escalated_since` lists escalated rows for the dashboard.
Every write is fenced (`$1` = epoch): only the leader queues or marks alerts.
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, Final

from src.db.epoch import EngineDb
from src.db.fence import FENCE, FencedTx
from src.db.oversee_errors import oversee_errors
from src.db.rows import OUTBOX_COLS, episode_from, episode_text, outbox_arrays, outbox_from_row
from src.oversee.notify import ESCALATED_TOKEN, EpisodeKey, OutboxRow, decode_channels, encode_channels

SQL_OUTBOX_INSERT: Final = (
    "INSERT INTO notification_outbox (created_at_ms, dedupe_key, episode, bypass_dedupe, severity, channels, "
    "subject, body, kind, account_id) "
    "SELECT t.created, t.dkey, t.dkey::jsonb, t.bypass, t.sev, "
    "CASE WHEN t.sev IN ('HIGH', 'CRITICAL', 'SOFT', 'HARD') THEN ARRAY['email', 'mobile_push'] "
    "ELSE ARRAY['email'] END, t.title, t.body, t.kind, t.acct "
    "FROM unnest($2::bigint[], $3::text[], $4::boolean[], $5::text[], $6::text[], $7::text[], $8::text[], "
    "$9::text[]) AS t(created, dkey, bypass, sev, title, body, kind, acct) "
    f"WHERE {FENCE}"
)
SQL_OUTBOX_PENDING: Final = (
    f"SELECT {OUTBOX_COLS} FROM notification_outbox WHERE sent_at_ms IS NULL ORDER BY outbox_id LIMIT $1"
)
SQL_OUTBOX_MARK: Final = (
    "UPDATE notification_outbox SET sent_at_ms = $3, delivered = $4, "
    "delivered_at_ms = CASE WHEN $4 THEN $3 ELSE delivered_at_ms END, attempts = attempts + 1 "
    f"WHERE outbox_id = $2 AND {FENCE}"
)
SQL_OUTBOX_DUE: Final = (
    f"SELECT {OUTBOX_COLS}, channels, attempts FROM notification_outbox "
    "WHERE sent_at_ms IS NULL AND next_attempt_at_ms <= $1 ORDER BY outbox_id LIMIT $2"
)
SQL_OUTBOX_RESCHEDULE: Final = (
    "UPDATE notification_outbox SET channels = $3::text[], next_attempt_at_ms = $4::bigint, "
    "last_error = $5::text, attempts = attempts + $6::int "
    f"WHERE outbox_id = $2 AND sent_at_ms IS NULL AND {FENCE}"
)
SQL_OUTBOX_FINISH: Final = (
    "UPDATE notification_outbox SET sent_at_ms = $3::bigint, delivered = $4::boolean, "
    "delivered_at_ms = CASE WHEN $4::boolean THEN $3::bigint ELSE delivered_at_ms END, "
    "attempts = attempts + 1, last_error = $5::text "
    f"WHERE outbox_id = $2 AND sent_at_ms IS NULL AND {FENCE}"
)
SQL_OUTBOX_ESCALATED: Final = (
    f"SELECT {OUTBOX_COLS}, channels, attempts, sent_at_ms, delivered, last_error FROM notification_outbox "
    "WHERE $1::text = ANY(channels) AND created_at_ms >= $2::bigint ORDER BY outbox_id DESC LIMIT $3"
)
SQL_OUTBOX_OLDEST_UNSENT: Final = "SELECT MIN(created_at_ms) FROM notification_outbox WHERE sent_at_ms IS NULL"
SQL_DEDUPE_GET: Final = "SELECT last_sent_at_ms FROM notification_dedupe WHERE dedupe_key = $1"
SQL_DEDUPE_ALL: Final = "SELECT dedupe_key, last_sent_at_ms FROM notification_dedupe"
#: Only the rows a re-page pass needs (primary-key lookups; IMPLEMENTATION_PLAN 8.2).
SQL_DEDUPE_SOME: Final = (
    "SELECT dedupe_key, last_sent_at_ms FROM notification_dedupe WHERE dedupe_key = ANY($1::text[])"
)
SQL_DEDUPE_RECORD: Final = (
    "INSERT INTO notification_dedupe (dedupe_key, first_sent_at_ms, last_sent_at_ms, sent_count) "
    f"SELECT $2::text, $3::bigint, $3::bigint, 1 WHERE {FENCE} "
    "ON CONFLICT (dedupe_key) DO UPDATE SET last_sent_at_ms = EXCLUDED.last_sent_at_ms, "
    "sent_count = notification_dedupe.sent_count + 1"
)


async def insert_outbox(tx: FencedTx, rows: Sequence[OutboxRow]) -> int:
    """Append alert rows inside the caller's fenced transaction."""
    if not rows:
        return 0
    return await tx.write(SQL_OUTBOX_INSERT, *outbox_arrays(rows), must=True)


def dedupe_key(key: EpisodeKey) -> str:
    text = episode_text(key)
    assert text is not None
    return text


@dataclass(frozen=True)
class EscalatedRow:
    row: OutboxRow                 # channels = still to deliver when it was last rescheduled
    sent_at_ms: int | None         # None while the relay is still retrying
    delivered: bool | None
    last_error: str | None


def _with_retry_state(r: Any) -> OutboxRow:
    """An outbox row with the relay's retry state: remaining channels, per-channel failures, escalated."""
    names, failures, escalated = decode_channels(str(c) for c in (r[8] or ()))
    return replace(outbox_from_row(r), channels=names, channel_failures=failures, escalated=escalated,
                   attempts=int(r[9] or 0))


class PgOutboxStore:
    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def add(self, rows: Sequence[OutboxRow]) -> None:
        if not rows:
            return
        async with oversee_errors():
            async with self._db.transaction() as tx:
                await insert_outbox(tx, rows)

    async def pending(self, limit: int) -> list[OutboxRow]:
        async with oversee_errors():
            async with self._db.source.acquire() as conn:
                return [outbox_from_row(r) for r in await conn.fetch(SQL_OUTBOX_PENDING, limit)]

    async def mark_sent(self, outbox_id: int, at_ms: int, delivered: bool) -> None:
        async with oversee_errors():
            async with self._db.transaction() as tx:
                await tx.write(SQL_OUTBOX_MARK, outbox_id, at_ms, delivered)


    # ---- relay bookkeeping (RelayOutboxStore) ------------------------------------------------------
    async def due(self, now_ms: int, limit: int) -> list[OutboxRow]:
        async with oversee_errors():
            async with self._db.source.acquire() as conn:
                rows = await conn.fetch(SQL_OUTBOX_DUE, now_ms, limit)
        return [_with_retry_state(r) for r in rows]

    async def reschedule(self, outbox_id: int, *, remaining: Sequence[str], next_attempt_ms: int,
                         error: str | None, attempted: bool, failures: Mapping[str, int] | None = None,
                         escalated: bool = False) -> None:
        channels = encode_channels(remaining, failures or {}, escalated)
        async with oversee_errors():
            async with self._db.transaction() as tx:
                await tx.write(SQL_OUTBOX_RESCHEDULE, outbox_id, channels, next_attempt_ms, error,
                               1 if attempted else 0)

    async def finish(self, outbox_id: int, at_ms: int, *, delivered: bool, error: str | None) -> None:
        async with oversee_errors():
            async with self._db.transaction() as tx:
                await tx.write(SQL_OUTBOX_FINISH, outbox_id, at_ms, delivered, error)

    async def escalated_since(self, since_ms: int, limit: int = 50) -> list[EscalatedRow]:
        """Rows escalated to the other channel (SM6 ESCALATED) created at or after `since_ms`, newest first:
        the dashboard's 'alert delivery failing' list. Read-only."""
        async with oversee_errors():
            async with self._db.source.acquire() as conn:
                rows = await conn.fetch(SQL_OUTBOX_ESCALATED, ESCALATED_TOKEN, since_ms, limit)
        return [EscalatedRow(_with_retry_state(r), None if r[10] is None else int(r[10]),
                             None if r[11] is None else bool(r[11]), None if r[12] is None else str(r[12]))
                for r in rows]

    async def oldest_undelivered_ms(self) -> int | None:
        async with oversee_errors():
            async with self._db.source.acquire() as conn:
                row = await conn.fetchrow(SQL_OUTBOX_OLDEST_UNSENT)
        return None if row is None or row[0] is None else int(row[0])


class PgDedupeStore:
    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def last_sent(self, key: EpisodeKey) -> int | None:
        async with oversee_errors():
            async with self._db.source.acquire() as conn:
                row = await conn.fetchrow(SQL_DEDUPE_GET, dedupe_key(key))
        return None if row is None else int(row[0])

    async def record(self, key: EpisodeKey, at_ms: int) -> None:
        async with oversee_errors():
            async with self._db.transaction() as tx:
                await tx.write(SQL_DEDUPE_RECORD, dedupe_key(key), at_ms, must=True)

    async def last_sent_of(self, keys: Sequence[EpisodeKey]) -> Mapping[EpisodeKey, int]:
        """The rows of `keys` only (the RiskSweep's re-page check, once per account pass)."""
        if not keys:
            return MappingProxyType({})
        texts = {dedupe_key(k): k for k in keys}
        async with oversee_errors():
            async with self._db.source.acquire() as conn:
                rows = await conn.fetch(SQL_DEDUPE_SOME, list(texts))
        out: dict[EpisodeKey, int] = {}
        for r in rows:
            key = texts.get(str(r[0]))
            if key is not None and r[1] is not None:
                out[key] = int(r[1])
        return MappingProxyType(out)

    async def snapshot(self) -> Mapping[EpisodeKey, int]:
        """Every row (operator / test use only: the RiskSweep reads `last_sent_of`)."""
        async with oversee_errors():
            async with self._db.source.acquire() as conn:
                rows = await conn.fetch(SQL_DEDUPE_ALL)
        out: dict[EpisodeKey, int] = {}
        for r in rows:
            try:
                key = episode_from(json.loads(str(r[0])))
            except (ValueError, IndexError, TypeError):
                continue   # a key written by another tool: not an episode
            if key is not None:
                out[key] = int(r[1])
        return MappingProxyType(out)
