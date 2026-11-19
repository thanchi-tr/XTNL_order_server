"""FAILED episodes in PostgreSQL: record with the immediate ping, re-ping, probe notes, open episodes, clear (design
§B8 Immediate ping, §B14 Record and Clearing, PR4; plan S24, R11-M05, RT-M5, ST-4).

* `PgArmFailuresStore.record(failure, ping)`: **one fenced transaction** inserts the `arm_failures` row, the ping's
  critical copy (`OutboxRow(kind="info", severity="critical")` through `insert_outbox(tx, ...)`, delivered by the
  Oversee relay on email and mobile push with its escalation) and the kind `arm` row on `arm_email` and `arm_push`
  (the ARM relay's), so all three commit or none does. The insert is `ON CONFLICT DO NOTHING` (`must=False`): a
  replayed episode (same `failure_id`, from memory or the `FailedSpoolPort`) or a second open episode of the same
  scope (the partial unique index) writes nothing and queues no second ping. The ping id must be in the title; the
  body is bounded (`MAX_PING_BODY`). Never `EngineSession.page`, which would queue a second critical row.
  `queue_ping=False` (S30a, the collapsed ping of §B14): the row only, for an episode folded into an ADAPTER or ARM
  episode whose own ping names it; its `ping_id` is that collapsing ping's id.
  The caller (S30) runs it on the engine pool when the ARM pool is the fault, and retries from memory until it
  commits.
* `reping` (every `fail_reping_h` while open): `last_ping_at_ms`, `ping_count + 1` and the same two outbox rows, in
  one fenced transaction; nothing when the episode is no longer open. `note_probe`: the last probe's time and result.
* `open_episodes()`: the open rows (`optional-bootstrap` reads them before the BarClock's first event, §B14).
* `PgArmFailedClear.clear(...)`: the `arm_clear_failed` handler's statement, the only SQL outside `src/ops` that sets
  `cleared_at_ms`. Always on the engine (owner) pool (`xtnl_arm` has no UPDATE on `cleared_at_ms` /
  `clear_request_id`), in a fenced transaction under `pg_advisory_xact_lock(ARM_FAILED_LOCK)` (the halt-clear pattern
  of `src/ops/halt.py`), linked to the request's `operator_audit` row: it applies only for an `arm_clear_failed`
  request of this epoch whose target is the episode's `SCOPE_KIND:scope_id`, and one request clears one episode.
  `reset=` (S38f): the error-budget counters the clear resets (`ErrorBudget.reset_rows`) are upserted into
  `arm_budget_counters` (`SQL_BUDGET_UPSERT`, fenced, `must=True`) in the same transaction, so the clear and its counter
  reset (a SERIES streak at 0, the ARM restart watermark at the clear time) commit together or not at all.

Rows hold ids, reason codes, counts and bucket times only; position references are `adapter_id:position_id`.
"""
from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, cast

from src.db.arm_budget_store import SQL_BUDGET_UPSERT
from src.db.arm_ports import ArmFailuresStore, BudgetCounterRow, ClearedFailure, FailedPing, FailureRecord, OpenFailure
from src.db.arm_schema import REASON_PATTERN, SCOPE_KINDS, probe_kind_for
from src.db.epoch import EngineDb
from src.db.fence import FENCE, FencedTx
from src.db.notify_store import insert_outbox
from src.db.pg import Row, json_dumps, json_loads
from src.oversee.notify import OutboxRow

# 64-bit advisory lock key ("XTNLARMF" in ASCII): serialises the FAILED clears (§B14 Clearing).
ARM_FAILED_LOCK: Final = 0x58544E4C41524D46
ID_PATTERN: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}")
POSITION_REF_PATTERN: Final = re.compile(r"[^:\s]+:\S+")
CODE_PATTERN: Final = re.compile(REASON_PATTERN.strip("^$"))
MAX_PING_TITLE: Final = 200
MAX_PING_BODY: Final = 2_000
ARM_PING_CHANNELS: Final = ("arm_email", "arm_push")

SQL_FAILURE_INSERT: Final = (
    "INSERT INTO arm_failures (failure_id, scope_kind, scope_id, edge_code, reason, probe_kind, ping_id, counters, "
    "position_ids, first_fault_at_ms, entered_at_ms, epoch, last_ping_at_ms, ping_count) "
    "SELECT $2::text, $3::text, $4::text, $5::text, $6::text, $7::text, $8::text, $9::text::jsonb, $10::text[], "
    f"$11::bigint, $12::bigint, $1::bigint, $13::bigint, 1 WHERE {FENCE} ON CONFLICT DO NOTHING"
)
#: The kind `arm` copy of the ping (no RETURNING: `xtnl_arm` has INSERT only on the outbox; never expires).
SQL_ARM_PING_INSERT: Final = (
    "INSERT INTO notification_outbox (created_at_ms, severity, channels, subject, body, kind) "
    "SELECT $2::bigint, 'CRITICAL', ARRAY['arm_email', 'arm_push']::text[], $3::text, $4::text, 'arm' "
    f"WHERE {FENCE}"
)
SQL_FAILURE_REPING: Final = (
    "UPDATE arm_failures SET last_ping_at_ms = $3::bigint, ping_count = ping_count + 1 "
    f"WHERE failure_id = $2::text AND ping_id = $4::text AND cleared_at_ms IS NULL AND {FENCE}"
)
SQL_FAILURE_PROBE: Final = (
    "UPDATE arm_failures SET last_probe_at_ms = $3::bigint, last_probe_result = $4::text "
    f"WHERE failure_id = $2::text AND cleared_at_ms IS NULL AND {FENCE}"
)
FAILURE_COLS: Final = (
    "failure_id, scope_kind, scope_id, edge_code, reason, probe_kind, ping_id, counters::text, position_ids, "
    "first_fault_at_ms, entered_at_ms, epoch, ping_count, last_ping_at_ms, last_probe_at_ms, last_probe_result"
)
SQL_FAILURES_OPEN: Final = (
    f"SELECT {FAILURE_COLS} FROM arm_failures WHERE cleared_at_ms IS NULL ORDER BY entered_at_ms, failure_id"
)
SQL_ARM_FAILED_LOCK: Final = "SELECT pg_advisory_xact_lock($1::bigint)"
#: The clear (engine pool only): `$4` must be an `arm_clear_failed` request of this epoch (`$1`) for the episode's
#: scope, with its operator_audit row, that has not cleared another episode.
SQL_FAILURE_CLEAR: Final = (
    "UPDATE arm_failures f SET cleared_at_ms = $3::bigint, clear_request_id = $4::bigint "
    "WHERE f.failure_id = $2::text AND f.cleared_at_ms IS NULL "
    "AND EXISTS (SELECT 1 FROM operator_requests r JOIN operator_audit a ON a.audit_id = r.audit_id "
    "WHERE r.request_id = $4::bigint AND r.kind = 'arm_clear_failed' AND r.requested_epoch = $1::bigint "
    "AND r.target = f.scope_kind || ':' || f.scope_id) "
    "AND NOT EXISTS (SELECT 1 FROM arm_failures g WHERE g.clear_request_id = $4::bigint) "
    f"AND {FENCE} RETURNING f.failure_id, f.scope_kind, f.scope_id"
)

AfterClear = Callable[[FencedTx, ClearedFailure], Awaitable[None]]


def validate_failure(f: FailureRecord) -> None:
    """Refuse a record the 0017 CHECKs would reject, or whose `probe_kind` is not the one its reason selects."""
    if not ID_PATTERN.fullmatch(f.failure_id) or not ID_PATTERN.fullmatch(f.ping_id):
        raise ValueError("failure_id and ping_id are short ids")
    if f.scope_kind not in SCOPE_KINDS or not f.scope_id:
        raise ValueError("unknown scope")
    if f.probe_kind != probe_kind_for(f.scope_kind, f.reason):
        raise ValueError(f"probe_kind {f.probe_kind!r} is not the probe of a {f.scope_kind} {f.reason!r} episode")
    if any(not POSITION_REF_PATTERN.fullmatch(p) for p in f.position_ids):
        raise ValueError("position ids are adapter_id:position_id")
    for name, value in f.counters:
        if not CODE_PATTERN.fullmatch(name) or isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("counters are codes with integer counts or bucket times")


def bounded_ping(ping_id: str, ping: FailedPing) -> FailedPing:
    """The ping as written: the ping id in the (bounded) title, the body cut at `MAX_PING_BODY`."""
    if ping_id not in ping.title[:MAX_PING_TITLE]:
        raise ValueError("the ping id must be in the title")
    body = ping.body if len(ping.body) <= MAX_PING_BODY else ping.body[:MAX_PING_BODY - 1] + "…"
    return FailedPing(title=ping.title[:MAX_PING_TITLE], body=body, created_ms=ping.created_ms)


async def _queue_ping(tx: FencedTx, ping: FailedPing) -> None:
    """The critical copy (Oversee relay) and the kind `arm` copy, in the caller's fenced transaction."""
    await insert_outbox(tx, [OutboxRow(kind="info", severity="critical", title=ping.title, body=ping.body,
                                       created_ms=ping.created_ms)])
    await tx.write(SQL_ARM_PING_INSERT, ping.created_ms, ping.title, ping.body, must=True)


def failure_from_row(r: Row) -> OpenFailure:
    raw = r[7]
    counters_obj = json_loads(raw) if isinstance(raw, (str, bytes)) else (raw or {})
    counters = tuple(sorted((str(k), int(v)) for k, v in cast(Mapping[str, Any], counters_obj).items()))
    rec = FailureRecord(
        failure_id=str(r[0]), scope_kind=str(r[1]), scope_id=str(r[2]), edge_code=None if r[3] is None else str(r[3]),
        reason=str(r[4]), probe_kind=str(r[5]), ping_id=str(r[6]), counters=counters,
        position_ids=tuple(str(p) for p in (r[8] or ())), first_fault_at_ms=int(r[9]), entered_at_ms=int(r[10]))
    return OpenFailure(record=rec, epoch=int(r[11]), ping_count=int(r[12]),
                       last_ping_at_ms=None if r[13] is None else int(r[13]),
                       last_probe_at_ms=None if r[14] is None else int(r[14]),
                       last_probe_result=None if r[15] is None else str(r[15]))


class PgArmFailuresStore(ArmFailuresStore):
    """The FAILED record, re-ping, probe notes and open-episode read over `db` (the ARM pool, or the engine pool
    when the ARM pool is the fault: the caller chooses)."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def record(self, failure: FailureRecord, ping: FailedPing, *, queue_ping: bool = True) -> bool:
        validate_failure(failure)
        out = bounded_ping(failure.ping_id, ping)
        async with self._db.transaction() as tx:
            inserted = await tx.write(
                SQL_FAILURE_INSERT, failure.failure_id, failure.scope_kind, failure.scope_id, failure.edge_code,
                failure.reason, failure.probe_kind, failure.ping_id, json_dumps(dict(failure.counters)),
                list(failure.position_ids), failure.first_fault_at_ms, failure.entered_at_ms, out.created_ms) > 0
            if inserted and queue_ping:
                await _queue_ping(tx, out)
        return inserted

    async def reping(self, failure_id: str, ping_id: str, ping: FailedPing) -> bool:
        out = bounded_ping(ping_id, ping)
        async with self._db.transaction() as tx:
            open_ = await tx.write(SQL_FAILURE_REPING, failure_id, out.created_ms, ping_id) > 0
            if open_:
                await _queue_ping(tx, out)
        return open_

    async def note_probe(self, failure_id: str, at_ms: int, result: str) -> bool:
        async with self._db.transaction() as tx:
            return await tx.write(SQL_FAILURE_PROBE, failure_id, at_ms, result[:64]) > 0

    async def open_episodes(self) -> list[OpenFailure]:
        async with self._db.source.acquire() as conn:
            return [failure_from_row(r) for r in await conn.fetch(SQL_FAILURES_OPEN)]


class PgArmFailedClear:
    """The `arm_clear_failed` clear statement, on the engine (owner) pool only (§B3, §B14 Clearing)."""

    def __init__(self, engine_db: EngineDb) -> None:
        self._db = engine_db

    async def clear(self, request_id: int, failure_id: str, at_ms: int, *, after: AfterClear | None = None,
                    reset: Sequence[BudgetCounterRow] = ()) -> ClearedFailure | None:
        """Set `cleared_at_ms` / `clear_request_id` under `ARM_FAILED_LOCK`; `after` runs in the same transaction
        (the positions' `prior_state` restore, S30) only when the clear applied. None when the episode is not open
        or the request does not match it (the caller answers the request refused)."""
        async with self._db.transaction() as tx:
            await tx.fetchrow(SQL_ARM_FAILED_LOCK, ARM_FAILED_LOCK)
            row = await tx.write_returning(SQL_FAILURE_CLEAR, failure_id, at_ms, request_id)
            if row is None:
                return None
            done = ClearedFailure(failure_id=str(row[0]), scope_kind=str(row[1]), scope_id=str(row[2]),
                                  cleared_at_ms=at_ms, request_id=request_id)
            for r in reset:
                if r.count < 0:
                    raise ValueError("a budget counter is never negative")
                await tx.write(SQL_BUDGET_UPSERT, r.scope_kind, r.scope_id, r.counter, r.count, r.streak_start_ms,
                               r.last_event_ms, at_ms, must=True)
            if after is not None:
                await after(tx, done)
        return done


if TYPE_CHECKING:
    from src.db.arm_ports import ArmFailedClearPort

    def _clear_port(c: PgArmFailedClear) -> ArmFailedClearPort:
        """The clear fills the S30b `ArmFailedClearPort` the FAILED manager calls."""
        return c


__all__ = ["ARM_FAILED_LOCK", "ARM_PING_CHANNELS", "AfterClear", "FAILURE_COLS", "MAX_PING_BODY", "MAX_PING_TITLE",
           "PgArmFailedClear", "PgArmFailuresStore", "SQL_ARM_FAILED_LOCK", "SQL_ARM_PING_INSERT",
           "SQL_FAILURES_OPEN", "SQL_FAILURE_CLEAR", "SQL_FAILURE_INSERT", "SQL_FAILURE_PROBE", "SQL_FAILURE_REPING",
           "bounded_ping", "failure_from_row", "validate_failure"]
