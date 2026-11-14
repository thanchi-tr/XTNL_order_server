"""Single-leader epoch (operations.md §2, skill §1 P12): lock-free fencing and its named exceptions.

* `Epoch` = (etcd cluster_id, lease revision). The fencing token is the revision, stored in
  `engine_epoch.epoch` (a column generated from `revision`, migration 0002).
* `bump_epoch`: LEADER_INIT step 1. Updates engine_epoch only when the revision is **newer** for the same
  cluster_id (it only ever increases); a different cluster_id or a 0-row bump -> EpochFenced. Extra
  LEADER_INIT work runs in the same transaction (`in_txn`). After COMMIT, backends whose
  application_name carries an older epoch are terminated (best effort), which rolls back any in-flight
  transaction of a paused ex-leader.
* `epoch_transaction`: every engine write transaction, READ COMMITTED, handing out a `FencedTx`: each
  state write carries `(SELECT epoch FROM engine_epoch WHERE id = 1) = $1` (src/db/fence.py). No advisory
  lock; `SET LOCAL statement_timeout = 0` before COMMIT; a failed COMMIT is an unknown outcome.
* Named exception 1, `snapshot_transaction`: READ ONLY REPEATABLE READ, writes nothing (RiskSweep read).
* Named exception 2, `spill_drain_insert`: idempotent INSERT ... ON CONFLICT (payload_hash) DO NOTHING
  into events_dlq, which any node may run (tier-persistence.md §5).
* The cTrader `token_generation` compare-and-set is the third unfenced write (src/db/tokens.py).
"""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Final, TypeVar

from src.db.fence import EpochMismatch, FencedTx
from src.db.pg import ConnectionSource, PgConnection, rowcount
from src.ports import DlqRow

log = logging.getLogger(__name__)
T = TypeVar("T")

# 64-bit advisory lock key ("XTNLHALT" in ASCII): serialises trip / clear (oversee-rules.md §8).
# It is not part of fencing; there is no epoch lock.
HALT_LOCK: Final = 0x58544E4C48414C54

SQL_BEGIN_RC: Final = "BEGIN ISOLATION LEVEL READ COMMITTED"
SQL_BEGIN_SNAPSHOT: Final = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
SQL_LOCK_TIMEOUT_2S: Final = "SET LOCAL lock_timeout = '2s'"
SQL_READ_EPOCH: Final = "SELECT cluster_id, revision FROM engine_epoch WHERE id = 1"
SQL_READ_EPOCH_FOR_UPDATE: Final = "SELECT cluster_id, revision FROM engine_epoch WHERE id = 1 FOR UPDATE"
SQL_INSERT_FIRST_EPOCH: Final = (
    "INSERT INTO engine_epoch (id, cluster_id, revision, node, bumped_at_ms) "
    "VALUES (1, $1, $2, $3, now_ms()) ON CONFLICT (id) DO NOTHING"
)
SQL_BUMP_EPOCH: Final = (
    "UPDATE engine_epoch SET revision = $2, node = $3, bumped_at_ms = now_ms() "
    "WHERE id = 1 AND cluster_id = $1 AND revision < $2"
)
SQL_TERMINATE_OLDER_EPOCHS: Final = (
    "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
    "WHERE pid <> pg_backend_pid() AND application_name ~ '^xtnl:[0-9]+:' "
    "AND split_part(application_name, ':', 2)::bigint < $1::bigint"
)
SQL_COMMIT_TIMEOUT_OFF: Final = "SET LOCAL statement_timeout = 0"
SQL_COMMIT: Final = "COMMIT"
SQL_ROLLBACK: Final = "ROLLBACK"
#: Idempotent on payload_hash in the active table AND in events_dlq_history (0009: RESOLVED rows moved there
#: by the lifecycle task), so a late re-drain never re-opens an archived row. A lineage origin that has
#: been archived is dropped from origin_dlq_id (the self-FK would otherwise fail the whole batch).
#: `u.h::bpchar`: payload_hash is char(64); comparing it with text would cast the column to text and skip
#: events_dlq_history_hash_idx (a sequential scan of the whole history per parked row).
SQL_SPILL_DRAIN_INSERT: Final = (
    "INSERT INTO events_dlq (event_type, entity_id, adapter_id, raw_payload, payload_hash, error_reason, "
    "origin_dlq_id) "
    "SELECT u.t, u.e, u.a, u.p, u.h, u.r, (SELECT o.dlq_id FROM events_dlq o WHERE o.dlq_id = u.o) "
    "FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::bigint[]) "
    "AS u(t, e, a, p, h, r, o) "
    "WHERE NOT EXISTS (SELECT 1 FROM events_dlq_history x WHERE x.payload_hash = u.h::bpchar) "
    "ON CONFLICT (payload_hash) DO NOTHING"
)


@dataclass(frozen=True)
class Epoch:
    """Epoch = (etcd cluster_id, lease revision); `token` is the fencing token in engine_epoch.epoch."""

    cluster_id: str
    revision: int

    @property
    def token(self) -> int:
        return self.revision

    @property
    def label(self) -> str:
        """The epoch part of redis:boot_id = run_id:epoch (the fencing token, as ActionGate compares it)."""
        return str(self.revision)


def application_name(epoch: Epoch, node: str) -> str:
    """'xtnl:<revision>:<node>' (<= 63 bytes) so a newer leader can find older-epoch backends."""
    return f"xtnl:{epoch.revision}:{node}"[:63]


class EpochFenced(RuntimeError):
    """The bump was refused (different cluster_id or a 0-row bump): FENCED + critical alert."""


class CommitUnknownError(RuntimeError):
    """COMMIT failed or the connection dropped at COMMIT: the outcome is unknown; re-read before acting."""


async def _rollback(conn: PgConnection) -> None:
    try:
        await conn.execute(SQL_ROLLBACK)
    except Exception as exc:  # the connection may already be gone; the server rolls back anyway
        log.debug("rollback failed: %s", exc)


async def _commit(conn: PgConnection) -> None:
    try:
        await conn.execute(SQL_COMMIT)
    except Exception as exc:
        raise CommitUnknownError("COMMIT outcome unknown; re-read before acting") from exc


@asynccontextmanager
async def epoch_transaction(conn: PgConnection, epoch: Epoch | int) -> AsyncIterator[FencedTx]:
    """Every engine write transaction (operations.md §2): READ COMMITTED, fenced writes, no lock.
    `epoch` is this node's Epoch or its bare fencing token (the RiskSweep passes the token)."""
    token = epoch.token if isinstance(epoch, Epoch) else int(epoch)
    await conn.execute(SQL_BEGIN_RC)
    try:
        yield FencedTx(conn, token)
        await conn.execute(SQL_COMMIT_TIMEOUT_OFF)
    except BaseException:
        await _rollback(conn)
        raise
    await _commit(conn)


@asynccontextmanager
async def snapshot_transaction(conn: PgConnection) -> AsyncIterator[PgConnection]:
    """Named exception 1: READ ONLY REPEATABLE READ snapshot, no fence, writes nothing."""
    await conn.execute(SQL_BEGIN_SNAPSHOT)
    try:
        yield conn
    finally:
        await _rollback(conn)  # read only: ending with ROLLBACK is equivalent and never waits on sync rep


async def spill_drain_insert(conn: PgConnection, rows: Sequence[DlqRow]) -> int:
    """Named exception 2: idempotent append-only DLQ insert, outside the fence. Returns rows inserted."""
    if not rows:
        return 0
    status = await conn.execute(
        SQL_SPILL_DRAIN_INSERT,
        [r.event_type for r in rows], [r.entity_id for r in rows], [r.adapter_id for r in rows],
        [r.raw_payload for r in rows], [r.payload_hash for r in rows], [r.error_reason for r in rows],
        [r.origin_dlq_id for r in rows],
    )
    return rowcount(status)


async def bump_epoch(
    conn: PgConnection,
    new: Epoch,
    node: str,
    *,
    in_txn: Callable[[PgConnection], Awaitable[T]] | None = None,
    terminate_older: bool = True,
) -> T | None:
    """LEADER_INIT step 1: commit the epoch bump (plus `in_txn` work in the same transaction).

    The epoch only increases: the UPDATE applies only for a newer revision of the same cluster_id.
    Raises EpochFenced for a different cluster_id or a 0-row bump (operator sign-off required). The row
    lock taken here serialises concurrent bumps only; fenced engine writes never wait on it.
    """
    await conn.execute(SQL_BEGIN_RC)
    try:
        await conn.execute(SQL_LOCK_TIMEOUT_2S)
        row = await conn.fetchrow(SQL_READ_EPOCH_FOR_UPDATE)
        if row is None:
            if rowcount(await conn.execute(SQL_INSERT_FIRST_EPOCH, new.cluster_id, new.revision, node)) == 0:
                raise EpochFenced("engine_epoch was created concurrently; retry LEADER_INIT")
        elif str(row[0]) != new.cluster_id:
            raise EpochFenced(f"cluster_id {row[0]!r} != {new.cluster_id!r}: operator sign-off required")
        elif rowcount(await conn.execute(SQL_BUMP_EPOCH, new.cluster_id, new.revision, node)) == 0:
            raise EpochFenced(f"0-row bump: stored revision {row[1]} >= {new.revision}")
        result = await in_txn(conn) if in_txn is not None else None
        await conn.execute(SQL_COMMIT_TIMEOUT_OFF)
    except BaseException:
        await _rollback(conn)
        raise
    await _commit(conn)
    if terminate_older:
        try:
            await conn.fetchrow(SQL_TERMINATE_OLDER_EPOCHS, new.revision)
        except Exception as exc:  # needs pg_signal_backend; the fence holds without it
            log.warning("could not terminate older-epoch backends: %s", exc)
    return result


async def read_epoch(conn: PgConnection) -> Epoch | None:
    row = await conn.fetchrow(SQL_READ_EPOCH)
    return None if row is None else Epoch(str(row[0]), int(row[1]))


class EngineDb:
    """Connection source + this node's current epoch: hands out the transaction kinds above."""

    def __init__(self, source: ConnectionSource, epoch: Callable[[], Epoch]) -> None:
        self.source = source
        self._epoch = epoch

    @property
    def epoch(self) -> Epoch:
        return self._epoch()

    @asynccontextmanager
    async def _tx(self, token: int | None) -> AsyncIterator[FencedTx]:
        async with self.source.acquire() as conn:
            async with epoch_transaction(conn, self._epoch() if token is None else token) as tx:
                yield tx

    def transaction(self, token: int | None = None) -> AbstractAsyncContextManager[FencedTx]:
        """Fenced READ COMMITTED transaction (this node's epoch, or an explicit fencing token)."""
        return self._tx(token)

    @asynccontextmanager
    async def _snap(self) -> AsyncIterator[PgConnection]:
        async with self.source.acquire() as conn:
            async with snapshot_transaction(conn) as tx:
                yield tx

    def snapshot(self) -> AbstractAsyncContextManager[PgConnection]:
        """Named exception 1 (RiskSweep read snapshot)."""
        return self._snap()

    async def spill_drain_insert(self, rows: Sequence[DlqRow]) -> int:
        """Named exception 2 (any node)."""
        async with self.source.acquire() as conn:
            return await spill_drain_insert(conn, rows)


__all__ = [
    "HALT_LOCK", "CommitUnknownError", "EngineDb", "Epoch", "EpochFenced", "EpochMismatch", "application_name",
    "bump_epoch", "epoch_transaction", "read_epoch", "snapshot_transaction", "spill_drain_insert",
]
