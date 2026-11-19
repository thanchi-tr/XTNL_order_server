"""GateContextReader (src/oversee/gate.py) over PostgreSQL + Redis + in-process state (oversee-rules.md §8).

One PostgreSQL round trip (engine_epoch.epoch, MAX(halt_events.id), the adapter's stream_health rows)
and one Redis pipeline (redis:boot_id, halt:state) plus INFO run_id. In-process inputs are injected:
this node's epoch and lease validity (lease keeper), the takeover settle window, the latest market open
(SM10) and the RiskSweep's per-account view. Any error propagates: `ActionGate` turns it into a denial.

Reconciled-in-this-epoch: every stream_health row of the adapter must carry `reconciled_epoch` = this
node's epoch; `reconciled_at_ms` is the oldest of them (the account is reconciled only once all are).

K6 (§8): "the adapter" is the **account's own** adapter, resolved in the same round trip with `adapter_of`
(account_state, else its positions / orders, else its halt_breaches rows), never the adapter the action names.
The stream_health rows, the latest open and the catch-up verdict are read for that adapter; an account that
resolves to none reads no stream rows and is denied (`account_adapter_unknown`), and an action naming another
adapter is denied (`adapter_mismatch`).
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final

import redis.asyncio as aioredis

from src.cache.live_reader import RunIdReader, info_run_id, read_gate_keys
from src.db.epoch import EngineDb
from src.db.pg import Row
from src.db.sweep_writer import adapter_of
from src.oversee.gate import Action, GateContext
from src.oversee.sweep import AccountView
from src.oversee.types import HaltLevel

SQL_GATE_PG: Final = (
    "SELECT (SELECT epoch FROM engine_epoch WHERE id = 1), (SELECT max(id) FROM halt_events), "
    f"{adapter_of('$1')}"
)
SQL_GATE_STREAMS: Final = (
    "SELECT stream, conn_lost_at, reconciled_at, reconciled_epoch FROM stream_health WHERE adapter_id = $1"
)


def _no_open(adapter_id: str) -> int | None:
    """No SM10 calendar wired: the latest open is unknown, so risk-increasing actions are denied."""
    return None


def _unverified(adapter_id: str) -> bool:
    """No downtime catch-up wired: nothing proves the book, so risk-increasing actions are denied."""
    return False


@dataclass(frozen=True)
class GateInputs:
    node_epoch: Callable[[], int | None]
    lease_valid: Callable[[], bool]
    settle_until_ms: Callable[[], int]
    sweep_view: Callable[[str], AccountView | None]
    clock_ms: Callable[[], int]
    latest_open_ms: Callable[[str], int | None] = _no_open
    sweep_interval_ms: int = 1_000
    #: adapter -> its downtime catch-up proved the book (`CatchupCoordinator.verified`); unwired = unverified.
    catchup_verified: Callable[[str], bool] = _unverified


def reconcile_state(rows: Sequence[Row], node_epoch: int | None) -> tuple[bool, int | None, int | None]:
    """(open_stream_health, reconciled_at_ms, reconciled_epoch) from the adapter's stream_health rows."""
    if not rows:
        return False, None, None
    open_health = any(r[1] is not None for r in rows)
    if any(r[2] is None for r in rows):
        return open_health, None, None
    epochs = {None if r[3] is None else int(r[3]) for r in rows}
    epoch = node_epoch if epochs == {node_epoch} else next(iter(sorted(epochs - {node_epoch}, key=str)))
    return open_health, min(int(r[2]) for r in rows), epoch


class TieredGateContextReader:
    def __init__(self, db: EngineDb, redis: aioredis.Redis, inputs: GateInputs,
                 run_id_reader: RunIdReader = info_run_id) -> None:
        self._db = db
        self._redis = redis
        self._in = inputs
        self._run_id = run_id_reader

    async def read(self, action: Action) -> GateContext:
        async with self._db.source.acquire() as conn:
            pg = await conn.fetchrow(SQL_GATE_PG, action.account_id)
            account_adapter = None if pg is None or pg[2] is None else str(pg[2])
            streams = [] if account_adapter is None else await conn.fetch(SQL_GATE_STREAMS, account_adapter)
        keys = await read_gate_keys(self._redis, self._run_id)
        node_epoch = self._in.node_epoch()
        open_health, reconciled_at, reconciled_epoch = reconcile_state(streams, node_epoch)
        halt: tuple[HaltLevel, int] | None = None
        if keys.halt_state is not None and keys.halt_state[0] in HaltLevel.__members__:
            halt = (HaltLevel[keys.halt_state[0]], keys.halt_state[1])
        view = self._in.sweep_view(action.account_id)
        return GateContext(
            now_ms=self._in.clock_ms(), redis_boot_id=keys.boot_id, redis_run_id=keys.run_id,
            node_epoch=node_epoch,
            engine_epoch=None if pg is None or pg[0] is None else int(pg[0]),
            lease_valid=self._in.lease_valid(), halt_state=halt,
            max_halt_event_id=None if pg is None or pg[1] is None else int(pg[1]),
            settle_until_ms=self._in.settle_until_ms(), reconciled_at_ms=reconciled_at,
            reconciled_epoch=reconciled_epoch,
            latest_open_ms=None if account_adapter is None else self._in.latest_open_ms(account_adapter),
            open_stream_health=open_health,
            stale_mark_symbols=frozenset(s for _, s in view.stale_symbols) if view else frozenset(),
            has_unknown_result=view.has_unknown if view else True,
            last_pass_ms=view.last_pass_ms if view else None,
            sweep_interval_ms=self._in.sweep_interval_ms,
            catchup_verified=account_adapter is not None and self._in.catchup_verified(account_adapter),
            account_adapter_id=account_adapter,
        )
