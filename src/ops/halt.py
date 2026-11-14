"""Operator halt clear for the CLI (oversee-rules.md s4, s8; tier-persistence.md s4).

The decision is `HaltSwitch.operator_clear` itself (src/oversee/halt.py), run over a HaltStore whose
transaction is the CLI's: a fenced READ COMMITTED transaction on the epoch read at the start of the command,
`pg_advisory_xact_lock(HALT_LOCK)` first, the statements of `PgHaltTxn`. Within that same transaction, when
the clear changed anything, it also writes

* one `oversee_events` row per rule state the clear resolved (`transition = resolved`, resolved by the
  operator; only a forced clear resolves states), and
* the `operator_audit` row.

The account named on the command line scopes a forced clear: it is refused while an active HARD state of
another account exists (that account needs its own reviewed clear). A clear only releases the HARD latch
and lowers the level to the sweep's target; it never enables any action (Phase 1 sends nothing, and
ActionGate keeps every other condition), and the sweep keeps evaluating every condition afterwards.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from src.db.epoch import HALT_LOCK, epoch_transaction
from src.db.fence import FencedTx
from src.db.halt_store import SQL_HALT_LOCK, PgHaltTxn
from src.db.pg import PgConnection
from src.oversee.halt import HaltEvent, HaltTxn, NewHaltEvent
from src.oversee.types import BreachState, Level, Transition

from src.ops.model import OpsRefused

AfterHook = Callable[[FencedTx, "OpsHaltTxn"], Awaitable[None]]


def operator_sweep_id(now_ms: int) -> int:
    """An HLC-shaped id (`now_ms << 20`) for the oversee_events rows of an operator clear: it lands in the
    month partition of its time and is unique per (rule, subject, transition) at millisecond resolution."""
    return now_ms << 20


class OpsHaltTxn(PgHaltTxn):
    """PgHaltTxn that records what the clear wrote and enforces the account scope of a forced clear."""

    def __init__(self, tx: FencedTx, *, account_id: str, force: bool) -> None:
        super().__init__(tx)
        self.fenced = tx
        self.account_id = account_id
        self.force = force
        self.saved: list[BreachState] = []
        self.inserted: HaltEvent | None = None

    async def breach_states(self) -> list[BreachState]:
        states = await super().breach_states()
        if self.force:
            other = sorted("/".join(s.key) + f"@{s.account_id}" for s in states
                           if s.active and s.level >= Level.HARD_HALT and s.account_id != self.account_id)
            if other:
                raise OpsRefused("active_hard_state_on_other_account", ";".join(other))
        return states

    async def save_states(self, states: Sequence[BreachState]) -> None:
        await super().save_states(states)
        self.saved.extend(states)

    async def insert_event(self, ev: NewHaltEvent) -> HaltEvent:
        out = await super().insert_event(ev)
        self.inserted = out
        return out

    @property
    def changed(self) -> bool:
        return bool(self.saved) or self.inserted is not None

    def resolved_by_operator(self, now_ms: int) -> list[BreachState]:
        return [s for s in self.saved if s.resolved_by == "operator" and s.resolved_at == now_ms]


def resolved_transitions(states: Sequence[BreachState], *, now_ms: int, reason: str) -> list[Transition]:
    sweep_id = operator_sweep_id(now_ms)
    return [Transition(s.rule_id, s.subject_type, s.subject_id, s.account_id, "resolved", sweep_id, now_ms,
                       Level.PASS, s.cap, f"operator_clear: {reason}"[:500], s.activation_id)
            for s in states]


class OpsHaltStore:
    """HaltStore for one CLI command: HALT_LOCK + fenced transaction on `token`; `after` runs inside the
    transaction, before COMMIT, only when the clear changed something."""

    def __init__(self, conn: PgConnection, token: int, *, account_id: str, force: bool, after: AfterHook) -> None:
        self._conn = conn
        self._token = token
        self._account_id = account_id
        self._force = force
        self._after = after
        self.last: OpsHaltTxn | None = None

    @asynccontextmanager
    async def _txn(self) -> AsyncIterator[HaltTxn]:
        async with epoch_transaction(self._conn, self._token) as tx:
            await tx.fetchrow(SQL_HALT_LOCK, HALT_LOCK)
            htx = OpsHaltTxn(tx, account_id=self._account_id, force=self._force)
            self.last = htx
            yield htx
            if htx.changed:
                await self._after(tx, htx)

    def transaction(self) -> AbstractAsyncContextManager[HaltTxn]:
        return self._txn()


def clear_detail(htx: OpsHaltTxn, *, now_ms: int, reviewed_event_id: int | None, force: bool) -> dict[str, Any]:
    """The operator_audit detail of an applied clear."""
    ev = htx.inserted
    return {
        "reviewed_event_id": reviewed_event_id, "force": force,
        "halt_event_id": None if ev is None else ev.id,
        "effective_level": None if ev is None else ev.effective_level.name,
        "resolved": ["/".join(s.key) for s in htx.resolved_by_operator(now_ms)],
        "states_reviewed": len(htx.saved),
    }


class InTxHaltStore:
    """HaltStore over a HaltTxn the caller already holds (inside its fenced transaction and HALT_LOCK): the
    CLI's `halt ack` runs `HaltSwitch.acknowledge` in the same transaction as its operator_audit row."""

    def __init__(self, htx: HaltTxn) -> None:
        self._htx = htx

    @asynccontextmanager
    async def _txn(self) -> AsyncIterator[HaltTxn]:
        yield self._htx

    def transaction(self) -> AbstractAsyncContextManager[HaltTxn]:
        return self._txn()
