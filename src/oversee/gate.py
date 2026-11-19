"""Fail-closed ActionGate (oversee-rules.md §8, the canonical list).

`allow(action)` is the contract every Phase 2 sender must call before every send. It returns False
unless every condition holds; any reader error or timeout is False. Phase 1 has no sender.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

from src.oversee.types import HaltLevel


@dataclass(frozen=True)
class Action:
    """A proposed outbound action. `risk_increasing` is False only for a purely risk-reducing action."""

    account_id: str
    adapter_id: str
    risk_increasing: bool
    symbol: str | None = None
    description: str = ""


@dataclass(frozen=True)
class GateContext:
    """Everything `allow()` checks, read by a GateContextReader from Redis, PostgreSQL and the sweep."""

    now_ms: int
    redis_boot_id: str | None          # Redis redis:boot_id
    redis_run_id: str | None           # Redis INFO run_id
    node_epoch: int | None             # this node's epoch
    engine_epoch: int | None           # PostgreSQL engine_epoch
    lease_valid: bool
    halt_state: tuple[HaltLevel, int] | None   # Redis halt:state = (effective_level, halt_event_id)
    max_halt_event_id: int | None      # PostgreSQL MAX(halt_events.id)
    settle_until_ms: int               # takeover settle window end (TTL + TCP_USER_TIMEOUT + ack timeout)
    reconciled_at_ms: int | None       # the account's latest reconcile commit
    reconciled_epoch: int | None       # epoch of that reconcile
    latest_open_ms: int | None         # latest market open for the account's adapter (SM10); None = unknown
    open_stream_health: bool           # any stream_health row of the adapter has conn_lost_at set
    stale_mark_symbols: frozenset[str]
    has_unknown_result: bool           # the latest pass has an UNKNOWN result for the account
    last_pass_ms: int | None           # end of the RiskSweep's last complete pass over the account
    sweep_interval_ms: int = 1_000
    freshness_passes: int = 3
    # The adapter's newest downtime catch-up of this term was complete and nothing interrupted its orders /
    # account path since (src/runtime/catchup.py `CatchupCoordinator.verified`). Fail closed: False unless read.
    catchup_verified: bool = False
    # K6: the account's own adapter (account_state, else its positions / orders, else its halt_breaches rows:
    # `adapter_of`, §8). The stream_health, reconcile-after-open, latest-open and catch-up inputs above are read for
    # this adapter, never for whatever adapter the action names. Fail closed: None (unresolved) denies.
    account_adapter_id: str | None = None


@dataclass(frozen=True)
class GateVerdict:
    allowed: bool
    reason: str


class GateContextReader(Protocol):
    async def read(self, action: Action) -> GateContext: ...


def evaluate_gate(ctx: GateContext, action: Action) -> GateVerdict:
    """The canonical §8 list, in order; the first failing condition names the denial."""
    if ctx.node_epoch is None or ctx.redis_run_id is None or ctx.redis_boot_id is None:
        return GateVerdict(False, "boot_id_missing")
    if ctx.redis_boot_id != f"{ctx.redis_run_id}:{ctx.node_epoch}":
        return GateVerdict(False, "boot_id_mismatch")
    if ctx.engine_epoch is None or ctx.node_epoch != ctx.engine_epoch:
        return GateVerdict(False, "epoch_mismatch")
    if not ctx.lease_valid:
        return GateVerdict(False, "lease_invalid")
    if ctx.halt_state is None:
        return GateVerdict(False, "halt_state_missing")
    level, event_id = ctx.halt_state
    if ctx.max_halt_event_id is None or event_id != ctx.max_halt_event_id:
        return GateVerdict(False, "halt_state_not_current")
    if level is HaltLevel.HARD:
        return GateVerdict(False, "hard_halt")
    if level is HaltLevel.SOFT and action.risk_increasing:
        return GateVerdict(False, "soft_halt_risk_increasing")
    # K6: every adapter-scoped condition is judged against the account's own adapter. An account that resolves to
    # no adapter, or an action that names another adapter than the account's, is denied whatever its direction.
    if ctx.account_adapter_id is None:
        return GateVerdict(False, "account_adapter_unknown")
    if action.adapter_id != ctx.account_adapter_id:
        return GateVerdict(False, "adapter_mismatch")
    if not action.risk_increasing:
        return GateVerdict(True, "ok")
    if ctx.now_ms < ctx.settle_until_ms:
        return GateVerdict(False, "settle_window")
    if ctx.reconciled_at_ms is None or ctx.reconciled_epoch != ctx.node_epoch:
        return GateVerdict(False, "not_reconciled")
    if ctx.latest_open_ms is None:   # calendar not loaded or no session for the adapter: can't verify
        return GateVerdict(False, "latest_open_unknown")
    if ctx.reconciled_at_ms < ctx.latest_open_ms:
        return GateVerdict(False, "not_reconciled")
    if ctx.open_stream_health:
        return GateVerdict(False, "stream_health_open")
    if not ctx.catchup_verified:
        return GateVerdict(False, "catchup_unverified")
    if ctx.stale_mark_symbols:
        return GateVerdict(False, "stale_mark")
    if ctx.has_unknown_result:
        return GateVerdict(False, "unknown_result")
    if ctx.last_pass_ms is None or ctx.now_ms - ctx.last_pass_ms >= ctx.freshness_passes * ctx.sweep_interval_ms:
        return GateVerdict(False, "sweep_not_current")
    return GateVerdict(True, "ok")


class ActionGate:
    """`allow(action)` -> bool; fails closed on any error or timeout (§8)."""

    def __init__(self, reader: GateContextReader | None, timeout_s: float = 0.5) -> None:
        self._reader = reader
        self._timeout_s = timeout_s

    async def check(self, action: Action) -> GateVerdict:
        if self._reader is None:
            return GateVerdict(False, "no_reader")
        try:
            ctx = await asyncio.wait_for(self._reader.read(action), self._timeout_s)
            return evaluate_gate(ctx, action)
        except Exception as exc:  # any Redis / PostgreSQL error or timeout -> deny
            return GateVerdict(False, f"error:{type(exc).__name__}")

    async def allow(self, action: Action) -> bool:
        return (await self.check(action)).allowed
