"""SM3 incident path for PostgreSQL damage (tier-persistence.md §3 step 2, operations.md §7).

HALTED -> SWITCHING_OVER (damage confined to the primary, the sync standby passes the same check)
       -> MANUAL_INTERVENTION (standby damaged too, Patroni unusable, or the switchover failed)
SWITCHING_OVER -> RECONCILING (promoted; the damaged member is reinitialised) | MANUAL_INTERVENTION
MANUAL_INTERVENTION -> RECONCILING only through operator_restored() (Tier 3 runbook done via Patroni)
RECONCILING: resume prices first -> broker reconcile committed -> FLUSHDB (removes redis:boot_id) -> SM8 ->
             resume full ingestion once SM8 is READY -> IDLE. A reconcile failing repeatedly
             ("divergence persists") -> MANUAL_INTERVENTION.
The engine never restores PostgreSQL itself. Each advance() is one non-blocking tick's worth of work.
"""
from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Final, Literal

import redis.asyncio as aioredis

from src.integrity.dbcheck import DbFinding
from src.integrity.ports import (
    BootSignal,
    BrokerReconciler,
    IngestionControl,
    IntegrityAlert,
    IntegrityState,
    Pager,
    PatroniControl,
    null_pager,
)

log = logging.getLogger(__name__)

ReconcilePhase = Literal["prices", "reconcile", "flush", "await_sm8"]
MAX_RECONCILE_ATTEMPTS: Final = 3
MAX_PATRONI_ATTEMPTS: Final = 3

S = IntegrityState


class IncidentMachine:
    def __init__(
        self,
        *,
        redis: aioredis.Redis,
        ingestion: IngestionControl,
        patroni: PatroniControl,
        reconciler: BrokerReconciler,
        boot: BootSignal,
        boot_ready: Callable[[], Awaitable[bool]],
        clock_ms: Callable[[], int],
        pager: Pager = null_pager,
        on_change: Callable[[IntegrityState | None, IntegrityState | None, str], None] | None = None,
        on_resolved: Callable[[], None] | None = None,
    ) -> None:
        self._redis = redis
        self._ingestion = ingestion
        self._patroni = patroni
        self._reconciler = reconciler
        self._boot = boot
        self._boot_ready = boot_ready
        self._clock_ms = clock_ms
        self._pager = pager
        self._on_change = on_change
        self._on_resolved = on_resolved
        self.state: IntegrityState | None = None
        self.findings: tuple[DbFinding, ...] = ()
        self.failover_loss = False
        self.damaged_member: str | None = None
        self.phase: ReconcilePhase = "prices"
        self.cash_flows_since_ms: int | None = None
        self._attempts = 0

    @property
    def active(self) -> bool:
        return self.state is not None

    # ---- entry points ------------------------------------------------------------------------------
    async def open(self, findings: Sequence[DbFinding]) -> None:
        """Corruption or seq_id regression found: HALTED (halt ingestion, critical alert)."""
        if self.active:
            return
        self.findings = tuple(findings)
        self.failover_loss = all(not f.is_damage for f in findings)
        self._attempts = 0
        self._go(S.HALTED, "; ".join(f"{f.kind}:{f.key}" for f in findings[:5]))
        await self._ingestion.halt_ingestion("integrity incident")
        self._page("critical", "integrity_halted",
                   f"PostgreSQL integrity check failed ({len(findings)} findings): "
                   + "; ".join(f"{f.kind} {f.key}: {f.detail}" for f in findings[:5]))

    async def operator_restored(self, restore_target_ms: int | None) -> None:
        """Operator Tier 3 restore runbook done: MANUAL_INTERVENTION -> RECONCILING."""
        if self.state is not S.MANUAL_INTERVENTION:
            raise RuntimeError(f"operator_restored() is only valid in MANUAL_INTERVENTION, not {self.state}")
        self.cash_flows_since_ms = restore_target_ms
        self._start_reconciling("operator restore done")

    # ---- per-tick advance ------------------------------------------------------------------------------
    async def advance(self) -> None:
        if self.state is S.HALTED:
            await self._from_halted()
        elif self.state is S.SWITCHING_OVER:
            await self._switch_over()
        elif self.state is S.RECONCILING:
            await self._reconcile_step()

    async def _from_halted(self) -> None:
        if self.failover_loss:
            # Data lost across a Patroni failover: nothing to switch away from; re-read the broker.
            self._start_reconciling("seq_id regression across a timeline switch")
            return
        try:
            self.damaged_member = await self._patroni.primary_member()
            standby_ok = await self._patroni.standby_passes_check()
        except Exception as exc:
            self._attempts += 1
            log.warning("integrity: Patroni check failed (%d): %s", self._attempts, exc)
            if self._attempts >= MAX_PATRONI_ATTEMPTS:
                self._manual(f"Patroni unusable: {exc}")
            return
        self._attempts = 0
        if standby_ok:
            self._go(S.SWITCHING_OVER, "damage confined to the primary; standby passes")
        else:
            self._manual("sync standby damaged too")

    async def _switch_over(self) -> None:
        try:
            new_primary = await self._patroni.switchover()
        except Exception as exc:
            self._manual(f"switchover failed: {exc}")
            return
        if self.damaged_member is not None:
            try:
                await self._patroni.reinit(self.damaged_member)
            except Exception as exc:
                self._page("high", "patroni_reinit_failed", f"reinit {self.damaged_member} failed: {exc}")
        self._start_reconciling(f"promoted {new_primary}")

    async def _reconcile_step(self) -> None:
        try:
            if self.phase == "prices":
                await self._ingestion.resume_prices()
                self.phase = "reconcile"
                self._attempts = 0
            if self.phase == "reconcile":
                if not await self._reconciler.reconcile_all(self.cash_flows_since_ms):
                    raise RuntimeError("reconcile did not commit")
                self.phase = "flush"
            if self.phase == "flush":
                await self._redis.flushdb()     # also removes redis:boot_id -> SM8 rebuilds from PostgreSQL
                self._boot.signal("integrity reconcile FLUSHDB")
                self.phase = "await_sm8"
            if self.phase == "await_sm8":
                if not await self._boot_ready():
                    return
                await self._ingestion.resume_all()
                self._resolve()
        except Exception as exc:
            self._attempts += 1
            log.warning("integrity RECONCILING %s failed (%d): %s", self.phase, self._attempts, exc)
            if self.phase == "reconcile" and self._attempts >= MAX_RECONCILE_ATTEMPTS:
                self._manual(f"divergence persists: {exc}")

    # ---- helpers ----------------------------------------------------------------------------------------
    def _start_reconciling(self, why: str) -> None:
        self.phase = "prices"
        self._attempts = 0
        if self._on_resolved is not None:
            self._on_resolved()             # new primary / restored data: fresh monotonicity baselines
        self._go(S.RECONCILING, why)

    def _manual(self, why: str) -> None:
        self._go(S.MANUAL_INTERVENTION, why)
        self._page("critical", "integrity_manual_intervention",
                   f"{why}: run the Tier 3 restore runbook through Patroni, then call operator_restored()")

    def _resolve(self) -> None:
        self._page("info", "integrity_resolved", "reconciled and SM8 READY: full ingestion resumed")
        self.findings = ()
        self.cash_flows_since_ms = None
        self.damaged_member = None
        self._go(None, "resolved")

    def _go(self, new: IntegrityState | None, why: str) -> None:
        old = self.state
        self.state = new
        log.warning("SM3 incident %s -> %s (%s)", old, new, why)
        if self._on_change is not None:
            self._on_change(old, new, why)

    def _page(self, severity: Literal["info", "high", "critical"], code: str, detail: str) -> None:
        self._pager(IntegrityAlert(severity, code, detail, self._clock_ms()))
