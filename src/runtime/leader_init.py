"""LEADER_INIT orchestration in the documented order (operations.md §4):

    1. commit the epoch bump + stream_health COALESCE fence + leader_terms (one fenced transaction;
       SAFE_HOLD set and the term aborted in it when due)
    2. re-seed the HLC from MAX(seq_id) (after the bump commits)
    3. FLUSHDB the local Redis: the engine's own database only, never FLUSHALL (another database on the
       same server is not the engine's to wipe)
       ruleset: install the active `rulesets` row before the first RiskSweep pass (IMPLEMENTATION_PLAN 4.5;
       an invalid row falls back and pages, it never exits)
    4. sod catch-up (a partial_session row raises a FLAG; never exits)
    5. SM10 init from the versioned schedule
    6. start SM4 for every adapter in parallel with SM8, all steps (SM8 writes redis:boot_id last);
       the RiskSweep starts at once, reading PostgreSQL
    7. DLQ start-up barrier
    then leader_terms.reached_leader_at.

Any failure other than a due SAFE_HOLD is a hard exit (`init_failed`). A lost fence (`EpochMismatch`,
from any step, `mark_reached_leader` included: a successor closed this term) is a hard exit with reason
`fence` (IMPLEMENTATION_PLAN 4.9). The 120 s deadline is
enforced by the lease-keeper thread (`LeaseKeeper.begin_leader_init`) and, as a second line, by
`asyncio.wait_for` here. Every dependency is a protocol; the coordinator wires the real ones.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from src.db.epoch import Epoch
from src.runtime.exits import ExitFn, hard_exit

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FenceOutcome:
    """Result of step 1 (see src/db/leader.py LeaderInitFence)."""

    safe_hold: bool
    safe_hold_reason: str | None = None
    streams_fenced: int = 0
    predecessors_closed: int = 0


@dataclass(frozen=True)
class SodOutcome:
    rows_written: int = 0
    partial_session: bool = False


class EpochFencer(Protocol):
    async def bump_and_fence(self, epoch: Epoch, node_id: str, now_ms: int) -> FenceOutcome:
        """`bump_epoch(conn, epoch, node, in_txn=leader_init_fence)`; raises EpochFenced."""
        ...


class SeqSource(Protocol):
    async def max_seq_id(self) -> int: ...


class HlcReseed(Protocol):
    def reseed(self, max_seq: int) -> None: ...


class LocalRedis(Protocol):
    """The NodeController's STANDBY-entry flush (src/runtime/node.py)."""

    async def flushdb(self) -> None:
        """FLUSHDB of the engine's Redis database (never FLUSHALL: other databases are not the engine's)."""
        ...


class EngineRedisFlush(Protocol):
    """LEADER_INIT step 3."""

    async def flushdb(self) -> None:
        """FLUSHDB of the engine's Redis database (removes redis:boot_id); never FLUSHALL."""
        ...


class RulesetInit(Protocol):
    async def load_ruleset(self, now_ms: int) -> int | None:
        """Install the active ruleset; returns its version (None: the base config)."""
        ...


class SodCatchUp(Protocol):
    async def catch_up(self, now_ms: int) -> SodOutcome: ...


class CalendarInit(Protocol):
    async def init_calendar(self, now_ms: int) -> int:
        """Load the latest schedule and initialise SM10; returns the calendar version."""
        ...


class IngestionStarter(Protocol):
    async def start_adapters(self) -> None:
        """Start SM4 for every adapter (spawns the supervised adapter tasks and returns)."""
        ...


class Sm8Bootstrap(Protocol):
    async def bootstrap(self) -> None:
        """SM8 all steps; returns once redis:boot_id has been written (last)."""
        ...


class RiskSweepStarter(Protocol):
    async def start_risksweep(self) -> None: ...


class DlqBarrier(Protocol):
    async def startup_barrier(self) -> None: ...


class TermRecorder(Protocol):
    async def mark_reached_leader(self, epoch: Epoch, now_ms: int) -> None: ...


class FlagRaiser(Protocol):
    def flag(self, code: str, detail: str) -> None: ...


@dataclass(frozen=True)
class LeaderInitDeps:
    fencer: EpochFencer
    seq: SeqSource
    hlc: HlcReseed
    redis: EngineRedisFlush
    sod: SodCatchUp
    calendar: CalendarInit
    ingestion: IngestionStarter
    sm8: Sm8Bootstrap
    risksweep: RiskSweepStarter
    dlq: DlqBarrier
    terms: TermRecorder
    flags: FlagRaiser
    rules: RulesetInit | None = None


class InitOutcome(str, Enum):
    LEADER = "LEADER"
    SAFE_HOLD = "SAFE_HOLD"


@dataclass
class InitReport:
    outcome: InitOutcome
    steps: list[str] = field(default_factory=list)
    calendar_version: int | None = None
    ruleset_version: int | None = None
    sod: SodOutcome | None = None
    fence: FenceOutcome | None = None


class LeaderInitFailed(RuntimeError):
    pass


def is_fence_loss(exc: BaseException) -> bool:
    """An `EpochMismatch` (src.db.fence, or the Oversee / sweep variant) anywhere in the chain."""
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if any(c.__name__ == "EpochMismatch" for c in type(cur).__mro__):
            return True
        if isinstance(cur, BaseExceptionGroup) and any(is_fence_loss(e) for e in cur.exceptions):
            return True      # step 6 runs in a TaskGroup
        cur = cur.__cause__ or cur.__context__
    return False


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


class LeaderInit:
    def __init__(self, deps: LeaderInitDeps, *, exit_fn: ExitFn = hard_exit, deadline_s: float = 120.0,
                 clock_ms: Callable[[], int] = _wall_ms) -> None:
        self._d = deps
        self._exit = exit_fn
        self._deadline_s = deadline_s
        self._clock_ms = clock_ms

    async def run(self, epoch: Epoch, node_id: str) -> InitReport | None:
        """Run LEADER_INIT. Returns the report (LEADER or SAFE_HOLD); on any failure calls the exit
        function with `init_failed` and returns None (production never returns)."""
        report = InitReport(InitOutcome.LEADER)
        try:
            await asyncio.wait_for(self._steps(epoch, node_id, report), self._deadline_s)
        except TimeoutError:
            log.critical("LEADER_INIT passed its %.0f s deadline after %s", self._deadline_s, report.steps)
            self._exit("init_failed: deadline")
            return None
        except Exception as exc:
            if is_fence_loss(exc):
                log.critical("LEADER_INIT lost the fence after %s: %r", report.steps, exc)
                self._exit(f"fence: {type(exc).__name__}: {exc}")
                return None
            log.critical("LEADER_INIT failed after %s: %r", report.steps, exc)
            self._exit(f"init_failed: {type(exc).__name__}: {exc}")
            return None
        return report

    async def _steps(self, epoch: Epoch, node_id: str, report: InitReport) -> None:
        d = self._d
        fence = await d.fencer.bump_and_fence(epoch, node_id, self._clock_ms())
        report.fence = fence
        report.steps.append("epoch_bump")
        if fence.safe_hold:
            report.outcome = InitOutcome.SAFE_HOLD
            log.critical("SAFE_HOLD due at LEADER_INIT: %s; term aborted", fence.safe_hold_reason)
            return
        d.hlc.reseed(await d.seq.max_seq_id())
        report.steps.append("hlc_reseed")
        await d.redis.flushdb()
        report.steps.append("flushdb")
        if d.rules is not None:
            report.ruleset_version = await d.rules.load_ruleset(self._clock_ms())
            report.steps.append("ruleset")
        sod = await d.sod.catch_up(self._clock_ms())
        report.sod = sod
        if sod.partial_session:
            d.flags.flag("partial_session", "sod catch-up wrote a partial_session row; MaxDailyLossRule capped at SOFT")
        report.steps.append("sod_catch_up")
        report.calendar_version = await d.calendar.init_calendar(self._clock_ms())
        report.steps.append("calendar")
        async with asyncio.TaskGroup() as tg:      # step 6: SM4 in parallel with SM8; sweep at once
            tg.create_task(self._start_sm4(report))
            tg.create_task(self._run_sm8(report))
            tg.create_task(self._start_sweep(report))
        await d.dlq.startup_barrier()
        report.steps.append("dlq_barrier")
        await d.terms.mark_reached_leader(epoch, self._clock_ms())
        report.steps.append("reached_leader")

    async def _start_sm4(self, report: InitReport) -> None:
        await self._d.ingestion.start_adapters()
        report.steps.append("sm4_started")

    async def _run_sm8(self, report: InitReport) -> None:
        await self._d.sm8.bootstrap()
        report.steps.append("sm8_ready")

    async def _start_sweep(self, report: InitReport) -> None:
        await self._d.risksweep.start_risksweep()
        report.steps.append("risksweep_started")
