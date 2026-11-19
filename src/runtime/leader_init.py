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

Any failure other than a due SAFE_HOLD is a hard exit (`init_failed`). A stop (K15: SIGTERM, `stop` set) is not a
failure: taken before step 1 it skips the epoch bump (outcome STOPPED, no term); taken after it, the step in flight is
cancelled (steps 2-7 and LEADER entry; step 1 itself is never cancelled, its commit must be known) and the outcome
is STOPPED with `epoch_bump` in the steps, so the caller ends the term through DRAINING (`switchover`). The wait for
the cancelled step to unwind is capped at `ABORT_CANCEL_S` and at the time left before the 120 s deadline, so a stop
just before the deadline still ends STOPPED, never `init_failed: deadline`. A step must never swallow
`CancelledError`; as a guard, once a stop has abandoned the steps every later step boundary raises `CancelledError`, so
a step that did swallow it can never go on to start SM4, the RiskSweep or mark the term LEADER. A lost fence (`EpochMismatch`,
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
from typing import Final, Protocol

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
    STOPPED = "STOPPED"      # K15: a stop request ended LEADER_INIT; `term_started` tells whether step 1 committed


ABORT_CANCEL_S: Final = 1.0
"""K15: how long a stop waits for the cancelled LEADER_INIT step to unwind before the term is ended anyway."""

DEADLINE_MARGIN_S: Final = 0.05
"""K15: the unwind wait ends at least this long before the LEADER_INIT deadline, so the outer `wait_for` never fires
on a stop that is already being handled."""


@dataclass
class InitReport:
    outcome: InitOutcome
    steps: list[str] = field(default_factory=list)
    calendar_version: int | None = None
    ruleset_version: int | None = None
    sod: SodOutcome | None = None
    fence: FenceOutcome | None = None

    @property
    def term_started(self) -> bool:
        """Step 1 committed: this node's leader_terms row exists (it must be ended with an end reason)."""
        return "epoch_bump" in self.steps


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
                 clock_ms: Callable[[], int] = _wall_ms, stop: asyncio.Event | None = None,
                 abort_cancel_s: float = ABORT_CANCEL_S) -> None:
        self._d = deps
        self._exit = exit_fn
        self._deadline_s = deadline_s
        self._clock_ms = clock_ms
        self._stop = stop
        self._abort_cancel_s = abort_cancel_s
        self._started_at = 0.0
        self._abandoned = False     # K15: a stop ended LEADER_INIT; the steps coroutine must not go on


    async def run(self, epoch: Epoch, node_id: str) -> InitReport | None:
        """Run LEADER_INIT. Returns the report (LEADER, SAFE_HOLD or STOPPED); on any failure calls the exit
        function with `init_failed` and returns None (production never returns)."""
        report = InitReport(InitOutcome.LEADER)
        self._started_at = asyncio.get_running_loop().time()
        self._abandoned = False
        try:
            await asyncio.wait_for(self._run(epoch, node_id, report), self._deadline_s)
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

    def _stop_requested(self) -> bool:
        return self._stop is not None and self._stop.is_set()

    def _stopped(self, report: InitReport, where: str) -> None:
        report.outcome = InitOutcome.STOPPED
        log.warning("LEADER_INIT stopped %s (after %s)%s", where, report.steps,
                    "; the term is ended through DRAINING" if report.term_started else "; no term was started")

    async def _run(self, epoch: Epoch, node_id: str, report: InitReport) -> None:
        if self._stop_requested():                  # K15: before the epoch bump: no term at all
            self._stopped(report, "before the epoch bump")
            return
        await self._step1(epoch, node_id, report)   # never cancelled by a stop: its commit must be known
        if report.outcome is InitOutcome.SAFE_HOLD:
            return
        if self._stop_requested():
            self._stopped(report, "at a step boundary")
            return
        stop = self._stop
        if stop is None:
            await self._steps(epoch, report)
            return
        rest = asyncio.ensure_future(self._steps(epoch, report))
        waiter = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({rest, waiter}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:                       # the deadline / an outer cancel: the steps go with it
            rest.cancel()
            waiter.cancel()
            raise
        waiter.cancel()
        if rest.done():
            rest.result()                           # finished by itself: LEADER, or its failure as before
            return
        # K15: a stop during steps 2-7 / LEADER entry. The step in flight is cancelled at its next await (the term
        # ends now, so nothing it would finish is needed; its writes are fenced transactions and the engine Redis is
        # flushed at STANDBY entry), bounded by `abort_cancel_s`, never by the step's own length (SM8: up to 100 s),
        # and by the time left before the LEADER_INIT deadline: a stop just before it still ends STOPPED (the outer
        # `wait_for` firing during this wait would turn it into `init_failed: deadline`).
        self._abandoned = True
        rest.cancel()
        left = self._deadline_s - (asyncio.get_running_loop().time() - self._started_at) - DEADLINE_MARGIN_S
        wait_s = min(self._abort_cancel_s, left)
        done: set[asyncio.Future[None]] = set()
        if wait_s > 0:
            done, _ = await asyncio.wait({rest}, timeout=wait_s)
        if not done:
            log.error("LEADER_INIT step did not unwind within %.2f s of the stop; ending the term anyway",
                      max(wait_s, 0.0))
            rest.add_done_callback(_retrieve)
        elif not rest.cancelled():
            exc = rest.exception()
            if exc is not None and is_fence_loss(exc):
                raise exc                           # a successor took over: the hard exit `fence`, as before
            if exc is not None:
                log.warning("LEADER_INIT step ended with %r while stopping", exc)
        self._stopped(report, "during a step")

    async def _step1(self, epoch: Epoch, node_id: str, report: InitReport) -> None:
        fence = await self._d.fencer.bump_and_fence(epoch, node_id, self._clock_ms())
        report.fence = fence
        report.steps.append("epoch_bump")
        if fence.safe_hold:
            report.outcome = InitOutcome.SAFE_HOLD
            log.critical("SAFE_HOLD due at LEADER_INIT: %s; term aborted", fence.safe_hold_reason)

    def _go_on(self) -> None:
        """K15 guard: no LEADER_INIT step (dependencies must re-raise `CancelledError`, never swallow it) may start
        once a stop has abandoned the steps, so a step that did swallow the cancel cannot go on to start SM4 or the
        RiskSweep, or mark the term LEADER, after the term ended."""
        if self._abandoned:
            raise asyncio.CancelledError("LEADER_INIT stopped")

    async def _steps(self, epoch: Epoch, report: InitReport) -> None:
        """Steps 2-7 and LEADER entry (after step 1 committed). Each step boundary checks `_go_on()`."""
        d = self._d
        self._go_on()
        d.hlc.reseed(await d.seq.max_seq_id())
        report.steps.append("hlc_reseed")
        self._go_on()
        await d.redis.flushdb()
        report.steps.append("flushdb")
        if d.rules is not None:
            self._go_on()
            report.ruleset_version = await d.rules.load_ruleset(self._clock_ms())
            report.steps.append("ruleset")
        self._go_on()
        sod = await d.sod.catch_up(self._clock_ms())
        report.sod = sod
        if sod.partial_session:
            d.flags.flag("partial_session", "sod catch-up wrote a partial_session row; MaxDailyLossRule capped at SOFT")
        report.steps.append("sod_catch_up")
        self._go_on()
        report.calendar_version = await d.calendar.init_calendar(self._clock_ms())
        report.steps.append("calendar")
        self._go_on()
        async with asyncio.TaskGroup() as tg:      # step 6: SM4 in parallel with SM8; sweep at once
            tg.create_task(self._start_sm4(report))
            tg.create_task(self._run_sm8(report))
            tg.create_task(self._start_sweep(report))
        self._go_on()
        await d.dlq.startup_barrier()
        report.steps.append("dlq_barrier")
        self._go_on()
        await d.terms.mark_reached_leader(epoch, self._clock_ms())
        report.steps.append("reached_leader")

    async def _start_sm4(self, report: InitReport) -> None:
        self._go_on()
        await self._d.ingestion.start_adapters()
        report.steps.append("sm4_started")

    async def _run_sm8(self, report: InitReport) -> None:
        self._go_on()
        await self._d.sm8.bootstrap()
        report.steps.append("sm8_ready")

    async def _start_sweep(self, report: InitReport) -> None:
        self._go_on()
        await self._d.risksweep.start_risksweep()
        report.steps.append("risksweep_started")


def _retrieve(fut: asyncio.Future[None]) -> None:
    if not fut.cancelled():
        fut.exception()
