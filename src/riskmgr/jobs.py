"""The per-strategy-version job runner (design §B4 Job runner, §B14 STRATEGY_VERSION; plan S22, R11-M17).

Each position's pinned guards, `propose` and `at_target` run as **one job** (a callable handed in by the BarClock, S27)
in a single-thread executor per pinned strategy `(KIND, IMPL_VERSION)`: its own
`ThreadPoolExecutor(max_workers=1)`, entered through `loop.run_in_executor` and awaited under a 250 ms cap, never
`asyncio.to_thread` (whose shared default executor one hung job could starve). The job runs under the version's pinned
Decimal context (the strategy class's `DECIMAL_CONTEXT`), set inside the worker thread, where contexts live.

* A timeout or a raise is a `STRATEGY_ERROR` result (`job_timeout`, `job_raised`); the exception text is never kept
  (it could hold a value). Only the cap expiring is `job_timeout`: a job that raises `TimeoutError` itself is
  `job_raised` and attributed like any raise. Any `BaseException` the job raises (`SystemExit` and
  `KeyboardInterrupt` included) is `job_raised`, so a job can never end the `arm` loop; only the caller's own
  cancellation propagates.
* A fault raised inside a guard is attributed to that guard's pinned `(KIND, VERSION)`: an exception carrying a
  `scope_id` (`ScopedFault`, e.g. the pipeline's `PipelineFault`) that names one of the job's guard scopes
  (`guard:<KIND>:<VERSION>`) counts against that guard, so a bad guard version never fails an unrelated strategy
  version. Anything else (a timeout included) counts against the strategy scope.
* Each scope has its own circuit: 3 consecutive faults across the positions pinning it raise one
  `VersionFault` signal (the STRATEGY_VERSION scope, S30 fails only those positions); a completed job resets the
  counters of the scopes it ran. The signal is given once per episode, until `reset(scope)` after the clear.
* Jobs of one version run one after another (an `asyncio.Lock` per version), so the cap measures the job, not its
  queue. **Hung lanes are retired** (round 12, plan S38c): a job still running past its cap leaves its thread behind;
  the lane is shut down without waiting (`shutdown(wait=False)`) and kept as an abandoned thread, and the next job of
  that version gets a fresh executor, so a hung strategy blocks neither the `arm` loop, another version, nor its own
  version's clear probe (`strategy_conformance` runs on a fresh lane). At most `max_abandoned` (3, the version fault
  limit, so one hanging version fails only its STRATEGY_VERSION scope) abandoned threads may be alive at once; a
  hang past that bound is not retired (the lane answers `executor_hung` at once, no new thread) and raises the
  ARM-global `restart_required` signal once (`take_restart_required`), because only a restart frees the threads.
  The signal is armed again once the abandoned count drops back under the bound, and the ARM-global clear probe
  refuses `threads_hung` while more threads are hung than the bound (`restart_still_required`, S38c review).
* **PC sleep (go-live gate `uptime_suspend`, plan S39b):** a job whose run spans a suspend of the process is never
  counted. The runner learns of a suspend two ways: the uptime tracker's signal (`note_suspended`, which the runtime
  calls from `EngineSession.add_suspend_listener`, and the BarClock calls when its own clock detector sees one) and
  its own reading, the job's elapsed monotonic time past the cap by more than `suspend_after_ms` (on Windows
  `time.monotonic()` keeps counting through a sleep, so the cap's timer fires the moment the PC wakes). A job whose
  cap expired across a suspend gets a fresh cap measured from the resume (awake time only, at most
  `MAX_SUSPEND_REWAITS` times); one that then finishes is a normal result, and one that raised is `job_suspended`
  (never a fault: the circuit is untouched, the caller treats the bar as missed, `DOWNTIME`). A job still running a
  full awake cap after the resume is a genuine `job_timeout` and counts.
* **CPU spin (accepted limit):** a strategy stuck in a pure-Python loop keeps its share of the GIL (the interpreter
  switches threads every 5 ms), so it slows, but never stops, the event loop that runs ingestion and Oversee; the
  conformance test bounds the event-loop lag with one spinning job under the 100 ms C3 bound, and the bound above
  limits how many such threads can exist.

Python cannot kill a hung thread; `close()` shuts the executors down without waiting and returns whether a job thread
(of a live or a retired lane) is still hung. **Shutdown relies on the hard exit:** a `ThreadPoolExecutor` worker is non-daemon and joined at
interpreter exit (the `concurrent.futures` atexit hook), so a hung strategy thread would keep a DRAINING or
SIGTERM-stopped node from exiting. When `close()` returns True the process must end through the engine's hard exit
(`src.runtime.exits.hard_exit`, `os._exit`, which runs no atexit hook), as the fence and the DRAINING deadline already
do; S27 / S31 wire that into the stop path. (A daemon worker would avoid this, but `threading` stays off the
lockout allow-list, `src/riskmgr/lockout.py`.) Nothing here logs.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Context, localcontext
from typing import Final, Generic, Protocol, TypeVar, runtime_checkable

from src.riskmgr.outcomes import Outcome, OutcomeCode
from src.riskmgr.types import ScopeKind

JOB_CAP_MS: Final = 250
VERSION_FAULT_LIMIT: Final = 3
MAX_ABANDONED_THREADS: Final = 3
"""Hung job threads that may be alive at once after their lanes were retired (S38c); past it: `restart_required`."""
JOB_TIMEOUT: Final = "job_timeout"
JOB_RAISED: Final = "job_raised"
EXECUTOR_HUNG: Final = "executor_hung"
"""`STRATEGY_ERROR` reasons (codes only)."""
JOB_SUSPENDED: Final = "job_suspended"
"""A job whose run spanned a suspend and did not finish cleanly: never a fault (plan S39b, gate `uptime_suspend`)."""
MAX_SUSPEND_REWAITS: Final = 3
"""Fresh caps a job may get after suspends during its run (each measured from a resume)."""

T = TypeVar("T")


@runtime_checkable
class ScopedFault(Protocol):
    """An exception that names the pinned scope it belongs to (`strategy:<KIND>:<VERSION>` or
    `guard:<KIND>:<VERSION>`)."""

    scope_id: str


@dataclass(frozen=True)
class JobSpec:
    """The pinned versions one job touches: the strategy scope (its executor), the guard scopes in pin order and
    the strategy's pinned Decimal context."""

    strategy_scope: str
    guard_scopes: tuple[str, ...] = ()
    decimal_context: Context | None = None

    def scopes(self) -> tuple[str, ...]:
        return (self.strategy_scope, *self.guard_scopes)


@dataclass(frozen=True)
class JobResult(Generic[T]):
    """A finished job: its value, or the fault (`reason`) and the scope it is attributed to."""

    value: T | None = None
    reason: str = ""
    fault_scope: str = ""

    @property
    def ok(self) -> bool:
        return not self.reason

    @property
    def suspended(self) -> bool:
        """The job's run spanned a suspend and it did not finish cleanly: not a fault (nothing was counted)."""
        return self.reason == JOB_SUSPENDED

    @property
    def code(self) -> OutcomeCode | None:
        """`STRATEGY_ERROR(<reason>)` for a fault, else None."""
        return None if self.ok else OutcomeCode.engine(Outcome.STRATEGY_ERROR, self.reason)


@dataclass(frozen=True)
class VersionFault:
    """The STRATEGY_VERSION fault signal for S30: `limit` consecutive faults across the positions pinning `scope_id`."""

    scope_id: str
    faults: int
    position_ids: tuple[str, ...]
    scope_kind: ScopeKind = ScopeKind.STRATEGY_VERSION


class _Lane:
    """One version's executor, its lock and whether its thread is still inside a job."""

    __slots__ = ("executor", "lock", "busy")

    def __init__(self, scope: str) -> None:
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"arm-job-{scope}")
        self.lock = asyncio.Lock()
        self.busy = False


class _Ticket:
    """One job's hand-off: `abandoned` once its cap expired (read by the worker before it starts the job)."""

    __slots__ = ("abandoned",)

    def __init__(self) -> None:
        self.abandoned = False


class _Abandoned(Exception):
    """A job whose cap expired before its thread started it (its result is never read)."""


class VersionCircuit:
    """Consecutive faults per scope; one signal per episode."""

    def __init__(self, *, limit: int = VERSION_FAULT_LIMIT) -> None:
        if limit < 1:
            raise ValueError("the fault limit is at least 1")
        self._limit = limit
        self._streak: dict[str, list[str]] = {}
        self._tripped: set[str] = set()

    def ok(self, scope: str) -> None:
        self._streak.pop(scope, None)

    def fault(self, scope: str, position_id: str) -> VersionFault | None:
        streak = self._streak.setdefault(scope, [])
        streak.append(position_id)
        if len(streak) >= self._limit and scope not in self._tripped:
            self._tripped.add(scope)
            return VersionFault(scope, len(streak), tuple(dict.fromkeys(streak)))
        return None

    def tripped(self, scope: str) -> bool:
        return scope in self._tripped

    def streak(self, scope: str) -> int:
        return len(self._streak.get(scope, ()))

    def reset(self, scope: str) -> None:
        """After the operator clear of the scope (S30)."""
        self._tripped.discard(scope)
        self._streak.pop(scope, None)


class JobRunner:
    def __init__(self, *, cap_ms: int = JOB_CAP_MS, fault_limit: int = VERSION_FAULT_LIMIT,
                 max_abandoned: int = MAX_ABANDONED_THREADS, suspend_after_ms: int | None = None,
                 monotonic: Callable[[], float] | None = None) -> None:
        """`suspend_after_ms`: a job whose cap expired more than this late (monotonic) ran across a suspend (the
        runtime passes `clockstep.SUSPEND_AFTER_MS`); None = only the `note_suspended` signal counts. `monotonic`:
        seconds (tests); default `time.monotonic`."""
        if cap_ms <= 0:
            raise ValueError("the job cap is positive")
        if max_abandoned < 0:
            raise ValueError("the abandoned-thread bound is not negative")
        if suspend_after_ms is not None and suspend_after_ms <= 0:
            raise ValueError("the suspend threshold is positive")
        self._cap_s = cap_ms / 1000
        self._suspend_after_s = None if suspend_after_ms is None else suspend_after_ms / 1000
        self._monotonic = monotonic or time.monotonic
        self._suspends = 0
        self.suspended_jobs = 0
        """Jobs whose run spanned a suspend (finished or not; none of them counted as a fault)."""
        self._lanes: dict[str, _Lane] = {}
        self._retired: list[_Lane] = []
        self._max_abandoned = max_abandoned
        self._restart_signalled = False
        self._restart_due: int | None = None
        self.circuit = VersionCircuit(limit=fault_limit)
        self._signals: list[VersionFault] = []

    def abandoned_threads(self) -> int:
        """Retired lanes whose job thread is still running (hung); finished ones are dropped. Once the count is back
        under the bound, a later hang past it is signalled again (`take_restart_required`, S38c review)."""
        self._retired = [lane for lane in self._retired if lane.busy]
        if len(self._retired) < self._max_abandoned:
            self._restart_signalled = False
        return len(self._retired)

    def hung_threads(self) -> int:
        """Job threads inside a job now: the abandoned ones plus every current lane that is busy (a lane kept past
        the bound answers `executor_hung` while its thread is still hung)."""
        return self.abandoned_threads() + sum(1 for lane in self._lanes.values() if lane.busy)

    def restart_still_required(self) -> bool:
        """`HungThreadsView` of the ARM-global clear probe (S38c review): more job threads are hung than the runner
        may abandon, so a `restart_required` episode must not clear until a restart (or the threads ending) frees
        them."""
        return self.hung_threads() > self._max_abandoned

    def _retire(self, scope: str, lane: _Lane) -> None:
        """The cap expired with the job thread still inside the job: give the version a fresh lane and keep the old
        one as an abandoned thread, while the bound allows; past it keep the lane (`executor_hung`, no new thread)
        and signal `restart_required` once."""
        if self.abandoned_threads() >= self._max_abandoned:
            if not self._restart_signalled:
                self._restart_signalled = True
                self._restart_due = self.abandoned_threads() + 1
            return
        lane.executor.shutdown(wait=False, cancel_futures=True)
        self._retired.append(lane)
        if self._lanes.get(scope) is lane:
            del self._lanes[scope]

    def take_restart_required(self) -> int | None:
        """The hung-thread count when `restart_required` became due since the last call (once per process), else
        None (the ARM-global fault, `ErrorBudget.restart_required`)."""
        due, self._restart_due = self._restart_due, None
        return due

    def _lane(self, scope: str) -> _Lane:
        lane = self._lanes.get(scope)
        if lane is None:
            lane = _Lane(scope)
            self._lanes[scope] = lane
        return lane

    def note_suspended(self, _suspended_ms: int | None = None) -> None:
        """The process was suspended (the uptime tracker's signal, or the BarClock's own detector): every job running
        now spanned it. Synchronous, no I/O (an `EngineSession.add_suspend_listener` listener)."""
        self._suspends += 1

    @property
    def suspends(self) -> int:
        return self._suspends

    def _spanned(self, mark: int, started_s: float) -> bool:
        """A suspend was signalled since `mark`, or the wait outlived the cap by more than the suspend threshold."""
        if self._suspends != mark:
            return True
        limit = self._suspend_after_s
        return limit is not None and self._monotonic() - started_s > self._cap_s + limit

    def hung(self, strategy_scope: str) -> bool:
        """The version's current lane is inside a job (its retired lanes do not block it)."""
        lane = self._lanes.get(strategy_scope)
        return lane is not None and lane.busy

    async def run(self, spec: JobSpec, position_id: str, job: Callable[[], T]) -> JobResult[T]:
        """Run one position's job on its strategy version's executor under the cap (a fresh awake cap after a
        suspend, module docstring)."""
        lane = self._lane(spec.strategy_scope)
        async with lane.lock:
            if lane.busy:
                return self._fault(spec.strategy_scope, position_id, EXECUTOR_HUNG)
            context = spec.decimal_context
            ticket = _Ticket()

            def work() -> T:
                # busy first, then the ticket (S38c review): the loop sets `abandoned` before it reads `busy`, so
                # either this thread sees the expired cap and never runs the job, or the loop sees the lane busy and
                # retires it; a job can never start after its cap on a lane that was not retired
                lane.busy = True
                if ticket.abandoned:      # its cap expired before the thread reached it: never run late
                    lane.busy = False
                    raise _Abandoned
                try:
                    if context is None:
                        return job()
                    with localcontext(context):
                        return job()
                finally:
                    lane.busy = False

            loop = asyncio.get_running_loop()
            mark, started = self._suspends, self._monotonic()
            future = loop.run_in_executor(lane.executor, work)
            done = asyncio.Event()
            future.add_done_callback(lambda _f: done.set())
            spanned = False
            try:
                finished = await self._wait(done)
                rewaits = 0
                # a cap that expired across a suspend measured sleep, not the job: give it a fresh awake cap
                while not finished and rewaits < MAX_SUSPEND_REWAITS and self._spanned(mark, started):
                    spanned = True
                    rewaits += 1
                    mark, started = self._suspends, self._monotonic()
                    finished = await self._wait(done)
            except asyncio.CancelledError:   # the caller's own cancellation: the job is abandoned, the cancel goes on
                ticket.abandoned = True
                future.cancel()
                raise
            spanned = spanned or self._spanned(mark, started)
            if spanned:
                self.suspended_jobs += 1
            if not finished:                 # the cap expired (awake time): a timeout, never a raised TimeoutError
                ticket.abandoned = True
                future.cancel()
                if lane.busy:                # its thread is still inside the job: retire the lane (S38c)
                    self._retire(spec.strategy_scope, lane)
                return self._fault(spec.strategy_scope, position_id, JOB_TIMEOUT)
            try:
                value = future.result()
            except BaseException as exc:     # SystemExit / KeyboardInterrupt from the job too: never past this point
                if spanned:                  # it ran across a sleep: not a fault of the version (gate uptime_suspend)
                    return JobResult(reason=JOB_SUSPENDED)
                scope = exc.scope_id if isinstance(exc, ScopedFault) else ""   # attributed, the text is never kept
                return self._fault(scope if scope in spec.scopes() else spec.strategy_scope, position_id, JOB_RAISED)
            for scope in spec.scopes():
                self.circuit.ok(scope)
            return JobResult(value)

    async def _wait(self, done: asyncio.Event) -> bool:
        """Wait up to one cap for the job (the future itself is never cancelled here); True when it finished."""
        try:
            await asyncio.wait_for(done.wait(), self._cap_s)
        except TimeoutError:
            return done.is_set()
        return True

    def _fault(self, scope: str, position_id: str, reason: str) -> JobResult[T]:
        signal = self.circuit.fault(scope, position_id)
        if signal is not None:
            self._signals.append(signal)
        return JobResult(reason=reason, fault_scope=scope)

    def take_signals(self) -> list[VersionFault]:
        """The STRATEGY_VERSION fault signals raised since the last call (each once)."""
        out, self._signals = self._signals, []
        return out

    def reset(self, scope: str) -> None:
        self.circuit.reset(scope)

    def close(self) -> bool:
        """Shut every executor down without waiting (a hung thread cannot be killed; queued jobs are cancelled).
        Returns True when a job thread is still hung: the process must then end through the hard exit, since the
        interpreter joins executor threads at exit."""
        hung = any(lane.busy for lane in self._lanes.values()) or self.abandoned_threads() > 0
        for lane in self._lanes.values():
            lane.executor.shutdown(wait=False, cancel_futures=True)
        self._lanes.clear()
        return hung
