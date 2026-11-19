"""`md-indicators`: the indicator job runner (design §A6 CPU budget, fault containment, write path; §A8; plan S18,
R11-M03, R11-m).

A supervised, non-critical task (stamp 10 s) added by `optional-bootstrap` (S31) only when `XTNL_MD_ENABLED=1`. It
reads bars through the read port (decision reads: SM12 LIVE, or BACKFILLING with the range complete), computes in a
worker thread under the indicator's pinned Decimal context, and writes `redis-md` through the `IND` script (writer
token and exact `md:boot`, like every write). It never touches ingestion, Oversee or PostgreSQL.

**Jobs.** One recompute queue, coalesced per indicator (a newer request widens the pending one), run in the
topological order of the registered specs (`registry.plan_specs`), so an input is always computed before its
dependants. A job is queued by:

* a released bar (`on_released`, the `MdWriter` listener): the newest value; a bar older than the indicator's newest
  value (a revision, or a resolved blind bucket arriving late) recomputes every held value from that bucket on;
* `on_resolved` (`md-bars` `bucket_resolved`, R11-M03): a blind BROKER bucket left the lane, so dependants are
  recomputed at once and become `warm` again without waiting `3 × length` new bars;
* `register()` (demand) and the `md_rebuild` work item: a re-seed from stored bars.

**Canonical on every update.** For an indicator without an exact sliding form (ATR) every value is the canonical value
of its window (`seed` over exactly the window's bars), so the same bars always give the same value; replay, `as_of`
reads and the incremental run agree by construction. Values are quantised per output only for storage.

**Event order** (R11-m): the bar is written first, with `bar_closed` from the same flush (`md-writer`); this task
then writes the value and the `IND` script adds `indicator_updated` after it; the in-process `on_updated` listeners
run after the write's reply. If the write fails the value is simply not there (not `warm`) and the job stays queued.

**Fault containment.** Every job ends in a result value, never an exception that reaches the task. An indicator whose
`seed` / `step` / `outputs` raises, whose value is not finite or below its class's bound, one of whose targets uses
more than 250 ms of thread CPU, or whose checkpoint does not decode is re-seeded from stored bars and retried; 3
faults in a row open its own circuit: `FAULTED` (never `warm`, the reason in its lineage; a fault marker is written for
each output, on its newest value's bucket or else the newest closed bucket), its jobs are dropped until `md_rebuild`
re-seeds it, and `on_fault(indicator_id, reason)` reports the §B14 series fault. FAULTED outlives the indicator's
leases (a tombstone kept by `remove`), and `register` never runs a job for an indicator already tracked. A job's batch
is sized to the CPU budget: it stops once its thread CPU passes half the cap and the rest is the next job, so a long
window over many targets is never a fault. Each indicator runs at most one thread at a time; a job still running after
`job_timeout_s` is FAULTED at once (`job_hung`: Python cannot kill a thread, so the hang is confined to that
indicator). Worker calls go to **this task's own bounded executor** (`ThreadPoolExecutor(max_workers=max_threads)`,
round 13, design §B4), never the loop's shared default executor that ingestion, the node controller and the DLQ spill
use, so a hung indicator thread can never starve core work; they are never cancelled (a call that had not started
yet still runs, then frees its indicator), at most `max_threads` are in flight, and at `max_hung_threads` hung threads
no job is dispatched (one page) until they return. `close()` shuts the executor down without waiting and says
whether a worker is still hung: the stop path then ends the process through the hard exit, as for a hung ARM job
thread (the executor's workers are joined at interpreter exit). A store that cannot be read or
written defers the job; it is never an indicator fault. `on_quarantine` (the supervisor's task-circuit hook) drops the
head job and marks its indicator FAULTED. The task stamps between the bounded steps of a job (each read, the thread,
each write), so no gap between stamps exceeds one step's timeout.

**Multi-input indicators** (an `IndicatorRef` input) are refused at registration (plan S38e,
`IndicatorSettings.indicator_inputs` False): a dependant's input would come from this process's memory, not from
`md:ind`, so it would not be canonical across a restart or a coalesced job. The DAG planner (`plan_specs`) stays,
for when the canonical read is built (design §A6, §0b).

**Warm** (§A6): the stored flag is the window rule (`warm_counts` evaluable bars, no `blind` bucket in the last
`warm_span` buckets: the `md-bars` lane, a quarantined bar); `is_warm` also needs the value's bucket to be the newest
closed bucket of the clock series, known from a released bar or the newest bar a job read (unknown: not warm). A
`Count` window reads further back past a long market closure until it holds its bars (bounded by the retention).

Nothing here logs, pages or formats a value or a price: indicator ids, series ids, buckets, counts and reason codes.
"""
from __future__ import annotations

import asyncio
import logging
import time
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, localcontext
from enum import Enum
from typing import Any, Final, Protocol, TypeVar

from src.marketdata.errors import safe_redis_error
from src.marketdata.indicators.base import (
    Anchored,
    Count,
    Indicator,
    IndicatorRef,
    IndicatorSpec,
    IndicatorUpdate,
    IndicatorValue,
    InputRef,
    Lineage,
    Point,
    SeriesRef,
    State,
    Window,
    encode_value,
    input_digest,
    quantise,
    quantum,
)
from src.marketdata.indicators.checkpoint import CHECKPOINT_RETENTION_MS, due, encode_checkpoint
from src.marketdata.indicators.registry import (
    REGISTRY,
    IndicatorRegistry,
    input_series,
    plan_specs,
    series_retention_ms,
)
from src.marketdata.ports import BarsRead, PagePort, StampPort
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.runner import ScriptOk, ScriptRefused, ScriptReply, StoreUnavailable
from src.marketdata.types import Bar, BarEndEvent, BarSpec, DataUnavailable, Quality, SeriesId, Source

log = logging.getLogger(__name__)

STAMP_BUDGET_S: Final = 10.0
CPU_CAP_MS: Final = 250
FAULT_LIMIT: Final = 3
JOB_TIMEOUT_S: Final = 5.0
IO_TIMEOUT_S: Final = 2.0
CYCLE_BUDGET_S: Final = 6.0
INTERVAL_S: Final = 0.25
RETRY_MS: Final = 1_000
BATCH: Final = 96
"""Values recomputed per job (a longer revision recompute continues in the next job)."""
KEEP: Final = 1_024
"""Exact values and value revisions kept in memory per indicator (dependants' inputs and recompute targets)."""
MAX_READ_BUCKETS: Final = 4_999
"""One port read stays under `port_impl.MAX_BARS_PER_READ`."""
MAX_THREADS: Final = 3
"""Worker calls in flight at once (running or hung): the size of `md-indicators`' own executor (round 13: never the
loop's shared default executor, which ingestion uses for adapter connects and the DLQ spill)."""
THREAD_PREFIX: Final = "md-ind"
MAX_HUNG_THREADS: Final = 2
"""Hung worker threads (still running after `job_timeout_s`) at which no new job is dispatched (one page)."""
SATURATED: Final = "indicators_saturated"
DAY_MS: Final = 86_400_000

APPLIED: Final = "applied"
REFUSED: Final = "refused"
SUPERSEDED_EXPIRED: Final = "superseded_expired"
NOT_REGISTERED: Final = "not_registered"
_QUALITY_RANK: Final = {Quality.COMPLETE: 0, Quality.PARTIAL: 1, Quality.DEGRADED: 2}
T = TypeVar("T")


# ---------------------------------------------------------------------------------------------------- ports
class BarsReadPort(Protocol):
    """The read port's bucket-addressed bar read (`MdPort.bars`)."""

    async def bars(self, series: SeriesId, *, at_bucket_ms: int | None = None, n: int | None = None,
                   from_bucket_ms: int | None = None, to_bucket_ms: int | None = None,
                   as_of_ms: int | None = None) -> BarsRead | DataUnavailable: ...


class IndWritePort(Protocol):
    """The writer connection (`MdWriteClient.run`)."""

    async def run(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]: ...


BlindSource = Callable[[SeriesId], frozenset[int]]
"""Known `blind` bucket starts of a series (the `md-bars` backfill lane, `MdBars.lane`)."""
FaultSink = Callable[[str, str], None]
Completion = Callable[[int, str, str], None]
ThreadRunner = Callable[[Callable[[], T]], Awaitable[T]]
Beat = Callable[[], None]
"""The task's liveness stamp (`StampPort.stamp`), called between bounded steps of a job."""


def _no_beat() -> None:
    return None


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def _mono_ms() -> int:
    return time.monotonic_ns() // 1_000_000


# ---------------------------------------------------------------------------------------------------- values
class IndState(str, Enum):
    PENDING = "pending"
    READY = "ready"
    FAULTED = "faulted"


@dataclass(frozen=True)
class IndicatorStatus:
    """What `md show` / `arm_status` report: states, buckets and counts only."""

    indicator_id: str
    state: IndState
    warm: bool
    last_bucket_ms: int | None
    fault_streak: int
    reason: str


@dataclass(frozen=True, kw_only=True)
class IndicatorSettings:
    cpu_cap_ms: int = CPU_CAP_MS
    fault_limit: int = FAULT_LIMIT
    job_timeout_s: float = JOB_TIMEOUT_S
    io_timeout_s: float = IO_TIMEOUT_S
    cycle_budget_s: float = CYCLE_BUDGET_S
    interval_s: float = INTERVAL_S
    retry_ms: int = RETRY_MS
    batch: int = BATCH
    keep: int = KEEP
    max_threads: int = MAX_THREADS
    max_hung_threads: int = MAX_HUNG_THREADS
    indicator_inputs: bool = False
    """Accept indicators whose inputs include another indicator (`IndicatorRef`, a DAG). Off (plan S38e): such an
    input is read from this process's memory (at most `keep` values, only those computed in this term), so after a
    restart or a coalesced job its window can have unmarked holes; until DAG inputs are read canonically from
    `md:ind` for the declared window, multi-input indicators are refused at registration (`indicator_inputs`)."""

    def __post_init__(self) -> None:
        if min(self.cpu_cap_ms, self.fault_limit, self.batch, self.keep, self.max_threads,
               self.max_hung_threads) < 1 or self.retry_ms < 0:
            raise ValueError("md-indicators settings must be positive")
        if not 0 < self.job_timeout_s < STAMP_BUDGET_S or not 0 < self.cycle_budget_s < STAMP_BUDGET_S:
            raise ValueError("md-indicators budgets must stay inside the 10 s stamp budget")
        if not 0 < self.io_timeout_s or self.job_timeout_s + self.io_timeout_s >= STAMP_BUDGET_S:
            raise ValueError("one job step (a thread or a store call) must stay inside the 10 s stamp budget")


@dataclass
class IndicatorCounters:
    cycles: int = 0
    jobs: int = 0
    values_written: int = 0
    values_unchanged: int = 0
    deferred: int = 0               # the read port or the store was not ready: retried, never a fault
    write_errors: int = 0
    faults: int = 0
    faulted: int = 0
    dropped: int = 0                # jobs dropped for a FAULTED indicator
    hung: int = 0
    cpu_capped: int = 0
    listener_errors: int = 0
    resolved: int = 0
    rebuilds: int = 0
    failed_cycles: int = 0          # cycles while the ARM is globally FAILED (only md_rebuild repairs run)


class IndicatorFault(Exception):
    """A per-indicator fault (a code, never a value)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Computed:
    """One computed bucket (exact outputs; hidden from `repr`)."""

    bucket_start_ms: int
    outputs: Mapping[str, Decimal] = field(repr=False)
    state: State = field(repr=False)
    warm: bool = False
    window_start_ms: int | None = None
    digest: str = "0" * 16
    final_at_ms: int | None = None
    quality: Quality = Quality.COMPLETE


@dataclass
class _Job:
    from_bucket_ms: int | None = None       # None: the newest value only
    seed: bool = False
    request_ids: list[int] = field(default_factory=list)

    def merge(self, other: _Job) -> None:
        if other.from_bucket_ms is not None:
            self.from_bucket_ms = (other.from_bucket_ms if self.from_bucket_ms is None
                                   else min(self.from_bucket_ms, other.from_bucket_ms))
        self.seed = self.seed or other.seed
        self.request_ids.extend(other.request_ids)


@dataclass
class _Ind:
    spec: IndicatorSpec
    ind: Indicator
    state: IndState = IndState.PENDING
    streak: int = 0
    reason: str = ""
    job: _Job | None = None
    not_before_ms: int = 0
    last_bucket: int | None = None
    last_warm: bool = False
    last_state: State | None = None
    values: dict[int, Mapping[str, Decimal]] = field(default_factory=dict)
    sigs: dict[int, tuple[str, int]] = field(default_factory=dict)
    defined: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class _Unavailable(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# ---------------------------------------------------------------------------------------------------- pure compute
def _complete_bucket(clock: BarSpec, end_ms: int) -> int:
    """The newest clock bucket whose end is at or before `end_ms` (an input bar's end)."""
    return clock.bucket_start_ms(end_ms) - clock.timeframe_ms


def evaluable(bar: Bar) -> bool:
    """An indicator input (§A4 evaluable bucket, bars only): not `blind` / `no_trade`, and a BROKER bar is final."""
    if bar.quality in (Quality.BLIND, Quality.NO_TRADE):
        return False
    return bar.series.source is not Source.BROKER or bar.final_at_ms is not None


def _end(item: Bar | Point, spec: BarSpec) -> int:
    return item.bucket_end_ms if isinstance(item, Bar) else spec.bucket_end_ms(item.bucket_start_ms)


def _take(items: Sequence[Bar | Point], ends: Sequence[int], t_start: int, clock: BarSpec, w: Window) -> list[Bar | Point]:
    upto = bisect_right(ends, clock.bucket_end_ms(t_start))
    avail = items[:upto]
    if isinstance(w, Anchored):
        anchor = w.anchor_spec.bucket_start_ms(t_start)
        return [x for x in avail if x.bucket_start_ms >= anchor][-w.max_bars:]
    return list(avail[-w.n:])


def compute_buckets(ind: Indicator, spec: IndicatorSpec, data: Mapping[InputRef, Sequence[Bar | Point]],
                    targets: Sequence[int], *, blind: Mapping[SeriesId, frozenset[int]] | None = None,
                    input_specs: Mapping[InputRef, BarSpec] | None = None,
                    cpu_cap_ms: int | None = None) -> list[Computed]:
    """The canonical value of each target clock bucket: `seed` over exactly its window (the alignment rule of
    `base`), outputs checked (finite, the declared bound, the declared names), the warm rule. Pure apart from the
    thread-CPU clock; raises `IndicatorFault` for a bad value or one target over the CPU cap, and whatever the class
    raises."""
    return compute_batch(ind, spec, data, targets, blind=blind, input_specs=input_specs, cpu_cap_ms=cpu_cap_ms)[0]


def compute_batch(ind: Indicator, spec: IndicatorSpec, data: Mapping[InputRef, Sequence[Bar | Point]],
                  targets: Sequence[int], *, blind: Mapping[SeriesId, frozenset[int]] | None = None,
                  input_specs: Mapping[InputRef, BarSpec] | None = None, cpu_cap_ms: int | None = None,
                  budget_ms: int | None = None) -> tuple[list[Computed], int]:
    """`compute_buckets` sized to the CPU budget: the computed buckets and how many targets were done. The CPU cap
    applies per target (one target over `cpu_cap_ms` is the fault); the batch stops after the target that takes the
    job's thread CPU past `budget_ms`, so the caller re-queues the rest and a long window over many targets is never
    an over-cap job retried unchanged. At least one target is always done (or the job faults)."""
    t0 = time.thread_time_ns()
    clock = ind.clock(spec)
    windows = ind.window(spec)
    counts = ind.warm_counts(spec)
    span = ind.warm_span(spec)
    names = {o.name for o in ind.OUTPUTS}
    specs = dict(input_specs or {})
    for ref in windows:
        if isinstance(ref, SeriesRef):
            specs.setdefault(ref, ref.series.spec)
        specs.setdefault(ref, clock)
    ends = {ref: [_end(x, specs[ref]) for x in data.get(ref, ())] for ref in windows}
    out: list[Computed] = []
    done = 0
    with localcontext(ind.DECIMAL_CONTEXT):
        for t in targets:
            if budget_ms is not None and done and (time.thread_time_ns() - t0) // 1_000_000 >= budget_ms:
                break
            t1 = time.thread_time_ns()
            done += 1
            history = {ref: _take(data.get(ref, ()), ends[ref], t, clock, w) for ref, w in windows.items()}
            state = ind.seed(spec, history)
            outs = ind.outputs(spec, state)
            if cpu_cap_ms is not None and (time.thread_time_ns() - t1) // 1_000_000 > cpu_cap_ms:
                raise IndicatorFault("cpu_cap")
            if outs is None:
                continue
            if set(outs) != names:
                raise IndicatorFault("bad_outputs")
            for v in outs.values():
                if not isinstance(v, Decimal) or not v.is_finite():
                    raise IndicatorFault("not_finite")
                if ind.LOWER_BOUND is not None and v < ind.LOWER_BOUND:
                    raise IndicatorFault("out_of_range")
            bars = [x for h in history.values() for x in h if isinstance(x, Bar)]
            warm = all(len(history.get(ref, ())) >= need for ref, need in counts.items())
            if warm and span > 0:
                lo = t - (span - 1) * clock.timeframe_ms
                for ref in windows:
                    if not isinstance(ref, SeriesRef):
                        continue
                    holes = (blind or {}).get(ref.series, frozenset())
                    if any(lo <= b <= t for b in holes):
                        warm = False
            starts = [x.bucket_start_ms for h in history.values() for x in h]
            finals = [x.final_at_ms for h in history.values() for x in h if x.final_at_ms is not None]
            worst = max((_QUALITY_RANK.get(b.quality, 2) for b in bars), default=0)
            quality = next(q for q, r in _QUALITY_RANK.items() if r == worst)
            out.append(Computed(t, dict(outs), state, warm, min(starts) if starts else None, input_digest(bars),
                                max(finals) if finals else None, quality))
    return out, done


# ---------------------------------------------------------------------------------------------------- the task
class MdIndicators:
    """The `md-indicators` task body. One instance per term (S31)."""

    def __init__(self, bars: BarsReadPort, *, registry: IndicatorRegistry = REGISTRY,
                 write: IndWritePort | None = None, token: Callable[[], K.WriterToken] | None = None,
                 boot: Callable[[], K.BootSentinel | None] | None = None, blind: BlindSource | None = None,
                 pager: PagePort | None = None, on_fault: FaultSink | None = None,
                 complete: Completion | None = None, settings: IndicatorSettings | None = None,
                 run_in_thread: ThreadRunner[Any] | None = None, clock_ms: Callable[[], int] = _wall_ms,
                 mono_ms: Callable[[], int] = _mono_ms,
                 each_cycle: Callable[[], Awaitable[object]] | None = None,
                 global_failed: Callable[[], bool] | None = None) -> None:
        """`global_failed`: the ARM-global FAILED flag (S32): while it reads True no queued recompute runs (§B14,
        md-indicators stops writing; the jobs stay queued for after the clear), only `md_rebuild` repairs (§B13).
        `run_in_thread` (tests only) replaces the task's own executor."""
        self._each_cycle = each_cycle
        self._global_failed = global_failed
        self._bars = bars
        self._registry = registry
        self._write = write
        self._token = token
        self._boot = boot
        self._blind = blind
        self._pager = pager
        self._on_fault = on_fault
        self._complete = complete
        self._cfg = settings or IndicatorSettings()
        self._executor: ThreadPoolExecutor | None = None
        self._closed = False
        self._thread: ThreadRunner[Any] = run_in_thread if run_in_thread is not None else self._own_executor
        self._clock_ms = clock_ms
        self._mono_ms = mono_ms
        self._inds: dict[str, _Ind] = {}
        self._known: dict[str, IndicatorSpec] = {}
        self._order: tuple[str, ...] = ()
        self._newest: dict[SeriesId, int] = {}
        self._listeners: list[Callable[[IndicatorUpdate], None]] = []
        self._head: str | None = None
        self._faulted: dict[str, str] = {}      # FAULTED tombstones of untracked ids: cleared only by md_rebuild
        self._busy: set[str] = set()            # ids with a worker call in flight (running, queued or hung)
        self._threads = 0                       # worker calls in flight (running, queued or hung)
        self._hung = 0                          # of which still running after `job_timeout_s`
        self._saturation_paged = False
        self.counters = IndicatorCounters()

    # -------------------------------------------------------------------------------------------- registration
    def add(self, spec: IndicatorSpec) -> str | None:
        """Track a spec (idempotent). None when accepted, else a problem code (the registration check of §A6)."""
        iid = spec.indicator_id
        if iid in self._inds:
            return None
        if not self._cfg.indicator_inputs and self._reads_indicator(spec):
            log.warning("md-indicators: %s refused (indicator_inputs: DAG inputs are not canonical yet)", iid)
            return "registration_refused"
        specs = {k: v.spec for k, v in self._inds.items()}
        specs[iid] = spec
        plan = plan_specs(self._registry, specs)
        if isinstance(plan, list):
            log.warning("md-indicators: %s refused (%d problem(s))", iid, len(plan))
            return "registration_refused"
        st = _Ind(spec, self._registry.of(spec))
        self._inds[iid] = st
        self._known[iid] = spec
        self._order = plan.order
        reason = self._faulted.pop(iid, None)
        if reason is not None:          # FAULTED before its leases lapsed: still FAULTED until md_rebuild
            st.state, st.reason = IndState.FAULTED, reason
            return None
        self._queue(st, _Job(seed=True))
        return None

    def _reads_indicator(self, spec: IndicatorSpec) -> bool:
        """The spec reads another indicator (an `IndicatorRef` input; an unknown class is left to `plan_specs`)."""
        ind = self._registry.get(spec.kind, spec.version)
        if ind is None:
            return False
        try:
            return any(isinstance(r, IndicatorRef) for r in ind.inputs(spec))
        except Exception:
            return True

    def remove(self, indicator_id: str) -> bool:
        """Stop computing an indicator (its demand ended). Kept while another tracked spec reads it. A FAULTED
        indicator leaves a tombstone, so demanding it again never clears FAULTED without `md_rebuild`."""
        if any(isinstance(r, IndicatorRef) and r.indicator_id == indicator_id
               for k, st in self._inds.items() if k != indicator_id for r in st.ind.inputs(st.spec)):
            return False
        st = self._inds.pop(indicator_id, None)
        if st is None:
            return False
        if st.state is IndState.FAULTED:
            self._faulted[indicator_id] = st.reason
        self._order = tuple(k for k in self._order if k != indicator_id)
        return True

    async def register(self, spec: IndicatorSpec) -> IndicatorStatus | None:
        """`register()` of §A6: track the spec, then compute its canonical value from stored bars (in a worker
        thread) before reporting `warm`. None when the registration check refuses it. An indicator already tracked
        (another consumer's lease) is never run again here: a FAULTED one stays FAULTED and a busy one keeps its one
        thread; its status is returned as it is."""
        iid = spec.indicator_id
        if iid in self._inds:
            return self.status(iid)
        if self.add(spec) is not None:
            return None
        st = self._inds[iid]
        if (st.state is IndState.FAULTED or st.job is None or iid in self._busy or self._saturated()
                or (self._global_failed is not None and self._global_failed())):
            return self.status(iid)     # the queued job (if any) runs in a later cycle (after an ARM-global clear)
        job, st.job = st.job, None
        await self._run_locked(st, job, _no_beat)
        return self.status(iid)

    def spec(self, indicator_id: str) -> IndicatorSpec | None:
        return self._known.get(indicator_id)

    def input_series(self, indicator_id: str) -> tuple[SeriesId, ...] | None:
        """Every bar series the indicator reads (the port's decision gate); None for an id never tracked here."""
        return input_series(self._registry, self._known.get, indicator_id)

    def tracked(self) -> tuple[str, ...]:
        return self._order

    # -------------------------------------------------------------------------------------------- inputs
    def on_released(self, event: BarEndEvent) -> None:
        """`MdWriter.on_released`: a bar was written (with its `bar_closed` / `bar_revised`)."""
        bucket = event.series.spec.bucket_start_ms(event.bar_end_ms - 1)
        newest = self._newest.get(event.series)
        if newest is None or bucket > newest:
            self._newest[event.series] = bucket
        self._dirty(event.series, bucket, revised=event.revised)

    def on_resolved(self, series: SeriesId, bucket_start_ms: int, resolution: object) -> None:
        """`md-bars` `bucket_resolved` (R11-M03): recompute the dependants at once."""
        self.counters.resolved += 1
        self._dirty(series, bucket_start_ms, revised=True)

    def _dirty(self, series: SeriesId, bucket: int, *, revised: bool) -> None:
        ref = SeriesRef(series)
        for st in self._inds.values():
            if ref not in st.ind.inputs(st.spec):
                continue
            clock = st.ind.clock(st.spec)
            t = clock.bucket_start_ms(bucket)
            older = st.last_bucket is not None and t <= st.last_bucket
            self._queue(st, _Job(from_bucket_ms=t if (revised or older) else None))

    def _queue(self, st: _Ind, job: _Job) -> None:
        if st.state is IndState.FAULTED and not job.request_ids:
            self.counters.dropped += 1
            return
        if st.job is None:
            st.job = job
        else:
            st.job.merge(job)

    # -------------------------------------------------------------------------------------------- work items
    def md_rebuild(self, request_id: int, indicator_id: str) -> str | None:
        """The `md_rebuild` work item (from the S32 handler): re-seed from stored bars, clear FAULTED, report warm and
        complete the request (`applied`) once computed. None when accepted, else the refusal code."""
        if indicator_id in self._busy:
            return "busy"
        st = self._inds.get(indicator_id)
        if st is None:
            if indicator_id not in self._faulted and indicator_id not in self._known:
                return NOT_REGISTERED
            # not demanded now: clear its tombstone; the next demand re-seeds it from stored bars
            self._faulted.pop(indicator_id, None)
            self.counters.rebuilds += 1
            self._finish_requests(_Job(request_ids=[request_id]), "not_tracked", ok=True)
            return None
        self.counters.rebuilds += 1
        st.state, st.streak, st.reason, st.not_before_ms = IndState.PENDING, 0, "", 0
        self._queue(st, _Job(seed=True, request_ids=[request_id]))
        return None

    def on_quarantine(self) -> None:
        """The supervisor's task-circuit hook: drop the head job and mark its indicator FAULTED."""
        iid = self._head or next((k for k in self._order if self._inds[k].job is not None), None)
        st = self._inds.get(iid) if iid is not None else None
        if st is None:
            return
        st.job = None
        self._open_circuit(st, "task_quarantine")

    # -------------------------------------------------------------------------------------------- task body
    async def run(self, stop: asyncio.Event, stamp: StampPort) -> None:
        while not stop.is_set():
            stamp.stamp()
            await self.cycle(stop, stamp)
            stamp.stamp()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._cfg.interval_s)
            except TimeoutError:
                pass

    async def cycle(self, stop: asyncio.Event | None = None, stamp: StampPort | None = None) -> None:
        """Run the queued jobs in recompute order, inside one cycle budget. The task stamps after the per-cycle hook,
        before each job and after each bounded step inside it (a read, the thread, a write), so no gap between two
        stamps exceeds one step's timeout (`job_timeout_s + io_timeout_s` < the 10 s stamp budget)."""
        beat = stamp.stamp if stamp is not None else _no_beat
        self.counters.cycles += 1
        deadline = self._mono_ms() + int(self._cfg.cycle_budget_s * 1000)
        if self._each_cycle is not None:      # the demand book's lease writes (S31 wiring), contained
            try:
                async with asyncio.timeout(self._cfg.io_timeout_s):
                    await self._each_cycle()
            except Exception as e:
                log.info("md-indicators: per-cycle hook failed (%s)", safe_redis_error(e))
            beat()
        if self._saturated():
            self._page_saturation()
            return                            # jobs stay queued until the hung threads return
        self._saturation_paged = False
        repairs_only = self._global_failed is not None and self._global_failed()
        if repairs_only:
            self.counters.failed_cycles += 1
        now = self._clock_ms()
        for iid in self._order:
            if stop is not None and stop.is_set():
                return
            if self._mono_ms() >= deadline or self._saturated():
                return
            st = self._inds.get(iid)
            if st is None or st.job is None or iid in self._busy or now < st.not_before_ms:
                continue
            if st.state is IndState.FAULTED and not st.job.request_ids:
                st.job = None
                self.counters.dropped += 1
                continue
            if repairs_only and not st.job.request_ids:
                continue                      # ARM global FAILED: kept queued, run after the clear
            job, st.job = st.job, None
            beat()
            await self._run_locked(st, job, beat)
            beat()

    def _saturated(self) -> bool:
        return self._hung >= self._cfg.max_hung_threads or self._threads >= self._cfg.max_threads

    def _page_saturation(self) -> None:
        if self._saturation_paged or self._hung < self._cfg.max_hung_threads:
            return
        self._saturation_paged = True
        log.warning("md-indicators: %d worker thread(s) hung; no indicator job is dispatched until they return",
                    self._hung)
        if self._pager is not None:
            try:
                self._pager.page("warning", "indicator threads hung",
                                 f"{self._hung} md-indicators worker thread(s) hung; indicator jobs paused")
            except Exception:
                log.warning("md-indicators: saturation page failed")

    async def _own_executor(self, fn: Callable[[], T]) -> T:
        """Run `fn` on `md-indicators`' own bounded executor (created on first use, `max_threads` workers): never the
        loop's default executor (round 13, §B4)."""
        if self._closed:
            raise RuntimeError("md-indicators is closed")
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self._cfg.max_threads, thread_name_prefix=THREAD_PREFIX)
        result: T = await asyncio.wrap_future(self._executor.submit(fn))
        return result

    @property
    def hung_threads(self) -> int:
        """Worker threads still running past `job_timeout_s` (they hold slots of the task's own executor only)."""
        return self._hung

    def close(self) -> bool:
        """Shut the own executor down without waiting (queued calls are cancelled; a running thread cannot be) and
        return True when a worker is still hung: the caller must then end the process through the hard exit, since
        the interpreter joins executor threads at exit (the S22 clean-stop rule, extended to indicators)."""
        self._closed = True
        executor, self._executor = self._executor, None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        return self._hung > 0

    async def _in_thread(self, iid: str | None, fn: Callable[[], T]) -> T:
        """Run `fn` on a worker thread (the task's own executor) without ever cancelling it: a call still
        running after `job_timeout_s` raises TimeoutError and stays counted (hung, and its id busy) until the thread
        returns, so a call that had not started yet is never lost with its id left busy. Counted per call, so
        `md-indicators` never queues behind its own hung workers."""
        runner = self._thread
        finished = asyncio.Event()
        hung = [False]

        async def call() -> T:
            result: T = await runner(fn)
            return result

        self._threads += 1
        if iid is not None:
            self._busy.add(iid)
        task = asyncio.create_task(call())

        def done(_: object) -> None:
            self._threads -= 1
            if iid is not None:
                self._busy.discard(iid)
            if hung[0]:
                self._hung -= 1
            if not task.cancelled():
                task.exception()              # retrieved here; the awaiting side reads it through result()
            finished.set()

        task.add_done_callback(done)
        try:
            await asyncio.wait_for(finished.wait(), timeout=self._cfg.job_timeout_s)
        except TimeoutError:
            if not task.done():
                hung[0] = True
                self._hung += 1
                raise
        return task.result()

    async def _run_locked(self, st: _Ind, job: _Job, beat: Beat) -> None:
        async with st.lock:
            self._head = st.spec.indicator_id
            try:
                await self._run_job(st, job, beat)
            except Exception as e:   # a job ends in a value: nothing reaches the task
                log.warning("md-indicators: %s job failed (%s)", st.spec.indicator_id, safe_redis_error(e))
                await self._fault(st, job, "internal")
            finally:
                self._head = None

    async def _run_job(self, st: _Ind, job: _Job, beat: Beat) -> None:
        self.counters.jobs += 1
        spec, ind = st.spec, st.ind
        try:
            newest = await self._newest_bucket(st)
        except _Unavailable:
            return self._defer(st, job)
        beat()
        if newest is None:
            return self._finish_requests(job, "no_bars")
        if job.from_bucket_ms is not None:
            targets = sorted({b for b in st.values if job.from_bucket_ms <= b <= newest} | {newest})
        else:
            targets = [newest]
        rest: list[int] = []
        if len(targets) > self._cfg.batch:
            targets, rest = targets[:self._cfg.batch], targets[self._cfg.batch:]
        try:
            data, specs, holes = await self._read_inputs(st, targets[0], targets[-1], None, beat)
        except _Unavailable:
            return self._defer(st, job)
        blind = self._blind_map(st, holes)
        cap = self._cfg.cpu_cap_ms
        budget = max(1, cap // 2)

        def work() -> tuple[list[Computed], int]:
            return compute_batch(ind, spec, data, targets, blind=blind, input_specs=specs, cpu_cap_ms=cap,
                                 budget_ms=budget)

        results: list[Computed]
        try:
            results, done = await self._in_thread(spec.indicator_id, work)
        except TimeoutError:
            beat()
            self.counters.hung += 1
            self._open_circuit(st, "job_hung")
            await self._write_fault(st)
            self._finish_requests(job, "job_hung")
            return None
        except IndicatorFault as e:
            beat()
            if e.reason == "cpu_cap":
                self.counters.cpu_capped += 1
            return await self._fault(st, job, e.reason)
        except Exception:   # the class raised in seed / outputs
            beat()
            return await self._fault(st, job, "raised")
        beat()
        rest = list(targets[done:]) + rest     # the CPU budget ended the batch early: the rest is the next job
        if not await self._store(st, results, beat):
            return self._defer(st, job)
        st.streak = 0
        if st.state is not IndState.FAULTED:
            st.state = IndState.READY
        if rest:
            self._queue(st, _Job(from_bucket_ms=rest[0]))
        self._finish_requests(job, "warm" if self.is_warm(spec.indicator_id) else "not_warm", ok=True)
        return None

    def _defer(self, st: _Ind, job: _Job) -> None:
        self.counters.deferred += 1
        st.not_before_ms = self._clock_ms() + self._cfg.retry_ms
        self._queue(st, job)

    def _finish_requests(self, job: _Job, reason: str, *, ok: bool = False) -> None:
        for rid in job.request_ids:
            if self._complete is None:
                continue
            try:
                self._complete(rid, APPLIED if ok else REFUSED, reason)
            except Exception:   # the request store's fault never stops the task
                log.warning("md-indicators: md_rebuild completion failed for request %d", rid)
        job.request_ids.clear()

    async def _fault(self, st: _Ind, job: _Job, reason: str) -> None:
        """A per-indicator fault: re-seed and retry; the `fault_limit`th in a row opens its circuit (FAULTED)."""
        self.counters.faults += 1
        st.streak += 1
        log.warning("md-indicators: %s fault %d/%d (%s)", st.spec.indicator_id, st.streak, self._cfg.fault_limit,
                    reason)
        if st.streak < self._cfg.fault_limit:
            st.not_before_ms = self._clock_ms() + self._cfg.retry_ms
            self._queue(st, _Job(from_bucket_ms=job.from_bucket_ms, seed=True, request_ids=list(job.request_ids)))
            return
        self._open_circuit(st, reason)
        await self._write_fault(st)
        self._finish_requests(job, reason)

    def _open_circuit(self, st: _Ind, reason: str) -> None:
        if st.state is IndState.FAULTED:
            return
        st.state, st.reason, st.last_warm = IndState.FAULTED, reason, False
        self.counters.faulted += 1
        iid = st.spec.indicator_id
        log.warning("md-indicators: %s FAULTED (%s); jobs dropped until md_rebuild", iid, reason)
        if self._on_fault is not None:
            try:
                self._on_fault(iid, reason)
            except Exception:
                log.warning("md-indicators: fault sink failed for %s", iid)
        if self._pager is not None:
            try:
                self._pager.page("warning", "indicator faulted", f"indicator {iid} faulted ({reason})")
            except Exception:
                log.warning("md-indicators: page failed for %s", iid)

    # -------------------------------------------------------------------------------------------- reads
    def _blind_map(self, st: _Ind, holes: Mapping[SeriesId, frozenset[int]]) -> dict[SeriesId, frozenset[int]]:
        """Known blind buckets per input series: the `md-bars` lane plus the buckets read back `blind`."""
        out: dict[SeriesId, frozenset[int]] = dict(holes)
        if self._blind is None:
            return out
        for ref in st.ind.inputs(st.spec):
            if isinstance(ref, SeriesRef):
                try:
                    lane = frozenset(self._blind(ref.series))
                except Exception:   # an unknown lane is no evidence of a hole; warm still needs W bars
                    lane = frozenset()
                out[ref.series] = out.get(ref.series, frozenset()) | lane
        return out

    async def _port(self, series: SeriesId, lo: int, hi: int, as_of_ms: int | None) -> list[Bar]:
        try:
            async with asyncio.timeout(self._cfg.io_timeout_s):
                read = await self._bars.bars(series, from_bucket_ms=lo, to_bucket_ms=hi, as_of_ms=as_of_ms)
        except TimeoutError:
            raise _Unavailable("store_timeout") from None
        if isinstance(read, DataUnavailable):
            raise _Unavailable(read.reason)
        return list(read.bars)

    def _primary(self, st: _Ind) -> SeriesRef:
        refs = [r for r in st.ind.inputs(st.spec) if isinstance(r, SeriesRef)]
        return next((r for r in refs if r.series == st.spec.series), refs[0])

    async def _newest_bucket(self, st: _Ind) -> int | None:
        """The newest clock bucket complete in the primary series (its newest stored bar's end, on the clock grid)."""
        ref = self._primary(st)
        clock = st.ind.clock(st.spec)
        known = self._newest.get(ref.series)
        try:
            async with asyncio.timeout(self._cfg.io_timeout_s):
                read = await self._bars.bars(ref.series, n=1)
        except TimeoutError:
            raise _Unavailable("store_timeout") from None
        if isinstance(read, DataUnavailable):
            raise _Unavailable(read.reason)
        seen = [b for b in read.bars if b.quality is not Quality.BLIND]
        ends = [b.bucket_end_ms for b in seen]
        if seen:        # recorded, so `is_warm` knows the newest closed bucket after a restart or takeover too
            newest = max(b.bucket_start_ms for b in seen)
            self._newest[ref.series] = newest if known is None else max(known, newest)
        if known is not None:
            ends.append(ref.series.spec.bucket_end_ms(known))
        if not ends:
            return None
        return _complete_bucket(clock, max(ends))

    async def _read_inputs(self, st: _Ind, lo_t: int, hi_t: int, as_of_ms: int | None, beat: Beat = _no_beat
                           ) -> tuple[dict[InputRef, list[Bar | Point]], dict[InputRef, BarSpec],
                                      dict[SeriesId, frozenset[int]]]:
        """Each input's evaluable bars (or values) for the targets `[lo_t, hi_t]`, the input timeframes, and the
        buckets the read returned `blind` (a quarantined bar: a hole for the warm rule). A `Count` window whose first
        read holds fewer than `n` evaluable bars at the oldest target (a long market closure) reads further back, in
        `MAX_READ_BUCKETS` steps, until it has them, a step returns nothing or the series retention is reached."""
        clock = st.ind.clock(st.spec)
        hi_end = clock.bucket_end_ms(hi_t)
        data: dict[InputRef, list[Bar | Point]] = {}
        specs: dict[InputRef, BarSpec] = {}
        holes: dict[SeriesId, frozenset[int]] = {}
        for ref, w in st.ind.window(st.spec).items():
            if isinstance(ref, SeriesRef):
                tf = ref.series.spec.timeframe_ms
                hi = hi_end - tf
                if isinstance(w, Anchored):
                    lo = w.anchor_spec.bucket_start_ms(lo_t)
                else:
                    span = 2 * w.n + 2 * DAY_MS // tf
                    lo = clock.bucket_start_ms(lo_t) - span * tf
                lo = max(lo, hi - MAX_READ_BUCKETS * tf + tf, 0)
                in_spec = ref.series.spec
                bars = await self._port(ref.series, in_spec.bucket_start_ms(lo), in_spec.bucket_start_ms(hi),
                                        as_of_ms)
                beat()
                if isinstance(w, Count):
                    bars = await self._extend_back(ref.series, bars, in_spec.bucket_start_ms(lo), w.n,
                                                   clock.bucket_end_ms(lo_t), as_of_ms, beat)
                data[ref] = [b for b in bars if evaluable(b)]
                holes[ref.series] = frozenset(b.bucket_start_ms for b in bars if b.quality is Quality.BLIND)
                specs[ref] = in_spec
            else:
                src = self._inds.get(ref.indicator_id)
                if src is None:
                    raise _Unavailable("input_not_tracked")
                src_clock = src.ind.clock(src.spec)
                data[ref] = [Point(bucket_start_ms=b, value=v[ref.output]) for b, v in sorted(src.values.items())
                             if ref.output in v]
                specs[ref] = src_clock
        return data, specs, holes

    async def _extend_back(self, series: SeriesId, bars: list[Bar], lo: int, need: int, need_end_ms: int,
                           as_of_ms: int | None, beat: Beat) -> list[Bar]:
        """Older bars until `need` evaluable bars end at or before `need_end_ms` (bounded by the retention)."""
        tf = series.spec.timeframe_ms
        floor = max(0, self._clock_ms() - series_retention_ms(series))
        while sum(1 for b in bars if evaluable(b) and b.bucket_end_ms <= need_end_ms) < need:
            top = lo - tf
            if top < floor:
                break
            new_lo = series.spec.bucket_start_ms(max(top - (MAX_READ_BUCKETS - 1) * tf, floor))
            more = await self._port(series, new_lo, top, as_of_ms)
            beat()
            if not more:
                break
            bars, lo = more + bars, new_lo
        return bars

    # -------------------------------------------------------------------------------------------- writes
    def _members(self, st: _Ind, c: Computed) -> tuple[tuple[tuple[str, str], ...], str, int]:
        """The stored members of one bucket, its signature (quantised values, quality, warm, inputs) and its value
        revision (+1 only when the signature changed)."""
        iid = st.spec.indicator_id
        ctx = st.ind.DECIMAL_CONTEXT
        values: list[tuple[str, IndicatorValue]] = []
        for o in st.ind.OUTPUTS:
            q = quantise(c.outputs[o.name], quantum(o.unit, st.spec.price_increment), ctx)
            values.append((o.name, IndicatorValue(indicator_id=iid, output=o.name, bucket_start_ms=c.bucket_start_ms,
                                                  value=q, quality=c.quality, warm=c.warm,
                                                  final_at_ms=c.final_at_ms,
                                                  lineage=Lineage(iid, c.window_start_ms, c.digest, 0))))
        sig = "|".join(f"{name}={format(v.value, 'f') if v.value is not None else ''}" for name, v in values)
        sig += f"|{c.quality.value}|{int(c.warm)}|{c.digest}"
        prev = st.sigs.get(c.bucket_start_ms)
        rev = 0 if prev is None else (prev[1] if prev[0] == sig else prev[1] + 1)
        members = tuple((name, encode_value(IndicatorValue(
            indicator_id=v.indicator_id, output=v.output, bucket_start_ms=v.bucket_start_ms, value=v.value,
            quality=v.quality, warm=v.warm, final_at_ms=v.final_at_ms,
            lineage=Lineage(iid, v.lineage.window_start_ms, v.lineage.input_digest, rev)))) for name, v in values)
        return members, sig, rev

    async def _store(self, st: _Ind, results: Sequence[Computed], beat: Beat = _no_beat) -> bool:
        """Write each computed bucket (unchanged ones skipped), then tell the listeners. False: retry later."""
        clock = st.ind.clock(st.spec)
        for c in results:
            members, sig, rev = self._members(st, c)
            prev = st.sigs.get(c.bucket_start_ms)
            if prev is not None and prev[0] == sig and st.defined:
                self.counters.values_unchanged += 1
                self._remember(st, c, prev)
                continue
            ckpt = encode_checkpoint(c.bucket_start_ms, c.state) if due(c.bucket_start_ms, clock) else ""
            write = S.IndicatorWrite(st.spec.indicator_id, c.bucket_start_ms, clock.bucket_end_ms(c.bucket_start_ms),
                                     members, ckpt, "" if st.defined else st.spec.canonical)
            sent = await self._send(st, write)
            beat()
            if not sent:
                return False
            self.counters.values_written += 1
            st.defined = True
            self._remember(st, c, (sig, rev))
            self._emit(IndicatorUpdate(st.spec.indicator_id, c.bucket_start_ms, write.bar_end_ms))
        return True

    def _remember(self, st: _Ind, c: Computed, sig: tuple[str, int]) -> None:
        st.values[c.bucket_start_ms] = dict(c.outputs)
        st.sigs[c.bucket_start_ms] = sig
        for d in (st.values, st.sigs):
            while len(d) > self._cfg.keep:
                del d[min(d)]
        if st.last_bucket is None or c.bucket_start_ms >= st.last_bucket:
            st.last_bucket, st.last_warm, st.last_state = c.bucket_start_ms, c.warm, c.state

    async def _send(self, st: _Ind, write: S.IndicatorWrite) -> bool:
        if self._write is None:
            return True                      # memory only (no store wired)
        token = self._token() if self._token is not None else None
        boot = self._boot() if self._boot is not None else None
        if token is None or boot is None:
            return False
        now = self._clock_ms()
        try:
            call = S.indicator_call(token, boot, write,
                                    value_floor_ms=now - series_retention_ms(st.spec.series),
                                    checkpoint_floor_ms=now - CHECKPOINT_RETENTION_MS)
        except ValueError:
            self.counters.write_errors += 1
            return False
        try:
            async with asyncio.timeout(self._cfg.io_timeout_s):
                replies = await self._write.run([call])
        except (StoreUnavailable, TimeoutError) as e:
            self.counters.write_errors += 1
            log.info("md-indicators: %s write deferred (%s)", st.spec.indicator_id, safe_redis_error(e))
            return False
        reply = replies[0] if replies else None
        if isinstance(reply, ScriptOk):
            return True
        self.counters.write_errors += 1
        code = reply.code if isinstance(reply, ScriptRefused) else "failed"
        log.info("md-indicators: %s write refused (%s)", st.spec.indicator_id, code)
        return False

    async def _write_fault(self, st: _Ind) -> None:
        """Fault markers (no value, the reason in the lineage) for the newest bucket, then `indicator_updated`."""
        iid = st.spec.indicator_id
        clock = st.ind.clock(st.spec)
        bucket = st.last_bucket
        if bucket is None:      # never computed: the newest closed bucket, the one an md_rebuild value replaces
            series = self._primary(st).series
            known = self._newest.get(series)
            end = series.spec.bucket_end_ms(known) if known is not None else self._clock_ms()
            bucket = _complete_bucket(clock, end)
        members = tuple((o.name, encode_value(IndicatorValue(
            indicator_id=iid, output=o.name, bucket_start_ms=bucket, value=None, quality=Quality.BLIND, warm=False,
            final_at_ms=None, lineage=Lineage(iid, None, "0" * 16, 0, st.reason[:32] or "faulted"))))
            for o in st.ind.OUTPUTS)
        write = S.IndicatorWrite(iid, bucket, clock.bucket_end_ms(bucket), members, "",
                                 "" if st.defined else st.spec.canonical)
        st.sigs.pop(bucket, None)
        if await self._send(st, write):
            self._emit(IndicatorUpdate(iid, bucket, write.bar_end_ms, faulted=True))

    def _emit(self, update: IndicatorUpdate) -> None:
        for listener in self._listeners:
            try:
                listener(update)
            except Exception:
                self.counters.listener_errors += 1
                log.warning("md-indicators: update listener failed for %s", update.indicator_id)

    # -------------------------------------------------------------------------------------------- reads for the port
    def on_updated(self, listener: Callable[[IndicatorUpdate], None]) -> None:
        """`indicator_updated` in process (after the value write)."""
        self._listeners.append(listener)

    def status(self, indicator_id: str) -> IndicatorStatus | None:
        st = self._inds.get(indicator_id)
        if st is None:
            return None
        return IndicatorStatus(indicator_id, st.state, self.is_warm(indicator_id), st.last_bucket, st.streak,
                               st.reason)

    def is_warm(self, indicator_id: str) -> bool:
        """The window rule held for the newest value and its bucket is the newest closed bucket of the clock series;
        never for a FAULTED indicator."""
        st = self._inds.get(indicator_id)
        if st is None or st.state is IndState.FAULTED or st.last_bucket is None or not st.last_warm:
            return False
        series = self._primary(st).series
        newest = self._newest.get(series)
        if newest is None:
            return False                # the newest closed bucket is unknown: never warm
        clock = st.ind.clock(st.spec)
        return _complete_bucket(clock, series.spec.bucket_end_ms(newest)) <= st.last_bucket

    def latest(self, indicator_id: str, output: str) -> Decimal | None:
        """The exact newest value (memory; never logged)."""
        st = self._inds.get(indicator_id)
        if st is None or st.last_bucket is None or st.state is IndState.FAULTED:
            return None
        return st.values.get(st.last_bucket, {}).get(output)

    def provisional(self, indicator_id: str, output: str,
                    forming: Callable[[SeriesId], Bar | None]) -> Decimal | None:
        """`provisional()` from the newest state and the forming bars (display only; never stored)."""
        st = self._inds.get(indicator_id)
        if st is None or st.last_state is None or st.state is IndState.FAULTED:
            return None
        bars: dict[InputRef, Bar | Point] = {}
        for ref in st.ind.inputs(st.spec):
            if isinstance(ref, SeriesRef):
                b = forming(ref.series)
                if b is not None:
                    bars[ref] = b
        try:
            with localcontext(st.ind.DECIMAL_CONTEXT):
                value = st.ind.provisional(st.spec, st.last_state, bars).get(output)
        except Exception:
            return None
        return value if value is not None and value.is_finite() else None

    async def value_as_of(self, indicator_id: str, bucket_start_ms: int,
                          as_of_ms: int) -> dict[str, IndicatorValue] | DataUnavailable:
        """The canonical value at `bucket` recomputed from the bars current at `as_of_ms` (§A10). A needed revision
        that has aged out of `md:bars_rev` gives `superseded_expired`."""
        spec = self._known.get(indicator_id)
        if spec is None:
            return DataUnavailable(NOT_REGISTERED)
        st = self._inds.get(indicator_id) or _Ind(spec, self._registry.of(spec))
        try:
            data, specs, holes = await self._read_inputs(st, bucket_start_ms, bucket_start_ms, as_of_ms)
            current, _, _ = await self._read_inputs(st, bucket_start_ms, bucket_start_ms, None)
        except _Unavailable as e:
            return DataUnavailable(e.reason)
        for ref, items in current.items():
            held = {x.bucket_start_ms for x in data.get(ref, ())}
            for x in items:
                if (isinstance(x, Bar) and x.bucket_start_ms not in held and x.revision > 0
                        and x.final_at_ms is not None and x.final_at_ms > as_of_ms):
                    return DataUnavailable(SUPERSEDED_EXPIRED)
        ind = st.ind
        blind = self._blind_map(st, holes)
        if self._saturated():
            return DataUnavailable(SATURATED)
        cap = self._cfg.cpu_cap_ms
        try:
            results: list[Computed] = await self._in_thread(None, lambda: compute_buckets(
                ind, spec, data, [bucket_start_ms], blind=blind, input_specs=specs, cpu_cap_ms=cap))
        except TimeoutError:
            return DataUnavailable("job_hung")
        except Exception:
            return DataUnavailable("indicator_faulted")
        if not results:
            return DataUnavailable("no_value")
        c = results[0]
        out: dict[str, IndicatorValue] = {}
        for o in ind.OUTPUTS:
            q = quantise(c.outputs[o.name], quantum(o.unit, spec.price_increment), ind.DECIMAL_CONTEXT)
            out[o.name] = IndicatorValue(indicator_id=indicator_id, output=o.name, bucket_start_ms=c.bucket_start_ms,
                                         value=q, quality=c.quality, warm=c.warm, final_at_ms=c.final_at_ms,
                                         lineage=Lineage(indicator_id, c.window_start_ms, c.digest, 0))
        return out


__all__ = [
    "MdIndicators", "IndicatorSettings", "IndicatorCounters", "IndicatorStatus", "IndicatorUpdate", "IndState",
    "IndicatorFault", "Computed", "compute_buckets", "compute_batch", "evaluable", "BarsReadPort", "IndWritePort", "APPLIED",
    "REFUSED", "SUPERSEDED_EXPIRED",
]
