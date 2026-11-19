"""The FAILED clear probes (design §B14 Clearing, §A7 trial bootstrap; plan S30b, R11-M09, R11-M12).

A clear (`python -m src.ops arm failed clear <scope> --reason <text>`, an audited `arm_clear_failed` request) is
accepted only after a probe of the episode's **recorded cause** passes. `arm_failures.probe_kind` was chosen from the
reason when the episode opened (`src.riskmgr.budget.probe_kind`):

| probe kind | reasons | passes when |
|---|---|---|
| `series_reread` | `invalid_bar`, `source_mismatch`, `refetch_failed` | a re-read of the series' rollback set passes every hard check, and the tick cross-check where tick bars exist (`SeriesProbePort.reread`, md-bars) |
| `fresh_bar` | `unresolved` | one fresh bar read reaches finality within `post_grace` (`SeriesProbePort.fresh_bar`, md-bars) |
| `indicator_rebuild` | `indicator_faulted` | `md_rebuild` has completed and every indicator on the series is warm (`IndicatorStateProbe`) |
| `span_write` | `stuck_frontier` (POSITION or ARM) | the failed span write of the recorded positions, re-run fenced on the ARM pool inside a rolled-back savepoint, succeeds (`SpanProbePort`, S38c) |
| `strategy_conformance` | `strategy_version` | the registry's declaration checks of that version and its own code against its conformance vectors (`src.riskmgr.vectors`), run as one successful job on its lane under the job cap (`VersionProbePort`; plan S39b, go-live gate `conformance_probe_partial` wired) |
| `adapter_series` | ADAPTER `series_failed` | the probe of each of the adapter's FAILED series passes (none open: passes) |
| `arm_global` | every other ARM reason | no more strategy job threads are hung than the job runner may abandon (`HungThreadsView`, check `threads_hung`; a `restart_required` episode refuses `probe_not_wired` without that view), no task circuit is open (`CircuitView`), a fenced test write on the ARM pool succeeds, and a **trial bootstrap** writes `md:boot` under the writer token and boot lock (`TrialBootstrapProbe`; allowed while globally FAILED, never counted toward the reset budget, never an MD_RESET notice) |

A refused probe names the failing check (a code) and, where one helps, the request to run first (`FIRST_COMMANDS`,
e.g. `md_rebuild` before an `indicator_faulted` clear). The operator answer is the code
`cause_present_<check>[_first_<command>]` (`refusal_reason`); `arm failed show` reads the last probe through
`arm_failures.last_probe_result` (`probe_note`). Ports are implemented by the runtime wiring (S31 / S32: md-bars for
the series reads, md-indicators for the indicator view, the BarClock's store for the span write, the registry and the
job runner for the version probe, the task supervisor for the circuits); a probe whose port is not wired refuses
(`probe_not_wired`, fail closed). **Plan S38e** wires the last two: `MdBarsSeriesProbe` (`series_reread`,
`fresh_bar` over `MdBars.probe_reread` / `probe_fresh_bar`) and `VersionConformanceProbe` (`strategy_conformance`:
the registry's declaration checks of the pinned class, run as one job on the version's lane through the job runner;
since S39b also the version's own code against its conformance vectors, `src.riskmgr.vectors`), so every FAILED scope
kind can be cleared. Nothing here formats a price, an indicator value or a spread: codes only.
"""
from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final, Protocol

from src.marketdata.bootstrap import BootResult
from src.marketdata.indicators.engine import IndicatorStatus, IndState
from src.marketdata.types import SeriesId, Source
from src.riskmgr.budget import ScopeRef
from src.riskmgr.jobs import JobRunner, JobSpec
from src.riskmgr.outcomes import reason_map_problems
from src.riskmgr.registry import ArmRegistry
from src.riskmgr.strategy import ReleasePolicy, metric_problems
from src.riskmgr.types import ArmEvent, ScopeKind
from src.riskmgr.vectors import vector_problems

SERIES_REREAD: Final = "series_reread"
FRESH_BAR: Final = "fresh_bar"
INDICATOR_REBUILD: Final = "indicator_rebuild"
SPAN_WRITE: Final = "span_write"
STRATEGY_CONFORMANCE: Final = "strategy_conformance"
ADAPTER_SERIES: Final = "adapter_series"
ARM_GLOBAL: Final = "arm_global"
PROBE_KINDS: Final = frozenset({SERIES_REREAD, FRESH_BAR, INDICATOR_REBUILD, SPAN_WRITE, STRATEGY_CONFORMANCE,
                                ADAPTER_SERIES, ARM_GLOBAL})

FIRST_COMMANDS: Final = frozenset({"md_backfill", "md_rebuild", "md_writer_reset", "md_rehash", "reset_task",
                                   "arm_repin"})
"""The requests a refused clear may name to run first (all allowed while the scope is FAILED, §B13)."""
CHECK_PATTERN: Final = re.compile(r"[a-z][a-z_]{0,23}")

# check codes this module and the clear path use
PROBE_NOT_WIRED: Final = "probe_not_wired"
PROBE_TIMEOUT: Final = "probe_timeout"
PROBE_ERROR: Final = "probe_error"
FAULT_DURING_PROBE: Final = "fault_during_probe"
"""A new fault of the scope arrived while its probe ran: the earlier pass no longer speaks for the scope."""
CIRCUIT_OPEN: Final = "circuit_open"
ARM_POOL: Final = "arm_pool"
TRIAL_BOOTSTRAP: Final = "trial_bootstrap"
INDICATOR_FAULTED_CHECK: Final = "indicator_faulted"
INDICATOR_NOT_WARM: Final = "indicator_not_warm"
INDICATOR_NOT_TRACKED: Final = "indicator_not_tracked"
INDICATOR_UNKNOWN: Final = "indicator_unknown"
THREADS_HUNG: Final = "threads_hung"
"""ARM global: more strategy job threads are still hung than the job runner may abandon (`restart_required`)."""
SERIES_UNKNOWN: Final = "series_unknown"
"""A SERIES scope id that is not a BROKER series' canonical id (the series probes, S38e)."""
VERSION_UNKNOWN: Final = "version_unknown"
"""A STRATEGY_VERSION scope naming no registered class (re-pin the positions first)."""
CONFORMANCE: Final = "conformance"
"""The pinned class fails the registry's declaration checks (S38e)."""
JOB_LANE_HUNG: Final = "job_lane_hung"
PROBE_POSITION: Final = "clear_probe"
"""The position id a version probe's job is counted under (no position: the probe's own job)."""


@dataclass(frozen=True)
class ProbeResult:
    """A probe's verdict: `passed`, or the failing `check` (a code) and the request to run `first` (or "")."""

    passed: bool
    check: str = ""
    first: str = ""

    def __post_init__(self) -> None:
        if self.passed and (self.check or self.first):
            raise ValueError("a passed probe names no check")
        if not self.passed and not CHECK_PATTERN.fullmatch(self.check):
            raise ValueError("a refused probe names its check as a short code")
        if self.first and self.first not in FIRST_COMMANDS:
            raise ValueError(f"{self.first!r} is not a request a refused clear may name")


PASSED: Final = ProbeResult(True)


def refused_probe(check: str, first: str = "") -> ProbeResult:
    return ProbeResult(False, check, first)


def refusal_reason(result: ProbeResult) -> str:
    """The operator answer code of a refused clear: `cause_present_<check>[_first_<command>]` (at most 64)."""
    reason = f"cause_present_{result.check}"
    if result.first:
        reason += f"_first_{result.first}"
    return reason[:64]


def probe_note(result: ProbeResult) -> str:
    """`arm_failures.last_probe_result` (`arm failed show`): `passed` or `refused:<check>[;first:<command>]`."""
    if result.passed:
        return "passed"
    return f"refused:{result.check}" + (f";first:{result.first}" if result.first else "")


# ---------------------------------------------------------------------------------------------------- ports
class SeriesProbePort(Protocol):
    """md-bars (S31 / S32): the probe reads of one series (canonical id), through its own rate-limited lane."""

    async def reread(self, series: str) -> ProbeResult:
        """Re-read the series' rollback set from the broker: passes when every bar passes every hard check and the
        tick cross-check where tick bars exist (`series_reread`)."""
        ...

    async def fresh_bar(self, series: str) -> ProbeResult:
        """One fresh bar read reaches finality within the series' `post_grace` (`fresh_bar`)."""
        ...


class IndicatorProbePort(Protocol):
    async def rebuilt_and_warm(self, series: str) -> ProbeResult: ...


class SpanProbePort(Protocol):
    async def test_span_write(self, position_ids: Sequence[str]) -> ProbeResult:
        """Re-run the failed span write of the recorded positions (`adapter_id:position_id`, the episode's
        `position_ids`) on the ARM pool, fenced, inside a rolled-back savepoint (`span_write`, S38c): a synthetic
        write would pass while a poisoned row still fails."""
        ...


class VersionProbePort(Protocol):
    async def probe(self, scope_id: str) -> ProbeResult:
        """The conformance checks plus one successful job of `strategy:<KIND>:<VERSION>` / `guard:<KIND>:<VERSION>`."""
        ...


class TrialProbePort(Protocol):
    async def trial(self) -> ProbeResult: ...


class HungThreadsView(Protocol):
    def restart_still_required(self) -> bool:
        """More strategy job threads are hung than the runner may abandon (`JobRunner.restart_still_required`)."""
        ...


class CircuitView(Protocol):
    def open_circuits(self) -> Sequence[str]:
        """The ARM task circuits open now (task names; `src.riskmgr.budget.CIRCUIT_TASKS`)."""
        ...


@dataclass(frozen=True, kw_only=True)
class ClearProbes:
    """The probe ports (None: not wired, the probe refuses `probe_not_wired`)."""

    series: SeriesProbePort | None = None
    indicators: IndicatorProbePort | None = None
    span: SpanProbePort | None = None
    versions: VersionProbePort | None = None
    trial: TrialProbePort | None = None
    circuits: CircuitView | None = None
    hung: HungThreadsView | None = None


# ---------------------------------------------------------------------------------------------------- concrete
class TrialPort(Protocol):
    def request_trial(self, done: Callable[[BootResult], None]) -> None:
        """`MdWriterService.request_trial`: run a trial bootstrap in the writer task, then call `done`."""
        ...


class TrialBootstrapProbe:
    """The ARM-global probe's trial bootstrap (§A7, R11-M09) through the `md-writer` task: it runs the bootstrap
    steps under the writer token and `md:boot_lock` and writes `md:boot` (an empty store included); it never counts
    toward the reset budget and sends no MD_RESET notice (`src.marketdata.bootstrap`)."""

    def __init__(self, port: TrialPort) -> None:
        self._port = port

    async def trial(self) -> ProbeResult:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def done(result: BootResult) -> None:
            if not fut.done():          # the probe may have timed out meanwhile
                fut.set_result(result)

        self._port.request_trial(done)
        result = await fut
        if isinstance(result, BootResult) and result.ok:
            return PASSED
        return refused_probe(TRIAL_BOOTSTRAP, "md_writer_reset")


class IndicatorView(Protocol):
    def status(self, indicator_id: str) -> IndicatorStatus | None:
        """`IndicatorEngine.status` (md-indicators): states, buckets and counts only."""
        ...


class IndicatorStateProbe:
    """`indicator_rebuild`: every indicator on the series is out of FAULTED (only `md_rebuild` clears FAULTED) and
    warm. `indicators_of(series)` lists the indicator ids reading the series (the demand book, S31)."""

    def __init__(self, view: IndicatorView, indicators_of: Callable[[str], Sequence[str]]) -> None:
        self._view = view
        self._indicators_of = indicators_of

    async def rebuilt_and_warm(self, series: str) -> ProbeResult:
        ids = sorted(set(self._indicators_of(series)))
        if not ids:
            return refused_probe(INDICATOR_UNKNOWN)
        for iid in ids:
            st = self._view.status(iid)
            if st is None:
                return refused_probe(INDICATOR_NOT_TRACKED)
            if st.state is IndState.FAULTED:
                return refused_probe(INDICATOR_FAULTED_CHECK, "md_rebuild")
            if not st.warm:
                return refused_probe(INDICATOR_NOT_WARM)
        return PASSED


class SeriesBarsProbe(Protocol):
    """`MdBars` (S38e): the clear-probe reads of one BROKER series; None passed, else a check code."""

    async def probe_reread(self, series: SeriesId) -> str | None: ...

    async def probe_fresh_bar(self, series: SeriesId) -> str | None: ...


def _broker_series(scope_id: str) -> SeriesId | None:
    try:
        series = SeriesId.parse(scope_id)
    except ValueError:
        return None
    return series if series.source is Source.BROKER and series.canonical == scope_id else None


def _code_result(code: str | None) -> ProbeResult:
    if code is None:
        return PASSED
    return refused_probe(code if CHECK_PATTERN.fullmatch(code) else PROBE_ERROR)


class MdBarsSeriesProbe:
    """`SeriesProbePort` over md-bars (plan S38e): `series_reread` re-reads the rollback set (hard checks and the tick
    cross-check where tick bars exist), `fresh_bar` waits for one fresh bar to reach finality within `post_grace`.
    The scope id is the series' canonical id; anything else refuses `series_unknown`."""

    def __init__(self, bars: SeriesBarsProbe) -> None:
        self._bars = bars

    async def reread(self, series: str) -> ProbeResult:
        sid = _broker_series(series)
        if sid is None:
            return refused_probe(SERIES_UNKNOWN)
        return _code_result(await self._bars.probe_reread(sid))

    async def fresh_bar(self, series: str) -> ProbeResult:
        sid = _broker_series(series)
        if sid is None:
            return refused_probe(SERIES_UNKNOWN)
        return _code_result(await self._bars.probe_fresh_bar(sid))


def _version_key(scope_id: str) -> tuple[str, str, int] | None:
    """`(strategy | guard, KIND, VERSION)` of a version scope id, or None."""
    head, _, rest = scope_id.partition(":")
    kind, _, version = rest.rpartition(":")
    if head not in ("strategy", "guard") or not kind or not version.isdigit():
        return None
    return head, kind, int(version)


def conformance_problems(registry: ArmRegistry, scope_id: str) -> list[str] | None:
    """The registry's declaration checks of the class a version scope pins (S38e): REASONS mapped, DECISION_METRICS
    typed and RELEASE_POLICY typed for a strategy; REASONS mapped and EVENTS holding POST for a guard. None when the
    scope names no registered class. (The full conformance suite, `tests/riskmgr/conformance.py`, gates every
    registered version at build time; this is its runtime part.)"""
    key = _version_key(scope_id)
    if key is None:
        return None
    head, kind, version = key
    owner = f"{head} {kind} v{version}"
    if head == "strategy":
        strategy = registry.strategy(kind, version)
        if strategy is None:
            return None
        problems = reason_map_problems(owner, strategy.REASONS) + metric_problems(owner, strategy.DECISION_METRICS)
        if not isinstance(getattr(strategy, "RELEASE_POLICY", None), ReleasePolicy):
            problems.append(f"{owner}: RELEASE_POLICY")
        return problems
    guard = registry.guard(kind, version)
    if guard is None:
        return None
    problems = reason_map_problems(owner, guard.REASONS)
    if ArmEvent.POST not in guard.EVENTS:
        problems.append(f"{owner}: EVENTS")
    return problems


class VersionConformanceProbe:
    """`VersionProbePort` (plan S38e, S39b): the pinned class is registered, passes the declaration checks and runs
    its own code against its conformance vectors (`src.riskmgr.vectors.vector_problems`: `parse_params`, `propose` /
    `at_target`, a guard's `applies` / `evaluate`), all computed as **one job on the version's lane** through the job
    runner under its cap and the version's pinned Decimal context. A hung lane, a job over the cap (`job_timeout`), a
    raise (`job_raised`) or a wrong answer refuses, so a version FAILED for `job_raised` or `job_timeout` is not
    cleared while it still misbehaves (go-live gate `conformance_probe_partial`, wired). The probe's job is never a
    position's."""

    def __init__(self, registry: ArmRegistry, jobs: JobRunner) -> None:
        self._registry = registry
        self._jobs = jobs

    async def probe(self, scope_id: str) -> ProbeResult:
        key = _version_key(scope_id)
        if key is None or conformance_problems(self._registry, scope_id) is None:
            return refused_probe(VERSION_UNKNOWN, "arm_repin")
        if self._jobs.hung(scope_id):
            return refused_probe(JOB_LANE_HUNG)
        head, kind, version = key
        cls: object = (self._registry.strategy(kind, version) if head == "strategy"
                       else self._registry.guard(kind, version))
        context = getattr(cls, "DECIMAL_CONTEXT", None)
        registry = self._registry

        def job() -> list[str]:
            return (conformance_problems(registry, scope_id) or []) + vector_problems(registry, head, kind, version)

        result = await self._jobs.run(JobSpec(scope_id, decimal_context=context), PROBE_POSITION, job)
        if not result.ok:
            return _code_result(result.reason or PROBE_ERROR)
        if result.value:
            return refused_probe(CONFORMANCE)
        return PASSED


# ---------------------------------------------------------------------------------------------------- after a clear
class ClearHooks(Protocol):
    def cleared(self, scope: ScopeRef) -> None:
        """An accepted clear of `scope` committed: the market-data and job-runner side effects (no I/O here)."""
        ...


@dataclass(frozen=True, kw_only=True)
class ScopeClearHooks:
    """The S31 wiring of `ClearHooks`: SERIES → `CrossChecker.clear(series)` and SM12 `FAILED → BACKFILLING`
    (`series`); ADAPTER → `adapter`; STRATEGY_VERSION → `JobRunner.reset(scope_id)` (`version`); ARM → the
    post-clear bootstrap (`MdWriterService.request_post_clear`, which also resets the reset budget and its one-hour
    window; every series then restarts in BACKFILLING)."""

    series: Callable[[str], None] | None = None
    adapter: Callable[[str], None] | None = None
    version: Callable[[str], None] | None = None
    post_clear: Callable[[], None] | None = None

    def cleared(self, scope: ScopeRef) -> None:
        if scope.kind is ScopeKind.SERIES and self.series is not None:
            self.series(scope.scope_id)
        elif scope.kind is ScopeKind.ADAPTER and self.adapter is not None:
            self.adapter(scope.scope_id)
        elif scope.kind is ScopeKind.STRATEGY_VERSION and self.version is not None:
            self.version(scope.scope_id)
        elif scope.kind is ScopeKind.ARM and self.post_clear is not None:
            self.post_clear()


__all__ = [
    "ADAPTER_SERIES", "ARM_GLOBAL", "ARM_POOL", "CHECK_PATTERN", "CIRCUIT_OPEN", "CircuitView", "ClearHooks",
    "CONFORMANCE", "ClearProbes", "FAULT_DURING_PROBE", "JOB_LANE_HUNG", "MdBarsSeriesProbe", "SERIES_UNKNOWN",
    "SeriesBarsProbe", "VERSION_UNKNOWN", "VersionConformanceProbe", "conformance_problems", "FIRST_COMMANDS", "FRESH_BAR","INDICATOR_FAULTED_CHECK", "INDICATOR_NOT_TRACKED",
    "INDICATOR_NOT_WARM", "INDICATOR_REBUILD", "INDICATOR_UNKNOWN", "IndicatorProbePort", "IndicatorStateProbe",
    "IndicatorView", "PASSED", "PROBE_ERROR", "PROBE_KINDS", "PROBE_NOT_WIRED", "PROBE_TIMEOUT", "ProbeResult",
    "SERIES_REREAD", "SPAN_WRITE", "STRATEGY_CONFORMANCE", "ScopeClearHooks", "SeriesProbePort", "SpanProbePort",
    "TRIAL_BOOTSTRAP", "TrialBootstrapProbe", "TrialPort", "TrialProbePort", "VersionProbePort", "probe_note",
    "refusal_reason", "refused_probe",
]
