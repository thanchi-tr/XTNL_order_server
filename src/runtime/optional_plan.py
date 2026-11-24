"""The production plan of the optional services: task wiring, DRAINING, request handlers, metrics, the dead-man
fields and the `arm_status` writer (design §A2, §A7, §B5.1, §B13, §B14, §C2, PR4, PR13; plan S32, RT-2).

`production_plan(environ)` is the `OptionalPlan` `build_engine` hands to the `optional-bootstrap` task
(`src/runtime/optional.py`), built and imported only with `XTNL_MD_ENABLED=1`. It turns what the bootstrap built for
the term into supervised tasks and non-blocking request handlers:

* **Market data** (`XTNL_MD_ENABLED=1`): `md-writer` (`MdWriterService.run`, stamp 5 s: the writer cycle, the
  bootstraps and the work items), `md-bars` (30 s), `md-indicators` (10 s; its circuit's quarantine hook is
  `MdIndicators.on_quarantine`) and `arm-status` (5 s: this node's §A7 market-data record every 5 s, the S16
  obligation, and the `arm_status` heartbeat row once the ARM runs). Handlers: `md_backfill` -> `md-bars`,
  `md_rebuild` -> `md-indicators`, `md_writer_reset` / `md_rehash` -> `md-writer`; each only enqueues and answers
  `Accepted(owner)` (or refuses at once with a code); the owner answers through `EngineSession.complete_request`.
* **ARM** (`XTNL_ARM_ENABLED=1`, after the bootstrap's ARM step): the S30 error budget (bound to the market-data
  `FaultRelay` and to every optional task's `on_circuit_reopen`), the `FailedManager` (record, ping, re-ping, spool,
  clear) driven by the `arm` task (5 s), and the `arm-relay` task (60 s, self-stamping) over the engine pool's ARM
  outbox store with the `XTNL_ARM_` channels and the send-time renderer over the market-data read port. Handler:
  `arm_clear_failed` -> the FAILED probe runner (`FailedManager.submit_clear`, answered by the `arm` task). The
  market-data layer's ARM-global FAILED flag (`md-bars` and `md-indicators` stop writing, except the `md_backfill` /
  `md_rebuild` repairs, and the read port stops serving; §B13, §B14) is bound to `FailedManager.summary()`'s
  `global_failed` (the ARM scope FAILED or the open episodes unreadable, as the dead-man and the metric report it).
  `redis-md` down time counts toward `fail_md_down_s` only while an armed position's market is OPEN.
* **ARM configuration** (S33): the `arm-config` task (120 s) runs the `ConfigInstaller`
  (`src/armconfig/installer.py`) over the Supabase reader (`src/armconfig/client.py`, `XTNL_ARM_CONFIG_DSN` and the
  pinned CA `XTNL_ARM_CONFIG_CA_FILE`) and the ARM pool's `PgArmConfigStore`; the known ids are this engine's
  adapters, their loaded instruments and accounts, and the routes `adapter:account` plus `*`. Without a DSN (or with
  a bad setting) the ARM configuration is idle: one page, no task, nothing accepted. The installer is the
  arm-tracker's `AcceptedConfigPort` (`OptionalRuntime.accepted_config()`); its pause, installed hash and counters
  go into the `arm_status` row.
* **Operator controls** (S34): the `arm-status` loop reads `arm_controls` (written only by the ops CLI) on the ARM
  pool every 5 s into `ControlsView`: its operator pause is ORed into the `arm_status` row, and its
  `--drain-notices` flag (only while paused) is the arm-relay's `drain_notices`. Unreadable controls are never a
  pause (the last read is kept).
* **The decision path** (S36b, RT-3): the `arm-tracker` task (10 s; `src/riskmgr/tracker.py` over the engine pool's
  `PgTrackerSnapshot`, the SM10 `CalendarGrid`, the installer as `AcceptedConfigPort`) with the handlers
  `arm_disarm` / `arm_repin` / `arm_rearm` (enqueued, answered by the tracker); the BarClock inside the `arm` loop
  (every 250 ms, once the open FAILED episodes are read and while no ARM-global FAILED holds) over the S21 join
  (`src/riskmgr/evaluation.py`, `PipelineEvaluator`), with the installer's `pause_reason` as its config pause; the
  notices (`ArmNotices`): the notice sink in the sink chain (`LateNoticeSink`, bound when the graph is built), the
  timer pass every 5 s in the `arm` loop (suppressed while paused), `FAILED_CLEARED`, the tracker's extras and the
  TEST notice of `arm notify-test`. The job runner's STRATEGY_VERSION signals and the stuck-frontier views feed the
  error budget; `armed_adapter_ids()`, `arm_overdue_positions`, the PR9 `stop_read_cost` and the `arm_status`
  counts come from the tracker. With the ARM off and market data on, the `arm-status` loop still writes the
  heartbeat row (engine pool), so the `md ...` requests are answerable.

**Round 14 (plan S38g):** liveness: the `arm-status` loop runs `src.riskmgr.liveness` every 5 s (independent of the
`arm` loop): armed positions not decided for more than 2 OPEN bars (the dead-man's `arm_overdue_positions`, the
`arm_status` `overdue_positions` and the `overdue` metric view; the tracker's old count of assigned but not armed
positions is now the `unarmed` view and counter), a stalled BarClock or arm-tracker pass and the arm-relay backlog,
each paged once per episode. Sleep: the BarClock and `md-writer` detect a suspend themselves (`SUSPEND_AFTER_MS`) and
also take the uptime tracker's signal (`EngineSession.add_suspend_listener`: the BarClock's one-shot latch, the
writer's `note_suspended`); the unclean-restart counter leaves out a PC sleep's own restart
(`src.runtime.sleeplog.SleepAwareRestarts` over the node's sleep-fence record), and each sleep restart is paged once
when the ARM starts.

Every task watches `EngineSession.stop_event` and starts no new write after it is set; a loop that returns while the
term runs is a death (restart with backoff), one that returns in DRAINING waits to be cancelled. The `redis-md`
clients and the ARM pool are closed in parallel by `aclose` (their closers were registered by the bootstrap) and
terminated when they hang. Nothing here touches ingestion, Oversee or the gate.

Logs, pages, metric labels and request answers carry codes, names, ids and counts only: never a price, a level, an
account value or a credential.
"""
from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from typing import TYPE_CHECKING, Any, Final

from src.db.arm_budget_store import PgArmBudgetStore, PgRestartHistory
from src.db.arm_config_store import PgArmConfigStore
from src.db.arm_failures_store import PgArmFailedClear, PgArmFailuresStore
from src.db.arm_outbox_store import PgArmOutboxInserter, PgArmOutboxStore
from src.db.instrument_reads import PgPointSizes
from src.db.arm_ports import ArmConfigStore, ArmPositionRow, ArmStatusRow, ArmStatusWriter, ControlsRow, is_lost_fence
from src.db.arm_status_store import PgArmStatusStore
from src.db.arm_store import PgArmStateStore
from src.db.arm_versions import PgArmVersionReader
from src.db.epoch import EngineDb
from src.marketdata.bootstrap import ServiceCounters, Sm12Event, WorkItem, WorkKind
from src.marketdata.brokerbars import BarsCounters, Coverage, MdBars
from src.marketdata.crosscheck import CrossCheckCounters
from src.marketdata.indicators.engine import IndicatorCounters, IndState
from src.marketdata.ports import StampPort
from src.marketdata.ticks import HookCounters
from src.marketdata.types import SeriesId, Sm12State
from src.marketdata.writer import WriterCounters
from src.notifications.arm_relay import ArmRelay, SendTimeRenderer
from src.notifications.factory import ARM_CHANNEL_NAMES, ChannelSet, build_arm_channels
from src.notifications.rotation import RotatingChannels, settings_from_secrets
from src.notifications.settings import ARM_PREFIX, NotificationSettings
from src.observability.metrics import Sample, gauge
from src.marketdata.clockstep import SUSPEND_AFTER_MS
from src.riskmgr.barclock import BarClock, BarClockCounters, BarClockSettings
from src.riskmgr.budget import CIRCUIT_TASKS, BudgetCounters, ErrorBudget
from src.riskmgr.evaluation import EvalCounters, PipelineEvaluator
from src.riskmgr.failed import FailedCounters, FailedManager, FailedStores
from src.riskmgr.golive import bar_capable_adapters as golive_bar_capable_adapters
from src.riskmgr.jobs import JobRunner
from src.riskmgr.liveness import LivenessWatch, overdue_positions
from src.riskmgr.notices import AcceptedPolicies, ArmNotices, NoticeCounters, RouteBook
from src.riskmgr.probes import (
    PROBE_NOT_WIRED,
    ClearProbes,
    IndicatorStateProbe,
    MdBarsSeriesProbe,
    ProbeResult,
    ScopeClearHooks,
    TrialBootstrapProbe,
    VersionConformanceProbe,
    refused_probe,
)
from src.riskmgr.registry import REGISTRY as ARM_REGISTRY
from src.riskmgr.sinks import DecisionSink, SinkChain
from src.riskmgr.types import indicator_of_scope
from src.riskmgr.stopreads import StopReadCounters, StopReads
from src.riskmgr.tracker import REQUEST_KINDS as TRACKER_KINDS
from src.riskmgr.tracker import AnswerStatus, ArmTracker, RequestAnswer, TrackerCounters, TrackerRequest, TrackerSettings
from src.runtime.arm_ports import (
    CalendarGrid,
    LateNoticeSink,
    OrchestratorExecutionTimes,
    PgTrackerSnapshot,
    TrackerBook,
    stop_read_cost,
)
from src.runtime.deadman import ArmFailedBeat
from src.runtime.md_ports import StampPortImpl
from src.runtime.optional import OptionalPlan, OptionalServices
from src.runtime.optional_settings import resolved_environ
from src.runtime.ports import OperatorRequest
from src.runtime.request_handlers import Accepted, Applied, HandlerResult, Moot, Refused, RequestHandler, RequestResult
from src.runtime.session import EngineSession
from src.runtime.sleeplog import SleepAwareRestarts
from src.runtime.supervisor import TaskContext, TaskSpec
from src.secrets.source import SecretsSource

if TYPE_CHECKING:
    import httpx

    from src.armconfig.client import ConfigSource
    from src.armconfig.installer import ConfigInstaller
    from src.armconfig.validate import KnownIds, ValidDocument
    from src.runtime.optional import ArmServices

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------------------------------- task names
MD_WRITER_TASK: Final = "md-writer"
MD_BARS_TASK: Final = "md-bars"
MD_INDICATORS_TASK: Final = "md-indicators"
ARM_STATUS_TASK: Final = "arm-status"
ARM_TASK: Final = "arm"
ARM_RELAY_TASK: Final = "arm-relay"
ARM_CONFIG_TASK: Final = "arm-config"
ARM_TRACKER_TASK: Final = "arm-tracker"
#: Maximum stamp age per task (plan S32, S33, S36b; design §B5.1).
STAMP_S: Final[Mapping[str, float]] = {
    MD_WRITER_TASK: 5.0, MD_BARS_TASK: 30.0, MD_INDICATORS_TASK: 10.0, ARM_STATUS_TASK: 5.0, ARM_TASK: 5.0,
    ARM_RELAY_TASK: 60.0, ARM_CONFIG_TASK: 120.0, ARM_TRACKER_TASK: 10.0,
}
GLOBAL_ROUTE: Final = "*"
STATUS_EVERY_S: Final = 5.0
STAMP_SLICE_S: Final = 1.0
STATUS_WRITE_S: Final = 3.0
ARM_TICK_S: Final = 1.0
POOL_CHECK_S: Final = 3.0
#: `ControlsView.drain()` holds only while the last controls read is at most this old (three status periods).
DRAIN_STALE_S: Final = 3 * STATUS_EVERY_S
DRAIN_FAILED_S: Final = 2.0
MD_KINDS: Final = frozenset({"md_backfill", "md_rebuild", "md_writer_reset", "md_rehash"})
ARM_KINDS: Final = frozenset({"arm_clear_failed", *TRACKER_KINDS})
NOTICES_EVERY_S: Final = 5.0
"""The notices' timer pass (`ArmNotices.step`) inside the `arm` loop."""
NOTIFY_TEST_WINDOW_MS: Final = 3_600_000
"""An `arm notify-test` request older than this when first seen (an earlier term's) sends nothing."""
MIN_SLEEP_S: Final = 0.01
PAYLOAD_VERSION: Final = 1

Loop = Callable[[asyncio.Event, StampPort], Awaitable[None]]


# ---------------------------------------------------------------------------------------------------- tasks
def loop_spec(session: EngineSession, name: str, body: Loop, *, on_quarantine: Callable[[], None] | None = None,
              on_circuit_reopen: Callable[[str], None] | None = None) -> TaskSpec:
    """A supervised optional task around a component loop `body(stop, stamp)`: non-critical, stamped by the loop
    itself against `STAMP_S[name]`. The loop watches the term's stop flag; a return outside DRAINING is a death, a
    return in DRAINING waits (exempt) to be cancelled by the supervisor."""

    async def factory(ctx: TaskContext) -> None:
        stop = session.stop_event
        await body(stop, StampPortImpl(ctx))
        if not stop.is_set():
            raise RuntimeError(f"{name} loop returned")
        ctx.set_exempt("draining")
        await asyncio.Event().wait()

    return TaskSpec(name, factory, max_stamp_age_s=STAMP_S[name], critical=False, on_quarantine=on_quarantine,
                    on_circuit_reopen=on_circuit_reopen)


async def _stop_or(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def _stop_stamped(stop: asyncio.Event, seconds: float, stamp: StampPort) -> None:
    """Wait `seconds` (or until the stop) in `STAMP_SLICE_S` slices, stamping after each, so a wait as long as the
    task's maximum stamp age never reads stale under event-loop lag."""
    deadline = time.monotonic() + seconds
    while not stop.is_set():
        left = deadline - time.monotonic()
        if left <= 0:
            return
        await _stop_or(stop, min(STAMP_SLICE_S, left))
        stamp.stamp()


# ---------------------------------------------------------------------------------------------------- circuits

def _no_trade(bars: MdBars, series: SeriesId, bucket_start_ms: int) -> bool:
    """`md-bars` resolved the bucket `no_trade` (its in-memory coverage map): the evaluator's `NO_BROKER_BAR` input
    (S38c). An unknown bucket (not tracked, blind, after a restart) is not `no_trade` (fail safe: the decision waits)."""
    return bars.coverage_of(series, bucket_start_ms) is Coverage.NO_TRADE

class CircuitRelay:
    """Every optional task's `on_circuit_reopen`: an ARM task circuit that opens again after its half-open retry is
    an ARM-global fault (§B14), filed into the error budget once the ARM graph binds it; before that (or with the
    ARM off) it is counted and logged only."""

    def __init__(self, clock_ms: Callable[[], int]) -> None:
        self._clock_ms = clock_ms
        self._budget: ErrorBudget | None = None
        self.reopens: dict[str, int] = {}

    def bind(self, budget: ErrorBudget | None) -> None:
        self._budget = budget

    def reopen(self, task: str) -> None:
        self.reopens[task] = self.reopens.get(task, 0) + 1
        budget = self._budget
        if budget is None:
            log.warning("task circuit of %s opened again (no ARM error budget bound)", task)
            return
        budget.on_circuit_reopen(task, self._clock_ms())


# ---------------------------------------------------------------------------------------------------- controls
class ControlsView:
    """The operator controls (`arm_controls`, written only by the ops CLI: `arm pause [--drain-notices]`, `arm
    resume`, `arm notify-test`; plan S34) as last read on the ARM pool by the `arm-status` loop (every 5 s).

    * `pause_reason()`: the operator pause (`operator`) while `paused`, ORed into the `arm_status` row with the
      installer's `config_hash` pause (S33 hand-off).
    * `drain()`: the arm-relay's `drain_notices` flag, only while paused (`arm resume` ends both).
    * **Unreadable controls are not a pause** (§B13): a failed read keeps the last row read and is counted; before any
      read nothing is paused or drained. The BarClock reads the controls on its own (S21 join) and holds its event
      while they are unreadable.
    * **A stale drain is no drain:** draining is destructive (a notice finished `suppressed` is never sent), so
      `drain()` holds only while the last successful read is at most `DRAIN_STALE_S` old (three refresh periods);
      after that the queued notices wait instead of being suppressed on a row the operator may have changed."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.row: ControlsRow | None = None
        self.reads = 0
        self.read_errors = 0
        self._clock = clock
        self._read_at: float | None = None

    async def refresh(self, store: ArmConfigStore, timeout_s: float) -> None:
        try:
            async with asyncio.timeout(timeout_s):
                row = await store.controls()
        except Exception as exc:
            if is_lost_fence(exc):
                raise
            self.read_errors += 1
            log.info("arm_controls not read (%s); the last read is kept", type(exc).__name__)
            return
        self.reads += 1
        if row is None:
            self.read_errors += 1          # the singleton row is missing: unreadable, never a pause
            return
        self.row = row
        self._read_at = self._clock()

    def pause_reason(self) -> str | None:
        row = self.row
        if row is None or not row.paused:
            return None
        return row.pause_reason or OPERATOR_PAUSE

    def drain(self) -> bool:
        row, read_at = self.row, self._read_at
        if row is None or read_at is None or not (row.paused and row.drain_notices):
            return False
        return self._clock() - read_at <= DRAIN_STALE_S


#: The `pause_reason` of an operator pause whose row carries none (the ops CLI writes `operator`).
OPERATOR_PAUSE: Final = "operator"
#: The pause reason when the installer's pause port raises (fail closed: held as paused).
CONFIG_PAUSE_UNREADABLE: Final = "config_unreadable"


# ---------------------------------------------------------------------------------------------------- answers
def request_result(answer: RequestAnswer) -> RequestResult:
    """An ARM task's `RequestAnswer` as the session's stored result."""
    if answer.status is AnswerStatus.APPLIED:
        return Applied()
    if answer.status is AnswerStatus.MOOT:
        return Moot(answer.reason)
    return Refused(answer.reason)


class SessionAnswers:
    """The ARM's `RequestAnswerPort` over `EngineSession.complete_request` (no I/O)."""

    def __init__(self, session: EngineSession) -> None:
        self._session = session

    def complete(self, request_id: int, answer: RequestAnswer) -> None:
        if not self._session.complete_request(request_id, request_result(answer)):
            log.info("request %d answered after it left this session", request_id)


# ---------------------------------------------------------------------------------------------------- ports
class _SessionClock:
    """The riskmgr `Clock` over the session's wall clock."""

    __slots__ = ("_clock_ms",)

    def __init__(self, clock_ms: Callable[[], int]) -> None:
        self._clock_ms = clock_ms

    def now_ms(self) -> int:
        return self._clock_ms()

    def monotonic_ms(self) -> int:
        return time.monotonic_ns() // 1_000_000


class _Maintenance:
    """The budget's `MaintenancePort` over SM10's maintenance overlay."""

    def __init__(self, session: EngineSession) -> None:
        self._session = session

    def in_maintenance(self, adapter_id: str, now_ms: int) -> bool:
        try:
            return self._session.calendar.in_maintenance(adapter_id)
        except Exception:
            return False


class _Circuits:
    """The ARM-global clear probe's `CircuitView`: the ARM task circuits open now."""

    def __init__(self, session: EngineSession) -> None:
        self._session = session

    def open_circuits(self) -> Sequence[str]:
        return [n for n in self._session.degraded_tasks() if n in CIRCUIT_TASKS]


class LateSpanProbe:
    """The `span_write` clear probe's `SpanProbePort` (S38c review): the BarClock's own rolled-back span write
    (`BarClock.test_span_write`, on the ARM pool, fenced). The FAILED manager is built before the BarClock, so the
    clock is bound once the graph is built; until then the probe refuses `probe_not_wired` (fail closed)."""

    def __init__(self) -> None:
        self._clock: BarClock | None = None

    def bind(self, clock: BarClock) -> None:
        self._clock = clock

    async def test_span_write(self, position_ids: Sequence[str]) -> ProbeResult:
        clock = self._clock
        if clock is None:
            return refused_probe(PROBE_NOT_WIRED)
        return await clock.test_span_write(position_ids)


# ---------------------------------------------------------------------------------------------------- request parsing
class PayloadError(ValueError):
    """A request payload that does not match its kind (answered `refused:bad_payload`)."""


def _payload(raw: object) -> Mapping[str, Any]:
    if isinstance(raw, (str, bytes)):
        try:
            raw = json.loads(raw)
        except ValueError as exc:
            raise PayloadError("payload is not JSON") from exc
    if not isinstance(raw, Mapping):
        raise PayloadError("payload is not an object")
    v = raw.get("v")
    if type(v) is not int or v != PAYLOAD_VERSION:      # present, an int (never a bool) and the known version
        raise PayloadError("missing or unknown payload version")
    return raw


def _ms(p: Mapping[str, Any], key: str) -> int:
    value = p.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PayloadError(f"{key} is a non-negative integer")
    return value


def parse_backfill(target: str, payload: object) -> tuple[SeriesId, int, int]:
    """`md_backfill`: target = a series id (`adapter:symbol:spec:SIDE:SOURCE`); payload `{"v": 1, "from_bucket_ms":
    int, "to_bucket_ms": int}` (bucket starts on the series' grid; ids and times only)."""
    try:
        series = SeriesId.parse(target)
    except ValueError as exc:
        raise PayloadError("bad series id") from exc
    p = _payload(payload)
    return series, _ms(p, "from_bucket_ms"), _ms(p, "to_bucket_ms")


def parse_tracker_request(req: OperatorRequest) -> TrackerRequest:
    """`arm_disarm` / `arm_rearm` (payload `{}`) and `arm_repin` (payload `{}`: the assignment in force; or
    `{"v": 1, "strategy_code": text, "strategy_version": int}`); target `adapter_id:position_id` (S34)."""
    raw: object = req.payload
    if raw is None or raw == "":
        raw = {}
    if isinstance(raw, (str, bytes)):
        try:
            raw = json.loads(raw)
        except ValueError as exc:
            raise PayloadError("payload is not JSON") from exc
    if not isinstance(raw, Mapping):
        raise PayloadError("payload is not an object")
    code: str | None = None
    version: int | None = None
    if raw:
        p = _payload(raw)
        if req.kind != "arm_repin" or set(p) != {"v", "strategy_code", "strategy_version"}:
            raise PayloadError("unexpected payload keys")
        code_raw, version_raw = p["strategy_code"], p["strategy_version"]
        if not isinstance(code_raw, str) or isinstance(version_raw, bool) or not isinstance(version_raw, int):
            raise PayloadError("strategy_code is text and strategy_version an integer")
        code, version = code_raw, version_raw
    try:
        return TrackerRequest.of(req.request_id, req.kind, req.target, strategy_code=code, strategy_version=version)
    except ValueError as exc:
        raise PayloadError("bad target") from exc


class _NoConfig:
    """The arm-tracker's `AcceptedConfigPort` while no configuration source is configured: nothing is accepted."""

    def accepted(self) -> ValidDocument | None:
        return None


class _GraphFailedView:
    """The FAILED manager's late-bound ports into the graph: the tracker's series book (`PositionSeriesPort`) and the
    FAILED_CLEARED notice (`FailedClearedPort`); both are built after the manager."""

    def __init__(self, rt: OptionalRuntime) -> None:
        self._rt = rt

    def series_of(self, row: ArmPositionRow) -> Sequence[str] | None:
        graph = self._rt.graph
        return None if graph is None or graph.tracker is None else graph.tracker.series_of(row)

    def failed_cleared(self, failure_id: str, scope_kind: str, scope_id: str, at_ms: int) -> None:
        graph = self._rt.graph
        if graph is not None and graph.notices is not None:
            graph.notices.failed_cleared(failure_id, scope_kind, scope_id, at_ms)


# ---------------------------------------------------------------------------------------------------- the ARM graph
@dataclass
class ArmGraph:
    """The ARM objects of one term (S32; the decision path joined in S36b)."""

    arm: ArmServices
    engine_db: EngineDb
    budget: ErrorBudget
    failed: FailedManager
    relay: ArmRelay
    controls_store: ArmConfigStore | None = None
    """The ARM pool's `arm_controls` reader (the `arm-status` loop refreshes `OptionalRuntime.controls`)."""
    config: ConfigInstaller | None = None
    """The `arm-config` installer; None while the configuration source is not configured (idle)."""
    jobs: JobRunner | None = None
    notices: ArmNotices | None = None
    tracker: ArmTracker | None = None
    clock: BarClock | None = None
    evaluator: PipelineEvaluator | None = None
    grid: CalendarGrid | None = None
    started: bool = False
    tick_errors: int = 0
    pool_ok: bool | None = None
    flush_errors_seen: int = 0
    decide_holds: int = 0
    notice_errors: int = 0
    notify_test_seen: int | None = None


class SuspendLatch:
    """The uptime tracker's suspend signal for the BarClock (round 14): set by the session's suspend listener on the
    loop, taken (and cleared) by the next BarClock pass, which then holds once."""

    def __init__(self) -> None:
        self._set = False
        self.notes = 0

    def note(self) -> None:
        self._set = True
        self.notes += 1

    def take(self) -> bool:
        was, self._set = self._set, False
        return was


# ---------------------------------------------------------------------------------------------------- per-term state
class OptionalRuntime:
    """The plan's state for one term (`OptionalServices.runtime`) and the session's `OptionalView`."""

    def __init__(self, svc: OptionalServices, plan: ProductionPlan) -> None:
        self.svc = svc
        self.session = svc.session
        self.plan = plan
        self.circuits = CircuitRelay(self.session.ctx.clock_ms)
        self.graph: ArmGraph | None = None
        self.controls = ControlsView()
        self.config_idle_reason: str | None = None
        self.record_saves = 0
        self.status_writes = 0
        self.status_errors = 0
        self.late_notices: LateNoticeSink | None = None
        self._md_status: ArmStatusWriter | None = None
        self._md_status_checked = False
        self.suspend_latch = SuspendLatch()
        self.liveness = LivenessWatch(svc.pager)
        self.liveness_errors = 0
        self.sleep_restarts_reported = 0
        add_listener = getattr(self.session, "add_suspend_listener", None)
        if add_listener is not None:
            add_listener(self._on_suspended)
        self.session.set_optional_view(self)

    def _on_suspended(self, suspended_ms: int | None) -> None:
        """The uptime tracker saw a suspend (round 14): the BarClock holds its next pass, every strategy job running
        now is told it spanned a sleep (S39b, never a timeout or a fault) and md-writer blinds the TICK buckets the
        suspend covered. Synchronous, no I/O."""
        self.suspend_latch.note()
        graph = self.graph
        if graph is not None and graph.jobs is not None:    # S39b: a job running now spanned the sleep
            graph.jobs.note_suspended(suspended_ms)
        writer = getattr(self.svc.md, "writer", None)
        if writer is not None:
            writer.note_suspended(suspended_ms or 0)

    # ------------------------------------------------------------------------------------------------ md tasks
    def md_tasks(self) -> list[TaskSpec]:
        md = self.svc.md
        s = self.session
        reopen = self.circuits.reopen
        return [
            loop_spec(s, MD_WRITER_TASK, md.service.run, on_circuit_reopen=reopen),
            loop_spec(s, MD_BARS_TASK, md.bars.run, on_circuit_reopen=reopen),
            loop_spec(s, MD_INDICATORS_TASK, self._indicators_loop, on_quarantine=md.indicators.on_quarantine,
                      on_circuit_reopen=reopen),
            loop_spec(s, ARM_STATUS_TASK, self._status_loop),
        ]

    async def _indicators_loop(self, stop: asyncio.Event, stamp: StampPort) -> None:
        """`md-indicators` on its own bounded executor (round 13); at the term's stop the executor is closed and a
        worker thread still hung ends the process through the hard exit (the S22 clean-stop rule)."""
        indicators = self.svc.md.indicators
        try:
            await indicators.run(stop, stamp)
        finally:
            if stop.is_set():
                self._close_indicators()

    def _close_indicators(self) -> None:
        try:
            hung = self.svc.md.indicators.close()
        except Exception as exc:
            log.warning("md-indicators: executor not closed (%s)", type(exc).__name__)
            return
        if hung:
            log.critical("md-indicators: a worker thread is still hung at stop; ending the process")
            self.session.ctx.exit_fn("md-indicators thread hung")

    def md_handlers(self) -> dict[str, RequestHandler]:
        return {"md_backfill": self._md_backfill, "md_rebuild": self._md_rebuild,
                "md_writer_reset": self._md_writer_reset, "md_rehash": self._md_rehash}

    def _md_backfill(self, req: OperatorRequest) -> HandlerResult:
        try:
            series, from_ms, to_ms = parse_backfill(req.target, req.payload)
        except PayloadError:
            return Refused("bad_payload")
        refusal = self.svc.md.bars.request_backfill(req.request_id, series, from_ms, to_ms)
        return Accepted(MD_BARS_TASK) if refusal is None else Refused(refusal)

    def _md_rebuild(self, req: OperatorRequest) -> HandlerResult:
        target = req.target.strip()
        if not target or target != req.target:
            return Refused("bad_target")
        refusal = self.svc.md.indicators.md_rebuild(req.request_id, target)
        return Accepted(MD_INDICATORS_TASK) if refusal is None else Refused(refusal)

    def _md_work(self, req: OperatorRequest, kind: WorkKind) -> HandlerResult:
        if not self.svc.md.service.submit(WorkItem(kind, req.request_id)):
            return Refused("queue_full")
        return Accepted(MD_WRITER_TASK)

    def _md_writer_reset(self, req: OperatorRequest) -> HandlerResult:
        return self._md_work(req, WorkKind.MD_WRITER_RESET)

    def _md_rehash(self, req: OperatorRequest) -> HandlerResult:
        return self._md_work(req, WorkKind.MD_REHASH)

    # ------------------------------------------------------------------------------------------------ arm-status
    async def _status_loop(self, stop: asyncio.Event, stamp: StampPort) -> None:
        """Every 5 s: this node's market-data record (§A7; the bootstrap's own bounded save), then the `arm_status`
        heartbeat row (unfenced telemetry): on the ARM pool once the ARM runs, else on the engine pool whenever the
        market-data services run (S36b, the S34 deviation 2: the `md ...` requests need a fresh heartbeat with the ARM
        off too). Nothing is written after the stop."""
        while not stop.is_set():
            stamp.stamp()
            try:
                if await self.svc.md.bootstrap.save_record():
                    self.record_saves += 1
            except Exception as exc:
                log.info("md node record not saved (%s)", type(exc).__name__)
            stamp.stamp()
            graph = self.graph
            writer = graph.arm.status if graph is not None else self._md_status_writer()
            if writer is not None and not stop.is_set():
                stamp.set_exempt("postgres")
                try:
                    if graph is not None and graph.controls_store is not None:
                        await self.controls.refresh(graph.controls_store, STATUS_WRITE_S)
                    async with asyncio.timeout(STATUS_WRITE_S):
                        await writer.upsert(self.status_row())
                    self.status_writes += 1
                except Exception as exc:
                    if is_lost_fence(exc):
                        raise
                    self.status_errors += 1
                    log.info("arm_status not written (%s)", type(exc).__name__)
                finally:
                    stamp.set_exempt(None)
            if graph is not None and not stop.is_set():
                self.liveness_pass(self.session.ctx.clock_ms())
            await _stop_stamped(stop, STATUS_EVERY_S, stamp)

    def _md_status_writer(self) -> ArmStatusWriter | None:
        """The engine pool's `arm_status` writer for the heartbeat while the ARM graph does not run (None with the
        in-memory leader ports, which have no engine pool)."""
        if not self._md_status_checked:
            self._md_status_checked = True
            engine_db: EngineDb | None = getattr(self.session.ports, "engine_db", None)
            if engine_db is not None:
                self._md_status = PgArmStatusStore(engine_db.source)
            else:
                log.info("no engine pool: the market-data heartbeat row is not written")
        return self._md_status

    def status_row(self) -> ArmStatusRow:
        """The heartbeat row: codes and counts only. `running` is True while the BarClock is built and the `arm`
        loop has started (S36b). `paused` is the operator pause (`arm_controls`, as last read; reason `operator`) or
        the config-hash pause (the installer's `pause_reason`, `config_hash`), the operator's reason first when both
        hold (S34). The installer's state is in the `config_state_*` counters; the controls reads in `controls_*`;
        the decision path's in `barclock_*`, `tracker_*`, `notices_*` and `evaluation_*`."""
        s = self.session
        now = s.ctx.clock_ms()
        graph = self.graph
        summary = graph.failed.summary() if graph is not None else None
        counters: list[tuple[str, int]] = []
        config = graph.config if graph is not None else None
        armed = unmanaged = overdue = 0
        running = False
        if graph is not None:
            counters.extend(graph.budget.snapshot())
            counters.append(("failed_pending", len(graph.failed.pending())))
            counters.append(("tick_errors", graph.tick_errors))
            counters.append(("decide_holds", graph.decide_holds))
            counters.append(("notice_errors", graph.notice_errors))
            parts: tuple[tuple[str, object], ...] = (
                ("barclock", graph.clock.counters if graph.clock is not None else None),
                ("tracker", graph.tracker.counters if graph.tracker is not None else None),
                ("notices", graph.notices.counters if graph.notices is not None else None),
                ("evaluation", graph.evaluator.counters if graph.evaluator is not None else None))
            for prefix, value in parts:
                counters.extend(_int_fields(prefix, value))
            running = graph.clock is not None and graph.started
            if graph.tracker is not None:
                armed = graph.tracker.armed_count()
                unmanaged = len(graph.tracker.unmanaged())
                overdue = len(self.overdue_now(now))
                counters.append(("unarmed_positions", graph.tracker.unarmed(now)))
            counters.append(("liveness_pages", self.liveness.pages))
            counters.append(("liveness_errors", self.liveness_errors))
            counters.append(("suspend_notes", self.suspend_latch.notes))
        if config is not None:
            counters.extend(config.snapshot())
        controls = self.controls
        counters.extend((("controls_reads", controls.reads), ("controls_read_errors", controls.read_errors),
                         ("controls_drain", int(controls.drain()))))
        pause = self.pause_reason()
        return ArmStatusRow(node=s.node_id, epoch=s.epoch.token, updated_at_ms=now, heartbeat_at_ms=now, leader=True,
                            running=running, paused=pause is not None, pause_reason=pause,
                            config_doc_hash=config.installed_hash() if config is not None else None,
                            armed_positions=armed, unmanaged_positions=unmanaged, overdue_positions=overdue,
                            failed_scopes=summary.count if summary is not None else 0,
                            failed_global=summary.global_failed if summary is not None else False,
                            counters=tuple(sorted(counters)))

    def pause_reason(self) -> str | None:
        """The operator pause (as last read) or the config-hash pause (a raising installer counts as paused)."""
        reason = self.controls.pause_reason()
        if reason is not None:
            return reason
        graph = self.graph
        config = graph.config if graph is not None else None
        if config is None:
            return None
        try:
            return config.pause_reason()
        except Exception:
            return CONFIG_PAUSE_UNREADABLE

    # ------------------------------------------------------------------------------------------------ ARM graph
    def arm_sinks(self) -> SinkChain:
        """The ARM sink chain (`OptionalPlan.arm_sinks`, built with the ARM pool): the decision sink, then the notice
        slot bound to `ArmNotices.sink()` once the graph is built (S36b)."""
        if self.late_notices is None:
            self.late_notices = LateNoticeSink()
        return SinkChain((DecisionSink(fatal=is_lost_fence), self.late_notices))

    def arm_graph(self) -> ArmGraph:
        """Build (no I/O) the ARM objects once per term, on the first `arm_handlers` / `arm_tasks` call: the S30
        FAILED machinery, the relay, the configuration installer and (S36b) the decision path: the arm-tracker, the
        BarClock with the S21 evaluation and the notices in the sink chain."""
        if self.graph is not None:
            return self.graph
        svc, s = self.svc, self.session
        arm = svc.arm
        settings = svc.settings.arm
        if arm is None or settings is None:
            raise RuntimeError("the ARM services are not built")
        engine_db = self._engine_db(arm)
        clock = _SessionClock(s.ctx.clock_ms)
        fail = settings.fail
        budget = ErrorBudget(fail, calendar=svc.calendar, health=svc.health, pg=arm.pg_health,
                             maintenance=_Maintenance(s))
        md = svc.md
        # S39b (gate uptime_suspend): a job whose run spans a PC sleep is never a timeout or a fault
        jobs = JobRunner(suspend_after_ms=SUSPEND_AFTER_MS)
        engine_failures = arm.failures if engine_db is arm.db else PgArmFailuresStore(engine_db)
        span = LateSpanProbe()
        # S38e: every probe port wired (series reads over md-bars, the version probe over the registry and the job
        # runner), so every FAILED scope kind can be cleared; none is None (asserted by tests/runtime/
        # test_optional_plan.py::test_arm_plan_binds_the_budget_and_reports_failed)
        probes = ClearProbes(
            series=MdBarsSeriesProbe(md.bars), versions=VersionConformanceProbe(ARM_REGISTRY, jobs),
            indicators=IndicatorStateProbe(md.indicators, self._indicators_of), span=span,
            trial=TrialBootstrapProbe(md.service), circuits=_Circuits(s), hung=jobs)
        hooks = ScopeClearHooks(series=self._series_cleared, version=jobs.reset,
                                post_clear=md.service.request_post_clear)
        late = _GraphFailedView(self)
        store = PgArmStateStore(arm.db)
        failed = FailedManager(
            budget=budget, stores=FailedStores(arm=arm.failures, engine=engine_failures),
            positions=store, spool=arm.spool, pager=svc.pager, clock=clock,
            epoch=lambda: s.epoch.token, fail=fail, answers=SessionAnswers(s), fatal=is_lost_fence,
            engine_positions=PgArmStateStore(engine_db), probes=probes, clear=PgArmFailedClear(engine_db),
            hooks=hooks, series_of=late, cleared_notice=late,
            # S38f: the D7 counters survive restarts and takeovers (ARM pool, fenced); unclean restarts count (the
            # leader_terms read on the engine pool)
            budget_store=PgArmBudgetStore(arm.db),
            # S38g: a PC sleep's own restart (a sleep fence recorded by the lease keeper) is not an unclean restart
            restarts=SleepAwareRestarts(PgRestartHistory(engine_db.source), self._sleep_fences))
        # The ARM channels follow the secrets source (plan S29, operations.md §13, oversee-rules.md §10): with a
        # `RotatingChannels` the relay re-reads them every credentials poll and at once after an auth error.
        built = self.plan.arm_channels(s)
        rotating = built if isinstance(built, RotatingChannels) else None
        channels = built.current if isinstance(built, RotatingChannels) else built
        renderer = SendTimeRenderer(md.port, keyring=lambda: md.settings.keyring)
        relay = ArmRelay(PgArmOutboxStore(engine_db), channels.channels, renderer=renderer,
                         disabled=channels.disabled, issues=channels.issues, lease_valid=s.ctx.lease.lease_valid,
                         clock_ms=s.ctx.clock_ms, page=svc.pager.page, drain_notices=self.controls.drain,
                         channel_source=rotating,
                         footer=lambda: s.email_footer(),   # read at send time
                         # XTNL_EMAIL_THEME of the ARM channels; the xtnl theme shows pips from the entry (point sizes
                         # read at send time, never stored or shown)
                         email_theme=channels.email_theme, point_sizes=PgPointSizes(engine_db))
        config = self._config_installer(arm)
        accepted = config if config is not None else _NoConfig()
        grid = CalendarGrid(schedule_of=s.calendar.schedule_of, skew_of=md.hook.skew_estimate,
                            bars_capable=self._bars_capable)
        disabled = frozenset(channels.disabled)
        notices = ArmNotices(store=store, outbox=PgArmOutboxInserter(), routes=RouteBook(),
                             policies=AcceptedPolicies(accepted), grid=grid, pager=svc.pager, clock=clock,
                             settings=settings, failed=failed, fatal=is_lost_fence, unmanaged=self._unmanaged_count,
                             channels_configured=lambda names: not (set(names) & disabled))
        snapshot = PgTrackerSnapshot(engine_db, integrity_incident=self._integrity_incident,
                                     catchup_verified=self._catchup_verified)
        tracker = ArmTracker(
            store=store, versions=PgArmVersionReader(arm.db), snapshot=snapshot, md=md.port, grid=grid,
            config=accepted, notices=notices, answers=SessionAnswers(s), pager=svc.pager, clock=clock,
            settings=settings, failed=failed, tracker=TrackerSettings(tick_symbols=svc.settings.tick_pairs),
            fatal=is_lost_fence,
            # S39b (gate entry_time_source): ESTIMATED -> BROKER from the broker's trade-history execution time
            exec_times=OrchestratorExecutionTimes(s.orchestrator))
        evaluator = PipelineEvaluator(md=md.port, snapshot=snapshot, versions=PgArmVersionReader(arm.db),
                                      decisions=store, grid=grid, calendar=svc.calendar, settings=settings,
                                      keyring=lambda: md.settings.keyring,
                                      held=lambda row: failed.scopes.covers_row(row, tracker.series_of(row)),
                                      no_trade=lambda series, bucket: _no_trade(md.bars, series, bucket),
                                      verified=md.bars.verified)
        stop_reads = StopReads(arm.stop_reads, TrackerBook(tracker), enabled=settings.stop_reads,
                               pre_offset_s=settings.pre_offset_s)
        barclock = BarClock(store=store, controls=PgArmConfigStore(arm.db), md=md.port, grid=grid,
                            evaluator=evaluator, jobs=jobs, pager=svc.pager, clock=clock, settings=settings,
                            lease_valid=s.ctx.lease.lease_valid, stop_reads=stop_reads, sinks=arm.sinks,
                            closes=grid, notices=notices, fatal=is_lost_fence,
                            config_pause=None if config is None else config.pause_reason,
                            keyring=lambda: md.settings.keyring, suspended=self.suspend_latch.take,
                            barclock=BarClockSettings(suspend_after_ms=SUSPEND_AFTER_MS),
                            # S39b (gate corrections_restart): the correction window survives a restart / takeover
                            recent=store)
        span.bind(barclock)
        # D4 / §A13 (round 13): one INFO notice per counted md reset (store lost, a fresh store on a later term)
        md.bootstrap.on_reset(lambda reset: notices.md_reset(str(reset.generation), clock.now_ms())
                              if reset.notice else None)
        if self.late_notices is not None:
            self.late_notices.bind(notices.sink())
        else:
            log.warning("arm: the wired sink chain has no late notice slot; decisions wait for the timer pass")
        svc.stop_read_cost = lambda adapter_id: stop_read_cost(tracker, adapter_id)
        md.faults.bind(budget)
        self.circuits.bind(budget)
        # the ARM scope FAILED, or the open episodes unreadable: what the dead-man and the metric report (fail closed)
        md.global_failed.bind(lambda: failed.summary().global_failed)
        self.graph = ArmGraph(arm=arm, engine_db=engine_db, budget=budget, failed=failed, relay=relay,
                              controls_store=PgArmConfigStore(arm.db), config=config, jobs=jobs, notices=notices,
                              tracker=tracker, clock=barclock, evaluator=evaluator, grid=grid,
                              flush_errors_seen=md.writer.counters.flush_errors)
        return self.graph

    def _unmanaged_count(self) -> int:
        graph = self.graph
        return 0 if graph is None or graph.tracker is None else len(graph.tracker.unmanaged())

    def _bars_capable(self, adapter_id: str) -> bool:
        """The arm-tracker's `bar_sides` (§B6.4): the adapter's `BrokerAdapter.price_bars_supported` **and** its
        membership in `golive.bar_capable_adapters()`, the same set admission and `arm config accept` use, so an
        assignment with no adapter (a wildcard, or scoped by account or symbol) never arms a position of an adapter
        S39's gates still hold back (S39 review); an unknown adapter has none."""
        if adapter_id not in golive_bar_capable_adapters():
            return False
        try:
            return bool(self.session.orchestrator.runtime(adapter_id).adapter.price_bars_supported)
        except Exception:
            return False

    def _integrity_incident(self) -> bool:
        return bool(self.session.integrity.integrity_incident)

    def _catchup_verified(self, adapter_id: str) -> bool:
        return bool(self.session._catchup_verified(adapter_id))  # noqa: SLF001 (the session's own gate input)

    def _config_installer(self, arm: ArmServices) -> ConfigInstaller | None:
        """The `arm-config` installer over the configured source and the ARM pool's config store; None (idle, one
        page naming the variables, never a value) when the source is not configured or its settings are bad."""
        source = self.plan.config_source(self.session)
        if source is None or isinstance(source, list):
            reason = "XTNL_ARM_CONFIG_DSN is not set" if source is None else "; ".join(source)
            self.config_idle_reason = "no_dsn" if source is None else "settings"
            self.svc.pager.page("high", "ARM configuration idle",
                                f"{reason}: no configuration is read or installed, the ARM arms nothing")
            return None
        from src.armconfig.installer import ConfigInstaller

        s = self.session
        return ConfigInstaller(source=source, store=PgArmConfigStore(arm.db), pager=self.svc.pager,
                               known=self.known_ids, clock_ms=s.ctx.clock_ms, node=s.node_id, fatal=is_lost_fence)

    def known_ids(self) -> KnownIds | None:
        """The ids a configuration document may name (§B9): this engine's adapters, their loaded instruments and
        accounts, and the notice routes `adapter:account` plus `*`. None while an adapter's instrument map is not
        loaded yet (nothing is validated then)."""
        from src.armconfig.validate import KnownIds

        orch = self.session.orchestrator
        symbols: set[str] = set()
        accounts: dict[str, str] = {}
        routes: set[str] = {GLOBAL_ROUTE}
        adapters = tuple(self.session.adapter_ids)
        for aid in adapters:
            try:
                rt = orch.runtime(aid)
                instruments = tuple(rt.instruments)
                account_ids = tuple(rt.accounts)
            except Exception:
                return None
            if not instruments:
                return None
            symbols.update(instruments)
            for acct in account_ids:
                accounts[acct] = acct
                routes.add(f"{aid}:{acct}")
        return KnownIds(symbols=frozenset(symbols), adapters=frozenset(adapters), accounts=accounts,
                        routes=frozenset(routes))

    def accepted_config(self) -> ConfigInstaller | None:
        """The arm-tracker's `AcceptedConfigPort` (the installer), once the ARM graph exists and a source is set."""
        graph = self.graph
        return None if graph is None else graph.config

    def _engine_db(self, arm: ArmServices) -> EngineDb:
        """The term pool's fenced `EngineDb` for the engine-pool stores (the ARM outbox, the FAILED clear, the
        engine-side open-episode and position reads: §B14 Record, "through the engine's own pool when the ARM pool is
        the fault"). Only the in-memory ports (plain `LeaderPorts`, tests) fall back to the ARM pool; production
        ports without it are a wiring fault: raised, so the ARM step retries and pages."""
        engine_db: EngineDb | None = getattr(self.session.ports, "engine_db", None)
        if engine_db is not None:
            return engine_db
        from src.runtime.pg_ports import PgLeaderPorts

        if isinstance(self.session.ports, PgLeaderPorts):
            raise RuntimeError("the engine pool's EngineDb is not wired (PgLeaderPorts.engine_db)")
        log.info("in-memory leader ports: the ARM's engine-pool stores use the ARM pool")
        return arm.db

    def _indicators_of(self, series: str) -> Sequence[str]:
        ind = self.svc.md.indicators
        own = indicator_of_scope(series)
        if own is not None:                      # an indicator's guard-input scope (round 13)
            return [own]
        out: list[str] = []
        for iid in ind.tracked():
            inputs = ind.input_series(iid) or ()
            if any(s.canonical == series for s in inputs):
                out.append(iid)
        return out

    def _series_cleared(self, series: str) -> None:
        """An accepted SERIES clear: the D6 cross-check re-arms and SM12 leaves FAILED (`FAILED -> BACKFILLING`). An
        indicator's guard-input scope has no series side effect (`md_rebuild` already re-seeded it)."""
        if indicator_of_scope(series) is not None:
            return
        try:
            sid = SeriesId.parse(series)
        except ValueError:
            log.warning("cleared series id does not parse; cross-check not reset")
            return
        self.svc.md.crosscheck.clear(sid)
        self.svc.md.book.apply(sid, Sm12Event.CLEAR)

    def arm_tasks(self) -> list[TaskSpec]:
        graph = self.arm_graph()
        s = self.session
        reopen = self.circuits.reopen

        async def relay_body(stop: asyncio.Event, stamp: StampPort) -> None:
            await graph.relay.run(stop, stamp.stamp)

        specs = [loop_spec(s, ARM_TASK, self._arm_loop, on_circuit_reopen=reopen),
                 loop_spec(s, ARM_RELAY_TASK, relay_body, on_circuit_reopen=reopen)]
        if graph.tracker is not None:
            specs.append(loop_spec(s, ARM_TRACKER_TASK, graph.tracker.run, on_circuit_reopen=reopen))
        config = graph.config
        if config is not None:
            specs.append(loop_spec(s, ARM_CONFIG_TASK, config.run, on_circuit_reopen=reopen))
        return specs

    def arm_handlers(self) -> dict[str, RequestHandler]:
        self.arm_graph()
        handlers: dict[str, RequestHandler] = {"arm_clear_failed": self._arm_clear_failed}
        for kind in sorted(TRACKER_KINDS):
            handlers[kind] = self._tracker_request
        return handlers

    def _arm_clear_failed(self, req: OperatorRequest) -> HandlerResult:
        graph = self.graph
        if graph is None:
            return Refused("arm_not_running")
        graph.failed.submit_clear(req.request_id, req.target)
        return Accepted(ARM_TASK)

    def _tracker_request(self, req: OperatorRequest) -> HandlerResult:
        """`arm_disarm` / `arm_repin` / `arm_rearm` (S26, §B13): enqueued to the `arm-tracker` (non-blocking), which
        answers applied, refused or moot through `EngineSession.complete_request`."""
        graph = self.graph
        tracker = None if graph is None else graph.tracker
        if tracker is None:
            return Refused("arm_not_running")
        try:
            request = parse_tracker_request(req)
        except PayloadError:
            return Refused("bad_payload")
        if not tracker.submit(request):
            return Refused("queue_full")
        return Accepted(ARM_TRACKER_TASK)

    # ------------------------------------------------------------------------------------------------ the arm loop
    async def _arm_loop(self, stop: asyncio.Event, stamp: StampPort) -> None:
        """The `arm` task: FAILED start (spool replay, open episodes), then one tick a second (the budget's
        observations, deferred faults, records, pings, SM11 FAILED and restore, clears; the job runner's
        STRATEGY_VERSION signals, the stuck-frontier views and the `arm notify-test` request), the BarClock's pass
        every 250 ms (or at the next PRE fire) and the notices' timer pass every 5 s (S36b). The BarClock decides
        only once the open FAILED episodes have been read and while no ARM-global FAILED holds. A lost fence raises
        (the process exits); any other error is counted and retried. On stop: unrecorded episodes go to the spool,
        running clears are answered `probe_interrupted` (bounded, no PostgreSQL write) and the BarClock's job
        executors and stop reads are closed (a hung job thread ends the process through the exit hook)."""
        graph = self.arm_graph()
        fm = graph.failed
        next_tick = 0.0
        next_notices = 0.0
        try:
            while not stop.is_set():
                stamp.stamp()
                try:
                    if not graph.started:
                        stamp.set_exempt("postgres")
                        try:
                            # once per term: a failed read of the open episodes is retried by tick(), never by
                            # replaying the spool again
                            if not await fm.start():
                                log.info("arm: open episodes unreadable at start; tick() retries")
                        finally:
                            stamp.set_exempt(None)
                        graph.started = True
                        await self.report_sleep_restarts()
                    if stop.is_set():
                        break
                    if time.monotonic() >= next_tick:
                        next_tick = time.monotonic() + ARM_TICK_S
                        await self._observe(graph, stamp, stop)
                        if stop.is_set():
                            break
                        stamp.set_exempt("postgres")
                        try:
                            await fm.tick()
                        finally:
                            stamp.set_exempt(None)
                        self._after_tick(graph)
                    if stop.is_set():
                        break
                    await self._decide(graph, stamp)
                    if stop.is_set():
                        break
                    if time.monotonic() >= next_notices:
                        next_notices = time.monotonic() + NOTICES_EVERY_S
                        await self._notice_pass(graph, stamp)
                except Exception as exc:
                    if is_lost_fence(exc):
                        raise
                    graph.tick_errors += 1
                    log.warning("arm: tick failed (%s)", type(exc).__name__)
                stamp.stamp()
                await _stop_or(stop, self._arm_sleep_s(graph, next_tick))
        finally:
            if stop.is_set():
                with contextlib.suppress(Exception):
                    async with asyncio.timeout(DRAIN_FAILED_S):
                        await fm.drain()
                self._close_clock(graph)

    def _arm_sleep_s(self, graph: ArmGraph, next_tick: float) -> float:
        """Until the next BarClock pass (250 ms, or the next PRE fire), never past the next FAILED tick."""
        left = max(0.0, next_tick - time.monotonic())
        clock = graph.clock
        if clock is not None:
            left = min(left, clock.sleep_ms(self.session.ctx.clock_ms()) / 1000)
        return max(MIN_SLEEP_S, left)

    def _may_decide(self, graph: ArmGraph) -> bool:
        """The open FAILED episodes have been read (until then every position is held), no ARM-global FAILED holds,
        and the bootstrap's open-episode read allowed advice."""
        scopes = graph.failed.scopes
        return scopes.ready and not scopes.global_failed and graph.arm.may_advise

    async def _decide(self, graph: ArmGraph, stamp: StampPort) -> None:
        clock = graph.clock
        if clock is None:
            return
        if not self._may_decide(graph):
            graph.decide_holds += 1
            return
        # exempt only while the BarClock awaits its own PostgreSQL calls (operations.md §3); its market-data reads,
        # prepares and jobs are bounded and stamped instead, so a hung read shows as a stale stamp or a timeout
        await clock.guarded_step(stamp=stamp.stamp, exempt=stamp.set_exempt)

    async def _notice_pass(self, graph: ArmGraph, stamp: StampPort) -> None:
        """The notices' timer pass, suppressed while either pause holds (the S33 hand-off); a failure is counted."""
        notices = graph.notices
        if notices is None or self.pause_reason() is not None:
            return
        stamp.set_exempt("postgres")
        try:
            await notices.step()
        except Exception as exc:
            if is_lost_fence(exc):
                raise
            graph.notice_errors += 1
            log.warning("arm: notice pass failed (%s)", type(exc).__name__)
        finally:
            stamp.set_exempt(None)

    def _after_tick(self, graph: ArmGraph) -> None:
        """Once a second, after the FAILED tick: the job runner's STRATEGY_VERSION signals into the budget, the
        stuck-frontier views (paused and held positions never count), the evaluator's memory pruned to the live
        rows, and a new `arm notify-test` request queued as the TEST notice."""
        now = self.session.ctx.clock_ms()
        settings = self.svc.settings.arm
        if graph.jobs is not None:
            for sig in graph.jobs.take_signals():
                graph.budget.version_fault(sig.scope_id, sig.faults, sig.position_ids, now,
                                           edge_code=None if settings is None else settings.edge_code)
            hung = graph.jobs.take_restart_required()
            if hung is not None:
                graph.budget.restart_required(hung, now)
        clock = graph.clock
        if clock is not None:
            graph.budget.observe_frontier(clock.frontier_views(now), now, running=self._may_decide(graph),
                                          failed_stuck=graph.failed.stuck_positions())
            if graph.evaluator is not None:
                graph.evaluator.forget(clock.live_refs())
        row = self.controls.row
        at = None if row is None else row.notify_test_at_ms
        if at is not None and at != graph.notify_test_seen and graph.notices is not None:
            graph.notify_test_seen = at
            if now - at <= NOTIFY_TEST_WINDOW_MS:
                graph.notices.request_test(at)

    def _close_clock(self, graph: ArmGraph) -> None:
        clock = graph.clock
        if clock is None:
            return
        try:
            hung = clock.close()
        except Exception as exc:
            log.warning("arm: BarClock not closed (%s)", type(exc).__name__)
            return
        if hung:
            log.critical("arm: a strategy job thread is still hung at stop; ending the process")
            self.session.ctx.exit_fn("arm job hung")

    def armed_adapter_ids(self) -> tuple[str, ...]:
        """The adapters of the armed positions (from the `arm-tracker`'s last pass)."""
        graph = self.graph
        if graph is None or graph.tracker is None:
            return ()
        return graph.tracker.armed_adapters()

    async def _observe(self, graph: ArmGraph, stamp: StampPort, stop: asyncio.Event) -> None:
        """The error budget's time counters (§B14): `redis-md` usable (store loaded, no bootstrap wanted, no new
        flush error) while an armed position's market is OPEN (the arm-tracker's armed adapters), and the ARM pool
        answering a `SELECT 1` within 3 s."""
        s = self.session
        now = s.ctx.clock_ms()
        writer = self.svc.md.writer
        errors = writer.counters.flush_errors
        usable = writer.boot is not None and writer.bootstrap_wanted is None and errors == graph.flush_errors_seen
        graph.flush_errors_seen = errors
        market_open = False
        for aid in self.armed_adapter_ids():
            with contextlib.suppress(Exception):
                market_open = market_open or bool(s.calendar.market_open(aid))
        graph.budget.observe_md(usable=usable, market_open=market_open, now_ms=now)
        if stop.is_set():
            return
        ok = True
        stamp.set_exempt("postgres")
        try:
            async with asyncio.timeout(POOL_CHECK_S):
                async with graph.arm.pool.acquire() as conn:
                    await conn.fetchrow("SELECT 1")
        except Exception as exc:
            if is_lost_fence(exc):
                raise
            ok = False
        finally:
            stamp.set_exempt(None)
        graph.pool_ok = ok
        graph.budget.observe_pool(arm_ok=ok, now_ms=s.ctx.clock_ms())

    # ------------------------------------------------------------------------------------------------ OptionalView
    def arm_oldest_undelivered_notification_ms(self) -> int | None:
        graph = self.graph
        return None if graph is None else graph.relay.oldest_undelivered_ms()

    def arm_overdue_positions(self) -> int | None:
        """Armed positions not decided for more than 2 OPEN bars (the dead-man field, design §B8; round 14:
        `src.riskmgr.liveness`); None while the tracker does not run."""
        graph = self.graph
        if graph is None or graph.tracker is None:
            return None
        return len(self.overdue_now(self.session.ctx.clock_ms()))

    def overdue_now(self, now_ms: int) -> dict[str, int]:
        """The confirmed overdue armed positions from the tracker's last rows and the SM10 grid (no I/O): every row
        in a deciding state not covered by a FAILED scope, while neither pause holds (`liveness.overdue_positions`),
        counted only once confirmed (`LivenessWatch.confirm`: seen on tracker rows read after a BarClock pass that
        started after the first sighting, or overdue for 2 min), so the stale cursor of rows read before a restart's
        DOWNTIME span never reaches the dead-man or a page (round 14 review)."""
        graph = self.graph
        if graph is None or graph.tracker is None or graph.grid is None:
            return {}
        tracker, failed, grid, clock = graph.tracker, graph.failed, graph.grid, graph.clock
        raw = overdue_positions(tracker.live_rows(), grid, now_ms, schedule_known=grid.has_schedule,
                                held=lambda row: failed.scopes.covers_row(row, tracker.series_of(row)),
                                paused=self.pause_reason() is not None)
        return self.liveness.confirm(raw, now_ms, rows_read_ms=tracker.rows_read_ms,
                                     barclock_last_pass_ms=None if clock is None else clock.last_pass_ms)

    def liveness_pass(self, now_ms: int) -> None:
        """The three liveness pages (round 14), from the `arm-status` loop every 5 s; never raises."""
        graph = self.graph
        if graph is None:
            return
        try:
            self.liveness.positions(self.overdue_now(now_ms))
            clock, tracker = graph.clock, graph.tracker
            self.liveness.loops(now_ms, barclock_last_pass_ms=None if clock is None else clock.last_pass_ms,
                                tracker_last_pass_ms=None if tracker is None else tracker.last_pass_ms,
                                barclock_running=clock is not None and graph.started and self._may_decide(graph),
                                tracker_running=tracker is not None)
            self.liveness.relay(now_ms, graph.relay.oldest_undelivered_ms())
        except Exception as exc:
            self.liveness_errors += 1
            log.warning("arm liveness not checked (%s)", type(exc).__name__)

    async def _sleep_fences(self) -> list[int]:
        """The node's recorded sleep fences (wall ms), for the unclean-restart counter (S38g); the node-local file is
        read in a thread, never on the event loop."""
        sleep_log = self.session.ctx.sleep_log
        if sleep_log is None:
            return []
        return [f.at_ms for f in await asyncio.to_thread(sleep_log.fences)]

    async def report_sleep_restarts(self) -> int:
        """Page each sleep restart not reported yet, once (round 14; the record is node-local, read off the loop)."""
        sleep_log = self.session.ctx.sleep_log
        if sleep_log is None:
            return 0
        try:
            fences = await asyncio.to_thread(sleep_log.take_unreported)
        except Exception as exc:
            log.warning("sleep restarts not read (%s)", type(exc).__name__)
            return 0
        for f in fences:
            self.svc.pager.page("warning", "ARM: engine restarted after a PC sleep",
                                f"the process was suspended about {f.gap_ms // 1000} s and the lease fence ended "
                                f"the term at {f.at_ms}; missed bars are spanned, the restart is not counted as "
                                "unclean (docs/runbooks/arm.md)")
        self.sleep_restarts_reported += len(fences)
        return len(fences)

    def arm_active(self) -> bool:
        """The ARM graph runs this term (its dead-man field and metric series are then due)."""
        return self.graph is not None

    def arm_failed(self) -> ArmFailedBeat | None:
        """None while the ARM does not run; an unreadable FAILED state reports one global FAILED (fail closed)."""
        graph = self.graph
        if graph is None:
            return None
        try:
            summary = graph.failed.summary()
        except Exception as exc:
            log.warning("arm failed state not read (%s); reported as global FAILED", type(exc).__name__)
            return UNKNOWN_FAILED
        return ArmFailedBeat(summary.count, summary.global_failed, summary.unrecorded)

    def metrics_samples(self) -> list[Sample]:
        """The market-data and ARM series of `deploy/observability/metrics.md` (counts and states only). The ARM
        series come first and are contained on their own (an unreadable FAILED state reports one global FAILED), so a
        market-data fault never drops them."""
        out = self._arm_samples()
        try:
            out += self._md_samples()
        except Exception as exc:
            log.warning("market-data metric series not read (%s)", type(exc).__name__)
        return out

    def _md_samples(self) -> list[Sample]:
        md = self.svc.md
        out: list[Sample] = []
        states: dict[str, int] = {st.value: 0 for st in Sm12State}
        for sid in md.book.series():
            states[md.book.state(sid).value] += 1
        for name, n in sorted(states.items()):
            out += gauge("xtnl_md_series", n, state=name)
        ind_states: dict[str, int] = {st.value: 0 for st in IndState}
        for iid in md.indicators.tracked():
            st = md.indicators.status(iid)
            if st is not None:
                ind_states[st.state.value] += 1
        for name, n in sorted(ind_states.items()):
            out += gauge("xtnl_md_indicators", n, state=name)
        out.append(Sample("xtnl_md_aof_wait_timeouts_total", float(md.writer.counters.aof_wait_timeouts), {}))
        md_counters: dict[str, object] = {"writer": md.writer.counters, "bars": md.bars.counters,
                                          "indicators": md.indicators.counters, "service": md.service.counters,
                                          "hook": md.hook.counters, "crosscheck": md.crosscheck.counters}
        for component in MD_EVENT_COMPONENTS:
            out += _counter_samples("xtnl_md_events_total", component, md_counters[component],
                                    skip=("aof_wait_timeouts",))
        status = md.writer.status()
        if status.memory_ok and status.memory_ratio is not None:     # a failed read: no series, never a stale one
            out += gauge("xtnl_md_redis_used_memory_ratio", status.memory_ratio)
        out += gauge("xtnl_md_shed_step", status.shed_level)
        out += gauge("xtnl_md_queue_depth", status.queued_bars, queue="closed_bars")
        out += gauge("xtnl_md_queue_depth", status.pending_ticks, queue="pending_ticks")
        return out

    def _arm_samples(self) -> list[Sample]:
        graph = self.graph
        if graph is None:
            return []
        beat = self.arm_failed() or UNKNOWN_FAILED
        out = failed_samples(beat)
        try:
            oldest = graph.relay.oldest_undelivered_ms()
            if oldest is not None:
                age = max(0, self.session.ctx.clock_ms() - oldest) / 1000.0
                out += gauge("xtnl_arm_oldest_undelivered_notice_age_seconds", age)
            clock = graph.clock
            arm_counters: dict[str, object] = {
                "failed": graph.failed.counters, "budget": graph.budget.counters,
                "tracker": graph.tracker.counters if graph.tracker is not None else None,
                "barclock": clock.counters if clock is not None else None,
                "stopreads": clock.stop_reads.counters if clock is not None else None,
                "notices": graph.notices.counters if graph.notices is not None else None,
                "evaluation": graph.evaluator.counters if graph.evaluator is not None else None}
            for component in ARM_EVENT_COMPONENTS:
                out += _counter_samples("xtnl_arm_events_total", component, arm_counters[component])
            if clock is not None:
                for reason, n in sorted(clock.counters.pre_dropped.items()):
                    if _EVENT_CODE.fullmatch(reason):
                        out.append(Sample("xtnl_arm_events_total", float(n),
                                          {"component": PRE_SKIPPED_COMPONENT, "event": reason}))
            out += gauge("xtnl_arm_paused", 1 if self.pause_reason() is not None else 0)
            tracker = graph.tracker
            if tracker is not None:
                out += gauge("xtnl_arm_positions", tracker.armed_count(), view="armed")
                out += gauge("xtnl_arm_positions", len(tracker.unmanaged()), view="unmanaged")
                now = self.session.ctx.clock_ms()
                out += gauge("xtnl_arm_positions", len(self.overdue_now(now)), view="overdue")
                out += gauge("xtnl_arm_positions", tracker.unarmed(now), view="unarmed")
        except Exception as exc:
            log.warning("ARM metric series not read (%s)", type(exc).__name__)
        return out


#: `xtnl_md_events_total{component, event}` (deploy/observability/metrics.md): component -> the counters dataclass whose
#: integer fields are its `event` values (the metrics contract test maps the design §C2 list onto these).
MD_EVENT_COMPONENTS: Final[Mapping[str, type]] = {
    "writer": WriterCounters, "bars": BarsCounters, "indicators": IndicatorCounters, "service": ServiceCounters,
    "hook": HookCounters, "crosscheck": CrossCheckCounters}
#: `xtnl_arm_events_total{component, event}` likewise (each absent while its object is not built this term).
ARM_EVENT_COMPONENTS: Final[Mapping[str, type]] = {
    "failed": FailedCounters, "budget": BudgetCounters, "tracker": TrackerCounters, "barclock": BarClockCounters,
    "stopreads": StopReadCounters, "notices": NoticeCounters, "evaluation": EvalCounters}
#: PRE skipped by reason (design §C2): `xtnl_arm_events_total{component="pre_skipped", event=<reason code>}`.
PRE_SKIPPED_COMPONENT: Final = "pre_skipped"
_EVENT_CODE: Final = re.compile(r"[a-z][a-z0-9_]{0,47}")

#: An unreadable FAILED state, reported fail closed (one scope, global).
UNKNOWN_FAILED: Final = ArmFailedBeat(1, True)


def failed_samples(beat: ArmFailedBeat) -> list[Sample]:
    """`xtnl_arm_failed_scopes` / `xtnl_arm_failed_global` of a FAILED summary (the off-node `XtnlArmFailed`)."""
    return (gauge("xtnl_arm_failed_scopes", beat.count)
            + gauge("xtnl_arm_failed_global", 1 if beat.global_failed else 0))


def _int_fields(prefix: str, counters: object) -> list[tuple[str, int]]:
    """A counters dataclass as `(prefix_field, value)` pairs (integer fields only)."""
    if counters is None or not is_dataclass(counters) or isinstance(counters, type):
        return []
    out: list[tuple[str, int]] = []
    for f in fields(counters):
        value = getattr(counters, f.name)
        if isinstance(value, int) and not isinstance(value, bool):
            out.append((f"{prefix}_{f.name}", value))
    return out


def _counter_samples(name: str, component: str, counters: object, *, skip: Iterable[str] = ()) -> list[Sample]:
    if not is_dataclass(counters) or isinstance(counters, type):
        return []
    omitted = set(skip)
    out: list[Sample] = []
    for f in fields(counters):
        value = getattr(counters, f.name)
        if f.name in omitted or isinstance(value, bool) or not isinstance(value, int):
            continue
        out.append(Sample(name, float(value), {"component": component, "event": f.name}))
    return out


# ---------------------------------------------------------------------------------------------------- the plan
ArmChannels = ChannelSet | RotatingChannels
"""What an ARM channel factory gives: a fixed `ChannelSet`, or a `RotatingChannels` the relay uses as its
`channel_source` (its `current` is the set to start with)."""


def default_arm_channels(environ: Mapping[str, str],
                         secrets: SecretsSource | None = None) -> Callable[[EngineSession], ArmChannels]:
    """The `XTNL_ARM_` channels (`arm_email`, `arm_push`) over the term's HTTP client (or a client of their own,
    closed with the term). With a secrets source other than the environment (AWS Secrets Manager), the channel
    credentials follow it (operations.md §13, oversee-rules.md §10, plan S29): a `RotatingChannels` over
    `settings_from_secrets(source, environ, prefix=ARM_PREFIX)` with the source's `invalidate`, exactly as the session
    builds the Oversee channels. With the env source the settings are read once, as before."""

    def build(session: EngineSession) -> ArmChannels:
        client: httpx.AsyncClient | None = session.http_client
        if client is None:
            own = session.ctx.http_client()
            session.register_closer("arm-http", own.aclose)
            client = own

        def footer() -> tuple[str, str | None]:
            return session.email_footer()      # read at send time
        if secrets is None or secrets.kind == "env":
            settings = NotificationSettings.from_env(environ, prefix=ARM_PREFIX)
            return build_arm_channels(settings, client, smtp_factory=session.ctx.smtp_factory, footer=footer)
        return RotatingChannels(settings_from_secrets(secrets, environ, prefix=ARM_PREFIX), client,
                                smtp_factory=session.ctx.smtp_factory, invalidate=secrets.invalidate,
                                names=ARM_CHANNEL_NAMES, footer=footer)

    return build


def default_config_source(environ: Mapping[str, str], secrets: SecretsSource | None = None,
                          ) -> Callable[[EngineSession], ConfigSource | None | list[str]]:
    """The Supabase reader of `XTNL_ARM_CONFIG_DSN` / `XTNL_ARM_CONFIG_CA_FILE` over asyncpg's `connect` (imported
    lazily: the untyped driver, as `src/db/pg.py` does). The factory returns None without a DSN (idle) and the
    settings problems (variable names only) for a bad setting. `XTNL_ARM_CONFIG_DSN` is read through the secrets source
    (laid over `environ`, operations.md §13) at each term's build."""

    def build(_session: EngineSession) -> ConfigSource | None | list[str]:
        from src.armconfig.client import SupabaseConfigSource, parse_source_settings

        settings = parse_source_settings(resolved_environ(secrets, environ))
        if settings is None or isinstance(settings, list):
            return settings
        asyncpg = importlib.import_module("asyncpg")
        return SupabaseConfigSource(settings, asyncpg.connect)

    return build


def _no_config_source(_session: EngineSession) -> ConfigSource | None | list[str]:
    return None


@dataclass(frozen=True)
class ProductionPlan:
    """What the production plan needs from the process (the environment's ARM channel and configuration source
    settings)."""

    arm_channels: Callable[[EngineSession], ArmChannels]
    config_source: Callable[[EngineSession], ConfigSource | None | list[str]] = _no_config_source

    def runtime(self, svc: OptionalServices) -> OptionalRuntime:
        rt = svc.runtime
        if isinstance(rt, OptionalRuntime):
            return rt
        built = OptionalRuntime(svc, self)
        svc.runtime = built
        return built

    def plan(self) -> OptionalPlan:
        return OptionalPlan(
            md_tasks=lambda svc: self.runtime(svc).md_tasks(),
            md_handlers=lambda svc: self.runtime(svc).md_handlers(),
            arm_tasks=lambda svc: self.runtime(svc).arm_tasks(),
            arm_handlers=lambda svc: self.runtime(svc).arm_handlers(),
            arm_sinks=lambda svc: self.runtime(svc).arm_sinks())


def production_plan(environ: Mapping[str, str], *,
                    arm_channels: Callable[[EngineSession], ArmChannels] | None = None,
                    config_source: Callable[[EngineSession], ConfigSource | None | list[str]] | None = None,
                    secrets: SecretsSource | None = None,
                    ) -> OptionalPlan:
    """The `OptionalPlan` of the production engine (S32; the `arm-config` source, S33). `secrets`: the engine's
    secrets source, which the ARM channels and the configuration DSN follow (operations.md §13)."""
    return ProductionPlan(arm_channels or default_arm_channels(environ, secrets),
                          config_source or default_config_source(environ, secrets)).plan()


def registry_tokens() -> tuple[str, ...]:
    """PR13: the sorted `KIND:VERSION` tokens of every strategy, guard and indicator this node can load (the health
    record's `registry`)."""
    from src.riskmgr.registry import REGISTRY

    return REGISTRY.registry_tokens()


__all__ = [
    "ARM_CONFIG_TASK", "ARM_KINDS", "ARM_RELAY_TASK", "ARM_STATUS_TASK", "ARM_TASK", "ArmGraph", "CircuitRelay",
    "ControlsView", "DRAIN_STALE_S", "MD_BARS_TASK", "OPERATOR_PAUSE",
    "MD_INDICATORS_TASK", "MD_KINDS", "MD_WRITER_TASK", "OptionalRuntime", "PayloadError", "ProductionPlan",
    "STAMP_S", "SessionAnswers", "default_arm_channels", "default_config_source", "loop_spec", "parse_backfill", "production_plan",
    "registry_tokens", "request_result",
]
