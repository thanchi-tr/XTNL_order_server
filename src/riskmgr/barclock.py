"""The BarClock: the `arm` task (design §B4 BarClock, §B5.2, §B5.3, §B6.6, §B13, §A7, §A13; plan S27).

A scheduler over the `(adapter, bar_spec)` set pinned by live `arm_positions` rows (v1: 15m). Every 250 ms (stamp age
5 s, §B5.1) one pass:

1. **Clock and lease.** Its own wall-versus-monotonic detector (`WallMonoDetector`, a step over 2 s either way, and
   since round 14 a suspend: both clocks moved more than 10 s between two 250 ms passes, which is how a Windows sleep
   shows, as `time.monotonic()` keeps counting through it) and the uptime tracker's suspend signal (`suspended`, the
   runtime's one-shot latch fed by `EngineSession.add_suspend_listener`): a step or a suspend holds the pass (nothing
   read that pass is used; the market-data layer serves `DATA_UNAVAILABLE(clock_step)` for the affected buckets and
   the next pass re-reads, so the bars the sleep covered are decided or spanned at their cap). The leader lease
   (`lease_valid`): no lease, no decision.
2. **Controls** (`arm_controls`, §B13) cached 1 s. **Unreadable controls are not a pause:** every due event is held
   and retried every 250 ms until its window ends; then PRE is not sent (counted `controls_unreadable`) and a POST
   ends as at `decision_cap` (an `UNRESOLVED` span with that reason). **Paused:** no decision at all, so every cursor
   stays put (`Hold(paused)`); bars that end while paused are spanned `paused` on resume if their cap passed; a pause
   longer than 6 h pages once per pause. The **config-hash pause** (§B9 / §B13, reason `config_hash`: the installed
   document is not the latest accepted one) is the injected `config_pause` port (the S33 installer's
   `pause_reason`); it pauses exactly as the operator pause does, read every pass and independent of the controls
   (it pauses even while they are unreadable; a port that raises counts as paused: fail closed). The installer pages
   it, so it has no 6 h page here.
3. **Bar-end events** per position through the S22 replay planner (`src.riskmgr.replay.next_step` over the open bar
   ends after the cursor): a bar whose `decision_cap` passed undecided becomes one `UNRESOLVED_RANGE` span per run and
   reason, which advances the cursor **before the next bar's POST is evaluated**, in the same pass (rounds of at most
   `max_rounds`); then the first undecided bar is decided when its inputs exist: `POST` when the broker's final bar
   `T` is stored and its indicators are updated for `T` (the evaluation port says `NotReady` until then), `LATE` once
   its POST notice has gone, `REOPEN` for a pre-close re-check from the open + settle (every 5 s; §B6.3: only an
   `ADJUST` or `COMPLETE` ends it early, any other result is retried with no row, and the last one is written with its
   reason, changing no state, when the first post-open POST is decided or its cap passes). A POST decided after
   `T + post_grace` carries the caveat `delayed` (§B5.2); a series still waiting at `T + post_grace` pages once per
   episode. A non-final result (a POST-time `DATA_UNAVAILABLE`, a job fault) writes **no row** and does not advance
   the cursor: the position is marked `pending_event` once, the reason is kept as the bar's expiry reason, and the bar
   is decided later or spanned at its cap (§B5.3). A bar pending only on a guard (`<kind>_pending`: its broker bar
   is final) is not spanned at its cap: it is decided once there, so the guard's timeout and `unknown_policy` apply
   (§B6.1, §B6.5: bar 0 ends `FIRST_BAR_UNKNOWN(timeout)`, Q3), and it never raises the post_grace page, which is
   for a bar still awaiting the broker (§B5.2). Only if that decision is not final is the bar spanned next pass.
4. **PRE** of the forming bar `T` at local `T + p50 − pre_offset`, valid until `fire + 10 s`
   (S38c: a 10 s window at every allowed offset); never on a bar of a closed period; with PR9 on it waits for its stop read (`src.riskmgr.stopreads`) and a
   position armed after that read was queued gets no PRE that bar. PRE rows never move the cursor or the SM11 state.
   `run` sleeps only until the next PRE fire when that comes before the next 250 ms pass, and a `pre_offset` outside
   the timing row (30-45 s) is refused at construction, as `XTNL_ARM_PRE_OFFSET_S` refuses it.
   While paused no stop read is queued.
5. **Reads before `BEGIN`.** Every market-data read happens in `EvaluationPort.prepare` (the S21 join, wired by S31
   once the user has confirmed the §9.2 Oversee change and C1; this module imports no S21 module, as S22 did not); the
   md generation is read before and after the reads of one round, and a change discards them all (§A13 read
   sessions). The evaluation itself is the prepared job, run through the S22 `JobRunner` (single-thread executor per
   pinned version, 250 ms cap, `STRATEGY_ERROR` on a fault).
6. **One fenced transaction per (event, bar end)** through the fixed sink chain (`src.riskmgr.sinks`): each position
   in its own savepoint; a CAS miss, an `ALREADY_DECIDED` (a replayed POST: no `EpochMismatch`, no duplicate) or a
   transient error drops only that position from the pass and re-reads its row. A fatal error (the lost fence,
   recognised by the injected `fatal` predicate, by default `src.db.arm_ports.is_lost_fence`) propagates so the caller
   hard-exits. A final result whose SM11 events the row's state refuses is not final (`sm11_refused`: no row, the
   bar stays pending and is re-decided or spanned at its cap).

7. **Correction re-checks (D4, round 13, plan S38d).** Every final bar-end decision this term committed (POST, LATE,
   CORRECTED) is kept with its recorded inputs (ids and keyed hashes) until its bar's `decision_cap`. Every
   `correction_every_ms` (5 s, not while paused or with the controls unreadable) the current hash of each input is
   read: series inputs through `MarketDataPort.events_since` (the restart-form cursor at the oldest recorded bar end
   of the series; the newest event per bar end), or, when the cursor has expired or the read fails, by bucket
   (`bars(at_bucket_ms)`, hashed under the market-data keyring); indicator inputs as their current value's lineage
   hash. `corrections.check_decision` decides: same id and a different hash on a final input is a `CORRECTION`;
   anything unverifiable is `RETRY` (an `UNVERIFIABLE` audit row when its window ends, `OpenChecks`). A correction
   re-runs the evaluation as a `CORRECTED` event on the current reads (for a release on that bar, from the state
   before the release: `TRACKING`), and `corrections.plan_revision` writes `revision + 1` with
   `supersedes_decision_id` in one fenced transaction: an older bar's revision is INFO only; the latest bar's
   re-proposes an `ADJUST`, releases, or withdraws a release made on it (`RELEASE_WITHDRAWN`: the position is managed
   again). A revision whose outcome, trigger and first-bar verdict did not change is stored with the `suppressed`
   sentinel. **After a restart or a takeover** (go-live gate `corrections_restart`, plan S39b) the first correction
   pass rebuilds the kept set from the committed final decisions still inside their `decision_cap` window
   (`RecentDecisionReader`, their stored input hashes): memory only, nothing re-sent; a correction found then is a
   new `CORRECTED` revision as within one term.
8. **PC sleep** (go-live gate `uptime_suspend`, plan S39b): a suspend seen by the detector or the latch is passed to
   the job runner (`JobRunner.note_suspended`); a job whose run spanned it is never a timeout or a strategy fault
   (`job_suspended`: the bar stays pending with `DOWNTIME` as its reason), and every bar that ended while the process
   was suspended, or whose `decision_cap` passed while it was suspended, has `DOWNTIME` as its expiry reason, so it is
   spanned `DOWNTIME` at its cap.

Every state change goes through `src.riskmgr.sm11.steps`; the cursor moves only as `replay.advance` says. This task
never awaits broker I/O (stop reads are tasks of `StopReads`). Logs and pages carry ids, codes, counts and times
only: never a level, a price or a units figure. The error budget (S30) consumes what this task records.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Final, Protocol

from src.db.arm_ports import (
    TERMINAL_ARM_STATES,
    ArmPositionRow,
    ArmStateStore,
    ArmStateTxn,
    ControlsRow,
    CorrectionAuditRow,
    DecisionRef,
    DecisionHandleRow,
    DecisionRow,
    DecisionWriteStatus,
    PositionCas,
    RecentDecisionReader,
    is_lost_fence,
    position_ref,
)
from src.marketdata.clockstep import SUSPEND, WallMonoDetector
from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.ports import BarsRead, EventsRead, IndicatorRead, MarketDataPort, StampPort
from src.marketdata.timing import (
    TIMING_TABLE,
    SkewEstimate,
    decision_cap_ms,
    local_bar_end_ms,
    post_grace_end_ms,
    pre_fire_ms,
    pre_offset_problem,
    pre_valid_until_ms,
    timing_for,
)
from src.marketdata.types import BarSpec, DataUnavailable, EventCursor, MdGeneration, SeriesId
from src.riskmgr.config import ArmSettings
from src.riskmgr.corrections import (
    CheckVerdict,
    CorrectionCheck,
    CurrentInput,
    DecisionInput,
    InputId,
    OpenChecks,
    RecordedDecision,
    RevisionKind,
    check_decision,
    input_id,
    plan_revision,
    recorded_inputs,
    summary_of,
)
from src.riskmgr.envelope import stamp
from src.riskmgr.frontier import FrontierView
from src.riskmgr.jobs import JOB_SUSPENDED, JobRunner, JobSpec
from src.riskmgr.ordering import advice, next_decision_seq
from src.riskmgr.outcomes import (
    DOWNTIME,
    FAILED,
    PAUSED,
    RELEASE_UNVERIFIED,
    STOP_STALE,
    Outcome,
    is_guard_pending,
)
from src.riskmgr.ports import Clock, Pager
from src.riskmgr.probes import PASSED, ProbeResult, refused_probe
from src.riskmgr.replay import (
    REOPEN_PENDING,
    BarSlot,
    Decide,
    DropRecheck,
    Frontier,
    Hold,
    ReopenCheck,
    Span,
    Wait,
    advance,
    next_step,
    reopen_check,
)
from src.riskmgr.sinks import Decided, SinkChain, WriteStatus, Written, default_sinks
from src.riskmgr.sm11 import MANAGED, ArmState, Refused, Sm11Event, Transition, steps
from src.riskmgr.stopreads import ReadBook, StopReads, StopReadView
from src.riskmgr.tracker import BarGridPort
from src.riskmgr.types import ArmEvent, IndicatorInput, SeriesInput, lineage_hash

log = logging.getLogger(__name__)

TASK: Final = "arm"
CONTROLS_UNREADABLE: Final = "controls_unreadable"
AWAITING_STOP_READ: Final = STOP_STALE
"""A ready bar-end item waiting for its PR9 read keeps `stop_stale` as its expiry reason."""
DELAYED: Final = "delayed"
SPAN_FIRST_BAR_STALE: Final = "first_bar_unknown_stale"
PRE_EXPIRED: Final = "pre_expired"
PRE_READ_MISSING: Final = "pre_stop_read_missing"
ARMED_AFTER_PRE_READ: Final = "armed_after_pre_read"
PRE_MARKET_CLOSED: Final = "market_closed"
MD_UNAVAILABLE: Final = "md_unavailable"
PREPARE_FAILED: Final = "prepare_failed"
PREPARE_TIMEOUT: Final = "prepare_timeout"
POSTGRES_EXEMPT: Final = "postgres"
NOT_FINAL: Final = "not_final"
NO_ITEM: Final = "no_item"
SM11_REFUSED: Final = "sm11_refused"
"""A final result whose SM11 events the row's state refuses: not final (no row, the bar stays pending)."""
UNRESOLVED_RANGE: Final = "UNRESOLVED_RANGE"
KEEP_MS: Final = 2 * 3_600_000
REBUILD_LIMIT: Final = 2_000
"""At most this many committed decisions are read back to rebuild the correction window at a term start (S39b)."""
DECIDING_STATES: Final = MANAGED | frozenset({ArmState.STRATEGY_COMPLETE})
SPAN_WRITE_FAILED: Final = "span_write_failed"
SPAN_ROW_CHANGED: Final = "span_row_changed"
SPAN_ALREADY_DECIDED: Final = "span_already_decided"
SPAN_ROWS_UNREADABLE: Final = "span_rows_unreadable"
INPUT_UNREADABLE: Final = "input_unreadable"
RERUN_NOT_READY: Final = "rerun_not_ready"
NOT_DECIDING: Final = "not_deciding"
"""Correction compare reasons (codes only): an input's current hash could not be read; the re-run was not final; a
confirmed correction could not be applied because the position is not in a deciding state (FAILED, SUSPENDED, ...;
kept open and retried, an `UNVERIFIABLE(not_deciding)` audit row when its window ends)."""
REOPEN_RETRY: Final = "reopen_retry"
"""A REOPEN attempt whose result does not end the re-check (§B6.3 REOPEN): retried, no row (a code only)."""
REOPEN_ENDING: Final = frozenset({Outcome.ADJUST.value, Outcome.COMPLETE.value})
"""The REOPEN outcomes that end the re-check (§B6.3 REOPEN; a closed position and the first post-open POST end it
through the planner)."""


def ends_recheck(outcome: str) -> bool:
    """A final REOPEN result with this outcome ends the re-check (§B6.3 REOPEN): `ADJUST` or `COMPLETE` only."""
    return outcome in REOPEN_ENDING


CORRECTABLE: Final = frozenset({ArmEvent.POST, ArmEvent.LATE, ArmEvent.CORRECTED})
RERUN_EVENTS: Final = frozenset({Sm11Event.FINAL_ADJUST, Sm11Event.ADVICE_WITHDRAWN, Sm11Event.HANDLES_REACHED})
"""Of a correction's re-run, the SM11 events re-applied with its `commit_state` (the latest bar only)."""
"""Checks of the `span_write` clear probe (`BarClock.test_span_write`, S38c review; codes only)."""
"""States the BarClock decides (a released position is evaluated silently, §B6.3 (4))."""


# ---------------------------------------------------------------------------------------------------- ports
class ControlsPort(Protocol):
    async def controls(self) -> ControlsRow | None:
        """The `arm_controls` singleton (`ArmConfigStore.controls`); None or a raise = unreadable."""
        ...


class SessionCloseView(Protocol):
    def next_open_ms(self, adapter_id: str, symbol: str, bar_end_ms: int) -> int | None:
        """For a pre-close bar (its end starts a daily break or the weekly close): the local time of the next open;
        None for any other bar (S31, from the calendar)."""
        ...


class PostNoticeView(Protocol):
    def post_sent(self, row: ArmPositionRow, bar_spec: str, bar_end_ms: int) -> bool:
        """The route's POST notice for this bar has gone, so a decision now is `LATE` (S28)."""
        ...


@dataclass(frozen=True, kw_only=True)
class EvalRequest:
    """One evaluation the BarClock asks for. `fire_ms` / `valid_until_ms` only at PRE (the quote is frozen at fire
    time); `stop_reads` the PR9 view (empty with PR9 off); `generation` the md generation of this read session."""

    row: ArmPositionRow
    spec: BarSpec
    event: ArmEvent
    bar_end_ms: int
    now_ms: int
    skew: SkewEstimate
    decision_cap_ms: int
    generation: MdGeneration
    fire_ms: int | None = None
    valid_until_ms: int | None = None
    delayed: bool = False
    previous_post_pending: bool = False
    stop_reads: StopReadView = field(default_factory=StopReadView)


@dataclass(frozen=True)
class NotReady:
    """The inputs do not exist yet (the broker's final bar, its indicators, a series not LIVE): `reason` is what an
    expiry of this bar records (`broker_bar_not_final`, a `DATA_UNAVAILABLE` reason, ...)."""

    reason: str


class Verdict(str, Enum):
    DECISION = "DECISION"
    PENDING = "PENDING"
    NO_ITEM = "NO_ITEM"


@dataclass(frozen=True, kw_only=True)
class Evaluated:
    """What one evaluation job returns. `decision`: the row's outcome fields (the BarClock sets the key, bar, event,
    `decision_seq`, `decided_at_ms`, `advice` and the pinned version fields); `final`: it decides the bar (POST /
    LATE move the cursor with `trigger_hash`; a REOPEN `ADJUST` or `COMPLETE` ends its re-check, any other final
    REOPEN result is retried and only the last one is written when the re-check ends, §B6.3); `sm11_events` in order
    (none at PRE);
    `strategy_state_json` None = unchanged; `pre_basis_change` writes `pre_basis_json` / `pre_basis_bar` as given
    (None clears); `reopen_candidate`: a final POST that came out `MARKET_CLOSED` with `trigger_met`."""

    verdict: Verdict
    reason: str = ""
    decision: DecisionRow | None = None
    handles: tuple[DecisionHandleRow, ...] = ()
    final: bool = False
    trigger_hash: str | None = None
    sm11_events: tuple[Sm11Event, ...] = ()
    strategy_state_json: str | None = field(default=None, repr=False)
    pre_basis_change: bool = False
    pre_basis_json: str | None = field(default=None, repr=False)
    pre_basis_bar: int | None = None
    reopen_candidate: bool = False
    indicator_tf: tuple[tuple[str, int], ...] = ()
    """Per indicator id the decision read, its own bar timeframe (ms): the bucket of a recorded indicator input is
    its bar end minus this, never minus the decision's timeframe (round 13 review)."""
    shown: bool = True
    """False = a silent decision (the pipeline's `shown`, §B6.3): the notice sink suppresses it (S28)."""
    committed: Callable[[], None] | None = field(default=None, repr=False, compare=False)
    """Run on the event loop once this result's decision row has committed (`Decided.on_commit`): how an evaluation
    port keeps memory without changing it from the job thread (a timed-out job, a CAS miss or a rolled-back
    transaction never reaches it)."""

    def __post_init__(self) -> None:
        if (self.verdict is Verdict.DECISION) != (self.decision is not None):
            raise ValueError("a DECISION carries a decision row and nothing else does")


@dataclass(frozen=True)
class Prepared:
    """Every market-data read is done: `job` is the pure evaluation, run on its version's executor."""

    spec: JobSpec
    job: Callable[[], Evaluated]


class EvaluationPort(Protocol):
    async def prepare(self, request: EvalRequest) -> Prepared | NotReady:
        """Do every market-data read of one evaluation (before any transaction) and return the job, or `NotReady`."""
        ...


# ---------------------------------------------------------------------------------------------------- settings
@dataclass(frozen=True, kw_only=True)
class BarClockSettings:
    pass_every_ms: int = 250
    controls_ttl_ms: int = 1_000
    rows_ttl_ms: int = 1_000
    pause_page_after_ms: int = 6 * 3_600_000
    reopen_retry_ms: int = 5_000
    max_rounds: int = 8
    prepare_concurrency: int = 8
    """Prepares (market-data reads) run this many at a time; the stamp is refreshed after each group and job."""
    prepare_timeout_ms: int = 2_000
    """A prepare still running after this long is `prepare_timeout` for that ask (a hung read never outlives the
    task's 5 s stamp: only this task's own PostgreSQL waits are exempt, operations.md §3)."""
    generation_timeout_ms: int = 1_000
    """The md generation read of a read session; a hang counts as unreadable (`md_unavailable`)."""
    correction_every_ms: int = 5_000
    """The correction re-check cadence (D4, round 13)."""
    suspend_after_ms: int | None = None
    """The clock detector's suspend trigger (round 14): both clocks moved more than this between two passes. The
    runtime passes `clockstep.SUSPEND_AFTER_MS` (10 s, 40 passes); None = steps only (unit rigs that jump the fake
    clock by minutes between passes)."""


@dataclass
class BarClockCounters:
    passes: int = 0
    errors: int = 0
    clock_steps: int = 0
    suspends: int = 0
    lease_holds: int = 0
    controls_unreadable: int = 0
    paused_passes: int = 0
    config_paused_passes: int = 0
    generation_discards: int = 0
    decisions: int = 0
    pre_decisions: int = 0
    spans: int = 0
    pending: int = 0
    job_faults: int = 0
    jobs_suspended: int = 0
    write_errors: int = 0
    cas_miss: int = 0
    already_decided: int = 0
    sm11_refused: int = 0
    correction_checks: int = 0
    corrections: int = 0
    corrections_shown: int = 0
    correction_retries: int = 0
    correction_errors: int = 0
    correction_audits: int = 0
    corrections_not_deciding: int = 0
    corrections_rebuilt: int = 0
    pre_dropped: dict[str, int] = field(default_factory=dict)

    def drop(self, reason: str) -> None:
        self.pre_dropped[reason] = self.pre_dropped.get(reason, 0) + 1


@dataclass
class _Pos:
    row: ArmPositionRow
    spec: BarSpec
    skew: SkewEstimate

    @property
    def ref(self) -> str:
        return self.row.ref


class _ProbeRollback(Exception):
    """Ends the clear probe's transaction so its span write is rolled back (carries the write's status)."""

    def __init__(self, status: DecisionWriteStatus) -> None:
        super().__init__(status.value)
        self.status = status


@dataclass(frozen=True)
class _Ask:
    """A position whose step needs an evaluation this round."""

    pos: _Pos
    event: ArmEvent
    bar_end_ms: int


@dataclass(frozen=True)
class _Recorded:
    """A committed final bar-end decision kept for its correction window (ids, codes and keyed hashes only)."""

    ref: str
    adapter_id: str
    position_id: str
    spec: BarSpec
    rec: RecordedDecision
    window_end_ms: int


# ---------------------------------------------------------------------------------------------------- the task
class BarClock:
    """The `arm` task of one term (see the module docstring)."""

    def __init__(self, *, store: ArmStateStore, controls: ControlsPort, md: MarketDataPort, grid: BarGridPort,
                 evaluator: EvaluationPort, jobs: JobRunner, pager: Pager, clock: Clock, settings: ArmSettings,
                 lease_valid: Callable[[], bool], stop_reads: StopReads | None = None,
                 sinks: SinkChain | None = None, closes: SessionCloseView | None = None,
                 notices: PostNoticeView | None = None, suspended: Callable[[], bool] | None = None,
                 fatal: Callable[[BaseException], bool] | None = None,
                 barclock: BarClockSettings | None = None,
                 config_pause: Callable[[], str | None] | None = None,
                 keyring: Callable[[], Keyring | None] | None = None,
                 recent: RecentDecisionReader | None = None) -> None:
        self._store = store
        self._controls_port = controls
        self._md = md
        self._grid = grid
        self._evaluator = evaluator
        self._jobs = jobs
        self._pager = pager
        self._clock = clock
        self._arm = settings
        self._lease_valid = lease_valid
        for spec in TIMING_TABLE:                     # at 30 s the PRE window is empty (valid until == fire)
            problem = pre_offset_problem(spec, settings.pre_offset_s)
            if problem is not None:
                raise ValueError(problem)
        self._fatal = fatal or is_lost_fence
        self._reads = stop_reads or StopReads(None, None, enabled=False, pre_offset_s=settings.pre_offset_s)
        self._sinks = sinks or default_sinks(fatal=self._fatal)
        self._closes = closes
        self._notices = notices
        self._suspended = suspended or (lambda: False)
        self._config_pause = config_pause or (lambda: None)
        self._cfg = barclock or BarClockSettings()
        self.counters = BarClockCounters()
        self._detector = WallMonoDetector(suspend_after_ms=self._cfg.suspend_after_ms)
        self._controls: tuple[int, ControlsRow] | None = None
        self._rows: list[ArmPositionRow] = []
        self._rows_at: int | None = None
        self._reason: dict[tuple[str, int], str] = {}
        """The reason each bar was last pending with while the ARM ran (its expiry reason)."""
        self._cap_tried: set[tuple[str, int]] = set()
        """Bars tried at or after their `decision_cap` that stayed undecided: a guard wait is then spanned, never
        retried (the frontier never sticks; live review round 2)."""
        self._pre_done: set[tuple[str, int]] = set()
        self._pre_wait: dict[tuple[str, int], str] = {}
        self._reopen_last: dict[str, int] = {}
        self._reopen_hold: dict[str, tuple[int, Evaluated]] = {}
        """Per position, the last retryable REOPEN result (a final HOLD or any other result that does not end the
        re-check) with its bar end: written and sent when the re-check ends without an ADJUST or COMPLETE (§B6.3
        REOPEN, "on expiry the last HOLD is sent with its reason"). Memory only: after a restart nothing is held."""
        self._grace_paged: set[tuple[str, str, str]] = set()
        self._span_paged: set[str] = set()
        self._pause_paged: int | None = None
        self._next_fire: int | None = None
        """The earliest PRE fire still ahead (local ms), so `run` wakes for it instead of up to 250 ms late."""
        self._stamp: Callable[[], None] = _no_stamp
        self._exempt: Callable[[str | None], None] = _no_exempt
        self._pg_depth = 0
        self._keyring = keyring or (lambda: None)
        self._recorded: dict[tuple[str, int], _Recorded] = {}
        """Final bar-end decisions inside their correction window, by (position ref, bar end) (round 13)."""
        self._checks = OpenChecks()
        self._recent = recent
        self._rebuilt = recent is None
        """The correction window was rebuilt from the committed decisions at this term's start (gate
        `corrections_restart`, plan S39b); retried every correction cadence until one read succeeds."""
        self._corrections_at: int | None = None
        self._ind_tf: dict[str, int] = {}
        """Each recorded indicator's own bar timeframe (ms), learned from the evaluations (`Evaluated.indicator_tf`)."""
        self._awake_ms: int | None = None
        """The wall time of the last pass the clock detector let through (the start of a later suspend, S39b)."""
        self.last_pass_ms: int | None = None
        """The start of the last pass that got through the clock and lease holds to its bar-end pass (the liveness
        watch's stalled-loop page and its overdue confirmation, plan S38g); a pass held by a clock step, a suspend or
        the lease never sets it."""

    @property
    def stop_reads(self) -> StopReads:
        return self._reads

    # ------------------------------------------------------------------------------------------------ loop
    async def run(self, stop: asyncio.Event, stamp_port: StampPort) -> None:
        while not stop.is_set():
            stamp_port.stamp()
            await self.guarded_step(stamp=stamp_port.stamp, exempt=stamp_port.set_exempt)
            stamp_port.stamp()
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.sleep_ms(self._clock.now_ms()) / 1000)
            except TimeoutError:
                pass

    async def guarded_step(self, stamp: Callable[[], None] | None = None,
                           exempt: Callable[[str | None], None] | None = None) -> None:
        """One pass as `run` makes it: an error is counted, logged by class and the rows re-read on the next pass;
        a fatal one (a lost fence) propagates. The `arm` task loop of the runtime calls this (plan S36b)."""
        try:
            await self.step(stamp=stamp, exempt=exempt)
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.errors += 1
            self._rows_at = None
            log.warning("%s: pass failed (%r)", TASK, type(e))

    def sleep_ms(self, now_ms: int) -> int:
        """Until the next pass: 250 ms, or less when a PRE fires sooner (a pass then lands on the fire time, so a
        short PRE window is never missed between two passes)."""
        every = self._cfg.pass_every_ms
        fire = self._next_fire
        if fire is None or fire <= now_ms:
            return every
        return max(1, min(every, fire - now_ms))

    def live_refs(self) -> set[str]:
        """The position refs of the rows read by the last pass."""
        return {r.ref for r in self._rows}

    def frontier_views(self, now_ms: int) -> list[FrontierView]:
        """The stuck-frontier views of the rows read by the last pass (S30 `ErrorBudget.observe_frontier`, S36b):
        per non-terminal position (by `adapter_id:position_id`) the oldest bar after its cursor whose `decision_cap` has passed. A position is
        `held` (never counted, §B13 / §B14) while it is not decided here (FAILED, SUSPENDED, ...) or while either
        pause holds (the operator pause as last read, the config-hash pause; a raising pause port is a pause)."""
        cached = self._controls
        paused = cached is not None and cached[1].paused
        try:
            paused = paused or self._config_pause() is not None
        except Exception:
            paused = True
        out: list[FrontierView] = []
        for row in self._rows:
            state = self._state(row)
            if state is None or state.value in TERMINAL_ARM_STATES:
                continue
            try:
                spec = BarSpec.parse(row.bar_spec)
            except ValueError:
                continue
            if timing_for(spec) is None:
                continue
            p = _Pos(row, spec, self._grid.skew(row.key.adapter_id))
            ends = self._grid.open_bar_ends(row.key.adapter_id, row.symbol, spec, self._frontier(p).cursor_ms,
                                            now_ms - p.skew.p50_ms)
            overdue = next((e for e in ends if decision_cap_ms(e, spec, p.skew) < now_ms), None)
            out.append(FrontierView(position_id=row.ref, overdue_bar_ms=overdue,
                                    held=paused or state not in DECIDING_STATES))
        return out

    def close(self) -> bool:
        """Stop the stop-read tasks and the job executors. True when a job thread is still hung: the caller must
        then end the process through the hard exit (`src.runtime.exits.hard_exit`, S31)."""
        self._reads.close()
        return self._jobs.close()

    async def step(self, stamp: Callable[[], None] | None = None,
                   exempt: Callable[[str | None], None] | None = None) -> None:
        """One pass. `stamp` refreshes the task's liveness stamp between slow parts (prepares, jobs); `exempt` (the
        task's `set_exempt`) is set to `postgres` only while this task awaits a PostgreSQL call of its own (the
        controls and rows reads, the transactions; operations.md §3). Every other wait is bounded instead: the
        prepares (`prepare_timeout_ms`), the md generation reads (`generation_timeout_ms`), the jobs (250 ms cap)."""
        self._stamp = stamp or _no_stamp
        self._exempt = exempt or _no_exempt
        self._pg_depth = 0
        self._next_fire = None
        now = self._clock.now_ms()
        self.counters.passes += 1
        stepped = self._detector.observe(now, self._clock.monotonic_ms())
        suspended = self._suspended()
        if stepped is not None or suspended:
            self.counters.clock_steps += 1
            if suspended or (stepped is not None and stepped.kind == SUSPEND):
                self.counters.suspends += 1
                self._missed_while_suspended(now)
            self._controls = None
            self._rows_at = None
            return
        self._awake_ms = now
        if not self._lease_valid():
            self.counters.lease_holds += 1
            return
        controls = await self._read_controls(now)
        positions = await self._positions(now)
        config_paused = self._config_paused()
        if (controls is not None and controls.paused) or config_paused:
            self.counters.paused_passes += 1
            if controls is not None and controls.paused:
                self._page_pause(controls, now)
            if config_paused:
                self.counters.config_paused_passes += 1
            await self._bar_end_pass(positions, now, hold_all=PAUSED, unreadable=False)
            self._prune(now)
            self.last_pass_ms = now
            return                                    # paused: no stop reads either (nothing is decided)
        self._reads.tick(now, self._books(positions, now))
        self._pause_paged = None
        unreadable = controls is None
        if unreadable:
            self.counters.controls_unreadable += 1
        await self._bar_end_pass(positions, now, hold_all=None, unreadable=unreadable)
        await self._pre_pass(positions, now, unreadable=unreadable)
        if not unreadable:
            await self._correction_pass(positions, now)
        self._prune(now)
        self.last_pass_ms = now

    def _missed_while_suspended(self, now: int) -> None:
        """A suspend (the uptime tracker's signal or this detector's, gate `uptime_suspend`, plan S39b): every strategy
        job running now spanned it (`JobRunner.note_suspended`: never counted as a timeout or a fault), and the bars
        that ended while the process was suspended were missed: their expiry reason is `DOWNTIME`, whatever an earlier
        pass held them for, so a bar the sleep covered is spanned `DOWNTIME` at its cap. So is a bar that ended
        before the sleep but whose whole deciding window (its `decision_cap`) passed during it."""
        self._jobs.note_suspended()
        since = self._awake_ms
        if since is None:
            return
        caps: dict[str, tuple[BarSpec, SkewEstimate]] = {}
        for row in self._rows:
            try:
                caps[row.ref] = (BarSpec.parse(row.bar_spec), self._grid.skew(row.key.adapter_id))
            except ValueError:
                continue

        def covered(key: tuple[str, int]) -> bool:
            if since < key[1] <= now:
                return True
            got = caps.get(key[0])
            return got is not None and since < decision_cap_ms(key[1], got[0], got[1]) <= now

        for key in [k for k in self._reason if covered(k)]:
            self._reason[key] = DOWNTIME

    # ------------------------------------------------------------------------------------------------ inputs
    @contextlib.contextmanager
    def _postgres(self) -> Iterator[None]:
        """Exempt from the stamp age while this task awaits PostgreSQL inside the block (nested blocks keep it);
        leaving the outermost block clears the exemption, which stamps."""
        self._pg_depth += 1
        if self._pg_depth == 1:
            self._exempt(POSTGRES_EXEMPT)
        try:
            yield
        finally:
            self._pg_depth -= 1
            if self._pg_depth == 0:
                self._exempt(None)

    def _config_paused(self) -> bool:
        """The config-hash pause (§B13) holds; a port that raises counts as paused (fail closed)."""
        try:
            return self._config_pause() is not None
        except Exception as e:
            if self._fatal(e):
                raise
            return True

    async def _read_controls(self, now: int) -> ControlsRow | None:
        cached = self._controls
        if cached is not None and now - cached[0] < self._cfg.controls_ttl_ms:
            return cached[1]
        try:
            with self._postgres():
                row = await self._controls_port.controls()
        except Exception as e:
            if self._fatal(e):
                raise
            row = None
        if row is None:
            self._controls = None
            return None
        self._controls = (now, row)
        return row

    async def _positions(self, now: int) -> list[_Pos]:
        if self._rows_at is None or now - self._rows_at >= self._cfg.rows_ttl_ms:
            with self._postgres():
                self._rows = list(await self._store.live_positions())
            self._rows_at = now
        out: list[_Pos] = []
        for row in self._rows:
            try:
                spec = BarSpec.parse(row.bar_spec)
            except ValueError:
                continue
            if timing_for(spec) is None:
                continue
            out.append(_Pos(row, spec, self._grid.skew(row.key.adapter_id)))
        return out

    def _books(self, positions: Sequence[_Pos], now: int) -> list[ReadBook]:
        books: dict[str, ReadBook] = {}
        for p in positions:
            if p.row.key.adapter_id in books:
                continue
            last = p.spec.bucket_start_ms(now - p.skew.p50_ms)
            books[p.row.key.adapter_id] = ReadBook(p.row.key.adapter_id, p.skew, last,
                                                   last + p.spec.timeframe_ms,
                                                   decision_cap_ms(last, p.spec, p.skew))
        return list(books.values())

    def _page_pause(self, controls: ControlsRow, now: int) -> None:
        since = controls.paused_at_ms
        if since is None or now - since < self._cfg.pause_page_after_ms or self._pause_paged == since:
            return
        self._pause_paged = since
        self._pager.page("warning", "ARM: paused for more than 6 h",
                         f"paused since {since}; no ARM decisions until arm resume")

    # ------------------------------------------------------------------------------------------------ bar-end events
    def _state(self, row: ArmPositionRow) -> ArmState | None:
        try:
            return ArmState(row.state)
        except ValueError:
            return None

    def _frontier(self, p: _Pos) -> Frontier:
        row = p.row
        if row.last_decided_bar is None:
            return Frontier(p.spec.bucket_start_ms(row.entry_time_ms))
        return Frontier(row.last_decided_bar, row.last_decided_hash)

    def _bar0_end(self, p: _Pos) -> int:
        return p.spec.bucket_start_ms(p.row.entry_time_ms) + p.spec.timeframe_ms

    def _next_open(self, p: _Pos, bar_end_ms: int) -> int | None:
        return None if self._closes is None else self._closes.next_open_ms(p.row.key.adapter_id, p.row.symbol,
                                                                           bar_end_ms)

    def _plan(self, p: _Pos, now: int, hold: str | None, not_due: set[str]) -> Span | Decide | Wait | Hold | \
            DropRecheck | None:
        frontier = self._frontier(p)
        ends = list(self._grid.open_bar_ends(p.row.key.adapter_id, p.row.symbol, p.spec, frontier.cursor_ms,
                                             now - p.skew.p50_ms))
        if hold is not None:
            for e in ends:
                self._reason.setdefault((p.ref, e), hold)
            return next_step(frontier, (), now, spec=p.spec, skew=p.skew, hold=hold)
        slots = [self._slot(p, e) for e in ends]
        rechecks: list[tuple[ReopenCheck, bool]] = []
        if p.row.reopen_bar_end_ms is not None:
            nxt = self._next_open(p, p.row.reopen_bar_end_ms)
            if nxt is not None and nxt > p.row.reopen_bar_end_ms:
                last = self._reopen_last.get(p.ref)
                due = p.ref not in not_due and (last is None or now - last >= self._cfg.reopen_retry_ms)
                rechecks.append((reopen_check(p.row.reopen_bar_end_ms, nxt, p.spec, p.skew), due))
        return next_step(frontier, slots, now, spec=p.spec, skew=p.skew, bar0_end_ms=self._bar0_end(p),
                         rechecks=rechecks)

    def _slot(self, p: _Pos, e: int) -> BarSlot:
        """Bar `e` as the planner sees it. A guard's `<kind>_pending` is a final broker bar waiting only on the guard:
        at its cap it is decided by the guard's timeout (§B6.1, §B6.5), unless that was already tried."""
        reason = self._reason.get((p.ref, e), DOWNTIME)
        return BarSlot(e, ready=False, reason=reason, post_sent=self._post_sent(p, e),
                       next_open_ms=self._next_open(p, e),
                       guard_timeout=is_guard_pending(reason) and (p.ref, e) not in self._cap_tried)

    def _post_sent(self, p: _Pos, bar_end_ms: int) -> bool:
        return self._notices is not None and self._notices.post_sent(p.row, p.spec.name, bar_end_ms)


    # ------------------------------------------------------------------------------------------------ bar-end pass
    async def _bar_end_pass(self, positions: Sequence[_Pos], now: int, *, hold_all: str | None,
                            unreadable: bool) -> None:
        """Spans, POST / LATE and REOPEN items in rounds: a committed span or decision lets the position go on to its
        next bar in the same pass (bar n is decided or expired before bar n + 1)."""
        active: dict[str, _Pos] = {}
        for p in positions:
            state = self._state(p.row)
            if state is ArmState.FAILED:
                self._plan(p, now, FAILED, set())      # marks the bars it holds: a later span records `failed`
            elif state in DECIDING_STATES:
                active[p.ref] = p
        not_due: set[str] = set()
        for _ in range(self._cfg.max_rounds):
            if not active:
                return
            writes: dict[tuple[ArmEvent, int], list[Decided]] = {}
            go_on: set[str] = set()
            asks: list[_Ask] = []
            for ref, p in active.items():
                step = self._plan(p, now, hold_all, not_due)
                if isinstance(step, Span):
                    writes.setdefault((ArmEvent.POST, step.bar_end_ms), []).append(self._span_write(p, step, now))
                    go_on.add(ref)
                elif isinstance(step, DropRecheck):
                    writes.setdefault((ArmEvent.REOPEN, step.bar_end_ms), []).append(
                        self._drop_recheck(p, step.bar_end_ms, now))
                    self._reopen_last.pop(ref, None)
                    go_on.add(ref)
                elif isinstance(step, Decide) and not unreadable:
                    asks.append(_Ask(p, step.event, step.bar_end_ms))
                elif isinstance(step, Decide) and step.event is not ArmEvent.REOPEN:
                    # a guard wait at its cap with the controls unreadable: not decided, so spanned next pass
                    self._cap_tried.add((ref, step.bar_end_ms))
                elif isinstance(step, Wait) and step.reason != REOPEN_PENDING:
                    if unreadable:                       # an earlier reason (broker_bar_not_final) is kept
                        self._reason.setdefault((ref, step.bar_end_ms), CONTROLS_UNREADABLE)
                    else:
                        event = ArmEvent.LATE if self._post_sent(p, step.bar_end_ms) else ArmEvent.POST
                        asks.append(_Ask(p, event, step.bar_end_ms))
            retried: set[str] = set()
            for ask, res in await self._run_asks(asks, now) or ():
                r = self._bar_end_result(ask, res, now)
                if r.write is not None:
                    writes.setdefault((ask.event, ask.bar_end_ms), []).append(r.write)
                if r.go_on:
                    go_on.add(ask.pos.ref)
                if r.retry:
                    not_due.add(ask.pos.ref)
                    retried.add(ask.pos.ref)
            committed = await self._commit(writes, active, now)
            active = {ref: p for ref, p in active.items()
                      if ref in retried or (ref in go_on and ref in committed)}

    def _bar_end_result(self, ask: _Ask, res: Evaluated | str, now: int) -> _Result:
        if isinstance(res, str):
            return self._not_ready(ask, res, now)
        if res.verdict is not Verdict.DECISION or not res.final or res.decision is None:
            reason = res.reason or (res.decision.reason if res.decision is not None and res.decision.reason
                                    else NOT_FINAL)
            return self._not_ready(ask, reason, now)
        if ask.event is ArmEvent.REOPEN and not ends_recheck(res.decision.outcome):
            # §B6.3 REOPEN: only ADJUST, COMPLETE, a closed position or the first post-open bar's POST ends the
            # re-check; any other result (PRICE_THROUGH_LEVEL at the open, a market not open yet, ...) is retried
            # on the 5 s cadence, and the last one is written when the re-check ends (`_drop_recheck`)
            self._reopen_hold[ask.pos.ref] = (ask.bar_end_ms, res)
            return self._not_ready(ask, REOPEN_RETRY, now)
        transition = self._transition(ask.pos.row, res)
        if isinstance(transition, Refused):
            # the state cannot take this result: write nothing (a decision whose state did not move would disagree
            # with it for good); re-read the row and decide the bar again, or span it at its cap
            self.counters.sm11_refused += 1
            self._rows_at = None
            log.warning("%s: SM11 refused the %s of %s for %s (%s)", TASK, ask.event.value, ask.bar_end_ms,
                        ask.pos.ref, transition.reason)
            return self._not_ready(ask, SM11_REFUSED, now)
        return _Result(ask, write=self._final_write(ask, res, now, transition), go_on=True)

    def _drop_recheck(self, p: _Pos, bar_end_ms: int, now: int) -> Decided:
        """The re-check ends undecided (the planner's `DropRecheck`: the first post-open bar's POST was decided, or
        its `decision_cap` passed). The last retryable result of this re-check, when one is held, is written now as
        the REOPEN row and sent with its reason (§B6.3 REOPEN), in the same CAS that clears `reopen_bar_end_ms`. It
        changes no position state (no SM11 event, strategy state or basis: like every retried attempt it did not
        decide the re-check, and a later POST may already have decided the position). With nothing held (a restart
        since), only the re-check is cleared."""
        held = self._reopen_hold.get(p.ref)
        if held is None or held[0] != bar_end_ms or held[1].decision is None:
            cas = PositionCas(row=replace(p.row, reopen_bar_end_ms=None, updated_at_ms=now),
                              expected_row_version=p.row.row_version)
            return Decided(ref=p.ref, decision=None, position=cas)
        record = replace(held[1], sm11_events=(), strategy_state_json=None, pre_basis_change=False,
                         pre_basis_json=None, pre_basis_bar=None, reopen_candidate=False)
        return self._final_write(_Ask(p, ArmEvent.REOPEN, bar_end_ms), record, now, None)

    def _transition(self, row: ArmPositionRow, ev: Evaluated) -> Transition | Refused | None:
        """The SM11 result of a final evaluation; None when the stored state is unreadable (kept as it is)."""
        current = self._state(row)
        if current is None:
            return None
        return steps(current, ev.sm11_events, prior_state=_state_or_none(row.prior_state))

    def _not_ready(self, ask: _Ask, reason: str, now: int) -> _Result:
        """No row: a REOPEN attempt is retried (5 s cadence, not due for the rest of the pass); a POST / LATE stays
        pending with `reason` as its expiry reason and the position marked `pending_event` once."""
        p = ask.pos
        if ask.event is ArmEvent.REOPEN:
            self._reopen_last[p.ref] = now
            return _Result(ask, retry=True)
        self.counters.pending += 1
        self._reason[(p.ref, ask.bar_end_ms)] = reason
        if now >= decision_cap_ms(ask.bar_end_ms, p.spec, p.skew):
            self._cap_tried.add((p.ref, ask.bar_end_ms))   # tried at its cap: spanned next pass, never retried
        if reason != RELEASE_UNVERIFIED and not is_guard_pending(reason):
            # the broker bar is final for a held release or a guard's wait: neither is an awaited broker bar (§B5.2)
            self._grace_page(ask, now)
        if p.row.pending_event == ask.event.value and p.row.pending_bar_end_ms == ask.bar_end_ms:
            return _Result(ask)
        cas = PositionCas(row=replace(p.row, pending_event=ask.event.value, pending_bar_end_ms=ask.bar_end_ms,
                                      updated_at_ms=now), expected_row_version=p.row.row_version)
        return _Result(ask, write=Decided(ref=p.ref, decision=None, position=cas))

    def _grace_page(self, ask: _Ask, now: int) -> None:
        p = ask.pos
        row = timing_for(p.spec)
        if ask.event is not ArmEvent.POST or row is None or now < post_grace_end_ms(ask.bar_end_ms, row, p.skew):
            return
        series = (p.row.key.adapter_id, p.row.symbol, p.spec.name)
        if series in self._grace_paged:
            return
        self._grace_paged.add(series)
        self._pager.page("warning", "ARM: awaiting broker bar after post_grace",
                         f"series {':'.join(series)}; bar end {ask.bar_end_ms}; POST waits until decision_cap")

    def _decision_row(self, p: _Pos, base: DecisionRow, event: ArmEvent, bar_end_ms: int, seq: int, now: int,
                      extra_caveats: tuple[str, ...] = ()) -> DecisionRow:
        row = p.row
        caveats = tuple(dict.fromkeys((*base.caveats, *extra_caveats)))
        return replace(base, key=row.key, bar_end_ms=bar_end_ms, bar_spec=p.spec.name, event=event.value,
                       decision_seq=seq, decided_at_ms=now, edge_code=row.edge_code, advice=advice(event),
                       caveats=caveats, strategy_kind=row.strategy_kind,
                       strategy_impl_version=row.strategy_impl_version, guards_hash=row.guards_hash)

    def _final_write(self, ask: _Ask, ev: Evaluated, now: int, transition: Transition | None) -> Decided:
        p, row = ask.pos, ask.pos.row
        assert ev.decision is not None
        seq = next_decision_seq(row.decision_seq)
        timing = timing_for(p.spec)
        delayed = (ask.event is ArmEvent.POST and timing is not None
                   and now > post_grace_end_ms(ask.bar_end_ms, timing, p.skew))
        decision = self._decision_row(p, ev.decision, ask.event, ask.bar_end_ms, seq, now,
                                      (DELAYED,) if delayed else ())
        self._ind_tf.update(ev.indicator_tf)
        state, prior = row.state, row.prior_state
        if transition is not None:
            state = transition.state.value
            prior = None if transition.prior_state is None else transition.prior_state.value
        frontier = advance(self._frontier(p), Decide(ask.bar_end_ms, ask.event), decided_hash=ev.trigger_hash)
        if ask.event is ArmEvent.REOPEN:
            reopen: int | None = None
        elif ev.reopen_candidate and self._next_open(p, ask.bar_end_ms) is not None:
            reopen = ask.bar_end_ms
        else:
            reopen = row.reopen_bar_end_ms
        pending_done = row.pending_bar_end_ms is not None and row.pending_bar_end_ms <= frontier.cursor_ms
        new = replace(
            row, state=state, prior_state=prior,
            strategy_state_json=row.strategy_state_json if ev.strategy_state_json is None
            else ev.strategy_state_json,
            last_decided_bar=frontier.cursor_ms, last_decided_hash=frontier.last_hash, decision_seq=seq,
            pending_event=None if pending_done else row.pending_event,
            pending_bar_end_ms=None if pending_done else row.pending_bar_end_ms,
            reopen_bar_end_ms=reopen,
            pre_basis_json=ev.pre_basis_json if ev.pre_basis_change else row.pre_basis_json,
            pre_basis_bar=ev.pre_basis_bar if ev.pre_basis_change else row.pre_basis_bar,
            updated_at_ms=now)
        return Decided(ref=p.ref, decision=decision,
                       position=PositionCas(row=new, expected_row_version=row.row_version), handles=ev.handles,
                       shown=ev.shown, on_commit=ev.committed)

    def _span_write(self, p: _Pos, span: Span, now: int) -> Decided:
        decided = self._span_decided(p, span, now)
        if span.reason == STOP_STALE:
            self._reads.note_stale(p.ref, ArmEvent.POST, span.bar_end_ms)
        if span.pages and p.ref not in self._span_paged:
            self._span_paged.add(p.ref)
            self._pager.page("warning", "ARM: bars unresolved at decision_cap",
                             f"position {p.ref}; {span.bars} bar(s) to {span.to_bar_ms}; reason {span.reason}")
        return decided

    def _span_decided(self, p: _Pos, span: Span, now: int) -> Decided:
        """The span's decision row and position CAS (pure: the pass and the clear probe build the same write)."""
        row = p.row
        seq = next_decision_seq(row.decision_seq)
        env = stamp(self._arm.envelope)
        decision = DecisionRow(
            key=row.key, bar_end_ms=span.bar_end_ms, bar_spec=p.spec.name, event=ArmEvent.POST.value,
            decision_seq=seq, decided_at_ms=now, edge_code=row.edge_code, outcome=span.code.outcome.value,
            category=span.code.category.value, reason=span.reason, advice=advice(ArmEvent.POST, span=True),
            caveats=(SPAN_FIRST_BAR_STALE,) if span.first_bar_stale else (), envelope_version=env.version,
            envelope_params_hash=env.params_hash, strategy_kind=row.strategy_kind,
            strategy_impl_version=row.strategy_impl_version, guards_hash=row.guards_hash,
            range_from_bucket=span.from_bar_ms - p.spec.timeframe_ms)
        frontier = advance(self._frontier(p), span)
        pending_done = row.pending_bar_end_ms is not None and row.pending_bar_end_ms <= frontier.cursor_ms
        new = replace(row, last_decided_bar=frontier.cursor_ms, last_decided_hash=frontier.last_hash,
                      decision_seq=seq, pending_event=None if pending_done else row.pending_event,
                      pending_bar_end_ms=None if pending_done else row.pending_bar_end_ms,
                      reopen_bar_end_ms=span.to_bar_ms if span.reopen is not None else row.reopen_bar_end_ms,
                      updated_at_ms=now)
        return Decided(ref=p.ref, decision=decision,
                       position=PositionCas(row=new, expected_row_version=row.row_version))

    # ------------------------------------------------------------------------------------------------ clear probe
    async def test_span_write(self, position_ids: Sequence[str]) -> ProbeResult:
        """`SpanProbePort` (design §B14 `span_write`; S38c review): re-run the span write of each recorded position
        (`adapter_id:position_id`) on this task's store (the ARM pool), fenced, and roll it back. The row is re-read
        live; its overdue bars after the cursor are spanned as the pass will span them once the scope clears
        (reason `failed`); a position closed since, or with no bar past its cap, has nothing left to write. Passes
        only when every write would commit; a lost fence propagates (the caller hard-exits). Codes only."""
        now = self._clock.now_ms()
        try:
            rows = {r.ref: r for r in await self._store.live_positions()}
        except Exception as e:
            if self._fatal(e):
                raise
            return refused_probe(SPAN_ROWS_UNREADABLE)
        for ref in sorted(set(position_ids)):
            row = rows.get(ref)
            if row is None or row.state in TERMINAL_ARM_STATES:
                continue
            try:
                spec = BarSpec.parse(row.bar_spec)
            except ValueError:
                return refused_probe(SPAN_WRITE_FAILED)
            if timing_for(spec) is None:
                return refused_probe(SPAN_WRITE_FAILED)
            p = _Pos(row, spec, self._grid.skew(row.key.adapter_id))
            frontier = self._frontier(p)
            ends = self._grid.open_bar_ends(row.key.adapter_id, row.symbol, spec, frontier.cursor_ms,
                                            now - p.skew.p50_ms)
            slots = [BarSlot(e, ready=False, reason=FAILED, post_sent=False, next_open_ms=self._next_open(p, e))
                     for e in ends]
            step = next_step(frontier, slots, now, spec=spec, skew=p.skew, bar0_end_ms=self._bar0_end(p))
            if not isinstance(step, Span):
                continue                                # no bar past its cap: the frontier is not stuck
            write = self._span_decided(p, step, now)
            assert write.decision is not None
            try:
                async with self._store.transaction() as tx:
                    result = await tx.record_decision(write.decision, (), position=write.position)
                    raise _ProbeRollback(result.status)
            except _ProbeRollback as rollback:
                status = rollback.status
            except Exception as e:
                if self._fatal(e):
                    raise
                return refused_probe(SPAN_WRITE_FAILED)
            if status is DecisionWriteStatus.CAS_MISS:
                return refused_probe(SPAN_ROW_CHANGED)
            if status is not DecisionWriteStatus.INSERTED:
                return refused_probe(SPAN_ALREADY_DECIDED)
        return PASSED

    # ------------------------------------------------------------------------------------------------ PRE
    async def _pre_pass(self, positions: Sequence[_Pos], now: int, *, unreadable: bool) -> None:
        asks: list[_Ask] = []
        for p in positions:
            if self._state(p.row) not in DECIDING_STATES:
                continue
            timing = timing_for(p.spec)
            if timing is None:
                continue
            bar = p.spec.bucket_start_ms(now - p.skew.p50_ms) + p.spec.timeframe_ms
            key = (p.ref, bar)
            if key in self._pre_done:
                continue
            fire = pre_fire_ms(bar, p.skew, self._arm.pre_offset_s)
            until = pre_valid_until_ms(fire, bar, timing, p.skew)
            if now < fire:
                self._next_fire = fire if self._next_fire is None else min(self._next_fire, fire)
                continue
            if now > until:
                self._pre_done.add(key)
                reason = self._pre_wait.pop(key, PRE_EXPIRED)
                self.counters.drop(reason)
                if reason == PRE_READ_MISSING:
                    self._reads.note_stale(p.ref, ArmEvent.PRE, bar)
                continue
            ends = self._grid.open_bar_ends(p.row.key.adapter_id, p.row.symbol, p.spec, bar - p.spec.timeframe_ms,
                                            bar)
            if bar not in ends:
                self._pre_done.add(key)
                self.counters.drop(PRE_MARKET_CLOSED)
                continue
            if unreadable:
                self._pre_wait[key] = CONTROLS_UNREADABLE
                continue
            read = self._reads.pre_state(p.row.key.adapter_id, bar, p.ref)
            if read in ("not_covered", "not_queued"):
                self._pre_done.add(key)
                self.counters.drop(ARMED_AFTER_PRE_READ)
                continue
            if read == "pending":
                self._pre_wait[key] = PRE_READ_MISSING
                continue
            asks.append(_Ask(p, ArmEvent.PRE, bar))
        writes: dict[tuple[ArmEvent, int], list[Decided]] = {}
        for ask, res in await self._run_asks(asks, now) or ():
            key = (ask.pos.ref, ask.bar_end_ms)
            if isinstance(res, str):
                self._pre_wait[key] = res
                continue
            if res.verdict is Verdict.NO_ITEM:
                self._pre_done.add(key)
                self.counters.drop(res.reason or NO_ITEM)
                continue
            if res.verdict is Verdict.PENDING or res.decision is None:
                self._pre_wait[key] = res.reason or NOT_FINAL
                continue
            view = self._reads.view(ask.pos.row.key.adapter_id, ask.bar_end_ms)
            if self._reads.enabled and not view.fresh(ArmEvent.PRE, now):
                self._reads.note_stale(ask.pos.ref, ArmEvent.PRE, ask.bar_end_ms)
            writes.setdefault((ArmEvent.PRE, ask.bar_end_ms), []).append(self._pre_write(ask, res, now))
        if writes:
            await self._commit(writes, {a.pos.ref: a.pos for a in asks}, now)

    def _pre_write(self, ask: _Ask, ev: Evaluated, now: int) -> Decided:
        """A PRE row: `decision_seq` under the CAS; nothing else of the position changes (no cursor, no state)."""
        p, row = ask.pos, ask.pos.row
        assert ev.decision is not None
        seq = next_decision_seq(row.decision_seq)
        decision = self._decision_row(p, ev.decision, ArmEvent.PRE, ask.bar_end_ms, seq, now)
        new = replace(row, decision_seq=seq, updated_at_ms=now)
        return Decided(ref=p.ref, decision=decision,
                       position=PositionCas(row=new, expected_row_version=row.row_version), handles=ev.handles,
                       shown=ev.shown, on_commit=ev.committed)

    # ------------------------------------------------------------------------------------------------ reads, jobs
    async def _generation(self) -> MdGeneration | None:
        try:
            async with asyncio.timeout(self._cfg.generation_timeout_ms / 1000):
                got = await self._md.md_generation()
        except TimeoutError:
            return None
        except Exception as e:
            if self._fatal(e):
                raise
            return None
        return None if isinstance(got, DataUnavailable) else got

    def _request(self, ask: _Ask, now: int, generation: MdGeneration) -> EvalRequest:
        p, bar = ask.pos, ask.bar_end_ms
        timing = timing_for(p.spec)
        fire = until = None
        if ask.event is ArmEvent.PRE and timing is not None:
            fire = pre_fire_ms(bar, p.skew, self._arm.pre_offset_s)
            until = pre_valid_until_ms(fire, bar, timing, p.skew)
        delayed = (ask.event is ArmEvent.POST and timing is not None
                   and now > post_grace_end_ms(bar, timing, p.skew))
        previous_pending = (ask.event is ArmEvent.PRE
                            and self._frontier(p).cursor_ms < bar - p.spec.timeframe_ms)
        return EvalRequest(
            row=p.row, spec=p.spec, event=ask.event, bar_end_ms=bar, now_ms=now, skew=p.skew,
            decision_cap_ms=decision_cap_ms(bar, p.spec, p.skew), generation=generation, fire_ms=fire,
            valid_until_ms=until, delayed=delayed, previous_post_pending=previous_pending,
            stop_reads=self._reads.view(p.row.key.adapter_id, bar))

    async def _run_asks(self, asks: Sequence[_Ask], now: int) -> list[tuple[_Ask, Evaluated | str]] | None:
        """Every market-data read of this round (one read session: the md generation before and after), then the
        jobs. None when the generation changed: everything read is discarded and nothing is decided this round.

        A bar-end item asks for its PR9 stop read first: while that read is not fresh it is not prepared at all
        (`stop_stale`), so the read view the evaluation gets is the one the freshness check saw."""
        if not asks:
            return []
        out: dict[int, Evaluated | str] = {}
        todo: list[tuple[int, _Ask]] = []
        for i, a in enumerate(asks):
            if a.event is not ArmEvent.PRE and not self._reads.request(a.pos.row.key.adapter_id, a.event, now):
                out[i] = AWAITING_STOP_READ
            else:
                todo.append((i, a))
        if todo:
            before = await self._generation()
            if before is None:
                out.update((i, MD_UNAVAILABLE) for i, _ in todo)
                return [(a, out[i]) for i, a in enumerate(asks)]
            prepared = await self._prepare_all(todo, now, before)
            if await self._generation() != before:
                self.counters.generation_discards += 1
                return None
            for i, a, got in prepared:
                if isinstance(got, NotReady):
                    out[i] = got.reason
                    continue
                result = await self._jobs.run(got.spec, a.pos.ref, got.job)
                self._stamp()
                if result.reason == JOB_SUSPENDED:      # ran across a sleep: a missed bar, never a fault (S39b)
                    self.counters.jobs_suspended += 1
                    out[i] = DOWNTIME
                    continue
                if not result.ok or result.value is None:
                    self.counters.job_faults += 1
                    out[i] = result.reason or PREPARE_FAILED
                    continue
                out[i] = result.value
        return [(a, out[i]) for i, a in enumerate(asks)]

    async def _prepare_all(self, todo: Sequence[tuple[int, _Ask]], now: int,
                           generation: MdGeneration) -> list[tuple[int, _Ask, Prepared | NotReady]]:
        """The prepares of one round, `prepare_concurrency` at a time; a fault is `prepare_failed` for that ask
        only, a fatal error propagates."""
        out: list[tuple[int, _Ask, Prepared | NotReady]] = []
        size = max(1, self._cfg.prepare_concurrency)
        for k in range(0, len(todo), size):
            group = todo[k:k + size]
            got = await asyncio.gather(*(self._bounded_prepare(self._request(a, now, generation))
                                         for _, a in group), return_exceptions=True)
            for (i, a), g in zip(group, got):
                if isinstance(g, BaseException):
                    if self._fatal(g) or not isinstance(g, Exception):
                        raise g
                    g = NotReady(PREPARE_FAILED)
                out.append((i, a, g))
            self._stamp()
        return out

    async def _bounded_prepare(self, request: EvalRequest) -> Prepared | NotReady:
        """One prepare under `prepare_timeout_ms`: a hung read is `prepare_timeout` for that ask (the bar waits or
        is spanned at its cap), never a stale stamp hidden behind an exemption."""
        try:
            async with asyncio.timeout(self._cfg.prepare_timeout_ms / 1000):
                return await self._evaluator.prepare(request)
        except TimeoutError:
            return NotReady(PREPARE_TIMEOUT)

    # ------------------------------------------------------------------------------------------------ the transaction
    async def _commit(self, writes: dict[tuple[ArmEvent, int], list[Decided]], active: dict[str, _Pos],
                      now: int) -> set[str]:
        """One fenced transaction per (event, bar end), oldest bar first; returns the refs whose write committed."""
        ok: set[str] = set()
        for key in sorted(writes, key=lambda k: (k[1], _EVENT_ORDER[k[0]])):
            event, bar = key
            items = writes[key]
            held: ArmStateTxn | None = None
            try:
                with self._postgres():
                    async with self._store.transaction() as tx:
                        held = tx
                        written = await self._sinks.run(tx, event, bar, items)
            except Exception as e:
                self._sinks.settle(held, committed=False)
                if self._fatal(e):
                    raise
                self.counters.write_errors += len(items)
                self._rows_at = None
                log.warning("%s: transaction %s %s failed (%r)", TASK, event.value, bar, type(e))
                continue
            except BaseException:
                self._sinks.settle(held, committed=False)
                raise
            self._sinks.settle(held, committed=True)
            for w in written:
                if self._after_write(w, active):
                    ok.add(w.item.ref)
        return ok

    def _after_write(self, w: Written, active: dict[str, _Pos]) -> bool:
        d = w.item
        if w.status not in (WriteStatus.INSERTED, WriteStatus.UPDATED) or d.position is None:
            if w.status is WriteStatus.CAS_MISS:
                self.counters.cas_miss += 1
            elif w.status is WriteStatus.ALREADY_DECIDED:
                self.counters.already_decided += 1
            else:
                self.counters.write_errors += 1
            self._rows_at = None                          # re-read the row before deciding it again
            return False
        new = replace(d.position.row, row_version=d.position.expected_row_version + 1)
        self._rows = [new if r.key == new.key else r for r in self._rows]
        p = active.get(d.ref)
        if p is not None:
            p.row = new
        dec = d.decision
        if dec is None:
            return True
        if d.on_commit is not None:
            try:
                d.on_commit()
            except Exception as e:
                if self._fatal(e):
                    raise
                log.warning("%s: commit hook of %s failed (%r)", TASK, d.ref, type(e))
        event = ArmEvent(dec.event)
        if event is ArmEvent.PRE:
            self.counters.pre_decisions += 1
            self._pre_done.add((d.ref, dec.bar_end_ms))
            self._pre_wait.pop((d.ref, dec.bar_end_ms), None)
            return True
        if dec.outcome == UNRESOLVED_RANGE:
            self.counters.spans += 1
        else:
            self.counters.decisions += 1
            self._span_paged.discard(d.ref)
            if event in (ArmEvent.POST, ArmEvent.LATE):
                self._grace_paged.discard((new.key.adapter_id, new.symbol, new.bar_spec))
        if event is ArmEvent.REOPEN:
            self._reopen_last.pop(d.ref, None)
            self._reopen_hold.pop(d.ref, None)
        if event in CORRECTABLE and dec.outcome != UNRESOLVED_RANGE and w.ref is not None:
            self._record(d.ref, dec, w.ref, new)
        cursor = new.last_decided_bar
        if cursor is not None:
            for k in [k for k in self._reason if k[0] == d.ref and k[1] <= cursor]:
                del self._reason[k]
            self._cap_tried = {k for k in self._cap_tried if k[0] != d.ref or k[1] > cursor}
        return True

    # ------------------------------------------------------------------------------------------------ corrections
    def _record(self, ref: str, dec: DecisionRow, dref: DecisionRef, row: ArmPositionRow) -> None:
        """Keep a committed final bar-end decision for its correction window (a revision replaces its original)."""
        inputs = recorded_inputs(dec.series_inputs, dec.indicator_inputs)
        if inputs is None or not inputs:
            return
        try:
            spec = BarSpec.parse(dec.bar_spec)
        except ValueError:
            return
        if timing_for(spec) is None:
            return
        rec = RecordedDecision(
            decision_id=dref.decision_id, event=ArmEvent(dec.event), bar_end_ms=dec.bar_end_ms,
            revision=dec.revision, inputs=inputs,
            summary=summary_of(dec.outcome, dec.category, dec.reason, dec.reason_kind, dec.trigger_met))
        window = decision_cap_ms(dec.bar_end_ms, spec, self._grid.skew(row.key.adapter_id))
        self._recorded[(ref, dec.bar_end_ms)] = _Recorded(ref, row.key.adapter_id, row.key.position_id, spec, rec,
                                                          window)

    async def _correction_pass(self, positions: Sequence[_Pos], now: int) -> None:
        """D4 (round 13): compare each kept decision's inputs with their current hashes and revise the corrected
        ones (module docstring, step 7). Contained: a failing pass is counted and retried at the next cadence; a
        lost fence propagates."""
        if self._corrections_at is not None and now - self._corrections_at < self._cfg.correction_every_ms:
            return
        self._corrections_at = now
        try:
            await self._corrections(positions, now)
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.correction_errors += 1
            log.warning("%s: correction pass failed (%r)", TASK, type(e))

    async def _rebuild_window(self, positions: Sequence[_Pos], now: int) -> None:
        """Go-live gate `corrections_restart` (plan S39b): at the first correction pass of a term (a restart or a
        takeover), every committed final bar-end decision of a live position still inside its `decision_cap` window is
        kept again for its correction re-check, from its stored input hashes (a revision replaces its original, as
        `_record` does). Memory only: nothing is written and no notice is queued here, so a notice already sent is
        never sent again; a genuine correction found later is a new `CORRECTED` revision, as within one term. A failed
        read is retried at the next cadence."""
        if self._rebuilt or self._recent is None:
            return
        live = {p.ref: p for p in positions}
        # only bars whose `decision_cap` window can still be open (about 16 min for 15m), never more than KEEP_MS
        reach = max((decision_cap_ms(0, p.spec, self._grid.skew(p.row.key.adapter_id)) for p in positions),
                    default=KEEP_MS)
        with self._postgres():
            rows = await self._recent.recent_final_decisions(now - min(reach, KEEP_MS), REBUILD_LIMIT)
        self._rebuilt = True
        restored = 0
        # the read is newest first (a truncated read drops the oldest); kept here oldest first, revisions in order
        for stored in sorted(rows, key=lambda s: (s.row.bar_end_ms, s.row.revision, s.row.decision_seq)):
            dec = stored.row
            p = live.get(position_ref(dec.key.adapter_id, dec.key.position_id))
            if p is None:
                continue
            held = self._recorded.get((p.ref, dec.bar_end_ms))
            if held is not None and held.rec.decision_id > stored.ref.decision_id:
                continue
            self._record(p.ref, dec, stored.ref, p.row)
            kept = self._recorded.get((p.ref, dec.bar_end_ms))
            if kept is not None and now >= kept.window_end_ms:
                del self._recorded[(p.ref, dec.bar_end_ms)]          # its window ended while the engine was off
            elif kept is not None and kept.rec.decision_id == stored.ref.decision_id:
                restored += 1
        self.counters.corrections_rebuilt += restored
        if len(rows) >= REBUILD_LIMIT:
            log.warning("%s: correction window rebuilt from the newest %d decisions only", TASK, REBUILD_LIMIT)

    async def _corrections(self, positions: Sequence[_Pos], now: int) -> None:
        live = {p.ref: p for p in positions}
        await self._rebuild_window(positions, now)
        await self._write_audits(now)
        for key in [k for k, r in self._recorded.items() if now >= r.window_end_ms or r.ref not in live]:
            del self._recorded[key]             # an open compare keeps its owner: its audit is still written
        kept = {i.indicator_id for r in self._recorded.values() for i in r.rec.inputs
                if isinstance(i, IndicatorInput)}
        self._ind_tf = {k: v for k, v in self._ind_tf.items() if k in kept}
        if not self._recorded:
            return
        current = await self._current_hashes(list(self._recorded.values()))
        asks: list[tuple[_Recorded, _Ask]] = []
        for r in sorted(self._recorded.values(), key=lambda x: (x.rec.bar_end_ms, x.ref)):
            self.counters.correction_checks += 1
            check = check_decision(r.rec, current, now_ms=now, window_end_ms=r.window_end_ms)
            self._checks.note(r.rec, check, r.window_end_ms, (r.adapter_id, r.position_id))
            if check.verdict is CheckVerdict.RETRY:
                self.counters.correction_retries += 1
            if check.verdict is not CheckVerdict.CORRECTION:
                continue
            p = live[r.ref]
            if self._state(p.row) not in DECIDING_STATES:
                # a confirmed correction the position's state cannot take now (FAILED, SUSPENDED): kept open and
                # retried; an `UNVERIFIABLE(not_deciding)` audit row if its window ends first (never silent)
                self.counters.corrections_not_deciding += 1
                self._checks.note(r.rec, CorrectionCheck(CheckVerdict.RETRY, check.changed, (NOT_DECIDING,)),
                                  r.window_end_ms, (r.adapter_id, r.position_id))
                continue
            row = p.row
            if (self._state(row) is ArmState.STRATEGY_COMPLETE and r.rec.summary.released
                    and row.last_decided_bar == r.rec.bar_end_ms):
                row = replace(row, state=ArmState.TRACKING.value)     # re-run from the state before the release
            asks.append((r, _Ask(_Pos(row, p.spec, p.skew), ArmEvent.CORRECTED, r.rec.bar_end_ms)))
        if not asks:
            return
        results = await self._run_asks([a for _, a in asks], now)
        if results is None:
            return                                            # the md generation changed: compared again later
        writes: dict[tuple[ArmEvent, int], list[Decided]] = {}
        for (r, _), (ask, res) in zip(asks, results):
            write = self._revision_write(r, live[r.ref], res, now)
            if write is None:
                self.counters.correction_retries += 1
                self._checks.note(r.rec, CorrectionCheck(CheckVerdict.RETRY, (), (RERUN_NOT_READY,)),
                                  r.window_end_ms, (r.adapter_id, r.position_id))
                continue
            writes.setdefault((ArmEvent.CORRECTED, ask.bar_end_ms), []).append(write)
        if writes:
            await self._commit(writes, dict(live), now)

    def _revision_write(self, r: _Recorded, p: _Pos, res: Evaluated | str, now: int) -> Decided | None:
        """The revision row of one genuine correction and its position CAS; None when the re-run is not final or
        the state refuses its events (compared again at the next cadence)."""
        if isinstance(res, str) or res.verdict is not Verdict.DECISION or not res.final or res.decision is None:
            return None
        row = p.row
        rerun = summary_of(res.decision.outcome, res.decision.category, res.decision.reason, res.decision.reason_kind,
                           res.decision.trigger_met)
        latest_bar = row.last_decided_bar if row.last_decided_bar is not None else r.rec.bar_end_ms
        state = self._state(row)
        released_on = (r.rec.bar_end_ms if state is ArmState.STRATEGY_COMPLETE and r.rec.summary.released
                       and latest_bar == r.rec.bar_end_ms else None)
        try:
            plan = plan_revision(r.rec, rerun, latest_decided_bar_ms=max(latest_bar, r.rec.bar_end_ms),
                                 released_on_bar_ms=released_on)
        except ValueError:
            return None
        events = list(plan.sm11_events)
        if plan.reapply_commit_state:
            events += [e for e in res.sm11_events if e in RERUN_EVENTS]
        transition = None if state is None else steps(state, events, prior_state=_state_or_none(row.prior_state))
        if isinstance(transition, Refused) and state is not None:
            transition = steps(state, plan.sm11_events, prior_state=_state_or_none(row.prior_state))
        if isinstance(transition, Refused):
            self.counters.sm11_refused += 1
            return None
        seq = next_decision_seq(row.decision_seq)
        self._ind_tf.update(res.indicator_tf)
        decision = replace(self._decision_row(p, res.decision, ArmEvent.CORRECTED, r.rec.bar_end_ms, seq, now),
                           revision=plan.revision, supersedes_decision_id=plan.supersedes_decision_id,
                           advice=plan.advice)
        new_state, prior = row.state, row.prior_state
        if transition is not None:
            new_state = transition.state.value
            prior = None if transition.prior_state is None else transition.prior_state.value
        reapply = plan.reapply_commit_state
        new = replace(
            row, state=new_state, prior_state=prior, decision_seq=seq, updated_at_ms=now,
            strategy_state_json=res.strategy_state_json if reapply and res.strategy_state_json is not None
            else row.strategy_state_json,
            last_decided_hash=res.trigger_hash if plan.latest and res.trigger_hash is not None
            else row.last_decided_hash,
            pre_basis_json=res.pre_basis_json if reapply and res.pre_basis_change else row.pre_basis_json,
            pre_basis_bar=res.pre_basis_bar if reapply and res.pre_basis_change else row.pre_basis_bar)
        self.counters.corrections += 1
        if plan.kind is RevisionKind.CORRECTED:
            self.counters.corrections_shown += 1
        return Decided(ref=p.ref, decision=decision,
                       position=PositionCas(row=new, expected_row_version=row.row_version), handles=res.handles,
                       shown=plan.shown, on_commit=res.committed)

    async def _current_hashes(self, recs: Sequence[_Recorded]) -> dict[InputId, CurrentInput]:
        """The current keyed hash of every recorded input (or why it is unknown): series inputs from
        `events_since` (by bucket when the cursor expired or the read failed), indicator inputs from their current
        value's lineage."""
        by_series: dict[str, list[SeriesInput]] = {}
        indicators: dict[InputId, tuple[IndicatorInput, BarSpec]] = {}
        for r in recs:
            for item in r.rec.inputs:
                if isinstance(item, SeriesInput):
                    by_series.setdefault(item.series_id, []).append(item)
                else:
                    indicators[input_id(item)] = (item, r.spec)
        out: dict[InputId, CurrentInput] = {}
        for sid_text, items in sorted(by_series.items()):
            try:
                sid = SeriesId.parse(sid_text)
            except ValueError:
                continue
            got = await self._series_events(sid, min(i.bar_end_ms for i in items))
            for item in items:
                h = got.get(item.bar_end_ms) if got is not None else None
                if h is None:
                    h = await self._bucket_hash(sid, item.bar_end_ms)
                out[input_id(item)] = CurrentInput(h) if h is not None else CurrentInput(None, INPUT_UNREADABLE)
            self._stamp()
        for key, (ind, spec) in sorted(indicators.items()):
            h = await self._indicator_hash(ind, self._ind_tf.get(ind.indicator_id, spec.timeframe_ms))
            out[key] = CurrentInput(h) if h is not None else CurrentInput(None, INPUT_UNREADABLE)
        return out

    async def _series_events(self, sid: SeriesId, from_bar_end_ms: int) -> dict[int, str] | None:
        """The newest event hash per bar end at or after `from_bar_end_ms` (`events_since`, restart form); None
        when the cursor expired or the read failed."""
        try:
            async with asyncio.timeout(self._cfg.prepare_timeout_ms / 1000):
                got = await self._md.events_since(sid, EventCursor(from_bar_end_ms, 0))
        except Exception as e:
            if self._fatal(e):
                raise
            return None
        if not isinstance(got, EventsRead):
            return None
        newest: dict[int, tuple[int, str]] = {}
        for ev in got.events:
            held = newest.get(ev.bar_end_ms)
            if held is None or ev.write_seq > held[0]:
                newest[ev.bar_end_ms] = (ev.write_seq, ev.content_hash)
        return {end: h for end, (_, h) in newest.items()}

    async def _bucket_hash(self, sid: SeriesId, bar_end_ms: int) -> str | None:
        keyring = self._keyring()
        if keyring is None:
            return None
        try:
            async with asyncio.timeout(self._cfg.prepare_timeout_ms / 1000):
                got = await self._md.bars(sid, at_bucket_ms=bar_end_ms - sid.spec.timeframe_ms)
        except Exception as e:
            if self._fatal(e):
                raise
            return None
        if not isinstance(got, BarsRead):
            return None
        bars = [b for b in got.bars if b.bucket_end_ms == bar_end_ms and b.final_at_ms is not None]
        if not bars:
            return None
        b = bars[-1]
        try:
            return content_hash(keyring, sid.canonical, bar_end_ms, b.open, b.high, b.low, b.close)
        except (ValueError, ArithmeticError):
            return None

    async def _indicator_hash(self, ind: IndicatorInput, timeframe_ms: int) -> str | None:
        """The current lineage hash of an indicator input; `timeframe_ms` is the indicator's own bar timeframe (its
        bucket is the recorded bar end minus it)."""
        keyring = self._keyring()
        if keyring is None:
            return None
        bucket = ind.bar_end_ms - timeframe_ms
        try:
            async with asyncio.timeout(self._cfg.prepare_timeout_ms / 1000):
                got = await self._md.indicator(ind.indicator_id, ind.output, at_bucket_ms=bucket)
        except Exception as e:
            if self._fatal(e):
                raise
            return None
        if not isinstance(got, IndicatorRead):
            return None
        values = [v for v in got.values if v.bucket_start_ms == bucket and not v.faulted]
        if not values:
            return None
        v = values[-1]
        start = v.lineage.window_start_ms if v.lineage.window_start_ms is not None else v.bucket_start_ms
        try:
            return lineage_hash(keyring, ind.indicator_id, ind.output, ind.bar_end_ms, start, v.lineage.input_digest,
                                v.lineage.value_revision)
        except ValueError:
            return None

    async def _write_audits(self, now: int) -> None:
        """One audit-only `UNVERIFIABLE(reason)` row per compare still open when its window ended (no notice, no
        page; a metric)."""
        due = self._checks.due_audits(now)
        if not due:
            return
        who = {r.rec.decision_id: r for r in self._recorded.values()}
        with self._postgres():
            async with self._store.transaction() as tx:
                for a in due:
                    held = who.get(a.decision_id)
                    adapter, position = a.adapter_id, a.position_id
                    if not adapter and held is not None:
                        adapter, position = held.adapter_id, held.position_id
                    if not adapter:
                        self.counters.correction_errors += 1      # never expected: every note carries its owner
                        continue
                    await tx.insert_correction_audit(CorrectionAuditRow(
                        ref=DecisionRef(decision_id=a.decision_id, bar_end_ms=a.bar_end_ms), adapter_id=adapter,
                        position_id=position, reason=a.reason, recorded_at_ms=now))
                    self.counters.correction_audits += 1

    def _prune(self, now: int) -> None:
        """Bounded memory on a 24/7 node: forget old bars and positions no longer live."""
        floor = now - KEEP_MS
        self._pre_done = {k for k in self._pre_done if k[1] >= floor}
        self._pre_wait = {k: v for k, v in self._pre_wait.items() if k[1] >= floor}
        live = {r.ref for r in self._rows}
        series = {(r.key.adapter_id, r.symbol, r.bar_spec) for r in self._rows}
        self._reason = {k: v for k, v in self._reason.items() if k[0] in live}
        self._cap_tried = {k for k in self._cap_tried if k[0] in live and k[1] >= floor}
        self._reopen_last = {k: v for k, v in self._reopen_last.items() if k in live}
        self._reopen_hold = {k: v for k, v in self._reopen_hold.items() if k in live}
        self._span_paged &= live
        self._grace_paged &= series
        self._reads.forget(live)


def _no_stamp() -> None:
    return None


def _no_exempt(reason: str | None) -> None:
    return None


def _state_or_none(text: str | None) -> ArmState | None:
    if text is None:
        return None
    try:
        return ArmState(text)
    except ValueError:
        return None


@dataclass(frozen=True)
class _Result:
    """How one evaluated bar-end ask ends this round: a write (and whether the position goes on to its next bar),
    or a REOPEN attempt to retry later."""

    ask: _Ask
    write: Decided | None = None
    go_on: bool = False
    retry: bool = False


_EVENT_ORDER: Final = {ArmEvent.POST: 0, ArmEvent.LATE: 1, ArmEvent.REOPEN: 2, ArmEvent.CORRECTED: 3,
                       ArmEvent.PRE: 4}

__all__ = [
    "ARMED_AFTER_PRE_READ", "BarClock", "BarClockCounters", "BarClockSettings", "CONTROLS_UNREADABLE", "ControlsPort",
    "DECIDING_STATES", "DELAYED", "EvalRequest", "EvaluationPort", "Evaluated", "MD_UNAVAILABLE", "NotReady",
    "PREPARE_TIMEOUT", "PRE_EXPIRED", "PRE_MARKET_CLOSED", "PRE_READ_MISSING", "PostNoticeView", "Prepared",
    "REOPEN_ENDING", "REOPEN_RETRY", "ends_recheck",
    "SM11_REFUSED", "SPAN_ALREADY_DECIDED", "SPAN_ROWS_UNREADABLE", "SPAN_ROW_CHANGED", "SPAN_WRITE_FAILED",
    "SessionCloseView", "TASK", "Verdict",
]
