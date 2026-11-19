"""RiskSweep: the level-triggered automatic writer of rule states and the halt level (oversee-rules.md §2).

Pure core: `apply_pass()` advances every (rule, subject) state of one account by one pass, and
`decide_level()` derives the halt level action from all accounts' committed states. `RiskSweep`
runs passes every `interval_ms` and at once for accounts signalled by `sweep_now()`, reading a
`SnapshotReader` (one READ ONLY REPEATABLE READ snapshot per account: no fence, writes nothing) and
writing through a `SweepWriter` in a separate READ COMMITTED transaction whose every statement is fenced
(`(SELECT epoch FROM engine_epoch WHERE id = 1) = $1`, lock-free: there is no epoch lock), with CAS on
last_sweep_id + clear_generation. Halt changes (trip / system clear) are still serialised by
`pg_advisory_xact_lock(HALT_LOCK)` inside the HaltSwitch. No Oversee timers exist: every deadline is a
stored timestamp.

Every pass is bracketed by the installed ruleset's `begin_pass()` / `end_pass()`, so a staged ruleset installs
only between passes. A lost fence (`EpochMismatch`, any variant, `is_epoch_mismatch`) from any sweep write is
never an account failure: it is reported to `on_fence_lost` (the runtime hard exit) and propagates.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Literal, Protocol

from src.dto import OrderDTO, PositionDTO
from src.oversee.config import OverseeConfig, SweepConfig
from src.oversee.dayroll import build_sod_mark, latest_roll_ms, trading_day_for_roll
from src.oversee.halt import HaltSwitch
from src.oversee.inhibit import AlertInhibitor, Why
from src.oversee.notify import (
    EpisodeKey,
    NotificationDispatcher,
    OutboxRow,
    alerts_for_transitions,
    is_acknowledged,
)
from src.oversee.risk import RiskUnknown, effective_stop_detail, needs_stop
from src.oversee.rules import INPUT_FLAG_RULES, STALE_MARK, EvalContext, Evaluation, RuleRegistry, stale_symbols
from src.oversee.snapshot import AccountSnapshot, DependencyUnavailable, SnapshotReader, SodMark, StopWatchKey
from src.oversee.states import StepParams, resolve_state, step_state
from src.oversee.types import (
    EVALUATION_RULE_ID,
    BreachState,
    Cap,
    HaltLevel,
    Level,
    Page,
    ResolvedBy,
    Result,
    StateKey,
    SymbolKey,
    Transition,
    Unknown,
)

log = logging.getLogger(__name__)

#: K13: the `partial_session` start-of-day alert (oversee-rules.md §2 Day roll, §10). Not a rule: its episode key
#: only gives the outbox row a `notification_dedupe` key, one per (account, trading_day).
SOD_PARTIAL_SESSION = "sod_partial_session"


def sod_partial_alert(account_id: str, sod: SodMark | None, now_ms: int) -> OutboxRow | None:
    """K13: the `sod_partial_session` alert for a `partial_session` sod_marks row this pass writes, or None.

    It is an outbox row in the same AccountWrite (one transaction) as the `sod_marks` row it reports (§10), so a
    failed write, a CasConflict retry, a crash or a lost fence after the write neither loses nor repeats it: the
    row commits with the alert or not at all, and a committed row is never rebuilt. Its episode key
    (sod_partial_session, account, `<account>:<trading_day>`) is deduplicated by the relay through
    `notification_dedupe`, so a row rebuilt over a placeholder still sends one alert per (account, trading_day),
    across restarts and failovers."""
    if sod is None or not sod.partial_session:
        return None
    day = sod.trading_day.isoformat()
    episode: EpisodeKey = (SOD_PARTIAL_SESSION, "account", f"{account_id}:{day}", 0, int(Level.SOFT_HALT))
    return OutboxRow("episode", "high", f"{SOD_PARTIAL_SESSION} account:{account_id} {day}",
                     ",".join(sod.partial_reasons) or "partial_session", now_ms, episode, account_id)


def _input_flag_state(s: BreachState) -> bool:
    """A stale / FENCE input FLAG state (`stale_account_image`, `stale_mark`): its re-pages are dependent alerts."""
    return s.rule_id in INPUT_FLAG_RULES


def _closed_market_stale_mark(s: BreachState, evals: Mapping[StateKey, Evaluation], snap: AccountSnapshot) -> bool:
    """A `stale_mark` state whose flagged symbols are **all** closed or in their roll break (SM10 session not OPEN)
    in this pass: price staleness the calendar explains. operations.md §10 "While CLOSED or in ROLLOVER, only price
    staleness is suppressed", so it is not re-paged (the state, its cap and the gate's stale_mark denial stay; its
    activation alert is still sent). Any flagged symbol that is OPEN (a frozen feed), or a flagged symbol whose
    session is unknown, keeps the re-pages (fail loud)."""
    if s.rule_id != STALE_MARK:
        return False
    ev = evals.get(s.key)
    if ev is None or not isinstance(ev.outcome, Result) or not ev.outcome.stale_symbols:
        return False
    for k in ev.outcome.stale_symbols:
        q = snap.quotes.get(k)
        if q is None or q.session == "OPEN":
            return False
    return True


# ---------------------------------------------------------------- pure core


@dataclass(frozen=True)
class PassResult:
    states: dict[StateKey, BreachState]                         # every stored state after the pass
    changed: list[tuple[BreachState, BreachState | None]]       # (new, previous) rows to write
    transitions: list[Transition]


def _missing_reason(state: BreachState, enabled: frozenset[str]) -> ResolvedBy:
    if state.rule_id == EVALUATION_RULE_ID:
        return "sweep"
    if state.rule_id not in enabled:
        return "reload"
    if state.subject_type == "account_day":
        return "day_end"
    return "entity_end"


def apply_pass(previous: Mapping[StateKey, BreachState], evaluations: Mapping[StateKey, Evaluation],
               now_ms: int, sweep_id: int, cfg: SweepConfig, enabled_rule_ids: frozenset[str]
               ) -> PassResult:
    """Advance one account's states by one successful pass (§2). A state whose subject is gone, terminal
    or no longer evaluated resolves (day_end / entity_end / reload); transitions are keyed by sweep_id."""
    states: dict[StateKey, BreachState] = {}
    changed: list[tuple[BreachState, BreachState | None]] = []
    transitions: list[Transition] = []
    for key, ev in evaluations.items():
        prev = previous.get(key)
        params = StepParams(now_ms, sweep_id, ev.confirm_ms, cfg)
        new, trs = step_state(prev, key[0], ev.subject, ev.outcome, params)
        transitions += trs
        if new is not None:
            states[key] = new
            if new != prev:
                changed.append((new, prev))
    for key, prev in previous.items():
        if key in evaluations:
            continue
        new, trs = resolve_state(prev, _missing_reason(prev, enabled_rule_ids),
                                 StepParams(now_ms, sweep_id, 0, cfg))
        transitions += trs
        states[key] = new
        if new != prev:
            changed.append((new, prev))
    return PassResult(states, changed, transitions)


@dataclass(frozen=True)
class LevelDecision:
    target: HaltLevel
    action: Literal["none", "trip", "clear"]
    reason: str = ""
    rule_id: str | None = None
    escalated: tuple[StateKey, ...] = ()
    drivers: tuple[BreachState, ...] = ()    # the states that each alone justify `target` (trip basis)


def decide_level(states: Iterable[BreachState], current: HaltLevel, hard_latch: bool,
                 level_rose_at_ms: int | None, now_ms: int, cfg: SweepConfig) -> LevelDecision:
    """Halt level after a pass (§2 Halt level): HARD if an active HARD state, the latch or an escalation
    (active uncapped SOFT with uncapped_ms >= escalate_after_ms); else SOFT if an active SOFT state;
    else NONE after cool-down. HARD is only ever released by an operator clear."""
    all_states = list(states)
    active = [s for s in all_states if s.active]
    hard = sorted((s for s in active if s.level >= Level.HARD_HALT), key=lambda s: s.key)
    soft = sorted((s for s in active if s.level == Level.SOFT_HALT), key=lambda s: s.key)
    escalated = [s for s in soft if s.cap is Cap.NONE and not s.unknown_origin
                 and s.uncapped_ms >= cfg.escalate_after_ms]
    if hard or escalated or hard_latch:
        target = HaltLevel.HARD
    elif soft:
        target = HaltLevel.SOFT
    else:
        target = HaltLevel.NONE
    esc_keys = tuple(s.key for s in escalated)
    if target > current:
        if hard:
            return LevelDecision(target, "trip", hard[0].reason, hard[0].rule_id, esc_keys,
                                 tuple(hard + escalated))
        if escalated:
            return LevelDecision(target, "trip", "escalation", escalated[0].rule_id, esc_keys, tuple(escalated))
        lead = soft[0] if soft else None
        return LevelDecision(target, "trip", lead.reason if lead else "latch", lead.rule_id if lead else None,
                             esc_keys, tuple(soft))
    if current is HaltLevel.SOFT and target is HaltLevel.NONE and not hard_latch:
        marks = [s.below_soft_since for s in all_states if s.below_soft_since is not None]
        if level_rose_at_ms is not None:
            marks.append(level_rose_at_ms)
        start = max(marks) if marks else 0
        if now_ms >= start + cfg.cool_down_ms:
            return LevelDecision(HaltLevel.NONE, "clear", "cool_down")
    return LevelDecision(target, "none", "", None, esc_keys)


def stop_watch_changes(snap: AccountSnapshot, now_ms: int) -> tuple[dict[StopWatchKey, int], list[StopWatchKey]]:
    """stop_watch rows to set (first seen needing a stop without one) and to clear (stop seen, or
    the entity no longer needs one / is gone) (§2 Deadlines). Unknown entities are left alone. The deadline
    follows `effective_stop_detail()` (design PR1): any effective stop clears it, including one with the
    `stop_kind_inconsistent` caveat, which MissingStopLossRule judges against the pass's MarkBook mark as
    UNKNOWN (the durable `unknown_since` / `unknown_grace_ms`, not this deadline)."""
    to_set: dict[StopWatchKey, int] = {}
    to_clear: list[StopWatchKey] = []
    live: set[StopWatchKey] = set()
    entities: list[tuple[str, OrderDTO | PositionDTO]] = [
        ("order", o) for o in snap.orders if not o.status.terminal]
    entities += [("position", p) for p in snap.positions if p.status == "OPEN"]
    for kind, e in entities:
        key = (kind, e.entity_key)
        live.add(key)
        try:
            missing = needs_stop(e, snap.positions, snap.orders) and \
                effective_stop_detail(e, snap.positions, snap.orders) is None
        except RiskUnknown:
            continue
        if missing and key not in snap.stop_watch:
            to_set[key] = now_ms
        elif not missing and key in snap.stop_watch:
            to_clear.append(key)
    to_clear += [k for k in snap.stop_watch if k not in live]
    return to_set, to_clear


# ---------------------------------------------------------------- write port


class CasConflict(Exception):
    """A halt_breaches compare-and-set failed (an operator clear or another writer moved the row)."""


class EpochMismatch(Exception):
    """A fenced write found engine_epoch moved on: this node is no longer the leader (operations.md §2)."""


def is_epoch_mismatch(exc: BaseException) -> bool:
    """A lost fence: an `EpochMismatch` (this one, src.db.fence's, or `SweepFenced`, matched by class name
    so no db import is needed) as the exception, or anywhere in its cause / context chain."""
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if any(c.__name__ == "EpochMismatch" for c in type(cur).__mro__):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


@dataclass(frozen=True)
class StateWrite:
    state: BreachState
    expected_sweep_id: int | None        # None: the row must not exist yet
    expected_generation: int | None


@dataclass(frozen=True)
class AccountWrite:
    account_id: str
    epoch: int
    sweep_id: int
    states: tuple[StateWrite, ...] = ()
    stop_watch_set: tuple[tuple[StopWatchKey, int], ...] = ()
    stop_watch_clear: tuple[StopWatchKey, ...] = ()
    events: tuple[Transition, ...] = ()
    outbox: tuple[OutboxRow, ...] = ()
    sod_mark: SodMark | None = None


class SweepWriter(Protocol):
    """One READ COMMITTED transaction, separate from the READ ONLY snapshot the pass was built from.
    Every statement carries the lock-free fence `(SELECT epoch FROM engine_epoch WHERE id = 1) = $1`
    (a lost fence raises EpochMismatch; no epoch lock is taken): halt_breaches CAS (last_sweep_id +
    clear_generation), stop_watch, oversee_events (ON CONFLICT DO NOTHING), notification_outbox,
    sod_marks (idempotent). HALT_LOCK is not taken here; it serialises only halt changes (HaltSwitch)."""

    async def write_account(self, batch: AccountWrite) -> None: ...

    async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None: ...


# ---------------------------------------------------------------- RiskSweep task


@dataclass
class AccountHealth:
    last_success_ms: int | None = None
    first_failure_ms: int | None = None
    last_attempt_ms: int | None = None
    quarantined_at_ms: int | None = None
    failure_paged: bool = False                # `evaluation_failed` has paged (it re-pages every escalate_after_ms)
    quarantine_stored: bool = False            # the sweep_quarantine row of this quarantine is written
    failure_state_since: int | None = None     # the capped SOFT `evaluation_failed` state is active since
    failure_row: BreachState | None = None     # that state as written to halt_breaches by a level decision
    last_error: str = ""                       # the latest pass failure (the page detail)


class InstallableRuleset(Protocol):
    """What `RiskSweep.install_ruleset` needs (src/oversee/ruleset.py `ActiveRuleset`): the live config and
    registry the sweep reads, and the sweep boundary that installs a staged ruleset."""

    @property
    def config(self) -> OverseeConfig: ...

    @property
    def registry(self) -> RuleRegistry: ...

    def at_boundary(self) -> bool: ...

    def begin_pass(self) -> bool:
        """A pass starts: install a staged ruleset (the sweep boundary), then count the pass as running."""
        ...

    def end_pass(self) -> None:
        """The pass ended: installs are allowed again until the next `begin_pass()`."""
        ...


@dataclass(frozen=True)
class AccountView:
    """What the gate needs from the sweep for one account (§8)."""

    last_pass_ms: int
    stale_symbols: frozenset[SymbolKey]
    has_unknown: bool


@dataclass
class PassReport:
    sweep_id: int
    succeeded: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    dependency_down: bool = False
    decision: LevelDecision | None = None


class HlcSweepIds:
    """Monotonic HLC sweep ids: max(now_ms << 20, last + 1)."""

    def __init__(self, clock: Callable[[], int], seed: int = 0) -> None:
        self._clock = clock
        self._last = seed

    def __call__(self) -> int:
        self._last = max(self._clock() << 20, self._last + 1)
        return self._last



def _log_cash_flow_request_failure(task: "asyncio.Task[object]") -> None:
    """Retrieve the outcome of a fire-and-forget cash-flow refresh so a failure is logged, not lost.
    The next pass retries it after `cash_flow_request_min_interval_ms`."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logging.getLogger(__name__).warning("cash-flow refresh request failed: %s", type(exc).__name__)

class RiskSweep:
    def __init__(self, *, reader: SnapshotReader, writer: SweepWriter, registry: RuleRegistry,
                 halt: HaltSwitch, config: OverseeConfig, clock: Callable[[], int], epoch: Callable[[], int],
                 sweep_ids: Callable[[], int] | None = None,
                 cash_flow_requester: Callable[[str], Coroutine[Any, Any, None]] | None = None,
                 pager: Callable[[Page], None] | None = None,
                 dispatcher: NotificationDispatcher | None = None,
                 on_fence_lost: Callable[[BaseException], None] | None = None,
                 inhibitor: AlertInhibitor | None = None) -> None:
        """`on_fence_lost(exc)`: called once, before the exception propagates out of `run_pass()`, when any
        sweep write (the account batch, sweep_quarantine, the evaluation_failed row, a halt trip / clear,
        unlisted-state resolution, re-page queueing) raises an `EpochMismatch`; production passes the
        runtime hard exit. A lost fence is never an account failure: it propagates out of `run_pass()` and
        `run()`.

        `inhibitor`: holds back the dependent (stale / FENCE) alert rows during an adapter's maintenance window and
        for RTO + 30 s after an epoch change, then pages one summary or escalation (operations.md §9, §10); the
        transitions are still written. None: every transition alerts."""
        self._reader = reader
        self._writer = writer
        self._registry = registry
        self._halt = halt
        self._cfg = config
        self._clock = clock
        self._epoch = epoch
        self._sweep_ids = sweep_ids or HlcSweepIds(clock)
        self._cf_requester = cash_flow_requester
        self._pager = pager
        self._dispatcher = dispatcher
        self._committed: dict[str, list[BreachState]] = {}
        self._read_at: dict[str, int] = {}
        self._views: dict[str, AccountView] = {}
        self._health: dict[str, AccountHealth] = {}
        self._cf_tasks: dict[str, asyncio.Task[None]] = {}
        self._cf_requested_at: dict[str, int] = {}
        self._signalled: set[str] = set()
        self._wake = asyncio.Event()
        self._stopping = False
        self._dependency_paged = False
        self._page_latch: dict[tuple[str, str | None, str], int] = {}   # (code, account, key) -> last paged
        self._unlisted_checked = False    # unresolved rows of unlisted accounts resolved since start-up
        self._unlisted_drop: BreachState | None = None   # latest of those that dropped below SOFT (cool-down)
        self._ruleset: InstallableRuleset | None = None
        self._next_ruleset: InstallableRuleset | None = None   # install_ruleset() called mid-pass
        self._passes_running = 0
        self._on_fence_lost = on_fence_lost
        self._quarantine_released: set[str] = set()   # accounts whose sweep_quarantine row is known released
        self._inhibitor = inhibitor

    # -- ruleset -------------------------------------------------------------------------------
    def install_ruleset(self, ruleset: InstallableRuleset) -> None:
        """Run on `ruleset` from the next pass: the sweep reads its live config and registry, every pass
        calls `begin_pass()` (the sweep boundary, before the sweep_id is drawn, so a staged ruleset is
        installed between passes) and `end_pass()` when it ends (returned, raised or cancelled), so the
        ruleset never installs mid-pass and one pass never mixes two rulesets; the dispatcher's re-page
        cadence follows the installed `escalate_after_ms`. Called while a pass runs, the switch waits until
        no pass is running. Replaces the `ActiveRuleset.wrap_sweep_ids` wrapper (still accepted: a boundary
        call inside a pass installs nothing)."""
        if self._passes_running:
            self._next_ruleset = ruleset
            return
        self._use_ruleset(ruleset)

    def _use_ruleset(self, ruleset: InstallableRuleset) -> None:
        self._next_ruleset = None
        self._ruleset = ruleset
        self._cfg = ruleset.config
        self._registry = ruleset.registry
        if self._dispatcher is not None:
            self._dispatcher.follow_cadence(lambda: self._cfg.sweep.escalate_after_ms)

    @property
    def pass_running(self) -> bool:
        return self._passes_running > 0

    def _begin_pass(self) -> InstallableRuleset | None:
        """The sweep boundary: take over a ruleset handed in mid-pass, install its staged ruleset and mark
        the pass running. Returns the ruleset whose `end_pass()` closes this pass."""
        if self._next_ruleset is not None and not self._passes_running:
            self._use_ruleset(self._next_ruleset)
        self._passes_running += 1
        ruleset = self._ruleset
        if ruleset is None:
            return None
        try:
            ruleset.begin_pass()
        except Exception as exc:   # keep sweeping on the ruleset installed so far (the pass still counts)
            self._bracket_page(f"{type(exc).__name__}: {exc}")
        else:
            self._unlatch("ruleset_install_failed", None)
        return ruleset

    def _end_pass(self, ruleset: InstallableRuleset | None) -> None:
        self._passes_running = max(0, self._passes_running - 1)
        if ruleset is not None:
            try:
                ruleset.end_pass()
            except Exception as exc:   # never masks the pass's own outcome
                self._bracket_page(f"end_pass: {type(exc).__name__}: {exc}")
        if self._next_ruleset is not None and not self._passes_running:
            try:
                self._use_ruleset(self._next_ruleset)
            except Exception as exc:   # never masks the pass's own outcome
                self._bracket_page(f"install_ruleset: {type(exc).__name__}: {exc}")

    def _bracket_page(self, detail: str) -> None:
        """`ruleset_install_failed` from the pass bracket. A failing pager must not escape the bracket: it
        would leave the pass counted as running (no ruleset could install again) or mask the pass's own
        outcome (a lost fence)."""
        try:
            self._latched_page(Page("high", "ruleset_install_failed", None, detail, self._clock()))
        except Exception:
            log.exception("ruleset_install_failed page failed (%s)", detail)

    # -- signals and views -------------------------------------------------------------------
    def sweep_now(self, account_id: str) -> None:
        """Non-blocking wake-up for one account (called by the feed observer)."""
        self._signalled.add(account_id)
        self._wake.set()

    def stop(self) -> None:
        self._stopping = True
        self._wake.set()

    def account_view(self, account_id: str) -> AccountView | None:
        return self._views.get(account_id)

    def committed_states(self) -> list[BreachState]:
        return [s for states in self._committed.values() for s in states]

    def health(self, account_id: str) -> AccountHealth:
        return self._health.setdefault(account_id, AccountHealth())

    def release_account(self, operator: str, account_id: str) -> None:
        """Audited operator release from sweep_quarantine (§4)."""
        h = self.health(account_id)
        h.quarantined_at_ms = None
        h.first_failure_ms = None
        self._quarantine_released.discard(account_id)   # the next good pass also releases the stored row
        self._unlatch("quarantine_unwritten", account_id)   # no row left to write; a new quarantine pages at once
        self._page(Page("info", "quarantine_released", account_id, f"by {operator}", self._clock()))

    def _page(self, page: Page) -> None:
        if self._pager is not None:
            self._pager(page)

    def _latched_page(self, page: Page, key: str = "") -> None:
        """A condition page: once when it starts, then again every escalate_after_ms while it lasts (§10)."""
        latch = (page.code, page.account_id, key)
        last = self._page_latch.get(latch)
        if last is not None and page.at_ms - last < self._cfg.sweep.escalate_after_ms:
            return
        self._page_latch[latch] = page.at_ms
        self._page(page)

    def _unlatch(self, code: str, account_id: str | None) -> None:
        """The condition ended: the next occurrence pages at once."""
        for latch in [k for k in self._page_latch if k[0] == code and k[1] == account_id]:
            del self._page_latch[latch]

    # -- passes ------------------------------------------------------------------------------
    async def run_pass(self, accounts: Sequence[str] | None = None) -> PassReport:
        """One pass over `accounts` (all when None), then the halt level decision. Bracketed by the
        ruleset's `begin_pass()` / `end_pass()`. A lost fence (`EpochMismatch`) from any write is reported
        to `on_fence_lost` and propagates: it is never an account failure, and the rest of the pass is
        abandoned (no further write, no level decision)."""
        ruleset = self._begin_pass()
        try:
            return await self._run_pass(accounts)
        except Exception as exc:
            if is_epoch_mismatch(exc) and self._on_fence_lost is not None:
                self._on_fence_lost(exc)
            raise
        finally:
            self._end_pass(ruleset)

    async def _run_pass(self, accounts: Sequence[str] | None) -> PassReport:
        report = PassReport(self._sweep_ids())
        try:
            ids = list(accounts) if accounts is not None else list(await self._reader.list_accounts())
        except DependencyUnavailable:
            self._dependency_down()
            report.dependency_down = True
            return report
        if accounts is None:
            await self._forget_unlisted(ids, report.sweep_id)
        for acct in ids:
            if not self._due(acct):
                report.skipped.append(acct)
                continue
            try:
                await self._pass_account(acct, report.sweep_id)
                report.succeeded.append(acct)
            except DependencyUnavailable:
                self._dependency_down()
                report.dependency_down = True
                break
            except Exception as exc:
                if is_epoch_mismatch(exc):
                    raise
                await self._record_failure(acct, exc)
                report.failed.append(acct)
        if not report.dependency_down:
            self._dependency_paged = False
        report.decision = await self._decide(report.sweep_id)
        return report

    async def _forget_unlisted(self, ids: Sequence[str], sweep_id: int) -> None:
        """Accounts gone from a full listing hold no states, views, health or pages any more, and their
        unresolved halt_breaches rows are resolved, so neither an in-memory `evaluation_failed` state nor a
        stored row can hold the level after the account has gone. Rows left by an earlier leader are
        resolved on the first full pass."""
        listed = set(ids)
        known = (set(self._committed) | set(self._read_at) | set(self._views) | set(self._health)
                 | set(self._cf_requested_at))
        gone = known - listed
        for acct in gone:
            self._committed.pop(acct, None)
            self._read_at.pop(acct, None)
            self._views.pop(acct, None)
            self._health.pop(acct, None)
            self._cf_requested_at.pop(acct, None)
            self._quarantine_released.discard(acct)
            task = self._cf_tasks.get(acct)
            if task is not None and task.done():
                del self._cf_tasks[acct]
            for latch in [k for k in self._page_latch if k[1] == acct]:
                del self._page_latch[latch]
        if gone and self._inhibitor is not None:
            self._inhibitor.forget(gone)
        if not gone and self._unlisted_checked:
            return
        now = self._clock()
        try:
            resolved = await self._halt.resolve_unlisted(listed, now_ms=now, sweep_id=sweep_id)
        except Exception as exc:
            if is_epoch_mismatch(exc):
                raise
            self._unlisted_checked = False    # retried on the next full pass
            self._latched_page(Page("high", "unlisted_states_unresolved", None, f"{type(exc).__name__}: {exc}",
                                    now))
            return
        self._unlisted_checked = True
        self._unlatch("unlisted_states_unresolved", None)
        if resolved:
            dropped = [s for s in resolved if s.below_soft_since is not None]
            if dropped:
                self._unlisted_drop = max(dropped, key=lambda s: s.below_soft_since or 0)
            self._page(Page("info", "unlisted_states_resolved", None,
                            ";".join("/".join(s.key) for s in resolved), now))

    def _dependency_down(self) -> None:
        if not self._dependency_paged:
            self._dependency_paged = True
            self._page(Page("critical", "postgres_unavailable", None, "RiskSweep pass skipped", self._clock()))

    def _due(self, acct: str) -> bool:
        h = self.health(acct)
        if h.quarantined_at_ms is None:
            return True
        last = h.last_attempt_ms or h.quarantined_at_ms
        return self._clock() - last >= self._cfg.sweep.quarantine_retry_ms   # half-open retry

    def _prepare(self, snap: AccountSnapshot, now: int) -> tuple[AccountSnapshot, SodMark | None]:
        roll = latest_roll_ms(now)
        new_sod: SodMark | None = None
        if snap.sod is None or snap.sod.trading_day != trading_day_for_roll(roll):
            snap = replace(snap, sod=None)
            try:
                new_sod = build_sod_mark(snap, roll, now, self._cfg)
                snap = replace(snap, sod=new_sod)
                # K13: a partial_session row's alert is an outbox row of the same write (`sod_partial_alert`).
                self._unlatch("sod_missing", snap.account_id)
            except RiskUnknown as exc:
                if now - roll >= self._cfg.sweep.sod_missing_page_ms:
                    self._latched_page(Page("high", "sod_missing", snap.account_id, str(exc), now),
                                       trading_day_for_roll(roll).isoformat())
        else:
            self._unlatch("sod_missing", snap.account_id)
        return snap, new_sod

    async def _pass_account(self, acct: str, sweep_id: int) -> None:
        for attempt in (1, 2):
            now = self._clock()
            snap = await self._reader.read_account(acct, now)
            snap, new_sod = self._prepare(snap, now)
            ctx = EvalContext(snap, self._cfg, now)
            evals = self._registry.evaluate(ctx)
            stored = {s.key: s for s in snap.breaches}
            prev = self._with_failure_state(acct, stored, now, sweep_id)
            result = apply_pass(prev, evals, now, sweep_id, self._cfg.sweep, self._registry.rule_ids)
            sw_set, sw_clear = stop_watch_changes(snap, now)
            alerted, held, why = self._split_alerts(snap, result.transitions, now)
            outbox = tuple(alerts_for_transitions(alerted))
            sod_alert = sod_partial_alert(acct, new_sod, now)
            batch = AccountWrite(
                account_id=acct, epoch=self._epoch(), sweep_id=sweep_id,
                states=tuple(StateWrite(new, old.last_sweep_id if old else None,
                                        old.clear_generation if old else None)
                             for new, old in ((n, stored.get(n.key)) for n, _ in result.changed)),
                stop_watch_set=tuple(sw_set.items()), stop_watch_clear=tuple(sw_clear),
                events=tuple(result.transitions), outbox=outbox + ((sod_alert,) if sod_alert is not None else ()),
                sod_mark=new_sod)
            try:
                await self._writer.write_account(batch)
            except CasConflict:
                if attempt == 2:
                    raise
                continue
            if self._inhibitor is not None:      # counted once, and only for a written pass
                self._inhibitor.record(acct, snap.adapter_id, held, why)
            self._on_success(acct, snap, now, evals, ctx, result)
            await self._release_quarantine(acct, now)
            await self._after_success(acct, snap, evals, result, now,
                                      alerted=[r.episode for r in outbox if r.episode is not None], why=why)
            return

    def _split_alerts(self, snap: AccountSnapshot, transitions: Sequence[Transition],
                      now: int) -> tuple[list[Transition], list[Transition], Why | None]:
        """(alerted, held back, why): the transitions whose outbox rows are queued; the dependent ones held back
        by the inhibitor are not (their oversee_events rows are still written)."""
        if self._inhibitor is None:
            return list(transitions), [], None
        return self._inhibitor.split(snap.adapter_id, transitions, now)

    def _with_failure_state(self, acct: str, stored: Mapping[StateKey, BreachState], now: int,
                            sweep_id: int) -> dict[StateKey, BreachState]:
        """The previous states for a pass. After an `evaluation_failed` spell whose state is not stored
        as active (its write was refused or failed), the state is added as active, so this first good pass
        resolves it like any other evaluation state: the row records `below_soft_since`, from which the
        cool-down runs (§2 Cool-down)."""
        prev = dict(stored)
        h = self.health(acct)
        if h.failure_state_since is None:
            return prev
        key: StateKey = (EVALUATION_RULE_ID, "account", acct)
        row = prev.get(key)
        if row is None or not row.active:
            activation = h.failure_row.activation_id if h.failure_row is not None else sweep_id
            prev[key] = self._failure_row(acct, h.failure_state_since, row, activation, now)
        return prev

    @staticmethod
    def _failure_row(acct: str, active_since: int, stored: BreachState | None, activation_id: int | None,
                     now: int) -> BreachState:
        """The capped SOFT `evaluation_failed` halt_breaches row, built over the stored row if any (its
        escalation clock and clear_generation carry over, like a reopened state)."""
        base = stored if stored is not None else BreachState(EVALUATION_RULE_ID, "account", acct, acct)
        return replace(base, level=Level.SOFT_HALT, cap=Cap.SOFT, reason="evaluation_failed", pending_level=None,
                       pending_since=None, active_since=active_since, activation_id=activation_id,
                       release_pending_since=None, unknown_since=None, unknown_origin=False, last_pass_ms=now,
                       below_soft_since=None, resolved_at=None, resolved_by=None)

    def _on_success(self, acct: str, snap: AccountSnapshot, now: int, evals: Mapping[StateKey, Evaluation],
                    ctx: EvalContext, result: PassResult) -> None:
        self._committed[acct] = list(result.states.values())
        self._read_at[acct] = now
        unknown = any(isinstance(ev.outcome, Unknown) for ev in evals.values())
        self._views[acct] = AccountView(self._clock(), stale_symbols(dict(evals), ctx), unknown)
        h = self.health(acct)
        if h.quarantined_at_ms is not None:
            self._quarantine_released.discard(acct)
            self._page(Page("info", "quarantine_released", acct, "first good pass", now))
        self._unlatch("evaluation_failed", acct)
        self._unlatch("quarantine_unwritten", acct)   # released: no row left to write; a later one pages at once
        self._health[acct] = AccountHealth(last_success_ms=now)

    async def _release_quarantine(self, acct: str, now: int) -> None:
        """A good pass releases the account's `sweep_quarantine` row (§2: "released by its first good pass").
        Written once per account per term (a row an earlier leader left open is released too) and again after
        every quarantine; a failed release is retried on the next good pass and never fails this one."""
        if acct in self._quarantine_released:
            return
        try:
            await self._writer.set_quarantine(acct, None, "first good pass")
        except Exception as exc:
            if is_epoch_mismatch(exc):
                raise
            self._latched_page(Page("high", "quarantine_release_failed", acct, f"{type(exc).__name__}: {exc}", now))
            return
        self._quarantine_released.add(acct)
        self._unlatch("quarantine_release_failed", acct)

    async def _after_success(self, acct: str, snap: AccountSnapshot, evals: Mapping[StateKey, Evaluation],
                             result: PassResult, now: int, *, alerted: Sequence[EpisodeKey] = (),
                             why: Why | None = None) -> None:
        """Cash-flow request, then the re-pages due (every active state, FLAG included, oversee-rules.md §10).
        `alerted`: the episodes whose alert this pass wrote. `why`: the adapter's dependent alerts are held back
        now (AlertInhibitor), so the input FLAG states (stale / FENCE) are not re-paged either. A `stale_mark` state
        whose symbols are all closed / in their roll break is not re-paged (`_closed_market_stale_mark`)."""
        wants = any(isinstance(ev.outcome, Result) and ev.outcome.wants_cash_flows for ev in evals.values())
        if wants:
            self._request_cash_flows(acct, now)
        if self._dispatcher is not None:
            states = [s for s in result.states.values()
                      if (why is None or not _input_flag_state(s)) and not _closed_market_stale_mark(s, evals, snap)]
            self._dispatcher.mark_alerted(alerted, now)
            try:
                await self._dispatcher.queue_repages(states, snap.acks, now)
            except Exception as exc:
                if is_epoch_mismatch(exc):
                    raise
                self._latched_page(Page("high", "repage_failed", acct, "", now))
            else:
                self._unlatch("repage_failed", acct)

    def _request_cash_flows(self, acct: str, now: int) -> None:
        """Unexplained balance decrease -> fetch_cash_flows via the orchestrator, at most once a minute."""
        if self._cf_requester is None:
            return
        task = self._cf_tasks.get(acct)
        if task is not None and not task.done():
            return
        last = self._cf_requested_at.get(acct)
        if last is not None and now - last < self._cfg.sweep.cash_flow_request_min_interval_ms:
            return
        self._cf_requested_at[acct] = now
        task = asyncio.get_running_loop().create_task(self._cf_requester(acct))
        task.add_done_callback(_log_cash_flow_request_failure)
        self._cf_tasks[acct] = task

    async def _record_failure(self, acct: str, exc: Exception) -> None:
        now = self._clock()
        h = self.health(acct)
        h.first_failure_ms = h.first_failure_ms if h.first_failure_ms is not None else now
        h.last_attempt_ms = now
        h.last_error = f"{type(exc).__name__}: {exc}"
        base = h.last_success_ms if h.last_success_ms is not None else h.first_failure_ms
        if h.quarantined_at_ms is None and now - base >= self._cfg.sweep.quarantine_after_ms:
            h.quarantined_at_ms = now
            h.quarantine_stored = False
            self._quarantine_released.discard(acct)
            self._page(Page("critical", "oversight_lapsed", acct, f"oversight lapsed for account {acct}", now))
        await self._store_quarantine(acct, now)

    async def _store_quarantine(self, acct: str, now: int) -> None:
        """Write the account's `sweep_quarantine` row (§2: the quarantine is durable, so `account status`, the
        dashboard and a new leader see it). A failed write pages (latched, critical) and is retried after every
        pass (`_store_quarantines`), not only at the account's 10 min half-open retry."""
        h = self._health.get(acct)
        if h is None or h.quarantined_at_ms is None or h.quarantine_stored:
            return
        try:
            await self._writer.set_quarantine(acct, h.quarantined_at_ms, "consecutive pass failures")
        except Exception as exc:
            if is_epoch_mismatch(exc):
                raise
            log.error("sweep_quarantine row for %s not written: %s: %s", acct, type(exc).__name__, exc)
            self._latched_page(Page("critical", "quarantine_unwritten", acct,
                                    f"oversight lapsed for account {acct} but its sweep_quarantine row was not "
                                    f"written ({type(exc).__name__}: {exc}); retrying", now))
            return
        h.quarantine_stored = True
        self._unlatch("quarantine_unwritten", acct)

    async def _store_quarantines(self, now: int) -> None:
        for acct in sorted(a for a, h in self._health.items()
                           if h.quarantined_at_ms is not None and not h.quarantine_stored):
            await self._store_quarantine(acct, now)

    def _failure_states(self, now: int) -> list[BreachState]:
        """Capped SOFT `evaluation_failed` for accounts without a good pass for 10 s (§2)."""
        out: list[BreachState] = []
        for acct, h in self._health.items():
            if h.first_failure_ms is None:
                continue
            base = h.last_success_ms if h.last_success_ms is not None else h.first_failure_ms
            if now - base >= self._cfg.sweep.evaluation_failed_after_ms:
                out.append(BreachState(EVALUATION_RULE_ID, "account", acct, acct, Level.SOFT_HALT, Cap.SOFT,
                                       "evaluation_failed", active_since=base + self._cfg.sweep.evaluation_failed_after_ms,
                                       last_pass_ms=now))
        return out

    async def _page_failures(self, failing: Sequence[BreachState], now: int) -> None:
        """`evaluation_failed` pages when the state starts, then again every escalate_after_ms (the installed
        ruleset's) while the account keeps failing (§2 capped states re-page "unless acknowledged"). Paged from
        the level decision, so a quarantined account that is retried only every 10 min still re-pages on time.

        A re-page is skipped (and the cadence restarts) when an operator acknowledged the stored state with its
        current level, cap and reason since it activated (§4 acknowledge). The account's snapshot is failing, so
        breach_acks is read through the HaltSwitch, and only when a re-page is due. An unreadable ack pages."""
        for s in failing:
            h = self.health(s.account_id)
            latch = ("evaluation_failed", s.account_id, "")
            last = self._page_latch.get(latch)
            if last is not None and now - last < self._cfg.sweep.escalate_after_ms:
                continue
            if last is not None and await self._failure_acknowledged(s):
                self._page_latch[latch] = now       # silenced: checked again one cadence later
                continue
            h.failure_paged = True
            self._latched_page(Page("high", "evaluation_failed", s.account_id, h.last_error, now))

    async def _failure_acknowledged(self, s: BreachState) -> bool:
        """The failing account's stored `evaluation_failed` state is acknowledged at its level, cap and reason."""
        h = self.health(s.account_id)
        state = h.failure_row
        if state is None:
            stored = next((b for b in self._committed.get(s.account_id, ()) if b.key == s.key and b.active), None)
            if stored is None or stored.reason != "evaluation_failed":
                return False      # not stored as active: nothing an operator could have acknowledged
            state = stored
        try:
            acks = await self._halt.acks_for([s.key])
        except Exception as exc:
            if is_epoch_mismatch(exc):
                raise
            log.warning("breach_acks of %s unreadable (%s): re-paging", s.account_id, type(exc).__name__)
            return False
        return is_acknowledged(state, acks)

    async def _write_failure_states(self, failing: Sequence[BreachState], now: int, sweep_id: int) -> None:
        """Write each new `evaluation_failed` state to halt_breaches (§2: "written by the next level
        decision"), with its `activated` event, so the operator clear, a new leader and the cool-down see it.
        Its alert is the `evaluation_failed` page. A failed write is retried at the next decision. A lost
        CAS (the stored row is not the one this sweep last read: an operator clear bumped it, or an earlier
        leader left it) is retried at once under HALT_LOCK over the row read there (`HaltSwitch.upsert_state`),
        so the state is still written while the account keeps failing."""
        for s in failing:
            h = self.health(s.account_id)
            if h.failure_state_since is None:
                h.failure_state_since = s.active_since
            since = h.failure_state_since
            if h.failure_row is not None or since is None:
                continue
            stored = next((b for b in self._committed.get(s.account_id, ()) if b.key == s.key), None)
            row, event = self._failure_write(s.account_id, since, stored, sweep_id, now)
            batch = AccountWrite(account_id=s.account_id, epoch=self._epoch(), sweep_id=sweep_id,
                                 states=(StateWrite(row, stored.last_sweep_id if stored else None,
                                                    stored.clear_generation if stored else None),),
                                 events=(event,))
            try:
                await self._writer.write_account(batch)
            except CasConflict:
                h.failure_row = await self._rewrite_failure_row(s.account_id, s.key, since, sweep_id, now)
                continue
            except Exception as exc:
                if is_epoch_mismatch(exc):
                    raise
                continue
            h.failure_row = row

    def _failure_write(self, acct: str, since: int, stored: BreachState | None, sweep_id: int,
                       now: int) -> tuple[BreachState, Transition]:
        row = replace(self._failure_row(acct, since, stored, sweep_id, now), last_sweep_id=sweep_id)
        event = Transition(row.rule_id, row.subject_type, row.subject_id, row.account_id, "activated",
                           sweep_id, now, row.level, row.cap, row.reason, row.activation_id)
        return row, event

    async def _rewrite_failure_row(self, acct: str, key: StateKey, since: int, sweep_id: int,
                                   now: int) -> BreachState | None:
        """The CAS retry: under HALT_LOCK (which every other writer of a failing account's row takes) the
        current row is read and the state written over it. None (retried at the next decision) on error."""
        def build(current: BreachState | None) -> tuple[BreachState, tuple[Transition, ...]] | None:
            if current is not None and current.active and current.reason == "evaluation_failed":
                return None                       # already stored as active: nothing to write
            row, event = self._failure_write(acct, since, current, sweep_id, now)
            return row, (event,)

        try:
            written = await self._halt.upsert_state(key, build)
        except Exception as exc:
            if is_epoch_mismatch(exc):
                raise
            self._latched_page(Page("high", "evaluation_failed_unwritten", acct, f"{type(exc).__name__}: {exc}",
                                    now))
            return None
        self._unlatch("evaluation_failed_unwritten", acct)
        return written

    def _read_time(self, state: BreachState, synthetic: set[str], now: int) -> int:
        """When the input behind `state` was read: now for a synthetic `evaluation_failed` state (the
        failure is a current fact), else the account's last good pass."""
        if state.rule_id == EVALUATION_RULE_ID and state.account_id in synthetic:
            return now
        return self._read_at.get(state.account_id, now)

    async def _decide(self, sweep_id: int) -> LevelDecision | None:
        now = self._clock()
        failing = self._failure_states(now)
        failing_accts = {s.account_id for s in failing}
        await self._page_failures(failing, now)
        await self._write_failure_states(failing, now, sweep_id)
        await self._store_quarantines(now)
        states = [s for s in self.committed_states()
                  if not (s.account_id in failing_accts and s.rule_id == EVALUATION_RULE_ID)] + failing
        if self._unlisted_drop is not None:   # resolved and inactive: only its below_soft_since counts
            states.append(self._unlisted_drop)
        try:
            status = await self._halt.status()
            decision = decide_level(states, status.effective_level, status.hard_latch, status.rose_at_ms,
                                    now, self._cfg.sweep)
            if decision.action == "trip":
                # Refused only when every state that alone justifies the trip was read before the latest
                # operator clear; a fresh breach elsewhere trips even while another account is failing.
                basis = max((self._read_time(s, failing_accts, now) for s in decision.drivers), default=now)
                await self._halt.trip(decision.target, rule_id=decision.rule_id, reason=decision.reason,
                                      now_ms=now, basis_ms=basis)
            elif decision.action == "clear":
                await self._halt.system_clear(reason="cool_down", now_ms=now)
        except Exception as exc:
            if is_epoch_mismatch(exc):
                raise
            self._latched_page(Page("critical", "halt_decision_failed", None, f"{type(exc).__name__}: {exc}",
                                    now))
            return None
        self._unlatch("halt_decision_failed", None)
        self._page_inhibition_summaries(now)
        return decision

    def _page_inhibition_summaries(self, now: int) -> None:
        """One summary or escalation for the accounts whose dependent alerts were held back and whose
        suppression has ended (operations.md §9 Inhibition)."""
        if self._inhibitor is None or not self._inhibitor.holding:
            return
        for page in self._inhibitor.summaries(self.committed_states(), now):
            self._page(page)

    async def run(self) -> None:
        """Supervised loop: a full pass every interval_ms, signalled accounts at once in between."""
        interval = self._cfg.sweep.interval_ms
        next_full = self._clock()
        while not self._stopping:
            now = self._clock()
            if now >= next_full:
                accounts: list[str] | None = None
                next_full = now + interval
                self._signalled.clear()
            elif self._signalled:
                accounts = sorted(self._signalled)
                self._signalled.clear()
            else:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=max(0, next_full - now) / 1000)
                except TimeoutError:
                    pass
                continue
            started = self._clock()
            await self.run_pass(accounts)
            if accounts is None:
                self._check_pass_time(started, self._clock())

    def _check_pass_time(self, started: int, ended: int) -> None:
        """A full pass slower than the interval pages, latched until a full pass is fast again (§2)."""
        if ended - started > self._cfg.sweep.interval_ms:
            self._latched_page(Page("high", "slow_pass", None, f"{ended - started} ms", ended))
        else:
            self._unlatch("slow_pass", None)
