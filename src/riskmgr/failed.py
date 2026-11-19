"""FAILED: in memory first, the record with the immediate ping, the re-ping and the spool (design §B12, §B14, §B8
Immediate ping, PR4; plan S30a, R11-M05, RT-M5, ST-4).

`FailedManager` takes the due faults of `ErrorBudget` (`src/riskmgr/budget.py`) and, per episode:

1. **Memory first.** `enter(fault)` (the budget's listener, synchronous) puts the scope into `FailedScopes` at once:
   from that moment `covers()` (the arm-tracker's `FailedScopeView`), `series_failed()` (decision reads,
   `DATA_UNAVAILABLE(failed)`), `global_failed` (the BarClock and `md-bars` / `md-indicators` stop) and `summary()`
   (the dead-man's `arm_failed`) see it, before PostgreSQL does. The ping text is rendered here and its log mirror
   goes out through `Pager.log_mirror()` (`log.critical` only), never `EngineSession.page`, which would queue a second
   critical outbox row.
2. **The record.** `tick()` writes the `arm_failures` row, the ping's critical copy (`OutboxRow(kind="info",
   severity="critical")`, delivered by the Oversee relay) and its kind `arm` copy in one fenced transaction through
   `ArmFailuresStore.record` (S24), on the engine (owner) pool when the ARM pool is the fault (reason `pool_down`,
   or the pool observed failing), else on the ARM pool. A failed write is retried from memory every `retry_ms` until
   it commits; on its first failure the episode is appended to the `FailedSpoolPort` (CRC-framed, S31), so an engine
   that stops before PostgreSQL returns replays it at the next `start()` **before any advice**; the spool record is
   acknowledged once the row has committed. The database is always tried first and the spool is only the fallback:
   its first append **pages at once** (`ARM FAILED not recorded`, S38c), because a leader change before PostgreSQL
   returns loses the node-local spool, the in-memory episode and the counters (the new leader starts from zero). An episode folded after its append is appended again (replay keeps the
   latest record of a failure id). A lost fence raises (the caller hard-exits). The ping's position list is taken at
   write time from the live positions read after the entry (through the engine pool's store when the ARM pool fails);
   the first write waits up to `rows_every_ms` for such a read.
3. **SM11 FAILED.** Every non-terminal live position the scope covers takes the SM11 `SCOPE_FAILED` edge
   (`src/riskmgr/sm11.py`, the only place a state is decided): state `FAILED`, `prior_state` the state it left,
   `state_reason` `scope_failed`, written through the `row_version` CAS on the ARM pool; a CAS miss or a failing pool
   retries on the next tick (memory already holds the position).
4. **Re-ping** every `fail_reping_h` while the episode is open: `ArmFailuresStore.reping` (the same two outbox rows,
   the same ping id, in one fenced transaction).
5. **The collapsed ping** (§B14): an ADAPTER or ARM episode folds the episodes it covers that have not pinged yet
   (still in memory): they are recorded with `queue_ping=False` under the collapsing ping's id, and that one ping names
   them all. A scope that fails while an ARM (or, for a series, its ADAPTER) episode is open is folded the same way,
   so a pool or `redis-md` outage never becomes a storm of pings; it is still **named**: while the collapsing ping is
   unsent it is rendered (at write time) with the scope in its "also FAILED with it" line, and once that ping has gone
   out the collapsing episode re-pings at the next tick naming every scope folded since (one re-ping per tick however
   many fold). A folded episode whose collapsing episode is no longer open re-pings on its own at once if no sent ping
   named it, else every `fail_reping_h`. Every entry is log-mirrored at once, folded or not.
6. **Restart and takeover.** `start()` replays the spool, then reads the open `arm_failures` rows; until that read
   succeeds `FailedScopes.ready` is False and `covers()` / `series_failed()` hold every position (the ARM advises
   nothing). Once a read has failed, `summary()` reports the state unknown as a global FAILED (`count` at least 1,
   `global_failed` True), so the dead-man pages instead of reading "nothing FAILED".
7. **Clear** (S30b, §B14 Clearing): `submit_clear(request_id, target)` is the non-blocking `arm_clear_failed`
   handler: it only queues. The next `tick()` checks the target (`SCOPE_KIND:scope_id`, an open and recorded
   episode, no probe of that scope running) and starts the probe of the episode's recorded cause
   (`src/riskmgr/probes.py`, by `probe_kind`) as its own task under `probe_timeout_ms`, so the loop never waits on
   it. A refused probe is answered `cause_present_<check>[_first_<request>]` and FAILED stays; a passed one is written
   by the clear statement (`ArmFailedClearPort`, engine pool, fenced, under the advisory lock, linked to the
   request's audit row). Then, in memory: the scope leaves `FailedScopes`, the budget resets its counters
   (`ErrorBudget.reset`), the `ClearHooks` run (cross-check reset and SM12 `FAILED -> BACKFILLING` for a series, the
   job runner's circuit for a version, the post-clear bootstrap and the reset window for the ARM scope), one
   `FAILED_CLEARED` INFO notice is queued, and the request is answered `applied`. The last probe is noted on the row
   (`last_probe_result`, `arm failed show`). Neither the probe's trial bootstrap nor the post-clear bootstrap counts
   toward the reset budget (`src.marketdata.bootstrap`).
9. **Durable counters** (plan S38f, D7). `start()` restores the error budget's counters from PostgreSQL
   `arm_budget_counters` (`ArmBudgetStore.load`, `ErrorBudget.restore`) before the BarClock's first event, so a
   restart, a takeover or a `redis-md` loss never resets a streak: a restart is never itself a clean result. Every tick
   writes the counters that changed since the last write (`ErrorBudget.durable_rows`, fenced, one transaction; a
   counter that went back to rest is written with count 0); nothing is written until the restore has succeeded (it is
   retried each tick), so a failed read never overwrites the stored streaks with zeros; a late restore merges with
   what the term counted meanwhile (`ErrorBudget.restore`), and runs only after the open episodes are read (a stored
   counter at its threshold is filed only while its scope is open). An accepted clear writes the counters it resets
   (`ErrorBudget.reset_rows`) in the clear's own transaction (`ArmFailedClearPort.clear(..., reset=)`). Once per
   term, after the first live-position read, the unclean restarts (`RestartHistoryPort`: leader terms ended `lost` /
   `init_failed`, except a term SAFE_HOLD itself aborted) go to `ErrorBudget.note_restarts` with whether any position
   is armed: `fail_restarts_n` within `fail_restart_window_s` is ARM-global FAILED `unclean_restarts` with the
   immediate ping. A clean stop or a planned switchover never counts. A crash loop meets the SAFE_HOLD brake first
   (3 abnormal ends within 15 min, operations §4): no node leads, so no ARM runs; after the operator's resume the
   ARM counts the same crashes (resume clears SAFE_HOLD, not the ARM's budget) and goes FAILED if they still reach
   the limit within the window (design §B14).
8. **Restore.** Level-triggered on every tick: a live position in SM11 `FAILED` that no open scope covers takes the
   `FAILED_CLEARED` edge back to its `prior_state` (state reason `failed_cleared`) through the `row_version` CAS, so
   a restore lost to a CAS miss, a failing pool or a restart is done on a later tick (or by the next term). Replay
   (§B6.6, the BarClock) then re-decides the missed bars inside its horizon.

The ping (the §B8 allow-list, never a price or value): scope, reason, counters (counts and bucket times), the time of
the first counted fault, the affected positions (alias, `adapter_id:position_id`, symbol, side) bounded to
`max_positions` then "and K more; see arm failed show", the "no advice ... do not act on advice issued since" line and
the clear command. FAILED never writes `halt_events`, `halt_breaches` or `halt:state`, and never stops ingestion,
Oversee or the integrity worker: nothing here reaches them.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final, Protocol

from src.db.arm_ports import (
    TERMINAL_ARM_STATES,
    ArmBudgetStore,
    BudgetCounterRow,
    RestartHistoryPort,
    ArmFailedClearPort,
    ArmFailuresStore,
    ArmPositionRow,
    ArmStateStore,
    FailedPing,
    FailureRecord,
    OpenFailure,
    PositionCas,
    is_lost_fence,
)
from src.riskmgr.budget import (
    ARM_SCOPE,
    CIRCUIT_TASKS,
    POOL_DOWN,
    RESTART_REQUIRED,
    SERIES_FAILED,
    ErrorBudget,
    Fault,
    ScopeRef,
    adapter_due,
    adapter_of,
    probe_kind,
)
from src.riskmgr.config import FailSettings
from src.riskmgr.frontier import STUCK_FRONTIER
from src.riskmgr.ports import Clock, FailedSpoolPort, FailedSpoolRecord, Pager
from src.riskmgr.probes import (
    ADAPTER_SERIES,
    ARM_GLOBAL,
    ARM_POOL,
    CIRCUIT_OPEN,
    FAULT_DURING_PROBE,
    FRESH_BAR,
    INDICATOR_REBUILD,
    PASSED,
    PROBE_ERROR,
    PROBE_NOT_WIRED,
    PROBE_TIMEOUT,
    SERIES_REREAD,
    SPAN_WRITE,
    STRATEGY_CONFORMANCE,
    THREADS_HUNG,
    ClearHooks,
    ClearProbes,
    ProbeResult,
    probe_note,
    refusal_reason,
    refused_probe,
)
from src.riskmgr.render import FailedLine
from src.riskmgr.sm11 import ArmState, Refused, Sm11Event, next_state
from src.riskmgr.tracker import APPLIED, RequestAnswer, RequestAnswerPort, moot, refused
from src.riskmgr.types import ScopeKind, indicator_of_scope

log = logging.getLogger(__name__)

SCOPE_FAILED_REASON: Final = "scope_failed"
"""`arm_positions.state_reason` of a position a FAILED scope covers."""
FAILED_CLEARED_REASON: Final = "failed_cleared"
"""`arm_positions.state_reason` of a position restored to its `prior_state` after the clear."""
# `arm_clear_failed` answer codes besides the probe's `cause_present_...` (`src.riskmgr.probes.refusal_reason`)
BAD_TARGET: Final = "bad_target"
NOT_FAILED: Final = "not_failed"
NOT_RECORDED: Final = "not_recorded"
EPISODES_UNREAD: Final = "episodes_unread"
PROBE_RUNNING: Final = "probe_running"
PROBE_INTERRUPTED: Final = "probe_interrupted"
REQUEST_MISMATCH: Final = "request_mismatch"
CLEAR_WRITE_FAILED: Final = "clear_write_failed"
EPISODE_NOT_OPEN: Final = "episode_not_open"
PROBING: Final = "probing"
"""`last_probe_result` written by the ARM-global probe's fenced test write on the ARM pool."""
CLEAR_COMMAND: Final = "python -m src.ops arm failed clear"
"""The clear command; the ping names it with the target and `--reason <text>`."""
SHOW_COMMAND: Final = "arm failed show"
HOUR_MS: Final = 3_600_000


ARMED_STATES: Final = frozenset({"ARMED", "TRACKING", "ADVICE_OUTSTANDING", "SUSPENDED", "FAILED"})
"""Position states that count as armed for the unclean-restart counter (S38f): advice is (or would be) issued."""


@dataclass(frozen=True, kw_only=True)
class FailedSettings:
    max_positions: int = 10
    """Positions listed in a ping body before "and K more; see arm failed show"."""
    retry_ms: int = 5_000
    """A record or re-ping that failed is retried from memory after this long."""
    rows_every_ms: int = 5_000
    """The live positions are re-read at most this often while no episode is pending."""
    probe_timeout_ms: int = 1_200_000
    """A clear probe still running after this long is refused `probe_timeout` (a fresh-bar probe waits for the next
    15m bar end plus its `post_grace`; an ADAPTER probe runs its series' probes at the same time, under this one
    limit)."""
    probe_grace_ms: int = 60_000
    """The loop's backstop: a probe task with no result `probe_timeout_ms` plus this long after it started (a port
    that ignores its cancellation) is abandoned and refused `probe_timeout`."""

    def __post_init__(self) -> None:
        if (self.max_positions < 1 or self.retry_ms <= 0 or self.rows_every_ms <= 0 or self.probe_timeout_ms <= 0
                or self.probe_grace_ms <= 0):
            raise ValueError("FAILED settings are positive")


@dataclass(frozen=True)
class FailedStores:
    """`arm`: the store on the ARM pool; `engine`: the same store on the engine (owner) pool, used when the ARM pool
    is the fault and for the open-episode read."""

    arm: ArmFailuresStore
    engine: ArmFailuresStore


class PositionSeriesPort(Protocol):
    def series_of(self, row: ArmPositionRow) -> Sequence[str] | None:
        """The series (canonical ids) the position's pinned specs read: trigger-side bars, FirstBarGuard bars and
        indicator inputs (the arm-tracker's book, from memory). None when unknown: then every series of the
        position's adapter and symbol is taken to cover it (fail closed)."""
        ...


@dataclass(frozen=True, kw_only=True)
class FailedSummary:
    """The dead-man's `arm_failed` and `arm_status`'s `failed_scopes` / `failed_global`, from memory. While the open
    episodes are unreadable (every position held) it is `count >= 1, global_failed=True`."""

    count: int
    global_failed: bool
    unrecorded: int = 0
    """Episodes whose PostgreSQL write failed and that are held only in memory and this node's spool (S38c review):
    the dead-man carries the count off-node, independent of PostgreSQL."""

    def payload(self) -> dict[str, object]:
        return {"count": self.count, "global_failed": self.global_failed, "unrecorded": self.unrecorded}


@dataclass
class FailedCounters:
    entered: int = 0
    folded: int = 0
    duplicates: int = 0
    recorded: int = 0
    record_errors: int = 0
    spooled: int = 0
    spool_errors: int = 0
    spool_page_errors: int = 0
    replayed: int = 0
    repings: int = 0
    reping_errors: int = 0
    positions_failed: int = 0
    position_errors: int = 0
    clears_refused: int = 0
    clears_applied: int = 0
    clears_moot: int = 0
    clear_errors: int = 0
    probes: int = 0
    probe_errors: int = 0
    positions_restored: int = 0
    restore_errors: int = 0
    restore_refused: int = 0
    load_errors: int = 0
    budget_errors: int = 0


@dataclass
class Episode:
    """One open FAILED episode in memory. `committed`: its row is in PostgreSQL; `queue_ping` False: folded into the
    collapsing episode whose ping id it carries; `named` False: folded after that ping went out, and no ping has named
    it yet; `respool`: its record changed (a fold) after it was spooled, so it is appended again."""

    scope: ScopeRef
    record: FailureRecord
    epoch: int
    ping: FailedPing
    queue_ping: bool = True
    committed: bool = False
    spooled: bool = False
    next_try_ms: int = 0
    ping_count: int = 0
    last_ping_at_ms: int | None = None
    named: bool = True
    respool: bool = False


class _Running(Protocol):
    """A probe task (`asyncio.create_task`), typed structurally: `asyncio.Task` is off the lockout allow-list."""

    def done(self) -> bool: ...

    def cancel(self) -> bool: ...


class _Done(Protocol):
    """A finished task as its done callback sees it."""

    def cancelled(self) -> bool: ...

    def exception(self) -> BaseException | None: ...


@dataclass
class _Probe:
    """One clear probe in flight: the request, the episode it probes and, once done, its result (or the lost-fence
    error the tick re-raises). `refault_at_ms`: a new fault of the scope arrived while it ran (the clear is then
    refused `fault_during_probe`)."""

    request_id: int
    scope: ScopeRef
    failure_id: str
    started_ms: int
    task: _Running | None = None
    result: ProbeResult | None = None
    error: BaseException | None = None
    refault_at_ms: int | None = None


class FailedClearedPort(Protocol):
    def failed_cleared(self, failure_id: str, scope_kind: str, scope_id: str, at_ms: int) -> None:
        """Queue the FAILED_CLEARED INFO notice (`ArmNotices.failed_cleared`)."""
        ...


# ---------------------------------------------------------------------------------------------------- pure helpers
def _retrieve(task: _Done) -> None:
    """Done callback of an ADAPTER probe's series task: its outcome is retrieved, so a cancelled or raising task no
    longer awaited is not reported as never retrieved."""
    if not task.cancelled():
        task.exception()


def parse_target(target: str) -> ScopeRef | None:
    """An `arm_clear_failed` target `SCOPE_KIND:scope_id` as a scope; None when it does not parse. Surrounding
    whitespace does not parse: the clear statement compares the target as filed, so a padded one would only be
    refused after its probe."""
    if target != target.strip():
        return None
    kind, sep, scope_id = target.partition(":")
    if not sep:
        return None
    try:
        return ScopeRef(ScopeKind(kind), scope_id)
    except ValueError:
        return None


def own_ping_id(failure_id: str) -> str:
    """The ping id an episode pings under when it is not folded (`f…` → `p…`)."""
    return "p" + failure_id[1:]


def episode_ids(scope: ScopeRef, reason: str, epoch: int, entered_ms: int) -> tuple[str, str]:
    """`(failure_id, ping_id)`: `f<epoch>-<entered ms>-<8 hex of scope and reason>` and the same with `p`."""
    digest = hashlib.sha256(f"{scope.target}|{reason}".encode()).hexdigest()[:8]
    failure_id = f"f{epoch}-{entered_ms}-{digest}"
    return failure_id, own_ping_id(failure_id)


def row_version_scopes(row: ArmPositionRow) -> tuple[str, ...] | None:
    """The STRATEGY_VERSION scope ids the stored pin touches (`strategy:<KIND>:<VERSION>`, `guard:<KIND>:<VERSION>`,
    as `src.riskmgr.tracker.version_scopes`); None when the stored guard list does not parse."""
    scopes = [versions_strategy(row)]
    try:
        guards = json.loads(row.guards_json)
        if not isinstance(guards, list):
            return None
        for g in guards:
            if not isinstance(g, dict) or not isinstance(g.get("kind"), str) or not isinstance(g.get("version"), int):
                return None
            scopes.append(f"guard:{g['kind']}:{g['version']}")
    except ValueError:
        return None
    return tuple(scopes)


def scope_covers_row(scope: ScopeRef, row: ArmPositionRow, series: Sequence[str] | None) -> bool:
    """Whether one FAILED scope covers a stored position. `series`: the series its pinned specs read; None
    (unknown) matches every series of its adapter and symbol, and an unparseable guard list matches every guard
    scope (fail closed)."""
    adapter = row.key.adapter_id
    if scope.kind is ScopeKind.ARM:
        return True
    if scope.kind is ScopeKind.POSITION:
        return scope.scope_id == row.ref
    if scope.kind is ScopeKind.ADAPTER:
        return scope.scope_id == adapter
    if scope.kind is ScopeKind.SERIES:
        if series is None:
            return scope.scope_id.startswith(f"{adapter}:{row.symbol}:")
        return scope.scope_id in series
    versions = row_version_scopes(row)
    if versions is None:
        return scope.scope_id.startswith("guard:") or scope.scope_id == versions_strategy(row)
    return scope.scope_id in versions


def versions_strategy(row: ArmPositionRow) -> str:
    return f"strategy:{row.strategy_kind}:{row.strategy_impl_version}"


def _utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def render_ping(record: FailureRecord, *, created_ms: int, rows: Mapping[str, ArmPositionRow],
                alias: Callable[[str, str], str] | None = None, folded: Sequence[tuple[ScopeRef, str]] = (),
                max_positions: int = 10, reping: int = 0) -> FailedPing:
    """The immediate ping (§B14): ids, codes, counts and times only. `rows` maps a position ref to its row (for the
    alias, symbol and side; a ref without a row is listed by its ref alone). `reping` > 0 marks a re-ping."""
    target = f"{record.scope_kind}:{record.scope_id}"
    head = "ARM FAILED" if reping == 0 else f"ARM FAILED re-ping {reping}"
    title = f"{head} [{record.ping_id}] {record.scope_kind} {record.scope_id}"
    lines = [f"scope: {record.scope_kind} {record.scope_id}", f"reason: {record.reason}"]
    if record.edge_code:
        lines.append(f"edge: {record.edge_code}")
    if record.counters:
        lines.append("counters: " + ", ".join(f"{k}={v}" for k, v in record.counters))
    lines.append(f"first counted fault: {_utc(record.first_fault_at_ms)}")
    if folded:
        lines.append("also FAILED with it: " + "; ".join(f"{s.kind.value} {s.scope_id} ({r})" for s, r in folded))
    refs = list(record.position_ids)
    lines.append(f"positions ({len(refs)}):")
    for ref in refs[:max_positions]:
        row = rows.get(ref)
        if row is None:
            lines.append(f"  {ref}")
            continue
        name = alias(row.key.adapter_id, row.account_id) if alias is not None else row.account_id
        lines.append(f"  {name} {ref} {row.symbol} {row.side}")
    if len(refs) > max_positions:
        lines.append(f"  and {len(refs) - max_positions} more; see {SHOW_COMMAND}")
    lines.append("no advice for these positions until FAILED is cleared; do not act on advice for them issued "
                 f"since {_utc(record.first_fault_at_ms)}")
    lines.append(f"clear, after fixing the cause: {CLEAR_COMMAND} {target} --reason <text>")
    return FailedPing(title=title, body="\n".join(lines), created_ms=created_ms)


def spool_record(ep: Episode) -> FailedSpoolRecord:
    r = ep.record
    return FailedSpoolRecord(failure_id=r.failure_id, scope_kind=ScopeKind(r.scope_kind), scope_id=r.scope_id,
                             reason=r.reason, probe_kind=r.probe_kind, ping_id=r.ping_id, epoch=ep.epoch,
                             first_fault_at_ms=r.first_fault_at_ms, entered_at_ms=r.entered_at_ms,
                             counters=r.counters, position_ids=r.position_ids, edge_code=r.edge_code)


def record_of_spool(s: FailedSpoolRecord) -> FailureRecord:
    return FailureRecord(failure_id=s.failure_id, scope_kind=s.scope_kind.value, scope_id=s.scope_id,
                         reason=s.reason, probe_kind=s.probe_kind, ping_id=s.ping_id,
                         first_fault_at_ms=s.first_fault_at_ms, entered_at_ms=s.entered_at_ms, edge_code=s.edge_code,
                         counters=s.counters, position_ids=s.position_ids)


# ---------------------------------------------------------------------------------------------------- the scopes
class FailedScopes:
    """The open FAILED scopes in memory (the truth for advice; PostgreSQL is the durable copy). Until the open
    episodes have been read (`ready`), every position is held (fail closed)."""

    def __init__(self) -> None:
        self.ready = False
        self.unread = False
        """The last read of the open episodes failed (`ready` is False until one succeeds)."""
        self.episodes: dict[ScopeRef, Episode] = {}

    @property
    def global_failed(self) -> bool:
        return ARM_SCOPE in self.episodes

    def is_open(self, scope: ScopeRef) -> bool:
        return scope in self.episodes

    def covers(self, adapter_id: str, series: Sequence[str], version_scopes: Sequence[str],
               position: str | None = None) -> bool:
        """A FAILED scope (ARM, the adapter, one of the series, a strategy or guard version, or the position itself
        when its ref `adapter_id:position_id` is given) covers a position with these pinned specs (the arm-tracker's
        `FailedScopeView`)."""
        if not self.ready or self.global_failed:
            return True
        if position is not None and ScopeRef(ScopeKind.POSITION, position) in self.episodes:
            return True
        if ScopeRef(ScopeKind.ADAPTER, adapter_id) in self.episodes:
            return True
        if any(ScopeRef(ScopeKind.SERIES, s) in self.episodes for s in series):
            return True
        return any(ScopeRef(ScopeKind.STRATEGY_VERSION, v) in self.episodes for v in version_scopes)

    def covers_row(self, row: ArmPositionRow, series: Sequence[str] | None) -> bool:
        """`covers` for a stored row (`scope_covers_row` over every open scope)."""
        if not self.ready:
            return True
        return any(scope_covers_row(s, row, series) for s in self.episodes)

    def series_failed(self, series: str) -> bool:
        """Decision reads of this series return `DATA_UNAVAILABLE(failed)` (§B14 Effect)."""
        if not self.ready or self.global_failed:
            return True
        scope = ScopeRef(ScopeKind.SERIES, series)
        return scope in self.episodes or ScopeRef(ScopeKind.ADAPTER, adapter_of(scope) or "") in self.episodes

    def open_scopes(self) -> list[FailedLine]:
        """The open scopes as status lines (the notices' `FailedLinesPort`)."""
        return [FailedLine(scope_kind=s.kind.value, scope_id=s.scope_id, reason=e.record.reason)
                for s, e in sorted(self.episodes.items())]

    def summary(self) -> FailedSummary:
        """From memory. Once a read of the open episodes has failed (and until one succeeds) every position is held
        and the open set is unknown: reported as a global FAILED, so the off-PC receiver pages. Before the first read
        (the start-up instant) it reports what memory holds."""
        if not self.ready and self.unread:
            return FailedSummary(count=max(len(self.episodes), 1), global_failed=True)
        return FailedSummary(count=len(self.episodes), global_failed=self.global_failed)

    def open_series_by_adapter(self) -> dict[str, list[str]]:
        """Open SERIES episodes per adapter; an indicator's guard-input scope (round 13) is not a failed series."""
        out: dict[str, list[str]] = {}
        for s in self.episodes:
            if s.kind is ScopeKind.SERIES and indicator_of_scope(s.scope_id) is None:
                out.setdefault(adapter_of(s) or "", []).append(s.scope_id)
        return out

    def open_adapters(self) -> set[str]:
        return {s.scope_id for s in self.episodes if s.kind is ScopeKind.ADAPTER}


# ---------------------------------------------------------------------------------------------------- the manager
class FailedManager:
    """Enter FAILED, record, ping, re-ping and spool (S30a). One instance per term, driven by the ARM task loop."""

    def __init__(self, *, budget: ErrorBudget, stores: FailedStores, positions: ArmStateStore,
                 spool: FailedSpoolPort, pager: Pager, clock: Clock, epoch: Callable[[], int], fail: FailSettings,
                 settings: FailedSettings | None = None, series_of: PositionSeriesPort | None = None,
                 alias: Callable[[str, str], str] | None = None, answers: RequestAnswerPort | None = None,
                 fatal: Callable[[BaseException], bool] | None = None,
                 engine_positions: ArmStateStore | None = None, probes: ClearProbes | None = None,
                 clear: ArmFailedClearPort | None = None, hooks: ClearHooks | None = None,
                 cleared_notice: FailedClearedPort | None = None, budget_store: ArmBudgetStore | None = None,
                 restarts: RestartHistoryPort | None = None) -> None:
        """`engine_positions`: the live-position reads on the engine (owner) pool, used when the ARM pool fails (the
        ping's position list); SM11 writes always go through `positions` (the ARM pool). `probes` and `clear` (the
        engine pool's clear statement): without either, every clear is refused `probe_not_wired`. `hooks` and
        `cleared_notice` (the FAILED_CLEARED notice) run after an accepted clear. `budget_store` (the ARM pool's
        durable error-budget counters) and `restarts` (the unclean-restart history on the engine pool): S38f; without
        them the counters live in memory only and restarts are not counted."""
        self._budget = budget
        self._stores = stores
        self._positions = positions
        self._engine_positions = engine_positions
        self._spool = spool
        self._pager = pager
        self._clock = clock
        self._epoch = epoch
        self._fail = fail
        self._cfg = settings or FailedSettings()
        self._series_of = series_of
        self._alias = alias
        self._answers = answers
        self._fatal = fatal or is_lost_fence
        self.scopes = FailedScopes()
        self.counters = FailedCounters()
        self._rows: dict[str, ArmPositionRow] = {}
        self._rows_at_ms: int | None = None
        self._clears: list[tuple[int, str]] = []
        self._probe_ports = probes
        self._clear = clear
        self._hooks = hooks
        self._notices = cleared_notice
        self._probes: dict[ScopeRef, _Probe] = {}
        self._restore_warned: set[str] = set()
        self._reload_due = False
        self._budget_store = budget_store
        self._budget_restored = budget_store is None
        self._budget_saved: dict[tuple[str, str, str], BudgetCounterRow] = {}
        self._restarts = restarts
        self._restarts_checked = restarts is None
        budget.listen(self.enter)

    # ------------------------------------------------------------------------------------------------ ports out
    def covers(self, adapter_id: str, series: Sequence[str], version_scopes: Sequence[str],
               position: str | None = None) -> bool:
        return self.scopes.covers(adapter_id, series, version_scopes, position)

    def open_scopes(self) -> list[FailedLine]:
        return self.scopes.open_scopes()

    def summary(self) -> FailedSummary:
        """The scopes' summary plus the episodes held only on this node (spooled after a failed PostgreSQL write)."""
        unrecorded = sum(1 for e in self.pending() if e.spooled)
        return replace(self.scopes.summary(), unrecorded=unrecorded)

    def stuck_positions(self) -> tuple[str, ...]:
        """The positions (`adapter_id:position_id`) whose POSITION `stuck_frontier` scope is open (S38c review: they
        count toward the ARM-global stuck-frontier bound, `ErrorBudget.observe_frontier(failed_stuck=...)`)."""
        return tuple(sorted(s.scope_id for s, e in self.scopes.episodes.items()
                            if s.kind is ScopeKind.POSITION and e.record.reason == STUCK_FRONTIER))

    def pending(self) -> list[Episode]:
        """Episodes whose row has not committed yet (in memory, maybe spooled)."""
        return [e for e in self.scopes.episodes.values() if not e.committed]

    # ------------------------------------------------------------------------------------------------ entry
    def _covered_refs(self, scope: ScopeRef) -> list[str]:
        return [ref for ref, row in self._rows.items()
                if row.state not in TERMINAL_ARM_STATES and scope_covers_row(scope, row, self._row_series(row))]

    def _row_series(self, row: ArmPositionRow) -> Sequence[str] | None:
        if self._series_of is None:
            return None
        try:
            return self._series_of.series_of(row)
        except Exception:
            return None

    def _render(self, record: FailureRecord, created_ms: int, *, folded: Sequence[tuple[ScopeRef, str]] = (),
                reping: int = 0) -> FailedPing:
        return render_ping(record, created_ms=created_ms, rows=self._rows, alias=self._alias, folded=folded,
                           max_positions=self._cfg.max_positions, reping=reping)

    def _collapser(self, scope: ScopeRef) -> Episode | None:
        """The open episode a new `scope` folds into: the ARM episode, or a series' ADAPTER episode."""
        arm = self.scopes.episodes.get(ARM_SCOPE)
        if arm is not None and scope != ARM_SCOPE:
            return arm
        if scope.kind is ScopeKind.SERIES:
            return self.scopes.episodes.get(ScopeRef(ScopeKind.ADAPTER, adapter_of(scope) or ""))
        return None

    def _foldable(self, scope: ScopeRef) -> list[Episode]:
        """Pending, not yet folded episodes a new ADAPTER / ARM episode covers (they have not pinged)."""
        out = []
        for e in self.scopes.episodes.values():
            if e.committed or not e.queue_ping or e.scope == scope:
                continue
            if scope == ARM_SCOPE or (scope.kind is ScopeKind.ADAPTER and adapter_of(e.scope) == scope.scope_id):
                out.append(e)
        return out

    def _owner(self, ep: Episode) -> Episode | None:
        """The open episode that pings under `ep`'s ping id on its behalf (its collapsing episode); None when `ep`
        pings for itself or its collapsing episode is no longer open."""
        if ep.queue_ping:
            return None
        return next((e for e in self.scopes.episodes.values()
                     if e.queue_ping and e is not ep and e.record.ping_id == ep.record.ping_id), None)

    def _folded_of(self, ep: Episode) -> list[Episode]:
        """The open episodes folded under `ep`'s ping (only a pinging episode has any), by scope."""
        if not ep.queue_ping:
            return []
        return sorted((e for e in self.scopes.episodes.values()
                       if not e.queue_ping and e is not ep and e.record.ping_id == ep.record.ping_id),
                      key=lambda e: e.scope)

    @staticmethod
    def _folded_pairs(folded: Sequence[Episode]) -> list[tuple[ScopeRef, str]]:
        return [(e.scope, e.record.reason) for e in folded]

    def enter(self, fault: Fault) -> None:
        """The budget's listener: the scope stops advising **now**, in memory; the record follows in `tick()`."""
        now = self._clock.now_ms()
        if fault.scope in self.scopes.episodes:
            self.counters.duplicates += 1
            probe = self._probes.get(fault.scope)
            if probe is not None and probe.refault_at_ms is None:
                probe.refault_at_ms = now   # the probe's pass would erase this evidence: its clear is refused
            return
        epoch = self._epoch()
        failure_id, own_id = episode_ids(fault.scope, fault.reason, epoch, now)
        collapser = self._collapser(fault.scope)
        folding = [] if collapser is not None else (
            self._foldable(fault.scope) if fault.scope.kind in (ScopeKind.ARM, ScopeKind.ADAPTER) else [])
        refs = set(fault.position_ids) | set(self._covered_refs(fault.scope))
        for e in folding:
            refs |= set(e.record.position_ids)
        ping_id = own_id if collapser is None else collapser.record.ping_id
        record = FailureRecord(
            failure_id=failure_id, scope_kind=fault.scope.kind.value, scope_id=fault.scope.scope_id,
            reason=fault.reason, probe_kind=probe_kind(fault.scope.kind, fault.reason), ping_id=ping_id,
            first_fault_at_ms=fault.first_fault_at_ms, entered_at_ms=now, edge_code=fault.edge_code,
            counters=tuple(sorted(fault.counters)), position_ids=tuple(sorted(refs)))
        old_ids = {e.record.ping_id for e in folding}
        for e in self.scopes.episodes.values():
            if any(e is f for f in folding) or (not e.committed and e.record.ping_id in old_ids):
                e.queue_ping = False        # folded (or folded into one now folded): the new ping names it
                e.named = True
                e.record = replace(e.record, ping_id=ping_id)
                e.ping = self._render(e.record, e.ping.created_ms)
                e.respool = e.spooled       # the spool still holds its old ping id: append it again
        ep = Episode(scope=fault.scope, record=record, epoch=epoch, ping=self._render(record, now),
                     queue_ping=collapser is None)
        self.scopes.episodes[fault.scope] = ep
        if collapser is not None:
            owner = self._owner(ep)
            # named by the collapsing ping while that is unsent (it is rendered at write time); else the collapsing
            # episode (or, with none open, this one) re-pings at the next tick naming it
            ep.named = owner is not None and not owner.committed
        ep.ping = self._render(record, now, folded=self._folded_pairs(self._folded_of(ep)))
        self.counters.entered += 1
        self.counters.folded += len(folding) + (0 if collapser is None else 1)
        self._pager.log_mirror(ep.ping.title, ep.ping.body)
        if fault.scope.kind is ScopeKind.SERIES:
            for due in adapter_due(self.scopes.open_series_by_adapter(), self.scopes.open_adapters(),
                                   self._fail.global_series_n):
                firsts = [self.scopes.episodes[ScopeRef(ScopeKind.SERIES, s)].record.first_fault_at_ms
                          for s in due.series]
                self.enter(Fault(scope=ScopeRef(ScopeKind.ADAPTER, due.adapter_id), reason=SERIES_FAILED,
                                 first_fault_at_ms=min(firsts), counters=(("failed_series", len(due.series)),)))

    # ------------------------------------------------------------------------------------------------ start
    async def start(self) -> bool:
        """Replay the spool, then read the open episodes (before the BarClock's first event). False while the open
        episodes cannot be read: every position stays held and `tick()` retries."""
        try:
            spooled = await self._spool.replay()
        except Exception as e:
            if self._fatal(e):
                raise
            spooled = ()
            log.warning("arm failed: spool replay failed (%r)", type(e))
        for s in spooled:
            self._adopt_spooled(s)
        loaded = await self._load_open(initial=True)
        await self._restore_budget()
        return loaded

    # ------------------------------------------------------------------------------------------------ S38f counters
    async def _restore_budget(self) -> None:
        """Adopt the stored error-budget counters (once; retried each tick until the read succeeds), after the open
        episodes have been read: a restored counter at its threshold counts as filed only while its scope is open.
        A late restore merges with what this term has counted meanwhile (`ErrorBudget.restore`)."""
        if self._budget_restored or self._budget_store is None or not self.scopes.ready:
            return
        try:
            rows = await self._budget_store.load()
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.budget_errors += 1
            log.warning("arm failed: error-budget counters unreadable (%r); retried, nothing written", type(e))
            return
        adopted = self._budget.restore(rows, self.scopes.episodes.keys())
        self._budget_saved = {r.key: r for r in rows}
        self._budget_restored = True
        if adopted:
            log.info("arm failed: %d error-budget counters restored", adopted)

    async def _save_budget(self, now: int) -> None:
        """Write the counters that changed since the last write (fenced, one transaction); a counter that went back
        to rest is written with count 0. Nothing before the restore has succeeded."""
        if not self._budget_restored or self._budget_store is None:
            return
        current = self._budget.durable_rows()
        changed = [r for k, r in current.items() if self._budget_saved.get(k) != r]
        changed += [BudgetCounterRow(scope_kind=k[0], scope_id=k[1], counter=k[2], count=0,
                                     last_event_ms=old.last_event_ms if k[2] == "unclean_restarts" else None)
                    for k, old in self._budget_saved.items() if k not in current and old.count > 0]
        if not changed:
            return
        try:
            await self._budget_store.save(changed, now)
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.budget_errors += 1
            log.warning("arm failed: error-budget counters not written (%r); retried next tick", type(e))
            return
        for r in changed:
            self._budget_saved[r.key] = r

    async def _check_restarts(self, now: int) -> None:
        """Once per term, after the first live-position read: the unclean restarts in the window (S38f)."""
        if self._restarts_checked or self._restarts is None or self._rows_at_ms is None or not self._budget_restored:
            return
        since = now - self._fail.restart_window_s * 1000
        try:
            ends = await self._restarts.unclean_ends_since(since)
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.budget_errors += 1
            log.warning("arm failed: restart history unreadable (%r); retried next tick", type(e))
            return
        armed = any(r.state in ARMED_STATES for r in self._rows.values())
        self._restarts_checked = True
        self._budget.note_restarts(ends, now, armed=armed)

    def _adopt_spooled(self, s: FailedSpoolRecord) -> None:
        """One replayed record; a later record of the same failure id (appended after a fold) replaces it."""
        scope = ScopeRef(s.scope_kind, s.scope_id)
        mine = self.scopes.episodes.get(scope)
        if mine is not None and not (mine.spooled and not mine.committed and mine.record.failure_id == s.failure_id):
            return
        record = record_of_spool(s)
        ping = self._render(record, s.entered_at_ms)
        self.scopes.episodes[scope] = Episode(scope=scope, record=record, epoch=s.epoch, ping=ping,
                                              queue_ping=s.ping_id == own_ping_id(s.failure_id), spooled=True)
        self.counters.replayed += 1

    async def _load_open(self, *, initial: bool = False) -> bool:
        try:
            stored = await self._stores.engine.open_episodes()
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.load_errors += 1
            self.scopes.unread = True
            log.warning("arm failed: open episodes unreadable (%r); advising nothing", type(e))
            return False
        by_scope: dict[ScopeRef, OpenFailure] = {}
        for of in stored:
            try:
                by_scope[ScopeRef(ScopeKind(of.record.scope_kind), of.record.scope_id)] = of
            except ValueError:
                log.warning("arm failed: open episode %s has an unknown scope", of.record.failure_id)
        acks: list[str] = []
        for scope, of in by_scope.items():
            mine = self.scopes.episodes.get(scope)
            if mine is not None and mine.record.failure_id != of.record.failure_id and mine.spooled:
                acks.append(mine.record.failure_id)
            if mine is not None and mine.record.failure_id == of.record.failure_id:
                if mine.spooled and not mine.committed:
                    acks.append(mine.record.failure_id)
                mine.committed = True
                mine.ping_count, mine.last_ping_at_ms = of.ping_count, of.last_ping_at_ms
                if mine.record.ping_id != of.record.ping_id:     # the stored row is the truth for its ping id
                    mine.record = replace(mine.record, ping_id=of.record.ping_id)
                    mine.queue_ping = of.record.ping_id == own_ping_id(of.record.failure_id)
                continue
            rec = of.record
            self.scopes.episodes[scope] = Episode(
                scope=scope, record=rec, epoch=of.epoch, committed=True, ping_count=of.ping_count,
                last_ping_at_ms=of.last_ping_at_ms, queue_ping=rec.ping_id == own_ping_id(rec.failure_id),
                ping=self._render(rec, rec.entered_at_ms))
        if not initial:
            for scope in [s for s, e in self.scopes.episodes.items() if e.committed and s not in by_scope]:
                log.warning("arm failed: %s is no longer open in PostgreSQL", scope.target)
                del self.scopes.episodes[scope]
        self.scopes.ready = True
        self.scopes.unread = False
        self._reload_due = False
        await self._ack(acks)
        return True

    async def _ack(self, failure_ids: Sequence[str]) -> None:
        if not failure_ids:
            return
        try:
            await self._spool.acknowledge(list(failure_ids))
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.spool_errors += 1
            log.warning("arm failed: spool acknowledge failed (%r)", type(e))

    # ------------------------------------------------------------------------------------------------ the loop
    async def tick(self) -> None:
        """One pass of the ARM loop: deferred faults, open-episode (re)read, clears (finished probes, then new ones),
        rows, records, SM11 FAILED and its restore, re-pings. A lost fence raises; any other error is retried on the
        next tick."""
        now = self._clock.now_ms()
        self._budget.poll(now)
        if not self.scopes.ready or self._reload_due:
            await self._load_open(initial=not self.scopes.ready)
        await self._restore_budget()
        await self._finish_probes(now)
        self._start_clears(now)
        await self._refresh_rows(now)
        await self._check_restarts(now)
        await self._flush(now)
        await self._mark_positions(now)
        await self._restore_positions(now)
        await self._reping(now)
        await self._save_budget(now)

    async def _refresh_rows(self, now: int) -> None:
        busy = bool(self.scopes.episodes)
        if not busy and self._rows_at_ms is not None and now - self._rows_at_ms < self._cfg.rows_every_ms:
            return
        sources = [self._positions]
        if self._engine_positions is not None:
            sources = ([self._engine_positions, self._positions] if self._budget.pool_failing
                       else [self._positions, self._engine_positions])
        for source in sources:
            try:
                rows = await source.live_positions()
            except Exception as e:
                if self._fatal(e):
                    raise
                log.warning("arm failed: live positions unreadable (%r)", type(e))
                continue
            self._rows = {r.ref: r for r in rows}
            self._rows_at_ms = now
            return

    def _rows_fresh(self, ep: Episode) -> bool:
        return self._rows_at_ms is not None and self._rows_at_ms >= ep.record.entered_at_ms

    def _rows_wait(self, ep: Episode, now: int) -> bool:
        """The first write waits (at most `rows_every_ms` after the entry) for a live-position read taken after the
        entry, so the ping never lists a stale or empty position set when one read away."""
        return not self._rows_fresh(ep) and now - ep.record.entered_at_ms < self._cfg.rows_every_ms

    def _prepare(self, ep: Episode, folded: Sequence[Episode]) -> None:
        """Before a write: the positions the scope (and what it folds) covers now, and the ping rendered with them
        and with the scopes folded under it (its creation time kept)."""
        if self._rows_fresh(ep):
            refs = set(ep.record.position_ids) | set(self._covered_refs(ep.scope))
            for f in folded:
                refs |= set(f.record.position_ids)
            ep.record = replace(ep.record, position_ids=tuple(sorted(refs)))
        ep.ping = self._render(ep.record, ep.ping.created_ms, folded=self._folded_pairs(folded))

    def _store_for(self, ep: Episode) -> ArmFailuresStore:
        if ep.record.reason == POOL_DOWN or self._budget.pool_failing:
            return self._stores.engine
        return self._stores.arm

    def _due(self, now: int, *, folded: bool) -> list[Episode]:
        """Pending episodes due a write: the pinging ones first; a folded one only once its collapsing episode (the
        pending one pinging under its ping id) has committed, so the row never claims a ping not yet queued."""
        pending = [e for e in self.scopes.episodes.values() if not e.committed and e.next_try_ms <= now]
        if not folded:
            return sorted((e for e in pending if e.queue_ping), key=lambda e: e.record.entered_at_ms)
        pinging = {e.record.ping_id for e in self.scopes.episodes.values() if not e.committed and e.queue_ping}
        return sorted((e for e in pending if not e.queue_ping and e.record.ping_id not in pinging),
                      key=lambda e: e.record.entered_at_ms)

    async def _flush(self, now: int) -> None:
        acks: list[str] = []
        for folded in (False, True):
            for ep in self._due(now, folded=folded):
                if self._rows_wait(ep, now):
                    continue
                if await self._write(ep, now) and ep.spooled:
                    acks.append(ep.record.failure_id)
        await self._ack(acks)
        for ep in self.pending():
            if ep.respool:
                await self._spool_once(ep)

    async def _write(self, ep: Episode, now: int) -> bool:
        """One record attempt; True once the row is in PostgreSQL (inserted now, or there already)."""
        folded = self._folded_of(ep)
        self._prepare(ep, folded)
        record, queue_ping = ep.record, ep.queue_ping
        try:
            inserted = await self._store_for(ep).record(record, ep.ping, queue_ping=queue_ping)
        except Exception as e:
            if self._fatal(e):
                raise
            ep.next_try_ms = now + self._cfg.retry_ms
            self.counters.record_errors += 1
            log.warning("arm failed: record of %s failed (%r); retrying from memory", ep.record.failure_id,
                        type(e))
            await self._spool_once(ep)
            return False
        ep.committed = True
        ep.ping_count, ep.last_ping_at_ms = 1, ep.ping.created_ms
        if ep.record.ping_id != record.ping_id:     # folded while the write was in flight: the row is the truth
            ep.record, ep.queue_ping = replace(ep.record, ping_id=record.ping_id), queue_ping
        self.counters.recorded += int(inserted)
        if inserted and queue_ping:
            for f in self._folded_of(ep):           # folded while the write was in flight: not in the sent ping
                f.named = any(f is g for g in folded)
        if not inserted:
            self._reload_due = True     # a replay of a committed row, or another open episode of the scope
        return True

    async def _spool_once(self, ep: Episode) -> None:
        if ep.spooled and not ep.respool:
            return
        try:
            await self._spool.append(spool_record(ep))
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.spool_errors += 1
            log.warning("arm failed: spool append of %s failed (%r)", ep.record.failure_id, type(e))
            return
        first = not ep.spooled
        ep.spooled = True
        ep.respool = False
        self.counters.spooled += 1
        if first:
            self._page_spooled(ep)

    def _page_spooled(self, ep: Episode) -> None:
        """S38c (round 12, AWS leader change): the episode could not be recorded and lives only in memory and this
        node's spool, so a leader change before PostgreSQL returns would lose it (the peer starts with no row and
        fresh counters). Page at once, naming the ping id. This page goes through the engine's notification outbox,
        which needs PostgreSQL, so the dead-man's `arm_failed.unrecorded` carries the condition off-node without it
        (the receiver pages at once and latches it across a leader change, S38c review)."""
        r = ep.record
        try:
            self._pager.page("critical", f"ARM FAILED not recorded [{r.ping_id}]",
                             f"scope: {r.scope_kind} {r.scope_id}\nreason: {r.reason}\n"
                             "the FAILED episode is held in memory and this node's spool only (PostgreSQL write "
                             "failed); it is recorded when PostgreSQL returns. A leader change before then loses it: "
                             "the new leader starts with no episode and fresh counters, so check `arm failed show` "
                             "after any takeover")
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.spool_page_errors += 1

    async def drain(self) -> None:
        """On stop: every episode whose row has not committed goes to the spool (replayed at the next start); a
        clear probe still running is stopped and its request answered `probe_interrupted` (the operator files it
        again), as is a queued clear."""
        for ep in self.pending():
            await self._spool_once(ep)
        probes, self._probes = list(self._probes.values()), {}
        for p in probes:
            if p.task is not None and not p.task.done():
                p.task.cancel()
            self._answer(p.request_id, refused(PROBE_INTERRUPTED))
        queued, self._clears = self._clears, []
        for request_id, _target in queued:
            self._answer(request_id, refused(PROBE_INTERRUPTED))

    async def _mark_positions(self, now: int) -> None:
        if not self.scopes.episodes or not self.scopes.ready:
            return
        for ref, row in sorted(self._rows.items()):
            if row.state in TERMINAL_ARM_STATES or row.state == ArmState.FAILED.value:
                continue
            if not self.scopes.covers_row(row, self._row_series(row)):
                continue
            try:
                t = next_state(ArmState(row.state), Sm11Event.SCOPE_FAILED)
            except ValueError:
                continue
            if isinstance(t, Refused) or t.prior_state is None:
                continue
            new = replace(row, state=t.state.value, prior_state=t.prior_state.value,
                          state_reason=SCOPE_FAILED_REASON, updated_at_ms=now)
            try:
                async with self._positions.transaction() as tx:
                    ok = await tx.update_position(PositionCas(row=new, expected_row_version=row.row_version))
            except Exception as e:
                if self._fatal(e):
                    raise
                self.counters.position_errors += 1
                log.warning("arm failed: SM11 FAILED write for %s failed (%r)", ref, type(e))
                return
            if ok:
                self.counters.positions_failed += 1
                self._rows[ref] = replace(new, row_version=row.row_version + 1)
            else:
                self._rows_at_ms = None     # a CAS miss: re-read and try again next tick

    def _reping_due(self, ep: Episode, now: int) -> bool:
        if not ep.committed or ep.next_try_ms > now:
            return False
        if not ep.queue_ping:
            if self._owner(ep) is not None:
                return False                # folded: its collapsing episode re-pings for it
            if not ep.named:
                return True                 # its collapsing episode is gone and no sent ping named it
        elif any(not f.named for f in self._folded_of(ep)):
            return True                     # scopes folded since its ping went out: name them now
        last = ep.last_ping_at_ms if ep.last_ping_at_ms is not None else ep.record.entered_at_ms
        return now - last >= self._fail.reping_h * HOUR_MS

    async def _reping(self, now: int) -> None:
        for ep in list(self.scopes.episodes.values()):
            if not self._reping_due(ep, now):
                continue
            n = max(ep.ping_count, 1)
            folded = self._folded_of(ep)
            ping = self._render(ep.record, now, reping=n, folded=self._folded_pairs(folded))
            try:
                still_open = await self._store_for(ep).reping(ep.record.failure_id, ep.record.ping_id, ping)
            except Exception as e:
                if self._fatal(e):
                    raise
                ep.next_try_ms = now + self._cfg.retry_ms
                self.counters.reping_errors += 1
                log.warning("arm failed: re-ping of %s failed (%r)", ep.record.failure_id, type(e))
                continue
            if not still_open:
                self._reload_due = True
                continue
            ep.ping_count, ep.last_ping_at_ms = n + 1, now
            ep.named = True
            for f in folded:
                f.named = True
            self.counters.repings += 1
            self._pager.log_mirror(ping.title, ping.body)

    # ------------------------------------------------------------------------------------------------ restore
    async def _restore_positions(self, now: int) -> None:
        """A live position in SM11 FAILED that no open scope covers returns to its `prior_state` (the clear's
        `FAILED_CLEARED` edge). Only once the open episodes are known; a refused edge (no valid `prior_state`) is
        left FAILED for the operator (`arm disarm`) and logged once."""
        if not self.scopes.ready:
            return
        for ref, row in sorted(self._rows.items()):
            if row.state != ArmState.FAILED.value or self.scopes.covers_row(row, self._row_series(row)):
                continue
            try:
                prior: ArmState | None = ArmState(row.prior_state) if row.prior_state else None
            except ValueError:
                prior = None
            t = next_state(ArmState.FAILED, Sm11Event.FAILED_CLEARED, prior_state=prior)
            if isinstance(t, Refused):
                if ref not in self._restore_warned:
                    self._restore_warned.add(ref)
                    self.counters.restore_refused += 1
                    log.warning("arm failed: %s stays FAILED after its clear (%s)", ref, t.reason)
                continue
            new = replace(row, state=t.state.value, prior_state=None, state_reason=FAILED_CLEARED_REASON,
                          updated_at_ms=now)
            try:
                async with self._positions.transaction() as tx:
                    ok = await tx.update_position(PositionCas(row=new, expected_row_version=row.row_version))
            except Exception as e:
                if self._fatal(e):
                    raise
                self.counters.restore_errors += 1
                log.warning("arm failed: restore of %s failed (%r); retrying", ref, type(e))
                return
            if ok:
                self.counters.positions_restored += 1
                self._rows[ref] = replace(new, row_version=row.row_version + 1)
            else:
                self._rows_at_ms = None     # a CAS miss: re-read and try again next tick

    # ------------------------------------------------------------------------------------------------ clear
    def submit_clear(self, request_id: int, target: str) -> bool:
        """The non-blocking `arm_clear_failed` handler: queue the request and return at once."""
        self._clears.append((request_id, target))
        return True

    def probing(self) -> tuple[str, ...]:
        """The targets with a clear probe in flight (`arm failed show`)."""
        return tuple(sorted(s.target for s in self._probes))

    def _answer(self, request_id: int, answer: RequestAnswer) -> None:
        if self._answers is None:
            return
        try:
            self._answers.complete(request_id, answer)
        except Exception as e:      # the request store's fault never stops the loop
            log.warning("arm failed: answer of request %d failed (%r)", request_id, type(e))

    def _refuse(self, request_id: int, reason: str) -> None:
        self.counters.clears_refused += 1
        self._answer(request_id, refused(reason))

    def _start_clears(self, now: int) -> None:
        """Check each queued clear and start its probe as a task (never awaited here)."""
        queued, self._clears = self._clears, []
        for request_id, target in queued:
            scope = parse_target(target)
            if scope is None:
                self._refuse(request_id, BAD_TARGET)
                continue
            if not self.scopes.ready:
                self._refuse(request_id, EPISODES_UNREAD)
                continue
            if scope in self._probes:
                self._refuse(request_id, PROBE_RUNNING)
                continue
            ep = self.scopes.episodes.get(scope)
            if ep is None:
                self.counters.clears_moot += 1
                self._answer(request_id, moot(NOT_FAILED))
                continue
            if not ep.committed:
                self._refuse(request_id, NOT_RECORDED)      # the row is not in PostgreSQL yet: file it again later
                continue
            if self._probe_ports is None or self._clear is None:
                self._refuse(request_id, PROBE_NOT_WIRED)
                continue
            p = _Probe(request_id=request_id, scope=scope, failure_id=ep.record.failure_id, started_ms=now)
            self._probes[scope] = p
            self.counters.probes += 1
            p.task = asyncio.create_task(self._run_probe(p, ep, self._probe_ports))

    async def _run_probe(self, p: _Probe, ep: Episode, ports: ClearProbes) -> None:
        """The probe task: its result (a timeout or an error is a refusal) or the lost fence for the tick. Only the
        probe's own deadline is `probe_timeout`; a port's own TimeoutError is a `probe_error`."""
        deadline = asyncio.timeout(self._cfg.probe_timeout_ms / 1000)
        try:
            async with deadline:
                result = await self._probe(ep, ports)
            p.result = refused_probe(PROBE_TIMEOUT) if deadline.expired() else result
        except Exception as e:
            if self._fatal(e):
                p.error = e
                return
            if isinstance(e, TimeoutError) and deadline.expired():
                p.result = refused_probe(PROBE_TIMEOUT)
                return
            self.counters.probe_errors += 1
            log.warning("arm failed: probe of %s raised (%r)", p.scope.target, type(e))
            p.result = refused_probe(PROBE_ERROR)

    async def _probe(self, ep: Episode, ports: ClearProbes) -> ProbeResult:
        """The probe of the recorded cause. An ADAPTER scope runs the probe of each of its open SERIES episodes at the
        same time (a fresh-bar probe can wait most of a bar, so one after another they could not finish within one
        `probe_timeout_ms`); the first refusal in scope order is the answer and the probes still running are
        cancelled."""
        if ep.record.probe_kind != ADAPTER_SERIES:
            return await self._probe_kind(ep, ports)
        series = sorted((e for s, e in self.scopes.episodes.items()
                         if s.kind is ScopeKind.SERIES and adapter_of(s) == ep.scope.scope_id),
                        key=lambda e: e.scope)
        tasks = [asyncio.create_task(self._probe_kind(s_ep, ports)) for s_ep in series]
        try:
            for task in tasks:
                result = await task
                if not result.passed:
                    return result
            return PASSED
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
                task.add_done_callback(_retrieve)

    async def _probe_kind(self, ep: Episode, ports: ClearProbes) -> ProbeResult:
        kind, scope_id = ep.record.probe_kind, ep.scope.scope_id
        if kind in (SERIES_REREAD, FRESH_BAR):
            if ports.series is None:
                return refused_probe(PROBE_NOT_WIRED)
            if kind == SERIES_REREAD:
                return await ports.series.reread(scope_id)
            return await ports.series.fresh_bar(scope_id)
        if kind == INDICATOR_REBUILD:
            if ports.indicators is None:
                return refused_probe(PROBE_NOT_WIRED)
            return await ports.indicators.rebuilt_and_warm(scope_id)
        if kind == SPAN_WRITE:
            if ports.span is None:
                return refused_probe(PROBE_NOT_WIRED)
            return await ports.span.test_span_write(ep.record.position_ids)
        if kind == STRATEGY_CONFORMANCE:
            if ports.versions is None:
                return refused_probe(PROBE_NOT_WIRED)
            return await ports.versions.probe(scope_id)
        if kind == ARM_GLOBAL:
            return await self._probe_arm(ep, ports)
        return refused_probe(PROBE_NOT_WIRED)

    async def _probe_arm(self, ep: Episode, ports: ClearProbes) -> ProbeResult:
        """ARM global (R11-M09): no more strategy job threads hung than the runner may abandon (S38c review; a
        `restart_required` episode needs that view), no task circuit open, a fenced test write on the ARM pool, then
        the trial bootstrap (allowed while globally FAILED, never counted, no MD_RESET notice)."""
        if ports.circuits is None or ports.trial is None:
            return refused_probe(PROBE_NOT_WIRED)
        if ports.hung is None:
            if ep.record.reason == RESTART_REQUIRED:
                return refused_probe(PROBE_NOT_WIRED)
        elif ports.hung.restart_still_required():
            return refused_probe(THREADS_HUNG)
        if any(t in CIRCUIT_TASKS for t in ports.circuits.open_circuits()):
            return refused_probe(CIRCUIT_OPEN, "reset_task")
        try:
            still_open = await self._stores.arm.note_probe(ep.record.failure_id, self._clock.now_ms(), PROBING)
        except Exception as e:
            if self._fatal(e):
                raise
            return refused_probe(ARM_POOL)
        if not still_open:
            return refused_probe(EPISODE_NOT_OPEN)
        return await ports.trial.trial()

    async def _finish_probes(self, now: int) -> None:
        for scope, p in list(self._probes.items()):
            if p.error is not None:
                del self._probes[scope]
                raise p.error                               # a lost fence: the caller hard-exits
            if p.result is None:
                if now - p.started_ms <= self._cfg.probe_timeout_ms + self._cfg.probe_grace_ms:
                    continue
                # the backstop: the task outlived its own deadline (a port that ignores its cancellation)
                if p.task is not None and not p.task.done():
                    p.task.cancel()
                del self._probes[scope]
                self.counters.probe_errors += 1
                log.warning("arm failed: probe of %s abandoned past its timeout", scope.target)
                await self._finish(p, refused_probe(PROBE_TIMEOUT), now)
                continue
            del self._probes[scope]
            await self._finish(p, p.result, now)

    async def _note(self, ep: Episode, result: ProbeResult, now: int) -> None:
        """`last_probe_at_ms` / `last_probe_result` (best effort: the answer does not depend on it)."""
        try:
            await self._store_for(ep).note_probe(ep.record.failure_id, now, probe_note(result))
        except Exception as e:
            if self._fatal(e):
                raise
            log.warning("arm failed: probe note of %s failed (%r)", ep.record.failure_id, type(e))

    async def _finish(self, p: _Probe, result: ProbeResult, now: int) -> None:
        ep = self.scopes.episodes.get(p.scope)
        if ep is None or ep.record.failure_id != p.failure_id:
            self.counters.clears_moot += 1
            self._answer(p.request_id, moot(NOT_FAILED))   # closed meanwhile (e.g. a re-read of the open rows)
            return
        if result.passed and p.refault_at_ms is not None:
            result = refused_probe(FAULT_DURING_PROBE)      # new evidence arrived while it ran: a clear would erase it
        await self._note(ep, result, now)
        if not result.passed:
            log.warning("arm failed: clear of %s refused (%s)", p.scope.target, probe_note(result))
            self._refuse(p.request_id, refusal_reason(result))
            return
        clear = self._clear
        if clear is None:
            self._refuse(p.request_id, PROBE_NOT_WIRED)
            return
        # S38f (R14 review): the counters the clear resets are written in the clear's own transaction, so a crash or
        # a failing ARM pool after it never brings them (or the restart watermark) back
        reset = self._budget.reset_rows(ep.scope, now) if self._budget_store is not None else []
        try:
            if reset:
                done = await clear.clear(p.request_id, ep.record.failure_id, now, reset=reset)
            else:
                done = await clear.clear(p.request_id, ep.record.failure_id, now)
        except Exception as e:
            if self._fatal(e):
                raise
            self.counters.clear_errors += 1
            log.warning("arm failed: clear of %s failed (%r)", ep.record.failure_id, type(e))
            self._refuse(p.request_id, CLEAR_WRITE_FAILED)
            return
        if done is None:
            self._refuse(p.request_id, REQUEST_MISMATCH)
            self._reload_due = True                         # maybe no longer open: re-read the open rows
            return
        for r in reset:
            self._budget_saved[r.key] = r                   # committed with the clear
        self._cleared(ep, now)
        self.counters.clears_applied += 1
        self._answer(p.request_id, APPLIED)

    def _cleared(self, ep: Episode, now: int) -> None:
        """The committed clear, in memory: the scope stops being FAILED, its counters reset, the side effects run,
        one FAILED_CLEARED notice; the positions are restored by `_restore_positions` from the next row read."""
        self.scopes.episodes.pop(ep.scope, None)
        self._budget.reset(ep.scope, now)
        self._rows_at_ms = None
        log.info("arm failed: %s cleared (%s)", ep.scope.target, ep.record.failure_id)
        if self._hooks is not None:
            try:
                self._hooks.cleared(ep.scope)
            except Exception as e:
                log.warning("arm failed: clear hook of %s failed (%r)", ep.scope.target, type(e))
        if self._notices is not None:
            try:
                self._notices.failed_cleared(ep.record.failure_id, ep.record.scope_kind, ep.record.scope_id, now)
            except Exception as e:
                log.warning("arm failed: FAILED_CLEARED notice of %s failed (%r)", ep.scope.target, type(e))


if TYPE_CHECKING:
    from src.marketdata.bootstrap import MdWriterService
    from src.riskmgr.notices import ArmNotices, FailedLinesPort
    from src.riskmgr.probes import (
        IndicatorProbePort,
        IndicatorStateProbe,
        TrialBootstrapProbe,
        TrialPort,
        TrialProbePort,
    )
    from src.riskmgr.tracker import FailedScopeView

    def _ports(m: FailedManager) -> tuple[FailedScopeView, FailedLinesPort, FailedScopeView, FailedLinesPort]:
        """The scopes fill the arm-tracker's `FailedScopeView` and the notices' `FailedLinesPort` (S31 wires them)."""
        return m.scopes, m.scopes, m, m

    def _clear_ports(n: ArmNotices, svc: MdWriterService, trial: TrialBootstrapProbe,
                     ind: IndicatorStateProbe) -> tuple[FailedClearedPort, TrialPort, TrialProbePort,
                                                        IndicatorProbePort]:
        """The notices fill `FailedClearedPort`, `md-writer` the trial port, and the concrete probes their ports."""
        return n, svc, trial, ind


__all__ = [
    "BAD_TARGET", "CLEAR_COMMAND", "CLEAR_WRITE_FAILED", "EPISODES_UNREAD", "EPISODE_NOT_OPEN", "Episode",
    "FAILED_CLEARED_REASON", "FailedClearedPort", "FailedCounters", "FailedManager", "FailedScopes", "FailedSettings",
    "FailedStores", "FailedSummary", "NOT_FAILED", "NOT_RECORDED", "PROBE_INTERRUPTED", "PROBE_RUNNING", "PROBING",
    "PositionSeriesPort", "REQUEST_MISMATCH", "SCOPE_FAILED_REASON", "episode_ids", "own_ping_id", "parse_target",
    "record_of_spool", "render_ping", "row_version_scopes", "scope_covers_row", "spool_record", "versions_strategy",
]
