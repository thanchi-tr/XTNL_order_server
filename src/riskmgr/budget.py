"""The D7 error budget: counting, counting pauses and the due FAILED faults (design §B14; plan S30a).

`ErrorBudget` turns healed-or-not signals into **faults** (`Fault`: a scope, a reason code, counts and times) for
`src/riskmgr/failed.py`, which enters FAILED, records the episode and pings. Its counters reset on a clean result and
an accepted clear resets the scope's counters (`reset`, S30b). **A restart is never itself a clean result** (plan
S38f): the counters it owns (the `unresolved` streaks, the `md_down` / `pool_down` time counters and the
unclean-restart watermark) are exported as `BudgetCounterRow`s (`durable_rows`), which the FAILED manager writes to
PostgreSQL `arm_budget_counters` (fenced, only on change) and hands back after a restart, a takeover or a `redis-md`
loss (`restore`): a streak resumes where it stopped, and the time counters keep their total (the gap adds nothing).
A restore that comes late (the first read failed, so this term has already counted) **merges**: a stored streak whose
last bucket precedes this term's first adds to it, a time counter adds its stored total, and the fault is due if the
sum reaches the threshold; a clean result this term saw before the restore supersedes the stored counter. An accepted
clear's counter reset (`reset_rows`) is written in the clear's own transaction, so a crash after the clear never
brings back the counters (or the restart watermark) it reset.
Unclean restarts themselves count (`note_restarts`): `fail_restarts_n` leader terms ended `lost` / `init_failed`
within `fail_restart_window_s` while positions are armed is an ARM-global fault (`unclean_restarts`). Nothing here
does I/O, logs a value or reads PostgreSQL.

**One counting unit** (§B14, R11-M08). A series counter counts **buckets, never reads**: md-bars and the D6
cross-check count invalid buckets at their caps and call `series_fault(series, invalid_bar | source_mismatch)` once
at `K` (S17a / S17b); decode errors and failed reads are retries inside the bucket's healing and never reach here.
The one bucket counter this module owns is `fail_unresolved_n` (`note_unresolved`): a bucket adds at most 1, only
when it is still not final at its live `decision_cap`; a final bar (`note_final`) resets the streak; a bucket
resolved `no_trade` (`note_no_trade`, decided `NO_BROKER_BAR`) neither adds nor resets (S38c).

**Counting pauses** (nothing counts, nothing enters FAILED) while:
* the series' market is not OPEN (SM10, `CalendarPort`), or a declared broker maintenance window is active
  (`MaintenancePort`);
* the adapter is not STREAMING, or its REST reads fail at every priority (R11-M10); for the bar-read counter
  (`unresolved`) an unknown REST state (`None`) pauses too ("bar-read failures count only while other REST priorities
  on the same adapter succeed");
* for the two decision-cap counters (no final bar, stuck frontier) also while **engine PostgreSQL is down**
  (`src.riskmgr.ports.engine_pg_down`: the term pool's last fenced commit older than 30 s, or the RiskSweep's
  "PostgreSQL unreachable" alert), R11-M11.

A one-shot series fault (`invalid_bar`, `source_mismatch`, `refetch_failed`, `indicator_faulted`) that arrives
during a pause is **deferred**, not dropped: the market-data side signals it once and then holds the series blocked,
so dropping it would leave a blocked series with no FAILED behind it; `poll()` files it when the pause ends. Deferred
faults are kept per (series, reason), and filing a fault of the series directly drops its deferred ones (the series'
one episode is open then). The one exception is `refetch_failed` raised while the broker is unavailable (the adapter
not STREAMING or its REST reads failing everywhere): a refetch failing during a broker outage is the outage, never a
series fault ("a 30-minute broker REST outage gives no FAILED"), so it is dropped and counted (`outage_refetch`); the
market-data side retries the refetch once the outage ends and signals again only if that fails.

**Time counters** (`fail_md_down_s`, `fail_pool_down_s`) add time only between two observations at most
`max_step_ms` apart, so a PC sleep (or any gap in the task's loop) adds nothing. The stuck-frontier watch
(`src/riskmgr/frontier.py`) counts only while the ARM runs and engine PostgreSQL is up; positions on a FAILED scope or
paused are `held` by the caller and never count, so a scoped FAILED never becomes a global one; its ARM-pool cause
(`ARM_WRITE`) is filed here as `pool_down`, never as a stuck frontier.

**Scopes** (§B14 table): SERIES (`invalid_bar`, `source_mismatch`, `refetch_failed`, `unresolved`,
`indicator_faulted`), ADAPTER (`series_failed`: `fail_global_series_n` series of one adapter FAILED at the same time,
`adapter_due`), STRATEGY_VERSION (`strategy_version`, from `JobRunner.take_signals`), POSITION (`stuck_frontier` on
fewer than `fail_frontier_positions_n` positions, S38c), ARM (`stuck_frontier` at that count, `md_down`, `md_resets`,
`bootstrap_failed`, `md_oom`, `pool_down`, `circuit_reopen`, `restart_required`). The ARM-global scope is reached
only through `redis-md`, the ARM pool, the task circuits, a stuck frontier shared by several positions and too many
hung strategy threads (R11-M10, S38c).

`probe_kind(scope_kind, reason)` is the riskmgr's copy of `src.db.arm_schema.probe_kind_for` (the scoped package may
not import `src.db.arm_schema`); `tests/riskmgr/test_budget.py` holds the two equal.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final, Protocol

from src.db.arm_ports import BudgetCounterRow
from src.marketdata.ports import AdapterHealthPort, CalendarPort, calendar_open
from src.marketdata.types import INDICATOR_FAULTED, INVALID_BAR, SOURCE_MISMATCH, SeriesId
from src.riskmgr.config import FailSettings
from src.riskmgr.frontier import STUCK_FRONTIER, STUCK_GRACE_MS, FrontierView, FrontierWatch
from src.riskmgr.ports import PgHealthPort, engine_pg_down
from src.riskmgr.types import ScopeKind, indicator_scope_id

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------------------------------- reason codes
REFETCH_FAILED: Final = "refetch_failed"
UNRESOLVED: Final = "unresolved"
SERIES_FAILED: Final = "series_failed"
STRATEGY_VERSION_FAULT: Final = "strategy_version"
MD_DOWN: Final = "md_down"
MD_RESETS: Final = "md_resets"
BOOTSTRAP_FAILED: Final = "bootstrap_failed"
MD_OOM: Final = "md_oom"
POOL_DOWN: Final = "pool_down"
CIRCUIT_REOPEN: Final = "circuit_reopen"
RESTART_REQUIRED: Final = "restart_required"
"""ARM-global: more hung strategy threads than `JobRunner` may abandon (S38c); only an engine restart frees them."""
UNCLEAN_RESTARTS: Final = "unclean_restarts"
"""ARM-global (S38f): `fail_restarts_n` unclean engine restarts / takeovers within `fail_restart_window_s` while
positions are armed: a fault the engine cannot heal by restarting."""

#: Durable counter codes (`arm_budget_counters.counter`, S38f).
COUNTER_UNRESOLVED: Final = "unresolved"
COUNTER_MD_DOWN: Final = "md_down_ms"
COUNTER_POOL_DOWN: Final = "pool_down_ms"
DURABLE_STEP_MS: Final = 10_000
"""A time counter is stored in whole steps of this (S38f): written at most every 10 s while it counts."""
COUNTER_RESTARTS: Final = "unclean_restarts"
"""ARM scope: `count` the unclean restarts counted at the last check, `last_event_ms` the watermark (an accepted
clear of the ARM scope moves it to the clear time, so the restarts behind a cleared episode never count again)."""

SERIES_REASONS: Final = frozenset({INVALID_BAR, SOURCE_MISMATCH, REFETCH_FAILED, UNRESOLVED, INDICATOR_FAULTED})
ONE_SHOT_SERIES_REASONS: Final = SERIES_REASONS - {UNRESOLVED}
"""Signalled once by the market-data side (counted there) or at once by their rule; deferred while paused."""
ARM_STORE_REASONS: Final = frozenset({MD_RESETS, BOOTSTRAP_FAILED, MD_OOM})
"""`redis-md` store faults filed at once (§B14: 3 bootstrap failures, 3 resets in an hour, OOM after the last
shedding step); counted by the market-data side."""
ARM_REASONS: Final = frozenset({STUCK_FRONTIER, MD_DOWN, POOL_DOWN, CIRCUIT_REOPEN,
                                RESTART_REQUIRED, UNCLEAN_RESTARTS}) | ARM_STORE_REASONS
CIRCUIT_TASKS: Final = ("md-writer", "md-bars", "md-indicators", "arm", "arm-tracker", "arm-relay")
"""The tasks whose circuit opening again after its half-open retry is an ARM-global fault (§B14)."""

#: The clear probe of each recorded cause (§B14 Clearing, R11-M12); equal to `src.db.arm_schema.REASON_PROBE_KIND`.
PROBE_KIND_BY_REASON: Final[Mapping[str, str]] = {
    INVALID_BAR: "series_reread", SOURCE_MISMATCH: "series_reread", REFETCH_FAILED: "series_reread",
    UNRESOLVED: "fresh_bar",
    INDICATOR_FAULTED: "indicator_rebuild",
    STUCK_FRONTIER: "span_write",
    STRATEGY_VERSION_FAULT: "strategy_conformance",
}
ADAPTER_PROBE: Final = "adapter_series"
ARM_GLOBAL_PROBE: Final = "arm_global"

MAX_STEP_MS: Final = 30_000
"""A time counter adds the gap between two observations only up to this; a longer gap (a PC sleep) adds nothing."""
LIVE_CAP_SLACK_MS: Final = 60_000
"""An unresolved bucket counts only when its cap passed at most this long ago: older caps (the engine was down or
the PC slept) go through replay and never count (§B14 Healing first)."""

# pause codes (metrics and `pause_reason`, never stored)
MARKET_CLOSED: Final = "market_closed"
MAINTENANCE: Final = "maintenance"
BROKER_UNAVAILABLE: Final = "broker_unavailable"
BROKER_UNKNOWN: Final = "broker_unknown"
ENGINE_PG_DOWN: Final = "engine_pg_down"


def probe_kind(scope_kind: ScopeKind, reason: str) -> str:
    """The `probe_kind` an episode is filed with (the riskmgr copy of `probe_kind_for`): ADAPTER always probes its
    series; else the cause probe of `reason`; else (ARM) the trial bootstrap. ValueError for a SERIES or
    STRATEGY_VERSION reason with no cause probe."""
    if scope_kind is ScopeKind.ADAPTER:
        return ADAPTER_PROBE
    if scope_kind is ScopeKind.POSITION and reason != STUCK_FRONTIER:
        raise ValueError(f"a POSITION episode has reason stuck_frontier, not {reason!r}")
    probe = PROBE_KIND_BY_REASON.get(reason)
    if probe is not None:
        return probe
    if scope_kind is ScopeKind.ARM:
        return ARM_GLOBAL_PROBE
    raise ValueError(f"no clear probe for a {scope_kind.value} episode with reason {reason!r}")


# ---------------------------------------------------------------------------------------------------- types
@dataclass(frozen=True, order=True)
class ScopeRef:
    """One FAILED scope: `ARM` has the scope id `arm`; SERIES a series' canonical id; ADAPTER the adapter id;
    STRATEGY_VERSION `strategy:<KIND>:<VERSION>` or `guard:<KIND>:<VERSION>`; POSITION `adapter_id:position_id`."""

    kind: ScopeKind
    scope_id: str

    def __post_init__(self) -> None:
        if not self.scope_id or any(c.isspace() for c in self.scope_id):
            raise ValueError("a scope id is a non-empty id")

    @property
    def target(self) -> str:
        """The `arm_clear_failed` request target (`SCOPE_KIND:scope_id`)."""
        return f"{self.kind.value}:{self.scope_id}"


ARM_SCOPE: Final = ScopeRef(ScopeKind.ARM, "arm")


def series_scope(series: SeriesId | str) -> ScopeRef:
    return ScopeRef(ScopeKind.SERIES, series if isinstance(series, str) else series.canonical)


def position_scope(position_ref: str) -> ScopeRef:
    """The POSITION scope of one position (`adapter_id:position_id`, S38c)."""
    return ScopeRef(ScopeKind.POSITION, position_ref)


def adapter_of(scope: ScopeRef) -> str | None:
    """The adapter a SERIES or ADAPTER scope belongs to; None for the others."""
    if scope.kind is ScopeKind.ADAPTER:
        return scope.scope_id
    if scope.kind in (ScopeKind.SERIES, ScopeKind.POSITION):
        return scope.scope_id.split(":", 1)[0]
    return None


@dataclass(frozen=True, kw_only=True)
class Fault:
    """A due FAILED fault: ids, a reason code, counts and bucket times only (never a value)."""

    scope: ScopeRef
    reason: str
    first_fault_at_ms: int
    counters: tuple[tuple[str, int], ...] = ()
    position_ids: tuple[str, ...] = ()
    """Affected positions the signal itself names (`adapter_id:position_id`); the FAILED logic adds the positions
    the scope covers."""
    edge_code: str | None = None

    def __post_init__(self) -> None:
        if self.scope.kind is ScopeKind.SERIES and self.reason not in SERIES_REASONS:
            raise ValueError(f"{self.reason!r} is not a SERIES reason")
        if self.scope.kind is ScopeKind.ARM and self.reason not in ARM_REASONS:
            raise ValueError(f"{self.reason!r} is not an ARM reason")
        if self.scope.kind is ScopeKind.ADAPTER and self.reason != SERIES_FAILED:
            raise ValueError("an ADAPTER fault has reason series_failed")
        if self.scope.kind is ScopeKind.STRATEGY_VERSION and self.reason != STRATEGY_VERSION_FAULT:
            raise ValueError("a STRATEGY_VERSION fault has reason strategy_version")
        if self.scope.kind is ScopeKind.POSITION and self.reason != STUCK_FRONTIER:
            raise ValueError("a POSITION fault has reason stuck_frontier")

    @property
    def probe_kind(self) -> str:
        return probe_kind(self.scope.kind, self.reason)


class MaintenancePort(Protocol):
    def in_maintenance(self, adapter_id: str, now_ms: int) -> bool:
        """A declared broker maintenance window is active for the adapter."""
        ...


@dataclass
class BudgetCounters:
    """Metrics (codes and counts only)."""

    faults: int = 0
    deferred: int = 0
    paused_buckets: int = 0
    stale_caps: int = 0
    unknown_circuits: int = 0
    sleeps: int = 0
    outage_refetch: int = 0
    no_trade_buckets: int = 0


@dataclass
class _Streak:
    count: int = 0
    last_bucket_ms: int | None = None
    first_at_ms: int = 0
    filed: bool = False
    first_bucket_ms: int | None = None
    """The first bucket this term counted (None for a restored streak): a late restore merges a stored streak that
    ended before it."""


@dataclass
class _Timer:
    """A time counter: counting milliseconds, the last observation and whether its fault was filed."""

    total_ms: int = 0
    last_ms: int | None = None
    first_at_ms: int | None = None
    filed: bool = False


@dataclass(frozen=True, kw_only=True)
class _Deferred:
    series: SeriesId
    reason: str
    at_ms: int
    counters: tuple[tuple[str, int], ...] = ()
    scope_id: str | None = None
    """The SERIES scope id when it is not the series' own (an indicator's guard-input scope, round 13)."""


# ---------------------------------------------------------------------------------------------------- the budget
class ErrorBudget:
    """The §B14 counters and pauses. Signals arrive synchronously (the market-data fault sinks, the job runner's
    signals, the tasks' circuits, the loop's observations); each due fault goes to the listener at once (the FAILED
    logic enters it in memory first) or, with none, is queued for `take()`."""

    def __init__(self, settings: FailSettings, *, calendar: CalendarPort, health: AdapterHealthPort,
                 pg: PgHealthPort, maintenance: MaintenancePort | None = None,
                 frontier_grace_ms: int = STUCK_GRACE_MS, max_step_ms: int = MAX_STEP_MS, live_cap_slack_ms: int = LIVE_CAP_SLACK_MS) -> None:
        if max_step_ms <= 0 or live_cap_slack_ms <= 0:
            raise ValueError("the step and slack bounds are positive")
        self.settings = settings
        self._calendar = calendar
        self._health = health
        self._pg = pg
        self._maintenance = maintenance
        self._frontier_grace_ms = frontier_grace_ms
        self._frontier = FrontierWatch(grace_ms=frontier_grace_ms)
        self._max_step_ms = max_step_ms
        self._live_cap_slack_ms = live_cap_slack_ms
        self._listener: Callable[[Fault], None] | None = None
        self._queue: list[Fault] = []
        self._streaks: dict[str, _Streak] = {}
        self._deferred: dict[tuple[str, str], _Deferred] = {}
        self._md = _Timer()
        self._pool = _Timer()
        self._restart_floor_ms: int | None = None
        self._restarts_counted = 0
        self._restarts_filed = False
        # S38f: what this term saw before `restore` ran (a late restore must not bring back a counter it reset)
        self._restored = False
        self._finals_early: dict[str, int] = {}
        self._cleared_early: set[str] = set()
        self._md_clean_early = False
        self._pool_clean_early = False
        self.counters = BudgetCounters()

    def listen(self, listener: Callable[[Fault], None]) -> None:
        """Deliver due faults to `listener` (the FAILED logic's in-memory entry) instead of queueing them; the queue
        is handed over first."""
        self._listener = listener
        queued, self._queue = self._queue, []
        for f in queued:
            listener(f)

    def take(self) -> list[Fault]:
        """The queued due faults (only while no listener is set)."""
        out, self._queue = self._queue, []
        return out

    def _emit(self, fault: Fault) -> None:
        self.counters.faults += 1
        if self._listener is None:
            self._queue.append(fault)
            return
        self._listener(fault)

    # ------------------------------------------------------------------------------------------------ pauses
    def series_pause(self, series: SeriesId, now_ms: int, *, bar_reads: bool = False,
                     decision_cap: bool = False) -> str | None:
        """The counting pause holding `series` now, or None. `bar_reads`: an unknown REST state pauses too;
        `decision_cap`: engine PostgreSQL down pauses too."""
        adapter = series.adapter_id
        if not calendar_open(self._calendar, adapter, series.symbol):
            return MARKET_CLOSED
        if self._maintenance is not None and self._maintenance.in_maintenance(adapter, now_ms):
            return MAINTENANCE
        if not self._health.is_streaming(adapter):
            return BROKER_UNAVAILABLE
        failing = self._health.rest_failing_everywhere(adapter)
        if failing:
            return BROKER_UNAVAILABLE
        if failing is None and bar_reads:
            return BROKER_UNKNOWN
        if decision_cap and engine_pg_down(self._pg, now_ms):
            return ENGINE_PG_DOWN
        return None

    def pg_down(self, now_ms: int) -> bool:
        return engine_pg_down(self._pg, now_ms)

    # ------------------------------------------------------------------------------------------------ SERIES
    def series_fault(self, series: SeriesId, reason: str, now_ms: int,
                     counters: Sequence[tuple[str, int]] = ()) -> None:
        """A one-shot series fault (`invalid_bar` / `source_mismatch` at K, `refetch_failed`, `indicator_faulted`):
        filed at once, or deferred (per series and reason, the first signal kept) while a counting pause holds the
        series; a `refetch_failed` raised during a broker outage is dropped."""
        if reason not in ONE_SHOT_SERIES_REASONS:
            raise ValueError(f"{reason!r} is not a one-shot series reason")
        item = _Deferred(series=series, reason=reason, at_ms=now_ms, counters=tuple(counters))
        pause = self.series_pause(series, now_ms)
        if reason == REFETCH_FAILED and pause == BROKER_UNAVAILABLE:
            self.counters.outage_refetch += 1
            return
        if pause is not None:
            self.counters.deferred += 1
            self._deferred.setdefault((series.canonical, reason), item)
            return
        self._drop_deferred(series.canonical)    # the series' episode opens now: a deferred one would be a duplicate
        self._file_series(item)

    def indicator_fault(self, indicator_id: str, series: SeriesId, reason: str, now_ms: int) -> None:
        """An indicator reached FAULTED (round 13, plan S38d): its own **guard-input scope**
        (`indicator_scope_id`: SERIES kind, never the input series' id), filed at once or deferred while the input
        series' counting pause holds. Only positions whose pinned specs still read the indicator are covered
        (FirstBarGuard's ATR until bar 0 is decided or spanned); the input series stays readable for every trigger."""
        if reason != INDICATOR_FAULTED:
            raise ValueError(f"{reason!r} is not an indicator fault reason")
        scope_id = indicator_scope_id(series.adapter_id, series.symbol, indicator_id)
        item = _Deferred(series=series, reason=reason, at_ms=now_ms, scope_id=scope_id)
        if self.series_pause(series, now_ms) is not None:
            self.counters.deferred += 1
            self._deferred.setdefault((scope_id, reason), item)
            return
        self._drop_deferred(scope_id)
        self._file_series(item)

    def _drop_deferred(self, canonical: str) -> None:
        for key in [k for k in self._deferred if k[0] == canonical]:
            del self._deferred[key]

    def _file_series(self, item: _Deferred) -> None:
        self._emit(Fault(scope=series_scope(item.scope_id or item.series), reason=item.reason,
                         first_fault_at_ms=item.at_ms, counters=item.counters))

    def note_unresolved(self, series: SeriesId, bucket_ms: int, cap_ms: int, now_ms: int) -> None:
        """A bucket of an OPEN bar still has no final broker bar at its live `decision_cap`: adds at most 1 per
        bucket; at `fail_unresolved_n` consecutive buckets the series fails with reason `unresolved`."""
        if now_ms - cap_ms > self._live_cap_slack_ms or now_ms < cap_ms:
            self.counters.stale_caps += 1        # replay territory: missed while down or asleep
            return
        if self.series_pause(series, now_ms, bar_reads=True, decision_cap=True) is not None:
            self.counters.paused_buckets += 1    # not an OPEN-and-counting bar: neither counts nor breaks the run
            return
        st = self._streaks.setdefault(series.canonical, _Streak())
        if st.last_bucket_ms is not None and bucket_ms <= st.last_bucket_ms:
            return                               # one bucket adds at most 1
        if st.count == 0:
            st.first_at_ms = now_ms
            st.first_bucket_ms = bucket_ms
        st.count += 1
        st.last_bucket_ms = bucket_ms
        if st.count >= self.settings.unresolved_n and not st.filed:
            st.filed = True
            self._emit(Fault(scope=series_scope(series), reason=UNRESOLVED, first_fault_at_ms=st.first_at_ms,
                             counters=(("last_bucket_ms", bucket_ms), ("unresolved_buckets", st.count))))

    def note_final(self, series: SeriesId, bucket_ms: int) -> None:
        """A final broker bar for a bucket: the clean result that resets the unresolved streak."""
        st = self._streaks.get(series.canonical)
        if not self._restored:
            self._finals_early[series.canonical] = max(bucket_ms, self._finals_early.get(series.canonical, bucket_ms))
        if st is not None and (st.last_bucket_ms is None or bucket_ms >= st.last_bucket_ms):
            del self._streaks[series.canonical]

    def note_no_trade(self, series: SeriesId, bucket_ms: int) -> None:
        """An OPEN bucket `md-bars` resolved `no_trade` (decided `NO_BROKER_BAR`, S38c): neither adds to the
        unresolved streak nor resets it (the same rule as the market-data error unit), so an unscheduled holiday
        never fails the series and a market that is simply shut is never a fault."""
        self.counters.no_trade_buckets += 1
        del series, bucket_ms

    def unresolved_streak(self, series: SeriesId) -> int:
        st = self._streaks.get(series.canonical)
        return 0 if st is None else st.count

    # ------------------------------------------------------------------------------------------------ STRATEGY
    def version_fault(self, scope_id: str, faults: int, position_ids: Sequence[str], now_ms: int, *,
                      edge_code: str | None = None) -> None:
        """A STRATEGY_VERSION fault signal (`JobRunner.take_signals`): 3 consecutive job faults of that version, at
        once. `edge_code` is the Phase 1 per-edge label on this scope (§B11)."""
        if not scope_id.startswith(("strategy:", "guard:")):
            raise ValueError("a version scope is strategy:<KIND>:<VERSION> or guard:<KIND>:<VERSION>")
        self._emit(Fault(scope=ScopeRef(ScopeKind.STRATEGY_VERSION, scope_id), reason=STRATEGY_VERSION_FAULT,
                         first_fault_at_ms=now_ms, counters=(("job_faults", faults),),
                         position_ids=tuple(sorted(set(position_ids))), edge_code=edge_code))

    # ------------------------------------------------------------------------------------------------ ARM
    def restart_required(self, hung_threads: int, now_ms: int) -> None:
        """`JobRunner.take_restart_required()`: more hung strategy threads than the runner may abandon (S38c): the
        ARM fails globally (`restart_required`); the threads cannot be killed, so the clear needs an engine restart
        first: the ARM-global probe refuses `threads_hung` while more threads are hung than the bound
        (`JobRunner.restart_still_required`, S38c review) and passes after the restart."""
        self._emit(Fault(scope=ARM_SCOPE, reason=RESTART_REQUIRED, first_fault_at_ms=now_ms,
                         counters=(("hung_threads", hung_threads),)))

    def store_fault(self, reason: str, now_ms: int) -> None:
        """A `redis-md` store fault (`md_resets`, `bootstrap_failed`, `md_oom`): ARM global at once."""
        if reason not in ARM_STORE_REASONS:
            raise ValueError(f"{reason!r} is not a redis-md store fault")
        self._emit(Fault(scope=ARM_SCOPE, reason=reason, first_fault_at_ms=now_ms))

    def on_circuit_reopen(self, task: str, now_ms: int) -> None:
        """A task circuit opened again after its half-open retry: ARM global at once for the ARM's own tasks."""
        if task not in CIRCUIT_TASKS:
            self.counters.unknown_circuits += 1
            return
        self._emit(Fault(scope=ARM_SCOPE, reason=CIRCUIT_REOPEN, first_fault_at_ms=now_ms,
                         counters=((f"circuit_{task.replace('-', '_')}", 1),)))

    def _step(self, timer: _Timer, now_ms: int) -> int:
        if timer.last_ms is None:
            return 0
        gap = now_ms - timer.last_ms
        if gap > self._max_step_ms:
            self.counters.sleeps += 1
            return 0
        return max(gap, 0)

    def observe_md(self, *, usable: bool, market_open: bool, now_ms: int) -> None:
        """`redis-md` reachable, accepting writes and with its store loaded (`usable`), and whether an armed
        position's market is OPEN. Unusable time counts only while a market is OPEN; past `fail_md_down_s` the ARM
        fails globally (`md_down`). A usable observation is the clean result."""
        t = self._md
        if usable:
            self._md = _Timer()
            self._md_clean_early = self._md_clean_early or not self._restored
            return
        if not market_open:
            t.last_ms = None                     # paused: keeps the total, counts nothing until it is OPEN again
            return
        t.total_ms += self._step(t, now_ms)
        t.last_ms = now_ms
        if t.first_at_ms is None:
            t.first_at_ms = now_ms
        if t.total_ms >= self.settings.md_down_s * 1000 and not t.filed:
            t.filed = True
            self._emit(Fault(scope=ARM_SCOPE, reason=MD_DOWN, first_fault_at_ms=t.first_at_ms,
                             counters=(("down_s", t.total_ms // 1000),)))

    def observe_pool(self, *, arm_ok: bool, now_ms: int) -> None:
        """The ARM pool can be built and its transactions succeed (`arm_ok`). Failing time counts only while the
        engine's main pool works (engine PostgreSQL not down); past `fail_pool_down_s` the ARM fails globally
        (`pool_down`), which also files a stuck frontier whose cause is the pool (R11-M11)."""
        t = self._pool
        if arm_ok:
            self._pool = _Timer()
            self._pool_clean_early = self._pool_clean_early or not self._restored
            return
        if engine_pg_down(self._pg, now_ms):
            t.last_ms = None
            return
        t.total_ms += self._step(t, now_ms)
        t.last_ms = now_ms
        if t.first_at_ms is None:
            t.first_at_ms = now_ms
        if t.total_ms >= self.settings.pool_down_s * 1000 and not t.filed:
            t.filed = True
            self._emit(Fault(scope=ARM_SCOPE, reason=POOL_DOWN, first_fault_at_ms=t.first_at_ms,
                             counters=(("down_s", t.total_ms // 1000),)))

    @property
    def pool_failing(self) -> bool:
        """The ARM pool is currently observed failing (the stuck frontier's `ARM_WRITE` cause)."""
        return self._pool.first_at_ms is not None

    def observe_frontier(self, views: Sequence[FrontierView], now_ms: int, *, running: bool,
                         failed_stuck: Sequence[str] = ()) -> None:
        """One stuck-frontier pass. The caller marks `held` every position on a FAILED scope or paused (they never
        count). Counting only while the ARM runs and engine PostgreSQL is up; an `ARM_WRITE` cause is never filed
        here (the pool row files it). `failed_stuck`: the positions whose POSITION `stuck_frontier` scope is still
        FAILED (S38c review): they count toward `frontier_positions_n`, so positions that get stuck one after another
        (on different bars) still reach the ARM-global scope once N are stuck."""
        counting = running and not engine_pg_down(self._pg, now_ms)
        fault = self._frontier.observe(views, now_ms, counting=counting, pool_failing=self.pool_failing)
        if fault is None:
            return
        earlier = tuple(sorted(set(failed_stuck) - set(fault.stuck or fault.position_ids)))
        if fault.stuck_count + len(earlier) >= self.settings.frontier_positions_n:   # a shared cause: ARM-global
            stuck = tuple(sorted({*(fault.stuck or fault.position_ids), *earlier}))
            self._emit(Fault(scope=ARM_SCOPE, reason=STUCK_FRONTIER, first_fault_at_ms=now_ms,
                             counters=(("oldest_bar_ms", fault.oldest_bar_ms), ("stuck_positions", len(stuck))),
                             position_ids=stuck))
            return
        for ref in fault.position_ids:                                 # S38c: only that position's scope
            self._emit(Fault(scope=position_scope(ref), reason=STUCK_FRONTIER, first_fault_at_ms=now_ms,
                             counters=(("oldest_bar_ms", fault.oldest_bar_ms), ("stuck_positions", 1)),
                             position_ids=(ref,)))

    # ------------------------------------------------------------------------------------------------ loop
    def poll(self, now_ms: int) -> None:
        """File the deferred one-shot series faults whose pause has ended (one per series: the oldest signal; the
        series' other deferred reasons would be duplicates of its open episode). A deferred `refetch_failed` whose
        series is now held by a broker outage is dropped."""
        for key, item in sorted(self._deferred.items(), key=lambda kv: (kv[1].at_ms, kv[0])):
            if key not in self._deferred:
                continue
            pause = self.series_pause(item.series, now_ms)
            if item.reason == REFETCH_FAILED and pause == BROKER_UNAVAILABLE:
                del self._deferred[key]
                self.counters.outage_refetch += 1
            elif pause is None:
                self._drop_deferred(key[0])
                self._file_series(item)

    def deferred(self) -> tuple[str, ...]:
        """The series with a deferred fault (canonical ids, sorted, once each)."""
        return tuple(sorted({k[0] for k in self._deferred}))

    # ------------------------------------------------------------------------------------------------ restarts (S38f)
    def note_restarts(self, unclean_ends_ms: Sequence[int], now_ms: int, *, armed: bool) -> None:
        """The end times of the leader terms that ended `lost` / `init_failed` (an engine crash, kill or fence, or a
        takeover after one; a clean stop or a planned switchover never counts), read once per term at the FAILED
        manager's start. Those inside `fail_restart_window_s` and after the watermark count; at `fail_restarts_n`
        while positions are `armed` the ARM fails globally (`unclean_restarts`) with the immediate ping."""
        window_ms = self.settings.restart_window_s * 1000
        floor = self._restart_floor_ms
        recent = sorted(t for t in unclean_ends_ms
                        if now_ms - window_ms <= t <= now_ms and (floor is None or t > floor))
        self._restarts_counted = len(recent)
        if not armed or len(recent) < self.settings.restarts_n or self._restarts_filed:
            return
        self._restarts_filed = True
        self._emit(Fault(scope=ARM_SCOPE, reason=UNCLEAN_RESTARTS, first_fault_at_ms=recent[0],
                         counters=(("unclean_restarts", len(recent)), ("window_s", self.settings.restart_window_s))))

    # ------------------------------------------------------------------------------------------------ durable (S38f)
    def durable_rows(self) -> dict[tuple[str, str, str], BudgetCounterRow]:
        """The counters that must survive a restart, keyed (scope kind, scope id, counter); a counter at rest is absent
        (the FAILED manager writes a vanished one with count 0)."""
        out: dict[tuple[str, str, str], BudgetCounterRow] = {}

        def put(row: BudgetCounterRow) -> None:
            out[row.key] = row
        for series, st in self._streaks.items():
            if st.count > 0:
                put(BudgetCounterRow(scope_kind=ScopeKind.SERIES.value, scope_id=series, counter=COUNTER_UNRESOLVED,
                                     count=st.count, streak_start_ms=st.first_at_ms, last_event_ms=st.last_bucket_ms))
        for code, t in ((COUNTER_MD_DOWN, self._md), (COUNTER_POOL_DOWN, self._pool)):
            if t.first_at_ms is not None:
                # whole DURABLE_STEP_MS only, so a counting timer is written at most every 10 s (restored at most
                # that much short); no last observation (a restored timer never adds the time the engine was down)
                put(BudgetCounterRow(scope_kind=ScopeKind.ARM.value, scope_id=ARM_SCOPE.scope_id, counter=code,
                                     count=t.total_ms - t.total_ms % DURABLE_STEP_MS, streak_start_ms=t.first_at_ms))
        if self._restart_floor_ms is not None or self._restarts_counted:
            put(BudgetCounterRow(scope_kind=ScopeKind.ARM.value, scope_id=ARM_SCOPE.scope_id,
                                 counter=COUNTER_RESTARTS, count=self._restarts_counted,
                                 last_event_ms=self._restart_floor_ms))
        return out

    def restore(self, rows: Sequence[BudgetCounterRow], open_scopes: Collection[ScopeRef] = ()) -> int:
        """Adopt the durable counters read at start (once per term; normally before the first signal of the term).
        `open_scopes`: the scopes with an open (or spooled) episode; a restored counter at or past its threshold counts
        as filed only when its scope is one of them, so a counter whose episode is gone files again on its next count.

        A late restore (the first read failed, so this term has already counted) merges (R14 review): a stored streak
        whose last bucket precedes this term's first bucket adds to this term's count (an overlap keeps the larger),
        a stored time total adds to this term's, and the fault is filed now if the sum reaches the threshold. A clean
        result this term saw first (a final bar, a usable observation, an accepted clear) supersedes the stored
        counter. Returns how many were adopted."""
        adopted = 0
        arm_open = ARM_SCOPE in open_scopes
        for row in rows:
            if row.count <= 0 and row.counter != COUNTER_RESTARTS:
                continue
            if row.scope_kind == ScopeKind.SERIES.value and row.counter == COUNTER_UNRESOLVED:
                if self._restore_streak(row, series_scope(row.scope_id) in open_scopes):
                    adopted += 1
            elif row.scope_kind == ScopeKind.ARM.value and row.counter in (COUNTER_MD_DOWN, COUNTER_POOL_DOWN):
                if self._restore_timer(row, arm_open):
                    adopted += 1
            elif row.scope_kind == ScopeKind.ARM.value and row.counter == COUNTER_RESTARTS:
                if row.last_event_ms is not None and (self._restart_floor_ms is None
                                                      or row.last_event_ms > self._restart_floor_ms):
                    self._restart_floor_ms = row.last_event_ms
                    adopted += 1
        self._restored = True
        self._finals_early.clear()
        self._cleared_early.clear()
        self._md_clean_early = self._pool_clean_early = False
        return adopted

    def _restore_streak(self, row: BudgetCounterRow, scope_open: bool) -> bool:
        series = row.scope_id
        final = self._finals_early.get(series)
        if series in self._cleared_early or (final is not None and (row.last_event_ms is None
                                                                    or final >= row.last_event_ms)):
            return False                         # a clean result this term came after the stored streak
        n = self.settings.unresolved_n
        st = self._streaks.get(series)
        if st is None:
            self._streaks[series] = _Streak(count=row.count, last_bucket_ms=row.last_event_ms,
                                            first_at_ms=row.streak_start_ms or 0, filed=scope_open and row.count >= n)
            return True
        if row.last_event_ms is not None and st.first_bucket_ms is not None and row.last_event_ms < st.first_bucket_ms:
            st.count += row.count                # the stored buckets, then this term's: one streak
        else:
            st.count = max(st.count, row.count)  # an overlap: the same buckets seen twice count once
        if row.last_event_ms is not None and (st.last_bucket_ms is None or row.last_event_ms > st.last_bucket_ms):
            st.last_bucket_ms = row.last_event_ms
        if row.streak_start_ms is not None:
            st.first_at_ms = min(st.first_at_ms, row.streak_start_ms) if st.first_at_ms else row.streak_start_ms
        st.first_bucket_ms = None
        st.filed = st.filed or (scope_open and st.count >= n)
        if st.count >= n and not st.filed:
            st.filed = True
            self._emit(Fault(scope=series_scope(series), reason=UNRESOLVED, first_fault_at_ms=st.first_at_ms,
                             counters=(("last_bucket_ms", st.last_bucket_ms or 0), ("unresolved_buckets", st.count))))
        return True

    def _restore_timer(self, row: BudgetCounterRow, arm_open: bool) -> bool:
        md = row.counter == COUNTER_MD_DOWN
        if self._md_clean_early if md else self._pool_clean_early:
            return False                         # a usable observation this term: the stored outage is over
        limit_ms = (self.settings.md_down_s if md else self.settings.pool_down_s) * 1000
        current = self._md if md else self._pool
        if current.first_at_ms is None:
            # last_ms stays None: the time between the last observation and now (the engine was down) adds nothing
            restored = _Timer(total_ms=row.count, last_ms=None, first_at_ms=row.streak_start_ms,
                              filed=arm_open and row.count >= limit_ms)
            if md:
                self._md = restored
            else:
                self._pool = restored
            return True
        current.total_ms += row.count            # this term's counted time follows the stored total
        if row.streak_start_ms is not None:
            current.first_at_ms = min(current.first_at_ms, row.streak_start_ms)
        current.filed = current.filed or (arm_open and current.total_ms >= limit_ms)
        if current.total_ms >= limit_ms and not current.filed:
            current.filed = True
            self._emit(Fault(scope=ARM_SCOPE, reason=MD_DOWN if md else POOL_DOWN,
                             first_fault_at_ms=current.first_at_ms, counters=(("down_s", current.total_ms // 1000),)))
        return True

    def reset_rows(self, scope: ScopeRef, now_ms: int) -> list[BudgetCounterRow]:
        """The durable counters an accepted clear of `scope` resets (S38f, R14 review), written in the clear's own
        transaction: a SERIES scope's `unresolved` streak; for the ARM scope both time counters and the restart
        watermark moved to the clear time. Equal to the rows the FAILED manager would write after `reset`."""
        if scope.kind is ScopeKind.SERIES:
            return [BudgetCounterRow(scope_kind=scope.kind.value, scope_id=scope.scope_id, counter=COUNTER_UNRESOLVED,
                                     count=0)]
        if scope.kind is ScopeKind.ARM:
            kind, sid = ScopeKind.ARM.value, ARM_SCOPE.scope_id
            return [BudgetCounterRow(scope_kind=kind, scope_id=sid, counter=COUNTER_MD_DOWN, count=0),
                    BudgetCounterRow(scope_kind=kind, scope_id=sid, counter=COUNTER_POOL_DOWN, count=0),
                    BudgetCounterRow(scope_kind=kind, scope_id=sid, counter=COUNTER_RESTARTS, count=0,
                                     last_event_ms=now_ms)]
        return []

    def reset(self, scope: ScopeRef, now_ms: int | None = None) -> None:
        """An accepted clear resets the scope's own counters (S30b, §B14 Clearing); for the ARM scope every ARM
        counter. An ADAPTER scope's only counter is the derived count of its open series, so its clear touches no
        per-series state: a deferred one-shot fault or an unresolved streak of another series of the adapter is kept
        (that series stays blocked until its fault is filed, so dropping it would leave it blocked with no episode)."""
        if scope.kind is ScopeKind.SERIES:
            self._streaks.pop(scope.scope_id, None)
            self._drop_deferred(scope.scope_id)
            if not self._restored:
                self._cleared_early.add(scope.scope_id)
        elif scope.kind is ScopeKind.ARM:
            self._md = _Timer()
            self._pool = _Timer()
            if not self._restored:
                self._md_clean_early = self._pool_clean_early = True
            self._frontier = FrontierWatch(grace_ms=self._frontier_grace_ms)
            if now_ms is not None:          # S38f: the restarts behind the cleared episode never count again
                self._restart_floor_ms = now_ms
            self._restarts_counted = 0
            self._restarts_filed = False

    def snapshot(self) -> tuple[tuple[str, int], ...]:
        """Codes and counts for `arm_status` / `md show`."""
        return (("deferred_faults", len(self._deferred)), ("faults", self.counters.faults),
                ("md_down_s", self._md.total_ms // 1000), ("pool_down_s", self._pool.total_ms // 1000),
                ("unresolved_series", sum(1 for s in self._streaks.values() if s.count)))


@dataclass(frozen=True, kw_only=True)
class AdapterDue:
    adapter_id: str
    series: tuple[str, ...]


def adapter_due(open_series: Mapping[str, Sequence[str]], open_adapters: frozenset[str] | set[str],
                n: int) -> list[AdapterDue]:
    """The ADAPTER faults due (§B14, `fail_global_series_n`): `n` or more series of one adapter FAILED at the same
    time, the adapter not FAILED already. `open_series` maps an adapter to its open SERIES scope ids."""
    return [AdapterDue(adapter_id=a, series=tuple(sorted(s))) for a, s in sorted(open_series.items())
            if len(s) >= n and a not in open_adapters]


__all__ = [
    "ADAPTER_PROBE", "ARM_GLOBAL_PROBE", "ARM_REASONS", "ARM_SCOPE", "ARM_STORE_REASONS", "AdapterDue",
    "BOOTSTRAP_FAILED", "BROKER_UNAVAILABLE", "BROKER_UNKNOWN", "BudgetCounters", "CIRCUIT_REOPEN", "CIRCUIT_TASKS",
    "ENGINE_PG_DOWN", "ErrorBudget", "Fault", "LIVE_CAP_SLACK_MS", "MAINTENANCE", "MARKET_CLOSED", "MAX_STEP_MS",
    "MD_DOWN", "MD_OOM", "MD_RESETS", "MaintenancePort", "ONE_SHOT_SERIES_REASONS", "POOL_DOWN",
    "PROBE_KIND_BY_REASON", "REFETCH_FAILED", "RESTART_REQUIRED", "UNCLEAN_RESTARTS", "COUNTER_MD_DOWN",
    "COUNTER_POOL_DOWN", "COUNTER_RESTARTS", "COUNTER_UNRESOLVED", "SERIES_FAILED", "SERIES_REASONS", "STRATEGY_VERSION_FAULT",
    "ScopeRef", "UNRESOLVED", "adapter_due", "adapter_of", "position_scope", "probe_kind", "series_scope",
]
