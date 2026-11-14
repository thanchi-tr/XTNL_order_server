"""EngineSession: everything one leader term runs (IMPLEMENTATION_PLAN 3.6; operations.md §3, §4).

Built per term by the NodeController's session factory (`SessionContext` + the new epoch). Building it
does no I/O; the work happens in LEADER_INIT (`init_deps`) and in supervised tasks:

    LEADER_INIT step     what the session does
    1  epoch bump        ports.fencer (bump_epoch + leader_init_fence; SAFE_HOLD mirrored to etcd)
    2  HLC re-seed       ports.seq -> the orchestrator's HybridLogicalClock
    3  FLUSHDB           the engine's Redis database only, never FLUSHALL (boot_id gone: the gate stays
                         shut until SM8); then the active `rulesets` row is installed (4.5)
    4  sod catch-up      one full RiskSweep pass writes any missing sod_marks row
    5  SM10              ScheduledCalendarInit over market_sessions
    6  SM4 || SM8        tasks: ingestion (critical), 4 consumers, DLQ spooler; SM8 run_once (boot_id
                         last), then the SM8 signal server; risksweep, feed observer, housekeeping,
                         calendar, readiness, ruleset poll, account live sampler, data lifecycle
    7  DLQ barrier       spill drain at start-up; then spill drain, DLQ recovery, integrity worker,
                         outbox relay, dead-man sender, SAFE_HOLD mirror

Rule configuration (IMPLEMENTATION_PLAN 4.5): `ActiveRuleset` holds the installed ruleset; the RiskSweep and
the price path (per-symbol clean-spread thresholds) read it, and a reload installs at a sweep boundary
(`RiskSweep.install_ruleset`; the re-page cadence follows the installed `escalate_after_ms`).
Optional housekeeping ports (`PgLeaderPorts.rulesets / live_samples / lifecycle / operator_requests /
auth_rearms`) are used when present.

Operator loop (IMPLEMENTATION_PLAN 4.9): every `operator_poll_s` the session consumes this epoch's
`operator_requests` (`reset_task` closes a task circuit in the TaskSupervisor), re-arms an AUTH_FAILED
adapter whose failed generation an operator set to REARMED, and asks the secrets source to re-read the
secret (`SessionContext.on_auth_failed`) once per newly failed generation. SM10 execution evidence: the
orchestrator's execution hook feeds `MarketCalendar.on_execution`.

Runtime wiring (IMPLEMENTATION_PLAN 5.6):
* the orchestrator pages (`page`), mutes conn-key / stale-price pages by the calendar, learns each adapter's
  transfer source (`SessionContext.transfer_sources`, else the adapter's HAS_TRANSFER_SOURCE) and swaps in
  an adapter built from rotated credentials (`SessionContext.refresh_adapter`);
* SM10: a scheduled entry into OPEN asks the orchestrator for the REST-only RESYNCING at open + 30-60 s
  (`market_open_resync`); the broker schedules are fetched and versioned by the `schedule-refresh` task
  (src/runtime/schedule_refresh.py) and reload the calendar; the sod mark uses each symbol's last scheduled
  close or break before the roll (`SnapshotInputs.scheduled_closes`);
* PRE_OPEN readiness checks each adapter's token validity (> 24 h);
* operator requests: `reset_task`, `release_account` (RiskSweep.release_account) and `refresh_cash_flows`
  (the orchestrator's rate-limited cash-flow fetch);
* notifications follow the secrets source (`RotatingChannels`, re-read on an auth error) and the installed
  ruleset's re-page cadence;
* telemetry: `metrics_samples()` (src/runtime/telemetry.py) and JSON log fields (`epoch` per term,
  client_order_id / position_id / seq_id / account_id around each stream item and each consumer commit).

Runtime wiring (IMPLEMENTATION_PLAN 6.5):
* REST priority classes on the adapters' shared bucket (operations.md §15): on-demand cash-flow fetches at
  CASH_FLOWS, the DLQ worker's check-before-retry reads at DLQ, the 60 s flat-account poll and the schedule refresh
  at POLL; everything else (snapshots, resyncs including the market-open REST-only RESYNCING, SM3 / SM8 re-reads)
  is untagged, i.e. RECONCILE;
* metrics: `xtnl_rest_bucket_tokens` / `xtnl_rest_requests_total` from `adapter.rest_budget.stats()` and
  `xtnl_token_age_seconds` from `adapter.token_issued_at_ms`;
* the orchestrator's clean-mid watch uses the same clean-spread thresholds as the committer;
* SM10: every configured adapter is checked for calendar sessions; a maintenance window sends one info notice;
  declared windows come from `SessionContext.maintenance` (src/runtime/maintenance.py), read once before SM10
  is computed at LEADER_INIT step 5 (`init_calendar`) and then every `maintenance_poll_s`, each read bounded
  by `maintenance_load_timeout_s`;
* inhibition (operations.md §9): the RiskSweep's `AlertInhibitor` holds back stale / FENCE alert rows during
  maintenance and for `epoch_settle_ms` after this term's LEADER_INIT; the orchestrator's conn-key, stale-price
  and clean-mid pages are muted for the same settle.

Runtime wiring (IMPLEMENTATION_PLAN 7.4):
* dead-man: the bearer token follows the secrets source (`SessionContext.deadman_token`, re-read per POST; a 401 /
  403 re-reads the secret); a copy of each heartbeat goes to the Melbourne dashboard (`heartbeat_copy`), which shows
  Redis live values only when their boot_id epoch is the latest heartbeat's; `oldest_risksweep_pass_age_ms` and
  `xtnl_risksweep_oldest_pass_age_seconds` cover every account the sweep lists, an account that never passed counting
  from when it was first listed (`risksweep_pass_ms`);
* metrics: `xtnl_frames_lost_total{stream}` from `orchestrator.frames_lost` (leader only);
* 7.1: `SnapshotInputs.pnl_source` = `orchestrator.pnl_source`; every direct `fetch_cash_flows` (SM3 reconcile, DLQ
  check-before-retry) is reported with `note_cash_flows`; the term's LocalStateReader has `account_states`
  (`PgLocalStateWithAccounts`) so a row without broker_seq inherits the stored one after a restart;
* 7.2: only subscribed symbols (`_wanted_symbols`) decide an adapter's OPEN; LEADER_INIT step 5 seeds the pre-close
  reference quotes from `last_clean_marks` (`seed_calendar_quotes`); the sweep marks under `calendar.mark_session`.

DRAINING budget (operations.md §4): close + DLQ flush + spill + end_reason + cluster flush + close-out
(`SessionTimings.drain_budget_s`) plus the supervisor stop and the lease revoke must fit inside the
keeper's 10 s DRAINING deadline; `check_drain_budget` is enforced by the wiring.

Every long-running loop is a supervised `TaskSpec` with a liveness stamp (operations.md §3): stamped
by the loop itself, or by a monitor that reads the component's `liveness_ms`. Only the ingestion
task is leadership-critical (its loss hard-exits). Waiting on PostgreSQL, on SM8, or while SM3 halted
ingestion counts as alive (`exempt_wait` / `set_exempt`).

Phase 1 is read-only: nothing here sends, modifies or cancels an order. `gate` is the ActionGate any
Phase 2 sender must call.
"""
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Coroutine, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Literal, Protocol

import httpx
import redis.asyncio as aioredis

from src.adapters.base import BrokerAdapter
from src.adapters.ratelimit import RestBudget, RestPriority, rest_priority
from src.cache import keys as K
from src.cache.feed_stream import FEED_GROUP, FeedEntry, RedisFeedStream
from src.cache.halt_mirror import RedisHaltMirror
from src.cache.live_reader import RunIdReader, info_run_id
from src.cache.scripts import NoBootQueue, RedisState, RedisStreamHealth, read_boot_id, trim_feed
from src.calendar.machine import OPEN_STATES, SessionState, SessionTransition
from src.calendar.market import MarketCalendar, ScheduledCalendarInit
from src.calendar.readiness import ReadinessDispatcher
from src.calendar.schedule import CalendarSchedule, MaintenanceWindow, ScheduleLoader, SymbolKey
from src.calendar.sessions import Phase, boundaries, phase_at
from src.db.account_sampler import AccountLiveSampler, LiveSampleStore
from src.db.epoch import Epoch
from src.db.gate_reader import GateInputs
from src.db.lifecycle import LifecycleRunner, PartitionMaintenance
from src.db.operator_requests import RequestPayloadError, parse_cash_flow_refresh, parse_release
from src.db.repository import TieredStateCommitter, TieredStreamHealth
from src.db.snapshot_reader import SnapshotInputs
from src.adapters.base import StreamEvent
from src.dto import AccountDTO, AccountLive, CashFlowDTO, OrderDTO, PositionDTO, PriceDTO
from src.integrity.bootstrap import RedisBootstrap
from src.integrity.bridge import OrchestratorDlqBridge
from src.integrity.dlq_recovery import DlqRecoveryWorker
from src.integrity.incident import IncidentMachine
from src.integrity.ports import IntegrityAlert, IntegrityState, PatroniControl
from src.integrity.redis_ops import BootIdentity
from src.integrity.spill import SpillDrainService
from src.integrity.worker import IntegrityWorker
from src.notifications.factory import build_channels
from src.notifications.mail import SmtpFactory, default_smtp_factory
from src.notifications.relay import RELAY_MAX_STAMP_AGE_S, Escalation, OutboxRelay
from src.notifications.rotation import RotatingChannels
from src.notifications.settings import DeadmanSettings, NotificationSettings
from src.oversee.config import OverseeConfig
from src.oversee.feed import FeedObserver
from src.oversee.gate import ActionGate
from src.oversee.halt import HaltStore, HaltSwitch, HaltTxn
from src.oversee.inhibit import EPOCH_SETTLE_MS, AlertInhibitor
from src.oversee.notify import NotificationDispatcher, OutboxRow, Severity
from src.oversee.ruleset import ActiveRuleset, RulesetReloader, RulesetSource
from src.oversee.snapshot import AccountSnapshot, BreachAck, SnapshotReader
from src.oversee.sweep import AccountWrite, HlcSweepIds, PassReport, RiskSweep, SweepWriter
from src.observability.logs import clear_epoch_if, entity_fields, log_context, set_process_fields
from src.observability.metrics import CounterVec, Sample, gauge
from src.observability.redact import redact_urls
from src.oversee.types import BreachState, HaltLevel, Page
from src.ports import CommitOutcome, DlqRow, DlqSink, StateCommitter
from src.pubsub.bus import EventBus
from src.pubsub.connection import ConnState
from src.pubsub.consumers import (
    AccountImageProjector,
    OrderStateConsumer,
    PositionStateConsumer,
    PriceSubscriber,
    TopicConsumer,
)
from src.pubsub.hlc import HybridLogicalClock
from src.pubsub.orchestrator import AdapterRefresher, IngestionOrchestrator
from src.pubsub.spooler import DlqSpooler
from src.runtime.catchup import CatchupCoordinator, CatchupMetrics, CatchupSettings, CatchupStore
from src.runtime.cluster import ClusterControl
from src.runtime.deadman import DeadmanSender, DeadmanTransport, HttpsDeadmanTransport
from src.runtime.exits import ExitFn, hard_exit
from src.runtime.ingestion_control import ConsumerSwitches, OrchestratorIngestionControl, OrchestratorReconciler
from src.runtime.leader_init import LeaderInitDeps, SodOutcome, is_fence_loss
from src.runtime.maintenance import MaintenanceSource
from src.runtime.ports import AuthRearmReader, LeaderPorts, MarketSessionStore, OperatorRequestQueue, PortsFactory
from src.runtime.probes import BrokerTlsMonitor
from src.runtime.roles import BrokerLink, EndReason
from src.runtime.schedule_refresh import SCHEDULE_REFRESH_MS, SCHEDULE_RETRY_MS, ScheduleRefresher
from src.runtime.supervisor import TaskContext, TaskSpec, TaskSupervisor
from src.oversee.dayroll import latest_roll_ms, trading_day_for_roll
from src.pubsub.tradingday import last_roll_ms

log = logging.getLogger(__name__)

# Task names (operations.md §3 supervision); RISKSWEEP_TASK matches src/runtime/node.py.
INGESTION_TASK: Final = "ingestion"
RISKSWEEP_TASK: Final = "risksweep"
DEFAULT_MAX_CLEAN_SPREAD_PIPS: Final = Decimal("5")
OPERATOR_TASK: Final = "operator"
# The dead-man pass-age key while the RiskSweep has not listed the accounts once in this term (7.4).
UNLISTED_ACCOUNTS: Final = "<accounts not listed yet>"
_INCIDENT_SWITCH_STATES: Final = frozenset({IntegrityState.HALTED, IntegrityState.SWITCHING_OVER})
# DRAINING time spent outside EngineSession.drain before the lease is released: the supervisor stopping its
# (cancelled) tasks. The lease revoke itself is bounded by LeaseTimings.request_timeout_s.
DRAIN_SUPERVISOR_STOP_S: Final = 0.5
# Fixed time EngineSession.drain adds beyond its configured step timeouts: the cluster flush's wait_for gets
# drain_cluster_s + 0.1 s, and the final spill after the DLQ flush gets at least 0.1 s.
DRAIN_CLUSTER_GRACE_S: Final = 0.1
DRAIN_FINAL_SPILL_MIN_S: Final = 0.1
DRAIN_STEP_SLACK_S: Final = DRAIN_CLUSTER_GRACE_S + DRAIN_FINAL_SPILL_MIN_S


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def default_clean_spread(adapter_id: str, symbol: str) -> Decimal | None:
    """The spec default `marks.max_clean_spread_pips` (the session reads the installed ruleset instead)."""
    return DEFAULT_MAX_CLEAN_SPREAD_PIPS


def default_credential_generation(adapter: BrokerAdapter) -> str:
    """adapter_auth_state generation: the cTrader token_generation, an adapter-declared generation, or
    'default' (Secrets Manager version ids are not wired in Phase 1)."""
    tg = getattr(adapter, "token_generation", None)
    if isinstance(tg, int) and not isinstance(tg, bool):
        return f"token:{tg}"
    cg = getattr(adapter, "credential_generation", None)
    return str(cg) if cg else "default"


def default_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=10.0)


class LeaseView(Protocol):
    def lease_valid(self, min_remaining_s: float = 0.0) -> bool: ...

    def holds_lease(self) -> bool: ...


class IncidentPatroni(Protocol):
    def with_incident_guard(self, incident_path: Callable[[], bool]) -> PatroniControl: ...


class _NoPatroni:
    """No Patroni control configured: every incident step fails, so SM3 reaches MANUAL_INTERVENTION."""

    async def primary_member(self) -> str:
        raise RuntimeError("Patroni control is not configured")

    async def standby_passes_check(self) -> bool:
        return False

    async def switchover(self) -> str:
        raise RuntimeError("Patroni control is not configured")

    async def reinit(self, member: str) -> None:
        raise RuntimeError("Patroni control is not configured")


@dataclass(frozen=True)
class SessionTimings:
    sm8_deadline_s: float = 100.0          # inside the 120 s LEADER_INIT deadline
    sm8_price_wait_ms: int = 10_000
    settle_ms: int = 15_000 + 2_000 + 5_000  # TTL + TCP_USER_TIMEOUT + broker ack timeout (§5)
    # DRAINING steps (their sum, drain_budget_s, must leave room for the supervisor stop and the lease
    # revoke inside the keeper's 10 s DRAINING deadline: check_drain_budget)
    drain_close_s: float = 1.5
    drain_flush_s: float = 2.0             # operations.md §4: PostgreSQL gets 2 s, then the spill file
    drain_spill_s: float = 1.5             # after drain_flush_s: write what is left to the local spill file
    drain_record_s: float = 0.75
    drain_cluster_s: float = 0.25          # pending fenced cluster_state writes
    drain_aclose_s: float = 0.5            # HTTP client + term pool close
    consumer_poll_s: float = 1.0
    spooler_interval_s: float = 0.5
    housekeeping_s: float = 10.0
    mirror_interval_s: float = 5.0
    monitor_poll_s: float = 1.0
    reconcile_commit_s: float = 30.0
    ruleset_poll_s: float = 5.0
    live_sample_interval_ms: int = 60_000
    live_sample_flush_s: float = 5.0
    lifecycle_every_ms: int = 6 * 3_600_000
    lifecycle_poll_s: float = 30.0
    operator_poll_s: float = 2.0
    schedule_poll_s: float = 30.0
    schedule_refresh_ms: int = SCHEDULE_REFRESH_MS
    schedule_retry_ms: int = SCHEDULE_RETRY_MS
    # operations.md §9 Inhibition: an epoch change holds back the dependent (stale / FENCE) alerts for RTO + 30 s:
    # the RiskSweep's AlertInhibitor and the orchestrator's conn-key / stale-price / clean-mid pages (6.5).
    epoch_settle_ms: int = EPOCH_SETTLE_MS
    maintenance_poll_s: float = 30.0       # declared maintenance windows re-read (SessionContext.maintenance)
    maintenance_load_timeout_s: float = 5.0   # one read of the source (a hung read pages, never stalls the task)

    @property
    def drain_budget_s(self) -> float:
        """Worst-case time EngineSession.drain() takes (every step is bounded by its own timeout), including
        the fixed slack the steps add (DRAIN_STEP_SLACK_S: the cluster flush's +0.1 s and the final spill's
        0.1 s minimum)."""
        return (self.drain_close_s + self.drain_flush_s + self.drain_spill_s + self.drain_record_s
                + self.drain_cluster_s + self.drain_aclose_s + DRAIN_STEP_SLACK_S)


def check_drain_budget(timings: SessionTimings, draining_deadline_s: float, lease_release_s: float) -> str | None:
    """None when DRAINING fits: the session's steps, the supervisor stop and the lease revoke must sum
    below the keeper's DRAINING deadline (it hard-exits `abdicate` otherwise). Else the problem."""
    total = timings.drain_budget_s + DRAIN_SUPERVISOR_STOP_S + lease_release_s
    if total < draining_deadline_s:
        return None
    return (f"DRAINING steps take up to {timings.drain_budget_s:.1f} s + supervisor stop "
            f"{DRAIN_SUPERVISOR_STOP_S:.1f} s + lease revoke {lease_release_s:.1f} s = {total:.1f} s, not below the "
            f"{draining_deadline_s:.1f} s DRAINING deadline")


@dataclass(frozen=True)
class SessionContext:
    """Process-wide dependencies shared by every term's session."""

    node_id: str
    spill_path: Path
    redis: aioredis.Redis
    ports: PortsFactory
    adapters: Callable[[], Sequence[BrokerAdapter]]
    lease: LeaseView
    notifications: NotificationSettings = field(default_factory=NotificationSettings)
    deadman: DeadmanSettings = field(default_factory=DeadmanSettings)
    oversee: OverseeConfig = field(default_factory=OverseeConfig)
    run_id_reader: RunIdReader = info_run_id
    http_client: Callable[[], httpx.AsyncClient] = default_http_client
    smtp_factory: SmtpFactory = default_smtp_factory
    patroni: IncidentPatroni | None = None
    cluster: ClusterControl | None = None
    tls: BrokerTlsMonitor | None = None
    clean_spread: Callable[[str, str], Decimal | None] | None = None   # None: the installed ruleset's
    credential_generation: Callable[[BrokerAdapter], str] = default_credential_generation
    chrony_synced: Callable[[], bool] = lambda: False
    exit_fn: ExitFn = hard_exit
    clock_ms: Callable[[], int] = _wall_ms
    timings: SessionTimings = field(default_factory=SessionTimings)
    page_sink: Callable[[str, str, str], None] | None = None
    on_auth_failed: Callable[[str], None] | None = None   # e.g. SecretsSource.invalidate (re-read the secret)
    # --- 5.6 ---------------------------------------------------------------------------------------------
    # Credential rotation within a term (orchestrator `refresh_adapter`): synchronous, no I/O, None = unchanged.
    refresh_adapter: AdapterRefresher | None = None
    # adapter_id -> has a deposit / withdrawal source (else the adapter's HAS_TRANSFER_SOURCE; else False).
    transfer_sources: Mapping[str, bool] | None = None
    # Notification channel settings re-read from the cached secret (no I/O); None = the fixed `notifications`.
    notification_settings: Callable[[], NotificationSettings] | None = None
    invalidate_secrets: Callable[[], None] | None = None   # an alert-channel auth error re-reads the secret
    # A lost fence seen by the RiskSweep (engine-independent backstop of GuardedEngineDb); None = exit_fn.
    on_fence_lost: Callable[[BaseException], None] | None = None
    # --- 6.5 ---------------------------------------------------------------------------------------------
    # Declared broker maintenance windows (SM10 BROKER_MAINTENANCE overlay), re-read every maintenance_poll_s;
    # None = no declared windows (only a schedule version's own windows apply).
    maintenance: MaintenanceSource | None = None
    # --- 7.4 ---------------------------------------------------------------------------------------------
    # The dead-man bearer token re-read from the cached secret for every heartbeat (no I/O; rotation without
    # restart, operations.md §13); None = the fixed `deadman.token`. A 401 / 403 calls `invalidate_secrets`.
    deadman_token: Callable[[], str | None] | None = None
    # The heartbeat copy for the Melbourne dashboard (operations.md §9 Dashboard: Redis live values only when
    # their boot_id epoch is the latest leader heartbeat's; src/dashboard/heartbeat.py). url None = no copy.
    heartbeat_copy: DeadmanSettings = field(default_factory=DeadmanSettings)
    heartbeat_copy_token: Callable[[], str | None] | None = None
    # --- downtime catch-up (migration 0012; src/runtime/catchup.py) ------------------------------------------
    # Window settings (XTNL_CATCHUP_OVERLAP_S / XTNL_CATCHUP_FIRST_RUN_DAYS); the process-wide metrics (cumulative
    # across terms); this process's engine_runs.run_id (src/runtime/uptime.py), recorded on each catchup_runs row.
    catchup: CatchupSettings = field(default_factory=CatchupSettings)
    catchup_metrics: CatchupMetrics = field(default_factory=CatchupMetrics)
    engine_run_id: Callable[[], str | None] = lambda: None


class _TrackingCommitter:
    """StateCommitter proxy recording the wall time of the last committed broker event (dead-man)."""

    def __init__(self, inner: StateCommitter, clock_ms: Callable[[], int],
                 on_live: Callable[[AccountLive], object] | None = None) -> None:
        self._inner = inner
        self._clock = clock_ms
        self._on_live = on_live
        self.last_commit_ms: int | None = None

    async def commit_entity(self, dto: OrderDTO | PositionDTO | AccountDTO) -> CommitOutcome:
        out = await self._inner.commit_entity(dto)
        if out in (CommitOutcome.APPLIED, CommitOutcome.NO_BOOT, CommitOutcome.NOOP_ADVANCED):
            self.last_commit_ms = self._clock()
        return out

    async def commit_cash_flow(self, cf: CashFlowDTO) -> bool:
        new = await self._inner.commit_cash_flow(cf)
        if new:
            self.last_commit_ms = self._clock()
        return new

    async def write_live(self, item: AccountLive | PriceDTO) -> None:
        await self._inner.write_live(item)
        if self._on_live is not None and isinstance(item, AccountLive):
            try:
                self._on_live(item)             # live account sampler: in-memory, no I/O
            except Exception:
                log.exception("live account sampler offer failed")


class _EngineRedisFlush:
    """LEADER_INIT step 3: FLUSHDB of the engine's Redis database (the one REDIS_URL selects), never
    FLUSHALL: another database on the same server is not the engine's to wipe. redis:boot_id must be gone
    afterwards, so the gate stays shut until SM8 (IMPLEMENTATION_PLAN 4.8)."""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def flushdb(self) -> None:
        await self._redis.flushdb()
        if await read_boot_id(self._redis) is not None:
            raise RuntimeError("redis:boot_id survived FLUSHDB")


class _StampingReader:
    """SnapshotReader proxy: every read stamps the RiskSweep task's liveness (a full pass lists the
    accounts every interval_ms) and remembers the account list (dead-man pass ages): the last list, whether
    any list succeeded this term, and when each listed account was first listed (its pass age while it has
    never passed, IMPLEMENTATION_PLAN 7.4; pruned to the current list, so bounded)."""

    def __init__(self, inner: SnapshotReader, clock_ms: Callable[[], int] = _wall_ms) -> None:
        self._inner = inner
        self._clock = clock_ms
        self.ctx: TaskContext | None = None
        self.accounts: tuple[str, ...] = ()
        self.listed = False
        self.first_listed_ms: dict[str, int] = {}

    def _stamp(self) -> None:
        if self.ctx is not None:
            self.ctx.stamp()

    async def list_accounts(self) -> Sequence[str]:
        self._stamp()
        with _pg_wait():
            out = await self._inner.list_accounts()
        now = self._clock()
        self.accounts = tuple(out)
        self.first_listed_ms = {a: self.first_listed_ms.get(a, now) for a in self.accounts}
        self.listed = True
        self._stamp()
        return out

    async def read_account(self, account_id: str, now_ms: int) -> AccountSnapshot:
        self._stamp()
        with _pg_wait():
            snap = await self._inner.read_account(account_id, now_ms)
        self._stamp()
        return snap


# The supervised task whose PostgreSQL awaits count as alive (operations.md §3: "a task counts as alive
# while it awaits a PostgreSQL call"; a sync-standby loss stalls COMMIT for up to ~20 s). Set by the
# RiskSweep body in its own asyncio task, so a store shared with another task (HaltSwitch.status from
# housekeeping) never exempts the RiskSweep by accident.
_PG_TASK: contextvars.ContextVar[TaskContext | None] = contextvars.ContextVar("xtnl_pg_task", default=None)


def _pg_wait() -> contextlib.AbstractContextManager[None]:
    ctx = _PG_TASK.get()
    return ctx.exempt_wait("postgres") if ctx is not None else contextlib.nullcontext()


class _ExemptSweepWriter:
    """SweepWriter proxy: the sweep's fenced write transactions count as alive while they wait."""

    def __init__(self, inner: SweepWriter) -> None:
        self._inner = inner

    async def write_account(self, batch: AccountWrite) -> None:
        with _pg_wait():
            await self._inner.write_account(batch)

    async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
        with _pg_wait():
            await self._inner.set_quarantine(account_id, at_ms, reason)


class _ExemptHaltStore:
    """HaltStore proxy: a halt transaction (status / trip / system_clear, COMMIT included) counts as alive."""

    def __init__(self, inner: HaltStore) -> None:
        self._inner = inner

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[HaltTxn]:
        with _pg_wait():
            async with self._inner.transaction() as tx:
                yield tx


class _ExemptDispatcher(NotificationDispatcher):
    """The sweep's re-page queueing (dedupe read + outbox insert) counts as alive while it waits."""

    async def queue_repages(self, states: Iterable[BreachState], acks: Sequence[BreachAck], now_ms: int) -> int:
        with _pg_wait():
            return await super().queue_repages(states, acks, now_ms)


class _DrainableDlqSink:
    """DlqSink proxy for the term's DlqSpooler (operations.md §4 DRAINING).

    `abandon()` turns the sink into a spill-only sink: an insert still waiting on PostgreSQL, and every
    later one, raises ConnectionError, so DlqSpooler.flush() itself spills the batch it popped plus the
    rest of the ring to the local file. Nothing is cancelled underneath the spooler (a cancelled flush
    drops the batch it had already popped)."""

    def __init__(self, inner: DlqSink) -> None:
        self._inner = inner
        self._abandoned = asyncio.Event()

    @property
    def abandoned(self) -> bool:
        return self._abandoned.is_set()

    def abandon(self) -> None:
        self._abandoned.set()

    async def insert_batch(self, rows: Sequence[DlqRow]) -> None:
        if self._abandoned.is_set():
            raise ConnectionError("DRAINING: DLQ sink abandoned; spilling locally")
        insert = asyncio.ensure_future(self._inner.insert_batch(rows))
        gate = asyncio.ensure_future(self._abandoned.wait())
        try:
            await asyncio.wait({insert, gate}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            gate.cancel()
            if not insert.done():
                insert.cancel()
                insert.add_done_callback(_retrieve)   # finishes in the background; never awaited here
        if insert.done() and not insert.cancelled():
            insert.result()                  # delivered, or the sink's own error (ConnectionError spills)
            return
        # Unknown outcome: the rows are spilled as well; the spill drain's insert is idempotent. The
        # cancelled insert is not awaited: a driver that is slow to unwind must not hold the spill up, and
        # waiting on it here could swallow a cancellation of this task (stop_all would then never finish).
        raise ConnectionError("DRAINING: PostgreSQL did not take the DLQ batch in time; spilling locally")


def _retrieve(task: asyncio.Future[None]) -> None:
    """Done callback of an abandoned insert: consume its outcome (no 'exception never retrieved')."""
    if not task.cancelled():
        task.exception()


def _retrieve_result(job: asyncio.Future[int]) -> None:
    """Done callback of a DRAINING flush job: consume its outcome (no 'exception never retrieved')."""
    if not job.cancelled():
        job.exception()


class _PacedFeedStream:
    """FeedStream proxy: an empty XREADGROUP that returns before its BLOCK time (a proxy or server that
    ignores BLOCK) waits out the rest, so the feed observer never spins the event loop."""

    def __init__(self, inner: RedisFeedStream, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._inner = inner
        self._mono = monotonic

    async def boot_id(self) -> str | None:
        return await self._inner.boot_id()

    async def read_group(self, count: int, block_ms: int) -> list[FeedEntry]:
        started = self._mono()
        out = await self._inner.read_group(count, block_ms)
        if not out:
            left = block_ms / 1000 - (self._mono() - started)
            if left > 0:
                await asyncio.sleep(left)
        return out

    async def autoclaim(self, min_idle_ms: int, count: int) -> list[FeedEntry]:
        return await self._inner.autoclaim(min_idle_ms, count)

    async def ack(self, entry_ids: Sequence[str]) -> None:
        await self._inner.ack(entry_ids)


class _RecordingMirror:
    """HaltMirror proxy remembering the last mirrored level (dead-man `halt_level`)."""

    def __init__(self, inner: RedisHaltMirror) -> None:
        self._inner = inner
        self.level: HaltLevel | None = None

    async def cas_set(self, level: HaltLevel, event_id: int) -> bool:
        ok = await self._inner.cas_set(level, event_id)
        self.level = level
        return ok

    async def read(self) -> tuple[HaltLevel, int] | None:
        return await self._inner.read()


# ======================================================================================== 5.6 helpers
_DAY_MS: Final = 86_400_000
_CLOSE_HORIZON_MS: Final = 15 * _DAY_MS


def _ingest_stream(item: object) -> str:
    """The `stream` label of xtnl_ingest_events_total (prices, orders, positions, account)."""
    if isinstance(item, PriceDTO):
        return "prices"
    if isinstance(item, OrderDTO):
        return "orders"
    if isinstance(item, PositionDTO):
        return "positions"
    return "account"


class _ObservedOrchestrator(IngestionOrchestrator):
    """The orchestrator plus telemetry on its parser path (still synchronous, no I/O): broker events read per
    (adapter, stream), and the JSON log fields (client_order_id / position_id / account_id) of the item
    being handled on every line logged while it is."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.ingested = CounterVec("xtnl_ingest_events_total")

    def handle_stream_item(self, adapter_id: str, stream: Any, item: Any) -> None:
        if isinstance(item, StreamEvent):
            super().handle_stream_item(adapter_id, stream, item)
            return
        self.ingested.inc(adapter=adapter_id, stream=_ingest_stream(item))
        with log_context(**entity_fields(item)):
            super().handle_stream_item(adapter_id, stream, item)

    # --- REST priority classes (operations.md §15; src/adapters/ratelimit.py) ------------------------------
    # Every adapter REST call draws from the adapter's one bucket; untagged calls (snapshots, resyncs including
    # the market-open REST-only RESYNCING, SM3 / SM8 re-reads, connect) run as RECONCILE. These two callers are
    # demoted to their class here, around the adapter call, so every caller of the method is covered.
    async def request_cash_flows(self, account_id: str) -> bool:
        """On-demand cash-flow fetch (Oversee, operator, PRE_OPEN readiness): RestPriority.CASH_FLOWS."""
        with rest_priority(RestPriority.CASH_FLOWS):
            return await super().request_cash_flows(account_id)

    async def poll_account(self, adapter_id: str) -> bool:
        """The 60 s flat-account REST poll: RestPriority.POLL (the lowest class)."""
        with rest_priority(RestPriority.POLL):
            return await super().poll_account(adapter_id)


class _DlqPriorityBridge(OrchestratorDlqBridge):
    """The DLQ worker's broker reads (check-before-retry, tier-persistence.md §5) at RestPriority.DLQ. Its direct
    `fetch_cash_flows` result is reported to the orchestrator (`note_cash_flows`, IMPLEMENTATION_PLAN 7.1), so a
    partial ledger read here turns `pnl_source` off like any other fetch."""

    def __init__(self, orchestrator: IngestionOrchestrator, adapter_ids: Callable[[], Collection[str]]) -> None:
        super().__init__(orchestrator, adapter_ids)
        self._orchestrator = orchestrator

    async def reread_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                            entity_id: str) -> OrderDTO | PositionDTO | AccountDTO | None:
        with rest_priority(RestPriority.DLQ):
            return await super().reread_entity(adapter_id, event_type, entity_id)

    async def read_entity(self, adapter_id: str, event_type: Literal["ORDER", "POSITION", "ACCOUNT"],
                          entity_id: str) -> OrderDTO | PositionDTO | AccountDTO | None:
        with rest_priority(RestPriority.DLQ):
            return await super().read_entity(adapter_id, event_type, entity_id)

    async def reread_cash_flow(self, adapter_id: str, transaction_id: str, since_ms: int) -> CashFlowDTO | None:
        """OrchestratorDlqBridge.reread_cash_flow plus the completeness report (the orchestrator stays the sole
        publisher: a found row goes through `resubmit_cash_flow`)."""
        orch = self._orchestrator
        with rest_priority(RestPriority.DLQ):
            flows = await orch.runtime(adapter_id).adapter.fetch_cash_flows(since_ms)
        orch.note_cash_flows(adapter_id, flows, since_ms)
        found = next((cf for cf in flows if cf.transaction_id == transaction_id), None)
        if found is not None:
            orch.resubmit_cash_flow(found)
        return found


class _TimedRiskSweep(RiskSweep):
    """RiskSweep recording the duration of its last complete pass over all accounts (metrics)."""

    last_full_pass_s: float | None = None

    async def run_pass(self, accounts: Sequence[str] | None = None) -> PassReport:
        started = time.monotonic()
        report = await super().run_pass(accounts)
        if accounts is None and not report.dependency_down:
            self.last_full_pass_s = time.monotonic() - started
        return report


class _RecordingScheduleLoader:
    """ScheduleLoader proxy remembering the calendar version the calendar was last initialised from."""

    def __init__(self, inner: ScheduleLoader) -> None:
        self._inner = inner
        self.last: CalendarSchedule | None = None

    async def load(self) -> CalendarSchedule:
        schedule = await self._inner.load()
        self.last = schedule
        return schedule


def scheduled_closes_before(schedule: CalendarSchedule | None, adapter_id: str,
                            roll_ms: int) -> dict[SymbolKey, int]:
    """tier-persistence.md §4 sod_marks: for each of the adapter's symbols that was not OPEN just before the
    roll, its last scheduled close or break before the roll (the sod mark freshness is measured from there,
    so a scheduled closure never marks the day partial_session). A symbol open at the roll is left out (the
    roll itself is its reference)."""
    out: dict[SymbolKey, int] = {}
    if schedule is None:
        return out
    for key, sym in schedule.symbols.items():
        if key[0] != adapter_id:
            continue
        try:
            if phase_at(sym, roll_ms - 1) is Phase.OPEN:
                continue
            for b in reversed(boundaries(sym, roll_ms - _CLOSE_HORIZON_MS, roll_ms)):
                if b <= roll_ms and phase_at(sym, b) is not Phase.OPEN and phase_at(sym, b - 1) is Phase.OPEN:
                    out[key] = b
                    break
        except Exception as exc:              # one bad schedule never breaks the sweep's snapshot read
            log.warning("%s:%s: last close before the roll not computable: %r", key[0], key[1], exc)
    return out


def token_expires_at_ms(adapter: BrokerAdapter) -> int | None:
    """The adapter's credential expiry: a public `token_expires_at_ms`, else the cTrader token pair's
    `expires_at_ms` (PepperstoneAdapter's TokenManager); None when it does not expire or is unknown."""
    v = getattr(adapter, "token_expires_at_ms", None)
    if v is None:
        pair = getattr(getattr(adapter, "_tokens", None), "pair", None)
        v = getattr(pair, "expires_at_ms", None)
    if isinstance(v, int) and not isinstance(v, bool):
        return v
    return None


def token_issued_at_ms(adapter: BrokerAdapter) -> int | None:
    """When the adapter's credential in use was issued (StoneX: last LogOn; cTrader: the token pair's save);
    None when unknown (xtnl_token_age_seconds is then absent)."""
    v = getattr(adapter, "token_issued_at_ms", None)
    if isinstance(v, int) and not isinstance(v, bool):
        return v
    return None


def rest_budget_of(adapter: BrokerAdapter) -> RestBudget | None:
    """The adapter's shared REST bucket (operations.md §15), or None for an unmetered adapter."""
    budget = getattr(adapter, "rest_budget", None)
    return budget if isinstance(budget, RestBudget) else None


def adapter_metric_samples(adapter_id: str, adapter: BrokerAdapter, now_ms: int) -> list[Sample]:
    """Per-adapter series of deploy/observability/metrics.md: credential expiry and age, REST bucket tokens and
    requests by priority. Each is absent when the adapter does not expose it."""
    out: list[Sample] = []
    exp = token_expires_at_ms(adapter)
    if exp is not None:
        out += gauge("xtnl_token_expires_in_seconds", (exp - now_ms) / 1000.0, adapter=adapter_id)
    issued = token_issued_at_ms(adapter)
    if issued is not None:
        out += gauge("xtnl_token_age_seconds", max(0, now_ms - issued) / 1000.0, adapter=adapter_id)
    budget = rest_budget_of(adapter)
    if budget is not None:
        st = budget.stats()
        out += gauge("xtnl_rest_bucket_tokens", st.tokens, adapter=adapter_id)
        for label in sorted(st.requests):
            out.append(Sample("xtnl_rest_requests_total", float(st.requests[label]),
                              {"adapter": adapter_id, "priority": label}))
    return out


_PageSeverity = Literal["info", "high", "critical"]


def _iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _severity(s: str) -> _PageSeverity:
    if s in ("info", "critical"):
        return s  # type: ignore[return-value]
    return "high"


class EngineSession:
    """The LeaderSession (src/runtime/node.py) of one term."""

    def __init__(self, ctx: SessionContext, epoch: Epoch) -> None:
        self.ctx = ctx
        self.epoch = epoch
        self.node_id = ctx.node_id
        self._clock = ctx.clock_ms
        self._t = ctx.timings
        set_process_fields(epoch=epoch.label)           # JSON log field `epoch` for this term
        self._stop = asyncio.Event()
        self._sup: TaskSupervisor | None = None
        self._background: set[asyncio.Task[Any]] = set()
        self._inflight = 0
        self._ingestion_closed = asyncio.Event()
        self._draining = False
        self._closed = False
        self._term_started_ms = self._clock()            # LEADER_INIT entry: the epoch change (inhibition)
        self._settle_until = self._term_started_ms + self._t.settle_ms
        self._maintenance_windows: tuple[MaintenanceWindow, ...] = ()
        self._maintenance_error: str | None = None       # the last unreadable-source error paged (once per streak)
        self._auth_failed_seen: dict[str, str] = {}      # adapter -> failed generation already reported
        self._rearms_done: dict[str, tuple[str, int]] = {}   # adapter -> (generation, rearmed_at_ms) acted on
        self._drain_jobs: set[asyncio.Future[int]] = set()   # DRAINING spooler flushes (never cancelled)
        self.halt_level_name: str | None = None
        # SM5 is out of UNKNOWN only while halt:state is present and its event id = MAX(halt_events.id) (the
        # gate's own test, SM5 edge hx); refreshed by housekeeping, False until the first good read (8.2).
        self.halt_state_current = False
        self.oldest_unacked_feed_ms: int | None = None
        self._feed_lag_paged = False

        self.ports: LeaderPorts = ctx.ports(epoch)
        ports = self.ports
        self.adapters = list(ctx.adapters())
        ids = [a.adapter_id for a in self.adapters]
        if len(set(ids)) != len(ids):
            raise ValueError(f"duplicate adapter ids: {ids}")
        self.adapter_ids: tuple[str, ...] = tuple(ids)
        self._by_id = {a.adapter_id: a for a in self.adapters}
        redis = ctx.redis
        self.redis = redis
        cfg = ctx.oversee

        # --- Redis half, ingestion -------------------------------------------------------------------
        self.identity = BootIdentity(lambda: epoch.label, self._run_id)
        self.no_boot = NoBootQueue()
        self.cache = RedisState(redis, self.identity.expected, no_boot=self.no_boot)
        # Rule configuration (4.5): the RiskSweep and the clean-spread thresholds read the installed ruleset.
        self.rules = ActiveRuleset(cfg, clock_ms=ctx.clock_ms)
        self.ruleset_source: RulesetSource | None = getattr(ports, "rulesets", None)
        self.ruleset_reloader: RulesetReloader | None = None
        clean_spread = ctx.clean_spread if ctx.clean_spread is not None else self.rules.clean_spread
        self.committer = TieredStateCommitter(ports.state_store, self.cache, clean_spread, clock_ms=ctx.clock_ms)
        self.hlc = HybridLogicalClock(ctx.clock_ms)
        live_store: LiveSampleStore | None = getattr(ports, "live_samples", None)
        self.live_sampler: AccountLiveSampler | None = None if live_store is None else AccountLiveSampler(
            live_store, self.hlc.next, clock_ms=ctx.clock_ms, interval_ms=self._t.live_sample_interval_ms)
        self.lifecycle: PartitionMaintenance | None = getattr(ports, "lifecycle", None)
        self.lifecycle_runner = LifecycleRunner(every_ms=self._t.lifecycle_every_ms)
        self.tracking = _TrackingCommitter(self.committer, ctx.clock_ms,
                                           None if self.live_sampler is None else self.live_sampler.offer)
        self.health_sink = TieredStreamHealth(ports.stream_health, RedisStreamHealth(self.cache, ctx.clock_ms))
        self.dlq_sink = _DrainableDlqSink(ports.dlq_sink)
        self.spooler = DlqSpooler(self.dlq_sink, spill_path=ctx.spill_path)
        self.bus = EventBus()
        self.orchestrator = _ObservedOrchestrator(
            self.bus, self.spooler, self.health_sink, self.hlc, clock_ms=ctx.clock_ms,
            local_state=ports.local_state, auth_store=ports.auth_store,
            credential_generation=self._credential_generation, instrument_sink=self.committer,
            on_price_tick=self._on_price_tick, on_execution=self._on_execution, pager=self.page,
            market_open=self._liveness_alerting, price_staleness_suppressed=self._price_alerts_muted,
            transfer_sources=ctx.transfer_sources, refresh_adapter=ctx.refresh_adapter,
            clean_spread=clean_spread)
        for a in self.adapters:
            self.orchestrator.add_adapter(a)
        self.switches = ConsumerSwitches()
        ckw: dict[str, Any] = {"pager": self.page, "clock_ms": ctx.clock_ms}
        self.consumers: list[tuple[TopicConsumer[Any], asyncio.Event]] = [
            (OrderStateConsumer(self.bus.orders, self.tracking, self.spooler, **ckw), self.switches.entities),
            (PositionStateConsumer(self.bus.positions, self.tracking, self.spooler, **ckw), self.switches.entities),
            (AccountImageProjector(self.bus.account, self.tracking, self.spooler, **ckw), self.switches.entities),
            (PriceSubscriber(self.bus.prices, self.tracking, self.spooler, **ckw), self.switches.prices),
        ]

        # --- SM10 -------------------------------------------------------------------------------------
        # Every configured adapter is checked for calendar sessions at init, each reload and each roll (an adapter
        # without any pages instead of leaving the gate silently shut, oversee-rules.md §8); a maintenance window
        # sends one pre-acknowledged info notice (operations.md §10).
        self.calendar = MarketCalendar(on_transition=self._on_transition, on_readiness=self._on_readiness,
                                       on_roll=self._on_roll, on_flag=self._on_calendar_flag,
                                       on_maintenance=self._on_maintenance,
                                       expected_adapters=lambda: self.adapter_ids,
                                       on_maintenance_notice=self._on_maintenance_notice,
                                       subscribed_symbols=self._wanted_symbols)
        self.schedule_loader = _RecordingScheduleLoader(ports.schedule)
        self.calendar_init = ScheduledCalendarInit(self.schedule_loader, self.calendar, adapters=self.adapter_ids)
        self.readiness = ReadinessDispatcher(_ReadinessActions(self))
        self._closes_cache: dict[tuple[str, int, int], dict[SymbolKey, int]] = {}
        self.market_sessions: MarketSessionStore | None = getattr(ports, "market_sessions", None)
        self.schedule_refresher: ScheduleRefresher | None = None
        if self.market_sessions is not None:
            self.schedule_refresher = ScheduleRefresher(
                self.market_sessions, adapter_ids=lambda: self.adapter_ids, adapter=self.orchestrator.adapter,
                ready=self._adapter_streaming, wanted_symbols=self._wanted_symbols,
                stored_symbols=self._stored_symbols, reload=self._reload_calendar,
                calendar_version=lambda: self.calendar.version, pager=self.page, clock_ms=ctx.clock_ms,
                refresh_ms=self._t.schedule_refresh_ms, retry_ms=self._t.schedule_retry_ms)

        # --- Oversee ----------------------------------------------------------------------------------
        # Dependent (stale / FENCE) alert rows are held back during an adapter's maintenance window (no settle
        # after it: FENCE is at full severity from the window's end, operations.md §10) and
        # for RTO + 30 s after this term's epoch change, then one summary or one escalation (operations.md §9).
        self.inhibitor = AlertInhibitor(in_maintenance=self.calendar.in_maintenance,
                                        epoch_changed_at_ms=lambda: self._term_started_ms,
                                        settle_ms=self._t.epoch_settle_ms)
        # pnl_source (7.1): a partial cash-flow fetch makes MaxDailyLossRule FLAG pnl_unverified.
        inputs = SnapshotInputs(session=self._session_state, integrity_incident=self._integrity_incident,
                                pnl_source=self.orchestrator.pnl_source,
                                transfer_source=self.orchestrator.transfer_source,
                                scheduled_closes=self._scheduled_closes)
        self.reader = _StampingReader(ports.snapshot_reader(inputs), ctx.clock_ms)
        self._sweep_started_ms: int | None = None
        self.halt_mirror = _RecordingMirror(RedisHaltMirror(self.cache))
        self.halt = HaltSwitch(_ExemptHaltStore(ports.halt_store), self.halt_mirror)
        # Re-page cadence: the installed ruleset's escalate_after_ms, re-read on every use (a reload changes it).
        self.dispatcher = _ExemptDispatcher(ports.outbox, ports.dedupe, (),
                                            lambda: self.rules.config.sweep.escalate_after_ms,
                                            lease_valid=self._lease_valid)
        self.sweep = _TimedRiskSweep(reader=self.reader, writer=_ExemptSweepWriter(ports.sweep_writer),
                                     registry=self.rules.registry, halt=self.halt, config=self.rules.config,
                                     clock=ctx.clock_ms, epoch=lambda: epoch.token,
                                     sweep_ids=HlcSweepIds(ctx.clock_ms),
                                     cash_flow_requester=self._request_cash_flows, pager=self._oversee_page,
                                     dispatcher=self.dispatcher, on_fence_lost=self._on_fence_lost,
                                     inhibitor=self.inhibitor)
        # The sweep boundary installs a staged ruleset and the dispatcher follows the installed cadence
        # (replaces the ActiveRuleset.wrap_sweep_ids wrapper).
        self.sweep.install_ruleset(self.rules)
        self.operator_requests: OperatorRequestQueue | None = getattr(ports, "operator_requests", None)
        self.auth_rearms: AuthRearmReader | None = getattr(ports, "auth_rearms", None)
        if self.ruleset_source is not None:
            self.ruleset_reloader = RulesetReloader(
                self.ruleset_source, self.rules, pager=self.page, accounts=self._known_accounts,
                sweep_now=self.sweep.sweep_now, poll_s=self._t.ruleset_poll_s)
        self.feed = FeedObserver(_PacedFeedStream(RedisFeedStream(redis, ctx.node_id)), self.sweep, self.identity.expected,
                                 ports.feed_quarantine, ctx.clock_ms, pager=self._oversee_page)
        self.gate = ActionGate(ports.gate_reader(GateInputs(
            node_epoch=self._node_epoch, lease_valid=self._lease_valid, settle_until_ms=lambda: self._settle_until,
            sweep_view=self.sweep.account_view, clock_ms=ctx.clock_ms, latest_open_ms=self._latest_open,
            sweep_interval_ms=cfg.sweep.interval_ms, catchup_verified=self._catchup_verified)))

        # --- integrity --------------------------------------------------------------------------------
        self.sm8 = RedisBootstrap(redis, self.cache, ports.integrity_db, self.identity, self.no_boot,
                                  clock_ms=ctx.clock_ms, price_adapters=lambda: self.adapter_ids,
                                  request_account_snapshots=self._refresh_all_accounts, pager=self._integrity_page,
                                  price_wait_ms=self._t.sm8_price_wait_ms)
        self.ingestion_control = OrchestratorIngestionControl(self.switches)
        self.reconciler = OrchestratorReconciler(self.orchestrator, lambda: self.adapter_ids, self.switches,
                                                 self._ingestion_idle, commit_timeout_s=self._t.reconcile_commit_s)
        patroni: PatroniControl = (ctx.patroni.with_incident_guard(self._incident_may_switch)
                                   if ctx.patroni is not None else _NoPatroni())
        self.incident = IncidentMachine(redis=redis, ingestion=self.ingestion_control, patroni=patroni,
                                        reconciler=self.reconciler, boot=self.sm8, boot_ready=self.sm8.is_booted,
                                        clock_ms=ctx.clock_ms, pager=self._integrity_page,
                                        on_resolved=self._integrity_resolved)
        self.integrity = IntegrityWorker(redis, self.cache, ports.integrity_db, self.identity, self.sm8,
                                         self.incident, clock_ms=ctx.clock_ms, pager=self._integrity_page)
        self.dlq_worker = DlqRecoveryWorker(ports.dlq_store, _DlqPriorityBridge(self.orchestrator,
                                                                                 lambda: self.adapter_ids),
                                            clock_ms=ctx.clock_ms, pager=self._integrity_page)
        self.spill = SpillDrainService(self.spooler, node=ctx.node_id, clock_ms=ctx.clock_ms,
                                       pager=self._integrity_page)

        # --- downtime catch-up (leader only; ports without a `catchup` store run without it) ---------------
        catchup_store: CatchupStore | None = getattr(ports, "catchup", None)
        self.catchup: CatchupCoordinator | None = None if catchup_store is None else CatchupCoordinator(
            self.orchestrator, catchup_store, ports.local_state, adapter_ids=lambda: self.adapter_ids,
            epoch_token=lambda: epoch.token, run_id=ctx.engine_run_id, pager=self.page, settings=ctx.catchup,
            metrics=ctx.catchup_metrics, clock_ms=ctx.clock_ms)

        # --- notifications / dead-man (built at LEADER_INIT step 7) -------------------------------------
        self._http: httpx.AsyncClient | None = None
        self.channels: RotatingChannels | None = None
        self.relay: OutboxRelay | None = None
        self.deadman: DeadmanSender | None = None

    # ================================================================================ LeaderSession
    def init_deps(self, supervisor: TaskSupervisor) -> LeaderInitDeps:
        self._sup = supervisor
        return LeaderInitDeps(
            fencer=self.ports.fencer, seq=self.ports.seq, hlc=self.hlc, redis=_EngineRedisFlush(self.redis),
            sod=self, calendar=self, ingestion=self, sm8=self, risksweep=self, dlq=self,
            terms=self.ports.terms, flags=self, rules=self)

    def broker_links(self) -> Mapping[str, BrokerLink]:
        out: dict[str, BrokerLink] = {}
        for aid in self.adapter_ids:
            state = self.orchestrator.runtime(aid).conn.state
            streaming = state is ConnState.STREAMING
            failed = None if streaming or self.ctx.tls is None else self.ctx.tls.failed_since_ms(aid)
            out[aid] = BrokerLink(net_failed_since_ms=failed, market_open=self.calendar.market_open(aid),
                                  auth_failed=state is ConnState.AUTH_FAILED,
                                  maintenance=self.calendar.in_maintenance(aid), streaming=streaming)
        return out

    def streaming_adapters(self) -> Mapping[str, bool]:
        return {aid: self.orchestrator.runtime(aid).conn.state is ConnState.STREAMING for aid in self.adapter_ids}

    # ================================================================================ uptime / catch-up
    def coverage_targets(self) -> tuple[int, list[str]] | None:
        """For the UptimeTracker's adapter_coverage heartbeat: (epoch token, the adapters that are STREAMING, have
        every stream reconciled and whose downtime catch-up vouches for it: finished, not retryable-incomplete,
        no interruption since its read; `CatchupCoordinator.vouches`). None while this session
        cannot vouch for anything (no catch-up store, lease not held, DRAINING or closed): coverage never moves
        past data that was not committed and caught up."""
        catchup = self.catchup
        if catchup is None or self._draining or self._closed or not self.ctx.lease.holds_lease():
            return None
        out: list[str] = []
        for aid in self.adapter_ids:
            rt = self.orchestrator.runtime(aid)
            if (rt.conn.state is ConnState.STREAMING and not rt.fence_holds
                    and not any(sub.lost for sub in rt.subs.values()) and catchup.vouches(aid)):
                out.append(aid)
        return self.epoch.token, out

    def _catchup_verified(self, adapter_id: str) -> bool:
        """ActionGate input: this term's downtime catch-up of the adapter was complete (`CatchupCoordinator.verified`).
        Ports without a catch-up store (in-memory test ports only; production always has one) keep the gate as
        it was before the catch-up existed."""
        catchup: CatchupCoordinator | None = getattr(self, "catchup", None)
        if catchup is None:
            return getattr(self.ports, "catchup", None) is None
        return catchup.verified(adapter_id)

    def resume_after_sleep(self, suspended_ms: int | None = None) -> bool:
        """The process was suspended (UptimeTracker): force SM4 RESYNCING and a `resume_after_sleep` catch-up for
        every adapter (fence held until each one finished). False when this session runs no catch-up."""
        if self.catchup is None or self._draining or self._closed:
            return False
        self.catchup.resume_after_sleep(suspended_ms)
        return True

    async def drain(self, reason: EndReason) -> None:
        """Stop intake, flush the DlqSpooler (PostgreSQL, or the spill file after 2 s), write this term's
        end_reason, close sockets. The controller then stops the supervisor and releases the lease."""
        log.warning("DRAINING (%s), epoch %s", reason, self.epoch.label)
        self._draining = True
        self.sweep.stop()
        self.feed.stop()
        self._stop.set()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._ingestion_closed.wait(), self._t.drain_close_s)
        await self._drain_dlq()
        try:
            await asyncio.wait_for(self.ports.terms.record_end_reason(self.epoch, reason), self._t.drain_record_s)
        except Exception as exc:
            log.error("DRAINING: end_reason not written: %r", exc)
        if self.ctx.cluster is not None:
            try:
                await asyncio.wait_for(self.ctx.cluster.flush(self._t.drain_cluster_s),
                                       self._t.drain_cluster_s + DRAIN_CLUSTER_GRACE_S)
            except Exception as exc:
                log.warning("DRAINING: cluster_state writes not flushed: %r", exc)
        await self.aclose(self._t.drain_aclose_s)

    async def _drain_dlq(self) -> None:
        """Flush the DlqSpooler to PostgreSQL; whatever PostgreSQL has not taken after drain_flush_s goes
        to the local spill file (operations.md §4), the batch in flight included. The flush is never
        cancelled: the sink is abandoned instead, so the spooler spills through its own ConnectionError
        path. The sink stays abandoned for the rest of the term, so a row offered later, or a background
        flush the supervisor cancels, can only reach the spill file, never vanish mid-insert."""
        flush = self._drain_flush_job()
        done, _ = await asyncio.wait({flush}, timeout=self._t.drain_flush_s)
        if not done:
            log.error("DRAINING: PostgreSQL did not take the DLQ within %.0f s; spilling to %s",
                      self._t.drain_flush_s, self.spooler.spill_path)
        self.dlq_sink.abandon()
        deadline = asyncio.get_running_loop().time() + self._t.drain_spill_s
        if not done:
            done, _ = await asyncio.wait({flush}, timeout=self._t.drain_spill_s)
        if not flush.done():
            log.error("DRAINING: DLQ spill did not finish within %.0f s", self._t.drain_spill_s)
            return
        if not flush.cancelled() and flush.exception() is not None:
            log.error("DRAINING: DLQ flush failed: %r", flush.exception())
        # Rows offered since: spill file only. Never cancelled either (a cancelled spill append still waits
        # for its worker thread, which would hold DRAINING past its budget): the job is left running and
        # `spill_busy()` keeps the node's standby spill drain off the file until it is done.
        left = max(DRAIN_FINAL_SPILL_MIN_S, deadline - asyncio.get_running_loop().time())
        final = self._drain_flush_job()
        done, _ = await asyncio.wait({final}, timeout=left)
        if not done:
            log.error("DRAINING: final DLQ spill did not finish within %.1f s", left)
            return
        if not final.cancelled() and final.exception() is not None:
            log.error("DRAINING: final DLQ spill failed: %r", final.exception())
        if len(self.spooler):
            log.error("DRAINING: %d DLQ rows could not be spilled", len(self.spooler))

    def _drain_flush_job(self) -> asyncio.Future[int]:
        job = asyncio.ensure_future(self.spooler.flush())
        self._drain_jobs.add(job)
        job.add_done_callback(self._drain_jobs.discard)
        job.add_done_callback(_retrieve_result)
        return job

    def spill_busy(self) -> bool:
        """A DRAINING flush of this term's DlqSpooler that DRAINING stopped waiting for is still running (it may
        still append to the spill file): the node's standby spill drain must not read or truncate the file
        meanwhile, or rows appended between its read and its truncate would be lost."""
        return any(not j.done() for j in self._drain_jobs)

    async def aclose(self, timeout_s: float = 5.0) -> None:
        """Release what the term holds (HTTP client, term pool, background work), within `timeout_s`.
        Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        clear_epoch_if(self.epoch.label)
        for t in list(self._background):
            t.cancel()

        async def close_all() -> None:
            if self._http is not None:
                with contextlib.suppress(Exception):
                    await self._http.aclose()
            await self.ports.close()

        try:
            await asyncio.wait_for(close_all(), timeout_s)
        except Exception as exc:
            log.warning("closing the term pool failed: %r", exc)

    def spill_torn_bytes(self) -> int:
        """The `<spill>.torn` size this term's drain service last saw (its tails are already paged): the
        NodeController hands it to the standby spill drain as its baseline after DRAINING."""
        return self.spill.health().torn_bytes

    # ================================================================================ LEADER_INIT
    async def load_ruleset(self, now_ms: int) -> int | None:
        """After step 3: install the active ruleset before the first RiskSweep pass (4.5). Without a
        rulesets port the base config (SessionContext.oversee) runs."""
        if self.ruleset_reloader is None:
            return self.rules.version
        res = await self.ruleset_reloader.load_initial()
        log.warning("LEADER_INIT ruleset: %s v%s %s", res.outcome, res.version, res.detail)
        return res.version

    async def catch_up(self, now_ms: int) -> SodOutcome:
        """Step 4: a missing sod_marks row of the current trading day is written by one full RiskSweep
        pass (the pass computes it from the pre-roll last_clean_marks and marks partial_session)."""
        day = trading_day_for_roll(latest_roll_ms(now_ms))
        missing = list(await self.ports.sod.missing_accounts(day))
        if not missing:
            return SodOutcome()
        await self.sweep.run_pass(None)
        status = await self.ports.sod.status(day, missing)
        return SodOutcome(status.rows, status.partial)

    async def start_adapters(self) -> None:
        """Step 6: SM4 for every adapter (one critical task), the consumers and the DLQ spooler. Before SM4 starts,
        the downtime catch-up holds every adapter's stream_health fence (no reconcile clears it until that
        adapter's catch-up has finished) and its supervised task waits for each first committed SNAPSHOTTING."""
        catchup = self.catchup
        if catchup is not None:
            catchup.begin_startup()
            self._add(self._monitored("catchup", 60.0, lambda ctx: catchup.run(self._stop, stamp=ctx.stamp),
                                      self_stamping=True))
        self._add(TaskSpec(INGESTION_TASK, self._ingestion, max_stamp_age_s=10.0, critical=True))
        for consumer, switch in self.consumers:
            self._add(TaskSpec(f"consumer:{consumer.name}", self._consumer_body(consumer, switch),
                               max_stamp_age_s=15.0))
        self._add(TaskSpec("dlq-spooler", self._spooler_body, max_stamp_age_s=15.0))

    async def bootstrap(self) -> None:
        """Step 6: SM8 all steps; returns once redis:boot_id is written. Then serve later signals."""
        deadline = self._clock() + int(self._t.sm8_deadline_s * 1000)
        if not await self.sm8.run_once(in_place=False, deadline_ms=deadline):
            raise RuntimeError("SM8: redis:boot_lock is held by another owner after FLUSHDB")
        self._add(self._monitored("sm8", 60.0, lambda ctx: self.sm8.run(self._stop)))

    async def start_risksweep(self) -> None:
        """Step 6: the RiskSweep at once (reading PostgreSQL), the feed observer (waits for SM8 itself),
        housekeeping (feed trim, halt level), the SM10 tick and the readiness dispatcher."""
        self._add(TaskSpec(RISKSWEEP_TASK, self._risksweep_body, max_stamp_age_s=5.0))
        self._add(self._monitored("feed-observer", 30.0, lambda ctx: self.feed.run(),
                                  liveness=lambda: self.feed.liveness_ms))
        self._add(TaskSpec("housekeeping", self._housekeeping_body, max_stamp_age_s=60.0))
        self._add(self._monitored("calendar", 10.0,
                                  lambda ctx: self.calendar.run(self._stop, self._clock, stamp=ctx.stamp),
                                  self_stamping=True))
        self._add(self._monitored("readiness", 120.0, lambda ctx: self.readiness.run(self._stop)))
        reloader = self.ruleset_reloader
        if reloader is not None:
            self._add(self._monitored("ruleset", max(30.0, 4 * self._t.ruleset_poll_s),
                                      lambda ctx: reloader.run(self._stop, stamp=ctx.stamp), self_stamping=True))
        if self.live_sampler is not None:
            self._add(TaskSpec("account-sampler", self._sampler_body, max_stamp_age_s=60.0))
        if self.lifecycle is not None:
            self._add(TaskSpec("lifecycle", self._lifecycle_body,
                               max_stamp_age_s=max(120.0, 4 * self._t.lifecycle_poll_s)))
        if self.ctx.maintenance is not None:
            self._add(TaskSpec("maintenance", self._maintenance_body,
                               max_stamp_age_s=max(120.0, 4 * self._t.maintenance_poll_s)))
        refresher = self.schedule_refresher
        if refresher is not None:
            self._add(self._monitored(
                "schedule-refresh", max(120.0, 4 * self._t.schedule_poll_s),
                lambda ctx: refresher.run(self._stop, poll_s=self._t.schedule_poll_s, stamp=ctx.stamp),
                self_stamping=True))

    async def startup_barrier(self) -> None:
        """Step 7: drain the spill file a crash left (never raises), then the DLQ / integrity /
        notification / dead-man / SAFE_HOLD-mirror tasks."""
        drained = await self.spill.drain_at_startup()
        if drained is None:
            log.warning("DLQ start-up barrier: PostgreSQL refused the spill drain; the drain task retries")
        self._add(self._monitored("spill-drain", 60.0, lambda ctx: self.spill.run(self._stop),
                                  liveness=lambda: self.spill.liveness_ms))
        self._add(self._monitored("dlq-recovery", 60.0, lambda ctx: self.dlq_worker.run(self._stop),
                                  liveness=lambda: self.dlq_worker.liveness_ms))
        self._add(self._monitored("integrity", 30.0, lambda ctx: self.integrity.run(self._stop),
                                  liveness=lambda: self.integrity.liveness_ms))
        # The first dead-man beat must carry the halt level and SM5 already read: Melbourne pages a null health
        # field at once (8.2), so it must not race housekeeping's first pass. Never raises (any error: SM5 UNKNOWN).
        await self.refresh_halt_view()
        self._start_notifications()
        if self.ctx.cluster is not None:
            self._add(TaskSpec("safe-hold-mirror", self._mirror_body, max_stamp_age_s=60.0))
        self._add(TaskSpec(OPERATOR_TASK, self._operator_body,
                           max_stamp_age_s=max(60.0, 4 * self._t.operator_poll_s)))

    def flag(self, code: str, detail: str) -> None:
        self.page("high", f"flag:{code}", detail)

    def _start_notifications(self) -> None:
        self._http = self.ctx.http_client()
        loader = self.ctx.notification_settings
        if loader is not None:
            # Channel credentials follow the secrets source (operations.md §13): re-read every
            # credentials_poll_ms and at once after an auth error. `initial=` never raises at build time.
            self.channels = RotatingChannels(loader, self._http, smtp_factory=self.ctx.smtp_factory,
                                             invalidate=self.ctx.invalidate_secrets or (lambda: None),
                                             initial=self.ctx.notifications)
            cs = self.channels.current
        else:
            cs = build_channels(self.ctx.notifications, self._http, smtp_factory=self.ctx.smtp_factory)
        dispatcher = self.dispatcher
        self.relay = OutboxRelay(self.ports.outbox, self.ports.dedupe, cs.channels, disabled=cs.disabled,
                                 issues=cs.issues, lease_valid=self._lease_valid, clock_ms=self._clock,
                                 channel_source=self.channels,
                                 escalate_after_ms=lambda: dispatcher.escalate_after_ms,
                                 on_auth_error=self._on_channel_auth_error, on_escalation=self._on_escalation)
        relay = self.relay
        self._add(self._monitored("outbox-relay", RELAY_MAX_STAMP_AGE_S,
                                  lambda ctx: relay.run(self._stop, stamp=ctx.stamp), self_stamping=True))
        url = self.ctx.deadman.url
        if url is None:
            log.error("dead-man heartbeat disabled: XTNL_DEADMAN_URL is not set")
            return
        # 7.4: the bearer tokens follow the secrets source (re-read per POST from the cached secret; a 401 / 403
        # re-reads the secret), and the dashboard gets a copy of each heartbeat (its Redis freshness check).
        invalidate = self.ctx.invalidate_secrets
        copies: list[DeadmanTransport] = []
        copy = self.ctx.heartbeat_copy
        if copy.url is not None:
            try:
                copies.append(HttpsDeadmanTransport(copy.url, token=copy.token, client=self._http,
                                                    token_source=self.ctx.heartbeat_copy_token,
                                                    on_auth_error=invalidate))
            except ValueError as exc:            # validated at start-up (load_settings); never fails the term
                log.error("dashboard heartbeat copy disabled: %s", exc)
        self.deadman = DeadmanSender(url, _DeadmanView(self), lease_valid=self._lease_valid,
                                     is_leader=self.ctx.lease.holds_lease, client=self._http,
                                     clock_ms=self._clock, token=self.ctx.deadman.token,
                                     token_source=self.ctx.deadman_token, on_auth_error=invalidate,
                                     copies=copies)
        sender = self.deadman
        self._add(self._monitored("deadman", 60.0, lambda ctx: sender.run(self._stop, stamp=ctx.stamp),
                                  self_stamping=True))

    # ================================================================================ tasks
    def _add(self, spec: TaskSpec) -> None:
        if self._sup is None:
            raise RuntimeError("init_deps(supervisor) must run before tasks are started")
        self._sup.add(spec)

    def _monitored(self, name: str, max_age_s: float, run: Callable[[TaskContext], Awaitable[None]], *,
                   liveness: Callable[[], int | None] | None = None, self_stamping: bool = False,
                   critical: bool = False) -> TaskSpec:
        """A supervised task around a component loop. The monitor stamps while the loop is alive and,
        when the component exposes `liveness_ms`, only while that is fresh. A loop that ends outside
        DRAINING is a death (restart with backoff); one that raises propagates its error."""
        async def factory(ctx: TaskContext) -> None:
            inner = asyncio.ensure_future(run(ctx))
            try:
                while True:
                    if inner.done():
                        if self._stop.is_set():
                            await asyncio.Event().wait()     # DRAINING: wait to be cancelled
                        inner.result()
                        raise RuntimeError(f"{name} loop returned")
                    if not self_stamping:
                        lv = liveness() if liveness is not None else None
                        if liveness is None or (lv is not None and self._clock() - lv <= max_age_s * 1000):
                            ctx.stamp()
                    await asyncio.wait({inner}, timeout=self._t.monitor_poll_s)
            finally:
                if not inner.done():
                    inner.cancel()
                with contextlib.suppress(BaseException):
                    await inner
        return TaskSpec(name, factory, max_stamp_age_s=max_age_s, critical=critical)

    async def _ingestion(self, ctx: TaskContext) -> None:
        """Leadership-critical: SM4 per adapter plus the stream-health mirror. A driver restarts itself after a
        non-fence error (SM4 backoff, `IngestionOrchestrator.run_adapter`); only a lost fence (`EpochMismatch`
        from a storage port) or a cancellation ends a driver, and a driver that raises ends this task, which
        hard-exits the process."""
        tasks: dict[str, asyncio.Task[None]] = {
            aid: asyncio.create_task(self.orchestrator.run_adapter(aid, self._stop), name=f"sm4:{aid}")
            for aid in self.adapter_ids}
        tasks["health-mirror"] = asyncio.create_task(self.orchestrator.health.run(self._stop), name="health-mirror")
        try:
            while True:
                for name, t in list(tasks.items()):
                    if not t.done():
                        continue
                    if not t.cancelled() and t.exception() is not None:
                        exc = t.exception()
                        raise RuntimeError(f"{name} failed: {type(exc).__name__}: {exc}") from exc
                    if not self._stop.is_set():
                        state = (self.orchestrator.runtime(name).conn.state.value
                                 if name in self._by_id else "-")
                        log.critical("%s driver ended (%s)", name, state)
                    del tasks[name]
                if self._stop.is_set() and not tasks:
                    self._ingestion_closed.set()
                    await asyncio.Event().wait()
                ctx.stamp()
                await asyncio.sleep(0.5)
        finally:
            for t in tasks.values():
                t.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            self._ingestion_closed.set()

    def _consumer_body(self, consumer: TopicConsumer[Any], switch: asyncio.Event
                       ) -> Callable[[TaskContext], Coroutine[Any, Any, None]]:
        async def body(ctx: TaskContext) -> None:
            while True:
                if not switch.is_set():
                    ctx.set_exempt("ingestion halted (SM3)")
                    await switch.wait()
                    ctx.set_exempt(None)
                try:
                    item = await asyncio.wait_for(consumer.topic.get(), self._t.consumer_poll_s)
                except TimeoutError:
                    ctx.stamp()
                    continue
                self._inflight += 1
                try:
                    if not switch.is_set():          # halted while this consumer was waiting: hold the item
                        ctx.set_exempt("ingestion halted (SM3)")
                        await switch.wait()
                        ctx.set_exempt(None)
                    with ctx.exempt_wait("postgres"), log_context(**entity_fields(item)):
                        await consumer.process(item)
                finally:
                    self._inflight -= 1
                ctx.stamp()
        return body

    async def _spooler_body(self, ctx: TaskContext) -> None:
        while True:
            try:
                with ctx.exempt_wait("postgres"):
                    await self.spooler.flush()
            except Exception:
                log.exception("DLQ flush failed")
            ctx.stamp()
            await asyncio.sleep(self._t.spooler_interval_s)

    async def _risksweep_body(self, ctx: TaskContext) -> None:
        if self._sweep_started_ms is None:
            self._sweep_started_ms = self._clock()
        self.reader.ctx = ctx
        token = _PG_TASK.set(ctx)      # this task's context only: its PostgreSQL waits count as alive
        try:
            await self.sweep.run()
        finally:
            _PG_TASK.reset(token)
            if self.reader.ctx is ctx:
                self.reader.ctx = None
        if self._stop.is_set():
            await asyncio.Event().wait()

    async def _housekeeping_body(self, ctx: TaskContext) -> None:
        """Every 10 s: XTRIM oversee:feed (lag alert at 50 % of the cap), the oldest un-acked feed entry
        and the effective halt level (dead-man fields)."""
        while True:
            try:
                res = await trim_feed(self.redis)
                if res.lag_alert and not self._feed_lag_paged:
                    self._feed_lag_paged = True
                    self.page("high", "oversee:feed lag", f"oversee:feed holds {res.length} entries (>= 50 % of cap)")
                elif not res.lag_alert:
                    self._feed_lag_paged = False
            except Exception as exc:
                log.warning("feed trim failed: %r", exc)
            try:
                self.oldest_unacked_feed_ms = await self._oldest_pending_ms()
            except Exception:
                self.oldest_unacked_feed_ms = None
            with ctx.exempt_wait("postgres"):
                await self.refresh_halt_view()
            ctx.stamp()
            await asyncio.sleep(self._t.housekeeping_s)

    async def refresh_halt_view(self) -> None:
        """The dead-man halt fields: the effective level from PostgreSQL `halt_events` and whether SM5 is out of
        UNKNOWN, i.e. Redis `halt:state` is present and carries MAX(halt_events.id) (the gate's
        `halt_state_missing` / `halt_state_not_current` test). Any read error leaves SM5 UNKNOWN (fail closed)."""
        max_id: int | None = None
        try:
            status = await self.halt.status()
            self.halt_level_name = status.effective_level.name
            max_id = status.event_id
        except Exception as exc:
            self.halt_state_current = False
            log.warning("halt status unreadable: %r", exc)
            return
        try:
            mirrored = await self.halt_mirror.read()
        except Exception as exc:
            self.halt_state_current = False
            log.warning("halt:state unreadable: %r", exc)
            return
        self.halt_state_current = mirrored is not None and max_id is not None and mirrored[1] == max_id

    async def _oldest_pending_ms(self) -> int | None:
        info = await self.redis.xpending(K.FEED, FEED_GROUP)
        if not isinstance(info, dict) or not int(info.get("pending", 0) or 0):
            return None
        raw = info.get("min")
        text = raw.decode() if isinstance(raw, bytes) else str(raw)
        return int(text.split("-", 1)[0])

    async def _mirror_body(self, ctx: TaskContext) -> None:
        cluster = self.ctx.cluster
        assert cluster is not None
        while True:
            try:
                with ctx.exempt_wait("postgres"):
                    await cluster.mirror_safe_hold()
            except Exception as exc:
                log.warning("SAFE_HOLD mirror failed: %r", exc)
            ctx.stamp()
            await asyncio.sleep(self._t.mirror_interval_s)

    async def _maintenance_body(self, ctx: TaskContext) -> None:
        """Declared broker maintenance windows (SessionContext.maintenance) -> the SM10 overlay."""
        while True:
            await self.refresh_maintenance()
            ctx.stamp()
            await asyncio.sleep(self._t.maintenance_poll_s)

    async def _sampler_body(self, ctx: TaskContext) -> None:
        """Live account sampler (4.8): due samples -> account_snapshots.live_ts_ms (fenced, rate-limited)."""
        sampler = self.live_sampler
        assert sampler is not None
        while True:
            with ctx.exempt_wait("postgres"):
                await sampler.flush()
            ctx.stamp()
            await asyncio.sleep(self._t.live_sample_flush_s)

    async def _lifecycle_body(self, ctx: TaskContext) -> None:
        """Data lifecycle (4.8): partitions ahead, archived-partition detach, outbox retention."""
        job, runner = self.lifecycle, self.lifecycle_runner
        assert job is not None
        while True:
            now = self._clock()
            if runner.due(now):
                runner.ran(now)
                try:
                    with ctx.exempt_wait("postgres"):
                        rep = await job.run_once(now)
                except Exception as exc:
                    if any(c.__name__ == "EpochMismatch" for c in type(exc).__mro__):
                        raise
                    log.warning("lifecycle run failed: %r", exc)
                else:
                    log.info("lifecycle: %d partitions ensured, detached %s, outbox -%d", len(rep.ensured),
                             rep.detached, rep.outbox_deleted)
                    for severity, title, body in runner.pages(rep):
                        self.page(severity, title, body)
            ctx.stamp()
            await asyncio.sleep(self._t.lifecycle_poll_s)

    async def _operator_body(self, ctx: TaskContext) -> None:
        """Operator requests, operator re-arms and the AUTH_FAILED secret re-read (IMPLEMENTATION_PLAN 4.9)."""
        while True:
            try:
                with ctx.exempt_wait("postgres"):
                    await self.operator_pass()
            except Exception as exc:
                if is_fence_loss(exc):
                    raise
                log.warning("operator pass failed: %r", exc)
            ctx.stamp()
            await asyncio.sleep(self._t.operator_poll_s)

    async def operator_pass(self) -> None:
        """One pass: report newly failed credential generations, re-arm adapters an operator re-armed,
        then consume this epoch's operator requests."""
        self._note_auth_failures()
        await self._rearm_from_operator()
        await self._consume_operator_requests()

    def _note_auth_failures(self) -> None:
        """A broker credential error (SM4 AUTH_FAILED) asks the secrets source to re-read the secret at once,
        once per (adapter, failed generation), so a rotated credential is seen without waiting for the
        periodic refresh (operations.md §13)."""
        for aid in self.adapter_ids:
            rt = self.orchestrator.runtime(aid)
            if rt.conn.state is not ConnState.AUTH_FAILED:
                continue
            gen = rt.failed_generation or "?"
            if self._auth_failed_seen.get(aid) == gen:
                continue
            self._auth_failed_seen[aid] = gen
            log.error("%s: AUTH_FAILED (generation %s): secrets cache invalidated", aid, gen)
            if self.ctx.on_auth_failed is not None:
                try:
                    self.ctx.on_auth_failed(aid)
                except Exception as exc:
                    log.warning("secrets invalidate failed: %r", exc)

    async def _rearm_from_operator(self) -> None:
        """An adapter sitting in AUTH_FAILED is re-armed once the operator CLI set its failed generation to
        REARMED in adapter_auth_state (`python -m src.ops adapter rearm`). Each operator re-arm (its
        `rearmed_at_ms`) is acted on once: SM4 enters AUTH_FAILED before its AUTH_FAILED row is written, so a
        pass that read the row still REARMED in that window must not re-arm the adapter a second time (another
        round of credential attempts against the broker: lock-out risk, operations.md §13)."""
        reader = self.auth_rearms
        if reader is None:
            return
        for aid in self.adapter_ids:
            rt = self.orchestrator.runtime(aid)
            gen = rt.failed_generation
            if rt.conn.state is not ConnState.AUTH_FAILED or gen is None:
                continue
            marker = await reader.rearmed_at(aid, gen)
            if marker is None or self._rearms_done.get(aid) == (gen, marker):
                continue
            self._rearms_done[aid] = (gen, marker)
            log.warning("%s: operator re-armed generation %s; leaving AUTH_FAILED", aid, gen)
            self.orchestrator.rearm(aid)
            self.page("info", f"adapter re-armed: {aid}", f"generation {gen} re-armed by an operator")

    async def _consume_operator_requests(self) -> None:
        queue = self.operator_requests
        if queue is None or self._sup is None:
            return
        for req in await queue.pending(self.epoch.token):
            if req.kind == "reset_task":
                try:
                    self._sup.reset_task(req.target, req.operator)
                    result = "applied"
                except KeyError:
                    result = "unknown_task"
            elif req.kind == "release_account":
                result = self._release_account(req.target, req.operator, req.payload)
            elif req.kind == "refresh_cash_flows":
                result = await self._refresh_cash_flows(req.target, req.operator, req.payload)
            else:
                result = "unsupported_kind"
            log.warning("operator request %d %s %s by %s: %s", req.request_id, req.kind, req.target,
                        req.operator, result)
            await queue.consume(self.epoch.token, req.request_id, result)

    def _release_account(self, target: str, operator: str, payload: object) -> str:
        """`python -m src.ops account release`: the RiskSweep releases its quarantine of the account (the next
        good pass also releases the stored row)."""
        try:
            req = parse_release(target, payload)
        except RequestPayloadError as exc:
            log.warning("release_account %s: bad payload: %s", target, exc)
            return "bad_payload"
        self.sweep.release_account(operator, req.account_id)
        return "applied"

    async def _refresh_cash_flows(self, target: str, operator: str, payload: object) -> str:
        """`python -m src.ops cash-flows refresh`: fetch_cash_flows since the current roll through the
        orchestrator (the single publisher; at most once a minute per account, operations.md §15). An explicit
        `since_ms` before the roll is refused (`since_unsupported`): the orchestrator fetches from the roll."""
        try:
            req = parse_cash_flow_refresh(target, payload)
        except RequestPayloadError as exc:
            log.warning("refresh_cash_flows %s: bad payload: %s", target, exc)
            return "bad_payload"
        owner = next((aid for aid in self.adapter_ids if req.account_id in self.orchestrator.runtime(aid).accounts),
                     None)
        if owner is None:
            return "unknown_account"
        if req.adapter_id is not None and req.adapter_id != owner:
            return "adapter_mismatch"
        if req.since_ms is not None and req.since_ms < last_roll_ms(self._clock()):
            return "since_unsupported"
        if self.orchestrator.runtime(owner).conn.state is not ConnState.STREAMING:
            return "not_streaming"
        try:
            ok = await self.orchestrator.request_cash_flows(req.account_id)
        except Exception as exc:
            if is_fence_loss(exc):
                raise
            log.warning("refresh_cash_flows %s by %s failed: %r", req.account_id, operator, exc)
            return "fetch_failed"
        return "requested" if ok else "rate_limited"

    # ================================================================================ helpers
    def _background_task(self, coro: Coroutine[Any, Any, Any], what: str) -> None:
        async def wrapper() -> None:
            try:
                await coro
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("%s failed: %r", what, exc)

        try:
            task = asyncio.get_running_loop().create_task(wrapper(), name=what)
        except RuntimeError:
            coro.close()
            return
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _run_id(self) -> str:
        rid = await self.ctx.run_id_reader(self.redis)
        if not rid:
            raise RuntimeError("Redis INFO returned no run_id")
        return rid

    def _known_accounts(self) -> list[str]:
        """Every account the RiskSweep knows (the last full pass's list plus committed states)."""
        accts = set(self.reader.accounts)
        accts |= {s.account_id for s in self.sweep.committed_states()}
        return sorted(accts)

    def _on_price_tick(self, tick: PriceDTO, snapshot: bool) -> None:
        """SM10 open-market evidence (4.8): every tick, on the parser path (pure, no I/O)."""
        if self.calendar.version is None:
            return
        self.calendar.on_tick(tick.adapter_id, tick.symbol, tick.bid, tick.ask, tick.timestamp_ms, self._clock(),
                              snapshot=snapshot)

    def _on_execution(self, adapter_id: str, symbol: str, ts_ms: int) -> None:
        """SM10 execution evidence (operations.md §10): an execution on the symbol, on the parser path (pure,
        no I/O). `ts_ms` is the row's receipt time."""
        if self.calendar.version is None:
            return
        self.calendar.on_execution(adapter_id, symbol, ts_ms)

    def _credential_generation(self, adapter_id: str) -> str:
        # The adapter the orchestrator drives now: a rotated replacement reports its own generation.
        return self.ctx.credential_generation(self.orchestrator.adapter(adapter_id))

    def _on_fence_lost(self, exc: BaseException) -> None:
        if self.ctx.on_fence_lost is not None:
            self.ctx.on_fence_lost(exc)
            return
        log.critical("lost fence: %s", exc)
        self.ctx.exit_fn(f"fence: {exc}")

    def _market_open_for_liveness(self, adapter_id: str) -> bool:
        """conn-key liveness pages only while the adapter's market is OPEN and it is not in maintenance."""
        return self.calendar.market_open(adapter_id) and not self.calendar.in_maintenance(adapter_id)

    def _epoch_settling(self) -> bool:
        """Within RTO + 30 s of this term's epoch change (operations.md §9 Inhibition)."""
        return self._clock() < self._term_started_ms + self._t.epoch_settle_ms

    def _liveness_alerting(self, adapter_id: str) -> bool:
        """The orchestrator's conn-key liveness pages: the market is OPEN, no maintenance, and the epoch change
        settled. A key still absent when the settle ends pages at once (its absence is measured throughout)."""
        return not self._epoch_settling() and self._market_open_for_liveness(adapter_id)

    def _price_alerts_muted(self, adapter_id: str, symbol: str) -> bool:
        """The orchestrator's stale-price / no-clean-mid pages: muted by the calendar, and while the epoch change
        settles (their grace then restarts at the end of the settle)."""
        return self._epoch_settling() or self._price_muted(adapter_id, symbol)

    def _price_muted(self, adapter_id: str, symbol: str) -> bool:
        """Price staleness is muted while the symbol is not OPEN; a symbol the calendar does not know (e.g. an
        FX cross subscribed only for marks) follows its adapter's market (5.2 wiring correction)."""
        st = self.calendar.state(adapter_id, symbol)
        if st is not None:
            return st not in OPEN_STATES
        return not self.calendar.market_open(adapter_id)

    def _scheduled_closes(self, adapter_id: str, roll_ms: int) -> Mapping[SymbolKey, int]:
        schedule = self.schedule_loader.last
        version = -1 if schedule is None else schedule.version
        key = (adapter_id, roll_ms, version)
        hit = self._closes_cache.get(key)
        if hit is None:
            if len(self._closes_cache) > 64:
                self._closes_cache.clear()
            hit = scheduled_closes_before(schedule, adapter_id, roll_ms)
            self._closes_cache[key] = hit
        return hit

    def _adapter_streaming(self, adapter_id: str) -> bool:
        return self.orchestrator.runtime(adapter_id).conn.state is ConnState.STREAMING

    def _wanted_symbols(self, adapter_id: str) -> list[str]:
        rt = self.orchestrator.runtime(adapter_id)
        return sorted(rt.tick_symbols | rt.pending_symbols)

    def _stored_symbols(self, adapter_id: str) -> list[str]:
        schedule = self.schedule_loader.last
        if schedule is None:
            return []
        return sorted(sym for aid, sym in schedule.symbols if aid == adapter_id)

    async def init_calendar(self, now_ms: int) -> int:
        """LEADER_INIT step 5 (`CalendarInit`): read the declared maintenance windows first, so SM10's state is
        computed with the maintenance overlay already in force (operations.md §4 step 5), then load the schedule.
        An unreadable source pages and the calendar starts without declared windows (never a hard exit); the
        supervised `maintenance` task keeps re-reading it."""
        await self.refresh_maintenance()
        version = await self.calendar_init.init_calendar(now_ms)
        await self.seed_calendar_quotes(now_ms)
        return version

    async def seed_calendar_quotes(self, now_ms: int) -> int:
        """LEADER_INIT step 5, after SM10 is computed (7.2): the pre-close reference quotes from `last_clean_marks`,
        so a takeover while CLOSED compares the first post-close tick with the quote the market closed at. An
        unreadable table is logged and seeds nothing (the first post-close quote then becomes the reference, never
        evidence); only a lost fence propagates. Returns the number of symbols seeded."""
        try:
            marks = await self.ports.integrity_db.last_clean_marks()
            rows = [(m.adapter_id, m.symbol, m.bid, m.ask, m.ts) for m in marks if m.adapter_id in self._by_id]
            seeded = self.calendar.seed_quotes(rows, now_ms)
        except Exception as exc:
            if is_fence_loss(exc):
                raise
            log.warning("SM10: pre-close reference quotes not seeded: %s", redact_urls(f"{type(exc).__name__}: {exc}"))
            return 0
        log.info("SM10: %d pre-close reference quotes seeded from last_clean_marks", seeded)
        return seeded

    async def _reload_calendar(self) -> int:
        return await self.calendar_init.init_calendar(self._clock())

    def _on_channel_auth_error(self, channel: str) -> None:
        log.error("notifications: %s rejected its credentials; re-reading the secret", channel)

    def _on_escalation(self, esc: Escalation) -> None:
        # Never writes notification_outbox (the relay owns it); the dashboard reads escalated_since().
        log.warning("notifications: alert escalated to another channel: %s", esc)

    def _lease_valid(self) -> bool:
        return self.ctx.lease.lease_valid()

    def _node_epoch(self) -> int | None:
        return self.epoch.token if self.ctx.lease.holds_lease() else None

    def _latest_open(self, adapter_id: str) -> int | None:
        if self.calendar.version is None:
            return None
        return self.calendar.latest_open_ms(adapter_id, self._clock())

    def _session_state(self, adapter_id: str, symbol: str) -> Literal["OPEN", "ROLLOVER", "CLOSED"]:
        """The session the RiskSweep marks a symbol under: `MarketCalendar.mark_session` (7.2: BROKER_MAINTENANCE
        is CLOSED, PRE_OPEN is CLOSED, an unknown symbol or no calendar loaded is OPEN)."""
        if self.calendar.version is None:
            return "OPEN"
        return self.calendar.mark_session(adapter_id, symbol)

    def _integrity_incident(self) -> bool:
        return self.integrity.integrity_incident

    def _incident_may_switch(self) -> bool:
        return self.incident.state in _INCIDENT_SWITCH_STATES

    def _integrity_resolved(self) -> None:
        self.integrity.checker.reset()

    def _ingestion_idle(self) -> bool:
        return self._inflight == 0 and all(c.topic.empty() for c, _ in self.consumers)

    async def _request_cash_flows(self, account_id: str) -> None:
        await self.orchestrator.request_cash_flows(account_id)

    async def _refresh_all_accounts(self) -> None:
        """SM8 in place, after READY: a fresh account snapshot from every STREAMING adapter."""
        for aid in self.adapter_ids:
            rt = self.orchestrator.runtime(aid)
            if rt.conn.state is not ConnState.STREAMING:
                continue
            for acct in list(rt.accounts):
                try:
                    await self.orchestrator.reread_entity(aid, "ACCOUNT", acct)
                except Exception as exc:
                    log.warning("%s: account snapshot %s failed: %r", aid, acct, exc)

    # --- calendar hooks (synchronous; I/O goes to background tasks) ---------------------------------------
    def _on_transition(self, t: SessionTransition) -> None:
        if t.old is t.new:
            return
        self._background_task(self.committer.force_clean_marks(t.adapter_id, [t.symbol]),
                              f"clean mark {t.adapter_id}:{t.symbol}")
        # operations.md §10 "At the open": every new market open the gate measures reconciliation against
        # (`latest_open_ms`, oversee-rules.md §8) asks for the REST-only RESYNCING at open + 30-60 s: a scheduled
        # entry into OPEN (also from OPEN_BY_EVIDENCE: the scheduled open then becomes the latest open) and an
        # entry into OPEN_BY_EVIDENCE (its entry time is the latest open). Not the LEADER_INIT / reload
        # initialisation. Several symbols opening together schedule one pass (the orchestrator dedupes while one
        # is pending).
        opened = ((t.new is SessionState.OPEN and t.old is not SessionState.OPEN)
                  or (t.new is SessionState.OPEN_BY_EVIDENCE and t.old not in OPEN_STATES))
        if opened and not t.reason.startswith("init:") and t.adapter_id in self._by_id:
            try:
                open_ms = self.calendar.latest_open_ms(t.adapter_id, t.at_ms)
                self.orchestrator.market_open_resync(t.adapter_id,
                                                     open_ms=open_ms if open_ms is not None else t.at_ms)
            except Exception:
                log.exception("%s: open resync not scheduled", t.adapter_id)

    def _on_roll(self, roll_ms: int) -> None:
        for aid in self.adapter_ids:
            self._background_task(self.committer.force_clean_marks(aid), f"roll marks {aid}")
        if self.schedule_refresher is not None:
            self.schedule_refresher.request_all()       # re-fetch the broker schedules after each roll

    def _on_readiness(self, adapter_id: str, reason: str) -> None:
        if adapter_id in self._by_id:
            self.readiness.request(adapter_id, reason)

    def _on_calendar_flag(self, adapter_id: str, symbol: str, flag: str) -> None:
        self.page("high", f"calendar:{flag}", f"{adapter_id}:{symbol}")

    def _on_maintenance(self, adapter_id: str, active: bool, until_ms: int | None) -> None:
        # Logged only: the window's single pre-acknowledged page is `_on_maintenance_notice` (operations.md §10).
        log.warning("%s: BROKER_MAINTENANCE %s%s", adapter_id, "begins" if active else "ends",
                    f" (until {until_ms})" if active else "")

    def _on_maintenance_notice(self, adapter_id: str, until_ms: int, reason: str) -> None:
        self.page("info", f"maintenance: {adapter_id}", f"until {_iso_utc(until_ms)}: {reason}")

    async def refresh_maintenance(self) -> bool:
        """Re-read the declared maintenance windows and hand them to the calendar (idempotent). An unreadable
        source keeps the last good windows and pages once per error streak (high). True on a good read."""
        source = self.ctx.maintenance
        if source is None:
            return True
        try:
            windows = tuple(await asyncio.wait_for(source.load(), self._t.maintenance_load_timeout_s))
        except Exception as exc:
            if is_fence_loss(exc):
                raise
            detail = (f"no answer within {self._t.maintenance_load_timeout_s:g} s"
                      if isinstance(exc, TimeoutError) else redact_urls(f"{type(exc).__name__}: {exc}"))
            if self._maintenance_error != detail:
                self._maintenance_error = detail
                self.page("high", "maintenance windows unreadable",
                          f"{detail}; keeping the {len(self._maintenance_windows)} windows read before")
            return False
        if self._maintenance_error is not None:
            log.warning("maintenance windows readable again")
            self._maintenance_error = None
        if windows != self._maintenance_windows:
            log.warning("declared maintenance windows: %s", ", ".join(
                f"{w.adapter_id} {_iso_utc(w.start_ms)}..{_iso_utc(w.end_ms)}" for w in windows) or "none")
            self._maintenance_windows = windows
        self.calendar.replace_declared_maintenance(windows)
        return True

    # --- telemetry (src/runtime/telemetry.py reads it at scrape time; in-memory only, no I/O) -------------
    def metrics_samples(self, *, leader: bool) -> list[Sample]:
        """This term's series of deploy/observability/metrics.md. `leader` = SM9 is LEADER: the leader-only
        series are exported only then; the spool ring depth in any role."""
        out: list[Sample] = gauge("xtnl_dlq_spool_ring_depth", len(self.spooler))
        if not leader:
            return out
        now = self._clock()
        for topic, depth in self.bus.depths().items():
            out += gauge("xtnl_bus_queue_depth", depth, topic=topic)
        out += self.orchestrator.ingested.samples()
        for aid in self.adapter_ids:
            out += gauge("xtnl_adapter_state", 1, adapter=aid, state=self.orchestrator.runtime(aid).conn.state.value)
            out += adapter_metric_samples(aid, self.orchestrator.adapter(aid), now)
        out += gauge("xtnl_risksweep_pass_duration_seconds", self.sweep.last_full_pass_s)
        passes = self.risksweep_pass_ms()
        if passes:
            out += gauge("xtnl_risksweep_oldest_pass_age_seconds", max(0, now - min(passes.values())) / 1000.0)
        # 7.4: lost frames / skipped broker rows per stream (summed over adapters; cumulative for this term)
        for stream, n in sorted(self.orchestrator.frames_lost.items()):
            out.append(Sample("xtnl_frames_lost_total", float(n), {"stream": stream}))
        return out

    def risksweep_pass_ms(self) -> dict[str, int]:
        """account_id -> the end of its last complete RiskSweep pass, over every account the sweep evaluates (the
        last account list), for the dead-man `oldest_risksweep_pass_age_ms` and the metric (IMPLEMENTATION_PLAN 7.4).
        An account that has not passed in this term counts from when the sweep first listed it, so an account
        failing from the start of a term (or newly listed and failing) ages like one whose passes stopped. Before
        the sweep has listed the accounts once in this term, the age runs from the sweep's start (or the term's)
        under the key UNLISTED_ACCOUNTS (fail closed: never an empty map, which the receiver reads as no account).
        Empty only when the last list was empty."""
        reader = self.reader
        if not reader.listed:
            start = self._sweep_started_ms if self._sweep_started_ms is not None else self._term_started_ms
            return {UNLISTED_ACCOUNTS: start}
        out: dict[str, int] = {}
        for acct in reader.accounts:
            view = self.sweep.account_view(acct)
            if view is not None:
                out[acct] = view.last_pass_ms
            else:
                out[acct] = reader.first_listed_ms.get(acct, self._term_started_ms)
        return out

    # --- pages ------------------------------------------------------------------------------------------
    def page(self, severity: str, title: str, body: str) -> None:
        """Operational page: logged, mirrored to the context's sink, and queued in notification_outbox
        (leader only; the relay delivers it)."""
        sev = _severity(severity)
        level = logging.CRITICAL if sev == "critical" else logging.ERROR if sev == "high" else logging.INFO
        log.log(level, "PAGE[%s] %s: %s", sev, title, body)
        if self.ctx.page_sink is not None:
            with contextlib.suppress(Exception):
                self.ctx.page_sink(sev, title, body)
        if self._closed or not self._lease_valid():
            return
        sev_out: Severity = sev
        row = OutboxRow(kind="info", severity=sev_out, title=title[:200], body=body[:2000],
                        created_ms=self._clock())
        self._background_task(self.ports.outbox.add([row]), "page outbox")

    def _oversee_page(self, p: Page) -> None:
        self.page(p.severity, f"oversee:{p.code}", f"account={p.account_id} {p.detail}".strip())

    def _integrity_page(self, alert: IntegrityAlert) -> None:
        self.page(alert.severity, f"integrity:{alert.code}", alert.detail)


class _ReadinessActions:
    """PRE_OPEN readiness (operations.md §10) over the orchestrator."""

    def __init__(self, s: EngineSession) -> None:
        self._s = s

    async def reset_backoff_and_reconnect(self, adapter_id: str) -> None:
        # SM4's back-off reset hook: a BACKOFF wait ends now and the adapter reconnects. An AUTH_FAILED
        # adapter is re-armed only by a changed credential generation or an operator, never here.
        cut = self._s.orchestrator.reset_backoff(adapter_id)
        state = self._s.orchestrator.runtime(adapter_id).conn.state
        if cut:
            log.warning("readiness: %s back-off reset; reconnecting now", adapter_id)
        elif state is not ConnState.STREAMING:
            log.warning("readiness: %s is %s", adapter_id, state.value)

    async def refresh_account_snapshot(self, adapter_id: str) -> None:
        rt = self._s.orchestrator.runtime(adapter_id)
        if rt.conn.state is not ConnState.STREAMING:
            raise RuntimeError(f"{adapter_id} is not STREAMING")
        for acct in list(rt.accounts):
            await self._s.orchestrator.reread_entity(adapter_id, "ACCOUNT", acct)

    async def fetch_cash_flows(self, adapter_id: str) -> None:
        rt = self._s.orchestrator.runtime(adapter_id)
        for acct in list(rt.accounts):
            await self._s.orchestrator.request_cash_flows(acct)

    def token_valid_for_s(self, adapter_id: str) -> float | None:
        """operations.md §10: tokens must be valid for more than 24 h at PRE_OPEN (None: no expiry known)."""
        exp = token_expires_at_ms(self._s.orchestrator.adapter(adapter_id))
        return None if exp is None else (exp - self._s.ctx.clock_ms()) / 1000.0

    def chrony_synced(self) -> bool:
        return self._s.ctx.chrony_synced()

    def adapter_streaming(self, adapter_id: str) -> bool:
        return self._s.orchestrator.runtime(adapter_id).conn.state is ConnState.STREAMING

    def page(self, severity: str, title: str, body: str) -> None:
        self._s.page(severity, title, body)


class _DeadmanView:
    """DeadmanSource (src/runtime/deadman.py) over the session. Missing values are None (JSON null)."""

    def __init__(self, s: EngineSession) -> None:
        self._s = s

    def node_id(self) -> str:
        return self._s.node_id

    def epoch_label(self) -> str | None:
        return self._s.epoch.label

    def gate_state(self) -> str | None:
        s = self._s
        if not s.sm8.ready or not s.ctx.lease.lease_valid():
            return "CLOSED"
        return "OPEN"

    def halt_level(self) -> str | None:
        s = self._s
        if s.halt_level_name is not None:
            return s.halt_level_name
        return None if s.halt_mirror.level is None else s.halt_mirror.level.name

    def adapter_ids(self) -> Sequence[str]:
        return self._s.adapter_ids

    def sm4_state(self, adapter_id: str) -> str | None:
        return self._s.orchestrator.runtime(adapter_id).conn.state.value

    def market_open(self, adapter_id: str) -> bool | None:
        cal = self._s.calendar
        if cal.version is None or adapter_id not in cal.adapters():
            return None
        return cal.market_open(adapter_id)

    def maintenance_until_ms(self, adapter_id: str) -> int | None:
        return self._s.calendar.maintenance_until(adapter_id, self._s.ctx.clock_ms())

    def calendar_version(self) -> int | None:
        return self._s.calendar.version

    def last_committed_broker_event_ms(self) -> int | None:
        return self._s.tracking.last_commit_ms

    def oldest_unacked_feed_ms(self) -> int | None:
        return self._s.oldest_unacked_feed_ms

    def risksweep_pass_ms(self) -> Mapping[str, int]:
        return self._s.risksweep_pass_ms()

    def oldest_undelivered_notification_ms(self) -> int | None:
        relay = self._s.relay
        return None if relay is None else relay.oldest_undelivered_ms()

    def sm8_state(self) -> str | None:
        return self._s.sm8.state.value

    def sm5_state(self) -> str | None:
        """UNKNOWN unless SM8 is READY and halt:state is current (present, event id = MAX(halt_events.id)), the
        same test the gate applies (oversee-rules.md §8, SM5 edge hx), so Melbourne's 'SM5 UNKNOWN > 60 s'
        fires whenever the gate is shut for want of a current halt:state."""
        s = self._s
        if not s.sm8.ready or not s.halt_state_current:
            return "UNKNOWN"
        return self.halt_level()

    def spill_bytes(self) -> int | None:
        return self._s.spooler.spill_bytes

    def degraded(self) -> Sequence[str]:
        sup = self._s._sup
        return [] if sup is None else sup.degraded()
