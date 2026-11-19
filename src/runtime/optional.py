"""`optional-bootstrap`: the body of the one task `EngineSession.start_optional_services` adds (design §A2, §B3, §B14,
§C2; plan S31, RT-M11, RT-M13, IN-10, R11-M20).

The task is non-critical and stamps within 60 s; nothing it does can end the term, stop ingestion or delay Oversee.
`make_optional_bootstrap(deps)` is the `SessionContext.optional_bootstrap` factory `build_engine` wires; the session
calls it only when `XTNL_MD_ENABLED=1`. One `OptionalBootstrapRunner` per term keeps its progress across restarts of
the task, so every step below runs once per term and a restart resumes where the last attempt stopped.

1. **Settings** (`parse_optional_settings`, RT-M13): the full parse with bounds; a bad value pages once and **parks**
   the optional services for the term (the task stays alive, stamping, doing nothing).
2. **Lockout scan** (`src.riskmgr.lockout.scan` of the real tree, in `asyncio.to_thread` under `exempt_wait`): a
   violation parks with a page; an incomplete scan (an unreadable file) raises, so the supervisor restarts the task
   with its backoff, and the third incomplete scan in a row pages.
3. **Market data**: the `redis-md` clients (`md_writer`; a 3 s first contact, whose failure only logs: the writer
   keeps retrying and the series read GAPPED, design §C3), the graph (`src/runtime/optional_md.py`), the late-bound
   tick hook with its seeded intervals and drop totals (S14), the per-market interval hook, and the clients' closer
   (closed in parallel in `aclose`, design §A2 DRAINING). The `XTNL_TICK_SYMBOLS` instrument check pages unknown
   symbols by name, once per adapter (PR5); an adapter whose instrument map is not loaded yet keeps its symbols and
   is checked again from the park loop. The kept symbols go to `IngestionOrchestrator.set_md_tick_symbols` and to
   the session's `md_schedule_symbols` (their schedules are fetched; S32), and never decide SM10, the gate or
   Oversee (`MarketCalendar(ignored=...)`). This node's §A7 record is keyed by the raw `node_id` and carries the
   writer-token form of it. The session closing during any await drops what was built (`register_closer` False:
   the caller closes it).
4. **ARM** (only with `XTNL_ARM_ENABLED=1`): every go-live gate wired or accepted (`src.riskmgr.golive`, plan S38e:
   else idle for the term with one page naming the open gate ids); both ARM channels (else idle with one page naming
   the missing one);
   the ARM pool (a copy of the term pool's `LazyPgSource`, `min_size` 0, connect timeout 3 s, wrapped in
   `GuardedEngineDb`, `application_name` `xtnl:<rev>:<node>:arm` cut to 63 characters so the successor's terminate
   reaches it, RT-M11; `SET ROLE xtnl_arm` in its connection init), retried 1 s doubling to 30 s within the term; the
   role's privilege self-check (a difference or unreadable grants page; never a value; never blocks);
   `ensure_months` for the current and next month of both partitioned ARM tables (engine pool; a failure or a
   missing port pages, never blocks); the FAILED spool replay and the open `arm_failures` read before anything that
   could advise (unreadable = advise nothing, retried); the `ARM_SINKS` assertion of the wired chain (a wrong set is
   a wiring fault: the ARM stays idle for the term with one page, never retried).
5. The plan's **handlers** are registered and its **tasks** added idempotently (`EngineSession.add_optional_task`,
   `TaskSupervisor.has`); the production plan (`src/runtime/optional_plan.py`, S32) builds the specs (`md-writer`,
   `md-bars`, `md-indicators`, `arm-status`, `arm`, `arm-relay`) and the request handlers.
6. A stamping **park loop** under `set_exempt("bootstrap complete")` until the term stops; it also retries a pending
   ARM step.

Logs and pages carry codes, names and counts only: never a DSN, a URL, a key, a price or an account value.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

from src.db.arm_failures_store import PgArmFailuresStore
from src.db.arm_ports import OpenFailure
from src.db.arm_schema import (
    ARM_PARTITIONED,
    ARM_ROLE,
    SQL_ROLE_COLUMN_PRIVILEGES,
    SQL_ROLE_SEQUENCE_PRIVILEGES,
    SQL_ROLE_TABLE_PRIVILEGES,
    privilege_problems,
    SQL_ARM_FAILURES_GUARD,
)
from src.db.arm_status_store import PgArmStatusStore
from src.db.epoch import Epoch, read_epoch
from src.db.fence import EpochMismatch
from src.db.lifecycle import LifecycleReport
from src.db.pg import AsyncpgSource, ConnectionSource, init_connection, pool_kwargs
from src.marketdata.clockstep import RECEIPT_GAP_MS
from src.marketdata.config import MdSettings
from src.marketdata.store import keys as K
from src.marketdata.store.client import MdClients
from src.oversee.risk import stop_at_entry
from src.riskmgr.golive import refusal_text as golive_refusal_text
from src.riskmgr.golive import verdict as golive_verdict
from src.riskmgr.lockout import ScanResult, SinkSetError, assert_sinks, scan
from src.riskmgr.sinks import SinkChain, default_sinks
from src.runtime.md_ports import (
    AdapterHealthPortImpl,
    BrokerBarsPortImpl,
    CalendarPortImpl,
    FailedSpoolPortImpl,
    LeaderCheckPortImpl,
    NodeRecordPortImpl,
    PagerWrapper,
    PgHealthPortImpl,
    StopHandleReadPortImpl,
)
from src.runtime.optional_md import MdInputs, MdServices, build_md_services, completion, seed_tick_hook
from src.runtime.optional_settings import (
    OptionalSettings,
    arm_channel_problems,
    check_tick_symbols,
    parse_optional_settings,
)
from src.runtime.pg_ports import GuardedEngineDb, LazyPgSource
from src.runtime.request_handlers import RequestHandler
from src.runtime.session import OPTIONAL_BOOTSTRAP_TASK, EngineSession
from src.runtime.supervisor import TaskContext, TaskSpec

if TYPE_CHECKING:
    from src.adapters.base import BrokerAdapter
    from src.adapters.ratelimit import RestBudget

log = logging.getLogger(__name__)

BOOTSTRAP_STAMP_S: Final = 60.0
PARK_STAMP_S: Final = 5.0
REDIS_MD_CONNECT_S: Final = 3.0
ARM_POOL_CONNECT_S: Final = 3.0
ARM_POOL_MAX_SIZE: Final = 4
ARM_RETRY_BASE_S: Final = 1.0
ARM_RETRY_MAX_S: Final = 30.0
SCAN_PAGE_AFTER: Final = 3
EXEMPT_COMPLETE: Final = "bootstrap complete"
EXEMPT_PARKED: Final = "optional services parked"
SPOOL_FILE: Final = "arm-failed.spool"
APP_NAME_MAX: Final = 63
SQL_PREVIOUS_TERM: Final = (
    "SELECT max(epoch_revision) FROM leader_terms WHERE cluster_id = $1::text AND epoch_revision < $2::bigint"
)


def arm_application_name(epoch: Epoch, node: str) -> str:
    """`xtnl:<rev>:<node>:arm`, cut to PostgreSQL's 63 characters: the `xtnl:<rev>:` prefix the successor's
    post-bump terminate matches always survives (RT-M11)."""
    return f"xtnl:{epoch.revision}:{node}:arm"[:APP_NAME_MAX]


def _month_start(d: date) -> date:
    return d.replace(day=1)


def current_and_next_month(now_ms: int) -> tuple[date, date]:
    today = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).date()
    first = _month_start(today)
    nxt = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
    return first, nxt


# ---------------------------------------------------------------------------------------------------- ARM pool
class ArmPool(ConnectionSource, Protocol):
    """The ARM pool: a `ConnectionSource` that can be closed (graceful) and terminated (now)."""

    async def close(self) -> None: ...

    def terminate(self) -> None: ...


async def arm_connection_init(raw: Any) -> None:
    """The ARM pool's connection init: the engine's init (keepalives, codecs), then `SET ROLE xtnl_arm` so every
    statement runs with exactly the §B3 grants (design §B3, R11-m)."""
    await init_connection(raw)
    await raw.execute(f"SET ROLE {ARM_ROLE}")


async def create_arm_source(dsn: str, *, application_name: str, statement_timeout_ms: int = 5_000,
                            connect_timeout_s: float = ARM_POOL_CONNECT_S, min_size: int = 0,
                            max_size: int = ARM_POOL_MAX_SIZE) -> AsyncpgSource:
    """The asyncpg pool of the ARM (`create_asyncpg_source` with the `SET ROLE` init)."""
    import importlib

    asyncpg = importlib.import_module("asyncpg")
    kw = pool_kwargs(application_name=application_name, statement_timeout_ms=statement_timeout_ms,
                     connect_timeout_s=connect_timeout_s, min_size=min_size, max_size=max_size)
    kw["init"] = arm_connection_init
    pool = await asyncpg.create_pool(dsn, **kw)
    return AsyncpgSource(pool)


def default_arm_pool(dsn: str) -> Callable[[str], ArmPool]:
    def build(application_name: str) -> ArmPool:
        return LazyPgSource(dsn, application_name=application_name, min_size=0, max_size=ARM_POOL_MAX_SIZE,
                            connect_timeout_s=ARM_POOL_CONNECT_S, factory=create_arm_source)
    return build


@runtime_checkable
class MonthEnsurer(Protocol):
    """The engine pool's partition step for one parent (`PgPartitionMaintenance.ensure_months`)."""

    async def ensure_months(self, parent: str, months: Sequence[date]) -> LifecycleReport: ...


# ---------------------------------------------------------------------------------------------------- deps, plan
@dataclass
class ArmServices:
    pool: ArmPool
    db: GuardedEngineDb
    failures: PgArmFailuresStore
    status: PgArmStatusStore
    spool: FailedSpoolPortImpl
    pg_health: PgHealthPortImpl
    stop_reads: StopHandleReadPortImpl
    sinks: SinkChain
    application_name: str
    open_failures: list[OpenFailure] | None = None
    """The open `arm_failures` episodes read before any advice; None while unreadable (advise nothing)."""
    privilege_problems: tuple[str, ...] = ()

    @property
    def may_advise(self) -> bool:
        return self.open_failures is not None


@dataclass
class OptionalServices:
    """What the bootstrap built for this term (S32's task specs and handlers are made from it)."""

    session: EngineSession
    settings: OptionalSettings
    pager: PagerWrapper
    health: AdapterHealthPortImpl
    calendar: CalendarPortImpl
    bars_port: BrokerBarsPortImpl
    md: MdServices
    arm: ArmServices | None = None
    runtime: object | None = None
    """The plan's per-term state (S32: `src.runtime.optional_plan.OptionalRuntime`); None for an empty plan."""
    stop_read_cost: Callable[[str], int] | None = None
    """PR9's stop-read cost per adapter, bound by the plan once the arm-tracker exists (S36b); it wins over
    `OptionalBootstrapRunner.stop_read_cost`."""


@dataclass(frozen=True)
class OptionalPlan:
    """The tasks and handlers of the built services (S32 fills it; empty here: nothing is driven yet)."""

    md_tasks: Callable[[OptionalServices], Sequence[TaskSpec]] = lambda _s: ()
    md_handlers: Callable[[OptionalServices], Mapping[str, RequestHandler]] = lambda _s: {}
    arm_tasks: Callable[[OptionalServices], Sequence[TaskSpec]] = lambda _s: ()
    arm_handlers: Callable[[OptionalServices], Mapping[str, RequestHandler]] = lambda _s: {}
    arm_sinks: Callable[[OptionalServices], SinkChain] = lambda _s: default_sinks(
        fatal=lambda e: isinstance(e, EpochMismatch))


def _default_md_clients(settings: MdSettings) -> MdClients:
    return MdClients.connect(settings.redis_url)


@dataclass(frozen=True)
class OptionalDeps:
    """The process-wide inputs of the bootstrap (`build_engine` builds them; tests replace each seam)."""

    environ: Mapping[str, str] = field(repr=False)
    repo_root: Path
    spool_dir: Path
    md_clients: Callable[[MdSettings], MdClients] = _default_md_clients
    arm_pool: Callable[[str], ArmPool] | None = None
    control: ConnectionSource | None = None
    scan: Callable[[], ScanResult] | None = None
    plan: OptionalPlan = field(default_factory=OptionalPlan)
    unfiltered: tuple[str, ...] = ()
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    retry_base_s: float = ARM_RETRY_BASE_S
    retry_max_s: float = ARM_RETRY_MAX_S
    park_stamp_s: float = PARK_STAMP_S


class Parked(Exception):
    """The optional services stay off for this term (a page has been sent)."""


class _Stopped(Exception):
    """The session closed while a step awaited: the step drops what it built and returns."""


# ---------------------------------------------------------------------------------------------------- the runner
class OptionalBootstrapRunner:
    """One term's optional bootstrap (see the module docstring). `run(ctx)` is the task body."""

    def __init__(self, session: EngineSession, deps: OptionalDeps) -> None:
        self.session = session
        self.deps = deps
        self.parked_reason: str | None = None
        self.settings: OptionalSettings | None = None
        self.scanned = False
        self.scan_incomplete_streak = 0
        self.services: OptionalServices | None = None
        self.md_started = False
        self.arm_idle_reason: str | None = None
        self.arm_attempts = 0
        self.arm_ready = False
        self.arm_started = False
        self._arm_next_mono = 0.0
        self._arm_backoff = deps.retry_base_s
        self._pool: ArmPool | None = None
        self._paged: set[str] = set()
        self.runs = 0
        self.tick_check_pending = False
        """True while a configured `XTNL_TICK_SYMBOLS` adapter's instrument map is not loaded (checked again)."""
        self.stop_read_cost: Callable[[str], int] | None = None
        """The PR9 stop-read cost per adapter per minute of the armed handles (bound by S32's tracker); with
        `XTNL_ARM_STOP_READS` off the demand book is given 0."""

    # ------------------------------------------------------------------------------------------------ helpers
    def spec(self) -> TaskSpec:
        return TaskSpec(OPTIONAL_BOOTSTRAP_TASK, self.run, max_stamp_age_s=BOOTSTRAP_STAMP_S, critical=False)

    def _page_once(self, key: str, severity: str, title: str, body: str) -> None:
        if key in self._paged:
            return
        self._paged.add(key)
        self.session.page(severity, title, body)

    def _park(self, key: str, title: str, body: str) -> None:
        self.parked_reason = key
        self._page_once(f"park:{key}", "high", title, body)
        log.error("optional services parked for this term: %s", key)
        raise Parked(key)

    @property
    def _arm_wanted(self) -> bool:
        return self.session.ctx.optional_flags.arm_enabled

    # ------------------------------------------------------------------------------------------------ task body
    async def run(self, ctx: TaskContext) -> None:
        self.runs += 1
        ctx.stamp()
        try:
            if self.parked_reason is None:
                await self._bootstrap(ctx)
        except Parked:
            pass
        await self._park_loop(ctx)

    async def _bootstrap(self, ctx: TaskContext) -> None:
        s = self.session
        if s.stopping:
            return
        if self.settings is None:
            parsed = parse_optional_settings(self.deps.environ, s.ctx.optional_flags, s.adapter_ids)
            if isinstance(parsed, list):
                self._park("settings", "optional services settings", "; ".join(parsed)[:1500])
                return
            self.settings = parsed
        ctx.stamp()
        if not self.scanned:
            await self._scan(ctx)
        ctx.stamp()
        if s.stopping:
            return
        if self.services is None:
            previous = await self._previous_term_epoch(ctx)
            if s.stopping:
                return
            svc = self._build_md(self.settings, previous)
            clients = svc.md.clients

            async def close_redis_md() -> None:
                await clients.aclose()
            if not s.register_closer("redis-md", close_redis_md):
                await clients.aclose()                # the session closed meanwhile: drop the clients here
                return
            self.services = svc
            await self._first_contact(ctx, svc.md)
        ctx.stamp()
        if s.stopping:
            return
        if not self.md_started:
            self._start_md(self.services)
        if self._arm_wanted and self.arm_idle_reason is None and self.settings.arm is not None:
            gates = golive_verdict(self.deps.environ, stop_rule_in_force=stop_at_entry())
            if gates.problems:
                self._page_once("golive_setting", "high", "ARM go-live setting", "; ".join(gates.problems)[:1500])
            problems = arm_channel_problems(self.deps.environ)
            if not gates.allowed:
                # S38e: go-live gates open and not accepted: the ARM stays idle for the term (md keeps running)
                self.arm_idle_reason = "golive_gates"
                self._page_once("golive_gates", "high", "ARM not started", golive_refusal_text(gates)[:1500])
            elif problems:
                self.arm_idle_reason = "arm_channels"
                self._page_once("arm_channels", "high", "ARM not started", "; ".join(problems))
            elif self.deps.arm_pool is None:
                self.arm_idle_reason = "arm_pool"
                self._page_once("arm_pool", "high", "ARM not started", "no ARM pool is wired in this build")

    # ------------------------------------------------------------------------------------------------ 2. lockout
    async def _scan(self, ctx: TaskContext) -> None:
        run_scan = self.deps.scan or (lambda: scan([self.deps.repo_root]))
        with ctx.exempt_wait("lockout scan"):
            result = await asyncio.to_thread(run_scan)
        if result.violations:
            detail = "; ".join(str(v) for v in result.violations[:5])
            self._park("lockout", "ARM lockout violation", f"{len(result.violations)} violation(s): {detail}"[:1500])
        if result.incomplete:
            self.scan_incomplete_streak += 1
            if self.scan_incomplete_streak >= SCAN_PAGE_AFTER:
                self._page_once("scan_incomplete", "high", "ARM lockout scan incomplete",
                                f"{len(result.incomplete)} file(s) could not be scanned "
                                f"({self.scan_incomplete_streak} scans in a row); retrying")
            raise RuntimeError("lockout scan incomplete")
        self.scan_incomplete_streak = 0
        self.scanned = True

    # ------------------------------------------------------------------------------------------------ 3. market data
    def _adapter(self, adapter_id: str) -> BrokerAdapter:
        return self.session.orchestrator.runtime(adapter_id).adapter

    def _budget(self, adapter_id: str) -> RestBudget | None:
        return self._adapter(adapter_id).rest_budget

    async def _previous_term_epoch(self, ctx: TaskContext) -> int | None:
        """The newest earlier term's epoch revision in this cluster (§A7 `store_foreign`); None when unreadable."""
        control = self.deps.control
        if control is None:
            return None
        s = self.session
        with ctx.exempt_wait("postgres"):
            try:
                async with asyncio.timeout(5.0):
                    async with control.acquire() as conn:
                        row = await conn.fetchrow(SQL_PREVIOUS_TERM, s.epoch.cluster_id, s.epoch.revision)
            except Exception as exc:
                log.warning("previous term not read (%s)", type(exc).__name__)
                return None
        value = None if row is None else row[0]
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    def _build_md(self, settings: OptionalSettings, previous_term_epoch: int | None) -> OptionalServices:
        s = self.session
        clock = s.ctx.clock_ms
        pager = PagerWrapper(s.page)
        health = AdapterHealthPortImpl(lambda aid: s.streaming_adapters().get(aid, False), self._budget,
                                       clock_ms=clock)
        calendar = CalendarPortImpl(s.calendar.state)
        bars_port = BrokerBarsPortImpl(self._adapter)
        clients = self.deps.md_clients(settings.md)
        token = K.WriterToken(cluster_id=_token_part(s.epoch.cluster_id), epoch_rev=s.epoch.revision,
                              node=_token_part(s.node_id), term_nonce=secrets.token_hex(4))
        control = self.deps.control
        # The record is keyed by the raw node_id (the arm_status row) and carries the token form the md bootstrap
        # writes into its record and sentinel (a hostname with dots or colons differs from it).
        records = None if control is None else NodeRecordPortImpl(PgArmStatusStore(control), s.node_id,
                                                                  record_node=token.node)
        stop_reads_on = settings.arm is not None and settings.arm.stop_reads
        leader = LeaderCheckPortImpl(s.ctx.lease.lease_valid, s.epoch, self._epoch_reader())
        inputs = MdInputs(
            node=token.node, token=token, leader=leader, leader_epoch=lambda: s.epoch.revision, pager=pager,
            bars_port=bars_port, health=health, calendar=calendar, complete=completion(s.complete_request),
            clock_ms=clock, records=records, previous_term_epoch=previous_term_epoch,
            bars_per_min=lambda aid: (b.bars_per_min if (b := self._budget(aid)) is not None else None),
            stop_reads_per_min=self._stop_reads_per_min if stop_reads_on else _no_stop_reads,
            unfiltered=self.deps.unfiltered)
        md = build_md_services(settings.md, clients, inputs)
        return OptionalServices(session=s, settings=settings, pager=pager, health=health, calendar=calendar,
                                bars_port=bars_port, md=md)

    def _stop_reads_per_min(self, adapter_id: str) -> int:
        """With `XTNL_ARM_STOP_READS=1`: the stop-read cost the arm-tracker binds (`OptionalServices.stop_read_cost`,
        S36b; else `stop_read_cost`), 0 until then."""
        svc = self.services
        cost = svc.stop_read_cost if svc is not None and svc.stop_read_cost is not None else self.stop_read_cost
        if cost is None:
            return 0
        try:
            return max(0, int(cost(adapter_id)))
        except Exception:
            return 0

    def _epoch_reader(self) -> Callable[[], Awaitable[Epoch | None]] | None:
        control = self.deps.control
        if control is None:
            return None

        async def read() -> Epoch | None:
            async with control.acquire() as conn:
                return await read_epoch(conn)
        return read

    async def _first_contact(self, ctx: TaskContext, md: MdServices) -> None:
        """One PING inside 3 s: a `redis-md` that is down never blocks the term (the writer retries, series GAPPED)."""
        with ctx.exempt_wait("redis-md"):
            try:
                async with asyncio.timeout(REDIS_MD_CONNECT_S):
                    await md.clients.writer.redis.ping()
            except Exception as exc:
                log.warning("redis-md did not answer at start (%s); market data GAPPED until it does",
                            type(exc).__name__)

    def _start_md(self, svc: OptionalServices) -> None:
        s = self.session
        hook = svc.md.hook
        now = s.ctx.clock_ms()
        orch = s.orchestrator
        # S39b (gate receipt_gap_feed): the receipt-gap watch is on for every adapter, fed by ticks and heartbeats
        hook.watch_receipt_gaps(s.adapter_ids, RECEIPT_GAP_MS)
        seed_tick_hook(hook, s.adapter_ids, orch.price_subscriptions, self._drops, now)
        s.set_md_subscription_hook(hook.on_price_subscription)
        s.set_md_frame_hook(hook.note_frame)
        s.set_md_hook(hook.on_price)
        self._check_tick_symbols(svc)
        for kind, handler in self.deps.plan.md_handlers(svc).items():
            s.register_request_handler(kind, handler)
        for spec in self.deps.plan.md_tasks(svc):
            s.add_optional_task(spec)
        self.md_started = True

    def _instruments(self, adapter_id: str) -> frozenset[str]:
        try:
            return frozenset(self.session.orchestrator.runtime(adapter_id).instruments)
        except Exception:
            return frozenset()

    def _check_tick_symbols(self, svc: OptionalServices) -> None:
        """PR5: keep the configured tick-only symbols the adapter lists and page the others by name, once per
        adapter. An adapter whose instrument map is not loaded yet (SNAPSHOTTING) keeps its symbols and is checked
        again from the park loop until it is."""
        kept, unknown = check_tick_symbols(svc.settings.tick_symbols, self._instruments)
        by_adapter: dict[str, list[str]] = {}
        for adapter_id, sym in unknown:
            by_adapter.setdefault(adapter_id, []).append(sym)
        for adapter_id, syms in sorted(by_adapter.items()):
            names = ", ".join(f"{adapter_id}:{sym}" for sym in syms[:16])
            self._page_once(f"tick_symbols:{adapter_id}", "high", "market data: unknown tick-only symbols",
                            f"{len(syms)} XTNL_TICK_SYMBOLS item(s) are not in the instrument list: {names}")
        svc.md.extras["tick_symbols"] = kept
        self._apply_tick_symbols(kept)
        self.tick_check_pending = any(not self._instruments(a) for a in svc.settings.tick_symbols)

    def _apply_tick_symbols(self, kept: Mapping[str, tuple[str, ...]]) -> None:
        """PR5 / IN-11 (S32): the kept tick-only symbols become the orchestrator's separate tick-only set and get a
        broker schedule (`md_schedule_symbols`); a failure for one adapter is logged and never stops the others."""
        s = self.session
        frozen = {a: tuple(v) for a, v in kept.items()}
        changed = False
        for adapter_id, syms in sorted(frozen.items()):
            try:
                if s.orchestrator.md_tick_symbols(adapter_id) != frozenset(syms):
                    s.orchestrator.set_md_tick_symbols(adapter_id, syms)
                    changed = True
            except Exception as exc:
                log.warning("%s: tick-only symbols not set (%s)", adapter_id, type(exc).__name__)

        def schedule_symbols(adapter_id: str) -> tuple[str, ...]:
            return frozen.get(adapter_id, ())
        s.md_schedule_symbols = schedule_symbols
        refresher = s.schedule_refresher
        if changed and refresher is not None:
            try:
                refresher.request_all()
            except Exception as exc:
                log.warning("schedule refresh not requested (%s)", type(exc).__name__)

    def _drops(self, adapter_id: str) -> Mapping[str, int] | None:
        raw = getattr(self._adapter(adapter_id), "dropped_price_frames", None)
        if not isinstance(raw, Mapping):
            return None
        return {str(k): int(v) for k, v in raw.items() if isinstance(v, int) and not isinstance(v, bool)}

    # ------------------------------------------------------------------------------------------------ 4. ARM
    def _arm_due(self) -> bool:
        return (self._arm_wanted and not self.arm_started and self.arm_idle_reason is None
                and self.services is not None and self.parked_reason is None
                and time.monotonic() >= self._arm_next_mono)

    async def _arm_step(self, ctx: TaskContext) -> None:
        """One attempt at the ARM part; a failure retries 1 s doubling to 30 s within the term."""
        svc = self.services
        assert svc is not None and self.deps.arm_pool is not None and svc.settings.arm is not None
        s = self.session
        if s.stopping:
            return
        self.arm_attempts += 1
        try:
            if svc.arm is None:
                svc.arm = await self._build_arm(ctx, svc)
            arm = svc.arm
            if not self.arm_ready:
                await self._prepare_arm(ctx, svc, arm)
            if arm.open_failures is None:
                raise ConnectionError("open arm_failures unreadable")
            if s.stopping:
                return
            try:
                assert_sinks(arm.sinks.names)
            except SinkSetError:
                # A wiring fault, not a passing one: the ARM stays idle for the term (never retried).
                self.arm_idle_reason = "arm_sinks"
                self._page_once("arm_sinks", "high", "ARM sink set refused",
                                f"the wired ARM sink chain {tuple(arm.sinks.names)} is not the Phase 1 set; "
                                "the ARM stays off for this term")
                log.error("ARM sink set refused: %s", tuple(arm.sinks.names))
                return
            for kind, handler in self.deps.plan.arm_handlers(svc).items():
                s.register_request_handler(kind, handler)
            for spec in self.deps.plan.arm_tasks(svc):
                s.add_optional_task(spec)
            self.arm_started = True
            self._arm_backoff = self.deps.retry_base_s
            log.info("ARM services ready (attempt %d)", self.arm_attempts)
        except EpochMismatch:
            raise
        except _Stopped:
            return
        except Exception as exc:
            wait = self._arm_backoff
            self._arm_backoff = min(self._arm_backoff * 2, self.deps.retry_max_s)
            self._arm_next_mono = time.monotonic() + wait
            log.warning("ARM bootstrap attempt %d failed (%s); retrying in %.0f s", self.arm_attempts,
                        type(exc).__name__, wait)
            if self.arm_attempts == 5:
                self._page_once("arm_retry", "high", "ARM not started yet",
                                f"the ARM bootstrap failed {self.arm_attempts} times ({type(exc).__name__}); retrying")

    async def _build_arm(self, ctx: TaskContext, svc: OptionalServices) -> ArmServices:
        s = self.session
        build = self.deps.arm_pool
        assert build is not None
        name = arm_application_name(s.epoch, s.node_id)
        if self._pool is None:
            if s.stopping:
                raise _Stopped()
            built = build(name)
            if not s.register_closer("arm-pool", built.close, built.terminate):
                built.terminate()                     # the session closed meanwhile: drop the pool here
                raise _Stopped()
            self._pool = built
        pool = self._pool
        with ctx.exempt_wait("postgres"):
            async with asyncio.timeout(ARM_POOL_CONNECT_S + 2.0):
                async with pool.acquire() as conn:
                    await conn.fetchrow("SELECT 1")
        if s.stopping:
            raise _Stopped()
        db = GuardedEngineDb(pool, lambda: s.epoch, self._fence_lost)
        status = PgArmStatusStore(pool)
        spool = FailedSpoolPortImpl(self.deps.spool_dir / SPOOL_FILE, pager=svc.pager.page)
        return ArmServices(
            pool=pool, db=db, failures=PgArmFailuresStore(db), status=status, spool=spool,
            pg_health=PgHealthPortImpl(s.pg_last_fenced_commit_ms, s.sweep_pg_unreachable),
            stop_reads=StopHandleReadPortImpl(s.orchestrator.reread_stop_orders),
            sinks=self.deps.plan.arm_sinks(svc), application_name=name)

    async def _prepare_arm(self, ctx: TaskContext, svc: OptionalServices, arm: ArmServices) -> None:
        s = self.session
        # The role's privilege self-check (design §B3): a difference, or grants that cannot be read, pages once and
        # never blocks the ARM (deviation 10).
        try:
            with ctx.exempt_wait("postgres"):
                async with arm.pool.acquire() as conn:
                    tables = [(r[0], r[1]) for r in await conn.fetch(SQL_ROLE_TABLE_PRIVILEGES, ARM_ROLE)]
                    columns = [(r[0], r[1], r[2]) for r in await conn.fetch(SQL_ROLE_COLUMN_PRIVILEGES, ARM_ROLE)]
                    seqs = [(r[0], r[1]) for r in await conn.fetch(SQL_ROLE_SEQUENCE_PRIVILEGES, ARM_ROLE)]
                    guards = [(r[0], r[1]) for r in await conn.fetch(SQL_ARM_FAILURES_GUARD)]   # 0020 (S38f)
            arm.privilege_problems = tuple(privilege_problems(tables, columns, seqs, guards))
        except EpochMismatch:
            raise
        except Exception as exc:
            log.warning("ARM role grants not read (%s)", type(exc).__name__)
            arm.privilege_problems = ("grants unreadable",)
        if arm.privilege_problems:
            self._page_once("arm_grants", "high", "ARM role grants differ",
                            "; ".join(arm.privilege_problems[:10])[:1500])
        ctx.stamp()
        # ensure_months for the current and next month of both partitioned ARM tables (engine pool, fenced).
        lifecycle = s.lifecycle
        if isinstance(lifecycle, MonthEnsurer):
            months = current_and_next_month(s.ctx.clock_ms())
            for parent in ARM_PARTITIONED:
                try:
                    with ctx.exempt_wait("postgres"):
                        report = await lifecycle.ensure_months(parent, months)
                    failed = bool(report.errors)
                except EpochMismatch:
                    raise
                except Exception as exc:
                    log.warning("ARM partitions of %s not ensured (%s)", parent, type(exc).__name__)
                    failed = True
                if failed:
                    self._page_once(f"ensure:{parent}", "high", "ARM partitions not ensured",
                                    f"{parent}: the current or next month partition could not be created")
        else:
            self._page_once("ensure:none", "high", "ARM partitions not ensured",
                            "no partition maintenance port is wired: the current and next month partitions of "
                            f"{', '.join(ARM_PARTITIONED)} were not checked")
        ctx.stamp()
        # The FAILED spool first, then the open episodes: before anything that could advise (§B14 Record).
        try:
            spooled = await arm.spool.replay()
        except Exception as exc:
            arm.open_failures = None
            self.arm_ready = False
            log.warning("ARM FAILED spool unreadable (%s): the ARM advises nothing", type(exc).__name__)
            return
        svc.md.extras["failed_spooled"] = len(spooled)
        with ctx.exempt_wait("postgres"):
            try:
                arm.open_failures = list(await arm.failures.open_episodes())
            except EpochMismatch:
                raise
            except Exception as exc:
                arm.open_failures = None
                log.warning("open arm_failures unreadable (%s): the ARM advises nothing", type(exc).__name__)
        self.arm_ready = arm.open_failures is not None

    def _fence_lost(self, exc: BaseException) -> None:
        """A lost fence on the ARM pool ends the process like one on the term pool (operations.md §3)."""
        ctx = self.session.ctx
        if ctx.on_fence_lost is not None:
            ctx.on_fence_lost(exc)
        else:
            log.critical("lost fence on the ARM pool: %s", type(exc).__name__)
            ctx.exit_fn(f"fence: {type(exc).__name__}")

    # ------------------------------------------------------------------------------------------------ 6. park
    async def _park_loop(self, ctx: TaskContext) -> None:
        s = self.session
        ctx.set_exempt(EXEMPT_PARKED if self.parked_reason is not None else EXEMPT_COMPLETE)
        while not s.stopping:
            ctx.stamp()
            if self.tick_check_pending and self.services is not None and self.parked_reason is None:
                self._check_tick_symbols(self.services)
            if self._arm_due():
                ctx.set_exempt(None)
                await self._arm_step(ctx)
                ctx.set_exempt(EXEMPT_COMPLETE)
            wait = self.deps.park_stamp_s
            if self._arm_wanted and not self.arm_started and self.arm_idle_reason is None:
                wait = max(0.0, min(wait, self._arm_next_mono - time.monotonic()))
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(s.stop_event.wait(), timeout=wait if wait > 0 else 0.001)


def _no_stop_reads(_adapter_id: str) -> int:
    return 0


def _token_part(text: str) -> str:
    """A writer-token part (`[A-Za-z0-9_-]`): any other character becomes `-`."""
    out = "".join(c if c.isascii() and (c.isalnum() or c in "_-") else "-" for c in text)
    return out[:64] or "x"


def make_optional_bootstrap(deps: OptionalDeps) -> Callable[[EngineSession], TaskSpec]:
    """The `SessionContext.optional_bootstrap` factory: one runner per term."""

    def factory(session: EngineSession) -> TaskSpec:
        return OptionalBootstrapRunner(session, deps).spec()

    return factory


__all__ = [
    "ArmPool", "ArmServices", "MonthEnsurer", "OptionalBootstrapRunner", "OptionalDeps", "OptionalPlan", "OptionalServices",
    "arm_application_name", "arm_connection_init", "create_arm_source", "current_and_next_month",
    "default_arm_pool", "make_optional_bootstrap",
]
