"""NodeController: drives SM9 for this process (operations.md §2-§4).

Once per second: probe and publish this node's health record (own etcd lease), read the peers and
the leader key, read cluster_state, then act on the role:

* STANDBY   -> CANDIDATE when `may_campaign()`; campaign through the LeaseKeeper (txn on /xtnl/leader).
* CANDIDATE -> LEADER_INIT on a win (the keeper enforces the 120 s deadline), else back to STANDBY.
* LEADER    -> DRAINING on an abdication decision or SIGTERM (`request_drain`).
* DRAINING  -> STANDBY (60 s back-off, the engine's Redis database flushed with FLUSHDB); the keeper
               enforces the 10 s deadline. The standby spill drain restarts one spill period later, with
               the term's torn-tail baseline (IMPLEMENTATION_PLAN 4.9).
* STANDBY  <-> SAFE_HOLD while cluster_state.safe_hold is set (operator `resume()` clears it).

Every leader-side component comes from a `LeaderSession` (the coordinator wires adapters, stores,
consumers, Oversee and the dead-man sender); it receives the TaskSupervisor so LEADER_INIT step 6 can
add supervised tasks. The lease-keeper link is a leadership-critical supervised task.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Protocol

from src.db.epoch import Epoch
from src.runtime.exits import ExitFn, hard_exit
from src.runtime.health import HealthProbe, HealthRecord
from src.runtime.leader_init import InitOutcome, LeaderInit, LeaderInitDeps, LocalRedis
from src.runtime.lease import LeaseKeeper
from src.runtime.roles import (
    AbdicationLedger,
    AbdicationMonitor,
    BrokerLink,
    ClusterState,
    ClusterView,
    EndReason,
    MemoryAbdicationLedger,
    NodeRole,
    RoleMachine,
    TermRow,
)
from src.runtime.supervisor import Pager, TaskContext, TaskSpec, TaskSupervisor

log = logging.getLogger(__name__)

RISKSWEEP_TASK = "risksweep"
LEASE_LINK_TASK = "lease-keeper-link"


class HealthBoardPort(Protocol):
    def publish(self, record: HealthRecord) -> None: ...

    def peers(self) -> list[HealthRecord]: ...


class TermStore(Protocol):
    async def own_terms(self, node_id: str) -> Sequence[TermRow]: ...


class ClusterStateReader(Protocol):
    async def read(self) -> ClusterState: ...


class StandbyWork(Protocol):
    """Node-level work that runs only while this node holds no leader term (the spill drain,
    tier-persistence.md §5): started on STANDBY entry, stopped before a session is built.

    `delay_s`: wait before the first attempt (after DRAINING: a spill append the term gave up on may still
    be running in a worker thread). `busy`: while it returns True, the term's DRAINING flush is still running
    (it may still append to the file), so the first attempt waits for it too. `torn_baseline`: the
    `<spill>.torn` size the term's own drain service last saw (already paged), so only a tail moved aside
    after it is paged again."""

    def start(self, *, delay_s: float = 0.0, torn_baseline: int | None = None,
              busy: Callable[[], bool] | None = None) -> None: ...

    async def stop(self) -> None: ...


class LeaderSession(Protocol):
    """Everything the leader runs; built per term by the coordinator's factory."""

    def init_deps(self, supervisor: TaskSupervisor) -> LeaderInitDeps: ...

    def broker_links(self) -> Mapping[str, BrokerLink]: ...

    async def drain(self, reason: EndReason) -> None:
        """Stop intake, flush the DlqSpooler (PostgreSQL, or the spill file after 2 s), write this
        term's end_reason, close sockets. The lease is released by the controller afterwards."""
        ...


LeaderSessionFactory = Callable[[Epoch], LeaderSession]


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


class NodeController:
    def __init__(self, node_id: str, *, keeper: LeaseKeeper, board: HealthBoardPort, probe: HealthProbe,
                 terms: TermStore, cluster: ClusterStateReader, local_redis: LocalRedis,
                 session_factory: LeaderSessionFactory, pager: Pager | None = None,
                 ledger: AbdicationLedger | None = None, exit_fn: ExitFn = hard_exit,
                 clock_ms: Callable[[], int] = _wall_ms, monotonic: Callable[[], float] = time.monotonic,
                 step_interval_s: float = 1.0, require_keeper_thread: bool = True,
                 standby_work: StandbyWork | None = None, startup_retry_s: float = 1.0,
                 startup_retry_max_s: float = 30.0, standby_delay_after_drain_s: float = 0.0) -> None:
        self.node_id = node_id
        self.machine = RoleMachine(node_id)
        self.keeper = keeper
        self._board = board
        self._probe = probe
        self._terms = terms
        self._cluster = cluster
        self._redis = local_redis
        self._factory = session_factory
        self._pager = pager
        self._abdication = AbdicationMonitor(ledger or MemoryAbdicationLedger(),
                                             pager=None if pager is None else pager.page)
        self._exit = exit_fn
        self._clock_ms = clock_ms
        self._mono = monotonic
        self._interval = step_interval_s
        self._require_thread = require_keeper_thread
        self._standby_work = standby_work
        self._startup_retry_s = startup_retry_s
        self._startup_retry_max_s = startup_retry_max_s
        self._standby_delay_after_drain_s = standby_delay_after_drain_s
        self._lease_free_since: int | None = None
        self.session: LeaderSession | None = None
        self.supervisor: TaskSupervisor | None = None
        self._sup_stop: asyncio.Event | None = None
        self._sup_task: asyncio.Task[None] | None = None
        self._drain_request: tuple[EndReason, bool] | None = None
        self._stop = asyncio.Event()
        self.last_record: HealthRecord | None = None
        self.last_view: ClusterView | None = None

    @property
    def role(self) -> NodeRole:
        return self.machine.role

    def is_leader(self) -> bool:
        return self.machine.role is NodeRole.LEADER

    # --- external requests -------------------------------------------------------------------------
    def request_drain(self, reason: EndReason = "switchover", *, exit_after: bool = True) -> None:
        """SIGTERM / planned switchover. A non-leader just stops."""
        if self.machine.role in (NodeRole.LEADER, NodeRole.LEADER_INIT):
            self._drain_request = (reason, exit_after)
        elif exit_after:
            self._stop.set()

    def stop(self) -> None:
        self._stop.set()

    # --- main loop ---------------------------------------------------------------------------------
    async def startup(self) -> None:
        """STARTING: learn the back-off from leader_terms, read cluster_state.

        PostgreSQL may be down when the process (re)starts. The reads are retried with a capped backoff
        and the node stays in STARTING (the LoopWatchdog keeps beating), so a database outage never
        crash-loops the unit into systemd's start limit (operations.md §3, §4). A stop request ends the
        wait without starting."""
        delay = self._startup_retry_s
        failures = 0
        while True:
            try:
                terms = await self._terms.own_terms(self.node_id)
                cluster = await self._cluster.read()
                break
            except Exception as exc:
                failures += 1
                lvl = logging.ERROR if failures == 1 or failures % 10 == 0 else logging.WARNING
                log.log(lvl, "STARTING: leader_terms / cluster_state unreadable (%s, attempt %d); "
                        "retrying in %.0f s", type(exc).__name__, failures, delay)
            try:
                await asyncio.wait_for(self._stop.wait(), delay)
                return                        # stop requested while STARTING
            except TimeoutError:
                pass
            delay = min(delay * 2, self._startup_retry_max_s)
        now = self._clock_ms()
        self.machine.start(terms, cluster, now)
        if self.machine.role is NodeRole.STANDBY:
            await self._flush_local_redis()
        self._start_standby_work()

    async def run(self) -> None:
        await self.startup()
        try:
            await self._loop()
        finally:
            await self._stop_standby_work()

    async def _loop(self) -> None:
        while not self._stop.is_set():
            await self.step()
            if self._stop.is_set():
                break
            try:
                await asyncio.wait_for(self._stop.wait(), self._interval)
            except TimeoutError:
                pass
        if self.machine.role is NodeRole.LEADER:
            await self._drain("switchover")

    async def step(self) -> None:
        if self._drain_request is not None and self.machine.role is NodeRole.LEADER:
            reason, exit_after = self._drain_request
            self._drain_request = None
            await self._drain(reason)
            if exit_after:
                self._stop.set()
            return
        view = await self._observe()
        role = self.machine.role
        if role is NodeRole.STANDBY:
            if view.safe_hold:
                self.machine.go(NodeRole.SAFE_HOLD, "cluster SAFE_HOLD set", view.now_ms)
            elif self.machine.may_campaign(view):
                await self._campaign(view)
        elif role is NodeRole.SAFE_HOLD:
            if not view.safe_hold:
                self.machine.go(NodeRole.STANDBY, "SAFE_HOLD resumed by operator", view.now_ms)
        elif role is NodeRole.LEADER:
            session = self.session
            links = session.broker_links() if session is not None else {}
            decision = self._abdication.observe(view, links)
            if decision is not None:
                log.critical("abdicating (%s) to %s: %s", decision.grounds, decision.target, decision.reason)
                await self._drain("abdicate")

    async def _observe(self) -> ClusterView:
        now = self._clock_ms()
        try:
            base = await self._probe.probe(now)
        except Exception as exc:              # an unknown health state fails every hard check
            log.warning("health probe failed: %r", exc)
            base = HealthRecord(self.node_id, now, self.machine.role.value, pg_writable=False,
                                etcd_reachable=False, redis_writable=False, chrony_offset_ms=None)
        peers: list[HealthRecord] = []
        holder: str | None = None
        etcd_ok = True
        try:
            peers = await asyncio.to_thread(self._board.peers)
            kv = await asyncio.to_thread(self.keeper.leader)
            holder = kv.value if kv is not None else None
        except Exception as exc:
            etcd_ok = False
            holder = "?"                      # unknown: never treat the lease as free
            log.warning("etcd read failed: %r", exc)
        cluster = await self._cluster.read()
        any_fresh = any(p.fresh(now) for p in peers)
        sweep_age = self.supervisor.circuit_open_age_s(RISKSWEEP_TASK) if self.supervisor else None
        record = dataclasses.replace(
            base, node=self.node_id, ts_ms=now, role=self.machine.role.value,
            etcd_reachable=base.etcd_reachable and etcd_ok,
            backoff_until_ms=self.machine.effective_backoff_until(now, any_fresh), safe_hold=cluster.safe_hold,
            risksweep_circuit_open_since_ms=None if sweep_age is None else now - int(sweep_age * 1000))
        self.last_record = record
        try:
            await asyncio.to_thread(self._board.publish, record)
        except Exception as exc:
            log.warning("health publish failed: %r", exc)
        if holder is None:
            if self._lease_free_since is None:
                self._lease_free_since = now
        else:
            self._lease_free_since = None
        view = ClusterView(now, record, peers, holder, self._lease_free_since, cluster.safe_hold)
        self.last_view = view
        return view

    # --- campaign / LEADER_INIT --------------------------------------------------------------------
    async def _campaign(self, view: ClusterView) -> None:
        self.machine.go(NodeRole.CANDIDATE, "may_campaign", view.now_ms)
        try:
            epoch = await asyncio.to_thread(self.keeper.campaign, self.node_id)
        except Exception as exc:
            log.warning("campaign failed: %r", exc)
            epoch = None
        if epoch is None:
            self.machine.go(NodeRole.STANDBY, "campaign lost", self._clock_ms())
            return
        self.keeper.begin_leader_init()
        self.machine.go(NodeRole.LEADER_INIT, f"won lease, epoch {epoch.label}", self._clock_ms())
        await self._stop_standby_work()       # the term's own spooler now owns the spill file
        session = self._factory(epoch)
        self.session = session
        sup = TaskSupervisor([self._lease_link_spec()], exit_fn=self._exit, pager=self._pager,
                             clock=self._mono)
        self.supervisor = sup
        self._sup_stop = asyncio.Event()
        self._sup_task = asyncio.create_task(sup.run(self._sup_stop), name="supervisor")
        report = await LeaderInit(session.init_deps(sup), exit_fn=self._exit,
                                  deadline_s=self.keeper.timings.leader_init_deadline_s,
                                  clock_ms=self._clock_ms).run(epoch, self.node_id)
        if report is None:                    # hard exit already requested (tests: injected exit)
            await self._stop_supervisor()
            self.machine.go(NodeRole.FENCED, "LEADER_INIT failed", self._clock_ms())
            self._stop.set()
            return
        if report.outcome is InitOutcome.SAFE_HOLD:
            await self._stop_supervisor()
            await asyncio.to_thread(self.keeper.release)    # blocking etcd revoke: never on the loop
            self.session = None
            self.machine.go(NodeRole.SAFE_HOLD, "SAFE_HOLD due at LEADER_INIT", self._clock_ms())
            self._start_standby_work()
            return
        self.keeper.reached_leader()
        self.machine.go(NodeRole.LEADER, f"LEADER_INIT done ({', '.join(report.steps)})", self._clock_ms())

    def _lease_link_spec(self) -> TaskSpec:
        keeper, require = self.keeper, self._require_thread

        async def link(ctx: TaskContext) -> None:
            while True:
                if keeper.fenced is not None:
                    raise RuntimeError(f"lease keeper fenced: {keeper.fenced}")
                if require and not keeper.is_alive():
                    raise RuntimeError("lease keeper thread is not running")
                ctx.stamp()
                await asyncio.sleep(0.5)

        return TaskSpec(LEASE_LINK_TASK, link, max_stamp_age_s=3.0, critical=True)

    # --- DRAINING ----------------------------------------------------------------------------------
    async def _drain(self, reason: EndReason) -> None:
        now = self._clock_ms()
        self.machine.go(NodeRole.DRAINING, reason, now)
        self.keeper.begin_draining()
        session = self.session
        if session is not None:
            try:
                await session.drain(reason)
            except Exception:
                log.exception("drain steps failed; releasing the lease anyway")
        await self._stop_supervisor()
        await asyncio.to_thread(self.keeper.release)
        torn = _torn_baseline(session)
        busy = _spill_busy(session)
        self.session = None
        now = self._clock_ms()
        self.machine.set_backoff(now, f"after DRAINING ({reason})")
        self.machine.go(NodeRole.STANDBY, f"drained ({reason})", now)
        await self._flush_local_redis()
        # Deliver whatever DRAINING spilled once PostgreSQL accepts, but only one spill period later (a spill
        # append DRAINING stopped waiting for may still be writing), from the term's torn-tail baseline.
        # The term's DRAINING flush may still be running past its budget (`busy`): the drain waits for it.
        self._start_standby_work(delay_s=self._standby_delay_after_drain_s, torn_baseline=torn, busy=busy)

    def _start_standby_work(self, *, delay_s: float = 0.0, torn_baseline: int | None = None,
                            busy: Callable[[], bool] | None = None) -> None:
        if self._standby_work is None or self.session is not None:
            return
        try:
            if busy is None:
                self._standby_work.start(delay_s=delay_s, torn_baseline=torn_baseline)
            else:
                self._standby_work.start(delay_s=delay_s, torn_baseline=torn_baseline, busy=busy)
        except Exception:
            log.exception("standby work failed to start")

    async def _stop_standby_work(self) -> None:
        if self._standby_work is None:
            return
        try:
            await self._standby_work.stop()
        except Exception:
            log.exception("standby work failed to stop")

    async def _stop_supervisor(self) -> None:
        if self._sup_stop is not None:
            self._sup_stop.set()
        if self._sup_task is not None:
            try:
                await self._sup_task
            except Exception:
                log.exception("supervisor ended with an error")
        self._sup_task = None
        self._sup_stop = None

    async def _flush_local_redis(self) -> None:
        """STANDBY flushes the engine's Redis database on entry (FLUSHDB, never FLUSHALL) so an ex-leader's
        Redis never shows a frozen book."""
        try:
            await self._redis.flushdb()
        except Exception as exc:
            log.warning("STANDBY FLUSHDB failed: %r", exc)


def _spill_busy(session: LeaderSession | None) -> Callable[[], bool] | None:
    """The term's `spill_busy` (EngineSession: a DRAINING flush still running), if it reports one. A probe that
    raises counts as busy (never read the file beside a possible writer)."""
    fn = getattr(session, "spill_busy", None)
    if fn is None:
        return None

    def busy() -> bool:
        try:
            return bool(fn())
        except Exception:
            return True
    return busy


def _torn_baseline(session: LeaderSession | None) -> int | None:
    """The term's last observed `<spill>.torn` size (EngineSession.spill_torn_bytes), if it reports one."""
    fn = getattr(session, "spill_torn_bytes", None)
    if fn is None:
        return None
    try:
        value = fn()
    except Exception:
        return None
    return value if isinstance(value, int) and not isinstance(value, bool) else None
