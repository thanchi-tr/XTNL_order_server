"""NodeController end to end against fakes: STARTING -> STANDBY -> CANDIDATE -> LEADER_INIT -> LEADER
-> DRAINING -> STANDBY, SAFE_HOLD, campaign loss, init failure, abdication (operations.md §4)."""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from src.db.epoch import Epoch
from src.runtime.etcd import LEADER_KEY
from src.runtime.health import HealthBoard, HealthRecord
from src.runtime.leader_init import LeaderInitDeps
from src.runtime.lease import LeaseKeeper
from src.runtime.node import LEASE_LINK_TASK, NodeController
from src.runtime.roles import BrokerLink, ClusterState, EndReason, NodeRole, TermRow
from src.runtime.supervisor import TaskSupervisor
from tests.runtime.fakes import Exits, FakeClock, FakeEtcd, record
from tests.runtime.initfakes import InitRecorder

R = NodeRole


@dataclass
class Session:
    rec: InitRecorder
    drained: list[str] = field(default_factory=list)
    links: dict[str, BrokerLink] = field(default_factory=dict)

    def init_deps(self, supervisor: TaskSupervisor) -> LeaderInitDeps:
        return self.rec.deps()

    def broker_links(self) -> Mapping[str, BrokerLink]:
        return self.links

    async def drain(self, reason: EndReason) -> None:
        self.drained.append(reason)


@dataclass
class World:
    mono: FakeClock = field(default_factory=lambda: FakeClock(1000.0))
    etcd: FakeEtcd = field(init=False)
    exits: Exits = field(default_factory=Exits)
    cluster: ClusterState = field(default_factory=ClusterState)
    terms: list[TermRow] = field(default_factory=list)
    flushes: int = 0
    probe_kw: dict[str, object] = field(default_factory=dict)
    sessions: list[Session] = field(default_factory=list)
    init: InitRecorder = field(default_factory=InitRecorder)

    def __post_init__(self) -> None:
        self.etcd = FakeEtcd(self.mono)

    def now_ms(self) -> int:
        return int(self.mono() * 1000)

    # protocols
    async def probe(self, now_ms: int) -> HealthRecord:
        return record("n1", now_ms, **self.probe_kw)

    async def own_terms(self, node_id: str) -> Sequence[TermRow]:
        return self.terms

    async def read(self) -> ClusterState:
        return self.cluster

    async def flushdb(self) -> None:
        self.flushes += 1

    def factory(self, epoch: Epoch) -> Session:
        s = Session(self.init)
        self.sessions.append(s)
        return s

    def controller(self) -> NodeController:
        keeper = LeaseKeeper(self.etcd, clock=self.mono, fence=self.exits)
        return NodeController("n1", keeper=keeper, board=HealthBoard(self.etcd, "n1"), probe=self,
                              terms=self, cluster=self, local_redis=self, session_factory=self.factory,
                              exit_fn=self.exits, clock_ms=self.now_ms, monotonic=self.mono,
                              step_interval_s=0.001, require_keeper_thread=False)


def roles(c: NodeController) -> list[NodeRole]:
    return [h[1] for h in c.machine.history]


async def test_full_cycle_leader_then_sigterm_drain() -> None:
    w = World()
    c = w.controller()
    await c.startup()
    assert c.role is R.STANDBY and w.flushes == 1
    await c.step()
    assert c.role is R.LEADER and c.is_leader()
    assert roles(c) == [R.STANDBY, R.CANDIDATE, R.LEADER_INIT, R.LEADER]
    assert w.init.log[-1] == "reached_leader"
    assert w.etcd.kv[LEADER_KEY].value == "n1" and c.keeper.deadline() is None
    assert c.supervisor is not None
    await asyncio.sleep(0.01)
    assert c.supervisor.running(LEASE_LINK_TASK)
    c.request_drain("switchover", exit_after=True)
    await c.step()
    assert roles(c)[-2:] == [R.DRAINING, R.STANDBY]
    assert w.sessions[0].drained == ["switchover"]
    assert LEADER_KEY not in w.etcd.kv and not c.keeper.holds_lease()
    assert c.machine.in_backoff(w.now_ms() + 1000, any_fresh_peer=False)     # 60 s, not waivable
    assert w.flushes == 2 and w.exits.reasons == []
    assert c.supervisor is not None and not c.supervisor.running(LEASE_LINK_TASK)


async def test_health_record_is_published_with_role_and_backoff() -> None:
    w = World(terms=[TermRow("c1", 5, "n1", 0)])        # open term: killed -> back-off
    peer = HealthBoard(w.etcd, "n2")
    peer.publish(record("n2", w.now_ms()))
    c = w.controller()
    await c.startup()
    await c.step()
    assert c.role is R.STANDBY                           # in back-off with a fresh peer
    rec = c.last_record
    assert rec is not None and rec.role == "STANDBY" and rec.backoff_until_ms == w.now_ms() + 60_000
    assert [p.node for p in peer.peers()] == ["n1"]


async def test_peer_holding_the_lease_keeps_us_standby() -> None:
    w = World()
    other = LeaseKeeper(w.etcd, clock=w.mono, fence=Exits())
    assert other.campaign("n2") is not None
    c = w.controller()
    await c.startup()
    await c.step()
    assert c.role is R.STANDBY and R.CANDIDATE not in roles(c)
    assert c.last_view is not None and c.last_view.lease_holder == "n2"


async def test_campaign_lost_race_returns_to_standby() -> None:
    w = World()
    c = w.controller()
    await c.startup()
    real = w.etcd.create_if_absent

    def race(key: str, value: str, *, lease_id: int, timeout_s: float):  # type: ignore[no-untyped-def]
        w.etcd.create_if_absent = real  # type: ignore[method-assign]
        real(key, "n2", lease_id=999, timeout_s=timeout_s)            # the peer wins just before us
        return real(key, value, lease_id=lease_id, timeout_s=timeout_s)

    w.etcd.create_if_absent = race  # type: ignore[method-assign]
    await c.step()
    assert roles(c)[-2:] == [R.CANDIDATE, R.STANDBY]


async def test_safe_hold_blocks_and_resume_returns_to_standby() -> None:
    w = World()
    c = w.controller()
    await c.startup()
    w.cluster = ClusterState(safe_hold=True, safe_hold_reason="flap")
    await c.step()
    assert c.role is R.SAFE_HOLD
    await c.step()
    assert c.role is R.SAFE_HOLD and w.etcd.kv.get(LEADER_KEY) is None
    w.cluster = ClusterState(last_resume_at=w.now_ms())
    await c.step()
    assert c.role is R.STANDBY


async def test_leader_init_failure_is_a_hard_exit() -> None:
    w = World(init=InitRecorder(fail_at="sod"))
    c = w.controller()
    await c.startup()
    await c.step()
    assert w.exits.reasons and w.exits.reasons[0].startswith("init_failed")
    assert c.role is R.FENCED


async def test_safe_hold_due_at_leader_init_releases_the_lease() -> None:
    w = World(init=InitRecorder(safe_hold=True))
    c = w.controller()
    await c.startup()
    await c.step()
    assert c.role is R.SAFE_HOLD and LEADER_KEY not in w.etcd.kv and w.exits.reasons == []


async def test_hard_check_abdication_drains_to_peer() -> None:
    w = World()
    c = w.controller()
    await c.startup()
    await c.step()
    assert c.role is R.LEADER
    peer = HealthBoard(w.etcd, "n2")
    w.probe_kw = {"redis_writable": False}
    for i in range(4):                     # probes at +10, +20, +30, +40 s: >= 2 probes over >= 30 s
        for _ in range(10):
            w.mono.advance(1)
            c.keeper.tick()                # keeps renewing: no fence
        peer.publish(record("n2", w.now_ms()))
        await c.step()
        if i < 3:
            assert c.role is R.LEADER
    assert w.exits.reasons == []
    assert roles(c)[-2:] == [R.DRAINING, R.STANDBY]
    assert w.sessions[0].drained == ["abdicate"] and LEADER_KEY not in w.etcd.kv


async def test_etcd_read_failure_never_treats_lease_as_free() -> None:
    w = World()
    c = w.controller()
    await c.startup()
    w.etcd.down = True
    await c.step()
    assert c.role is R.STANDBY and c.last_view is not None and c.last_view.lease_holder == "?"
    assert c.last_record is not None and not c.last_record.etcd_reachable


async def test_run_loop_until_sigterm() -> None:
    w = World()
    c = w.controller()
    task = asyncio.create_task(c.run())
    for _ in range(200):
        if c.role is R.LEADER:
            break
        await asyncio.sleep(0.001)
    assert c.role is R.LEADER
    c.request_drain()
    await asyncio.wait_for(task, 2)
    assert c.role is R.STANDBY and w.sessions[0].drained == ["switchover"]


async def test_non_leader_sigterm_just_stops() -> None:
    w = World(cluster=ClusterState(safe_hold=True))
    c = w.controller()
    task = asyncio.create_task(c.run())
    await asyncio.sleep(0.01)
    c.request_drain()
    await asyncio.wait_for(task, 1)
    assert c.role is R.SAFE_HOLD and w.sessions == []


async def test_probe_failure_fails_every_hard_check() -> None:
    w = World()

    async def broken(now_ms: int) -> HealthRecord:
        raise ConnectionError("probe down")

    c = w.controller()
    c._probe.probe = broken  # type: ignore[method-assign]
    await c.startup()
    await c.step()
    assert c.role is R.STANDBY and c.last_record is not None
    assert not c.last_record.passes_hard_checks(w.now_ms())


async def test_safe_hold_at_leader_init_revokes_the_lease_off_the_event_loop() -> None:
    """safe-hold-release-blocks-loop / blocking-lease-release-on-loop: the blocking etcd revoke runs in a
    worker thread (as in DRAINING), never on the event loop."""
    import threading

    w = World(init=InitRecorder(safe_hold=True))
    c = w.controller()
    threads: list[str] = []
    real = w.etcd.lease_revoke

    def revoke(lease_id: int, *, timeout_s: float) -> None:
        threads.append(threading.current_thread().name)
        real(lease_id, timeout_s=timeout_s)

    w.etcd.lease_revoke = revoke  # type: ignore[method-assign]
    await c.startup()
    await c.step()
    assert c.role is R.SAFE_HOLD and LEADER_KEY not in w.etcd.kv
    assert threads and threading.main_thread().name not in threads


async def test_startup_retries_while_postgres_is_down_instead_of_crashing() -> None:
    """startup-crash-loop-on-pg-outage: own_terms() raising at STARTING keeps the node in STARTING and
    retries with backoff; once PostgreSQL answers, the node reaches STANDBY. Before: the exception left
    run() and the process exited (systemd start-limit crash loop)."""
    w = World()
    calls = {"n": 0}

    async def flaky(node_id: str) -> Sequence[TermRow]:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("connection refused")
        return w.terms

    w.own_terms = flaky  # type: ignore[method-assign]
    keeper = LeaseKeeper(w.etcd, clock=w.mono, fence=w.exits)
    c = NodeController("n1", keeper=keeper, board=HealthBoard(w.etcd, "n1"), probe=w, terms=w, cluster=w,
                       local_redis=w, session_factory=w.factory, exit_fn=w.exits, clock_ms=w.now_ms,
                       monotonic=w.mono, require_keeper_thread=False, startup_retry_s=0.001)
    await asyncio.wait_for(c.startup(), 2)
    assert calls["n"] == 3 and c.role is R.STANDBY and w.exits.reasons == []


async def test_stop_during_startup_retry_ends_cleanly() -> None:
    w = World()

    async def down(node_id: str) -> Sequence[TermRow]:
        raise ConnectionError("down")

    w.own_terms = down  # type: ignore[method-assign]
    c = w.controller()
    task = asyncio.create_task(c.run())
    await asyncio.sleep(0.05)
    c.request_drain()
    await asyncio.wait_for(task, 2)
    assert c.role is R.STARTING and w.sessions == []


class StandbyWorkSpy:
    def __init__(self, c: list[NodeController]) -> None:
        self.c = c
        self.events: list[str] = []
        self.starts: list[tuple[float, int | None]] = []
        self.busy: list[Callable[[], bool] | None] = []
        self.running = False

    def start(self, *, delay_s: float = 0.0, torn_baseline: int | None = None,
              busy: Callable[[], bool] | None = None) -> None:
        self.events.append(f"start:{self.c[0].role.value}")
        self.starts.append((delay_s, torn_baseline))
        self.busy.append(busy)
        self.running = True

    async def stop(self) -> None:
        self.events.append(f"stop:{self.c[0].role.value}")
        self.running = False


async def test_standby_spill_drain_runs_only_outside_a_term_and_resumes_after_draining() -> None:
    """standby-never-redrains-spill: the node-level spill drain starts on STANDBY entry, is stopped before
    the session is built, and starts again after DRAINING, so a spill written while draining is delivered
    without a process restart."""
    w = World()
    holder: list[NodeController] = []
    spy = StandbyWorkSpy(holder)
    keeper = LeaseKeeper(w.etcd, clock=w.mono, fence=w.exits)

    def factory(epoch: Epoch) -> Session:
        assert not spy.running                         # never alongside the term's own spooler
        return w.factory(epoch)

    c = NodeController("n1", keeper=keeper, board=HealthBoard(w.etcd, "n1"), probe=w, terms=w, cluster=w,
                       local_redis=w, session_factory=factory, exit_fn=w.exits, clock_ms=w.now_ms,
                       monotonic=w.mono, require_keeper_thread=False, standby_work=spy)
    holder.append(c)
    await c.startup()
    assert spy.events == ["start:STANDBY"] and spy.running
    await c.step()
    assert c.role is R.LEADER and spy.events[-1] == "stop:LEADER_INIT" and not spy.running
    c.request_drain("abdicate", exit_after=False)
    await c.step()
    assert c.role is R.STANDBY and spy.events[-1] == "start:STANDBY" and spy.running
