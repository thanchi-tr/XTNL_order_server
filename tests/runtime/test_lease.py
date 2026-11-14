"""LeaseKeeper timing with a fake clock and a fake etcd (operations.md §2, §3, §4 deadlines)."""
from __future__ import annotations

import time

import pytest

from src.runtime.etcd import LEADER_KEY
from src.runtime.lease import LeaseKeeper, LeaseTimings
from tests.runtime.fakes import Exits, FakeClock, FakeEtcd


def keeper(clock: FakeClock | None = None) -> tuple[LeaseKeeper, FakeEtcd, Exits, FakeClock]:
    clk = clock or FakeClock(1000.0)
    etcd = FakeEtcd(clk)
    exits = Exits()
    return LeaseKeeper(etcd, clock=clk, fence=exits), etcd, exits, clk


def run_until(k: LeaseKeeper, clk: FakeClock, until: float, step: float = 0.1) -> None:
    while clk.t < until:
        clk.t = min(until, clk.t + step)
        k.tick()


def test_ordering_and_budget_are_validated() -> None:
    t = LeaseTimings()
    assert (t.watchdog_s, t.fence_after_s, t.ttl_s, t.margin) == (3.0, 12.0, 15, 3.0)
    with pytest.raises(ValueError):
        LeaseTimings(fence_after_s=15.0)            # fence must be < TTL
    with pytest.raises(ValueError):
        LeaseTimings(watchdog_s=12.0)               # watchdog must be < fence
    with pytest.raises(ValueError):
        LeaseTimings(renew_interval_s=3.0)          # renew must be < watchdog
    with pytest.raises(ValueError):
        LeaseTimings(fence_after_s=7.5, watchdog_s=2.0)   # 6.5 s budget < election + failover


def test_campaign_wins_and_second_node_loses() -> None:
    k, etcd, _, clk = keeper()
    epoch = k.campaign("n1")
    assert epoch is not None and epoch.cluster_id == "c1" and epoch.revision == etcd.kv[LEADER_KEY].create_revision
    assert k.holds_lease() and k.lease_valid()
    assert k.lease_deadline() == pytest.approx(clk.t + 15 - 3)
    other = LeaseKeeper(etcd, clock=clk, fence=Exits())
    granted_before = len(etcd.leases)
    assert other.campaign("n2") is None
    assert len(etcd.leases) == granted_before            # the loser revoked its lease
    assert other.leader() is not None and other.leader().value == "n1"  # type: ignore[union-attr]


def test_renews_every_second() -> None:
    k, etcd, _, clk = keeper()
    k.campaign("n1")
    run_until(k, clk, clk.t + 5.0, step=0.25)
    assert etcd.calls.count("keepalive") == 5
    assert k.lease_deadline() == pytest.approx(clk.t + 12, abs=0.11)


def test_fence_fires_at_12s_never_before() -> None:
    k, etcd, exits, clk = keeper()
    t0 = clk.t
    k.campaign("n1")
    etcd.down = True
    run_until(k, clk, t0 + 11.9, step=0.05)
    assert exits.reasons == [] and k.fenced is None
    assert k.lease_valid()                                # deadline = t0 + 12
    assert k.lease_valid(min_remaining_s=0.2) is False
    run_until(k, clk, t0 + 12.0, step=0.05)
    assert len(exits.reasons) == 1 and exits.reasons[0].startswith("fence")
    assert not k.lease_valid() and k.lease_deadline() is None
    run_until(k, clk, t0 + 20.0)
    assert len(exits.reasons) == 1                        # fires once
    assert etcd.calls.count("keepalive") > 100            # retried at once, not once per second


def test_recovery_before_the_fence_resets_the_clock() -> None:
    k, etcd, exits, clk = keeper()
    t0 = clk.t
    k.campaign("n1")
    etcd.down = True
    run_until(k, clk, t0 + 11.0)
    etcd.down = False
    run_until(k, clk, t0 + 11.2)
    etcd.down = True
    run_until(k, clk, t0 + 22.9)
    assert exits.reasons == []
    run_until(k, clk, t0 + 23.3)
    assert len(exits.reasons) == 1


def test_lease_reported_gone_fences_immediately() -> None:
    k, etcd, exits, clk = keeper()
    k.campaign("n1")
    etcd.keepalive_ttl = 0
    run_until(k, clk, clk.t + 1.0)
    assert exits.reasons == ["fence: etcd reports the lease expired"]


def test_slow_keepalive_cannot_push_past_the_fence() -> None:
    k, etcd, exits, clk = keeper()
    t0 = clk.t
    k.campaign("n1")
    etcd.down = True
    run_until(k, clk, t0 + 11.5, step=0.25)
    real = etcd.lease_keepalive
    budgets: list[float] = []

    def slow(lease_id: int, *, timeout_s: float) -> int:
        budgets.append(timeout_s)
        assert clk.t + timeout_s <= t0 + 12.0 + 1e-9      # bounded by the time left to the fence
        clk.advance(timeout_s)                            # the call hangs for its whole budget
        return real(lease_id, timeout_s=timeout_s)

    etcd.lease_keepalive = slow  # type: ignore[method-assign]
    while not exits.reasons and clk.t < t0 + 13:
        clk.advance(0.01)
        k.tick()
    assert budgets and len(exits.reasons) == 1
    assert clk.t <= t0 + 12.0 + 0.011


def test_leader_init_deadline_revokes_and_exits() -> None:
    k, etcd, exits, clk = keeper()
    t0 = clk.t
    k.campaign("n1")
    k.begin_leader_init()
    run_until(k, clk, t0 + 119.9, step=0.5)
    assert exits.reasons == []
    run_until(k, clk, t0 + 120.0, step=0.1)
    assert exits.reasons == ["init_failed: deadline passed"]
    assert "revoke" in etcd.calls and LEADER_KEY not in etcd.kv


def test_reached_leader_clears_the_init_deadline() -> None:
    k, _, exits, clk = keeper()
    k.campaign("n1")
    k.begin_leader_init()
    run_until(k, clk, clk.t + 60)
    k.reached_leader()
    assert k.deadline() is None
    run_until(k, clk, clk.t + 100, step=0.5)
    assert exits.reasons == []


def test_draining_deadline_10s() -> None:
    k, etcd, exits, clk = keeper()
    k.campaign("n1")
    k.begin_draining()
    t0 = clk.t
    run_until(k, clk, t0 + 9.9)
    assert exits.reasons == []
    run_until(k, clk, t0 + 10.0)
    assert exits.reasons == ["abdicate: deadline passed"] and LEADER_KEY not in etcd.kv


def test_release_revokes_and_clears() -> None:
    k, etcd, exits, clk = keeper()
    k.campaign("n1")
    k.begin_draining()
    k.release()
    assert LEADER_KEY not in etcd.kv and not k.holds_lease() and k.deadline() is None
    run_until(k, clk, clk.t + 30)
    assert exits.reasons == [] and k.remaining_s() == 0.0


def test_no_campaign_after_fence() -> None:
    k, etcd, exits, clk = keeper()
    k.campaign("n1")
    etcd.keepalive_ttl = 0
    run_until(k, clk, clk.t + 1.0)
    etcd.keepalive_ttl = None
    assert k.campaign("n1") is None


def test_thread_renews_off_the_event_loop() -> None:
    etcd = FakeEtcd(FakeClock())
    etcd.clock = time.monotonic  # type: ignore[assignment]
    exits = Exits()
    k = LeaseKeeper(etcd, timings=LeaseTimings(renew_interval_s=0.02), fence=exits)
    k.start()
    try:
        assert k.campaign("n1") is not None
        deadline = time.monotonic() + 2.0
        while etcd.calls.count("keepalive") < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert etcd.calls.count("keepalive") >= 3 and k.is_alive() and exits.reasons == []
        assert k.renew_latencies
    finally:
        k.stop()
    assert not k.is_alive()


def test_release_during_keepalive_does_not_fence() -> None:
    """release() revokes the lease after tick() captured it but before its keepalive lands: etcd
    answers TTL 0, which is the revoke, not a lost lease, so a clean drain must not fence-exit."""
    k, etcd, exits, clk = keeper()
    k.campaign("n1")
    real = etcd.lease_keepalive

    def racing(lease_id: int, *, timeout_s: float) -> int:
        k.release()                                       # the DRAINING thread wins the race
        return real(lease_id, timeout_s=timeout_s)        # the lease is gone: TTL 0

    etcd.lease_keepalive = racing  # type: ignore[method-assign]
    clk.advance(1.0)
    k.tick()
    assert exits.reasons == [] and k.fenced is None and not k.holds_lease()


def test_release_during_a_failing_keepalive_at_the_fence_edge_does_not_fence() -> None:
    """Skeptic review: the keepalive in flight when release() runs fails and uses the rest of the fence
    budget. The post-keepalive age check fenced on the released lease; a clean drain must end in STANDBY."""
    k, etcd, exits, clk = keeper()
    t0 = clk.t
    k.campaign("n1")
    etcd.down = True
    run_until(k, clk, t0 + 11.0, step=0.05)               # renewals failing; still inside the fence budget
    assert exits.reasons == []

    def racing(lease_id: int, *, timeout_s: float) -> int:
        k.release()                                       # DRAINING's release lands meanwhile
        clk.advance(timeout_s)                            # the attempt burns the remaining budget
        raise ConnectionError("etcd unreachable")

    etcd.lease_keepalive = racing  # type: ignore[method-assign]
    clk.t = max(clk.t, k._next_renew)
    k.tick()
    assert exits.reasons == [] and k.fenced is None and not k.holds_lease()
