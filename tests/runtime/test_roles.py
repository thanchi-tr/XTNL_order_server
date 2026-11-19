"""SM9: transitions, back-off learned at STARTING, campaign gating, abdication, SAFE_HOLD (operations.md §4)."""
from __future__ import annotations

import pytest

from src.runtime.roles import (
    BACKOFF_MS,
    AbdicationMonitor,
    BrokerLink,
    ClusterState,
    ClusterView,
    IllegalRoleTransition,
    MemoryAbdicationLedger,
    NodeRole,
    RoleMachine,
    TermRow,
    learn_backoff,
    safe_hold_due,
)
from tests.runtime.fakes import record

NOW = 10_000_000
R = NodeRole


def term(rev: int, *, started: int = NOW - 100_000, ended: int | None = None, reason: str | None = None,
         reached: int | None = None, node: str = "n1") -> TermRow:
    return TermRow("c1", rev, node, started, reached, ended, reason)  # type: ignore[arg-type]


def view(*, own: object | None = None, peers: tuple[object, ...] = (), holder: str | None = None,
         free_since: int | None = NOW, safe_hold: bool = False, now: int = NOW) -> ClusterView:
    return ClusterView(now, own or record("n1", now), list(peers), holder, free_since, safe_hold)  # type: ignore[arg-type]


def standby(backoff_terms: tuple[TermRow, ...] = ()) -> RoleMachine:
    m = RoleMachine("n1")
    m.start(backoff_terms, ClusterState(), NOW)
    return m


# --- transitions ----------------------------------------------------------------------------------
def test_happy_path_and_illegal_transitions() -> None:
    m = standby()
    for role in (R.CANDIDATE, R.LEADER_INIT, R.LEADER, R.DRAINING, R.STANDBY):
        m.go(role, "t", NOW)
    assert [h[1] for h in m.history] == [R.STANDBY, R.CANDIDATE, R.LEADER_INIT, R.LEADER, R.DRAINING, R.STANDBY]
    with pytest.raises(IllegalRoleTransition):
        m.go(R.LEADER, "skip", NOW)
    m.go(R.FENCED, "fence", NOW)
    with pytest.raises(IllegalRoleTransition):
        m.go(R.STANDBY, "no way back", NOW)


def test_start_into_safe_hold() -> None:
    m = RoleMachine("n1")
    m.start((), ClusterState(safe_hold=True, safe_hold_reason="flap"), NOW)
    assert m.role is R.SAFE_HOLD
    m.go(R.STANDBY, "resume", NOW)


# --- back-off -------------------------------------------------------------------------------------
def test_backoff_learned_from_leader_terms() -> None:
    assert learn_backoff([], NOW) is None
    killed = learn_backoff([term(5)], NOW)                       # open term: the process was killed
    assert killed is not None and killed.waivable and killed.until_ms == NOW + BACKOFF_MS
    lost = learn_backoff([term(5, ended=NOW - 59_000, reason="lost")], NOW)
    assert lost is not None and lost.waivable
    assert learn_backoff([term(5, ended=NOW - 61_000, reason="init_failed")], NOW) is None
    drained = learn_backoff([term(5, reason="abdicate")], NOW)     # DRAINING wrote it, no successor yet
    assert drained is not None and not drained.waivable
    assert learn_backoff([term(5, ended=NOW - 70_000, reason="switchover")], NOW) is None
    both = learn_backoff([term(4), term(5, reason="switchover")], NOW)
    assert both is not None and not both.waivable                 # the non-waivable one wins


def test_fence_backoff_waived_without_fresh_peer() -> None:
    m = standby((term(5),))
    assert m.in_backoff(NOW + 1000, any_fresh_peer=True)
    assert not m.in_backoff(NOW + 1000, any_fresh_peer=False)
    assert not m.in_backoff(NOW + BACKOFF_MS, any_fresh_peer=True)
    m.set_backoff(NOW, "after DRAINING")
    assert m.in_backoff(NOW + 1000, any_fresh_peer=False)         # DRAINING back-off is never waived
    assert m.effective_backoff_until(NOW + 1000, False) == NOW + BACKOFF_MS


# --- campaign gating ------------------------------------------------------------------------------
def test_may_campaign_basic_gates() -> None:
    m = standby()
    assert m.may_campaign(view())
    assert not m.may_campaign(view(holder="n2"))
    assert not m.may_campaign(view(safe_hold=True))
    assert not m.may_campaign(view(own=record("n1", NOW, redis_writable=False)))
    m2 = standby((term(5),))
    assert not m2.may_campaign(view(peers=(record("n2", NOW),)))   # back-off, fresh peer present
    assert m2.may_campaign(view(peers=(record("n2", NOW - 6000),)))  # stale peer counts as absent
    m.go(R.CANDIDATE, "t", NOW)
    assert not m.may_campaign(view())                              # only from STANDBY


def test_preference_and_lease_free_for_15s() -> None:
    m = standby()
    better = record("n2", NOW, hosts_patroni_primary=True)
    assert not m.may_campaign(view(peers=(better,)))
    assert not m.may_campaign(view(peers=(better,), free_since=NOW - 14_999))
    assert m.may_campaign(view(peers=(better,), free_since=NOW - 15_000))   # any node passing hard checks
    assert m.may_campaign(view(peers=(record("n2", NOW),)))                  # tie: both may campaign
    ineligible = record("n2", NOW, hosts_patroni_primary=True, pg_writable=False)
    assert m.may_campaign(view(peers=(ineligible,)))                         # only eligible peers count
    backing_off = record("n2", NOW, hosts_patroni_primary=True, backoff_until_ms=NOW + 1)
    assert m.may_campaign(view(peers=(backing_off,)))
    assert m.prefers(view(own=record("n1", NOW, broker_tls={"stonex": True}), peers=(better,)))


# --- SAFE_HOLD ------------------------------------------------------------------------------------
def test_safe_hold_due_on_abnormal_ends() -> None:
    terms = [term(i, ended=NOW - i * 60_000, reason="lost") for i in (1, 2)]
    assert safe_hold_due(terms, NOW, None) is None
    terms.append(term(3, ended=NOW - 14 * 60_000, reason="init_failed"))
    assert safe_hold_due(terms, NOW, None) == "3 abnormal term ends within 15 min"
    assert safe_hold_due(terms, NOW, last_resume_at=NOW - 2 * 60_000) is None   # only after the resume
    old = [term(i, ended=NOW - 16 * 60_000, reason="lost") for i in range(3)]
    assert safe_hold_due(old, NOW, None) is None


def test_stale_terms_closed_at_start_up_are_no_flap() -> None:
    """2026-11-18 single PC: terms left open by kills hours or days ago are all closed `lost` by the next
    LEADER_INIT at the same instant; counting them by that closing time made a false SAFE_HOLD."""
    stale = [term(i, started=NOW - (i + 1) * 3_600_000, ended=NOW, reason="lost") for i in range(4)]
    assert safe_hold_due(stale, NOW, None) is None
    loop = [term(10 + i, started=NOW - (i + 1) * 120_000, ended=NOW - i * 120_000, reason="lost")
            for i in range(3)]          # a real restart loop: each term starts and dies within minutes
    assert safe_hold_due(stale + loop, NOW, None) == "3 abnormal term ends within 15 min"


def test_safe_hold_due_on_epoch_change_rate() -> None:
    six = [term(i, started=NOW - i * 60_000, ended=NOW, reason="switchover") for i in range(6)]
    assert safe_hold_due(six, NOW, None) is None
    seven = six + [term(7, started=NOW - 29 * 60_000)]
    assert safe_hold_due(seven, NOW, None) == "7 epoch changes within 30 min"


# --- abdication -----------------------------------------------------------------------------------
def leader_view(now: int, *, own_kw: dict[str, object] | None = None, peers: tuple[object, ...] = (),
                safe_hold: bool = False) -> ClusterView:
    own = record("n1", now, role="LEADER", **(own_kw or {}))
    return ClusterView(now, own, list(peers), "n1", None, safe_hold)  # type: ignore[arg-type]


def test_hard_check_abdication_needs_two_probes_over_30s_and_a_newer_eligible_peer() -> None:
    mon = AbdicationMonitor(MemoryAbdicationLedger())
    bad = {"redis_writable": False}
    t0 = NOW
    assert mon.observe(leader_view(t0, own_kw=bad, peers=(record("n2", t0),)), {}) is None
    assert mon.observe(leader_view(t0 + 29_000, own_kw=bad, peers=(record("n2", t0 + 29_000),)), {}) is None
    # peer record older than the start of the failure is not a target
    assert mon.observe(leader_view(t0 + 30_000, own_kw=bad, peers=(record("n2", t0 - 1),)), {}) is None
    # peer in back-off is not eligible
    busy = record("n2", t0 + 31_000, backoff_until_ms=t0 + 90_000)
    assert mon.observe(leader_view(t0 + 31_000, own_kw=bad, peers=(busy,)), {}) is None
    # SAFE_HOLD blocks
    assert mon.observe(leader_view(t0 + 31_000, own_kw=bad, peers=(record("n2", t0 + 31_000),),
                                   safe_hold=True), {}) is None
    d = mon.observe(leader_view(t0 + 32_000, own_kw=bad, peers=(record("n2", t0 + 32_000),)), {})
    assert d is not None and d.grounds == "hard_check" and d.target == "n2" and "redis_writable" in d.reason


def test_recovery_resets_the_failure_window() -> None:
    mon = AbdicationMonitor(MemoryAbdicationLedger())
    bad = {"pg_writable": False}
    mon.observe(leader_view(NOW, own_kw=bad), {})
    mon.observe(leader_view(NOW + 10_000), {})                       # passes again
    assert mon.failing_since_ms is None
    mon.observe(leader_view(NOW + 20_000, own_kw=bad), {})
    assert mon.observe(leader_view(NOW + 45_000, own_kw=bad, peers=(record("n2", NOW + 45_000),)), {}) is None
    assert mon.observe(leader_view(NOW + 50_000, own_kw=bad, peers=(record("n2", NOW + 50_000),)), {}) is not None


def test_risksweep_circuit_abdicates_at_most_once_per_incident() -> None:
    ledger = MemoryAbdicationLedger()
    open_since = NOW - 6 * 60_000
    kw = {"risksweep_circuit_open_since_ms": open_since}
    mon = AbdicationMonitor(ledger)
    mon.observe(leader_view(NOW, own_kw=kw), {})
    d = mon.observe(leader_view(NOW + 30_000, own_kw=kw, peers=(record("n2", NOW + 30_000),)), {})
    assert d is not None and ledger.risksweep_abdicated()
    # the new leader's circuit opens too: it stays leader and pages "failing on both nodes"
    mon2 = AbdicationMonitor(ledger)
    mon2.observe(leader_view(NOW + 60_000, own_kw={"risksweep_circuit_open_since_ms": NOW - 400_000}), {})
    d2 = mon2.observe(leader_view(NOW + 95_000, own_kw={"risksweep_circuit_open_since_ms": NOW - 400_000},
                                  peers=(record("n1", NOW + 95_000),)), {})
    assert d2 is None and mon2.both_nodes_paged
    mon2.observe(leader_view(NOW + 100_000), {})                     # RiskSweep recovered: incident over
    assert not ledger.risksweep_abdicated() and not mon2.both_nodes_paged


def link(**kw: object) -> BrokerLink:
    base: dict[str, object] = dict(net_failed_since_ms=NOW - 20_000, market_open=True, auth_failed=False,
                                   maintenance=False, streaming=False)
    base.update(kw)
    return BrokerLink(**base)  # type: ignore[arg-type]


def test_broker_grounds_abdication_rules() -> None:
    peer_ok = record("n2", NOW, broker_tls={"stonex": True})
    v = leader_view(NOW, peers=(peer_ok,))
    for blocked in (link(net_failed_since_ms=NOW - 19_999), link(market_open=False), link(auth_failed=True),
                    link(maintenance=True), link(streaming=True), link(net_failed_since_ms=None)):
        assert AbdicationMonitor(MemoryAbdicationLedger()).observe(v, {"stonex": blocked}) is None
    no_peer = leader_view(NOW, peers=(record("n2", NOW, broker_tls={"stonex": False}),))
    assert AbdicationMonitor(MemoryAbdicationLedger()).observe(no_peer, {"stonex": link()}) is None
    ledger = MemoryAbdicationLedger()
    d = AbdicationMonitor(ledger).observe(v, {"stonex": link()})
    assert d is not None and d.grounds == "broker" and d.adapter_id == "stonex" and d.target == "n2"
    # at most once until the adapter reaches STREAMING on some node
    assert AbdicationMonitor(ledger).observe(v, {"stonex": link()}) is None
    AbdicationMonitor(ledger).observe(v, {"stonex": link(streaming=True)})
    assert AbdicationMonitor(ledger).observe(v, {"stonex": link()}) is not None
