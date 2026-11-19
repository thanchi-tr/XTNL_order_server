"""SM9 node role, campaign eligibility, back-off, abdication and the anti-flap SAFE_HOLD
(operations.md §4).

    STARTING -> STANDBY -> CANDIDATE -> LEADER_INIT -> LEADER -> DRAINING -> STANDBY
    any -> FENCED (the process is killed and restarts at STARTING); STANDBY <-> SAFE_HOLD
    a stop during LEADER_INIT (K15): LEADER_INIT -> DRAINING once step 1 committed, else LEADER_INIT -> STANDBY

Everything here is pure (callers pass `now_ms` and the views); the node controller performs the I/O.
"""
from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Literal, Protocol

from src.runtime.health import HealthRecord

log = logging.getLogger(__name__)

BACKOFF_MS: Final = 60_000
LEASE_FREE_ANY_NODE_MS: Final = 15_000
ABDICATE_MIN_PROBES: Final = 2
ABDICATE_MIN_FAILING_MS: Final = 30_000
T_UNHEALTHY_MS: Final = 20_000
ABNORMAL_WINDOW_MS: Final = 15 * 60_000
ABNORMAL_LIMIT: Final = 3
EPOCH_WINDOW_MS: Final = 30 * 60_000
EPOCH_CHANGE_LIMIT: Final = 6

EndReason = Literal["switchover", "abdicate", "lost", "init_failed"]
ABNORMAL: Final = frozenset({"lost", "init_failed"})


class NodeRole(str, Enum):
    STARTING = "STARTING"
    STANDBY = "STANDBY"
    CANDIDATE = "CANDIDATE"
    LEADER_INIT = "LEADER_INIT"
    LEADER = "LEADER"
    DRAINING = "DRAINING"
    SAFE_HOLD = "SAFE_HOLD"
    FENCED = "FENCED"


R = NodeRole
ALLOWED: Final[Mapping[NodeRole, frozenset[NodeRole]]] = {
    R.STARTING: frozenset({R.STANDBY, R.SAFE_HOLD, R.FENCED}),
    R.STANDBY: frozenset({R.CANDIDATE, R.SAFE_HOLD, R.FENCED}),
    R.CANDIDATE: frozenset({R.LEADER_INIT, R.STANDBY, R.SAFE_HOLD, R.FENCED}),
    # K15: a stop during LEADER_INIT ends a started term through DRAINING, or goes back to STANDBY when it came
    # before the epoch bump (no term).
    R.LEADER_INIT: frozenset({R.LEADER, R.DRAINING, R.STANDBY, R.SAFE_HOLD, R.FENCED}),
    R.LEADER: frozenset({R.DRAINING, R.FENCED}),
    R.DRAINING: frozenset({R.STANDBY, R.FENCED}),
    R.SAFE_HOLD: frozenset({R.STANDBY, R.FENCED}),
    R.FENCED: frozenset(),
}


class IllegalRoleTransition(RuntimeError):
    pass


@dataclass(frozen=True)
class TermRow:
    """One `leader_terms` row."""

    cluster_id: str
    epoch_revision: int
    node: str
    started_at: int
    reached_leader_at: int | None = None
    ended_at: int | None = None
    end_reason: EndReason | None = None


@dataclass(frozen=True)
class Backoff:
    until_ms: int
    waivable: bool      # after FENCED / a kill: waived when no peer has a fresh health record
    reason: str


@dataclass(frozen=True)
class ClusterState:
    """`cluster_state` (mirrored to the etcd key /xtnl/safe_hold that may_campaign() reads)."""

    safe_hold: bool = False
    safe_hold_since: int | None = None
    safe_hold_reason: str | None = None
    last_resume_at: int | None = None


def learn_backoff(own_terms: Sequence[TermRow], now_ms: int) -> Backoff | None:
    """STARTING: an open term of this node (it was killed) or one ended lost / init_failed within the
    last 60 s starts a waivable back-off; a term this node drained (end_reason switchover / abdicate
    written, not yet closed by a successor, or closed within 60 s) starts the plain DRAINING back-off."""
    best: Backoff | None = None
    for t in own_terms:
        cand: Backoff | None = None
        if t.end_reason is None and t.ended_at is None:
            cand = Backoff(now_ms + BACKOFF_MS, True, f"open term {t.epoch_revision}: process was killed")
        elif t.end_reason in ABNORMAL and t.ended_at is not None and now_ms - t.ended_at < BACKOFF_MS:
            cand = Backoff(now_ms + BACKOFF_MS, True, f"term {t.epoch_revision} ended {t.end_reason}")
        elif t.end_reason in ("switchover", "abdicate") and (t.ended_at is None or now_ms - t.ended_at < BACKOFF_MS):
            cand = Backoff(now_ms + BACKOFF_MS, False, f"term {t.epoch_revision} drained ({t.end_reason})")
        if cand is not None and (best is None or (best.waivable and not cand.waivable)):
            best = cand
    return best


def safe_hold_due(terms: Sequence[TermRow], now_ms: int, last_resume_at: int | None) -> str | None:
    """Pure mirror of the SQL check in `src/db/leader.py`: 3 terms that started and ended abnormally within
    15 min, or more than 6 epoch changes within 30 min; only terms after the last operator resume count. A term
    left open by a kill and closed later by its successor started outside the window, so it is no flap."""
    floor = last_resume_at or 0
    window_start = now_ms - ABNORMAL_WINDOW_MS
    abnormal = sum(1 for t in terms if t.end_reason in ABNORMAL and t.ended_at is not None
                   and t.ended_at >= window_start and t.started_at >= window_start and t.ended_at > floor)
    if abnormal >= ABNORMAL_LIMIT:
        return f"{abnormal} abnormal term ends within 15 min"
    changes = sum(1 for t in terms if t.started_at >= now_ms - EPOCH_WINDOW_MS and t.started_at > floor)
    if changes > EPOCH_CHANGE_LIMIT:
        return f"{changes} epoch changes within 30 min"
    return None


@dataclass(frozen=True)
class ClusterView:
    """What a campaign decision reads (all from etcd + this node's probe)."""

    now_ms: int
    own: HealthRecord
    peers: Sequence[HealthRecord]
    lease_holder: str | None
    lease_free_since_ms: int | None
    safe_hold: bool

    def fresh_peers(self) -> list[HealthRecord]:
        return [p for p in self.peers if p.fresh(self.now_ms)]


@dataclass
class RoleMachine:
    """SM9 for this node. `history` records (old, new, reason, at_ms)."""

    node_id: str
    role: NodeRole = NodeRole.STARTING
    backoff: Backoff | None = None
    history: deque[tuple[NodeRole, NodeRole, str, int]] = field(default_factory=lambda: deque(maxlen=500))  # bounded

    def go(self, new: NodeRole, reason: str, now_ms: int) -> None:
        if new not in ALLOWED[self.role]:
            raise IllegalRoleTransition(f"{self.role.value} -> {new.value} ({reason})")
        self.history.append((self.role, new, reason, now_ms))
        log.info("role %s -> %s: %s", self.role.value, new.value, reason)
        self.role = new

    # --- back-off ----------------------------------------------------------------------------------
    def start(self, own_terms: Sequence[TermRow], cluster: ClusterState, now_ms: int) -> None:
        """STARTING -> STANDBY (or SAFE_HOLD), learning the back-off from leader_terms."""
        self.backoff = learn_backoff(own_terms, now_ms)
        if cluster.safe_hold:
            self.go(R.SAFE_HOLD, f"cluster SAFE_HOLD: {cluster.safe_hold_reason}", now_ms)
        else:
            self.go(R.STANDBY, "started" + (f"; back-off: {self.backoff.reason}" if self.backoff else ""), now_ms)

    def set_backoff(self, now_ms: int, reason: str, *, waivable: bool = False) -> None:
        self.backoff = Backoff(now_ms + BACKOFF_MS, waivable, reason)

    def in_backoff(self, now_ms: int, any_fresh_peer: bool) -> bool:
        b = self.backoff
        if b is None or now_ms >= b.until_ms:
            return False
        return not (b.waivable and not any_fresh_peer)

    def effective_backoff_until(self, now_ms: int, any_fresh_peer: bool) -> int | None:
        return self.backoff.until_ms if self.backoff and self.in_backoff(now_ms, any_fresh_peer) else None

    # --- campaign ----------------------------------------------------------------------------------
    def eligible(self, view: ClusterView) -> bool:
        """Passes the hard checks, not in back-off, no SAFE_HOLD (preferences ignored)."""
        return (view.own.passes_hard_checks(view.now_ms)
                and not self.in_backoff(view.now_ms, bool(view.fresh_peers()))
                and not view.safe_hold)

    def prefers(self, view: ClusterView) -> bool:
        """No fresh eligible peer has a strictly better preference score."""
        mine = view.own.preference_score()
        return all(p.preference_score() <= mine for p in view.fresh_peers() if p.eligible_target(view.now_ms))

    def may_campaign(self, view: ClusterView) -> bool:
        """eligible and the lease is free and (prefers, or the lease has been free for 15 s)."""
        if self.role is not R.STANDBY or view.lease_holder is not None or not self.eligible(view):
            return False
        free_long = (view.lease_free_since_ms is not None
                     and view.now_ms - view.lease_free_since_ms >= LEASE_FREE_ANY_NODE_MS)
        return self.prefers(view) or free_long


# --- abdication -----------------------------------------------------------------------------------
@dataclass(frozen=True)
class BrokerLink:
    """The leader's view of one adapter for broker-grounds abdication."""

    net_failed_since_ms: int | None   # own DNS / TCP / TLS failure start (post-TLS errors never count)
    market_open: bool                 # SM10
    auth_failed: bool                 # AUTH_FAILED for the current credential generation
    maintenance: bool                 # BROKER_MAINTENANCE overlay
    streaming: bool                   # SM4 STREAMING (healthy whatever a probe says)


@dataclass(frozen=True)
class AbdicationDecision:
    grounds: Literal["hard_check", "broker"]
    reason: str
    target: str
    adapter_id: str | None = None


class AbdicationLedger(Protocol):
    """Cluster-wide 'at most once' memory (cluster_state). The RiskSweep incident uses the
    `risksweep_incident_id` / `risksweep_abdicated_at` columns."""

    def risksweep_abdicated(self) -> bool: ...

    def record_risksweep_abdication(self, now_ms: int) -> None: ...

    def risksweep_recovered(self) -> None: ...

    def broker_blocked(self, adapter_id: str) -> bool: ...

    def record_broker_abdication(self, adapter_id: str, now_ms: int) -> None: ...

    def adapter_streaming(self, adapter_id: str) -> None: ...


@dataclass
class MemoryAbdicationLedger:
    risksweep_done: bool = False
    blocked: set[str] = field(default_factory=set)

    def risksweep_abdicated(self) -> bool:
        return self.risksweep_done

    def record_risksweep_abdication(self, now_ms: int) -> None:
        self.risksweep_done = True

    def risksweep_recovered(self) -> None:
        self.risksweep_done = False

    def broker_blocked(self, adapter_id: str) -> bool:
        return adapter_id in self.blocked

    def record_broker_abdication(self, adapter_id: str, now_ms: int) -> None:
        self.blocked.add(adapter_id)

    def adapter_streaming(self, adapter_id: str) -> None:
        self.blocked.discard(adapter_id)


class AbdicationMonitor:
    """Leader-side abdication rules, fed once per health probe."""

    def __init__(self, ledger: AbdicationLedger, pager: Callable[[str, str, str], None] | None = None) -> None:
        self.ledger = ledger
        self.pager = pager            # (severity, title, body): the "RiskSweep failing on both nodes" page
        self.failing_since_ms: int | None = None
        self.failing_probes = 0
        self.both_nodes_paged = False
        self._risksweep_seen_open = False

    def observe(self, view: ClusterView, brokers: Mapping[str, BrokerLink]) -> AbdicationDecision | None:
        now = view.now_ms
        for adapter, link in brokers.items():
            if link.streaming:
                self.ledger.adapter_streaming(adapter)
        failed = view.own.failed_hard_checks(now, as_leader=True)
        if view.own.risksweep_circuit_open_since_ms is not None:
            self._risksweep_seen_open = True
        elif self._risksweep_seen_open:
            # the RiskSweep recovered on this leader: the incident is over
            self._risksweep_seen_open = False
            self.both_nodes_paged = False
            self.ledger.risksweep_recovered()
        decision = self._hard_check(view, failed)
        if decision is not None:
            return decision
        return self._broker(view, brokers)

    def _hard_check(self, view: ClusterView, failed: list[str]) -> AbdicationDecision | None:
        now = view.now_ms
        if not failed:
            self.failing_since_ms, self.failing_probes = None, 0
            return None
        if self.failing_since_ms is None:
            self.failing_since_ms = now
        self.failing_probes += 1
        if self.failing_probes < ABDICATE_MIN_PROBES or now - self.failing_since_ms < ABDICATE_MIN_FAILING_MS:
            return None
        if failed == ["risksweep_circuit"] and self.ledger.risksweep_abdicated():
            if not self.both_nodes_paged:
                self.both_nodes_paged = True
                log.critical("RiskSweep failing on both nodes: staying leader, ingestion continues")
                if self.pager is not None:
                    try:
                        self.pager("critical", "RiskSweep failing on both nodes",
                                   "the RiskSweep circuit is open on this leader after one abdication for this "
                                   "incident; staying leader, ingestion continues (operations.md §3)")
                    except Exception:
                        log.exception("both-nodes page failed")
            return None
        since = self.failing_since_ms
        targets = [p for p in view.peers if p.ts_ms > since and p.eligible_target(now) and not view.safe_hold]
        if not targets:
            return None
        target = max(targets, key=lambda p: (p.preference_score(), p.ts_ms))
        if failed == ["risksweep_circuit"]:
            self.ledger.record_risksweep_abdication(now)
        return AbdicationDecision("hard_check", "hard checks failing: " + ", ".join(failed), target.node)

    def _broker(self, view: ClusterView, brokers: Mapping[str, BrokerLink]) -> AbdicationDecision | None:
        now = view.now_ms
        if view.safe_hold:
            return None
        for adapter, link in sorted(brokers.items()):
            if (link.streaming or link.auth_failed or link.maintenance or not link.market_open
                    or link.net_failed_since_ms is None or now - link.net_failed_since_ms < T_UNHEALTHY_MS
                    or self.ledger.broker_blocked(adapter)):
                continue
            peers = [p for p in view.peers if p.eligible_target(now) and p.broker_tls.get(adapter) is True]
            if not peers:
                continue
            self.ledger.record_broker_abdication(adapter, now)
            target = max(peers, key=lambda p: (p.preference_score(), p.ts_ms))
            return AbdicationDecision("broker", f"{adapter}: own network path down for "
                                      f"{(now - link.net_failed_since_ms) // 1000} s while OPEN", target.node,
                                      adapter)
        return None
