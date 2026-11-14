"""LeaseKeeper: the etcd leader lease, self-fencing and the role deadlines, in a dedicated thread
(operations.md §2, §3, §4).

* Campaign: grant a lease (TTL 15 s), then a txn `create_revision(/xtnl/leader) == 0 -> put with lease`.
  The epoch is (etcd cluster_id, revision of that txn), the fencing token bumped in PostgreSQL.
* Renew every 1 s; a failed keepalive is retried at once (the client fails over to the next endpoint).
* `lease_deadline` = monotonic send time of the last acknowledged keepalive + granted TTL - margin.
  The default margin is TTL - fence (3 s), so `lease_valid()` turns False exactly when the fence fires.
* Fence: 12 s after the send time of the last acknowledged keepalive, or at once when etcd reports the
  lease gone, the keeper calls the fence function (production: `os._exit`, see `exits.py`).
  Never before 12 s: a transient etcd outage (an election takes <= 2 x 2.5 s, endpoint failover
  <= 2 s) fits in the 11 s budget fence - renew interval.
* Deadlines: LEADER_INIT must reach LEADER within 120 s and DRAINING must finish within 10 s; on expiry
  the keeper revokes the lease and exits (`init_failed` / `abdicate`), independently of the event loop.
* Ordering checked at construction: watchdog 3 s < fence 12 s < TTL 15 s.

`tick()` performs one keeper iteration and is what the thread loops over; tests drive it directly
with a fake monotonic clock and a fake EtcdClient.
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, Literal

from src.db.epoch import Epoch
from src.runtime.etcd import LEADER_KEY, EtcdClient, EtcdError, KeyValue
from src.runtime.exits import ExitFn, hard_exit

log = logging.getLogger(__name__)

DeadlineReason = Literal["init_failed", "abdicate"]
ETCD_ELECTION_WORST_S: Final = 2 * 2.5
ENDPOINT_FAILOVER_S: Final = 2.0


@dataclass(frozen=True)
class LeaseTimings:
    ttl_s: int = 15
    renew_interval_s: float = 1.0
    fence_after_s: float = 12.0
    watchdog_s: float = 3.0
    margin_s: float | None = None          # None -> ttl - fence_after
    leader_init_deadline_s: float = 120.0
    draining_deadline_s: float = 10.0
    request_timeout_s: float = 2.0         # total budget of one etcd call (all endpoints)
    retry_pause_s: float = 0.05            # pause between immediate keepalive retries

    def __post_init__(self) -> None:
        if not self.watchdog_s < self.fence_after_s < self.ttl_s:
            raise ValueError("ordering violated: watchdog < fence < TTL required (operations.md §3)")
        if self.renew_interval_s >= self.watchdog_s:
            raise ValueError("renew interval must be shorter than the watchdog")
        if self.fence_after_s - self.renew_interval_s <= ETCD_ELECTION_WORST_S + ENDPOINT_FAILOVER_S:
            raise ValueError("fence budget must exceed the worst etcd election plus endpoint failover")

    @property
    def margin(self) -> float:
        return self.margin_s if self.margin_s is not None else self.ttl_s - self.fence_after_s


@dataclass
class _Held:
    lease_id: int
    epoch: Epoch
    last_ack_send: float
    granted_ttl: float


class LeaseKeeper:
    def __init__(self, etcd: EtcdClient, *, timings: LeaseTimings | None = None,
                 clock: Callable[[], float] = time.monotonic, fence: ExitFn = hard_exit,
                 leader_key: str = LEADER_KEY) -> None:
        self._etcd = etcd
        self.timings = timings or LeaseTimings()
        self._clock = clock
        self._fence_fn = fence
        self._key = leader_key
        self._lock = threading.Lock()
        self._held: _Held | None = None
        self._deadline: tuple[float, DeadlineReason] | None = None
        self._next_renew = 0.0
        self._fenced: str | None = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self.renew_latencies: list[float] = []      # recent keepalive round trips (lease-renew p99 metric)

    # --- queries (any thread) ----------------------------------------------------------------------
    @property
    def fenced(self) -> str | None:
        return self._fenced

    @property
    def epoch(self) -> Epoch | None:
        with self._lock:
            return self._held.epoch if self._held is not None else None

    def holds_lease(self) -> bool:
        with self._lock:
            return self._held is not None and self._fenced is None

    def lease_deadline(self) -> float | None:
        """Monotonic instant until which externally visible actions are allowed."""
        with self._lock:
            h = self._held
            if h is None or self._fenced is not None:
                return None
            return h.last_ack_send + h.granted_ttl - self.timings.margin

    def lease_valid(self, min_remaining_s: float = 0.0) -> bool:
        """Synchronous check before any externally visible action (§2): broker sessions, token
        refreshes, notifications. `min_remaining_s` serves e.g. the 8 s token-refresh rule (§13; at most
        ~TTL - margin = fence 12 s remains right after a renewal, so a bound must stay below fence - renew)."""
        deadline = self.lease_deadline()
        return deadline is not None and self._clock() + min_remaining_s < deadline

    def remaining_s(self) -> float:
        deadline = self.lease_deadline()
        return 0.0 if deadline is None else max(0.0, deadline - self._clock())

    def leader(self) -> KeyValue | None:
        """Current holder of the leader key (blocking etcd read; call via to_thread)."""
        return self._etcd.get(self._key, timeout_s=self.timings.request_timeout_s)

    # --- campaign / release (blocking; call via asyncio.to_thread) ---------------------------------
    def campaign(self, node_id: str) -> Epoch | None:
        """Try to take the leader key. Returns the new epoch, or None when another node holds it."""
        if self._fenced is not None:
            return None
        t = self.timings
        send = self._clock()
        grant = self._etcd.lease_grant(t.ttl_s, timeout_s=t.request_timeout_s)
        try:
            res = self._etcd.create_if_absent(self._key, node_id, lease_id=grant.lease_id,
                                              timeout_s=t.request_timeout_s)
        except EtcdError:
            self._revoke_quietly(grant.lease_id)
            raise
        if not res.succeeded:
            self._revoke_quietly(grant.lease_id)
            return None
        epoch = Epoch(res.cluster_id or grant.cluster_id, res.revision)
        with self._lock:
            self._held = _Held(grant.lease_id, epoch, send, float(min(grant.ttl_s, t.ttl_s)))
            self._next_renew = send + t.renew_interval_s
            self._deadline = None
        self._wake.set()
        log.info("won leader lease %d, epoch %s", grant.lease_id, epoch.label)
        return epoch

    def release(self) -> None:
        """End of DRAINING: revoke the lease (the key goes with it) and clear the deadline."""
        with self._lock:
            held, self._held, self._deadline = self._held, None, None
        if held is not None:
            self._revoke_quietly(held.lease_id)

    def _revoke_quietly(self, lease_id: int) -> None:
        try:
            self._etcd.lease_revoke(lease_id, timeout_s=self.timings.request_timeout_s)
        except Exception as exc:   # the TTL expires it anyway
            log.warning("lease revoke %d failed: %r", lease_id, exc)

    # --- deadlines ---------------------------------------------------------------------------------
    def begin_leader_init(self) -> None:
        with self._lock:
            self._deadline = (self._clock() + self.timings.leader_init_deadline_s, "init_failed")
        self._wake.set()

    def reached_leader(self) -> None:
        with self._lock:
            self._deadline = None

    def begin_draining(self) -> None:
        with self._lock:
            self._deadline = (self._clock() + self.timings.draining_deadline_s, "abdicate")
        self._wake.set()

    def deadline(self) -> tuple[float, DeadlineReason] | None:
        with self._lock:
            return self._deadline

    # --- the keeper iteration ----------------------------------------------------------------------
    def _fence(self, reason: str) -> None:
        with self._lock:
            if self._fenced is not None:
                return
            self._fenced = reason
        log.critical("FENCE: %s", reason)
        self._fence_fn(reason)

    def tick(self) -> None:
        """One iteration: deadlines, fence, renewal."""
        if self._fenced is not None:
            return
        now = self._clock()
        with self._lock:
            held, deadline = self._held, self._deadline
        if deadline is not None and now >= deadline[0]:
            if held is not None:
                self._revoke_quietly(held.lease_id)
            self._fence(f"{deadline[1]}: deadline passed")
            return
        if held is None:
            return
        t = self.timings
        if now - held.last_ack_send >= t.fence_after_s:
            self._fence(f"fence: lease not renewed for {now - held.last_ack_send:.1f} s")
            return
        if now < self._next_renew:
            return
        budget = held.last_ack_send + t.fence_after_s - now
        send = now
        try:
            ttl = self._etcd.lease_keepalive(held.lease_id, timeout_s=min(t.request_timeout_s, budget))
        except Exception as exc:
            log.warning("lease keepalive failed: %r; retrying at once", exc)
            self._next_renew = self._clock() + t.retry_pause_s
            ttl = None
        if ttl is not None:
            if ttl <= 0:
                with self._lock:
                    still_held = self._held is held
                if not still_held:
                    # release() revoked this lease while the keepalive was in flight (end of DRAINING or
                    # SAFE_HOLD at LEADER_INIT): TTL 0 is the revoke's answer, not a lost lease.
                    log.info("lease %d released during keepalive; ignoring its TTL 0", held.lease_id)
                    return
                self._fence("fence: etcd reports the lease expired")
                return
            with self._lock:
                if self._held is held:
                    held.last_ack_send = send
                    held.granted_ttl = float(min(ttl, t.ttl_s))
            self._next_renew = send + t.renew_interval_s
            self.renew_latencies = (self.renew_latencies + [self._clock() - send])[-300:]
        now2 = self._clock()
        with self._lock:
            released = self._held is not held
        if released:
            return                            # release() ran while the keepalive was in flight: no fence
        if now2 - held.last_ack_send >= t.fence_after_s:
            self._fence(f"fence: lease not renewed for {now2 - held.last_ack_send:.1f} s")

    def _sleep_s(self) -> float:
        now = self._clock()
        cands = [0.25]
        with self._lock:
            held, deadline = self._held, self._deadline
        if held is not None:
            cands += [self._next_renew - now, held.last_ack_send + self.timings.fence_after_s - now]
        if deadline is not None:
            cands.append(deadline[0] - now)
        return max(0.005, min(cands))

    # --- thread ------------------------------------------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set() and self._fenced is None:
            try:
                self.tick()
            except Exception:       # never let the keeper die silently; the link task notices death
                log.exception("lease keeper iteration failed")
            self._wake.wait(self._sleep_s())
            self._wake.clear()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="lease-keeper", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
