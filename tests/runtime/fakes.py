"""Fakes for the runtime tests: a controllable clock and an in-memory etcd (no network)."""
from __future__ import annotations

from dataclasses import dataclass, field

from src.runtime.etcd import EtcdError, KeyValue, LeaseGrant, TxnResult
from src.runtime.health import HealthRecord


class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@dataclass
class _Lease:
    ttl: int
    expires: float


@dataclass
class FakeEtcd:
    """EtcdClient with leases that expire on the fake clock. `down` makes every call fail."""

    clock: FakeClock
    cluster_id: str = "c1"
    down: bool = False
    keepalive_ttl: int | None = None           # override the TTL a keepalive returns (0 = expired)
    revision: int = 100
    leases: dict[int, _Lease] = field(default_factory=dict)
    kv: dict[str, KeyValue] = field(default_factory=dict)
    calls: list[str] = field(default_factory=list)
    _next_id: int = 1

    def _check(self, op: str) -> None:
        self.calls.append(op)
        self._expire()
        if self.down:
            raise EtcdError(f"{op}: down")

    def _expire(self) -> None:
        now = self.clock()
        for lid in [lid for lid, lease in self.leases.items() if lease.expires <= now]:
            self._drop_lease(lid)

    def _drop_lease(self, lid: int) -> None:
        self.leases.pop(lid, None)
        for k in [k for k, v in self.kv.items() if v.lease_id == lid]:
            del self.kv[k]

    def lease_grant(self, ttl_s: int, *, timeout_s: float) -> LeaseGrant:
        self._check("grant")
        lid = self._next_id
        self._next_id += 1
        self.leases[lid] = _Lease(ttl_s, self.clock() + ttl_s)
        return LeaseGrant(lid, ttl_s, self.cluster_id, self.revision)

    def lease_keepalive(self, lease_id: int, *, timeout_s: float) -> int:
        self._check("keepalive")
        lease = self.leases.get(lease_id)
        if lease is None:
            return 0
        if self.keepalive_ttl is not None:
            return self.keepalive_ttl
        lease.expires = self.clock() + lease.ttl
        return lease.ttl

    def lease_revoke(self, lease_id: int, *, timeout_s: float) -> None:
        self._check("revoke")
        self._drop_lease(lease_id)

    def put(self, key: str, value: str, *, lease_id: int = 0, timeout_s: float) -> int:
        self._check("put")
        self.revision += 1
        old = self.kv.get(key)
        self.kv[key] = KeyValue(key, value, old.create_revision if old else self.revision, self.revision, lease_id)
        return self.revision

    def get(self, key: str, *, timeout_s: float) -> KeyValue | None:
        self._check("get")
        return self.kv.get(key)

    def get_prefix(self, prefix: str, *, timeout_s: float) -> list[KeyValue]:
        self._check("get_prefix")
        return [v for k, v in sorted(self.kv.items()) if k.startswith(prefix)]

    def create_if_absent(self, key: str, value: str, *, lease_id: int, timeout_s: float) -> TxnResult:
        self._check("txn")
        if key in self.kv:
            return TxnResult(False, self.cluster_id, self.revision)
        self.revision += 1
        self.kv[key] = KeyValue(key, value, self.revision, self.revision, lease_id)
        return TxnResult(True, self.cluster_id, self.revision)


def record(node: str = "n1", ts_ms: int = 0, **kw: object) -> HealthRecord:
    base: dict[str, object] = dict(node=node, ts_ms=ts_ms, role="STANDBY", pg_writable=True, etcd_reachable=True,
                                   redis_writable=True, chrony_offset_ms=0.5)
    base.update(kw)
    return HealthRecord(**base)  # type: ignore[arg-type]


class Exits:
    """Injected exit function: records instead of exiting."""

    def __init__(self) -> None:
        self.reasons: list[str] = []

    def __call__(self, reason: str) -> None:
        self.reasons.append(reason)
