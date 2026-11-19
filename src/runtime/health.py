"""Node health record (operations.md §4) and its publication under the node's own etcd lease.

Hard checks: PostgreSQL primary writable, etcd reachable, local Redis writable, chrony within 50 ms,
and for the leader only the RiskSweep circuit not open for more than 5 min (§3).
Preferences: a TLS handshake per broker endpoint (ignored while that adapter's market is closed),
chrony < 1 ms, hosting the Patroni primary (tie-break). The record also carries the spill-file size
and its timestamp; a record older than 5 s counts as absent.
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Final, Protocol

from src.runtime.etcd import HEALTH_PREFIX, EtcdClient, EtcdError

log = logging.getLogger(__name__)

STALE_AFTER_MS: Final = 5_000
CHRONY_HARD_MS: Final = 50.0
CHRONY_PREFERRED_MS: Final = 1.0
RISKSWEEP_CIRCUIT_HARD_MS: Final = 5 * 60_000
HEALTH_LEASE_TTL_S: Final = 10


@dataclass(frozen=True)
class HealthRecord:
    node: str
    ts_ms: int
    role: str
    pg_writable: bool
    etcd_reachable: bool
    redis_writable: bool
    chrony_offset_ms: float | None
    # leader only: when the RiskSweep task circuit opened (None = closed)
    risksweep_circuit_open_since_ms: int | None = None
    # preferences: adapter -> handshake ok (None = market closed or adapter AUTH_FAILED / maintenance)
    broker_tls: Mapping[str, bool | None] = field(default_factory=dict)
    hosts_patroni_primary: bool = False
    spill_bytes: int = 0
    backoff_until_ms: int | None = None      # this node's effective back-off (peers read it)
    safe_hold: bool = False
    # The ARM / market-data registry this node runs (design §B6.1 version lifecycle, PR13): the sorted `KIND:VERSION`
    # tokens of its strategies, guards and indicators; empty until the optional services publish it (S32). Readers
    # test membership (`arm config accept` refuses while a fresh node's list lacks a referenced version).
    registry: tuple[str, ...] = ()

    # --- checks ------------------------------------------------------------------------------------
    def fresh(self, now_ms: int) -> bool:
        return now_ms - self.ts_ms <= STALE_AFTER_MS

    def failed_hard_checks(self, now_ms: int, *, as_leader: bool) -> list[str]:
        failed = [name for name, ok in (("pg_writable", self.pg_writable), ("etcd_reachable", self.etcd_reachable),
                                        ("redis_writable", self.redis_writable)) if not ok]
        if self.chrony_offset_ms is None or abs(self.chrony_offset_ms) > CHRONY_HARD_MS:
            failed.append("chrony")
        if (as_leader and self.risksweep_circuit_open_since_ms is not None
                and now_ms - self.risksweep_circuit_open_since_ms > RISKSWEEP_CIRCUIT_HARD_MS):
            failed.append("risksweep_circuit")
        return failed

    def passes_hard_checks(self, now_ms: int, *, as_leader: bool = False) -> bool:
        return not self.failed_hard_checks(now_ms, as_leader=as_leader)

    def in_backoff(self, now_ms: int) -> bool:
        return self.backoff_until_ms is not None and now_ms < self.backoff_until_ms

    def eligible_target(self, now_ms: int) -> bool:
        """Fresh, passes the hard checks, not in back-off, no SAFE_HOLD (eligibility ignores preferences)."""
        return (self.fresh(now_ms) and self.passes_hard_checks(now_ms) and not self.in_backoff(now_ms)
                and not self.safe_hold)

    def preference_score(self) -> tuple[int, int, int]:
        """(reachable brokers with an open market, chrony < 1 ms, hosts the Patroni primary)."""
        brokers = sum(1 for ok in self.broker_tls.values() if ok)
        chrony = int(self.chrony_offset_ms is not None and abs(self.chrony_offset_ms) < CHRONY_PREFERRED_MS)
        return (brokers, chrony, int(self.hosts_patroni_primary))

    # --- wire form ---------------------------------------------------------------------------------
    def to_json(self) -> str:
        d = asdict(self)
        d["broker_tls"] = dict(self.broker_tls)
        d["registry"] = list(self.registry)
        return json.dumps(d, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def from_json(text: str) -> "HealthRecord":
        d: dict[str, Any] = json.loads(text)
        return HealthRecord(
            node=str(d["node"]), ts_ms=int(d["ts_ms"]), role=str(d.get("role", "")),
            pg_writable=bool(d["pg_writable"]), etcd_reachable=bool(d["etcd_reachable"]),
            redis_writable=bool(d["redis_writable"]),
            chrony_offset_ms=None if d.get("chrony_offset_ms") is None else float(d["chrony_offset_ms"]),
            risksweep_circuit_open_since_ms=d.get("risksweep_circuit_open_since_ms"),
            broker_tls={str(k): (None if v is None else bool(v)) for k, v in (d.get("broker_tls") or {}).items()},
            hosts_patroni_primary=bool(d.get("hosts_patroni_primary", False)),
            spill_bytes=int(d.get("spill_bytes", 0)),
            backoff_until_ms=d.get("backoff_until_ms"), safe_hold=bool(d.get("safe_hold", False)),
            registry=_registry(d.get("registry")))


def _registry(raw: object) -> tuple[str, ...]:
    """The published registry tokens; a record from a node without the field (or with a malformed one) has none."""
    if not isinstance(raw, (list, tuple)):
        return ()
    return tuple(x for x in raw if isinstance(x, str) and x)


class HealthProbe(Protocol):
    """Builds this node's record (the coordinator wires PostgreSQL / Redis / chrony / TLS probes).
    `role`, `backoff_until_ms` and `safe_hold` are overwritten by the node controller."""

    async def probe(self, now_ms: int) -> HealthRecord: ...


class HealthBoard:
    """Publishes this node's record under its own etcd lease and reads the peers' records.
    Blocking (etcd over HTTP): call through `asyncio.to_thread`."""

    def __init__(self, etcd: EtcdClient, node_id: str, *, timeout_s: float = 1.0) -> None:
        self._etcd = etcd
        self._node = node_id
        self._timeout = timeout_s
        self._lease_id: int | None = None

    def publish(self, record: HealthRecord) -> None:
        if self._lease_id is not None:
            try:
                if self._etcd.lease_keepalive(self._lease_id, timeout_s=self._timeout) <= 0:
                    self._lease_id = None
            except EtcdError:
                self._lease_id = None
        if self._lease_id is None:
            self._lease_id = self._etcd.lease_grant(HEALTH_LEASE_TTL_S, timeout_s=self._timeout).lease_id
        self._etcd.put(HEALTH_PREFIX + self._node, record.to_json(), lease_id=self._lease_id,
                       timeout_s=self._timeout)

    def peers(self) -> list[HealthRecord]:
        out: list[HealthRecord] = []
        for kv in self._etcd.get_prefix(HEALTH_PREFIX, timeout_s=self._timeout):
            if kv.key == HEALTH_PREFIX + self._node:
                continue
            try:
                out.append(HealthRecord.from_json(kv.value))
            except (ValueError, KeyError, TypeError) as exc:
                log.warning("unreadable health record %s: %r", kv.key, exc)
        return out


def wall_ms() -> int:
    return time.time_ns() // 1_000_000
