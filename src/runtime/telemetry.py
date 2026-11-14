"""Engine telemetry: the collectors behind `GET /metrics` (operations.md §9; deploy/observability/metrics.md).

`build_telemetry(...)` returns a `MetricsRegistry` plus the `PeriodicReader`s that do the I/O (each bounded by
a timeout, run as background tasks of the process in every SM9 role):

    in memory (scrape time)       role, lease p99 / remaining, health records (this node + peers from the last
                                  etcd read), co-location, event-loop lag, chrony offset, and the leader
                                  term's series (`EngineSession.metrics_samples`: bus / spool / SM4 / ingest /
                                  RiskSweep / token expiry and age / REST bucket tokens and requests)
    PostgreSQL (control pool)     engine_epoch, epoch changes in 30 min, SAFE_HOLD, events_dlq per status
                                  (leader), pg_stat_replication replay lag
    Patroni REST                  members running, leader, sync standby count, timelines; the local member's
                                  pg_stat_archiver through a short read-only connection
    Redis (own)                   used_memory / maxmemory; oversee:feed length, PEL size, oldest pending (leader)
    etcd                          member health, raft leader, db size / quota, alarms
    files                         disk use per mount, certificate notAfter

Per adapter (6.5): `xtnl_rest_bucket_tokens` / `xtnl_rest_requests_total` come from the adapter's shared REST
bucket (`adapter.rest_budget.stats()`, absent for an unmetered adapter) and `xtnl_token_age_seconds` from
`adapter.token_issued_at_ms` (absent while unknown: a StoneX adapter without a session, a cTrader pair loaded
without its save time). Nothing here writes anything anywhere.
"""
from __future__ import annotations

import asyncio
import importlib
import logging
import os
import shutil
import ssl
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Protocol

import redis.asyncio as aioredis

from src.cache import keys as K
from src.cache.feed_stream import FEED_GROUP
from src.db.pg import ConnectionSource
from src.observability.metrics import MetricsRegistry, Sample, gauge
from src.observability.periodic import PeriodicReader
from src.runtime.etcd import EtcdClusterStatus
from src.runtime.health import HealthRecord
from src.runtime.patroni import PatroniMember, dsn_for_member
from src.runtime.roles import NodeRole

log = logging.getLogger(__name__)

CONTRACT_ROLES: Final = tuple(r.value for r in (NodeRole.STARTING, NodeRole.STANDBY, NodeRole.CANDIDATE,
                                                NodeRole.LEADER_INIT, NodeRole.LEADER, NodeRole.DRAINING))
DLQ_STATUSES: Final = ("QUEUED", "LEASED", "BACKOFF", "QUARANTINED")
ETCD_QUOTA_BYTES: Final = 2 * 1024 ** 3          # deploy/etcd/etcd.conf.yml quota-backend-bytes
DEFAULT_MOUNTS: Final = ("/", "/var/lib/postgresql", "/var/lib/etcd", "/var/lib/redis", "/var/lib/xtnl")
CERTS_VAR: Final = "XTNL_METRICS_CERTS"
LEASE_P99_WINDOW: Final = 60                     # keepalives (1 s renew interval: the last ~60 s)

SQL_EPOCH: Final = "SELECT revision, bumped_at_ms FROM engine_epoch WHERE id = 1"
SQL_TERMS_SINCE: Final = "SELECT count(*) FROM leader_terms WHERE started_at >= $1::bigint"
SQL_SAFE_HOLD: Final = "SELECT safe_hold FROM cluster_state WHERE id = 1"
SQL_DLQ_COUNTS: Final = "SELECT status, count(*) FROM events_dlq WHERE status <> 'RESOLVED' GROUP BY status"
SQL_REPLICATION: Final = (
    "SELECT application_name, state, EXTRACT(EPOCH FROM replay_lag)::float8 FROM pg_stat_replication"
)
SQL_ARCHIVER: Final = (
    "SELECT failed_count, EXTRACT(EPOCH FROM (now() - last_archived_time))::float8 FROM pg_stat_archiver"
)


# ============================================================================================ ports
class ControllerView(Protocol):
    """What the collectors read from the NodeController (src/runtime/node.py)."""

    @property
    def role(self) -> NodeRole: ...

    @property
    def session(self) -> object: ...

    @property
    def last_record(self) -> HealthRecord | None: ...


class KeeperView(Protocol):
    renew_latencies: list[float]

    def holds_lease(self) -> bool: ...

    def remaining_s(self) -> float: ...


class LagView(Protocol):
    def recent_max_lag_s(self, window_s: float = 10.0) -> float | None: ...


class PatroniView(Protocol):
    async def cluster(self) -> list[PatroniMember]: ...


Connector = Callable[[str], Awaitable[Any]]      # dsn -> connection with fetchrow() and close()


def p99(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round(0.99 * len(ordered) + 0.5)) - 1))
    return ordered[idx]


# ============================================================================================ in-memory collectors
def role_samples(role: NodeRole) -> list[Sample]:
    names = list(CONTRACT_ROLES) + ([role.value] if role.value not in CONTRACT_ROLES else [])
    return [Sample("xtnl_node_role", 1.0 if r == role.value else 0.0, {"role": r}) for r in names]


def lease_samples(keeper: KeeperView, leader: bool) -> list[Sample]:
    if not leader or not keeper.holds_lease():
        return []
    out = gauge("xtnl_lease_remaining_seconds", keeper.remaining_s())
    out += gauge("xtnl_lease_renew_p99_seconds", p99(list(keeper.renew_latencies)[-LEASE_P99_WINDOW:]))
    return out


def health_samples(own: HealthRecord | None, peers: Iterable[HealthRecord], now_ms: int) -> list[Sample]:
    """One set per subject: this node's own last record, and every peer record from the last etcd read."""
    records: dict[str, HealthRecord] = {}
    for rec in peers:
        records[rec.node] = rec
    if own is not None:
        records[own.node] = own
    out: list[Sample] = []
    for node, rec in sorted(records.items()):
        is_leader = rec.role == NodeRole.LEADER.value
        out += gauge("xtnl_health_record_age_seconds", max(0, now_ms - rec.ts_ms) / 1000.0, subject=node)
        out += gauge("xtnl_health_hard_checks_ok", rec.passes_hard_checks(now_ms, as_leader=is_leader), subject=node)
        out += gauge("xtnl_health_campaign_eligible", is_leader or rec.eligible_target(now_ms), subject=node)
        out += gauge("xtnl_health_spill_file_bytes", rec.spill_bytes, subject=node)
    return out


# ============================================================================================ periodic readers
async def read_pg_cluster(control: ConnectionSource, *, leader: Callable[[], bool],
                          clock_ms: Callable[[], int]) -> list[Sample]:
    """engine_epoch / leader_terms / cluster_state (any role), events_dlq per status (leader), replication lag."""
    now = clock_ms()
    out: list[Sample] = []
    async with control.acquire() as conn:
        row = await conn.fetchrow(SQL_EPOCH)
        if row is not None:
            out += gauge("xtnl_engine_epoch", int(row[0]))
            if row[1] is not None:
                out += gauge("xtnl_epoch_change_age_seconds", max(0, now - int(row[1])) / 1000.0)
        terms = await conn.fetchrow(SQL_TERMS_SINCE, now - 30 * 60_000)
        if terms is not None:
            out += gauge("xtnl_epoch_changes_30m", int(terms[0] or 0))
        hold = await conn.fetchrow(SQL_SAFE_HOLD)
        if hold is not None:
            out += gauge("xtnl_safe_hold", bool(hold[0]))
        if leader():
            counts = {str(r[0]): int(r[1]) for r in await conn.fetch(SQL_DLQ_COUNTS)}
            for status in DLQ_STATUSES:
                out += gauge("xtnl_dlq_rows", counts.get(status, 0), status=status)
        for r in await conn.fetch(SQL_REPLICATION):
            name, state, lag = str(r[0] or ""), str(r[1] or ""), r[2]
            if not name:
                continue
            if lag is None:
                if state != "streaming":
                    continue
                lag = 0.0            # caught up and idle: PostgreSQL shows no lag sample, not an unknown one
            out += gauge("xtnl_pg_replication_lag_seconds", float(lag), member=name)
    return out


async def read_patroni(patroni: PatroniView, *, local_hosts: Callable[[], frozenset[str]], dsn: str | None,
                       connect: Connector | None) -> list[Sample]:
    members = await patroni.cluster()
    out: list[Sample] = []
    for m in members:
        out += gauge("xtnl_patroni_member_running", m.state in ("running", "streaming"), member=m.name)
        if m.timeline is not None:
            out += gauge("xtnl_pg_timeline", m.timeline, member=m.name)
    out += gauge("xtnl_patroni_has_leader", any(m.is_primary and m.state == "running" for m in members))
    out += gauge("xtnl_patroni_sync_standby_count", sum(1 for m in members if m.is_sync_standby))
    if dsn is not None and connect is not None:
        hosts = await asyncio.to_thread(local_hosts)     # may resolve the hostname (DNS): never on the loop
        local = next((m for m in members if m.host is not None and m.host in hosts), None)
        if local is not None and local.host is not None:
            conn = await connect(dsn_for_member(dsn, local.host, local.port))
            try:
                row = await conn.fetchrow(SQL_ARCHIVER)
            finally:
                await conn.close()
            if row is not None:
                out += gauge("xtnl_wal_archive_failed_total", int(row[0] or 0), member=local.name)
                if row[1] is not None:
                    out += gauge("xtnl_wal_archive_age_seconds", max(0.0, float(row[1])), member=local.name)
    return out


def _decode(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


async def read_redis(redis: aioredis.Redis, *, leader: Callable[[], bool], clock_ms: Callable[[], int]) -> list[Sample]:
    out: list[Sample] = []
    info = await redis.info("memory")
    used, cap = int(info.get("used_memory", 0) or 0), int(info.get("maxmemory", 0) or 0)
    if cap > 0:                          # maxmemory 0 = no limit configured: the ratio is undefined (absent)
        out += gauge("xtnl_redis_used_memory_ratio", used / cap)
    if leader():
        out += gauge("xtnl_feed_stream_length", int(await redis.xlen(K.FEED)))
        try:
            pend = await redis.xpending(K.FEED, FEED_GROUP)
        except aioredis.ResponseError:
            pend = None                  # no group yet (SM8 not run): the PEL is unknown, not empty
        if isinstance(pend, dict):
            count = int(pend.get("pending", 0) or 0)
            out += gauge("xtnl_feed_pending_entries", count)
            if count == 0:
                out += gauge("xtnl_feed_oldest_pending_age_seconds", 0)
            elif pend.get("min") is not None:
                oldest = int(_decode(pend["min"]).split("-", 1)[0])
                out += gauge("xtnl_feed_oldest_pending_age_seconds", max(0, clock_ms() - oldest) / 1000.0)
    return out


def etcd_samples(st: EtcdClusterStatus, quota_bytes: int = ETCD_QUOTA_BYTES) -> list[Sample]:
    out: list[Sample] = []
    for m in st.members:
        out += gauge("xtnl_etcd_endpoint_healthy", m.healthy, member=m.member)
        if m.db_size is not None:
            out += gauge("xtnl_etcd_db_quota_ratio", m.db_size / quota_bytes, member=m.member)
    answered = [m for m in st.members if m.healthy and m.has_leader is not None]
    if answered:
        out += gauge("xtnl_etcd_has_leader", any(m.has_leader for m in answered))
    out += gauge("xtnl_etcd_alarms_active", st.alarms_active)
    return out


def disk_samples(mounts: Sequence[str]) -> list[Sample]:
    out: list[Sample] = []
    for mount in mounts:
        if not os.path.isdir(mount):
            continue
        try:
            du = shutil.disk_usage(mount)
        except OSError:
            continue
        if du.total > 0:
            out += gauge("xtnl_disk_used_ratio", du.used / du.total, mount=mount)
    return out


def cert_not_after_s(path: str) -> float | None:
    """notAfter of a PEM certificate (epoch seconds), via the stdlib's certificate decoder; None if unreadable."""
    decode = getattr(getattr(ssl, "_ssl", None), "_test_decode_cert", None)
    if decode is None or not os.path.isfile(path):
        return None
    try:
        return float(ssl.cert_time_to_seconds(str(decode(path)["notAfter"])))
    except Exception:
        return None


def cert_samples(certs: Mapping[str, str], now_s: float) -> list[Sample]:
    out: list[Sample] = []
    for name, path in sorted(certs.items()):
        na = cert_not_after_s(path)
        if na is not None:
            out += gauge("xtnl_cert_expires_in_seconds", na - now_s, cert=name)
    return out


def certs_from_env(environ: Mapping[str, str]) -> dict[str, str]:
    """XTNL_METRICS_CERTS='etcd-client=/etc/xtnl/pki/etcd-client.crt,ca=/etc/xtnl/pki/ca.crt,...'; without it,
    the etcd client certificate and CA the engine already uses (XTNL_ETCD_CERT / XTNL_ETCD_CA)."""
    raw = (environ.get(CERTS_VAR) or "").strip()
    out: dict[str, str] = {}
    if raw:
        for part in raw.split(","):
            name, _, path = part.partition("=")
            if name.strip() and path.strip():
                out[name.strip()] = path.strip()
        return out
    if environ.get("XTNL_ETCD_CERT"):
        out["etcd-client"] = environ["XTNL_ETCD_CERT"]
    if environ.get("XTNL_ETCD_CA"):
        out["ca"] = environ["XTNL_ETCD_CA"]
    return out


async def asyncpg_connect(dsn: str, *, application_name: str = "xtnl-metrics") -> Any:
    """A short read-only connection (pg_stat_archiver of the local member)."""
    asyncpg = importlib.import_module("asyncpg")
    return await asyncpg.connect(dsn, timeout=3.0, server_settings={
        "application_name": application_name, "default_transaction_read_only": "on", "statement_timeout": "3000"})


# ============================================================================================ assembly
@dataclass
class Telemetry:
    registry: MetricsRegistry
    readers: list[PeriodicReader] = field(default_factory=list)

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [asyncio.create_task(r.run(stop), name=f"metrics:{r.name}") for r in self.readers]
        try:
            await stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


def build_telemetry(*, node_id: str, controller: ControllerView, keeper: KeeperView, watchdog: LagView | None = None,
                    chrony_offset_s: Callable[[], float | None] | None = None,
                    peers: Callable[[], Sequence[HealthRecord]] | None = None,
                    control: ConnectionSource | None = None, redis: aioredis.Redis | None = None,
                    patroni: PatroniView | None = None, dsn: str | None = None, connect: Connector | None = None,
                    local_hosts: Callable[[], frozenset[str]] | None = None,
                    etcd_status: Callable[[], EtcdClusterStatus] | None = None,
                    mounts: Sequence[str] = DEFAULT_MOUNTS, certs: Mapping[str, str] | None = None,
                    clock_ms: Callable[[], int] = _wall_ms, interval_s: float = 5.0) -> Telemetry:
    reg = MetricsRegistry(node_id)

    def is_leader() -> bool:
        return controller.role is NodeRole.LEADER

    reg.register("role", lambda: role_samples(controller.role))
    reg.register("lease", lambda: lease_samples(keeper, is_leader()))

    def health() -> list[Sample]:
        return health_samples(controller.last_record, peers() if peers is not None else (), clock_ms())
    reg.register("health", health)

    def colocated() -> list[Sample]:
        rec = controller.last_record
        if not is_leader() or rec is None:
            return []
        return gauge("xtnl_leader_colocated_with_primary", rec.hosts_patroni_primary)
    reg.register("colocated", colocated)
    if watchdog is not None:
        reg.register("loop", lambda: gauge("xtnl_event_loop_lag_seconds", watchdog.recent_max_lag_s(10.0)))
    if chrony_offset_s is not None:
        reg.register("clock", lambda: gauge("xtnl_clock_offset_seconds", chrony_offset_s()))

    def term() -> list[Sample]:
        session = controller.session
        fn = getattr(session, "metrics_samples", None)
        if fn is None:
            return gauge("xtnl_dlq_spool_ring_depth", 0)     # no term on this node: no ring, so it is empty
        out: list[Sample] = fn(leader=is_leader())
        return out
    reg.register("term", term)

    readers: list[PeriodicReader] = []

    def add(name: str, read: Callable[[], Awaitable[Sequence[Sample]]], every: float = interval_s) -> None:
        r = PeriodicReader(name, read, interval_s=every, timeout_s=min(4.0, every))
        readers.append(r)
        reg.register(name, r.collect)

    if control is not None:
        add("postgres", lambda: read_pg_cluster(control, leader=is_leader, clock_ms=clock_ms))
    if patroni is not None:
        hosts = local_hosts or (lambda: frozenset())
        add("patroni", lambda: read_patroni(patroni, local_hosts=hosts, dsn=dsn, connect=connect), 10.0)
    if redis is not None:
        add("redis", lambda: read_redis(redis, leader=is_leader, clock_ms=clock_ms))
    if etcd_status is not None:
        status = etcd_status

        async def etcd() -> list[Sample]:
            return etcd_samples(await asyncio.to_thread(status))
        add("etcd", etcd, 10.0)

    async def files() -> list[Sample]:
        return (await asyncio.to_thread(disk_samples, list(mounts))
                + await asyncio.to_thread(cert_samples, dict(certs or {}), clock_ms() / 1000.0))
    add("files", files, 30.0)
    return Telemetry(reg, readers)


__all__ = ["CERTS_VAR", "CONTRACT_ROLES", "DLQ_STATUSES", "ETCD_QUOTA_BYTES", "Telemetry", "asyncpg_connect",
           "build_telemetry", "cert_samples", "certs_from_env", "disk_samples", "etcd_samples", "health_samples",
           "lease_samples", "p99", "read_patroni", "read_pg_cluster", "read_redis", "role_samples"]
