"""Engine telemetry collectors (src/runtime/telemetry.py; deploy/observability/metrics.md; IMPLEMENTATION_PLAN 5.6).
No network: fakes, fakeredis, httpx.MockTransport."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fakeredis
import httpx

from src.cache import keys as K
from src.cache.feed_stream import FEED_GROUP
from src.observability.metrics import CONTRACT, Sample
from src.runtime import telemetry as T
from src.runtime.etcd import EtcdClusterStatus, EtcdMemberStatus, HttpEtcdClient
from src.runtime.health import HealthRecord
from src.runtime.patroni import PatroniMember
from src.runtime.probes import ChronyReader, parse_chrony_signed_s
from src.runtime.roles import (
    AbdicationMonitor,
    ClusterView,
    MemoryAbdicationLedger,
    NodeRole,
)
from src.runtime.supervisor import LoopWatchdog
from tests.db.conftest import FakeConn, FakeSource


def by_name(samples: list[Sample]) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return {(s.name, tuple(sorted(s.labels.items()))): s.value for s in samples}


def rec(node: str, ts: int, role: str = "STANDBY", **kw: Any) -> HealthRecord:
    base: dict[str, Any] = dict(pg_writable=True, etcd_reachable=True, redis_writable=True, chrony_offset_ms=0.2)
    base.update(kw)
    return HealthRecord(node, ts, role, **base)


def test_role_series_one_for_the_current_role() -> None:
    got = by_name(T.role_samples(NodeRole.LEADER))
    assert got[("xtnl_node_role", (("role", "LEADER"),))] == 1
    assert got[("xtnl_node_role", (("role", "STANDBY"),))] == 0
    assert len(got) == 6


@dataclass
class Keeper:
    held: bool = True
    renew_latencies: list[float] = field(default_factory=list)

    def holds_lease(self) -> bool:
        return self.held

    def remaining_s(self) -> float:
        return 11.5


def test_lease_p99_over_the_recent_window_and_only_for_the_leader() -> None:
    """Before 5.6: LeaseKeeper.renew_latencies was collected and never read (no p99 > 500 ms alert)."""
    k = Keeper(renew_latencies=[0.01] * 59 + [0.9])
    got = by_name(T.lease_samples(k, leader=True))
    assert got[("xtnl_lease_renew_p99_seconds", ())] == 0.9
    assert got[("xtnl_lease_remaining_seconds", ())] == 11.5
    assert T.lease_samples(k, leader=False) == []
    assert T.p99([]) is None


def test_health_series_per_subject() -> None:
    now = 100_000
    own = rec("node-a", now - 1_000, role="LEADER")
    peer_ok = rec("node-b", now - 2_000, spill_bytes=42)
    got = by_name(T.health_samples(own, [peer_ok, rec("node-a", 0)], now))
    assert got[("xtnl_health_record_age_seconds", (("subject", "node-a"),))] == 1.0   # own record wins
    assert got[("xtnl_health_campaign_eligible", (("subject", "node-a"),))] == 1      # the LEADER
    assert got[("xtnl_health_campaign_eligible", (("subject", "node-b"),))] == 1
    assert got[("xtnl_health_spill_file_bytes", (("subject", "node-b"),))] == 42
    backoff = rec("node-b", now - 1_000, backoff_until_ms=now + 60_000)
    bad = by_name(T.health_samples(None, [backoff], now))
    assert bad[("xtnl_health_campaign_eligible", (("subject", "node-b"),))] == 0
    assert bad[("xtnl_health_hard_checks_ok", (("subject", "node-b"),))] == 1


def test_event_loop_lag_is_the_worst_of_the_last_ten_seconds() -> None:
    """Before 5.6: only an all-time max and a log line; no metric for the 100 ms alert."""
    clock = [0.0]
    wd = LoopWatchdog(_Notifier(), clock=lambda: clock[0])
    assert wd.recent_max_lag_s() is None
    wd.beat(0.3)
    clock[0] = 5.0
    wd.beat(0.01)
    assert wd.recent_max_lag_s() == 0.3
    clock[0] = 11.0
    wd.beat(0.02)
    assert wd.recent_max_lag_s() == 0.02


class _Notifier:
    def notify(self, msg: str) -> None:
        return None


def test_chrony_offset_is_signed_seconds() -> None:
    line = "A1B2,10.0.0.1,3,1700000000.0,-0.000250,0.0,0.0"
    assert parse_chrony_signed_s(line) == -0.00025
    reader = ChronyReader(command=("definitely-not-a-binary-xtnl",))
    assert reader._run() is None and reader.signed_offset_s is None


async def test_postgres_reader_epoch_terms_safe_hold_dlq_and_replication() -> None:
    conn = FakeConn()
    conn.returns(T.SQL_EPOCH, (4711, 90_000))
    conn.returns(T.SQL_TERMS_SINCE, (3,))
    conn.returns(T.SQL_SAFE_HOLD, (False,))
    conn.returns_all(T.SQL_DLQ_COUNTS, [("QUEUED", 2), ("QUARANTINED", 1)])
    conn.returns_all(T.SQL_REPLICATION, [("node-b", "streaming", None), ("node-c", "startup", None),
                                         ("node-d", "streaming", 0.25)])
    got = by_name(await T.read_pg_cluster(FakeSource(conn), leader=lambda: True, clock_ms=lambda: 100_000))
    assert got[("xtnl_engine_epoch", ())] == 4711
    assert got[("xtnl_epoch_change_age_seconds", ())] == 10.0
    assert got[("xtnl_epoch_changes_30m", ())] == 3
    assert got[("xtnl_safe_hold", ())] == 0
    assert got[("xtnl_dlq_rows", (("status", "QUEUED"),))] == 2
    assert got[("xtnl_dlq_rows", (("status", "LEASED"),))] == 0           # read: a real zero
    assert got[("xtnl_pg_replication_lag_seconds", (("member", "node-b"),))] == 0.0
    assert ("xtnl_pg_replication_lag_seconds", (("member", "node-c"),)) not in got   # unknown: absent
    assert got[("xtnl_pg_replication_lag_seconds", (("member", "node-d"),))] == 0.25
    assert conn.args_of(T.SQL_TERMS_SINCE) == [(100_000 - 30 * 60_000,)]

    standby = FakeConn()
    standby.returns_all(T.SQL_DLQ_COUNTS, [("QUEUED", 9)])
    got = by_name(await T.read_pg_cluster(FakeSource(standby), leader=lambda: False, clock_ms=lambda: 0))
    assert not any(k[0] == "xtnl_dlq_rows" for k in got)                  # leader-only series


async def test_redis_reader_memory_and_feed() -> None:
    r = fakeredis.FakeAsyncRedis()
    memory = {"used_memory": 250, "maxmemory": 1_000}

    async def info(section: str) -> dict[str, int]:      # fakeredis has no INFO
        assert section == "memory"
        return memory
    r.info = info  # type: ignore[method-assign]
    try:
        got = by_name(await T.read_redis(r, leader=lambda: True, clock_ms=lambda: 0))
        assert got[("xtnl_redis_used_memory_ratio", ())] == 0.25
        assert got[("xtnl_feed_stream_length", ())] == 0
        assert ("xtnl_feed_pending_entries", ()) not in got                # no group yet: unknown
        await r.xgroup_create(K.FEED, FEED_GROUP, id="0", mkstream=True)
        await r.xadd(K.FEED, {"a": "1"}, id="5000-0")
        await r.xreadgroup(FEED_GROUP, "c1", {K.FEED: ">"}, count=10)
        got = by_name(await T.read_redis(r, leader=lambda: True, clock_ms=lambda: 15_000))
        assert got[("xtnl_feed_pending_entries", ())] == 1
        assert got[("xtnl_feed_oldest_pending_age_seconds", ())] == 10.0
        memory["maxmemory"] = 0
        standby = by_name(await T.read_redis(r, leader=lambda: False, clock_ms=lambda: 0))
        assert standby == {}                     # no limit: no ratio; standby: no feed series
    finally:
        await r.aclose()


class FakePatroni:
    async def cluster(self) -> list[PatroniMember]:
        return [PatroniMember("node-a", "leader", "running", "10.0.1.10", 5432, None, None, 3),
                PatroniMember("node-b", "sync_standby", "streaming", "10.0.2.10", 5432, None, 0, 3)]


class ArchConn:
    closed = False

    async def fetchrow(self, sql: str) -> tuple[int, float]:
        assert sql == T.SQL_ARCHIVER
        return (2, 31.5)

    async def close(self) -> None:
        ArchConn.closed = True


async def test_patroni_reader_members_and_local_archiver() -> None:
    dsns: list[str] = []

    async def connect(dsn: str) -> ArchConn:
        dsns.append(dsn)
        return ArchConn()
    got = by_name(await T.read_patroni(FakePatroni(), local_hosts=lambda: frozenset({"10.0.2.10"}),
                                       dsn="postgresql://u:pw@h1:5432,h2:5432/xtnl", connect=connect))
    assert got[("xtnl_patroni_member_running", (("member", "node-b"),))] == 1
    assert got[("xtnl_patroni_has_leader", ())] == 1
    assert got[("xtnl_patroni_sync_standby_count", ())] == 1
    assert got[("xtnl_pg_timeline", (("member", "node-a"),))] == 3
    assert got[("xtnl_wal_archive_failed_total", (("member", "node-b"),))] == 2      # the local member only
    assert got[("xtnl_wal_archive_age_seconds", (("member", "node-b"),))] == 31.5
    assert dsns and "10.0.2.10" in dsns[0] and ArchConn.closed


async def test_the_local_address_lookup_never_runs_on_the_event_loop() -> None:
    """Before the fix: read_patroni called local_hosts() (socket.getaddrinfo, a DNS lookup) on the event loop."""
    import threading
    loop_thread = threading.get_ident()
    threads: list[int] = []

    def local_hosts() -> frozenset[str]:
        threads.append(threading.get_ident())
        return frozenset({"10.0.2.10"})

    async def connect(dsn: str) -> ArchConn:
        return ArchConn()
    await T.read_patroni(FakePatroni(), local_hosts=local_hosts, dsn="postgresql://u@h1:5432/xtnl", connect=connect)
    assert threads and all(t != loop_thread for t in threads)


def test_etcd_series() -> None:
    st = EtcdClusterStatus((EtcdMemberStatus("node-a", "https://a:2379", True, T.ETCD_QUOTA_BYTES // 4, True),
                            EtcdMemberStatus("witness", "https://w:2379", False, None, None)), 1)
    got = by_name(T.etcd_samples(st))
    assert got[("xtnl_etcd_endpoint_healthy", (("member", "node-a"),))] == 1
    assert got[("xtnl_etcd_endpoint_healthy", (("member", "witness"),))] == 0
    assert got[("xtnl_etcd_db_quota_ratio", (("member", "node-a"),))] == 0.25
    assert got[("xtnl_etcd_has_leader", ())] == 1
    assert got[("xtnl_etcd_alarms_active", ())] == 1


def test_etcd_cluster_status_reads_every_endpoint_by_member_name() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        host, path = request.url.host, request.url.path
        if path == "/v3/cluster/member/list":
            return httpx.Response(200, json={"members": [
                {"name": "node-a", "clientURLs": ["https://a:2379"]},
                {"name": "witness", "clientURLs": ["https://w:2379"]}]})
        if path == "/v3/maintenance/alarm":
            return httpx.Response(200, json={"alarms": [{"alarm": "NOSPACE"}]})
        if path == "/v3/maintenance/status":
            if host == "w":
                return httpx.Response(503)
            return httpx.Response(200, json={"dbSize": "1024", "leader": "123"})
        return httpx.Response(404)
    client = HttpEtcdClient(["https://a:2379", "https://w:2379"], transport=httpx.MockTransport(handler))
    try:
        st = client.cluster_status(timeout_s=0.5)
    finally:
        client.close()
    assert st.alarms_active == 1
    assert [(m.member, m.healthy, m.db_size, m.has_leader) for m in st.members] == [
        ("node-a", True, 1024, True), ("witness", False, None, None)]


def test_disk_and_missing_certs(tmp_path: Path) -> None:
    got = by_name(T.disk_samples([str(tmp_path), str(tmp_path / "missing")]))
    assert list(got) == [("xtnl_disk_used_ratio", (("mount", str(tmp_path)),))]
    assert T.cert_samples({"ca": str(tmp_path / "none.crt")}, 0.0) == []           # unreadable: absent
    assert T.certs_from_env({"XTNL_ETCD_CERT": "/c", "XTNL_ETCD_CA": "/ca"}) == {"etcd-client": "/c", "ca": "/ca"}
    assert T.certs_from_env({"XTNL_METRICS_CERTS": "pg-server=/p, ca=/x"}) == {"pg-server": "/p", "ca": "/x"}


@dataclass
class Controller:
    role: NodeRole = NodeRole.STANDBY
    session: object = None
    last_record: HealthRecord | None = None


class TermSession:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    def metrics_samples(self, *, leader: bool) -> list[Sample]:
        self.calls.append(leader)
        return [Sample("xtnl_dlq_spool_ring_depth", 4)] + (
            [Sample("xtnl_bus_queue_depth", 1, {"topic": "market.price.dto"})] if leader else [])


def test_build_telemetry_renders_only_contract_series_and_gates_leader_values() -> None:
    ctl = Controller(last_record=rec("node-a", 1_000, hosts_patroni_primary=True))
    tel = T.build_telemetry(node_id="node-a", controller=ctl, keeper=Keeper(renew_latencies=[0.1]),
                            chrony_offset_s=lambda: 0.0005, clock_ms=lambda: 2_000, mounts=())
    text = tel.registry.render()
    assert 'xtnl_node_role{node="node-a",role="STANDBY"} 1' in text
    assert "xtnl_dlq_spool_ring_depth" in text and 'xtnl_dlq_spool_ring_depth{node="node-a"} 0' in text
    assert "xtnl_lease_renew_p99_seconds" not in text and "xtnl_leader_colocated_with_primary" not in text
    ctl.role, ctl.session = NodeRole.LEADER, TermSession()
    text = tel.registry.render()
    assert 'xtnl_leader_colocated_with_primary{node="node-a"} 1' in text
    assert 'xtnl_bus_queue_depth{node="node-a",topic="market.price.dto"} 1' in text
    assert 'xtnl_clock_offset_seconds{node="node-a"} 0.0005' in text
    for line in text.splitlines():
        if not line.startswith("#"):
            assert line.split("{", 1)[0] in CONTRACT
    assert tel.registry.dropped == 0


def test_both_nodes_risksweep_failure_pages() -> None:
    """Before 5.6: AbdicationMonitor only logged 'RiskSweep failing on both nodes' (no page)."""
    pages: list[tuple[str, str, str]] = []
    ledger = MemoryAbdicationLedger()
    ledger.record_risksweep_abdication(0)
    mon = AbdicationMonitor(ledger, pager=lambda *p: pages.append(p))
    for now in (0, 40_000, 80_000):
        own = rec("node-a", now, role="LEADER", risksweep_circuit_open_since_ms=now - 10 * 60_000)
        assert mon.observe(ClusterView(now, own, [], "node-a", None, False), {}) is None
    assert [p[:2] for p in pages] == [("critical", "RiskSweep failing on both nodes")]


def test_exposition_json_roundtrip_is_not_needed_but_values_are_finite() -> None:
    samples = T.role_samples(NodeRole.STANDBY)
    assert json.loads(json.dumps([s.value for s in samples]))
