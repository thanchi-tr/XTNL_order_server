"""Health record checks, preferences, wire form and the etcd board (operations.md §4)."""
from __future__ import annotations

from src.runtime.etcd import HEALTH_PREFIX
from src.runtime.health import HealthBoard, HealthRecord
from tests.runtime.fakes import FakeClock, FakeEtcd, record


def test_hard_checks() -> None:
    now = 1_000_000
    assert record(ts_ms=now).passes_hard_checks(now)
    assert record(ts_ms=now, pg_writable=False).failed_hard_checks(now, as_leader=False) == ["pg_writable"]
    assert record(ts_ms=now, chrony_offset_ms=51.0).failed_hard_checks(now, as_leader=False) == ["chrony"]
    assert record(ts_ms=now, chrony_offset_ms=None).failed_hard_checks(now, as_leader=False) == ["chrony"]
    sweep = record(ts_ms=now, risksweep_circuit_open_since_ms=now - 5 * 60_000 - 1)
    assert sweep.passes_hard_checks(now) and not sweep.passes_hard_checks(now, as_leader=True)
    assert record(ts_ms=now, risksweep_circuit_open_since_ms=now - 60_000).passes_hard_checks(now, as_leader=True)


def test_freshness_backoff_and_eligibility() -> None:
    now = 1_000_000
    assert record(ts_ms=now - 5000).fresh(now) and not record(ts_ms=now - 5001).fresh(now)
    assert record(ts_ms=now).eligible_target(now)
    assert not record(ts_ms=now, backoff_until_ms=now + 1).eligible_target(now)
    assert not record(ts_ms=now, safe_hold=True).eligible_target(now)
    assert not record(ts_ms=now - 6000).eligible_target(now)


def test_preference_score_ignores_closed_markets() -> None:
    a = record(broker_tls={"stonex": True, "pepper": None}, chrony_offset_ms=0.2, hosts_patroni_primary=True)
    b = record(broker_tls={"stonex": True, "pepper": False}, chrony_offset_ms=2.0)
    assert a.preference_score() == (1, 1, 1) and b.preference_score() == (1, 0, 0)


def test_json_roundtrip() -> None:
    r = record(ts_ms=5, broker_tls={"stonex": True, "pepper": None}, spill_bytes=12, backoff_until_ms=9,
               risksweep_circuit_open_since_ms=3)
    assert HealthRecord.from_json(r.to_json()) == r


def test_board_publishes_under_own_lease_and_reads_peers() -> None:
    clk = FakeClock()
    etcd = FakeEtcd(clk)
    me, peer = HealthBoard(etcd, "n1"), HealthBoard(etcd, "n2")
    me.publish(record("n1", 1))
    peer.publish(record("n2", 2))
    me.publish(record("n1", 3))                       # keepalive, same lease
    assert etcd.kv[HEALTH_PREFIX + "n1"].lease_id != etcd.kv[HEALTH_PREFIX + "n2"].lease_id
    assert [p.node for p in me.peers()] == ["n2"]
    clk.advance(11)                                   # n1's health lease expires
    me.publish(record("n1", 4))                       # re-granted
    assert HealthRecord.from_json(etcd.kv[HEALTH_PREFIX + "n1"].value).ts_ms == 4
    etcd.put(HEALTH_PREFIX + "bad", "not json", timeout_s=1)
    assert [p.node for p in me.peers()] == []         # n2 expired; garbage skipped
