"""ClusterControl: cluster_state reads, the durable abdication ledger (fenced writes), the etcd SAFE_HOLD
mirror and leader_terms (src/runtime/cluster.py, src/db/cluster_state.py). Also the pg ports helpers."""
from __future__ import annotations

import json
from datetime import date
from typing import Any

import pytest

from src.db import cluster_state as C
from src.db.epoch import Epoch
from src.db.fence import EpochMismatch
from src.runtime.cluster import ClusterControl, mirror_json, parse_mirror
from src.runtime.etcd import SAFE_HOLD_KEY
from src.runtime.pg_ports import (
    SQL_LATEST_CALENDAR,
    SQL_SOD_MISSING,
    SQL_SOD_STATUS,
    GuardedEngineDb,
    LazyPgSource,
    PgScheduleLoader,
    PgSodLedger,
    safe_redact,
)
from src.runtime.roles import ClusterState
from tests.db.conftest import EPOCH, FakeConn, FakeSource
from tests.runtime.fakes import FakeClock, FakeEtcd


class BrokenSource:
    def acquire(self) -> Any:
        raise ConnectionRefusedError("postgres down")


def row(safe: bool = False, abdications: Any = None, risksweep_at: int | None = None) -> tuple[Any, ...]:
    return (safe, 10 if safe else None, "3 abnormal" if safe else None, None, None, risksweep_at,
            abdications if abdications is not None else {})


async def test_read_from_postgres_and_ledger_view() -> None:
    conn = FakeConn()
    conn.returns(C.SQL_READ_CLUSTER, row(True, json.dumps({"stonex": 5}), risksweep_at=7))
    cc = ClusterControl(FakeSource(conn), epoch=lambda: EPOCH)
    cs = await cc.read()
    assert cs == ClusterState(True, 10, "3 abnormal", None) and cc.source_of_last == "postgres"
    assert cc.broker_blocked("stonex") and not cc.broker_blocked("pepperstone")
    assert cc.risksweep_abdicated()


async def test_read_falls_back_to_etcd_then_last_known() -> None:
    etcd = FakeEtcd(FakeClock())
    cc = ClusterControl(BrokenSource(), epoch=lambda: None, etcd=etcd)
    assert await cc.read() == ClusterState() and cc.source_of_last == "last_known"
    etcd.put(SAFE_HOLD_KEY, mirror_json(ClusterState(True, 5, "flap", None)), timeout_s=1)
    cs = await cc.read()
    assert cs.safe_hold and cs.safe_hold_reason == "flap" and cc.source_of_last == "etcd"
    etcd.down = True
    assert (await cc.read()).safe_hold and cc.source_of_last == "last_known"
    assert parse_mirror(mirror_json(cs)) == cs


async def test_broker_abdication_is_written_fenced_and_cleared_when_streaming() -> None:
    conn = FakeConn()
    cc = ClusterControl(FakeSource(conn), epoch=lambda: EPOCH)
    cc.record_broker_abdication("stonex", 1_000)
    assert cc.broker_blocked("stonex")
    await cc.flush()
    assert conn.args_of(C.SQL_BROKER_ABDICATED) == [(EPOCH.token, "stonex", 1_000)]
    # a read before the clear is written still reports the local change
    conn.returns(C.SQL_READ_CLUSTER, row(abdications={"stonex": 1_000}))
    await cc.read()
    cc.adapter_streaming("stonex")
    assert not cc.broker_blocked("stonex")
    cc.adapter_streaming("stonex")                    # already clear: no second write
    await cc.flush()
    assert conn.args_of(C.SQL_BROKER_STREAMING) == [(EPOCH.token, "stonex")]
    conn.returns(C.SQL_READ_CLUSTER, row())
    await cc.read()
    assert not cc.broker_blocked("stonex") and cc.write_failures == 0


async def test_risksweep_incident_recorded_once_and_cleared() -> None:
    conn = FakeConn()
    cc = ClusterControl(FakeSource(conn), epoch=lambda: EPOCH)
    cc.record_risksweep_abdication(42)
    assert cc.risksweep_abdicated()
    cc.risksweep_recovered()
    cc.risksweep_recovered()
    await cc.flush()
    assert conn.args_of(C.SQL_RISKSWEEP_ABDICATED) == [(EPOCH.token, "risksweep:42", 42)]
    assert len(conn.args_of(C.SQL_RISKSWEEP_RECOVERED)) == 1


async def test_lost_fence_or_no_epoch_writes_nothing() -> None:
    conn = FakeConn()
    conn.stored_epoch = 99                           # another leader bumped the epoch
    cc = ClusterControl(FakeSource(conn), epoch=lambda: EPOCH)
    cc.record_broker_abdication("stonex", 1)
    await cc.flush()
    assert cc.write_failures == 1
    idle = ClusterControl(FakeSource(FakeConn()), epoch=lambda: None)
    idle.record_broker_abdication("stonex", 1)
    await idle.flush()
    assert idle.broker_blocked("stonex") and idle.write_failures == 0


async def test_safe_hold_mirror_writes_on_change_only() -> None:
    conn = FakeConn()
    etcd = FakeEtcd(FakeClock())
    cc = ClusterControl(FakeSource(conn), epoch=lambda: EPOCH, etcd=etcd)
    conn.returns(C.SQL_READ_CLUSTER, row(True), row(True), row(False))
    assert await cc.mirror_safe_hold()
    assert not await cc.mirror_safe_hold()
    assert await cc.mirror_safe_hold()
    kv = etcd.get(SAFE_HOLD_KEY, timeout_s=1)
    assert kv is not None and not parse_mirror(kv.value).safe_hold


async def test_own_terms() -> None:
    conn = FakeConn()
    conn.returns_all(C.SQL_OWN_TERMS, [("c1", 7, "node-a", 100, 150, None, None), ("c1", 6, "node-a", 50, None, 90,
                                                                                    "init_failed")])
    terms = await ClusterControl(FakeSource(conn), epoch=lambda: None).own_terms("node-a")
    assert [t.epoch_revision for t in terms] == [7, 6] and terms[1].end_reason == "init_failed"
    assert conn.args_of(C.SQL_OWN_TERMS) == [("node-a",)]


async def test_guarded_db_reports_a_lost_fence() -> None:
    conn = FakeConn()
    conn.stored_epoch = 99
    lost: list[BaseException] = []
    db = GuardedEngineDb(FakeSource(conn), lambda: EPOCH, lost.append)
    with pytest.raises(EpochMismatch):
        async with db.transaction() as tx:
            await C.record_broker_abdication(tx, "x", 1)
    assert len(lost) == 1


async def test_schedule_loader_skips_bad_rows() -> None:
    conn = FakeConn()
    good = {"sessions": [{"open": {"day": "SUN", "time": "17:05"}, "close": {"day": "FRI", "time": "16:55"}}]}
    conn.returns_all(SQL_LATEST_CALENDAR, [(3, "stonex", "EURUSD", "America/New_York", json.dumps(good), "[]"),
                                           (3, "stonex", "BAD", "Mars/Olympus", good, [])])
    loader = PgScheduleLoader(FakeSource(conn))
    sched = await loader.load()
    assert sched.version == 3 and list(sched.symbols) == [("stonex", "EURUSD")] and loader.skipped == ["stonex:BAD"]
    empty = await PgScheduleLoader(FakeSource(FakeConn())).load()
    assert empty.version == 0 and not empty.symbols


async def test_sod_ledger() -> None:
    conn = FakeConn()
    conn.returns_all(SQL_SOD_MISSING, [("A1",), ("A2",)])
    conn.returns(SQL_SOD_STATUS, (2, True))
    ledger = PgSodLedger(FakeSource(conn))
    day = date(2026, 11, 13)
    assert await ledger.missing_accounts(day) == ["A1", "A2"]
    st = await ledger.status(day, ["A1", "A2"])
    assert st.rows == 2 and st.partial
    assert conn.args_of(SQL_SOD_STATUS) == [(day, ["A1", "A2"])]


async def test_lazy_source_connects_on_first_use_only() -> None:
    created: list[dict[str, Any]] = []

    class Pool:
        def __init__(self) -> None:
            self.conn = FakeConn()
            self.closed = False

        def acquire(self) -> Any:
            return FakeSource(self.conn).acquire()

        async def close(self) -> None:
            self.closed = True

    pool = Pool()

    async def factory(dsn: str, **kw: Any) -> Any:
        created.append(kw)
        return pool

    dsn = "postgresql://u:secret-pw@h1:5432,h2:5432/db"
    src = LazyPgSource(dsn, application_name="xtnl:7:n", factory=factory)
    assert not src.created and "secret-pw" not in repr(src)
    async with src.acquire() as c:
        assert c is pool.conn
    async with src.acquire():
        pass
    assert len(created) == 1 and created[0]["application_name"] == "xtnl:7:n"
    await src.close()
    assert pool.closed
    with pytest.raises(ConnectionError):
        async with src.acquire():
            pass
    assert safe_redact(dsn) == "postgresql://u:***@h1:5432,h2:5432/db"
    assert "secret" not in safe_redact("postgresql://u:secret@h1:5432/db")


def test_epoch_type_is_shared() -> None:
    assert isinstance(EPOCH, Epoch)
