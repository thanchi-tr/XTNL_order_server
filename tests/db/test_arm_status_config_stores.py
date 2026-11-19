"""S24: the unfenced `arm_status` upsert, the config cache / audit and the read-only operator rows."""
from __future__ import annotations

import pytest

from src.db import arm_config_store as C
from src.db import arm_status_store as S
from src.db.arm_ports import AcceptedRow, ArmStatusRow, ConfigAuditRow, ConfigCacheRow, NodeMdRecordRow
from src.db.epoch import EngineDb, Epoch
from src.db.fence import FENCE, EpochMismatch
from tests.db.arm_emulator import ArmEmulator


@pytest.fixture
def emu() -> ArmEmulator:
    return ArmEmulator(epoch=7)


@pytest.fixture
def db(emu: ArmEmulator) -> EngineDb:
    return EngineDb(emu, lambda: Epoch("c", 7))


def status(**kw: object) -> ArmStatusRow:
    base: dict[str, object] = dict(node="n1", epoch=7, updated_at_ms=100, heartbeat_at_ms=100, leader=True,
                                   running=True, armed_positions=2, failed_scopes=1,
                                   counters=(("series_invalid", 1), ("unresolved", 0)))
    base.update(kw)
    return ArmStatusRow(**base)  # type: ignore[arg-type]


async def test_status_is_unfenced_and_only_moves_forward(emu: ArmEmulator) -> None:
    store = S.PgArmStatusStore(emu)
    emu.epoch = 99                                         # a fenced-out node still reports itself
    assert await store.upsert(status()) is True
    assert await store.upsert(status(updated_at_ms=90, running=False)) is False
    assert await store.upsert(status(updated_at_ms=110, paused=True, pause_reason="operator")) is True
    [row] = await store.all()
    assert row.paused and row.pause_reason == "operator" and row.counters == (("series_invalid", 1),
                                                                               ("unresolved", 0))
    assert FENCE not in S.SQL_STATUS_UPSERT


SENTINEL = "abc123:clusterA:n1:5.2"


async def test_md_record_is_kept_apart_from_the_status_upsert(emu: ArmEmulator) -> None:
    """§A7 / S31: the node's market-data record lives in its arm_status row; it only moves forward on
    last_write_ms and the status upsert never overwrites it (nor it the status)."""
    store = S.PgArmStatusStore(emu)
    emu.epoch = 99                                          # unfenced, like the status row
    assert await store.load_md_record("n1") is None
    rec = NodeMdRecordRow(node="n1", sentinel=SENTINEL, last_write_ms=1_000)
    assert await store.save_md_record(rec) is True          # before any status write: creates the row
    assert await store.upsert(status()) is True             # updated_at_ms 100 > 0: the status applies
    assert await store.load_md_record("n1") == rec
    assert await store.save_md_record(NodeMdRecordRow(node="n1", sentinel="abc123:clusterA:n1:5.1",
                                                      last_write_ms=900)) is False   # older: refused
    newer = NodeMdRecordRow(node="n1", sentinel="abc123:clusterA:n1:5.3", last_write_ms=1_000)
    assert await store.save_md_record(newer) is True        # equal time, new sentinel
    assert await store.upsert(status(updated_at_ms=120)) is True
    assert await store.load_md_record("n1") == newer
    [row] = await store.all()
    assert row.updated_at_ms == 120 and row.armed_positions == 2
    assert FENCE not in S.SQL_MD_RECORD_SAVE and FENCE not in S.SQL_MD_RECORD_GET


@pytest.mark.parametrize("bad", [
    NodeMdRecordRow(node="n1", sentinel="no-colons", last_write_ms=1),
    NodeMdRecordRow(node="n1", sentinel="abc:c:n1:5", last_write_ms=1),
    NodeMdRecordRow(node="n1", sentinel="ab c:c:n1:5.1", last_write_ms=1),
    NodeMdRecordRow(node="n1", sentinel=SENTINEL, last_write_ms=-1),
])
async def test_md_record_holds_ids_and_times_only(emu: ArmEmulator, bad: NodeMdRecordRow) -> None:
    with pytest.raises(ValueError):
        await S.PgArmStatusStore(emu).save_md_record(bad)


@pytest.mark.parametrize("counters", [(("Bad", 1),), (("x", True),), (("x", 1.5),)])
async def test_status_counters_are_codes_with_integer_counts(emu: ArmEmulator,
                                                             counters: tuple[tuple[str, object], ...]) -> None:
    with pytest.raises(ValueError):
        await S.PgArmStatusStore(emu).upsert(status(counters=counters))


async def test_config_cache_and_audit_are_fenced(emu: ArmEmulator, db: EngineDb) -> None:
    store = C.PgArmConfigStore(db)
    assert await store.cached() is None
    cache = ConfigCacheRow(doc_hash="b" * 64, schema_version=1, document_json='{"assignments": []}',
                           row_hashes=(("assignments:1", "c" * 64),), chain_head="d" * 64, chain_rows=1,
                           updated_at_ms=10)
    assert await store.save_cache(cache) is True
    assert await store.cached() == cache
    assert await store.audit(ConfigAuditRow(at_ms=10, node="n1", action="installed", doc_hash="b" * 64,
                                            row_refs=("assignments:1:" + "c" * 64,))) is True
    emu.epoch = 8
    with pytest.raises(EpochMismatch):
        await store.save_cache(cache)
    with pytest.raises(EpochMismatch):
        await store.audit(ConfigAuditRow(at_ms=11, node="n1", action="installed"))
    assert len(emu.t.config_audit) == 1


async def test_accepted_and_controls_are_read_only(emu: ArmEmulator, db: EngineDb) -> None:
    store = C.PgArmConfigStore(db)
    assert await store.latest_accepted() is None
    emu.t.accepted += [(1, "a" * 64, 1, "manual", 10, "op.one"), (2, "b" * 64, 1, "manual", 20, "op.one")]
    assert await store.latest_accepted() == AcceptedRow(accepted_id=2, doc_hash="b" * 64, schema_version=1,
                                                        mode="manual", accepted_at_ms=20, accepted_by="op.one")
    controls = await store.controls()
    assert controls is not None and controls.paused is False and controls.drain_notices is False
    emu.t.controls = None
    assert await store.controls() is None                  # unreadable: the caller holds, never a pause
    for sql in (C.SQL_ACCEPTED_LATEST, C.SQL_CONTROLS_GET):
        assert sql.startswith("SELECT")
