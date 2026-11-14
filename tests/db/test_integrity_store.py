"""PgIntegrityDb / PgDlqStore over IntegrityEmulator (their exact SQL, emulated table semantics).

IntegrityDb results must match tests/integrity/fakes.FakeIntegrityDb for the same rows; DlqStore is checked
for lease-owner checks, fencing, lineage ordering and one full SM2 lineage run of DlqRecoveryWorker.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.cache.digest import fold
from src.db import integrity_store as I
from src.db.epoch import EngineDb, Epoch
from src.db.fence import EpochMismatch
from src.dto import AccountLive, InstrumentSpec
from src.integrity.dbcheck import check_row
from src.integrity.dlq_recovery import DlqOutcome, DlqRecoveryWorker
from src.integrity.ports import CleanMark, DlqStatus, StatefulDTO
from src.oversee.dayroll import trading_day
from tests.db.integrity_emulator import IntegrityEmulator
from tests.integrity.fakes import T0, FakeDlqBroker, FakeIntegrityDb, ManualClock, Pages, account, order, position

EPOCH = Epoch("cluster-a", 7)


def spec(symbol: str = "EURUSD", adapter_id: str = "stonex") -> InstrumentSpec:
    return InstrumentSpec(adapter_id=adapter_id, symbol=symbol, base_ccy=symbol[:3], quote_ccy=symbol[3:],
                          pnl_ccy=symbol[3:], contract_size=Decimal("1"), unit_kind="UNITS",
                          point_size=Decimal("0.0001"), min_qty=Decimal("1"), swap_booked_to_balance=True)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def emu(clock: ManualClock) -> IntegrityEmulator:
    return IntegrityEmulator(clock)


def seed(emu: IntegrityEmulator, fake: FakeIntegrityDb) -> None:
    items: list[tuple[StatefulDTO, int]] = [(order("B2", seq=1003), T0 + 20), (order("B1", seq=1001), T0 + 10), (position("P1", seq=1002), T0 + 10),
             (account("ACC", seq=1004), T0 + 30), (order("B9", adapter_id="ctrader", seq=1005), T0 + 40)]
    for item, at in items:
        emu.put(item, at)
        fake.put(item, at)
    emu.put(spec(), T0 + 15, seq_id=900)
    fake.put(spec(), T0 + 15, seq_id=900)


# ==================================================================================== IntegrityDb

async def test_integrity_reads_match_the_reference_fake(emu: IntegrityEmulator) -> None:
    fake = FakeIntegrityDb()
    seed(emu, fake)
    db = I.PgIntegrityDb(emu)
    for lo, hi, limit in ((T0, T0 + 100, 100), (T0 + 10, T0 + 20, 100), (T0, T0 + 100, 3), (T0 + 50, T0 + 60, 5)):
        assert list(await db.committed_rows(lo, hi, limit)) == list(await fake.committed_rows(lo, hi, limit))
    for after, limit in (("", 100), ("instrument:", 2), ("order:stonex:B1", 10), ("position:~", 5)):
        assert list(await db.rows_after_key(after, limit)) == list(await fake.rows_after_key(after, limit))
    keys = ["order:stonex:B1", "position:stonex:P1", "account:stonex:ACC", "instrument:stonex:EURUSD",
            "order:stonex:NOPE", "price:stonex:EURUSD", "garbage"]
    assert dict(await db.rows_by_keys(keys)) == dict(await fake.rows_by_keys(keys))
    assert await db.digest() == await fake.digest() == fold(r.digest_hash for r in fake.rows.values())
    assert await db.latest_commit_ms() == await fake.latest_commit_ms() == T0 + 40


async def test_rebuilt_rows_pass_step2_and_damage_is_visible(emu: IntegrityEmulator) -> None:
    emu.put(order("B1"), T0)
    emu.put(order("B2"), T0, checksum="0" * 64)
    db = I.PgIntegrityDb(emu)
    rows = {r.key: r for r in await db.committed_rows(T0, T0, 10)}
    good, bad = rows["order:stonex:B1"], rows["order:stonex:B2"]
    assert check_row(good) == [] and good.entity is not None and good.write().payload == good.payload
    assert {f.kind for f in check_row(bad)} == {"checksum", "digest_hash"}


async def test_row_that_cannot_rebuild_raises_corrupt_row(emu: IntegrityEmulator) -> None:
    row = list(emu.put(order("B1"), T0))
    row[10] = None   # ordered_units NULL without instrument_unknown: the engine never writes this
    emu.t.current["orders_state"]["order:stonex:B1"] = tuple(row)
    with pytest.raises(I.CorruptRowError) as info:
        await I.PgIntegrityDb(emu).rows_after_key("", 10)
    assert info.value.key == "order:stonex:B1"


async def test_empty_database(emu: IntegrityEmulator) -> None:
    db = I.PgIntegrityDb(emu)
    assert await db.digest() == "0" * 64 and await db.latest_commit_ms() is None
    assert await db.latest_halt_event() is None and await db.rows_by_keys([]) == {}
    assert await db.committed_rows(T0, T0 - 1, 10) == [] and await db.rows_after_key("", 0) == []


async def test_halt_high_water_marks_and_live_samples(emu: IntegrityEmulator) -> None:
    emu.t.halt_events = [(1, "SOFT"), (3, "HARD"), (2, "NONE")]
    emu.t.timeline, emu.t.max_seq_id = 4, 99
    emu.t.marks = [CleanMark("stonex", "USDJPY", Decimal("150"), Decimal("150.02"), T0, Decimal("2")),
                   CleanMark("stonex", "EURUSD", Decimal("1.1"), Decimal("1.1001"), T0 - 5)]
    emu.t.lives = [AccountLive(account_id="ACC", adapter_id="stonex", currency="USD", equity=Decimal("10"),
                               margin_used=None, free_margin=Decimal("9"), unrealised_pnl=Decimal("0"),
                               timestamp_ms=T0)]
    db = I.PgIntegrityDb(emu)
    halt = await db.latest_halt_event()
    assert halt is not None and (halt.id, halt.effective_level) == (3, "HARD")
    hw = await db.high_water()
    assert (hw.timeline, hw.epoch, hw.max_seq_id) == (4, 7, 99)
    assert [m.symbol for m in await db.last_clean_marks()] == ["EURUSD", "USDJPY"]
    lives = await db.latest_live_samples()
    assert len(lives) == 1 and lives[0].sampled and lives[0].equity == Decimal("10") and lives[0].margin_used is None


def test_split_key() -> None:
    assert I.split_key("order:stonex:B:1") == (I.ORDERS_CURRENT, "stonex", "B:1")
    assert I.split_key("instrument:stonex:EURUSD") == (I.INSTRUMENTS_CURRENT, "stonex", "EURUSD")
    assert I.split_key("price:stonex:EURUSD") is None and I.split_key("order:stonex") is None


def test_digest_sql_folds_four_64_bit_chunks() -> None:
    assert I.SQL_DIGEST.count("bit_xor") == 4 and I.SQL_DIGEST.count("UNION ALL") == len(I.CURRENT_TABLES) - 1
    for t in I.CURRENT_TABLES:
        assert f'COLLATE "C"' in t.sql_window and "ORDER BY committed_at_ms, k" in t.sql_window


# ==================================================================================== DlqStore

def store(emu: IntegrityEmulator) -> I.PgDlqStore:
    return I.PgDlqStore(EngineDb(emu, lambda: EPOCH))


def status(emu: IntegrityEmulator, dlq_id: int) -> str:
    return str(emu.t.dlq[dlq_id]["status"])


async def test_lease_due_rows_of_streaming_adapters_in_order(emu: IntegrityEmulator, clock: ManualClock) -> None:
    s = store(emu)
    a = emu.park("ORDER", "B1", "QueueFull")
    b = emu.park("ORDER", "B2", "QueueFull", adapter_id="ctrader")
    c = emu.park("ORDER", "B3", "QueueFull")
    emu.t.dlq[c]["next_retry_at_ms"] = T0 + 5_000            # not due yet
    emu.t.dlq[a]["next_retry_at_ms"] = T0 - 1                # due first
    got = await s.lease("w1", T0, 30_000, {"stonex"}, 10)
    assert [r.dlq_id for r in got] == [a]
    assert got[0].status is DlqStatus.LEASED and got[0].lease_owner == "w1" and got[0].lease_until_ms == T0 + 30_000
    assert status(emu, b) == "QUEUED" and status(emu, c) == "QUEUED"
    assert await s.lease("w1", T0, 30_000, set(), 10) == []
    assert await s.lease("w2", T0, 30_000, {"stonex"}, 10) == []           # already leased


async def test_every_row_update_checks_the_lease_owner(emu: IntegrityEmulator) -> None:
    s = store(emu)
    ids = [emu.park("ORDER", f"B{i}", "timeout") for i in range(4)]
    await s.lease("w1", T0, 30_000, {"stonex"}, 10)
    for dlq_id in ids:
        assert not await s.requeue(dlq_id, "w2") and not await s.resolve(dlq_id, "w2")
        assert not await s.backoff(dlq_id, "w2", 1, T0 + 1, "x") and not await s.quarantine(dlq_id, "w2", "x")
        assert not await s.merge_into_origin(dlq_id, "w2", ids[0], 1, T0)
        assert status(emu, dlq_id) == "LEASED"
    assert await s.requeue(ids[0], "w1") and status(emu, ids[0]) == "QUEUED"
    assert await s.resolve(ids[1], "w1") and status(emu, ids[1]) == "RESOLVED"
    assert await s.backoff(ids[2], "w1", 2, T0 + 4_000, "read failed")
    row = emu.t.dlq[ids[2]]
    assert (row["status"], row["retry_count"], row["next_retry_at_ms"], row["last_error"], row["lease_owner"]) == \
        ("BACKOFF", 2, T0 + 4_000, "read failed", None)
    assert await s.quarantine(ids[3], "w1", "converged after illegal_transition")
    assert not await s.resolve(ids[1], "w1")                                 # no longer leased


async def test_release_expired_keeps_retry_count(emu: IntegrityEmulator) -> None:
    s = store(emu)
    a = emu.park("ORDER", "B1", "timeout", retry_count=3)
    await s.lease("w1", T0, 1_000, {"stonex"}, 10)
    assert await s.release_expired(T0 + 1_000) == 0
    assert await s.release_expired(T0 + 1_001) == 1
    row = emu.t.dlq[a]
    assert (row["status"], row["retry_count"], row["lease_owner"]) == ("QUEUED", 3, None)


async def test_quarantine_without_owner_writes_events_quarantine(emu: IntegrityEmulator) -> None:
    s = store(emu)
    a = emu.park("CASH_FLOW", "T1", "schema_invalid", payload='{"account_id":"ACC","broker_ts":5}')
    b = emu.park("ACCOUNT", "ACC", "checksum", payload="not json")
    assert await s.quarantine(a, None, "invalid x5 (lineage)")
    assert await s.quarantine(b, None, "x")
    assert not await s.quarantine(a, None, "again")                          # already quarantined
    assert [(q[0], q[1], q[4], q[6]) for q in emu.t.quarantine] == [
        (a, "CASH_FLOW", "ACC", "schema_invalid: invalid x5 (lineage)"), (b, "ACCOUNT", "ACC", "checksum: x")]


async def test_lineage_lookup_orders_by_updated_at_and_skips_children(emu: IntegrityEmulator,
                                                                    clock: ManualClock) -> None:
    s = store(emu)
    old = emu.park("ORDER", "B1", "future_timestamp")
    newer = emu.park("ORDER", "B1", "future_timestamp")
    child = emu.park("ORDER", "B1", "future_timestamp")
    for dlq_id, upd in ((old, T0 + 300), (newer, T0 + 200), (child, T0 + 400)):
        emu.t.dlq[dlq_id].update(status="RESOLVED", updated_at_ms=upd)
    emu.t.dlq[child]["origin_dlq_id"] = old
    probe = emu.park("ORDER", "B1", "future_timestamp")
    found = await s.find_lineage_origin("ORDER", "B1", "future_timestamp", probe, T0, adapter_id="stonex")
    assert found is not None and found.dlq_id == old                        # newest updated_at, not newest id
    assert await s.find_lineage_origin("ORDER", "B1", "future_timestamp", probe, T0 + 301, adapter_id="stonex") is None
    assert await s.find_lineage_origin("ORDER", "B1", "checksum", probe, T0, adapter_id="stonex") is None
    assert await s.find_lineage_origin("ORDER", "B1", "future_timestamp", old, T0, adapter_id="stonex") is None     # dlq_id < before
    found = await s.find_lineage_origin("ORDER", "B1", "future_timestamp", child, T0, adapter_id="stonex")
    assert found is not None and found.dlq_id == old


async def test_merge_into_origin_is_one_transaction(emu: IntegrityEmulator) -> None:
    s = store(emu)
    origin = emu.park("ORDER", "B1", "checksum")
    emu.t.dlq[origin]["status"] = "RESOLVED"
    child = emu.park("ORDER", "B1", "checksum")
    await s.lease("w1", T0, 30_000, {"stonex"}, 10)
    assert await s.merge_into_origin(child, "w1", origin, 2, T0 + 2_000)
    assert (status(emu, child), emu.t.dlq[child]["origin_dlq_id"]) == ("RESOLVED", origin)
    assert (status(emu, origin), emu.t.dlq[origin]["retry_count"]) == ("BACKOFF", 2)
    # an origin that was quarantined meanwhile refuses the merge; the child's update is rolled back
    emu.t.dlq[origin]["status"] = "QUARANTINED"
    other = emu.park("ORDER", "B1", "checksum")
    await s.lease("w1", T0, 30_000, {"stonex"}, 10)
    assert not await s.merge_into_origin(other, "w1", origin, 3, T0)
    assert status(emu, other) == "LEASED" and emu.t.dlq[other]["origin_dlq_id"] is None


async def test_fenced_writes_raise_epoch_mismatch(emu: IntegrityEmulator) -> None:
    s = store(emu)
    a = emu.park("ORDER", "B1", "timeout")
    emu.epoch = 8                                                            # a newer leader bumped the epoch
    with pytest.raises(EpochMismatch):
        await s.lease("w1", T0, 30_000, {"stonex"}, 10)
    with pytest.raises(EpochMismatch):
        await s.release_expired(T0)
    with pytest.raises(EpochMismatch):
        await s.resolve(a, "w1")
    with pytest.raises(EpochMismatch):
        await s.mark_transfers_unverified("stonex", "ACC", T0)
    assert status(emu, a) == "QUEUED"


async def test_cash_flow_since_is_min_of_committed_and_parked(emu: IntegrityEmulator) -> None:
    s = store(emu)
    assert await s.cash_flow_since_ms("stonex") is None
    emu.t.cash_flow_ts["stonex"] = [T0 - 50, T0 - 10]
    assert await s.cash_flow_since_ms("stonex") == T0 - 10                  # last committed broker_ts
    emu.park("CASH_FLOW", "T1", "QueueFull", payload=f'{{"broker_ts": {T0 - 99}}}')
    emu.park("CASH_FLOW", "T2", "schema_invalid", payload="{broken")
    resolved = emu.park("CASH_FLOW", "T3", "QueueFull", payload=f'{{"broker_ts": {T0 - 999}}}')
    emu.t.dlq[resolved]["status"] = "RESOLVED"
    emu.park("CASH_FLOW", "T4", "QueueFull", adapter_id="ctrader", payload=f'{{"broker_ts": {T0 - 5000}}}')
    assert await s.cash_flow_since_ms("stonex") == T0 - 99


async def test_mark_transfers_unverified_flags_the_current_trading_day(emu: IntegrityEmulator) -> None:
    await store(emu).mark_transfers_unverified("stonex", "ACC", T0)
    assert list(emu.t.sod_flags) == [("stonex", "ACC", trading_day(T0))]


async def test_dlq_worker_lineage_runs_on_the_postgres_store(emu: IntegrityEmulator, clock: ManualClock) -> None:
    """The SM2 deterministic-lineage scenario of tests/integrity, through PgDlqStore."""
    broker, pages = FakeDlqBroker(), Pages()
    broker.entities["B1"] = order("B1")
    w = DlqRecoveryWorker(store(emu), broker, clock_ms=clock, pager=pages, owner="w1", jitter=lambda: 0.0)
    origin = emu.park("ORDER", "B1", "missing_client_order_id")
    await w.run_once()
    assert status(emu, origin) == "RESOLVED"
    for n in range(1, 5):
        clock.advance(1)
        again = emu.park("ORDER", "B1", "missing_client_order_id")
        assert (again, DlqOutcome.MERGED) in await w.run_once()
        assert emu.t.dlq[again]["origin_dlq_id"] == origin and emu.t.dlq[origin]["retry_count"] == n
        if n < 4:
            clock.advance(60_000)
            assert (origin, DlqOutcome.RESOLVED) in await w.run_once()
    clock.advance(60_000)
    await w.run_once()
    clock.advance(1)
    emu.park("ORDER", "B1", "missing_client_order_id")
    await w.run_once()
    assert status(emu, origin) == "QUARANTINED" and "dlq_quarantined" in pages.codes()
    assert emu.t.quarantine and emu.t.quarantine[-1][0] == origin


async def test_lineage_lookup_and_merge_are_adapter_scoped(emu: IntegrityEmulator) -> None:
    """dlq-lineage-not-adapter-scoped: entity ids are unique only per adapter."""
    s = store(emu)
    stonex = emu.park("ORDER", "B1", "schema_invalid")
    emu.t.dlq[stonex]["status"] = "RESOLVED"
    probe = emu.park("ORDER", "B1", "schema_invalid", adapter_id="ctrader")
    assert await s.find_lineage_origin("ORDER", "B1", "schema_invalid", probe, T0, adapter_id="ctrader") is None
    found = await s.find_lineage_origin("ORDER", "B1", "schema_invalid", probe, T0, adapter_id="stonex")
    assert found is not None and found.dlq_id == stonex
    # a merge across adapters is refused and rolled back (the child keeps its lease, the origin its count)
    await s.lease("w1", T0, 30_000, {"ctrader"}, 10)
    assert not await s.merge_into_origin(probe, "w1", stonex, 1, T0)
    assert status(emu, probe) == "LEASED" and emu.t.dlq[probe]["origin_dlq_id"] is None
    assert (status(emu, stonex), emu.t.dlq[stonex]["retry_count"]) == ("RESOLVED", 0)
