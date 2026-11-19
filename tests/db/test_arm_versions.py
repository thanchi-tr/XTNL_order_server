"""S24: `event_log` reads for entry time and the `S_at_T` candidates (design §B6.4, §B7; R11-M06)."""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

import pytest

from src.db import arm_versions as V
from src.db import epoch as E
from src.db.epoch import EngineDb, Epoch
from src.riskmgr.stopbasis import STOP_CHANGED_AFTER_BAR_END, StopAtT, StopUnavailable, stop_at_bar_end
from src.riskmgr.types import StopBasis
from tests.db.arm_emulator import ArmEmulator
from tests.db.arm_rows import to_stop_version

T = 10_000_000
PK = "position:syn:P1"
OK = "order:syn:O1"


@pytest.fixture
def emu() -> ArmEmulator:
    return ArmEmulator()


@pytest.fixture
def reader(emu: ArmEmulator) -> V.PgArmVersionReader:
    return V.PgArmVersionReader(EngineDb(emu, lambda: Epoch("c", 7)))


def child(level: str, status: str = "WORKING", units: str = "2") -> dict[str, Any]:
    return {"status": status, "entry_price": level, "remaining_units": units, "order_kind": "STOP"}


def at_t(versions: list[Any], bar_end_ms: int = T) -> StopAtT | StopUnavailable:
    return stop_at_bar_end([to_stop_version(v) for v in versions], side="BUY", entry=Decimal("10"),
                           units_to_cover=Decimal("2"), bar_end_ms=bar_end_ms)


# ---- SQL shape ------------------------------------------------------------------------------------------------------
def test_counting_query_excludes_history_and_dropped_stale() -> None:
    sql = V.SQL_STOP_COUNTING
    assert "action = 'APPLIED'" in sql and "broker_time_ms <= $3::bigint" in sql
    origins = set(re.findall(r"'([a-z_]+)'", sql.split("origin IN", 1)[1].split(")", 1)[0]))
    assert origins == {"stream", "stop_read", "snapshot", "dlq"}
    assert "history" not in sql and "DROPPED_STALE" not in sql
    for marker in (V.SQL_STOP_NONCOUNTING, V.SQL_STOP_READ_AFTER):
        assert "action = 'APPLIED'" in marker
    # round 14 (S38g): the newer non-counting versions carry their level (order state) for the direction rule; the
    # stop_read-after-T marker carries none
    assert "stop_loss" in V.SQL_STOP_NONCOUNTING and "entry_price" in V.SQL_STOP_NONCOUNTING
    assert "stop_loss" not in V.SQL_STOP_READ_AFTER and "entry_price" not in V.SQL_STOP_READ_AFTER
    for sql in (V.SQL_STOP_COUNTING, V.SQL_STOP_NONCOUNTING, V.SQL_STOP_READ_AFTER, V.SQL_ENTRY_STREAM,
                V.SQL_ENTRY_ORDER_FILL):
        assert sql.lstrip("(").startswith("SELECT") and "entity_type" in sql and "entity_key" in sql


def test_entry_queries_never_use_placement_or_watermark_versions() -> None:
    assert "broker_time_ms IS NOT NULL" in V.SQL_ENTRY_STREAM and "origin = 'stream'" in V.SQL_ENTRY_STREAM
    assert "'OPEN'" in V.SQL_ENTRY_STREAM
    fill = V.SQL_ENTRY_ORDER_FILL
    assert "broker_time_ms IS NOT NULL" in fill and "'FILLED', 'PARTIALLY_FILLED'" in fill
    assert "PENDING" not in fill and "WORKING" not in fill


# ---- entry time ------------------------------------------------------------------------------------------------------
async def test_entry_time_prefers_the_first_stream_open_version(emu: ArmEmulator,
                                                                reader: V.PgArmVersionReader) -> None:
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=3, payload={"status": "OPEN"}, origin="stream",
                    broker_time_ms=5_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=4, payload={"status": "OPEN"}, origin="stream",
                    broker_time_ms=6_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2, payload={"status": "OPEN"}, origin="snapshot",
                    broker_time_ms=4_000)
    got = await reader.entry_time("syn", "P1", "E1")
    assert got is not None and (got.source, got.broker_time_ms, got.origin) == ("STREAM", 5_000, "stream")


async def test_stop_entry_placed_hours_before_its_fill_uses_the_fill(emu: ArmEmulator,
                                                                     reader: V.PgArmVersionReader) -> None:
    """A stop entry placed at t0 and filled hours later, seen only through history and a watermark-labelled
    position read: the entry time is the fill, never the placement."""
    ek = "order:syn:E1"
    emu.add_version(entity_type="ORDER", entity_key=ek, seq_id=1, payload={"status": "WORKING"}, origin="history",
                    broker_time_ms=1_000)
    emu.add_version(entity_type="ORDER", entity_key=ek, seq_id=2, payload={"status": "FILLED"}, origin="history",
                    broker_time_ms=1_000 + 4 * 3_600_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=3, payload={"status": "OPEN"}, origin="snapshot",
                    broker_time_ms=None)
    got = await reader.entry_time("syn", "P1", "E1")
    assert got is not None and (got.source, got.broker_time_ms) == ("BROKER", 1_000 + 4 * 3_600_000)
    assert await reader.entry_time("syn", "P1", None) is None                       # caller: ESTIMATED


# ---- S_at_T ------------------------------------------------------------------------------------------------------
async def test_golden_amended_then_cancelled_while_off(emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    """The stop was amended to 9.5 and then cancelled while the engine was off. The catch-up sees only the StoneX
    history mapping of the cancelled order (`src/adapters/stonex/history.py`): a placement version stamped at
    `CreatedDateTimeUtc` with a working status, and the terminal CANCELLED version stamped at the last change, both
    carrying the last level (9.5). The history level never counts: PRIOR_VERSION 9.0."""
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=1, payload=child("9.0"), origin="stream",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=2, payload=child("9.5"),          # placement (Created)
                    origin="history", broker_time_ms=T - 120_000 + 1)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=3, payload=child("9.5", "CANCELLED"),
                    origin="history", broker_time_ms=T - 30_000)                       # terminal (LastChanged)
    versions = await reader.stop_versions("syn", "P1", ["O1"], T)
    assert {v.level for v in versions if v.level is not None and v.counts} == {Decimal("9.0")}
    got = at_t(versions)        # the history level 9.5 is nearer entry: the farther prior level 9.0 is tested
    assert isinstance(got, StopAtT) and got.level == Decimal("9.0") and got.basis is StopBasis.PRIOR_VERSION


async def test_the_reads_of_one_call_share_one_snapshot(emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    """Every statement of `stop_versions` (POSITION and ORDER groups) and of `entry_time` runs inside one READ ONLY
    REPEATABLE READ transaction: a version committed between two reads is seen by none of them."""
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload={"status": "OPEN", "stop_loss": "9.1",
                    "units": "2"}, origin="stream", broker_time_ms=T - 60_000)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=2, payload=child("9.0"), origin="stream",
                    broker_time_ms=T - 50_000)
    seq = iter(range(100, 200))

    def concurrent_commit(sql: str) -> None:
        if sql.startswith(("SELECT", "(SELECT")):
            n = next(seq)
            emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=n, payload=child("9.9"), origin="history",
                            broker_time_ms=T - 1_000)
            emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=n + 50, payload={"status": "OPEN"},
                            origin="stream", broker_time_ms=1)

    emu.after_statement = concurrent_commit
    emu.sql.clear()
    versions = await reader.stop_versions("syn", "P1", ["O1"], T)
    assert [v.seq_id for v in versions] == [2, 1]                    # no marker from a version committed mid-call
    reads = [V.SQL_STOP_COUNTING, V.SQL_STOP_NONCOUNTING, V.SQL_STOP_READ_AFTER] * 2
    assert emu.sql == [E.SQL_BEGIN_SNAPSHOT, V.SQL_LINKED_STOP_CHILDREN, *reads, E.SQL_ROLLBACK]


async def test_entry_time_reads_share_one_snapshot(emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    """No stream OPEN version yet; a fill of the entry order committed after the first read is not seen by the
    second (the next call, a new snapshot, sees it)."""
    def concurrent_fill(sql: str) -> None:
        if sql == V.SQL_ENTRY_STREAM:
            emu.add_version(entity_type="ORDER", entity_key="order:syn:E1", seq_id=5, payload={"status": "FILLED"},
                            origin="stream", broker_time_ms=7_000)

    emu.after_statement = concurrent_fill
    assert await reader.entry_time("syn", "P1", "E1") is None
    assert emu.sql == [E.SQL_BEGIN_SNAPSHOT, V.SQL_ENTRY_STREAM, V.SQL_ENTRY_ORDER_FILL, E.SQL_ROLLBACK]
    emu.after_statement = None
    got = await reader.entry_time("syn", "P1", "E1")
    assert got is not None and (got.source, got.broker_time_ms) == ("BROKER", 7_000)


async def test_dropped_stale_versions_are_never_read(emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=1, payload=child("9.0"), origin="stream",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=2, payload=child("9.8"), origin="stream",
                    broker_time_ms=T - 10_000, action="DROPPED_STALE")
    versions = await reader.stop_versions("syn", "P1", ["O1"], T)
    assert [v.seq_id for v in versions] == [1]
    got = at_t(versions)
    assert isinstance(got, StopAtT) and got.level == Decimal("9.0") and got.basis is StopBasis.STREAM


async def test_only_a_history_version_is_unavailable(emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=1, payload=child("9.5"), origin="history",
                    broker_time_ms=T - 10_000)
    versions = await reader.stop_versions("syn", "P1", ["O1"], T)
    assert not any(v.counts for v in versions)
    assert isinstance(at_t(versions), StopUnavailable)


async def test_position_field_watermark_read_is_a_noncounting_newest(emu: ArmEmulator,
                                                                     reader: V.PgArmVersionReader) -> None:
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload={"status": "OPEN", "stop_loss": "9.1",
                    "units": "2"}, origin="stream", broker_time_ms=T - 60_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2, payload={"status": "OPEN", "stop_loss": "9.6",
                    "units": "2"}, origin="snapshot", broker_time_ms=None)
    versions = await reader.stop_versions("syn", "P1", [], T)
    assert [(v.handle_kind, v.seq_id, v.level) for v in versions] == [
        ("POSITION_FIELD", 1, Decimal("9.1")), ("POSITION_FIELD", 2, Decimal("9.6"))]
    got = at_t(versions)        # 9.6 is nearer entry (a tightening): the farther prior level 9.1 is tested
    assert isinstance(got, StopAtT) and got.level == Decimal("9.1") and got.basis is StopBasis.PRIOR_VERSION


async def test_golden_a_loosened_amend_seen_only_through_an_untimed_rest_row(emu: ArmEmulator,
                                                                           reader: V.PgArmVersionReader) -> None:
    """Round 14 (plan S38g): the user loosened the stop 9.1 -> 8.6 before `T` and the engine saw the amend only
    through a watermark-labelled REST read (no broker time). The prior counting level is 9.1; the read returns the
    untimed level with the marker, and `S_at_T` is the farther from entry (8.6): a loosened stop is never tested at a
    smaller risk."""
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload={"status": "OPEN", "stop_loss": "9.1",
                    "units": "2"}, origin="stream", broker_time_ms=T - 60_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2, payload={"status": "OPEN", "stop_loss": "8.6",
                    "units": "2"}, origin="snapshot", broker_time_ms=None)
    got = at_t(await reader.stop_versions("syn", "P1", [], T))
    assert isinstance(got, StopAtT) and got.level == Decimal("8.6") and got.basis is StopBasis.PRIOR_VERSION


async def test_a_newer_untimed_version_without_a_level_is_unresolved(emu: ArmEmulator,
                                                                    reader: V.PgArmVersionReader) -> None:
    """The `stop_loss` field cleared, seen only through an untimed read: the level the handle held at `T` cannot be
    resolved against the prior one, so the bar is `DATA_UNAVAILABLE(stop_at_bar_end)`."""
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload={"status": "OPEN", "stop_loss": "9.1",
                    "units": "2"}, origin="stream", broker_time_ms=T - 60_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2, payload={"status": "OPEN", "units": "2"},
                    origin="snapshot", broker_time_ms=None)
    assert isinstance(at_t(await reader.stop_versions("syn", "P1", [], T)), StopUnavailable)


async def test_every_newer_noncounting_version_is_read_with_its_level(emu: ArmEmulator,
                                                                      reader: V.PgArmVersionReader) -> None:
    """Round 14 review: a history loosen (8.4) and then a history re-tighten (9.5), both after the newest counting
    version (9.1) and at or before `T`: both are read (not only the newest), so `S_at_T` is the farthest (8.4)."""
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload={"status": "OPEN", "stop_loss": "9.1",
                    "units": "2"}, origin="stream", broker_time_ms=T - 60_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2, payload={"status": "OPEN", "stop_loss": "8.4",
                    "units": "2"}, origin="history", broker_time_ms=T - 30_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=3, payload={"status": "OPEN", "stop_loss": "9.5",
                    "units": "2"}, origin="history", broker_time_ms=T - 10_000)
    versions = await reader.stop_versions("syn", "P1", [], T)
    assert [(v.seq_id, v.level) for v in versions] == [(1, Decimal("9.1")), (2, Decimal("8.4")), (3, Decimal("9.5"))]
    got = at_t(versions)
    assert isinstance(got, StopAtT) and got.level == Decimal("8.4") and got.basis is StopBasis.PRIOR_VERSION
    assert "NOT EXISTS" in V.SQL_STOP_NONCOUNTING and "DISTINCT ON" not in V.SQL_STOP_NONCOUNTING


async def test_more_noncounting_versions_than_the_bound_leave_the_handle_unresolved(
        emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    """More than `NONCOUNTING_LIMIT` newer non-counting versions: the oldest were not read, so the handle is never
    tested without them (`DATA_UNAVAILABLE(stop_at_bar_end)`); exactly the limit still resolves."""
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload={"status": "OPEN", "stop_loss": "9.1",
                    "units": "2"}, origin="stream", broker_time_ms=T - 60_000)
    for i in range(V.NONCOUNTING_LIMIT):
        emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2 + i,
                        payload={"status": "OPEN", "stop_loss": "9.2", "units": "2"}, origin="snapshot",
                        broker_time_ms=None)
    got = at_t(await reader.stop_versions("syn", "P1", [], T))
    assert isinstance(got, StopAtT) and got.level == Decimal("9.1")
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2 + V.NONCOUNTING_LIMIT,
                    payload={"status": "OPEN", "stop_loss": "9.2", "units": "2"}, origin="snapshot",
                    broker_time_ms=None)
    assert isinstance(at_t(await reader.stop_versions("syn", "P1", [], T)), StopUnavailable)


async def test_a_stop_read_after_t_gives_the_caveat(emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=1, payload=child("9.0"), origin="stop_read",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=2, payload=child("9.9"), origin="stop_read",
                    broker_time_ms=T + 60_000)
    versions = await reader.stop_versions("syn", "P1", ["O1"], T)
    assert [v.level for v in versions] == [Decimal("9.0"), None]
    got = at_t(versions)
    assert isinstance(got, StopAtT) and got.basis is StopBasis.READ
    assert got.caveats == (STOP_CHANGED_AFTER_BAR_END,)


async def test_a_cancelled_child_holds_no_stop(emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=1, payload=child("9.0"), origin="stream",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=2, payload=child("9.0", "CANCELLED", "0"),
                    origin="stream", broker_time_ms=T - 1_000)
    versions = await reader.stop_versions("syn", "P1", ["O1"], T)
    assert [v.live for v in versions] == [False]
    assert isinstance(at_t(versions), StopUnavailable)


async def test_a_handle_created_after_t_takes_no_part(emu: ArmEmulator, reader: V.PgArmVersionReader) -> None:
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=1, payload=child("9.0"), origin="stream",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="ORDER", entity_key="order:syn:O2", seq_id=2, payload=child("9.4"), origin="stream",
                    broker_time_ms=T + 5_000)
    versions = await reader.stop_versions("syn", "P1", ["O1", "O2"], T)
    assert [v.handle_id for v in versions] == ["O1"]
    got = at_t(versions)
    assert isinstance(got, StopAtT) and got.level == Decimal("9.0")
