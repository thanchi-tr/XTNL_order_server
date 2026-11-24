"""Live review round 2: the `S_at_T` SQL reader ignores an untimed stop version known to come after `T` (design
§B6.4 / §B6.3 (7a) `S_at_T`, C1; `docs/design/live-review-2026-11-25.md` "Post-T stop versions in SQL").

A watermark-labelled REST row (no broker time) that is newer by `seq` than a counting version timed after `T` was read
after that version, so it shows state from after `T` and takes no part. Round 1 made the pure rule
(`src/riskmgr/stopbasis.py::_untimed_newer`) say so; `SQL_STOP_NONCOUNTING` now says the same, so a stop loosened
after `T` (seen first through the stream, then through an untimed REST read) never withholds a bar that met the
trigger. Synthetic ids, levels and times only.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import pytest

from src.db import arm_versions as V
from src.db.epoch import EngineDb, Epoch
from src.dto.entities import Side
from src.riskmgr.stopbasis import STOP_CHANGED_AFTER_BAR_END, StopAtT, StopUnavailable, stop_at_bar_end
from src.riskmgr.types import StopBasis
from tests.db.arm_emulator import ArmEmulator
from tests.db.arm_rows import to_stop_version

T = 10_000_000
PK = "position:syn:P1"
OK = "order:syn:O1"
D = Decimal


@pytest.fixture
def emu() -> ArmEmulator:
    return ArmEmulator()


@pytest.fixture
def reader(emu: ArmEmulator) -> V.PgArmVersionReader:
    return V.PgArmVersionReader(EngineDb(emu, lambda: Epoch("c", 7)))


def pos(level: str) -> dict[str, Any]:
    return {"status": "OPEN", "stop_loss": level, "units": "2"}


def child(level: str) -> dict[str, Any]:
    return {"status": "WORKING", "entry_price": level, "remaining_units": "2", "order_kind": "STOP"}


def at_t(versions: list[Any], side: Side = "BUY", entry: str = "10") -> StopAtT | StopUnavailable:
    return stop_at_bar_end([to_stop_version(v) for v in versions], side=side, entry=D(entry),
                           units_to_cover=D("2"), bar_end_ms=T)


# ---- SQL shape ------------------------------------------------------------------------------------------------------
def test_the_noncounting_read_carries_the_post_t_counting_predicate() -> None:
    sql = V.SQL_STOP_NONCOUNTING
    assert "a.broker_time_ms > $3::bigint AND a.seq_id < e.seq_id" in sql
    tail = sql.split("FROM event_log a", 1)[1]
    assert f"a.origin IN {V.COUNTING_SQL}" in tail and "a.action = 'APPLIED'" in tail
    # only untimed versions are bounded by it (timed non-counting ones are at or before T by construction)
    assert "e.broker_time_ms IS NOT NULL OR NOT EXISTS" in sql


# ---- reader over the event_log harness ------------------------------------------------------------------------------
@pytest.mark.parametrize("side,entry,held,loosened", [("BUY", "10", "9.1", "8.0"), ("SELL", "10", "10.9", "12.0")])
async def test_untimed_read_after_a_post_t_stream_version_takes_no_part(
        emu: ArmEmulator, reader: V.PgArmVersionReader, side: Side, entry: str, held: str, loosened: str) -> None:
    """The stop was at `held` at `T`, loosened after `T` (a stream version timed after T), then seen again through an
    untimed REST read. That read comes after the post-T version: `S_at_T` is the stream level at T, never the loosened
    one."""
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload=pos(held), origin="stream",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2, payload=pos(loosened), origin="stream",
                    broker_time_ms=T + 30_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=3, payload=pos(loosened), origin="snapshot",
                    broker_time_ms=None)
    versions = await reader.stop_versions("syn", "P1", [], T)
    assert [v.seq_id for v in versions] == [1]
    got = at_t(versions, side, entry)
    assert isinstance(got, StopAtT) and got.level == D(held) and got.basis is StopBasis.STREAM


async def test_untimed_read_before_the_post_t_version_still_takes_part(emu: ArmEmulator,
                                                                       reader: V.PgArmVersionReader) -> None:
    """An untimed read older by `seq` than the first counting version after `T` may show the level held at `T`: it is
    still read with its level, and the direction rule tests the farther (the loosened) level (round 14)."""
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload=pos("9.1"), origin="stream",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2, payload=pos("8.6"), origin="snapshot",
                    broker_time_ms=None)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=3, payload=pos("8.0"), origin="stream",
                    broker_time_ms=T + 30_000)
    versions = await reader.stop_versions("syn", "P1", [], T)
    assert [(v.seq_id, v.level) for v in versions] == [(1, D("9.1")), (2, D("8.6"))]
    got = at_t(versions)
    assert isinstance(got, StopAtT) and got.level == D("8.6") and got.basis is StopBasis.PRIOR_VERSION


async def test_untimed_read_between_two_post_t_stop_reads_takes_no_part(emu: ArmEmulator,
                                                                        reader: V.PgArmVersionReader) -> None:
    """Two `stop_read` versions after `T` with an untimed read between them: the level-less marker is the newest
    `stop_read` after T, so the pure rule alone would see the untimed read as older than every post-T version. The
    SQL drops it (it was read after the first post-T stop read): READ basis at the level held at T, with the
    caveat."""
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=1, payload=child("9.0"), origin="stop_read",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=2, payload=child("8.2"), origin="stop_read",
                    broker_time_ms=T + 20_000)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=3, payload=child("8.2"), origin="snapshot",
                    broker_time_ms=None)
    emu.add_version(entity_type="ORDER", entity_key=OK, seq_id=4, payload=child("8.2"), origin="stop_read",
                    broker_time_ms=T + 40_000)
    versions = await reader.stop_versions("syn", "P1", ["O1"], T)
    assert [(v.seq_id, v.level) for v in versions] == [(1, D("9.0")), (4, None)]
    got = at_t(versions)
    assert isinstance(got, StopAtT) and got.level == D("9.0") and got.basis is StopBasis.READ
    assert got.caveats == (STOP_CHANGED_AFTER_BAR_END,)


async def test_a_post_t_history_version_does_not_bound_untimed_reads(emu: ArmEmulator,
                                                                     reader: V.PgArmVersionReader) -> None:
    """Only a counting version after `T` bounds the untimed reads (a `history` one never counts, §B6.4)."""
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload=pos("9.1"), origin="stream",
                    broker_time_ms=T - 60_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=2, payload=pos("8.0"), origin="history",
                    broker_time_ms=T + 30_000)
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=3, payload=pos("8.6"), origin="snapshot",
                    broker_time_ms=None)
    versions = await reader.stop_versions("syn", "P1", [], T)
    assert [(v.seq_id, v.level) for v in versions] == [(1, D("9.1")), (3, D("8.6"))]


# ---- the real statement on PostgreSQL (disposable test database only; reads a CTE, writes nothing) ----------------
_CTE = ("WITH event_log (entity_type, entity_key, seq_id, broker_time_ms, origin, action, payload) AS (VALUES "
        "('POSITION', 'position:syn:P1', 1::bigint, {t} - 60000, 'stream', 'APPLIED', '{{\"status\":\"OPEN\","
        "\"stop_loss\":\"9.1\",\"units\":\"2\"}}'::jsonb), "
        "('POSITION', 'position:syn:P1', 2::bigint, NULL::bigint, 'snapshot', 'APPLIED', '{{\"status\":\"OPEN\","
        "\"stop_loss\":\"8.6\",\"units\":\"2\"}}'::jsonb), "
        "('POSITION', 'position:syn:P1', 3::bigint, {t} + 30000, 'stream', 'APPLIED', '{{\"status\":\"OPEN\","
        "\"stop_loss\":\"8.0\",\"units\":\"2\"}}'::jsonb), "
        "('POSITION', 'position:syn:P1', 4::bigint, NULL::bigint, 'snapshot', 'APPLIED', '{{\"status\":\"OPEN\","
        "\"stop_loss\":\"8.0\",\"units\":\"2\"}}'::jsonb)) ")


@pytest.mark.pg
@pytest.mark.skipif(not os.environ.get("ORDER_SERVER_TEST_DSN"), reason="ORDER_SERVER_TEST_DSN not set")
async def test_the_statement_on_postgres_drops_untimed_reads_after_a_post_t_counting_version() -> None:
    from src.db.pg import create_asyncpg_source

    dsn = os.environ["ORDER_SERVER_TEST_DSN"]
    src = await create_asyncpg_source(dsn, application_name="xtnl:5:it")
    try:
        async with src.acquire() as conn:
            rows = await conn.fetch(_CTE.format(t=T) + V.SQL_STOP_NONCOUNTING, "POSITION", [PK], T,
                                    V.NONCOUNTING_LIMIT + 1)
    finally:
        await src.close()
    assert [(int(r[1]), r[4]) for r in rows] == [(2, "8.6")]     # seq 4 was read after the post-T version 3
