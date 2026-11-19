"""Round 13 (plan S38d): `S_at_T` over every stop child ever linked to the position (design §B6.4).

Golden cases, long and short: the stop child that held the stop at `T` is cancelled / replaced (a new order id)
after `T`: while a delayed POST waits for the broker's final bar, over the weekend before a REOPEN re-check, and
before a LATE replay. The evaluation names only the current child (the replacement); the reader adds every linked
child that may have been live at `T`, so the bar is decided on the pre-replace level with a STREAM basis, never a
`DATA_UNAVAILABLE(stop_at_bar_end)` span. A child created after `T` takes no part; a child that ended before `T` is
not read. Synthetic ids and levels only.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from src.db import arm_versions as V
from src.db.epoch import EngineDb, Epoch
from src.riskmgr.stopbasis import StopAtT, stop_at_bar_end
from src.riskmgr.types import StopBasis
from tests.db.arm_emulator import ArmEmulator
from tests.db.arm_rows import to_stop_version

T = 10_000_000_000
MIN = 60_000
DAY = 86_400_000
PK = "position:syn:P1"


def order(oid: str) -> str:
    return f"order:syn:{oid}"


def child(level: str, status: str = "WORKING", units: str = "2", pid: str = "P1") -> dict[str, Any]:
    return {"status": status, "entry_price": level, "remaining_units": units, "order_kind": "STOP",
            "linked_position_id": pid, "is_reduce_only": True}


@pytest.fixture
def emu() -> ArmEmulator:
    return ArmEmulator()


@pytest.fixture
def reader(emu: ArmEmulator) -> V.PgArmVersionReader:
    return V.PgArmVersionReader(EngineDb(emu, lambda: Epoch("c", 7)))


def _replace(emu: ArmEmulator, old: str, new: str, at_ms: int, seq: int) -> None:
    """A StoneX cancel/replace at `at_ms`: the old child cancelled, a new child (a new order id) working."""
    emu.add_version(entity_type="ORDER", entity_key=order("O1"), seq_id=seq, payload=child(old, "CANCELLED"),
                    origin="stream", broker_time_ms=at_ms)
    emu.add_version(entity_type="ORDER", entity_key=order("O2"), seq_id=seq + 1, payload=child(new),
                    origin="stream", broker_time_ms=at_ms)


@pytest.mark.parametrize(("side", "held", "moved"), [("BUY", "9.0", "9.5"), ("SELL", "11.0", "10.5")],
                         ids=["long", "short"])
@pytest.mark.parametrize("replaced_after", [20_000, 2 * DAY + 3 * 3_600_000, 5 * MIN],
                         ids=["delayed_post", "weekend_reopen", "late_replay"])
async def test_a_cancel_replace_after_t_decides_on_the_pre_replace_level(
        emu: ArmEmulator, reader: V.PgArmVersionReader, side: str, held: str, moved: str,
        replaced_after: int) -> None:
    emu.add_version(entity_type="POSITION", entity_key=PK, seq_id=1, payload={"status": "OPEN", "units": "2"},
                    origin="stream", broker_time_ms=T - 3_600_000)
    # an older child, cancelled an hour before T: it held no stop at T and is not read at all
    emu.add_version(entity_type="ORDER", entity_key=order("O0"), seq_id=2, payload=child("8.0"), origin="stream",
                    broker_time_ms=T - 2 * 3_600_000)
    emu.add_version(entity_type="ORDER", entity_key=order("O0"), seq_id=3, payload=child("8.0", "CANCELLED"),
                    origin="stream", broker_time_ms=T - 3_600_000)
    emu.add_version(entity_type="ORDER", entity_key=order("O1"), seq_id=4, payload=child(held), origin="stream",
                    broker_time_ms=T - 30 * MIN)
    _replace(emu, held, moved, T + replaced_after, seq=5)
    versions = await reader.stop_versions("syn", "P1", ["O2"], T)       # the evaluation names the current child
    ids = {v.handle_id for v in versions if v.handle_kind == "CHILD_ORDER"}
    assert ids == {"O1"}          # the replaced child is read; O2 has no version by T; O0 ended before T: not read
    assert "SELECT broker_order_id FROM orders_state" in emu.sql[1]
    got = stop_at_bar_end([to_stop_version(v) for v in versions], side=side,  # type: ignore[arg-type]
                          entry=Decimal("10"), units_to_cover=Decimal("2"), bar_end_ms=T)
    assert isinstance(got, StopAtT)
    assert got.level == Decimal(held) and got.basis is StopBasis.STREAM
    assert [h.handle.id for h in got.handles] == ["O1"]                 # O2 was created after T: no part


async def test_only_reduce_only_children_of_this_position_are_added(emu: ArmEmulator,
                                                                    reader: V.PgArmVersionReader) -> None:
    other = child("9.2", pid="P2")
    entry = child("9.3") | {"is_reduce_only": False}
    emu.add_version(entity_type="ORDER", entity_key=order("X1"), seq_id=1, payload=other, origin="stream",
                    broker_time_ms=T - MIN)
    emu.add_version(entity_type="ORDER", entity_key=order("X2"), seq_id=2, payload=entry, origin="stream",
                    broker_time_ms=T - MIN)
    versions = await reader.stop_versions("syn", "P1", [], T)
    assert {v.handle_id for v in versions if v.handle_kind == "CHILD_ORDER"} == set()


def test_the_linked_children_query_reads_only_order_state() -> None:
    sql = V.SQL_LINKED_STOP_CHILDREN
    assert sql.startswith("SELECT broker_order_id FROM orders_state")
    assert "linked_position_id = $2::text" in sql and "is_reduce_only" in sql
    assert "broker_updated_at_ms > $3::bigint" in sql
    for level_field in ("entry_price", "stop_loss", "payload"):
        assert level_field not in sql                                    # ids only
