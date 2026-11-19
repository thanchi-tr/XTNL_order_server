"""S39b: the ARM store's new read-only reads over the emulator (`recent_final_decisions` for the correction-window
rebuild; `decision_memory_before` and `decision_handles` for `arm replay --rerun`). Synthetic levels only."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.db import arm_store as A
from src.db.epoch import EngineDb, Epoch
from tests.db.arm_emulator import ArmEmulator
from tests.db.arm_rows import KEY, decision_row, handle_row, position_row


@pytest.fixture
def emu() -> ArmEmulator:
    return ArmEmulator(epoch=7)


@pytest.fixture
def store(emu: ArmEmulator) -> A.PgArmStateStore:
    return A.PgArmStateStore(EngineDb(emu, lambda: Epoch("c", 7)))


async def _decide(store: A.PgArmStateStore) -> list[object]:
    refs = []
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        for seq, (event, bar, outcome, rev) in enumerate((
                ("PRE", 900_000, "ADJUST", 0), ("POST", 900_000, "ADJUST", 0), ("POST", 1_800_000, "HOLD", 0),
                ("CORRECTED", 1_800_000, "ADJUST", 1), ("POST", 2_700_000, "UNRESOLVED_RANGE", 0)), start=1):
            w = await txn.record_decision(
                decision_row(event=event, bar_end_ms=bar, outcome=outcome, revision=rev, decision_seq=seq,
                             proposed_stop=Decimal("10.0") if outcome == "ADJUST" else None),
                (handle_row(level_read=Decimal("9.0"), level_at_pre=Decimal("9.5") if event == "PRE" else None),))
            refs.append(w.ref)
    return refs


async def test_recent_final_decisions_are_bar_end_finals_newest_first(store: A.PgArmStateStore) -> None:
    await _decide(store)
    got = await store.recent_final_decisions(0, 100)
    assert [(s.row.event, s.row.bar_end_ms, s.row.revision) for s in got] == [
        ("CORRECTED", 1_800_000, 1), ("POST", 1_800_000, 0), ("POST", 900_000, 0)]     # no PRE, no span
    assert [s.row.event for s in await store.recent_final_decisions(1_000_000, 100)] == ["CORRECTED", "POST"]
    cut = await store.recent_final_decisions(0, 1)                         # a truncated read keeps the newest
    assert [(s.row.event, s.row.bar_end_ms) for s in cut] == [("CORRECTED", 1_800_000)]
    assert "ORDER BY d.bar_end_ms DESC, d.decision_seq DESC LIMIT" in A.SQL_RECENT_FINAL_DECISIONS


async def test_the_memory_before_a_decision_and_its_handles(store: A.PgArmStateStore) -> None:
    refs = await _decide(store)
    now = await store.decision_memory(KEY, "15m", 1_800_000)
    before = await store.decision_memory_before(KEY, "15m", 1_800_000, 3)    # before the HOLD of bar 2
    assert now.last_outcome == "UNRESOLVED_RANGE" and before.last_outcome == "ADJUST"
    assert before.final_advice_target == Decimal("10.0") and before.prior_final_adjust == Decimal("10.0")
    pre = await store.decision_memory_before(KEY, "15m", 900_000, 2)         # the POST of bar 1 saw its PRE
    assert pre.pre_levels == (("POSITION_FIELD", Decimal("9.5")),) and pre.last_outcome is None
    assert (await store.decision_memory_before(KEY, "15m", 900_000, 1)).pre_levels == ()
    assert await store.decision_handles(refs[1]) == (("P1", Decimal("9.0")),)  # type: ignore[arg-type]
