"""Live review, finding 1: a re-pin of E or S0 drops the recorded break-even `target` from the strategy state in the
same CAS (FILL_PROGRESS, ENTRY_CORRECTION, local rearm), keeping `latched`, so a REOPEN never reuses a target recorded
under the old E (one that can sit on the loss side of the new E). Fakes from `tracker_fakes.py`; synthetic levels.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from src.db.arm_ports import EntryTime
from src.dto import OrderStatus
from src.riskmgr.sm11 import ArmState
from src.riskmgr.strategies import BREAK_EVEN_ON_RISK_CLOSE_V1
from src.riskmgr.strategy import StrategyState
from src.riskmgr.tracker import REPIN_STALE_FIELD, AnswerStatus, TrackerRequest, state_after_entry_repin
from tests.riskmgr.tracker_fakes import ADAPTER, T0, Rig, entry_order, image, position, rig

D = Decimal
HOUR = 3_600_000
SCHEMA = BREAK_EVEN_ON_RISK_CLOSE_V1.STATE_SCHEMA


def run(coro: object) -> None:
    asyncio.run(coro)  # type: ignore[arg-type]


def _plant(r: Rig, state: StrategyState) -> None:
    """Put a recorded trigger into the stored row (as `commit_state` would after a final bar)."""
    row = r.store.row()
    r.store.rows[row.key] = replace(row, strategy_state_json=state.encode())


def _state(r: Rig) -> StrategyState:
    got = StrategyState.decode(r.store.row().strategy_state_json)
    assert got is not None
    return got


def _latched_at(level: str) -> StrategyState:
    return StrategyState.of(latched=True, target=D(level))


# ---------------------------------------------------------------------------------------------------- the helper
def test_the_helper_drops_only_the_target_and_only_on_a_re_pin() -> None:
    s = _latched_at("1.2000").encode()
    out = StrategyState.decode(state_after_entry_repin(s, SCHEMA, repinned=True))
    assert out is not None and out.text(REPIN_STALE_FIELD) is None and out.flag("latched")
    assert state_after_entry_repin(s, SCHEMA, repinned=False) == s                 # nothing re-pinned
    assert state_after_entry_repin(s, None, repinned=True) == s                    # unknown version: untouched
    assert state_after_entry_repin(s, frozenset({"latched"}), repinned=True) == s  # schema without a target
    plain = StrategyState.of(latched=False).encode()
    assert state_after_entry_repin(plain, SCHEMA, repinned=True) == plain          # no target recorded
    assert state_after_entry_repin("not json", SCHEMA, repinned=True) == "not json"


# ---------------------------------------------------------------------------------------------------- the tracker
@pytest.mark.parametrize("side,new_entry,new_stop", [("BUY", "1.2010", "1.1950"), ("SELL", "1.1990", "1.2050")])
def test_fill_progress_re_pin_drops_the_recorded_target(side: str, new_entry: str, new_stop: str) -> None:
    r = rig()
    r.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1)
    stop = "1.1900" if side == "BUY" else "1.2100"
    working = entry_order("E1", status=OrderStatus.PARTIALLY_FILLED)
    r.snapshot.images = [image(position("P1", side=side, stop=stop, opened_by="E1"), orders=[working])]
    run(r.tracker.step())
    _plant(r, _latched_at("1.2000"))
    r.snapshot.images = [image(position("P1", side=side, units="2000", entry=new_entry, stop=new_stop,
                                        opened_by="E1"), orders=[working])]
    r.clock.now += 2_000
    run(r.tracker.step())
    row = r.store.row()
    assert (row.entry_level, row.state_reason) == (D(new_entry), "fill_progress")
    state = _state(r)
    assert state.text(REPIN_STALE_FIELD) is None and state.flag("latched")      # written in the same CAS as E


@pytest.mark.parametrize("side,stop,new_entry", [("BUY", "1.1900", "1.2005"), ("SELL", "1.2100", "1.1995")])
def test_entry_correction_drops_the_recorded_target(side: str, stop: str, new_entry: str) -> None:
    r = rig()
    r.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1)
    r.snapshot.images = [image(position("P1", side=side, stop=stop))]
    run(r.tracker.step())
    _plant(r, _latched_at("1.2000"))
    roll = 1_699_999_200_000                       # well after any roll: an entry change is a correction
    r.snapshot.images = [image(position("P1", side=side, stop=stop, entry=new_entry, updated_ms=roll + 3 * HOUR))]
    run(r.tracker.step())
    row = r.store.row()
    assert (row.entry_level, row.state_reason) == (D(new_entry), "entry_corrected")
    state = _state(r)
    assert state.text(REPIN_STALE_FIELD) is None and state.flag("latched")


@pytest.mark.parametrize("side,stop,new_entry", [("BUY", "1.1900", "1.2010"), ("SELL", "1.2100", "1.1990")])
def test_local_rearm_re_pin_drops_the_recorded_target(side: str, stop: str, new_entry: str) -> None:
    r = rig()
    r.snapshot.images = [image(position("P1", side=side, stop=stop))]
    run(r.tracker.step())
    _plant(r, _latched_at("1.2000"))
    r.snapshot.images = [image(position("P1", side=side, stop=stop, units="2000", entry=new_entry))]
    run(r.tracker.step())                                            # no live entry order: suspends
    assert r.store.row().state == ArmState.SUSPENDED.value
    assert D(_state(r).text(REPIN_STALE_FIELD) or "0") == D("1.2000")   # a suspension re-pins nothing
    r.clock.now += HOUR
    r.tracker.submit(TrackerRequest.of(41, "arm_rearm", f"{ADAPTER}:P1"))
    run(r.tracker.step())
    assert r.rec.answers[-1][1].status is AnswerStatus.APPLIED
    row = r.store.row()
    assert (row.state_reason, row.entry_level) == ("local_rearm", D(new_entry))
    state = _state(r)
    assert state.text(REPIN_STALE_FIELD) is None and state.flag("latched")


def test_a_change_that_keeps_e_and_s0_keeps_the_recorded_target() -> None:
    r = rig()
    r.versions.entries["P1"] = EntryTime(source="STREAM", broker_time_ms=T0 + 60_000, seq_id=1)
    r.snapshot.images = [image(position("P1"))]
    run(r.tracker.step())
    _plant(r, _latched_at("1.2000"))
    r.snapshot.images = [image(position("P1", units="500"))]                  # a units decrease: E unchanged
    run(r.tracker.step())
    assert r.store.row().state_reason == "units_decrease"
    assert D(_state(r).text(REPIN_STALE_FIELD) or "0") == D("1.2000")
