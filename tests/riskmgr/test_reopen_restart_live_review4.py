"""Live review round 4: a REOPEN across an engine restart over the weekend, and a re-check that ends during downtime.

Design §B6.3 REOPEN: the pending re-check is stored in `arm_positions` (`reopen_bar_end_ms`), retried from the first
open instant + settle until the first post-open bar's POST decision (at the latest its `decision_cap`); its tests
name "wake from a weekend sleep then the open" (a sleep longer than the fence ends the term, so the engine comes
back as a restart, with a fresh BarClock and no memory). Round-11 confirmation C3 (the REOPEN item at the open).
Tests only; synthetic ids and times.
"""
from __future__ import annotations

from src.riskmgr.barclock import Evaluated
from src.riskmgr.sm11 import Sm11Event
from src.riskmgr.types import ArmEvent
from tests.riskmgr.barclock_fakes import SETTLE, T1, TF, BcRig, FakeGrid, bc_rig, decided
from tests.riskmgr.test_barclock import Closes, row_of

OPEN_MS = T1 + 8 * TF
CLOSED = frozenset(T1 + k * TF for k in range(1, 9))


def _rule(req) -> Evaluated:  # type: ignore[no-untyped-def]
    if req.event is ArmEvent.POST and req.bar_end_ms == T1:
        return decided(outcome="MARKET_CLOSED", category="HOLD", reopen=True)
    if req.event is ArmEvent.REOPEN:
        return decided(outcome="ADJUST", category="ACTIONABLE", events=(Sm11Event.FINAL_ADJUST,))
    return decided(outcome="STRATEGY_REASON", category="INFO", reason="BELOW_TRIGGER")


def _rig(now: int, store: object | None = None) -> BcRig:
    return bc_rig(rule=_rule, grid=FakeGrid(closed=CLOSED), closes=Closes(T1, OPEN_MS), store=store, now=now)


def _rows(r: BcRig) -> list[tuple[str, int, str, str | None]]:
    return [(d.event, (d.bar_end_ms - T1) // TF, d.outcome, d.reason) for d in r.store.rows_of()]


async def test_wake_from_a_weekend_sleep_then_the_open_runs_the_reopen_from_the_stored_recheck() -> None:
    r = _rig(T1 + SETTLE + 1_000)
    await r.step(T1 + SETTLE + 1_000)
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None)] and row_of(r.store).reopen_bar_end_ms == T1
    r.clock_.close()
    r2 = _rig(OPEN_MS + SETTLE + 1, store=r.store)        # a new term: no in-memory REOPEN cadence or HOLD
    try:
        await r2.step(OPEN_MS + SETTLE + 1)
        assert _rows(r2) == [("POST", 0, "MARKET_CLOSED", None), ("REOPEN", 0, "ADJUST", None)]
        reopen = r.store.rows_of()[-1]
        assert reopen.advice is True and reopen.bar_end_ms == T1
        row = row_of(r.store)
        assert row.reopen_bar_end_ms is None and row.state == "ADVICE_OUTSTANDING" and row.last_decided_bar == T1
    finally:
        r2.clock_.close()


async def test_a_restart_after_the_recheck_window_drops_it_and_decides_the_open_bars() -> None:
    r = _rig(T1 + SETTLE + 1_000)
    await r.step(T1 + SETTLE + 1_000)
    r.clock_.close()
    late = OPEN_MS + 3 * TF                               # past the first post-open bar's cap
    r2 = _rig(late, store=r.store)
    try:
        await r2.step(late)
        assert _rows(r2) == [("POST", 0, "MARKET_CLOSED", None), ("POST", 9, "UNRESOLVED_RANGE", "downtime"),
                             ("POST", 10, "STRATEGY_REASON", "BELOW_TRIGGER"),
                             ("POST", 11, "STRATEGY_REASON", "BELOW_TRIGGER")]
        assert [d for d in r.store.rows_of() if d.event == "REOPEN"] == []
        assert row_of(r.store).reopen_bar_end_ms is None
    finally:
        r2.clock_.close()
