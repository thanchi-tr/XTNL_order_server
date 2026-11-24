"""Live review round 4: a REOPEN that does not end the re-check is retried, and the last one is written at its end.

Design §B6.3 REOPEN: "From the first OPEN instant + settle it is retried every 5 s ... `PRICE_THROUGH_LEVEL` ... [is]
retryable, because spreads are widest at the open; only `ADJUST`, `COMPLETE`, a closed position, or the first
post-open bar's POST decision (at the latest its `decision_cap`) ends it ... on expiry the last HOLD is sent with its
reason." Plan S22 (a retryable REOPEN is marked not due for the rest of the pass). Before the fix, any final REOPEN
result ended the re-check, so a trigger re-checked at the open was written once as a HOLD and dropped.
Synthetic ids and times only.
"""
from __future__ import annotations

from dataclasses import replace

from src.marketdata.timing import COLD_SKEW, decision_cap_ms
from src.marketdata.types import M15
from src.riskmgr.barclock import NotReady
from src.riskmgr.sm11 import Sm11Event
from src.riskmgr.types import ArmEvent
from tests.riskmgr.barclock_fakes import SETTLE, T1, TF, FakeGrid, bc_rig, decided
from tests.riskmgr.test_barclock import Closes, row_of

OPEN_MS = T1 + 8 * TF
CLOSED = frozenset(T1 + k * TF for k in range(1, 9))
FIRST_OPEN_BAR = T1 + 9 * TF
DEADLINE = decision_cap_ms(FIRST_OPEN_BAR, M15, COLD_SKEW)


def _hold(outcome: str = "PRICE_THROUGH_LEVEL"):  # type: ignore[no-untyped-def]
    """A REOPEN HOLD whose state fields would change the row if they were applied (they must not be)."""
    ev = decided(outcome=outcome, category="HOLD", events=(Sm11Event.FINAL_ADJUST,))
    return replace(ev, strategy_state_json='{"changed": true}', pre_basis_change=True, pre_basis_json='[]',
                   pre_basis_bar=T1)


def _rig(reopen, post_open=None):  # type: ignore[no-untyped-def]
    attempts: list[int] = []

    def rule(req):  # type: ignore[no-untyped-def]
        if req.event is ArmEvent.POST and req.bar_end_ms == T1:
            return decided(outcome="MARKET_CLOSED", category="HOLD", reopen=True)
        if req.event is ArmEvent.REOPEN:
            attempts.append(req.now_ms)
            return reopen(len(attempts))
        if req.event is ArmEvent.POST and post_open is not None:
            return post_open(req)
        return NotReady("broker_bar_not_final")

    r = bc_rig(rule=rule, grid=FakeGrid(closed=CLOSED), closes=Closes(T1, OPEN_MS))
    return r, attempts


def _rows(r):  # type: ignore[no-untyped-def]
    return [(d.event, (d.bar_end_ms - T1) // TF, d.outcome, d.reason) for d in r.store.rows_of()]


def test_only_adjust_and_complete_end_the_recheck() -> None:
    from src.riskmgr.barclock import ends_recheck

    assert ends_recheck("ADJUST") and ends_recheck("COMPLETE")
    for outcome in ("PRICE_THROUGH_LEVEL", "MARKET_CLOSED", "BROKER_AHEAD", "STRATEGY_REASON", "NO_STOP",
                    "AT_TARGET_UNCONFIRMED", "LOOSENED_AFTER_COMPLETE"):
        assert not ends_recheck(outcome)


async def test_a_reopen_price_through_level_is_retried_then_adjusts() -> None:
    r, attempts = _rig(lambda n: _hold() if n == 1 else decided(outcome="ADJUST", category="ACTIONABLE"))
    await r.step(T1 + SETTLE + 1_000)
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]
    await r.step(OPEN_MS + SETTLE + 1)                    # first attempt: price through the level at the open
    assert len(attempts) == 1 and _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]
    assert row_of(r.store).reopen_bar_end_ms == T1        # the re-check stays open, no row
    await r.step(OPEN_MS + SETTLE + 3_000)                # inside the 5 s cadence: no attempt
    assert len(attempts) == 1
    await r.step(OPEN_MS + SETTLE + 6_000)                # 5 s later: retried, ADJUST ends it
    assert len(attempts) == 2
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None), ("REOPEN", 0, "ADJUST", None)]
    reopen = r.store.rows_of()[-1]
    assert reopen.advice and reopen.bar_end_ms == T1
    assert row_of(r.store).reopen_bar_end_ms is None


async def test_a_market_not_open_yet_at_the_open_is_retried() -> None:
    """The Sunday open with non-tradeable first quotes (§B6.3 tests): MARKET_CLOSED at the first attempt."""
    r, attempts = _rig(lambda n: _hold("MARKET_CLOSED") if n == 1 else decided(outcome="ADJUST",
                                                                               category="ACTIONABLE"))
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)
    await r.step(OPEN_MS + SETTLE + 6_000)
    assert len(attempts) == 2 and _rows(r)[-1] == ("REOPEN", 0, "ADJUST", None)


async def test_only_holds_until_the_deadline_write_one_reopen_hold_with_its_reason() -> None:
    r, attempts = _rig(lambda n: _hold())
    await r.step(T1 + SETTLE + 1_000)
    before = row_of(r.store)
    t = OPEN_MS + SETTLE + 1
    while t < DEADLINE:
        await r.step(t)
        t += 6_000
    assert len(attempts) > 2
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]   # nothing written while it is retried
    await r.step(DEADLINE + 1)
    rows = _rows(r)
    reopen = [x for x in rows if x[0] == "REOPEN"]
    assert reopen == [("REOPEN", 0, "PRICE_THROUGH_LEVEL", None)]
    assert rows[:2] == [("POST", 0, "MARKET_CLOSED", None), ("REOPEN", 0, "PRICE_THROUGH_LEVEL", None)]
    row = row_of(r.store)
    assert row.reopen_bar_end_ms is None
    # the expiry write records and sends; it changes no state (no SM11 event, strategy state or basis)
    assert (row.state, row.strategy_state_json, row.pre_basis_json, row.pre_basis_bar) == (
        before.state, before.strategy_state_json, before.pre_basis_json, before.pre_basis_bar)
    n = len(attempts)
    await r.step(DEADLINE + 20_000)
    assert len(attempts) == n and len([x for x in _rows(r) if x[0] == "REOPEN"]) == 1


async def test_the_first_post_open_post_ends_the_recheck_and_the_last_hold_is_written() -> None:
    def post_open(req):  # type: ignore[no-untyped-def]
        if req.now_ms >= FIRST_OPEN_BAR + SETTLE:
            return decided(outcome="STRATEGY_REASON", category="INFO", reason="BELOW_TRIGGER")
        return NotReady("broker_bar_not_final")

    r, attempts = _rig(lambda n: _hold(), post_open)
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)
    await r.step(OPEN_MS + SETTLE + 6_000)
    assert len(attempts) == 2 and [x[0] for x in _rows(r)] == ["POST"]
    await r.step(FIRST_OPEN_BAR + SETTLE + 1_000)
    rows = _rows(r)
    assert rows == [("POST", 0, "MARKET_CLOSED", None), ("POST", 9, "STRATEGY_REASON", "BELOW_TRIGGER"),
                    ("REOPEN", 0, "PRICE_THROUGH_LEVEL", None)]
    row = row_of(r.store)
    assert row.reopen_bar_end_ms is None and row.last_decided_bar == FIRST_OPEN_BAR
