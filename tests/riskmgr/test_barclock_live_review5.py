"""Live review round 5: a held REOPEN row never outranks the POST that ended its re-check.

Design §B6.6: "`advice` (the row states the position's current verdict) is true for POST, `LATE` and `REOPEN` rows
... 'Newest decision' for Phase 2 = max(`decision_seq`) among `advice` rows." §B6.3 REOPEN: "only `ADJUST`,
`COMPLETE`, a closed position, or the first post-open bar's POST decision ... ends it ... on expiry the last HOLD is
sent with its reason." Before the fix, the re-check was dropped in the round after that POST committed, so the stale
REOPEN HOLD of the pre-close bar took a higher `decision_seq` than the newer bar's POST (and its notice went out
after the fresher one). Now the held REOPEN row is written first and the POST is chained on the row it leaves, in the
same pass. Synthetic ids and times only.
"""
from __future__ import annotations

from src.riskmgr.barclock import NotReady
from src.riskmgr.sm11 import Sm11Event
from src.riskmgr.types import ArmEvent
from tests.riskmgr.barclock_fakes import SETTLE, T1, decided
from tests.riskmgr.test_barclock import row_of
from tests.riskmgr.test_barclock_live_review4 import DEADLINE, FIRST_OPEN_BAR, OPEN_MS, _hold, _rig, _rows


def _adjust_at_first_open_bar(req):  # type: ignore[no-untyped-def]
    if req.now_ms >= FIRST_OPEN_BAR + SETTLE:
        return decided(outcome="ADJUST", category="ACTIONABLE", events=(Sm11Event.FINAL_ADJUST,))
    return NotReady("broker_bar_not_final")


def _newest_advice(r):  # type: ignore[no-untyped-def]
    return max((d for d in r.store.rows_of() if d.advice), key=lambda d: d.decision_seq)


async def test_a_first_post_open_adjust_stays_the_newest_advice_after_the_held_reopen_hold() -> None:
    r, attempts = _rig(lambda n: _hold(), _adjust_at_first_open_bar)
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)                    # held: price through the level at the open
    assert len(attempts) == 1
    await r.step(FIRST_OPEN_BAR + SETTLE + 1_000)         # the first post-open bar's POST decides in this step
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None), ("REOPEN", 0, "PRICE_THROUGH_LEVEL", None),
                        ("POST", 9, "ADJUST", None)]
    reopen, post = r.store.rows_of()[1:]
    assert reopen.advice and post.advice and reopen.decision_seq < post.decision_seq
    newest = _newest_advice(r)
    assert (newest.event, newest.bar_end_ms, newest.outcome) == ("POST", FIRST_OPEN_BAR, "ADJUST")
    row = row_of(r.store)
    assert row.state == "ADVICE_OUTSTANDING" and row.decision_seq == post.decision_seq
    assert row.reopen_bar_end_ms is None and row.last_decided_bar == FIRST_OPEN_BAR


async def test_the_post_is_chained_on_the_reopen_write_and_waits_when_that_write_fails() -> None:
    """If the REOPEN write rolls back, the POST misses its `row_version` CAS: neither row is written out of order,
    and both are decided again (REOPEN first) on the next pass."""
    r, _ = _rig(lambda n: _hold(), _adjust_at_first_open_bar)
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)
    r.store.raise_once.add("P1")                          # the next decision write (the REOPEN row) fails once
    await r.step(FIRST_OPEN_BAR + SETTLE + 1_000)
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]
    assert row_of(r.store).reopen_bar_end_ms is not None
    await r.step(FIRST_OPEN_BAR + SETTLE + 2_000)
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None), ("REOPEN", 0, "PRICE_THROUGH_LEVEL", None),
                        ("POST", 9, "ADJUST", None)]
    assert _newest_advice(r).event == ArmEvent.POST.value


async def test_a_late_reopen_hold_is_still_written_at_the_deadline_with_no_post() -> None:
    """The deadline path is unchanged: no newer advice row exists there to outrank."""
    r, _ = _rig(lambda n: _hold())
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)
    await r.step(DEADLINE + 1)
    assert [x for x in _rows(r) if x[0] == "REOPEN"] == [("REOPEN", 0, "PRICE_THROUGH_LEVEL", None)]
    assert row_of(r.store).reopen_bar_end_ms is None
