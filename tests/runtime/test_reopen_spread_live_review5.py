"""Live review round 5: a REOPEN attempt whose exit spread is not clean is retried (design §B6.3 REOPEN).

§B6.3 REOPEN: "`PRICE_THROUGH_LEVEL` and a spread that is not clean (the existing clean-spread rule) are
**retryable**, because spreads are widest at the open; only `ADJUST`, `COMPLETE`, a closed position, or the first
post-open bar's POST decision (at the latest its `decision_cap`) ends it ... on expiry the last HOLD is sent with its
reason." Before the fix the Oversee clean-spread threshold never reached the ARM, so a REOPEN `ADJUST` taken on a
wide opening spread ended the re-check at once.

The BarClock runs over the round-4 REOPEN rig with `ReopenSpreadGate` (`src/runtime/reopen_spread.py`) as its
evaluation port. Synthetic `SYN.FIVE` values only (a 5-digit pair whose point, the pip, is 0.0001); the threshold is
the spec default 5 pips.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import replace
from decimal import Decimal

import pytest

from src.marketdata.ports import QuoteRead
from src.marketdata.timing import COLD_SKEW, decision_cap_ms
from src.marketdata.types import M15, DataUnavailable, Liveness, MdGeneration, Quote
from src.riskmgr.barclock import EvalRequest, Evaluated, NotReady, Prepared
from src.riskmgr.sm11 import Sm11Event
from src.riskmgr.types import ArmEvent
from src.runtime.reopen_spread import EXIT_SPREAD_NOT_CLEAN, ReopenSpreadGate, held_back
from tests.riskmgr.barclock_fakes import SETTLE, T1, TF, BcRig, FakeEval, FakeGrid, arm_row, bc_rig, decided
from tests.riskmgr.test_barclock import Closes, row_of

D = Decimal
FIVE = "SYN.FIVE"
POINT = D("0.0001")
THRESHOLD = D("5")
BID = D("1.20000")
CLEAN_ASK = D("1.20010")          # 1 pip
UNCLEAN_ASK = D("1.20090")        # 9 pips
OPEN_MS = T1 + 8 * TF
CLOSED = frozenset(T1 + k * TF for k in range(1, 9))
FIRST_OPEN_BAR = T1 + 9 * TF
DEADLINE = decision_cap_ms(FIRST_OPEN_BAR, M15, COLD_SKEW)
GEN = MdGeneration(1, 1)
SECRET_TEXTS = ("1.2000", "1.2001", "1.2009", "0.0001", "0.0009", "0.001", "9 pips", "5 pips")


class Quotes:
    """The live exit quote of `SYN.FIVE`; `ask` None = no quote."""

    def __init__(self, ask: Decimal | None) -> None:
        self.ask = ask
        self.reads = 0

    async def quote(self, adapter_id: str, symbol: str) -> QuoteRead | DataUnavailable:
        self.reads += 1
        if self.ask is None:
            return QuoteRead(None, Liveness.STALE, GEN)
        return QuoteRead(Quote(adapter_id=adapter_id, symbol=symbol, bid=BID, ask=self.ask, broker_ms=1, received_ms=1),
                         Liveness.LIVE, GEN)


class Sizes:
    def __init__(self, size: object | None = POINT, fail: bool = False) -> None:
        self.size, self.fail = size, fail

    async def __call__(self, keys: Sequence[tuple[str, str]]) -> Mapping[tuple[str, str], object]:
        if self.fail:
            raise ConnectionError("down")
        return {} if self.size is None else {k: self.size for k in keys}


def _hold(outcome: str = "PRICE_THROUGH_LEVEL") -> Evaluated:
    return decided(outcome=outcome, category="HOLD", events=(Sm11Event.FINAL_ADJUST,))


def _adjust() -> Evaluated:
    return decided(outcome="ADJUST", category="ACTIONABLE", events=(Sm11Event.FINAL_ADJUST,))


def _rig(reopen, *, side: str = "BUY", quotes: Quotes, post_open=None,  # type: ignore[no-untyped-def]
         threshold=lambda a, s: THRESHOLD, sizes: Sizes | None = None,
         post=None) -> tuple[BcRig, list[int], ReopenSpreadGate]:
    attempts: list[int] = []

    def rule(req):  # type: ignore[no-untyped-def]
        if req.event is ArmEvent.POST and req.bar_end_ms == T1:
            if post is not None:
                return post
            return decided(outcome="MARKET_CLOSED", category="HOLD", reopen=True)
        if req.event is ArmEvent.REOPEN:
            attempts.append(req.now_ms)
            return reopen(len(attempts))
        if req.event is ArmEvent.POST and post_open is not None:
            return post_open(req)
        return NotReady("broker_bar_not_final")

    row = replace(arm_row(), symbol=FIVE, side=side)
    r = bc_rig(row, rule=rule, grid=FakeGrid(closed=CLOSED), closes=Closes(T1, OPEN_MS))
    gate = ReopenSpreadGate(r.eval, md=quotes, clean_spread=threshold, point_sizes=sizes or Sizes())
    r.clock_._evaluator = gate  # noqa: SLF001  (the plan wires the gate as the BarClock's evaluation port)
    return r, attempts, gate


def _rows(r: BcRig) -> list[tuple[str, int, str, str | None]]:
    return [(d.event, (d.bar_end_ms - T1) // TF, d.outcome, d.reason) for d in r.store.rows_of()]


def _no_secret(text: str) -> None:
    for s in SECRET_TEXTS:
        assert s not in text, s


@pytest.mark.parametrize("side", ["BUY", "SELL"])
async def test_an_unclean_spread_at_reopen_is_retried_then_a_clean_quote_adjusts(
        side: str, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    quotes = Quotes(UNCLEAN_ASK)
    r, attempts, gate = _rig(lambda n: _adjust(), side=side, quotes=quotes)
    await r.step(T1 + SETTLE + 1_000)
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]
    await r.step(OPEN_MS + SETTLE + 1)                   # the open: ADJUST on a 9-pip spread is held back
    assert len(attempts) == 1 and _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]
    assert row_of(r.store).reopen_bar_end_ms == T1       # the re-check stays open, no row
    assert gate.counters.unclean_spread == 1
    await r.step(OPEN_MS + SETTLE + 3_000)               # inside the 5 s cadence: no attempt
    assert len(attempts) == 1
    await r.step(OPEN_MS + SETTLE + 6_000)               # still wide: retried, still no row
    assert len(attempts) == 2 and _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]
    quotes.ask = CLEAN_ASK                               # the spread narrows to 1 pip
    await r.step(OPEN_MS + SETTLE + 12_000)
    assert len(attempts) == 3
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None), ("REOPEN", 0, "ADJUST", None)]
    assert row_of(r.store).reopen_bar_end_ms is None
    assert gate.counters.clean == 1 and gate.counters.unknown == 0
    # never a spread, a threshold, a point size or a price in a log record or a decision row
    _no_secret(caplog.text)
    for d in r.store.rows_of():
        _no_secret(repr(d))
    _no_secret(repr(gate.counters))


@pytest.mark.parametrize("side", ["BUY", "SELL"])
async def test_a_spread_unclean_until_the_deadline_writes_the_last_held_hold(side: str) -> None:
    """A HOLD at the first attempt is held; the later wide-spread ADJUSTs never replace it; at the cap the HOLD is
    written with its reason (one of the §B6.2 codes, never the spread code)."""
    r, attempts, _ = _rig(lambda n: _hold() if n == 1 else _adjust(), side=side, quotes=Quotes(UNCLEAN_ASK))
    await r.step(T1 + SETTLE + 1_000)
    t = OPEN_MS + SETTLE + 1
    while t < DEADLINE:
        await r.step(t)
        t += 6_000
    assert len(attempts) > 2
    assert _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]
    await r.step(DEADLINE + 1)
    reopen = [x for x in _rows(r) if x[0] == "REOPEN"]
    assert reopen == [("REOPEN", 0, "PRICE_THROUGH_LEVEL", None)]
    assert row_of(r.store).reopen_bar_end_ms is None
    assert all(d.outcome != EXIT_SPREAD_NOT_CLEAN and d.reason != EXIT_SPREAD_NOT_CLEAN for d in r.store.rows_of())


async def test_only_unclean_adjusts_until_the_deadline_clear_the_recheck_with_no_row() -> None:
    """Nothing retryable was held (every attempt an ADJUST on a wide spread): the existing expiry behaviour."""
    r, attempts, _ = _rig(lambda n: _adjust(), quotes=Quotes(UNCLEAN_ASK))
    await r.step(T1 + SETTLE + 1_000)
    t = OPEN_MS + SETTLE + 1
    while t <= DEADLINE + 1:
        await r.step(t)
        t += 6_000
    assert len(attempts) > 2
    assert [x for x in _rows(r) if x[0] == "REOPEN"] == []     # nothing held: the re-check is cleared, no row
    assert row_of(r.store).reopen_bar_end_ms is None


async def test_the_first_post_open_post_ends_an_unclean_recheck_with_the_last_hold() -> None:
    def post_open(req):  # type: ignore[no-untyped-def]
        if req.now_ms >= FIRST_OPEN_BAR + SETTLE:
            return decided(outcome="STRATEGY_REASON", category="INFO", reason="BELOW_TRIGGER")
        return NotReady("broker_bar_not_final")

    r, attempts, _ = _rig(lambda n: _hold() if n == 1 else _adjust(), quotes=Quotes(UNCLEAN_ASK),
                          post_open=post_open)
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)
    await r.step(OPEN_MS + SETTLE + 6_000)
    assert len(attempts) == 2 and [x[0] for x in _rows(r)] == ["POST"]
    await r.step(FIRST_OPEN_BAR + SETTLE + 1_000)
    rows = _rows(r)
    assert ("REOPEN", 0, "PRICE_THROUGH_LEVEL", None) in rows
    assert rows[-1] == ("POST", 9, "STRATEGY_REASON", "BELOW_TRIGGER")
    assert row_of(r.store).reopen_bar_end_ms is None


@pytest.mark.parametrize("case", ["threshold_none", "threshold_raises", "no_quote", "no_point_size",
                                  "point_size_unreadable", "point_size_zero"])
async def test_an_unknown_cleanliness_does_not_end_the_recheck(
        case: str, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    def raising(a: str, s: str) -> Decimal | None:
        raise KeyError("boom")

    threshold = {"threshold_none": lambda a, s: None, "threshold_raises": raising}.get(
        case, lambda a, s: THRESHOLD)
    sizes = {"no_point_size": Sizes(None), "point_size_unreadable": Sizes(fail=True),
             "point_size_zero": Sizes(D("0"))}.get(case, Sizes())
    quotes = Quotes(None if case == "no_quote" else CLEAN_ASK)
    r, attempts, gate = _rig(lambda n: _adjust(), quotes=quotes, threshold=threshold, sizes=sizes)
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)
    await r.step(OPEN_MS + SETTLE + 6_000)
    assert len(attempts) == 2 and _rows(r) == [("POST", 0, "MARKET_CLOSED", None)]
    assert row_of(r.store).reopen_bar_end_ms == T1
    assert gate.counters.unknown == 2
    _no_secret(caplog.text)


async def test_a_spread_at_the_threshold_is_clean() -> None:
    """The existing rule: clean when the spread in pips is at most the threshold."""
    r, _, gate = _rig(lambda n: _adjust(), quotes=Quotes(BID + THRESHOLD * POINT))
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)
    assert _rows(r)[-1] == ("REOPEN", 0, "ADJUST", None) and gate.counters.clean == 1


async def test_reopen_holds_and_complete_pass_unchanged_on_an_unclean_spread() -> None:
    r, attempts, _ = _rig(lambda n: decided(outcome="COMPLETE", category="ACTIONABLE"), quotes=Quotes(UNCLEAN_ASK))
    await r.step(T1 + SETTLE + 1_000)
    await r.step(OPEN_MS + SETTLE + 1)
    assert len(attempts) == 1 and _rows(r)[-1] == ("REOPEN", 0, "COMPLETE", None)
    assert not held_back(_hold()) and not held_back(decided(outcome="COMPLETE", category="ACTIONABLE"))
    assert not held_back(decided(final=False)) and held_back(_adjust())


@pytest.mark.parametrize("event", [ArmEvent.POST, ArmEvent.LATE, ArmEvent.PRE, ArmEvent.CORRECTED])
async def test_other_events_never_read_the_spread(event: ArmEvent) -> None:
    """§B6.3 names the spread rule as a REOPEN retry condition only: the job of any other event is passed as is."""
    value = _adjust()
    quotes = Quotes(UNCLEAN_ASK)
    gate = ReopenSpreadGate(FakeEval(lambda req: value), md=quotes, clean_spread=lambda a, s: THRESHOLD,
                            point_sizes=Sizes())
    req = EvalRequest(row=replace(arm_row(), symbol=FIVE), spec=M15, event=event, bar_end_ms=T1, now_ms=T1,
                      skew=COLD_SKEW, decision_cap_ms=DEADLINE, generation=GEN)
    got = await gate.prepare(req)
    assert isinstance(got, Prepared)
    assert got.job() is value and quotes.reads == 0 and gate.counters.checks == 0
    # the same ADJUST at REOPEN on that quote is held back (code only, no row)
    got = await gate.prepare(replace(req, event=ArmEvent.REOPEN))
    assert isinstance(got, Prepared)
    held = got.job()
    assert held.decision is None and not held.final and held.reason == EXIT_SPREAD_NOT_CLEAN


async def test_a_post_on_an_unclean_spread_decides_as_before() -> None:
    """The POST outcome does not change with the spread (through the BarClock)."""
    quotes = Quotes(UNCLEAN_ASK)
    r, attempts, gate = _rig(lambda n: _adjust(), quotes=quotes, post=_adjust())
    await r.step(T1 + SETTLE + 1_000)
    assert _rows(r) == [("POST", 0, "ADJUST", None)] and quotes.reads == 0 and gate.counters.checks == 0
