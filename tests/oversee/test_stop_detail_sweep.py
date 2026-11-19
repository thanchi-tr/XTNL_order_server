"""Design PR1, Oversee side (S07): `effective_stop_detail()` through MissingStopLossRule and the RiskSweep.

A STOP child counts at any price; one on its non-triggerable side of the pass's MarkBook mark is UNKNOWN
`stop_kind_inconsistent`, which goes through the rule's durable `unknown_since` / `unknown_grace_ms` (30 s)
before it becomes a capped SOFT state; no new timer, no new column (R11-M07). Synthetic prices only.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.dto import OrderDTO, OrderKind, OrderStatus, Side
from src.oversee.config import OverseeConfig
from src.oversee.risk import STOP_KIND_INCONSISTENT, MarkQuality
from src.oversee.rules import (
    EvalContext,
    MaxAggregateOpenRiskRule,
    MaxPositionRiskRule,
    MissingStopLossRule,
    run_rule,
    stop_mark,
)
from src.oversee.snapshot import AccountSnapshot, SymbolQuotes
from src.oversee.types import Cap, HaltLevel, Level, Result, Unknown
from tests.oversee.helpers import AD, T0, clean_mark, order, position, quote, snap
from tests.oversee.test_sweep import STOP_KEY, Env

# S38f: these are the S07 / PR1 rules, live only with XTNL_OVERSEE_STOP_AT_ENTRY=1 (tests/oversee/test_stop_rule_flag.py
# holds the flag-off behaviour to the pre-S07 rule).
pytestmark = pytest.mark.usefixtures("stop_at_entry_on")

D = Decimal
CFG = OverseeConfig()
GRACE = CFG.sweep.unknown_grace_ms


def child(oid: str, level: str, kind: OrderKind | None, units: str = "100000", side: Side = "SELL",
          status: OrderStatus = OrderStatus.WORKING) -> OrderDTO:
    return order(oid, side, status, ordered=units, entry=level, reduce_only=True, linked="P", kind=kind,
                 remaining="0" if status.terminal else None)


def stop_rule(s: AccountSnapshot, now: int = T0) -> Result | Unknown:
    c = EvalContext(s, CFG, now)
    p = s.positions[0]
    return run_rule(MissingStopLossRule(CFG.missing_stop_loss), c, c.entity_subject(p))


# ---------------------------------------------------------------- rule level


def test_break_even_stop_child_is_a_stop_present() -> None:
    r = stop_rule(snap(positions=(position("P", entry="1.1000"),), orders=(child("C", "1.1000", "STOP"),),
                       quotes={(AD, "EURUSD"): quote("1.1060", T0)}))
    assert isinstance(r, Result) and r.level is Level.PASS and r.reason == "stop_present"


def test_mislabelled_take_profit_is_unknown_stop_kind_inconsistent() -> None:
    r = stop_rule(snap(positions=(position("P", entry="1.1000"),), orders=(child("T", "1.1200", "STOP"),)))
    assert isinstance(r, Unknown) and r.reason == STOP_KIND_INCONSISTENT and "T" in r.detail


def test_break_even_stop_across_a_weekend_close_uses_the_last_mark() -> None:
    p, c = position("P", entry="1.1000"), child("C", "1.1000", "STOP")
    above = SymbolQuotes(last_clean=clean_mark("1.1080", T0 - 3_600_000), session="CLOSED")
    r = stop_rule(snap(positions=(p,), orders=(c,), quotes={(AD, "EURUSD"): above}))
    assert isinstance(r, Result) and r.reason == "stop_present"
    below = SymbolQuotes(last_clean=clean_mark("1.0950", T0 - 3_600_000), session="CLOSED")
    r = stop_rule(snap(positions=(p,), orders=(c,), quotes={(AD, "EURUSD"): below}))
    assert isinstance(r, Unknown) and r.reason == STOP_KIND_INCONSISTENT


def test_no_mid_ever_seen_is_not_judged() -> None:
    r = stop_rule(snap(positions=(position("P", entry="1.1000"),), orders=(child("T", "1.1200", "STOP"),),
                       quotes={}))
    assert isinstance(r, Result) and r.reason == "stop_present"


def test_ctrader_position_stop_field_is_a_stop() -> None:
    p = position("P", "SELL", units="5000", entry="1.1000", stop="1.1100")
    r = stop_rule(snap(positions=(p,)))
    assert isinstance(r, Result) and r.reason == "stop_present"


def test_limit_child_and_partial_child_are_missing_after_the_grace() -> None:
    key = ("position", f"{AD}:P")
    for orders in ((child("L", "1.0900", "LIMIT"),), (child("C", "1.0900", "STOP", units="60000"),)):
        r = stop_rule(snap(positions=(position("P"),), orders=orders, stop_watch={key: T0 - 2000}))
        assert isinstance(r, Result) and r.level is Level.SOFT_HALT and r.reason == "missing_stop_loss"


def test_unfilled_entry_with_stop_child_beyond_entry_is_missing_not_stop_present() -> None:
    """As before S07: a WORKING buy entry whose only STOP child sits above the entry (a mislabelled
    take-profit) has no stop. MissingStopLossRule goes uncapped SOFT_HALT `missing_stop_loss` after the
    2 s grace, and the account's aggregate risk is not blinded by an UNKNOWN leg."""
    e = order("E", "BUY", OrderStatus.WORKING, entry="1.1000")
    c = order("C", "SELL", OrderStatus.WORKING, entry="1.1050", reduce_only=True, linked="E", kind="STOP")
    p = position("P", stop="1.0900")
    s = snap(positions=(p,), orders=(e, c), stop_watch={("order", e.entity_key): T0 - 5000})
    ctx = EvalContext(s, CFG, T0)
    r = run_rule(MissingStopLossRule(CFG.missing_stop_loss), ctx, ctx.entity_subject(e))
    assert isinstance(r, Result) and r.reason != "stop_present"
    assert r.level is Level.SOFT_HALT and r.reason == "missing_stop_loss" and r.cap is Cap.NONE
    pos = run_rule(MaxPositionRiskRule(CFG.max_position_risk, CFG.sweep.release_margin), ctx,
                   ctx.entity_subject(e))
    assert isinstance(pos, Result) and pos.reason == "no_stop"
    agg = run_rule(MaxAggregateOpenRiskRule(CFG.max_aggregate_open_risk, CFG.sweep.release_margin), ctx,
                   ctx.account_subject())
    assert isinstance(agg, Result) and agg.reason == "aggregate_open_risk"


def test_unknown_detail_names_only_the_inconsistent_child() -> None:
    good = child("G", "1.0900", "STOP", units="60000")
    bad = child("T", "1.1200", "STOP", units="40000")
    r = stop_rule(snap(positions=(position("P", entry="1.1000"),), orders=(good, bad)))
    assert isinstance(r, Unknown) and r.reason == STOP_KIND_INCONSISTENT and r.detail == "stop child T"


def test_stop_mark_records_no_stale_symbol() -> None:
    stale = SymbolQuotes(last_clean=clean_mark("1.1000", T0 - 60_000))
    c = EvalContext(snap(quotes={(AD, "EURUSD"): stale}), CFG, T0)
    m = stop_mark(c, position("P"))
    assert m is not None and m.quality is MarkQuality.LAST_MID
    assert c.book.stale_seen == set() and c.book.qualities == {}


# ---------------------------------------------------------------- RiskSweep with now_ms stepping


async def test_touched_stop_awaiting_fill_is_unknown_only_after_the_grace_then_capped_soft() -> None:
    env = Env(positions=(position("P", entry="1.1000"),), orders=(child("C", "1.1000", "STOP"),))
    env.mid = "1.1050"
    await env.at(T0)
    assert STOP_KEY not in env.db.t.breaches and env.db.t.stop_watch == {}
    env.mid = "1.0995"                     # the market touched the break-even stop; the fill has not arrived
    t1 = T0 + 1000
    await env.every(t1, t1 + GRACE - 1000)
    s = env.state(STOP_KEY)
    assert not s.active and s.unknown_since == t1 and await env.level() is HaltLevel.NONE
    assert env.events("missing_stop_loss") == [] and env.db.t.stop_watch == {}
    await env.at(t1 + GRACE)
    s = env.state(STOP_KEY)
    assert s.active and s.level is Level.SOFT_HALT and s.cap is Cap.SOFT and s.reason == STOP_KIND_INCONSISTENT
    assert await env.level() is HaltLevel.SOFT and env.events("missing_stop_loss") == ["activated"]
    # the fill arrives: the child is FILLED and the position CLOSED, so the state resolves
    await env.at(t1 + GRACE + 1000, positions=(position("P", entry="1.1000", status="CLOSED"),),
                 orders=(child("C", "1.1000", "STOP", status=OrderStatus.FILLED),))
    assert not env.state(STOP_KEY).active and env.events("missing_stop_loss") == ["activated", "resolved"]


async def test_touched_stop_that_fills_inside_the_grace_never_pages() -> None:
    env = Env(positions=(position("P", entry="1.1000"),), orders=(child("C", "1.1000", "STOP"),))
    env.mid = "1.0995"
    await env.every(T0, T0 + 10_000)
    await env.at(T0 + 11_000, positions=(position("P", entry="1.1000", status="CLOSED"),),
                 orders=(child("C", "1.1000", "STOP", status=OrderStatus.FILLED),))
    assert env.events("missing_stop_loss") == [] and await env.level() is HaltLevel.NONE
    assert not [r for r in env.db.t.outbox if "missing_stop_loss" in r.title + r.body]
    s = env.db.t.breaches.get(STOP_KEY)
    assert s is None or (not s.active and s.unknown_since is None)


async def test_mislabelled_take_profit_capped_soft_never_escalates() -> None:
    env = Env(positions=(position("P", entry="1.1000"),), orders=(child("T", "1.1200", "STOP"),))
    await env.at(T0)
    assert env.db.t.stop_watch == {} and not env.state(STOP_KEY).active
    await env.at(T0 + GRACE)
    s = env.state(STOP_KEY)
    assert s.level is Level.SOFT_HALT and s.cap is Cap.SOFT and s.reason == STOP_KIND_INCONSISTENT
    await env.at(T0 + GRACE + CFG.sweep.escalate_after_ms + 1000)
    assert await env.level() is HaltLevel.SOFT and env.state(STOP_KEY).uncapped_ms == 0


async def test_partial_child_is_missing_stop_loss_after_two_seconds() -> None:
    env = Env(positions=(position("P"),), orders=(child("C", "1.0900", "STOP", units="60000"),))
    await env.at(T0)
    assert env.db.t.stop_watch == {("position", f"{AD}:P"): T0}
    await env.at(T0 + 2000)
    s = env.state(STOP_KEY)
    assert s.level is Level.SOFT_HALT and s.cap is Cap.NONE and s.reason == "missing_stop_loss"
    await env.at(T0 + 3000, orders=(child("C", "1.0900", "STOP"),))          # now covers every unit
    assert env.db.t.stop_watch == {}
