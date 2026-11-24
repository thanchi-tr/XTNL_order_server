"""Live review 2026-11-25, finding 5, as reverted in round 2: MissingStopLossRule's grace does not hold an active
state. oversee-rules.md §2 (unchanged) says a capped UNKNOWN state, `stop_kind_inconsistent` included, resolves once
a known result is below SOFT, and Release resolves an uncapped state after `release_ms` below its level. Whether the
grace should hold the state instead is an open owner decision (docs/design/live-review-2026-11-25.md).
Synthetic prices only."""
from __future__ import annotations

import pytest

from src.dto import OrderDTO, OrderKind, OrderStatus, Side
from src.oversee.config import OverseeConfig
from src.oversee.risk import STOP_KIND_INCONSISTENT
from src.oversee.types import Cap, HaltLevel, Level
from tests.oversee.helpers import AD, T0, order, position
from tests.oversee.test_sweep import STOP_KEY, Env

pytestmark = pytest.mark.usefixtures("stop_at_entry_on")

CFG = OverseeConfig()
GRACE = CFG.sweep.unknown_grace_ms
STOP_GRACE = CFG.missing_stop_loss.grace_ms
RELEASE = CFG.sweep.release_ms


def child(oid: str, level: str, kind: OrderKind | None, side: Side = "SELL",
          status: OrderStatus = OrderStatus.WORKING) -> OrderDTO:
    return order(oid, side, status, ordered="100000", entry=level, reduce_only=True, linked="P", kind=kind,
                 remaining="0" if status.terminal else None)


def activations(env: Env) -> set[int | None]:
    return {e.activation_id for e in env.db.t.events.values() if e.rule_id == "missing_stop_loss"}


def resolved_rows(env: Env) -> list[str]:
    return [r.title for r in env.db.t.outbox if "missing_stop_loss" in r.title + r.body and "resolved" in
            (r.title + r.body).lower()]


async def inconsistent_state_active() -> Env:
    """A long whose only STOP child sits above the mark (a mislabelled take-profit): capped SOFT after 30 s."""
    env = Env(positions=(position("P", entry="1.1000"),), orders=(child("T", "1.1200", "STOP"),))
    await env.at(T0)
    await env.at(T0 + GRACE)
    s = env.state(STOP_KEY)
    assert s.active and s.cap is Cap.SOFT and s.reason == STOP_KIND_INCONSISTENT and s.unknown_origin
    assert env.events("missing_stop_loss") == ["activated"]
    return env


async def test_cancelling_the_inconsistent_child_resolves_then_a_fresh_missing_stop_episode_activates() -> None:
    """§2 UNKNOWN: the capped state resolves on the first known PASS below SOFT (the grace's `stop_grace`); once the
    2 s grace ends, the still missing stop is a new `missing_stop_loss` episode."""
    env = await inconsistent_state_active()
    first = env.state(STOP_KEY).activation_id
    cancelled = (child("T", "1.1200", "STOP", status=OrderStatus.CANCELLED),)
    t = T0 + GRACE + 1000
    env.kw.update(orders=cancelled)                         # the only stop is gone: the grace starts
    await env.at(t)
    s = env.state(STOP_KEY)
    assert not s.active and s.resolved_by == "sweep" and not s.unknown_origin
    assert env.events("missing_stop_loss") == ["activated", "resolved"]
    await env.every(t + 1000, t + STOP_GRACE)
    s = env.state(STOP_KEY)
    assert env.events("missing_stop_loss") == ["activated", "resolved", "activated"]
    assert s.active and s.level is Level.SOFT_HALT and s.cap is Cap.NONE and s.reason == "missing_stop_loss"
    assert s.activation_id != first and len(activations(env)) == 2


async def test_a_stop_placed_during_the_grace_leaves_the_state_resolved() -> None:
    env = await inconsistent_state_active()
    t = T0 + GRACE + 1000
    await env.at(t, orders=(child("T", "1.1200", "STOP", status=OrderStatus.CANCELLED),))
    await env.at(t + 1000, orders=(child("S", "1.0900", "STOP"),))
    assert not env.state(STOP_KEY).active and env.events("missing_stop_loss") == ["activated", "resolved"]


async def test_a_fresh_missing_stop_still_activates_after_the_grace() -> None:
    env = Env(positions=(position("P"),))
    await env.at(T0)
    await env.at(T0 + STOP_GRACE - 1000)
    assert STOP_KEY not in env.db.t.breaches or not env.state(STOP_KEY).active
    await env.at(T0 + STOP_GRACE)
    s = env.state(STOP_KEY)
    assert s.active and s.cap is Cap.NONE and s.reason == "missing_stop_loss"
    assert env.events("missing_stop_loss") == ["activated"]


async def test_a_stop_that_flickers_on_an_uncapped_state_resolves_after_release_then_reactivates() -> None:
    """A stop present for 3 s and then gone again: the release wait that started with the stop (5 s, §2 Release)
    ends inside the new 2 s grace, so the state resolves, and the grace's end activates a new episode."""
    env = Env(positions=(position("P"),))
    await env.every(T0, T0 + STOP_GRACE)
    assert env.events("missing_stop_loss") == ["activated"]
    t = T0 + STOP_GRACE + 1000
    env.kw.update(orders=(child("S", "1.0900", "STOP"),))
    await env.every(t, t + 3000)
    env.kw.update(orders=())
    await env.every(t + 4000, t + 4000 + RELEASE + STOP_GRACE)
    s = env.state(STOP_KEY)
    assert env.events("missing_stop_loss") == ["activated", "resolved", "activated"]
    assert s.active and s.cap is Cap.NONE and s.reason == "missing_stop_loss"
