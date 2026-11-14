"""HaltSwitch semantics (oversee-rules.md §4, §8) against the in-memory HaltStore."""
from __future__ import annotations

import pytest

from src.oversee.halt import HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.types import BreachState, Cap, HaltLevel, Level


def state(rule: str, level: Level, cap: Cap = Cap.NONE, uncapped: int = 0, **kw: object) -> BreachState:
    return BreachState(rule, "account", "A1", "A1", level=level, cap=cap, reason=rule, active_since=0,
                       activation_id=1, uncapped_ms=uncapped, **kw)  # type: ignore[arg-type]


def setup(*states: BreachState) -> tuple[InMemoryOverseeDb, HaltSwitch]:
    db = InMemoryOverseeDb()
    for s in states:
        db.t.breaches[s.key] = s
    return db, HaltSwitch(db, db)


async def test_trip_writes_and_alerts_only_on_rise() -> None:
    db, hs = setup()
    ev = await hs.trip(HaltLevel.SOFT, rule_id="r", reason="x", now_ms=10)
    assert ev is not None and ev.effective_level is HaltLevel.SOFT and not ev.hard_latch
    assert await hs.trip(HaltLevel.SOFT, rule_id="r", reason="x", now_ms=11) is None
    hard = await hs.trip(HaltLevel.HARD, rule_id="r", reason="y", now_ms=12)
    assert hard is not None and hard.hard_latch
    assert len(db.t.halt_events) == 2 and [r.kind for r in db.t.outbox] == ["trip", "trip"]
    assert db.halt_state == (HaltLevel.HARD, hard.id)
    status = await hs.status()
    assert status.effective_level is HaltLevel.HARD and status.rose_at_ms == 12


async def test_trip_refused_after_newer_operator_clear() -> None:
    db, hs = setup()
    await hs.trip(HaltLevel.SOFT, rule_id="r", reason="x", now_ms=10)
    await hs.operator_clear("op", "reviewed", 1, now_ms=20)
    assert await hs.trip(HaltLevel.HARD, rule_id="r", reason="escalation", now_ms=30, basis_ms=15) is None
    assert await hs.trip(HaltLevel.HARD, rule_id="r", reason="escalation", now_ms=30, basis_ms=25) is not None


async def test_system_clear_refused_with_latch_or_active_soft() -> None:
    db, hs = setup(state("r", Level.SOFT_HALT))
    await hs.trip(HaltLevel.SOFT, rule_id="r", reason="x", now_ms=1)
    assert await hs.system_clear(reason="cool_down", now_ms=2) is None
    db.t.breaches.clear()
    ev = await hs.system_clear(reason="cool_down", now_ms=3)
    assert ev is not None and ev.effective_level is HaltLevel.NONE
    await hs.trip(HaltLevel.HARD, rule_id="r", reason="x", now_ms=4)
    assert await hs.system_clear(reason="cool_down", now_ms=5) is None


async def test_clear_refused_on_newer_event() -> None:
    db, hs = setup()
    await hs.trip(HaltLevel.SOFT, rule_id="r", reason="x", now_ms=1)
    await hs.trip(HaltLevel.HARD, rule_id="r", reason="x", now_ms=2)
    out = await hs.operator_clear("op", "ok", reviewed_event_id=1, now_ms=3)
    assert out.status == "refused" and out.reason.startswith("newer_halt_event")


async def test_clear_refused_while_hard_active_names_it() -> None:
    db, hs = setup(state("max_daily_loss", Level.HARD_HALT))
    await hs.trip(HaltLevel.HARD, rule_id="max_daily_loss", reason="x", now_ms=1)
    out = await hs.operator_clear("op", "ok", 1, now_ms=2)
    assert out.status == "refused" and "max_daily_loss" in out.reason
    out = await hs.operator_clear("op", "forced", 1, force=True, now_ms=2)
    assert out.status == "refused" and out.reason.startswith("hard_state_uncapped")
    assert (await hs.status()).hard_latch


async def test_forced_clear_resolves_capped_hard() -> None:
    db, hs = setup(state("max_daily_loss", Level.HARD_HALT, Cap.SOFT), state("agg", Level.SOFT_HALT, uncapped=5000))
    await hs.trip(HaltLevel.HARD, rule_id="max_daily_loss", reason="x", now_ms=1)
    out = await hs.operator_clear("op", "", 1, force=True, now_ms=2)
    assert out.status == "refused" and out.reason == "force_requires_reason"
    out = await hs.operator_clear("op", "feed frozen, verified by phone", 1, force=True, now_ms=2)
    assert out.status == "cleared" and out.event is not None
    assert out.event.effective_level is HaltLevel.SOFT and not out.event.hard_latch   # the sweep's target
    hard = db.t.breaches[("max_daily_loss", "account", "A1")]
    assert hard.resolved_by == "operator" and not hard.active
    soft = db.t.breaches[("agg", "account", "A1")]
    assert soft.uncapped_ms == 0 and soft.clear_generation == 1
    assert db.halt_state == (HaltLevel.SOFT, out.event.id)


async def test_clear_releases_escalation_latch_and_resets_clocks() -> None:
    db, hs = setup(state("agg", Level.SOFT_HALT, uncapped=900_000), state("f", Level.FLAG, uncapped=7))
    await hs.trip(HaltLevel.HARD, rule_id="agg", reason="escalation", now_ms=1)
    out = await hs.operator_clear("op", "reviewed", 1, now_ms=2)
    assert out.status == "cleared" and out.event is not None and out.event.effective_level is HaltLevel.SOFT
    assert all(s.uncapped_ms == 0 and s.clear_generation == 1 for s in db.t.breaches.values())


async def test_acknowledge_records_and_changes_nothing() -> None:
    s = state("agg", Level.SOFT_HALT)
    db, hs = setup(s)
    ack = await hs.acknowledge("op", "known", s.key, now_ms=5)
    assert ack.level is Level.SOFT_HALT and db.t.breaches[s.key] == s and db.t.halt_events == []
    with pytest.raises(LookupError):
        await hs.acknowledge("op", "x", ("nope", "account", "A1"), now_ms=5)


async def test_mirror_cas_never_goes_backwards() -> None:
    db = InMemoryOverseeDb()
    assert await db.cas_set(HaltLevel.SOFT, 2)
    assert not await db.cas_set(HaltLevel.NONE, 1)
    assert db.halt_state == (HaltLevel.SOFT, 2)
