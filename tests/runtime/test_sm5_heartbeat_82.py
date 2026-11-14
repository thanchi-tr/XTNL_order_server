"""IMPLEMENTATION_PLAN 8.2: the heartbeat's sm5_state is UNKNOWN whenever SM5 is UNKNOWN.

Before: `_DeadmanView.sm5_state` was the PostgreSQL halt level as soon as SM8 was READY, even with Redis
`halt:state` missing or behind MAX(halt_events.id) (the gate denies everything then: SM5 edge hx), so the
independent Melbourne page 'SM5 UNKNOWN > 60 s' never fired. Now the housekeeping read compares halt:state with
MAX(halt_events.id) exactly as the gate does, and any read error leaves SM5 UNKNOWN. No network: fakeredis +
lupa, in-memory ports."""
from __future__ import annotations

from typing import Any

from src.cache import keys as K
from src.integrity.ports import BootState
from src.oversee.types import HaltLevel
from src.runtime.deadman import build_payload
from src.runtime.session import _DeadmanView
from tests.runtime.test_wiring_74 import T0, Built, built  # noqa: F401  (fixture)


async def test_sm5_is_unknown_unless_halt_state_carries_the_latest_event_id(built: Built) -> None:
    s = built.session
    view = _DeadmanView(s)
    s.sm8.state = BootState.READY
    await s.refresh_halt_view()
    assert view.sm5_state() == "UNKNOWN"                      # no halt_events row: the gate is shut too

    ev = await s.halt.trip(HaltLevel.SOFT, rule_id="max_position_risk", reason="r", now_ms=T0)
    assert ev is not None
    await s.refresh_halt_view()
    assert view.sm5_state() == "SOFT" and build_payload(view, T0).sm5_state == "SOFT"

    await s.redis.delete(K.HALT_STATE)                        # SM8 READY, PostgreSQL SOFT, halt:state missing
    await s.refresh_halt_view()
    assert view.halt_level() == "SOFT"
    assert view.sm5_state() == "UNKNOWN" and build_payload(view, T0).sm5_state == "UNKNOWN"

    await s.halt_mirror.cas_set(HaltLevel.SOFT, ev.id)        # restored ...
    ev2 = await s.halt.trip(HaltLevel.HARD, rule_id="max_position_risk", reason="r2", now_ms=T0 + 1)
    assert ev2 is not None
    await s.refresh_halt_view()
    assert view.sm5_state() == "HARD"
    await s.redis.delete(K.HALT_STATE)
    await s.halt_mirror.cas_set(HaltLevel.SOFT, ev.id)        # ... behind MAX(halt_events.id)
    await s.refresh_halt_view()
    assert view.sm5_state() == "UNKNOWN"

    await s.halt_mirror.cas_set(HaltLevel.HARD, ev2.id)
    await s.refresh_halt_view()
    assert view.sm5_state() == "HARD"
    s.sm8.state = BootState.HEAL                              # SM8 not READY: UNKNOWN whatever halt:state says
    assert view.sm5_state() == "UNKNOWN"


async def test_an_unreadable_halt_state_or_status_leaves_sm5_unknown(built: Built) -> None:
    s = built.session
    view = _DeadmanView(s)
    s.sm8.state = BootState.READY
    ev = await s.halt.trip(HaltLevel.SOFT, rule_id="max_position_risk", reason="r", now_ms=T0)
    assert ev is not None
    await s.refresh_halt_view()
    assert view.sm5_state() == "SOFT"

    async def boom(*_: Any) -> Any:
        raise ConnectionError("redis down")

    s.halt_mirror.read = boom                                 # type: ignore[method-assign]
    await s.refresh_halt_view()
    assert view.sm5_state() == "UNKNOWN"
    del s.halt_mirror.read                                    # back to the class method
    await s.refresh_halt_view()
    assert view.sm5_state() == "SOFT"
    s.halt.status = boom                                      # type: ignore[method-assign]
    await s.refresh_halt_view()
    assert view.sm5_state() == "UNKNOWN"


async def test_housekeeping_refreshes_the_halt_view(built: Built) -> None:
    """The production path: the housekeeping task (every 10 s) runs the halt:state test, not only a direct call."""
    import asyncio

    from src.runtime.supervisor import TaskContext

    s = built.session
    view = _DeadmanView(s)
    s.sm8.state = BootState.READY
    ev = await s.halt.trip(HaltLevel.SOFT, rule_id="max_position_risk", reason="r", now_ms=T0)
    assert ev is not None and view.sm5_state() == "UNKNOWN"   # nothing read yet: UNKNOWN (fail closed)
    task = asyncio.create_task(s._housekeeping_body(TaskContext("housekeeping", lambda: 0.0)))
    try:
        for _ in range(200):
            if s.halt_state_current:
                break
            await asyncio.sleep(0.01)
        assert view.sm5_state() == "SOFT" and view.halt_level() == "SOFT"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def test_the_first_dead_man_beat_already_carries_the_halt_view(built: Built) -> None:
    """LEADER_INIT step 7 reads the halt view before the dead-man sender starts, so the first beat never carries a
    null halt_level (Melbourne pages a null health field at once, 8.2) because housekeeping has not run yet."""
    s = built.session
    s.sm8.state = BootState.READY
    ev = await s.halt.trip(HaltLevel.SOFT, rule_id="max_position_risk", reason="r", now_ms=T0)
    assert ev is not None
    s.halt_level_name = None
    s.halt_mirror.level = None
    seen: list[tuple[str | None, str | None]] = []
    view = _DeadmanView(s)
    s._add = lambda spec: None                                        # type: ignore[method-assign]
    s._start_notifications = lambda: seen.append((view.halt_level(), view.sm5_state()))  # type: ignore[method-assign]
    await s.startup_barrier()
    assert seen == [("SOFT", "SOFT")]
