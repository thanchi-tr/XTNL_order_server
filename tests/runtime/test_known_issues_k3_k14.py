"""IMPLEMENTATION_PLAN known issues K3 and K14 (fixed 2026-11-18).

K3 (tier-persistence.md §2): a post-commit Redis write refused with NO_BOOT (or lost to a Redis error) is kept in
the NoBootQueue AND signals SM8. Before, `NoBootQueue.on_enqueue` was not wired, so the queued write waited for SM8's
next periodic pass or another trigger.

K14 (operations.md §9, oversee-rules.md §8): the heartbeat's gate_state is CLOSED in every case where ActionGate
denies every action. Before, it was OPEN with halt:state missing / behind, at a HARD level, without the lease, and
while draining.

No network: fakeredis + lupa, in-memory ports."""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from src.cache import keys as K
from src.cache.scripts import ApplyResult, EntityWrite, instrument_version
from src.integrity.ports import BootState
from src.oversee.types import HaltLevel
from src.runtime.deadman import build_payload
from src.runtime.session import NO_BOOT_SIGNAL_MIN_S, _DeadmanView
from tests.runtime.test_wiring_74 import T0, Built, built  # noqa: F401  (fixture)


def _write(n: int = 1) -> EntityWrite:
    return EntityWrite(key=f"order:k3-{n}", version=instrument_version(n), seq_id=n, checksum=f"c{n}",
                       payload="{}", entity_type="ORDER", entity_id=f"k3-{n}", account_id="ACC")


# ============================================================================================ K3
async def test_k3_a_no_boot_after_commit_queues_the_write_and_signals_sm8(built: Built) -> None:
    s = built.session
    assert list(s.sm8.signals) == []
    res = await s.cache.apply_after_commit(_write(1))           # no redis:boot_id: NO_BOOT
    assert res is ApplyResult.NO_BOOT and len(s.no_boot) == 1
    assert [x for x in s.sm8.signals if x.startswith("NO_BOOT")] == ["NO_BOOT: post-commit Redis write queued"]
    # A burst inside the rate-limit interval adds no signal (SM8's own 5 s pass still covers it).
    await s.cache.apply_after_commit(_write(2))
    assert len(s.no_boot) == 2 and len(s.sm8.signals) == 1
    assert NO_BOOT_SIGNAL_MIN_S <= 1.0


async def test_k3_a_redis_error_after_commit_is_replayed_at_once_not_on_the_next_periodic_pass(built: Built) -> None:
    """SM8's serve loop re-checks only every 5 s on its own; the signal wakes it, so a write lost to a passing Redis
    error is replayed well inside that."""
    s = built.session
    await s.redis.set(K.BOOT_ID, await s.identity.refresh())    # Redis booted for this term
    stop = asyncio.Event()
    task = asyncio.create_task(s.sm8.run(stop))
    try:
        for _ in range(200):
            if s.sm8.state is BootState.READY:
                break
            await asyncio.sleep(0.005)
        assert s.sm8.state is BootState.READY
        await asyncio.sleep(0.05)                               # the loop is now waiting on its wake event
        real = s.cache.apply

        async def flaky(w: EntityWrite, *, lock_token: str | None = None) -> ApplyResult:
            s.cache.apply = real                                # type: ignore[method-assign]
            raise ConnectionError("redis blip")

        s.cache.apply = flaky                                   # type: ignore[method-assign]
        w = _write(7)
        assert await s.cache.apply_after_commit(w) is ApplyResult.NO_BOOT and len(s.no_boot) == 1
        for _ in range(100):                                    # <= 1 s, far less than the 5 s periodic pass
            if not len(s.no_boot):
                break
            await asyncio.sleep(0.01)
        assert len(s.no_boot) == 0
        stored = await s.redis.hgetall(w.key)
        assert stored, "the queued write reached Redis"
    finally:
        stop.set()
        s.sm8.signal("test stop")
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, 2)


async def test_k3_a_closed_session_does_not_signal(built: Built) -> None:
    s = built.session
    await s.aclose(1.0)
    s.no_boot.add(_write(3))
    assert list(s.sm8.signals) == []


async def test_k3_the_sm8_signal_log_is_bounded(built: Built) -> None:
    s = built.session
    for i in range(2_000):
        s.sm8.signal(f"s{i}")
    assert len(s.sm8.signals) <= 500 and s.sm8.signals[-1] == "s1999"


# ============================================================================================ K14
async def _open(s: Any) -> None:
    s.sm8.state = BootState.READY
    ev = await s.halt.trip(HaltLevel.SOFT, rule_id="max_position_risk", reason="r", now_ms=T0)
    assert ev is not None
    await s.refresh_halt_view()


async def test_k14_gate_state_is_open_only_when_some_action_may_pass(built: Built) -> None:
    s = built.session
    view = _DeadmanView(s)
    s.sm8.state = BootState.READY
    await s.refresh_halt_view()
    assert view.gate_state() == "CLOSED"                        # no halt_events row: halt:state not current
    await _open(s)
    assert view.gate_state() == "OPEN" and build_payload(view, T0).gate_state == "OPEN"   # SOFT: reducing only


async def test_k14_gate_state_closed_while_halt_state_is_missing_or_behind(built: Built) -> None:
    s = built.session
    view = _DeadmanView(s)
    await _open(s)
    await s.redis.delete(K.HALT_STATE)
    await s.refresh_halt_view()
    assert view.gate_state() == "CLOSED"                        # before K14: OPEN (SM8 READY, lease valid)

    async def boom(*_: Any) -> Any:
        raise ConnectionError("redis down")

    s.halt_mirror.read = boom                                   # type: ignore[method-assign]
    await s.refresh_halt_view()
    assert view.gate_state() == "CLOSED"


async def test_k14_gate_state_closed_at_a_hard_level(built: Built) -> None:
    s = built.session
    view = _DeadmanView(s)
    await _open(s)
    ev = await s.halt.trip(HaltLevel.HARD, rule_id="max_position_risk", reason="r2", now_ms=T0 + 1)
    assert ev is not None
    await s.refresh_halt_view()
    assert s.halt_state_current and view.halt_level() == "HARD"
    assert view.gate_state() == "CLOSED"                        # before K14: OPEN


async def test_k14_gate_state_closed_without_the_lease_sm8_or_while_draining(built: Built) -> None:
    s = built.session
    view = _DeadmanView(s)
    await _open(s)
    assert view.gate_state() == "OPEN"
    s.ctx.lease.holds_lease = lambda: False                     # type: ignore[method-assign]
    assert view.gate_state() == "CLOSED"                        # no node epoch: boot_id_missing at the gate
    del s.ctx.lease.holds_lease
    s.ctx.lease.valid = False                                   # type: ignore[attr-defined]
    assert view.gate_state() == "CLOSED"
    s.ctx.lease.valid = True                                    # type: ignore[attr-defined]
    s.sm8.state = BootState.HEAL
    assert view.gate_state() == "CLOSED"
    s.sm8.state = BootState.READY
    assert view.gate_state() == "OPEN"
    s._draining = True
    assert view.gate_state() == "CLOSED"


# ============================================================================================ review follow-ups
async def test_k3_a_write_suppressed_inside_the_interval_gets_one_trailing_signal(built: Built,
                                                                                 monkeypatch: Any) -> None:
    """Review follow-up: a write queued just after the first signal (e.g. after SM8 took its replay snapshot) was
    left to SM8's 5 s pass; it now gets one signal at the end of the interval, and never more than one per interval."""
    import src.runtime.session as session_mod
    monkeypatch.setattr(session_mod, "NO_BOOT_SIGNAL_MIN_S", 0.05)
    s = built.session
    await s.cache.apply_after_commit(_write(1))
    await s.cache.apply_after_commit(_write(2))
    await s.cache.apply_after_commit(_write(3))                 # a burst: one trailing signal, not two
    assert len(s.sm8.signals) == 1
    await asyncio.sleep(0.15)
    assert list(s.sm8.signals) == ["NO_BOOT: post-commit Redis write queued",
                                   "NO_BOOT: post-commit Redis write queued (trailing)"]


async def test_k3_no_trailing_signal_once_the_queue_is_replayed_or_the_session_closed(built: Built,
                                                                                     monkeypatch: Any) -> None:
    import src.runtime.session as session_mod
    monkeypatch.setattr(session_mod, "NO_BOOT_SIGNAL_MIN_S", 0.05)
    s = built.session
    await s.cache.apply_after_commit(_write(1))
    await s.cache.apply_after_commit(_write(2))
    for w in s.no_boot.snapshot():                              # SM8 replayed both before the interval ended
        s.no_boot.discard(w)
    await asyncio.sleep(0.15)
    assert len(s.sm8.signals) == 1
    await s.cache.apply_after_commit(_write(3))                 # signal 2 (interval passed)
    await s.cache.apply_after_commit(_write(4))                 # trailing scheduled ...
    await s.aclose(1.0)                                         # ... and cancelled by the close
    await asyncio.sleep(0.15)
    assert len(s.sm8.signals) == 2


async def test_k14_the_beat_refresh_reports_a_hard_trip_without_waiting_for_housekeeping(built: Built) -> None:
    """Review follow-up: the halt fields were refreshed only by the 10 s housekeeping pass, so a beat could say OPEN
    after a HARD trip. The dead-man sender now refreshes them right before building each payload."""
    s = built.session
    view = _DeadmanView(s)
    await _open(s)
    assert view.gate_state() == "OPEN"
    ev = await s.halt.trip(HaltLevel.HARD, rule_id="max_position_risk", reason="r2", now_ms=T0 + 1)
    assert ev is not None
    assert view.gate_state() == "OPEN"                          # the stale view: what the beat used to send
    sent: list[str | None] = []

    class Transport:
        host = "deadman.test"

        async def post(self, payload: Any) -> None:
            sent.append(payload.gate_state)

        async def aclose(self) -> None:
            return None

    from src.runtime.deadman import DeadmanSender
    sender = DeadmanSender(None, view, lease_valid=lambda: True, is_leader=lambda: True,
                           transport=Transport(), prepare=s.refresh_beat_view)  # type: ignore[arg-type]
    assert await sender.send_once() and sent == ["CLOSED"]


async def test_k14_the_beat_refresh_fails_closed_on_a_read_error_or_overrun(built: Built, monkeypatch: Any) -> None:
    import src.runtime.session as session_mod
    s = built.session
    view = _DeadmanView(s)
    await _open(s)
    assert view.gate_state() == "OPEN"

    async def hang() -> Any:
        await asyncio.sleep(10)

    monkeypatch.setattr(session_mod, "BEAT_VIEW_REFRESH_S", 0.05)
    s.halt.status = hang                                        # type: ignore[method-assign]
    await s.refresh_beat_view()
    assert view.gate_state() == "CLOSED" and view.sm5_state() == "UNKNOWN"


async def test_k14_gate_state_closed_when_engine_epoch_is_not_this_nodes(built: Built) -> None:
    """Review follow-up: the gate's `epoch_mismatch` condition (node epoch = engine_epoch) is part of gate_state."""
    s = built.session
    view = _DeadmanView(s)
    await _open(s)
    assert s.engine_epoch_current and view.gate_state() == "OPEN"
    built.backend.tables.epoch = s.epoch.token + 1              # another leader bumped the epoch
    await s.refresh_halt_view()
    assert not s.engine_epoch_current and view.gate_state() == "CLOSED"
    built.backend.tables.epoch = s.epoch.token

    async def boom(*_: Any) -> Any:
        raise ConnectionError("postgres down")

    s._gate_reader.read = boom                                  # type: ignore[method-assign]
    await s.refresh_halt_view()
    assert view.gate_state() == "CLOSED"


async def test_k14_an_older_refresh_never_overwrites_a_newer_one(built: Built) -> None:
    s = built.session
    view = _DeadmanView(s)
    await _open(s)
    gate = asyncio.Event()
    real = s.halt_mirror.read
    calls = 0

    async def slow_first() -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            await gate.wait()                                   # the older refresh reads the mirror late
            return None                                         # ... and would report halt:state missing
        return await real()

    s.halt_mirror.read = slow_first                             # type: ignore[method-assign]
    older = asyncio.create_task(s.refresh_halt_view())
    await asyncio.sleep(0.01)
    await s.refresh_halt_view()                                 # the newer one lands first: current
    gate.set()
    await older
    assert s.halt_state_current and view.gate_state() == "OPEN"


async def test_k2_the_abort_close_uses_the_terms_drain_aclose_budget(built: Built) -> None:
    s = built.session
    seen: list[float] = []

    async def aclose(timeout_s: float = 5.0) -> None:
        seen.append(timeout_s)

    s.aclose = aclose                                           # type: ignore[method-assign]
    await s.aclose_for_abort()
    assert seen == [s._t.drain_aclose_s] and s.aclose_budget_s == s._t.drain_aclose_s < 5.0
