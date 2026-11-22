"""Review round 7 (final review): two engines on one source before detection, and receiver wall clock steps.

1. Two engines that share one source (same node and epoch, a restore drill on the spare) switched each other's
   conditions on and off until the rate rule detected `two_engines` (6 beats inside the window): 3 critical pages
   and 2 resolved notices for one open circuit at a 22 s cadence, and the spare's duration conditions paged late
   because each main-box beat dropped their start times. Now the previous beat of a source counts as well from the
   first pair of its beats that came less than 12 s apart on both clocks (one engine never beats that fast).
2. Duration start times and silence ran on the receiver's wall clock. A wall step back of 90 s while a duration
   condition was active gave a false resolved notice and a second page; a step forward of 90 s gave a false
   silence page. Both now run on the monotonic clock.
"""
from __future__ import annotations

from typing import Any

from src.deadman_receiver.evaluate import BeatError, Evaluator, parse_beat
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService
from src.deadman_receiver.settings import ReceiverSettings
from src.runtime.deadman import AdapterBeat
from tests.deadman_receiver.test_evaluate import T0, body, payload
from tests.deadman_receiver.test_service import ENV, Clock, FakeTransport
from tests.deadman_receiver.test_service import payload as svc_payload
from tests.deadman_receiver.test_source_views_round6 import run

S = 1_000
BACKOFF = {"stonex": AdapterBeat("BACKOFF", True, None)}
CIRCUIT = "circuit_open:integrity"
NOT_STREAMING = "adapter_not_streaming:stonex"
MAIN = {"node": "pc1", "epoch": "5"}

# (receive and sent time in ms after T0, payload keywords)
Plan = list[tuple[int, dict[str, Any]]]


def shared(spare: dict[str, Any], *, cadence_s: float, spare_from_s: float = 107.5, until_s: int = 600) -> Plan:
    """The finding's set-up: the main box (pc1 epoch 5) beats every `cadence_s` from 0 s, the spare (also pc1 epoch
    5) every `cadence_s` from `spare_from_s`. Both clocks agree, so every beat is accepted."""
    step = int(cadence_s * S)
    plan: Plan = [(t, MAIN) for t in range(0, until_s * S, step)]
    plan += [(t, {**MAIN, **spare}) for t in range(int(spare_from_s * S), until_s * S, step)]
    return sorted(plan, key=lambda x: x[0])


def changes(plan: Plan, key: str, *, until_s: int = 600) -> list[tuple[float, bool]]:
    """(second, now active) at each change of `key` on a 0.5 s receiver tick."""
    ev = Evaluator(started_ms=T0)
    i, active, out = 0, False, []
    for half in range(until_s * 2):
        t = T0 + half * 500
        while i < len(plan) and T0 + plan[i][0] <= t:
            rx = T0 + plan[i][0]
            try:
                ev.accept(parse_beat(body(payload(sent_at_ms=rx, **plan[i][1]))), rx, rx_ms=rx)
            except BeatError:
                pass
            i += 1
        now = key in ev.active(t, rx_ms=t)
        if now != active:
            out.append((half / 2, now))
            active = now
    return out


# ---------------------------------------------------------------- 1. two engines on one source
def test_shared_source_open_circuit_pages_once_at_22_s() -> None:
    """The finding's reproduction (it gave PAGE 107.5, RESOLVED 110, PAGE 129.5, RESOLVED 132, PAGE 151.5)."""
    assert changes(shared({"degraded": ["integrity"]}, cadence_s=22), CIRCUIT) == [(107.5, True)]


def test_shared_source_open_circuit_pages_once_at_15_s() -> None:
    assert changes(shared({"degraded": ["integrity"]}, cadence_s=15), CIRCUIT) == [(107.5, True)]


def test_shared_source_duration_condition_is_not_late() -> None:
    """The spare in BACKOFF: adapter_not_streaming pages 120 s after the spare's first beat (it paged at 293.5 s)."""
    assert changes(shared({"adapters": BACKOFF}, cadence_s=22), NOT_STREAMING) == [(227.5, True)]


def test_shared_source_condition_resolves_after_the_spare_stops() -> None:
    plan = [(t, kw) for t, kw in shared({"degraded": ["integrity"]}, cadence_s=22)
            if "degraded" not in kw or t <= 300 * S]
    seen = changes(plan, CIRCUIT)
    assert [c[1] for c in seen] == [True, False]
    assert 300 < seen[1][0] <= 300 + 2 * 22 + 1


def test_one_engine_never_counts_as_shared() -> None:
    """One engine beats every 15 s; a stalled receiver handles two of its queued beats in the same millisecond (the
    receive gap is 0 but the sent gap stays 15 s): no two_engines. Round 9: the circuit the older beat showed (it
    really was open at 90 s) is held SOURCE_LIVE_MS like any condition a later beat no longer shows (one beat that
    does not show a condition cannot end it: it may come from a second engine on the same source), then resolves.
    Round 10: the fast-pair "shared source" mode is gone (a single engine's restart under the same source looked like
    one, and paged an ended condition), so only the two_engines rules are checked here."""
    ev = Evaluator(started_ms=T0)
    for k in range(6):
        t = T0 + k * 15 * S
        ev.accept(parse_beat(body(payload(sent_at_ms=t, **MAIN))), t, rx_ms=t)
    late = T0 + 100 * S
    ev.accept(parse_beat(body(payload(sent_at_ms=T0 + 90 * S, degraded=["integrity"], **MAIN))), late, rx_ms=late)
    ev.accept(parse_beat(body(payload(sent_at_ms=T0 + 105 * S, **MAIN))), late, rx_ms=late)
    assert "two_engines" not in ev.active(late, rx_ms=late)
    assert CIRCUIT in ev.active(late, rx_ms=late)
    for k in range(1, 3):                       # the engine beats on, clean
        t = late + k * 15 * S
        ev.accept(parse_beat(body(payload(sent_at_ms=T0 + (105 + k * 15) * S, **MAIN))), t, rx_ms=t)
    assert CIRCUIT not in ev.active(late + 41 * S, rx_ms=late + 41 * S)
    assert "two_engines" not in ev.active(late + 41 * S, rx_ms=late + 41 * S)


async def test_shared_source_through_the_receiver_service() -> None:
    """The paging path: one critical page for the spare's open circuit and no resolved notice."""
    clock = Clock()
    t = FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([t], clock), clock_ms=clock)
    start = clock.ms
    plan = shared({"degraded": ["integrity"]}, cadence_s=22, until_s=400)
    i = 0
    for half in range(800):
        clock.ms = start + half * 500
        while i < len(plan) and start + plan[i][0] <= clock.ms:
            svc.on_beat(svc_payload(start + plan[i][0], **plan[i][1]).to_json().encode())
            i += 1
        await svc.tick()
    circuit = [m for m in t.sent if "task circuit open (integrity)" in m.subject]
    assert [m.kind for m in circuit if "still active" not in m.subject] == ["episode"]
    assert not [m for m in circuit if "resolved" in m.subject]


# ---------------------------------------------------------------- 2. receiver wall clock steps
def backoff_engine() -> list[tuple[int, int, dict[str, Any]]]:
    return [(s, s, {"node": "pc1", "epoch": "5", "adapters": BACKOFF}) for s in range(0, 400, 15)]


def test_wall_step_back_keeps_a_duration_condition() -> None:
    """The finding's reproduction: it gave [(120, True), (200, False), (210, True)]."""
    assert run(backoff_engine(), NOT_STREAMING, until_s=400, step_at_s=200, step_ms=90 * S) == [(120, True)]


def test_wall_step_forward_starts_no_duration_early() -> None:
    assert run(backoff_engine(), NOT_STREAMING, until_s=400, step_at_s=60, step_ms=-90 * S) == [(120, True)]


def test_wall_step_forward_is_not_silence() -> None:
    """The finding's reproduction: it gave [(200, True), (210, False)] although no beat was missed."""
    plan = [(s, s, {"node": "pc1", "epoch": "5"}) for s in range(0, 400, 15)]
    assert run(plan, "silence", until_s=400, step_at_s=200, step_ms=-90 * S) == []
    assert run(plan, "silence", until_s=400, step_at_s=200, step_ms=90 * S) == []


def test_real_silence_still_pages_on_the_monotonic_clock() -> None:
    """The engine stops after its beat at 150 s: silence pages 61 s later, whatever the wall clock does."""
    plan = [(s, s, {"node": "pc1", "epoch": "5"}) for s in range(0, 151, 15)]
    assert run(plan, "silence", until_s=400) == [(211, True)]
    assert run(plan, "silence", until_s=400, step_at_s=180, step_ms=90 * S) == [(211, True)]


def test_silence_before_the_first_beat_uses_the_monotonic_start() -> None:
    """The receiver starts with wall and monotonic clocks far apart (production): silence counts from the monotonic
    start, so a wall step forward does not page early."""
    ev = Evaluator(started_ms=T0, started_rx=5_000)
    assert "silence" not in ev.active(T0 + 120 * S, rx_ms=5_000 + 30 * S)
    assert "silence" in ev.active(T0 + 61 * S, rx_ms=5_000 + 61 * S)


async def test_service_wall_step_sends_no_extra_page() -> None:
    """Through ReceiverService with separate wall and monotonic clocks: an engine in BACKOFF, the wall clock steps
    back 90 s at 200 s and forward 180 s at 300 s. One adapter page, no resolved notice, no silence page."""
    wall, mono = Clock(), Clock()
    mono.ms = 10_000
    t = FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([t], wall), clock_ms=wall, mono_ms=mono)
    start = wall.ms
    for sec in range(400):
        offset = -90 * S if 200 <= sec < 300 else (90 * S if sec >= 300 else 0)
        wall.ms = start + sec * S + offset
        mono.ms = 10_000 + sec * S
        if sec % 15 == 0:
            svc.on_beat(svc_payload(start + sec * S, node="pc1", epoch="5", adapters=BACKOFF).to_json().encode())
        await svc.tick()
    adapter = [m for m in t.sent if "not STREAMING" in m.subject and "still active" not in m.subject]
    assert [m.kind for m in adapter] == ["episode"]
    assert not [m for m in t.sent if "resolved" in m.subject]
    assert not [m for m in t.sent if "no heartbeat" in m.subject]
