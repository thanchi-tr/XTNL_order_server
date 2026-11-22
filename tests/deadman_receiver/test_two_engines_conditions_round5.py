"""Review round 5: while two engines beat, the other conditions do not switch on and off at each beat.

The finding: two engines whose clocks agree both have their beats accepted, in turn. Every condition came from the
one newest beat, so a condition only one engine showed switched on at its beat and off at the other's (a critical
page plus a resolved notice about every 15 s while both ran), and the other engine's beats reset the duration start
times, so a duration condition shown by one engine never paged. The conditions now come from the latest beat of
each source that still beats, each source with its own duration start times.
"""
from __future__ import annotations

from typing import Any

from src.deadman_receiver.evaluate import SOURCE_LIVE_MS, BeatError, Evaluator, parse_beat
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService
from src.deadman_receiver.settings import ReceiverSettings
from tests.deadman_receiver.test_evaluate import T0, body, payload
from tests.deadman_receiver.test_service import ENV, Clock, FakeTransport
from tests.deadman_receiver.test_service import payload as svc_payload

S = 1_000
CIRCUIT = "circuit_open:integrity"


def feed(ev: Evaluator, *, rx: int, sent: int | None = None, **kw: Any) -> None:
    try:
        ev.accept(parse_beat(body(payload(sent_at_ms=rx if sent is None else sent, **kw))), rx, rx_ms=rx)
    except BeatError:
        pass                                  # refused beats still count for W19


Plan = list[tuple[int, dict[str, Any]]]


def interleaved(a: dict[str, Any], b: dict[str, Any], *, until_s: int, offset_s: int = 8,
                b_first: bool = False) -> Plan:
    """Source `a` beats every 15 s from 0 s, source `b` every 15 s from `offset_s` (or the other way round)."""
    first, second = (b, a) if b_first else (a, b)
    out = [(k * 15 * S, first) for k in range(until_s // 15)]
    out += [((k * 15 + offset_s) * S, second) for k in range(until_s // 15)]
    return sorted(out, key=lambda x: x[0])


def flips(plan: Plan, key: str, *, until_s: int) -> list[tuple[int, bool]]:
    """(second, now active) at each change of `key`, on a 1 s receiver tick."""
    ev = Evaluator(started_ms=T0)
    i, active, out = 0, False, []
    for sec in range(until_s):
        t = T0 + sec * S
        while i < len(plan) and T0 + plan[i][0] <= t:
            feed(ev, rx=T0 + plan[i][0], **plan[i][1])
            i += 1
        now = key in ev.active(t, rx_ms=t)
        if now != active:
            out.append((sec, now))
            active = now
    return out


MAIN = {"node": "pc1", "epoch": "5"}
SPARE_DEGRADED = {"node": "pc1", "epoch": "3", "degraded": ["integrity"]}


def test_an_instant_condition_of_the_second_engine_pages_once() -> None:
    """The finding's reproduction (a restore drill on the spare, same node name): one activation, not 40."""
    changes = flips(interleaved(MAIN, SPARE_DEGRADED, until_s=600), CIRCUIT, until_s=600)
    assert changes == [(8, True)]


def test_an_instant_condition_of_the_first_engine_pages_once() -> None:
    """The degraded engine beats first: the spare's first beat does not resolve it (its last beat still counts)."""
    changes = flips(interleaved(MAIN, SPARE_DEGRADED, until_s=600, b_first=True), CIRCUIT, until_s=600)
    assert changes == [(0, True)]


def test_it_resolves_once_after_the_engine_showing_it_stops() -> None:
    plan = [(t, src) for t, src in interleaved(MAIN, SPARE_DEGRADED, until_s=600) if src is MAIN or t <= 300 * S]
    changes = flips(plan, CIRCUIT, until_s=600)
    assert [c[1] for c in changes] == [True, False]
    assert 300 < changes[1][0] <= 300 + SOURCE_LIVE_MS // S + 1


def test_a_duration_condition_of_one_engine_pages_and_stays() -> None:
    """The variant: the main engine reports SM8 REBUILDING, the spare READY. Before, the spare's beats reset the
    duration every 15 s and `sm8_not_ready` never paged."""
    main = {**MAIN, "sm8_state": "REBUILDING"}
    spare = {"node": "pc1", "epoch": "3"}
    changes = flips(interleaved(main, spare, until_s=600), "sm8_not_ready", until_s=600)
    assert changes == [(60, True)]


def test_conditions_of_another_source_name_it() -> None:
    ev = Evaluator(started_ms=T0)
    for t, src in interleaved(MAIN, SPARE_DEGRADED, until_s=60):
        if t <= 45 * S:
            feed(ev, rx=T0 + t, **src)
    active = ev.active(T0 + 46 * S, rx_ms=T0 + 46 * S)     # the newest beat is the main box's (45 s)
    assert active[CIRCUIT].detail.endswith("(from pc1 epoch 3)")


def test_an_engine_whose_clock_runs_behind_still_shows_its_conditions() -> None:
    """Its beats are refused as not newer (sent 9 s behind, received 8 s after the main box's), but they still
    update its own view."""
    plan = interleaved(MAIN, SPARE_DEGRADED, until_s=300)
    ev = Evaluator(started_ms=T0)
    i, seen = 0, []
    for sec in range(300):
        t = T0 + sec * S
        while i < len(plan) and T0 + plan[i][0] <= t:
            rx, src = T0 + plan[i][0], plan[i][1]
            feed(ev, rx=rx, sent=rx - 9 * S if src is SPARE_DEGRADED else rx, **src)
            i += 1
        seen.append(CIRCUIT in ev.active(t, rx_ms=t))
    assert ev.rejected > 0
    assert seen[8:] == [True] * (300 - 8)


def test_two_engines_sharing_one_source_stop_flipping_once_detected() -> None:
    """Same node and epoch (fresh etcd volumes from one compose file): the rate rule detects them; from then on a
    condition only one of them shows stays active (the previous beat of the source counts while `two_engines`
    is active or held)."""
    a = {"node": "pc1", "epoch": "3"}
    b = {"node": "pc1", "epoch": "3", "degraded": ["integrity"]}
    plan = interleaved(a, b, until_s=600)
    ev = Evaluator(started_ms=T0)
    i, detected, changes, active = 0, None, [], False
    for sec in range(600):
        t = T0 + sec * S
        while i < len(plan) and T0 + plan[i][0] <= t:
            feed(ev, rx=T0 + plan[i][0], **plan[i][1])
            i += 1
        now = ev.active(t, rx_ms=t)
        if detected is None and "two_engines" in now:
            detected = sec
        if (CIRCUIT in now) != active:
            changes.append(sec)
            active = CIRCUIT in now
    assert detected is not None and detected <= 90
    assert active and all(c <= detected + 15 for c in changes), (detected, changes)


def test_a_clean_failover_starts_no_new_page_from_the_stopped_engine() -> None:
    """The old leader showed SM8 not READY for 45 s (under the 60 s bound) when the new one took over: its duration
    stops at its last beat, so it never pages, and the new leader's clean beats are not disturbed."""
    ev = Evaluator(started_ms=T0)
    for k in range(4):                          # 0, 15, 30, 45 s
        feed(ev, rx=T0 + k * 15 * S, node="pc1", epoch="7", sm8_state="REBUILDING")
    for k in range(6):                          # the new term from 60 s
        feed(ev, rx=T0 + (60 + k * 15) * S, node="pc1", epoch="8")
    for sec in range(45, 150):
        assert "sm8_not_ready" not in ev.active(T0 + sec * S, rx_ms=T0 + sec * S), sec


async def test_service_pages_the_spare_circuit_once_while_both_beat() -> None:
    """The paging path of the finding: 1 critical page and no resolved notice in 10 minutes (it was 40 and 39)."""
    clock = Clock()
    t = FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([t], clock), clock_ms=clock)
    start = clock.ms
    plan = interleaved({"node": "pc1", "epoch": "5"}, {"node": "pc1", "epoch": "3", "degraded": ["integrity"]},
                       until_s=600)
    i = 0
    for sec in range(600):
        clock.ms = start + sec * S
        while i < len(plan) and plan[i][0] <= sec * S:
            rx, src = plan[i]
            svc.on_beat(svc_payload(start + rx, **src).to_json().encode())
            i += 1
        await svc.tick()
    circuit = [m for m in t.sent if "task circuit open (integrity)" in m.subject]
    assert [m.kind for m in circuit if "still active" not in m.subject] == ["episode"]
    assert circuit[0].severity == "critical"
    assert not [m for m in circuit if "resolved" in m.subject]
