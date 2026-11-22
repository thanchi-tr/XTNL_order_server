"""Review round 5: a second sender whose beats are malformed or skewed does not make `payload_invalid` flap.

The finding: `payload_invalid` was one shared flag. A malformed POST set it and the next accepted beat from any
engine cleared it. A second, older engine whose payload lacks a field (an old desktop left with its scheduled task,
on an older checkout) switched it on at each of its POSTs and off at the other engine's next beat: a critical page
plus a resolved notice about every 15 s. Its beats never parse, so `two_engines` never said why. Now the condition
stays active until an accepted beat has come after the last bad POST and none has come for SOURCE_LIVE_MS, and its
text says that valid beats are also arriving.
"""
from __future__ import annotations

import json
from typing import Any

from src.deadman_receiver.evaluate import SOURCE_LIVE_MS, BeatError, Evaluator, parse_beat
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService
from src.deadman_receiver.settings import ReceiverSettings
from tests.deadman_receiver.test_evaluate import T0, body, payload
from tests.deadman_receiver.test_service import ENV, Clock, FakeTransport
from tests.deadman_receiver.test_service import payload as svc_payload

S = 1_000
KEY = "payload_invalid"


def old_body(sent: int) -> bytes:
    """The older checkout's beat: `calendar_version` missing, so it does not parse."""
    d = json.loads(body(payload(node="old-desktop", epoch="2", sent_at_ms=sent)))
    d.pop("calendar_version")
    return json.dumps(d).encode()


def post(ev: Evaluator, raw: bytes, rx: int) -> None:
    """What ReceiverService.on_beat does with one body."""
    try:
        beat = parse_beat(raw)
    except BeatError as exc:
        ev.reject(str(exc), rx, rx_ms=rx)
        return
    try:
        ev.accept(beat, rx, rx_ms=rx)
    except BeatError:
        pass


Plan = list[tuple[int, bytes]]


def two_senders(bad: Any, *, until_s: int, offset_s: int = 7) -> Plan:
    """pc1 epoch 5 sends valid beats every 15 s from 0 s; the second sender every 15 s from `offset_s`."""
    good = [(k * 15 * S, body(payload(node="pc1", epoch="5", sent_at_ms=T0 + k * 15 * S)))
            for k in range(until_s // 15)]
    second = [((k * 15 + offset_s) * S, bad(T0 + (k * 15 + offset_s) * S)) for k in range(until_s // 15)]
    return sorted(good + second, key=lambda x: x[0])


def run(plan: Plan, *, until_s: int) -> tuple[list[tuple[int, bool]], Evaluator, list[str]]:
    """(second, now active) at each change of payload_invalid on a 1 s tick, the evaluator, the details seen."""
    ev = Evaluator(started_ms=T0)
    i, active, out, details = 0, False, [], []
    for sec in range(until_s):
        t = T0 + sec * S
        while i < len(plan) and T0 + plan[i][0] <= t:
            post(ev, plan[i][1], T0 + plan[i][0])
            i += 1
        conds = ev.active(t, rx_ms=t)
        if KEY in conds:
            details.append(conds[KEY].detail)
        if (KEY in conds) != active:
            out.append((sec, KEY in conds))
            active = KEY in conds
    return out, ev, details


def test_an_older_second_engine_pages_once_not_every_15_s() -> None:
    """The finding's reproduction: one activation in 4 minutes (it was 31 transitions)."""
    changes, _, _ = run(two_senders(old_body, until_s=240), until_s=240)
    assert changes == [(7, True)]


def test_it_resolves_source_live_ms_after_the_second_sender_stops() -> None:
    plan =two_senders(old_body, until_s=240) + [((240 + k * 15) * S, body(payload(
        node="pc1", epoch="5", sent_at_ms=T0 + (240 + k * 15) * S))) for k in range(6)]
    changes, _, _ = run(sorted(plan, key=lambda x: x[0]), until_s=330)
    last_bad_s = 225 + 7
    assert changes == [(7, True), (last_bad_s + SOURCE_LIVE_MS // S + 1, False)]


def test_the_text_says_valid_beats_are_also_arriving() -> None:
    """After a valid beat came between two malformed POSTs, the page says to look for a second sender."""
    _, _, details = run(two_senders(old_body, until_s=60), until_s=60)
    assert "calendar_version" in details[0] and "also arriving" not in details[0]   # first bad POST, 7 s
    assert "valid beats are also arriving (latest from pc1 epoch 5)" in details[-1]
    assert "second sender" in details[-1]


def test_a_skewed_second_engine_pages_once() -> None:
    """The skew path flipped the same flag: a second engine 10 min off the receiver clock."""
    def skewed(sent: int) -> bytes:
        return body(payload(node="old-desktop", epoch="2", sent_at_ms=sent - 600_000))
    changes, _, details = run(two_senders(skewed, until_s=240), until_s=240)
    assert changes == [(7, True)]
    assert "off the receiver clock (from old-desktop epoch 2)" in details[-1]


def test_malformed_beats_alone_stay_active() -> None:
    """With no accepted beat since, the condition stays active as before (no SOURCE_LIVE_MS expiry)."""
    ev = Evaluator(started_ms=T0)
    post(ev, old_body(T0), T0)
    assert KEY in ev.active(T0 + 10 * SOURCE_LIVE_MS, rx_ms=T0 + 10 * SOURCE_LIVE_MS)
    assert "also arriving" not in ev.active(T0 + S, rx_ms=T0 + S)[KEY].detail


def test_one_bad_post_then_good_beats_resolves_after_source_live_ms() -> None:
    ev = Evaluator(started_ms=T0)
    post(ev, body(payload(sent_at_ms=T0)), T0)
    post(ev, old_body(T0 + 15 * S), T0 + 15 * S)
    for k in range(2, 6):
        post(ev, body(payload(sent_at_ms=T0 + k * 15 * S)), T0 + k * 15 * S)
    bad = T0 + 15 * S
    assert KEY in ev.active(bad + SOURCE_LIVE_MS, rx_ms=bad + SOURCE_LIVE_MS)
    assert KEY not in ev.active(bad + SOURCE_LIVE_MS + 1, rx_ms=bad + SOURCE_LIVE_MS + 1)


async def test_service_pages_the_malformed_heartbeat_once_while_both_send() -> None:
    """The paging path: 1 critical page and no resolved notice in 4 minutes (it was ~16 of each)."""
    clock = Clock()
    t = FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([t], clock), clock_ms=clock)
    start = clock.ms

    def old(sent: int) -> bytes:
        d = json.loads(svc_payload(sent, node="old-desktop", epoch="2").to_json())
        d.pop("calendar_version")
        return json.dumps(d).encode()

    plan = sorted([(k * 15 * S, svc_payload(start + k * 15 * S, node="pc1", epoch="5").to_json().encode())
                   for k in range(16)] + [((k * 15 + 7) * S, old(start + (k * 15 + 7) * S)) for k in range(16)],
                  key=lambda x: x[0])
    i = 0
    for sec in range(240):
        clock.ms = start + sec * S
        while i < len(plan) and plan[i][0] <= sec * S:
            svc.on_beat(plan[i][1])
            i += 1
        await svc.tick()
    bad = [m for m in t.sent if "malformed heartbeat" in m.subject]
    assert [m.kind for m in bad if "still active" not in m.subject] == ["episode"]
    assert bad[0].severity == "critical"
    assert not [m for m in bad if "resolved" in m.subject]
