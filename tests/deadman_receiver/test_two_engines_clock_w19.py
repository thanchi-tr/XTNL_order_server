"""W19 review round 2: one engine never looks like two because of the receiver's clock.

The two-engine rules time beats on the receiver's monotonic clock (`rx_ms`), so a wall clock step back cannot put
one engine's beats inside the window twice. The rate rule also needs the beats' own `sent_at_ms` to be fast, so a
stalled receiver that handles several queued beats of one engine in the same millisecond does not page either.
Two engines that really share a source still page.
"""
from __future__ import annotations

from src.deadman_receiver.evaluate import BeatError, Evaluator, Thresholds, _hms, parse_beat
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService, monotonic_ms, wall_ms
from src.deadman_receiver.settings import ReceiverSettings
from tests.deadman_receiver.test_evaluate import T0, body, payload
from tests.deadman_receiver.test_service import ENV, Clock, FakeTransport
from tests.deadman_receiver.test_service import payload as svc_payload

S = 1_000


def beat(ev: Evaluator, *, wall: int, rx: int, sent: int, epoch: str = "42") -> None:
    try:
        ev.accept(parse_beat(body(payload(node="pc1", epoch=epoch, sent_at_ms=sent))), wall, rx_ms=rx)
    except BeatError:
        pass                                  # refused beats still count for W19


def test_a_30_s_wall_clock_step_back_is_not_two_engines() -> None:
    """The review's reproduction: beats at wall 100..160 s, then the clock steps back 30 s (145, 160, 175 s)."""
    ev = Evaluator(started_ms=T0)
    walls = [100, 115, 130, 145, 160, 145, 160, 175, 190, 205]
    for i, w in enumerate(walls):
        rx = T0 + (100 + 15 * i) * S          # monotonic: every 15 s, whatever the wall clock did
        sent = T0 + (100 + 15 * i) * S        # the engine's own clock is not stepped
        beat(ev, wall=T0 + w * S, rx=rx, sent=sent)
        active = ev.active(T0 + w * S, rx_ms=rx)
        assert "two_engines" not in active, active.get("two_engines")


def test_without_a_monotonic_clock_the_step_would_have_paged() -> None:
    """Guard on the test itself: the same wall times as receive times are what the old rule saw."""
    ev = Evaluator(started_ms=T0)
    walls = [100, 115, 130, 145, 160, 145, 160, 175]
    for w in walls:
        beat(ev, wall=T0 + w * S, rx=T0 + w * S, sent=T0 + w * S)          # sent fast too: old behaviour
    assert "two_engines" in ev.active(T0 + 175 * S)


def test_four_beats_handled_in_the_same_millisecond_are_not_two_engines() -> None:
    """The receiver stalls: the engine gave up after 5 s on each POST and beat again 15 s later (20 s apart), and
    the four requests are then handled at once. Normal beats follow."""
    for window_ms in (45_000, 60_000):
        ev = Evaluator(started_ms=T0, thresholds=Thresholds(two_engines_window_ms=window_ms))
        rx_bunch = T0 + 200 * S
        for sent_s in (140, 160, 180, 200):                                  # one engine's queued beats
            beat(ev, wall=rx_bunch, rx=rx_bunch, sent=T0 + sent_s * S)
            assert "two_engines" not in ev.active(rx_bunch, rx_ms=rx_bunch)
        for k in range(1, 8):                                                # then every 15 s again
            t = rx_bunch + 15 * k * S
            beat(ev, wall=t, rx=t, sent=T0 + (200 + 15 * k) * S)
            assert "two_engines" not in ev.active(t, rx_ms=t), (window_ms, k)


def test_two_engines_under_one_source_still_page_on_the_monotonic_clock() -> None:
    ev = Evaluator(started_ms=T0)
    events = sorted([(t, t) for t in range(0, 121, 15)] + [(t, t) for t in range(7, 128, 15)])
    for rx_s, sent_s in events:
        beat(ev, wall=T0 + rx_s * S - 30 * S, rx=T0 + rx_s * S, sent=T0 + sent_s * S)   # wall off by 30 s: moot
    cond = ev.active(T0 + 97 * S, rx_ms=T0 + 127 * S)["two_engines"]
    assert "mean interval 7 s" in cond.detail


def test_texts_show_wall_times_not_monotonic_ones() -> None:
    ev = Evaluator(started_ms=T0)
    for rx_s in sorted(list(range(0, 121, 15)) + list(range(7, 128, 15))):
        beat(ev, wall=T0 + rx_s * S, rx=5_000 + rx_s * S, sent=T0 + rx_s * S)   # monotonic near 0
    cond = ev.active(T0 + 127 * S, rx_ms=5_000 + 127 * S)["two_engines"]
    assert f"last {_hms(T0 + 127 * S)}" in cond.detail and _hms(5_000 + 127 * S) not in cond.detail


async def test_the_service_times_beats_on_its_monotonic_clock() -> None:
    wall = Clock()
    mono = [10_000_000]
    t = FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([t], wall), clock_ms=wall, mono_ms=lambda: mono[0])
    start = wall.ms
    sent = start
    for i in range(12):
        if i == 6:
            wall.ms -= 30 * S                 # NTP steps the receiver's wall clock back 30 s
        svc.on_beat(svc_payload(sent, node="pc1").to_json().encode())
        await svc.tick()
        assert "two_engines" not in svc.active
        wall.ms += 15 * S
        mono[0] += 15 * S
        sent += 15 * S
    assert not [m for m in t.sent if "two engines" in m.subject]


def test_production_defaults_use_a_monotonic_clock() -> None:
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([FakeTransport()], wall_ms))
    assert svc._mono is monotonic_ms
    fake = Clock()
    assert ReceiverService(ReceiverSettings.from_env(ENV), Pager([FakeTransport()], fake), clock_ms=fake)._mono is fake
