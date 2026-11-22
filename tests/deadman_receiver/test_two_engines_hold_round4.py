"""W19 review round 4: `two_engines` pages once per incident, and a stalled receiver's handover is not two engines.

Finding 1: the engine's real cadence is a little over 15 s (the next beat waits the full interval after the POST, and
the POST plus its PostgreSQL reads take seconds; about 22 s while PostgreSQL does not answer). With few beats per
source in the window, the interleave test dropped out between beats and came back at the next one, and every flip
was a resolved notice plus a new critical page. The condition now holds for one window after its last detection,
and the interleave test looks back over at least 90 s, so it still sees the slow cadence at the 45 s window.

Finding 2: beats handled in the same millisecond by a stalled receiver were "inside" each other's span on receive
time. The interleave must now also hold on the beats' own `sent_at_ms`.
"""
from __future__ import annotations

import pytest

from src.deadman_receiver.evaluate import BeatError, Evaluator, Thresholds, parse_beat
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService
from src.deadman_receiver.settings import ReceiverSettings
from tests.deadman_receiver.test_evaluate import T0, body, payload
from tests.deadman_receiver.test_service import ENV, Clock, FakeTransport
from tests.deadman_receiver.test_service import payload as svc_payload

S = 1_000


def feed(ev: Evaluator, *, node: str, epoch: str, rx: int, sent: int) -> None:
    try:
        ev.accept(parse_beat(body(payload(node=node, epoch=epoch, sent_at_ms=sent))), rx, rx_ms=rx)
    except BeatError:
        pass                                  # refused beats still count for W19


def two_engines(period_s: float, *, until_s: int, stop_b_s: int | None = None) -> list[tuple[int, str, str]]:
    """pc1 epoch 5 and pc2 epoch 9 beating every `period_s`, pc2 offset by 7 s (the review's simulation)."""
    out: list[tuple[int, str, str]] = []
    k = 0
    while k * period_s < until_s:
        out.append((int(k * period_s * S), "pc1", "5"))
        b = int((k * period_s + 7) * S)
        if stop_b_s is None or b <= stop_b_s * S:
            out.append((b, "pc2", "9"))
        k += 1
    return sorted(out)


def transitions(window_s: int, period_s: float, *, until_s: int = 600,
                stop_b_s: int | None = None) -> tuple[int, int, list[int]]:
    """(activations, resolutions, times of each change in s) of `two_engines` on a 1 s receiver tick."""
    ev = Evaluator(started_ms=T0, thresholds=Thresholds(two_engines_window_ms=window_s * S))
    beats = two_engines(period_s, until_s=until_s, stop_b_s=stop_b_s)
    i, active, ups, downs, changes = 0, False, 0, 0, []
    for t in range(0, (until_s + 300) * S, S):
        while i < len(beats) and beats[i][0] <= t:
            rx, node, epoch = beats[i]
            feed(ev, node=node, epoch=epoch, rx=T0 + rx, sent=T0 + rx)
            i += 1
        now = "two_engines" in ev.active(T0 + t, rx_ms=T0 + t)
        if now != active:
            ups, downs = ups + now, downs + (not now)
            changes.append(t // S)
            active = now
    return ups, downs, changes


@pytest.mark.parametrize("window_s", [45, 60])
@pytest.mark.parametrize("period_s", [15.0, 15.3, 15.8, 22.0])
def test_two_engines_page_once_while_both_run(window_s: int, period_s: float) -> None:
    """The review counted 121-150 flips in 10 minutes at 15.3 s / 45 s and 22 s / 60 s: now one activation."""
    ups, downs, changes = transitions(window_s, period_s, until_s=600, stop_b_s=None)
    assert ups == 1, (ups, changes)
    assert changes[0] <= 90                   # seen within the 90 s look-back
    # it resolves once, only after both stopped beating (until_s), never while they run
    assert downs <= 1 and all(c > 600 or c == changes[0] for c in changes)


@pytest.mark.parametrize("window_s", [45, 60])
@pytest.mark.parametrize("period_s", [15.3, 22.0])
def test_it_resolves_once_after_the_second_engine_stops(window_s: int, period_s: float) -> None:
    ups, downs, changes = transitions(window_s, period_s, until_s=600, stop_b_s=300)
    assert (ups, downs) == (1, 1), changes
    # cleared within the look-back (90 s) + the one-window hold + one beat of pc2's last beat
    assert 300 < changes[1] <= 300 + 90 + window_s + int(period_s) + 1, changes


async def test_service_sends_one_critical_and_no_resolved_while_both_beat() -> None:
    """The paging path: at 15.3 s / 45 s the old rule sent a critical page and a resolved notice about every 4 s."""
    clock = Clock()
    t = FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env({**ENV, "XTNL_RECEIVER_TWO_ENGINES_WINDOW_S": "45"}),
                          Pager([t], clock), clock_ms=clock)
    start = clock.ms
    beats = two_engines(15.3, until_s=600)
    i = 0
    for sec in range(0, 600):
        clock.ms = start + sec * S
        while i < len(beats) and beats[i][0] <= sec * S:
            rx, node, epoch = beats[i]
            svc.on_beat(svc_payload(start + rx, node=node, epoch=epoch).to_json().encode())
            i += 1
        await svc.tick()
    two = [m for m in t.sent if "two engines beating" in m.subject]
    episodes = [m for m in two if m.kind == "episode"]
    resolved = [m for m in two if "resolved" in m.subject]
    assert len(episodes) == 1 and episodes[0].severity == "critical"
    assert resolved == []


# ------------------------------------------------------------------ finding 2: stalled receiver, clean handover
def test_a_clean_restart_behind_a_stalled_receiver_is_not_two_engines() -> None:
    """The review's reproduction: pc1 epoch 7 beats at rx 0 and 15 s; its last two beats (sent +30 s, +45 s) and
    the new term's first two (pc1 epoch 8, sent +70 s, +85 s) are all handled at rx 90 s."""
    ev = Evaluator(started_ms=T0)
    feed(ev, node="pc1", epoch="7", rx=T0, sent=T0)
    feed(ev, node="pc1", epoch="7", rx=T0 + 15 * S, sent=T0 + 15 * S)
    bunch = T0 + 90 * S
    for epoch, sent_s in (("7", 30), ("7", 45), ("8", 70), ("8", 85)):
        feed(ev, node="pc1", epoch=epoch, rx=bunch, sent=T0 + sent_s * S)
        assert "two_engines" not in ev.active(bunch, rx_ms=bunch)
    for k in range(1, 10):                    # the new term then beats normally
        t = bunch + 15 * k * S
        feed(ev, node="pc1", epoch="8", rx=t, sent=T0 + (85 + 15 * k) * S)
        assert "two_engines" not in ev.active(t, rx_ms=t), k


def test_two_real_engines_still_page_when_their_beats_bunch() -> None:
    """Two engines behind the same stalled receiver: their beats interleave on sent time too, so it still pages."""
    ev = Evaluator(started_ms=T0)
    bunch = T0 + 90 * S
    for node, sent_s in sorted([("pc1", s) for s in (0, 15, 30, 45, 60, 75)] + [("pc2", s) for s in (7, 22, 37, 52)],
                               key=lambda x: x[1]):
        feed(ev, node=node, epoch="5" if node == "pc1" else "9", rx=bunch, sent=T0 + sent_s * S)
    assert "two_engines" in ev.active(bunch, rx_ms=bunch)


def test_two_engines_with_clocks_a_few_seconds_apart_still_page() -> None:
    """Both engines lead only after their clock hard check; a 3 s difference keeps the interleave on sent time."""
    ev = Evaluator(started_ms=T0)
    beats = sorted([(t, "pc1", "5", t) for t in range(0, 121, 15)] + [(t, "pc2", "9", t + 3) for t in range(7, 122, 15)])
    for rx_s, node, epoch, sent_s in beats:
        feed(ev, node=node, epoch=epoch, rx=T0 + rx_s * S, sent=T0 + sent_s * S)
    assert "two_engines" in ev.active(T0 + 122 * S, rx_ms=T0 + 122 * S)
