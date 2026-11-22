"""W19 review round 1: two engines that share one source (node, epoch) are caught by their beat rate.

The engine's epoch label is only the lease revision, so two home PCs from the same env file (XTNL_NODE_ID=pc1), each
on a fresh etcd volume, can beat as ('pc1', '5'). One engine beats every 15 s or slower; two interleaved average
about 7.5 s, so the rate rule pages `two_engines` while a single engine (with delivery jitter) never trips it.
"""
from __future__ import annotations

import random

from src.deadman_receiver.evaluate import RATE_MIN_BEATS, Evaluator, Thresholds
from tests.deadman_receiver.test_evaluate import T0
from tests.deadman_receiver.test_two_engines_w19 import S, every, run, send


def test_two_engines_under_one_source_page_two_engines() -> None:
    ev = Evaluator(started_ms=T0)
    seen = run(ev, every("pc1", 0, 300, epoch="5") + every("pc1", 7, 307, epoch="5"))
    assert ev.tracked_sources == 1
    assert any("two_engines" in keys for keys in seen)
    cond = ev.active(T0 + 307 * S)["two_engines"]
    assert cond.severity == "critical" and "pc1 epoch 5" in cond.detail and "mean interval 7 s" in cond.detail


def test_two_engines_one_second_apart_under_one_source_page_too() -> None:
    ev = Evaluator(started_ms=T0)
    run(ev, every("pc1", 0, 120, epoch="5") + every("pc1", 1, 121, epoch="5"))
    assert "two_engines" in ev.active(T0 + 121 * S)


def test_it_clears_about_one_window_after_one_of_them_stops() -> None:
    ev = Evaluator(started_ms=T0)
    run(ev, every("pc1", 0, 120, epoch="5") + every("pc1", 7, 127, epoch="5") + every("pc1", 135, 400, epoch="5"))
    assert "two_engines" not in ev.active(T0 + 400 * S)


def test_one_engine_with_delivery_jitter_never_trips_the_rate_rule() -> None:
    """Each beat waits 15 s after its POST, which may take up to the 5 s timeout; receive times jitter by that."""
    rng = random.Random(7)
    ev = Evaluator(started_ms=T0)
    sent = 0.0
    for _ in range(200):
        delay = rng.uniform(0, 5)
        rx = int((sent + delay) * S)
        send(ev, "pc1", T0 + rx, epoch="5")
        assert "two_engines" not in ev.active(T0 + rx)
        sent += delay + 15            # the next beat is built 15 s after this POST returned


def test_the_rate_rule_needs_enough_beats_and_works_at_the_smallest_window() -> None:
    ev = Evaluator(started_ms=T0, thresholds=Thresholds(two_engines_window_ms=45_000))
    times = [0, 7, 15, 22, 30, 37]
    assert len(times) == RATE_MIN_BEATS
    for t in times[:-1]:
        send(ev, "pc1", T0 + t * S, epoch="5")
    assert "two_engines" not in ev.active(T0 + 30 * S)        # five beats: not yet
    send(ev, "pc1", T0 + 37 * S, epoch="5")
    assert "two_engines" in ev.active(T0 + 37 * S)
