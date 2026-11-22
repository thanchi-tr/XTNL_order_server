"""Review round 8: the skew gate also accepts a beat that agrees with the engine clock carried on from the latest
accepted beat (its `sent_at_ms` plus the receiver monotonic time since), so a receiver wall clock step of more than
MAX_CLOCK_SKEW_MS no longer refuses every beat (payload_invalid, then silence). A sender whose own clock is off is
off both references and still refused. The matrix (test_condition_matrix.py, I8) covers the paging; these pin the
edges of the gate itself.
"""
from __future__ import annotations

import pytest

from src.deadman_receiver.evaluate import BeatError, Evaluator, parse_beat
from tests.deadman_receiver.test_evaluate import T0
from tests.deadman_receiver.test_service import payload

S = 1000
STEP = 600 * S                  # the receiver's wall clock steps 10 min; its monotonic clock does not


def beat(sent_ms: int, **kw: object) -> bytes:
    return payload(sent_ms, node="pc1", epoch="5", **kw).to_json().encode()


def test_beats_after_a_receiver_wall_step_stay_accepted() -> None:
    ev = Evaluator(started_ms=T0)
    ev.accept(parse_beat(beat(T0)), T0, rx_ms=T0)
    for k in range(1, 20):
        ev.accept(parse_beat(beat(T0 + k * 15 * S)), T0 + k * 15 * S + STEP, rx_ms=T0 + k * 15 * S)
    assert ev.beats == 20 and ev.invalid is None
    assert ev.active(T0 + 290 * S + STEP, rx_ms=T0 + 290 * S) == {}


def test_a_skewed_sender_is_still_refused_after_a_receiver_wall_step() -> None:
    ev = Evaluator(started_ms=T0)
    ev.accept(parse_beat(beat(T0)), T0, rx_ms=T0)
    with pytest.raises(BeatError, match="too far"):     # 400 s behind the engine clock carried on
        ev.accept(parse_beat(payload(T0 + 15 * S - 400 * S, node="pc2", epoch="1").to_json().encode()),
                  T0 + 15 * S + STEP, rx_ms=T0 + 15 * S)
    assert "payload_invalid" in ev.active(T0 + 16 * S + STEP, rx_ms=T0 + 16 * S)


def test_a_first_beat_off_the_receiver_wall_clock_is_refused() -> None:
    """With no accepted beat there is no engine clock to carry on: a first beat off the receiver's wall clock (and
    off its start wall time carried on by the monotonic clock) is refused."""
    ev = Evaluator(started_ms=T0)
    with pytest.raises(BeatError, match="too far"):
        ev.accept(parse_beat(beat(T0 - STEP)), T0, rx_ms=T0)
    assert ev.beats == 0 and ev.invalid is not None


def test_a_wall_step_before_the_first_beat_refuses_nothing() -> None:
    """Round 10 (I8): the receiver's wall clock steps 10 min between its start and the engine's first beat. The
    start wall time carried on by the monotonic clock still agrees with the engine, so the beat is accepted. (Before,
    the stepped wall clock alone decided and every beat was refused for good: payload_invalid, then silence.)"""
    for step in (STEP, -STEP):
        ev = Evaluator(started_ms=T0)
        ev.accept(parse_beat(beat(T0 + 12 * S)), T0 + 12 * S + step, rx_ms=T0 + 12 * S)
        assert ev.beats == 1 and ev.invalid is None
    ev = Evaluator(started_ms=T0)                         # a wrong start clock corrected later also works
    ev.accept(parse_beat(beat(T0 + STEP + 12 * S)), T0 + STEP + 12 * S, rx_ms=T0 + 12 * S)
    assert ev.beats == 1


def test_a_far_future_beat_cannot_lock_out_later_beats() -> None:
    """A stray far-future `sent_at_ms` is off both references, so it never becomes the "newest" beat."""
    ev = Evaluator(started_ms=T0)
    ev.accept(parse_beat(beat(T0)), T0, rx_ms=T0)
    with pytest.raises(BeatError, match="too far"):
        ev.accept(parse_beat(beat(T0 + 86_400 * S)), T0 + 15 * S, rx_ms=T0 + 15 * S)
    ev.accept(parse_beat(beat(T0 + 30 * S)), T0 + 30 * S, rx_ms=T0 + 30 * S)
    assert ev.beats == 2
