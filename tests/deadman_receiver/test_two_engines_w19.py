"""W19 (home-server-plan §4.12 / §6): the receiver pages when two engines beat at once, and notices a node change.

Beats are tracked per source (node, epoch) from every authenticated, parsed beat, including ones refused as not
newer, so one live engine can no longer hide another. Clean failovers raise only the `node_changed` notice.
"""
from __future__ import annotations

from typing import Any

import pytest

from src.deadman_receiver.evaluate import (
    BEATS_PER_SOURCE,
    MAX_SOURCES,
    BeatError,
    Evaluator,
    Thresholds,
    parse_beat,
)
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService
from src.deadman_receiver.settings import ReceiverConfigError, ReceiverSettings
from tests.deadman_receiver.test_evaluate import T0, body, payload
from tests.deadman_receiver.test_service import ENV, Clock, FakeTransport
from tests.deadman_receiver.test_service import payload as svc_payload

S = 1_000


def send(ev: Evaluator, node: str, rx_ms: int, *, sent_ms: int | None = None, epoch: str = "7") -> bool:
    """One authenticated beat received at `rx_ms` (engine clock `sent_ms`). True when accepted."""
    beat = parse_beat(body(payload(node=node, epoch=epoch, sent_at_ms=rx_ms if sent_ms is None else sent_ms)))
    try:
        ev.accept(beat, rx_ms)
    except BeatError:
        return False
    return True


def run(ev: Evaluator, schedule: list[tuple[int, str, dict[str, Any]]]) -> list[set[str]]:
    """Feed (rx offset ms, node, kwargs) in receive order; the active keys after each beat."""
    seen: list[set[str]] = []
    for rx, node, kw in sorted(schedule, key=lambda x: x[0]):
        send(ev, node, T0 + rx, **kw)
        seen.append(set(ev.active(T0 + rx)))
    return seen


def every(node: str, start_s: int, end_s: int, **kw: Any) -> list[tuple[int, str, dict[str, Any]]]:
    """A node beating every 15 s from `start_s` up to and including `end_s`."""
    return [(t * S, node, dict(kw)) for t in range(start_s, end_s + 1, 15)]


# ------------------------------------------------------------------------------------------------ evaluator
def test_steady_single_node_raises_nothing() -> None:
    ev = Evaluator(started_ms=T0)
    seen = run(ev, every("node-a", 0, 600))
    assert all(not ({"two_engines", "node_changed"} & keys) for keys in seen)
    assert ev.tracked_sources == 1


def test_clean_failover_raises_the_notice_only() -> None:
    """The old leader stops within ~15 s: here one stray beat of node-a lands 5 s after node-b's first."""
    ev = Evaluator(started_ms=T0)
    seen = run(ev, every("node-a", 0, 105) + every("node-b", 100, 400))
    assert not any("two_engines" in keys for keys in seen)
    active = ev.active(T0 + 400 * S)
    assert set(active) == {"node_changed"}
    notice = active["node_changed"]
    assert notice.severity == "high" and notice.summary == "dead-man: beating node changed (node-a -> node-b)"
    assert "node-b" in notice.detail and "UTC" in notice.detail
    # the notice clears 5 min after the last change (node-b took over for good at 115 s, after the stray beat)
    assert "node_changed" in ev.active(T0 + (115 + 300) * S)
    assert "node_changed" not in ev.active(T0 + (115 + 301) * S)


def test_fail_back_inside_the_window_is_not_two_engines() -> None:
    """A -> B -> A, each running alone: no A beat falls inside B's span, so it is a flap, not two engines."""
    ev = Evaluator(started_ms=T0)
    seen = run(ev, every("node-a", 0, 30) + every("node-b", 40, 70) + every("node-a", 80, 140, epoch="8"))
    assert not any("two_engines" in keys for keys in seen)
    assert "node_changed" in ev.active(T0 + 140 * S)


def test_interleaved_engines_page_critical() -> None:
    ev = Evaluator(started_ms=T0)
    seen = run(ev, every("node-a", 0, 120) + every("node-b", 7, 127))
    first = next(i for i, keys in enumerate(seen) if "two_engines" in keys)
    assert first <= 6                                      # A0 B7 A15 B22 A30 B37: paged within ~40 s
    cond = ev.active(T0 + 127 * S)["two_engines"]
    assert cond.severity == "critical" and cond.summary == "dead-man: two engines beating (node-a, node-b)"
    assert "node-a epoch 7" in cond.detail and "node-b epoch 7" in cond.detail and "within 90 s" in cond.detail
    # the interleave test looks back over max(window, 90 s) (review round 4, OVERLAP_MIN_SPAN_MS)


def test_refused_beats_of_the_second_engine_still_count() -> None:
    """node-b's clock is 20 s behind: every one of its beats is refused as not newer, yet it is seen."""
    ev = Evaluator(started_ms=T0)
    for t in range(0, 121, 15):
        assert send(ev, "node-a", T0 + t * S)
        assert not send(ev, "node-b", T0 + (t + 7) * S, sent_ms=T0 + (t + 7 - 20) * S)
    assert ev.rejected == 9 and ev.beats == 9
    active = ev.active(T0 + 128 * S)
    assert "two_engines" in active and "silence" not in active
    assert "node-b epoch 7 x4" in ev.status_line(T0 + 128 * S)


def test_two_home_pcs_with_the_same_node_name_page_by_epoch() -> None:
    """Two PCs started from one env file share XTNL_NODE_ID=pc1 but each has its own etcd epoch."""
    ev = Evaluator(started_ms=T0)
    run(ev, every("pc1", 0, 90, epoch="7") + every("pc1", 5, 95, epoch="3"))
    active = ev.active(T0 + 95 * S)
    assert "two_engines" in active and "node_changed" not in active
    assert "pc1 epoch 3" in active["two_engines"].detail and "pc1 epoch 7" in active["two_engines"].detail


def test_engine_restart_on_the_same_node_raises_nothing() -> None:
    ev = Evaluator(started_ms=T0)
    seen = run(ev, every("pc1", 0, 60, epoch="7") + every("pc1", 120, 300, epoch="8"))
    assert all(not ({"two_engines", "node_changed"} & keys) for keys in seen)


def test_recovery_when_one_engine_stops() -> None:
    ev = Evaluator(started_ms=T0)
    run(ev, every("node-a", 0, 300) + every("node-b", 7, 127))
    assert "two_engines" not in ev.active(T0 + 300 * S)
    # it was active while both beat, and cleared within the 90 s look-back, plus the one-window hold (review
    # round 4), plus one beat, of node-b's last beat
    ev2 = Evaluator(started_ms=T0)
    run(ev2, every("node-a", 0, 127) + every("node-b", 7, 127))
    assert "two_engines" in ev2.active(T0 + 127 * S)
    cleared = None
    for t in range(127, 300, 15):
        send(ev2, "node-a", T0 + (t + 8) * S)
        if "two_engines" not in ev2.active(T0 + (t + 8) * S):
            cleared = t + 8
            break
    assert cleared is not None and cleared - 127 <= 90 + 60 + 15


def test_a_wider_window_keeps_the_alarm_longer() -> None:
    ev = Evaluator(started_ms=T0, thresholds=Thresholds(two_engines_window_ms=300_000))
    run(ev, every("node-a", 0, 240) + every("node-b", 7, 97))
    assert "two_engines" in ev.active(T0 + 240 * S)          # node-b's beats are still inside 300 s
    ev60 = Evaluator(started_ms=T0)
    run(ev60, every("node-a", 0, 240) + every("node-b", 7, 97))
    assert "two_engines" not in ev60.active(T0 + 240 * S)


def test_tracked_sources_are_bounded() -> None:
    ev = Evaluator(started_ms=T0)
    for i in range(40):
        send(ev, f"node-{i:02d}", T0 + i * 100)
    assert ev.tracked_sources == MAX_SOURCES
    assert all(len(times) <= BEATS_PER_SOURCE for times in ev._sources.values())
    # the bound drops the least recently heard sources, so two real engines are still seen after a flood
    run(ev, [(t, n, {}) for t, n, _ in every("node-x", 10, 100) + every("node-y", 17, 107)])
    assert ev.tracked_sources <= MAX_SOURCES
    assert "node-x, node-y" in ev.active(T0 + 107 * S)["two_engines"].summary
    # and a source is forgotten once all its beats have left the window
    assert ev.tracked_sources == 2


def test_text_names_nodes_and_times_only() -> None:
    ev = Evaluator(started_ms=T0)
    run(ev, every("node-a", 0, 90) + every("node-b", 7, 97))
    for cond in (ev.active(T0 + 97 * S)[k] for k in ("two_engines", "node_changed")):
        text = cond.summary + cond.detail
        assert "OPEN" not in text and "STREAMING" not in text and "$" not in text


# ------------------------------------------------------------------------------------------------ settings
def test_window_setting_default_bounds_and_wiring() -> None:
    assert ReceiverSettings.from_env(ENV).two_engines_window_s == 60
    s = ReceiverSettings.from_env({**ENV, "XTNL_RECEIVER_TWO_ENGINES_WINDOW_S": "300"})
    assert s.two_engines_window_s == 300
    svc = ReceiverService(s, Pager([FakeTransport()], Clock()), clock_ms=Clock())
    assert svc.evaluator.thresholds.two_engines_window_ms == 300_000
    for bad in ("44", "601", "soon"):
        with pytest.raises(ReceiverConfigError, match="XTNL_RECEIVER_TWO_ENGINES_WINDOW_S") as info:
            ReceiverSettings.from_env({**ENV, "XTNL_RECEIVER_TWO_ENGINES_WINDOW_S": bad})
        assert bad not in str(info.value)


# ------------------------------------------------------------------------------------------------ paging
async def test_service_pages_notice_high_and_two_engines_critical_then_resolves() -> None:
    clock = Clock()
    t = FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env({**ENV, "XTNL_RECEIVER_REPAGE_S": "60"}), Pager([t], clock),
                          clock_ms=clock)
    start = clock.ms
    beats = sorted([(s, "node-a") for s in range(0, 121, 15)] + [(s, "node-b") for s in range(7, 122, 15)])
    for at, node in beats:
        clock.ms = start + at * S
        svc.on_beat(svc_payload(clock.ms, node=node).to_json().encode())
        await svc.tick()
    subjects = [(m.subject, m.severity, m.kind) for m in t.sent]
    assert ("[XTNL DEADMAN] dead-man: beating node changed (node-a -> node-b)", "high", "episode") in subjects
    two = [m for m in t.sent if "two engines beating" in m.subject]
    assert two[0].severity == "critical" and two[0].kind == "episode"
    assert any(m.kind == "repage" and m.severity == "critical" for m in two)          # re-paged after 60 s
    notices = [m for m in t.sent if "beating node changed" in m.subject]
    assert all(m.severity == "high" for m in notices)                                  # re-pages keep their severity
    assert "beats in the last 60 s: node-a epoch 7 x" in two[0].body
    # node-b stops: two_engines resolves (info) within a window, node-a keeps beating
    for at in range(135, 300, 15):
        clock.ms = start + at * S
        svc.on_beat(svc_payload(clock.ms, node="node-a").to_json().encode())
        await svc.tick()
    assert "two_engines" not in svc.active
    resolved = [m for m in t.sent if m.subject.startswith("[XTNL DEADMAN] resolved: dead-man: two engines")]
    assert len(resolved) == 1 and resolved[0].severity == "info"
