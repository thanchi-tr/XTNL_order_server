"""W1 review round 1: the live view's Engine line shows the dead-man receiver state from the leader's gauges.

The probe already keeps `xtnl_deadman_configured`, `xtnl_deadman_consecutive_failures` and
`xtnl_deadman_last_ok_age_seconds`; the state payload now carries them under `engine.deadman` (counts and ages only,
through the payload guard), and the page renders "dead-man receiver: ..." on the Engine tile.
"""
from __future__ import annotations

from pathlib import Path

from src.liveview.events import guard_payload
from src.liveview.probe import ProbeAnswer, interpret
from src.liveview.state import deadman_view

T = 1_800_000_000_000
APP_JS = Path(__file__).resolve().parents[2] / "src" / "liveview" / "static" / "app.js"


def _leader(**samples: float) -> ProbeAnswer:
    raw: dict[str, list[tuple[dict[str, str], float]]] = {"xtnl_node_role": [({"role": "LEADER"}, 1.0)]}
    for name, v in samples.items():
        raw[name] = [({}, v)]
    return interpret(raw, T, 1)


def test_ok_receiver() -> None:
    p = _leader(xtnl_deadman_configured=1, xtnl_deadman_consecutive_failures=0, xtnl_deadman_last_ok_age_seconds=12.7)
    assert p.leader_answer
    view = deadman_view(p)
    assert view == {"configured": True, "failures": 0, "ok_age_s": 12, "stale": None}
    guard_payload({"engine": {"status": "running", "deadman": view}})


def test_not_acknowledging_and_not_configured() -> None:
    p = _leader(xtnl_deadman_configured=1, xtnl_deadman_consecutive_failures=21)
    assert deadman_view(p) == {"configured": True, "failures": 21, "ok_age_s": None, "stale": None}
    p = _leader(xtnl_deadman_configured=0)
    assert deadman_view(p) == {"configured": False, "failures": None, "ok_age_s": None, "stale": None}


def test_nothing_without_an_answering_leader() -> None:
    assert deadman_view(None) is None
    assert deadman_view(ProbeAnswer(ok=False, at_ms=T, poll_no=1)) is None
    standby = interpret({"xtnl_node_role": [({"role": "STANDBY"}, 1.0)], "xtnl_deadman_configured": [({}, 1.0)]},
                        T, 1)
    assert deadman_view(standby) is None


def test_the_page_renders_the_three_states() -> None:
    js = APP_JS.read_text(encoding="utf-8")
    for text in ("dead-man receiver: not configured", "dead-man receiver: not acknowledging, ",
                 "dead-man receiver: OK, last ack ", "var dm = eg.deadman;"):
        assert text in js, text
