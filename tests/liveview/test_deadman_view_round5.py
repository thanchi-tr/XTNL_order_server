"""W1 review round 2: the Engine line does not show a green "OK" beside an ever-growing ack age.

The failed-beat streak only counts beats that were sent and refused. When the dead-man task dies, its circuit opens
or its loop sticks before the POST, the leader keeps exporting `xtnl_deadman_consecutive_failures 0` while
`xtnl_deadman_last_ok_age_seconds` climbs. The view now carries `stale` ("warn" past two missed 15 s beats, "alert"
past 60 s) and the page shows "no beat sent or acknowledged for N s" in that tone.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.liveview.events import guard_payload
from src.liveview.probe import interpret
from src.liveview.state import DEADMAN_ACK_ALERT_S, DEADMAN_ACK_WARN_S, deadman_stale, deadman_view
from src.runtime.deadman import DEADMAN_INTERVAL_S

T = 1_800_000_000_000
APP_JS = Path(__file__).resolve().parents[2] / "src" / "liveview" / "static" / "app.js"


def _view(age: float | None, failures: float = 0) -> dict[str, object] | None:
    raw: dict[str, list[tuple[dict[str, str], float]]] = {
        "xtnl_node_role": [({"role": "LEADER"}, 1.0)],
        "xtnl_deadman_configured": [({}, 1.0)],
        "xtnl_deadman_consecutive_failures": [({}, failures)],
    }
    if age is not None:
        raw["xtnl_deadman_last_ok_age_seconds"] = [({}, age)]
    return deadman_view(interpret(raw, T, 1))


def test_thresholds_follow_the_beat_interval() -> None:
    assert DEADMAN_ACK_WARN_S == int(2 * DEADMAN_INTERVAL_S)
    assert DEADMAN_ACK_ALERT_S == 60


@pytest.mark.parametrize(("age", "stale"), [(0, None), (14.9, None), (30, None), (31, "warn"), (60, "warn"),
                                            (61, "alert"), (900, "alert"), (3600, "alert")])
def test_stale_from_the_age_alone_while_failures_stay_zero(age: float, stale: str | None) -> None:
    view = _view(age)
    assert view is not None and view["failures"] == 0
    assert view["stale"] == stale
    guard_payload({"engine": {"status": "running", "deadman": view}})


def test_no_ack_yet_is_not_stale() -> None:
    assert deadman_stale(None) is None
    view = _view(None)
    assert view is not None and view["ok_age_s"] is None and view["stale"] is None


def test_the_page_picks_the_tone_from_stale_before_saying_ok() -> None:
    js = APP_JS.read_text(encoding="utf-8")
    stale_at = js.index("dead-man receiver: no beat sent or acknowledged for ")
    ok_at = js.index("dead-man receiver: OK, last ack ")
    assert stale_at < ok_at
    assert "dm.stale === 'warn' || dm.stale === 'alert'" in js
