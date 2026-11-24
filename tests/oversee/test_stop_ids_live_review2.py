"""Live review 2026-11-25, round 2: a `stop_kind_inconsistent` alert names only the inconsistent child ids
(oversee-rules.md §6 MissingStopLossRule). The ids ride in `Transition.detail`, never in the reason, so the stored
reason, the `oversee_events` row and every exact-equality use of `reason` are unchanged. Synthetic prices only."""
from __future__ import annotations

import re

import pytest

from src.dto import OrderStatus
from src.notifications.email_html import STOP_KIND_INCONSISTENT_ACTION, rule_action
from src.oversee.config import SweepConfig
from src.oversee.notify import alerts_for_transitions
from src.oversee.risk import STOP_KIND_INCONSISTENT
from src.oversee.states import StepParams, step_state
from src.oversee.types import Cap, Level, Result, Subject, Transition, Unknown
from tests.oversee.helpers import T0, position
from tests.oversee.test_stop_detail_live_review import GRACE, child, inconsistent_state_active
from tests.oversee.test_sweep import STOP_KEY, Env

CFG = SweepConfig()
POS = Subject("position", "stonex:P", "A1")
EPISODE_BODY = re.compile(r"^(?P<reason>.*) \(cap (?P<cap>[^,()]+), sweep (?P<sweep>[^()]+)\)$", re.S)


def test_unknown_detail_rides_on_the_transition_and_never_in_the_reason() -> None:
    out = Unknown(STOP_KIND_INCONSISTENT, "stop child stonex:T1,stonex:T2")
    s, trs = step_state(None, "missing_stop_loss", POS, out, StepParams(0, 1, 0, CFG))
    assert trs == [] and s is not None
    s, trs = step_state(s, "missing_stop_loss", POS, out, StepParams(CFG.unknown_grace_ms, 2, 0, CFG))
    assert s is not None and s.active and s.reason == STOP_KIND_INCONSISTENT and s.cap is Cap.SOFT
    (t,) = trs
    assert t.transition == "activated" and t.reason == STOP_KIND_INCONSISTENT
    assert t.detail == "stop child stonex:T1,stonex:T2"


def test_alert_body_names_only_the_inconsistent_child_ids_and_keeps_the_reason_code() -> None:
    t = Transition("missing_stop_loss", "position", "stonex:P", "A1", "activated", 7, 1000, Level.SOFT_HALT,
                   Cap.SOFT, STOP_KIND_INCONSISTENT, 7, detail="stop child stonex:T1")
    (row,) = alerts_for_transitions([t])
    assert row.body == "stop_kind_inconsistent: stop child stonex:T1 (cap SOFT, sweep 7)"
    m = EPISODE_BODY.match(row.body)
    assert m is not None and m.group("cap") == "SOFT" and m.group("sweep") == "7"
    # the email's action line still recognises the code inside the longer reason text
    assert rule_action("missing_stop_loss", 2, m.group("reason"), "position") == \
        STOP_KIND_INCONSISTENT_ACTION.format(what="position")
    (info,) = alerts_for_transitions([Transition("missing_stop_loss", "position", "stonex:P", "A1", "capped", 8,
                                                 2000, Level.SOFT_HALT, Cap.SOFT, STOP_KIND_INCONSISTENT, 7,
                                                 detail="stop child stonex:T1")])
    assert info.body == "stop_kind_inconsistent: stop child stonex:T1 (level SOFT_HALT, cap SOFT)"


def test_other_unknown_details_stay_out_of_the_alert_text() -> None:
    t = Transition("max_position_risk", "position", "stonex:P", "A1", "activated", 7, 1000, Level.SOFT_HALT,
                   Cap.SOFT, "mark_unknown", 7, detail="no mid ever seen for ('stonex', 'EURUSD')")
    (row,) = alerts_for_transitions([t])
    assert row.body == "mark_unknown (cap SOFT, sweep 7)"


def test_capped_transition_of_another_reason_gets_no_stop_detail() -> None:
    """An active uncapped missing_stop_loss state that turns UNKNOWN stop_kind_inconsistent is capped under its own
    reason: the detail belongs to the new result, not to the old reason's alert."""
    s, trs = step_state(None, "missing_stop_loss", POS, Result(Level.SOFT_HALT, "missing_stop_loss"),
                        StepParams(0, 1, 0, CFG))
    assert s is not None and s.active
    out = Unknown(STOP_KIND_INCONSISTENT, "stop child stonex:T1")
    s, _ = step_state(s, "missing_stop_loss", POS, out, StepParams(1000, 2, 0, CFG))
    assert s is not None
    s, trs = step_state(s, "missing_stop_loss", POS, out, StepParams(1000 + CFG.unknown_grace_ms, 3, 0, CFG))
    (t,) = trs
    assert t.transition == "capped" and t.reason == "missing_stop_loss" and t.detail == ""


@pytest.mark.usefixtures("stop_at_entry_on")
async def test_sweep_alert_names_the_child_id_and_the_stored_reason_is_the_bare_code() -> None:
    env = await inconsistent_state_active()
    assert env.state(STOP_KEY).reason == STOP_KIND_INCONSISTENT
    (ev,) = [e for e in env.db.t.events.values() if e.rule_id == "missing_stop_loss"]
    assert ev.reason == STOP_KIND_INCONSISTENT
    (row,) = [r for r in env.db.t.outbox if r.title.startswith("SOFT_HALT missing_stop_loss")]
    assert row.body.startswith("stop_kind_inconsistent: stop child T (cap SOFT, sweep ")
    # a correctly placed stop: no ids (nothing is inconsistent) and the state resolves as before
    await env.at(T0 + GRACE + 1000, orders=(child("T", "1.1200", "STOP", status=OrderStatus.CANCELLED),
                                            child("S", "1.0900", "STOP")))
    assert not env.state(STOP_KEY).active


@pytest.mark.usefixtures("stop_at_entry_on")
async def test_only_the_inconsistent_child_is_named_next_to_a_correct_stop() -> None:
    env = Env(positions=(position("P", entry="1.1000"),),
              orders=(child("T", "1.1200", "STOP"), child("S", "1.0900", "STOP")))
    await env.at(T0)
    await env.at(T0 + GRACE)
    (row,) = [r for r in env.db.t.outbox if r.title.startswith("SOFT_HALT missing_stop_loss")]
    assert row.body.startswith("stop_kind_inconsistent: stop child T (cap SOFT, sweep ")
