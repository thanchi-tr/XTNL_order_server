"""Live review 2026-11-25, round 3: a `stop_kind_inconsistent` **re-page** names only the inconsistent child ids, like
its transition alert (oversee-rules.md §6 MissingStopLossRule: "naming only the inconsistent child ids", "... stays
capped SOFT and re-pages"; §10 re-pages). The ids are this pass's UNKNOWN detail, handed from the RiskSweep to the
dispatcher; they are never stored and never enter the reason, the ack match or the episode / dedupe key.
Synthetic prices only."""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import replace

import pytest

from src.oversee.config import SweepConfig
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import NotificationDispatcher, episode_key, is_acknowledged, repages_due
from src.oversee.risk import STOP_KIND_INCONSISTENT
from src.oversee.snapshot import BreachAck
from src.oversee.states import StepParams, step_state
from src.oversee.types import BreachState, Cap, Subject, Unknown
from tests.oversee.helpers import T0, position
from tests.oversee.test_stop_detail_live_review import GRACE, child
from tests.oversee.test_sweep import STOP_KEY
from tests.oversee.test_sweep_pass_bracket import Fenced

CFG = SweepConfig()
ESC = CFG.escalate_after_ms
POS = Subject("position", "stonex:P", "A1")
DETAIL = "stop child stonex:T1"


def inconsistent_state() -> BreachState:
    """The capped SOFT state after the 30 s UNKNOWN grace (the scratchpad reproduction)."""
    out = Unknown(STOP_KIND_INCONSISTENT, DETAIL)
    s, _ = step_state(None, "missing_stop_loss", POS, out, StepParams(0, 1, 0, CFG))
    s, trs = step_state(s, "missing_stop_loss", POS, out, StepParams(CFG.unknown_grace_ms, 2, 0, CFG))
    assert s is not None and s.active and s.cap is Cap.SOFT and s.reason == STOP_KIND_INCONSISTENT
    assert trs[0].detail == DETAIL
    return s


def test_repage_after_the_grace_names_the_inconsistent_child() -> None:
    s = inconsistent_state()
    (row,) = repages_due([s], [], {}, CFG.unknown_grace_ms + ESC, ESC, details={s.key: DETAIL})
    assert row.kind == "repage" and row.body == f"stop_kind_inconsistent: {DETAIL} (cap SOFT)"
    assert row.episode == episode_key(s.rule_id, s.subject_type, s.subject_id, s.activation_id, s.level)
    assert s.reason == STOP_KIND_INCONSISTENT          # the stored reason stays the bare code


def test_without_this_pass_detail_the_repage_shows_the_reason_alone() -> None:
    """A state whose current result is not the UNKNOWN (for example releasing) gets no ids, never stale ones."""
    s = inconsistent_state()
    (row,) = repages_due([s], [], {}, CFG.unknown_grace_ms + ESC, ESC)
    assert row.body == "stop_kind_inconsistent (cap SOFT)"
    (other,) = repages_due([s], [], {}, CFG.unknown_grace_ms + ESC, ESC, details={("x", "y", "z"): DETAIL})
    assert other.body == "stop_kind_inconsistent (cap SOFT)"


def test_other_reasons_never_show_a_detail() -> None:
    s = replace(inconsistent_state(), reason="mark_unknown")
    (row,) = repages_due([s], [], {}, CFG.unknown_grace_ms + ESC, ESC, details={s.key: "no mid ever seen"})
    assert row.body == "mark_unknown (cap SOFT)"


def test_an_ack_still_silences_the_repage_whatever_the_detail() -> None:
    s = inconsistent_state()
    ack = BreachAck(1, s.rule_id, s.subject_type, s.subject_id, s.level, s.cap, s.reason, "op", "seen",
                    CFG.unknown_grace_ms + 1)
    assert is_acknowledged(s, [ack])
    assert repages_due([s], [ack], {}, CFG.unknown_grace_ms + ESC, ESC, details={s.key: DETAIL}) == []


async def test_dispatcher_takes_staged_details_for_the_next_call_only() -> None:
    db = InMemoryOverseeDb()
    d = NotificationDispatcher(db, db, (), escalate_after_ms=ESC)
    s = inconsistent_state()
    d.stage_repage_details({s.key: DETAIL})
    assert await d.queue_repages([s], [], CFG.unknown_grace_ms + ESC) == 1
    assert db.t.outbox[-1].body == f"stop_kind_inconsistent: {DETAIL} (cap SOFT)"
    # the staged map was taken: a later call (one cadence on) without new details shows the reason alone
    assert await d.queue_repages([s], [], CFG.unknown_grace_ms + 2 * ESC) == 1
    assert db.t.outbox[-1].body == "stop_kind_inconsistent (cap SOFT)"
    # a newer stage replaces an older one (only the current pass's ids)
    d.stage_repage_details({s.key: "stop child stonex:OLD"})
    d.stage_repage_details({s.key: DETAIL})
    assert await d.queue_repages([s], [], CFG.unknown_grace_ms + 3 * ESC) == 1
    assert db.t.outbox[-1].body == f"stop_kind_inconsistent: {DETAIL} (cap SOFT)"


class ThreeArgDispatcher(NotificationDispatcher):
    """Like the runtime's liveness wrapper: overrides `queue_repages` with the plain three-argument signature."""

    calls = 0

    async def queue_repages(self, states: Iterable[BreachState], acks: Sequence[BreachAck], now_ms: int) -> int:
        self.calls += 1
        return await super().queue_repages(states, acks, now_ms)


@pytest.mark.usefixtures("stop_at_entry_on")
@pytest.mark.parametrize("cls", [NotificationDispatcher, ThreeArgDispatcher])
async def test_sweep_repage_names_the_inconsistent_child(cls: type[NotificationDispatcher]) -> None:
    db = InMemoryOverseeDb()
    dispatcher = cls(db, db, (), escalate_after_ms=ESC)
    env = Fenced(db=db, dispatcher=dispatcher, positions=(position("P", entry="1.1000"),),
                 orders=(child("T", "1.1200", "STOP"), child("S", "1.0900", "STOP")))
    await env.at(T0)
    await env.at(T0 + GRACE)
    s = env.state(STOP_KEY)
    assert s.active and s.cap is Cap.SOFT and s.reason == STOP_KIND_INCONSISTENT
    await env.at(T0 + GRACE + ESC)
    (row,) = [r for r in db.t.outbox if r.kind == "repage"]
    assert row.title.startswith("still SOFT_HALT missing_stop_loss position:")
    assert row.body == "stop_kind_inconsistent: stop child T (cap SOFT)"     # only the inconsistent child
    assert env.state(STOP_KEY).reason == STOP_KIND_INCONSISTENT
    assert not env.fences and "repage_failed" not in [p.code for p in env.pages]
    if isinstance(dispatcher, ThreeArgDispatcher):
        assert dispatcher.calls >= 3
