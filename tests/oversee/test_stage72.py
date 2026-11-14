"""IMPLEMENTATION_PLAN 7.2 Oversee: every active rule state (FLAG included) re-pages every escalate_after_ms unless
acknowledged (oversee-rules.md §10), and FENCE alerts are at full severity as soon as a maintenance window ends
(operations.md §10). Clock injected; in-memory store."""
from __future__ import annotations

from dataclasses import replace

from src.oversee.inhibit import ESCALATION_CODE, SUMMARY_CODE, AlertInhibitor
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import EmailChannel, NotificationDispatcher, alerts_for_transitions, episode_key, repages_due
from src.oversee.rules import STALE_ACCOUNT_IMAGE, STALE_MARK, RuleRegistry
from src.oversee.snapshot import BreachAck, StreamHealthRow
from src.oversee.sweep import RiskSweep
from src.oversee.types import BreachState, Cap, Level, Transition
from tests.oversee.helpers import ACCT, AD, T0, position
from tests.oversee.test_sweep import CFG, Env

ESC = CFG.sweep.escalate_after_ms
MARK_MS = CFG.marks.stale_mark_ms
LOST = (StreamHealthRow(AD, "orders", T0 - 11_000),)


def flag_state(**kw: object) -> BreachState:
    base = BreachState(STALE_MARK, "account", ACCT, ACCT, level=Level.FLAG, reason=STALE_MARK, active_since=100,
                       activation_id=5)
    return replace(base, **kw)  # type: ignore[arg-type]


def key_of(s: BreachState) -> tuple[str, str, str, int, int]:
    return episode_key(s.rule_id, s.subject_type, s.subject_id, s.activation_id, s.level)


# ---------------------------------------------------------------- pure re-page rule
def test_an_active_flag_state_repages_by_email_every_escalate_after_ms() -> None:
    s = flag_state()
    sent = {key_of(s): 1_000}
    assert repages_due([s], [], sent, 1_000 + ESC - 1, ESC) == []
    (row,) = repages_due([s], [], sent, 1_000 + ESC, ESC)
    assert (row.kind, row.severity, row.episode, row.route) == ("repage", "info", key_of(s), ("email",))
    assert row.title == f"still FLAG {STALE_MARK} account:{ACCT}"


def test_an_acknowledged_flag_state_does_not_repage_until_it_changes() -> None:
    s = flag_state()
    ack = BreachAck(1, STALE_MARK, "account", ACCT, Level.FLAG, Cap.NONE, STALE_MARK, "op", "weekend", 200)
    assert repages_due([s], [ack], {key_of(s): 0}, 10 * ESC, ESC) == []
    assert len(repages_due([replace(s, reason="other")], [ack], {key_of(s): 0}, 10 * ESC, ESC)) == 1


def test_an_episode_never_delivered_repages_from_its_active_since() -> None:
    s = flag_state(active_since=5_000)            # its activation alert was held back: no dedupe row
    assert repages_due([s], [], {}, 5_000 + ESC - 1, ESC) == []
    assert [r.episode for r in repages_due([s], [], {}, 5_000 + ESC, ESC)] == [key_of(s)]


def test_resolved_or_held_states_never_repage() -> None:
    s = flag_state()
    assert repages_due([replace(s, level=Level.PASS)], [], {}, 10 * ESC, ESC) == []
    assert repages_due([replace(s, resolved_at=50)], [], {}, 10 * ESC, ESC) == []
    assert repages_due([s], [], {}, 10 * ESC, ESC, held=lambda st: st.rule_id == STALE_MARK) == []
    soft = replace(s, rule_id="agg", level=Level.SOFT_HALT)
    assert [r.severity for r in repages_due([s, soft], [], {}, 10 * ESC, ESC,
                                            held=lambda st: st.rule_id == STALE_MARK)] == ["high"]


async def test_a_rise_of_a_long_active_state_is_not_repaged_on_top_of_its_own_alert() -> None:
    db = InMemoryOverseeDb()
    disp = NotificationDispatcher(db, db, [EmailChannel()], escalate_after_ms=ESC)
    risen = flag_state(rule_id="agg", level=Level.SOFT_HALT, active_since=0)    # active for long, rose just now
    t = Transition("agg", "account", ACCT, ACCT, "raised", 9, 10 * ESC, Level.SOFT_HALT, Cap.NONE, "r", 5)
    alert = alerts_for_transitions([t])
    await db.add(alert)
    disp.mark_alerted([r.episode for r in alert if r.episode], 10 * ESC)
    assert await disp.queue_repages([risen], [], 10 * ESC) == 0
    assert await disp.queue_repages([risen], [], 10 * ESC + 1_000) == 0         # still within one cadence
    assert await disp.queue_repages([risen], [], 11 * ESC) == 1                  # undelivered for a whole cadence


# ---------------------------------------------------------------- through the RiskSweep
def with_dispatcher(env: Env, inhibitor: AlertInhibitor | None = None) -> NotificationDispatcher:
    disp = NotificationDispatcher(env.db, env.db, [EmailChannel()], escalate_after_ms=ESC)
    env.sweep = RiskSweep(reader=env.db, writer=env.db, registry=RuleRegistry.from_config(CFG), halt=env.halt,
                          config=CFG, clock=env.clock, epoch=lambda: env.db.engine_epoch, pager=env.pages.append,
                          dispatcher=disp, inhibitor=inhibitor)
    return disp


def repages(env: Env, rule: str) -> list[str]:
    return [r.severity for r in env.db.t.outbox if r.kind == "repage" and rule in r.title]


async def test_sweep_repages_an_active_stale_mark_flag_by_email() -> None:
    env = Env(positions=(position("P", stop="1.0990"),))
    disp = with_dispatcher(env)
    env.stale_quotes = True
    start = T0 + MARK_MS + 1_000
    await env.at(start)
    assert env.state((STALE_MARK, "account", ACCT)).active
    await disp.drain(start)                                    # the activation email is delivered and recorded
    await env.at(start + ESC - 1_000)
    assert repages(env, STALE_MARK) == []
    await env.at(start + ESC)
    assert repages(env, STALE_MARK) == ["info"]
    await env.at(start + ESC + 1_000)                         # once per cadence
    assert repages(env, STALE_MARK) == ["info"]


async def test_maintenance_holds_back_fence_repages_then_they_page_once_the_window_ends() -> None:
    maint = {AD: True}
    env = Env(stream_health=LOST)
    with_dispatcher(env, AlertInhibitor(in_maintenance=lambda a: maint.get(a, False)))
    await env.at(T0)
    assert env.state((STALE_ACCOUNT_IMAGE, "account", ACCT)).active
    await env.at(T0 + ESC + 5_000)                             # a long window: held back, re-pages too
    assert repages(env, STALE_ACCOUNT_IMAGE) == []
    maint[AD] = False                                          # the window ends: full severity at once
    await env.at(T0 + ESC + 6_000)
    assert repages(env, STALE_ACCOUNT_IMAGE) == ["info"]
    pages = [(p.code, p.severity) for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)]
    assert pages == [(SUMMARY_CODE, "info")]


async def test_an_epoch_change_hold_back_still_escalates_a_fence_that_outlasts_it() -> None:
    env = Env(stream_health=LOST)
    with_dispatcher(env, AlertInhibitor(epoch_changed_at_ms=lambda: T0, settle_ms=60_000))
    await env.at(T0)
    await env.at(T0 + 60_000)
    pages = [(p.code, p.severity) for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)]
    assert pages == [(ESCALATION_CODE, "high")]


async def test_maintenance_and_epoch_change_together_escalate() -> None:
    inh = AlertInhibitor(in_maintenance=lambda a: True, epoch_changed_at_ms=lambda: T0)
    t = Transition(STALE_ACCOUNT_IMAGE, "account", ACCT, ACCT, "activated", 1, T0, Level.FLAG, Cap.NONE, "x", 1)
    inh.record(ACCT, AD, [t], "epoch_change")
    inh.record(ACCT, AD, [t], "maintenance")
    fenced = BreachState(STALE_ACCOUNT_IMAGE, "account", ACCT, ACCT, level=Level.FLAG, active_since=T0)
    inh2 = AlertInhibitor(epoch_changed_at_ms=lambda: T0, settle_ms=1)
    inh2.record(ACCT, AD, [t], "epoch_change")
    inh2.record(ACCT, AD, [t], "maintenance")
    assert inh.summaries([fenced], T0 + 1) == []               # still suppressed
    (page,) = inh2.summaries([fenced], T0 + 10)
    assert (page.code, page.severity) == (ESCALATION_CODE, "high")


async def test_sweep_does_not_repage_a_rise_on_top_of_its_own_alert() -> None:
    from tests.oversee.test_sweep import AGG

    env = Env(positions=(position("P1", stop="1.0780"),))       # aggregate risk 2-3 %: FLAG
    disp = with_dispatcher(env)
    await env.every(T0, T0 + 2_000)
    await disp.drain(T0 + 2_000)
    two = (position("P1", stop="1.0780"), position("P2", stop="1.0780"))
    await env.at(T0 + ESC + 5_000, positions=two)              # > 3 %: rises to SOFT after confirm_ms
    await env.at(T0 + ESC + 6_500)
    assert env.state(AGG).level is Level.SOFT_HALT
    rows = [(r.kind, r.title.split()[0]) for r in env.db.t.outbox if "aggregate" in r.title]
    assert rows == [("episode", "FLAG"), ("repage", "still"), ("episode", "SOFT_HALT")]
