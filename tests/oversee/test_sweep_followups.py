"""IMPLEMENTATION_PLAN 4.9 Oversee follow-ups (oversee-rules.md §2, §4, §7, §10): quarantine release by a good
pass, evaluation_failed re-pages, CAS retry of the evaluation_failed row, RiskSweep.install_ruleset and the
re-page cadence of a reloaded ruleset. Clock injected; in-memory store."""
from __future__ import annotations

from typing import Any

from src.oversee.config import BandRuleConfig, OverseeConfig
from src.oversee.halt import HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import NotificationDispatcher
from src.oversee.ruleset import ActiveRuleset, parse_ruleset
from src.oversee.sweep import RiskSweep
from src.oversee.types import BreachState, Cap, HaltLevel, Level, Page
from tests.oversee.helpers import ACCT, AD, T0, live, position, quote, snap
from tests.oversee.test_sweep import CFG, Env

EVAL_KEY = ("evaluation", "account", ACCT)
ESC = CFG.sweep.escalate_after_ms


def codes(env: Env, code: str) -> int:
    return [p.code for p in env.pages].count(code)


# ---------------------------------------------------------------- sweep_quarantine released by a good pass
async def test_first_good_pass_releases_the_stored_quarantine_row() -> None:
    env = Env()
    await env.at(T0)
    env.db.failing_accounts.add(ACCT)
    await env.at(T0 + CFG.sweep.quarantine_after_ms)
    assert ACCT in env.db.t.quarantine
    env.db.failing_accounts.clear()
    await env.at(T0 + CFG.sweep.quarantine_after_ms + CFG.sweep.quarantine_retry_ms)
    assert ACCT not in env.db.t.quarantine and env.sweep.health(ACCT).quarantined_at_ms is None
    assert "quarantine_released" in [p.code for p in env.pages]


async def test_quarantine_row_left_by_an_earlier_leader_is_released_by_the_first_good_pass() -> None:
    db = InMemoryOverseeDb()
    db.t.quarantine[ACCT] = T0 - 600_000
    env = Env(db=db)
    await env.at(T0)
    assert ACCT not in db.t.quarantine


async def test_a_failed_quarantine_release_is_retried_and_never_fails_the_pass() -> None:
    class FlakyRelease(InMemoryOverseeDb):
        fail = 1

        async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
            if at_ms is None and self.fail:
                self.fail -= 1
                raise ConnectionError("release lost")
            await super().set_quarantine(account_id, at_ms, reason)

    db = FlakyRelease()
    db.t.quarantine[ACCT] = T0 - 600_000
    env = Env(db=db)
    r = await env.at(T0)
    assert r.succeeded == [ACCT] and ACCT in db.t.quarantine and codes(env, "quarantine_release_failed") == 1
    await env.at(T0 + 1000)
    assert ACCT not in db.t.quarantine


# ---------------------------------------------------------------- evaluation_failed re-pages
async def test_failing_account_repages_every_escalate_after_ms_even_while_quarantined() -> None:
    env = Env()
    await env.at(T0)
    env.db.failing_accounts.add(ACCT)
    first = T0 + CFG.sweep.evaluation_failed_after_ms
    await env.at(first - 1000)
    assert codes(env, "evaluation_failed") == 0
    await env.at(first)
    assert codes(env, "evaluation_failed") == 1
    page = next(p for p in env.pages if p.code == "evaluation_failed")
    assert page.account_id == ACCT and "RuntimeError" in page.detail
    await env.at(first + ESC - 1000)
    assert codes(env, "evaluation_failed") == 1
    r = await env.at(first + ESC)                       # quarantined since +5 min: skipped, still re-paged
    assert r.skipped == [ACCT] and codes(env, "evaluation_failed") == 2
    await env.at(first + 2 * ESC)
    assert codes(env, "evaluation_failed") == 3
    env.db.failing_accounts.clear()
    good = first + 2 * ESC + CFG.sweep.quarantine_retry_ms
    await env.at(good)
    env.db.failing_accounts.add(ACCT)                   # a new spell pages at once after 10 s
    await env.at(good + 1000)
    await env.at(good + CFG.sweep.evaluation_failed_after_ms)
    assert codes(env, "evaluation_failed") == 4


# ---------------------------------------------------------------- lost CAS on the evaluation_failed row
async def test_lost_cas_on_the_evaluation_failed_row_is_retried_under_halt_lock() -> None:
    env = Env()
    await env.at(T0)
    env.db.failing_accounts.add(ACCT)
    for t in range(T0 + 1000, T0 + 10_000, 1000):
        await env.at(t)
    # another writer moved the evaluation row the sweep last read (a resolved row, as a new term finds it)
    env.db.t.breaches[EVAL_KEY] = BreachState("evaluation", "account", ACCT, ACCT, Level.PASS, resolved_at=T0 - 5,
                                              resolved_by="sweep", last_sweep_id=7, clear_generation=4)
    await env.at(T0 + 10_000)
    row = env.state(EVAL_KEY)
    assert row.active and row.level is Level.SOFT_HALT and row.cap is Cap.SOFT and row.reason == "evaluation_failed"
    assert env.events("evaluation") == ["activated"] and await env.level() is HaltLevel.SOFT
    writes = env.db.writes
    await env.at(T0 + 11_000)                            # written once: no rewrite on later decisions
    assert env.db.writes == writes and env.state(EVAL_KEY) == row
    env.db.failing_accounts.clear()
    await env.at(T0 + 20_000)                            # the recovery pass resolves the stored row
    assert env.state(EVAL_KEY).resolved_by == "sweep" and env.state(EVAL_KEY).below_soft_since == T0 + 20_000


async def test_cas_retry_keeps_an_evaluation_failed_row_already_stored_active() -> None:
    db = InMemoryOverseeDb()
    stored = BreachState("evaluation", "account", ACCT, ACCT, Level.SOFT_HALT, Cap.SOFT, "evaluation_failed",
                         active_since=T0 - 60_000, activation_id=99, last_sweep_id=5, clear_generation=2)
    env = Env(db=db)
    db.failing_accounts.add(ACCT)
    db.set_snapshot(env.snapshot(T0))
    db.t.breaches[EVAL_KEY] = stored                     # left by the previous leader; this term never read it
    for t in range(T0, T0 + 11_000, 1000):
        await env.at(t)
    assert db.t.breaches[EVAL_KEY] == stored and env.sweep.health(ACCT).failure_row == stored


# ---------------------------------------------------------------- install_ruleset + reloaded re-page cadence
def _sweep_with_ruleset(db: InMemoryOverseeDb, active: ActiveRuleset, clock: list[int],
                        dispatcher: NotificationDispatcher | None = None,
                        pages: list[Page] | None = None) -> RiskSweep:
    sweep = RiskSweep(reader=db, writer=db, registry=active.registry, halt=HaltSwitch(db, db),
                      config=OverseeConfig(), clock=lambda: clock[0], epoch=lambda: db.engine_epoch,
                      dispatcher=dispatcher, pager=None if pages is None else pages.append)
    sweep.install_ruleset(active)
    return sweep


def _set(db: InMemoryOverseeDb, t: int, **kw: Any) -> None:
    db.set_snapshot(snap(now=t, account_live=live("1000"), quotes={(AD, "EURUSD"): quote("1.1000", t)}, **kw))


async def test_install_ruleset_installs_a_staged_ruleset_at_the_next_pass_without_the_wrapper() -> None:
    clock = [T0]
    db = InMemoryOverseeDb()
    active = ActiveRuleset(OverseeConfig(), clock_ms=lambda: clock[0])
    sweep = _sweep_with_ruleset(db, active, clock)
    _set(db, T0, positions=(position("P", stop="1.0840"),))                 # 1.6 %: max_position_risk SOFT
    await sweep.run_pass()
    pos_key = ("max_position_risk", "position", f"{AD}:P")
    assert db.t.breaches[pos_key].active
    active.stage(parse_ruleset({"max_position_risk": {"enabled": False}}, 2))
    clock[0] += 1000
    _set(db, clock[0], positions=(position("P", stop="1.0840"),))
    await sweep.run_pass()
    assert active.version == 2 and db.t.breaches[pos_key].resolved_by == "reload"


async def test_repage_cadence_follows_a_reloaded_ruleset() -> None:
    clock = [T0]
    db = InMemoryOverseeDb()
    active = ActiveRuleset(OverseeConfig(max_position_risk=BandRuleConfig(enabled=False)), clock_ms=lambda: clock[0])
    dispatcher = NotificationDispatcher(db, db, (), escalate_after_ms=900_000)   # the old constructor argument
    sweep = _sweep_with_ruleset(db, active, clock, dispatcher)
    assert dispatcher.escalate_after_ms == 900_000
    _set(db, T0, positions=(position("P"),))                                     # missing stop -> SOFT at +2 s
    for _ in range(3):
        await sweep.run_pass()
        clock[0] += 1000
    key = ("missing_stop_loss", "position", f"{AD}:P")
    assert db.t.breaches[key].active
    await dispatcher.drain(clock[0])                                             # the episode alert: dedupe set
    sent_at = clock[0]
    active.stage(parse_ruleset({"max_position_risk": {"enabled": False},
                                "sweep": {"escalate_after_ms": 600_000}}, 2))
    clock[0] = sent_at + 600_000
    _set(db, clock[0], positions=(position("P"),))
    await sweep.run_pass()                                                       # installs v2, then re-pages
    assert active.version == 2 and dispatcher.escalate_after_ms == 600_000
    assert [r.kind for r in db.t.outbox].count("repage") == 1


async def test_a_failing_ruleset_boundary_pages_and_the_pass_still_runs() -> None:
    class Broken(ActiveRuleset):
        def at_boundary(self) -> bool:
            raise RuntimeError("install failed")

    clock = [T0]
    db = InMemoryOverseeDb()
    pages: list[Page] = []
    sweep = _sweep_with_ruleset(db, Broken(OverseeConfig(), clock_ms=lambda: clock[0]), clock, pages=pages)
    _set(db, T0)
    r = await sweep.run_pass()
    assert r.succeeded == [ACCT] and [p.code for p in pages] == ["ruleset_install_failed"]


# ---------------------------------------------------------------- dispatcher cadence
async def test_dispatcher_rereads_a_callable_cadence() -> None:
    db = InMemoryOverseeDb()
    cadence = [900_000]
    dispatcher = NotificationDispatcher(db, db, (), escalate_after_ms=lambda: cadence[0])
    state = BreachState("max_aggregate_open_risk", "account", ACCT, ACCT, Level.SOFT_HALT, active_since=T0,
                        activation_id=1)
    db.t.dedupe[("max_aggregate_open_risk", "account", ACCT, 1, int(Level.SOFT_HALT))] = T0
    assert await dispatcher.queue_repages([state], (), T0 + 300_000) == 0
    cadence[0] = 300_000
    assert await dispatcher.queue_repages([state], (), T0 + 300_000) == 1
    dispatcher.follow_cadence(1_000_000)
    assert dispatcher.escalate_after_ms == 1_000_000


async def test_an_unsent_repage_is_not_queued_again_on_every_pass() -> None:
    """4.9 skeptic: a reload can shorten the dispatcher's cadence below the relay's (RelayPolicy, fixed), which
    then suppresses the row without recording a send; the next pass must not queue another row (one per
    second per state) but wait a full cadence."""
    db = InMemoryOverseeDb()
    dispatcher = NotificationDispatcher(db, db, (), escalate_after_ms=600_000)
    state = BreachState("max_aggregate_open_risk", "account", ACCT, ACCT, Level.SOFT_HALT, active_since=T0,
                        activation_id=1)
    db.t.dedupe[("max_aggregate_open_risk", "account", ACCT, 1, int(Level.SOFT_HALT))] = T0
    t = T0 + 600_000
    assert await dispatcher.queue_repages([state], (), t) == 1
    for dt in range(1000, 10_000, 1000):                   # never sent: dedupe still says T0
        assert await dispatcher.queue_repages([state], (), t + dt) == 0
    assert await dispatcher.queue_repages([state], (), t + 600_000) == 1
    assert [r.kind for r in db.t.outbox].count("repage") == 2
