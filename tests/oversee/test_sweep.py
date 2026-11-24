"""RiskSweep end to end against the in-memory store (oversee-rules.md §2, §4, §8). Clock injected."""
from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from typing import Any

from src.oversee.config import BandRuleConfig, OverseeConfig, SweepConfig
from src.oversee.halt import HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.rules import RuleRegistry
from src.oversee.snapshot import AccountSnapshot, StreamHealthRow, SymbolQuotes
from src.oversee.sweep import PassReport, RiskSweep
from src.oversee.types import BreachState, Cap, HaltLevel, Level, Page
from tests.oversee.helpers import ACCT, AD, ROLL, T0, account, clean_mark, flow, live, position, quote, snap, sod

CFG = OverseeConfig(max_position_risk=BandRuleConfig(enabled=False))
AGG = ("max_aggregate_open_risk", "account", ACCT)
STOP_KEY = ("missing_stop_loss", "position", f"{AD}:P")


class Clock:
    def __init__(self, t: int) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


class Env:
    def __init__(self, cfg: OverseeConfig = CFG, db: InMemoryOverseeDb | None = None, **kw: Any) -> None:
        self.clock = Clock(T0)
        self.db = db or InMemoryOverseeDb()
        self.pages: list[Page] = []
        self.cf_calls: list[str] = []
        self.cf_gate = asyncio.Event()
        self.mid = "1.1000"
        self.stale_quotes = False
        self.kw: dict[str, Any] = dict(account_live=live("1000")) | kw
        self.halt = HaltSwitch(self.db, self.db)
        self.sweep = RiskSweep(reader=self.db, writer=self.db, registry=RuleRegistry.from_config(cfg),
                               halt=self.halt, config=cfg, clock=self.clock, epoch=lambda: self.db.engine_epoch,
                               pager=self.pages.append, cash_flow_requester=self._request)

    async def _request(self, account_id: str) -> None:
        self.cf_calls.append(account_id)
        await self.cf_gate.wait()

    def snapshot(self, t: int) -> AccountSnapshot:
        q = quote(self.mid, T0 if self.stale_quotes else t)
        if self.stale_quotes:
            q = SymbolQuotes(latest=q.latest, last_clean=clean_mark(self.mid, T0))
        return snap(now=t, **({"quotes": {(AD, "EURUSD"): q}} | self.kw))

    async def at(self, t: int, **kw: Any) -> PassReport:
        self.clock.t = t
        self.kw.update(kw)
        self.db.set_snapshot(self.snapshot(t))
        return await self.sweep.run_pass()

    async def every(self, t0: int, t1: int, step: int = 1000) -> None:
        for t in range(t0, t1 + 1, step):
            await self.at(t)

    async def level(self) -> HaltLevel:
        return (await self.halt.status()).effective_level

    def state(self, key: tuple[str, str, str]) -> BreachState:
        return self.db.t.breaches[key]

    def events(self, rule: str) -> list[str]:
        return [e.transition for e in sorted(self.db.t.events.values(), key=lambda e: e.sweep_id)
                if e.rule_id == rule]


async def test_aggregate_soft_then_resolved_without_escalation() -> None:
    env = Env(positions=(position("P1", stop="1.0840"), position("P2", stop="1.0840")))   # 3.2 %
    await env.at(T0)
    assert AGG in env.db.t.breaches and not env.state(AGG).active          # confirm_ms 1000
    await env.at(T0 + 1000)
    assert env.state(AGG).level is Level.SOFT_HALT and await env.level() is HaltLevel.SOFT
    env.kw["positions"] = (position("P1", stop="1.0880"), position("P2", stop="1.1000"))  # 1.2 %
    await env.every(T0 + 2000, T0 + 6000)
    assert env.state(AGG).active
    await env.at(T0 + 7000)
    assert not env.state(AGG).active and env.state(AGG).resolved_by == "sweep"
    assert env.events("max_aggregate_open_risk") == ["activated", "resolved"]
    assert await env.level() is HaltLevel.SOFT
    await env.at(T0 + 7000 + CFG.sweep.cool_down_ms - 1000)
    assert await env.level() is HaltLevel.SOFT
    await env.at(T0 + 7000 + CFG.sweep.cool_down_ms)
    assert await env.level() is HaltLevel.NONE
    assert all(e.effective_level is not HaltLevel.HARD for e in env.db.t.halt_events)


async def test_missing_stop_after_grace_is_soft() -> None:
    env = Env(positions=(position("P"),))
    await env.at(T0)
    assert env.db.t.stop_watch == {("position", f"{AD}:P"): T0}
    await env.at(T0 + 1000)
    assert STOP_KEY not in env.db.t.breaches
    await env.at(T0 + 2000)
    assert env.state(STOP_KEY).level is Level.SOFT_HALT and await env.level() is HaltLevel.SOFT
    env.kw["positions"] = (position("P", stop="1.09"),)
    await env.at(T0 + 3000)
    assert env.db.t.stop_watch == {}


async def test_failed_pass_changes_nothing_then_evaluation_failed_then_quarantine() -> None:
    env = Env(positions=(position("P"),))
    await env.every(T0, T0 + 2000)
    before = dict(env.db.t.breaches)
    env.db.failing_accounts.add(ACCT)
    report = await env.at(T0 + 3000)
    assert report.failed == [ACCT] and env.db.t.breaches == before and await env.level() is HaltLevel.SOFT
    await env.at(T0 + 12_000)
    assert [p.code for p in env.pages] == ["evaluation_failed"]
    assert report.decision is not None
    r = await env.at(T0 + 200_000)
    assert r.decision is not None and r.decision.target is HaltLevel.SOFT    # last committed states held
    await env.at(T0 + 2000 + CFG.sweep.quarantine_after_ms)
    assert "oversight_lapsed" in [p.code for p in env.pages] and ACCT in env.db.t.quarantine
    r = await env.at(T0 + 2000 + CFG.sweep.quarantine_after_ms + 1000)
    assert r.skipped == [ACCT]                                            # half-open: retried every 10 min
    env.db.failing_accounts.clear()
    r = await env.at(T0 + 2000 + CFG.sweep.quarantine_after_ms + CFG.sweep.quarantine_retry_ms)
    assert r.succeeded == [ACCT] and env.sweep.health(ACCT).quarantined_at_ms is None


async def test_dependency_wide_failure_is_not_an_account_fault() -> None:
    env = Env()
    await env.at(T0)
    env.db.dependency_down = True
    for t in range(T0 + 1000, T0 + 30_000, 1000):
        r = await env.at(t)
        assert r.dependency_down and r.failed == []
    assert env.sweep.health(ACCT).first_failure_ms is None
    assert [p.code for p in env.pages] == ["postgres_unavailable"]


async def test_account_fence_caps_results_and_flags_image() -> None:
    lost = (StreamHealthRow(AD, "orders", T0 - 11_000),)
    env = Env(cfg=OverseeConfig(), stream_health=lost, positions=(position("P", stop="1.0840"),))   # 1.6 %
    await env.at(T0)
    pos = env.state(("max_position_risk", "position", f"{AD}:P"))
    assert pos.level is Level.SOFT_HALT and pos.cap is Cap.SOFT
    assert env.state(("stale_account_image", "account", ACCT)).level is Level.FLAG
    view = env.sweep.account_view(ACCT)
    assert view is not None and view.last_pass_ms == T0


async def test_fenced_daily_loss_never_reaches_hard() -> None:
    lost = (StreamHealthRow(AD, "account", T0 - 11_000),)
    env = Env(stream_health=lost, positions=(position("P", "BUY", "100000", "1.1350", stop="1.0"),))
    await env.every(T0, T0 + 5000)
    day = next(s for s in env.db.t.breaches.values() if s.rule_id == "max_daily_loss")
    assert day.level is Level.SOFT_HALT and day.cap is Cap.SOFT and await env.level() is HaltLevel.SOFT


def losing_day(**kw: Any) -> Env:
    return Env(sod=sod("110000", "100000", uiar="10000", written=ROLL + 1000), account=account("110000"),
               positions=(position("P", "BUY", "100000", "1.1350", stop="1.1340"),), **kw)


def day_state(env: Env) -> BreachState:
    return next(s for s in env.db.t.breaches.values() if s.rule_id == "max_daily_loss")


async def test_daily_loss_with_late_pre_roll_deposit_trips_hard_and_latches() -> None:
    env = losing_day()
    await env.at(T0)
    assert not day_state(env).active
    await env.at(T0 + 1000)
    assert day_state(env).level is Level.HARD_HALT and await env.level() is HaltLevel.HARD
    env.kw["cash_flows"] = (flow("dep", "DEPOSIT", "10000", ROLL - 60_000, committed=T0 + 1500),)
    await env.at(T0 + 2000)
    assert day_state(env).level is Level.HARD_HALT    # explaining the deposit does not change the loss
    latest = env.db.t.halt_events[-1].id
    out = await env.halt.operator_clear("op", "reviewed", latest, now_ms=T0 + 2500)
    assert out.status == "refused" and "max_daily_loss" in out.reason
    env.mid = "1.1350"                                  # price recovers: state resolves after release_ms
    await env.every(T0 + 3000, T0 + 9000)
    assert not day_state(env).active
    await env.at(T0 + 9000 + 2 * CFG.sweep.cool_down_ms)
    assert await env.level() is HaltLevel.HARD            # latch held until an operator clear
    out = await env.halt.operator_clear("op", "reviewed", env.db.t.halt_events[-1].id, now_ms=T0 + 10**6)
    assert out.status == "cleared" and await env.level() is HaltLevel.NONE


async def test_forced_clear_resolves_capped_hard_and_capped_results_recreate_only_soft() -> None:
    env = losing_day()
    await env.every(T0, T0 + 1000)
    assert await env.level() is HaltLevel.HARD
    env.stale_quotes = True                              # every result capped from now on
    await env.every(T0 + 2000, T0 + 10_000)
    hard = day_state(env)
    assert hard.level is Level.HARD_HALT and hard.cap is Cap.SOFT   # capped can't lower HARD
    latest = env.db.t.halt_events[-1].id
    out = await env.halt.operator_clear("op", "feed frozen; checked with broker", latest, force=True,
                                        now_ms=T0 + 10_500)
    assert out.status == "cleared" and day_state(env).resolved_by == "operator"
    await env.every(T0 + 11_000, T0 + 15_000)
    st = day_state(env)
    assert st.level is Level.SOFT_HALT and st.cap is Cap.SOFT and await env.level() is HaltLevel.SOFT


async def test_escalation_by_accumulated_time_then_clear_resets_clock() -> None:
    cfg = replace(CFG, sweep=replace(SweepConfig(), escalate_after_ms=5000))
    env = Env(cfg=cfg, positions=(position("P"),))
    await env.every(T0, T0 + 6000)
    assert await env.level() is HaltLevel.SOFT          # uncapped since activation at +2000: 4 000 ms
    await env.at(T0 + 7000)
    assert await env.level() is HaltLevel.HARD and env.db.t.halt_events[-1].reason == "escalation"
    out = await env.halt.operator_clear("op", "reviewed", env.db.t.halt_events[-1].id, now_ms=T0 + 7500)
    assert out.status == "cleared" and await env.level() is HaltLevel.SOFT
    assert env.state(STOP_KEY).uncapped_ms == 0 and env.state(STOP_KEY).clear_generation == 1
    await env.every(T0 + 8000, T0 + 11_000)
    assert await env.level() is HaltLevel.SOFT          # a full escalate_after_ms again


async def test_operator_clear_between_read_and_write_forces_recompute() -> None:
    class RacingDb(InMemoryOverseeDb):
        race = False

        async def read_account(self, account_id: str, now_ms: int) -> AccountSnapshot:
            s = await super().read_account(account_id, now_ms)
            if self.race:
                self.race = False
                for k, b in list(self.t.breaches.items()):
                    self.t.breaches[k] = replace(b, uncapped_ms=0, clear_generation=b.clear_generation + 1)
            return s

    db = RacingDb()
    env = Env(db=db, positions=(position("P"),))
    await env.every(T0, T0 + 4000)
    db.race = True
    r = await env.at(T0 + 5000)
    assert r.succeeded == [ACCT]
    st = env.state(STOP_KEY)
    assert st.clear_generation == 1 and st.uncapped_ms == 1000   # rebuilt from the cleared row


async def test_epoch_mismatch_write_propagates_instead_of_failing_the_account() -> None:
    """5.3: a lost fence is never an account failure; it propagates (the runtime hard-exits)."""
    import pytest

    from src.oversee.sweep import EpochMismatch
    env = Env(positions=(position("P"),))
    await env.at(T0)
    env.db.engine_epoch = 2
    env.sweep._epoch = lambda: 1     # a paused ex-leader still on epoch 1
    with pytest.raises(EpochMismatch):
        await env.at(T0 + 1000)
    assert env.sweep.health(ACCT).first_failure_ms is None


async def test_sod_row_written_on_first_pass_after_roll() -> None:
    from src.oversee.dayroll import NEW_YORK
    from datetime import datetime
    roll = int(datetime(2026, 11, 11, 17, tzinfo=NEW_YORK).timestamp() * 1000)
    env = Env(sod=None)
    await env.at(roll + 5000)
    row = next(iter(env.db.t.sod_marks.values()))
    assert row.roll_ms == roll and not row.partial_session
    env2 = Env(sod=None)
    await env2.at(roll + 31_000)
    assert next(iter(env2.db.t.sod_marks.values())).partial_session
    assert [r.title.split()[0] for r in env2.db.t.outbox].count("sod_partial_session") == 1


async def test_previous_day_state_resolves_at_roll() -> None:
    env = losing_day()
    await env.every(T0, T0 + 1000)
    old = day_state(env)
    from datetime import datetime
    from src.oversee.dayroll import NEW_YORK
    roll = int(datetime(2026, 11, 11, 17, tzinfo=NEW_YORK).timestamp() * 1000)
    env.kw.update(sod=None, account=account("100000"), positions=())
    await env.at(roll + 1000)
    assert env.db.t.breaches[old.key].resolved_by == "day_end"
    assert await env.level() is HaltLevel.HARD          # the latch still needs an operator


async def test_unexplained_decrease_requests_cash_flows_once_a_minute() -> None:
    env = Env(account=account("98000"))
    await env.at(T0)
    await asyncio.sleep(0)
    assert env.cf_calls == [ACCT]
    env.cf_gate.set()
    await asyncio.sleep(0)
    await env.at(T0 + 30_000)
    await asyncio.sleep(0)
    assert env.cf_calls == [ACCT]
    await env.at(T0 + 60_000)
    await asyncio.sleep(0)
    assert env.cf_calls == [ACCT, ACCT]


async def test_gate_view_reports_stale_marks_and_unknowns() -> None:
    env = Env(account_live=None, positions=(position("P", stop="1.09"),))
    env.stale_quotes = True
    await env.at(T0 + 10_000)
    view = env.sweep.account_view(ACCT)
    assert view is not None and view.has_unknown and (AD, "EURUSD") in view.stale_symbols


async def test_run_loop_sweeps_on_interval_and_on_signal() -> None:
    cfg = replace(CFG, sweep=replace(SweepConfig(), interval_ms=20))
    db = InMemoryOverseeDb()
    db.set_snapshot(snap(now=int(time.time() * 1000)))
    sweep = RiskSweep(reader=db, writer=db, registry=RuleRegistry.from_config(cfg), halt=HaltSwitch(db, db),
                      config=cfg, clock=lambda: int(time.time() * 1000), epoch=lambda: 1)
    task = asyncio.create_task(sweep.run())
    await asyncio.sleep(0.08)
    sweep.sweep_now(ACCT)
    await asyncio.sleep(0.02)
    sweep.stop()
    await asyncio.wait_for(task, 1)
    assert sweep.account_view(ACCT) is not None


async def test_removed_account_no_longer_blocks_trips_after_a_clear() -> None:
    env = Env(positions=(position("P"),))
    await env.at(T0)
    env.db.base.clear()
    env.clock.t = T0 + 1000
    await env.sweep.run_pass()
    assert env.sweep.committed_states() == [] and env.sweep.account_view(ACCT) is None
    await env.halt.trip(HaltLevel.SOFT, rule_id="x", reason="x", now_ms=T0 + 1500)
    await env.halt.operator_clear("op", "reviewed", 1, now_ms=T0 + 2000)
    assert await env.level() is HaltLevel.NONE
    for t in range(T0 + 3000, T0 + 6000, 1000):
        await env.at(t)
    assert await env.level() is HaltLevel.SOFT     # the account is back; its missing-stop state trips again


# ---------------------------------------------------------------- review findings (group "oversee")

def other_account(t: int, balance: str = "100000", acct: str = "B1") -> AccountSnapshot:
    """A second, healthy account; a balance below its start of day is a daily loss."""
    return snap(now=t, account_id=acct, account=replace(account(balance), account_id=acct),
                sod=replace(sod(), account_id=acct), account_live=replace(live("1000", ts=t), account_id=acct))


async def test_fresh_hard_breach_trips_while_another_account_fails_after_a_clear() -> None:
    """trip-refused-stale-basis: a failing account's frozen read time must not veto a fresh trip."""
    env = Env()
    for t in range(T0, T0 + 2000, 1000):
        env.db.set_snapshot(other_account(t))
        await env.at(t)
    env.db.failing_accounts.add(ACCT)
    for t in range(T0 + 2000, T0 + 14_000, 1000):
        env.db.set_snapshot(other_account(t))
        await env.at(t)
    assert await env.level() is HaltLevel.SOFT and env.db.t.halt_events[-1].reason == "evaluation_failed"
    await env.halt.trip(HaltLevel.HARD, rule_id="manual", reason="drill", now_ms=T0 + 14_100)
    out = await env.halt.operator_clear("op", "drill reviewed", env.db.t.halt_events[-1].id, now_ms=T0 + 14_500)
    assert out.status == "cleared" and await env.level() is not HaltLevel.HARD
    env.cf_gate.set()                                     # the cash-flow fetch finds no transfer
    for t in range(T0 + 15_000, T0 + 20_000, 1000):      # B1 loses 5 %: an uncapped HARD on a healthy account
        env.db.set_snapshot(other_account(t, "95000"))
        await env.at(t)
        await asyncio.sleep(0)
    hard = env.db.t.breaches[("max_daily_loss", "account_day", f"B1:{sod().trading_day.isoformat()}")]
    assert hard.level is Level.HARD_HALT and hard.cap is Cap.NONE
    assert await env.level() is HaltLevel.HARD and env.db.t.halt_events[-1].rule_id == "max_daily_loss"
    assert env.db.t.halt_events[-1].hard_latch


async def test_stale_states_of_a_failing_account_alone_cannot_re_trip_after_a_clear() -> None:
    """The basis still refuses a trip justified only by states read before the operator clear."""
    env = losing_day()
    await env.every(T0, T0 + 1000)
    assert await env.level() is HaltLevel.HARD
    env.db.failing_accounts.add(ACCT)                     # its HARD state is now frozen at T0 + 1000
    env.db.t.halt_events.append(replace(env.db.t.halt_events[-1], id=len(env.db.t.halt_events) + 1,
                                        level=HaltLevel.NONE, effective_level=HaltLevel.NONE,
                                        hard_latch=False, by="op", at_ms=T0 + 1500))
    await env.at(T0 + 2000)
    assert await env.level() is HaltLevel.NONE            # refused: read before the clear


async def test_evaluation_failed_state_is_written_and_cool_down_runs_from_recovery() -> None:
    """cooldown-skipped-eval-failed: the SOFT it caused clears cool_down_ms after recovery, not after rose_at."""
    env = Env()
    await env.at(T0)
    env.db.failing_accounts.add(ACCT)
    for t in range(T0 + 1000, T0 + 13_000, 1000):
        await env.at(t)
    key = ("evaluation", "account", ACCT)
    row = env.state(key)                                   # written by the level decision (section 2)
    assert row.active and row.level is Level.SOFT_HALT and row.cap is Cap.SOFT
    assert env.events("evaluation") == ["activated"]
    out = await env.halt.operator_clear("op", "seen", env.db.t.halt_events[-1].id, now_ms=T0 + 13_500)
    assert out.status == "cleared" and await env.level() is HaltLevel.SOFT   # the clear sees the failure
    for t in range(T0 + 14_000, T0 + 290_000, 10_000):
        await env.at(t)
    env.db.failing_accounts.clear()
    recovered = T0 + 290_000
    await env.at(recovered)
    row = env.state(key)
    assert row.resolved_by == "sweep" and row.below_soft_since == recovered
    assert env.events("evaluation") == ["activated", "resolved"]
    await env.at(recovered + 30_000)
    assert await env.level() is HaltLevel.SOFT             # rose_at + cool_down has passed, not recovery + it
    await env.at(recovered + CFG.sweep.cool_down_ms - 1000)
    assert await env.level() is HaltLevel.SOFT
    await env.at(recovered + CFG.sweep.cool_down_ms)
    assert await env.level() is HaltLevel.NONE


async def test_cool_down_runs_from_recovery_when_the_failure_state_write_failed() -> None:
    class NoFailureRowDb(InMemoryOverseeDb):
        async def write_account(self, batch: Any) -> None:
            if any(w.state.rule_id == "evaluation" and w.state.active for w in batch.states):
                raise RuntimeError("write failed")
            await super().write_account(batch)

    env = Env(db=NoFailureRowDb())
    await env.at(T0)
    env.db.failing_accounts.add(ACCT)
    for t in range(T0 + 1000, T0 + 290_000, 10_000):
        await env.at(t)
    assert await env.level() is HaltLevel.SOFT and ("evaluation", "account", ACCT) not in env.db.t.breaches
    env.db.failing_accounts.clear()
    recovered = T0 + 290_000
    await env.at(recovered)
    assert env.state(("evaluation", "account", ACCT)).below_soft_since == recovered
    await env.at(recovered + 30_000)
    assert await env.level() is HaltLevel.SOFT
    await env.at(recovered + CFG.sweep.cool_down_ms)
    assert await env.level() is HaltLevel.NONE


async def test_cool_down_runs_from_the_close_of_a_soft_position() -> None:
    """cooldown-skipped-entity-end: the store (like PgSnapshotReader) no longer loads the resolved position
    row as a live subject on later passes, yet its below_soft_since still bounds the cool-down."""
    env = Env(positions=(position("P"),))
    await env.every(T0, T0 + 2000)
    assert env.state(STOP_KEY).level is Level.SOFT_HALT and await env.level() is HaltLevel.SOFT
    closed = T0 + 600_000
    env.kw["positions"] = ()
    await env.at(closed)
    assert env.state(STOP_KEY).resolved_by == "entity_end" and env.state(STOP_KEY).below_soft_since == closed
    await env.at(closed + 1000)
    assert await env.level() is HaltLevel.SOFT              # was NONE 1 s after the close
    await env.at(closed + CFG.sweep.cool_down_ms - 1000)
    assert await env.level() is HaltLevel.SOFT
    await env.at(closed + CFG.sweep.cool_down_ms)
    assert await env.level() is HaltLevel.NONE


async def test_failing_account_gone_from_the_listing_no_longer_holds_the_level() -> None:
    """health-not-pruned: no in-memory health or stored evaluation_failed row survives the account."""
    env = Env()
    env.db.set_snapshot(other_account(T0))
    await env.at(T0)
    env.db.failing_accounts.add(ACCT)
    for t in range(T0 + 1000, T0 + 13_000, 1000):
        env.db.set_snapshot(other_account(t))
        await env.at(t)
    assert await env.level() is HaltLevel.SOFT
    del env.db.base[ACCT]
    gone = T0 + 13_000

    async def full_pass(t: int) -> None:
        env.clock.t = t
        env.db.set_snapshot(other_account(t))
        await env.sweep.run_pass()

    await full_pass(gone)
    assert ACCT not in env.sweep._health
    row = env.db.t.breaches.get(("evaluation", "account", ACCT))
    assert row is None or (not row.active and row.below_soft_since == gone)
    for t in (gone + 1000, gone + CFG.sweep.cool_down_ms - 1000):
        await full_pass(t)
        assert await env.level() is HaltLevel.SOFT
    await full_pass(gone + CFG.sweep.cool_down_ms)
    assert await env.level() is HaltLevel.NONE


async def test_rows_left_by_an_unlisted_account_are_resolved_on_the_first_full_pass() -> None:
    db = InMemoryOverseeDb()
    stale = BreachState("missing_stop_loss", "position", "stonex:OLD", "GONE", Level.SOFT_HALT, active_since=T0 - 1)
    db.t.breaches[stale.key] = stale
    env = Env(db=db)
    await env.at(T0)
    row = db.t.breaches[stale.key]
    assert row.resolved_by == "entity_end" and row.clear_generation == 1 and row.below_soft_since == T0
    assert ("missing_stop_loss", "position", "stonex:OLD", "resolved") in [k[:4] for k in db.t.events]


async def test_repeating_condition_pages_are_latched() -> None:
    """undeduped-repeating-pages: sod_missing, halt_decision_failed and slow_pass page once per condition,
    then every escalate_after_ms, not on every pass."""
    env = Env(account=None, sod=None, positions=(position("P", stop="1.09"),))
    await env.every(T0, T0 + 9000)
    assert [p.code for p in env.pages].count("sod_missing") == 1
    await env.at(T0 + CFG.sweep.escalate_after_ms)
    assert [p.code for p in env.pages].count("sod_missing") == 2

    env2 = Env()

    async def broken() -> Any:
        raise ConnectionError("halt store down")

    env2.halt.status = broken  # type: ignore[method-assign]
    await env2.every(T0, T0 + 9000)
    assert [p.code for p in env2.pages] == ["halt_decision_failed"]

    sweep = env2.sweep
    env2.pages.clear()
    for _ in range(3):
        sweep._check_pass_time(2500, 2500)
    sweep._check_pass_time(200, 3200)                     # a fast pass ends the condition
    sweep._check_pass_time(2000, 6000)
    assert [p.code for p in env2.pages] == ["slow_pass", "slow_pass"]


async def test_k13_partial_session_alert_is_an_outbox_row_of_the_sod_write() -> None:
    """K13 (oversee-rules.md §2 Day roll, §10): the sod_partial_session alert is an outbox row in the same
    AccountWrite as the sod_marks row, keyed (account, trading_day): a failed or CasConflict-retried pass writes
    neither, the committed pass writes both, nothing goes through the direct pager (no in-memory memo), and a row
    rebuilt later carries the same episode key, so the relay's notification_dedupe sends it once."""
    from datetime import datetime

    from src.notifications.relay import OutboxRelay
    from src.oversee.dayroll import NEW_YORK
    from src.oversee.memory import InMemoryOverseeDb
    from src.oversee.notify import RecordingChannel
    from src.oversee.sweep import SOD_PARTIAL_SESSION, AccountWrite, CasConflict
    roll = int(datetime(2026, 11, 11, 17, tzinfo=NEW_YORK).timestamp() * 1000)
    env = Env(sod=None)
    real = env.db.write_account
    fails: list[BaseException] = [RuntimeError("write failed"), CasConflict("moved"), CasConflict("moved")]
    batches: list[AccountWrite] = []

    async def flaky(batch: AccountWrite) -> None:
        batches.append(batch)
        if fails:
            raise fails.pop(0)
        await real(batch)

    def sod_rows() -> list[Any]:
        return [r for r in env.db.t.outbox if r.episode is not None and r.episode[0] == SOD_PARTIAL_SESSION]

    env.db.write_account = flaky        # type: ignore[method-assign]
    await env.at(roll + 31_000)         # write fails: no row, no alert
    await env.at(roll + 32_000)         # CasConflict twice: the pass fails again, still nothing
    assert not env.db.t.sod_marks and not sod_rows()
    # every attempted batch carried the alert beside its sod_mark: they commit together or not at all
    assert all(b.sod_mark is not None and sum(1 for r in b.outbox if r.episode and r.episode[0] == SOD_PARTIAL_SESSION)
               == 1 for b in batches)
    await env.at(roll + 33_000)         # written: the row and its one alert
    sod = next(iter(env.db.t.sod_marks.values()))
    assert sod.partial_session and len(sod_rows()) == 1
    row = sod_rows()[0]
    day = sod.trading_day.isoformat()
    assert row.kind == "episode" and row.severity == "high" and row.account_id == ACCT
    assert row.episode == (SOD_PARTIAL_SESSION, "account", f"{ACCT}:{day}", 0, int(Level.SOFT_HALT))
    assert "sod_partial_session" not in [p.code for p in env.pages]      # not the direct, at-most-once pager
    await env.at(roll + 34_000)         # the committed row is not rebuilt: no second alert
    assert len(sod_rows()) == 1
    env.db.t.sod_marks.clear()          # the row vanishes from the snapshot: rebuilt with the same episode key
    await env.at(roll + 35_000)
    assert len(sod_rows()) == 2 and len({r.episode for r in sod_rows()}) == 1
    # the relay's episode dedupe (notification_dedupe) delivers the (account, trading_day) alert once
    assert isinstance(env.db, InMemoryOverseeDb)
    email = RecordingChannel("email")
    relay = OutboxRelay(env.db, env.db, [email, RecordingChannel("mobile_push")], lease_valid=lambda: True)
    await relay.tick()
    assert [r.title.split()[0] for r in email.sent].count(SOD_PARTIAL_SESSION) == 1
