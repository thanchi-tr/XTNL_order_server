"""IMPLEMENTATION_PLAN 6.3 Oversee: FLAG stale_mark (oversee-rules.md §5), dependent-alert inhibition during
maintenance and after an epoch change (operations.md §9, §10), an ack silences evaluation_failed re-pages (§4),
a durable quarantine row (§2), and the per-symbol leaderless sod check (tier-persistence.md §4). Clock injected;
in-memory store."""
from __future__ import annotations

from dataclasses import replace
from typing import Any

from src.oversee.config import OverseeConfig
from src.oversee.dayroll import build_sod_mark
from src.oversee.inhibit import ESCALATION_CODE, SUMMARY_CODE, AlertInhibitor
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.rules import STALE_ACCOUNT_IMAGE, STALE_MARK, EvalContext, RuleRegistry, StaleMarkRule, run_rule
from src.oversee.snapshot import AccountSnapshot, StreamHealthRow, SymbolQuotes
from src.oversee.sweep import AccountWrite, CasConflict, RiskSweep
from src.oversee.types import Cap, Level, Result
from tests.oversee.helpers import ACCT, AD, ROLL, T0, account, clean_mark, position, quote, snap, spec
from tests.oversee.test_sweep import CFG, Env

FULL = OverseeConfig()
MARK_MS = FULL.marks.stale_mark_ms
EVAL_KEY = ("evaluation", "account", ACCT)
ESC = CFG.sweep.escalate_after_ms
STALE_KEY = (STALE_MARK, "account", ACCT)
IMAGE_KEY = (STALE_ACCOUNT_IMAGE, "account", ACCT)
EUR = (AD, "EURUSD")


def stale_mark(s: AccountSnapshot, now: int = T0) -> Result:
    c = EvalContext(s, FULL, now)
    r = run_rule(StaleMarkRule(), c, c.account_subject())
    assert isinstance(r, Result)
    return r


def frozen(mid: str = "1.1000", age: int = MARK_MS + 1, symbol: str = "EURUSD") -> SymbolQuotes:
    return quote(mid, T0 - age, symbol=symbol)


# ---------------------------------------------------------------- FLAG stale_mark (§5 stale cases 2 and 3)
def test_a_frozen_feed_while_open_flags_stale_mark() -> None:
    r = stale_mark(snap(positions=(position("P", stop="1.09"),), quotes={EUR: frozen()}))
    assert (r.level, r.reason, r.cap, r.stale_symbols) == (Level.FLAG, STALE_MARK, Cap.NONE, frozenset({EUR}))


def test_closed_and_roll_break_flag_stale_mark() -> None:
    for session in ("CLOSED", "ROLLOVER"):
        q = replace(quote("1.1", T0 - 60_000), session=session)
        r = stale_mark(snap(positions=(position("P", stop="1.09"),), quotes={EUR: q}))
        assert r.level is Level.FLAG and r.stale_symbols == frozenset({EUR}), session


def test_fresh_or_ticking_unclean_marks_do_not_flag() -> None:
    held = (position("P", stop="1.09"),)
    assert stale_mark(snap(positions=held)).level is Level.PASS                       # fresh clean mid
    ticking = quote("1.1", T0, clean=False)                                            # liquidation side: capped only
    assert stale_mark(snap(positions=held, quotes={EUR: ticking})).level is Level.PASS
    assert stale_mark(snap(quotes={EUR: frozen()})).level is Level.PASS                # nothing held
    assert stale_mark(snap(positions=held, quotes={})).level is Level.PASS             # no mid ever: UNKNOWN elsewhere


def test_a_frozen_cross_that_converts_pnl_flags_too() -> None:
    gbp = (AD, "GBPUSD")
    s = snap(account=account(ccy="GBP"), positions=(position("P", stop="1.09"),),
             instruments={EUR: spec(), gbp: spec("GBPUSD", "GBP", "USD")},
             quotes={EUR: quote("1.1", T0), gbp: frozen("1.3", symbol="GBPUSD")})
    assert stale_mark(s).stale_symbols == frozenset({gbp})


def test_without_redis_only_closed_symbols_flag() -> None:
    held = (position("P", stop="1.09"),)
    fallback = SymbolQuotes(last_clean=clean_mark("1.1", T0 - 60_000))
    assert stale_mark(snap(positions=held, redis_ok=False, quotes={EUR: fallback})).level is Level.PASS
    closed = replace(fallback, session="CLOSED")
    assert stale_mark(snap(positions=held, redis_ok=False, quotes={EUR: closed})).level is Level.FLAG


def test_registry_runs_stale_mark_and_the_fence_does_not_cap_it() -> None:
    reg = RuleRegistry.from_config(FULL)
    assert STALE_MARK in reg.rule_ids
    lost = (StreamHealthRow(AD, "orders", T0 - 60_000),)
    evals = reg.evaluate(EvalContext(snap(positions=(position("P", stop="1.09"),), quotes={EUR: frozen()},
                                          stream_health=lost), FULL, T0))
    out = evals[STALE_KEY].outcome
    assert isinstance(out, Result) and out.level is Level.FLAG and out.cap is Cap.NONE


async def test_sweep_records_the_stale_mark_flag_state_with_one_email() -> None:
    env = Env(positions=(position("P", stop="1.0990"),))
    await env.at(T0)
    assert STALE_KEY not in env.db.t.breaches or not env.state(STALE_KEY).active
    env.stale_quotes = True
    await env.at(T0 + MARK_MS + 1_000)
    st = env.state(STALE_KEY)
    assert st.active and st.level is Level.FLAG and st.reason == STALE_MARK
    assert env.events(STALE_MARK) == ["activated"]
    assert [r for r in env.db.t.outbox if STALE_MARK in r.title and r.kind == "episode" and r.severity == "info"]


# ---------------------------------------------------------------- dependent-alert inhibition (§9, §10)
def with_inhibitor(env: Env, inhibitor: AlertInhibitor) -> Env:
    env.sweep = RiskSweep(reader=env.db, writer=env.db, registry=RuleRegistry.from_config(CFG), halt=env.halt,
                          config=CFG, clock=env.clock, epoch=lambda: env.db.engine_epoch, pager=env.pages.append,
                          inhibitor=inhibitor)
    return env


def outbox_of(env: Env, rule: str) -> list[str]:
    return [r.title for r in env.db.t.outbox if rule in r.title]


LOST = (StreamHealthRow(AD, "orders", T0 - 11_000),)


async def test_epoch_change_holds_back_fence_alerts_then_escalates_once_if_still_fenced() -> None:
    env = with_inhibitor(Env(stream_health=LOST, positions=(position("P1", stop="1.0840"),
                                                             position("P2", stop="1.0840"))),
                         AlertInhibitor(epoch_changed_at_ms=lambda: T0))
    await env.at(T0)
    await env.at(T0 + 1_000)
    assert env.state(IMAGE_KEY).active and env.events(STALE_ACCOUNT_IMAGE) == ["activated"]   # still audited
    assert outbox_of(env, STALE_ACCOUNT_IMAGE) == []                                            # alert held back
    agg = [t for t in outbox_of(env, "max_aggregate_open_risk")]
    assert agg and agg[0].startswith("SOFT_HALT")          # risk alerts are never held back, even when capped
    await env.at(T0 + 119_000)
    assert [p.code for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)] == []
    await env.at(T0 + 120_000)
    await env.at(T0 + 121_000)
    pages = [p for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)]
    assert [(p.code, p.severity) for p in pages] == [(ESCALATION_CODE, "high")]
    assert ACCT in pages[0].detail
    env.kw["stream_health"] = ()
    await env.at(T0 + 122_000)
    await env.at(T0 + 128_000)                            # after the window the resolution alerts normally
    assert any(t.startswith("resolved") for t in outbox_of(env, STALE_ACCOUNT_IMAGE))


async def test_a_fence_that_clears_inside_the_window_sends_one_info_summary() -> None:
    env = with_inhibitor(Env(stream_health=LOST), AlertInhibitor(epoch_changed_at_ms=lambda: T0))
    await env.at(T0)
    env.kw["stream_health"] = ()
    await env.at(T0 + 30_000)
    await env.at(T0 + 36_000)                             # released after release_ms
    assert env.events(STALE_ACCOUNT_IMAGE) == ["activated", "resolved"] and outbox_of(env, STALE_ACCOUNT_IMAGE) == []
    await env.at(T0 + 120_000)
    await env.at(T0 + 121_000)
    pages = [p for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)]
    assert [(p.code, p.severity) for p in pages] == [(SUMMARY_CODE, "info")]


async def test_maintenance_holds_back_the_adapters_fence_alerts_only_while_it_lasts() -> None:
    maint = {AD: True}
    env = with_inhibitor(Env(stream_health=LOST), AlertInhibitor(in_maintenance=lambda a: maint.get(a, False)))
    await env.at(T0)
    assert env.state(IMAGE_KEY).active and outbox_of(env, STALE_ACCOUNT_IMAGE) == []
    await env.at(T0 + 600_000)
    assert [p.code for p in env.pages if p.code == ESCALATION_CODE] == []
    maint[AD] = False                    # 7.2: the window ends -> FENCE at full severity at once (no settle)
    await env.at(T0 + 601_000)
    pages = [p for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)]
    assert [(p.code, p.severity) for p in pages] == [(SUMMARY_CODE, "info")]   # the reconnect is expected
    assert "still fenced after the maintenance window" in pages[0].detail and ACCT in pages[0].detail
    assert pages[0].extra == {"accounts": ACCT}


async def test_a_fence_that_clears_after_maintenance_alerts_its_resolution_at_full_severity() -> None:
    maint = {AD: True}
    env = with_inhibitor(Env(stream_health=LOST), AlertInhibitor(in_maintenance=lambda a: maint.get(a, False)))
    await env.at(T0)
    maint[AD] = False                                      # the window ends; the stream is back 10 s later
    await env.at(T0 + 1_000)
    env.kw["stream_health"] = ()
    await env.every(T0 + 10_000, T0 + 20_000)              # resolved after release_ms: no longer held back
    assert env.events(STALE_ACCOUNT_IMAGE) == ["activated", "resolved"]
    assert [t.split()[0] for t in outbox_of(env, STALE_ACCOUNT_IMAGE)] == ["resolved"]
    pages = [p for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)]
    assert [(p.code, p.severity) for p in pages] == [(SUMMARY_CODE, "info")]


async def test_a_still_active_stale_mark_is_named_in_the_summary_not_escalated() -> None:
    # A weekend takeover: the fence clears inside RTO + 30 s, the closed market keeps stale_mark active (info).
    closed = replace(quote("1.1000", T0 - 60_000), session="CLOSED")
    env = with_inhibitor(Env(stream_health=LOST, positions=(position("P", stop="1.0990"),)),
                         AlertInhibitor(epoch_changed_at_ms=lambda: T0))
    env.snapshot = lambda t: snap(now=t, **({"quotes": {EUR: closed}} | env.kw))   # type: ignore[method-assign]
    await env.at(T0)
    assert env.state(STALE_KEY).active and outbox_of(env, STALE_MARK) == []
    env.kw["stream_health"] = ()
    await env.every(T0 + 1_000, T0 + 10_000)
    await env.at(T0 + 120_000)
    pages = [p for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)]
    assert [(p.code, p.severity) for p in pages] == [(SUMMARY_CODE, "info")]
    assert f"stale_mark still active: {ACCT}" in pages[0].detail


async def test_held_alerts_are_counted_once_per_written_pass() -> None:
    class Racy(InMemoryOverseeDb):
        conflicts = 1
        broken = False

        async def write_account(self, batch: AccountWrite) -> None:
            if self.broken:
                raise ConnectionError("write lost")
            if self.conflicts and batch.events:
                self.conflicts -= 1
                raise CasConflict("moved")
            await super().write_account(batch)

    db = Racy()
    env = with_inhibitor(Env(db=db, stream_health=LOST), AlertInhibitor(epoch_changed_at_ms=lambda: T0))
    await env.at(T0)                                      # first attempt loses its CAS, the retry is written
    assert env.events(STALE_ACCOUNT_IMAGE) == ["activated"]
    db.broken = True
    env.kw["stream_health"] = ()
    await env.every(T0 + 1_000, T0 + 7_000)               # passes whose write fails hold nothing back
    db.broken = False
    await env.at(T0 + 120_000)
    pages = [p for p in env.pages if p.code in (SUMMARY_CODE, ESCALATION_CODE)]
    assert len(pages) == 1 and pages[0].detail.startswith("1 stale/FENCE alerts held back")


async def test_without_suppression_fence_alerts_go_out_and_a_failing_hook_never_suppresses() -> None:
    def broken(adapter: str) -> bool:
        raise RuntimeError("calendar gone")

    env = with_inhibitor(Env(stream_health=LOST), AlertInhibitor(in_maintenance=broken,
                                                               epoch_changed_at_ms=lambda: T0 - 120_000))
    await env.at(T0)
    assert outbox_of(env, STALE_ACCOUNT_IMAGE)


# ---------------------------------------------------------------- ack silences evaluation_failed re-pages (§4)
def eval_pages(env: Env) -> int:
    return [p.code for p in env.pages].count("evaluation_failed")


async def test_an_ack_on_the_evaluation_failed_state_stops_its_repages() -> None:
    env = Env()
    await env.at(T0)
    env.db.failing_accounts.add(ACCT)
    first = T0 + CFG.sweep.evaluation_failed_after_ms
    await env.at(first)
    assert eval_pages(env) == 1 and env.state(EVAL_KEY).active
    await env.halt.acknowledge("op", "known outage", EVAL_KEY, first + 1_000)
    await env.at(first + ESC)
    await env.at(first + 2 * ESC)
    assert eval_pages(env) == 1                            # silenced while level, cap and reason stay the same
    assert "oversight_lapsed" in [p.code for p in env.pages]   # the quarantine page is not a re-page


async def test_an_ack_for_another_reason_does_not_silence_and_an_unreadable_ack_still_pages() -> None:
    env = Env()
    await env.at(T0)
    env.db.failing_accounts.add(ACCT)
    first = T0 + CFG.sweep.evaluation_failed_after_ms
    await env.at(first)
    ack = await env.halt.acknowledge("op", "x", EVAL_KEY, first + 1_000)
    env.db.t.acks[-1] = replace(ack, reason="risk_unknown")
    await env.at(first + ESC)
    assert eval_pages(env) == 2

    async def boom(keys: Any) -> Any:
        raise ConnectionError("acks unreadable")

    env.db.t.acks[-1] = ack
    env.halt.acks_for = boom   # type: ignore[method-assign]
    await env.at(first + 2 * ESC)
    assert eval_pages(env) == 3


# ---------------------------------------------------------------- durable quarantine (§2)
async def test_a_failed_quarantine_write_pages_and_is_retried_until_the_row_exists() -> None:
    class Flaky(InMemoryOverseeDb):
        fail = 2

        async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
            if at_ms is not None and self.fail:
                self.fail -= 1
                raise ConnectionError("quarantine write lost")
            await super().set_quarantine(account_id, at_ms, reason)

    db = Flaky()
    env = Env(db=db)
    await env.at(T0)
    db.failing_accounts.add(ACCT)
    q_at = T0 + CFG.sweep.quarantine_after_ms
    await env.at(q_at)
    codes = [p.code for p in env.pages]
    assert "oversight_lapsed" in codes and ACCT not in db.t.quarantine
    lapsed = [p for p in env.pages if p.code == "quarantine_unwritten"]
    assert len(lapsed) == 1 and lapsed[0].severity == "critical"
    await env.at(q_at + 1_000)          # skipped (half-open retry is 10 min away), but the row write is retried
    assert db.t.quarantine.get(ACCT) == q_at
    assert len([p for p in env.pages if p.code == "quarantine_unwritten"]) == 1


async def test_a_released_account_whose_next_quarantine_row_fails_pages_at_once() -> None:
    class Down(InMemoryOverseeDb):
        down = True

        async def set_quarantine(self, account_id: str, at_ms: int | None, reason: str) -> None:
            if at_ms is not None and self.down:
                raise ConnectionError("quarantine write lost")
            await super().set_quarantine(account_id, at_ms, reason)

    db = Down()
    env = Env(db=db)
    await env.at(T0)
    db.failing_accounts.add(ACCT)
    q_at = T0 + CFG.sweep.quarantine_after_ms
    await env.at(q_at)
    env.sweep.release_account("op", ACCT)              # well within escalate_after_ms of the first page
    db.failing_accounts.discard(ACCT)
    await env.at(q_at + 1_000)                          # good pass: released
    db.failing_accounts.add(ACCT)
    await env.at(q_at + 2_000)
    await env.at(q_at + 2_000 + CFG.sweep.quarantine_after_ms)
    assert q_at + 2_000 + CFG.sweep.quarantine_after_ms - q_at < CFG.sweep.escalate_after_ms
    assert len([p for p in env.pages if p.code == "quarantine_unwritten"]) == 2


# ---------------------------------------------------------------- leaderless per held symbol (tier-persistence §4)
def roll_snap(**kw: Any) -> AccountSnapshot:
    base: dict[str, Any] = dict(positions=(position("P", stop="1.09"),),
                                quotes={EUR: SymbolQuotes(last_clean=clean_mark("1.1", ROLL - 3_600_000 - 60_000))},
                                scheduled_close_before_roll_ms={EUR: ROLL - 3_600_000}, sod=None)
    base.update(kw)
    return snap(now=ROLL + 1_000, **base)


def reasons(s: AccountSnapshot) -> tuple[str, ...]:
    return build_sod_mark(s, ROLL, ROLL + 1_000, FULL).partial_reasons


def test_leaderless_is_measured_from_each_held_symbols_reference_point() -> None:
    # A leader ran in the 5 min before the roll but not before the symbol's close: leaderless for that symbol.
    assert "leaderless:EURUSD" in reasons(roll_snap(leader_terms=((ROLL - 240_000, None),)))
    # A leader covered the close, then none before the roll: the closed symbol's mark is good.
    covered = reasons(roll_snap(leader_terms=((ROLL - 4 * 3_600_000, ROLL - 3_000_000),)))
    assert not any(r.startswith("leaderless") for r in covered)
    # A symbol open at the roll uses the roll itself.
    open_at_roll = roll_snap(scheduled_close_before_roll_ms={},
                             quotes={EUR: SymbolQuotes(last_clean=clean_mark("1.1", ROLL - 1_000))},
                             leader_terms=((ROLL - 4 * 3_600_000, ROLL - 3_000_000),))
    assert "leaderless:EURUSD" in reasons(open_at_roll)


def test_a_flat_account_is_never_leaderless_and_the_account_wide_flag_is_the_fallback() -> None:
    assert reasons(roll_snap(positions=(), leader_terms=())) == ()
    assert "leaderless" in reasons(roll_snap(leader_terms=None, leaderless_before_roll=True))
