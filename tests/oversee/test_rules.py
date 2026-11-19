"""Rule plugins and their bands (oversee-rules.md §6), plus the account fence cap (§2)."""
from __future__ import annotations

from decimal import Decimal

from src.dto import OrderStatus
from src.oversee.config import OverseeConfig
from src.oversee.rules import (
    INPUT_FLAG_RULES,
    STALE_ACCOUNT_IMAGE,
    EvalContext,
    MarginUsageRule,
    MaxAggregateOpenRiskRule,
    MaxDailyLossRule,
    MaxPositionRiskRule,
    MissingStopLossRule,
    RuleRegistry,
    run_rule,
)
from src.oversee.snapshot import AccountSnapshot, StreamHealthRow, SymbolQuotes
from src.oversee.types import Cap, Level, Result, Subject, Unknown
from tests.oversee.helpers import ACCT, AD, ROLL, T0, account, flow, live, order, position, quote, snap, sod

D = Decimal
CFG = OverseeConfig()
M = CFG.sweep.release_margin


def ctx(s: AccountSnapshot, now: int = T0) -> EvalContext:
    return EvalContext(s, CFG, now)


def pos_result(stop: str, **kw: object) -> Result | Unknown:
    p = position("P", "BUY", "100000", "1.1000", stop=stop)
    c = ctx(snap(positions=(p,), **kw))
    return run_rule(MaxPositionRiskRule(CFG.max_position_risk, M), c, c.entity_subject(p))


def test_position_risk_bands() -> None:
    r1, r2, r3 = pos_result("1.0900"), pos_result("1.0880"), pos_result("1.0840")
    assert isinstance(r1, Result) and r1.level is Level.PASS and r1.risk_pct == D(1)   # exactly 1R
    assert isinstance(r2, Result) and r2.level is Level.FLAG
    assert isinstance(r3, Result) and r3.level is Level.SOFT_HALT and r3.cap is Cap.NONE


def test_position_risk_release_level_has_margin() -> None:
    r = pos_result("1.0855")   # 1.45 %: FLAG, but within 10 % of the 1.5 % edge
    assert isinstance(r, Result) and r.level is Level.FLAG and r.release_level is Level.SOFT_HALT


def test_position_risk_stale_mark_caps() -> None:
    r = pos_result("1.0840", quotes={(AD, "EURUSD"): SymbolQuotes(latest=quote("1.1", T0 - 9000).latest)})
    assert isinstance(r, Result) and r.cap is Cap.SOFT and r.effective_level is Level.SOFT_HALT


def test_position_risk_unknown_without_spec() -> None:
    assert isinstance(pos_result("1.0840", instruments={}), Unknown)


def test_order_uses_ordered_units_for_whole_trade() -> None:
    o = order("O", "BUY", OrderStatus.PARTIALLY_FILLED, ordered="100000", remaining="10000", stop="1.0840")
    c = ctx(snap(orders=(o,)))
    r = run_rule(MaxPositionRiskRule(CFG.max_position_risk, M), c, c.entity_subject(o))
    assert isinstance(r, Result) and r.risk_pct == D("1.6")


def test_aggregate_bands_and_pending_excluded() -> None:
    p1 = position("P1", stop="1.0900")          # 1.0 %
    w = order("W", "BUY", OrderStatus.WORKING, stop="1.0880")    # 1.2 %
    pend = order("X", "BUY", OrderStatus.PENDING, stop="1.0000")  # excluded
    rule = MaxAggregateOpenRiskRule(CFG.max_aggregate_open_risk, M)
    c = ctx(snap(positions=(p1,), orders=(w, pend)))
    r = run_rule(rule, c, c.account_subject())
    assert isinstance(r, Result) and r.risk_pct == D("2.2") and r.level is Level.FLAG
    c2 = ctx(snap(positions=(p1, position("P2", stop="1.0780"))))
    r2 = run_rule(rule, c2, c2.account_subject())
    assert isinstance(r2, Result) and r2.level is Level.SOFT_HALT and r2.risk_pct == D("3.2")


def test_aggregate_unknown_leg_makes_unknown() -> None:
    c = ctx(snap(positions=(position("P1", stop="1.09"), position("P2", symbol="GBPUSD", stop="1.2"))))
    r = run_rule(MaxAggregateOpenRiskRule(CFG.max_aggregate_open_risk, M), c, c.account_subject())
    assert isinstance(r, Unknown) and r.reason == "risk_unknown"


def test_missing_stop_grace() -> None:
    p = position("P")
    rule = MissingStopLossRule(CFG.missing_stop_loss)
    key = ("position", p.entity_key)
    for since, expected in ((None, Level.PASS), (T0 - 1999, Level.PASS), (T0 - 2000, Level.SOFT_HALT)):
        c = ctx(snap(positions=(p,), stop_watch={key: since} if since else {}))
        r = run_rule(rule, c, c.entity_subject(p))
        assert isinstance(r, Result) and r.level is expected
    c = ctx(snap(positions=(position("P", stop="1.09"),), stop_watch={key: T0 - 10_000}))
    r = run_rule(rule, c, c.entity_subject(p))
    assert isinstance(r, Result) and r.level is Level.PASS


def daily(**kw: object) -> Result | Unknown:
    c = ctx(snap(**kw))
    rule = MaxDailyLossRule(CFG.max_daily_loss, M, CFG.sweep.skew_ms)
    return run_rule(rule, c, rule.subjects(c)[0])


def test_daily_loss_hard_and_subject_is_account_day() -> None:
    c = ctx(snap())
    rule = MaxDailyLossRule(CFG.max_daily_loss, M, CFG.sweep.skew_ms)
    assert rule.subjects(c)[0].subject_type == "account_day" and rule.subjects(c)[0].subject_id.startswith(ACCT)
    r = daily(positions=(position("P", "BUY", "100000", "1.1350"),))     # -3.5 %
    assert isinstance(r, Result) and r.level is Level.HARD_HALT and r.cap is Cap.NONE
    r = daily(positions=(position("P", "BUY", "100000", "1.1250"),))     # -2.5 %
    assert isinstance(r, Result) and r.level is Level.PASS


def test_daily_loss_caps() -> None:
    losing = (position("P", "BUY", "100000", "1.1350"),)
    cases = [dict(sod=sod(partial=True)),
             dict(last_status_or_units_change_ms=T0 - 1000, balance_committed_at_ms=T0 - 2000),
             dict(quotes={(AD, "EURUSD"): SymbolQuotes(latest=quote("1.1", T0 - 9000).latest)})]
    for extra in cases:
        r = daily(positions=losing, **extra)
        assert isinstance(r, Result) and r.cap is Cap.SOFT and r.effective_level is Level.SOFT_HALT, extra
    r = daily(positions=losing, last_status_or_units_change_ms=T0 - 6000, balance_committed_at_ms=T0 - 7000)
    assert isinstance(r, Result) and r.cap is Cap.NONE   # skew window (5 s) has passed


def test_daily_loss_unknown_without_sod() -> None:
    assert isinstance(daily(sod=None), Unknown)


def test_daily_loss_unexplained_increase_flags_and_decrease_requests_cash_flows() -> None:
    r = daily(account=account("101000"))
    assert isinstance(r, Result) and r.level is Level.FLAG and r.reason == "unexplained_balance_delta"
    r = daily(account=account("98000"))
    assert isinstance(r, Result) and r.wants_cash_flows and r.level is Level.PASS


def test_daily_loss_late_pre_roll_deposit_does_not_hide_a_loss() -> None:
    """10 000 deposited just before the roll, unexplained at the roll (excluded from sod_equity)."""
    s_row = sod("110000", "100000", uiar="10000", written=ROLL + 1000)
    losing = (position("P", "BUY", "100000", "1.1350"),)    # -3 500
    before = daily(sod=s_row, account=account("110000"), positions=losing)
    late = flow("dep", "DEPOSIT", "10000", ROLL - 60_000, committed=T0 - 1000)
    after = daily(sod=s_row, account=account("110000"), positions=losing, cash_flows=(late,))
    for r in (before, after):
        assert isinstance(r, Result) and r.level is Level.HARD_HALT and r.risk_pct == D("-3.5")


def test_daily_loss_equity_divergence_flag() -> None:
    r = daily(account_live=live("100", equity="90000"))
    assert isinstance(r, Result) and r.level is Level.FLAG and r.reason == "equity_divergence"


def test_margin_usage_bands_and_unknown() -> None:
    rule = MarginUsageRule(CFG.margin_usage, M)
    for margin, level in (("79000", Level.PASS), ("85000", Level.FLAG), ("96000", Level.SOFT_HALT)):
        c = ctx(snap(account_live=live(margin)))
        r = run_rule(rule, c, c.account_subject())
        assert isinstance(r, Result) and r.level is level
    for lv in (None, live("1000", sampled=True), live(None)):
        c = ctx(snap(account_live=lv))
        r = run_rule(rule, c, c.account_subject())
        assert isinstance(r, Unknown) and r.reason == "live_unknown"


def test_account_fence_caps_everything_and_flags() -> None:
    lost = (StreamHealthRow(AD, "orders", T0 - 11_000),)
    reg = RuleRegistry.from_config(CFG)
    evals = reg.evaluate(ctx(snap(positions=(position("P", stop="1.0840"),), stream_health=lost)))
    img = evals[(STALE_ACCOUNT_IMAGE, "account", ACCT)].outcome
    assert isinstance(img, Result) and img.level is Level.FLAG
    pos = next(e for k, e in evals.items() if k[0] == "max_position_risk")
    assert isinstance(pos.outcome, Result) and pos.outcome.cap is Cap.SOFT
    recent = (StreamHealthRow(AD, "orders", T0 - 5_000), StreamHealthRow("other", "orders", T0 - 60_000))
    evals = reg.evaluate(ctx(snap(stream_health=recent)))
    img = evals[(STALE_ACCOUNT_IMAGE, "account", ACCT)].outcome
    assert isinstance(img, Result) and img.level is Level.PASS


def test_integrity_incident_caps() -> None:
    evals = RuleRegistry.from_config(CFG).evaluate(ctx(snap(integrity_incident=True)))
    assert all(e.outcome.cap is Cap.SOFT for k, e in evals.items()
               if isinstance(e.outcome, Result) and k[0] not in INPUT_FLAG_RULES)


def test_rule_error_is_unknown() -> None:
    class Boom:
        rule_id, confirm_ms = "boom", 0

        def subjects(self, c: EvalContext) -> list[Subject]:
            return [c.account_subject()]

        def evaluate(self, c: EvalContext, s: Subject) -> Result:
            raise ZeroDivisionError("x")

    c = ctx(snap())
    r = run_rule(Boom(), c, c.account_subject())
    assert isinstance(r, Unknown) and "rule_error" in r.detail


def test_registry_respects_enabled_flags() -> None:
    from dataclasses import replace
    cfg = replace(CFG, margin_usage=replace(CFG.margin_usage, enabled=False))
    assert "margin_usage" not in RuleRegistry.from_config(cfg).rule_ids
    assert {"max_position_risk", "max_daily_loss", STALE_ACCOUNT_IMAGE} <= RuleRegistry.from_config(CFG).rule_ids


def test_k12_daily_loss_caps_only_for_the_listed_reasons() -> None:
    """K12 (oversee-rules.md §2, §6): SOFT cap only for partial_session, stale inputs and the balance skew window
    (plus the registry's fence / integrity cap). An outstanding cash-flow request (an unexplained decrease asks the
    orchestrator for fetch_cash_flows) does not cap: a real loss is HARD while the request is still in flight."""
    rule = MaxDailyLossRule(CFG.max_daily_loss, M, CFG.sweep.skew_ms)
    losing = (position("P", "BUY", "100000", "1.1350"),)                 # -3.5 %, plus a balance decrease
    c = ctx(snap(positions=losing, account=account("99000")))
    assert rule._cap_reasons(c, frozenset()) == []
    r = run_rule(rule, c, rule.subjects(c)[0])
    assert isinstance(r, Result) and r.level is Level.HARD_HALT and r.cap is Cap.NONE and r.wants_cash_flows
    assert "cash_flow_request_pending" not in AccountSnapshot.__dataclass_fields__


def test_k11_unexplained_increase_stays_excluded_while_pnl_unverified() -> None:
    """K11 (oversee-rules.md §5): on a pnl_unverified day (a partial cash-flow fetch: it can drop DEPOSIT rows as
    well as TRADE_PNL rows) today's unexplained increase stays excluded from equity_mark, whatever transfer_source
    says, so a deposit missing from a truncated page can never offset a real loss (fail closed); the FLAG
    pnl_unverified reports the day."""
    plus = dict(account=account("101000"), positions=())               # +1 000 with no ledger row
    verified = daily(**plus)
    assert isinstance(verified, Result) and verified.level is Level.FLAG
    assert verified.reason == "unexplained_balance_delta" and verified.risk_pct == D("0")
    rule = MaxDailyLossRule(CFG.max_daily_loss, M, CFG.sweep.skew_ms)
    for transfers in (True, False):
        c = ctx(snap(pnl_source=False, transfer_source=transfers, **plus))
        eq = c.equity()
        assert eq.unexplained_delta == D("1000") and eq.excluded_increase == eq.unexplained_delta
        assert eq.value == D("100000")
        assert "pnl_unverified" in rule._flags(c, eq)                     # the day's FLAG is raised
        unverified = daily(pnl_source=False, transfer_source=transfers, **plus)
        assert isinstance(unverified, Result) and unverified.level is Level.FLAG and unverified.risk_pct == D("0")
    # A missing deposit cannot hide a real loss: -4 % on the position and +1 000 unexplained is still HARD_HALT
    hidden = daily(pnl_source=False, account=account("101000"),
                   positions=(position("P", "BUY", "100000", "1.1400"),))
    assert isinstance(hidden, Result) and hidden.level is Level.HARD_HALT
    # A decrease always counts as loss, whatever the flags.
    loss = daily(pnl_source=False, account=account("96000"), positions=())
    assert isinstance(loss, Result) and loss.level is Level.HARD_HALT and loss.risk_pct == D("-4")
