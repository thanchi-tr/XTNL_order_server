"""Rule plugins (Strategy) and the RuleRegistry (oversee-rules.md §6).

Rules are pure: `evaluate(ctx, subject)` reads only the `EvalContext` (one account snapshot plus the
values derived from it once per pass, such as equity_mark) and returns a Result or Unknown.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from functools import cached_property
from typing import Protocol

from src.dto import OrderDTO, OrderStatus, PositionDTO
from src.oversee.config import BandRuleConfig, DailyLossConfig, MissingStopConfig, OverseeConfig
from src.oversee.dayroll import latest_roll_ms, trading_day_for_roll
from src.oversee.risk import (
    HUNDRED,
    ZERO,
    EquityMark,
    MarkBook,
    MarkQuality,
    RiskUnknown,
    effective_stop,
    equity_mark,
    needs_stop,
    net_transfers_today,
    opening_units,
    pct_of,
    risk,
)
from src.oversee.snapshot import AccountSnapshot
from src.oversee.types import Cap, Level, Result, RuleOutcome, StateKey, Subject, SubjectType, SymbolKey, Unknown

STALE_ACCOUNT_IMAGE = "stale_account_image"
STALE_MARK = "stale_mark"
# Built-in FLAG rules about the inputs themselves: never capped by the account fence (a capped FLAG would only
# add capped / uncapped transitions), and their alerts are the dependent ones the AlertInhibitor holds back.
INPUT_FLAG_RULES = frozenset({STALE_ACCOUNT_IMAGE, STALE_MARK})


class EvalContext:
    """One pass's view of one account: the snapshot, `now`, the mark book and cached derived values."""

    def __init__(self, snap: AccountSnapshot, cfg: OverseeConfig, now_ms: int) -> None:
        self.snap = snap
        self.cfg = cfg
        self.now_ms = now_ms
        self.book = MarkBook(snap.quotes, snap.instruments, now_ms, cfg.marks.stale_mark_ms)
        self.roll_ms = snap.sod.roll_ms if snap.sod is not None else latest_roll_ms(now_ms)

    @property
    def account_ccy(self) -> str:
        if self.snap.account is None:
            raise RiskUnknown("risk_unknown", "no account row")
        return self.snap.account.currency

    @cached_property
    def _equity(self) -> EquityMark | RiskUnknown:
        try:
            if self.snap.account is None:
                raise RiskUnknown("risk_unknown", "no account row")
            return equity_mark(self.snap.account.balance, self.snap.account.currency, self.snap.positions,
                               self.book, self.snap.cash_flows, self.snap.sod)
        except RiskUnknown as exc:
            return exc

    def equity(self) -> EquityMark:
        eq = self._equity
        if isinstance(eq, RiskUnknown):
            raise eq
        return eq

    @cached_property
    def fenced(self) -> bool:
        """Stale account image (§2): any stream of the adapter lost > ACCOUNT_FENCE_TIMEOUT_MS, or SM3."""
        if self.snap.integrity_incident:
            return True
        timeout = self.cfg.sweep.account_fence_timeout_ms
        return any(h.adapter_id == self.snap.adapter_id and h.conn_lost_at_ms is not None
                   and self.now_ms - h.conn_lost_at_ms > timeout for h in self.snap.stream_health)

    @property
    def trading_day_subject_id(self) -> str:
        return f"{self.snap.account_id}:{trading_day_for_roll(self.roll_ms).isoformat()}"

    def account_subject(self) -> Subject:
        return Subject("account", self.snap.account_id, self.snap.account_id)

    def entity_subject(self, e: OrderDTO | PositionDTO) -> Subject:
        kind: SubjectType = "position" if isinstance(e, PositionDTO) else "order"
        return Subject(kind, e.entity_key, self.snap.account_id)

    def entity(self, subject: Subject) -> OrderDTO | PositionDTO | None:
        pool: Sequence[OrderDTO | PositionDTO] = (
            self.snap.positions if subject.subject_type == "position" else self.snap.orders)
        return next((e for e in pool if e.entity_key == subject.subject_id), None)


class Rule(Protocol):
    @property
    def rule_id(self) -> str: ...

    @property
    def confirm_ms(self) -> int: ...

    def subjects(self, ctx: EvalContext) -> list[Subject]: ...

    def evaluate(self, ctx: EvalContext, subject: Subject) -> RuleOutcome: ...


def band_level(value: Decimal, flag_above: Decimal, halt_above: Decimal, halt_level: Level) -> Level:
    if value > halt_above:
        return halt_level
    if value > flag_above:
        return Level.FLAG
    return Level.PASS


def band_result(value_pct: Decimal, cfg: BandRuleConfig, margin: Decimal, reason: str,
                stale: frozenset[SymbolKey]) -> Result:
    """Level from the bands; release level from the bands lowered by the release margin (§2)."""
    k = 1 - margin
    level = band_level(value_pct, cfg.flag_above_pct, cfg.halt_above_pct, Level.SOFT_HALT)
    release = band_level(value_pct, cfg.flag_above_pct * k, cfg.halt_above_pct * k, Level.SOFT_HALT)
    return Result(level, reason, Cap.SOFT if stale else Cap.NONE, value_pct, release, stale)


def _live_entities(ctx: EvalContext) -> list[OrderDTO | PositionDTO]:
    out: list[OrderDTO | PositionDTO] = [o for o in ctx.snap.orders if not o.status.terminal]
    out += [p for p in ctx.snap.positions if p.status == "OPEN"]
    return out


@dataclass
class MaxPositionRiskRule:
    """Each non-terminal order (ordered_units; opening_units if reduce-only) and OPEN position:
    <= 1.0 % PASS, <= 1.5 % FLAG, > 1.5 % SOFT_HALT of equity_mark (§6)."""

    cfg: BandRuleConfig
    release_margin: Decimal
    rule_id: str = "max_position_risk"

    @property
    def confirm_ms(self) -> int:
        return self.cfg.confirm_ms

    def subjects(self, ctx: EvalContext) -> list[Subject]:
        return [ctx.entity_subject(e) for e in _live_entities(ctx)]

    def evaluate(self, ctx: EvalContext, subject: Subject) -> RuleOutcome:
        e = ctx.entity(subject)
        if e is None:
            return Unknown("risk_unknown", "entity missing")
        snap = ctx.snap
        if isinstance(e, OrderDTO):
            if e.instrument_unknown or e.ordered_units is None:
                raise RiskUnknown("risk_unknown", "instrument_unknown")
            units = opening_units(e, snap.positions, snap.orders) if e.is_reduce_only else e.ordered_units
        else:
            if e.units is None:
                raise RiskUnknown("risk_unknown", "instrument_unknown")
            units = e.units
        fig = risk(e, units, ctx.book, ctx.account_ccy, snap.positions, snap.orders)
        eq = ctx.equity()
        stale = fig.stale | eq.stale
        if fig.no_stop:
            return Result(Level.PASS, "no_stop", Cap.SOFT if stale else Cap.NONE, None, None, stale)
        return band_result(pct_of(fig.value, eq.value), self.cfg, self.release_margin, "position_risk", stale)


@dataclass
class MaxAggregateOpenRiskRule:
    """sum(risk) over opening_units of WORKING / PARTIALLY_FILLED orders plus OPEN positions, no netting;
    > 2.0 % FLAG, > 3.0 % SOFT_HALT; any UNKNOWN leg makes the result UNKNOWN (§6)."""

    cfg: BandRuleConfig
    release_margin: Decimal
    rule_id: str = "max_aggregate_open_risk"

    @property
    def confirm_ms(self) -> int:
        return self.cfg.confirm_ms

    def subjects(self, ctx: EvalContext) -> list[Subject]:
        return [ctx.account_subject()]

    def evaluate(self, ctx: EvalContext, subject: Subject) -> RuleOutcome:
        snap = ctx.snap
        total = ZERO
        stale: frozenset[SymbolKey] = frozenset()
        for o in snap.orders:
            if o.status in (OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED):
                units = opening_units(o, snap.positions, snap.orders)
                if units > ZERO:
                    fig = risk(o, units, ctx.book, ctx.account_ccy, snap.positions, snap.orders)
                    total, stale = total + fig.value, stale | fig.stale
        for p in snap.positions:
            if p.status == "OPEN":
                if p.units is None:
                    raise RiskUnknown("risk_unknown", "instrument_unknown")
                fig = risk(p, p.units, ctx.book, ctx.account_ccy, snap.positions, snap.orders)
                total, stale = total + fig.value, stale | fig.stale
        eq = ctx.equity()
        stale |= eq.stale
        return band_result(pct_of(total, eq.value), self.cfg, self.release_margin, "aggregate_open_risk", stale)


@dataclass
class MissingStopLossRule:
    """SOFT_HALT once now - stop_watch.effective_stop_missing_since_ms >= grace_ms (§6)."""

    cfg: MissingStopConfig
    rule_id: str = "missing_stop_loss"

    @property
    def confirm_ms(self) -> int:
        return self.cfg.confirm_ms

    def subjects(self, ctx: EvalContext) -> list[Subject]:
        return [ctx.entity_subject(e) for e in _live_entities(ctx)]

    def evaluate(self, ctx: EvalContext, subject: Subject) -> RuleOutcome:
        e = ctx.entity(subject)
        if e is None:
            return Unknown("risk_unknown", "entity missing")
        snap = ctx.snap
        if not needs_stop(e, snap.positions, snap.orders):
            return Result(Level.PASS, "no_stop_needed")
        if effective_stop(e, snap.positions, snap.orders) is not None:
            return Result(Level.PASS, "stop_present")
        since = snap.stop_watch.get((subject.subject_type, subject.subject_id))
        if since is not None and ctx.now_ms - since >= self.cfg.grace_ms:
            return Result(Level.SOFT_HALT, "missing_stop_loss")
        return Result(Level.PASS, "stop_grace")


@dataclass
class MaxDailyLossRule:
    """(equity_mark - sod_equity - net_transfers_today) / sod_equity < -limit -> HARD_HALT; subject
    (account, trading_day); capped at SOFT on partial_session, stale inputs, the balance / position
    skew window or a pending cash-flow request (§2, §6)."""

    cfg: DailyLossConfig
    release_margin: Decimal
    balance_skew_ms: Callable[[str], int]
    rule_id: str = "max_daily_loss"

    @property
    def confirm_ms(self) -> int:
        return self.cfg.confirm_ms

    def subjects(self, ctx: EvalContext) -> list[Subject]:
        return [Subject("account_day", ctx.trading_day_subject_id, ctx.snap.account_id)]

    def _cap_reasons(self, ctx: EvalContext, stale: frozenset[SymbolKey]) -> list[str]:
        snap, out = ctx.snap, []
        if snap.sod is not None and snap.sod.partial_session:
            out.append("partial_session")
        if stale:
            out.append("stale_mark")
        change, bal = snap.last_status_or_units_change_ms, snap.balance_committed_at_ms
        if (change is not None and (bal is None or change > bal)
                and ctx.now_ms - change < self.balance_skew_ms(snap.adapter_id)):
            out.append("balance_skew")
        if snap.cash_flow_request_pending:
            out.append("cash_flow_request_pending")
        return out

    def evaluate(self, ctx: EvalContext, subject: Subject) -> RuleOutcome:
        snap = ctx.snap
        sod = snap.sod
        if sod is None:
            return Unknown("risk_unknown", "sod_marks row missing")
        if sod.sod_equity <= ZERO:
            return Unknown("risk_unknown", "sod_equity is not positive")
        eq = ctx.equity()
        transfers = net_transfers_today(snap.cash_flows, sod, ctx.book, ctx.account_ccy)
        pct = (eq.value - sod.sod_equity - transfers.value) / sod.sod_equity * HUNDRED
        limit = -self.cfg.limit_pct_of_sod_equity
        level = Level.HARD_HALT if pct < limit else Level.PASS
        release = Level.HARD_HALT if pct < limit * (1 - self.release_margin) else Level.PASS
        reason = "daily_loss"
        if level is Level.PASS:
            flags = self._flags(ctx, eq)
            if flags:
                level, release, reason = Level.FLAG, Level.FLAG, flags[0]
        stale = eq.stale | transfers.stale
        caps = self._cap_reasons(ctx, stale)
        if caps and level is Level.HARD_HALT:
            reason = f"daily_loss({','.join(caps)})"
        wants = eq.unexplained_delta is not None and eq.unexplained_delta < ZERO
        return Result(level, reason, Cap.SOFT if caps else Cap.NONE, pct, release, stale, wants)

    def _flags(self, ctx: EvalContext, eq: EquityMark) -> list[str]:
        snap, out = ctx.snap, []
        if eq.excluded_increase > ZERO:
            out.append("unexplained_balance_delta")
        if not snap.pnl_source:
            out.append("pnl_unverified")
        if not snap.transfer_source:
            out.append("transfers_unverified")
        live = snap.account_live
        if live is not None and live.equity is not None and eq.value > ZERO:
            if abs(live.equity - eq.value) / eq.value * HUNDRED > self.cfg.equity_divergence_pct:
                out.append("equity_divergence")
        return out


@dataclass
class MarginUsageRule:
    """margin_used / equity_mark > 80 % FLAG, > 95 % SOFT_HALT; missing or sampled live -> UNKNOWN (§6)."""

    cfg: BandRuleConfig
    release_margin: Decimal
    rule_id: str = "margin_usage"

    @property
    def confirm_ms(self) -> int:
        return self.cfg.confirm_ms

    def subjects(self, ctx: EvalContext) -> list[Subject]:
        return [ctx.account_subject()]

    def evaluate(self, ctx: EvalContext, subject: Subject) -> RuleOutcome:
        live = ctx.snap.account_live
        if live is None or live.sampled or live.margin_used is None:
            return Unknown("live_unknown", "account_live missing or sampled")
        eq = ctx.equity()
        return band_result(pct_of(live.margin_used, eq.value), self.cfg, self.release_margin,
                           "margin_usage", eq.stale)


@dataclass
class StaleAccountImageRule:
    """Built-in FLAG `stale_account_image` while the account is fenced or SM3 is in an incident (§2)."""

    rule_id: str = STALE_ACCOUNT_IMAGE
    confirm_ms: int = 0

    def subjects(self, ctx: EvalContext) -> list[Subject]:
        return [ctx.account_subject()]

    def evaluate(self, ctx: EvalContext, subject: Subject) -> RuleOutcome:
        if ctx.fenced:
            return Result(Level.FLAG, STALE_ACCOUNT_IMAGE)
        return Result(Level.PASS, "account_image_current")


@dataclass
class StaleMarkRule:
    """Built-in FLAG `stale_mark` (§5 Clean mid, stale cases 2 and 3): a symbol the account holds or works
    (or a cross that converts its P&L into the account currency) is marked at its last mid because there has
    been no quote at all for longer than `stale_mark_ms` while OPEN (a frozen feed), or because it is closed
    or in its roll break (SM10). Subject: the account; `stale_symbols` lists them. The results that use those
    marks are capped at SOFT by the rules themselves; this rule only records the FLAG state (halt_breaches,
    oversee_events, one email). Without Redis (`redis_ok` False) every mark is a last mid by construction, so
    only the closed / roll-break case flags then (the caps still apply): a Redis blip is SM8's page, not a
    frozen feed on every account."""

    rule_id: str = STALE_MARK
    confirm_ms: int = 0

    def subjects(self, ctx: EvalContext) -> list[Subject]:
        return [ctx.account_subject()]

    def evaluate(self, ctx: EvalContext, subject: Subject) -> RuleOutcome:
        snap = ctx.snap
        book = MarkBook(snap.quotes, snap.instruments, ctx.now_ms, ctx.cfg.marks.stale_mark_ms)
        ccy = snap.account.currency if snap.account is not None else None
        for e in _live_entities(ctx):
            try:
                book.mark(e.adapter_id, e.symbol, e.side)
                if ccy is not None:
                    book.fx(e.adapter_id, book.spec(e.adapter_id, e.symbol).pnl_ccy, ccy)
            except RiskUnknown:
                continue          # an unknown input is the other rules' UNKNOWN, not a stale mark
        wanted = ({MarkQuality.LAST_MID, MarkQuality.CLOSED_LAST_CLEAN} if snap.redis_ok
                  else {MarkQuality.CLOSED_LAST_CLEAN})
        flagged = frozenset(k for k, q in book.qualities.items() if q in wanted)
        if flagged:
            return Result(Level.FLAG, STALE_MARK, stale_symbols=flagged)
        return Result(Level.PASS, "marks_current")


@dataclass(frozen=True)
class Evaluation:
    subject: Subject
    outcome: RuleOutcome
    confirm_ms: int


def run_rule(rule: Rule, ctx: EvalContext, subject: Subject) -> RuleOutcome:
    """Evaluate one rule; a missing input or a rule that raised is UNKNOWN (§2)."""
    try:
        return rule.evaluate(ctx, subject)
    except RiskUnknown as exc:
        return Unknown(exc.reason, exc.detail)
    except Exception as exc:  # a rule that raised an error is UNKNOWN, never a failed pass
        return Unknown("risk_unknown", f"rule_error: {type(exc).__name__}: {exc}")


class RuleRegistry:
    """Builds the enabled rules from config (§6); `evaluate` applies the account fence cap (§2)."""

    def __init__(self, rules: Sequence[Rule], ruleset_version: int = 1) -> None:
        self.rules = list(rules)
        self.ruleset_version = ruleset_version

    @property
    def rule_ids(self) -> frozenset[str]:
        return frozenset(r.rule_id for r in self.rules)

    def confirm_ms(self, rule_id: str) -> int:
        return next((r.confirm_ms for r in self.rules if r.rule_id == rule_id), 0)

    @staticmethod
    def from_config(cfg: OverseeConfig) -> RuleRegistry:
        m = cfg.sweep.release_margin
        rules: list[Rule] = [StaleAccountImageRule(), StaleMarkRule()]
        if cfg.max_position_risk.enabled:
            rules.append(MaxPositionRiskRule(cfg.max_position_risk, m))
        if cfg.max_aggregate_open_risk.enabled:
            rules.append(MaxAggregateOpenRiskRule(cfg.max_aggregate_open_risk, m))
        if cfg.missing_stop_loss.enabled:
            rules.append(MissingStopLossRule(cfg.missing_stop_loss))
        if cfg.max_daily_loss.enabled:
            rules.append(MaxDailyLossRule(cfg.max_daily_loss, m, cfg.sweep.skew_ms))
        if cfg.margin_usage.enabled:
            rules.append(MarginUsageRule(cfg.margin_usage, m))
        return RuleRegistry(rules, cfg.ruleset_version)

    def evaluate(self, ctx: EvalContext) -> dict[StateKey, Evaluation]:
        out: dict[StateKey, Evaluation] = {}
        for rule in self.rules:
            try:
                subjects = rule.subjects(ctx)
            except Exception:  # cannot enumerate: one UNKNOWN on the account
                subjects = [ctx.account_subject()]
            for subject in subjects:
                outcome = run_rule(rule, ctx, subject)
                if ctx.fenced and isinstance(outcome, Result) and rule.rule_id not in INPUT_FLAG_RULES:
                    outcome = outcome.capped()
                out[(rule.rule_id, subject.subject_type, subject.subject_id)] = Evaluation(
                    subject, outcome, rule.confirm_ms)
        return out


def stale_symbols(evals: dict[StateKey, Evaluation], ctx: EvalContext) -> frozenset[SymbolKey]:
    """Every stale_mark symbol seen in this pass (the gate denies risk-increasing actions on them, §8)."""
    seen = set(ctx.book.stale_seen)
    for ev in evals.values():
        if isinstance(ev.outcome, Result):
            seen |= ev.outcome.stale_symbols
    return frozenset(seen)


__all__ = ["EvalContext", "Rule", "RuleRegistry", "Evaluation", "MaxPositionRiskRule",
           "MaxAggregateOpenRiskRule", "MissingStopLossRule", "MaxDailyLossRule", "MarginUsageRule",
           "StaleAccountImageRule", "StaleMarkRule", "band_level", "band_result", "run_rule", "stale_symbols",
           "INPUT_FLAG_RULES", "STALE_ACCOUNT_IMAGE", "STALE_MARK"]
