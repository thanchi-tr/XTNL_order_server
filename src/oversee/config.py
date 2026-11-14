"""Oversee configuration with the diagram's example `rule_cfg` defaults (oversee-rules.md §2, §5, §6)."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType
from typing import Final

ADAPTER_WILDCARD: Final = "*"   # symbol key of a per-adapter clean-spread threshold


@dataclass(frozen=True)
class SweepConfig:
    """RiskSweep timing (§2). All durations in milliseconds."""

    interval_ms: int = 1_000
    release_ms: int = 5_000
    release_margin: Decimal = Decimal("0.10")
    unknown_grace_ms: int = 30_000
    account_fence_timeout_ms: int = 10_000
    escalate_after_ms: int = 900_000
    cool_down_ms: int = 300_000
    evaluation_failed_after_ms: int = 10_000
    quarantine_after_ms: int = 300_000
    quarantine_retry_ms: int = 600_000
    partial_session_late_ms: int = 30_000
    sod_missing_page_ms: int = 60_000
    cash_flow_request_min_interval_ms: int = 60_000
    default_balance_skew_ms: int = 5_000
    balance_skew_ms: Mapping[str, int] = field(default_factory=lambda: MappingProxyType({}))
    gate_freshness_passes: int = 3

    def skew_ms(self, adapter_id: str) -> int:
        return self.balance_skew_ms.get(adapter_id, self.default_balance_skew_ms)


@dataclass(frozen=True)
class MarksConfig:
    """Clean-mid selection (§5). `sod_mark_max_age_ms`: tier-persistence.md §4 (5 min).

    `max_clean_spread_pips` is the default clean-quote threshold; `symbol_clean_spread_pips` overrides it
    per (adapter_id, symbol), and `(adapter_id, "*")` per adapter (rulesets table, IMPLEMENTATION_PLAN 4.5).
    """

    stale_mark_ms: int = 5_000
    sod_mark_max_age_ms: int = 300_000
    max_clean_spread_pips: Decimal = Decimal("5")
    symbol_clean_spread_pips: Mapping[tuple[str, str], Decimal] = field(
        default_factory=lambda: MappingProxyType({}))

    def clean_spread_pips(self, adapter_id: str, symbol: str) -> Decimal:
        """The clean-mid spread threshold of one (adapter_id, symbol): symbol, then adapter, then default."""
        by = self.symbol_clean_spread_pips
        hit = by.get((adapter_id, symbol))
        if hit is None:
            hit = by.get((adapter_id, ADAPTER_WILDCARD))
        return self.max_clean_spread_pips if hit is None else hit


@dataclass(frozen=True)
class BandRuleConfig:
    enabled: bool = True
    flag_above_pct: Decimal = Decimal("1.0")
    halt_above_pct: Decimal = Decimal("1.5")
    confirm_ms: int = 0


@dataclass(frozen=True)
class MissingStopConfig:
    enabled: bool = True
    grace_ms: int = 2_000
    confirm_ms: int = 0


@dataclass(frozen=True)
class DailyLossConfig:
    enabled: bool = True
    limit_pct_of_sod_equity: Decimal = Decimal("3.0")
    confirm_ms: int = 1_000
    equity_divergence_pct: Decimal = Decimal("0.5")


@dataclass(frozen=True)
class OverseeConfig:
    sweep: SweepConfig = SweepConfig()
    marks: MarksConfig = MarksConfig()
    max_position_risk: BandRuleConfig = BandRuleConfig()
    max_aggregate_open_risk: BandRuleConfig = BandRuleConfig(
        flag_above_pct=Decimal("2.0"), halt_above_pct=Decimal("3.0"), confirm_ms=1_000)
    missing_stop_loss: MissingStopConfig = MissingStopConfig()
    max_daily_loss: DailyLossConfig = DailyLossConfig()
    margin_usage: BandRuleConfig = BandRuleConfig(
        flag_above_pct=Decimal("80"), halt_above_pct=Decimal("95"), confirm_ms=0)
    ruleset_version: int = 1
