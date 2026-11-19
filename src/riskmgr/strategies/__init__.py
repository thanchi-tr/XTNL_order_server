"""Registered stop strategies (design §B6.1, §B6.3; plan S20). v1: kind (a) `break_even_on_risk_close` (Q1)."""
from __future__ import annotations

from src.riskmgr.strategies.break_even_on_risk_close import BREAK_EVEN_ON_RISK_CLOSE_V1, BreakEvenOnRiskClose

__all__ = ["BREAK_EVEN_ON_RISK_CLOSE_V1", "BreakEvenOnRiskClose"]
