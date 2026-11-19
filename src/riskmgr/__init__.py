"""Automated Risk Manager (ARM), Phase 1 (design `docs/design/automated-risk-manager.md` Part B).

Advice only: the ARM tracks open positions, evaluates the pinned stop strategy at bar ends and records decisions and
notices. It never sends, changes or removes a broker order; the read-only lockout (`src/riskmgr/lockout.py`, design
§B3) gates every module in this package, `src/marketdata` and `src/armconfig`. Off unless `XTNL_ARM_ENABLED=1`.
"""
