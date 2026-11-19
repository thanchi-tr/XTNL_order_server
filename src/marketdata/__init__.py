"""Market-data layer, Phase 1 (design `docs/design/automated-risk-manager.md` Part A).

Ticks, bars and indicators derived from the broker live only in the separate `redis-md` store, never in PostgreSQL,
logs or pages. This package never imports `src.riskmgr` or `src.armconfig` and is under the read-only lockout
(`src/riskmgr/lockout.py`, design §B3). Off unless `XTNL_MD_ENABLED=1`.
"""
