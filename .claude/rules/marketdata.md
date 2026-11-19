---
paths:
  - "src/marketdata/**"
---

# Market-data layer rules

Design: `docs/design/automated-risk-manager.md` Part A; plan `docs/design/arm-implementation-plan.md`.

1. **Market data lives only in `redis-md`** (ticks, bars, quotes, spreads, indicator values), derived from the broker;
   it is not a duplicate of PostgreSQL, sits outside SM8 and the integrity worker, and never reaches PostgreSQL, the
   engine Redis, a log record, a page or a notice body. Logs and pages carry series ids, buckets, field names, check
   names and revisions, never values.
2. **Lockout:** `src/marketdata` is in `LOCKOUT_SCOPE`; the import allow-list, asyncio list, reflection rules and name
   ban are in `riskmgr.md` §2-§3 (one copy, kept equal to `src/riskmgr/lockout.py`). In short: `zlib` with `crc32`
   only; `redis` only under `src/marketdata/store`; `concurrent.futures` (`ThreadPoolExecutor` only) and
   `asyncio.wrap_future` only in `src/marketdata/indicators/engine.py` (md-indicators' own bounded executor, plan S38d;
   `FUTURES_FILES`); never `src.riskmgr` or `src.armconfig`; no `uuid`, `secrets` or
   `random` (the writer nonce and jitter come from the runtime through a port).
3. **Never blocks ingestion:** the tick hook is O(1); writes go through bounded queues; a `redis-md` fault parks
   market data (GAPPED) and never stops ingestion or Oversee. Off unless `XTNL_MD_ENABLED=1`.
4. **Adding an indicator:** a new `(KIND, VERSION)` in `src/marketdata/indicators/registry.py` (never edit a pinned
   version); `OUTPUTS` each with a unit (PRICE, RATIO, COUNT) and its quantum pinned per VERSION; `parse_params`
   rejects unknown keys and out-of-bound values; `window()` returns `Count(n)` or `Anchored(anchor_spec, max_bars)`;
   `step()` pure and deterministic under the pinned Decimal context; golden vectors (canonical equals incremental at
   every checkpoint; replay from re-fetched bars gives the identical quantised value); `indicator_id` is the opaque
   hash of the canonical spec, so no id shows the quantum; names follow `riskmgr.md` §3.
