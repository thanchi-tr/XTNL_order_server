"""Read-only reports built from the engine's own PostgreSQL log (no broker access, no writes).

* `lifecycle`: order / position versions from `event_log` plus the current rows, rebuilt into per-order life
  cycles (pure timeline functions, one read-only transaction for the read).
* `pinescript`: one TradingView Pine Script v6 indicator per symbol that draws those life cycles.
* `export`: the `python -m src.ops pinescript` command's read -> render -> write pipeline, and its in-memory
  variant (`plan_pinescript`, used by the order-server API in src/api).

Privacy: nothing here selects, keeps or renders a size, an amount, a balance or a spread. Only symbols, broker
ids, sides, times, status names and price levels leave the database.
"""
