# XTNL Read-Only Broker Ingestion & Pub-Sub Engine

## Scope & Operational Boundary
This repository implements Phase 1 of the execution engine: **Read-Only Broker Ingestion, Non-Blocking Pub-Sub Normalization (`OrderDTO`, `PositionDTO`, `PriceDTO`, `AccountDTO`), Dead-Letter Queue (DLQ) Recovery, Tiered State Integrity (`Redis Cache > PostgreSQL DB > Image Backup`), and Oversee risk-rule monitoring with email / mobile alerts**.
- **STRICT PROHIBITION:** Live outbound order execution (`place_order`, `modify_order`, `cancel_order`) is mechanically blocked via PreToolUse hooks and interface definitions.
- **Halts are recorded, not enforced, in Phase 1:** The Oversee RiskSweep keeps one rule state per (rule, subject) in PostgreSQL `halt_breaches`, logs each transition in `oversee_events`, and sets the halt level. `halt_events` (the truth for the level) gets a row only when the level rises or is cleared, and Redis `halt:state` mirrors it. Level changes are alerted. Nothing in Phase 1 sends orders. Any future outbound sender (Phase 2 `ExecutionGateway`) must call `ActionGate.allow(action)` before every send. `ActionGate` **fails closed**: no readable, current halt state means no action.
- **Mandatory Skill Invocation:** Whenever modifying files in `src/adapters/`, `src/pubsub/`, `src/dto/`, `src/integrity/`, `src/oversee/`, `src/marketdata/`, `src/riskmgr/`, or `src/armconfig/`, explicitly invoke `/live-trading-server` first.
- **Market data only in `redis-md`:** ticks, bars, quotes, spreads and indicator values are derived from the broker and live only in the separate `redis-md` store (not a duplicate of PostgreSQL; outside SM8 and the integrity worker); never in PostgreSQL, the engine Redis, logs or pages. `src/riskmgr`, `src/marketdata` and `src/armconfig` are under the read-only lockout scanner `src/riskmgr/lockout.py` (rules: `.claude/rules/riskmgr.md`, `.claude/rules/marketdata.md`); the market-data and ARM services stay off unless `XTNL_MD_ENABLED` / `XTNL_ARM_ENABLED` are set.

## Repository Architecture
Design reference: `docs/architecture/phase1_ingestion_arch.drawio` (architecture, design patterns, state machines, use cases, deployment). Edit it via `docs/architecture/generator/gen_phase1_arch.py`, not by hand.

- `src/dto/`: Immutable canonical DTOs (`OrderDTO`, `PositionDTO`, `PriceDTO`, `AccountDTO`, `CashFlowDTO`, `InstrumentSpec`).
  - Each stateful DTO carries a **version** (broker revision, broker time, then `observed_seq`: at a tie the later observation wins; `seq_id` only orders equal keys with different content), so a stale REST read can never overwrite a newer stream update.
  - Quantities are canonical units (`ordered_units`, `remaining_units`, `is_reduce_only` on orders). DTOs carry no risk figures; Oversee owns risk.
- `src/adapters/`: Pluggable read-only `BrokerAdapter` implementations (`StoneXAdapter`, `PepperstoneAdapter`, `MockBrokerAdapter`; Pepperstone via the cTrader Open API). Zero direct database, cache, or pub-sub imports allowed. `BROKER_ENV` is `sandbox` or `live_readonly` (read-only credentials on the real accounts); anything else is refused at start-up.
- `src/pubsub/`: Non-blocking asynchronous event bus with four topics: `orders.internal.dto`, `position.state.dto`, `market.price.dto`, `account.state.dto`. The ingestion orchestrator is the sole publisher. Overflow goes to an in-memory `DlqSpooler` (`offer()` does no I/O), and sockets are never left unread.
- `src/integrity/`: Three-tier verification worker (`Redis > PostgreSQL DB > Immutable Image Backup`), the Redis-loss bootstrap (SM8), and the `events_dlq` check-before-retry consumer.
- `src/db/`: PostgreSQL single source of truth, transactional outbox, append-only event log, and 114-trade baseline schema migrations.
- `src/cache/`: Tier 1 Redis cache, an **intentional, disposable duplicate** of PostgreSQL state.
  - Entity keys are written only after the PostgreSQL commit, and only through `apply_state.lua` (atomic version compare-and-set, then `XADD` to the `oversee:feed` stream). Live keys (prices, live margin, connection health) use a separate light script; `halt:state` has its own CAS script.
  - If Redis loses its data, `ActionGate` denies everything until SM8 has rebuilt it from PostgreSQL.
- `src/oversee/`: Observer of **committed** state.
  - The feed observer reads the Redis Stream `oversee:feed` (never the in-memory bus) and wakes the **RiskSweep** for that account.
  - The RiskSweep is level-triggered: every second it re-evaluates every account from one PostgreSQL snapshot plus current prices, and it is the only writer of rule states and the halt level. Deadlines (stop grace, confirm, escalation, cool-down) are durable timestamps; there are no Oversee timers.
  - Runs pluggable `Rule` strategies on orders and open positions, using shared pure risk functions (directional risk in the account currency, effective stop, clean-mid equity mark). Example: `MaxPositionRiskRule` (≤ 1.0 % (1R) PASS, 1.0–1.5 % FLAG, > 1.5 % SOFT_HALT).
  - Also holds `HaltSwitch`, `ActionGate`, and `NotificationDispatcher` (email + mobile push).
- `src/marketdata/`, `src/riskmgr/`, `src/armconfig/`: Stage 9 market-data layer and ARM (design `docs/design/automated-risk-manager.md`); advice only, nothing is sent to a broker.
- **24/7 backbone:** the server runs unattended as the base of the future automated trading system.
  - Two London engine nodes in different AZs plus a witness.
  - One fenced LEADER: etcd lease + PostgreSQL epoch, lease keeper off the event loop, hard-kill fence.
  - Patroni sync standby (RPO 0).
  - Health-gated election with an anti-flap SAFE_HOLD; market calendar (SM10); off-node alerting and a push dead-man's switch.
  - Disaster image, dashboard and on-call in Melbourne.
  - Rules: `.claude/rules/operations.md`; diagram page 5 and SM9 / SM10.

## Known gaps before implementation
- `verify_execution.py` and both hooks were rewritten against the current rules (2026-11-13): the suite imports the real `src/` code and reports SKIP for modules not built yet (`--strict` turns SKIP into FAIL).
- Broker endpoints, rate limits and read-only scopes must be confirmed with StoneX and Pepperstone (cTrader Open API).

## Commands
Use the project virtualenv (`.venv`, created with `python -m venv .venv` and `pip install -e .[dev]`).
- **Run Deterministic Replay & Integrity Suite:** `.venv/Scripts/python.exe .claude/skills/live-trading-server/scripts/verify_execution.py --strict`
- **Run Unit & Contract Tests:** `.venv/Scripts/python.exe -m pytest -q tests/`
- **Type Check:** `.venv/Scripts/python.exe -m mypy src/` (strict mode is set in `pyproject.toml`)
- **StoneX read-only probe (needs the STONE_X_* environment variables):** `.venv/Scripts/python.exe -m src.adapters.stonex.probe`
- **XTNL** (`C:\Users\Thanc\OneDrive\Desktop\XTNL`) is a read-only reference: never modify it.
- **Regenerate Architecture Diagram:** `python docs/architecture/generator/gen_phase1_arch.py docs/architecture/phase1_ingestion_arch.drawio docs/architecture/generator/measured.json`
