# Market-data layer and ARM: implementation plan (for design v11)

Status: **plan; S00, S01, S02, S03, S06, S04, S05, S07, S08, S09, S10, S11, S12, S13, S14, S15, S16, S17a, S17b, S18, S19, S20, S22, S23, S24, S25, S26, S27, S28, S29, S30a, S30b, S31, S32, S33, S34, S35, S36, S36b, S37, S38, S38b, S38c, S38d, S38e, S38f, S38g, S39 and S39b done (2026-11-17 / 2026-11-18; S07 merges only after
the user confirms the Oversee change; the S11 `.claude/` diff waits for approval); S21 built ahead of the S07 gate, not accepted (it waits for the
user's confirmation of the §9.2 Oversee change and C1; S22, S27, S28, S29, S30a, S30b, S31, S32, S33, S34, S35 and S36 were built without importing it; S36b joins it to the runtime as its stage text directs, on the §9.2 defaults C1-C4, which stay unconfirmed and switchable; S37's `.claude/` rule diff waits for approval; S39 built after the PR3 probe, StoneX kept out of `BAR_CAPABLE_ADAPTERS` until its other go-live gates are wired or accepted; S39b wired the six engineering go-live gates, leaving the user-decision and PR3 gates open).** The build starts only on the user's signal (design review gate). Every path below is
relative to the repository root `C:\Users\Thanc\OneDrive\Desktop\order-server`.

Inputs: `docs/design/automated-risk-manager.md` (v11; "the design"), the binding user decisions D1-D7, the round-11
review findings, and the three code maps (runtime, ingestion, storage). Code-map mismatches are cited as `RT-Mn`
(runtime map, M1-M13), `IN-n` (ingestion map, items 1-15) and `ST-n` (storage map, items 0.1-0.14). Round-11 MAJOR
findings are `R11-M01` … `R11-M20` (§5); minor ones are `R11-m01` … (§6).

## 1. Ground rules for every stage

* **Read-only.** No `place_order` / `modify_order` / `cancel_order`, no sender, no intent table, no adapter mutation
  method, no outbound order path of any kind. `MockBrokerAdapter` is the only broker in tests. The only REST client the
  adapters construct stays `ReadOnlyTransport`. `FORBIDDEN_METHOD_NAMES` / `assert_read_only` (`src/adapters/base.py:440`),
  the `ReadOnlyTransport` deny-list (`src/adapters/transport.py`) and the hook's repo-wide `FORBIDDEN_PATTERNS` are never
  narrowed. Every new adapter capability (`fetch_price_bars` S03, `fetch_active_stop_orders` S04, the PR3 probe S03,
  the StoneX bar endpoint S39) is a GET through the adapter's existing `ReadOnlyTransport` (StoneX) or a read-only cTrader
  request (Pepperstone), and its stage adds a test that it constructs no other HTTP client and attempts no denied path.
* **No market data in PostgreSQL.** No stage adds a tick, bar (OHLC), quote, spread, indicator value, or a value the close
  can be recomputed from (favour in R, favour / risk) to any PostgreSQL table, column, `event_log` / `operator_requests`
  payload, outbox body (unsent, expired or finished), log record or page. PostgreSQL keeps ids, bucket times, check
  names, counts, order-state levels (entry, stops, proposed stop) and HMAC hashes with their key id only (design §B3,
  §B8). The existing `last_clean_marks` and `sod_marks.clean_mids` are left exactly as they are pending Q15 (§9.1).
  S23 adds a schema-scan test over 0015-0017 and S38 the end-to-end no-market-data test.
* Never read `.env` files or `deploy/env/local.env`. Never modify the XTNL folder (it may be read as a reference only).
  No account values, balances, P&L, pip / point sizes or spreads in code comments, docs, logs, test names or the
  probe report. Test fixtures use synthetic instruments.
* **Skill:** a stage that edits `src/adapters/`, `src/pubsub/`, `src/dto/`, `src/integrity/` or `src/oversee/` invokes
  `/live-trading-server` first. From S11 on, `src/marketdata/`, `src/riskmgr/` and `src/armconfig/` join that list (design
  §C2).
* **Green gate G** (every stage ends with all three):
  `.venv/Scripts/python.exe -m pytest -q tests/`,
  `.venv/Scripts/python.exe -m mypy src/` (strict),
  `.venv/Scripts/python.exe .claude/skills/live-trading-server/scripts/verify_execution.py --strict`.
  A new `verify_execution.py` check is added only in the stage that builds the module it imports (a missing module is
  SKIP, which `--strict` turns into FAIL).
* **Off by default.** Nothing new runs until the user sets local flags (§2). With the flags off, the engine behaves exactly
  as before; the prerequisite stages (S01-S10) change existing behaviour only where a stage says so explicitly.
* **Changes under `.claude/` and to `CLAUDE.md`** (hooks, rules, `verify_execution.py`) are shown to the user as a diff
  and applied only after the user approves them (S07, S11, S35, S37). If approval is pending, the rest of the stage still
  lands and stays green; the parity test that needs the hook is skipped with the reason "hook update pending".
* `pre_tool_guard.py` blocks any `/cancel…` text in `src/**/*.py` outside `src/adapters/transport.py`, docstrings
  included (ST-9). Planted-violation tests build their samples in `tmp_path` under `tests/`, never in `src/`.
* **The lockout gates every stage from S11 on, approval or not:** `tests/riskmgr/test_lockout.py` scans the real
  `LOCKOUT_SCOPE` tree (and `src/runtime/md_ports.py` once it exists) as part of the pytest leg of G, so a name-ban or
  import violation fails G even while the `.claude/` diff of S11 waits for approval. No file is created in
  `src/marketdata`, `src/riskmgr` or `src/armconfig` before S11 is green.
* **Test layout:** tests mirror `src/` (`tests/marketdata/`, `tests/riskmgr/`, `tests/armconfig/`, `tests/hooks/`, new
  modules under the existing `tests/db/`, `tests/runtime/`, `tests/notifications/`, `tests/ops/`); every new test
  package gets an `__init__.py` as the existing ones have, created in the stage that first adds a test there.
* Each stage ticks one line in a new "Stage 9: market data and ARM" section of `docs/IMPLEMENTATION_PLAN.md` (added in
  S00). Commits only when the user asks.
* The working tree already has uncommitted work in `deploy/local/`, `src/api/`, `src/reports/` and `tests/deploy/`
  (git status at plan time). S36 touches some of those files; it builds on whatever is current and never reverts it.
  `src/reports/lifecycle.py` (also uncommitted) selects explicit `event_log` / `orders_state` columns, so S01 and S04
  need no change there; showing `order_kind` in the lifecycle export is an optional display follow-up (design PR1).
* **Off-by-default infrastructure:** the `redis-md` container is in compose profile `md` (dev and PC) and is started by
  `start-xtnl.ps1` only when `XTNL_MD_ENABLED=1` as the engine reads it (S36); the AWS systemd unit is installed but
  not enabled (S37). Migrations 0015-0017 apply with the flags off and change no existing read or write path except as
  each stage states.

## 2. Flags and isolation (enabling never affects ingestion or Oversee)

| Setting (local only, never Supabase) | Default | Effect |
|---|---|---|
| `XTNL_MD_ENABLED` | `0` | `start_optional_services()` adds `optional-bootstrap` only when this is `1`. |
| `XTNL_ARM_ENABLED` | `0` | Adds the ARM tasks; requires `XTNL_MD_ENABLED=1`, every go-live gate wired or accepted (S38e, `XTNL_ARM_GOLIVE_ACCEPT`), both ARM channels and an accepted config (else idle with one page). |
| `XTNL_TICK_SYMBOLS` | empty | PR5 tick-only symbols; a separate orchestrator set, never `rt.tick_symbols`. |
| `XTNL_STONEX_PRICE_UNFILTERED` | `0` | `setRequestedMaxFrequency("unfiltered")` on StoneX price subscriptions; turned on only after the PR3 probe. |
| `XTNL_ARM_STOP_READS` | `0` | PR9 stop-handle reads; turned on only if PR3 shows manual stop moves are not streamed. |
| `XTNL_MD_*`, `XTNL_ARM_*`, `XTNL_ARM_FAIL_*` | design values | Parsed and bound-checked only inside `optional-bootstrap` (RT-M13); a bad value pages and parks the optional services, never a `WiringError`. |

The two enable flags are parsed tolerantly in `build_engine` (an invalid value means off, plus one page). Isolation is
proven by tests in S09, S31, S32 and S38: with the flags on and `redis-md` down, hung or raising, the ingestion rate,
the RiskSweep pass age, Oversee pages, the DRAINING budget (9.2 s of 10 s) and core operator requests are unchanged.

## 3. Stage overview

Estimated size is the changed lines including tests. "Existing" marks stages that change existing modules. Stages over
about 1500 lines are built as two PRs (S17a / S17b, S30a / S30b), each ending green on G; elsewhere "S17" or "S30"
means both halves. Golden-vector and drill-heavy stages (S16, S17, S18, S30, S38) carry most of their size in tests.

| Stage | Title | Depends on | Size | Existing |
|---|---|---|---|---|
| S00 | Design patch v11.1 and backlog entry (**done**) | — | doc | doc |
| S01 | PR-A: migration 0015 and DTO fields (**done**) | S00 | ~700 | yes |
| S02 | PR-B1: adapters, `order_kind` and `price_increment` (**done**) | S01 | ~600 | yes |
| S03 | PR-B2: adapters, price stream fields, per-market events, bar capability, PR3 probe script (**done**) | S01 | ~900 | yes |
| S06 | PR-D: `RestPriority.BARS`, BARS cap, send-time stamp (**done**) | S01 | ~400 | yes |
| S04 | PR-C1: orchestrator versions, origin, `reread_entity` result, stop-order list read (**done**) | S02, S06 | ~1200 | yes |
| S05 | PR-C2: orchestrator tick-only set, per-market subscriptions (**done**) | S03 | ~600 | yes |
| S07 | PR-E: Oversee `effective_stop_detail()` (**done**; merge waits for the §9.2 confirmation) | S04 | ~600 | yes |
| S08 | PR-F: outbox 0016, migrate post step, savepoints, dead-man optional fields (**done**) | S01 | ~800 | yes |
| S09 | PR-G1: runtime hooks (`start_optional_services`, supervisor, `aclose`, composite price hook) (**done**) | S00 | ~800 | yes |
| S10 | PR-G2: operator-request kind filter and handler registry, health registry, lifecycle ensure (**done**) | S09 | ~600 | yes |
| S11 | Lockout scanner, package skeletons, guardrails (**done**; `.claude/` diff waits for approval) | S09 | ~900 | `.claude` |
| S12 | MD-1: market-data core types, timing, validation, keyring, codec, ports (**done**) | S11 | ~900 | no |
| S13 | MD-2: `redis-md` store, Lua scripts, clients, rollback set (**done**) | S12 | ~1100 | dev compose |
| S14 | MD-3: tick path (hook rings, TICK bars, ClockSkew, clock steps) (**done**) | S12, S05 | ~1100 | no |
| S15 | MD-4: `md-writer` (closed-bar queue, AOF release, shedding) (**done**) | S13, S14 | ~700 | no |
| S16 | MD-5: bootstrap, SM12, reset flag, read port, pull cursor, re-hash (**done**) | S15 | ~1500 | no |
| S17 | MD-6: `md-bars` (two PRs: S17a BROKER bars, finality, error unit, backfill lane, outage pause (**done**); S17b cross-check (**done**)) | S16, S03, S06 | ~1000 + ~900 | no |
| S18 | MD-7: indicators (ATR), `md-indicators`, demand and admission (**done**) | S16, S17 | ~1300 | no |
| S19 | ARM-1: riskmgr core types, protocols, registry, settings, conformance suite (**done**) | S12 | ~700 | no |
| S20 | ARM-2: FirstBarGuard and kind (a) `break_even_on_risk_close` (**done**) | S19, S18, **Q1** | ~800 | no |
| S21 | ARM-3: envelope pipeline, `S_at_T`, release rule, PRE rules (**built ahead of the S07 gate, not accepted**: waits for the user's §9.2 / C1 confirmation) | S20, S07 | ~1000 | no |
| S22 | ARM-4: replay, frontier, corrections, per-strategy job runner (**done**; imports no S21 module, joined to the pipeline in S27) | S21 | ~900 | no |
| S23 | DB-1: migration 0017, lifecycle, roles, grants, request kinds (**done**) | S08, S10 | ~900 | yes |
| S24 | DB-2: ARM stores in `src/db` (**done**) | S23 | ~1100 | no |
| S25 | CFG-1: config document, validation, resolver, seen-row chain (pure) (**done**) | S19 | ~800 | no |
| S26 | ARM-5: `arm-tracker` (**done**) | S24, S22, S25 | ~1000 | no |
| S27 | ARM-6: BarClock, per-event transaction, sinks (**done**; imports no S21 module: the evaluation is a port the S21 join fills once the user confirms §9.2 / C1) | S26 | ~1000 | no |
| S28 | ARM-7: notices and the send-time renderer (**done**; the outbox insert is a port S29 fills) | S27 | ~900 | no |
| S29 | NT-1: `arm-relay` and the ARM outbox store (**done**; nothing wired, both flags stay off) | S28, S08 | ~900 | yes |
| S30 | FL-1: error budget, FAILED, immediate ping, clear probes (two PRs: S30a budget, FAILED, ping, spool (**done**); S30b clear probes (**done**)) | S29, S17 | ~1000 + ~800 | no |
| S31 | RT-1: `md_ports.py` and `optional-bootstrap` | S30, S10 | ~1300 | yes |
| S32 | RT-2: task wiring, DRAINING, handlers, metrics, dead-man, `arm_status` (**done**; BarClock and `arm-tracker` wait for S21 / S33) | S31 | ~900 | yes |
| S33 | CFG-2: Supabase client, installer, admission check (**done**; the arm-tracker wiring still waits for its snapshot / grid ports) | S32, S25 | ~900 | no |
| S34 | OPS-1: ops CLI controls and requests (**done**; the `arm-tracker` kinds stay pending until the tracker is wired) | S33 | ~900 | yes |
| S35 | OPS-2: ops CLI views, `arm replay`, registry check (**done**; the `verify_execution.py` check waits for approval) | S34 | ~800 | `.claude` |
| S36 | DEP-1: PC deploy (PR10) (**done**; both flags stay `0`) | S32 | ~700 | yes |
| S36b | RT-3: ARM runtime join (tracker, BarClock, notices, handlers) (**done**; builds on the unaccepted S21 with the §9.2 defaults C1-C4, still unconfirmed) | S36, S33, S21 | ~1200 | no |
| S37 | DEP-2: AWS deploy, observability, runbooks, rules, diagram (**done**; the `.claude/` rule diff waits for approval) | S36 | ~900 | `.claude` |
| S38 | E2E: end-to-end, failure drills, no-market-data test, load test (**done**) | all above | ~1500 | tests |
| S38b | DOC-1: design sync with the build, design v11.2 (**done**) | S38 | doc | doc |
| S38c | FIX-R12: round-12 design and code fixes, design v11.3 (**done**) | S38b | — | yes |
| S38d | HARD-1: go-live hardening, decision path, design v11.4 (**done**; the `.claude/` diff waits for approval) | S38c | — | yes |
| S38e | HARD-2: go-live hardening, operations and gates, design v11.5 (**done**; the `.claude/` diff waits for approval; `XTNL_ARM_ENABLED=1` and S39 refused until the go-live gates are wired or accepted) | S38d | — | yes |
| S38f | HARD-3: round-14 safety fixes, design v11.6 (**done**; migrations above `XTNL_MIGRATIONS_ACCEPTED_UPTO` (14) and the S07 rule (`XTNL_OVERSEE_STOP_AT_ENTRY`) opt-in; 0020 FAILED guard and durable error budget; the `.claude/` diff waits for approval) | S38e | — | yes |
| S38g | HARD-4: round-14 decision path, liveness and sleep, design v11.7 (**done**; final-advice moves never discounted by AT_PRE; PRIOR_VERSION direction rule; liveness pages; Windows sleep detection, sleep fences and the sleep restart left out of `unclean_restarts`; the `.claude/` diff waits for approval) | S38f | — | yes |
| S39b | GATES: the six engineering go-live gates (**done**; `uptime_suspend`, `receipt_gap_feed`, `replay_rerun`, `conformance_probe_partial`, `entry_time_source`, `corrections_restart` wired; no migration; the `.claude/` diff waits for approval) | S39 | ~2000 | yes |
| S39 | BRK: StoneX broker bars and PR9 enablement (**done**; StoneX `fetch_price_bars` on, `pr3_finality` wired; `BAR_CAPABLE_ADAPTERS` stays empty until the other S39 gates are wired or accepted; the `.claude/` diff waits for approval) | S03, S04, S06, **PR3 probe run** | ~600 | yes |

Order of work: S00, S01, S02, S03, S06, S04, S05, S07, S08, S09, S10 (prerequisites, smallest safe changes to existing
code first), then S11-S18 (market data), S19-S22 (pure ARM core), S23-S24 (persistence), S25-S28, S29-S30 (notices
and FAILED), S31-S32 (runtime wiring), S33 (Supabase), S34-S35 (ops CLI), S36-S36b-S37 (deploy and runtime join),
S38 (end to end), S38b (design sync), S38c (round-12 fixes). S39 slots in whenever the PR3 probe results exist;
enabling the ARM on StoneX waits for it.

## 4. Stages

Each stage lists: goal; files (C = create, M = modify); key types and functions; tests; acceptance (always G plus what
is listed); findings resolved; risks.

### S00 Design patch v11.1 and backlog entry

Status: done 2026-11-16. All 17 §8 patches applied to `docs/design/automated-risk-manager.md` (title and status now
v11.1; no section says the FAILED ping's critical copy goes through `EngineSession.page`: it is the `insert_outbox`
row in the episode's fenced transaction, `page()` only a log mirror); `docs/IMPLEMENTATION_PLAN.md` gained "Stage 9 —
market data and ARM" (S00 ticked, S01-S39 unchecked, S17 and S30 split into their a / b PRs). Line drift (IN-15,
ST-14): every `file:line` citation in the design was re-checked against the working tree and all point at the cited
code; only `reread_entity()`'s caller list was wrong (it also serves the account refreshes at `session.py:1782` /
`:1946`) and is fixed in PR8 / PR9. No code changed. Deviations: (1) the §9.2 confirmations are also listed in
the design's §0 as C1-C5, each with its built default, so the design is self-contained; (2) a few §6 minor findings
that §8 does not list were written into the design where the surrounding text would otherwise contradict what later
stages build: the BROKER verification re-read at `min(final + 60 s, cap − 30 s)` with `unverified_final` (§A4), the
per-bar-spec timing table (§B5.2), the `xtnl_arm` pool's `application_name` (§B3), the post-part-4 item key (event
POST, part 0, `position:resource:owner:revision`, §B8), and `envelope_version` on decisions (§B6.1); (3) the clear probe
of an ADAPTER scope (not stated in this plan) is written as "the probe of each of its FAILED series" (§B14), to be
confirmed in S30b. The §9.2 confirmations themselves are still open with the user.
Review fixes (2026-11-16, design and this plan; still no code): (a) §B3 allow-list made buildable: `zlib` (`crc32`
only) in `src/marketdata`, `concurrent.futures` (`ThreadPoolExecutor` only) plus asyncio `get_running_loop` /
`wrap_future` in `src/riskmgr` for the per-version job executors (§B4; never `to_thread`), and `lockout.py` exempt by
name (`SCANNER_SELF`) under its own allow-list; mirrored in S11, S12, S22; (b) the `xtnl_arm` grants are a complete
list again (SELECT on the ARM state tables restored; `engine_epoch`, `event_log`, `orders_state`, `positions_state`
read-only; INSERT only on the outbox), and the relay, `operator_requests`, the tracker snapshot, the partition ensure
and the FAILED clear statement run on the engine pool; mirrored in S23, S24, S29; (c) the ARM-global scope also
names the stuck frontier, which counts only positions not FAILED or paused while the ARM runs (S30); (d) the FAILED
ping's log mirror is `PagerWrapper.log_mirror()` (`log.critical`), never `EngineSession.page`, which queues its own
critical row (S30, S31); (e) the clear probe's trial bootstrap and the post-clear bootstrap never count toward the
reset budget, and an accepted clear resets the counters and window (S16, S30); (f) `pre_basis` takes the level farther
from entry of it and the current stop and is keyed by handle role, not order id (S21); (g) PRE notices carry no
favour figures, recorded as confirmation C5 (§0, §9.2; S28); (h) §B6.3 (7) uses `max(R, 0)` with the caveat
`be_offset_first_seen`, the §6 row S20 builds.

* **Goal:** the design states exactly what will be built before code exists. Doc only.
* **M:** `docs/design/automated-risk-manager.md` (every patch in §8, version line "v11.1"); `docs/IMPLEMENTATION_PLAN.md`
  (new section "Stage 9: market data and ARM" with S01-S39 unchecked, pointing here).
* **Acceptance:** each §8 patch present; no section still says the FAILED ping's critical copy goes through
  `EngineSession.page`; the user confirms the §9.2 items before S21 / S28 / S30 depend on them.
* **Resolves:** the document halves of R11-M01 … R11-M20; IN-15 and ST-14 line drift.
* **Risk:** keep each patch to the stated sentences; any change of meaning is listed in §9.2 for the user.

### S01 PR-A: migration 0015 and DTO fields (PR1, PR2, PR7, PR8 fields only)

Status: done 2026-11-16. Built: `src/db/migrations/0015_arm_prereqs.sql` (`orders_state.order_kind` with its CHECK,
the three `instruments` columns, `event_log.origin` with its CHECK `NOT VALID` and `event_log.broker_time_ms`, drop of
`sod_marks.position_marks`; idempotent, guarded constraints); the DTO fields (`OrderDTO.order_kind`,
`PriceDTO.ts_source` / `received_ms`, the `InstrumentSpec` metadata, `origin` / `broker_time_source` on the three
stateful DTOs) with the `OrderKind`, `TsSource`, `Origin`, `BrokerTimeSource` types; `NON_PAYLOAD_FIELDS` +=
`origin`, `broker_time_source`; `OPTIONAL_PAYLOAD_FIELDS` skipped by `payload_checksum` while `None`; the `order_kind`
check in `schema_problem`; `ORDERS.payload_cols` / `COL_TYPES`, `ORDER_COLS` (index 21), `INSTRUMENT_COLS` (11-13),
`SQL_INSTRUMENT_UPSERT` (`$17`-`$19` and `DO UPDATE SET`), `SQL_EVENT_LOG` (`$11` origin, `$12` broker_time_ms; `_log`
takes them as keyword arguments defaulting to NULL and no caller passes them yet), the dashboard orders view. Tests:
`tests/dto/test_arm_prereq_fields.py` (golden checksums computed before the change for order, position, account,
instrument and cash flow; set `order_kind` / instrument fields change the checksum; version labels never do; JSON
encodings byte-identical while `None`), `tests/pubsub/test_order_kind_validation.py`,
`tests/db/test_arm_prereqs_migration.py` (idempotency regexes, column allow-list, nullable without default, CHECK
forms, no index), `tests/db/test_arm_prereq_rows.py` (row round trip, `integrity_store` + `dbcheck` recompute with
`order_kind = STOP`, event_log NULL from every caller), `tests/db/test_schema.py` now builds each table's columns from
every migration (ST-5: later `ADD COLUMN` counts, `DROP COLUMN` removes; also the idempotency and no-float checks over
every file), and a `pg`-marked round trip in `tests/db/test_pg_integration.py` (run once against a disposable
PostgreSQL 17 container: 14 `pg` tests passed, 0015 applied twice). G green; `verify_execution.py --strict` 11 / 11.
Deviations: (1) the two JSON encoders (`src/cache/codec.py` `dto_dict`, `src/pubsub/dlq_rows.py` `dto_json`) leave out
a `None` field of the new set `OMITTED_WHEN_NONE` (the optional payload fields, the PR8 labels and the PR2 price
timing fields), so Redis / jsonb payloads and the DLQ `payload_hash` also stay byte-identical (not in the plan's file
list; a set value is encoded as usual); (2) `ORDER_KINDS` (frozenset) is exported for the validation check, and
`OrderKind` / `Origin` / `BrokerTimeSource` / `TsSource` are `Literal` types like `Side` rather than enums, so the
`order_kind` check is the `side` form (`not in` a set) rather than the `status` `isinstance` form; (3) two test
fixtures in `tests/db/test_snapshot_reader.py` gained the new trailing columns. Not done here, by design: the
`event_log (entity_type, entity_key, broker_time_ms)` index (0017, S23) and the origin CHECK validation (S08 post
step). The `/live-trading-server` skill could not be invoked from the build agent (it is user-invocable only); the
changes follow `.claude/rules/dto-contracts.md` and `tier-persistence.md` as written. A proposed rule text for the new
fields is in `docs/design/proposed-claude-changes/S01-dto-contract-fields.md` (needs the user's approval; no test
depends on it). Note for S04: once the orchestrator sets `origin`, the commit path's Redis payload carries it while a
heal rebuilt from `orders_state` does not (it has no `origin` column); integrity compares version and checksum, so
this is harmless, but a payload-equality test there must ignore the labels.

* **Goal:** add every new field and column with no behaviour change: every producer still emits `None`, and every
  existing checksum stays byte-identical.
* **Skill:** `/live-trading-server` (`src/dto`, `src/pubsub`).
* **C:** `src/db/migrations/0015_arm_prereqs.sql`: `orders_state.order_kind text` (nullable, `CHECK (order_kind IN
  ('STOP','LIMIT'))`); `instruments.price_increment numeric`, `min_stop_distance numeric`, `asset_class text`;
  `event_log.origin text` with a CHECK added `NOT VALID` (validated by the S08 post step); `event_log.broker_time_ms
  bigint` (nullable; set only when the version carries a true broker time, R11-M06); `ALTER TABLE sod_marks DROP COLUMN
  IF EXISTS position_marks`. Header block, `SET client_min_messages = warning;`, idempotent.
* **M:**
  * `src/dto/entities.py`: `OrderDTO.order_kind: OrderKind | None = None`; `PriceDTO.ts_source: TsSource | None = None`,
    `received_ms: int | None = None`; `InstrumentSpec.price_increment`, `min_stop_distance` (`Decimal | None`),
    `asset_class: str | None`; non-payload `origin: Origin | None = None` and `broker_time_source: BROKER | WATERMARK |
    None` on the stateful DTOs.
  * `src/dto/versioning.py`: `NON_PAYLOAD_FIELDS` += `origin`, `broker_time_source`; new `OPTIONAL_PAYLOAD_FIELDS =
    {order_kind, price_increment, min_stop_distance, asset_class}`, skipped by `payload_checksum` while `None`.
  * `src/pubsub/validation.py` `schema_problem`: `order_kind` enum check (copy the `status` check).
  * `src/db/entity_sql.py` (`ORDERS.payload_cols`, `COL_TYPES`), `src/db/rows.py` (`ORDER_COLS`, `order_from_row`
    index 21, `INSTRUMENT_COLS`, `instrument_from_row`), `src/db/repository.py` (`SQL_INSTRUMENT_UPSERT` `$17…` and the
    `DO UPDATE SET`; `SQL_EVENT_LOG` and `_log` write `origin`, `broker_time_ms`, NULL from every caller for now),
    `src/dashboard/views.py` `ORDER_COLS` (display; `src/reports/lifecycle.py` needs no change, §1).
  * `tests/db/test_schema.py:76` scans every migration file, not only 0001 (ST-5).
* **Tests:** golden checksum fixtures for orders, positions and instruments are unchanged with the new fields `None`;
  a set `order_kind` changes the checksum; row round trip through `order_from_row` and `integrity_store` recompute
  (dbcheck) with `order_kind = STOP`; 0015 idempotency regex test; validation rejects an unknown `order_kind`.
* **Acceptance:** G; no checksum churn in the 114-trade baseline replay (`verify_execution.py`).
* **Resolves:** R11-M06 (column), R11-m (event_log index query: the `broker_time_ms` column), ST-5.
* **Risk:** a field leaking into `payload_checksum` would make integrity heal every row. The golden fixtures are the
  gate.

### S02 PR-B1: adapters, `order_kind` and `price_increment` (PR1 producers, PR7)

Status: done 2026-11-16. Built: `codes.py` `order_kind_of()` (+ `ORDER_KIND`: 2 -> STOP, 3 -> LIMIT, 1 / unknown ->
None; takes a code, a word or a parsed `StoneXOrderType`); `mapping.py` `map_stop_limit_order(..., order_kind=None)`
(the caller's typing wins, else the row's own `stop_limit_type(raw)`, which now also reads `TypeId`),
`map_trade_order` -> None; `adapter.py` `_map_open_orders` passes `order_kind_of(child.order_type)` for children nested
in a position / IfDone (typed by key); `history.py` `normalised` remaps `TypeId` to `Type` (IN-1), `_trade_order` ->
None; `orders.py` `_order()` sets `order_kind_of(memo.order_type)` (`ORDER_FIELDS` unchanged); StoneX
`instruments.py` `price_increment()` = `10^-PriceDecimalPlaces` (None without decimal places, never `point_size`);
Pepperstone `mapping.py` `order_kind()` (LIMIT -> LIMIT; STOP, STOP_LIMIT -> STOP; STOP_LOSS_TAKE_PROFIT -> STOP when
`stopPrice` is set, else LIMIT; others None) and `price_increment = 10^-digits`; `MockBrokerAdapter.set_order_kind()`,
`set_instrument_metadata()` and `fx_spec(price_increment=, min_stop_distance=, asset_class=)` (default fixtures keep
every new field None, so their checksums are unchanged). `asset_class` and `min_stop_distance` stay None on both real
adapters (no field read states them; PR3). Tests: `test_stonex_wire_codes.py` (order-kind table, `price_increment`
from decimal places, `point_size` never a fallback), `test_stonex_rest_mapping.py` (row type incl. `TypeId`, nested
children typed by key, trade order None, checksum only when set), `test_stonex_adapter.py` (open orders / by-id kinds
end to end against the fake CIAPI), `test_stonex_orders.py` (stream kinds, an item without `Type` keeps the memo's
type, a REST read teaches the type, IN-7 documented), `test_stonex_history.py` (`TypeId` rows give STOP / LIMIT on
the placement and end versions, trade rows None), `tests/adapters/pepperstone/test_order_kind_and_increment.py`
(kind table, increment from digits never `pipPosition`), `test_mock_and_contracts.py` (scripted kind / metadata). G
green: pytest 2551 passed / 29 skipped, mypy strict clean, `verify_execution.py --strict` 11 / 11.
Deviations: (1) `OrderStreamMapper.learn_order` also records a REST row's known `order_kind` as the memo's order type
(set only, never cleared), so a later ORDERS item without `Type` keeps the kind REST saw; it changes no parent / bracket
decision (`learn_order` already fixes parent-or-standalone); (2) the cTrader increment is set in
`pepperstone/instruments.py` `spec_from_symbol` (where the spec and `digits` live), not `pepperstone/mapping.py`;
(3) `map_stop_limit_order` keeps `order_kind` keyword-only with default None (derived from the row), so existing
callers are unchanged. The `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
the changes follow `.claude/rules/adapter-isolation.md` and `dto-contracts.md`. No `.claude/` change is needed for this
stage (adapter-isolation §2's `order_kind` text is part of S01's / S37's pending rule proposals). Note for S04: until
the `_label` inheritance lands, a stop child that fills reaches the orchestrator as STOP from the stream and the
stop / limit history but as None from the trade history's FILLED row, and an existing stored order gains its kind at
its next re-read (at an equal broker version the later observation wins; new checksum, so one history row each).

* **Skill:** `/live-trading-server`.
* **M:** `src/adapters/stonex/codes.py` (helper `order_kind_of(type_id)`: 2 → STOP, 3 → LIMIT, 1 → None);
  `src/adapters/stonex/mapping.py` (`map_stop_limit_order(..., order_kind)` from `stop_limit_type(raw)`, which now also
  reads `TypeId`; nested children typed by key in `attached_orders`; `map_trade_order` → None);
  `src/adapters/stonex/history.py` (the `normalised` dict at :393-396 also remaps `TypeId`, IN-1; `_trade_order` → None);
  `src/adapters/stonex/orders.py` (`_order()` sets `order_kind` from `memo.order_type`; `ORDER_FIELDS` unchanged);
  `src/adapters/stonex/instruments.py` (`price_increment = Decimal(1).scaleb(-PriceDecimalPlaces)`, never from
  `point_size`); `src/adapters/pepperstone/mapping.py` `map_order` (LIMIT → LIMIT; STOP, STOP_LIMIT → STOP;
  STOP_LOSS_TAKE_PROFIT → STOP when `stopPrice` set, else LIMIT; others None) and `price_increment` from digits;
  `src/adapters/mock.py` (scripted `order_kind`, `price_increment`, `asset_class`). `asset_class` is set only where an
  adapter field states it (recorded by PR3); None makes the position UNSUPPORTED at arming (fail closed).
* **Tests:** `tests/adapters/test_stonex_wire_codes.py` (order-type table), `test_stonex_rest_mapping.py` (stop / limit
  kinds), `test_stonex_orders.py` (an item without `Type` keeps the memo's type), `test_stonex_history.py` (`TypeId`
  rows give STOP / LIMIT), Pepperstone mapping table, instrument `price_increment` and a test that `point_size` is never
  a fallback; a test that documents IN-7 (an ORDERS item with `OpenPrice` empty re-emits the old level) as a PR3 question.
* **Resolves:** IN-1, IN-7 (documented, decided by PR3), PR1 producers, PR7 adapter side.
* **Risk:** `ORDER_FIELDS` is frozen; adding `TriggerPrice` would fail the whole subscription.

### S03 PR-B2: price stream fields, per-market events, bar capability, PR3 probe script (PR2, PR3, PR6 adapter side)

Status: done 2026-11-16. Built: `src/dto/bars.py` (`BrokerBar` with OHLC hidden from `repr`, the response's own side
tag and `partial`; `PriceBarsResponse` with `bars`, `truncated`, `received_ms`, the request form, `supported`, `note`,
`field_names`; `BarSide`, `bar_request_problem()` for the count XOR range form, R11-M15; exported from `src.dto`);
`src/adapters/base.py` (`StreamEvent.market`, `PriceBarsDecodeError`, `price_bars_supported` and the
`fetch_price_bars(symbol, side, timeframe_s, *, count, from_ms, to_ms)` default answering "not supported");
`src/adapters/stonex/streaming.py` (`FrameChannel(stamp_receipt=..., drops=...)` stamps `_xtnl_received_ms` on the
library thread through the injected clock, `MarketDropCounter` counts dropped frames and lost updates per market on
drop-mode channels, IN-13, while orders / account lost updates still open a gap; `BoundSink` on `_market` passes the
market with its events and lost updates; `LightstreamerTransport(price_unfiltered=...)` calls
`setRequestedMaxFrequency("unfiltered")` on PRICES MERGE subscriptions only when the flag is on); `src/config.py`
(`StoneXSettings.price_unfiltered` from `XTNL_STONEX_PRICE_UNFILTERED`, parsed by the new `tolerant_flag`: anything
but 1/true/yes/on is off, never a start-up error); `src/adapters/stonex/mapping.py` `map_price` (`ts_source` BROKER /
RECEIPT, `received_ms`); `src/adapters/stonex/adapter.py` (the default transport follows the flag, the price channel
stamps and counts, `stream_ticks` maps with the stamp, per-market events name the canonical symbol, the
`dropped_price_frames` property per symbol, monotonic, and `probe_price_bars()` for the probe; `fetch_price_bars`
stays the base default until S39); `src/adapters/pepperstone/adapter.py` (spot frames stamped by the socket reader,
`ts_source` / `received_ms`), `pepperstone/channel.py` (`put_event(..., market=)`); `src/adapters/mock.py`
(`fetch_price_bars` scripted per (symbol, side, timeframe) with `set_price_bars(..., max_bars=, truncated=,
field_names=)`, `fail_price_bars`, `undecodable_price_bars`, `set_price_bars_supported`, `price_bar_requests`, and the
REST priority of each call in `rest_calls`; a `wall_ms` clock); `src/adapters/stonex/probe_bars.py` (the PR3 probe,
`run(settings, adapter=...)`; see deviations). Tests: `tests/dto/test_price_bars.py`,
`tests/adapters/test_price_stream_fields.py` (`map_price` sources, the stamp on the library thread end to end,
`StreamEvent.market` for SUBSCRIBED / ERROR and None for DISCONNECTED, lost updates per market and never a gap, no
`setRequestedMaxFrequency` with the flag off and PRICES-only with it on, tolerant flag parsing, cTrader spots),
`tests/adapters/test_price_bars_capability.py` (base default on StoneX and Pepperstone, mock count / range / cap /
truncated / failing / undecodable / unsupported / priority, the StoneX probe read's GET requests and parameters,
decode errors without values, the GET-only guard refusing a read-only POST before it is sent, bar paths that cannot be
bent, the deny-list still applied), `tests/adapters/test_probe_bars.py` (the probe against the mock with an injected
clock: finality lag, time convention, range form, tick timing, no price key or value in the report; against the fake
CIAPI: bar reads GET only, no denied path; the default report path never in the repository or OneDrive; the probe
modules import no HTTP client); `tests/adapters/test_streaming_bridge.py`'s `RecordingSink` takes the new `market`
argument. G green: pytest 2603 passed / 29 skipped, mypy strict clean (202 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) new file `src/adapters/stonex/bars.py` (not in the file list): pure decoding of the candidate CIAPI
bar answers (`GET market/{id}/barhistory` count form, `GET market/{id}/barhistorybetween` range form, from the CIAPI
documentation; no XTNL reference code reads bars), plus two GET methods on `StoneXRestClient`
(`price_bar_history`, `price_bar_history_between`, market id digits only) and the `rest_get_only()` guard in
`src/adapters/stonex/rest.py`; (2) the probe reads bars through a separate `StoneXAdapter.probe_price_bars()` (same
signature) so `fetch_price_bars` stays "not supported" on StoneX until S39 (§9.1); the probe uses
`probe_price_bars` when the adapter has it, else `fetch_price_bars` (the mock); (3) the probe covers bars (1m and 15m,
bid and ask: answer field names, side tag present / matching, time convention from two votes, the hard checks
as counts, finality at +5 s / +30 s / +5 min with p50 / p99 lag over `--rounds`, the range form once per series) and a
price-stream sample (`--tick-seconds`: broker-timed share, TickDateRaw resolution, receipt minus broker p50 / p99,
per-symbol drops); the other PR3 items (roll-window comparison over several days, manual stop moves on ORDERS,
`LastChangedDateTimeUTC` on amendments, Open `LastChangedTime`, `OpenPrice` across the roll, Supabase TLS, flag on
against off) are listed in the report's `not_measured`; the default report goes to the system temp folder and the
probe refuses to write (exit 2) when that folder is inside the repository or a OneDrive folder, or when the report
would hold a price-named key or a Decimal; (4) `StreamEvent.market` carries the canonical symbol (what the S05
`priced_symbols` change uses), not the StoneX MarketId; a MarketId without a known instrument keeps the raw MarketId
(as `dropped_price_frames` does), never None, which means "whole stream"; (5)
Pepperstone emits no new per-symbol events: its one batch SUBSCRIBED keeps `market=None`, because a new per-symbol
SUBSCRIBED would today reset every symbol's snapshot tracking in the orchestrator (IN-12); per-symbol cTrader events
and cTrader per-symbol drop counts are left to S05; (6) `XTNL_STONEX_PRICE_UNFILTERED` is read in
`StoneXSettings.from_env` (tolerant) rather than inside `optional-bootstrap`, since it shapes the ingestion adapter's
own subscription; it is not added to the env examples (S36). The `/live-trading-server` skill could not be invoked by
the build agent (user-invocable only); the changes follow `.claude/rules/adapter-isolation.md` and `dto-contracts.md`.
A proposed rule text for the new capability and stream fields is in
`docs/design/proposed-claude-changes/S03-price-bars-and-stream-fields.md` (needs the user's approval; no test depends
on it). For the user: run the probe (`.venv/Scripts/python.exe -m src.adapters.stonex.probe_bars`, with the
`STONE_X_*` variables set in your own shell; about 20 minutes per round with the default 15m timeframe) and record
the results in the design's PR3 row before S39. Run it with the engine stopped, or only after confirming that StoneX
allows concurrent sessions for the read-only user: the probe opens its own LogOn and Lightstreamer session on the same
credentials, which may displace the running engine's session in single-PC mode.

Review fixes (2026-11-16): the probe's time convention now takes two independent votes: the forming bar votes only
when its date is on the bucket grid (an off-grid forming bar, e.g. dated by its last tick, is counted in
`convention_votes.partial_off_grid`), and the newest complete bar votes from the round's reads taken before the next
bucket ends (a complete bar dated T's end means 'end'; T dated by its start means 'start'); disagreeing votes give
'unknown'. With an unknown convention each round is measured under both hypotheses (`finality_lag_s_if_start`,
`finality_lag_s_if_end`, rounds `status: convention_unknown`), never as `never_final`; a round whose last read failed
is `unmeasured`, not `never_final`; a `SessionExpiredError` logs on again (`adapter.connect()`) and retries the read
once (`relogons` in the report); side tags keep their distinct raw values (bounded, short, nothing decimal-like) in
`side_tag.values` and count a mismatch only for BID / ASK / B / A / OFFER, other forms in `side_tag.form_unknown`.
The count form means `count` complete bars plus the forming one (base docstring, mock: `complete[-count:]` plus its
forming bar); a count answer cut by the mock's cap is truncated, and the StoneX probe read marks a count answer with
fewer complete bars than asked truncated. `tolerant_flag` logs a warning (name only) for a non-empty value that is not
an on / off word, and never raises. Tests: off-grid forming bar under an 'end' broker (never 'start'), both hypotheses,
failed last read, re-logon, side-tag forms, mock count semantics and cap, the flag warning, the unknown-market event,
decode errors checked with a distinctive value. G green: pytest 2617 passed / 29 skipped, mypy strict clean (202
files), `verify_execution.py --strict` 11 / 11. The `/live-trading-server` skill again could not be invoked by the
agent (user-invocable only).

* **Skill:** `/live-trading-server`.
* **C:** `src/dto/bars.py` (`BrokerBar`: bucket start, OHLC as Decimal, side tag as returned, partial flag;
  `PriceBarsResponse`: bars, `truncated`, `received_ms`; transport types only, never persisted);
  `src/adapters/stonex/probe_bars.py` (read-only PR3 probe, `run(settings, adapter=...)` like `probe.py`; writes a JSON
  report of timings, flags, field names and counts only, never a price or spread; it calls only the adapter's read
  methods, so every request goes through the adapter's existing `ReadOnlyTransport`; it builds no HTTP client of its own
  and never writes the report under the repository or OneDrive unless the user passes a path).
* **M:** `src/adapters/base.py` (`StreamEvent.market: str | None = None`; optional capability
  `fetch_price_bars(symbol, side, timeframe_s, *, count=None, from_ms=None, to_ms=None)` defaulting to "not supported",
  copied from the `fetch_market_sessions` default pattern; the range form answers R11-M15);
  `src/adapters/stonex/streaming.py` (stamp `_received_ms` in `FrameChannel.on_frame` on the library thread through an
  injected clock; `BoundSink.on_event` carries a structured `market`; `FrameChannel.on_lost` counts lost updates on
  drop-mode channels per market, IN-13; MERGE branch calls `setRequestedMaxFrequency("unfiltered")` only when
  `XTNL_STONEX_PRICE_UNFILTERED=1`); `src/adapters/stonex/mapping.py` `map_price` (`ts_source` BROKER or RECEIPT,
  `received_ms` from the stamp); `src/adapters/stonex/adapter.py` (`stream_ticks` passes the stamp; per-market
  `dropped_price_frames`); `src/adapters/pepperstone/adapter.py` (`ts_source`, `StreamEvent.market`);
  `src/adapters/mock.py` (`fetch_price_bars` scripted per (symbol, side, timeframe), including partial, truncated,
  failing and undecodable responses; records the REST priority of each call).
* **Tests:** `map_price` sources; the stamp comes from the library thread; `StreamEvent.market`; lost updates per market;
  with the flag off no `setRequestedMaxFrequency` call; mock bars; the probe runs against the mock (no network), refuses
  any non-GET or denied path, and its report contains no numeric price field.
* **Resolves:** PR2, PR3 capability, PR6 adapter part, IN-13, R11-M15 (port range form).
* **Risk:** the StoneX bar endpoint itself is not implemented here (it needs the probe, S39). The probe's candidate
  read-only endpoints come from the broker's API documentation and may be checked against the XTNL reference code,
  which is read, never edited.

### S06 PR-D: `RestPriority.BARS`, BARS cap and send-time stamp (PR11)

Status: done 2026-11-16 (review fixes applied the same day). Built: `src/adapters/ratelimit.py` (`RestPriority.BARS
= 2`, DLQ 3, POLL 4; labels still from the names; reserve 0.15 and max wait 15 s in the default maps; the per-adapter
BARS cap in `acquire`: a sliding 60 s window of admitted BARS calls, a call at the cap refused at once with
`TransientBrokerError` and counted in `refused` and `bars_capped`, a waiter that times out or is cancelled gives its
place back, `spend()` is not capped; `default_bars_per_min(rate)` = a quarter of the rate per minute, StoneX 75,
cTrader 300; the cap is clamped to `max_bars_per_min(rate)` = half the rate per minute, with a warning naming
`*_REST_BARS_PER_MIN` only; `bars_per_min` is a settable property; ContextVar `_SENT_MS` holds the earliest and the
newest send since `clear_rest_sent_ms()`, read by `rest_sent_ms()` (earliest) and `rest_last_sent_ms()` (newest), set
by `mark_rest_sent()` / `RestBudget.mark_sent()` and by `acquire()` / `spend()` unless `stamp=False`;
`note_result(priority, ok)` and `RestOutcome` (ok, failed, consecutive failures, last success / failure wall time,
plus `throttled` / `last_throttled_ms` from `note_throttled(priority)`) per priority in `RestBudgetStats.outcomes`,
plus `bars_per_min`, `bars_in_window`, `bars_capped` and `failing_at_every_priority(now_ms, within_ms=60_000)` for
S31; an injectable `wall_ms`); `src/adapters/stonex/rest.py` `_send` (token with `stamp=False`, stamp right before
the send after the session check, so a session lost while waiting for a token leaves no stamp; unmetered sends stamp
from an injectable `wall_ms`; outcome per request: timeout, network error and 5xx are failures, a throttle answer is
counted apart, any other answer is ok); `src/adapters/pepperstone/session.py` `request` (token with `stamp=False`,
stamp right before the send, after the historical-request spacing and only while the socket is open; outcome per
request: timeout and socket lost are failures, REQUEST_FREQUENCY_EXCEEDED is counted apart, other error answers ok);
`src/config.py` (`rest_bars_per_min` on both settings from `STONE_X_REST_BARS_PER_MIN` / `CTRADER_REST_BARS_PER_MIN`,
tolerant: an unreadable or < 1 value logs a warning with the name only and keeps the default); the adapters' own
default buckets and `src/runtime/wiring.py` `RestBudgets.for_plan` pass the cap (a changed rate or burst replaces the
bucket; a cap-only change is set in place, keeping tokens, `requests_total` and the outcome counters);
`src/adapters/mock.py` stamps from its wall clock when unmetered; `deploy/observability/metrics.md` names the `bars`
label value. Tests: `tests/adapters/test_rest_budget_and_sockets.py` (order with BARS, BARS / CASH_FLOWS / DLQ
reserves, the four existing classes' floors and waits unchanged, cap refusal at once and after a minute, a refused
or cancelled BARS waiter frees its place, default cap, cap clamped to half the rate, tolerant setting, stamp after
the token wait and not on a refusal, the earliest and newest stamps, a two-account StoneX `fetch_open_orders` whose
requests each wait for a token stamped at its first send, no stamp when the StoneX session is lost while waiting,
the unmetered StoneX client's injected clock, StoneX calls by priority with the stamp reaching the caller through the
adapter, outcomes at BARS only versus a network outage at every priority, the two stale-outcome cases (an hour-old
success or failure never decides the verdict), throttles never read as an outage, cTrader outcomes, a cTrader
historical request stamped after its spacing sleep, a cTrader timeout and a socket lost mid-request each a failure
at the caller's priority, mock stamp; the existing order test now also shows `bars: 0` with no BARS caller),
`tests/runtime/test_telemetry.py` (`priority="bars"` rendered, every line in `CONTRACT`),
`tests/runtime/test_wiring_65.py` (cap from the settings, unreadable value keeps the bucket, a cap-only change keeps
the bucket and its counters, a clamped cap is stable). G green: pytest 2644 passed / 29 skipped, mypy strict clean
(202 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) the cap setting lives in `src/config.py` beside the other REST limits (`*_REST_BARS_PER_MIN`, not in
the plan's file list), parsed tolerantly so a bad value never stops start-up (§2), and clamped to half the bucket's
rate in `RestBudget` (a larger value would quietly disable the cap); it is not added to the env examples (S36);
(2) `src/observability/metrics.py` and `deploy/observability/gen_alerts.py` need no change: they declare label names,
not values, and `xtnl_rest_requests_total` already has `priority`; no new metric was added (the outcome counters are
read by S31's `AdapterHealthPortImpl`, not exported yet); (3) `rest_sent_ms()` is the EARLIEST request's send time
since the caller's `clear_rest_sent_ms()` (an adapter call that sends several requests, such as StoneX
`fetch_open_orders` over several trading accounts plus market lookups, is aged from its first request, so the PR9 /
IN-5 read age is never understated; `rest_last_sent_ms()` gives the newest); it is visible to the caller only
through awaits in the same task, so S04 / S31 clear it before the read and read `rest_sent_ms()` after; the REST
layers stamp only a request that really leaves; (4) `ok` in the outcome counters means "the broker answered": 4xx
answers (not found, session expired, a denied request) are ok, since they say nothing about an outage; a throttle
answer (HTTP 429 / 5002 / REQUEST_FREQUENCY_EXCEEDED) is counted apart (`throttled`, `last_throttled_ms`) and
never touches the success / failure record, because a throttle our own BARS use caused empties the bucket for every
priority and must not read as a broker outage; (5) `failing_at_every_priority(now_ms, within_ms=60_000)` judges only
priorities whose newest success or failure is inside the window: None (unknown) when no non-BARS priority has one
(S31 then neither counts BARS failures toward FAILED nor pages `broker_unavailable`), True when the newest outcome of
every recent priority is a failure, else False; (6) the PR3 probe (`probe_bars.py`) keeps reading at POLL, so the
BARS cap never limits it. The `/live-trading-server` skill could not be invoked by the build agent (user-invocable
only); the changes follow `.claude/rules/adapter-isolation.md` and `operations.md` §15. A proposed rule text for §15
is in `docs/design/proposed-claude-changes/S06-rest-bars-priority.md` (needs the user's approval; no test depends
on it).

* **Skill:** `/live-trading-server`.
* **M:** `src/adapters/ratelimit.py` (`BARS = 2`; DLQ 3, POLL 4; reserve 0.15 and max wait 15 s in the default maps;
  per-adapter BARS-per-minute cap in `acquire`, refused with `TransientBrokerError`; new ContextVar `_SENT_MS` set right
  after a successful `acquire` (or by the REST layer right before the send), read by `rest_sent_ms()` (earliest send); per-priority outcome counters `note_result(priority, ok)` with
  last-success time and consecutive failures exposed in `RestBudgetStats`, which S31's `AdapterHealthPortImpl` reads for
  the "REST failing at every priority" test of R11-M10); `src/adapters/stonex/rest.py` `_send` (sets the stamp after
  `budget.acquire()`, IN-5, and reports each outcome); Pepperstone REST path likewise (`src/adapters/pepperstone/session.py:155`);
  `src/runtime/wiring.py:401` (cap setting);
  `src/observability/metrics.py`, `deploy/observability/metrics.md:96` (label `bars`),
  `deploy/observability/gen_alerts.py:119`.
* **Tests:** `tests/adapters/test_rest_budget_and_sockets.py` (priority order with BARS, reserves, cap refusal, stamp
  set after the token wait, StoneX calls by priority; per-priority outcome counters: failures at every priority versus
  only at BARS); `tests/runtime/test_telemetry.py` CONTRACT. With no BARS caller (flags off) the order and reserves seen
  by RECONCILE, CASH_FLOWS, DLQ and POLL are unchanged (test).
* **Resolves:** PR11, IN-5, R11-M10 (input signal).
* **Risk:** grep for numeric uses of priorities; labels come from names, so dashboards are unchanged.

### S04 PR-C1: orchestrator versions, origin, `reread_entity` result, stop-order list read (PR1, PR8, PR9 base)

Status: done 2026-11-16. Built: `src/pubsub/orchestrator.py` (`AdapterRuntime.order_kind`, a plain dict seeded in
`_seed_broker_seq` from the stored orders; `_label` takes `origin`, gives an incoming None the known kind before the
version compare through `_order_kind`, re-seals the checksum, and sets `broker_time_source`; two different known kinds
page `order_kind_conflict` once per entity through `_episodes` and store None; `_accept_entity(..., origin)` now wraps
`_accept_detail`, which returns the labelled row and the in-memory version decision (IN-4); origins: stream frames
`stream`, `run_snapshot` and its by-id re-reads `snapshot`, catch-up history rows `history` and its by-id rechecks
`snapshot`, `poll_account` `snapshot`; `RereadStatus` APPLIED / NOOP / DROPPED_STALE / NOT_FOUND / TRANSIENT,
`RereadResult(status, dto, version, sent_ms)` with `found`; `reread_entity(..., origin="dlq")` clears the send stamp
before its read and reports `rest_sent_ms()` after it (IN-5); new `reread_stop_orders(adapter_id, origin="stop_read")`
returning `StopReadResult`: one `fetch_active_stop_orders()` at REST class BARS, rows applied one by one at the read's
send time, rows of another adapter skipped, nothing absent closed, re-read or touched); `src/adapters/base.py`
(`ActiveStopOrders` with `orders`, `positions`, `supported`, `note`; the optional `fetch_active_stop_orders()`
defaulting to `supported=False`); StoneX `fetch_active_stop_orders` (one `GET order/activestoplimitorders` per
trading account inside `rest_get_only`, mapped by `_map_open_orders`, typed STOP / LIMIT, children linked); Pepperstone
(the open positions from one reconcile request, POSITION_FIELD handles); `MockBrokerAdapter` (typed working orders, or
open positions with `set_stop_orders_supported(True, positions=True)`; `set_stop_orders_supported(False)`,
`stop_order_reads`, the priority in `rest_calls`); `src/integrity/bridge.py` maps the result back to `StatefulDTO |
None` (the broker's row whenever it had one, as before; origin `dlq`), `src/integrity/ports.py` docs;
`src/runtime/session.py` (the two account refreshes pass `snapshot`; `_DlqPriorityBridge` documents `dlq`, set by the
bridge); `src/db/repository.py` (`version_labels(dto)`; the APPLIED and DROPPED_STALE `event_log` rows write `origin`
and `broker_time_ms`, the latter only for `BROKER`; cash flows and instruments still NULL); `src/dto/versioning.py`
comment. Tests: `tests/pubsub/test_orchestrator.py` (stream-then-REST and REST-then-stream keep STOP with the STOP
checksum; conflict pages once and stays None; origin on every path incl. catch-up history / rechecks and the poll;
untimed REST row WATERMARK with `broker_time_ms` NULL, timed BROKER, local-time accounts WATERMARK, checksums
unchanged; `reread_entity` statuses incl. TRANSIENT for a parked row; send time of its own read; `reread_stop_orders`
one list read at BARS, no by-id read, an absent cancelled order stays WORKING, NOOP on repeat, position handles,
unsupported, foreign rows), `tests/pubsub/test_ingestion_71.py` (kind inherited after a restart, also by a later stream
row; a stored order without a kind seeds nothing), `tests/adapters/test_active_stop_orders.py` (base default; StoneX
one GET per trading account, typed, never a denied path or a by-id read, an unmappable row counted, the method builds
no HTTP client; cTrader one reconcile; mock), `tests/integrity/test_bridge.py` (result mapped back, TRANSIENT still
converged, NOT_FOUND None), `tests/runtime/test_wiring_65.py` (account refreshes `snapshot`, DLQ bridge `dlq`),
`tests/db/test_arm_prereq_rows.py` (the S01 "NULL from every caller" test now checks that only labelled entity versions
write labels); the old `reread_entity` callers in `tests/pubsub/test_orchestrator.py` and `tests/e2e/scenario.py` use
the result. Existing DLQ / SM2 tests unchanged and green. G green: pytest 2665 passed / 29 skipped, mypy strict clean
(202 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) `broker_time_source` is `BROKER` only when the type declares broker time and the raw time is non-zero;
everything else is `WATERMARK`, including the local-time domain (StoneX accounts), so `WATERMARK` means "not a real
broker time" (the `BrokerTimeSource` comment says so); (2) a `RereadStatus` is the orchestrator's in-memory version
compare at publish time (an entity evicted from the LRU reads APPLIED; the store repeats the compare against
PostgreSQL); TRANSIENT means "read but not handed over" (parked for any reason, or no `broker_seq` to inherit), and its
`dto` is the broker row as read, so the DLQ bridge returns exactly what it returned before; broker errors still
propagate from `reread_entity` / `reread_stop_orders` (the DLQ worker's and the readiness check's failure handling is
unchanged); (3) `reread_stop_orders` sets REST class BARS itself (design PR9) rather than leaving it to the S27 / S31
caller, and returns a `StopReadResult` (one `RereadResult` per row, `sent_ms`, `skipped`); (4) the StoneX stop read
lists only `activestoplimitorders` (not the open positions' attached children, which `fetch_open_orders` adds from a
second read): a child absent from the active list is simply not listed, and never closed; the unknown-market lookup
(`market/information`, a read-only POST) runs outside `rest_get_only`; (5) a conflicted entity stays None for the rest
of the term (sticky); only non-terminal stored orders are seeded after a restart, so a history FILLED row without a type
can still store None for an order finished while the engine was off (the S02 note; harmless for PR1, which judges
working children); (6) the labels now also appear in the JSON payloads written to `event_log.payload`, the outbox and
Redis (encoded when set, S01 `OMITTED_WHEN_NONE`); integrity compares version and checksum only, so a heal rebuilt from
`orders_state` (no label columns) is not a mismatch. The `/live-trading-server` skill could not be invoked by the build
agent (user-invocable only); the changes follow `.claude/rules/dto-contracts.md`, `adapter-isolation.md` and
`tier-persistence.md` as written. A proposed rule text is in
`docs/design/proposed-claude-changes/S04-orchestrator-labels-and-stop-reads.md` (needs the user's approval; no test
depends on it). Nothing calls `reread_stop_orders` yet (S27 / S31, behind `XTNL_ARM_STOP_READS`, off by default).

* **Skill:** `/live-trading-server` (`src/pubsub`, `src/integrity`).
* **M:** `src/pubsub/orchestrator.py`:
  * `rt.order_kind` dict (not the LRU), seeded in `_seed_broker_seq` / `_load_local`; `_label` gives an incoming None the
    stored known kind before the version compare (copy :732-734); two different known kinds page `order_kind_conflict`
    through `_episodes` and store None.
  * `origin` threaded through `_accept_entity(..., origin)` into `_label`'s `replace(...)`, with
    `broker_time_source = BROKER` when the raw broker time was non-zero, else `WATERMARK` (IN-3). Paths: stream
    (`handle_stream_item`), snapshot (`run_snapshot` and its by-id re-reads), history (catch-up), the catch-up by-id
    rechecks at :1360 / :1374 → `snapshot`, account poll → `snapshot`.
  * `reread_entity(..., origin: Origin = "dlq")` returns `RereadResult(status APPLIED | NOOP | DROPPED_STALE |
    NOT_FOUND | TRANSIENT, labelled DTO, version_str, sent_ms)`; `_accept_entity` exposes the labelled DTO (IN-4).
    `sent_ms` comes from `rest_sent_ms()` (S06, IN-5). Every existing caller is updated in this stage: the integrity
    bridge (`src/integrity/bridge.py:29`, origin `dlq`, mapped back to its `StatefulDTO | None` contract so
    `src/integrity/dlq_recovery.py:171` is unchanged), `_DlqPriorityBridge` (`src/runtime/session.py:645`, `dlq`), and
    the account refreshes at `src/runtime/session.py:1782` and `:1946` (`snapshot`, result ignored as today).
  * New `reread_stop_orders(adapter_id, origin="stop_read")`: one list read per adapter, applied row by row through
    `_accept_entity`, never closing an entity absent from the list (R11-M20). Adapter capability
    `fetch_active_stop_orders()` (optional, read-only) in `src/adapters/base.py`; StoneX implementation over
    `rest.py` `active_stop_limit_orders(trading_account_id)` per trading account; Pepperstone returns its open positions'
    stop fields (POSITION_FIELD handles).
* **M:** `src/integrity/bridge.py` and `src/integrity/ports.py` docs (the DLQ bridge keeps its bool contract through an
  adapter on `RereadResult`); `src/runtime/session.py` `_DlqPriorityBridge` passes `origin="dlq"`;
  `src/db/repository.py` `_log` writes `origin` and `broker_time_ms` (the DTO's broker time only when
  `broker_time_source = BROKER`).
* **Tests:** `tests/pubsub/test_orchestrator.py` (stream-then-REST and REST-then-stream on one StoneX child keep STOP;
  conflict page; origin per path; untimed REST row → `WATERMARK` and `broker_time_ms` NULL);
  `tests/pubsub/test_ingestion_71.py` (`order_kind` inherited after a restart); `reread_entity` statuses; existing DLQ
  tests unchanged; `reread_stop_orders` never closes an absent order.
* **Resolves:** IN-3, IN-4, IN-5 (with S06), IN-6 / R11-M20 (orchestrator part), R11-M06 (marker), R11-m
  (`reread_entity` callers and default origins).
* **Risk:** DLQ check-before-retry semantics must not change; the bridge adapter is covered by the existing SM2 tests.

### S05 PR-C2: orchestrator tick-only set and per-market subscriptions (PR5, PR6)

Status: done 2026-11-16. Built: `src/pubsub/orchestrator.py` (per adapter `md_tick_symbols` / `md_subscribed` /
`md_pending` on `AdapterRuntime`; `set_md_tick_symbols(adapter_id, symbols)`, `md_tick_symbols()`, `is_md_only()`;
the prices stream starts with `required ∪ md` (`_stream_symbols`) while `rt.tick_symbols` / `pending_symbols` /
`required_tick_symbols()` stay the held set, IN-8; `flush_md_symbols()` runs the tick-only `ensure_ticks()` in a call
of its own after the required one, never raises, retried after 60 s on failure; per-market SUBSCRIBED intervals
`price_sub_since` from `StreamEvent.market` (a whole-stream SUBSCRIBED opens every requested market, a repeated one
restarts the interval; a per-market UNSUBSCRIBED / ERROR, a whole-stream one, `_lose_all`, the end of the prices pump
and its restart in `start_missing_streams` close them; `hold_fence` never does), `price_subscriptions()`; constructor
hook `on_price_subscription(adapter_id, market, subscribed, ts_ms)` in its own try/except, its log throttled per
adapter; `_on_control` resets only the event's market's snapshot tracking when `market` is set, IN-12;
`dropped_prices_by_symbol` and `dropped_prices_by_market()` (the orchestrator's drops plus the adapter's
`dropped_price_frames`)); `src/runtime/session.py` (`md_schedule_symbols`, a callable defaulting to none, and
`_schedule_symbols()` = `_wanted_symbols ∪ md_schedule_symbols()` given to `ScheduleRefresher`, a raising callable
logged and left out, IN-11; `_on_price_tick` skips SM10 evidence for a tick-only symbol; `MarketCalendar` unchanged);
`src/adapters/pepperstone/adapter.py` + `channel.py` (the cTrader part S03 left to S05: `ensure_ticks()` reports each
symbol newly subscribed on the live stream with its own SUBSCRIBED naming it; `EventChannel(on_drop=...)` and the
adapter's `dropped_price_frames` per canonical symbol). Tests: `tests/pubsub/test_runtime_hooks.py` (tick-only set at
stream start and never in the held sets, its ticks to the hook and never the topic, a symbol that becomes held is
ordinary, flags-off stream unchanged, separate ensure call, failing ensure retried without gap / resync / state change,
stale-price and clean-mid watches ignore it, a tick-only market's ERROR never opens the gap while a required one's
does, one market's SUBSCRIBED keeps another's snapshot tracking, intervals and hook timestamps, pump restart closes
intervals, no hook from the catch-up hold or its release, a raising hook never blocks and logs once, drops per market),
`tests/runtime/test_md_tick_symbols.py` (a tick-only symbol OPEN on a Saturday while the held FX symbol is CLOSED
leaves `market_open(adapter)` and liveness unchanged, with a held control; never SM10 evidence; the refresher asks for
the schedule of `md_schedule_symbols()`; a failing callable keeps the held schedules),
`tests/adapters/pepperstone/test_adapter.py` (the per-symbol SUBSCRIBED from `ensure_ticks()`, none when already
subscribed; per-symbol drop counts). Review fixes (2026-11-16): with no required symbol the prices stream starts with
the empty required set, the tick-only symbols wait for their own `ensure_ticks()` (`md_pending`) and the stream is
trivially subscribed by the orchestrator when tick-only symbols are in play (`price_required_asked`; flags off the
adapter's own SUBSCRIBED decides, as before), so a bad or silent tick-only market on a flat account never sends SM4
to BACKOFF; a removed tick-only symbol moves to `md_retired` (never reset: the adapters subscribe every market they
ever had again at each start) and stays tick-only (not published, no gap, no SM10 evidence) unless held or wanted
again; a tick-only SUBSCRIBED never marks the stream subscribed after a required-market or whole-stream
UNSUBSCRIBED / ERROR or a socket loss (`price_stream_failed`, reset at the pump start); a whole-stream SUBSCRIBED opens
intervals only for the adapter's `subscribed_price_markets` (new on `StoneXAdapter` and `PepperstoneAdapter`; the
asked-for set when an adapter has none); `flush_md_symbols` re-raises `SessionExpiredError` and the STREAMING step
loses the session as on the required path; every interval timestamp is the orchestrator's `_clock_ms()` (never
`ev.at_ms`); cTrader `stream_ticks` subscribes the asked-for spots first and the kept ones (earlier `ensure_ticks`)
in a call whose broker refusal is only logged. Tests added in `tests/pubsub/test_runtime_hooks.py` (flat account with
a bad / silent tick-only market reaches STREAMING before and after a reconnect; flags-off flat stream unchanged; a
removed symbol after a prices restart; a tick-only SUBSCRIBED after a required ERROR leaves the gap open; session
expiry in the tick-only ensure; whole-stream intervals from `subscribed_price_markets`),
`tests/adapters/pepperstone/test_adapter.py` (a refused kept spot never fails the stream) and
`tests/adapters/test_stonex_adapter.py` (`subscribed_price_markets`). G green: pytest 2689 passed / 29 skipped, mypy
strict clean (202 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) a tick-only symbol's tick is not published to `market.price.dto` (it reaches `on_price_tick` only):
otherwise the price consumer would write Redis price keys and `last_clean_marks` rows for it (new price data in
PostgreSQL, §1); the S14 tick path reads ticks through the price hook (S09 composite hook), so nothing is lost; a symbol
that is also held or a needed cross is published as before; (2) `set_md_tick_symbols` is per adapter
(`set_md_tick_symbols(adapter_id, symbols)`), and a removed symbol stays subscribed for the adapter's life (no
read-only unsubscribe path; it stays tick-only as `md_retired`); (3) a per-market UNSUBSCRIBED / ERROR on a tick-only market closes its interval and logs, but never
opens the prices gap, and a tick-only market's SUBSCRIBED does not move the stream's `subscribed_seq` once the stream
is subscribed, so a bad tick-only symbol can never send SM4 to RESYNCING or BACKOFF; a required market's events behave
as before; (4) `session._on_price_tick` skips SM10 evidence for tick-only symbols (otherwise a scheduled tick-only
symbol could reach OPEN_BY_EVIDENCE, request an open RESYNCING or page `calendar_mismatch`); (5) cTrader per-symbol
SUBSCRIBED events and per-symbol drop counts (left to S05 by S03) are in `src/adapters/pepperstone/` (not in the file
list); a symbol whose cTrader subscribe fails is still only logged (no per-symbol ERROR, as before) and gets no
interval (not in `subscribed_price_markets`); (6) open point for S32, recorded in the proposed rule
file: once `md_schedule_symbols` is wired, tick-only symbols have stored schedules and, like any previously held symbol
whose schedule is still stored, take part in `latest_open_ms`, the open-transition RESYNCING and `deciding_sessions`'s
fallback to every scheduled symbol; S32 must keep them out of those paths or accept it before `XTNL_MD_ENABLED=1`;
(7) flags off, a newly held symbol's cTrader `ensure_ticks()` now sends a per-market SUBSCRIBED (before S05 nothing):
it opens that market's interval and, like every required market's SUBSCRIBED (StoneX's per-market acks already did
this before S05), bumps the prices stream's `subscribed_seq`, so a REST pass already in flight cannot clear an open
prices gap and the next one does (one pass later, never earlier: conservative, kept on purpose).
The `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); the changes follow
`.claude/rules/adapter-isolation.md` §5, `dto-contracts.md` §4 and `operations.md` §10 as written. A proposed rule
text is in `docs/design/proposed-claude-changes/S05-tick-only-symbols.md` (needs the user's approval; no test depends
on it). Nothing calls `set_md_tick_symbols` or passes `on_price_subscription` yet (S31 / S32, behind the flags).

* **Skill:** `/live-trading-server`.
* **M:** `src/pubsub/orchestrator.py`: separate `md_tick_symbols` set with `set_md_tick_symbols(symbols)`; passed to
  `stream_ticks(...)` at stream start (:1576) and to `ensure_ticks`, never merged into `rt.tick_symbols` /
  `pending_symbols` (IN-8); per-market SUBSCRIBED intervals from `StreamEvent.market`; constructor hook
  `on_price_subscription(adapter_id, market, subscribed, ts)` beside `on_price_tick`, in its own try/except, never fired
  from `hold_fence` or `_lose_all` unless the socket is really lost; `_on_control` clears `priced_symbols` only for the
  event's market when `market` is set (IN-12); `dropped_prices` per market.
  `src/runtime/session.py`: `ScheduleRefresher(wanted_symbols=...)` gets `_wanted_symbols ∪ md_schedule_symbols()`
  (a callable, empty until S32) so tick-only and pre-warm symbols get a schedule (IN-11); `MarketCalendar
  (subscribed_symbols=...)` unchanged. `XTNL_TICK_SYMBOLS` is parsed in S31 (≤ 8 per adapter, checked against
  `instruments`).
* **Tests:** a tick-only symbol OPEN while every held symbol is CLOSED leaves `market_open(adapter)` unchanged; the
  stale-price watch ignores it; its schedule is fetched; one market's SUBSCRIBED does not reset another's snapshot
  tracking; the subscription hook is not fired from the catch-up hold; a raising hook never blocks
  (`tests/pubsub/test_runtime_hooks.py`).
* **Resolves:** PR5, PR6 orchestrator part, IN-8, IN-11, IN-12.

### S07 PR-E: Oversee `effective_stop_detail()` (PR1 Oversee side)

Status: done 2026-11-16 (built and green; **merge still waits for the user's confirmation of the Oversee change**,
§9.2 last item / design §0 C4, and the `.claude/` rule text for approval). Built: `src/oversee/risk.py`
(`StopHandle(kind CHILD_ORDER | POSITION_FIELD, id, level, units, order_kind)`, `StopDetail(level, handles,
caveats)`, `STOP_KIND_UNKNOWN` / `STOP_KIND_INCONSISTENT` / `STOP_CAVEATS`, `effective_stop_detail(e, positions,
orders, mark=None)`: `stop_loss` first as one POSITION_FIELD handle, else the covering non-terminal linked
reduce-only children with STOP at any price, LIMIT never, None only strictly on the loss side; the most distant
level; `effective_stop()` now returns `detail.level` with its signature unchanged; `risk()` keeps the PENDING /
WORKING wrong-side raise, which now also catches a STOP child beyond an unfilled entry); `src/oversee/rules.py`
(`stop_mark(ctx, e)`: the pass's MarkBook mark through `select_mark` without recording a stale symbol;
`MissingStopLossRule` returns `Unknown("stop_kind_inconsistent", "stop child <ids>")` for a caveated detail, which
`_step_unknown` holds for `unknown_grace_ms` (30 s) and then turns into a capped SOFT state; no new timer, column or
config); `src/oversee/sweep.py` (`stop_watch_changes` uses the detail). Tests: `tests/oversee/test_risk.py`
(break-even STOP child counts and risks 0, short side, LIMIT never, None child with caveat and None at entry not
counted, partial coverage None and two children covering, terminal / unlinked children, cTrader POSITION_FIELD
handle, `stop_loss` wins, the triggerable side per side / mark quality, no judgement without a usable mark, the
children of an unfilled entry not judged, weekend close against the last mark, a PENDING / WORKING entry with a STOP
child beyond entry is no stop (`no_stop`, as before S07) while a PARTIALLY_FILLED one keeps it, a `stop_loss` beyond an
unfilled entry still UNKNOWN, `inconsistent_ids` names only the inconsistent child, a market entry order with a STOP
child, a seeded property test `effective_stop == effective_stop_detail(...).level` == an independent oracle of the
spec, with the `stop_kind_unknown` caveat checked, over 400 generated entity sets × 4 marks),
`tests/oversee/test_stop_detail_sweep.py`
(rule level: break-even stop present, mislabelled take-profit UNKNOWN, weekend close, no mid, cTrader stop,
LIMIT / partial child `missing_stop_loss` after 2 s, an unfilled entry with a STOP child beyond entry is uncapped
`missing_stop_loss` with a 5 s old `stop_watch` row while position and aggregate risk stay known, the UNKNOWN detail
names only the inconsistent child, `stop_mark` records nothing; RiskSweep with `now_ms` stepping:
a touched stop is UNKNOWN with no state, page or `stop_watch` row until the 30 s grace, then a capped SOFT
`stop_kind_inconsistent` and halt SOFT, resolved by the fill; a touched stop that fills inside the grace never pages;
a mislabelled take-profit capped SOFT never escalates; a partial child is `missing_stop_loss` after 2 s);
`tests/oversee/helpers.py` (`order(..., kind=)`). G green (after the review fixes): pytest 2727 passed / 29 skipped, mypy strict clean
(202 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) the `mark` parameter is the existing `Mark` (MarkBook's mark), not a `TimedQuote` (no such type
exists); judged qualities are CLEAN and LIQUIDATION (a fresh mark while OPEN) and CLOSED_LAST_CLEAN (the last mark
while closed); LAST_MID (OPEN with no quote at all past `stale_mark_ms`, or Redis down) and no mark at all are not
judged, since that case is already `stale_mark` / capped; (2) the triggerable-side test applies to positions only: a
still-unfilled entry order's children are not live (a buy-stop entry above the market legitimately has its stop child
above the market), and `risk()` checks those against the entry; (3) a touch is "at or beyond the mark" (inclusive);
(4) `stop_kind_unknown` is set whenever a linked candidate child of kind None is present, counted or not (a detail
of None carries no caveats); (5) the sweep passes the MarkBook mark through `EvalContext` to `MissingStopLossRule`
(`stop_mark`) rather than to `stop_watch_changes`: the `stop_watch` deadline only needs whether a level exists, and
the caveat's wait is the UNKNOWN grace, not that deadline; (6) the UNKNOWN detail names only the inconsistent child
ids (`StopDetail.inconsistent_ids`), never a price; (7) review fixes 2026-11-16: a STOP child strictly beyond the entry
of a PENDING / WORKING entry order is not counted (pre-S07 behaviour; counting it made `risk()` UNKNOWN and blinded
`MaxAggregateOpenRiskRule` for the account), the side is judged at the mid while a clean quote is fresh (a stop between
the bid / ask and the mid is caught a little late, never falsely), and the §9.2 note lists the behaviour changes. The `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); the changes
follow `.claude/rules/oversee-rules.md` §2, §5, §6 as written. The rule text is proposed in
`docs/design/proposed-claude-changes/S07-effective-stop-detail.md` (needs the user's approval; no test depends on it).
This stage changes live Oversee monitoring on purpose (a STOP child at or beyond entry counts, except beyond an
unfilled entry; a LIMIT child no longer counts; a STOP child on the wrong side of the mark is UNKNOWN for 30 s, then a
capped SOFT that never escalates where it used to escalate to HARD); nothing is behind the ARM flags.

* **Gate:** the user confirms the §9.2 Oversee behaviour change (a STOP child at or beyond entry counts as a stop)
  before this stage merges, since it changes live monitoring regardless of the ARM flags. Until then S08-S20 and
  S23-S25 proceed; S21 waits for it.
* **Skill:** `/live-trading-server`.
* **M:** `src/oversee/risk.py`: `effective_stop_detail(e, positions, orders, mark: TimedQuote | None = None) ->
  StopDetail | None` with `level`, `handles` (kind CHILD_ORDER | POSITION_FIELD, id, level, units, order_kind) and
  `caveats`. Rules: `stop_loss` first; else covering non-terminal linked reduce-only children; STOP counts at any price;
  LIMIT never; None keeps the loss-side test (:278) with caveat `stop_kind_unknown`; a STOP child on its
  non-triggerable side against a fresh mark while OPEN (against the last mark while closed) keeps its level and adds
  caveat `stop_kind_inconsistent`. `effective_stop()` returns `detail.level` (signature unchanged).
  `risk()`: check the PENDING / WORKING raise at :319-321 with kind rules on.
  `src/oversee/rules.py` `MissingStopLossRule`: the `stop_kind_inconsistent` caveat yields UNKNOWN
  (`stop_kind_inconsistent`), which goes through the existing durable `unknown_since` / `unknown_grace_ms` (30 s, then
  capped SOFT and paged). No new timer, no new column (R11-M07).
  `src/oversee/sweep.py`: passes the MarkBook mark; `stop_watch_changes` uses the detail.
  `.claude/rules/oversee-rules.md` §2, §5, §6 text (user approval).
* **Tests:** `tests/oversee/test_risk.py` (a STOP child at break-even counts, the "disappears" fix; LIMIT never; None
  child with caveat; partial coverage → None); a sweep test with `now_ms` stepping shows UNKNOWN only after the 30 s
  grace and a capped SOFT state; property test `effective_stop == effective_stop_detail(...).level` over generated
  entity sets; the design §C3 PR1 cases: a break-even STOP child across a weekend close (judged against the last mark),
  a touched stop awaiting fill (UNKNOWN only after the 30 s grace, never at once), a take-profit mislabelled as STOP on
  the wrong side (caveat, then UNKNOWN), a cTrader position stop (POSITION_FIELD handle), a child covering only part of
  the units (Oversee `missing_stop_loss`).
* **Resolves:** R11-M07, IN-2, PR1 Oversee part.
* **Risk:** this changes live risk monitoring on purpose (a STOP child at or beyond entry now counts as a stop). The
  baseline replay in `verify_execution.py` and `tests/oversee` are the gate; the change is called out to the user.

### S08 PR-F: outbox 0016, migrate post step, savepoints, dead-man optional fields (PR4 base)

Status: done 2026-11-16. Built: `src/db/migrations/0016_outbox_arm.sql` (`expires_at_ms`, `superseded_by` nullable
bigint, no default; `notification_outbox_arm_due_idx` `ON ONLY` the parent; `notification_outbox_arm_channels_check`
in the design's `cardinality` / `array_to_string` form, `NOT VALID`, guarded by a catalog check). `src/db/migrate.py`:
`post_step()` (no transaction, after every file and on every run, under the runner lock): registry `POST_STEP` of
`PartitionIndex` / `ValidateCheck` items (0015 `event_log_origin_check`, the 0016 index and CHECK), partitions from
`pg_inherits` (DEFAULT included), per partition `CREATE INDEX CONCURRENTLY IF NOT EXISTS <partition>_arm_due_idx` (an
INVALID leftover dropped first) and `ALTER INDEX ... ATTACH PARTITION`, `VALIDATE CONSTRAINT` unless `convalidated`;
session `lock_timeout 2s`, the file retries, identifiers checked. `src/db/notify_store.py`: `kind <> 'arm'` in
`SQL_OUTBOX_PENDING`, `SQL_OUTBOX_DUE`, `SQL_OUTBOX_OLDEST_UNSENT` (insert and escalated unchanged).
`src/oversee/memory.py` `pending` filter with `ARM_OUTBOX_KIND` (`src/oversee/notify.py`). `src/db/fence.py`:
`FencedTx.savepoint()` (`SAVEPOINT` / `RELEASE` / `ROLLBACK TO` + `RELEASE`, one name, LIFO nesting).
`src/runtime/deadman.py`: `ArmFailedBeat(count, global_failed)`, optional `DeadmanPayload` fields
`arm_oldest_undelivered_notification_age_ms`, `arm_overdue_positions`, `arm_failed` (default None),
`OPTIONAL_PAYLOAD_FIELDS`, three `DeadmanSource` getters read by `build_payload`; `src/runtime/session.py`
`_DeadmanView` getters return None until S32. `src/deadman_receiver/evaluate.py`: `OPTIONAL_FIELDS`, `Beat` fields
(absent or null = None, values validated), conditions `arm_failed` (count > 0), `arm_overdue_positions` (> 0), both at
once, and `arm_notification_undelivered` (age > `Thresholds.arm_notification_age_ms`). `tests/db/pg_emulator.py`:
savepoints, `SQL_OUTBOX_DUE` / `SQL_OUTBOX_OLDEST_UNSENT` / `SQL_OUTBOX_FINISH`, the `arm` filter, `add_outbox_row`.
Tests: `tests/db/test_outbox_arm_migration.py` (form, idempotency, CHECK table evaluated from the file's own regex),
`tests/db/test_migrate_post_step.py` (FakeConn: first run, idempotent second run, later partition, invalid
leftover, retries, errors, version gating, migrate ordering), `tests/db/test_outbox_arm_reads.py` (memory and
emulator: pending, due and oldest never see an `arm` row), `tests/db/test_savepoint.py`,
`tests/deadman_receiver/test_arm_optional_fields.py`, `test_notify_store.py`
`test_existing_statements_unchanged_for_the_dispatcher` updated, `test_evaluate.py` field test now `PAYLOAD_FIELDS ∪
OPTIONAL_FIELDS`, the two `DeadmanSource` test fakes gained the three getters; `pg`-marked tests in
`tests/db/test_pg_integration.py` (both NOT VALID CHECKs accepted and validated, index attached on every partition and
valid, a later partition gets it, second post step empty, the CHECK table on the server, validation over existing
Oversee rows, savepoint rollback on PostgreSQL), run once against a disposable PostgreSQL 17 container: 17 `pg` tests
passed. G green: pytest 2798 passed / 32 skipped, mypy strict clean, `verify_execution.py --strict` 11 / 11.
Deviations: (1) the optional dead-man fields are left out of the JSON while None (not sent as null), so the beat is
byte-identical with the ARM off; the receiver treats absent as None; (2) `arm_failed` is an object `{count,
global_failed}`; (3) the ARM notice age threshold is 60 s (`Thresholds.arm_notification_age_ms`, the Oversee value;
the design names no number); (4) the design's CHECK is an "if and only if" over the whole array, so a non-arm row
whose channels mix `arm_*` with other names passes (no Oversee writer produces one; kept as designed, recorded in the
CHECK table test); (5) the post step also runs when no file is pending, and only for registered items whose migration
is in the directory (so temporary migration sets in tests issue no post-step SQL); (6) `relkind` is read as text (the
real server returns the `"char"` type as bytes). The `/live-trading-server` skill could not be invoked by the build
agent (user-invocable only); the `src/oversee` change (the `pending` filter and the `ARM_OUTBOX_KIND` constant) follows
`.claude/rules/oversee-rules.md` §10 and `tier-persistence.md` §4 as written. No `.claude/` change is needed.

* **C:** `src/db/migrations/0016_outbox_arm.sql`: `notification_outbox.expires_at_ms`, `superseded_by` (nullable, no
  default); index `ON ONLY notification_outbox (kind, next_attempt_at_ms, outbox_id) WHERE sent_at_ms IS NULL`; CHECK
  `NOT VALID`: `(kind = 'arm') = (coalesce(cardinality(channels), 0) > 0 AND coalesce(array_to_string(channels, ','),
  '') ~ '^(arm_email|arm_push)(:[0-9]+)?(,(arm_email|arm_push)(:[0-9]+)?)*$')` (no subquery, ST-11; an empty or NULL
  array never passes for `arm`, R11-m outbox CHECK).
* **M:** `src/db/migrate.py`: a no-transaction post step after all files (ST-3): per registered parent index, `CREATE
  INDEX CONCURRENTLY IF NOT EXISTS` on every partition (DEFAULT included, listed from `pg_inherits`), `ALTER INDEX …
  ATTACH PARTITION`; `VALIDATE CONSTRAINT` for the 0015 `event_log.origin` and 0016 outbox CHECKs; idempotent, with
  `lock_timeout` and retries. `src/db/notify_store.py`: `AND kind <> 'arm'` in `SQL_OUTBOX_PENDING`, `SQL_OUTBOX_DUE`,
  `SQL_OUTBOX_OLDEST_UNSENT`; `SQL_OUTBOX_INSERT` and `SQL_OUTBOX_ESCALATED` unchanged. `src/oversee/memory.py:150`
  `pending` filter. `tests/db/pg_emulator.py` handlers. `src/db/fence.py`: `FencedTx.savepoint()` async context
  manager (`SAVEPOINT` / `RELEASE` / `ROLLBACK TO`, ST-2). `src/runtime/deadman.py`: optional `DeadmanPayload` fields
  `arm_oldest_undelivered_notification_age_ms`, `arm_overdue_positions`, `arm_failed` (count and global flag), default
  None; `DeadmanSource`, `build_payload`; `src/runtime/session.py` `_DeadmanView` getters returning None until S32.
  `src/deadman_receiver/evaluate.py`: `OPTIONAL_FIELDS` beside `PAYLOAD_FIELDS`, parsed in `parse_beat`, Conditions in
  `Evaluator` (page when `arm_failed` > 0 or `arm_overdue_positions` > 0, or the age is over its threshold) (RT-M7,
  ST-12).
* **Tests:** `tests/db/test_notify_store.py` (update `test_existing_statements_unchanged_for_the_dispatcher`; the Oversee
  relay's pending and due reads never return an `arm` row, in memory and emulator); CHECK table test (arm `arm_email:3`
  passes; `+escalated` fails; non-arm with an `arm_*` channel fails; non-arm with empty channels passes; arm with empty or
  NULL fails); post step idempotent on `FakeConn`; `pg`-marked integration test that PG17 accepts the `NOT VALID` CHECKs on
  both partitioned tables (`notification_outbox` and the 0015 `event_log.origin` CHECK), validates them in the post step,
  and attaches the index; savepoint rollback keeps the outer transaction;
  `tests/deadman_receiver/test_evaluate.py:52` becomes `PAYLOAD_FIELDS ∪ OPTIONAL_FIELDS == DeadmanPayload fields`; an
  old payload without the optional fields still parses.
* **Resolves:** PR4 base, ST-2, ST-3, ST-11, ST-12, RT-M7, R11-m (outbox CHECK on empty arrays).
* **Risk:** on AWS the receiver deploys first (it already ignores unknown keys). The post step must never run inside the
  per-file transaction.

### S09 PR-G1: runtime hooks

Status: done 2026-11-16. Built: `src/runtime/node.py` `LeaderSession.start_optional_services()` in the protocol;
`NodeController` calls it once right after `machine.go(LEADER)` inside `try/except Exception` that logs and pages "optional
services failed to start" (error type only) and never exits, drains or changes the role (RT-M1).
`src/runtime/supervisor.py`: `has(name)` (RT-M2); `TaskSpec.on_circuit_reopen(name)` called (errors logged) where a
half-open death keeps the circuit open, and `reopen_count(name)` (RT-M3). `src/runtime/optional_settings.py`:
`OptionalFlags`, `optional_flags_from_env` (unset / empty / `0` off, `1` on, anything else off plus a value-free problem;
`XTNL_ARM_ENABLED=1` without `XTNL_MD_ENABLED=1` leaves the ARM off with a problem; never raises). `src/runtime/session.py`:
`SessionContext.optional_flags` / `optional_bootstrap` (`OptionalBootstrap = Callable[[EngineSession], TaskSpec]`, None
until S31); `EngineSession.start_optional_services()` (pages the flag problems once per term; with `XTNL_MD_ENABLED` off
adds nothing; with it on and a factory wired, adds `optional-bootstrap` idempotently through `has()`, never in DRAINING or
after `aclose`; a spec with another name or `critical=True` raises for the controller to page);
`set_md_hook()` (late-bound `_md_hook`) and `MarketDataHookGuard` (20 consecutive raises: skipped 60 s, one page;
logs carry the error type only); the composite `_on_price_tick` calls the market-data hook first, before the
`calendar.version is None` guard, then the SM10 path unchanged (RT-M12); `register_closer(name, close, terminate)`;
`aclose` runs the HTTP client close, `ports.close()` and the extra closers in parallel inside `timeout_s`
(`drain_aclose_s` in DRAINING), cancels what is still running and calls its `terminate()` (RT-M6).
`src/runtime/ports.py` `LeaderPorts.terminate` (optional); `src/db/pg.py` `AsyncpgSource.terminate()`;
`src/runtime/pg_ports.py` `LazyPgSource.terminate()` (also reaches a pool whose `close()` is running or was cancelled),
wired as the term pool's `terminate` in `pg_leader_ports`. `src/runtime/wiring.py`: `build_engine` parses the two flags
(problems logged as config warnings, never a `WiringError`) into the context; `EngineHooks.optional_bootstrap` test
seam; `check_composition` never reaches LEADER, so never calls the hook. `deploy/env/engine.env.example` documents
`XTNL_MD_ENABLED=0` / `XTNL_ARM_ENABLED=0` (the env-documentation test requires every variable the code reads).
Tests: `tests/runtime/test_optional_hooks.py` (38: hook called once right after LEADER; a raising hook keeps LEADER, the
lease and health publishing, one page; `has()`; `on_circuit_reopen` on each half-open death and a raising hook;
flags off / no factory / DRAINING / closed add no task; idempotent add; bad spec raises; problems paged once without the
value; the market-data hook runs while `calendar.version is None`; a hook raising on every tick still feeds the
calendar, trips after 20 and pages once; tick-only symbols reach the hook but not SM10; guard window, retry and reset;
parallel `aclose`; `terminate()` for hung steps; DRAINING under `drain_budget_s + 0.4` with hung extra closers;
`check_drain_budget` unchanged (6.7 s); `AsyncpgSource` / `LazyPgSource` / term-pool terminate; flag parse table;
`build_engine` never refuses a bad flag; `check_composition` never calls the factory); fakes
`tests/runtime/test_node.py` `Session` (inherited by `_TornSession`) and `tests/runtime/test_telemetry.py`
`TermSession` gained `start_optional_services`. G green: pytest 2836 passed / 32 skipped, mypy strict clean (203 files),
`verify_execution.py --strict` 11 / 11.
Deviations: (1) the extra closers are registered on the session (`EngineSession.register_closer`, returns False after
`aclose`), not on the process-wide `SessionContext`; the context carries `optional_flags` and `optional_bootstrap`;
(2) the bootstrap factory returns the `TaskSpec` and the session adds it (name and non-critical checked), so `has()`
idempotency lives in one place; (3) the "one page" for a bad flag is sent by the leader at `start_optional_services`
(once per term), since `build_engine` has no session to page through (it logs a config warning); (4) the market-data
hook guard pages once per guard (per term or per `set_md_hook`), not on every 60 s re-trip; (5) `LeaderPorts` gained an
optional `terminate` field (None for in-memory ports); (6) `MarketDataHookGuard` lives in `session.py` (no extra
module). No file in `src/adapters`, `src/pubsub`, `src/dto`, `src/integrity` or `src/oversee` changed, so the
`/live-trading-server` skill was not needed; no `.claude/` change is needed.
Review minors fixed 2026-11-16 (all six): `LazyPgSource._get` terminates a pool the factory returns after `close()` /
`terminate()` and raises ConnectionError; `aclose` cancelled from outside cancels and terminates every step still running,
then re-raises; the controller logs the optional-services error type only (no message, no traceback); `reopen_count` is
per incident (reset when the circuit closes: clean half-open run or `reset_task`); tests added for each, plus
`build_engine` passing `EngineHooks.optional_bootstrap` into the context, a real `EngineSession` reaching LEADER through
the wired controller with `XTNL_MD_ENABLED=1` adding `optional-bootstrap`, and a barrier-based parallel `aclose` test
(no wall-clock bound). pytest 3134 passed / 50 skipped, mypy strict clean (208 files), `verify_execution.py --strict`
11 / 11.

* **M:** `src/runtime/node.py`: `LeaderSession.start_optional_services() -> None`; called after
  `self.machine.go(NodeRole.LEADER, ...)` (:315) inside `try/except Exception` that logs and pages
  "optional services failed to start" and never ends the term (RT-M1). Fakes: `tests/runtime/test_node.py:24`
  `Session` (inherited by `_TornSession`), `tests/runtime/test_telemetry.py:263` `TermSession`.
  `src/runtime/supervisor.py`: `has(name) -> bool` (RT-M2); `TaskSpec.on_circuit_reopen: Callable[[str], None] | None`
  called where a half-open death keeps the circuit open (:238-242), plus `reopen_count(name)` (RT-M3).
  `src/runtime/session.py`: `start_optional_services()` calls `ctx.optional_bootstrap(self)` when that factory is set
  and a flag is on (None until S31); the composite price hook: `_on_price_tick` first calls the late-bound
  `self._md_hook` (None until S31) in its own try/except with its own failure counter (after 20 consecutive raises it is
  skipped for 60 s and pages once), before the `calendar.version is None` guard, then the calendar path unchanged
  (RT-M12); `aclose` closes the HTTP client, `ports.close()` and every registered extra closer with `asyncio.gather`
  inside `drain_aclose_s`, then calls `terminate()` on what is still open (RT-M6). `src/db/pg.py`
  `AsyncpgSource.terminate()`, `src/runtime/pg_ports.py` `LazyPgSource.terminate()`. `src/runtime/wiring.py`:
  `SessionContext.optional_bootstrap`, `extra closers`; `check_composition` never calls the hook.
  C: `src/runtime/optional_settings.py` (tolerant parse of `XTNL_MD_ENABLED` / `XTNL_ARM_ENABLED`; the full parse
  happens in S31, RT-M13).
* **Tests:** the controller calls the hook after LEADER; a raising hook leaves the node LEADER and publishing health;
  `has()`; `on_circuit_reopen` fires on a half-open death (mirror `test_half_open_death_keeps_circuit_open_for_another_10_min`);
  a market-data hook raising on every tick still feeds the calendar; the hook runs while `calendar.version is None`;
  DRAINING stays under `drain_budget_s + 0.4` with a hung extra closer (mirror `test_followups_4_9.py:260-296`);
  `check_drain_budget` unchanged; flags off → no task added.
* **Resolves:** RT-M1, RT-M2, RT-M3, RT-M6, RT-M12, RT-M13 (flag part).
* **Risk:** `node.py` is HA-critical; the call sits after the role change and touches nothing else.

### S10 PR-G2: operator-request kind filter and handler registry, health registry, lifecycle ensure

Status: done 2026-11-16. Built: `src/db/operator_requests.py` `CORE_REQUEST_KINDS` (the three session-consumed kinds;
`REQUEST_KINDS` equals it until 0017) and `SQL_PENDING_WITH_PAYLOAD` gains `AND r.kind = ANY($2::text[])` before the
`ORDER BY ... LIMIT 50`; `src/runtime/ports.py` `OperatorRequestQueue.pending(epoch_token, kinds)` and
`src/runtime/pg_ports.py` `PgOperatorRequests.pending` (passes the sorted kinds as `$2`). New
`src/runtime/request_handlers.py`: results `Applied` / `Refused(reason)` / `Moot(reason)` / `Accepted(owner_task)`
(stored as `applied`, `refused:<code>`, `moot:<code>`; reasons are short lower-case codes, never values),
`RequestHandler` (synchronous), `HANDLER_ERROR`, `HANDLER_NOT_SYNC`. `src/runtime/session.py`:
`register_request_handler(kind, fn)` (refuses core kinds and malformed kinds; re-registering replaces),
`unregister_request_handler(kind)`, `request_kinds()` (core ∪ registered, passed to `pending`),
`complete_request(request_id, result)`; `_consume_operator_requests` first consumes the owners' recorded answers,
then dispatches core kinds (`_core_request`, `unsupported_kind` only for a core kind without a branch) and
registered kinds (`_dispatch_request`: a raise or a wrong return type is `refused:handler_error`, an awaitable is
closed unrun and `refused:handler_not_sync`, logs carry the error type only); a row of any other kind is never
consumed (RT-M10); an `Accepted` request is skipped while its owner task is registered in the supervisor.
`src/runtime/health.py` `HealthRecord.registry: tuple[str, ...] = ()` after `safe_hold` (`to_json` writes it,
`from_json` defaults it, a malformed value reads as empty); `src/runtime/probes.py` `NodeHealthProbe.registry_tokens`
(None until S32) fills it sorted and de-duplicated, a raising source publishes none. `src/ops/etcd.py`
`OpsEtcd.get_prefix` (the implementation is `HttpEtcdClient.get_prefix`) and `HEALTH_PREFIX` re-exported from
`src/runtime/etcd.py` (RT-M8, IN-9, ST-13). `src/db/lifecycle.py` `PgPartitionMaintenance.ensure_months(parent,
months)` around `_ensure` (fenced, `lock_timeout`, one call per distinct month start, errors reported, unknown parent
ValueError, `EpochMismatch` propagates; RT-M9 method). Tests: `tests/runtime/test_request_registry.py` (12: 60
pending `arm_disarm` rows without a handler and one `reset_task`, the `reset_task` applied and the 60 never
consumed, then applied 50 + 10 once a handler registers in the same epoch; registration rules and unregister;
value-free result codes; a `reset_task` behind a hanging `arm_clear_failed` owner task applied within one
`operator_poll_s` and the owner's later answer consumed once; `complete_request` rules; an owner that is gone gets
the request dispatched again; raising / wrong-type / async handlers refused without delaying core requests and
without the error text in logs; `HealthRecord.from_json` without `registry`, round trip, malformed values; the probe's
registry tokens; `get_prefix` on the ops fake and the shared `HEALTH_PREFIX`); `tests/db/test_request_kinds_and_ensure.py`
(6: the kind filter sits before the `LIMIT 50`; core kinds; a `pg`-marked real-PostgreSQL run of the 60 + 1 case
with the CHECK widened to a test-only kind list inside a rolled-back transaction, skipped without
`ORDER_SERVER_TEST_DSN`; `ensure_months` fenced and once per month, a failed month reported, unknown parent and lost
fence). Updated: `tests/runtime/test_followups_4_9.py` `FakeQueue` (kind filter, order, `LIMIT 50`; an unhandled
kind now stays pending instead of `unsupported_kind`; `$2` in the pending args), `tests/runtime/test_wiring_56.py`,
`tests/ops/conftest.py` `FakeEtcd.get_prefix`; `docs/IMPLEMENTATION_PLAN.md` S10 ticked. G green: pytest 2853 passed
/ 33 skipped, mypy strict clean (204 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) the owner's answer is `EngineSession.complete_request(request_id, result)` (not `complete`), and it is
synchronous: it records the answer and the operator loop marks the request consumed (fenced) on its next pass, so the
owning task never touches PostgreSQL and a failed consume is retried by the loop; (2) added
`unregister_request_handler` and `request_kinds()`, and an accepted request whose owner task is no longer registered in
the supervisor is dispatched again (otherwise it would be skipped for the rest of the term); (3) handler bugs are
consumed as `refused:handler_error` / `refused:handler_not_sync` rather than left pending (no hot retry loop);
(4) `registry_tokens` is a wiring-set attribute of `NodeHealthProbe` like `leader_streaming` / `market_open`, not a
constructor argument; `from_json` keeps the published order, the probe sorts; (5) `ensure_months` returns a
`LifecycleReport` and accepts only parents in `PARTITIONED` (S23 adds the ARM tables there);
(6) `tests/runtime/fakes.py` `record()` needed no change (`registry` has a default); (7) the 60 + 1 test runs on the
in-memory queue that mirrors the SQL plus a `pg`-marked real-PostgreSQL test. No file in `src/adapters`,
`src/pubsub`, `src/dto`, `src/integrity` or `src/oversee` changed, so the `/live-trading-server` skill was not
needed; no `.claude/` change is needed.
Review fixes (2026-11-16): (1) `REASON_PATTERN` is now `^[a-z][a-z_]{0,63}$` (letters and `_` only), so a number
such as `1.2345`, `105000` or `0.5-1.5` can never be stored as a reason; the value-free test rejects them. (2)
Accepted, unanswered requests take no slot of the `LIMIT 50`: `SQL_PENDING_WITH_PAYLOAD` gains
`AND NOT (r.request_id = ANY($3::bigint[]))`, `OperatorRequestQueue.pending(epoch_token, kinds, exclude=())` and
`PgOperatorRequests.pending` pass the session's accepted ids (sorted) as `$3`; test: 50 accepted requests on a hung
owner plus one `reset_task`, the `reset_task` applied. (3) An `Accepted(owner)` whose owner task is not registered
when the handler returns is recorded as accepted and waited for (the handler is not called again, no repeated
enqueue); once the owner is registered it answers through `complete_request`; only an owner that was registered and
has since gone gets the request dispatched again (`_accepted_requests` holds (kind, owner, owner seen)). (4) An
awaitable returned by a handler is closed (coroutine) and cancelled (Task / Future), so nothing keeps running
unsupervised; parametrised Task / Future test. (5) The `pg`-marked test runs only when `ORDER_SERVER_TEST_DSN` names
a disposable `*_test` database (`migrate()` commits outside the rolled-back transaction), reads through
`PgOperatorRequests` on the same connection and checks the `$3` exclusion; the session loop over
`PgOperatorRequests` is not driven there (its consume is fenced on `engine_epoch`), the loop is covered by the
in-memory queue. G green: pytest 2857 passed / 33 skipped, mypy strict clean (204 files),
`verify_execution.py --strict` 11 / 11.

* **M:** `src/db/operator_requests.py` `SQL_PENDING_WITH_PAYLOAD` gains `AND r.kind = ANY($2::text[])` (the index
  `(requested_epoch, kind)` already serves it); `src/runtime/ports.py:76` `pending(epoch_token, kinds)`;
  `src/runtime/pg_ports.py:234`; fake `FakeQueue` (`tests/runtime/test_followups_4_9.py:329`).
  `src/runtime/session.py` `_consume_operator_requests`: passes core kinds ∪ registered kinds; new
  `register_request_handler(kind, fn)`; a registered handler is non-blocking: it returns `Applied | Refused | Moot |
  Accepted(owner_task)`; an `Accepted` request is skipped on later polls and its owning task calls
  `complete(request_id, result)`; the `unsupported_kind` branch stays only for kinds in the core list (RT-M10, R11-m
  non-blocking handlers). `src/runtime/health.py`: `registry: tuple[str, ...] = ()` after `safe_hold`; `from_json`
  defaults it; `src/runtime/probes.py:337` fills it from an injected `registry_tokens()` (empty until S32);
  `tests/runtime/fakes.py:102-106`. `src/ops/etcd.py` `OpsEtcd.get_prefix` (implementation inherited from
  `src/runtime/etcd.py:252`; `HEALTH_PREFIX` from `src/runtime/etcd.py:38`, RT-M8, IN-9, ST-13).
  `src/db/lifecycle.py`: public `ensure_months(parent, months)` around `_ensure` (RT-M9).
* **Tests:** 60 pending `arm_disarm` rows (inserted with a test-only kind list) with no handler and one `reset_task`:
  the `reset_task` is applied; a request whose handler registers later in the same epoch is applied; a handler that
  hangs does not delay a `reset_task` beyond one `operator_poll_s`; `HealthRecord.from_json` without `registry`;
  `get_prefix` on the ops fake.
* **Resolves:** RT-M8, RT-M9 (method), RT-M10, IN-9, ST-13, R11-m (non-blocking handlers).

### S11 Lockout scanner, package skeletons, guardrails

Status: done 2026-11-16 (code and tests green; **the `.claude/` and `CLAUDE.md` changes wait for the user's approval**,
proposed in `docs/design/proposed-claude-changes/S11-lockout-scanner.md`). Built: `src/riskmgr/__init__.py`,
`src/marketdata/__init__.py`, `src/armconfig/__init__.py` (docstrings only); `src/riskmgr/lockout.py`: `LOCKOUT_SCOPE`,
`NAME_BAN_FILES` (`src/runtime/md_ports.py`, name ban only), `SCANNER_SELF` with its own allow-list (`sys` for
`sys.modules` only), per-package import allow-lists and `REFUSED_MODULES` with named reasons (direct imports only,
relative imports resolved; `zlib` / `crc32` only in marketdata, `concurrent.futures` / `ThreadPoolExecutor` only in
riskmgr, `redis` only under `src/marketdata/store`, `ssl` / `asyncpg` only in armconfig, marketdata never imports the
ARM, no scoped module imports the scanner), member limits through import aliases and attribute chains (asyncio list
plus `get_running_loop` / `wrap_future` in riskmgr only; `logging.handlers` / `logging.config`), reflection rules,
the `_`-attribute rule, port-reflection rule, `normalise_name` / `NAME_BAN_PATTERN` / `DEF_NAME_RE` /
`banned_names_in_text` / `in_name_ban_scope` (the hook copy's surface), `scan(roots, modules=None) -> ScanResult
(violations, incomplete, scanned)` over the scope directories plus the `__file__` of every loaded module of the scoped
packages, `scan_source`, `require_clean` (`LockoutViolation` / `ScanIncomplete`) and `assert_sinks` (`PHASE1_SINKS` =
decision, notice). Tests: `tests/riskmgr/test_lockout.py` (191: the plan's planted samples and more, the 80-name plan
fixture as names and as definitions, unreadable / undecodable / unparsable files → `incomplete`, loaded modules from
outside the root, compiled files, the scanner under its own name vs a copy, sinks, and the real-tree scan that gates
every G from now on); `tests/hooks/test_pre_tool_guard_parity.py` (16: `FORBIDDEN_PATTERNS` never narrowed and the
`src/adapters/mock.py` `set_order` edit passing run now; 14 parity / hook-behaviour tests skip with "hook update
pending" and passed against a scratch copy of the proposed hook). The proposed `verify_execution.py` check body was run
against today's tree (passes). G: pytest 3050 passed / 47 skipped (14 of them "hook update pending"), mypy strict clean (208 files), `verify_execution.py --strict`
11 / 11 (12 once the proposed check is applied).
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); the
code follows the plan and design §B3; (2) tightenings beyond §B3, mirrored in the proposed rule text: star imports,
`eval` / `exec` / `compile`, the `__builtins__` name and `locals()` are refused; compiled files (`.pyc` outside
`__pycache__`, `.pyd`, `.so`) in the scope are refused; a loaded scoped module with no source file or a non-`.py` file
is a violation; the normaliser also collapses runs of `_` (so `place__order` cannot slip through), in the hook copy too;
(3) interpretations: `vars` / `dir` / `globals` / `locals` pass only when subscripted at once with a literal key not
starting with `_`; `super()` counts as `self` for the `_`-attribute rule; a "port Protocol" parameter is one annotated
with a name ending in `Port`, `Pager`, `PagerWrapper` or a Protocol defined in the same file (plus `self.<..port>` /
`self.<..pager>` attributes); `src/armconfig` may import `src.armconfig`, `src.riskmgr.registry` and
`src.marketdata.indicators.registry` but no other ARM / market-data module (the "`parse_params` only" restriction is
documented, not statically enforced); (4) `assert_sinks` takes the wired sink names (S31 passes them); (5) `scan`'s
roots are repository roots and it takes an optional `modules` mapping (default `sys.modules`) as a test seam;
(6) `CLAUDE.md` is `.claude/CLAUDE.md`; the proposed hook also reads each MultiEdit `new_string` for the name ban.
Review fixes (2026-11-16): (a) refused-module names (`os`, `sys`, `inspect`, ... plus `threading`, `io`, `types`,
`weakref`, `copy`, `traceback`, `warnings` and the member-limited `asyncio` / `logging` / `concurrent` / `zlib`) are
refused as an attribute on any receiver and as `from <any module> import <name>` (`logging.os`, `typing.sys.modules`
and `from contextlib import sys` had passed); a module bound by an import is used only through attribute access;
(b) the builtins the call checks cover may appear only as the direct callee (aliases, arguments, defaults refused; a
function's own local of that name passes); (c) the event loop's socket and process methods are refused on any receiver
(`get_running_loop().create_connection` had passed); (d) a `logging` member list, and `handlers` / `addHandler` /
`removeHandler` refused off `self`; (e) `open`, `breakpoint`, `input` refused; (f) `self` / `cls` count only as the
first parameter of a non-static method in a class body; `_` attributes in match patterns and `.format` templates are
refused, and `.format` only on a string literal; (g) the normaliser splits letter-to-digit (`place_order2`), and the
name-ban path match ignores case, mirrored in the proposed hook; (h) the scanner's own file is checked for dynamic code,
`__builtins__` and the refused builtins; (i) a file too deep to parse or walk is `incomplete`, never raised; (j) the
proposed hook also runs `FORBIDDEN_PATTERNS` over each MultiEdit `new_string` (only widens). Deviation: the skill was
again not invokable (user-only). Not covered: digit-for-letter spellings such as `Place0rder` (the letter-to-digit
split catches only suffixes). G: pytest 3127 passed / 50 skipped (17 "hook update pending"; all 19 parity tests passed
against a scratch copy of the proposed hook), mypy strict clean, `verify_execution.py --strict` 11 / 11.
Left for the user: approve and apply the proposal (hook, `post_tool_verify.py` `WATCHED_DIRS`, `verify_execution.py`
check, `.claude/rules/riskmgr.md`, `.claude/rules/marketdata.md`, `.claude/CLAUDE.md` lines).

* **Goal:** the read-only lockout exists before any code in the new packages.
* **C:** `src/riskmgr/__init__.py`, `src/marketdata/__init__.py`, `src/armconfig/__init__.py` (docstrings only);
  `src/riskmgr/lockout.py`: `LOCKOUT_SCOPE`, per-package import allow-lists (design §B3; direct imports only, so
  `src.oversee.risk`'s own imports are not followed, IN-14; `zlib` with only `crc32` in `src/marketdata`,
  `concurrent.futures` with only `ThreadPoolExecutor` in `src/riskmgr`), `SCANNER_SELF` (the scanner file itself is
  exempt by name from the package allow-list, the reflection rules and the `_`-attribute rule, and is held to its own
  allow-list `__future__`, `ast`, `dataclasses`, `enum`, `pathlib`, `re`, `sys` for `sys.modules` only, `typing`; the
  name ban still applies to it), refused modules with named reasons, the name-ban normaliser
  and pattern, reflection rules (refuse `getattr` / `hasattr` / `delattr` / `vars` / `dir` / `globals` with a literal
  name starting `_`, and with a non-literal name; refuse `logging.handlers`, `logging.config`, and `dataclasses.asdict` /
  `dataclasses.fields` / `vars` applied to a parameter typed as a port Protocol; an attribute allow-list for
  `asyncio`: `sleep`, `gather`, `wait_for`, `Event`, `Lock`, `Queue`, `create_task`, `timeout`, `CancelledError`,
  `to_thread`, plus `get_running_loop` and `wrap_future` in `src/riskmgr` only), `scan(roots) -> ScanResult(violations, incomplete)` covering the package directories plus the `__file__`
  of every loaded module of the scanned packages; `assert_sinks(wired)` (used in S31).
  `tests/riskmgr/__init__.py`, `tests/riskmgr/test_lockout.py` (planted samples, plus a scan of the real scope tree that
  runs in every G from now on, §1); `tests/hooks/__init__.py`, `tests/hooks/test_pre_tool_guard_parity.py` (loads the
  hook with `importlib.util`; scope, normaliser and pattern equal `lockout.py`; skipped with "hook update pending" until
  the hook diff is approved).
* **M (user approval):** `.claude/skills/live-trading-server/scripts/verify_execution.py` (new `@check("read-only: ARM
  lockout scanner")` modelled on the adapters AST walk at :67-80; docstring list :8-16);
  `.claude/hooks/pre_tool_guard.py` (scope copy, normaliser and pattern applied to scope files and
  `src/runtime/md_ports.py` only; `FORBIDDEN_PATTERNS` unchanged and repo-wide); `.claude/hooks/post_tool_verify.py:12`
  `WATCHED_DIRS` += `src/riskmgr/`, `src/marketdata/`, `src/armconfig/`, `src/runtime/`, `src/notifications/`;
  new `.claude/rules/riskmgr.md` and `.claude/rules/marketdata.md` (allow-list copy, naming convention
  `held_positions` / `arm_row_update` / `mark_arm_closed`, "adding an indicator", "adding a strategy"); `CLAUDE.md`
  lines (market data only in `redis-md`; the skill is mandatory for the three new packages).
* **Tests:** planted `_place_order`, `class ModifyStop`, `def trail_stops`, `import socket`, `getattr(x, "_y")`,
  `asyncio.open_connection`, `import src.adapters` in `tmp_path` packages are refused with their reasons, and so are
  `import zlib` in a riskmgr sample, `zlib.compress`, `concurrent.futures.ProcessPoolExecutor`, `import sys` outside
  the scanner file and `asyncio.wrap_future` in a marketdata sample; `zlib.crc32` in marketdata and
  `ThreadPoolExecutor` in riskmgr pass; today's tree passes, `lockout.py` included (it passes only under
  `SCANNER_SELF`, and a copy of it under another name is refused for `ast` / `sys`); an edit to `src/adapters/mock.py` (`set_order`) passes the hook; an unreadable file gives `incomplete`; every
  name this plan introduces in the scope and in `src/runtime/md_ports.py` (`StopHandleReadPortImpl`, `StopBasis`,
  `break_even_on_risk_close`, `close_eligible`, `held_positions`, `arm_row_update`, `mark_arm_closed`, `pre_basis`) is
  listed in a fixture and passes the name ban, so a later stage cannot be blocked by its own plan names.
* **Resolves:** §B3 lockout, ST-8 (convention), ST-9 (documented), IN-14, R11-m (reflection bypasses).
* **Risk:** the hook copy and the scanner can drift; the parity test is the gate.

### S12 MD-1: market-data core (pure)

Status: done 2026-11-16. Built (all pure, no I/O; prices never in a `repr`, a log text or an error): `src/marketdata/types.py`
(`BarSpec` 1m / 5m / 15m / 1h on the UTC grid, `Side`, `Source`, `SeriesId` with canonical `adapter:symbol:spec:side:source`
and key-safe parts, `Bar`, `Quality`, `Quote`, `Liveness`, `Sm12State`, `LeaseClass`, `MdGeneration`, `EventCursor`,
`BarEndEvent`, `DataUnavailable` / `Unsupported` / `Transient` / `CursorExpired` and the reason codes);
`src/marketdata/timing.py` (`TIMING_TABLE` with the 15m row only, `timing_problem`, `pre_offset_problem`,
`rollback_n_problem`, `SkewEstimate` / `settle_ms`, `decision_cap_ms` = `T + timeframe + p50 + settle`, `first_read_ms`,
the §B5.2 re-read schedule `next_read_ms` (5 s / 30 s / 60 s, stops before the cap), `verification_read_ms`, PRE fire and
valid-until, `post_grace_end_ms`, a `zoneinfo` copy of the 17:00 New York roll and `overlaps_roll`);
`src/marketdata/validate.py` (checks 1-5 as values, NaN / infinity fail without raising, `split_response` with forming bars
as evidence only, `check_tick_bar`, `check_stored` with `HashStatus`, `check_quote`); `src/marketdata/hashing.py`
(`Keyring`, `content_hash` = key id ‖ HMAC-SHA256[:16] over canonical decimal text, `verify` (retained ids valid, unknown
id MISSING), `compare_hashes` (different key ids INCOMPARABLE), unkeyed CRC32 `record_digest`); `src/marketdata/codec.py`
(member `<bucket_start:13><revision:4>|record:87|digest:8|hash:34`, scaled integers, `decode_member` returns
`CodecFailure` values); `src/marketdata/config.py` (`parse_md_settings` → `MdSettings` or value-free problems);
`src/marketdata/errors.py` (`safe_redis_error`); `src/marketdata/ports.py` (`MarketDataPort`, `BrokerBarsPort`,
`BarsResult`, `StopHandleReadPort` with `HandleRef` / `HandleRead` / `StopHandleRead`, `StampPort`, `CalendarPort` with
`calendar_open` (None = closed), `AdapterHealthPort`, `NodeRecordPort` / `NodeMdRecord`). Tests: `tests/marketdata/`
(210: types, timing incl. a negative p50 and the roll copy equal to `src.pubsub.tradingday` over 300 days and at
roll - 1 ms / roll / roll + 1 ms on every day, the validation table incl. the forming bar, future-dated bars and the
range check, golden hashes under fixed test keys plus the documented construction, rotation (retained id valid,
unknown id missing, ids only increase), codec round trips, distinct members and a corrupt key id as a digest failure,
config bounds, redis-py's own error parser through `safe_redis_error`, typed port fakes incl. `MarketDataPort`).
G: pytest 3344 passed / 50 skipped, mypy strict clean (216 files), `verify_execution.py --strict` 11 / 11; the
lockout real-tree scan passes with the new modules.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); (2) a
key's 1-byte id is set by the operator with the key (`XTNL_MD_HMAC_KEY_ID`, required, 1-255;
`XTNL_MD_HMAC_KEY_PREV_ID`, required with `XTNL_MD_HMAC_KEY_PREV`) and only increases (a retained id must be lower than
the current one; one key under two ids is refused), so a new key never takes the id of any retired key whose hashes
stay in durable records (an id derived from the key, as first built, collided with some retired id with probability
1/256 per rotation; review fix). The two names join design §C2 and the S36 runbook (rotation: new key, next id);
(3) `safe_redis_error` recognises a `ResponseError` by the class path in the MRO (this module may not import `redis`) and
reads the code from redis-py's `status_code` or the first token; script codes `NO_BOOT`, `NO_WRITER`, `NO_LOCK`,
`BAD_ARGS`, `STALE_SEQ` are pre-listed for S13 (widen there if S13 names others); (4) the indicator reads (`indicator`,
`provisional`, `on_indicator_updated`) join `MarketDataPort` in S18 with the indicator types; (5) the re-read schedule is
measured from the local bar end (`T + p50`), the clock of every other timer; (6) the `XTNL_MD_*` names parsed by
`config.py` (`XTNL_MD_CROSSCHECK_K`, `_MISMATCH_K`, `_ROLLBACK_N`, `_DECISION_SYMBOLS`, `_BROKER_BAR_SETTLE_S` default
10, `_BROKER_BAR_LAG_MS` default 5000, plus the four secrets) are not yet in `deploy/env/engine.env.example`: the module
reads no process state, and S31 / S36 document them when they are first read (the two defaults the design leaves open
are to be confirmed on PR3 data); (7) the codec digest covers `prefix|record|hash` (member order unchanged), so a corrupt bucket, revision or key-id
byte is a `digest` failure (quarantine, §A4) and `missing` can only come from a real rotation; (8) review fixes:
only the bucket already started at receipt (start <= receipt + 5 s) can be forming, a later bar fails check 4 and is
quarantined; `split_response` fails every closed bar of a response for another adapter or symbol on a new
`instrument` check; `canonical_decimal` refuses (ValueError) a value beyond 10^±100 or needing more than 60 digits,
and `verify` / `check_stored` never raise; `safe_redis_error` survives an exception whose `__str__` or `status_code`
raises; (9) **design edge for the user:** at `pre_offset` 30 s (the design's lower bound) the PRE window is empty
(`valid_until = min(fire + 10 s, T + p50 - 30 s) = fire`), so the 15m row allows 31-45 s and `pre_offset_problem` also
refuses any offset with no window; design §B5.2 still says 30-45 s until the user confirms; (10)
`XTNL_MD_BROKER_BAR_LAG_MS` feeds `next_read_ms(..., lag_ms)`: the 5 s cadence lasts until `T + max(post_grace, lag)`
(no change at the 5 s default), and the lag metric (S17).

* **C:** `src/marketdata/types.py` (`BarSpec` on the UTC grid, `Side`, `Source` TICK | BROKER, `SeriesId`
  canonical string, `Bar`, `Quality`, `Quote`, `Liveness`, `MdGeneration(epoch_rev, n)`, `BarEndEvent(series,
  bar_end_ms, content_hash, write_seq, revision)`, port result values `DataUnavailable(reason)`, `Unsupported`,
  `Transient`, `CursorExpired`); `src/marketdata/timing.py` (per-bar-spec timing table: `pre_offset` bounds,
  `post_grace`, re-read schedule, UNSUPPORTED lag limit, rollback N, cross-check TICK spec; v1 has only the 15m row, and
  validation refuses a strategy whose `bar_spec` has no row; `decision_cap(T) = local T + timeframe + p50 + settle`,
  the one clock for every cap-based timer; a `zoneinfo` copy of the 17:00 New York roll, IN-14);
  `src/marketdata/validate.py` (hard checks 1-5 returning results; check 2 = on the spec grid and inside the requested
  range `[from, to]`; checks apply only to bars whose bucket end ≤ receipt + 5 s, so the forming bar of a response is
  never validated, stored or counted, R11-M15; quote checks); `src/marketdata/hashing.py` (`Keyring`: key id → key,
  current id, retained older ids; `content_hash = key_id ‖ HMAC-SHA256(key, series ‖ bar_end_ms ‖ o ‖ h ‖ l ‖ c)[:16]`
  over canonical decimal text; `verify` accepts any retained key id, an unknown id is "missing", R11-M13; an unkeyed
  CRC32 record digest (`zlib.crc32`, allowed in `src/marketdata` only) for out-of-process readers, R11-m); `src/marketdata/codec.py` (fixed-width member
  `<bucket_start:13><revision:4>|record|digest|hash`, scaled integers); `src/marketdata/config.py` (pure parse of a
  `Mapping[str, str]` into `MdSettings` or a list of errors); `src/marketdata/errors.py` (`safe_redis_error`: class name
  and the first token of a `ResponseError` against an allow-list, never the message, R11-m); `src/marketdata/ports.py`
  (Protocols: `MarketDataPort`, `BrokerBarsPort`, `StopHandleReadPort`, `BarsResult`, `StopHandleRead`, `StampPort`,
  `CalendarPort` (`state(adapter, symbol)`; None counts as closed), `AdapterHealthPort`, `NodeRecordPort`).
* **Tests:** `tests/marketdata/` validation table (each check, forming bar ignored, range check); golden hashes under a
  fixed test key; a retained key id verifies and an unknown one is missing; codec round trip and two identical bars in
  different buckets stay distinct members; `safe_redis_error` never contains message text; `decision_cap` with a
  negative p50.
* **Resolves:** R11-M13 (keyring), R11-M15 (validation), R11-m (cap clock, timing table, out-of-process digest, safe
  Redis errors).

### S13 MD-2: `redis-md` store

Status: done 2026-11-16. Built: `src/marketdata/store/keys.py` (every `md:*` key builder, key-safe parts,
`WriterToken` = `cluster_id:epoch_rev:node:term_nonce`, `BootSentinel` = `run_id:cluster_id:node:epoch_rev.n` with a
never-raising `parse`); `src/marketdata/store/scripts.py` (Lua as `Final` strings, each checking everything before its
first write: `FLUSH` (token → `NO_WRITER`, exact `md:boot` and the ARGV `run_id` equal to the sentinel's → `NO_BOOT`;
ticks with explicit ids and a monotonic guard read from `XINFO STREAM` last-generated-id, so a backward step or a
stream trimmed empty uses last id + 1 and is counted; forming bar rewrite; closed buckets by `ZREMRANGEBYSCORE` +
`ZADD`, superseded members to `md:bars_rev` stamped `|superseded_at_ms`, trimmed to 7 days; an identical member is a
no-op with no event and no `write_seq`; rollback entry `<write_seq:20>|<bar_end_ms:13>|<member>` scored by
`HINCRBY md:meta write_seq` and trimmed to N; `bar_closed` / `bar_revised` on `md:events MAXLEN ~ 100000` with id,
hash and `write_seq` only; `md:meta` first / last bucket, `last_write_ms` and caller fields), `LEASE` (token only, SET PX
or DEL, registry entry), `BOOT_LOCK` / `RENEW` / `RELEASE`, `BOOT_WRITE` (bootstrap steps 3 / 4 under token and lock:
forming cleared, meta, rollback entries restored with `write_seq` raised to the highest, `STALE_SEQ` on a conflicting
entry, registry), `FINISH` (`md_reset` event, then `md:boot`, then the lock released) and `QUARANTINE` (check name and
record into `md:quarantine`, member removed from `md:bars`); argument builders that keep prices out of `repr`);
`src/marketdata/store/runner.py` (`ScriptRunner`: one connect hook = `SCRIPT LOAD` of every script with its SHA checked
plus the `INFO server run_id` read, re-run after every reconnect, connection error or NOSCRIPT (only the NOSCRIPT calls
are re-sent); `EVALSHA` only, through `pipeline(transaction=False)`; replies `ScriptOk` / `ScriptRefused(code)` /
`ScriptFailed(safe text)`; `StoreUnavailable(timeout | connection | error)` with `safe_redis_error` text only);
`src/marketdata/store/client.py` (read client: pool of 4, 500 ms socket and connect timeouts; write client: a blocking
pool of exactly one connection, 2 s timeouts, redis-py's connect callback marks the runner stale; redis-py retries off
on both; `RedisAofWaiter` = `WAITAOF 1 0 1500`, `write_and_wait` returns the reply's first element, 0 on a timeout or
error, no `WAITAOF` with AOF off; `install_token` (plain SET), `read_boot`; `close_within` closes in parallel inside the
budget and drops the rest, never `QUIT`). `src/marketdata/errors.py` gains `redis_error_code`. `deploy/docker-compose.dev.yml`:
`redis-md` in profile `md` (AOF everysec, `aof-load-truncated`, `noeviction`, `default off`, the §A2 `md_writer` /
`md_reader` / `md_health` users with throwaway dev-only passwords, healthcheck as `md_health`, tmpfs); `pyproject.toml`
marker `md`. Tests: `tests/marketdata/conftest.py` (fakeredis + lupa, fixed `run_id` reader, fake `AofWaiter`),
`test_store.py` (22: atomic flush on a late bad argument, ex-leader token, `NO_BOOT` on missing / older / foreign / newer
sentinel and on a `run_id` change with the sentinel unchanged, idempotent re-write, revision gets a new higher
`write_seq`, rollback set holds N, silent re-write, tick ids after a trim, lease needs only the token, lock / renew /
release, boot write + `STALE_SEQ` + finish, quarantine, non-transactional pipeline and no `EVAL`, NOSCRIPT reload,
connection error re-runs the hook, no message text in errors, scripts never name `KEYS` / `SCAN` / `INFO`),
`test_store_client.py` (9), `test_acl_live.py` (marker `md`, 2), `tests/deploy/test_redis_md_compose.py` (2, static ACL
equality with design §A2). G: pytest 3377 passed / 52 skipped, mypy strict clean (221 files),
`verify_execution.py --strict` 11 / 11; the lockout real-tree scan passes with the store. The live ACL test was run
once against the dev `redis-md` (2 passed: the real flush with `WAITAOF` confirmed ≥ 1, every listed command refused to
`md_writer`, `md_reader` read-only, default user off, `md_health` PING only); the container was removed afterwards.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) the dev `redis-md` listens on `127.0.0.1:56383`, not 56382, which design §A2 reserves for the PC stack's `redis-md`
(S36) so both can run side by side; the engine Redis already follows this pattern (dev 56379, PC 56380);
(3) the dev engine Redis has no credentials, so the dev `redis-md` users carry throwaway `*_dev_only` passwords inline
(as `xtnl_dev_only` for PostgreSQL); `test_acl_live.py` reads them from the compose file and
`ORDER_SERVER_TEST_REDIS_MD` names the host only (`redis://127.0.0.1:56383/0`); (4) fakeredis has no `INFO` either, so
besides the `AofWaiter` the runner takes an injectable `run_id` reader (default `INFO server`); redis-py's connect
callback does not fire under fakeredis, so tests call `mark_reconnected()` as that callback does (the live test covers
the real callback); (5) two scripts beyond the plan's list, both needed by S16 and both under token and lock:
`BOOT_WRITE` (bootstrap step 3 / 4 writes, the only user of `STALE_SEQ`) and the `LEASE` registry entry; `QUARANTINE`
also removes the stored member (the S16 "moved to quarantine"); (6) the boot sentinel's generation is written
`epoch_rev.n` (`MdGeneration.__str__`); (7) tick retention (`XTRIM MINID`), the closed-bar queue and the release rule's
use of `write_and_wait` are S15; the rollback reconciliation logic and the read port are S16.
Review fixes (2026-11-16): every script now also checks the stored state it compares or increments before its first
write (`md:meta` `first_bucket` / `last_bucket` / `write_seq` in `FLUSH`, `write_seq` in `BOOT_WRITE`, `quarantined`
in `QUARANTINE`: absent or digits, else `BAD_ARGS meta_state`), and every ARGV number is bounded (times 13 digits,
`rollback_n` 3, `write_seq` 14), so a corrupted field can no longer half-apply a flush whose retry reads `UNCHANGED`;
`BOOT_WRITE` refuses non-digit `first_bucket` / `last_bucket` / `last_write_ms` / `quarantined`, two entries with one
`write_seq` (`STALE_SEQ`) and a rollback member whose tail is not a bar member; `quarantined` is reserved in `FLUSH`;
`FINISH` takes the connection's `run_id` at send time and gives `NO_BOOT` when the sentinel's differs; the builders
enforce `1 <= rollback_n <= 64` and `TickWrite` refuses a malformed price, source or status at build time;
`write_and_wait` reports `aof_error = reconnected` (not confirmed) when the runner's connect counter changed between
the scripts and `WAITAOF` (redis-py reconnects silently on checkout); `ScriptRunner.run_sequential` /
`MdWriteClient.run_sequential` stop at the first non-`ScriptOk` reply (dependent bootstrap steps must use it), and the
runner documents that a `ScriptFailed` Lua runtime error means unknown, possibly partial state. Not done: the optional
"`UNCHANGED` only when the rollback entry also exists" self-heal (a trimmed entry would re-emit an old bar), now
unnecessary since pass 2 has no remaining argument- or state-driven failure. Tests: `test_store.py` +14,
`test_store_client.py` +1 (reconnect before `WAITAOF`), `test_acl_live.py` now asserts redis-py's real connect callback
(counter ≥ 1, stale after a pool disconnect) and runs a second flush into the existing tick stream, `LEASE`,
`BOOT_LOCK`, `RENEW`, `BOOT_WRITE` + `FINISH` (sequential), `QUARANTINE` and `RELEASE` as `md_writer` (run against the
dev `redis-md`: 2 passed; container removed). G: pytest 3392 passed / 52 skipped, mypy strict clean (221 files),
`verify_execution.py --strict` 11 / 11.

* **C:** `src/marketdata/store/__init__.py`, `keys.py`, `scripts.py` (Lua as `Final` strings, ST-7: `FLUSH` checks the
  writer token, `md:boot` equality and the `run_id` in ARGV, else `NO_BOOT`; adds ticks with explicit ids and a
  monotonic guard; rewrites forming bars; replaces closed buckets (`ZREMRANGEBYSCORE` + `ZADD`) with their hash; keeps
  `md:bars_rev`; writes the rollback set `md:rb:{series}` with score = a per-series `write_seq` (`HINCRBY` in `md:meta`)
  and trims it to N, R11-M16; updates `md:meta` and `XADD md:events MAXLEN ~`; `LEASE` checks the token only;
  `BOOT_LOCK` / `RENEW` / `RELEASE` / `FINISH` (writes `md:boot` last); `QUARANTINE`), `runner.py` (SCRIPT LOAD of every
  script on each (re)connect in the same hook that reads `INFO server run_id`; `EVALSHA` through a non-transactional
  pipeline, `transaction=False`; NOSCRIPT reload, R11-m), `client.py` (a read client with 500 ms and a write client with
  2 s timeouts; one dedicated writer connection for flush plus `WAITAOF`; injectable `AofWaiter` with the real
  `WAITAOF 1 0 1500` and a fake for fakeredis, ST-10; sockets closed without `QUIT` on terminate).
  `tests/marketdata/conftest.py` (fakeredis plus lupa, as `tests/cache/conftest.py`);
  `tests/marketdata/test_acl_live.py` (marker `md`, copy of `tests/cache/test_dashboard_acl_live.py`; skipped unless
  `ORDER_SERVER_TEST_REDIS_MD` names the dev `redis-md`).
* **M:** `deploy/docker-compose.dev.yml` (`redis-md` service under compose profile `md`, so a plain `up` is unchanged;
  AOF on, the same ACL, following how the dev stack provides the engine Redis credentials); `pyproject.toml` marker `md`.
* **Tests:** a flush is atomic; an ex-leader token is refused; `NO_BOOT` on a missing, older or foreign sentinel and on a
  `run_id` mismatch; lease writes need only the token; the rollback set holds N with a monotonic `write_seq`, a revision
  of an already acknowledged bar gets a new `write_seq`; NOSCRIPT reload; live ACL test (marker `md`): `md_writer`
  cannot run ACL, CONFIG, FLUSHDB, FLUSHALL, KEYS, SCAN, DEBUG, SHUTDOWN, REPLICAOF, MIGRATE, FUNCTION or SCRIPT FLUSH or
  touch a key outside `md:*`; one real flush with `WAITAOF` as `md_writer`; `md_reader` cannot write; default user off.
* **Resolves:** ST-6, ST-7, ST-10, R11-M16 (store), R11-m (pipeline and SCRIPT LOAD under the ACL).
* **Risk:** `uuid`, `secrets` and `random` are refused by the lockout; the writer term nonce comes from the runtime
  through a port, and back-off jitter is derived deterministically from the term nonce.

### S14 MD-3: tick path

Status: done 2026-11-16. Built (all pure and synchronous, no I/O; prices never in a `repr`, a log or an error):
`src/marketdata/ticks.py` (`MdHook.on_price(dto, snapshot)` with the session's `MdHook` signature, bounded work per
tick: per `(adapter, symbol)` a bounded tick ring (`RingTick` with status `tradeable` / `indicative` / `snapshot`,
oldest dropped and counted, `drain()` for S15), the validated latest quote (tradeable ticks only; a failing tick clears
it), the PR6 coverage intervals (`on_price_subscription` with the orchestrator's signature, `seed_subscriptions` for the
intervals already open when the hook is bound; a bucket is covered only when one interval spans it and the hook was
running at its start), `note_drop_totals` (the adapter's cumulative `dropped_price_frames`; the first reading per
adapter baselines every market it has, after it a missing market reads as 0 and a market created later starts at 0
from the last reading; an increase or a counter that went back flags the buckets between two readings), `note_missed` (a range the hook's guard
skipped), `note_frame` (heartbeats for the receipt-gap watch), `HookCounters`; per adapter a `ClockSkew` whose p50 /
settle are cached by `advance(now)` so the price path never sorts; `advance` finalises every bucket at
`T + p50 + settle`, blinds silent subscriptions and then resets the forming bars of a market in an open receipt gap;
`clock_step(step)` blinds the stretch at risk (`[last wall, last wall + monotonic delta]` at the pre-step p50) plus
the forming buckets, resets the forming bars, re-anchors the receipt-gap watch (`ReceiptGapDetector.rebase`) and
re-baselines every ClockSkew; refused symbols are remembered and counted once); `src/marketdata/tickbars.py` (`TickBarBuilder`: 1-minute
bid and ask OHLC from tradeable, broker-timed, non-snapshot ticks; exact 5m / 15m / 1h roll-ups (open of the first
child with ticks, close of the last, extremes, summed counts) with worst-child quality; finality at the first tick of a
later bucket or the cap; late ticks and late flags re-emit the minute and every final roll-up holding it with
`revision + 1` only when content or flags changed, keeping the first `final_at_ms`; non-final minutes and open
roll-ups kept in start order so the price path never sorts, pruned only when the floor moves; retained window aligned to the largest roll-up, older ticks
`TOO_LATE`; quality `blind` > `degraded` (drops / lost updates / rejected or receipt-timed ticks) > `partial` (not
covered) > `complete`, capped at `partial` under `filtered` delivery; `close_eligible` = covered throughout, no drops,
not blind, last tick broker-timed; `boundary` flag within the adapter's timestamp resolution; `TickBar(bar,
close_eligible, boundary)` in a bounded hand-off buffer); `src/marketdata/skew.py` (`ClockSkew`: §A9 fresh-sample
filter `fresh_sample`, window of 512, nearest-rank p50 / p99, cold `COLD_SKEW` below 20 samples, settle via
`timing.settle_ms` plus resolution, `rebaseline`, the settled rule, edge-triggered page signals `skew_p50_high` /
`skew_p50_cleared` (5-minute hysteresis) and `skew_not_settled` (10 minutes after the later of the open and the
baseline, once per baseline), `SkewSnapshot` for `md:skew`); `src/marketdata/clockstep.py` (`WallMonoDetector`, step
over 2 s either way, `ClockStep` with the span of both wall frames plus the last wall reading and monotonic delta; `ReceiptGapDetector` per `(adapter, market)`: found,
extended at each check, ended by the next frame or the interval's close). Tests: `tests/marketdata/test_tickbars.py`
(21: exact roll-ups equal to the raw ticks and the 1m children, worst-child quality, each quality class,
receipt-timed ticks, `close_eligible` true under filtered delivery with complete coverage and false with a drop or an
uncovered minute, late-tick revision of the minute and its 5m / 15m roll-ups, flag revisions only on change, too-late
refusal, boundary flag, reset, empty minutes, bounded buffer, no prices in `repr`), `test_skew_clockstep.py` (12: cold
start, settle clamp low / mid / high plus resolution, negative p50, re-baseline needing 20 new samples, settled
spread, p50 page hysteresis, not-settled page once per baseline, fresh-sample filter, both step directions, receipt
gap found / extended / ended, heartbeats, unwatched adapters), `test_ticks.py` (19: the hook through
`MarketDataHookGuard` with the session's `MdHook` type, finality at the next bucket and at the cap, ring overflow
never degrades a closed bar, filtered `close_eligible`, hook started mid-bucket, unsubscribed stretch, drop readings
incl. a late reading revising a final bucket, receipt-timed and rejected ticks, snapshot / indicative capture,
quote validation, late tick revision, clock step blinding the overlapping buckets plus forming reset plus skew
re-baseline, receipt gap, skew samples only from fresh ticks, refused symbols, no prices in `repr`). G: pytest 3444
passed / 52 skipped, mypy strict clean (225 files), `verify_execution.py --strict` 11 / 11; the lockout real-tree scan
passes with the four new modules.
Review fixes 2026-11-16: a market's first drops after an empty (or earlier) first reading are flagged (per-adapter
baseline; missing = 0; markets created later start at 0); a wall-clock step blinds only the stretch at risk, never
future broker minutes or past final minutes; the receipt-gap watch is re-anchored after a step (a backward step no
longer silences it); an open receipt gap resets the forming bars; revisions keep the first `final_at_ms` (minutes and
roll-ups); the price path no longer sorts (ordered deques, prune only when the floor moves, refused symbols cached and
counted once). New tests: `test_ticks.py` +6 (empty first drop reading, market created after the baseline, forward
and backward wall steps, gap reset of forming bars, gap watch after a backward step), `test_skew_clockstep.py` +1
(`rebase`), `test_tickbars.py` (revision keeps `final_at_ms`). G: pytest 3451 passed / 52 skipped, mypy strict clean
(225 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) the receipt-gap detector watches only adapters with a stated keepalive interval (`keepalive_ms`, none by default):
the hook sees ticks, not heartbeats, so without them a quiet market would read as a gap and blind its buckets; S31
passes the interval together with a heartbeat feed (`note_frame`); (3) only a wall-versus-monotonic step re-baselines
the ClockSkew; a receipt gap blinds that market's buckets but keeps the adapter's samples; (4) the filtered-delivery
cap is applied to the bar's one `quality` (Bar has no separate high / low quality); the close is judged by
`close_eligible` alone, as S17b's per-field eligibility expects; (5) `ts_source` None is treated as receipt-timed
(fail closed); rejected ticks (bad prices, broker time more than 5 s after receipt) count as lost updates of their
bucket; indicative ticks never become the latest quote; (6) the hook builds 1m always and the 5m / 15m / 1h roll-ups
by default (`rollup_specs`), so demand filtering and shedding of undemanded TICK specs stay with S15 / S18; closed bars
wait in a bounded per-symbol hand-off buffer (2048) until `md-writer` (S15) moves them into its 6 h queue; (7) S31 must
call `seed_subscriptions(orchestrator.price_subscriptions(adapter))` and a first `note_drop_totals` when it binds the
hook, else the first buckets read `partial` (conservative); (8) no `.claude/` change is needed by this stage (the
market-data rule file is part of the pending S11 proposal); (9) `note_drop_totals` takes the adapter's own
`dropped_price_frames` (the drops before the hook), not the orchestrator's `dropped_prices_by_market`: that adds the
post-hook drops (a full price topic, `backpressure_overflow`) whose ticks the hook did see, and would turn off the D6
close check whenever the topic is full; S31 passes `adapter.dropped_price_frames` (no orchestrator change); (10) a
receipt gap resets the forming bars only after `advance` has finalised the buckets that were due, so a bucket that
only waited for the cap keeps its prices and reads `blind`; a gap's samples are still kept (deviation 3).

* **C:** `src/marketdata/ticks.py` (`MdHook.on_price(dto)`: O(1); bounded per-symbol ring; validated latest quote;
  forming 1-minute bars per side; drop counters; RECEIPT-timed ticks make a bucket `degraded`),
  `src/marketdata/tickbars.py` (1-minute builder from tradeable, broker-timed, non-snapshot ticks; exact 5m / 15m / 1h
  roll-ups with worst-child quality; finality at the next bucket's first tick or `T + p50 + settle`; boundary flag;
  under filtered delivery high / low quality capped at `partial`; a separate per-bucket `close_eligible` flag = coverage
  complete (subscribed throughout per PR6 intervals, no drops or lost updates in the bucket, last tick broker-timed),
  R11-M01; it consumes `PriceDTO.ts_source` / `received_ms` from S01 / S03 and the per-market SUBSCRIBED intervals of
  S05's `on_price_subscription`, hence the S05 dependency), `src/marketdata/skew.py` (ClockSkew per adapter: fresh-sample filter, p50 / p99, settle clamp, re-baseline
  after a step, "settled" rule, page signals), `src/marketdata/clockstep.py` (wall-versus-monotonic detector and
  per-subscription receipt-gap detector).
* **Tests:** exact roll-ups; quality classes; a late tick makes a revision; ring overflow never degrades a closed bar;
  skew settle clamp and re-baseline; a clock step blinds overlapping buckets; `close_eligible` true under filtered
  delivery when coverage is complete.
* **Resolves:** R11-M01 (tick-side input), §A4 TICK base, §A9.

### S15 MD-4: `md-writer`

Status: done 2026-11-16. Built: `src/marketdata/writer.py` (`MdWriter`: `run(stop, stamp)` loops every 250 ms
until the session's stop flag and stamps through `StampPort`; each `cycle` runs its own `WallMonoDetector` (a step
goes to `MdHook.clock_step`), `MdHook.advance(now)`, drains every ring (ticks into a bounded per-symbol pending
buffer, newest kept and the oldest counted; closed TICK bars checked with `check_tick_bar`, failures sent to
`md:quarantine` through `QUARANTINE`) and writes one `FLUSH` per symbol (ticks, forming bars written only on change
and cleared once final, the queued closed bars of every series of the symbol) through `write_and_wait` when the flush
carries closed bars, otherwise through `run` with no `WAITAOF`; closed-bar
queue per series keyed by bucket (a newer revision replaces a queued one), bound 360 = 6 h of 1-minute bars, the
oldest dropped at the bound (TICK: absent = `blind`; BROKER: `refetch(series, from, to)` request), `queue_bar` for
`md-bars` (S17); release rule: bars leave only on `ScriptOk` and a confirmed `WAITAOF` (or AOF off), a timeout or
error keeps them queued (`counters.aof_wait_timeouts` = `md_aof_wait_timeouts_total`) and the next flush re-writes
them idempotently, a revision queued while a write is in flight stays queued; ticks leave on the script reply and are
never re-sent after one; released bars that produced an event go to `on_released` listeners as `BarEndEvent`
(indicator jobs, S18; wake-ups, S16); a connection error, timeout or the 4 s flush cap puts the ticks back, keeps the
queue and marks the series GAPPED until a write for them succeeds; `NO_BOOT` / `NO_WRITER` pause ordinary writes and
call `request_bootstrap` once (`install_boot` resumes; a writer built without a sentinel waits with reason `start`);
retention every 60 s through the new `RETAIN` script (ticks `XTRIM MINID` 2 h plus the 200 000 cap; TICK 1m 7 days,
TICK 5m / 15m / 1h 90 days); memory shedding from `INFO memory` every 10 s at 75 / 80 / 85 / 90 % in the §A8 order,
each step paged once when entered and left 5 points below its threshold, the 70 % page, and one page for a memory
refusal (`OOM`) after the last step; `WriterStatus` for `md show`). `src/marketdata/store/scripts.py`: `RETAIN`
(token, exact `md:boot`, `run_id`; checks every floor and the stored `first_bucket` before its first write; `XTRIM
MINID` then `MAXLEN` on the tick stream, `ZREMRANGEBYSCORE` on TICK series below their floor, `first_bucket` moved to
the oldest kept bucket; never `md:bars_rev`, `md:rb` or `md:events`), `retain_call` (refuses a BROKER series or another
symbol), `parse_retain_reply`, `TICK_MAXLEN`. `src/marketdata/store/client.py`: `MdWriteClient.memory_ratio()`
(`INFO memory`, None for `maxmemory` 0). `src/marketdata/ports.py`: `PagePort`. Tests:
`tests/marketdata/test_writer.py` (25: release on `WAITAOF` with listener events; AOF wait timeout and error keep bars
queued and the re-write is idempotent (no new event, member or `write_seq`, ticks not re-sent); AOF off releases on
the reply; a revision queued mid-flush stays queued; Redis down keeps the newest ticks and every bar, series GAPPED,
recovery writes the newest ticks; a flush over its budget is a store error; `NO_BOOT` requests one bootstrap and
resumes on `install_boot`; `NO_WRITER`; a writer without a sentinel writes nothing; queue bound with BROKER re-fetch
and TICK blind; forming written / unchanged / cleared; a failing TICK bar quarantined; retention by `MINID` and floors,
BROKER never trimmed; shedding steps paged in order with the retention floors of each level, undemanded TICK specs
not stored at step 4 while BROKER is, hysteresis; OOM after the last step pages once; a failing memory read changes
nothing; stop flag; the writer's own clock-step detector; the writer imports no adapter / runtime / bus module and
completes while a broker read hangs; no price in logs, pages, status or counters; `RETAIN` script trims and moves
`first_bucket`, keeps the rollback set, refuses BROKER / other-symbol floors, `NO_BOOT`, and refuses a corrupt
`first_bucket` before trimming; `memory_ratio`). G: pytest 3476 passed / 52 skipped, mypy strict clean (226 files),
`verify_execution.py --strict` 11 / 11; the lockout real-tree scan passes with `writer.py`.
Review fixes (2026-11-16): (a) one deadline per cycle (`CYCLE_BUDGET_S` = stamp budget 5 s less 1 s) shared by the
flush, the memory read and `RETAIN`, each also capped at 4 s; a flush that hit a store error or the deadline skips the
cycle's maintenance, so a hung connection never makes the stamp stale (which would restart the task and lose the queue);
(b) `WAITAOF` only on a flush that carries closed bars (§A8), so tick / forming-only flushes never block on the disk
nor count `aof_wait_timeouts`; (c) a bar the codec cannot hold (e.g. a 13-decimal price) is found when queued: a closed
bar is dropped and counted (`unencodable`, its bucket stays blind), a forming bar is skipped (counted once per bucket,
an older stored record cleared), a quarantine entry that cannot be encoded is dropped, `quarantine_call` and per-entry
encoding are guarded, and a `flush_call` error puts the taken ticks back, so nothing stops the cycle or holds back the
other series and ticks of the symbol; (d) ticks go back only after a store error, a `ScriptRefused` or an `OOM` (Redis
refuses a script's first write only); any other `ScriptFailed`, an unparseable `ScriptOk` or a missing reply drops them
(`ticks_unconfirmed`) since a re-sent tick would be stored twice, while bars stay queued; (e) the `OOM` page re-arms
only when shedding drops below step 4 or after 240 clean flushes, an `OOM` below step 4 makes the memory read due at
once, and an `OOM` while the memory level is unknown (no port, failed read) pages once; (f) memory reads and retention
are scheduled on the monotonic clock (a backward wall step no longer pauses them); (g) `track_retention(series,
markets)` lets earlier terms' series and tick streams be trimmed; **S16 obligation:** the bootstrap seeds it from
`md:registry:series` and the stored `md:meta:*` / `md:ticks:*` so a symbol no longer subscribed is still trimmed. Tests
now 37 (adds: cycle and `run` stamp gaps bounded while the write port, memory read and `RETAIN` hang; tick / forming-only
flush sends no `WAITAOF`; 13-decimal price; tick re-send per reply kind; alternating OOM / success pages once and re-arms
after clean flushes; OOM below step 4 reads memory at once and pages when memory is unknown; backward wall step keeps
memory reads and retention running; retention of an earlier term's series); the hollow "broker read hangs" part of the
import-scan test was removed. The deadline uses `time.monotonic` and `asyncio.timeout` (the lockout scanner's asyncio
allow-list has no `timeout_at` / `get_running_loop` outside `src/riskmgr`, and it was not widened). G after the fixes:
pytest 3488 passed / 52 skipped, mypy strict clean (226 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) tick retention is a new `RETAIN` script run every 60 s rather than part of `FLUSH`, and it also applies the §A5 TICK
bar retention (1m 7 days, roll-ups 90 days), which no stage listed; both `XTRIM`s are exact (`MINID`, `MAXLEN`
200 000) instead of `MAXLEN ~` (fakeredis ignores `~`, and an exact trim of a few entries is cheap); (3) no demand
port exists before S18: `is_demanded` is optional, and without it every TICK spec counts as undemanded at shedding
step 4 (memory first); normally every built TICK spec is stored (the demand filter of §A4 is S18); (4) the queue bound
is a count per series (360 bars, any spec), and a newer revision of a queued bucket replaces the queued entry rather
than queueing a second one; (5) the writer owns `MdHook.advance` and the clock-step detector: S31 must not call
`advance` elsewhere; (6) metrics are plain counters on `MdWriter.counters` (`aof_wait_timeouts` is
`md_aof_wait_timeouts_total`); S32 exports them; pages go through the new `PagePort` (S31 wraps `EngineSession.page`);
(7) the §B14 FAILED for an `OOM` after the last step and for the 6 h bound is S30: here they are counted and paged
once; (8) `NO_WRITER` also requests a bootstrap (step 1 re-SETs a missing token; a fenced ex-leader's bootstrap fails
its lease check in S16); (9) the dev-only live ACL test does not yet run `RETAIN` (its commands, `XTRIM`, `ZRANGE`,
`HDEL`, are inside `+@stream` / `+@sortedset` / `+@hash`); (10) no `.claude/` change is needed by this stage.

* **C:** `src/marketdata/writer.py`: task body (stamp through `StampPort`, watches the stop flag); every 250 ms drains
  the rings and flushes per symbol; closed-bar queue (6 h of 1-minute bars per series; never dropped while ticks are
  shed; at the bound the oldest bar is dropped, its bucket becomes `blind` and a BROKER bucket gets a re-fetch request);
  release rule (pipelined `EVALSHA`, then `WAITAOF` on the same connection; bars leave the queue only on a reply ≥ 1;
  timeout keeps them queued; metric `md_aof_wait_timeouts_total`; with AOF off no `WAITAOF`); indicator jobs enqueued
  after the write; Redis errors mark series GAPPED; memory shedding at 75 / 80 / 85 / 90 % (`INFO memory`) in the
  §A8 order, each step paged; tick retention `XTRIM MINID`; a `NO_BOOT` reply requests a bootstrap (S16).
* **Tests:** an AOF wait timeout keeps bars queued and re-writes them idempotently; Redis down: rings keep the newest
  ticks, the queue keeps bars, series GAPPED; shedding order; queue bound; the writer never awaits broker I/O.

### S16 MD-5: bootstrap, SM12, reset flag, read port, pull cursor, re-hash

Status: done 2026-11-16. Built: `src/marketdata/bootstrap.py` (`MdBootstrap`: steps 1-5 inside `md-writer`, each attempt
inside a 4 s budget: (1) token SET only when missing, at a term start or on `md_writer_reset` (a fresh token, then
`MdWriter.use_token`), each after `LeaderCheckPort.confirm_leader()`; another writer's token in-term is `no_writer`;
(2) `BOOT_LOCK`, `RENEW` every 2 s between steps, lock released on failure; (3) `classify` against this node's record
(`NodeRecordPort`; `intact` / `store_foreign` / `store_lost`, plus `fresh` for no record and an empty store), every
known series (writer memory, `md:registry:series`, the new stored-series index) marked GAPPED from `min(meta
last_bucket, writer's last final bucket) + timeframe` (EMPTY with neither; FAILED kept), `md:forming:*` cleared,
rollback set reconciled both ways (store to memory only for entries that decode, name their bar end and verify under a
retained key id; memory to store for every `write_seq` the store lacks), `md:meta` `sm12` / `gap_from`; (4) registry
from the in-process lease set (`LeaseSourcePort`, `HeldLease`) plus every live lease in the store (lease index +
`EXISTS`), in-process leases re-asserted by `LEASE` after `BOOT_WRITE` (sequential); (5) `FINISH` with generation
`(token epoch, n)` (n = attempts in this term), then `install_boot`, `track_retention` of every stored series and tick
stream, record saved best-effort. `MdReset(generation, class, reason, notice, counted)` to in-process listeners;
`notice` only for `store_lost` / `store_foreign` or an in-term `run_id` change, `NO_BOOT` or `NO_WRITER`, never for a
trial or post-clear bootstrap; `ResetBudget` (3 in one hour → `on_fault("md_resets")`, counting only in-term
`NO_BOOT` / `NO_WRITER` / `run_id` resets; `clear_budget()` on an accepted clear resets counters and window); 3 failed
bootstraps in a row → `on_fault("bootstrap_failed")` (trial failures, `not_leader`, `no_writer` never count). SM12 as
one table `SM12_TRANSITIONS` and `SeriesStateBook` (state, gap count, backfilled range for BACKFILLING reads).
`MdWriterService` = the `md-writer` task body: writer cycle, a stamp, then one of: a trial bootstrap
(`request_trial`), a due bootstrap (term start, `NO_BOOT` / `NO_WRITER` read from `MdWriter.bootstrap_wanted`,
`md_writer_reset`, `request_post_clear`) with back-off 1 s doubling to 30 s, or one work-item step (`submit`:
`md_writer_reset`, `md_rehash`; completion `applied` / `refused` / `moot` through a callback); writer GAPPED series
→ SM12 GAP, released TICK bars → LIVE.) `src/marketdata/events.py` (`events_after` / `rollback_events`: entries after
the cursor's `write_seq` in `write_seq` order with revisions of older ids; `CURSOR_EXPIRED` for another or missing
generation, a trimmed or overtaken `write_seq`, or a corrupt entry; the restart form `EventCursor(last_decided_bar,
0)`; `RefetchQueue`: BROKER only, coalesced per series, bounded, also the `MdWriter` `refetch` callback).
`src/marketdata/port_impl.py` (`MdPort`: read sessions `md:boot` + `md:writer` before and `md:boot` after,
`generation_changed`; STALE_NODE when the writer epoch is not `leader_epoch()` (unknown = stale); decision reads gated
by the book (LIVE, or BACKFILLING with the range complete; FAILED or global FAILED → `failed`); read-side checks with
the keyring's retained ids; a failing bar returned `blind` (zeros) and handed to `MdWriter.quarantine_stored`; a
missing key id `blind` without quarantine; `as_of_ms` over `md:bars` + `md:bars_rev`; `quote` / `forming_bar` from the
hook; `best_bars` display only; `wake` routes released events to `on_bar_closed` / `on_bar_revised`;
`as_market_data_port` type check). `src/marketdata/rehash.py` (`MdRehash`: pages of 128 per step, members verified
under their old retained id rewritten silently through `FLUSH` + `WAITAOF`, current-id members skipped, unverifiable
ones counted and left; `rehash_key` in `md:meta` per series; `RehashStatus`; `retire_allowed(now, window)`).
`src/marketdata/store/reads.py` (`StoreReader` over either client: `head`, `boot`, `metas`, `rollback`, ranges,
pages, newest n, superseded, registry, stored index, `live_leases`; member-form parsers). Changed:
`store/scripts.py` (`LEASE` also writes / deletes `md:registry:leases`; new `INDEX` script and `index_call`),
`store/keys.py` (`LEASE_INDEX`, `STORED`, `lease_index_field`, `market_field`), `store/client.py`
(`current_run_id`, `run_one`), `writer.py` (in-memory rollback set bounded at N, last final bucket per series,
`last_write_ms`, `INDEX` pipelined with the flush once per sentinel, `quarantine_stored`, `use_token`,
`known_series` / `known_markets`, `is_queued`, `BarEndEvent.revised`), `types.py` (`EventCursor.generation`, not
compared; `BarEndEvent.revised`), `ports.py` (`EventsRead.next_cursor`). Tests: `tests/marketdata/test_bootstrap.py`
(27: restart between bars raises the generation, one notice, consumer resumes with no duplicate and no price in logs;
FLUSHALL gives `NO_WRITER`, `store_lost` and the rollback set restored from memory; older AOF `store_lost`, memory
entries back, `gap_from`; Q17 off; term start no notice, not counted, store to memory hash-checked, no quarantine;
trial on an empty store writes `md:boot`; 3 resets → one fault, refused clear, accepted clear and post-clear bootstrap
do not re-trip; revision pulled by `write_seq` with no wake-up; consumer stopped 3 h leaves writer memory at N; registry
from held + live store leases (expired skipped); stored series handed to retention; index written; record save failure;
`md_writer_reset` fresh token not counted; `md_rehash` moot; 3 failures → one fault; trial / `not_leader` never count;
fenced ex-leader; back-off; stamps; stop flag; classification table; notice rule; budget window; SM12 table; book
gate; restart-form cursor), `test_port_impl.py` (19), `test_rehash.py` (3: rotation with zero quarantine, both ids
read mid-way, no `bar_revised`, never CORRECTED-comparable, second request moot; retirement after completion plus one
window; a retired id skipped and read `blind` without quarantine), `test_store_reads.py` (6). G: pytest 3543 passed /
52 skipped, mypy strict clean (231 files), `verify_execution.py --strict` 11 / 11; the lockout real-tree scan passes
with the four new modules.
Review fixes (2026-11-16): restart-form cursor expires on a hole in `(cursor, newest]` (backfill-ordered set); boot lock
value stable per node and term (`boot_{node}_{nonce}_{epoch}`, re-entered across attempts) and `lock_busy` never counts
toward the failure streak; classification uses this process's fresher in-memory record; the store-side `held` set is
every stored score (`StoreReader.rollback_seqs`), so a corrupt `md:rb` member no longer gives `STALE_SEQ`; the re-hash
rewrites rollback entries (store and memory) and tells the writer on `NO_BOOT` / `NO_WRITER`
(`MdWriter.note_lost_boot`); `bootstrap()` maps any other exception to failure `internal`; a stamp after the
bootstrap, before the record save; the first failing reply names the failure; `BOOT_WRITE` prunes expired lease-index
entries (`StoreReader.lease_index`, at most 256 per bootstrap). Tests added: `test_bootstrap.py` +6 (backfill-ordered
restart cursor, own leftover lock re-taken, busy lock never counted, corrupt rollback member, internal error releases
the lock, first failing reply), older-AOF drill without the explicit record save, wake-up callbacks registered in the
revision test, lease-index pruning; `test_rehash.py` +1 (`NO_BOOT` refusal tells an idle writer) and the rollback set
after a re-hash. G after the fixes: pytest 3550 passed / 52 skipped, mypy strict clean (231 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) two `md:*` hashes beyond design §A3, both ids only and both needed to avoid `KEYS` / `SCAN`:
`md:registry:leases` (`consumer:spec_id` → lease value, written atomically by `LEASE`, so the bootstrap can find
"every live lease in the store") and `md:registry:stored` (series and tick streams written, by the new token-only
`INDEX` script, so a later term finds the stored `md:meta:*` / `md:ticks:*` for retention, the S15 obligation);
expired lease-index entries are skipped and pruned by `BOOT_WRITE` (review fix), and `md:registry:series` entries are added but never removed
(soft state, bounded by the symbol universe); (3) a fourth class `fresh` (no record, empty store; no notice);
`store_foreign` with a record needs `previous_term_epoch` from S31 (without it an intact store reads `intact`); a newer
sentinel this node wrote itself counts as its record (the record save after a bootstrap failed); (4) a term-start
bootstrap sends a notice only when the class is `store_lost` / `store_foreign` (the routine, intact one sends none);
`NO_WRITER` (a FLUSHALL) is treated like `NO_BOOT` for the notice and the budget; (5) this node's record is saved by
the bootstrap and on demand (`MdBootstrap.save_record` / `node_record`); S32's `arm_status` writer must save it every
5 s (S32 obligation); in-process the bootstrap classifies against this process's own record when it is fresher than the
stored one (same sentinel, newer `last_write_ms`, or none stored); (6) `EventCursor` gained
`generation` (excluded from ordering) and `write_seq` 0 means the restart form; events come in `write_seq` order, the
consumer orders by bar; (7) the post-clear and trial bootstraps are requested through `MdWriterService`
(`request_post_clear`, `request_trial`) and run in the writer task so no flush interleaves; a successful trial also
serves a pending `NO_BOOT` / term-start bootstrap; (7a) the restart-form cursor also needs every grid bucket from the
cursor to the set's newest bar end (a hole, a closed market included, is `CURSOR_EXPIRED`: read by bucket); (8) `demand` / `release` refuse until S18 builds the demand port;
(9) a silent re-hash moves the old-key member to `md:bars_rev` (FLUSH's replace path), so `as_of` picks the highest
revision with the current member first; the silent FLUSH path also rewrites, in place, a rollback entry holding the
replaced member (same `write_seq` and bar end), and the re-hash does the same in writer memory (an entry of an older,
superseded revision keeps its old id); (10) the S31 wiring must register `MdPort.wake` and the service's own
listener on `MdWriter.on_released`, pass `RefetchQueue` as the writer's `refetch`, build the writer with `boot=None`
(the service runs the term-start bootstrap) and pass `previous_term_epoch`; (11) no `.claude/` change is needed.

* **C:** `src/marketdata/bootstrap.py` (SM12; bootstrap steps 1-5; generation `(epoch revision, n)`; classification
  `intact` / `store_foreign` / `store_lost` against this node's record through `NodeRecordPort`; rollback-set
  reconciliation both ways, hash-checked; registry rebuilt from the in-process lease set **and** every live lease in the
  store, R11-m; the `md_reset` event always, but the INFO notice only for `store_lost`, `store_foreign`, or an in-term
  `run_id` change or `NO_BOOT`; the D7 "3 resets in one hour" budget counts only in-term resets that are not term starts, not
  operator-requested, not a clear probe's trial bootstrap and not the bootstrap re-run after a global clear (neither of
  the last two sends an MD_RESET notice), and an accepted clear resets the scope's counters and the one-hour window,
  R11-m; `bootstrap(trial=True)` runs the same steps for a clear probe and is allowed
  while the ARM is globally FAILED, R11-M09); `src/marketdata/events.py` (`events_since(series, cursor)` with `cursor =
  (bar_end_ms, last_write_seq)` returning every rollback-set entry with a higher `write_seq`, revisions of older ids
  included; `CURSOR_EXPIRED` judged on `write_seq` and generation; `refetch(series, from, to)` request, R11-M16);
  `src/marketdata/port_impl.py` (`MarketDataPort` implementation: read sessions check `md:boot` before and after;
  decision reads are bucket-addressed and return `DATA_UNAVAILABLE` unless LIVE or BACKFILLING with the range
  complete; read-side validation and hash check against the keyring's retained ids; a failing stored bar is returned
  `blind` and moved to quarantine; `as_of` reads across `md:bars` and `md:bars_rev`; `STALE_NODE`);
  `src/marketdata/rehash.py` (`md_rehash`: verify each member under its old key id, rewrite it under the current id
  through the ordinary write script, emit no `bar_revised`, progress in `md:meta`; `md show` reports completion; the
  previous key is retired only after that and after one correction window, R11-M13). Request work items (enqueued by
  the S32 handlers, never run in the operator loop): `md_writer_reset` (fresh writer token and a bootstrap; operator-
  requested, so it never counts toward the reset budget) and `md_rehash`, each completing its request as applied,
  refused or moot. After each bootstrap, hand every stored TICK series and tick stream (`md:registry:series`, the stored
  `md:meta:*` / `md:ticks:*`) to `MdWriter.track_retention` (S15 review fix), so `RETAIN` also trims symbols this term
  never writes.
* **Tests:** a `redis-md` restart between bars raises the generation and sends one INFO notice, and a fake consumer
  resumes with no duplicate; FLUSHALL while running gives `NO_BOOT` and a bootstrap; an older AOF; Q17 off; a term start
  sends no MD_RESET notice and does not count toward the reset budget; a trial bootstrap on an empty store writes
  `md:boot`; after a global FAILED from 3 resets in one hour, a refused clear, an accepted clear and the post-clear
  bootstrap within the same hour do not re-trip FAILED; a revision of an acknowledged bar is pulled by `write_seq` with no wake-up; a key rotation leaves zero
  quarantined bars and readers accept both ids until the re-hash completes; a restart quarantines no stored bar; a
  consumer stopped for 3 hours leaves writer memory unchanged.
* **Resolves:** R11-M09 (store side), R11-M13, R11-M16, R11-m (MD_RESET classes, reset budget, lease registry rebuild).

### S17 MD-6: `md-bars` (BROKER bars)

Status: S17a done 2026-11-16, review fixes applied 2026-11-16; S17b done 2026-11-16 (below). S17a built: `src/marketdata/brokerbars.py` (`MdBars`, the `md-bars` task
body: one pass per second inside a 20 s pass budget under the 30 s stamp; per series the live schedule (first read at
`T + p50 + settle`, re-reads 5 s / 30 s / 60 s until `decision_cap` with `XTNL_MD_BROKER_BAR_LAG_MS` only starting the
back-off; one range read per series reaching the bucket in progress); finality on broker-side evidence (a later bucket
in the same answer, closed or forming; two equal reads `broker_bar_settle_s` apart; the partial flag only where
`trusts_partial_flag` says PR3 confirmed it); one verification re-read at `min(final + 60 s, cap − 30 s)` (a change is
queued as `revision + 1`), else `unverified_final` (`MdBars.unverified`); validation before queueing through
`validate.split_response`, a bad bar quarantined once per bucket (`MdWriter.quarantine_stored`, check name only) and
its bucket blind, a forming bar evidence only; final bars compared with `md:bars` by keyed hash (`StoredBarsPort` /
`StoreBarsReader`; this task's memory only for a bucket the writer still holds queued): identical not re-written,
changed `revision + 1`, new revision 0; the error unit (a bucket adds at most 1, at its cap: invalid → `K` streak,
not final → `unresolved_streak`; a clean final bar resets both; decode errors and failed reads a metric and one `info`
page per episode; `K` invalid buckets → `on_fault(series, "invalid_bar")` once); counting pauses for an adapter
outage, a closed calendar, failed reads while REST is unknown or failing everywhere, and a cap passed while the task
was not running (60 s slack: a PC sleep counts nothing); broker outage (`AdapterHealthPort`: not STREAMING or REST
failing everywhere) pauses reads and counters, pages `broker_unavailable` once per adapter episode and GAPs the
series, never FAILED; SM12 BACKFILLING of the demanded `W + 3 × length` newest first in range pages (one bucket past
each page as evidence; `note_complete` per page; LIVE when done; a job resumes after a GAP, a long gap starts a fresh
one); the backfill lane (blind buckets after the cap, failed / truncated backfill buckets, `RefetchQueue` consumer
re-fetches, `md_backfill` via `request_backfill` completed `applied`, the gap-heal pass over stored holes inside
retention oldest first), at most 2 lane reads per pass after every live read, back-off 60 s doubling to 30 min,
`unfillable` after 12 answers that leave the bucket unknown or beyond `history_reach_ms`; `on_resolved` listeners
(`bucket_resolved` for S18); the coverage map in memory with counts and the SM12 state mirrored to `md:meta`
(`bars_phase`, `bars_k`, `cov_blind`, `cov_no_trade`, `cov_unfillable`, `sm12`); every series' fetch, decode and
validation in its own try/except, reads of one pass concurrent; ARM global FAILED (`global_failed`) stops it).
Changed: `writer.py` (`MdWriter.set_meta`: `md:meta` fields carried by the series' next flush, checked by the `FLUSH`
builder first, dropped once accepted), `ports.py` (`AdapterHealthPort.rest_failing_everywhere` returns `bool | None`,
None = unknown). Tests: `tests/marketdata/test_brokerbars.py` (55: first read time; re-read schedule to the cap and
the lane; lag only starts the back-off; final on a later bucket; forming bar never stored or counted; finality at the
17:00 New York break and Friday's close by two agreeing reads; a changed read restarts the agreement; trusted partial
flag; empty bucket rules; closed calendar; verification re-read changed / unchanged; `unverified_final`; 3 undecodable
answers then a valid bar leave LIVE with K = 0; corrupt bar quarantined at once and K buckets fault once; a clean
bucket resets the streak; failed reads count only while other priorities succeed (and not when unknown); a 30-minute
REST outage pages once, GAPs and never fails, then LIVE with the outage buckets re-fetched; not STREAMING is an
outage; a sleep longer than the cap counts nothing; one series' raising / garbage / NaN / wrong-instrument answer
leaves the other fetching; UNSUPPORTED stops reads; global FAILED stops it; identical stored bar not re-written;
changed stored bar → next revision; unreadable store defers; backfill newest first then LIVE; readable while
BACKFILLING; GAPPED backfills again; FAILED not read; blind bucket at the cap resolved in the lane with
`bucket_resolved`; unfillable after misses and beyond reach; `md_backfill` completion and refusals; consumer re-fetch
drained; gap heal; `md:meta` mirror holds states and counts only; no price in logs or pages; settings; review edges:
an empty read then failed reads to the cap stays blind in the lane, a store outage across the cap never counts, a
FAILED series refuses `md_backfill` (pending ones too), a raising health port is no outage, a clean later bucket keeps
K strict under skew drift, a capped empty bucket beyond the broker's real reach ends unfillable, a slow series skips
its backfill page without cutting the pass; an integration
run over fakeredis with the real `MdWriter` and `StoreBarsReader`, where a new term re-fetching the same history writes
nothing). G (after the review fixes): pytest 3605 passed / 52 skipped, mypy strict clean (232 files), `verify_execution.py --strict` 11 / 11; the
lockout real-tree scan passes with the new module.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) an empty bucket is `no_trade` only on a later **closed** bar in the same answer, a closed calendar, or a complete
empty answer at its cap (not on a forming bar or two empty reads, which a lagging broker would turn into false
`no_trade`); `no_trade` neither adds to nor resets the error unit; (3) the coverage map is kept in memory with counts
mirrored to `md:meta` (no per-bucket map in Redis); after a restart the gap-heal pass rediscovers holes (a closed
weekend costs one lane read per run of buckets); (4) `unfillable` = older than `history_reach_ms` (default the 1-year
BROKER retention until PR3 measures the broker's reach) or 12 answers in a row that leave the bucket unknown
(truncated, invalid, or empty without evidence: a capped empty bucket of a complete lane or backfill answer is
`no_trade` only when the same answer holds a closed bar later or earlier than it, and each lane read reaches one bucket
either side, so history beyond the broker's real reach ends `unfillable`, never `no_trade`; PR3 must still measure the
reach so `history_reach_ms` stops wasting those misses); failed reads only back off; (5) md-bars pauses its reads too, not only
the counters, while the adapter is in outage (the series are GAPPED and backfilled afterwards); (6) the FAILED record,
ping and the `fail_unresolved_n` decision are S30's: S17a calls `on_fault(series, "invalid_bar")` at K and exposes
`unresolved_streak` (status and counters); invalid buckets of a backfill page go to the lane and do not add to K (K
counts at live caps only); (7) the "awaiting broker bar" page at `T + post_grace` is left to the ARM POST side (S27 /
S28), md-bars has no notion of an armed POST; (8) `unverified_final` is kept per series in memory
(`MdBars.unverified`), not in the bar record; (9) a stored hash under a key id the ring no longer holds is treated as
changed (rewritten as `revision + 1`, one spurious `bar_revised`; only after a key is retired too early);
(10) `AdapterHealthPort.rest_failing_everywhere` widened to `bool | None` so the S06 "unknown" reaches md-bars;
(11) the S31 wiring must pass `MdWriter` as the sink, `StoreBarsReader` over the writer client's reader, the term's
`SeriesStateBook`, the same `RefetchQueue` as `MdPort` / `MdWriter`, a health port mapping
`RestBudgetStats.failing_at_every_priority` (None stays None), the adapter's `ClockSkew.estimate`, a
`trusts_partial_flag` from the PR3 result (default False), `on_fault` into S30, `complete` into the operator-request
store and `global_failed`; the demand source (`DemandSource`, series → `W + 3 × length`) is S18's, `demanded()` is a
fixed stand-in; (12) no `.claude/` change is needed; (13) review fixes (2026-11-16): a failed read clears the
bucket's "empty" flag, so only an empty answer that is still the latest at the cap makes `no_trade`; a bar final on
broker evidence whose store comparison fails (or that the writer refuses) is deferred: re-checked from the store every
5 s without a broker re-read, and at its cap moved to the lane uncounted (a `redis-md` outage never feeds
`unresolved_streak`); only a bar that reached the store resets the streaks, and a bucket older than the newest clean
one never adds to them (K stays "in a row" when the skew drifts); a raising `is_streaming` is unknown (counting
pauses, logged once, no page, no GAP), not a broker outage; `md_backfill` is refused `series_failed` for a FAILED
series and pending requests are completed `refused` when the series enters FAILED; each series' pass runs under its
own deadline, a series whose live read used more than half the pass budget skips its backfill page that pass, and the
lane and gap heal run with what remains, so one slow series cuts only itself (the lane can still wait while a series
stays that slow).

Status: S17b done 2026-11-16, review fixes applied 2026-11-16 (see after G). Built: `src/marketdata/crosscheck.py` (pure, no I/O, no clock of its own): `evaluate`
(per-field eligibility: the close only when the TICK bucket is `close_eligible`, high / low only when the TICK bar is
`complete`, none eligible → skipped `not_eligible`; tolerance `k` × the bucket's median clean spread, a field agrees
when it differs by **less** than the tolerance; a bar within tolerance of its own TICK bar is always a match; a failing
bar is a mismatch, labelled `shift_prev` / `shift_next` / `swap` only when that alternative passes the tolerance with at
least 0.5 × tolerance to spare on the same eligible vector, else `value`; the shift test skipped when the TICK bucket's
range is below 3 × its median spread); `TickRefBook` (the closed 15m TICK bars in memory, newest revision per bucket,
bounded per series and in series count); `CrossChecker` (per BROKER series: verdicts, `caveat` (the `source_mismatch`
flag), the mismatch streak in bucket order over checked buckets (a skip neither flags nor resets, a match resets),
`K` → `on_fault(series, "source_mismatch")` once per streak and `blocked`; a live bar waits up to 120 s for its TICK
bar (`poll`), then is skipped `no_tick_bar`; a backfill / lane bar is checked only when its TICK bar is in memory and
never counts as skipped; the skipped ratio over the newest 16 live buckets, more than half skipped → one `info` page
per episode; D6 status `ACTIVE` / `CLOSE_ONLY` / `UNAVAILABLE` from that window; `meta_pairs` (`d6`, `xc_checked`,
`xc_k`, `xc_mismatched`, `xc_skip_recent`, `xc_skipped`), `status`, counters per kind and skip reason). Changed:
`tickbars.py` (each minute keeps a bounded spread histogram of its clean ticks, at most 64 distinct values, else
unknown; `TickBar.median_spread` = the lower median, roll-ups merge their children; memory only, out of `repr`),
`writer.py` (`MdWriter.on_closed_tick`: every closed TICK bar drained from the hook goes to the listeners before
shedding; a raising listener is counted, `tick_listener_errors`), `brokerbars.py` (optional `crosscheck`: every
committed final bar, queued or identical to the stored one, is submitted, live-path bars as demanded buckets; `poll`
each pass; `forget` when a series leaves demand; the D6 status and `xc_*` counts join the `md:meta` mirror;
`BrokerSeriesStatus` gains `d6` and `mismatch_streak`; every cross-check call is contained). Tests:
`tests/marketdata/test_crosscheck.py` (27: eligibility table; an ineligible field never decides; no TICK bar / no
spread skipped; tolerance strictness; within tolerance always a match; shift only with the margin; quiet bucket skips
the shift test; swap on a quiet bucket; K mismatches fault once, skips neither flag nor reset, a match re-arms; streak
in bucket order; waiting for the TICK bar then `no_tick_bar`; history bar unchecked and uncounted; no TICK spec; skipped
ratio pages once above 8 of 16 and re-arms; D6 status transitions; meta / status carry states and counts only; a
raising fault sink; no price, spread or tolerance in logs or pages; the TICK book bounds and revisions; **golden runs
through `md-bars` with TICK bars from the real builder**: a filtered feed shifted by one bucket reaches
`source_mismatch` on the third live bucket (D6 `CLOSE_ONLY`, `shift_prev` seen), a flat Asian session with equal
closes never flags (D6 `ACTIVE`), a filtered bid / ask swap is detected, a raising TICK source never stops `md-bars`),
`test_tickbars.py` (+4: lower median, roll-up merge, distinct-value bound, not in `repr`), `test_writer.py` (+1: the
tick tap and a raising listener). The S17 golden items already built in S17a (three undecodable answers then K = 0, the
blind bucket in the lane with `bucket_resolved`, the 30-minute outage, failed reads counting only while POLL succeeds,
quarantine, one series' malformed payload, finality at the 17:00 break and Friday's close, the forming bar) stay in
`test_brokerbars.py`. G: pytest 3637 passed / 52 skipped, mypy strict clean (233 files), `verify_execution.py
--strict` 11 / 11; the lockout real-tree scan passes with the new module. Review fixes (2026-11-16): the writer's
tick tap now runs `check_tick_bar` first and hands a listener only a closed TICK bar that passed (a quarantined bar is
never a D6 reference; still ahead of the undemanded shed, so undemanded TICK series feed the book); `poll` contains a
failing TICK lookup per waiting bar (counted in `errors`, that bar skipped `no_tick_bar`; other bars and series are
still checked); `meta_pairs` sends zero `xc_*` counts for a series without state, so no stale streak stays in
`md:meta`; `forget(series, now_ms=)` keeps the state for one timeframe of the series (`poll` drops it after that;
`resume`, called by `md-bars` on re-demand, or the next bar keeps it), so a brief lapse of demand never resets a
partial streak; new `CrossChecker.clear(series)` for the operator clear (streak and fault re-armed, verdicts and
caveats kept, only buckets after the newest one judged count toward the next streak); the skipped-ratio page re-arms
only at a quarter or fewer skipped (hysteresis). Tests +7: `test_writer.py` (+2: a closed TICK bar failing PRICES or
FUTURE never reaches a listener), `test_crosscheck.py` (+5: poll with one raising lookup, a brief demand lapse keeps the
streak, operator clear re-arms the fault, a count swinging 8 / 9 of 16 pages once, and a golden run through the real
`MdHook` -> `MdWriter` (fakeredis) -> `on_closed_tick` -> `TickRefBook` path over simulated time: the shifted feed
faults on the third live bucket and the T + 1 neighbour is absent from the book at every check, so `shift_next` is
never shown). G after the fixes: pytest 3644 passed / 52 skipped, mypy strict clean (233 files),
`verify_execution.py --strict` 11 / 11.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) the design names no source for "the bucket's median clean spread": the TICK builder now keeps it per bucket in
memory (`TickBar.median_spread`, the lower median of the spreads of the ticks the bar was built from; a minute with
more than 64 distinct spreads has none and its buckets skip `no_spread`); spreads are never stored in `redis-md`, so
the cross-check works from the in-memory `TickRefBook` fed by the writer's new tick tap, and after a restart the
buckets built before it are not checked (they would not be `close_eligible` anyway); (3) "agree within tolerance" is
read strictly (difference < `k` × spread): at `k` = 1 a bid / ask swap moves every price by exactly the spread, which a
`≤` test would accept; a swap is still missed when the bucket's spread at the close is narrower than its median (PR3
data must confirm `k`); (4) every tolerance failure is a mismatch and counts toward `K`; the shift / swap rule decides
only the label (`value` otherwise), "passes it by a margin of at least 0.5 × tolerance" is read as the alternative's
largest difference ≤ 0.5 × tolerance, and the quiet-range skip applies to the shift test only (a swap is tested on
quiet buckets too); (5) the cross-check is evaluated once the BROKER bar is final, with the TICK bars present then:
the T + 1 neighbour is usually not final yet, so an early-labelled feed is mostly reported as `value` (still a
mismatch); a later TICK revision of a checked bucket does not re-check it (the next BROKER revision does), and a
verification re-read re-checks the bucket (run counters count both); (6) the D6 status before any live bucket is
`UNAVAILABLE`, and a mismatch is counted whatever the SM10 state (counting pauses gate the md-bars error unit, not the
cross-check verdict of a final bar); (7) the `source_mismatch` caveat and the blocked state are exposed
(`CrossChecker.caveat` / `blocked`); turning them into `DATA_UNAVAILABLE(source_mismatch)` reads and FAILED is S30 /
S31 (the fault goes to `on_fault`, as S17a's `invalid_bar`); (8) S31 wiring: build one `TickRefBook` per term,
register `book.note` with `MdWriter.on_closed_tick`, build `CrossChecker(book, k=MdSettings.crosscheck_k,
mismatch_k=MdSettings.mismatch_k, pager=…, on_fault=…)` with the same fault sink as `md-bars`, and pass it to `MdBars`
as `crosscheck`; S30 / S31 call `CrossChecker.clear(series)` when the operator clears that series' FAILED
(`arm_clear_failed`), so a feed that is still wrong faults again after `K` new mismatches; (9) no `.claude/` change is
needed by this stage.

Built as two PRs, each green: **S17a** `brokerbars.py` (schedule, finality, verification re-read, error unit, backfill
lane, gap heal, broker-outage pause, `md_backfill` work item) with its tests; **S17b** `crosscheck.py` (eligibility,
shift / swap rule, skipped ratio, D6 status) with the cross-check golden tests. S17b needs S17a and S14.

* **C:** `src/marketdata/brokerbars.py`: per-series schedule (first read at `T + p50 + settle`; `broker_bar_lag_ms`
  only starts the back-off and the metric, R11-m; re-reads every 5 s until `post_grace`, 30 s until `T + 5 min`, 60 s
  until the cap); finality on broker-side evidence (bucket T+1 in the same response, used only as evidence; or two equal
  reads ≥ settle apart; or the partial flag clear); one verification re-read at `min(final + 60 s, cap − 30 s)`, else
  the caveat `unverified_final` (R11-m); validation before queueing; quarantine. **Error unit (R11-M08):** a bucket adds
  at most 1 to K, and only when it is still invalid at its `decision_cap` (or after its full re-read schedule); decode
  errors and failed reads of a bucket are retries inside its healing and only feed a metric and a once-per-episode page.
  **Backfill lane (R11-M03):** after the cap a blind BROKER bucket moves to a low-priority lane inside the BARS budget and
  is re-fetched until it resolves or broker history no longer reaches it (then `unfillable` in the coverage map); a gap
  heal pass walks blind buckets inside retention oldest-first at a low rate (R11-m). BACKFILLING of `W + 3 × length`
  newest-first; consumer `refetch`. **Broker outage (R11-M10):** while `AdapterHealthPort` shows the adapter not
  STREAMING, or REST failing at every priority, the series' cap counters pause, one `broker_unavailable` page goes out
  and the series is GAPPED, never FAILED; bar-read failures count only while other REST priorities on the same adapter
  succeed (S06 outcome counters). Every series' fetch, decode and validation runs in its own try/except. `md_backfill`
  work item (from the S32 handler) re-fetches the requested range in the backfill lane.
  `src/marketdata/crosscheck.py` (R11-M01, R11-M02): per-field eligibility (close when the TICK bucket is
  `close_eligible`; high and low only when the TICK bar is `complete`); tolerance `k` × the bucket's median clean
  spread; a bar within tolerance of its own TICK bar is always a match; a shift or swap counts only when bucket T fails
  tolerance **and** T − 1, T + 1 or the other side passes it by a margin of at least 0.5 × tolerance on the eligible
  vector; the shift test is skipped when the bucket's range is below 3 × its median spread; a skipped-cross-check ratio
  per series (metric, and a page when more than half of the last 16 demanded buckets were skipped); D6 status `ACTIVE |
  CLOSE_ONLY | UNAVAILABLE` per series in `md:meta` and `md show`.
* **Tests (golden):** under filtered delivery a broker feed shifted by one bucket reaches `source_mismatch` within 3
  buckets; a flat Asian session with equal consecutive closes never flags; a bid / ask swap is detected; three
  undecodable responses for one bucket then a valid final bar leave the series LIVE with K = 0; one blind bucket at the
  cap is re-fetched within the lane and becomes evaluable (its `bucket_resolved` event is asserted here; the ATR
  recompute is tested in S18); a 30-minute broker REST outage gives `broker_unavailable` and no FAILED; bar reads failing
  while POLL reads succeed still count; a corrupt bar is quarantined at once and its bucket is `blind`; one series'
  malformed payload leaves every other series fetching; finality at the 17:00 New York daily break and at Friday's
  close (two agreeing reads, no bucket T+1); a forming bar in a response is never stored or counted.
* **Resolves:** R11-M01, R11-M02, R11-M03, R11-M08 (market-data side), R11-M10 (market-data side), R11-M15, R11-m (first
  read time, verification re-read, gap heal).

### S18 MD-7: indicators, `md-indicators`, demand and admission

Status: done 2026-11-16. Built: `src/marketdata/indicators/base.py` (the `Indicator` Protocol of §A6 with `KIND`,
`VERSION`, `OUTPUTS` (`Output(name, Unit)`, PRICE / RATIO / COUNT), `DECIMAL_CONTEXT`, `LOWER_BOUND`, `EXACT_SLIDING`,
`parse_params`, `clock`, `inputs` (`SeriesRef` / `IndicatorRef`), `window` (`Count(n)` or `Anchored(anchor_spec,
max_bars)`), `warm_counts`, `warm_span`, `backfill_bars`, `seed`, `step`, `outputs`, `provisional`; `quantum(unit,
price_increment)` = increment × 10^-4 / 1e-6 / 1, `ROUND_HALF_EVEN`; `IndicatorSpec` (kind, version, primary series,
normalised params, price increment hidden from `repr`) with `indicator_id = sha256(canonical)[:16]` and `parse`;
`State` (named canonical text, JSON for checkpoints); `IndicatorValue` with `Lineage(indicator_id, window_start,
input_digest (ids and revisions only), value_revision, reason)`; the `md:ind` member codec, a FAULTED marker carrying
the reason; `IndicatorUpdate`), `registry.py` (`IndicatorRegistry` keyed `(KIND, VERSION)`, deprecation without
removal, `spec()`; `plan_specs`: class known, `IndicatorRef` targets and outputs, cycles, topological recompute order,
every window and the primary backfill inside its input's retention; `REGISTRY` ships ATR v1), `atr.py` (canonical ATR
over `Count(W + 1)`, `W = 10 × length` (the extra bar lends only its close), seed = mean of the first `length` TRs,
then RMA, exact under the pinned `ATR_CONTEXT`; pure `atr_exact(bars, length, prev_close=)` over any OHLC for
FirstBarGuard; `step` / `provisional` as the RMA recurrence; length 1-200, default 14), `checkpoint.py` (every 96
buckets on the UTC grid, 30 days, `<bucket>|<state json>`), `engine.py` (`MdIndicators`, the `md-indicators` task
body (stamp 10 s): one recompute queue coalesced per indicator, run in topological order inside a 6 s cycle budget;
jobs from `on_released` (a bar older than the newest value, or a revision, recomputes every held value from that
bucket), `on_resolved` (`bucket_resolved`, R11-M03), `register()` and `md_rebuild`; reads through the read port (SM12
gate), compute in a worker thread (`asyncio.to_thread`, one thread per indicator at a time) with `compute_buckets`
(the alignment rule `bucket_end ≤ t_end` for same, lower and higher timeframes; canonical value on every update;
outputs finite, inside the class bound, the declared names; warm = `warm_counts` evaluable bars and no blind bucket
(the `md-bars` lane or a bar read back blind) in the last `warm_span` buckets); thread CPU over 250 ms, a raising
class or a bad value is a fault: re-seed and retry, the 3rd in a row FAULTED (reason in the lineage, a fault marker
per output, `on_fault(indicator_id, reason)`, one page), jobs dropped until `md_rebuild`; a job still running after
5 s is FAULTED at once (`job_hung`); an unreadable store or a refused write defers, never a fault; `on_quarantine`
drops the head job and faults its indicator; values written by the new `IND` script then `indicator_updated`, the
in-process `on_updated` listeners after the reply; unchanged values (same quantised values, quality, warm, inputs) not
rewritten; `is_warm` also needs the newest closed bucket; `latest`, `provisional`, `value_as_of` (recompute from the
`as_of` bars; `superseded_expired` when a needed revision aged out of `md:bars_rev`); `each_cycle` hook for the lease
writes), `src/marketdata/demand.py` (`DemandBook`: `define` (series id `s` + `sha256[:16]`, indicator id), leases
with TTL 60 s, `ARMED > PREWARM`, decision-grade = a BROKER 15m series or an indicator over one, counted per symbol;
PREWARM-only symbols evicted newest first (the adapter's own first) to admit ARMED; refused only at the hard ceiling
`min(decision symbols, (BARS cap − stop reads per minute) / reads per symbol)` per adapter and the §A5 memory formula
`(maxmemory × 0.5 − 15 MiB) / 60 MiB` across adapters; `demand()` registers a new indicator with `md-indicators`
(canonical value computed in the thread before warm); `broker_demand()` (md-bars `DemandSource`, `W + 3 × length`),
`is_demanded()` (md-writer shedding), `held_leases()` (bootstrap `LeaseSourcePort`), `on_evicted`, `sync()` writing
`LEASE` calls (registry in `md:registry:ind` / `md:registry:series`, releases, renewals every 20 s)). Changed:
`store/scripts.py` (new `IND` script: token, exact `md:boot`, `run_id`; per output `md:ind:{id}:{output}` replaced and
trimmed below the value floor, optional checkpoint trimmed at 30 days, optional `md:registry:ind` definition, then
`indicator_updated` with id and bar end only; `IndicatorWrite`, `indicator_call`), `store/reads.py`
(`indicator_range`, `indicator_newest`, `checkpoint_newest`, `registry_ind`), `ports.py` (`MarketDataPort` gains
`indicator`, `provisional`, `define`, `on_indicator_updated`; `IndicatorRead`, `ProvisionalRead`,
`IndicatorCallback`), `port_impl.py` (`MdPort` with optional `demand_book` and `indicators`: `indicator` in one read
session (a FAULTED newest value is `DataUnavailable(indicator_faulted)`; `as_of` recompute through
`value_as_of`), `provisional` from the hook's forming bars, `wake_indicator`, `define` / `demand` / `release` through
the book, refused without one), `bootstrap.py` (`HeldLease.registry_key`, default `md:registry:series`, so an
indicator lease is re-asserted into `md:registry:ind`). Tests: `tests/marketdata/indicators/test_atr.py` (8: golden
vectors at lengths 5, 14 and 50 against an independent float window-seeded reference within the quantum and plain
`ta.atr` at a relative 1e-3; hand-checked small vectors with and without a previous close; the value depends on the
`W + 1` window only; params; RMA step and provisional), `test_indicator_types.py` (11: quantum per output; spec ids
pin params, series and increment; value and checkpoint codecs; registry and deprecation; plan order, missing input,
cycle, retention; a multi-output (bands), a multi-input (bid and ask, with a RATIO output), a lower-timeframe (15m
clock over 1m bars, no lookahead) and an anchored (hour start) test indicator through `md-indicators` with no store or
port change), `test_engine.py` (14: a planted raising indicator FAULTED after 3 faults with one fault report and one
page while ATR on two series keeps updating; `md_rebuild` clears FAULTED and completes `applied`; CPU cap and
out-of-range faults; a hung job confined to its indicator; `on_quarantine`; an unreadable store defers; recompute
after a revision; a blind bucket resolved in the lane makes ATR warm again with no new bar (R11-M03); checkpoint
state equals canonical at every checkpoint (values and checkpoint states; `step` from each tracks canonical to the
next within C3's relative 1e-3); replay from re-fetched bars gives the identical
quantised value and input digest; `as_of` across a revision and `superseded_expired` past `md:bars_rev` retention; a
failed write defers and is retried once; over fakeredis with the real `MdWriter` / `MdPort`: `bar_closed` then
`indicator_updated` in `md:events` and in process, port reads (n, at bucket, three outputs of the bands), the registry
definition, no price in logs, a FAULTED marker read as `indicator_faulted`; the `IND` script's trims, checkpoint,
registry, emit flag, `BAD_ARGS` and `NO_BOOT`), `tests/marketdata/test_demand.py` (10: key-safe ids; ARMED evicts
PREWARM newest first and is refused only at the ceiling; TICK leases never count; unknown and non-15m refusals; BARS
budget less stop reads and the memory formula (memory binds across adapters); TTL expiry; demand sources (backfill
`W + 3 × length`, an indicator lease keeps its series live, held leases with registry keys); indicator registration
and removal; `register` computes before warm; `LEASE` writes, renewals, a store failure kept due, release; `MdPort`
demand through the book); `test_ports.py` / `test_port_impl.py` updated for the new port members. G: pytest 3687
passed / 52 skipped, mypy strict clean (240 files), `verify_execution.py --strict` 11 / 11; the lockout real-tree scan
passes with the new modules.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) the worker thread is `asyncio.to_thread` (the lockout refuses `concurrent.futures` in `src/marketdata`), so the
"single-thread executor per indicator" is enforced as one in-flight thread per indicator: a hang past 5 s FAULTs that
indicator at once (`job_hung`) and `md_rebuild` is refused `busy` until its thread returns; the 250 ms cap is thread
CPU (`time.thread_time_ns`) measured inside the job, so an over-cap job is discarded after it finishes; (3) every value
is the canonical value recomputed from stored bars (ATR has no exact sliding form), so a "re-seed" is the same
recompute and checkpoints are written and verified (`incremental = canonical` at each one) but not used to resume;
their decode is therefore never a fault path; (4) ATR's window is `Count(W + 1)`: the oldest bar only lends its close
to the first TR ("the previous bar's close when it exists"), warm needs `W` bars; (5) the stored `warm` flag is the
window rule; "its last updated bucket equals the latest closed input bucket" is checked at read time by
`MdIndicators.is_warm` (the ARM, S20 / S27, must also compare the value's bucket with the bar it decides on);
blind buckets come from a `BlindSource` (S31: `MdBars.lane`) plus bars read back `blind`; there is no calendar here,
so an absent bucket that is not in the lane does not block warm; (6) on a revision only values held in memory
(the last 1024 per indicator) and the newest are recomputed, 96 per job; older stored values keep their revision
until re-read through `as_of`; (7) the multi-input rule "fires when every input has a final bar for T, or at `T + max
settle` with quality `degraded`" is left to the class (a missing input gives no value); the engine has no settle
timer; (8) the `as_of` read recomputes only values that became final after `as_of_ms` and needs the spec in this
process (`md-indicators` keeps every spec it tracked); an `n` read with `as_of` drops values it cannot recompute;
(9) value `final_at_ms` = the newest `final_at_ms` of its window's bars (deterministic, so `as_of` agrees with the
bars); value revisions are node-local, kept in memory; (10) demand ceiling inputs: `reads_per_symbol_per_min`
(default 2) is a placeholder until PR3 measures the live schedule's real cost, `stop_reads_per_min` comes from S27
(default 0) and `bars_per_min` from the S06 BARS cap; the memory figure uses §A5's 60 MiB per symbol and 15 MiB fixed
at `maxmemory` 1 GiB × 0.5 (8 symbols); a non-15m BROKER series lease is refused `spec_not_decision_grade`; (11)
indicator values are written over the writer client by `md-indicators` (not through `md-writer`'s flush; they need no
`WAITAOF`); a write needs the writer's token and installed sentinel, so values wait for the bootstrap; (12) S31 wiring:
build one `DemandBook(bars_per_min=…, stop_reads_per_min=…, host=engine)` and one `MdIndicators(md_port,
write=writer_client, token=lambda: writer.token, boot=lambda: writer.boot, blind=md_bars.lane, pager=…,
on_fault=S30 series fault, complete=request store, each_cycle=lambda: book.sync(writer_client, writer.token))`;
register `engine.on_released` with `MdWriter.on_released`, `engine.on_resolved` with `MdBars.on_resolved`,
`port.wake_indicator` with `engine.on_updated`; pass `book.broker_demand` to `MdBars` as `demand`, `book.is_demanded`
to `MdWriter`, `book` as the bootstrap's `leases`, and `demand_book=book, indicators=engine` to `MdPort`; the
supervisor's task-circuit hook calls `engine.on_quarantine`; the `md_rebuild` handler (S32) calls
`engine.md_rebuild(request_id, indicator_id)`; (13) no `.claude/` change is needed by this stage (no new
`verify_execution.py` check: the plan lists none for S18).
Review fixes (2026-11-16): (a) `register()` of an indicator already tracked (a second consumer's lease) returns its
status and never runs a job, so a hung indicator never gets a second thread and a FAULTED one is never computed over
its marker; (b) `remove()` keeps a FAULTED tombstone and `add()` restores it, so FAULTED survives every lease lapsing
and only `md_rebuild` clears it (for an untracked id it clears the tombstone and completes `applied` /
`not_tracked`; the next demand re-seeds); (c) `MdPort.indicator` is a decision read: the spec is resolved through
`md-indicators` (`input_series`) or else `md:registry:ind` with the port's registry (`registry.input_series`, through
`IndicatorRef` inputs); unresolved is `not_registered`, and every input series passes the `bars` gate (global FAILED
and FAILED `failed`, else `not_live`) over the buckets read; (d) the 250 ms CPU cap applies per target and a job's
batch stops once its thread CPU passes half the cap (`compute_batch`), the rest re-queued, so a length-200 ATR
revision over 96 targets is split, never FAULTED; (e) worker calls run through `_in_thread`: never cancelled (a call
not yet started still runs, then clears its indicator's busy mark), at most `max_threads` (3) in flight on the shared
default executor, and at `max_hung_threads` (2) hung threads no job is dispatched (one page) and `value_as_of` is
`indicators_saturated` (a dedicated executor stays a `.claude/` lockout decision for the user); (f) the task stamps
after the per-cycle hook, before each job and after each bounded step (reads, thread, each write), and settings refuse
`job_timeout_s + io_timeout_s` ≥ the stamp budget; (g) `_newest_bucket` records the newest bar it reads and `is_warm`
is False while the newest closed bucket is unknown; (h) a `Count` window reads further back in `MAX_READ_BUCKETS` steps
past a long closure until it holds `n` evaluable bars (bounded by retention, or an empty step); (i) older FAULTED
markers are dropped from `n` reads, and a marker for a never-computed indicator sits on the newest closed bucket, so the
`md_rebuild` value replaces it; (j) the checkpoint test is renamed (checkpoint state equals canonical) and now also
steps from each checkpoint. Tests: `test_engine.py` +10 (re-register on hung and FAULTED, FAULTED across lease lapses,
hung-thread bound and a never-started call, stamping, `is_warm` after a takeover, the closure read, `compute_batch`
budget and per-target cap, the LENGTH_MAX revision recomputed without a fault, the port's decision gate, older markers in `n` reads), `test_demand.py` +1 (a second consumer and a
lease lapse keep FAULTED with no `IND` write).

* **C:** `src/marketdata/indicators/__init__.py`, `base.py` (Indicator Protocol per §A6; `OUTPUTS` each with its quantum
  unit PRICE (`price_increment × 10^-4`), RATIO (`1e-6`) or COUNT, pinned per VERSION; `window()` returns `Count(n)` or
  `Anchored(anchor_spec, max_bars)`, R11-m), `registry.py` (keyed `(KIND, VERSION)`; `indicator_id = sha256(spec)[:16]`),
  `atr.py` (canonical ATR over `W = 10 × length` evaluable bars, exact Decimal under the pinned context; pure
  `atr_exact(bars, length)` shared with FirstBarGuard), `engine.py` (`md-indicators` job runner: worker thread with the
  pinned context, 250 ms CPU cap, recompute coalesced per indicator; per-indicator circuit, FAULTED after 3 faults with
  the reason in its lineage, jobs dropped until `md_rebuild`; `on_quarantine` drops the head job; `indicator_updated` is
  emitted after the indicator write and `bar_closed` by the flush with the bar, R11-m; recompute when a blind BROKER
  bucket resolves, R11-M03), `checkpoint.py` (every 96 buckets); `src/marketdata/demand.py` (leases with TTL 60 s;
  classes ARMED > PREWARM; PREWARM eviction newest-first; hard ceiling from the §A5 formula and the per-adapter BARS
  budget, which includes the stop-read cost of armed handles, R11-M20; `register()` computes the canonical value in a
  thread before reporting `warm`). `md_rebuild` work item (from the S32 handler): re-seeds one indicator from stored
  bars, clears FAULTED and reports warm, completing its request.
* **Depends on S17** because recompute is driven by S17's `bucket_resolved` / revision events and ATR reads BROKER bars.
* **Tests:** ATR golden vectors at lengths 5, 14 and 50 against a window-seeded reference (within the quantum) and plain
  `ta.atr` (relative 1e-3); a multi-output and a multi-input test indicator added with no store or port change; a
  lower-timeframe test; an anchored test indicator; a planted raising indicator becomes FAULTED while every other series
  keeps ATR; recompute after a revision; a blind BROKER bucket resolved by the S17 backfill lane makes ATR warm again
  without waiting 3 × length bars (R11-M03); incremental equals canonical at every checkpoint; replay from re-fetched
  broker bars gives the identical quantised value; `md_rebuild` clears FAULTED; `as_of` across a revision and past
  `md:bars_rev` retention; ARMED lease evicts PREWARM and is refused only at the ceiling.
* **Resolves:** R11-M03 (indicator side), R11-M20 (budget input), R11-m (event order, quantum per output, anchored window).
* **Risk:** without a TradingView export the reference is an independent test implementation plus hand-checked small
  vectors; the user may supply exported Pine values later (§9).

### S19 ARM-1: riskmgr core

Status: done 2026-11-16, review fixes applied 2026-11-16 (after the deviations). Built: `src/riskmgr/types.py` (σ `sigma`; `Resource` (STOP_LEVEL only), `ArmEvent` (PRE,
POST, LATE, REOPEN, CORRECTED) with `BAR_END_EVENTS`, `BarEvent` (PRE / POST only), `StopBasis`, `S0Source`,
`EntryTimeSource`, `CloseSource`, `ScopeKind`; typed metrics `RMultiple` (0.01) and `Ratio` (0.001), quantised under a
fixed context; `HandleView`, `ArmContext`, `BarContext` (no units; entry, stops, S0, increment and min stop distance
hidden from `repr`; `r` computed under the fixed context); keyed input ids `SeriesInput` / `IndicatorInput`,
`window_hash` and the HMAC `inputs_digest` (key id first, order-independent)), `outcomes.py` (`Category`, `Outcome`,
`ENGINE_CATEGORY`, KIND-scoped reasons mapped only to INFO / HOLD (`reason_map_problems`), `OutcomeCode`,
`UNRESOLVED_RANGE` INFO reasons `armed_late`, `downtime`, `paused`, `failed`, else FAULT), `strategy.py` (`SlStrategy`
Protocol per §B6.1, `ParamError` and the param readers, `StrategyParams` with `params_hash` / `guards_hash`,
`StrategyState`, `Refuse`, `GuardSpec`, `IndicatorNeed` / `HistoryNeed`, `ArmingRequirements`, `LoosenPolicy`,
`ReopenPolicy`, `Proposal[STOP_LEVEL]`, `Evaluation` with `trigger_met`), `guard.py` (`Guard` Protocol, `GuardResult`
PASS / HOLD / PENDING / UNKNOWN, `UnknownPolicy`, the scope ids `guard:<KIND>:<VERSION>` and
`strategy:<KIND>:<VERSION>`), `registry.py` (`ArmRegistry`: strategies and guards keyed `(KIND, VERSION)`, only
`STOP_LEVEL`, declarations checked on add, deprecated flag (loadable, refused for new pins), guard and strategy KINDs
disjoint, `registry_tokens()` with the indicator registry's tokens, `parse_params` = the one parser of a strategy
row's params with its guard list; `REGISTRY` empty), `envelope.py` (`ENVELOPE_VERSION` 1, `EnvelopeParams`
(`min_gap_r`), `ENVELOPE_KEYS`, `EnvelopeStamp`, `envelope_changed`), `config.py` (`parse_arm_settings`:
`XTNL_ARM_MIN_GAP_R`, `XTNL_ARM_PRE_OFFSET_S`, `XTNL_ARM_EDGE`, `XTNL_ARM_STOP_READS` and the five `XTNL_ARM_FAIL_*`
thresholds with the design bounds; problems name the variable and bounds only, never raises), `ports.py` (`Pager`
`__slots__` wrapper with `page` / `log_mirror`, `Clock` / `SystemClock`, `PgHealthPort` and `engine_pg_down`,
`FailedSpoolRecord` / `FailedSpoolPort`; the market-data ports re-exported), `sm11.py` (`ArmState`, `Sm11Event`,
`EDGES`, `next_state`, `Transition`, `Refused`). Tests: `tests/riskmgr/conformance.py` (the registry-wide suite:
`check_registry` over every registered strategy and guard against its case in `STRATEGY_CASES` / `GUARD_CASES`, a
class without a case is a problem; purity and determinism forwards, backwards and under a hostile ambient Decimal
context; every REASON mapped; DECISION_METRICS typed and every returned metric declared; `parse_params` refuses
unknown keys, envelope keys, out-of-bound rows, a missing required guard and a guard listed twice, never echoing a
value; `guards(params)` is the parsed list in order; `migrate_state` round-trips and covers every older version;
`trigger_met` equals the case's stated bar-end condition, proposals name only current handles), `toys.py` (a toy
strategy and guard), `test_conformance.py` (17: the empty and engine registries, the toy passing, and one test per
planted defect), `test_registry.py` (another RESOURCE refused, declarations, duplicates, tokens, parse and refusals,
deprecation), `test_config.py` (defaults and every bound), `test_sm11.py` (the table equals an independently written
§B12 edge list; every other pair refused; PRE and stop moves never change the state; only `RELEASED` reaches
STRATEGY_COMPLETE; FAILED stores and restores `prior_state` while CLOSED / DISARMED / SUSPENDED still apply),
`test_types.py`. G: pytest 3836 passed / 52 skipped, mypy strict clean (249 files), `verify_execution.py --strict`
11 / 11; the lockout real-tree scan passes with the new modules.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) every strategy and guard method that needs the pinned params takes them as its first argument
(`propose(params, ctx, state, event)`, `evaluate(params, ctx)`, ...), since the §B6.1 sketch leaves their source
implicit; `parse_params` returns normalised `(key, text)` pairs as indicators do, and the registry (not the strategy)
splits off and parses `guards`; (3) `required_indicators` / `required_history` return templates on `(bar_spec, side)`,
always BROKER (`IndicatorNeed.resolve`, `HistoryNeed.resolve`), because an `IndicatorSpec` needs the position's
adapter, symbol and increment; (4) `next_state(state, event, *, prior_state)` returns `Transition(state, prior_state)`
because the FAILED edges read and write `prior_state`; a `SUSPEND` while FAILED keeps the position FAILED and makes
SUSPENDED the state the clear restores, a second `SCOPE_FAILED` is refused `already_failed` (S30 tracks covering
scopes); SM11 events are named (`ARMING_PASSED`, `FIRST_POST`, `FINAL_ADJUST`, ...), with `PRE_DECIDED` and
`STOP_MOVED` present only to be refused; (5) KIND-scoped reasons may map only to INFO or HOLD (ACTIONABLE is the
engine's `ADJUST`); (6) `XTNL_ARM_EDGE` defaults to `default`; the series `K` stays the D6 setting
`XTNL_MD_MISMATCH_K`; channel credentials (S29), the config DSN (S33) and the enable flag are parsed by their own
stages; `pre_offset` bounds come from the timing row (31-45 s, the S12 choice); (7) the envelope property test and
the planted hanging strategy belong to S21 / S22 (the pipeline and the job runner); (8) no `.claude/` change and no
new `verify_execution.py` check (the plan lists none for S19).
Review fixes (2026-11-16): `OutcomeCode` now represents `PRE_NOT_CONFIRMED(reason)` (INFO, §B6.3 (7a)): a KIND-scoped
reason with its owner KIND (`FIRST_BAR_VOLATILE` of `first_bar`, `BELOW_TRIGGER` of kind (a)) or the engine's
`AT_TARGET_UNCONFIRMED` with no owner (`OutcomeCode.pre_not_confirmed(owner, reason)`); `GuardResult` and `OutcomeCode`
carry an optional code-only `detail` for the sub-reason S20 needs (`FIRST_BAR_UNKNOWN(atr_degenerate | timeout |
stale)`); SM11 applies `POSITION_CLOSED` / `LOCAL_DISARM` out of FAILED whatever the stored `prior_state` (only
`FAILED_CLEARED` and `SUSPEND` read it), and `steps(state, events)` applies one decision's events in order (a first
POST ending in ADJUST is `FIRST_POST` then `FINAL_ADJUST`; documented in `sm11.py`, with the note for S26 that
SUSPENDED --`LOCAL_REARM`--> ARMED is reachable from DISCOVERED, so the re-arm handler re-runs the arming guard);
`RMultiple` / `Ratio` refuse non-canonical exponents and negative zero, and `of()` never returns −0; `read_decimal`
and `XTNL_ARM_MIN_GAP_R` accept only plain decimal text (no underscores, exponent or non-ASCII digits; strategy text
also no surrounding whitespace, env values are stripped as for the integer settings); `ArmRegistry.parse_params`
turns any non-`ParamError` exception of a class's own `parse_params` into `ParamError("<owner>: params could not be
parsed")` with no value; `window_hash` refuses `|` in the indicator id or output. The conformance suite now also
checks that no `ParamError` echoes a value in which an out-of-bound row differs from the valid row, that a guard's
`required_indicators` / `required_history` are registered `IndicatorNeed`s / `HistoryNeed`s (`check_guard(registry,
guard, case)`), and that the case's inputs compare equal to a deep copy taken before the run; its docstring now says
the suite (not the class) calls `at_target` / `commit_state` only on bar-end events. `test_conformance.py` has 22
tests (new planted defects: an echoed out-of-bound value for a strategy and a guard, mutated inputs, a guard's
unregistered indicator, malformed history need and raising `required_indicators`). G after the fixes: pytest
3857 passed / 52 skipped, mypy strict clean (249 files), `verify_execution.py --strict` 11 / 11.

* **C:** `src/riskmgr/types.py` (σ, `BarContext`, `StopBasis`, input ids with keyed hashes), `outcomes.py` (outcomes,
  categories, `UNRESOLVED_RANGE` reasons), `strategy.py` (`SlStrategy` Protocol per §B6.1; `Evaluation` with
  `trigger_met`, `Proposal[STOP_LEVEL]`, reason), `guard.py` (`Guard` Protocol; PASS / HOLD / PENDING / UNKNOWN),
  `registry.py` (strategies and guards keyed `(KIND, VERSION)`; deprecated flag; only `RESOURCE = STOP_LEVEL`;
  `registry_tokens()` for PR13), `envelope.py` (EnvelopeParams from local settings only; `envelope_version` and a params
  hash recorded on each decision, R11-m), `config.py` (pure parse of `XTNL_ARM_*` and `XTNL_ARM_FAIL_*` with bounds,
  RT-M13), `ports.py` (non-database ports: `Pager` as a `__slots__` wrapper object, `Clock`, `PgHealthPort`,
  `FailedSpoolPort`; market-data ports re-exported from `src.marketdata.ports`), `sm11.py` (design §B12 as a pure
  transition table `next_state(state, event) -> state | Refused`: DISCOVERED, ARMED, TRACKING, ADVICE_OUTSTANDING,
  STRATEGY_COMPLETE, SUSPENDED, FAILED with `prior_state`, CLOSED, DISARMED; PRE and between-bar stop moves have no edge;
  only a final bar-end check reaches STRATEGY_COMPLETE; used by S26, S27 and S30 so no other module sets `state`).
  `tests/riskmgr/conformance.py` (registry-wide suite: pure and deterministic under replay; every REASON mapped;
  DECISION_METRICS typed; `parse_params` rejects unknown keys and out-of-bound values; `migrate_state` round-trips or
  refuses; `trigger_met` means "the bar-end condition for the target holds", R11-m).
* **Tests:** the registry refuses another RESOURCE; settings bounds; the suite runs on the empty registry; SM11 table:
  every §B12 edge allowed, every other (state, event) pair refused, PRE never changes the state, FAILED restores
  `prior_state` on clear while CLOSED / DISARMED / SUSPENDED still apply.

### S20 ARM-2: FirstBarGuard and kind (a)

Status: done 2026-11-16. Built: `src/riskmgr/guards/first_bar.py` (`FirstBarGuard` `first_bar` v1: params `anchor`
(`fill_bucket` default | `next_full_bucket`), `measure` (`high_low` | `body` | `true_range`), `comparison` (`gt` | `ge`),
`atr_bar` (`current` | `previous`), `multiplier` (1.318, 0.5-5.0), `atr_length` (14, 5-50), `unknown` (`hold_bar` |
`skip_test`); `EVENTS = {POST}`; REASONS `FIRST_BAR_VOLATILE`, `FIRST_BAR_UNKNOWN`, `PRE_ANCHOR` (HOLD); requires the
BROKER bid `ATR(atr_length)` and `W + 2` bid BROKER bars; bar 0 derived from the entry time and the anchor; the entry
bucket under `next_full_bucket` is `PRE_ANCHOR` at POST and through `known_before_bar_end` at PRE ("no advice this bar
(entry bucket)"); exact Decimal verdict `measure > multiplier × ATR_exact` (`≥` for `ge`) with `ATR_exact` recomputed by
`atr_exact` over the window's evaluable bars (the same rule as `md-indicators`); PENDING until bar 0's `decision_cap`
while bar 0 is not final, the window is short, the `md-indicators` value for the ATR bucket is missing, FAULTED or not
warm (R11-M03), or the entry time is still `ESTIMATED`; `FIRST_BAR_UNKNOWN(atr_degenerate | entry_ambiguous |
entry_time_unknown | increment_unknown)`; `on_timeout` = `FIRST_BAR_UNKNOWN(timeout)`; metric `first_bar_ratio`
(0.001), caveats `borderline` and `roll_affected`; `conditional_label` / `pre_label`),
`src/riskmgr/strategies/break_even_on_risk_close.py` (kind (a) v1: params `trigger_basis` (`exit_side` | `bid`),
`trigger_comparison`, `latch`, `be_offset_r` (0-0.5), `rearm_on_loosen`, `bar_spec` (15m only);
`REQUIRED_GUARDS = {first_bar}`; `favour` / `risk` with the bar-end stop `S_at_T` or the farther of it and
`pre_basis`, the current stop and the frozen quote at PRE; `target = E + σ·be_offset_r·max(R, 0)` with the caveat
`be_offset_first_seen` when S0 is not `AT_FILL`; reasons `NOT_IN_FAVOUR` / `BELOW_TRIGGER`; caveat `borderline`; metric
`risk_r` only; `at_target` = every handle at or beyond the target; `commit_state` records `trigger_met` (state
`latched`, `target`), unchanged after a FAULT; `latch` re-proposes without counting as this bar's trigger;
`REOPEN_POLICY = REUSE_TARGET` (REOPEN reuses the recorded target); `loosen_policy` from `rearm_on_loosen`), the two
package `__init__`s, and `REGISTRY` now registers both. Changed: `types.py` (`BarContext.decision_cap_ms`,
`entry_time_ambiguous`), `guard.py` (`GuardResult.metrics` / `caveats`; Guard Protocol `known_before_bar_end` and
`on_timeout`; `final_verdict` = PENDING until `until`, then `on_timeout`, then `unknown_policy`: HOLD_BAR → HOLD with
the reason and detail, SKIP → PASS with the caveat `<kind>_skipped`), `strategy.py` (SlStrategy Protocol
`trigger_side`), `registry.py`. Tests: `tests/riskmgr/kind_a.py` (synthetic 5-digit and 3-digit pairs, fixtures and
the conformance case data), `test_first_bar.py` (33: golden vectors one quantum either side of the threshold for both
pairs and both comparisons against an independent `Fraction` reference, the exact boundary for `gt` / `ge` with ATR
previous and current, measures, borderline, a fill at 17:06 New York with and without break prints, applicability,
`PRE_ANCHOR` at POST and PRE, every PENDING cause, the cold cap, blind / no-trade bars, `atr_degenerate`, entry-time
cases, `final_verdict` under both policies, outcome codes), `test_break_even.py` (25: long and short boundaries for
`gt` / `ge`, reasons, borderline, buffer and `R ≤ 0`, the first-seen caveat, proposals, `at_target` with an off-grid
entry, `pre_basis`, `S_at_T` against the current stop, PRE inputs, engine-fault inputs, `risk_r`, `commit_state`,
latch, REOPEN, `migrate_state`), `test_first_bar_md.py` (R11-M03 end to end over `FakeBars` and the real
`MdIndicators`: one blind bucket in the lane, a fill 1 h later is PENDING and then `FIRST_BAR_UNKNOWN(timeout)` at its
cap; after the lane resolves the bucket the same fill gets a real verdict, and a widened bar 0 turns volatile); the
conformance suite runs both engine classes (`test_engine_registry_conforms`) and now also checks `trigger_side`,
`on_timeout` and `known_before_bar_end`; `test_registry.py` updated for the non-empty engine registry. G: pytest 3916
passed / 52 skipped, mypy strict clean (253 files), `verify_execution.py --strict` 11 / 11; the lockout real-tree scan
passes with the new modules.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) Protocol additions that S21 consumes: `SlStrategy.trigger_side(params, side)` (the engine fills `BarContext.bar` and
the PRE quote from that side), `Guard.known_before_bar_end` (how `PRE_ANCHOR` is known at PRE) and `Guard.on_timeout`
(the UNKNOWN a PENDING becomes at its cap), with `final_verdict` in `guard.py` as the one PENDING / `unknown_policy`
rule; `GuardResult` carries metrics and caveats; the toys implement the new members; (3) `BarContext` gains
`decision_cap_ms` (bar 0's cap for PENDING; without it the cold-skew cap is used) and `entry_time_ambiguous` (a
permanently ambiguous entry time); (4) FirstBarGuard derives bar 0 from the entry time and its anchor (the engine's
`bar_index` is only the fallback with no entry time), and `applies` is also true on the entry bucket under
`next_full_bucket` so its POST records `PRE_ANCHOR`; "POST" in `EVENTS` stands for every bar-end decision (POST, LATE,
REOPEN, CORRECTED), which S21 must honour; (5) the guard reads bar 0 and the ATR window from its own bid history (never
`BarContext.bar`, which is the trigger side), gates on the `md-indicators` value for exactly the ATR bucket (warm, not
FAULTED) and recomputes `ATR_exact` from the bars; an `ESTIMATED` entry time is PENDING until the cap (an upgrade is
still possible), so one that never upgrades ends `FIRST_BAR_UNKNOWN(timeout)`; extra details `entry_time_unknown` and
`increment_unknown`; (6) `roll_affected` is set on any bar 0 overlapping the 17:00 New York roll, not only after PR3
shows break prints; (7) the strategy applies the `pre_basis` farther-from-entry rule itself (idempotent with S21's
`S_at_T`), and a missing trigger-side bar, bar-end stop or PRE quote raises (`STRATEGY_ERROR` through S22's runner);
the proposal holds the unrounded target (rounding toward profit stays envelope step (9), S21) and `at_target` compares
against it, which also counts a handle exactly at an off-grid `E`; (8) metrics: the strategy returns only `risk_r`
(current risk in R) and the guard `first_bar_ratio`; `borderline` is a caveat; favour is never a metric (§B8);
(9) notice texts are module constants for S28 (`CONDITIONAL_LABEL`, `pre_label`, `PRE_ANCHOR_TEXT`,
`BE_OFFSET_FIRST_SEEN_TEXT`); (10) no `.claude/` change and no new `verify_execution.py` check (the plan lists none).
Review fixes (2026-11-16): (11) `HistoryNeed` now means the newest `bars` **evaluable** BROKER bars ending at the
decided bar (the `md-indicators` `Count` rule), with `HistoryNeed.held_by` for S21's reader, which must read back past
`no_trade` / `blind` buckets (S21 C); before, `W + 2` raw buckets with 2-3 quiet ones gave a PENDING (then
`FIRST_BAR_UNKNOWN(timeout)`) or an ATR without the lent close; test: two `no_trade` buckets in the span, both
`atr_bar` modes, real verdict whose quantised ATR equals the stored `md-indicators` value; (12) kind (a): one
`_effective_target` (REOPEN reuses the recorded target) used by both `propose` and `at_target`, and `Evaluation` gains
an optional `target` (an order-state level, only with a proposal) so `commit_state` records the target even when no
handle moves; REOPEN test with a re-pinned R; (13) tests: the R11-M03 end-to-end test now asserts the exact status and
ATR parity against a `Fraction` reference, the 17:06 New York test builds a reopen-only bar 0 and one whose low is a
break print (measure, verdict and ratio checked), and a 16:59 fill pins `overlaps_roll`'s half-open interval.
G after the fixes: pytest 3921 passed / 52 skipped, mypy strict clean (253 files), `verify_execution.py --strict` 11 / 11.

* **Gate:** Q1 answered (the user's task names kind (a); a different answer swaps only the strategy class).
* **C:** `src/riskmgr/guards/__init__.py`, `guards/first_bar.py` (params per §B6.7; `EVENTS = {POST}`; `applies` = bar
  index 0; `PRE_ANCHOR` known at PRE as well, so under `next_full_bucket` the fill bucket's PRE says "no advice this bar
  (entry bucket)", R11-m; exact Decimal verdict against `atr_exact`; `atr_degenerate`; PENDING until bar 0's
  `decision_cap`, then `unknown_policy`; `conditional_label`); `src/riskmgr/strategies/__init__.py`,
  `strategies/break_even_on_risk_close.py` (KIND, IMPL_VERSION 1; `REQUIRED_GUARDS = {first_bar}`; favour and risk;
  `target = E + σ·be_offset_r·max(R, 0)`, and the caveat `be_offset_first_seen` ("buffer based on first-seen stop") when `be_offset_r > 0` and S0 is
  not `AT_FILL` (design §B6.3 (7)), R11-m; `at_target`; `commit_state`; `REOPEN_POLICY = REUSE_TARGET`; `loosen_policy`).
* **Tests:** golden vectors at `multiplier × ATR ± 1 quantum` for a 5-digit and a 3-digit synthetic pair; a fill at
  17:06 New York with and without break prints; long and short boundaries for `gt` / `ge`; `borderline`; `R ≤ 0`;
  conformance suite passes for both classes; R11-M03 end to end on the in-memory market-data fakes: one BROKER bucket
  blind at its cap, resolved by the backfill lane, and a fill 1 h later gets a real first-bar verdict, not
  `FIRST_BAR_UNKNOWN(timeout)`.

### S21 ARM-3: envelope pipeline, `S_at_T`, release rule, PRE rules

Status: **built ahead of the S07 gate, not accepted** (2026-11-16; pure modules only: nothing is wired, both flags stay
off). The S07 gate says "S21 waits for" the user's §9.2 Oversee confirmation; that gate is the user's decision (design
review gate) and was not waived by the user, so S21 is not done: the user must explicitly confirm the §9.2 Oversee
change and C1 (option (a), the built default) before anything relies on it, and S22 onward does not build on S21 until
then. Review fixes 2026-11-16: the history reader steps back in full `MAX_READ_BUCKETS` reads (the `md-indicators`
step), so a closure of 4.5 days or more no longer ends the read early (tests: 4.5, 5 and 9 day closures);
`stop_at_bar_end` collects every handle first and gives `StopUnavailable` only when an unresolved handle could change
`S` or the cover (an unresolved POSITION_FIELD; an unresolved non-LIMIT child when no POSITION_FIELD resolved);
a watermark-labelled version (stored with `broker_time_ms` NULL) newer by seq than the newest counting candidate is a
non-counting newest candidate (`PRIOR_VERSION`, or unresolved), so a watermark-only handle is never taken as absent;
an undeclared guard or strategy reason is `PipelineFault` of that guard / strategy version and is never stored (also
not as `PRE_NOT_CONFIRMED`); a final bar-end FAULT (`NO_STOP`) carries its SM11 events (ARMED → TRACKING, advice
withdrawn); `ADVICE_WITHDRAWN` follows §B12 ("POST without ADJUST and latch off") through a new `SlStrategy.latch(params)
-> bool` accessor (like `loosen_policy`; kind (a): the `latch` param; checked by the conformance suite; a §B6.1
Protocol addition for the user to confirm with S21), and with the latch on only when the strategy proposed nothing;
at PRE `at_target` is checked before the guards (no row from a known guard HOLD); the envelope property test asserts
the exact set of moved handles. Built: `src/riskmgr/stopbasis.py`
(`StopVersion` (handle, level, live, units, order kind, PR8 origin / `broker_time_source` / broker time, `APPLIED` /
`DROPPED_STALE`, seq), `handle_at_bar_end` and `stop_at_bar_end` = `S_at_T`: only APPLIED versions of origin `stream`,
`stop_read`, `snapshot`, `dlq` with a real BROKER time ≤ T count; a newest candidate that does not count (`history`,
watermark) falls back to the newest earlier counting version (`PRIOR_VERSION`), else `StopUnavailable(stop_at_bar_end)`;
a handle with no candidate did not exist at T; `stop_loss` first, else the covering STOP / loss-side children with the
most distant level (LIMIT never), partial cover or no handle = unavailable; basis `STREAM` / `READ` / `PRIOR_VERSION`
(the weakest of the handles used); caveat `stop_changed_after_bar_end` for a `stop_read` version after T;
`handle_roles` (`POSITION_FIELD`, `CHILD_ORDER:<rank>` by distance from entry, never an order id); `PreBasis` (levels by
role, `pre_basis_bar`, `s_pre`); `farther_from_entry`, `farther_basis`), `src/riskmgr/pipeline.py` (`run_pipeline`:
pre-guards (1)-(5) incl. the released-position rules (4) and PENDING "awaiting broker bar" until `decision_cap` then
`UNRESOLVED(broker_bar_not_final)`; pinned guards in order with `final_verdict`, PENDING clamped to the cap as
`DATA_UNAVAILABLE(<kind>_pending)`; at PRE a POST-only guard is never run (`known_before_bar_end` or its
`conditional_label`); the stop-basis rule (the farther of `S_at_T` and the held `S_pre`, `AT_PRE`; the stored basis
cleared when `S_at_T` is at or beyond it, or when a bar end confirms the trigger); `propose`; the release check (7a) on
the Evaluation and `at_target` (`COMPLETE`, `AT_TARGET_UNCONFIRMED` shown on the first bar of a run,
`PRE_NOT_CONFIRMED(reason)` with `pre_basis` stored); `apply_envelope` = post-guards (8)-(13) (`BROKER_AHEAD` per handle,
`round_toward_profit`, `MARKET_CLOSED` through `CalendarPort.state()` with None = closed, `exit_quote_not_live`,
`PRICE_THROUGH_LEVEL` on the exit side, `ADJUST`); PRE rules (`pre_window`: valid until `min(fire + 10 s, local T + p50
− 30 s)`; no item when expired, released, a fill in the last `pre_offset`, closed or at target; a quote received after
fire is refused; never releases, no SM11 event, no basis stored, `pre_levels` for the PRE row); `commit_state` on final
decisions; the SM11 events of each final decision; `PipelineFault(scope_id)` for a raising strategy or guard),
`src/riskmgr/history.py` (`read_history` / `read_histories`: the newest `bars` evaluable BROKER bars ending at the
decided bar, reading back past `no_trade` / `blind` / absent buckets until `HistoryNeed.held_by`, nothing older or the
retention; trimmed to the newest `bars` evaluable buckets). Changed: `src/riskmgr/strategy.py` (`at_target` docstring:
the pipeline reads it at PRE only to drop the item; review fix: `latch(params) -> bool`),
`src/riskmgr/strategies/break_even_on_risk_close.py` (`latch`), `tests/riskmgr/conformance.py` (`latch` is a bool),
`tests/riskmgr/toys.py` (`latch`). Tests: `tests/riskmgr/test_stopbasis.py` (20: the golden amended
then cancelled while off, history-only unavailable, every counting origin, watermark (NULL broker time, as `event_log`
stores it) / DROPPED_STALE / after-T, a watermark superseded by a newer counting version, a watermark-only handle, an
unresolved LIMIT child or a history-only child beside a POSITION_FIELD ignored, a version exactly at T, the after-T
caveat, cover and kind rules, roles across a cancel / replace, `PreBasis`),
`tests/riskmgr/test_pipeline.py` (41: D3 long and short (COMPLETE; PRE_NOT_CONFIRMED then managed under `pre_basis`
until a confirming bar end; loosened beyond `pre_basis` → current stop, cleared; a new child id keeps `pre_basis`), bar 0
PRE then a volatile first bar → `PRE_NOT_CONFIRMED(FIRST_BAR_VOLATILE)`, a spy guard proving PRE never runs FirstBarGuard
and never changes the state, `PRE_ANCHOR` at PRE, PRE at offset 30 s, the default window, the fill in the last
`pre_offset`, unknown / closed calendar → `MARKET_CLOSED`, COMPLETE with the market closed, rounding and the gap,
pre-guard faults, released positions, awaiting broker bar then UNRESOLVED at the cap, guard PENDING then timeout,
`S_at_T` unavailable, `commit_state` under a post-guard HOLD, LATE / REOPEN / CORRECTED, the release rule on a toy kind,
a raising guard attributed to `guard:first_bar:1`, an undeclared guard reason (also with a PRE proposal) and an
undeclared strategy reason as `PipelineFault` of their version, a final NO_STOP leaving ARMED and withdrawing advice,
post-guard HOLDs withdrawing advice only with the latch off, PRE at target before a known guard HOLD, and the
envelope property test over 600 generated cases with the exact set of moved handles),
`tests/riskmgr/test_history.py` (9: quiet buckets, a three-day closure, 4.5 / 5 / 9 day closures, nothing older / retention, no bar after the
decided one, `DataUnavailable` passed through, largest need per series, FirstBarGuard's window equals the `md-indicators`
Count rule). G (after the review fixes): pytest 3991 passed / 52 skipped, mypy strict clean (256 files), `verify_execution.py --strict` 11 / 11;
the lockout real-tree scan passes with the new modules.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) the S07 gate says "S21 waits for" the user's Oversee confirmation; S21 was built anyway as pure, unwired code that
reads only `src.oversee.risk`'s handle kind. This was not the user's exception to grant itself: S21 is recorded as
built ahead of the gate, not accepted, until the user confirms the §9.2 Oversee change and C1; (3) the history reader lives in its own
module `src/riskmgr/history.py` (the plan names no file); (4) "current S" for clearing `pre_basis` is the bar-end stop
`S_at_T` (Q8), so a loosening made after T applies at the next bar end and never makes this bar's test easier;
(5) the pipeline calls `at_target` at PRE only to drop the item (§B6.3), and the Protocol docstring says so;
(6) "this bar's PRE proposed a move" = the PRE ended in `ADJUST` (its `pre_levels` are stored on the PRE row and passed
back as `this_bar_pre`); a second `PRE_NOT_CONFIRMED` keeps whichever basis has the `S_pre` farther from entry;
(7) on a guard HOLD `propose` is not run (first match wins), so the strategy state is unchanged; (8) the envelope
property allows `BROKER_AHEAD` (INFO, design table step 8) besides HOLD and FAULT; (9) the result also carries the
decision's SM11 events (a final FAULT included; `ADVICE_WITHDRAWN` per §B12 through `SlStrategy.latch(params)`, added
to the §B6.1 Protocol like `loosen_policy`, and with the latch on also when the strategy proposed nothing),
`PENDING` / `NO_ITEM` result kinds and `shown` (False = `suppressed`); only a `DATA_UNAVAILABLE` is not
final (a bar-end `NO_STOP` decides the bar); (10) a raising strategy or guard raises `PipelineFault(scope_id)` without
the exception text, for S22's runner to record as `STRATEGY_ERROR`; (11) a PENDING guard is
`DATA_UNAVAILABLE(<kind>_pending)` (`first_bar_pending`, §B6.5); a PRE with no frozen quote or one received after fire
is a `DATA_UNAVAILABLE` row (`pre_quote_unavailable`, `pre_quote_after_fire`); (12) pre-guard (2) measures the age from
the handles' `read_ms` (send time) and applies only with `XTNL_ARM_STOP_READS`; `stop_changed_after_bar_end` only for
`stop_read` versions; `snapshot` / `dlq` / `stop_read` give basis `READ`; (13) `ENVELOPE_VERSION` stays 1 (the first
envelope; no decision was recorded under another); (14) no `.claude/` change and no new `verify_execution.py` check
(the plan lists none).

* **C:** `src/riskmgr/pipeline.py` (pre-guards (1)-(5), pinned guards, `propose`, release check (7a) defined on the
  Evaluation, post-guards (8)-(13), rounding toward profit; PRE: the quote is frozen at fire time and valid-until =
  `min(fire + 10 s, local T + p50 − 30 s)`, R11-m; no PRE item when `at_target`; bar 0's conditional label; market
  closed through `CalendarPort.state()` with None = closed, IN-10). **`pre_basis` (R11-M04, option (a), §9.2):** after a
  `PRE_NOT_CONFIRMED`, later bar-end trigger tests use the stop as it was before the PRE-prompted move until a bar end
  confirms the trigger against it; then normal Q8 ("current") resumes. Per position the trigger takes, of the
  `pre_basis` stop and the current stop, the one farther from entry, and `pre_basis` is cleared once the current stop is
  at or beyond it away from entry; `pre_basis` is keyed by handle role (`POSITION_FIELD`, or `CHILD_ORDER` ranked by
  distance from entry), never by order id; the same farther-level rule applies to the same-bar `AT_PRE` basis (design
  §B6.3, §B6.4). `src/riskmgr/stopbasis.py` (`S_at_T` over a
  per-handle version list: only APPLIED versions whose origin is `stream`, `stop_read`, `snapshot` or `dlq` with
  `broker_time_source = BROKER` and broker time ≤ T; `history` and `DROPPED_STALE` never count; with only a history
  candidate, `PRIOR_VERSION` from the newest earlier non-history version, else `DATA_UNAVAILABLE(stop_at_bar_end)`,
  R11-M06; `AT_PRE`; the chosen `stop_basis` recorded). **History reader (from the S20 review):** `BarContext.history`
  holds, per `HistoryNeed`, the newest `bars` **evaluable** BROKER bars ending at the decided bar (the `md-indicators`
  `Count` rule): the reader reads further back past `no_trade` / `blind` buckets until `HistoryNeed.held_by` is true,
  the read returns nothing older or the retention is reached; a raw-bucket count would shorten FirstBarGuard's window
  (a PENDING, or an ATR without the lent close that differs from `md-indicators`).
* **Tests:** envelope property test (it only turns an ADJUST into a HOLD or a FAULT); D3 tests long and short (PRE
  ADJUST, stop moved, close confirms → COMPLETE; close below → PRE_NOT_CONFIRMED; next bar's close just beyond entry →
  still managed under `pre_basis` until a bar end confirms; a stop loosened beyond `pre_basis` after PRE_NOT_CONFIRMED →
  tested at the current, looser stop and `pre_basis` cleared; a StoneX cancel / replace giving the stop child a new
  order id after PRE_NOT_CONFIRMED → `pre_basis` still applies); bar 0 PRE with a volatile first bar →
  `PRE_NOT_CONFIRMED(FIRST_BAR_VOLATILE)`; golden `S_at_T`: a stop amended and then cancelled while the engine was off
  (the history version is excluded); PRE at offset 30 s computes nothing after `T − 30 s`; an unknown calendar →
  `MARKET_CLOSED`; a fill in the last `pre_offset` of a bar gets no PRE for that bar (D1); PRE never runs FirstBarGuard
  and never changes the SM11 state (D2, D3); at POST a trigger met with one handle still short of break-even is not a
  release (D3).
* **Resolves:** R11-M04, R11-M06 (rule), IN-10, R11-m (PRE quote time, PRE_ANCHOR, release rule contract).

### S22 ARM-4: replay, frontier, corrections, per-strategy job runner

Status: done 2026-11-16, review findings fixed 2026-11-16 (pure modules only: nothing is wired, both flags stay off;
see "Review fixes" below). **Built without building on the unaccepted S21:** no S22 module imports `src/riskmgr/pipeline.py`, `stopbasis.py` or `history.py`; the job runner takes
the job as a callable and the replay planner returns steps, so S27 joins them to the pipeline once the user confirms
the §9.2 Oversee change and C1 (one test checks that S21's `PipelineFault` satisfies the runner's `ScopedFault`).
Built: `src/riskmgr/replay.py` (`Frontier` `(last_decided_bar, last_decided_hash)`; `newest_expired_bar_end` = arming
time − horizon; `arming_frontier`: the later of the fill bucket's start (or the stored cursor on re-arming) and the
newest **open** bar end at or before the newest expired bar end, taken from the BarClock's open bar ends
(`bar_ends`), the skipped open bars one `armed_late` span (a closed-period bucket is never counted, never its last bar
and never the cursor; no open bar in the range, no span); `BarSlot` (bar ends of open buckets after the cursor,
`ready`, the expiry `reason`, `post_sent`, `next_open_ms` on a pre-close bar); `next_step` → `Span` (one terminal
`UNRESOLVED_RANGE` per contiguous run with one reason, `bar_end_ms` = its last bar, INFO for `armed_late` / `downtime` /
`paused` / `failed` else FAULT (`pages`), `first_bar_stale` when it holds bar 0) | `Decide` (POST, or LATE once the POST
notice went; REOPEN for a re-check) | `Wait` | `Hold` (FAILED scope or paused: cursor stays put) | `DropRecheck`;
`advance` (span → cursor at its last bar, hash NULL; final POST / LATE → its bar and hash; REOPEN never moves it);
Friday / daily-break rule: a pre-close bar spanned at its cap with `broker_bar_not_final` carries a `ReopenCheck`, and
`reopen_after_closed_post` builds one for a final POST that came out `MARKET_CLOSED` with `trigger_met` (S27 calls
it); re-read from the open + settle and decided as REOPEN while due, on the §B6.3 REOPEN schedule: it ends at the
first post-open bar's POST decision (the cursor reaching that bar), at the latest that bar's `decision_cap`
(`deadline_ms`), then `DropRecheck`; it never blocks that POST (a retryable REOPEN is marked not due for the rest of
the pass) and waits as `reopen_pending` only when nothing else is decidable; `Span.first_bar_detail` gives `stale`; `classify_event` (WAKE after the cursor,
NOOP same hash or after a span, INCOMPARABLE under another key id, CORRECTION_CHECK on another hash or an older id);
`in_bar_order`; `SeriesCursor` (the in-memory `write_seq` cursor per series and generation; restart form
`EventCursor(oldest last_decided_bar, 0)` after `CURSOR_EXPIRED` or a generation change; read by bucket when the
restart form expires)), `src/riskmgr/corrections.py` (`compare_input` through `compare_hashes`, so hashes under
different key ids are never compared; `check_decision` → NOT_ELIGIBLE (PRE, spans, superseded, REOPEN) | FINAL (window
= the bar's `decision_cap` ended) | NO_CHANGE | RETRY (missing, not final, quarantined, unreadable, absent at decision,
`key_id_differs`) | CORRECTION; `OpenChecks` → one audit-only `UNVERIFIABLE(reason)` per compare still open at its
window end; `revision_kind` CORRECTED (outcome, `trigger_met` or first-bar verdict changed) versus SUPPRESSED (a
recomputed FAULT never counts); `plan_revision` (`revision + 1`, `supersedes_decision_id`, `advice`, `actionable`
only for a re-proposed ADJUST on the latest bar, `info_only` for an older bar, `reapply_commit_state`, release rules:
RELEASE / WITHDRAW of a release made on that same latest bar, with the SM11 events `RELEASED` / `RELEASE_WITHDRAWN`)),
`src/riskmgr/ordering.py` (`next_decision_seq`, `advice` (POST, LATE, REOPEN and a revision of the latest bar; never
PRE, spans, suppressed rows or INFO corrections of an older bar), `OrderedRow`, `newest_advice`, `in_decision_order`),
`src/riskmgr/jobs.py` (`JobRunner`: one `ThreadPoolExecutor(max_workers=1)` per pinned strategy scope entered through
`loop.run_in_executor` and `asyncio.wait_for` under the 250 ms cap; the job runs under the pinned `DECIMAL_CONTEXT` set
inside the worker thread; `STRATEGY_ERROR(job_timeout | job_raised | executor_hung)`, no exception text kept; a
`ScopedFault` naming one of the job's guard scopes counts against that guard, anything else against the strategy;
`VersionCircuit`: 3 consecutive faults per scope → one `VersionFault` (STRATEGY_VERSION) per episode until `reset`;
`take_signals`, `close` (returns whether a job thread is still hung, for the hard exit)), `src/riskmgr/frontier.py`
(`FrontierWatch`: counting time only, per (position, overdue bar); positions on a FAILED or paused scope never count;
cause `ARM_WRITE` while the ARM pool or its transactions fail, never filed here (S30's `fail_pool_down_s` row files
the pool outage), else `DATA` at once after the 120 s grace, one `FrontierFault` (key `stuck_frontier:<cause>`) per
cause and episode). Tests (after the review fixes): `tests/riskmgr/test_replay.py` (31: arming 3 weeks after the fill → one
`armed_late` span with `first_bar_stale` and only the current bar decided; a 9-hour shutdown → one INFO line per
position, no page; a bar never final → a FAULT span at its cap and the next bar decided in the same pass; spans split
by reason; in-order POST / LATE; a POST-time `DATA_UNAVAILABLE` keeps the cursor; FAILED / paused hold; the cap follows
the skew; arming on a Saturday and at the Sunday open (4 real bars, the span ending on Friday's last bar, no span on
re-arming across the weekend); Friday's last bar final only at `T + 20 min` → UNRESOLVED at the cap, nothing while
closed, REOPEN at the Sunday open + settle before the first post-open POST; the bar final between the first
post-open bar's first read and its POST → REOPEN; a re-check never blocks the first post-open POST (also a retryable
REOPEN) and ends with it, or at that bar's cap; a trigger on the 16:45-17:00 New York bar → POST in the break
(MARKET_CLOSED, cursor advances), nothing while closed, REOPEN at 17:05 + settle before the 17:15 POST; event classification; the generation restart, the expired restart form,
unavailable reads), `test_corrections.py` (19: same hash no-op, different hash CORRECTION, a different key id never a
change, every unreadable form, absent-at-decision, eligibility and the window, the UNVERIFIABLE audit, CORRECTED versus
suppressed, latest versus older bar, release and withdrawal, the first reason kept across RETRYs),
`test_ordering.py` (3), `test_jobs.py` (12: a planted
hanging strategy leaves another version advising and the `arm` loop stamping, later jobs of the hung version answered
at once as `executor_hung`, one STRATEGY_VERSION signal; a planted raising guard version faults only the positions
pinning it; foreign scope ids and plain raises go to the strategy with no text kept; the pinned Decimal context under
a hostile ambient one; a job calling `sys.exit` or raising `KeyboardInterrupt` → `job_raised`, the lane still works; a
raised `TimeoutError` → `job_raised` with its guard attribution; the caller's cancellation propagates; a subprocess
with a planted hung job exits through the hard exit `close()` asks for), `test_frontier.py` (7: failing ARM writes
never filed here, DATA once the pool recovers; one fault per episode; a DATA stuck frontier at the grace; held
positions and non-counting time never count; counting resumes from its total after a pause, a held pass resets it;
the clock restarts when the cursor moves). G: pytest 4050 passed / 52 skipped, mypy strict clean (261 files), `verify_execution.py
--strict` 11 / 11; the lockout real-tree scan passes with the new modules.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) S22 imports no S21 module (see above): `jobs.py` runs any callable and attributes faults through the
`ScopedFault` Protocol (an exception with `scope_id`), and the replay planner is not joined to `run_pipeline` here
(S27); (3) replay is a step planner (`next_step` / `advance`) over `BarSlot`s the BarClock lists (every bar end of an
open bucket after the cursor; a closed-period bucket is not a bar), and the slot carries the reason an expiry records
(the BarClock passes `broker_bar_not_final` while awaiting the broker bar, else the default `downtime`); (4) the REOPEN
re-check window follows the design's REOPEN schedule (§B5.2, §B6.3): from the first open instant + settle until the
first post-open bar's POST decision, at the latest its `decision_cap`; it never delays that POST; a bar spanned with
`broker_bar_not_final` gets one, and so does a final POST that came out `MARKET_CLOSED` with `trigger_met`
(`reopen_after_closed_post`); a pre-close bar spanned for downtime gets none (it is replay's, as §B5.2 says); (5) an input that cannot be verified holds
the whole compare at RETRY even when another input changed, so a correction is decided on a complete input set; a
hash under another key id is RETRY (and at the window end `UNVERIFIABLE(key_id_differs)`), never a change; a `CORRECTED`
revision is itself correctable (it is the non-superseded row of its bar), REOPEN rows are not (their window has ended);
(6) jobs of one version run one at a time (an `asyncio.Lock` per version) so the cap measures the job, not its queue;
a timeout counts against the strategy scope (a hang cannot be located inside a guard); a completed job resets the
counters of every scope it ran; (7) the stuck-frontier cause is ARM-side exactly while the ARM pool or its
transactions fail (the caller's `pool_failing`), and then `FrontierWatch` files nothing (S30's `fail_pool_down_s`
row owns the outage), else DATA; time counts only between two counting passes (the caller's
`counting`: ARM running and no §B14 pause such as engine PostgreSQL down); (8) `OpenChecks` is in memory per term (the
next trigger reopens a compare after a restart); (9) no `.claude/` change and no new `verify_execution.py` check (the
plan lists none).
Review fixes (2026-11-16): (a) `arming_frontier` takes the BarClock's open bar ends (`bar_ends`, required), so an
arming span across a closed period counts real bars and ends on an open bar; (b) `reopen_after_closed_post` for a
`MARKET_CLOSED` POST and a rewritten 16:45-17:00 test; (c) the REOPEN window per (4), `ReopenCheck` carries
`first_open_bar_ms`, `open_ms` = open + settle; (d) `FrontierWatch` never files `ARM_WRITE` (its hard-coded
`pool_down_ms` is gone) and latches one fault per (cause, episode); (e) `JobRunner.run` turns any `BaseException`
from a job into `job_raised` (the caller's own `CancelledError` still propagates) and tells the cap expiry
(`wait_for` cancelled the future) from a raised `TimeoutError`, which keeps its guard attribution; (f) a hung job
thread still blocks a normal interpreter exit (executor workers are joined at exit; a daemon worker needs
`threading`, which the lockout refuses): `close()` returns True then and S27 / S31 must end the process through
`hard_exit`; proposed rule `docs/design/proposed-claude-changes/S22-hung-job-shutdown.md` (not applied, no test
depends on it); (g) `OpenChecks` keeps the first RETRY reason; (h) the unused `first_read_ms` import is gone and
`FIRST_BAR_STALE` is used by `Span.first_bar_detail`. G after the fixes: pytest 4063 passed / 52 skipped, mypy
strict clean (261 files), `verify_execution.py --strict` 11 / 11.

* **C:** `src/riskmgr/replay.py` (cursor and starting frontier `max(fill bucket − 1, arming time − horizon)`; one
  `UNRESOLVED_RANGE` row per span; in-order decisions; horizon = `decision_cap`; after a generation change the in-memory
  `write_seq` cursor restarts from the durable `(last_decided_bar, hash)`; Friday rule: a pre-close bar UNRESOLVED at its
  cap is re-read at the open and, if final then, decided as a REOPEN item, R11-m), `src/riskmgr/corrections.py` (hash
  compare only within one key id; `UNVERIFIABLE` audit; CORRECTED versus `suppressed`; release and withdrawal rules),
  `src/riskmgr/ordering.py` (`decision_seq`, `advice`), `src/riskmgr/jobs.py` (R11-M17: each position's guards +
  `propose` + `at_target` run as one job in a single-thread executor per pinned `(KIND, IMPL_VERSION)` (its own
  `concurrent.futures.ThreadPoolExecutor(max_workers=1)`, entered through `loop.run_in_executor`, never
  `asyncio.to_thread`, whose default executor is shared) with the pinned Decimal context and a 250 ms cap; a timeout or raise is a `STRATEGY_ERROR` result; 3 consecutive faults across that
  version's positions raise a STRATEGY_VERSION fault signal for S30; a fault raised inside a guard is attributed to that
  guard's pinned `(KIND, VERSION)` (its own counter and scope id `guard:<KIND>:<VERSION>`), so a bad guard version does
  not fail an unrelated strategy version; a hung thread blocks only its own version's executor), `src/riskmgr/frontier.py` (stuck-frontier detection after a 120 s grace, classified by cause: ARM pool or
  PostgreSQL write failures versus data, R11-M11).
* **Tests:** arming 3 weeks after the fill writes one `armed_late` row and decides only the current bar; a 9-hour
  shutdown gives one line per position and no page; a bar never final gives a span at its cap and the next bar is
  decided on time; same hash is a no-op, a different hash is CORRECTED, a different key id is never a change; a planted
  hanging strategy leaves other versions advising and the `arm` loop stamping; a planted raising guard version faults
  only the positions pinning it; a stuck frontier with failing ARM writes is classified ARM-side; Friday's last bar final
  only at `T + 20 min` (after its cap) is UNRESOLVED at the cap, re-read at the Sunday open and decided as a REOPEN item;
  a trigger on the 16:45-17:00 New York bar.
* **Resolves:** R11-M11 (detection), R11-M17 (runner), R11-m (Friday cap).

### S23 DB-1: migration 0017, lifecycle, roles, grants, request kinds

Status: done 2026-11-16. Built: `src/db/migrations/0017_arm.sql` (`arm_positions`, `arm_decisions` and
`arm_decision_handles` partitioned on `bar_end_ms` with a DEFAULT partition and months −1..2 by an explicit loop,
`arm_correction_audit`, `arm_notices` (unique key and the two part CHECKs of §B8), `arm_status`, `arm_failures`
(`scope_kind`, `probe_kind`, `ping_id`, partial unique index on open episodes), `arm_controls` (singleton row),
`arm_accepted`, `arm_config_audit`, `arm_config_cache`; the `operator_requests_kind_check` drop and re-add with the 11
kinds; `event_log_broker_time_idx` `ON ONLY`; role `xtnl_arm` NOLOGIN granted to the migration runner (the owner the
engine pool connects as) and to `xtnl_engine` when that role exists, with exactly the §B3 grants after a `REVOKE ALL`;
three dashboard views `arm_positions_view`, `arm_decisions_view`, `arm_failures_view` with SELECT for
`xtnl_dashboard` only); `src/db/arm_schema.py` (the grant list, CHECK value sets, catalog reads and
`privilege_problems()` for the pg test and the S31 self-check; read statements only); `src/db/operator_requests.py`
(`RequestKind`, `ARM_REQUEST_KINDS`, `REPAIR_REQUEST_KINDS`, `REQUEST_KINDS`, docstring table); `src/db/lifecycle.py`
(`PARTITIONED` and `AUDIT_TABLES` += both tables with shift 0, `DEFAULT_WATCHED`, step 1a `SQL_DEFAULT_ROWS` (bounded
count), `LifecycleReport.default_not_empty` / `default_rows`, page key `default` "lifecycle: DEFAULT partition not
empty"); `src/db/migrate.py` `POST_STEP` += `PartitionIndex(17, "event_log", ...)`. Tests:
`tests/db/test_arm_migration.py` (idempotent DDL, keys, the −1..2 loop, every CHECK set equal to `arm_schema` and to the
riskmgr enums, kind CHECK = `REQUEST_KINDS`, GRANT statements parsed = §B3, no UPDATE of `arm_failures.cleared_at_ms` /
`clear_request_id` and none on `notification_outbox`, `privilege_problems`, FENCE in every write `SQL_*` of the changed
modules), `tests/db/test_arm_schema_no_market_data.py` (schema scan over 0015-0017 tables, ADD COLUMNs and view select
lists, with planted `bid` / `close` / `atr` / `favour_r` ... columns failing), `tests/db/test_housekeeping_stores.py`
(3 × 7 ensured partitions, DEFAULT count and page), `tests/db/test_dashboard_role_migration.py` (0017 view grants, no
`xtnl_api`), `tests/db/test_migrate_post_step.py` (the 0017 item), and a `pg`-marked
`test_0017_arm_schema_role_grants_and_post_step` in `tests/db/test_pg_integration.py` (run once against a disposable
PostgreSQL 17 container: catalog privileges equal the §B3 list; as `xtnl_arm` setting `cleared_at_ms`, updating the
outbox, reading `operator_requests` and deleting are refused (42501); a decision lands in the month partition, not the
DEFAULT; 0017 re-executed by hand a second time without error). `tests/ops/test_ack_release_cashflow_archive.py` now
checks `CORE_REQUEST_KINDS` equal to the three core kinds. Deviations: (1) the `arm_positions` key is `(adapter_id,
position_id, resource, owner)` and the decision key adds `adapter_id` in front, because `positions_state` is keyed per
adapter and two brokers may reuse a position id; for the same reason S24 / S28 / S30 must write every other position
reference as `adapter_id:position_id`: the elements of `arm_failures.position_ids` (the FAILED ping) and the §B8
per-item `arm_notices.item_key` (`adapter_id:position_id:resource:owner:revision`), so two adapters' items never
collide on `arm_notices_key` (column comments in 0017 say so); (2) `probe_kind` has seven values: the five cause probes, `adapter_series` (an ADAPTER scope
runs the probe of each of its FAILED series, §B14) and `arm_global`; (3) `failure_id` and `ping_id` are text (the S19
`FailedSpoolRecord` makes them in memory before PostgreSQL has the row, so a spool replay is idempotent), not serials;
(4) `arm_positions` also has `pending_bar_end_ms` and `reopen_bar_end_ms` (the pending event's bar and the §B6.3 REOPEN
re-check), `pinned_units` (the units-increase test of §B7), `state_reason` and `config_doc_hash`; (5) the suppressed
sentinel is `arm_decisions.notice_id = 0` (`SUPPRESSED_NOTICE_ID`; real notice ids start at 1); (6) `arm_notices.event`
is checked only for its form (`^[A-Z][A-Z_]{1,31}$`), so S28 can add notice events without a migration; (7)
`arm_config_cache` is one row (document, row hashes, chain head); (8) the Supabase file `deploy/supabase/arm_schema.sql`
(§6 row "Supabase provisioning") is left to S33, which builds the client and self-check. No file in `src/adapters`,
`src/pubsub`, `src/dto`, `src/integrity` or `src/oversee` changed, so the `/live-trading-server` skill was not needed;
no `.claude/` change is needed. Pre-existing, unrelated: the S10 `pg` test
`test_sixty_unhandled_requests_never_hide_a_reset_task_on_postgres` fails on a real server with `AttributeError:
'AsyncpgConnection' object has no attribute 'transaction'` (it calls the raw asyncpg API on the wrapper); it is
skipped in the G gate (no DSN).
Review fixes 2026-11-16: `src/db/arm_schema.py` adds `REASON_PATTERN`, `REASON_PROBE_KIND` (`invalid_bar` /
`source_mismatch` / `refetch_failed` → `series_reread`, `unresolved` → `fresh_bar`, `indicator_faulted` →
`indicator_rebuild`, `stuck_frontier` → `span_write`, `strategy_version` → `strategy_conformance`), `SCOPE_PROBE_KIND`
(ADAPTER → `adapter_series`, any other ARM reason → `arm_global`) and `probe_kind_for(scope_kind, reason)` (S30 files
episodes through it; a test holds the mappings equal to the seven-value CHECK and the riskmgr / marketdata reason
constants to the reason CHECK; `FrontierFault.key` is documented as an in-memory key, never a reason, and the
`frontier.py` docstrings now say reason `stuck_frontier`, probe kind `span_write`); the catalog privilege reads
include partitions (a grant on a partition is reported); 0017 now revokes `ALL TABLES` and `ALL SEQUENCES IN SCHEMA
public` from `xtnl_arm` before granting; `arm_positions.config_doc_hash` has the hex CHECK and
`arm_positions_failed_prior_check` is `(state = 'FAILED') = (prior_state IS NOT NULL)` (the clear sets `prior_state`
back to NULL, as `sm11.Transition` already does); the lifecycle task reads `to_regclass` once per run and skips the
ARM parents (`OPTIONAL_PARENTS`, reported in `LifecycleReport.absent`) while 0017 is not applied, so a missing 0017
never fills the `errors` page key; the schema scan also reads `ADD` without `COLUMN`, `CREATE TABLE` without
`IF NOT EXISTS` and materialized views (jsonb content stays S38's).

* **C:** `src/db/migrations/0017_arm.sql`: `arm_positions` (key `(position_id, resource, owner)` with CHECKs `resource =
  'STOP_LEVEL'`, `owner = 'position'`; pins; entry and S0 sources; `edge_code`; `attribution_source` CHECK IN
  (`SINGLE_EDGE_DEFAULT`, `OPERATOR_TAG`, `INTENT`), R11-M18; `opened_by_client_order_id` with a comment that on StoneX it
  is a broker id with no edge information; `strategy_state`; `state`; `prior_state` (restored after FAILED);
  `row_version`; `last_decided_bar`, `last_decided_hash` (with key id); `pending_event`; `decision_seq`; `pre_basis`
  (handle levels, order state) and `pre_basis_bar`, R11-M04); `arm_decisions` and `arm_decision_handles` (partitioned on
  `bar_end_ms`, DEFAULT partition, months created by an explicit `-1..2` loop, ST-1; unique key per §C1;
  `envelope_version`, `envelope_params_hash`, R11-m; `notice_id`; `stop_basis`; `close_source`; input ids with keyed
  hashes `(series_id, bar_end_ms, key_id‖hash)` / `(indicator_id, output, bar_end_ms, window_start_bucket, key_id‖hash)`
  and the HMAC `inputs_digest`; R values from order state only and the first-bar ratio per design §B8; no OHLC, quote,
  spread, indicator value, favour R or favour / risk column); `arm_correction_audit`; `arm_notices` (unique `(route, bar_spec, event, bar_end_ms, part, item_key)`,
  CHECKs per §B8); `arm_status` (per node, unfenced); `arm_failures` (`scope_kind` CHECK IN (`SERIES`, `ADAPTER`,
  `STRATEGY_VERSION`, `ARM`), R11-M10 / R11-M17; `probe_kind`, R11-M12; `ping_id`; counters; partial unique index on open
  episodes); `arm_controls`; `arm_accepted`; `arm_config_audit`; `arm_config_cache` (row hashes and chain head, R11-m);
  `operator_requests_kind_check` dropped and re-added with the full list (`arm_disarm`, `arm_repin`, `arm_rearm`,
  `arm_clear_failed`, `md_backfill`, `md_rebuild`, `md_writer_reset`, `md_rehash`); `event_log` index `ON ONLY
  (entity_type, entity_key, broker_time_ms)` registered for the S08 post step; role `xtnl_arm` (NOLOGIN; the complete
  grant list of design §B3: SELECT, INSERT and UPDATE on the ARM state tables (`arm_positions`, `arm_decisions`,
  `arm_decision_handles`, `arm_correction_audit`, `arm_notices`, `arm_status`, `arm_config_cache`, `arm_config_audit`;
  the CAS and `INSERT … RETURNING` need SELECT); SELECT and INSERT on `arm_failures` and UPDATE only on its columns other
  than `cleared_at_ms` / `clear_request_id`; INSERT only on `notification_outbox`; SELECT only on `arm_accepted`,
  `arm_controls`, `engine_epoch`, `event_log`, `orders_state` and `positions_state`; USAGE on the sequences behind its
  inserts; granted to the engine role so the ARM pool can `SET ROLE xtnl_arm`, R11-m. The `arm-relay` queries and
  UPDATEs, `operator_requests` reads and completion, the `arm-tracker` snapshot, `xtnl_ensure_month_partition` and the
  `arm_clear_failed` clear statement run on the engine (owner) pool); ARM dashboard views with `GRANT SELECT … TO xtnl_dashboard`, no `xtnl_api` grant (R11-m).
* **M:** `src/db/operator_requests.py` (`RequestKind`, `REQUEST_KINDS`, docstring table); `src/db/lifecycle.py`
  (`PARTITIONED` += both tables with shift 0; `AUDIT_TABLES` += both; "DEFAULT partition not empty" page key and a
  `LifecycleReport` count); `tests/db/test_housekeeping_stores.py` (3 × 7 ensured partitions);
  `tests/db/test_dashboard_role_migration.py` (new grants).
* **Tests:** idempotent DDL; the partition loop covers −1..2; the `xtnl_arm` privileges (table, column and sequence
  grants read from the catalog in the `pg` test, the GRANT statements parsed otherwise) equal the §B3 list, and the
  role has no UPDATE of `arm_failures.cleared_at_ms` and no UPDATE on `notification_outbox`; the kind CHECK equals `REQUEST_KINDS`; every write `SQL_*`
  in the new modules contains FENCE (pattern of `tests/db/test_repository.py:425-428`); `pg`-marked integration test of
  the migration and post step; **schema scan** (`tests/db/test_arm_schema_no_market_data.py`): every column added by
  0015-0017 is on an explicit allow-list (ids, times, codes, counts, order-state levels, keyed hashes, the §B8 R values),
  and a column named like `bid`, `ask`, `open`, `high`, `low`, `close`, `mid`, `spread`, `atr`, `favour` or `quote`
  fails the test.
* **Resolves:** ST-1, RT-M9, R11-M04 / M10 / M12 / M17 / M18 (schema parts), R11-m (grants, `xtnl_arm` role, seen-row
  chain, envelope version).

### S24 DB-2: ARM stores

Status: done 2026-11-16. Built: `src/db/arm_ports.py` (frozen row dataclasses and the `ArmStateTxn` /
`ArmStateStore` / `ArmVersionReader` / `ArmFailuresStore` / `ArmStatusWriter` / `ArmConfigStore` Protocols; imports
only the standard library and `src.dto`; levels and parameters kept out of `repr`), `src/db/arm_store.py` (positions
insert `ON CONFLICT DO NOTHING` and the `row_version` CAS `must=False` that never touches a CLOSED / DISARMED row;
`record_decision` = decision `INSERT ... ON CONFLICT DO NOTHING RETURNING`, its handle rows and the position CAS
inside one `FencedTx.savepoint()`, returning INSERTED / ALREADY_DECIDED / CAS_MISS, a miss rolling back only that
savepoint; notices `ON CONFLICT DO NOTHING RETURNING notice_id`; `attach_notice` / `suppress` (sentinel 0) only where
`notice_id` IS NULL; correction audit; "decisions of this route with `notice_id` IS NULL"; live / single position
reads), `src/db/arm_versions.py` (entry time: first stream OPEN POSITION version, else the entry order's FILLED /
PARTIALLY_FILLED version with a real broker time; `S_at_T` reads through the 0017 index), `src/db/arm_failures_store.py`
(`record`: the `arm_failures` row, the critical `OutboxRow(kind="info", severity="critical")` through
`insert_outbox(tx, ...)` and the kind `arm` row on `arm_email` / `arm_push` in one fenced transaction, idempotent on
replay and on a second open episode of a scope; ping id required in the title, body bounded; `reping`, `note_probe`,
`open_episodes`; `PgArmFailedClear` for the engine pool under `pg_advisory_xact_lock(ARM_FAILED_LOCK)` with an `after`
hook in the same transaction), `src/db/arm_status_store.py` (unfenced, forward-only upsert; integer counters only),
`src/db/arm_config_store.py` (fenced cache upsert and audit insert, `must=False`; read-only `arm_accepted` /
`arm_controls`), `src/db/safe_errors.py` (`safe_db_error`: class name and SQLSTATE only). Tests:
`tests/db/arm_emulator.py` (a handler per SQL constant, transactions and savepoints), `tests/db/arm_rows.py`,
`tests/db/test_arm_store.py`, `test_arm_versions.py` (golden: stop amended then cancelled while off, the history level
excluded, PRIOR_VERSION; DROPPED_STALE never read; watermark read; caveat; stop-entry placed hours before its fill),
`test_arm_failures_store.py` (all three rows or none, parametrised over each failing statement),
`test_arm_status_config_stores.py`, `test_safe_errors.py`, `test_arm_stores_static.py` (FENCE and `$1` in every write
`SQL_*`, `arm_status` the named unfenced exception, `arm_versions` read-only, the §B3 scan over every string in
`src/**/*.py` outside `src/ops` with planted violations, the `arm_ports` import rule), and a `pg`-marked
`test_s24_arm_stores_as_xtnl_arm_on_postgres` in `tests/db/test_pg_integration.py` (run once against a disposable
PostgreSQL 17 container: every store statement works as `xtnl_arm`; the clear is refused with 42501 there and applies
on the owner pool). Deviations: (1) the `S_at_T` read is three statements: `SQL_STOP_COUNTING` (newest counting
version per handle at or before T, `history` and `DROPPED_STALE` excluded: the only rows that carry a level) plus two
level-less marker reads (`SQL_STOP_NONCOUNTING`: newest non-counting candidate and newest version with no broker
time; `SQL_STOP_READ_AFTER`: newest `stop_read` after T), so the S21 rule can still label `PRIOR_VERSION` and the
`stop_changed_after_bar_end` caveat while a history level can never reach `S_at_T`; (2) the entry order's fill version
may have any origin as long as its broker time is real (a stream FILLED version's broker time is the fill too); (3)
jsonb columns travel as JSON text (`*_json`), and a read returns PostgreSQL's jsonb rendering; (4) the clear is its
own class (`PgArmFailedClear`, engine pool) and applies only for an `arm_clear_failed` request of the current epoch
whose target is `SCOPE_KIND:scope_id`, one request clearing at most one episode; (5) the kind `arm` ping row is
inserted by `SQL_ARM_PING_INSERT` here (S29's ARM outbox store does not exist yet; S29 may move it); (6)
`arm_notices.outbox_id` is not set (the ARM role cannot read outbox ids; left to S29); (7) the route of
`unnoticed_decisions` is its (adapter_id, account_id) pairs, joined through `arm_positions`; (8) a decision that
already exists (`ON CONFLICT`) skips the position CAS; (9) the §B3 static scan covers Python sources only (0017 itself
seeds the `arm_controls` singleton row). Review fixes (2026-11-16): (a) 0017 now grants `xtnl_arm` only SELECT on
`arm_failures` plus column-level INSERT and UPDATE on every column but `cleared_at_ms` / `clear_request_id`
(`ARM_GRANTS`, `ARM_COLUMN_GRANTS`, design §B3 wording), so an episode inserted already cleared is refused with 42501
(new `_denied` assert in the pg test); the §B3 static scan now also reports any `arm_failures` INSERT / UPDATE / MERGE
that names either owner-only column outside an `IS NULL` read, joins `+` operands, resolves module-level string
constants inside f-strings and `+` chains, and accepts double-quoted identifiers (planted forms added); (b)
`entry_time` and `stop_versions` run all their statements in one READ ONLY REPEATABLE READ transaction
(`EngineDb.snapshot()`), with emulator tests that a version committed between two reads is seen by none; (c) the
golden fixture follows the StoneX history mapping (placement WORKING at Created, CANCELLED at the last change), in the
pg test too; (d) `unnoticed_decisions` returns an `UnnoticedPage` (`decisions`, `truncated`: it fetches `limit + 1`),
and the S29 notice transaction reads again until `truncated` is False; (e) `arm_status` gains `md_sentinel` (CHECK on
the `md:boot` pattern) and `md_last_write_ms` for this node's §A7 `NodeMdRecord`, with unfenced, forward-only
`save_md_record` / `load_md_record` (`ArmNodeRecordStore`, row `NodeMdRecordRow`) that the 5 s status upsert never
overwrites; S31's `NodeRecordPort` maps to them. 0017 was edited in place (applied so far only to disposable test
databases); the pg tests passed again against a disposable PostgreSQL 17 container. No file in `src/adapters`,
`src/pubsub`, `src/dto`, `src/integrity`, `src/oversee`, `src/riskmgr`, `src/marketdata` or `src/armconfig` changed,
so the `/live-trading-server` skill was not needed; no `.claude/` change is needed.

* **C:** `src/db/arm_ports.py` (Protocols and frozen row dataclasses the riskmgr imports; it imports only the standard
  library and `src.dto`, never `src.riskmgr` or `asyncpg`, so `src/db` does not depend on the ARM and the lockout
  allow-list entry "the `src.db` store Protocols" names exactly this module; riskmgr maps its own types to these rows),
  `src/db/arm_store.py` (positions insert and
  `row_version` CAS `must=False`; decisions `INSERT … ON CONFLICT DO NOTHING RETURNING`; handles; notices; correction
  audit; "decisions of this route with `notice_id` IS NULL"; every decision inside `FencedTx.savepoint()`),
  `src/db/arm_versions.py` (snapshot reads: entry time from the first stream POSITION version with OPEN or the entry
  order's FILLED history version; `S_at_T` candidates per handle from `event_log` with `action = 'APPLIED'`,
  `broker_time_ms ≤ T`, origin and source, through the 0017 index, R11-M06), `src/db/arm_failures_store.py` (one fenced
  transaction inserts the `arm_failures` row, the critical `OutboxRow(kind="info", severity="critical")` through
  `insert_outbox(tx, ...)` and the kind `arm` ping row, R11-M05; clear under an advisory lock with the `operator_audit`
  link, the `src/ops/halt.py` pattern, always on the engine (owner) pool, since `xtnl_arm` cannot set `cleared_at_ms`; open-episode read), `src/db/arm_status_store.py` (unfenced upsert like
  `src/db/uptime_store.py`), `src/db/arm_config_store.py` (cache, audit; read-only access to `arm_accepted` /
  `arm_controls`), `src/db/safe_errors.py` (`safe_db_error`: class name and SQLSTATE only, R11-m).
* **Tests:** `FakeConn` and `pg_emulator` handlers per SQL constant; FENCE in every write; a CAS miss rolls back only its
  savepoint; the `S_at_T` query excludes `history` and `DROPPED_STALE`; the failure transaction writes all three rows or
  none; static check: no SQL outside `src/ops` writes `arm_controls` / `arm_accepted` or sets `cleared_at_ms` (except the
  clear handler's statement).
* **Resolves:** R11-M05 (store), R11-M06 (query), RT-M5, ST-4, R11-m (safe database errors).

### S25 CFG-1: config document, validation, resolver, seen-row chain (pure)

Status: done 2026-11-16. Built: `src/armconfig/document.py` (`parse_document`: the four top-level keys, an envelope /
`pre_offset` or sink key at the top refused by name and any other unknown key without echoing it, a list of objects
per table, an integer `id` unique per table, JSON values only (a float is refused; the client parses with Decimal), a
depth limit of 8, an 8 KiB row cap and 1000 rows per table; `canonical_json` with sorted keys, ASCII-escaped text and
numbers in plain form without trailing zeros, so a re-encoding never looks like tampering; row hash = sha256 of
`table` + canonical row, every column including the append-only guard columns; `doc_hash` = sha256 over
`(table, id, row hash)` in document order plus `schema_version`; `schema_ahead` above `SUPPORTED_SCHEMA_VERSION` 1),
`src/armconfig/validate.py` (`validate_document(doc, KnownIds, registry)` → `VALID` with a `ValidDocument`, `INVALID`
with every `ConfigProblem`, or `SCHEMA_AHEAD`, never validated: exact columns per table, envelope / sink keys anywhere
in a row including nested params, guard columns' form, the code regex, registered and not deprecated `(kind,
impl_version)`, guard settings only inside `guards` (a guard KIND or `<kind>.<key>` among the strategy's own params is
refused), `REGISTRY.parse_params(new_pin=True)`, a bar spec with no timing row (`no_timing_row`), `(code, version)`
unique, `edge_code` NULL, symbols / adapters / accounts (id or local alias, stored as the account id) / routes from
the injected sets, the named strategy row present, `enabled` a bool, duplicate `effective_from_ms` per key refused
on both rows, §B8 route policy; problems name rows as `table:id:hash` with a fixed code (`page_text`) and keep an
escaped `detail` for `arm config show`; `escape_text`), `src/armconfig/resolver.py` (`assignments_in_force`,
`resolve(doc, PositionScope, resource, at_ms)` with precedence edge > symbol > account > adapter, set beats NULL, and
`route_policy_at` with the §B8 defaults), `src/armconfig/chain.py` (`SeenRows` over `(ref, row hash)` in canonical
order with a sha256 fold from `CHAIN_SEED`, matching `ConfigCacheRow.row_hashes` / `chain_head` / `chain_rows`;
`check_rows` → CLEAN, PENDING (rows not seen before) or TAMPERED (`CHANGED`, `VANISHED`, `CHAIN_BROKEN`); `extend`
never takes a changed seen row). Tests: `tests/armconfig/` (`docs.py` fixtures with synthetic ids, `test_document.py`,
`test_validate.py`, `test_resolver.py`, `test_chain.py`: added, changed and deleted rows, empty read, schema ahead,
future-dated rows, duplicate `effective_from`, tamper detection including a broken stored chain, a strategy whose
`bar_spec` has no timing row) and two tests in `tests/riskmgr/test_registry.py`. Review fixes (2026-11-16):
`parse_document` never raises on untrusted numbers (the range is checked on the raw value before any arithmetic, an
int at or above `10**65` and an id or `schema_version` like `1e999999999` are refused before conversion) and numbers
are normalised under one fixed context that refuses rounding, so hashes never depend on the caller's decimal context
and two different values never share a hash; `schema_version` is read first, so a newer schema may add top-level
tables (ignored and not hashed; envelope and sink keys are still refused by name); `validate_document(..., seen=,
at_ms=)` applies the new-pin checks (deprecated strategy or guard, timing rows) only to pending rows and checks the
symbols, adapters, accounts and routes of a seen row only while it is live (an enabled assignment in force, a route
policy in force, or a future-dated row), so a deprecation or a removed id never makes every later document invalid
(design §B6.1); a pending enabled assignment naming a strategy a new pin would refuse is refused on that row;
`check_rows(..., schema_ahead=True)` skips only `CHANGED`, so `VANISHED` and `CHAIN_BROKEN` still page under a schema
bump; stored chain rows of the wrong type are `CHAIN_BROKEN`, never an exception; one `latest_per_key` is shared by
validation and the resolver. G green: pytest 4371 passed, 54 skipped; mypy strict clean (273 files);
`verify_execution.py --strict` 11 / 11. Deviations: (1) the
`/live-trading-server` skill could not be invoked by the build agent (user-invocable only); (2) `src/riskmgr/registry.py`
changed (an existing S19 module): `parse_params` with `new_pin=True` refuses a strategy whose `triggers` name a bar
spec with no timing row (`TimingRowError`, a `ParamError`), so arming refuses it too, and the module exports
`ParamError`, `TimingRowError`, `StrategyParams` and `ENVELOPE_KEYS`, because the lockout lets `src/armconfig` import
only `src.riskmgr.registry` of the ARM (no allow-list was widened); **open: needs the user's sign-off** (§9.1), since
S25 was planned to touch no existing code; the check runs only with `new_pin=True` and has no other `src` caller yet;
(3) the raw row contract is fixed here for S33's
client: times as integer ms (`effective_from_ms`, `set_at_ms`), plus `id`, `session_user`, `current_user`, and
`route_policy` rows carry `route`, `mode`, `pre_channels`, `post_channels`, `effective_from_ms` (no tombstone; no
row in force means the default policy); (4) the default `post_channels` is `arm_email` only (the design names a
default for `pre_channels` only); (5) a row becomes "seen" when its document is accepted (S33 calls
`SeenRows.extend` with the accepted read), so a pending row that disappears before accept is not tampered; the chain
order is canonical (table, id) so it can be rebuilt from the `row_hashes` jsonb object; (6) a schema-ahead read is
chain-checked with `schema_ahead=True`: `CHANGED` is skipped (its rows may carry new columns), `VANISHED` and
`CHAIN_BROKEN` are not; (7) S33 must pass the accepted `seen` rows and the current time to `validate_document`
(without them every row is checked as a new pin, the stricter form); a seen row with an id removed since is checked
only while live, so a seen future-dated row naming a removed id keeps the document invalid until a newer row for its
key (or route) is in force. No `.claude/` change is needed.

* **C:** `src/armconfig/document.py`, `validate.py` (§B9 row validation; guard settings only in `guards: [...]`;
  envelope keys, `pre_offset`, sink keys and a non-NULL `edge_code` rejected; code regex; known symbols, adapters and
  accounts through injected sets; text escaping), `resolver.py` (rows in force at t, tombstones, precedence edge >
  symbol > account > adapter, at most one row per (scope, RESOURCE)), `chain.py` (sha256 chain over seen rows; pending
  versus tampered).
* **M:** `src/riskmgr/registry.py` (S19 module: `TimingRowError`, `_check_timing_rows` on `parse_params(new_pin=True)`,
  and the re-exports `ParamError`, `TimingRowError`, `StrategyParams`, `ENVELOPE_KEYS`; deviation 2, awaiting the
  user's sign-off).
* **Tests:** added, changed and deleted rows; empty read; schema ahead; future-dated rows; duplicate `effective_from`;
  tamper detection; a strategy whose `bar_spec` has no timing row is refused.

### S26 ARM-5: `arm-tracker`

Status: done 2026-11-16. Built: `src/riskmgr/tracker.py` (`ArmTracker`, the `arm-tracker` task: a 2 s pass over
`TrackerSnapshotPort` images (accounts with OPEN positions plus those with live ARM rows; only a **fresh** image arms
or ends anything); arming guard (entry known, PR1 `effective_stop_detail`, an accepted document and its assignment
through `src.armconfig.resolver.resolve` at arming time); `UNASSIGNED` / `UNSUPPORTED` kept in memory (no row: nothing
to pin) with a one-shot `POSITION_NOT_MANAGED` notice keyed by position and reason, `UNSUPPORTED` paged once, listed
by `unmanaged()`; UNSUPPORTED reasons `no_broker_bars` (a needed side missing from `BarGridPort.bar_sides`),
`asset_class`, `pin_refused` / `no_timing_row` (the strategy row re-parsed with `new_pin=True`: the first arming caller
of the S25 registry check), `increment_unknown`, `s0_not_at_fill` / `entry_time_not_exact` (`arming_requirements`),
`demand_ceiling` (an ARMED lease refused; the leases this arming added are released) and, with
`XTNL_ARM_STOP_READS=1`, `stop_read_budget` (armed handles of the adapter plus this position's over
`TrackerSettings.armed_handle_ceiling`, counted for every live row before any arming of the pass, R11-M20); "assigned
position not armed after 5 minutes" pages once; the pin: `edge_code = XTNL_ARM_EDGE`, `attribution_source =
SINGLE_EDGE_DEFAULT`, an accepted document with an enabled assignment naming another edge refused for arming with one
page per document (R11-M18); entry time and S0 sources; strategy params, guard list, indicator and history needs as
templates (no instrument increment reaches PostgreSQL), `init_state`, the cursor of
`src.riskmgr.replay.arming_frontier` with its one `armed_late` `UNRESOLVED_RANGE` row in the same fenced transaction
as the position insert; a position on a FAILED scope (`FailedScopeView`) arms into FAILED (prior ARMED); changes:
closed / gone / replaced (CLOSED; replaced with a notice), fill progress (E, units, S0 re-pinned; entry time and cursor
kept), any other units increase (SUSPENDED: notice, page, re-page every 6 h), `ROLL_ADJUST` (noted once, E kept),
entry correction (E re-pinned, noted), a pin this node cannot load (SUSPENDED `pinned_version_missing`); demand leases
owned by the task: ARMED for each live, non-suspended row (BROKER series and indicators of its pin, TICK 1m and 15m bid
and ask), PREWARM for the pre-warm set, renewed every 15 s, re-asserted at once on an md generation change and rebuilt
from the stored pins after a takeover even while the image is stale, released when no longer wanted; requests
`arm_disarm`, `arm_repin` (target `(code, version)` or the assignment in force; refused `version_not_registered`,
`strategy_unknown`, `bar_spec_changed`, `state_unreadable`, `migrate_failed`; moot `already_pinned`,
`migrate_refused`) and `arm_rearm` (SUSPENDED only; the arming guard re-run; E, units and S0 re-pinned; cursor as at
arming with its span written by `record_decision` under the position CAS) through `submit()` and
`RequestAnswerPort`, up to 3 CAS attempts each; every state change through `sm11.next_state`) and
`src/riskmgr/arming.py` (pure: `second_edges`, `entry_choice`, `s0_pick`, `classify_change`, `near_ny_roll`,
`prewarm_symbols`, the reason codes). Tests: `tests/riskmgr/tracker_fakes.py`, `tests/riskmgr/test_arming.py`,
`tests/riskmgr/test_tracker.py` (stop-entry placed hours before its fill, live and through the catch-up; the late-arming
cursor and its one span; partial fills then a suspension and its 6 h re-page; closed and replaced; a stale image;
`UNSUPPORTED(stop_read_budget)` at the handle ceiling and not without PR9; demand ceiling and missing broker bars;
leases renewed every 15 s, re-asserted after `NO_BOOT` and after a takeover; the second enabled edge refused; FAILED
scope; the 5-minute page; disarm then moot; repin with state migrated and a CAS miss retried, `already_pinned`, and
`version_not_registered`; rearm of a SUSPENDED position and refused while the guard fails; S0 `AT_FILL`; no level or
units in pages and notices; request and answer forms; the run loop surviving a failing pass). G green: pytest 4402
passed, 54 skipped; mypy strict clean (275 files); `verify_execution.py --strict` 11 / 11. Deviations: (1) the
`/live-trading-server` skill could not be invoked by the build agent (user-invocable only), as in S25; (2) the pure
rules live in a second new module, `src/riskmgr/arming.py`, beside `tracker.py`; (3) the ports the tracker needs
(`TrackerSnapshotPort`, `BarGridPort`, `AcceptedConfigPort`, `FailedScopeView`, `TrackerNoticePort`,
`RequestAnswerPort`) are Protocols in `tracker.py`, implemented by S31 / S32 (S30 for `FailedScopeView`, S28 for the
notices, S33 for the accepted document); the lost-fence error is recognised through an injected `fatal` predicate
(the scoped package cannot import `src.db.fence`), so S31 must pass `isinstance(e, EpochMismatch)`; (4) the arming
span row is written by the tracker (event `POST`, `advice` false, `range_from_bucket` = the first bar's bucket start,
caveat `first_bar_unknown_stale` when it holds bar 0); (5) `arm_positions.indicator_specs` / `required_history` store
the need templates `(kind, version, bar_spec, side, params)` / `(bar_spec, side, bars)`, never a resolved
`IndicatorSpec` (its canonical form holds the instrument increment, design §B3); (6) S0 `AT_FILL` requires each
handle's newest counting version at arming to be the same stream version read at fill + 10 s ("no later
amendment"); (7) `ROLL_ADJUST` is judged on the position's newest broker update time and noted durably (review
fix 5); (8) "replaced" is a vanished position while another unarmed OPEN position of the same symbol and side is in
the image; (9) the armed-handle ceiling default is 16 per adapter (`TrackerSettings`; the design gives no number);
(10) leases are not held for SUSPENDED positions (demanded again by the rearm before its CAS). Review fixes
(2026-11-16): (1) fill progress is a units increase the entry order's fills explain, whatever its status
(`arming.entry_fills_cover`: a working order up to its size, a terminal one up to `ordered - remaining`), so the final
fill (FILLED with the units in one image) and a last partial fill with a CANCELLED remainder re-pin instead of
suspending; only an increase beyond the order's fills suspends; (2) an OPEN position whose row is CLOSED or DISARMED
(`arm_disarm`, or `position_gone` that shows up again) is never re-planned, demands no ARMED lease and never gets the
5-minute page (one `ArmStateStore.position` read per position and term, remembered while the position stays OPEN on a
fresh image); a failed insert releases the leases that arming added; (3) an `ESTIMATED` entry time is upgraded to
`STREAM` / `BROKER` while the pinned bar 0's `decision_cap` has not passed (§B7), and with no decision yet and the fill
bucket moved the cursor is set again as at arming (a span, if any, under the position CAS); (4) a units decrease lowers
`pinned_units` (`state_reason` `units_decrease`, nothing else changes) and the entry is compared whatever the units did,
so a later scale-in still suspends and a decrease with an entry change re-pins E; (5) `ROLL_ADJUST` is written durably:
`state_reason = roll_adjust_<16 hex>` (`arming.roll_note`, a SHA-256 fingerprint of the position ref and the broker's
adjusted entry: opaque, no level, fits the 0017 CHECK), so after a takeover the adjusted entry is neither re-pinned as a
correction nor noticed again; a decrease keeps the note; another change that re-pins E replaces it. S27 must not
overwrite a `roll_adjust_*` reason on a row whose E it does not re-pin; (6) `arm_repin` to another strategy kind is
refused `kind_changed` (`migrate_state` has no "from kind"); (7) `arm_rearm` re-checks `arming_requirements`
(`s0_not_at_fill`, `entry_time_not_exact`) and demands its ARMED leases before the CAS (`demand_ceiling`;
`specs_unresolved` when the pin's specs cannot be resolved); `arm_repin` demands the new pin's leases before its CAS
too; (8) an `ESTIMATED` entry time is the first sight only, never the position's broker update time (StoneX REST
`LastChangedDateTimeUTC` may be the placement, PR3); (9) PR9 handle counts include stale images (after a takeover);
(10) `_notified` is pruned by position and the unused `_paged` set is gone. Not changed: released (`STRATEGY_COMPLETE`)
positions keep their ARMED leases, because the pipeline still decides them at bar ends (§B6.3 (4): `COMPLETE` recorded,
`LOOSENED_AFTER_COMPLETE`, `REARM_ON_LOOSEN`, and SM11 `RELEASE_WITHDRAWN` on a corrected release bar all need the
pinned series). Known limitation: on Pepperstone a partial fill suspends (cTrader positions carry no entry order id and
the adapter does not map the order's `positionId`); matching it needs an adapter change under `/live-trading-server`
(test `test_pepperstone_partial_fill_suspends_as_a_known_limitation`). New tests in `test_tracker.py` (final fill,
CANCELLED remainder, Pepperstone, decrease then scale-in, decrease with entry change, entry correction, roll adjust
across a takeover and the next day's roll, `position_gone` never re-armed, pinned version missing, ESTIMATED upgrade and
none after the cap, ESTIMATED never the placement, repin `kind_changed`, rearm leases before the CAS, stale-image
handle count, disarmed and still OPEN with no page or ARMED demand) and `test_arming.py`. G after the fixes: pytest
4417 passed, 54 skipped; mypy strict clean (275 files); `verify_execution.py --strict` 11 / 11. The S25 sign-off item of §9.1 (the
`registry.py` change) is now exercised by its first arming caller and still needs the user's sign-off. No `.claude/`
change is needed.

* **C:** `src/riskmgr/tracker.py`: snapshot every 2 s of accounts with OPEN positions; arming guard; `UNASSIGNED` /
  `UNSUPPORTED` (no broker bars for a needed side, asset class, demand ceiling, `arming_requirements`, and
  `stop_read_budget` when the armed-handle ceiling per adapter is reached, R11-M20); entry time and S0 sources; pins;
  Phase 1 single edge: `edge_code = XTNL_ARM_EDGE`, `attribution_source = SINGLE_EDGE_DEFAULT`, and a config enabling a
  second edge is refused (multi-edge contract in §B11, R11-M18); the lease set (ARMED specs, TICK cross-check specs,
  pre-warm set ≤ 8 per adapter) renewed every 15 s and re-asserted after `NO_BOOT` and a takeover; position changes
  (partial fills re-pin E and S0, SUSPENDED, ROLL_ADJUST, replaced, CLOSED); a new position on a FAILED scope arms into
  FAILED; one-shot `POSITION_NOT_MANAGED`; "assigned position not armed after 5 minutes" page. Request work items (from
  the S32 handlers, run in this task): `arm_disarm` (→ DISARMED), `arm_repin` (`migrate_state` under the `row_version`
  CAS; refused when the target version is not registered), `arm_rearm` (SUSPENDED → ARMED, cursor as at arming, §B6.6);
  each completes its request as applied, refused or moot. Every state change goes through `sm11.next_state`.
* **Tests:** a stop-entry order placed hours before its fill, live and through the catch-up; partial fills; the
  late-arming cursor; the handle ceiling gives `UNSUPPORTED(stop_read_budget)`; leases re-asserted after `NO_BOOT`;
  a second enabled edge in the accepted document is refused; disarm, repin (state migrated, CAS miss retried) and rearm
  of a SUSPENDED position.

### S27 ARM-6: BarClock, per-event transaction, sinks

Status: done 2026-11-16 (nothing is wired, both flags stay off). **Built without building on the unaccepted S21**, as
S22 was: no S27 module imports `src/riskmgr/pipeline.py`, `stopbasis.py` or `history.py`; the evaluation is the
`EvaluationPort` (`prepare(EvalRequest) -> Prepared(JobSpec, job) | NotReady`), which S31 fills with the S21 pipeline
once the user confirms the §9.2 Oversee change and C1. Built: `src/riskmgr/barclock.py` (`BarClock`, the `arm` task:
every 250 ms its own `WallMonoDetector` and the suspend signal hold the pass on a step; no lease, no decision;
`arm_controls` cached 1 s, unreadable = every due event held, PRE dropped at its valid-until (`controls_unreadable`),
a POST spanned at its cap with that reason; paused = `Hold(paused)` for every position, no evaluation, "paused for
more than 6 h" paged once per pause, the paused bars spanned `paused` (INFO) on resume once their cap passed; bar-end
events in rounds through the S22 planner (`replay.next_step` / `advance` over the grid's open bar ends after the
cursor): spans first, then POST / LATE (`PostNoticeView`) / REOPEN (`SessionCloseView`, 5 s cadence, retried and
never blocking the first post-open POST), so bar n is decided or spanned before bar n + 1 in the same pass; a POST
decided after `T + post_grace` carries `delayed`, a series still waiting then pages once per episode; a FAULT span pages
once per position episode; a final POST that came out `MARKET_CLOSED` with `trigger_met` (`reopen_candidate`) on a
pre-close bar stores `reopen_bar_end_ms`; PRE of the forming bar at local `T + p50 − pre_offset` until `min(fire +
10 s, local T + p50 − 30 s)` (`fire_ms` / `valid_until_ms` in the request, `previous_post_pending`), none on a closed
bar, dropped by reason; every market-data read in `prepare`, the md generation read before and after the round's
reads (a change discards the round, `md_unavailable` when unreadable); jobs through the S22 `JobRunner`; one fenced
transaction per (event, bar end) through the sink chain; every state change through `sm11.steps`; `decision_seq`
under the CAS; `close()` returns the job runner's hung flag for the hard exit), `src/riskmgr/stopreads.py`
(`StopReads`, active only with `XTNL_ARM_STOP_READS=1`, reads as tasks so the BarClock never awaits broker I/O; one
list read per adapter per anchor through `StopHandleReadPort` for the handles `StopHandlesPort` names; PRE anchor at
local `T + p50 − pre_offset − 15 s` with the covered positions remembered (a position armed later gets no PRE that
bar); POST anchor at `T + p50` (`StopReadView.at_bar_end`, the `S_at_T` read); POST re-read when ready and the newest
read is over 15 s old, retried every 10 s; on-demand reads before LATE / REOPEN / CORRECTED, REOPEN staggered (one per
10 s per adapter), coalesced while any read of the adapter is in flight; age from the send stamp, bounds 30 s PRE /
15 s otherwise; `stop_stale` counted per position), `src/riskmgr/sinks.py` (`Sink.on_decisions(tx, event,
bar_end_ms, decisions)`, `ARM_SINKS = ("decision", "notice")` enforced by `SinkChain`; `DecisionSink`: each position's
row, handles and CAS through `record_decision` (its own savepoint), a position change alone through the CAS, a
transient error recorded as `WRITE_ERROR` for that position only, a fatal one propagated; `DeferredNoticeSink` until
S28: inserted decisions keep `notice_id` NULL, which the S28 notice transaction reads). Tests:
`tests/riskmgr/barclock_fakes.py`, `test_barclock.py` (21: POST on time in one transaction; a replayed POST from a
stale row gives `ALREADY_DECIDED`, no `EpochMismatch` and no duplicate; a transient error in one savepoint; a broker
bar final at `T + 3 min` gives a `DELAYED` POST with one post_grace page; a bar never final spanned
`UNRESOLVED(broker_bar_not_final)` before the next POST in the same pass; PRE at offsets 30 s and 45 s; PRE not decided
by its valid-until; previous POST pending and none on a closed bar; unreadable controls hold and end by the window,
and until the cap give a span; paused: no decisions, one 6 h page, `paused` span on resume; FAILED held; a clock
step; no lease; a generation change; a job fault and a non-final POST write no row; REOPEN at the open; PR9
armed-after-the-PRE-read; SM11 release), `test_stopreads.py` (9: with the BARS bucket idle and saturated at offsets
30 s and 45 s PRE is never `stop_stale`; 8 positions × 2 handles cost one list read per anchor; the `T + 3 min` POST
takes `S_at_T` from the `T + p50` read and the post-guards from a fresh re-read; `XTNL_ARM_STOP_READS=0` queues
nothing; REOPEN staggering and coalescing; a PRE whose read never returns is not sent and counts `stop_stale`),
`test_sinks.py` (3). G: pytest 4450 passed / 54 skipped, mypy strict clean (278 files), `verify_execution.py
--strict` 11 / 11; the lockout real-tree scan passes with the new modules.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only);
(2) the S21 join is a port (see above), so the tests "`S_at_T`" and "live post-guards" check what the BarClock hands
the evaluation (the `T + p50` read as `at_bar_end`, a fresh read as `current`, `delayed`), not the pipeline's use of
them; (3) a non-final bar-end result (a POST-time `DATA_UNAVAILABLE`, a job fault, an evaluation still waiting for its
stop read) writes **no** decision row: the `arm_decisions` key `(position, bar_spec, event, bar_end_ms, revision)`
would make that row block the bar's final POST; the position is marked `pending_event` / `pending_bar_end_ms` once
(position CAS only) and the reason becomes the bar's expiry reason (a FAULT span at the cap, e.g. `stop_stale`); PRE
rows are written once per bar as the evaluation returns them; (4) LATE needs the route's "POST notice has gone" fact,
which S28 owns: `PostNoticeView` is a port, and with none wired every bar-end item is POST; (5) pre-close facts
(`next_open_ms`) come from a `SessionCloseView` port (S31, from the calendar); (6) the armed handles per adapter come
from a `StopHandlesPort` (S31, from the tracker's book); (7) a POST anchor read is queued only within 60 s of `T +
p50` (a BarClock started later relies on the POST re-read), and a REOPEN that ends at its deadline sends no "last
HOLD" here (the notice is S28's); (8) correction re-checks (`events_since`, `corrections.check_decision`, CORRECTED
items), the stuck-frontier watch and the error budget are not run by the BarClock in S27 (S28 / S30 consume what it
records; the CORRECTED on-demand stop read exists); (9) the `ARM_SINKS` assertion of the scanner on the runtime wiring
belongs to S31 (the wiring does not exist yet); (10) no `.claude/` change and no new `verify_execution.py` check (the
plan lists none).
Review fixes (2026-11-16): `PgArmTxn.update_position` (the `pending_event` CAS alone) now runs in its own savepoint, so
its error no longer aborts the (event, bar end) transaction and every other position's decision in it (the ARM
emulator now models PostgreSQL's aborted transaction; tests over the real store and through the BarClock); a
`pre_offset` with no PRE window (30 s: valid-until == fire) is refused when the BarClock is built, as the config
already refuses it, `run` wakes for the next PRE fire when it comes before the next pass, and the PRE / PR9 tests now
use offsets 31 s and 45 s with passes 100 ms after the fire and a gated stop-read port (the BARS-saturated request
leaves at the fire time; a slow answer 400 ms later is still decided in the window); the default `fatal` predicate is
`src.db.arm_ports.is_lost_fence` (by class name: `arm_ports` imports only the stdlib and `src.dto`), so an
`EpochMismatch` propagates even with none injected; a final result whose SM11 events the state refuses is not final
(`sm11_refused`: no row, the bar stays pending, re-read, spanned at its cap); unreadable controls keep a bar's earlier
pending reason; a bar-end item asks for its stop read before it is prepared (no prepare while it waits, and the view
matches the freshness check); prepares run 8 at a time with the stamp refreshed after each group and job; closed
positions are pruned from the BarClock's page / REOPEN books and `StopReads` (`forget`); no stop read is queued while
paused; a read without `sent_ms` counts as failed (`no_send_time`); `ARM_SINKS` stays a copy of the lockout's
`PHASE1_SINKS` (no scoped module may import the scanner), kept equal by a test. Tests: `test_barclock.py` 29,
`test_stopreads.py` 12, `test_sinks.py` 4, `tests/db/test_arm_store.py` +3. G: pytest 4465 passed / 54 skipped, mypy
strict clean (278 files), `verify_execution.py --strict` 11 / 11.

* **C:** `src/riskmgr/barclock.py` (scheduler over the pinned `(adapter, bar_spec)` set; every 250 ms: lease check,
  suspend and clock-step check, controls cached 1 s (unreadable = hold and retry), due events (PRE at `T + p50 −
  pre_offset`; POST when bar T is final and its indicators are updated for T); every market-data read finishes before
  `BEGIN`; read-session generation check; evaluation through `jobs.py`; one fenced transaction per (event, bar end) with
  a savepoint per position; a POST-time `DATA_UNAVAILABLE` does not advance; at the cap `UNRESOLVED` and the frontier
  advances before the next POST; "paused for more than 6 h" pages once, §B13), `src/riskmgr/stopreads.py` (PR9
  schedule, active only when `XTNL_ARM_STOP_READS=1`, through `StopHandleReadPort`: one list read per adapter queued at
  local `T + p50 − pre_offset − 15 s` for PRE and at `T + p50` for POST, a re-read when the POST is ready and the newest
  read is older than 15 s (retried every 10 s within `decision_cap`), on-demand before LATE, REOPEN and CORRECTED items;
  age counted from the send stamp; bounds 30 s PRE, 15 s otherwise; REOPEN reads staggered per adapter and coalesced
  per anchor, R11-M20; `stop_stale` counted per position as a metric; a position armed after its PRE read was queued gets
  no PRE that bar), `src/riskmgr/sinks.py` (`Sink.on_decisions(tx, event, bar_end, decisions)`; the fixed
  `ARM_SINKS` order: decision sink, notice sink).
* **Tests:** a replayed POST gives no `EpochMismatch` and no duplicate; a transient error in one savepoint; a broker bar
  final at `T + 3 min` gives a `DELAYED` POST on `S_at_T` with live post-guards; a bar never final is UNRESOLVED before
  the next POST; PRE at offsets 30 s and 45 s; unreadable controls hold the event; paused means no decisions; a clock
  step; PR9 (design PR9 tests): with the BARS bucket idle and saturated, offsets 30 s and 45 s, PRE is never
  `stop_stale` when its read returns; 8 positions × 2 handles on one adapter cost one list read per anchor; a broker bar
  final at `T + 3 min` gives a POST with `S_at_T` from the `T + p50` read and post-guards on a fresh read; with
  `XTNL_ARM_STOP_READS=0` no stop read is queued.

### S28 ARM-7: notices and the send-time renderer

Status: done 2026-11-16, review fixes applied 2026-11-16 (see "Review fixes" below; nothing is wired, both flags stay off;
no S28 module imports an S21 module). Built:
`src/riskmgr/render.py` (the ids-only outbox body `NoticeBody` / `ItemBody` / `PendingBody` / `ExtraBody` /
`FailedLine` / `AliveCounts` with `encode_body` (canonical JSON) and a strict `decode_body` held to `BODY_KEYS`,
`ITEM_KEYS`, ...: notice id and key, decision ids, outcome codes, order-state fields (proposed stop, pinned `E` and
`S0`, `current_risk_r`, first-bar ratio) and the trigger bar's id with its keyed hash, `trigger_input` (the BROKER bar
of the spec at the decision's bar end, the exit side when both sides are present); the pure renderer `render(body,
bars=, keyring=, settings=)`: subject with the notice id, event, part and bar end in UTC and Melbourne; every
outcome's text, the §B8 release texts (`PRE_NOT_CONFIRMED` says what releases the position now, R11-M04) and the Q10
line; LATE / REOPEN / CORRECTED labels (a correction superseding an earlier decision says the earlier advice is
withdrawn); the `PROVISIONAL` header and "valid until" on PRE; bar 0's conditional first-bar label (`pre_label`) from
the caveat `first_bar_conditional` (D2); `DELAYED` with its delay; caveat texts; the entry only with Q13; every data
text escaped (`escape_text`); favour R and favour / risk only when the re-read bar is supplied and its keyed content
hash verifies (`hashing.verify`), omitted otherwise and never for a PRE item (C5); `bar_reads(body)` names the
`(series, bar_end_ms)` reads the S29 relay makes), `src/riskmgr/notices.py` (`ArmNotices`: routes by local account
alias (`RouteBook`, else `adapter_id:account_id`), policy through `AcceptedPolicies` (`route_policy_at`, the §B8
defaults without a document); "decisions of this route with `notice_id` IS NULL" read in every notice transaction,
each attached, suppressed (sentinel 0) or left for part 5; PRE / POST bar notices: part 0 with the first decisions,
POST part 0 at `T + post_grace` listing the pending positions, parts 1-4 per later commit, after part 4 a POST
`ADJUST` / `COMPLETE` sent at once as its own item notice (event POST, part 0,
`adapter_id:position_id:resource:owner:revision`, R11-M19) and every other decision left for part 5 at the window end
(PRE valid-until, POST `decision_cap`), part 5 only with items; a POST committed after part 5 sent as an item notice,
a PRE item past its window suppressed and counted; LATE / CORRECTED / REOPEN always their own part-0 item notices
(REOPEN keyed by its decision time); what is shown: the evaluation's `shown` flag, a repeated `AT_TARGET_UNCONFIRMED`
(D3, also without the flag), the `actionable` mode rules (PRE `ADJUST`; at bar end `ADJUST`, HOLD, any FAULT,
`COMPLETE`, `PRE_NOT_CONFIRMED`, a change from PRE, a correction, a folded item), so every position released gives no
bar notice (§B1); Q9 extras (`TrackerNoticePort.notice`) fold into the route's next bar notice or go out alone one bar
later (`fold` inside / separate / off), `POSITION_NOT_MANAGED` keyed at bar end 0 (one-shot for good), `MD_RESET`
(key: the md generation) and `FAILED_CLEARED` (key: the failure id) on route `*`; the hourly `ALIVE` per route with
managed positions (managed and FAULT counts, open FAILED scopes through `FailedLinesPort`; in `every_bar` mode only
after an hour without another notice), the daily `TEST` per channel; FAILED status lines on every bar notice; the pure
`supersedes` and `expires_at_ms` rules for the S29 relay (PRE at local `T + p50` and superseded by its POST; POST parts
and items one bar after creation and superseded by the route's next POST part 0; a part-5 notice never before it has
been sent once); a decision NULL for more than 60 s (a part-5 waiter: 60 s after its window end) pages once per route
episode while the route's channels are configured; `post_sent` is the BarClock's `PostNoticeView`; `NoticeSink` is
the second `ARM_SINKS` element; `step` / `run` the 5 s timer task). Changed: `src/riskmgr/sinks.py` (`Decided.shown`,
docstrings; `default_sinks` keeps the pass-through `DeferredNoticeSink` until S31 passes `ArmNotices.sink()`),
`src/riskmgr/barclock.py` (`Evaluated.shown`, passed to `Decided` on bar-end and PRE writes). Tests:
`tests/riskmgr/notice_fakes.py`, `tests/riskmgr/test_notices.py` (34: 8 symbols final at 8 times each get their ADJUST
in their own commit (parts 0-4, then item notices); 6 positions after part 0 give parts 1-4 and part 5 at the cap;
part 5 only with items and a later POST as an item; 2 LATE items in one bar both queued; REOPEN and CORRECTED keys;
D3 at-target shown once; every position released gives no bar notice; actionable mode; the PRE notice of bar 0 with
the conditional label; a PRE ADJUST with no favour whatever bars are held (C5); POST part 0 at grace with the pending
list; PRE part 5 at valid-until; the outbox INSERT payload carries no price-derived field (key allow-lists, no banned
key name, no bar price in body or subject, favour only after the send-time render); extras folded, alone one bar
later, separate, off; MD_RESET once per generation; ALIVE once per hour and skipped after a busy hour; the daily TEST
per channel; the 60 s page; unrouted accounts; supersede and expiry rules; the BarClock with the real chain
suppressing a silent evaluation), `tests/riskmgr/test_render.py` (37: every outcome, every event, every extra kind,
pending / ALIVE / TEST / MD_RESET / FAILED lines, release texts, favour only from a verified bar (missing bar, no
keyring, tampered bar, unheld key id, PRE, short side), the conditional label, valid until, DELAYED, Q13, escaping,
subject, body round trip and strict decoding, trigger-input choice and `bar_reads`). G: pytest 4536 passed / 54 skipped
(after the review fixes), mypy strict clean (280 files), `verify_execution.py --strict` 11 / 11; the lockout real-tree
scan passes with the new modules.
Review fixes (2026-11-16): (a) the notices' memory follows the commit: every notice transaction stages its memory
changes (part counters, closed windows, D3 / PRE memory, notice hours, counters, taken extras) in a `_Stage`, applied
only after the commit (the timer pass's own transactions on exit; the BarClock's through the new
`SinkChain.settle(tx, committed=)`, which `BarClock._commit` calls after every transaction and `NoticeSink.settle`
forwards), discarded on a rollback with the taken extras put back; ALIVE and TEST count as done only after their
commit; a part that already exists is a key conflict and the next part is tried; (b) Q9 extras fold into POST bar
notices only, never a PRE one; (c) ADJUST names the handles to move with the stop's read time (`ItemBody.
stop_read_at_ms`, the latest `read_at_ms` of the moved handles, order-state time) and "only if your stop is still
worse than this" (`render.STILL_WORSE`), PRE `provisional: ...` too; (d) the category text is escaped in every
renderer branch; (e) a POST decision committed after its part 5 carries `ItemBody.late` and renders with the LATE
label; (f) a failing route is counted and skipped (`step` guards each route, the global extras and the TEST); (g)
`_prune` also drops route info of positions gone `keep_ms`, rebuilds the members (live and remembered positions,
queued extras) and drops ALIVE marks, notice hours and pages of routes no longer there; (h) a decision found waiting
for part 5 (its parts 0-4 are key conflicts, e.g. after a takeover) counts as a waiter for the 60 s page. Tests added:
a fake transaction that really rolls back (`NStore.fail_commits` / `fail_events`), five failed BarClock commits keep
part 0 and the extra, a rolled-back at-target hold stays silent, a failed ALIVE / TEST is retried, a failing route
does not hold back the others, PRE-expired counting, the part-5 waiter's page timing, a takeover does not page,
extras never on PRE, handles and read time in the body, the LATE label, category escaping, pruning.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only), as in
S25-S27; (2) the outbox insert is the `NoticeOutboxPort` (`enqueue(tx, OutboxNotice)` in the notice's transaction),
which S29's ARM outbox store implements (`arm_notices.outbox_id` stays unset, S24 deviation 6); (3) `Decided.shown` /
`Evaluated.shown` were added to the S27 modules: the pipeline's `PipelineResult.shown` (released positions, repeated
at-target holds) reaches the notice sink through them in the S21 join (S31); a silent decision is suppressed in the
same transaction, so the flag is never needed after commit; (4) bar 0's conditional first-bar label travels as the
caveat code `first_bar_conditional` (`render.FIRST_BAR_CONDITIONAL`; the S21 join maps the guard's
`conditional_label` to it), since `arm_decisions` has no label column; (5) symbol, side, `E` and `S0` are not on the
decision row: the notices take them from the position rows (`learn`: the CAS row of each fresh decision,
`live_positions` in the timer pass), so a leftover decision of a position already CLOSED before a takeover renders
without them (and without favour figures); (6) handles to move are known only for fresh decisions (the sink path), so
an item rebuilt by the timer pass after a restart lists no handle ids and no stop read time (the
qualifier "only if your stop is still worse than this" is still printed); (7) Q9 extras, part counters, closed windows,
the D3 memory and the PRE outcomes are in memory, changed only after a commit (review fix (a)): after a takeover a
part insert that conflicts tries the next part (so a decision whose parts 0-4 exist is found to be a part-5 waiter and
does not page), and an extra queued but not yet sent is lost (the tracker's own one-shot memory is not durable
either); (8) a notice
failure inside the BarClock's transaction fails that transaction, decisions included (PostgreSQL aborts the whole
transaction on any error), so "no decision without a notice" holds and the decisions are retried on the next pass;
(9) the expiry of the non-bar notices is a choice the design leaves open: `ALIVE` one hour, the others (`TEST`,
extras, `MD_RESET`, `FAILED_CLEARED`) one day; the daily `TEST` time is 22:00 UTC by default; (10) LATE / CORRECTED /
REOPEN items are always sent at once as their own notices (the §B8 key definition makes them part 0 with an item
key); Q9 folding applies to the tracker extras only; (11) the body key for the decision's close source is
`trigger_source` (a key named like `close` would trip the S38 name scan); note for S38: kind (a)'s reason code
`NOT_IN_FAVOUR` is a code, not a value, so a body scan must look at keys and numbers, not at the word "favour" inside
codes; (12) no `.claude/` change and no new `verify_execution.py` check (the plan lists none).

* **C:** `src/riskmgr/notices.py` (routes by account alias; parts 0-4 and the end-of-window part 5; **a POST ADJUST or
  COMPLETE committed after part 4 is sent at once as its own item-keyed notice, and a part-5 item is never superseded
  before it has been sent once**, R11-M19; item keys; the `suppressed` sentinel; expiry and supersede rules; Q9 folding;
  hourly ALIVE, daily TEST, `POSITION_NOT_MANAGED`, MD_RESET, FAILED status lines; **the outbox body holds only ids**:
  notice id, decision ids, outcome codes and order-state fields, R11-M14), `src/riskmgr/render.py` (pure renderer:
  decisions plus optional verified bar inputs → text; favour R and favour / risk only when the input bar is supplied and
  its keyed hash verifies; a PRE item has no input bar, so PRE notices carry outcome codes and order state only, never
  favour figures (design §B8, confirmation C5); §B8 allow-list; escaping; the PRE_NOT_CONFIRMED text says what will release the position
  now, R11-M04).
* **Tests:** a render test for every outcome and item; 8 symbols whose bars become final at 8 distinct times each get
  their ADJUST within one pass; 6 positions after part 0 give parts 1-4 and part 5 at the cap; 2 LATE items in one bar;
  the outbox INSERT payload carries no price-derived field; an "at target, not yet confirmed" HOLD is shown once, not on
  every bar (D3); every position released gives no bar notice (§B1); the PRE notice of bar 0 carries the conditional
  first-bar label (D2); a PRE `ADJUST` notice renders with no favour R or favour / risk, whatever bars the market-data
  fake holds (C5).
* **Resolves:** R11-M14 (body), R11-M19.

### S29 NT-1: `arm-relay` and the ARM outbox store

Status: done 2026-11-16 (nothing is wired: S31 / S32 build the relay task, the renderer's market-data port and keyring,
the drain flag and the page route; both flags stay off; no S29 module imports an S21 module). Built:
`src/db/arm_outbox_store.py` (`PgArmOutboxInserter.enqueue`, the S28 `NoticeOutboxPort`: inside the notice's own
`PgArmTxn` on the ARM pool, one fenced INSERT with no RETURNING, kind `arm`, severity `INFO`, the route's `arm_email` /
`arm_push` channels (non-empty, no repeats, else refused), subject, the ids-only body, `next_attempt_at_ms` = creation
and `expires_at_ms`; `PgArmOutboxStore` on the engine (owner) pool: `due` (next attempt due or expiry passed),
`pending`, `recent` (the supersede read), `oldest_undelivered_ms`, `reschedule` (`encode_arm_channels`: `name` /
`name:n`, never empty, never `+escalated`, so the 0016 CHECK always holds; `delivered_at_ms` = the first channel
acceptance), `finish` (sent / expired / superseded / suppressed / bad_body: `sent_at_ms`, `delivered`,
`superseded_by` and the body replaced by `finished_body`: notice id, notice key, decision ids, outcome codes and the
status; a ping's plain-text body is kept); every statement names `kind = 'arm'`), `src/notifications/arm_relay.py`
(`ArmRelay`, its own class: per-channel delivery with min(60, 2^n) s back-off, no dedupe, no escalation, no fallback,
notices at `info` and the ping copy at `high`, never `critical`; lease checked before the batch and before every send;
expiry (finished scrubbed; an expired POST / LATE notice with an `ADJUST` pages once); `drain_notices()` finishes due
notices as `suppressed` and never a ping; supersede through `src.riskmgr.notices.supersedes` against the kind `arm`
rows created since the oldest due notice, a part-5 notice only once `delivered_at_ms` is set; a body that does not
decode is finished `bad_body`, never sent; `SendTimeRenderer` re-reads each `bar_reads(body)` bar through the
market-data port (`bars(series, at_bucket_ms=bar_end - timeframe)`, 2 s timeout) and calls the pure `render`, which
shows favour figures only for a hash-verified bar; a failing renderer falls back to the text without bar figures; "ARM
channel X failing" pages once per down episode after 5 consecutive failures; channel credentials follow a
`ChannelSource`; `oldest_undelivered_ms()` for the dead-man field; a `TYPE_CHECKING` check that the stores fill
`NoticeOutboxPort` and the relay's ports and that `MarketDataPort` fits `BarReadPort`). Changed:
`src/notifications/settings.py` (`from_env(env, prefix=)` with `OVERSEE_PREFIX` `XTNL_` (the default; the parse is
unchanged) and `ARM_PREFIX` `XTNL_ARM_`; `_email`, `_push`, `_recipients`, `_ses`, `_smtp` take the prefix;
`CHANNEL_VARS`, the one list of channel variable names, anything else refused by `_var`),
`src/notifications/factory.py` (`build_channels(..., names=)`, `build_arm_channels`, `ARM_CHANNEL_NAMES`),
`src/notifications/rotation.py` (`settings_from_secrets(..., prefix=)`, `RotatingChannels(..., names=)`),
`src/db/lifecycle.py` (step 5: expired unsent ARM rows finished in batches with `finished_body`, whether or not the ARM
runs; `LifecycleReport.arm_expired`; errors reported by class name only). Tests: `tests/db/arm_outbox_emulator.py`
(PgEmulator plus the S29 statements on the one `notification_outbox`; every kind `arm` write checked against the 0016
CHECK regex read from the migration), `tests/notifications/test_arm_relay.py` (27: the ids-only INSERT, its refusals and
a lost fence; send-time favour from a verified bar and the scrubbed finished body with no order state, price or favour
in any column; favour omitted for an unavailable bar, `redis-md` down, no port, no key, a tampered bar and PRE;
renderer fallback; a failed channel rescheduled 8 times in CHECK form, never `+escalated`, no fallback, one page;
partial delivery retries only the failed channel; an unconfigured channel waits at the cap; non-ARM channels refused;
no lease, no send; expiry with its page; PRE superseded by its POST; a part-5 notice not superseded before one channel
took it; drain suppresses notices, never a ping, and the ping goes at `high`; `bad_body`; oldest undelivered; the
Oversee relay never touches an ARM row and the ARM relay never an Oversee row; the ARM channels down while a HARD
alert is delivered by the Oversee relay with no escalation), `tests/db/test_arm_outbox_store.py` (30: every statement
names `kind = 'arm'`, writes fenced with contiguous parameters, the insert without RETURNING; finished bodies keep ids
and codes only; the notice key read from full and finished bodies; reschedule encodings pass the CHECK and invalid ones
are refused; pending / oldest / recent read ARM rows only; the housekeeping expiry: scrubbed bodies, pings and Oversee
rows untouched, sanitised errors, a lost fence raises, `run_once` runs it), `tests/notifications/test_settings.py`
(+24: Oversee settings parity over ten environments whatever `XTNL_ARM_*` holds, the same rules under the ARM prefix
with renamed issues, neither prefix reads the other's variables, an unknown prefix refused, `build_arm_channels`,
rotating ARM channels keep their names). G: pytest 4616 passed / 54 skipped, mypy strict clean (282 files), `verify_execution.py --strict` 11 / 11; the lockout real-tree scan passes.
Review fixes (2026-11-16): (1) a body that decodes but cannot be rendered (the fallback `render` raising too, e.g. an
out-of-range `bar_end_ms`) is finished `bad_body`; each row runs isolated in `tick()` (a lost fence or a PostgreSQL
connection error still stops the tick; anything else is logged by class name, counted in `row_errors`, and the batch
goes on), so one bad row never blocks the rows or the ping behind it; (2) housekeeping step 5 waits
`LifecycleConfig.arm_expiry_grace_ms` (1 h) past expiry so the relay finishes and pages first, and an expired POST /
LATE `ADJUST` notice it does finish is reported (`LifecycleReport.arm_expired_adjust`, notice ids) and paged by
`LifecycleRunner.pages` (`pages_when_expired`, the stored-body form of `expired_adjust`); (3) `--drain-notices` also
finishes queued notices still in back-off (`pending`) as `suppressed`, never a ping, an expired row keeping the expiry
path; (4) counters and the expiry page follow the `finish` / `reschedule` result; (5) `SendTimeRenderer` reads a body's
bars concurrently under one overall timeout and, after a failed or timed-out read, skips the port for the rest of the
tick (`new_tick()`); (6) the render settings live on `SendTimeRenderer` only (`fallback(body)`; a differing
`render_settings` is refused). Tests: `test_arm_relay.py` +8 (unrenderable row before a good notice and a ping, row
error isolation and a connection error stopping the tick, drain of a row in back-off, a row finished elsewhere first,
lease lost mid-batch, LATE expiry pages and PRE does not, hung market data, one settings source),
`test_arm_outbox_store.py` +6 (grace and the housekeeping page; the stored-body page rule equals the relay's). G: pytest
4630 passed / 54 skipped, mypy strict clean (282 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only), as in
S25-S28; (2) a finished body also keeps the notice key (route, bar spec, event, bar end, part, item key: ids), so the
supersede read works on finished rows; order-state fields are dropped; a ping body (plain text: scope, reason code,
position ids, §B14) is kept as written; (3) "sent once" for the part-5 rule is `delivered_at_ms` (the first channel
acceptance, set by the reschedule and the finish); (4) there is no attempt cap: a notice ends by delivery, supersede
or expiry, and a ping retries until delivered (its critical copy goes through the Oversee relay); a channel that is
not configured is retried at the 60 s cap; (5) the ARM relay sends the ping copy at `high` (Pushover priority 1),
never `critical` (Pushover emergency priority, RT-M4); (6) the pages ("ARM channel X failing", an expired ADJUST
notice) go through an injected `page(severity, title, body)` callable that S32 wires to the Oversee route; (7)
`CHANNEL_VARS` is a frozenset of the literal names (the dead-man receiver's coverage test reads them; a set keeps the
deploy env-var scanner from counting the optional SES / SMTP names as required assignments); (8)
`arm_notices.outbox_id` stays unset (the ARM role cannot read outbox ids, S24 deviation 6); (9) no `.claude/` change
and no new `verify_execution.py` check (the plan lists none).

* **C:** `src/notifications/arm_relay.py` (its own class, not `OutboxRelay`: no escalation, no fallback, no emergency
  priority, `lease_valid`; channels `arm_email` / `arm_push`; at send time it calls the injected renderer, which re-reads
  each input bar through `MarketDataPort` by `(series, bar_end_ms)` and checks the keyed hash, omitting the numbers if
  unavailable, R11-M14; the finish statement replaces the body with ids and codes; expiry and supersede; `--drain-notices`
  suppression never touches pings, RT-M4), `src/db/arm_outbox_store.py` (own insert with kind `arm` and encoded
  `arm_*` channels; due, pending, finish and oldest-unsent queries with `kind = 'arm'`; the relay's queries and UPDATEs run on the
  engine (owner) pool like the Oversee relay, the ARM pool only inserts).
* **M:** `src/notifications/settings.py` (`_email`, `_push`, `_recipients` take a prefix, `XTNL_` or `XTNL_ARM_`; the
  Oversee settings parse exactly as today), `src/notifications/factory.py` (ARM channels via `RecordingChannel(name=...)`
  in tests and named real channels), housekeeping expiry path in `src/db/lifecycle.py` (finishes expired ARM rows with
  the body scrubbed).
* **Tests:** an ARM row rescheduled after a failure passes the CHECK; no `+escalated` ever; the Oversee relay never
  touches an `arm` row; the ARM channel down while a HARD alert is delivered by the Oversee relay; unsent, expired and
  finished bodies have no price-derived field; Oversee settings parity (`tests/notifications/test_settings.py`).
* **Resolves:** RT-M4, R11-M14 (send-time render).

### S30 FL-1: error budget, FAILED, immediate ping, clear probes

Status: S30a done 2026-11-16 (S30b done, see below; nothing is wired: S31 / S32 build the sinks, the ARM task loop, the
`FailedSpoolPortImpl`, the dead-man field and the `arm_clear_failed` route; both flags stay off). Built:
`src/riskmgr/budget.py` (`ErrorBudget`: the one counting unit (one-shot series faults at K from md-bars / D6 /
indicators / refetch; `note_unresolved` adds at most 1 per bucket at its live cap, `note_final` resets it); counting
pauses (market not OPEN, `MaintenancePort`, adapter not STREAMING or REST failing everywhere, unknown REST for bar
reads, engine PostgreSQL down for the two decision-cap counters); a one-shot fault during a pause is deferred and
filed by `poll()` when the pause ends; `fail_md_down_s` / `fail_pool_down_s` time counters that add only gaps up to
30 s (a PC sleep counts nothing) and cap time older than 60 s never counts; stuck frontier through `FrontierWatch`
(counting only while running and engine PostgreSQL is up, held positions never count, the `ARM_WRITE` cause filed as
`pool_down`); STRATEGY_VERSION faults from `JobRunner` signals with the `edge` label; ARM store faults
(`md_resets`, `bootstrap_failed`, `md_oom`) and `on_circuit_reopen` for the six ARM tasks; `adapter_due` for
`fail_global_series_n`; `probe_kind`, the riskmgr copy of `probe_kind_for`, held equal by a test; `reset(scope)` for
the S30b clear), `src/riskmgr/failed.py` (`FailedManager`: `enter` (the budget's listener) puts the scope into
`FailedScopes` in memory first, with no I/O, and log-mirrors the ping through `Pager.log_mirror` only; `tick()`
records the row, the critical `info` copy and the kind `arm` copy in one fenced transaction through
`ArmFailuresStore.record` (engine pool for `pool_down` or while the ARM pool is failing), retried from memory every
5 s, appended once to the `FailedSpoolPort` on the first failure and acknowledged after commit; SM11 `SCOPE_FAILED`
for every covered non-terminal position through the `row_version` CAS (`state_reason` `scope_failed`); re-ping every
`fail_reping_h` (same ping id, one critical row per ping); collapsed pings: an ADAPTER / ARM episode folds the pending
episodes it covers and a scope failing under an open ARM (or its ADAPTER) episode is folded, recorded with
`queue_ping=False` under the collapsing ping id; `start()` replays the spool, then reads the open rows (until then
every position is held); `submit_clear` only queues and `tick()` answers every clear `refused: probe_not_built`;
`FailedScopes` fills the tracker's `FailedScopeView` and the notices' `FailedLinesPort`, `series_failed` the
`DATA_UNAVAILABLE(failed)` reads, `summary()` the dead-man's `arm_failed`). Changed: `src/db/arm_failures_store.py`
and `src/db/arm_ports.py` (`record(..., queue_ping=True)`: `False` writes the row only, for a folded episode). Tests:
`tests/riskmgr/test_budget.py` (43), `tests/riskmgr/test_failed.py` (31, over the real `PgArmFailuresStore` and the
`ArmEmulator`), `tests/riskmgr/failed_fakes.py`, `tests/db/test_arm_failures_store.py` (+1). Review fixes
(2026-11-16): a scope folded under a collapsing episode is always named (rendered into the unsent collapsing ping at
write time, or one re-ping of the collapsing episode at the next tick once its ping went out; a folded episode whose
collapser closed unnamed re-pings on its own at once; every entry is log-mirrored); `FailedSpoolRecord` carries
`edge_code`, a fold after the spool append re-appends the episode and replay keeps the latest record of a failure id;
the ping's position list is recomputed at write time from a live-position read taken after the entry (the first write
waits up to `rows_every_ms` for it; optional `engine_positions` reads through the engine pool when the ARM pool
fails); deferred one-shot faults are kept per (series, reason), one is filed per series and a direct fault drops the
series' deferred ones; a `refetch_failed` raised while the broker is unavailable is dropped (`outage_refetch`), so S31
must have md-bars retry the refetch after the outage and signal again only if that fails; `summary()` reports
`count >= 1, global_failed=True` once a read of the open episodes has failed (until one succeeds); R11-M08 is tested
through md-bars and the mode / pause bypass behaviourally. G: pytest 4705 passed / 54 skipped, mypy strict clean (284
files), `verify_execution.py --strict` 11 / 11; the lockout real-tree scan passes.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only), as in
S25-S29; (2) the riskmgr may not import `src.db.arm_schema`, so `budget.py` keeps its own reason → probe-kind table,
held equal to `probe_kind_for` by a test; (3) `ArmFailuresStore.record` gained `queue_ping` (an S24 store change) for
the collapsed ping: a folded episode's row carries the collapsing ping's id and `ping_count` 1 and is re-pinged only
once its collapser is no longer open; (4) reason codes added: ADAPTER `series_failed`; ARM `md_down`, `pool_down`,
`circuit_reopen`, `md_resets`, `bootstrap_failed`, `md_oom` (all pass the 0017 CHECK and map to `arm_global`);
(5) a one-shot series fault arriving during a counting pause is deferred, not dropped (the market-data side signals it
once and keeps the series blocked); (6) which series a position reads comes from a `PositionSeriesPort` (S31, from the
tracker's book); with none, or an unknown answer, every series of the position's adapter and symbol covers it, and an
unparseable stored guard list matches every guard scope (fail closed); (7) the md-writer has no fault sink for an OOM
after the last shedding step or the 6 h queue bound: `ErrorBudget.store_fault("md_oom")` exists and S31 must feed it;
the indicator sink reports an indicator id, so S31 maps it to its series before `series_fault(..., indicator_faulted)`;
(8) the ping's "alias" is an injected `(adapter_id, account_id) -> alias` callable (S31 passes `RouteBook.route_of`);
(9) global FAILED stopping `md-bars` / `md-indicators` and the BarClock reads `FailedScopes.global_failed` /
`series_failed` once S31 wires them; nothing here touches `md-writer`, which keeps capturing ticks (the C2 default,
still to be confirmed); (10) no `.claude/` change and no new `verify_execution.py` check (the plan lists none).

Status: S30b done 2026-11-16 (nothing is wired: S31 / S32 build the probe ports' implementations, route
`arm_clear_failed` to `FailedManager.submit_clear`, and pass `probes`, `clear`, `hooks` and `cleared_notice`; both
flags stay off; no S21 module imported). Built: `src/riskmgr/probes.py` (`ProbeResult` (passed, or a check code and
the request to run first from `FIRST_COMMANDS`), `refusal_reason` (the answer code
`cause_present_<check>[_first_<request>]`), `probe_note` (`last_probe_result`); the probe ports `SeriesProbePort`
(`reread` for `series_reread`, `fresh_bar`; md-bars), `IndicatorProbePort`, `SpanProbePort`, `VersionProbePort`,
`TrialProbePort`, `CircuitView`, gathered in `ClearProbes`; concrete `TrialBootstrapProbe` (over
`MdWriterService.request_trial`: the trial bootstrap in the writer task, never counted, no MD_RESET notice) and
`IndicatorStateProbe` (every indicator on the series out of FAULTED, which only `md_rebuild` clears, and warm, else
refused with `md_rebuild` first); `ClearHooks` / `ScopeClearHooks` (SERIES → `CrossChecker.clear` and SM12 clear,
ADAPTER, STRATEGY_VERSION → `JobRunner.reset`, ARM → `MdWriterService.request_post_clear`)), `src/riskmgr/failed.py`
(the clear path: `submit_clear` only queues; the next `tick()` checks the target (`bad_target`, `episodes_unread`,
`probe_running`, moot `not_failed`, `not_recorded`, `probe_not_wired`) and starts the probe of the recorded
`probe_kind` as its own task under `probe_timeout_ms` (20 min; `probe_timeout` only when that deadline expired; any
other raise, a port's own TimeoutError included, is `probe_error`; a lost fence is re-raised by the tick; the loop
abandons a task still without a result `probe_grace_ms` (60 s) past the deadline, e.g. a port that ignores its
cancellation, as `probe_timeout`); a target with surrounding whitespace is `bad_target` at once; a new fault of the
scope while its probe runs refuses that clear `cause_present_fault_during_probe`; ADAPTER runs the probe of each of
its open SERIES episodes at the same time (first refusal in scope order, the rest cancelled); ARM global = no ARM task circuit
open (`reset_task` first), a fenced test write on the ARM pool (`note_probe(..., "probing")`), then the trial bootstrap
(`md_writer_reset` first); the result is noted on the row; a passed probe runs `ArmFailedClearPort.clear` (engine
pool; None → `request_mismatch`, an error → `clear_write_failed`), then in memory: scope removed, `ErrorBudget.reset`
(the scope's own counters only: an ADAPTER clear keeps every series' deferred faults and unresolved streaks), hooks, one FAILED_CLEARED notice, answer `applied`; `_restore_positions`, level-triggered every tick: a live SM11 FAILED
position no open scope covers takes `FAILED_CLEARED` back to its `prior_state` through the CAS (state reason
`failed_cleared`; a refused edge stays FAILED, logged once), so a CAS miss, a failing pool or a restart is restored
later; `drain()` answers running and queued clears `probe_interrupted`; `probing()` for `arm failed show`),
`src/db/arm_ports.py` (`ArmFailedClearPort`, filled by `PgArmFailedClear`, checked under `TYPE_CHECKING` in
`src/db/arm_failures_store.py`). Tests: `tests/riskmgr/test_failed_clear.py` (29: a series clear refused while the
cause is present (answer, note, FAILED kept) and accepted after (row cleared with its request id, positions restored,
hooks, notice, counters reset so a new fault opens a new episode and pings); the unresolved streak reset; the probe
never blocks the loop (a fault recorded while it hangs; a second clear `probe_running`); timeout; a raising probe;
a port's own TimeoutError is `probe_error`; a fault during the probe refuses the clear; a probe ignoring its
cancellation is abandoned by the loop; ADAPTER probes each series and keeps the series episodes; an ADAPTER probe of
two slow fresh-bar series (each over half the timeout) is applied; an ADAPTER clear keeps another series' deferred
fault, filed when its pause ends; `indicator_faulted` refused until `md_rebuild`, then until
warm; STRATEGY_VERSION refused, then accepted and the job runner's circuit reset; the stuck-frontier span probe; after
the AOF-aside runbook step (store flushed, new `run_id`) a global clear succeeds through the real `MdWriterService`
trial over fakeredis (refused first while a circuit is open and while the ARM pool fails; the trial neither counted nor
a notice; the post-clear bootstrap then run by the writer, neither counted nor a notice); a restart while globally FAILED (`md_down`) with an empty `redis-md` can
still be cleared; a failed trial names `md_writer_reset`; bad targets, not FAILED, a missing or other-epoch request, not
recorded, a lost fence raises, a failing clear write; drain; the restore after a CAS miss and by the next term; a FAILED
position without a valid `prior_state` stays FAILED; probe kinds equal the budget's table; codes only),
`tests/riskmgr/test_failed.py` (the S30a refusal test now expects `probe_not_wired` when no probe ports are passed),
`tests/riskmgr/test_budget.py` (an ADAPTER reset keeps every series' state). Review fixes (2026-11-16): ADAPTER reset
no longer drops per-series state, concurrent ADAPTER series probes, fault-during-probe refusal, the loop's probe
backstop, padded targets refused at once, a port's TimeoutError kept apart from the probe deadline, and the post-clear
bootstrap test.
G: pytest 4735 passed / 54 skipped, mypy strict clean (285 files), `verify_execution.py --strict` 11 / 11; the lockout
real-tree scan passes.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only), as in
S25-S30a; (2) the series re-read, fresh-bar, span-write and version probes are ports (`SeriesProbePort`,
`SpanProbePort`, `VersionProbePort`): their implementations need md-bars, the BarClock's store and the job runner and
come with the S31 / S32 wiring ("probe reads go to md-bars"); an unwired port refuses (`probe_not_wired`, fail closed);
(3) the ARM pool test transaction is the probe's own fenced `note_probe` write on the ARM pool (no new SQL); (4) the
ADAPTER probe (the §B14 wording, S00 deviation 3, confirmed here) runs the cause probe of each **open** SERIES episode
of the adapter and passes when none is open; clearing the ADAPTER episode leaves the SERIES episodes open (one request
clears one episode), so their positions stay FAILED until each series is cleared; (5) the `prior_state` restore is a
separate level-triggered CAS on the ARM pool, not inside the clear statement's transaction (the clear runs on the
engine pool; `PgArmFailedClear`'s `after` hook stays unused), so it survives a failed write and a restart; (6) the
operator answer is a code (`RequestAnswer` reasons are `[a-z_]` codes): `cause_present_<check>` plus
`_first_<request>` instead of the free text "refused: cause still present (<check>)"; (7) the `md show` / `arm failed
show` views, the ops CLI command and the `docs/runbooks/arm.md` FAILED section stay with S34 / S35 / S37; (8) no
`.claude/` change and no new `verify_execution.py` check (the plan lists none).

Built as two PRs, each green: **S30a** `budget.py` and `failed.py` up to the record, ping, re-ping and spool (the clear
handler refuses every clear with "probe not built"); **S30b** the probe kinds, the clear path and its tests.

* **C:** `src/riskmgr/budget.py` (counters per SERIES, ADAPTER, STRATEGY_VERSION and ARM; the one counting unit: a
  bucket counts once, at its cap, after its healing, R11-M08; pause predicates: market not OPEN, declared maintenance,
  adapter not STREAMING or REST failing at every priority (R11-M10), engine PostgreSQL down = the term pool's last
  successful fenced commit older than 30 s or the RiskSweep's "PostgreSQL unreachable" alert active (R11-M11);
  `fail_global_series_n` counted per adapter and failing that adapter's scope; ARM-global only for `redis-md`, the ARM
  pool, task circuits (`on_circuit_reopen`) and the stuck frontier; the stuck frontier counts only positions whose scope
  is neither FAILED nor paused while the ARM runs, with the span write itself failing, so a scoped FAILED never becomes
  a global one; it is an ARM-scope fault keyed by cause, deferring to
  `fail_pool_down_s` when the ARM pool or its transactions fail (R11-M11); one collapsed ping when a global FAILED
  follows N series), `src/riskmgr/failed.py` (enter FAILED in memory first; SM11 FAILED with `prior_state`; the record
  through `arm_failures_store` (the engine pool when the ARM pool is the fault), R11-M05; ping id in the title; body
  bounded to N positions then "and K more; see arm failed show"; retried from memory until committed; if the engine may
  stop before PostgreSQL returns, the episode goes to `FailedSpoolPort` (CRC-framed, like the DLQ spill) and is replayed
  at the next start before any advice, R11-m; re-ping every `fail_reping_h`; the log mirror is `PagerWrapper.log_mirror()`
  (`log.critical` only), never `EngineSession.page`, which queues its own critical outbox row; an accepted clear resets
  the scope's counters, and the probe's trial bootstrap and the post-clear bootstrap never count toward the reset budget; **probe kinds** chosen from the reason, R11-M12: `invalid_bar` / `source_mismatch` = rollback-set
  re-read passes every hard check and the cross-check; `unresolved` = one fresh bar read reaches finality within
  `post_grace`; `indicator_faulted` = `md_rebuild` completed and the indicator warm; `stuck_frontier` = a test fenced span
  write on the ARM pool; `strategy_version` = the conformance checks plus one successful job; ARM global = a trial
  bootstrap writes `md:boot`, an ARM pool test transaction succeeds and no circuit is open, R11-M09; a refused clear names
  the command to run first; `md_rebuild`, `md_backfill`, `md_writer_reset`, `md_rehash` and `reset_task` are allowed
  during FAILED; global FAILED stops serving and advice while `md-writer` keeps capturing ticks (§9.2), R11-M10).
* **Tests:** K invalid bars on one series fail only that series and its positions, other series keep advising, the
  ingestion rate and RiskSweep pass age are unchanged; `redis-md` down past `fail_md_down_s` gives a global FAILED with
  Oversee unaffected; a 30-minute broker REST outage gives no FAILED; the ARM pool down gives one ARM-scope fault, no
  series storm and one ping; the ping bypasses `actionable` mode and a pause; with PostgreSQL down `arm_failed` reaches
  the dead-man and the ping is written on recovery; FAILED survives a restart and a takeover; a clear is refused while
  the cause is present and accepted after; after the AOF-aside runbook a global clear succeeds; a restart while globally
  FAILED with an empty `redis-md` can still be cleared; an indicator_faulted clear is refused until `md_rebuild`; a PC
  sleep longer than `decision_cap` counts nothing; each ping (entry or re-ping) yields exactly one `kind='info'`
  critical outbox row; a SERIES FAILED held for 10 minutes never produces a stuck-frontier ARM fault.
* **Resolves:** R11-M05, R11-M08, R11-M09, R11-M10, R11-M11, R11-M12, R11-M17 (scope), R11-m (ADAPTER scope, spool,
  bounded body).

### S31 RT-1: `md_ports.py` and `optional-bootstrap`

Status: done 2026-11-16 (the bootstrap and the market-data graph exist and `build_engine` wires the factory, but
with both flags off, the default, nothing new is built, imported or run; no task specs or handlers yet: S32). Built:
`src/runtime/md_ports.py` (name-ban checked): `BrokerBarsPortImpl` (the adapter's `fetch_price_bars` inside
`rest_priority(BARS)`, 20 s cap; `ValueError` is `Unsupported(bad_request)`, decode / throttle or BARS-cap / auth /
other broker errors and timeouts `Transient(...)`, an unsupported answer `Unsupported(not_supported)`, an unknown
adapter `Unsupported(unknown_adapter)`; a supported answer is the port value itself, `PriceBarsResponse`),
`StopHandleReadPortImpl` (one `reread_stop_orders` per call; rows mapped to `HandleRead` OK with the row's send time
or the read's and the committed version label, TRANSIENT / NOT_FOUND rows; a pinned handle the list does not name is
`NOT_FOUND`), `CalendarPortImpl` (errors = None = closed), `AdapterHealthPortImpl` (SM4 STREAMING;
`RestBudgetStats.failing_at_every_priority`, None for an unmetered or unknown adapter), `PgHealthPortImpl` (an error
in the unreachable read counts as down), `StampPortImpl`, `PagerWrapper` (a `riskmgr.ports.Pager`, `__slots__ = ()`;
`page()` takes info / warning / high / critical and forwards to `EngineSession.page`; `log_mirror()` is `log.critical`
only), `LeaderCheckPortImpl` (lease valid and, with a reader, `engine_epoch` equal to the term's; any error False),
`NodeRecordPortImpl` (`ArmNodeRecordStore`; keyed by the raw `node_id`, carrying the writer-token form the md
bootstrap uses; refuses another node's record), `FailedSpoolPortImpl` (append-only file of
`XFS1` + length + CRC-32 framed JSON, fsync per append, file I/O in a thread; ack frames; replay = the latest record
per failure id not acked, oldest first; a CRC-bad frame, a bad header or a torn region is skipped by resyncing at
the next sound `XFS1` frame, a torn tail is cut back to the last good boundary before every write, one page per
distinct damage count; truncated once nothing readable is pending). `src/runtime/optional_settings.py`: `OptionalSettings`,
`parse_optional_settings` (`parse_md_settings`, `parse_arm_settings` only with the ARM flag, `XTNL_TICK_SYMBOLS` as
`adapter:SYMBOL` items, at most 8 per adapter, configured adapters only), `check_tick_symbols` (against the adapter's
loaded instrument map; an adapter whose map is not loaded keeps its symbols), `arm_channel_problems` (both `XTNL_ARM_`
channels); the market-data / ARM imports are lazy. `src/runtime/optional_md.py`: `build_md_services` (the S14-S18
wiring obligations: `MdHook`; `RefetchQueue` as the writer's `refetch` and the port's queue; `MdWriter` with
`boot=None`, `memory` and the demand filter; `SeriesStateBook`; `MdRehash`; `TickRefBook` fed by `on_closed_tick`;
one `CrossChecker` passed to `MdBars` as `crosscheck` with md-bars' fault sink; `MdBars` with the BARS port,
`StoreBarsReader`, the health and calendar ports, the hook's skew estimate, `trusts_partial_flag` False;
`MdIndicators` reading bars through the read port, `blind=MdBars.lane`, `each_cycle` = the lease sync; `DemandBook`
with the BARS cap per adapter and `host` = the engine; the bootstrap with the demand book as lease source, this node's
record and `previous_term_epoch`; `MdWriterService`; `MdPort` with `demand_book` / `indicators`; `on_released` → port
and engine, `on_resolved` → engine, `on_updated` → port), `FaultRelay` (series / store / indicator faults counted and
logged by code, the newest 256 kept until the S30 error budget is bound), `completion` (to
`EngineSession.complete_request`), `seed_tick_hook`. `src/runtime/optional.py`: `OptionalBootstrapRunner` (one per
term, progress kept across task restarts; TaskSpec `optional-bootstrap`, stamp 60 s, non-critical): settings (a bad
value pages, name only, and parks), the lockout scan in `asyncio.to_thread` under `exempt_wait` (a violation parks
with a page; an incomplete scan raises, paged on the third in a row), the `redis-md` clients with one PING inside 3 s
(a failure only logs: market data GAPPED), the graph, closer `redis-md`, the seeded tick hook and the per-market
interval hook, the instrument check of the tick-only symbols (unknown ones paged by name), the plan's market-data
handlers and tasks; ARM (flag on): both channels (else idle with one page), the ARM pool (`default_arm_pool`:
`LazyPgSource`, `min_size` 0, connect 3 s, `create_arm_source` with `arm_connection_init` = the engine init then
`SET ROLE xtnl_arm`; `arm_application_name` = `xtnl:<rev>:<node>:arm` cut to 63) reached with `SELECT 1` and retried
1 s doubling to 30 s within the term, `GuardedEngineDb` whose lost fence goes to `on_fence_lost`, closer `arm-pool`
with terminate, the role's privilege self-check (`privilege_problems`, paged once), `ensure_months` for the current
and next month of `arm_decisions` / `arm_decision_handles` (engine pool), the spool replay then the open
`arm_failures` read (unreadable: `may_advise` False, nothing added, retried), `assert_sinks` of the wired chain, then
the plan's ARM handlers and tasks; a stamping park loop under `set_exempt("bootstrap complete")` (or "optional
services parked") that also retries a pending ARM step. `make_optional_bootstrap(deps)`. Changed:
`src/runtime/session.py` (`set_md_subscription_hook` and the orchestrator's `on_price_subscription` forwarding to it,
`add_optional_task` (non-critical, idempotent through `has()`, never in DRAINING or after close), `has_task`,
`stopping`, `stop_event`, `pg_last_fenced_commit_ms`, `sweep_pg_unreachable`, `_TimedRiskSweep.dependency_down`),
`src/runtime/ports.py` (optional `LeaderPorts.last_fenced_commit_ms`), `src/runtime/pg_ports.py`
(`GuardedEngineDb.last_commit_ms`, wired into the term ports), `src/runtime/wiring.py`
(`_production_optional_bootstrap`, built only with `XTNL_MD_ENABLED=1`; the ARM pool factory only with the ARM flag;
the control pool reads the epoch, the previous term and this node's record; the spool beside the DLQ spill file;
StoneX `price_unfiltered` gives the hook's unfiltered adapters; an `EngineHooks.optional_bootstrap` still wins),
`deploy/env/engine.env.example` (`XTNL_TICK_SYMBOLS=`), `docs/IMPLEMENTATION_PLAN.md` (S31 ticked). Tests:
`tests/runtime/test_md_ports.py` (32), `tests/runtime/test_optional_settings.py` (12),
`tests/runtime/test_optional_bootstrap.py` (28: the graph binds the hooks, the closer and the D6 cross-checker;
`redis-md` down never blocks; an out-of-bounds `XTNL_ARM_FAIL_*` value pages (name only) and parks with the request
kinds and price path untouched; a planted violation parks; an incomplete scan raises, retries and pages after 3; the
real-tree scan passes; a pool failing once then succeeding runs the ARM in the same term (one pool, two attempts, the
plan's `arm` task added); unreadable open failures add nothing until readable; a missing push channel leaves the ARM
idle with one page; `application_name` matches `^xtnl:[0-9]+:`, also when cut to 63; `SET ROLE` in the init; the lazy
pool's `min_size` 0 / 3 s; flags off: no factory from `build_engine` and no task; through the wired `NodeController`
and a real `EngineSession` the node stays LEADER and keeps publishing health while the bootstrap hangs, and a raising
constructor leaves no `lost` / `init_failed` term; review fixes below). G green after the review fixes: pytest 4807
passed / 54 skipped, mypy strict clean (288 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) the market-data graph is its own module `src/runtime/optional_md.py` (not in the file list), so
`optional.py` holds only the bootstrap steps; (2) "handlers registered; tasks added" is a seam (`OptionalPlan`:
market-data and ARM tasks / handlers and the ARM sink chain) that is empty in S31: the task specs (`md-writer`,
`md-bars`, `md-indicators`, `arm`, `arm-tracker`, `arm-relay`, the `arm_status` writer) and the request handlers are
S32's, so with `XTNL_MD_ENABLED=1` today the hook fills its bounded rings and nothing drains them; the ARM graph
(tracker, BarClock, notices, relay, `FailedManager`, probes) is S32's too, so the bootstrap's own spool replay and
open-failures read gate only the ARM plan (S32's `FailedManager.start()` replays again before advice); (3) the sink
chain asserted in S31 is `default_sinks()` (pass-through notice sink) until S32 builds it with `ArmNotices.sink()`;
(4) `FaultRelay` holds faults until S32 binds the S30 `ErrorBudget` (`bind`); (5) `XTNL_TICK_SYMBOLS` →
`orchestrator.set_md_tick_symbols`, `md_schedule_symbols` and `CrossChecker.clear` on an accepted `arm_clear_failed`
are S32's (the checked symbols wait in `MdServices.extras["tick_symbols"]`); (6) the S14 receipt-gap `keepalive_ms` /
`note_frame` heartbeat feed is not wired (the orchestrator has no heartbeat hook): the gap detector stays off, as S14
built it by default; (7) `trusts_partial_flag` is False for every adapter until PR3; (8) a pinned stop handle the
list read does not name is `NOT_FOUND` ("not active at the read's send time", PR9); with S04 deviation 4 (StoneX lists
only `activestoplimitorders`), S39 must confirm attached children appear there before `XTNL_ARM_STOP_READS=1` on
StoneX; (9) this node's record and `previous_term_epoch` use the control pool (owner role), so they work with the ARM
off; without a control pool (tests) the bootstrap classifies without a record; (10) the privilege self-check pages a
difference but does not block the ARM; (11) the `/live-trading-server` skill was not needed: no file in
`src/adapters`, `src/pubsub`, `src/dto`, `src/integrity`, `src/oversee`, `src/marketdata`, `src/riskmgr` or
`src/armconfig` changed; no `.claude/` change is needed (the pending S11 proposal already covers `md_ports.py`);
(12) `DemandBook`'s stop-read rate is 0 with `XTNL_ARM_STOP_READS` off; with it on it is
`OptionalBootstrapRunner.stop_read_cost`, the armed-handle cost S32's tracker binds (0 until S32).
Review fixes (2026-11-16): (a) FAILED spool: `scan_frames` resyncs at the next sound `XFS1` frame after a CRC-bad,
bad-header or torn region (a torn append mid-file no longer hides later records and acks), every write first cuts a
torn tail back to the last good boundary, the directory is fsynced when the file is created, `acknowledge` empties
the file once nothing readable is pending, and damage pages once per distinct damage count (not per replay / ARM
retry); (b) `NodeRecordPortImpl` stores under the raw `node_id` and carries the writer-token form (`record_node`), so
a hostname such as `ip-10-0-1-5.eu-west-2.compute.internal` saves its §A7 record; (c) a wrong sink set
(`SinkSetError`) leaves the ARM idle (`arm_sinks`) with one "ARM sink set refused" page, never retried; (d) the
bootstrap re-checks `stopping` after every await and drops what it built when `register_closer` returns False
(clients closed / pool terminated); (e) the tick-only symbol check pages once per adapter and re-runs from the park
loop while a configured adapter's instrument map is not loaded; (f) `ensure_months` is typed (`MonthEnsurer`), a
missing port or a failure pages once and never blocks; (g) unreadable grants page "grants unreadable" and never
block; the spool replay has its own try and log line. Tests added: torn mid-file / torn tail / page once /
`scan_frames` boundary / hostname node record (md_ports); `ensure_months` calls for both tables and both months,
partition failure and missing port, unreadable grants, wrong sink set, node_id `a.b:c`, unknown tick symbols after
the map loads, the stop-read wiring, closing during the previous-term read, and a subprocess check that flags off
import no `src.marketdata` / `src.riskmgr` / optional runtime module (bootstrap).

* **C:** `src/runtime/md_ports.py` (name-ban checked): `BrokerBarsPortImpl` (`orch.runtime(adapter_id).adapter
  .fetch_price_bars(...)` inside `rest_priority(RestPriority.BARS)`, copying `reread_cash_flow` at `session.py:652-662`;
  converts `BrokerBar` to port bars; errors become `TRANSIENT` / `UNSUPPORTED` values); `StopHandleReadPortImpl` (one
  `reread_stop_orders` per adapter per anchor, send time from the stamp, committed version label; R11-M20);
  `CalendarPortImpl` (`MarketCalendar.state()`, None = closed, IN-10); `AdapterHealthPortImpl` (SM4 state and REST
  failures per priority, R11-M10); `PgHealthPortImpl` (R11-M11); `StampPortImpl`; `PagerWrapper` (`__slots__`; `page()` for ordinary pages, `log_mirror()` = `log.critical` only, for
  the FAILED ping);
  `NodeRecordPortImpl`; `FailedSpoolPortImpl`. `src/runtime/optional.py`: the `optional-bootstrap` body (stamp 60 s):
  full settings parse with bounds, a bad value pages and parks (RT-M13); lockout scan in `asyncio.to_thread` under
  `ctx.exempt_wait`, a violation parks with a page, an incomplete scan raises and pages after 3 in a row; `redis-md`
  clients (connect 3 s) and the ARM pool (copy of `LazyPgSource`, `min_size=0`, `connect_timeout_s=3.0`, wrapped in
  `GuardedEngineDb`, `application_name = xtnl:<rev>:<node>:arm` truncated to 63 characters so the successor's terminate
  reaches it, RT-M11; `SET ROLE xtnl_arm` in its init); both ARM channels required for the ARM; `ensure_months` for the
  current and next month of both partitioned tables; open `arm_failures` read before the BarClock (unreadable = advise
  nothing); handlers registered; tasks added idempotently through `has()`; the late-bound `session._md_hook` set; extra
  closers registered; `ARM_SINKS` asserted by the scanner; then a stamping park loop under `set_exempt("bootstrap
  complete")`. `src/runtime/optional_settings.py` completes the parse, including `XTNL_TICK_SYMBOLS`.
* **M:** `src/runtime/session.py` / `src/runtime/wiring.py` (`SessionContext.optional_bootstrap` set from `build_engine`).
* **D6 wiring (from S17b):** one `TickRefBook` per term, `MdWriter.on_closed_tick(book.note)`, one `CrossChecker`
  passed to `MdBars` as `crosscheck` (same fault sink as `md-bars`); the S30 clear of a series' FAILED
  (`arm_clear_failed`) must also call `CrossChecker.clear(series)`, or `blocked()` stays True with no FAILED behind it
  and the fault never re-arms.
* **Tests:** a pool build that fails once then succeeds ends with the ARM running in the same term; the node keeps
  publishing health while the bootstrap hangs; a raising constructor leaves no abnormal `leader_terms` row; an
  out-of-bounds `XTNL_ARM_FAIL_*` value pages and ingestion is unaffected; a planted violation parks; an incomplete
  scan retries; `application_name` matches `^xtnl:[0-9]+:`; flags off → no task.
* **Resolves:** RT-M11, RT-M13, IN-10, R11-M20 (port), R11-m (pager wrapper).

### S32 RT-2: task wiring, DRAINING, handlers, metrics, dead-man, `arm_status`

Status: done 2026-11-16 (both flags stay off by default; with `XTNL_MD_ENABLED=1` the market-data tasks run, with
`XTNL_ARM_ENABLED=1` the FAILED machinery and the ARM relay run; nothing is armed or decided yet, see deviation 1).
Built: `src/runtime/optional_plan.py` (`production_plan(environ)`, the `OptionalPlan` `build_engine` now hands to the
bootstrap; `OptionalRuntime`, the per-term state kept on `OptionalServices.runtime` and bound as the session's
`OptionalView`): `loop_spec` (non-critical tasks around `body(stop, stamp)`, stamp ages `md-writer` 5 s, `md-bars`
30 s, `md-indicators` 10 s with `on_quarantine` = `MdIndicators.on_quarantine`, `arm-status` 5 s, `arm` 5 s,
`arm-relay` 60 s self-stamping; every loop watches `session.stop_event`, a return outside DRAINING is a death, one in
DRAINING waits exempt to be cancelled); `CircuitRelay` (every optional task's `on_circuit_reopen`, filed into
`ErrorBudget.on_circuit_reopen` once the ARM graph binds it, counted and logged before); the handlers (S10 registry,
non-blocking): `md_backfill` -> `MdBars.request_backfill` (payload `{"v": 1, "from_bucket_ms", "to_bucket_ms"}`,
`refused:bad_payload` otherwise), `md_rebuild` -> `MdIndicators.md_rebuild`, `md_writer_reset` / `md_rehash` ->
`MdWriterService.submit` (`refused:queue_full`), `arm_clear_failed` -> `FailedManager.submit_clear`; each answers
`Accepted(owner)` or refuses at once with the component's code; the `arm-status` loop saves this node's §A7 record
every 5 s (the S16 obligation) and, once the ARM runs, upserts the `arm_status` heartbeat row on the ARM pool
(codes and counts: budget snapshot, pending FAILED records, tick errors; `running` False while nothing decides); the
ARM graph (built once, no I/O): `ErrorBudget` (calendar / health / PostgreSQL-health ports, SM10 maintenance) bound to
the market-data `FaultRelay` and the circuits, `FailedManager` (stores on the ARM pool and the engine pool, the
`PgArmFailedClear` statement, `ClearProbes` with `TrialBootstrapProbe` over `md-writer`, `IndicatorStateProbe` and the
open ARM circuits, `ScopeClearHooks`: SERIES -> `CrossChecker.clear` + SM12 `CLEAR`, ARM -> the post-clear
bootstrap), `ArmRelay` over `PgArmOutboxStore` on the engine pool with the `XTNL_ARM_` channels (`build_arm_channels`
over the term's HTTP client) and `SendTimeRenderer` over the read port and keyring (the engine-pool stores use the
term pool's `PgLeaderPorts.engine_db`; production ports without it raise, so the ARM step retries and pages; only the
in-memory ports fall back to the ARM pool); the market-data ARM-global flag bound to `FailedManager.summary()`'s
`global_failed` (the ARM scope FAILED or the open episodes unreadable, as the dead-man and the metric report it) and
read by `md-bars`, `md-indicators` and the read port: while it holds, `md-bars` makes no live, re-fetch or gap-heal
read and writes only the lane buckets of an accepted `md_backfill`, and `md-indicators` runs only `md_rebuild` jobs
(other recomputes and new registrations stay queued until the clear), so the §B13 repairs are applied during FAILED;
the `arm` loop (FAILED `start()` once per term before anything else, `tick()` retrying an unreadable open-episode
read, then once a second: `observe_md` (writer booted, no bootstrap wanted, no new flush error, while an armed
position's market is OPEN: none before S33, so `redis-md` down time never counts yet), `observe_pool` (a `SELECT 1`
within 3 s), `tick()`; a lost fence raises; on stop `drain()` within 2 s, spool only); the dead-man fields
(`arm_failed` from `FailedManager.summary()`, an unreadable one reported as one global FAILED, fail closed;
`arm_oldest_undelivered_notification_ms` from the relay); the metric
series (`xtnl_md_series`, `xtnl_md_indicators`, `xtnl_md_aof_wait_timeouts_total`, `xtnl_md_events_total`,
`xtnl_arm_failed_scopes`, `xtnl_arm_failed_global`, `xtnl_arm_oldest_undelivered_notice_age_seconds`,
`xtnl_arm_events_total`: counts and states only, leader only, absent with the flags off); `registry_tokens()` (PR13,
`REGISTRY.registry_tokens()`). Changed: `src/runtime/session.py` (`OptionalView` with `arm_active`,
`set_optional_view`, `degraded_tasks`, `http_client`; `_DeadmanView`'s three ARM getters read the view, a raising view
reports None except `arm_failed`, which reports one global FAILED while the ARM runs; `metrics_samples` appends the
view's series, contained, and on a raise with the ARM running emits `xtnl_arm_failed_scopes` / `_global` = 1;
`MarketCalendar(ignored=orchestrator.is_md_only)` and
`_on_transition` skips the clean mark and the open RESYNCING for a tick-only symbol), `src/calendar/market.py`
(`ignored` predicate: such symbols are left out of `deciding_sessions` and its fallback, `latest_open_ms`, the
PRE_OPEN and maintenance-ended readiness checks; None = unchanged; a raising predicate ignores nothing),
`src/runtime/optional.py` (`OptionalServices.runtime`; the kept `XTNL_TICK_SYMBOLS` go to
`orchestrator.set_md_tick_symbols` and `session.md_schedule_symbols`, with a schedule refresh when they change),
`src/runtime/optional_md.py` (`LateFlag`: the late-bound ARM-global FAILED flag of `md-bars`, `md-indicators` and the
read port, `MdServices.global_failed`), `src/marketdata/brokerbars.py` (`MdBars` during the ARM-global FAILED: a
request-only lane pass, `BarsCounters.failed_passes`), `src/marketdata/indicators/engine.py` (`MdIndicators`
`global_failed`: only `md_rebuild` jobs run and `register()` computes nothing while it holds,
`IndicatorCounters.failed_cycles`), `src/runtime/pg_ports.py` (`PgLeaderPorts.engine_db`, the term pool's fenced
`EngineDb` for the engine-pool stores), `src/runtime/wiring.py` (the production plan in
`_production_optional_bootstrap`; `probe.registry_tokens` set only with `XTNL_MD_ENABLED=1`),
`src/observability/metrics.py` (`CONTRACT` += the eight series), `deploy/observability/metrics.md` and
`gen_alerts.py` (the rows, alert `XtnlArmFailed` / `xtnl-arm-failed` on `max(xtnl_arm_failed_scopes) > 0` for 1 min,
critical, the §B14 off-node backstop; the generated files regenerated), `docs/runbooks/off-node-alerts.md`
(`XtnlArmFailed` section), `src/db/operator_requests.py` (payload doc), `docs/IMPLEMENTATION_PLAN.md` (S32 ticked).
Tests: `tests/runtime/test_optional_plan.py` (17: the market-data tasks, stamp ages, quarantine hook and handlers, the
writer's term-start bootstrap; md requests through the operator loop: bad payloads and an undemanded series refused
at once, `md_writer_reset` applied and `md_rehash` moot by `md-writer`; payload parsing (a missing `v`, `"v": true` and
`1.0` refused); tick-only symbols reach the
orchestrator and the schedule and decide nothing (no open RESYNCING, no clean mark); the calendar's `ignored`
predicate (market, latest open, PRE_OPEN readiness, a raising predicate); the ARM graph binds the fault relay, the
circuits and the ARM-global flag (also True while the open episodes are unreadable), a circuit reopening gives an
in-memory global FAILED seen by the dead-man view, the metric series and the status row, every series in the contract
and rendered, `md-indicators` and `md-bars` then write nothing, a raising FAILED summary or view is reported as one
global FAILED (dead-man and metric); a `reset_task` filed behind a hanging `arm_clear_failed` probe applied within one
`operator_poll_s`; the loop bodies alone: `redis-md` down past `fail_md_down_s` with every market OPEN and nothing
armed gives no FAILED (with an armed adapter it does), `_observe`'s ARM-pool `SELECT 1` raising or timing out feeds
`observe_pool(arm_ok=False)` and the md usable / flush-error logic, the `arm-status` loop saves the node record, writes
the row once the graph exists, stamps through its waits, writes nothing after the stop and raises on a lost fence, the
`arm` loop runs `start()` once while the open episodes are unreadable, `default_arm_channels` registers the `arm-http`
closer only without a term client; `redis-md` down / hung / raising at start-up with both flags on: LEADER, ingestion
commit latency and the RiskSweep pass age within bounds of a baseline term with both flags off, no series LIVE and a
decision read `DataUnavailable`, and DRAINING with an ARM query in flight and both closers hung (the ARM pool's and
`redis-md`'s) ends inside 10 s; the registry in the health record only with the MD flag; `build_engine` hands the
production plan to the bootstrap), `tests/marketdata/test_brokerbars.py` (+1: an `md_backfill` is read, written and
completed during the ARM-global FAILED while no live read is made), `tests/marketdata/indicators/test_engine.py` (+1:
during the ARM-global FAILED no recompute or registration writes, `md_rebuild` does; the queued work runs after the
clear), `tests/runtime/test_telemetry.py` (+1: the new series
render under the contract, a stray label is dropped), `tests/deadman_receiver/test_arm_optional_fields.py` (the
session view test now covers no view, a raising view (fail closed for `arm_failed` while the ARM runs) and a wired
one). G green: pytest 4820 passed / 54 skipped, mypy strict clean (289 files), `verify_execution.py --strict` 11 / 11.
Review fixes (2026-11-16, after the S32 review): G green again: pytest 4827 passed / 54 skipped, mypy strict clean (289
files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) **the BarClock (`arm` decisions) and the `arm-tracker` are not wired**: the BarClock needs the S21
`EvaluationPort` (waiting for the user's §9.2 / C1 confirmation) and the tracker needs S33's `AcceptedConfigPort`
plus the engine-pool `TrackerSnapshotPort` and `BarGridPort`, which no stage has built yet (the tracker's docstring
places the snapshot port in `md_ports.py`; S31 did not build it): S33 (or the S21 join) adds them through
`OptionalRuntime.arm_tasks` / `arm_handlers`. Until then nothing is armed or decided, `arm_disarm` / `arm_repin` /
`arm_rearm` have no handler (their requests stay pending, S10), `arm_overdue_positions` is not reported, the
`arm_status` row says `running` False, the stop-read cost (`stop_read_cost`) stays 0 and `ArmNotices` (the
`FAILED_CLEARED` notice and the notice sink of the chain) is not built (`cleared_notice` None); (2) the `arm` task
runs the FAILED manager only (its loop is where the BarClock joins); `observe_md`'s "market OPEN" is computed over
`OptionalRuntime.armed_adapter_ids()`, empty until the tracker (S33) fills it, so `redis-md` down time does not count
toward `fail_md_down_s` yet (design §B14: only while an armed position's market is OPEN); (3) the series re-read, fresh-bar, span-write and version probe ports
stay unwired (`probe_not_wired`, fail closed), as does the ADAPTER clear hook; (4) the `arm_status` writer is its own
task `arm-status`, added with the market-data tasks so the node record is saved with the ARM off; its heartbeat row
goes on the ARM pool only once the ARM runs; (5) the S05 open point 6 is closed by keeping tick-only symbols out
(the calendar `ignored` predicate plus the session's transition hook), not by accepting it; pre-warm symbols are not
in `md_schedule_symbols` (no tracker yet); a tick-only schedule stored in an earlier term with the flag now off
behaves like any previously held symbol's (as before S32); (6) the ARM relay's channels use the environment snapshot
(no secrets-source rotation yet: deploy work of S36 / S37) and the drain flag is always False (`arm_controls` is
S34's); (7) metrics: eight series from the §C2 list (states, counters by component, FAILED, notice age); the rest
of the list is S37's; (8) `docs/runbooks/off-node-alerts.md` gained the `XtnlArmFailed` section (the deploy test
requires one per alert); (9) the review fixes changed `src/marketdata/brokerbars.py` and
`src/marketdata/indicators/engine.py` (the §B14 ARM-global FAILED effect on `md-bars` / `md-indicators` with the §B13
repairs); the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only), so the
read-only rules were followed by hand (no broker call or send path added; no file in `src/adapters`, `src/pubsub`,
`src/dto`, `src/integrity`, `src/oversee`, `src/riskmgr` or `src/armconfig` changed); no `.claude/` change is needed
and no new `verify_execution.py` check (the plan lists none).

* **M / C:** tasks `md-writer` (5 s), `md-bars` (30 s), `md-indicators` (10 s, `on_quarantine`), `arm` (5 s),
  `arm-tracker` (10 s), `arm-relay` (60 s, monitored and self-stamping like `session.py:1231`), the `arm_status` writer
  (5 s); all watch `session._stop` and start no new write after it is set; the ARM pool and `redis-md` clients close in
  parallel in `aclose` and leftovers are terminated; operator handlers (S10 registry, non-blocking) enqueue to their
  owning task and return `Accepted`: `arm_disarm` / `arm_repin` / `arm_rearm` → `arm-tracker` (S26), `md_backfill` →
  `md-bars` (S17), `md_rebuild` → `md-indicators` (S18), `md_writer_reset` / `md_rehash` → `md-writer` (S16),
  `arm_clear_failed` → the probe runner of S30 (probe reads go to `md-bars`);
  `on_circuit_reopen` feeds the ARM-global budget; dead-man optional fields from memory; health `registry` from the
  riskmgr and marketdata registries; `XTNL_TICK_SYMBOLS` → `orchestrator.set_md_tick_symbols`; `md_schedule_symbols()`;
  `metrics_samples` series with `deploy/observability/metrics.md` entries and `gen_alerts.py` (`arm_failed_scopes`
  off-node alert).
* **Tests:** DRAINING under 10 s with `redis-md` hung and an ARM query in flight; with both flags on and `redis-md` down,
  hung or raising, the ingestion rate and RiskSweep pass age are unchanged; `redis-md` down at start-up: the engine
  reaches LEADER, ingestion and Oversee run, market data GAPPED (design §C3); the market-data hook raising on every tick;
  a `reset_task` filed behind a hanging `arm_clear_failed` probe is applied within one `operator_poll_s`; the registry
  appears in the health record; `tests/runtime/test_telemetry.py` CONTRACT.

### S33 CFG-2: Supabase client, installer, admission

Status: done 2026-11-16, review fixes applied the same day (see "Review fixes" after the deviations; two items wait
for the user's decision: R1 and R2) (both flags stay off by default; with `XTNL_ARM_ENABLED=1` and
`XTNL_ARM_CONFIG_DSN` set the `arm-config` task reads, checks and installs accepted documents; nothing is armed yet,
see deviation 1). Built:
`src/armconfig/client.py` (`parse_source_settings`: `XTNL_ARM_CONFIG_DSN` (secret, out of `repr`) and
`XTNL_ARM_CONFIG_CA_FILE` (the pinned CA), problems name the variable only; `tls_context`: `PROTOCOL_TLS_CLIENT`,
CERT_REQUIRED, hostname check, TLS 1.2+, only the pinned CA, never `sslmode=require`; `SupabaseConfigSource`: one
connection per read through the injected driver `connect` (asyncpg's in production), connect 5 s, statement 5 s,
`REPEATABLE READ READ ONLY` transaction always rolled back, `SET LOCAL search_path = pg_catalog` and every relation
schema-qualified; `self_check_problems` on every connection: the four `arm` tables present, SELECT and no other
privilege (`has_table_privilege`), RLS enabled and forced (`relforcerowsecurity`), no other `arm` relation granting
more, `rolbypassrls` / `rolinherit` / `rolsuper` false, failing closed with `SelfCheckError` (fixed codes); bounded
read: `LIMIT` = 1001 rows per table, a server-side per-row byte cap (a row over it comes without its text), a 4 MiB
total cap, `row_to_json` text parsed with `Decimal`, no NaN / Infinity and a depth limit, in `asyncio.to_thread`
under a 5 s timeout together with `parse_document`; every driver / network / TLS error re-raised `from None` as
`ConfigSourceError(stage, ClassName[(SQLSTATE)])`, never the message), `src/armconfig/installer.py`
(`ConfigInstaller`, the `arm-config` task and the tracker's `AcceptedConfigPort`: every 120 s the cache and the
latest `arm_accepted` row (ARM pool), the cache restored once the known ids are loaded and **its hash compared with
the latest accepted hash** (else PAUSED `config_hash`, the cached document kept for arming; no accepted row: IDLE,
`accepted()` None, one page); the Supabase read (unreachable for 15 min pages once; a failed self-check pages at
once); the seen-row chain (`TAMPERED` pages critical at once and is audited, nothing installs, not even after a
local accept of the tampered hash; `schema_ahead` pages); `validate_document` with the seen rows and now (invalid:
page by `table:id:hash`, audited); pending rows page after 1 day; the candidate is the read when it hashes to the
latest accepted hash, else its rows with `set_at_ms <= accepted_at_ms` when those do (rows added after the accept
stay pending); **install at `T + post_grace`** (first 15m bar end `T` whose `T + 60 s` is after now; the accepted
hash compared again at install), then the fenced cache save with the candidate's rows added to the seen chain, an
`installed` audit row and the `config_hash` pause ended; a new accept keeps the installed document until its bar
boundary; audit rows once per distinct event; a lost fence raises, every other error is counted and retried; pages,
logs and audit rows carry codes, counts and `table:id:hash` refs only), `src/armconfig/admission.py` (`admission(doc,
loads, limits, at_ms=)`: enabled assignments in force or future-dated, expanded per bar-capable adapter to the
explicit symbol or the pre-warm set (at most 8); per symbol the strategy's and guards' BROKER series (long and short
trigger sides, history and indicator needs), indicators and the four TICK cross-check series; refusals with the
numbers: decision symbols over the per-adapter ceiling (8), §A5 bytes (BROKER 1 year, TICK 1m 7 days, TICK 15m 90
days, 270 / 300 bytes a member, ticks and revisions per symbol, `md:events`) over `maxmemory x target` (512 MiB),
BARS reads per bar end (series x 2 + PR9 stop reads: one list read per trading account with armed handles per anchor,
PRE / POST / refresh, never per handle) over the cap for the 60 s read window, and the REOPEN burst over the cap
for 120 s; an adapter without bars or without a BARS cap is noted, not refused), `deploy/supabase/arm_schema.sql`
(applied by hand, idempotent: schema `arm` with `schema_version` 1, `strategies`, `assignments`, `route_policy` in
the S25 raw row contract (`set_at_ms`, `effective_from_ms` as integer ms, `"session_user"` / `"current_user"`
quoted), server CHECKs (code / kind form, `params` an object of at most 4 KB, `edge_code IS NULL`, channel sets),
the append-only guards (BEFORE INSERT sets `set_at_ms`, `session_user`, `current_user`, bounds `effective_from_ms`
to now − 1 min .. now + 366 days and caps 1000 rows; UPDATE / DELETE / TRUNCATE refused), `xtnl_arm_reader` (LOGIN
NOINHERIT NOBYPASSRLS CONNECTION LIMIT 2, read-only default, 5 s statement timeout, `search_path = pg_catalog`,
SELECT only) and `xtnl_arm_writer` (NOLOGIN, SELECT and INSERT; the owner `SET ROLE`s to it), RLS enabled and forced
with read and append policies, everything revoked from PUBLIC, `anon`, `authenticated`, `service_role` including
default privileges). Changed: `src/runtime/optional_plan.py` (`ARM_CONFIG_TASK` `arm-config`, stamp 120 s, added to
the ARM tasks when a source is configured; `default_config_source(environ)` (asyncpg's `connect` imported lazily);
`ProductionPlan.config_source`; `OptionalRuntime.known_ids()` (adapters, their loaded instruments and accounts,
routes `adapter:account` and `*`; None while an instrument map is not loaded, so nothing is validated early),
`accepted_config()`, `config_idle_reason`; without a DSN or with a bad setting the ARM configuration is idle with
one page and no task; the `arm_status` row gets `paused` / `pause_reason` / `config_doc_hash` and the `config_*`
counters), `docs/IMPLEMENTATION_PLAN.md` (S33 ticked). Tests: `tests/armconfig/test_client.py` (22: settings and
the DSN never in `repr` or a problem; the TLS context; every self-check failure code; decoding bounds (row count,
row and total bytes, depth, NaN, Decimal numbers); the transaction order, connect arguments and schema-qualified
reads; a failed self-check still rolls back and closes and reads nothing; planted driver errors sanitised; a hanging
close terminated), `tests/armconfig/test_installer.py` (18: idle with one page; install only at `T + post_grace`;
restart restores a matching cache; cache hash not the latest accepted pauses until the new install; a new accept
waits for its bar boundary; rows added after the accept stay pending while the accepted subset installs; pending
pages after a day; **a tampered row pages and keeps the accepted document** (changed and vanished; a local accept of
it never installs); **schema ahead**; invalid rows paged by ref without their text; known ids not loaded; **unreachable
for 15 min pages once**; self-check failure; a lost fence raises; **a planted DSN, host and password appear in no
log record, page, audit row or counter** (through the real `SupabaseConfigSource` with a failing driver); the run
loop), `tests/armconfig/test_admission.py` (8), `tests/deploy/test_supabase_arm_schema.py` (6: columns equal
`validate.COLUMNS`, guards, schema version, forced RLS, reader SELECT only, hardening, no secret) and
`tests/runtime/test_optional_plan.py` (+5: no DSN = idle with one page and no task; the task, the installer as
`accepted_config()` and the status row; `known_ids`; `default_config_source` never shows the DSN; a planted DSN in a
bad setting reaches no page, status row or log). `deploy/supabase/arm_schema.sql` and the client's real statements
were also run once against a disposable PostgreSQL 17 container (with `anon` / `authenticated` / `service_role`
created): applied twice (idempotent), writer inserts got `set_at_ms` / `session_user` / `current_user` from the guard,
UPDATE / DELETE as the writer refused (42501), a backdated `effective_from_ms` refused (P0001), the reader's insert
refused (25006), the self-check passed and the read validated; with `NO FORCE ROW LEVEL SECURITY` and an INSERT
grant the self-check failed closed (`privilege_insert:route_policy`, `rls_not_forced:strategies`). G green: pytest
4886 passed / 54 skipped, mypy strict clean (292 files), `verify_execution.py --strict` 11 / 11.
Deviations: (1) **the `arm-tracker` is still not wired**: it needs the engine-pool `TrackerSnapshotPort` and
`BarGridPort` (S31 / S32 built neither); S33 provides its `AcceptedConfigPort` (`OptionalRuntime.accepted_config()`,
the installer), so `armed_adapter_ids()` stays empty, `arm_disarm` / `arm_repin` / `arm_rearm` stay pending and the
stop-read cost stays 0 until a later stage (or the S21 join) builds those ports; (2) the `arm-config` task spec lives
in `src/runtime/optional_plan.py` with the other ARM task specs (S32's split), not in `src/runtime/optional.py`;
(3) the driver's `connect` is injected (the scoped package cannot import `importlib`, and asyncpg is untyped; the
engine imports it lazily as `src/db/pg.py` does); the client is still the only Supabase code and lives in
`src/armconfig`; (4) "idle (one notice, one page)" is one page only: the ARM notices (`ArmNotices`) are not built
yet (S32 deviation 1), so the idle notice, the automatic-install notice and the pinned-CA expiry alert (S37) are not
sent; `SupabaseConfigSource` builds the pinned CA's context once per term; (5) Q12 automatic mode is not built: an
`arm_accepted` row with mode `automatic` is treated as a manual accept of its hash (the default, "after local
accept"); (6) "install at `T + post_grace`" is read as the first 15m bar end `T` whose `T + post_grace` (60 s) is
after now, so a document never changes mid-bar; at start-up a cache whose hash equals the latest accepted hash is
restored at once (it was installed before); (7) the installer reads the accepted document back from Supabase:
when rows were added after the accept, the accepted subset is rebuilt from the rows set at or before a cut-off and
installs only if it hashes to the accepted hash, else the ARM stays paused (`config_hash`) until a matching read.
**Two clocks:** `set_at_ms` is the Supabase server's clock (the guard) and `accepted_at_ms` the engine host's, so
the cut-offs tried are `accepted_at_ms` and each distinct `set_at_ms` within 5 min of it (`subset_skew_ms`, at most
64 cut-offs, nearest first); skew only changes which cut-off finds the rows, never what installs (the hash decides).
Skew beyond 5 min keeps the ARM paused (safe, a liveness cost); storing the accepted row refs (or the max row id per
table) in `arm_accepted` at `arm config accept` (S34) would remove the clock entirely and is the suggested S34
follow-up; (8) the admission check counts PR9 stop reads as one list read per
trading account with armed handles per anchor (PRE, POST, the POST refresh; design PR9 "never one call per handle"),
reporting the armed handles beside it, rather than the plan's literal "armed handles x reads per bar"; the read
window is `post_grace` (60 s) and the REOPEN window 120 s (`AdmissionLimits`; the design gives no number); its
caller (`arm config accept`, S34) supplies the per-adapter loads; (9) local aliases of accounts do not exist in the
engine yet, so `KnownIds.accounts` maps each account id to itself; (10) the `XTNL_ARM_CONFIG_*` settings are not added
to `deploy/env/engine.env.example` (the env file's ARM settings are S36's; the DSN is a secret for the User
environment / secrets source); (11) the `/live-trading-server` skill could not be invoked by the build agent
(user-invocable only), so the read-only rules were followed by hand: no broker call, sender or order path was added,
the new `src/armconfig` modules pass the real-tree lockout scan (`ssl` and the injected driver only), and no file in
`src/adapters`, `src/pubsub`, `src/dto`, `src/integrity`, `src/oversee`, `src/riskmgr` or `src/marketdata`
changed; no `.claude/` change and no new `verify_execution.py` check are needed (the plan lists none for S33).

Review fixes (2026-11-16, after the S33 review; the `/live-trading-server` skill again not invoked by the agent, no
file in `src/adapters`, `src/pubsub`, `src/dto`, `src/integrity`, `src/oversee` or `src/marketdata` changed, no
broker call, sender or order path added; `src/riskmgr/barclock.py` changed, see 2):
(1) **an invalid row could block every later install for good** (append-only: it can never be deleted). Fixed twice:
`validate.py` now checks known ids (symbol, adapter, account, route) on a **pending** row only while it is live at
`at_ms`, exactly as for seen rows, so a typo that a later row tombstoned or superseded no longer invalidates the
document; and when the full read is INVALID the installer still builds its accepted subset, validates it and
installs it, so rows added after the accept, valid or not, can never block the accepted document. **R1 (user
decision):** this relaxes "every pending row is checked against the known ids" to "every live pending row"; a
non-live typo is no longer paged. Also open for the user: `route_policy` has no tombstone (no `enabled` column),
so a pending route policy naming an unknown route stays live (it is the latest row for its route) and keeps every
later accept that includes it invalid; only fix (b) protects the accepted document. Options: an `enabled` column in
schema version 2, or treat an unknown-route policy as inert (a warning, not a refusal).
(2) **the config-hash pause reached nothing that decides.** `BarClock` now takes an injected `config_pause` port
(`Callable[[], str | None]`; the installer's `pause_reason`): while it returns a reason the pass is paused exactly
as for the operator pause (`Hold(paused)`, no stop reads, the bars spanned `paused` on resume), it holds even while
the controls are unreadable, a port that raises counts as paused (fail closed), and `config_paused_passes` counts it
(no 6 h page: the installer pages). **Hand-off to the S21 join** (which builds the BarClock in production): pass
`config_pause=installer.pause_reason`, and mark config-paused positions `held` in the `ErrorBudget.observe_frontier`
views (the budget's own rule for paused positions, so no new budget input: §B13 "paused positions count neither ...
nor toward the D7 error budget"); the ARM notices (`ArmNotices`, not built) must also be suppressed during either
pause. **Hand-off to S34:** `status_row`'s `paused` is the config pause only; S34 ORs in the `arm_controls` pause.
**R2 (user decision):** the design leaves open whether D7 series counters (bar faults, unresolved buckets) also stop
during a pause; this build keeps them counting (they are market-data health, not decisions).
(3) the Supabase SQL revoked the API roles' access before the tables, identity sequences and guard functions existed,
so project-wide default grants survived the first apply: the `anon` / `authenticated` / `service_role` loop now runs
at the end, after every object exists. Applying the file as a Supabase-like owner (not a superuser) also showed
that the unconditional `ALTER ROLE ... NOSUPERUSER NOBYPASSRLS NOREPLICATION` fails for a non-superuser on PostgreSQL
16+ (it names those attributes): the re-run now changes only an attribute that is wrong, and the `schema_version`
upsert lifts FORCE RLS for itself inside the transaction (it no longer depends on the owner having BYPASSRLS).
(4) the guard now draws `id` from the column's identity sequence (`nextval`, the writer has USAGE on the sequences,
never UPDATE), so `OVERRIDING SYSTEM VALUE` cannot set a zero, negative or far-ahead id.
(5) the self-check also reads `has_any_column_privilege` (INSERT / UPDATE / REFERENCES: a column-level grant failed
to show) and the reader's role memberships (`pg_auth_members`; any membership fails with `role_member`: NOINHERIT
does not stop `SET ROLE`, and a PostgreSQL 16+ grant `WITH INHERIT TRUE` overrides it); a missing column fails
closed.
(6) the installer pages once (`known_ids`, "ARM configuration waiting for instruments") when the known ids have not
loaded for 15 min (`known_page_ms`; for example an adapter stuck in AUTH_FAILED), counts `config_known_waits`, and
reports its state as `config_state_<starting|idle|paused|installed>` flags in the `arm_status` counters.
(7) a pinned CA that cannot be loaded (`ConfigSourceError` stage `tls_ca`) pages at once ("ARM configuration CA
unusable", a configuration fault, not an outage).
(8) deviation 7 rewritten for the two clocks (above).
(9) tests: `tests/armconfig/test_installer.py` (+4: a typo tombstoned after the accept never blocks a later install;
a live invalid row added after the accept still lets the accepted subset install (and an accept of the invalid
document never installs); the accepted subset found across clock skew; the CA page; the known-ids page and state
flags; the pending counter grows each poll and `assignments:3` stays out of the seen chain), `test_validate.py`
(+1: a pending typo refused only while live), `test_client.py` (+8 self-check cases: column grants, a row without
the column checks, role memberships; the statements), `tests/riskmgr/test_barclock.py` (+2: the config pause holds
decisions like the operator pause, even with unreadable controls; a raising port counts as paused),
`tests/deploy/test_supabase_arm_schema.py` (+4: the API revoke after every CREATE; the id from the sequence; the
upsert inside NO FORCE / FORCE; no unconditional superuser attributes) and the new
`tests/deploy/test_supabase_arm_schema_pg.py` (`integration`, ORDER_SERVER_TEST_DSN as a superuser: applies the file
twice as a Supabase-like owner (NOSUPERUSER NOBYPASSRLS CREATEROLE, global default grants to the API roles) and
checks nothing in `arm` is left to them; the writer's inserts (an `OVERRIDING SYSTEM VALUE` id of -5 replaced),
UPDATE / DELETE / a backdated `effective_from_ms` refused; the real `SupabaseConfigSource` self-check and read as
the reader; a column grant and a membership fail closed). It passed twice against a throwaway `postgres:17`
container. `certifi` (used by the TLS tests) is now declared in the `[dev]` extras. G green after the fixes: pytest
4905 passed / 55 skipped, mypy strict clean (292 files), `verify_execution.py --strict` 11 / 11.

* **C:** `src/armconfig/client.py` (asyncpg with an `SSLContext`: CERT_REQUIRED, hostname check, TLS 1.2+, pinned CA;
  `search_path = pg_catalog`; client `LIMIT` and byte cap; JSON parsed with Decimal and a depth limit in a thread with a
  timeout; connect self-check: `has_table_privilege` SELECT only, `pg_class.relforcerowsecurity`, `rolbypassrls = false`,
  `rolinherit = false`, failing closed with a page, R11-m), `src/armconfig/installer.py` (poll, pending and tampered
  pages, cached hash equals the latest `arm_accepted` hash or pause, install at `T + post_grace`, Q12),
  `src/armconfig/admission.py` (series count, bytes from the §A5 formula, BARS reads per bar end including armed handles
  × reads per bar and the REOPEN cadence, R11-M20); `deploy/supabase/arm_schema.sql` (applied manually, versioned by
  `arm.schema_version`, R11-m); the `arm-config` task (120 s) added in `src/runtime/optional.py`.
* **Tests:** a fake source: a tampered row pages and keeps the accepted document; schema ahead; unreachable for 15 min
  pages; a planted DSN, host and password appear in no log record, page, `arm_status` row, audit row or outbox body.

### S34 OPS-1: ops CLI controls and requests

Status: done 2026-11-16, review fixes applied 2026-11-16 (both flags stay off by default; nothing sends, changes or
cancels a broker order: the commands write operator rows and `operator_requests` only). Review fixes: the heartbeat
check counts only a leader row of the epoch the request is fenced on (`SQL_ARM_HEARTBEAT ... AND epoch = $1`); the
arm-tracker kinds (`TRACKER_REQUEST_KINDS`: disarm / rearm / repin, no handler yet) have a 20-request budget of their
own, so they never block `arm failed clear` or the `md` repairs, and their `pending` answer says no handler is wired;
`arm_accepted.accepted_at_ms` never goes backwards (max(now, latest + 1), the clock gap warned and audited as
`clock_behind_ms`); `ControlsView.drain()` lapses once the last good controls read is older than `DRAIN_STALE_S`
(15 s, three status periods; the pause keeps the last read); `notify-test` says no notice is sent; a PR13 refusal for a
node with an empty registry names `XTNL_MD_ENABLED`; the hash / indicator-id checks use `fullmatch` (a trailing
newline is refused); `config show` warns when an accepted document is not cached yet; the BARS cap comes from
`src/config.py rest_bars_per_min` (the adapters' own `_rest_limits` / `_bars_cap`, plus `default / max /
effective_bars_per_min`, a copy of `src/adapters/ratelimit.py`'s rule that a test checks against it: src/ops may not
import src.adapters and the `/live-trading-server` skill, needed to make ratelimit import it, is user-invoked only), so
a bad `{prefix}_REST_RATE_PER_S` / `_REST_BURST` refuses `arm config accept` (`settings`) instead of a silent default.
Built: `src/ops/arm_sql.py` (the only SQL that writes
`arm_controls` / `arm_accepted`; heartbeat, pending-count, answer, status, ids / loads, `arm_positions` state and
`arm_failures` reads; ids, codes, counts and times only), `src/ops/arm.py` (`ArmContext.from_env`: the Supabase reader
of the engine (`XTNL_ARM_CONFIG_DSN`, pinned CA, asyncpg lazily), `XTNL_TICK_SYMBOLS`, `XTNL_ARM_STOP_READS`, the
per-adapter BARS cap by the adapters' own rule; `ArmOps`): `arm config show` (latest accepts, cache hash and seen
rows, controls, every node's `arm_status`; a cache hash that is not the latest accepted is warned as the
`config_hash` pause) and `--pending` (the Supabase read: every row `seen` / `pending` / `tampered:changed|vanished`,
the row text of non-seen rows and the cached "was" text of a changed row, escaped and bounded; validation problems;
the admission numbers per adapter; the referenced `KIND:VERSION` tokens; whether `accept` would be refused and the
exact accept command; read-only, not audited); `arm config accept <hash>` (re-reads Supabase and refuses, audited,
`hash_changed` when the read no longer hashes to the reviewed hash, `tampered` / `schema_ahead` by the S25 chain
against the cached seen rows, `known_ids_missing` / `invalid` by `validate_document` with this engine's ids from
PostgreSQL (instruments, `account_state` accounts, routes `adapter:account` and `*`) and the seen rows, `no_broker_bars`
for an assignment in force or future-dated scoped to an adapter without broker bars (§B6.4), `admission` with the
numbers (`admission()` over loads from PostgreSQL and the settings: instruments, held symbols plus tick symbols as the
pre-warm set, armed positions and their accounts for the PR9 stop reads), `registry` (PR13 through `get_prefix` under
`HEALTH_PREFIX`: a fresh record lacking a referenced token, a record without the field, an unreadable record, no etcd
configured or reachable); then, under `LOCK TABLE arm_accepted`, one `arm_accepted` row (mode `manual`,
`accepted_at_ms` on the engine host's clock, `audit_id`) and its `operator_audit` row (detail: hash, row count, the
accepted pending rows as `table:id:hash`, the previous hash, series / bytes, referenced versions); the same hash again
is an audited no-op; source errors are refused with fixed codes and sanitised reasons, never the DSN);
`arm pause [--drain-notices]` / `arm resume` / `arm notify-test` (`LOCK TABLE arm_controls`, the row `FOR UPDATE`,
the audit row first and its id in `arm_controls.audit_id`; pause reason `operator`, the first pause time kept, a pause
never narrows the drain, a repeat is an audited no-op; resume ends the pause and the drain and never clears FAILED);
`arm failed show [--all]` (episodes with scope, reason, probe kind, ping id, integer counters, affected positions
bounded to 20 then "and K more", last ping / probe, pending clear requests); the requests `arm failed clear
<SCOPE_KIND:scope_id>` (`arm_clear_failed`, moot `not_failed` without an open episode), `arm disarm | rearm`
(`adapter:position`, moot `not_armed` for no row or CLOSED / DISARMED, as the tracker answers), `arm repin [--strategy
CODE --strategy-version N]` (payload `{"v": 1, "strategy_code", "strategy_version"}` or `{}`), `md backfill <series>
--from-ms --to-ms` (the S32 payload), `md rebuild <indicator_id>`, `md writer-reset` (target `md-writer`), `md rehash`
(target `md`): each in one fenced transaction refused `no_arm_heartbeat` (no leader `arm_status` heartbeat younger
than 60 s), `too_many_pending` (20 ARM / market-data requests pending in the epoch), `already_pending` or
`epoch_moved`, else the audit row and the request; a moot request is audited `noop` and not filed; then the CLI polls
the answer for `--wait` seconds (default 5, 0-60) and reports `applied`, `pending`, `moot:<code>` or
`refused:<code>` (exit 3)), `src/ops/cli.py` (the `arm` and `md` groups, `dispatch_arm`, `main(..., arm_context=)`,
synopsis and exit codes), `src/ops/service.py` (`OpsService.arm`, public `conn` / `clock_ms` / `audit` / `fenced` /
`plain` / `refused`), `src/runtime/optional_plan.py` (the S33 hand-off: `ControlsView` reads `arm_controls` on the ARM
pool in the `arm-status` loop every 5 s; its operator pause is ORed into the `arm_status` row (reason `operator`
first, then `config_hash`) with `controls_*` counters; its drain flag, only while paused, is the arm-relay's
`drain_notices`; an unreadable or missing row keeps the last read and is never a pause; a lost fence raises),
`src/db/operator_requests.py` (payload docs). Tests: `tests/ops/arm_emulator.py` (`ArmOpsPg`: a handler per new SQL
constant, every 0017 request kind), `tests/ops/test_arm_ops.py` (36: controls applied / no-op / drain widening /
resume / notify-test / missing row / a failed write leaves no audit row / actor checks; requests refused without a
fresh leader heartbeat (none, stale, standby only), at 20 pending (a core request not counted), already pending, moot
without filing, filed fenced with audit id and payload, the answer applied / moot / refused / pending, repin payloads,
the clear scope form, every md target and payload, an epoch bump mid-filing; `failed show`; `config show` with and
without `--pending` (row statuses, escaped text, the "was" of a changed row, no DSN refused and not audited);
`config accept` applied and its no-op, `hash_changed`, tampered / vanished / schema ahead / invalid / no broker bars /
admission, the five PR13 refusals and a stale record ignored, failing sources with a planted DSN in no output or
audit row, a document problem; `referenced_tokens`, `no_bar_refusals`, `bars_cap`; the S24 static write check flags
`src/ops/arm_sql.py`'s writes anywhere outside `src/ops`; the CLI end to end, a refused answer exiting 3, no DSN in the
output, every command parsing, no broker verb or adapter import), `tests/runtime/test_arm_controls_view.py` (2: the
view and the status-row OR), `tests/runtime/test_optional_plan.py` (the relay's drain flag is the controls view; the
status-loop rig gains `controls_store`). G green: pytest 4955 passed / 55 skipped, mypy strict clean (294 files),
`verify_execution.py --strict` 11 / 11. (The env-documentation test of `tests/deploy/test_deploy_assets.py` scans
for literal setting names: `src/ops/arm.py` takes `STOP_READS_VAR` from `src/riskmgr/config.py` and builds the
`{prefix}_REST_*` names at run time, so no undocumented setting is added; the env example's ARM settings stay S36's.)
Deviations: (1) **bar capability**: `src/ops` never imports an adapter, so the bar-capable adapters are the reviewed
constant `src/ops/arm.py BAR_CAPABLE_ADAPTERS`, empty in v1 (no adapter's `price_bars_supported` is true; S39 adds
`stonex` there when it enables StoneX bars): until then an assignment scoped to an adapter is refused
`no_broker_bars` and a wildcard one only noted; (2) **md requests need the ARM heartbeat**: the §B13 heartbeat rule is
applied to every new kind, and S32 writes the `arm_status` heartbeat row only while the ARM runs, so with
`XTNL_MD_ENABLED=1` and the ARM off the `md ...` requests are refused `no_arm_heartbeat` (fail closed; writing the
heartbeat row in md-only mode is a small `arm-status` change left for the user's choice); likewise the nodes publish
their registry tokens only with `XTNL_MD_ENABLED` on (`src/runtime/wiring.py`), so with the services off (the
default) every fresh health record has an empty registry and `arm config accept` is refused `registry` (fail closed,
PR13; the detail says `no registry published: XTNL_MD_ENABLED is off on that node`); (3) **`notify-test`** only
records `arm_controls.notify_test_at_ms`: the test-notice sender (`ArmNotices`) is not wired (S32 deviation 1), so no
test notice is sent yet (the command's output says so); (4) the `arm_disarm` / `arm_repin` / `arm_rearm` requests are filed and reported `pending`:
no handler is registered until the `arm-tracker` is wired (S32 / S33 deviation 1), and they stay pending in the epoch
(S10); (5) the S33-suggested follow-up (storing the accepted row refs in `arm_accepted` so the installer needs no clock
cut-off) is **not** built: it needs a migration (0018) and an installer change outside this stage; (6) the CLI's
known ids come from PostgreSQL (`instruments`, `account_state`), not from the engine's loaded instrument maps (the
same sets once loaded; local aliases do not exist yet, S33 deviation 9); an empty `instruments` table refuses the
accept (`known_ids_missing`); (7) the referenced versions checked against the nodes' registries are those of every
strategy row of the document (with its guards and required indicators), not only the assigned ones (stricter);
no fresh health record at all is a warning, not a refusal (no running node to lack a version); (8) the accept is
single-person (`--approver` optional): design §B9 makes two-person acceptance mandatory only in Phase 2; (9) the
operator controls are read every 5 s by the `arm-status` loop (status row, relay drain); the BarClock's own 1 s
controls read comes with the S21 join; (10) `arm show` / `md show` / `arm replay` and the registry check command are
S35's; (11) the `/live-trading-server` skill was not invoked: no file in `src/adapters`, `src/pubsub`, `src/dto`,
`src/integrity`, `src/oversee`, `src/marketdata`, `src/riskmgr` or `src/armconfig` changed; no `.claude/` change and
no new `verify_execution.py` check are needed (the plan lists none for S34).

* **C / M:** `src/ops/arm.py`, `src/ops/arm_sql.py`, `src/ops/cli.py`, `src/ops/service.py`: `arm config show
  [--pending]`, `arm config accept <hash>` (admission check, and the PR13 check through `get_prefix`: refused while a
  fresh node record lacks a referenced version; a record without the field lacks everything), `arm pause | resume
  [--drain-notices]`, `arm notify-test`, `arm failed show | clear <scope> --reason`, `arm disarm | repin | rearm`,
  `md backfill | rebuild | writer-reset | rehash`; table lock plus `operator_audit` row; refused when `arm_status` has no
  heartbeat younger than 60 s, or when 20 ARM requests are pending in the epoch; results applied / pending / moot /
  refused.
* **Tests:** `tests/ops/` for each command; the static write check from S24 covers the new SQL.

### S35 OPS-2: views, `arm replay`, registry check

Status: done 2026-11-17 (read-only views and checks in `src/ops`; nothing wired into the engine, both flags stay off;
review fixes applied 2026-11-17, below).
Built: `src/ops/md_view.py` (`parse_reader_settings` / `parse_keyring`: `XTNL_MD_REDIS_READER_URL` and the
`XTNL_MD_HMAC_KEY*` keyring, problems naming variables only, URL and keys never in a `repr`; `OpsMdReader` over the
`md_reader` client using the `md_reader` ACL's commands only, store errors as `OpsRefused("md_unreadable")` with the
class name and allow-listed code only; `md show` (md generation, boot node, writer node and epoch, never the term
nonce; per stored series its `md:meta` states and counters through an allow-list of integer and plain-code values,
the rollback-set depth and, with the keyring, the re-hash progress `rehash: complete | N series pending`; FAULTED
indicators), `md bars <series> [--newest N]` (counts by quality, revised, not-final and superseded members,
quarantined buckets with check names and times parsed from the member prefix only, the newest buckets with revision,
quality, finality, tick count and keyed-hash status `valid` / `mismatch` / `key_retired` / `unchecked`), `md indicators
[--indicator ID]` (class, input series, params, per output the newest value's bucket, quality, warm, finality, window
start, value revision and FAULTED reason; never the value or the spec's price increment)), `src/ops/arm_replay.py`
(each stored decision's bar inputs re-read **as of its `decided_at_ms`** from `md:bars` + `md:bars_rev` and checked
under the keyring: `MATCH`, `CHANGED`, `DIGEST_MISMATCH` (the stored `inputs_digest` recomputed under the key id it
names), `bars_missing` (a BROKER bar not retained while `md-bars` can still re-fetch it), `UNVERIFIABLE(key_retired |
superseded_expired | beyond_broker_reach | tick_history_gone | bar_invalid | malformed_input)`, `NO_INPUTS` (PRE rows,
spans); `ENVELOPE_CHANGED` against this engine's `ENVELOPE_VERSION` and local `XTNL_ARM_MIN_GAP_R`),
`src/ops/arm_view.py` (`ArmViews`: `arm show [<adapter:position>] [--all]`, `arm decisions [<adapter:position>]
[--from-ms --to-ms]`, `arm config resolve --position X` (the installed document validated against this engine's ids,
the assignment in force at arming and now, whether it matches the pin, the route policy now; refused `not_armed`,
`no_cached_document`, `cache_unreadable`, `cache_invalid`), `arm replay [--request-backfill --operator A --reason
...]` (one audited `md_backfill` per contiguous run of missing buckets, at most `BACKFILL_CHUNK_MAX` buckets each and
8 per run, through the S34 filing path: heartbeat, budget, pending), `arm registry check --against-db` (every `KIND:VERSION` referenced by `arm_positions` pins,
guard lists and indicator specs, by `arm_decisions` and by the installed document; refused exit 3 `registry_missing` or
`pin_unreadable`, naming who references the missing version), `md show | bars | indicators`), `src/ops/arm_registry.py`
(`PINNED_TOKENS`, the reviewed append-only list of released versions: `break_even_on_risk_close:1`, `first_bar:1`,
`ATR:1`; `pin_tokens`, `missing_tokens`, `pinned_registry_problems`), `src/ops/arm_sql.py` (`SQL_POSITIONS_VIEW`,
`SQL_DECISIONS_VIEW`, `SQL_PINNED_VERSIONS`, `SQL_DECISION_VERSIONS`: ids, codes, counts, times and keyed input hashes;
no entry / S0 level, pinned units, `pre_basis` (only its presence), proposed stop, risk R or first-bar ratio),
`src/ops/arm.py` (`ArmContext.md_reader` / `envelope` / `envelope_problems` from the environment, `ArmOps.context`,
`service`, `local_config`, `local_ids`), `src/ops/cli.py` (the parsers, synopsis and dispatch). Every text read from
PostgreSQL or `redis-md` is escaped (`escape_text`: control characters as `\xNN`) and bounded. **User approval:** the
pinned-registry check for `verify_execution.py` is proposed in
`docs/design/proposed-claude-changes/S35-pinned-registry-check.md` (not applied); the same assertion gates the pytest
leg today. Tests: `tests/ops/arm_view_fakes.py` (`ArmViewPg`: a handler per new SQL constant; fakeredis seeders),
`tests/ops/test_arm_replay.py` (22: replay on stored decisions matches; a missing bar reports `bars_missing`; TICK and
beyond-reach bars `UNVERIFIABLE`; a retired key reports `UNVERIFIABLE(key_retired)` and verifies while the previous key
is retained; a changed bar `CHANGED`, a tampered digest `DIGEST_MISMATCH`; a superseded revision read as of the
decision, or `superseded_expired` once aged out; `ENVELOPE_CHANGED` and an unparseable envelope; `--request-backfill`
files one audited `md_backfill` with the bucket range, is refused without a heartbeat and needs an actor; no reader or
no keyring refused; the CLI prints codes only, never a planted price, hash, URL or password; the review-fix tests
below), `tests/ops/test_arm_views.py` (13: the view SQL selects no level / unit / R column and writes nothing; `arm show`
escapes control characters and hides ended rows unless named or `--all`; `arm decisions` newest first; `arm config
resolve` at arming versus now and its refusals; `PINNED_TOKENS` all load and a bare registry fails; pin tokens fail
closed; the registry check fails on a missing pinned version (position, live position, decision, installed document)
and on an unreadable pin; a kind with control characters is unreadable, never printed; the CLI exit codes),
`tests/ops/test_md_views.py` (10: settings and keyring problems name
variables only; the `md:meta` allow-list; `md show` states, rollback depth, re-hash progress, no nonce / price /
increment; `md bars` counts, checks and hash statuses with no value; undecodable members; `md indicators` lineage
without the value or increment, FAULTED; a store error refused without the URL; `md bars` reads are bounded).
**Review fixes (2026-11-17):** (a) a bucket re-fetched after the decision (`md_backfill`, the gap heal or a
re-bootstrap store it as revision 0 with `final_at_ms` at the fetch time) no longer ends in
`UNVERIFIABLE(superseded_expired)`: when no stored revision was current at `decided_at_ms`, a validated later member
whose content verifies against the recorded keyed hash gives `MATCH` (other content stays `superseded_expired`), so
`bars_missing` → `--request-backfill` → re-run converges; (b) `backfill_ranges` groups the missing buckets per series
into contiguous runs split at `BACKFILL_CHUNK_MAX` = min(`MD_BACKFILL_MAX`, `LANE_MAX_PER_SERIES`) buckets (the leader
refuses larger ranges as `range_too_large` / `lane_full`), files at most one pending request per series per run (the
leader refuses a second one as `already_pending`) and lists the rest under `backfill_not_filed`; (c) a malformed stored
input is shown as `malformed#i`, never its text (which carries a keyed hash); (d) `token()` accepts only
`[A-Za-z0-9_]{1,64}` kinds (strategy, guard and required-indicator kinds), so anything else is counted unreadable
(fail closed) and never printed raw; (e) the broker reach is per spec (`broker_reach_ms`: `md-bars`'
`BrokerBarsSettings.history_reach_ms` for a spec with a timing row, else 0), so a missing 1m BROKER bar is
`UNVERIFIABLE(beyond_broker_reach)`, never a backfill `md-bars` cannot serve; (f) the revision current at the decision
held only under a retired key id is `UNVERIFIABLE(key_retired)`, ahead of `superseded_expired`; (g) the
`arm_registry` docstring names the real test; (h) `md bars` reads at most `COUNT_MAX` (50 000) newest members,
superseded revisions and quarantine entries (`ZRANGE -n -1`, `ZRANGEBYSCORE ... LIMIT`) and says `truncated` when the
series holds more. G: pytest 5006 passed / 55 skipped, mypy strict clean (298 files), `verify_execution.py --strict`
11 / 11 (12 / 12 once the proposed check is applied).
Deviations: (1) **`arm replay` verifies, it does not re-run the pipeline**: each decision's recorded inputs are re-read
as of its decision time and their keyed hashes and the inputs digest re-checked; re-running `run_pipeline` on them
(and so a recomputed outcome) waits for the S21 join, which waits for the user's §9.2 / C1 confirmation (S35 imports
no S21 module); (2) indicator inputs are checked through the inputs digest only (their window bars are not re-read:
nothing builds indicator inputs before the S21 join, and the window membership rule is the pipeline's); (3) the
pinned-registry check of `verify_execution.py` is a static check of the reviewed `PINNED_TOKENS` against this build's
registry (the script has no database); the database half is `arm registry check --against-db`; (4) `md show` does
not show the store's reset class (it is carried on the `md:events` `md_reset` event, which the read set used here does
not read; `arm_status` and the MD_RESET notice report it); (5) a missing TICK bar is `UNVERIFIABLE(tick_history_gone)`,
not `bars_missing` (TICK bars are never re-fetched), and a BROKER bar older than the broker reach of its spec
(`broker_reach_ms`: `md-bars`' history reach, or 0 for a spec without a timing row) is
`UNVERIFIABLE(beyond_broker_reach)`; neither files a backfill; (6) a replay `CHANGED` /
`DIGEST_MISMATCH` is reported (rows, counts, a warning) with exit 0, not as a refusal: the command is a read-only
check, not an audited action; (7) the views select no order-state level either (entry, S0, proposed stop, handle
levels), only their sources and presence, which is stricter than the §1 rule; (8) the `/live-trading-server` skill was
not invoked: no file in `src/adapters`, `src/pubsub`, `src/dto`, `src/integrity`, `src/oversee`, `src/marketdata`,
`src/riskmgr` or `src/armconfig` changed (the reader settings parse lives in `src/ops/md_view.py` rather than in
`src/marketdata/config.py`, whose `parse_md_settings` requires the writer URL); (9) the md views and `arm replay` still
open the PostgreSQL connection every CLI command opens (`ORDER_SERVER_DSN`), although the md views do not query it.


* **C / M:** `md show | bars | indicators`, `arm show | decisions`, `arm config resolve --position X`, `arm replay
  [--request-backfill]` (read-only through `XTNL_MD_REDIS_READER_URL` and the keyring; reports `bars_missing`,
  `UNVERIFIABLE`, `ENVELOPE_CHANGED`), `arm registry check --against-db`; output escapes control characters and shows no
  values. **User approval:** a pinned-registry check in `verify_execution.py`.
* **Tests:** replay on stored decisions matches; a missing bar reports `bars_missing`; a retired key reports
  `UNVERIFIABLE`; the registry check fails on a missing pinned version.

### S36 DEP-1: PC deploy (PR10)

Status: done 2026-11-17 (deploy assets and tests only; no engine code changed except the API's refusal list; both
flags stay `0`). Built: `deploy/local/docker-compose.local.yml` service `redis-md` (profile `md`, `redis:7.4`,
`restart: unless-stopped`, `127.0.0.1:56382:6379`, AOF `everysec` with `aof-load-truncated`, no RDB, `--maxmemory 1gb`
`noeviction`, `mem_limit: 2g`, `--aclfile` from a long-syntax read-only bind of `${LOCALAPPDATA}/XTNL/redis-md/users.acl`
with `create_host_path: false`, named volume `xtnl_local_redis_md` with explicit `name:`, healthcheck as `md_health`;
header "NEVER `down -v`" line and port list updated; `docker compose --profile md config` validates);
`deploy/local/start-xtnl.ps1` (step 5b `Start-RedisMd`: `--profile md up -d redis-md`, run only when the loaded env
file sets `XTNL_MD_ENABLED=1`, skipped with a log line when the ACL file is missing, never fatal and never waited for;
`$UserVarNames` / `$ForbiddenInFile` gain the §C2 secrets, the Oversee channel credentials and `XTNL_DEADMAN_TOKEN`;
`$ApiDropEnv` gains `XTNL_MD_*`, `XTNL_ARM_*`, `XTNL_SMTP_*`, `XTNL_SES_*`, `XTNL_PUSH_*`, `XTNL_DEADMAN_TOKEN`,
`BROKER_ENV`); `src/api/settings.py` `FORBIDDEN_PREFIXES` / `FORBIDDEN_NAMES` likewise; `stop-xtnl.ps1 -Containers`
and `status-xtnl.ps1` pass `--profile md`; status adds the Windows time-sync offset (the engine's own clock probe), a
redis-md section (flag, ACL file present, container state, `md show` with the reader URL and keys copied from the User
environment for that call only) and an ARM section (flag, `arm failed show`); `$Hide` covers every new secret name;
`deploy/env/engine.env.example` and `deploy/env/local-live.env.example` (both flags `0`,
`XTNL_STONEX_PRICE_UNFILTERED=0`, `XTNL_ARM_STOP_READS=0`, every non-secret `XTNL_MD_*` / `XTNL_ARM_*` setting at its
parser default, the secrets as comments only); `deploy/local/new-md-credentials.ps1` (two random 32-byte passwords,
the ACL file with `#<sha256>` entries and the design's rules, written UTF-8 without BOM via a temporary file, refused
inside the repository or OneDrive; both URLs and the HMAC key in the Windows User environment only; `-Rotate` for
the passwords, `-RotateKey` / `-RetirePrevKey` for the §A13 key rotation). Tests: `tests/deploy/test_single_pc_autostart.py`
(`SERVICES` with `redis-md`, port dict with 56382 and not in the dev stack, the three volumes in the `down -v` line,
`$ForbiddenInFile` exact set, profile-only service, AOF / memory flags, ACL bind, secret-free healthcheck, stop / status
with the profile, env examples with both flags off and the parsers' defaults), `tests/deploy/test_order_server_api_assets.py`
(drop-list rule: every `$UserVarNames` name other than `XTNL_API_*` is dropped and refused by the API; the md step is
guarded by `XTNL_MD_ENABLED` and non-fatal; `new-md-credentials.ps1` shape and never printing or filing a secret),
`tests/deploy/test_deploy_assets.py` (`USER_ENVIRONMENT_ONLY` += the MD and ARM secrets), `tests/api/test_settings.py`.
A throwaway `redis:7.4` container loaded an ACL file built from the script's own rules and hash function: `md_health`
PONG, `md_writer` writes `md:*` only and is refused FLUSHALL and other keys, `md_reader` reads only, the default user
is off. G: pytest 5033 passed, 55 skipped; mypy strict clean (298 files); `verify_execution.py --strict` 11 / 11.
Deviations: (1) `BROKER_ENV` joins the API drop list and `FORBIDDEN_NAMES`, because the plan's rule test (every
`$UserVarNames` name other than `XTNL_API_*` is dropped) covers it; the API never reads it, but a hand-started
`python -m src.api` in a shell that carries `BROKER_ENV` now refuses (exit 78), as it already did for `STONE_X_*`;
(2) the env-var detector of `tests/deploy/test_deploy_assets.py` now also scans modules that define a `*_VAR`
constant (the pure parsers `src/marketdata/config.py`, `src/riskmgr/config.py`, `src/armconfig/client.py`, whose caller
passes the environment) and reads `tolerant_flag`, so the existing "every variable the code reads is documented" tests
cover the new settings; the ARM channel credentials (read under a prefix) are matched against `CHANNEL_VARS`; (3) the
key ids `XTNL_MD_HMAC_KEY_ID` / `_PREV_ID` are non-secret env-file settings (`1` / empty), not User-environment
variables; `new-md-credentials.ps1 -RotateKey` prints the two values to set; (4) `status-xtnl.ps1` also runs `md show`
(flag `1` and a reader URL set) besides the plan's sections; (5) the two settings earlier stages left to S36 are
documented: `XTNL_STONEX_PRICE_UNFILTERED=0` (S03) in both examples, `STONE_X_REST_BARS_PER_MIN` /
`CTRADER_REST_BARS_PER_MIN` (S06) as comments beside the REST bucket overrides; (6) no runbook changed (S37 writes
`docs/runbooks/marketdata.md` / `arm.md`); no `.claude/` change is needed by this stage.
Review fixes (2026-11-17): (a) `start-xtnl.ps1` reads `XTNL_MD_ENABLED` as the engine does
(`src/runtime/optional_settings.py` `optional_flags_from_env`, not `tolerant_flag`, which only
`XTNL_STONEX_PRICE_UNFILTERED` uses): the process value after the env file is applied (so a Windows User environment
value counts too), trimmed, exactly `1` on; unset / `0` off; anything else off with a warning line. The review
suggested 1/true/yes/on, which would have started redis-md while the engine kept the services off; the engine's own
rule was kept instead. `status-xtnl.ps1` uses the same test; both env-example comments say so. (b) `Start-RedisMd`'s
body is in try / catch (`$ErrorActionPreference = 'Stop'` made a `Process.Start` or pipe error fail the attempt);
the test asserts the try / catch. (c) New non-fatal `Stop-RedisMd`: with the flag off, a redis-md container left
running (`restart: unless-stopped`) is stopped (`--profile md ps -q`, then `stop`; the volume is kept). (d)
`test_real_local_env_has_no_credentials_when_present` checks every `$ForbiddenInFile` name (parsed from the script),
not only `STONE_X_*`; it passes on this PC's `local.env`. (e) Runbooks `single-pc-autostart.md` (the full refused
list, an "Optional market-data store (redis-md)" section: flag rule, enable / disable steps, `new-md-credentials.ps1`,
status) and `order-server-api.md` (the full `$ApiDropEnv` drop-and-refuse list, `BROKER_ENV` included) updated, so
deviation (6) no longer holds for these two. G: pytest 5033 passed, 55 skipped; mypy strict clean (298 files);
`verify_execution.py --strict` 11 / 11.
Left for the user: before the next `start-xtnl.ps1` run, make sure `deploy/env/local.env` assigns none of the newly
forbidden names (the Oversee channel credentials `XTNL_SMTP_USERNAME` / `_PASSWORD`, `XTNL_SES_ACCESS_KEY_ID` /
`_SECRET_ACCESS_KEY` / `_SESSION_TOKEN`, `XTNL_PUSH_TOKEN` / `_USER`, `XTNL_DEADMAN_TOKEN`, or any market-data / ARM
secret): the script now refuses such a file (exit 78, names only in the log); move any such value to the Windows User
environment. The live `redis-md` path (`new-md-credentials.ps1`, then `XTNL_MD_ENABLED=1`) is §9.3 step 2.

* **M:** `deploy/local/docker-compose.local.yml` (`redis-md`: profile `md`, `redis:7.4`, `restart: unless-stopped`,
  `127.0.0.1:56382:6379`, named volume `xtnl_local_redis_md` with explicit `name:`, AOF flags, `--aclfile` from a
  read-only bind of `%LOCALAPPDATA%\XTNL\redis-md\users.acl`, `mem_limit: 2g`, `--maxmemory 1gb`, `noeviction`,
  healthcheck as `md_health`; header "NEVER down -v" line and port list updated); `deploy/local/start-xtnl.ps1` (a
  separate non-fatal `--profile md up -d redis-md` step after step 5, run only when the loaded env file sets
  `XTNL_MD_ENABLED=1`, mirroring how Step-Api runs only when its secret is set; with the flag `0` the container is not
  started; `$UserVarNames` and `$ForbiddenInFile` gain the
  exact secret names of §C2, the Oversee channel credentials and `XTNL_DEADMAN_TOKEN`; `$ApiDropEnv` gains the prefixes
  `XTNL_MD_`, `XTNL_ARM_`, `XTNL_SMTP_`, `XTNL_SES_`, `XTNL_PUSH_` and `XTNL_DEADMAN_TOKEN`, R11-m);
  `src/api/settings.py` `FORBIDDEN_PREFIXES` / `FORBIDDEN_NAMES` likewise; `deploy/local/stop-xtnl.ps1` and
  `status-xtnl.ps1` (`--profile md`, a `redis-md` section, an ARM section with FAILED scopes, and the Windows time-sync
  offset, design §C2); `deploy/env/local-live.env.example` and
  `deploy/env/engine.env.example` (non-secret `XTNL_MD_*` / `XTNL_ARM_*`, both enable flags `0`).
  **C:** `deploy/local/new-md-credentials.ps1` (copy of `new-api-credentials.ps1`: two `redis-md` passwords and the HMAC
  key in the Windows User environment only; ACL file with `#<sha256>` entries outside the repository and OneDrive).
* **Tests:** `tests/deploy/test_single_pc_autostart.py` (`SERVICES`, port dict with 56382 and not in the dev stack,
  volumes, `$ForbiddenInFile` exact set), `tests/deploy/test_order_server_api_assets.py` (drop list equality, plus the
  rule that every `$UserVarNames` name other than `XTNL_API_*` is in the API drop list; the `md` step is guarded by
  `XTNL_MD_ENABLED`), `tests/deploy/test_deploy_assets.py`
  (`USER_ENVIRONMENT_ONLY` += the MD and ARM secrets); a test for `new-md-credentials.ps1` mirroring the API one (not
  added to `SCRIPTS`, whose rule allows only `'Process'`).
* **Risk:** these files carry uncommitted work today; build on the current content.

### S36b RT-3: ARM runtime join (added 2026-11-17)

Status: done 2026-11-17 (both flags stay off by default; with `XTNL_MD_ENABLED=1` and `XTNL_ARM_ENABLED=1` an OPEN
position with an accepted assignment is armed, decided at PRE and POST on the bar clock and its notices are queued in
the ARM outbox; nothing sends, changes or cancels a broker order). **Built on the unaccepted S21** as this stage
directs, with the plan §9.2 / design §0 defaults, **still unconfirmed by the user** and each switchable later: C1
`pre_basis` kept until a bar end confirms the trigger (the S21 rule), C2 ticks recorded while globally FAILED (the
market-data layer's, unchanged), C3 the REOPEN item at the open (the S22 planner), C4 the S07 stop rule
(`effective_stop_detail`); the §9.2 Oversee change itself is still the user's to confirm. Built:
`src/riskmgr/evaluation.py` (the S21 join, `PipelineEvaluator`, the BarClock's `EvaluationPort`: every read before
the job (pin re-parsed, strategy state, SM11 state, the account image shared by one pass's prepares, the current
handles with their PR9 read times, `S_at_T` from `ArmVersionReader.stop_versions`, the broker's final trigger bar,
the exit quote, `required_history`, the indicator values of the last two closed buckets), the job runs
`run_pipeline` and maps `PipelineResult` to `Evaluated`: the decision row (codes, `reason_kind`, `trigger_met`,
`borderline`, the proposed stop and handle levels as order state, `current_risk_r = σ(E − S)/R` from order state,
the first-bar ratio, caveats with `first_bar_conditional` for a guard label, close source, stop basis, the trigger
bar as the one series input `series|T|key_id‖hash` with its HMAC `inputs_digest`), the handle rows by role, the
trigger hash for the frontier, SM11 events, state, `pre_basis` (`encode_pre_basis` / `decode_pre_basis`), the
REOPEN candidate and `shown`; a pending bar is `PENDING(broker_bar_not_final)`; `NotReady` codes
`pin_unreadable`, `state_unreadable`, `account_image_missing`, `position_not_open`, `failed` (a FAILED scope covers
the row)), `src/runtime/arm_ports.py` (`PgTrackerSnapshot`: the engine-pool `TrackerSnapshotPort` in one READ ONLY
REPEATABLE READ snapshot: accounts with OPEN positions plus those asked for, OPEN positions plus positions with a live
`arm_positions` row, non-terminal orders plus the entry orders, instruments, freshness reasons `orders_fence`,
`account_fence`, `integrity_incident`, `catchup_unverified`; `CalendarGrid`: the `BarGridPort` and
`SessionCloseView` over the SM10 schedule (a bucket with any scheduled OPEN part is a bar; the next open of a
pre-close bar), the tick hook's skew, both quote sides for a `price_bars_supported` adapter; `LateNoticeSink`, the
`ARM_SINKS` notice slot bound to `ArmNotices.sink()` once the graph exists; `TrackerBook` (PR9 `StopHandlesPort`,
the FAILED manager's `PositionSeriesPort`); `stop_read_cost`). Changed: `src/runtime/optional_plan.py` (the ARM
graph builds the `JobRunner`, `ArmNotices` (routes `adapter:account`, `AcceptedPolicies`, the ARM outbox inserter,
`FAILED_CLEARED` from the FAILED manager), the `ArmTracker` (task `arm-tracker`, stamp 10 s) and the `BarClock`
(evaluator, the installer's `pause_reason` as its config pause, the controls on the ARM pool, PR9 `StopReads`);
`ProductionPlan.arm_sinks`; handlers `arm_disarm` / `arm_repin` / `arm_rearm` (`parse_tracker_request`, enqueued,
answered applied / refused / moot by the tracker; `refused:bad_payload`, `queue_full`, `arm_not_running` at once);
the `arm` loop: FAILED start, then each second the observations, `FailedManager.tick`, the job runner's
STRATEGY_VERSION signals, the stuck-frontier views (`BarClock.frontier_views`, paused / held positions never count:
the S33 hand-off) and the `arm notify-test` request; the BarClock's pass every 250 ms (or at the next PRE fire) once
the open episodes are read and while no ARM-global FAILED holds; the notices' timer pass every 5 s, skipped while
either pause holds; on stop the BarClock's executors and stop reads are closed (a hung job thread ends the process
through `ctx.exit_fn`); `armed_adapter_ids()` from the tracker, so `redis-md` down time now counts toward
`fail_md_down_s` while an armed position's market is OPEN; `arm_overdue_positions`; the `arm_status` row with
`running` True while the BarClock runs, the armed / unmanaged / overdue counts and the `barclock_*`, `tracker_*`,
`notices_*`, `evaluation_*` counters; S34 deviation 2: the `arm-status` loop writes the heartbeat row on the engine
pool whenever the market-data services run and the ARM graph does not), `src/runtime/optional.py`
(`OptionalServices.stop_read_cost`, bound by the plan to the tracker's handles), `src/riskmgr/tracker.py` (views
`armed_adapters`, `armed_count`, `overdue`, `stop_handles`, `handle_count`, `series_of`), `src/riskmgr/barclock.py`
(`guarded_step`, `live_refs`, `frontier_views`), `src/riskmgr/notices.py` (`request_test`: one TEST per channel
keyed by the request time), `src/calendar/market.py` (`schedule_of`), `src/ops/arm.py` (the tracker-kind comment and
the pending warning: the arm-tracker answers them now; their separate 20-request budget is kept). Tests:
`tests/riskmgr/test_arm_join.py` (3: end to end on fakes (the real tracker, BarClock, pipeline, job runner, sink
chain and notices over one in-memory ARM store, fake account images, `event_log` versions and read port): arming
with its `armed_late` span, POST with a strategy reason, a PRE `ADJUST` on the frozen quote, a POST `ADJUST`
queued in the outbox with its trigger input, the stop moved to target: no item at PRE, release (`COMPLETE`, SM11
RELEASED) only at the POST, every decision noticed, `arm_disarm` applied then moot, no page; the tracker views; a
FAILED scope holds the evaluation), `tests/riskmgr/test_evaluation.py` (7: `pre_basis` round trip and unreadable
forms, roles / proposed stop / `current_risk_r`, every `NotReady` code, a POST without the final bar pending with
`broker_bar_not_final`, a POST row's keyed trigger input verifying under the keyring and no bar or quote figure in
the row, PRE ADJUST then a bar end below the trigger storing `pre_basis` (C1), a PRE quote received after fire
refused), `tests/runtime/test_arm_ports.py` (9: the grid over a real FX schedule (weekend, daily break, pre-close
next open, no schedule, raising inputs), the late notice slot before and after binding, the snapshot in one
snapshot with its freshness reasons and read-only SQL, tracker request payloads, the handlers answering at once
and the tracker answering applied / moot / refused, `notify-test` queuing one TEST per channel once and ignoring an
old request, the stop-read cost, the md-only heartbeat writer). Updated: `tests/runtime/test_optional_plan.py` (the
tracker task and kinds are registered; the arm loop runs the BarClock and the notices; the md-only heartbeat row),
`tests/runtime/test_arm_controls_view.py` (the status-row stub; a raising installer counts as a pause),
`tests/ops/test_arm_ops.py` (the tracker kinds are the arm-tracker's and every ARM kind has a handler). Flags off:
nothing new is built or imported (the existing S31 / S32 tests). G: pytest 5052 passed, 55 skipped; mypy strict
clean (300 files); `verify_execution.py --strict` 11 / 11.
Review fixes (2026-11-17): (major) `this_bar_pre` is no longer kept only in RAM: a bar-end prepare re-reads it from
the committed PRE `ADJUST` row's handle rows (`handle_role`, `level_at_pre`), and "previous final at target" from the
newest committed bar-end decision (spans included), through the new read-only `ArmDecisionReader.decision_memory`
(`PgArmStateStore`, SQL `SQL_PRE_ADJUST_LEVELS` / `SQL_LAST_BAR_END_DECISION`, `src/db/arm_ports.py`
`DecisionMemory`); unreadable is `NotReady(decisions_unreadable)`, so a leader change or restart between PRE and POST
decides `PRE_NOT_CONFIRMED` and stores `pre_basis` as before (regression test: a fresh evaluator and BarClock
between PRE and POST); (minor) the job is pure (`evaluated()`): the one memory left ("LOOSENED told") is changed on
the loop by `Evaluated.committed` / `Decided.on_commit`, which the BarClock runs only for a committed row, so a
timed-out job, a CAS miss or a rolled-back PRE changes nothing (test: a PRE `ADJUST` whose commit failed leaves no
basis); (minor) "previous final at target" follows spans and restarts (from the store); (minor) `bar_index` looks for
bar 0 in 16-bucket windows from the fill (at most 14 days), never a walk to `T`; (minor) the BarClock is exempt
(`postgres`) only while it awaits its own controls / rows reads and transactions (`guarded_step(exempt=...)`), its
prepares run unexempt under `prepare_timeout_ms` (2 s, `prepare_timeout`) and the md generation reads under 1 s, so a
hung market-data read is a timeout and never hidden by the exemption (the evaluator's own PostgreSQL reads in a
prepare are bounded by the same timeout); (minor) the flags-off import test also checks `src.runtime.optional_plan`
and `src.runtime.arm_ports`. G after the fixes: pytest 5061 passed, 55 skipped; mypy strict clean (300 files);
`verify_execution.py --strict` 11 / 11.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); the
read-only rules were followed by hand (no broker call, sender or order path added; only reads through the existing
ports; no file in `src/adapters`, `src/pubsub`, `src/dto`, `src/integrity` or `src/oversee` changed); (2) S21 is
still "built ahead of the gate, not accepted": this stage joins it as directed, so the decision path now depends on
the user's §9.2 Oversee and C1-C4 confirmations (recorded above as unconfirmed); (3) the join lives in
`src/riskmgr/evaluation.py` and the engine ports in a new `src/runtime/arm_ports.py` (the plan allowed either
module for the ports); (4) the snapshot's freshness has no "consumers running" input (the session exposes none):
stream fences, the integrity incident and the catch-up verdict only; (5) a symbol without a loaded schedule has no
bar (fail closed: nothing is decided or spanned for it until its schedule loads); (6) `this_bar_pre` and
"previous final at target" are read from the committed decisions (review fix above); only "LOOSENED told" is kept in
memory per term, set on the loop after its commit (a restart may show `LOOSENED_AFTER_COMPLETE` once more); (7) `S_at_T`
reads the versions of the `stop_loss` field and the **current** stop children: a child replaced after `T` is not
named, so such a bar gives `DATA_UNAVAILABLE(stop_at_bar_end)` until it is spanned at its cap (fail safe);
(8) the decision row's series inputs are the trigger bar only (the history bars and indicator windows are not
listed; `indicator_inputs` stays empty, `inputs_digest` covers the trigger input); `entry_time_ambiguous` is always
False (no history execution-time source is wired); (9) the notices' timer pass is skipped while either pause holds
(the S33 hand-off "suppressed during either pause"), so ALIVE, TEST, extras and `FAILED_CLEARED` wait for the
resume; (10) the BarClock decides only once the open FAILED episodes are read and while no ARM-global FAILED holds
(scoped FAILED positions are held through SM11 and the evaluator's `held` predicate); (11) `arm notify-test`: a
request seen more than 1 h after it was made (an earlier term's) sends nothing; the TEST is keyed by the request
time, so a request seen again is a key conflict; (12) the PR9 stop-read cost is 3 list reads (PRE, POST, the POST
re-read) per adapter with armed handles, the reads of one 60 s window; (13) the md-only heartbeat is written on the
term's engine pool (`PgLeaderPorts.engine_db`); the in-memory ports have none and write no row; (14) the stuck-frontier
views and the STRATEGY_VERSION signals were wired here (the S33 / S30 hand-offs to "the S21 join"); (15) not wired:
the `MD_RESET` notice (no md reset hook reaches the plan yet), the uptime suspend signal of the BarClock, the
series re-read / fresh-bar / span / version clear probes (still `probe_not_wired`, fail closed) and correction
re-checks (S27 deviation 8); (16) `MarketCalendar.schedule_of` was added (a read accessor) and `src/ops/arm.py`'s
pending text changed; (17) no `.claude/` change and no new `verify_execution.py` check are needed.
Left for the user: confirm the §9.2 Oversee change and C1-C4 (S21 acceptance) before relying on ARM advice; the
end-to-end run against the dev compose (`md` / `pg` markers) is S38's.

Why: S32, S33 and S34 each recorded that the decision path is still not wired. The BarClock (`arm` decisions), the
`arm-tracker`, `ArmNotices` and the arm-tracker request handlers have no runtime home. The engine-pool
`TrackerSnapshotPort` and `BarGridPort` and the S21 `EvaluationPort` join were never built. Without this stage the ARM
starts, but it never arms a position, never decides a bar and never sends a notice. S38 cannot test the real path
without it.

Goal: complete the join, so that with `XTNL_MD_ENABLED=1` and `XTNL_ARM_ENABLED=1` (mock broker in tests) an OPEN
position with an accepted assignment is armed, gets PRE and POST decisions on the bar clock, and has its notices queued
in the ARM outbox.

- **Ports:** build the engine-pool `TrackerSnapshotPort` and `BarGridPort` (in `src/runtime/md_ports.py` or a new
  `src/runtime/arm_ports.py`), and the S21 `EvaluationPort` join.
- **Confirmation defaults:** use the design §0 defaults for C1-C4 (`pre_basis` kept until a bar end confirms; ticks
  recorded while globally FAILED; REOPEN item at the open; the S07 stop rule). These are the plan §9.2 safe defaults,
  each switchable later. Do not block on the user's confirmation; record in the Status line that they are still
  unconfirmed.
- **Wiring:** in `OptionalRuntime.arm_tasks` / `arm_handlers`, wire:
  - the `arm-tracker` task, with its `arm_disarm`, `arm_repin` and `arm_rearm` handlers;
  - the BarClock inside the `arm` task loop, next to the FAILED manager;
  - `ArmNotices`: the notice sink in the sink chain, `FAILED_CLEARED`, and the test-notice sender that answers
    `arm_controls.notify_test_at_ms`.
- **Status and reporting:** `armed_adapter_ids()` filled from the tracker; the real `stop_read_cost`;
  `arm_overdue_positions` reported; and the `arm_status` row showing `running` True while it runs.
- **S34 deviation 2:** with market data on and the ARM off, `md ...` requests must be answerable. Have the status loop
  write the heartbeat whenever the market-data services run.
- **S34 review minor:** once the handlers exist, requests no longer pile up unanswered. Keep the 20-request budget.

Tests:
- an end-to-end test on fakes (mock broker bars, fake `redis-md`, emulated PostgreSQL stores): arm, PRE, POST, an
  ADJUST notice queued, a disarm applied, release at a bar-end check only;
- flags off: nothing is built (existing tests);
- the tracker request handlers answer applied, refused or moot;
- `notify-test` queues one test notice.

G green.

### S38b DOC-1: design sync with the build (added 2026-11-17)

Status: done 2026-11-17 (doc only; no code, test or `.claude/` change). `docs/design/automated-risk-manager.md` is now
v11.2: status line "built behind flags, off by default; S39 pending the PR3 probe" with the open gates; a new §0b
(what exists, open gates, the pending `.claude/` proposals with the interim lockout-test guard, a table of every stage
deviation that changes behaviour or keys, the state kept only in memory, what is not wired, the price-derived values
inventory with `account_snapshots.equity` / `unrealised_pnl` and `sod_marks.sod_equity`, and the round-12 findings left
for S38c); every `file:line` citation replaced by a `path::symbol` anchor checked against the tree (all anchors
resolve; new files marked); in-place updates: C4 and PR1 (the unfilled-entry exception and its tests; S07 present
uncommitted), PRE bound 31-45 s (§B5.2, §B6.7, D1, PR9, §C3), ARM keys with `adapter_id` (§B5.3, §B6.6, §B8 item
keys, §B11, §B12, §B14 `position_ids`, §C1), `md:registry:leases` / `md:registry:stored` (§A3, §A7), the rollback
`write_seq` re-base (§A3, §A7, §A13), BROKER `no_trade` and cross-check as built (§A4), indicator worker and ATR window
(§A6), the accepted-subset install, install time, automatic mode not built (§B9), FAILED covering scopes, restore and
re-arm into FAILED (§B12, §B14), the full `arm_failures` columns and probe kinds, unwired probes, ping priority and
global-FAILED repairs (§B14), operator-request answers and the md-only heartbeat (§B5.1, §B13), `S_at_T` reads,
FirstBarGuard and arming as built (§B6.4, §B6.5, §B7), notice expiries and `arm replay` (§B8), the `ARM_SINKS` location,
the full hard guarantee and the pending hook copy (§B3), PR status markers (PR1-PR13), §C2 secrets read by the ops CLI,
key ids, metrics, AWS ACL rendering and the rules status. Diagrams under `docs/design/arm/` regenerated with Graphviz
(labels changed: PRE 31-45 s; Supabase poll 120 s; `ARM_SINKS`; flags off by default; the `md:registry` indexes and
`md:rb`; SM11 keyed with `adapter_id`, FAILED entered from any covering scope and left only when no open scope covers
it, re-arm into FAILED). G: pytest 5148 passed, 58 skipped; mypy strict clean (301 files); `verify_execution.py --strict` 11 / 11.
Deviations: (1) the design changes no behaviour: the round-12 MAJOR design fixes (PRE window formula, `NO_BROKER_BAR`,
one stop basis, scoped stuck frontier, AWS spool, `config_hash` page, grant narrowing, generic warm-up,
`RELEASE_POLICY`, hung threads) are listed in §0b as open for S38c, and the text states the built behaviour; (2) where
the code already does what a round-12 finding asks (rollback `write_seq` re-base, the lease and stored-series indexes,
the accepted subset, FAILED restore only when no scope covers, re-arm into FAILED, the AWS `redis-md` proxy section,
the ops CLI reading the config DSN), the design now says so, so S38c's items for the rollback set, the data model and
`prior_state` need only their tests; (3) the inventory states the account-level values as a judgement for the user
(Q15) rather than narrowing the §0 price-data definition; (4) the symbol anchors are now checked by
`tests/docs/test_design_anchors.py` (every `path::symbol` in the design resolves to a def, class or assignment, and no
anchor is a bare filename; added by the S38b review fixes); (5) the `/live-trading-server` skill was not invoked: no file in a skill directory
changed.
Review fixes (2026-11-17): S38c added to the `docs/IMPLEMENTATION_PLAN.md` checklist and to the order of work; "(new)"
added to the ten anchors whose files are untracked; `keys.py::STORED` written as `src/marketdata/store/keys.py::STORED`;
both Q4 texts say 31-45 s; SM11 diagram: FAILED entered from any non-terminal state (note), left to the stored
`prior_state`, the SUSPEND self-loop and re-arm into FAILED with `prior_state := ARMED`, re-rendered at 130 dpi. G after the fixes:
pytest 5193 passed, 58 skipped; mypy strict clean (301 files); `verify_execution.py --strict` 11 / 11.
Left for the user: Q15 (now including the account-level values), C1-C5, the S07 / C4 merge, S21 acceptance, the S25
registry sign-off and the `.claude/` proposals, as listed in design §0b.

Why: review round 12 compared design v11.1 with the working tree. Architecture consistency fell from 84 to 77, mostly
because the design document lags the build. The findings are in the scratchpad file review12_findings.txt, at
C:\Users\Thanc\AppData\Local\Temp\claude\bf90cb8a26adfd44\26e2fd39-4604-4976-8a69-152e97972080\scratchpad\review12_findings.txt.

Goal: make `docs/design/automated-risk-manager.md` (becomes v11.2) describe what is built.
- **Status and header:** fix the status line ("built behind flags, off by default; S39 pending the PR3 probe").
- **Code references:** re-check every `file:line` citation against the working tree, and mark which ones are new code.
- **Deviations:** carry every stage deviation recorded in this plan into the design, where it changes behaviour or keys:
  - the ARM keys include `adapter_id`;
  - the Oversee unfilled-entry exception in PR1;
  - the PRE validity bound;
  - the S36b items still not wired (MD_RESET notice, uptime suspend signal, the remaining clear probes, correction
    re-checks);
  - the RAM-only items;
  - the C1-C4 defaults in use;
  - the dependency on the pending `.claude` proposals.
- **Inventory:** add the price-in-PostgreSQL inventory items round 12 named (`account_snapshots` equity and unrealised
  P&L), with their status.
- **Diagrams:** regenerate the four diagrams under `docs/design/arm/` (dot files plus PNG via Graphviz at
  `C:\Program Files\Graphviz\bin\dot.exe`) if any labelled behaviour changed.

Doc only. G green.

### S38c FIX-R12: round-12 design and code fixes (added 2026-11-17)

Status: done 2026-11-17 (design v11.3 and code; both flags stay off; nothing sends, changes or cancels a broker order;
no `.claude/` change). Built, per finding: **PRE window** `valid_until = fire + 10 s`
(`src/marketdata/timing.py::pre_valid_until_ms`; the 15m row allows 30-45 s again, a 10 s window at every offset;
`src/riskmgr/config.py`, `barclock.py`, `pipeline.py` texts, `engine.env.example`); **NO_BROKER_BAR** (new INFO engine
outcome `src/riskmgr/outcomes.py`; pipeline step (5) decides an OPEN bucket `md-bars` resolved `no_trade`: final,
recorded only, no trigger, no `commit_state`, SM11 state and `pre_basis` kept; `PipelineInput.bar_no_trade` filled by
`PipelineEvaluator(no_trade=...)` from `MdBars.coverage` in `src/runtime/optional_plan.py::_no_trade`, an unknown
bucket is never `no_trade`; `ErrorBudget.note_no_trade` neither adds nor resets the unresolved streak; render text);
**one stop basis** (`src/riskmgr/stopbasis.py::trigger_basis`, used by PRE and every bar-end event; kind (a)'s
`trigger_stop` applies `pre_basis` at PRE too; PRE with a held basis at target renders `AT_TARGET_UNCONFIRMED` with
the caveat `pre_basis_held`); **stuck frontier scoped** (`ScopeKind.POSITION`, scope id `adapter_id:position_id`;
`FrontierWatch` files each newly stuck position once per episode; `ErrorBudget.observe_frontier` files one POSITION
scope per position below `XTNL_ARM_FAIL_FRONTIER_POSITIONS_N` (default 2, 1-8) and ARM-global at it;
`scope_covers_row` / `FailedScopes.covers(position=...)` / the tracker's arming and re-arm pass the position ref;
`SpanProbePort.test_span_write(position_ids)` gets the episode's recorded positions); **AWS leader change** (the
database is tried first; the first spool append pages `critical` "ARM FAILED not recorded [ping id]";
`FailedCounters.spool_page_errors`; the new leader's zero counters stated in design §B14 and the runbook); **later
Supabase row** (`ConfigInstaller.lingering_after_ms`: a `config_hash` pause past the first install slot after the
accept, or at start-up on an older accept, pages `high` once per accepted hash); **`xtnl_arm` privileges** (migration
`0018_arm_round12.sql`: REVOKE UPDATE on `arm_decisions`, `arm_decision_handles`, `arm_correction_audit`,
`arm_config_audit`; GRANT UPDATE (`notice_id`) on `arm_decisions`; the outcome CHECK gains `NO_BROKER_BAR`, the
scope-kind CHECK `POSITION`; `src/db/arm_schema.py` `ARM_IMMUTABLE_TABLES`, `ARM_GRANTS`, `ARM_COLUMN_GRANTS`,
`OUTCOMES`, `SCOPE_KINDS`, `probe_kind_for` (POSITION only with `stuck_frontier`); the S31 self-check compares the
catalog with the new list); **generic indicators** (the engine already read only the declared `warm_counts` /
`warm_span` / `backfill_bars`; `src/marketdata/demand.py` no longer imports ATR for the bare-lease default
`DEFAULT_BACKFILL_BARS = 182`; protocol and docstrings say so); **`RELEASE_POLICY`** (`src/riskmgr/strategy.py::ReleasePolicy`
`ON_CONFIRMED_TARGET` | `NEVER` on the `SlStrategy` protocol; kind (a) `ON_CONFIRMED_TARGET`; the registry refuses a
strategy without one; (7a) only under `ON_CONFIRMED_TARGET`, a `NEVER` strategy at target with the trigger met stays
TRACKING as `AT_TARGET_UNCONFIRMED`); **hung strategy threads** (`src/riskmgr/jobs.py`: a lane whose job thread is
still running past the cap is retired (`shutdown(wait=False)`) and the version gets a fresh executor;
`MAX_ABANDONED_THREADS = 3`; a further hang keeps its lane (`executor_hung`) and raises `take_restart_required()`
once, filed by `optional_plan._after_tick` as the ARM-global `restart_required` (`ErrorBudget.restart_required`,
probe `arm_global`); `close()` also reports retired hung threads; the CPU-spin limit documented). Design
`automated-risk-manager.md` v11.3: §0b "Round-12 findings: resolved" table and deviation rows, D1 / Q4 (30-45 s),
§A3/§A4/§A5/§A6/§A7 (`no_trade` POST, generic warm-up), §B3 grants, §B4 job runner, §B5.2 PRE row, §B6.1
`RELEASE_POLICY`, §B6.2 `NO_BROKER_BAR`, §B6.3 (5), (7), (7a) and the PRE basis, §B6.6 decision-bar sequence, §B6.7,
§B9 lingering pause page, §B14 stuck-frontier row, spool and probes, §C1 0018 row, §C3; diagram
`docs/design/arm/arm_bar_pipeline.dot` / `.png` (PRE 30-45 s, valid 10 s); runbook `docs/runbooks/arm.md` (two new
pages, POSITION scope, `span_write` and `restart_required` rows). Tests: `tests/riskmgr/test_round12.py` (10:
`NO_BROKER_BAR` decides and counts nothing, a holiday gives no FAILED, `no_trade` read from the coverage map, a
weekend gives no stuck frontier, one stuck position fails only its scope and a later one its own, N positions go
global, one poisoned row leaves the others advising and its clear is refused, two covering scopes keep a position
FAILED until both clear, arming and re-arm into FAILED under the position's own scope, the spool page,
`restart_required`), `tests/riskmgr/test_pipeline.py` (+5: offset 30 s window, PRE and POST agree after a partial
tighten (long and short), PRE at target with a held basis (long and short), `NEVER` stays TRACKING (long and short),
a strategy without a policy refused), `tests/riskmgr/test_jobs.py` (+3 and the hanging-strategy test rewritten:
bound and `restart_required`, a finished abandoned thread frees its slot, a CPU-spinning job keeps event-loop lag
under 100 ms), `tests/riskmgr/test_failed_clear.py` (+1, the POSITION clear; the span probe receives the positions),
`tests/db/test_arm_round12_migration.py` (6: grants, the self-check catches a leftover UPDATE, every ARM store
statement fits the narrowed grants, CHECK sets, no column), `tests/db/test_arm_migration.py` (0017 then 0018),
`tests/armconfig/test_installer.py` (+2), `tests/marketdata/test_demand.py` (+1), `tests/marketdata/test_timing.py`,
`tests/riskmgr/test_break_even.py`, `test_config.py`, `test_stopreads.py` (offset 30), `test_barclock.py`,
`test_render.py`, `conformance.py` / `toys.py` (`RELEASE_POLICY`). G: pytest 5233 passed, 58 skipped; mypy strict
clean (301 files); `verify_execution.py --strict` 11 / 11.
Deviations: (1) the PRE formula chosen is `fire + 10 s` (no `min` with the bar end); (2) the stuck-frontier scope is a
new `POSITION` scope kind (migration 0018), with `XTNL_ARM_FAIL_FRONTIER_POSITIONS_N` (default 2) deciding when it
is global; the `span_write` probe port now receives the recorded positions but stays **not wired** (refuses
`probe_not_wired`, as S36b deviation 15), so a POSITION or ARM stuck-frontier scope still cannot be cleared until the
BarClock's store implements the rolled-back re-run; (3) the AWS fix is the page plus the documented counter reset,
not a peer replay of the spool or an unfenced `arm_failures` spool drain (both would need a `.claude/` operations §2
exception); (4) no BEFORE UPDATE trigger on `arm_decisions` (the column grant is the guard); (5) `NO_BROKER_BAR` is
not shown in a notice (recorded only, `suppressed`) and needs `md-bars`' in-memory coverage, so after a restart an
earlier `no_trade` bucket is unknown and waits for its cap as before (fail safe); `ErrorBudget.note_unresolved` /
`note_no_trade` are still not called by the runtime (S30 left the unresolved counter unwired); (6) the generic
indicator finding needed no protocol change: the code already declared `warm_counts` / `warm_span` /
`backfill_bars` per indicator (the finding's `warmup` / `clean_tail` under their built names); (7) a `NEVER` strategy
at target with the trigger met records `AT_TARGET_UNCONFIRMED` (its text says "not yet confirmed"; a policy-specific
text waits for the first such strategy); (8) the rollback-set re-base, the data-model indexes, `prior_state`, the
S36b `this_bar_pre` rebuild, the S37 Redis 7.2 check and the S38 10:16 drill were already built by earlier stages or
their review fixes; S38c adds tests only where none existed (`prior_state` with two scopes, arming under a POSITION
scope); (9) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); of the
skill's directories only `src/riskmgr`, `src/marketdata` and `src/armconfig` changed (`src/adapters`, `src/pubsub`,
`src/dto`, `src/integrity` and `src/oversee` untouched); (10) no `.claude/` change is needed: the hook's name ban and
the rules do not mention the new names; the pending S37 operations / oversee-rules proposals are unchanged.
Review fixes (2026-11-17): (a) **NO_BROKER_BAR after a PRE heads-up**: when this bar's PRE proposed a move the
pipeline folds its levels into the stored basis (`farther_basis`, as every other non-confirming bar end) and shows the
item with the reason `pre_not_confirmed` (`pipeline.NO_TRADE_PRE_NOT_CONFIRMED`; render "the heads-up was not
confirmed"); (b) **`restart_required` clear**: `ClearProbes.hung` (`probes.HungThreadsView`, wired to the job runner)
refuses the ARM-global probe `threads_hung` while `JobRunner.restart_still_required()` (abandoned plus busy lanes above
`MAX_ABANDONED_THREADS`); a `restart_required` episode without the view refuses `probe_not_wired`; the signal re-arms
once the abandoned count drops under the bound; the worker sets `busy` before it reads the ticket, so a job never
starts after its cap on a lane the loop did not retire; (c) **`span_write` probe wired**: `BarClock.test_span_write`
re-reads the recorded positions, spans their overdue bars as the pass will (`failed`), runs the write through the
BarClock's store (ARM pool, fenced) and rolls the transaction back (checks `span_write_failed`, `span_row_changed`,
`span_already_decided`, `span_rows_unreadable`); `optional_plan.LateSpanProbe` binds it once the graph is built, so
POSITION and ARM stuck-frontier scopes can now be cleared; (d) **AWS leader change**: `FailedSummary.unrecorded`
(episodes spooled after a failed PostgreSQL write) goes off-node in the dead-man beat as `arm_failed.unrecorded`
(`ArmFailedBeat.unrecorded`), independent of PostgreSQL; the receiver pages `arm_failed_unrecorded` at once and,
when a beat from another node or epoch arrives while it was outstanding, latches "ARM FAILED episode lost in a leader
change" for `arm_unrecorded_latch_ms` (1 h) or until a FAILED scope is reported again (an older engine without the
key parses as 0); the spool page itself still goes through the outbox; (e) **later Supabase row, bounded pause**:
`InstallerSettings.pause_bound_slots` (4) install slots after the lingering page, with a document installed, the
installer goes to the new state `held`: `pause_reason()` is None, advice resumes under the installed document (armed
positions keep their pins; the tracker already armed and ended under it during the pause), the accepted document
still installs once a read yields it, one `high` page "ARM pause bounded: installed configuration kept"; with
nothing installed the pause is not bounded; (f) `NO_BROKER_BAR` never breaks an at-target run
(`SQL_LAST_BAR_END_DECISION` skips it, and the notices' previous-bar memory ignores it); (g) the open POSITION
`stuck_frontier` scopes count toward `XTNL_ARM_FAIL_FRONTIER_POSITIONS_N` (`ErrorBudget.observe_frontier(
failed_stuck=FailedManager.stuck_positions())`), so positions stuck one after another still go ARM-global; (h) the
`arm failed clear` usage text names `POSITION:adapter_id:position_id`; (i) `_no_trade` uses the point lookup
`MdBars.coverage_of` (no copy of the coverage map); (j) migration 0018 run on a real PostgreSQL (a disposable local
PostgreSQL 18 cluster; production is 17): the new pg test `test_0018_round12_grants_and_checks_on_postgres` and the
0017 / S24 pg tests pass. Runbook `arm.md`: the bounded-pause page, the off-node unrecorded pages, `threads_hung`, the
span probe's checks, `span_write` no longer listed as not wired. Tests: `tests/riskmgr/test_round12.py` (+8: the
PRE basis kept by NO_BROKER_BAR (long and short), the real span probe applies a POSITION clear, the `restart_required`
clear refused while hung and applied after, the evaluator plus BarClock `NO_BROKER_BAR` path, sequential stuck
positions go global, the manager's stuck list, the off-node beat in the spool test; the holiday test now says the
streak is not counted by the runtime today), `tests/riskmgr/test_jobs.py` (+2), `tests/riskmgr/test_notices.py`
(+1), `tests/db/test_arm_store.py` (+1), `tests/armconfig/test_installer.py` (+2), `tests/deadman_receiver/
test_arm_optional_fields.py` (+3), `tests/ops/test_arm_ops.py` (+1), `tests/db/test_pg_integration.py` (+1, pg).
Still open: `ErrorBudget.note_unresolved` / `note_no_trade` remain unwired (deviation 5); a pg run in CI is not set
up (the pre-existing pg test `test_request_kinds_and_ensure.py` fails on `AsyncpgConnection.transaction`, outside
this stage).
Left for the user: approve the pending `.claude/` proposals as before; Q15, C1-C5 and the S07 / S21 gates are
unchanged.

Resolve every round-12 MAJOR finding in `review12_findings.txt` in both design and code, each with a test:
- **PRE window:** the PRE validity window must have positive length across 30-45 s. Use one documented formula (for
  example valid until fire + 10 s) and match the code.
- **No broker bar:** define the decision-bar sequence as evaluable buckets only. Add an INFO `NO_BROKER_BAR` POST
  outcome for an OPEN `no_trade` bucket. It advances the cursor and never counts toward `fail_unresolved_n`. Test: a
  holiday the broker does not trade gives no FAILED, and a weekend gives no stuck frontier.
- **One stop basis:** PRE and every bar-end event use the same stop-basis function (the farther of `S_pre` while set and
  the current S). PRE still renders an item while `pre_basis` is set.
- **Stuck frontier:** a stuck frontier on one position fails only that position's scope, and its clear probe retests
  that position.
- **Restoring `prior_state`:** do not restore it while another FAILED scope still covers the position.
- **AWS leader change:** a FAILED episode spooled to a local file must survive it. Replay from the peer, or record the
  episode in the database first and use the spool only as a fallback with a page. Document the new leader's counters.
- **Pause from a later Supabase row:** a row added after an accept must not pause all advice for hours. Scope the pause
  to the changed rows, or page at once with a bounded pause.
- **`xtnl_arm` privileges:** the role must not UPDATE immutable decision rows or audit tables. Narrow the grants with
  column-level UPDATE and a new migration, then re-run the privilege self-check.
- **Rollback set:** re-base `write_seq` when memory entries are restored into a lost store.
- **Data model:** add the lease index and stored-series index to §A3 and the code that bootstrap step (4) reads.
- **Generic indicators:** replace hard-coded ATR `length` assumptions in warm-up, backfill depth and retention fit with
  each indicator's declared window.
- **Release (7a):** strategies may opt out of it (`RELEASE_POLICY`); kind (a) keeps it.
- **Hung strategy threads:** contain them so their FAILED scope can be cleared. Document the CPU-spin limit.
- **S36b review major:** if it is still open, rebuild `this_bar_pre` from the committed PRE decision rows.
- **S37 review major:** the AWS install checks for Redis 7.2 or newer.
- **S38 review major:** make the 10:16 `redis-md` restart drill test what design §B6.6 asks for.

G green.

### S38d HARD-1: go-live hardening, decision path (added 2026-11-17, user: "Yes, harden it")

Status: done 2026-11-17 (design v11.4 and code; both flags stay off; nothing sends, changes or cancels a broker order;
the one `.claude/` change is a proposal, `docs/design/proposed-claude-changes/S38d-indicator-executor.md`). Built,
per finding: **`S_at_T` over every stop child** (`src/db/arm_versions.py::SQL_LINKED_STOP_CHILDREN`: in the same
snapshot the reader adds every reduce-only `orders_state` child linked to the position that is not terminal or last
changed after `T`; a child created after `T` has no candidate, one ended before `T` is not read; the evaluator and
the `ArmVersionReader` protocol unchanged in shape); **D4 corrections** (`src/riskmgr/barclock.py`: every final
POST / LATE / CORRECTED the term commits is kept with its parsed inputs until its `decision_cap`; every 5 s
(`BarClockSettings.correction_every_ms`, not while paused or the controls are unreadable) current hashes come from
`events_since` (restart-form cursor) or, when it expired or failed, by bucket under the keyring (new `keyring` port);
`corrections.check_decision` / `OpenChecks` (audit rows `UNVERIFIABLE` at the window end through
`insert_correction_audit`); a correction re-runs the evaluation as `CORRECTED` (a release on that bar from
`TRACKING`; `prev_final_at_target` False for a re-run) and `plan_revision` writes `revision + 1`,
`supersedes_decision_id`, `advice`, `RELEASED` / `RELEASE_WITHDRAWN` plus the re-run's `FINAL_ADJUST` /
`ADVICE_WITHDRAWN` / `HANDLES_REACHED` on the latest bar, in one fenced transaction through the sink chain;
`src/riskmgr/corrections.py` gains the stored-form parsers and `summary_of`); **MD_RESET** (`optional_plan.arm_graph`
registers `md.bootstrap.on_reset` → `ArmNotices.md_reset` for a reset whose `notice` is set); **finality**
(`src/marketdata/brokerbars.py`: `trusts_next_bucket`, False until PR3, so a later bucket alone is not live-path
finality; two equal reads `settle_ms` apart are); **release hold** (`MdBars.awaiting_verification`; the evaluator's
new `awaiting_verification` port turns a bar-end `COMPLETE` on an unverified bar into `PENDING`
`release_awaiting_verification`, `src/riskmgr/outcomes.py::RELEASE_UNVERIFIED`; no grace page for it; an `ADJUST` is
never held); **ATR fault scope** (`src/riskmgr/types.py::indicator_scope_id`: an indicator fault is
`ErrorBudget.indicator_fault`, a SERIES-kind guard-input scope `adapter:symbol:ind:<indicator_id>`, routed by
`FaultRelay.indicator`; `Needs` / `PositionSpecs` keep the guard-only specs apart; `tracker.past_guards(row)`
(`last_decided_bar` set) drops them from `series_of` and from the lease renewal, so the ATR lease is released after
bar 0; `open_series_by_adapter` skips indicator scopes; the clear probe and the clear hook understand them);
**PRE quote** (`MdHook.quote_at` over a per-market quote ring, `QUOTE_KEEP_MS` 15 s, at most 2048 entries;
`MarketDataPort.quote_at`; the evaluator reads the PRE quote at `fire_ms`); **D6 K** (`XTNL_MD_MISMATCH_K` 2-10,
`MISMATCH_K_MIN` / `MAX`; `CrossChecker` refuses `mismatch_k < 2`; env example); **md-indicators executor**
(`ThreadPoolExecutor(max_workers=max_threads)` per term, `asyncio.wrap_future`; `close()`; the runtime's
`_indicators_loop` closes it at stop and hard-exits on a hung worker; `src/riskmgr/lockout.py::FUTURES_FILES` allows
it in that one file); **decision inputs** (`src/riskmgr/evaluation.py::decision_inputs`: trigger first, every history
bar, every indicator value as an `IndicatorInput` with a keyed lineage hash, `src/riskmgr/types.py::lineage_hash`; all
in `inputs_digest`); **Q10** (`ItemBody.loosen_policy` on `COMPLETE` items from `notices.pinned_loosen_policy`;
`RenderSettings.rearm_on_loosen` removed; `LOOSENED_AFTER_COMPLETE` always tell-only). Design
`automated-risk-manager.md` v11.4: §0b round-13 table and deviation rows (PRE quote, `S_at_T`, decision inputs,
indicators), memory-only list, "Not wired yet" (MD_RESET, correction re-checks and `span_write` removed); §A4
finality, §A6 executor, §B5.2 the one valid-until formula, §B6.4, §B6.6, §B8 inputs and release text, §B14 `K`
bounds, the guard-input scope row and its effect. Tests: `tests/riskmgr/test_round13.py` (10: COMPLETE then a revised
bar withdraws the release with a CORRECTED item, an ADJUST's bar revised then revised back, the release waits for
verification, a 10 ticks/s PRE, the ATR scope long and short past bar 0 and bar 0 covered, the indicator fault's own
scope, the golden bar-0 inputs conformance, Q10 per item and from the pin, the design's one formula),
`tests/marketdata/test_round13_md.py` (7: no finality on `T + 1` alone, `awaiting_verification`, K, three quote-ring
tests, hung indicators leave the default executor responsive), `tests/db/test_arm_versions_round13.py` (8: three
golden cancel/replace cases long and short, the link filter, the SQL), `tests/runtime/test_optional_plan.py` (+1:
the MD_RESET wiring and the indicator scope hooks); updated: `test_config.py` (K bounds), `test_brokerbars.py` /
`test_crosscheck.py` (their rigs opt into the PR3-trusted `trusts_next_bucket` path), `test_arm_join.py` (quote
history, `quote_at`, `events_since`, `revise_bar`, the BarClock keyring), `test_evaluation.py` (an after-fire quote
is now `pre_quote_unavailable`), `test_tracker.py` (the ATR lease released after the arming span),
`test_arm_versions.py` and `tests/db/arm_emulator.py` (the linked-children statement), `test_render.py` (Q10),
`tests/marketdata/test_ports.py` (`quote_at`). G: pytest 5283 passed, 59 skipped; mypy strict clean (301 files); `verify_execution.py --strict` 11 / 11.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); of
its directories only `src/marketdata`, `src/riskmgr` and `src/runtime` changed (`src/adapters`, `src/pubsub`,
`src/dto`, `src/integrity`, `src/oversee` untouched); (2) the correction window is kept **in memory** per term: after
a restart or a takeover the previous term's decisions are not re-checked (their releases were held until
verification); (3) a correction re-run evaluates on the current reads and stored state (the stored `pre_basis` and
strategy state as they are now, the release bar's state `TRACKING`), not on a snapshot of the order state at the
original decision; (4) the release gate uses `md-bars`' in-memory `verified` book (review fix): after a restart (or for a
bucket the lane or a backfill made final, or one whose re-read was inconclusive) a bar is not confirmed, so a release
on it is held and the bar spanned at its cap (`release_awaiting_verification`, paged): fail safe, the position is
released only on a later, verified bar (a REOPEN re-check of such a bar drops at its deadline); (5) the indicator fault's scope is a SERIES-kind scope with an `ind` id (no new scope kind, no
migration); bar-0 positions on a FAULTED ATR stay FAILED (the plan's "or FAILED"), not `FIRST_BAR_UNKNOWN` under Q3;
(6) the indicator input's hash is a keyed hash of the value's lineage (window input digest by bar id and revision,
value revision), not `window_hash` over the window bars' content hashes (the evaluator does not hold the window
bars); (7) existing md-bars and cross-check tests opt into the PR3-trusted next-bucket finality to keep testing what
they test; the default (off) is covered by the new tests; (8) the MD_RESET wiring is tested by calling the bootstrap's
registered listener, not by a real store reset; (9) `tests/runtime/test_optional_plan.py::
test_a_reset_task_behind_a_hanging_clear_probe_is_applied_within_one_poll` was timing-flaky in this stage's runs
(about one run in three to eight; it passes alone and on a rerun): with `HangPools` every arm tick can spend up to 3 s
in `_observe`'s ARM-pool `SELECT 1`, so the clear probe may start after the test's 5 s wait. That path (`_observe`,
`FailedManager.tick`, the operator pass) is not changed by this stage; not fixed here.
Review fixes (2026-11-17, S38d review: 1 critical, 3 major, 3 minor): **guard horizon** (`tracker.past_guards(row,
guard_end_ms, guard_cap_ms=, now_ms=)`: the cursor at or after the last bar a pinned guard applies to, asked of the
guards themselves (`guard_horizon_end` over `open_bar_ends_after`, so `next_full_bucket` keeps one bar longer), **and**
that bar's `decision_cap` passed; an armed-mid-bucket position (cursor = fill bucket start, no span) keeps the ATR
lease and the indicator-scope coverage; `first_bar_end` is the evaluator's `bar_index` lookup too); **guard inputs per
bar** (`PinNeeds.guard_history` / `guard_indicators`, `tracker.guards_apply`: read and recorded only where a guard
applies, so a later decision neither reads released leases nor records them); **CORRECTION wins**
(`corrections.check_decision`: a changed input is a correction even while another is unverifiable); **positive
verification** (`MdBars.verified`, `VERIFIED_KEEP` 256 per series: the same hash or a stored revision; the evaluator's
port is now `verified=`, a COMPLETE is held unless it is True); **`S_at_T` join goldens**
(`tests/riskmgr/test_s_at_t_join.py`, 6: delayed POST, weekend REOPEN, LATE replay, long and short, through the real
BarClock and `PgArmVersionReader` over the emulator: final on the pre-replace level, `stop_basis` STREAM, no span, no
page; they fail with the linked-children read removed); **indicator timeframe** (`Evaluated.indicator_tf`, the
BarClock's re-check reads the indicator's own bucket); **audits never dropped** (`OpenChecks` keeps `(adapter_id,
position_id)`; a confirmed correction on a non-deciding position is counted `corrections_not_deciding`, kept open and
audited `not_deciding`); **index** (`0019_orders_linked_idx.sql`, post-step `TableIndex`
`orders_state_linked_stop_idx (adapter_id, linked_position_id) WHERE is_reduce_only`, built `CONCURRENTLY`). Design
§0b D4 / ATR rows, §A4 and the §B14 effect text updated. Tests: `test_round13.py` (21: armed mid-bucket long and short
through `tracker.step()`, `past_guards`, the anchor horizon, guard inputs per bar, a revised trigger with an unreadable
input, audits after leaving the live set and `not_deciding`, the indicator timeframe, three never-confirmed releases),
`test_round13_md.py` (+4), `test_corrections.py`, `test_tracker.py` (6 ARMED leases; the comment corrected),
`tests/db/test_orders_linked_idx_migration.py` (4), `test_migrate_post_step.py`, `test_arm_round12_migration.py`,
`tests/e2e/test_arm_no_market_data.py` (arms inside the fill bucket so bar 0 reads the planted ATR),
`notice_fakes.NStore.audits`. G: pytest 5310 passed, 59 skipped; mypy strict clean (301 files);
`verify_execution.py --strict` 11 / 11.
Left for the user: approve `S38d-indicator-executor.md` with the S11 / S22 proposals; S38e (operations and gates:
the remaining clear probes, retention, `redis-md` grace, the §0b gate rows) is next; Q15, C1-C5 and the S07 / S21
gates are unchanged.

Source: review round 13 MAJOR findings, in the scratchpad file review13_findings.txt at
C:\Users\Thanc\AppData\Local\Temp\claude\bf90cb8a26adfd44\26e2fd39-4604-4976-8a69-152e97972080\scratchpad\review13_findings.txt.
Resolve these in code, design and tests:
- **`S_at_T` over every stop child:** read it over every stop child ever linked to the position, live or terminal. A
  child terminal after T counts as live at T; a child created after T takes no part. Golden tests, long and short: a
  cancel/replace while a delayed POST waits, over the weekend before a REOPEN, and before a LATE replay. Each decides on
  the pre-replace level, with no FAULT span.
- **D4 corrections:** wire them. The BarClock consumes `events_since` and `corrections.check_decision`: the same id with
  a different hash gives a CORRECTED item (older bar INFO; latest bar re-proposed, or a withdrawn release). Wire the
  MD_RESET info notice. Until PR3 proves StoneX stops amending bar T once T+1 appears, count a bar final only after two
  equal reads `broker_bar_settle_s` apart. Hold a COMPLETE until the verification re-read confirms the same hash (ADJUST
  is still sent at once). Test: a bar revised after a COMPLETE never gives a silent release.
- **ATR fault scope:** a FAILED covering set depends on what the position still needs. After bar 0 is decided, the
  FirstBarGuard / ATR specs drop out of coverage and its ATR lease is released. Shorts never depend on the bid trigger
  series.
- **PRE quote:** freeze it at or before fire, from the last tick at fire, so a liquid market does not turn PRE into
  DATA_UNAVAILABLE. The design's §B5.2 valid-until formula must match S38c.
- **D6 block threshold:** K must be at least 2 (config refuses 1), so one mismatch only flags.
- **md-indicators jobs:** run them on their own bounded executor, not the shared default one (§B4).
- **Decision inputs:** record every input (trigger bar, guard bars, indicator ids and hashes), so corrections and the
  Phase 2 contract see all of them.
- **Q10 policy:** `rearm_on_loosen` is pinned per position and wired, not a node-global setting that is never set.

G green.

### S38e HARD-2: go-live hardening, operations and gates (added 2026-11-17)

Status: done 2026-11-17 (design v11.5; both flags stay off; nothing sends, changes or cancels a broker order; the one
`.claude/` change is a proposal, `docs/design/proposed-claude-changes/S38e-rules-arm-exceptions.md`). Built:
**clear probes** (every FAILED scope can be cleared): `MdBars.probe_reread` / `probe_fresh_bar`
(`src/marketdata/brokerbars.py`: broker reads through the same `BrokerBarsPort`, also while the series is FAILED,
never written or counted; reread = the newest `rollback_n` closed buckets pass every hard check and the D6 cross-check
via the new record-nothing `CrossChecker.check_only`; fresh bar = the newest closed bucket final on two equal reads
`settle` apart, or a trusted clear partial flag, within `post_grace`), `src/riskmgr/probes.py::MdBarsSeriesProbe`
and `VersionConformanceProbe` (the pinned class's registry declaration checks run as one job on the version's lane:
`version_unknown` first `arm_repin`, `job_lane_hung`, the job's own fault codes, `conformance`), wired in
`optional_plan.arm_graph` (`ClearProbes` has no None port); **D7 triggers**: md-bars hands each bucket counted
unresolved at its cap to the error budget's `note_unresolved` (clean bars `note_final`, `no_trade` buckets
`note_no_trade`) through `FaultRelay` (new `BucketTarget` forwarding, dropped and counted while unbound), and a consumer
re-fetch the broker can no longer serve (`unfillable`, or beyond `history_reach_ms`) gives `refetch_failed` once per
series episode; **retention**: the new `RETAIN_BROKER` script (`src/marketdata/store/scripts.py`) in md-writer's
retention pass trims every stored BROKER series (bootstrap-tracked or written, demanded or not) below 1 year or its
widest declared backfill (`broker_keep_ms` from the demand book), and `md:bars_rev` / `md:quarantine` below 7 days
(never the rollback set or `md:events`); `DemandBook.memory_symbols` reads md-writer's measured memory ratio (the
formula cut to the admitted symbols plus the room left below the target); **redis-md grace**: bootstrap failures from
an unreachable, timing-out, `LOADING` or `BUSY` store (`UNREACHABLE_FAILURES`) are never counted toward the 3-in-a-row
fault, the service backs off on `unreachable_streak` (1 s doubling to 30 s) and `fail_md_down_s` decides; **gates**:
`src/riskmgr/golive.py` (the go-live table: 6 wired, 13 open after the review fixes), `optional-bootstrap` leaves the ARM idle with one `ARM
not started` page while a gate is open and not in `XTNL_ARM_GOLIVE_ACCEPT` (an unknown or wired id pages `ARM go-live
setting`), the offline `python -m src.ops arm gates` (exit 3 while enabling would be refused), S39 guarded by a test
over `BAR_CAPABLE_ADAPTERS`; **indicators**: `IndicatorSettings.indicator_inputs` (False) refuses `IndicatorRef`
(multi-input) specs at registration, the limit stated in design §A6; **docs**: design §0b (gate rows, "Go-live
gates", "Round-13 findings: operations and gates", deviations, not-wired list, the `clean_mids` inventory row), §0 Q15
(c), §A5, §A6, §A7, §B3, §B14 (rows and Clearing), §C2 Local flags (the S07 effect instead of "exactly as before",
`XTNL_ARM_GOLIVE_ACCEPT`); the S37 proposal no longer records Q15 (a) as a user override and names `RETAIN_BROKER`;
`src/db/arm_versions.py`'s docstring points at the new proposal; runbooks `arm.md` (gates, the probe refusal checks,
the two pages) and `marketdata.md` (retention, redis-md down or loading); `deploy/env/engine.env.example` /
`local-live.env.example` (`XTNL_ARM_GOLIVE_ACCEPT`, the S07 wording). Tests: `tests/marketdata/test_s38e_md.py` (16:
`RETAIN_BROKER` script, guard and builder; the writer trims an undemanded stored series and keeps a wider backfill; the
measured ceiling; md-bars' notes, `refetch_failed` both ways and none without a re-fetch; both probes and their codes;
`check_only` records nothing; the relay's bucket notes; the DAG refusal), `tests/riskmgr/test_s38e.py` (12: the
series and version probes, over the real md-bars and job runner; every probe kind maps to a port; two unresolved
buckets → SERIES `unresolved` through the real budget, a clean bar between resets it, `refetch_failed` reaches the
budget; the gate verdict, S39 guard and `arm gates` exit codes), `tests/marketdata/test_bootstrap.py` (the counted
fault now needs an answering store; down / `LOADING` never counted; the service back-off sequence),
`tests/runtime/test_optional_bootstrap.py` (open gates leave the ARM idle with one page; `ARM_CHANNELS` now accepts every
open gate explicitly), `tests/runtime/test_optional_plan.py` (no None probe port; the notes and memory wiring). G:
pytest 5351 passed, 59 skipped; mypy strict clean (302 files); `verify_execution.py --strict` 11 / 11.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); of
its directories only `src/marketdata` and `src/riskmgr` changed (`src/adapters`, `src/pubsub`, `src/dto`,
`src/integrity`, `src/oversee` untouched); (2) BROKER retention is its own script (`RETAIN_BROKER`) in the same
retention pass, not a branch of `RETAIN`; it does not trim the indicator values or checkpoints of a series no longer
demanded (they are floored only by that indicator's next `IND` write; listed in design §0b); TICK series no longer
demanded were already trimmed by `RETAIN`; (3) the demand ceiling uses the measured `used_memory` headroom, not a
subtraction of stored-but-undemanded symbols; (4) the runtime `strategy_conformance` check is the registry's
declaration checks (REASONS, DECISION_METRICS, RELEASE_POLICY, guard EVENTS) run as one job on the version's lane; the
full conformance suite stays a build-time test; (5) the series reread reads the newest `rollback_n` closed buckets
from the broker (not the stored `md:rb` members); (6) no break-glass forced clear was added (every probe is wired);
(7) the indicator framework took the plan's second option: multi-input indicators are refused and the limit
documented, the canonical DAG read is not built; (8) the user acceptance is the local setting
`XTNL_ARM_GOLIVE_ACCEPT` (per gate id, no wildcard); "the ops enable path" is `arm gates` plus the runbook step, and
the engine itself refuses; S39 is enforced by a test, since S39 is a code change; (9) existing ARM runtime tests now
accept every open gate through the shared `ARM_CHANNELS` fixture; (10) `tests/runtime/test_optional_plan.py::
test_arm_plan_binds_the_budget_and_reports_failed` shows an intermittent teardown `RuntimeWarning` (`coroutine
'InMemoryOverseeDb.add' was never awaited` at `aclose`, about one run in three; the test passes): the stop path of
the in-memory Oversee store, not a path this stage changed.
Review fixes (2026-11-17): `probe_fresh_bar` now takes the next bar end `T'` after it starts, sleeps to
`T' + p50 + settle` and passes only when bucket `T' - timeframe` is final (two equal reads `settle` apart, or a trusted
clear partial flag) by local `T' + post_grace` (an already-closed bar never passes it; a calendar that raises refuses);
`store_error` left `UNREACHABLE_FAILURES` (a store answering NOPERM / another SHA / no run_id counts toward
`bootstrap_failed` again), and a raised error naming `LOADING` / `BUSY` maps to `store_loading` / `store_busy`;
`probe_reread` reads `XTNL_MD_ROLLBACK_N` buckets (`BrokerBarsSettings.rollback_n`, set by `from_md`); new OPEN gates
`s25_registry`, `conformance_probe_partial` (the version probe runs declaration checks only, not
`propose` / `at_target` / guards: recorded instead of built) and `entry_time_source` (golive.py and design §0b);
`arm gates --env-file PATH` reads the engine's env file and the report names `settings_source` (runbook `arm.md`
uses it); `CrossChecker.check_only` no longer counts a malformed TICK reference; stale references fixed
(`optional_plan.py` comment, golive docstring test path, design §A13 MD_RESET wiring, §J Q15 readers, §B14 Clearing
rows). Tests added: the fresh-bar refusal when only an older closed bar is final and when finality comes after
`post_grace`, the configured rollback set, `NOPERM` trips `bootstrap_failed`, a raised `BUSY` is uncounted, the env-file
verdict, `check_only`'s exception path; the streak-reset test now runs past B + 2TF's cap (caught by mutation of
`note_final`). G: pytest 5356 passed, 59 skipped; mypy strict clean (302 files); `verify_execution.py --strict` 11 / 11.
Left for the user: approve `S38e-rules-arm-exceptions.md` (with the S11 / S37 / S38d proposals); answer or accept
the open go-live gates (`python -m src.ops arm gates`; design §0b "Go-live gates"); Q15, C1-C5 and S07 are unchanged.

Same source file. Resolve:
- **FAILED scopes:** every one can be cleared. Wire the remaining clear probes. Both D7 triggers that are never
  produced today must fire. Gaps that cannot be closed are listed in a go-live gate.
- **Retention:** enforce BROKER bar retention (1 year) in RETAIN, and match the sizing and the demand ceiling.
- **redis-md unreachable or still loading:** retry with back-off and give a bounded grace before ARM-global FAILED, so
  that a start-up `LOADING` reply does not fail the ARM within seconds.
- **Gate rows in design §0b:** S39 and `XTNL_ARM_ENABLED=1` are refused until each go-live item is wired (or explicitly
  accepted by the user). The ops enable path checks the gates.
- **Design wording:** remove "flags off = exactly as before" while S07 is uncommitted, and state the S07 effect
  instead. Fix the Q15 reader list (no nonexistent `sod_marks.clean_mids` reader). Fix the S37 rules proposal so it
  does not record Q15 (a) as a user override.
- **Rules proposals:** add `.claude` proposal files naming the ARM unfenced writes and REPEATABLE READ snapshots as
  operations §2 exceptions. Reconcile code that breaks current rules, or list it as needing the proposal.
- **Indicator framework:** make DAG inputs canonical from the store, not process memory, at least for the declared
  window. Otherwise document the limit and refuse multi-input indicators until it is built.

G green.

### S38f HARD-3: round-14 safety fixes (added 2026-11-17, user: "Yes, fix those 3")

Status: done 2026-11-17 (design v11.6; nothing committed; both flags stay off; nothing sends, changes or cancels a
broker order). Built: **(1) nothing unreviewed goes live at the next start:** `src/db/migrate.py`
(`XTNL_MIGRATIONS_ACCEPTED_UPTO`, default 14, `accepted_upto_from_env` (a bad value is the default, fail closed),
`migrate(accepted_upto=, held=)`, `held_line`; the entry point holds back every pending migration above the level,
prints one line naming them and exits 0; the post step now runs only for applied migrations, so a held-back 0016 CHECK
is never validated); `deploy/local/start-xtnl.ps1` step 6 logs the level (the env file and the Windows User
environment reach the child as before) and `deploy/systemd/xtnl-migrate.service` documents it; `deploy/env/
engine.env.example`, `local-live.env.example`, `local.env.example` set `XTNL_MIGRATIONS_ACCEPTED_UPTO=14` and
`XTNL_OVERSEE_STOP_AT_ENTRY=0`; the S07 rule behind `XTNL_OVERSEE_STOP_AT_ENTRY=1` (`src/oversee/stop_rule.py`
(new; `configure_stop_rule`, read once in `build_engine`, which logs the rule in force), `src/oversee/risk.py`
(process-wide `stop_at_entry()`, `_pre_s07_detail` = commit 31d9b58's rule, `effective_stop_detail(..., at_entry=)` /
`effective_stop(..., at_entry=)`: None = the process setting); the ARM passes `at_entry=True` (`src/riskmgr/
evaluation.py`, `tracker.py`) and `src/riskmgr/golive.py::verdict` counts `oversee_c4` as accepted only with
`XTNL_OVERSEE_STOP_AT_ENTRY=1`); the old schema: `src/db/schema_compat.py` (new; catalog detection of the 0015 columns
at pool creation, `LegacyRewriter` / `LegacyConnection`: the four core writes naming 0015 columns (orders insert and
update, `event_log`, instruments) lose the column and its argument, the `ORDER_COLS` / `INSTRUMENT_COLS` / dashboard
order lists select NULL; process note `optional_payload_stored()`), `src/db/pg.py` (`AsyncpgSource(wrap=)`,
`create_asyncpg_source(schema_check=True)`, `with_schema_compat`), `src/dashboard/server.py` (its pool too),
`src/dto/versioning.py::without_optional_payload` (+ `src/dto/__init__.py`), `src/pubsub/orchestrator.py`
(`optional_payload=`: on an older schema every entity row (before validation) and every instrument refresh enters in
its pre-0015 shape, so stored checksums are the pre-0015 ones), `src/runtime/session.py` (passes it),
`src/db/lifecycle.py` (step 5, the ARM-notice expiry, skipped while 0017's tables are absent). **(2) `xtnl_arm`
cannot clear or rewrite a FAILED episode:** `src/db/migrations/0020_arm_hardening.sql` (REVOKE UPDATE on
`arm_failures`, column UPDATE only on `counters`, `position_ids`, `last_ping_at_ms`, `ping_count`,
`last_probe_at_ms`, `last_probe_result`; BEFORE UPDATE trigger `arm_failures_guard`: identity columns never change,
`position_ids` only grow, a cleared episode is final, only the table owner role clears; refusals 42501),
`src/db/arm_schema.py` (`ARM_FAILURES_IDENTITY`, `ARM_FAILURES_UPDATABLE`, `ARM_FAILURES_GUARD`,
`SQL_ARM_FAILURES_GUARD`, `guard_problems`, `privilege_problems(..., triggers=)`, the S23 grant list), the S31 self-check
in `src/runtime/optional.py` reads the trigger. **(3) D7 error budget survives restarts and takeovers:** 0020's
`arm_budget_counters` (SELECT, INSERT, UPDATE for `xtnl_arm`); `src/db/arm_budget_store.py` (new; `PgArmBudgetStore`
fenced upsert in one transaction, `must=True`; `PgRestartHistory` reads `leader_terms` ended `lost` / `init_failed`
on the engine pool); `src/db/arm_ports.py` (`BudgetCounterRow`, `ArmBudgetStore`, `RestartHistoryPort`);
`src/riskmgr/budget.py` (`durable_rows` / `restore`: the `unresolved` streaks, the `md_down` / `pool_down` time
counters in whole 10 s steps, the restart watermark; `note_restarts` and the ARM-global reason `unclean_restarts`;
`reset(scope, now_ms)` moves the watermark); `src/riskmgr/failed.py` (`budget_store=`, `restarts=`: restore at
`start()` and retried each tick, nothing written before it succeeds; write only what changed (a counter back at rest
as count 0); the restart check once per term after the first live-position read, armed = a position in ARMED,
TRACKING, ADVICE_OUTSTANDING, SUSPENDED or FAILED); `src/riskmgr/config.py` (`XTNL_ARM_FAIL_RESTARTS_N` 3 (2-10),
`XTNL_ARM_FAIL_RESTART_WINDOW_S` 1800 (300-7200)); `src/runtime/optional_plan.py` wires both. Design: header, §0b
("What turns each on, and in what order", "On a 0014 schema", "Round-14 safety fixes", gate `oversee_c4`), §B3, §B14
(durable counters, unclean restarts, the AWS takeover sentence), §C1 (0019 and 0020 rows), §C2 (local flags);
runbooks `arm.md`, `marketdata.md`, `single-pc-autostart.md` ("Reviewed-change switches"), `install-node.md`.
**User approval:** `docs/design/proposed-claude-changes/S38f-round14-switches.md` (operations §12, oversee-rules §5,
the S11 `riskmgr.md`). Tests: `tests/db/test_migrate_accepted.py` (20), `tests/oversee/test_stop_rule_flag.py` (20:
flag off equal to commit 31d9b58's `effective_stop` over 600 generated entity sets, MissingStopLossRule never
`stop_kind_inconsistent` off, the ARM call sites, the `oversee_c4` acceptance), `tests/db/test_schema_compat.py` (11 +
1 `pg`: a fake 0014 connection refusing every new column runs the entity, `event_log` and instrument writes and the
local-state read; every core SQL constant scanned; the orchestrator's pre-0015 shape; a `pg` test on a scratch schema
held at 0014), `tests/db/test_arm_hardening_migration.py` (8 + 1 `pg`: as `xtnl_arm` re-keying `scope_id` /
`scope_kind` / `probe_kind` / `reason` and clearing are refused 42501, a shrinking `position_ids` too, the owner clears
once and never re-opens), `tests/riskmgr/test_budget_durable.py` (11: K-1 unresolved buckets, a takeover, one more ->
SERIES FAILED with the ping; the in-memory control; written only on change and never before the restore; time counters
restored without the downtime; a fault crashing the engine every 2 minutes reaches ARM FAILED `unclean_restarts`; a
clean restart (`switchover`, `abdicate`) does not count; not without armed positions or outside the window; a clear
never re-trips on the same restarts); updated: `tests/conftest.py` (new; the stop rule off per test,
`stop_at_entry_on`), `tests/oversee/test_risk.py`, `test_stop_detail_sweep.py` (S07 tests run with the switch on),
`tests/db/test_arm_migration.py` (grants after 0020), `tests/riskmgr/test_s38e.py`, `tests/runtime/
test_optional_bootstrap.py` (`oversee_c4` with the switch; the fake pool answers the trigger read),
`tests/dashboard/test_dashboard.py` (the pool's schema read). G: pytest 5435 passed, 61 skipped;
mypy strict clean (305 files); `verify_execution.py --strict` 11 / 11.
Deviations: (1) the `live-trading-server` skill could not be invoked by the build agent (user-invocable only), as in
earlier stages; (2) the old-schema support rewrites statements by exact text in one module (`schema_compat.py`) instead
of a second set of constants in every store; a scan test keeps every core statement covered; (3) the stop rule is a
process-wide setting read once at start (`set_stop_at_entry`), not an `OverseeConfig` field, because the ARM and
Oversee share `effective_stop_detail`; the ARM passes `at_entry=True` explicitly and needs the switch on (gate);
(4) the 0020 trigger refuses changes to `edge_code`, `ping_id`, `entered_at_ms` and `epoch` too, and keeps
`position_ids` grow-only (the review's suggestion), and treats a cleared episode as final for every role;
(5) the durable counters cover the budget's own counters (the `unresolved` streaks, `md_down`, `pool_down`, the
restart watermark); the market-data side's K streaks (`invalid_bar` in md-bars, the D6 `source_mismatch` streak in the
cross-check) are still per term (design §B14 says so); (6) unclean restarts are read from `leader_terms` (the engine's
own record) rather than counted by a new marker, and checked once per term at the FAILED manager's start; a PC sleep
longer than the fence counts as one; (7) the restart counter is not stored as a count but as a watermark (the
history itself is durable in `leader_terms`).
**Review fixes (2026-11-17, S38f review: 2 major, 4 minor; all fixed):** (M1) a failed first counter read no longer
loses the stored streaks: `ErrorBudget.restore(rows, open_scopes)` merges a late restore with what the term counted
meanwhile (stored streak ending before the term's first bucket: counts add, the stored start kept, the fault filed at
once at N; time counters add; a final bar, a usable observation or a clear seen first supersedes the stored counter),
and `FailedManager` restores only after the open episodes are read; (M2) SAFE_HOLD preempts the restart counter
(design §B14: no ARM runs during the hold; after `resume()` the ARM counts the crashes before the hold and goes
FAILED if they still reach N in the window; the engine core never writes ARM tables) and `SQL_UNCLEAN_ENDS` leaves out
the term the brake aborted (`init_failed`, `reached_leader_at` NULL, `ended_at = started_at`); the crash-loop test now
drives the real `leader_init_fence` (`LeaderTerms` fake in `tests/riskmgr/test_budget_durable.py`); (m3) an accepted
clear writes its counter reset (`ErrorBudget.reset_rows`: SERIES streak 0; ARM time counters 0 and the restart
watermark at the clear time) in the clear's own transaction (`ArmFailedClearPort.clear(..., reset=)`,
`PgArmFailedClear` upserts fenced, `must=True`), and a restored counter at its threshold is filed only while its scope
is open; (m4) 0020 grants `xtnl_arm` UPDATE only on `last_ping_at_ms`, `ping_count`, `last_probe_at_ms`,
`last_probe_result` (`ARM_FAILURES_UPDATABLE`, new `ARM_FAILURES_EVIDENCE`; the `pg` test refuses `counters` /
`position_ids` updates); (m5) `golive.verdict(..., stop_rule_in_force=)`: the engine passes
`src.oversee.risk.stop_at_entry()`, so `oversee_c4` is refused while Oversee runs the pre-S07 rule even with the
setting 1 (the offline `arm gates` report still reads the setting); (m6) the old-schema scan covers every `src` module
but the market-data / ARM-only ones (`CORE_EXCLUDED`). Tests added: `tests/riskmgr/test_budget_durable.py` (18 now),
`tests/db/test_arm_failures_store.py` (the clear's reset in its transaction, rolled back with it),
`tests/oversee/test_stop_rule_flag.py` (the stop rule in force); the ARM emulator gained `arm_budget_counters`;
`tests/runtime/test_optional_bootstrap.py` and `test_optional_plan.py` run with the S07 rule in force (as
`build_engine` sets it from their `XTNL_OVERSEE_STOP_AT_ENTRY=1`). G after the fixes: pytest 5444 passed, 61 skipped;
mypy strict clean (305 files); `verify_execution.py --strict` 11 / 11; the `pg` marker tests still not run here.
Left for the user: approve (or edit) `S38f-round14-switches.md`; decide when to raise `XTNL_MIGRATIONS_ACCEPTED_UPTO`
(20 applies 0015-0020) and to set `XTNL_OVERSEE_STOP_AT_ENTRY=1` (after confirming C4); the `pg` marker tests
(`tests/db/test_schema_compat.py`, `tests/db/test_arm_hardening_migration.py`) were not run here (no
`ORDER_SERVER_TEST_DSN`).

The source is the review round 14 findings in the scratchpad file review14_findings.txt, at
C:\Users\Thanc\AppData\Local\Temp\claude\bf90cb8a26adfd44\26e2fd39-4604-4976-8a69-152e97972080\scratchpad\review14_findings.txt.
Fix exactly these three in code, design and tests.

1. **No unreviewed change goes live at the next engine start.** Today `deploy/local/start-xtnl.ps1` runs
   `python -m src.db.migrate` at every start, so migrations 0015-0019 and the uncommitted S07 Oversee change would go
   live at the next restart. Make both opt-in:
   - **Migrations:** `src/db/migrate.py` applies a migration above the accepted level only when the local setting
     `XTNL_MIGRATIONS_ACCEPTED_UPTO` (an integer, default 14) allows it. Otherwise it logs one clear line naming the
     pending migrations and exits 0, so the start continues. The PC start script and the AWS units pass the setting
     through. Document it in `deploy/env/*.example`.
   - **S07 Oversee stop rule:** `effective_stop_detail()` counting a STOP child at or beyond entry is behind
     `XTNL_OVERSEE_STOP_AT_ENTRY=1`. It is off by default, which keeps the exact pre-S07 behaviour. Invoke the
     `live-trading-server` skill first if you can; this touches `src/oversee`.
   - **Old schema:** with every flag off and the schema at 0014, the engine must start and behave as before. Every write
     path that touches columns added by 0015-0019 must tolerate their absence, using schema detection at start-up, or
     the new code paths must stay unused while flags are off. Prove it with a test against a 0014-level schema; use the
     `pg` marker plus a fake when no database is set.
   - **Design:** update §0b and the runbooks: what turns each on, and in what order.
2. **`xtnl_arm` cannot clear or rewrite a FAILED episode.** Add a new migration (0020) behind the same accepted-level
   gate:
   - narrow `arm_failures` privileges to column-level UPDATE on non-identity columns only;
   - add a BEFORE UPDATE trigger refusing changes to identity columns (scope, scope id, reason, `opened_at_ms`,
     `failure_id`, `probe_kind`) and to `cleared_at_ms` / `clear_request_id` unless the engine-owner role makes them.
   Extend `privilege_problems()`, the S23 grant list and the `pg` test (refused with 42501, or a trigger error).
3. **D7 error budget survives restarts and takeovers.**
   - Persist the error-budget counters (per scope: consecutive counts, streak start, last event) durably in PostgreSQL:
     a new table in 0020, written fenced and only on change. The FAILED manager restores them at bootstrap. They
     survive a `redis-md` loss.
   - Count unclean engine restarts and takeovers themselves: N within a window (local setting, default 3 in 30 min)
     while positions are armed enters ARM-global FAILED with the immediate ping.
   - Tests: a fault that crashes the engine every 2 minutes reaches FAILED; counters restore after a takeover; a clean
     restart does not count.

G green. Nothing is committed.

### S38g HARD-4: round-14 decision path, liveness and sleep (added 2026-11-17, user: "Yes, fix the 10")

Status: done 2026-11-17 (design v11.7; both flags stay off; nothing sends, changes or cancels a broker order; the
`.claude/` diff waits for approval: `docs/design/proposed-claude-changes/S38g-round14-sleep-and-liveness.md`). Built:
(1) **final advice versus AT_PRE:** `DecisionMemory.final_advice_target` (the proposed stop of the newest final
`ADJUST` of an earlier bar, `src/db/arm_store.py::SQL_FINAL_ADVICE_TARGET`, read with the other two memory reads),
`PipelineInput.final_advice_target`, `src/riskmgr/pipeline.py::answers_final_advice` (ADVICE_OUTSTANDING, this bar's
PRE proposed a move, `S_at_T` nearer entry than that PRE's `S_pre` and at or short of the final target): this bar's
PRE levels are dropped for the whole bar-end evaluation (no AT_PRE, no PRE_NOT_CONFIRMED, no `pre_basis`), caveat
`moved_on_final_advice` (rendered text in `render.CAVEAT_TEXT`); (2) **PRIOR_VERSION direction:**
`SQL_STOP_NONCOUNTING` returns the order-state fields of the newest non-counting and the newest untimed version;
`stopbasis.handle_at_bar_end(..., side=, entry=)` takes the farther from entry of the prior counting level and every
newer non-counting level, a newer version holding the handle with no level (or an unknown direction) is
`StopUnavailable(stop_at_bar_end)`; (3) **liveness:** `src/riskmgr/liveness.py` (new: `overdue_positions`,
`undecided_open_bars`, `LivenessWatch`) run by the `arm-status` loop every 5 s (`OptionalRuntime.liveness_pass`):
armed rows (ARMED / TRACKING / ADVICE_OUTSTANDING, not FAILED-held, no pause) with more than 2 OPEN bars past their
cap undecided, or no loaded schedule (`CalendarGrid.has_schedule`), feed the dead-man `arm_overdue_positions`,
`arm_status.overdue_positions` and the `overdue` metric view, and page once per episode; a BarClock with no completed
pass for 60 s (`BarClock.last_pass_ms`), an arm-tracker with none for 120 s (`ArmTracker.last_pass_ms`) and an unsent
ARM notice older than 15 min page once per episode; the tracker's old count is `ArmTracker.unarmed` (view and counter
`unarmed`), its rows `live_rows()`; (4) **PC sleep:** `WallMonoDetector(suspend_after_ms=)` reports both clocks moving
more than `SUSPEND_AFTER_MS` (10 s) as a `ClockStep` of kind `suspend` (the BarClock via
`BarClockSettings.suspend_after_ms`, md-writer via its constructor; both set by the runtime);
`EngineSession.add_suspend_listener` (the uptime tracker's `resume_after_sleep` now tells listeners first: the
BarClock's `SuspendLatch`, `MdWriter.note_suspended`); `LeaseKeeper` measures the gap between its own iterations
(monotonic and wall) and names a fence after a gap over 5 s (`process suspended N s`), calling
`on_suspended_fence` first, bound by `build_engine` to `src/runtime/sleeplog.py::SleepFenceLog.record` (new; node-local
`sleep_fences.jsonl` next to the spill file, times only, 24 h); the FAILED manager reads restarts through
`SleepAwareRestarts` (the first unclean end within 15 min after a sleep fence is left out), and the `arm` loop pages
each sleep restart once ("ARM: engine restarted after a PC sleep"); design §A7 "PC sleep" documents the path, its
costs and the drill; (5) **design inventory:** §C1 rows 0015-0020 in order, the "Locks" paragraph names 0019's post-step
index, every "0015-0017" scan text says "from 0015 on", and the schema scan test globs every migration from 0015 on
(0020's `arm_budget_counters` columns allow-listed). Design: v11.7 (§0b "Round-14 findings: decision path, liveness
and sleep", §A7, §B6.3, §B6.4, §B8, §C1, §C2, gate `uptime_suspend` widened). Runbook `arm.md`: the five new pages.
`deploy/observability/metrics.md`: the `overdue` / `unarmed` views. Tests: `tests/riskmgr/test_round14.py` (3: the
final-advice golden through the runtime join, on time and with the POST DELAYED past the next PRE; the loosened
untimed amend gives BELOW_TRIGGER), `tests/riskmgr/test_pipeline.py` (+6: long and short golden, a move past the
target, a partial move), `tests/riskmgr/test_stopbasis.py` (+6), `tests/db/test_arm_versions.py` (+2, three updated
to the new marker levels), `tests/riskmgr/test_liveness.py` (6), `tests/runtime/test_sleep_path.py` (10: suspend
detection, the named and recorded sleep fence, the sleep log, sleep vs crash restarts in the budget, the 60 s drill
after a releasing bar end, the BarClock latch, the session's listeners), `tests/db/test_arm_schema_no_market_data.py`
(+1), `tests/runtime/test_optional_plan.py` (the wiring), and fakes updated (`tests/db/arm_emulator.py`,
`tests/riskmgr/notice_fakes.py`, `tests/runtime/test_arm_controls_view.py`). G: pytest 5482 passed, 61 skipped; mypy strict clean
(307 files); `verify_execution.py --strict` 11 / 11; the `pg` marker tests not run here (no `ORDER_SERVER_TEST_DSN`;
`tests/db/test_pg_integration.py` was updated for the new marker levels).
Deviations: (1) the `/live-trading-server` skill could not be invoked (user-invocable only), as in earlier stages;
`src/riskmgr` and `src/marketdata` changed without it; (2) the suspend trigger is opt-in on `WallMonoDetector`,
`BarClockSettings` and `MdWriter` (the runtime sets it; unit rigs that jump a fake clock by minutes keep steps only);
(3) a PC sleep's restart is **not** counted toward `unclean_restarts` (the plan's "counts once ... only if it really
was one": a sleep fence is a suspend, not a crash), matched as the first unclean term end within 15 min after a
recorded sleep fence; an unreadable record counts every end; (4) the sleep restart is reported by a page when the ARM
starts, not by an ARM notice; with the ARM off the record is only logged and kept 24 h; (5) not built from the review
text beyond the stage: `arm_position_faulting`, the 5-minute `redis-md` unreachable page, the
`xtnl_arm_last_final_decision_age_seconds` series and its off-node alert, and the persisted verification result
(§B8 As built and gate `uptime_suspend` say so; `corrections_restart` stays open); (6) the gate `uptime_suspend` stays
OPEN (the job wall-clock timing across a sleep is not changed).
Review fixes (2026-11-17, stage review: 1 major, 5 minor, all fixed): (1) **liveness after a long downtime**: the
arm-tracker's rows keep the old cursor for up to 10 s after the new term's BarClock wrote the DOWNTIME span, so a
position first seen overdue now counts (dead-man, `arm_status`, metric, page) only once a BarClock pass that started
after that sighting has completed and tracker rows read after it (`ArmTracker.rows_read_ms`) still show it overdue,
or after 2 min (`LivenessWatch.confirm`, `OptionalRuntime.overdue_now`); (2) **stalled-loop page after a hold**: the
age is measured from no earlier than the time the watch first saw the loop allowed to run (`running` turning True,
forgotten when it turns False); (3) **withdrawn final advice**: a REOPEN / CORRECTED re-run takes the advice target
only from the newest final bar-end decision of an earlier bar when it is an `ADJUST`
(`SQL_PRIOR_FINAL_DECISION`, `DecisionMemory.prior_final_adjust`, `evaluation.final_advice_target_of`); a POST / LATE
keeps the newest earlier `ADJUST` behind the ADVICE_OUTSTANDING check; (4) **PRIOR_VERSION**: `SQL_STOP_NONCOUNTING`
returns every non-counting version newer than the newest counting one (timed at or before `T`, and untimed), at most
`NONCOUNTING_LIMIT` (32) per handle, one more making the handle unresolved; an ended level-less newer version taking
no part is documented as accepted (§B6.4); (5) **suspend versus stall**: the lease keeper confirms a suspend with
`system_suspend_clock` (Windows `GetTickCount64` / `QueryUnbiasedInterruptTime`, Linux `CLOCK_BOOTTIME` /
`CLOCK_MONOTONIC`); any other long gap is an ordinary fence (`process stalled N s`) that still counts toward
`unclean_restarts`; (6) **tests of the sleep report**: the restart history reads the sleep record in a thread. Tests:
`tests/runtime/test_liveness_restart.py` (2, new: a 3 h old cursor and a restart give no page and a dead-man count of
0; a row that stays undecided still counts), `tests/riskmgr/test_liveness.py` (+2), `tests/db/test_arm_store.py` (+1),
`tests/riskmgr/test_pipeline.py` (+2: the CORRECTED golden, long and short), `tests/riskmgr/test_stopbasis.py` (+2),
`tests/db/test_arm_versions.py` (+2), `tests/runtime/test_sleep_path.py` (the stall and the suspend clock),
`tests/runtime/test_optional_plan.py` (+1: a real `SleepFenceLog` pages one sleep restart once, none on the next
start, and the bound restart history leaves out the `lost` end 40 s after the fence). Design §A7, §B6.3, §B6.4 and
§B8 As built updated; the proposed `.claude/` patch names the suspend clock. G: pytest 5496 passed, 61 skipped; mypy
strict clean (307 files); `verify_execution.py --strict` 11 / 11.
Left for the user: approve (or edit) `S38g-round14-sleep-and-liveness.md`; run the `pg` marker tests against the dev
compose.

Source: review round 14 findings in the scratchpad file review14_findings.txt, at
C:\Users\Thanc\AppData\Local\Temp\claude\bf90cb8a26adfd44\26e2fd39-4604-4976-8a69-152e97972080\scratchpad\review14_findings.txt.
Fix each in code, design and tests:

1. **Final advice versus AT_PRE.** A stop move that answers an outstanding final ADJUST (one from POST, LATE, REOPEN or
   CORRECTED that has not been withdrawn, with the position ADVICE_OUTSTANDING) is never discounted by AT_PRE. The
   PRE_NOT_CONFIRMED path is not taken and `pre_basis` is not stored. The basis carries the caveat
   `moved_on_final_advice`.
   Golden tests, long and short, including a POST that is DELAYED until after the next bar's PRE.
2. **PRIOR_VERSION direction.** When a newer version that does not count exists (history or watermark), the trigger uses
   whichever of the prior counting level and that newer level is farther from entry. The marker read returns the level,
   which is order state, not a market price. If the two levels differ in a way that cannot be resolved, the decision is
   DATA_UNAVAILABLE(stop_at_bar_end). §B6.4 names the watermark case.
   Golden test: a loosened amend seen only through an untimed REST row gives BELOW_TRIGGER, not ADJUST.
3. **Liveness pages.** Build the promised liveness pages:
   - armed positions that silently stop being evaluated (no final decision for more than 2 OPEN bars), as a local page
     and through the dead-man field;
   - BarClock and tracker stamps going stale;
   - the arm-relay backlog age.
   Each one is tested.
4. **PC sleep.**
   - Detect a Windows sleep or resume from the wall clock against the monotonic clock jump, plus the term fence, on the
     node / session path.
   - Document the restart path the engine actually takes after a sleep longer than the fence.
   - Make sure the ARM treats that path correctly: the bars it missed become a span, it counts once as an unclean
     restart only if it really was one, and any sleep restart is reported.
   - Test the path.
5. **Design inventory.** Add migration 0019 (and 0020) to the design's migration inventory, and correct the text about
   which migrations the design has.

G green.

### S38h HARD-5: round-14 market data and extensibility (added 2026-11-17)

Same source. Fix each in code, design and tests:

1. **Tick-hook circuit.** When the market-data hook's circuit trips, mark the missed interval as a receipt gap for every
   affected symbol, so TICK bars that overlap it are `partial` or `blind`, never `complete`.
   Test: a trip in mid-bucket leaves that bucket not complete.
2. **Indicator history.** Stored indicator history follows bar revisions and late buckets outside the in-memory window.
   The value and its checkpoints are recomputed from the store over the declared window and re-stored, and an
   `indicator_updated` revision is emitted.
   Test: a revision older than the in-memory window updates the stored value.
3. **Indicator trimming.** Trim stored values of indicators that are no longer demanded after a grace period (a local
   setting, default 7 days without a lease) through RETAIN, with sizing to match.
   Test: a parameter change leaves no orphan series after the grace period.
4. **Bar spec checks.** FirstBarGuard v1 declares the bar specs it supports, and the strategy declares the specs it
   supports. Admission and accept refuse any pairing that does not match, such as kind (a) on a spec other than 15m,
   with a clear reason. A second bar spec can never give a silently wrong first-bar test.
5. **Generic decision metrics.** Store decision metrics generically: guard and strategy metrics in one
   `metrics_json` (codes and ratios only, never prices) on `arm_decisions`, through a new migration behind the
   accepted-level gate. The notice renderer and Phase 2 read them by guard or strategy kind; FirstBarGuard's ratio and
   verdict are no longer hard-coded.

G green.

### S37 DEP-2: AWS deploy, observability, runbooks, rules, diagram

Status: done 2026-11-17 (deploy assets, metrics, runbooks and diagram; both flags stay off and `xtnl-redis-md` is
installed but not enabled; the `.claude/` rule diff waits for the user's approval). Built: `deploy/redis/redis-md.conf`
(`127.0.0.1:6381`, AOF `everysec` with `aof-load-truncated`, no RDB, `maxmemory 1gb` `noeviction`, `dir
/var/lib/xtnl-redis-md`, `aclfile /run/xtnl-redis-md/users.acl`, no `user` line), `deploy/systemd/xtnl-redis-md.service`
(`User=redis`, `Type=notify`, `MemoryMax=2G`, runtime and state directories, `ExecStartPre=+... -m src.ops.md_acl
--env-file /etc/xtnl/engine.env --out /run/xtnl-redis-md/users.acl --group redis`, exit 78 for a configuration error and
75 for a secrets source that cannot be read now (retried), `RestartPreventExitStatus=78`, hardening;
no engine unit depends on it), `src/ops/md_acl.py` (the boot-time ACL renderer: env file plus the secrets source
(`XTNL_SECRETS_SOURCE=aws-sm` laid over it), each password taken from its URL (`XTNL_MD_REDIS_URL` = `md_writer`,
`XTNL_MD_REDIS_READER_URL` = `md_reader`, both required to point at `127.0.0.1:6381` and to differ), `#<sha256>`
entries and the design's rules only, written atomically 0640; refusals name variables only),
`deploy/redis/redis-tls-proxy.conf` second section `[xtnl-redis-md]` `${NODE_IP}:6382` → `127.0.0.1:6381` with the
dashboard section's mutual TLS, `deploy/redis/redis.conf` and `xtnl-redis-tls-proxy.service` (comments only);
observability: `gen_alerts.py` / `metrics.md` (+ regenerated rules, alarms and agent config) gain
`xtnl_md_redis_used_memory_ratio`, `xtnl_md_shed_step`, `xtnl_md_queue_depth{queue}`, `xtnl_arm_positions{view}`,
`xtnl_arm_paused`, the `hook` / `crosscheck` components of `xtnl_md_events_total`, the `tracker` / `barclock` /
`stopreads` / `notices` / `evaluation` / `pre_skipped` components of `xtnl_arm_events_total`, the alert
`XtnlRedisMdMemoryHigh` (70 %, warning, absent-safe; `off-node-alerts.md` section) and a "Design §C2 metric list" table
mapping every §C2 item to its series or saying where to read it; the exporter side: `src/observability/metrics.py`
`CONTRACT` and `src/runtime/optional_plan.py` (`MD_EVENT_COMPONENTS`, `ARM_EVENT_COMPONENTS`, `PRE_SKIPPED_COMPONENT`,
the new samples; reason codes only as `event`); runbooks `docs/runbooks/marketdata.md` (every md page with its fix,
enable / disable on AWS, md requests, the AOF-aside procedure, the HMAC key rotation: add the key, `md rehash`, wait
for `rehash: complete` plus one correction window, then retire the previous key) and `docs/runbooks/arm.md` (every ARM
page with its fix, the FAILED section with each probe kind, what passes it and the command to run first, the refusal
codes, configuration, pause / disarm, enable / disable), `docs/runbooks/README.md`, `install-node.md` step 7b
(install, never enable); `docs/architecture/generator/gen_phase1_arch.py` page 6 "Market data & ARM" (data flow,
SM12, SM11; layout checks pass) and the regenerated `phase1_ingestion_arch.drawio` (pages 1-5 unchanged).
**User approval:** `docs/design/proposed-claude-changes/S37-rules-redis-md-and-arm.md` (operations §1, §2, §3, §4,
§5, §9, §11, §12, §13, §16; tier-persistence §1-§4; dto-contracts §6, §7; adapter-isolation §2, §3; oversee-rules
§10). Tests: `tests/deploy/test_md_arm_deploy.py` (44: redis-md.conf, the unit and its ACL path, no engine
dependency, install-not-enable, the proxy section, redis.conf comments only, one ACL rule list across renderer /
dev compose / PC script, every §C2 item has a row whose series are defined metrics and real counter fields, the
memory alert, an AST scan that every page title the md / ARM code sends is in the runbooks, the FAILED section covers
`PROBE_KINDS` and `FIRST_COMMANDS`, the AOF-aside and rotation order, the diagram regenerates identically with no
layout problem), `tests/ops/test_md_acl.py` (15), `tests/runtime/test_optional_plan.py` (the new series and
components; a non-code PRE reason is never exported), `tests/deploy/test_deploy_assets.py` (`RUNBOOK_FILES`). G:
pytest 5122 passed, 55 skipped; mypy strict clean (301 files); `verify_execution.py --strict` 11 / 11.
Deviations: (1) the ACL is rendered by a new `src/ops/md_acl.py` run as the unit's root `ExecStartPre` (the plan said
only "rendered from the secrets source at boot"); it derives the passwords from the two URLs, so no separate password
secret exists and the store cannot disagree with the engine; (2) beyond `metrics.md` / `gen_alerts.py`, the stage
changed the exporter (`metrics.py` `CONTRACT`, `optional_plan.py` samples) so the new rows are exported, as the
contract test requires (`PENDING_EXPORT` stays empty); §C2 items with no series yet (writer lag as a lag, md
generation and store class, rollback-set depth, consumer cursor lag, skew, POST decision latency, close source, last
POST age, config age) and the partial ones are marked in the table with where to read them; (3) one new off-node
alert, `XtnlRedisMdMemoryHigh`, mirrors design §A2's 70 % page; (4) the diagram gains a new page 6 instead of
redrawing pages 1 / 3; its boxes are not in `measured.json` yet (default heights; re-measure with `measure.html`);
(5) the new rule files `marketdata.md` / `riskmgr.md` and the `CLAUDE.md` line stay in S11's proposal and are not
repeated; (6) the `/live-trading-server` skill was not invoked: no file in `src/adapters`, `src/pubsub`, `src/dto`,
`src/integrity`, `src/oversee`, `src/riskmgr` or `src/armconfig` changed; the review fix in `src/marketdata/writer.py`
(`WriterStatus.memory_ok`) was made without it, because the skill is user-invocable only and the build agent cannot
invoke it (as in earlier stages); (7) `arm.md` states that
the `series_reread`, `fresh_bar`, `span_write` and `strategy_conformance` clears are still refused `probe_not_wired`
(S36b deviation 15); (8) Redis 7.2 or newer is required for `WAITAOF` (stated in the config; Ubuntu's own package is
older); (9) no PC change (S36 holds it).
Review fixes (2026-11-17): (a) `install-node.md` prerequisites install Redis 7.4 from `packages.redis.io` with a
pinned, held package version and check `redis-server --version`; `marketdata.md` "Enable" starts with that check
(7.2 or newer; Ubuntu 24.04's own 7.0 rejects the md ACL's `+waitaof` / `+client|setinfo`); (b) a secrets source
that cannot be read now exits `md_acl` 75 (EX_TEMPFAIL, the unit retries) instead of 78: `src/secrets/source.py`
gains `SecretsUnavailable(SecretsError)` for an AWS / IMDS / network read failure (an IAM denial included), and any
other exception from the source maps to 75 too; 78 stays for a missing or wrong URL, env file or source; the unit's
comment now says a failed `ExecStartPre` may still be retried until the start limit; (c) `WriterStatus.memory_ok`
and `optional_plan._md_samples` export `xtnl_md_redis_used_memory_ratio` only while the last memory read succeeded
(never a stale value); (d) the security group opens 6382 from `VPN_CIDR` only once redis-md is enabled
(`install-node.md` prerequisites, `marketdata.md` "Enable" step 0); (e) `marketdata.md` "redis-md password
rotation" (secret, restart `xtnl-redis-md`, restart the engine, standby first, then switch over and repeat);
(f) the runbook page scan fails on any title it cannot read (a variable or an f-string without a literal head)
outside the `WRAPPERS` allow-list, and reads a local bound only to literals. Tests: `test_md_arm_deploy.py` (+4:
Redis version in both runbooks, 6382, rotation order, the strict scan), `test_md_acl.py` (+4: exit 75 not 78),
`tests/secrets/test_source.py` (an AWS read failure is `SecretsUnavailable`, a malformed secret is not),
`tests/marketdata/test_writer.py` (+1), `tests/runtime/test_optional_plan.py` (no stale gauge). G after the fixes:
pytest 5131 passed, 55 skipped; mypy strict clean (301 files); `verify_execution.py --strict` 11 / 11.
Left for the user: approve (or edit) the S37 rule proposal; on AWS, enabling follows `marketdata.md` "Enable" and
`arm.md` once the secrets exist.

* **C / M:** `deploy/redis/redis-md.conf` and `deploy/systemd/xtnl-redis-md.service` (`MemoryMax=2G`, bound to
  `127.0.0.1:6381`; installed but not enabled, enabled by the operator with `XTNL_MD_ENABLED=1`), ACL rendered
  from the secrets source at boot; `deploy/redis/redis-tls-proxy.conf` second section `[xtnl-redis-md]` 6382 → 6381;
  `deploy/redis/redis.conf` unchanged except comments; `deploy/observability/metrics.md` and `gen_alerts.py` (the §C2
  metric list); `docs/runbooks/arm.md` and `docs/runbooks/marketdata.md` (every page with its fix; the FAILED section
  with each probe kind and the command to run first; the AOF-aside procedure; the HMAC key rotation: add the new key,
  run `md rehash`, wait for `md show` to report completion and one correction window, then retire the previous key,
  R11-M13); `docs/runbooks/README.md`. **User approval:** `.claude/rules/operations.md`, `tier-persistence.md`,
  `dto-contracts.md`, `adapter-isolation.md` and `oversee-rules.md` §10 per §C2 (oversee-rules §2, §5, §6 were done in
  S07). `docs/architecture/generator/gen_phase1_arch.py` (market-data
  layer, ARM, SM11, SM12), then the regenerate command from `CLAUDE.md`.
* **Tests:** `tests/deploy/` asset tests for the new files; metrics contract test.

### S38 E2E: end to end, failure drills, no-market-data test, load test

Status: done 2026-11-17, review fixes applied 2026-11-17 (tests only; no `src/` file changed, both flags stay off, nothing sends, changes or cancels a
broker order). Built: `tests/e2e/arm_rig.py` (the end-to-end rig: the real md store on fakeredis + lupa or the dev
`redis-md` as `md_writer` (the S16 writer, bootstrap, hook and `MdPort`; the demand book and the indicator reads are
the only stand-ins, the latter serving a planted value), the real `PgArmStateStore`, `PgArmVersionReader` and
`PgArmOutboxInserter` over `ArmEmulator` plus the ARM outbox INSERT (`E2eEmulator`, every statement's arguments
kept), `event_log` versions written from real DTOs through `dto_json` / `version_labels`, the real `ArmTracker`,
`PipelineEvaluator`, `JobRunner`, `BarClock`, sink chain and `ArmNotices` driven as the runtime's loops drive them
(`guarded_step`, counted notice / tracker errors, a lost fence propagates), a term builder for restarts and
takeovers), `tests/e2e/test_arm_e2e.py` (7: arm → POST → PRE `ADJUST` → POST `ADJUST` queued in the ARM outbox →
the user moves the stop → PRE silent → POST `COMPLETE`, disarm applied then moot, no HTTP request; every adapter REST
client is `ReadOnlyTransport` (AST: the one `reset_http_client` call site wraps it, cTrader has no HTTP client, the
md / ARM trees import no network module) and a production-built StoneX client refuses every order-changing path
before the wire; no adapter or md port has an order-changing method; PR1 (the read `XTNL_ARM_STOP_READS=1` turns on): a stop moved
between stream frames is read by `StopHandleReadPortImpl` over the orchestrator and
`MockBrokerAdapter.fetch_active_stop_orders` and seen by `effective_stop_detail` and the `S_at_T` function
(`stop_at_bar_end` over `event_log`) at the same level, before and after the move; both flags `0`: the
real `EngineSession` replay scenario unchanged, no task, no factory call, no hook, no `md:*` key, no `arm` row, reads
only from the broker; the same scenario on the dev `redis-md` under marker `md`), `tests/e2e/test_arm_no_market_data.py`
(4: every statement argument the ARM pool received, every ARM table, every log record, page and request answer, and
the outbox bodies unsent, as inserted, finished `sent` and finished `expired` by the real `ArmRelay` carry no market
value (bar OHLC both sides, quotes, the planted increment and indicator quantum and value), none of their digit
runs and none of them scaled to an integer (an `int` argument or a bare integer in a string equal to a value ×
10**k, k = 0..8); decision rows keep `trigger_met` / `borderline` only, every decimal written is order state or an R figure
quantised to 0.01; the scanner finds planted leaks, a scaled integer one included), `tests/e2e/test_arm_drills.py` (7: a 30 s
PostgreSQL outage (passes fail and are counted, nothing written or paged, the POST lands once on time after);
`redis-md` restarted at "10:16" during backfill with the 10:15 bar decided `ADJUST` (design §B6.6: in-place
bootstrap, new generation, the queued backfill bars, the 10:15 bar again with the same OHLC among them, written after
it and read back through `MdPort.bars` under the new generation; no `CORRECTED` item, no revision, no new outbox item
for 10:15 and its row unchanged; the next POST normal); engine and `redis-md` restarting together (new term over the
same tables and AOF store, no re-arm, no duplicate row or notice); takeover (the new leader boots its md writer with
the epoch-8 token, the old md writer's next flush is refused `NO_WRITER` and the old leader's PostgreSQL write is
`EpochMismatch`; the new one decides the bar once); PC sleep (two hours: one `downtime` span, the bar after the wake
decided); the md hook raising on every tick in the real session (scenario unchanged, the guard trips once, one page,
the price path keeps writing, the calendar sees every pushed tick); the Oversee relay never touches the run's `arm`
rows and the ARM relay never an Oversee row, an unsent one included), `tests/marketdata/test_load.py` (2, marker `md`: one symbol at full
v1 retention through the real `FLUSH` / `IND` scripts; 16 ATR(14) series in the real `md-indicators` through 96
closes, every 8th revising the bar 10 buckets back, with a lag probe; each `md` test deletes every key it wrote, the
store-wide `md:writer`, `md:boot`, `md:events` and registries included, so the `md` tests run in any order). Results recorded in design §A5: **57.7 MB per symbol** (107,520 bar + 70,080 indicator members +
200,000 ticks), **about 476 MB for 8 symbols** plus `md:events`, worst event-loop lag **29.9 ms** (limit 100 ms).
G (after the review fixes): pytest 5148 passed, 58 skipped; mypy strict clean (301 files); `verify_execution.py
--strict` 11 / 11 (the `md` marker tests were not re-run after the review fixes: no dev `redis-md` in this run). Markers
run once against the dev compose (`ORDER_SERVER_TEST_DSN`, `ORDER_SERVER_TEST_REDIS`, `ORDER_SERVER_TEST_REDIS_MD`,
`-m "integration or pg or md"`): 39 passed, 1 failed, the failure pre-existing and outside S38
(`tests/db/test_request_kinds_and_ensure.py::test_sixty_unhandled_requests_never_hide_a_reset_task_on_postgres`, an
S10 test calling `conn.transaction()` on `src.db.pg.AsyncpgConnection`, which has no such method).
Deviations: (1) no `pg` marker test was added for the ARM path: the e2e runs the real ARM stores on the SQL emulator
(the real statements already run on PostgreSQL in the S24 `pg` test); (2) the tracker's account images are built from
DTOs (`FakeSnapshot`), not read by `PgTrackerSnapshot` (S36b tests its SQL); the market-data demand book and the
indicator reads are stand-ins (the latter on purpose, to plant an indicator value); (3) the first-bar test does not
run in the e2e (the position is armed late, `first_bar_ratio` stays empty); (4) the 30 s outage, sleep and restart
drills drive the loops by hand (the FakeClock), not the `optional_plan` tasks; the sleep drill moves wall and monotonic
clocks together (the uptime suspend signal is not wired, S36b deviation 15); (5) the sizing fill writes no
checkpoints, revisions or forming bars, and its indicator values are synthetic members of the engine's encoding;
(6) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only); no file in the
skill's directories was changed; (7) no `.claude/` change and no new `verify_execution.py` check are needed;
(8) the operator-request answers scanned by the no-market-data test are the tracker's answers as handed to its answer
port (the `Recorder` stand-in), not an `operator_requests` payload over the emulator, and the md `arm_status` record
goes to the S16 `Records()` fake, not the scanned emulator (both carry ids, statuses and reasons only); (9) PR1 calls
the `S_at_T` function (`stop_at_bar_end` over `event_log`) directly: the ARM pipeline with
`ArmSettings(stop_reads=True)` is not run in the e2e.
Left for the user: the pre-existing S10 `pg` test failure above; the dev `redis-md` container started for the `md` run
was removed again (tmpfs, nothing kept).

* **C:** `tests/e2e/test_arm_e2e.py` (MockBrokerAdapter, fakeredis or real `redis-md` under the `md` marker,
  `pg_emulator` or real PostgreSQL under `pg`): arm → PRE → POST ADJUST → the user moves the stop → COMPLETE; nothing
  reaches the broker: every adapter REST client is `ReadOnlyTransport` and no denied path is attempted (R11-m); with
  `XTNL_ARM_STOP_READS=1`, a stop moved between stream frames is seen by Oversee and the ARM at the same level (design
  §C3 PR1); both flags `0`: no new task, no `redis-md` connection, no ARM row, and the existing e2e suites unchanged;
  `tests/e2e/test_arm_no_market_data.py` (every ARM column, log record, page body, `operator_requests` payload, and
  unsent, expired and finished outbox bodies including the INSERT payload: nothing gives a bar close more finely than
  `trigger_met` and `borderline`; a planted instrument increment and indicator quantum appear nowhere, R11-M14);
  `tests/e2e/test_arm_drills.py` (30 s PostgreSQL outage, `redis-md` restart at 10:16 during backfill, engine and
  `redis-md` restarting together, takeover, PC sleep, market-data hook raising, Oversee relay never touching `arm` rows);
  `tests/marketdata/test_load.py` (marker `md`: filled to full retention, sizing recorded in design §A5; recompute for 8
  symbols keeps event-loop lag under 100 ms).
* **Acceptance:** G with the `md` and `pg` markers run once against the dev compose; results recorded in the design.

### S39 BRK: StoneX broker bars and PR9 enablement (after the PR3 probe)

Status: done 2026-11-17 (nothing committed; both flags stay off; nothing sends, changes or cancels a broker order;
the `.claude/` change is a proposal, `docs/design/proposed-claude-changes/S39-stonex-broker-bars.md`). Built:
`src/adapters/stonex/adapter.py` (`price_bars_supported` true and `fetch_price_bars`: the confirmed `barhistory` /
`barhistorybetween` GETs under `rest_get_only()` through the adapter's `ReadOnlyTransport` and REST budget at the
caller's priority; `BarDate` = bucket start; no side tag, so `BrokerBar.side` stays None and the side is the
request's, which §A4 check (3) / `split_response` already accept; a timeframe without a StoneX interval answers "not
supported"; the S03 `probe_price_bars` is removed); `src/adapters/stonex/bars.py`, `rest.py`, `base.py`,
`src/dto/bars.py` (docstrings: endpoints confirmed, the time convention, no side tag); `src/adapters/stonex/probe_bars.py`
(reads through `fetch_price_bars`; `--check-stops` / `run(check_stops=True)` runs only `check_stop_children`: the
working STOP children linked to open positions (`fetch_positions`, `fetch_open_orders`) against
`fetch_active_stop_orders`, reported as counts and order ids only, `conclusive` False without a child; the item is in
`NOT_MEASURED` until it runs); `src/riskmgr/golive.py` (`pr3_finality` WIRED; new OPEN gates `pr3_stop_children`
and `pr3_roll_prints`; `claude_rules` names the S39 proposal); `src/ops/arm.py` (`BROKER_BAR_ADAPTERS = {stonex}`,
`bar_capable_adapters()`, and `BAR_CAPABLE_ADAPTERS` derived from the gate table: `{stonex}` only once no S39
blocker is OPEN, so today it stays empty; `arm gates` reports `bar_capable_adapters`); `src/marketdata/config.py`
(the settle / lag rows name the PR3 measurement; values unchanged, 10 s and 5000 ms); defaults unchanged:
`XTNL_STONEX_PRICE_UNFILTERED` 0 (filtered delivery), `XTNL_ARM_STOP_READS` 0, `trusts_partial_flag` and
`trusts_next_bucket` False. Docs: design status, §0b (S39 paragraph, gate rows, stop list row, not-wired list, the
pending proposals list), §A4 (the PR3 finality result), §A11 PR3 (results) and PR9; runbook `arm.md` (S39 gates and
`--check-stops`); env examples' comments (`engine.env.example`, `local-live.env.example`); the diagram tooltip
(generator, regenerated). Tests: `tests/adapters/test_stonex_broker_bars.py` (7: the PR3 field list decoded with
synthetic values, start-dated and side-less; count / range GETs and parameters, no denied path, no other HTTP client
built; the request side passes check 3 and the other side fails it; md-bars' port reads StoneX at `BARS` and
answers "not supported" for an unknown timeframe; the gate states; the bar-capable set follows the gate table),
`tests/adapters/test_probe_bars.py` (+4: `--check-stops` against the mock and the fake CIAPI, inconclusive and
failing reads, the CLI flag), `tests/adapters/test_price_bars_capability.py` (StoneX is no longer the base default;
the read tests call `fetch_price_bars`), `tests/riskmgr/test_s38e.py` (`pr3_finality` wired, the two new gates open).
The transport deny-list test (`test_stonex_rest_mapping.py`) passes unchanged. G: pytest 5509 passed, 61 skipped;
mypy strict clean (307 files); `verify_execution.py --strict` 11 / 11.
Deviations: (1) `stonex` is **not** a member of `BAR_CAPABLE_ADAPTERS` yet: the S38e guard
(`test_s39_waits_for_every_gate_it_does_not_close_itself`, design §0b) refuses an adapter there while any gate other
than `pr3_finality` is OPEN, and the user-decision gates (`oversee_c4`, `s21_c1`, `q1_q2`, `c2_c3_c5`,
`claude_rules`, `s25_registry`) and the Phase 1 gaps are still open; the build cannot accept them. The set is derived
from the gate table instead, so StoneX joins it (and `arm config accept` takes StoneX-scoped assignments) once those
gates are wired or recorded `accepted` in `golive.py`. The adapter capability itself is on: with `XTNL_MD_ENABLED=1`
md-bars can read StoneX bars for a demanded series, and the arm-tracker's `bar_sides` (`price_bars_supported`) now
sees StoneX as bar-capable, but the ARM stays idle while any gate is open and not accepted; (2) the bar decoding
stays in `src/adapters/stonex/bars.py` (the S03 deviation), not `mapping.py`: no side or bucket field needed a new
mapping (the side is request-derived, `BarDate` was already read as the bucket start), so `mapping.py` is unchanged;
(3) `probe_price_bars` is removed rather than kept beside `fetch_price_bars` (the probe report's `bar_reader` now says
`fetch_price_bars`); (4) next-bucket finality was not added as an option: the default (two equal reads) stands and
`trusts_next_bucket` stays False everywhere; (5) `RANGE_CAP` (4000) stays unconfirmed (the probe saw no truncated
range answer); (6) the first bar read of a symbol also resolves its market id inside the caller's REST class (`BARS`
for md-bars), as the probe read did; (7) the `/live-trading-server` skill could not be invoked by the build agent
(user-invocable only); the changes follow `.claude/rules/adapter-isolation.md` and `dto-contracts.md` (only
docstrings changed in `src/dto`; `src/pubsub`, `src/integrity`, `src/oversee` untouched); (8) the design version
number is not bumped (S38h is not built yet); the design header names S39 instead. Pre-existing and unrelated: the
intermittent teardown `RuntimeWarning` in `tests/runtime/test_optional_plan.py` (S38e deviation 10).
Review fixes (2026-11-17): (major) the bar-capable derivation moved to `src/riskmgr/golive.py`
(`BROKER_BAR_ADAPTERS`, `bar_capable_adapters()`; `src/ops/arm.py` re-exports them) and the runtime arm-tracker's
`bar_sides` (`src/runtime/optional_plan.py::OptionalRuntime._bars_capable`) now needs `price_bars_supported` **and** membership in it,
so an assignment with no adapter can no longer arm a StoneX position through the runtime while admission still
treats StoneX as not bar-capable (`XTNL_ARM_GOLIVE_ACCEPT` never makes an adapter bar-capable;
`tests/runtime/test_s39_bar_gate.py`: every open gate env-accepted, a wildcard assignment, a StoneX position ->
`bar_sides` empty, `UNSUPPORTED(no_broker_bars)`; with the S39 blockers closed the same position arms); (minor) the
range form keeps only bars whose bucket starts inside `[from_ms, to_ms]` (the whole-second GET widens the window;
truncation counted before the filter); `--check-stops` reads the StopOrders attached to `openpositions` rows on their
own (`StoneXAdapter.fetch_attached_stop_ids`, GET only, ids only), reports both sources apart (`attached_*`) and is
conclusive only with at least one attached child compared and no open position without a stop child, otherwise
`not_conclusive` names why and the CLI says not to set `XTNL_ARM_STOP_READS=1` from it; new StoneX bar tests (range
parameters with non-whole-second bounds and the filter, `truncated` for a short count and a capped range answer,
`PriceBarsDecodeError` without values, a POST inside a bar read refused by `rest_get_only` before it is sent) and a
`--check-stops` test where the only child is in `activestoplimitorders` (not conclusive). Deviation (9), recorded by
the review: the Ops line leaves "the stop-read and roll items as their own gates", but `pr3_stop_children` and
`pr3_roll_prints` are S39 blockers (not `closed_by_s39`) and, as open gates, in the `XTNL_ARM_ENABLED=1` verdict, so
an `XTNL_ARM_GOLIVE_ACCEPT` list complete before S39 must now also name them: both are PR3 measurements the S39 gate
was to supply and did not, and the gate table keeps one fail-closed rule for every open item (design §0b "Go-live
gates"). The `/live-trading-server` skill again could not be invoked by the fixing agent (user-invocable only); the
adapter changes are GET-only reads and a local filter, nothing narrowed. G after the fixes: pytest 5521 passed, 61
skipped; mypy strict clean (307 files); `verify_execution.py --strict` 11 / 11.
Left for the user: approve `S39-stonex-broker-bars.md` (with the S03 proposal); run
`.venv/Scripts/python.exe -m src.adapters.stonex.probe_bars --check-stops` in your own shell (`STONE_X_*` set, engine
stopped) while a position with a stop is open, to decide `pr3_stop_children` / `XTNL_ARM_STOP_READS`; the roll-window
comparison for `pr3_roll_prints`; accept or wire the other gates S39 waits for (`python -m src.ops arm gates`).

* **Gate:** the user has run `.venv/Scripts/python.exe -m src.adapters.stonex.probe_bars` with the `STONE_X_*`
  environment variables and shared the report (timings, flags and field names only).
* **M:** `src/adapters/stonex/rest.py` (the confirmed read-only bar endpoint(s), the range form if supported),
  `src/adapters/stonex/mapping.py` (`BrokerBar` mapping with the side and bucket fields the probe found),
  `src/adapters/stonex/adapter.py` (`fetch_price_bars`); design §A4 / PR3 (p50 / p99 lag, `broker_bar_lag_ms`,
  filtered or unfiltered, `TickDateRaw` resolution, whether manual stop moves stream, `LastChangedDateTimeUTC` on
  amendments, `roll_affected`, UNSUPPORTED sides); defaults for `XTNL_STONEX_PRICE_UNFILTERED` and
  `XTNL_ARM_STOP_READS` set from the findings.
* **Tests:** mapping fixtures built from the probe's field list (synthetic values); the transport deny-list test
  (`test_stonex_rest_mapping.py:166`) still passes.
* **PR3 probe results.** The user ran the probe on 2026-11-17, live_readonly, EURUSD. The reports are in the user's
  %TEMP% folder: `xtnl-pr3-bars-1794912799195.json` (1 round, 60 s and 900 s) and `xtnl-pr3-bars-1794913604369.json`
  (4 rounds, 900 s). Both hold no prices.
  - **Supported:** 1m and 15m bars, bid and ask, in both count form and range form (range not truncated, no off-grid
    bars). The reader is the adapter's `probe_price_bars`; its endpoints are now confirmed for the real
    `fetch_price_bars`.
  - **Fields:** `PriceBars[]`, `PartialPriceBar`, `bar.BarDate`, `bar.Open/High/Low/Close`, `bar.Volume`.
  - **Time convention:** `BarDate` is the bucket START (all votes; partial bars on the grid).
  - **Side:** no side tag on bars. The side comes from the request, which §A4 check (3) must accept as request-derived.
  - **Finality:** bar T was present, with bucket T+1 already present, at +5 s; the same at +30 s and at +300 s.
    15m bid and ask: 4 of 4 samples each, p50 = p99 = 5 s, never_final 0. 1m: 1 sample each, same result.
    - Set `broker_bar_lag_ms` / `XTNL_MD_BROKER_BAR_LAG_MS` to about 5000 and `broker_bar_settle_s` to 10 (the
      placeholders now confirmed).
    - Keep the S38d two-equal-reads finality rule. The probe shows no amendment after T+1, which satisfies the
      "unless PR3 proves" condition, so next-bucket finality may be allowed as an option. Default: keep two equal
      reads; the cost is small.
  - **Checks failed:** 0 (high/low bounds, non-positive, off-grid).
  - **Ticks:** all broker-timed, receipt minus broker p50 about 0.2 s and p99 about 0.65-0.8 s. Delivery is filtered
    (`XTNL_STONEX_PRICE_UNFILTERED` off).
  - **Not measured:** roll prints, manual stop moves on ORDERS, `LastChangedDateTimeUTC` on amendments, stop children
    of open positions in `activestoplimitorders`.
    - `XTNL_ARM_STOP_READS` stays 0 (off) until a stop-children check exists. Add a read-only `--check-stops` option
      to the probe that lists, by id only (never prices), whether the stop children of open positions appear in
      `activestoplimitorders`, for the user to run later.
    - `trusts_partial_flag` stays False.
* **Ops:** add `stonex` to `BAR_CAPABLE_ADAPTERS` in `src/ops/arm.py`, so accept no longer refuses StoneX-scoped
  assignments. Close the S39 go-live gate in design §0b, leaving the stop-read and roll items as their own gates. Update
  the design's PR3 row with these results.

### S39b GATES: the six engineering go-live gates (added 2026-11-18, user: "Build the 6 gates")

Status: done 2026-11-18, review findings fixed the same day (nothing committed; both flags stay off; no migration; nothing sends, changes or cancels a
broker order; `python -m src.ops arm gates` shows all six `wired`, 13 wired and 8 open in all; the `.claude/` change
is a proposal, `docs/design/proposed-claude-changes/S39b-six-gates.md`). Built:
(1) **`uptime_suspend`:** `src/riskmgr/jobs.py` (`JobRunner.note_suspended`, `suspend_after_ms`, `monotonic`;
the job is awaited through an `asyncio.Event` so the cap never cancels it: a cap that expired across a suspend (the
signal, or the job's monotonic elapsed time past cap + 10 s) gets a fresh awake cap, up to `MAX_SUSPEND_REWAITS`
(3); a job that then finishes is a normal result, one that raised is `job_suspended` (never counted, the circuit
untouched), one still running a full awake cap after the resume is a counted `job_timeout`), `src/riskmgr/
barclock.py` (a suspend from its detector or the latch calls `JobRunner.note_suspended` and gives every bar that
ended while asleep the `DOWNTIME` reason, `_missed_while_suspended`; a `job_suspended` result is a pending bar with
`DOWNTIME`, counter `jobs_suspended`, never `job_faults`), `src/runtime/optional_plan.py` (`JobRunner(suspend_after_ms=
SUSPEND_AFTER_MS)`; the session's suspend listener also tells the job runner). (2) **`receipt_gap_feed`:**
`src/pubsub/orchestrator.py` (`on_stream_frame` constructor hook, called adapter-wide on every prices-stream
HEARTBEAT while SUBSCRIBED and not lost, guarded and throttled like `on_price_subscription`), `src/runtime/
session.py` (`set_md_frame_hook`, `_on_stream_frame`), `src/marketdata/clockstep.py` (`RECEIPT_GAP_MS` 20 s,
`ReceiptGapDetector.set_keepalive`), `src/marketdata/ticks.py` (`MdHook.watch_receipt_gaps`; a seeded market's watch
starts at the bind), `src/runtime/optional.py` (`_start_md` turns the watch on for every adapter and binds
`note_frame`). (3) **`replay_rerun`:** `src/riskmgr/rerun.py` (new: `AsOfMarketData` (point-in-time bars and
indicators, no quote, `demand` / `refetch` refused), `AsOfDecisions`, `AsOfCalendar`, `rerun_one` over the real
`PipelineEvaluator` and `JobRunner` at `now = decided_at_ms`, `compare`: MATCH, DIFF(outcome), UNVERIFIABLE with
`not_ready:*`, `job:*`, `not_final`, `no_digest`, `key_changed`, `inputs_differ`, `order_state_changed`,
`exit_quote_not_recorded`), `src/ops/arm_rerun.py` (new: `Rerunner` builds the evaluator over the CLI's one
connection (an `EngineDb` without an epoch: READ ONLY snapshots only) and the `md_reader` (`MdPort` with the store's
writer epoch, every stored bar still hash-checked), the latest `market_sessions` schedules), `src/ops/arm_view.py`
/ `src/ops/cli.py` (`arm replay --rerun`: per-row `rerun`, `rerun_outcome`, a `reruns` summary; settings or setup
problems are a warning, the inputs report still stands), `src/ops/arm.py` (`ArmContext.arm_settings`),
`src/db/arm_store.py` (`decision_memory_before` with the four `*_BEFORE` reads bounded by `decision_seq`,
`decision_handles`, `SQL_DECISION_HANDLES_READ`), `src/db/arm_ports.py` (`PastDecisionReader`),
`src/marketdata/port_impl.py` (public `token_epoch`). (4) **`conformance_probe_partial`:** `src/riskmgr/vectors.py`
(new: 6 strategy vectors for kind (a) v1 and 5 guard vectors for `first_bar` v1, synthetic; `vector_problems` runs
`parse_params`, `propose`, `at_target`, `applies`, `evaluate`; a version without vectors is `no_vectors`),
`src/riskmgr/probes.py` (`VersionConformanceProbe`'s one job runs the declaration checks plus the vectors under the
job cap). (5) **`entry_time_source`:** `src/pubsub/orchestrator.py` (`history_execution_time`: one read-only
`fetch_history` at `RestPriority.RECONCILE`, the entry order's opening FILLED / PARTIALLY_FILLED times, nothing
published; `ExecutionTimeResult`), `src/riskmgr/ports.py` (`ExecutionTimePort`, `HistoryFill`),
`src/riskmgr/tracker.py` (`exec_times=`; `_history_entry` runs the lookup as its own task while bar 0's cap has not
passed, at most `HISTORY_MAX_ATTEMPTS` (5) per position and term, `HISTORY_RETRY_MS` (60 s) apart, 30 s timeout, the
earliest opening fill gives `BROKER`; `close()` cancels lookups at the task's end), `src/runtime/arm_ports.py`
(`OrchestratorExecutionTimes`), `src/runtime/optional_plan.py` (wired). (6) **`corrections_restart`:**
`src/db/arm_store.py` (`recent_final_decisions`, `SQL_RECENT_FINAL_DECISIONS`), `src/db/arm_ports.py`
(`RecentDecisionReader`), `src/riskmgr/barclock.py` (`recent=`; `_rebuild_window` at the term's first correction
pass: every committed POST / LATE / CORRECTED decision of a live position with its `decision_cap` window still open
is kept again from its stored input hashes; memory only, no write, no notice; a failed read retried each cadence),
`src/runtime/optional_plan.py` (`recent=store`). `src/riskmgr/golive.py`: the six gates `WIRED` with their S39b
details; `claude_rules` names the S39b proposal. Docs: design header, §0b (gate rows, "S39b: the six engineering
gates", not-wired list, memory-only list, pending proposals), §A7 PC sleep, §B6.6 corrections, §B14 Clearing;
module docstrings (`barclock.py` steps 7 and 8, `jobs.py`, `probes.py`, `arm_replay.py`, `clockstep.py`, `ticks.py`,
`orchestrator.py`). Tests: `tests/riskmgr/test_s39b.py` (10: a job across a sleep that then finishes / raises / is
seen by its own monotonic jump / is still hung after an awake cap; the BarClock's sleep during a job (no fault,
pending, spanned `DOWNTIME` at its cap); a suspend making the bars it covered `DOWNTIME` and telling the jobs; a
takeover mid-window then a bar revision gives a CORRECTED item with no POST notice re-queued; the control without the
rebuild; a failed rebuild read retried and a window ended while off not kept; the six gates wired),
`tests/riskmgr/test_s39b_rerun.py` (10: MATCH through the real pipeline with every read pinned to the decision time
and nothing written; the exit-quote and moved-stop UNVERIFIABLE cases; DIFF; a revised bar and a closed position;
PRE rows and spans not re-run; the read-only as-of ports and calendar; the memory before a decision),
`tests/riskmgr/test_s39b_vectors.py` (8: the engine versions meet their vectors; the probe passes them; a hung, a
raising, a wrong strategy and a wrong guard version and a version without vectors fail it; problems name codes
only), `tests/riskmgr/test_s39b_entry_time.py` (3: ESTIMATED to BROKER from the history; failures retried, bounded,
never after bar 0's cap; `close` cancels a lookup), `tests/pubsub/test_s39b_history_exec.py` (2: reconcile
priority, opening fills only, nothing published; the runtime port), `tests/marketdata/test_s39b_receipt_gaps.py`
(7: heartbeats keep a quiet market out of a gap; a silence blinds the minute it overlaps; seeded markets watched
from the bind; unwatched adapters; the orchestrator feeds prices heartbeats only while subscribed and not lost; a
raising hook stays off the socket path; the session forwards), `tests/ops/test_arm_rerun.py` (3),
`tests/db/test_arm_store_s39b.py` (2, emulator); updated: `tests/riskmgr/test_s38e.py` (the open set),
`tests/runtime/test_optional_bootstrap.py` and `test_optional_plan.py` (the wiring), `tests/riskmgr/test_arm_join.py`
(`new_term(recent=)`), `tests/riskmgr/notice_fakes.py` and `tests/db/arm_emulator.py` (the new reads). G: pytest 5575
passed, 61 skipped; mypy strict clean (310 files); `verify_execution.py --strict` 11 / 11.
Review fixes (2026-11-18): (a) `corrections_restart`: `SQL_RECENT_FINAL_DECISIONS` is now `ORDER BY d.bar_end_ms DESC,
d.decision_seq DESC LIMIT` (a truncated read drops the oldest bars, not the in-window ones), `_rebuild_window` re-sorts
oldest first and reads only bar ends from `now - (timeframe + p50 + settle)` (the live window, at most `KEEP_MS`), so
the limit is almost never reached; the condition with the lost line continuation is rewritten; both fakes follow;
(b) `entry_time_source`: `ExecutionTimePort.supports` and `HISTORY_EXEC_TIME_ADAPTERS` (`src/riskmgr/ports.py`,
StoneX only): `OrchestratorExecutionTimes` answers another adapter at once with no history read and the tracker never
starts a lookup for it; a lookup and its try count are dropped once bar 0's cap passes or the row is no longer live
(`ArmTracker._drop_history`: a running task cancelled, a finished one's exception consumed); (c) `replay_rerun`:
`AsOfImages` (the account image's freshness is not judged from today's stream fences) in `Rerunner`, and a
`MARKET_CLOSED` on one side only is `UNVERIFIABLE(calendar_not_recorded)` before the DIFF branch; (d) `uptime_suspend`:
a bar whose `decision_cap` passed during the sleep is `DOWNTIME` as well; (e) `receipt_gap_feed`: documented as per
adapter, not per subscription (deviation 3, design §A4 / §A7, `clockstep.py`). Tests added or changed:
`tests/riskmgr/test_s39b.py` (a truncated rebuild read keeps the newest in-window decision; the suspend test now
covers a bar whose cap passed in the sleep, plus a control), `tests/db/test_arm_store_s39b.py` (newest first),
`tests/riskmgr/test_s39b_entry_time.py` (an unsupported adapter is never looked up; a lookup left at bar 0's cap is
dropped with its exception consumed), `tests/pubsub/test_s39b_history_exec.py` (the port does no read on a non-StoneX
adapter), `tests/riskmgr/test_s39b_rerun.py` (a fence open now; `calendar_not_recorded`). G after the fixes: pytest
5583 passed, 61 skipped; mypy strict clean (310 files); `verify_execution.py --strict` 11 / 11. `src/pubsub` was not
changed by the fixes.
Deviations: (1) the `/live-trading-server` skill could not be invoked by the build agent (user-invocable only), as in
earlier stages; of its directories `src/pubsub` (the orchestrator's frame hook and its read-only history lookup) and
`src/riskmgr` / `src/marketdata` changed; `src/adapters`, `src/dto`, `src/integrity` and `src/oversee` are untouched;
(2) `uptime_suspend`: a job that spanned a sleep and raised is retried, not counted (a version raising every time
counts on its next awake run); a loop stall longer than cap + 10 s reads as a suspend to the runner (it has no
suspend clock; the lease keeper still tells a stall from a sleep for the fence); (3) `receipt_gap_feed`: the
keepalive is a constant (`RECEIPT_GAP_MS`, 20 s), not a setting; the hook fires on prices-stream heartbeats only
(ticks already feed the detector); those heartbeats are made locally by the channel while the client reports
CONNECTED and refresh every market of the adapter, so the gap granularity is per adapter, not per subscription: a
single market whose subscription went silent while the session stays up is not blinded (design §A4 / §A7); (4) `replay_rerun` runs offline in the ops CLI (`--rerun`), not through an
operator request (that would need a migration for a request kind); the account image and the pin are read as of now
(only `S_at_T`, the decision memory, the bars and indicators and the SM10 state are point-in-time; the image's
freshness is not judged from today's fences, `AsOfImages`; the SM10 state comes from the latest schedule with no
overlay, so a `MARKET_CLOSED` on one side only is `UNVERIFIABLE(calendar_not_recorded)`), so a decision whose
handles moved since is `UNVERIFIABLE(order_state_changed)` (from its `arm_decision_handles` read levels) rather than a
false DIFF, and one decided on the exit quote (never recorded, C5) is `UNVERIFIABLE(exit_quote_not_recorded)`; SM12 is
trusted offline (the engine's memory) and the skew is the cold default; the ops wiring has no `pg` test here (no
`ORDER_SERVER_TEST_DSN`): `Rerunner` is covered through a fake and its store reads through the emulator; (5)
`conformance_probe_partial`: the runtime vectors are a subset of the build-time suite (`tests/riskmgr/conformance.py`
stays the full gate), kept in `src/riskmgr/vectors.py` keyed by `(kind, version)` rather than on the classes, and a
registered version without vectors now fails the probe; (6) `entry_time_source`: StoneX only (`HISTORY_EXEC_TIME_ADAPTERS`; on cTrader no id of the position is the broker
order id); the lookup reads the whole
`fetch_history` window (StoneX: the trade history of every trading account plus the open positions and active
stop / limit orders) rather than a by-order read, bounded as above; its rows are not published (the catch-up still
publishes history); `entry_time_ambiguous` stays False (the BROKER time settles the entry; the flag is not stored);
(7) `corrections_restart`: the rebuild reads the committed final decisions of the live window (bar ends from
`now - (timeframe + p50 + settle)`, never more than `KEEP_MS`), newest first, at most `REBUILD_LIMIT` (2000), once per
term; the `OpenChecks` RETRY state is not persisted (a compare left open re-opens on
its next trigger, and its audit row is written by the term that sees its window end); (8) no `verify_execution.py`
check was added (the `.claude/` hard rule); pre-existing and unrelated: the intermittent teardown `RuntimeWarning` in
`tests/runtime/test_optional_plan.py` (S38e deviation 10).
Left for the user: approve `docs/design/proposed-claude-changes/S39b-six-gates.md` (with the earlier proposals);
the open go-live gates are now only user decisions and PR3 measurements (`oversee_c4`, `s21_c1`, `q1_q2`,
`c2_c3_c5`, `claude_rules`, `s25_registry`, `pr3_stop_children`, `pr3_roll_prints`); try
`python -m src.ops arm replay --rerun` against the dev store once decisions exist; run the `pg` marker tests.

Close these six gates in `src/riskmgr/golive.py` by building what each names. Mark each one `wired` only when it is
built and tested; never by acceptance. For each gate, read its text and title in `golive.py` and in design §0b, plus
the deviations recorded in earlier stages.

1. **`uptime_suspend`.** The uptime tracker's suspend signal reaches the BarClock and every strategy job. A job whose
   run spans a sleep is not counted as a timeout or a strategy fault. Bars that were missed get the DOWNTIME span.
   Test: sleep during a job.
2. **`receipt_gap_feed`.** Give the orchestrator a per-adapter heartbeat or frame hook, so the S14 receipt-gap
   detector is fed live and turned on. Gaps mark the TICK buckets they overlap as partial or blind. Invoke the
   `live-trading-server` skill first if you can; this touches `src/pubsub`.
3. **`replay_rerun`.** `arm replay` re-runs the real pipeline for a past bar, read-only and with no notices sent. It
   compares the result with the stored decision by `inputs_digest` and outcome, and reports MATCH, DIFF or
   UNVERIFIABLE.
4. **`conformance_probe_partial`.** The `strategy_conformance` clear probe runs the pinned version's own code against
   its conformance vectors, under the job runner's time cap. A hung or raising version fails the probe.
5. **`entry_time_source`.** Wire the history execution-time source, so the entry time can be upgraded from ESTIMATED to
   BROKER. It uses StoneX trade history `ExecutedDateTimeUtc` through `fetch_history` / `reread`, read-only, at the
   reconcile REST priority.
6. **`corrections_restart`.** The D4 correction window survives a restart or takeover. On start, the BarClock rebuilds
   its recheck set from committed decisions still inside their `decision_cap` window, using the stored input hashes.
   It never re-sends a notice that was already sent.
   Test: a takeover in mid-window, then a bar revision, gives a CORRECTED item.

Every new or changed write is fenced and stays behind the flags. No new migration should be needed; if one is, it goes
behind `XTNL_MIGRATIONS_ACCEPTED_UPTO`. `python -m src.ops arm gates` must show all six as `wired`. G green.

## 5. Round-11 MAJOR findings: where each is resolved

"Doc" = the matching design patch (applied in S00, listed in §8).

| ID | Finding | Stage(s) | Resolution | Doc |
|---|---|---|---|---|
| R11-M01 | D6 tick cross-check never runs under filtered delivery | S14, S17 | Per-field eligibility: close when the TICK bucket's coverage is complete (`close_eligible`), high / low only when the TICK bar is `complete`; skipped-ratio metric and page; D6 status per series in `md show`; PR3 go / no-go names the UNAVAILABLE case. Golden: a shifted feed under filtered delivery reaches `source_mismatch` in 3 buckets. | yes: §A4, PR3 |
| R11-M02 | Time-shift / side-swap test fails healthy quiet series | S17 | Count a shift or swap only when bucket T fails tolerance and the neighbour or other side passes it by ≥ 0.5 × tolerance on the eligible vector; skip the shift test when the bucket range is below 3 × its median spread; a bar within tolerance is always a match. Golden: flat Asian session never flags. | yes: §A4 |
| R11-M03 | One blind BROKER bucket at the cap disables the first-bar test for hours | S17, S18, S20 | After the cap a blind BROKER bucket moves to a low-priority backfill lane inside the BARS budget until resolved or out of broker reach; ATR recomputes on resolution (S18); gap-heal pass. Test (S20): a fill 1 h later gets a real verdict. | yes: §A4, §A8 |
| R11-M04 | AT_PRE protection lasts one bar | S21, S23, S28 | Option (a): persist `pre_basis` on `arm_positions` after PRE_NOT_CONFIRMED; later bar-end trigger tests use the pre-move stop until a bar end confirms; the PRE_NOT_CONFIRMED text says what releases the position now. Long / short test. User confirms (§9.2). | yes: §B6.3, §B6.4, §B8 |
| R11-M05 | FAILED ping's critical copy via `EngineSession.page` is not durable | S24, S30 | The `arm_failures` row, `OutboxRow(kind="info", severity="critical")` via `insert_outbox(tx, ...)` and the kind `arm` row commit in one fenced transaction (engine pool when the ARM pool is the fault); ping id in the title; body bounded; retried from memory until committed; the log mirror is `PagerWrapper.log_mirror()`, never `EngineSession.page` (which queues a second critical row). | yes: PR4, §B8, §B14 |
| R11-M06 | `S_at_T` can use a history version stamped at placement with the last level | S01, S04, S21, S24 | `event_log.broker_time_ms` (real broker time only) and `broker_time_source`; only APPLIED versions with origin `stream`, `stop_read`, `snapshot`, `dlq` and a real broker time count; `history` and `DROPPED_STALE` never; fallback `PRIOR_VERSION` or `DATA_UNAVAILABLE(stop_at_bar_end)`. Golden: amended then cancelled while off. | yes: §B6.4, PR8, §C1 |
| R11-M07 | PR1 Oversee triggerable-side rule needs a quote and a timer | S07 | `effective_stop_detail(e, positions, orders, mark)` with the MarkBook mark; a caveat, turned into UNKNOWN by `MissingStopLossRule` through the existing durable `unknown_since` / `unknown_grace_ms` (30 s); no new timer or column. | yes: PR1 (10 s grace → the rule's 30 s unknown grace), oversee-rules §2, §5, §6 |
| R11-M08 | K counts reads in §A4 but bars in §B14 | S17, S30 | One unit, defined in §B14: a bucket adds at most 1, only when still invalid at its cap or after its full re-read schedule; decode errors are retries (metric, one page). Test: 3 undecodable responses then a valid bar → LIVE, K = 0. | yes: §A4 points to §B14 |
| R11-M09 | Global FAILED from a bootstrap failure cannot be cleared | S16, S30 | The ARM-scope clear probe runs a trial bootstrap under the writer token and boot lock; `md_writer_reset` and term-start bootstraps run while globally FAILED (store repair, never advice). Tests: AOF-aside then clear; restart while FAILED with an empty store. | yes: §A7, §B14 |
| R11-M10 | A self-healing broker REST outage becomes a global FAILED | S06, S17, S30, S31 | Cap counters and the re-fetch row pause while the adapter is not STREAMING or REST fails at every priority (`broker_unavailable` page, GAPPED); new ADAPTER scope; ARM-global only for `redis-md`, the ARM pool and task circuits; global FAILED stops serving and advice, tick capture continues (user confirms, §9.2). | yes: §B14, §A7 SM12 |
| R11-M11 | Stuck frontier filed as a series fault; undefined "PostgreSQL down" | S22, S30, S31 | ARM-scope fault keyed by cause, deferring to `fail_pool_down_s` for ARM pool failures; "engine PostgreSQL down" = term pool's last fenced commit older than 30 s or the RiskSweep's unreachable alert; grace 120 s; one collapsed ping for a global FAILED. | yes: §B14 table, §B6.6 |
| R11-M12 | Clear probes do not test the recorded cause | S23, S30 | `arm_failures.probe_kind` chosen from the reason (five probe kinds plus ARM global); a refused clear names the command to run first; `md_rebuild`, `md_backfill`, `reset_task` allowed during FAILED. | yes: §B14 Clearing |
| R11-M13 | HMAC key rotation strands the history | S12, S16, S23, S32, S34, S37 | Keyring with retained key ids accepted on read; `md_rehash` request rewrites members under the new id (no `bar_revised`); PREV retired only after completion plus one correction window; runbook. Test: rotation causes no quarantine, FAILED or CORRECTED. | yes: §A13, §B13, §C2 |
| R11-M14 | Close-recoverable values in ARM outbox bodies (and WAL) | S28, S29, S38 | Outbox body = ids, outcome codes and order state only; `arm-relay` renders favour R and favour / risk at send time from a hash-checked `MarketDataPort` read, omitting them if unavailable; C3 test covers unsent, expired and finished rows and the INSERT payload. §0 states plainly that Q15 (a) keeps price data in PostgreSQL. | yes: §B8, §0 Q15, §C3 |
| R11-M15 | Forming-bar validation and "bucket requested" vs a count-based port | S03, S12, S17 | Validation and storage only for bars with bucket end ≤ receipt + 5 s; the forming bar is finality evidence only; check (2) = on the grid and inside `[from, to]`; port gains an optional range form. | yes: §A4, PR3, PR12 |
| R11-M16 | Pull cursor cannot deliver same-id revisions | S13, S16, S22 | Rollback-set score = per-series `write_seq`; cursor `(bar_end_ms, write_seq)`; `events_since` returns revisions of older ids; the ARM's durable acknowledgement stays `(last_decided_bar, hash)`, the `write_seq` cursor is in memory per generation. | yes: §A10, §A13, §B11 |
| R11-M17 | No per-strategy fault containment | S19, S22, S23, S30 | Per-version executor with a 250 ms cap; per-version circuit, guard faults attributed to the guard's own `(KIND, VERSION)`; STRATEGY_VERSION scope failing only its positions, one ping, operator clear; conformance test with a planted hanging strategy. Edge = a label on that scope in Phase 1. | yes: §B4, §B11, §B14 |
| R11-M18 | Edge attribution input carries no edge on StoneX | S23, S26 | `attribution_source` enumerated (`SINGLE_EDGE_DEFAULT`, `OPERATOR_TAG`, `INTENT`); Phase 1 refuses a second enabled edge; §B11 states that StoneX's stored id has no edge information and that with several edges an unattributed position is UNATTRIBUTED, never armed under a default edge; `arm_reattribute` belongs to the multi-edge design. | yes: §B11 |
| R11-M19 | Notice part cap delays POST advice | S28 | A POST ADJUST or COMPLETE committed after part 4 is sent at once as its own item-keyed notice; a part-5 item is never superseded before it is sent once. Test: 8 symbols final at 8 times. | yes: §B8 |
| R11-M20 | PR9 stop reads cost one call per handle and are not budgeted | S04, S18, S26, S27, S31, S33 | One list read per adapter per anchor (`reread_stop_orders`, StoneX `active_stop_limit_orders`), never closing absent entities; stop reads in the admission check and the demand ceiling; per-adapter armed-handle ceiling → `UNSUPPORTED(stop_read_budget)`; the PR9 schedule in `src/riskmgr/stopreads.py` (S27) coalesces reads per adapter and anchor, staggers REOPEN reads and counts `stop_stale` per position. Test (S27): 8 positions × 2 handles with the BARS bucket saturated, PRE never `stop_stale`. | yes: PR9, §B4, §A6 |

## 6. Round-11 minor findings

| Finding | Stage | Resolution |
|---|---|---|
| PR9 reads per handle do not fit the PRE window | S04, S27, S33 | As R11-M20: one list read per adapter per anchor, admission counts stop reads, `stop_stale` metric, a position armed after its PRE read was queued gets no PRE. |
| PRE quote sampled after `T − 30 s` | S21, S27 | Quote frozen at fire time; valid-until `min(fire + 10 s, local T + p50 − 30 s)`; test at offset 30 s. |
| PRE of the fill bucket under `next_full_bucket` | S20 | `PRE_ANCHOR` known at PRE; "no advice this bar (entry bucket)". |
| `be_offset_r` with R ≤ 0 or FIRST_SEEN S0 | S20 | `max(R, 0)`; caveat "buffer based on first-seen stop". |
| Friday-close cap loses REOPEN | S22 | A pre-close bar UNRESOLVED at its cap is re-read at the open and decided as REOPEN if final then (doc patch; user may override). |
| Verification re-read after the cap | S17 | At `min(final + 60 s, cap − 30 s)`, else caveat `unverified_final`. |
| `reread_entity` claim and origin defaults | S04 | `dlq` for the bridge, `snapshot` for account refreshes; `md_ports` calls the orchestrator directly. |
| event_log index cannot serve "latest ≤ T" | S01, S23 | `broker_time_ms` column and index `(entity_type, entity_key, broker_time_ms)`. |
| ARM dashboard views without grants | S23 | `GRANT SELECT … TO xtnl_dashboard`; no `xtnl_api` grant. |
| Outbox CHECK passes on an empty array | S08 | `coalesce(cardinality(...), 0) > 0` form; test. |
| `safe_error()` / redact overstated | S12, S24 | `safe_redis_error`, `safe_db_error`; doc wording "scheme, host and port". |
| MD_RESET notice at every term start | S16 | Notice only for `store_lost`, `store_foreign`, in-term `run_id` change; budget excludes term starts and operator resets. |
| `arm_clear_failed` blocks the operator loop | S10, S30, S32 | Non-blocking handlers; work enqueued to the owning task. |
| Clock of `decision_cap` | S12 | `local T + timeframe + p50 + settle` for every cap-based timer. |
| Global escalation across adapters | S30 | ADAPTER scope. |
| FAILED lost if the engine restarts while PostgreSQL is down | S30, S31 | `FailedSpoolPort` (CRC-framed spool) replayed before any advice. |
| Ping not atomic | S24, S30 | As R11-M05. |
| Lockout reflection bypasses; ReadOnlyTransport not cited | S11, S38 | Reflection rules; `PagerWrapper`; e2e asserts ReadOnlyTransport. |
| Phase 2 re-send of a stop amend | S00 | Doc only (§B10 known requirements: one non-terminal intent per handle, expected broker version). |
| API drop list misses `XTNL_DEADMAN_TOKEN` | S36 | Added, plus the drop-list rule test. |
| ARM pool uses the owner role | S23, S31 | `xtnl_arm` role via `SET ROLE`; static check kept. |
| Supabase provisioning and seen-row chain | S23, S33 | `deploy/supabase/arm_schema.sql`, self-check, chain in `arm_config_cache`. |
| LATE / REOPEN / CORRECTED actionability wording | S00 | Doc only (§B10). |
| `md_writer` ACL and pipelined EVALSHA | S13 | `transaction=False`, SCRIPT LOAD on connect, live ACL flush test. |
| Event order §A6 vs §A8 | S18 | `bar_closed` with the bar; `indicator_updated` after the indicator write; POST waits for both. |
| Indicator quantum tied to price increment | S18 | Quantum per output (PRICE, RATIO, COUNT), pinned per VERSION. |
| Fixed-count `window()` | S18 | `Count` or `Anchored`; anchored test indicator. |
| First broker read at p99 lag | S17 | First read at `T + p50 + settle`. |
| Retained BROKER history holes | S17 | Gap-heal pass; `unfillable`; count in `md show`. |
| Out-of-process read check (5) needs the key | S12 | Unkeyed CRC32 digest in the member for out-of-process readers; HMAC key engine and ops CLI only. |
| Lease path for out-of-process consumers | S00, S16 | Doc: an in-engine registration port later; bootstrap rebuilds the registry from every live lease. |
| Release rule hard-codes kind (a) reasons | S19, S21 | Rule defined on the Evaluation; conformance suite. |
| Decisions lack envelope version | S19, S23, S35 | `envelope_version` and params hash stored; replay reports `ENVELOPE_CHANGED`. |
| Timing constants assume 15m | S12 | Per-bar-spec timing table; only the 15m row in v1. |

## 7. Code-map mismatches: where each is fixed

| Mismatch | Stage | Mismatch | Stage | Mismatch | Stage |
|---|---|---|---|---|---|
| RT-M1 hook can end the term | S09 | IN-1 history `TypeId` | S02 | ST-1 partitions −1..2 | S23 |
| RT-M2 `has()` | S09 | IN-2 grace and quote in Oversee | S07 | ST-2 savepoints | S08 |
| RT-M3 half-open reopen hook | S09 | IN-3 watermark marker | S01, S04 | ST-3 migrate post step | S08 |
| RT-M4 `OutboxRelay` escalation | S29 | IN-4 `reread_entity` result | S04 | ST-4 ping atomicity | S24, S30 |
| RT-M5 `page()` not durable | S24, S30 | IN-5 send time after token wait | S06, S04 | ST-5 schema test reads 0001 only | S01 |
| RT-M6 `aclose` sequential, no terminate | S09 | IN-6 per-handle reads | S04, S31 | ST-6 EVAL vs EVALSHA | S13 |
| RT-M7 receiver optional fields | S08 | IN-7 ORDERS level when `OpenPrice` empty | S02 (test), S39 | ST-7 `src.cache` banned, Lua as constants | S13 |
| RT-M8 `OpsEtcd.get_prefix` | S10 | IN-8 `tick_symbols` not wired | S05 | ST-8 name-ban traps | S11 |
| RT-M9 public ensure, `AUDIT_TABLES` | S10, S23 | IN-9 etcd module | S10 | ST-9 `/cancel` hook text | S11 (rule) |
| RT-M10 unknown kinds consumed | S10 | IN-10 `mark_session` fails open | S21, S31 | ST-10 `WAITAOF` in fakeredis | S13 |
| RT-M11 `application_name` pattern | S31 | IN-11 schedules for tick-only symbols | S05 | ST-11 CHECK without subquery | S08 |
| RT-M12 late-bound hook | S09 | IN-12 SUBSCRIBED resets all symbols | S05 | ST-12 receiver optional fields | S08 |
| RT-M13 settings validation place | S09, S31 | IN-13 lost updates per market | S03 | ST-13 `HEALTH_PREFIX` location | S10 |
| | | IN-14 banned imports, roll helper | S11, S12 | ST-14 line drift | S00 |
| | | IN-15 line drift | S00 | | |

## 8. Design-document patches (v11 → v11.1, applied in S00)

1. **§A4** Cross-check: per-field eligibility; shift / swap rule with the margin and the quiet-range skip; skipped-ratio
   metric and page; D6 status per series (R11-M01, R11-M02). Validation applies only to bars with bucket end ≤ receipt +
   5 s; the forming bar is evidence only; check (2) = grid plus requested range (R11-M15). The K sentence points to §B14
   (R11-M08). After the cap a blind BROKER bucket moves to the backfill lane; gap heal (R11-M03). First read at
   `T + p50 + settle` (R11-m).
2. **§A6 / §A8:** event order (`bar_closed` with the bar, `indicator_updated` after the indicator write); quantum per
   output; `window()` Count or Anchored; md-bars wording at the cap matches §A4.
3. **§A7 / §A13:** trial bootstrap for the clear probe; bootstraps allowed while globally FAILED (R11-M09); MD_RESET
   notice classes and reset-budget counting (R11-m); rollback-set `write_seq` and cursor `(bar_end_ms, write_seq)`
   (R11-M16); retained key ids, `md_rehash`, rotation order (R11-M13), and the matching §C2 Secrets sentence:
   `XTNL_MD_HMAC_KEY_PREV` is kept until `md show` reports the re-hash complete plus one correction window, not for one
   correction window only; unkeyed record digest for out-of-process readers.
4. **§A10 / §B11:** `events_since(series, cursor)` returns revisions; out-of-process lease path stated as later work;
   StoneX `opened_by_client_order_id` carries no edge; `attribution_source` enumeration; the multi-edge UNATTRIBUTED rule
   (R11-M18); per-edge error budget = a label on the STRATEGY_VERSION scope in Phase 1 (R11-M17).
5. **PR1:** `effective_stop_detail(e, positions, orders, mark)`; inconsistency is a caveat turned into UNKNOWN by
   `MissingStopLossRule` through the existing 30 s unknown grace (replacing "10 s grace") (R11-M07).
6. **PR4 / §B8 / §B14:** the FAILED ping's critical copy is `insert_outbox(tx, ...)` in the same fenced transaction;
   the log mirror is `PagerWrapper.log_mirror()`, never `EngineSession.page`; ping id in the title; bounded body (R11-M05). The outbox CHECK text uses the
   `array_to_string` / `cardinality` form.
7. **PR8 / §B6.4 / §C1:** `event_log.broker_time_ms` and `broker_time_source`; `S_at_T` accepts only APPLIED,
   non-history versions with a real broker time; index on `broker_time_ms`; `reread_entity` callers and default origins
   (R11-M06, R11-m).
8. **PR9 / §B4 / §A6:** one list read per adapter per anchor; stop reads in admission and the ceiling;
   `UNSUPPORTED(stop_read_budget)` (R11-M20).
9. **§B6.3 / §B6.4 / §B8:** `pre_basis` after PRE_NOT_CONFIRMED (R11-M04); release rule on the Evaluation (R11-m); PRE
   quote frozen at fire time (R11-m); `PRE_ANCHOR` at PRE.
10. **§B8:** ids-only outbox body and send-time rendering (R11-M14); immediate sending of post-part-4 ADJUST / COMPLETE
    (R11-M19).
11. **§B14:** one counting unit; pause predicates (adapter not STREAMING / broker-wide REST failure, engine PostgreSQL
    down defined); scopes SERIES, ADAPTER, STRATEGY_VERSION, ARM; stuck frontier as ARM scope with 120 s grace;
    collapsed pings; probe kinds; requests allowed during FAILED; global FAILED keeps tick capture (R11-M08 … R11-M12,
    R11-M17); FailedSpool.
12. **§B3 / §B9 / §C2:** reflection rules and ReadOnlyTransport as the hard guarantee; `safe_db_error` /
    `safe_redis_error`; redact wording "scheme, host and port"; `xtnl_arm` role; `XTNL_DEADMAN_TOKEN` in the API drop
    list; the two enable flags and `XTNL_STONEX_PRICE_UNFILTERED` / `XTNL_ARM_STOP_READS`; Supabase provisioning file and
    self-check.
13. **§B10:** actionable = advice, not superseded, not PRE, not UNRESOLVED_RANGE, within the maximum age, not under
    FAILED; LATE / REOPEN / CORRECTED are labelled, not refused by default; one non-terminal intent per handle with the
    expected broker version.
14. **§0 Q15:** state plainly that under (a) price data stays in PostgreSQL in two existing tables, and that (a) is an
    explicit user override, not the default (R11-M14, market-data lens).
15. **§B5.2 / §B6.6:** `decision_cap` clock; the Friday-close REOPEN rule (R11-m).
16. **§C1 / §B13:** 0017 partition loop −1..2 (ST-1); `probe_kind`, `scope_kind` values, `pre_basis`, envelope version,
    grants, the `xtnl_arm` role; the `event_log` index becomes `(entity_type, entity_key, broker_time_ms)`; `md_rehash` in
    the §B13 request-kind list; §B13 states that registered handlers are non-blocking (enqueue to the owning task, answer
    later as applied / refused / moot). Line-drift fixes (IN-15, ST-14).
17. **§B12:** SM11 is implemented as one pure transition table (`src/riskmgr/sm11.py`); the FAILED edge stores and
    restores `prior_state`.

## 9. What needs the user

### 9.1 Before or during the build

| Item | Needed for | Until then |
|---|---|---|
| The signal to start building (design review gate) | S00 | nothing is built |
| ★ Q1 (strategy kind) | S20 | S00-S19 and S23-S25 proceed; the task brief names kind (a), so S20 builds (a) unless the user says otherwise |
| ★ Q2 (first-bar measure and ATR bar) | enabling | both forms exist as FirstBarGuard params; defaults `high_low`, `>`, including bar 0 |
| ★ Q15 (existing price tables) | S00 wording | (a) is kept and stated as an override; (b) / (c) would add a stage after S38 |
| Approval of the `.claude/**` and `CLAUDE.md` diffs (hooks, rules, `verify_execution.py`) | S07, S11, S35, S37 | code lands; the hook parity test and the registry check are skipped with a reason; the pytest lockout scan of the real tree still gates every stage (§1) |
| Confirmation of the S07 Oversee change (§9.2, last item) | S07 merge, then S21 onward | S08-S20 and S23-S25 proceed; live Oversee monitoring stays as today |
| Sign-off of the S25 change to the S19 module `src/riskmgr/registry.py` (timing-row check on `new_pin=True`, re-exports; S25 deviation 2) | S26 (the first arming caller) | the change stays in the tree, tested; it runs only on a new pin and has no other `src` caller yet |
| PR3 probe run: `.venv/Scripts/python.exe -m src.adapters.stonex.probe_bars` with the `STONE_X_*` variables set in the user's own shell | S39; StoneX ARM enablement; `XTNL_STONEX_PRICE_UNFILTERED`, `XTNL_ARM_STOP_READS` | the build uses `MockBrokerAdapter` bars; StoneX `fetch_price_bars` stays "not supported", so the ARM would mark StoneX positions UNSUPPORTED |
| `deploy/local/new-md-credentials.ps1` run by the user (two `redis-md` passwords, ACL file, HMAC key in the Windows User environment) | real `redis-md` on the PC | fakeredis with lupa in tests; the `md`-marked tests skip without `ORDER_SERVER_TEST_REDIS_MD` |
| ARM channel accounts (separate SMTP or SES account; ntfy with a token or Pushover) as `XTNL_ARM_*` in the User environment | enabling the ARM (both channels are required) | `RecordingChannel` in tests; with them missing the ARM does not start and one page names the missing channel |
| Supabase project: apply `deploy/supabase/arm_schema.sql`, create `xtnl_arm_reader` / `xtnl_arm_writer`, MFA on the account, network restrictions, pinned CA, `XTNL_ARM_CONFIG_DSN` | S33 live use | a fake config source in tests; without a DSN the ARM is idle (one notice, one page) |
| Optional: TradingView Pine exports for the ATR golden vectors | S18 | independent reference implementation and hand-checked vectors |
| Optional: an off-PC dead-man receiver | the PC backstop | stated as accepted: on the single PC the ping goes out when PostgreSQL returns |
| Commits | any stage | the work stays uncommitted |

### 9.2 New confirmations raised by round 11 (each has a safe default the build uses)

* **R11-M04:** after PRE_NOT_CONFIRMED, keep testing against the stop as it was before your heads-up move until a bar end
  confirms (option (a), built), or treat the moved stop as the current SL from the next bar (option (b)).
* **R11-M10:** while the ARM is globally FAILED, keep recording ticks (built; tick history cannot be re-fetched) but
  serve no data and give no advice; or stop recording too.
* **R11-m Friday close:** a pre-close bar still not final at its cap is re-read at the open and decided as a REOPEN item
  (built), or the cap at a close means the end of the first post-open bar.
* The Oversee behaviour change in S07: a STOP child at or beyond entry now counts as a stop (the fix for the stop that
  "disappears" at break-even). Three consequences to confirm with it: (1) a LIMIT child no longer counts; (2) a position
  whose only stop-kind child is a STOP on the wrong side of the market (a mislabelled take-profit) used to escalate to
  HARD; it now stays UNKNOWN for 30 s, then capped SOFT `stop_kind_inconsistent` that re-pages but never escalates;
  (3) a still-unfilled (PENDING / WORKING) entry order whose STOP child sits beyond its entry keeps the old behaviour:
  no stop, uncapped SOFT `missing_stop_loss` after 2 s, and its risk reads `no_stop` (so the account's aggregate-risk
  rule is not made UNKNOWN by it).
* **C5 (S00 review):** PRE notices show outcome codes and order state only, with no favour R or favour / risk, because
  those are rendered at send time from a hash-checked bar and PRE has no bar (built); or keep the PRE quote outside
  PostgreSQL so they can be shown.

### 9.3 Enabling order (user)

1. Deploy with both flags `0`; run the G gate, the `pg` marker tests, and the `md` marker tests against the dev compose
   started with `--profile md` (the PC `redis-md` is not running yet).
2. Run `deploy/local/new-md-credentials.ps1`, set `XTNL_MD_ENABLED=1` in the env file (the next `start-xtnl.ps1` run then
   starts `redis-md` through the `md` profile step), and let it soak for a few
   days: `md show` LIVE, D6 status ACTIVE or CLOSE_ONLY, no FAILED.
3. After the PR3 probe and S39, accept a config (`arm config accept`), then set `XTNL_ARM_ENABLED=1`.

## 10. Cross-stage risks

* **Checksum neutrality (S01, S02, S04):** any new field in `payload_checksum` while None would trigger integrity heals
  across every stored row. Golden fixtures and the baseline replay guard it.
* **HA path (S09, S31, S32):** the hook, the composite price hook and `aclose` sit on the leader's critical path; the
  DRAINING budget test and the "raising hook keeps LEADER" test gate every one of these stages.
* **Oversee semantics (S07):** the only intended change to live monitoring; gated on the user's confirmation (§9.1).
* **Market data reaching PostgreSQL (S01, S23, S24, S28, S29, S30):** a new column, payload or outbox body could leak a
  close-recoverable value. The §1 rule, the S23 schema scan and the S38 no-market-data test (unsent, expired and finished
  bodies and the INSERT payload) guard it.
* **Dependency through shared files:** `src/runtime/session.py` is changed by S04, S05, S08, S09, S10 and S31; each of
  those stages rebases on the previous one and re-runs the DRAINING and "raising hook keeps LEADER" tests.
* **Lockout drift (S11):** the hook copy and `lockout.py` must stay equal; the parity test and the verify check guard it.
* **Thread hangs (S18, S22):** Python cannot kill a hung worker thread; a per-version (and per-indicator) single-thread
  executor confines a hang to its own scope, which then FAILs through its circuit.
* **Unconfirmed broker facts (S03, S39):** the bar endpoint, finality lag, filtered delivery and streamed stop moves are
  unknown until PR3; the build never guesses them into production defaults.
* **Uncommitted work in `deploy/local`, `src/api`, `src/reports`, `tests/deploy` (S36):** build on top, never revert.
