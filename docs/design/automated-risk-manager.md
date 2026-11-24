# Market Data layer and Automated Risk Manager (ARM): design v11.7 (Phase 1)

Status: **built behind flags, off by default; S39 built after the PR3 probe (StoneX bars refused to the ARM while S39's go-live gates are open); S39b wired the six engineering go-live gates (the open gates left are user decisions and PR3 measurements).** Stages S00-S39 of
`docs/design/arm-implementation-plan.md` are built in the working tree, **uncommitted** (on top of commit `31d9b58`),
with `XTNL_MD_ENABLED` and `XTNL_ARM_ENABLED` both `0`: with the flags off the engine behaves as before. Since v11.6
(plan S38f) that includes the Oversee change of PR1 (S07), now behind its own switch `XTNL_OVERSEE_STOP_AT_ENTRY`
(off: the pre-S07 rule), and migrations 0015-0020, which a start applies only up to `XTNL_MIGRATIONS_ACCEPTED_UPTO`
(default 14) (§0b "What turns each on"). Still open: the §0 ★ questions Q1, Q2 and
Q15; the round-11 confirmations C1-C5 (the build runs on their defaults, §0b); the S07 merge (C4); S21's acceptance;
the pending `.claude/` proposals (§0b); the go-live gates S39 waits for (§0b "Go-live gates"). Ticks, quotes, bars and
indicator values live only in a dedicated Redis, inside a market-data layer that future automated trading will use to
build indicators; the ARM adds none of them to PostgreSQL (the existing price columns are Q15). Review rounds 1-10 are
addressed, and the user's decisions D1-D7 (§0) are applied. **v11.1** applied the round-11 findings (plan §8, stage
S00). **v11.2** (plan stage S38b, 2026-11-17) syncs the document with the build: §0b lists what is built, the stage
deviations that change behaviour or keys, the state kept only in memory, what is not wired yet and the round-12
findings; **v11.3** (plan stage S38c, 2026-11-17) resolves the round-12 MAJOR findings in design and code (§0b
"Round-12 findings: resolved"); **v11.4** (plan stage S38d, 2026-11-17) hardens the decision path against the
round-13 MAJOR findings (§0b "Round-13 findings: decision path"); **v11.5** (plan stage S38e, 2026-11-17) hardens
operations and adds the go-live gates (§0b "Go-live gates" and "Round-13 findings: operations and gates"); **v11.6**
(plan stage S38f, 2026-11-17) fixes three round-14 safety findings (§0b "Round-14 safety fixes"): nothing unreviewed
goes live at the next start, `xtnl_arm` cannot clear or rewrite a FAILED episode, and the D7 error budget survives
restarts and takeovers; **v11.7** (plan stage S38g, 2026-11-17) fixes the round-14 decision-path, liveness and
sleep findings (§0b "Round-14 findings: decision path, liveness and sleep"); **S39** (plan stage S39, 2026-11-17) records the PR3 probe results (§A11 PR3) and builds StoneX broker bars; **S39b** (plan stage S39b,
2026-11-18) wires the six engineering go-live gates (§0b "Go-live gates", "S39b: the six engineering gates"); the
affected sections are updated in place. Code references are `path::symbol`
anchors (they survive edits), checked against the working tree on 2026-11-17 and kept checked by
`tests/docs/test_design_anchors.py`; "(new)" marks a file this build added (uncommitted), "(changed, Sxx)" an existing
file a stage changed.

**Scope.** Part A: the market-data layer (`src/marketdata/`). Part B: the ARM, Phase 1 (evaluate stop-loss strategies
and notify; nothing is sent to the broker). The Phase 2 order actioner and multi-edge operation get their own designs;
here they have a hand-off contract (§B10, §B11).

**"Price data"** = ticks, quotes, bars (OHLC), spreads, indicator values, and any stored value a price can be
recomputed from (a favour R multiple next to the stored entry and stop, an unkeyed hash of a bar; §B8). Order
levels (entry, stop, a proposed stop)
are order state and stay with order state. Prerequisites are numbered PR1… (not the skill's P-numbers).

## 0. Decisions

**Taken** (binding user decisions given after review round 10; each replaces any earlier default and is specified once,
in the section named):

| # | Decision | Specified in |
|---|---|---|
| D1 | Two anchors. PRE (anchor 1) is a provisional heads-up at a local offset before bar end (default 40 s, allowed 30-45 s; valid until fire + 10 s, so every allowed offset has a 10 s window, §B5.2; never from Supabase). POST (anchor 2) is on StoneX's final bar-end price. In Phase 2 only POST counts; PRE never acts and never pre-stages. | §B1, §B5.2 |
| D2 | The first-bar test (1.318 × ATR) runs only at POST, on StoneX's final bar 0. POST waits for StoneX's final bar, however late, up to `decision_cap` (the end of the next bar); there is no streamed-close fallback. This answers Q18. | §B5.2, §B6.4, §B6.5 |
| D3 | Only a POST bar-end check releases a position (no longer managed). PRE never changes a position's state. | §B6.3, §B12 |
| D4 | Market-data reset: a reset flag; every bar-end event has a deterministic id and a keyed (HMAC) content hash; each consumer acknowledges in its own store and re-applies in order from its own cursor. | §A7, §A13, §B6.6 |
| D5 | Acknowledged ids are recycled; the writer keeps a rollback set of the last 8 bar-end events per series; anything older is re-fetched from StoneX. | §A13 |
| D6 | Zero trust: every bar is validated on write and on read and cross-checked against the tick-built bar. | §A4 |
| D7 | Fail fast: an error budget per series and per ARM; past it the scope enters FAILED, stops advising and pings at once. FAILED never stops ingestion or Oversee, and only a local operator command clears it. | §B14 |

**Needed from the user** (build blockers marked ★). Defaults are the literal reading of the request, or the safer
choice where it is silent. Evidence: §20. The decisions above are built in and not asked again; they answer Q18 and
change Q4.

| # | Question | Default |
|---|---|---|
| Q1 ★ | The move when `(close − entry) > (entry − SL)`: (a) stop straight to break-even; (b) gated trail `S + σ·(favour − risk)` capped at break-even; (c) your SOP's 1:1 trail (ignores the inequality) | (a); only the chosen kind is built |
| Q2 ★ | First-bar test: whole candle (high − low) **>** 1.318 × ATR(14) of that bar, or body ≥ 1.318 × ATR; ATR including that bar or the bar before | high − low, `>`, including that bar |
| Q3 | First-bar test not computable: hold the first bar, or skip the test | hold, and say so |
| Q4 | The PRE heads-up (D1: default 40 s before bar end, a local setting of 30-45 s, valid for 10 s after it fires, §B5.2) by email only, or push and email. Email alone is the literal reading of your request, but an email may land after the bar closes | email only, as you asked; push and email recommended |
| Q5 | Trigger close on the exit side (bid for a long, ask for a short) or always bid | exit side |
| Q6 | Break-even exactly at entry, or entry plus a buffer in R | exactly entry |
| Q7 | "First bar right after entry": the candle the fill is in, or the first full candle after it | the candle the fill is in |
| Q8 | "SL" in the trigger: the current stop or the original | current |
| Q9 | Extra items (armed, ended, late, corrected, config waiting): inside the per-bar notice, separate, or off | inside the per-bar notice |
| Q10 | After the ARM has released a position (a bar end confirmed the trigger with every stop at break-even, §B6.3), if you move the stop back below entry: tell only, or manage again | tell only |
| Q11 | A bar triggered but the stop was not moved: keep advising it on later bars, or re-test each bar (a trigger on the last bar before a market break is always re-checked once at the reopen) | re-test each bar |
| Q12 | Supabase edits take effect after a local `accept`, or automatically after validation (Phase 1 only; tamper checks still apply; §B9) | after local accept |
| Q13 | Notices show the proposed stop and R values only, or also entry and current stop | proposed stop and R only |
| Q14 | Match your TradingView chart: candle and ATR from StoneX bid bars, or tell us which broker feed your chart uses (the server cannot read TradingView data; another feed needs its own design). If PR3 finds daily-break prints in StoneX bars, decisions still follow those bars, as a chart on the same feed would, with a `roll_affected` caveat | StoneX bid bars |
| Q15 ★ | (Full inventory with the account-level values round 12 added: §0b Inventory.) Two **existing** PostgreSQL tables hold prices: `last_clean_marks` (bid, ask, spread per symbol) and, in `sod_marks`, `clean_mids` (written with the start-of-day row). `sod_marks.position_marks` is never written or read (always `'[]'`); 0015 drops it whatever you choose. Readers today: the Redis-loss bootstrap (SM8 step 3 seeds prices); the RiskSweep's every-pass mark input (closed-market and roll-break marks, and its fallback when Redis is unavailable); the RiskSweep's start-of-day row at every 17:00 New York roll and the LEADER_INIT catch-up; and the calendar's reference quotes after a takeover. Writers: the price consumer's 10 s upsert and the forced write at open / close / roll transitions, and the start-of-day row. Options: (a) keep them (nothing changes); (b) move `last_clean_marks` to `redis-md` (§A12): each reader gets a bounded 500 ms read that falls back to today's fail-closed path (no seed, marks UNKNOWN or stale and capped at SOFT, a `partial_session` start-of-day row), which changes the inputs of `MaxDailyLossRule` and of closed-market marking, and on AWS every takeover loses the latest marks unless `redis-md` is replicated to the peer; (c) also move `sod_marks.clean_mids`: it has no reader (S38e: decoded into `SodMark.marks` and never consumed; the roll takes its mids from `last_clean_marks` / the Redis quotes), so (c) changes no Oversee input and only removes the PostgreSQL audit copy of the start-of-day mids. | **No default; recommended (a).** Plainly: under (a) price data (the latest bid, ask and spread per symbol, and the start-of-day clean mids) **stays in PostgreSQL** in these two existing tables, against your instruction that PostgreSQL holds no price data. (a) is therefore an explicit override of that rule that only you can make, not a default. It is recommended because these are latest-mark state that keeps oversight fail-closed, not price history; (b) and (c) are a separate change to oversight. Until you choose, the build changes neither table (S01's 0015 drops only the unused `position_marks`), which is (a) in effect. Please choose. |
| Q16 | Retention in the market-data Redis (§A5) | as stated |
| Q17 | The market-data Redis keeps an append-only file on disk, so history survives a restart | on |
| Q18 | **Decided by you (D2):** the "right after bar end" (POST) notice waits for StoneX's own final bar, however late (latency measured by PR3), up to `decision_cap` (the end of the next bar); a POST after `T + post_grace` is labelled `DELAYED`; there is no streamed-close fallback, and a bar still missing then is `UNRESOLVED` (§B5.2, §B6.4) | decided: wait for the broker's final bar |

**Round-11 confirmations** (v11.1; implementation plan §9.2). Each has a safe default that the build uses; the user
confirms them before S21 / S28 / S30 (and S07 before it merges) depend on them.

| # | Question | Default built |
|---|---|---|
| C1 | After `PRE_NOT_CONFIRMED`: keep testing against the stop as it was before your heads-up move until a bar end confirms (`pre_basis`, §B6.3), or treat the moved stop as the current SL from the next bar | keep the pre-move stop (`pre_basis`) |
| C2 | While the ARM is globally FAILED (§B14): keep recording ticks (tick history cannot be re-fetched) but serve no data and give no advice, or stop recording too | keep recording ticks |
| C3 | A pre-close bar (daily break, Friday close) still not final at its `decision_cap`: re-read at the open and decide it as a REOPEN item (§B5.2), or let the cap at a close mean the end of the first post-open bar | re-read at the open, REOPEN item |
| C4 | Oversee change (PR1, S07): a STOP child at or beyond entry now counts as a stop (the fix for the stop that "disappears" at break-even), **except** a STOP child strictly beyond the entry of a still-unfilled (PENDING / WORKING) entry order, which still counts as no stop (the pre-S07 `missing_stop_loss` SOFT_HALT; counting it would make `risk()` UNKNOWN and blind `MaxAggregateOpenRiskRule` for the account); a LIMIT child no longer counts; a STOP child on the wrong side of the mark is UNKNOWN for 30 s, then a capped SOFT | counts, with the unfilled-entry exception. **The change is present, uncommitted, in the working tree** (`src/oversee/risk.py`, `rules.py`, `sweep.py`; tests `tests/oversee/test_risk.py::test_unfilled_entry_order_with_stop_child_beyond_entry_is_no_stop`, `tests/oversee/test_stop_detail_sweep.py::test_unfilled_entry_with_stop_child_beyond_entry_is_missing_not_stop_present` (new)); it must not be committed or deployed until you confirm |
| C5 | PRE notices (§B8): favour R and favour / risk are rendered at send time from a hash-checked bar, and PRE has no bar (its quote is frozen at fire time and never stored), so a PRE notice shows outcome codes and order state only, with no favour figures; or keep the PRE quote somewhere other than PostgreSQL so they can be shown | no favour figures on PRE |

## 0b. Build status (v11.7)

**What exists** (plan stages S00-S38, 2026-11-16/17; uncommitted). New packages `src/marketdata/` (Part A),
`src/riskmgr/` (Part B), `src/armconfig/` (§B9); runtime wiring `src/runtime/optional.py`, `optional_md.py`,
`optional_plan.py`, `optional_settings.py`, `md_ports.py`, `arm_ports.py`, `request_handlers.py`; stores
`src/db/arm_*.py`; the ARM relay `src/notifications/arm_relay.py`; ops views and commands `src/ops/arm*.py`,
`md_view.py`, `md_acl.py`; migrations 0015-0020 (held back until `XTNL_MIGRATIONS_ACCEPTED_UPTO` allows them,
S38f); deploy assets (PC profile `md`, AWS unit installed but not enabled,
metrics, alerts, runbooks `docs/runbooks/arm.md` and `marketdata.md`). Both enable flags default to `0`. With
`XTNL_MD_ENABLED=1` and `XTNL_ARM_ENABLED=1` an OPEN position with an accepted assignment is armed, decided at PRE
and POST on the bar clock and its notices are queued in the ARM outbox (plan S36b); nothing sends, changes or cancels
a broker order. **S39** (after the PR3 probe, 2026-11-17): StoneX `fetch_price_bars` serves bars
(`src/adapters/stonex/adapter.py::StoneXAdapter`, `price_bars_supported` true); Pepperstone is not bar-capable in v1. The ARM side follows the gate table: `src/riskmgr/golive.py::bar_capable_adapters` (new; moved from `src/ops/arm.py` by the S39 review) is `{stonex}` only once no gate S39 waits for is open (§0b "Go-live gates"), and it is the one bar-capable set both readers use: admission and `arm config accept` (`src/ops/arm.py::BAR_CAPABLE_ADAPTERS`) and the runtime arm-tracker's `bar_sides` (`src/runtime/optional_plan.py::OptionalRuntime._bars_capable`: `price_bars_supported` **and** membership). So until then `arm config accept` refuses StoneX-scoped assignments, and no assignment, wildcard or scoped by account or symbol included, can arm a StoneX position (it stays `UNSUPPORTED(no_broker_bars)`), whatever `XTNL_ARM_GOLIVE_ACCEPT` names.

**What turns each on, and in what order** (plan S38f, round 14). On the single PC the working tree is the deployment:
`deploy/local/start-xtnl.ps1` runs `python -m src.db.migrate` at every start and the engine from the repository. So
nothing reviewed-but-unconfirmed is live by default; each item below is a local setting (never Supabase), read at
start:

| Order | Setting (default) | What it turns on | Before turning it on |
|---|---|---|---|
| 1 | `XTNL_MIGRATIONS_ACCEPTED_UPTO` (`14`) | `src/db/migrate.py::accepted_upto_from_env`: migrations up to this level are applied at the next start (PC: start step 6; AWS: `xtnl-migrate.service`, an operator job); later ones are held back, named in one line, and the start continues (exit 0). An invalid value is the default. `20` applies 0015-0020. | review 0015-0020 (§C1); raise it on the standby first on AWS |
| 2 | `XTNL_OVERSEE_STOP_AT_ENTRY` (`0`) | `src/oversee/stop_rule.py::configure_stop_rule`: the S07 / PR1 Oversee stop rule (a STOP child at or beyond entry counts, a LIMIT child never, the `stop_kind_*` caveats). Off: exactly the pre-S07 rule (`src/oversee/risk.py::effective_stop_detail` with `at_entry` False; a test holds it equal to commit `31d9b58`'s `effective_stop` over generated entity sets). | confirm C4 (§0) |
| 3 | `XTNL_MD_ENABLED` (`0`) | the market-data layer (§C2) | 1 at 20 |
| 4 | `XTNL_ARM_ENABLED` (`0`) | the ARM (§C2); the go-live gate `oversee_c4` counts as accepted only together with `XTNL_OVERSEE_STOP_AT_ENTRY=1` (`src/riskmgr/golive.py::verdict`) and, in the engine, only while the rule Oversee actually runs is the S07 one (`src.oversee.risk.stop_at_entry()`, set once by `build_engine`; R14 review): the ARM always reads stops by the S07 rule and never advises on a rule Oversee is not running | 1, 2, 3 and every go-live gate |

**On a 0014 schema** (every flag off, migrations held back) the engine starts and behaves as before
(`src/db/schema_compat.py`): every pool reads the catalog once at creation (`src/db/pg.py::with_schema_compat`, the
dashboard's pool too) and, without the 0015 columns, runs the core statements in their 0014 form (the `order_kind`,
PR7 instrument and PR8 `event_log` columns and their arguments left out; the decoder column lists select NULL), and the
ingestion orchestrator publishes every row in its pre-0015 shape (`src/dto/versioning.py::without_optional_payload`), so
stored checksums are the pre-0015 ones. The lifecycle's ARM-notice expiry is skipped while 0017's tables are absent;
every other 0016-0020 column or table is used only by the market-data / ARM code. Tests: `tests/db/test_schema_compat.py`
(a fake 0014 connection refusing every new column, a scan of every core SQL constant, and a `pg` test on a schema held
at 0014).

**Round-14 safety fixes** (plan stage S38f, 2026-11-17; user: "Yes, fix those 3"):

| Finding | Resolution | Section |
|---|---|---|
| The uncommitted S07 change and migrations 0015-0019 go live at the next PC start | migrations are opt-in by `XTNL_MIGRATIONS_ACCEPTED_UPTO` (default 14); the S07 rule is opt-in by `XTNL_OVERSEE_STOP_AT_ENTRY` (default off); the engine runs on a 0014 schema; the order is the table above (`tests/db/test_migrate_accepted.py`, `tests/oversee/test_stop_rule_flag.py`, `tests/db/test_schema_compat.py`) | §0b, §C2 |
| `xtnl_arm` could re-key or clear an open FAILED episode | migration 0020: UPDATE on `arm_failures` only on the ping and probe notes (an episode's `counters` and `position_ids` are written by the INSERT only, R14 review); the BEFORE UPDATE trigger `arm_failures_guard` refuses (42501) any identity-column change, a shrinking `position_ids`, any change of a cleared episode and a clear by any role but the table owner's; the S31 privilege self-check also checks the trigger (`tests/db/test_arm_hardening_migration.py`) | §B3, §B14, §C1 |
| D7 counters reset on every restart or takeover | the budget's counters are durable in `arm_budget_counters` (0020; fenced, only on change, restored at the FAILED manager's start; they survive a `redis-md` loss); unclean restarts and takeovers count: `XTNL_ARM_FAIL_RESTARTS_N` (3) within `XTNL_ARM_FAIL_RESTART_WINDOW_S` (1800) while positions are armed is ARM-global FAILED `unclean_restarts` with the immediate ping (`tests/riskmgr/test_budget_durable.py`) | §B14 |

**Round-14 findings: decision path, liveness and sleep** (plan stage S38g, 2026-11-17; user: "Yes, fix the 10"):

| Finding | Resolution | Section |
|---|---|---|
| AT_PRE discounted a stop move that answered an earlier bar's final POST ADJUST (PRE_NOT_CONFIRMED against the ARM's own final advice, a stale `pre_basis`) | while the position is ADVICE_OUTSTANDING and `S_at_T` moved toward entry after this bar's PRE but not past the target of the newest final `ADJUST` (POST, LATE, REOPEN or CORRECTED) of an earlier bar, this bar's PRE levels are not used: `S_at_T` is tested, no `PRE_NOT_CONFIRMED`, no `pre_basis`, caveat `moved_on_final_advice` (`src/riskmgr/pipeline.py::answers_final_advice`, the target read by `src/db/arm_store.py::SQL_FINAL_ADVICE_TARGET`); golden tests long and short, and through the runtime join with the POST on time and DELAYED past the next PRE (`tests/riskmgr/test_pipeline.py`, `tests/riskmgr/test_round14.py`) | §B6.3, §B6.4 |
| PRIOR_VERSION ignored the direction of the newer non-counting version | the non-counting read returns its level (order state); the handle takes the farther from entry of the prior counting level and every newer non-counting level; a newer version holding the handle with no level is `DATA_UNAVAILABLE(stop_at_bar_end)` (`src/riskmgr/stopbasis.py::handle_at_bar_end`); golden: a loosened amend seen only through an untimed REST row gives BELOW_TRIGGER, not ADJUST | §B6.4 |
| Promised liveness pages not built | `src/riskmgr/liveness.py` (new), run by the `arm-status` loop every 5 s: armed positions with more than 2 OPEN bars past their cap undecided, or no schedule (dead-man `arm_overdue_positions`, `arm_status.overdue_positions`, metric view `overdue`; the tracker's old "assigned, not armed" count is now `unarmed`), a stalled BarClock or arm-tracker pass, the arm-relay backlog (15 min); each paged once per episode (`tests/riskmgr/test_liveness.py`) | §B8, §C2 |
| PC sleep: the in-process detectors could not see a Windows sleep; the restart path was undocumented | the BarClock and `md-writer` detectors also count both clocks moving more than 10 s between 250 ms passes as a suspend (`src/marketdata/clockstep.py::SUSPEND_AFTER_MS`), and the uptime tracker's signal reaches both (`EngineSession.add_suspend_listener`); the lease keeper names a fence that follows a suspend and records it node-locally (`src/runtime/sleeplog.py` (new)); the ARM leaves that restart out of `unclean_restarts` and pages each sleep restart once; the restart path is §A7 "PC sleep" (`tests/runtime/test_sleep_path.py`) | §A7, §B14 |
| Migration inventory stale; the schema scan stopped at 0017 | §C1 lists 0018-0020 in order and the "Locks" paragraph names 0019's post-step index; the no-market-data schema scan covers every migration from 0015 on (`tests/db/test_arm_schema_no_market_data.py`) | §B3, §C1, §C3 |

**Gates still open** (each the user's):

| Gate | State |
|---|---|
| §0 ★ Q1, Q2, Q15 | unanswered; the build implements Q1 (a), Q2 `high_low` / `>` / including bar 0, and leaves both price tables unchanged (Q15 (a) in effect) |
| C1-C5 (§0) | unconfirmed; the build runs on the defaults: C1 `pre_basis` kept until a bar end confirms; C2 ticks recorded while globally FAILED; C3 the REOPEN item at the open; C4 the S07 stop rule with the unfilled-entry exception; C5 no favour figures on PRE. Each is switchable later |
| S07 (PR1 Oversee change, C4) | built, present uncommitted; must not be committed or deployed until confirmed |
| S21 (envelope pipeline) | built ahead of the S07 gate, **not accepted**; S36b joined it to the runtime, so the decision path depends on the C4 and C1 confirmations |
| S25 change to `src/riskmgr/registry.py` | needs sign-off (plan §9.1): `parse_params(new_pin=True)` refuses a strategy whose trigger bar spec has no timing row |
| PR3 probe, S39 | probe run 2026-11-17 (live_readonly, results in §A11 PR3); S39 built: StoneX bars enabled in the adapter, `pr3_finality` wired; `XTNL_STONEX_PRICE_UNFILTERED` stays 0 (delivery measured filtered), `XTNL_ARM_STOP_READS` stays 0 (`pr3_stop_children`), finality stays two equal reads |
| Go-live gates (S38e) | `XTNL_ARM_ENABLED=1` is **refused**, and StoneX stays out of `BAR_CAPABLE_ADAPTERS`, until every go-live item below is wired or explicitly accepted by you ("Go-live gates") |

**Pending `.claude/` proposals.** The code already relies on rule and hook text that is not applied; each change is a
proposal file under `docs/design/proposed-claude-changes/`, status **proposed, not applied**, and no test depends on
one. Until they are approved the code and the current rules disagree in these places (rules as written win for any
new work): adapter-isolation §4 does not list `fetch_price_bars` / `fetch_active_stop_orders`
(`S03-price-bars-and-stream-fields.md`, `S04-orchestrator-labels-and-stop-reads.md`); operations §15 does not list
`RestPriority.BARS` (`S06-rest-bars-priority.md`); dto-contracts lacks the new DTO fields and labels
(`S01-dto-contract-fields.md`, `S04-…`); adapter-isolation §5 / operations §10 lack tick-only symbols
(`S05-tick-only-symbols.md`); oversee-rules §2, §5, §6 lack `effective_stop_detail()` (`S07-effective-stop-detail.md`,
also gated by C4); `CLAUDE.md`'s mandatory-skill list, `post_tool_verify.py` `WATCHED_DIRS`, the hook's `LOCKOUT_SCOPE`
name ban, `verify_execution.py`'s lockout check and the new rule files `marketdata.md` / `riskmgr.md`
(`S11-lockout-scanner.md`); the hung-job clean-stop rule (`S22-hung-job-shutdown.md`); the pinned-registry check in
`verify_execution.py` (`S35-pinned-registry-check.md`); the `redis-md` and ARM text of operations, tier-persistence,
dto-contracts, adapter-isolation and oversee-rules §10 (`S37-rules-redis-md-and-arm.md`; S38e amended its Q15 wording,
which no longer records Q15 (a) as answered); md-indicators' own executor (`S38d-indicator-executor.md`); the ARM's
unfenced `arm_status` telemetry, its READ ONLY REPEATABLE READ snapshots (`PgTrackerSnapshot`, `PgArmVersionReader`)
and the operator writes of `arm_accepted` / `arm_controls` as operations §2 / §12 exceptions
(`S38e-rules-arm-exceptions.md`); the accepted migration level, the S07 stop-rule switch and the FAILED /
error-budget hardening (`S38f-round14-switches.md`); the sleep fence and the ARM liveness pages in operations §3,
§9 and §17 (`S38g-round14-sleep-and-liveness.md`); StoneX `fetch_price_bars` enabled after PR3, replacing the S03
sentence about `probe_price_bars` (`S39-stonex-broker-bars.md`); the ARM's entry-time history read in the reconcile
class and the receipt-gap heartbeat hook (`S39b-six-gates.md`). **Interim guard:** the
pytest lockout scan of the real tree (`tests/riskmgr/test_lockout.py`) gates every stage, so a name-ban or import
violation fails the green gate while the hook copy waits; the hook parity test skips with "hook update pending". No
deny-list, `FORBIDDEN_METHOD_NAMES`, `ReadOnlyTransport` path or hook `FORBIDDEN_PATTERNS` is narrowed by any of them.

**Deviations that change behaviour or keys** (from the plan's stage records; the sections named are updated):

| Area | As built | Stage | Section |
|---|---|---|---|
| ARM keys | every key carries `adapter_id` first: `arm_positions` `(adapter_id, position_id, resource, owner)`, `arm_decisions_key (adapter_id, position_id, resource, owner, bar_spec, event, bar_end_ms, revision)`; every other position reference is `adapter_id:position_id` (`arm_failures.position_ids`, notice item keys `adapter_id:position_id:resource:owner:revision`), because two brokers may reuse a position id | S23 | §B5.3, §B8, §B11, §B12, §B14, §C1 |
| PR1 Oversee rule | the unfilled-entry exception (C4 above); the triggerable-side test applies to positions only (an unfilled entry order's children are judged against its entry); the side is judged at the mid while a clean quote is fresh; LAST_MID and no mark are not judged | S07 | §0 C4, §B2 PR1 |
| PRE validity window | **resolved in S38c:** `valid_until = fire + 10 s` (`src/marketdata/timing.py::pre_valid_until_ms` (new)), a 10 s window at every offset of D1's 30-45 s range (the S12-S38 build allowed 31-45 s under the old `min(fire + 10 s, T + p50 − 30 s)`, which was empty at 30 s) | S12, S19, S38c | §B5.2, §B6.7 |
| PRE quote | **S38d:** the quote is the one as it was at fire (`MarketDataPort.quote_at`, the hook's per-symbol quote ring, `src/marketdata/ticks.py::MdHook.quote_at`), LIVE when at most `quote_stale_ms` old at fire; with no quote at fire the PRE is a `DATA_UNAVAILABLE(pre_quote_unavailable)` row (`pre_quote_after_fire` can no longer arise from ticks after fire) | S21, S38d | §B5.2, §B6.3 |
| Non-final bar end | a POST-time `DATA_UNAVAILABLE`, a job fault or a wait for a stop read writes **no** decision row (it would block the bar's final POST on the decision key); the position is marked `pending_event` / `pending_bar_end_ms` and the reason becomes the expiry reason of the span at the cap | S27 | §B5.3 |
| `this_bar_pre`, "previous final at target" | re-read from the committed PRE `ADJUST` handle rows and the newest bar-end decision (`ArmDecisionReader.decision_memory`); unreadable is `NotReady(decisions_unreadable)` | S36b | §B6.3 |
| `S_at_T` | **S38d:** reads the versions of the `stop_loss` field, the current stop children **and every reduce-only child `orders_state` links to the position that may have been live at `T`** (not terminal now, or last changed after `T`: `src/db/arm_versions.py::SQL_LINKED_STOP_CHILDREN`), so a cancel/replace after `T` decides on the pre-replace level; a child created after `T` takes no part (three version statements as before, `history` and `DROPPED_STALE` never carry a level) | S24, S36b, S38d | §B6.4 |
| Decision inputs | **S38d:** the row lists the trigger bar first, then every history bar the strategy and its guards read (series inputs with keyed hashes), and every indicator value read (`I|id|output|bar_end|window_start|hash`, the hash a keyed HMAC of the value's **lineage**: its window's input digest by bar id and revision and the value revision, never the value; `src/riskmgr/types.py::lineage_hash`); `inputs_digest` covers all of them (`src/riskmgr/evaluation.py::decision_inputs`); `entry_time_ambiguous` is always False (no history execution-time source wired) | S36b, S38d | §B8, §B10 |
| Suppressed sentinel | `arm_decisions.notice_id = 0` marks `suppressed` (real notice ids start at 1) | S23 | §B8 |
| Bar-capable adapters | none in v1 (above); an assignment scoped to an adapter is refused `no_broker_bars`, a wildcard one only noted | S34 | §B6.4, §B9 |
| Config install | the candidate is the read itself when it hashes to the accepted hash, else the **accepted subset** (rows set at or before a cut-off near `accepted_at_ms`), so rows added after an accept stay pending; install at the first 15m bar end `T` whose `T + post_grace` is after now; Q12 `automatic` is not built (treated as a manual accept of its hash); the accept is single-person | S33, S34 | §B9 |
| FAILED restore | a position leaves `FAILED` only when no open episode covers it (`src/riskmgr/failed.py`, level-triggered CAS on the ARM pool); `SUSPEND` while FAILED changes only the state it will return to; `arm_rearm` re-runs the arming guard and arms into `FAILED` under an open scope; an ADAPTER clear leaves its SERIES episodes open | S19, S30, S26 | §B12, §B14 |
| Clear answer | the refusal is the code `cause_present_<check>` plus `_first_<request>`; ping copy on ARM channels at Pushover priority `high`, never emergency; no attempt cap | S29, S30 | §B14 |
| md requests | answered with the ARM off: the `arm-status` loop writes the heartbeat row on the engine pool whenever the market-data services run; nodes publish registry tokens only with `XTNL_MD_ENABLED=1`, so `arm config accept` is refused `registry` while it is off | S34, S36b | §B13 |
| `redis-md` index keys | `md:registry:leases` and `md:registry:stored` (ids only), so bootstrap step 4 and retention find every lease and stored series without `KEYS` / `SCAN` | S13, S16 | §A3, §A7 |
| Rollback set | a restored entry raises `md:meta` `write_seq` to the highest restored score in the same script, so new entries are never trimmed below restored ones | S16 | §A7, §A13 |
| `no_trade` (BROKER) | an empty bucket is `no_trade` only on a later closed bar in the same answer, a closed calendar, or a complete empty answer still the latest at its cap; `unfillable` after `history_reach_ms` or 12 answers in a row that leave it unknown; `md-bars` pauses its reads during an adapter outage | S17a | §A4 |
| Cross-check | "within tolerance" is strict (`<`), the median spread comes from the in-memory TICK builder (spreads are never stored) | S17b | §A4 |
| Indicators | ATR window `Count(W + 1)` (the oldest bar lends only its close), warm needs `W` bars; warm "last bucket equals the latest closed input bucket" is checked at read time; **S38d:** the worker is `md-indicators`' **own** bounded executor (`ThreadPoolExecutor(max_workers=3)`, never the loop's shared default executor) with one in-flight thread per indicator (a hang past 5 s FAULTs it, `job_hung`); `close()` reports a hung worker and the stop path then hard-exits; the 250 ms cap is thread CPU measured inside the job | S18, S38d | §A6 |
| Job runner | jobs of one version run one at a time; a timeout counts against the strategy scope; **S38c:** a hung lane is retired and the next job gets a fresh executor, at most 3 abandoned threads alive, a further hang is `restart_required` (ARM-global) | S22, S38c | §B4, §B14 |
| HMAC key ids | `XTNL_MD_HMAC_KEY_ID` (1-255, required) and `XTNL_MD_HMAC_KEY_PREV_ID`, operator-set and only increasing (non-secret env-file settings) | S12, S36 | §A13, §C2 |
| Dead-man | `arm_failed` is an object `{count, global_failed}`; optional fields are omitted while None | S08 | §B14 |
| Tick-only symbols | never published to `market.price.dto` (no Redis price key, no `last_clean_marks` row); a removed one stays subscribed as `md_retired`; skipped as SM10 evidence; kept out of the calendar's deciding paths | S05, S32 | PR5 |
| BARS priority | the per-minute cap is clamped to half the bucket rate; a throttle answer is counted apart, never as an outage; `failing_at_every_priority` may be unknown (then neither FAILED nor `broker_unavailable`) | S06 | PR11, §B14 |
| StoneX stop list read | lists only `activestoplimitorders` (attached children must be confirmed there before `XTNL_ARM_STOP_READS=1`: PR3 did not measure it; S39 added `probe_bars --check-stops`, gate `pr3_stop_children`) | S04, S31, S39 | PR9 |
| AWS `redis-md` ACL | rendered at boot by `src/ops/md_acl.py` (unit `ExecStartPre`) from the two URLs; Redis 7.2 or newer is required (`WAITAOF`) | S37 | §A2, §C2 |
| BROKER retention | **S38e:** `RETAIN_BROKER` (`src/marketdata/store/scripts.py::RETAIN_BROKER`, its own script in md-writer's retention pass) trims every stored BROKER series, demanded or not, below 1 year (or its widest declared backfill) and `md:bars_rev` / `md:quarantine` below 7 days; indicator values of a series no longer demanded are not trimmed (they are trimmed only by the next `IND` write of that indicator) | S38e | §A5 |
| Demand memory ceiling | **S38e:** `src/marketdata/demand.py::DemandBook.memory_symbols` takes md-writer's measured `used_memory` ratio: the formula, cut to the admitted symbols plus the room left below the target | S38e | §A5, §A6 |
| `redis-md` bootstrap failures | **S38e:** only a store that answered counts toward the 3-in-a-row fault; unreachable, timeout, `LOADING` and `BUSY` (`src/marketdata/bootstrap.py::UNREACHABLE_FAILURES`) are retried with back-off (1 s doubling to 30 s) and fall under `fail_md_down_s` | S38e | §A7, §B14 |
| Multi-input indicators | **S38e:** an indicator with an `IndicatorRef` input is refused at registration (`src/marketdata/indicators/engine.py::IndicatorSettings` `indicator_inputs` False) until DAG inputs are read canonically from `md:ind`; v1's ATR has one bar input | S38e | §A6 |

**Kept only in memory** (lost on restart or takeover; each is either rebuilt or fails safe): the BarClock's
correction window (rebuilt since S39b at a term's first correction pass from the committed final decisions still in
their window, gate `corrections_restart`), the job runner's suspend count (the next suspend is signalled again), the
arm-tracker's history-lookup attempts (a new term may look up again), the hook's quote ring (`QUOTE_KEEP_MS`, 15 s), the
BROKER coverage map
per bucket (counts mirrored to `md:meta`; the gap-heal pass rediscovers holes), `unverified_final` per series, the
error-budget counters and the D6 mismatch streak (§B14), the TICK median spread used by the cross-check, indicator
value revisions (the last 1024 values per indicator), the md generation count `n`, the open correction compares
(`OpenChecks`), the notices' Q9 extras, part counters, closed windows, D3 memory and PRE outcomes (an extra queued but
not yet sent is lost on takeover), the "LOOSENED told" flag per term (a restart may show `LOOSENED_AFTER_COMPLETE`
once more) and the tracker's one-shot notice memory.

**Not wired yet:** `trusts_partial_flag` and `trusts_next_bucket` (False; kept False after PR3, S39);
`entry_time_ambiguous` (always False: the history execution time sets a `BROKER` entry time, the ambiguity flag is
not stored, S39b). **Wired in S39b** (the six engineering go-live gates, "S39b" below): the suspend signal reaching
every strategy job, the receipt-gap heartbeat feed, `arm replay --rerun`, the `strategy_conformance` probe running
the version's own code, the history execution-time source, the correction window across a restart. **Wired in S38e:** the
clear probes `series_reread`, `fresh_bar` (`src/riskmgr/probes.py::MdBarsSeriesProbe` over
`src/marketdata/brokerbars.py::MdBars.probe_reread` / `MdBars.probe_fresh_bar`) and `strategy_conformance`
(`src/riskmgr/probes.py::VersionConformanceProbe`), so every FAILED scope kind can be cleared; the D7 triggers
`unresolved` (md-bars feeds `note_unresolved`) and `refetch_failed` (md-bars emits it).

**Inventory: price-derived values in PostgreSQL** (round 12; all pre-existing, none added by this build; Q15 decides):

| Column | Writer | Readers | Status |
|---|---|---|---|
| `last_clean_marks` (bid, ask, spread, ts) | price consumer 10 s upsert and forced open / close / roll writes (`src/db/repository.py::SQL_LAST_CLEAN_MARK`, `TieredStateCommitter._persist_mark`, `TieredStateCommitter.force_clean_marks`) | SM8 seed (`src/integrity/bootstrap.py::RedisBootstrap._seed_live`), RiskSweep marks (`src/db/snapshot_reader.py::SQL_CLEAN_MARKS`), calendar reference quotes (`EngineSession.seed_calendar_quotes`) | Q15, unchanged |
| `sod_marks.clean_mids` | start-of-day row (`src/db/sweep_writer.py::SQL_SOD_INSERT`) | **none** (S38e correction): decoded into `SodMark.marks` by `src/db/rows.py::sod_from_row` and never consumed, so the column is audit only; its values come from `last_clean_marks` / the Redis quotes through `src/oversee/risk.py::PreRollMarkBook` at the roll | Q15, unchanged |
| `account_snapshots.equity`, `.unrealised_pnl` (sampled rows, about one per account per minute) | live account sampler (`src/db/account_sampler.py::SQL_LIVE_SAMPLE`) | SM8 seed (`src/db/integrity_store.py::PgIntegrityDb.latest_live_samples`) | **new to the inventory**; with one open position, unrealised P&L over units plus entry gives back the mark series at the sampling rate |
| `sod_marks.sod_equity`, `.unexplained_increase_at_roll` | start-of-day row (`SQL_SOD_INSERT`) | `MaxDailyLossRule` base (`src/db/rows.py::SOD_COLS`, read with the RiskSweep snapshot) | **new to the inventory**; mark-derived account values |

So the §B3 sentence "the only price data in PostgreSQL is in the two existing tables" holds only if account-level
equity and P&L are treated as account state rather than price data. That is a judgement for you (Q15): keep them
(recommended, as for the two price tables; the RiskSweep and SM8 depend on them), or ask for a separate design that
coarsens or moves them. The build changes none of them.

**Go-live gates** (plan S38e; `src/riskmgr/golive.py::GATES`, the one table the code reads). `XTNL_ARM_ENABLED=1`
leaves the ARM idle for the term, with one `ARM not started` page naming the gate ids, while any gate is **open** and
not named in the local setting `XTNL_ARM_GOLIVE_ACCEPT` (your explicit acceptance, comma-separated ids); the
market-data layer still runs and ingestion and Oversee are untouched. `python -m src.ops arm gates` prints the same
verdict offline (exit 3 while enabling would be refused). **S39** (an adapter in `BAR_CAPABLE_ADAPTERS`) waits for
every open gate except `pr3_finality`, which it closed (2026-11-17); the bar-capable set is derived from the table (`src/riskmgr/golive.py::bar_capable_adapters`, read by admission and by the runtime arm-tracker alike), and a test fails otherwise. An S39 acceptance is recorded in the
table (state `accepted`), never inferred; an `XTNL_ARM_GOLIVE_ACCEPT` entry starts the ARM but never makes an adapter bar-capable. **Deviation (S39, recorded by its review):** the plan's S39 left the stop-read and roll items "as their own gates"; the build makes `pr3_stop_children` and `pr3_roll_prints` S39 blockers too (not `closed_by_s39`), and, as open gates, part of the `XTNL_ARM_ENABLED=1` verdict, so an `XTNL_ARM_GOLIVE_ACCEPT` list complete before S39 must now also name them. Reason: both are PR3 measurements the S39 stage gate was to supply and did not (stop children in the list read; break prints in the roll-window bars), and the table's rule is one rule for every open item, fail closed; accepting them is the user's call, made in the table or in the setting, never inferred.

**User decisions recorded 2026-11-18** (state `accepted` in `src/riskmgr/golive.py`):
- **Q1:** (a) straight to break-even.
- **Q2:** high − low > 1.318 × ATR(14), with the ATR including bar 0.
- **C1:** keep the pre-move stop (`pre_basis`); S21 is accepted.
- **C2:** keep recording ticks while FAILED.
- **C3:** decide at the reopen.
- **C5:** no favour figures on PRE.
- **C4:** confirmed. It counts only with `XTNL_OVERSEE_STOP_AT_ENTRY=1`, which `golive.verdict` enforces even for the
  table acceptance.
- **S25:** registry check signed off.

`claude_rules` stays open until the proposals are applied.

| Gate | State | What is missing, and what it risks |
|---|---|---|
| `clear_probes` | wired (S38e) | every FAILED scope kind has a wired probe |
| `d7_triggers` | wired (S38e) | `unresolved` and `refetch_failed` fire |
| `d4_corrections`, `md_reset_notice` | wired (S38d) | correction re-checks; the MD_RESET notice |
| `broker_retention` | wired (S38e) | 1-year BROKER retention; the measured memory ceiling |
| `redis_md_grace` | wired (S38e) | an unreachable or loading `redis-md` is retried, never an ARM-global FAILED within seconds |
| `oversee_c4` | open (yours) | the S07 Oversee stop rule runs only with `XTNL_OVERSEE_STOP_AT_ENTRY=1` (S38f; off by default: the pre-S07 rule); accepting the gate counts only with the switch on (C4) |
| `s21_c1` | open (yours) | the decision path runs on the unaccepted S21 and the C1 default |
| `q1_q2` | open (yours) | Q1 (a) and the Q2 defaults are built unanswered |
| `c2_c3_c5` | open (yours) | the C2, C3, C5 defaults |
| `claude_rules` | open (yours) | the pending `.claude/` proposals above |
| `s25_registry` | open (yours) | the S25 change to `src/riskmgr/registry.py` (row above) is not signed off |
| `pr3_finality` | wired (S39) | PR3 run 2026-11-17: StoneX 1m / 15m bid and ask bars, final by +5 s; StoneX `fetch_price_bars` enabled; finality kept at two equal reads, unfiltered prices off (delivery filtered) |
| `pr3_stop_children` | open (S39) | PR3 did not measure whether open positions' stop children appear in `activestoplimitorders`, whether manual stop moves stream on ORDERS, or `LastChangedDateTimeUTC` on amendments: `XTNL_ARM_STOP_READS` stays 0; run `python -m src.adapters.stonex.probe_bars --check-stops` (`src/adapters/stonex/probe_bars.py::check_stop_children`) with a stopped open position; the report is conclusive only when at least one StopOrder attached to an `openpositions` row (`StoneXAdapter.fetch_attached_stop_ids`, the independent source) was compared with the list and every open position has a stop child (S39 review) |
| `pr3_roll_prints` | open (S39) | StoneX 15m bid bars across 16:45-17:15 New York were not compared with TICK bars on several days; `roll_affected` is set on every bar 0 overlapping 17:00 New York meanwhile (Q14) |
| `uptime_suspend` | wired (S39b) | the suspend signal reaches the BarClock, md-writer and every strategy job (`src/riskmgr/jobs.py::JobRunner.note_suspended`, from the session's suspend listener and the BarClock's own detector, plus the runner's own monotonic-jump check): a job whose run spans a sleep gets a fresh awake cap and is never a `job_timeout` or a strategy fault (`job_suspended`); the bars the sleep covered (ended, or reached their `decision_cap`, while asleep) are spanned `DOWNTIME` (`src/riskmgr/barclock.py::BarClock._missed_while_suspended`) |
| `receipt_gap_feed` | wired (S39b) | the orchestrator's per-adapter frame hook (`on_stream_frame`, every prices-stream heartbeat while SUBSCRIBED and not lost) feeds `src/marketdata/ticks.py::MdHook.note_frame`; the watch is on per adapter (`src/marketdata/clockstep.py::RECEIPT_GAP_MS`, 20 s); a silence blinds the TICK buckets it overlaps; gap granularity is per adapter, not per subscription (the heartbeat is the channel's own while CONNECTED, §A7) |
| `replay_rerun` | wired (S39b) | `arm replay --rerun` re-runs the real S21 evaluation and job runner for each past bar over as-of ports (`src/riskmgr/rerun.py::rerun_one`, `src/ops/arm_rerun.py::Rerunner`), read-only, no notice: MATCH, DIFF or UNVERIFIABLE by `inputs_digest` and outcome (a stream fence open now does not stop it, `AsOfImages`; a `MARKET_CLOSED` on one side only is `UNVERIFIABLE(calendar_not_recorded)`) |
| `conformance_probe_partial` | wired (S39b) | the `strategy_conformance` probe's job runs the declaration checks and the pinned version's own code against its conformance vectors (`src/riskmgr/vectors.py::vector_problems`) under the job cap: a hung, raising or wrong version fails it |
| `entry_time_source` | wired (S39b) | the arm-tracker upgrades an `ESTIMATED` entry time to `BROKER` from the trade-history execution time (`src/pubsub/orchestrator.py::IngestionOrchestrator.history_execution_time`: read-only `fetch_history` at the reconcile priority, StoneX `ExecutedDateTimeUtc`), in its own task, until bar 0's `decision_cap`; StoneX only (`src/riskmgr/ports.py::HISTORY_EXEC_TIME_ADAPTERS`: on cTrader no id of the position is the broker order id, so no read is spent) |
| `corrections_restart` | wired (S39b) | at a term's first correction pass the BarClock rebuilds its recheck set from the committed final decisions still inside their `decision_cap` window (`src/db/arm_store.py::PgArmStateStore.recent_final_decisions`, newest first over the live window only, the stored input hashes); memory only, no notice re-sent |

**S39b: the six engineering gates** (plan stage S39b, 2026-11-18; tests `tests/riskmgr/test_s39b.py`,
`test_s39b_rerun.py`, `test_s39b_vectors.py`, `test_s39b_entry_time.py`, `tests/marketdata/test_s39b_receipt_gaps.py`,
`tests/pubsub/test_s39b_history_exec.py`, `tests/ops/test_arm_rerun.py`). No migration; both flags stay off; nothing
sends, changes or cancels a broker order. Limits stated: a job still running a full awake cap after the resume is a
genuine `job_timeout`; a job that spanned a sleep and raised is retried, not counted (a version that raises every time
counts on its next, awake run); `arm replay --rerun` reads the account image and the pin as of now (the stop at `T` is
`S_at_T`, the memory the one before the decision; the image's freshness is not judged from today's fences), so a
decision whose handles were moved since is `UNVERIFIABLE(order_state_changed)`, one decided on the exit quote (never
recorded) is `UNVERIFIABLE(exit_quote_not_recorded)` and a `MARKET_CLOSED` on one side only (the SM10 state at `T`
is not recorded; the re-run has the latest schedule) is `UNVERIFIABLE(calendar_not_recorded)`; the history lookup reads at most 5 times per position and term, one a minute,
until bar 0's cap; the conformance vectors are a runtime subset of the build-time suite.

**Round-13 findings: operations and gates** (plan stage S38e, 2026-11-17; tests `tests/riskmgr/test_s38e.py`,
`tests/marketdata/test_s38e_md.py`, `tests/marketdata/test_bootstrap.py`, `tests/runtime/test_optional_bootstrap.py`):

| Finding | Resolution | Section |
|---|---|---|
| FAILED scopes that cannot be cleared | the `series_reread` / `fresh_bar` probes read the broker through md-bars (also while the series is FAILED; they never write or count) and the `strategy_conformance` probe runs the pinned class's declaration checks as one job on the version's lane; the runtime wires every `ClearProbes` port (a graph test asserts none is None) | §B14 Clearing |
| Two D7 triggers never produced | md-bars hands each bucket counted unresolved at its cap to `ErrorBudget.note_unresolved` (and clean bars to `note_final`, `no_trade` to `note_no_trade`) through the fault relay, so two in a row give SERIES `unresolved`; a consumer re-fetch (beyond the rollback set) the broker can no longer serve gives `refetch_failed` once per series episode | §B14 |
| BROKER retention never enforced | `RETAIN_BROKER` every retention pass (1 year or the widest backfill; revisions and quarantine 7 days) for every stored BROKER series, demanded or not; the demand ceiling also reads the measured memory | §A5 |
| `redis-md` unreachable or loading fails the ARM in seconds | uncounted, retried with back-off; `fail_md_down_s` decides (test: a store down or `LOADING` for many attempts gives no fault and ends LIVE once it answers) | §A7, §B14 |
| Gate rows | "Go-live gates" above; the ARM start path and `arm gates` check them; S39 waits for them | §0b, §C2 |
| "Flags off = exactly as before" | §C2 now states the S07 effect | §C2 |
| Q15 reader list | `sod_marks.clean_mids` has no reader (audit only); Q15 (c) restated | §0 Q15, §0b Inventory |
| S37 rules proposal records Q15 (a) | amended: the tables stay as they are until you answer Q15 | `.claude` proposals |
| ARM exceptions not named in operations §2 | `S38e-rules-arm-exceptions.md` lists the snapshots, the `arm_status` telemetry and the operator config writes | §B3 |
| S38e review (2026-11-17) | the `fresh_bar` probe waits for the next bar end and passes only on its finality by `T' + post_grace`; `store_error` (a store that answered with a permanent error) counts toward `bootstrap_failed` again, raised `LOADING` / `BUSY` replies stay uncounted; the reread probe uses `XTNL_MD_ROLLBACK_N`; `arm gates --env-file`; new open gates `s25_registry`, `conformance_probe_partial`, `entry_time_source` | §B14, §A7 |
| Indicator framework not canonical for DAG inputs | multi-input (`IndicatorRef`) indicators are refused at registration until their inputs are read from `md:ind` for the declared window; the limits (inputs from memory, values older than the last 1024 not recomputed on a revision, `indicator(n)` skipping holes) are stated in §A6 | §A6 |

**Round-13 findings: decision path** (plan stage S38d, 2026-11-17; each with a test,
`tests/riskmgr/test_round13.py` unless named; the operations and gates findings are plan stage S38e):

| Finding | Resolution | Section |
|---|---|---|
| `S_at_T` over every stop child | the reader adds every reduce-only child linked to the position that may have been live at `T`; golden tests long and short: a cancel/replace while a delayed POST waits, over a weekend before a REOPEN, before a LATE replay, each deciding on the pre-replace level with a STREAM basis and no span (`tests/db/test_arm_versions_round13.py`) | §B6.4 |
| D4 corrections | **wired:** the BarClock re-checks every decision it committed inside its window (`events_since`, by bucket on an expired cursor) with `corrections.check_decision`; a genuine correction re-runs the evaluation as `CORRECTED` and writes `revision + 1` (older bar INFO; latest bar re-proposed, released, or its release withdrawn: `RELEASE_WITHDRAWN`); the `MD_RESET` notice is wired (the md bootstrap's reset listener queues it); **finality:** a later bucket alone no longer makes a BROKER bar final (`trusts_next_bucket`, False until PR3): two equal reads `broker_bar_settle_s` apart do; **a release (`COMPLETE`) stands only on a bar the verification re-read positively confirmed** (`MdBars.verified`: the same hash, or a revision the store took; an inconclusive re-read, a failed store read, no verification time left, a bucket final from the lane or an untracked series is never confirmed, so the release is held, reason `release_awaiting_verification`, and the bar is spanned at its cap with a page: never a silent release; an `ADJUST` goes at once). A genuine change of one input is a correction even while another input cannot be read now (the re-run reads current inputs). FirstBarGuard's inputs are read and recorded only on a bar where the guard applies. Tests: a bar revised after a COMPLETE withdraws the release (never silent), an ADJUST's bar revised gives a CORRECTED withdrawal then a re-proposal, a release waits for verification (`tests/marketdata/test_round13_md.py`: finality, verification) | §A4, §A13, §B6.6 |
| ATR fault scope | an indicator fault fails its own **guard-input scope** (SERIES kind, id `adapter:symbol:ind:<indicator_id>`), never its input series; a position depends on FirstBarGuard's inputs (its ATR and, for a short, the bid series) only until the last bar a guard applies to (bar 0 under its `anchor`, asked of the guard) is decided or spanned **and** that bar's `decision_cap` has passed (a correction of bar 0 can still read them); a cursor merely set at arming (the fill bucket's start) is not past bar 0. Then they leave its FAILED coverage and its leases (the ATR lease is released). Bar-0 positions stay covered (FAILED, Q3 not used) | §A6, §B12, §B14 |
| PRE quote | frozen at fire from the hook's quote ring (`quote_at`); a 10 ticks/s stream across the fire gives a PRE `ADJUST`, never `pre_quote_after_fire`; §B5.2's valid-until is `fire + 10 s` everywhere | §B5.2, §B6.3 |
| D6 block threshold | `XTNL_MD_MISMATCH_K` is bounded 2-10 (1 refused); the cross-check refuses `mismatch_k < 2` (`tests/marketdata/test_config.py`) | §A4, §B14 |
| md-indicators jobs | their own bounded executor; N hung indicators leave `run_in_executor(None, ...)` responsive; a hung worker at stop ends the process through the hard exit (the S22 rule extended; `.claude` proposal `S38d-indicator-executor.md` for the hook copy of the lockout list) | §A6, §B4 |
| Decision inputs | every input recorded and in the digest; the golden bar-0 conformance test checks that FirstBarGuard's declared history and indicator needs all appear | §B8, §B10 |
| Q10 policy | each `COMPLETE` item carries the position's pinned `loosen_policy` (`REARM` / `TELL_ONLY`, from `SlStrategy.loosen_policy` of the stored pin); `RenderSettings.rearm_on_loosen` is removed; `LOOSENED_AFTER_COMPLETE` always renders tell-only | §B6.7, §B8 |

**Round-12 findings: resolved** (plan stage S38c, 2026-11-17; each with a test, `tests/riskmgr/test_round12.py`
unless named):

| Finding | Resolution | Section |
|---|---|---|
| PRE window empty at 30 s | `valid_until = fire + 10 s`; `pre_offset` 30-45 s (`tests/marketdata/test_timing.py`, `test_pipeline.py`) | §B5.2, §B6.7 |
| No broker bar | the decision-bar sequence is the calendar-OPEN (evaluable) buckets only; an OPEN bucket `md-bars` resolved `no_trade` is `NO_BROKER_BAR` (INFO, final, recorded only, no trigger, no state change, never counted toward `fail_unresolved_n`); a weekend gives no bar and no stuck frontier | §A4, §B6.2, §B6.3, §B6.6, §B14 |
| One stop basis | `src/riskmgr/stopbasis.py::trigger_basis` for PRE and every bar-end event; PRE renders `AT_TARGET_UNCONFIRMED` (caveat `pre_basis_held`) while a basis is held (`test_pipeline.py`) | §B6.3, §B6.4 |
| Stuck frontier | a POSITION scope per stuck position below `XTNL_ARM_FAIL_FRONTIER_POSITIONS_N` (2); ARM-global at that count; the `span_write` probe re-runs the span write of the recorded positions (`test_failed_clear.py`) | §B14 |
| `prior_state` | already built (S30); tests: two covering scopes, clearing one keeps the position FAILED; arming and re-arm into FAILED under the position's own scope | §B12, §B14 |
| AWS leader change | the database is always tried first; the first spool append pages `critical` at once ("ARM FAILED not recorded"); the new leader's counters start from zero (stated; since S38f they are restored from `arm_budget_counters`) | §B14 |
| Later Supabase row | the accepted subset (S33) plus a `high` page when a `config_hash` pause outlives the first install slot after the accept (`tests/armconfig/test_installer.py`) | §B9 |
| `xtnl_arm` privileges | migration 0018: SELECT and INSERT only on `arm_decision_handles`, `arm_correction_audit`, `arm_config_audit`; `arm_decisions` plus column UPDATE (`notice_id`) only; the S31 self-check compares the catalog with the new list (`tests/db/test_arm_round12_migration.py`) | §B3, §C1 |
| Rollback set, data model | already built (S13, S16; tests `tests/marketdata/test_bootstrap.py`) | §A3, §A7, §A13 |
| Generic indicators | the engine reads only each indicator's declared `warm_counts` (W), `warm_span` (clean tail) and `backfill_bars` (W plus warm-up); the bare-lease backfill is a series default, not ATR's (`tests/marketdata/test_demand.py`) | §A6 |
| `RELEASE_POLICY` | `ON_CONFIRMED_TARGET` (kind (a)) or `NEVER`; the registry refuses a strategy without one (`test_pipeline.py`) | §B6.1, §B6.3 |
| Hung strategy threads | lane retirement, a bound of 3 abandoned threads, `restart_required`; CPU-spin limit documented and tested (`tests/riskmgr/test_jobs.py`) | §B4, §B14 |
| S36b / S37 / S38 review majors | already resolved in their own review fixes (`this_bar_pre` from committed rows; Redis 7.2 check in the AWS install; the 10:16 drill) | §B6.3, §C2, §B6.6 |

---

# Part A: the market-data layer

## A1. Purpose and principles

Ticks → bars → indicators, kept only in `redis-md`, read through one port by the ARM now and by automated strategies
later. The broker is the source of truth: **BROKER bar series and indicators built on them are rebuildable**; TICK
series and raw ticks are best-effort (per node, never backfilled). Decision-grade consumers may register indicators on
BROKER series only.

## A2. Store: `redis-md`

* Dedicated Redis (PC: Docker service `redis-md`, `127.0.0.1:56382`, named volume `xtnl_local_redis_md` covered by the
  compose file's "never `down -v`" warning; AWS: one per node, bound to `127.0.0.1:6381` only, like the engine Redis).
  On AWS, remote readers (the dashboard, a future strategy service) reach it only through a second section of the node's
  mutual-TLS proxy (`deploy/redis/redis-tls-proxy.conf`, `${NODE_IP}:6382`) as `md_reader`; the security group opens
  6382 to the VPN CIDR only (and to the peer node under Q15 (b), §A12). Separate from the engine Redis (256 MB on the PC,
  2 GB on AWS, `noeviction`, wiped at every leader change) and from `redis-api`.
* `maxmemory` from the sizing figure (§A5), so full retention sits near 50 %; `noeviction`; the layer enforces retention
  and sheds in a fixed order at fixed thresholds (§A8). The AOF rewrite fork's copy-on-write memory is not counted in
  `maxmemory`, so its headroom is the container or unit memory limit (2 × `maxmemory`, §A5). Memory pages at 70 %.
* Persistence (Q17): `appendonly yes`, `appendfsync everysec`, `aof-load-truncated yes`. An AOF that Redis still refuses
  to load has a `docs/runbooks/marketdata.md` step: move the AOF directory aside, start with an empty store, and let the
  bootstrap and backfill rebuild the BROKER series (§A7, §A13). **If Q17 = off:** closed bars leave the queue on the
  script's reply (§A8); every Redis restart loses the whole store (classified `store_lost`, §A7); BROKER series are
  re-fetched for each indicator's declared `backfill_bars` (§A6) only, and TICK history is gone.
* **ACL** on every deployment, as allow-lists in the pattern of `xtnl_dashboard` in `deploy/redis/redis.conf`:
  `user default off`;
  `md_writer`: `resetkeys ~md:* resetchannels -@all +@string +@hash +@sortedset +@stream +del +unlink +exists +pexpire
  +evalsha +script|load +info +waitaof +ping +select +client|setinfo` (the data commands are listed because Redis also
  checks each command a script runs; `INFO` is `@dangerous`, so it is granted by name, as for `xtnl_engine`);
  `md_reader`: `resetkeys %R~md:* resetchannels -@all +get +hgetall +zrange +zrangebyscore +xrange +xrevrange +xread
  +exists +ping +select +client|setinfo` (never `+@read`, which includes `KEYS` and `SCAN`; it writes nothing, not even
  acknowledgements, §A13);
  `md_health`: `nopass resetkeys resetchannels -@all +ping`.
  Passwords from the secrets source, as `#<sha256>` entries only. ACL tests: `md_writer` cannot run ACL, CONFIG,
  FLUSHDB, FLUSHALL, KEYS, SCAN, DEBUG, SHUTDOWN, REPLICAOF, MIGRATE, FUNCTION or SCRIPT FLUSH, nor touch a key outside
  `md:*`; `md_reader` cannot write; the default user is off. Credential names, where they are kept and the lists that
  keep them from the API child: §C2 Secrets.
* Client timeouts: 500 ms reads, 2 s writes. A timeout is `DATA_UNAVAILABLE` for that use, never a task death.
* `redis-md` is **not** a node health check and never blocks LEADER_INIT; its loss marks series GAPPED. It is
  **unrecoverable**, the §B14 global fault (FAILED and an immediate ping), only under the `redis-md` rows of the §B14
  error-budget table (the one list). Market-data and
  ARM tasks start only after the node reaches LEADER, through a new hook `EngineSession.start_optional_services()`
  (added to the `LeaderSession` protocol and its test fakes) that `NodeController` calls right after
  `machine.go(NodeRole.LEADER)` (`src/runtime/node.py`). The hook is synchronous and never awaits: when
  `XTNL_MD_ENABLED=1` (§C2 Local flags; off by default, and with it off the hook adds nothing) it only adds one
  supervised, non-critical task, `optional-bootstrap` (stamp 60 s), which runs the lockout scan (in a thread), builds the
  `redis-md` client and the ARM pool with explicit timeouts (connect 3 s, pool `min_size 0`), registers the ARM and
  market-data handlers for their `operator_requests` kinds, then adds the market-data and ARM tasks in one idempotent
  step (names already registered are skipped; clients already built are reused). After success it **stays resident**
  (a stamping park loop under `ctx.set_exempt("bootstrap complete")`), so a clean return never counts as a death. The
  lockout scan has two failure results: a **violation** (a banned import or name) parks with a page instead of returning,
  for the rest of the term; **scan incomplete** (a file it cannot read or parse, such as a OneDrive placeholder or a
  locked file) raises, so the supervisor retries it with its backoff and circuit, and it pages after 3 incomplete scans
  in a row. The scan covers the package directories plus the `__file__` of every loaded module of the scanned packages. Failures are retried with the supervisor's backoff and circuit,
  never end the term, and never stop ingestion or Oversee (tests: a pool build that fails once then succeeds ends with the ARM running in the
  same term; the node keeps publishing health while the bootstrap hangs; a raising constructor leaves no abnormal
  `leader_terms` row). Until the writer is live, the hook marks open bars `partial`.
  **DRAINING:** the market-data and ARM tasks watch the session's stop flag (set at the start of `drain`) and start no
  new write or transaction. `EngineSession.aclose` closes the ARM pool and the `redis-md` client in parallel with the
  HTTP client and the term pool, inside the existing `drain_aclose_s` (0.5 s), so `check_drain_budget` is unchanged
  (9.2 s of the 10 s deadline); whatever has not closed by then is dropped (`Pool.terminate()`; `redis-md` sockets
  closed without `QUIT`). Nothing is flushed: closed bars not yet released die with the term (the next term re-fetches
  BROKER buckets; TICK buckets become `blind`). Test: DRAINING stays under 10 s with `redis-md` hung and an ARM query in
  flight.
* **PC start path:** `redis-md` is a separate compose profile (`md`) started in its own, non-blocking step of
  `start-xtnl.ps1`, run only when the loaded env file sets `XTNL_MD_ENABLED=1` (the existing `up -d --wait` keeps waiting only for postgres, redis, redis-api and etcd); its
  healthcheck uses `md_health`. `stop-xtnl.ps1 -Containers` and `status-xtnl.ps1` pass `--profile md`, so the service
  is stopped and listed too. `deploy/local/new-md-credentials.ps1` creates the passwords, the content-hash key (§A13)
  and the read-only ACL file (§C2 Secrets). The dev compose (`deploy/docker-compose.dev.yml`) gains a `redis-md` service (AOF on,
  the same ACL) for the C3 tests, on `127.0.0.1:56383` so it can run beside the PC stack's 56382 (built, S13).
* PostgreSQL stores no ticks, quotes, bars or indicator values.

## A3. Data model

| Key | Type | Content |
|---|---|---|
| `md:ticks:{adapter}:{symbol}` | Stream, explicit ids `<received_ms>-<seq>` from the engine clock (monotonic guard; a backward step uses last id + 1 and is counted) | bid, ask, broker time, `ts_source`, status |
| `md:bars:{series}` | Sorted set, score = bucket start; member = `<bucket_start:13><revision:4>\|` + fixed-width compact record (scaled integers) + an unkeyed CRC32 record digest (for out-of-process readers without the key, §A13) + the bar's content hash (§A13), so two identical bars in different buckets can never collapse | o, h, l, c, tick count, quality, revision, `final_at_ms`, record digest, content hash |
| `md:bars_rev:{series}` | Sorted set, 7 days | superseded revisions, each stamped with `superseded_at_ms` when replaced (for `as_of` reads, §A10) |
| `md:forming:{series}` | Hash | the forming bar, rewritten at every `md-writer` flush (250 ms, §A8) |
| `md:ind:{indicator_id}:{output}` | Sorted set (one per output) | value (quantised), quality, warm, `final_at_ms`, lineage |
| `md:ind_ckpt:{indicator_id}` | Sorted set | state checkpoints every 96 buckets |
| `md:demand:{consumer}:{spec_id}` | String with TTL | demand lease on a series or an indicator (§A6) |
| `md:skew:{adapter}` | Hash | ClockSkew p50 / p99 and settle (§A9) |
| `md:registry:series`, `md:registry:ind` | Hash | id → canonical definition (soft state, rebuilt from leases; series entries are added, never removed, bounded by the symbol universe) |
| `md:registry:leases` | Hash (built, S16) | lease index: `consumer:spec_id` → lease value and expiry, written atomically with each `md:demand` SET by the `LEASE` script; expired entries are skipped and pruned by `BOOT_WRITE` (`EXISTS`), so the bootstrap (§A7 step 4) finds every live lease without `KEYS` / `SCAN` (`src/marketdata/store/keys.py::LEASE_INDEX` (new)) |
| `md:registry:stored` | Hash (built, S16) | stored-series index: every series and tick stream ever written (ids only), by the token-only `INDEX` script, so a later term finds stored `md:meta:*` / `md:ticks:*` for retention and the gap-heal pass (`src/marketdata/store/keys.py::STORED` (new)) |
| `md:meta:{series}` | Hash | first / last bucket, gaps, delivery (`unfiltered` / `filtered`), SM12 state, `last_write_ms` (set by every flush; §A7 classification), validation and cross-check run counters (§A4) |
| `md:events` | Stream, `MAXLEN ~ 100000` | `bar_closed`, `bar_revised`, `gap_opened`, `gap_filled`, `indicator_updated`, `md_reset`; bar events carry the event id `(series, bar_end_ms)` and the content hash (§A13), never the revision as identity; wake-ups only: consumers re-read through the port |
| `md:writer`, `md:boot`, `md:boot_lock` | String | writer token; bootstrap sentinel with the md generation; bootstrap single-flight lock (§A7) |
| `md:rb:{series}` | Sorted set, the newest N (8), score = the per-series `write_seq` (`HINCRBY` in `md:meta`) | the rollback set: the last N bar-end events with id, hash, revision and record (§A13); `write_seq` never decreases within a store (re-based on restore, §A13) |
| `md:quarantine:{series}` | Sorted set, 7 days | bars that failed validation, with the check name (§A4); never copied to PostgreSQL |
| `md:lastmark:{adapter}:{symbol}` | Hash | only if Q15 = (b) or (c) |

**Series** = `(adapter, symbol, bar_spec, side, source)`, **single-source, never mixed**: `TICK` series are built from
ticks; `BROKER` series hold only broker bars. A "best bar" read (BROKER where present, else TICK, labelled) exists only
for display; the ARM never uses it and has no TICK-close fallback (§B6.4). **Bar spec** v1: 1m, 5m, 15m, 1h on the UTC grid (TradingView FX
boundaries). 4h and 1d need the 17:00 New York session anchor and come later with their own golden tests.

## A4. Building bars

* **TICK base:** 1-minute bars per side (bid, ask) from tradeable, broker-timed, non-snapshot ticks; 5m / 15m / 1h are
  exact roll-ups of complete 1-minute bars (quality = worst child). Built only for demanded series (§A6) on
  `XTNL_TICK_SYMBOLS` and held symbols.
* **BROKER:** only for demanded specs, fetched natively (`md-bars`), never rolled up from TICK.
* **Buckets** `[start, start + timeframe)` in broker time. A tick within its timestamp uncertainty of a boundary is
  assigned by broker time and flagged.
* **Finality:** a TICK bar is final at the first tick of the next bucket or at `T + p50 + settle`. A BROKER bar is final
  only on **broker-side** evidence: the same response also returns bucket T+1 (used as evidence only, never stored; see
  Validation), or two reads at least `broker_bar_settle_s` apart return the same OHLC (or the probe's partial flag is
  clear). The first read is at `T + p50 + settle` (the adapter's skew, §A9); `broker_bar_lag_ms`, set from the PR3 p99
  finality lag, only starts the re-read back-off and the lag metric. The same rule applies at a daily break or Friday close.
  `final_at_ms` records when it became final; a bar that fails the zero-trust bar checks is not final. POST
  waits for this finality up to `decision_cap` (§B5.2); no TICK close ever stands in for it. PR3 go / no-go: the p50 /
  p99 lag is recorded here; a p99 above `post_grace` means POSTs are routinely `DELAYED` (§B5.2); no broker bars (for the adapter, or for a side a
  position needs), or a p99 above 5 minutes, make the adapter or that side `UNSUPPORTED` for the ARM (§B6.4, §B7).
  **PR3 (2026-11-17):** StoneX bar `T` was present and unchanged with `T + 1` present at +5 s, +30 s and +5 min (15m bid and ask 4 of 4 samples each, 1m 1 each; p50 = p99 = 5 s, never final 0), so `broker_bar_lag_ms` 5000 and `broker_bar_settle_s` 10 are confirmed and POSTs are not routinely `DELAYED`. Next-bucket finality could now be allowed as an option; S39 keeps the default below (the cost is one more read).
  **As built (S38d, round 13):** until the PR3 probe shows the broker never amends bar `T` once `T + 1` appears, a
  later bucket in the same answer is **not** finality evidence on the live path (`trusts_next_bucket`, False for
  every adapter); two equal reads at least `broker_bar_settle_s` apart are. A release decided on a final bar stands
  only once the bar's verification re-read positively confirmed it (`MdBars.verified`); otherwise it is held and
  the bar is spanned at its cap (never a silent release).
* **Empty broker buckets:** a bucket covered by a complete broker response (read succeeded, not truncated) with no bar
  is `no_trade` (evidence `broker_history`); `blind` applies when the read failed or was truncated, and `md-bars`
  retries it on the §B5.2 schedule until its `decision_cap`. **After the cap** a blind BROKER bucket moves to a
  low-priority **backfill lane** inside the BARS budget and is re-fetched until it resolves or broker history no longer
  reaches it (then `unfillable` in the coverage map); dependants (ATR) are recomputed when it resolves (§A6), so one
  blind bucket never disables the first-bar test for hours. A **gap-heal** pass walks blind buckets inside retention,
  oldest first, at a low rate, in the same lane. **As built (S17a):** an empty BROKER bucket is `no_trade` only on a
  later **closed** bar in the same answer, a closed calendar, or a complete empty answer that is still the latest at
  its cap (never on a forming bar or two empty reads, which a lagging broker would turn into a false `no_trade`);
  `no_trade` neither adds to nor resets the error unit; a bucket is `unfillable` once older than `history_reach_ms`
  (default the 1-year retention until PR3 measures the broker's reach) or after 12 answers in a row that leave it
  unknown. The coverage map is kept per bucket in memory with its counts mirrored to `md:meta`; after a restart the
  gap-heal pass rediscovers holes. A POST of an OPEN bucket resolved `no_trade` is `NO_BROKER_BAR` (§B6.3 (5), S38c):
  the evaluator reads `md-bars`' coverage map, and an unknown bucket (blind, or the map lost on a restart) is never
  taken as `no_trade`, so it waits for the broker's bar as before.
* **Absent means blind:** a missing record is never read as "closed": a scheduled-OPEN bucket with no stored record and
  no positive `no_trade` evidence is `blind`. Each series keeps a coverage map in `md:meta`, so a missing member (lost,
  never written) can never be mistaken for a non-trading bucket, and ATR over such a hole is not `warm`.
* **Revisions:** a late tick (TICK) or a changed re-read (BROKER: one verification re-read at `min(final + 60 s,
  decision_cap − 30 s)`; when no such time remains the bar carries the caveat `unverified_final`) replaces the bucket with `revision + 1`, keeps the superseded record in `md:bars_rev`,
  emits `bar_revised` with the same event id and the new content hash (§A13), and recomputes dependants.
* **Validation (zero trust; every closed bar, TICK and BROKER, on write and on read; checks return values, never
  raise).** Only bars whose bucket end is at most 5 s after receipt are validated, stored or counted; a BROKER
  response's forming bar (bucket end later than that) is finality evidence only (bucket T+1 above), never validated,
  stored or counted, and never a check failure. Hard checks block use: (1) prices > 0, `h ≥ max(o, c)`,
  `l ≤ min(o, c)`; (2) the bucket start is on the spec grid and inside the requested range `[from, to]` (BROKER) or is
  the bucket built (TICK); (3) the side is the one requested (against the response's own price-type field where StoneX
  returns one, recorded by PR3; a TICK bar carries the side it was built from); (4) the bucket end is not in the
  future: at most 5 s after receipt (the dto-contracts `future_timestamp` rule; for a BROKER response this is the
  filter above); (5) on read, the stored content hash matches (§A13), checked under any key id the keyring retains, by
  the engine and the ops CLI, which hold the key; an out-of-process reader without the key checks the unkeyed CRC32
  record digest instead (integrity only, not authenticity). The generation is checked per read session, never per bar
  (§A13), so a restart never quarantines history. A bar that fails on write is not stored: its record goes to
  `md:quarantine:{series}` and its bucket is `blind`, never `no_trade`; a stored bar that fails on read is returned as
  `blind` (reason `quarantined`) and moved there. `md-bars` re-fetches a quarantined BROKER bucket like a failed read,
  and the first valid read replaces it. The latest quote is checked the same way (bid > 0, ask ≥ bid, check (4)); a
  failing quote is not kept and `quote()` reports `STALE`.
  **Cross-check:** each final BROKER bar is compared with the TICK bar of the same bucket and side (leased per §A6),
  **field by field**: the close only when the TICK bucket is `close_eligible` (its coverage is complete: subscribed
  throughout per the PR6 intervals, no drops or lost updates in the bucket, last tick broker-timed), which can hold
  under `filtered` delivery too; high and low only when the TICK bar is `complete`. The fields that are eligible form
  the bucket's eligible vector; with none eligible the cross-check is skipped. Each eligible field must agree within
  `k` × that bucket's median clean spread (`k` a local setting, default 1.0, confirmed on PR3 data; built as a strict
  `<`, and the median spread is kept per bucket in memory by the TICK builder, never stored, so buckets built before a
  restart are not checked, S17b). A bar within
  tolerance of its own TICK bar is always a match. A time shift (bucket T − 1 or T + 1) or a side swap (the other
  side's TICK bar) counts as a mismatch only when bucket T fails the tolerance **and** the neighbour or the other side
  passes it by a margin of at least 0.5 × tolerance on the eligible vector; the shift test is skipped when the
  bucket's range (high − low) is below 3 × its median spread, so a quiet, flat series never flags. One mismatch flags:
  a `source_mismatch` caveat on decisions that use the bar, and a metric; no page. **K** consecutive mismatches on a
  series (default 3, a local setting), or K invalid buckets in a row inside the demanded window, block the series: its
  decision reads return `DATA_UNAVAILABLE(source_mismatch)` or `DATA_UNAVAILABLE(invalid_bar)`, and it is a §B14
  series fault (FAILED, an immediate ping, cleared only by the operator). K counts buckets, never reads, in the one
  counting unit defined in §B14 (decode errors and failed reads of a bucket are retries inside its healing). A skipped
  cross-check neither flags nor resets the count; the skipped ratio per series is a metric, and more than half of the
  last 16 demanded buckets skipped pages. Each series has a **D6 status** in `md:meta` and `md show`: `ACTIVE` (close,
  high and low checked), `CLOSE_ONLY` (only the close is ever eligible, typical of `filtered` delivery) or
  `UNAVAILABLE` (the cross-check cannot run; only the hard checks apply). Quarantine entries, metrics, pages and reason
  text carry the series id, bucket and check name only, never a price, and none of it goes to PostgreSQL. Golden
  tests: a corrupt bar is quarantined at once and its bucket is `blind`; under `filtered` delivery a feed shifted by
  one bucket reaches `source_mismatch` within 3 buckets, and a bid / ask swap is detected; a flat Asian session with
  equal consecutive closes never flags; a `redis-md` restart quarantines no stored bar; a forming bar in a response is
  never stored or counted.
* **Quality:** `complete`; `partial` (started inside the bucket); `degraded` (the hook missed ticks: adapter drops, lost
  updates, receipt-time ticks); `no_trade` (positive evidence: subscribed with heartbeats throughout and the broker
  returned no bar; the heartbeats are per adapter, so a receipt gap is per adapter too, §A7 "Suspend and clock
  steps"); `blind` (unknown, including a quarantined bar, above). Under `filtered` delivery (PR3), TICK high /
  low quality is capped at `partial`. Writer-side tick overflow never degrades a bar (closed bars have their own queue,
  §A8; only its 6 h bound makes a bucket `blind`).
* **Evaluable bucket:** a bar exists, and the schedule is OPEN at some instant in it **or** the schedule says closed but
  the bucket holds a final BROKER bar (the broker traded, which SM10 shows as `OPEN_BY_EVIDENCE`). The rule needs only
  bars and the schedule, so canonical values stay reproducible. The BarClock fires PRE and POST while SM10 is
  `OPEN_BY_EVIDENCE`, and decisions on such a bucket carry a `calendar_mismatch` caveat (test: a holiday marked closed,
  the broker trading, a position armed). Only evaluable buckets with a bar are indicator inputs; `no_trade`, closed and
  rollover-only buckets are skipped (as on the chart).

## A5. Retention and sizing (Q16)

| Data | Retention | Built for |
|---|---|---|
| ticks | 2 h (`XTRIM MINID` from the engine clock; `MAXLEN ~ 200000`) | tick symbols and held symbols |
| TICK 1m | 7 days | demanded only |
| TICK 5m / 15m / 1h | 90 days | demanded only |
| BROKER 15m (v1) | 1 year (never less than the widest declared `backfill_bars`, §A6) | demanded only |
| indicator values | as their input series; checkpoints 30 days | demanded only |

Sizing: `Σ_series (bars retained × bytes per member) + Σ_indicators (values × bytes) + ticks + revisions + events`.
Every retained series is far above `zset-max-listpack-entries` (a 15m series holds up to 35,040 members a year), so
sorted sets are **skiplist**-encoded: about 270 bytes per member (record and content hash, skiplist node, dict entry,
allocator rounding), about 300 with indicator lineage. One symbol with v1 demand (TICK 1m bid + ask for 7 days, TICK 15m
bid + ask for 90 days, BROKER 15m bid + ask for 1 year, ATR on both: about 180,000 members) is about 50 MB, plus the
tick stream at `MAXLEN` (about 10 MB) and `md:bars_rev`, `md:quarantine`, the rollback set and checkpoints (about
2 MB): about 60 MB. 8 symbols plus `md:events` (about 15 MB) come to about 500 MB. `maxmemory 1gb` on the PC and on each
AWS node, so full retention sits near 50 %; the fork headroom (§A2) is `mem_limit: 2g` on the PC container and
`MemoryMax=2G` in the AWS unit. The figures are verified by a load test filled to full retention before production and
recorded here. **Enforced (S38e):** md-writer's retention pass runs `RETAIN_BROKER` for every stored BROKER series
(the bootstrap hands over the stored-series index, so a series no longer demanded is trimmed too): bars below 1 year
(or the widest declared `backfill_bars` when longer), `md:bars_rev` and `md:quarantine` below 7 days; the rollback
set is never trimmed. Shedding (§A8) still never touches BROKER series: this is retention, not shedding. The demand
ceiling (§A6 Demand) is the formula above cut to the admitted symbols plus the room the **measured** `used_memory`
leaves below the target, so history that piled up is counted as it is.

Load test result (plan S38, `tests/marketdata/test_load.py`, marker `md`, run 2026-11-17 against the dev `redis-md`
(Redis 7.4, `md_writer` ACL, AOF on), synthetic instruments): one symbol filled through the real `FLUSH` / `IND`
scripts to the v1 retention above (BROKER 15m bid + ask for a year, TICK 15m bid + ask for 90 days, TICK 1m bid + ask
for 7 days: 107,520 bar members; ATR(14) on both BROKER sides for the year: 70,080 indicator members; the tick stream
at its 200,000 cap) took **57.7 MB** of `used_memory`, about 340 bytes per member with the tick stream spread over
them (checkpoints, `md:bars_rev` revisions and the forming bars not filled). 8 symbols plus `md:events` at its cap
project to **about 476 MB**, under half of `maxmemory 1gb`, as sized above. Recompute: 16 ATR(14) series (8 symbols ×
bid / ask, 7 days stored) registered with the real `md-indicators`, then 96 bar closes on all 16 at once with every
8th close revising a bar 10 buckets back (3,440 values written): the worst event-loop lag measured on the same loop
was **29.9 ms** (limit 100 ms).

## A6. Indicators

```
Indicator (Protocol)  -- general from v1, so multi-input and multi-timeframe indicators need no protocol change
  KIND, VERSION                 frozen; registry keyed on (KIND, VERSION); never removed while pinned
  CLOCK(params) -> BarSpec      the output timeframe that drives step() and keys md:ind (part of indicator_id)
  OUTPUTS: tuple[Output, ...]   name + unit (PRICE | RATIO | COUNT); e.g. ("value", PRICE) for ATR;
                                ("upper","mid","lower") for Bollinger
  Params; parse_params(raw)     bounds; unknown keys rejected
  inputs(params) -> tuple[InputRef, ...]          SeriesRef | IndicatorRef(id, output): a DAG
  window(params) -> Mapping[InputRef, Count(n) | Anchored(anchor_spec, max_bars)]
                                bars / values per input that define one value (ATR: Count(W), W = 10 × length);
                                Anchored = from an anchor (e.g. a session start), at most max_bars
  DECIMAL_CONTEXT               pinned per VERSION
  QUANTUM(output)               per output, from its unit, pinned per VERSION (Precision below)
  seed(history: Mapping[InputRef, Sequence[Bar | Value]]) -> State
  step(state, aligned: Mapping[InputRef, Bar | Value | NoTrade]) -> (State, {output: value})   # pure, deterministic
  provisional(state, forming: Mapping[InputRef, Bar | Value]) -> {output: value}
```

`aligned` holds, for each input, relative to the CLOCK bucket t: the input's bar for t (same timeframe); its last
**closed** bar (higher timeframe); or the sequence of closed child bars inside t (lower timeframe); or a `NoTrade` /
`Closed` marker. `register()` checks for cycles, computes the topological recompute order, and checks that every input's
window fits inside that input's retention. ATR implements this signature with one input on its own clock. C3 includes a
lower-timeframe golden test.

**Precision:** values are computed in exact Decimal under the pinned context (`ROUND_HALF_EVEN`) and **quantised only for
storage and display**; the quantum is per output, by its unit, pinned per VERSION: a PRICE output uses
`price_increment × 10^-4` (per instrument, pinned in the indicator spec), a RATIO output `1e-6`, a COUNT output 1.
Decisions that compare against a threshold use the exact canonical value recomputed from the W bars, never a stored,
quantised one.

**Write path for BROKER data:** `md-bars` validates each bar (§A4) and hands it to `md-writer`'s closed-bar queue, so
every bar has one writer, the same Lua script family (writer token and `md:boot` checks) and the AOF release (§A8),
retried until acknowledged. Dependent indicators are then recomputed in one place, the recompute queue of the
`md-indicators` task, in order per indicator. The order is always: the bar written, with `bar_closed` emitted by the
same flush script (event id and content hash, §A13); then the indicator value written (or the indicator `FAULTED`);
then `indicator_updated` emitted after that write. POST waits for both: bar T final **and** its indicators updated for
T (§B5.2). If the indicator write fails after the bar write, the value is simply not `warm` for that bucket and the ARM
sees `first_bar_pending` until it succeeds.

* **Canonical value (reproducible from bars alone):** the value at bucket t is computed over exactly the `W` evaluable
  input bars ending at t: seeded per the class rule (ATR: simple average of the first `length` true ranges of the
  window, the first TR using the previous bar's close when it exists), then stepped to t, quantised to `QUANTUM`. ATR's
  `W(length) = 10 × length` (140 at the default 14; the seed then keeps a weight of about e^−9). **Generic warm-up
  (round 12, S38c):** the engine never assumes an ATR `length`. Each indicator class declares, per spec,
  `warm_counts` (the evaluable bars `W(input)` a warm value needs), `warm_span` (the clean tail: trailing clock buckets
  that must hold no `blind` bucket) and `backfill_bars` (W plus its warm-up); the admission check (§B4), the
  backfill (§A7), the retention check and first-sight warm-up read only these (ATR declares `W`, `3 × length` and
  `W + 3 × length`), and a bare series lease (no indicator) backfills a series default (182 bars). A new indicator
  class needs no engine change (tests: the S18 test indicators and `tests/marketdata/test_demand.py`). For windowed-seed
  indicators (ATR) the canonical value **is computed on every update** (at most 500 Decimal steps per 15-minute bar is
  negligible) and that is the stored value; an O(1) incremental path is allowed only for indicators whose sliding
  form is exact (e.g. SMA with integer running sums), and is the same value by construction. Replay and recompute use
  the same definition, so the same bars give the same value. **Golden tests:** TradingView's `ta.atr` seeds its RMA at
  the chart's first bar, not the window's, so the within-quantum reference is a small Pine script whose RMA is seeded
  at the window's first bar (a `bar_index` offset); plain `ta.atr(length)` is compared at a stated relative tolerance
  of 1e-3. Vectors at lengths 5, 14 and 50.
* **Lineage** on every value: `(indicator_id, window_start_bucket, input_revision_digest, value_revision)`.
* **Demand (leases):** a consumer asserts `md:demand:{consumer}:{spec_id}` (a series id or an indicator id; an
  indicator lease also keeps its input series live) with a TTL (60 s) and renews it; the arm-tracker also leases the
  TICK series of every armed symbol for the §A4 cross-check (its whole lease set: §B4).
  A spec is live while any lease is live; the registry is soft state rebuilt at bootstrap from the in-process lease set
  and every live lease in the store (§A7 step 4). Demand is **bounded** and
  **prioritised**: lease classes `ARMED > PREWARM`; to admit an ARMED lease the layer evicts PREWARM leases (newest
  first) and refuses ARMED only at a hard ceiling computed from the §A5 formula and the per-adapter BARS budget (a local
  setting, default 8 decision-grade symbols per adapter, counted separately from the TICK subscription cap); the BARS
  budget includes the PR9 stop-read cost of armed handles. v1 accepts
  only the 15m bar spec for decision-grade demand. `register()` computes
  the canonical value synchronously from stored bars (in a worker thread) before reporting `warm`; a value is `warm`
  only if its window has its declared `warm_counts` evaluable bars with no `blind` bucket in its declared `warm_span`
  (ATR: `W` and `3 × length`) and its last updated bucket
  equals the latest closed input bucket.
* **Multi-input / multi-timeframe:** every bar and value carries `final_at_ms`; reads take `as_of_ms` and return
  what was current then (§A10). A multi-input update for bucket T fires when every input has a final bar for T (or a known
  `no_trade`), or at `T + max settle` with quality `degraded`. A higher-timeframe input contributes only its last
  **closed** bar (no lookahead, like `request.security` with lookahead off); a forming higher bar appears only in
  `provisional()`.
* **CPU budget:** seeding, recompute and backfill decoding run in a worker thread with the pinned Decimal context, or in
  chunks of at most 20 ms with `await asyncio.sleep(0)` between them; recompute jobs are queued and coalesced per
  indicator. Metric: recompute queue depth and duration. **As built (S18, S38d):** the worker is `md-indicators`' own
  bounded executor (`ThreadPoolExecutor(max_workers=3)`, `src/marketdata/indicators/engine.py`; the lockout allows
  `concurrent.futures.ThreadPoolExecutor` in that one file only, `src/riskmgr/lockout.py::FUTURES_FILES`), never the
  shared default executor, with one in-flight thread per indicator; a hung worker at stop ends the process through
  the hard exit; a thread still
  running after 5 s FAULTs that indicator at once (`job_hung`) and `md_rebuild` is refused `busy` until it returns; the
  250 ms cap is the job's thread CPU, so an over-cap job is discarded after it finishes. ATR's window is `Count(W + 1)`
  (the oldest bar only lends its close to the first true range; warm needs `W` bars); every value is the canonical
  recompute, so checkpoints are written and verified but never used to resume; on a revision only the values held in
  memory (the last 1024 per indicator) and the newest are recomputed, 96 per job; value revisions are node-local, in
  memory. The "last updated bucket equals the latest closed input bucket" half of `warm` is checked at read time
  (`MdIndicators.is_warm`).
* **Fault containment (per series, per indicator):** recompute runs in its own supervised task, `md-indicators` (stamp
  10 s), separate from `md-bars`. Each recompute job and each series' fetch, decode and validation runs in its own
  try/except and ends in a result value, never an exception that reaches the task. An indicator whose `seed`, `step` or
  checkpoint decode raises, whose value is not finite or outside its class's declared range (ATR ≥ 0), or whose job runs
  over 250 ms of CPU is re-seeded from stored bars and retried; 3 faults in a row open that indicator's own circuit:
  `FAULTED` (never `warm`, the reason in its lineage, jobs dropped until `md_rebuild` re-seeds it, §B13), consumers get
  `DATA_UNAVAILABLE(indicator_faulted)`, and it is a §B14 series fault for the series and positions that use it. An
  undecodable broker response is a failed read of that series only, counted by §A4. Neither ever opens the `md-bars` or
  `md-indicators` task circuit, so every other series keeps its bars and indicators. Only a fault outside any job (the
  task itself crash-looping) opens a task circuit, which is the §B14 global fault; the `md-indicators` `on_quarantine`
  hook then drops the head job and marks its indicator `FAULTED` (operations §3). Test: a planted raising indicator and
  a malformed broker payload leave ATR and bar fetching running for every other series, and the affected series reaches
  FAILED and a ping after 3 faults.
* **Multi-input (DAG) limit (S38e).** As built, an `IndicatorRef` input would be read from the source indicator's
  values held in this process (at most 1024, only those computed in this term), not from `md:ind`, so after a restart
  or a coalesced job its window could have holes with no `blind` marker; a revision recomputes only values held in
  memory; `MarketDataPort.indicator(n=k)` returns the newest `k` stored members and does not mark missing buckets.
  Until DAG inputs are read canonically from `md:ind` for the declared window (with missing buckets as holes that
  fail the warm rule), **multi-input indicators are refused at registration** (`IndicatorSettings.indicator_inputs`
  False; the DAG planner and its tests stay). v1's ATR has one bar input and is unaffected.
* **v1 ships `ATR`.** SMA, EMA, RSI, Bollinger bands, cross-symbol and higher-timeframe indicators are each one class
  plus golden vectors against TradingView, with no change to the store or the port (indicators over another
  indicator's output wait for the canonical DAG read above).

## A7. Writer token, bootstrap and continuity (SM12, per series)

* **Token** `md:writer = (cluster_id, epoch revision, node, term nonce)`. A new leader, after a lease check and after
  confirming `engine_epoch` in PostgreSQL is its own (cluster_id adoption included), SETs it unconditionally; every write
  script requires equality, so a fenced ex-leader cannot write.
* **Sentinel** `md:boot = run_id:cluster_id:node:generation` (the md generation, §A13), written **last** by the
  bootstrap. The writer holds the expected value and every ordinary write script requires **equality** (not just
  presence), so a flush, an older AOF restored, or a takeover all return `NO_BOOT`. Because an AOF reload restores the
  old sentinel too, a Redis **restart** is detected on the connection: on every (re)connect the client reads
  `INFO server run_id`, and the expected `run_id` is also passed in `ARGV` and checked inside each write script; a
  mismatch forces `NO_BOOT`. Lease writes (`md:demand:*`) check only the writer token, never `md:boot`, so the
  arm-tracker's re-assertion after `NO_BOOT` or a takeover lands at once.
* **Bootstrap** (mirrors SM8; owned by `md-writer` alone; run at its start in every term, on any `NO_BOOT` and on an
  `md_writer_reset` request, §B13; these bootstraps also run while the ARM is globally FAILED, because they repair the
  store and never give advice; the ARM-scope clear probe runs the same steps as a **trial** bootstrap under the writer
  token and `md:boot_lock`, §B14 Clearing): (1) SET the token again if it is missing (same lease and `engine_epoch` checks as
  above); (2) take `md:boot_lock` (`SET NX PX 10000`, renewed every 2 s); the bootstrap scripts check the token and the
  lock instead of `md:boot`; (3) drop the in-memory indicator state, mark every series GAPPED from `min(last final
  bucket in md:meta after the reload, the writer's in-memory last final bucket)`, clear `md:forming:*` for the series
  the writer knows (memory, `md:registry:series` and `md:registry:stored`, never `KEYS` / `SCAN`), and reconcile the
  rollback set (§A13): memory to store where the store lost it, store to memory (hash-checked) where the process lost
  it; `BOOT_WRITE` sets `md:meta` `write_seq` to max(stored `write_seq`, the highest restored entry) in the same script,
  before any FLUSH (built, `src/marketdata/store/scripts.py` `BOOT_WRITE`); (4) take the
  demand set synchronously from the arm-tracker's in-process lease set (empty at a fresh start; leases written later
  register the rest) plus every live lease already in the store (read through the `md:registry:leases` index, expired
  entries pruned), and write the registry and the leases; (5) write `md:boot` with the new generation last and
  release the lock. It takes seconds and does no broker I/O; meanwhile the rings and the closed-bar queue hold the data
  (§A8). Only then does `md-bars` run SM12 BACKFILLING per series (backfill and verify the declared `backfill_bars` for every
  demanded BROKER series) through the ordinary scripts. A bootstrap that fails is retried with back-off; 3 failures in a
  row make the store unrecoverable (§B14). **Only a store that answered counts (S38e):** a refused script, an
  inconsistent state or a restore error. A store that is unreachable, times out, or replies `LOADING` (AOF replay at
  start-up, after a Docker restart or a PC wake) or `BUSY` is retried with back-off (1 s doubling to 30 s) and never
  counted; while it lasts, `redis-md` is unusable and the §B14 `fail_md_down_s` time counter decides (only while an
  armed market is OPEN). Tests: a restart after 10 minutes of downtime leaves the leases re-registered
  and the BROKER series LIVE before the next POST; tick writes resume within 5 s of `NO_BOOT`.
* **Classification** (info only; shown in `arm_status`, `md show` and the §A13 reset notice): each node keeps its own
  record of the sentinel and the newest `last_write_ms` its `md-writer` wrote, in its own `arm_status` row (keyed by
  node). A bootstrap compares the store with **this node's** record, never with the previous term, which on AWS may
  have run on the peer. `intact`: the sentinel equals the record and the newest `last_write_ms` in `md:meta` is at most
  2 s behind it (the `everysec` AOF window). `store_foreign` (AWS): intact, but other nodes have led since; the series
  are GAPPED from the record and backfilled. `store_lost`: the sentinel is missing or older than the record, or
  `md:meta` is more than 2 s behind it (a lost or older AOF, a flush; with Q17 = off, every restart). As built (S16) a
  fourth class `fresh` (no record, empty store) sends nothing, and `NO_WRITER` (a FLUSHALL) is treated like `NO_BOOT`. No class pages:
  the store heals by bootstrap and backfill (§A13); only a store that cannot heal reaches §B14.
* **SM12:** `EMPTY → BACKFILLING → LIVE`; `LIVE → GAPPED` (engine down, subscription lost, suspension, outage,
  including a broker outage: the adapter not STREAMING or its REST reads failing at every priority, which pages
  `broker_unavailable` once and is never FAILED, §B14);
  `GAPPED → LIVE` once BROKER series are backfilled (TICK gaps stay `blind`); any state → `FAILED` (§B14) →
  `BACKFILLING` after an operator clear. The backfill completes each indicator's
  warm-up range before publishing a value. It runs inside `md-bars` after LEADER, rate-limited, never in LEADER_INIT.
* **Suspend and clock steps:** besides the uptime tracker's suspend event, `md-writer` and the BarClock each run their
  own wall-versus-monotonic detector (a step over 2 s in either direction) and a per-subscription receipt-gap detector
  (no inbound frame for longer than the keepalive interval while SUBSCRIBED). On any trigger: every TICK bucket
  overlapping `[last good flush, now]` becomes `blind`, forming bars reset, and consumers get
  `DATA_UNAVAILABLE(clock_step)`. ClockSkew drops samples taken before the step and re-baselines; it is "settled" after
  20 post-step fresh samples with `p99 − p50 ≤ 250 ms` (never compared with the pre-step value); not settled 10 minutes
  after the market is OPEN pages. As built: the wall-versus-monotonic detectors run; since S38g each also counts a
  **suspend** (both clocks moved more than 10 s between two of its 250 ms passes, `SUSPEND_AFTER_MS`): on Windows
  `time.monotonic()` keeps counting through a sleep, so a sleep is no step, and before S38g only the uptime tracker's
  "both deltas far beyond the tick" branch (120 s) saw it. A suspend blinds the TICK buckets of `[last wall, now]`
  and resets the forming bars exactly like a step, and holds one BarClock pass. The uptime tracker's suspend signal
  also reaches both (`EngineSession.add_suspend_listener`: the BarClock's one-shot latch, `MdWriter.note_suspended`).
  The receipt-gap detector runs only for adapters with a stated keepalive interval fed with heartbeats. As built
  (S39b, gate `receipt_gap_feed`) its granularity is **per adapter, not per subscription**: the feed is the prices
  channel's HEARTBEAT, which the channel makes locally whenever the client reports CONNECTED (not an inbound broker
  frame), and a heartbeat refreshes every market of the adapter. So a gap is found only when the whole adapter stops
  looking connected; a single market whose subscription went silent while the session stays up is not blinded by the
  detector (it still shows as missing ticks and, for BROKER series, as a missing bar).
* **PC sleep** (round 14, plan S38g). The path a sleep actually takes on the single PC depends on its length:
  * **shorter than the fence (12 s):** the process keeps its term; on wake the BarClock and `md-writer` see the
    suspend (above), the uptime tracker's own detector fires only for gaps over 120 s (then SM4 RESYNCING and a
    `resume_after_sleep` catch-up). The BarClock decides the bars the sleep covered on its next passes, or spans them
    at their cap.
  * **longer than the fence (the normal case for a real sleep):** the lease keeper's monotonic deadline has passed
    on wake, so it fences at once (hard exit, code 75, operations §3); its reason names the suspend
    (`process suspended N s`) and it first records the fence in the node-local `sleep_fences.jsonl` next to the spill
    file (`src/runtime/sleeplog.py`). A keeper gap alone cannot tell a suspend from a stalled process (a GIL hog, a
    VM pause, swap thrashing, a wall-clock step), so the keeper confirms it with a clock that stops during sleep
    (Windows `QueryUnbiasedInterruptTime` against `GetTickCount64`, Linux `CLOCK_MONOTONIC` against
    `CLOCK_BOOTTIME`): only when the sleeping clock moved more than 5 s further than the non-sleeping one is the fence
    a sleep fence; any other long gap is an ordinary fence named `process stalled N s`, so a loop of stalls still
    reaches the FAILED restart budget (round 14 review). `run-local.ps1` starts the engine again; LEADER_INIT closes the old term `lost`
    and runs the `startup` catch-up (operations §17). For the ARM this costs what any restart costs: the BarClock's
    in-memory state (verification results of md-bars; the correction window is rebuilt from the committed
    decisions since S39b, gate `corrections_restart`), the TICK
    median spreads of the D6 cross-check (buckets built before the restart are not checked) and one bar's PRE; the
    D7 counters are durable (§B14). The ARM treats it so: the bars missed while asleep are spanned (`downtime`, one
    span per run) and the next bar is decided normally; the sleep's own restart is **not** an unclean restart (the
    first `lost` / `init_failed` term end within 15 min after a recorded sleep fence is left out of
    `unclean_restarts`, `SleepAwareRestarts`; any other end still counts, so a crash loop between sleeps still reaches
    FAILED); and every sleep restart is paged once when the ARM starts ("ARM: engine restarted after a PC sleep").
  * **Drill** (`tests/runtime/test_sleep_path.py`): sleep 60 s starting 30 s after a releasing bar end; the engine
    restarts into a new term and the release (`COMPLETE`) is decided right after it, with no FAULT page; the sleep's
    restart does not count toward `unclean_restarts`, three crashes do.
* **AWS:** one `redis-md` per node, not replicated. Readers trust a node's data only while `md:writer`'s epoch equals
  the current leader epoch (from `engine_epoch` / the leader heartbeat); otherwise reads return liveness `STALE_NODE`. At
  takeover the new leader clears `md:forming:*` before its first write. TICK history is per node and best-effort;
  decision-grade data is BROKER-only and is backfilled after a takeover. The dashboard follows the same freshness rule.

## A8. Write path (never blocks ingestion)

* **Hook:** a composite parser hook calls the market-data hook (its own circuit and try/except, independent of the
  calendar's "no calendar loaded" guard), then today's calendar hook unchanged. The market-data hook is pure and O(1):
  append to a bounded per-symbol tick ring, update the in-memory forming 1-minute bars and the latest quote.
* **`md-writer`** (supervised; stamp 5 s): every 250 ms drains the rings; one Lua script per symbol per flush (writer
  token and `md:boot` checked) adds ticks, rewrites forming bars, replaces closed buckets (`ZREMRANGEBYSCORE s s` then
  `ZADD`, atomically) with their content hash, and updates the rollback set (§A13), metadata and events; it then
  enqueues indicator jobs for `md-indicators` (§A6). Closed bars, TICK and BROKER, go through their own bounded queue
  (6 h of 1-minute bars per series) that never drops while ticks are shed.
  **Release rule:** every bar write goes over one dedicated writer connection. A flush that wrote closed bars pipelines
  its `EVALSHA` calls and then `WAITAOF 1 0 1500` on that connection (`WAITAOF` covers only that connection's earlier
  writes); those bars leave the queue only when the reply's first element is ≥ 1. On a timeout or error they stay
  queued (metric `md_aof_wait_timeouts_total`) and are re-written on the next flush (the bucket replace is idempotent).
  The wait blocks that flush for at most 1.5 s; the rings absorb it. With Q17 = off, `WAITAOF` is never sent (Redis
  refuses it without `appendonly`) and bars leave the queue on the script's reply (§A2). At the queue's 6 h bound the
  oldest bar is dropped and its bucket becomes `blind` (a BROKER bucket is re-fetched by `md-bars`); by then §B14 has
  already declared the store unrecoverable.
* **`md-bars`** (supervised; stamp 30 s): broker reads through the orchestrator relay at priority `BARS` (PR11):
  first attempt at `T + p50 + settle` (the one rule, §A4; `broker_bar_lag_ms` only starts the back-off). While the
  bar is not final it re-reads with back-off inside the BARS reserve and the per-adapter budget (§A6, PR11), on the one
  schedule in §B5.2, until `decision_cap`, when live re-reading stops: the ARM's POST is `UNRESOLVED` (§B5.2, §B14),
  and the still `blind` bucket moves to the backfill lane (§A4), where it is re-fetched until it resolves or is
  `unfillable`.
  Every read is validated before it is queued for writing (§A4), and one series' failed or undecodable read never
  delays another (§A6 fault containment). The PR3-measured p50 / p99 POST latency is recorded in this document, and
  POST decision latency p95 is an SLO metric with a page threshold; ask series only for symbols with an armed short;
  backfill newest-first per series but each indicator published only after its warm-up range is complete; consumer
  re-fetches (§A13), the backfill lane, the gap-heal pass (§A4) and `md_backfill` use the same BARS budget.
* **Memory shedding order** (each step paged, at 75 / 80 / 85 / 90 % of `maxmemory`): ticks to 30 minutes → TICK 1m
  beyond 2 days → TICK 5m beyond 30 days → stop building undemanded TICK specs. BROKER series, their indicators and the
  rollback sets are never shed.
* **Redis errors** never touch ingestion: rings keep the newest ticks, the closed-bar queue holds bars, series go
  GAPPED until writes succeed.

## A9. Clock

ClockSkew (new, in `src/marketdata`), **per adapter**: p50 / p99 of (`received_ms` − broker time) from fresh ticks only
(broker-timed, tradeable, not snapshots, not within 10 s of a resubscribe, changed values); persisted in
`md:skew:{adapter}`; cold start per adapter p50 = 0, settle 5 s; `settle = clamp(p99 − p50 + 250 ms, 250 ms, 5 s)` plus
that adapter's `TickDateRaw` resolution. Bar finality, PRE timing and `md-bars` use their own adapter's p50 and settle.
`|p50|` beyond resolution + 1 s pages per adapter (5-minute hysteresis).

## A10. Read port

```
MarketDataPort
  quote(adapter, symbol) -> Quote, liveness (LIVE | STALE | STALE_NODE)
  forming_bar(series) -> Bar, coverage_start
  bars(series, at_bucket | n | range, as_of_ms) -> [Bar]            # single-source; quality, revision, final_at
  best_bars(...)                                                    # display only, never the ARM
  indicator(indicator_id, output, at_bucket | n, as_of_ms) -> [Value] # quality, warm, lineage
  provisional(indicator_id, output) -> Value
  demand(spec, consumer) / release(...)                             # leases
  on_bar_closed(cb) / on_bar_revised(cb) / on_indicator_updated(cb)   # wake-ups only (§A13)
  events_since(series, cursor) -> [BarEndEvent] | CURSOR_EXPIRED   # pull cursor (bar_end_ms, write_seq); returns
                                                                    # revisions of older ids too (§A13)
  refetch(series, from_bucket, to_bucket)                           # BROKER re-fetch by md-bars (§A13)
  md_generation() -> (epoch revision, n)                            # reset flag; also on every result (§A13)
  series_status(series) -> SM12 state, gaps
```

Decision reads are bucket-addressed and return `DATA_UNAVAILABLE` unless the series is LIVE, or BACKFILLING with the
needed range complete. All market-data reads finish before any PostgreSQL transaction begins. Out-of-process consumers
(a future strategy service, the dashboard) use `md_reader` and `md:events` and check the unkeyed record digest (§A4
check (5)); they write nothing. A lease path for them is later work: an in-engine registration port through which the
engine writes their leases (never a write ACL for the consumer); the bootstrap already rebuilds the registry from every
live lease in the store (§A7 step 4).

**`as_of_ms` is point-in-time:** for each bucket a read returns the revision with `final_at_ms ≤ as_of_ms` and
`superseded_at_ms > as_of_ms` (the current revision counts as never superseded), reading `md:bars` and `md:bars_rev`
together. An indicator read whose `as_of_ms` is before the current value's `final_at_ms` recomputes the canonical value
(§A6) from the `as_of` bars. `arm replay` reads at each decision's `decided_at_ms`. A needed revision that has aged out
of `md:bars_rev` (7 days) gives `UNVERIFIABLE(superseded_expired)`: `arm replay` reports it, never a mismatch, and files
no `md_backfill` for it (the broker returns only the current bar).

## A11. Market-data prerequisites

| # | Change |
|---|---|
| PR2 | **Done (S01, S03).** `PriceDTO.ts_source: BROKER \| RECEIPT`, `received_ms` (Lightstreamer thread). |
| PR3 | **Probe run 2026-11-17 (live_readonly, EURUSD; two reports, 1 round of 60 s and 900 s and 4 rounds of 900 s; no prices). Results: 1m and 15m bars, bid and ask, in the count and the range form (range not truncated, no off-grid bar); fields `PriceBars[]`, `PartialPriceBar`, `bar.BarDate`, `bar.Open/High/Low/Close`, `bar.Volume`; `BarDate` is the bucket **start** (every vote; the forming bar on the grid); **no side tag** on bars (the side is the request's, which the §A4 side check accepts as request-derived); bar `T` present and unchanged with `T + 1` at +5 s, +30 s and +5 min (p50 = p99 = 5 s, never final 0: `broker_bar_lag_ms` 5000, `broker_bar_settle_s` 10); hard checks failed 0; ticks all broker-timed, receipt minus broker p50 about 0.2 s, p99 about 0.65-0.8 s; delivery filtered (`XTNL_STONEX_PRICE_UNFILTERED` stays 0). Go for StoneX, both sides, D6 reachable as measured. Not measured: roll prints (gate `pr3_roll_prints`), manual stop moves on ORDERS, `LastChangedDateTimeUTC` on amendments and stop children in `activestoplimitorders` (gate `pr3_stop_children`; `XTNL_ARM_STOP_READS` stays 0, `--check-stops` added), Open `LastChangedTime`, `OpenPrice` across the roll, Supabase TLS. `trusts_partial_flag` and `trusts_next_bucket` stay False. S39 enabled StoneX `fetch_price_bars` (`src/adapters/stonex/adapter.py::StoneXAdapter`); the probe now reads through it.** **Read-only probe** (as specified): StoneX price bars (1m, 15m; bid and ask; time convention; partial flag; finality at +5 s, +30 s, +5 min, with the p50 / p99 finality lag; on several days, the 15m bid bars covering 16:45-17:15 New York against tradeable-only TICK bars, recording whether break or indicative prints are included; if they are, a decision on a bucket overlapping 17:00 New York carries the caveat `roll_affected`, Q14); `setRequestedMaxFrequency("unfiltered")` (used on StoneX price subscriptions only when `XTNL_STONEX_PRICE_UNFILTERED=1`, set after the probe, §C2); `TickDateRaw` resolution and rounding; whether a manual stop move appears on ORDERS, and whether a REST order read carries a `LastChangedDateTimeUTC` that moves with every stop amendment (`S_at_T`, §B6.4); Open `LastChangedTime` vs `ExecutedDateTimeUtc`; `OpenPrice` across the roll; TLS verify-full against the Supabase host. Then the optional adapter capability `fetch_price_bars(symbol, side, timeframe_s, *, count=None, from_ms=None, to_ms=None)` (a count or a range form; default "not supported"; Pepperstone not bar-capable in v1). Go / no-go per adapter and side: no usable broker bars (no capability, no ask bars, or bars that fail the probe's checks) makes that adapter or side `UNSUPPORTED` for the ARM (§B7; there is no TICK fallback); a slow but working feed is a go, because POST waits for the final bar up to `decision_cap` (§B6.4), and its p99 lag sets `broker_bar_lag_ms` (the re-read back-off start, §A4). The go / no-go also names the D6 status (§A4) each adapter and side can reach under the measured delivery: `ACTIVE`, `CLOSE_ONLY`, or `UNAVAILABLE` (TICK buckets rarely `close_eligible`), in which case only the hard checks guard that series. The probe also records whether a bar response names its side and bucket start, which the §A4 side and alignment checks rely on. |
| PR5 | **Done (S05, S32; `src/pubsub/orchestrator.py` changed).** `XTNL_TICK_SYMBOLS` (≤ 8 per adapter; checked against `instruments`; unknown or silent symbols page through the market-data layer) as a **separate** per-adapter orchestrator set (`IngestionOrchestrator.set_md_tick_symbols(adapter_id, symbols)`, `AdapterRuntime.md_tick_symbols`): subscribed together with the required symbols (stream start, `src/pubsub/orchestrator.py::IngestionOrchestrator._stream_symbols`, and in its own `ensure_ticks()` call, `flush_md_symbols`), but never added to `rt.tick_symbols` / `pending_symbols`, which feed the session's `_wanted_symbols` (`src/runtime/session.py::EngineSession._wanted_symbols`) and the stale-price watch (`IngestionOrchestrator.check_liveness`). Merged into the existing `tick_symbols` config (`AdapterRuntime.tick_symbols`, read by `IngestionOrchestrator.required_tick_symbols()`), they would change SM10's adapter OPEN (operations §10: OPEN when any subscribed symbol is OPEN), which drives the dead-man `market_open`, liveness alerts and broker-grounds abdication (`src/calendar/market.py::MarketCalendar.market_open`); a tick-only symbol with longer hours would keep an FX adapter OPEN. As built: a tick-only symbol's tick is not published to `market.price.dto` (it reaches only the market-data hook), so it writes no Redis price key and no `last_clean_marks` row; a removed symbol stays subscribed for the adapter's life as `md_retired` (no read-only unsubscribe path); it is not SM10 evidence and the calendar ignores it in its deciding paths (the `ignored` predicate of `MarketCalendar`). Test: a tick-only symbol OPEN while every held symbol is CLOSED leaves `market_open(adapter)` unchanged. |
| PR6 | **Done (S03, S05; `StreamEvent.market` carries the canonical symbol, or the raw market id when no instrument is known; cTrader keeps one batch SUBSCRIBED with per-symbol events from S05).** `StreamEvent.market`; per-market SUBSCRIBED intervals in the orchestrator; constructor-supplied pure hook `on_price_subscription(adapter_id, market, subscribed, ts)` (not from the catch-up hold); per-market `dropped_price_frames` including lost updates. |
| PR10 | `redis-md` service in its own compose profile `md` (non-blocking start step; `stop-xtnl.ps1 -Containers` and `status-xtnl.ps1` pass `--profile md`), named volume `xtnl_local_redis_md` (added to the compose header's "NEVER `down -v`" line, which today names only `xtnl_local_pg` / `xtnl_local_etcd`), ACL file, URLs, HMAC key, `new-md-credentials.ps1` and the process lists (§C2 Secrets), status-script section. |
| PR11 | **Done (S06; `src/adapters/ratelimit.py` changed).** `RestPriority.BARS = 2` between CASH_FLOWS and DLQ (`src/adapters/ratelimit.py::RestPriority`; DLQ and POLL moved down one value, and metric labels come from the name, so dashboards are unchanged): reserve 0.15, max wait 15 s, metric label `bars`. A per-adapter cap on `BARS` reads per minute (`STONE_X_REST_BARS_PER_MIN` / `CTRADER_REST_BARS_PER_MIN` in `src/config.py`, default a quarter of the bucket's rate, clamped to half of it) bounds `md-bars` re-reads while a POST waits for a late bar (§A8) and the PR9 stop reads, so DLQ and POLL keep their share; a read refused by the cap is a `TRANSIENT` port value. `rest_sent_ms()` is the earliest send time since the caller cleared it (the PR9 read age). The outcome counters treat a broker answer (4xx included) as ok and count a throttle answer apart, never as an outage; `failing_at_every_priority` is None (unknown) when no non-BARS priority has a recent outcome. operations §15 (rule text pending, §0b). |
| PR12 | **Done (S12, S31; `src/marketdata/ports.py` and `src/runtime/md_ports.py` new).** Two narrow Protocols in `src/marketdata/ports.py`: `BrokerBarsPort.fetch_price_bars(adapter, symbol, side, timeframe_s, *, count=None, from_ms=None, to_ms=None) -> BarsResult` (count or range form, R11-M15) and `StopHandleReadPort.read_stop_handles(adapter_id, handles) -> StopHandleRead` (per handle: `OK` with the broker request's send time and the committed version label, `NOT_FOUND` or `TRANSIENT`; the ARM uses the level only once PostgreSQL shows that version, PR9). Errors and decode failures are port-level values (`TRANSIENT`, `UNSUPPORTED`), never broker exceptions, so one bad response never raises into `md-bars`. Implemented in `src/runtime/md_ports.py` as wrappers that apply `rest_priority(RestPriority.BARS)` around the orchestrator, called directly (never through the integrity bridge; `reread_stop_orders()` for stop handles: one list read per adapter, PR9), and keep the orchestrator private. `start_optional_services()` passes only these ports, the `MarketDataPort`, the `src/db` stores and the session's `page` callable, wrapped (`PagerWrapper`): `PagerWrapper.page()` for ordinary pages, and `PagerWrapper.log_mirror()` (a `log.critical` line with the ping id, scope and reason, never `EngineSession.page`, which also queues an outbox row) as the log mirror of the FAILED ping, whose durable critical copy is written by the `src/db` failure store (PR4, §B14); the lockout scanner checks that. |

## A12. Option (b) for Q15 (only if chosen)

`md-writer` keeps `md:lastmark:{adapter}:{symbol}` on the same 10 s cadence plus the forced open / close / roll writes
(AOF-backed). Each existing reader gets a bounded 500 ms read through a small port and falls back to its existing
fail-closed path: SM8 step 3 seeds nothing; the RiskSweep treats the mark as UNKNOWN or stale (capped SOFT); the
start-of-day row is written `partial_session`; the calendar seeds no reference quote. On AWS the leader also writes the
mark to the peer's `redis-md` through the peer's TLS proxy as `md_lastmark` (`resetkeys ~md:lastmark:* resetchannels
-@all +hset +ping`), never as `md_writer`, or every takeover is accepted as `partial_session`. The PostgreSQL table is dropped only
after a release in which nothing reads it.

## A13. Reset flag, event ids, acknowledgements and the rollback set

How every consumer survives a `redis-md` reset without trusting the store. The producer side is `md-writer` and
`md-bars`; the ARM's consumer side is in §B6.6.

* **Reset flag (the md generation).** The generation is `(epoch revision, n)`: the term's `engine_epoch` revision, then
  the number of bootstraps in this term (in memory, from 1). It needs no store of its own and only rises, across
  restarts, takeovers and nodes. Every bootstrap (§A7: start of term, any `NO_BOOT` including a changed `run_id`, and
  `md_writer_reset`) raises it and writes it into `md:boot` as its last step. Every `MarketDataPort` result carries the
  generation it was read under. The writer emits one `md_reset(generation, class)` event, in process and on
  `md:events`, for every bootstrap. The ARM turns it into one INFO notice (§B8) only for class `store_lost` or
  `store_foreign`, or an in-term `run_id` change or `NO_BOOT`; the routine bootstrap at a term start sends none (wired
  in S38d: the bootstrap's reset listener reaches the ARM plan, which sends the INFO notice; `arm_status` and
  `md show` carry the class). A
  reset alone is never a page. The §B14 global fault is a store that cannot heal, or 3 resets within one hour, counting
  only in-term resets that are none of: a term start, operator-requested (`md_writer_reset`, `md_rehash`), the
  `arm_clear_failed` probe's trial bootstrap, or the bootstrap `md-writer` re-runs after a global clear (§B14
  Clearing). Neither of the last two sends an MD_RESET notice. An accepted clear resets the scope's counters and, for the ARM
  scope, the one-hour reset window, so a clear never re-trips FAILED on its own bootstraps.
* **Read sessions.** The generation is checked per read session, never per stored bar: a consumer reads `md:boot`
  before and after the reads behind one decision (for the ARM, one BarClock pass for one event). If the two differ, it
  discards those reads and handles the reset as below. A bar written under an earlier generation stays usable; it is
  checked by its content hash and §A4, so a restart never quarantines history.
* **Event id and content hash.** Every bar-end key event (`bar_closed`, `bar_revised`) has the id
  `(series, bar_end_ms)`, the same for the same bar on any node and after any reset; the revision is node-local
  information and never part of an id. The content hash is `HMAC-SHA256(key, series ‖ bar_end_ms ‖ o ‖ h ‖ l ‖ c)` with
  prices in canonical decimal form, truncated to 16 bytes, with a 1-byte key id in front (the operator-set `XTNL_MD_HMAC_KEY_ID`, §C2). It covers the OHLC only, never
  the revision, `final_at_ms`, quality or tick count, so an identical bar re-fetched or re-written has the same hash.
  Being keyed, a hash stored in PostgreSQL cannot be brute-forced back into prices. The key (`XTNL_MD_HMAC_KEY`, 32
  random bytes from the secrets source, one key for both AWS nodes) is never logged. **Keyring:** the current key plus
  the retained previous key (`XTNL_MD_HMAC_KEY_PREV`); a member hashed under any retained key id verifies on read, and a
  hash under a key id that is not retained counts as missing (never as changed). The writer stores the hash in each bar
  member (§A3) and every read re-checks it (§A4). Each member also carries an unkeyed CRC32 digest of its record, so an
  out-of-process reader without the key can still detect a corrupt member (§A10).
* **Key rotation** (in this order, runbook `docs/runbooks/marketdata.md`): install the new key as current and keep the
  old one as `XTNL_MD_HMAC_KEY_PREV`; file an `md_rehash` request (§B13), which verifies each member under its old key
  id and rewrites it under the new id through the ordinary write script, with no `bar_revised` event (the bar did not
  change), and records its progress in `md:meta`; retire the previous key only after `md show` reports the re-hash
  complete **and** one correction window (`decision_cap`) has passed. Test: a rotation causes no quarantine, no FAILED
  and no CORRECTED item, and readers accept both ids until the re-hash completes.
* **Acknowledgement in the consumer's own store.** A consumer acknowledges an event by durably recording the id and hash
  it applied in its own store, in the same transaction as the effect. For the ARM that is `arm_positions`
  (`last_decided_bar` plus the input hash), in the per-event fenced transaction (§B5.3, §B6.6). Nothing is acknowledged
  in `redis-md`: `md_reader` stays read-only, and the writer keeps no per-consumer state and never waits for a
  consumer. A consumer's in-memory set of pulled, unacknowledged ids drops each id once it is acknowledged (recycled);
  its store keeps only the last acknowledged id per subject.
* **Pull cursor and ordered re-apply.** After a reset a consumer cannot acknowledge, so it waits: decision reads return
  `DATA_UNAVAILABLE` until the series is LIVE again (§A7 SM12, §A10). It then calls
  `events_since(series, cursor)` with the cursor `(bar_end_ms, write_seq)` and re-applies the events in bar order. The
  cursor's `write_seq` part lives in memory per md generation; after a generation change it restarts from the
  consumer's durable acknowledgement (ARM: `(last_decided_bar, hash)`), which never stores a `write_seq`. Same id and same hash: a
  no-op. Same id and a different hash, both final BROKER bars: a genuine correction (§B6.6). A missing bar or hash (not
  yet re-fetched, another key id) never counts as changed; the consumer keeps waiting for it. `md:events` and the
  `on_bar_*` callbacks are wake-ups only; a consumer always re-reads through the port, which validates (§A4).
* **Rollback set.** For every series it writes, the writer keeps the last N bar-end events (N = 8, a local setting of at
  least 2; 2 h at 15m): id, hash, revision and record, in memory and in `md:rb:{series}` (written by the same flush
  script as `md:bars`), scored by a per-series `write_seq` that rises on every entry, so a revision of a bar already
  acknowledged gets a new, higher `write_seq`. The two copies restore each other in the bootstrap (§A7 step 3).
  **Invariant:** `md:meta` `write_seq` never decreases within a store; when memory entries are restored into a store
  that lost them, the same script sets `write_seq` to max(stored, highest restored), so the next new entry scores above
  every restored one and is never trimmed in their favour (built, S16).
  `events_since(series, (bar_end_ms, write_seq))` serves from this set every entry with a higher `write_seq`,
  revisions of older ids included, so a same-id revision is delivered without a wake-up; a cursor whose `write_seq` is
  older than the set's oldest entry, or that belongs to another generation, gets `CURSOR_EXPIRED`. The consumer then reads the bars it needs by bucket,
  and for a BROKER bucket missing from the store calls `refetch(series, from, to)`: `md-bars` re-fetches it from StoneX
  in the BARS lane and validates it (§A4) before it is written. A re-fetch still failing at `decision_cap`, or broker
  history that no longer reaches the bucket, is a §B14 series fault (FAILED, immediate ping). TICK buckets cannot be
  re-fetched and stay `blind`. The set holds N events per series whatever the consumers do, so a slow or stopped
  consumer never grows writer memory.
* **Tests:** a `redis-md` restart between bars raises the generation, sends one INFO notice, and the ARM resumes from its
  last acknowledged bar with no duplicate and no CORRECTED item; the engine and `redis-md` restarting together give
  `CURSOR_EXPIRED`, a re-fetch from StoneX with equal hashes and no CORRECTED item; a re-fetched bar with different OHLC
  gives exactly one correction; a re-fetch failing until `decision_cap` gives series FAILED and a ping; a consumer
  stopped for 3 hours leaves writer memory unchanged; a term start sends no MD_RESET notice and counts nothing toward
  the reset budget; a revision of an acknowledged bar is pulled by `write_seq` with no wake-up; no PostgreSQL column, log record or page holds a price, and every
  stored hash is keyed.

---

# Part B: the Automated Risk Manager (Phase 1)

## B1. What it does

For every open position the ARM runs one stop-loss strategy. A "listener" here means the market-data parser hook (§A8) plus ARM tasks that read committed state (PostgreSQL, `redis-md`); like Oversee, it never subscribes to a bus topic (dto-contracts §7). Each 15-minute bar produces **PRE** (anchor 1: a
provisional heads-up at a local offset before the bar end, 30-45 s (valid 10 s after it fires), default 40 s, §B5.2; the notice starts
"PROVISIONAL, final advice after the bar closes"; never actionable, and it never changes a position's state) and
**POST** (anchor 2: on StoneX's final bar-end price; final; the only decision that counts; only a POST bar-end check
can release a position, §B6.3 (7a)). One notice per event, grouping all managed positions by a locally configured
account alias; positions decided later in the window go into follow-up parts (§B8). A notice is created only when it
has an item to show: none for a bar with no managed position, and none when every item is silent (released positions,
a repeated "at target, not yet confirmed" hold). The hourly "ARM alive" line and the daily test notice then show that
the ARM is running (§B8). Render test: every position released gives no bar notice.

## B2. ARM prerequisites

| # | Change |
|---|---|
| PR1 | **Done (S01, S02, S04 producers and labels; S07 Oversee side, present uncommitted and gated by C4, §0).** **StopHandle** model: `effective_stop_detail(e, positions, orders, mark)` (`src/oversee/risk.py::effective_stop_detail`) (`mark`: the RiskSweep's MarkBook mark for the symbol, or `None`) → `None` or `{level, handles: [{kind: CHILD_ORDER \| POSITION_FIELD, id, level, units, order_kind}], caveats}`, with exactly `effective_stop`'s rules (`src/oversee/risk.py::effective_stop`): `stop_loss` first (one POSITION_FIELD handle); else the non-terminal linked reduce-only stop children whose remaining units together cover the position's units, level = the most distant; else `None`. `effective_stop()` returns its `level` (signature unchanged), so Oversee and the ARM never disagree. `OrderDTO.order_kind: STOP \| LIMIT \| None`. A `STOP` child counts at any price, **except** strictly beyond the entry of a still-unfilled (PENDING / WORKING) entry order (`src/oversee/risk.py::_beyond_unfilled_entry`, used by `_counts_as_stop`): nothing is filled, so it cannot be a break-even stop; it counts as no stop and keeps the pre-S07 `missing_stop_loss` SOFT_HALT, instead of an UNKNOWN that would blind `MaxAggregateOpenRiskRule` for the account (C4; test `tests/oversee/test_risk.py::test_unfilled_entry_order_with_stop_child_beyond_entry_is_no_stop`). While OPEN with a fresh mark a position's STOP child should sit on its triggerable side (judged at the mid while a clean quote is fresh, a touch inclusive; an unfilled entry order's children are judged against its entry, not the mark; LAST_MID and no mark are not judged), else it keeps its level and the detail carries the caveat `stop_kind_inconsistent`, which `MissingStopLossRule` turns into UNKNOWN `stop_kind_inconsistent` through the rule's existing durable `unknown_since` / `unknown_grace_ms` (30 s; covers "touched, awaiting fill"), then capped SOFT and paged as every UNKNOWN is; no new timer and no new column; while closed it is judged against the last mark. A `LIMIT` child never counts. A `None` child keeps today's rule (it counts only strictly on the loss side of entry, `src/oversee/risk.py::_counts_as_stop`) and adds the caveat `stop_kind_unknown`, shown in ARM notices. **Producers** (adapter-isolation §2): StoneX `Type` / `OrderTypeId` 2 → STOP, 3 → LIMIT, 1 → `None` (`src/adapters/stonex/codes.py::order_kind_of`, `ORDER_KIND`); a child nested in a position or entry is typed by its key (`StopOrder` / `Stop` → STOP, `LimitOrder` / `Limit` → LIMIT). Files: `stonex/orders.py` (ORDERS stream; an item without `Type` takes the memo's last known type), `stonex/mapping.py` (`map_stop_limit_order` for activeorders, `order/{id}` and nested children; `map_trade_order` → `None`), `stonex/history.py` (stoplimitorderhistory by `Type`; tradehistory → `None`), `pepperstone/mapping.py` `map_order` (LIMIT → LIMIT; STOP, STOP_LIMIT → STOP; STOP_LOSS_TAKE_PROFIT → STOP when `stopPrice` is set, else LIMIT; others `None`), `MockBrokerAdapter` (as scripted). **Sources that disagree:** in `_label` (`src/pubsub/orchestrator.py::IngestionOrchestrator._label`, through `_order_kind`), before the version compare, an incoming `None` takes the entity's stored known `order_kind`, as `broker_seq` does (dto-contracts §2), so a stream / REST resync never flips it; two different known kinds page `order_kind_conflict` and the child counts as `None`. `payload_checksum` skips `OPTIONAL_PAYLOAD_FIELDS` while `None`. Column lists: `ORDER_COLS` and `order_from_row` (`src/db/rows.py`, used by `integrity_store`, `snapshot_reader` and `local_state`), the `orders_state` column spec and upsert (`src/db/entity_sql.py`); display only: `src/dashboard/views.py`, `src/reports/lifecycle.py`. Tests: §C3. Fixes today's issue that a StoneX stop moved to break-even "disappears". Migration 0015. |
| PR4 | **Done (S08 outbox and dead-man fields, S24 / S29 ARM stores and relay).** Outbox: `expires_at_ms`, `superseded_by`; index `(kind, next_attempt_at_ms, outbox_id) WHERE sent_at_ms IS NULL`; CHECK `kind = 'arm'` ⇔ the channel array is non-empty and every element matches `(arm_email\|arm_push)(:[0-9]+)?` (the encoded retry form), written without a subquery as `(kind = 'arm') = (coalesce(cardinality(channels), 0) > 0 AND coalesce(array_to_string(channels, ','), '') ~ '^(arm_email\|arm_push)(:[0-9]+)?(,(arm_email\|arm_push)(:[0-9]+)?)*$')`, so an empty or NULL array never passes for `arm` (added `NOT VALID`, then validated, §C1). In `src/db/notify_store.py` (the Oversee relay's store), `SQL_OUTBOX_DUE`, `SQL_OUTBOX_PENDING` and `SQL_OUTBOX_OLDEST_UNSENT` (the last also feeds the dead-man field `oldest_undelivered_notification_age_ms`) add `kind <> 'arm'`; `SQL_OUTBOX_ESCALATED` needs no change (the CHECK keeps `+escalated` out of ARM rows), and `SQL_OUTBOX_INSERT` (its channel CASE yields `email` / `mobile_push`) is never used for kind `arm`. The ARM relay has its own store in `src/db` (own insert and queries), no fallbacks, no emergency priority, `lease_valid`, `XTNL_ARM_*` settings through the secrets source (a separate SMTP account or SES account; ntfy with a token, or Pushover). **The FAILED ping never depends on an ARM row:** its critical copy is an `OutboxRow(kind="info", severity="critical")` (no episode, so it is never deduplicated; `email` + `mobile_push` by severity through the Oversee relay) inserted with `insert_outbox(tx, ...)` (`src/db/notify_store.py`) in the **same fenced transaction** as the `arm_failures` row and the kind `arm` copy (the engine's own pool when the ARM pool is the fault; §B14 Record), so ARM modes, route policy and ARM channels never delay it, and it is durable once the episode is; the kind `arm` copy of the immediate ping (§B8) is a second channel only. `EngineSession.page` (`src/runtime/session.py::EngineSession.page`) is not durable and always queues its own `kind='info'` outbox row through a background task when the node is leader, so it is **never** called for the FAILED ping: the log mirror is `PagerWrapper.log_mirror()` (PR12), a `log.critical` line only. Test: each ping (entry or re-ping) yields exactly one `kind='info'` critical row. Both copies carry the ping id in their title; the body is bounded (§B14). Housekeeping finishes expired ARM rows. Dead-man receiver gains `OPTIONAL_FIELDS` with `arm_oldest_undelivered_notification_age_ms`, `arm_overdue_positions` and `arm_failed` (§B14: the number of scopes in FAILED and whether one is global, taken from memory so it is still sent while PostgreSQL is down; the receiver pages above 0; built as an object `{count, global_failed}`, and each optional field is left out of the JSON while None, so the beat is byte-identical with the ARM off). Test (§C3): the Oversee relay's pending and due reads never return an `arm` row. 0016. |
| PR7 | **Done (S01, S02).** Optional `InstrumentSpec.price_increment`, `min_stop_distance`, `asset_class` (checksum-neutral while `None`, by PR1's `OPTIONAL_PAYLOAD_FIELDS` rule). They are instrument metadata, not market data: like `point_size` they stay in the `event_log`, outbox and Redis payload (`src/cache/scripts.py::instrument_write` → `dto_json`), so `dbcheck` recomputes the checksum from the row as today; they never appear in a notice, page or log line (§B3). Adapter side: StoneX sets `price_increment = 10^−PriceDecimalPlaces` in its market-information mapping (confirmed by the PR3 probe), cTrader from the symbol's digits; `point_size` is never used as a fallback (unit test). Touchpoints: `INSTRUMENT_COLS`, `instrument_from_row`, `SQL_INSTRUMENT_UPSERT`; dto-contracts §3. 0015. |
| PR8 | **Done (S01 columns, S04 orchestrator labels and results).** `event_log.origin` (`stream` / `snapshot` / `history` / `dlq` / `stop_read`), set by the orchestrator from the path that produced the version, and `broker_time_source` (`BROKER` when the raw broker time was real, else `WATERMARK` for a watermark-labelled REST row), both carried like `source` as non-payload DTO fields (`src/dto/versioning.py::NON_PAYLOAD_FIELDS`; the S01 encoders leave a `None` label out, `OMITTED_WHEN_NONE`), so checksums do not change. `event_log.broker_time_ms` (nullable) holds the version's broker time only when `broker_time_source = BROKER` (real broker time only, never a watermark); the `event_log` insert (`src/db/repository.py::SQL_EVENT_LOG`) writes `origin` and `broker_time_ms`, and the 0017 index `(entity_type, entity_key, broker_time_ms)` serves "latest at or before T". `reread_entity()` (`src/pubsub/orchestrator.py::IngestionOrchestrator.reread_entity`) serves the DLQ check-before-retry (the integrity bridge and the session's `_DlqPriorityBridge`) and the account refreshes in `src/runtime/session.py` (`EngineSession._refresh_all_accounts` and the per-account refresh); it takes `origin` with default `dlq` (the account refreshes pass `snapshot`) and returns a result with its status (`APPLIED`, `NOOP`, `DROPPED_STALE`, `NOT_FOUND`, `TRANSIENT`), the labelled version and the request's send time; the DLQ bridge keeps its existing contract. PR9 list reads use `reread_stop_orders()` with origin `stop_read`. Readers: entry time and `S0` (§B7), `S_at_T` (§B6.4). 0015. |
| PR9 | **Built behind `XTNL_ARM_STOP_READS` (off; S04 `IngestionOrchestrator.reread_stop_orders`, S27 `src/riskmgr/stopreads.py`, S31 wiring). As built, the StoneX list read covers only `activestoplimitorders`; attached children must be confirmed there before the flag is turned on for StoneX (PR3 did not measure it; `probe_bars --check-stops`, gate `pr3_stop_children`); a pinned handle the list does not name is `NOT_FOUND`.** Only if PR3 shows manual stop moves are not streamed (then `XTNL_ARM_STOP_READS=1`, §C2; off by default): read-only `read_stop_handles(adapter_id, handles)` (PR12) makes **one list read per adapter per anchor** for all its armed handles, never one call per handle: the orchestrator's `reread_stop_orders(adapter_id, origin="stop_read")` (StoneX `active_stop_limit_orders` per trading account; cTrader the open positions' stop fields, POSITION_FIELD handles) through the adapter's read-only capability `fetch_active_stop_orders()`, at priority `BARS`, applied row by row and never closing an entity absent from the list; each handle comes back with its level and `LastChangedDateTimeUTC`. Stop reads count in the admission check and the demand ceiling (§B4, §A6), and a per-adapter armed-handle ceiling makes a further position `UNSUPPORTED(stop_read_budget)` (§B7). The schedule below lives in `src/riskmgr/stopreads.py`: reads coalesced per adapter and anchor, REOPEN reads staggered per adapter, `stop_stale` counted per position (metric), and a position armed after its PRE read was queued gets no PRE that bar. The orchestrator stays the only publisher: a changed level is versioned and committed like any REST read (origin `stop_read`, PR8), so Oversee, the ARM and `S_at_T` (§B6.4) all see one level. A read's age counts from when its request was sent to the broker (after any `BARS` wait; the token wait before the send does not count), never from when it was queued. **PRE:** one read queued 15 s before PRE fires (local `T + p50 − pre_offset − 15 s`, about `T + p50 − 55 s` at the default 40 s offset, §B5.2); `BARS` waits at most 15 s, so the request leaves by the fire time, and PRE waits for it until its valid-until (§B5.2). PRE accepts a read up to 30 s old, which a read sent at or after `fire − 15 s` still is at valid-until (at most `fire + 10 s`, §B5.2). **POST:** one read queued at `T + p50`, in parallel with the first `md-bars` attempt, which gives `S_at_T` (§B6.4); when the POST is ready (the final bar has arrived) and the newest read is more than 15 s old, one more read (retried every 10 s), and the POST waits for it within `decision_cap` (POST has no tight deadline). If PR3 finds that reads carry no such change time, a read is also queued at `T + p50 − 15 s` and `S_at_T` is the newest read sent by `T + p50` (caveat `stop_basis_approx`). POST, LATE, REOPEN and CORRECTED use a 15 s bound (age bounds per event: §B6.3 (2)), with an on-demand read before every LATE, REOPEN or CORRECTED item. Tests: REST bucket idle and saturated, offsets 30 s and 45 s: PRE is never `stop_stale` when its read returns (its window is 10 s at every offset, S38c); 8 positions × 2 handles on one adapter cost one list read per anchor; a broker bar final at `T + 3 min` gives a POST with `S_at_T` from the `T + p50` read and post-guards on a fresh read. |
| PR13 | **Done (S10 field, S32 publishing, S34 accept check; tokens are published only with `XTNL_MD_ENABLED=1`, so with it off every fresh record has an empty registry and `arm config accept` is refused `registry`).** Optional `HealthRecord.registry` (`src/runtime/health.py`): the sorted `KIND:VERSION` tokens of every strategy, guard and indicator the node can load; `to_json` writes it, `from_json` defaults it to empty so an older peer still parses; the fakes in `tests/runtime/fakes.py` gain it. `arm config accept` reads every node's record under `HEALTH_PREFIX` (`src/ops/etcd.py` `get_prefix`) and refuses while a fresh record lacks a referenced version; a record without the field counts as lacking every version (fail closed). |

## B3. Constraints

| Constraint | How |
|---|---|
| Phase 1 sends nothing | No sender code. A pure scanner `src/riskmgr/lockout.py` (used by `verify_execution.py` as a new check and by the session before registering ARM / market-data tasks; refusal pages, ingestion continues) covers one scope, `LOCKOUT_SCOPE` = `src/riskmgr`, `src/marketdata`, `src/armconfig` and any later stop-move package (whose own design decides whether the ban is lifted there). **Imports** are allow-listed per package; the list is a constant in `lockout.py`, copied into `.claude/rules/riskmgr.md`, and widening it is a reviewed change. **The scanner itself** (`src/riskmgr/lockout.py`, named by the constant `SCANNER_SELF`) is exempt by name from the package allow-list, the reflection rules and the `_`-attribute rule, because it parses source files and reads the `__file__` of loaded modules through `sys.modules`; it has its own allow-list (`__future__`, `ast`, `dataclasses`, `enum`, `pathlib`, `re`, `sys` for `sys.modules` only, `typing`), the name ban still applies to it, and no other module in the scope imports it. `src/riskmgr` and `src/marketdata`: `__future__`, `abc`, `asyncio`, `bisect`, `collections`, `contextlib`, `dataclasses`, `datetime`, `decimal`, `enum`, `functools`, `hashlib`, `hmac`, `itertools`, `json`, `logging`, `math`, `re`, `struct`, `time`, `typing`, `zoneinfo`; `src.dto`, `src.oversee.risk` (pure functions), `src.calendar.sessions` / `schedule` / `machine`, the `src.db` store Protocols and the scoped packages (`src/marketdata` never imports `src.riskmgr` or `src.armconfig`); `redis` only in `src/marketdata/store`; `zlib` only in `src/marketdata`, and from it only `crc32` (the unkeyed record digest, §A3, §A13); `concurrent.futures` only in `src/riskmgr`, and from it only `ThreadPoolExecutor` (one single-worker executor per pinned version, §B4 Job runner; `ProcessPoolExecutor` and the module's other names are refused). `src/armconfig`: the same plus `ssl` and `asyncpg`, and the strategy / guard / indicator registries (`parse_params` only). Always refused, with a named reason: `os`, `sys`, `builtins`, `importlib`, `__import__`, `gc`, `inspect`, `socket`, `http`, `urllib`, `smtplib`, `subprocess`, `ctypes`, `httpx`, `aiohttp`, `requests`, `src.adapters`, `src.runtime` (the orchestrator included; task loops get a small stamp Protocol), `src.pubsub`, `src.cache`, `src.notifications`, `src.calendar.readiness`; `ssl` outside `src/armconfig`; asyncio's `open_connection`, `create_connection` and `create_subprocess_*`; attribute access to a `_`-prefixed name on anything but `self` / `cls`; `getattr` / `setattr` with a non-literal name. **Reflection rules:** `getattr`, `hasattr`, `delattr`, `vars`, `dir` and `globals` are refused with a non-literal name or a literal name starting with `_`; `logging.handlers` and `logging.config` are refused; `dataclasses.asdict`, `dataclasses.fields` and `vars` are refused on a parameter typed as a port Protocol (ports such as the pager are `__slots__` wrapper objects, `PagerWrapper`, so nothing behind them is reachable); from `asyncio` only `sleep`, `gather`, `wait_for`, `Event`, `Lock`, `Queue`, `create_task`, `timeout`, `CancelledError` and `to_thread` may be used, plus `get_running_loop` and `wrap_future` in `src/riskmgr` only (a job runs through `loop.run_in_executor(<its version's executor>, …)`; `to_thread` shares the loop's default executor, so it is never used for a strategy or guard job). The scanner is a review gate, not the guarantee: **the hard guarantee that Phase 1 sends nothing** is that the only REST client the adapters construct is `ReadOnlyTransport` (`src/adapters/transport.py`, whose deny-list refuses every mutating path), that no adapter has a mutation method (`src/adapters/base.py::FORBIDDEN_METHOD_NAMES` / `assert_read_only`), that the cTrader session sends only the requests on `src/adapters/pepperstone/protocol.py::OUTBOUND_ALLOW_LIST` under the `SCOPE_VIEW` check, that the StoneX Lightstreamer streams are subscribe-only, and that the scoped packages cannot import `src.adapters` at all; none of these is ever narrowed (the S03 / S04 bar and stop-list reads are GETs through `ReadOnlyTransport`, `rest_get_only`, or read-only cTrader requests). **Sinks:** the Phase 1 set is fixed as `ARM_SINKS` = (decision sink, notice sink), a constant in `src/riskmgr/sinks.py` (new); the runtime wiring (`src/runtime/optional_plan.py`, new) builds the chain with `default_sinks()` and binds its notice slot to `ArmNotices.sink()` once the ARM graph exists; a Supabase document cannot name a sink (validation rejects the key), and the scanner asserts the wired set. The Supabase client lives in `src/armconfig`; the ARM relay in `src/notifications`; PostgreSQL stores in `src/db` as ports. `src/riskmgr`, `src/marketdata`, `src/armconfig`, `src/runtime`, `src/notifications` join the PostToolUse watched directories (pending the S11 `.claude/` proposal, §0b). A static check: no SQL outside `src/ops` writes `arm_controls` or `arm_accepted`. |
| Name ban (one rule for the scanner and the hook) | Each `def` / `class` name in `LOCKOUT_SCOPE` and in the PR12 wrapper module (`src/runtime/md_ports.py`) is normalised (leading underscores stripped, CamelCase split into snake_case, lower-cased) and refused when `re.search` finds `(^\|_)(place\|modify\|cancel\|submit\|close\|amend\|update\|move\|set\|replace\|adjust\|trail\|send\|execute\|edit\|open\|create\|new)_(order\|stop\|stop_loss\|sl\|sltp\|position\|trade)s?($\|_)`. `pre_tool_guard.py` applies a copy of the scope, the normaliser and the pattern to files in that scope only, and a test asserts the copy equals `lockout.py` (the hook copy is the pending S11 proposal; until it is approved the parity test skips with "hook update pending" and the pytest scan of the real tree, `tests/riskmgr/test_lockout.py`, is the gate, §0b). As built, the normaliser also collapses runs of `_`, and the scanner also refuses star imports, `eval` / `exec` / `compile`, `__builtins__`, `locals()` and compiled files in the scope (S11). Its existing narrow `FORBIDDEN_PATTERNS` (`place_order`, `close_position` and the rest) stay repo-wide and unchanged, so nothing new applies outside the scope: `MockBrokerAdapter.set_order` / `set_position` and the StoneX REST `open_positions` stay legal. Internal names follow a published convention (`arm_row_update`, `mark_arm_closed`, `held_positions`). Tests, for the scanner and the hook alike: `_place_order`, `class ModifyStop` and `def trail_stops` in `src/riskmgr` are refused; an edit to `src/adapters/mock.py` passes. |
| No market data in PostgreSQL | §B8. The ARM and the market-data layer add no price data to PostgreSQL. The price data already there is the two existing tables of Q15 plus the mark-derived account values round 12 added to the inventory (`account_snapshots.equity` / `unrealised_pnl`, `sod_marks.sod_equity`; §0b Inventory), all kept unchanged until the user chooses (§0). A test inspects every ARM column, log record, page body and ARM outbox body (unsent, expired and finished, and the INSERT payload), and a schema scan holds every column added by any migration from 0015 on (0015-0020 today) to an allow-list. Hashes of market data kept in PostgreSQL (`arm_positions.last_decided_hash`, the input hashes and `inputs_digest` in `arm_decisions`) are HMAC-SHA256 under a local key, `XTNL_MD_HMAC_KEY` (§A13; secrets source, the same on every node; never in PostgreSQL, `redis-md` or a log; it joins PR10's secret lists), so they cannot be brute-forced back into prices. Each stored hash carries its key id, and hashes under different key ids are never compared, so a key rotation can never look like a correction. Bar quarantine state and its reason text live only in `redis-md`; PostgreSQL (including `events_quarantine` and `operator_requests` payloads) and pages carry series ids, buckets and check names only. The existing plain SHA-256 checksums of order, position and instrument rows stay as they are: they hash order state, which PostgreSQL holds anyway. |
| Leader-only; fenced writes | §B5.3. The ARM pool is built per term with `application_name` `xtnl:<epoch revision>:<node>:arm` (truncated to 63 characters, so a successor's terminate reaches it), and closed with the term. It runs as the role `xtnl_arm` (`SET ROLE` in the pool's connection init; `NOLOGIN`, granted to the engine role). **Grants (the complete list, §C1):** SELECT, INSERT and UPDATE on the mutable ARM state tables (`arm_positions`, `arm_notices`, `arm_status`, `arm_config_cache`) and SELECT and INSERT on the immutable ones (`arm_decisions`, `arm_decision_handles`, `arm_correction_audit`, `arm_config_audit`; below), because the `row_version` CAS (`UPDATE … WHERE`) and `INSERT … ON CONFLICT DO NOTHING RETURNING` need SELECT; SELECT on `arm_failures`, INSERT only on its columns other than `cleared_at_ms` and `clear_request_id` (an episode cannot be inserted already cleared) and, since 0020 (S38f), UPDATE only on `last_ping_at_ms`, `ping_count`, `last_probe_at_ms` and `last_probe_result` (the re-ping and the probe note; the recorded evidence, `counters` and `position_ids`, is written by the INSERT only), with the BEFORE UPDATE trigger `arm_failures_guard` refusing any change of an identity column (`failure_id`, scope, `edge_code`, `reason`, `probe_kind`, `ping_id`, `first_fault_at_ms`, `entered_at_ms`, `epoch`), any change of a cleared episode and a clear by any role but the table owner's (so `xtnl_arm` can neither clear nor re-key an episode; the database enforces D7, not only the code); SELECT, INSERT and UPDATE on `arm_budget_counters` (0020, no DELETE); INSERT only on `notification_outbox` (the kind `arm` rows and the FAILED ping's critical `info` copy; no RETURNING and no `ON CONFLICT DO UPDATE`); SELECT only on `arm_accepted`, `arm_controls`, `engine_epoch` (the FENCE subquery), `event_log`, `orders_state` and `positions_state` (entry time, S0 and the `S_at_T` candidates, `src/db/arm_versions.py`); USAGE on the sequences behind those inserts. **On the engine (owner) pool instead, never as `xtnl_arm`:** `arm-relay` (its due and pending queries and its finish / supersede / body-replace / reschedule UPDATEs of `notification_outbox`, like the Oversee relay; its statements name `kind = 'arm'`); the `arm-tracker` snapshot (`snapshot_transaction`, read only) and the pre-guard (1) reads; `operator_requests` reads and completion (handlers answer through an engine-pool port); the `xtnl_ensure_month_partition` call (a partition needs the table owner); and the `arm_clear_failed` clear statement that sets `cleared_at_ms` (the probe's own test writes run on the ARM pool, §B14 Clearing). **Immutable rows (0018, round 12):** the decision and audit tables are not updatable by the role: `arm_decisions` has SELECT, INSERT and a column-level UPDATE of `notice_id` only (the one update a decision row takes, "no decision without a notice", §B8); `arm_decision_handles`, `arm_correction_audit` and `arm_config_audit` have SELECT and INSERT only; the table-wide UPDATE stays on `arm_positions`, `arm_notices`, `arm_status` and `arm_config_cache`. A test compares the role's privileges with this list (built: `src/db/arm_schema.py`, 0017, 0018 and 0020; the privilege self-check in `optional-bootstrap` also reads the guard trigger, `src/db/arm_schema.py::guard_problems`, and pages a difference but does not block the ARM). The static check that no SQL outside `src/ops` writes `arm_accepted` or `arm_controls` stays. |
| Supervised; PostgreSQL waits are not deaths | Transient errors retry within the event window; deterministic errors become `WRITE_ERROR`. |
| No Oversee timers | `src/riskmgr` is outside Oversee; level-triggered scheduling. |
| Entity state from PostgreSQL | `snapshot_transaction` (a READ ONLY REPEATABLE READ snapshot that writes nothing, like the RiskSweep's named exception 1; the ARM's snapshots, its unfenced `arm_status` telemetry and the operator writes of `arm_accepted` / `arm_controls` are listed as operations §2 / §12 exceptions in the pending proposal `S38e-rules-arm-exceptions.md`, §0b). |
| No money amounts, no pip sizes | R values are dimensionless multiples, quantised to 0.01 R; ratios to 0.001; the R distance is never stored, logged or shown. `price_increment`, `min_stop_distance`, the indicator quantum and the step-(12) gap never appear in a notice, page or log record (a `PRICE_THROUGH_LEVEL` item gives the reason only). `indicator_id` is an opaque hash of the canonical spec (sha256, first 16 hex), so no id shows the pinned quantum; the readable spec is kept only in `md:registry:ind`. Market-data logs and pages carry series ids, buckets, field names and revisions, never values. The §C3 no-market-data test also plants an instrument with a known increment and asserts that neither it nor the quantum appears in any log record, page or notice body. |

## B4. Components

| Component | Responsibility |
|---|---|
| **arm-tracker** | Snapshot every 2 s of accounts with OPEN positions; arms, updates, ends positions (CAS, every state change through SM11's table, §B12); entry time and S0 (§B7); resolves and pins the assignment (Phase 1 single edge, §B11); under PR9 enforces the per-adapter armed-handle ceiling (`UNSUPPORTED(stop_read_budget)`, §B7); **owns every ARM demand lease**, renewed every 15 s from the cached accepted document (never from the Supabase poll) and re-asserted at once after `NO_BOOT` and after a takeover: the specs of armed positions (BROKER 15m bid, plus ask for a short under `exit_side`, with their indicators; TICK 1m and 15m bid and ask, the tick-built bars that broker bars are cross-checked against) plus the **pre-warm set** (explicit symbols in accepted assignments; for wildcard scopes, held symbols ∪ `XTNL_TICK_SYMBOLS` on bar-capable adapters; never "all instruments"; capped at 8 per adapter). A symbol outside the set warms on first sight through a synchronous backfill of each indicator's declared `backfill_bars` (§A6). As built (S26, S36b): `src/riskmgr/tracker.py` (new) with the engine-pool snapshot `src/runtime/arm_ports.py::PgTrackerSnapshot` (new; one READ ONLY REPEATABLE READ snapshot; freshness from the stream fences, the integrity incident and the catch-up verdict, no "consumers running" input) and the bar grid `CalendarGrid` over the SM10 schedule (a symbol without a loaded schedule has no bar: nothing is decided or spanned for it until it loads); armed-handle ceiling default 16 per adapter; no lease is held for a SUSPENDED position. |
| **arm-config** (`src/armconfig`) | Polls Supabase every 120 s; validates; installs accepted documents only. `arm config accept` runs an admission check (expanded series count, bytes from the §A5 formula, BARS reads per bar end against the per-adapter budget, including the PR9 stop reads: armed handles' list reads per anchor and the REOPEN cadence) and refuses with the numbers when a limit is exceeded. |
| **BarClock** (`arm`; `src/riskmgr/barclock.py`, joined to the S21 pipeline by `src/riskmgr/evaluation.py::PipelineEvaluator` (new), S36b) | A scheduler over the `(adapter, bar_spec)` set pinned by armed positions (v1: 15m). Every 250 ms: lease check; suspend / clock-step check; controls (cached 1 s; unreadable = hold the event and retry, never a pause, §B13); due events; market-data reads; evaluation; one fenced transaction per event. Never awaits broker I/O. |
| **EvaluationPipeline** | Engine-owned envelope around each strategy (§B6). |
| **Job runner** (`src/riskmgr/jobs.py`) | Each position's pinned guards, `propose` and `at_target` run as one job in a single-thread executor per pinned strategy `(KIND, IMPL_VERSION)` (its own `concurrent.futures.ThreadPoolExecutor(max_workers=1)`, entered through `loop.run_in_executor` and awaited under the cap; never `asyncio.to_thread`, whose shared default executor one hung job could starve; §B3 allow-list), under the pinned Decimal context, with a 250 ms cap; a timeout or raise is a `STRATEGY_ERROR` result. Each version has its own circuit: 3 consecutive faults across its positions fail the STRATEGY_VERSION scope (§B14). A fault raised inside a guard is attributed to that guard's pinned `(KIND, VERSION)` (scope id `guard:<KIND>:<VERSION>`), so a bad guard version never fails an unrelated strategy version; a hung thread blocks only its own version's executor. Conformance test: a planted hanging strategy leaves other versions advising and the `arm` loop stamping. As built (S22, S38c): jobs of one version run one at a time (one lock per version) and a timeout counts against the strategy scope. **Lane retirement:** a job still running past its cap leaves its thread behind; the lane is shut down without waiting and the next job of that version (the `strategy_conformance` clear probe included) gets a fresh executor, so the version's FAILED scope can be cleared without a restart. At most 3 abandoned threads (the version fault limit, so one hanging version fails only its own scope) may be alive at once; a further hang keeps its lane (`executor_hung`, no new thread) and raises the ARM-global fault `restart_required` once (only a restart frees the threads; runbook: restart, FAILED survives it, then clear). **CPU-spin limit (accepted):** a strategy stuck in a pure-Python loop keeps its share of the GIL (a switch every 5 ms), so it slows but never stops the event loop that runs ingestion and Oversee; the conformance test bounds the lag with one spinning job under the C3 100 ms bound. DRAINING ends through the hard exit while any job thread (live or retired lane) is hung (the clean-stop rule is the pending `S22-hung-job-shutdown.md` proposal). |
| **Sinks**, **arm-relay** | §B8; each sink in its own savepoint, in a fixed order. |

## B5. Runtime rules

### B5.1 Tasks

| Task | Stamp age |
|---|---|
| `md-writer` | 5 s |
| `md-bars` | 30 s |
| `md-indicators` | 10 s |
| `arm` | 5 s |
| `arm-tracker` | 10 s |
| `arm-config` | 120 s (Supabase connect 5 s, statement 5 s) |
| `arm-relay` | 60 s |
| `arm-status` (built, S32) | 5 s |
| `optional-bootstrap` | 60 s (resident, §A2) |

The leader writes `arm_status` every 5 s (unfenced telemetry, like `engine_runs`): heartbeat, epoch, circuits, channel
status, paused state, market-data summary, FAILED scopes and error-budget counters (§B14). As built the writer is its
own task `arm-status` (`src/runtime/optional_plan.py`), added with the market-data tasks so this node's §A7 record
(`md_sentinel`, `md_last_write_ms`) is saved with the ARM off; it writes the heartbeat row on the ARM pool while the
ARM runs and on the term's engine pool while only the market-data services run, so `md ...` requests are answerable
with the ARM off (S36b); `running` is True while the BarClock runs. The `arm` loop ticks every second (FAILED
manager, STRATEGY_VERSION signals, stuck-frontier views, `notify-test`), runs the BarClock pass every 250 ms once
the open FAILED episodes are read and while no ARM-global FAILED holds, and the notices' timer pass every 5 s, skipped
while either pause holds (so ALIVE, TEST, extras and `FAILED_CLEARED` wait for the resume).

### B5.2 Validity windows

| Event | Fires | Valid until | Otherwise |
|---|---|---|---|
| PRE (anchor 1) | local `T + p50 − pre_offset` (`pre_offset` 40 s, allowed 30-45 s, `src/marketdata/timing.py::pre_offset_problem`) | `fire + 10 s` (S38c): a 10 s window at every allowed offset, ending at local `T + p50 − 20 s` at 30 s and `T + p50 − 35 s` at 45 s (`T + p50 − 30 s` at the default) | not sent (counted by reason) |
| POST (anchor 2) | when StoneX's final bar `T` is stored (§B6.4) and its indicators are updated for `T` (§A6) | `decision_cap` | `UNRESOLVED(broker_bar_not_final)` |

* **Two anchors.** PRE is a provisional manual heads-up. It is never actionable and never pre-stages an order; in
  Phase 2 only POST decisions count (§B10). `pre_offset` is a local engine setting, never from Supabase (§B6.7); like
  `md-bars`, PRE uses the adapter's skew (§A9). Timing values live in one per-bar-spec table (`pre_offset` bounds,
  `post_grace`, the re-read schedule, the UNSUPPORTED lag limit, rollback N, the cross-check TICK spec); v1 has only
  the 15m row, and validation refuses a strategy whose `bar_spec` has no row. PRE's provisional close is the live,
  tradeable quote on the trigger side (Q5), **frozen at fire time**: PRE computes nothing from a quote taken after
  fire (as built S38d: the quote as it was at fire, `MarketDataPort.quote_at`), and its valid-until is `fire + 10 s`
  (`src/marketdata/timing.py::pre_valid_until_ms`, the table above); PRE never runs FirstBarGuard (§B6.5); its
  stop read is in PR9. No PRE for a fill in the last `pre_offset` of a bar, or when the PRE time is in a closed period
  (the PRE notice says "market closes at bar end" when `T` is a close). While the previous bar's POST is still pending,
  the PRE says so.
* **POST waits for StoneX's final bar** however late, up to `decision_cap`; there is no streamed-close fallback (Q18).
  `md-bars` re-reads a missing or non-final bar every 5 s until `T + post_grace` (`post_grace` 60 s), every 30 s until
  `T + 5 min`, then every 60 s until the cap (about 30 reads per series per bar end, at `BARS` priority inside the
  per-adapter budget). A position still waiting is shown as INFO "awaiting broker bar" (never a FAULT, never counted
  for `arm_position_faulting`) and joins a follow-up part once decided (parts and their timing: §B8). A POST decided after `T + post_grace` is
  labelled `DELAYED` with its delay ("decided N min after bar end"; `decided_at_ms` in `arm_decisions`); like every
  POST it uses the stop at bar end and re-runs the post-guards on the live quote (§B6.4). At `T + post_grace` a series
  still waiting pages once per series episode, and a metric gives the POST wait age per series.
* **`decision_cap`** = the end of the next bar on the local clock: `local T + timeframe + p50 + settle` (the adapter's
  skew, §A9; about `T + 15 min` for 15m): one engine rule, never a setting, and the one clock of every cap-based timer.
  It is the POST outer cap, the time of the end-of-window notice part (§B8), the replay horizon and the correction
  window (§B6.6); none of them uses another number. At the cap a POST still undecided becomes
  `UNRESOLVED(broker_bar_not_final)` and advances the frontier, in the same BarClock pass and before the next bar's
  POST is evaluated, so decisions stay in bar order. **Market close (daily break, Friday close):** a pre-close bar still
  not final at its cap is recorded `UNRESOLVED` as above, but kept as a pending re-check: it is re-read at the open and,
  if final then, decided as a `REOPEN` item (§B6.3), on the REOPEN schedule (round-11 confirmation C3, §0; the
  alternative is that the cap at a close means the end of the first post-open bar). It counts toward the fail-fast error budget only when the
  engine ran and `md-bars` kept reading for the whole window; bars whose cap passed while the engine was down or
  asleep are handled by replay (§B6.6) and never count.

### B5.3 One transaction per event

Every market-data read and evaluation finishes before `BEGIN`. One fenced transaction per (event, bar end) for every
position decided now, each in a savepoint, then the notice. A not-yet-decidable position stays pending
(`arm_positions.pending_event`) and goes into the next notice part once decided (parts: §B8); what it becomes if its
window ends first is set in §B5.2. A POST-time `DATA_UNAVAILABLE` does **not** advance `last_decided_bar` (replay
re-decides it later). As built (S27) a non-final bar-end result (a POST-time `DATA_UNAVAILABLE`, a job fault, a wait
for a stop read) writes **no** decision row, because a row under the decision key `(adapter_id, position_id,
resource, owner, bar_spec, event, bar_end_ms, revision)` would block the bar's final POST; the position is marked
`pending_event` / `pending_bar_end_ms` once (position CAS only), and the reason becomes the bar's expiry reason (a
FAULT span at the cap, e.g. `stop_stale`). PRE rows are written once per bar. A notice failure inside the transaction
fails it, decisions included, so "no decision without a notice" holds and the decisions are retried next pass.

| Statement | Mode |
|---|---|
| `INSERT arm_decisions ... ON CONFLICT DO NOTHING RETURNING` | `must=False`; conflict = already decided |
| `UPDATE arm_positions ... WHERE row_version = $n AND state NOT IN ('CLOSED','DISARMED')` (sets the cursor, its hash and `decision_seq`, §B6.6) | `must=False`; miss = the decision's savepoint rolled back, then re-read |
| `INSERT arm_positions ... ON CONFLICT DO NOTHING` | `must=False` |
| `INSERT arm_notices(route, bar_spec, event, bar_end_ms, part, item_key) ... ON CONFLICT DO NOTHING RETURNING` (key, parts and CHECKs: §B8) | `must=False`; outbox only if returned |
| `INSERT notification_outbox` (kind `arm`) | `must=True` |
| `arm_config_audit`, `arm_config_cache` | `must=False` |

## B6. Strategy pattern, envelope and the first strategy

### B6.1 Interface (sketch)

```
SlStrategy (Protocol)
  KIND, IMPL_VERSION           frozen; registry keyed on (KIND, IMPL_VERSION)
  RESOURCE = STOP_LEVEL        the only RESOURCE the v1 registry accepts (Resources below)
  Params; parse_params(raw)    the strategy's own keys plus guards: [{kind, version, params}] (§B6.7); unknown keys
                               rejected; envelope parameters are separate
  STATE_SCHEMA; migrate_state(from_impl_version, old_state) -> State | Refuse(reason)
                               # on the target version; run only by arm_repin, inside its fenced row_version CAS;
                               # Refuse keeps the old pin (request reported moot); a decision is always re-run
                               # (correction, arm replay) under the version recorded on it
  REASONS                      KIND-scoped reason codes, each mapped to an engine category
  DECISION_METRICS             typed RMultiple / Ratio only
  triggers(params) -> [BarEvent(bar_spec, PRE | POST)]      # v1 accepts only BarEvent
  REQUIRED_GUARDS              guard KINDs every row of this strategy must list (kind a: first_bar)
  guards(params) -> [GuardSpec]                             # returns the parsed params.guards in declared order (contract below)
  loosen_policy(params) -> REARM | TELL_ONLY                # the engine never reads a strategy-specific key
  REOPEN_POLICY = REUSE_TARGET | REPROPOSE                  # kind (a): REUSE_TARGET; a re-pin of E or S0 (fill progress,
                                                            # entry correction, local rearm) drops the recorded target in
                                                            # the same CAS, so REOPEN recomputes it from the new E and S0
  RELEASE_POLICY = ON_CONFIRMED_TARGET | NEVER              # kind (a): ON_CONFIRMED_TARGET; §B6.3 (7a) runs only under it (S38c)
  required_indicators(params) -> [IndicatorSpec]            # BROKER series only
  required_history(params) -> {SeriesSpec: bars}            # raw BROKER bars in ctx; leased, admitted, retention-checked like indicators
  arming_requirements(params) -> {s0: AT_FILL | ANY, entry_time: EXACT | ANY}   # unmet at arming = UNSUPPORTED (§B7)
  init_state(arm_ctx) -> State
  propose(ctx, state, event) -> Evaluation                  # pure: trigger_met, Proposal[RESOURCE] | reason
  commit_state(state, evaluation, final_outcome) -> State   # final POST / LATE / REOPEN only
  at_target(ctx, state) -> bool                             # all handles at the target; bar-end checks only, never PRE;
                                                            # released only at a bar end with trigger_met, §B6.3 (7a), §B12
```

* **BarContext** (filled from `MarketDataPort` and PR1's `effective_stop_detail()`): side; `E`; that call's stop handles
  with read time; `S` = its level, equal to Oversee's `effective_stop` by construction (`stop_loss` first, else the most
  distant of the stop children that together cover the position's units); `S_at_T` = the same call over the order and
  position versions with broker time ≤ `T` that §B6.4 lets count (`event_log`, PR8; used by every POST, §B6.4);
  `pre_basis` when set (§B6.3); `S0` (that level at fill
  or first sight) and source; `R = σ·(E − S0)`; bar index;
  the final BROKER bar at bar-end events (quality, revision; at PRE no bar, only the trigger-side quote frozen at fire
  time, §B5.2); the BROKER bars declared by `required_history`; indicator values (quality,
  warm, lineage); exit quote and liveness; bucket evaluability; `account_image_fresh` with reasons; entry-time source.
  No units.
* **Pinned per position** (in `arm_positions`, at arming; `arm_repin` replaces both pins together): `(KIND,
  IMPL_VERSION)`, strategy parameters (including `rearm_on_loosen`, the Q10 answer), the guard list (every entry's
  `(kind, version, params)`, in order, plus the list's hash), indicator specs and `required_history`. Live evaluation,
  replay and corrections read only the pin, never the current document. **Always current:** the envelope (pre- and post-guards, rounding, and
  EnvelopeParams such as `min_gap_r`), versioned with the engine; each decision records `envelope_version` and a hash
  of the EnvelopeParams it used, and `arm replay` reports `ENVELOPE_CHANGED` when they differ. **EnvelopeParams come only from local engine settings,
  never from Supabase**; validation rejects envelope keys in a Supabase document. A property test proves the envelope
  only turns an `ADJUST` into a HOLD or FAULT, and a test proves an accepted document never changes a pinned or envelope
  value of an armed position, guard params included, in live evaluation, replay or correction.
* **Resources:** `Proposal` and the envelope are typed per RESOURCE. v1 has one: `Proposal[STOP_LEVEL]` = {StopHandle →
  target}, and `Envelope[STOP_LEVEL]` = the pre- and post-guards, rounding and EnvelopeParams of §B6.3. The v1 registry
  refuses a strategy with any other RESOURCE. A new resource (e.g. take-profit) needs its own Proposal type, envelope,
  envelope property test and SM11 rows; an amend touching two resources at once is a Phase 2 item (§B10).
* **Guard contract:**

  ```
  Guard (Protocol)
    KIND, VERSION                       registry keyed on them; never removed while pinned
    Params; parse_params(raw)           one entry of the strategy row's params.guards: [{kind, version, params}]; unknown keys rejected
    EVENTS                              events it runs on, fixed per VERSION (FirstBarGuard: {POST}; never run at PRE)
    required_indicators(params)         merged into the arm-tracker leases, the admission check and the retention-fit check
    required_history(params)            the same, for raw BROKER bars
    applies(ctx, state) -> bool         e.g. bar index 0 for FirstBarGuard
    evaluate(ctx) -> PASS | HOLD(reason) | PENDING(until) | UNKNOWN(reason)
    REASONS                             guard-scoped, mapped to engine categories
    unknown_policy(params)              HOLD_BAR | SKIP from the guard's own params (FirstBarGuard: `unknown`, the Q3 answer)
    conditional_label(params)           caveat on an applicable event outside EVENTS (FirstBarGuard: bar 0's PRE, §B6.5)
  ```

  The engine runs a position's **pinned** guards in order between the pre-guards and `propose`, each only on an event
  in its `EVENTS`, which always include POST. On another event (PRE) where `applies()` holds, the guard is not
  evaluated: `propose` still runs and the result carries the guard's `conditional_label` (PRE is never actionable,
  §B10). `PENDING(until)` maps to `DATA_UNAVAILABLE(guard_pending)` up to `until` (never later than `decision_cap`),
  then to `unknown_policy(params)`. Validation rejects a strategy row that lacks a kind in `REQUIRED_GUARDS` (kind (a)
  without `first_bar`), lists a kind twice, names an unregistered `(kind, version)` or has guard params out of bounds.
  A guard reason is stored with its guard KIND and takes the category the guard maps it to (§B6.2).
* **Version lifecycle:** a strategy `(KIND, IMPL_VERSION)`, guard `(KIND, VERSION)` or indicator `(KIND, VERSION)` is never
  removed from the registry while any `arm_positions` row or `arm_accepted` document references it; deprecated versions
  stay importable and are only refused for new pins. `verify_execution.py` and `python -m src.ops arm registry check
  --against-db` fail a deploy that lacks a pinned version; each node publishes its registry (the sorted
  `KIND:VERSION` tokens of its strategies, guards and indicators) in its health record (PR13), and `arm config accept`
  refuses while any fresh node's list lacks a referenced version (a membership test, not a digest comparison).
* **As built (S19-S21, `src/riskmgr/strategy.py`, `guard.py`, `registry.py`, new):** every strategy and guard method
  that needs the pinned params takes them as its first argument (`propose(params, ctx, state, event)`,
  `evaluate(params, ctx)`); the registry, not the strategy, splits off and parses `guards`; `required_indicators` /
  `required_history` return templates on `(bar_spec, side)` (always BROKER), resolved per position, and
  `arm_positions` stores the templates, never a resolved `IndicatorSpec` (its canonical form holds the increment);
  Protocol members added: `SlStrategy.trigger_side(params, side)`, `SlStrategy.latch(params)`,
  `Guard.known_before_bar_end` and `Guard.on_timeout`; KIND-scoped reasons map only to INFO or HOLD (ACTIONABLE is the
  engine's `ADJUST`); the strategy applies the `pre_basis` farther-from-entry rule itself and holds the unrounded
  target; a raising strategy or guard is `PipelineFault(scope_id)` without the exception text; `ENVELOPE_VERSION` is 1.
  The pipeline (`src/riskmgr/pipeline.py`) is S21, built ahead of its gate and not accepted (§0b).

### B6.2 Engine outcomes

| Outcome | Category | Step |
|---|---|---|
| `ADJUST` | ACTIONABLE | 13 |
| `COMPLETE` (released; bar-end decisions only) | INFO | 7a |
| `PRE_NOT_CONFIRMED` (reason) | INFO | 7a |
| `AT_TARGET_UNCONFIRMED` | HOLD | 7a |
| `LOOSENED_AFTER_COMPLETE` (once per episode) | INFO | 4 |
| `BROKER_AHEAD` | INFO | 8 |
| strategy reason codes (kind a: `BELOW_TRIGGER`, `NOT_IN_FAVOUR`) | INFO | 7 |
| guard reason codes (FirstBarGuard: `FIRST_BAR_VOLATILE`, `FIRST_BAR_UNKNOWN`, `PRE_ANCHOR`) | HOLD | 6 |
| `MARKET_CLOSED` | HOLD | 10 |
| `PRICE_THROUGH_LEVEL` | HOLD | 12 |
| `NO_STOP` | FAULT | 3 |
| `DATA_UNAVAILABLE` (reason) | FAULT | 1, 2, 5, 11 |
| `STRATEGY_ERROR`, `WRITE_ERROR` | FAULT | — |
| `NO_BROKER_BAR` (S38c: an OPEN bucket `md-bars` resolved `no_trade`; final, recorded only, no trigger, no state change, never counted toward `fail_unresolved_n`) | INFO | 5 |
| `UNRESOLVED_RANGE` (reason; §B6.6; `UNRESOLVED(reason)` elsewhere in this document is a one-bar range, e.g. `broker_bar_not_final` at `decision_cap`) | INFO for `armed_late`, `downtime`, `paused`, `failed`; else FAULT | — |

`borderline` = `|ratio / multiplier − 1| ≤ 0.03` (first bar) or `|favour / risk − 1| ≤ 0.03` (trigger); shown, never
hidden. Released positions (`STRATEGY_COMPLETE`) are evaluated silently
(§B6.3 (4)); `COMPLETE` itself is shown once, on the bar that releases the position.

### B6.3 Pipeline (first match wins)

**Pre-guards:** (1) account image fresh (no `orders` / `account` fence, which covers the catch-up hold; no integrity
incident; consumers running; latest catch-up finished; an incomplete one is a caveat); (2) stop level not stale
(PR9 only: the newest read is older than its PR9 bound at decision time, 30 s for PRE and 15 s otherwise →
`DATA_UNAVAILABLE(stop_stale)`); (3) an effective stop exists (PR1: `effective_stop_detail()` is not `None`; stop children that do not cover the position's units give `NO_STOP`, the same view as Oversee's `missing_stop_loss`); (3b) price increment
known, else `DATA_UNAVAILABLE(increment_unknown)`; (4) a released position (`STRATEGY_COMPLETE`, §B12)
gets no new advice: a handle on the loss side of the target (below it for a long, above it for a short) with
`rearm_on_loosen` off → `LOOSENED_AFTER_COMPLETE` (once per
episode, Q10); with it on, the position returns to `TRACKING` and goes on to (5); otherwise `COMPLETE`, recorded and
never shown. Its FAULTs from (1)-(3b) are recorded, never shown and never page (Oversee's `MissingStopLossRule` still
watches the stop). There is no other completion pre-guard: a managed position whose handles already sit at or beyond
the target goes on through (5)-(7), because only a bar end releases it (7a); (5) a bucket `md-bars` resolved
`no_trade` (a complete broker answer with no bar for a calendar-OPEN bucket: an unscheduled holiday, a thin bucket) →
`NO_BROKER_BAR` (INFO, final: the cursor moves on; no trigger, no `commit_state`, the SM11 state and `pre_basis` kept,
so an ARMED position stays ARMED and its first bar-end decision is the next traded bucket; never counted toward
`fail_unresolved_n`; S38c); otherwise POST without StoneX's final bar → pending (INFO "awaiting broker bar") until
`decision_cap`, then `UNRESOLVED(broker_bar_not_final)` (§B5.2).

**Strategy (kind a, `break_even_on_risk_close`):** (6) bar 0 at POST → FirstBarGuard (§B6.5); (7) `favour = σ·(close − E)`,
`risk = σ·(E − S)` (at POST `close` is StoneX's final bar and `S` is `S_at_T`; at PRE the trigger-side quote frozen at
fire time and the current stop; at both, while a basis is held, the farther of `S_pre` and that `S`: **one stop-basis
function**, `src/riskmgr/stopbasis.py::trigger_basis`, S38c, so PRE previews what the bar end will test); `trigger_met = favour > risk`; if met, propose `target = E + σ·be_offset_r·max(R, 0)` for every handle not
yet at it (an `R ≤ 0` gives no buffer; when `be_offset_r > 0` and S0's source is not `AT_FILL`, §B7, the item carries the caveat
`be_offset_first_seen`, "buffer based on first-seen stop"); else `NOT_IN_FAVOUR` / `BELOW_TRIGGER`; with `latch` on, a recorded trigger re-proposes.

**Post-guards:** (8) a target that would loosen a handle as it is now (current
handles, not `S_at_T`; under PR9 a read at most 15 s old) → `BROKER_AHEAD` for that handle; (9) round toward profit to
`price_increment`; (10) market not open now (SM10 `OPEN` or
`OPEN_BY_EVIDENCE`, as the RiskSweep reads it; a "closed period" in Part B means the same) →
`MARKET_CLOSED`; (11) exit quote not live (subscription, heartbeat,
tradeable, `STALE_NODE`) → `DATA_UNAVAILABLE`; (12) `σ·(quote − target) < max(min_gap_r·R, min_stop_distance, one
increment)` → `PRICE_THROUGH_LEVEL`; (13) → `ADJUST`, naming each handle and printing the stop's read time with "only if
your stop is still worse than this".

`commit_state` (final POST / LATE / REOPEN) records `trigger_met` whatever the post-guards said; after a FAULT the
state is unchanged.

**Release check (7a), the one rule for ending management (D3).** It is defined on the strategy's `Evaluation`
(`trigger_met`, meaning "the bar-end condition for the target holds", and the proposal's targets) and on `at_target`,
never on one kind's reason codes, and it applies only to a strategy that declares `RELEASE_POLICY =
ON_CONFIRMED_TARGET` (kind (a)); a strategy declaring `NEVER` (a trailing or continuing one whose target keeps
moving) stays managed at target with the trigger met (`AT_TARGET_UNCONFIRMED`, TRACKING; S38c, §B6.1), so every
strategy kind of a policy gets the same rule (the conformance suite checks the meaning
of `trigger_met`, §C2). It runs right after (7), before the post-guards, and only on a final bar-end decision: POST, or a LATE, REOPEN or CORRECTED decision that becomes advice (§B6.6). PRE never
releases and never changes `arm_positions.state`, and a stop move seen between bars never releases a position on its
own. `at_target` = every handle at or beyond the unrounded target (`σ·(level − target) ≥ 0`); for on-grid levels
this equals at or beyond the target rounded toward profit, and a handle exactly at an off-grid `E` with offset 0
counts. It is judged on the current handles (as the post-guards use them). A tightening stop move made after this
bar's PRE proposed one is left out of `trigger_met` in (7) (the `AT_PRE` stop basis, §B6.4), so acting
on the heads-up never makes the bar-end test easier; a trigger latched on an earlier bar does not count here. Then:
* `trigger_met` on this bar's close and `at_target` → `COMPLETE`: the position is released (`STRATEGY_COMPLETE`, no
  longer managed). It needs no open market or live quote, because nothing is moved.
* `at_target` without `trigger_met` → HOLD `AT_TARGET_UNCONFIRMED`, still managed. It is shown only on the first bar
  of each at-target run (the position's previous final decision was not at target); later ones are marked
  `suppressed`.
* If this bar's PRE proposed a move and the POST ends in a guard HOLD (6), a strategy reason with `trigger_met` false
  (kind (a): `BELOW_TRIGGER`, `NOT_IN_FAVOUR`) or `AT_TARGET_UNCONFIRMED`, the outcome is `PRE_NOT_CONFIRMED` (INFO,
  carrying that reason, shown in both modes) and the position stays managed; it counts as the shown at-target hold.
  FAULTs and post-guard HOLDs are not `PRE_NOT_CONFIRMED`, because they do not say the bar end disagreed.
* **`pre_basis`** (round-11 confirmation C1, §0): a `PRE_NOT_CONFIRMED` stores on `arm_positions` the handle levels the
  PRE used, before the move it prompted (`pre_basis`, order state, with `pre_basis_bar`). It is keyed by the position
  and the handle's role (`POSITION_FIELD`, or `CHILD_ORDER` ranked by distance from entry), never by order id, and the
  trigger uses its basis stop `S_pre` (the effective stop of those levels, §B2 PR1). Later bar-end trigger tests use,
  of `S_pre` and the current `S`, **the one farther from entry** (stop basis `AT_PRE` while that is `S_pre`, §B6.4),
  until a bar end confirms the trigger against it; then `pre_basis` is cleared and the current stop (Q8) applies again.
  It is also cleared once the current `S` is at or farther from entry than `S_pre` (the stop was loosened back past
  it, so the current, larger risk already applies). So a move made on an unconfirmed heads-up never makes a later bar's
  test easier, and a stop loosened after it is never tested at a smaller risk than it now carries; a StoneX cancel /
  replace that gives the stop child a new order id keeps the entry. `at_target` and the post-guards still use the
  current handles.
* **A move on final advice is never discounted** (round 14, plan S38g): the AT_PRE rule exists so that a move made
  on a provisional heads-up never confirms itself. A stop move that answers an outstanding **final** `ADJUST` (the
  newest POST, LATE, REOPEN or CORRECTED `ADJUST` of an earlier bar, not withdrawn or superseded: the position is
  ADVICE_OUTSTANDING) is not such a move, even when this bar's PRE proposed the same target (Q11 re-tests each bar)
  or the POST was DELAYED past this bar's PRE. When `S_at_T` moved toward entry after this bar's PRE and is at or
  short of that final target, this bar's PRE levels are left out: the trigger tests `S_at_T` (Q8's current stop),
  the outcome is never `PRE_NOT_CONFIRMED`, no `pre_basis` is stored, and the decision carries the caveat
  `moved_on_final_advice`. A move past the final target is not covered by the advice and stays under AT_PRE.
  A REOPEN or CORRECTED re-run of a bar cannot use the row's SM11 state (it already includes that bar's own or a
  later decision), so it takes the target only from the newest final bar-end decision of an **earlier** bar when
  that decision is an `ADJUST` (`SQL_PRIOR_FINAL_DECISION`, `DecisionMemory.prior_final_adjust`); a later
  withdrawal or a latched HOLD reads as no advice, so the AT_PRE discount applies (the conservative side; round 14
  review, `tests/riskmgr/test_round14.py`).
* At PRE a position with `at_target` gets no item, **unless a basis is held**: then PRE tests the trigger against
  `S_pre` like the next bar end and shows `AT_TARGET_UNCONFIRMED` with the caveat `pre_basis_held` ("the bar end tests
  against your stop as it was before your heads-up move"), so the heads-up never goes silent on the bar that may
  release the position (S38c). Notice texts: §B8.
* **As built (S21, S36b):** "this bar's PRE proposed a move" means the PRE ended in `ADJUST`; its handle levels are
  stored on the PRE row's `arm_decision_handles` (`handle_role`, `level_at_pre`) and a bar-end prepare re-reads them,
  and "previous final at target", from the committed decisions (`src/db/arm_ports.py::ArmDecisionReader.decision_memory` (new)),
  so a leader change or restart between PRE and POST still decides `PRE_NOT_CONFIRMED` and stores `pre_basis`;
  unreadable is `NotReady(decisions_unreadable)`. A second `PRE_NOT_CONFIRMED` keeps whichever basis has the `S_pre`
  farther from entry; "current S" for clearing `pre_basis` is the bar-end `S_at_T`. A PRE with no frozen quote or one
  received after fire is a `DATA_UNAVAILABLE` row (`pre_quote_unavailable`, `pre_quote_after_fire`); a pending guard is
  `DATA_UNAVAILABLE(first_bar_pending)`; on a guard HOLD `propose` is not run. Pre-guard (2) measures age from the
  handles' send time and applies only with `XTNL_ARM_STOP_READS`. PRE and every bar-end event use the one stop-basis
  function (`trigger_basis`, S38c): the farther of `S_pre` (while set) and the current `S`.

Tests, long and short: PRE `ADJUST`, the user moves the stop to break-even before the bar end, the close confirms →
`COMPLETE`; the same with a close below the trigger → `PRE_NOT_CONFIRMED`, still managed, and the next bar decides
again against `pre_basis` (a next close just beyond entry is still managed until a bar end confirms against the
pre-move stop); a stop at break-even with no PRE proposal and an unconfirmed close → one `AT_TARGET_UNCONFIRMED`, silent on the
next bar; bar 0 with a PRE proposal and a volatile first bar → `PRE_NOT_CONFIRMED(FIRST_BAR_VOLATILE)`; after a
`PRE_NOT_CONFIRMED` the user loosens the stop beyond its `pre_basis` level → the next bar is tested at the current,
looser stop and `pre_basis` is cleared; after a `PRE_NOT_CONFIRMED` a StoneX cancel / replace gives the stop child a
new order id → `pre_basis` still applies to it.

**REOPEN** (independent of `latch`): a `trigger_met` recorded on a bar whose POST fell in a closed period (daily break,
Friday close) is a pending re-check stored in `arm_positions`. From the first OPEN instant + settle it is retried every
5 s (a PR9 stop read at most once per 10 s, so one inside the 15 s bound
exists; post-guards on the live quote). `PRICE_THROUGH_LEVEL` and a spread that is
not clean (the existing clean-spread rule) are **retryable**, because spreads are widest at the open; only `ADJUST`,
`COMPLETE`, a closed position, or the first post-open bar's POST decision (at the latest its
`decision_cap`) ends it. It is sent **as its own notice
event** (`REOPEN`, key: §B8) as soon as it is decided; on expiry the last HOLD is sent with its reason. So a trigger on the last bar before a break or the weekend is never
silently lost (tests: Sunday open with non-tradeable first quotes; wake from a weekend sleep then the open).

Kinds (b) and (c) are specified in §20 Q1 and built only if chosen; (c) needs `S0` = `AT_FILL`.

### B6.4 Bar-end close and stop at POST

**Close:** StoneX's final 15m bar `T` on the trigger side (Q5), final per §A4. POST waits for it up to `decision_cap`
(§B5.2); there is no TICK or streamed-close fallback, so every decision input is a BROKER bar. An adapter without
broker bars, or a position whose needed side has none (bid for the first-bar test; ask for a short under `exit_side`),
is `UNSUPPORTED` (§B7), and `arm config accept` refuses an assignment scoped to an adapter without broker bars.

**Stop at bar end (`S_at_T`):** the trigger at every POST, on time or `DELAYED`, applies the definition of `S` (§B6.1)
to each handle's latest version with broker time at or before `T`. **Only these versions count:** `APPLIED` versions
in `event_log` (never `DROPPED_STALE`) with origin `stream`, `stop_read`, `snapshot` or `dlq` and
`broker_time_source = BROKER`, i.e. a real broker time in `event_log.broker_time_ms` (PR8; watermark-labelled REST
rows never count, as for entry time, §B7). `history` versions never count: StoneX history keeps only the last level,
stamped at placement, so it can show a level the stop did not have at `T`. A handle whose newest candidate does not
count (a `history` version, or a watermark-labelled REST row with no broker time, which is newer by `seq` than the
newest counting version at or before `T` and older by `seq` than the first counting version after `T`: a later APPLIED
untimed row was read after that post-`T` version, so it shows state from after `T` and takes no part) takes its newest earlier counting version (stop basis `PRIOR_VERSION`); with none,
`DATA_UNAVAILABLE(stop_at_bar_end)`. **Direction** (round 14, plan S38g): the newer non-counting version may show
the level the handle held at `T` (a loosened amend seen only through an untimed REST read), so the handle's level is
the farther from entry of the prior counting level and that newer level: a loosened stop is never tested at a
smaller risk. A newer non-counting version that holds the handle but has no level cannot be resolved against the
prior one: `DATA_UNAVAILABLE(stop_at_bar_end)`. Golden: a loosened amend seen only through an untimed REST row, the
close between the old and the new trigger, gives `BELOW_TRIGGER`, not `ADJUST`. A handle whose `LastChangedDateTimeUTC` in the `T + p50` read is at or before `T`
has that read's level; one changed after `T` keeps its newest earlier version (caveat `stop_changed_after_bar_end`); a
handle with no version at or before `T` did not exist at `T`; no handle at all → `DATA_UNAVAILABLE(stop_at_bar_end)`.
When this bar's PRE proposed a move, a handle moved after that PRE counts at the level the PRE used (stored on the PRE
decision, matched by handle role, not order id), and after a `PRE_NOT_CONFIRMED` the same holds on later bars through
`pre_basis` until a bar end confirms (§B6.3); in both cases the trigger takes, of that level and the current one, the
level farther from entry, so a move made on the heads-up never confirms itself and a loosened stop is never tested at
a smaller risk; a move that answers an outstanding final `ADJUST` is not a heads-up move and counts at `S_at_T`
(§B6.3, caveat `moved_on_final_advice`). Golden test: a stop amended and then cancelled while the
engine was off (the history version is excluded). The decision stores its `stop_basis` (`STREAM`, `READ`, `PRIOR_VERSION`,
`AT_PRE`) and the handle levels it used (order state, §B8). Post-guards (8)-(12) always use the handles as they are
now (under PR9 a read at most 15 s old) and the live quote.

**As built (S24, S36b):** `src/db/arm_versions.py` (new) reads `S_at_T` in one READ ONLY REPEATABLE READ snapshot with
three statements: the newest counting version per handle at or before `T` (`history` and `DROPPED_STALE` excluded),
**every** version newer than that counting one that does not count (timed non-counting candidates at or before `T`
and untimed watermark versions older by `seq` than every counting version after `T` (live review 2026-11-25),
with their order-state fields since S38g; round 14 review: so a non-counting loosen
followed by a non-counting re-tighten is tested at the loosened level; at most 32 per handle, and one more makes the
handle unresolved, `DATA_UNAVAILABLE(stop_at_bar_end)`), and a level-less marker read of the newest `stop_read` after `T`, so
`PRIOR_VERSION` and `stop_changed_after_bar_end` can be labelled and a non-counting level can only make the test
harder, never easier. **S38d (round 13):** the handles read are the `stop_loss` field, the current stop children the
evaluator names, and every reduce-only child `orders_state` links to the position (`linked_position_id`) that may have
been live at `T` (not terminal now, or last changed after `T`), all in the same snapshot. A child cancelled or
replaced after `T` therefore counts with its version at or before `T`; a child created after `T` has no candidate and
takes no part; a child that ended before `T` is not read. A StoneX cancel/replace while a delayed POST waits, over a
weekend before a REOPEN, or before a LATE replay decides on the pre-replace level, never a
`DATA_UNAVAILABLE(stop_at_bar_end)` span.
**Accepted (round 14 review):** a newer non-counting version that **ends** the handle and carries no level takes no
part in the direction rule. Its time is not when the stop ended (StoneX history stamps the last version at placement;
an untimed REST row is read after the fact), so it cannot show the stop was already gone at `T`; treating it as
`StopUnavailable` would turn every ordinary cancel seen later through history into a `DATA_UNAVAILABLE` span. An
ended version that still carries a level takes part like any other.

### B6.5 First-bar test (`FirstBarGuard`)

Bar 0 (Q7): the bucket containing the fill, or the first full bucket after it (the fill bucket is then `PRE_ANCHOR`,
which is known at PRE too: under `next_full_bucket` the fill bucket's PRE says "no advice this bar (entry bucket)"). A
fill within its timestamp uncertainty of a boundary is cross-checked against the history execution time; if still
ambiguous the entry time is not exact. **At POST only** (`EVENTS = {POST}`), on StoneX's final
bar 0 (§B6.4); PRE never runs it. The verdict is taken in **exact Decimal**: `FIRST_BAR_VOLATILE` iff
`measure(BROKER bar 0, bid) > multiplier × ATR_exact` (`≥` for `ge`), where `ATR_exact` is the unquantised canonical
ATR(`atr_length`) recomputed from its `W(length)` bars (§A6; Q2: `high_low` default; ATR including bar 0); the ratio
is quantised only for the notice. An `ATR_exact` below one `price_increment` (zero included) is
`FIRST_BAR_UNKNOWN(atr_degenerate)` under Q3 and pages once per series (unit test). Golden vectors sit at
`multiplier × ATR ± 1 quantum` for a 5-digit and a 3-digit pair, plus a fill at 17:06 New York (bar 0 just after the
daily break, with and without break prints in the broker bars, Q14). Volatile → hold this bar; the next bar is
normal. **Bar 0 not yet final:** the POST waits for it (§B5.2). **ATR not yet warm**, or an
entry-time upgrade still possible → `DATA_UNAVAILABLE(first_bar_pending)` **only until bar 0's `decision_cap`**; then
bar 0 is decided per Q3 as `FIRST_BAR_UNKNOWN(timeout)` and `last_decided_bar` advances, so bar 1 onward is evaluated
normally; a verdict that becomes computable after the cap is kept for audit only, because the correction window is
`decision_cap` (§B5.2). Entry time permanently ambiguous → `FIRST_BAR_UNKNOWN` (Q3) at once. **Bar 0's PRE** evaluates
only the trigger and is labelled conditional: "first-bar check pending at bar end; if this bar closes calm, the
trigger is met" (or "not met"). In `actionable` mode a bar-0 PRE that proposes a move is shown only with this label,
never as a plain provisional `ADJUST` (render test).

**As built (S20, `src/riskmgr/guards/first_bar.py`):** bar 0 is derived from the entry time and the anchor (the
engine's bar index only when there is no entry time); `applies` also holds on the entry bucket under
`next_full_bucket`, so its POST records `PRE_ANCHOR`; the guard reads bar 0 and the ATR window from its own bid history,
gates on the `md-indicators` value for exactly the ATR bucket (warm, not FAULTED) and recomputes `ATR_exact` from the
bars; an `ESTIMATED` entry time is PENDING until the cap, so one that never upgrades ends `FIRST_BAR_UNKNOWN(timeout)`;
`roll_affected` is set on any bar 0 overlapping the 17:00 New York roll (not only after PR3 shows break prints); the
conditional PRE label travels as the caveat `first_bar_conditional`.

### B6.6 Replay and corrections

* **Acknowledgement and replay (the ARM side of §A13):** `arm_positions.last_decided_bar` is the ARM's durable cursor
  and its acknowledgement of bar-end events: the last bar end finally decided for that position, with
  `last_decided_hash`, the keyed hash (§A13) of that bar's trigger-series bar (NULL after a span). Both are set by the
  `row_version` CAS (§B5.3) in the same savepoint as the decision row, never on receipt and never in `redis-md`. While a
  series is not LIVE (or BACKFILLING with the range complete), for example after a market-data reset, its decision reads
  give `DATA_UNAVAILABLE` (§A10), so nothing on it is decided or acknowledged. The ARM then pulls the series' bar-end
  events after the oldest cursor that uses it, oldest first (§A13), and in one BarClock pass decides, in order and before
  any newer bar, every bar after each cursor whose inputs exist (whole pipeline, guards included, `commit_state` per
  bar); the rest stay pending. An event is only a wake-up: every input is re-read and validated through
  `MarketDataPort`. A pulled id at or before the cursor with the same hash is a no-op; with a different hash it goes to
  the correction check below. The replay horizon is `decision_cap` (§B5.2): a bar still undecided at its cap is never
  decided, except the pre-close bar re-read at the open as a `REOPEN` item (§B5.2 Market close). A bar's cap is the next bar's end, so bar n + 1 is never committed before bar n is decided or expired, and
  that never delays it; the ARM never needs an id older than the rollback set (§A13). A bar decided after its POST
  notice has gone is a `LATE` item: trigger on the stop as at bar end (`S_at_T`), post-guards on the live quote and stop
  (§B5.2). Metric: cursor age per position.
* **Expired bars and late arming:** at arming (and re-arming) the cursor is the later of the start of the fill bucket
  (fill bucket − 1) and the newest bar end whose `decision_cap` had passed (arming time − replay horizon). So a position
  armed long after its fill (first deployment with a weeks-old trade, a fill during a PC shutdown, a long suspension)
  decides at most the current bar. While no ARM runs, the ARM is paused or the position's scope is FAILED (§B14), its
  cursor stays put. Bars whose cap has passed undecided become **one** terminal `UNRESOLVED_RANGE(from_bar, to_bar,
  reason)` row per contiguous span (one per episode), written in one transaction (`bar_end_ms` = `to_bar`'s end; no
  `commit_state`; hash NULL); it advances the cursor and is one line in the next notice. Reasons: `armed_late`,
  `downtime`, `paused`, `failed` (INFO, never paged, never counted toward D7), or the reason the bar was last pending
  with while the ARM ran (FAULT; pages once per position episode and counts toward the D7 error budget, §B14). A span
  containing bar 0 records `FIRST_BAR_UNKNOWN(stale)`, and FirstBarGuard never applies after it. A cursor still behind
  a bar whose cap has passed while the ARM runs (the span write itself failing) is a stuck frontier: a D7 fault (§B14 table). Every row's `bar_end_ms` is at most about two timeframes before its write (`REOPEN` rows aside), so it
  lands in a partition that exists (§C1). Tests: arming a position filled 3 weeks earlier writes one `armed_late` row
  and decides only the current bar; a 9-hour PC shutdown gives one notice line per position and no page; a bar n that
  never turns final gives a span at its cap and bar n + 1 is decided on time.
* **Corrections:** there is no separate reconcile pass after `NO_BOOT` or a takeover; a correction comes only from this
  hash compare. Each decision stores, per input, its id `(series_id, bar_end_ms)` and keyed hash (§A13); an indicator
  value is stored as `(indicator_id, output, bar_end_ms)` with an HMAC over the hashes of its window's bars. Revisions
  are information only, so the compare works across stores, restarts and terms. A decision's correction window ends at
  its bar's `decision_cap`; after that it is final (a later revision is visible only to `arm replay`). PRE rows are never
  corrected (their POST replaces them). On a new md generation (§A13), a `bar_revised` or `indicator_updated` event, or a
  pulled id it has already acknowledged, the ARM re-reads the current hash of every input of each non-superseded POST or
  `LATE` decision still in its window:
  * same id, same hash: no-op;
  * an input missing, not final, quarantined or unreadable now, or absent when the decision was made: **never a
    change**; the compare is retried on the next trigger, and one still open when the window ends gets an audit-only
    `arm_correction_audit` row `UNVERIFIABLE(reason)` (no notice, no page; a metric);
  * a different hash on a final, validated input: a **genuine correction**. The pipeline re-runs on the recorded order
    state (`E`, handles, `S_at_T`, pinned versions) and writes a new row (`revision + 1`, `supersedes_decision_id`); a
    recomputed FAULT never counts as an outcome change. If the outcome, `trigger_met` or the first-bar verdict changed,
    the row is a `CORRECTED` item; otherwise it is stored with the `suppressed` sentinel (§B8), which acknowledges the
    new hash.

  A `CORRECTED` item for a bar **older** than the position's latest decided bar is INFO only: it states the corrected
  verdict, points to the current bar's decision and is never actionable. For the latest decided bar an `ADJUST` is
  re-proposed with post-guards (8)-(12) re-run on the live quote, as for `LATE`, and `commit_state` re-applied; the
  corrected row also decides release (§B12): it can release the position, or withdraw a release made on that bar
  (INFO), never one made on a later bar.

  **Order:** every row has `decided_at_ms` and `decision_seq`, monotonic per (adapter, position, resource, owner), taken from
  `arm_positions.decision_seq` under the same CAS. `LATE`, `REOPEN` and revised rows carry the original bar's
  `bar_end_ms`. `advice` (the row states the position's current verdict) is true for POST, `LATE` and `REOPEN` rows and
  for a revision of the latest decided bar, and false for PRE rows, spans, `suppressed` revisions and INFO corrections
  of an older bar. "Newest decision" for Phase 2 = max(`decision_seq`) among `advice` rows. Tests: `redis-md` restarts
  at 10:16 while BROKER 15m backfills → no `CORRECTED` item for the 10:15 `ADJUST`, and the backfilled 10:15 bar with
  the same OHLC is a no-op; a re-fetched 10:15 bar with a different close before its cap → `CORRECTED`, re-proposed;
  the engine and `redis-md` restart together → the ARM resumes from its PostgreSQL cursor and re-sends nothing.

  **As built (S22, S27, S36b):** replay is a step planner over the bar slots the BarClock lists (every bar end of an
  open bucket after the cursor; a closed-period bucket is not a bar, so weekends and daily breaks are skipped, not
  spanned); the REOPEN re-check window runs from the first open instant + settle until the first post-open bar's POST
  decision (at the latest its cap). **Correction re-checks are wired (S38d):** the BarClock keeps every final
  bar-end decision it committed until its bar's `decision_cap` and, every 5 s, reads each input's current hash
  (`events_since` with the restart-form cursor; by bucket under the keyring when the cursor expired) and calls
  `corrections.check_decision`; a genuine correction re-runs the evaluation as `CORRECTED` (a release on that bar is
  re-run from `TRACKING`) and `plan_revision` writes `revision + 1` with `supersedes_decision_id` in one fenced
  transaction (older bar INFO; latest bar re-proposed, released, or its release withdrawn; an unchanged verdict is
  stored suppressed). A release itself waits for its bar's verification re-read. The `OpenChecks` compares are in
  memory per term; the correction window is rebuilt at a term's first correction pass from the committed final
  decisions still inside their `decision_cap` window (S39b, gate `corrections_restart`; the read covers bar ends
  from `now − (timeframe + p50 + settle)` on, newest first, so a truncated read drops only the oldest), so a takeover mid-window
  still gives a CORRECTED item and nothing already sent is sent again. The
  stuck-frontier views are wired (S36b); a stuck frontier on fewer than `fail_frontier_positions_n` positions fails
  only their POSITION scopes (§B14, S38c). The **decision-bar sequence** is the calendar-OPEN (evaluable) buckets
  only: closed buckets (weekends, daily breaks) are no bars, so they are neither spanned nor overdue, and an OPEN bucket
  resolved `no_trade` is decided `NO_BROKER_BAR`; neither can make a stuck frontier. The 10:16 `redis-md` restart drill
  tests §B6.6 as built (S38 review fixes).

### B6.7 Parameters

Strategy (pinned per position): `trigger_basis` (`exit_side` | `bid`), `trigger_comparison` (`gt` | `ge`), `latch`
(`off`), `be_offset_r` (0.0, 0-0.5), `rearm_on_loosen` (`off`), `bar_spec` (15m), and `guards`: the list
`[{kind, version, params}]`, the only place guard settings live (§B6.1), pinned per position together with the
strategy; validation rejects a kind (a) document without exactly one `first_bar` entry. FirstBarGuard entry
`{kind: first_bar, version: 1, params}` (`EVENTS` = {POST}: POST only, §B6.5; pinned with the guard list) params:
`anchor` (`fill_bucket` | `next_full_bucket`), `measure` (`high_low` | `body` | `true_range`), `comparison` (`gt` |
`ge`), `atr_bar` (`current` | `previous`), `multiplier` (1.318, 0.5-5.0), `atr_length` (14, 5-50; sets
`W = 10 × atr_length`, §A6), `unknown` (`hold_bar` | `skip_test`, the Q3 answer). These keys exist only in the guard
entry; a `first_bar.*` key in strategy params is rejected. Local engine settings only, never from Supabase (a document that sets one is rejected): `min_gap_r` (envelope; 0.0, 0-0.5)
and `pre_offset` (40 s, 30-45 s, §B5.2). Engine constants: `post_grace` (60 s) and `decision_cap` (§B5.2).

## B7. Position inputs

* **Arming guard:** fresh image, entry price known, an effective stop (PR1; children covering only part of the units do not count, as in Oversee), an accepted assignment. A position without an
  assignment is `UNASSIGNED`; one whose assignment cannot be served (no broker bars for the adapter or for a side the
  position needs, §B6.4; asset class unsupported;
  over the demand cap, the strategy's `arming_requirements` unmet, §B6.1; under PR9 the adapter's armed-handle ceiling
  reached, `UNSUPPORTED(stop_read_budget)`) is `UNSUPPORTED`. Both get a one-shot `POSITION_NOT_MANAGED` notice (keyed by position and
  reason) on first sight, `UNSUPPORTED` also pages once, and unmanaged open positions are listed in the daily test
  notice, `arm_status` and `arm show`. An assigned OPEN position not armed 5 minutes after its image is
  fresh pages.
* **Entry time (the fill, never the placement):** the broker time of the first `POSITION` version with `origin stream`
  and status OPEN, or of the entry order's `FILLED` / `PARTIALLY_FILLED` version from history (`ExecutedDateTimeUtc`);
  filtered on `entity_type`; `PENDING` / `WORKING` versions (a stop or limit entry's placement time) are never used;
  watermark-labelled REST rows are never used. Source `STREAM` / `BROKER`, else `ESTIMATED`; stored at arming (repin
  reuses it). If PR3 shows the Open item's `LastChangedTime` is not the execution time, arming reads
  `ExecutedDateTimeUtc` from trade history and the entry time is `ESTIMATED` until then. Positions filled during downtime
  are armed after the catch-up covering the fill, with the cursor set as in §B6.6 (bars whose `decision_cap` has passed
  are one `UNRESOLVED_RANGE` line, never replayed); `ESTIMATED → BROKER` upgrades are allowed until bar 0's window ends.
  Test: a stop-entry order placed hours before it fills, live and through the catch-up.
* **S0:** `AT_FILL` only when the stop handle's level comes from a **live stream version** with broker time at most
  fill + 10 s and no later amendment before it; a level taken from history (StoneX keeps only the last level) is
  `HISTORY_LAST_LEVEL`, treated as `FIRST_SEEN` with a caveat in the notice; otherwise `FIRST_SEEN`. Kind (c) refuses to
  arm without a true `AT_FILL` (its `arming_requirements` say `s0: AT_FILL`). `R ≤ 0` at arming: no R values shown; released only at a bar end (§B6.3 (7a)).
* **Changes:** units decrease, unchanged; units increase **while the entry order is still non-terminal** (partial fills)
  is fill progress: bar 0 stays at the first fill's bucket, `E` (volume-weighted) and `S0` are re-pinned and noted; any
  other units increase, `SUSPENDED` (notice; re-paged every 6 h); entry change with
  units unchanged within ±2 min of the 17:00 New York roll, `ROLL_ADJUST` (original `E` kept); otherwise a correction
  (re-pinned, noted); replaced positions end with a notice.
* **As built (S26, `src/riskmgr/tracker.py`, `arming.py`):** fill progress is a units increase that the entry order's
  fills explain, whatever its status (a working order up to its size, a terminal one up to ordered − remaining), so a
  final fill or a last partial fill with a cancelled remainder re-pins instead of suspending; a units decrease lowers
  `pinned_units` (`state_reason` `units_decrease`); `S0` `AT_FILL` also requires each handle's newest counting version
  at arming to be the stream version read at fill + 10 s; `ROLL_ADJUST` is judged on the position's newest broker
  update and noted durably (`state_reason = roll_adjust_<16 hex>`, an opaque fingerprint, no level); "replaced" is a
  vanished position while another unarmed OPEN position of the same symbol and side is in the image; a row CLOSED or
  DISARMED is never re-planned; the arming span row is written by the tracker (caveat `first_bar_unknown_stale` when it
  holds bar 0).

## B8. Notifications and what PostgreSQL keeps

* **Policy** (`route_policy` in the accepted document; checked before a row counts as pending, unknown keys rejected):
  `mode` (`every_bar` default or `actionable`); `post_channels` ⊆ {`arm_email`, `arm_push`} and contains `arm_email`;
  `pre_channels` ⊆ {`arm_email`, `arm_push`}, never empty (default `arm_email` only, Q4). The PRE offset is a local
  engine setting (§B5.2, D1), never a policy key: a document that sets one is rejected. Recipients, senders and account
  aliases come from local settings only. D7 pings ignore this policy (below).
* **No decision without a notice:** every decision row records the `notice_id` it was rendered in (NULL until then);
  each notice transaction selects "decisions of this route with `notice_id` IS NULL", so no committed decision can be
  left out. An actionable `LATE` or `CORRECTED` item (an `ADJUST`, or a withdrawn `ADJUST`) is sent at once as its own
  notice (part 0, key below), whatever Q9 says.
* **Notice keys and parts** (the one definition; §B1 and §B5.3 point here): `arm_notices` is unique on `(route,
  bar_spec, event, bar_end_ms, part, item_key)`, `item_key text NOT NULL DEFAULT ''`, with CHECKs `part BETWEEN 0 AND
  5` and `part = 0` unless `event` is PRE or POST. `item_key` is empty for the PRE and POST bar notices and the hourly
  ALIVE line (keyed by the hour in `bar_end_ms`), and names the item for every other event and for a POST item sent
  after part 4: `adapter_id:position_id:resource:owner:revision` (LATE, CORRECTED, POST after part 4),
  `adapter_id:position_id:resource:owner:<decided_at_ms>` (REOPEN), `adapter_id:position_id:reason`
  (POSITION_NOT_MANAGED) (`adapter_id` in front because two brokers may reuse a position id; `src/riskmgr/notices.py::item_key_of` (new),
  `src/db/arm_ports.py::position_ref` (new)), the channel (daily TEST), the md generation (MD_RESET, §A13), the
  `failure_id` (FAILED_CLEARED, §B14). So LATE, CORRECTED and REOPEN items are always
  part 0 and never collide with each other or with the bar notice.
  * Parts of a PRE or POST notice: part 0 goes out with the event's first decisions, or at `T + post_grace` for POST if none
    is decided by then, and lists every position still pending (for POST "awaiting the broker's final bar", INFO,
    not a FAULT). Each later commit of decisions for the event makes the next part, 1 to 4. After part 4, a POST
    `ADJUST` or `COMPLETE` is sent at once as its own item-keyed notice (event POST, part 0, `item_key`
    `adapter_id:position_id:resource:owner:revision`), so advice is never held back by the part cap; every other decision waits for
    part 5, the one end-of-window part, sent only if it has items, at the end of the event's window: PRE at its
    valid-until time, POST at `decision_cap` (§B5.2). A part-5 item is never superseded before it has been sent once.
    A final decision committed after its event's part 5 (a replayed bar) goes out only as a LATE item. `DELAYED`
    labels: §B5.2.
  * A decision whose `notice_id` stays NULL for more than 60 s (for one waiting for part 5, 60 s after the window
    end) while its route's channels are configured pages.
  * Test: one POST with 6 positions decided after part 0 (parts 1-4, then part 5 at `decision_cap`), and 2 LATE items
    for different positions in the same bar (both queued); 8 symbols whose bars become final at 8 distinct times each
    get their `ADJUST` at once (parts 1-4, then item-keyed notices).
* **Notice:** one per notice key above; exactly-once at write time, delivered at least once until `expires_at_ms`
  (PRE: local `T + p50`, superseded once the POST exists; POST parts and LATE, CORRECTED and REOPEN items: superseded
  once the route's next POST part 0 exists, and at the latest one bar after they were created; no escalation; a
  duplicate is recognisable by the notice id in the subject, which also carries the bar end in UTC and Melbourne, the
  event and the part). As built (S28, S29): `ALIVE` expires after one hour and the other non-bar notices (`TEST`,
  extras, `MD_RESET`, `FAILED_CLEARED`) after one day; the daily `TEST` goes out at 22:00 UTC by default; LATE,
  CORRECTED and REOPEN items are always their own notices (Q9 folding applies to the tracker extras only); a notice has
  no attempt cap (it ends by delivery, supersede or expiry); symbol, side, `E` and `S0` come from the position rows, so
  an item rebuilt after a restart lists no handle ids and no stop read time; the body key for the close source is
  `trigger_source`; a finished body keeps the notice key (ids) and drops order-state fields.
* **Release texts** (rule: §B6.3 (7a)): `COMPLETE`: "released: the bar end confirmed break-even; the ARM no longer
  manages this position" (plus the position's **pinned** Q10 policy, carried on the item as `loosen_policy`, S38d;
  `LOOSENED_AFTER_COMPLETE` is always the tell-only text); `PRE_NOT_CONFIRMED`: "bar end did not confirm; the rule would not
  have moved your stop", the reason, and what releases the position now: "still managed until a bar end confirms the
  trigger against your stop as it was before this move" (`pre_basis`, §B6.3); `AT_TARGET_UNCONFIRMED`: "at target, not yet confirmed;
  still managed until a bar end confirms".
* **Immediate ping** (the D7 route; when it fires and how FAILED is cleared: §B14): queued at once when a scope enters
  FAILED, as two outbox rows with the same ping id (in both titles), inserted in the same fenced transaction as the
  `arm_failures` row (§B14 Record): a `critical` page through the Oversee relay (`OutboxRow(kind="info",
  severity="critical")` through `insert_outbox(tx, ...)`; email and mobile push, as `channels_for` in
  `src/oversee/notify.py` routes `critical`, with that relay's escalation) and a kind `arm` row on `arm_email` and
  `arm_push` (both are required for the ARM to start, §B14; the ARM relay sends it at Pushover priority `high`, never
  emergency, and retries it until delivered). Neither expires. The log mirror is `PagerWrapper.log_mirror()` (a `log.critical` line, PR12), never
  `EngineSession.page`, which would queue a second critical row. It bypasses everything that can delay or drop an ARM
  notice: route policy (`mode`, `pre_channels`, `post_channels`), Q9 folding, notice parts, digests, a pause and
  `--drain-notices`. The body names the scope (series id, adapter id, strategy version or `arm`), the reason code and
  the affected position ids, bounded to N positions then "and K more; see `arm failed show`"; never a price or error
  text. If the outbox cannot be written, the off-node alert path and the push dead-man's switch
  are the backstop (§B14).
* `actionable` mode: PRE with a provisional `ADJUST` (on bar 0 always rendered as conditional on the first-bar check,
  §B6.5); POST with `ADJUST`, a HOLD, **any FAULT**, `COMPLETE`, `PRE_NOT_CONFIRMED`, a change from PRE, or a folded
  item. Both modes: an hourly "ARM alive" line carrying the number of positions in FAULT (in `every_bar` mode only in
  an hour with no other ARM notice); released positions and a repeated `AT_TARGET_UNCONFIRMED` are never shown
  (§B6.3). Decisions not shown are marked with a `suppressed` notice sentinel, so they are never dumped
  later (built: the sentinel is `arm_decisions.notice_id = 0`, `SUPPRESSED_NOTICE_ID`; real notice ids start at 1). Page `arm_position_faulting` after 2
  consecutive final POST FAULTs while the market is OPEN (one episode per position and reason); `arm_overdue_positions`
  ("no final decision for more than 2 OPEN bars") pages locally as well as going to the dead-man. A daily test notice per channel.
  **As built (S38g, `src/riskmgr/liveness.py`):** `arm_overdue_positions` counts the live rows in ARMED, TRACKING or
  ADVICE_OUTSTANDING (not released, SUSPENDED or FAILED, not covered by a FAILED scope, no pause) with more than 2
  OPEN bar ends after `last_decided_bar` whose `decision_cap` has passed, from the SM10 schedule (`CalendarGrid`), and
  every such row whose symbol has no loaded schedule (never zero); it is the dead-man field, `arm_status`'s
  `overdue_positions` and the `xtnl_arm_positions{view="overdue"}` series, and pages "ARM: armed position not decided
  for 2 OPEN bars" once per episode. The tracker's count of assigned positions not armed after 5 min is the `unarmed`
  view (its own page is unchanged). The same watch pages a BarClock with no completed pass for 60 s ("ARM: decision
  loop stalled"), an arm-tracker with none for 120 s ("ARM: arm-tracker stalled") and an unsent ARM notice older
  than 15 min ("ARM: notice relay backlog"). It runs in the `arm-status` loop (every 5 s), independent of the `arm`
  loop it watches. **Round 14 review:** the rows are the arm-tracker's (read every 10 s), so after a restart that
  follows a long downtime they show the old cursor for up to 10 s after the BarClock wrote the DOWNTIME span; a
  position first seen overdue is counted and paged only once a BarClock pass that started after that sighting has
  completed and tracker rows read after it still show it overdue, or after it has stayed overdue for 2 min
  (`LivenessWatch.confirm`). A stalled-loop age is measured from no earlier than the time the watch first saw the
  loop allowed to run (`running` turning True), so a BarClock that starts or resumes after a long hold is never paged
  before it could pass. Not built yet: `arm_position_faulting` and the 5-minute `redis-md` unreachable page (FAULTs show
  in the hourly ALIVE line; `redis-md` down counts toward `fail_md_down_s`).
* **Pages** (episode keys and re-page cadence): a POST or LATE notice with an `ADJUST` that expires or fails (via the
  Oversee route); assigned symbol not ready (BROKER ATR not warm for more than 2 bars while its market is OPEN);
  `redis-md` unreachable for 5 minutes (whether or not a position is armed); demand refused over the cap; ARM channel
  failing; config pending > 1 day, tampered, schema ahead, or unreachable for 15 min;
  POST missed while running; assigned position not armed; two non-evaluable bars in a row; a market-data series GAPPED
  for more than one bar on an armed symbol; `redis-md` memory 70 % and each shedding step (§A8); circuits; paused > 6 h;
  SUSPENDED. Market-data faults that cannot heal reach the user through §B14 (§A2, §A4, §A6, §A13); a `redis-md` reset
  alone is one INFO notice (§A13), never a page.
  PRE expiry is a metric, not a page.
* **Content allow-list:** account alias, position id, symbol, side, bar times, event, outcome and category, proposed stop,
  handles to move (ids) and the stop's read time, R values (0.01 R), ratios (0.001), ATR quality (not its value),
  `borderline`, sources (entry, S0, close), caveats, strategy code (`^[a-z0-9_]{1,32}$`, escaped) / version / hash prefix,
  "valid until"; entry and current stop only if Q13. A render test covers every outcome and item.
* **`arm_decisions` keeps:** outcome and category, `trigger_met`, `borderline`, the proposed stop (order state), R
  values from order state only (the current risk `σ·(E − S)` in R, 0.01 R), the first-bar ratio (0.001), quality
  flags, `close_source` (`QUOTE` on PRE rows, `BROKER` on every bar-end row; there is no TICK close), input ids with keyed hashes `(series_id, bar_end_ms, hash)` and `(indicator_id, output,
  bar_end_ms, window_start_bucket, hash)` (revisions as information only, §B6.6), `inputs_digest` (an HMAC over them,
  keyed like every stored content hash, never a plain hash), `supersedes_decision_id`, `decision_seq`,
  `decided_at_ms`, `advice`, `envelope_version` and the EnvelopeParams hash (§B6.1), `stop_basis`, `notice_id`, and
  one `arm_decision_handles` row per stop handle (§B10). **No** OHLC, quote, spread or indicator value, and no favour
  value: `favour` in R and `favour / risk` give the close back from the stored entry and stop, so they never reach
  PostgreSQL, not even inside an outbox body. **An ARM outbox body holds ids only**, from its INSERT on: the notice id,
  decision ids, outcome codes and order-state fields. The arm-relay renders the text **at send time**: it re-reads each
  input bar through `MarketDataPort` by `(series, bar_end_ms)` and checks its keyed hash, and shows favour R and
  favour / risk only when that check passes (omitted when the bar is unavailable). A PRE item has no input bar (its
  quote is frozen at fire time and never stored), so **a PRE notice carries outcome codes and order state only, never
  favour R or favour / risk** (round-11 confirmation C5, §0). As built (S36b, S38d), a bar-end row lists the trigger
  bar `series|T|key_id‖hash` first, every history bar the strategy and its guards read (same form), and every
  indicator value read as `I|id|output|bar_end|window_start|key_id‖hash` (the keyed hash of the value's lineage,
  never the value), all covered by its HMAC `inputs_digest`; the first-bar ratio is the only indicator-derived figure. The statement that finishes an ARM
  outbox row (sent, expired or superseded) replaces its body with the notice id and outcome codes. Test: no ARM column,
  ARM outbox body (unsent, expired or finished) or INSERT payload, `operator_requests` row or page body gives a bar
  close more finely than `trigger_met` and `borderline` do. `arm replay` (read-only, except an optional audited `md_backfill` request
  with `--request-backfill`) recomputes from `redis-md` (current or superseded revisions) and checks the digest; if the
  bars are no longer retained it reports `bars_missing`, and with `--request-backfill` it files an audited `md_backfill`
  operator request that the leader's `md-bars` carries out, and reports `pending`; if broker history no longer reaches
  the window start it reports `UNVERIFIABLE`, not a mismatch. As built (S35) `arm replay` re-reads each decision's
  recorded inputs as of its decision time and re-checks their keyed hashes and the digest; it does not yet re-run the
  pipeline (no recomputed outcome); a missing TICK bar is `UNVERIFIABLE(tick_history_gone)`, a BROKER bar beyond the
  broker's reach `UNVERIFIABLE(beyond_broker_reach)`; a `CHANGED` or `DIGEST_MISMATCH` is reported with exit 0.

## B9. Configuration in Supabase

* Schema `arm` (not exposed through PostgREST; `REVOKE ALL` from `PUBLIC`, `anon`, `authenticated`, `service_role`;
  matching default privileges; RLS forced: `SELECT` for `xtnl_arm_reader`, `INSERT` for `xtnl_arm_writer`). Server-side
  CHECKs: `octet_length(params::text)` ≤ 4 KB, per-table row caps. `arm.schema_version`; `strategies(code, version, kind,
  impl_version, params)` (`params` = the strategy's own keys plus its `guards` list, §B6.7; there is no separate guard
  table), `assignments(edge_code NULL, symbol NULL, account NULL, adapter NULL, strategy_code, strategy_version,
  enabled, effective_from)` (NULL = wildcard; v1 requires `edge_code` NULL), `route_policy`. Append-only guards set `id`,
  `set_at`, `session_user`, `current_user` and bound `effective_from`; the SQL editor must `SET ROLE xtnl_arm_writer`.
  Network restrictions on the Supabase project allow only the engine's egress addresses where available. The schema,
  roles, grants, RLS policies and guards are provisioned from one file in the repository,
  `deploy/supabase/arm_schema.sql`, which the user applies by hand and which is versioned by `arm.schema_version`.
* **Versions and rows in force** (one resolver in `src/armconfig`, shared by validation, install, the arm-tracker and
  `arm config resolve`): rows are never updated; a change is a new row. A `strategies` row is immutable and unique on
  `(code, version)`; any change, guard params included, is a new `version`, and an assignment names
  `(strategy_code, strategy_version)`. For `assignments` per (scope, RESOURCE of the named strategy) and for
  `route_policy` per route, the row in force at time `t` is the one with the greatest `effective_from` ≤ `t`; an
  assignment with `enabled = false` is a tombstone (nothing in force for that key from then on). Two rows with the same
  key and the same `effective_from` are a validation error. Assignments are resolved at the position's arming time (the
  row in force per key first, then the precedence below); route policy at each event's bar end. A future-dated row is
  accepted ahead and applies from its `effective_from`; a newer row or a tombstone never touches an armed position
  (§B12).
* Reader hardening: `NOINHERIT`, `NOBYPASSRLS`, `CONNECTION LIMIT 2`, role-level read-only, timeouts, no CREATE, owns
  nothing; `XTNL_ARM_CONFIG_DSN` (§C2 Secrets);
  direct host or session pooler; `SSLContext` (CERT_REQUIRED, hostname check, TLS 1.2+, pinned CA, never
  `sslmode=require`); `search_path = pg_catalog`; client `LIMIT` and byte cap; JSON parsed with Decimal and a depth limit
  in a thread with a timeout; connect self-check on every connection (`has_table_privilege` shows SELECT only,
  `pg_class.relforcerowsecurity` is true on the `arm` tables, the role has `rolbypassrls = false` and
  `rolinherit = false`; any failure fails closed: keep the cache and page); pinned-CA expiry in the certificate alert.
* **Acceptance (Q12):** the document (every row; the resolver picks those in force) installs only after `python -m src.ops arm config accept <hash>`
  (refused if the pending document changed after `arm config show --pending`, which shows the full diff). **Trust boundary:** Supabase
  is untrusted storage. The only trust boundary is the local accept: the `arm_accepted` hash, written by the ops CLI on
  the engine host. RLS and the append-only guards bind `xtnl_arm_writer`, not the project owner, so the Supabase account
  needs MFA. The engine keeps a sha256 chain over every row it has seen. **Pending** = a row not seen before (a new row,
  including one that supersedes an older row): the accepted document stays in force until accept, and a page follows
  after 1 day. **Tampered** = a seen row whose content or hash changed, or that vanished (fewer rows, broken chain): the
  accepted document stays in force, it pages at once, and `arm config show --pending` lists the row as tampered. A newer schema is `schema_ahead`. The cached
  document's hash must equal the latest `arm_accepted` hash at start-up and install, else the ARM pauses. **What
  installs** (built, `src/armconfig/installer.py`, S33): the candidate is the polled read itself when it hashes to the
  latest accepted hash; otherwise the **accepted subset**, the rows set at or before a cut-off that hash to it (cut-offs
  tried: `accepted_at_ms`, the engine host's clock, and each distinct `set_at_ms`, the Supabase server's clock, within
  5 min of it, nearest first, at most 64); rows added after the accept stay pending and can never block the accepted
  document, and the hash comparison, never the cut-off, decides. Skew beyond 5 min keeps the ARM paused (`config_hash`).
  A `config_hash` pause pages `warning` at once and **`high` once it outlives the first install slot after the accept**
  (the first `T + post_grace` after the accept and one poll, plus one more poll; at start-up on an older accept at
  once; `src/armconfig/installer.py::ConfigInstaller.lingering_after_ms`, S38c), so no row added after an accept can
  silently pause advice for hours. The cache hash is recomputed from the cached rows. At start-up a cache whose
  hash equals the latest accepted hash is restored at once. No accepted
  document: the ARM is idle (one page). Install at `T + post_grace`: the
  first 15m bar end `T` whose `T + post_grace` is after now, so a document never changes mid-bar. Precedence: `edge` (later) > `symbol` >
  `account` > `adapter`, set beats NULL; per (scope, RESOURCE) the resolver gives at most one row in force; spot FX
  on bar-capable adapters only in v1. If Q12 = automatic: validation (bounds, known ids) and tamper
  detection on seen rows still apply, a document with a tampered row never installs without a local accept, each
  automatic install writes `arm_config_audit` and is announced in the next notice, and automatic mode is Phase 1 only.
  Local two-person acceptance becomes mandatory when Phase 2 consumes the config. As built, automatic mode is not
  built (an `arm_accepted` row with mode `automatic` is a manual accept of its hash), and `arm config accept` is
  single-person (`--approver` optional). The default `post_channels` is `arm_email` only.

* **Row validation** (zero trust applies to config too; a row is checked before it counts as pending; a failing row
  keeps the accepted document and pages by row id): `strategies.code` matches `^[a-z0-9_]{1,32}$`; every kind and
  version a row names (strategy, guard, indicator) is registered, and its params pass that registry's `parse_params`
  (guard settings only as `guards: [{kind, version, params}]`); `assignments.symbol` is a symbol in `instruments`,
  `adapter` a configured adapter id, `account` a known account id or local alias, `edge_code` NULL; `route_policy`
  per §B8; any other text column is rejected. Pages, logs, `arm_status` and `arm_config_audit` name Supabase rows only
  by id and row hash, never by their text; `arm config show` escapes control and non-printing characters.
* **Error text** (`src/armconfig`, `src/marketdata`, `src/riskmgr`, the arm-relay): these packages catch driver
  exceptions at their boundary and log, page, store or re-raise only a sanitised error: the exception class, the
  SQLSTATE or the Redis error prefix (`NOAUTH`, `WRONGPASS`, `NOPERM`, `READONLY`, ...) and a fixed reason code, as
  `safe_error()` in `src/notifications/errors.py` does today: `safe_db_error` (`src/db`: the class name and SQLSTATE
  only) and `safe_redis_error` (`src/marketdata`: the class name and the first token of a `ResponseError` checked
  against an allow-list, never the message). Never `str(exc)`, a driver traceback, a DSN, host, port, user or
  password: the supervisor logs and pages a dying task's `str(exc)`, and the URL filter in
  `src/observability/redact.py` only cuts URLs and DSNs to scheme, host and port. Test: a planted DSN, host and password appear in no
  log record, page, `arm_status` row, `arm_config_audit` row or outbox body.

## B10. Phase 2 hand-off (contract only)

* POST decisions are durable, immutable, superseded by revisions, pinned, and carry input ids with keyed hashes,
  `inputs_digest` and one `arm_decision_handles` row per stop handle: `handle_id`, kind (`CHILD_ORDER` |
  `POSITION_FIELD`), the level and broker version (`broker_seq`, `broker_updated_at_ms`) as at bar end (`S_at_T`) and as
  last read before the post-guards, that read's time, and the target (all order state). The actionable key is (the
  `arm_decisions` key, `handle_id`); "newest" is max(`decision_seq`) among `advice` rows (§B6.6). An actioner refuses a
  handle whose broker re-read differs from the observed level or version, unless the only change is a tighter level
  still short of the target.
* **Actionable** = all of: an `advice` row; not superseded; not PRE; not `UNRESOLVED_RANGE`; within the local maximum
  age (measured from both the bar end and `decided_at_ms`); not under a FAILED scope. `LATE`, `REOPEN` and
  `CORRECTED` rows are **labelled** (by event and revision) so an actioner can tell them apart and show them, but they
  are not refused by default: they are bar-end advice like POST.
* Sinks plug in through `Sink.on_decisions(tx, event, bar_end, decisions)` (own savepoint, no I/O); a future
  `StopMoveSink` lives in its own package.
* **Only bar-end decisions can be actionable.** An action may come only from a decision on StoneX's final bar-end close:
  POST (also when delayed waiting for the broker bar) and the LATE, REOPEN and CORRECTED items re-run from it (with
  their revisions). PRE is never actionable, never pre-stages or prepares an order and is never an input to one, bar 0's
  conditional PRE included. PRE, superseded, `advice = false` and `UNRESOLVED_RANGE` rows are marked (by event,
  revision and `advice`) so an actioner refuses them; `LATE`, `REOPEN` and `CORRECTED` rows are labelled, not refused
  (Actionable, above); an actioner also refuses a decision older than the local maximum age, and re-reads the broker
  before acting.
* **FAILED blocks actions.** An actioner refuses every action for a position covered by a FAILED scope (§B14): a series
  it depends on, its adapter, its pinned strategy or guard version, or the whole ARM; an unreadable FAILED state counts as FAILED. This check is separate from, and in
  addition to, `ActionGate.allow(action)`: the gate does not see ARM data health, and it passes a purely risk-reducing
  action (a stop tighten) after only the boot id, epoch, lease and halt checks (`src/oversee/gate.py::evaluate_gate`, the
  `not action.risk_increasing` branch). A decision
  taken before FAILED was raised never becomes actionable after FAILED is cleared.
* No intent, sender or adapter mutation method exists; the lockout scanner makes adding one a reviewed change.
* Known requirements for the Phase 2 design: queued, expiring intents with a non-colliding id per stop handle; at most
  one non-terminal intent per stop handle, each carrying the handle's expected broker version (`broker_seq` /
  `broker_updated_at_ms`), so a re-send of a stop amend is refused once the handle has changed; gate,
  broker re-read and price re-check before `SENDING`; `Action.kind` for stop moves applying the risk-increasing freshness
  list; the operations §5 send rules (intent committed first; `lease_deadline − max_send_time` with no await before the
  write; a new leader keeps the gate shut and marks earlier `SENDING` intents `UNKNOWN`, never re-sent); StoneX
  resolution without client ids; a transport-scoped amend that can only tighten; manual-override ownership; approvals
  and a durable switch; non-owner engine database role (then the engine gets SELECT only on `arm_accepted` /
  `arm_controls`); a maximum decision age, measured from both the bar end and `decided_at_ms`; an amend that touches two
resources at once (a cTrader stop and take-profit change in one request).

## B11. Multi-edge hand-off (contract only)

Phase 1 is single-edge: `arm_positions.edge_code` = `XTNL_ARM_EDGE`, `attribution_source = SINGLE_EDGE_DEFAULT`;
`attribution_source` is an enumeration with a CHECK (`SINGLE_EDGE_DEFAULT`, `OPERATOR_TAG`, `INTENT`), and Phase 1
writes only the first. Phase 1 refuses a configuration that enables a second edge. With several enabled edges (the
multi-edge design), a position whose edge cannot be attributed is `UNATTRIBUTED` and is never armed under a default
edge; `arm_reattribute` belongs to that design. Decisions and metrics carry an `edge` label; assignments already have a
NULL-only `edge_code`; `arm_positions` is keyed on
(adapter_id, position_id, resource, owner) (`adapter_id` first: two brokers may reuse a position id; every position
reference outside the keyed tables is `adapter_id:position_id`). In Phase 1 both extra key parts are NOT NULL constants with CHECKs: `resource =
'STOP_LEVEL'`, `owner = 'position'` (the whole position). Attribution is per position on brokers that keep one position
per opening order (StoneX). On a netting account an owner would be an owner-scoped tranche with its own `E`, `S0`, entry
time, units and stop handles, not a single handle; that is the multi-edge design's work. Arming stores the position's
`opened_by_client_order_id` in `arm_positions` as the future attribution input. On StoneX this stored id is the
broker's own order id (StoneX has no client order ids here), so it carries **no edge information**: attribution on
StoneX needs an operator tag or a Phase 2 intent (`OPERATOR_TAG`, `INTENT`). The multi-edge design covers attribution sources
and timing, hedging versus netting accounts, edge-scoped assignments and policies, the thesis allowing several deployed
edges, and strategy chaining. The market-data layer is already shared.

**Market-data consumer contract** (canonical here; the ARM is the first consumer, §B6.6; every future strategy, edge or
out-of-process service uses it unchanged):

* **Pull, never push:** a consumer reads bar-end events after its own cursor `(bar_end_ms, write_seq)`, in order,
  through `MarketDataPort` (§A10), revisions of ids it already applied included (§A13). Events are identified by `(series, bar_end_ms)` and versioned by the keyed content hash (§A13); a revision is
  node-local and never part of an id. The producer keeps no per-consumer state, never waits for a consumer and never
  stores acknowledgements, so an out-of-process consumer needs only `md_reader`.
* **Acknowledge in your own store:** a consumer acknowledges an event by durably recording its id and hash in its own
  store, in the same transaction as the effect it derived from it (ARM: `last_decided_bar` plus hash, §B6.6). Never in
  `redis-md`.
* **Apply:** same id and same hash is a no-op; same id and a different hash, both final BROKER bars, is a genuine
  correction; missing or unreadable data never counts as changed. An event is a wake-up only: the consumer re-reads and
  re-validates the bar through the port (the read-side checks of Part A) before use.
* **Reset and rollback:** on a reset flag (md generation change, §A13) a consumer stops acknowledging and waits until
  its series are LIVE again (or BACKFILLING with the range complete, §A10), then resumes from its own cursor and
  re-applies in order. A cursor older than the writer's rollback set gets `CURSOR_EXPIRED`: `md-bars` re-fetches the
  missing events from StoneX, and if that fails the consumer records one unresolved range and applies its own fail-fast
  rule (ARM: §B14).
* **Isolation:** each consumer, and each edge, has its own cursor (per position for the ARM), cursor-lag metric and
  error budget (in Phase 1 the per-edge budget is the `edge` label on the STRATEGY_VERSION scope, §B14). A slow or
  FAILED consumer never slows the producer or another consumer.

## B12. State machine SM11 (per adapter, position, resource, owner)

```
DISCOVERED --(fresh image, entry known, stop, accepted assignment)--> ARMED        (UNASSIGNED: no assignment)
ARMED --(first POST, any outcome)--> TRACKING
TRACKING --(final ADJUST)--> ADVICE_OUTSTANDING --(final ADJUST, better target)--> ADVICE_OUTSTANDING
ADVICE_OUTSTANDING --(POST without ADJUST and latch off: "withdrawn" | handles reached the proposal)--> TRACKING
ARMED | TRACKING | ADVICE_OUTSTANDING --(bar-end check on the final close, fresh image: trigger met, all handles at target)--> STRATEGY_COMPLETE (released, §B6.3 (7a))
STRATEGY_COMPLETE --(correction of the release bar, still the latest decided bar, no longer releases)--> TRACKING
STRATEGY_COMPLETE --(handle below target, rearm_on_loosen on)--> TRACKING
any --(position CLOSED)--> CLOSED          any --(local disarm)--> DISARMED          (terminal)
any --(3 strategy errors | pinned version missing | units increase)--> SUSPENDED --(local re-arm)--> ARMED
any non-terminal --(a FAILED scope covers it: series, adapter, strategy version or ARM, §B14)--> FAILED (stores prior_state)
FAILED --(no open FAILED episode covers it any more)--> prior_state
FAILED --(SUSPEND)--> FAILED (prior_state := SUSPENDED)
SUSPENDED --(local re-arm, arming guard passes, a FAILED scope covers it)--> FAILED (prior_state := ARMED)
```

SM11 is implemented as one pure transition table, `src/riskmgr/sm11.py` (new; built as `next_state(state, event, *,
prior_state) -> Transition(state, prior_state) | Refused`, because the FAILED edges read and write `prior_state`; events
are named, e.g. `ARMING_PASSED`, `FIRST_POST`, `FINAL_ADJUST`, `RELEASED`, `SCOPE_FAILED`, `FAILED_CLEARED`); no other module sets `arm_positions.state`. Every edge above is allowed and every other (state, event) pair
is refused; PRE and a stop move seen between bars have no edge. The FAILED edge stores the state it left in
`arm_positions.prior_state` and the clear edge restores it. **A position can be covered by several FAILED scopes at
once** (its trigger series, its FirstBarGuard / ATR series, its adapter, its strategy or guard version, the ARM): a
second `SCOPE_FAILED` over a FAILED position is refused `already_failed`, and the FAILED manager
(`src/riskmgr/failed.py`, new) restores `prior_state` only when **no** open `arm_failures` episode covers the position
(the covering set from its pinned specs, its adapter and its versions; a level-triggered `row_version` CAS on the ARM
pool, state reason `failed_cleared`). Clearing one scope only removes it from that set. `SUSPEND` while FAILED only
changes the state the clear will restore. `LOCAL_REARM` / `arm_rearm` re-run the arming guard and arm into `FAILED`
while any covering scope is open, as the arm-tracker does for a new position.

A final ADJUST is one taken on the final bar-end close (POST, LATE, REOPEN or CORRECTED). PRE never changes the state.
PRE, a stop move seen between bars and the arm-tracker never release a position: only a bar-end check reaches
STRATEGY_COMPLETE (no longer managed; the test is in §B6.3 (7a)), and a stop moved back after release follows Q10.
`AT_TARGET_UNCONFIRMED` and `PRE_NOT_CONFIRMED` leave it managed. While a FAILED scope (§B14) covers a position, it is
in `FAILED`, gets no advice and no strategy transition, and its previous state is kept and restored once no FAILED
scope covers it; CLOSED and DISARMED still apply (terminal), and SUSPEND applies only to the state it returns to. Supabase changes never alter or end an armed position.

## B13. Local controls and operator tools

`arm_controls` and `arm_accepted` are written only by the ops CLI under a table lock, each with an `operator_audit` row.
Controls: `pause` / `resume`, `accept`, `notify-test`.

* **Pause** (an operator `pause`, or the config-hash pause of §B9 with reason `config_hash`, which ends when the hashes
  match again): no decision is made, so `last_decided_bar` stays where it was. No ARM notice goes out except the daily
  test notice (which says `paused`) and D7 pings (§B8). The arm-tracker keeps arming and ending positions and renewing
  leases. `arm_status` shows the pause and its reason. Paused positions count neither in `arm_overdue_positions` (page
  and dead-man field) nor toward the D7 error budget (§B14); only `paused > 6 h` pages. `resume` never clears FAILED
  (§B14).
* **On `resume`:** replay (§B6.6) re-decides the paused bars inside its horizon, older ones go into one
  `UNRESOLVED_RANGE` row, and one-shot notices held during the pause are sent.
* **`arm pause --drain-notices`** also makes the arm-relay finish queued, unsent ARM outbox rows as `suppressed`,
  never a D7 ping. The CLI only writes the flag in `arm_controls`.
* **Unreadable controls are not a pause:** the BarClock holds the due event and retries every 250 ms until the
  event's window ends (§B5.2). If they are still unreadable then, the event ends as its window rules say (PRE not
  sent, counted `controls_unreadable`; POST as at `decision_cap`).

One-shot requests through `operator_requests` (new kinds):
`arm_disarm`, `arm_repin` (`migrate_state`, §B6.1), `arm_rearm` (cursor as at arming, §B6.6), `md_backfill`, `md_rebuild` (an indicator), `md_writer_reset`,
`md_rehash` (key rotation, §A13), `arm_clear_failed` (§B14). The CLI refuses
when `arm_status` shows no heartbeat younger than 60 s and reports each request as applied, pending or moot. The
session's `_consume_operator_requests` dispatches the new kinds to handlers that `optional-bootstrap` registers; while no
handler is registered, a request stays **pending** (never `unsupported_kind`) and is applied once the ARM starts in the
same epoch (test). **Registered handlers are non-blocking:** a handler only enqueues the work to the task that owns it
(`arm-tracker`, `md-bars`, `md-indicators`, `md-writer`, or the FAILED probe runner) and returns at once; the owning
task answers the request later as applied, refused or moot, so a slow probe never delays a core request (test: a
`reset_task` filed behind a hanging `arm_clear_failed` probe is applied within one `operator_poll_s`). Pending ARM requests never hide core ones behind the existing `LIMIT 50`: the pending query
(`src/db/operator_requests.py::SQL_PENDING_WITH_PAYLOAD`) has `AND r.kind = ANY($2)` (done in S10), the core kinds
(`CORE_REQUEST_KINDS`) plus those with a registered handler, and the CLI refuses a new ARM request while 20 are pending in the epoch. `REQUEST_KINDS` and
the kind CHECK widen together: 0017 drops and re-adds `operator_requests_kind_check` with the full list, as 0009 did.
Test: 60 pending `arm_disarm` rows with no handler and one `reset_task`; the `reset_task` is applied. While a scope
is FAILED, `md_rebuild`, `md_backfill`, `md_writer_reset`, `md_rehash` and `reset_task` are still applied (they
repair, never advise; §B14). Runbooks:
`docs/runbooks/arm.md`, `docs/runbooks/marketdata.md` (every page, with its fix).

**As built (S10, S32, S34, S36b):** the owner answers through `EngineSession.complete_request(request_id, result)`
(synchronous; the operator loop marks the request consumed, fenced, on its next pass); a raising or non-synchronous
handler is consumed as `refused:handler_error` / `refused:handler_not_sync`; an accepted request whose owner task is no
longer registered is dispatched again. The handlers live in `src/runtime/request_handlers.py` and
`src/runtime/optional_plan.py`: `arm_disarm` / `arm_repin` / `arm_rearm` are parsed and enqueued to the arm-tracker
(answered applied, refused or moot by it; `refused:bad_payload`, `queue_full` or `arm_not_running` at once). With
`XTNL_MD_ENABLED=1` and the ARM off the heartbeat row is written on the engine pool, so `md ...` requests pass the
heartbeat rule; the 20-pending budget per epoch is kept for the ARM kinds and, separately, the arm-tracker kinds.
`arm notify-test` queues one TEST per channel keyed by the request time; a request first seen more than 1 h after it
was made (an earlier term's) sends nothing. The operator controls are read every 5 s by the `arm-status` loop and
every second by the BarClock.

## B14. Error budget, FAILED and the immediate ping

D7. When the market-data layer or the ARM meets a fault it cannot heal, the affected scope enters **FAILED**: it stops
advising (fail closed) and pings the user at once.

**Healing first.** A fault counts only after its normal healing has failed: a bar still invalid after its re-fetches
(§A4); no final broker bar at `decision_cap` for a live POST (§B5.2); a StoneX re-fetch still failing at `decision_cap`
(§A13); a supervisor circuit that opens again after its half-open retry (operations §1). A fault that heals is a metric
or an ordinary page. Bars missed while the engine was down or the PC slept go through replay (§B6.6) and never count.
Counters reset on a clean result and are shown in `arm_status` and `md show`. **A restart is never itself a clean
result** (S38f): the budget's own counters (the `unresolved` streaks, the `md_down` / `pool_down` time counters and
the unclean-restart watermark; `src/riskmgr/budget.py::ErrorBudget.durable_rows`) are written to PostgreSQL
`arm_budget_counters` (0020) on the ARM pool, fenced, in one transaction and only on change (a time counter in whole
10 s steps; a counter back at rest is written with count 0), and restored by the FAILED manager at start before the
BarClock's first event (`src/riskmgr/failed.py::FailedManager.start`); until that read succeeds nothing is written.
A restore that comes late (the first read failed, so the term has already counted) **merges** (R14 review): a stored
streak whose last bucket precedes the term's first adds to the term's count, a stored time total adds to the term's,
and the fault is filed at once if the sum reaches the threshold; a clean result the term saw first (a final bar, a
usable observation, an accepted clear) supersedes the stored counter. The restore runs after the open episodes are
read: a stored counter at its threshold counts as filed only while its scope has an open episode, so it files again
on its next count otherwise. An accepted clear writes the counters it resets (a SERIES streak at 0; for the ARM scope
both time counters at 0 and the restart watermark at the clear time) in the clear statement's own transaction
(`ArmFailedClearPort.clear(..., reset=)`, engine pool), so a crash or a failing ARM pool after the clear never brings
them back. They survive a restart, a takeover and a `redis-md` loss; a restored time counter never adds the time the
engine was down. **Unclean restarts count:** once per term the FAILED manager reads the end times of the leader terms
that ended `lost` or `init_failed` (`src/db/arm_budget_store.py::PgRestartHistory`, engine pool);
`XTNL_ARM_FAIL_RESTARTS_N` (3, 2-10) of them within `XTNL_ARM_FAIL_RESTART_WINDOW_S` (1800 s, 300-7200) while a
position is armed is ARM-global FAILED `unclean_restarts` with the immediate ping (probe `arm_global`). A clean stop or
a planned switchover ends its term `switchover` / `abdicate` and never counts; a PC sleep longer than the fence is a
hard exit and counts. A term the SAFE_HOLD brake aborted (operations §4: `leader_init_fence` ends its own new term
`init_failed` in the transaction that finds SAFE_HOLD due or set, so `reached_leader_at` is NULL and `ended_at =
started_at`) is a deliberate stop and never counts. **SAFE_HOLD preempts** (R14 review): with the default N = 3 a
crash loop trips the anti-flap brake (3 abnormal ends within 15 min) at the 4th LEADER_INIT, before any term's ARM
has seen 3 unclean ends; no node leads, so no ARM runs and no advice is issued, and the SAFE_HOLD page is the alert.
The operator's `resume()` clears SAFE_HOLD only: it is not a clean result for the ARM's budget, so the next term's ARM
counts the crashes before the hold and, if they still reach N within the window, goes FAILED `unclean_restarts` at
once and stays so until its own clear (the cluster may lead again; advice waits for the ARM clear). The engine core
never writes ARM tables (the ARM is optional and off by default), so FAILED is not filed in the SAFE_HOLD
transaction. An accepted clear of the ARM scope moves the restart watermark, so the restarts behind it never count
again. As built,
the market-data side's own K streaks (`invalid_bar` in md-bars, `source_mismatch` in the cross-check) are still kept
per term (plan S38f deviation).

**One counting unit** (the one definition; §A4 points here). A series counter counts **buckets, never reads**: a
bucket adds at most 1, and only when it is still invalid (or not final) at its `decision_cap`, or after its full re-read
schedule. Decode errors, undecodable responses and failed reads of a bucket are retries inside that bucket's healing:
they feed a metric and one page per episode, never the counter. Test: 3 undecodable responses for one bucket, then a
valid final bar, leave the series LIVE with K = 0.

**Counting pauses** (nothing counts, nothing enters FAILED) while:
* the market is not OPEN (SM10), or a declared broker maintenance window is active;
* the adapter is not STREAMING, or its REST reads fail at every priority (a broker outage, read from the per-priority
  REST outcome counters): the series' cap counters and the re-fetch row pause, one `broker_unavailable` page goes out
  per episode and the series is GAPPED (§A7 SM12), never FAILED; bar-read failures count only while other REST
  priorities on the same adapter succeed;
* for the two decision-cap counters (no final bar, stuck frontier) also while **engine PostgreSQL is down**, defined as:
  the term pool's last successful fenced commit is older than 30 s, or the RiskSweep's "PostgreSQL unreachable" alert
  is active (that outage is paged by Oversee and the dead-man).

**Error budget.** Thresholds are local engine settings (`XTNL_ARM_FAIL_*`; the series `K` is the market-data setting
`XTNL_MD_MISMATCH_K`, bounded 2-10, S38d), never from Supabase; an out-of-bounds value
stops the ARM from starting, with a page. **Scopes:** `SERIES`, `ADAPTER`, `STRATEGY_VERSION` (a pinned strategy
`(KIND, IMPL_VERSION)` or guard `(KIND, VERSION)`; in Phase 1 the per-edge budget is the `edge` label on this scope,
§B11) and `ARM` (global). The ARM-global scope is reached only through `redis-md`, the ARM pool, the task circuits and the stuck frontier.

| Scope | Cannot heal when | Setting: default (bounds) |
|---|---|---|
| SERIES | `K` consecutive buckets invalid after their healing, or `K` consecutive tick-bar mismatches (the D6 block, §A4, is this entry) | `K`, the D6 setting: 3 (2-10) |
| SERIES | 2 consecutive OPEN bars with no final broker bar at their `decision_cap` (S38e: md-bars feeds each counted bucket to the budget's `note_unresolved`) | `fail_unresolved_n`: 2 (1-4) |
| SERIES | a replay or correction inside the correction window needs a bar older than the rollback set (D5) and the StoneX re-fetch fails (S38e: a consumer re-fetch whose bucket ends `unfillable` or is beyond broker reach; dropped during a broker outage) | at once |
| SERIES (guard-input scope) | an indicator reaches `FAULTED` (3 faults in a row, §A6): S38d files it under the indicator's own scope id `adapter:symbol:ind:<indicator_id>`, never the input series; it covers only positions that still read the indicator (FirstBarGuard's ATR until bar 0 is decided or spanned); probe `indicator_rebuild` | at once |
| ADAPTER | this many series of one adapter FAILED at the same time (the adapter's series and positions then fail together) | `fail_global_series_n`: 3 (2-8) |
| STRATEGY_VERSION | 3 consecutive job faults (a raise or the 250 ms cap, §B4 Job runner) across the positions pinning that version; a guard fault counts against the guard's own version | at once |
| ARM | a **stuck replay frontier** (§B6.6): while the ARM runs, a bar of a position whose scope is neither FAILED nor paused is still neither decided nor `UNRESOLVED` 120 s after its `decision_cap`, because the `UNRESOLVED_RANGE` span write itself fails. A position on a FAILED scope (SERIES, ADAPTER, STRATEGY_VERSION) or a paused one never counts (its cursor stays put by design, §B6.6), nor does time with no ARM running, so a scoped FAILED never becomes a global one. Keyed by cause: when the cause is the ARM pool or its transactions failing, it defers to the `fail_pool_down_s` row below; otherwise at once after the 120 s grace. **Scoped (S38c):** while fewer than `XTNL_ARM_FAIL_FRONTIER_POSITIONS_N` (default 2, 1-8) positions are over the grace in one pass, each newly stuck position fails only its own **POSITION** scope (`adapter_id:position_id`, migration 0018), so one poisoned row leaves the others advising; at that count (a shared cause) the ARM fails globally | at once (after the grace) |
| ARM | `redis-md` unreachable, refusing every write, or unable to load its store, for longer than the setting while an armed position's market is OPEN | `fail_md_down_s`: 900 s (300-3600) |
| ARM | the `redis-md` bootstrap fails 3 times in a row with the store answering (§A7; S38e: unreachable, timeout, `LOADING` and `BUSY` never count, they are the `fail_md_down_s` row above), the store resets 3 times within one hour (counting per §A13), or it refuses writes for memory after the last shedding step (§A8) | at once |
| ARM | the ARM pool cannot be built, or its transactions fail, for longer than the setting while the engine's main pool works | `fail_pool_down_s`: 900 s (300-3600) |
| ARM | the circuit of `md-writer`, `md-bars`, `md-indicators`, `arm`, `arm-tracker` or `arm-relay` opens again after its half-open retry | at once |

**Effect (fail closed).**
* **SERIES FAILED:** decision reads of the series return `DATA_UNAVAILABLE(failed)`, and every non-terminal armed
  position whose pinned specs read it (trigger-side bars, FirstBarGuard bars, indicator inputs) enters SM11 `FAILED`
  (§B12). Other series and their positions carry on. **S38d:** a guard's inputs count only while the guard can still
  apply (FirstBarGuard: until bar 0 is decided or spanned and its `decision_cap` has passed), so a position past
  bar 0 never depends on the ATR or, for a short, on the bid series.
* **ADAPTER FAILED:** every series of that adapter is served as FAILED and every position on it enters `FAILED`; other
  adapters carry on.
* **STRATEGY_VERSION FAILED:** only the positions pinning that strategy or guard version enter `FAILED`; every other
  version keeps advising.
* **ARM FAILED (global):** every armed position enters `FAILED`, the BarClock makes no decisions, `MarketDataPort`
  serves no decision reads, and `md-bars` and `md-indicators` stop their ordinary writes; as built (S32) they still
  carry out the operator repairs of §B13 (`md-bars` runs only a repair pass for `md_backfill`; `md-indicators` runs
  only `md_rebuild`, its queued recomputes waiting for the clear), which write data and never advise. `md-writer` keeps capturing ticks and TICK
  bars, because tick history cannot be re-fetched (round-11 confirmation C2, §0), and still runs bootstraps, which
  repair the store and never advise (§A7).
* A position in `FAILED` gets no PRE, POST, LATE, REOPEN or CORRECTED advice; its frontier stays where it is; each bar
  notice of its route lists it in one status line ("FAILED since …, no advice"); it is left out of
  `arm_overdue_positions` and `arm_position_faulting`. A pause or `resume` neither hides nor clears FAILED.
* **Never stopped:** ingestion, the orchestrator, the market-data parser hook (pure, own circuit), Oversee (RiskSweep,
  HaltSwitch, ActionGate, its relay) and the integrity worker; in the ARM, `arm-tracker` (a new position on a FAILED
  scope arms straight into `FAILED`), `arm-relay` (it carries the ping), the `arm_status` writer and the operator-request
  handlers. The repair requests `md_rebuild`, `md_backfill`, `md_writer_reset`, `md_rehash` and `reset_task` are
  applied during FAILED (§B13). FAILED never writes `halt_events`, `halt_breaches` or `halt:state`.
* **Phase 2 (§B10):** the actioner refuses every action for a position in `FAILED` or on a FAILED scope, and treats an
  unreadable FAILED state as FAILED (fail closed, like `ActionGate`).

**Record.** `arm_failures` (0017, with the POSITION scope kind of 0018; `src/db/migrations/0017_arm.sql` and
`0018_arm_round12.sql` are the canonical schema), one row per episode:
`(failure_id text, scope_kind SERIES | ADAPTER | STRATEGY_VERSION | ARM, scope_id, edge_code, reason, probe_kind,
ping_id text, counters, position_ids, first_fault_at_ms, entered_at_ms, epoch, last_ping_at_ms, ping_count,
last_probe_at_ms, last_probe_result, cleared_at_ms, clear_request_id)`, with a partial unique index on `(scope_kind,
scope_id) WHERE cleared_at_ms IS NULL`. `probe_kind` is one of seven: `series_reread`, `fresh_bar`,
`indicator_rebuild`, `span_write`, `strategy_conformance`, `adapter_series` (an ADAPTER scope) and `arm_global`
(`src/db/arm_schema.py::probe_kind_for` (new)). `failure_id` and `ping_id` are text, made in memory before PostgreSQL has the
row, so a spool replay is idempotent; `position_ids` elements are `adapter_id:position_id`. `xtnl_arm` may not set
`cleared_at_ms` / `clear_request_id` (column grants, §B3). Counters hold
counts, bucket times and check names only, never values. The scope stops advising **in memory first**; then the row,
the ping's critical copy (`OutboxRow(kind="info", severity="critical")` through `insert_outbox(tx, ...)`) and its kind
`arm` copy commit in **one fenced transaction** (through the engine's own pool when the ARM pool is the fault), retried
from memory until committed (PR4). `optional-bootstrap` reads the open rows before the BarClock's first event, so
FAILED survives a restart and a takeover; if it cannot read them, the ARM advises nothing. `md:meta` mirrors a series'
FAILED state for `md show` (SM12, §A7); PostgreSQL is the truth. If PostgreSQL is down when the budget is exceeded,
the scope stops advising at once from memory, and its row and ping are written first when PostgreSQL returns; if the
engine may stop before then, the episode is appended to a CRC-framed local spool (`FailedSpoolPort`, like the DLQ
spill) and replayed at the next start before any advice. The database is always tried first and the spool is only
the fallback: its **first append pages `critical` at once** ("ARM FAILED not recorded [ping id]", S38c), because on
AWS a spool on a node that a leader change removes is lost with it: the new leader starts with no episode and **its
error-budget counters start from the last ones written to `arm_budget_counters`** (S38f), so the fault is counted
on from there before FAILED returns.
The page, the log mirror and the dead-man's `arm_failed` (from memory) tell the operator to check `arm failed show`
after any takeover.

**The immediate ping.** On entry, one critical ping per episode by push and email through the immediate route (§B8),
outside `mode`, any digest or quiet setting and any Supabase route policy; recipients come from local settings only.
Its critical copy goes through the Oversee relay as the outbox row committed with the episode (Record, PR4), so a
failing ARM channel cannot hide it; the log mirror is `PagerWrapper.log_mirror()` (`log.critical` only, PR12); `EngineSession.page` is never called for the ping, because it queues its own critical outbox row and every ping would be delivered twice.
The ping id is in the title. Content (the §B8 allow-list; never a price or value): scope, reason, counters, the time of
the first counted fault, the affected positions (alias, position id, symbol, side), bounded to N positions then "and K
more; see `arm failed show`", "no advice for these positions until FAILED is cleared; do not act on advice for them
issued since <first fault>", and the clear command. When a global FAILED follows series or adapter scopes that failed
together, one collapsed ping names them all instead of one ping each. It is re-sent every `fail_reping_h` (4 h, 1-24)
while FAILED. `optional-bootstrap` does not start the ARM unless both `arm_email` and `arm_push` are configured (a page
names the missing one), because the ping needs both; Q4 chooses channels for routine notices only.

**Backstop when PostgreSQL or the outbox is down:** the dead-man payload carries `arm_failed` (the number of FAILED
scopes and whether one is global, from memory), and the receiver pages through its own channel when it is non-zero; the
metric `arm_failed_scopes` has an off-node alert (`gen_alerts.py`). Neither needs PostgreSQL or the outbox. On the single
PC without an off-PC receiver there is no backstop: the ping goes out when PostgreSQL returns (§C2 Liveness).

**Clearing.** Only `python -m src.ops arm failed clear <scope> --reason <text>` on the engine host, after the cause is
fixed. It files an audited `arm_clear_failed` request (§B13); the handler enqueues the probe and returns (non-blocking).
The probe tests the **recorded cause**: `arm_failures.probe_kind` is chosen from the reason when the episode opens:
* `invalid_bar`, `source_mismatch`, `refetch_failed`: a StoneX re-read of the series' rollback set passes every hard
  check, and the tick cross-check where tick bars exist;
* `unresolved`: one fresh bar read reaches finality within `post_grace`: the probe waits for the next bar end `T'`
  after it starts, reads bucket `T' − timeframe` from `T' + p50 + settle`, and passes only when two equal reads
  `settle` apart (or a trusted clear partial flag) make it final by local `T' + post_grace` (a bar already closed
  when the probe starts is never the evidence);
* `indicator_faulted`: `md_rebuild` has completed and the indicator is `warm`;
* `stuck_frontier` (POSITION or ARM): the failed span write of the recorded positions, re-run fenced on the ARM pool
  inside a rolled-back savepoint, succeeds (a synthetic write would pass while the poisoned row still fails, S38c);
* `unclean_restarts` (ARM, S38f: `XTNL_ARM_FAIL_RESTARTS_N` unclean engine restarts or takeovers within the window
  while positions are armed): the ARM-global probe; fix what keeps crashing or fencing the engine first
  (`journalctl -u xtnl-engine`, the PC log), then clear; the clear moves the restart watermark.
* `restart_required` (ARM, S38c: more hung strategy threads than the job runner may abandon): the ARM-global probe,
  after the engine restart the cause needs;
* `strategy_version`: the conformance checks plus one successful job for that version (as built, S38e and S39b:
  the registry's declaration checks and the version's own `propose` / `at_target` / guard code against its
  conformance vectors, `src/riskmgr/vectors.py::vector_problems`, run as one job on the version's lane under the job
  cap; go-live gate `conformance_probe_partial`, wired);
* ARM global: a **trial bootstrap** (§A7, allowed while FAILED) writes `md:boot` under the writer token and boot lock,
  a test transaction on the ARM pool succeeds, and no task circuit is open. An ADAPTER scope runs the probe of each of
  its FAILED series.

If the probe fails, the request is answered `refused: cause still present (<check>)`, naming the command to run first
(for example `md rebuild` before an `indicator_faulted` clear), and FAILED stays (built as the reason code
`cause_present_<check>` plus `_first_<request>`). **Every probe is wired (S38e):** `series_reread` and `fresh_bar`
read the broker through md-bars' own probe reads (also while the series is FAILED; nothing is written or counted),
`strategy_conformance` runs the pinned class's declaration checks and its own code against its conformance vectors
as one job on the version's lane (S39b, gate `conformance_probe_partial`), `span_write`
re-runs the span write (S38c), `indicator_rebuild`, `adapter_series` and `arm_global` as before; `probe_not_wired`
remains only for a port missing from the wiring (none in this build) and the `span_write` probe before the graph
binds the BarClock. Otherwise `cleared_at_ms` is set,
one INFO notice goes out, each position that no other open episode covers returns to its `prior_state` (§B12; an
ADAPTER clear leaves its SERIES episodes open, one request clearing one episode), and replay (§B6.6) re-decides the missed
bars inside its horizon (older ones become one `UNRESOLVED_RANGE` row per position episode). After a global clear,
`md-writer` re-runs the bootstrap (§A7) and every series restarts in `BACKFILLING`. That bootstrap and the probe's
trial bootstrap (whether the clear is accepted or refused) never count toward the reset budget and send no MD_RESET
notice (§A13); an accepted clear resets the scope's counters and, for the ARM scope, the one-hour reset window. `arm failed show` lists open
episodes, counters and the last probe.

**Tests:** `K` invalid bars on one series fail that series and its positions only, while other series keep advising and
ingestion and the RiskSweep pass age are unaffected; `redis-md` down past `fail_md_down_s` gives a global FAILED with
Oversee unaffected; a 30-minute broker REST outage gives `broker_unavailable` and no FAILED; the ARM pool down gives
one ARM-scope fault, no series storm and one ping; a planted hanging strategy version fails only its own positions; the
ping bypasses `actionable` mode and a pause; with PostgreSQL down, `arm_failed` reaches the dead-man and the ping is
written on recovery; FAILED survives a restart and a takeover; a clear is refused while the cause is present and
accepted after; after the AOF-aside runbook step a global clear succeeds; a restart while globally FAILED with an empty
`redis-md` can still be cleared; an `indicator_faulted` clear is refused until `md rebuild`; a PC sleep longer than
`decision_cap` counts nothing. `docs/runbooks/arm.md` has a FAILED section (each reason, its probe, its fix, the
command to run first, the clear).

---

# Part C: storage, operations, testing

## C1. Storage

| Migration | Contents |
|---|---|
| 0015 | PR1 `orders_state.order_kind`; PR7 `instruments` columns; PR8 `event_log.origin` (its CHECK added `NOT VALID`, validated by the migrate post step) and `event_log.broker_time_ms` (nullable; real broker time only); drop the unused `sod_marks.position_marks` (no code in N or N+1 writes or reads it, so the drop is safe before the switchover; Q15) |
| 0016 | PR4 outbox columns, index (`ON ONLY`, post step), CHECK (`NOT VALID`, the `cardinality` / `array_to_string` form of PR4, validated by the post step) |
| 0017 | `arm_positions` (key: `(adapter_id, position_id, resource, owner)`, built so (S23) because two brokers may reuse a position id; also `pending_bar_end_ms`, `reopen_bar_end_ms`, `pinned_units`, `state_reason`, `config_doc_hash`; pinned semantics and specs; entry and S0 sources; `edge_code`, `attribution_source` (CHECK IN `SINGLE_EDGE_DEFAULT`, `OPERATOR_TAG`, `INTENT`, §B11); `opened_by_client_order_id` (on StoneX a broker id with no edge information); `strategy_state`; `state`; `prior_state` (restored after FAILED, §B12); `pre_basis` (handle levels, order state) and `pre_basis_bar` (§B6.3); `row_version`; the acknowledgement pair `last_decided_bar` + `last_decided_hash` (HMAC with its key id, §B3) of the bar-end event the frontier last applied, written in the same fenced transaction as that decision (§B6.6); `pending_event`; `decision_seq`); `arm_decisions` (append-only; range-partitioned on `bar_end_ms` with a DEFAULT partition, the migration creating the previous, current and next two months by an explicit loop over month offsets −1..2 (0009's `events_dlq_history` loop covers only 0..2, so it is not copied as is), and `optional-bootstrap` ensuring the current and next month through the lifecycle's fenced `xtnl_ensure_month_partition` call on the engine (owner) pool before the ARM's first write; no row predates its write by more than about two timeframes except `REOPEN` rows (§B6.6); unique key `arm_decisions_key (adapter_id, position_id, resource, owner, bar_spec, event, bar_end_ms, revision)` so `ON CONFLICT` works per partition; `notice_id = 0` is the `suppressed` sentinel; `decision_seq`, `decided_at_ms`, `advice`; `envelope_version` and `envelope_params_hash`; `stop_basis`; `close_source`; `notice_id`; input ids with keyed hashes (key id in front) and the HMAC `inputs_digest`, §B8; no OHLC, quote, spread, indicator value, favour R or favour / risk column; `supersedes_decision_id` is a composite reference including `bar_end_ms`; an `UNRESOLVED_RANGE` row also stores `range_from_bucket` and takes the range's last bucket as `bar_end_ms`; added to the lifecycle task's `PARTITIONED` (shift 0, ms ranges like `notification_outbox`, created ahead) **and** `AUDIT_TABLES`: kept hot, never exported, detached or dropped by the engine; growth about 2 rows per armed position per bar, under 200 rows a day per position; partitions below); `arm_decision_handles` (one row per stop handle per decision, §B10; partitioned and kept like `arm_decisions`); `arm_correction_audit` (`UNVERIFIABLE` correction results, §B6.6); `arm_notices` (`event` checked only for its form `^[A-Z][A-Z_]{1,31}$`, `item_key` per §B8); `arm_status` (keyed by node; also `md_sentinel`, `md_last_write_ms` for this node's §A7 record); `arm_failures` (§B14: FAILED scopes, one row per episode, columns in §B14; `scope_kind` CHECK IN (`SERIES`, `ADAPTER`, `STRATEGY_VERSION`, `ARM`); `probe_kind`; `ping_id`; ids, reason codes and counts only, never a price; inserted by the engine under the fence; cleared only through the ops CLI: the leader's `arm_clear_failed` handler, after its probe, sets `cleared_at_ms` in a fenced transaction under an advisory lock, linked to the request's `operator_audit` row (the halt-clear pattern of `src/ops/halt.py`); §B3's static check refuses any other SQL that sets `cleared_at_ms`); `arm_controls`; `arm_accepted`; `arm_config_audit`; `arm_config_cache` (row hashes and the seen-row chain head, §B9); `operator_requests` kinds (§B13, `md_rehash` included; the kind CHECK dropped and re-added with the full list); an `event_log` index `ON ONLY (entity_type, entity_key, broker_time_ms)` (built per partition by the post step) serving the earliest version (entry time, §B7) and the latest version at or before `T` (`S_at_T`, §B6.4); the role `xtnl_arm` (NOLOGIN, granted to the engine role; grants as in §B3); dashboard views (outcomes, R values, quality) with `GRANT SELECT … TO xtnl_dashboard` and no `xtnl_api` grant |
| 0018 | `0018_arm_round12.sql` (S38c, round 12): `xtnl_arm` loses UPDATE on `arm_decisions`, `arm_decision_handles`, `arm_correction_audit` and `arm_config_audit` (SELECT and INSERT stay), and gets a column-level UPDATE of `arm_decisions.notice_id` only (§B3); the `arm_decisions.outcome` CHECK gains `NO_BROKER_BAR` (§B6.2); the `arm_failures.scope_kind` CHECK gains `POSITION` (§B14). No column is added. |
| 0019 | `0019_orders_linked_idx.sql` (S38d): the partial index `orders_state_linked_stop_idx` on `orders_state (adapter_id, linked_position_id) WHERE is_reduce_only`, built `CONCURRENTLY` by the migrate post step (`TableIndex` 19); no column |
| 0020 | `0020_arm_hardening.sql` (S38f, round 14): `xtnl_arm` loses UPDATE on the identity columns of `arm_failures` (column UPDATE only on the ping and probe notes; `counters` and `position_ids` INSERT only) and the BEFORE UPDATE trigger `arm_failures_guard` (§B3); the table `arm_budget_counters` (scope kind, scope id, counter code, count, streak start, last event, epoch, update time; key (scope kind, scope id, counter); §B14) with SELECT, INSERT, UPDATE for `xtnl_arm`. No market-data column. |

**`arm_decisions` partitions:** 0017 creates the previous, current and next two months plus a DEFAULT partition (as every
partitioned table has, e.g. `notification_outbox_default` and `event_log_default` in `0001_init.sql`); lifecycle keeps
creating months ahead (`src/db/lifecycle.py::PARTITIONED`, which 0017's tables joined, and `ensure_months`).
Every insert's `bar_end_ms` lies within the replay horizon of now (the starting frontier and the one-row expired span,
§B6.6), except a `REOPEN` row, which is at most a market break old and lands in the current or previous month, so no
insert targets a month without a partition; the DEFAULT partition stays empty, and lifecycle pages if it
is not.

**Locks on partitioned tables** (`notification_outbox` and `event_log` are range-partitioned; `src/db/migrate.py` runs each
file in one transaction with `lock_timeout 2s` and retries, operations §12): new columns are nullable with no default, a
catalog-only change. A new CHECK on a partitioned table (the 0015 `event_log.origin` CHECK, the 0016 outbox CHECK) is added `NOT VALID` (enforced at once for
every new or updated row), then validated with `VALIDATE CONSTRAINT`, which takes only SHARE UPDATE EXCLUSIVE, so writes
continue. An index on a partitioned table (the 0016 outbox index, the 0017 `event_log` index) is created `ON ONLY` the
parent inside the migration (it builds nothing); a new no-transaction post step of `src/db/migrate.py` then runs
`CREATE INDEX CONCURRENTLY IF NOT EXISTS` on each partition (the DEFAULT one included) and `ALTER INDEX ... ATTACH
PARTITION`, after which the parent index is valid and partitions created later get it automatically. The post step and
the validation are idempotent and run before the switchover; no query needs the index or the validated CHECK to be
correct. 0019's partial index `orders_state_linked_stop_idx` (`orders_state` is not partitioned) is built by the same
post step with a plain `CREATE INDEX CONCURRENTLY IF NOT EXISTS` (`TableIndex` 19); an interrupted build leaves an
INVALID index, which the next post step drops and builds again. Small tables (`operator_requests`) keep the plain
drop-and-add of 0009.

`redis-md` holds all market data (§A3). Existing price columns and mark-derived account values in PostgreSQL: Q15 and
§0b Inventory (none added by 0015-0020; the schema scan test holds every column added by any migration from 0015 on to
an allow-list, S38g).

## C2. Operations

* **Liveness:** the `arm-status` loop computes `arm_overdue_positions` (§B8 As built, S38g) → dead-man (optional
  field, receiver first) and pages it locally, with the stalled-loop and relay-backlog pages. On the single PC only an
  off-PC receiver (e.g. `src.deadman_receiver` on the NAS) can notice silence; until then the per-bar notices, the
  daily test notice and these local pages are the signal.
* **Metrics** (contract, `metrics.md`, `gen_alerts.py`; bounded labels): market data (ticks in / dropped / receipt-time,
  ring and queue depth, writer lag, series by SM12 state, gaps, bars by source and quality, revisions, bar-fetch latency
  and REST waits / timeouts, backfill remaining buckets, indicator warm / `FAULTED` and recompute queue, validation
  failures and quarantined buckets by check, BROKER-vs-TICK mismatches and run length, AOF wait timeouts, md generation
  and store class, rollback-set depth, consumer cursor lag (reported by each consumer), `redis-md` memory and shedding
  step, skew);
  ARM (positions by state, decisions by kind / event / outcome, PRE skipped by reason, POST decision latency, close
  source, last POST age, notice hand-off lag and expiries, config age, FAILED scopes and error-budget counters (§B14)).
  As built (S32, S37) the exported set is the `CONTRACT` in `src/observability/metrics.py` and `deploy/observability/metrics.md`;
  items with no series yet (writer lag as a lag, md generation and store class, rollback-set depth, consumer cursor
  lag, skew, POST decision latency, close source, last POST age, config age) are marked there with where to read them;
  the POST latency page threshold is not set (it waits for PR3).
* **Operator views:** `md show` (md generation and store class, SM12 states, check counters, rollback-set depth,
  `FAULTED` indicators), `md bars` (counts, quality, revisions, quarantined buckets with check names; no values),
  `md indicators` (warm, lineage, `FAULTED`);
  `arm show`, `arm decisions`, `arm config show [--pending]`, `arm config resolve --position X`, `arm replay`. The status
  script reports ARM state (FAILED scopes included), a `redis-md` section and the Windows time-sync offset.
* **Local flags** (local settings only, never from Supabase). With both enable flags off no market-data or ARM task
  runs, no `redis-md` connection is made and no ARM row is written; enabling them never affects ingestion or
  Oversee. **The S07 change has its own switch** (PR1, C4; S38f): `XTNL_OVERSEE_STOP_AT_ENTRY=1` makes Oversee's
  `effective_stop` / `MissingStopLossRule` count a STOP child at or beyond entry as a stop (except beyond the entry of
  an unfilled entry order), no longer count a LIMIT child, and turn a stop on the wrong side of the mark into UNKNOWN
  then a capped SOFT; unset or `0`, Oversee runs exactly the pre-S07 rule. **A PC restart deploys the working tree**,
  so migrations are applied only up to `XTNL_MIGRATIONS_ACCEPTED_UPTO` (default 14) and the engine runs on the older
  schema meanwhile (§0b "What turns each on"). The prerequisite changes S03-S06 (the `RestPriority` numbering, the S05
  calendar predicate, the PR1 producers) are live with the flags off, but their stored fields wait for 0015:

  | Setting | Default | Effect |
  |---|---|---|
  | `XTNL_MIGRATIONS_ACCEPTED_UPTO` | `14` | migrations above it are held back at every start (named in one line, exit 0); `20` applies 0015-0020 (S38f) |
  | `XTNL_OVERSEE_STOP_AT_ENTRY` | `0` | `1`: the S07 / PR1 Oversee stop rule; `0`: the pre-S07 rule (S38f); needed for `oversee_c4` |
  | `XTNL_MD_ENABLED` | `0` | `start_optional_services()` adds `optional-bootstrap` only when `1` (§A2); on the PC `start-xtnl.ps1` starts `redis-md` only then |
  | `XTNL_ARM_ENABLED` | `0` | adds the ARM tasks; requires `XTNL_MD_ENABLED=1`, every go-live gate wired or accepted (§0b, S38e), both ARM channels and an accepted config (otherwise idle with one page) |
  | `XTNL_ARM_GOLIVE_ACCEPT` | empty | the open go-live gate ids you explicitly accept (comma-separated; `python -m src.ops arm gates`) |
  | `XTNL_TICK_SYMBOLS` | empty | PR5 tick-only symbols, a separate orchestrator set |
  | `XTNL_STONEX_PRICE_UNFILTERED` | `0` | `setRequestedMaxFrequency("unfiltered")` on StoneX price subscriptions; turned on only after the PR3 probe |
  | `XTNL_ARM_STOP_READS` | `0` | the PR9 stop reads; turned on only if PR3 shows that manual stop moves are not streamed |
  | `XTNL_MD_*`, `XTNL_ARM_*`, `XTNL_ARM_FAIL_*` | design values | parsed and bound-checked only inside `optional-bootstrap`; a bad value pages and parks the optional services, never a `WiringError` |

  The two enable flags are parsed tolerantly in `build_engine`: an invalid value means off, plus one page.
* **Secrets** (the canonical list; values are never logged, paged, printed or written to a file in the repository):
  `XTNL_MD_REDIS_URL` (`md_writer`, engine only); `XTNL_MD_REDIS_READER_URL` (`md_reader`, read-only: no consumer ever
  writes to `redis-md`, D4); `XTNL_MD_HMAC_KEY` (32 random bytes, hex: the key of every content hash stored in
  PostgreSQL, D4; stored hashes carry a key id, a rotation keeps the previous key as `XTNL_MD_HMAC_KEY_PREV` until
  `md show` reports the `md_rehash` complete plus one correction window, not for one correction window only (§A13), and
  `arm replay` reports `UNVERIFIABLE` for a hash whose key is gone); `XTNL_ARM_CONFIG_DSN`
  (§B9); and the ARM channel credentials `XTNL_ARM_SMTP_USERNAME`, `XTNL_ARM_SMTP_PASSWORD`,
  `XTNL_ARM_SES_ACCESS_KEY_ID`, `XTNL_ARM_SES_SECRET_ACCESS_KEY`, `XTNL_ARM_SES_SESSION_TOKEN`, `XTNL_ARM_PUSH_TOKEN`,
  `XTNL_ARM_PUSH_USER` (PR4). Of these the ops CLI reads the reader URL and the HMAC key (`arm replay`, the md views)
  and `XTNL_ARM_CONFIG_DSN` (`arm config show --pending` and `accept`), so the reader role's `CONNECTION LIMIT 2` allows
  one engine and one CLI connection. Non-secret settings beside them: `XTNL_ARM_CONFIG_CA_FILE` (env file) and the
  key ids `XTNL_MD_HMAC_KEY_ID` (1-255, required) / `XTNL_MD_HMAC_KEY_PREV_ID` (required with the previous key):
  operator-set and only increasing, so a new key never takes a retired key's id (S12; `new-md-credentials.ps1
  -RotateKey` prints the two values to set).
  Hashes compare only under the same key id; a stored hash under a retired key never counts as changed (D4).
  - **PC:** only in the Windows User environment. `start-xtnl.ps1` adds every name above, and the existing Oversee
    channel credentials (`XTNL_SMTP_USERNAME`, `XTNL_SMTP_PASSWORD`, `XTNL_SES_ACCESS_KEY_ID`,
    `XTNL_SES_SECRET_ACCESS_KEY`, `XTNL_SES_SESSION_TOKEN`, `XTNL_PUSH_TOKEN`, `XTNL_PUSH_USER`, `XTNL_DEADMAN_TOKEN`),
    to `$UserVarNames` and `$ForbiddenInFile`, so the FAILED page path (§B14) can deliver on the PC too. Non-secret
    `XTNL_ARM_*` settings stay in the env file. The API child never holds any of them: `$ApiDropEnv` and
    `src/api/settings.py` `FORBIDDEN_PREFIXES` both gain `XTNL_MD_`, `XTNL_ARM_`, `XTNL_SMTP_`, `XTNL_SES_` and
    `XTNL_PUSH_`, and the drop list and `FORBIDDEN_NAMES` gain `XTNL_DEADMAN_TOKEN`; the existing deploy test keeps the
    two lists equal, and a rule test checks that every `$UserVarNames` name other than `XTNL_API_*` is dropped.
  - `deploy/local/new-md-credentials.ps1` (like `new-api-credentials.ps1`) creates the two `redis-md` passwords and the
    HMAC key, stores the URLs and the key only in the Windows User environment, and writes the ACL file to
    `%LOCALAPPDATA%\XTNL\redis-md\users.acl` (outside the repository and OneDrive), mounted read-only. The file holds
    `#<sha256>` password entries only; `md_health` is `nopass` with `+ping` only, so the healthcheck needs no secret.
  - **AWS:** the same names from the secrets source (the HMAC key identical on both nodes); the ACL file is rendered
    from it at boot, `#<sha256>` entries only (built: `src/ops/md_acl.py`, run as the `xtnl-redis-md` unit's root
    `ExecStartPre`, deriving the passwords from the two URLs; the unit is installed but not enabled; Redis 7.2 or newer
    is required for `WAITAOF`).
* **Rules:** CLAUDE.md (market data only in `redis-md`, derived from the broker, not a duplicate of PostgreSQL;
  mandatory skill for `src/marketdata`, `src/riskmgr`, `src/armconfig`); tier-persistence §1 (a market-data tier), §2 and
  §3 (`redis-md` is outside SM8 and the integrity worker), §4; operations §1, §2, §3, §4 (hard check is the engine
  Redis), §5, §9 (dashboard freshness rule, notification content; the FAILED ping and its dead-man backstop, PR4), §10 (tick-only
  symbols never decide an adapter's OPEN, PR5), §11, §12 (migration locks, §C1), §15 (`BARS`, PR11); dto-contracts §2
  (`order_kind` inheritance, PR1), §3, §6, §7 (the ARM is not a bus subscriber, §B1); adapter-isolation §2 (`order_kind`
  mapping, PR1), §4, §5; oversee-rules §2, §5 (`effective_stop_detail()`, PR1), §10; new `.claude/rules/marketdata.md` and `.claude/rules/riskmgr.md`
  ("adding an indicator"; "adding a strategy", which includes a registry-wide conformance suite run over every
  registered strategy version: pure and deterministic under replay, every REASON mapped to a category, DECISION_METRICS
  typed, `parse_params` rejects unknown keys and out-of-bound values, `migrate_state` round-trips from every older
  version or refuses, the envelope property test; naming convention); the architecture generator gains the market-data
  layer, the ARM, SM11 and SM12 (built as page 6 of the diagram, S37). Also oversee-rules §10 (channel credentials never
  baked into the environment) is an accepted single-PC deviation: the ARM and Oversee channel secrets live in the
  Windows User environment, never in a repository file, and are dropped from the API child (as operations §17 frames
  the single PC). **Status:** none of these rule, hook or `CLAUDE.md` changes is applied; each is a pending proposal
  under `docs/design/proposed-claude-changes/` (list and interim guard: §0b), applied only after the user approves it.

## C3. Testing

* Market data: tick → 1m → roll-ups (exact); single-source series; quality classes; late ticks and BROKER re-read
  revisions; finality at the daily break and Friday close (two agreeing reads); `no_trade` vs `blind`; ring overflow does
  not degrade bars; writer token rejects an ex-leader; `NO_BOOT` after FLUSHALL while running, after an older AOF, and
  without AOF (in-place bootstrap); `redis-md` down at start-up (engine reaches LEADER, ingestion and Oversee run,
  market data GAPPED); suspension; retention and shedding order; a sizing load test; ACL (reader cannot write; default
  user off); recompute with 8 symbols keeps event-loop lag under 100 ms.
* Indicators: ATR canonical value equals the window-seeded Pine reference within the quantum and plain
  `ta.atr` within the stated relative tolerance (§A6), at lengths 5, 14 and 50; incremental
  equals canonical at checkpoints; replay from re-fetched broker bars gives the identical quantised value; recompute
  after a revision; `as_of` reads across a revision and past the `md:bars_rev` retention; a multi-output and a
  multi-input test indicator added without store or port changes.
* ARM: envelope property tests; kind (a) boundaries, long / short; triggers on the 16:45-17:00 New York bar and Friday's
  last bar (`MARKET_CLOSED`, then `REOPEN`); first-bar anchors and boundary ambiguity; replay without restart;
  corrections; validity windows (PRE at offsets 30 s and 45 s, each with a 10 s window, §B5.2); a broker bar final at `T + 3 min` gives a `DELAYED` POST
on `S_at_T` with live post-guards; a bar never final is `UNRESOLVED` at `decision_cap`, before the next bar's POST);
clock step.
* PR1: break-even stop across a weekend, touched stop awaiting fill, mislabelled take-profit, cTrader position stop; DTO
  round-trip through every mapper and `dbcheck`; a child stop covering only part of the units (ARM `NO_STOP`, Oversee
  `missing_stop_loss`); stream-then-REST and REST-then-stream on one StoneX child (no version flip, `order_kind` stays
  `STOP`); a `None`-kind child at entry (not counted, caveat `stop_kind_unknown`); with PR9 on, a stop moved between
  stream frames is seen by Oversee and the ARM at the same level.
* No market data in PostgreSQL: every ARM column, log record and page body checked, and every ARM outbox body (unsent,
  expired and finished) and its INSERT payload; a schema scan of every column added by any migration from 0015
  on against an allow-list; R quantisation.
* Failure drills: replayed POST (no `EpochMismatch`, no duplicate); transient error in a savepoint; 30 s PostgreSQL
  outage; market-data hook raising on every tick (calendar still fed); ARM channel down while a HARD alert is delivered;
  an Oversee relay tick never touches `arm` rows; an ARM row rescheduled after a failure passes the channel CHECK.
* Notices, config acceptance (added, changed, deleted rows; empty read; schema ahead), lockout scanner (passes today's
  tree, fails on a planted sender name or network import), end to end on `MockBrokerAdapter` (nothing reaches the broker).

## 20. Questions in full

1. **Q1.** Your message: "at every 15 min candle until sl order is at break even, if (end price − entry price) >
   (entry price − sl) then adjust the sl accordingly". (a) The first close beyond the current risk moves the stop to
   entry. (b) Each such close moves the stop by the excess, `S + σ·(favour − risk)`, until entry. (c) Your SOP legend
   says "Trigger bar length < 1.318 → Standard trailing", and the earlier XTNL break-even trail moved the stop 1:1 with
   each new favourable close (`S0 + favour`), which does not use the inequality.
2. **Q2.** Your message: "if the bar is > 1.318 ATR (of the candle at that moment)". SOP legend: "Trigger bar length ≥
   1.318 → Delay Trail by 1 Bar" (static text, no formula; the HUD separately shows "Cur body ratio" = body ÷ ATR(14)).
3. **Q3:** hold the first bar, or skip the test, when it cannot be computed?
4. **Q4:** the PRE heads-up (D1: 40 s before bar end by default, a local setting of 30-45 s, valid for 10 s after
   it fires, §B5.2) by email only (default, as your request says), or push and email (recommended: an email can
   arrive after the bar has closed)?
5. **Q5:** trigger close on the exit side, or always bid?
6. **Q6:** break-even exactly at entry, or plus a buffer in R?
7. **Q7:** first bar = the candle the fill is in (a fill at 10:14:50 makes 10:00-10:15 the first bar), or the first
   full candle after it?
8. **Q8:** "SL" = the current stop or the original?
9. **Q9:** extra items inside the per-bar notice, separate, or off?
10. **Q10:** stop moved back below entry after the ARM released the position (a bar end confirmed break-even): tell
    only, or manage again?
11. **Q11:** a bar triggered but you did not move the stop: keep advising it, or re-test each bar?
12. **Q12:** Supabase edits only after a local `accept` on the engine host, or automatically after validation?
    Automatic means whoever holds the Supabase login changes the advice without touching the engine host; validation
    and tamper detection still apply, and local accept becomes mandatory once Phase 2 reads the config.
13. **Q13:** notices show only the proposed stop and R values, or also entry and current stop?
14. **Q14:** candle and ATR from StoneX bid bars (default), or which broker feed does your TradingView chart use? The
    server cannot read TradingView data, so another feed needs its own design. If PR3 finds daily-break prints in StoneX
    bars, decisions follow those bars (as a chart on the same feed would) with a `roll_affected` caveat.
15. **Q15:** the existing PostgreSQL price columns are `last_clean_marks` and `sod_marks.clean_mids`. The four
    readers belong to `last_clean_marks`: the Redis-loss bootstrap (SM8 step 3 seeds prices;
    `src/integrity/bootstrap.py::RedisBootstrap._seed_live`); the RiskSweep's every-pass mark input (closed-market and
    roll-break marks, and its fallback when Redis is unavailable; `src/db/snapshot_reader.py::SQL_CLEAN_MARKS`,
    `src/oversee/risk.py::_latest_clean`); the RiskSweep's start-of-day row at every 17:00 New York roll and the
    LEADER_INIT catch-up (`src/oversee/risk.py::PreRollMarkBook`, `src/runtime/session.py::EngineSession.catch_up`);
    and the calendar's reference quotes after a takeover (`src/runtime/session.py::EngineSession.seed_calendar_quotes`).
    `sod_marks.clean_mids` has no reader (S38e: decoded and never consumed), so it is an audit copy only. Writers: the
    price consumer's 10 s upsert and the forced write at open / close / roll transitions
    (`src/db/repository.py::SQL_LAST_CLEAN_MARK`, `TieredStateCommitter._maybe_persist_clean` /
    `force_clean_marks`), and the start-of-day row (`src/db/sweep_writer.py::SQL_SOD_INSERT`). Keep (recommended,
    deviates from your instruction), move `last_clean_marks` to `redis-md` (each reader needs a bounded, fail-closed
    replacement; on AWS a takeover loses the marks unless `redis-md` is replicated), or also move
    `sod_marks.clean_mids` (no Oversee input changes; only the PostgreSQL audit copy of the start-of-day mids goes)?
    Round 12 adds two mark-derived account values to the same decision: `account_snapshots.equity` /
    `unrealised_pnl` (the live sampler, about one row per account per minute, read by SM8) and
    `sod_marks.sod_equity` (the `MaxDailyLossRule` base). Keep them as account state (recommended; Oversee depends on
    them), or ask for a separate design (§0b Inventory).
16. **Q16:** retention in the market-data Redis as in §A5?
17. **Q17:** keep an append-only file on disk for the market-data Redis?
18. **Q18 (decided by you, D2):** "when check bar end price (even at the delay)". The bar-end notice waits for StoneX's
    own final bar, however late, up to the end of the next bar (`decision_cap`); after that the bar is `UNRESOLVED`.
    There is no streamed-close fallback, so no advice is built from a streamed (TICK) close. Accepted trade-off: a slow
    broker bar delays the advice (labelled `DELAYED`) instead of sending faster advice on the streamed close that might
    later be withdrawn.
