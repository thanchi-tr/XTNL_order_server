# Runbook: Automated Risk Manager (ARM)

**Phase 1 never sends orders.** The ARM only **advises**: it watches open positions that have an accepted strategy
assignment, decides on each bar end whether the stop should move (for example to break-even) and sends a notice to
the ARM channels. It never places, modifies or cancels an order and never closes a position; acting on a notice is a
human decision taken in the broker's own platform. It is **optional**: it runs only with `XTNL_MD_ENABLED=1` and
`XTNL_ARM_ENABLED=1`, both ARM channels and an accepted configuration; enabling, pausing, failing or disabling it never
affects ingestion, Oversee, the gate or leadership (design §B1-§B3, §C2). Notices, pages, tickets and this runbook name
positions by id and outcome codes only: never a price, a level, an R value, a spread or an account amount.

Design: `docs/design/automated-risk-manager.md` Part B (§B5 runtime, §B6 strategy and pipeline, §B8 notices, §B9
configuration, §B12 SM11, §B13 operator tools, §B14 FAILED). Market data underneath: `marketdata.md`. Note: the
decision path is built on the plan §9.2 defaults (C1-C4) that the user has not confirmed yet; treat advice accordingly.

## Trigger

### FAILED pings

`ARM FAILED [<ping id>] <SCOPE> <scope id>` (and `ARM FAILED re-ping N [...]`): a FAILED scope opened (design §B14).
The ping goes out at once (critical, by the ARM channels and the engine's outbox) and its log mirror; the off-node
`XtnlArmFailed` is the backstop. No advice is given for the positions the scope covers until an operator clears it.
Go to "FAILED: find the cause, run the first command, clear" below.

### Pages and their fixes

| Page | Meaning | Fix |
|---|---|---|
| `ARM not started` | `XTNL_ARM_ENABLED=1` but go-live gates are open and not accepted (the body names the gate ids, plan S38e), an ARM channel is missing (the body names it) or no ARM pool is wired | gates: "Go-live gates" below (`$OPS arm gates`); a channel: set the named `XTNL_ARM_*` channel credentials (secrets source / Windows User environment); then restart the engine |
| `ARM go-live setting` | `XTNL_ARM_GOLIVE_ACCEPT` names an unknown gate id or one already wired (ignored; nothing is accepted by it) | correct the list (`$OPS arm gates` prints the ids), restart the engine |
| `ARM not started yet` | the ARM bootstrap failed N times; it keeps retrying | read the exception type in the body; `journalctl -u xtnl-engine`; ARM pool or `redis-md` reachable? |
| `ARM sink set refused` | the wired sink chain is not the Phase 1 set; the ARM stays off this term | a build error: do not enable the ARM with this build; report it |
| `ARM lockout violation` | the read-only lockout scan found a banned name or import in `src/riskmgr`, `src/marketdata` or `src/armconfig`; the optional services are parked for the term | never enable this build; the body lists the violations; fix the code and run the three checks |
| `ARM lockout scan incomplete` | 3 scans in a row could not read some files (a OneDrive placeholder, a locked file); retried | make the files local / readable (PC: "Always keep on this device"); it clears by itself |
| `ARM role grants differ` | the `xtnl_arm` role's grants are not the design's (§B3) | re-run the migration job (`xtnl-migrate`); compare with `src/db/migrations/0017*`; never grant by hand |
| `ARM partitions not ensured` | the current / next month partition of an ARM table could not be created | `partition-archive.md` checks; the lifecycle task retries; rows land in DEFAULT meanwhile (lifecycle pages if it fills) |
| `ARM FAILED spool damaged` | the local FAILED spool file has a damaged record (it is moved aside); FAILED state comes from PostgreSQL | `arm failed show`; nothing else to do unless PostgreSQL was down at the same time |
| `ARM FAILED not recorded [<ping id>]` | a FAILED episode could not be written to PostgreSQL; it is held in memory and this node's spool only and is recorded when PostgreSQL returns (design §B14, S38c) | restore PostgreSQL; after any leader change before then, run `arm failed show` on the new leader: the episode and the error-budget counters do not move with the lease, so the scope advises again until its fault is counted again; keep the positions on manual management meanwhile. This page goes through the engine outbox (PostgreSQL), so the same condition also reaches you off-node as the dead-man page `dead-man: ARM FAILED not recorded (<n> episodes)` (`arm_failed.unrecorded` in the beat); if the leader changes first it becomes `dead-man: ARM FAILED episode lost in a leader change` (latched 1 h, or until a FAILED scope is reported again) |
| `ARM configuration idle` / `ARM idle: no accepted configuration` | no configuration source (`XTNL_ARM_CONFIG_DSN`) or no accepted document: the ARM arms nothing | "Configuration" below |
| `ARM paused: configuration hash` | the installed document is not the latest accepted one; no decision until it installs | `arm config show`; wait one `arm-config` pass; if it stays, read the other configuration pages |
| `ARM paused past its install slot` | the `config_hash` pause outlived the first install slot after the accept (or the engine started on an older accept): no advice (design §B9, S38c) | `arm config show --pending`: an invalid, tampered or pending read, or Supabase unreachable; fix it, or accept the document Supabase now holds (`arm config accept <hash>`) |
| `ARM pause bounded: installed configuration kept` | the `config_hash` pause lasted 4 install slots past the page above while a document is installed: the pause ends (state `held`) and advice resumes under the installed document; armed positions keep their pins; the accepted document installs at its next slot once a read yields it (design §B9, S38c review) | as above; until it installs, route policy and arming follow the installed document, not the accepted one: if that is wrong, `arm pause` until it is fixed |
| `ARM configuration source unreachable` | no Supabase read for N minutes; the accepted document stays in force | check the network / Supabase status; nothing changes until it is reachable |
| `ARM configuration read refused` / `ARM configuration CA unusable` | the read was refused (TLS, auth) / the pinned CA file cannot be loaded | fix `XTNL_ARM_CONFIG_CA_FILE` or the reader credentials; the accepted document stays in force |
| `ARM configuration schema ahead` | `arm.schema_version` is newer than this engine reads | upgrade the engine before using the new schema |
| `ARM configuration invalid` / `ARM configuration self-check failed` / `ARM cached configuration refused` / `ARM configuration cache unreadable` | a document failed validation / the Supabase self-check / the cached copy is refused or does not hash | `arm config show --pending` for the codes; fix the document in Supabase; the accepted document stays in force |
| `ARM configuration waiting for instruments` | an adapter's instrument map has not loaded; nothing is installed | the adapter's SM4 state (`auth-failed-rearm.md`, `broker-maintenance.md`) |
| `ARM configuration pending for a day` | new rows in Supabase were not accepted for a day | review with `arm config show --pending`, then `arm config accept <hash>` or revert them in Supabase |
| `ARM configuration tampered` | a row already seen changed without a new version (critical); nothing installs | treat as a security incident: rotate the Supabase writer credential, review `arm_config_audit`, restore the rows |
| `ARM: configuration enables a second edge; refused for arming` | Phase 1 arms one edge only (§B11) | disable the extra edge in Supabase |
| `ARM: assigned position not armed after 5 minutes` | an assigned OPEN position has no fresh image / entry / stop yet (the body says what it waits for) | `arm show <adapter:position>`; usually the position has no stop or the adapter is not STREAMING |
| `ARM: position not managed (UNSUPPORTED)` | the position cannot be managed (no broker bars for its side, stop-read budget, units unknown; reason in the body) | nothing to clear; manage it by hand |
| `ARM: position SUSPENDED` / `ARM: position still SUSPENDED` | 3 strategy errors, a missing pinned version or a unit increase | fix the cause (`arm registry check --against-db` for a missing version), then `arm rearm <adapter:position>` |
| `ARM: paused for more than 6 h` | the operator pause has lasted 6 h | `arm resume` when intended; FAILED pings still go out while paused |
| `ARM: awaiting broker bar after post_grace` | the broker bar of the bucket is not final after `post_grace`; POST waits until `decision_cap` | usually the broker's own bar delay; `md bars <series>`; it resolves or becomes UNRESOLVED |
| `ARM: bars unresolved at decision_cap` | the bar never became final: the bar is decided as an UNRESOLVED span | nothing to clear; `arm decisions <adapter:position>`; a repeat reaches FAILED (SERIES `unresolved`) |
| `ARM: decisions without a notice` | decisions are still without a notice 60 s after their window | `arm-relay` / ARM channel state (next two rows); `task reset arm` if the `arm` task is stuck |
| `ARM channel <name> failing` | consecutive delivery failures on an ARM channel; notices are queued and retried until they expire | fix the channel (credentials, provider status); `arm notify-test` after the fix |
| `ARM notice #<id> expired undelivered` | an ADJUST notice expired before any delivery | read the advice in `arm decisions`; it is no longer actionable |
| `ARM: armed position not decided for 2 OPEN bars` | liveness (plan S38g): an armed position (ARMED, TRACKING or ADVICE_OUTSTANDING, not FAILED, no pause) has more than 2 OPEN bars past their `decision_cap` with no decision, or its symbol has no loaded market schedule; the same count is the dead-man's `arm_overdue_positions` | `arm show <adapter:position>` and `arm decisions <adapter:position>`; the market schedule (`calendar show`); the `arm` task state (`task show`); a BarClock that keeps holding (clock steps, lease) shows in `arm status` counters `barclock_*`; manage the position by hand until it is decided again |
| `ARM: decision loop stalled` | the BarClock has completed no pass for 60 s while it should be deciding (stamps may still be fresh) | `task show`; `arm status` (`barclock_clock_steps`, `barclock_lease_holds`, `barclock_errors`); `task reset arm`; if it repeats, restart the engine |
| `ARM: arm-tracker stalled` | the arm-tracker has completed no pass for 120 s | `task show`; `task reset arm-tracker`; check the engine pool (its snapshot read) |
| `ARM: notice relay backlog` | the oldest unsent ARM notice is older than 15 min (the dead-man pages the same age off-node) | the ARM channel pages above; `arm-relay` task state; `arm notify-test` after the fix |
| `ARM: engine restarted after a PC sleep` | the PC slept longer than the lease fence (12 s): the keeper fenced on wake and the engine restarted (design §A7 "PC sleep"). Bars missed during the sleep are spanned (`downtime`); this restart is **not** counted toward `unclean_restarts` | nothing to clear; read the spans with `arm decisions`; keep the PC awake during market hours if the ARM must advise |

## Prerequisites

* **Order** (plan S38f, design §0b "What turns each on"): first raise `XTNL_MIGRATIONS_ACCEPTED_UPTO` to `20` once
  migrations 0015-0020 are reviewed (the next start applies them; until then they are held back and named in one
  `[migrate]` / journal line, and the engine runs on the 0014 schema), then `XTNL_OVERSEE_STOP_AT_ENTRY=1` once the
  S07 Oversee stop rule (design §0 C4) is confirmed, then `XTNL_MD_ENABLED=1`, then `XTNL_ARM_ENABLED=1`. The go-live
  gate `oversee_c4` counts as accepted only with `XTNL_OVERSEE_STOP_AT_ENTRY=1`.
* `XTNL_MD_ENABLED=1` and `redis-md` running (`marketdata.md` "Enable"), then `XTNL_ARM_ENABLED=1`.
* Every go-live gate wired or explicitly accepted ("Go-live gates" below): `$OPS arm gates --env-file
  /etc/xtnl/engine.env` exits 0 (PC: `$OPS arm gates` in a new shell, which reads the Windows User environment).
* Secrets (design §C2, never printed): `XTNL_ARM_CONFIG_DSN` (Supabase reader), the ARM channel credentials
  `XTNL_ARM_SMTP_USERNAME`, `XTNL_ARM_SMTP_PASSWORD` or `XTNL_ARM_SES_*`, `XTNL_ARM_PUSH_TOKEN`, `XTNL_ARM_PUSH_USER`
  (a separate account from Oversee's channels; design PR4: ntfy with a token, or Pushover, so an ntfy push without
  `XTNL_ARM_PUSH_TOKEN` leaves the ARM idle with "ARM not started"). AWS: the Secrets Manager secret. PC: the Windows
  User environment.
* The Supabase project provisioned (`deploy/supabase/arm_schema.sql`, self-check) and a document accepted locally.

```
cd /opt/xtnl/order-server
OPS=".venv/bin/python -m src.ops"
$OPS arm config show                     # the accepted / installed document
$OPS arm config show --pending           # reads Supabase: row diff, validation, admission, registry
$OPS arm config accept <hash> --operator <you> --reason "<ticket>"
$OPS arm show --all                      # SM11 state, pins, cursor of every position; no level
$OPS arm show <adapter:position>         # one position
$OPS arm decisions <adapter:position>    # outcome codes per bar; no level or R value
$OPS arm failed show                     # open FAILED episodes, reason, probe kind, last probe result (--all: cleared too)
```

## Checks

* `$OPS arm show`: armed positions in `ARMED` / `TRACKING` / `ADVICE_OUTSTANDING`; none `FAILED` unless a ping said so.
* `arm_status` (the `arm-status` loop's row): `running` true on the leader, `paused` false, failed scopes 0.
* Metrics: `xtnl_arm_failed_scopes` 0, `xtnl_arm_paused` 0, `xtnl_arm_positions{view="overdue"}` 0,
  `xtnl_arm_oldest_undelivered_notice_age_seconds` small or absent.
* `$OPS arm notify-test --operator <you> --reason "<ticket>"` queues one TEST notice per ARM channel; both arrive.
* `$OPS arm registry check --against-db` exits 0 (every pinned version is in this build).
* Ingestion and Oversee unchanged (`xtnl_risksweep_oldest_pass_age_seconds`, `xtnl_ingest_events_total`).

## FAILED: find the cause, run the first command, clear

`$OPS arm failed show` lists each open episode with its scope (`SERIES`, `ADAPTER`, `STRATEGY_VERSION`, `POSITION`, `ARM`), reason,
**probe kind** and the last probe result. A clear is an audited request; the leader first runs the probe of the
**recorded cause** and clears only when it passes (design §B14 Clearing):

```
$OPS arm failed clear <SCOPE_KIND:scope_id> --operator <you> --reason "<ticket>: <what was fixed>"
```

A refused clear answers `cause_present_<check>[_first_<command>]`: run the named request first, then clear again.

| Probe kind | Reasons | Passes when | Run first |
|---|---|---|---|
| `series_reread` | `invalid_bar`, `source_mismatch`, `refetch_failed` | a re-read of the series' rollback set passes every hard check and the tick cross-check | `md backfill <series> --from-ms --to-ms` for the bad range (`arm replay --request-backfill` files it per run) |
| `fresh_bar` | `unresolved` | one fresh bar read reaches finality within `post_grace` | none: wait for the broker's bars (`md bars <series>`) |
| `indicator_rebuild` | `indicator_faulted` | `md_rebuild` has completed and every indicator on the series is warm | `md rebuild <indicator_id>` (answer `..._first_md_rebuild`) |
| `span_write` | `stuck_frontier` (a POSITION scope `adapter:position` when fewer than `XTNL_ARM_FAIL_FRONTIER_POSITIONS_N` positions are stuck, else ARM) | the failed span write of the recorded positions, re-run by the BarClock on the ARM pool, fenced, in a transaction it rolls back, would commit (refusals `span_write_failed`, `span_row_changed` (retry), `span_already_decided`, `span_rows_unreadable`) | for a POSITION scope: `arm failed show` names the row; fix or `arm disarm` it; for ARM: restore the ARM pool / PostgreSQL first (`journalctl -u xtnl-engine`) |
| `strategy_conformance` | `strategy_version` | the conformance checks plus one successful job of that version | `arm repin <adapter:position>` to a working version, or deploy a fixed build (`arm registry check --against-db`) |
| `adapter_series` | ADAPTER `series_failed` | every FAILED series of the adapter passes its own probe | clear the adapter's SERIES scopes first |
| `arm_global` | every other ARM reason (`redis-md` down, ARM pool, task circuits, bootstrap failures, `restart_required`: more hung strategy threads than the job runner may abandon; restart the engine first, FAILED survives the restart; `unclean_restarts` (S38f): `XTNL_ARM_FAIL_RESTARTS_N` unclean engine restarts or takeovers within `XTNL_ARM_FAIL_RESTART_WINDOW_S` while positions are armed: find what keeps crashing or fencing the engine first, `journalctl -u xtnl-engine` / the PC's daily log, then clear; the clear moves the restart watermark; a crash loop trips SAFE_HOLD first (3 abnormal ends in 15 min), and after the SAFE_HOLD `resume()` this FAILED follows if those crashes still reach the limit: the resume does not clear the ARM) | no more strategy job threads hung than the job runner may abandon (else `threads_hung`: restart the engine), no task circuit open, a fenced test write on the ARM pool, and a **trial bootstrap** of `redis-md` | `task reset <task>` for an open circuit (answer `..._first_reset_task`); `md writer-reset` for a failed trial bootstrap (answer `..._first_md_writer_reset`); `marketdata.md` "AOF aside" when `redis-md` will not load |

Every probe kind is wired (plan S38e: `series_reread` and `fresh_bar` read the broker through md-bars, also while
the series is FAILED; `strategy_conformance` runs the pinned class's declaration checks as one job on the version's
lane). Their refusal checks: `series_reread`: `invalid_bar`, `source_mismatch`, `no_bars` (no closed bar in the
rollback window: retry once the market trades), `read_failed`, `broker_unavailable`, `unsupported`, `series_unknown`;
`fresh_bar`: `not_final` / `no_bar` (no final bar within `post_grace`: retry), `market_closed` (retry after the open),
`invalid_bar`, `read_failed`, `broker_unavailable`; `strategy_conformance`: `version_unknown` (answer
`..._first_arm_repin`), `job_lane_hung`, `job_timeout`, `job_raised`, `executor_hung`, `conformance`.

Other refusals: `probe_not_wired` (a probe port missing from the wiring, which a build with S38e never has, and the
ARM-global probe before the graph has bound it: the clear is refused, fail closed; retry), `probe_timeout` / `probe_error` (retry the clear),
`fault_during_probe` (a new fault arrived during the probe: fix it, then clear), `circuit_open`, `arm_pool`,
`trial_bootstrap`, `threads_hung` (a `restart_required` episode while hung threads remain: restart the engine first),
`indicator_not_warm` (wait for warm-up), `indicator_not_tracked` / `indicator_unknown`.

The requests a refused clear may name (all allowed while FAILED, design §B13):

```
$OPS md backfill <series> --from-ms 1763020800000 --to-ms 1763021700000 --operator <you> --reason "<ticket>"
$OPS md rebuild <indicator_id> --operator <you> --reason "<ticket>"
$OPS md writer-reset --operator <you> --reason "<ticket>"
$OPS md rehash --operator <you> --reason "<ticket>"
$OPS task reset <md-writer|md-bars|md-indicators|arm|arm-relay|arm-config|arm-tracker|arm-status> --operator <you> --reason "<ticket>"
$OPS arm repin <adapter:position> --operator <you> --reason "<ticket>"   # optional: --strategy CODE --strategy-version N
```

After a clear the positions return to the SM11 state they had before FAILED (`prior_state`); advice issued for them
since the first counted fault must not be acted on (the ping says since when).

## Configuration

The configuration lives in Supabase (`arm` schema, read as `xtnl_arm_reader`) and is never trusted until accepted
locally: `arm config show --pending` shows the row diff, validation, admission and registry check; `arm config accept
<hash>` accepts exactly that document. Supabase changes never alter or end an armed position.

## Pause, resume, disarm

```
$OPS arm pause --operator <you> --reason "<ticket>"                    # no decision until resume (--drain-notices: also finish queued notices)
$OPS arm resume --operator <you> --reason "<ticket>"                    # never clears FAILED
$OPS arm disarm <adapter:position> --operator <you> --reason "<ticket>" # stop managing one position
```

## Go-live gates

With `XTNL_ARM_ENABLED=1` the ARM starts only when every go-live gate is **wired** (built and tested) or **accepted**
by you in `XTNL_ARM_GOLIVE_ACCEPT` (comma-separated gate ids, a local setting; plan S38e, design §0b). Otherwise it
stays idle for the term with one `ARM not started` page naming the open gates; market data still runs, ingestion and
Oversee are untouched. Check before enabling (offline, no database):

```
$OPS arm gates --env-file /etc/xtnl/engine.env   # AWS: the setting the engine reads at start
$OPS arm gates                                    # PC: this shell's environment (open a new shell after setting it)
# every gate, its state, accepted or not; exit 3 while enabling would be refused
```

The report's `settings_source` names what was read: `env_file:<path>` or `process_environment`. Without
`--env-file` the verdict is this shell's environment, which can differ from what the engine reads at start.

Accept a gate only after reading its row in design §0b "Go-live gates" (what is not built, and what that risks). S39
(StoneX broker bars) is built and closed `pr3_finality`; StoneX joins `BAR_CAPABLE_ADAPTERS` (so `arm config accept`
takes StoneX-scoped assignments) only once every other gate is wired or recorded `accepted` in
`src/riskmgr/golive.py` (the `arm gates` report's `s39_waits_for` and `bar_capable_adapters`), not in the env file.
For `pr3_stop_children`, run `.venv/Scripts/python.exe -m src.adapters.stonex.probe_bars --check-stops` (your own
shell, `STONE_X_*` set, the engine stopped) while a position with a stop is open; the report lists order ids only.

## Enable / disable (AWS)

Enable: after `marketdata.md` "Enable" and `$OPS arm gates --env-file /etc/xtnl/engine.env` exiting 0, set `XTNL_ARM_ENABLED=1` in `/etc/xtnl/engine.env` on both nodes, restart the
standby, switch over, restart the other node; then the checks above. Disable: `XTNL_ARM_ENABLED=0` the same way (or
`arm pause` at once while the restart is scheduled). PC: the same flag in `deploy/env/local.env` and an engine restart.

## Rollback

`arm pause` stops all advice at once; `XTNL_ARM_ENABLED=0` and an engine restart remove the ARM tasks entirely (no ARM
task, no ARM row written; the uncommitted S07 Oversee stop rule, design §0 C4, is not behind this flag). Stored decisions and FAILED episodes stay in
PostgreSQL for audit (`arm_decisions` is never exported or detached). A wrongly accepted configuration is rolled back
by accepting the previous document's hash again (`arm config show` lists it). Nothing at the broker needs rolling back:
the ARM never touched it.
