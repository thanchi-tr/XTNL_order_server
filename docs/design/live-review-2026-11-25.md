# Live review fixes, 2026-11-25

These are fixes from the review of the running server's live data and of the SL edge recommendation path.

**Rule for every design choice:** it must come from an existing requirement, and the row names which one. When no requirement decides the choice, the docs' default or recommended path is used, and the row says so. A fix that would have changed a requirement was reverted, not merged.

## Round 1

Round 1 produced 38 findings. Three skeptics checked each one, and 31 were confirmed.

The final independent differential check of the SL edge recommendation ran more than 300,000 random and boundary cases against a fresh exact-arithmetic oracle written from the spec. It found **0 disagreements**. The spec it was written from:
- Q1 (a)
- Q2: 1.318 × ATR(14), including bar 0
- Q5, Q7, Q11
- C1 `pre_basis`
- REOPEN, release
- rounding toward profit

| Fix | Requirement it follows |
|---|---|
| ~~StoneX children of a working entry map `PENDING` in every REST shape~~ **Reverted in round 3** to the owner-accepted IfDone shape only | `adapter-isolation.md` accepted deviation 2026-11-21 (IfDone only); `IMPLEMENTATION_PLAN` 8.1 (the adapter never invents a status) |
| ~~`fetch_history` leaves untimed working rows to the snapshot~~ **Reverted in round 3**: they are offered, skipped as unversioned, and leave the catch-up incomplete, with the ids and window named in the page | `adapter-isolation.md` §4; `operations.md` §17 step 3; `oversee-rules.md` §8 |
| The incomplete catch-up page names the skipped ids and window | `operations.md` §17 (skipped rows are reported); `oversee-rules.md` §8 (`catchup_unverified`) |
| A timed-out feed `XREADGROUP BLOCK` reads as empty instead of killing the feed observer. This consumer's own pending entries are drained at once (XREADGROUP id 0) | `oversee-rules.md` §1 (the observer never waits on a condition), §3 (consumer-group recovery); `operations.md` §3 (a task stays alive across transient waits) |
| RiskSweep passes are scheduled and timed on the monotonic clock | `oversee-rules.md` §2 ("monotonic `sweep_id`"); `operations.md` §17 (a wall-clock step is detected, never trusted) |
| `slow_pass` pages on every pass longer than the interval, latched until a fast pass | `oversee-rules.md` §2, **unchanged**. A proposed debounce (3 slow passes or a gate-threatening one) was reverted because no requirement asks for it |
| Pending or UNKNOWN episodes that end without activating are stored resolved once, with no transition or alert | `oversee-rules.md` §2 "Confirm" |
| ~~A stop removed under an active missing-stop state holds the episode through the grace~~ **Reverted in round 2; needs an owner decision.** It contradicted the unchanged §2 UNKNOWN rule (a capped state "resolves once a known result is below SOFT"), and it needed a §6 text change that has been taken out again. The code is back to plain `Result(PASS, 'stop_grace')`. A cancelled `stop_kind_inconsistent` child resolves the state, and the end of the 2 s grace starts a new `missing_stop_loss` episode | `oversee-rules.md` §2 UNKNOWN and Release (unchanged); the owner's requirement rule |
| The market-session review page is sent once per change, and `reviewed_config` rows silence it. The operator command `market-sessions accept` (audited) writes them | `operations.md` §10 / SM10 (operator-reviewed calendar, `reviewed_config` source in migration 0001); `oversee-rules.md` §4 (operator actions are authenticated and audited) |
| REOPEN drops a stored break-even target when E or S0 is re-pinned, so advice never puts the stop on the loss side of entry | `automated-risk-manager.md` Q1 (a) "straight to break-even"; §B6.3 REOPEN `REUSE_TARGET`, whose target is the break-even of the current E |
| `S_at_T` ignores an untimed stop version that is known to come after T | §B6.3 (`S_at_T` = the stop at the bar end); C1 |
| The Q10 release sentence follows the side ("below" for a long, "above" for a short) | §20 Q10 (a stop moved back toward loss); §B6.3 step (4), made side-neutral |
| `favour / risk` is shown only when its denominator is the stop the trigger tested | §B8 (notices state the decision's own inputs); C5 (no favour figures on PRE) |
| ~~The headline says "break-even" only when the proposed stop equals entry~~ **Reverted in round 4.** §B8 fixes no ADJUST headline text, and §20 Q6 / Q1 (a) call every `break_even_on_risk_close` target "break-even", at the entry and at the entry plus a buffer (as the single §B8 COMPLETE text, round 3). The headline and preheader say "break-even" for that strategy kind again; the preheader still states the level. A different wording for a buffered target would be an owner decision on §B8 / Q6 | §B8, §20 Q6, Q1 (a) |
| A push body over the channel limit is cut on a line boundary with a "see email" marker | §B8 (email and push carry the same notice); D-decisions on channels |
| `stop_kind_inconsistent` alerts name the condition and its action | `oversee-rules.md` §5 / §6 (UNKNOWN `stop_kind_inconsistent`) |
| `at_target` is judged against the unrounded target (doc wording aligned) | §B6.3 (7a) already counts a handle exactly at an off-grid E with offset 0, which only the unrounded target does; broker handle levels are on-grid, where the two are equal |
| New tests pin: R < 0 clamp, rounding toward profit on 7 grids, gap from the rounded target, Q5 exit side in the gap, every handle moved, the Q2 one-bar delay end to end, Q7 bucket boundary, conformance vectors checking the target, BROKER_AHEAD | Q1, Q2, Q5, Q7, §B6.3, §B6.4, `riskmgr.md` §6 (vectors) |

## Carried to round 2

- **Feed client socket timeout.** Wire a dedicated feed Redis client with socket-timeout headroom over the `BLOCK` (`src/cache/client.py` `feed_redis_from` is ready) into `src/runtime/session.py`. Requirement: `oversee-rules.md` §1, `operations.md` §3.
- **Post-T stop versions in SQL.** Add the post-T counting-version predicate to the stop-version SQL reader (`src/db`), so the `S_at_T` rule above also holds for reads from PostgreSQL. Requirement: §B6.3.
- **Notice ordering and wording.** Order ARM notice items as ADJUST first. ~~Make the COMPLETE text say "break-even" only at entry.~~ *Reverted in round 3:* §B8 has a single `COMPLETE` text, and §20 Q6 calls the target "break-even" both at the entry and at the entry plus a buffer. Requirement: §B8, Q6.
- **Child ids in alerts.** Carry the inconsistent child ids into the `stop_kind_inconsistent` alert. Requirement: `oversee-rules.md` §6 ("naming only the inconsistent child ids"). *Round 2: done for transition alerts.* The ids travel in `Transition.detail` and never in the reason, so the alert body reads `stop_kind_inconsistent: stop child <ids>`. *Round 3: done for re-pages too* (`oversee-rules.md` §6 "re-pages", §10). The RiskSweep hands this pass's UNKNOWN detail to the dispatcher (`NotificationDispatcher.stage_repage_details`), so the re-page body reads `stop_kind_inconsistent: stop child <ids> (cap SOFT)`. Nothing is stored: the ids are the current pass's only, never in the reason, the ack match or the episode / dedupe key; a state whose current result is not that UNKNOWN re-pages with the reason alone.
- **Silent engine after wake.** Explain why the engine wrote nothing after the 02:08 wake on 2026-11-24. Requirement: `operations.md` §17 sleep detection, resume catch-up, and hard exit plus restart by `run-local.ps1`.

## Round 2

Round 2 produced 17 new findings, and 14 were confirmed. Verifiers also checked that each fix follows a requirement without changing one.

An independent check then ran 360,000 cases through the real `run_pipeline` (POST, LATE, PRE; 8 price grids; 141k bar-0 cases; 11k cases with favour exactly equal to risk). It found **0 disagreements**. A second, end-to-end check drove the runtime path from BarClock and ArmTracker through the rendered email and push.

| Fix | Requirement it follows |
|---|---|
| The feed observer has its own Redis client, with socket-timeout headroom over the `BLOCK`. The paced stream passes `read_own_pending` through, so the own-pending drain runs in production | `oversee-rules.md` §1, §3; `operations.md` §3 |
| The fence arms an `os._exit` backstop before any I/O, and the keeper's logging no longer blocks on the stderr pipe | `operations.md` §1 / §3 self-fencing, off the event loop (a fenced engine must exit) |
| ARM channels use the rotating secrets source. ARM push needs ntfy with a token, or Pushover | `operations.md` §13 (secrets, rotation); design PR4 (ARM channels) |
| The `S_at_T` post-T predicate is added to the SQL reader, matching the round-1 pure rule | §B6.3 `S_at_T`; C1 |
| A guard-pending bar 0 is decided at its decision cap as `FIRST_BAR_UNKNOWN(timeout)`, not spanned as a FAULT | §B6.5; Q2, Q3, Q7 |
| `answers_final_advice` no longer carries a "loss-side target never counts" clause that §B6.4 round 14 does not contain | §B6.4 round 14 |
| Every ADJUST level is stated at the price increment's precision (REOPEN shows `1.2000`) | §B6.3 step (9) |
| ~~The COMPLETE text says "break-even" only when the target is the entry~~ **Reverted in round 3.** No requirement asks for a second release text: §B8 "Release texts" has one `COMPLETE` text, and §20 Q6 uses "break-even" for the entry and for the entry plus a buffer. Every release renders the §B8 text again. `COMPLETE_TARGET_TEXT`, the `release_at_entry` body key (a stored body that carries it still decodes; the key is ignored) and `SlStrategy.target_is_entry` (not in §B6.1) are removed. The pinned `BreakEvenOnRiskClose` v1 is restored as it was before round 2. A different wording for a buffered target would be an owner decision on §B8 / Q6 | §B8 "Release texts", §20 Q6, §B6.1; `riskmgr.md` §6 (requirement wins) |
| `stop_kind_inconsistent` alerts name only the inconsistent child ids | `oversee-rules.md` §6 |
| Never-activated unlisted rows resolve silently | `oversee-rules.md` §2 Confirm |
| **Reverted:** round 1's "the missing-stop grace holds an active state". The unchanged §2 says a capped UNKNOWN state resolves once a known result is below SOFT | `oversee-rules.md` §2 UNKNOWN (requirement wins) |
| Runbook: a Windows "Shut down" with Fast Startup is a log-off plus a kernel hibernate, which kills the engine; it is not a sleep | `single-pc-autostart.md` "What happens on reboot, sleep, Windows update" |

The engine's two silences were not sleeps:
- **2026-11-24:** a Windows shutdown with Fast Startup ended the logon session at 02:08. The task restarted at the next logon.
- **2026-11-23:** the whole process tree was ended at about 01:38 local while the PC was awake. The cause is not proven. It coincided with a full `pytest tests/` run from a Claude session and with an Office update's Restart Manager.

Nothing off the PC pages a dead engine until the dead-man receiver (W6) runs.

## Round 3

Round 3 produced 3 new findings. Two were confirmed, and the 5 carried items were also fixed.

A third independent check rendered about 31,000 decisions from the real `run_pipeline`, each in 10 renderings (text, both push limits, both email themes), plus 3,000 multi-item notices. In every one, the stated level equals the oracle's at the increment's precision, the side words match, pips from entry are right, there is no favour figure on PRE, push equals email, and no account values appear. The exception is the increment leak listed under round 4. A fresh exact-arithmetic run of `run_pipeline` also agreed, apart from one low reading of `pre_basis`, also listed under round 4.

| Fix | Requirement it follows |
|---|---|
| The StoneX child mapping is restored to the IfDone shape only, and `fetch_history` offers untimed working rows again (both round-1 reverts) | `adapter-isolation.md` 2026-11-21 deviation and §4; `IMPLEMENTATION_PLAN` 8.1; `operations.md` §17; `oversee-rules.md` §8 |
| The own-pending drain read is timeout-tolerant | `oversee-rules.md` §1, §3; `operations.md` §3 |
| `market-sessions accept` can no longer be lost to the engine's concurrent schedule save (re-merged under `MARKET_SESSIONS_LOCK`) | `operations.md` §10 Review / Accept |
| `hard_exit` arms an `os._exit` backstop before any logging | `operations.md` §1 / §3 |
| COMPLETE always uses the single §B8 release text. Round 2's second text was reverted | §B8 release texts |
| `stop_kind_inconsistent` re-pages also name the inconsistent child ids | `oversee-rules.md` §6 |

## Round 4

Round 4 had 3 open items and 10 new items from a regression review of round 3 (which found 0 defects) and from a completeness critic over the SL path (clause map plus a stdlib line-coverage trace of tests/riskmgr and tests/notifications). Each item went through 3 skeptics, and 10 were confirmed and fixed.

| Fix | Requirement it follows |
|---|---|
| ADJUST headline: "break-even" for every `break_even_on_risk_close` target again (round-1 change reverted) | §B8 notice texts (single wording); §20 Q6 |
| PRE_NOT_CONFIRMED no longer stores a PRE basis that the bar-end `S_at_T` has already loosened past | §B6.3 C1 (cleared once the current S, = bar-end `S_at_T`, is farther from entry) |
| A REOPEN that does not end the re-check (PRICE_THROUGH_LEVEL) is retried; the last result is written when the re-check ends | §B6.3 REOPEN (retryable outcomes) |
| The xtnl ARM email shows the bar-0 conditional PRE label, "valid until", the DELAYED delay and every decision caveat | §B6.5 bar-0 PRE label; §B5.2 DELAYED; §B6.3 / §B7 caveats; §B8 |
| Tests for duplicate bar-end events, an older-bar CORRECTED item, PRE pre-guards (1)-(3b), the 15m grid / roll caveat / 17:00 New York fills across both 2026 DST changes, and REOPEN across a weekend restart | §B6.6, §A13, §B6.3, Q7, Q14, SM10, C3 |

**Decided from the requirement text: the C1 tie.** When `S_at_T` equals `S_pre` exactly, the PRE basis is kept, which is the as-built behaviour. The literal clear sentence ("at or farther") and C1's purpose sentence ("a move made on an unconfirmed heads-up never makes a later bar's test easier") conflict at the tie. Clearing at the tie would let a heads-up move made after T make the next test easier, which the purpose sentence forbids. The pipeline docstring follows the purpose sentence.

## Round 5 (to do)

- **Increment leak.** The xtnl note "Moves the stop N pips/points" prints the price increment when the move is one increment. `riskmgr.md` §5 forbids any increment or pip size in a notice; the owner's exception covers only each stop's distance from entry in pips. Drop the numeric move size.
- **Current stop with `show_levels` off.** Both HTML themes show the current stop when `show_levels` is off. Q13's built default is "proposed stop and R only", and §B8 says "entry and current stop only if Q13".
- **NO_BROKER_BAR basis.** NO_BROKER_BAR folds this bar's PRE levels into `pre_basis` without the C1 clear that round 4 added to every other non-confirming bar end (`arm-implementation-plan.md` S38c fix (a): "as every other non-confirming bar end").
- **Latch on, CORRECTED re-run.** With `latch = on` (not the Q11 default), a CORRECTED re-run of the bar that recorded the trigger reads its own committed state, so a withdrawn trigger keeps re-proposing. Check whether §B6.6 ("re-run on the recorded order state, `commit_state` re-applied") decides the state to re-run on.
- **REOPEN clean-spread retry.** Needs the Oversee clean-spread threshold injected in `src/runtime/optional_plan.py` (§B6.3 REOPEN).
- **Regression review of round 4.**

## Operator items (not code)

- **PC sleep.** The PC slept at night (2026-11-23 and 2026-11-24). `docs/runbooks/single-pc-autostart.md` prerequisite 5 says: when plugged in, sleep after **Never**.
- **Calendar review.** Check the StoneX EURUSD / AUDUSD default FX hours and holidays, then run `python -m src.ops market-sessions accept`.
- **ARM switched off.** The ARM is off on the live server (`XTNL_MD_ENABLED` off). Turning it on needs the go-live gates the owner holds in `src/riskmgr/golive.py`.
- **Live-trading-server skill.** The `/live-trading-server` skill cannot be invoked by an agent (it is reserved for the user).
