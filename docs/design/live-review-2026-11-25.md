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
| StoneX children of a working entry map `PENDING` at the entry's broker time in every REST shape (IfDone, direct, own row), never untimed `WORKING` | `dto-contracts.md` versions (a stale REST read never overwrites a newer stream update); `adapter-isolation.md` accepted deviation of 2026-11-21, widened to the shapes seen live |
| `fetch_history` leaves untimed working rows to the snapshot | `operations.md` §17 (an untimed history row is never labelled with the watermark); the snapshot already reconciles working orders |
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
| The headline says "break-even" only when the proposed stop equals entry | §20 Q6 (break-even at entry, or plus a buffer) |
| A push body over the channel limit is cut on a line boundary with a "see email" marker | §B8 (email and push carry the same notice); D-decisions on channels |
| `stop_kind_inconsistent` alerts name the condition and its action | `oversee-rules.md` §5 / §6 (UNKNOWN `stop_kind_inconsistent`) |
| `at_target` is judged against the unrounded target (doc wording aligned) | §B6.3 (7a) already counts a handle exactly at an off-grid E with offset 0, which only the unrounded target does; broker handle levels are on-grid, where the two are equal |
| New tests pin: R < 0 clamp, rounding toward profit on 7 grids, gap from the rounded target, Q5 exit side in the gap, every handle moved, the Q2 one-bar delay end to end, Q7 bucket boundary, conformance vectors checking the target, BROKER_AHEAD | Q1, Q2, Q5, Q7, §B6.3, §B6.4, `riskmgr.md` §6 (vectors) |

## Carried to round 2

- **Feed client socket timeout.** Wire a dedicated feed Redis client with socket-timeout headroom over the `BLOCK` (`src/cache/client.py` `feed_redis_from` is ready) into `src/runtime/session.py`. Requirement: `oversee-rules.md` §1, `operations.md` §3.
- **Post-T stop versions in SQL.** Add the post-T counting-version predicate to the stop-version SQL reader (`src/db`), so the `S_at_T` rule above also holds for reads from PostgreSQL. Requirement: §B6.3.
- **Notice ordering and wording.** Order ARM notice items as ADJUST first. Make the COMPLETE text say "break-even" only at entry. Requirement: §B8, Q6.
- **Child ids in alerts.** Carry the inconsistent child ids into the `stop_kind_inconsistent` alert. Requirement: `oversee-rules.md` §6 ("naming only the inconsistent child ids"). *Round 2: done for transition alerts.* The ids travel in `Transition.detail` and never in the reason, so the alert body reads `stop_kind_inconsistent: stop child <ids>`. A re-page still names only the reason, because the stored `halt_breaches` row has no detail column.
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
| The COMPLETE text says "break-even" only when the target is the entry | §20 Q6 |
| `stop_kind_inconsistent` alerts name only the inconsistent child ids | `oversee-rules.md` §6 |
| Never-activated unlisted rows resolve silently | `oversee-rules.md` §2 Confirm |
| **Reverted:** round 1's "the missing-stop grace holds an active state". The unchanged §2 says a capped UNKNOWN state resolves once a known result is below SOFT | `oversee-rules.md` §2 UNKNOWN (requirement wins) |
| Runbook: a Windows "Shut down" with Fast Startup is a log-off plus a kernel hibernate, which kills the engine; it is not a sleep | `single-pc-autostart.md` "What happens on reboot, sleep, Windows update" |

The engine's two silences were not sleeps:
- **2026-11-24:** a Windows shutdown with Fast Startup ended the logon session at 02:08. The task restarted at the next logon.
- **2026-11-23:** the whole process tree was ended at about 01:38 local while the PC was awake. The cause is not proven. It coincided with a full `pytest tests/` run from a Claude session and with an Office update's Restart Manager.

Nothing off the PC pages a dead engine until the dead-man receiver (W6) runs.

## Round 3 (to do)

- **StoneX child shapes.** Revert round 1's widening of the 2026-11-21 accepted deviation (own-row and directly nested children mapped `PENDING`). The owner accepted the IfDone shape only, and `IMPLEMENTATION_PLAN` 8.1 says the adapter never invents a status.
- **History filter.** Revert round 1's filter that left untimed working rows out of `fetch_history`. `adapter-isolation.md` §4 lists every order whose state changed in the window. `operations.md` §17 and `oversee-rules.md` §8 say an untimed row is skipped, reported, and leaves the catch-up incomplete. The page now names the ids and the window, as round 1 made it.
- **Own-pending drain timeout.** Make the own-pending drain read timeout-tolerant (`src/cache/feed_stream.py`).
- **Market-sessions lost update.** Make `market-sessions accept` safe against the engine's concurrent schedule save (`src/db/market_sessions.py`).
- **Hard-exit backstop.** Arm the `hard_exit` backstop timer before logging (`src/runtime/exits.py`).
- **ARM notice order.** Order ARM notice items ADJUST first, only if a requirement asks for it (the plain-text baselines are pinned).
- **Regression review of round 2.**

## Operator items (not code)

- **PC sleep.** The PC slept at night (2026-11-23 and 2026-11-24). `docs/runbooks/single-pc-autostart.md` prerequisite 5 says: when plugged in, sleep after **Never**.
- **Calendar review.** Check the StoneX EURUSD / AUDUSD default FX hours and holidays, then run `python -m src.ops market-sessions accept`.
- **ARM switched off.** The ARM is off on the live server (`XTNL_MD_ENABLED` off). Turning it on needs the go-live gates the owner holds in `src/riskmgr/golive.py`.
- **Live-trading-server skill.** The `/live-trading-server` skill cannot be invoked by an agent (it is reserved for the user).
