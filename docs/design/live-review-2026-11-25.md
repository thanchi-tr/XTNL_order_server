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
| A stop removed under an active missing-stop state holds the episode through the grace (no "resolved / No action" alert) | `oversee-rules.md` §2 (release only with known inputs below the level), §6 `MissingStopLossRule` grace |
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
- **Child ids in alerts.** Carry the inconsistent child ids into the `stop_kind_inconsistent` alert. Requirement: `oversee-rules.md` §6 ("naming only the inconsistent child ids").
- **Silent engine after wake.** Explain why the engine wrote nothing after the 02:08 wake on 2026-11-24. Requirement: `operations.md` §17 sleep detection, resume catch-up, and hard exit plus restart by `run-local.ps1`.

## Operator items (not code)

- **PC sleep.** The PC slept at night (2026-11-23 and 2026-11-24). `docs/runbooks/single-pc-autostart.md` prerequisite 5 says: when plugged in, sleep after **Never**.
- **Calendar review.** Check the StoneX EURUSD / AUDUSD default FX hours and holidays, then run `python -m src.ops market-sessions accept`.
- **ARM switched off.** The ARM is off on the live server (`XTNL_MD_ENABLED` off). Turning it on needs the go-live gates the owner holds in `src/riskmgr/golive.py`.
- **Live-trading-server skill.** The `/live-trading-server` skill cannot be invoked by an agent (it is reserved for the user).
