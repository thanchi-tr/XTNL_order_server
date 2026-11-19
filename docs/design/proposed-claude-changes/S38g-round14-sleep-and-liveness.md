# Proposed change (S38g): PC sleep fences and the ARM liveness pages in the rules

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it:
the S38g tests (`tests/runtime/test_sleep_path.py`, `tests/riskmgr/test_liveness.py`, `tests/riskmgr/test_round14.py`,
`tests/riskmgr/test_pipeline.py`, `tests/riskmgr/test_stopbasis.py`, `tests/db/test_arm_versions.py`,
`tests/db/test_arm_schema_no_market_data.py`) read only `src/`, `deploy/`, `docs/` and `tests/`. Nothing here narrows a
read-only rule, `FORBIDDEN_METHOD_NAMES`, the `ReadOnlyTransport` deny-list or the hook's `FORBIDDEN_PATTERNS`.

## Why

Review round 14 (plan stage S38g, user: "Yes, fix the 10") found that on the single PC almost every real sleep is
longer than the 12 s fence, because `time.monotonic()` keeps counting through a Windows sleep: the wake is a fence
hard exit and a restart, a path operations §17 mentions only in passing. The build now names and records such a
fence, so the ARM can tell a sleep's restart from a crash, and adds the liveness pages design §B8 promised. Two rule
texts describe the old behaviour.

## 1. `.claude/rules/operations.md`

§3 "Self-fencing", after the bullet that starts "At **12 s** without renewal": add

> A fence in a keeper iteration in which the machine slept more than 5 s since the previous iteration is a **sleep
> fence**: the process was suspended. The keeper measures the sleep with a clock pair of which one counts the
> machine's sleep and the other leaves it out (Windows `GetTickCount64` / `QueryUnbiasedInterruptTime`, Linux
> `CLOCK_BOOTTIME` / `CLOCK_MONOTONIC`), so a stalled process (a GIL hog, a VM pause, swap thrashing, a wall-clock
> step) is never taken for a sleep: its fence is ordinary and named `process stalled N s`. A sleep fence's reason
> says so (`process suspended N s`) and, just before the hard exit, the keeper appends the fence time to the
> node-local `sleep_fences.jsonl` next to the spill file (`src/runtime/sleeplog.py`; times only, kept 24 h). The
> fence itself is unchanged.

§17 "Single-PC mode", the **Sleep detection** bullet: append

> On Windows the monotonic clock keeps counting through a sleep, so a sleep longer than the fence is the usual case:
> the keeper fences on wake (a sleep fence, §3) and `run-local.ps1` restarts the engine. The `md-writer` and the
> BarClock also detect a suspend themselves (both clocks moved more than 10 s between two 250 ms passes) and take the
> uptime tracker's signal (`EngineSession.add_suspend_listener`): the TICK buckets the sleep covered are `blind` and
> the BarClock holds one pass. The ARM does not count a sleep fence's own restart toward `unclean_restarts` (the first
> `lost` / `init_failed` term end within 15 min after a recorded sleep fence) and pages each sleep restart once
> ("ARM: engine restarted after a PC sleep").

## 2. `.claude/rules/operations.md` §9 (dead-man) — the ARM optional field

In the push dead-man bullet, after the field list: add

> With the ARM running the beat also carries `arm_overdue_positions`: armed positions (ARMED, TRACKING,
> ADVICE_OUTSTANDING; not FAILED, no pause) with more than 2 OPEN bars past their decision cap undecided, or whose
> symbol has no loaded schedule (`src/riskmgr/liveness.py`); the receiver pages above 0. Locally the `arm-status` loop
> pages the same condition, a BarClock with no completed pass for 60 s, an arm-tracker with none for 120 s and an
> unsent ARM notice older than 15 min, each once per episode.
