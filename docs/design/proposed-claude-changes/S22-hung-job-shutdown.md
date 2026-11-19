# Proposed change (S22): `.claude/rules/operations.md`, a hung ARM job thread and the clean stop

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.

## Why

The ARM job runner (`src/riskmgr/jobs.py`, plan S22) gives each pinned strategy version its own
`ThreadPoolExecutor(max_workers=1)`. Python cannot kill a hung thread, and `concurrent.futures` joins every executor
worker at interpreter exit (its atexit hook). So a strategy thread hung inside a job would keep a node from exiting
after a clean stop (SIGTERM / SIGINT, planned switchover, DRAINING that finished in time) until systemd's stop
timeout sends SIGKILL. The fence and the DRAINING deadline are not affected: they already end the process with
`os._exit` (`src/runtime/exits.py`), which runs no atexit hook.

A daemon worker thread would avoid the join, but `threading` is refused in the lockout scope
(`src/riskmgr/lockout.py`, `EXTRA_REFUSED_MODULE_NAMES`), and widening that list is a reviewed change. S22 keeps the
lockout as it is: `JobRunner.close()` returns True when a job thread is still hung, and the stop path wired in S27 /
S31 must then end the process through `hard_exit`. This rule records that contract where the operations rules live.

## Diff

```diff
--- a/.claude/rules/operations.md
+++ b/.claude/rules/operations.md
@@ §4 (DRAINING)
    - **DRAINING** (planned switchover with a named target, SIGTERM, or abdication): stop intake, flush the DlqSpooler (to PostgreSQL, or to the local spill file if PostgreSQL does not answer within 2 s), write this term's `end_reason`, close sockets, release the lease. Redis is not drained, because the next leader never reads it.
+   - **Hung ARM job threads:** a clean stop (after DRAINING, or a non-leader's stop) closes the ARM job runner (`JobRunner.close()`, `src/riskmgr/jobs.py`). When it reports a job thread still hung (Python cannot kill one, and the interpreter joins executor threads at exit), the process ends through `hard_exit` (`os._exit`) after the `engine_runs` stop row is written, never a normal interpreter exit, so one hung strategy cannot hold the node until systemd's stop timeout.
```
