# Proposed change (S38d): md-indicators' own executor in the lockout copy and the clean-stop rule

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it:
the pytest lockout scan of the real tree (`tests/riskmgr/test_lockout.py`, over `src/riskmgr/lockout.py`) already
gates the change, and the hook parity test stays skipped with "hook update pending" until the S11 proposal is applied.

## Why

Round 13 found that `md-indicators` ran its jobs on the event loop's **shared default executor**
(`asyncio.to_thread`), which ingestion also uses (adapter connects, the DLQ spill write, the node controller's
health, campaign and lease release). Design §B4 rules that executor out for hang-prone jobs. Plan stage S38d gives
`md-indicators` its own bounded executor:

* `src/marketdata/indicators/engine.py` now imports `concurrent.futures.ThreadPoolExecutor` and uses
  `asyncio.wrap_future` (one `ThreadPoolExecutor(max_workers=3, thread_name_prefix="md-ind")` per term);
* `src/riskmgr/lockout.py` widens its allow-list for **that one file only** (`FUTURES_FILES`): `concurrent.futures`
  (`ThreadPoolExecutor` only) and `asyncio.wrap_future`. Every other `src/marketdata` file still refuses both;
* `MdIndicators.close()` shuts the executor down without waiting and returns True while a worker is hung; the
  runtime's `md-indicators` loop (`src/runtime/optional_plan.py::OptionalRuntime._indicators_loop`) then ends the
  process through the engine's hard exit, exactly as for a hung ARM job thread (S22).

Nothing here touches a sender, a network client, `FORBIDDEN_METHOD_NAMES`, the `ReadOnlyTransport` deny-list or the
hook's `FORBIDDEN_PATTERNS`.

## Diff 1: the lockout text of the S11 proposal (the hook copy and `.claude/rules/marketdata.md`)

Apply together with `S11-lockout-scanner.md` (it amends that proposal's §2 Imports bullet):

```diff
-     `src.riskmgr`, `src.marketdata`, `src.armconfig` and `concurrent.futures` (`ThreadPoolExecutor` only);
-     `src/marketdata` also `src.marketdata` and `zlib` (`crc32` only), and `redis` only under `src/marketdata/store`.
+     `src.riskmgr`, `src.marketdata`, `src.armconfig` and `concurrent.futures` (`ThreadPoolExecutor` only);
+     `src/marketdata` also `src.marketdata` and `zlib` (`crc32` only), and `redis` only under `src/marketdata/store`;
+     `src/marketdata/indicators/engine.py` alone also `concurrent.futures` (`ThreadPoolExecutor` only) and
+     `asyncio.wrap_future` (md-indicators' own bounded executor, plan S38d; `FUTURES_FILES` in `lockout.py`).
```

and in the hook's copy of the scanner (`.claude/hooks/pre_tool_guard.py`, once S11's `LOCKOUT_SCOPE` part is
applied), the same `FUTURES_FILES` set and the two checks that use it, mirrored from `src/riskmgr/lockout.py`
(`_member_limits` and `_import_refusal`).

## Diff 2: `.claude/rules/operations.md` (extends the S22 proposal)

```diff
@@ §4 (DRAINING)
-   - **Hung ARM job threads:** a clean stop (after DRAINING, or a non-leader's stop) closes the ARM job runner (`JobRunner.close()`, `src/riskmgr/jobs.py`). When it reports a job thread still hung (Python cannot kill one, and the interpreter joins executor threads at exit), the process ends through `hard_exit` (`os._exit`) after the `engine_runs` stop row is written, never a normal interpreter exit, so one hung strategy cannot hold the node until systemd's stop timeout.
+   - **Hung ARM job and indicator threads:** a clean stop (after DRAINING, or a non-leader's stop) closes the ARM job runner (`JobRunner.close()`, `src/riskmgr/jobs.py`) and md-indicators' own executor (`MdIndicators.close()`, `src/marketdata/indicators/engine.py`). When either reports a thread still hung (Python cannot kill one, and the interpreter joins executor threads at exit), the process ends through `hard_exit` (`os._exit`) after the `engine_runs` stop row is written, never a normal interpreter exit, so one hung strategy or indicator cannot hold the node until systemd's stop timeout. Indicator jobs never run on the loop's default executor.
```
