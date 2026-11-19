# Proposed change (S39b): `.claude/rules/operations.md` and `adapter-isolation.md`, the six go-live gates

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.
It sits on top of the pending S11 (`riskmgr.md`), S37, S38e, S38f, S38g and S39 proposals; approve them together.

## Why

Plan stage S39b wired the six engineering go-live gates (`src/riskmgr/golive.py`). Two of them add a new caller of
an existing read-only path, and the rules name every caller of those paths today:

1. **`entry_time_source`.** The arm-tracker upgrades an `ESTIMATED` entry time to `BROKER` from the broker's trade
   history (`IngestionOrchestrator.history_execution_time`, StoneX `ExecutedDateTimeUtc`). It reads
   `fetch_history(since, until)` at `RestPriority.RECONCILE` on the adapter's one REST budget: a new, bounded caller
   of the catch-up read (at most 5 reads per armed position per term, one a minute, only until bar 0's
   `decision_cap`). Nothing is published, versioned or written; only the entry order's opening fill times are used.
   `operations.md` §15 lists the users of the reconcile class and `adapter-isolation.md` §4 names `fetch_history` as
   the single-PC catch-up read.
2. **`receipt_gap_feed`.** The orchestrator gained a per-adapter frame hook (`on_stream_frame`): on every
   prices-stream HEARTBEAT while that subscription is SUBSCRIBED and not lost, it calls the market-data layer's
   receipt-gap detector (pure, never awaited, errors logged and never on the socket path), like the existing
   `on_price_tick` / `on_price_subscription` hooks. `dto-contracts.md` §4 / §6 are unaffected (it publishes
   nothing); the note keeps the hook list in `operations.md` complete.

The other four gates (`uptime_suspend`, `replay_rerun`, `conformance_probe_partial`, `corrections_restart`) change
`src/riskmgr`, `src/ops` and `src/db` read paths only and need no rules text. Nothing here narrows
`FORBIDDEN_METHOD_NAMES`, the `ReadOnlyTransport` deny-list or the hook's `FORBIDDEN_PATTERNS`; no lockout list was
widened (the job runner's suspend handling uses `asyncio.Event` and `time`, already allowed).

## Diff

```diff
--- a/.claude/rules/operations.md
+++ b/.claude/rules/operations.md
@@ §15 Rate limits
-15. **Rate limits:** one REST token bucket per adapter is shared by snapshots, resync re-reads, DLQ checks, cash-flow fetches and the 60 s account poll, with priority reconcile > on-demand cash-flow fetches requested by Oversee or an operator (relayed by the orchestrator, at most once a minute per account) > DLQ > poll.
+15. **Rate limits:** one REST token bucket per adapter is shared by snapshots, resync re-reads, DLQ checks, cash-flow fetches and the 60 s account poll, with priority reconcile > on-demand cash-flow fetches requested by Oversee or an operator (relayed by the orchestrator, at most once a minute per account) > DLQ > poll. The ARM's entry-time history read (`IngestionOrchestrator.history_execution_time`, plan S39b: one `fetch_history` per armed position whose entry time is still ESTIMATED, at most once a minute and 5 times per term, only until bar 0's decision cap) runs in the reconcile class; it publishes and writes nothing.
@@ §17 Single-PC mode, Sleep detection
-    - **Sleep detection:** a 5 s tick compares wall-clock and monotonic deltas; ...
+    - **Sleep detection:** a 5 s tick compares wall-clock and monotonic deltas; ... With the ARM on, the suspend signal also reaches every strategy job (`JobRunner.note_suspended`: a job whose run spans the sleep gets a fresh awake cap and is never counted as a timeout or a strategy fault) and the market-data layer's receipt-gap watch, which the orchestrator feeds with every prices-stream heartbeat (`on_stream_frame`, plan S39b).

--- a/.claude/rules/adapter-isolation.md
+++ b/.claude/rules/adapter-isolation.md
@@ §4 Read-Only Interface Lock, fetch_history
-   - `fetch_history(since_ms, until_ms) -> BrokerHistory` (single-PC mode, `operations.md` §17): ...
+   - `fetch_history(since_ms, until_ms) -> BrokerHistory` (single-PC mode, `operations.md` §17; since plan S39b also the ARM's entry-time lookup, `IngestionOrchestrator.history_execution_time`, read-only, at reconcile priority): ...
```

The `...` in the diff stands for the unchanged text of the same line.
