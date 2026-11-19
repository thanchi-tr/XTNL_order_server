# Proposed change (S06): `.claude/rules/operations.md` §15, the `BARS` REST priority

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.

## Why

S06 (design PR11) added `RestPriority.BARS` between `CASH_FLOWS` and `DLQ` in `src/adapters/ratelimit.py`, with a
per-adapter cap on `BARS` reads per minute, a send-time stamp (IN-5) and per-priority outcome counters (R11-M10).
Rule §15 lists the priority order, so without this change it no longer matches the code. Nothing reads at `BARS`
while `XTNL_MD_ENABLED` / `XTNL_ARM_ENABLED` are off, and the order and reserves of the four existing classes are
unchanged (tested in `tests/adapters/test_rest_budget_and_sockets.py`).

## Diff

```diff
--- a/.claude/rules/operations.md
+++ b/.claude/rules/operations.md
@@ §15
-15. **Rate limits:** one REST token bucket per adapter is shared by snapshots, resync re-reads, DLQ checks, cash-flow fetches and the 60 s account poll, with priority reconcile > on-demand cash-flow fetches requested by Oversee or an operator (relayed by the orchestrator, at most once a minute per account) > DLQ > poll.
+15. **Rate limits:** one REST token bucket per adapter is shared by snapshots, resync re-reads, DLQ checks, cash-flow fetches, the 60 s account poll and (only with the optional market-data / ARM services on) broker bar reads and the ARM's stop reads, with priority reconcile > on-demand cash-flow fetches requested by Oversee or an operator (relayed by the orchestrator, at most once a minute per account) > bars > DLQ > poll.
+    - `bars` keeps a 15 % reserve, waits at most 15 s, and is capped per adapter at `*_REST_BARS_PER_MIN` reads per minute (default a quarter of the bucket's rate, at most half of it); a read over the cap is refused at once (`TransientBrokerError`), so DLQ and poll keep their share.
+    - A read's age counts from when its first request was sent, after the token wait (`rest_sent_ms()`, the earliest send since the caller cleared it), never from when it was queued.
+    - Each request's outcome is counted per priority (`RestBudgetStats.outcomes`; throttle answers are counted apart and never read as an outage). REST failing at every priority with a recent outcome (last 60 s) is a broker outage (`broker_unavailable`, never an ARM FAILED); failures only at `bars` are bar-read failures; with no recent non-`bars` outcome the verdict is unknown and decides nothing.
```
