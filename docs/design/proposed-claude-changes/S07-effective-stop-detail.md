# Proposed change (S07): `.claude/rules/oversee-rules.md` §2, §5, §6, `effective_stop_detail()` (design PR1)

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.
It also needs the user's confirmation of the Oversee behaviour change itself (plan §9.2, last item; design §0 C4)
before S07 merges.

## Why

S07 builds the Oversee side of design PR1. `effective_stop()` now returns `effective_stop_detail(...).level`, and a
linked child counts as a stop by its `order_kind` instead of only by its price:

* a `STOP` child counts at any price, so a stop moved to break-even or beyond entry is still a stop (the fix for the
  StoneX stop that "disappears" at break-even); before S07 it stopped counting and the position read as "no stop".
  The one exception is a still-unfilled (PENDING / WORKING) entry order: a `STOP` child strictly beyond its entry
  (above it for a buy, below for a sell; in practice a mislabelled take-profit) is still no stop, exactly as before
  S07, so the order goes uncapped SOFT `missing_stop_loss` after 2 s and its risk reads `no_stop` instead of an
  UNKNOWN that would also make `MaxAggregateOpenRiskRule` UNKNOWN for the whole account;
* a `LIMIT` child never counts (before S07 a limit on the loss side of entry counted);
* a child of unknown kind (`None`) keeps today's rule (it counts only strictly on the loss side of entry) and the
  detail carries the caveat `stop_kind_unknown`;
* a counted `STOP` child of an OPEN position that is not on its triggerable side of the pass's mark (a fresh mark
  while the market is OPEN, the last clean mark while closed) keeps its level, and `MissingStopLossRule` turns the
  caveat `stop_kind_inconsistent` into UNKNOWN, which goes through the existing `unknown_since` /
  `unknown_grace_ms` (30 s) before it becomes a capped SOFT state. No new timer and no new column (R11-M07).
  The side is judged at the mid while a clean quote is fresh (at the bid / ask once the mark is the liquidation
  side), so a stop between the bid / ask and the mid is noticed a little late; never a false UNKNOWN.

**Behaviour change to confirm:** an OPEN position whose only stop-kind child is a `STOP` on the wrong side (a
take-profit mislabelled as STOP) used to read "no stop", go uncapped SOFT `missing_stop_loss` after 2 s and
escalate to HARD after `escalate_after_ms`. It now stays UNKNOWN for 30 s, then a **capped SOFT that never
escalates** and re-pages every `escalate_after_ms` until acknowledged or fixed.

The rule text in §5 and §6 describes only the old price-only test, so it no longer matches the code.

## Diff

```diff
--- a/.claude/rules/oversee-rules.md
+++ b/.claude/rules/oversee-rules.md
@@ §2 RiskSweep, "Deadlines are durable timestamps"
-   - **Deadlines are durable timestamps**, compared by the sweep. `MissingStopLossRule` uses `effective_stop_missing_since_ms` in the sweep-owned table `stop_watch` (never in the orchestrator's rows, so the two writers never contend), written the first time the sweep sees `needs_stop()` with no `effective_stop()` and cleared when a stop is seen. ...
+   - **Deadlines are durable timestamps**, compared by the sweep. `MissingStopLossRule` uses `effective_stop_missing_since_ms` in the sweep-owned table `stop_watch` (never in the orchestrator's rows, so the two writers never contend), written the first time the sweep sees `needs_stop()` with no `effective_stop()` and cleared when a stop is seen (any effective stop, including one with the `stop_kind_inconsistent` caveat, whose wait is the UNKNOWN grace instead). ...
@@ §2 RiskSweep, "Each result per (rule, subject)", the UNKNOWN bullet
-     - a missing or unknown input (no `InstrumentSpec`, no mid ever seen, a missing or only sampled live field, a rule that raised an error) makes the result **UNKNOWN**.
+     - a missing or unknown input (no `InstrumentSpec`, no mid ever seen, a missing or only sampled live field, a rule that raised an error, a STOP child on its non-triggerable side: `stop_kind_inconsistent`, §5) makes the result **UNKNOWN**.
@@ §2 Rule states, the UNKNOWN bullet
-     - **UNKNOWN** changes nothing for `unknown_grace_ms` (30 s), measured from `unknown_since` (stored in `halt_breaches`, so a restart doesn't restart it). After that it becomes a **capped SOFT** state (`risk_unknown`, `mark_unknown`, `live_unknown` or `evaluation_failed`), which resolves once a known result is below SOFT.
+     - **UNKNOWN** changes nothing for `unknown_grace_ms` (30 s), measured from `unknown_since` (stored in `halt_breaches`, so a restart doesn't restart it). After that it becomes a **capped SOFT** state (`risk_unknown`, `mark_unknown`, `live_unknown`, `stop_kind_inconsistent` or `evaluation_failed`), which resolves once a known result is below SOFT.
@@ §5 Shared Risk Definitions
-   - **`effective_stop(e)`**: `e.stop_loss`; else the non-terminal linked reduce-only stop children whose remaining units cover `e`'s units; else `None`.
+   - **`effective_stop_detail(e, positions, orders, mark=None)`** → `None` or `{level, handles, caveats}` (design PR1): `e.stop_loss` first (one `POSITION_FIELD` handle, e.g. a cTrader position stop); else the non-terminal linked reduce-only stop children (`CHILD_ORDER` handles) whose remaining units together cover `e`'s units, level = the most distant; else `None`. A child counts by its `order_kind`: `STOP` at any price (a stop at or beyond entry is still a stop) except strictly beyond the entry of a still-unfilled PENDING / WORKING entry order (no stop, as before), `LIMIT` never, `None` only strictly on the loss side of entry (caveat `stop_kind_unknown`, also set when a `None`-kind linked child is present but not counted). `mark` is the pass's MarkBook mark of the symbol (liquidation side): a counted `STOP` child of an OPEN position that is not strictly on its triggerable side of a fresh mark while OPEN (below it for a long, above it for a short), or of the last clean mark while closed, keeps its level and adds the caveat `stop_kind_inconsistent` (touched and awaiting its fill, or a mislabelled take-profit); the side is judged at the mid while a clean quote is fresh, so a stop between the bid / ask and the mid is caught a little late. An OPEN symbol with no quote at all for longer than `stale_mark_ms`, no mark, and the children of a still-unfilled entry order are not judged.
+   - **`effective_stop(e)`** = `effective_stop_detail(e, ...).level` (no mark), so Oversee and the ARM never disagree.
+   - `risk()`: a stop at or past breakeven counts 0 for a position (a break-even STOP child included); a PENDING / WORKING order whose `stop_loss` is on the wrong side of its entry stays UNKNOWN (a STOP child beyond an unfilled entry is not counted at all, so that order reads `no_stop`).
@@ §6 Rule Plugins
-   - **`MissingStopLossRule`**: for every entity where `needs_stop()` holds and `effective_stop()` is `None`, `SOFT_HALT` once now − `effective_stop_missing_since_ms` ≥ `missing_stop_loss.grace_ms` (2000). Oversee never calls the broker.
+   - **`MissingStopLossRule`**: for every entity where `needs_stop()` holds and `effective_stop_detail()` is `None`, `SOFT_HALT` once now − `effective_stop_missing_since_ms` ≥ `missing_stop_loss.grace_ms` (2000). Stop children that cover only part of the units are no stop. A detail with the caveat `stop_kind_inconsistent` is **UNKNOWN** `stop_kind_inconsistent`, naming only the inconsistent child ids (§2: nothing for 30 s, then a capped SOFT state, paged as every UNKNOWN is; capped, so it never escalates: a position whose only stop-kind child is on the wrong side used to escalate to HARD, it now stays capped SOFT and re-pages). Oversee never calls the broker.
```

The `...` in the diff stands for the unchanged text of the same line.
