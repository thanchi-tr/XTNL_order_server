# Proposed change (S04): `.claude/rules/dto-contracts.md` and `adapter-isolation.md`, orchestrator labels and stop reads

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.
It builds on the S01 proposal (`S01-dto-contract-fields.md`), which adds the fields themselves.

## Why

S04 makes the orchestrator set the PR8 version labels and the PR1 `order_kind` inheritance, and adds the read-only
adapter capability `fetch_active_stop_orders()` (design PR9). The rule files describe the inheritance of `broker_seq`
and list the adapter's allowed read methods; without this change they no longer match the code:

* `dto-contracts.md` §2 says only `broker_seq` is inherited. The code now also inherits a known `order_kind`, so a
  stream / REST resync never flips a STOP child to unknown (the stop that "disappears" at break-even).
* The S01 proposal says `WATERMARK` means "a watermark label". The code also uses it for an entity type kept in the
  local-time domain (StoneX accounts): it means "not a real broker time", and neither reaches `event_log.broker_time_ms`.
* `adapter-isolation.md` §4 lists every allowed adapter method. `fetch_active_stop_orders()` is a new read-only list
  read (StoneX `GET order/activestoplimitorders` per trading account; cTrader the reconcile request already on the
  ProtoOA allow-list). It sends, changes and cancels nothing.

## Diff

```diff
--- a/.claude/rules/dto-contracts.md
+++ b/.claude/rules/dto-contracts.md
@@ §2 Versioning, "Declared per (adapter, entity type)"
      - whether `broker_seq` exists. If it does, every row of that type carries one; a row that lacks it inherits the entity's last seen `broker_seq`.
+     - `order_kind` likewise: an order row without a kind takes the entity's known kind (seeded from the stored rows before the first pass, so it survives a restart) before the version compare. Two different known kinds page `order_kind_conflict` once and the order counts as kind unknown (`None`) for the rest of the term.
@@ §2 Versioning (the S01 label sentence)
-   ... `broker_time_source: Optional[Literal["BROKER", "WATERMARK"]]`, whether `broker_updated_at_ms` is a real broker time or a watermark label. ...
+   ... `broker_time_source: Optional[Literal["BROKER", "WATERMARK"]]`: `BROKER` when `broker_updated_at_ms` is the broker's own record time, else `WATERMARK` (a watermark-labelled REST row, or a type kept in the local-time domain). `origin` is `stream` (stream frames), `snapshot` (SNAPSHOTTING / RESYNCING passes and their by-id re-reads, the catch-up's by-id rechecks, the account poll and refreshes), `history` (catch-up history rows), `dlq` (check-before-retry re-reads) or `stop_read` (PR9 stop-order list reads). ...

--- a/.claude/rules/adapter-isolation.md
+++ b/.claude/rules/adapter-isolation.md
@@ §4 Read-Only Interface Lock
    - `fetch_history(since_ms, until_ms) -> BrokerHistory` (single-PC mode, ...)
+   - `fetch_active_stop_orders() -> ActiveStopOrders` (design PR9, optional; off unless `XTNL_ARM_STOP_READS=1`): ONE list read per adapter, never one call per handle. StoneX: `GET order/activestoplimitorders` per trading account (inside `rest_get_only`); cTrader: the open positions from one reconcile request (their `stopLoss` is the handle). The base default answers `supported=False`. The orchestrator's `reread_stop_orders()` applies the rows like any REST read (origin `stop_read`, REST class BARS) and never closes an entity absent from the list.
```

The `...` in the diff stands for the unchanged text of the same line.
