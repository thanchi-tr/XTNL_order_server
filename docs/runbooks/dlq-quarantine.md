# DLQ, quarantine and the spill file

Phase 1 never sends orders. A DLQ row is a broker **event** the engine could not apply; replaying it only
re-reads broker state.

## Trigger

* "DLQ row quarantined" (`events_dlq.status = 'QUARANTINED'`, copied to `events_quarantine` with `source='dlq'`).
* "feed entry quarantined" (an `oversee:feed` entry delivered 5 times, `source='feed'`).
* "spill file non-empty > 60 s" on any node (from the health records / dead-man payload).
* DLQ depth metric rising.

## Checks

```
$OPS dlq list                                  # counts per status, newest 20 rows, unreleased quarantine rows
$OPS dlq list --status BACKOFF --limit 50
$OPS dlq list --all                            # include quarantine rows already released
ls -l /var/lib/xtnl/dlq_spill.jsonl            # on every node
```

`dlq list` never prints `raw_payload`. Do not paste it into tickets or chat from `psql` either: it can contain
account identifiers.

## Commands

* **Spill file non-empty:** the drain is idempotent (`INSERT ... ON CONFLICT DO NOTHING` into `events_dlq`)
  and any node may run it. It runs at engine start-up and continuously while PostgreSQL answers. If
  PostgreSQL is healthy and the file stays non-empty, restart the engine on that node (a STANDBY restart is
  harmless; for the leader, do a switchover first). Never delete the file by hand while it has content.
* **Row waiting in BACKOFF** that you want retried now (e.g. the broker outage behind it is over):
  ```
  $OPS dlq requeue <dlq_id> --operator <you> --reason "<ticket>: <why now>"
  ```
  Only `QUEUED` / `BACKOFF` rows: the row becomes due now and keeps its `retry_count`. A `LEASED` row
  belongs to the DLQ worker (`refused: status_leased`; wait for the lease to end); a `QUARANTINED` row
  needs the release below.
* **Quarantined row:** classify the `error_reason`:
  * mapping / schema error (code fix needed): leave it quarantined, open a bug with the `dlq_id`; after the
    fix ships, release it (the `quarantine_id` from `dlq list`):
    ```
    $OPS dlq release-quarantine <quarantine_id> --operator <you> --reason "<bug id>: fix shipped in <release>"
    ```
    The DLQ row goes back to `QUEUED` with `retry_count = 0`, and the quarantine row records
    `released_by` / `release_audit_id`, all in one transaction with the `operator_audit` row. The worker
    re-reads the broker row first (check-before-retry); the parked payload is never replayed as is.
  * superseded (the entity has since been reconciled from a newer broker read): no action; the
    check-before-retry consumer already proved the current state is newer.
* **Feed entry quarantined** (`source = feed`; `release-quarantine` refuses it with `feed_entry`): the
  RiskSweep still evaluates the account every second from PostgreSQL, so oversight continues. Check the
  account in `sweep_quarantine`; fix the cause.
* **Account quarantined by the RiskSweep** (page "oversight lapsed for account X": its pass failed for
  5 min). It is retried every 10 min and released by its first good pass. Once the cause is fixed, release
  it at once (`release_account()`, oversee-rules.md s4):
  ```
  $OPS account status                                   # quarantined accounts, why and since when
  $OPS account release <account> --operator <you> --reason "<ticket>: <cause fixed>"
  ```
  The `sweep_quarantine` row is released (`released_by` = you), and a `release_account` request for the
  current leader is queued, in one transaction with the `operator_audit` row; the leader drops its in-memory
  quarantine and evaluates the account at once. If the cause persists, the account fails again and is
  re-quarantined after 5 min. Refusals: `not_quarantined`, `already_released`, `already_pending`,
  `epoch_moved`.

## Expected outcome

Spill file size 0 on every node; `events_dlq` has no rows older than the retry horizon outside
`RESOLVED` / `QUARANTINED`; each quarantined row has a ticket.

## Rollback

An account release cannot be undone and needs no undo: the RiskSweep keeps evaluating the account and
re-quarantines it if its passes keep failing. Requeue and release are harmless to repeat (the check-before-retry consumer compares versions before
applying), so a mistaken requeue only costs a broker read: a stale event is ignored. A row that fails again
for a deterministic reason is quarantined again after `MAX_DLQ_RETRIES`. Every requeue / release is in
`$OPS audit tail --command dlq.requeue` / `--command dlq.release_quarantine`.
