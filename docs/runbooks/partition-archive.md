# Partition archive and detach (data lifecycle)

Phase 1 never sends orders. This runbook only moves old rows out of the hot PostgreSQL tables
(operations.md s11); it touches no broker and no halt state.

## Trigger

* Page "lifecycle: partitions past 90 days await export" (the export job is not configured, or failed).
* Page "lifecycle: export row count mismatch (not detached)".
* Page "lifecycle: maintenance errors" naming `export <partition>`.
* Disk alert at 70 / 85 % on a PostgreSQL member.

## How it works

Every 6 hours the leader's lifecycle task creates partitions ahead, moves `RESOLVED` `events_dlq` rows older
than 7 days to `events_dlq_history` (partitioned by seq_id range, like `event_log`), then, for each
`event_log`, `account_snapshots`, `events_dlq_history` and `notification_outbox` partition whose month ended more than 90 days ago and that
has no `partition_archive` row, writes `$XTNL_ARCHIVE_DIR/<parent>/<partition>.jsonl.gz` (gzip JSONL, one
`to_jsonb` row per line, from one read-only snapshot) and a `.sha256` sidecar, reads the file back
(gzip CRC, line count, sha256), and only then records the `partition_archive` row (fenced on the epoch).
The detach step of the same run detaches a partition only when its row count still equals the recorded
`row_count`. Audit tables (`oversee_events`, `halt_events`, `cash_flows`, `leader_terms`) are never
exported or detached by the engine. Dropping a detached table is an operator step (below).

## Checks

```
grep XTNL_ARCHIVE_DIR /etc/xtnl/engine.env                 # unset = no export: partitions only page
ls -l /var/lib/xtnl/archive/*/                              # archive files and their .sha256 sidecars
cd /var/lib/xtnl/archive/event_log && sha256sum -c event_log_p202607.jsonl.gz.sha256
zcat event_log_p202607.jsonl.gz | wc -l                     # = partition_archive.row_count
$PSQL -c "SELECT partition_name, row_count, checksum, archive_uri, verified_by, detached_at_ms FROM partition_archive ORDER BY partition_name"
journalctl -u xtnl-engine --since "-7 h" | grep -E "lifecycle"
```

## Commands

* **Export not configured:** set `XTNL_ARCHIVE_DIR=/var/lib/xtnl/archive` in `/etc/xtnl/engine.env` on
  both nodes (the directory is created by `xtnl.tmpfiles.conf`) and restart the standby, then switch over
  (failover-and-switchover.md), so the leader picks it up without an unplanned gap.
* **Copy to S3** (the archive directory is the staging area; S3 is the long-term copy). The file is on
  the node that was leader when it was exported (`partition_archive.verified_by` = `engine:<node>`), so run
  the copy on **both** engine nodes:
  ```
  aws s3 cp /var/lib/xtnl/archive/ s3://<archive bucket>/xtnl/ --recursive --sse aws:kms --exclude "*.part"
  aws s3 ls s3://<archive bucket>/xtnl/event_log/
  ```
* **Count mismatch** (rows were written into an old partition after its export), or an archive file lost
  before its S3 copy: do not detach by hand. With a second person and a ticket, move the stale file aside
  and forget its ledger row (audited, fenced on the epoch, refused for a detached partition) so the next
  run re-exports it:
  ```
  $OPS archive list
  mv /var/lib/xtnl/archive/<parent>/<partition>.jsonl.gz{,.stale}
  $OPS archive forget <partition> --operator <you> --approver <second person> --reason "<ticket>: count mismatch"
  ```
  The output shows the archived and the current row counts; a warning says when they still match (only
  useful when the file was lost).
* **Drop a detached table** (only after the S3 copy is verified with `sha256sum -c`; two people):
  ```
  $PSQL -c "DROP TABLE <partition>"
  ```

## Rollback

A detached partition is still a table: re-attach it with
`ALTER TABLE <parent> ATTACH PARTITION <partition> FOR VALUES FROM (...) TO (...)` (the bounds are in the
partition's former definition, `pg_get_expr` in the archive ticket) and clear `detached_at_ms`. A dropped
table is restored from the S3 archive file (`zcat ... | jq` into a staging table) or from the Tier 3 backup.
