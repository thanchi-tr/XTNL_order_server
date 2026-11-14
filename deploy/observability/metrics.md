# XTNL engine metrics (operations.md §9; IMPLEMENTATION_PLAN 5.5 defines, 5.6 exports)

Phase 1 never sends orders. These metrics only describe ingestion, storage, Oversee and the HA backbone.

This file is the **contract** between the exporter (5.6) and the off-node alert rules in this directory
(`prometheus/xtnl-alerts.rules.yml`, `alertmanager/alertmanager.yml`, `cloudwatch/xtnl-alarms.json`).
5.6 exports **exactly** these names, types and labels; `tests/deploy/test_observability.py` checks that every
metric a rule or alarm uses is listed here, and that every alert of operations.md §9 has a rule and an alarm.

## Export conventions

* **Who:** every engine process (`xtnl-engine.service` on node A and node B) exports, in **every** SM9 role
  (STANDBY included), so the off-node evaluation never depends on the leader being alive: a dead or silent
  node is an `up == 0` / missing-data alert, and "no LEADER" is computed across both nodes.
* **How:** Prometheus text exposition (version 0.0.4) on `http://${NODE_IP}:9464/metrics` (VPC only, no
  inbound internet; operations.md §16). Prometheus (Amazon Managed Service for Prometheus via a scraper, or
  a self-run Prometheus) scrapes it every **5 s** as `job="xtnl-engine"`. The CloudWatch agent scrapes the
  same endpoint (Prometheus scrape -> embedded metric format) into namespace **`XTNL`**, with the labels below
  as dimensions; CloudWatch metric names are the same names.
* **Labels:** every metric carries `node` (the exporting node id, `XTNL_NODE_ID`, e.g. `node-a`). Further
  labels are listed per metric. No label ever carries an account id, a credential, a DSN or a URL with
  credentials. `subject` is the node a health record describes (health records are read from etcd by every
  node, so a peer's state is visible even when that peer's own exporter is down).
* **Units:** seconds and bytes (Prometheus base units); ratios are 0..1; booleans are gauges 0/1.
* **Absence:** a value the node cannot read right now is **not exported** (no NaN, no fake 0), except
  where a row says otherwise; the rules treat absence explicitly.
* Values that depend on the role are exported only in that role (column "role": `any` = every role,
  `leader` = only while SM9 is LEADER).

## Metrics

### Role, epoch, leadership (operations.md §2, §4)

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_exporter_up` | gauge | `node` | any | always 1 while the exporter answers (the CloudWatch "engine down" alarm treats its absence as breaching) |
| `xtnl_node_role` | gauge | `node`, `role` | any | 1 for the node's current SM9 role (`STARTING`, `STANDBY`, `CANDIDATE`, `LEADER_INIT`, `LEADER`, `DRAINING`); the other role series are 0 or absent |
| `xtnl_engine_epoch` | gauge | `node` | any | `engine_epoch.epoch` (fencing token) as last read from PostgreSQL |
| `xtnl_epoch_change_age_seconds` | gauge | `node` | any | now − `engine_epoch.bumped_at_ms` (seconds since the latest epoch bump; drives the RTO + 30 s inhibition) |
| `xtnl_epoch_changes_30m` | gauge | `node` | any | number of `leader_terms` rows started in the last 30 min (SAFE_HOLD at > 6) |
| `xtnl_safe_hold` | gauge | `node` | any | 1 while `cluster_state.safe_hold` is set |
| `xtnl_leader_colocated_with_primary` | gauge | `node` | leader | 1 when the leader's node hosts the Patroni primary, else 0 |
| `xtnl_lease_renew_p99_seconds` | gauge | `node` | leader | p99 of etcd lease keepalive round trips over the last 60 s |
| `xtnl_lease_remaining_seconds` | gauge | `node` | leader | `lease_deadline` − now (monotonic) |

### Health records (etcd; operations.md §4, §9 "standby not ready", tier-persistence.md §5 spill file)

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_health_record_age_seconds` | gauge | `node`, `subject` | any | age of `subject`'s health record as this node reads it; absent when there is no record (a record older than 5 s counts as absent for campaigning) |
| `xtnl_health_hard_checks_ok` | gauge | `node`, `subject` | any | 1 when `subject`'s record says it passes every hard check (PostgreSQL primary writable, etcd, local Redis, chrony ≤ 50 ms; leader: RiskSweep circuit) |
| `xtnl_health_campaign_eligible` | gauge | `node`, `subject` | any | 1 when `subject` is the LEADER or an eligible takeover target (hard checks pass, not in back-off, no SAFE_HOLD); 0 otherwise |
| `xtnl_health_spill_file_bytes` | gauge | `node`, `subject` | any | spill-file size `subject` published in its record |

### etcd (operations.md §1)

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_etcd_endpoint_healthy` | gauge | `node`, `member` | any | 1 when the member (`node-a`, `node-b`, `witness`) answers a maintenance status call within 1 s |
| `xtnl_etcd_has_leader` | gauge | `node` | any | 1 when the member status reports a raft leader |
| `xtnl_etcd_db_quota_ratio` | gauge | `node`, `member` | any | member `dbSize` / `quota-backend-bytes` (2 GiB, `deploy/etcd/etcd.conf.yml`) |
| `xtnl_etcd_alarms_active` | gauge | `node` | any | number of active etcd alarms (`NOSPACE`, `CORRUPT`, ...) from the alarm list |

### PostgreSQL / Patroni / WAL archive (operations.md §1, §6, §8)

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_patroni_member_running` | gauge | `node`, `member` | any | 1 when the Patroni REST `/cluster` view shows the member `running` or `streaming` |
| `xtnl_patroni_has_leader` | gauge | `node` | any | 1 when the Patroni cluster has a leader (primary) |
| `xtnl_patroni_sync_standby_count` | gauge | `node` | any | members in role `sync_standby` (0 = sync replication degraded to async) |
| `xtnl_pg_replication_lag_seconds` | gauge | `node`, `member` | any | replay lag of each standby member (`pg_stat_replication.replay_lag` on the primary) |
| `xtnl_pg_timeline` | gauge | `node`, `member` | any | timeline id of the member (`pg_control_checkpoint().timeline_id`) |
| `xtnl_wal_archive_failed_total` | counter | `node`, `member` | any | `pg_stat_archiver.failed_count` of the local member |
| `xtnl_wal_archive_age_seconds` | gauge | `node`, `member` | any | now − `pg_stat_archiver.last_archived_time` of the local member |
| `xtnl_disk_used_ratio` | gauge | `node`, `mount` | any | used / size of `/var/lib/postgresql`, `/var/lib/etcd`, `/var/lib/redis`, `/var/lib/xtnl` and `/` |

### Redis (tier-persistence.md §1)

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_redis_used_memory_ratio` | gauge | `node` | any | `used_memory` / `maxmemory` of the node's own Redis (`noeviction`) |
| `xtnl_feed_stream_length` | gauge | `node` | leader | `XLEN oversee:feed` (cap `MAXLEN ~ 1000000`; alert at 50 %) |
| `xtnl_feed_pending_entries` | gauge | `node` | leader | PEL size of the Oversee consumer group |
| `xtnl_feed_oldest_pending_age_seconds` | gauge | `node` | leader | age of the oldest un-acked feed entry (0 when the PEL is empty) |

### Ingestion, bus, DLQ (skill §3, tier-persistence.md §5, operations.md §15)

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_ingest_events_total` | counter | `node`, `adapter`, `stream` | leader | broker events read per stream (`prices`, `orders`, `positions`, `account`) |
| `xtnl_bus_queue_depth` | gauge | `node`, `topic` | leader | items in each bounded bus queue (`orders.internal.dto`, `position.state.dto`, `market.price.dto`, `account.state.dto`) |
| `xtnl_dlq_spool_ring_depth` | gauge | `node` | any | items in the in-memory `DlqSpooler` ring |
| `xtnl_dlq_rows` | gauge | `node`, `status` | leader | `events_dlq` rows per status (`QUEUED`, `LEASED`, `BACKOFF`, `QUARANTINED`; `RESOLVED` not exported) |
| `xtnl_adapter_state` | gauge | `node`, `adapter`, `state` | leader | 1 for each adapter's current SM4 state |
| `xtnl_rest_bucket_tokens` | gauge | `node`, `adapter` | leader | tokens left in the adapter's shared REST bucket |
| `xtnl_rest_requests_total` | counter | `node`, `adapter`, `priority` | leader | REST calls through the bucket by priority (`reconcile`, `cash_flows`, `dlq`, `poll`) |
| `xtnl_frames_lost_total` | counter | `node`, `stream` | leader | lost-frame events of the ingestion orchestrator (`IngestionOrchestrator.frames_lost`, summed over adapters, per stream `prices`, `orders`, `account`; positions travel on `orders`): +1 per frame it could not validate or map, and +1 per check that found newly skipped broker rows (`unmapped_rows`, one event however many rows). Each one opened a gap on its stream (or was a dropped price frame), never a silent loss (dto-contracts.md §4). A rising value means a broker sends something the adapter does not understand. Exported by the leader (`EngineSession.metrics_samples`); no alert uses it yet |

### Oversee, runtime, time, credentials (operations.md §3, §13, §14; oversee-rules.md §2)

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_risksweep_pass_duration_seconds` | gauge | `node` | leader | duration of the last complete pass over all accounts |
| `xtnl_risksweep_oldest_pass_age_seconds` | gauge | `node` | leader | age of the oldest per-account last complete pass |
| `xtnl_event_loop_lag_seconds` | gauge | `node` | any | worst event-loop lag measured by the 1 s watchdog heartbeat over the last 10 s |
| `xtnl_clock_offset_seconds` | gauge | `node` | any | chrony system-time offset (signed) |
| `xtnl_token_age_seconds` | gauge | `node`, `adapter` | leader | age of the adapter's current credential (cTrader token pair since its save; StoneX session since LogOn) |
| `xtnl_token_expires_in_seconds` | gauge | `node`, `adapter` | leader | time until the adapter's credential expires (absent when it does not expire) |
| `xtnl_cert_expires_in_seconds` | gauge | `node`, `cert` | any | time until a TLS certificate's notAfter (`etcd-client`, `etcd-peer`, `pg-server`, `patroni-rest`, `ca`) |

### Uptime and downtime catch-up (migration 0012; the engine is not on 24/7)

Counts and durations only: no series carries an amount. No alert uses them yet (the catch-up pages itself).

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_engine_uptime_seconds` | gauge | `node` | any | wall time since this engine process started (`engine_runs.started_at_ms` of the current run; includes any time the PC slept) |
| `xtnl_suspended_gaps_total` | counter | `node` | any | times this process noticed it had been suspended (PC sleep / hibernate: a wall-clock jump not matched by monotonic time, or both far beyond the 5 s tick); each one forces SM4 RESYNCING and a `resume_after_sleep` catch-up on the leader |
| `xtnl_offline_gap_seconds` | gauge | `node` | any | the offline span of the newest downtime catch-up of this process: from the adapter's last recorded `adapter_coverage.last_streaming_ms` to the term start (or the resume); absent before the first catch-up with a coverage row |
| `xtnl_catchup_runs_total` | counter | `node`, `adapter`, `complete` | any | downtime catch-ups finished by this process per adapter, `complete` = `true` / `false` (an incomplete one also pages "downtime catch-up incomplete") |
| `xtnl_catchup_rows_total` | counter | `node`, `adapter`, `kind` | any | rows the catch-ups changed, per `kind`: `order_new`, `order_updated`, `position_new`, `position_closed`, `cash_flow_new`, `anomaly` (a stored working order / OPEN position the broker no longer knows by id) |

## Witness host

The witness runs no engine and no exporter. Its disks (`/` and the etcd member's own volume `/var/lib/etcd`,
operations.md §1, §11) are watched by the CloudWatch agent with `cloudwatch/agent-witness.json` (generated):
the standard `disk_used_percent`, namespace `CWAgent`, published with exactly the dimensions `host` =
`witness` and `path` (the instance hostname is omitted, the device dropped), which the `xtnl-witness-disk-*`
alarms read. The 70 % alarm treats missing data as breaching, so a witness agent that stops reporting raises a
warning instead of leaving both alarms in INSUFFICIENT_DATA. Where a self-run Prometheus is used instead,
node_exporter on the witness must be scraped as `job="witness-node"` (`node_filesystem_avail_bytes`,
`node_filesystem_size_bytes`, `mountpoint` `/` and `/var/lib/etcd`); no such scrape is deployed by default.
Its etcd member is covered by `xtnl_etcd_endpoint_healthy{member="witness"}` from both engine nodes.

## What uses what

| Alert (operations.md) | Prometheus rule | CloudWatch alarm | Metrics |
|---|---|---|---|
| no LEADER > 30 s | `XtnlNoLeader` | `xtnl-no-leader` | `xtnl_node_role` |
| two LEADERs | `XtnlTwoLeaders` | `xtnl-two-leaders` | `xtnl_node_role` |
| etcd unhealthy | `XtnlEtcdUnhealthy`, `XtnlEtcdNoLeader`, `XtnlEtcdAlarm` | `xtnl-etcd-unhealthy-*`, `xtnl-etcd-no-leader`, `xtnl-etcd-alarm` | `xtnl_etcd_endpoint_healthy`, `xtnl_etcd_has_leader`, `xtnl_etcd_alarms_active` |
| etcd quota 50 % | `XtnlEtcdQuota50` | `xtnl-etcd-quota-50` | `xtnl_etcd_db_quota_ratio` |
| Patroni unhealthy | `XtnlPatroniUnhealthy`, `XtnlPatroniNoLeader` | `xtnl-patroni-member-*`, `xtnl-patroni-no-leader` | `xtnl_patroni_member_running`, `xtnl_patroni_has_leader` |
| sync standby missing | `XtnlSyncStandbyMissing` | `xtnl-sync-standby-missing` | `xtnl_patroni_sync_standby_count` |
| epoch-change rate | `XtnlEpochChangeRate` | `xtnl-epoch-change-rate` | `xtnl_epoch_changes_30m` |
| SAFE_HOLD (one off-node page) | `XtnlSafeHold` | `xtnl-safe-hold` | `xtnl_safe_hold` |
| spill file non-empty > 60 s | `XtnlSpillFileNonEmpty` | `xtnl-spill-file-*` | `xtnl_health_spill_file_bytes` |
| standby not ready (record absent / stale > 60 s) | `XtnlHealthRecordStale`, `XtnlEngineDown` | `xtnl-health-record-stale-*`, `xtnl-engine-down-*` | `xtnl_health_record_age_seconds`, `up` / `xtnl_exporter_up` |
| standby not ready (hard check / cannot campaign > 5 min) | `XtnlStandbyNotEligible` | `xtnl-standby-not-eligible-*` | `xtnl_health_campaign_eligible` |
| leader not on the primary's node > 10 min | `XtnlLeaderNotOnPrimary` | `xtnl-leader-not-on-primary` | `xtnl_leader_colocated_with_primary` |
| disk 70 / 85 % | `XtnlDisk70`, `XtnlDisk85`, `XtnlWitnessDisk70`, `XtnlWitnessDisk85` | `xtnl-disk-70-*`, `xtnl-disk-85-*`, `xtnl-witness-disk-*` | `xtnl_disk_used_ratio`, `disk_used_percent` / `node_filesystem_*` |
| certificate expiry 30 days | `XtnlCertExpiry` | `xtnl-cert-expiry-*` | `xtnl_cert_expires_in_seconds` |
| PITR horizon broken | `XtnlPitrHorizonBroken` | `xtnl-pitr-horizon-broken-*` | `xtnl_wal_archive_failed_total`, `xtnl_pg_timeline` |
| lease-renew p99 > 500 ms | `XtnlLeaseRenewSlow` | `xtnl-lease-renew-slow` | `xtnl_lease_renew_p99_seconds` |
| event-loop lag 100 ms | `XtnlEventLoopLag` | `xtnl-event-loop-lag-*` | `xtnl_event_loop_lag_seconds` |
| Redis memory before the limit | `XtnlRedisMemoryHigh` | `xtnl-redis-memory-*` | `xtnl_redis_used_memory_ratio` |
| feed lag at 50 % of the cap | `XtnlFeedLengthHigh`, `XtnlFeedPendingOld` | `xtnl-feed-length-high`, `xtnl-feed-pending-old` | `xtnl_feed_stream_length`, `xtnl_feed_oldest_pending_age_seconds` |
| replication lag | `XtnlReplicationLag` | `xtnl-replication-lag` | `xtnl_pg_replication_lag_seconds` |
| clock | `XtnlClockOffset` | `xtnl-clock-offset-*` | `xtnl_clock_offset_seconds` |
| inhibition: epoch change, RTO + 30 s | `XtnlEpochChangeSettling` (source of `inhibit_rules`) | `xtnl-epoch-change-settling` (`ActionsSuppressor`) | `xtnl_epoch_change_age_seconds` |

## CloudWatch notes

* Alarms with a 10 s period need high-resolution metrics: the CloudWatch agent must publish the `XTNL`
  namespace at 1 s storage resolution. If it can only publish at 60 s, the 30 s / 60 s alarms fire after
  their first 60 s period instead (Prometheus keeps the exact durations).
* `xtnl_wal_archive_failed_total` must reach CloudWatch as its raw (cumulative) value; the alarm takes
  `DIFF()` itself.
* Epoch-dependent alarms notify through a composite alarm (`<name>-notify`) whose `ActionsSuppressor` is
  `xtnl-epoch-change-settling`: each is held back, not grouped (CloudWatch has no grouping).

The PITR "automatic incremental backup" is not an alert action: `xtnl-pitr-guard` (this directory, run by
Patroni's `on_role_change` callback on promotion and by `xtnl-pitr-guard.timer` every minute) takes it.
