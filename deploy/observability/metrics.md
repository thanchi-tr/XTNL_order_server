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
| `xtnl_rest_requests_total` | counter | `node`, `adapter`, `priority` | leader | REST calls through the bucket by priority (`reconcile`, `cash_flows`, `bars`, `dlq`, `poll`; labels come from the priority names, so a new class adds a label and changes no other; `bars` stays 0 while the optional market-data and ARM services are off) |
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
| `xtnl_cert_expires_in_seconds` | gauge | `node`, `cert` | any | time until a TLS certificate's notAfter: the files this node serves or presents (`XTNL_METRICS_CERTS`: `etcd-client`, `etcd-peer`, `etcd-server`, `pg-server`, `patroni-rest`, `patroni-etcd-client`, `patroni-ca`, `redis-proxy`, `redis-proxy-client-ca`, `ca`, each at the path its service loads; on the single PC also `api-server`) and the certificates served on hosts without an exporter, read by a verifying handshake every 300 s (`XTNL_METRICS_CERT_ENDPOINTS`: `etcd-witness-server`, `etcd-witness-peer`, `deadman-receiver`, `dashboard-heartbeat`); absent while unreadable |
| `xtnl_cert_readable` | gauge | `node`, `cert` | any | 1 when that certificate could be read (an endpoint's: and verified), 0 when not (missing or unreadable file, failed handshake, expired / untrusted / misnamed certificate); one series per configured certificate (K5) |

### Uptime and downtime catch-up (migration 0012; the engine is not on 24/7)

Counts and durations only: no series carries an amount. No alert uses them yet (the catch-up pages itself).

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_engine_uptime_seconds` | gauge | `node` | any | wall time since this engine process started (`engine_runs.started_at_ms` of the current run; includes any time the PC slept) |
| `xtnl_suspended_gaps_total` | counter | `node` | any | times this process noticed it had been suspended (PC sleep / hibernate: a wall-clock jump not matched by monotonic time, or both far beyond the 5 s tick); each one forces SM4 RESYNCING and a `resume_after_sleep` catch-up on the leader |
| `xtnl_offline_gap_seconds` | gauge | `node` | any | the offline span of the newest downtime catch-up of this process: from the adapter's last recorded `adapter_coverage.last_streaming_ms` to the term start (or the resume); absent before the first catch-up with a coverage row |
| `xtnl_catchup_runs_total` | counter | `node`, `adapter`, `complete` | any | downtime catch-ups finished by this process per adapter, `complete` = `true` / `false` (an incomplete one also pages "downtime catch-up incomplete") |
| `xtnl_catchup_rows_total` | counter | `node`, `adapter`, `kind` | any | rows the catch-ups changed, per `kind`: `order_new`, `order_updated`, `position_new`, `position_closed`, `cash_flow_new`, `anomaly` (a stored working order / OPEN position the broker no longer knows by id) |

### Market data and ARM (optional services, plan S32; design §B14, §C2)

Exported by the leader only while `XTNL_MD_ENABLED=1` (the ARM rows also need `XTNL_ARM_ENABLED=1` and a running
ARM); absent otherwise. Counts and states only: no series carries a price, a level, an indicator value or an amount.
Labels are bounded: `state` is an SM12 or indicator state, `component` a fixed component name and `event` a counter
field name of that component.

| Name | Type | Labels | Role | Meaning |
|---|---|---|---|---|
| `xtnl_md_series` | gauge | `node`, `state` | leader | market-data series per SM12 state (`EMPTY`, `BACKFILLING`, `LIVE`, `GAPPED`, `FAILED`) |
| `xtnl_md_indicators` | gauge | `node`, `state` | leader | tracked indicators per state (`pending`, `ready`, `faulted`) |
| `xtnl_md_aof_wait_timeouts_total` | counter | `node` | leader | `redis-md` AOF confirmations that timed out (the bars stay queued and are re-written on the next flush) |
| `xtnl_md_events_total` | counter | `node`, `component`, `event` | leader | market-data counters of this term per component (`writer`, `bars`, `indicators`, `service`, `hook`, `crosscheck`; `src/runtime/optional_plan.py` `MD_EVENT_COMPONENTS`) and counter name (`flush_errors`, `invalid_bars`, `faulted`, `bootstraps`, `ticks`, `mismatches`, ...) |
| `xtnl_md_redis_used_memory_ratio` | gauge | `node` | leader | `redis-md` `used_memory` / `maxmemory` as `md-writer` last read it (`INFO memory` every 10 s; absent while unknown). md-writer pages at 70 % and sheds TICK data in its fixed order (design §A2, §A8) |
| `xtnl_md_shed_step` | gauge | `node` | leader | `md-writer`'s memory shedding step (0 = none, 1-4 = the §A8 order) |
| `xtnl_md_queue_depth` | gauge | `node`, `queue` | leader | `md-writer`'s in-memory backlog: `closed_bars` (the closed-bar queue, 6 h bound) and `pending_ticks` (ticks drained from the hook rings, not yet written) |
| `xtnl_arm_failed_scopes` | gauge | `node` | leader | open ARM FAILED scopes, from memory (§B14; the off-node backstop `XtnlArmFailed`) |
| `xtnl_arm_failed_global` | gauge | `node` | leader | 1 while the ARM-global scope is FAILED (or the open episodes are unreadable) |
| `xtnl_arm_oldest_undelivered_notice_age_seconds` | gauge | `node` | leader | age of the oldest undelivered kind `arm` outbox row (absent when none) |
| `xtnl_arm_events_total` | counter | `node`, `component`, `event` | leader | ARM counters of this term per component (`failed`, `budget`, `tracker`, `barclock`, `stopreads`, `notices`, `evaluation`; `ARM_EVENT_COMPONENTS`) and counter name (`entered`, `recorded`, `clears_applied`, `faults`, `decisions`, ...); component `pre_skipped` counts the PRE decisions not taken, with the reason code as `event` |
| `xtnl_arm_positions` | gauge | `node`, `view` | leader | positions as the `arm-tracker`'s last pass saw them: `armed` (live `arm_positions` rows), `unmanaged`, `overdue` (armed positions with more than 2 OPEN bars past their decision cap undecided, or no loaded schedule: the dead-man's `arm_overdue_positions`, S38g), `unarmed` (assigned but not armed after 5 min). SM11 states per position: `arm show` |
| `xtnl_arm_paused` | gauge | `node` | leader | 1 while the ARM is paused (operator `arm pause` or the configuration-hash pause): no decision until resumed |

### Design §C2 metric list (plan S37)

Where each item of the design's §C2 metric list is exported. Series are written `name{label="value"}`; for the two
`*_events_total` families the `event` is a counter field of the component's counters class
(`MD_EVENT_COMPONENTS` / `ARM_EVENT_COMPONENTS` in `src/runtime/optional_plan.py`), so a renamed counter fails
`tests/deploy/test_md_arm_deploy.py`. An item marked "not exported" has no metric series yet; the row names where an
operator reads it instead. Nothing here carries a price, a level, an indicator value, a spread or an amount.

| §C2 item | Series | Status |
|---|---|---|
| ticks in | `xtnl_md_events_total{component="hook",event="ticks"}`, `xtnl_md_events_total{component="writer",event="ticks_written"}` | exported |
| ticks dropped | `xtnl_md_events_total{component="hook",event="ring_dropped"}`, `xtnl_md_events_total{component="writer",event="ticks_dropped"}`, `xtnl_md_events_total{component="hook",event="rejected"}` | exported |
| receipt-time ticks | `xtnl_md_events_total{component="hook",event="receipt_timed"}` | exported |
| ring and queue depth | `xtnl_md_queue_depth{queue="closed_bars"}`, `xtnl_md_queue_depth{queue="pending_ticks"}`, `xtnl_md_events_total{component="writer",event="queue_dropped"}` | exported (the per-market hook ring depth itself is not: its overflow is `ring_dropped`) |
| writer lag | `xtnl_md_events_total{component="writer",event="deadline_skips"}`, `xtnl_md_events_total{component="writer",event="flush_errors"}` | not exported as a lag: the backlog is `xtnl_md_queue_depth`; `md show` lists `last_write_ms` per series |
| series by SM12 state | `xtnl_md_series{state="LIVE"}` | exported (one series per state) |
| gaps | `xtnl_md_series{state="GAPPED"}`, `xtnl_md_events_total{component="bars",event="heal_holes"}`, `xtnl_md_events_total{component="bars",event="unfillable"}` | exported |
| bars by source and quality | `xtnl_md_events_total{component="bars",event="bars_queued"}`, `xtnl_md_events_total{component="bars",event="no_trade"}`, `xtnl_md_events_total{component="writer",event="bars_released"}` | partly: no per-quality count (`md bars <series>` counts by quality) |
| revisions | `xtnl_md_events_total{component="bars",event="revisions"}` | exported |
| bar-fetch latency and REST waits / timeouts | `xtnl_rest_requests_total{priority="bars"}`, `xtnl_md_events_total{component="bars",event="pass_timeouts"}`, `xtnl_md_events_total{component="bars",event="series_timeouts"}`, `xtnl_md_events_total{component="bars",event="failed_reads"}` | partly: no latency histogram yet |
| backfill remaining buckets | `xtnl_md_events_total{component="bars",event="lane_added"}`, `xtnl_md_events_total{component="bars",event="lane_resolved"}`, `xtnl_md_events_total{component="bars",event="backfill_done"}` | partly: the remaining count is added minus resolved; `md show` per series |
| indicator warm / FAULTED and recompute queue | `xtnl_md_indicators{state="ready"}`, `xtnl_md_events_total{component="indicators",event="deferred"}` | exported |
| validation failures and quarantined buckets by check | `xtnl_md_events_total{component="bars",event="invalid_bars"}`, `xtnl_md_events_total{component="writer",event="quarantined"}` | partly: per check name only in `md bars <series>` (a check label would not be bounded per series) |
| BROKER-vs-TICK mismatches and run length | `xtnl_md_events_total{component="crosscheck",event="mismatches"}`, `xtnl_md_events_total{component="crosscheck",event="skipped"}` | partly: the run length per series is in `md show` (D6 counters) |
| AOF wait timeouts | `xtnl_md_aof_wait_timeouts_total` | exported |
| md generation and store class | `xtnl_md_events_total{component="service",event="bootstraps"}` | not exported as a value: `md show` and `arm_status` |
| rollback-set depth | | not exported: `md show` |
| consumer cursor lag | | not exported: no out-of-process consumer exists in Phase 1 |
| redis-md memory and shedding step | `xtnl_md_redis_used_memory_ratio`, `xtnl_md_shed_step` | exported (`XtnlRedisMdMemoryHigh` at 70 %) |
| skew | | not exported: `md:skew:{adapter}` through `md show` |
| positions by state | `xtnl_arm_positions{view="armed"}`, `xtnl_arm_paused` | partly: tracker views, not SM11 states (`arm show`) |
| decisions by kind / event / outcome | `xtnl_arm_events_total{component="barclock",event="decisions"}`, `xtnl_arm_events_total{component="barclock",event="pre_decisions"}`, `xtnl_arm_events_total{component="barclock",event="spans"}` | partly: no per-outcome count (`arm decisions`) |
| PRE skipped by reason | `xtnl_arm_events_total{component="pre_skipped"}` | exported (`event` = the reason code) |
| POST decision latency | | not exported: `arm decisions` shows `decided_at_ms` against the bar end |
| close source | | not exported: `arm decisions` |
| last POST age | | not exported: `arm_status` / `arm show` |
| notice hand-off lag and expiries | `xtnl_arm_oldest_undelivered_notice_age_seconds`, `xtnl_arm_events_total{component="notices",event="pre_expired"}` | exported |
| config age | | not exported: `arm config show` and the `arm_status` `config_state_*` counters |
| FAILED scopes and error-budget counters | `xtnl_arm_failed_scopes`, `xtnl_arm_failed_global`, `xtnl_arm_events_total{component="failed",event="entered"}`, `xtnl_arm_events_total{component="budget",event="faults"}` | exported (`XtnlArmFailed`) |

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
| certificate unreadable / not verifying / not watched | `XtnlCertUnreadable` | `xtnl-cert-unreadable-*` | `xtnl_cert_readable`, `xtnl_exporter_up` |
| PITR horizon broken | `XtnlPitrHorizonBroken` | `xtnl-pitr-horizon-broken-*` | `xtnl_wal_archive_failed_total`, `xtnl_pg_timeline` |
| lease-renew p99 > 500 ms | `XtnlLeaseRenewSlow` | `xtnl-lease-renew-slow` | `xtnl_lease_renew_p99_seconds` |
| event-loop lag 100 ms | `XtnlEventLoopLag` | `xtnl-event-loop-lag-*` | `xtnl_event_loop_lag_seconds` |
| Redis memory before the limit | `XtnlRedisMemoryHigh` | `xtnl-redis-memory-*` | `xtnl_redis_used_memory_ratio` |
| feed lag at 50 % of the cap | `XtnlFeedLengthHigh`, `XtnlFeedPendingOld` | `xtnl-feed-length-high`, `xtnl-feed-pending-old` | `xtnl_feed_stream_length`, `xtnl_feed_oldest_pending_age_seconds` |
| replication lag | `XtnlReplicationLag` | `xtnl-replication-lag` | `xtnl_pg_replication_lag_seconds` |
| clock | `XtnlClockOffset` | `xtnl-clock-offset-*` | `xtnl_clock_offset_seconds` |
| ARM FAILED scopes (design §B14 backstop) | `XtnlArmFailed` | `xtnl-arm-failed` | `xtnl_arm_failed_scopes` |
| redis-md memory 70 % (design §A2) | `XtnlRedisMdMemoryHigh` | `xtnl-redis-md-memory-*` | `xtnl_md_redis_used_memory_ratio` |
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
