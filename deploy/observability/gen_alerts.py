"""Generate the off-node alert definitions from one table (operations.md §9; IMPLEMENTATION_PLAN 5.5).

    python deploy/observability/gen_alerts.py            # rewrite the generated files
    python deploy/observability/gen_alerts.py --check    # exit 1 when a generated file is out of date

Outputs (do not edit them by hand):

* `prometheus/xtnl-alerts.rules.yml`  Prometheus alerting rules (Amazon Managed Service for Prometheus rules
  namespace, or a self-run Prometheus); routed by `alertmanager/alertmanager.yml` to SNS.
* `cloudwatch/xtnl-alarms.json`       CloudFormation template: the equivalent CloudWatch alarms -> SNS, with
  composite alarms that suppress the epoch-dependent alarms for RTO + 30 s (`ActionsSuppressor`).
* `cloudwatch/agent-prometheus.json`  CloudWatch agent config for both engine nodes: the EMF declarations (which
  `xtnl_*` metrics are published to namespace `XTNL`, with which dimensions) and the log shipping section (the
  engine's JSON log lines, written to /var/log/xtnl/ by `logs/30-xtnl-rsyslog.conf`, shipped off-node to
  CloudWatch Logs; operations.md §9 "Logs ... shipped off-node").
* `cloudwatch/xtnl-prometheus.yaml`   the Prometheus scrape config the agent reads (`prometheus_config_path`):
  job `xtnl-engine`, the node's own exporter on `${NODE_IP}:9464` every 5 s (render with envsubst per node).
* `cloudwatch/agent-witness.json`     CloudWatch agent config for the witness (no engine, no exporter): the
  standard `disk_used_percent` of `WITNESS_MOUNTS` as namespace `CWAgent` with exactly the dimensions
  {host=witness, path} the witness disk alarms read (operations.md §11 "disk alerts at 70 / 85 %"), and the
  witness's own log lines (the rolling etcd defrag) shipped off-node.

`DEFINED_METRICS` must equal the tables of `metrics.md`; `METRICS` is the part the engine exports today (the
contract `src/observability/metrics.py` implements): `DEFINED_METRICS` minus `PENDING_EXPORT`, the rows that are
defined but whose export lands in a later backlog item (no alert may use one). The deploy tests check that, and
that every operations.md §9 alert has a rule and an alarm. Standard library only.
"""
from __future__ import annotations

import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
PROM_RULES = HERE / "prometheus" / "xtnl-alerts.rules.yml"
CW_ALARMS = HERE / "cloudwatch" / "xtnl-alarms.json"
CW_AGENT = HERE / "cloudwatch" / "agent-prometheus.json"
CW_SCRAPE = HERE / "cloudwatch" / "xtnl-prometheus.yaml"
CW_WITNESS_AGENT = HERE / "cloudwatch" / "agent-witness.json"
#: Where the agent finds the scrape config (install-node.md copies CW_SCRAPE there, rendered per node).
CW_SCRAPE_INSTALLED = "/opt/aws/amazon-cloudwatch-agent/etc/xtnl-prometheus.yaml"
SCRAPE_JOB = "xtnl-engine"
SCRAPE_PORT = 9464                                 # src/observability/exporter.py DEFAULT_METRICS_PORT
SCRAPE_INTERVAL_S = 5
#: Log files written by rsyslog (logs/30-xtnl-rsyslog.conf) from the journald SyslogIdentifier of each unit,
#: shipped to CloudWatch Logs: (file, log group, retention days).
LOG_DIR = "/var/log/xtnl"
LOG_FILES: tuple[tuple[str, str, int], ...] = (
    ("xtnl-engine", "/xtnl/engine", 90),           # JSON lines (src/observability/logs.py)
    ("xtnl-migrate", "/xtnl/ops", 90),
    ("xtnl-pitr-guard", "/xtnl/ops", 90),
    ("xtnl-pg-backup", "/xtnl/ops", 90),
    ("xtnl-etcd-defrag", "/xtnl/ops", 90),
)

NAMESPACE = "XTNL"
NODES = ("node-a", "node-b")                       # XTNL_NODE_ID of the two engine nodes
ETCD_MEMBERS = ("node-a", "node-b", "witness")
PG_MEMBERS = ("node-a", "node-b")                  # PG_MEMBER_NAME of the two Patroni members
#: K5 (operations.md §13 "Certificate expiry alert at 30 days"): every certificate an engine node serves or presents,
#: read from its file on that node (XTNL_METRICS_CERTS), plus the certificates served on hosts without an exporter,
#: read by each engine node over a verifying TLS handshake (XTNL_METRICS_CERT_ENDPOINTS): the witness's etcd member
#: (client and peer ports) and the Melbourne receiver / dashboard heartbeat listeners. The Melbourne-local client
#: certificate (the dashboard's Redis client certificate) is watched by the dead-man receiver (XTNL_RECEIVER_CERTS).
#: Each FILE_CERTS name maps to the file its service actually loads (engine.env.example XTNL_METRICS_CERTS:
#: /etc/etcd/pki/<ETCD_NAME>{,-peer}.crt, /etc/patroni/pki/<PG_MEMBER_NAME>{,-pg,-etcd-client}.crt, both CAs), so a
#: renewed served certificate is the one watched.
FILE_CERTS = ("etcd-client", "etcd-peer", "etcd-server", "pg-server", "patroni-rest", "patroni-etcd-client",
              "patroni-ca", "redis-proxy", "redis-proxy-client-ca", "ca")
ENDPOINT_CERTS = ("etcd-witness-server", "etcd-witness-peer", "deadman-receiver", "dashboard-heartbeat")
CERTS = FILE_CERTS + ENDPOINT_CERTS
MOUNTS = ("/", "/var/lib/postgresql", "/var/lib/etcd", "/var/lib/redis", "/var/lib/xtnl")
#: The witness: root volume and its etcd member's own gp3 volume (operations.md §1), watched by the CloudWatch
#: agent (`agent-witness.json`) as CWAgent `disk_used_percent` {host=WITNESS_HOST, path=<mount>}.
WITNESS_HOST = "witness"
WITNESS_MOUNTS = ("/", "/var/lib/etcd")
#: Log idents written on the witness (only the defrag job runs there).
WITNESS_LOG_IDENTS = ("xtnl-etcd-defrag",)
RTO_PLUS_30_S = 120                                # node / AZ loss RTO ~45-90 s (operations.md §6) + 30 s
RUNBOOK = "docs/runbooks/off-node-alerts.md"


# ============================================================================================ metrics
@dataclass(frozen=True)
class Metric:
    name: str
    type: str                  # gauge | counter
    labels: tuple[str, ...]    # always starts with "node"


DEFINED_METRICS: tuple[Metric, ...] = tuple(Metric(n, t, tuple(ls.split())) for n, t, ls in (
    ("xtnl_exporter_up", "gauge", "node"),
    ("xtnl_node_role", "gauge", "node role"),
    ("xtnl_engine_epoch", "gauge", "node"),
    ("xtnl_epoch_change_age_seconds", "gauge", "node"),
    ("xtnl_epoch_changes_30m", "gauge", "node"),
    ("xtnl_safe_hold", "gauge", "node"),
    ("xtnl_leader_colocated_with_primary", "gauge", "node"),
    ("xtnl_lease_renew_p99_seconds", "gauge", "node"),
    ("xtnl_lease_remaining_seconds", "gauge", "node"),
    ("xtnl_health_record_age_seconds", "gauge", "node subject"),
    ("xtnl_health_hard_checks_ok", "gauge", "node subject"),
    ("xtnl_health_campaign_eligible", "gauge", "node subject"),
    ("xtnl_health_spill_file_bytes", "gauge", "node subject"),
    ("xtnl_etcd_endpoint_healthy", "gauge", "node member"),
    ("xtnl_etcd_has_leader", "gauge", "node"),
    ("xtnl_etcd_db_quota_ratio", "gauge", "node member"),
    ("xtnl_etcd_alarms_active", "gauge", "node"),
    ("xtnl_patroni_member_running", "gauge", "node member"),
    ("xtnl_patroni_has_leader", "gauge", "node"),
    ("xtnl_patroni_sync_standby_count", "gauge", "node"),
    ("xtnl_pg_replication_lag_seconds", "gauge", "node member"),
    ("xtnl_pg_timeline", "gauge", "node member"),
    ("xtnl_wal_archive_failed_total", "counter", "node member"),
    ("xtnl_wal_archive_age_seconds", "gauge", "node member"),
    ("xtnl_disk_used_ratio", "gauge", "node mount"),
    ("xtnl_redis_used_memory_ratio", "gauge", "node"),
    ("xtnl_feed_stream_length", "gauge", "node"),
    ("xtnl_feed_pending_entries", "gauge", "node"),
    ("xtnl_feed_oldest_pending_age_seconds", "gauge", "node"),
    ("xtnl_ingest_events_total", "counter", "node adapter stream"),
    ("xtnl_bus_queue_depth", "gauge", "node topic"),
    ("xtnl_dlq_spool_ring_depth", "gauge", "node"),
    ("xtnl_dlq_rows", "gauge", "node status"),
    ("xtnl_adapter_state", "gauge", "node adapter state"),
    ("xtnl_rest_bucket_tokens", "gauge", "node adapter"),
    ("xtnl_rest_requests_total", "counter", "node adapter priority"),
    ("xtnl_frames_lost_total", "counter", "node stream"),
    ("xtnl_risksweep_pass_duration_seconds", "gauge", "node"),
    ("xtnl_risksweep_oldest_pass_age_seconds", "gauge", "node"),
    ("xtnl_event_loop_lag_seconds", "gauge", "node"),
    ("xtnl_clock_offset_seconds", "gauge", "node"),
    ("xtnl_token_age_seconds", "gauge", "node adapter"),
    ("xtnl_token_expires_in_seconds", "gauge", "node adapter"),
    ("xtnl_cert_expires_in_seconds", "gauge", "node cert"),
    ("xtnl_cert_readable", "gauge", "node cert"),
    # engine uptime + downtime catch-up (migration 0012); no alert uses them yet
    ("xtnl_engine_uptime_seconds", "gauge", "node"),
    ("xtnl_suspended_gaps_total", "counter", "node"),
    ("xtnl_offline_gap_seconds", "gauge", "node"),
    ("xtnl_catchup_runs_total", "counter", "node adapter complete"),
    ("xtnl_catchup_rows_total", "counter", "node adapter kind"),
    # dead-man receiver acknowledgement (home-server plan W1); the engine pages it through the outbox itself
    ("xtnl_deadman_configured", "gauge", "node"),
    ("xtnl_deadman_consecutive_failures", "gauge", "node"),
    ("xtnl_deadman_last_ok_age_seconds", "gauge", "node"),
    # optional market-data and ARM services (plan S32); absent while the flags are off
    ("xtnl_md_series", "gauge", "node state"),
    ("xtnl_md_indicators", "gauge", "node state"),
    ("xtnl_md_aof_wait_timeouts_total", "counter", "node"),
    ("xtnl_md_events_total", "counter", "node component event"),
    # plan S37: the rest of the design §C2 list the term already holds (metrics.md "Design §C2 metric list")
    ("xtnl_md_redis_used_memory_ratio", "gauge", "node"),
    ("xtnl_md_shed_step", "gauge", "node"),
    ("xtnl_md_queue_depth", "gauge", "node queue"),
    ("xtnl_arm_positions", "gauge", "node view"),
    ("xtnl_arm_paused", "gauge", "node"),
    ("xtnl_arm_failed_scopes", "gauge", "node"),
    ("xtnl_arm_failed_global", "gauge", "node"),
    ("xtnl_arm_oldest_undelivered_notice_age_seconds", "gauge", "node"),
    ("xtnl_arm_events_total", "counter", "node component event"),
))
#: Defined in metrics.md, exported from a later backlog item. Empty: the engine exports every defined row
#: (`xtnl_frames_lost_total`, the last one, since IMPLEMENTATION_PLAN 7.4; dropped here in 8.2). The exporter
#: contract (`src/observability/metrics.py` CONTRACT) must equal `METRICS`, so a new row whose export lands later
#: is listed here until that change. No alert may use a pending metric (tests/deploy).
PENDING_EXPORT: frozenset[str] = frozenset()
#: What the engine exports today: the exporter contract.
METRICS: tuple[Metric, ...] = tuple(m for m in DEFINED_METRICS if m.name not in PENDING_EXPORT)
METRIC_NAMES = frozenset(m.name for m in METRICS)
#: Metrics of other exporters the rules use (not exported by the engine).
EXTERNAL_METRICS = frozenset({"up", "node_filesystem_avail_bytes", "node_filesystem_size_bytes",
                              "disk_used_percent"})


# ============================================================================================ CloudWatch helpers
def dims(**kv: str) -> list[dict[str, str]]:
    return [{"Name": k, "Value": v} for k, v in kv.items()]


def stat(metric: str, d: list[dict[str, str]], s: str = "Maximum", period: int = 10,
         namespace: str = NAMESPACE) -> dict[str, Any]:
    return {"Metric": {"Namespace": namespace, "MetricName": metric, "Dimensions": d}, "Period": period, "Stat": s}


@dataclass(frozen=True)
class CwAlarm:
    """One CloudWatch alarm: `expression` over `inputs` (id -> MetricStat), compared with `threshold`."""

    name: str
    description: str
    inputs: dict[str, dict[str, Any]]
    expression: str
    op: str                    # GreaterThanThreshold | LessThanThreshold | GreaterThanOrEqualToThreshold | ...
    threshold: float
    periods: int               # consecutive breaching periods (the rule's `for`)
    missing: str = "missing"   # breaching | notBreaching | missing | ignore
    #: An alarm whose ALARM state means "suppress my notifications" (a composite `-notify` alarm with
    #: ActionsSuppressor notifies instead of this one); None = notify directly (or via the epoch-settle composite).
    suppressed_by: str | None = None

    def properties(self, severity: str, actions: bool) -> dict[str, Any]:
        queries: list[dict[str, Any]] = [{"Id": k, "MetricStat": v, "ReturnData": False}
                                         for k, v in self.inputs.items()]
        queries.append({"Id": "e1", "Expression": self.expression, "Label": self.name, "ReturnData": True})
        props: dict[str, Any] = {
            "AlarmName": self.name, "AlarmDescription": self.description, "Metrics": queries,
            "ComparisonOperator": self.op, "Threshold": self.threshold, "EvaluationPeriods": self.periods,
            "DatapointsToAlarm": self.periods, "TreatMissingData": self.missing}
        if actions:
            topic = {"Ref": "SnsCriticalTopicArn" if severity == "critical" else "SnsWarningTopicArn"}
            props["AlarmActions"] = [topic]
            props["OKActions"] = [topic]
        return props


def any_of(name: str, desc: str, metric: str, variants: Sequence[dict[str, str]], *, combine: str, op: str,
           threshold: float, periods: int, missing: str = "missing", fill: float | None = None,
           s: str = "Maximum", period: int = 10) -> CwAlarm:
    """Combine one metric over several dimension sets (e.g. the two observer nodes) with MAX / MIN / SUM."""
    inputs = {f"m{i}": stat(metric, dims(**v), s, period) for i, v in enumerate(variants)}
    terms = [f"FILL(m{i}, {fill:g})" if fill is not None else f"m{i}" for i in range(len(variants))]
    expr = f"{combine}([{', '.join(terms)}])" if combine != "SUM" else " + ".join(terms)
    return CwAlarm(name, desc, inputs, expr, op, threshold, periods, missing)


def per_node(fn: Callable[[str], CwAlarm]) -> list[CwAlarm]:
    return [fn(n) for n in NODES]


def observers(**kv: str) -> list[dict[str, str]]:
    """The same series as seen by each engine node (label `node` = the observer)."""
    return [{"node": n, **kv} for n in NODES]


# ============================================================================================ alerts
@dataclass(frozen=True)
class Alert:
    name: str                       # Prometheus alertname
    spec: str                       # the operations.md / tier-persistence.md requirement it implements
    severity: str                   # critical | warning | none (inhibition source only)
    expr: str
    for_: str
    summary: str
    metrics: tuple[str, ...]
    cloudwatch: Callable[[], list[CwAlarm]]
    depends_on_epoch: bool = False  # suppressed for RTO + 30 s after an epoch change (operations.md §9)
    extra_labels: dict[str, str] = field(default_factory=dict)


def _a(name: str, spec: str, severity: str, expr: str, for_: str, summary: str, metrics: str,
       cloudwatch: Callable[[], list[CwAlarm]], *, dep: bool = False) -> Alert:
    return Alert(name, spec, severity, " ".join(expr.split()), for_, summary, tuple(metrics.split()), cloudwatch, dep)


def _leaders(name: str, desc: str, op: str, threshold: float, periods: int) -> list[CwAlarm]:
    return [any_of(name, desc, "xtnl_node_role", [{"node": n, "role": "LEADER"} for n in NODES], combine="SUM",
                   op=op, threshold=threshold, periods=periods, missing="breaching" if op.startswith("Less")
                   else "notBreaching", fill=0)]


_WITNESS_RE = "|".join(WITNESS_MOUNTS)


def _mount_suffix(mount: str) -> str:
    return mount.replace("/", "-").rstrip("-") or "-root"


def _witness_disk(mount: str, pct: int, missing: str) -> CwAlarm:
    """CWAgent `disk_used_percent` of one witness mount, as `agent-witness.json` publishes it."""
    return CwAlarm(f"xtnl-witness-disk-{pct}{_mount_suffix(mount)}", f"Witness {mount} > {pct} %",
                   {"m0": stat("disk_used_percent", dims(host=WITNESS_HOST, path=mount), period=60,
                               namespace="CWAgent")}, "m0", "GreaterThanThreshold", pct, 5, missing)


ALERTS: tuple[Alert, ...] = (
    # ---- leadership (operations.md §4, §9) ------------------------------------------------------------------
    _a("XtnlNoLeader", "no LEADER for more than 30 s", "critical",
       '(sum(xtnl_node_role{role="LEADER"} == 1) or vector(0)) < 1', "30s",
       "No engine node is LEADER: ingestion and Oversee are not running", "xtnl_node_role",
       lambda: _leaders("xtnl-no-leader", "No engine LEADER for 30 s", "LessThanThreshold", 1, 3)),
    _a("XtnlTwoLeaders", "two LEADERs", "critical",
       'count(xtnl_node_role{role="LEADER"} == 1) > 1', "15s",
       "Two engine nodes report LEADER at once (fencing must resolve this within the lease TTL)", "xtnl_node_role",
       lambda: _leaders("xtnl-two-leaders", "Two engine LEADERs", "GreaterThanThreshold", 1, 2)),
    _a("XtnlEngineDown", "standby not ready: a node's exporter / health record absent for more than 60 s",
       "critical", 'up{job="xtnl-engine"} == 0 or absent(up{job="xtnl-engine"})', "60s",
       "An engine node's exporter is unreachable (process down, node lost or network cut)", "up xtnl_exporter_up",
       lambda: per_node(lambda n: CwAlarm(
           f"xtnl-engine-down-{n}", f"Engine {n} exports nothing for 60 s",
           {"m0": stat("xtnl_exporter_up", dims(node=n))}, "m0", "LessThanThreshold", 1, 6, "breaching"))),
    _a("XtnlHealthRecordStale", "standby not ready: a peer's health record absent or stale for more than 60 s",
       "critical",
       '(min by (subject) (xtnl_health_record_age_seconds) > 5) or '
       '(label_replace(count by (node) (xtnl_node_role), "subject", "$1", "node", "(.*)") '
       'unless on (subject) count by (subject) (xtnl_health_record_age_seconds))', "60s",
       "A node's etcd health record is absent or older than 5 s: it cannot take over", "xtnl_health_record_age_seconds xtnl_node_role",
       lambda: per_node(lambda n: any_of(
           f"xtnl-health-record-stale-{n}", f"Health record of {n} absent or stale for 60 s",
           "xtnl_health_record_age_seconds", observers(subject=n), combine="MIN", op="GreaterThanThreshold",
           threshold=5, periods=6, missing="breaching", fill=1e9))),
    _a("XtnlStandbyNotEligible", "standby not ready: a peer failing a hard check or unable to campaign for more "
       "than 5 min", "critical", "max by (subject) (xtnl_health_campaign_eligible) == 0", "5m",
       "A node fails a hard check or cannot campaign (back-off, SAFE_HOLD): no takeover target",
       "xtnl_health_campaign_eligible",
       lambda: per_node(lambda n: any_of(
           f"xtnl-standby-not-eligible-{n}", f"{n} not campaign-eligible for 5 min", "xtnl_health_campaign_eligible",
           observers(subject=n), combine="MAX", op="LessThanThreshold", threshold=1, periods=5, missing="breaching",
           fill=0, period=60))),
    _a("XtnlSpillFileNonEmpty", "a non-empty spill file on any node for more than 60 s (from the health records)",
       "critical", "max by (subject) (xtnl_health_spill_file_bytes) > 0", "60s",
       "A node's DLQ spill file is not draining into PostgreSQL", "xtnl_health_spill_file_bytes",
       lambda: per_node(lambda n: any_of(
           f"xtnl-spill-file-{n}", f"Spill file on {n} non-empty for 60 s", "xtnl_health_spill_file_bytes",
           observers(subject=n), combine="MAX", op="GreaterThanThreshold", threshold=0, periods=6,
           missing="notBreaching", fill=0))),
    _a("XtnlSafeHold", "SAFE_HOLD set: one off-node page", "critical", "max(xtnl_safe_hold) == 1", "0s",
       "SAFE_HOLD is set: no node campaigns and the gate stays closed (operator resume needed)", "xtnl_safe_hold",
       lambda: [any_of("xtnl-safe-hold", "SAFE_HOLD set", "xtnl_safe_hold", [{"node": n} for n in NODES],
                       combine="MAX", op="GreaterThanOrEqualToThreshold", threshold=1, periods=1, fill=0)]),
    _a("XtnlEpochChangeRate", "epoch-change rate", "warning", "max(xtnl_epoch_changes_30m) >= 3", "0s",
       "3 or more epoch changes in 30 min (SAFE_HOLD at more than 6)", "xtnl_epoch_changes_30m",
       lambda: [any_of("xtnl-epoch-change-rate", ">= 3 epoch changes in 30 min", "xtnl_epoch_changes_30m",
                       [{"node": n} for n in NODES], combine="MAX", op="GreaterThanOrEqualToThreshold",
                       threshold=3, periods=1, fill=0, period=60)]),
    _a("XtnlEpochChangeSettling", f"inhibition: an epoch change suppresses dependent alerts for RTO + 30 s "
       f"({RTO_PLUS_30_S} s)", "none", f"min(xtnl_epoch_change_age_seconds) < {RTO_PLUS_30_S}", "0s",
       "Epoch changed recently: dependent alerts are held back, then sent as one summary",
       "xtnl_epoch_change_age_seconds",
       lambda: [any_of("xtnl-epoch-change-settling", f"Epoch changed less than {RTO_PLUS_30_S} s ago",
                       "xtnl_epoch_change_age_seconds", [{"node": n} for n in NODES], combine="MIN",
                       op="LessThanThreshold", threshold=RTO_PLUS_30_S, periods=1, fill=1e9)]),
    _a("XtnlLeaderNotOnPrimary", "leader not on the Patroni primary's node for more than 10 min", "warning",
       "max(xtnl_leader_colocated_with_primary) == 0", "10m",
       "The engine leader is not on the Patroni primary's node: schedule a planned switchover",
       "xtnl_leader_colocated_with_primary",
       lambda: [any_of("xtnl-leader-not-on-primary", "Leader not co-located with the primary for 10 min",
                       "xtnl_leader_colocated_with_primary", [{"node": n} for n in NODES], combine="MAX",
                       op="LessThanThreshold", threshold=1, periods=10, missing="notBreaching", period=60)]),
    _a("XtnlLeaseRenewSlow", "lease-renew p99 > 500 ms", "warning", "max(xtnl_lease_renew_p99_seconds) > 0.5", "1m",
       "etcd lease keepalive p99 above 500 ms (fence at 12 s without renewal)", "xtnl_lease_renew_p99_seconds",
       lambda: [any_of("xtnl-lease-renew-slow", "Lease renew p99 > 500 ms", "xtnl_lease_renew_p99_seconds",
                       [{"node": n} for n in NODES], combine="MAX", op="GreaterThanThreshold", threshold=0.5,
                       periods=6, missing="notBreaching")]),
    # ---- etcd (operations.md §1) --------------------------------------------------------------------------
    _a("XtnlEtcdUnhealthy", "etcd unhealthy (a member)", "critical",
       "max by (member) (xtnl_etcd_endpoint_healthy) == 0", "30s",
       "An etcd member does not answer from either engine node (quorum is 2 of 3)", "xtnl_etcd_endpoint_healthy",
       lambda: [any_of(f"xtnl-etcd-unhealthy-{m}", f"etcd member {m} unhealthy", "xtnl_etcd_endpoint_healthy",
                       observers(member=m), combine="MAX", op="LessThanThreshold", threshold=1, periods=3,
                       missing="breaching", fill=0) for m in ETCD_MEMBERS]),
    _a("XtnlEtcdNoLeader", "etcd unhealthy (no raft leader)", "critical", "max(xtnl_etcd_has_leader) == 0", "15s",
       "etcd has no raft leader: no lease can be renewed", "xtnl_etcd_has_leader",
       lambda: [any_of("xtnl-etcd-no-leader", "etcd has no leader", "xtnl_etcd_has_leader",
                       [{"node": n} for n in NODES], combine="MAX", op="LessThanThreshold", threshold=1, periods=2,
                       missing="breaching", fill=0)]),
    _a("XtnlEtcdAlarm", "any etcd alarm", "critical", "max(xtnl_etcd_alarms_active) > 0", "0s",
       "etcd raised an alarm (NOSPACE / CORRUPT): writes may be refused", "xtnl_etcd_alarms_active",
       lambda: [any_of("xtnl-etcd-alarm", "etcd alarm active", "xtnl_etcd_alarms_active",
                       [{"node": n} for n in NODES], combine="MAX", op="GreaterThanThreshold", threshold=0, periods=1,
                       fill=0)]),
    _a("XtnlEtcdQuota50", "etcd quota 50 %", "warning", "max by (member) (xtnl_etcd_db_quota_ratio) > 0.5", "5m",
       "An etcd member's database is above 50 % of quota-backend-bytes (defrag / compaction)",
       "xtnl_etcd_db_quota_ratio",
       lambda: [any_of(f"xtnl-etcd-quota-50-{m}", f"etcd {m} db > 50 % of quota", "xtnl_etcd_db_quota_ratio",
                       observers(member=m), combine="MAX", op="GreaterThanThreshold", threshold=0.5, periods=5,
                       period=60) for m in ETCD_MEMBERS]),
    # ---- PostgreSQL / Patroni (operations.md §1, §6, §8) ---------------------------------------------------
    _a("XtnlPatroniUnhealthy", "Patroni unhealthy (a member)", "critical",
       "max by (member) (xtnl_patroni_member_running) == 0", "30s",
       "A Patroni member is not running / streaming", "xtnl_patroni_member_running",
       lambda: [any_of(f"xtnl-patroni-member-{m}", f"Patroni member {m} not running", "xtnl_patroni_member_running",
                       observers(member=m), combine="MAX", op="LessThanThreshold", threshold=1, periods=3,
                       missing="breaching", fill=0) for m in PG_MEMBERS]),
    _a("XtnlPatroniNoLeader", "Patroni unhealthy (no primary)", "critical", "max(xtnl_patroni_has_leader) == 0", "30s",
       "The Patroni cluster has no primary", "xtnl_patroni_has_leader",
       lambda: [any_of("xtnl-patroni-no-leader", "Patroni has no primary", "xtnl_patroni_has_leader",
                       [{"node": n} for n in NODES], combine="MAX", op="LessThanThreshold", threshold=1, periods=3,
                       missing="breaching", fill=0)]),
    _a("XtnlSyncStandbyMissing", "sync standby missing (sync-degraded)", "critical",
       "max(xtnl_patroni_sync_standby_count) < 1", "30s",
       "No synchronous standby: replication degraded to async, RPO 0 is lost", "xtnl_patroni_sync_standby_count",
       lambda: [any_of("xtnl-sync-standby-missing", "No sync standby", "xtnl_patroni_sync_standby_count",
                       [{"node": n} for n in NODES], combine="MAX", op="LessThanThreshold", threshold=1, periods=3,
                       missing="breaching", fill=0)], dep=True),
    _a("XtnlReplicationLag", "replication lag", "warning", "max by (member) (xtnl_pg_replication_lag_seconds) > 10",
       "1m", "A standby replays more than 10 s behind the primary", "xtnl_pg_replication_lag_seconds",
       lambda: [any_of(f"xtnl-replication-lag-{m}", f"Replication lag of {m} > 10 s", "xtnl_pg_replication_lag_seconds",
                       observers(member=m), combine="MAX", op="GreaterThanThreshold", threshold=10, periods=6,
                       missing="notBreaching") for m in PG_MEMBERS], dep=True),
    _a("XtnlPitrHorizonBroken", "PITR horizon broken: a dropped WAL or a timeline switch (operations.md §8)",
       "critical",
       "(max by (member) (increase(xtnl_wal_archive_failed_total[5m])) > 0) or "
       "(max by (member) (changes(xtnl_pg_timeline[15m])) > 0)", "0s",
       "WAL archiving failed or the timeline switched: xtnl-pitr-guard takes an incremental backup",
       "xtnl_wal_archive_failed_total xtnl_pg_timeline",
       lambda: [CwAlarm(f"xtnl-pitr-horizon-broken-{m}", f"WAL archive failure or timeline switch on {m}",
                        {f"f{i}": stat("xtnl_wal_archive_failed_total", dims(node=n, member=m), period=60)
                         for i, n in enumerate(NODES)} |
                        {f"t{i}": stat("xtnl_pg_timeline", dims(node=n, member=m), period=60)
                         for i, n in enumerate(NODES)},
                        "MAX([FILL(DIFF(f0), 0), FILL(DIFF(f1), 0), FILL(ABS(DIFF(t0)), 0), FILL(ABS(DIFF(t1)), 0)])",
                        "GreaterThanThreshold", 0, 1, "notBreaching") for m in PG_MEMBERS]),
    _a("XtnlDisk70", "disk 70 %", "warning", "max by (node, mount) (xtnl_disk_used_ratio) > 0.70", "5m",
       "Disk above 70 % on an engine node", "xtnl_disk_used_ratio",
       lambda: [CwAlarm(f"xtnl-disk-70-{n}{mt.replace('/', '-').rstrip('-') or '-root'}", f"{mt} on {n} > 70 %",
                        {"m0": stat("xtnl_disk_used_ratio", dims(node=n, mount=mt), period=60)}, "m0",
                        "GreaterThanThreshold", 0.70, 5) for n in NODES for mt in MOUNTS]),
    _a("XtnlDisk85", "disk 85 %", "critical", "max by (node, mount) (xtnl_disk_used_ratio) > 0.85", "5m",
       "Disk above 85 % on an engine node", "xtnl_disk_used_ratio",
       lambda: [CwAlarm(f"xtnl-disk-85-{n}{mt.replace('/', '-').rstrip('-') or '-root'}", f"{mt} on {n} > 85 %",
                        {"m0": stat("xtnl_disk_used_ratio", dims(node=n, mount=mt), period=60)}, "m0",
                        "GreaterThanThreshold", 0.85, 5) for n in NODES for mt in MOUNTS]),
    _a("XtnlWitnessDisk70", "disk 70 % (witness)", "warning",
       f'1 - node_filesystem_avail_bytes{{job="witness-node", mountpoint=~"{_WITNESS_RE}"}} / '
       f'node_filesystem_size_bytes{{job="witness-node", mountpoint=~"{_WITNESS_RE}"}} > 0.70', "5m",
       "Disk above 70 % on the witness (or its disk metric is missing)",
       "node_filesystem_avail_bytes node_filesystem_size_bytes",
       # missing data breaches: the witness has no exporter-down alarm, so a silent agent must show up here
       lambda: [_witness_disk(mt, 70, "breaching") for mt in WITNESS_MOUNTS]),
    _a("XtnlWitnessDisk85", "disk 85 % (witness)", "critical",
       f'1 - node_filesystem_avail_bytes{{job="witness-node", mountpoint=~"{_WITNESS_RE}"}} / '
       f'node_filesystem_size_bytes{{job="witness-node", mountpoint=~"{_WITNESS_RE}"}} > 0.85', "5m",
       "Disk above 85 % on the witness", "node_filesystem_avail_bytes node_filesystem_size_bytes",
       lambda: [_witness_disk(mt, 85, "missing") for mt in WITNESS_MOUNTS]),
    # ---- runtime / credentials (operations.md §3, §9, §13; tier-persistence.md §1) -------------------------
    _a("XtnlCertExpiry", "certificate expiry alert at 30 days", "warning",
       "min by (node, cert) (xtnl_cert_expires_in_seconds) < 30 * 86400", "0s",
       "A TLS certificate expires within 30 days", "xtnl_cert_expires_in_seconds",
       lambda: [CwAlarm(f"xtnl-cert-expiry-{n}-{c}", f"{c} on {n} expires within 30 days",
                        {"m0": stat("xtnl_cert_expires_in_seconds", dims(node=n, cert=c), "Minimum", 300)}, "m0",
                        "LessThanThreshold", 30 * 86400, 1) for n in NODES for c in CERTS]),
    # K5: a configured certificate the node cannot read (or, for an endpoint, that does not verify: expired,
    # untrusted, misnamed) is alerted on, and so is a node that exports fewer than the required certificates
    # (one left out of XTNL_METRICS_CERTS / _ENDPOINTS). CloudWatch: missing data breaches after 15 min, but a node
    # that is down (planned stop or loss) already raises XtnlEngineDown: each per-cert alarm notifies through a
    # composite suppressed by xtnl-engine-down-<node>, so a down node is one page, not one per certificate.
    _a("XtnlCertUnreadable", "unreadable-certificate alert", "warning",
       f'min by (node, cert) (xtnl_cert_readable) == 0 or (max by (node) (xtnl_exporter_up) unless on (node) '
       f'count by (node) (xtnl_cert_readable{{cert=~"{"|".join(CERTS)}"}}) >= {len(CERTS)})', "15m",
       "A TLS certificate is unreadable, does not verify, or is not watched on this node", "xtnl_cert_readable xtnl_exporter_up",
       lambda: [CwAlarm(f"xtnl-cert-unreadable-{n}-{c}", f"{c} on {n} unreadable, not verifying or not exported",
                        {"m0": stat("xtnl_cert_readable", dims(node=n, cert=c), "Minimum", 300)}, "m0",
                        "LessThanThreshold", 1, 3, missing="breaching", suppressed_by=f"xtnl-engine-down-{n}")
                for n in NODES for c in CERTS]),
    _a("XtnlEventLoopLag", "event-loop lag alert at 100 ms", "warning", "max by (node) (xtnl_event_loop_lag_seconds) > 0.1",
       "30s", "Engine event loop lagging more than 100 ms (watchdog kills at 3 s)", "xtnl_event_loop_lag_seconds",
       lambda: per_node(lambda n: CwAlarm(
           f"xtnl-event-loop-lag-{n}", f"Event-loop lag on {n} > 100 ms",
           {"m0": stat("xtnl_event_loop_lag_seconds", dims(node=n))}, "m0", "GreaterThanThreshold", 0.1, 3))),
    _a("XtnlRedisMemoryHigh", "Redis memory alert before the limit (noeviction)", "warning",
       "max by (node) (xtnl_redis_used_memory_ratio) > 0.8", "2m",
       "Redis above 80 % of maxmemory (noeviction: writes fail at the limit)", "xtnl_redis_used_memory_ratio",
       lambda: per_node(lambda n: CwAlarm(
           f"xtnl-redis-memory-{n}", f"Redis on {n} > 80 % of maxmemory",
           {"m0": stat("xtnl_redis_used_memory_ratio", dims(node=n), period=60)}, "m0", "GreaterThanThreshold",
           0.8, 2))),
    _a("XtnlFeedLengthHigh", "oversee:feed lag alert at 50 % of the MAXLEN cap", "warning",
       "max(xtnl_feed_stream_length) > 500000", "1m", "oversee:feed holds more than 50 % of its 1,000,000 cap",
       "xtnl_feed_stream_length",
       lambda: [any_of("xtnl-feed-length-high", "oversee:feed > 50 % of cap", "xtnl_feed_stream_length",
                       [{"node": n} for n in NODES], combine="MAX", op="GreaterThanThreshold", threshold=500_000,
                       periods=6, missing="notBreaching")], dep=True),
    _a("XtnlFeedPendingOld", "oldest un-acked feed entry > 30 s", "warning",
       "max(xtnl_feed_oldest_pending_age_seconds) > 30", "30s",
       "The Oversee feed observer is not acknowledging entries", "xtnl_feed_oldest_pending_age_seconds",
       lambda: [any_of("xtnl-feed-pending-old", "Oldest un-acked feed entry > 30 s",
                       "xtnl_feed_oldest_pending_age_seconds", [{"node": n} for n in NODES], combine="MAX",
                       op="GreaterThanThreshold", threshold=30, periods=3, missing="notBreaching")], dep=True),
    # ---- ARM FAILED backstop (design §B14: needs neither PostgreSQL nor the outbox; absent while the ARM is off) -----
    _a("XtnlArmFailed", "ARM FAILED scopes (arm_failed_scopes) off-node backstop", "critical",
       "max(xtnl_arm_failed_scopes) > 0", "1m",
       "The ARM has a FAILED scope: no advice for its positions until it is cleared (see arm failed show)",
       "xtnl_arm_failed_scopes",
       lambda: [any_of("xtnl-arm-failed", "ARM FAILED scopes > 0", "xtnl_arm_failed_scopes",
                       [{"node": n} for n in NODES], combine="MAX", op="GreaterThanThreshold", threshold=0,
                       periods=6, missing="notBreaching", fill=0)]),
    # ---- redis-md memory (design §A2: pages at 70 %; the off-node copy needs no engine page path) ----------------
    _a("XtnlRedisMdMemoryHigh", "redis-md memory at 70 % of maxmemory (design §A2)", "warning",
       "max by (node) (xtnl_md_redis_used_memory_ratio) > 0.7", "2m",
       "redis-md above 70 % of maxmemory (noeviction; md-writer sheds TICK data in its fixed order)",
       "xtnl_md_redis_used_memory_ratio",
       lambda: per_node(lambda n: CwAlarm(
           f"xtnl-redis-md-memory-{n}", f"redis-md on {n} > 70 % of maxmemory",
           {"m0": stat("xtnl_md_redis_used_memory_ratio", dims(node=n), period=60)}, "m0", "GreaterThanThreshold",
           0.7, 2, "notBreaching"))),
    _a("XtnlClockOffset", "chrony synced within 50 ms (hard check)", "warning",
       "max by (node) (abs(xtnl_clock_offset_seconds)) > 0.05", "1m",
       "Clock offset above 50 ms: the node fails its hard check", "xtnl_clock_offset_seconds",
       lambda: per_node(lambda n: CwAlarm(
           f"xtnl-clock-offset-{n}", f"Clock offset on {n} > 50 ms",
           {"m0": stat("xtnl_clock_offset_seconds", dims(node=n))}, "ABS(m0)", "GreaterThanThreshold", 0.05, 6))),
)

#: operations.md §9 (and the sections it cites) -> the alerts implementing it. The deploy test checks this
#: covers every listed requirement.
SPEC_COVERAGE: dict[str, tuple[str, ...]] = {
    "no LEADER > 30 s": ("XtnlNoLeader",),
    "two LEADERs": ("XtnlTwoLeaders",),
    "etcd unhealthy": ("XtnlEtcdUnhealthy", "XtnlEtcdNoLeader", "XtnlEtcdAlarm"),
    "Patroni unhealthy": ("XtnlPatroniUnhealthy", "XtnlPatroniNoLeader"),
    "sync standby missing": ("XtnlSyncStandbyMissing",),
    "epoch-change rate": ("XtnlEpochChangeRate",),
    "non-empty spill file > 60 s": ("XtnlSpillFileNonEmpty",),
    "standby not ready": ("XtnlEngineDown", "XtnlHealthRecordStale", "XtnlStandbyNotEligible"),
    "leader not on the primary's node > 10 min": ("XtnlLeaderNotOnPrimary",),
    "etcd quota 50 %": ("XtnlEtcdQuota50",),
    "disk 70 / 85 %": ("XtnlDisk70", "XtnlDisk85", "XtnlWitnessDisk70", "XtnlWitnessDisk85"),
    "certificate expiry": ("XtnlCertExpiry", "XtnlCertUnreadable"),
    "PITR horizon broken": ("XtnlPitrHorizonBroken",),
    "SAFE_HOLD off-node page": ("XtnlSafeHold",),
    "lease-renew p99 > 500 ms": ("XtnlLeaseRenewSlow",),
    "event-loop lag 100 ms": ("XtnlEventLoopLag",),
    "Redis memory before the limit": ("XtnlRedisMemoryHigh",),
    "feed lag 50 % of the cap": ("XtnlFeedLengthHigh", "XtnlFeedPendingOld"),
    "inhibition: epoch change, RTO + 30 s": ("XtnlEpochChangeSettling",),
    "ARM FAILED scopes (design §B14 backstop)": ("XtnlArmFailed",),
    "redis-md memory 70 % (design §A2)": ("XtnlRedisMdMemoryHigh",),
}


# ============================================================================================ rendering
def _q(s: str) -> str:
    """A YAML double-quoted scalar."""
    return json.dumps(s, ensure_ascii=False)


def render_prometheus() -> str:
    lines = ["# GENERATED by deploy/observability/gen_alerts.py - do not edit by hand.",
             "# Off-node alerting rules (operations.md §9). Phase 1 never sends orders: these only page people.",
             "# Load into Amazon Managed Service for Prometheus (rule groups namespace) or a self-run Prometheus;",
             "# Alertmanager (alertmanager/alertmanager.yml) routes them to SNS. Metrics: metrics.md.",
             "groups:",
             "  - name: xtnl-offnode",
             "    interval: 10s",
             "    rules:"]
    for a in ALERTS:
        lines += [f"      - alert: {a.name}",
                  f"        expr: {_q(a.expr)}",
                  f"        for: {a.for_}",
                  "        labels:",
                  f"          severity: {a.severity}",
                  "          team: xtnl",
                  f"          depends_on_epoch: {_q('true' if a.depends_on_epoch else 'false')}",
                  "        annotations:",
                  f"          summary: {_q(a.summary)}",
                  f"          spec: {_q(a.spec)}",
                  f"          runbook: {_q(RUNBOOK + '#' + a.name.lower())}"]
    return "\n".join(lines) + "\n"


def _logical_id(name: str) -> str:
    return "".join(p.capitalize() for p in name.replace("/", "-").split("-") if p)


def render_cloudwatch() -> str:
    settling = next(a for a in ALERTS if a.name == "XtnlEpochChangeSettling").cloudwatch()[0].name
    resources: dict[str, Any] = {}
    for a in ALERTS:
        for cw in a.cloudwatch():
            lid = _logical_id(cw.name)
            assert lid not in resources, lid
            direct = not a.depends_on_epoch and a.severity != "none" and cw.suppressed_by is None
            resources[lid] = {"Type": "AWS::CloudWatch::Alarm", "Properties": cw.properties(a.severity, direct)}
            if cw.suppressed_by is not None:
                assert not a.depends_on_epoch, cw.name     # one suppressor per composite
                topic = {"Ref": "SnsCriticalTopicArn" if a.severity == "critical" else "SnsWarningTopicArn"}
                resources[lid + "Notify"] = {
                    "Type": "AWS::CloudWatch::CompositeAlarm",
                    "DependsOn": [lid, _logical_id(cw.suppressed_by)],
                    "Properties": {
                        "AlarmName": cw.name + "-notify",
                        "AlarmDescription": f"{cw.description} (held back while {cw.suppressed_by} is in ALARM)",
                        "AlarmRule": f'ALARM("{cw.name}")',
                        "ActionsSuppressor": cw.suppressed_by,
                        # the per-cert alarm breaches after 15 min of missing data, long after engine-down (60 s);
                        # after recovery the first good 300 s datapoint may take up to 5 min to clear it
                        "ActionsSuppressorWaitPeriod": 60,
                        "ActionsSuppressorExtensionPeriod": 600,
                        "AlarmActions": [topic], "OKActions": [topic]}}
                continue
            if a.depends_on_epoch:
                topic = {"Ref": "SnsCriticalTopicArn" if a.severity == "critical" else "SnsWarningTopicArn"}
                resources[lid + "Notify"] = {
                    "Type": "AWS::CloudWatch::CompositeAlarm",
                    "DependsOn": [lid, _logical_id(settling)],
                    "Properties": {
                        "AlarmName": cw.name + "-notify",
                        "AlarmDescription": f"{cw.description} (held back for {RTO_PLUS_30_S} s after an epoch change)",
                        "AlarmRule": f'ALARM("{cw.name}")',
                        "ActionsSuppressor": settling,
                        "ActionsSuppressorWaitPeriod": 60,
                        "ActionsSuppressorExtensionPeriod": 30,
                        "AlarmActions": [topic], "OKActions": [topic]}}
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "XTNL Phase 1 off-node alarms (operations.md s9), equivalent to "
                       "prometheus/xtnl-alerts.rules.yml. GENERATED by deploy/observability/gen_alerts.py.",
        "Parameters": {
            "SnsCriticalTopicArn": {"Type": "String", "Description": "SNS topic -> phone (critical pages)"},
            "SnsWarningTopicArn": {"Type": "String", "Description": "SNS topic -> email / chat (warnings)"}},
        "Resources": resources}
    return json.dumps(template, indent=1, sort_keys=False) + "\n"


def render_agent() -> str:
    decls = []
    for m in DEFINED_METRICS:       # a pending metric's selector matches nothing until it is exported
        decls.append({"source_labels": ["job"], "label_matcher": f"^{SCRAPE_JOB}$", "dimensions": [list(m.labels)],
                      "metric_selectors": [f"^{m.name}$"]})
    files = [{"file_path": f"{LOG_DIR}/{ident}.log", "log_group_name": group, "log_stream_name": "{hostname}/" + ident,
              "retention_in_days": days, "timezone": "UTC"} for ident, group, days in LOG_FILES]
    cfg = {"agent": {"run_as_user": "cwagent"},
           "logs": {"logs_collected": {"files": {"collect_list": files}},
                    "force_flush_interval": 5,
                    "metrics_collected": {"prometheus": {
                        "prometheus_config_path": CW_SCRAPE_INSTALLED,
                        "emf_processor": {"metric_namespace": NAMESPACE, "metric_declaration": decls}}}},
           "_comment": f"GENERATED by deploy/observability/gen_alerts.py; scrape job {SCRAPE_JOB} on :{SCRAPE_PORT} "
                       f"every {SCRAPE_INTERVAL_S} s (cloudwatch/xtnl-prometheus.yaml); logs from {LOG_DIR}"}
    return json.dumps(cfg, indent=1) + "\n"


def render_witness_agent() -> str:
    """CloudWatch agent config for the witness: disk_used_percent with dimensions exactly {host, path}
    (`omit_hostname` drops the instance hostname, the disk plugin appends host=witness, `drop_device` drops the
    device, and `aggregation_dimensions` publishes the {host, path} series the alarms read), plus its log lines."""
    files = [{"file_path": f"{LOG_DIR}/{ident}.log", "log_group_name": group, "log_stream_name": "{hostname}/" + ident,
              "retention_in_days": days, "timezone": "UTC"} for ident, group, days in LOG_FILES
             if ident in WITNESS_LOG_IDENTS]
    cfg = {"agent": {"run_as_user": "cwagent", "omit_hostname": True, "metrics_collection_interval": 60},
           "metrics": {"namespace": "CWAgent",
                       "metrics_collected": {"disk": {
                           "measurement": ["used_percent"], "resources": list(WITNESS_MOUNTS), "drop_device": True,
                           "ignore_file_system_types": ["tmpfs", "devtmpfs", "overlay", "squashfs"],
                           "append_dimensions": {"host": WITNESS_HOST}}},
                       "aggregation_dimensions": [["host", "path"]]},
           "logs": {"logs_collected": {"files": {"collect_list": files}}, "force_flush_interval": 5},
           "_comment": "GENERATED by deploy/observability/gen_alerts.py; witness only (no engine, no exporter): "
                       f"disk_used_percent of {', '.join(WITNESS_MOUNTS)} for xtnl-witness-disk-*; logs from {LOG_DIR}"}
    return json.dumps(cfg, indent=1) + "\n"


def render_scrape() -> str:
    """Prometheus scrape config for the CloudWatch agent (it fills the `node` label from the exporter itself)."""
    return "\n".join((
        "# GENERATED by deploy/observability/gen_alerts.py. Install on each engine node as",
        f"#     {CW_SCRAPE_INSTALLED}",
        "# rendered with envsubst (NODE_IP = this node's VPC address, the host part of XTNL_METRICS_ADDR).",
        "global:",
        f"  scrape_interval: {SCRAPE_INTERVAL_S}s",
        f"  scrape_timeout: {SCRAPE_INTERVAL_S - 1}s",
        "scrape_configs:",
        f"  - job_name: {SCRAPE_JOB}",
        "    honor_labels: true",
        "    sample_limit: 20000",
        "    static_configs:",
        f"      - targets: ['${{NODE_IP}}:{SCRAPE_PORT}']",
        "",
    ))


OUTPUTS: dict[Path, Callable[[], str]] = {PROM_RULES: render_prometheus, CW_ALARMS: render_cloudwatch,
                                          CW_AGENT: render_agent, CW_SCRAPE: render_scrape,
                                          CW_WITNESS_AGENT: render_witness_agent}


def main(argv: Sequence[str]) -> int:
    check = "--check" in argv
    stale = []
    for path, render in OUTPUTS.items():
        text = render()
        if check:
            if not path.exists() or path.read_text(encoding="utf-8") != text:
                stale.append(path.name)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")
    if stale:
        print("out of date: " + ", ".join(stale) + " (run python deploy/observability/gen_alerts.py)")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
