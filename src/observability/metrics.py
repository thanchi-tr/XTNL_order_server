"""Metrics registry + Prometheus text exposition (operations.md §9 Metrics; IMPLEMENTATION_PLAN 5.6).

The contract is `deploy/observability/metrics.md` (`gen_alerts.METRICS`): the engine exports **exactly** those
names, types and labels (tests/observability checks that `CONTRACT` equals it), every series carries `node`,
`xtnl_exporter_up` is always 1, and a value the node cannot read right now is **absent** (never NaN, never a
fake 0).

Values are pulled at scrape time: a `Collector` is a callable returning `Sample`s from in-memory state (no I/O
on the scrape path; periodic I/O readers cache their last result, `src/observability/collectors.py`). A
collector that raises, or a sample that breaks the contract (unknown name, wrong labels, non-finite value),
is dropped and logged, never exported. Counters are cumulative values owned by their component
(`CounterVec`). Standard library only.
"""
from __future__ import annotations

import logging
import math
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Final, Literal

log = logging.getLogger(__name__)

MetricType = Literal["gauge", "counter"]

#: deploy/observability/metrics.md, one row per metric: (name, type, labels without `node`).
CONTRACT: Final[Mapping[str, tuple[MetricType, tuple[str, ...]]]] = {
    "xtnl_exporter_up": ("gauge", ()),
    "xtnl_node_role": ("gauge", ("role",)),
    "xtnl_engine_epoch": ("gauge", ()),
    "xtnl_epoch_change_age_seconds": ("gauge", ()),
    "xtnl_epoch_changes_30m": ("gauge", ()),
    "xtnl_safe_hold": ("gauge", ()),
    "xtnl_leader_colocated_with_primary": ("gauge", ()),
    "xtnl_lease_renew_p99_seconds": ("gauge", ()),
    "xtnl_lease_remaining_seconds": ("gauge", ()),
    "xtnl_health_record_age_seconds": ("gauge", ("subject",)),
    "xtnl_health_hard_checks_ok": ("gauge", ("subject",)),
    "xtnl_health_campaign_eligible": ("gauge", ("subject",)),
    "xtnl_health_spill_file_bytes": ("gauge", ("subject",)),
    "xtnl_etcd_endpoint_healthy": ("gauge", ("member",)),
    "xtnl_etcd_has_leader": ("gauge", ()),
    "xtnl_etcd_db_quota_ratio": ("gauge", ("member",)),
    "xtnl_etcd_alarms_active": ("gauge", ()),
    "xtnl_patroni_member_running": ("gauge", ("member",)),
    "xtnl_patroni_has_leader": ("gauge", ()),
    "xtnl_patroni_sync_standby_count": ("gauge", ()),
    "xtnl_pg_replication_lag_seconds": ("gauge", ("member",)),
    "xtnl_pg_timeline": ("gauge", ("member",)),
    "xtnl_wal_archive_failed_total": ("counter", ("member",)),
    "xtnl_wal_archive_age_seconds": ("gauge", ("member",)),
    "xtnl_disk_used_ratio": ("gauge", ("mount",)),
    "xtnl_redis_used_memory_ratio": ("gauge", ()),
    "xtnl_feed_stream_length": ("gauge", ()),
    "xtnl_feed_pending_entries": ("gauge", ()),
    "xtnl_feed_oldest_pending_age_seconds": ("gauge", ()),
    "xtnl_ingest_events_total": ("counter", ("adapter", "stream")),
    "xtnl_frames_lost_total": ("counter", ("stream",)),
    "xtnl_bus_queue_depth": ("gauge", ("topic",)),
    "xtnl_dlq_spool_ring_depth": ("gauge", ()),
    "xtnl_dlq_rows": ("gauge", ("status",)),
    "xtnl_adapter_state": ("gauge", ("adapter", "state")),
    "xtnl_rest_bucket_tokens": ("gauge", ("adapter",)),
    "xtnl_rest_requests_total": ("counter", ("adapter", "priority")),
    "xtnl_risksweep_pass_duration_seconds": ("gauge", ()),
    "xtnl_risksweep_oldest_pass_age_seconds": ("gauge", ()),
    "xtnl_event_loop_lag_seconds": ("gauge", ()),
    "xtnl_clock_offset_seconds": ("gauge", ()),
    "xtnl_token_age_seconds": ("gauge", ("adapter",)),
    "xtnl_token_expires_in_seconds": ("gauge", ("adapter",)),
    "xtnl_cert_expires_in_seconds": ("gauge", ("cert",)),
    # engine uptime + downtime catch-up (migration 0012; src/runtime/uptime.py, src/runtime/catchup.py)
    "xtnl_engine_uptime_seconds": ("gauge", ()),
    "xtnl_suspended_gaps_total": ("counter", ()),
    "xtnl_offline_gap_seconds": ("gauge", ()),
    "xtnl_catchup_runs_total": ("counter", ("adapter", "complete")),
    "xtnl_catchup_rows_total": ("counter", ("adapter", "kind")),
}

HELP: Final[Mapping[str, str]] = {
    "xtnl_exporter_up": "1 while the exporter answers",
    "xtnl_node_role": "1 for the node's current SM9 role",
    "xtnl_engine_epoch": "engine_epoch fencing token as last read from PostgreSQL",
}


@dataclass(frozen=True)
class Sample:
    name: str
    value: float
    labels: Mapping[str, str] = field(default_factory=dict)


Collector = Callable[[], Iterable[Sample]]


class CounterVec:
    """A cumulative counter family owned by one component (thread-safe increments, monotonic values)."""

    def __init__(self, name: str) -> None:
        if name not in CONTRACT or CONTRACT[name][0] != "counter":
            raise ValueError(f"{name} is not a counter of the metrics contract")
        self.name = name
        self._lock = threading.Lock()
        self._values: dict[tuple[tuple[str, str], ...], float] = {}

    def inc(self, amount: float = 1.0, **labels: str) -> None:
        if amount < 0:
            raise ValueError("a counter only increases")
        key = tuple(sorted(labels.items()))
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def value(self, **labels: str) -> float:
        with self._lock:
            return self._values.get(tuple(sorted(labels.items())), 0.0)

    def samples(self) -> list[Sample]:
        with self._lock:
            return [Sample(self.name, v, dict(k)) for k, v in sorted(self._values.items())]


def _escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _fmt(v: float) -> str:
    if v == int(v) and abs(v) < 1e15:
        return str(int(v))
    return repr(float(v))


class MetricsRegistry:
    """Collectors -> one exposition. `node` is added to every series here; collectors never set it."""

    def __init__(self, node: str) -> None:
        self.node = node
        self._collectors: list[tuple[str, Collector]] = []
        self.dropped = 0

    def register(self, name: str, collector: Collector) -> None:
        self._collectors.append((name, collector))

    def unregister(self, name: str) -> None:
        self._collectors = [(n, c) for n, c in self._collectors if n != name]

    def _valid(self, s: Sample) -> bool:
        spec = CONTRACT.get(s.name)
        if spec is None:
            return False
        if set(s.labels) != set(spec[1]) or any(not isinstance(v, str) for v in s.labels.values()):
            return False
        return isinstance(s.value, (int, float)) and not isinstance(s.value, bool) and math.isfinite(s.value)

    def collect(self) -> list[Sample]:
        out: list[Sample] = [Sample("xtnl_exporter_up", 1.0)]
        seen: set[tuple[str, tuple[tuple[str, str], ...]]] = {("xtnl_exporter_up", ())}
        for cname, collector in list(self._collectors):
            try:
                samples = list(collector())
            except Exception as exc:
                self.dropped += 1
                log.warning("metrics collector %s failed: %s", cname, type(exc).__name__)
                continue
            for s in samples:
                if not self._valid(s):
                    self.dropped += 1
                    log.debug("metrics: dropped invalid sample %s %s from %s", s.name, dict(s.labels), cname)
                    continue
                key = (s.name, tuple(sorted(s.labels.items())))
                if key in seen:
                    continue                          # the first collector for a series wins
                seen.add(key)
                out.append(s)
        return out

    def render(self) -> str:
        """Prometheus text exposition 0.0.4."""
        by_name: dict[str, list[Sample]] = {}
        for s in self.collect():
            by_name.setdefault(s.name, []).append(s)
        lines: list[str] = []
        for name in sorted(by_name):
            kind = CONTRACT[name][0]
            if name in HELP:
                lines.append(f"# HELP {name} {HELP[name]}")
            lines.append(f"# TYPE {name} {kind}")
            for s in by_name[name]:
                labels = {"node": self.node, **s.labels}
                body = ",".join(f'{k}="{_escape(v)}"' for k, v in labels.items())
                lines.append(f"{name}{{{body}}} {_fmt(s.value)}")
        return "\n".join(lines) + "\n"


def gauge(name: str, value: float | int | bool | None, **labels: str) -> list[Sample]:
    """One gauge sample, or none when the value is unknown (absence, never a fake 0)."""
    if value is None:
        return []
    return [Sample(name, float(value), labels)]


__all__ = ["CONTRACT", "Collector", "CounterVec", "MetricType", "MetricsRegistry", "Sample", "gauge"]
