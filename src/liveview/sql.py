"""Every statement the live view runs (docs/design/live-view.md s4.2-s4.5).

Each statement is a bounded `SELECT` from an lv_ view of migration 0021, or `SELECT now_ms()`. `ALL_SQL` lists them
all and `ALL_VIEWS` is the set of views they read; tests/db/test_liveview_role_migration.py keeps that set equal to
the 0021 grant list. No value from a URL ever reaches a statement: parameters are ids and times this process holds.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Final

SQL_NOW: Final = "SELECT now_ms()"

ENTITY_COLS: Final = ("log_id, seq_id, entity_type, entity_key, adapter_id, account_id, action, logged_at_ms, "
                      "version_path, broker_ms, facts")
OVERSEE_COLS: Final = ("event_id, sweep_id, rule_id, subject_type, subject_id, transition, adapter_id, account_id, "
                       "level, cap, reason_code, at_ms, activation_id")
HALT_COLS: Final = ("id, level, effective_level, hard_latch, rule_id, reason_code, by_whom, at_ms, reviewed_event_id, "
                    "forced")
ALERT_COLS: Final = ("outbox_id, created_at_ms, severity, kind, info_class, account_id, delivery, escalated, attempts, "
                     "sent_at_ms, delivered_at_ms, ep_rule_id, ep_subject_type, ep_subject_id, ep_activation_id, "
                     "ep_level, tr_transition, tr_rule_id, tr_subject_type, tr_subject_id")
OPERATOR_COLS: Final = "audit_id, at_ms, command, target_id, operator, approver, outcome, epoch"
DLQ_COLS: Final = ("dlq_id, origin_dlq_id, event_type, entity_id, adapter_id, dlq_reason, retry_count, status, "
                   "parked_at_ms, updated_at_ms")
CATCHUP_COLS: Final = ("id, run_id::text AS run_id, epoch, adapter_id, reason, gap_start_ms, gap_end_ms, "
                       "covered_from_ms, offline_from_ms, started_at_ms, finished_at_ms, orders_seen, orders_new, "
                       "orders_updated, positions_seen, positions_new, positions_closed, cash_flows_new, anomalies, "
                       "complete")
TERM_COLS: Final = ("term_id, cluster_id, epoch_revision, node, started_at, reached_leader_at, ended_at, end_reason")
RUN_COLS: Final = "run_id::text AS run_id, node_id, started_at_ms, last_seen_ms, stopped_at_ms, end_code, suspended_gaps"
STREAM_COLS: Final = "adapter_id, stream, conn_lost_at, reconciled_at, reconciled_epoch, updated_at_ms"
CLUSTER_COLS: Final = ("safe_hold, safe_hold_since, hold_code, hold_count, last_resume_at, resumed_by, updated_at_ms")
EPOCH_COLS: Final = "cluster_id, revision, epoch, node, bumped_at_ms"
BREACH_COLS: Final = ("breach_id, rule_id, subject_type, subject_id, adapter_id, account_id, level, pending_level, cap, "
                      "reason_code, activation_id, pending_since, active_since, release_pending_since, unknown_since, "
                      "uncapped_ms, last_pass_at_ms, below_soft_since, resolved_at, resolved_by, updated_at_ms, acked")
ORDER_COLS: Final = ("adapter_id, broker_order_id, client_order_id, account_id, symbol, side, status, entry_level, "
                     "stop_level, target_level, is_reduce_only, linked_position_id, leg_kind, broker_updated_at_ms, "
                     "committed_at_ms")
POSITION_COLS: Final = ("adapter_id, position_id, account_id, symbol, side, entry_level, stop_level, target_level, "
                        "opened_by_client_order_id, broker_updated_at_ms, committed_at_ms")
TIMING_COLS: Final = "ruleset_version, escalate_after_ms, cool_down_ms, unknown_grace_ms"
QUARANTINE_COLS: Final = ("adapter_id, account_id, failing_since_ms, quarantined_at_ms, next_retry_at_ms, "
                          "released_at_ms")
AUTH_COLS: Final = "adapter_id, state, failed_at_ms, rearmed_at_ms"

PAGE_ROWS: Final = 500
MAX_PAGES: Final = 10


@dataclass(frozen=True)
class TailSource:
    """One append-only view tailed by its database-issued id `key`; `prune` names the partition-pruning column and
    `shift` how a millisecond time maps onto it (20 for the HLC columns, 0 for plain ms), or None (no pruning)."""

    name: str
    view: str
    key: str
    cols: str
    prune: str | None
    shift: int
    backfill_time: str      # the column (ms, or HLC when shift is 20) the backfill window is read by

    def _where_prune(self, n: int) -> str:
        return f" AND {self.prune} >= ${n}" if self.prune else ""

    @property
    def sql_max_pruned(self) -> str:
        return f"SELECT max({self.key}) FROM {self.view} WHERE {self.prune} >= $1 LIMIT 1" if self.prune else \
            self.sql_max

    @property
    def sql_max(self) -> str:
        return f"SELECT max({self.key}) FROM {self.view} LIMIT 1"

    @property
    def sql_present(self) -> str:
        """Ids present in ($1, $2] (start: in-flight transactions)."""
        return (f"SELECT {self.key} FROM {self.view} WHERE {self.key} > $1 AND {self.key} <= $2"
                f"{self._where_prune(3)} ORDER BY {self.key} LIMIT 1001")

    @property
    def sql_tail(self) -> str:
        """($1 = H, $2 = pending ids, $3 = last id of the previous page, $4 = prune bound when pruned)."""
        return (f"SELECT {self.cols} FROM {self.view} WHERE ({self.key} > $1 OR {self.key} = ANY($2::bigint[])) "
                f"AND {self.key} > $3{self._where_prune(4)} ORDER BY {self.key} LIMIT {PAGE_ROWS}")

    @property
    def sql_recheck(self) -> str:
        return (f"SELECT {self.cols} FROM {self.view} WHERE {self.key} = ANY($1::bigint[]){self._where_prune(2)} "
                f"ORDER BY {self.key} LIMIT 10000")

    @property
    def sql_backfill(self) -> str:
        """($1 = H, $2 = window start on backfill_time, $3 = row limit)."""
        return (f"SELECT {self.cols} FROM {self.view} WHERE {self.key} <= $1 AND {self.backfill_time} >= $2 "
                f"ORDER BY {self.key} DESC LIMIT $3")

    def statements(self) -> tuple[str, ...]:
        return (self.sql_max_pruned, self.sql_max, self.sql_present, self.sql_tail, self.sql_recheck,
                self.sql_backfill)


ENTITY: Final = TailSource("entity", "lv_entity_events", "log_id", ENTITY_COLS, "seq_id", 20, "seq_id")
OVERSEE: Final = TailSource("oversee", "lv_oversee_events", "event_id", OVERSEE_COLS, "sweep_id", 20, "sweep_id")
ALERTS: Final = TailSource("alerts", "lv_alerts", "outbox_id", ALERT_COLS, "created_at_ms", 0, "created_at_ms")
HALT: Final = TailSource("halt", "lv_halt_events", "id", HALT_COLS, None, 0, "at_ms")
OPERATOR: Final = TailSource("operator", "lv_operator_actions", "audit_id", OPERATOR_COLS, None, 0, "at_ms")
DLQ: Final = TailSource("dlq", "lv_dlq", "dlq_id", DLQ_COLS, None, 0, "updated_at_ms")
CATCHUPS: Final = TailSource("catchups", "lv_catchups", "id", CATCHUP_COLS, None, 0, "finished_at_ms")
TERMS: Final = TailSource("terms", "lv_leader_terms", "term_id", TERM_COLS, None, 0, "started_at")

#: Narration order within a poll (s4.3 "Order within a poll"): terms, then catch-ups, then the others.
TAIL_SOURCES: Final = (TERMS, CATCHUPS, ENTITY, OVERSEE, HALT, OPERATOR, DLQ, ALERTS)

# ---- watched sets (rows changed in place) ----------------------------------------------------------------------
SQL_ALERTS_WATCHED: Final = (f"SELECT {ALERT_COLS} FROM lv_alerts WHERE outbox_id = ANY($1::bigint[]) "
                             "AND created_at_ms >= $2 LIMIT 10000")
SQL_ALERTS_SEED: Final = (f"SELECT {ALERT_COLS} FROM lv_alerts WHERE delivery IN ('queued', 'retrying') "
                          "AND kind <> 'arm' AND created_at_ms >= $1 ORDER BY outbox_id LIMIT 10000")
SQL_DLQ_WATCHED: Final = f"SELECT {DLQ_COLS} FROM lv_dlq WHERE dlq_id = ANY($1::bigint[]) LIMIT 10000"
SQL_DLQ_SEED: Final = (f"SELECT {DLQ_COLS} FROM lv_dlq WHERE status IN ('QUEUED', 'LEASED', 'BACKOFF', 'QUARANTINED') "
                       "ORDER BY dlq_id LIMIT 10000")
SQL_TERMS_WATCHED: Final = (f"SELECT {TERM_COLS} FROM lv_leader_terms WHERE term_id = ANY($1::bigint[]) "
                            "LIMIT 10000")
SQL_TERMS_SEED: Final = f"SELECT {TERM_COLS} FROM lv_leader_terms WHERE ended_at IS NULL ORDER BY term_id LIMIT 1000"
SQL_RUNS_DISCOVER: Final = f"SELECT {RUN_COLS} FROM lv_engine_runs ORDER BY started_at_ms DESC LIMIT 50"

# ---- re-read whole each poll (diff sources) --------------------------------------------------------------------
SQL_STREAMS: Final = f"SELECT {STREAM_COLS} FROM lv_stream_health ORDER BY adapter_id, stream LIMIT 1000"
SQL_CLUSTER: Final = f"SELECT {CLUSTER_COLS} FROM lv_cluster_state LIMIT 1"
SQL_EPOCH: Final = f"SELECT {EPOCH_COLS} FROM lv_engine_epoch LIMIT 1"
SQL_QUARANTINE: Final = f"SELECT {QUARANTINE_COLS} FROM lv_sweep_quarantine ORDER BY adapter_id, account_id LIMIT 1000"
SQL_AUTH: Final = f"SELECT {AUTH_COLS} FROM lv_auth_state ORDER BY adapter_id LIMIT 1000"

# ---- StateReader ------------------------------------------------------------------------------------------------
SQL_HALT_LATEST: Final = f"SELECT {HALT_COLS} FROM lv_halt_events ORDER BY id DESC LIMIT 1"
SQL_HALT_BEFORE: Final = f"SELECT {HALT_COLS} FROM lv_halt_events WHERE at_ms < $1 ORDER BY id DESC LIMIT 1"
SQL_BREACHES_OPEN: Final = (f"SELECT {BREACH_COLS} FROM lv_breaches WHERE resolved_at IS NULL "
                            "AND (level IS NOT NULL OR pending_level IS NOT NULL) ORDER BY breach_id LIMIT 1000")
SQL_OPEN_ORDERS: Final = f"SELECT {ORDER_COLS} FROM lv_open_orders ORDER BY adapter_id, broker_order_id LIMIT 500"
SQL_OPEN_POSITIONS: Final = (f"SELECT {POSITION_COLS} FROM lv_open_positions ORDER BY adapter_id, position_id "
                             "LIMIT 500")
SQL_TIMING: Final = f"SELECT {TIMING_COLS} FROM lv_ruleset_timing ORDER BY ruleset_version DESC LIMIT 1"
SQL_ALERTS_WAITING: Final = ("SELECT count(*) AS waiting, min(created_at_ms) AS oldest_ms FROM lv_alerts "
                             "WHERE delivery IN ('queued', 'retrying') AND created_at_ms >= $1 LIMIT 1")
SQL_ALERTS_TODAY: Final = ("SELECT count(*) FILTER (WHERE delivery = 'undeliverable') AS undeliverable, "
                           "count(*) FILTER (WHERE escalated) AS escalated_today "
                           "FROM lv_alerts WHERE created_at_ms >= $1 LIMIT 1")
SQL_DLQ_COUNTS: Final = "SELECT status, count(*) AS n FROM lv_dlq GROUP BY status ORDER BY status LIMIT 10"
SQL_TERM_LATEST: Final = f"SELECT {TERM_COLS} FROM lv_leader_terms ORDER BY term_id DESC LIMIT 1"

ALL_SQL: Final = (SQL_NOW, *(s for src in TAIL_SOURCES for s in src.statements()),
                  SQL_ALERTS_WATCHED, SQL_ALERTS_SEED, SQL_DLQ_WATCHED, SQL_DLQ_SEED, SQL_TERMS_WATCHED,
                  SQL_TERMS_SEED, SQL_RUNS_DISCOVER, SQL_STREAMS, SQL_CLUSTER, SQL_EPOCH, SQL_QUARANTINE, SQL_AUTH,
                  SQL_HALT_LATEST, SQL_HALT_BEFORE, SQL_BREACHES_OPEN, SQL_OPEN_ORDERS, SQL_OPEN_POSITIONS, SQL_TIMING,
                  SQL_ALERTS_WAITING, SQL_ALERTS_TODAY, SQL_DLQ_COUNTS, SQL_TERM_LATEST)

ALL_VIEWS: Final = frozenset({
    "lv_entity_events", "lv_oversee_events", "lv_halt_events", "lv_alerts", "lv_operator_actions", "lv_dlq",
    "lv_catchups", "lv_engine_runs", "lv_leader_terms", "lv_stream_health", "lv_cluster_state", "lv_engine_epoch",
    "lv_breaches", "lv_open_orders", "lv_open_positions", "lv_ruleset_timing", "lv_sweep_quarantine",
    "lv_auth_state"})

__all__ = ["ALERTS", "ALL_SQL", "ALL_VIEWS", "CATCHUPS", "DLQ", "ENTITY", "HALT", "MAX_PAGES", "OPERATOR", "OVERSEE",
           "PAGE_ROWS", "TAIL_SOURCES", "TERMS", "TailSource"]
