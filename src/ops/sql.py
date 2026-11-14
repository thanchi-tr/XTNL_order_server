"""SQL of the operator CLI (src/ops). By the fencing convention (src/db/fence.py) `$1` of every fenced
statement is the engine epoch the change is written under; reads and the audit insert are unfenced.
"""
from __future__ import annotations

from typing import Final

from src.db.fence import FENCE
from src.db.operator_requests import SQL_INSERT_WITH_PAYLOAD
from src.db.rows import ACK_COLS

# ---- operator_audit (migration 0007; append-only) -----------------------------------------------------
SQL_AUDIT_INSERT: Final = (
    "INSERT INTO operator_audit (command, target, operator, approver, reason, outcome, detail, epoch, os_user) "
    "VALUES ($1::text, $2::text, $3::text, $4::text, $5::text, $6::text, $7::text::jsonb, $8::bigint, $9::text) "
    "RETURNING audit_id, at_ms"
)
SQL_AUDIT_TAIL: Final = (
    "SELECT audit_id, at_ms, command, target, operator, approver, outcome, reason, epoch, db_user, detail "
    "FROM operator_audit WHERE ($1::text IS NULL OR command = $1::text) ORDER BY audit_id DESC LIMIT $2::int"
)

# ---- SAFE_HOLD (cluster_state, operations.md s4) ------------------------------------------------------
SQL_SAFE_HOLD_RESUME: Final = (
    "UPDATE cluster_state SET safe_hold = false, last_resume_at = now_ms(), resumed_by = $2::text, "
    f"updated_at_ms = now_ms() WHERE id = 1 AND safe_hold AND {FENCE} "
    "RETURNING safe_hold_since, safe_hold_reason, last_resume_at"
)
SQL_SAFE_HOLD_ROW: Final = (
    "SELECT safe_hold, safe_hold_since, safe_hold_reason, last_resume_at, resumed_by, updated_at_ms "
    "FROM cluster_state WHERE id = 1"
)

# ---- adapter_auth_state (re-arm SQL is src/db/auth_state.py SQL_AUTH_REARM) -------------------------------
SQL_AUTH_ROW: Final = (
    "SELECT state, attempts, failed_at_ms, rearmed_by, rearmed_at_ms FROM adapter_auth_state "
    "WHERE adapter_id = $1::text AND credential_generation = $2::text"
)
SQL_AUTH_LIST: Final = (
    "SELECT adapter_id, credential_generation, state, attempts, failed_at_ms, rearmed_by, rearmed_at_ms "
    "FROM adapter_auth_state ORDER BY failed_at_ms DESC LIMIT $1::int"
)

# ---- operator_requests (reset_task) ---------------------------------------------------------------------
SQL_REQUEST_PENDING: Final = (
    "SELECT request_id FROM operator_requests WHERE kind = $1::text AND target = $2::text "
    "AND requested_epoch = $3::bigint AND consumed_at_ms IS NULL ORDER BY request_id LIMIT 1"
)
SQL_REQUEST_INSERT: Final = (
    "INSERT INTO operator_requests (kind, target, audit_id, requested_epoch) "
    f"SELECT $2::text, $3::text, $4::bigint, $1::bigint WHERE {FENCE} RETURNING request_id"
)

# ---- DLQ (tier-persistence.md s5) -----------------------------------------------------------------------
DLQ_LIST_COLS: Final = (
    "dlq_id, event_type, entity_id, adapter_id, error_reason, retry_count, status, next_retry_at_ms, "
    "parked_at_ms, updated_at_ms"
)
SQL_DLQ_COUNTS: Final = "SELECT status, count(*) FROM events_dlq GROUP BY status ORDER BY status"
SQL_DLQ_LIST: Final = (
    f"SELECT {DLQ_LIST_COLS} FROM events_dlq WHERE ($1::text IS NULL OR status = $1::text) "
    "ORDER BY dlq_id DESC LIMIT $2::int"
)
SQL_DLQ_GET: Final = f"SELECT {DLQ_LIST_COLS} FROM events_dlq WHERE dlq_id = $1::bigint"
QUARANTINE_COLS: Final = (
    "quarantine_id, source, dlq_id, feed_entry_id, event_type, entity_id, adapter_id, account_id, error_reason, "
    "retry_count, quarantined_at_ms, released_at_ms, released_by"
)
SQL_QUARANTINE_LIST: Final = (
    f"SELECT {QUARANTINE_COLS} FROM events_quarantine WHERE ($1::boolean OR released_at_ms IS NULL) "
    "ORDER BY quarantine_id DESC LIMIT $2::int"
)
SQL_QUARANTINE_GET: Final = f"SELECT {QUARANTINE_COLS} FROM events_quarantine WHERE quarantine_id = $1::bigint"
# Requeue: a waiting row (QUEUED / BACKOFF) becomes due now; retry_count and lineage are kept. A LEASED row
# belongs to the DLQ worker and is never touched here.
SQL_DLQ_REQUEUE: Final = (
    "UPDATE events_dlq SET status = 'QUEUED', next_retry_at_ms = now_ms(), updated_at_ms = now_ms() "
    f"WHERE dlq_id = $2::bigint AND status IN ('QUEUED', 'BACKOFF') AND {FENCE} RETURNING retry_count"
)
# Release: a QUARANTINED row (after the code fix shipped) is queued again with a fresh retry budget.
SQL_DLQ_UNQUARANTINE: Final = (
    "UPDATE events_dlq SET status = 'QUEUED', retry_count = 0, next_retry_at_ms = now_ms(), lease_owner = NULL, "
    f"lease_until_ms = NULL, updated_at_ms = now_ms() WHERE dlq_id = $2::bigint AND status = 'QUARANTINED' "
    f"AND {FENCE} RETURNING dlq_id"
)
SQL_QUARANTINE_RELEASE: Final = (
    "UPDATE events_quarantine SET released_at_ms = now_ms(), released_by = $3::text, release_audit_id = $4::bigint "
    f"WHERE quarantine_id = $2::bigint AND released_at_ms IS NULL AND {FENCE}"
)

# ---- engine_epoch (operations.md s2, s8) ---------------------------------------------------------------
SQL_EPOCH_ROW: Final = "SELECT cluster_id, revision, epoch, node, bumped_at_ms FROM engine_epoch WHERE id = 1"
SQL_TERMS_RECENT: Final = (
    "SELECT node, cluster_id, epoch_revision, started_at, reached_leader_at, ended_at, end_reason "
    "FROM leader_terms ORDER BY term_id DESC LIMIT $1::int"
)
# Adoption of a new etcd cluster_id: a compare-and-set on the reviewed (cluster_id, revision) under the row
# lock bump_epoch also takes. The revision restarts at 0 for the new cluster_id, so the new cluster's first
# LEADER_INIT bump (same cluster_id, revision > 0) succeeds, and no node of the old cluster (token > 0)
# passes the fence any more.
SQL_EPOCH_ADOPT: Final = (
    "UPDATE engine_epoch SET cluster_id = $1::text, revision = 0, node = $2::text, bumped_at_ms = now_ms() "
    "WHERE id = 1 AND cluster_id = $3::text AND revision = $4::bigint"
)

# ---- halts (oversee-rules.md s4, s8) --------------------------------------------------------------------
SQL_ACCOUNT_KNOWN: Final = (
    "SELECT EXISTS (SELECT 1 FROM account_state WHERE account_id = $1::text) "
    "OR EXISTS (SELECT 1 FROM halt_breaches WHERE account_id = $1::text)"
)

# ---- rulesets (oversee-rules.md s7, operations.md s12) -----------------------------------------------------
# Operator-owned versioned config: the engine only reads it (src/db/ruleset_store.py) and audits what it
# installs, so these writes are not engine state and carry no epoch fence. The table lock serialises
# concurrent operator writes (version numbering, the single active row); engine reads are never blocked.
RULESET_COLS: Final = "ruleset_version, config, active, created_by, reason, created_at_ms"
SQL_RULESET_LOCK: Final = "LOCK TABLE rulesets IN SHARE ROW EXCLUSIVE MODE"
SQL_RULESET_LIST: Final = f"SELECT {RULESET_COLS} FROM rulesets ORDER BY ruleset_version DESC LIMIT $1::int"
SQL_RULESET_GET: Final = f"SELECT {RULESET_COLS} FROM rulesets WHERE ruleset_version = $1::bigint"
SQL_RULESET_ACTIVE: Final = f"SELECT {RULESET_COLS} FROM rulesets WHERE active ORDER BY ruleset_version DESC LIMIT 1"
SQL_RULESET_NEXT_VERSION: Final = "SELECT COALESCE(max(ruleset_version), 0) + 1 FROM rulesets"
SQL_RULESET_INSERT: Final = (
    "INSERT INTO rulesets (ruleset_version, config, active, created_by, reason) "
    "VALUES ($1::bigint, $2::text::jsonb, false, $3::text, $4::text) RETURNING created_at_ms"
)
SQL_RULESET_DEACTIVATE: Final = (
    "UPDATE rulesets SET active = false WHERE active AND ruleset_version <> $1::bigint RETURNING ruleset_version"
)
SQL_RULESET_ACTIVATE: Final = "UPDATE rulesets SET active = true WHERE ruleset_version = $1::bigint AND NOT active"

# ---- halt acknowledge (oversee-rules.md s4 acknowledge(operator, why, breach_id)) --------------------------
SQL_BREACH_BY_ID: Final = (
    "SELECT rule_id, subject_type, subject_id, account_id, resolved_at FROM halt_breaches WHERE breach_id = $1::bigint"
)
SQL_BREACH_ACKS: Final = (
    f"SELECT {ACK_COLS} FROM breach_acks k JOIN halt_breaches b ON b.breach_id = k.breach_id "
    "WHERE k.breach_id = $1::bigint ORDER BY k.ack_id"
)
SQL_BREACH_IDS: Final = (
    "SELECT breach_id, rule_id, subject_type, subject_id FROM halt_breaches WHERE resolved_at IS NULL"
)

# ---- sweep_quarantine (oversee-rules.md s2, s4 release_account) ---------------------------------------------
QUARANTINE_ACCOUNT_COLS: Final = (
    "adapter_id, account_id, failing_since_ms, quarantined_at_ms, reason, released_at_ms, released_by"
)
SQL_SWEEP_QUARANTINE_GET: Final = (
    f"SELECT {QUARANTINE_ACCOUNT_COLS} FROM sweep_quarantine WHERE account_id = $1::text"
)
SQL_SWEEP_QUARANTINE_LIST: Final = (
    f"SELECT {QUARANTINE_ACCOUNT_COLS} FROM sweep_quarantine WHERE ($1::boolean OR released_at_ms IS NULL) "
    "ORDER BY quarantined_at_ms DESC LIMIT $2::int"
)
SQL_SWEEP_QUARANTINE_RELEASE: Final = (
    "UPDATE sweep_quarantine SET released_at_ms = now_ms(), released_by = $3::text "
    f"WHERE account_id = $2::text AND released_at_ms IS NULL AND {FENCE} RETURNING quarantined_at_ms, reason"
)

# ---- accounts ------------------------------------------------------------------------------------------
SQL_ACCOUNT_ADAPTER: Final = (
    "SELECT adapter_id FROM account_state WHERE account_id = $1::text ORDER BY adapter_id LIMIT 1"
)

# ---- operator_requests with a payload (migration 0009; src/db/operator_requests.py) ----------------------
SQL_REQUEST_INSERT_PAYLOAD: Final = SQL_INSERT_WITH_PAYLOAD

# ---- engine uptime + downtime catch-up (migration 0012; read-only, counts only) ---------------------------
SQL_UPTIME_RUNS: Final = (
    "SELECT run_id::text, node_id, started_at_ms, last_seen_ms, stopped_at_ms, end_reason, suspended_gaps "
    "FROM engine_runs WHERE ($1::text IS NULL OR node_id = $1::text) ORDER BY started_at_ms DESC LIMIT $2::int"
)
SQL_UPTIME_COVERAGE: Final = (
    "SELECT adapter_id, last_streaming_ms, last_streaming_run_id::text FROM adapter_coverage ORDER BY adapter_id"
)
SQL_CATCHUP_RUNS: Final = (
    "SELECT id, run_id::text, epoch, adapter_id, reason, gap_start_ms, gap_end_ms, covered_from_ms, offline_from_ms, "
    "started_at_ms, finished_at_ms, orders_seen, orders_new, orders_updated, positions_seen, positions_new, "
    "positions_closed, cash_flows_new, anomalies, complete, notes FROM catchup_runs "
    "WHERE ($1::text IS NULL OR adapter_id = $1::text) ORDER BY finished_at_ms DESC, id DESC LIMIT $2::int"
)

# ---- partition_archive (operations.md s11; docs/runbooks/partition-archive.md) ---------------------------
ARCHIVE_COLS: Final = (
    "partition_name, parent_table, row_count, checksum, archive_uri, verified_at_ms, verified_by, detached_at_ms, "
    "detached_epoch"
)
SQL_ARCHIVE_GET: Final = f"SELECT {ARCHIVE_COLS} FROM partition_archive WHERE partition_name = $1::text"
SQL_ARCHIVE_LIST: Final = f"SELECT {ARCHIVE_COLS} FROM partition_archive ORDER BY partition_name DESC LIMIT $1::int"
# Forget a verified export that was never detached (count mismatch, lost file): the next lifecycle run
# re-exports the partition. Fenced like the lifecycle's own insert; a detached row is never removed here.
SQL_ARCHIVE_FORGET: Final = (
    f"DELETE FROM partition_archive WHERE partition_name = $2::text AND detached_at_ms IS NULL AND {FENCE} "
    "RETURNING parent_table, row_count, checksum, archive_uri"
)
# Current row count of an attached partition without dynamic SQL in the client (NULL when no such table).
SQL_PARTITION_ROWS: Final = (
    "SELECT CASE WHEN to_regclass(quote_ident($1::text)) IS NULL THEN NULL ELSE "
    "(xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %I', $1::text), false, true, '')))[1]"
    "::text::bigint END"
)
