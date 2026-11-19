"""SQL of the ARM and market-data operator commands (`python -m src.ops arm ...` / `md ...`; design §B9, §B13, §B14;
plan S34).

* `arm_controls` and `arm_accepted` are written **only** here (design §B3 static check,
  `tests/db/test_arm_stores_static.py`): operator-owned controls the engine only reads, so, like `rulesets`, the
  writes run unfenced under a table lock with the `operator_audit` row in the same transaction.
* Requests reuse the fenced `operator_requests` insert (`src/ops/sql.py SQL_REQUEST_INSERT_PAYLOAD`, `$1` = the
  epoch); the reads below are unfenced.
* Every read names ids, codes, counts and times only: no price, level or account value is selected (the arm_positions
  read takes the state alone, the arm_failures read the counters and ids).
"""
from __future__ import annotations

from typing import Final

# ---- liveness and request budget (design §B13) -----------------------------------------------------------------
#: The freshest leader heartbeat in `arm_status` of epoch `$1` (the epoch the request is fenced on) and its age on
#: the database clock (the CLI refuses a request when it is older than 60 s or absent). Every node writes its row
#: with `leader` set and a fenced node never rewrites it, so only the epoch tells the current leader's row apart.
SQL_ARM_HEARTBEAT: Final = (
    "SELECT node, heartbeat_at_ms, now_ms() - heartbeat_at_ms, epoch FROM arm_status "
    "WHERE leader AND epoch = $1::bigint AND heartbeat_at_ms IS NOT NULL ORDER BY heartbeat_at_ms DESC LIMIT 1"
)
#: Pending requests of the given kinds in the epoch (the CLI refuses a new ARM request at 20).
SQL_ARM_PENDING_COUNT: Final = (
    "SELECT count(*) FROM operator_requests WHERE requested_epoch = $1::bigint AND consumed_at_ms IS NULL "
    "AND kind = ANY($2::text[])"
)
#: A filed request's answer (`result` is `applied`, `refused:<code>` or `moot:<code>` once consumed).
SQL_REQUEST_RESULT: Final = (
    "SELECT consumed_at_ms, result FROM operator_requests WHERE request_id = $1::bigint"
)
#: Pending `arm_clear_failed` requests per target (`arm failed show`).
SQL_PENDING_CLEARS: Final = (
    "SELECT request_id, target FROM operator_requests WHERE kind = 'arm_clear_failed' AND consumed_at_ms IS NULL "
    "AND requested_epoch = $1::bigint ORDER BY request_id"
)

# ---- arm_status (per node telemetry; read only) --------------------------------------------------------------------
SQL_ARM_STATUS_NODES: Final = (
    "SELECT node, epoch, heartbeat_at_ms, now_ms() - heartbeat_at_ms, leader, running, paused, pause_reason, "
    "config_doc_hash, armed_positions, overdue_positions, failed_scopes, failed_global FROM arm_status ORDER BY node"
)

# ---- arm_controls (singleton; written only here) -----------------------------------------------------------------
SQL_CONTROLS_LOCK: Final = "LOCK TABLE arm_controls IN SHARE ROW EXCLUSIVE MODE"
SQL_CONTROLS_FOR_UPDATE: Final = (
    "SELECT paused, pause_reason, paused_at_ms, drain_notices, notify_test_at_ms, updated_at_ms, updated_by "
    "FROM arm_controls WHERE id = 1 FOR UPDATE"
)
#: Pause (or widen a pause to `--drain-notices`): the first pause time is kept while it stays paused.
SQL_CONTROLS_PAUSE: Final = (
    "UPDATE arm_controls SET paused = true, pause_reason = $1::text, "
    "paused_at_ms = CASE WHEN paused AND paused_at_ms IS NOT NULL THEN paused_at_ms ELSE now_ms() END, "
    "drain_notices = $2::boolean, updated_at_ms = now_ms(), updated_by = $3::text, audit_id = $4::bigint "
    "WHERE id = 1 RETURNING paused_at_ms"
)
#: Resume: the pause and the drain flag end together (a resume never clears FAILED, §B13).
SQL_CONTROLS_RESUME: Final = (
    "UPDATE arm_controls SET paused = false, pause_reason = NULL, paused_at_ms = NULL, drain_notices = false, "
    "updated_at_ms = now_ms(), updated_by = $1::text, audit_id = $2::bigint WHERE id = 1 AND paused "
    "RETURNING updated_at_ms"
)
SQL_CONTROLS_NOTIFY_TEST: Final = (
    "UPDATE arm_controls SET notify_test_at_ms = now_ms(), updated_at_ms = now_ms(), updated_by = $1::text, "
    "audit_id = $2::bigint WHERE id = 1 RETURNING notify_test_at_ms"
)

# ---- arm_accepted (append-only from here; the only trust boundary of the configuration, §B9) -----------------------
SQL_ACCEPTED_LOCK: Final = "LOCK TABLE arm_accepted IN SHARE ROW EXCLUSIVE MODE"
#: `accepted_at_ms` is the engine host's clock (the CLI runs there; the installer's accepted-subset cut-off, S33).
SQL_ACCEPTED_INSERT: Final = (
    "INSERT INTO arm_accepted (doc_hash, schema_version, mode, accepted_at_ms, accepted_by, audit_id) "
    "VALUES ($1::text, $2::int, 'manual', $3::bigint, $4::text, $5::bigint) RETURNING accepted_id"
)
SQL_ACCEPTED_RECENT: Final = (
    "SELECT accepted_id, doc_hash, schema_version, mode, accepted_at_ms, accepted_by, audit_id FROM arm_accepted "
    "ORDER BY accepted_at_ms DESC, accepted_id DESC LIMIT $1::int"
)

# ---- local ids and loads for validation and admission (never Supabase; no value is read) --------------------------
SQL_INSTRUMENT_SYMBOLS: Final = "SELECT adapter_id, symbol FROM instruments ORDER BY adapter_id, symbol"
SQL_ACCOUNT_IDS: Final = "SELECT adapter_id, account_id FROM account_state ORDER BY adapter_id, account_id"
SQL_HELD_SYMBOLS: Final = (
    "SELECT adapter_id, symbol FROM positions_state WHERE status = 'OPEN' GROUP BY adapter_id, symbol "
    "ORDER BY adapter_id, symbol"
)
#: Armed (non-ended) positions per adapter and their trading accounts (PR9 stop-read load of the admission check).
SQL_ARMED_LOAD: Final = (
    "SELECT adapter_id, count(*), count(DISTINCT account_id) FROM arm_positions "
    "WHERE state NOT IN ('CLOSED', 'DISARMED') GROUP BY adapter_id ORDER BY adapter_id"
)
SQL_ARM_POSITION_STATE: Final = (
    "SELECT state FROM arm_positions WHERE adapter_id = $1::text AND position_id = $2::text "
    "ORDER BY resource, owner LIMIT 1"
)

# ---- arm_failures (read only: the clear is the leader's arm_clear_failed handler, §B14) ----------------------------
FAILED_COLS: Final = (
    "failure_id, scope_kind, scope_id, edge_code, reason, probe_kind, ping_id, counters::text, position_ids, "
    "first_fault_at_ms, entered_at_ms, epoch, last_ping_at_ms, ping_count, last_probe_at_ms, last_probe_result, "
    "cleared_at_ms, clear_request_id"
)
SQL_FAILED_LIST: Final = (
    f"SELECT {FAILED_COLS} FROM arm_failures WHERE ($1::boolean OR cleared_at_ms IS NULL) "
    "ORDER BY entered_at_ms DESC, failure_id LIMIT $2::int"
)
SQL_FAILED_OPEN_SCOPE: Final = (
    "SELECT failure_id, reason, probe_kind FROM arm_failures WHERE scope_kind = $1::text AND scope_id = $2::text "
    "AND cleared_at_ms IS NULL"
)

# ---- views (plan S35; read only). No level, unit, quote or R column is selected: entry / S0 levels, pinned units,
# pre_basis, proposed stops, risk R and the first-bar ratio stay in PostgreSQL; the views show ids, codes and times.
POSITION_VIEW_COLS: Final = (
    "adapter_id, position_id, account_id, symbol, side, state, prior_state, state_reason, edge_code, "
    "attribution_source, strategy_code, strategy_version, strategy_kind, strategy_impl_version, guards_hash, "
    "bar_spec, config_doc_hash, entry_time_ms, entry_time_source, s0_source, last_decided_bar, "
    "last_decided_hash IS NOT NULL, pending_event, pending_bar_end_ms, reopen_bar_end_ms, decision_seq, "
    "pre_basis IS NOT NULL, armed_at_ms, updated_at_ms, ended_at_ms, epoch, row_version"
)
#: `arm show` / `arm config resolve`: `$1` adapter (NULL: any), `$2` position (NULL: any), `$3` include ended rows.
SQL_POSITIONS_VIEW: Final = (
    f"SELECT {POSITION_VIEW_COLS} FROM arm_positions WHERE ($1::text IS NULL OR adapter_id = $1::text) "
    "AND ($2::text IS NULL OR position_id = $2::text) AND ($3::boolean OR state NOT IN ('CLOSED', 'DISARMED')) "
    "ORDER BY adapter_id, position_id, resource, owner LIMIT $4::int"
)
DECISION_VIEW_COLS: Final = (
    "decision_id, bar_end_ms, adapter_id, position_id, bar_spec, event, revision, decision_seq, decided_at_ms, "
    "epoch, outcome, category, reason, reason_kind, trigger_met, borderline, advice, quality_flags, caveats, "
    "close_source, stop_basis, series_inputs, indicator_inputs, inputs_digest, envelope_version, "
    "envelope_params_hash, strategy_kind, strategy_impl_version, supersedes_decision_id, range_from_bucket, "
    "notice_id"
)
#: `arm decisions` / `arm replay`: `$1` adapter, `$2` position (NULL: any), `$3` / `$4` bar-end range (inclusive),
#: newest first, at most `$5` rows.
SQL_DECISIONS_VIEW: Final = (
    f"SELECT {DECISION_VIEW_COLS} FROM arm_decisions WHERE ($1::text IS NULL OR adapter_id = $1::text) "
    "AND ($2::text IS NULL OR position_id = $2::text) AND bar_end_ms BETWEEN $3::bigint AND $4::bigint "
    "ORDER BY bar_end_ms DESC, decision_seq DESC, revision DESC LIMIT $5::int"
)
#: `arm registry check --against-db`: every distinct stored pin (strategy, guard list, indicator specs) with its row
#: counts (all rows, and those not ended), and every strategy version a decision was made under (replay re-runs it).
SQL_PINNED_VERSIONS: Final = (
    "SELECT strategy_kind, strategy_impl_version, guards::text, indicator_specs::text, count(*), "
    "count(*) FILTER (WHERE state NOT IN ('CLOSED', 'DISARMED')) FROM arm_positions "
    "GROUP BY strategy_kind, strategy_impl_version, guards::text, indicator_specs::text "
    "ORDER BY strategy_kind, strategy_impl_version"
)
SQL_DECISION_VERSIONS: Final = (
    "SELECT strategy_kind, strategy_impl_version, count(*) FROM arm_decisions "
    "GROUP BY strategy_kind, strategy_impl_version ORDER BY strategy_kind, strategy_impl_version"
)

__all__ = [
    "DECISION_VIEW_COLS", "FAILED_COLS", "POSITION_VIEW_COLS", "SQL_ACCEPTED_INSERT", "SQL_ACCEPTED_LOCK",
    "SQL_ACCEPTED_RECENT", "SQL_ACCOUNT_IDS", "SQL_ARMED_LOAD", "SQL_ARM_HEARTBEAT", "SQL_ARM_PENDING_COUNT",
    "SQL_ARM_POSITION_STATE", "SQL_ARM_STATUS_NODES", "SQL_CONTROLS_FOR_UPDATE", "SQL_CONTROLS_LOCK",
    "SQL_CONTROLS_NOTIFY_TEST", "SQL_CONTROLS_PAUSE", "SQL_CONTROLS_RESUME", "SQL_DECISIONS_VIEW",
    "SQL_DECISION_VERSIONS", "SQL_FAILED_LIST", "SQL_FAILED_OPEN_SCOPE", "SQL_HELD_SYMBOLS", "SQL_INSTRUMENT_SYMBOLS",
    "SQL_PENDING_CLEARS", "SQL_PINNED_VERSIONS", "SQL_POSITIONS_VIEW", "SQL_REQUEST_RESULT",
]
