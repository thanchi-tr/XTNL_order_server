"""Schema scan (plan §1, S23 and S38g; design §B3 "No market data in PostgreSQL", §B8): every column that any
migration from 0015 on adds (0015-0020 today, and every later ARM migration without a code change here), in a CREATE TABLE body, an ALTER TABLE ... ADD COLUMN or a view's select list, is on an explicit
allow-list of ids, times, codes, counts, order-state levels, keyed hashes and the §B8 R values; and a column named like
market data (a `_`-separated part `bid`, `ask`, `open`, `high`, `low`, `close`, `mid`, `spread`, `atr`, `favour`,
`quote`, ...) fails, unless it is one of the named exceptions below (a code column whose name contains such a word).

A new ARM column therefore needs a deliberate allow-list entry here; a tick, bar, quote, spread, indicator value,
favour R or favour / risk column can never slip in. The scan reads CREATE TABLE (with or without IF NOT EXISTS),
ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS] and CREATE [OR REPLACE] [MATERIALIZED] VIEW. It checks column names only:
the content of the jsonb columns (`strategy_state`, `pre_basis`, `counters`, `document`, `strategy_params`, ...) is
not checked here; that, and the end-to-end no-market-data test (rows, payloads, outbox bodies, logs, pages), is S38."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Final

import pytest

from src.db import MIGRATIONS_DIR

FIRST_SCANNED: Final = 15
"""Every migration numbered from this one on is scanned (plan S38g, round 14: 0018, 0019, 0020 and later ones too)."""


def _scanned() -> tuple[str, ...]:
    out: list[str] = []
    for path in sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9][0-9]_*.sql")):
        if int(path.name[:4]) >= FIRST_SCANNED:
            out.append(path.name)
    return tuple(out)


SCANNED = _scanned()

#: A `_`-separated part of a column name that means market data.
MARKET_DATA_WORDS = frozenset({"bid", "ask", "open", "high", "low", "close", "mid", "spread", "atr", "favour",
                               "favor", "quote", "ohlc", "tick", "ticks", "price", "prices", "indicator", "vwap"})
#: Columns whose name contains such a word but that hold no market data (column -> why).
NAMED_EXCEPTIONS: dict[str, str] = {
    "close_source": "a source code (QUOTE on PRE rows, BROKER on bar-end rows), never a value (design §B8)",
    "price_increment": "instrument metadata (PR7), like point_size; never in a notice, page or log",
    "indicator_specs": "the pinned indicator specs (kind, version, params), config, never a value",
    "indicator_inputs": "input ids with keyed hashes (indicator_id|output|bar_end_ms|window_start|key_id hash)",
}

#: Every allowed column of the scanned migrations, grouped by what it holds.
ALLOWED: dict[str, frozenset[str]] = {
    "ids and keys": frozenset({
        "adapter_id", "position_id", "account_id", "symbol", "resource", "owner", "node", "id", "decision_id",
        "notice_id", "audit_id", "config_audit_id", "accepted_id", "failure_id", "ping_id", "scope_id",
        "handle_id", "outbox_id", "route", "item_key", "opened_by_client_order_id", "clear_request_id",
        "supersedes_decision_id", "superseded_by", "position_ids", "row_refs", "updated_by", "accepted_by",
        "edge_code", "strategy_code", "md_sentinel"}),
    "times (ms) and bucket times": frozenset({
        "bar_end_ms", "broker_time_ms", "expires_at_ms", "entry_time_ms", "last_decided_bar", "pending_bar_end_ms",
        "reopen_bar_end_ms", "pre_basis_bar", "armed_at_ms", "updated_at_ms", "ended_at_ms", "decided_at_ms",
        "supersedes_bar_end_ms", "range_from_bucket", "read_at_ms", "broker_updated_at_bar_end",
        "broker_updated_read", "recorded_at_ms", "created_at_ms", "heartbeat_at_ms", "first_fault_at_ms",
        "entered_at_ms", "last_ping_at_ms", "last_probe_at_ms", "cleared_at_ms", "paused_at_ms",
        "notify_test_at_ms", "accepted_at_ms", "at_ms", "md_last_write_ms", "streak_start_ms", "last_event_ms"}),
    "codes and flags": frozenset({
        "order_kind", "asset_class", "origin", "side", "state", "prior_state", "state_reason", "attribution_source",
        "strategy_kind", "bar_spec", "entry_time_source", "s0_source", "pending_event", "event", "outcome",
        "category", "reason", "reason_kind", "trigger_met", "borderline", "advice", "quality_flags", "caveats",
        "close_source", "stop_basis", "handle_kind", "handle_role", "result", "leader", "running", "paused",
        "pause_reason", "failed_global", "scope_kind", "probe_kind", "last_probe_result", "drain_notices", "mode",
        "action", "kind"}),
    "versions, counts and sequence numbers": frozenset({
        "row_version", "revision", "decision_seq", "epoch", "strategy_version", "strategy_impl_version",
        "counter", "count",
        "envelope_version", "schema_version", "part", "decision_count", "armed_positions", "unmanaged_positions",
        "overdue_positions", "failed_scopes", "counters", "ping_count", "chain_rows", "broker_seq_at_bar_end",
        "broker_seq_read"}),
    "order state and instrument metadata (levels, units)": frozenset({
        "entry_level", "s0_level", "pinned_units", "proposed_stop", "pre_basis", "level_at_bar_end", "level_read",
        "level_at_pre", "target_level", "price_increment", "min_stop_distance", "strategy_state"}),
    "R values from order state (§B8: 0.01 R, ratio 0.001)": frozenset({"current_risk_r", "first_bar_ratio"}),
    "keyed hashes, ids with keyed hashes, config hashes": frozenset({
        "last_decided_hash", "inputs_digest", "series_inputs", "indicator_inputs", "envelope_params_hash",
        "guards_hash", "config_doc_hash", "doc_hash", "chain_head", "row_hashes"}),
    "pinned config (strategy and guard params, specs)": frozenset({
        "strategy_params", "guards", "indicator_specs", "required_history", "document"}),
    "live view (0021): ids, times, codes, counts, order levels": frozenset({
        "acked", "activation_id", "active_since", "anomalies", "approver", "attempts", "below_soft_since",
        "breach_id", "broker_ms", "broker_order_id", "broker_updated_at_ms", "bumped_at_ms", "by_whom", "cap",
        "cash_flows_new", "client_order_id", "cluster_id", "command", "committed_at_ms", "complete", "conn_lost_at",
        "cool_down_ms", "covered_from_ms", "delivered_at_ms", "delivery", "dlq_id", "dlq_reason", "effective_level",
        "end_code", "end_reason", "ended_at", "entity_id", "entity_key", "entity_type", "ep_activation_id",
        "ep_level", "ep_rule_id", "ep_subject_id", "ep_subject_type", "epoch_revision", "escalate_after_ms",
        "escalated", "event_id", "event_type", "facts", "failed_at_ms", "failing_since_ms", "finished_at_ms",
        "forced", "gap_end_ms", "gap_start_ms", "hard_latch", "hold_code", "hold_count", "info_class",
        "is_reduce_only", "last_pass_at_ms", "last_resume_at", "last_seen_ms", "leg_kind", "level",
        "linked_position_id", "log_id", "logged_at_ms", "next_retry_at_ms", "node_id", "offline_from_ms", "operator",
        "orders_new", "orders_seen", "orders_updated", "origin_dlq_id", "parked_at_ms", "pending_level",
        "pending_since", "positions_closed", "positions_new", "positions_seen", "quarantined_at_ms",
        "reached_leader_at", "rearmed_at_ms", "reason_code", "reconciled_at", "reconciled_epoch",
        "release_pending_since", "released_at_ms", "resolved_at", "resolved_by", "resumed_by", "retry_count",
        "reviewed_event_id", "rule_id", "ruleset_version", "run_id", "safe_hold", "safe_hold_since", "sent_at_ms",
        "seq_id", "severity", "started_at", "started_at_ms", "status", "stop_level", "stopped_at_ms", "stream",
        "subject_id", "subject_type", "suspended_gaps", "sweep_id", "target_id", "term_id", "tr_rule_id",
        "tr_subject_id", "tr_subject_type", "tr_transition", "transition", "uncapped_ms", "unknown_grace_ms",
        "unknown_since", "version_path"}),
}
ALLOW_LIST: frozenset[str] = frozenset().union(*ALLOWED.values())

_KEYWORDS = frozenset({"PRIMARY", "UNIQUE", "CHECK", "CONSTRAINT", "FOREIGN", "EXCLUDE"})


def _code(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def added_columns(sql: str) -> set[tuple[str, str]]:
    """(relation, column) for every column a migration adds: CREATE TABLE bodies, ADD [COLUMN], view select lists."""
    code = _code(sql)
    out: set[tuple[str, str]] = set()
    for table, block in re.findall(r"CREATE TABLE (?:IF NOT EXISTS )?(\w+)\s*\((.*?)\n\)", code, re.S):
        for line in block.splitlines():
            m = re.match(r"\s+(\w+)\s+\w", line)
            if m and m.group(1).upper() not in _KEYWORDS:
                out.add((table, m.group(1)))
    for table, stmt in re.findall(r"ALTER TABLE (?:IF EXISTS )?(?:ONLY )?(\w+)\s(.*?);", code, re.S):
        added = re.findall(r"\bADD\s+(?:COLUMN\s+)?(?:IF NOT EXISTS\s+)?(\w+)", stmt)
        out |= {(table, c) for c in added if c.upper() not in _KEYWORDS}
    for view, select in re.findall(
            r"CREATE (?:OR REPLACE )?(?:MATERIALIZED )?VIEW (?:IF NOT EXISTS )?(\w+) AS\s+SELECT(.*?)\sFROM\s", code,
            re.S):
        for col in select.split(","):
            name = col.strip().split()[-1]                    # `expr AS alias` -> alias
            out.add((view, name))
    return out


def looks_like_market_data(column: str) -> bool:
    return bool(set(column.lower().split("_")) & MARKET_DATA_WORDS)


def problems(columns: set[tuple[str, str]]) -> list[str]:
    out: list[str] = []
    for rel, col in sorted(columns):
        if col not in ALLOW_LIST:
            out.append(f"{rel}.{col}: not on the allow-list")
        if looks_like_market_data(col) and col not in NAMED_EXCEPTIONS:
            out.append(f"{rel}.{col}: named like market data")
    return out


def scanned_columns() -> set[tuple[str, str]]:
    cols: set[tuple[str, str]] = set()
    for name in SCANNED:
        cols |= added_columns((MIGRATIONS_DIR / name).read_text(encoding="utf-8"))
    return cols


def test_every_scanned_migration_exists() -> None:
    for name in SCANNED:
        assert (MIGRATIONS_DIR / name).is_file(), name


def test_every_migration_from_0015_on_is_scanned() -> None:
    """Round 14 (plan S38g): the scan is not a fixed list; 0018, 0019, 0020 and any later migration are in it."""
    assert SCANNED[0] == "0015_arm_prereqs.sql"
    assert {"0018_arm_round12.sql", "0019_orders_linked_idx.sql", "0020_arm_hardening.sql"} <= set(SCANNED)
    assert all(int(n[:4]) >= FIRST_SCANNED for n in SCANNED)


def test_every_column_of_0015_on_is_allowed() -> None:
    cols = scanned_columns()
    assert len(cols) > 100                                      # the scan really sees the ARM tables
    assert problems(cols) == []


def test_the_scan_sees_every_kind_of_column() -> None:
    cols = scanned_columns()
    assert ("orders_state", "order_kind") in cols                # 0015 ADD COLUMN
    assert ("notification_outbox", "expires_at_ms") in cols      # 0016 ADD COLUMN
    assert ("arm_decisions", "inputs_digest") in cols            # 0017 CREATE TABLE
    assert ("arm_decisions_view", "current_risk_r") in cols      # 0017 view
    assert ("arm_budget_counters", "streak_start_ms") in cols    # 0020 CREATE TABLE


LIVEVIEW_VIEWS: Final = frozenset({
    "lv_entity_events", "lv_oversee_events", "lv_halt_events", "lv_alerts", "lv_operator_actions", "lv_dlq",
    "lv_catchups", "lv_engine_runs", "lv_leader_terms", "lv_stream_health", "lv_cluster_state", "lv_engine_epoch",
    "lv_breaches", "lv_open_orders", "lv_open_positions", "lv_ruleset_timing", "lv_sweep_quarantine",
    "lv_auth_state"})


def test_the_scan_sees_all_18_live_view_views() -> None:
    """docs/design/live-view.md s3.2 / s6: every lv_ view of 0021 is parsed by added_columns, so each of its columns
    is checked against the allow-list (a view written with a column list or WITH (...) would hide them)."""
    sql = (MIGRATIONS_DIR / "0021_liveview_role.sql").read_text(encoding="utf-8")
    assert {rel for rel, _ in added_columns(sql)} == LIVEVIEW_VIEWS
    code = _code(sql)
    assert "WITH (" not in code
    assert not re.search(r"CREATE (?:OR REPLACE )?VIEW \w+\s*\(", code)


def test_the_live_view_group_is_exactly_the_new_names() -> None:
    group = ALLOWED["live view (0021): ids, times, codes, counts, order levels"]
    assert len(group) == 120
    others = frozenset().union(*(v for k, v in ALLOWED.items() if not k.startswith("live view")))
    assert not group & others
    assert {c for c in group if looks_like_market_data(c)} == set()


def test_named_exceptions_are_allowed_and_really_named_like_market_data() -> None:
    for col in NAMED_EXCEPTIONS:
        assert col in ALLOW_LIST and looks_like_market_data(col), col


def test_allow_list_has_no_market_data_name_outside_the_exceptions() -> None:
    assert {c for c in ALLOW_LIST if looks_like_market_data(c)} == set(NAMED_EXCEPTIONS)


@pytest.mark.parametrize("column", ["bid", "ask", "open", "high", "low", "close", "mid", "spread", "atr", "favour",
                                    "quote", "bar_close", "close_px", "atr_value", "favour_r", "mid_price",
                                    "quote_bid", "spread_pips", "high_low"])
def test_a_market_data_column_fails(tmp_path: Path, column: str) -> None:
    """A planted column named like market data fails the scan, in a table (with or without IF NOT EXISTS), an
    ADD COLUMN, a bare ADD, a view and a materialized view alike."""
    planted = (f"CREATE TABLE IF NOT EXISTS arm_planted (\n    decision_id bigint,\n    {column} numeric\n);\n"
               f"CREATE TABLE arm_planted_plain (\n    decision_id bigint,\n    {column} numeric\n);\n"
               f"ALTER TABLE arm_decisions ADD COLUMN IF NOT EXISTS {column} numeric;\n"
               f"ALTER TABLE arm_notices ADD {column} numeric;\n"
               f"CREATE OR REPLACE VIEW arm_planted_view AS\n    SELECT decision_id, x AS {column}\n    FROM t;\n"
               f"CREATE MATERIALIZED VIEW arm_planted_mv AS\n    SELECT decision_id, x AS {column}\n    FROM t;\n")
    path = tmp_path / "0099_planted.sql"
    path.write_text(planted, encoding="utf-8")
    found = problems(added_columns(path.read_text(encoding="utf-8")))
    for rel in ("arm_planted", "arm_planted_plain", "arm_decisions", "arm_notices", "arm_planted_view",
                "arm_planted_mv"):
        assert f"{rel}.{column}: named like market data" in found, (rel, found)


def test_add_constraint_and_keys_are_not_columns() -> None:
    sql = ("ALTER TABLE t ADD CONSTRAINT t_check CHECK (x > 0);\nALTER TABLE t ADD PRIMARY KEY (id);\n"
           "ALTER TABLE t ADD UNIQUE (id), ADD COLUMN IF NOT EXISTS decision_id bigint, ADD node text;\n")
    assert added_columns(sql) == {("t", "decision_id"), ("t", "node")}


def test_an_unlisted_column_fails() -> None:
    sql = "CREATE TABLE IF NOT EXISTS arm_x (\n    decision_id bigint,\n    mystery_value numeric\n);\n"
    assert problems(added_columns(sql)) == ["arm_x.mystery_value: not on the allow-list"]
