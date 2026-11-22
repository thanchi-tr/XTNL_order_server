"""Migration 0021: the read-only `xtnl_liveview` role and its lv_ views (docs/design/live-view.md s3, s3.5, s6
"Database").

Static checks run against the migration text: the grant list equals src.liveview.sql.ALL_VIEWS, nothing else is
granted, the role attributes and settings, security_barrier on every view, every view column on the allow-list,
the facts keys, the free-text columns that never leave their table, the file layout (role block first, one index),
the post step, the closing-order rule and the placeholders. The codes the engine writes (transition subjects,
operator commands, Oversee reasons, unknown symbols) are checked against the patterns parsed from the migration.

`pg` tests (skipped unless ORDER_SERVER_TEST_DSN points at a database with every migration applied) read the views
as `xtnl_liveview` inside a transaction that is always rolled back; rows they insert carry explicit ids, so no
sequence advances and nothing is left behind.
"""
from __future__ import annotations

import ast
import os
import re
import secrets
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from src.db import MIGRATIONS_DIR
from src.db.migrate import POST_STEP, PartitionIndex
from src.db.rows import subject_to_db
from src.liveview import sql as Q
from src.liveview.events import CODE_RE, COMMAND_RE, DECIMAL_IN_TEXT, check_value
from tests.db.test_arm_schema_no_market_data import ALLOW_LIST, ALLOWED, _code, added_columns

ROOT = Path(__file__).resolve().parents[2]
MIG = MIGRATIONS_DIR / "0021_liveview_role.sql"
TEXT = MIG.read_text(encoding="utf-8")
LINES = TEXT.splitlines()
CODE = _code(TEXT)
LV_GROUP = ALLOWED["live view (0021): ids, times, codes, counts, order levels"]
DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
ROLE_BLOCK_LINES = 32          # pinned with the carry-over runbook (tests/deploy/test_single_pc_autostart.py)


def ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def statements(code: str = CODE) -> list[str]:
    """Top-level statements, split on `;` outside quotes and $$ bodies."""
    out: list[str] = []
    buf: list[str] = []
    i, quote, dollar = 0, False, False
    while i < len(code):
        if not quote and code.startswith("$$", i):
            dollar = not dollar
            buf.append("$$")
            i += 2
            continue
        ch = code[i]
        if ch == "'" and not dollar:
            quote = not quote
        if ch == ";" and not quote and not dollar:
            out.append(ws("".join(buf)))
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = ws("".join(buf))
    assert tail == "", f"text after the last statement: {tail[:60]}"
    return [s for s in out if s]


STATEMENTS = statements()
VIEWS: dict[str, str] = {m.group(1): m.group(2) for m in
                         re.finditer(r"CREATE OR REPLACE VIEW (\w+) AS(.*?);", CODE, re.S)}


def view(name: str) -> str:
    return VIEWS[name]


def select_list(body: str) -> list[str]:
    m = re.match(r"\s*SELECT(.*?)\sFROM\s", body, re.S)
    assert m
    return [ws(c) for c in m.group(1).split(",")]


def laterals(body: str) -> dict[str, dict[str, str]]:
    """alias -> {name: expr} for every `CROSS JOIN LATERAL (SELECT ... ) <alias>` block (top-level commas)."""
    out: dict[str, dict[str, str]] = {}
    for m in re.finditer(r"CROSS JOIN LATERAL \(SELECT", body):
        i, depth, quote, items, buf = m.end(), 1, False, [], []
        while True:
            ch = body[i]
            if ch == "'":
                quote = not quote
            elif not quote and ch == "(":
                depth += 1
            elif not quote and ch == ")":
                depth -= 1
                if depth == 0:
                    break
            if ch == "," and depth == 1 and not quote:
                items.append(ws("".join(buf)))
                buf = []
            else:
                buf.append(ch)
            i += 1
        items.append(ws("".join(buf)))
        alias = re.match(r"\s*(\w+)", body[i + 1:])
        assert alias, body[i:i + 20]
        named: dict[str, str] = {}
        for it in items:
            mm = re.fullmatch(r"(.*) AS (\w+)", it, re.S)
            assert mm, it
            named[mm.group(2)] = mm.group(1)
        out[alias.group(1)] = named
    return out


def lateral_items(body: str, alias: str = "x") -> dict[str, str]:
    return laterals(body)[alias]


def call_args(text: str, fn: str) -> list[list[str]]:
    """Top-level argument lists of every `fn(` call in `text` (quotes and nesting respected)."""
    calls: list[list[str]] = []
    for m in re.finditer(rf"\b{fn}\(", text):
        i, depth, quote, args, buf = m.end(), 1, False, [], []
        while True:
            ch = text[i]
            if ch == "'":
                quote = not quote
            elif not quote and ch == "(":
                depth += 1
            elif not quote and ch == ")":
                depth -= 1
                if depth == 0:
                    break
            if ch == "," and depth == 1 and not quote:
                args.append(ws("".join(buf)))
                buf = []
            else:
                buf.append(ch)
            i += 1
        args.append(ws("".join(buf)))
        calls.append(args)
    return calls


# ---- grants --------------------------------------------------------------------------------------------------------
def test_the_grant_select_list_equals_all_views() -> None:
    grants = [s for s in STATEMENTS if s.startswith("GRANT SELECT")]
    assert len(grants) == 1
    m = re.fullmatch(r"GRANT SELECT ON TABLE (.*) TO xtnl_liveview", grants[0])
    assert m
    names = [n.strip() for n in m.group(1).split(",")]
    assert len(names) == len(set(names))
    assert set(names) == set(Q.ALL_VIEWS)
    assert set(names) == set(VIEWS)


def test_no_other_grant_than_connect_usage_and_the_view_select() -> None:
    grants = [m.start() for m in re.finditer(r"\bGRANT\b", CODE, re.I)]
    assert len(grants) == 3
    do = re.search(r"DO \$\$(.*?)\$\$;", CODE, re.S)
    assert do
    inside = [g for g in grants if do.start() <= g < do.end()]
    assert len(inside) == 1
    assert "EXECUTE format('GRANT CONNECT ON DATABASE %I TO xtnl_liveview', current_database());" in do.group(1)
    top = [s for s in STATEMENTS if re.match(r"GRANT\b", s, re.I)]
    assert sorted(top, key=len)[0] == "GRANT USAGE ON SCHEMA public TO xtnl_liveview"
    assert len(top) == 2 and top[1].startswith("GRANT SELECT ON TABLE lv_")


def test_no_table_write_password_default_privilege_or_login_grant() -> None:
    assert not re.search(r"\bGRANT\s+(?:ALL|INSERT|UPDATE|DELETE|TRUNCATE|REFERENCES|TRIGGER|MAINTAIN|CREATE)\b",
                         CODE, re.I)
    assert not re.search(r"\b(?:INSERT|UPDATE|DELETE|TRUNCATE)\b", CODE, re.I)
    assert not re.search(r"\bON\s+ALL\s+TABLES\b", CODE, re.I)
    assert not re.search(r"\bPASSWORD\b", CODE, re.I)
    assert not re.search(r"\bDEFAULT\s+PRIVILEGES\b", CODE, re.I)
    assert not re.search(r"\bLOGIN\b", CODE, re.I)          # NOLOGIN only
    assert not re.search(r"\b(?:REVOKE|DROP|SECURITY\s+DEFINER|CREATE\s+(?:TABLE|FUNCTION|PROCEDURE|TRIGGER|RULE))\b",
                         CODE, re.I)
    grant_select = next(s for s in STATEMENTS if s.startswith("GRANT SELECT"))
    for rel in re.findall(r"\b(\w+)\b", grant_select.split(" ON TABLE ", 1)[1].split(" TO ")[0]):
        assert rel.startswith("lv_"), rel                  # no base table is granted


def test_the_create_role_has_every_no_attribute_and_the_connection_limit() -> None:
    m = re.search(r"CREATE ROLE xtnl_liveview(.*?);", CODE, re.S)
    assert m
    assert ws(m.group(1)) == ("NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS "
                              "CONNECTION LIMIT 3")
    assert "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_liveview')" in CODE


def test_the_three_role_settings() -> None:
    alters = [s for s in STATEMENTS if s.startswith("ALTER ROLE")]
    assert alters == ["ALTER ROLE xtnl_liveview SET default_transaction_read_only = on",
                      "ALTER ROLE xtnl_liveview SET statement_timeout = '5s'",
                      "ALTER ROLE xtnl_liveview SET idle_in_transaction_session_timeout = '10s'"]


def test_every_view_is_a_security_barrier_view() -> None:
    assert set(VIEWS) == set(Q.ALL_VIEWS) and len(VIEWS) == 18
    barriers = [s for s in STATEMENTS if s.startswith("ALTER VIEW")]
    assert sorted(barriers) == sorted(f"ALTER VIEW {v} SET (security_barrier = true)" for v in VIEWS)
    assert "WITH (" not in CODE


def test_every_statement_is_of_an_expected_kind() -> None:
    allowed = (r"SET client_min_messages = warning", r"DO \$\$ .* \$\$", r"ALTER ROLE xtnl_liveview SET .*",
               r"CREATE INDEX IF NOT EXISTS event_log_log_id_idx ON ONLY event_log \(log_id\)",
               r"GRANT USAGE ON SCHEMA public TO xtnl_liveview", r"CREATE OR REPLACE VIEW lv_\w+ AS SELECT .*",
               r"ALTER VIEW lv_\w+ SET \(security_barrier = true\)", r"GRANT SELECT ON TABLE .* TO xtnl_liveview")
    for s in STATEMENTS:
        assert any(re.fullmatch(p, s, re.S) for p in allowed), s[:80]


# ---- columns -------------------------------------------------------------------------------------------------------
def test_every_view_column_is_on_the_allow_list() -> None:
    cols = added_columns(TEXT)
    assert {rel for rel, _ in cols} == set(Q.ALL_VIEWS)
    bad = sorted(f"{rel}.{c}" for rel, c in cols if c not in LV_GROUP and c not in ALLOW_LIST)
    assert bad == []
    for item in (c for body in VIEWS.values() for c in select_list(body)):
        # a flat list of names: a bare column or `alias.col AS name`, nothing computed in the select list
        assert re.fullmatch(r"\w+|\w+\.(?:\w+|\"\w+\") AS \w+", item), item


SQL_COLUMNS = {
    "lv_entity_events": Q.ENTITY_COLS, "lv_oversee_events": Q.OVERSEE_COLS, "lv_halt_events": Q.HALT_COLS,
    "lv_alerts": Q.ALERT_COLS, "lv_operator_actions": Q.OPERATOR_COLS, "lv_dlq": Q.DLQ_COLS,
    "lv_catchups": Q.CATCHUP_COLS, "lv_engine_runs": Q.RUN_COLS, "lv_leader_terms": Q.TERM_COLS,
    "lv_stream_health": Q.STREAM_COLS, "lv_cluster_state": Q.CLUSTER_COLS, "lv_engine_epoch": Q.EPOCH_COLS,
    "lv_breaches": Q.BREACH_COLS, "lv_open_orders": Q.ORDER_COLS, "lv_open_positions": Q.POSITION_COLS,
    "lv_ruleset_timing": Q.TIMING_COLS, "lv_sweep_quarantine": Q.QUARANTINE_COLS, "lv_auth_state": Q.AUTH_COLS,
}


@pytest.mark.parametrize("name", sorted(SQL_COLUMNS))
def test_each_view_exposes_exactly_the_columns_the_service_reads(name: str) -> None:
    exposed = [c.split()[-1] for c in select_list(view(name))]
    read = [c.strip().split()[-1] for c in SQL_COLUMNS[name].split(",")]
    assert exposed == read


FORBIDDEN_KEYS = {"units", "ordered_units", "remaining_units", "raw_qty", "accrued_swap", "commission", "balance",
                  "equity", "margin_used", "free_margin", "unrealised_pnl", "realised_pnl_today", "amount",
                  "currency", "bid", "ask", "spread_pips", "entry_price", "stop_loss", "take_profit", "close_price",
                  "execution_price", "price", "point_size", "contract_size", "price_increment", "min_stop_distance",
                  "checksum", "version", "payload", "detail", "reason"}
ALLOWED_FACT_KEYS = {"broker_order_id", "client_order_id", "symbol", "side", "status", "is_reduce_only",
                     "linked_position_id", "leg_kind", "instrument_unknown", "source", "broker_updated_at_ms",
                     "entry_level", "stop_level", "target_level", "position_id", "opened_by_client_order_id",
                     "account_id"}
ALLOWED_PAYLOAD_SOURCES = (ALLOWED_FACT_KEYS - {"leg_kind", "entry_level", "stop_level", "target_level"}) | {
    "order_kind", "entry_price", "stop_loss", "take_profit"}


def test_no_jsonb_build_object_names_a_forbidden_key() -> None:
    calls = call_args(CODE, "jsonb_build_object")
    assert len(calls) == 3                                  # ORDER, POSITION, ACCOUNT
    for args in calls:
        assert len(args) % 2 == 0
        keys = args[0::2]
        for k in keys:
            m = re.fullmatch(r"'(\w+)'", k)
            assert m, f"a key must be a quoted literal: {k}"
            assert m.group(1) in ALLOWED_FACT_KEYS, m.group(1)
            assert m.group(1) not in FORBIDDEN_KEYS
        for value in args[1::2]:
            srcs = re.findall(r"payload->>?'(\w+)'", value)
            assert srcs, value
            assert set(srcs) <= ALLOWED_PAYLOAD_SOURCES, value
            # a level source only under its level name, never under an id / code key
    for args in calls:
        pairs = dict(zip([k.strip("'") for k in args[0::2]], args[1::2], strict=True))
        for key, value in pairs.items():
            levels = {s for s in re.findall(r"payload->>?'(\w+)'", value)} & {"entry_price", "stop_loss",
                                                                               "take_profit"}
            if levels:
                assert key in {"entry_level", "stop_level", "target_level"}, key


def test_every_payload_reference_in_the_file_names_an_allowed_key() -> None:
    refs = re.findall(r"payload\s*->>?\s*'(\w+)'", CODE)
    assert refs and set(refs) <= ALLOWED_PAYLOAD_SOURCES
    assert not re.search(r"payload\s*(?:#>|\?|@>|\|\|)", CODE)       # no path / key-test / whole-payload use
    assert not re.search(r"\be\.payload\b(?!\s*->)", CODE)            # never the whole payload


def test_operator_audit_free_text_never_leaves_the_table() -> None:
    body = view("lv_operator_actions")
    for word in ("reason", "detail", "db_user", "os_user", "client_addr"):
        assert not re.search(rf"\b{word}\b", body), word
    assert "a.command ~ '^[a-z_]+(\\.[a-z_]+){0,3}$'" in body


def test_halt_reason_only_inside_the_system_guard() -> None:
    body = view("lv_halt_events")
    guard = re.search(r"CASE WHEN h\.\"by\" = 'system' AND .*? END AS reason_code", body, re.S)
    assert guard
    occ = [m.start() for m in re.finditer(r"\breason\b", body)]
    assert occ and all(guard.start() <= i < guard.end() for i in occ)
    assert all(body[i - 2:i] == "h." for i in occ)


ALERT_FREE_TEXT_USES = {
    "subject": (r"regexp_match\(n\.subject,", r"n\.subject = 'HALT cleared'", r"n\.subject LIKE 'HALT cleared by %'"),
    "last_error": (r"n\.last_error = 'deduplicated'",),
    "channels": (r"n\.channels @> ARRAY\['\+escalated'\]",),
    "body": (),
    "dedupe_key": (),
}


@pytest.mark.parametrize("col", sorted(ALERT_FREE_TEXT_USES))
def test_outbox_free_text_is_only_compared(col: str) -> None:
    body = view("lv_alerts")
    occ = [m.start() for m in re.finditer(rf"\b{col}\b", body)]
    allowed = [m.start() + m.group(0).index(col) for p in ALERT_FREE_TEXT_USES[col] for m in re.finditer(p, body)]
    assert sorted(occ) == sorted(allowed), col


def test_the_transition_subject_leaves_only_through_the_four_captures() -> None:
    body = view("lv_alerts")
    uses = re.findall(r"t\.tm\b(?:\[\d\])?", body)
    assert sorted(uses) == sorted(["t.tm", "t.tm[1]", "t.tm[2]", "t.tm[3]", "t.tm[4]"])
    items = lateral_items(body)
    assert items["tr_transition"] == "t.tm[1]" and items["tr_rule_id"] == "t.tm[2]"
    assert items["tr_subject_type"] == "upper(t.tm[3])" and items["tr_subject_id"] == "t.tm[4]"
    assert "WHEN t.tm IS NOT NULL THEN 'transition'" in items["info_class"]
    assert re.search(r"CASE WHEN n\.kind = 'info' THEN regexp_match\(n\.subject,", body)


@pytest.mark.parametrize("name", ["lv_dlq", "lv_engine_runs", "lv_oversee_events", "lv_breaches", "lv_halt_events"])
def test_code_guards_carry_the_decimal_test(name: str) -> None:
    body = view(name)
    guarded = 0
    for m in re.finditer(r"(\w+\.(?:\"\w+\"|\w+)) ~ '\^\[A-Za-z0-9_\(\):,-\]\{1,80\}\$'", body):
        assert f"{m.group(1)} !~ '[0-9][.,][0-9]'" in body, m.group(1)
        guarded += 1
    assert guarded


# ---- layout ----------------------------------------------------------------------------------------------------------
def _line_of(pattern: str) -> int:
    return next(i for i, ln in enumerate(LINES, 1) if re.search(pattern, ln) and not ln.lstrip().startswith("--"))


def test_the_role_block_comes_first_and_ends_before_the_index_and_views() -> None:
    last_alter = max(i for i, ln in enumerate(LINES, 1) if ln.startswith("ALTER ROLE xtnl_liveview SET"))
    assert last_alter == ROLE_BLOCK_LINES
    first_view = _line_of(r"CREATE OR REPLACE VIEW")
    index = _line_of(r"CREATE INDEX")
    assert last_alter < index < first_view
    head = "\n".join(ln for ln in LINES[:ROLE_BLOCK_LINES] if not ln.lstrip().startswith("--"))
    assert not re.search(r"\b(VIEW|INDEX|PASSWORD|USAGE|SELECT ON)\b", head)
    assert "CREATE ROLE xtnl_liveview" in head and "GRANT CONNECT" in head


def test_the_only_index_is_the_parent_log_id_index() -> None:
    idx = [s for s in STATEMENTS if re.search(r"\bINDEX\b", s)]
    assert idx == ["CREATE INDEX IF NOT EXISTS event_log_log_id_idx ON ONLY event_log (log_id)"]
    assert len(re.findall(r"\bCREATE\s+(?:UNIQUE\s+)?INDEX\b", CODE)) == 1
    assert "CONCURRENTLY" not in CODE


def test_post_step_builds_the_per_partition_log_id_indexes() -> None:
    item = PartitionIndex(21, "event_log", "event_log_log_id_idx", "log_id_idx", "log_id")
    assert item in POST_STEP
    assert [p for p in POST_STEP if isinstance(p, PartitionIndex) and p.version == 21] == [item]


# ---- the closing-order rule and placeholders -------------------------------------------------------------------------
def test_lv_open_orders_carries_the_closing_order_rule() -> None:
    body = view("lv_open_orders")
    items = lateral_items(body)
    assert items == {"entry_level": "CASE WHEN o.is_reduce_only AND (o.status = 'FILLED' OR o.order_kind IS NULL) "
                                    "THEN NULL ELSE o.entry_price END"}
    assert len(re.findall(r"\bentry_price\b", body)) == 1               # never exposed unguarded
    assert "x.entry_level AS entry_level" in select_list(body)
    assert ws(body).endswith("WHERE o.status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED')")


def _facts_branch(kind: str) -> str:
    facts = lateral_items(view("lv_entity_events"))["facts"]
    m = re.search(rf"WHEN '{kind}' THEN (jsonb_strip_nulls\(jsonb_build_object\(.*?\)\))(?= WHEN | END)", facts)
    assert m, kind
    return m.group(1)


def test_the_facts_order_branch_carries_the_closing_order_rule() -> None:
    order = _facts_branch("ORDER")
    rule = ("'entry_level', CASE WHEN (coalesce((e.payload->>'is_reduce_only')::boolean, false) AND "
            "(e.payload->>'status' = 'FILLED' OR e.payload->>'order_kind' IS NULL)) "
            "OR (e.payload->>'status' = 'FILLED' AND e.payload->>'order_kind' IS NULL) THEN NULL "
            "ELSE e.payload->'entry_price' END")
    assert rule in order
    assert len(re.findall(r"'entry_price'", order)) == 1
    assert "'entry_price'" not in _facts_branch("ACCOUNT")


def test_cash_flow_and_instrument_rows_are_id_placeholders() -> None:
    body = view("lv_entity_events")
    items = lateral_items(body)
    for col, src in (("entity_key", "e.entity_key"), ("account_id", "e.account_id"),
                     ("broker_ms", "e.broker_time_ms")):
        assert items[col] == f"CASE WHEN e.entity_type IN ('CASH_FLOW', 'INSTRUMENT') THEN NULL ELSE {src} END"
        assert f"x.{col} AS {col}" in select_list(body)
    facts = items["facts"]
    assert facts.startswith("CASE e.entity_type ") and facts.endswith(" END")
    assert re.findall(r"WHEN '(\w+)' THEN", facts) == ["ORDER", "POSITION", "ACCOUNT"]
    assert " ELSE " not in facts.replace("ELSE e.payload->'entry_price'", "")
    assert "x.facts AS facts" in select_list(body)


ARM_PASSTHROUGH = {"outbox_id", "created_at_ms", "kind"}


def test_arm_rows_are_placeholders_in_lv_alerts() -> None:
    body = view("lv_alerts")
    cols = select_list(body)
    bare = {c for c in cols if re.fullmatch(r"\w+", c)}
    assert bare == ARM_PASSTHROUGH
    items = lateral_items(body)
    for name, expr in items.items():
        if name.startswith("tr_"):
            assert "t.tm" in expr                    # NULL unless kind = 'info' (tm is only computed for info rows)
        elif name == "info_class":
            assert expr.startswith("CASE WHEN n.kind <> 'info' THEN NULL")
        else:
            assert (expr.startswith("CASE WHEN n.kind = 'arm' THEN NULL")
                    or expr.startswith("CASE WHEN n.kind <> 'arm' AND")), name
    assert {c.split()[-1] for c in cols} - ARM_PASSTHROUGH == set(items)


def test_cluster_state_mapping_never_exposes_the_phrase() -> None:
    body = view("lv_cluster_state")
    assert "safe_hold_reason" not in " ".join(select_list(body))
    items = lateral_items(body)
    assert set(items) == {"hold_code", "hold_count", "resumed_by"}
    assert "'system:single-pc-auto-resume' THEN 'auto-resume'" in items["resumed_by"]


# ---- codes the engine writes, against the patterns of the migration ----------------------------------------------
TM_PATTERN = re.search(r"regexp_match\(n\.subject,\s*'([^']*)'\)", CODE)
assert TM_PATTERN is not None
TM = re.compile(TM_PATTERN.group(1))


def _transitions() -> list[Any]:
    from src.oversee.types import Cap, Level, Transition
    out = []
    subjects = [("account", "ACC1"), ("account_day", "ACC1:2026-11-20"), ("order", "stonex:ORD-77"),
                ("position", "ctrader:1039708845"), ("account", "pepperstone:4471"), ("order", "mock:O_1@x/y")]
    rules = ["max_position_risk", "max_aggregate_open_risk", "missing_stop_loss", "max_daily_loss", "margin_usage",
             "evaluation"]
    for kind in ("lowered", "resolved", "capped", "uncapped", "activated", "raised"):
        for (stype, sid), rule in zip(subjects, rules, strict=True):
            out.append(Transition(rule_id=rule, subject_type=stype, subject_id=sid,  # type: ignore[arg-type]
                                  account_id="ACC1", transition=kind, sweep_id=1 << 40,  # type: ignore[arg-type]
                                  at_ms=1_795_000_000_000, level=Level.SOFT_HALT, cap=Cap.NONE,
                                  reason="position_risk", activation_id=None))
    return out


def test_every_info_transition_subject_matches_the_tm_pattern() -> None:
    from src.oversee.notify import alerts_for_transitions
    trs = _transitions()
    rows = alerts_for_transitions(trs)
    assert len(rows) == len(trs)
    seen_types = set()
    for t, row in zip(trs, rows, strict=True):
        m = TM.fullmatch(row.title) if row.kind == "info" else None
        if t.transition in ("activated", "raised"):
            assert row.kind == "episode" and TM.match(row.title) is None
            continue
        assert row.kind == "info"
        assert m, row.title
        assert m.group(1) == t.transition
        assert m.group(2) == t.rule_id
        assert m.group(3).upper() == subject_to_db(t.subject_type)
        assert m.group(4) == t.subject_id
        seen_types.add(t.subject_type)
    assert seen_types == {"account", "account_day", "order", "position"}


def test_the_daily_loss_subject_id_with_a_date_matches() -> None:
    m = TM.fullmatch("lowered max_daily_loss account_day:ACC1:2026-11-20")
    assert m and m.group(3) == "account_day" and m.group(4) == "ACC1:2026-11-20"
    assert TM.fullmatch("lowered max_daily_loss account_day:ACC 1") is None
    assert TM.fullmatch("lowered rule account:1.25") is None
    assert TM.fullmatch("SOFT_HALT max_position_risk position:P1") is None


def _ops_commands() -> set[str]:
    out: set[str] = set()
    for path in sorted((ROOT / "src" / "ops").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.name)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
            if name in ("Report", "_file", "_audit") and node.args:
                first = node.args[0] if name != "_audit" else (node.args[1] if len(node.args) > 1 else None)
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    out.add(first.value)
        for m in re.finditer(r"command = '([^']+)'", path.read_text(encoding="utf-8")):
            out.add(m.group(1))
    out |= set(re.findall(r"command = '([^']+)'", (ROOT / "src" / "db" / "cluster_state.py").read_text("utf-8")))
    return out


def test_every_operator_command_matches_command_and_fails_code() -> None:
    commands = _ops_commands()
    assert {"halt.clear", "dlq.release_quarantine", "arm.config.accept", "safe_hold.resume"} <= commands
    cmd_guard = re.search(r"a\.command ~ '([^']+)'", view("lv_operator_actions"))
    assert cmd_guard and cmd_guard.group(1) == COMMAND_RE.pattern
    for c in sorted(commands):
        assert COMMAND_RE.fullmatch(c), c
        assert re.fullmatch(cmd_guard.group(1), c), c
        if "." in c:
            assert not CODE_RE.fullmatch(c), c
            assert not check_value("code", c), c
        assert check_value("command", c), c


def _oversee_reasons() -> set[str]:
    out: set[str] = set()
    rules = ROOT / "src" / "oversee"
    for path in sorted(rules.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.name)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
                pos = {"Result": 1, "Unknown": 0, "RiskUnknown": 0, "band_result": 3, "LevelDecision": 2}.get(name)
                if pos is not None and len(node.args) > pos:
                    a = node.args[pos]
                    if isinstance(a, ast.Constant) and isinstance(a.value, str) and a.value:
                        out.add(a.value)
                if path.name == "rules.py" and name == "append" and node.args:
                    a = node.args[0]
                    if isinstance(a, ast.Constant) and isinstance(a.value, str):
                        out.add(a.value)
                for kw in node.keywords:
                    if kw.arg == "reason" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str) \
                            and kw.value.value:
                        out.add(kw.value.value)
            if isinstance(node, ast.Assign) and path.name in ("rules.py", "risk.py"):
                for t in node.targets:
                    if isinstance(t, ast.Name) and re.fullmatch(r"(STALE_\w+|STOP_KIND_INCONSISTENT)", t.id) \
                            and isinstance(node.value, ast.Constant):
                        out.add(str(node.value.value))
            if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "reason"
                                                    for t in node.targets):
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    out.add(node.value.value)
    return out


def test_every_oversee_reason_code_matches_code_without_a_decimal() -> None:
    reasons = _oversee_reasons()
    composite = {"daily_loss(partial_session,stale_account_image)", "daily_loss(partial_session)",
                 "daily_loss(stale_mark,balance_skew)", "latch"}
    assert {"position_risk", "aggregate_open_risk", "missing_stop_loss", "daily_loss", "risk_unknown",
            "stale_account_image", "stale_mark", "evaluation_failed", "cool_down", "escalation",
            "partial_session"} <= reasons
    sql_guard = re.search(r"o\.reason ~ '([^']+)'", view("lv_oversee_events"))
    assert sql_guard and sql_guard.group(1) == CODE_RE.pattern
    for r in sorted(reasons | composite):
        assert CODE_RE.fullmatch(r), r
        assert not DECIMAL_IN_TEXT.search(r), r
        assert check_value("code", r), r


def test_unknown_symbols_pass_the_symbol_slot() -> None:
    from src.adapters.pepperstone.instruments import unknown_symbol as ct_unknown
    from src.adapters.stonex.instruments import unknown_symbol as sx_unknown
    for s in (sx_unknown("401484347"), sx_unknown(None), sx_unknown(""), ct_unknown(1), ct_unknown(None),
              "UNKNOWN:?", "UNKNOWN:99887766"):
        assert check_value("symbol", s), s
    assert sx_unknown(None) == "UNKNOWN:?" and ct_unknown(None) == "UNKNOWN:?"


# ---- pg tests (a database with every migration applied) --------------------------------------------------------------
pg = [pytest.mark.pg, pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")]
BIG = 9_000_000_000_000_000_000                 # explicit ids: no sequence advances


@pytest.fixture
async def tx() -> AsyncIterator[Any]:
    """A superuser connection inside a transaction that is always rolled back."""
    asyncpg = pytest.importorskip("asyncpg")
    assert DSN is not None
    conn = await asyncpg.connect(DSN, timeout=10)
    t = conn.transaction()
    await t.start()
    try:
        await conn.execute("SET LOCAL statement_timeout = '10s'; SET LOCAL lock_timeout = '3s'")
        yield conn
    finally:
        await t.rollback()
        await conn.close()


async def as_liveview(conn: Any, sql: str, *args: Any) -> list[Any]:
    await conn.execute("SET LOCAL ROLE xtnl_liveview")
    try:
        return list(await conn.fetch(sql, *args))
    finally:
        await conn.execute("RESET ROLE")


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_every_lv_view_is_a_security_barrier(tx: Any) -> None:
    rows = await tx.fetch("SELECT c.relname, c.reloptions FROM pg_class c JOIN pg_namespace n ON n.oid = "
                          "c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'v' AND c.relname LIKE 'lv\\_%'")
    got = {r["relname"]: list(r["reloptions"] or []) for r in rows}
    assert set(got) == set(Q.ALL_VIEWS)
    for name, opts in got.items():
        assert "security_barrier=true" in opts, name


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_the_role_reads_every_view_and_no_table(tx: Any) -> None:
    from src.db.readonly_role import SQL_READABLE_TABLES, SQL_VIEW_SELECT
    await tx.execute("SET LOCAL ROLE xtnl_liveview")
    assert await tx.fetchval(SQL_READABLE_TABLES) == 0
    for v in sorted(Q.ALL_VIEWS):
        assert await tx.fetchval(SQL_VIEW_SELECT, v), v
    await tx.execute("RESET ROLE")
    attrs = await tx.fetchrow("SELECT rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolreplication, "
                              "rolbypassrls, rolconnlimit, rolconfig FROM pg_roles WHERE rolname = 'xtnl_liveview'")
    assert attrs is not None
    assert (attrs[0], attrs[1], attrs[2], attrs[3], attrs[4], attrs[5], attrs[6]) == (False, False, False, False,
                                                                                      False, False, 3)
    assert set(attrs[7]) >= {"default_transaction_read_only=on", "statement_timeout=5s",
                             "idle_in_transaction_session_timeout=10s"}


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
@pytest.mark.parametrize(("reason", "code", "count"), [
    ("safe_hold already set", "already_set", None),
    ("3 abnormal term ends within 15 min", "abnormal_term_ends", 3),
    ("12 epoch changes within 30 min", "epoch_changes", 12),
    ("999999 epoch changes within 30 min", "epoch_changes", 999999),
    ("operator note: 1.25 lots", None, None),
    ("3 abnormal term ends within 15 minutes", None, None),   # hold_count is NULL whenever hold_code is
    ("x3 epoch changes within 30 min", None, None),
    (None, None, None),
])
async def test_pg_safe_hold_phrase_maps_to_a_code_and_count(tx: Any, reason: str | None, code: str | None,
                                                            count: int | None) -> None:
    await tx.execute("UPDATE cluster_state SET safe_hold = true, safe_hold_reason = $1 WHERE id = 1", reason)
    rows = await as_liveview(tx, "SELECT hold_code, hold_count FROM lv_cluster_state")
    assert [(r["hold_code"], r["hold_count"]) for r in rows] == [(code, count)]


def test_the_safe_hold_phrases_are_the_leader_module_phrases() -> None:
    text = (ROOT / "src" / "db" / "leader.py").read_text(encoding="utf-8")
    assert "safe_hold already set" in text
    assert "abnormal term ends within 15 min" in text and "epoch changes within 30 min" in text


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
@pytest.mark.parametrize(("raw", "want"), [
    ("system:single-pc-auto-resume", "auto-resume"),
    ("j.smith", "j.smith"),
    ("ops@desk", "ops@desk"),
    ("system:other", None),
    ("bad name", None),
    ("x", None),
    ("v1.25", None),
    (None, None),
])
async def test_pg_resumed_by_mapping(tx: Any, raw: str | None, want: str | None) -> None:
    await tx.execute("UPDATE cluster_state SET resumed_by = $1 WHERE id = 1", raw)
    rows = await as_liveview(tx, "SELECT resumed_by FROM lv_cluster_state")
    assert [r["resumed_by"] for r in rows] == [want]


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_auth_state_is_the_latest_row_per_adapter(tx: Any) -> None:
    a, b = f"lvt_{secrets.token_hex(4)}", f"lvt_{secrets.token_hex(4)}"
    ins = ("INSERT INTO adapter_auth_state (adapter_id, credential_generation, state, failed_at_ms, detail, "
           "rearmed_at_ms) VALUES ($1, $2, $3, $4, 'secret detail', $5)")
    await tx.execute(ins, a, "gen-1", "AUTH_FAILED", 1000, None)
    await tx.execute(ins, a, "gen-2", "AUTH_FAILED", 2000, None)
    await tx.execute(ins, b, "gen-9", "REARMED", 5000, 6000)
    await tx.execute(ins, b, "gen-1", "AUTH_FAILED", 4000, None)
    rows = await as_liveview(tx, "SELECT * FROM lv_auth_state WHERE adapter_id = ANY($1::text[]) ORDER BY adapter_id",
                             [a, b])
    got = {r["adapter_id"]: (r["state"], r["failed_at_ms"], r["rearmed_at_ms"]) for r in rows}
    assert got == {a: ("AUTH_FAILED", 2000, None), b: ("REARMED", 5000, 6000)}
    assert set(rows[0].keys()) == {"adapter_id", "state", "failed_at_ms", "rearmed_at_ms"}


ORDER_CASES = [
    # (name, status, is_reduce_only, order_kind, entry visible?)
    ("ctrader_market_close_partial", "PARTIALLY_FILLED", True, None, False),
    ("ctrader_market_close_part_filled_cancelled", "CANCELLED", True, None, False),
    ("stonex_history_closing_trade", "FILLED", True, None, False),
    ("filled_stop_child", "FILLED", True, "STOP", False),
    ("working_stop_child", "WORKING", True, "STOP", True),
    ("working_limit_child", "WORKING", True, "LIMIT", True),
    ("working_entry_limit", "WORKING", False, "LIMIT", True),
    ("pending_entry_market", "PENDING", False, None, True),
    # a closing trade read without its closing evidence (reduce-only lost): FILLED with no kind never has a level
    ("filled_kindless_not_reduce_only", "FILLED", False, None, False),
    ("filled_entry_stop", "FILLED", False, "STOP", True),
]
OPEN = ("PENDING", "WORKING", "PARTIALLY_FILLED")


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_the_closing_order_rule_in_open_orders_and_facts(tx: Any) -> None:
    import json
    adapter = f"lvt_{secrets.token_hex(4)}"
    seq0 = 1_885_000_000_000_000_000          # inside a monthly partition (or the default one)
    for i, (name, status, ro, kind, _visible) in enumerate(ORDER_CASES):
        await tx.execute(
            "INSERT INTO orders_state (adapter_id, broker_order_id, account_id, symbol, side, status, entry_price, "
            "stop_loss, take_profit, ordered_units, remaining_units, raw_qty, is_reduce_only, order_kind, "
            "broker_updated_at_ms, observed_seq, source, version, seq_id, checksum, digest_hash) VALUES "
            "($1, $2, 'ACC', 'EURUSD', 'SELL', $3, 1.10203, 1.2, 1.0, 1000, 500, 1000, $4, $5, 1, 1, 'STREAM', "
            "repeat('0', 56), $6, repeat('0', 64), repeat('0', 64))",
            adapter, name, status, ro, kind, seq0 + i)
        payload = {"broker_order_id": name, "status": status, "is_reduce_only": ro, "order_kind": kind,
                   "entry_price": "1.10203", "stop_loss": "1.2", "take_profit": "1.0", "symbol": "EURUSD",
                   "side": "SELL", "ordered_units": "1000", "remaining_units": "500", "raw_qty": "1000",
                   "accrued_swap": "-1.5", "commission": "2"}
        await tx.execute(
            "INSERT INTO event_log (log_id, seq_id, entity_type, entity_key, adapter_id, account_id, action, "
            "payload, epoch_revision, origin, broker_time_ms) VALUES ($1, $2, 'ORDER', $3, $4, 'ACC', 'APPLIED', "
            "$5::jsonb, 1, 'stream', 1)",
            BIG + i, seq0 + i, f"{adapter}:{name}", adapter, json.dumps(payload))
    open_rows = await as_liveview(tx, "SELECT broker_order_id, entry_level, stop_level, target_level, leg_kind "
                                      "FROM lv_open_orders WHERE adapter_id = $1", adapter)
    facts_rows = await as_liveview(tx, "SELECT entity_key, facts FROM lv_entity_events WHERE adapter_id = $1",
                                   adapter)
    opened = {r["broker_order_id"]: r for r in open_rows}
    facts = {r["entity_key"].split(":", 1)[1]: json.loads(r["facts"]) for r in facts_rows}
    assert set(opened) == {c[0] for c in ORDER_CASES if c[1] in OPEN}
    assert set(facts) == {c[0] for c in ORDER_CASES}
    for name, status, _ro, kind, visible in ORDER_CASES:
        f = facts[name]
        assert ("entry_level" in f) is visible, name
        assert f["stop_level"] == "1.2" and f["target_level"] == "1.0"
        assert set(f) <= ALLOWED_FACT_KEYS, set(f) - ALLOWED_FACT_KEYS
        assert f.get("leg_kind") == kind
        if status in OPEN:
            r = opened[name]
            assert (r["entry_level"] is not None) is visible, name
            assert r["leg_kind"] == kind


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_cash_flow_and_instrument_rows_are_placeholders(tx: Any) -> None:
    adapter = f"lvt_{secrets.token_hex(4)}"
    seq0 = 1_885_000_000_100_000_000
    for i, etype in enumerate(("CASH_FLOW", "INSTRUMENT")):
        await tx.execute(
            "INSERT INTO event_log (log_id, seq_id, entity_type, entity_key, adapter_id, account_id, action, "
            "payload, epoch_revision, origin, broker_time_ms) VALUES ($1, $2, $3, 'k1', $4, 'ACC', $5, "
            "'{\"amount\": \"12.5\", \"point_size\": \"0.0001\"}'::jsonb, 1, 'stream', 77)",
            BIG + 100 + i, seq0 + i, etype, adapter, "CASH_FLOW" if etype == "CASH_FLOW" else "APPLIED")
    rows = await as_liveview(tx, "SELECT * FROM lv_entity_events WHERE adapter_id = $1 ORDER BY log_id", adapter)
    assert [r["entity_type"] for r in rows] == ["CASH_FLOW", "INSTRUMENT"]
    for r in rows:
        assert (r["entity_key"], r["account_id"], r["broker_ms"], r["facts"]) == (None, None, None, None)
        assert r["log_id"] is not None and r["version_path"] == "stream"


@pytest.mark.pg
@pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")
async def test_pg_alerts_arm_placeholders_and_transition_captures(tx: Any) -> None:
    import json
    created = 1_795_100_000_000
    ins = ("INSERT INTO notification_outbox (outbox_id, created_at_ms, severity, channels, subject, body, kind, "
           "account_id, episode, attempts, last_error) VALUES ($1, $2, $3, $4, $5, 'body text 1.25', $6, 'ACC', "
           "$7::jsonb, $8, $9)")
    base = BIG + 200
    await tx.execute(ins, base, created, "INFO", ["arm_email"], "ARM notice 1.2345", "arm",
                     json.dumps(["r", "position", "x", 1, 2]), 2, "smtp 1.5 err")
    await tx.execute(ins, base + 1, created, "INFO", ["email"], "capped max_daily_loss account_day:ACC:2026-11-20",
                     "info", None, 0, None)
    await tx.execute(ins, base + 2, created, "INFO", ["email"], "lowered max_position_risk position:P 1.5", "info",
                     None, 0, None)
    await tx.execute(ins, base + 3, created, "SOFT", ["email", "+escalated"], "SOFT_HALT x", "episode",
                     json.dumps(["max_position_risk", "position", "ctrader:1", 0, 2]), 1, "deduplicated")
    rows = await as_liveview(tx, "SELECT * FROM lv_alerts WHERE outbox_id >= $1 AND outbox_id < $2 "
                                 "AND created_at_ms = $3 ORDER BY outbox_id", base, base + 10, created)
    by = {r["outbox_id"] - base: dict(r) for r in rows}
    arm = by[0]
    assert {k for k, v in arm.items() if v is not None} == {"outbox_id", "created_at_ms", "kind"}
    tr = by[1]
    assert (tr["info_class"], tr["tr_transition"], tr["tr_rule_id"], tr["tr_subject_type"], tr["tr_subject_id"]) == \
        ("transition", "capped", "max_daily_loss", "ACCOUNT_DAY", "ACC:2026-11-20")
    notice = by[2]
    assert notice["info_class"] == "notice" and notice["tr_transition"] is None and notice["tr_subject_id"] is None
    ep = by[3]
    assert (ep["ep_rule_id"], ep["ep_subject_type"], ep["ep_subject_id"], ep["ep_activation_id"], ep["ep_level"]) == \
        ("max_position_risk", "POSITION", "ctrader:1", 0, 2)
    assert ep["delivery"] == "suppressed" and ep["escalated"] is True and ep["info_class"] is None
    for r in rows:
        for v in r.values():
            assert not (isinstance(v, str) and DECIMAL_IN_TEXT.search(v)), v
