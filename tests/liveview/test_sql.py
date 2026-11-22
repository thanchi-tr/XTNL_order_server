"""Every statement the live view runs (docs/design/live-view.md s4.2, s6 "sql"): one bounded SELECT from lv_ views
(or `SELECT now_ms()`), no 0015-0020 column, nothing that writes or calls a pg_ function, and ALL_VIEWS equal to the
views the statements read (tests/db/test_liveview_role_migration.py keeps that equal to the 0021 grant list)."""
from __future__ import annotations

import re

import pytest

from src.liveview import sql as Q

FROM_JOIN = re.compile(r"\b(?:FROM|JOIN)\s+(\w+)", re.I)
# tests/db/test_schema_compat.py: the 0015-0020 columns, searched after output aliases are stripped
NEW_COLUMN = re.compile(r"\b(order_kind|price_increment|min_stop_distance|asset_class|broker_time_ms|origin)\b")
FORBIDDEN = re.compile(r"\b(INSERT|UPDATE|DELETE|ALTER|CREATE|GRANT|REVOKE|COPY|TRUNCATE|DROP|EXECUTE|CALL|DO|"
                       r"LOCK|VACUUM|SET|RESET|NOTIFY|LISTEN)\b", re.I)


def _ids() -> list[str]:
    return [f"{i}:{s[:40]}" for i, s in enumerate(Q.ALL_SQL)]


def test_all_sql_is_not_empty() -> None:
    assert len(set(Q.ALL_SQL)) > 40      # an unpruned source's sql_max_pruned is its sql_max (same text)
    assert Q.SQL_NOW == "SELECT now_ms()" and Q.SQL_NOW in Q.ALL_SQL


def test_all_sql_holds_every_module_statement() -> None:
    module = {v for k, v in vars(Q).items() if k.startswith("SQL_") and isinstance(v, str)}
    assert module <= set(Q.ALL_SQL)
    for src in Q.TAIL_SOURCES:
        assert set(src.statements()) <= set(Q.ALL_SQL), src.name


@pytest.mark.parametrize("stmt", Q.ALL_SQL, ids=_ids())
def test_each_statement_is_one_bounded_select_from_lv_views(stmt: str) -> None:
    assert stmt == stmt.strip()
    assert stmt.startswith("SELECT ")
    assert stmt.upper().count("SELECT") == 1, "no subquery, no second statement"
    if stmt == "SELECT now_ms()":
        return
    rels = FROM_JOIN.findall(stmt)
    assert rels, "names at least one view"
    assert all(r.startswith("lv_") for r in rels), rels
    assert re.search(r"\bLIMIT\s+(\d+|\$\d+)\b", stmt)


@pytest.mark.parametrize("stmt", Q.ALL_SQL, ids=_ids())
def test_no_statement_writes_or_calls_a_system_function(stmt: str) -> None:
    assert ";" not in stmt
    assert not FORBIDDEN.search(stmt), FORBIDDEN.search(stmt)
    assert not re.search(r"\bpg_\w*", stmt, re.I)
    assert "extract(epoch" not in stmt.lower()
    assert "--" not in stmt and "/*" not in stmt
    assert not re.search(r"\b(?:dblink|lo_\w+|set_config|current_setting|nextval|setval)\s*\(", stmt, re.I)


@pytest.mark.parametrize("stmt", Q.ALL_SQL, ids=_ids())
def test_no_statement_names_a_0015_to_0020_column(stmt: str) -> None:
    assert not NEW_COLUMN.search(re.sub(r"\bAS \w+", "", stmt)), stmt


def test_the_column_lists_use_only_the_renamed_columns() -> None:
    cols = " ".join([Q.ENTITY_COLS, Q.ORDER_COLS, Q.POSITION_COLS])
    assert "version_path" in cols and "broker_ms" in cols and "leg_kind" in cols
    assert not re.search(r"\b\w*price\w*\b", " ".join(Q.ALL_SQL))


def test_all_views_equals_the_views_the_statements_read() -> None:
    named = {r for s in Q.ALL_SQL for r in FROM_JOIN.findall(s)}
    assert named == set(Q.ALL_VIEWS)
    assert len(Q.ALL_VIEWS) == 18


def test_parameters_are_numbered_placeholders_only() -> None:
    """No value from a URL reaches a statement: no format slot or quote is left open for one."""
    for stmt in Q.ALL_SQL:
        assert "{" not in stmt and "}" not in stmt and "%s" not in stmt
        for lit in re.findall(r"'([^']*)'", stmt):
            assert re.fullmatch(r"[A-Za-z_]+", lit), lit


def test_each_tail_source_reads_its_own_view_by_its_key() -> None:
    for src in Q.TAIL_SOURCES:
        assert src.view in Q.ALL_VIEWS
        for stmt in src.statements():
            assert FROM_JOIN.findall(stmt) == [src.view]
            assert src.key in stmt
    assert Q.PAGE_ROWS == 500
