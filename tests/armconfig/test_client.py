"""The Supabase reader (plan S33): settings, TLS, self-check, bounded read, sanitised errors. No network."""
from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import certifi
import pytest

from src.armconfig import client as C
from src.armconfig.document import MAX_ROWS_PER_TABLE, TABLES, Document, DocumentProblem
from tests.armconfig.docs import raw_doc, route_row, strategy_row

DSN = "postgresql://xtnl_arm_reader:Pl4nted-Secret@db.planted-host.example:6543/postgres"
SECRETS = ("Pl4nted-Secret", "planted-host", DSN, "6543")


# ---------------------------------------------------------------------------------------------------- settings, TLS
def test_settings_need_a_dsn_and_a_pinned_ca_and_never_show_the_dsn() -> None:
    assert C.parse_source_settings({}) is None
    assert C.parse_source_settings({C.DSN_VAR: "  "}) is None
    problems = C.parse_source_settings({C.DSN_VAR: DSN})
    assert isinstance(problems, list) and problems == [f"{C.CA_FILE_VAR} is required (the pinned CA of the "
                                                       "configuration database)"]
    bad = C.parse_source_settings({C.DSN_VAR: "host=x password=y", C.CA_FILE_VAR: "ca.pem"})
    assert isinstance(bad, list) and len(bad) == 1 and "password" not in bad[0]
    ok = C.parse_source_settings({C.DSN_VAR: DSN, C.CA_FILE_VAR: "ca.pem"})
    assert isinstance(ok, C.SourceSettings) and ok.dsn == DSN
    assert not any(s in repr(ok) for s in SECRETS)
    assert not any(s in " ".join(problems) for s in SECRETS)


def test_tls_context_requires_the_certificate_the_hostname_and_tls_1_2() -> None:
    ctx = C.tls_context(certifi.where())
    assert ctx.verify_mode is ssl.CERT_REQUIRED and ctx.check_hostname
    assert ctx.minimum_version >= ssl.TLSVersion.TLSv1_2
    assert ctx.protocol is ssl.PROTOCOL_TLS_CLIENT
    assert ctx.get_ca_certs()                       # only what the pinned file holds (no system store loaded)


# ---------------------------------------------------------------------------------------------------- self-check
def table_row(name: str, *, rls: bool = True, forced: bool = True, select: bool = True,
              extra: str | None = None, column: str | None = None) -> tuple[Any, ...]:
    privs = [p == "SELECT" and select or p == extra for p in C.PRIVILEGES]
    columns = [p == column for p in C.COLUMN_PRIVILEGES]
    return (name, rls, forced, *privs, *columns)


GOOD_TABLES = [table_row(t) for t in C.ARM_TABLES]
GOOD_ROLE = [(False, False, False, 0)]


def test_self_check_passes_a_hardened_reader() -> None:
    assert C.self_check_problems(GOOD_TABLES, GOOD_ROLE) == ()


@pytest.mark.parametrize(("tables", "role", "code"), [
    ([table_row(t, extra="INSERT" if t == "strategies" else None) for t in C.ARM_TABLES], GOOD_ROLE,
     "privilege_insert:strategies"),
    ([table_row(t, extra="DELETE" if t == "schema_version" else None) for t in C.ARM_TABLES], GOOD_ROLE,
     "privilege_delete:schema_version"),
    ([table_row(t, forced=t != "assignments") for t in C.ARM_TABLES], GOOD_ROLE, "rls_not_forced:assignments"),
    ([table_row(t, rls=t != "route_policy") for t in C.ARM_TABLES], GOOD_ROLE, "rls_disabled:route_policy"),
    ([table_row(t, select=t != "strategies") for t in C.ARM_TABLES], GOOD_ROLE, "no_select:strategies"),
    (GOOD_TABLES[1:], GOOD_ROLE, "missing_table:schema_version"),
    ([*GOOD_TABLES, table_row("notes", extra="UPDATE")], GOOD_ROLE, "privilege_update:other"),
    (GOOD_TABLES, [(True, False, False, 0)], "role_bypassrls"),
    (GOOD_TABLES, [(False, True, False, 0)], "role_inherit"),
    (GOOD_TABLES, [(False, False, True, 0)], "role_superuser"),
    (GOOD_TABLES, [], "role_unknown"),
    # a column-level grant passes has_table_privilege: the column check catches it
    ([table_row(t, column="UPDATE" if t == "assignments" else None) for t in C.ARM_TABLES], GOOD_ROLE,
     "column_privilege_update:assignments"),
    ([table_row(t, column="INSERT" if t == "strategies" else None) for t in C.ARM_TABLES], GOOD_ROLE,
     "column_privilege_insert:strategies"),
    ([*GOOD_TABLES, table_row("notes", column="REFERENCES")], GOOD_ROLE, "column_privilege_references:other"),
    # a row without the column checks counts as holding them (fail closed)
    ([row[:3 + len(C.PRIVILEGES)] for row in GOOD_TABLES], GOOD_ROLE, "column_privilege_insert:strategies"),
    # a member of any role (SET ROLE works despite NOINHERIT; a PG 16+ grant WITH INHERIT TRUE overrides it)
    (GOOD_TABLES, [(False, False, False, 1)], "role_member"),
    (GOOD_TABLES, [(False, False, False, None)], "role_member"),
    (GOOD_TABLES, [(False, False, False)], "role_unknown"),
])
def test_self_check_fails_closed(tables: list[tuple[Any, ...]], role: list[tuple[Any, ...]], code: str) -> None:
    assert code in C.self_check_problems(tables, role)


def test_a_view_in_the_schema_with_select_only_passes() -> None:
    assert C.self_check_problems([*GOOD_TABLES, table_row("strategies_view")], GOOD_ROLE) == ()


def test_the_self_check_statements_read_column_privileges_and_memberships() -> None:
    for p in C.COLUMN_PRIVILEGES:
        assert f"pg_catalog.has_any_column_privilege(c.oid, '{p}')" in C.SQL_SELF_TABLES
    assert "pg_catalog.pg_auth_members" in C.SQL_SELF_ROLE and "m.member = r.oid" in C.SQL_SELF_ROLE


# ---------------------------------------------------------------------------------------------------- decoding
def rows_of(raw: dict[str, Any]) -> dict[str, list[tuple[Any, ...]]]:
    out: dict[str, list[tuple[Any, ...]]] = {}
    for t in TABLES:
        out[t] = []
        for r in raw[t]:
            text = json.dumps(r, default=str)
            out[t].append((r["id"], len(text.encode()), text))
    return out


def test_build_document_reads_rows_with_decimal_numbers() -> None:
    raw = raw_doc(strategies=[strategy_row(params={"guards": [{"kind": "first_bar", "version": 1, "params": {}}]})])
    rows = rows_of(raw)
    doc = C.build_document([(1,)], rows)
    assert isinstance(doc, Document) and len(doc.rows) == 3 and doc.schema_version == 1
    rows["strategies"] = [(1, 40, '{"id": 1, "x": 1.50}')]
    doc2 = C.build_document([(1,)], rows)
    assert isinstance(doc2, Document)
    assert doc2.table("strategies")[0].columns()["x"] == Decimal("1.5")


def test_build_document_bounds() -> None:
    rows = rows_of(raw_doc())
    assert isinstance(C.build_document([], rows), DocumentProblem)
    assert C.build_document([(1,), (2,)], rows).code == "schema_version_form"  # type: ignore[union-attr]
    many = dict(rows)
    many["route_policy"] = [(i, 10, json.dumps(route_row(i))) for i in range(1, MAX_ROWS_PER_TABLE + 2)]
    p = C.build_document([(1,)], many)
    assert isinstance(p, DocumentProblem) and p.code == "too_many_rows" and p.table == "route_policy"
    big = dict(rows)
    big["strategies"] = [(7, C.ROW_TEXT_CAP + 1, None)]
    p = C.build_document([(1,)], big)
    assert isinstance(p, DocumentProblem) and (p.code, p.row_id) == ("row_too_large", 7)
    huge = dict(rows)
    huge["strategies"] = [(i, C.ROW_TEXT_CAP, "{}") for i in range(1, C.READ_BYTE_CAP // C.ROW_TEXT_CAP + 2)]
    p = C.build_document([(1,)], huge)
    assert isinstance(p, DocumentProblem) and p.code == "read_too_large"
    deep = dict(rows)
    text = '{"id": 1, "p": ' + "[" * 40 + "]" * 40 + "}"
    deep["strategies"] = [(1, len(text), text)]
    p = C.build_document([(1,)], deep)
    assert isinstance(p, DocumentProblem) and p.code == "value_form" and p.detail == "nesting too deep"
    nan = dict(rows)
    nan["strategies"] = [(1, 20, '{"id": 1, "p": NaN}')]
    p = C.build_document([(1,)], nan)
    assert isinstance(p, DocumentProblem) and p.code == "value_form"


def test_json_depth_ignores_brackets_inside_strings() -> None:
    assert C.json_depth('{"a": "[[[{{{", "b": [1, [2]]}') == 3
    assert C.json_depth('"\\"[["') == 0


# ---------------------------------------------------------------------------------------------------- the source
class FakeConn:
    def __init__(self, *, tables: Sequence[Any] = GOOD_TABLES, role: Sequence[Any] = GOOD_ROLE,
                 raw: dict[str, Any] | None = None, fail_on: str | None = None, error: Exception | None = None,
                 hang_close: bool = False) -> None:
        self.statements: list[str] = []
        self.tables, self.role = tables, role
        self.rows = rows_of(raw or raw_doc())
        self.fail_on, self.error = fail_on, error
        self.closed = self.terminated = False
        self.hang_close = hang_close

    async def execute(self, query: str, *args: Any) -> str:
        self.statements.append(query)
        return "OK"

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self.statements.append(query)
        if self.fail_on is not None and self.fail_on in query:
            assert self.error is not None
            raise self.error
        if query == C.SQL_SELF_TABLES:
            return list(self.tables)
        if query == C.SQL_SELF_ROLE:
            return list(self.role)
        if query == C.SQL_SCHEMA_VERSION:
            return [(1,)]
        for t, sql in C.SQL_ROWS.items():
            if query == sql:
                assert args == (C.ROW_LIMIT, C.ROW_TEXT_CAP)
                return self.rows[t]
        raise AssertionError(query)

    async def close(self) -> None:
        if self.hang_close:
            await asyncio.Event().wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True


class PlantedDbError(Exception):
    sqlstate = "42501"


def source_over(conn: FakeConn | Exception, calls: list[dict[str, Any]] | None = None) -> C.SupabaseConfigSource:
    async def connect(dsn: str, **kw: Any) -> FakeConn:
        if calls is not None:
            calls.append({"dsn": dsn, **kw})
        if isinstance(conn, Exception):
            raise conn
        return conn

    settings = C.SourceSettings(dsn=DSN, ca_file=certifi.where())
    return C.SupabaseConfigSource(settings, connect)


async def test_read_runs_one_read_only_transaction_after_the_self_check() -> None:
    conn = FakeConn()
    calls: list[dict[str, Any]] = []
    src = source_over(conn, calls)
    doc = await src.read()
    assert isinstance(doc, Document) and len(doc.rows) == 3
    assert conn.statements[:5] == [C.SQL_BEGIN, C.SQL_SEARCH_PATH, C.SQL_STATEMENT_TIMEOUT, C.SQL_SELF_TABLES,
                                   C.SQL_SELF_ROLE]
    assert conn.statements[-1] == C.SQL_ROLLBACK and conn.closed
    assert "READ ONLY" in C.SQL_BEGIN and "pg_catalog" in C.SQL_SEARCH_PATH
    (call,) = calls
    assert call["dsn"] == DSN and isinstance(call["ssl"], ssl.SSLContext)
    assert call["ssl"].verify_mode is ssl.CERT_REQUIRED and call["timeout"] == C.CONNECT_TIMEOUT_S
    assert call["server_settings"] == {"application_name": C.APPLICATION_NAME}
    assert not any(s in repr(src) for s in SECRETS)
    # every read statement names its relation by schema (search_path is pg_catalog)
    for sql in (C.SQL_SCHEMA_VERSION, *C.SQL_ROWS.values()):
        assert " arm." in sql and "LIMIT" in sql


async def test_a_failed_self_check_raises_and_still_rolls_back_and_closes() -> None:
    conn = FakeConn(role=[(True, True, False, 0)])
    with pytest.raises(C.SelfCheckError) as err:
        await source_over(conn).read()
    assert err.value.problems == ("role_bypassrls", "role_inherit")
    assert C.SQL_ROLLBACK in conn.statements and conn.closed
    assert not any(t in conn.statements for t in C.SQL_ROWS.values())     # nothing read past a failed check


async def test_driver_errors_are_sanitised() -> None:
    planted = f"connection to {DSN} failed: password authentication failed for planted-host"
    with pytest.raises(C.ConfigSourceError) as err:
        await source_over(OSError(planted)).read()
    assert (err.value.stage, err.value.reason) == ("connect", "OSError")
    assert not any(s in str(err.value) for s in SECRETS) and err.value.__suppress_context__
    conn = FakeConn(fail_on="arm.strategies", error=PlantedDbError(planted))
    with pytest.raises(C.ConfigSourceError) as err:
        await source_over(conn).read()
    assert str(err.value) == "read: PlantedDbError(42501)" and conn.closed
    assert C.safe_source_error(PlantedDbError(planted)) == "PlantedDbError(42501)"
    with pytest.raises(C.ConfigSourceError) as err:
        await C.SupabaseConfigSource(C.SourceSettings(dsn=DSN, ca_file="no-such-ca-file.pem"),
                                     lambda *a, **k: None).read()   # type: ignore[arg-type,return-value]
    assert err.value.stage == "tls_ca" and "no-such" not in str(err.value)


async def test_a_hanging_close_is_terminated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(C, "CLOSE_TIMEOUT_S", 0.05)
    conn = FakeConn(hang_close=True)
    doc = await source_over(conn).read()
    assert isinstance(doc, Document) and conn.terminated
