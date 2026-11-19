"""`deploy/supabase/arm_schema.sql` and the client's real statements against a DISPOSABLE PostgreSQL (plan S33):
ORDER_SERVER_TEST_DSN as a superuser (it creates the `xtnl_arm_*` roles and `SET ROLE`s to them; the roles have no
password and cannot log in from outside). Skipped when the variable is not set or the role is not a superuser.

The file is applied twice (idempotent) as a Supabase-like owner (not a superuser, no BYPASSRLS, project-wide default
privileges granting everything to `anon` / `authenticated` / `service_role`): nothing in `arm` is left to the API
roles and the schema version upsert works with RLS forced; the writer's inserts get their id and metadata from the guards (an
`OVERRIDING SYSTEM VALUE` id is replaced); UPDATE / DELETE and a backdated `effective_from_ms` are refused; the
reader's self-check passes and the read validates through the real `SupabaseConfigSource`; a column-level grant and
a role membership make the self-check fail closed."""
from __future__ import annotations

import asyncio
import os
import ssl
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from src.armconfig.client import SelfCheckError, SourceSettings, SupabaseConfigSource
from src.armconfig.document import Document

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
SQL = (Path(__file__).resolve().parents[2] / "deploy" / "supabase" / "arm_schema.sql").read_text(encoding="utf-8")

pytestmark = [pytest.mark.integration, pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")]


class ReaderConn:
    """A real connection `SET ROLE`d to xtnl_arm_reader (the client's `RawConnection`)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    async def execute(self, query: str, *args: Any) -> Any:
        return await self._conn.execute(query, *args)

    async def fetch(self, query: str, *args: Any) -> Sequence[Sequence[Any]]:
        return [tuple(r) for r in await self._conn.fetch(query, *args)]

    async def close(self) -> None:
        await self._conn.close()

    def terminate(self) -> None:
        self._conn.terminate()


async def _connect() -> Any:
    import asyncpg

    return await asyncpg.connect(DSN)


async def _reader(_dsn: str, **_kw: Any) -> ReaderConn:
    conn = await _connect()
    await conn.execute("SET ROLE xtnl_arm_reader")
    return ReaderConn(conn)


def _source() -> SupabaseConfigSource:
    # the disposable server has no TLS: the context is built but the injected connect ignores it
    return SupabaseConfigSource(SourceSettings(dsn="postgresql://unused@localhost/x", ca_file="unused"), _reader,
                                tls=lambda _ca: ssl.create_default_context())


API_ROLES = ("anon", "authenticated", "service_role")
OWNER = "xtnl_arm_test_owner"


async def _drop_test_roles(admin: Any) -> None:
    for role in ("xtnl_arm_reader", "xtnl_arm_writer", OWNER):
        if await admin.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", role):
            await admin.execute(f"DROP OWNED BY {role} CASCADE")
            await admin.execute(f"DROP ROLE {role}")


async def _apply_as_supabase_owner(admin: Any) -> None:
    """Apply the file twice as a Supabase-like owner: not a superuser, no BYPASSRLS, with project-wide default
    privileges that grant everything to the API roles (as Supabase's do)."""
    await admin.execute(f"CREATE ROLE {OWNER} NOSUPERUSER NOBYPASSRLS CREATEROLE")
    await admin.execute(f"GRANT CREATE ON DATABASE {await admin.fetchval('SELECT current_database()')} TO {OWNER}")
    for role in API_ROLES:
        if not await admin.fetchval("SELECT 1 FROM pg_roles WHERE rolname = $1", role):
            await admin.execute(f"CREATE ROLE {role} NOLOGIN")
    await admin.execute(f"SET ROLE {OWNER}")
    api = ", ".join(API_ROLES)
    for kind in ("TABLES", "SEQUENCES", "FUNCTIONS"):
        await admin.execute(f"ALTER DEFAULT PRIVILEGES GRANT ALL ON {kind} TO {api}")
    await admin.execute(SQL)
    await admin.execute(SQL)                                             # idempotent, RLS already forced
    await admin.execute("RESET ROLE")
    leaks = await admin.fetch(
        "SELECT r, c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, "
        "unnest($1::text[]) r WHERE n.nspname = 'arm' AND ("
        "(c.relkind = 'r' AND (has_table_privilege(r, c.oid, 'SELECT') OR has_table_privilege(r, c.oid, 'INSERT')))"
        " OR (c.relkind = 'S' AND has_sequence_privilege(r, c.oid, 'USAGE'))) "
        "UNION ALL SELECT r, p.proname::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace, "
        "unnest($1::text[]) r WHERE n.nspname = 'arm' AND has_function_privilege(r, p.oid, 'EXECUTE')",
        list(API_ROLES))
    assert [tuple(x) for x in leaks] == []
    assert await admin.fetchval("SELECT version FROM arm.schema_version") == 1
    assert await admin.fetchval("SELECT relforcerowsecurity FROM pg_class WHERE oid = 'arm.schema_version'::regclass")


async def _scenario() -> None:
    admin = await _connect()
    try:
        if not await admin.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname = current_user"):
            pytest.skip("ORDER_SERVER_TEST_DSN is not a superuser")
        await admin.execute("DROP SCHEMA IF EXISTS arm CASCADE")
        await _drop_test_roles(admin)
        await _apply_as_supabase_owner(admin)
        now = int(time.time() * 1000)
        await admin.execute("SET ROLE xtnl_arm_writer")
        await admin.execute(
            "INSERT INTO arm.strategies (code, version, kind, impl_version, params, set_at_ms, \"session_user\", "
            "\"current_user\") VALUES ('be_a', 1, 'break_even_on_risk_close', 1, "
            "'{\"guards\": [{\"kind\": \"first_bar\", \"version\": 1, \"params\": {}}]}', 0, 'x', 'x')")
        await admin.execute(
            "INSERT INTO arm.assignments (id, symbol, strategy_code, strategy_version, enabled, effective_from_ms, "
            "set_at_ms, \"session_user\", \"current_user\") OVERRIDING SYSTEM VALUE "
            f"VALUES (-5, NULL, 'be_a', 1, true, {now}, 0, 'x', 'x')")
        row = await admin.fetchrow("SELECT id, set_at_ms, \"current_user\" FROM arm.assignments")
        assert row["id"] >= 1 and row["set_at_ms"] > 0 and row["current_user"] == "xtnl_arm_writer"
        for bad in ("UPDATE arm.assignments SET enabled = false", "DELETE FROM arm.assignments",
                    "INSERT INTO arm.assignments (strategy_code, strategy_version, enabled, effective_from_ms, "
                    f"set_at_ms, \"session_user\", \"current_user\") VALUES ('be_a', 1, true, {now - 3_600_000}, 0, "
                    "'x', 'x')"):
            with pytest.raises(Exception):
                await admin.execute(bad)
        await admin.execute("RESET ROLE")

        doc = await _source().read()
        assert isinstance(doc, Document) and doc.schema_version == 1
        assert {r.ref.split(":")[0] for r in doc.rows} == {"strategies", "assignments"}
        assert all(r.row_id >= 1 for r in doc.rows)

        await admin.execute("GRANT UPDATE (enabled) ON arm.assignments TO xtnl_arm_reader")
        with pytest.raises(SelfCheckError) as err:
            await _source().read()
        assert "column_privilege_update:assignments" in err.value.problems
        await admin.execute("REVOKE UPDATE (enabled) ON arm.assignments FROM xtnl_arm_reader")

        await admin.execute("GRANT xtnl_arm_writer TO xtnl_arm_reader")
        with pytest.raises(SelfCheckError) as err:
            await _source().read()
        assert "role_member" in err.value.problems
        await admin.execute("REVOKE xtnl_arm_writer FROM xtnl_arm_reader")
        assert isinstance(await _source().read(), Document)
    finally:
        try:
            await admin.execute("RESET ROLE")
            await admin.execute("DROP SCHEMA IF EXISTS arm CASCADE")
            await _drop_test_roles(admin)
        finally:
            await admin.close()


def test_arm_schema_and_the_client_against_a_real_postgres() -> None:
    asyncio.run(_scenario())
