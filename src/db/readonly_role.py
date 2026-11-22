"""Shared read-only role checks for the read-only PostgreSQL logins (dashboard, API, live view).

`SQL_ROLE_CHECK` returns the session's role, whether it is a superuser, its `default_transaction_read_only`
setting, and how many relations (tables, partitioned tables, views and materialized views) it could write
(INSERT / UPDATE / DELETE / TRUNCATE, inherited grants and ownership included). A DSN pointed at a writable role
(the engine's, an operator's, a superuser) is refused, so a read-only session never rests on the startup parameter
alone. Messages name the caller's variable and role, never a DSN or a value.

`verify_view_only_role` is the live view's stricter second check (docs/design/live-view.md s4.2): the session is
`xtnl_liveview`, can read every view it needs, can read no table at all, and is not a member of the predefined
read-all / write-all roles.
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Final

SQL_ROLE_CHECK: Final = (
    "SELECT current_user::text, r.rolsuper, current_setting('default_transaction_read_only'), "
    "(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r','p','v','m') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%' "
    "AND (has_table_privilege(c.oid, 'INSERT') OR has_table_privilege(c.oid, 'UPDATE') "
    "OR has_table_privilege(c.oid, 'DELETE') OR has_table_privilege(c.oid, 'TRUNCATE'))) "
    "FROM pg_roles r WHERE r.rolname = current_user"
)

#: Tables (not views) outside the system schemas that the session can read at all.
SQL_READABLE_TABLES: Final = (
    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r','p') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%' "
    "AND (has_table_privilege(c.oid, 'SELECT') OR has_any_column_privilege(c.oid, 'SELECT'))"
)
SQL_PREDEFINED_MEMBERSHIP: Final = (
    "SELECT current_user::text, pg_has_role('pg_read_all_data', 'MEMBER'), pg_has_role('pg_write_all_data', 'MEMBER')"
)
SQL_VIEW_SELECT: Final = (
    "SELECT CASE WHEN to_regclass($1::text) IS NULL THEN false "
    "ELSE has_table_privilege(to_regclass($1::text), 'SELECT') END"
)


class ReadOnlyRoleError(RuntimeError):
    """The session's role is not a read-only role (the message names the variable and role, never a value)."""


def read_only_problems(row: Sequence[object] | None) -> list[str]:
    """Why a SQL_ROLE_CHECK row may not serve a read-only reader (empty = acceptable)."""
    if row is None:
        return ["role not found"]
    problems: list[str] = []
    if bool(row[1]):
        problems.append("is a superuser")
    writable = row[3]
    count = int(writable) if isinstance(writable, (int, str)) else 1
    if count:
        problems.append(f"can write {count} table(s)")
    if str(row[2]).lower() != "on":
        problems.append("session is not default_transaction_read_only")
    return problems


async def verify_read_only_connection(conn: Any, *, dsn_var: str, role_hint: str) -> None:
    """Refuse a connection whose role is a superuser, can write any relation, or is not read-only by default."""
    row = await conn.fetchrow(SQL_ROLE_CHECK)
    problems = read_only_problems(row)
    if problems:
        role = str(row[0])[:64] if row is not None else "?"
        raise ReadOnlyRoleError(f"refusing PostgreSQL role {role!r}: {', '.join(problems)} "
                                f"(set {dsn_var} to role {role_hint}, SELECT grants only)")


async def verify_view_only_role(conn: Any, *, views: Iterable[str], role_name: str, dsn_var: str) -> None:
    """The live view's check: the role is `role_name`, reads every view in `views`, reads no table, and is not a
    member of pg_read_all_data / pg_write_all_data."""
    row = await conn.fetchrow(SQL_PREDEFINED_MEMBERSHIP)
    if row is None:
        raise ReadOnlyRoleError(f"role check returned nothing (check {dsn_var})")
    role = str(row[0])[:64]
    problems: list[str] = []
    if role != role_name:
        problems.append(f"is not {role_name}")
    if bool(row[1]) or bool(row[2]):
        problems.append("is a member of pg_read_all_data or pg_write_all_data")
    tables = int(await conn.fetchval(SQL_READABLE_TABLES) or 0)
    if tables:
        problems.append(f"can read {tables} table(s)")
    missing = [v for v in views if not await conn.fetchval(SQL_VIEW_SELECT, v)]
    if missing:
        problems.append(f"cannot read {len(missing)} view(s)")
    if problems:
        raise ReadOnlyRoleError(f"refusing PostgreSQL role {role!r}: {', '.join(problems)} "
                                f"(set {dsn_var} to role {role_name}; apply migration 0021)")


__all__ = ["ReadOnlyRoleError", "SQL_PREDEFINED_MEMBERSHIP", "SQL_READABLE_TABLES", "SQL_ROLE_CHECK",
           "SQL_VIEW_SELECT", "read_only_problems", "verify_read_only_connection", "verify_view_only_role"]
