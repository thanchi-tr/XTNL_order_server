"""The read-only Supabase client of the ARM configuration (design §B9 Reader hardening, §B3; plan S33, R11-m).

Supabase is untrusted storage (§B9 Trust boundary): this client only reads the schema `arm` as `xtnl_arm_reader` and
turns the read into a `Document` (`src/armconfig/document.py`) or a `DocumentProblem`. Nothing here installs; the
installer (`src/armconfig/installer.py`) checks the chain, validates and installs accepted documents only.

* **Connection** (one per read, closed after it; `CONNECTION LIMIT 2` on the role): the driver's `connect` is
  injected (asyncpg's in production, `src/runtime/optional_plan.py`), with an explicit `ssl.SSLContext`:
  `PROTOCOL_TLS_CLIENT` (CERT_REQUIRED, hostname check), TLS 1.2 or newer, and only the **pinned CA**
  (`XTNL_ARM_CONFIG_CA_FILE`; no system store, never `sslmode=require`: an `sslmode` in the DSN is overridden by the
  context). Connect 5 s, statement 5 s (§B5.1).
* **Session:** one `REPEATABLE READ READ ONLY` transaction (every table from one snapshot), `search_path =
  pg_catalog` (every relation is schema-qualified, so nothing in `public` or `arm` can shadow a function), always
  rolled back.
* **Self-check on every connection** (`self_check_problems`): each of the four `arm` tables is present, readable
  with SELECT and no other privilege (`has_table_privilege`, and no INSERT / UPDATE / REFERENCES on any column,
  `has_any_column_privilege`), has RLS enabled and forced (`pg_class.relforcerowsecurity`); no other `arm` relation
  grants more than SELECT; the role has `rolbypassrls = false`, `rolinherit = false`, is not a superuser and is a
  member of no role (`pg_auth_members`: NOINHERIT does not stop `SET ROLE`, and a PostgreSQL 16+ grant `WITH
  INHERIT TRUE` overrides it). Any failure raises `SelfCheckError` with fixed codes: the
  installer keeps the accepted document and pages (fail closed).
* **Bounded read:** a client `LIMIT` of one more row than `MAX_ROWS_PER_TABLE` per table (an overflow is seen, never
  cut silently), a per-row byte cap applied by the server (a row over it is fetched without its text) and a total
  byte cap. Rows are read as `row_to_json` text and parsed with `Decimal` numbers, a nesting-depth limit and no
  NaN / Infinity, in a worker thread under a timeout (`asyncio.to_thread`), together with `parse_document`.
* **Error text** (§B9 Error text): every driver, network or TLS error is caught here and re-raised as
  `ConfigSourceError(stage, reason)`: a fixed stage code plus `safe_source_error(exc)` (the class name and, for a
  database error, its SQLSTATE), raised `from None`. Never `str(exc)`, a DSN, host, port, user or password.

`parse_source_settings(env)` reads `XTNL_ARM_CONFIG_DSN` (a secret: kept out of `repr`, never logged) and
`XTNL_ARM_CONFIG_CA_FILE`; problems name the variable only.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import ssl
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Final, Protocol

from src.armconfig.document import (
    MAX_DEPTH,
    MAX_ROW_BYTES,
    MAX_ROWS_PER_TABLE,
    SCHEMA_VERSION_KEY,
    TABLES,
    Document,
    DocumentProblem,
    parse_document,
)

log = logging.getLogger(__name__)

DSN_VAR: Final = "XTNL_ARM_CONFIG_DSN"
CA_FILE_VAR: Final = "XTNL_ARM_CONFIG_CA_FILE"
SCHEMA: Final = "arm"
SCHEMA_VERSION_TABLE: Final = "schema_version"
ARM_TABLES: Final = (SCHEMA_VERSION_TABLE, *TABLES)
"""The four tables of `deploy/supabase/arm_schema.sql` the self-check requires."""
APPLICATION_NAME: Final = "xtnl:arm-config"
CONNECT_TIMEOUT_S: Final = 5.0
STATEMENT_TIMEOUT_MS: Final = 5_000
READ_TIMEOUT_S: Final = 20.0
PARSE_TIMEOUT_S: Final = 5.0
CLOSE_TIMEOUT_S: Final = 2.0
ROW_LIMIT: Final = MAX_ROWS_PER_TABLE + 1
ROW_TEXT_CAP: Final = 2 * MAX_ROW_BYTES
"""Bytes of one row's `row_to_json` text the server returns (the canonical form is capped at `MAX_ROW_BYTES`)."""
READ_BYTE_CAP: Final = 4 * 1024 * 1024
JSON_DEPTH_CAP: Final = 2 * MAX_DEPTH
"""Nesting the decoder accepts before `parse_document` applies its own depth rule (protects `json.loads`)."""
PRIVILEGES: Final = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")
COLUMN_PRIVILEGES: Final = ("INSERT", "UPDATE", "REFERENCES")
"""Checked again per column (`has_any_column_privilege`): `has_table_privilege` misses a column-level grant such as
`GRANT UPDATE (enabled) ON arm.assignments`."""

SQL_BEGIN: Final = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
SQL_SEARCH_PATH: Final = "SET LOCAL search_path = pg_catalog"
SQL_STATEMENT_TIMEOUT: Final = f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"
SQL_ROLLBACK: Final = "ROLLBACK"
SQL_SELF_TABLES: Final = (
    "SELECT c.relname::text, c.relrowsecurity, c.relforcerowsecurity, "
    + ", ".join(f"pg_catalog.has_table_privilege(c.oid, '{p}')" for p in PRIVILEGES) + ", "
    + ", ".join(f"pg_catalog.has_any_column_privilege(c.oid, '{p}')" for p in COLUMN_PRIVILEGES)
    + " FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    f"WHERE n.nspname = '{SCHEMA}' AND c.relkind IN ('r', 'p', 'v', 'm', 'f') ORDER BY c.relname"
)
SQL_SELF_ROLE: Final = (
    "SELECT r.rolbypassrls, r.rolinherit, r.rolsuper, "
    "(SELECT pg_catalog.count(*) FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid) "
    "FROM pg_catalog.pg_roles r WHERE r.rolname = current_user"
)
"""The role flags and its role memberships: NOINHERIT does not stop `SET ROLE` to a granted role, and on PostgreSQL
16+ a grant made `WITH INHERIT TRUE` overrides it, so the reader must be a member of no role at all."""
SQL_SCHEMA_VERSION: Final = f"SELECT v.version FROM {SCHEMA}.{SCHEMA_VERSION_TABLE} v LIMIT 2"
SQL_ROWS: Final[Mapping[str, str]] = {
    t: ("SELECT s.id, pg_catalog.octet_length(s.j), CASE WHEN pg_catalog.octet_length(s.j) <= $2::int THEN s.j END "
        f"FROM (SELECT t.id, pg_catalog.row_to_json(t)::text AS j FROM {SCHEMA}.{t} t ORDER BY t.id LIMIT $1::int) s "
        "ORDER BY s.id")
    for t in TABLES
}

_DSN_RE: Final = re.compile(r"postgres(?:ql)?://\S+")
_SQLSTATE_RE: Final = re.compile(r"[0-9A-Z]{5}")
_CLASS_TEXT: Final = re.compile(r"<class '([A-Za-z_][A-Za-z0-9_.]*)'>")


# ---------------------------------------------------------------------------------------------------- settings
@dataclass(frozen=True)
class SourceSettings:
    """Where the configuration is read from. `dsn` is a secret: kept out of `repr`, never logged or paged."""

    dsn: str = field(repr=False)
    ca_file: str


def parse_source_settings(env: Mapping[str, str]) -> SourceSettings | None | list[str]:
    """`SourceSettings`; None when `XTNL_ARM_CONFIG_DSN` is unset (the ARM config is then idle); or the problems
    (variable names only, never a value). Never raises."""
    dsn = (env.get(DSN_VAR) or "").strip()
    if not dsn:
        return None
    problems: list[str] = []
    if not _DSN_RE.fullmatch(dsn):
        problems.append(f"{DSN_VAR} must be a postgresql:// connection string")
    ca = (env.get(CA_FILE_VAR) or "").strip()
    if not ca:
        problems.append(f"{CA_FILE_VAR} is required (the pinned CA of the configuration database)")
    return problems if problems else SourceSettings(dsn=dsn, ca_file=ca)


def tls_context(ca_file: str) -> ssl.SSLContext:
    """CERT_REQUIRED, hostname check, TLS 1.2 or newer, trusting only the pinned CA in `ca_file` (blocking: it reads
    the file; called in a worker thread)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    ctx.load_verify_locations(cafile=ca_file)
    return ctx


# ---------------------------------------------------------------------------------------------------- errors
def _class_name(exc: BaseException) -> str:
    try:
        text = str(type(exc))
    except Exception:   # a metaclass whose __str__ raises: this runs on error paths and must never raise
        return "Error"
    m = _CLASS_TEXT.fullmatch(text)
    return m.group(1).rpartition(".")[2] if m else "Error"


def safe_source_error(exc: BaseException) -> str:
    """`ClassName`, or `ClassName(SQLSTATE)` for a database error: never the message (it can carry the host, the
    user or a statement)."""
    name = _class_name(exc)
    try:
        state = getattr(exc, "sqlstate", None)
    except Exception:   # a property that raises
        state = None
    if isinstance(state, str) and _SQLSTATE_RE.fullmatch(state):
        return f"{name}({state})"
    return name


class ConfigSourceError(Exception):
    """A read that did not happen: `stage` (`tls_ca`, `connect`, `read`, `timeout`, `parse_timeout`) and a sanitised
    `reason`. Its text is exactly `stage: reason`."""

    def __init__(self, stage: str, reason: str = "") -> None:
        super().__init__(f"{stage}: {reason}" if reason else stage)
        self.stage = stage
        self.reason = reason


class SelfCheckError(ConfigSourceError):
    """The connection self-check failed (fail closed): `problems` are fixed codes, never catalog text."""

    def __init__(self, problems: Sequence[str]) -> None:
        super().__init__("self_check", ",".join(problems)[:500])
        self.problems = tuple(problems)


# ---------------------------------------------------------------------------------------------------- self-check
def self_check_problems(tables: Sequence[Sequence[Any]], role: Sequence[Sequence[Any]]) -> tuple[str, ...]:
    """The codes of everything the reader's catalog shows wrong (empty = pass). `tables`: rows of `SQL_SELF_TABLES`;
    `role`: rows of `SQL_SELF_ROLE`. A missing column counts as the privilege held (fail closed)."""
    problems: list[str] = []
    seen: set[str] = set()
    cols = 3 + len(PRIVILEGES)
    for row in tables:
        name = str(row[0])
        privileges = dict(zip(PRIVILEGES, (bool(v) for v in row[3:cols]), strict=False))
        columns = dict(zip(COLUMN_PRIVILEGES, (bool(v) for v in row[cols:cols + len(COLUMN_PRIVILEGES)]),
                           strict=False))
        label = name if name in ARM_TABLES else "other"
        for p in PRIVILEGES[1:]:
            if privileges.get(p, True):
                problems.append(f"privilege_{p.lower()}:{label}")
        for p in COLUMN_PRIVILEGES:
            if columns.get(p, True):
                problems.append(f"column_privilege_{p.lower()}:{label}")
        if name not in ARM_TABLES:
            continue
        seen.add(name)
        if not privileges.get("SELECT", False):
            problems.append(f"no_select:{name}")
        if not bool(row[1]):
            problems.append(f"rls_disabled:{name}")
        if not bool(row[2]):
            problems.append(f"rls_not_forced:{name}")
    problems.extend(f"missing_table:{t}" for t in ARM_TABLES if t not in seen)
    if len(role) != 1 or len(role[0]) < 4:
        problems.append("role_unknown")
    else:
        bypass, inherit, superuser = (bool(v) for v in role[0][:3])
        members = role[0][3]
        if bypass:
            problems.append("role_bypassrls")
        if inherit:
            problems.append("role_inherit")
        if superuser:
            problems.append("role_superuser")
        if not isinstance(members, int) or isinstance(members, bool) or members != 0:
            problems.append("role_member")
    return tuple(sorted(set(problems)))


# ---------------------------------------------------------------------------------------------------- decoding
def json_depth(text: str) -> int:
    """The deepest nesting of `[` / `{` in JSON text, ignoring brackets inside strings (no recursion)."""
    depth = deepest = 0
    in_string = escaped = False
    for ch in text:
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch in "[{":
            depth += 1
            deepest = max(deepest, depth)
        elif ch in "]}":
            depth -= 1
    return deepest


def _no_constant(_name: str) -> object:
    raise ValueError("NaN and Infinity are not JSON values")


def _decode_row(table: str, row_id: int | None, text: str) -> object | DocumentProblem:
    if json_depth(text) > JSON_DEPTH_CAP:
        return DocumentProblem("value_form", table, row_id, detail="nesting too deep")
    try:
        value: object = json.loads(text, parse_float=Decimal, parse_int=int, parse_constant=_no_constant)
        return value
    except (ValueError, RecursionError):
        return DocumentProblem("value_form", table, row_id, detail="not JSON")


def _row_id(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def build_document(versions: Sequence[Sequence[Any]],
                   tables: Mapping[str, Sequence[Sequence[Any]]]) -> Document | DocumentProblem:
    """The read as a `Document`: `versions` = rows of `SQL_SCHEMA_VERSION`, `tables` = rows of `SQL_ROWS` (`id`, byte
    length, text or NULL when over `ROW_TEXT_CAP`). Bounds first, then the JSON, then `parse_document`. Pure and
    blocking (run in a worker thread); never raises."""
    if len(versions) != 1:
        return DocumentProblem("schema_version_form", detail="arm.schema_version must hold exactly one row")
    raw: dict[str, object] = {SCHEMA_VERSION_KEY: versions[0][0]}
    total = 0
    for table in TABLES:
        rows = tables.get(table, ())
        if len(rows) > MAX_ROWS_PER_TABLE:
            return DocumentProblem("too_many_rows", table, detail=f"at most {MAX_ROWS_PER_TABLE}")
        decoded: list[object] = []
        for row in rows:
            row_id = _row_id(row[0])
            size = row[1] if isinstance(row[1], int) else 0
            total += max(size, 0)
            if total > READ_BYTE_CAP:
                return DocumentProblem("read_too_large", detail=f"at most {READ_BYTE_CAP} bytes")
            text = row[2]
            if not isinstance(text, str) or size > ROW_TEXT_CAP:
                return DocumentProblem("row_too_large", table, row_id, detail=f"at most {ROW_TEXT_CAP} bytes")
            value = _decode_row(table, row_id, text)
            if isinstance(value, DocumentProblem):
                return value
            decoded.append(value)
        raw[table] = decoded
    return parse_document(raw)


# ---------------------------------------------------------------------------------------------------- the source
class RawConnection(Protocol):
    """The part of a driver connection (asyncpg's) the client uses."""

    async def execute(self, query: str, *args: Any) -> Any: ...

    async def fetch(self, query: str, *args: Any) -> Sequence[Sequence[Any]]: ...

    async def close(self) -> None: ...

    def terminate(self) -> None: ...


ConnectFn = Callable[..., Awaitable[RawConnection]]
"""`connect(dsn, *, ssl, timeout, command_timeout, statement_cache_size, server_settings)` (asyncpg's signature)."""


class ConfigSource(Protocol):
    async def read(self) -> Document | DocumentProblem:
        """One read of the whole configuration; `ConfigSourceError` (sanitised) when it could not be read."""
        ...


class SupabaseConfigSource:
    """`ConfigSource` over Supabase (module docstring). One connection per `read()`."""

    def __init__(self, settings: SourceSettings, connect: ConnectFn, *,
                 tls: Callable[[str], ssl.SSLContext] = tls_context) -> None:
        self._settings = settings
        self._connect = connect
        self._tls_factory = tls
        self._tls: ssl.SSLContext | None = None
        self.reads = 0

    def __repr__(self) -> str:
        return "SupabaseConfigSource()"

    async def _context(self) -> ssl.SSLContext:
        if self._tls is None:
            try:
                self._tls = await asyncio.wait_for(asyncio.to_thread(self._tls_factory, self._settings.ca_file),
                                                   CONNECT_TIMEOUT_S)
            except Exception as exc:
                raise ConfigSourceError("tls_ca", safe_source_error(exc)) from None
        return self._tls

    async def read(self) -> Document | DocumentProblem:
        ctx = await self._context()
        try:
            conn = await asyncio.wait_for(self._connect(
                self._settings.dsn, ssl=ctx, timeout=CONNECT_TIMEOUT_S, command_timeout=STATEMENT_TIMEOUT_MS / 1000,
                statement_cache_size=0, server_settings={"application_name": APPLICATION_NAME}),
                CONNECT_TIMEOUT_S + 1.0)
        except Exception as exc:
            raise ConfigSourceError("connect", safe_source_error(exc)) from None
        self.reads += 1
        try:
            versions, tables = await asyncio.wait_for(self._read(conn), READ_TIMEOUT_S)
        except ConfigSourceError:
            raise
        except TimeoutError:
            raise ConfigSourceError("timeout", "read") from None
        except Exception as exc:
            raise ConfigSourceError("read", safe_source_error(exc)) from None
        finally:
            await self._close(conn)
        try:
            return await asyncio.wait_for(asyncio.to_thread(build_document, versions, tables), PARSE_TIMEOUT_S)
        except TimeoutError:
            raise ConfigSourceError("parse_timeout") from None

    @staticmethod
    async def _read(conn: RawConnection) -> tuple[Sequence[Sequence[Any]], dict[str, Sequence[Sequence[Any]]]]:
        await conn.execute(SQL_BEGIN)
        try:
            await conn.execute(SQL_SEARCH_PATH)
            await conn.execute(SQL_STATEMENT_TIMEOUT)
            problems = self_check_problems(await conn.fetch(SQL_SELF_TABLES), await conn.fetch(SQL_SELF_ROLE))
            if problems:
                raise SelfCheckError(problems)
            versions = await conn.fetch(SQL_SCHEMA_VERSION)
            tables = {t: await conn.fetch(SQL_ROWS[t], ROW_LIMIT, ROW_TEXT_CAP) for t in TABLES}
        finally:
            try:
                await conn.execute(SQL_ROLLBACK)
            except Exception as exc:   # the connection is closed (or terminated) next anyway
                log.debug("arm-config: rollback failed (%s)", safe_source_error(exc))
        return versions, tables

    @staticmethod
    async def _close(conn: RawConnection) -> None:
        try:
            await asyncio.wait_for(conn.close(), CLOSE_TIMEOUT_S)
        except Exception:
            try:
                conn.terminate()
            except Exception as exc:
                log.info("arm-config: connection not terminated (%s)", safe_source_error(exc))


__all__ = [
    "APPLICATION_NAME", "ARM_TABLES", "CA_FILE_VAR", "COLUMN_PRIVILEGES", "ConfigSource", "ConfigSourceError", "ConnectFn", "DSN_VAR",
    "READ_BYTE_CAP", "ROW_LIMIT", "ROW_TEXT_CAP", "RawConnection", "SQL_ROWS", "SQL_SCHEMA_VERSION",
    "SQL_SELF_ROLE", "SQL_SELF_TABLES", "SelfCheckError", "SourceSettings", "SupabaseConfigSource",
    "build_document", "json_depth", "parse_source_settings", "safe_source_error", "self_check_problems",
    "tls_context",
]
