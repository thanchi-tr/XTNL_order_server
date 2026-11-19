"""`deploy/supabase/arm_schema.sql` (plan S33, design §B9): the provisioning file matches the engine's raw row
contract, the reader's self-check and the hardening list. Static (the file is applied by hand in Supabase)."""
from __future__ import annotations

import re
from pathlib import Path

from src.armconfig.client import ARM_TABLES, SCHEMA
from src.armconfig.document import SUPPORTED_SCHEMA_VERSION
from src.armconfig.validate import COLUMNS

SQL = (Path(__file__).resolve().parents[2] / "deploy" / "supabase" / "arm_schema.sql").read_text(encoding="utf-8")
CODE = re.sub(r"--[^\n]*", "", SQL)


def table_columns(table: str) -> set[str]:
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {SCHEMA}\.{table} \((.*?)\n\);", CODE, re.S)
    assert m, table
    cols: set[str] = set()
    for line in m.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith(("UNIQUE", "FOREIGN", "PRIMARY", "CHECK", "AND")):
            continue
        cols.add(line.split()[0].strip('"'))
    return cols


def test_tables_hold_exactly_the_engine_row_contract() -> None:
    for table, cols in COLUMNS.items():
        assert table_columns(table) == set(cols), table
    assert table_columns("schema_version") == {"singleton", "version"}
    assert set(ARM_TABLES) == {"schema_version", *COLUMNS}


def test_reserved_column_names_are_quoted_and_set_by_the_guard() -> None:
    assert re.search(r'NEW\."session_user" := session_user;', CODE)
    assert re.search(r'NEW\."current_user" := current_user;', CODE)
    assert "NEW.set_at_ms := now_ms;" in CODE
    assert "BEFORE UPDATE OR DELETE" in CODE and "BEFORE TRUNCATE" in CODE


def test_schema_version_is_the_supported_one() -> None:
    m = re.search(r"INSERT INTO arm\.schema_version \(singleton, version\) VALUES \(true, (\d+)\)", CODE)
    assert m and int(m.group(1)) == SUPPORTED_SCHEMA_VERSION


def test_rls_is_forced_and_the_reader_reads_only() -> None:
    for table in ARM_TABLES:
        assert f"ALTER TABLE arm.{table} ENABLE ROW LEVEL SECURITY;" in CODE
        assert f"ALTER TABLE arm.{table} FORCE ROW LEVEL SECURITY;" in CODE
    grants = re.findall(r"GRANT ([A-Z, ]+) ON arm\.[^;]* TO ([a-z_, ]+);", CODE)
    reader = [privs for privs, roles in grants if "xtnl_arm_reader" in roles]
    assert reader == ["SELECT"]
    assert "FOR INSERT TO xtnl_arm_writer" in CODE and "FOR INSERT TO xtnl_arm_reader" not in CODE


def test_reader_hardening_and_no_api_exposure() -> None:
    create = re.search(r"CREATE ROLE xtnl_arm_reader ([^;]+);", CODE)
    assert create
    for word in ("NOINHERIT", "NOBYPASSRLS", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "CONNECTION LIMIT 2"):
        assert word in create.group(1)
    assert "ALTER ROLE xtnl_arm_reader SET default_transaction_read_only = on;" in CODE
    assert "ARRAY['anon', 'authenticated', 'service_role']" in CODE
    assert "REVOKE ALL ON SCHEMA arm FROM PUBLIC;" in CODE
    assert "edge_code          text    CHECK (edge_code IS NULL)" in CODE


def test_no_secret_in_the_file() -> None:
    assert not re.search(r"PASSWORD\s+'[^.']", CODE)
    assert "postgresql://" not in SQL


def test_the_api_roles_are_revoked_after_every_object_exists() -> None:
    # a schema-scoped default-privilege revoke cannot cancel global default grants: the created objects themselves
    # must be revoked, so the API roles' loop runs after every CREATE TABLE / CREATE FUNCTION / trigger
    loop = CODE.rfind("ARRAY['anon', 'authenticated', 'service_role']")
    assert loop > 0 and CODE.count("ARRAY['anon', 'authenticated', 'service_role']") == 1
    for create in ("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE FUNCTION", "CREATE TRIGGER"):
        assert CODE.rfind(create) < loop, create
    tail = CODE[loop:]
    for kind in ("TABLES", "SEQUENCES", "FUNCTIONS"):
        assert f"REVOKE ALL ON ALL {kind} IN SCHEMA arm FROM %I" in tail


def test_the_guard_draws_every_id_from_the_identity_sequence() -> None:
    # OVERRIDING SYSTEM VALUE cannot choose an id: the guard always takes the next sequence value
    assert ("NEW.id := nextval(pg_get_serial_sequence(format('arm.%I', TG_TABLE_NAME), 'id')::regclass);"
            in CODE)
    assert "GRANT USAGE ON ALL SEQUENCES IN SCHEMA arm TO xtnl_arm_writer;" in CODE
    assert not re.search(r"GRANT [A-Z, ]*(UPDATE|ALL)[A-Z, ]* ON (ALL )?SEQUENCES?[^;]*TO", CODE)   # no setval


def test_a_rerun_upserts_the_schema_version_without_bypassrls() -> None:
    # on Supabase `postgres` is not a superuser: the owner lifts FORCE for the upsert and forces it again after
    no_force = CODE.index("ALTER TABLE arm.schema_version NO FORCE ROW LEVEL SECURITY;")
    upsert = CODE.index("INSERT INTO arm.schema_version")
    force = CODE.index("ALTER TABLE arm.schema_version FORCE ROW LEVEL SECURITY;")
    assert no_force < upsert < force
    assert CODE.index("BEGIN;") < no_force and force < CODE.rindex("COMMIT;")


def test_a_rerun_never_names_superuser_attributes_unconditionally() -> None:
    # PostgreSQL refuses a non-superuser (Supabase's `postgres`) any ALTER ROLE naming SUPERUSER / REPLICATION /
    # BYPASSRLS, even to switch them off: the re-run changes only an attribute that is wrong
    assert not re.search(r"^ALTER ROLE xtnl_arm_\w+ [^;]*NO(SUPERUSER|BYPASSRLS|REPLICATION)", CODE, re.M)
    for attr in ("rolsuper", "rolbypassrls", "rolreplication", "rolinherit"):
        assert f"IF r.{attr} THEN EXECUTE format('ALTER ROLE %I NO" in CODE
