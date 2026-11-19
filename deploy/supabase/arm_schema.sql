-- ARM configuration schema for Supabase (design docs/design/automated-risk-manager.md §B9; plan S33).
--
-- Applied BY HAND by the project owner in the Supabase SQL editor (or psql as the project owner), never by the
-- engine. Versioned by arm.schema_version: the engine reads schema_version 1 (src/armconfig/document.py
-- SUPPORTED_SCHEMA_VERSION) and treats a newer one as "schema ahead" (nothing installs). Idempotent: safe to re-run.
--
-- Supabase is untrusted storage: the only trust boundary is the local `arm config accept <hash>` on the engine
-- host (arm_accepted in the engine's PostgreSQL). RLS and the append-only guards bind xtnl_arm_writer, not the
-- project owner, so the Supabase account needs MFA, and the project's network restrictions should allow only the
-- engine's egress addresses where available.
--
-- After applying:
--   * set a password for each role (ALTER ROLE xtnl_arm_reader PASSWORD '...'; never in this file or the repo),
--   * build XTNL_ARM_CONFIG_DSN for xtnl_arm_reader (direct host or session pooler) and XTNL_ARM_CONFIG_CA_FILE
--     (the pinned CA of the project's certificate) in the engine host's secrets source,
--   * edit rows only as xtnl_arm_writer: in the SQL editor run `SET ROLE xtnl_arm_writer;` first.
--
-- Rows are never updated or deleted: a change is a new row (a strategies change is a new `version`; an assignment
-- with enabled = false is a tombstone). The guards set id, set_at_ms, session_user and current_user and bound
-- effective_from_ms; the engine detects any change to a seen row as tampering (src/armconfig/chain.py).
-- Column names are the engine's raw row contract (src/armconfig/validate.py COLUMNS): times are integer ms;
-- "session_user" and "current_user" are quoted (reserved words) and set by the guard, never by the writer.
-- Configuration only: strategy codes, parameters and routes; never market data, account values or credentials.

BEGIN;

CREATE SCHEMA IF NOT EXISTS arm;

-- ---------------------------------------------------------------------------------------------
-- Roles (hardening of §B9: NOINHERIT, NOBYPASSRLS, CONNECTION LIMIT 2, read-only, no CREATE, owns nothing)
-- ---------------------------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_arm_reader') THEN
        CREATE ROLE xtnl_arm_reader LOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
            CONNECTION LIMIT 2;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_arm_writer') THEN
        CREATE ROLE xtnl_arm_writer NOLOGIN NOINHERIT NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END
$$;
-- Re-assert the hardening on a re-run, changing only an attribute that is wrong: on Supabase `postgres` is not a
-- superuser, and PostgreSQL refuses a non-superuser any ALTER ROLE that names SUPERUSER / REPLICATION / BYPASSRLS,
-- even to switch them off, so an unconditional ALTER would fail the whole file (an attribute it cannot fix still
-- fails here, loudly, and the engine's self-check refuses such a reader anyway).
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT * FROM pg_roles WHERE rolname IN ('xtnl_arm_reader', 'xtnl_arm_writer') LOOP
        IF r.rolsuper THEN EXECUTE format('ALTER ROLE %I NOSUPERUSER', r.rolname); END IF;
        IF r.rolbypassrls THEN EXECUTE format('ALTER ROLE %I NOBYPASSRLS', r.rolname); END IF;
        IF r.rolreplication THEN EXECUTE format('ALTER ROLE %I NOREPLICATION', r.rolname); END IF;
        IF r.rolinherit THEN EXECUTE format('ALTER ROLE %I NOINHERIT', r.rolname); END IF;
        IF r.rolcreatedb THEN EXECUTE format('ALTER ROLE %I NOCREATEDB', r.rolname); END IF;
        IF r.rolcreaterole THEN EXECUTE format('ALTER ROLE %I NOCREATEROLE', r.rolname); END IF;
        IF r.rolname = 'xtnl_arm_reader' AND r.rolconnlimit <> 2 THEN
            EXECUTE format('ALTER ROLE %I CONNECTION LIMIT 2', r.rolname);
        END IF;
    END LOOP;
END
$$;
ALTER ROLE xtnl_arm_reader SET default_transaction_read_only = on;
ALTER ROLE xtnl_arm_reader SET statement_timeout = '5s';
ALTER ROLE xtnl_arm_reader SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE xtnl_arm_reader SET search_path = pg_catalog;
-- the project owner may SET ROLE xtnl_arm_writer in the SQL editor
GRANT xtnl_arm_writer TO CURRENT_USER;

-- Not exposed through PostgREST: nothing for PUBLIC or the Supabase API roles (anon, authenticated,
-- service_role), now or by default. The API roles' revoke runs at the end, AFTER the tables, identity sequences and
-- guard functions exist (a schema-scoped ALTER DEFAULT PRIVILEGES cannot cancel the project's global default
-- grants, so only a revoke of the created objects removes what those defaults granted on the first apply).
REVOKE ALL ON SCHEMA arm FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA arm REVOKE ALL ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA arm REVOKE ALL ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA arm REVOKE ALL ON FUNCTIONS FROM PUBLIC;
GRANT USAGE ON SCHEMA arm TO xtnl_arm_reader, xtnl_arm_writer;

-- ---------------------------------------------------------------------------------------------
-- Tables
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm.schema_version (
    singleton      boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version        int     NOT NULL CHECK (version >= 1)
);
-- On a re-run RLS is already forced on this table and there is no policy for the owner. On Supabase `postgres` is
-- not a superuser, so the upsert must not depend on it having BYPASSRLS: the owner lifts FORCE for the upsert, and
-- the privileges section below forces it again, all inside this one transaction.
ALTER TABLE arm.schema_version NO FORCE ROW LEVEL SECURITY;
INSERT INTO arm.schema_version (singleton, version) VALUES (true, 1)
    ON CONFLICT (singleton) DO UPDATE SET version = GREATEST(arm.schema_version.version, EXCLUDED.version);

CREATE TABLE IF NOT EXISTS arm.strategies (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code           text   NOT NULL CHECK (code ~ '^[a-z0-9_]{1,32}$'),
    version        int    NOT NULL CHECK (version >= 1),
    kind           text   NOT NULL CHECK (kind ~ '^[a-z][a-z0-9_]{0,31}$'),
    impl_version   int    NOT NULL CHECK (impl_version >= 1),
    params         jsonb  NOT NULL CHECK (jsonb_typeof(params) = 'object' AND octet_length(params::text) <= 4096),
    set_at_ms      bigint NOT NULL,
    "session_user" text   NOT NULL,
    "current_user" text   NOT NULL,
    UNIQUE (code, version)
);

CREATE TABLE IF NOT EXISTS arm.assignments (
    id                 bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    edge_code          text    CHECK (edge_code IS NULL),          -- Phase 1: single edge (§B11)
    symbol             text    CHECK (symbol IS NULL OR symbol ~ '^[A-Za-z0-9._/#-]{1,32}$'),
    account            text    CHECK (account IS NULL OR account ~ '^[A-Za-z0-9._:-]{1,64}$'),
    adapter            text    CHECK (adapter IS NULL OR adapter ~ '^[a-z0-9_]{1,32}$'),
    strategy_code      text    NOT NULL,
    strategy_version   int     NOT NULL,
    enabled            boolean NOT NULL,
    effective_from_ms  bigint  NOT NULL,
    set_at_ms          bigint  NOT NULL,
    "session_user"     text    NOT NULL,
    "current_user"     text    NOT NULL,
    FOREIGN KEY (strategy_code, strategy_version) REFERENCES arm.strategies (code, version)
);

CREATE TABLE IF NOT EXISTS arm.route_policy (
    id                 bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    route              text    NOT NULL CHECK (route ~ '^[A-Za-z0-9._:*-]{1,100}$'),
    mode               text    NOT NULL CHECK (mode IN ('every_bar', 'actionable')),
    pre_channels       text[]  NOT NULL CHECK (cardinality(pre_channels) BETWEEN 1 AND 2
                                               AND pre_channels <@ ARRAY['arm_email', 'arm_push']::text[]),
    post_channels      text[]  NOT NULL CHECK (cardinality(post_channels) BETWEEN 1 AND 2
                                               AND post_channels <@ ARRAY['arm_email', 'arm_push']::text[]
                                               AND 'arm_email' = ANY (post_channels)),
    effective_from_ms  bigint  NOT NULL,
    set_at_ms          bigint  NOT NULL,
    "session_user"     text    NOT NULL,
    "current_user"     text    NOT NULL
);

-- ---------------------------------------------------------------------------------------------
-- Append-only guards: the server sets id / set_at_ms / session_user / current_user, bounds effective_from_ms,
-- caps the row count, and refuses UPDATE, DELETE and TRUNCATE. `id` is always taken from the column's identity
-- sequence, so `INSERT ... OVERRIDING SYSTEM VALUE` cannot choose one (a zero, negative or far-ahead id would break
-- the engine's append-only id order and its row id form).
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION arm.guard_insert() RETURNS trigger
    LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE
    now_ms bigint := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
    n bigint;
BEGIN
    NEW.id := nextval(pg_get_serial_sequence(format('arm.%I', TG_TABLE_NAME), 'id')::regclass);
    NEW.set_at_ms := now_ms;
    NEW."session_user" := session_user;
    NEW."current_user" := current_user;
    IF TG_TABLE_NAME IN ('assignments', 'route_policy') THEN
        IF NEW.effective_from_ms < now_ms - 60000 OR NEW.effective_from_ms > now_ms + 366::bigint * 86400000 THEN
            RAISE EXCEPTION 'effective_from_ms must lie between now - 1 min and now + 366 days';
        END IF;
    END IF;
    EXECUTE format('SELECT count(*) FROM arm.%I', TG_TABLE_NAME) INTO n;
    IF n >= 1000 THEN
        RAISE EXCEPTION 'arm.% holds at most 1000 rows', TG_TABLE_NAME;
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION arm.guard_append_only() RETURNS trigger
    LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'arm.% is append-only: add a new row instead', TG_TABLE_NAME;
END
$$;

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['strategies', 'assignments', 'route_policy'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS guard_insert ON arm.%I', t);
        EXECUTE format('CREATE TRIGGER guard_insert BEFORE INSERT ON arm.%I '
                       'FOR EACH ROW EXECUTE FUNCTION arm.guard_insert()', t);
        EXECUTE format('DROP TRIGGER IF EXISTS guard_append_only ON arm.%I', t);
        EXECUTE format('CREATE TRIGGER guard_append_only BEFORE UPDATE OR DELETE ON arm.%I '
                       'FOR EACH ROW EXECUTE FUNCTION arm.guard_append_only()', t);
        EXECUTE format('DROP TRIGGER IF EXISTS guard_truncate ON arm.%I', t);
        EXECUTE format('CREATE TRIGGER guard_truncate BEFORE TRUNCATE ON arm.%I '
                       'FOR EACH STATEMENT EXECUTE FUNCTION arm.guard_append_only()', t);
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION arm.guard_insert() FROM PUBLIC;
REVOKE ALL ON FUNCTION arm.guard_append_only() FROM PUBLIC;

-- ---------------------------------------------------------------------------------------------
-- Privileges and RLS (forced): SELECT for xtnl_arm_reader, INSERT for xtnl_arm_writer, nothing else.
-- ---------------------------------------------------------------------------------------------
REVOKE ALL ON ALL TABLES IN SCHEMA arm FROM PUBLIC, xtnl_arm_reader, xtnl_arm_writer;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA arm FROM PUBLIC, xtnl_arm_reader, xtnl_arm_writer;
GRANT SELECT ON arm.schema_version, arm.strategies, arm.assignments, arm.route_policy TO xtnl_arm_reader;
GRANT SELECT, INSERT ON arm.strategies, arm.assignments, arm.route_policy TO xtnl_arm_writer;
GRANT SELECT ON arm.schema_version TO xtnl_arm_writer;
-- guard_insert runs as the writer (SECURITY INVOKER) and draws the id itself: nextval only (no setval)
GRANT USAGE ON ALL SEQUENCES IN SCHEMA arm TO xtnl_arm_writer;

-- The Supabase API roles: revoked now that every table, sequence and function exists (see the top of the file).
DO $$
DECLARE r text;
BEGIN
    FOREACH r IN ARRAY ARRAY['anon', 'authenticated', 'service_role'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('REVOKE ALL ON SCHEMA arm FROM %I', r);
            EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA arm FROM %I', r);
            EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA arm FROM %I', r);
            EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA arm FROM %I', r);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA arm REVOKE ALL ON TABLES FROM %I', r);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA arm REVOKE ALL ON SEQUENCES FROM %I', r);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA arm REVOKE ALL ON FUNCTIONS FROM %I', r);
        END IF;
    END LOOP;
END
$$;

ALTER TABLE arm.schema_version ENABLE ROW LEVEL SECURITY;
ALTER TABLE arm.schema_version FORCE ROW LEVEL SECURITY;
ALTER TABLE arm.strategies ENABLE ROW LEVEL SECURITY;
ALTER TABLE arm.strategies FORCE ROW LEVEL SECURITY;
ALTER TABLE arm.assignments ENABLE ROW LEVEL SECURITY;
ALTER TABLE arm.assignments FORCE ROW LEVEL SECURITY;
ALTER TABLE arm.route_policy ENABLE ROW LEVEL SECURITY;
ALTER TABLE arm.route_policy FORCE ROW LEVEL SECURITY;

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['schema_version', 'strategies', 'assignments', 'route_policy'] LOOP
        EXECUTE format('DROP POLICY IF EXISTS arm_read ON arm.%I', t);
        EXECUTE format('CREATE POLICY arm_read ON arm.%I FOR SELECT TO xtnl_arm_reader, xtnl_arm_writer '
                       'USING (true)', t);
    END LOOP;
    FOREACH t IN ARRAY ARRAY['strategies', 'assignments', 'route_policy'] LOOP
        EXECUTE format('DROP POLICY IF EXISTS arm_append ON arm.%I', t);
        EXECUTE format('CREATE POLICY arm_append ON arm.%I FOR INSERT TO xtnl_arm_writer WITH CHECK (true)', t);
    END LOOP;
END
$$;

COMMIT;
