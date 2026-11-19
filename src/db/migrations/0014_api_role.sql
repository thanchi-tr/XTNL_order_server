-- 0014_api_role.sql : the read-only `xtnl_api` role for the order-server API (python -m src.api, the one
-- network-facing listener in single-PC mode; docs/runbooks/order-server-api.md).
--
-- * Role `xtnl_api`: NOLOGIN as created here. No credential is ever written into a migration: the operator
--   enables login once with a random password (deploy/local/new-api-credentials.ps1 -DbRole does it on the local
--   stack and stores the DSN as XTNL_API_DSN in the Windows User environment only):
--       ALTER ROLE xtnl_api LOGIN PASSWORD '...';
-- * SELECT only, and only on the tables the Pine Script compile reads (src/reports/lifecycle.py SQL_LOG_VERSIONS,
--   SQL_CURRENT_ORDERS, SQL_CURRENT_POSITIONS; a test keeps the two lists equal). No INSERT / UPDATE / DELETE /
--   TRUNCATE anywhere and no default privileges: a compromise of the API process cannot write the engine's state
--   (halt tables included), and src/api/pinescript.py refuses to compile over any role that could.
-- * Session defaults: default_transaction_read_only on, statement_timeout 60 s, at most 3 connections.
-- Idempotent (the role is created only when missing; GRANT / ALTER ROLE ... SET repeat safely). Creating a role
-- needs CREATEROLE: run the migration with the schema-owner DSN (start-xtnl.ps1 step 6 does on the local stack).

SET client_min_messages = warning;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_api') THEN
        CREATE ROLE xtnl_api NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION
            NOBYPASSRLS CONNECTION LIMIT 3;
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO xtnl_api', current_database());
END
$$;

ALTER ROLE xtnl_api SET default_transaction_read_only = on;
ALTER ROLE xtnl_api SET statement_timeout = '60s';
ALTER ROLE xtnl_api SET idle_in_transaction_session_timeout = '10s';

GRANT USAGE ON SCHEMA public TO xtnl_api;

GRANT SELECT ON TABLE
    event_log,
    orders_state,
    positions_state
TO xtnl_api;
