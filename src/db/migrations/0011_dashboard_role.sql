-- 0011_dashboard_role.sql : the read-only `xtnl_dashboard` role for the Melbourne dashboard (operations.md s9
-- Dashboard; IMPLEMENTATION_PLAN 6.4).
--
-- * Role `xtnl_dashboard`: NOLOGIN as created here. No credential is ever written into a migration: the operator
--   enables login once with a password from Secrets Manager (docs/runbooks/install-node.md, "Dashboard role"):
--       ALTER ROLE xtnl_dashboard LOGIN;   \password xtnl_dashboard
-- * SELECT only, and only on the tables the dashboard reads (src/dashboard/views.py ALL_SQL; a test keeps the
--   two lists equal). No INSERT / UPDATE / DELETE / TRUNCATE anywhere, so src/dashboard/server.py
--   verify_read_only_connection accepts it, and no default privileges: a new table stays invisible until a
--   later migration grants it.
-- * Session defaults: default_transaction_read_only on, statement_timeout 5 s, at most 5 connections.
-- Idempotent (the role is created only when missing; GRANT / ALTER ROLE ... SET repeat safely). Creating a role
-- needs CREATEROLE: run the migration job with the schema-owner DSN (/etc/xtnl/migrate.env). Applied by
-- src/db/migrate.py with lock_timeout 2 s and retries.

SET client_min_messages = warning;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_dashboard') THEN
        CREATE ROLE xtnl_dashboard NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION
            NOBYPASSRLS CONNECTION LIMIT 5;
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO xtnl_dashboard', current_database());
END
$$;

ALTER ROLE xtnl_dashboard SET default_transaction_read_only = on;
ALTER ROLE xtnl_dashboard SET statement_timeout = '5s';
ALTER ROLE xtnl_dashboard SET idle_in_transaction_session_timeout = '10s';

GRANT USAGE ON SCHEMA public TO xtnl_dashboard;

GRANT SELECT ON TABLE
    orders_state,
    positions_state,
    account_state,
    halt_events,
    halt_breaches,
    events_dlq,
    stream_health,
    engine_epoch,
    leader_terms,
    cluster_state
TO xtnl_dashboard;
