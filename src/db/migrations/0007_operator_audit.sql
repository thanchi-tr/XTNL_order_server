-- 0007_operator_audit.sql : operator CLI audit trail and engine requests (operations.md s16, backlog 4.6).
--
-- `python -m src.ops` (src/ops/) writes one operator_audit row in the SAME transaction as every change it
-- makes (SAFE_HOLD resume, adapter re-arm, reset_task request, DLQ requeue / quarantine release, epoch
-- adoption, operator halt clear); a refused or no-op command gets its own row. The table is append-only:
-- a trigger rejects UPDATE / DELETE / TRUNCATE for every role, and the grant section below removes those
-- privileges from the engine role. operator_requests carries requests the running leader must act on
-- in-process (reset_task closes a task circuit inside the engine), scoped to the epoch that was current.
-- Numbered 0007 because 0006 is taken (0006_dlq_lineage_adapter.sql); the runner refuses duplicates.
-- Idempotent (IF NOT EXISTS / CREATE OR REPLACE / guarded DO blocks), applied in order by src/db/migrate.py.

SET client_min_messages = warning;

-- ---------------------------------------------------------------------------------------------
-- operator_audit: who did what, why, under which epoch, with which outcome.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operator_audit (
    audit_id     bigserial PRIMARY KEY,
    at_ms        bigint  NOT NULL DEFAULT now_ms(),
    command      text    NOT NULL,                  -- e.g. 'safe_hold.resume', 'halt.clear'
    target       text,                              -- adapter / task / dlq id / account / cluster id
    operator     text    NOT NULL CHECK (operator ~ '^[A-Za-z0-9._@-]{2,64}$' AND lower(operator) <> 'system'),
    approver     text    CHECK (approver IS NULL OR (approver ~ '^[A-Za-z0-9._@-]{2,64}$'
                                                     AND lower(approver) <> lower(operator)
                                                     AND lower(approver) <> 'system')),
    reason       text    NOT NULL CHECK (length(btrim(reason)) >= 8),
    outcome      text    NOT NULL CHECK (outcome IN ('applied', 'refused', 'noop')),
    detail       jsonb   NOT NULL DEFAULT '{}'::jsonb,
    epoch        bigint,                            -- engine_epoch.epoch the change was fenced on
    db_user      text    NOT NULL DEFAULT session_user,   -- the authenticated PostgreSQL login
    os_user      text,
    client_addr  inet    DEFAULT inet_client_addr()
);
CREATE INDEX IF NOT EXISTS operator_audit_command_idx ON operator_audit (command, audit_id DESC);
COMMENT ON TABLE operator_audit IS
    'Append-only operator action audit (operations.md s16). Written by src/ops in the same transaction as '
    'the change it records; UPDATE / DELETE / TRUNCATE are rejected by trigger and revoked from the engine role.';

CREATE OR REPLACE FUNCTION xtnl_operator_audit_append_only() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'operator_audit is append-only';
END
$$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operator_audit_append_only') THEN
        CREATE TRIGGER operator_audit_append_only BEFORE UPDATE OR DELETE ON operator_audit
            FOR EACH ROW EXECUTE FUNCTION xtnl_operator_audit_append_only();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operator_audit_no_truncate') THEN
        CREATE TRIGGER operator_audit_no_truncate BEFORE TRUNCATE ON operator_audit
            FOR EACH STATEMENT EXECUTE FUNCTION xtnl_operator_audit_append_only();
    END IF;
END
$$;

-- ---------------------------------------------------------------------------------------------
-- operator_requests: in-process actions for the current leader (reset_task closes a task circuit that
-- lives in the engine's TaskSupervisor). A request is valid only for requested_epoch: a new leader starts
-- with fresh circuits, so an older-epoch request is moot. The leader marks it consumed.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operator_requests (
    request_id       bigserial PRIMARY KEY,
    kind             text    NOT NULL CHECK (kind IN ('reset_task')),
    target           text    NOT NULL,
    audit_id         bigint  NOT NULL REFERENCES operator_audit (audit_id),
    requested_epoch  bigint  NOT NULL,
    requested_at_ms  bigint  NOT NULL DEFAULT now_ms(),
    consumed_at_ms   bigint,
    consumed_epoch   bigint,
    result           text
);
CREATE INDEX IF NOT EXISTS operator_requests_pending_idx ON operator_requests (requested_epoch, kind)
    WHERE consumed_at_ms IS NULL;

-- ---------------------------------------------------------------------------------------------
-- events_quarantine: an operator release (dlq release-quarantine) is recorded on the quarantine row.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE events_quarantine ADD COLUMN IF NOT EXISTS released_at_ms   bigint;
ALTER TABLE events_quarantine ADD COLUMN IF NOT EXISTS released_by      text;
ALTER TABLE events_quarantine ADD COLUMN IF NOT EXISTS release_audit_id bigint;

-- ---------------------------------------------------------------------------------------------
-- Grants. The engine role may read and append audit rows but never rewrite them. If the migration job
-- runs as the table owner, the owner keeps the right to re-grant itself, so the trigger above is the
-- guard that holds for every role. An optional operator role (xtnl_operator) gets the same append rights.
-- ---------------------------------------------------------------------------------------------
DO $$
DECLARE
    r text;
BEGIN
    FOREACH r IN ARRAY ARRAY['xtnl_engine', 'xtnl_operator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON operator_audit FROM %I', r);
            EXECUTE format('GRANT SELECT, INSERT ON operator_audit TO %I', r);
            EXECUTE format('GRANT USAGE ON SEQUENCE operator_audit_audit_id_seq TO %I', r);
            EXECUTE format('GRANT SELECT, INSERT, UPDATE ON operator_requests TO %I', r);
            EXECUTE format('REVOKE DELETE, TRUNCATE ON operator_requests FROM %I', r);
            EXECUTE format('GRANT USAGE ON SEQUENCE operator_requests_request_id_seq TO %I', r);
        END IF;
    END LOOP;
END
$$;
