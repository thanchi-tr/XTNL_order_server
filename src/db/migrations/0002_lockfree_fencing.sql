-- 0002_lockfree_fencing.sql : single-leader lock-free fencing + columns the Oversee / pubsub stores need.
--
-- Spec: skill §1 P12 (`WHERE (SELECT epoch FROM engine_epoch) = $1` on every state write), operations.md §2,
-- tier-persistence.md §4, oversee-rules.md §2, §8, §10.
-- Idempotent (IF NOT EXISTS / guarded DO blocks), applied in order by src/db/migrate.py.

SET client_min_messages = warning;

-- ---------------------------------------------------------------------------------------------
-- Fencing token: engine_epoch.epoch (= the etcd lease revision). Generated, so it can never diverge
-- from `revision`; bump_epoch only ever increases it for the same cluster_id.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE engine_epoch ADD COLUMN IF NOT EXISTS epoch bigint GENERATED ALWAYS AS (revision) STORED;
COMMENT ON TABLE engine_epoch IS
    'Single-row fencing token (operations.md §2). Bumped only to a newer revision of the same cluster_id; '
    'every engine state write is conditional on (SELECT epoch FROM engine_epoch WHERE id = 1) = $1 in the '
    'same statement (lock-free fencing, no advisory lock).';

-- ---------------------------------------------------------------------------------------------
-- Balance / position skew input (oversee-rules.md §2): commit time of the last status or units change.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE orders_state    ADD COLUMN IF NOT EXISTS status_units_changed_at_ms bigint NOT NULL DEFAULT now_ms();
ALTER TABLE positions_state ADD COLUMN IF NOT EXISTS status_units_changed_at_ms bigint NOT NULL DEFAULT now_ms();
CREATE INDEX IF NOT EXISTS orders_state_open_idx ON orders_state (account_id)
    WHERE status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED');
CREATE INDEX IF NOT EXISTS positions_state_open_acct_idx ON positions_state (account_id) WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS account_state_account_idx ON account_state (account_id);

-- ---------------------------------------------------------------------------------------------
-- Oversee rule states: Oversee keys accounts by account_id; adapter_id is filled from account_state
-- where known (nullable), and halt_breaches gains the BreachState field it lacked.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE halt_breaches ADD COLUMN IF NOT EXISTS unknown_origin boolean NOT NULL DEFAULT false;
ALTER TABLE halt_breaches ALTER COLUMN adapter_id DROP NOT NULL;
CREATE INDEX IF NOT EXISTS halt_breaches_account_only_idx ON halt_breaches (account_id);
CREATE INDEX IF NOT EXISTS halt_breaches_unresolved_idx ON halt_breaches (account_id) WHERE resolved_at IS NULL;

ALTER TABLE stop_watch ALTER COLUMN adapter_id DROP NOT NULL;
CREATE INDEX IF NOT EXISTS stop_watch_account_idx ON stop_watch (account_id);

ALTER TABLE oversee_events ALTER COLUMN adapter_id DROP NOT NULL;
ALTER TABLE oversee_events ADD COLUMN IF NOT EXISTS activation_id bigint;

-- Swap the primary key to (account_id) FIRST: PostgreSQL rejects DROP NOT NULL on a column that is still
-- part of a primary key (ERROR 42P16 "column ... is in a primary key"), and dropping the old key leaves
-- the column's NOT NULL in place, so the explicit DROP NOT NULL must follow the swap.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_attribute a
                   ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
               WHERE c.conname = 'sweep_quarantine_pkey' AND a.attname = 'adapter_id') THEN
        ALTER TABLE sweep_quarantine DROP CONSTRAINT sweep_quarantine_pkey;
        ALTER TABLE sweep_quarantine ADD CONSTRAINT sweep_quarantine_pkey PRIMARY KEY (account_id);
    END IF;
END
$$;
ALTER TABLE sweep_quarantine ALTER COLUMN adapter_id DROP NOT NULL;
ALTER TABLE sweep_quarantine ALTER COLUMN next_retry_at_ms SET DEFAULT 0;

-- ---------------------------------------------------------------------------------------------
-- notification_outbox: the OutboxRow fields (kind, account_id) and send bookkeeping; 'HIGH' severity.
-- ---------------------------------------------------------------------------------------------
ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS kind       text NOT NULL DEFAULT 'info';
ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS account_id text;
ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS episode    jsonb;
ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS sent_at_ms bigint;
ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS delivered  boolean;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'notification_outbox_severity_check'
               AND pg_get_constraintdef(oid) NOT LIKE '%HIGH%') THEN
        ALTER TABLE notification_outbox DROP CONSTRAINT notification_outbox_severity_check;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'notification_outbox_severity_check') THEN
        ALTER TABLE notification_outbox ADD CONSTRAINT notification_outbox_severity_check
            CHECK (severity IN ('INFO', 'FLAG', 'SOFT', 'HARD', 'HIGH', 'CRITICAL'));
    END IF;
END
$$;
CREATE INDEX IF NOT EXISTS notification_outbox_pending_idx ON notification_outbox (outbox_id) WHERE sent_at_ms IS NULL;

-- ---------------------------------------------------------------------------------------------
-- adapter_auth_state lookups by (adapter, generation) are the PK; schema_migrations is created by the
-- runner itself (src/db/migrate.py), not here.
-- ---------------------------------------------------------------------------------------------
