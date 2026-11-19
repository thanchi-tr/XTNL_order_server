-- 0018_arm_round12.sql (plan S38c; design §B3, §B6.3, §B14, §C1): round-12 fixes to the ARM schema of 0017.
--
-- * xtnl_arm grants narrowed (design §B3, §B10 "durable, immutable"): the role may no longer UPDATE the immutable
--   decision rows or the audit tables. arm_decisions keeps SELECT and INSERT plus a column-level UPDATE on notice_id
--   only (the one update those rows need: "no decision without a notice", §B8); arm_decision_handles,
--   arm_correction_audit and arm_config_audit keep SELECT and INSERT only. arm_positions, arm_notices, arm_status and
--   arm_config_cache keep SELECT, INSERT and UPDATE. src/db/arm_schema.py ARM_GRANTS / ARM_COLUMN_GRANTS state the
--   complete list after this migration; the S31 start-up privilege self-check compares the catalog with it.
--   The REVOKE runs on the parents only, as 0017's GRANT did (a partition carries no xtnl_arm grant).
-- * arm_decisions.outcome gains NO_BROKER_BAR (INFO): a scheduled-OPEN bucket md-bars resolved no_trade (§B6.3 (5)).
-- * arm_failures.scope_kind gains POSITION: a stuck replay frontier on fewer than fail_frontier_positions_n positions
--   fails only those positions (§B14).
--
-- No column is added: nothing here holds a tick, bar, quote, spread, indicator value, favour R or favour / risk.
-- Idempotent (REVOKE / GRANT repeat cleanly; DROP CONSTRAINT IF EXISTS + ADD). Applied by src/db/migrate.py with
-- lock_timeout 2 s and retries; run with the schema-owner DSN, as 0017.

SET client_min_messages = warning;

-- ---------------------------------------------------------------------------------------------
-- xtnl_arm: no UPDATE on the immutable decision rows and the audit tables
-- ---------------------------------------------------------------------------------------------
REVOKE UPDATE ON TABLE
    arm_decisions,
    arm_decision_handles,
    arm_correction_audit,
    arm_config_audit
FROM xtnl_arm;

GRANT UPDATE (notice_id) ON TABLE arm_decisions TO xtnl_arm;

-- ---------------------------------------------------------------------------------------------
-- CHECK value sets widened (the constraint names are PostgreSQL's defaults for 0017's column CHECKs)
-- ---------------------------------------------------------------------------------------------
ALTER TABLE arm_decisions DROP CONSTRAINT IF EXISTS arm_decisions_outcome_check;
ALTER TABLE arm_decisions ADD CONSTRAINT arm_decisions_outcome_check
    CHECK (outcome IN ('ADJUST', 'COMPLETE', 'PRE_NOT_CONFIRMED', 'AT_TARGET_UNCONFIRMED', 'LOOSENED_AFTER_COMPLETE',
                       'BROKER_AHEAD', 'STRATEGY_REASON', 'GUARD_REASON', 'MARKET_CLOSED', 'PRICE_THROUGH_LEVEL',
                       'NO_STOP', 'DATA_UNAVAILABLE', 'STRATEGY_ERROR', 'WRITE_ERROR', 'UNRESOLVED_RANGE',
                       'NO_BROKER_BAR'));

ALTER TABLE arm_failures DROP CONSTRAINT IF EXISTS arm_failures_scope_kind_check;
ALTER TABLE arm_failures ADD CONSTRAINT arm_failures_scope_kind_check
    CHECK (scope_kind IN ('SERIES', 'ADAPTER', 'STRATEGY_VERSION', 'ARM', 'POSITION'));

COMMENT ON COLUMN arm_failures.scope_kind IS
    'SERIES, ADAPTER, STRATEGY_VERSION, ARM, or POSITION (scope_id adapter_id:position_id: a stuck frontier on fewer '
    'than fail_frontier_positions_n positions, design §B14, 0018).';
