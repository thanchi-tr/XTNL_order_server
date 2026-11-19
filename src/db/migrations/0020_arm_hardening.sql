-- 0020_arm_hardening.sql (plan S38f; design §B3, §B14, §C1): round-14 safety fixes to the ARM schema.
--
-- * arm_failures (D7: only a local operator command clears FAILED): xtnl_arm loses UPDATE on every identity column.
--   The REVOKE of the table privilege also revokes 0017's column-level UPDATE grants (PostgreSQL revokes a table
--   privilege's column privileges with it); the role then gets UPDATE back only on the columns its statements set
--   (the re-ping and the probe note): last_ping_at_ms, ping_count, last_probe_at_ms, last_probe_result
--   (src/db/arm_schema.py ARM_COLUMN_GRANTS). An episode's recorded evidence (counters, position_ids) is written once,
--   by the INSERT. INSERT stays column-level, without cleared_at_ms / clear_request_id.
-- * arm_failures_guard (BEFORE UPDATE, every row, every role): refuses a change of an identity column (failure_id,
--   scope_kind, scope_id, edge_code, reason, probe_kind, ping_id, first_fault_at_ms, entered_at_ms, epoch), a
--   position_ids that does not keep every earlier element, any change of a cleared episode, and a change of
--   cleared_at_ms / clear_request_id unless the current role is the table owner or a member of it (the engine pool's
--   arm_clear_failed statement); xtnl_arm is neither (the owner is a member of xtnl_arm, not the other way round).
--   Refusals raise SQLSTATE 42501 (insufficient_privilege) naming the column, never a value.
-- * arm_budget_counters: the D7 error-budget counters, durable across restarts, takeovers and a redis-md loss (§B14):
--   one row per (scope_kind, scope_id, counter) with its count (consecutive buckets, or counted milliseconds of a
--   time counter), the streak start and the last counted event. Written fenced by the ARM pool, only on change; the
--   FAILED manager restores them at start. xtnl_arm: SELECT, INSERT, UPDATE (no DELETE: a reset is count 0).
--
-- No column holds a tick, bar, quote, spread, indicator value, favour R or favour / risk: scope ids, codes, counts and
-- times only. Idempotent (IF NOT EXISTS, CREATE OR REPLACE FUNCTION, DROP TRIGGER IF EXISTS + CREATE, REVOKE / GRANT
-- repeat cleanly). Held back by src/db/migrate.py until XTNL_MIGRATIONS_ACCEPTED_UPTO >= 20; applied with
-- lock_timeout 2 s and retries; run with the schema-owner DSN, as 0017 and 0018.

SET client_min_messages = warning;

-- ---------------------------------------------------------------------------------------------
-- arm_failures: column-level UPDATE on the non-identity columns only
-- ---------------------------------------------------------------------------------------------
REVOKE UPDATE ON TABLE
    arm_failures
FROM xtnl_arm;

GRANT UPDATE (last_ping_at_ms, ping_count, last_probe_at_ms, last_probe_result) ON TABLE arm_failures TO xtnl_arm;

-- ---------------------------------------------------------------------------------------------
-- arm_failures_guard: identity columns never change; only the owner role clears
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION arm_failures_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    owner_oid oid;
BEGIN
    IF OLD.cleared_at_ms IS NOT NULL THEN
        RAISE EXCEPTION 'arm_failures: a cleared episode never changes' USING ERRCODE = '42501';
    END IF;
    IF NEW.failure_id IS DISTINCT FROM OLD.failure_id OR NEW.scope_kind IS DISTINCT FROM OLD.scope_kind
       OR NEW.scope_id IS DISTINCT FROM OLD.scope_id OR NEW.edge_code IS DISTINCT FROM OLD.edge_code
       OR NEW.reason IS DISTINCT FROM OLD.reason OR NEW.probe_kind IS DISTINCT FROM OLD.probe_kind
       OR NEW.ping_id IS DISTINCT FROM OLD.ping_id OR NEW.first_fault_at_ms IS DISTINCT FROM OLD.first_fault_at_ms
       OR NEW.entered_at_ms IS DISTINCT FROM OLD.entered_at_ms OR NEW.epoch IS DISTINCT FROM OLD.epoch THEN
        RAISE EXCEPTION 'arm_failures: an identity column of an episode never changes' USING ERRCODE = '42501';
    END IF;
    IF NOT (NEW.position_ids @> OLD.position_ids) THEN
        RAISE EXCEPTION 'arm_failures: position_ids only grow' USING ERRCODE = '42501';
    END IF;
    IF NEW.cleared_at_ms IS DISTINCT FROM OLD.cleared_at_ms
       OR NEW.clear_request_id IS DISTINCT FROM OLD.clear_request_id THEN
        SELECT c.relowner INTO owner_oid FROM pg_class c WHERE c.oid = TG_RELID;
        IF current_user = 'xtnl_arm' OR NOT pg_has_role(current_user, owner_oid, 'MEMBER') THEN
            RAISE EXCEPTION 'arm_failures: only the engine owner role clears an episode' USING ERRCODE = '42501';
        END IF;
    END IF;
    RETURN NEW;
END
$$;

REVOKE ALL ON FUNCTION arm_failures_guard() FROM PUBLIC;

DROP TRIGGER IF EXISTS arm_failures_guard ON arm_failures;
CREATE TRIGGER arm_failures_guard BEFORE UPDATE ON arm_failures
    FOR EACH ROW EXECUTE FUNCTION arm_failures_guard();

COMMENT ON FUNCTION arm_failures_guard() IS
    'D7 (plan S38f): an episode''s identity columns never change, position_ids only grow, a cleared episode is final, '
    'and only the table owner role (the engine pool''s arm_clear_failed statement) sets cleared_at_ms / '
    'clear_request_id. Refusals raise 42501.';

-- ---------------------------------------------------------------------------------------------
-- arm_budget_counters (§B14: the error-budget counters, durable)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_budget_counters (
    scope_kind                 text     NOT NULL CHECK (scope_kind IN ('SERIES', 'ADAPTER', 'STRATEGY_VERSION', 'ARM',
                                                                       'POSITION')),
    scope_id                   text     NOT NULL CHECK (scope_id ~ '^[^[:space:]]{1,200}$'),
    counter                    text     NOT NULL CHECK (counter ~ '^[a-z][a-z0-9_]{0,63}$'),
    count                      bigint   NOT NULL CHECK (count >= 0),
    streak_start_ms            bigint,
    last_event_ms              bigint,
    epoch                      bigint   NOT NULL,
    updated_at_ms              bigint   NOT NULL,
    PRIMARY KEY (scope_kind, scope_id, counter)
);
COMMENT ON TABLE arm_budget_counters IS
    'D7 error-budget counters (design §B14, plan S38f): per scope and counter code, the count, the streak start and '
    'the last counted event; written fenced by the ARM pool only on change, restored by the FAILED manager at start. '
    'Scope ids, codes, counts and times only, never a price.';

GRANT SELECT, INSERT, UPDATE ON TABLE
    arm_budget_counters
TO xtnl_arm;
