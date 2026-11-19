-- 0017_arm.sql : the ARM's PostgreSQL state (docs/design/automated-risk-manager.md §B3, §B8, §B9, §B11-§B14, §C1 0017;
-- docs/design/arm-implementation-plan.md S23). No behaviour change: nothing writes these tables until the ARM stores
-- (S24) and the ARM tasks (S26 on) exist, and those run only with XTNL_ARM_ENABLED=1.
--
-- * arm_positions: one row per (adapter_id, position_id, resource, owner); resource = 'STOP_LEVEL' and
--   owner = 'position' in Phase 1 (CHECKs, §B11). Pins (strategy and guard versions, params, indicator specs,
--   required history), entry and S0 sources, the Phase 1 single edge with its attribution_source, SM11 state and the
--   prior_state FAILED restores, row_version (the CAS), the acknowledgement pair last_decided_bar + last_decided_hash,
--   pending_event, decision_seq, and pre_basis / pre_basis_bar (round-11 confirmation C1, §B6.3).
-- * arm_decisions and arm_decision_handles: append-only, range-partitioned on bar_end_ms (ms ranges, shift 0) with a
--   DEFAULT partition; the months -1..2 are created below by an explicit loop (0009's loop covers only 0..2, ST-1),
--   and the lifecycle task keeps creating months ahead (src/db/lifecycle.py PARTITIONED, AUDIT_TABLES: kept hot,
--   never exported, detached or dropped by the engine). Unique key per §C1 so ON CONFLICT works per partition.
-- * arm_correction_audit (UNVERIFIABLE correction results), arm_notices (notice keys and parts, §B8), arm_status
--   (per node, unfenced), arm_failures (FAILED episodes, §B14), arm_controls and arm_accepted (written only by the
--   ops CLI), arm_config_audit and arm_config_cache (row hashes and the seen-row chain head, §B9).
-- * operator_requests_kind_check is dropped and re-added with the full kind list (src/db/operator_requests.py
--   REQUEST_KINDS), as 0009 did.
-- * event_log_broker_time_idx (entity_type, entity_key, broker_time_ms): the earliest version (entry time, §B7) and
--   the latest version at or before T (S_at_T, §B6.4). Created ON ONLY the partitioned parent (it builds nothing
--   here); the migrate post step builds and attaches it on every partition (src/db/migrate.py POST_STEP).
-- * Role xtnl_arm (NOLOGIN, granted to the engine role so the ARM pool can SET ROLE xtnl_arm) with the complete
--   grant list of §B3 (src/db/arm_schema.py ARM_GRANTS; a test compares the two, and the catalog in the pg test).
--   The arm-relay, operator_requests, the arm-tracker snapshot, xtnl_ensure_month_partition and the
--   arm_clear_failed clear statement run on the engine (owner) pool instead.
-- * Dashboard views (outcomes, R values, quality) with SELECT for xtnl_dashboard; no xtnl_api grant.
--
-- No column added here holds a tick, bar (OHLC), quote, spread, indicator value, favour R or favour / risk (§B8; the
-- schema scan tests/db/test_arm_schema_no_market_data.py holds every column of 0015-0017 to an allow-list). Market
-- data appears only as ids, bucket times and keyed hashes: a keyed hash is the text key_id || HMAC-SHA256 (two hex
-- digits of the key id, then 32 hex digits; src/marketdata/hashing.py), and hashes under different key ids are never
-- compared. Levels kept here (entry, stops, the proposed stop, pre_basis) are order state. R values are
-- dimensionless multiples (0.01 R), ratios 0.001. Idempotent (IF NOT EXISTS, guarded role creation, DROP CONSTRAINT
-- IF EXISTS + ADD, CREATE OR REPLACE VIEW, REVOKE ALL ON ALL TABLES / SEQUENCES then GRANT). Creating a role needs
-- CREATEROLE: run the migration with the schema-owner DSN, as for 0011 / 0014. Applied by src/db/migrate.py with
-- lock_timeout 2 s and retries.

SET client_min_messages = warning;

-- ---------------------------------------------------------------------------------------------
-- arm_positions (§B6.1 pins, §B7 inputs, §B11 edge, §B12 SM11, §B6.3 pre_basis, §B6.6 cursor)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_positions (
    adapter_id                 text     NOT NULL,
    position_id                text     NOT NULL,
    resource                   text     NOT NULL DEFAULT 'STOP_LEVEL' CHECK (resource = 'STOP_LEVEL'),
    owner                      text     NOT NULL DEFAULT 'position' CHECK (owner = 'position'),
    account_id                 text     NOT NULL,
    symbol                     text     NOT NULL,
    side                       text     NOT NULL CHECK (side IN ('BUY', 'SELL')),
    state                      text     NOT NULL CHECK (state IN ('DISCOVERED', 'ARMED', 'TRACKING',
                                            'ADVICE_OUTSTANDING', 'STRATEGY_COMPLETE', 'SUSPENDED', 'FAILED',
                                            'CLOSED', 'DISARMED')),
    prior_state                text     CHECK (prior_state IN ('DISCOVERED', 'ARMED', 'TRACKING',
                                            'ADVICE_OUTSTANDING', 'STRATEGY_COMPLETE', 'SUSPENDED')),
    state_reason               text     CHECK (state_reason ~ '^[a-z][a-z0-9_]{0,63}$'),
    row_version                bigint   NOT NULL DEFAULT 0 CHECK (row_version >= 0),
    edge_code                  text     NOT NULL CHECK (edge_code ~ '^[a-z0-9_]{1,32}$'),
    attribution_source         text     NOT NULL CHECK (attribution_source IN ('SINGLE_EDGE_DEFAULT', 'OPERATOR_TAG',
                                            'INTENT')),
    opened_by_client_order_id  text,
    strategy_code              text     NOT NULL CHECK (strategy_code ~ '^[a-z0-9_]{1,32}$'),
    strategy_version           int      NOT NULL CHECK (strategy_version > 0),
    strategy_kind              text     NOT NULL,
    strategy_impl_version      int      NOT NULL CHECK (strategy_impl_version > 0),
    strategy_params            jsonb    NOT NULL,
    guards                     jsonb    NOT NULL DEFAULT '[]'::jsonb,
    guards_hash                text     NOT NULL CHECK (guards_hash ~ '^[0-9a-f]{16,64}$'),
    indicator_specs            jsonb    NOT NULL DEFAULT '[]'::jsonb,
    required_history           jsonb    NOT NULL DEFAULT '[]'::jsonb,
    bar_spec                   text     NOT NULL,
    config_doc_hash            char(64) NOT NULL CHECK (config_doc_hash ~ '^[0-9a-f]{64}$'),
    entry_level                numeric  NOT NULL,
    entry_time_ms              bigint   NOT NULL,
    entry_time_source          text     NOT NULL CHECK (entry_time_source IN ('STREAM', 'BROKER', 'ESTIMATED')),
    s0_level                   numeric  NOT NULL,
    s0_source                  text     NOT NULL CHECK (s0_source IN ('AT_FILL', 'FIRST_SEEN', 'HISTORY_LAST_LEVEL')),
    pinned_units               numeric  NOT NULL,
    strategy_state             jsonb    NOT NULL DEFAULT '{}'::jsonb,
    last_decided_bar           bigint,
    last_decided_hash          char(34) CHECK (last_decided_hash ~ '^[0-9a-f]{34}$'),
    pending_event              text     CHECK (pending_event IN ('PRE', 'POST', 'LATE', 'REOPEN', 'CORRECTED')),
    pending_bar_end_ms         bigint,
    reopen_bar_end_ms          bigint,
    decision_seq               bigint   NOT NULL DEFAULT 0 CHECK (decision_seq >= 0),
    pre_basis                  jsonb,
    pre_basis_bar              bigint,
    armed_at_ms                bigint   NOT NULL,
    updated_at_ms              bigint   NOT NULL,
    ended_at_ms                bigint,
    epoch                      bigint   NOT NULL,
    PRIMARY KEY (adapter_id, position_id, resource, owner),
    CONSTRAINT arm_positions_failed_prior_check CHECK ((state = 'FAILED') = (prior_state IS NOT NULL)),
    CONSTRAINT arm_positions_pre_basis_check CHECK ((pre_basis IS NULL) = (pre_basis_bar IS NULL)),
    CONSTRAINT arm_positions_pending_check CHECK ((pending_event IS NULL) = (pending_bar_end_ms IS NULL))
);
CREATE INDEX IF NOT EXISTS arm_positions_live_idx ON arm_positions (adapter_id, account_id)
    WHERE state NOT IN ('CLOSED', 'DISARMED');
CREATE INDEX IF NOT EXISTS arm_positions_strategy_idx ON arm_positions (strategy_kind, strategy_impl_version)
    WHERE state NOT IN ('CLOSED', 'DISARMED');
COMMENT ON TABLE arm_positions IS
    'ARM position rows (design §B6.1, §B7, §B11, §B12): pins, sources, SM11 state (set only through '
    'src/riskmgr/sm11.py), row_version CAS, the last_decided_bar / last_decided_hash acknowledgement (§B6.6).';
COMMENT ON COLUMN arm_positions.opened_by_client_order_id IS
    'Future edge attribution input (§B11). On StoneX this is the broker''s own order id (no client order ids), so '
    'it carries no edge information: attribution there needs OPERATOR_TAG or a Phase 2 INTENT.';
COMMENT ON COLUMN arm_positions.attribution_source IS
    'How edge_code was attributed (§B11). Phase 1 writes only SINGLE_EDGE_DEFAULT (edge_code = XTNL_ARM_EDGE).';
COMMENT ON COLUMN arm_positions.prior_state IS
    'The SM11 state left when a FAILED scope covered the position; set exactly while state = FAILED (CHECK), so '
    'the operator clear that restores it (§B12) also sets it back to NULL.';
COMMENT ON COLUMN arm_positions.strategy_state IS
    'The strategy''s STATE_SCHEMA document: flags and order-state levels only, never market data.';
COMMENT ON COLUMN arm_positions.last_decided_hash IS
    'key_id || HMAC of the trigger-series bar last decided (§B6.6); NULL after an UNRESOLVED_RANGE span.';
COMMENT ON COLUMN arm_positions.pre_basis IS
    'After PRE_NOT_CONFIRMED: the handle levels the PRE used (order state), keyed by handle role, never order id '
    '(round-11 confirmation C1, §B6.3); cleared when a bar end confirms or the stop is loosened past it.';
COMMENT ON COLUMN arm_positions.reopen_bar_end_ms IS
    'A trigger_met recorded on a bar whose POST fell in a closed period: the pending REOPEN re-check (§B6.3).';

-- ---------------------------------------------------------------------------------------------
-- arm_decisions (§B5.3, §B6.6, §B8, §B10, §C1): append-only, partitioned on bar_end_ms
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_decisions (
    decision_id                bigserial,
    bar_end_ms                 bigint   NOT NULL,           -- partition key; a span takes its last bucket's end
    adapter_id                 text     NOT NULL,
    position_id                text     NOT NULL,
    resource                   text     NOT NULL CHECK (resource = 'STOP_LEVEL'),
    owner                      text     NOT NULL CHECK (owner = 'position'),
    bar_spec                   text     NOT NULL,
    event                      text     NOT NULL CHECK (event IN ('PRE', 'POST', 'LATE', 'REOPEN', 'CORRECTED')),
    revision                   int      NOT NULL DEFAULT 0 CHECK (revision >= 0),
    decision_seq               bigint   NOT NULL CHECK (decision_seq > 0),
    decided_at_ms              bigint   NOT NULL,
    epoch                      bigint   NOT NULL,
    edge_code                  text     NOT NULL,
    outcome                    text     NOT NULL CHECK (outcome IN ('ADJUST', 'COMPLETE', 'PRE_NOT_CONFIRMED',
                                            'AT_TARGET_UNCONFIRMED', 'LOOSENED_AFTER_COMPLETE', 'BROKER_AHEAD',
                                            'STRATEGY_REASON', 'GUARD_REASON', 'MARKET_CLOSED',
                                            'PRICE_THROUGH_LEVEL', 'NO_STOP', 'DATA_UNAVAILABLE', 'STRATEGY_ERROR',
                                            'WRITE_ERROR', 'UNRESOLVED_RANGE')),
    category                   text     NOT NULL CHECK (category IN ('ACTIONABLE', 'INFO', 'HOLD', 'FAULT')),
    reason                     text     CHECK (reason ~ '^[A-Za-z][A-Za-z0-9_]{0,63}$'),
    reason_kind                text,                         -- the strategy or guard KIND a reason code belongs to
    trigger_met                boolean,
    borderline                 boolean  NOT NULL DEFAULT false,
    advice                     boolean  NOT NULL,
    proposed_stop              numeric,                      -- order state (the target level)
    current_risk_r             numeric(12, 2),               -- sigma * (E - S) in R, from order state only
    first_bar_ratio            numeric(12, 3),               -- §B8: the first-bar ratio, 0.001
    quality_flags              text[]   NOT NULL DEFAULT '{}',
    caveats                    text[]   NOT NULL DEFAULT '{}',
    close_source               text     CHECK (close_source IN ('QUOTE', 'BROKER')),
    stop_basis                 text     CHECK (stop_basis IN ('STREAM', 'READ', 'PRIOR_VERSION', 'AT_PRE')),
    series_inputs              text[]   NOT NULL DEFAULT '{}',   -- 'series_id|bar_end_ms|key_id||hash'
    indicator_inputs           text[]   NOT NULL DEFAULT '{}',   -- 'indicator_id|output|bar_end_ms|window_start|hash'
    inputs_digest              char(34) CHECK (inputs_digest ~ '^[0-9a-f]{34}$'),
    envelope_version           int      NOT NULL CHECK (envelope_version > 0),
    envelope_params_hash       text     NOT NULL CHECK (envelope_params_hash ~ '^[0-9a-f]{16,64}$'),
    strategy_kind              text     NOT NULL,
    strategy_impl_version      int      NOT NULL CHECK (strategy_impl_version > 0),
    guards_hash                text     NOT NULL CHECK (guards_hash ~ '^[0-9a-f]{16,64}$'),
    supersedes_decision_id     bigint,
    supersedes_bar_end_ms      bigint,
    range_from_bucket          bigint,
    notice_id                  bigint   CHECK (notice_id >= 0),  -- NULL until rendered; 0 = the suppressed sentinel
    PRIMARY KEY (decision_id, bar_end_ms),
    CONSTRAINT arm_decisions_key UNIQUE (adapter_id, position_id, resource, owner, bar_spec, event, bar_end_ms,
                                         revision),
    CONSTRAINT arm_decisions_supersedes_check CHECK ((supersedes_decision_id IS NULL) = (supersedes_bar_end_ms IS NULL)),
    CONSTRAINT arm_decisions_range_check CHECK ((range_from_bucket IS NULL) = (outcome <> 'UNRESOLVED_RANGE')),
    CONSTRAINT arm_decisions_pre_advice_check CHECK (event <> 'PRE' OR NOT advice)
) PARTITION BY RANGE (bar_end_ms);
CREATE TABLE IF NOT EXISTS arm_decisions_default PARTITION OF arm_decisions DEFAULT;
CREATE INDEX IF NOT EXISTS arm_decisions_unnoticed_idx ON arm_decisions (bar_spec, event, bar_end_ms)
    WHERE notice_id IS NULL;
CREATE INDEX IF NOT EXISTS arm_decisions_position_idx ON arm_decisions (adapter_id, position_id, decision_seq);
COMMENT ON TABLE arm_decisions IS
    'Append-only ARM decisions (design §B6.6, §B8, §B10). No OHLC, quote, spread, indicator value, favour R or '
    'favour / risk: inputs are ids with keyed hashes plus the HMAC inputs_digest. Partitioned on bar_end_ms (ms); '
    'kept hot by the lifecycle task (AUDIT_TABLES): never exported, detached or dropped by the engine.';
COMMENT ON COLUMN arm_decisions.notice_id IS
    'The arm_notices row this decision was rendered in; NULL until then; 0 = the suppressed sentinel (§B8).';
COMMENT ON COLUMN arm_decisions.supersedes_decision_id IS
    'With supersedes_bar_end_ms: the composite reference (decision_id, bar_end_ms) of the revised row (§B6.6).';

CREATE TABLE IF NOT EXISTS arm_decision_handles (
    decision_id                bigint   NOT NULL,
    bar_end_ms                 bigint   NOT NULL,           -- partition key (the decision's)
    handle_id                  text     NOT NULL,
    handle_kind                text     NOT NULL CHECK (handle_kind IN ('CHILD_ORDER', 'POSITION_FIELD')),
    handle_role                text     NOT NULL CHECK (handle_role ~ '^(POSITION_FIELD|CHILD_ORDER:[0-9]{1,3})$'),
    level_at_bar_end           numeric,                      -- order state as at bar end (S_at_T)
    broker_seq_at_bar_end      bigint,
    broker_updated_at_bar_end  bigint,
    level_read                 numeric,                      -- order state as last read before the post-guards
    broker_seq_read            bigint,
    broker_updated_read        bigint,
    read_at_ms                 bigint,
    level_at_pre               numeric,                      -- the level this bar's PRE used (matched by role)
    target_level               numeric,
    PRIMARY KEY (decision_id, bar_end_ms, handle_id)
) PARTITION BY RANGE (bar_end_ms);
CREATE TABLE IF NOT EXISTS arm_decision_handles_default PARTITION OF arm_decision_handles DEFAULT;
COMMENT ON TABLE arm_decision_handles IS
    'One row per stop handle per decision (design §B10), all order state; partitioned and kept like arm_decisions.';

DO $$
DECLARE
    m int;
    base date := date_trunc('month', now() AT TIME ZONE 'UTC')::date;
BEGIN
    FOR m IN -1..2 LOOP
        PERFORM xtnl_ensure_month_partition('arm_decisions',        (base + make_interval(months => m))::date, 0);
        PERFORM xtnl_ensure_month_partition('arm_decision_handles', (base + make_interval(months => m))::date, 0);
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------------------------
-- arm_correction_audit (§B6.6: UNVERIFIABLE correction results, audit only)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_correction_audit (
    audit_id                   bigserial PRIMARY KEY,
    decision_id                bigint   NOT NULL,
    bar_end_ms                 bigint   NOT NULL,
    adapter_id                 text     NOT NULL,
    position_id                text     NOT NULL,
    result                     text     NOT NULL CHECK (result = 'UNVERIFIABLE'),
    reason                     text     NOT NULL CHECK (reason ~ '^[a-z][a-z0-9_]{0,63}$'),
    recorded_at_ms             bigint   NOT NULL,
    epoch                      bigint   NOT NULL,
    UNIQUE (decision_id, bar_end_ms)
);

-- ---------------------------------------------------------------------------------------------
-- arm_notices (§B8: notice keys and parts)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_notices (
    notice_id                  bigserial PRIMARY KEY,
    route                      text     NOT NULL,
    bar_spec                   text     NOT NULL DEFAULT '',
    event                      text     NOT NULL CHECK (event ~ '^[A-Z][A-Z_]{1,31}$'),
    bar_end_ms                 bigint   NOT NULL,
    part                       smallint NOT NULL DEFAULT 0,
    item_key                   text     NOT NULL DEFAULT '',
    decision_count             int      NOT NULL DEFAULT 0 CHECK (decision_count >= 0),
    outbox_id                  bigint,
    created_at_ms              bigint   NOT NULL,
    epoch                      bigint   NOT NULL,
    CONSTRAINT arm_notices_key UNIQUE (route, bar_spec, event, bar_end_ms, part, item_key),
    CONSTRAINT arm_notices_part_check CHECK (part BETWEEN 0 AND 5),
    CONSTRAINT arm_notices_part_event_check CHECK (part = 0 OR event IN ('PRE', 'POST'))
);
CREATE INDEX IF NOT EXISTS arm_notices_created_idx ON arm_notices (created_at_ms);
COMMENT ON COLUMN arm_notices.item_key IS
    'Per-item notices (§B8): adapter_id:position_id:resource:owner:revision, so items of two brokers that reuse a '
    'position id never share a key; empty for bar notices.';

-- ---------------------------------------------------------------------------------------------
-- arm_status (per node, unfenced, like engine_runs / uptime)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_status (
    node                       text     PRIMARY KEY,
    epoch                      bigint,
    updated_at_ms              bigint   NOT NULL,
    heartbeat_at_ms            bigint,
    leader                     boolean  NOT NULL DEFAULT false,
    running                    boolean  NOT NULL DEFAULT false,
    paused                     boolean  NOT NULL DEFAULT false,
    pause_reason               text     CHECK (pause_reason ~ '^[a-z][a-z0-9_]{0,63}$'),
    config_doc_hash            char(64),
    armed_positions            int      NOT NULL DEFAULT 0 CHECK (armed_positions >= 0),
    unmanaged_positions        int      NOT NULL DEFAULT 0 CHECK (unmanaged_positions >= 0),
    overdue_positions          int      NOT NULL DEFAULT 0 CHECK (overdue_positions >= 0),
    failed_scopes              int      NOT NULL DEFAULT 0 CHECK (failed_scopes >= 0),
    failed_global              boolean  NOT NULL DEFAULT false,
    counters                   jsonb    NOT NULL DEFAULT '{}'::jsonb,  -- counts and codes only, never a value
    -- this node's market-data record (§A7 NodeMdRecord): the md:boot sentinel it wrote (ids only) and the newest
    -- last_write_ms its writer wrote; saved by its own statement, never by the 5 s status upsert
    md_sentinel                text     CHECK (md_sentinel ~
                                            '^[A-Za-z0-9]{1,64}:[A-Za-z0-9._-]{1,64}:[A-Za-z0-9._-]{1,64}:[0-9]{1,18}\.[0-9]{1,18}$'),
    md_last_write_ms           bigint   CHECK (md_last_write_ms >= 0),
    CONSTRAINT arm_status_md_record_check CHECK ((md_sentinel IS NULL) = (md_last_write_ms IS NULL))
);

-- ---------------------------------------------------------------------------------------------
-- arm_failures (§B14: one row per FAILED episode)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_failures (
    failure_id                 text     PRIMARY KEY CHECK (failure_id ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$'),
    scope_kind                 text     NOT NULL CHECK (scope_kind IN ('SERIES', 'ADAPTER', 'STRATEGY_VERSION', 'ARM')),
    scope_id                   text     NOT NULL,
    edge_code                  text,                         -- the edge label on a STRATEGY_VERSION scope (§B11)
    reason                     text     NOT NULL CHECK (reason ~ '^[a-z][a-z0-9_]{0,63}$'),
    probe_kind                 text     NOT NULL CHECK (probe_kind IN ('series_reread', 'fresh_bar',
                                            'indicator_rebuild', 'span_write', 'strategy_conformance',
                                            'adapter_series', 'arm_global')),
    ping_id                    text     NOT NULL CHECK (ping_id ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$'),
    counters                   jsonb    NOT NULL DEFAULT '{}'::jsonb,  -- counts, bucket times, check names only
    position_ids               text[]   NOT NULL DEFAULT '{}',
    first_fault_at_ms          bigint   NOT NULL,
    entered_at_ms              bigint   NOT NULL,
    epoch                      bigint   NOT NULL,
    last_ping_at_ms            bigint,
    ping_count                 int      NOT NULL DEFAULT 1 CHECK (ping_count >= 0),
    last_probe_at_ms           bigint,
    last_probe_result          text     CHECK (last_probe_result ~ '^[a-z][a-z0-9_:]{0,63}$'),
    cleared_at_ms              bigint,
    clear_request_id           bigint,
    CONSTRAINT arm_failures_clear_check CHECK ((cleared_at_ms IS NULL) = (clear_request_id IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS arm_failures_open_idx ON arm_failures (scope_kind, scope_id)
    WHERE cleared_at_ms IS NULL;
COMMENT ON TABLE arm_failures IS
    'FAILED scopes (design §B14), one row per episode: ids, reason codes and counts only, never a price. Inserted '
    'by the engine under the fence; cleared only by the leader''s arm_clear_failed handler on the engine pool '
    '(cleared_at_ms, clear_request_id; xtnl_arm cannot set them).';
COMMENT ON COLUMN arm_failures.probe_kind IS
    'The clear probe of the recorded cause (§B14 Clearing, R11-M12), chosen from the reason when the episode opens '
    '(src/db/arm_schema.py probe_kind_for).';
COMMENT ON COLUMN arm_failures.position_ids IS
    'The affected positions as adapter_id:position_id (two brokers may reuse a position id; S23 deviation 1).';

-- ---------------------------------------------------------------------------------------------
-- arm_controls, arm_accepted (written only by the ops CLI, with an operator_audit row; §B9, §B13)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_controls (
    id                         smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    paused                     boolean  NOT NULL DEFAULT false,
    pause_reason               text     CHECK (pause_reason ~ '^[a-z][a-z0-9_]{0,63}$'),
    paused_at_ms               bigint,
    drain_notices              boolean  NOT NULL DEFAULT false,
    notify_test_at_ms          bigint,
    updated_at_ms              bigint   NOT NULL DEFAULT now_ms(),
    updated_by                 text,
    audit_id                   bigint
);
INSERT INTO arm_controls (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS arm_accepted (
    accepted_id                bigserial PRIMARY KEY,
    doc_hash                   char(64) NOT NULL CHECK (doc_hash ~ '^[0-9a-f]{64}$'),
    schema_version             int      NOT NULL,
    mode                       text     NOT NULL DEFAULT 'manual' CHECK (mode IN ('manual', 'automatic')),
    accepted_at_ms             bigint   NOT NULL,
    accepted_by                text     NOT NULL,
    audit_id                   bigint
);
CREATE INDEX IF NOT EXISTS arm_accepted_latest_idx ON arm_accepted (accepted_at_ms DESC, accepted_id DESC);

-- ---------------------------------------------------------------------------------------------
-- arm_config_audit, arm_config_cache (§B9: rows named by id and row hash only, never by their text)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS arm_config_audit (
    config_audit_id            bigserial PRIMARY KEY,
    at_ms                      bigint   NOT NULL,
    epoch                      bigint,
    node                       text     NOT NULL,
    action                     text     NOT NULL CHECK (action ~ '^[a-z][a-z0-9_]{0,63}$'),
    doc_hash                   char(64) CHECK (doc_hash ~ '^[0-9a-f]{64}$'),
    row_refs                   text[]   NOT NULL DEFAULT '{}'   -- 'table:row_id:row_hash'
);

CREATE TABLE IF NOT EXISTS arm_config_cache (
    id                         smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    doc_hash                   char(64) NOT NULL CHECK (doc_hash ~ '^[0-9a-f]{64}$'),
    schema_version             int      NOT NULL,
    document                   jsonb    NOT NULL,             -- the accepted document (config, never market data)
    row_hashes                 jsonb    NOT NULL DEFAULT '{}'::jsonb,   -- seen row ref -> row hash
    chain_head                 char(64) NOT NULL CHECK (chain_head ~ '^[0-9a-f]{64}$'),
    chain_rows                 int      NOT NULL DEFAULT 0 CHECK (chain_rows >= 0),
    updated_at_ms              bigint   NOT NULL,
    epoch                      bigint
);
COMMENT ON TABLE arm_config_cache IS
    'The cached accepted ARM document plus the sha256 chain over every Supabase row the engine has seen (§B9): '
    'pending = a row not seen before; tampered = a seen row changed or vanished.';

-- ---------------------------------------------------------------------------------------------
-- operator_requests: the full kind list (design §B13; src/db/operator_requests.py REQUEST_KINDS)
-- ---------------------------------------------------------------------------------------------
ALTER TABLE operator_requests DROP CONSTRAINT IF EXISTS operator_requests_kind_check;
ALTER TABLE operator_requests ADD CONSTRAINT operator_requests_kind_check
    CHECK (kind IN ('reset_task', 'release_account', 'refresh_cash_flows', 'arm_disarm', 'arm_repin', 'arm_rearm',
                    'arm_clear_failed', 'md_backfill', 'md_rebuild', 'md_writer_reset', 'md_rehash'));

-- ---------------------------------------------------------------------------------------------
-- event_log index for entry time and S_at_T (PR8, R11-M06); built per partition by the post step
-- ---------------------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS event_log_broker_time_idx
    ON ONLY event_log (entity_type, entity_key, broker_time_ms);

-- ---------------------------------------------------------------------------------------------
-- Role xtnl_arm and its grants (§B3: the complete list; src/db/arm_schema.py ARM_GRANTS)
-- ---------------------------------------------------------------------------------------------
DO $$
DECLARE
    r text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_arm') THEN
        CREATE ROLE xtnl_arm NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    -- the engine role(s): the migration runner (the owner the engine pool connects as) and xtnl_engine if it exists
    FOREACH r IN ARRAY ARRAY[current_user::text, 'xtnl_engine'] LOOP
        IF r <> 'xtnl_arm' AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
            EXECUTE format('GRANT xtnl_arm TO %I', r);
        END IF;
    END LOOP;
END
$$;

GRANT USAGE ON SCHEMA public TO xtnl_arm;

-- clean slate: every table, view, partition and sequence of the schema (an earlier hand-run grant never survives)
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM xtnl_arm;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM xtnl_arm;

GRANT SELECT, INSERT, UPDATE ON TABLE
    arm_positions,
    arm_decisions,
    arm_decision_handles,
    arm_correction_audit,
    arm_notices,
    arm_status,
    arm_config_cache,
    arm_config_audit
TO xtnl_arm;

-- arm_failures: SELECT on the table; INSERT and UPDATE only on the columns other than cleared_at_ms /
-- clear_request_id, so xtnl_arm can neither clear an episode nor insert one already cleared (§B3, §B14)
GRANT SELECT ON TABLE
    arm_failures
TO xtnl_arm;

GRANT INSERT (failure_id, scope_kind, scope_id, edge_code, reason, probe_kind, ping_id, counters, position_ids,
              first_fault_at_ms, entered_at_ms, epoch, last_ping_at_ms, ping_count, last_probe_at_ms,
              last_probe_result) ON TABLE arm_failures TO xtnl_arm;

GRANT UPDATE (failure_id, scope_kind, scope_id, edge_code, reason, probe_kind, ping_id, counters, position_ids,
              first_fault_at_ms, entered_at_ms, epoch, last_ping_at_ms, ping_count, last_probe_at_ms,
              last_probe_result) ON TABLE arm_failures TO xtnl_arm;

GRANT INSERT ON TABLE
    notification_outbox
TO xtnl_arm;

GRANT SELECT ON TABLE
    arm_accepted,
    arm_controls,
    engine_epoch,
    event_log,
    orders_state,
    positions_state
TO xtnl_arm;

GRANT USAGE ON SEQUENCE
    arm_decisions_decision_id_seq,
    arm_correction_audit_audit_id_seq,
    arm_notices_notice_id_seq,
    arm_config_audit_config_audit_id_seq,
    notification_outbox_outbox_id_seq
TO xtnl_arm;

-- ---------------------------------------------------------------------------------------------
-- Dashboard views (outcomes, R values, quality): SELECT for xtnl_dashboard, never for xtnl_api
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW arm_positions_view AS
    SELECT adapter_id, position_id, account_id, symbol, side, state, prior_state, state_reason, edge_code,
           strategy_code, strategy_version, strategy_kind, strategy_impl_version, bar_spec, entry_time_ms,
           entry_time_source, s0_source, last_decided_bar, pending_event, decision_seq, armed_at_ms, updated_at_ms,
           ended_at_ms
    FROM arm_positions;

CREATE OR REPLACE VIEW arm_decisions_view AS
    SELECT decision_id, bar_end_ms, adapter_id, position_id, bar_spec, event, revision, decision_seq, decided_at_ms,
           edge_code, outcome, category, reason, reason_kind, trigger_met, borderline, advice, current_risk_r,
           first_bar_ratio, quality_flags, caveats, close_source, stop_basis, envelope_version, strategy_kind,
           strategy_impl_version, notice_id
    FROM arm_decisions;

CREATE OR REPLACE VIEW arm_failures_view AS
    SELECT failure_id, scope_kind, scope_id, edge_code, reason, probe_kind, ping_id, position_ids,
           first_fault_at_ms, entered_at_ms, ping_count, last_probe_at_ms, last_probe_result, cleared_at_ms
    FROM arm_failures;

GRANT SELECT ON TABLE
    arm_positions_view,
    arm_decisions_view,
    arm_failures_view
TO xtnl_dashboard;
