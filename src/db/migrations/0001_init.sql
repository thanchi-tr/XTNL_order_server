-- 0001_init.sql : XTNL Phase 1 schema (PostgreSQL 16+; production runs 17 under Patroni).
--
-- Spec: .claude/rules/tier-persistence.md §4-§5, operations.md §2, §4, §11, §13, oversee-rules.md §2-§10.
-- Idempotent: every object uses IF NOT EXISTS / CREATE OR REPLACE, so re-running is a no-op.
-- Run as an operator job before a switchover with SET lock_timeout = '2s' (operations.md §12), never at
-- engine start-up.
--
-- Conventions
--   * Money and quantities: NUMERIC (never float).
--   * Engine timestamps and deadlines: BIGINT epoch milliseconds (columns ending in _ms, and the
--     spec-named deadline columns such as pending_since, conn_lost_at, started_at). now_ms() below gives
--     the server clock in ms so SQL-side defaults match the engine's units.
--   * seq_id / sweep_id: BIGINT HLC values, seq_id = max(epoch_ms << 20, last + 1).
--   * Entity current rows carry the version as separate columns (broker_seq, broker_updated_at_ms,
--     observed_seq) PLUS the fixed-width string `version` (src.dto.version_str, 56 digits) that Redis
--     apply_state.lua compares, seq_id (the last tie-breaker), checksum and digest_hash
--     (sha256(redis_key || version || checksum), hex) so PostgreSQL holds the same commutative digest as
--     Redis state:digest without a single hot digest row (XOR-aggregate with bit_xor when needed).

SET client_min_messages = warning;

-- ---------------------------------------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION now_ms() RETURNS bigint
    LANGUAGE sql VOLATILE PARALLEL SAFE
AS $$ SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint $$;
COMMENT ON FUNCTION now_ms() IS 'Server wall clock in epoch milliseconds (engine timestamp unit).';

-- Creates (if missing) the monthly range partition of `parent` that holds `month_start`.
-- shift = 20 for seq_id / sweep_id (HLC: epoch_ms << 20), 0 for plain ms columns.
CREATE OR REPLACE FUNCTION xtnl_ensure_month_partition(parent text, month_start date, shift int)
    RETURNS text
    LANGUAGE plpgsql
AS $$
DECLARE
    m0      date   := date_trunc('month', month_start)::date;
    m1      date   := (date_trunc('month', month_start) + interval '1 month')::date;
    lo      bigint := ((extract(epoch FROM (m0::timestamp AT TIME ZONE 'UTC')) * 1000)::bigint) << shift;
    hi      bigint := ((extract(epoch FROM (m1::timestamp AT TIME ZONE 'UTC')) * 1000)::bigint) << shift;
    part    text   := parent || '_p' || to_char(m0, 'YYYYMM');
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s)',
                   part, parent, lo, hi);
    RETURN part;
END
$$;
COMMENT ON FUNCTION xtnl_ensure_month_partition(text, date, int) IS
    'operations.md §11: monthly range partitions (HLC seq_id ranges map to ~monthly windows). '
    'The lifecycle job creates months ahead and exports/verifies before detach.';

-- ---------------------------------------------------------------------------------------------
-- HA / runtime (operations.md §2, §4)
-- ---------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS engine_epoch (
    id            smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    cluster_id    text     NOT NULL,             -- etcd cluster_id
    revision      bigint   NOT NULL,             -- etcd lease revision (the fencing token)
    node          text     NOT NULL,
    bumped_at_ms  bigint   NOT NULL DEFAULT now_ms()
);
COMMENT ON TABLE engine_epoch IS
    'Single-row fencing token. Bumped under pg_advisory_xact_lock(EPOCH_LOCK) only if the revision is newer '
    'for the same cluster_id; every engine transaction re-reads it under the shared lock (operations.md §2).';

CREATE TABLE IF NOT EXISTS leader_terms (
    term_id            bigserial PRIMARY KEY,
    cluster_id         text   NOT NULL,
    epoch_revision     bigint NOT NULL,
    node               text   NOT NULL,
    started_at         bigint NOT NULL,          -- ms
    reached_leader_at  bigint,                   -- ms, set on LEADER entry
    ended_at           bigint,                   -- ms
    end_reason         text CHECK (end_reason IN ('switchover', 'abdicate', 'lost', 'init_failed')),
    UNIQUE (cluster_id, epoch_revision)
);
CREATE INDEX IF NOT EXISTS leader_terms_node_idx  ON leader_terms (node, started_at DESC);
CREATE INDEX IF NOT EXISTS leader_terms_ended_idx ON leader_terms (ended_at) WHERE ended_at IS NOT NULL;
COMMENT ON TABLE leader_terms IS
    'Anti-flap brake (operations.md §4): written by LEADER_INIT step 1, LEADER entry and DRAINING. '
    'Abnormal = lost, init_failed.';

CREATE TABLE IF NOT EXISTS cluster_state (
    id                         smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    safe_hold                  boolean NOT NULL DEFAULT false,
    safe_hold_since            bigint,
    safe_hold_reason           text,
    last_resume_at             bigint,           -- operator resume(); only terms ended after it count
    resumed_by                 text,
    risksweep_incident_id      text,             -- operations.md §3: at most one abdication per incident
    risksweep_abdicated_at     bigint,
    updated_at_ms              bigint NOT NULL DEFAULT now_ms()
);
INSERT INTO cluster_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
COMMENT ON TABLE cluster_state IS 'Durable SAFE_HOLD (mirrored to etcd) and per-incident abdication record.';

-- ---------------------------------------------------------------------------------------------
-- Entity current rows (tier-persistence.md §4) - one row per entity, version CAS in the engine
-- ---------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS orders_state (
    adapter_id            text    NOT NULL,
    broker_order_id       text    NOT NULL,
    client_order_id       text,                  -- nullable: not every broker row carries one
    account_id            text    NOT NULL,
    symbol                text    NOT NULL,
    side                  text    NOT NULL CHECK (side IN ('BUY', 'SELL')),
    status                text    NOT NULL CHECK (status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED',
                                                             'FILLED', 'CANCELLED', 'REJECTED')),
    entry_price           numeric,
    stop_loss             numeric,
    take_profit           numeric,
    ordered_units         numeric,
    remaining_units       numeric,
    instrument_unknown    boolean NOT NULL DEFAULT false,
    raw_qty               numeric,
    is_reduce_only        boolean NOT NULL DEFAULT false,
    linked_position_id    text,
    -- version (dto-contracts.md §2)
    broker_seq            bigint,
    broker_updated_at_ms  bigint  NOT NULL,
    observed_seq          bigint  NOT NULL,
    source                text    NOT NULL CHECK (source IN ('STREAM', 'REST')),
    version               char(56) NOT NULL,     -- src.dto.version_str
    seq_id                bigint  NOT NULL,
    checksum              char(64) NOT NULL,
    digest_hash           char(64) NOT NULL,
    committed_at_ms       bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (adapter_id, broker_order_id)
);
CREATE INDEX IF NOT EXISTS orders_state_account_idx   ON orders_state (adapter_id, account_id);
CREATE INDEX IF NOT EXISTS orders_state_client_idx    ON orders_state (client_order_id) WHERE client_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS orders_state_committed_idx ON orders_state (committed_at_ms);

CREATE TABLE IF NOT EXISTS positions_state (
    adapter_id                 text    NOT NULL,
    position_id                text    NOT NULL,
    account_id                 text    NOT NULL,
    symbol                     text    NOT NULL,
    side                       text    NOT NULL CHECK (side IN ('BUY', 'SELL')),
    units                      numeric,
    entry_price                numeric,
    status                     text    NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    instrument_unknown         boolean NOT NULL DEFAULT false,
    raw_qty                    numeric,
    stop_loss                  numeric,
    take_profit                numeric,
    opened_by_client_order_id  text,
    accrued_swap               numeric NOT NULL DEFAULT 0,
    commission                 numeric NOT NULL DEFAULT 0,
    broker_seq                 bigint,
    broker_updated_at_ms       bigint  NOT NULL,
    observed_seq               bigint  NOT NULL,
    source                     text    NOT NULL CHECK (source IN ('STREAM', 'REST')),
    version                    char(56) NOT NULL,
    seq_id                     bigint  NOT NULL,
    checksum                   char(64) NOT NULL,
    digest_hash                char(64) NOT NULL,
    committed_at_ms            bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (adapter_id, position_id)
);
CREATE INDEX IF NOT EXISTS positions_state_account_idx   ON positions_state (adapter_id, account_id);
CREATE INDEX IF NOT EXISTS positions_state_open_idx      ON positions_state (adapter_id, account_id) WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS positions_state_committed_idx ON positions_state (committed_at_ms);

CREATE TABLE IF NOT EXISTS account_state (
    adapter_id            text    NOT NULL,
    account_id            text    NOT NULL,
    currency              text    NOT NULL,
    balance               numeric NOT NULL,
    realised_pnl_today    numeric,
    broker_seq            bigint,
    broker_updated_at_ms  bigint  NOT NULL,
    observed_seq          bigint  NOT NULL,
    source                text    NOT NULL CHECK (source IN ('STREAM', 'REST')),
    version               char(56) NOT NULL,
    seq_id                bigint  NOT NULL,
    checksum              char(64) NOT NULL,
    digest_hash           char(64) NOT NULL,
    committed_at_ms       bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (adapter_id, account_id)
);
CREATE INDEX IF NOT EXISTS account_state_committed_idx ON account_state (committed_at_ms);

-- Versioned AccountDTO history; live fields sampled at a fixed interval (sampled = true).
CREATE TABLE IF NOT EXISTS account_snapshots (
    adapter_id            text    NOT NULL,
    account_id            text    NOT NULL,
    seq_id                bigint  NOT NULL,
    currency              text    NOT NULL,
    balance               numeric,
    realised_pnl_today    numeric,
    equity                numeric,
    margin_used           numeric,
    free_margin           numeric,
    unrealised_pnl        numeric,
    live_ts_ms            bigint,                -- timestamp of the sampled live fields
    sampled               boolean NOT NULL DEFAULT false,
    version               char(56),
    checksum              char(64),
    recorded_at_ms        bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (adapter_id, account_id, seq_id)
) PARTITION BY RANGE (seq_id);
CREATE TABLE IF NOT EXISTS account_snapshots_default PARTITION OF account_snapshots DEFAULT;

CREATE TABLE IF NOT EXISTS instruments (
    adapter_id              text    NOT NULL,
    symbol                  text    NOT NULL,
    base_ccy                text    NOT NULL,
    quote_ccy               text    NOT NULL,
    pnl_ccy                 text    NOT NULL,
    contract_size           numeric NOT NULL,
    unit_kind               text    NOT NULL CHECK (unit_kind IN ('UNITS', 'CENTI_UNITS', 'LOTS', 'CONTRACTS', 'STAKE')),
    point_size              numeric NOT NULL,
    min_qty                 numeric NOT NULL,
    swap_booked_to_balance  boolean NOT NULL,
    broker_symbol           text    NOT NULL DEFAULT '',
    version                 char(56) NOT NULL,
    seq_id                  bigint  NOT NULL,
    checksum                char(64) NOT NULL,
    digest_hash             char(64) NOT NULL,
    committed_at_ms         bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (adapter_id, symbol)
);

-- ---------------------------------------------------------------------------------------------
-- Ledger, start of day, marks
-- ---------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cash_flows (
    adapter_id       text    NOT NULL,
    transaction_id   text    NOT NULL,
    account_id       text    NOT NULL,
    kind             text    NOT NULL CHECK (kind IN ('DEPOSIT', 'WITHDRAWAL', 'TRADE_PNL', 'FINANCING',
                                                      'COMMISSION', 'ADJUSTMENT')),
    amount           numeric NOT NULL,
    currency         text    NOT NULL,
    broker_ts        bigint  NOT NULL,           -- ms
    seq_id           bigint  NOT NULL,
    checksum         char(64) NOT NULL,
    committed_at_ms  bigint  NOT NULL DEFAULT now_ms(),
    -- Unique on transaction_id, scoped by adapter so two brokers' numeric ids can never collide.
    PRIMARY KEY (adapter_id, transaction_id)
);
CREATE INDEX IF NOT EXISTS cash_flows_account_ts_idx ON cash_flows (adapter_id, account_id, broker_ts);

CREATE TABLE IF NOT EXISTS sod_marks (
    adapter_id                     text    NOT NULL,
    account_id                     text    NOT NULL,
    trading_day                    date    NOT NULL,  -- the day that starts at the 17:00 America/New_York roll
    roll_at_ms                     bigint  NOT NULL,
    sod_equity                     numeric,
    balance                        numeric,
    position_marks                 jsonb   NOT NULL DEFAULT '[]'::jsonb,
    clean_mids                     jsonb   NOT NULL DEFAULT '{}'::jsonb,  -- {symbol: {mid, ts}} taken before the roll
    unexplained_increase_at_roll   numeric NOT NULL DEFAULT 0,
    partial_session                boolean NOT NULL DEFAULT false,
    partial_reasons                text[]  NOT NULL DEFAULT '{}',
    transfers_unverified           boolean NOT NULL DEFAULT false,
    pnl_unverified                 boolean NOT NULL DEFAULT false,
    written_by                     text    NOT NULL CHECK (written_by IN ('sweep', 'leader_init', 'startup')),
    sweep_id                       bigint,
    written_at_ms                  bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (adapter_id, account_id, trading_day)
);

CREATE TABLE IF NOT EXISTS last_clean_marks (
    adapter_id     text    NOT NULL,
    symbol         text    NOT NULL,
    bid            numeric NOT NULL,
    ask            numeric NOT NULL,
    spread_pips    numeric,
    ts             bigint  NOT NULL,             -- ms, the quote's own timestamp
    written_at_ms  bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (adapter_id, symbol)
);
COMMENT ON TABLE last_clean_marks IS
    'Upserted at most every 10 s per (adapter, symbol) and forced at each open/close/roll, epoch-checked '
    '(operations.md §11). Seeds price:/clean_mid: in SM8 step 3.';

-- ---------------------------------------------------------------------------------------------
-- Oversee state (oversee-rules.md §2-§10)
-- ---------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stop_watch (
    entity_type                       text   NOT NULL CHECK (entity_type IN ('ORDER', 'POSITION')),
    entity_id                         text   NOT NULL,   -- adapter_id:broker_order_id | adapter_id:position_id
    adapter_id                        text   NOT NULL,
    account_id                        text   NOT NULL,
    effective_stop_missing_since_ms   bigint NOT NULL,
    last_sweep_id                     bigint,
    PRIMARY KEY (entity_type, entity_id)
);

CREATE TABLE IF NOT EXISTS stream_health (
    adapter_id        text   NOT NULL,
    stream            text   NOT NULL CHECK (stream IN ('prices', 'orders', 'account')),
    conn_lost_at      bigint,                    -- ms; NULL = SUBSCRIBED and reconciled in this term
    reconciled_at     bigint,                    -- ms
    reconciled_epoch  bigint,                    -- engine_epoch.revision of the clearing term
    updated_at_ms     bigint NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (adapter_id, stream)
);
COMMENT ON TABLE stream_health IS
    'Account fence (oversee-rules.md §2). LEADER_INIT step 1 sets conn_lost_at = COALESCE(conn_lost_at, now) '
    'for every row; the orchestrator sets it on leaving SUBSCRIBED or a gap and clears it after reconcile.';

CREATE TABLE IF NOT EXISTS halt_breaches (
    breach_id              bigserial PRIMARY KEY,
    rule_id                text    NOT NULL,
    subject_type           text    NOT NULL CHECK (subject_type IN ('ACCOUNT', 'ACCOUNT_DAY', 'ORDER', 'POSITION')),
    subject_id             text    NOT NULL,
    adapter_id             text    NOT NULL,
    account_id             text    NOT NULL,
    level                  text    CHECK (level IN ('FLAG', 'SOFT', 'HARD')),   -- NULL while only pending
    pending_level          text    CHECK (pending_level IN ('FLAG', 'SOFT', 'HARD')),
    cap                    text    NOT NULL DEFAULT 'NONE' CHECK (cap IN ('NONE', 'SOFT')),
    reason                 text    NOT NULL DEFAULT '',
    activation_id          bigint,               -- state episode id (sweep_id of activation)
    pending_since          bigint,               -- ms
    active_since           bigint,               -- ms
    release_pending_since  bigint,               -- ms
    unknown_since          bigint,               -- ms
    uncapped_ms            bigint  NOT NULL DEFAULT 0,   -- accumulated escalation clock
    last_pass_at_ms        bigint,               -- for min(now - previous pass, 2 x interval)
    below_soft_since       bigint,               -- ms
    resolved_at            bigint,               -- ms
    resolved_by            text    CHECK (resolved_by IN ('sweep', 'day_end', 'entity_end', 'reload', 'operator')),
    last_sweep_id          bigint,               -- compare-and-set
    clear_generation       bigint  NOT NULL DEFAULT 0,   -- compare-and-set, bumped by operator clear()
    created_at_ms          bigint  NOT NULL DEFAULT now_ms(),
    updated_at_ms          bigint  NOT NULL DEFAULT now_ms(),
    UNIQUE (rule_id, subject_type, subject_id)
);
CREATE INDEX IF NOT EXISTS halt_breaches_account_idx ON halt_breaches (adapter_id, account_id);
CREATE INDEX IF NOT EXISTS halt_breaches_active_idx  ON halt_breaches (level) WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS breach_acks (
    ack_id      bigserial PRIMARY KEY,
    breach_id   bigint NOT NULL REFERENCES halt_breaches (breach_id),
    operator    text   NOT NULL,
    why         text   NOT NULL,
    level       text   NOT NULL,                 -- the acked (level, cap, reason); a change re-arms re-pages
    cap         text   NOT NULL,
    reason      text   NOT NULL,
    acked_at_ms bigint NOT NULL DEFAULT now_ms()
);
CREATE INDEX IF NOT EXISTS breach_acks_breach_idx ON breach_acks (breach_id, acked_at_ms DESC);

CREATE TABLE IF NOT EXISTS sweep_quarantine (
    adapter_id          text   NOT NULL,
    account_id          text   NOT NULL,
    failing_since_ms    bigint NOT NULL,
    quarantined_at_ms   bigint NOT NULL,
    last_retry_at_ms    bigint,
    next_retry_at_ms    bigint NOT NULL,         -- half-open retry every 10 min
    reason              text   NOT NULL,
    released_at_ms      bigint,
    released_by         text,                    -- 'pass' or operator id (release_account)
    PRIMARY KEY (adapter_id, account_id)
);

CREATE TABLE IF NOT EXISTS halt_events (
    id               bigserial PRIMARY KEY,
    level            text    NOT NULL CHECK (level IN ('NONE', 'SOFT', 'HARD')),
    effective_level  text    NOT NULL CHECK (effective_level IN ('NONE', 'SOFT', 'HARD')),
    hard_latch       boolean NOT NULL,
    rule_id          text,
    reason           text    NOT NULL,
    "by"             text    NOT NULL,           -- 'system' | operator id (quoted: BY is a keyword)
    at_ms            bigint  NOT NULL DEFAULT now_ms(),
    reviewed_event_id bigint,                    -- operator clear(): the event it reviewed
    forced           boolean NOT NULL DEFAULT false
);
COMMENT ON TABLE halt_events IS
    'Append-only truth for the halt level; inserted only under pg_advisory_xact_lock(HALT_LOCK) by trip(), '
    'system clear() and operator clear() (oversee-rules.md §8).';

CREATE TABLE IF NOT EXISTS oversee_events (
    event_id      bigserial,
    sweep_id      bigint  NOT NULL,              -- HLC, partition key
    rule_id       text    NOT NULL,
    subject_type  text    NOT NULL,
    subject_id    text    NOT NULL,
    transition    text    NOT NULL CHECK (transition IN ('activated', 'raised', 'lowered', 'resolved',
                                                         'capped', 'uncapped')),
    adapter_id    text    NOT NULL,
    account_id    text    NOT NULL,
    level         text,
    cap           text,
    reason        text,
    at_ms         bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (event_id, sweep_id),
    UNIQUE (rule_id, subject_type, subject_id, transition, sweep_id)
) PARTITION BY RANGE (sweep_id);
CREATE TABLE IF NOT EXISTS oversee_events_default PARTITION OF oversee_events DEFAULT;

CREATE TABLE IF NOT EXISTS notification_outbox (
    outbox_id           bigserial,
    created_at_ms       bigint  NOT NULL DEFAULT now_ms(),   -- partition key
    dedupe_key          text,                    -- (rule_id, subject_type, subject_id, activation id, level)
    bypass_dedupe       boolean NOT NULL DEFAULT false,      -- only the level-raising trip() alert
    severity            text    NOT NULL CHECK (severity IN ('INFO', 'FLAG', 'SOFT', 'HARD', 'CRITICAL')),
    channels            text[]  NOT NULL DEFAULT '{email}',
    subject             text    NOT NULL,
    body                text    NOT NULL,
    source_event        text,                    -- e.g. 'oversee_events:<id>' / 'halt_events:<id>'
    attempts            int     NOT NULL DEFAULT 0,
    next_attempt_at_ms  bigint  NOT NULL DEFAULT now_ms(),
    delivered_at_ms     bigint,
    last_error          text,
    PRIMARY KEY (outbox_id, created_at_ms)
) PARTITION BY RANGE (created_at_ms);
CREATE TABLE IF NOT EXISTS notification_outbox_default PARTITION OF notification_outbox DEFAULT;

CREATE TABLE IF NOT EXISTS notification_dedupe (
    dedupe_key       text   PRIMARY KEY,
    first_sent_at_ms bigint NOT NULL,
    last_sent_at_ms  bigint NOT NULL,
    sent_count       int    NOT NULL DEFAULT 1
);
COMMENT ON TABLE notification_dedupe IS 'Alert dedupe state in PostgreSQL so a new leader does not re-send.';

-- ---------------------------------------------------------------------------------------------
-- Event log, outbox, DLQ (tier-persistence.md §4-§5)
-- ---------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS event_log (
    log_id          bigserial,
    seq_id          bigint  NOT NULL,            -- partition key
    entity_type     text    NOT NULL CHECK (entity_type IN ('ORDER', 'POSITION', 'ACCOUNT', 'CASH_FLOW', 'INSTRUMENT')),
    entity_key      text    NOT NULL,
    adapter_id      text    NOT NULL,
    account_id      text,
    action          text    NOT NULL CHECK (action IN ('APPLIED', 'DROPPED_STALE', 'CASH_FLOW')),
    version         char(56),
    checksum        char(64),
    payload         jsonb   NOT NULL,
    epoch_revision  bigint  NOT NULL,
    logged_at_ms    bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (seq_id, log_id)
) PARTITION BY RANGE (seq_id);
CREATE TABLE IF NOT EXISTS event_log_default PARTITION OF event_log DEFAULT;
COMMENT ON TABLE event_log IS 'Append-only; records applied versions and dropped lower versions (never parked).';

-- Append-only guard: event_log rows are never updated or deleted by the engine (partitions are detached
-- by the lifecycle job instead).
CREATE OR REPLACE FUNCTION xtnl_event_log_append_only() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'event_log is append-only';
END
$$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'event_log_append_only') THEN
        CREATE TRIGGER event_log_append_only BEFORE UPDATE OR DELETE ON event_log
            FOR EACH ROW EXECUTE FUNCTION xtnl_event_log_append_only();
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS outbox_events (
    outbox_id      bigserial PRIMARY KEY,
    seq_id         bigint  NOT NULL,
    topic          text    NOT NULL,
    entity_type    text    NOT NULL,
    entity_key     text    NOT NULL,
    payload        jsonb   NOT NULL,
    created_at_ms  bigint  NOT NULL DEFAULT now_ms()
);
COMMENT ON TABLE outbox_events IS 'Transactional outbox; rows deleted after relay (operations.md §11).';

-- Active DLQ (not partitioned: payload_hash must be globally unique). History is archived by the
-- lifecycle job into seq-range partitions elsewhere.
CREATE TABLE IF NOT EXISTS events_dlq (
    dlq_id              bigserial PRIMARY KEY,
    origin_dlq_id       bigint REFERENCES events_dlq (dlq_id),
    event_type          text    NOT NULL CHECK (event_type IN ('ORDER', 'POSITION', 'ACCOUNT', 'CASH_FLOW')),
    entity_id           text    NOT NULL,
    adapter_id          text    NOT NULL,
    raw_payload         text    NOT NULL,
    payload_hash        char(64) NOT NULL UNIQUE,
    error_reason        text    NOT NULL,
    retry_count         int     NOT NULL DEFAULT 0,
    status              text    NOT NULL DEFAULT 'QUEUED'
                                CHECK (status IN ('QUEUED', 'LEASED', 'BACKOFF', 'RESOLVED', 'QUARANTINED')),
    next_retry_at_ms    bigint  NOT NULL DEFAULT now_ms(),
    lease_owner         text,
    lease_until_ms      bigint,
    parked_at_ms        bigint  NOT NULL DEFAULT now_ms(),
    updated_at_ms       bigint  NOT NULL DEFAULT now_ms()
);
CREATE INDEX IF NOT EXISTS events_dlq_ready_idx   ON events_dlq (status, next_retry_at_ms);
CREATE INDEX IF NOT EXISTS events_dlq_entity_idx  ON events_dlq (entity_id, error_reason);
CREATE INDEX IF NOT EXISTS events_dlq_origin_idx  ON events_dlq (origin_dlq_id) WHERE origin_dlq_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS events_quarantine (
    quarantine_id      bigserial PRIMARY KEY,
    source             text    NOT NULL CHECK (source IN ('dlq', 'feed')),
    dlq_id             bigint,
    feed_entry_id      text,                     -- oversee:feed entry delivered 5 times
    event_type         text,
    entity_id          text,
    adapter_id         text,
    account_id         text,
    raw_payload        text,
    error_reason       text    NOT NULL,
    retry_count        int     NOT NULL DEFAULT 0,
    quarantined_at_ms  bigint  NOT NULL DEFAULT now_ms(),
    alerted            boolean NOT NULL DEFAULT false
);

-- ---------------------------------------------------------------------------------------------
-- Credentials, Phase 2 contract, versioned config
-- ---------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS adapter_auth_state (
    adapter_id             text   NOT NULL,
    credential_generation  text   NOT NULL,      -- Secrets Manager version id or cTrader token_generation
    state                  text   NOT NULL CHECK (state IN ('AUTH_FAILED', 'REARMED')),
    attempts               int    NOT NULL DEFAULT 0,
    detail                 text,
    failed_at_ms           bigint NOT NULL DEFAULT now_ms(),
    rearmed_by             text,
    rearmed_at_ms          bigint,
    PRIMARY KEY (adapter_id, credential_generation)
);

-- cTrader token pair is state (operations.md §13): KMS-encrypted, saved before use, CAS on token_generation
-- (NOT epoch-gated, like the spill drain).
CREATE TABLE IF NOT EXISTS ctrader_tokens (
    adapter_id                    text   PRIMARY KEY,
    token_generation              bigint NOT NULL DEFAULT 0,   -- monotonic, bumped on every save
    access_token_enc              bytea,
    refresh_token_enc             bytea,
    kms_key_id                    text,
    expires_at_ms                 bigint,
    refresh_in_flight_generation  bigint,        -- 'refresh in flight' for generation N
    refresh_in_flight_epoch       bigint,
    refresh_started_at_ms         bigint,
    saved_at_ms                   bigint NOT NULL DEFAULT now_ms()
);

-- Phase 2 only: no Phase 1 code writes this table (operations.md §5).
CREATE TABLE IF NOT EXISTS outbound_intent (
    client_order_id  text   PRIMARY KEY,         -- deterministic
    cluster_id       text   NOT NULL,
    epoch_revision   bigint NOT NULL,
    adapter_id       text   NOT NULL,
    account_id       text   NOT NULL,
    action           text   NOT NULL,
    state            text   NOT NULL CHECK (state IN ('SENDING', 'ACKED', 'UNKNOWN', 'RESOLVED', 'ABORTED')),
    payload          jsonb  NOT NULL,
    created_at_ms    bigint NOT NULL DEFAULT now_ms(),
    resolved_at_ms   bigint,
    resolved_by      text
);

CREATE TABLE IF NOT EXISTS rulesets (
    ruleset_version  bigint  PRIMARY KEY,
    config           jsonb   NOT NULL,
    active           boolean NOT NULL DEFAULT false,
    created_by       text    NOT NULL,
    reason           text    NOT NULL,
    created_at_ms    bigint  NOT NULL DEFAULT now_ms()
);
CREATE UNIQUE INDEX IF NOT EXISTS rulesets_one_active ON rulesets ((true)) WHERE active;

CREATE TABLE IF NOT EXISTS market_sessions (
    calendar_version  bigint NOT NULL,
    adapter_id        text   NOT NULL,
    symbol            text   NOT NULL,
    timezone          text   NOT NULL,
    schedule          jsonb  NOT NULL,           -- weekly sessions, breaks, rollover
    holidays          jsonb  NOT NULL DEFAULT '[]'::jsonb,
    source            text   NOT NULL,           -- 'ctrader' | 'stonex' | 'reviewed_config'
    loaded_at_ms      bigint NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (calendar_version, adapter_id, symbol)
);

-- ---------------------------------------------------------------------------------------------
-- Initial partitions: current month and the next two (operations.md §11)
-- ---------------------------------------------------------------------------------------------

DO $$
DECLARE
    m int;
    base date := date_trunc('month', now() AT TIME ZONE 'UTC')::date;
BEGIN
    FOR m IN 0..2 LOOP
        PERFORM xtnl_ensure_month_partition('event_log',           (base + make_interval(months => m))::date, 20);
        PERFORM xtnl_ensure_month_partition('account_snapshots',   (base + make_interval(months => m))::date, 20);
        PERFORM xtnl_ensure_month_partition('oversee_events',      (base + make_interval(months => m))::date, 20);
        PERFORM xtnl_ensure_month_partition('notification_outbox', (base + make_interval(months => m))::date, 0);
    END LOOP;
END
$$;
