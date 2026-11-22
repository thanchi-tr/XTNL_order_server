-- 0021_liveview_role.sql : the read-only `xtnl_liveview` role and its privacy-safe `lv_` views for the local live
-- view (docs/design/live-view.md s3). The live view never sends orders and has no write path of any kind.
--
-- * Role `xtnl_liveview`: NOLOGIN as created here. No credential is ever written into a migration: the owner
--   enables login with deploy/local/new-liveview-credentials.ps1 (docs/runbooks/live-view.md).
-- * SELECT only, and only on the lv_ views below (src/liveview/sql.py ALL_VIEWS; a test keeps the two lists equal).
--   No grant on any table, no write grant, no default privileges. The views are owned by the schema owner, so the
--   role needs nothing on the base tables. Every view is a security_barrier view.
-- * The views expose ids, times, codes, counts and order levels only: never a size, a money figure, a market-data
--   value or free text. Free-text columns are left out or passed through a code guard (NULL when not a code).
-- * The lv_ views pin their base columns: a later migration that changes a column's type or drops a column must
--   drop and recreate the affected views in the same migration (operations.md s12).
-- * The role block (through the last ALTER ROLE line) comes first, so the carry-over runbook can pipe it alone
--   before pg_restore (docs/runbooks/single-pc-autostart.md).
-- Idempotent. Creating a role needs CREATEROLE: run the migration with the schema-owner DSN. Applied by
-- src/db/migrate.py with lock_timeout 2 s and retries; the per-partition log_id indexes are built by the post step.

SET client_min_messages = warning;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_liveview') THEN
        CREATE ROLE xtnl_liveview NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION
            NOBYPASSRLS CONNECTION LIMIT 3;
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO xtnl_liveview', current_database());
END
$$;

ALTER ROLE xtnl_liveview SET default_transaction_read_only = on;
ALTER ROLE xtnl_liveview SET statement_timeout = '5s';
ALTER ROLE xtnl_liveview SET idle_in_transaction_session_timeout = '10s';

-- The live view's id cursor on event_log (s4.3): the parent index only; the post step builds and attaches the
-- per-partition indexes concurrently (PartitionIndex(21, ...) in src/db/migrate.py), like 0017's broker-time index.
CREATE INDEX IF NOT EXISTS event_log_log_id_idx ON ONLY event_log (log_id);

GRANT USAGE ON SCHEMA public TO xtnl_liveview;

-- Entity rows: all five types. CASH_FLOW and INSTRUMENT rows are id placeholders (no key, account, time or facts).
-- facts holds only the named payload keys, with the levels renamed; a filled or kind-less closing order has no
-- entry level (owner decision 8.1-A: never an execution price), and neither has any FILLED order with no stop /
-- limit kind, whatever its reduce-only flag says (a closing trade read without its closing evidence; an entry fill
-- is still shown through its position's entry level).
CREATE OR REPLACE VIEW lv_entity_events AS
    SELECT log_id, seq_id, entity_type, x.entity_key AS entity_key, adapter_id, x.account_id AS account_id, action,
        logged_at_ms, e.origin AS version_path, x.broker_ms AS broker_ms, x.facts AS facts
    FROM event_log e
    CROSS JOIN LATERAL (SELECT
        CASE WHEN e.entity_type IN ('CASH_FLOW', 'INSTRUMENT') THEN NULL ELSE e.entity_key END AS entity_key,
        CASE WHEN e.entity_type IN ('CASH_FLOW', 'INSTRUMENT') THEN NULL ELSE e.account_id END AS account_id,
        CASE WHEN e.entity_type IN ('CASH_FLOW', 'INSTRUMENT') THEN NULL ELSE e.broker_time_ms END AS broker_ms,
        CASE e.entity_type
            WHEN 'ORDER' THEN jsonb_strip_nulls(jsonb_build_object(
                'broker_order_id', e.payload->'broker_order_id',
                'client_order_id', e.payload->'client_order_id',
                'symbol', e.payload->'symbol',
                'side', e.payload->'side',
                'status', e.payload->'status',
                'is_reduce_only', e.payload->'is_reduce_only',
                'linked_position_id', e.payload->'linked_position_id',
                'leg_kind', e.payload->'order_kind',
                'instrument_unknown', e.payload->'instrument_unknown',
                'source', e.payload->'source',
                'broker_updated_at_ms', e.payload->'broker_updated_at_ms',
                'entry_level', CASE WHEN (coalesce((e.payload->>'is_reduce_only')::boolean, false)
                                          AND (e.payload->>'status' = 'FILLED' OR e.payload->>'order_kind' IS NULL))
                                         OR (e.payload->>'status' = 'FILLED' AND e.payload->>'order_kind' IS NULL)
                                    THEN NULL ELSE e.payload->'entry_price' END,
                'stop_level', e.payload->'stop_loss',
                'target_level', e.payload->'take_profit'))
            WHEN 'POSITION' THEN jsonb_strip_nulls(jsonb_build_object(
                'position_id', e.payload->'position_id',
                'symbol', e.payload->'symbol',
                'side', e.payload->'side',
                'status', e.payload->'status',
                'entry_level', e.payload->'entry_price',
                'stop_level', e.payload->'stop_loss',
                'target_level', e.payload->'take_profit',
                'opened_by_client_order_id', e.payload->'opened_by_client_order_id',
                'instrument_unknown', e.payload->'instrument_unknown',
                'source', e.payload->'source',
                'broker_updated_at_ms', e.payload->'broker_updated_at_ms'))
            WHEN 'ACCOUNT' THEN jsonb_strip_nulls(jsonb_build_object(
                'account_id', e.payload->'account_id',
                'source', e.payload->'source',
                'broker_updated_at_ms', e.payload->'broker_updated_at_ms'))
        END AS facts) x;

CREATE OR REPLACE VIEW lv_oversee_events AS
    SELECT event_id, sweep_id, rule_id, subject_type, subject_id, transition, adapter_id, account_id, level, cap,
        x.reason_code AS reason_code, at_ms, activation_id
    FROM oversee_events o
    CROSS JOIN LATERAL (SELECT
        CASE WHEN o.reason ~ '^[A-Za-z0-9_():,-]{1,80}$' AND o.reason !~ '[0-9][.,][0-9]' THEN o.reason END
            AS reason_code) x;

-- Operator reasons are free text and never leave the table; the operator label passes the OPERATOR guard.
CREATE OR REPLACE VIEW lv_halt_events AS
    SELECT id, level, effective_level, hard_latch, rule_id, x.reason_code AS reason_code, x.by_whom AS by_whom,
        at_ms, reviewed_event_id, forced
    FROM halt_events h
    CROSS JOIN LATERAL (SELECT
        CASE WHEN h."by" = 'system' AND h.reason ~ '^[A-Za-z0-9_():,-]{1,80}$' AND h.reason !~ '[0-9][.,][0-9]'
             THEN h.reason END AS reason_code,
        CASE WHEN h."by" ~ '^[A-Za-z0-9._@-]{2,64}$' AND h."by" !~ '[0-9][.,][0-9]' THEN h."by"
             ELSE 'operator' END AS by_whom) x;

-- Alerts: subject, body, channels, dedupe_key and last_error are only compared, never exposed. ARM rows are id
-- placeholders. The transition subject passes only through the anchored tm pattern (four fixed-class captures).
CREATE OR REPLACE VIEW lv_alerts AS
    SELECT outbox_id, created_at_ms, x.severity AS severity, kind, x.info_class AS info_class,
        x.account_id AS account_id, x.delivery AS delivery, x.escalated AS escalated, x.attempts AS attempts,
        x.sent_at_ms AS sent_at_ms, x.delivered_at_ms AS delivered_at_ms, x.ep_rule_id AS ep_rule_id,
        x.ep_subject_type AS ep_subject_type, x.ep_subject_id AS ep_subject_id,
        x.ep_activation_id AS ep_activation_id, x.ep_level AS ep_level, x.tr_transition AS tr_transition,
        x.tr_rule_id AS tr_rule_id, x.tr_subject_type AS tr_subject_type, x.tr_subject_id AS tr_subject_id
    FROM notification_outbox n
    CROSS JOIN LATERAL (SELECT
        CASE WHEN n.kind = 'info' THEN regexp_match(n.subject,
            '^(lowered|resolved|capped|uncapped) ([a-z0-9_]{1,64}) (account_day|account|order|position):([A-Za-z0-9:_@/-]{1,64})$')
        END AS tm,
        CASE WHEN jsonb_typeof(n.episode) = 'array' THEN n.episode END AS ep) t
    CROSS JOIN LATERAL (SELECT
        CASE WHEN n.kind = 'arm' THEN NULL ELSE n.severity END AS severity,
        CASE WHEN n.kind <> 'info' THEN NULL
             WHEN n.subject = 'HALT cleared' THEN 'halt_clear'
             WHEN n.subject LIKE 'HALT cleared by %' THEN 'halt_clear_operator'
             WHEN t.tm IS NOT NULL THEN 'transition'
             ELSE 'notice' END AS info_class,
        CASE WHEN n.kind = 'arm' THEN NULL ELSE n.account_id END AS account_id,
        CASE WHEN n.kind = 'arm' THEN NULL
             WHEN n.last_error = 'deduplicated' THEN 'suppressed'
             WHEN n.delivered THEN 'delivered'
             WHEN n.sent_at_ms IS NOT NULL THEN 'undeliverable'
             WHEN n.attempts > 0 THEN 'retrying'
             ELSE 'queued' END AS delivery,
        CASE WHEN n.kind = 'arm' THEN NULL ELSE n.channels @> ARRAY['+escalated'] END AS escalated,
        CASE WHEN n.kind = 'arm' THEN NULL ELSE n.attempts END AS attempts,
        CASE WHEN n.kind = 'arm' THEN NULL ELSE n.sent_at_ms END AS sent_at_ms,
        CASE WHEN n.kind = 'arm' THEN NULL ELSE n.delivered_at_ms END AS delivered_at_ms,
        CASE WHEN n.kind <> 'arm' AND t.ep->>0 ~ '^[a-z0-9_]{1,64}$' THEN t.ep->>0 END AS ep_rule_id,
        CASE WHEN n.kind <> 'arm' AND t.ep->>1 ~ '^(account_day|account|order|position)$'
             THEN upper(t.ep->>1) END AS ep_subject_type,
        CASE WHEN n.kind <> 'arm' AND t.ep->>2 ~ '^[A-Za-z0-9:_@/-]{1,64}$' THEN t.ep->>2 END AS ep_subject_id,
        CASE WHEN n.kind <> 'arm' AND t.ep->>3 ~ '^[0-9]{1,18}$' THEN (t.ep->>3)::bigint END AS ep_activation_id,
        CASE WHEN n.kind <> 'arm' AND t.ep->>4 ~ '^[0-9]$' THEN (t.ep->>4)::int END AS ep_level,
        t.tm[1] AS tr_transition,
        t.tm[2] AS tr_rule_id,
        upper(t.tm[3]) AS tr_subject_type,
        t.tm[4] AS tr_subject_id) x;

-- No reason, no detail, no db_user, os_user or client address.
CREATE OR REPLACE VIEW lv_operator_actions AS
    SELECT audit_id, at_ms, x.command AS command, x.target_id AS target_id, x.operator AS operator,
        x.approver AS approver, outcome, epoch
    FROM operator_audit a
    CROSS JOIN LATERAL (SELECT
        CASE WHEN a.command ~ '^[a-z_]+(\.[a-z_]+){0,3}$' THEN a.command END AS command,
        CASE WHEN a.target ~ '^[A-Za-z0-9:_@/-]{1,64}$' THEN a.target END AS target_id,
        CASE WHEN a.operator ~ '^[A-Za-z0-9._@-]{2,64}$' AND a.operator !~ '[0-9][.,][0-9]' THEN a.operator END
            AS operator,
        CASE WHEN a.approver ~ '^[A-Za-z0-9._@-]{2,64}$' AND a.approver !~ '[0-9][.,][0-9]' THEN a.approver END
            AS approver) x;

-- No raw payload, no hash, no lease owner.
CREATE OR REPLACE VIEW lv_dlq AS
    SELECT dlq_id, origin_dlq_id, event_type, x.entity_id AS entity_id, adapter_id, x.dlq_reason AS dlq_reason,
        retry_count, status, parked_at_ms, updated_at_ms
    FROM events_dlq d
    CROSS JOIN LATERAL (SELECT
        CASE WHEN d.entity_id ~ '^[A-Za-z0-9:_@/-]{1,64}$' THEN d.entity_id END AS entity_id,
        CASE WHEN d.error_reason ~ '^[A-Za-z0-9_():,-]{1,80}$' AND d.error_reason !~ '[0-9][.,][0-9]'
             THEN d.error_reason END AS dlq_reason) x;

-- No notes.
CREATE OR REPLACE VIEW lv_catchups AS
    SELECT id, run_id, epoch, adapter_id, reason, gap_start_ms, gap_end_ms, covered_from_ms, offline_from_ms,
        started_at_ms, finished_at_ms, orders_seen, orders_new, orders_updated, positions_seen, positions_new,
        positions_closed, cash_flows_new, anomalies, complete
    FROM catchup_runs;

CREATE OR REPLACE VIEW lv_engine_runs AS
    SELECT run_id, node_id, started_at_ms, last_seen_ms, stopped_at_ms, x.end_code AS end_code, suspended_gaps
    FROM engine_runs r
    CROSS JOIN LATERAL (SELECT
        CASE WHEN r.end_reason ~ '^[A-Za-z0-9_():,-]{1,80}$' AND r.end_reason !~ '[0-9][.,][0-9]'
             THEN r.end_reason END AS end_code) x;

CREATE OR REPLACE VIEW lv_leader_terms AS
    SELECT term_id, cluster_id, epoch_revision, node, started_at, reached_leader_at, ended_at, end_reason
    FROM leader_terms;

CREATE OR REPLACE VIEW lv_stream_health AS
    SELECT adapter_id, stream, conn_lost_at, reconciled_at, reconciled_epoch, updated_at_ms
    FROM stream_health;

-- The SAFE_HOLD phrase (src/db/leader.py) is mapped to a code and a count; the phrase itself is never exposed.
CREATE OR REPLACE VIEW lv_cluster_state AS
    SELECT safe_hold, safe_hold_since, x.hold_code AS hold_code, x.hold_count AS hold_count, last_resume_at,
        x.resumed_by AS resumed_by, updated_at_ms
    FROM cluster_state c
    CROSS JOIN LATERAL (SELECT
        CASE WHEN c.safe_hold_reason = 'safe_hold already set' THEN 'already_set'
             WHEN c.safe_hold_reason ~ '^[0-9]{1,6} abnormal term ends within 15 min$' THEN 'abnormal_term_ends'
             WHEN c.safe_hold_reason ~ '^[0-9]{1,6} epoch changes within 30 min$' THEN 'epoch_changes'
        END AS hold_code,
        CASE WHEN c.safe_hold_reason ~ '^[0-9]{1,6} (abnormal term ends within 15|epoch changes within 30) min$'
             THEN ((regexp_match(c.safe_hold_reason, '^([0-9]{1,6}) '))[1])::int END AS hold_count,
        CASE WHEN c.resumed_by = 'system:single-pc-auto-resume' THEN 'auto-resume'
             WHEN c.resumed_by ~ '^[A-Za-z0-9._@-]{2,64}$' AND c.resumed_by !~ '[0-9][.,][0-9]' THEN c.resumed_by
        END AS resumed_by) x;

CREATE OR REPLACE VIEW lv_engine_epoch AS
    SELECT cluster_id, revision, epoch, node, bumped_at_ms
    FROM engine_epoch;

-- The free-text reason is not exposed; acked is the src/oversee/notify.py is_acknowledged predicate. No ack why.
CREATE OR REPLACE VIEW lv_breaches AS
    SELECT breach_id, rule_id, subject_type, subject_id, adapter_id, account_id, level, pending_level, cap,
        x.reason_code AS reason_code, activation_id, pending_since, active_since, release_pending_since,
        unknown_since, uncapped_ms, last_pass_at_ms, below_soft_since, resolved_at, resolved_by, updated_at_ms,
        x.acked AS acked
    FROM halt_breaches b
    CROSS JOIN LATERAL (SELECT
        CASE WHEN b.reason ~ '^[A-Za-z0-9_():,-]{1,80}$' AND b.reason !~ '[0-9][.,][0-9]' THEN b.reason END
            AS reason_code,
        EXISTS (SELECT 1 FROM breach_acks a WHERE a.breach_id = b.breach_id AND a.level = b.level
                AND a.cap = b.cap AND a.reason = b.reason
                AND (b.active_since IS NULL OR a.acked_at_ms >= b.active_since)) AS acked) x;

-- Working orders only; no units. The closing-order rule as in lv_entity_events.facts.
CREATE OR REPLACE VIEW lv_open_orders AS
    SELECT adapter_id, broker_order_id, client_order_id, account_id, symbol, side, status,
        x.entry_level AS entry_level, o.stop_loss AS stop_level, o.take_profit AS target_level, is_reduce_only,
        linked_position_id, o.order_kind AS leg_kind, broker_updated_at_ms, committed_at_ms
    FROM orders_state o
    CROSS JOIN LATERAL (SELECT
        CASE WHEN o.is_reduce_only AND (o.status = 'FILLED' OR o.order_kind IS NULL) THEN NULL
             ELSE o.entry_price END AS entry_level) x
    WHERE o.status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED');

-- Open positions only; no units, swap or commission.
CREATE OR REPLACE VIEW lv_open_positions AS
    SELECT adapter_id, position_id, account_id, symbol, side, p.entry_price AS entry_level,
        p.stop_loss AS stop_level, p.take_profit AS target_level, opened_by_client_order_id, broker_updated_at_ms,
        committed_at_ms
    FROM positions_state p
    WHERE p.status = 'OPEN';

CREATE OR REPLACE VIEW lv_ruleset_timing AS
    SELECT ruleset_version, x.escalate_after_ms AS escalate_after_ms, x.cool_down_ms AS cool_down_ms,
        x.unknown_grace_ms AS unknown_grace_ms
    FROM rulesets r
    CROSS JOIN LATERAL (SELECT
        CASE WHEN r.config #>> '{sweep,escalate_after_ms}' ~ '^[0-9]{1,15}$'
             THEN (r.config #>> '{sweep,escalate_after_ms}')::bigint END AS escalate_after_ms,
        CASE WHEN r.config #>> '{sweep,cool_down_ms}' ~ '^[0-9]{1,15}$'
             THEN (r.config #>> '{sweep,cool_down_ms}')::bigint END AS cool_down_ms,
        CASE WHEN r.config #>> '{sweep,unknown_grace_ms}' ~ '^[0-9]{1,15}$'
             THEN (r.config #>> '{sweep,unknown_grace_ms}')::bigint END AS unknown_grace_ms) x
    WHERE r.active;

-- No reason, no released_by.
CREATE OR REPLACE VIEW lv_sweep_quarantine AS
    SELECT adapter_id, account_id, failing_since_ms, quarantined_at_ms, next_retry_at_ms, released_at_ms
    FROM sweep_quarantine;

-- The latest row per adapter only; no detail, no credential generation.
CREATE OR REPLACE VIEW lv_auth_state AS
    SELECT adapter_id, state, failed_at_ms, rearmed_at_ms
    FROM adapter_auth_state a
    WHERE NOT EXISTS (SELECT 1 FROM adapter_auth_state n WHERE n.adapter_id = a.adapter_id
                      AND (n.failed_at_ms, n.credential_generation) > (a.failed_at_ms, a.credential_generation));

ALTER VIEW lv_entity_events SET (security_barrier = true);
ALTER VIEW lv_oversee_events SET (security_barrier = true);
ALTER VIEW lv_halt_events SET (security_barrier = true);
ALTER VIEW lv_alerts SET (security_barrier = true);
ALTER VIEW lv_operator_actions SET (security_barrier = true);
ALTER VIEW lv_dlq SET (security_barrier = true);
ALTER VIEW lv_catchups SET (security_barrier = true);
ALTER VIEW lv_engine_runs SET (security_barrier = true);
ALTER VIEW lv_leader_terms SET (security_barrier = true);
ALTER VIEW lv_stream_health SET (security_barrier = true);
ALTER VIEW lv_cluster_state SET (security_barrier = true);
ALTER VIEW lv_engine_epoch SET (security_barrier = true);
ALTER VIEW lv_breaches SET (security_barrier = true);
ALTER VIEW lv_open_orders SET (security_barrier = true);
ALTER VIEW lv_open_positions SET (security_barrier = true);
ALTER VIEW lv_ruleset_timing SET (security_barrier = true);
ALTER VIEW lv_sweep_quarantine SET (security_barrier = true);
ALTER VIEW lv_auth_state SET (security_barrier = true);

GRANT SELECT ON TABLE
    lv_entity_events,
    lv_oversee_events,
    lv_halt_events,
    lv_alerts,
    lv_operator_actions,
    lv_dlq,
    lv_catchups,
    lv_engine_runs,
    lv_leader_terms,
    lv_stream_health,
    lv_cluster_state,
    lv_engine_epoch,
    lv_breaches,
    lv_open_orders,
    lv_open_positions,
    lv_ruleset_timing,
    lv_sweep_quarantine,
    lv_auth_state
TO xtnl_liveview;
