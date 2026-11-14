-- 0003_integrity_stores.sql : columns and indexes the PostgreSQL integrity ports need (src/db/integrity_store.py).
--
-- Spec: tier-persistence.md §3 (SM3 reads), §5 (events_dlq: lease owner, lineage, check-before-retry).
-- Idempotent (IF NOT EXISTS), applied in order by src/db/migrate.py.

SET client_min_messages = warning;

-- ---------------------------------------------------------------------------------------------
-- events_dlq: the last failure of a BACKOFF / QUARANTINED row (DlqStore.backoff / quarantine).
-- ---------------------------------------------------------------------------------------------
ALTER TABLE events_dlq ADD COLUMN IF NOT EXISTS last_error text;

-- Lineage lookup (DlqStore.find_lineage_origin): the newest RESOLVED origin row of an
-- (event_type, entity_id, error_reason), ordered by updated_at_ms DESC.
CREATE INDEX IF NOT EXISTS events_dlq_lineage_idx
    ON events_dlq (event_type, entity_id, error_reason, updated_at_ms DESC, dlq_id DESC)
    WHERE status = 'RESOLVED' AND origin_dlq_id IS NULL;

-- Expired leases (DlqStore.release_expired).
CREATE INDEX IF NOT EXISTS events_dlq_leased_idx ON events_dlq (lease_until_ms) WHERE status = 'LEASED';

-- Parked cash flows of an adapter (DlqStore.cash_flow_since_ms).
CREATE INDEX IF NOT EXISTS events_dlq_parked_cash_flow_idx ON events_dlq (adapter_id)
    WHERE event_type = 'CASH_FLOW' AND status IN ('QUEUED', 'LEASED', 'BACKOFF');

-- Latest committed cash flow per adapter (DlqStore.cash_flow_since_ms).
CREATE INDEX IF NOT EXISTS cash_flows_adapter_ts_idx ON cash_flows (adapter_id, broker_ts);

-- ---------------------------------------------------------------------------------------------
-- Integrity reads (IntegrityDb.committed_rows): instruments by commit time, like the entity tables.
-- ---------------------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS instruments_committed_idx ON instruments (committed_at_ms);

-- Latest sampled live fields per account (IntegrityDb.latest_live_samples, SM8 step 3).
CREATE INDEX IF NOT EXISTS account_snapshots_live_idx ON account_snapshots (account_id, live_ts_ms DESC)
    WHERE live_ts_ms IS NOT NULL;

COMMENT ON COLUMN sod_marks.transfers_unverified IS
    'Set by the DLQ worker when a parked cash flow could not be confirmed (tier-persistence.md §5). A row '
    'written only to carry this flag has sod_equity NULL (a placeholder); the sweep''s day-roll insert fills it.';
