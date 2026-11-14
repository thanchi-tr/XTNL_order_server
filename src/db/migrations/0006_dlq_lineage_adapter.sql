-- 0006_dlq_lineage_adapter.sql : adapter-scoped DLQ lineage lookup (tier-persistence.md §5).
--
-- Entity ids (client_order_id, position_id, account_id, transaction_id) are unique only per adapter, so
-- DlqStore.find_lineage_origin now filters on adapter_id as well (src/db/integrity_store.py
-- SQL_DLQ_LINEAGE). The lineage index leads with adapter_id; the 0003 index it replaces is dropped.
-- Idempotent (IF NOT EXISTS / IF EXISTS), applied in order by src/db/migrate.py.

SET client_min_messages = warning;

CREATE INDEX IF NOT EXISTS events_dlq_lineage_adapter_idx
    ON events_dlq (adapter_id, event_type, entity_id, error_reason, updated_at_ms DESC, dlq_id DESC)
    WHERE status = 'RESOLVED' AND origin_dlq_id IS NULL;
DROP INDEX IF EXISTS events_dlq_lineage_idx;
