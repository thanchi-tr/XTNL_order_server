-- 0004_cluster_state_abdications.sql : durable broker-abdication memory (operations.md s4).
--
-- "After one such abdication, no further one for that adapter until it has reached STREAMING on some
-- node": the leader records {adapter_id: abdicated_at_ms} here (fenced write) before it drains, and the
-- leader that next sees that adapter STREAMING removes the entry. src/db/cluster_state.py.
-- Idempotent (IF NOT EXISTS), applied in order by src/db/migrate.py.

SET client_min_messages = warning;

ALTER TABLE cluster_state ADD COLUMN IF NOT EXISTS broker_abdications jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN cluster_state.broker_abdications IS
    'adapter_id -> abdicated_at_ms of the last broker-grounds abdication; cleared once the adapter reaches '
    'STREAMING on some node (operations.md s4). Written only by the leader, fenced on engine_epoch.';
