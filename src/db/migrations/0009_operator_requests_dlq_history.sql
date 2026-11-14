-- 0009_operator_requests_dlq_history.sql : operator request kinds, DLQ history, idempotent lineage bumps
-- (IMPLEMENTATION_PLAN 5.5; operations.md s11, s16; oversee-rules.md s4; tier-persistence.md s4, s5).
--
-- * operator_requests: two more in-process request kinds the leader consumes (5.6 wires the consumer):
--     release_account     target = account_id; payload {"v": 1, "account_id", "adapter_id"}
--                         -> RiskSweep.release_account(operator, account_id) (the sweep_quarantine row is
--                            released by the CLI in the same transaction as the request).
--     refresh_cash_flows  target = account_id; payload {"v": 1, "account_id", "adapter_id", "since_ms"}
--                         (since_ms NULL = since the current trading day's 17:00 New York roll)
--                         -> the orchestrator's fetch_cash_flows for that account (rate-limited there).
--   `payload` is new (jsonb, '{}' for reset_task). The kind CHECK is replaced, not loosened to any text.
-- * events_dlq_history: RESOLVED events_dlq rows moved out of the active table by the leader's lifecycle
--   task, partitioned by an HLC-shaped seq_id range (seq_id = resolved_at_ms << 20 | (dlq_id & 0xFFFFF)), so
--   the monthly partitions are created ahead, exported, verified and detached exactly like event_log
--   (src/db/lifecycle.py, operations.md s11).
-- * dlq_lineage_bumps: one row per re-parked payload_hash whose deterministic failure was counted on its
--   origin row, so a re-delivered batch (spooler retry after an unknown COMMIT, spill re-drain) never bumps
--   the origin's retry_count twice (RepositoryDlqSink idempotence).
-- * partition_archive: the table / column comments now describe the engine's own exporter (gzip JSONL in
--   XTNL_ARCHIVE_DIR, sha256 sidecar; S3 copy by the operator job), replacing 0008's Parquet / S3 wording.
-- Idempotent (IF NOT EXISTS / DROP CONSTRAINT IF EXISTS + ADD / CREATE OR REPLACE), applied in order by
-- src/db/migrate.py with lock_timeout 2 s and retries.

SET client_min_messages = warning;

-- ---------------------------------------------------------------------------------------------
-- operator_requests: kinds + payload
-- ---------------------------------------------------------------------------------------------
ALTER TABLE operator_requests ADD COLUMN IF NOT EXISTS payload jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE operator_requests DROP CONSTRAINT IF EXISTS operator_requests_kind_check;
ALTER TABLE operator_requests ADD CONSTRAINT operator_requests_kind_check
    CHECK (kind IN ('reset_task', 'release_account', 'refresh_cash_flows'));
COMMENT ON COLUMN operator_requests.payload IS
    'Request parameters (src/db/operator_requests.py): {} for reset_task; {"v":1,"account_id","adapter_id"} '
    'for release_account; {"v":1,"account_id","adapter_id","since_ms"} for refresh_cash_flows.';

-- ---------------------------------------------------------------------------------------------
-- events_dlq_history (operations.md s11: DLQ history partitioned by seq_id range)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events_dlq_history (
    seq_id            bigint  NOT NULL,           -- partition key: resolved_at_ms << 20 | (dlq_id & 0xFFFFF)
    dlq_id            bigint  NOT NULL,           -- the events_dlq id it had
    origin_dlq_id     bigint,                     -- lineage (no FK: the origin may be archived too)
    event_type        text    NOT NULL,
    entity_id         text    NOT NULL,
    adapter_id        text    NOT NULL,
    raw_payload       text    NOT NULL,
    payload_hash      char(64) NOT NULL,
    error_reason      text    NOT NULL,
    retry_count       int     NOT NULL,
    status            text    NOT NULL,           -- RESOLVED
    parked_at_ms      bigint  NOT NULL,
    resolved_at_ms    bigint  NOT NULL,           -- events_dlq.updated_at_ms when moved
    archived_at_ms    bigint  NOT NULL DEFAULT now_ms(),
    PRIMARY KEY (seq_id, dlq_id)
) PARTITION BY RANGE (seq_id);
CREATE TABLE IF NOT EXISTS events_dlq_history_default PARTITION OF events_dlq_history DEFAULT;
CREATE INDEX IF NOT EXISTS events_dlq_history_entity_idx ON events_dlq_history (entity_id, error_reason);
-- The spill drain (src/db/epoch.py SQL_SPILL_DRAIN_INSERT) skips a payload_hash already archived here.
CREATE INDEX IF NOT EXISTS events_dlq_history_hash_idx ON events_dlq_history (payload_hash);
COMMENT ON TABLE events_dlq_history IS
    'operations.md s11: RESOLVED events_dlq rows moved by the lifecycle task after the keep window; '
    'partitioned by HLC-shaped seq_id ranges, exported, verified and detached like event_log.';

DO $$
DECLARE
    m int;
    base date := date_trunc('month', now() AT TIME ZONE 'UTC')::date;
BEGIN
    FOR m IN 0..2 LOOP
        PERFORM xtnl_ensure_month_partition('events_dlq_history', (base + make_interval(months => m))::date, 20);
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------------------------
-- dlq_lineage_bumps (tier-persistence.md s5 lineage, made idempotent per re-parked payload)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dlq_lineage_bumps (
    payload_hash    char(64) PRIMARY KEY,         -- the re-parked event's payload_hash
    origin_dlq_id   bigint   NOT NULL,
    bumped_at_ms    bigint   NOT NULL DEFAULT now_ms()
);
COMMENT ON TABLE dlq_lineage_bumps IS
    'One row per deterministic re-park counted on its origin events_dlq row; a re-delivery of the same '
    'payload_hash is recognised and never counted twice (src/db/repository.py PgDlqSink).';

-- ---------------------------------------------------------------------------------------------
-- partition_archive: current description (0008 described an external Parquet exporter)
-- ---------------------------------------------------------------------------------------------
COMMENT ON TABLE partition_archive IS
    'operations.md s11: partitions exported by the leader''s lifecycle task as gzip JSONL (+ .sha256 sidecar) '
    'to XTNL_ARCHIVE_DIR and verified by read-back (gzip CRC, line count, sha256) before this fenced row is '
    'written; the detach requires it and an unchanged row count. The S3 copy is an operator job '
    '(docs/runbooks/partition-archive.md); `python -m src.ops archive forget` removes a row that was never detached.';
COMMENT ON COLUMN partition_archive.checksum IS 'sha256:<hex> of the gzip JSONL archive file';
COMMENT ON COLUMN partition_archive.archive_uri IS 'file:// URI of the archive file on the exporting node';
COMMENT ON COLUMN partition_archive.verified_by IS 'engine:<node> of the exporting leader';
