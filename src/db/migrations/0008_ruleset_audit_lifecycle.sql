-- 0008_ruleset_audit_lifecycle.sql : ruleset reload audit and the data-lifecycle job (IMPLEMENTATION_PLAN 4.5, 4.8).
--
-- * ruleset_audit: one row per ruleset the engine installs, falls back to or rejects (oversee-rules.md §7
--   "a reload ... is audited"). Written by src/db/ruleset_store.py, fenced on the epoch. The operator write
--   path (a new rulesets row and its activation) is the operator CLI (4.6).
-- * partition_archive: the export ledger of the lifecycle job (operations.md §11). The leader's lifecycle
--   task (src/db/lifecycle.py PartitionExporter) writes a row once a partition has been exported as gzip
--   JSONL to XTNL_ARCHIVE_DIR and verified by read-back (gzip CRC, line count, sha256; the S3 copy is an
--   operator job). The engine detaches a partition past the hot window only when such a row exists and the
--   partition still holds exactly `row_count` rows; dropping a detached table stays an operator action.
--   (Comment-only revision accepted by src/db/migrate.py ACCEPTED_REVISIONS; the table comment is
--   refreshed by 0009.)
-- * xtnl_detach_archived_partition(): that check plus the DETACH in one call, so the engine's fenced
--   statement (`SELECT xtnl_detach_archived_partition(...) WHERE <fence>`) carries the fence itself.
-- Numbered 0008: 0005-0007 are taken (0007_operator_audit.sql is the operator CLI, 4.6).
-- Idempotent (IF NOT EXISTS / CREATE OR REPLACE), applied in order by src/db/migrate.py.

SET client_min_messages = warning;

CREATE TABLE IF NOT EXISTS ruleset_audit (
    audit_id         bigserial PRIMARY KEY,
    ruleset_version  bigint,                      -- NULL: no usable row, the base config runs
    outcome          text    NOT NULL CHECK (outcome IN ('installed', 'fallback', 'base', 'rejected')),
    detail           text    NOT NULL DEFAULT '',
    epoch_revision   bigint  NOT NULL,
    node             text    NOT NULL,
    at_ms            bigint  NOT NULL DEFAULT now_ms()
);
CREATE INDEX IF NOT EXISTS ruleset_audit_version_idx ON ruleset_audit (ruleset_version, at_ms);
COMMENT ON TABLE ruleset_audit IS
    'Every ruleset the engine installed, fell back to or rejected (oversee-rules.md §7); append-only.';

CREATE TABLE IF NOT EXISTS partition_archive (
    partition_name   text    PRIMARY KEY,         -- e.g. event_log_p202608
    parent_table     text    NOT NULL,
    row_count        bigint  NOT NULL CHECK (row_count >= 0),
    checksum         text    NOT NULL,            -- sha256:<hex> of the gzip JSONL archive file
    archive_uri      text    NOT NULL,            -- file:// URI on the exporting node
    verified_at_ms   bigint  NOT NULL,
    verified_by      text    NOT NULL,
    detached_at_ms   bigint,
    detached_epoch   bigint
);
COMMENT ON TABLE partition_archive IS
    'operations.md §11: partitions exported to S3 and verified; the lifecycle job detaches only these.';

CREATE OR REPLACE FUNCTION xtnl_detach_archived_partition(parent text, part text, epoch bigint)
    RETURNS text
    LANGUAGE plpgsql
AS $$
DECLARE
    expected bigint;
    actual   bigint;
BEGIN
    SELECT row_count INTO expected FROM partition_archive
     WHERE partition_name = part AND parent_table = parent AND detached_at_ms IS NULL;
    IF expected IS NULL THEN
        RETURN 'not_archived';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_inherits i
                     JOIN pg_class c ON c.oid = i.inhrelid
                     JOIN pg_class p ON p.oid = i.inhparent
                    WHERE c.relname = part AND p.relname = parent) THEN
        RETURN 'not_attached';
    END IF;
    EXECUTE format('SELECT count(*) FROM %I', part) INTO actual;
    IF actual IS DISTINCT FROM expected THEN
        RETURN 'count_mismatch';
    END IF;
    EXECUTE format('ALTER TABLE %I DETACH PARTITION %I', parent, part);
    UPDATE partition_archive SET detached_at_ms = now_ms(), detached_epoch = epoch WHERE partition_name = part;
    RETURN 'detached';
END
$$;
COMMENT ON FUNCTION xtnl_detach_archived_partition(text, text, bigint) IS
    'operations.md §11: detach a partition only after a verified export with a matching row count.';
