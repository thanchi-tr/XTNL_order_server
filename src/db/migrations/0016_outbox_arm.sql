-- 0016_outbox_arm.sql : notification_outbox prerequisites of the ARM relay (docs/design/automated-risk-manager.md
-- PR4 and §C1 0016; docs/design/arm-implementation-plan.md S08). No behaviour change: nothing writes kind 'arm' yet.
--
-- * expires_at_ms: an ARM notice is delivered at least once until this time, then housekeeping finishes it.
-- * superseded_by: the outbox_id of the ARM notice that replaced this unsent one (no foreign key: the outbox key is
--   (outbox_id, created_at_ms) on a partitioned table).
-- * notification_outbox_arm_due_idx (kind, next_attempt_at_ms, outbox_id) WHERE sent_at_ms IS NULL: the ARM
--   relay's due read. Created ON ONLY the parent here, which builds nothing and leaves it invalid; the migrate
--   post step (src/db/migrate.py POST_STEP) builds it on every partition with CREATE INDEX CONCURRENTLY and
--   attaches each, after which the parent index is valid and later partitions get it automatically.
-- * notification_outbox_arm_channels_check: kind 'arm' if and only if the channel array is non-empty and every
--   element is arm_email or arm_push, optionally with the relay's ':n' failure count (the encoded retry form).
--   Written without a subquery; coalesce makes an empty or NULL array fail for kind 'arm' and pass for any other
--   kind, and an arm_* channel on any other kind fail. notification_outbox is partitioned, so the CHECK is added
--   NOT VALID (enforced at once for every new or updated row, no scan here); the post step validates it.
--
-- The Oversee relay's reads (src/db/notify_store.py SQL_OUTBOX_PENDING, SQL_OUTBOX_DUE, SQL_OUTBOX_OLDEST_UNSENT)
-- skip kind 'arm'. No column added here holds a tick, bar, quote, spread or indicator value. Both new columns are
-- nullable with no default, a catalog-only change. Idempotent (IF NOT EXISTS, the constraint guarded by a catalog
-- check), applied in order by src/db/migrate.py with lock_timeout 2 s and retries.

SET client_min_messages = warning;

ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS expires_at_ms bigint;
ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS superseded_by bigint;

CREATE INDEX IF NOT EXISTS notification_outbox_arm_due_idx
    ON ONLY notification_outbox (kind, next_attempt_at_ms, outbox_id) WHERE sent_at_ms IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'notification_outbox'::regclass
                     AND conname = 'notification_outbox_arm_channels_check') THEN
        ALTER TABLE notification_outbox ADD CONSTRAINT notification_outbox_arm_channels_check
            CHECK ((kind = 'arm') = (coalesce(cardinality(channels), 0) > 0
                AND coalesce(array_to_string(channels, ','), '')
                    ~ '^(arm_email|arm_push)(:[0-9]+)?(,(arm_email|arm_push)(:[0-9]+)?)*$')) NOT VALID;
    END IF;
END
$$;

COMMENT ON COLUMN notification_outbox.expires_at_ms IS
    'ARM notices only: delivered at least once until this time, then finished by housekeeping; NULL otherwise.';
COMMENT ON COLUMN notification_outbox.superseded_by IS
    'ARM notices only: outbox_id of the notice that replaced this unsent one; NULL otherwise.';
