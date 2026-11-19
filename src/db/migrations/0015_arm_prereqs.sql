-- 0015_arm_prereqs.sql : columns for the market-data / ARM prerequisites (docs/design/automated-risk-manager.md
-- §C1 0015; docs/design/arm-implementation-plan.md S01). No behaviour change: every producer still writes NULL.
--
-- * orders_state.order_kind (PR1): the broker order type of a working / child order, 'STOP' or 'LIMIT'; NULL
--   when not known. An optional payload field of OrderDTO (checksum-neutral while NULL).
-- * instruments.price_increment, min_stop_distance, asset_class (PR7): instrument metadata, not market data;
--   optional payload fields of InstrumentSpec (checksum-neutral while NULL).
-- * event_log.origin (PR8): the path that produced the version ('stream', 'snapshot', 'history', 'dlq',
--   'stop_read'). event_log is range-partitioned, so its CHECK is added NOT VALID (enforced at once for every
--   new row, no scan here); the migrate post step validates it later (S08).
-- * event_log.broker_time_ms (PR8, R11-M06): the version's broker time, set only when it is a real broker time
--   (never a stream watermark). The "latest at or before T" index comes with 0017.
-- * sod_marks.position_marks is dropped: no code writes or reads it (Q15 keeps clean_mids unchanged).
--
-- No column added here holds a tick, bar, quote, spread or indicator value. Every new column is nullable with no
-- default, a catalog-only change on the partitioned event_log. Idempotent (IF [NOT] EXISTS, constraints guarded by
-- catalog checks), applied in order by src/db/migrate.py with lock_timeout 2 s and retries.

SET client_min_messages = warning;

ALTER TABLE orders_state ADD COLUMN IF NOT EXISTS order_kind text;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'orders_state'::regclass AND conname = 'orders_state_order_kind_check') THEN
        ALTER TABLE orders_state ADD CONSTRAINT orders_state_order_kind_check
            CHECK (order_kind IN ('STOP', 'LIMIT'));
    END IF;
END
$$;

ALTER TABLE instruments ADD COLUMN IF NOT EXISTS price_increment numeric;
ALTER TABLE instruments ADD COLUMN IF NOT EXISTS min_stop_distance numeric;
ALTER TABLE instruments ADD COLUMN IF NOT EXISTS asset_class text;

ALTER TABLE event_log ADD COLUMN IF NOT EXISTS origin text;
ALTER TABLE event_log ADD COLUMN IF NOT EXISTS broker_time_ms bigint;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'event_log'::regclass AND conname = 'event_log_origin_check') THEN
        ALTER TABLE event_log ADD CONSTRAINT event_log_origin_check
            CHECK (origin IN ('stream', 'snapshot', 'history', 'dlq', 'stop_read')) NOT VALID;
    END IF;
END
$$;

ALTER TABLE sod_marks DROP COLUMN IF EXISTS position_marks;

COMMENT ON COLUMN orders_state.order_kind IS
    'Broker order type (STOP or LIMIT; NULL = not known). Optional payload field, checksum-neutral while NULL.';
COMMENT ON COLUMN event_log.origin IS
    'Path that produced the version (stream, snapshot, history, dlq, stop_read); NULL before 0015 labelling.';
COMMENT ON COLUMN event_log.broker_time_ms IS
    'The version''s broker time, set only when the broker stated it (never a stream watermark); else NULL.';
