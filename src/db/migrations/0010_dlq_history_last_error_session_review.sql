-- 0010_dlq_history_last_error_session_review.sql : follow-ups to 0009 (IMPLEMENTATION_PLAN 5.5 re-check).
--
-- * events_dlq_history.last_error: the last failure text of a RESOLVED row (events_dlq.last_error, 0003) moves
--   with the row (src/db/lifecycle.py SQL_DLQ_HISTORY_MOVE), so triage data is not lost by the history move.
-- * market_sessions review flags from the 5.1 adapter contract (src/adapters/base.py MarketSessions):
--     is_default      true when the adapter used a documented default because the broker gave no hours
--                     (not broker data; needs review)
--     holidays_known  false when the broker has no holiday source (an empty holidays list then does not
--                     mean "no holidays")
--     note            the adapter's note on how the row was derived
--   Written by src/db/market_sessions.py PgMarketSessionStore; the runtime loader ignores them.
-- Idempotent (ADD COLUMN IF NOT EXISTS with constant defaults: no table rewrite), applied by src/db/migrate.py
-- with lock_timeout 2 s and retries.

SET client_min_messages = warning;

ALTER TABLE events_dlq_history ADD COLUMN IF NOT EXISTS last_error text;

ALTER TABLE market_sessions ADD COLUMN IF NOT EXISTS is_default boolean NOT NULL DEFAULT false;
ALTER TABLE market_sessions ADD COLUMN IF NOT EXISTS holidays_known boolean NOT NULL DEFAULT true;
ALTER TABLE market_sessions ADD COLUMN IF NOT EXISTS note text NOT NULL DEFAULT '';
COMMENT ON COLUMN market_sessions.is_default IS
    'operations.md s10: the adapter used a documented default schedule (no broker hours for the symbol); review it';
COMMENT ON COLUMN market_sessions.holidays_known IS
    'false when the broker has no holiday source: an empty holidays list does not mean "no holidays"';
