-- 0005_balance_changed_at.sql : commit time of the latest balance change (oversee-rules.md §2 skew cap).
--
-- MaxDailyLossRule is capped at SOFT while an order / position changed status or units more recently than
-- the account's latest balance version. account_state.committed_at_ms cannot serve as that time: every
-- UPDATE moves it, including a same-checksum NOOP_ADVANCE (a re-sent, unchanged balance) and a
-- realised_pnl_today-only change. balance_changed_at_ms moves only when `balance` itself changes
-- (src/db/entity_sql.py ACCOUNTS.stamp_col) and is what src/db/rows.py ACCOUNT_COLS exposes.
-- Idempotent (IF NOT EXISTS / guarded), applied in order by src/db/migrate.py.

SET client_min_messages = warning;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'account_state' AND column_name = 'balance_changed_at_ms') THEN
        ALTER TABLE account_state ADD COLUMN balance_changed_at_ms bigint;
        -- Existing rows: the last commit is the best (latest possible) estimate of the last balance change.
        UPDATE account_state SET balance_changed_at_ms = committed_at_ms;
        ALTER TABLE account_state ALTER COLUMN balance_changed_at_ms SET DEFAULT now_ms();
        ALTER TABLE account_state ALTER COLUMN balance_changed_at_ms SET NOT NULL;
    END IF;
END
$$;
COMMENT ON COLUMN account_state.balance_changed_at_ms IS
    'Commit time (ms) of the latest change of `balance` (INSERT or an UPDATE that changed it); the '
    'balance / position skew input of MaxDailyLossRule (oversee-rules.md §2). Not moved by NOOP_ADVANCE.';
