-- 0019_orders_linked_idx.sql (plan S38d review; design §B6.4): an index for the S_at_T linked-children read.
--
-- src/db/arm_versions.py SQL_LINKED_STOP_CHILDREN reads, inside the REPEATABLE READ snapshot of every S_at_T read,
-- the reduce-only orders_state rows linked to one position (adapter_id, linked_position_id, is_reduce_only).
-- orders_state keeps every terminal row for good (one row per order), so without an index that read scans the whole
-- table, and the table only grows on a 24/7 node.
--
-- Expand-only: one partial index, orders_state_linked_stop_idx ON orders_state (adapter_id, linked_position_id)
-- WHERE is_reduce_only. It is built by the migrate post step (src/db/migrate.py POST_STEP, TableIndex 19) with
-- CREATE INDEX CONCURRENTLY outside any transaction (a migration file runs in one, where CONCURRENTLY cannot), so
-- ingestion's writes to orders_state are never blocked; an interrupted build is dropped and rebuilt by the next run.
-- No query needs the index to be correct.
--
-- No column is added: nothing here holds a tick, bar, quote, spread, indicator value, favour R or favour / risk.
-- Applied by src/db/migrate.py with lock_timeout 2 s and retries; run with the schema-owner DSN, as 0017 and 0018.

SET client_min_messages = warning;
