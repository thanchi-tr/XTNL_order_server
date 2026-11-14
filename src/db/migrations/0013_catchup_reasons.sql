-- 0013_catchup_reasons.sql : two more downtime catch-up reasons (src/runtime/catchup.py).
--
-- * 'reconnect': the adapter's orders / account path was interrupted after its last catch-up read (a gap, a lost
--   socket or auth). Orders placed and finished while it lasted are in no reconcile snapshot, so the leader
--   re-reads the broker history from adapter_coverage (which stops advancing at the interruption).
-- * 'retry': the previous catch-up was incomplete for a cause on the engine side (failed reads, rows not
--   committed); adapter_coverage is held back and the catch-up runs again with backoff.
--
-- Expand only (operations.md s12): the old reasons stay valid, so engine N (0012 reasons) and N+1 both write
-- accepted rows. Idempotent: the constraint is dropped and re-added under the same name.

SET client_min_messages = warning;

ALTER TABLE catchup_runs DROP CONSTRAINT IF EXISTS catchup_runs_reason_check;
ALTER TABLE catchup_runs ADD CONSTRAINT catchup_runs_reason_check
    CHECK (reason IN ('startup', 'resume_after_sleep', 'operator', 'reconnect', 'retry'));
