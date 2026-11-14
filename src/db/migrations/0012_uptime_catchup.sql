-- 0012_uptime_catchup.sql : engine uptime tracking and downtime catch-up (the engine runs on a PC that is not on
-- 24/7; it must know when it was running and, at start-up and after the PC wakes from sleep, pull what the
-- broker changed while it was off so no order is lost).
--
-- * engine_runs: one row per engine process run (every node, any SM9 role). Written by the UptimeTracker
--   (src/runtime/uptime.py): the row at start, `last_seen_ms` every XTNL_UPTIME_PING_S (60..1800 s, default
--   900), `suspended_gaps` +1 whenever the process notices it was suspended (PC sleep / hibernate: a wall-clock
--   jump not matched by monotonic time, or both far beyond the tick), and `stopped_at_ms` + `end_reason` on a
--   clean stop. A row whose `stopped_at_ms` is NULL and that is not the current run ended without a clean stop
--   (hard exit, crash, power loss).
-- * adapter_coverage: per adapter, the newest wall time at which the LEADER had that adapter STREAMING with every
--   stream reconciled and its downtime catch-up finished (`last_streaming_ms`), and the run that wrote it. The
--   next start-up catch-up covers [last_streaming_ms - XTNL_CATCHUP_OVERLAP_S, now].
-- * catchup_runs: one row per downtime catch-up of one adapter (src/runtime/catchup.py), written by the leader.
--   Counts and ids only: never an amount, a balance or any currency figure.
--
-- Fencing (operations.md s2): engine_runs and the adapter_coverage heartbeat are node telemetry, not engine
-- state (no Oversee or ingestion decision reads them, and every node writes its own engine_runs row in any role),
-- so they are written WITHOUT the hard epoch fence, like the idempotent spill drain. The coverage write is still
-- skipped (0 rows, never an EpochMismatch) when engine_epoch no longer holds the writer's epoch, so a fenced-out
-- ex-leader cannot advance coverage past data it can no longer commit (a soft condition, not the fence).
-- catchup_runs is written by the leader with the fence ($1 = epoch), like every other engine write.
-- Idempotent (CREATE ... IF NOT EXISTS). Applied by src/db/migrate.py with lock_timeout 2 s and retries.

SET client_min_messages = warning;

CREATE TABLE IF NOT EXISTS engine_runs (
    run_id          uuid    PRIMARY KEY,
    node_id         text    NOT NULL,
    started_at_ms   bigint  NOT NULL,
    last_seen_ms    bigint  NOT NULL,
    stopped_at_ms   bigint,
    end_reason      text,
    suspended_gaps  integer NOT NULL DEFAULT 0 CHECK (suspended_gaps >= 0),
    CHECK (last_seen_ms >= started_at_ms),
    CHECK (stopped_at_ms IS NULL OR end_reason IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS engine_runs_node_idx ON engine_runs (node_id, started_at_ms DESC);
CREATE INDEX IF NOT EXISTS engine_runs_started_idx ON engine_runs (started_at_ms DESC);
COMMENT ON TABLE engine_runs IS
    'Engine process runs (any node, any role): start, light heartbeat (last_seen_ms), suspended gaps (PC sleep), '
    'clean stop. Node telemetry: written without the epoch fence (migration 0012).';

CREATE TABLE IF NOT EXISTS adapter_coverage (
    adapter_id             text   PRIMARY KEY,
    last_streaming_ms      bigint NOT NULL,
    last_streaming_run_id  uuid,
    updated_at_ms          bigint NOT NULL DEFAULT now_ms()
);
COMMENT ON TABLE adapter_coverage IS
    'Newest time the leader had the adapter STREAMING, reconciled and caught up (the next catch-up starts at it '
    'minus XTNL_CATCHUP_OVERLAP_S). Only ever moves forward. Written without the hard fence, skipped when the '
    'writer''s epoch is no longer current (migration 0012).';

CREATE TABLE IF NOT EXISTS catchup_runs (
    id                 bigserial PRIMARY KEY,
    run_id             uuid,
    epoch              bigint  NOT NULL,
    adapter_id         text    NOT NULL,
    reason             text    NOT NULL CHECK (reason IN ('startup', 'resume_after_sleep', 'operator')),
    gap_start_ms       bigint  NOT NULL,
    gap_end_ms         bigint  NOT NULL,
    covered_from_ms    bigint,
    offline_from_ms    bigint,
    started_at_ms      bigint  NOT NULL,
    finished_at_ms     bigint  NOT NULL,
    orders_seen        integer NOT NULL DEFAULT 0,
    orders_new         integer NOT NULL DEFAULT 0,
    orders_updated     integer NOT NULL DEFAULT 0,
    positions_seen     integer NOT NULL DEFAULT 0,
    positions_new      integer NOT NULL DEFAULT 0,
    positions_closed   integer NOT NULL DEFAULT 0,
    cash_flows_new     integer NOT NULL DEFAULT 0,
    anomalies          integer NOT NULL DEFAULT 0,
    complete           boolean NOT NULL,
    notes              text,
    CHECK (gap_end_ms >= gap_start_ms),
    CHECK (finished_at_ms >= started_at_ms)
);
CREATE INDEX IF NOT EXISTS catchup_runs_adapter_idx ON catchup_runs (adapter_id, finished_at_ms DESC);
CREATE INDEX IF NOT EXISTS catchup_runs_finished_idx ON catchup_runs (finished_at_ms DESC);
COMMENT ON TABLE catchup_runs IS
    'One downtime catch-up per adapter: window, counts (no amounts), completeness. Written by the leader, fenced.';
