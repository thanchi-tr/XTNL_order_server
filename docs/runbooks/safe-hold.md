# SAFE_HOLD (anti-flap brake, SM9)

Phase 1 never sends orders. During SAFE_HOLD no node is leader: there is no ingestion and ActionGate is
closed. The broker accounts are unaffected.

## Trigger

One off-node page "SAFE_HOLD set". Due when 3 terms end abnormally (`lost`, `init_failed`) within 15 min,
or more than 6 epoch changes happen within 30 min (only terms ended after the last resume count). The row
`cluster_state.safe_hold = true` is durable and mirrored to etcd `/xtnl/safe_hold`.

## Checks

```
$OPS safe-hold status                                   # PostgreSQL row + whether the etcd mirror agrees
$OPS epoch show                                         # engine_epoch and the last 5 leader_terms
$PSQL -c "SELECT node, epoch_revision, started_at, reached_leader_at, ended_at, end_reason
          FROM leader_terms ORDER BY term_id DESC LIMIT 12"
journalctl -u xtnl-engine --since "-45 min" | grep -E "FENCE|hard exit|init_failed|abdicat|watchdog"
$ETCDCTL endpoint status -w table ; patronictl -c /etc/patroni/patroni.yml list
chronyc tracking                                        # offset < 50 ms on both nodes
```

Find the cause before resuming: typical ones are etcd latency (lease-renew p99 > 500 ms), a PostgreSQL
stall, a crash in LEADER_INIT (`init_failed`), memory pressure (MemoryMax kill), or clock trouble.

## Commands

1. Fix the cause. Confirm both nodes publish fresh health records that pass the hard checks.
2. Resume (authenticated and audited, two people, recorded in the ticket):
   ```
   $OPS safe-hold resume --operator <you> --approver <second person> --reason "<ticket>: <cause fixed>"
   ```
   In one fenced transaction this clears `cluster_state.safe_hold`, sets `last_resume_at` / `resumed_by`
   and writes the `operator_audit` row; it then writes the etcd mirror `/xtnl/safe_hold` in the leader's
   format.
   * `refused: not_in_safe_hold` (exit 3): nothing to do; check `$OPS safe-hold status`.
   * `refused: epoch_moved`: a node bumped the epoch meanwhile; re-check, then re-run.
   * Exit 4 (`warning: etcd mirror not written`): the resume is committed; PostgreSQL is the truth and nodes
     read it first. Run `$OPS safe-hold status`; the next leader re-mirrors the key within seconds. Do not
     edit `/xtnl/safe_hold` by hand.

## Expected outcome

Within a few seconds a node campaigns, runs LEADER_INIT and reaches LEADER; `leader_terms` gets a new row
with `reached_leader_at`; the dead-man payload resumes every 15 s. Earlier abnormal terms no longer count.

## Rollback

If the flapping restarts, SAFE_HOLD is set again automatically (fail-safe). Stop both engines
(`systemctl stop xtnl-engine`) if you need a quiet cluster while investigating.
