# Failover and switchover

Phase 1 never sends orders. ActionGate stays closed throughout every case below.

## Trigger

* Planned: rolling upgrade, instance maintenance, alert "leader not on the Patroni primary's node > 10 min".
* Unplanned: alerts "no LEADER > 30 s", "two LEADERs", "sync standby missing", "etcd unhealthy", node or
  zone loss.

## Checks (always first)

```
$ETCDCTL endpoint status -w table          # 3 members, one raft leader, no alarms
$ETCDCTL get /xtnl/leader                  # current engine leader
$ETCDCTL get --prefix /xtnl/health/        # both nodes fresh (< 5 s), hard checks passing
patronictl -c /etc/patroni/patroni.yml list   # Leader + Sync Standby, lag 0
$PSQL -c "SELECT node, epoch_revision, started_at, reached_leader_at, ended_at, end_reason
          FROM leader_terms ORDER BY term_id DESC LIMIT 5"
$PSQL -c "SELECT safe_hold, safe_hold_reason FROM cluster_state"   # must be false before any switchover
```

## Planned engine switchover (rolling upgrade)

1. Run migrations first if the release has any: `sudo systemctl start xtnl-migrate` (expand / contract).
2. Upgrade the STANDBY node's release, `sudo systemctl restart xtnl-engine` there, wait for its health record.
3. If the Patroni primary is not on the standby's node, move it: `patronictl switchover --leader <old> --candidate <new>`.
4. On the leader: `sudo systemctl stop xtnl-engine` (SIGTERM -> DRAINING <= 10 s: intake stopped, spooler
   flushed, `end_reason='switchover'`, lease released).
5. Expected: the peer campaigns and reaches LEADER in ~30-60 s; `leader_terms` shows the old term ended
   `switchover` and a new term with `reached_leader_at` set.
6. Upgrade the old leader, `sudo systemctl start xtnl-engine` (it enters STANDBY with a 60 s back-off).

## Unplanned cases

| Case | Expected (operations.md s6) | Operator action |
|---|---|---|
| Engine crash / hang | watchdog (3 s) or fence (12 s) kills it; peer takes the lease; ingestion back ~30-60 s | read the journal of the dead node; it restarts into STANDBY |
| Node or zone A lost | Patroni promotes B (~15-25 s), lease moves to B; ~45-90 s | rebuild A (`install-node.md`), `patronictl reinit xtnl-pg pg-a` |
| PostgreSQL process fails on primary | sync standby promoted at once (`primary_start_timeout 0`) | `patronictl list`; reinit the old member when it returns |
| Standby lost | commits stall <= ~20 s, then Patroni drops sync -> **async + CRITICAL** "sync standby missing" | restore the standby; confirm `Sync Standby` returns; RPO is not 0 until it does |
| Witness / etcd leader's zone lost | etcd re-elects inside the fence budget; no outage | replace the member (below) |
| Two LEADERs alert | impossible by lease + epoch; one is already fenced | check `engine_epoch`; the node whose epoch is older will hard-exit; page engineering |

## Planned etcd member restart / replacement

Only while 3/3 are healthy, one member at a time.

```
$ETCDCTL endpoint status -w table                       # note the raft leader's member id
$ETCDCTL move-leader <other-member-id>                  # if restarting the current raft leader
sudo systemctl restart etcd                             # on that member
$ETCDCTL endpoint health                                # back to 3/3 before the next member
```

Replacing a member: `$ETCDCTL member remove <id>`, `$ETCDCTL member add etcd-x --peer-urls=https://<ip>:2380`,
start it with `initial-cluster-state: existing`.

## Rollback

A switchover is its own rollback: switch back the same way once the original node is healthy. Never force a
lease (`etcdctl del /xtnl/leader`) while a leader is alive; if a stuck node holds it, stop its unit and let
the lease expire (15 s).
