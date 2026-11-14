# Redis loss on the leader (SM8 bootstrap)

Phase 1 never sends orders. Redis is a disposable duplicate of PostgreSQL; losing it loses nothing durable.

## Trigger

Redis restarted, OOM-killed or flushed on the leader node; alerts "SM8 not READY > 60 s", "gate closed",
dead-man threshold "SM8 not READY". Detection is automatic: `apply_state.lua` returns `NO_BOOT`, or the
integrity tick sees `redis:boot_id` != `run_id:epoch`.

## What happens automatically (no operator action needed)

ActionGate denies everything. The RiskSweep keeps running from PostgreSQL; only the feed observer waits.
The integrity worker takes `redis:boot_lock` and rebuilds in order: `halt:state` from `halt_events`, the
`oversee:feed` consumer group, prices / live values from `last_clean_marks` and `account_snapshots`, a heal
of every entity, the queued `NO_BOOT` writes, then writes `redis:boot_id` last. Expected: gate closed
~10-30 s, no abdication (the Redis hard check must fail >= 30 s before a leader moves).

## Checks

```
systemctl status redis-server ; journalctl -u redis-server -n 50
redis-cli --user xtnl_engine --askpass INFO server | grep run_id
redis-cli --user xtnl_engine --askpass GET redis:boot_id         # = <run_id>:<epoch> when READY
redis-cli --user xtnl_engine --askpass INFO memory | grep -E "used_memory_human|maxmemory_human|maxmemory_policy"
journalctl -u xtnl-engine --since "-10 min" | grep -E "SM8|boot_id|NO_BOOT"
```

`maxmemory_policy` must be `noeviction`. If Redis ran out of memory, writes were rejected (not evicted):
raise `maxmemory` in `/etc/redis/redis.conf`, check `oversee:feed` length (`XLEN oversee:feed`, cap ~1 000 000),
and look for a stuck consumer (`XPENDING oversee:feed <group>`).

## Commands

* Redis down: `sudo systemctl restart redis-server`. SM8 runs in place.
* SM8 stuck > 60 s (pages): check PostgreSQL reachability first; if Redis keeps failing, stop Redis's
  node from being leader: `sudo systemctl stop xtnl-engine` on that node (DRAINING), so the peer takes over
  and runs SM8 on its own Redis.
* Never write `redis:boot_id` or `halt:state` by hand.

## Expected outcome

`redis:boot_id` = current `run_id:epoch`, the gate reopens (subject to the other gate conditions), the
dead-man payload shows SM8 READY.

## Rollback

Nothing to roll back: Redis is rebuilt from PostgreSQL. If a manual FLUSHDB was run by mistake, SM8
handles it the same way.
