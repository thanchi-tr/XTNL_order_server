# Runbook: market-data layer (`redis-md`, md-writer, md-bars, md-indicators)

**Phase 1 never sends orders.** The market-data layer only reads prices and broker bars and keeps ticks, bars and
indicator values in its own store, `redis-md`. No step below places, modifies or cancels an order or closes a
position. It is **optional**: it runs only with `XTNL_MD_ENABLED=1`, and enabling, disabling or losing it never
affects ingestion, Oversee, the gate or leadership (design §A2, §C2). Market prices (ticks, bars, quotes, spreads,
indicator values) never go into PostgreSQL, a page, a log line or a ticket: describe a problem by series id, bucket
time and check name only.

Design: `docs/design/automated-risk-manager.md` Part A (§A2 store, §A4 bars, §A5 sizing, §A7 SM12, §A8 write path,
§A13 keys and rotation). The ARM on top of it: `arm.md`.

## Trigger

Use this runbook for any of these pages (titles as the engine sends them) or the off-node `XtnlRedisMdMemoryHigh`:

| Page | Meaning | Fix |
|---|---|---|
| `redis-md memory high` | `redis-md` above 70 % of `maxmemory` (cleared below 65 %) | "redis-md memory" below |
| `redis-md memory shedding step N` | md-writer entered shedding step N (§A8 order: tick history first; BROKER series, indicators and rollback sets are never shed) | "redis-md memory" below |
| `redis-md refuses writes for memory` | a write was refused with `OOM` after the last step, or while the memory level is unknown | "redis-md memory" below, at once |
| `broker bars unavailable` | the adapter is not STREAMING or its REST reads fail at every priority: a broker outage (`broker_unavailable`, never FAILED); series stay GAPPED | wait for the adapter to stream again (`auth-failed-rearm.md`, `broker-maintenance.md`); md-bars resumes and backfills by itself |
| `broker bar reads failing` | bar reads fail only at the `bars` REST priority (cap reached, decode errors) | check the REST budget (`xtnl_rest_requests_total{priority="bars"}`); a persistent case reaches the ARM as a SERIES fault (`arm.md` FAILED) |
| `tick cross-check skipped` | more than half of the last 16 demanded buckets of a series skipped the BROKER-vs-TICK cross-check (D6) | read `md show` (D6 status `CLOSE_ONLY` / `UNAVAILABLE` is expected under filtered delivery); no action unless the PR3 probe said the feed should be unfiltered |
| `indicator faulted` | an indicator's computation failed and it was marked FAULTED (its consumers get `DATA_UNAVAILABLE`) | `md indicators --indicator <id>`, then `md rebuild <id>` (below) |
| `indicator threads hung` | md-indicators worker threads did not return; indicator jobs paused | `task reset md-indicators` (below); a repeat means a strategy or indicator version bug: report it |
| `market data: unknown tick-only symbols` | an `XTNL_TICK_SYMBOLS` item is not in the adapter's instrument list; it is ignored | correct `XTNL_TICK_SYMBOLS` in the env file and restart the engine (planned switchover on AWS) |
| `optional services settings` | an `XTNL_MD_*` / `XTNL_ARM_*` value is out of bounds; the optional services are parked for this term (the page lists the variable names) | correct the named variables, then restart the engine; ingestion and Oversee never stopped |

The off-node alert `XtnlRedisMdMemoryHigh` (`off-node-alerts.md`) is the same 70 % condition seen from outside the
engine.

## Prerequisites

* **Order** (plan S38f, design §0b "What turns each on"): first raise `XTNL_MIGRATIONS_ACCEPTED_UPTO` to `20` once
  migrations 0015-0020 are reviewed (the next start applies them; until then they are held back and named in one
  `[migrate]` / journal line, and the engine runs on the 0014 schema), then `XTNL_OVERSEE_STOP_AT_ENTRY=1` once the
  S07 Oversee stop rule (design §0 C4) is confirmed, then `XTNL_MD_ENABLED=1`, then `XTNL_ARM_ENABLED=1`. The go-live
  gate `oversee_c4` counts as accepted only with `XTNL_OVERSEE_STOP_AT_ENTRY=1`.
* The flag: `XTNL_MD_ENABLED=1` in `/etc/xtnl/engine.env` (AWS, both nodes) or `deploy/env/local.env` (PC). Exactly
  `1` is on; unset or `0` is off; anything else is off plus one page.
* The secrets (design §C2 Secrets, never printed, never in a file in the repository): `XTNL_MD_REDIS_URL` (`md_writer`,
  engine only), `XTNL_MD_REDIS_READER_URL` (`md_reader`, the ops CLI), `XTNL_MD_HMAC_KEY` (32 random bytes, hex; the
  same on both AWS nodes) with the non-secret `XTNL_MD_HMAC_KEY_ID` in the env file. AWS: in the Secrets Manager secret
  the env file names (`XTNL_SECRETS_SOURCE=aws-sm`). PC: the Windows User environment only
  (`deploy/local/new-md-credentials.ps1`).
* AWS: `deploy/redis/redis-md.conf` as `/etc/xtnl/redis-md.conf` and `deploy/systemd/xtnl-redis-md.service` installed
  (`install-node.md`), **not enabled** until this runbook's "Enable" step. The URLs point at `127.0.0.1:6381`.
* PC: the `redis-md` compose service (profile `md`, `127.0.0.1:56382`) and the ACL file
  `%LOCALAPPDATA%\XTNL\redis-md\users.acl` from `new-md-credentials.ps1` (`single-pc-autostart.md`).

```
cd /opt/xtnl/order-server
OPS=".venv/bin/python -m src.ops"     # needs XTNL_MD_REDIS_READER_URL and the keyring for md views and arm replay
$OPS md show                          # md generation, SM12 / D6 state per series, re-hash progress, FAULTED indicators
$OPS md bars <adapter:symbol:spec:SIDE:SOURCE> --newest 16    # counts, quality, quarantined buckets; never a value
$OPS md indicators                                            # warm, lineage, FAULTED; never a value (--indicator ID: one)
```

## Enable (AWS, one node at a time, standby first)

```
# 0. prerequisites: Redis 7.2 or newer (the redis-md ACL grants +waitaof / +client|setinfo; Redis 7.0, Ubuntu 24.04's
#    own package, rejects the aclfile and xtnl-redis-md fails at every start). install-node.md installs the pinned 7.4.
redis-server --version                         # must print v=7.2 or newer (v=7.4.x); stop here otherwise
#    security group: open 6382 (the redis-md section of the TLS proxy) from VPN_CIDR to A and B, nothing else
# 1. secrets: add XTNL_MD_REDIS_URL=redis://md_writer:<pw>@127.0.0.1:6381/0, XTNL_MD_REDIS_READER_URL=redis://md_reader:<pw2>@127.0.0.1:6381/0
#    and XTNL_MD_HMAC_KEY (openssl rand -hex 32) to the Secrets Manager secret; two different random passwords.
# 2. store
sudo install -m 0644 deploy/redis/redis-md.conf /etc/xtnl/redis-md.conf
sudo install -m 0644 deploy/systemd/xtnl-redis-md.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now xtnl-redis-md      # ExecStartPre renders /run/xtnl-redis-md/users.acl (hashes only)
redis-cli -p 6381 --user md_health --pass unused PING          # PONG; md_health needs no secret
# 3. engine: XTNL_MD_ENABLED=1 in /etc/xtnl/engine.env, then restart the standby, switch over, restart the other node
sudo systemctl restart xtnl-engine             # on the STANDBY only (failover-and-switchover.md for the leader)
```

The engine starts `optional-bootstrap` only after it reaches LEADER; until `md-writer`'s bootstrap has run, open bars
are `partial`. `md show` should list the demanded series `BACKFILLING`, then `LIVE`.

## Checks

* `systemctl is-active xtnl-redis-md` → `active`; `ss -ltn '( sport = :6381 )'` lists only loopback.
* `ls -l /run/xtnl-redis-md/users.acl` → `-rw-r----- root redis`; `grep -c '>' /run/xtnl-redis-md/users.acl` → `0`
  (password hashes only).
* `$OPS md show`: `md generation` set, writer epoch = the leader's epoch (`epoch show`), every demanded series `LIVE`
  (or `GAPPED` while its market is closed), `rehash: complete`.
* Metrics (`metrics.md` "Market data and ARM"): `xtnl_md_series{state="LIVE"}`, `xtnl_md_redis_used_memory_ratio`
  below 0.7, `xtnl_md_shed_step` 0, `xtnl_md_aof_wait_timeouts_total` flat.
* Ingestion and Oversee are unchanged: `xtnl_ingest_events_total` rate and `xtnl_risksweep_oldest_pass_age_seconds`
  as before enabling.

## redis-md memory

`$OPS md show` (series count and states). Compare with the sizing in design §A5 (about 60 MB per symbol at full v1
retention; 8 symbols near 50 % of 1 GB). Causes: more demanded symbols than sized for, a stuck retention pass
(`xtnl_md_events_total{component="writer",event="retain_errors"}` rising), or the AOF rewrite fork. Fix: raise
`maxmemory` in `/etc/xtnl/redis-md.conf` and `MemoryMax` in the unit (always twice `maxmemory`), then
`sudo systemctl restart xtnl-redis-md` (series go GAPPED and are backfilled; TICK buckets of the gap become `blind`),
or remove symbols from the demand (`XTNL_TICK_SYMBOLS`, the ARM configuration). PC: `--maxmemory` and `mem_limit` in
`deploy/local/docker-compose.local.yml`. Never `FLUSHALL`, never `CONFIG SET` on a live node.

Retention (plan S38e): every retention pass (60 s) also runs `RETAIN_BROKER`, which keeps 1 year of every stored
BROKER series (demanded or not; longer only for a wider declared backfill) and 7 days of revisions and quarantine
entries, so memory settles at the §A5 sizing instead of growing; a series' bars older than a year are gone from
`redis-md` (they are re-fetchable from the broker only while its history reaches them). The demand book refuses a new
decision-grade symbol once the measured memory leaves no room below 50 %.

## redis-md down or loading at start-up

A `redis-md` that is not up yet, restarting, or still loading its AOF (`LOADING`) is **not** a failed bootstrap
(plan S38e): md-writer retries the bootstrap with back-off (1 s doubling to 30 s), series stay GAPPED, and nothing
pages. Only when it lasts longer than `XTNL_ARM_FAIL_MD_DOWN_S` (900 s) while an armed market is OPEN does the ARM fail
globally (`md_down`, see `arm.md`). Three bootstraps in a row that the store answered and refused
(`bootstrap_failed`) are the immediate fault: then "AOF aside" below.

## md requests (audited; allowed while the ARM is FAILED)

```
$OPS md backfill <series> --from-ms 1763020800000 --to-ms 1763021700000 --operator <you> --reason "<ticket>"   # first / last bucket start (epoch ms)
$OPS md rebuild <indicator_id> --operator <you> --reason "<ticket>"          # after "indicator faulted"
$OPS md writer-reset --operator <you> --reason "<ticket>"                     # md-writer circuit + bootstrap
$OPS md rehash --operator <you> --reason "<ticket>"                           # after a key rotation (below)
$OPS task reset md-indicators --operator <you> --reason "<ticket>"            # after "indicator threads hung"
```

Each is an `operator_requests` row the leader answers (applied / refused / moot); `--wait` waits for the answer.

## AOF aside (redis-md refuses to load its AOF)

`journalctl -u xtnl-redis-md -b` shows "Bad file format reading the append only file" (or similar) and the unit keeps
failing. `aof-load-truncated yes` already accepts a torn tail, so this is real damage. The store is rebuildable: BROKER
series are re-fetched from the broker; TICK history is lost (it is best effort, design §A1).

```
sudo systemctl stop xtnl-redis-md
sudo mv /var/lib/xtnl-redis-md/appendonlydir /var/lib/xtnl-redis-md/appendonlydir.aside-$(date -u +%Y%m%dT%H%M%SZ)
sudo systemctl start xtnl-redis-md             # starts empty
$OPS md show                                   # md-writer sees NO_BOOT, bootstraps (store_lost), md-bars backfills
```

PC: `docker compose -f deploy/local/docker-compose.local.yml --profile md stop redis-md`, then move the AOF out of
the volume with a throwaway container (`docker run --rm -v xtnl_local_redis_md:/data redis:7.4 sh -c 'mv
/data/appendonlydir /data/appendonlydir.aside'`) and start it again (`start-xtnl.ps1`). Keep the aside copy until
the series are `LIVE` again, then delete it. If an ARM-global FAILED opened while the store was down, clear it as
`arm.md` describes (its probe runs a trial bootstrap).

## HMAC key rotation (design §A13, R11-M13)

Every content hash stored in PostgreSQL carries the key id it was made under. A hash whose key is gone can no longer be
verified (`arm replay` reports `UNVERIFIABLE(key_retired)`), so the previous key is kept until the re-hash is done.

1. **Add the new key.** AWS: in the secret, set `XTNL_MD_HMAC_KEY_PREV` to the current key and `XTNL_MD_HMAC_KEY` to a
   new one (`openssl rand -hex 32`); in both env files set `XTNL_MD_HMAC_KEY_PREV_ID` to the current id and
   `XTNL_MD_HMAC_KEY_ID` to a higher one. Both nodes need the identical keyring. PC:
   `deploy/local/new-md-credentials.ps1 -RotateKey` (it prints the two ids to set). Restart the engine (standby
   first, then a switchover).
2. **Run the re-hash:** `$OPS md rehash --operator <you> --reason "<ticket>: key rotation"`. Members are rewritten
   under the new id; no `bar_revised` event, no quarantine, no FAILED, no CORRECTED.
3. **Wait** until `$OPS md show` reports `rehash: complete` **and then one more correction window** (`decision_cap`,
   the latest a decision can still be corrected) has passed.
4. **Retire the previous key:** remove `XTNL_MD_HMAC_KEY_PREV` from the secret (PC:
   `new-md-credentials.ps1 -RetirePrevKey`) and `XTNL_MD_HMAC_KEY_PREV_ID` from the env files; restart the engine
   the same way.

Never retire the previous key before step 3: stored hashes under it would read as retired, not as changed.

## redis-md password rotation (md_writer / md_reader)

The `redis-md` ACL is rendered only when `xtnl-redis-md` starts (`ExecStartPre`, `src.ops.md_acl`), while the engine
re-reads Secrets Manager without a restart (operations §13). After a password change in the secret the engine and the
store disagree until `redis-md` is restarted, and `md-writer` then fails authentication. Keep this order, one node at a
time, standby first, so the leader's writer never meets a store with other passwords:

1. **New URLs in the secret:** set `XTNL_MD_REDIS_URL=redis://md_writer:<pw>@127.0.0.1:6381/0` and
   `XTNL_MD_REDIS_READER_URL=redis://md_reader:<pw2>@127.0.0.1:6381/0` with two new, different random passwords.
   Nothing restarts yet; the store still holds the old hashes.
2. **Re-render the ACL on the standby:** `sudo systemctl restart xtnl-redis-md` (the new hashes; check
   `grep -c '>' /run/xtnl-redis-md/users.acl` → `0`).
3. **Restart the standby engine:** `sudo systemctl restart xtnl-engine` (it reads the new URLs).
4. **Switch over** (`failover-and-switchover.md`); the former leader is now the standby.
5. **Repeat steps 2 and 3 on the former leader.** Until then its `redis-md` still holds the old hashes; it is not
   written while it is the standby.
6. Give remote readers (the dashboard, through the TLS proxy on 6382) the new `md_reader` password.

PC: `deploy/local/new-md-credentials.ps1 -Rotate` (new passwords and ACL file), then restart `redis-md` and the engine
task, in that order.

## Disable

```
# engine first: XTNL_MD_ENABLED=0 (and XTNL_ARM_ENABLED=0) in /etc/xtnl/engine.env, restart standby, switch over
sudo systemctl disable --now xtnl-redis-md     # the data directory is kept; delete it only if the store is retired
```

PC: set the flag to `0` and restart the engine task; `start-xtnl.ps1` stops a `redis-md` container left running.

## Rollback

Disabling is the rollback: with `XTNL_MD_ENABLED=0` the engine builds nothing of the market-data layer, opens no
`redis-md` connection and behaves exactly as before. `redis-md` data needs no restore (BROKER series are re-fetched on
the next enable). An AOF moved aside can be moved back while the unit is stopped if the rebuilt store is worse. A key
rotation is rolled back by putting the previous key back as `XTNL_MD_HMAC_KEY` with its old id before it was retired.
