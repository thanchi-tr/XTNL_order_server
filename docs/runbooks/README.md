# XTNL Phase 1 runbooks

**Phase 1 never sends orders.** The engine reads broker state, normalises it, stores it, runs Oversee and
records halts. No runbook step places, modifies or cancels an order or closes a position, and no step may
be improvised to do so. A halt is recorded and alerted; acting on it at the broker is a human decision taken
in the broker's own platform.

| Runbook | Use when |
|---|---|
| [install-node.md](install-node.md) | Building or rebuilding an engine node, the witness, or a unit in a crash loop |
| [failover-and-switchover.md](failover-and-switchover.md) | Planned switchover, rolling upgrade, node / zone / standby / etcd member loss |
| [safe-hold.md](safe-hold.md) | Page "SAFE_HOLD set" (anti-flap brake) |
| [redis-loss.md](redis-loss.md) | Redis restarted or lost on the leader (SM8 rebuild) |
| [dlq-quarantine.md](dlq-quarantine.md) | Page "DLQ row quarantined" / "feed entry quarantined", non-empty spill file |
| [auth-failed-rearm.md](auth-failed-rearm.md) | Page "adapter AUTH_FAILED" |
| [deadman-alert.md](deadman-alert.md) | Melbourne dead-man page (silence or a threshold) |
| [disaster-restore-melbourne.md](disaster-restore-melbourne.md) | PostgreSQL damage on both members, or region loss |
| [live-readonly-smoke-test.md](live-readonly-smoke-test.md) | First contact with real accounts using read-only credentials |
| [halt-clear.md](halt-clear.md) | Page "HALT HARD" / a HARD latch to review and clear |
| [ruleset-change.md](ruleset-change.md) | A reviewed Oversee threshold change, an invalid active ruleset, or a rollback |
| [partition-archive.md](partition-archive.md) | Lifecycle pages: partitions awaiting export, export count mismatch, disk alerts |
| [broker-maintenance.md](broker-maintenance.md) | A broker announces maintenance: declare the window (`/etc/xtnl/maintenance.json`, identical on both nodes) |
| [downtime-catchup.md](downtime-catchup.md) | Single-PC mode: page "downtime catch-up" / "downtime catch-up incomplete", reading `uptime show` / `catchup show` |
| [single-pc-autostart.md](single-pc-autostart.md) | Single-PC mode: start the engine at log-on (scheduled task `XTNL engine`), start / stop / status, reboot / sleep / update behaviour, removal |
| [order-server-api.md](order-server-api.md) | Single-PC mode: the read-only HTTPS API for the thesis website (Pine Script order history): credentials, firewall / router / DNS steps you run yourself, Vercel settings, rotation, closing it again |
| [live-view.md](live-view.md) | Single-PC mode: the local read-only live view (`http://127.0.0.1:8090/`): migration 0021, its login (`new-liveview-credentials.ps1`), rotation, exit 78 / `db: down`, turning it off |
| [home-backup-restore.md](home-backup-restore.md) | Single-PC home server: the nightly encrypted PostgreSQL + partition-archive backup to B2 (task `XTNL backup`, account `xtnl-backup`), Healthchecks.io check C red, backup exit codes, the restore and the quarterly drill |
| [deadman-receiver-home.md](deadman-receiver-home.md) | Single-PC home server: the dead-man receiver on a small VPS over Tailscale (unit, env file, `/healthz` timer = Healthchecks.io check B), and the engine's own page "dead-man receiver not acknowledging" |
| [pinescript-export.md](pinescript-export.md) | Draw the past week's order life cycles on a TradingView chart (`python -m src.ops pinescript`) |
| [marketdata.md](marketdata.md) | Optional market-data store `redis-md` and its tasks: enable / disable on AWS, the md pages (memory, broker bars, indicators), AOF aside, HMAC key rotation, redis-md password rotation |
| [arm.md](arm.md) | Optional Automated Risk Manager (advice only): every ARM page with its fix, FAILED pings (probe kinds, the command to run first, clear), configuration, pause / disarm, enable / disable |
| [off-node-alerts.md](off-node-alerts.md) | Any off-node (Alertmanager / CloudWatch -> SNS) alert; balance-move cash-flow refresh |

Conventions:

* Hosts: node A (eu-west-2a), node B (eu-west-2b), witness (eu-west-2c). Access via SSM / VPN only.
* `$PSQL` = `psql "$ORDER_SERVER_DSN"` run as an operator role (read-only role for diagnostics).
* `ETCDCTL` = `etcdctl --endpoints=https://$NODE_A_IP:2379,https://$NODE_B_IP:2379,https://$WITNESS_IP:2379 --cacert=/etc/etcd/pki/ca.crt`.
* Times in the schema are epoch milliseconds (`now_ms()` in SQL).
* Operator actions (SAFE_HOLD resume, re-arm adapter auth, `reset_task`, DLQ requeue / quarantine release,
  halt clear / acknowledge, account release, cash-flow refresh, epoch adoption, ruleset add / activate,
  archive forget) must be **authenticated and audited** (operations.md s16). They go through the
  operator CLI, never through hand-written SQL:
  ```
  cd /opt/xtnl/order-server
  OPS=".venv/bin/python -m src.ops"     # ORDER_SERVER_DSN = your operator login; XTNL_ETCD_ENDPOINTS = all 3 members
  $OPS safe-hold status | epoch show | halt status | dlq list | account status | archive list | audit tail  # reads
  $OPS <group> <command> ... --operator <you> --reason "<ticket + why>" [--approver <second person>]
  ```
  * Every change and its `operator_audit` row commit in **one transaction**; a refused command is audited
    too. The PostgreSQL login is recorded as the authenticated identity (`db_user`).
  * Engine-state changes are fenced on the current `engine_epoch` (operations.md s2): if a leader bumps the
    epoch mid-command, the command is refused (`epoch_moved`) and nothing changes; re-check and re-run.
  * Two-person control (`--approver`, a different person) is required for `safe-hold resume`,
    `epoch adopt`, `halt clear --force`, `ruleset activate` and `archive forget`; record both names in the
    ticket.
  * `task reset`, `account release` and `cash-flows refresh` queue an `operator_requests` row for the leader
    of the current epoch (consumed in-process); a request of an older epoch is moot.
  * `ruleset` commands write the operator-owned `rulesets` table (the engine only reads it), so they are
    not epoch-fenced; they run under a table lock and are audited like every other change.
  * Exit codes: 0 done (or no-op), 1 error (PostgreSQL unreachable, COMMIT outcome unknown: run
    `$OPS audit tail` before re-running), 2 usage, 3 refused (the output names why), 4 applied but the etcd
    SAFE_HOLD mirror was not written.
  * `operator_audit` is append-only (trigger; UPDATE / DELETE revoked from the engine role). Review it with
    `$OPS audit tail --limit 50 [--command safe_hold.resume]`.
* Never print, paste or log a credential. Never read or edit `.env` files on a node; the environment comes
  from `/etc/xtnl/engine.env` rendered from Secrets Manager. The CLI prints the DSN only redacted.
