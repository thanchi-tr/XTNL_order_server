# Deployment assets (Phase 1)

Phase 1 is **read-only**: these assets deploy an engine that ingests broker state, runs Oversee and records
halts. Nothing deployed here sends, modifies or cancels orders or closes positions.

Topology and rules: `.claude/rules/operations.md`, architecture diagram page 5 (Deployment).
Two engine nodes in London (eu-west-2a, eu-west-2b) plus an etcd witness (eu-west-2c); disaster image,
dashboard, dead-man receiver and on-call in Melbourne.

| File | Installed as | Hosts |
|---|---|---|
| `systemd/xtnl-engine.service` | `/etc/systemd/system/xtnl-engine.service` | node A, node B |
| `systemd/xtnl-migrate.service` | `/etc/systemd/system/xtnl-migrate.service` (operator job) | node A, node B |
| `systemd/xtnl.tmpfiles.conf` | `/etc/tmpfiles.d/xtnl.conf` | node A, node B |
| `systemd/xtnl.sysusers.conf` | `/etc/sysusers.d/xtnl.conf` | node A, node B |
| `env/engine.env.example` | `/etc/xtnl/engine.env` (placeholders replaced from Secrets Manager) | node A, node B |
| `env/dashboard.env.example` | `/etc/xtnl/dashboard.env` (0640 root:xtnl; read-only PostgreSQL role + Redis ACL user) | Melbourne host |
| `etcd/etcd.conf.yml` | `/etc/etcd/etcd.conf.yml` | node A, node B, witness |
| `patroni/patroni.yml` | `/etc/patroni/patroni.yml` | node A, node B |
| `patroni/pgbackrest.conf` | `/etc/pgbackrest/pgbackrest.conf` | node A, node B, DR stack |
| `patroni/pgbackrest-cipher.conf.example` | `/etc/pgbackrest/conf.d/xtnl-cipher.conf` (0640 root:postgres; repo passphrase from Secrets Manager, read by every pgBackRest call: WAL archive / restore, PITR guard, backups, operator restores) | node A, node B, DR stack |
| `redis/redis.conf` | `/etc/redis/redis.conf` (loopback only; ACL users `xtnl_engine` and read-only `xtnl_dashboard`) | node A, node B |
| `redis/redis-tls-proxy.conf` + `systemd/xtnl-redis-tls-proxy.service` | `/etc/xtnl/redis-tls-proxy.conf` (`envsubst`, `NODE_IP`) + `/etc/systemd/system/`: stunnel, mutual TLS on `${NODE_IP}:6380` -> `127.0.0.1:6379`, for the Melbourne dashboard | node A, node B |
| `systemd/xtnl-pitr-guard.service`, `.timer` | `/etc/systemd/system/` (timer enabled) | node A, node B |
| `observability/xtnl-pitr-guard.sh` | `/usr/local/bin/xtnl-pitr-guard` (also the Patroni `on_role_change` callback) | node A, node B |
| `systemd/xtnl-pg-backup.service`, `.timer` + `jobs/xtnl-pg-backup.sh` | `/etc/systemd/system/` (timer enabled) + `/usr/local/bin/xtnl-pg-backup`: daily pgBackRest incremental, full on Saturday, primary only | node A, node B |
| `systemd/xtnl-etcd-defrag.service`, `.timer` + `jobs/xtnl-etcd-defrag.sh` | `/etc/systemd/system/` (timer enabled) + `/usr/local/bin/xtnl-etcd-defrag` + `/etc/xtnl/etcd-defrag.env`: weekly rolling defrag under an etcd lock, only while 3/3 healthy | node A, node B, witness |
| `observability/logs/30-xtnl-rsyslog.conf` | `/etc/rsyslog.d/30-xtnl.conf`: journald -> `/var/log/xtnl/<unit>.log` (raw JSON lines) | node A, node B, witness |
| `observability/logs/journald-xtnl.conf` | `/etc/systemd/journald.conf.d/xtnl.conf` (`ForwardToSyslog=yes`) | node A, node B, witness |
| `observability/logs/xtnl.logrotate` | `/etc/logrotate.d/xtnl` | node A, node B, witness |
| `observability/cloudwatch/xtnl-prometheus.yaml` | `/opt/aws/amazon-cloudwatch-agent/etc/xtnl-prometheus.yaml` (`envsubst`, `NODE_IP`; generated) | node A, node B |
| `env/dashboard-heartbeat.json.example` | `/etc/xtnl/dashboard/heartbeat.json` (0640 root:xtnl; `envsubst` `DASHBOARD_VPN_IP`, token = the engine's `XTNL_HEARTBEAT_COPY_TOKEN`): the leader heartbeat listener, https on `:8444` with `/etc/xtnl/dashboard/heartbeat.crt` / `.key` (0640 root:xtnl), inbound 8444 from node A and node B only | Melbourne host |
| `systemd/xtnl-dashboard.service` | `/etc/systemd/system/xtnl-dashboard.service`: the read-only dashboard (`python -m src.dashboard`) | Melbourne host |
| `systemd/xtnl-deadman-receiver.service` | `/etc/systemd/system/xtnl-deadman-receiver.service`: the dead-man receiver (`python -m src.deadman_receiver`) | Melbourne host |
| `env/deadman-receiver.env.example` | `/etc/xtnl/deadman-receiver.env` (0600 root; paging channel outside eu-west-2) | Melbourne host |
| `observability/metrics.md` | the metric contract the engine exports on `:9464/metrics` | - |
| `observability/prometheus/xtnl-alerts.rules.yml` | AMP rule groups namespace / Prometheus `rule_files` (generated) | monitoring account |
| `observability/alertmanager/alertmanager.yml` | AMP alert manager definition / Alertmanager config (`envsubst`) | monitoring account |
| `observability/cloudwatch/xtnl-alarms.json` | CloudFormation stack (SNS topic ARNs as parameters; generated) | monitoring account |
| `observability/cloudwatch/agent-witness.json` | CloudWatch agent config on the witness: CWAgent `disk_used_percent` of `/` and `/var/lib/etcd` {host=witness, path} for the witness disk alarms + its defrag log lines (generated) | witness |
| `observability/cloudwatch/agent-prometheus.json` | CloudWatch agent config on both engine nodes: EMF metrics + log shipping of `/var/log/xtnl/*.log` to CloudWatch Logs `/xtnl/engine`, `/xtnl/ops` (generated) | node A, node B |
| `docker-compose.dev.yml` | local development only | your workstation |

Templates use `${NAME}` placeholders (`NODE_A_IP`, `NODE_B_IP`, `WITNESS_IP`, `NODE_IP`, `ETCD_NAME`,
`ETCD_SELF_IP`, `ETCD_CLUSTER_TOKEN`, `PG_MEMBER_NAME`, `VPN_CIDR`, `PGBACKREST_BUCKET`, `DASHBOARD_VPN_IP`). Render them with
`envsubst` per host; no secret is ever written into a template.

Timing chain that every file must respect (asserted by `tests/deploy/test_deploy_assets.py`):

| Setting | Value | Where |
|---|---|---|
| engine heartbeat (`WATCHDOG=1`) | every 1 s | `src/runtime/supervisor.py` `LoopWatchdog` |
| systemd watchdog | `WatchdogSec=3`, `WatchdogSignal=SIGKILL` | `xtnl-engine.service` |
| self-fence | 12 s without renewal | `src/runtime/lease.py` |
| etcd lease | TTL 15 s, renew 1 s | `src/runtime/lease.py` |
| etcd raft | heartbeat 250 ms, election 2500 ms | `etcd.conf.yml` |
| DRAINING deadline | 10 s (systemd `TimeoutStopSec=20`) | `lease.py`, unit |
| Patroni | ttl 15, loop_wait 5, retry_timeout 5 | `patroni.yml` |
| `idle_in_transaction_session_timeout` | 10 s (5 s statement_timeout + 10 s <= MAX_TXN_MS 15 s) | `patroni.yml`, `src/integrity/worker.py` |

Off-node alerting (operations.md §9): `observability/gen_alerts.py` generates the Prometheus rules, the
CloudWatch alarms and the agent declarations from one table; run it after any change and commit the outputs
(`python deploy/observability/gen_alerts.py --check` fails when they are stale). Every alert maps to a section
of `docs/runbooks/off-node-alerts.md`.

Runbooks: `docs/runbooks/` (start with `install-node.md`).

## Running the integration tests locally

The unit suite needs no servers. The PostgreSQL and Redis integration tests
(`tests/db/test_pg_integration.py`, `tests/cache/test_redis_integration.py`) are skipped unless
`ORDER_SERVER_TEST_DSN` / `ORDER_SERVER_TEST_REDIS` point at a **disposable** server: they truncate the
engine tables and flush the selected Redis database.

1. Start the throwaway containers (Docker Desktop or Docker Engine with Compose v2):

   ```
   docker compose -f deploy/docker-compose.dev.yml up -d --wait
   ```

2. Point the tests at them and run (PowerShell):

   ```
   $env:ORDER_SERVER_TEST_DSN = "postgresql://xtnl:xtnl_dev_only@127.0.0.1:55432/xtnl_test"
   $env:ORDER_SERVER_TEST_REDIS = "redis://127.0.0.1:56379/15"
   .venv/Scripts/python.exe -m pytest -q tests/db/test_pg_integration.py tests/cache/test_redis_integration.py
   ```

   Git Bash / Linux:

   ```
   ORDER_SERVER_TEST_DSN=postgresql://xtnl:xtnl_dev_only@127.0.0.1:55432/xtnl_test \
   ORDER_SERVER_TEST_REDIS=redis://127.0.0.1:56379/15 \
   .venv/Scripts/python.exe -m pytest -q tests/db/test_pg_integration.py tests/cache/test_redis_integration.py
   ```

   The PostgreSQL test applies the migrations itself (`src.db.migrate.migrate`). To apply them by hand:
   `ORDER_SERVER_DSN=<the test DSN> .venv/Scripts/python.exe -m src.db.migrate`.

3. Tear down (the data lives on tmpfs and is discarded):

   ```
   docker compose -f deploy/docker-compose.dev.yml down -v
   ```

The single etcd member (`http://127.0.0.1:52379`) is for manual experiments with the lease keeper; it is
not a quorum and has no TLS. Never reuse these ports, users or passwords anywhere else.
