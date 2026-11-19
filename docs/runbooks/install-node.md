# Install an engine node

Phase 1 never sends orders: the node installed here only reads broker state.

## Trigger

New node, rebuilt node after `patronictl reinit` / instance replacement, or alert "standby not ready"
(peer health record absent > 60 s, or the unit is `failed` after a crash loop).

## Prerequisites

* Ubuntu 24.04 LTS, c7i.2xlarge-class (engine nodes) or a small non-burstable instance (witness, etcd only).
* chrony using Amazon Time Sync (`server 169.254.169.123 prefer iburst`), `chrony-wait.service` enabled,
  `makestep` disabled after boot (never stepped).
* Security groups: 2379/2380 between the three nodes only; 5432/8008 between A and B; 5432 and 6380 (the
  dashboard's Redis TLS proxy) from `VPN_CIDR` (Melbourne) to A and B; 6382 (the redis-md TLS proxy section) from
  `VPN_CIDR` to A and B **only once redis-md is enabled** (`marketdata.md` "Enable" step 0); no inbound internet.
* **Redis 7.4** (engine nodes; the witness needs none) from `packages.redis.io`, pinned, **not** Ubuntu 24.04's own
  `redis-server` package (7.0.x). Redis 7.2 or newer is required: the optional `redis-md` store's ACL grants
  `+waitaof` and `+client|setinfo`, which Redis 7.0 rejects, so `xtnl-redis-md` would fail at every start. Install it
  before step 3 (the release notes name the pinned 7.4 build; `apt-cache madison redis-server` lists the builds):

  ```
  curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
  echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb noble main"        | sudo tee /etc/apt/sources.list.d/redis.list
  REDIS_PKG_VERSION=6:7.4.<patch>-1rl1~noble1        # the pinned 7.4 build of this release
  sudo apt-get update && sudo apt-get install -y redis-server=$REDIS_PKG_VERSION redis-tools=$REDIS_PKG_VERSION
  sudo apt-mark hold redis-server redis-tools        # no unattended upgrade past the pin
  redis-server --version                             # must print v=7.4.x (7.2 or newer); stop here otherwise
  ```
* Witness: etcd data on its own gp3 volume mounted at `/var/lib/etcd` (operations.md §1; its disk alarms watch
  `/` and `/var/lib/etcd`).
* Secrets in AWS Secrets Manager; certificates from the internal CA (etcd, Patroni, PostgreSQL).
* etcd client certificates for every etcd client before `client-cert-auth` is turned on in
  `deploy/etcd/etcd.conf.yml`: the engine (`XTNL_ETCD_CERT` / `XTNL_ETCD_KEY`) and Patroni (`etcd3.cert` /
  `etcd3.key` in `deploy/patroni/patroni.yml`, `/etc/patroni/pki/<member>-etcd-client.{crt,key}`).
  Patroni's pair must be in place **before Patroni first starts**, even while `client-cert-auth` is still
  off: `patroni.yml` always names it, and Patroni cannot reach etcd when the files are missing.
* `XTNL_ARCHIVE_DIR` (`/var/lib/xtnl/archive`, created by `xtnl.tmpfiles.conf`) for the partition export
  (`partition-archive.md`).

## Commands (engine node)

```
# 1. user, directories, code
sudo install -m 0644 deploy/systemd/xtnl.sysusers.conf /etc/sysusers.d/xtnl.conf && sudo systemd-sysusers
sudo mkdir -p /opt/xtnl && sudo cp -r <release> /opt/xtnl/order-server
cd /opt/xtnl/order-server && sudo python3.12 -m venv .venv && sudo .venv/bin/pip install .
sudo install -m 0644 deploy/systemd/xtnl.tmpfiles.conf /etc/tmpfiles.d/xtnl.conf && sudo systemd-tmpfiles --create

# 2. environment: render from Secrets Manager (never copy the example's placeholders into production)
sudo install -m 0640 -o root -g xtnl /dev/null /etc/xtnl/engine.env   # then fill it via the provisioning job
sudo systemd-run --wait --pipe --uid=xtnl --gid=xtnl -p EnvironmentFile=/etc/xtnl/engine.env \
     -p WorkingDirectory=/opt/xtnl/order-server /opt/xtnl/order-server/.venv/bin/python -m src.main --check-config

# 3. etcd, Patroni, pgBackRest, Redis: render the templates in deploy/ with envsubst.
#    pgBackRest BEFORE Patroni starts (archive_command / restore_command / replica restore use it at once).
#    The repo is encrypted: the passphrase goes ONLY into the include file every pgBackRest call reads (WAL
#    archive-push / archive-get under Patroni, the PITR guard, the backup job, operator commands). Never as
#    PGBACKREST_REPO1_CIPHER_PASS in an environment (it would override the file for one caller only).
sudo install -d -m 0755 /etc/pgbackrest /etc/pgbackrest/conf.d
sudo install -d -m 0750 -o postgres -g postgres /var/spool/pgbackrest /var/log/pgbackrest   # archive-async spool, logs
#    redis.conf: replace BOTH ACL placeholders (xtnl_engine and xtnl_dashboard, '#<sha256 hex>' from Secrets
#    Manager) before starting redis-server; Redis refuses to start while either placeholder is present.
PGBACKREST_BUCKET=<bucket> envsubst '${PGBACKREST_BUCKET}' < deploy/patroni/pgbackrest.conf \
     | sudo tee /etc/pgbackrest/pgbackrest.conf >/dev/null
sudo install -m 0640 -o root -g postgres /dev/null /etc/pgbackrest/conf.d/xtnl-cipher.conf
     # then fill it from deploy/patroni/pgbackrest-cipher.conf.example via the provisioning job (Secrets Manager)
sudo systemctl enable --now etcd patroni redis-server
sudo -u postgres pgbackrest --stanza=xtnl check     # the first member only after `stanza-create` (pgbackrest.conf)

# 3b. certificate expiry / readability (K5, operations.md s13): the engine's exporter reads every certificate this
#     node serves or presents at the path its service loads (engine.env XTNL_METRICS_CERTS: etcd-a / pg-a on node A,
#     etcd-b / pg-b on node B). Certificates are public: 0644, directories traversable; private keys stay 0600 and
#     owned by their service. Re-apply after every renewal (or have the renewal job install with these modes).
ETCD_NAME=etcd-a PG_MEMBER_NAME=pg-a                 # node B: etcd-b, pg-b
sudo chmod 0755 /etc/etcd/pki /etc/patroni/pki
sudo chmod 0644 /etc/etcd/pki/ca.crt /etc/etcd/pki/$ETCD_NAME.crt /etc/etcd/pki/$ETCD_NAME-peer.crt      /etc/patroni/pki/ca.crt /etc/patroni/pki/$PG_MEMBER_NAME.crt /etc/patroni/pki/$PG_MEMBER_NAME-pg.crt      /etc/patroni/pki/$PG_MEMBER_NAME-etcd-client.crt
sudo chmod 0600 /etc/etcd/pki/*.key /etc/patroni/pki/*.key
for f in $(sudo sed -n 's/^XTNL_METRICS_CERTS=//p' /etc/xtnl/engine.env | tr ',' '
' | cut -d= -f2); do
  sudo -u xtnl test -r "$f" || echo "NOT READABLE by xtnl: $f"     # must print nothing (after step 7 too)
done

# 4. units
sudo install -m 0644 deploy/systemd/xtnl-engine.service deploy/systemd/xtnl-migrate.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start xtnl-migrate.service            # operator job; idempotent; only before a switchover
# it applies only up to XTNL_MIGRATIONS_ACCEPTED_UPTO (default 14, /etc/xtnl/engine.env); later ones are held back and
# named in one journal line (plan S38f, design §0b); raise it to 20 only once 0015-0020 are reviewed
sudo systemctl enable --now xtnl-engine.service

# 5. scheduled jobs (operations.md s1, s8): PITR guard, daily pgBackRest backup (primary only), rolling etcd defrag
sudo install -m 0755 deploy/observability/xtnl-pitr-guard.sh /usr/local/bin/xtnl-pitr-guard
sudo install -m 0755 deploy/jobs/xtnl-pg-backup.sh /usr/local/bin/xtnl-pg-backup
sudo install -m 0755 deploy/jobs/xtnl-etcd-defrag.sh /usr/local/bin/xtnl-etcd-defrag
sudo install -m 0644 deploy/systemd/xtnl-pitr-guard.{service,timer} deploy/systemd/xtnl-pg-backup.{service,timer} \
     deploy/systemd/xtnl-etcd-defrag.{service,timer} /etc/systemd/system/
sudo install -m 0640 -o root -g etcd /dev/null /etc/xtnl/etcd-defrag.env      # see deploy/jobs/xtnl-etcd-defrag.sh
sudo systemctl daemon-reload
sudo systemctl enable --now xtnl-pitr-guard.timer xtnl-pg-backup.timer xtnl-etcd-defrag.timer

# 6. off-node logs and the CloudWatch metrics path (operations.md s9)
sudo install -m 0644 deploy/observability/logs/journald-xtnl.conf -D /etc/systemd/journald.conf.d/xtnl.conf
sudo install -m 0644 deploy/observability/logs/30-xtnl-rsyslog.conf /etc/rsyslog.d/30-xtnl.conf
sudo install -m 0644 deploy/observability/logs/xtnl.logrotate /etc/logrotate.d/xtnl
sudo install -d -m 0750 -o syslog -g adm /var/log/xtnl
sudo usermod -aG adm cwagent          # the agent runs as cwagent and reads /var/log/xtnl/*.log (0640 syslog:adm)
sudo systemctl restart systemd-journald rsyslog
NODE_IP=<this node's VPC ip> envsubst '${NODE_IP}' < deploy/observability/cloudwatch/xtnl-prometheus.yaml \
     | sudo tee /opt/aws/amazon-cloudwatch-agent/etc/xtnl-prometheus.yaml >/dev/null
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s \
     -c file:/opt/xtnl/order-server/deploy/observability/cloudwatch/agent-prometheus.json

# 7. Redis TLS proxy for the Melbourne dashboard (operations.md s9: read-only over the VPN). Redis stays on
#    loopback; stunnel accepts mutual TLS on ${NODE_IP}:6380. Set the xtnl_dashboard ACL password hash in
#    /etc/redis/redis.conf (step 3) from Secrets Manager. Server certificate from the internal CA with this node's
#    VPC address as an IP SAN; client-ca.crt = the CA that issues the dashboard's client certificate.
sudo apt-get install -y stunnel4
#    The two certificates are 0644 (public; the exporter watches them, XTNL_METRICS_CERTS, K5); the key stays 0600.
sudo install -d -m 0755 /etc/xtnl/redis-tls-proxy
sudo install -m 0644 <server certificate> /etc/xtnl/redis-tls-proxy/tls.crt
sudo install -m 0600 <its private key> /etc/xtnl/redis-tls-proxy/tls.key
sudo install -m 0644 <internal CA certificate> /etc/xtnl/redis-tls-proxy/client-ca.crt
NODE_IP=<this node's VPC ip> envsubst '${NODE_IP}' < deploy/redis/redis-tls-proxy.conf \
     | sudo tee /etc/xtnl/redis-tls-proxy.conf >/dev/null
sudo install -m 0644 deploy/systemd/xtnl-redis-tls-proxy.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now xtnl-redis-tls-proxy

# 7b. optional market-data store redis-md (design §A2): INSTALL ONLY, do not enable. It is enabled together with
#     XTNL_MD_ENABLED=1 by marketdata.md "Enable"; until then the engine never connects to it. The proxy's second
#     section (${NODE_IP}:6382 -> 127.0.0.1:6381) is already in the file rendered above.
sudo install -m 0644 deploy/redis/redis-md.conf /etc/xtnl/redis-md.conf
sudo install -m 0644 deploy/systemd/xtnl-redis-md.service /etc/systemd/system/
sudo systemctl daemon-reload                    # no enable / start here

# 8. declared broker maintenance windows: the same file on BOTH engine nodes (broker-maintenance.md)
sudo install -m 0640 -o root -g xtnl /dev/null /etc/xtnl/maintenance.json     # empty = no window declared
```

The instance role needs `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`,
`logs:PutRetentionPolicy` on `/xtnl/*` and `cloudwatch:PutMetricData` (namespace `XTNL`), nothing else.

## Commands (witness)

The witness runs chrony, its etcd member (`deploy/etcd/etcd.conf.yml` with `ETCD_NAME=etcd-w`, data on the
`/var/lib/etcd` volume), the rolling defrag job, log shipping and the CloudWatch agent with the **witness**
config (disk alarms 70 / 85 %, operations.md §11). No engine, no PostgreSQL, no Redis, no exporter. Copy the
release's `deploy/` directory to `/opt/xtnl/order-server/deploy` (no Python environment is needed).

```
# chrony as in the prerequisites; etcd (render deploy/etcd/etcd.conf.yml with ETCD_NAME=etcd-w)
sudo systemctl enable --now etcd
# rolling defrag (operations.md s1): the same job as on the engine nodes, under the cluster-wide etcd lock
sudo install -d -m 0750 /etc/xtnl
sudo install -m 0755 deploy/jobs/xtnl-etcd-defrag.sh /usr/local/bin/xtnl-etcd-defrag
sudo install -m 0644 deploy/systemd/xtnl-etcd-defrag.service deploy/systemd/xtnl-etcd-defrag.timer /etc/systemd/system/
sudo install -m 0640 -o root -g etcd /dev/null /etc/xtnl/etcd-defrag.env      # see deploy/jobs/xtnl-etcd-defrag.sh
sudo systemctl daemon-reload && sudo systemctl enable --now xtnl-etcd-defrag.timer
# off-node logs (a failed defrag's crit line leaves the node) and the witness disk metrics
sudo install -m 0644 deploy/observability/logs/journald-xtnl.conf -D /etc/systemd/journald.conf.d/xtnl.conf
sudo install -m 0644 deploy/observability/logs/30-xtnl-rsyslog.conf /etc/rsyslog.d/30-xtnl.conf
sudo install -m 0644 deploy/observability/logs/xtnl.logrotate /etc/logrotate.d/xtnl
sudo install -d -m 0750 -o syslog -g adm /var/log/xtnl
sudo usermod -aG adm cwagent
sudo systemctl restart systemd-journald rsyslog
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s \
     -c file:/opt/xtnl/order-server/deploy/observability/cloudwatch/agent-witness.json
```

`agent-witness.json` publishes CWAgent `disk_used_percent` with the dimensions `host=witness` and `path` (`/`,
`/var/lib/etcd`): exactly what the `xtnl-witness-disk-*` alarms read. Checks: `systemctl list-timers
xtnl-etcd-defrag`; the CloudWatch metric `CWAgent / disk_used_percent {host=witness, path=/var/lib/etcd}` appears
within 2 min; the log group `/xtnl/ops` gets a stream `<hostname>/xtnl-etcd-defrag` after the first Saturday run.

The witness's instance role differs from the engine nodes' in one place: `cloudwatch:PutMetricData` for namespace
**`CWAgent`** (not `XTNL`), plus the same `logs:*` actions on `/xtnl/*`. A role limited to `XTNL` drops every
witness disk sample; the 70 % alarms then go to ALARM (missing data counts as breaching), which is how it shows.

## Dashboard role (once per cluster, after migration 0011)

Migration `0011_dashboard_role.sql` creates `xtnl_dashboard` NOLOGIN with SELECT on the dashboard's tables only
(needs a migration DSN with CREATEROLE, `/etc/xtnl/migrate.env`). Enable login with a password from Secrets
Manager (never typed into a file or a shell history):

```
$PSQL -c "ALTER ROLE xtnl_dashboard LOGIN"
$PSQL -c "\password xtnl_dashboard"
$PSQL -c "SELECT rolname, rolcanlogin, rolconfig FROM pg_roles WHERE rolname = 'xtnl_dashboard'"
```

The dashboard refuses any role that can write a table (`verify_read_only_connection`).

## Melbourne dead-man receiver

On the Melbourne host (never in London), with the code in `/opt/xtnl/order-server` as above:

```
sudo install -d -m 0750 /etc/xtnl /etc/xtnl/deadman-receiver
sudo install -m 0600 /dev/null /etc/xtnl/deadman-receiver.env   # from deploy/env/deadman-receiver.env.example
sudo install -m 0600 <server certificate issued by the internal CA> /etc/xtnl/deadman-receiver/tls.crt
sudo install -m 0600 <its private key> /etc/xtnl/deadman-receiver/tls.key
sudo install -m 0644 deploy/systemd/xtnl-deadman-receiver.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now xtnl-deadman-receiver
journalctl -u xtnl-deadman-receiver -n 20     # "listening on ...", then the "receiver started" page arrives
```

The token equals the engine's `XTNL_DEADMAN_TOKEN`; keep it in the receiver's own Secrets Manager secret in
ap-southeast-4 (`XTNL_SECRETS_SOURCE=aws-sm`, the host's instance role needs `secretsmanager:GetSecretValue` on it)
so it rotates without a restart (`deadman-alert.md` "Token rotation"). The engine's `XTNL_DEADMAN_URL` is
`https://<receiver VPN name>:8443/xtnl/heartbeat` and the certificate must be valid for that name.
The engine's heartbeat client (httpx) verifies it against certifi's public roots unless the engine env sets
`SSL_CERT_FILE` to a bundle that contains the internal CA (`/etc/xtnl/pki/ca-bundle.crt`, engine.env.example):
without it every POST fails the TLS check and the receiver pages "no heartbeat" although the engine is healthy.
Check from each engine node: `curl --cacert /etc/xtnl/pki/ca-bundle.crt https://<receiver VPN name>:8443/healthz`
prints `ok`.
`--check-config` exit 78: a channel in eu-west-2, no channel, a short token or missing TLS files.

## Melbourne dashboard host

The read-only dashboard (`python -m src.dashboard`, operations.md §9) runs on the Melbourne host (never in
London), with the code in `/opt/xtnl/order-server` as in step 1 and the `xtnl` user (`xtnl.sysusers.conf`). It
reaches PostgreSQL as `xtnl_dashboard` (above) and each node's Redis through that node's TLS proxy (step 7) as
the ACL user `xtnl_dashboard`, both over the VPN.

```
# 0751: other users may traverse but not list, so the dead-man receiver (a DynamicUser) can read the public
# Redis client certificate for its expiry check (K5, XTNL_RECEIVER_CERTS); keys, tokens and env stay 0640.
sudo install -d -m 0751 -o root -g xtnl /etc/xtnl /etc/xtnl/dashboard
sudo install -m 0640 -o root -g xtnl /dev/null /etc/xtnl/dashboard.env   # from deploy/env/dashboard.env.example
sudo install -m 0640 -o root -g xtnl <internal CA certificate> /etc/xtnl/dashboard/ca.crt
sudo install -m 0644 -o root -g xtnl <client certificate issued by the CA> /etc/xtnl/dashboard/redis-client.crt
sudo install -m 0640 -o root -g xtnl <its private key> /etc/xtnl/dashboard/redis-client.key
# Leader heartbeat listener (https :8444): a server certificate issued by the internal CA, valid for the name in
# the engine's XTNL_HEARTBEAT_COPY_URL, and the settings file (token = the engine's XTNL_HEARTBEAT_COPY_TOKEN).
sudo install -m 0640 -o root -g xtnl <server certificate for the dashboard VPN name> /etc/xtnl/dashboard/heartbeat.crt
sudo install -m 0640 -o root -g xtnl <its private key> /etc/xtnl/dashboard/heartbeat.key
DASHBOARD_VPN_IP=<this host's VPN address> envsubst '${DASHBOARD_VPN_IP}' < deploy/env/dashboard-heartbeat.json.example | sudo install -m 0640 -o root -g xtnl /dev/stdin /etc/xtnl/dashboard/heartbeat.json
sudoedit /etc/xtnl/dashboard/heartbeat.json              # "token": the engine's XTNL_HEARTBEAT_COPY_TOKEN (>= 16 chars)
sudo -u xtnl test -r /etc/xtnl/dashboard/heartbeat.json && sudo -u xtnl test -r /etc/xtnl/dashboard/heartbeat.crt && sudo -u xtnl test -r /etc/xtnl/dashboard/heartbeat.key && echo "xtnl can read the heartbeat files"
sudo install -m 0644 deploy/systemd/xtnl-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now xtnl-dashboard
# ExecStartPre runs --check-config as xtnl: it reads heartbeat.json and loads heartbeat.crt / .key (exit 78 if not)
journalctl -u xtnl-dashboard -n 20       # listening on 127.0.0.1:8088 and "leader heartbeat listener on <ip>:8444/xtnl/heartbeat (https)"
curl -s http://127.0.0.1:8088/api/live | head -c 300     # "stale": false and "source": "<leader node>"
```

Firewall: the dashboard host's security group allows inbound TCP **8444 from node A and node B only**
(`${NODE_A_IP}/32`, `${NODE_B_IP}/32`; plus any host firewall), never from `VPN_CIDR` as a whole or the internet.
On both engine nodes `XTNL_HEARTBEAT_COPY_URL=https://<dashboard VPN name>:8444/xtnl/heartbeat` and
`XTNL_HEARTBEAT_COPY_TOKEN` = the file's token (both follow the engine's secrets source; `SSL_CERT_FILE` must hold
the internal CA, `engine.env.example`). Check from **each** engine node (proves the route, the security group and
the certificate; 401 because no token is sent):
`curl -s -o /dev/null -w '%{http_code}\n' --cacert /etc/xtnl/pki/ca-bundle.crt -X POST https://<dashboard VPN name>:8444/xtnl/heartbeat`
prints `401`. A timeout = security group / route; a TLS error = certificate name or CA.

Live values marked stale with "no leader heartbeat listener configured": `heartbeat.json` is missing (the
dashboard starts without a listener). "no leader heartbeat received yet" or "latest leader heartbeat ... s old":
the leader's copies do not arrive (the curl check above, the token, `journalctl -u xtnl-engine | grep "heartbeat
copy"`). `--check-config` exit 78 with "heartbeat ...": the file is not readable by `xtnl`, not valid JSON, a
short / placeholder token, or a certificate / key that cannot be loaded.

`pg_hba` needs `hostssl xtnl xtnl_dashboard ${VPN_CIDR} scram-sha-256` (Patroni `pg_hba`). Live values marked
stale with "unreachable" for both nodes: check the proxy, the security group (6380 from `VPN_CIDR`) or the client
certificate; with "boot epoch N" for both: SM8 has not rebuilt the leader's Redis yet (`redis-loss.md`).
`--check-config` exit 78: no `XTNL_DASHBOARD_DSN`, or an invalid value.

## Checks / expected outcome

* `systemctl show xtnl-engine -p Type -p WatchdogUSec -p Restart` -> `notify`, `3s`, `always`.
* `journalctl -u xtnl-engine -n 50` shows `node <id> starting (uvloop, BROKER_ENV=sandbox|live_readonly)`.
* `$ETCDCTL get --prefix /xtnl/health/` lists a fresh record for this node (< 5 s old).
* `$ETCDCTL endpoint health` -> 3/3 healthy; `patronictl list` -> one Leader, one Sync Standby.
* The node stays STANDBY if a peer is leader; that is correct.
* `systemctl list-timers 'xtnl-*'` shows the PITR guard (every minute), `xtnl-pg-backup` (daily) and
  `xtnl-etcd-defrag` (Saturday); `journalctl -t xtnl-pg-backup -n 5` on the primary shows `backup done`.
* WAL archiving works with the encrypted repo on both members: `sudo -u postgres pgbackrest --stanza=xtnl check`
  succeeds, and `SELECT archived_count, failed_count, last_archived_time FROM pg_stat_archiver` shows
  `last_archived_time` within the last 60 s (`archive_timeout`) and no rising `failed_count`.
* `systemctl is-active xtnl-redis-tls-proxy` -> `active`; `ss -ltn '( sport = :6379 )'` lists only loopback.
* `.venv/bin/python -m src.ops maintenance check` -> `outcome: ok`, the same `sha256` on node A and node B.
* `ls /var/log/xtnl/` shows `xtnl-engine.log` growing; the CloudWatch log group `/xtnl/engine` has a stream
  `<hostname>/xtnl-engine`.
* Engine PostgreSQL connections carry client TCP keepalives (5 s idle, 1 s interval, 3 probes, plus
  `TCP_USER_TIMEOUT` 8 s): `ss -tno state established '( dport = :5432 )'` shows `timer:(keepalive,...)`.
* `--check-config` exit 78 = config refused (BROKER_ENV not sandbox/live_readonly, fewer than three etcd
  endpoints, no XTNL_DEADMAN_URL). Fix the env file; systemd does not restart on 78.

## Crash loop

`systemctl status xtnl-engine` shows `start-limit-hit` after 20 starts in 15 min. Read
`journalctl -u xtnl-engine -b` for the cause, fix it, then `sudo systemctl reset-failed xtnl-engine && sudo
systemctl start xtnl-engine`.

## Rollback

`sudo systemctl disable --now xtnl-engine` on the new node (the peer stays or becomes leader). Restore the
previous `/opt/xtnl/order-server` release directory and start again. Never roll back a migration: schema
changes are expand / contract, so the previous release runs on the new schema.
