# Order-server API for the thesis website (single-PC mode)

`python -m src.api` is a small read-only HTTPS API that lets the XTNL thesis website (Vercel) ask this PC for
**this week's Pine Script order history** (the same scripts as `python -m src.ops pinescript`, see
[pinescript-export.md](pinescript-export.md)). Phase 1 never sends orders: the API has no broker access and no
order code, and it reads PostgreSQL in READ ONLY transactions only.

It is the **only** network-facing listener of the server. PostgreSQL, Redis, etcd and the metrics endpoint stay
bound to 127.0.0.1. Nothing is reachable from the internet until **you** add the firewall rule and the router
port forward below; the scripts in this repository never change a firewall, router, power or Docker setting.

## Trigger

* You want the thesis website's "ingest order history" button to fetch this week's Pine Script from this PC.
* You rotate the API secret or the TLS certificate (suspected leak, new public IP / DNS name, routine).
* You want to close the API again (Rollback).

## What is exposed and why it is safe

| Endpoint | What it does |
|---|---|
| `GET /v1/health` | `{"ok": true, "service": "xtnl-order-server", "api": 1}` |
| `POST /v1/pinescript/compile` body `{"force": false}` | starts (or reuses) the compile of the current ISO week (Monday 00:00 UTC to now); `202 {"week": "2026-W46", "status": "compiling" \| "ready"}` |
| `GET /v1/pinescript/latest` | `{"week", "status": "ready" \| "compiling" \| "missing" \| "failed", "generated_at", "window", "scripts": [{"symbol", "text"}]}` (scripts only when ready; failed carries a short `error` code such as `db_unavailable`) |

Anything else is `404 {}`. Protections, all in `src/api/`:

* **TLS 1.2+**, forward-secret AEAD suites only, self-signed certificate (`var\api-tls\`, git-ignored). The website
  trusts no CA for it: it pins the certificate's SHA-256 fingerprint, so a man in the middle cannot impersonate
  this PC even with a publicly trusted certificate.
* **Every request is signed**: `X-XTNL-Signature` = lowercase hex HMAC-SHA256(secret, `METHOD\nPATH\nTIMESTAMP\nNONCE\nsha256(body)`),
  compared in constant time; `X-XTNL-Timestamp` must be within 60 s of this PC's clock; each `X-XTNL-Nonce` is
  accepted once within 120 s (bounded replay cache; unsigned traffic never fills it). Wrong or missing: `401`.
* **Abuse limits**: 30 requests a minute per client IP (`429` with `Retry-After`; unsigned requests count too),
  request head <= 8 KiB / 32 headers (`431`), body <= 4 KiB (`413`, never read), no chunked bodies, total
  deadlines of 5 s for the head and 10 s for the body (slow clients are cut off, `408`), one request per
  connection. Connection caps apply **before the TLS handshake** (the socket is accepted as plain TCP with
  reading paused, counted, then upgraded): at most 128 connections, 8 per IP and 120 new connections a minute per
  IP, handshake within 5 s; a refused socket is aborted unread. Refusals are logged at most once per 10 s with a
  count of the ones not logged, so a flood cannot also flood the log.
* **No internal detail** in any response (no stack trace, no exception text); the log has one JSON line per
  request with method, path, status, client IP and latency, never a header, body, signature or the secret.
* **Read-only data path**: one REPEATABLE READ, READ ONLY transaction through the report code, as PostgreSQL role
  `xtnl_api` (migration 0014: SELECT on `event_log`, `orders_state`, `positions_state` only, sessions read-only by
  default). Before reading, the API checks the role and refuses (`failed`, `error: db_role_refused`) any role that
  is a superuser or could write a table. The scripts carry symbols, sides, times, status names and price levels
  only: never a size, amount, balance or spread.
* **Least privilege for the one network-facing process**: `start-xtnl.ps1` starts the API without any name of
  its `$ApiDropEnv` list: the broker credentials and settings (`STONE_X_*`, `CTRADER_*`, `BROKER_ENV`), the
  engine's `ORDER_SERVER_DSN` and `REDIS_URL`, the market-data / ARM settings and secrets (`XTNL_MD_*`,
  `XTNL_ARM_*`), the alert channel credentials (`XTNL_SMTP_*`, `XTNL_SES_*`, `XTNL_PUSH_*`) and the dead-man token
  `XTNL_DEADMAN_TOKEN`. The API refuses to start (exit 78) if it sees any of them (the same list:
  `src/api/settings.py` `FORBIDDEN_PREFIXES` / `FORBIDDEN_NAMES`). It uses `XTNL_API_DSN` (role `xtnl_api`, Windows User environment) and its own
  Redis (`XTNL_API_REDIS_URL`). It still runs as your Windows user, so a full code-execution bug in it could read
  what that user can (the Windows User environment included); running it under a separate low-privilege Windows
  account or in a container would close that too.
* **Cache in Redis only**: the API's own Redis (compose service `redis-api`, `127.0.0.1:56381`, memory only, 32 MB),
  database 2 (`XTNL_API_REDIS_DB`), key `pinescript:week:<YYYY-Www>`, TTL 8 days. It never touches the engine's
  Redis (entity keys, `halt:state`). Nothing is written to disk or to PostgreSQL. Redis keeps no data across a
  container restart: the next click simply compiles again.
* **The secret** (`XTNL_API_SECRET`, 32 random bytes as hex) lives only in the Windows User environment of this
  PC and in Vercel's server-side settings. It is never printed, logged or written to a file.

What remains: the port is visible to internet scanners (they get a TLS handshake and `401`/`404`); a flood can
still use the home uplink; and anyone holding the secret can read this week's order history. Rotate on any doubt.
The repository sits under OneDrive, so `var\api-tls\server.key` would be synced to your OneDrive account like the
rest of the folder. **Recommended:** pass `-OutDir $env:USERPROFILE\.xtnl\api-tls` (outside OneDrive) and set
`XTNL_API_TLS_CERT` / `XTNL_API_TLS_KEY` in `deploy\env\local.env` to match (this PC uses
`C:\Users\<you>\.xtnl\api-tls\server.crt` / `server.key`).
Never put them under `%LOCALAPPDATA%` (`AppData\Local`): files written there by a packaged (MSIX) app, such as
the Claude desktop app or a Store-installed terminal, are redirected into that app's private copy of AppData, so
the scheduled task cannot see them, and the API exits 78 "TLS certificate or key unusable" (`FileNotFoundError`).

## Prerequisites

* The engine runs through the `XTNL engine` scheduled task ([single-pc-autostart.md](single-pc-autostart.md));
  `deploy\local\start-xtnl.ps1` starts the API beside the engine once `XTNL_API_SECRET` exists.
* Git for Windows (its `openssl.exe` makes the certificate; found automatically).
* Admin access to your router, and a public IPv4 address that is not behind carrier-grade NAT (step 5).
* The PC's clock synchronised (w32time; the engine already checks it): a skew over 60 s makes every request `401`.

## 1. Create the credentials

From the repository root, in a normal (not elevated) PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-api-credentials.ps1 -Hosts <public-ip>[,<ddns-name>]
```

It writes `server.crt` / `server.key` to `-OutDir` (default `var\api-tls\`; see the OneDrive / AppData note above;
the key readable by you only), stores `XTNL_API_SECRET` in your
Windows User environment (not shown), gives the read-only PostgreSQL role `xtnl_api` a random password and stores
its DSN as `XTNL_API_DSN` in the Windows User environment (not shown; needs the local stack up and the migrations
applied, which the engine task does at every start: otherwise it says so, and you run it again later with
`-DbRole`), and prints `ORDER_SERVER_URL` and `ORDER_SERVER_TLS_FINGERPRINT` for Vercel.
Running it again keeps existing credentials; `-Rotate` replaces them. The `-Hosts` list only fills the
certificate's subjectAltName (the website pins the certificate itself, so a later IP change needs no new
certificate).

## 2. Start the API (restart the engine task)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1
Start-ScheduledTask -TaskName 'XTNL engine'
```

The daily log `var\logs\xtnl-<yyyy-MM-dd>.log` then shows `order-server API enabled` and `[api]` lines, among
them `"event":"listening"` with the certificate fingerprint.

## 3. Check it locally

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\status-xtnl.ps1
```

Section `order-server API`: `listening: 0.0.0.0:47443 (pid N)`, then `healthy: https://127.0.0.1:47443/v1/health -> 200 ...`
(a signed request pinned to the local certificate, `python -m src.api.selfcheck`) and the fingerprint.

## 4. Windows Firewall (elevated PowerShell, you run it)

Open PowerShell **as administrator** and run:

```powershell
New-NetFirewallRule -DisplayName 'XTNL order-server API' -Direction Inbound -Protocol TCP -LocalPort 47443 -Action Allow -Profile Private,Public
Get-NetFirewallRule -DisplayName 'XTNL order-server API' | Format-List DisplayName, Enabled, Direction, Action, Profile
```

To remove it again:

```powershell
Remove-NetFirewallRule -DisplayName 'XTNL order-server API'
```

Vercel's outbound addresses are not fixed, so the rule cannot be limited to them; the HMAC signature and the
certificate pin are what keep others out.

## 5. Router: reserve the PC's address and forward TCP 47443

1. Find this PC's LAN address and MAC address:

   ```powershell
   Get-NetIPConfiguration | Where-Object { $_.IPv4DefaultGateway } | Format-List InterfaceAlias, IPv4Address, IPv4DefaultGateway
   Get-NetAdapter | Where-Object Status -eq 'Up' | Format-Table Name, MacAddress
   ```

2. In the router's admin page (usually the gateway address above), add a **DHCP reservation** (also called
   static lease / address reservation) binding that MAC address to that IPv4 address, so the forward keeps
   pointing at this PC.
3. Add a **port forward** (virtual server / NAT rule): protocol **TCP**, external port **47443**, internal address
   = the reserved LAN address, internal port **47443**. Nothing else: no DMZ, no UPnP, no other ports.

## 6. Find the public IP and check for CGNAT

```powershell
Invoke-RestMethod https://api.ipify.org
```

Compare it with the **WAN / Internet IP** on the router's status page:

* Same address: you have a public IPv4; use it in `ORDER_SERVER_URL`.
* Different, or the router's WAN address is in 100.64.0.0/10, 10.0.0.0/8, 172.16.0.0/12 or 192.168.0.0/16: you
  are behind **carrier-grade NAT** and no port forward can reach this PC. Ask the ISP for a public (often static)
  IPv4; until then the website reports the gateway as disabled.

Test from outside your network (for example a phone on mobile data, not your Wi-Fi: many routers cannot loop
back to their own public address). An unsigned request must answer `401`, which proves the path works:

```powershell
curl.exe -k -s -o NUL -w "%{http_code}\n" https://<public-ip>:47443/v1/health
```

## 7. Optional: dynamic DNS

Home public addresses change. Enable the router's DDNS client (or any DDNS provider's updater) for a name such as
`xtnl-example.duckdns.org`, then use `https://<that-name>:47443` as `ORDER_SERVER_URL`. Re-run step 1 with
`-Rotate -CertOnly -Hosts <ip>,<name>` only if you want the name in the certificate (not required by the pin).

## 8. Vercel settings

Project > Settings > Environment Variables (server-side only, never `NEXT_PUBLIC_`), then redeploy:

| Variable | Value |
|---|---|
| `ORDER_SERVER_URL` | `https://<public-ip-or-ddns-name>:47443` |
| `ORDER_SERVER_TLS_FINGERPRINT` | the fingerprint printed in step 1 (`AB:CD:...`, colons optional) |
| `ORDER_SERVER_API_SECRET` | run `deploy\local\new-api-credentials.ps1 -CopySecret` and paste; then clear the clipboard |

If any of them is missing, the website shows the gateway as disabled and never calls this PC.

## Checks

* `status-xtnl.ps1` section `order-server API`: listening and `healthy`.
* `[api]` lines in the daily log: one `"event":"request"` per call with its status. `401` with `reason`
  `timestamp_outside_window` means a clock skew over 60 s on either side; `bad_signature` means the website's
  secret differs from this PC's.
* `"event":"compile_failed"` with `"error":"db_unavailable"`: PostgreSQL is down (`docker compose -f
  deploy\local\docker-compose.local.yml ps`); `503 cache_unavailable`: the API's Redis (`redis-api`) is down.
  `"error":"db_role_refused"`: `XTNL_API_DSN` points at a role that could write (run
  `new-api-credentials.ps1 -DbRole` and restart the task).
* `[api]` exit 78 in the log: configuration refused (missing / weak secret, certificate missing, `XTNL_API_DSN` or
  `XTNL_API_REDIS_URL` missing, or a name of `$ApiDropEnv` in its environment: `STONE_X_*`, `CTRADER_*`,
  `BROKER_ENV`, `ORDER_SERVER_DSN`, `REDIS_URL`, `XTNL_MD_*`, `XTNL_ARM_*`, `XTNL_SMTP_*`, `XTNL_SES_*`,
  `XTNL_PUSH_*`, `XTNL_DEADMAN_TOKEN`); start-xtnl.ps1
  disables the API until the next start. Any other exit is restarted after 5 s, doubling to 5 min.
* `"event":"connection_refused"` (at most one line per 10 s, `suppressed` = refusals not logged): a client hit the
  connection caps; the thesis website keeps one or two connections at most.
* The website polls `latest` for at most 3 minutes; keep its poll interval at 3 s or more so a whole session stays
  under 30 requests a minute. The 30-a-minute bucket is per client IP, and every website user arrives from the
  same Vercel egress address, so **all website users share one bucket**: the website's route therefore serves one
  shared `latest` answer per 4 s to all pollers and limits each user (one compile per 15 s;
  `lib/orderServer/README.md` in the thesis repository). With many simultaneous users raise
  `XTNL_API_RATE_PER_MIN` (1..600) in `deploy\env\local.env` and restart the task.

## Rotation

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-api-credentials.ps1 -Rotate -Hosts <public-ip>
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-api-credentials.ps1 -CopySecret
```

Then restart the task (step 2) and update `ORDER_SERVER_API_SECRET` and `ORDER_SERVER_TLS_FINGERPRINT` in Vercel
and redeploy. The old secret and certificate stop working as soon as the API restarts. `-Rotate -CertOnly`
replaces only the certificate (then only the fingerprint changes in Vercel).

## Rollback

Close everything again, in this order:

1. Firewall (elevated PowerShell): `Remove-NetFirewallRule -DisplayName 'XTNL order-server API'`
2. Router: delete the TCP 47443 port forward (and the DHCP reservation if nothing else needs it).
3. Disable the API and restart the task (nothing listens afterwards):

   ```powershell
   [Environment]::SetEnvironmentVariable('XTNL_API_SECRET', $null, 'User')
   powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1
   Start-ScheduledTask -TaskName 'XTNL engine'
   ```

4. Delete the certificate and key: `Remove-Item -Recurse var\api-tls`
5. Remove the three `ORDER_SERVER_*` variables from Vercel and redeploy.
6. Optional: drop the cached scripts (the API's own Redis, database 2 only; never FLUSHALL, never the engine's
   `redis` service): `docker compose -f deploy\local\docker-compose.local.yml exec redis-api redis-cli -n 2 FLUSHDB`
7. Optional: remove the database credential: `[Environment]::SetEnvironmentVariable('XTNL_API_DSN', $null, 'User')`
   and, in the postgres container, `ALTER ROLE xtnl_api NOLOGIN PASSWORD NULL;`
