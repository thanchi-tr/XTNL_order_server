# Single-PC autostart (the engine on this Windows PC, started at log-on)

Phase 1 never sends orders. The engine started here only **reads** the broker (`BROKER_ENV=live_readonly`,
read-only credentials); nothing in this runbook places, modifies or cancels an order or closes a position.
Logs, pages and the views below carry counts, ids and times only, never an amount.

Rules: `.claude/rules/operations.md` §17 (single-PC mode). Related: [downtime-catchup.md](downtime-catchup.md)
(what the engine re-reads after it was off), [redis-loss.md](redis-loss.md) (Redis is rebuilt after every
Docker restart).

| File | What it does |
|---|---|
| `deploy/local/docker-compose.local.yml` | Live stack `xtnl-local`: PostgreSQL 17 (volume `xtnl_local_pg`), Redis 7.4 (memory only), etcd 3.5.17 (volume `xtnl_local_etcd`); `restart: unless-stopped`; ports 127.0.0.1:55433 / 56380 / 52380. Separate from the throwaway test stack (`deploy/docker-compose.dev.yml`, 55432 / 56379 / 52379) |
| `deploy/env/local-live.env.example` -> `deploy/env/local.env` | Engine settings (no credentials) |
| `deploy/local/start-xtnl.ps1` | What the task runs: waits for Docker, `compose up --wait`, migrations, clock check, then `run-local.ps1`; retries all of that itself after a failure |
| `deploy/local/run-local.ps1` | Runs the engine and restarts it after a hard exit (codes 70-79) |
| `deploy/local/stop-xtnl.ps1` (+ `send-ctrl-c.ps1`) | Graceful stop (Ctrl+C to the hidden console, the engine drains) |
| `deploy/local/install-autostart.ps1` / `uninstall-autostart.ps1` | Register / remove the scheduled task `XTNL engine` |
| `deploy/local/status-xtnl.ps1` | Task, containers, last log lines, the last `ALERT:` / `PAGE[critical]` lines, `uptime show` / `catchup show`, the API, the clock, redis-md and the ARM |
| `deploy/local/new-md-credentials.ps1` | Optional market-data layer only: redis-md's ACL file (`%LOCALAPPDATA%\XTNL\redis-md\users.acl`) and its URLs / content-hash key in the Windows User environment (see "Optional market-data store (redis-md)") |

## Trigger

* First set-up of the engine on this PC, or after rebuilding the PC.
* The engine is not running after a log-on (check with `status-xtnl.ps1`).
* Planned stop (maintenance, travel), or removing the autostart.

## Prerequisites

1. **Credentials** only in the Windows **User** environment (System Properties > Environment Variables > User
   variables): `STONE_X_USERNAME`, `STONE_X_PASS`, `STONE_X_APP_KEY`, `BROKER_ENV=live_readonly`, and if used
   `STONE_X_BASE_URL`, `STONE_X_BASE_URL_DEPRECATED`, `STONE_X_STREAM_URL`; the other secrets the script copies
   from there when set are listed in `$UserVarNames` in `start-xtnl.ps1`. Never in a file: `start-xtnl.ps1`
   refuses to start (exit 78) when `deploy\env\local.env` assigns any name of its `$ForbiddenInFile` list:
   * the broker credentials `STONE_X_USERNAME`, `STONE_X_PASS`, `STONE_X_APP_KEY`;
   * the order-server API's `XTNL_API_SECRET`, `XTNL_API_DSN`;
   * the Oversee alert channels `XTNL_SMTP_USERNAME`, `XTNL_SMTP_PASSWORD`, `XTNL_SES_ACCESS_KEY_ID`,
     `XTNL_SES_SECRET_ACCESS_KEY`, `XTNL_SES_SESSION_TOKEN`, `XTNL_PUSH_TOKEN`, `XTNL_PUSH_USER`, and the dead-man
     token `XTNL_DEADMAN_TOKEN`;
   * the market-data secrets `XTNL_MD_REDIS_URL`, `XTNL_MD_REDIS_READER_URL`, `XTNL_MD_HMAC_KEY`,
     `XTNL_MD_HMAC_KEY_PREV`;
   * the ARM secrets `XTNL_ARM_CONFIG_DSN`, `XTNL_ARM_SMTP_USERNAME`, `XTNL_ARM_SMTP_PASSWORD`,
     `XTNL_ARM_SES_ACCESS_KEY_ID`, `XTNL_ARM_SES_SECRET_ACCESS_KEY`, `XTNL_ARM_SES_SESSION_TOKEN`,
     `XTNL_ARM_PUSH_TOKEN`, `XTNL_ARM_PUSH_USER`.

   It logs variable names only, never values.
2. **Env file:** `deploy\env\local.env` is a copy of `deploy\env\local-live.env.example` (git-ignored):
   ```
   Copy-Item deploy\env\local-live.env.example deploy\env\local.env
   ```
3. **Project virtualenv:** `.venv\Scripts\python.exe` exists (`python -m venv .venv`, `pip install -e .[dev]`).
4. **Docker Desktop:** Settings > General > **Start Docker Desktop when you sign in to your computer**: on.
   The task waits up to 10 min for the Docker engine and logs a line each minute, then retries (below).
5. **Power settings** (set them yourself; the scripts never change Windows settings):
   * Settings > System > Power & battery > Screen and sleep: **When plugged in, put my device to sleep after:
     Never**. (On battery it may sleep; the engine catches up when it wakes.)
   * Control Panel > Power Options > Choose what closing the lid does: **When I close the lid, plugged in: Do
     nothing**.
   * Settings > Windows Update > Advanced options > **Active hours**: cover the hours you trade, so Windows
     does not restart for updates then.
6. **Clock:** w32time syncs every 15 min. The engine's hard check needs |offset| <= 50 ms; `start-xtnl.ps1`
   waits up to 15 min for it and never resyncs (that needs admin). When the wait gives up it logs an `ALERT:` line
   and starts the engine anyway: the engine applies the same check itself, so it records nothing until the clock
   passes and then becomes leader at once (`-NoRetry` keeps the old exit 13). Measuring the offset needs no admin;
   to resync by hand from an elevated PowerShell: `w32tm /resync` (the scripts never do it).

## Install

From a PowerShell window in the project folder (no admin needed for a task of your own user; if Windows answers
"Access is denied", run the same command from an elevated PowerShell: the task still runs as you, not elevated):

```
cd <project folder>                                                                  # the order-server checkout
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-autostart.ps1            # register only
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-autostart.ps1 -StartNow  # and start now
```

The task `XTNL engine`: at log-on of your user with a 60 s delay; `powershell.exe -NoProfile -ExecutionPolicy
Bypass -WindowStyle Hidden -File <root>\deploy\local\start-xtnl.ps1`; run only when you are logged on (Docker
Desktop runs per user); restart on failure every 1 min up to 999 times; no time limit; runs on batteries and
keeps running when unplugged; a missed start runs as soon as possible; normal priority. Re-running the install
replaces the task.

Task Scheduler's restart on failure covers a task that fails to launch, not a script that exits with an error
code, so `start-xtnl.ps1` retries its own failures: Docker not up in 10 min, compose or migrations failing, the
clock not within 50 ms in 15 min, or the engine ending with any code other than 0 (clean stop) or 78
(configuration). It logs `attempt ended (exit N)` and starts again from the Docker check after 60 s, doubling to
15 min (reset after a run of 10 min or more). Only a clean stop, a configuration problem (78) or `stop-xtnl.ps1`
ends it. If a terminal window stays open after log-on (Windows Terminal as the default terminal
ignores `-WindowStyle Hidden`), re-install with `-ViaConhost`.

The first start creates the volumes and an empty database; the migrations run; the first catch-up reads
`XTNL_CATCHUP_FIRST_RUN_DAYS` (7) days of broker history (see "Carry over" below for the alternative).

## Start, stop, status

```
Start-ScheduledTask -TaskName 'XTNL engine'                                          # start (hidden)
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\start-xtnl.ps1      # or in this window (Ctrl+C stops)
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1       # graceful stop, containers keep running
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1 -Containers   # also `docker compose stop`
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\status-xtnl.ps1     # task, containers, log tail, uptime / catch-up
```

* Only one `start-xtnl.ps1` runs at a time (mutex `Global\XTNL-engine-start` + `var\logs\start-xtnl.pid`); a
  second copy logs "already running" and exits 0.
* `stop-xtnl.ps1` sends Ctrl+C to the hidden console: the engine drains on SIGINT and records
  `end_reason = SIGINT`; `start-xtnl.ps1` exits 0, so the task does not restart it. After 90 s it ends the
  task hard; every child is in a kill-on-close job object, so nothing is left running, and the next start-up
  catch-up re-reads what changed.
* Do not also run `run-local.ps1` by hand while the task runs; the etcd lease keeps a single leader anyway.

## SAFE_HOLD on this PC (auto-resume)

SAFE_HOLD is the anti-flap brake ([safe-hold.md](safe-hold.md)): while it is set the engine never becomes leader
and records nothing. On AWS only two operators may resume it. On this PC the engine runs with `XTNL_SINGLE_PC=1`
(`start-xtnl.ps1` and `run-local.ps1` set it unless `deploy\env\local.env` sets it; `0` there makes the resume
operator-only again), and resumes a hold itself (operations.md §4 / §17, accepted deviation):

* It finds SAFE_HOLD set (at start, a hold left by an earlier run included, or while held) and its hard checks pass
  (PostgreSQL writable, etcd reachable, engine Redis writable, clock within 50 ms). Log: `SAFE_HOLD set since ...;
  single PC: auto-resume N of 6 in S s while the hard checks pass`.
* It waits the cool-down: 5 min for the first auto-resume, doubling for each earlier one within the 24 h before the
  hold (10, 20, 40, then 60 min). A failing hard check restarts the cool-down.
* It then runs the operator resume itself: `cluster_state.safe_hold` cleared, `last_resume_at` set, `resumed_by`
  = `system:single-pc-auto-resume`, one `operator_audit` row (`safe_hold.resume`, operator
  `system.single-pc-auto-resume`, the held reason in the detail), the etcd mirror written. It clears only the hold
  it timed the cool-down for: a newer hold set meanwhile stays set and gets its own cool-down and budget check. It
  campaigns at once and pages info `SAFE_HOLD auto-resumed on the single PC` (sent through the alert channels when
  it reaches leader; logged only while they are unset).
* After 6 auto-resumes within 24 h it stays held and pages critical `SAFE_HOLD auto-resume stopped on the single
  PC`: a genuine restart loop needs a human. Find the cause and resume as in [safe-hold.md](safe-hold.md). Your
  resume starts a fresh budget: only auto-resumes after the last resume by a person count, so a later hold is
  auto-resumed again (first cool-down 5 min).
* The two critical pages (this one and `engine not recording` below) arise only while there is no leader, so they
  go **straight to the alert channels** (`XTNL_ALERT_EMAIL_TO` with SMTP / SES, `XTNL_PUSH_*`), not through the
  leader's outbox; a failed channel is tried again after 1 and 5 min. The log says `direct page '...' sent` or
  `... not delivered`. With no channel configured they are only `PAGE[critical]` log lines:
  `status-xtnl.ps1` lists the last `ALERT:` / `PAGE[critical]` / `direct page` lines of every log.
* Timings: `XTNL_SINGLE_PC_RESUME_FIRST_S` (300), `XTNL_SINGLE_PC_RESUME_MAX_S` (3600),
  `XTNL_SINGLE_PC_RESUME_LIMIT` (6), `XTNL_SINGLE_PC_RESUME_WINDOW_S` (86400) in `deploy\env\local.env`.

Check: `python -m src.ops safe-hold status` and `python -m src.ops audit tail --command safe_hold.resume`.

With the flag on, a node that has held no leader term for 10 min (`XTNL_SINGLE_PC_IDLE_PAGE_S`, 600) also pages
critical `engine not recording on the single PC`, naming its role and the failing hard checks (straight to the
alert channels, as above), and pages info when it records again.

## Reviewed-change switches (plan S38f)

On this PC the working tree is the deployment: every start runs step 6 (migrations) and the engine from the
repository. Two local settings in `deploy\env\local.env` keep unreviewed changes off:

* `XTNL_MIGRATIONS_ACCEPTED_UPTO` (default `14`): step 6 applies migrations only up to it. Later ones (0015-0020, the
  market-data / ARM schema) are held back, named in one `[migrate] held back above ...` line, and the start continues;
  the engine runs on the older schema (design §0b). Raise it (to `20`) only once they are reviewed.
* `XTNL_OVERSEE_STOP_AT_ENTRY` (default `0`): `1` turns on the S07 Oversee stop rule (design §0 C4); `0` keeps the
  pre-S07 rule. The engine logs which rule runs at start (`oversee stop rule: ...`).

* **Order** (plan S38f, design §0b "What turns each on"): first raise `XTNL_MIGRATIONS_ACCEPTED_UPTO` to `20` once
  migrations 0015-0020 are reviewed (the next start applies them; until then they are held back and named in one
  `[migrate]` / journal line, and the engine runs on the 0014 schema), then `XTNL_OVERSEE_STOP_AT_ENTRY=1` once the
  S07 Oversee stop rule (design §0 C4) is confirmed, then `XTNL_MD_ENABLED=1`, then `XTNL_ARM_ENABLED=1`. The go-live
  gate `oversee_c4` counts as accepted only with `XTNL_OVERSEE_STOP_AT_ENTRY=1`.

## Optional market-data store (redis-md)

Off by default, and the engine never needs it: with `XTNL_MD_ENABLED` unset or `0` nothing below runs, and
ingestion and Oversee behave exactly as without it. Turning it on never affects them either.

* **Flag:** `XTNL_MD_ENABLED` is read as the engine reads it: the process value (the one in
  `deploy\env\local.env`, or one inherited from the Windows User environment), trimmed. Only `1` is on; unset or
  `0` is off; anything else (`true`, `yes`, `on` included) counts as off and the engine pages it.
  `start-xtnl.ps1` and `status-xtnl.ps1` use the same test.
* **Enable:**
  1. run `powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-md-credentials.ps1`: it writes
     redis-md's ACL file to `%LOCALAPPDATA%\XTNL\redis-md\users.acl` (outside the repository and OneDrive) and
     sets `XTNL_MD_REDIS_URL`, `XTNL_MD_REDIS_READER_URL` and the content-hash key in the Windows User
     environment (never in a file, never printed);
  2. set `XTNL_MD_ENABLED=1` in `deploy\env\local.env`;
  3. restart the task (`stop-xtnl.ps1`, then `Start-ScheduledTask -TaskName 'XTNL engine'`).

  After the plain `compose up --wait`, `start-xtnl.ps1` then runs `docker compose -f
  deploy\local\docker-compose.local.yml --profile md up -d redis-md` (`127.0.0.1:56382`, 2 GB limit). This step
  is never fatal and never waited for: a missing ACL file or a failed start is logged (`[compose md]` lines) and
  the engine starts without redis-md; the market-data services then page and stay idle. The ARM
  (`XTNL_ARM_ENABLED=1`) needs `XTNL_MD_ENABLED=1` too; enable it only after the market-data layer has soaked.
* **Disable:** set `XTNL_MD_ENABLED=0` (or remove it) and restart the task. redis-md has
  `restart: unless-stopped`, so Docker would bring it back after every reboot; with the flag off `start-xtnl.ps1`
  therefore stops a running redis-md (`XTNL_MD_ENABLED is off but redis-md is running; stopping it`; never fatal,
  the data volume is kept). By hand: `docker compose -f deploy\local\docker-compose.local.yml --profile md stop
  redis-md`.
* **Status:** `status-xtnl.ps1` section `redis-md` shows the flag, whether the ACL file exists, the container
  state and, with the flag on, `python -m src.ops md show` (counts and states only).

## Logs

* `var\logs\xtnl-<yyyy-MM-dd>.log`: one file per day (a long run continues in the next day's file), files older
  than 14 days are deleted at start-up and at each day change. Lines from the script are prefixed
  `[start-xtnl]`, `[compose]`, `[migrate]`, `[clock]`; engine lines are JSON (`XTNL_LOG_FORMAT=json`).
* Container logs: `docker compose -f deploy\local\docker-compose.local.yml logs --tail 100 postgres`
  (json-file, 3 x 10 MB per container).
* Task history: Task Scheduler > Task Scheduler Library > `XTNL engine` > History, or `status-xtnl.ps1`
  (`last result`: 0 clean stop, 78 configuration, 0x41301 = running; 10 Docker not up, 11 compose failed,
  12 migrations failed, 13 clock not within 50 ms appear only with `start-xtnl.ps1 -NoRetry`, because the task's
  run retries them itself).

## What happens on reboot, sleep, Windows update

* **Reboot / power cut / Windows update restart:** at your next log-on Docker Desktop starts, the containers come
  back (`restart: unless-stopped`; PostgreSQL and etcd data on volumes, Redis empty), and 60 s later the task
  runs `start-xtnl.ps1`: compose up (no-op if running), migrations (no-op), clock check, engine. The engine
  rebuilds Redis from PostgreSQL (SM8) and runs a `startup` catch-up from the last recorded coverage
  ([downtime-catchup.md](downtime-catchup.md)). Nothing runs before you log on (the task is "only when logged
  on"); turn on automatic sign-in yourself if you need that.
* **Sleep / hibernate:** every process is suspended. On wake the engine detects the wall-clock jump and runs a
  `resume_after_sleep` catch-up; if the sleep outlived the 15 s lease, it hard-exits and `run-local.ps1`
  restarts it (back-off 5 s doubling to 120 s), which runs a `startup` catch-up. A short clock offset right after
  wake is corrected by w32time; the health check fails closed meanwhile.
* **Docker Desktop restart or update:** the containers restart; the engine loses PostgreSQL / Redis / etcd for a
  while. Short outages are handled in place (Redis loss: SM8); a lease loss hard-exits the engine and
  `run-local.ps1` restarts it.
* **Log-off:** the task stops with your session (hard stop; next log-on starts it again with a catch-up).

## Checks

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\status-xtnl.ps1
Get-Content (Get-ChildItem var\logs\xtnl-*.log | Sort-Object LastWriteTime | Select-Object -Last 1).FullName -Tail 50
```

Expected after a start: `clock check passed`, `running deploy\local\run-local.ps1`, then engine JSON lines; task
state `Running`; three containers `healthy`; `uptime show` lists the current run with a recent `last_seen`
(within `XTNL_UPTIME_PING_S` = 900 s); `catchup show` lists a `startup` catch-up, `complete` true. An
incomplete catch-up: follow [downtime-catchup.md](downtime-catchup.md).

If the engine does not come up: read the newest log for the `[start-xtnl]` lines before each
`attempt ended (exit N)` (it keeps retrying) or before `start-xtnl exits (N)` (it stopped). `ALERT:` lines mark
what keeps it from recording: `the engine has not run for N min` (after 30 min of failed attempts, hourly after),
`configuration refused (exit 78)`, `clock not within the limit ... starting the engine anyway`, `engine restart
loop` (run-local.ps1: 5 hard exits within 30 min); once the engine runs, `PAGE[critical] engine not recording on
the single PC` and `SAFE_HOLD auto-resume stopped` (also sent to the alert channels when configured). `status-xtnl.ps1`
lists the last 20 of them over every log (`-AlertLines N` for more); by hand:
`Select-String -Path var\logs\xtnl-*.log -Pattern 'ALERT:|PAGE\[critical\]' | Select-Object -Last 20`. 78 = configuration (env file missing or assigning a name of `$ForbiddenInFile` (Prerequisites, step 1), a
Windows User variable missing, BROKER_ENV differing between the User environment and `local.env`).

## Carry over the earlier week's data (optional, once)

The default is a fresh database: the first catch-up re-reads 7 days of broker history. To keep the rows the
engine already stored in the throwaway stack's `xtnl_live` database (tmpfs: lost when that stack stops), copy it
**before** the first start of the task, with the engine stopped (this only reads `xtnl_live`):

```
docker compose -f deploy\local\docker-compose.local.yml up -d --wait
docker exec xtnl-dev-postgres-1 pg_dump -U xtnl -Fc -f /tmp/xtnl_live.dump xtnl_live
docker cp xtnl-dev-postgres-1:/tmp/xtnl_live.dump var\xtnl_live.dump
docker exec xtnl-dev-postgres-1 rm -f /tmp/xtnl_live.dump
# Start from an empty target (also after a failed earlier attempt):
docker exec xtnl-local-postgres-1 psql -U xtnl -d postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS xtnl WITH (FORCE)" -c "CREATE DATABASE xtnl OWNER xtnl"
# Roles are cluster-wide, so pg_dump does not carry them, but the dump GRANTs to xtnl_dashboard (migration 0011).
# Create it first with the migration's own statements (NOLOGIN, no credential):
Get-Content src\db\migrations\0011_dashboard_role.sql -TotalCount 26 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
docker cp var\xtnl_live.dump xtnl-local-postgres-1:/tmp/xtnl_live.dump
docker exec xtnl-local-postgres-1 pg_restore -U xtnl -d xtnl --no-owner --exit-on-error /tmp/xtnl_live.dump
docker exec xtnl-local-postgres-1 rm -f /tmp/xtnl_live.dump
Remove-Item var\xtnl_live.dump      # it holds account data
$env:ORDER_SERVER_DSN = 'postgresql://xtnl:xtnl_local_only@127.0.0.1:55433/xtnl'
.venv\Scripts\python.exe -m src.db.migrate      # idempotent: "applied: none (up to date)" when the dump was current
```

Without the role step, `pg_restore --exit-on-error` stops at `role "xtnl_dashboard" does not exist`; without
`--exit-on-error` it skips those GRANTs and the dashboard role later sees nothing. Compare row counts only
(`orders_state`, `positions_state`, `event_log`, `catchup_runs`, `engine_runs`, `rulesets`) between the two
databases before starting.

`engine_epoch` then names the dev stack's etcd `cluster_id`, so the engine FENCES at its first start (as it
must, operations.md §2). Adopt the new cluster once, audited, then start again:

```
$env:ORDER_SERVER_DSN = 'postgresql://xtnl:xtnl_local_only@127.0.0.1:55433/xtnl'
$env:XTNL_ETCD_ENDPOINTS = 'http://127.0.0.1:52380,http://127.0.0.1:52380,http://127.0.0.1:52380'
.venv\Scripts\python.exe -m src.ops epoch show
.venv\Scripts\python.exe -m src.ops epoch adopt <etcd_cluster_id> --from-cluster-id <cluster_id> `
    --operator <you> --approver <second person> --reason "single-PC move from xtnl-dev to xtnl-local"
```

## Rollback

Remove the autostart (stops the engine gracefully first):

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\uninstall-autostart.ps1
docker compose -f deploy\local\docker-compose.local.yml stop      # containers stopped, data kept
docker compose -f deploy\local\docker-compose.local.yml down      # containers removed, volumes kept
```

`down -v` (or `docker volume rm xtnl_local_pg xtnl_local_etcd`) deletes the whole engine history; do it only on
purpose. A new etcd volume means a new `cluster_id`: the engine then fences until `epoch adopt` (above). To go
back to the development run instead: `deploy\local\run-local.ps1` with `deploy\env\local.env.example`
(throwaway stack, sandbox).
