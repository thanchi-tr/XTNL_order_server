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
| `deploy/local/ups-watch.ps1` / `install-ups-watch.ps1` | UPS fallback (home-server plan W12): drains the engine (`stop-xtnl.ps1 -Containers`) and shuts Windows down before the UPS battery runs out; task `XTNL UPS watch` (see "UPS: clean stop before the battery runs out") |

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
     `XTNL_ARM_PUSH_TOKEN`, `XTNL_ARM_PUSH_USER`;
   * the live view's `XTNL_LIVEVIEW_DSN`;
   * the heartbeat copy `XTNL_HEARTBEAT_COPY_URL` and `XTNL_HEARTBEAT_COPY_TOKEN` (home-server plan W2). On the
     home server the copy URL is the Healthchecks.io check A ping URL, a secret (anyone holding it can keep the
     check green), so it goes in the **User** environment like the dead-man token. Earlier stop-gap notes put it in
     `local.env`: move it, or the start refuses. Leave `XTNL_HEARTBEAT_COPY_TOKEN` unset for Healthchecks.io.

   It also refuses (exit 78, "backup credentials belong to the separate backup Windows account") an env file that
   assigns **any** name starting with `XTNL_BACKUP_`, `RCLONE_` or `B2_` (`$ForbiddenPrefixesInFile`): those belong
   to the backup account only ([home-backup-restore.md](home-backup-restore.md)). When this account's own
   environment holds such a name, it logs `warning: this account's environment holds <names>` and removes them from
   the engine, the API and the live view. Delete them from this account, and **rotate every secret among them at
   once** (review round 5): anything running as this account could read them. That is a new check C ping URL for
   `XTNL_BACKUP_HC_URL` and a new B2 upload key for `XTNL_BACKUP_B2_ACCOUNT` / `XTNL_BACKUP_B2_KEY`, set in the
   backup account (home-backup-restore.md "If a backup secret leaked"). Write the earliest time they may have
   leaked in the rotation note. Check C runs recorded from that time until the rotation are not trusted for a
   restore, and rotating later does not make them trusted. Runs recorded before that time stay trusted
   (review round 7). Only `XTNL_BACKUP_HC_URL` and the B2 upload key are secret. When the names are only
   non-secret ones (`XTNL_BACKUP_B2_BUCKET`, `XTNL_BACKUP_AGE_RECIPIENT`, `XTNL_BACKUP_EXCLUDE_MARKS`,
   `XTNL_BACKUP_ARCHIVE_HOST`), it logs `warning: non-secret backup names found in this account's environment`
   instead: delete them, nothing needs rotating and no check C run is distrusted. Any other `XTNL_BACKUP_`,
   `RCLONE_` or `B2_` name counts as a secret. It logs variable names only, never values.

   Each machine needs its own `XTNL_NODE_ID` (`home1` on the home server). While it is still the example's `pc1`,
   `start-xtnl.ps1` logs `warning: XTNL_NODE_ID is the example's 'pc1'`: the dead-man receiver tells two engines
   apart by node and epoch (deadman-alert.md, "Two engines"). After you change `XTNL_NODE_ID` on the same box
   (for example `pc1 -> home1`) and restart, the receiver sends one `beating node changed (pc1 -> home1)` notice
   (review round 6). That one is expected: it clears after 5 min and needs no action. Only a change you did not
   make points to a second machine.
2. **Env file:** `deploy\env\local.env` is a copy of `deploy\env\local-live.env.example` (git-ignored):
   ```
   Copy-Item deploy\env\local-live.env.example deploy\env\local.env
   ```
3. **Project virtualenv:** `.venv\Scripts\python.exe` exists (`python -m venv .venv`, `pip install -e .[dev]`).
4. **Docker Desktop:** Settings > General > **Start Docker Desktop when you sign in to your computer**: on.
   The task waits up to 10 min for the Docker engine and logs a line each minute, then retries (below).
5. **Power settings** (set them yourself; the scripts never change Windows settings):
   * Settings > System > Power & battery > Screen and sleep: **When plugged in, put my device to sleep after:
     Never**. (On a laptop, on battery it may sleep; the engine catches up when it wakes. Behind a UPS, Windows
     sees the UPS as a battery and uses the **on-battery** settings during a power cut, where its default is to
     sleep within minutes: set those to Never too, as in "UPS: clean stop before the battery runs out".)
   * Control Panel > Power Options > Choose what closing the lid does: **When I close the lid, plugged in: Do
     nothing**.
   * Settings > Windows Update > Advanced options > **Active hours**: cover the hours you trade, so Windows
     does not restart for updates then.
   * **Fast Startup off** (admin PowerShell, once; home-server plan §4.3): `powercfg /hibernate off` turns off
     hibernate and Fast Startup. Check: `powercfg /a` lists neither Hibernate nor Fast Startup. While Fast
     Startup is on, Shut down is a log-off plus a kernel hibernate (see "What happens on reboot, sleep, Windows
     update").
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
* **Sleep / hibernate:** (you stay logged on; a Shut down with Fast Startup on is not this case, see below) every
  process is suspended. On wake the engine detects the wall-clock jump and runs a
  `resume_after_sleep` catch-up; if the sleep outlived the 15 s lease, it hard-exits and `run-local.ps1`
  restarts it (back-off 5 s doubling to 120 s), which runs a `startup` catch-up. A short clock offset right after
  wake is corrected by w32time; the health check fails closed meanwhile.
* **Docker Desktop restart or update:** the containers restart; the engine loses PostgreSQL / Redis / etcd for a
  while. Short outages are handled in place (Redis loss: SM8); a lease loss hard-exits the engine and
  `run-local.ps1` restarts it.
* **Log-off:** the task stops with your session (hard stop; next log-on starts it again with a catch-up).
* **Shut down while Windows Fast Startup is on (the Windows default):** Shut down is a log-off plus a kernel
  hibernate, not a sleep. Event Viewer > System shows Winlogon 7002 (log-off), then Kernel-Power 187 and 42
  "entering sleep" (reason Application API), and at the next start Kernel-Power 107 and Kernel-Boot 27 "boot type
  0x1" (a hybrid boot). The task still stopped at the log-off, as in "Log-off": a hard stop that leaves the
  `engine_runs` row open (its `last_seen_ms` is the last proof of life) with `suspended_gaps` 0; no sleep fence and
  no `resume_after_sleep` ran, and `run-local.ps1` stopped with the task. The next log-on starts the task and the
  engine runs a `startup` catch-up. The real sleep and wake times are in Power-Troubleshooter event 1. On a Modern
  Standby PC (`powercfg /a`: Standby (S0 Low Power Idle), no S3), Kernel-Power 506/507 at that wake belong to the
  resume, not to a separate sleep. `powercfg /hibernate off` (Prerequisites step 5; home-server plan §4.3) turns
  Fast Startup off; Shut down is then a full shutdown, as in "Reboot".
* **Power cut behind a UPS:** the UPS carries the box; before the battery runs out PowerPanel (or the watcher)
  drains the engine and shuts Windows down, and when mains returns the PC powers on and the task starts again.
  See the next section.

## UPS: clean stop before the battery runs out (home-server plan W12)

Windows shows a USB UPS as a battery. Left alone, a power cut runs the box on battery until Windows' own battery
actions (by default it sleeps within minutes on battery; a sleep fences the engine on wake) or until the battery
is flat (a hard power loss: no drain, the `engine_runs` row stays open). What you want instead, in this order:
the engine drains (`end_reason` `SIGINT`), Windows shuts down, the UPS turns its output off, and when mains returns
the PC powers on by itself (BIOS) and the task starts the engine again. Nothing here sends anything to a broker.

**1. Windows power plan, on battery (admin PowerShell, once).** The plugged-in values are in Prerequisites step 5
and the home-server plan §4.3; these are the on-battery (DC) ones:

```
powercfg /change standby-timeout-dc 0
powercfg /change hibernate-timeout-dc 0
powercfg /change disk-timeout-dc 0
powercfg /hibernate off
powercfg /setdcvalueindex SCHEME_CURRENT SUB_BATTERY BATACTIONLOW 0      # Low battery action: Do nothing
powercfg /setdcvalueindex SCHEME_CURRENT SUB_BATTERY BATACTIONCRIT 0     # Critical battery action: Do nothing
powercfg /setactive SCHEME_CURRENT
powercfg /query SCHEME_CURRENT SUB_BATTERY                               # check: both actions 0x00000000 on DC
```

Or in Control Panel > Power Options > Change plan settings > Change advanced power settings: Sleep > Sleep after
and Hibernate after: **Never** on battery; Hard disk > Turn off hard disk after: **Never** (0) on battery;
Battery > Low battery action and Critical battery action: **Do nothing** on battery. PowerPanel or the watcher
does the shutdown, never Windows. *Unverified:* whether this Windows build accepts Do nothing for the critical
action on battery (some offer only Sleep / Hibernate / Shut down there). If it does not, pick **Shut down**: it
fires only at the critical level, below the thresholds below, as a last backstop (a hard engine stop).

**2. BIOS (once):** Restore on AC power loss: **Power On**; RTC wake **daily** (the backstop when the box stays
off); ErP/EuP deep sleep **off** (it blocks AC restore, RTC wake and Wake-on-LAN); Wake-on-LAN **on**.

**3a. Path A, preferred: PowerPanel Personal.** Install CyberPower PowerPanel Personal as admin with the UPS USB
cable connected, then in its settings (*menu names unverified; they differ between versions and models*):

* **Shutdown at a runtime threshold**: shut down when the remaining runtime reaches **10 min**. Use runtime, not
  battery %: the charge a UPS reports is often non-linear.
* **Run command before shutdown**: on, pointing at a command file such as `C:\xtnl\ups-stop.cmd` (outside the
  repository, so a checkout never changes it), with at least **180 s** allowed for it (`stop-xtnl.ps1` waits up to
  90 s for the drain, then the containers stop; PostgreSQL has a 60 s grace):
  ```
  @echo off
  "%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "C:\xtnl\order-server\deploy\local\stop-xtnl.ps1" -Containers
  ```
* **Turn off UPS output after shutdown**: **on**. Otherwise, when mains returns after Windows shut down but before
  the battery is flat, the PC never sees the power drop and never powers on.
* Shutdown type: shut down (never hibernate).

`stop-xtnl.ps1` drains the engine by sending Ctrl+C to `start-xtnl.ps1`'s hidden console (`send-ctrl-c.ps1`), and
a console can only be reached **from the same Windows session** (the `xtnl` session). If PowerPanel runs the
command from its service (another session), the helper fails (`Ctrl+C could not be delivered (helper exit 2)`)
and `stop-xtnl.ps1` ends the engine hard instead: no drain (the next start-up catch-up still re-reads what changed).
The UPS test (step 4) shows which you have: afterwards, `uptime show` (in `status-xtnl.ps1`) shows the run's
reason `SIGINT` for a drain; an open run or another reason means a hard stop. Then use path B2.

**3b. Path B, the watcher (`deploy\local\ups-watch.ps1`).** Install it as `xtnl` (it must run in that session,
like the engine):

* **B1**, no PowerPanel, or PowerPanel without the command option: the watcher drains and shuts down.
  ```
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-ups-watch.ps1 -StartNow
  ```
* **B2**, PowerPanel runs its command outside the session: the watcher only drains (`-NoShutdown`) at a **higher**
  runtime than PowerPanel's shutdown threshold, and PowerPanel still shuts down and turns the UPS output off.
  Example: watcher at 15 min, PowerPanel shutdown at 10 min. PowerPanel's command can stay (`stop-xtnl.ps1` then
  finds nothing running) or be removed.
  ```
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-ups-watch.ps1 -RuntimeMinutes 15 -NoShutdown -StartNow
  ```

The task `XTNL UPS watch` mirrors `XTNL engine`: at log-on of the current user (15 s delay), Interactive (run only
when logged on), limited rights, restart on a failed launch every 1 min, no time limit, **keeps running on
battery**. Re-running the install replaces it: a watcher that is already running keeps the arguments it started
with, so the installer stops it first and starts the new definition at once (`stopped the running 'XTNL UPS watch'`
in its output). Remove it with `install-ups-watch.ps1 -Uninstall`. Install it only
on the PC behind the UPS: on a laptop it would shut the laptop down when its own battery runs low.

What the watcher does, every `-PollSeconds` (30) reading `Win32_Battery`:

* **No battery reported** (no UPS, or its USB cable is out): one log line, then nothing; it keeps checking.
* **On mains:** nothing, ever. "On battery" needs both a discharging `Win32_Battery` (`BatteryStatus` 1) and a
  power line status that is not Online, so one odd reading never acts on mains power.
* **On battery** with charge <= `-ChargePercent` (45) or runtime <= `-RuntimeMinutes` (10): an `ALERT: UPS battery
  low` line, `stop-xtnl.ps1 -Containers` (at most 300 s), a fresh reading, then `shutdown /s /t 60` (B1), or
  nothing more with `-NoShutdown` (B2). The runtime is the main trigger; the charge threshold is a high backstop
  because the reported charge is non-linear. `0` turns a threshold off.
* **Mains back before the shutdown was issued:** no shutdown, and if the engine task was running before the watcher
  stopped it, `Start-ScheduledTask 'XTNL engine'` starts it again (its `compose up` brings the containers back).
  After a battery reading, mains counts as back only after two mains readings in a row, 5 s apart (the same rule
  as the countdown below), both in the main loop and in the re-check right after the stop. A single odd reading
  logs `one mains reading ... was not repeated` and the watcher stays on battery, so a glitch near the threshold
  never restarts the engine only to drain it again out of the remaining runtime.
* **Mains back during B1's 60 s countdown:** the watcher keeps reading the power every 5 s while the shutdown is
  pending; two readings in a row on mains run `shutdown /a` (log line `shutdown aborted (shutdown /a)`) and start
  the engine task again as above. Once the countdown has passed, the box stays off until RTC wake, Wake-on-LAN or a
  power cycle if mains returns before the battery is flat, because Windows cannot tell the UPS to cut its output.
  That is why path A or B2 is preferred.
* **Log:** `[ups-watch]` lines (and `[stop-xtnl]` for the stop's output) in `var\logs\xtnl-<yyyy-MM-dd>.log`, so
  `status-xtnl.ps1` lists its `ALERT:` lines too. It logs UPS charge and runtime only, no account value. Exit codes:
  0 shutdown issued and its countdown passed, `-Once` done or another copy running; 78 a parameter out of range or
  a missing file.

Look at what the UPS reports (safe at any time, it never acts):

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\ups-watch.ps1 -DryRun -Once
Get-CimInstance Win32_Battery | Format-List BatteryStatus, EstimatedChargeRemaining, EstimatedRunTime
```

On mains expect `BatteryStatus` 2 and `EstimatedRunTime` 71582788 (Windows' "unknown"); on battery `BatteryStatus`
1 and a runtime in minutes. *Unverified on the chosen UPS model:* that it reports both on battery (check with the
commands above while the UPS is unplugged); and that `shutdown /s` works for the standard `xtnl` user while its
session is connected over RDP (it works at the console).

**4. Test (home-server plan §4.13 step 6).** First a dry run: install with `-DryRun -StartNow` (path B), pull the
UPS plug, and check the log shows `on battery` and, at the threshold, `DRY RUN: threshold reached`; plug back in
(`mains power back`). To reach the threshold sooner, install the dry run with a runtime threshold just under what
the UPS reports. Re-install without `-DryRun`, with `-StartNow` and the thresholds you want (the installer stops the
dry-run copy first). Before the real test, check that the newest `ups-watch started` line in
`var\logs\xtnl-<yyyy-MM-dd>.log` names the real mode (`then shutdown /s` for B1, `-NoShutdown` for B2), not
`DRY RUN`: a dry-run copy still running would only log at the threshold. The same check after any re-install with
new thresholds. Then the real test: pull the UPS plug with everything running and
check that the box does **not** sleep in the first 15 minutes; at the threshold the engine drains and Windows shuts
down; the UPS cuts its output (path A / B2); the box powers back on when mains returns and the task starts. Then
`uptime show` (reason `SIGINT`), `catchup show` (`startup`, complete) and `status-xtnl.ps1`. Note the runtime.

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
`Select-String -Path var\logs\xtnl-*.log -Pattern 'ALERT:|PAGE\[critical\]' | Select-Object -Last 20`. 78 = configuration:
* the env file missing, or assigning a name of `$ForbiddenInFile` (Prerequisites, step 1; this includes
  `XTNL_HEARTBEAT_COPY_URL` / `_TOKEN` and `XTNL_LIVEVIEW_DSN`), or any `XTNL_BACKUP_*`, `RCLONE_*` or `B2_*` name
  ("backup credentials belong to the separate backup Windows account");
* a Windows User variable missing, or BROKER_ENV differing between the User environment and `local.env`;
* the engine's own configuration stop, for example `XTNL_DEADMAN_ALERT_AFTER_S` outside 60..3600 or not a whole
  number (default 300). The engine's `config: XTNL_DEADMAN_ALERT_AFTER_S must be ...` line names the variable: it
  has no prefix and comes just before `ALERT: configuration refused (exit 78; see the lines above)`; search the
  log for `XTNL_DEADMAN_ALERT_AFTER_S` (deadman-alert.md "Receiver not acknowledging").

`XTNL_DEADMAN_URL is set but XTNL_DEADMAN_TOKEN is empty` (an ERROR line at engine start) is **not** a stop: the
engine runs, but the receiver refuses every beat with 401 and nothing off the PC watches it. Set
`XTNL_DEADMAN_TOKEN` in the User environment and restart the task.

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
# Roles are cluster-wide, so pg_dump does not carry them, but the dump GRANTs to every role a migration creates
# (0011 xtnl_dashboard, 0014 xtnl_api, 0017 xtnl_arm, 0021 xtnl_liveview). Create them first with the migrations'
# own statements (NOLOGIN, no credential), each through its last ALTER ROLE line: role-level ALTER ROLE ... SET
# values are not in a dump either, and the grants after that line name tables not restored yet.
Get-Content src\db\migrations\0011_dashboard_role.sql -TotalCount 30 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
Get-Content src\db\migrations\0014_api_role.sql -TotalCount 30 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
# 0017's role block is lines 399-413 (its DO block): it creates xtnl_arm and grants it to xtnl, a membership
# that is only in the cluster's globals, never in the dump; the ARM pool runs as xtnl_arm (SET ROLE).
Get-Content src\db\migrations\0017_arm.sql | Select-Object -Skip 398 -First 15 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
Get-Content src\db\migrations\0021_liveview_role.sql -TotalCount 32 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
docker cp var\xtnl_live.dump xtnl-local-postgres-1:/tmp/xtnl_live.dump
docker exec xtnl-local-postgres-1 pg_restore -U xtnl -d xtnl --no-owner --exit-on-error /tmp/xtnl_live.dump
docker exec xtnl-local-postgres-1 rm -f /tmp/xtnl_live.dump
Remove-Item var\xtnl_live.dump      # it holds account data
$env:ORDER_SERVER_DSN = 'postgresql://xtnl:xtnl_local_only@127.0.0.1:55433/xtnl'
.venv\Scripts\python.exe -m src.db.migrate      # idempotent: "applied: none (up to date)" when the dump was current
```

Without the role step, `pg_restore --exit-on-error` stops at `role "xtnl_dashboard" does not exist` (or
`xtnl_api`, `xtnl_arm`, `xtnl_liveview`); without `--exit-on-error` it skips those GRANTs and the role later sees
nothing. A role step for a migration the dump does not have yet is harmless: the role is created NOLOGIN and the
later `src.db.migrate` run finds it and adds its grants. The roles come back NOLOGIN, so run
`deploy\local\new-liveview-credentials.ps1 -Rotate` and restart the task before using the live view
([live-view.md](live-view.md)), and `deploy\local\new-api-credentials.ps1 -DbRole` before using the API
(`xtnl_arm` never logs in: the ARM pool reaches it with `SET ROLE`). Compare row counts only
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

## Rebuild etcd with a new cluster token (home-server plan W18)

Use this when PostgreSQL keeps (or gets back) its data but the etcd volume `xtnl_local_etcd` is new: the volume was
deleted or lost, the PC was rebuilt, or PostgreSQL was restored from a backup
([home-backup-restore.md](home-backup-restore.md) Restore step 4; an off-site backup holds no etcd copy).

**Why a new token is needed.** etcd computes a new cluster's member and cluster ids from the peer URL and the
`--initial-cluster-token` alone (etcd 3.5 source; *unverified by a drill on this PC*, `epoch show` below checks it).
A fresh volume made from this compose file with the same token therefore gets the **same** `cluster_id`, with its
revision back near 1. `engine_epoch` still holds the old revision, so the engine's epoch bump (`src/db/epoch.py`,
`... AND revision < $2`) changes 0 rows and the engine is **FENCED** at every start, and
`epoch adopt` refuses with `already_adopted`, because the cluster_id did not change. So a new etcd volume does
**not** by itself mean a new cluster_id: give it a new token.

**How the token reaches compose.** The compose file reads
`--initial-cluster-token=${XTNL_ETCD_CLUSTER_TOKEN:-xtnl-local}` from the environment of the process that runs
`docker compose`. It matters only when the volume is created: etcd reads the `--initial-*` flags only while its
data directory is empty, and ignores them on an existing volume. So set it **only in the PowerShell that creates
the volume**, for that one command, as below. `start-xtnl.ps1` would pass a value on to compose too (it loads
`deploy\env\local.env` into its own process, and the task inherits the Windows User environment), but do **not**
put it in either: the live view refuses to start while any `XTNL_...TOKEN` name is in its environment
(`src/liveview/settings.py`; exit 78, the live view stays off until the next start), and the value would change
nothing after the volume exists. Afterwards the next `compose up` (the task's step 5) sees the default token
again and recreates the etcd container **once** (a `[compose]` line `Recreate`): the volume is kept, etcd ignores
the token, and the cluster_id stays the new one. That happens before the engine starts.

**Never reuse a token that any earlier etcd volume used, on any PC.** The cluster_id depends only on the token and
the peer URL, and the peer URL is the same on every PC built from this compose file, so a token used once on the
old desktop (or on a spare during a drill) recreates the cluster_id that `engine_epoch` may still store: a restore
onto a new PC with it is fenced, and `epoch adopt` refuses with `already_adopted`. A token "this PC has never used"
is not enough. Use the date-and-time form, `xtnl-local-rYYYYMMDD-HHMM` (for example `xtnl-local-r20261122-0930`),
which is unique per rebuild: a date alone is not, because a second rebuild on the same day (a lost volume, or a
retry after the adoption) would reuse it and recreate the cluster_id just adopted. Note it with the restore record.

```
# 0. Engine stopped, and the task cannot start it meanwhile (containers keep running):
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1
Disable-ScheduledTask -TaskName 'XTNL engine'      # skip (here and in step 6) when the task is not installed yet
# 1. What PostgreSQL holds now (cluster_id = the stored one; etcd_cluster_id = what etcd reports):
$env:ORDER_SERVER_DSN = 'postgresql://xtnl:xtnl_local_only@127.0.0.1:55433/xtnl'
$env:XTNL_ETCD_ENDPOINTS = 'http://127.0.0.1:52380,http://127.0.0.1:52380,http://127.0.0.1:52380'
.venv\Scripts\python.exe -m src.ops epoch show
# 2. Remove the etcd container and its volume ONLY (never `down -v`: that deletes PostgreSQL too):
docker compose -f deploy\local\docker-compose.local.yml rm -s -f etcd
docker volume rm xtnl_local_etcd
# 3. Create it again with a new token, set in this shell only:
$env:XTNL_ETCD_CLUSTER_TOKEN = 'xtnl-local-r20261122-0930'   # date and time now: never a token used before
docker compose -f deploy\local\docker-compose.local.yml up -d --wait etcd
Remove-Item Env:XTNL_ETCD_CLUSTER_TOKEN
# 4. etcd_cluster_id must now DIFFER from cluster_id (and the warning "etcd reports a different cluster_id" shows):
.venv\Scripts\python.exe -m src.ops epoch show
# 5. Adopt it, audited, with a second person (phone them; give the old and new cluster_id and the reason):
.venv\Scripts\python.exe -m src.ops epoch adopt <etcd_cluster_id> --from-cluster-id <cluster_id> `
    --operator <you> --approver <second person> --reason "etcd rebuilt with token xtnl-local-r20261122-0930"
# 6. Start again:
Enable-ScheduledTask -TaskName 'XTNL engine'
Start-ScheduledTask -TaskName 'XTNL engine'
```

* If step 4 still shows the **same** cluster_id, either the token did not reach compose (the variable was not set
  in that shell, or was empty: an empty value falls back to `xtnl-local`), or the token was used before by some
  etcd volume, on this or another PC: pick another one (the current date and time; if that form was already used,
add a suffix such as `-2`). Repeat steps 2-4.
* If `epoch show` says `engine_epoch` is absent (a fresh database, nothing restored), there is nothing to adopt:
  the first leader creates the row. Skip step 5.
* `epoch adopt` refuses with `cluster_id_not_reported_by_etcd` when the id you typed is not what etcd reports, and
  with `cluster_id_changed` when `--from-cluster-id` is not the stored one: copy both from step 4. The approver
  check only compares names; the second person is a procedure (home-server plan §3.2 step 5).
* Adopting sets the stored revision to 0, so the new cluster's first lease bumps it; `catchup show` after the start
  should list a `startup` catch-up, complete.

**Or carry the etcd volume over** (a move between two PCs that are both at hand, never from an off-site backup):
copy `xtnl_local_etcd` together with the PostgreSQL data **from the same moment**, both with the engine stopped
(`stop-xtnl.ps1 -Containers` on the old PC). Same cluster_id and a revision at least as high as the stored one, so
no fence and no adoption. With the `postgres:17` image already on both PCs (*unverified as written; check it in a
drill*):

```
# old PC, engine and containers stopped:
docker run --rm -v xtnl_local_etcd:/data -v "${PWD}\var:/out" postgres:17 tar -C /data -czf /out/xtnl_local_etcd.tgz .
# new PC, before its first `compose up`; then move PostgreSQL per "Carry over" above:
docker volume create xtnl_local_etcd
docker run --rm -v xtnl_local_etcd:/data -v "${PWD}\var:/in" postgres:17 tar -C /data -xzf /in/xtnl_local_etcd.tgz
```

Compose may warn that the volume `xtnl_local_etcd` already exists but was not created by Compose; it uses it.

## Rollback

Remove the autostart (stops the engine gracefully first):

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\uninstall-autostart.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-ups-watch.ps1 -Uninstall   # if installed
docker compose -f deploy\local\docker-compose.local.yml stop      # containers stopped, data kept
docker compose -f deploy\local\docker-compose.local.yml down      # containers removed, volumes kept
```

`down -v` (or `docker volume rm xtnl_local_pg xtnl_local_etcd`) deletes the whole engine history; do it only on
purpose. A new etcd volume made with the same token does **not** get a new `cluster_id`: it gets the same one with
its revision back near 1, so the engine fences and `epoch adopt` refuses (`already_adopted`). Rebuild it with a new
token and adopt, as in "Rebuild etcd with a new cluster token" above. To go
back to the development run instead: `deploy\local\run-local.ps1` with `deploy\env\local.env.example`
(throwaway stack, sandbox).
