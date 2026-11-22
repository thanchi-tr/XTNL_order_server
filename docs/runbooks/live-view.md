# Live view: the local read-only page of what the order-server is doing (single-PC mode)

`python -m src.liveview` serves a page on this PC only, `http://127.0.0.1:8090/` (stories, status band, day panel,
console) and the static Phase 2 shell page `http://127.0.0.1:8090/edge`. The live view **never sends orders** and
has no write path of any kind: no POST route, no operator command, no Redis connection, no broker access. It reads
PostgreSQL only as role `xtnl_liveview` (migration 0021: `SELECT` on the `lv_` views only, read-only sessions),
plus the engine's own loopback metrics endpoint (nine allow-listed names: exporter up, role, SAFE_HOLD, epoch,
adapter states, RiskSweep pass age and the three `xtnl_deadman_*` gauges; docs/design/live-view.md §4.6). The
Engine tile shows the dead-man receiver as the leader sees it: `dead-man receiver: OK, last ack N s ago`,
`not acknowledging, N failed beats` (see deadman-alert.md "Receiver not acknowledging") or `not configured`. With
no failed beat but an old last ack it shows `no beat sent or acknowledged for N s`: amber past 30 s (two missed
beats), red past 60 s. That means the engine stopped sending beats (dead-man task down or stuck), not that the
receiver refused them. It
shows ids, times, codes, counts and order levels (entry, stop, take-profit), never a size, money figure, pip or
point size, spread or market-data value.

`start-xtnl.ps1` runs it as its third supervised child (after the engine and the order-server API), inside step 8
only, its lines prefixed `[liveview] ` in the daily log `var\logs\xtnl-<yyyy-MM-dd>.log`. It is down whenever
Docker, the migrations or the clock wait are retrying, so the page being unreachable means the engine is not
running either. Design: `docs/design/live-view.md`.

## Trigger

* You want the live view on this PC for the first time (Steps 1-4).
* You rotate its database password (suspected leak, routine) or carried the database over (Rotation).
* The log shows `live view refused its configuration or its login (exit 78 ...)` or the page says `db: down`
  (Checks).
* You want it off again (Rollback).

## Prerequisites

* The single-PC engine is installed and starts (docs/runbooks/single-pc-autostart.md).
* You are the owner: the steps edit `deploy\env\local.env` and the Windows User environment, which only you do.

## Steps

### 1. Apply migration 0021 (accepted level 21)

Migration 0021 creates the role `xtnl_liveview` (NOLOGIN, no credential in any file) and its `lv_` views. A start
applies migrations only up to `XTNL_MIGRATIONS_ACCEPTED_UPTO` (default 14). Edit `deploy\env\local.env` yourself
and set:

```
XTNL_MIGRATIONS_ACCEPTED_UPTO=21
```

This also applies 0015-0020 (the market-data / ARM schema); `XTNL_MD_ENABLED` and `XTNL_ARM_ENABLED` stay 0, so
nothing of the ARM runs. Then restart the engine task so step 6 of `start-xtnl.ps1` applies them:

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1
Start-ScheduledTask -TaskName 'XTNL engine'
```

The log shows the `[migrate]` lines; nothing is held back below 21 any more.

### 2. Give the role its login

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-liveview-credentials.ps1
```

It checks that `xtnl_liveview` exists, gives it a random 32-byte password inside the postgres container (sent on
psql's stdin, never on a command line) and stores `XTNL_LIVEVIEW_DSN` (`postgresql://xtnl_liveview:...@127.0.0.1:55433/xtnl`)
in the Windows User environment only. The password and the DSN are never printed or written to a file. An
existing `XTNL_LIVEVIEW_DSN` is kept unless you pass `-Rotate`. `role xtnl_liveview not found` means step 1 has
not run yet.

### 3. Restart the engine task

`start-xtnl.ps1` reads the Windows User environment once, at its start:

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1
Start-ScheduledTask -TaskName 'XTNL engine'
```

The log then says `live view enabled (XTNL_LIVEVIEW_DSN is set): http://127.0.0.1:8090/ ...` and, once the engine
runs, `starting the live view: python -m src.liveview --stop-on-stdin-eof`.

### 4. Open the pages

* `http://127.0.0.1:8090/`: the live page.
* `http://127.0.0.1:8090/edge`: the empty Phase 2 shell page (no controls; nothing on it acts).

Only literal loopback addresses are accepted: open `127.0.0.1`, not `localhost` (a wrong `Host` gets 421). If port
8090 is taken, set `XTNL_LIVEVIEW_ADDR=127.0.0.1:<port>` in `deploy\env\local.env` (127.0.0.0/8 or `[::1]` only; a
name, `localhost` included, is refused with exit 78) and restart the task.

## Checks

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\status-xtnl.ps1
Select-String -Path var\logs\xtnl-*.log -Pattern '\[liveview\]|live view' | Select-Object -Last 20
```

`status-xtnl.ps1` prints `live view: listening on <configured address>` (it connects to `XTNL_LIVEVIEW_ADDR`, default
`127.0.0.1:8090`) or `nothing listens on ...`, and says when `XTNL_LIVEVIEW_DSN` is not set (name only).

**`db: down` on the page vs. a refused login.** They mean different things:

* `db: down` (the page stays up, the service retries every 1 s doubling to 10 s): PostgreSQL itself cannot be
  reached (a socket or connect failure, or a temporary refusal `57P03` / `53300`). The engine cannot record either:
  look at the containers (`docker compose -f deploy\local\docker-compose.local.yml ps`) and the `[start-xtnl]`
  lines.
* A refused login (SQLSTATE `28000`, `28P01`, `3D000`) is a configuration error, not an outage: the service exits
  78 naming `XTNL_LIVEVIEW_DSN` and the page goes away, while the engine runs normally. Causes: migration 0021 not
  applied (step 1), the role still NOLOGIN (after a database carry-over), or a stale password (`-Rotate` without a
  restart). Fix: run step 2 (with `-Rotate` when `XTNL_LIVEVIEW_DSN` is already set), then step 3.

**Exit 78** (the log line `live view refused its configuration or its login (exit 78; see the [liveview] lines);
disabled until the next start`): `start-xtnl.ps1` leaves it off until the task is started again; the engine and
the API are not affected. The `[liveview]` lines just before it name the variable (never a value): a bad
`XTNL_LIVEVIEW_*` setting (address not literal loopback, a value out of bounds), a refused name in its environment
(for example an `XTNL_*` variable named like a password, secret, token, DSN or key that you set in the Windows User
environment yourself; remove it or ask for it to be added to `$LiveViewDropEnv`; the heartbeat copy
`XTNL_HEARTBEAT_COPY_URL` / `_TOKEN` and any `XTNL_BACKUP_*`, `RCLONE_*` or `B2_*` name are refused too and dropped
by `start-xtnl.ps1`), a DSN that is not the
`xtnl_liveview` role, or a refused login (above). Fix the cause, then restart the task. Any other exit is restarted
after 5 s, doubling to 5 min.

## Rotation

```
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-liveview-credentials.ps1 -Rotate
powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1
Start-ScheduledTask -TaskName 'XTNL engine'
```

Always restart after `-Rotate`: the running live view still holds the old DSN, and its next login is refused (exit
78) until `start-xtnl.ps1` starts again with the new one. After a database carry-over
(single-pc-autostart.md, "Carry over the earlier week's data") the role is re-created NOLOGIN, so run `-Rotate`
and restart too.

## Rollback

The role can stay: it is NOLOGIN until step 2 and can only read the `lv_` views. Lowering
`XTNL_MIGRATIONS_ACCEPTED_UPTO` again does not undo an applied migration.

* Off for one manual run (the scheduled task runs without it):
  ```
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\start-xtnl.ps1 -NoLiveView
  ```
* Off for good: remove the variable and restart the task.
  ```
  [Environment]::SetEnvironmentVariable('XTNL_LIVEVIEW_DSN', $null, 'User')
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1
  Start-ScheduledTask -TaskName 'XTNL engine'
  ```
  The log then says `live view disabled: XTNL_LIVEVIEW_DSN is not set ...`. To also take the login away:
  ```
  docker exec xtnl-local-postgres-1 psql -U xtnl -d xtnl -c "ALTER ROLE xtnl_liveview NOLOGIN"
  ```
