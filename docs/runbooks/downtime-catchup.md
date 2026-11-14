# Downtime catch-up (single-PC mode: the engine is not on 24/7)

Phase 1 never sends orders. The catch-up only **reads** from the brokers; nothing here places, modifies or
cancels an order or closes a position. Pages, logs and the two views below carry counts, ids and times only,
never an amount.

Rules: `.claude/rules/operations.md` §17 (single-PC mode), `oversee-rules.md` §8 (`catchup_unverified`),
`adapter-isolation.md` §4 (`fetch_history`).

## Trigger

* PAGE[info] `downtime catch-up: <adapter>`: sent after every start-up and every wake from sleep, and after a
  broker reconnect or a retry that found something. It says how long the engine was off and what it found.
  Nothing to do unless the numbers surprise you.
* PAGE[high] `downtime catch-up incomplete: <adapter>`: the engine could not prove that it knows every order
  the broker changed while it was off. Follow **What to do on an incomplete catch-up** below.
* PAGE[high] `clock moved back: <adapter>` (body says `clock_moved_back`): the adapter's coverage is later than
  the PC clock now, so the clock was stepped back after it was written. Follow **Clock moved back** below.

## What happens automatically

1. **The engine records when it is on.** At start an `engine_runs` row (run id, node, start time); every
   `XTNL_UPTIME_PING_S` (default 900 s, 60–1800) a light heartbeat (`last_seen`); at a clean stop
   (Ctrl+C, SIGTERM) the stop time and reason. A crash, a hard exit or a power cut leaves the row without a
   stop: its last heartbeat is the last time the engine is known to have been on.
2. **Coverage.** While the engine is the leader and an adapter is streaming, reconciled and caught up, the same
   heartbeat records `adapter_coverage.last_streaming`: the engine knows everything that adapter's broker did
   up to that time.
3. **Start-up.** Before the adapters connect, each adapter's fence is held (ActionGate denies risk-increasing
   actions, Oversee treats the account as in a gap). After the first snapshot is committed, the engine asks the
   broker for its history from `last_streaming − XTNL_CATCHUP_OVERLAP_S` (default 900 s) to now: orders
   placed and finished, positions opened and closed, stops placed and cancelled, working orders that changed,
   and cash flows. Every row goes through the normal path (newer version wins, so an older history row never
   overwrites newer state). Stored working orders / open positions the broker listed nowhere are re-read by id.
   One `catchup_runs` row is written, the pages go out, and the fence is released. Without a coverage row (the
   first run, an adapter added later, or every earlier catch-up failed) the window starts at the oldest of: the
   last `XTNL_CATCHUP_FIRST_RUN_DAYS` days (default 7), the oldest stored working order / open position, and the
   last time the engine is known to have been on before this run (the earliest end of any earlier
   `engine_runs` row, and the newest `event_log` row written before this run). A window that would reach back
   more than 365 days is cut there and reported incomplete, naming the part not read.
4. **Sleep.** When the PC wakes, the engine notices the wall-clock jump (`suspended_gaps` +1), re-subscribes every
   broker stream and runs a `resume_after_sleep` catch-up from the coverage written before the sleep. If the
   sleep outlived the leader lease, the engine hard-exits instead; `deploy/local/run-local.ps1` restarts it
   (exit codes 70–79, back-off 5 s doubling to 120 s) and the restart runs a `startup` catch-up.
5. **Broker drop while running.** A lost socket, a gap or a lost login on the orders / account path stops
   coverage at once; after the reconnect a `reconnect` catch-up re-reads the history for that period.
6. **A failed catch-up** (broker unreachable, reads or single history pages timing out or refused, rows the
   adapter could not map, rows not committed) is retried after 5 min, doubling to 1 h; coverage does not move
   meanwhile, so the next start-up re-reads the same window. Only a genuine limit (retention, the broker's own
   paging limit, the StoneX switches, no history facility, the 365-day cut, a clock stepped back) is paged once
   while coverage moves on. The page says `retry in ...` and `adapter_coverage held back` when it will retry.
   Rows the adapter cannot map during a catch-up never count as a broker drop, so they never start a
   `reconnect` loop; a live row skipped while a catch-up runs still counts as one.
7. **ActionGate.** Until the adapter's newest catch-up in this run is complete, risk-increasing actions for that
   adapter are denied (`catchup_unverified`). Phase 1 sends nothing; this protects the future sender.

## Checks

```
.venv\Scripts\python.exe -m src.ops uptime show --limit 10      # when was the engine on, offline gaps, coverage
.venv\Scripts\python.exe -m src.ops catchup show --limit 10     # every catch-up: window, counts, complete
```

Expected after a normal start-up: the newest run is `running or no clean stop`, the previous one `stopped`
with its reason (or `no clean stop` after a crash or power cut), the newest catch-up per adapter is
`complete = true`, and each adapter's coverage is within one heartbeat interval of now.

## How to read `uptime show`

```
.venv\Scripts\python.exe -m src.ops uptime show [--limit 20] [--node dev1]
```

* **runs** (newest first): `started`, `last_seen` (last heartbeat), `stopped` + `reason`, `suspended_gaps`
  (times the PC slept during that run), `up`, and `state`:
  * `stopped`: a clean stop (`SIGINT` = Ctrl+C, `SIGTERM`, `stopped`, `fenced`, `error:<type>`);
  * `no clean stop`: an older run that ended in a crash, hard exit or power cut; it was on at least until
    `last_seen` (up to one heartbeat interval more);
  * `running or no clean stop`: the newest run of the node.
* **offline gaps**: from the end of one run (its stop, else its last heartbeat) to the start of the next.
* **adapter coverage**: per adapter, the time up to which the engine is known to have everything
  (`last_streaming`). The next catch-up reads from here minus the overlap. A coverage much older than the
  newest run means the adapter has not caught up since (check `catchup show`).

## How to read `catchup show`

```
.venv\Scripts\python.exe -m src.ops catchup show [--limit 20] [--adapter stonex]
```

One row per catch-up, newest first:

* `reason`: `startup`, `resume_after_sleep`, `reconnect` (broker drop while running), `retry` (an earlier one
  failed), `operator`.
* `window`: what was asked of the broker; `covered_from`: from when the broker's answer actually covers (later
  than the window start when the broker keeps less history); `offline_from`: the coverage the window started
  from (empty on a first run).
* Counts are what the catch-up added on top of the start-up snapshot (the snapshot already holds every order
  and position that is still open): `orders_new` (mostly orders placed and finished while off),
  `orders_updated`, `positions_new`, `positions_closed`, `cash_flows_new`, and `anomalies` (stored working
  orders / open positions the broker no longer knows by id: never treated as closed).
* `complete` and `notes`: why it is incomplete, without amounts.

## What to do on an incomplete catch-up

1. Read the page and `catchup show --adapter <adapter> --limit 5`: the `notes` say why, the page names the
   **uncovered window**.
2. By cause:
   * **`history incomplete` from StoneX with a paging / stop-limit note**: expected while
     `STONE_X_STOP_LIMIT_HISTORY` / `STONE_X_HISTORY_PAGING_CONFIRMED` are 0 (the StoneX history endpoint and its
     paging are not confirmed yet). Check the StoneX platform's order history for the uncovered window.
     Turn the switches on only after the live read-only probe confirmed them (`live-readonly-smoke-test.md`).
   * **retention** (`broker history retention starts after the gap start`): the engine was off longer than the
     broker keeps history. Check the broker platform for the part before `covered_from`.
   * **`catch-up failed: <error> after N attempts`**, **`history incomplete (retryable)`** (a failed page, a
     timeout, a throttle / maintenance code, `rows the adapter could not map`), **by-id re-reads failed**,
     **rows not committed**, **parked**: a problem on this side or a passing one. It retries by itself (5 min,
     doubling to 1 h) and coverage is held back. Fix the cause (broker reachable? PostgreSQL up? `dlq list`;
     for unmappable rows, the ERROR log names the row), or restart the engine: the next start-up re-reads the
     same window.
   * **`history incomplete (broker limit)`**: the broker cannot answer better (its paging limit, the 13-week
     cTrader request cap, the StoneX switches); paged once, no retry. Check the broker platform for the
     uncovered window.
   * **`no coverage recorded and the window would start at ...: cut at 365 days`**: the engine was last known
     on more than a year ago (or the database holds very old runs). Check the broker platform for the uncovered
     window; the next catch-ups start from the new coverage.
   * **`stored working entities not found by id`** (anomalies, ids in the page): the broker no longer knows an
     order / position the engine has as working. Look the ids up in the broker platform. The engine never closes
     them on its own.
   * **`adapter has no fetch_history`**: that adapter cannot list its history; check the broker platform for
     the whole window.
3. **Clock moved back** (`clock moved back: <adapter>`, notes `clock moved back: adapter_coverage ... is ahead
   of the wall clock`): the offline span is unknown, so the window was read as if no coverage were recorded
   (the first-run window: `XTNL_CATCHUP_FIRST_RUN_DAYS`, the stored working entities and the engine's last known
   active time; never less than the overlap). Check the PC clock (Windows time sync), then check the broker
   platform for the time since the engine was last on, before the window start if it was off longer.
   Coverage cannot move forward until the clock passes the stored value; until a later catch-up is complete,
   ActionGate keeps the adapter's risk-increasing actions denied.
4. If the broker platform shows an order or fill the engine does not have, it will come in with the next
   snapshot only if it is still open; a finished one shows up only through a complete catch-up. Restart the
   engine (or wait for the next wake / reconnect) once the cause is fixed and confirm the new row in
   `catchup show` is `complete`.
5. Until a catch-up of the current run is complete, ActionGate keeps that adapter's risk-increasing actions
   denied. There is no override in Phase 1 (nothing sends orders).

## Rollback

The catch-up changes no broker state and writes only through the engine's normal versioned path, so there is
nothing to undo. Settings can be rolled back:

* A StoneX history switch turned on that proves wrong: set it back to 0 in `deploy/env/local.env` and restart;
  the next catch-ups are again reported incomplete (fail closed).
* A restart loop you do not want: stop the script (Ctrl+C) and run it once with `-NoRestart`.
* `XTNL_UPTIME_PING_S` / `XTNL_CATCHUP_OVERLAP_S` / `XTNL_CATCHUP_FIRST_RUN_DAYS`: restore the previous values and
  restart (a value the engine does not accept is refused at start-up, by name).

## Keeping the gaps short (Windows PC)

* Start the containers and the engine at logon: `docker compose -f deploy/docker-compose.dev.yml up -d --wait`,
  then `powershell -ExecutionPolicy Bypass -File deploy/local/run-local.ps1`. Setting this up in Task
  Scheduler, and the PC's sleep settings, are the PC owner's choice; the engine does not change them.
* Prefer a clean stop (Ctrl+C) before shutting the PC down: the run gets its stop time and reason.
* `XTNL_UPTIME_PING_S` lower (e.g. 300) shortens the uncertainty after a crash; `XTNL_CATCHUP_OVERLAP_S` must
  stay at least that value and at least 300 s.
