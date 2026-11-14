# Declare a broker maintenance window

Phase 1 never sends orders. A declared window only tells the engine that an adapter's outage is expected;
nothing here touches a broker.

## Trigger

* A broker announces maintenance (StoneX CIAPI, Pepperstone / cTrader Open API), or the dead-man / liveness
  runbooks say "use a declared maintenance window instead" (`deadman-alert.md`).
* A window is over or was declared wrongly (remove or correct it).

What a declared window does (operations.md §10 BROKER_MAINTENANCE, SM10 overlay, per adapter):

* **during the window (the hold-back):** the adapter's liveness pages (conn-key / not STREAMING) and its
  accounts' FENCE alerts (`stale_account_image`, `stale_mark` and the `capped` / `uncapped` info rows) are held
  back, **only while the window lasts**: there is no settle after it (`src/oversee/inhibit.py`
  `AlertInhibitor`). The transitions are still written (`halt_breaches`, `oversee_events`); only the alert rows
  are held back, and risk alerts (every other rule's activations, raises, lowers, resolutions) are never held
  back. Its dead-man heartbeat carries `maintenance_until_ms`, so Melbourne does not page "adapter not
  STREAMING" either. A PRE_OPEN or roll readiness check that falls inside the window is deferred to its end;
* one pre-acknowledged **info** notice "maintenance: <adapter>" is sent when the window begins (none for an
  extension, an overlap, the end or a schedule reload);
* SM4 keeps its normal back-off throughout.

**At the end of the window** (the engine's own behaviour, operations.md §10 "FENCE is at full severity from
then on"):

* the hold-back stops **at once**, on the first RiskSweep pass after the end. From then on the adapter's stale /
  FENCE transitions alert normally, its still-active input FLAG states re-page every `escalate_after_ms` (an
  episode held back during the window re-pages from its `active_since`), and the conn-key liveness pages are live
  again while its market is open. Expect pages within seconds of the end if the adapter is still down;
* one **info summary** lists the accounts whose alerts were held back, naming those still fenced (expected: the
  adapter only reconnects at the end) and those still holding a `stale_mark` symbol;
* each symbol returns to its scheduled state. If an open (or a roll) passed during the window, the PRE_OPEN
  readiness check runs then ("maintenance ended after an open" / "... after a roll"): reset back-off, reconnect,
  wait up to **60 s** for STREAMING, fresh account snapshot and cash flows, token and chrony checks, and a page
  if the adapter is still not STREAMING;
* Melbourne: once `maintenance_until_ms` is in the past, "adapter not STREAMING" pages after its usual 2 min
  while the market is open.

A window hides real outages of that adapter for its whole length: declare only what the broker announced.

## Prerequisites

* The source is a reviewed JSON file, `XTNL_MAINTENANCE_FILE` (default `/etc/xtnl/maintenance.json`), re-read
  by the leader every 30 s and before SM10 is computed at LEADER_INIT. There is no database table or CLI write
  path for windows in Phase 1.
* The file must be **identical on node A and node B**. A window present only on the leader is silently gone
  after a failover (the new leader reads its own file), so its outage pages again.
* Format: `{adapter_id: [window, ...]}`; each window is `{"start": "<ISO-8601 instant with an offset>",
  "end": "...", "reason": "..."}` or `{"start_ms": <epoch ms>, "end_ms": <epoch ms>}`. Naive times, an end
  before the start, a non-list value or invalid JSON make the engine keep the windows it read before and page
  "maintenance windows unreadable" (it never silently declares or un-declares a window).
* Ticket with the broker's announcement; declare at least 2 min before the start (one re-read plus margin).

## Commands

1. Write the new file on your workstation (keep every window still needed; remove ended ones):
   ```
   {"stonex": [{"start": "2026-11-14T22:00:00+00:00", "end": "2026-11-15T02:00:00+00:00",
                "reason": "INC-123 CIAPI weekend maintenance"}]}
   ```
2. Validate it with the engine's own reader (offline, no database), then copy it to **both** engine nodes:
   ```
   cd /opt/xtnl/order-server
   OPS=".venv/bin/python -m src.ops"
   $OPS maintenance check --file /tmp/maintenance.json      # exit 3 = the engine would refuse it
   ```
3. On node A, then node B, install it atomically (the engine never reads a half-written file):
   ```
   sudo install -m 0640 -o root -g xtnl /tmp/maintenance.json /etc/xtnl/maintenance.json.new
   sudo mv /etc/xtnl/maintenance.json.new /etc/xtnl/maintenance.json
   $OPS maintenance check                                    # reads XTNL_MAINTENANCE_FILE / the default path
   ```
   The file is 0640 root:xtnl: run the check as a member of group `xtnl` (or `sudo -u xtnl`); an unreadable
   file is `refused: unreadable` (exit 3), never a silent pass. Your shell does not load `engine.env`: if it
   sets `XTNL_MAINTENANCE_FILE` to another path, pass that path with `--file`. `refused: changed` means the
   file was replaced during the check; run it again.
4. Compare the two `sha256:` lines of step 3 (node A and node B). They must be equal; if not, repeat step 3
   on the node that differs.

To end a window early or remove one, edit the file the same way (both nodes). An empty file or `{}` declares
no window.

## Checks

* Both nodes: `$OPS maintenance check` -> `outcome: ok`, the same `sha256`, each window listed with
  `state=future|active`; no `has already ended` warning left in the file.
* Leader, within 30 s of the copy, the engine logs the new list (adapter and UTC range); no "maintenance
  windows unreadable" page:
  ```
  journalctl -u xtnl-engine --since "-2 min" | grep "declared maintenance windows"
  ```
* At the start: the info notice "maintenance: <adapter> until <UTC end>" arrives; the leader logs
  `<adapter>: BROKER_MAINTENANCE begins`; Melbourne receives `maintenance_until_ms` for the adapter and does
  not page "not STREAMING".
* At the end: `BROKER_MAINTENANCE ends`, the info summary of the held-back accounts arrives, and the adapter
  returns to STREAMING within about a minute. If it does not, FENCE / liveness pages arrive at full severity
  straight away (no 30 s grace), and the readiness check pages "not STREAMING" after its 60 s wait when an open
  passed during the window. Follow `deadman-alert.md` / `off-node-alerts.md` as for any outage, or extend the
  window if the broker announced an overrun.

## Rollback

Remove the window from the file on both nodes (steps 2-4). The leader drops the overlay at its next re-read
(30 s); suppressed liveness / FENCE pages fire again if the adapter is still down.
