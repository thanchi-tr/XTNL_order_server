# Dead-man's switch alert (Melbourne)

Phase 1 never sends orders. The dead-man page means oversight may be blind; it never means an order is at risk
from this system.

## Trigger

The Melbourne receiver pages (through a channel outside eu-west-2) when:

* no heartbeat for 60 s (the leader POSTs every 15 s);
* any adapter not STREAMING for 2 min while its `market_open` is true and it is not in maintenance;
* a payload field is missing (at once: the first beat with a null health field pages), or an age passes its
  threshold: oldest un-acked feed entry > 30 s, any
  account's RiskSweep pass > 10 s, oldest undelivered alert > 60 s, SM8 not READY or SM5 UNKNOWN > 60 s,
  spill file non-empty > 60 s;
* any task circuit is open (`degraded: <task>`);
* two engines beat at the same time (`two_engines`, critical), or the beating node changed (`node_changed`, a
  lower-severity notice) (W19).

The **engine** pages too when the receiver itself stops answering: `PAGE[high] dead-man receiver not
acknowledging` (home-server plan W1), see "Receiver not acknowledging (engine page)" below. The receiver cannot page
about its own death; that page and Healthchecks.io check B cover it.

A daily test page proves the path; a missing daily test page is itself an incident.

The receiver is `src/deadman_receiver` (`xtnl-deadman-receiver.service` on the Melbourne host, install steps in
`install-node.md`; for the single home PC it runs on a small VPS over Tailscale instead, see
[deadman-receiver-home.md](deadman-receiver-home.md)). Every page subject starts `[XTNL DEADMAN]`; the body names the condition, the receiver time and
the last beat (node, epoch, gate, halt level). Conditions (operations.md §9 thresholds):

| Page subject | Condition key | Fires when |
|---|---|---|
| `no heartbeat for N s` | `silence` | no accepted beat for > 60 s (also > 60 s after the receiver starts) |
| `malformed heartbeat` | `payload_invalid` | a field missing / wrong type, or `sent_at_ms` > 5 min off the engine clock carried on from the latest accepted beat (after a silence: off both that and the receiver clock; before the first accepted beat: off both the receiver clock and its start time carried on by its monotonic clock; review rounds 8-10, below). Stays active until an accepted beat came after the last bad POST and none has come for 40 s (review round 5), so a second sender does not page and resolve at every beat. The text adds `valid beats are also arriving` when good beats come between bad ones: look for a second, older engine ("Two engines" below) |
| `adapter <a> not STREAMING while its market is open` | `adapter_not_streaming:<a>` | >= 2 min, `market_open` true, no maintenance window |
| `adapter <a> state missing` / `heartbeat field <f> missing` | `adapter_fields_missing:<a>` / `fields_missing:<f>` | null in the latest beat (at once, no grace) |
| `oversee feed entry un-acked > 30 s` | `feed_lag` | oldest un-acked feed entry > 30 s |
| `RiskSweep pass older than 10 s` | `risksweep_pass_age` | any account's pass > 10 s |
| `alert undelivered > 60 s` | `notification_undelivered` | oldest undelivered `notification_outbox` row > 60 s |
| `SM8 not READY > 60 s` / `SM5 UNKNOWN > 60 s` | `sm8_not_ready` / `sm5_unknown` | for > 60 s |
| `spill file non-empty > 60 s` | `spill_nonempty` | for > 60 s |
| `task circuit open (<task>)` | `circuit_open:<task>` | the task is in `degraded` |
| `two engines beating (<nodes>)` | `two_engines` | two engine sources (node, epoch) each sent >= 2 beats inside the other's span, by receive time and by `sent_at_ms`, within max(`XTNL_RECEIVER_TWO_ENGINES_WINDOW_S` (60 s), 90 s); or one source sent >= 6 beats in the window with a mean interval under 12 s by receive time (monotonic) and by `sent_at_ms` (two engines sharing one node name and epoch; one engine beats every 15 s); beats refused as not newer count too. Once seen it stays active until a full window passes without it (one page per incident, review round 4). Clears about 90 s + one window after one of them stops |
| `beating node changed (<old> -> <new>)` | `node_changed` | severity **high** (a notice): the latest beat's node differs from the one before; stays active 5 min after the last change. Expected after an AWS failover, and once after you change `XTNL_NODE_ID` on the same box (for example `pc1 -> home1`); otherwise unexpected on the single home PC |
| `receiver token source invalid` | `token_source` | the receiver's secret has no / a short `XTNL_RECEIVER_TOKEN` (or `_NEXT`), or cannot be read; the last valid tokens stay accepted |

`SM5 UNKNOWN` is reported by the engine whenever its gate would deny for want of a current `halt:state`: SM8 not
READY, or `halt:state` missing / not carrying `MAX(halt_events.id)` (also before the first `halt_events` row).

A condition pages once when it starts, again every `XTNL_RECEIVER_REPAGE_S` (15 min) while it lasts, and sends a
`resolved:` notice when it clears. Every page is critical except `node_changed`, which pages (and re-pages) as
high. The status line in every page lists the beats per source in the last window
(`beats in the last 60 s: <node> epoch <e> xN, ...`). `receiver started` (info) means the receiver process restarted.

## Checks

```
$ETCDCTL get /xtnl/leader                       # is there a leader?
$ETCDCTL get --prefix /xtnl/health/             # are both nodes alive?
$OPS safe-hold status                          # SAFE_HOLD in PostgreSQL and its etcd mirror
journalctl -u xtnl-engine --since "-15 min" --no-pager | grep -E 'WARNING|ERROR|CRITICAL'      # on the leader
curl -s -o /dev/null -w "%{http_code}\n" https://<deadman-host>:8443/healthz   # from the leader: 200 = Melbourne reachable
# home PC: curl.exe -s -o NUL -w "%{http_code}`n" https://<vps>.<tailnet>.ts.net/healthz   (deadman-receiver-home.md)
# on the Melbourne host:
systemctl status xtnl-deadman-receiver
sudo journalctl -u xtnl-deadman-receiver --since "-30 min" --no-pager | grep -E 'WARNING|ERROR|CRITICAL'   # rejected beats ('wrong bearer token' / 'rejected:'), 'page via' failures
```

Both services write their log lines to stderr with no syslog priority prefix, so journald stores every line at
priority info and `journalctl -p warning` hides the warnings. Grep for the level word, as above. Run the
receiver's `journalctl` with `sudo`: an operator login outside the `adm` / `systemd-journal` groups gets
`-- No entries --`, which means no access, not a clean journal (review round 6).

## Commands by cause

| Cause | Go to |
|---|---|
| No leader, SAFE_HOLD set | `safe-hold.md` |
| No leader, no SAFE_HOLD | `failover-and-switchover.md` (no LEADER > 30 s) |
| Leader alive, Melbourne unreachable | network / VPN path; the off-node London alerts still cover HA |
| `malformed heartbeat` / beats refused 401 | engine and receiver versions or tokens differ: `XTNL_DEADMAN_TOKEN` (engine) must equal `XTNL_RECEIVER_TOKEN`, `XTNL_RECEIVER_TOKEN_NEXT` or a token replaced less than `XTNL_RECEIVER_TOKEN_OVERLAP_S` ago (see "Token rotation"); chrony on both sides for a clock-skew refusal |
| `receiver token source invalid` | fix the receiver's secret (ap-southeast-4) or env file: `XTNL_RECEIVER_TOKEN` (>= 16 characters) present; the page resolves at the next refresh (<= 5 min, or `systemctl restart xtnl-deadman-receiver`) |
| No daily test page / `page via <backend> failed` in the receiver journal | the Melbourne channel (SES ap-southeast-4, SMTP relay, push provider) is failing: fix its credentials in `/etc/xtnl/deadman-receiver.env`, `systemctl restart xtnl-deadman-receiver` |
| Adapter not STREAMING while OPEN | adapter logs; `auth-failed-rearm.md` if AUTH_FAILED; else broker status page |
| RiskSweep pass age / circuit open | `journalctl ... | grep -i risksweep`; > 5 min open on the leader moves leadership once per incident |
| SM8 not READY | `redis-loss.md` |
| Spill file / undelivered alert | `dlq-quarantine.md`; check SMTP / SES / push provider status |
| `two engines beating` | "Two engines" below, at once |
| `beating node changed` | AWS: check that a failover happened (`$ETCDCTL get /xtnl/leader`, `failover-and-switchover.md`); it resolves after 5 min. Single home PC: if you just changed `XTNL_NODE_ID` on this box (for example `pc1 -> home1`, as `start-xtnl.ps1` advises) and restarted, one notice is expected; it clears after 5 min and needs no action (review round 6). Only a change you did not make means a second machine beats under another node name: "Two engines" below |
| Task circuit open | find the task in the journal; after the fix, `$OPS task reset <task> --operator <you> --reason "<ticket>"` (an audited request the current leader consumes; a new leader starts with closed circuits) |

## Two engines

Only the leader beats, so two sources beating together mean two engines run (W19; home-server-plan §4.12). The
page names each source's node, epoch, beat count and last receive time. A source is (node, epoch), and the epoch
is only the engine's lease revision: two home PCs started from the same env file share `XTNL_NODE_ID`, and two
fresh etcd volumes from the same compose file get the same cluster_id and the same low revisions, so both can beat
as one source (for example `pc1 epoch 5`). The receiver catches that by the rate: one source with 6 or more beats
in the window at a mean interval under 12 s, both by the receiver's monotonic clock and by the beats' own
`sent_at_ms` (the page then names the source with `mean interval N s (one engine beats every 15 s)`). The
monotonic clock means a step of the VPS wall clock never makes one engine look like two, and the `sent_at_ms`
test means a stalled receiver handling one engine's queued beats at once does not either (review round 2). The
interleave of two sources is also checked on both clocks (review round 4), so a clean restart whose last old-term
beats and first new-term beats a stalled receiver handles in the same millisecond is not two engines. The
interleave test looks back over at least 90 s (or the window when longer): an engine whose PostgreSQL does not
answer beats about every 22 s, and a shorter look-back would hold too few beats. Once the page is raised it stays
active until a full window passes without either rule seeing two engines, so a beat that briefly drops out of the
look-back no longer sends a resolved notice and a new critical page each time.

Each fault has **one** timer and one page, whichever engine shows it (review round 9; rounds 5-8 kept one timer
per source (node, epoch), and each round's fix left a new case with a false resolved notice and a second page).
The timer starts at the first beat that shows the fault, and every later beat that shows it, from any engine,
keeps it going. A "for > 60 s" fault pages once it has held that long: counted up to now while the newest beat
shows it, otherwise only up to the last beat that showed it (while two engines beat: up to that engine's next
expected beat at most, below), so an engine that no longer shows a fault, or that stopped, never starts a new page,
and a fault that held for less than its threshold never pages. A fault ends only when a later beat did not show it
**and** no beat has shown it for 40 s. So a cleared fault resolves 40 s after the last beat that showed it (within
one beat period + 40 s), and a fault that clears for less than 40 s and comes back stays one page. With no later
beat at all (a silence, or the gap of an engine restart or failover) the fault stays. When the receiver stalls and
then handles queued beats at one instant, a beat that shows the fault continues it when it was sent within 40 s of
the last beat that showed it (review round 10): the stall alone never ends a fault.

What this gives you:

* A fault that carries on through an engine restart (a new epoch) or a failover is one page, with no false
  resolved notice: an adapter stuck in BACKOFF, SM8 not READY, a spill file. This holds whatever the two engines'
  clocks say, when a stalled receiver handles the old term's last beat and the new term's first beat in the same
  instant, and when the new leader's first beats are refused as not newer. A fault that starts just before the
  change pages on time.
* While two engines beat, a fault only one of them shows (a task circuit open, SM8 not READY on the main box) pages
  once and stays active; the other engine's beats neither clear it nor restart its timer. Its page ends with
  `(from <node> epoch <epoch>)` when the engine that sent the newest beat does not show it. While `two engines
  beating` is active or held, a "for > 60 s" timer of a fault the newest beat does not show counts up to the
  showing engine's next expected beat (its last showing beat plus the interval before it), so the fault pages
  within one beat of its threshold, but a fault that engine showed for less than its threshold and then cleared
  never pages (review round 10). A fault only the engine that stopped showed resolves about 40 s after its last
  beat, once nobody shows it. Two engines on one source (same node and epoch) are told apart by the rate rule
  only; the round-7 "two beats < 12 s apart" test is gone, because a single engine restarting within seconds
  under the same source made such a pair and then paged a fault that had already ended.
* After a clean failover the old leader's last faults resolve up to 40 s after its last beat. A late last beat of
  the old leader (its bytes sat behind `tailscale serve`) that arrives after the new leader's first beat, more than
  15 s behind the engine clock, only keeps a fault going that is still active; it never pages a fault again and
  counts for `two engines beating` only. The engine clock is estimated from the least delayed of the last four
  accepted beats (review round 10), so a new leader's first beat that was itself delivered late does not make an old
  beat look fresh.
* A malformed POST is not a beat and never touches a fault's timer.

All receiver timers run on its monotonic clock (review round 7): silence, the fault timers, the 40 s, and the
two-engine and node-change holds. A step of the VPS wall clock (a chrony or timesyncd correction) therefore sends
no false `no heartbeat` page, no false resolved notice and no second page. Page texts still show wall time.
Clock-skew check (review rounds 8 and 9): while the engine is live (an accepted beat in the last 60 s), a beat
whose `sent_at_ms` is more than 5 min off the engine clock carried on from the latest accepted beat (its
`sent_at_ms` plus the receiver's monotonic time since) is refused as `malformed heartbeat`, whatever the VPS wall
clock says. So a wall step of the VPS neither lets a second sender 5-10 min off in (before, it was refused only as
not newer: a missed page or a false resolved notice) nor, ahead, lets it become the newest beat and lock the
healthy engine out (a false resolved notice, then a false `no heartbeat` page). After a silence, a beat within
5 min of either the carried engine clock or the VPS wall clock passes, so a wall step larger than 5 min does not
refuse every beat. Before the first accepted beat after a receiver start, a beat within 5 min of either the VPS
wall clock or the receiver's start time carried on by its monotonic clock passes (review round 10), so a wall step
between the receiver's start and the engine's first beat refuses nothing. An engine
whose own clock steps by more than 5 min is refused until 60 s pass without an accepted beat (a `malformed
heartbeat` page, then `no heartbeat`): check chrony on the engine and `timedatectl` on the VPS.
A second engine whose clock is more than 5 min off is still recorded per source: it is listed in the page's
`beats in the last 60 s: <node> epoch <epoch> xN` line, and it can raise `beating node changed` when its node
differs. It cannot raise `two engines beating`, because its `sent_at_ms` are more than 5 min apart from the other
engine's, so its beats never interleave with them; it raises `malformed heartbeat` instead. Only a second engine
whose beats do not parse (an older checkout that lacks a field) never counts as a source. It also raises `malformed
heartbeat`, once, with `valid beats are also arriving (latest from <node> epoch <epoch>)` in the text. That page
resolves 40 s after the second engine's last bad POST.
Give every machine its own `XTNL_NODE_ID` (`home1` on the home server); `start-xtnl.ps1` logs
`warning: XTNL_NODE_ID is the example's 'pc1'` until you do. The first beat under the new name raises one
`beating node changed (pc1 -> home1)` notice: that one is yours, it clears after 5 min and needs no action.

* **AWS:** two LEADERs is also an off-node alert; follow `failover-and-switchover.md`, check
  `$ETCDCTL get /xtnl/leader` and `$OPS safe-hold status`, and stop the engine that does not hold the lease.
* **Single home PC:** find the other machine (an old desktop, a restored spare). On it, run the two commands
  below, then delete its broker, alert and dead-man variables from its User environment, or rotate the StoneX
  read-only credentials (home-server-plan §4.12). The page resolves within about 90 s plus one window (about
  2.5 min at the default 60 s) after its last beat.

```powershell
deploy\local\uninstall-autostart.ps1
deploy\local\stop-xtnl.ps1 -Containers
```

`XTNL_RECEIVER_TWO_ENGINES_WINDOW_S` (45..600, default 60) sets how long a stopped engine's beats still count
for the rate rule, and how long the page is held after the last detection (the interleave test looks back over
at least 90 s whatever the window). A clean failover never trips the alarm at any window, because the old leader's
beats end before the new leader's span on both clocks.

## Receiver not acknowledging (engine page)

Home-server plan W1 (`src/runtime/deadman.py`). The receiver cannot page about itself, so the engine watches its own
main heartbeat. While it is the leader and the POST to `XTNL_DEADMAN_URL` has failed without a break for
`XTNL_DEADMAN_ALERT_AFTER_S` (default 300 s), it pages once through its outbox (email and push):

* `PAGE[high] dead-man receiver not acknowledging`: `receiver <host>: N failed beats in a row over S s
  (deadman-alert.md, receiver not acknowledging)`. One page per episode. The episode is kept in PostgreSQL
  (`notification_dedupe`, keys `deadman_ack` / `receiver` / host), so an engine restart or a new leader term during
  the same outage does not page again. Exception (review round 4): an inherited episode whose high page is more
  than 6 h old no longer silences the new term. If the new term's own beats fail for `XTNL_DEADMAN_ALERT_AFTER_S`,
  it pages high again as a new episode (a repeat page for a very long outage, never a missed one for a new outage).
* `PAGE[info] dead-man receiver acknowledging again`: the first acknowledged beat after that page. The body
  (`acknowledged again after N failed beats over S s`) is fixed at the first attempt, so a page that could not be
  queued and goes out on a later beat still names the episode's real count and duration. When it comes in a later
  leader term the earlier term's count is not known: the body then gives this term's failures (or `the first beat
  of this leader term was acknowledged`) and `the episode was paged in an earlier term S s before this beat`.
  Only after an info page can a new outage page again. A page that could not be queued logs
  `dead-man receiver page not queued (no valid lease or outbox insert failed)` and is tried again on the next beat.
  The episode's closing row is written after the info page, always later than the high page's row (even after a
  wall-clock step back). If it cannot be written (`dead-man receiver episode not recorded`), it is tried again on
  every following beat until `dead-man receiver <host>: the closed episode is now recorded`.
  The high page's row is handled the same way (review round 5). It is always written later than the previous
  episode's closing row, even after a wall-clock step back. If it cannot be written, it is tried again on every
  following beat until `dead-man receiver <host>: the paged episode is now recorded`. A closing row waits for it.

While the receiver is not acknowledging, **nothing off the PC watches this engine** (except Healthchecks.io check A,
which only sees the heartbeat copy). Checks, from the engine PC:

```
curl.exe -s -o NUL -w "%{http_code}`n" https://<vps>.<tailnet>.ts.net/healthz      # 200 = receiver up and reachable
Select-String -Path var\logs\xtnl-*.log -Pattern 'dead-man heartbeat to' | Select-Object -Last 5
tailscale status                                                                   # both machines online
```

* `HTTP 401` / `HTTP 403` in the heartbeat lines (and `refused the heartbeat bearer token`): the token.
  `XTNL_DEADMAN_TOKEN` (Windows User environment) must equal the receiver's `XTNL_RECEIVER_TOKEN` (or `_NEXT`);
  "Token rotation" below. The start-up ERROR `XTNL_DEADMAN_URL is set but XTNL_DEADMAN_TOKEN is empty` means it is
  not set at all: set it and restart the task. The engine keeps running either way.
* `ConnectTimeout` / `ConnectError` and `/healthz` not 200: Tailscale (is the VPS online in `tailscale status`?),
  the VPS itself, or the receiver unit: [deadman-receiver-home.md](deadman-receiver-home.md) "Checks"
  (`systemctl status xtnl-deadman-receiver`, `sudo journalctl -u xtnl-deadman-receiver`). Check B in Healthchecks.io goes
  red too when the receiver or the VPS is down.
* `/healthz` 200 but beats still fail: the URL path (`XTNL_DEADMAN_URL` must end in the receiver's
  `XTNL_RECEIVER_PATH`), or a receiver clock more than 300 s off (chrony on the VPS).

Settings and series:

* `XTNL_DEADMAN_ALERT_AFTER_S`: whole seconds, 60..3600, default 300 (`deploy/env/engine.env.example`,
  `local-live.env.example`). Below 60 would page on every receiver restart. A value outside the range or not a whole
  number stops the engine with exit 78 (configuration). The engine's line names the variable: `config:
  XTNL_DEADMAN_ALERT_AFTER_S must be between 60 and 3600 s` (or `... must be a whole number of seconds`). It has
  no prefix and comes just before `ALERT: configuration refused (exit 78; see the lines above)`; search the log for
  `XTNL_DEADMAN_ALERT_AFTER_S` (review round 5).
* The streak is timed on the monotonic clock: a Windows clock step (w32time) neither delays nor hastens the page.
* Gauges on the engine's metrics endpoint (leader only; `deploy/observability/metrics.md`):
  `xtnl_deadman_configured` (1 when `XTNL_DEADMAN_URL` is set), `xtnl_deadman_consecutive_failures` (the current
  streak), `xtnl_deadman_last_ok_age_seconds` (age of the last acknowledged beat in this term; absent until the
  first one). The live view's Engine line shows the same three (`dead-man receiver: OK, last ack N s ago`, `not
  acknowledging, N failed beats`, or `not configured`). A streak of 0 with a last ack older than 30 s shows `no beat
  sent or acknowledged for N s` (amber, red past 60 s): the engine is not sending beats at all, for example the
  `deadman` task is down or its circuit is open. The receiver's silence page covers this case.

## Token rotation (no restart, no refused heartbeat)

The engine re-reads `XTNL_DEADMAN_TOKEN` from its cached secret for every beat (refreshed every 5 min, at once
after a 401). The receiver re-reads its tokens from its own source every second (`XTNL_SECRETS_SOURCE=aws-sm`:
the cached secret in ap-southeast-4, refreshed every 5 min and within a minute after a request with an unknown
token). It accepts `XTNL_RECEIVER_TOKEN`, `XTNL_RECEIVER_TOKEN_NEXT` when set, and a replaced token for
`XTNL_RECEIVER_TOKEN_OVERLAP_S` (default 1800 s) after it first saw it replaced. The receiver journal logs
`authenticated with the current / next / previous token` when the slot changes (never the value).

Standard order (no timing constraint):

1. Receiver secret: add `XTNL_RECEIVER_TOKEN_NEXT=<new>` (keep `XTNL_RECEIVER_TOKEN=<old>`). Go on only once the
   receiver journal shows `tokens rotated: current + next` (<= 5 min; or restart the receiver to read it at once,
   its source is read at start).
2. Engine secret (eu-west-2): `XTNL_DEADMAN_TOKEN=<new>`. Within <= 5 min the receiver journal shows
   `authenticated with the next token`.
3. Receiver secret: `XTNL_RECEIVER_TOKEN=<new>`, remove `XTNL_RECEIVER_TOKEN_NEXT`. The old token stays accepted
   for the overlap window, then is refused.

Short order: replace `XTNL_RECEIVER_TOKEN` in the receiver secret, then update the engine secret within the
overlap window (30 min) minus 10 min. Never update the engine first: its new token is refused until the
receiver's secret has it, and Melbourne pages "no heartbeat" after 60 s.

With `XTNL_SECRETS_SOURCE=aws-sm` keep `XTNL_RECEIVER_TOKEN_NEXT` out of `/etc/xtnl/deadman-receiver.env`: the
secret is laid over the env file, so a key removed from the secret (step 3) falls back to the env file's value and
that token would stay accepted.

With `XTNL_SECRETS_SOURCE=env` (the env file) the gates differ, so do not follow the three steps above as
written: the env source is read only at start and logs no `tokens rotated: current + next` line, not even after a
restart (the restarted receiver simply starts with both tokens). Step 1's gate is the
`systemctl restart xtnl-deadman-receiver` itself, and step 2's `authenticated with the next token` appears only
after the engine is restarted with the new token. A restart forgets the overlap, so use `_NEXT`, never the short
order. On the home VPS the engine side is the Windows User environment, also read only at start: follow
[deadman-receiver-home.md](deadman-receiver-home.md) "Token rotation" only, which has the four steps with their
restarts.

### Dashboard heartbeat copy token (`XTNL_HEARTBEAT_COPY_TOKEN`, K10)

The same three steps, with the dashboard's `/etc/xtnl/dashboard/heartbeat.json` in place of the receiver secret
(re-read every 30 s, no restart): 1. add `"token_next": "<new>"` (the dashboard journal shows
`dashboard heartbeat tokens rotated: current + next`); 2. set the engine secret's `XTNL_HEARTBEAT_COPY_TOKEN=<new>`
(the engine re-reads it within 5 min, at once after a 401; the journal shows `authenticated with the next
token`); 3. set `"token": "<new>"` and remove `"token_next"`: the old token stays accepted for `token_overlap_s`.
A renewed `heartbeat.crt` / `.key` is loaded in place within 30 s. The dashboard's PostgreSQL / Redis passwords
rotate without a restart when it reads them from Secrets Manager (`dashboard.env.example`, `XTNL_SECRETS_SOURCE`).
The receiver's paging channel credentials follow its secret the same way (re-read every minute).

## Expected outcome

Heartbeats every 15 s with all ages under threshold; the page auto-resolves.

## Rollback

None needed. Do not silence the receiver; for an announced broker outage use a declared maintenance window
instead (`broker-maintenance.md`: `/etc/xtnl/maintenance.json`, identical on both engine nodes).
