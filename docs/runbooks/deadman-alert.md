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
* any task circuit is open (`degraded: <task>`).

A daily test page proves the path; a missing daily test page is itself an incident.

The receiver is `src/deadman_receiver` (`xtnl-deadman-receiver.service` on the Melbourne host, install steps in
`install-node.md`). Every page subject starts `[XTNL DEADMAN]`; the body names the condition, the receiver time and
the last beat (node, epoch, gate, halt level). Conditions (operations.md §9 thresholds):

| Page subject | Condition key | Fires when |
|---|---|---|
| `no heartbeat for N s` | `silence` | no accepted beat for > 60 s (also > 60 s after the receiver starts) |
| `malformed heartbeat` | `payload_invalid` | a field missing / wrong type, or `sent_at_ms` > 5 min off the receiver clock |
| `adapter <a> not STREAMING while its market is open` | `adapter_not_streaming:<a>` | >= 2 min, `market_open` true, no maintenance window |
| `adapter <a> state missing` / `heartbeat field <f> missing` | `adapter_fields_missing:<a>` / `fields_missing:<f>` | null in the latest beat (at once, no grace) |
| `oversee feed entry un-acked > 30 s` | `feed_lag` | oldest un-acked feed entry > 30 s |
| `RiskSweep pass older than 10 s` | `risksweep_pass_age` | any account's pass > 10 s |
| `alert undelivered > 60 s` | `notification_undelivered` | oldest undelivered `notification_outbox` row > 60 s |
| `SM8 not READY > 60 s` / `SM5 UNKNOWN > 60 s` | `sm8_not_ready` / `sm5_unknown` | for > 60 s |
| `spill file non-empty > 60 s` | `spill_nonempty` | for > 60 s |
| `task circuit open (<task>)` | `circuit_open:<task>` | the task is in `degraded` |
| `receiver token source invalid` | `token_source` | the receiver's secret has no / a short `XTNL_RECEIVER_TOKEN` (or `_NEXT`), or cannot be read; the last valid tokens stay accepted |

`SM5 UNKNOWN` is reported by the engine whenever its gate would deny for want of a current `halt:state`: SM8 not
READY, or `halt:state` missing / not carrying `MAX(halt_events.id)` (also before the first `halt_events` row).

A condition pages once when it starts, again every `XTNL_RECEIVER_REPAGE_S` (15 min) while it lasts, and sends a
`resolved:` notice when it clears. `receiver started` (info) means the receiver process restarted.

## Checks

```
$ETCDCTL get /xtnl/leader                       # is there a leader?
$ETCDCTL get --prefix /xtnl/health/             # are both nodes alive?
$OPS safe-hold status                          # SAFE_HOLD in PostgreSQL and its etcd mirror
journalctl -u xtnl-engine --since "-15 min" -p warning      # on the leader
curl -s -o /dev/null -w "%{http_code}\n" https://<deadman-host>:8443/healthz   # from the leader: 200 = Melbourne reachable
# on the Melbourne host:
systemctl status xtnl-deadman-receiver
journalctl -u xtnl-deadman-receiver --since "-30 min" -p warning   # rejected beats (401 token / 400 fields), page failures
```

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
| Task circuit open | find the task in the journal; after the fix, `$OPS task reset <task> --operator <you> --reason "<ticket>"` (an audited request the current leader consumes; a new leader starts with closed circuits) |

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

With `XTNL_SECRETS_SOURCE=env` (the env file) the same three steps apply, each receiver step followed by
`systemctl restart xtnl-deadman-receiver` (a restart forgets the overlap, so use `_NEXT`, never the short order).

## Expected outcome

Heartbeats every 15 s with all ages under threshold; the page auto-resolves.

## Rollback

None needed. Do not silence the receiver; for an announced broker outage use a declared maintenance window
instead (`broker-maintenance.md`: `/etc/xtnl/maintenance.json`, identical on both engine nodes).
