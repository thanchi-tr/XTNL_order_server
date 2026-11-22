# Live view: server-to-page data contract (build reference for `src/liveview`)

Companion to `docs/design/live-view.md` (v9). The server (`src/liveview/server.py`) produces exactly these shapes;
the page (`src/liveview/static/app.js`, `edge.js`) consumes them. All times are epoch milliseconds (int). Levels are
decimal **strings** (never numbers) and appear only in the fields named `entry`, `stop`, `take_profit`, `from_level`,
`to_level` and in level-history points. No field ever holds a size, a money figure, a market-data value or free text.
A value the privacy guard refused arrives as the string `"(not shown)"`.

## GET /api/snapshot

```json
{
  "instance": "uuid",
  "last_sid": "uuid.n" | null,
  "last_n": int,                   // n of the newest ring entry (activity or story_merge); 0 when empty
  "state": State | null,           // null until the first poll has read the database
  "events": [Event, ...],          // ring activity entries, oldest first (by n), at most HISTORY
  "merges": [{"n", "from", "into", "held"}, ...],   // ring story_merge entries still in the ring
  "stories": [StoryHeader, ...],   // touched today, at most 500, newest last activity first
  "day": Day,
  "series": Series,
  "poll": {"at_ms": int|null, "db": "ok"|"down", "engine": "ok"|"down"|"off", "backlog": int}
}
```

## GET /api/stream?instance=<uuid>&after=<n>   (text/event-stream)

| SSE event | data | id |
|---|---|---|
| `hello` | `{"instance": uuid}` | none |
| `activity` | `Event` | `<instance>.<n>` |
| `story_merge` | `{"n", "from", "into", "held": bool}` | `<instance>.<n>` |
| `state` | `State` | none |
| `story` | `StoryHeader` | none |
| `day` | `Day` | none |
| `series` | `Series` | none |
| `poll` | `{"at_ms", "db", "engine", "backlog"}` | none |
| `resync` | `{}` (then the server closes) | none |

The page must call `es.close()` on `resync` and on every `onerror`, then back off (1 s doubling to 30 s, reset after
60 s open), re-fetch `/api/snapshot`, render it, and open `/api/stream?instance=<snapshot.instance>&after=<snapshot.last_n>`.

## Event (one feed line, immutable)

```json
{
  "sid": "uuid.n", "n": int,
  "at_ms": int|null, "clock": "broker"|"server"|"engine", "detected_ms": int,
  "late": null|"history"|"snapshot"|"dlq"|"delayed"|"while_offline",
  "category": "orders"|"positions"|"risk"|"halt"|"alerts"|"operator"|"dlq"|"engine"|"streams"|"catchup",
  "severity": "debug"|"info"|"notice"|"warn"|"alert",
  "kind": "position.opened" | ...,       // closed enum
  "title": "EURUSD SELL 1039708845: stop-loss moved 1.10203 → 1.09903",
  "ids": {"position_id": "...", "broker_order_id": "...", ...},
  "story": "pos:stonex:1039708845" | null,
  "parent": "term:12" | null,
  "episode": "rule|SUBJECT_TYPE|subject_id|activation" | null,
  "housekeeping": bool
}
```

## StoryHeader

```json
{
  "key": "pos:stonex:1039708845",
  "kind": "trade"|"account"|"halt"|"outage"|"engine"|"dlq",
  "state": "open"|"closed"|"active"|"resolved"|"lost"|"reconciled"|"starting"|"done"|"queued"|"quarantined",
  "symbol": "EURUSD"|null, "side": "BUY"|"SELL"|null,
  "ids": {"position_id": "..."},
  "first_ms": int, "last_ms": int,
  "worst": "info"|"notice"|"warn"|"alert",
  "counts": {"orders": 2, "risk": 1},
  "header": "EURUSD SELL 1039708845: open at 1.09903, stop 1.10203, take-profit 1.09303 · stop moved 1x",
  "levels": {"entry": "1.09903"|null, "stop": "1.10203"|null, "take_profit": "1.09303"|null},
  "history": [{"at_ms": int, "entry": str|null, "stop": str|null, "take_profit": str|null}],   // ≤ 100
  "episodes": [{"episode": "...", "rule_id": "...", "level": "FLAG"|"SOFT"|"HARD"|null, "cap": "NONE"|"SOFT"|null,
                "since_ms": int|null, "escalate_min": int|null, "acked": bool, "repages": int,
                "last_repage_ms": int|null, "resolved": bool}],
  "badges": {"<sid>": ["delivered"|"queued"|"retrying"|"suppressed"|"undeliverable"|"escalated"]},
  "related": ["halt:6", ...],
  "merged_into": "pos:..."|null, "held": bool,
  "gap_ms": int|null,              // outage only
  "dlq_status": "QUEUED"|...|null, // dlq only
  "events": [n, ...]               // last 200 arrival numbers
}
```

## State (the status band, Open now, open breaches)

```json
{
  "halt": {"id": int, "level": "NONE"|"SOFT"|"HARD", "effective_level": "...", "hard_latch": bool,
           "rule_id": str|null, "reason_code": str|null, "by_whom": str, "at_ms": int} | null,
  "halt_why": {"escalate_min": int|null, "capped": bool, "cooldown_clear_ms": int|null, "acked": bool},
  "engine": {"status": "running"|"stalled"|"down"|"off"|"unknown", "role": "LEADER"|"STANDBY"|"SAFE_HOLD"|
             "LEADER_INIT"|"CANDIDATE"|"DRAINING"|null, "since_ms": int|null, "sweep_age_s": int|null,
             "last_ok_ms": int|null},
  "leader": {"term_id": int, "node": str, "epoch": int, "since_ms": int, "reached_leader_ms": int|null,
             "draining": str|null} | null,
  "safe_hold": {"on": bool, "since_ms": int|null, "hold_code": str|null, "hold_count": int|null,
                "resumed_by": str|null, "last_resume_ms": int|null},
  "streams": [{"adapter": "stonex", "stream": "orders"|"account"|"prices",
               "status": "live"|"lost"|"fenced"|"inactive"|"unknown", "since_ms": int|null,
               "adapter_state": "STREAMING"|...|null, "role_note": "role: STANDBY"|null}],
  "auth": [{"adapter": "stonex", "shown": "as_recorded"|"cleared"|"rearmed"|"failed_now", "since_ms": int}],
  "oversight": [{"adapter": str, "account_id": str, "since_ms": int}],
  "alerts": {"waiting": int, "oldest_ms": int|null, "undeliverable": int, "escalated_today": int},
  "dlq": {"QUEUED": int, "LEASED": int, "BACKOFF": int, "QUARANTINED": int},
  "breaches": {"FLAG": int, "SOFT": int, "HARD": int, "pending": int},
  "open_breaches": [{"breach_id": int, "rule_id": str, "subject_type": str, "subject_id": str,
                     "symbol": str|null, "side": str|null, "level": str, "cap": str, "reason_code": str|null,
                     "since_ms": int|null, "escalate_min": int|null, "capped": bool, "acked": bool,
                     "repages": int, "story": str|null}],
  "open_now": [{"story": str, "kind": "position"|"order", "adapter": str, "id": str, "symbol": str,
                "side": "BUY"|"SELL", "status": str, "entry": str|null, "stop": str|null,
                "take_profit": str|null, "history": [{"at_ms", "entry", "stop", "take_profit"}]}],
  "timing": {"escalate_after_ms": int, "cool_down_ms": int, "unknown_grace_ms": int}
}
```

## Day

```json
{
  "start_ms": int, "now_ms": int,
  "buckets": [{"activity": int, "notice": int, "warn": int, "alert": int, "housekeeping": int, "partial": bool}],  // 96
  "halt": [{"from_ms": int, "to_ms": int|null, "level": "NONE"|"SOFT"|"HARD", "hard_latch": bool}],
  "engine": [{"from_ms": int, "to_ms": int|null, "state": "running"|"unknown"|"engine_off"}],
  "streams": {"stonex": {"orders": [{"from_ms", "to_ms", "state": "up"|"down"|"fenced"|"engine_off"|"unknown"|
              "not_observed"|"inactive"}]}},
  "inactive": ["pepperstone"]
}
```

## Series (last 60 one-minute points, each `[minute_start_ms, value|null]`)

```json
{"sweep_pass_age_s": [[t, v], ...], "alerts_waiting": [...], "dlq_queued": [...], "open_breaches": [...]}
```

## GET /api/story?key=<key>

`{"story": StoryHeader, "events": [Event, ...]}` (oldest first). 400 on a bad key, 404 when unknown.
