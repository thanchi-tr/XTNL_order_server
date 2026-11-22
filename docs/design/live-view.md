# Live view: a real-time page of what the order-server is doing

Status: design v9 (2026-11-21). v2 was revised after the five-lens adversarial review (privacy, tailing, web
security, repo fit, usefulness); v3 fixes the 17 problems the independent check of v2 left open; v4 fixes the
problems N1-N12 the check of v3 left open, adopts prototype D's outage and DLQ stories, and names the XTNL theme;
v5 fixes the five problems the round-1 check of v4 found (the `event_log` index, start-up group keys, StoneX
pending-entry brackets, fold holds, stale AUTH_FAILED) and re-checks section 1 against prototype D and the theme;
v6 fixes the three problems the round-2 check of v5 found (false "AUTH_FAILED cleared" events, run / term order in
one poll, id-equality story rules on cTrader) and aligns section 1 with prototype D's story kinds and the XTNL
theme again; v7 keys each start-up story by its leader term and files members by the epoch the database records,
replacing the run / term matching, holds and probe-uptime proof that the round-3 check of v6 found unsound; v8
fixes the three problems the check of v7 found (catch-up rows that are committed before their reconcile, term
endings that write only `end_reason` or nothing, outage keys for a loss never seen); v9 adds, at the owner's
request, a text hierarchy (§1.7) and a static "Edge & automated trading" shell page for Phase 2 (§1.8). The
mapping from each finding to its change is in section 9 (9.1 for v2, 9.2 for v3, 9.3 for v4, 9.4 for v5, 9.5 for
v6, 9.6 for v7, 9.7 for v8, 9.8 for v9).

Settled owner decisions (kept from v1):
- The page runs on **this PC only** (127.0.0.1, no login).
- Orders and positions show **levels** (entry / stop / take-profit). They never show sizes, money, pips or
  spreads.
- The read-only login comes from **migration 0021**. The PC's accepted level is raised to 21, which also applies
  0016-0020. ARM stays off.
- The page UI follows **prototype D** (`docs/design/live-view-prototypes/prototype-d-bridge.html`). It bridges
  prototype A (timeline) and prototype B (console).

The live view **never sends orders** and has no write path of any kind:
- no POST route and no operator command;
- a read-only PostgreSQL login that can see only privacy-safe views;
- no Redis connection;
- every value it shows comes from those views or from the engine's own loopback metrics endpoint, which is
  allow-listed.

Owner questions that are still open are in section 8. Until the owner answers, the build uses the
privacy-preserving option.

## 1. What the user sees (prototype D)

One page, `http://127.0.0.1:8090/`. Wide screens use two columns: the stories in the main column (from A) and the
console on the right (from B). Phone width stacks them: status, day, stories, console. Side gutters are 16 px and
the page never scrolls sideways. A second, static page, `http://127.0.0.1:8090/edge`, is an empty shell kept for
Phase 2 edge and automated trading (§1.8); a header nav links the two. Text sizes on both pages follow §1.7.

**Theme.** The page uses the XTNL theme (`docs/design/live-view-prototypes/xtnl-theme.md`): the site's colour
tokens (`--canvas`, `--base`, `--card`, `--ink-0..3`, `--green`, `--red`, `--amber`, `--blue`, the hairline
`--line*` borders) defined once on `:root` in `app.css`, dark by default with the theme file's light variant behind
a toggle. Severity maps to the theme's accents (alert = red, warn = amber, notice = blue, healthy / NONE / LIVE =
green). Text names `Inter` and data, ids and times name `JetBrains Mono`, with the system fallbacks only: no web
font is loaded (the CSP has no font source and the page works offline). Since 2026-11-22 the look follows the
site's **logged-in pages** (Profile, Data, Session; spec `docs/design/live-view-prototypes/xtnl-site-spec.md`,
extracted read-only from the site's source): a sticky glass header with the site's X mark, the gradient `XTNL`
wordmark and an `ORDER-SERVER` tag, uppercase nav links with a green underline, a `READ-ONLY` pill; a centred
container (1360px); a green mono eyebrow over the page title with a hairline below; panels and tiles with the
site's faint sheen, hairline border and inset highlight (6px corners, 12px for the Edge page's large sections);
tinted chips and badges (colour on a .10 fill with a .20 border); journal-style story rows with a 3px severity
accent; hover lift on clickable cards only; and a glass **data path bar** fixed to the bottom, modelled on the
site's pipeline bar (broker streams → engine → database → this page, each done ✓ / attention ▲ / blocked ✕ / off ○,
always glyph plus word). The top strip is the site's status strip, kept static, and carries status only (halt,
leader, streams, last poll), never a value. The site is dark-only; our light variant stays behind the toggle. The
light / dark choice is a per-browser convenience kept in
`localStorage` inside `try`/`catch`; the page renders correctly without it. Prototype D sets the layout, the story
kinds and the behaviour; where its look differs from the theme (its own per-category `--c-*` / `--t-*` palette,
light default, rounded cards, shadows), the theme wins: category chips are told apart by label and icon on the
theme's ink and tints, and colour carries severity.

### 1.1 Status band (sticky, live)

| Item | Shows | Source |
|---|---|---|
| Halt | NONE / SOFT / HARD, the hard latch, since when, and the rule code | latest `lv_halt_events` |
| Why / when | For SOFT: "HARD in about N min unless fixed", or "capped: will not escalate". Also "cool-down clear about HH:MM" and "acked" | `lv_breaches` + `lv_ruleset_timing` (§4.5) |
| Engine | running / RiskSweep stalled / not responding since T; role LEADER / STANDBY / SAFE_HOLD; RiskSweep pass age | engine probe (§4.6), `lv_cluster_state` |
| Leader | node, epoch, since; "draining (reason)" | `lv_leader_terms`, `lv_engine_epoch` |
| Streams | per adapter × {orders, account, prices}: live / lost since T / fenced / inactive (not configured; known only from a LEADER probe answer, §4.4.7) / "role: STANDBY" (or the current role) while no LEADER answer has been seen / adapter state (STREAMING, BACKOFF, AUTH_FAILED). A recorded AUTH_FAILED stays "AUTH_FAILED (as recorded, since T)" until a LEADER probe answer shows the adapter past a successful authentication (SNAPSHOTTING, STREAMING or RESYNCING), then "AUTH_FAILED cleared (credentials changed)" (§4.5) | `lv_stream_health`, `lv_auth_state`, probe |
| Oversight | "Oversight lapsed: account X since T" for each quarantined account | `lv_sweep_quarantine` |
| Alert delivery | OK / N waiting (oldest M min) / failing / N escalated today | `lv_alerts` |
| DLQ | queued / quarantined counts | `lv_dlq` |
| Breaches | open count by level (FLAG / SOFT / HARD), plus pending count | `lv_breaches` |
| Page | live / reconnecting / server down; "db: down"; time of last poll; "catching up N rows" while a backlog drains | server poll state |

**Banners.** A banner replaces the band's top line when something is down. The page shows only one, chosen in this
order:
1. **Live view server not reachable; retrying (attempt n).** The page cannot reach the live view.
2. **Database and engine both unreachable since T.** Showing the state as of T.
3. **Database unreachable since T.** The engine cannot record anything or queue alerts while it is down. Showing
   the state as of T.
4. **Engine not responding since T (last proof of life).** Leader, streams and halt are greyed and labelled "last
   known before the engine stopped".

### 1.2 Day panel: halt band, stream uptime bars and day strip

One horizontal 24-hour axis for the local day, from 00:00 to now. A "now" tick moves along it. There are three
lanes:

- **Halt band.** Coloured intervals for the effective halt level (NONE / SOFT / HARD; the hard latch is hatched).
  The intervals come from `lv_halt_events`, including the level that was in force at 00:00.
- **Stream uptime bars.** One row per adapter × stream, plus one **engine** row. Each segment is one of:
  - `up`;
  - `down` (lost);
  - `fenced` (leader start, not yet reconciled);
  - `engine_off`;
  - `unknown` (an engine run that ended without a clean stop, from its last proof of life to the next start);
  - `not_observed` (before the live view started, when no source tells us). It is drawn hatched and never
    guessed;
  - `inactive` (an adapter the engine does not run now, §4.4.7). Its rows are collapsed by default.

  §4.7 says how each segment is derived.
- **Day strip.** 96 buckets of 15 minutes. Each bucket is a stacked count of events by class (activity, notice,
  warn, alert). Housekeeping is counted separately and hidden by default. Clicking a bucket scrolls the stories to
  that time. A bucket's tooltip gives counts only.

### 1.3 Stories (main column)

The feed is a list of **stories** (threads), newest activity first. There are six story kinds, exactly prototype
D's `KIND` table (label, icon and category chip); outages and DLQ episodes are stories of their own, not standalone
lines, as in prototype D's `buildStories`. §4.4.3 gives the key and grouping rule of each:

| Kind | Label | Chip | Groups | Ends when |
|---|---|---|---|---|
| trade | "Trade story" | `positions` | a position (its entry order, its stop-loss and take-profit children, its rule episodes and their alerts), or an entry order not yet filled with its bracket children (StoneX links children to a pending entry's `OrderId`). On cTrader no id links an entry order to the position it opens, so the two stay separate trade stories (§4.4.3) | the position closes or the entry order ends unfilled; in any case once every order and position in it is terminal |
| account | "Account rule story" | `risk` | rule episodes on an account or account-day subject, and quarantine of that account | every episode in it is resolved |
| halt | "Halt story" | `halt` | one halt episode, linked to the story of the episode that caused it | the level returns to NONE |
| outage | "Outage story" | `streams` | one stream loss that is not the leader-start fence: lost, then reconciled, plus a `reconnect` / `retry` / `operator` / `resume_after_sleep` catch-up of the adapter read while the outage is the adapter's oldest open one, and the other losses that catch-up's fence hold opened ("streams held for catch-up", `info`, §4.4.3) | the stream is reconciled |
| engine | "Engine start story" | `engine` | one leader term, keyed by the term (every term: a first start, an SM9 re-campaign, a single-PC SAFE_HOLD auto-resume, a retry after `init_failed`): the term's start, LEADER and end lines, the leader-start fence, the catch-up rows recorded under the term's epoch that join no outage, and each stream's first reconcile in that epoch (the start-up story, §4.4.7). Engine run start / stop lines are standalone lines, never part of it | the term has reached LEADER and every active stream is reconciled in its epoch, or 120 s after the term was found, or the term ends (`end_reason` or `ended_at` set); rows of the term that arrive later still join it by key |
| dlq | "DLQ story" | `dlq` | one DLQ episode: a row parked, its retries, re-parks under the same lineage, quarantine and release, linked to the trade story of the order or position it names | the row is RESOLVED |

Each story header carries its kind's chip and icon on the XTNL theme's ink and tints (the theme, not prototype D's
per-category palette, sets the look, §1); its colour comes from the story's worst severity: an outage is `warn`
while lost (a loss later shown as held for catch-up counts as `info`, §4.4.3), a DLQ story `warn` (`alert` once
quarantined), a halt story by its level.

- The collapsed header is one sentence. It leads with "SYMBOL SIDE id" when the story has them, then the state,
  worst severity, last activity time and alert badges. An outage shows its gap length on a 0-60 s scale, the others
  on a 0-1 h scale (prototype D `spanOf`). Examples:
  - "EURUSD SELL 1039708845: open at 1.09903, stop 1.10203, take-profit 1.09303 · stop moved 1x · FLAG
    missing_stop_loss resolved";
  - "pc1 LEADER at 08:01:12, epoch 150031; StoneX caught up (4 orders seen, 1 new, complete) in 14 s; streams
    live";
  - "StoneX orders stream gap 12 s · reconciled · reconnect catch-up complete · prices, account held for
    catch-up";
  - "DLQ row 3 parked (ORDER 1039712003, illegal_transition) · resolved".
- **Links between stories.** A halt story links to the story of its causing episode, and a DLQ story to the trade
  story of the entity it names (§4.4.3). A link is shown as a dashed chip that opens the other story; it never moves
  a line from one story to another.
- Expanding a story shows its lines in arrival order. Lines that arrived late are marked "reported late" or
  "while offline" and also show the action time.
- Lines that belong to no story stand alone: operator actions, engine run start / stop lines, engine probe lines,
  gaps, standalone alert lines.
- Each line shows:
  - the local time (local and UTC on hover);
  - a category chip and a severity colour;
  - the plain-language title;
  - the ids.
- Alert deliveries are **badges** on the line that caused them. They do not get lines of their own (§4.4.6):
  - "e-mailed + pushed ✓";
  - "NOT delivered";
  - "suppressed (duplicate)";
  - "escalated";
  - "re-paged 3x, last 14:30".
- Controls:
  - category chips: `orders`, `positions`, `risk`, `halt`, `alerts`, `operator`, `dlq`, `engine`, `streams`,
    `catchup`;
  - a text filter (id / symbol);
  - **pause**, which buffers new lines and shows a count;
  - **show housekeeping** (off by default): account record updates, stale-read drops, no-visible-change rows,
    DLQ lease cycles, alert lines that only repeat a badge.

### 1.4 Console (right column)

- **Open now.** Each open position and each working order not attached to a position shows:
  - symbol and side;
  - status;
  - a **ladder**: entry, stop and take-profit drawn on a horizontal line in level order, each labelled with its
    level;
  - a **level sparkline**: the stop and take-profit level steps since 00:00.

  Rules for both:
  - **Brackets.** Children are folded into their parent through the bracket model (§4.4.2). On StoneX the stop and
    take-profit come from the child orders.
  - **No current price.** The ladder has no current-price marker (market data never reaches a page).
  - **No scale.** The ladder has no axis numbers, no distances and no pip labels. Whether it is drawn to scale is
    owner decision 8.3.

  Clicking a row opens the story drawer.
- **Open breaches.** For each: rule, subject (SYMBOL SIDE id, or account), level and cap, a plain-language reason
  (from a glossary of codes), since when, escalation estimate, "acked", and "re-paged N×".
- **Health sparklines** (last 60 min, 1-minute points, counts and ages only):
  - RiskSweep oldest-pass age (s);
  - alerts waiting;
  - DLQ queued;
  - open breaches.

### 1.5 Detail drawer

The drawer opens from a story header, a ladder row or a breach. It holds:
- every line of the story the server still holds, oldest first;
- the level history (a table and a larger sparkline);
- the ids (each can be copied);
- for each rule episode: its timing (pending since, active since, uncapped time against `escalate_after_ms`,
  cool-down) and the glossary sentence for its reason code;
- for each alert: its delivery history.

The drawer is served from memory by `GET /api/story?key=…` (§4.8). No value from a URL ever reaches SQL.

### 1.6 Never shown (hard rule; memory "no account values")

The page never shows:
- money and account figures: balance, equity, margin, P&L, any cash or currency amount, commission, swap;
- sizes: units, quantities, any figure derived from a size (but see 8.2);
- broker and market figures: pip or point size, spreads, bid / ask / mid, current price, any market-data value;
- **execution prices** of closing fills (owner decision 8.1);
- distances between levels in pips or points;
- free text: alert subjects and bodies, operator reasons and `detail`, DLQ raw payloads, error strings, quarantine
  reasons, auth `detail`;
- who and where: OS user, client address.

The page **does** show operator labels: `operator`, `approver`, `by_whom` and `resumed_by`. They are the names an
operator types into `src.ops` (the `operator_audit` CHECK pattern), never an OS user, a database login or an
address.

### 1.7 Text hierarchy

Every piece of text on both pages (§1.8) uses exactly one of five levels, plus hero values. Colours are the XTNL
theme's ink tokens (§1); fonts are the named faces with system fallbacks only (no web font). `app.css` defines one
class per level (`.t1` … `.t5`, `.hero`, and `.t2h` for the optional L2 heading), and no rule elsewhere sets a
font size, weight or text colour outside these classes, severity accents excepted.

| Level | Used for | Font | Size / weight | Other | Colour |
|---|---|---|---|---|---|
| L1 page title | the page name under its green eyebrow, one per page | Inter | 26px / 700 (22px on phones), as the site's page headings | `letter-spacing: -0.03em` | `--ink-0` |
| L2 section label | panel and section eyebrows ("STATUS", "OPEN NOW", "EDGES") | JetBrains Mono | 11px / 700 | uppercase, `letter-spacing: .14em` | `--ink-2` |
| L2 heading (optional) | a section that needs a sentence-case heading as well as its label | Inter | 16px / 700 | | `--ink-0` |
| L3 item title | a story header, an Open-now row, a breach, an empty-state title | Inter | 14px / 600 (the site's 13px card title, raised) | `letter-spacing: -0.01em` | `--ink-0` |
| L4 body | story lines, reasons, glossary sentences, empty-state text | Inter | 13-14px / 400 | `line-height: 1.5` | `--ink-1` |
| L5 meta | times, ids, counts, chips, tooltips, axis labels | JetBrains Mono | 11-12px / 400 | | `--ink-2` |
| Hero values | the few status values a glance should catch (halt level, engine role, ActionGate on the edge page) | JetBrains Mono | 22-26px / 700 | | coloured by meaning: the severity accents of §1 (`--green` NONE / LIVE / healthy, `--amber` warn / SOFT, `--red` alert / HARD / DENY, `--blue` notice) |

Rules:
- **No text below 11px**, anywhere (the site's 9-10.5px eyebrows and labels are raised to 11px). Phones
  (`max-width: 640px`) change only L1, to 22px.
- **Chrome exceptions:** the header's wordmark and `ORDER-SERVER` tag, and the nav link labels (13px uppercase), are
  set by their own rules, as on the site; the page eyebrow is L2 in `--green`.
- **At most three levels per card** (a story card: L3 title, L4 lines, L5 meta; a status tile: L2 label, hero
  value, L5 meta).
- **Ids in titles** (order, position, DLQ and epoch ids inside an L3 title) are set in JetBrains Mono at the title's
  size in `--ink-1`, so the SYMBOL SIDE words lead and the id reads as data.
- Hero values are never money, sizes or market figures (§1.6); they are states only.

### 1.8 Edge & automated trading page (shell)

**Purpose.** A placeholder page, kept now so the Phase 2 automated-trading surface has a home and the navigation
does not change later. In Phase 1 it is **only a shell: it is not connected to anything that trades, decides or
advises**. Nothing in Phase 1 may send orders (CLAUDE.md), and this page adds no path towards that.

**Routing (one approach, real routes).** The two pages are separate documents on real server routes, not a
client-side hash router: `GET /` serves `index.html` (the live page) and `GET /edge` serves `edge.html` (§4.8).
Both pages carry the same header nav, above the status band on the live page: two plain links, "Live" (`<a
href="/">`) and "Edge (shell)" (`<a href="/edge">`), the current one marked `aria-current="page"`. Real routes need
no router script, keep each page's CSP and script set separate, and give each page its own URL to bookmark. There
are no `#/live` or `#/edge` fragments.

**What it reads.** Only `GET /api/snapshot`, the same response the live page already gets (§4.8), fetched by a
small `static/edge.js` at load and then every 5 s (backing off from 1 s doubling to 30 s on failure, like the live
page). It opens no `/api/stream`, so it uses no stream slot. From the snapshot it uses only `state` (halt) and
`poll` (`db`, `engine`, `at_ms`). There is no new SQL, no new view, no new route that returns data, no new
environment variable and no new probe metric.

**Layout** (text levels from §1.7; same theme, gutters and phone stacking as the live page):
- **Header:** L1 "Edge & automated trading", an L5 line "Phase 2 shell · not connected · nothing on this page can
  send an order", and the nav.
- **Status strip** (three tiles: L2 label, hero value, L5 meta):
  - **ActionGate: DENY.** Always DENY in Phase 1, shown in `--red`. It is *derived on the page*, never read from the
    engine: the live view cannot import `src.oversee` (§2) and the page is never an input to the real `ActionGate`.
    `edge.js` has no code path that produces any other value. The L5 meta lists why: always "Phase 1: no order
    sender exists"; then, when they apply, "halt SOFT / HARD", "database unreachable" and "status not current"
    (snapshot older than 10 s or the fetch failing), the reasons the real gate also fails closed on.
  - **Halt.** NONE / SOFT / HARD and since when, from `state.halt`: the latest `lv_halt_events` row, the same
    PostgreSQL truth the live page's band shows (the live view has no Redis connection, so this is not the Redis
    `halt:state` mirror). "unknown" when the snapshot is missing or stale.
  - **ARM.** "not connected": a fixed label. ARM is advice only and off unless `XTNL_ARM_ENABLED` is set, and the
    live view cannot read that setting (`XTNL_ARM_*` is on its refuse list, §4.1, §7), so the page never claims more.
- **Empty-state sections**, each an L2 label, an L3 title and one L4 sentence, with no numbers, lists or charts:
  - **Edges** — "No edges defined. Phase 2 will list each edge and its state here.";
  - **Signals** — "No signals. Phase 2 will show signals as they are produced.";
  - **Execution intents** — "No execution intents. In Phase 2 every intent passes ActionGate and operator approval
    before anything is sent.";
  - **Risk budget** — "Not connected. Phase 2 must show it without account values (§1.6).";
  - **Automation controls** — locked placeholders: each control is a non-interactive row with a lock icon and the
    L5 label "locked · Phase 2". If drawn as a `<button>`, it carries `disabled` and `aria-disabled="true"` and has
    no handler.

**What it never has:** no `<form>`, `<input>`, `<select>` or `<textarea>`; no button with an action (not even the
theme toggle: the page applies the theme stored by the live page, read inside `try`/`catch`, and the toggle stays on
the live page); no POST or any other method (the server answers 405, §4.8); no fetch other than `/api/snapshot`; no
`EventSource`, `WebSocket`, `XMLHttpRequest` or `sendBeacon`; no account value (§1.6). `edge.js` builds the DOM
with `textContent` / `createElement` only, like `app.js`. Neither file names any order-changing method, endpoint or
message from `.claude/hooks/pre_tool_guard.py` (§4 "Hook names"): the hook scans only `.py` files under `src/`, so a
test checks the static files (§6), and the Python side (`server.py`'s route and handler names) is covered by the
hook and the existing rule that nothing in `src/liveview` uses those names.

**Phase 2.** Phase 2 replaces this shell; it does not grow it in place. The replacement is designed and reviewed
separately. Per CLAUDE.md, every outbound send goes through the Phase 2 `ExecutionGateway`, which must call
`ActionGate.allow(action)` before each send (fail closed: no readable, current halt state means no action), and
sending needs the operator's approval. Until then the page stays read-only and disconnected, and the live view keeps its rule of no
write path of any kind.

## 2. Architecture

```
PostgreSQL (engine writes) --views lv_* (migration 0021)--> xtnl_liveview (read-only, views only)
        |                                     engine exporter 127.0.0.1:9464/metrics (allow-listed lines)
        |                                                   |
   python -m src.liveview  (asyncio + aiohttp.web, 127.0.0.1:8090, supervised child of start-xtnl.ps1)
        ├─ Poller: one READ ONLY REPEATABLE READ transaction per poll (XTNL_LIVEVIEW_POLL_MS, default 1000)
        │    ├─ Tailer   : cursor tail of the append-only views + watched sets + diffs  -> Events (§4.3)
        │    └─ StateReader: status band, open-now, breaches, timing                   -> State (§4.5)
        ├─ EngineProbe: GET /metrics every poll, 2 s timeout                           -> State + Events (§4.6)
        ├─ Narrator + StoryIndex + DayModel + Series (in memory)                      (§4.4, §4.7)
        └─ HTTP: GET /, /edge (static shell, §1.8), /static/app.js, /static/edge.js, /static/app.css,
                 /api/snapshot, /api/stream (SSE), /api/story, /healthz
Browser: static HTML + JS (EventSource), DOM built with textContent only; no inline script
```

**Why poll PostgreSQL and not Redis.**
- PostgreSQL is the truth and holds every kind of action.
- The Redis feed carries only entity commits (ids only).
- The Redis feed is flushed on every leader change, so SM8 heal bursts would look like activity.
- Reading Redis needs engine-wide rights on the single PC, which has no ACLs.

Correctness does not depend on any clock or on how long a poll is missed (§4.3). Latency is the poll interval plus
the commit lag, about 1 s. Two kinds of line wait on purpose: a fill or close line waits up to 10 s for its
partner row so the pair can be one line (§4.4.2), and "Engine stopped responding" waits 60 s for a new run
(§4.4.7). No other line waits: a leader term and its start-up lines are filed at once, by keys the rows themselves
carry (the term's epoch), never by matching the term to an engine run (§4.4.7). The status band and Open now are
not held.

**Import boundary.**
- Forbidden: `src.liveview` imports nothing from `src.adapters`, `src.pubsub`, `src.oversee`, `src.integrity`,
  `src.cache`, `src.riskmgr`, `src.marketdata`, `src.armconfig`, `src.runtime`, `src.notifications`,
  `src.dashboard` or `src.api`. It does not import `redis` either.
- Allowed: `src.db.readonly_role` (new, §4.2), `src.db.pg` helpers, `src.db.safe_errors`,
  `src.observability.logs`, the standard library, `aiohttp`, and `asyncpg` loaded through `importlib` (asyncpg has
  no `py.typed`; same pattern as `src/db/pg.py`).

A test imports `src.liveview.__main__` and asserts that no `src.cache*`, `redis*`, `src.dashboard*` or
`src.oversee*` module is in `sys.modules`.

**Isolation from the engine.** The live view is not on the bus and never blocks the engine:
- it is a separate process with its own login;
- the login has a connection limit of 3 and a 5 s statement timeout;
- `application_name = xtnl-liveview`, which never matches the epoch-kill pattern `^xtnl:[0-9]+:`.

## 3. Migration 0021 (`src/db/migrations/0021_liveview_role.sql`)

### 3.1 Layout of the file (order matters)

The file follows the 0011 / 0014 style and has three parts, in this order.

1. **Role block.** These lines come first, so the carry-over runbook can pipe them alone before `pg_restore`
   (§3.6):
   - a `DO` block that creates role `xtnl_liveview` only when it is missing, with
     `NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 3`, and
     then runs `EXECUTE format('GRANT CONNECT ON DATABASE %I TO xtnl_liveview', current_database())`, exactly as
     `0011_dashboard_role.sql:24` does. The grant is inside the role block, so the carry-over pipe (§3.6) carries
     it too;
   - `ALTER ROLE xtnl_liveview SET default_transaction_read_only = on`;
   - `ALTER ROLE xtnl_liveview SET statement_timeout = '5s'`;
   - `ALTER ROLE xtnl_liveview SET idle_in_transaction_session_timeout = '10s'`.
2. **Index and views.**
   - `CREATE INDEX IF NOT EXISTS event_log_log_id_idx ON ONLY event_log (log_id)`. `event_log`'s primary key is
     `(seq_id, log_id)` and no index starts with `log_id`, so without it the start rule's fallback `max(log_id)`
     over the whole view and the 7-day late recheck by `log_id` (§4.3) can approach the 5 s statement timeout on a
     large table. `ON ONLY` creates only the (invalid) parent index and builds nothing inside the migration
     transaction. The per-partition indexes are built by the post step: `src/db/migrate.py` `POST_STEP` gains
     `PartitionIndex(21, "event_log", "event_log_log_id_idx", "log_id_idx", "log_id")`, which runs
     `CREATE INDEX CONCURRENTLY IF NOT EXISTS` on each partition and then `ALTER INDEX … ATTACH PARTITION`, so no
     partition is locked against writes. This is exactly how 0017 adds `event_log_broker_time_idx`
     (`0017_arm.sql:393-394` with `PartitionIndex(17, …)` at `migrate.py:182`), and it is what `operations.md` §12
     requires for every new index on a partitioned table. The live view needs no index to be correct: until the
     post step has attached every partition, its reads are only slower.
   - `GRANT USAGE ON SCHEMA public`, then one `CREATE OR REPLACE VIEW` per view in §3.3. The views are owned by the
     schema owner, so the role needs no grant on any base table.
3. **Security barrier and grants.**
   - `ALTER VIEW lv_… SET (security_barrier = true)` for **every** lv_ view. It is set by `ALTER` rather than
     `WITH (...)`, so the 0015+ schema scan still parses each view (§3.2).
   - Then **one** `GRANT SELECT ON TABLE <every lv_ view> TO xtnl_liveview`.

The migration grants nothing else:
- no password;
- no default privileges;
- no grant on any table;
- no write grant;
- no `CREATE` on `public` (PG17 default, `deploy/local/docker-compose.local.yml:55`).

The privacy boundary is in the database. A forbidden column is in no view. With `security_barrier`, a caller's own
predicate cannot read rows that a view filters out.

The 0021 header also says: the lv_ views pin their base columns. A later migration that changes a column's type
or drops a column must drop and recreate the affected views in the same migration. `operations.md` §12 gains the
same line.

### 3.2 View-writing rules

`tests/db/test_arm_schema_no_market_data.py` scans every migration from 0015 on, including view select lists
(`added_columns`, regex `VIEW (\w+) AS\s+SELECT(.*?)\sFROM\s`, split on commas, last word taken as the name).
Every view in 0021 therefore follows these rules:
- `CREATE OR REPLACE VIEW lv_x AS SELECT …`, with no column list and no `WITH (…)`.
- The **select list is a flat list of names**: each item is either an unqualified column (`log_id`) or
  `alias.col AS name`. A qualified column without `AS` (`e.log_id`) is not allowed: the scan splits the item on
  whitespace and takes `e.log_id` as the name, which fails the allow-list. The list has no comma inside an item,
  no function call with several arguments, and no `FROM` or subquery.
- Every computed value (CASE, `jsonb_build_object`, `#>>`, `EXISTS`, regex tests) is computed in a
  `CROSS JOIN LATERAL (SELECT … ) x` placed after the main `FROM`, and named in the select list as
  `x.name AS name`.
- No `float` or `double precision` anywhere (`tests/db/test_schema.py:103`).
- **Renames.** No view exposes a name containing a market-data word (`price`, `bid`, `ask`, `mid`, `spread`, …):
  - `entry_price AS entry_level`;
  - `stop_loss AS stop_level`;
  - `take_profit AS target_level`.
- **0015 columns renamed**, so the service's SQL never names a 0015-0020 column (`tests/db/test_schema_compat.py`
  strips aliases and fails on any of `order_kind|price_increment|min_stop_distance|asset_class|broker_time_ms|origin`):
  - `origin AS version_path`;
  - `order_kind AS leg_kind`;
  - `broker_time_ms AS broker_ms`.
- **Free-text columns** are either left out or passed through a **code guard** in the LATERAL:
  `CASE WHEN col ~ '<pattern>' THEN col END`. A value that is not a code becomes NULL, and the page says "reason
  not shown". No pattern contains a space. Where a pattern allows `.` or `,`, the guard also requires
  `col !~ '[0-9][.,][0-9]'`, so no decimal number can hide in it:
  - `CODE` = `^[A-Za-z0-9_():,-]{1,80}$`, plus the decimal test. The comma is needed:
    `src/oversee/rules.py:317` writes `daily_loss(partial_session,stale_account_image)`;
  - `ID` = `^[A-Za-z0-9:_@/-]{1,64}$`;
  - `OPERATOR` = `^[A-Za-z0-9._@-]{2,64}$` (the existing `operator_audit` CHECK), plus the decimal test;
  - `COMMAND` = `^[a-z_]+(\.[a-z_]+){0,3}$`, for `operator_audit.command`. Commands are dotted (`halt.clear`,
    `dlq.release_quarantine`, `arm.config.accept`; `0007_operator_audit.sql:20`, `src/ops/`), so the CODE guard
    would turn every one into NULL. A dot here is never between digits.

### 3.3 Views

Columns are exactly as listed.

| View | Source and filter | Columns |
|---|---|---|
| `lv_entity_events` | `event_log e`, **all five** entity types (no row filter; see below) | `log_id, seq_id, entity_type, entity_key, adapter_id, account_id, action, logged_at_ms, version_path, broker_ms, facts` |
| `lv_oversee_events` | `oversee_events` | `event_id, sweep_id, rule_id, subject_type, subject_id, transition, adapter_id, account_id, level, cap, reason_code` (CODE guard on `reason`)`, at_ms, activation_id`. `subject_type` here is the database form (`ACCOUNT`, `ACCOUNT_DAY`, `ORDER`, `POSITION`; `src/db/rows.py` `subject_to_db`) |
| `lv_halt_events` | `halt_events` | `id, level, effective_level, hard_latch, rule_id, reason_code, by_whom, at_ms, reviewed_event_id, forced`. `reason_code` = `CASE WHEN "by" = 'system' AND reason ~ CODE THEN reason END`: operator reasons are free text (`src/oversee/halt.py:216`) and never leave the table. `by_whom` = `CASE WHEN "by" ~ OPERATOR AND "by" !~ '[0-9][.,][0-9]' THEN "by" ELSE 'operator' END` (the OPERATOR guard with its decimal test, §3.2) |
| `lv_alerts` | `notification_outbox`, all rows. For `kind = 'arm'` every column except `outbox_id`, `created_at_ms` and `kind` is NULL (a placeholder, so the tailer sees the id; the narrator skips it) | `outbox_id, created_at_ms, severity, kind, info_class, account_id, delivery, escalated, attempts, sent_at_ms, delivered_at_ms, ep_rule_id, ep_subject_type, ep_subject_id, ep_activation_id, ep_level, tr_transition, tr_rule_id, tr_subject_type, tr_subject_id` (detail below) |
| `lv_operator_actions` | `operator_audit` | `audit_id, at_ms, command` (COMMAND guard)`, target_id` (ID guard on `target`)`, operator, approver, outcome, epoch`. **No `reason`, no `detail`, no `db_user`** |
| `lv_dlq` | `events_dlq` | `dlq_id, origin_dlq_id, event_type, entity_id, adapter_id, dlq_reason` (`error_reason`, CODE guard)`, retry_count, status, parked_at_ms, updated_at_ms` |
| `lv_catchups` | `catchup_runs` | `id, run_id, epoch, adapter_id, reason, gap_start_ms, gap_end_ms, covered_from_ms, offline_from_ms, started_at_ms, finished_at_ms, orders_seen, orders_new, orders_updated, positions_seen, positions_new, positions_closed, cash_flows_new, anomalies, complete` (no `notes`) |
| `lv_engine_runs` | `engine_runs` | `run_id, node_id, started_at_ms, last_seen_ms, stopped_at_ms, end_code` (CODE guard on `end_reason`)`, suspended_gaps` |
| `lv_leader_terms` | `leader_terms` | `term_id, cluster_id, epoch_revision, node, started_at, reached_leader_at, ended_at, end_reason` (CHECK-constrained list) |
| `lv_stream_health` | `stream_health` | `adapter_id, stream, conn_lost_at, reconciled_at, reconciled_epoch, updated_at_ms` |
| `lv_cluster_state` | `cluster_state` | `safe_hold, safe_hold_since, hold_code, hold_count, last_resume_at, resumed_by, updated_at_ms`. The only writer, `src/db/leader.py:95-99`, writes three fixed phrases, two of them with a count, so the phrase is **mapped** to a code in the LATERAL and never exposed (detail below). `resumed_by` = `CASE WHEN resumed_by = 'system:single-pc-auto-resume' THEN 'auto-resume' WHEN resumed_by ~ OPERATOR AND resumed_by !~ '[0-9][.,][0-9]' THEN resumed_by END`: the single-PC auto-resume writes that fixed actor (`src/runtime/single_pc.py:23`), whose colon fails OPERATOR, and it is the most common resume on this PC |
| `lv_engine_epoch` | `engine_epoch` | `cluster_id, revision, epoch, node, bumped_at_ms` |
| `lv_breaches` | `halt_breaches b` | `breach_id, rule_id, subject_type, subject_id, adapter_id, account_id, level, pending_level, cap, reason_code, activation_id, pending_since, active_since, release_pending_since, unknown_since, uncapped_ms, last_pass_at_ms, below_soft_since, resolved_at, resolved_by, updated_at_ms, acked`. `reason_code` = the CODE guard on `reason` (§3.2); the free-text `reason` itself is not exposed. `acked` = `EXISTS (SELECT 1 FROM breach_acks a WHERE a.breach_id = b.breach_id AND a.level = b.level AND a.cap = b.cap AND a.reason = b.reason AND (b.active_since IS NULL OR a.acked_at_ms >= b.active_since))`, the same predicate as `src/oversee/notify.py` `is_acknowledged`. `breach_acks.why` is never exposed |
| `lv_open_orders` | `orders_state` WHERE `status IN ('PENDING','WORKING','PARTIALLY_FILLED')` | `adapter_id, broker_order_id, client_order_id, account_id, symbol, side, status, entry_level, stop_level, target_level, is_reduce_only, linked_position_id, leg_kind, broker_updated_at_ms, committed_at_ms`. `entry_level` is computed in the LATERAL with the closing-order rule below (`x.entry_level AS entry_level`) |
| `lv_open_positions` | `positions_state` WHERE `status = 'OPEN'` | `adapter_id, position_id, account_id, symbol, side, entry_level, stop_level, target_level, opened_by_client_order_id, broker_updated_at_ms, committed_at_ms` |
| `lv_ruleset_timing` | `rulesets` WHERE `active` | `ruleset_version, escalate_after_ms, cool_down_ms, unknown_grace_ms` (from `config #>> '{sweep,<key>}'` cast to bigint; NULL means the `src/oversee/config.py` default). The StateReader reads `ORDER BY ruleset_version DESC LIMIT 1`, the same row `src/db/ruleset_store.py:20` treats as active (`max(ruleset_version) WHERE active`) |
| `lv_sweep_quarantine` | `sweep_quarantine` | `adapter_id, account_id, failing_since_ms, quarantined_at_ms, next_retry_at_ms, released_at_ms` (no `reason`, no `released_by`) |
| `lv_auth_state` | `adapter_auth_state a`, **the latest row per adapter only**: `WHERE NOT EXISTS (SELECT 1 FROM adapter_auth_state n WHERE n.adapter_id = a.adapter_id AND (n.failed_at_ms, n.credential_generation) > (a.failed_at_ms, a.credential_generation))`. The primary key is `(adapter_id, credential_generation)` (`0001_init.sql:521-531`), so without the filter one adapter has several rows and the diff key is undefined | `adapter_id, state, failed_at_ms, rearmed_at_ms` (no `detail`, no `credential_generation`) |

**`lv_alerts` derived columns** (computed in the LATERAL). `last_error`, `subject`, `body`, `channels` and
`dedupe_key` are only compared, never exposed.

```sql
delivery = CASE WHEN last_error = 'deduplicated'          THEN 'suppressed'
                WHEN delivered                            THEN 'delivered'
                WHEN sent_at_ms IS NOT NULL               THEN 'undeliverable'   -- no channel / gave up
                WHEN attempts > 0                         THEN 'retrying'
                ELSE 'queued' END
escalated  = channels @> ARRAY['+escalated']
info_class = CASE WHEN kind <> 'info'                                   THEN NULL
                  WHEN subject = 'HALT cleared'                         THEN 'halt_clear'
                  WHEN subject LIKE 'HALT cleared by %'                 THEN 'halt_clear_operator'
                  WHEN tm IS NOT NULL                                   THEN 'transition'   -- full match only
                  ELSE 'notice' END
ep_rule_id = episode->>0, ep_subject_type = upper(episode->>1), ep_subject_id = episode->>2,
ep_activation_id = (episode->>3)::bigint, ep_level = (episode->>4)::int     -- src/oversee/notify.py EpisodeKey
-- transition subject, src/oversee/notify.py:160,167: f"{t.transition} {t.rule_id} {t.subject_type}:{t.subject_id}"
tm = regexp_match(subject, '^(lowered|resolved|capped|uncapped) ([a-z0-9_]{1,64}) '
                           '(account_day|account|order|position):([A-Za-z0-9:_@/-]{1,64})$')   -- NULL when kind <> 'info'
tr_transition = tm[1], tr_rule_id = tm[2], tr_subject_type = upper(tm[3]), tr_subject_id = tm[4]
```

Notes on these columns:
- **Case of the subject type.** The episode key and the transition subject carry the Python form of the subject
  type (`position`, `account_day`; `src/oversee/types.py` `SubjectType`), while `oversee_events` and
  `halt_breaches` store the upper-case database form (`src/db/rows.py` `subject_to_db`). So `ep_subject_type` and
  `tr_subject_type` are `upper(...)`, and matching compares like with like.
- **`ep_level`** is `int(Level)`: 1 = FLAG, 2 = SOFT, 3 = HARD (`src/oversee/types.py` `Level`).
- **`ep_activation_id`** is 0 when the transition had no `activation_id` (`notify.py:153`, `activation_id or 0`).
  Matching therefore uses `coalesce(activation_id, 0)` on the Oversee and breach side (§4.4.6).
- **`info_class = 'transition'`** is set only when `tm` matched in full. An `info` row whose subject starts with a
  transition word but does not match is `notice` (a standalone line).
- The `regexp_match` call has several arguments, so it lives in the LATERAL (§3.2), like every other computed
  value.

- **`tr_transition`** is one of the four fixed words. It is needed because one sweep pass can give one subject two
  info transitions (for example `capped` with `lowered`), and both alerts then carry the same rule, subject and
  `at_ms`; `oversee_events` is unique on `(rule_id, subject_type, subject_id, transition, sweep_id)`, so adding the
  transition word makes the match unique (§4.4.6).

The view passes the subject only through these fixed patterns. It exposes only the four captured groups, and each
of them is limited to a fixed word or a code or id character class. So no `page()` title can carry a value to the page, whoever
writes it (`src/runtime/session.py:2509`).

**`lv_cluster_state` mapping** (in the LATERAL, `r` = `safe_hold_reason`):

```sql
hold_code  = CASE WHEN r = 'safe_hold already set'                            THEN 'already_set'
                  WHEN r ~ '^[0-9]{1,6} abnormal term ends within 15 min$'    THEN 'abnormal_term_ends'
                  WHEN r ~ '^[0-9]{1,6} epoch changes within 30 min$'         THEN 'epoch_changes'
                  END                                     -- NULL for any other text: "reason not shown"
hold_count = CASE WHEN r ~ '^[0-9]{1,6} (abnormal term ends|epoch changes) within' THEN
                  ((regexp_match(r, '^([0-9]{1,6}) '))[1])::int END
```

The title comes from a template per code: "SAFE_HOLD set: {hold_count} abnormal term ends within 15 min", "SAFE_HOLD
set: {hold_count} epoch changes within 30 min", "SAFE_HOLD was already set". `regexp_match` is used rather than
`substring(r FROM …)`, so no `FROM` keyword appears inside a view body other than the main one.

**`lv_entity_events.facts`.** It is built in the LATERAL as
`CASE e.entity_type WHEN 'ORDER' THEN … WHEN 'POSITION' THEN … WHEN 'ACCOUNT' THEN … END`, with
`jsonb_strip_nulls(jsonb_build_object(...))` over the named payload keys only, renamed as shown. Any other type
gives NULL. Everything else in the payload is dropped.
- **ORDER:**
  - ids and codes: `broker_order_id, client_order_id, symbol, side, status, is_reduce_only, linked_position_id`,
    `leg_kind ← order_kind`, `instrument_unknown, source, broker_updated_at_ms`;
  - levels: `entry_level ← entry_price`, `stop_level ← stop_loss`, `target_level ← take_profit`.

  **Closing-order rule** (until owner decision 8.1 says otherwise): `entry_level` in `facts` is
  `CASE WHEN (coalesce((payload->>'is_reduce_only')::boolean, false) AND (payload->>'status' = 'FILLED' OR payload->>'order_kind' IS NULL)) OR (payload->>'status' = 'FILLED' AND payload->>'order_kind' IS NULL) THEN NULL ELSE payload->'entry_price' END`
  (in `lv_open_orders`: `CASE WHEN is_reduce_only AND (status = 'FILLED' OR order_kind IS NULL) THEN NULL ELSE entry_price END`).
  The second clause (build, 2026-11-22) also hides any FILLED order with no stop / limit kind whatever its
  reduce-only flag says: a StoneX closing trade re-read without its closing evidence (an in-memory memo lost on a
  restart, a history row without OpeningOrderIds) would otherwise be stored as a kind-less, not reduce-only fill
  carrying its execution price. An entry fill is still shown through its position's entry level.
  `lv_open_orders` does not need that clause: FILLED is outside its `WHERE`.
  A filled closing order's price is an execution price (`src/adapters/stonex/history.py:191-195`,
  `src/adapters/stonex/mapping.py:264`). A reduce-only order with no stop / limit kind is a closing order whose
  price can be an execution price at **any** status: cTrader's `order_entry_price` falls back to `executionPrice`
  for MARKET and MARKET_RANGE orders (`src/adapters/pepperstone/mapping.py:157-167`), closing orders are
  reduce-only (`closingOrder`, `:205`), and `order_kind` is NULL for them (`:170-180`), so a PARTIALLY_FILLED or a
  part-filled-then-CANCELLED market close would otherwise show its fill price. StoneX Trade-type orders have no kind
  either (`src/adapters/stonex/codes.py:105-109`). The cost: a StoneX child whose order type was never seen
  (normally impossible, `src/adapters/stonex/orders.py:17-18`) shows "level not shown".
- **POSITION:** `position_id, symbol, side, status, entry_level ← entry_price, stop_level ← stop_loss,
  target_level ← take_profit, opened_by_client_order_id, instrument_unknown, source, broker_updated_at_ms`.
- **ACCOUNT:** `account_id, source, broker_updated_at_ms`. The line says only "account record updated".

**CASH_FLOW and INSTRUMENT rows** are in the view **as id placeholders only**. A cash-flow row is a money movement
and an instrument row carries the point size, so for these two types the LATERAL sets `entity_key`, `account_id`,
`broker_ms` and `facts` to NULL (`CASE WHEN e.entity_type IN ('CASH_FLOW','INSTRUMENT') THEN NULL ELSE … END`).
What is left is `log_id`, `seq_id`, `entity_type`, `adapter_id`, `action`, `logged_at_ms` and `version_path`.
The rows are there only so the tailer sees their `log_id`s (`src/db/repository.py:279,294` write them into the same
`event_log` sequence): without them every such id would wait in `Pending`, expire, and be rechecked for 24 h. The
narrator skips them: they are marked seen and produce no event, no story and no day-strip count.

### 3.4 Schema-scan allow-list (`tests/db/test_arm_schema_no_market_data.py`)

Add one `ALLOWED` group, `"live view (0021): ids, times, codes, counts, order levels"`, holding these 120 names
(computed against today's `ALLOW_LIST`; none is named like market data, so `NAMED_EXCEPTIONS` is unchanged):

`acked activation_id active_since anomalies approver attempts below_soft_since breach_id broker_ms broker_order_id
broker_updated_at_ms bumped_at_ms by_whom cap cash_flows_new client_order_id cluster_id command committed_at_ms
complete conn_lost_at cool_down_ms covered_from_ms delivered_at_ms delivery dlq_id dlq_reason effective_level
end_code end_reason ended_at entity_id entity_key entity_type ep_activation_id ep_level ep_rule_id ep_subject_id
ep_subject_type epoch_revision escalate_after_ms escalated event_id event_type facts failed_at_ms failing_since_ms
finished_at_ms forced gap_end_ms gap_start_ms hard_latch hold_code hold_count info_class is_reduce_only
last_pass_at_ms last_resume_at last_seen_ms leg_kind level linked_position_id log_id logged_at_ms next_retry_at_ms
node_id offline_from_ms operator orders_new orders_seen orders_updated origin_dlq_id parked_at_ms pending_level
pending_since positions_closed positions_new positions_seen quarantined_at_ms reached_leader_at rearmed_at_ms
reason_code reconciled_at reconciled_epoch release_pending_since released_at_ms resolved_at resolved_by resumed_by
retry_count reviewed_event_id rule_id ruleset_version run_id safe_hold safe_hold_since sent_at_ms seq_id severity
started_at started_at_ms status stop_level stopped_at_ms stream subject_id subject_type suspended_gaps sweep_id
target_id term_id tr_rule_id tr_subject_id tr_subject_type tr_transition transition uncapped_ms unknown_grace_ms unknown_since
version_path`

`entry_level` and `target_level` are already allowed.

### 3.5 Grants test (`tests/db/test_liveview_role_migration.py`)

The test checks that:
- the 0021 grant list equals the set of views that `src.liveview.sql.ALL_SQL` reads;
- there is no table grant, no write grant, no password and no default privileges;
- the role has the NO* attributes and the three role settings;
- every view has `ALTER VIEW … SET (security_barrier = true)`;
- every view column is on the §3.4 group;
- no `jsonb_build_object` names a forbidden payload key (`units`, `ordered_units`, `remaining_units`, `raw_qty`,
  `accrued_swap`, `commission`, …);
- no view column comes from `operator_audit.reason` / `detail`, from `halt_events.reason` outside the
  `"by" = 'system'` guard, or from `notification_outbox.subject` / `body` / `last_error`, except the four
  anchored `tm` captures `tr_transition`, `tr_rule_id`, `tr_subject_type` and `tr_subject_id` (§3.3);
- the role block ends before the first `CREATE OR REPLACE VIEW` and its line count equals the pinned N (§3.6);
- the only other DDL is `CREATE INDEX IF NOT EXISTS event_log_log_id_idx ON ONLY event_log (log_id)` (the `ON ONLY`
  form; a plain `ON event_log` fails the test), placed after the role block (so the carry-over pipe never runs it
  before `pg_restore`);
- `src/db/migrate.py` `POST_STEP` holds
  `PartitionIndex(21, "event_log", "event_log_log_id_idx", "log_id_idx", "log_id")`;
- `lv_open_orders` and the `facts` ORDER branch both carry the closing-order rule (§3.3);
- on a live database (a `pg` test, skipped without one), `pg_class.reloptions` holds `security_barrier=true` for
  every lv_ view.

`ALL_SQL` reads the clock as `SELECT now_ms()`, never `extract(epoch FROM …)`: the grant test's
`\b(?:FROM|JOIN)\s+` regex would otherwise count `clock_timestamp` as a relation.

### 3.6 Carry-over and accepted level

- `docs/runbooks/single-pc-autostart.md`, carry-over step: before `pg_restore`, pipe
  `Get-Content 0021_liveview_role.sql -TotalCount N` (through the last `ALTER ROLE` line) into `psql`, exactly as
  for `xtnl_dashboard`. Piping the whole file would fail, because the views name tables that do not exist yet.
  Role-level `ALTER ROLE … SET` values are not in a `pg_dump`, and 0021 is recorded as applied by the dump, so this
  step is the only place they are set. `tests/deploy/test_single_pc_autostart.py` pins N in the same way as the
  0011 step.
- `XTNL_MIGRATIONS_ACCEPTED_UPTO` examples stay at 14 (the AWS default). The PC's `deploy/env/local.env` is set to
  21 by the owner decision; the owner edits it.

## 4. The service (`src/liveview/`)

Modules:
- `settings.py`: the only module that reads `os.environ`; it holds no status or action literals (§4.1);
- `sql.py`: every statement, plus `ALL_SQL` and `ALL_VIEWS`;
- `tailer.py`: cursors, watched sets, diffs;
- `state.py`: the StateReader;
- `probe.py`: the engine probe;
- `events.py`: Event, slots, allow-lists, `assert_safe`;
- `narrate.py`: titles, bracket model, start-up stories;
- `stories.py`: StoryIndex, DayModel, Series;
- `server.py`: aiohttp app;
- `__main__.py`;
- `static/index.html`, `static/app.js`, `static/app.css`;
- `static/edge.html`, `static/edge.js`: the Phase 2 shell page (§1.8), sharing `app.css`.

The static files are read at start from `Path(__file__).parent / "static"` through a fixed allow-list of five
names. `pyproject.toml` has no package-data configuration, so nothing claims they are packaged. Tests live in
`tests/liveview/`, which has an `__init__.py` (`tests/api/` already has `test_server.py`).

**Hook names.** `.claude/hooks/pre_tool_guard.py:19-26` blocks, anywhere under `src/`:
- a `def` named `place_order`, `modify_order`, `cancel_order`, `submit_order`, `close_position`, `amend_order` or
  `update_order`;
- the StoneX order paths `/newtradeorder`, `/newstoplimitorder`, `/updatetradeorder`, `/updatestoplimitorder` and
  `/cancel` (followed by a word boundary and no `_`);
- the cTrader messages `ProtoOANewOrderReq`, `ProtoOAAmendOrderReq`, `ProtoOAAmendPositionSLTPReq`,
  `ProtoOAClosePositionReq` and `ProtoOACancelOrderReq`.

No function, string, route or comment in `src/liveview` uses any of these. (`src/liveview` is outside the ARM
lockout name-ban scope, but its names avoid that pattern too.)

**Literal sets and the env scanner.** `tests/deploy/test_deploy_assets.py:157-189` scans every module whose text
contains `environ` or `getenv` (or defines a `*_VAR` / `*_ENV` constant), and reads every string in a tuple, every
subscript and every `.get("…")` argument that matches `ENV_NAME` (`^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$`, `:29`) as an
environment variable the code reads. `PARTIALLY_FILLED`, `CASH_FLOW` and `DROPPED_STALE` match it. So:
- every status, kind or type set in `src/liveview` is a `frozenset({...})`, never a tuple;
- no module in `src/liveview` other than `settings.py` contains the substrings `environ` or `getenv` (not even in a
  docstring: write "environment variable" as "setting") or defines a `*_VAR` / `*_ENV` constant;
- a test asserts both.

### 4.1 Settings

All settings are environment variables, documented in `deploy/env/engine.env.example` and
`local-live.env.example`.

| Variable | Default | Rule |
|---|---|---|
| `XTNL_LIVEVIEW_DSN` | required | Windows User environment only (`USER_ENVIRONMENT_ONLY`), never logged; `postgresql://xtnl_liveview:…@127.0.0.1:55433/xtnl` |
| `XTNL_LIVEVIEW_ADDR` | `127.0.0.1:8090` | Host must be a **literal** loopback address: `127.0.0.0/8`, or `[::1]`. A name, `localhost` included, is refused (exit 78), so a hosts-file entry can never move the bind off loopback |
| `XTNL_LIVEVIEW_POLL_MS` | `1000` | 250..10000 |
| `XTNL_LIVEVIEW_HISTORY` | `2000` | ring size in events, 200..20000 |
| `XTNL_LIVEVIEW_BACKFILL_H` | `24` | hours shown at start, 1..168; the backfill always reaches back at least to local 00:00, so the day panel is complete |
| `XTNL_LIVEVIEW_MAX_CLIENTS` | `4` | concurrent streams, 1..16. Browsers allow 6 HTTP/1.1 connections per host, so 4 leaves room for page and snapshot fetches |
| `XTNL_LIVEVIEW_ENGINE_METRICS` | `127.0.0.1:9464` | the engine exporter (`XTNL_METRICS_ADDR` in `local.env.example:19`); literal loopback only, like ADDR; `off` disables the probe and the band shows "engine probe off" |

**Refused environment.** The service refuses to start (exit 78, naming the variable, never a value) when its
environment holds any of the following:
- names starting with `STONE_X_`, `CTRADER_`, `XTNL_MD_`, `XTNL_ARM_`, `XTNL_SMTP_`, `XTNL_SES_`, `XTNL_PUSH_`,
  `XTNL_API_`, `XTNL_BACKUP_`, `RCLONE_` or `B2_` (the last three: the backup credentials of the separate backup
  Windows account, home-server plan W2 and §3.2);
- the names `ORDER_SERVER_DSN`, `REDIS_URL`, `BROKER_ENV`, `XTNL_DEADMAN_TOKEN`, `XTNL_HEARTBEAT_COPY_TOKEN`,
  `XTNL_DASHBOARD_DSN` and `XTNL_HEARTBEAT_COPY_URL` (home-server plan W2: a Healthchecks.io ping URL is a
  capability URL that the pattern below does not catch);
- any other `XTNL_*` name matching `(PASS|SECRET|TOKEN|DSN|KEY)`, except `XTNL_LIVEVIEW_DSN`.

The pattern is limited to `XTNL_*`, because the inherited Windows environment can hold unrelated tool tokens that
are not the engine's.

`start-xtnl.ps1` drops the same names from the child's environment (`$LiveViewDropEnv`, §5), so normally nothing
is refused.

How `settings.py` holds the refuse-list matters, because `tests/deploy/test_deploy_assets.py`
`env_vars_read_by_code` reads every string in a **tuple** (`:186`) as an environment variable the code reads, and
`test_every_env_var_the_code_reads_is_documented` then requires each one in `engine.env.example`.
`XTNL_DASHBOARD_DSN` is documented only in `dashboard.env.example`, so a tuple holding it would fail that test. So:
- the refused **names** are a `frozenset({...})` (an `ast.Set`, which the scanner does not read), exactly as
  `src/api/settings.py:54` holds `FORBIDDEN_NAMES`;
- the refused **prefixes** may be a tuple: each ends in `_`, so none matches the scanner's `ENV_NAME` pattern;
- the generic rule is one compiled regex, `^XTNL_\w*(PASS|SECRET|TOKEN|DSN|KEY)`, applied in code with the single
  exception `XTNL_LIVEVIEW_DSN`. It lives only in `settings.py`, never in `start-xtnl.ps1` (§5);
- no tuple in the module holds status literals, so a string such as `PARTIALLY_FILLED` is never mistaken for an
  environment variable.

### 4.2 Database access and role check

- **Pool.** An asyncpg pool (min 1, max 2), loaded through `importlib`, with server settings
  `application_name = xtnl-liveview`, `default_transaction_read_only = on` and `statement_timeout = 5000`.
  `with_schema_compat` is not used: the service's SQL names only lv_ views.
- **New `src/db/readonly_role.py`.** It imports only the standard library and `src.db.pg`. It holds:
  - the existing `SQL_ROLE_CHECK`, moved from `src/dashboard/server.py`, with `relkind IN ('r','p','v','m')`, so a
    write grant on a view also counts;
  - `verify_read_only_connection(conn, *, dsn_var, role_hint)`. Its message names the caller's variable and role,
    never a value.

  `src/dashboard/server.py` and `src/api/pinescript.py` import it from there, **keeping their public names**, which
  existing tests use:
  - `src/dashboard/server.py` keeps `SQL_ROLE_CHECK` (re-exported, bound to the shared constant and still in
    `__all__`), `DashboardRoleError`, and a one-argument `async def verify_read_only_connection(conn)` wrapper that
    calls the shared check with `dsn_var="XTNL_DASHBOARD_DSN", role_hint="xtnl_dashboard"` and raises
    `DashboardRoleError` with the same messages ("superuser", "can write N table", "default_transaction_read_only",
    "role not found"). The pool `init=` keeps passing `conn` only. `tests/dashboard/test_dashboard.py:186-194`
    passes unchanged.
  - `src/api/pinescript.py` keeps `SQL_ROLE_CHECK` (bound to the shared constant) and `role_problems`;
    `tests/api/test_export_memory.py:11,54` passes unchanged.
  - Both modules today hold the same text with `relkind IN ('r', 'p')`; the shared constant widens it to
    `('r','p','v','m')`. The tests compare by name, not by text, so they still pass, and a view with a write grant
    now also counts for the dashboard and the API.
- **Pool `init`.** For the live view, `init` runs `verify_read_only_connection(conn, dsn_var="XTNL_LIVEVIEW_DSN",
  role_hint="xtnl_liveview")`, then `verify_view_only_role(conn, views=ALL_VIEWS)`. The second check requires all
  of the following:
  - `current_user = 'xtnl_liveview'`;
  - `has_table_privilege(v, 'SELECT')` for every view in `ALL_VIEWS`;
  - **zero** tables of relkind `r` / `p` outside system schemas on which
    `has_table_privilege(c.oid, 'SELECT') OR has_any_column_privilege(c.oid, 'SELECT')`;
  - `NOT pg_has_role('pg_read_all_data', 'MEMBER')` and `NOT pg_has_role('pg_write_all_data', 'MEMBER')`.

  A DSN that points at `xtnl_dashboard`, `xtnl_api` or the engine fails here.
- **Start-up.** The first connection is made at start.
  - A role-check failure is a configuration error: exit 78, naming `XTNL_LIVEVIEW_DSN`.
  - A **login refusal** is also a configuration error, exit 78 naming `XTNL_LIVEVIEW_DSN`: SQLSTATE class `28`
    (`28000` invalid authorization, which covers a role that is missing or NOLOGIN; `28P01` invalid password) and
    `3D000` (no such database). These happen when 0021 is not applied yet, when the role is still NOLOGIN after a
    carry-over, or when the password is stale after `-Rotate`; reporting them as "db: down" would raise banner 3
    ("the engine cannot record anything") while the engine is fine. The SQLSTATE is read from the asyncpg
    exception's `sqlstate`, never from its message.
  - Only a socket or connect failure, or a temporary server refusal (`57P03` cannot connect now, `53300` too many
    connections), is temporary. The service starts, reports `db: down` and retries with back-off (1 s doubling to
    10 s). Correctness does not depend on the back-off (§4.3). The same split applies to a reconnect after start: a
    class `28` / `3D000` refusal later (for example after `-Rotate` without a restart) exits 78 too.
- **Errors.** Every database error is logged only through `src.db.safe_errors.safe_db_error`, never `str(exc)`.
- **Statements.** Every statement is a `SELECT` from an lv_ view with a `LIMIT`, or `SELECT now_ms()`. `ALL_SQL`
  lists them all, and the role test (§3.5) keeps the grant list equal to the views they read.

### 4.3 The tailer: exactly once, by database-issued ids

**Premise (checked).**
- Every append-only source has a database-issued `bigserial` key `k`: `log_id`, `event_id`, `outbox_id`,
  `halt_events.id`, `audit_id`, `dlq_id`, `catchup_runs.id` and `term_id`.
- No migration sets `CACHE` or uses `ALTER SEQUENCE`, so `nextval` is monotonic across sessions.
- Commit order is **not** id order: up to `MAX_TXN_MS` = 15 s passes between `nextval` and commit
  (`src/integrity/worker.py:58`).
- The time columns are **not** usable as cursors. `seq_id`, `sweep_id`, `created_at_ms` and the halt and Oversee
  `at_ms` are stamped from the engine clock, possibly long before the commit (queues, sweep passes, the relay's
  pre-send `now`). The database runs in Docker on the WSL2 clock, which can differ from the engine's.

So **no clock value affects correctness**. Time columns are used only as wide partition-pruning bounds.

**State per append-only source:**
- `H`: the largest id seen;
- `C`: the contiguous floor; every id ≤ C is done;
- `Pending`: a map from each id in `(C, H]` not yet seen to the database time at which it was first found missing;
- `Seen`: the ids in `(C, H]` already emitted;
- `Expired`: ids that left `Pending` unseen, each with its expiry time. At most 10 000 entries, kept for 24 h.
  Entries older than 24 h are evicted each recheck. When an insert would pass 10 000, the **oldest** entries are
  evicted first, and the evicted count is logged (one count line, no ids). No tailed view filters rows out:
  CASH_FLOW and INSTRUMENT rows are in `lv_entity_events`, and ARM rows in `lv_alerts`, as placeholders (§3.3). So
  a committed id never looks missing, and the budget is spent only on ids that really never committed.

There is no time-based expiry of `Seen`.

**Start**, in one `READ ONLY REPEATABLE READ` transaction:
1. `H = C = max(k)`. For the three partitioned sources (`event_log`, `oversee_events`, `notification_outbox`)
   this is read within the 7-day prune bound; if that finds nothing, `max(k)` over the whole view. An empty source
   gives `H = C = 0`, and its first poll can only return rows committed after start. **The cursor never starts
   below the newest row**, so an empty or quiet source never replays old rows.
2. `Pending` gets each id in `(H − 1000, H]` that is absent from the view, stamped with `db_now`. These are
   transactions still in flight at start.
3. **Backfill**, for display only. Rows with `k ≤ H`, newest first, from the earlier of `db_now − BACKFILL_H` and
   local 00:00, with at most `HISTORY` rows per source. Backfill never moves a cursor.

**Each poll** runs in one `READ ONLY REPEATABLE READ` transaction whose first statement is `SELECT now_ms()`.
1. Read `SELECT … FROM lv_x WHERE (k > $H OR k = ANY($pending)) AND <prune> ORDER BY k LIMIT 500`. Keep paging on
   `k > last_k` until a page comes back short. Read at most 10 pages per source per poll. If that cap is hit, the
   cursor stays where it got to, the band shows "catching up N rows", and the next poll continues. Nothing is
   dropped.
2. Emit each returned row whose id is not in `Seen`. Add it to `Seen` and remove it from `Pending`.
3. Each id in `(old H, new H]` that was not returned goes into `Pending`, stamped with `db_now`.
4. Expire `Pending` entries with `db_now − t > HOLD_MS` into `Expired`. `HOLD_MS` = `MAX_TXN_MS + 2 × poll + 30 s`
   = 15 + 2 + 30 = 47 s at defaults. Log only the count. Rolled-back sweeps after `CasConflict`, fenced-out inserts and
   `ON CONFLICT DO NOTHING` normally consume ids, so most expired ids never commit.
5. `C = min(Pending) − 1` if `Pending` is non-empty, else `H`. Drop `Seen` entries ≤ C.
6. **Late recheck**, every 60 s: `SELECT … WHERE k = ANY($expired) AND <wide prune>` (7 days). A row found here is
   emitted, marked late, and removed from `Expired`. This covers a commit slower than `HOLD_MS` (COMMIT runs with
   `statement_timeout 0`, `src/db/pg.py:6`) and a row stamped before the 24 h prune bound.

**Prune bounds.** They are used only for partition pruning and are never a cursor:

| Source | Bound per poll | Bound for the late recheck |
|---|---|---|
| `lv_entity_events` | `seq_id >= (db_now − 24 h) << 20` (primary key `(seq_id, log_id)`) | `(db_now − 7 d) << 20` |
| `lv_oversee_events` | `sweep_id >= (db_now − 24 h) << 20` | `(db_now − 7 d) << 20` |
| `lv_alerts` | `created_at_ms >= db_now − 24 h` | `db_now − 7 d` |
| the others | none (unpartitioned, small) | none |

**Why this misses nothing (proof sketch).**
- Take a committed row with id `k`.
- **Case 1:** `k` is above `H` when it becomes visible. Step 1 returns it.
- **Case 2:** `k` is below some `k' > k` that was seen at database time `T`. Because `nextval` is monotonic, `k`
  was drawn before `k'`, so its insert began before `T`. It commits by `T + MAX_TXN_MS` (statement timeout 5 s
  plus idle-in-transaction timeout; entity commits are also cut at 5 s by `src/pubsub/consumers.py:83`). `k` stays
  in `Pending` until `T + HOLD_MS`, which is later, so the first poll after its commit returns it.
- **Case 3:** anything slower than that, or stamped outside the 24 h bound, is caught by the late recheck for 24 h.
- **No duplicates:** `Seen` and `C` rule them out.
- **Outages:** they do not matter. A database outage or a live-view restart simply resumes from `H`. The
  live-view restart case is covered by the start rule (only the newest-row floor is lost, and the backfill
  shows those rows).

**Rows changed in place (watched sets).** New ids enter a watched set through the cursor. Each poll reads
`WHERE id = ANY($watched) AND <prune>`. The tracked columns form a versioned key, and an event is emitted only
when that key changes. An id leaves the set when it is final.

| Source | Tracked key | Final when | Seeded at start from |
|---|---|---|---|
| `lv_alerts` (rows with `kind = 'arm'` **never** enter the set: as placeholders their `sent_at_ms` stays NULL, so they would never become final and would pin the prune bound below) | `(delivery, escalated, attempts > 0)` | `sent_at_ms IS NOT NULL` | `delivery IN ('queued','retrying') AND created_at_ms >= db_now − 7 d` (rows queued before a weekend shutdown are still followed) |
| `lv_dlq` | `(status, retry_count)` | `RESOLVED` for 10 min only (RESOLVED → BACKOFF can happen). `QUARANTINED` is **not** final: `dlq release-quarantine` moves it back to QUEUED (`src/ops/sql.py:80-83`) | `status IN ('QUEUED','LEASED','BACKOFF','QUARANTINED')` |
| `lv_leader_terms` | `(reached_leader_at IS NOT NULL, end_reason, ended_at)` | `ended_at IS NOT NULL` | `ended_at IS NULL` |
| `lv_engine_runs` | `(last_seen_ms bucket, stopped_at_ms, end_code)`; the key is `run_id` (uuid, no cursor) | `stopped_at_ms` set, or superseded and reported | the discovery query below, at start |

The watched query for alerts also carries `created_at_ms >= min(created_at_ms of the set)`, for pruning.

**Watched ids that disappear.** The lifecycle job moves old RESOLVED DLQ rows to `events_dlq_history`
(`src/db/lifecycle.py:120-131`), and old `notification_outbox` rows leave with their archived partitions, which
the lifecycle job detaches after export (`src/db/lifecycle.py:86`; its row purge at `:109` is of `outbox_events`, not
of `notification_outbox`). A watched id that the watched query no longer returns
is dropped from the set and **emits nothing**: its last reported state stands. This keeps the sets bounded.

**Engine runs have no id cursor** (`run_id` is a uuid), so they are found by a discovery query, run at start and on
every poll: `SELECT … FROM lv_engine_runs ORDER BY started_at_ms DESC LIMIT 50`. The result is diffed by `run_id`:
a `run_id` not seen before is a new run (at start, the result only seeds the known set, silently), and a known
`run_id` whose tracked key changed gives a heartbeat, stop or "superseded" event. Runs not in the latest 50 are
forgotten. `started_at_ms` is an engine-clock column, but it is used here only to rank the 50 newest runs, never
as a cursor: runs start rarely, so a run with a skewed clock is still among the 50 newest.

**Diff sources.** These are re-read whole each poll, and each key keeps its last values for as long as it is in
the read. They are diffed on **values**, not booleans:
- `lv_stream_health`:
  - a new non-null `conn_lost_at` means "lost";
  - a changed `reconciled_at` means "reconciled";
  - a changed `reconciled_at` with no observed loss means "lost and reconciled between polls". Its `conn_lost_at`
    was never read (NULL in both readings), so anything keyed by it uses the new `reconciled_at` instead, which is
    always non-null (`SQL_STREAM_RECONCILED` sets `reconciled_at = now_ms()`, `src/db/repository.py:123-129`).

  A `conn_lost_at` equal to a `lv_leader_terms.started_at` is the LEADER_INIT fence (`src/db/leader.py:29,84,87`
  use the same `now_ms`, in the epoch-bump transaction that also inserts the term row, so both are visible in the
  same poll). It produces **one** `info` event per term, "streams fenced at leader start (epoch N)", and no
  per-stream "lost".

  Each "reconciled" event also carries the row's new `reconciled_epoch` and its value in the previous reading.
  `SQL_STREAM_RECONCILED` (`src/db/repository.py:123-129`) stamps `reconciled_epoch` with the writer's fencing token
  (`$1` of the fenced statement), which is `engine_epoch.epoch` = the lease revision = `leader_terms.epoch_revision`
  of the reconciling term (`src/db/epoch.py:75-88`, `src/db/fence.py`); the fence makes a write by any other epoch
  impossible. §4.4.7 files the event by these two values.
- `lv_cluster_state`: SAFE_HOLD set / resumed.
- `lv_engine_epoch`: epoch changes.
- `lv_sweep_quarantine`: oversight lapsed / restored.
- `lv_auth_state`: AUTH_FAILED / REARMED, keyed on `adapter_id` (the view holds one row per adapter, §3.3) and
  tracking `(state, failed_at_ms, rearmed_at_ms)`, so a new credential generation that fails again is a new
  AUTH_FAILED event. A new generation that **authenticates** writes no row (`src/db/auth_state.py` writes only on
  failure, `SQL_AUTH_FAILED`, and on re-arm, `SQL_AUTH_REARM`), so the latest row stays AUTH_FAILED; whether it
  still holds is decided with the probe (§4.5), which also gives the "AUTH_FAILED cleared" event for a failure this
  process saw arrive.

The first reading after start is silent: a state that was already true produces no event. This includes derived
states: an AUTH_FAILED row already present at start that the probe later shows cleared changes the band, but emits
no event (§4.5).

**Order within a poll.** Sources are narrated in a fixed order: new engine runs (the discovery diff), then leader
terms (the `term_id` cursor and the terms watched set), then `lv_catchups`, then the stream-health diff and the
other sources. Start-up filing never depends on this order or on how many polls apart rows arrive: a start-up
story's members join it by the epoch they carry (§4.4.7). The outage join and the "held for catch-up" fold
(§4.4.3) are decided when a catch-up row is read, and this order reads a catch-up row before a reconcile found in
the same poll, which is the order the engine commits them: `_job` records the row and only then calls
`release_fence`, whose reconcile is enqueued after it (`src/runtime/catchup.py:398-408`,
`src/pubsub/orchestrator.py:1648-1661, 971-980`). A member is never read before its term row: the term row commits in
LEADER_INIT step 1 (the fence in the same transaction), before any catch-up or reconcile of that term, and each
poll reads one snapshot. The order only makes the term's own start line the first line of its story when they
arrive in one poll. Engine run rows are standalone lines, so when a run row arrives (before, with or after a
term) changes nothing in any story.

### 4.4 Events and narration (`events.py`, `narrate.py`)

```
Event(
  sid: str            # "<instance uuid>.<n>", the SSE id
  n: int              # arrival counter (detection order); the ring and feed are ordered by n
  at_ms: int | None   # when the action happened, as recorded
  clock: "broker" | "server" | "engine"   # which clock at_ms comes from
  detected_ms: int    # database time of the poll that found it
  late: None | "history" | "snapshot" | "dlq" | "delayed" | "while_offline"
  category, severity: "debug" | "info" | "notice" | "warn" | "alert"
  kind: str           # closed enum, e.g. "position.opened", "bracket.stop_moved", "rule.raised"
  title: str          # rendered on the server from the kind's template and the slots
  slots: dict[str, Slot]        # typed values used by the title (see 4.4.5)
  ids: dict[str, str]
  story: str | None   # story key (4.4.3); parent: str | None (the enclosing story, e.g. a start-up story)
  episode: str | None # "rule_id|subject_type|subject_id|activation_id"
  housekeeping: bool
  source: str, source_key: str  # view and key, for the drawer
)
```

**Clocks.** `at_ms` comes from these columns:
- `broker_ms`, when present, with clock `broker`;
- else `logged_at_ms` (clock `server`);
- Oversee, halt and alert times are clock `engine`;
- `operator_audit.at_ms` is clock `server`.

**Late marking.** An event is marked late when `at_ms < detected_ms − 60 s`, or when `version_path` is `history`,
`snapshot` or `dlq`. The line then says "reported N min after it happened" or "found while the engine was off",
and shows the action time.

**Ordering.** The live feed is ordered by arrival. Backfill events are numbered in `at_ms` order at start.

**Grouping is presentation only.** Story grouping (keys, joins, merges, folds and links, §4.4.3) only decides
where a line is displayed: every row is shown exactly once whatever story it lands in, and a grouping error can
never hide, duplicate or alter a row.

#### 4.4.1 Entity narration

- **Cache.** An entity facts cache is keyed by `entity_key` (LRU of 20 000; entities that are still open are
  pinned). It is seeded at start from `lv_open_orders` and `lv_open_positions` (the current applied facts), then
  updated from the backfill oldest first.
- **Which rows update it.** Only `APPLIED` rows update the cache. `DROPPED_STALE` rows are housekeeping and never
  touch it.
- **Order within a poll.** Each batch is sorted by `(entity_key, seq_id)` before narration, so versions of one
  entity are narrated in version order.
- **Cache miss on an open status** (PENDING / WORKING / PARTIALLY_FILLED / OPEN). The seed holds every entity open
  at start, so the entity is new since start: "placed" or "opened". It is marked "found on reconnect" when
  `version_path = 'snapshot'`.
- **Cache miss on a terminal status.** "first seen by live view: {status}". It never says "placed".
- **No visible change.** An `APPLIED` row whose visible facts equal the cached facts (swap accrual, units-only
  change, catch-up re-read) becomes `debug`, "updated, no visible change", and is shown only with housekeeping on.
  Size changes stay invisible unless owner decision 8.2 adds a direction.

#### 4.4.2 Bracket model (StoneX and cTrader alike)

- **Closing orders are not children.** A reduce-only order with NULL `leg_kind` is a **closing order** (a
  closing trade), never a bracket child. StoneX writes each closing trade from history as a FILLED reduce-only order
  with `linked_position_id` = the opening order and `order_kind` NULL (`src/adapters/stonex/history.py:184-199`),
  and cTrader closing MARKET orders are reduce-only with no kind (`src/adapters/pepperstone/mapping.py:170-180,
  205`). Under the closing-order rule (§3.3) such a row has no level at any status, so it could never be classed by
  side. It is narrated as "closed (closing trade)" or, while still working, "closing order working"; with
  `version_path = 'history'` it is also marked "found while the engine was off".
- **Children.** A child is an order with `is_reduce_only`, a `linked_position_id` and a non-NULL `leg_kind`. StoneX
  children are separate reduce-only orders whose level is their own `entry_level`; the parent's `stop_level` /
  `target_level` stay empty (`src/adapters/stonex/orders.py:7-8, 462-494`; `.claude/rules/adapter-isolation.md`
  §2). The key `(adapter_id, linked_position_id)` equals the position's `position_id`, or the pending entry's
  `broker_order_id`.
- **Kind of child.** `leg_kind` STOP means stop-loss and LIMIT means take-profit. The page never guesses a kind
  from the side of entry (v3's loss-side rule is gone: under 8.1-A a NULL-kind row has no level to compare). So the
  page can show "no stop" where Oversee, with `XTNL_OVERSEE_STOP_AT_ENTRY` off, counts a kind-less child on the
  loss side as a stop; the breach panel, read from `lv_breaches`, stays the authority on whether a stop is missing.
- **Effective levels:**
  - stop = `COALESCE(parent.stop_level, STOP child.entry_level)`;
  - take-profit = `COALESCE(parent.target_level, LIMIT child.entry_level)`.

  These are used by the ladder, the story header and the level history.
- **Narration of children.** A change to a child's `entry_level` reads "EURUSD SELL 1039708845: stop-loss moved
  1.10203 → 1.09903" (or "take-profit moved"). It never reads "entry changed". A new child reads "stop-loss 1.10203
  attached". A child that is cancelled while its position closes is folded into the close.
- **Fold hold.** Activity lines are immutable numbered ring entries (§4.8): no event retracts or replaces a line
  already sent, and the two rows of a pair can arrive in different polls, because commit order is not id order
  (up to `MAX_TXN_MS` = 15 s, §4.3). So folding is done by **holding**, like the 60 s "engine stopped responding"
  hold (§4.4.7):
  - A **fold opener** is held instead of emitted: a child (`leg_kind` STOP or LIMIT) or closing order FILLED, a
    POSITION CLOSED, an entry order FILLED, or a POSITION OPEN that is new to the story. The last two are openers
    only on adapters in `POSITION_IS_ENTRY_ORDER` (§4.4.3), the only ones where a fill can be paired.
  - The hold waits for the **partners** the story's facts say are due: the other half of the pair (child or
    closing-order fill and position close; entry fill and position open) and, for a close, the terminal row of
    each sibling child still working.
  - The held line is emitted, folded, as soon as every due partner has arrived, or as it stands at the first poll
    whose `db_now` is 10 s or more past the poll that found the opener. The 10 s are counted in database time from
    detection, never from `at_ms`, so engine-clock skew cannot stretch or skip a hold.
  - The line takes its arrival number `n` when it is emitted. A partner that arrives after the hold ended is an
    ordinary line (a late sibling cancel is the housekeeping line "take-profit cancelled with the close").
  - Only these lines are held. Other lines of the same story are not delayed, and the status band and Open now
    (read from the views) show the fill or close at once. A held line is never lost on stop: a restart's backfill
    shows its rows.
- **Folding a close.** A child (`leg_kind` STOP or LIMIT) FILLED and the position CLOSED, within one hold, give one
  line: "closed by stop-loss" or "closed by take-profit", with "(take-profit cancelled)" when the sibling's
  cancellation arrived within the hold. A FILLED closing order (NULL `leg_kind`) folds into the close as "closed
  (closing trade)", never as stop-loss or take-profit, so a manual close is never read as a stop. A close with
  neither is "closed".
- **Folding a fill.** The entry order FILLED and the position OPEN, within one hold, give one line: "entry filled →
  position opened at L". The pair is known only on adapters whose position id is the entry order's id (the
  `POSITION_IS_ENTRY_ORDER` set, §4.4.3: StoneX and the mock). On cTrader `orderId` and `positionId` are separate id
  spaces and no position names its opening order, so no fill partner is ever due: the entry FILLED and the POSITION
  OPEN are emitted without a fill hold, as two lines in two trade stories. Close folding is unaffected, because a
  cTrader closing or SL/TP order carries the real `positionId` as its `linked_position_id`
  (`src/adapters/pepperstone/mapping.py:205-225`).
- **Levels in a close line.** The close line names the *level* of the child that filled, as last seen while it was
  working, and never an execution price (8.1).

#### 4.4.3 Stories

Each event gets a story key; the page builds threads from these keys.

| Story key | For | Rule |
|---|---|---|
| `pos:{adapter}:{position_id}` | a position, its children, its rule episodes | POSITION rows; an ORDER row with `linked_position_id` = L **only when a position with id L is known** (in the facts cache or the StoryIndex); and, **only for an adapter in `POSITION_IS_ENTRY_ORDER`** (below), an entry ORDER whose `broker_order_id` already names an existing `pos:` story (StoneX: position id = entry `OrderId`) |
| `ord:{adapter}:{broker_order_id}` | an entry order with no known position, and the bracket children linked to it | other ORDER rows. **Only for an adapter in `POSITION_IS_ENTRY_ORDER`**, an ORDER row with `linked_position_id` = L and no known position L joins `ord:{adapter}:L` when an entry order with `broker_order_id` L is known: StoneX children link to the parent `OrderId`, which can be a still-pending entry (§4.4.2; `adapter-isolation.md` §2), and are emitted after it. A linked ORDER row with neither known (on any other adapter: with no known position L) gets its own `ord:{adapter}:{broker_order_id}` story ("parent not seen"), which ends when that order is terminal; it is never filed under a `pos:` key that no position will close. When a POSITION row's `opened_by_client_order_id` equals an `ord:` story's `client_order_id` or `broker_order_id`, the server emits `story_merge {from, into}` and the page re-parents the thread, children included. With no such link, the stories stay separate: no guessing. That is every cTrader position: neither `map_position` (`src/adapters/pepperstone/mapping.py:234`) nor the position built from history (`:310`) sets `opened_by_client_order_id`, so a cTrader entry order and its position are always two trade stories. If the entry is cancelled unfilled, its story ends with it, children included |
| `acct:{adapter}:{account_id}` | rule episodes on ACCOUNT / ACCOUNT_DAY subjects; quarantine | Oversee and breach rows by `subject_type` |
| `halt:{raise id}` | one halt episode | from the `halt_events` row that raises the level above NONE until the row that returns it to NONE. Linked (not merged) to the story of every episode of the raising row's `rule_id` that is active at SOFT or HARD at the raise (from `lv_breaches`); none is guessed across rules, and a halt raised by escalation or by an operator links to nothing it cannot name |
| `out:{adapter}:{stream}:{conn_lost_at}`, or `out:{adapter}:{stream}:r{reconciled_at}` for a loss never seen | one stream outage that is not the leader-start fence (kind outage) | opened by a "lost" diff of `lv_stream_health` (a new non-null `conn_lost_at`, §4.3) that is not the leader-start fence. Such a loss can only follow the stream's reconcile in the current term (or be a stream's first row): LEADER_INIT fences every row and `SQL_STREAM_LOST` keeps an existing `conn_lost_at` (`COALESCE`), so a drop before the first reconcile writes nothing new. Closed by the next "reconciled" diff of the same (adapter, stream); when that reconcile is the stream's first in its epoch, the line itself belongs to the term's start-up story (§4.4.7) and the outage closes with a link to it. A "lost and reconciled between polls" diff whose previous `reconciled_epoch` already equals the new one is one closed outage keyed `out:{adapter}:{stream}:r{reconciled_at}` by the new `reconciled_at` (its `conn_lost_at` was never read, §4.3). A catch-up row that is not `startup` can join it, and losses its fence hold opened can fold into it ("Catch-up rows and outages", below). Every key part is a stored non-null value, so the key is the same after a live-view restart; a key is never built from a NULL (no `None` in a key) |
| `dlq:{root dlq_id}` | one DLQ episode (kind dlq) | every `lv_dlq` event of a row, keyed by `coalesce(origin_dlq_id, dlq_id)`, so a re-submitted row that is parked again joins its origin's story. Closed when the row is RESOLVED; QUARANTINED stays open (a release moves it back to QUEUED, §4.3). Linked to the `pos:` / `ord:` story whose `broker_order_id` or `position_id` equals `entity_id` for `event_type` ORDER / POSITION, when such a story exists; ACCOUNT and CASH_FLOW rows link to nothing |
| `term:{term_id}` | one start-up story (4.4.7; kind engine): one leader term | the term's start, LEADER and end lines; the leader-start fence (its `conn_lost_at` equals the term's `started_at`); `lv_catchups` rows whose `epoch` equals the term's `epoch_revision` and that join no outage (below); `lv_stream_health` "reconciled" events whose `reconciled_epoch` equals the term's `epoch_revision` and whose previous `reconciled_epoch` did not (the stream's first reconcile in the term). Membership is by these stored values only, never by time or by engine run, so a row that arrives late joins the same story (§4.4.7) |

- **`POSITION_IS_ENTRY_ORDER`.** The two id-equality rules above compare an order id with a position id. That is
  valid only where a position's id **is** its entry order's id: StoneX (`position_id = OrderId`,
  `src/adapters/stonex/mapping.py:183-195`; `adapter-isolation.md` §2) and the mock adapter, which copies that model
  (`src/adapters/mock.py:638-655`). On cTrader `orderId` and `positionId` are separate id spaces
  (`src/adapters/pepperstone/mapping.py:188-225, 234-253`), so a numeric match would file a row in an unrelated
  story. `narrate.py` holds the set as `POSITION_IS_ENTRY_ORDER = frozenset({"stonex", "mock"})`, keyed by
  `adapter_id` (`src/adapters/stonex/instruments.py:18`); an adapter not in it (cTrader `pepperstone`, and any future
  adapter until it is added) never uses either rule. It is a `frozenset`, not a tuple, so the env-var scanner of
  §4.1 never reads its strings. The `pos:` rule by `linked_position_id` (an order naming a known position) and the
  `opened_by_client_order_id` merge stay on for every adapter: they compare like with like.
- **Catch-up rows and outages.** What the code does (checked):
  - every catch-up request calls `hold_fence` first (`request`, `src/runtime/catchup.py:281-290`), which marks every
    stream of the adapter that is not already lost as lost, writing a new `conn_lost_at` through `SQL_STREAM_LOST`
    (`src/pubsub/orchestrator.py:1635-1646`);
  - no reconcile is written while a hold exists (`_mark_reconciled`, `src/pubsub/orchestrator.py:1498-1502`);
  - the job records the `catchup_runs` row (`src/runtime/catchup.py:573, 602`) and only then calls `release_fence`
    (`:398-408`), whose reconcile clears the held streams, or asks for a RESYNCING whose pass clears them later.

  So a catch-up row is always read **before** the reconcile that ends the outages open when it was written, and the
  hold's losses are read before the row. A `reconnect` is requested only after an interruption (`_follow_ups`,
  `:323-337`), and `_open_gap` / `_lose_all` mark the interrupted stream lost before that (`orchestrator.py:860-882`;
  a socket or auth loss marks every stream, so the hold then writes nothing new). Nothing stored tells a hold's
  loss from a real drop: both are the same `SQL_STREAM_LOST` with `now_ms()`, and `stream_health` has no reason
  column (`0001_init.sql:297-305`). The rules, all decided by read order in this process:
  - **Join, at read time.** A `lv_catchups` row with `reason` `reconnect`, `retry`, `operator` or
    `resume_after_sleep` joins the **oldest** outage story of its adapter that is open (state `lost`) when the row
    is read: the smallest `conn_lost_at`, ties broken by stream name (`account`, `orders`, `prices`). The fence hold
    keeps that outage open until after the row. With no open outage of that adapter, the row joins the start-up
    story of its term by its epoch (§4.4.7). A `startup` row always joins its term's story. A row found in the
    backfill finds no open outage (the stream-health diff is not backfilled, §4.3), so it joins its term's story.
  - **Open set.** When such a row R is read, the live view records `open(R)`: the outage stories of R's adapter open
    at that moment, plus the "lost and reconciled between polls" outages of that adapter found in R's poll (read
    after R, §4.3; their loss is not seen, so a hold loss and its release that both fall between two polls land
    here).
  - **Fold ("streams held for catch-up").** An outage in `open(R)` whose closing reconcile is the first "reconciled"
    diff of its (adapter, stream) read after R (R's poll included) was cleared by R's fence release. Each such outage
    other than the one R joined is folded into R's story: the server emits `story_merge {from, into, held: true}`
    (numbered like any merge, below). An outage is folded at most once, by the first row whose open set held it. An
    outage whose closing reconcile is the stream's first in its epoch is not folded: that reconcile belongs to the
    start-up story (§4.4.7) and the outage closes with a link to it, as above.
  - **Severity.** Lines of a folded outage are shown at `info` in R's story, grouped as "streams held for catch-up:
    prices, account (lost 08:14:02, reconciled 08:14:16)". The outage R joined keeps its own severity when R is a
    `reconnect`: it is the oldest open loss, written before the hold, so it is the interruption (or an older real
    loss), not a loss this hold made. When R is a `retry`, `operator` or `resume_after_sleep` row, no stream drop
    requested it, so the joined outage too is shown at `info` when it closes by the first reconcile after R; its
    header reads "StoneX streams held for retry catch-up 14 s · reconciled · complete".
  - **Accepted limit.** Because nothing stored distinguishes them, a real drop of another stream of the same adapter
    that is open when R is read, or that is lost and reconciled in R's poll, is folded and shown at `info` too (for a
    `retry`, `operator` or `resume_after_sleep` row, the joined real drop as well). Until R is read such a loss shows
    as an ordinary `warn` outage; after the fold it is still shown once, with its times and ids, in R's story. The
    band and the day panel show the streams' fenced state from `lv_stream_health` either way, and an incomplete
    catch-up still breaks out as a `warn` line wherever R lands.
- **Rule episodes.** An Oversee or breach row whose `subject_type` is POSITION or ORDER joins the story of its
  `subject_id` (an `entity_key`, `src/oversee/rules.py:94-96`) and carries `episode`. The page shows one episode as
  a sub-thread.
- **Story header.** For each story the StoryIndex keeps:
  - key, kind, symbol, side, ids;
  - first and last activity, state (`open` / `closed` / `active` / `resolved`), worst severity, counts by category;
  - the header sentence;
  - the effective levels;
  - the level history since 00:00 (at most 100 points of `{at_ms, entry, stop, take_profit}`);
  - the episodes, each with rule, level, cap, since, escalation estimate and `acked`;
  - the alert badges;
  - `related`: the keys of linked stories (halt ↔ causing episode's story, DLQ → trade, an outage closed by a
    stream's first reconcile in a term → that term's start-up story);
  - `merged_into`: the key of the story it was merged into, else null, and `held` (true when that merge is a
    "held for catch-up" fold);
  - for an outage, `gap_ms` (null while lost); for a DLQ story, its status;
  - the last 200 event numbers.

  These come only from rows the views already expose. Story states: trade `open` / `closed`, account and halt
  `active` / `resolved`, outage `lost` / `reconciled`, engine `starting` / `done`, DLQ `queued` / `quarantined` /
  `resolved`.
- **Merges are numbered.** `story_merge {from, into}` is a ring entry with its own `sid` (`<instance>.<n>`), like
  an activity event, so a merge that happens between the snapshot and the stream open, or during a reconnect, is
  replayed with the other ring entries (§4.8). A snapshot taken after the merge already shows it, because the
  `from` header carries `merged_into`. The page applies a merge idempotently. A `held: true` merge (a "held for
  catch-up" fold) re-parents the outage's lines into the `into` story and displays them at `info` with the label
  "held for catch-up"; the events themselves are not rewritten (the ring is append-only, and the console shows the
  line as emitted, tagged "held for catch-up" once folded), and the StoryIndex counts them as `info` in the `into`
  story's worst severity.
- **Trade stories always end.** Besides its own "Ends when" rule (§1.3), a trade story is `closed` once every
  order and position in it is terminal in the facts cache (no OPEN position, no PENDING / WORKING /
  PARTIALLY_FILLED order). So no linking gap can leave a trade story `open` for ever.
- **Memory bound.** Stories with no activity for 24 h that are not open are dropped. A story's events stay
  viewable while they are in the ring or in its own last-200 list.

#### 4.4.4 Titles (examples; every title starts with SYMBOL SIDE when the story knows them)

- Orders:
  - `EURUSD SELL stop order 1039708845 placed at 1.09903`;
  - `EURUSD SELL 1039708845: stop-loss 1.10203 attached`;
  - `EURUSD SELL 1039708845: entry filled → position opened at 1.09903`;
  - `EURUSD SELL 1039708845: stop-loss moved 1.10203 → 1.09903`;
  - `EURUSD SELL 1039708845: closed by stop-loss (take-profit cancelled)`;
  - `EURUSD SELL order 1039708847 cancelled`.
- Risk: `EURUSD SELL 1039708845: rule missing_stop_loss raised to SOFT`.
- Halt:
  - `Halt NONE → SOFT (missing_stop_loss)`;
  - `Halt cleared by june (reviewed event 6)`, with the note "reason recorded in operator_audit; read it with
    src.ops".
- Operator: `Operator june: halt.clear 401648225 (applied)`.
- DLQ: `ORDER stonex:1039708847 parked in DLQ (illegal_transition)`.
- Catch-up and engine:
  - `StoneX caught up (startup): 4 orders seen, 1 new, complete, offline 9 h 12 min`;
  - `Engine started on pc1`;
  - `Previous run on pc1 ended without a clean stop (last seen 02:14)`;
  - `pc1 is LEADER (epoch 150031)`;
  - `Term 812 draining (switchover)`;
  - `Term 811 closed at the start of term 812`.
- Streams:
  - `Streams fenced at leader start (epoch 150031)`;
  - `StoneX orders stream lost`;
  - `StoneX orders stream reconciled`;
  - `StoneX orders stream lost and reconciled between polls`;
- Closing orders: `EURUSD SELL 1039708845: closed (closing trade)` (a reduce-only order with no stop / limit
  kind, §4.4.2).
- DLQ story lines: `parked`, `retry N`, `parked again (same lineage)`, `quarantined after 5 retries`, `released
  from quarantine`, `resolved`.
- Other:
  - `SAFE_HOLD set: 3 abnormal term ends within 15 min`;
  - `Engine stopped responding` / `Engine responding again` / `Engine restarted (not responding for 40 s)`;
  - `RiskSweep stalled for 14 s` / `RiskSweep passing again`;
  - `Oversight lapsed: account 401648225`;
  - `StoneX: AUTH_FAILED (re-arm needed)` / `StoneX: AUTH_FAILED cleared (credentials changed)` (§4.5);
  - `Live view was not reading from 10:02:11 to 10:04:40; rows from then are shown now, marked late`.

#### 4.4.5 Privacy guard (`assert_safe`, always on)

Every event, and every row of the `state`, `story`, `day` and `series` payloads, passes `assert_safe` before it is
cached or sent. It **raises** `PrivacyError`; it never uses `assert`, which `-O` strips. Two outcomes, never more:
- **A forbidden or unknown name** (rules 1 and 3) drops the whole item. The kind and the failing *name* are logged,
  never a value.
- **A value that fails its slot type** (rule 2) is replaced by the marker "(not shown)" and the item is kept, so a
  real order or position never vanishes from the feed or from Open now because one value has an unexpected shape.
  The value is discarded, not shown; the kind and the slot name are counted in the log line.

1. **Closed allow-list.** `events.py` lists the allowed slot, `ids` and `fields` names for each event kind and
   payload type, and the allowed keys of each state and story row. Any other name is rejected.
2. **Typed slots.** Each value must match its slot type:

   | Slot type | Rule |
   |---|---|
   | `code` | an enum member, or the CODE pattern (§3.2), and not matching `\d[.,]\d` |
   | `command` | the COMMAND pattern (§3.2); used for `lv_operator_actions.command` |
   | `id` | the ID pattern, and not shaped like a decimal number (`\d[.,]\d`) |
   | `operator` | the OPERATOR pattern (§3.2), and not matching `\d[.,]\d`; used for `operator`, `approver`, `by_whom`, `resumed_by`. Operator names may contain `.` (`j.smith`), which the `id` slot refuses |
   | `symbol` | `^[A-Za-z0-9._/:#?-]{1,40}$`, and not matching `\d[.,]\d`. The colon is needed for `UNKNOWN:<id>` (`src/adapters/stonex/instruments.py:117`, `src/adapters/pepperstone/instruments.py:41`), the `?` for `UNKNOWN:?` (no market id), and 40 characters fit long cleaned StoneX names |
   | `side` | `BUY` or `SELL` |
   | `count` | an int from 0 to 10^9 |
   | `time` | an int number of ms |
   | `bool` | a boolean |
   | `level` | a Decimal rendered as a string; allowed **only** in the slots `entry`, `stop`, `take_profit`, `from_level`, `to_level` of the order, bracket and position kinds, and in level-history points |

   No slot holds free text. A title is rendered **only** from its kind's template and these slots. The rendered
   title is then checked: outside the level slots it must contain no decimal number.
3. **Token deny check** (second layer). Names are split on `_`. A name is rejected if any token is in: `units`,
   `qty`, `size`, `balance`, `equity`, `margin`, `pnl`, `amount`, `commission`, `swap`, `spread`, `pip`, `pips`,
   `point`, `bid`, `ask`, `mid`, `price`, `notional`, `currency`, `payload`, `body`, `detail`, `error`.
   The whole names `price_increment` and `min_stop_distance` are also rejected. (`subject` is not a deny token:
   `subject_id` / `subject_type` are ids and codes. The alert subject text is never fetched.) If owner decision 8.2
   picks B, `size_change` is the one named exception to the `size` token, and only with the values `up` / `down`. Whole-token matching means `ask`
   does not match `task` and `point` does not match `checkpoint`.

   The DLQ field is `dlq_reason`, so it passes this check: no token is `error`.
4. **Length.** Titles are built to fit at most 200 characters. When a template would overflow, ids are shortened
   (…) before the guard runs, so an event is never dropped for length.

#### 4.4.6 Alerts as badges

An alert is attached to the line that caused it:
- **`episode` and `repage`:** attached by `(ep_rule_id, ep_subject_type, ep_subject_id, ep_activation_id)`, compared
  with `(rule_id, subject_type, subject_id, coalesce(activation_id, 0))` of the Oversee and breach rows. The
  `coalesce` is needed because `episode_key` writes 0 for a missing `activation_id` (`src/oversee/notify.py:153`).
  The event's `episode` string uses the same `coalesce`, so both sides build the same key.
- **`trip`:** attached to the system `halt_events` row whose `at_ms` equals the alert's `created_at_ms`. Both are
  written with the same `now_ms` (`src/oversee/halt.py:160-161`).
- **`info` with `info_class` `halt_clear` or `halt_clear_operator`:** attached to the halt row with equal `at_ms`
  (`halt.py:175-176, 217-219`).
- **`info` with `info_class` `transition`:** attached to the one Oversee row with the same `account_id`, the same
  `(tr_transition, tr_rule_id, tr_subject_type, tr_subject_id)` as its `(transition, rule_id, subject_type,
  subject_id)`, and an `at_ms` equal to `created_at_ms` (`src/oversee/notify.py:159-170` uses `t.at_ms` for both). One sweep pass gives every
  transition of an account the same `at_ms`, so time alone would put a badge on unrelated stories; the parsed
  subject picks the right one, and the transition word separates two transitions of one subject in one pass (for
  example `capped` with `lowered`). If no row matches (or several do), the alert is a standalone line, never a badge on
  a guessed story.

`source_event` is never set by the INSERT (`src/db/notify_store.py` `SQL_OUTBOX_INSERT`), so it is not used.

**Badge states:**
- `queued`;
- `retrying` (shown as a `warn` line, once per row);
- `delivered`;
- `suppressed (duplicate)`;
- `NOT delivered` (`undeliverable`, shown as a `warn` line);
- `escalated` (shown as a `notice` line).

**Re-pages** become one counter on the episode, "re-paged N×, last HH:MM", and get no lines of their own.

**Standalone lines** exist only for alerts with nothing to attach to: `info_class = 'notice'` ("Engine notice
e-mailed (INFO)"; no key ties it to a term, so it is never filed in a start-up story by timing, §4.4.7) and
`transition` alerts that match no single Oversee row ("Rule {tr_rule_id} notice e-mailed"). The subject text is
never available.

#### 4.4.7 Start-up story

A **start-up story** is one leader term, keyed `term:{term_id}`. Every term has its own: a first start, an SM9
re-campaign in the same process (DRAINING → STANDBY → CANDIDATE → LEADER_INIT), a single-PC SAFE_HOLD auto-resume
campaign (`src/runtime/single_pc.py` docstring: the node "then campaigns normally"; `operations.md` §4) and a retry
after `init_failed`. Its members are found **only by values the database already records**, never by timing and
never by matching the term to an engine run. No start-up line is held while waiting for another row.
- **The epoch is the join key.** `leader_terms.epoch_revision` is the lease revision, which is also the fencing
  token `engine_epoch.epoch` (`src/db/epoch.py:75-88`). `catchup_runs.epoch` is written with the same token: the
  coordinator's `epoch_token` is `lambda: epoch.token` (`src/runtime/session.py:1160`), and the insert is fenced on
  it (`src/db/uptime_store.py:81-87, 232-238`). `stream_health.reconciled_epoch` is the same token too (§4.3,
  `src/db/repository.py:123-129`). Because every one of these writes is fenced, a row carrying epoch E was written
  while E was the current epoch, by the leader of the term whose `epoch_revision` is E. `(cluster_id,
  epoch_revision)` is unique (`0001_init.sql:77`), and `bump_epoch` refuses another `cluster_id` until an operator
  signs off a DR adoption (`operations.md` §2, §8). So within one cluster an epoch names exactly one term. If two
  known terms ever share an `epoch_revision` (only after such an adoption), a row with that epoch joins neither and
  stands alone.
- **Members.**
  - The term's own lines from `lv_leader_terms` (cursor and watched set, §4.3): started, LEADER reached, draining
    (`end_reason`), ended or closed by a successor ("Term 811 closed at the start of term 812" joins term 811's
    story).
  - The leader-start fence: the one "streams fenced at leader start (epoch N)" event whose `conn_lost_at` equals
    this term's `started_at` (§4.3). Both are written by LEADER_INIT step 1 in one transaction with the same
    `now_ms`, so the match is exact and both are read in the same poll. A fence-like value that matches no known
    term's `started_at` stands alone.
  - `lv_catchups` rows with `epoch` = the term's `epoch_revision`: the `startup` row of each adapter (LEADER_INIT
    step 6, `src/runtime/catchup.py:275-279`) always; a `reconnect`, `retry`, `operator` or `resume_after_sleep` row
    of that term only when no outage story of its adapter is open when it is read (otherwise it joins the oldest
    open one, §4.4.3 "Catch-up rows and outages").
  - `lv_stream_health` "reconciled" events whose new `reconciled_epoch` is the term's `epoch_revision` while the
    previous reading's `reconciled_epoch` was different (or the row is new): **the stream's first reconcile in this
    term**. LEADER_INIT fences every row, so that reconcile is the one that clears this term's fence, whatever
    reconnects happened before it (they write no new `conn_lost_at`, §4.4.3). A later reconcile in the same epoch
    is an outage's close (§4.4.3). A reconciled event whose epoch is an older term's never joins a newer story: it
    is filed by its own epoch.
- **Engine runs.** `lv_engine_runs` start, stop, "superseded" and "ended without a clean stop" lines are standalone
  `engine` lines carrying their `run_id` and node. They never own, open or close a start-up story, and a run row
  that lands polls after its process's first term (the `UptimeTracker` writes it from its first 5 s tick,
  `src/runtime/uptime.py:13, 199-205`) or a crashed run left unstopped (`src/db/uptime_store.py:36-37`) cannot
  misfile anything. The term line names its node, so the reader can see which machine led.
- **Opening.** The story opens when its term is first seen: in a poll, in the backfill, or in the start seed of
  open terms (§4.3; the seed is silent, so a term seeded at start gets its story but no "started" line). The story
  header and drawer read the term from the StoryIndex.
- **Late members.** A member found after the story closed still joins it by key, marked "reported late" when the
  late rule applies (§4.4). It does not re-open the story and changes only its counts and worst severity. A member
  whose story is no longer in memory (dropped after 24 h without activity, §4.4.3), or whose term this process never
  saw, is a standalone line carrying its epoch; it is never put in another story.
- **Active adapters.** LEADER_INIT fences **every** `stream_health` row (`src/db/leader.py:28-30`, no WHERE), and
  rows remain for any adapter that ever ran. The engine exports `xtnl_adapter_state{adapter,state}` for each
  configured adapter **only while the node is LEADER**: `src/runtime/telemetry.py:400-401, 426` calls
  `metrics_samples(leader=is_leader())` with `is_leader` = role LEADER, and `src/runtime/session.py:2445-2456` returns before the adapter lines
  when `leader` is false. So an answer from a LEADER_INIT, STANDBY or SAFE_HOLD node has no adapter lines, and an
  empty set from it means nothing. The rule:
  - The **active set** is taken only from a probe answer that has `xtnl_node_role{role="LEADER"} 1`: the adapters
    in its `xtnl_adapter_state` lines.
  - The last LEADER answer's set is **kept** across later non-leader answers and probe failures. It is replaced only
    by the next LEADER answer.
  - Before any LEADER answer has been seen by this process (and whenever the probe is off), every row counts, as in
    v2, and no row is called "inactive": the band shows the streams with "role: <role>" (from
    `xtnl_node_role`), or "engine probe off".
  - Rows of adapters outside the active set are shown "inactive" in the band and the day panel, and they never hold
    a story open or raise a "still lost" warning.
- **Closing.** A start-up story closes at the first of:
  - the term has reached LEADER (`lv_leader_terms.reached_leader_at` set) **and** every active adapter's
    `lv_stream_health` rows have `conn_lost_at IS NULL` and `reconciled_epoch` = the term's `epoch_revision`;
  - 120 s of database time after the poll that found the term;
  - the term ends: `end_reason` **or** `ended_at` is set (both are in the watched key, §4.3). DRAINING's
    `record_end_reason` sets only `end_reason` (`switchover` / `abdicate`, `src/db/leader.py:112-116`); a SAFE_HOLD
    abort in LEADER_INIT step 1 sets both (`SQL_END_TERM` with `init_failed`, `:57-58, 103`); a successor's close
    sets both (`SQL_CLOSE_PREDECESSORS`, `:31-35`, in the successor's epoch-bump transaction, so it is read in the
    same poll as the successor's term row). An `init_failed` hard exit (exit code 76, `src/runtime/exits.py:23`)
    writes nothing, so that term's story closes at the 120 s end or when its successor's term starts (whose close
    then records `init_failed`), whichever comes first.

  Still-fenced rows carry an older `reconciled_epoch`, so a story never closes while LEADER_INIT fences the streams,
  and those rows are never counted as "lost outside a story". With the probe off, or before any LEADER answer, every
  row counts, so a row of an adapter the engine no longer runs keeps the story open until the 120 s end. Closing
  only changes the story's state (`starting` → `done`); it never decides membership.
- **Contents, all at `info`.** Inside the story the fence, the reconciled lines and the catch-up rows are `info`.
  While a start-up story is open (and only for severity, never for filing), Oversee `stale_account_image`
  activations and `capped` / `uncapped` transitions are narrated at `info` in their own stories; Oversee rows are
  never members. Catch-up history rows (`late = history`) are nested under the story as "While the engine was off:
  …".
- **What comes back after a live-view restart.** The term lines and catch-up rows are cursor-tailed and come back
  in the backfill. The fence and reconciled lines come from the `lv_stream_health` diff, whose first reading after a
  start is silent (§4.3) and is not backfilled: a fence or reconcile that happened while the live view was down is
  shown only as the band's current stream state, not as a line.
- **Planned restarts.** An "Engine stopped responding" line from the probe (§4.6) is held for 60 s after the probe's
  last success. If a new engine run appears within that time, it is emitted at `info` instead, as the standalone
  line "Engine restarted (not responding for N s)" carrying the new `run_id`. Otherwise it is emitted at `alert`
  when the 60 s end. Banner 4 is not held: it shows as soon as the probe has failed 3 times.
- **Break-outs.** These members keep a raised severity inside the story, as `warn` lines:
  - a stream of an active adapter still lost 60 s after the story opened (before any LEADER answer has been seen,
    every row counts as active, as above);
  - an incomplete catch-up (`complete = false`) or `anomalies > 0`.

  SOFT and HARD transitions and an unclean previous run are never members: they are lines in their own stories or
  standalone `warn` / `alert` lines (§4.4.8).

#### 4.4.8 Severity

| Severity | Events |
|---|---|
| `alert` | halt raised; HARD transitions; "engine stopped responding" (not followed by a new run within 60 s); DLQ quarantined |
| `warn` | SOFT transitions; stream of an active adapter lost (not the leader-start fence; a loss later folded as "held for catch-up" is shown at `info`, §4.4.3), or still lost 60 s after its start-up story opened; catch-up incomplete; previous run unclean; alert retrying / NOT delivered; AUTH_FAILED; oversight lapsed; SAFE_HOLD set; RiskSweep stalled |
| `notice` | fills, closes, level moves, operator actions, halt cleared, escalated |
| `info` | everything else |
| `debug` (housekeeping) | `DROPPED_STALE`, ACCOUNT updates, no-visible-change rows, DLQ QUEUED / LEASED / BACKOFF cycles, plain re-park retries |

### 4.5 StateReader (`state.py`)

The StateReader runs in the same transaction as the tailer each poll. It pushes `event: state` only when the result
changes.

- **Halt:** the latest `lv_halt_events` row.
- **Open breaches:** `lv_breaches WHERE resolved_at IS NULL AND level IS NOT NULL`. Pending-only rows
  (`level IS NULL AND pending_level IS NOT NULL`) are counted separately.
- **Escalation estimate.** HARD comes when an active, uncapped SOFT state reaches
  `uncapped_ms >= escalate_after_ms` (`src/oversee/sweep.py:259-266`).
  - The estimate is `escalate_after_ms − uncapped_ms` for each `level = 'SOFT' AND cap = 'NONE'` row, shown as
    "about N min".
  - It is computed as of `last_pass_at_ms`. The clock moves only while the RiskSweep runs, so the page labels it
    as an estimate.
  - `cap = 'SOFT'` shows "capped: will not escalate".
- **Cool-down.** When the halt is SOFT and no active state is at SOFT or HARD, the clear is expected at
  `max(below_soft_since) + cool_down_ms` (`src/oversee/states.py:157-158`).
- **Timing:** `lv_ruleset_timing ORDER BY ruleset_version DESC LIMIT 1` (the row `src/db/ruleset_store.py:20`
  treats as active), with the defaults from `src/oversee/config.py` when NULL. A test pins the defaults to
  `SweepConfig`.
- **Open now:** `lv_open_positions` and `lv_open_orders` (LIMIT 500 each), folded by the bracket model. Each row
  carries its story key.
- **Alert delivery:** `count(*)` and `min(created_at_ms)` over `lv_alerts WHERE delivery IN ('queued','retrying')
  AND created_at_ms >= db_now − 7 d`, which is the dead-man's `oldest_undelivered`. Also the count of
  `undeliverable` rows and of escalated rows today.
- **DLQ:** counts by status.
- **Also read:** stream health, cluster state, epoch, quarantine (`released_at_ms IS NULL`), auth state (the latest
  row per adapter).
- **Auth state shown.** A new credential generation that authenticates writes nothing to `adapter_auth_state`
  (§4.3), so the latest row alone would keep "AUTH_FAILED (re-arm needed)" next to a STREAMING adapter for ever. The
  probe decides, but only on **evidence of a successful authentication after the failure**:
  - **Evidence.** A LEADER probe answer, from a poll later than the one that first read the row, with
    `xtnl_adapter_state{adapter=<id>,state=S} 1` for S in `SNAPSHOTTING`, `STREAMING` or `RESYNCING`. SM4 reaches
    these only through `on_authenticated` (AUTHENTICATING → SNAPSHOTTING, then SNAPSHOTTING → STREAMING, and
    RESYNCING only from STREAMING; `src/pubsub/connection.py:90-93, 132-135, 140-155`), and it leaves AUTH_FAILED only through
    AUTHENTICATING (`adapter-isolation.md` §5). SM4 enters AUTH_FAILED before its row is written
    (`src/runtime/session.py:1966`), so an answer read in a later poll and showing one of these states proves the
    adapter authenticated after that failure. An answer from the same poll could predate the failure and is not
    used.
  - **No evidence.** `DISCONNECTED`, `AUTHENTICATING` and `BACKOFF` say nothing. On a credential-generation change
    SM4 goes AUTH_FAILED → AUTHENTICATING and tries the new credentials (`src/pubsub/orchestrator.py:2427-2453`): they
    may still fail (a new row follows). When `adapter_auth_state` cannot be read, SM4 goes to BACKOFF
    (`:2435-2438`), and its next AUTHENTICATING can meet `known_bad` and return to AUTH_FAILED **without writing a
    row** (`:2440-2445`), so no new AUTH_FAILED event could ever correct an early "cleared". `AUTH_FAILED` itself
    keeps the failure, and so does an adapter missing from the LEADER answer ("inactive", §4.4.7).
  - **Shown.** A row with `state = 'AUTH_FAILED'` is shown as "AUTH_FAILED (as recorded, since T)", with T its
    `failed_at_ms`, until evidence arrives; then as "AUTH_FAILED cleared (credentials changed)" until a newer row for
    that adapter replaces it. If a later LEADER answer shows `state="AUTH_FAILED"` again with no newer row, the band
    shows AUTH_FAILED again (from the probe; no event without a row). With the probe off or before any LEADER
    answer, the row stays "as recorded, since T": the band says what is recorded and when, and never guesses. A
    `REARMED` row is shown as "re-armed".
  - **Event.** One `info` event "StoneX: AUTH_FAILED cleared (credentials changed)" is emitted per
    `(adapter_id, failed_at_ms)`, and **only for a failure this process saw arrive**: a row whose AUTH_FAILED event
    the tailer emitted after start (a changed `lv_auth_state` key, §4.3). A row already present at start is the
    first reading, which is silent (§4.3): when evidence arrives the band changes to "cleared", but no event is
    emitted. So a failure fixed weeks ago, whose AUTH_FAILED row stays the newest row for that adapter, never
    re-emits "cleared" after a live-view restart.

### 4.6 Engine probe (`probe.py`)

Each poll the probe sends `GET http://<XTNL_LIVEVIEW_ENGINE_METRICS>/metrics`.
- **Limits.** Timeout 2 s; at most 1 MiB is read; the probe follows no redirects.
- **Parsing.** Each line is matched against a strict regex
  `^(xtnl_[a-z_]+)(\{[^}]*\})? (-?[0-9.eE+]+|NaN)$`, and only these names are kept:
  - `xtnl_exporter_up`;
  - `xtnl_node_role{role}`;
  - `xtnl_safe_hold`;
  - `xtnl_engine_epoch`;
  - `xtnl_adapter_state{adapter,state}`;
  - `xtnl_risksweep_oldest_pass_age_seconds`;
  - `xtnl_deadman_configured`, `xtnl_deadman_consecutive_failures`, `xtnl_deadman_last_ok_age_seconds` (home-server
    plan W1: whether `XTNL_DEADMAN_URL` is set, the current failed-beat streak, the age of the last acknowledged
    beat; leader only, like the adapter lines).

  Every other line is dropped unread. These are states, counts and ages, never money
  (`src/observability/metrics.py`). Nine names in all.
- **Results.**
  - Engine `running` with its role, the RiskSweep pass age, the dead-man receiver line (`dead-man receiver: OK,
    last ack N s ago`, `not acknowledging, N failed beats`, `not configured`; state `engine.deadman`, from a LEADER
    answer only; `src/liveview/state.py` `deadman_view`) and the SM4 state for each adapter. The adapter lines
    and the pass age exist only in a LEADER answer (`src/runtime/session.py:2449`); the probe records, with each
    answer, whether it had `xtnl_node_role{role="LEADER"} 1`, and the active adapter set follows only LEADER
    answers (§4.4.7).
  - After 3 consecutive failures, `not responding since <last success>`. This greys the leader, stream and halt
    values and raises banner 4 at once. The `alert` event "Engine stopped responding" is held for 60 s and
    replaced by the `info` line "Engine restarted (not responding for N s)" if a new run appears in that time
    (§4.4.7). "Engine responding again" follows on
    recovery.
  - **RiskSweep stalled.** The exporter runs on the engine's event loop (`src/observability/exporter.py`
    docstring: "asyncio streams, no thread"), so a wedged loop already shows as "not responding". But the loop can
    be alive while the RiskSweep makes no progress. So when the engine is LEADER and
    `xtnl_risksweep_oldest_pass_age_seconds` is above 10 (the sweep passes every second), the band shows "engine
    running, RiskSweep stalled for N s" and one `warn` event "RiskSweep stalled" is emitted, with "RiskSweep passing
    again" on recovery. The metric is exported only by the leader (`src/runtime/session.py:2449-2461`), so its
    absence on a STANDBY or SAFE_HOLD node is not a stall. The metric is the age of the **oldest** account's last
    pass (`max(0, now − min(passes))`), so two cases are not judged as a stall: while `lv_sweep_quarantine` has an
    unreleased row (a quarantined account is retried only every 10 min; the band already shows "Oversight
    lapsed"), and within 60 s of the term's `reached_leader_at` (before the sweep has listed every account). The
    pass-age sparkline still shows the value in both cases.
  - `engine_runs` is used only for history: its heartbeat is every 900 s by default, too slow to show liveness.

### 4.7 Day model and series (`stories.py`)

These are server-side and in memory. They are rebuilt from the backfill at start and reset at local 00:00.

- **`day.buckets`.** 96 × 15 min, each `{activity, notice, warn, alert, housekeeping}` counts from every event with
  `at_ms` (or `detected_ms` when late) inside the bucket. When a source's backfill hit its row limit, the buckets
  before its oldest loaded row are marked `partial`.
- **`day.halt`.** A list of `{from_ms, to_ms|null, level, hard_latch}`, built from the last `lv_halt_events` row
  before 00:00 and every row since.
- **`day.engine`.** A list of `{from_ms, to_ms, state}`:
  - `running`, from `started_at_ms` to `stopped_at_ms`, or to now for the current run;
  - `unknown`, from `last_seen_ms` to the next start, for a run without a clean stop;
  - `engine_off` between runs.

  The current run is known to be running only while the probe answers. A probe failure turns its tail `unknown`.
- **`day.streams[adapter][stream]`.** A list of `{from_ms, to_ms, state}`, built from these facts in priority order:
  1. live diffs observed by this process: `down` from `conn_lost_at` to `reconciled_at`; `fenced` from a fence to
     its reconcile;
  2. at start, a row with `conn_lost_at IS NULL` is `up` from `max(reconciled_at, 00:00)` to now; a row with
     `conn_lost_at` set is `down` (or `fenced`) since then;
  3. `lv_catchups` `gap_start_ms..gap_end_ms` for that adapter: `down`;
  4. `day.engine` `engine_off` / `unknown`: the same state for every stream;
  5. anything else: `not_observed`.

  An adapter that is inactive (§4.4.7) has its rows drawn `inactive` from the first **LEADER** probe answer that
  left it out; earlier parts, and every part before this process saw a LEADER answer, follow rules 1-5. A non-leader
  answer never starts an `inactive` segment. Intervals are never interpolated. These observations are not persisted, so after
  a live-view restart the earlier part of the day falls back to rules 2-5.
- **`series`.** Each series is sampled every poll into 1-minute points (the maximum in each minute), keeping 60
  points:
  - `sweep_pass_age_s` (probe);
  - `alerts_waiting`;
  - `dlq_queued`;
  - `open_breaches`.

  Each is pushed as `event: series` once a minute.

### 4.8 HTTP (`server.py`)

The app runs under `web.AppRunner(app, handler_cancellation=True, shutdown_timeout=2, access_log=None)`.

| Route | Response |
|---|---|
| `GET /` | `index.html` |
| `GET /edge` | `edge.html`, the static Phase 2 shell (§1.8): bytes loaded at start, `text/html; charset=utf-8`, the same request checks and headers as every route. It reads nothing and has no handler logic beyond returning those bytes; a query string is ignored and `/edge/` is not routed (404) |
| `GET /static/app.js`, `GET /static/edge.js`, `GET /static/app.css` | the bytes loaded at start. Served with `web.Response(body=…)` and fixed types `text/html; charset=utf-8`, `text/javascript; charset=utf-8`, `text/css; charset=utf-8`. Never `add_static`, `FileResponse` or `mimetypes` (the Windows registry can map `.js` to `text/plain`, which `nosniff` then blocks). No path from the URL reaches the filesystem |
| `GET /api/snapshot` | `{"instance", "last_sid", "state", "events": [the ring, newest HISTORY], "stories": [headers of stories touched today, ≤ 500], "day", "series", "poll": {"at_ms", "db": "ok"\|"down", "engine": "ok"\|"down"\|"off", "backlog": n}}` |
| `GET /api/stream?instance=<uuid>&after=<n>` | `text/event-stream`, detailed below. Registered with `allow_head=False` |
| `GET /api/story?key=<key>` | one story's header, its events from memory and its level history. `key` must match `^(pos\|ord\|acct\|halt\|term\|out\|dlq):[A-Za-z0-9:._@/-]{1,120}$`, else 400. Unknown key: 404. Dictionary lookup only, never SQL |
| `GET /healthz` | `200 ok` / `503 db down` |

**The stream (`/api/stream`):**
- **Event types.** `event: hello` (the instance) is always sent first. Then `activity` (one Event), `state`,
  `story` (a header changed), `story_merge`, `day` (an interval or bucket changed), `series`, and `resync`. A
  `: ping` comment is sent every 15 s.
- **Ids.** Each `activity` and `story_merge` event's `id:` is `<instance>.<n>`, so a browser reconnect can tell
  this process from an old one. Both are ring entries and both are replayed (§4.4.3 "Merges are numbered").
- **Where to resume.** The page never relies on the browser's own reconnect (see "Reconnect" below), so every
  stream it opens is a fresh `EventSource` whose query (`instance`, `after`) it fills from the snapshot it just
  rendered. Nothing is lost or repeated between the snapshot and the stream. A `Last-Event-ID` header, which only a
  native reconnect sends, still wins over the query when present, and is parsed as below.
- **Current state after every (re)open.** `state`, `story`, `day` and `series` carry no id and are pushed only on
  change, so a change made during a gap would otherwise be lost. After `hello` and any replay, the server therefore
  always sends the current `state`, `day` and `series`, plus the headers of every story touched by the replayed
  events.
- **Parsing.** The id is parsed with `^([0-9a-f-]{36})\.(\d{1,18})$`. Replay sends the newer ring events. A
  different instance, a malformed id or an id older than the ring gets `resync` and the stream is closed. A
  missing id on both gets `resync`. Nothing raises; a 4300-digit header is just malformed.
- **Fan-out.** Each client has a queue of 2000, filled with `put_nowait`. On `QueueFull` the server clears that
  client's queue and enqueues `resync` and a close sentinel.
- **Writes.** Every write is `asyncio.wait_for(resp.write(…), 5)`. A timeout or `ClientConnectionResetError` calls
  `request.transport.abort()`. The client count is decremented in `finally`, so a closed tab frees its slot within
  about 2 s (`handler_cancellation`).
- **Limit.** Beyond `MAX_CLIENTS` the answer is `503` (JSON `{"error": "busy"}`).

**Request checks, in this order:**
1. **Host** (DNS-rebinding defence). `request.headers.get("Host")`, lowercased, must equal
   `<bound literal>:<port>` or `localhost:<port>` (IPv6 bracketed). The forms without a port are also accepted when
   the port is 80. A missing Host gets 421, and so does any other value.
2. **Cross-site.** If `Sec-Fetch-Site` is present and not `same-origin` / `none`, the answer is 403. It is also 403
   if `Origin` is present and is not `http://<an allowed Host>`. This check runs before a stream slot is counted, so
   another website cannot use up the slots.
3. **Method.** GET only (HEAD on non-stream routes). Every other method gets 405.

**Headers on every response.** They are added in `app.on_response_prepare`, so 404, 405, 421, 403, 503, 500 and
the SSE response all carry them:
- `Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'none'`;
- `X-Content-Type-Options: nosniff`;
- `X-Frame-Options: DENY`;
- `Referrer-Policy: no-referrer`;
- `Cache-Control: no-store`;
- `Cross-Origin-Resource-Policy: same-origin`;
- `Cross-Origin-Opener-Policy: same-origin`.

There are no CORS headers.

**Errors.** A catch-all middleware logs only `type(exc).__name__` and returns a plain `500 internal error`.

**The page (`app.js`):**
- It builds the DOM with `textContent` and `createElement` only: no `innerHTML`, no inline script, no inline style
  attribute strings. SVG ladders and sparklines are built with `createElementNS`.
- **Reconnect.** The page calls `es.close()` on every `resync` event and on **every** `onerror`, whatever the
  `readyState`. After a normal end of the response (the server's close after `resync`, or a server restart) the
  browser's native reconnect would otherwise run in state CONNECTING, not CLOSED, and resend the stale
  `Last-Event-ID`, which wins over the query: it would get `resync` again about every 3 s, forever. Having closed
  the stream, the page backs off from 1 s doubling to 30 s (reset after a stream has stayed open for 60 s),
  re-fetches `/api/snapshot`, renders it, then opens a new `/api/stream?instance=…&after=…`.
- **Too many tabs.** A snapshot fetch that takes more than 5 s shows "too many live-view tabs open in this
  browser?".
- **Glossary.** A static glossary maps reason and rule codes to one sentence each (`risk_unknown`,
  `stale_account_image`, `daily_loss(partial_session)`, …). An unknown code is shown as the code itself.

### 4.9 Process and logging

- **Command.** `python -m src.liveview [--check-config] [--stop-on-stdin-eof]`.
- **Exit codes.** 0 on stop; 78 on a configuration error (the supervisor leaves it off until the next start); 75 on
  a temporary failure.
- **Stdin EOF.** A daemon thread reads the raw descriptor, `os.read(sys.stdin.fileno(), 4096)`, until EOF, then
  `loop.call_soon_threadsafe(stop.set)`. It works on the Windows Proactor loop. It does not use
  `sys.stdin.buffer.read` (the `src/api/__main__.py` pattern): a daemon thread blocked in a buffered read holds the
  buffer's lock, and the interpreter then aborts at exit ("could not acquire lock … at interpreter shutdown"), so
  the exit code (78 / 75 / 0) never reaches `start-xtnl.ps1`.
- **Stop order.** Set the stop event, push the close sentinel into every client queue, `runner.cleanup()`, close
  the pool. The process exits within 5 s.
- **Logging.**
  - Logging goes through `src.observability.logs.configure_logging`, with the `src/api/__main__.py`
    `JsonFormatter` approach: exception type only.
  - There is one line per start, stop, db up / down, probe up / down, client-count change and backlog; and one
    count line per expired-pending batch or privacy-guard drop.
  - Nothing from a row and no exception message is logged.
  - The aiohttp access log is off (`access_log=None`).

## 5. Deployment on the PC (files to change)

**`deploy/local/start-xtnl.ps1`:**
- Add `Step-LiveView` / `Stop-LiveView`. The live view is a third supervised child,
  `python -m src.liveview --stop-on-stdin-eof`, log prefix `[liveview] `. It is started **inside step 8 only**,
  after the API (`start-xtnl.ps1:653-667`), when `XTNL_LIVEVIEW_DSN` is in the Windows User environment and
  `-NoLiveView` is not given. So it is down whenever docker, migrations or the clock wait are retrying, and the
  page's "server down" means that.
- Restart back-off is like the API's: 5 s doubling to 300 s; exit 78 disables it until the next start.
- The wait loop's `foreach ($st in @($script:Engine, $script:Api.St))` gains the live view's stream, and
  `Invoke-Cleanup` calls `Stop-LiveView`.
- `$LiveViewDropEnv` holds only the **explicit** prefixes and names of the §4.1 refuse-list, never the generic
  `(PASS|SECRET|TOKEN|DSN|KEY)` rule. `Start-Pumped` removes every name that is `-like` any entry
  (`start-xtnl.ps1:201-206`), so a wildcard such as `XTNL_*DSN*` would also remove `XTNL_LIVEVIEW_DSN`, and the
  child would exit 78 on every start. The list is:
  `'STONE_X_*', 'CTRADER_*', 'XTNL_MD_*', 'XTNL_ARM_*', 'XTNL_SMTP_*', 'XTNL_SES_*', 'XTNL_PUSH_*', 'XTNL_API_*',
  'ORDER_SERVER_DSN', 'REDIS_URL', 'BROKER_ENV', 'XTNL_DEADMAN_TOKEN', 'XTNL_HEARTBEAT_COPY_TOKEN',
  'XTNL_DASHBOARD_DSN', 'XTNL_HEARTBEAT_COPY_URL', 'XTNL_BACKUP_*', 'RCLONE_*', 'B2_*'` (the last four since
  home-server plan W2). It covers every `$UserVarNames` entry except `XTNL_LIVEVIEW_DSN`, and none of its entries
  matches `XTNL_LIVEVIEW_DSN`. The generic rule lives only in `settings.py`.
- `XTNL_LIVEVIEW_DSN` is added to `$UserVarNames`, `$ForbiddenInFile`, `$EngineDropEnv` and `$ApiDropEnv`.

**Other files:**
- **`src/api/settings.py`:** `FORBIDDEN_NAMES` gains `"XTNL_LIVEVIEW_DSN"`. This is the name only, not an
  `XTNL_LIVEVIEW_` prefix, so the non-secret `XTNL_LIVEVIEW_ADDR` etc. never make the API exit 78. It keeps
  `$ApiDropEnv` equal to `FORBIDDEN_PREFIXES* ∪ FORBIDDEN_NAMES`
  (`tests/deploy/test_order_server_api_assets.py:103`, `:199-216`).
- **`src/db/readonly_role.py` (new), `src/dashboard/server.py`, `src/api/pinescript.py`:** the shared role check
  (§4.2).
- **`deploy/local/new-liveview-credentials.ps1` (new; the owner runs it):**
  - It is ASCII and says "never sends orders".
  - `Set-Location -LiteralPath $Root`.
  - It checks first that `rolname = 'xtnl_liveview'` exists.
  - It makes a 32-byte `RandomNumberGenerator` password, written as hex.
  - It runs `ALTER ROLE xtnl_liveview LOGIN PASSWORD …` only through `StandardInput.WriteLine` of
    `docker exec … psql` (never in `$psi.Arguments`).
  - Its only `SetEnvironmentVariable` is `($DsnName, $dsn, 'User')`; the DSN is `postgresql://xtnl_liveview:` with
    `127.0.0.1:55433/xtnl`.
  - It never prints or writes `$pw` or `$dsn`.
  - `-Rotate` forces a new password.

  It is **not** added to `SCRIPTS` in `test_single_pc_autostart.py`, which only allows `'Process'`. It gets its own
  tests (§6). The runbook says to restart start-xtnl after `-Rotate`: the User environment is read once at start.
- **`deploy/local/status-xtnl.ps1`:** "live view: listening on <configured address>". It probes the configured
  address, not a fixed one. `stop-xtnl.ps1` mentions the live view.
- **Environment examples:**
  - `deploy/env/engine.env.example` assigns `XTNL_LIVEVIEW_ADDR`, `_POLL_MS`, `_HISTORY`, `_BACKFILL_H`,
    `_MAX_CLIENTS` and `_ENGINE_METRICS`.
  - It documents `XTNL_LIVEVIEW_DSN` only in two consecutive `#` lines, one of which says "Windows User", as the
    `test_every_env_var_the_code_reads_is_documented` regex requires.
  - `local-live.env.example` assigns only names the code reads.
- **Runbooks:** new `docs/runbooks/live-view.md` (Trigger / Checks / Steps / Rollback, a ``` block, "never sends
  orders"), plus a link in `docs/runbooks/README.md`.
- **Rules and CLAUDE.md:** `operations.md` §9, §12 (view pinning) and §17 gain one line each, and `CLAUDE.md`
  gains one line.

## 6. Tests

**Database:**
- `tests/db/test_liveview_role_migration.py` (§3.5).
- `tests/db/test_arm_schema_no_market_data.py`: the new allow-list group (§3.4), and a check that all 18 lv_ views
  are seen by `added_columns`.
- The scan test also confirms that no view uses `WITH (` or a column list.
- `tests/db/test_schema_compat.py` passes unchanged: the service SQL names no 0015 column.
- Every reason code the Oversee rules and the halt switch emit (including the composite
  `daily_loss(partial_session,stale_account_image)`), every `dlq_rows` reason and every `end_reason` the
  `UptimeTracker` writes match the CODE pattern. Otherwise a real code would show as "reason not shown".
- Every operator command string under `src/ops/` matches the COMMAND pattern and fails the CODE pattern (so the
  guard choice matters).
- The three `safe_hold_reason` phrases of `src/db/leader.py:95-99`, built with sample counts, map to `already_set`
  / `abnormal_term_ends` / `epoch_changes` with the right `hold_count` (a `pg` test against the view; skipped
  without a database). Any other text gives NULL.
- Every `alerts_for_transitions` subject (`src/oversee/notify.py:159-170`), for each subject type and for an
  `account_day` subject id with a date, matches the `tm` pattern, and `tr_subject_type` equals `subject_to_db` of
  the transition's subject type.
- `lv_entity_events` returns CASH_FLOW and INSTRUMENT rows with `entity_key`, `account_id`, `broker_ms` and `facts`
  all NULL; `lv_alerts` returns `kind = 'arm'` rows with every column but `outbox_id`, `created_at_ms` and `kind`
  NULL.
- `UNKNOWN:<id>` and `UNKNOWN:?` from both adapters' `unknown_symbol` pass the `symbol` slot.
- **Closing-order rule** (`pg` tests against the views): a cTrader PARTIALLY_FILLED reduce-only MARKET order (no
  `order_kind`, `entry_price` from `executionPrice`), a part-filled-then-CANCELLED one, a StoneX history closing
  trade and a FILLED child all give `entry_level` NULL, in `lv_open_orders` and in `lv_entity_events.facts`; a
  WORKING STOP / LIMIT child keeps its level.
- `lv_auth_state` returns one row per adapter when `adapter_auth_state` holds two credential generations of it
  (the later `failed_at_ms` wins).
- `lv_cluster_state.resumed_by` is `auto-resume` for `system:single-pc-auto-resume`, the name for a valid operator,
  and NULL for anything else.
- Every `alerts_for_transitions` info subject gives `tr_transition` equal to the transition word.
- 0021 creates `event_log_log_id_idx` with `ON ONLY event_log (log_id)`, and it is the only index it creates.
- `tests/db/test_migrate_post_step.py`: `test_registered_items` (`:164`, an exact tuple) gains
  `PartitionIndex(21, "event_log", "event_log_log_id_idx", "log_id_idx", "log_id")` at its end, and a test modelled
  on `test_the_0017_event_log_index_is_built_and_attached_per_partition` checks that the post step runs
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS <partition>_log_id_idx ON <partition> (log_id)` and
  `ALTER INDEX event_log_log_id_idx ATTACH PARTITION …` for every partition, the DEFAULT one included.

**Deploy tests (named edits):**
- `tests/deploy/test_order_server_api_assets.py:107`: `$EngineDropEnv` also has `XTNL_LIVEVIEW_DSN`.
- New tests in `tests/deploy/test_order_server_api_assets.py`:
  - `$LiveViewDropEnv` covers every `$UserVarNames` entry except `XTNL_LIVEVIEW_DSN`;
  - `XTNL_LIVEVIEW_DSN` matches **no** `$LiveViewDropEnv` pattern (`fnmatch.fnmatchcase` with both sides
    upper-cased, since PowerShell `-like` ignores case), and no entry has a `*` anywhere but at its end;
  - `Step-LiveView` / `Stop-LiveView` order and the wait-loop / `Invoke-Cleanup` entries, like lines 78-94.
- `tests/deploy/test_single_pc_autostart.py:329`: `$ForbiddenInFile` gains `{"XTNL_LIVEVIEW_DSN"}`. The 0021 role
  block `-TotalCount N` is pinned.
- `tests/deploy/test_deploy_assets.py`:
  - `USER_ENVIRONMENT_ONLY` gains `XTNL_LIVEVIEW_DSN`;
  - `RUNBOOK_FILES` gains `"live-view.md"`.
- New `tests/deploy/test_liveview_credentials.py` checks the credentials script shape (§5), modelled on
  `test_credentials_script_shape`, `…_never_prints_or_files_the_secret` and `…_sets_the_read_only_db_role`.

**`tests/liveview/` (with `__init__.py`):**
- **settings:**
  - only literal loopback addresses; `localhost` is refused;
  - bounds;
  - every refused name and prefix exits 78, and the message names the variable without its value;
  - an unrelated `GITHUB_TOKEN` is not refused;
  - `ENGINE_METRICS=off`;
  - no module under `src/liveview` other than `settings.py` contains `environ` or `getenv` or defines a
    `*_VAR` / `*_ENV` constant, and no module there holds a tuple of strings that match `ENV_NAME` (§4);
  - `test_every_env_var_the_code_reads_is_documented` passes with `src/liveview` present.
- **sql:**
  - every statement is a bounded `SELECT` from an lv_ view, or `SELECT now_ms()`;
  - `ALL_VIEWS` equals the views named.
- **role check:**
  - `xtnl_liveview` passes;
  - fake `xtnl_dashboard` / `xtnl_api` / engine / superuser / `pg_read_all_data` member rows are refused with exit
    78;
  - an import of `src.liveview.__main__` loads no `src.cache*`, `redis*`, `src.dashboard*` or `src.oversee*`;
  - a login refusal (`28000`, `28P01`, `3D000`) exits 78 naming `XTNL_LIVEVIEW_DSN`, at start and on a later
    reconnect; a socket failure, `57P03` or `53300` reports `db: down` and retries;
  - `tests/dashboard/test_dashboard.py` and `tests/api/test_export_memory.py` pass unchanged after the move to
    `src/db/readonly_role.py`.
- **tailer**, against a fake database with commit control and an engine clock offset:
  1. 1 200 rows committed in one second are all emitted, exactly once.
  2. An order row with an older `seq_id` and a higher `log_id`, committed 45 s after a fresher position row, is
     emitted.
  3. An engine clock 120 s behind, or ahead of, the database clock loses nothing.
  4. A database outage of 5 min with the pool in back-off loses nothing.
  5. A commit that takes 90 s is emitted by the late recheck, marked late.
  6. An empty source at start emits nothing old, and a source with rows from months ago emits nothing.
  7. A quiet hour produces no repeated "Engine started" / "is LEADER" / halt / audit line (10 min of polls with no
     duplicates).
  8. A rolled-back id expires into `Expired` and its count is logged.
  9. An alert whose send takes 50 s still produces "delivered"; deduplicated, gave-up and no-channel rows produce
     "suppressed" / "NOT delivered", never "delivered"; an alert queued before a 60 h shutdown is followed.
  10. An engine restart produces one "streams fenced" event and no per-stream "lost".
  11. A loss and reconcile between two polls produces "lost and reconciled between polls".
  12. DLQ: lease cycles are housekeeping; re-park, RESOLVED and QUARANTINED are shown; a QUARANTINED row released
      back to QUEUED is still watched and its release is shown.
  13. Terms: draining, then ended; a term closed by its successor reads "closed at the start of term N"; a crashed
      run produces "ended without a clean stop".
  14. Restart backfill emits nothing twice.
  15. CASH_FLOW and INSTRUMENT `log_id`s between ORDER rows never enter `Pending` and emit nothing.
  16. `Expired` at 10 000 evicts the oldest entries first and logs the count.
  17. A watched DLQ row moved to history, and a watched outbox row purged, leave their sets silently.
  18. A new engine run is found by the discovery query (newest 50 by `started_at_ms`); the start-up seed is
      silent.
  19. A new `kind = 'arm'` outbox id is marked seen by the cursor and never enters the alerts watched set; the
      watched query's `created_at_ms` bound is never pinned by it.
  20. `adapter_auth_state` with two generations of one adapter gives one AUTH_FAILED event, and a later failure of
      a new generation gives a new one; no flip-flop between the generations.
- **narration:**
  - every kind from fixture rows;
  - a StoneX bracket stop move reads "stop-loss moved", never "entry changed";
  - a cTrader position stop move;
  - close folding (stop-loss / take-profit / manual) and fill folding;
  - `DROPPED_STALE` does not change the cache;
  - batch sort by `(entity_key, seq_id)`;
  - an open entity at start whose stop moves reads "moved", not "first seen";
  - no-visible-change rows are housekeeping;
  - SYMBOL SIDE prefixes;
  - start-up story membership and break-outs;
  - late marking for history rows;
  - story keys and `story_merge`;
  - alert badge matching by episode (with `activation_id` NULL on the Oversee side and 0 in the episode) and by
    equal `at_ms`;
  - two transitions of one account in one sweep pass: each `transition` alert badges only its own story, and an
    unparsable one is a standalone line;
  - a fenced `stream_health` row of an adapter missing from the last LEADER answer's `xtnl_adapter_state` is
    "inactive", does not hold the
    start-up story open and raises no "still lost" warning;
  - "Engine stopped responding" followed by a new run within 60 s becomes the standalone `info` line "Engine
    restarted (not responding for N s)", in no story;
  - **active set and roles (N1):** a probe answer from a LEADER_INIT, STANDBY or SAFE_HOLD node (no adapter lines)
    leaves the last LEADER answer's active set unchanged; before any LEADER answer every row counts and the band
    shows "role: <role>", never "inactive"; a start-up story does not close while LEADER_INIT fences the streams
    (their `reconciled_epoch` is older) and the probe answers as non-leader, and the still-fenced rows raise no
    "lost" warning when the LEADER lines appear;
  - **closing orders (N3):** a StoneX history closing trade (FILLED, reduce-only, `linked_position_id` = the
    opening order, `leg_kind` NULL) reads "closed (closing trade)" and is never folded as stop-loss or
    take-profit; a filled STOP child still folds as "closed by stop-loss";
  - two `info` transitions of one subject in one pass (`capped` and `lowered`) each badge their own Oversee row;
  - **outage stories:** a stream loss after the stream's first reconcile in the term opens one `out:` story that
    closes on the next reconcile; a drop before the first reconcile writes no new `conn_lost_at` and gives no
    outage; a stream's first row (no fence) that is lost and then reconciled for the first time in the term closes
    its outage with a link, the reconciled line being in the `term:` story; the key is the same after a live-view
    restart;
  - **catch-up join and fold (v8), fixtures in the engine's commit order** (hold losses, then the `catchup_runs`
    row, then the release reconcile, as `src/runtime/catchup.py:398-408` writes them): an orders gap, then a
    `reconnect` hold that loses prices and account, then the row, then the reconcile: the row joins the orders
    outage (the oldest open one), which keeps `warn`; the prices and account outages are folded into it with
    `story_merge {held: true}` and shown at `info` as "held for catch-up"; the same with the row and the reconcile
    in one poll, and with the row and reconcile one and five polls apart, gives the same result; a socket loss
    (all three streams lost by `_lose_all`) joins the oldest and folds the other two; a `retry`, an `operator` and a
    `resume_after_sleep` row with no earlier drop join the oldest hold loss, and every one of their outages,
    the joined one included, is `info`; with two outages of the adapter open the row joins the one with the
    smaller `conn_lost_at` (equal values: `account` before `orders` before `prices`); with none open (and for a
    row found in the backfill) the row joins its term's story by epoch; a hold loss and its release that both fall
    in the row's poll (never seen open) fold as `r{reconciled_at}` outages; an outage closed by the stream's first
    reconcile in its epoch is not folded; a stream reconciled by the release and then lost again opens an ordinary
    `warn` outage that no earlier row folds; every row and every loss and reconcile line appears exactly once
    across all stories in every case;
  - **outage keys (v8):** a loss and reconcile that both fall between two polls (previous `reconciled_epoch`
    equal to the new one) give one closed outage keyed `out:{adapter}:{stream}:r{reconciled_at}`; two such
    outages of one stream give two keys; no story key ever contains `None` (a fixture with a NULL key part makes
    the key builder raise, and in production the event is a standalone line); every `out:` key, both forms, passes
    the `/api/story` key pattern;
  - **DLQ stories:** park, retries, quarantine, release and RESOLVED of one row are one `dlq:` story; a re-parked
    row with `origin_dlq_id` joins its origin's story; the story links to the trade story of its ORDER
    `entity_id`;
  - a halt story links to the story of the active SOFT / HARD episode of its `rule_id`, and to nothing for an
    escalation or operator raise it cannot name;
  - **start-up story keys (v7):** every term opens its own `term:{term_id}` story: a first start, an SM9
    re-campaign (DRAINING → STANDBY → CANDIDATE → LEADER_INIT) in the same run, a single-PC SAFE_HOLD auto-resume
    campaign and a retry after `init_failed`; a start-up story never re-opens and never merges into another; every key
    passes the `/api/story` key pattern, and a `run:` key gets 400;
  - **StoneX pending-entry brackets (v5):** stop and take-profit children linked to a still-pending entry's
    `OrderId` join `ord:{adapter}:{OrderId}`, not a `pos:` story; when the entry is cancelled unfilled that one
    story ends, children included, and nothing stays `open`; when the entry fills, the `story_merge` into
    `pos:{adapter}:{OrderId}` carries the children and later child rows go to `pos:`; a linked order whose parent
    was never seen gets its own story that ends when the order is terminal; a trade story whose orders and
    position are all terminal is `closed`;
  - **fold hold (v5):** a child FILLED and the position CLOSED found in two polls 3 s apart (the second row with
    the lower id, committed later) give one line "closed by stop-loss"; a partner found 12 s after the opener
    gives two lines, the first emitted at the 10 s mark; an entry FILLED and the position OPEN in separate polls
    give one "entry filled → position opened" line; the hold is measured in database time, so an engine clock
    120 s off changes nothing; other lines of the story and the band are not delayed; a sibling cancel after the
    hold is the housekeeping line "take-profit cancelled with the close";
  - **auth state (v6):** an AUTH_FAILED row read after start, then a LEADER answer from a later poll with
    `xtnl_adapter_state{state="STREAMING"}` (and, separately, `SNAPSHOTTING` and `RESYNCING`) for that adapter shows
    "AUTH_FAILED cleared (credentials changed)" and emits one `info` event, once; a LEADER answer with
    `state="AUTHENTICATING"`, `"BACKOFF"` or `"DISCONNECTED"` keeps "AUTH_FAILED (as recorded, since T)" and emits
    nothing; a LEADER answer with `state="AUTH_FAILED"` keeps AUTH_FAILED; before any LEADER answer, with the probe
    off, or with only a same-poll answer, the row shows "as recorded, since T"; a later failure of a new generation
    (new `failed_at_ms`) shows AUTH_FAILED again;
  - **auth restart case (v6):** with an AUTH_FAILED row from weeks ago still the newest row of its adapter and the
    adapter STREAMING, a live-view start followed by LEADER answers in later polls shows "cleared" in the band and
    emits **no** event; a second restart does the same;
  - **auth generation change that fails (v6):** AUTH_FAILED row (event), then LEADER answers showing AUTHENTICATING
    and BACKOFF (no "cleared" event), then a new AUTH_FAILED row for the new generation (a new AUTH_FAILED event);
    and the `known_bad` path: BACKOFF (auth state unreadable), then AUTH_FAILED again with no new row: no "cleared"
    event at any point, the band keeps AUTH_FAILED;
  - **keyed membership (v7):** the term, its fence (`conn_lost_at` = the term's `started_at`), its `startup`
    catch-up rows (`epoch` = `epoch_revision`) and each stream's first reconcile with `reconciled_epoch` =
    `epoch_revision` all land in `term:{term_id}`, whichever poll each is found in and whatever order the fixture
    returns the sources in; a second reconcile of a stream in the same epoch is an outage close, not a member; a
    `retry` row of that epoch read with no outage of its adapter open lands in `term:{term_id}`;
  - **late catch-up row (v7):** a `startup` catch-up row found 5 min after its term's story closed (by the
    120 s end, and separately by the term reaching LEADER with all streams reconciled) joins that story by its
    epoch, marked late, without re-opening it; a `retry` row of that epoch an hour later, read with no outage of
    its adapter open, joins it too; the same row
    with an epoch no known term has, or whose story was dropped, is a standalone line and joins no other story;
  - **crash then restart (v7):** run R1 crashes (its `engine_runs` row stays unstopped), a new process starts and
    its term T2 is found while its run row R2 is not yet written (and, separately, never written, and found three
    polls later): T2's start, LEADER, fence, catch-up and first-reconcile lines are filed at once under
    `term:{T2 term_id}` by T2's epoch, nothing is held, and nothing goes to R1 or to the previous term's story; R1's
    "ended without a clean stop" and R2's start are standalone lines whenever they arrive; a live-view start after
    such an engine start (previous run crashed, run row late) does the same;
  - **old epoch never joins a newer story (v7):** with term T1 (epoch E1) and then term T2 (epoch E2 > E1) known, a
    reconciled event carrying `reconciled_epoch` = E1 (and a catch-up row with `epoch` = E1) found after T2's story
    opened joins T1's story, or stands alone when T1's story is gone, and never T2's; T2's story does not close
    on it;
  - **closing (v7):** a start-up story closes when the term has reached LEADER and every active adapter's rows have
    `conn_lost_at IS NULL` and `reconciled_epoch` = the term's epoch; it closes at 120 s with a stream still
    fenced (that stream raising the 60 s `warn` line); it closes in the poll that reads `end_reason` set with
    `ended_at` still NULL (a drain: `switchover`, and separately `abdicate`), and in the poll that reads both set
    (a SAFE_HOLD abort in step 1, `init_failed`; a successor's close); a term whose process hard-exited
    `init_failed` (no write: `reached_leader_at`, `end_reason` and `ended_at` stay NULL) closes at the 120 s end,
    and, in a separate fixture where the successor's term row arrives first, in the poll that reads the
    successor's term (its close sets `ended_at` and `end_reason = 'init_failed'`); with the probe off a row of an
    adapter the engine no longer runs keeps it open until the 120 s end;
  - **no run-based waiting (v7):** no term or start-up line waits for an engine run row; a run and its first term
    in one poll, a run a poll later and a run never written give the same story contents;
  - **cTrader ids (v6):** a cTrader reduce-only order whose `linked_position_id` equals the `broker_order_id` of an
    unrelated cTrader entry order, with no known position of that id, gets its own story, never `ord:` of that
    entry; a cTrader entry order whose `broker_order_id` equals an existing cTrader `pos:` story's `position_id`
    stays in its own `ord:` story; a cTrader entry FILLED and its POSITION OPEN give two lines in two stories, with
    no fill hold; the same rows with `adapter_id = 'stonex'` (and `'mock'`) still pair and merge; a cTrader stop-loss
    order FILLED and the position CLOSED still fold into "closed by stop-loss".
- **privacy guard:**
  - every DTO field name in `src/dto/entities.py` that is not on the allow-list is rejected, and every allowed
    name is accepted;
  - a DLQ event passes (`dlq_reason`);
  - a decimal in an id, code, operator or symbol slot is replaced by "(not shown)" and the item is kept;
  - a forbidden or unknown name drops the item;
  - `j.smith` passes the `operator` slot; `halt.clear` passes the `command` slot;
  - a level outside the level slots is rejected;
  - state, story, day and series payloads with a forbidden key are rejected;
  - the guard raises under `python -O`;
  - long titles are shortened, not dropped.
- **probe:**
  - only the allow-listed metrics are kept, and other lines are never parsed into state;
  - 3 failures produce "not responding";
  - a pass age above 10 s on a LEADER gives "RiskSweep stalled"; an absent metric does not, nor does a high age
    while an account is quarantined or within 60 s of `reached_leader_at`;
  - each answer records whether it had `xtnl_node_role{role="LEADER"} 1`;
  - `xtnl_engine_uptime_seconds` and every other metric not on the allow-list are dropped unread;
  - a 1 MiB cap;
  - a timeout.
- **day and series:**
  - halt intervals across midnight;
  - stream intervals from each source rule;
  - `not_observed` is never filled in;
  - buckets are marked partial when the backfill hit its limit;
  - reset at midnight.
- **server:**
  - Host 421, including the missing-Host and port-80 forms;
  - a cross-site `Origin` or `Sec-Fetch-Site` gets 403 and uses no slot;
  - 405 on POST, and HEAD on `/api/stream`;
  - headers on 404, 405, 421, 503 and 500;
  - exact static content types;
  - `/api/story` key validation;
  - an event emitted between the snapshot and the stream open is delivered exactly once (`?after=`);
  - a `story_merge` made between the snapshot and the stream open, or during a reconnect, is replayed by id, and a
    snapshot taken after it carries `merged_into`;
  - `/api/story` accepts `out:` and `dlq:` keys;
  - after `hello` and replay the server sends the current `state`, `day` and `series` and the touched story
    headers, so a halt change made during a gap reaches the band;
  - `Last-Event-ID` from another instance or malformed gets resync, never a 500;
  - a slow client gets resync and is aborted within 5 s;
  - a disconnected client frees its slot within 2 s;
  - max clients 503;
  - stdin EOF with 3 open streams exits within 5 s;
  - no access-log line, and an exception message never reaches the log;
  - **edge page (v9):** `GET /edge` returns `edge.html` with `text/html; charset=utf-8` and every header of §4.8
    (the exact CSP, `nosniff`, `X-Frame-Options`, `Referrer-Policy`, `Cache-Control`, CORP, COOP), the same as `GET
    /`; HEAD works; POST, PUT and DELETE on `/edge` get 405 with the same headers; a bad Host gets 421 and a
    cross-site `Origin` 403 on `/edge` too; `GET /static/edge.js` has `text/javascript; charset=utf-8`; serving
    `/edge` runs no SQL and no probe (fake database and probe record zero calls); `ALL_SQL` and `ALL_VIEWS` are
    unchanged and `src/liveview/settings.py` reads no new variable.
- **page:**
  - no `innerHTML`, `outerHTML`, `insertAdjacentHTML` or `document.write` in `app.js`;
  - `app.js` calls `close()` on the `EventSource` in its `resync` handler and in its `onerror` handler, and opens
    streams only after a snapshot fetch;
  - no inline script or style in `index.html`;
  - the glossary covers every reason code the Oversee rules emit (`src/oversee`);
  - `app.css` defines the XTNL theme tokens on `:root` with the light variant behind the toggle, and loads no web
    font; every `localStorage` access in `app.js` is inside `try`;
  - **text hierarchy (v9):** `app.css` defines `.t1`, `.t2`, `.t2h`, `.t3`, `.t4`, `.t5` and `.hero` with the
    fonts, sizes, weights, letter-spacing and ink tokens of §1.7 (`.t1` 19px inside the `max-width: 640px` query);
    no `font-size` in `app.css` is below 11px or given in a unit other than `px`;
  - **edge page (v9):** `index.html` and `edge.html` both carry the nav links `href="/"` and `href="/edge"`, and
    neither contains `#/`; `edge.html` contains no `<form`, `<input`, `<select` or `<textarea`, no inline script or
    style, and every `<button` in it carries `disabled` and `aria-disabled="true"`; `edge.js` contains exactly one
    `fetch(` call, whose argument is the literal `'/api/snapshot'`, passes no `method` or `body`, and contains no
    `EventSource`, `WebSocket`, `XMLHttpRequest`, `sendBeacon`, `innerHTML`, `outerHTML`, `insertAdjacentHTML`,
    `document.write`, `addEventListener('click'` / `onclick`, or the string `ALLOW`; every `localStorage` access in
    `edge.js` is inside `try`;
  - **forbidden names in the static files (v9):** neither `edge.html` nor `edge.js` (nor `index.html`, `app.js`)
    contains, case-insensitively, any name `pre_tool_guard.py` blocks: `place_order`, `modify_order`,
    `cancel_order`, `submit_order`, `close_position`, `amend_order`, `update_order`, `newtradeorder`,
    `newstoplimitorder`, `updatetradeorder`, `updatestoplimitorder`, `ProtoOANewOrderReq`, `ProtoOAAmendOrderReq`,
    `ProtoOAAmendPositionSLTPReq`, `ProtoOAClosePositionReq`, `ProtoOACancelOrderReq`. The hook scans only `.py`
    files, so this test is what covers them; a parity check asserts each listed name appears in the hook's
    `FORBIDDEN_PATTERNS`, so the list cannot fall behind the hook. A second check runs each `FORBIDDEN_PATTERNS`
    regex over every `.py` file in `src/liveview` and finds nothing.

## 7. Out of scope

- ARM notices: `lv_alerts` holds `kind = 'arm'` rows only as id placeholders (§3.3), and the narrator skips them.
  A later migration exposes them under a reserved `arm` category when the ARM is turned on. The live view cannot read `XTNL_ARM_ENABLED`, because `XTNL_ARM_*` is on its refuse
  list.
- Remote viewing and logins.
- Writing anything.
- Any edge, signal, execution-intent, risk-budget or automation function: `/edge` is an unconnected shell (§1.8),
  replaced in Phase 2 behind `ActionGate.allow(action)` and operator approval.
- Persisting the live view's own observations across restarts.

## 8. Open owner decisions

Until the owner answers, the build uses the option marked **(build default)**.

**8.1 Execution price of a filled closing order** (privacy finding 6). A StoneX closing trade or history row sets
`entry_price` from `Price` / `ExecutionPrice` (`src/adapters/stonex/history.py:191-195`,
`src/adapters/stonex/mapping.py:264`). Shown next to the position's entry and stop, it lets the user work out the
R in favour, which `operations.md` §9 keeps out of notifications.
- **A (build default):** `entry_level` is NULL for FILLED reduce-only orders, for reduce-only orders with no
  stop / limit kind at any status (closing orders, whose price can be a fill price; v4, §3.3), and for any FILLED
  order with no stop / limit kind (a closing trade read without its closing evidence; build note §3.3). The close reads
  "closed by stop-loss", naming the stop *level* last seen while it was working, or "closed (closing trade)".
- **B:** show the fill price.
- **C:** show the fill price only for stream rows of a child stop or limit (its trigger level), and hide it for
  closing trades and history rows.

*Recommendation: A.* It matches the "levels, not fills" decision, and the close line still says which level closed
the position.

**8.2 Direction of a size change** (usefulness finding 5). Today a StoneX partial close, or a scale-in, keeps the
position OPEN with the same levels, so the page shows it only as a housekeeping "no visible change" line.
- **A (build default):** show nothing beyond that.
- **B:** add one derived fact, `size_change` ∈ {`up`, `down`}, computed in the view by comparing `payload->>'units'`
  (or `remaining_units`) with the previous `APPLIED` row of the same `entity_key` (a `LIMIT 1` correlated subquery
  inside the LATERAL). It is narrated as "partly closed" / "added to". No size or ratio is exposed, and a test
  asserts that `size_change` is the only units-derived key.

*Recommendation: B.* A partial close is an action the owner would want to see, and a direction is not a size. It
needs the owner's yes because it is derived from units.

**8.3 Ladder geometry.** The ladder places entry, stop and take-profit on a line.
- **A:** draw it to scale. The stop-to-target ratio is then visible as a shape (it can already be worked out from
  the three levels shown).
- **B:** draw evenly spaced points in level order.

Neither option shows pips, distances or a current price. *Recommendation: A.* It shows at a glance a stop that sits
oddly close or far, and it reveals nothing the printed levels do not.

## 9. Changes

### 9.1 v2: after the five-lens review

Finding ids: P = privacy, T = tailing, W = web security, R = repo fit, U = usefulness. Every accepted finding was
checked against the code it cites.

**Accepted:**

| Finding | Change |
|---|---|
| P1 (major) operator free-text reasons | `lv_operator_actions` has no `reason` / `detail`. `lv_halt_events.reason_code` only for `"by" = 'system'` plus the CODE guard. The page says "reason recorded in operator_audit". Role test (§3.5). Checked: `halt.py:216`; `0007_operator_audit.sql:26` only checks `length >= 8` |
| P2 (major) guard matching undefined | Closed allow-list per kind and payload, plus a whole-token deny check; `dlq_reason`; a DTO field-name test (§4.4.5) |
| P3 (major) values unchecked | Typed slots; titles only from templates; no decimal outside the level slots; no free-text slot (§4.4.5) |
| P4 (minor) alert subjects | The subject is never exposed. `info_class` comes from fixed patterns in the view, and badges come from episode and time matching. Titles are shortened, never dropped (§3.3, §4.4.5-6). See "partly rejected" for `source_event` |
| P5 (minor) state payload unguarded | `assert_safe` runs on state, story, day and series rows (§4.4.5) |
| P6 (minor) closing-fill price | Owner decision 8.1; build default A in the `facts` CASE (§3.3) |
| P7 (minor) no `security_barrier` | `ALTER VIEW … SET (security_barrier = true)` on every view (by ALTER, to keep R1's scan); `facts` by `CASE entity_type`; `reloptions` test |
| P8 (minor) role check too weak, import breaks §2 | `src/db/readonly_role.py` plus `verify_view_only_role` (§4.2); the import boundary no longer names `src.dashboard`; `sys.modules` test |
| P9 (minor) error logging | `safe_db_error` only; `access_log=None`; catch-all middleware; test (§4.2, §4.9) |
| T1 (blocker) `LIMIT 500` window starvation, mixed cursor | Keyset cursor on the database-issued id with paging and a page cap (§4.3) |
| T2 (major) engine-clock columns used as cursors | Clocks used only for 24 h / 7 d prune bounds; proof sketch (§4.3) |
| T3 (major) outage longer than the look-back | Cursor tail, so outage length is irrelevant; back-off is 1-10 s anyway |
| T4 (major) "Alert sent" missed and mislabelled | Watched set until `sent_at_ms`; `delivery` derived label; 7-day seed for pending rows (§3.3, §4.3) |
| T5 (major) empty backfill replays old rows | Cursor starts at `max(k)`; backfill never moves a cursor |
| T6 (major) seen-set TTL re-emits | No TTL. Contiguous floor plus `Seen` above it; per-key value maps for re-read sources |
| T7 (major) LEADER_INIT fence alarms | One "streams fenced" info event (`conn_lost_at = term.started_at`); diffs on values; "lost and reconciled between polls" |
| T8 (major) mixed clocks in ordering | Arrival order; `clock` tag; late marker (§4.4) |
| T9 (major) stale rows, row order, cache misses | Only APPLIED rows update the cache; batch sort; seeding from the open views; "first seen" rule (§4.4.1) |
| T10 (minor) DLQ keys | Watched set keyed `(status, retry_count)`; LEASED and the cycles are housekeeping |
| T11 (minor) terms and crashed runs | Draining / ended / "closed at the start of term N"; unclean-run warning |
| W1 (major) snapshot-to-stream gap | `?instance=&after=`, with the header winning; test |
| W2 (major) integer id cannot detect another instance | `<instance>.<n>` ids; strict regex; `hello` first |
| W3 (major) aiohttp defaults | `handler_cancellation=True`, `shutdown_timeout=2`, write timeouts with `abort()`, sentinel shutdown, page-driven reconnect; tests |
| W4 (major) dashboard import pulls in redis / cache | Same fix as P8 |
| W5 (minor) misconfigured DSN looks like "db down" | `verify_view_only_role` gives exit 78 |
| W6 (minor) cross-site slot exhaustion | Origin / `Sec-Fetch-Site` 403 before counting a slot; CORP and COOP headers |
| W7 (minor) Host list versus bind address | Literal-only bind; Host set derived from the bound literal; port-80 forms; missing Host 421; status probes the configured address |
| W8 (minor) HEAD, huge ids, headers on errors | `allow_head=False`; regex parse; `on_response_prepare`; catch-all 500 |
| W9 (minor) access log | `access_log=None`; type-only exception logging |
| W10 (minor) Windows MIME types | In-memory bytes with fixed content types |
| W11 (minor) 6-connection browser limit | `MAX_CLIENTS` default 4; "too many tabs" hint (BroadcastChannel: see "partly rejected") |
| W12 (minor) DLQ field drops events | `dlq_reason`; whole-token matching; `raise`, not `assert` |
| R1 (blocker) schema scan fails | View-writing rules (§3.2), the 115-name allow-list group (§3.4), level aliases, LATERAL for computed values, no `WITH (` |
| R2 (major) `broker_time_ms` not renamed | `broker_time_ms AS broker_ms`; `facts` read whole |
| R3 (major) `src/api/settings.py` | `FORBIDDEN_NAMES` gains `XTNL_LIVEVIEW_DSN` (name only) |
| R4 (major) exact-equality deploy pins | Every edit named in §5 and §6; step-8-only note |
| R5 (major) burst over `LIMIT 500` | Same fix as T1; 1 200-row test |
| R6 (major) re-emission | Same fix as T6; 10-minute no-duplicate test |
| R7 (minor) runbook index | `docs/runbooks/README.md` and `RUNBOOK_FILES` |
| R8 (minor) credentials script tests | Own test file; not in `SCRIPTS`; restart after `-Rotate` |
| R9 (minor) role check location, relkind, mypy | `src/db/readonly_role.py` with `relkind IN ('r','p','v','m')`; asyncpg through `importlib` |
| R10 (minor) guard drops DLQ events | Same fix as W12 |
| R11 (minor) env scanner and refuse-list | `os.environ` read only in `settings.py`, which has no status literals; `XTNL_HEARTBEAT_COPY_TOKEN` added; pattern for `XTNL_*` |
| R12 (minor) carry-over and role settings | Role block first, `-TotalCount N` pinned; view-pinning note in the header and in operations §12 |
| R13 (minor) packaging and test basenames | `Path(__file__)` loading; `tests/liveview/__init__.py`; hook-name rule |
| U1 (major) StoneX brackets | Bracket model (§4.4.2), used by the ladder, headers and narration |
| U2 (major) engine down looks healthy | Engine probe (§4.6); banners (§1.1) |
| U3 (major) alert delivery misreported | `delivery` / `escalated`; band item; `warn` lines for retrying and NOT delivered |
| U4 (major) one incident becomes about 12 lines | Stories and episodes; alerts as badges; re-page counter (§4.4.3, §4.4.6) |
| U5 (major) no-change rows and partial closes | No-visible-change rows become housekeeping. The `size_change` direction is owner decision 8.2 |
| U6 (major) start-up burst flagged as alarms | Start-up group with downgrade and break-out rules (§4.4.7) |
| U7 (major) history rows sort out of sight | Arrival order, late marker, nesting under the start-up group |
| U8 (major) "why SOFT", HARD estimate | New columns in `lv_breaches` plus `acked`; `lv_ruleset_timing`, `lv_sweep_quarantine`, `lv_auth_state`; estimate and cool-down (§4.5); glossary |
| U9 (minor) LRU misses old positions | Seeding from the open views |
| U10 (minor) titles lead with ids | SYMBOL SIDE prefix; fill and close folding |
| U11 (minor) DLQ and stream blips | Same fixes as T10 and T7 |
| U12 (minor) database-down meaning | Banner texts that tell engine, database and both apart |
| U13 (minor) ARM rows, open-breach predicate | ARM rows left out of `lv_alerts` (see below); open-breach predicate fixed (§4.5) |

**Rejected or partly rejected (with evidence):**
- **P4, part.** "Add `source_event` to the view" is rejected. The outbox INSERT never sets it
  (`src/db/notify_store.py` `SQL_OUTBOX_INSERT` column list), so it is always NULL. Matching uses episode keys and
  equal `at_ms` instead.

  "A repo test that pins outbox titles to a safe pattern" is replaced by something stronger: the subject never
  leaves the database. A test over every `page()` caller would be brittle (titles are f-strings across many
  modules).
- **T3, part.** "Cap the back-off at LOOKBACK/3" is moot: there is no look-back window any more. The back-off cap
  of 10 s is kept only for faster recovery.
- **T1, second scenario (WSL clock lag starving the window).** Moot under the cursor tail: no correctness predicate
  uses a clock. The behaviour is still covered by tailer test 3.
- **W11, part.** BroadcastChannel stream sharing is rejected as unneeded complexity for a single-user PC. With a
  default of 4 streams, the browser's 6-connection limit leaves 2 for page and snapshot fetches, and the page
  explains a stalled snapshot.
- **R11, part.** "Refuse any name matching `(PASS|SECRET|TOKEN|DSN)`" is narrowed to `XTNL_*` names. The child
  inherits the full Windows User environment (`Start-Pumped` only removes `-DropEnv` patterns), which can hold
  unrelated tool tokens. Refusing those would stop the live view for reasons unrelated to the engine.
- **U4, part.** "Match a trip alert to the nearest halt row within 5 s" is replaced by exact equality.
  `halt.py:160-161` writes the halt row and its alert with the same `now_ms`, so no window is needed.
- **U13, part.** "Map `kind = 'arm'` to a category shown when `XTNL_ARM_ENABLED`" is changed: the live view must
  not see `XTNL_ARM_*` (§4.1 refuse-list), so ARM rows are left out of `lv_alerts` until a later migration adds
  them.
- **U5, part.** The `size_change` key is not built until the owner answers 8.2, because it is derived from units.

### 9.2 v3: after the independent check of v2

An independent check of v2 confirmed that all 37 blocker and major review findings were addressed, and listed 17
problems that remained. Each was checked against the code it cites before it was changed. Sections 8.1-8.3 and the
settled owner decisions are unchanged; the build still uses option A for each open decision until the owner
answers.

| # | Problem | Verified against | Change in v3 |
|---|---|---|---|
| 1 | CODE pattern has no comma | `src/oversee/rules.py:317` writes `daily_loss(<a>,<b>)` | CODE gains `,` plus a `[0-9][.,][0-9]` decimal test (§3.2, §4.4.5); §6 test names the composite code |
| 2 | Operator commands are dotted | `0007_operator_audit.sql:20`; `src/ops/` commands such as `halt.clear`, `arm.config.accept` | New COMMAND guard for `lv_operator_actions.command` and a `command` slot type (§3.2, §3.3, §4.4.5); §6 test |
| 3 | Operator names can contain `.` | `0007_operator_audit.sql:22-23` CHECK allows `.` | New `operator` slot type for `operator`, `approver`, `by_whom`, `resumed_by` (§4.4.5) |
| 4 | `hold_code` is a phrase with a count | `src/db/leader.py:95-99` | `hold_code` mapped to `already_set` / `abnormal_term_ends` / `epoch_changes`, plus `hold_count`; title from a template (§3.3). Uses `regexp_match`, not `substring(… FROM …)`, so no extra `FROM` appears in a view body |
| 5 | `UNKNOWN:<id>` symbols rejected; whole items dropped | `stonex/instruments.py:115`, `pepperstone/instruments.py:39` | `symbol` = `^[A-Za-z0-9._/:#-]{1,40}$` with the decimal test. A value failing its slot type becomes "(not shown)" and the item is kept; only a forbidden or unknown name drops an item (§4.4.5) |
| 6 | Native SSE reconnect loops; id-less events lost in gaps | HTML EventSource reconnects after a normal close in state CONNECTING with the old `Last-Event-ID` | `app.js` closes the stream on `resync` and every `onerror` and always reconnects through a snapshot; the server sends current `state`, `day`, `series` and touched story headers after `hello` and replay (§4.8); tests |
| 7 | A refused name in a tuple breaks a deploy test | `tests/deploy/test_deploy_assets.py:186` reads tuples; `XTNL_DASHBOARD_DSN` is only in `dashboard.env.example` | Refused names in a `frozenset({...})`, as `src/api/settings.py:54`; prefixes end in `_` and fail `ENV_NAME` (§4.1) |
| 8 | Drop-list wildcards would strip the live view's DSN | `start-xtnl.ps1:201-204` removes every `-like` match | `$LiveViewDropEnv` lists explicit prefixes and names only; the generic rule is only in `settings.py`; a deploy test asserts `XTNL_LIVEVIEW_DSN` matches no entry (§5, §6) |
| 9 | Filtered entity types leave permanent id gaps | `src/db/repository.py:279,294` log CASH_FLOW and INSTRUMENT into `event_log` | Both types stay in `lv_entity_events` with `entity_key`, `account_id`, `broker_ms` and `facts` NULL; the narrator skips them. `Expired` eviction is oldest-first with a count logged (§3.3, §4.3). **Extended:** `lv_alerts` had the same gap for `kind = 'arm'`, so ARM rows are now id placeholders too (§3.3, §7) |
| 10 | Transition badges land on unrelated stories | `src/oversee/notify.py:159-170` subject format | `tm` regex in the LATERAL exposes `tr_rule_id`, `tr_subject_type`, `tr_subject_id`; `info_class = 'transition'` only on a full match; badge on the one matching Oversee row, else a standalone line (§3.3, §4.4.6). **Corrected:** the check's regex used upper-case subject types, but the subject carries the Python form (`position`, `account_day`; `src/oversee/types.py` `SubjectType`), so the regex matches lower case and the view applies `upper()`. While verifying this, the same mismatch was found in v2's `ep_subject_type`, which now also uses `upper(episode->>1)` |
| 11 | DLQ "final when QUARANTINED" is wrong | `src/ops/sql.py:80-83` `SQL_DLQ_UNQUARANTINE` | Only "RESOLVED for 10 min" is final; QUARANTINED rows are seeded and kept in the watched set (§4.3) |
| 12 | Archived watched ids stay forever | `src/db/lifecycle.py:120-131` history move; outbox purge | A watched id the query no longer returns is dropped silently (§4.3) |
| 13 | Old adapters stay "lost" forever | `src/db/leader.py:28-30` fences every row; `xtnl_adapter_state` is exported per configured adapter (`src/runtime/session.py:2455-2456`) | Only adapters in `xtnl_adapter_state` close the start-up group or warn; others are "inactive" in the band and day panel; with the probe off or down every row counts (§1.1, §1.2, §4.4.7, §4.7) |
| 14 | No query discovers new engine runs | §4.3 v2 named none | Discovery query `ORDER BY started_at_ms DESC LIMIT 50`, diffed by `run_id`, at start (silent seed) and every poll (§4.3) |
| 15 | Probe "runs on the exporter's own thread" | **Premise does not hold:** the `src/observability/exporter.py` docstring says "on the event loop (asyncio streams, no thread)", so a wedged loop already shows as "not responding" | The proposed check is still added, for a different case: a live loop with a stuck RiskSweep. On a LEADER, pass age above 10 s shows "RiskSweep stalled" (`warn`); an absent metric (STANDBY, SAFE_HOLD) is not a stall (§4.6, §4.4.8) |
| 16 | Episode matching on NULL `activation_id` | `src/oversee/notify.py:153` `activation_id or 0` | Matching and the event's `episode` string use `coalesce(activation_id, 0)` (§3.3, §4.4.6) |
| 17a | "bare column" wording | `tests/db/test_arm_schema_no_market_data.py:110` splits on whitespace | "unqualified column, or `alias.col AS name`"; `e.log_id` without `AS` is called out (§3.2) |
| 17b | `lv_ruleset_timing` row choice | `src/db/ruleset_store.py:20` `max(ruleset_version) WHERE active` | StateReader reads `ORDER BY ruleset_version DESC LIMIT 1` (§3.3, §4.5) |
| 17c | `GRANT CONNECT` outside the role block | `0011_dashboard_role.sql:24` | Moved into the role `DO` block with `format(… current_database())`, so the carry-over pipe carries it (§3.1) |
| 17d | Operator labels not stated as shown | §1.6 | §1.6 says `operator`, `approver`, `by_whom` and `resumed_by` are shown |
| 17e | Planned restarts page as alerts | §4.6 v2 emitted the alert at once | "Engine stopped responding" is held 60 s and folded into the next start-up group at `info` when a new run appears; banner 4 is not held (§4.4.7, §4.6, §4.4.8). A hold was chosen over re-labelling an alert already sent to the page |

The schema-scan allow-list (§3.4) grows from 115 to 119 names: `hold_count`, `tr_rule_id`, `tr_subject_id` and
`tr_subject_type`. None is on today's `ALLOW_LIST` or contains a market-data word.

### 9.3 v4: after the check of v3

The check of v3 confirmed problems 1-12, 14-16 and 17a-e fixed, found problem 13's fix wrong (N1), and listed
N1-N12. Each was checked against the code it cites before it was changed; all twelve hold. Sections 8.1-8.3 and the
settled owner decisions are unchanged (8.1-A now names the kind-less closing orders it also covers); the build
still uses option A for each open decision until the owner answers.

| # | Problem | Verified against | Change in v4 |
|---|---|---|---|
| N1 | Adapter active set wrong when the node is not LEADER | `src/runtime/telemetry.py:400-401, 426` passes `leader=is_leader()`; `src/runtime/session.py:2449` returns before the `xtnl_adapter_state` lines when not leader | Active set taken only from answers with `xtnl_node_role{role="LEADER"} 1` and kept across non-leader answers; before any LEADER answer every row counts and the band shows "role: <role>", never "inactive"; a start-up group never closes before the first LEADER answer and `reached_leader_at` (120 s cap kept); `inactive` day segments start only from a LEADER answer (§1.1, §4.4.7, §4.6, §4.7); tests |
| N2 | Execution price can leak | `src/adapters/pepperstone/mapping.py:157-167` (`executionPrice` fallback), `:205` (`closingOrder` is reduce-only), `:170-180` (MARKET has no kind) | Closing-order rule in `facts` **and** `lv_open_orders`: NULL when reduce-only and (FILLED or no kind). The check's fix is kept as written, because a status list would still leak a part-filled-then-CANCELLED market close. Cost: a StoneX child whose type was never seen shows "level not shown" (§3.3, 8.1-A); `pg` tests |
| N3 | StoneX closing trades look like bracket children | `src/adapters/stonex/history.py:184-199` (FILLED, reduce-only, `linked_position_id`, `order_kind` NULL) | A reduce-only order with NULL `leg_kind` is a closing order, never a child: "closed (closing trade)". Children need a non-NULL `leg_kind`; v3's loss-side rule is removed (under 8.1-A such a row has no level). Close folding as stop-loss / take-profit only for STOP / LIMIT children; a filled StoneX stop child keeps STOP through the orchestrator's inheritance (`history.py:186-187`) (§4.4.2); tests. Simplified from the check's "first seen FILLED or `history`" test: the kind alone decides, which also covers cTrader closing MARKET orders |
| N4 | A login failure shows as "Database unreachable" | §4.2 v3 treated every connection failure as temporary | SQLSTATE class `28` and `3D000` exit 78 naming `XTNL_LIVEVIEW_DSN`, at start and on reconnect; only socket / connect failures, `57P03` and `53300` are "db: down" (§4.2); tests |
| N5 | Env-scanner trap outside `settings.py` | `tests/deploy/test_deploy_assets.py:157-189` scans any module containing `environ` / `getenv` and reads tuples, subscripts and `.get()` arguments matching `ENV_NAME` (`:29`) | Every status / kind / type set is a `frozenset`; no `src/liveview` module other than `settings.py` contains `environ` / `getenv` (the word "environment" contains it) or a `*_VAR` / `*_ENV` constant; tests (§4, §6) |
| N6 | Role-check move breaks existing tests | `tests/dashboard/test_dashboard.py:186-194`; `tests/api/test_export_memory.py:11,54` | The dashboard keeps `SQL_ROLE_CHECK`, `DashboardRoleError` and a one-argument `verify_read_only_connection(conn)` wrapper; `pinescript` keeps `SQL_ROLE_CHECK` and `role_problems`, both bound to the shared constant (§4.2); tests |
| N7 | `lv_auth_state` has several rows per adapter | `0001_init.sql:521-531` primary key `(adapter_id, credential_generation)` | The view keeps the latest row per adapter (`NOT EXISTS` on a later `(failed_at_ms, credential_generation)`; the generation breaks a tie); the diff is keyed on `adapter_id` and tracks `(state, failed_at_ms, rearmed_at_ms)` (§3.3, §4.3); tests |
| N8 | Auto-resume actor hidden | `src/runtime/single_pc.py:23` `resumed_by = system:single-pc-auto-resume` | `resumed_by` maps that fixed value to `auto-resume`, else the OPERATOR guard with the decimal test (§3.3); test |
| N9 | `story_merge` lost in a gap | §4.8 v3 sent it without an id | `story_merge` is a numbered ring entry (`<instance>.<n>`) and is replayed; story headers carry `merged_into`, so a later snapshot is right too (§4.4.3, §4.8); test |
| N10 | ARM placeholders never leave the alerts watched set | §3.3 v3: their `sent_at_ms` is NULL | Rows with `kind = 'arm'` never enter the watched set (§4.3); test |
| N11 | Two transitions of one subject in one pass | `src/oversee/states.py:59, 82, 116` emit several transition words; `oversee_events` unique key includes `transition` (`0001_init.sql:387`) | `tr_transition = tm[1]` exposed and matched against `oversee_events.transition`; the allow-list group grows to 120 names (§3.3, §3.4, §3.5, §4.4.6); tests |
| N12 | Nits | as cited | `HOLD_MS` is 47 s at defaults (§4.3). `lv_breaches.reason_code` is named as the CODE guard on `reason` (§3.3). The hook note lists every banned name, path and cTrader message of `pre_tool_guard.py:19-26` (§4). 0021 adds `event_log_log_id_idx` after the role block, like 0017's `event_log_broker_time_idx`, applied in step 6 before the engine starts (§3.1, §3.5) |

Also fixed from the check's table notes:
- the `by_whom` CASE now has the decimal test (problem 3 note);
- the `symbol` slot accepts `UNKNOWN:?` (problem 5 note; `stonex/instruments.py:117`, `pepperstone/instruments.py:41`);
- outbox rows leave the watched set through partition detach, not the `outbox_events` purge at `lifecycle.py:109`
  (problem 12 note);
- the RiskSweep stall is not judged while an account is quarantined or in the first 60 s after `reached_leader_at`,
  because the pass age is the oldest account's (problem 15 note).
- Problem 4's note (`already_set` never fires, because `leader.py` skips the write when the hold is already set) is
  harmless and left as is: the mapping stays for completeness.

**Alignment with prototype D.** Section 1.3 now lists prototype D's six story kinds (trade, account, halt, outage,
engine, DLQ; its `KIND` table and `buildStories`). v3 had no outage or DLQ story: stream losses outside a start-up
group and DLQ rows were standalone lines. They are now stories with keys `out:{adapter}:{stream}:{conn_lost_at}`
and `dlq:{root dlq_id}` (§4.4.3), using the prototype's rules: an outage runs from lost to reconciled, plus a
reconnect catch-up within 60 s; a DLQ story runs from parked to resolved and links to the trade story of its order.
Two deliberate differences from the prototype: the DLQ story follows lineage (`origin_dlq_id`) and stays open while
QUARANTINED, because a release moves the row back to QUEUED; and the halt story's cause link is limited to active
episodes of the same `rule_id`, all of them linked, where the prototype picks one open episode of that rule
(preferring one that started at the same time). Story headers gain
`related`, and `/api/story` accepts the two new prefixes (§4.8).

**Theme.** Section 1 names the XTNL theme (`docs/design/live-view-prototypes/xtnl-theme.md`): its tokens, dark
default with a light toggle, no web fonts (the CSP allows none), flat square styling and a status-only ticker; §6
adds a page test.

### 9.4 v5: after the round-1 check of v4

The round-1 check of v4 confirmed N1-N11 fixed and N12 partly wrong (its index fix), and listed five new problems.
Each was checked against the code it cites before it was changed; all five hold. Sections 8.1-8.3 and the settled
owner decisions are unchanged; the build still uses option A for each open decision until the owner answers.

| # | Problem | Verified against | Change in v5 |
|---|---|---|---|
| 1 (and N12's index part) | The `event_log` index breaks the partitioned-index rule; §3.1 misdescribed 0017 | `0017_arm.sql:393-394` uses `ON ONLY event_log (...)`; `src/db/migrate.py:182` `PartitionIndex(17, "event_log", …)` in `POST_STEP` builds it per partition with `CREATE INDEX CONCURRENTLY` plus `ATTACH`; `operations.md` §12 requires this for every new index on a partitioned table; `tests/db/test_migrate_post_step.py:164` pins `POST_STEP` exactly | 0021 uses `CREATE INDEX IF NOT EXISTS event_log_log_id_idx ON ONLY event_log (log_id)`; `POST_STEP` gains `PartitionIndex(21, "event_log", "event_log_log_id_idx", "log_id_idx", "log_id")`; the false "0017 does it the same way" sentence is replaced by the real mechanism (§3.1); §3.5 pins the `ON ONLY` form and the `POST_STEP` item; §6 updates `test_registered_items` and adds a per-partition build test |
| 2 | Start-up group key `run:{run_id}` collides | `leader_terms` has no `run_id` (`0001_init.sql:68-78`); `src/runtime/single_pc.py` docstring: after the auto-resume "the node then campaigns normally" in the same process; SM9 re-campaigns and `init_failed` retries keep the process | Groups are keyed per term: `run:{run_id}:start` for a new run (its first term joins it), `run:{run_id}:{term_id}` for every later term, which also closes any open group of the run; `run:unknown:{term_id}` when no run matches the term's node; the `/api/story` pattern already accepts these (§1.3, §4.4.3, §4.4.7); tests |
| 3 | StoneX pending-entry brackets split into two stories, one never ending | `.claude/rules/adapter-isolation.md` §2 ("`linked_position_id` = the parent `OrderId` (which may still be a pending entry)"; each child "emitted once, after its parent"); `src/adapters/stonex/orders.py:7-8` | A linked ORDER joins `pos:` only when a position with that id is known, else `ord:{adapter}:{linked}` when that entry order is known, else its own story ending with the order; the merge carries the children; a trade story whose orders and position are all terminal is `closed`, so none can stay `open` (§1.3, §4.4.3); tests |
| 4 | Fill and close folding cannot be built as written | Activity events are immutable ring entries with no retract (§4.8); commit order is not id order by up to `MAX_TXN_MS` = 15 s (`src/integrity/worker.py:58`) | The hold option is taken (a fold within one poll batch would miss most pairs, whose rows are committed separately): fold openers are held up to 10 s of database time for their due partners, then emitted folded or as they stand; only those lines wait; the band and Open now do not; §2 states the latency (§2, §4.4.2); tests |
| 5 | `lv_auth_state` keeps AUTH_FAILED after a working credential change | `src/db/auth_state.py`: rows are written only by `SQL_AUTH_FAILED` and `SQL_AUTH_REARM`; the engine exports `xtnl_adapter_state` (`src/runtime/session.py:2456`) only as LEADER; SM4 enters AUTH_FAILED before writing the row (`session.py:1966`) | AUTH_FAILED is shown only before a LEADER answer from a poll after the row was first read (or with the probe off: "as recorded"), or while the LEADER answer says `state="AUTH_FAILED"`; otherwise "AUTH_FAILED cleared (credentials changed)" with one `info` event per `(adapter_id, failed_at_ms)`. Same-poll answers are not used, because they can predate the failure (§1.1, §4.3, §4.4.4, §4.5); tests |

**Section 1 against prototype D and the theme.** Re-checked: the six story kinds of §1.3 are prototype D's `KIND`
table (trade, account, halt, outage, engine, DLQ), and the outage and DLQ episodes are stories, as in its
`buildStories`, with the deliberate differences already listed in 9.3. v5 widens two rows to match the fixes above:
the trade story names a pending entry's bracket children and the all-terminal end, and the engine start story
covers a later term of the same process. Section 1 now also says that the page uses the XTNL theme for its look
and that, where prototype D's own styling (per-category palette, light default, rounded cards, shadows) differs,
the theme wins while prototype D keeps the layout and behaviour.

### 9.5 v6: after the round-2 check of v5

The round-2 check of v5 confirmed N1-N11, N12's index part and new problems 2-4 fixed, found problem 5 (stale
AUTH_FAILED) partly wrong, and listed three new problems, A-C. Each was checked against the code it cites before it
was changed; all three hold (C as stated, including "all cTrader rows"). For B the check's own fix ("hold until a
newer run appears, else fall back") would also hold every same-process term (SM9 re-campaign, SAFE_HOLD
auto-resume, `init_failed` retry) for 120 s and then file it under `run:unknown`, because those terms never get a
new run; v6 adds a probe-based proof of "same process" so they are filed at once. Sections 8.1-8.3 and the settled
owner decisions are unchanged; the build still uses option A for each open decision until the owner answers.

| # | Problem | Verified against | Change in v6 |
|---|---|---|---|
| A (and v5's 5) | False "AUTH_FAILED cleared" events: re-emitted after every live-view restart for an old fixed failure, and emitted during AUTHENTICATING or BACKOFF, where the `known_bad` path can return to AUTH_FAILED without a new row | `src/db/auth_state.py` writes only on failure and re-arm; `src/pubsub/orchestrator.py:2427-2453` (AUTHENTICATING with new credentials; `:2435-2438` BACKOFF when the auth state is unreadable; `:2440-2445` `known_bad` → AUTH_FAILED, no row); `src/pubsub/connection.py:90-93, 132-135, 140-155` (SNAPSHOTTING only after `on_authenticated`, STREAMING only from SNAPSHOTTING, RESYNCING only from STREAMING) | Only a LEADER answer from a later poll showing `SNAPSHOTTING`, `STREAMING` or `RESYNCING` counts as evidence; `DISCONNECTED`, `AUTHENTICATING`, `BACKOFF` are no evidence. The event fires only for a failure this process saw arrive; a row present at start changes the band silently (first reading silent, §4.3). Without evidence the band shows "AUTH_FAILED (as recorded, since T)" (§1.1, §4.3, §4.5); tests for the restart case, a failing generation change and the `known_bad` path (§6) |
| B | Run and term order in one poll unspecified; a term found before its run row (or after a crash, with the dead run still unstopped) splits one start into two groups or files it under the old run | `src/runtime/uptime.py:13, 199-205` (start row from the first 5 s tick, retried each tick); `src/db/uptime_store.py:36-37` (`stopped_at_ms` set only on a clean stop); `src/main.py:148-150` and `src/runtime/uptime.py:149` (`xtnl_engine_uptime_seconds` exported in any role by the tracker that writes the run row) | Runs are narrated before terms in a poll (§4.3). A term joins an open, term-less `run:R:start`; else it opens `run:R:{term_id}` at once only when every probe answer since R's newest group implies a process start no later than it (same process, proven); else it is held, with its start-up lines, until a new run of its node appears (then it joins that run's start group), or for at most 120 s, then filed under the newest unstopped run or `run:unknown:{term_id}`. The probe allow-list gains `xtnl_engine_uptime_seconds` (an age; §4.6). §2 lists the new wait (§2, §4.4.7); tests for run and term in one poll, run a poll later, crash and same-process terms (§6) |
| C | The id-equality story rules (`pos:` by entry `broker_order_id`, `ord:{adapter}:L` by linked id) assume position id = entry order id, true only on StoneX; on cTrader they misfile, and the merge never fires | `src/adapters/stonex/mapping.py:183-195` (`position_id` = `opened_by_client_order_id` = `OrderId`); `src/adapters/pepperstone/mapping.py:188-225, 234-253, 310` (separate `orderId` / `positionId`; no `opened_by_client_order_id`); `src/adapters/mock.py:638-655` (StoneX model) | Both rules apply only to adapters in `POSITION_IS_ENTRY_ORDER = frozenset({"stonex", "mock"})`; "some cTrader rows" becomes every cTrader position; cTrader entry fills give two lines in two stories with no fill hold, while close folding still works (§1.3, §4.4.2, §4.4.3); tests (§6) |

**Section 1 against prototype D and the theme.** Re-checked again. §1.3 now states that its six kinds are exactly
prototype D's `KIND` table, with each kind's chip (`positions`, `risk`, `halt`, `streams`, `engine`, `dlq`) and
icon, and that outages and DLQ episodes are stories of their own, as in prototype D's `buildStories`; the trade row
says that a cTrader entry order and its position stay two stories (fix C). The header colour follows severity on
the XTNL theme (`docs/design/live-view-prototypes/xtnl-theme.md`), which §1 already names as the page's theme (dark
default, light toggle from the theme file's light variant, no web fonts, flat square styling, status-only ticker);
where prototype D's own styling differs, the theme wins and prototype D keeps the layout and behaviour.

### 9.6 v7: after the round-3 check of v6

The round-3 check of v6 confirmed A and C fixed and found B partly wrong, with three new problems, D-F, all in the
engine start-up grouping. D holds: the "same process, proven" rule could file a restarted process's term under a
crashed run whose row was still open. Rather than patch the proof again, the lead decided (owner-side engineering
decision) to stop tying terms to runs by timing and probe uptime, and to key start-up stories by data the database
already records. The key semantics were checked against the code before the change. Section 3 is unchanged: every
column used (`lv_leader_terms.epoch_revision`, `lv_catchups.epoch`, `lv_stream_health.reconciled_epoch`) is already
in the views and in the 120-name allow-list. Sections 8.1-8.3 and the settled owner decisions are unchanged.

| # | Problem | Verified against | Change in v7 |
|---|---|---|---|
| B (rest), D | "Same process, proven" could file a restarted process's term under the crashed run (the old process's probe answers pass the test while the new run row is late); rule 1 had the same gap | `src/db/epoch.py:75-88` (the fencing token `engine_epoch.epoch` is the lease revision, = `leader_terms.epoch_revision`); `src/db/repository.py:123-129` (`reconciled_epoch` = the writer's fenced `$1`); `src/runtime/session.py:1160` and `src/db/uptime_store.py:81-87, 232-238` (`catchup_runs.epoch` = `epoch.token`, fenced insert); `src/db/leader.py:28-38, 85-87` (fence and term row in one transaction with one `now_ms`); `0001_init.sql:77` (`UNIQUE (cluster_id, epoch_revision)`) | A start-up story is keyed `term:{term_id}`. Members: the term's own lines, the fence (`conn_lost_at` = the term's `started_at`), catch-up rows with `epoch` = `epoch_revision` (a `reconnect` row that joins an outage stays there), and each stream's first reconcile with `reconciled_epoch` = `epoch_revision`. Engine run lines are standalone and never own a term. No hold, no 120 s wait for a run, no probe-uptime proof; `xtnl_engine_uptime_seconds` leaves the probe allow-list (its only use). The story closes when the term has reached LEADER and every active stream is reconciled in its epoch, at 120 s, or when the term ends; later members still join by key; members whose story is gone or whose term is unknown stand alone. `/api/story` accepts `term:` instead of `run:` (§1.3, §4.3, §4.4.3, §4.4.7, §4.6, §4.8); tests for keyed membership, a late catch-up row, a crash then restart, an old epoch, closing, no run-based waiting; the run / term, same-process and uptime tests are removed (§6) |
| E | §2 said only a term whose run is unknown waits, while rule 3 also held known-but-unproven terms | follows from the new rule | §2 now lists two waits only (fill / close pairing, 10 s; "Engine stopped responding", 60 s) and says no term or start-up line waits |
| F | "A held term is never lost on stop" was only true for cursor-tailed rows | §4.3 (diff sources: first reading silent, not backfilled) | Holds are gone; §4.4.7 states what comes back after a live-view restart: term lines and catch-up rows (cursor-tailed, backfilled) do; fence and reconciled lines (from the `lv_stream_health` diff) do not, and show only as the band's current state |

Consistency changes from the same rule: a stream loss that is not the leader-start fence always opens an outage
story (no "inside an open group" test; a drop before the stream's first reconcile in a term writes no new
`conn_lost_at`), and the first reconcile in a term closes such an outage with a link to the start-up story
(§4.4.3). "Engine stopped responding" followed by a new run within 60 s becomes the standalone `info` line "Engine
restarted (not responding for N s)" (§4.4.7, §4.6). The "Engine notice e-mailed" alert is a standalone line, no
longer attached to an open group by timing (§4.4.6). Oversee `stale_account_image` and `capped` / `uncapped` rows are
never members; while a start-up story is open they are still narrated at `info`, in their own stories. Incomplete
catch-ups and still-lost streams stay in the story as `warn` lines; SOFT / HARD transitions and an unclean previous
run are not members (§4.4.7, §4.4.8). "Start-up group" is renamed "start-up story" outside the change log.

### 9.7 v8: after the check of v7

The check of v7 confirmed the epoch semantics and found one major and two minor problems. Each was verified
against the code before the change. Section 3 is unchanged (no new column: `conn_lost_at`, `reconciled_at`,
`epoch`, `reason`, `end_reason` and `ended_at` are already in the views and the allow-list). Sections 8.1-8.3 and
the settled owner decisions are unchanged.

| # | Problem | Verified against | Change in v8 |
|---|---|---|---|
| 1 (major) | The 60 s reconnect rule could not be built: the `catchup_runs` row is committed before the reconcile it was measured against, so every reconnect row landed in the `term:` story; a catch-up's fence hold opens "lost" outages for streams that never dropped; with several outages open, the joined one was unnamed | `src/runtime/catchup.py:281-290, 323-337, 398-408, 573, 602` (`request` holds first; `_follow_ups` requests `reconnect` after an interruption; the row is recorded, then `release_fence`); `src/pubsub/orchestrator.py:860-882` (`_open_gap` / `_lose_all` mark the interrupted stream lost first), `:1498-1502` (no reconcile while held), `:1635-1661` (`hold_fence` writes `SQL_STREAM_LOST` for every stream not yet lost; `release_fence` reconciles); `src/db/repository.py:117-122` and `0001_init.sql:297-305` (`conn_lost_at = now_ms()` for both kinds of loss, no reason column: nothing stored tells a hold's loss from a real drop) | The join is decided when the row is read: a `reconnect`, `retry`, `operator` or `resume_after_sleep` row joins the oldest outage of its adapter still open (smallest `conn_lost_at`, ties by stream name; the hold keeps it open), else its term's story by epoch; `startup` and backfilled rows join the term's story. Every other outage open when the row is read (or lost and reconciled in its poll) and closed by the first reconcile after it is folded into the row's story with `story_merge {held: true}` and shown at `info` as "streams held for catch-up". For a `reconnect` the joined outage keeps its severity (it predates the hold); for the other reasons it is `info` too. The accepted limit is stated: a real drop of another stream open at that moment is folded as well, still shown once. `lv_catchups` is narrated before the stream-health diff in a poll, matching commit order (§1.3, §4.3, §4.4.3, §4.4.7, §4.4.8); tests (§6) |
| 2 (minor) | The closing rule listed term endings that do not set `ended_at` | `src/db/leader.py:31-35` (`SQL_CLOSE_PREDECESSORS` sets both), `:57-58, 103` (`SQL_END_TERM`, SAFE_HOLD abort in step 1, both), `:112-116` (`record_end_reason`: `end_reason` only); `src/runtime/exits.py:23` (`init_failed` hard exit 76 writes nothing) | A start-up story closes when `end_reason` or `ended_at` is set; an `init_failed` crash closes at 120 s or when its successor's term starts (§1.3, §4.4.7); the §6 closing test covers a drain, a both-set end, a crash at 120 s and a crash closed by its successor |
| 3 (minor) | A loss and reconcile between two polls had no outage key (`conn_lost_at` never read, so `…:None`) | `src/db/repository.py:123-129` (`reconciled_at = now_ms()`, never NULL after a reconcile) | Such an outage is keyed `out:{adapter}:{stream}:r{reconciled_at}`; no key part is ever NULL (the key builder refuses one; the event then stands alone); the `/api/story` pattern already accepts the `r` form (§4.3, §4.4.3); tests (§6) |

Also new: §4.4 states that story grouping is presentation only. Every row is shown exactly once whatever story it
lands in, and a grouping error can never hide, duplicate or alter a row.

### 9.8 v9: owner request (text hierarchy, edge shell page)

The owner asked for a visible text hierarchy and for a page kept for future edge and automated trading, as a shell
only, not connected. No review finding is involved. Section 3, the SQL, the views, the settings and environment
variables, the probe, the tailer, the narration and sections 8.1-8.3 are unchanged; nothing in the design gains a
write path.

| # | Request | Change in v9 |
|---|---|---|
| O1 | Text hierarchy present | New §1.7: L1 page title Inter 22px / 800, -0.02em, `--ink-0` (19px on phones); L2 section label JetBrains Mono 11px / 700 uppercase .14em `--ink-2` (optional L2 heading Inter 16px / 700 `--ink-0`); L3 item title Inter 15px / 600 `--ink-0`; L4 body Inter 13-14px / 400 `--ink-1`, line-height 1.5; L5 meta JetBrains Mono 11-12px `--ink-2`; hero values JetBrains Mono 22-26px / 700 coloured by meaning. Rules: no text below 11px, at most three levels per card, ids in titles in mono `--ink-1`. §1 points to it; §6 page tests check the classes and the 11px floor |
| O2 | Page for future edge / automated trading, shell only | New §1.8: static `GET /edge` (`edge.html` + `edge.js`, sharing `app.css`), linked from a header nav on both pages using real routes (`/` and `/edge`, no hash router). It reads only `/api/snapshot` (halt from `lv_halt_events`, poll state); ActionGate is derived on the page and always DENY in Phase 1; ARM is a fixed "not connected" label. Empty-state sections: Edges, Signals, Execution intents, Risk budget, Automation controls (locked placeholders). No form, no POST, no button with an action, no new SQL, view or environment variable. Phase 2 replaces the shell behind `ActionGate.allow(action)` and operator approval; nothing in Phase 1 may send orders. Consistency edits: §1 intro, §2 route list, §4 module list and static allow-list (three names to five), §4.8 route table, §7 |
| O3 | Tests for the shell | §6 server: `/edge` carries the same security headers and request checks, 405 on other methods, no SQL or probe call, `ALL_SQL` / `ALL_VIEWS` / settings unchanged. §6 page: no form or form control, every button `disabled` + `aria-disabled="true"`, one `fetch` of `/api/snapshot` only, no other network API, no `ALLOW`, and none of the `pre_tool_guard.py` forbidden names in the static files (the hook scans only `.py`), with a parity check against the hook and a scan of `src/liveview/*.py` |

### 9.9 Build notes (2026-11-21)

The build follows v9. Where it settles something the text leaves open, or differs from it:

- **Owner decisions:** 8.1 = A (no execution price of a closing fill), 8.2 = A (no size direction), 8.3 = B (the
  ladder is evenly spaced in level order, as approved in prototype E).
- **Server-to-page contract:** the exact JSON shapes are in `docs/design/live-view-contract.md`. The snapshot also
  carries `last_n` (the newest ring entry, activity or merge) and `merges` (the merge entries still in the ring), and
  the server pushes a `poll` SSE event each poll; the page opens its stream with `after=last_n`.
- **Facts cache keys** are typed (`ORDER|adapter:id`, `POSITION|adapter:id`): on StoneX an order and its position
  share the entity key `adapter:id`.
- **Start-up backfill** interleaves the sources by time but never reorders one source's rows (each keeps its id
  order), so a skewed engine clock cannot replay halt or term rows out of order. The open-entity seed (§4.4.1) is
  applied after the backfill, so the backfill narrates history from an empty cache and later live rows read against
  the current facts.
- **Title suffixes** are a closed list (`events.SUFFIXES`) chosen by name in the `extra` slot, so optional phrases
  ("(take-profit cancelled)", "; reason recorded in operator_audit …") never carry free text.
- **Unexpected ids:** a story key built from an id that fails the key pattern is never registered; the line stands
  alone instead of being dropped.
- **Review fixes (2026-11-21/22),** after two multi-agent reviews:
  - A poll is all-or-nothing: the tailer state is checkpointed before the transaction and restored on any database
    error, so a failed poll re-reads the same ids; a start does all its reads before narrating anything and is
    redone in full after a failure. A non-database exception fails the process (exit 75) instead of "db: down".
  - After every stream (re)open the server resends every header of today, not only those the replay touched: a
    header can change with no event (a badge, an episode's timing, a state change at a tick).
  - Late rows (the 60 s recheck) enter the watched sets and are narrated like cursor rows, terms included; watched
    reads are chunked well under their `LIMIT`, so an id missing from an answer really is gone.
  - A DLQ row's story is fixed at its first read; a lineage merge says "parked again (same lineage)", closes the
    row's own story and merges it into `dlq:{origin}`; only a story's own root row sets its state; an origin named
    by a lineage is watched too (it joins silently at its first read, so a re-arm after a restart is followed).
  - Every new halt row joins the day band in id order, whatever its engine-clock `at_ms` (a time before midnight is
    clamped to 00:00).
  - An Oversee row for a trade entity with no facts stands alone, unless the entity is open at start
    (`lv_open_*`), so a restart never loses a position's episodes; absorbed sibling cancels released without the
    close become their own "cancelled with the close" lines with their own ids and time.
  - The narrator's maps are pruned (a week, or a size cap) for 24/7 running.
