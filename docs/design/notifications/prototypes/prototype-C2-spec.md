# Prototype C2: alert email spec (status ticket, second iteration)

Design spec only. Nothing is wired into the engine, and `src/` and `tests/` are unchanged.

- Page: [prototype-C2-status-ticket.html](prototype-C2-status-ticket.html), 13 made-up alerts across all six segments.
- Built by [gen_prototype_c2.py](gen_prototype_c2.py) (`python gen_prototype_c2.py`).
- The first version is [prototype C](prototype-C-status-ticket.html); [README.md](README.md) compares A, B and C.

## What changed from C v1

| v1 | C2 |
|---|---|
| ACT / FYI / DONE | **ACT / FIX / FYI / DONE**. The word says *who* acts: ACT is you at StoneX, FIX is the operator. The colour says *how bad* (Urgent, Important, Heads-up, Info, Resolved), so a HARD halt is a red ACT and a broker login failure is a red FIX. |
| Band plus a 2 x 2 grid (from, for, severity, stage) | **One band** (status word, segment, "For: …", who acts) and **one meta line** (severity, stage, day and time). Each label is shown once. |
| Plain numbered steps | Each step has a **place chip**: AT STONEX, SERVER PC, CHECK or JUST WAIT. You can see at once whether the step is yours, at the broker. |
| Reminders were separate emails | A reminder **keeps its first alert's subject**, so the phone groups New and its reminders in one thread (see "Subjects and threading"). The overnight reminders become one thread in the inbox. |
| Alerts stood alone | A **Linked** line ties an alert to its cause or follow-up ("This also raised the halt level to SOFT", "Ends the 20:14 halt"). |
| Ladder showed only "now" | The ladder shows the level it moved **from** (dashed, "was") and **to** ("now"). |
| 6 samples | 13 samples: HARD halt, halt cleared, catch-up incomplete, broker login refused, server not recording / recording again, and ARM advice. |

## Anatomy, top to bottom

1. **Band.** The status word, the segment, "For: audience", then who acts in words ("Action for you", "Action for the operator", "No action needed", "Closed, no action needed"). The band colour is the severity.
2. **Meta line.** A severity pill, the stage (New, Reminder n with how long it has been open, Level up / down, Closed, Update, Final advice) and the time in Melbourne with the day.
3. **Headline.** One sentence: an instruction ("Add a stop loss to order ORD-DEMO-2 (EUR/USD)"), or what is now true ("Halt level is back to NORMAL: trade as usual").
4. **To do.** One to three numbered steps, each with a place chip. With no steps, a tinted "Nothing to do. …" line. For FYI alerts with a conditional step, the heading is "ONLY IF IT APPLIES".
5. **Visual** (only where it adds meaning). The halt ladder for halt emails, the life strip for alerts that have a life (New → Reminders → Closed; Offline → Back → Caught up; Stopped → Recovering → Recording).
6. **Facts.** Two to four label / value pairs: ids, symbols, times, counts. Never money or price figures.
7. **Why** and **Next.** One or two sentences each. Next says when you will hear again.
8. **Linked** (optional). The cause or the follow-up, by segment and time.
9. **For the operator.** One small monospace line: rule id, level and cap, subject, alert kind and id, UTC time, and the **old subject line**, so existing searches still find the alert.
10. **Footer.** The read-only line (never places, changes or cancels orders) and the time-zone note.

## Status word: who acts

| Word | Meaning | Used for |
|---|---|---|
| **ACT** | You do something, at StoneX | Rule states at SOFT or HARD (new and reminders), halt level rising, ARM advice that suggests a move |
| **FIX** | The operator does something, on the server PC or the broker setup | Operational pages with an action: catch-up incomplete, broker login refused, server not recording, integrity, ruleset rejected, quarantine |
| **FYI** | Nobody needs to act | FLAG rule states, catch-up complete, ruleset loaded, maintenance windows, "no action" ops pages |
| **DONE** | Something that was open has closed | A rule state resolved or lowered to PASS, halt cleared, recording again, adapter re-armed, `integrity_resolved`, `quarantine_released` |

Severity colours stay as today (WCAG AA with white text): Urgent `#B42318` (HARD, critical pages), Important `#C4320A` (SOFT, high pages), Heads-up `#B54708` (FLAG), Info `#175CD3`, Resolved `#067647`.

## Segment and audience for every context

The renderer already classifies each alert into a context (`email_html.classify` → `Context.name`). Segment, audience and status word can all be derived from that name, plus the level or the action text already there. No new data is needed for this table.

| Context (`Context.name`) | Segment | For | Word |
|---|---|---|---|
| `oversee.episode`, `oversee.repage`: rule at SOFT or HARD | Risk check | You, the trader | ACT |
| `oversee.episode`, `oversee.repage`: rule at FLAG | Risk check | For your records | FYI |
| the same, when the subject is an account day (`max_daily_loss`, `sod_partial_session`) | Daily account check | For your records (ACT at HARD) | FYI / ACT |
| `oversee.resolved`, `oversee.lowered` (to PASS) | Risk check / Daily account check | You, the trader | DONE |
| `oversee.lowered` (still FLAG or above), `oversee.capped`, `oversee.uncapped` | Risk check | For your records | FYI |
| `halt.soft`, `halt.hard` | Halt level | You, the trader | ACT |
| `halt.cleared`, `halt.cleared_by_operator` | Halt level | You, the trader | DONE |
| `ops.catchup` | Broker connection and data | For your records | FYI (FIX when it ends INCOMPLETE) |
| `ops.catchup_incomplete`, `ops.clock_moved_back`, `ops.cash_flow_ledger_incomplete` | Broker connection and data | Operator | FIX |
| `ops.auth_failed`, `ops.pre_open_failed`, `ops.ingestion`, `ops.consumer` | Broker connection and data | Operator | FIX |
| `ops.adapter_rearmed` | Broker connection and data | Operator | DONE |
| `ops.calendar`, `ops.sessions_review`, `ops.session_unusable`, `ops.sessions_not_saved`, `ops.maintenance_unreadable` | Broker connection and data (market hours) | Operator | FIX |
| `ops.maintenance` | Broker connection and data (market hours) | For your records | FYI |
| `ops.not_recording`, `ops.safe_hold_stopped`, `ops.task_circuit`, `ops.task_stale`, `ops.risksweep_both`, `ops.optional_services`, `ops.lifecycle`, `oversee.feed_lag` | System health | Operator | FIX |
| `ops.recording_again`, `ops.safe_hold_resumed` | System health | Operator | DONE / FYI |
| `ops.ruleset` | System health | For your records | FYI |
| `ops.flag` (`flag:<code>`, for example `pnl_unverified`) | System health | Operator | FIX |
| `ops.ruleset_rejected`, `ops.ruleset_rejected_init` | System health | Operator | FIX |
| `oversee.page` (by code; the "No action." codes → FYI, `quarantine_released` / `unlisted_states_resolved` → DONE) | System health | Operator | FIX / FYI / DONE |
| `integrity.page` (`integrity_resolved` → DONE; `digest_repaired`, `redis_orphan` → FYI) | System health | Operator | FIX / FYI / DONE |
| `md.page` | ARM advice (market data) | Operator | FIX |
| ARM notices: `arm.pre_adjust`, `arm.post_adjust`, `arm.post_mixed`, `arm.late_adjust` | ARM advice | You, the trader | ACT |
| ARM notices: `arm.post_pending`, `arm.post_delayed`, `arm.corrected`, `arm.alive`, `arm.test` | ARM advice | For your records | FYI |
| `arm.failed_ping`, `arm.failed_not_recorded`, `arm.operations`, `arm.md_reset` | ARM advice | Operator | FIX |
| `arm.failed_cleared` | ARM advice | Operator | DONE |
| `generic` (a title no rule knows) | System health | Operator | FIX if severity is high or critical, else FYI |

You are both the trader and the operator today, so "Operator" only means "this is about the server, not your trades". If a second person is ever on call, FIX alerts could go to their address too.

## Subjects and threading

- **Grammar:** `[WORD] Segment: headline`, for example `[ACT] Risk check: add a stop loss to ORD-DEMO-2`. Aim for 60 characters or fewer, so the whole line shows in a phone inbox.
- **Reminders reuse the subject of their first alert.** Phone mail apps then group them into one conversation, and the inbox shows `(2)`, `(3)`, … instead of a new line every 30 minutes. The reminder email itself still says "Reminder n, open 30 min" in its meta line, life strip and preview text. Threading also needs mail headers:
  - The first alert of an episode gets `Message-ID: <ep-{hash of the episode key}@xtnl>`.
  - Its reminders send `In-Reply-To` and `References` set to that id.
- **DONE gets its own subject**, so a resolution shows as a new line in the inbox. It also sends `References` to the episode's id, so Outlook and Apple Mail still thread it. Gmail starts a new conversation, because Gmail needs the same subject.
- **Filters:** `subject:"[ACT]"` is everything waiting on you; `subject:"[FIX]"` is server work.
- **Cost:** the subject and the plain-text part change. `tests/notifications/text_baseline.json` would be regenerated, and any mail filter on `[XTNL HIGH]` etc. moves to the new tags. Keeping the old subject in the operator line means old searches still find the alerts.

## Phone push (ntfy)

Push is sent only for Important and Urgent, as today (`channels_for`).

- **Title:** `WORD · headline`, at most about 50 characters. An Urgent push puts `URGENT:` after the word.
- **Body:** `Segment · for audience · stage`, or the cause for a halt.
- **Priority (proposed):** Urgent → ntfy 5 (max), Important → 4 (high).

## Plain-language glossary

Internal words appear only in the operator line. Everywhere else, the email uses these:

| Internal | In the email |
|---|---|
| SOFT_HALT / SOFT | soft halt: don't add new risk |
| HARD_HALT / HARD | hard halt: stop all trading until the operator clears it |
| NONE (halt level) | NORMAL: trade as usual |
| FLAG | Heads-up |
| PASS / resolved | closed (DONE) |
| episode | alert |
| re-page | reminder |
| acknowledge | stop the reminders (operator) |
| cap SOFT | (operator line only; in prose, "some data is unknown, so it is held at soft halt") |
| sweep / RiskSweep / Oversee | Risk check |
| HaltSwitch / gate | Halt level |
| adapter / SM4 / stream | the StoneX connection |
| downtime catch-up | reading what changed at StoneX while the server was offline |
| fence / `stale_account_image` | account data from StoneX is out of date |
| `stale_mark` | prices are out of date (normal while the market is closed) |
| `missing_stop_loss` | no stop loss |
| `max_position_risk` | one trade risks too much |
| `max_aggregate_open_risk` | your open trades together risk too much |
| `max_daily_loss` | daily loss limit |
| `margin_usage` | margin use is high |
| `transfers_unverified` | today's deposits and withdrawals are not confirmed |
| `pnl_unverified` | today's trading result is not confirmed by StoneX |
| `sod_missing` / `sod_partial_session` | the start-of-day snapshot is missing / partial |
| AUTH_FAILED | StoneX refused the login |
| SAFE_HOLD / not recording | the server stopped recording |
| bar | candle |

## Writing rules

- The headline is an instruction or a plain fact. Never a rule id, a level code or "still SOFT_HALT …".
- Give one to three steps. Each step happens in one place and starts with a verb.
- Above the operator line, show only order / position / account ids, symbols, times and counts. Never account size, balances, P&L, money amounts, pip / point sizes, spreads, or ticks / bars / quotes / indicator values.
- **Exception for order levels (your request, 2026-11-21):** an order's own levels (entry, current stop, proposed stop) may be shown, each with its distance from the entry in pips ("32.0 pips below entry"). They are order data, not market data. The instrument's pip / point size itself, spreads, and any market price (current price, quote, tick, bar) are still never shown. See the Levels block in [prototype-C3b-timeline.html](prototype-C3b-timeline.html).
- Never suggest the server places, changes or cancels anything. Actions at the broker are "yours", and halts are "a signal for you".
- Times are Melbourne local time with the day ("Fri 20 Nov, 20:14"). UTC goes in the operator line.
- Every email says when you will hear again (Next), or that it is closed.

## Data the renderer would need

Most of C2 comes from data the alert already carries. The gaps, field by field:

| Field | Source today | Gap |
|---|---|---|
| Word, segment, audience | `Context.name`, level, action | none |
| Severity, kind, alert id, UTC time | `AlertMessage` | none |
| Halt ladder from / to | halt title and body (`HALT SOFT` / `HARD` / cleared) | "from" needs the previous level (in `halt_events`) |
| First alert time ("open since") | `BreachState.active_since` | not in the outbox row; add it to the reminder body |
| Reminder number | `notification_dedupe` (last sent only) | needs a per-episode delivery count |
| Next reminder time | last sent + `escalate_after_ms` | derivable when the row is written |
| Symbol of an order or position | the order / position rows | not in the alert; look it up when the row is written, or leave it out |
| Linked line (rule ↔ halt) | the halt trip and the transition are written in the same transaction | the trip's cause is in its reason; the reverse link needs the trip id on the rule row |
| Threading headers | the episode key | the mail transport must set `Message-ID` / `In-Reply-To` / `References` |
| Entry and stop levels, pips from entry | `OrderDTO` / `PositionDTO` `entry_price`, `stop_loss`; the ARM notice's proposed stop | pips = \|stop − entry\| ÷ the pip unit. The unit derives from `InstrumentSpec.point_size`, but needs a per-asset-class factor: on FX quoted to 5 decimals a point is a tenth of a pip, and indices and commodities should say "points". Confirm against StoneX's instrument data. The renderer's content rule (`email_html` docstring, `operations.md` §9) must be widened to allow these, never the pip size itself |

## Open questions for you

1. **Reminder threading.** Is it OK that reminders fold into one thread (fewer inbox lines, but a reminder no longer shows as a new email in the list)? The push still arrives each time for Important and Urgent alerts.
2. **FYI volume.** Should FYI emails (daily FLAG, catch-up complete, ruleset loaded) go to a separate Gmail label, or be collected into one morning digest?
3. **Operator mail.** Keep FIX alerts on the same address, or send them to a second address or label?
