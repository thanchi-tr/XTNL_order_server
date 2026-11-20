# Alert email redesign: three prototypes

Design prototypes only. Nothing here is wired into the engine, and `src/` and `tests/` are unchanged.

| File | Prototype |
|---|---|
| [prototype-A-action-card.html](prototype-A-action-card.html) | A, Action card |
| [prototype-B-plain-note.html](prototype-B-plain-note.html) | B, Plain note |
| [prototype-C-status-ticket.html](prototype-C-status-ticket.html) | C, Status ticket |
| [gen_prototypes.py](gen_prototypes.py) | Builds all three from one shared set of samples: `python gen_prototypes.py` |
| [prototype-C2-status-ticket.html](prototype-C2-status-ticket.html) | **C2**, the second iteration of C: ACT / FIX / FYI / DONE, one header band, place chips on the steps, threaded reminders, 13 samples across all six segments |
| [prototype-C2-spec.md](prototype-C2-spec.md) | C2 rules: anatomy, status words, segment and audience for every alert context, subjects and threading, push format, glossary, data gaps |
| [gen_prototype_c2.py](gen_prototype_c2.py) | Builds C2: `python gen_prototype_c2.py` |
| [prototype-C3a-glance.html](prototype-C3a-glance.html) | **C3a Glance**, developed from C2: everything on the first phone screen |
| [prototype-C3b-timeline.html](prototype-C3b-timeline.html) | **C3b Timeline**, developed from C2: the alert's whole story in each email, now in the **XTNL Solutions theme** (built by [gen_prototype_c3b.py](gen_prototype_c3b.py)) |
| [prototype-C3b-timeline-v1-light.html](prototype-C3b-timeline-v1-light.html) | C3b's first, light version, kept for comparison |
| [prototype-C3c-status-board.html](prototype-C3c-status-board.html) | **C3c Status board**, developed from C2: the ticket plus a board of the whole account |
| [gen_prototype_c3.py](gen_prototype_c3.py) | Builds the three C3 pages: `python gen_prototype_c3.py` |

Each page shows the same six made-up alerts:

1. A new SOFT halt: an order has no stop loss.
2. The 30-minute reminder of that alert.
3. Its resolution.
4. The halt level rising to SOFT.
5. A daily FLAG, `transfers_unverified`.
6. A downtime catch-up after the PC slept.

For each alert the page shows the inbox row (subject and preview text), the phone push where one is sent (Important or Urgent only), and the email itself.

## What all three share

- **Action first.** The first thing you read is what to do now ("Add a stop loss to order ORD-DEMO-2 (EUR/USD) at StoneX"), or that no action is needed. After that comes what happened, then why it matters, then what happens next.
- **Segment label (FROM):** which part of the system sent the alert. The six samples use Risk check, Halt level, Daily account check and Broker connection and data. The other segments would be ARM advice and System health.
- **Audience label (FOR):** who the alert is for: "You, the trader", "For your records" or "For your records (operator)".
- **Severity in plain words:** Urgent (HARD), Important (SOFT), Heads-up (FLAG), Info, Resolved. Each has the same colours as today, all WCAG AA.
- **Stage:** New, Reminder 1 (open 30 min), Resolved, Level change: up, or Update.
- **Plain language:** the rule ids and words like SOFT_HALT, re-page, cap, episode and adapter appear only in a small technical block for the operator. That block also keeps the **old subject line**, so existing searches and filters still find the alert.
- **Times:** Melbourne first; the UTC time is in the technical block.
- **Read-only:** every footer says the server never places, changes or cancels orders. Every action is for you to take at StoneX. The halt email says that nothing is blocked at StoneX and that the halt is a signal for you.
- **Email-safe:** tables only, inline styles, no `<style>`, scripts, images or links, at most 600 px wide. All three were checked at 375 px phone width, with no sideways scrolling.

## The three approaches

| | A, Action card | B, Plain note | C, Status ticket |
|---|---|---|---|
| **Idea** | One big coloured box (DO THIS NOW / NO ACTION NEEDED), then three short labelled sections, a facts table and a grey operator box | A short note from an assistant: a From / For / About / Status header, a bold first line with the request, two short paragraphs | A coloured band with one status word (ACT / FYI / DONE), a 2 x 2 label grid, numbered to-do steps, and a strip showing the alert's life (new, reminder, resolved) or the halt-level ladder |
| **Subject style** | `Action needed: add a stop loss to ORD-DEMO-2 (EUR/USD)` | `Please add a stop loss to order ORD-DEMO-2` | `[ACT] Risk check: add a stop loss to ORD-DEMO-2` |
| **Strengths** | The action can't be missed on a phone. The same reading order every time. Maps one to one onto today's renderer (`Context.action`, `fields`, `detail`) | The friendliest and easiest to read. Almost no colour, so it reads well in any mail app and in dark mode. The shortest emails | The easiest to scan. The `[ACT]` / `[FYI]` / `[DONE]` subject tag sorts the inbox at a glance. The life strip shows whether this is the first alert, a reminder or the end. The ladder makes "SOFT" mean something |
| **Trade-offs** | Longer than B. The section headings repeat on every email | Harder to skim: the request is a sentence, not a box. The "I" voice has to be written by hand for each of the ~70 alert kinds and is easy to get uneven. No severity colour in the inbox | The densest. The life strip needs data the renderer doesn't have today (reminder number, next reminder time). The grid labels repeat what the band says |

## Recommendation

**A, with two parts of C:**

- **The `[ACT]` / `[FYI]` / `[DONE]` subject tag.** Most of the reading happens in the phone's inbox list, and this tag answers "do I need to do something?" before the email is opened. It also makes Gmail filters simple.
- **The halt-level ladder (NORMAL, SOFT, HARD), for halt emails only.** It turns SOFT and HARD into something you can see. `email_visuals.halt_ladder` already draws one, so this needs only new labels.

A on its own does the core job best. The action is the biggest thing on the screen, and FROM and FOR are the first two lines. It keeps the same order of sections for every alert kind, and it fits the current `email_html.Context` model. Most of the work is new wording (`rule_action`, the headlines, `TITLE_RULES`), not a new layout. B is the most pleasant to read, but it is harder to keep consistent across ~70 alert kinds and slower to skim. C's full life strip is worth adding later, once the alert carries its reminder number.

## Data the renderer would need when this is wired in

These are design notes for a later step. Nothing has been changed.

- **The order's symbol (EUR/USD) is not in the alert today.** A missing-stop alert carries only `order:ORD-DEMO-2`. To show the symbol, the outbox row needs it added when the alert is written, or the email should leave it out.
- **Reminder number and next reminder time:** neither is in the outbox row today. The time can be worked out from the 30-minute cadence (`escalate_after_ms`). The number needs the episode's delivery count from `notification_dedupe`.
- **Segment and audience:** both can be derived from what is already classified (`Context.name`, for example `oversee.episode` → Risk check, `halt.*` → Halt level, `ops.catchup` → Broker connection and data). Audience follows severity and kind: SOFT and HARD rule states and halts → you, the trader; FLAG and info → for your records; ops pages → operator.
- **Tests:** the plain-text part (`tests/notifications/text_baseline.json`) stays unchanged unless the subject line changes. A new subject style would change that baseline and any mail filters that match `[XTNL HIGH]` and similar tags.

## C3: three developments of C2

All three keep C2's rules ([prototype-C2-spec.md](prototype-C2-spec.md)): ACT / FIX / FYI / DONE, segment and audience, severity colour, place chips, and threaded reminders. They show the same 8 alerts, picked from C2's 13 to cover every status word and five of the six segments:

1. Missing stop, new.
2. Its reminder.
3. Its closure.
4. HARD halt.
5. Daily FLAG.
6. Catch-up incomplete.
7. Server not recording.
8. ARM advice.

| | C3a Glance | C3b Timeline | C3c Status board |
|---|---|---|---|
| **Idea** | The headline inside the coloured band. The first step as one big box; the other steps, why, next and facts as one line each. The full detail only in the operator line | The C2 ticket plus "the story so far", newest first: what is **coming** (dashed), **this email**, **linked** halt changes and the past events | The C2 ticket plus "Your account at hh:mm": six tiles for the halt level, waiting on you (ACT), waiting on the operator (FIX), heads-ups (FYI), StoneX data and the server |
| **Best at** | The first phone screen answers what, who and where. The shortest email | Reminders and DONE emails: open the latest email in a thread and you have the history without scrolling back. Shows cause and effect (a missing stop → halt SOFT) | Answers "is everything else OK?" from any email. The not-recording email turns the tiles grey (unknown) instead of showing old values |
| **Trade-offs** | Less explanation on screen. Why and next are cut to one sentence | Longest of the three. A new alert has little history, so the timeline is mostly "coming" | Every email gets longer by about 300 px. The board is a snapshot that may be out of date by the time you read it. It needs the most new data |
| **New data needed** | none beyond C2 | the episode's events (`oversee_events`, `halt_events`) when the alert is written; "coming" times from the reminder cadence and cool-down | a snapshot when the alert is written: the latest halt level, active states counted by level, `stream_health`, the single-PC recording state. Each tile must be able to say "unknown" |

### Recommendation for C3

**C3a Glance as the layout for every email**, with two parts of the others:

- **C3b's timeline only on reminders and DONE emails**, where the history is the point. A new alert keeps the Glance layout.
- **C3c's board as one line, not a block**, under the meta line: "Account now: halt SOFT · 1 waiting on you · StoneX live". Where it can't be read, it says "unknown". The full six-tile board fits better in a once-a-day morning summary email. That would also answer the C2 spec's question about where FYI emails should go.

The emails are read on a phone, so the most useful thing is getting the action onto the first screen, and Glance does that best. The timeline and the board add a lot to some emails and only length to the others, so each goes only where it helps.

## C3b in the XTNL Solutions theme

C3b now uses the look of [xtnl-solutions.com](https://www.xtnl-solutions.com/), taken from the site's own CSS variables:

- **Navy surfaces:** page `#04080f`, email card `#0b1622`, panels `#0f1e2e`.
- **Text:** `#eef2f8` for headings and values, `#9ab0c8` for body text and labels.
- **Brand mark:** the green `#00cc7a` ✕ XTNL mark and a "● READ-ONLY · PHASE 1 · STONEX" ticker, like the site's top bar.
- **Fonts:** Inter for text; JetBrains Mono for labels, ids, times and facts, like the site's figure tiles.

**Severity uses the site's accents:** Urgent red `#f03a57`, Important amber `#f0a030`, Heads-up blue `#4d9cf5`, Info slate `#9ab0c8`, Resolved green `#00cc7a`. The Heads-up blue and slate Info replace the light theme's amber and blue, because the site has no orange.

**Readability:**
- Every text colour is at least 4.5:1 on its background.
- The site's dimmer greys (`#5a7490`, `#2a3d52`) are too faint for text on the card (3.8:1 and 1.6:1), so they are used only for the timeline rail and dots.
- Accent fills (the ACT chip, the current ladder level) carry dark text, like the site's green button. White on the red would only be 3.9:1.

**Email-safety:**
- Solid colours only: the site's translucent lines are pre-blended onto the card, and its gradients and glow are left out.
- No web fonts are loaded: phones fall back to their system fonts.
- Tables and inline styles only, as before. Checked at 375 px with no sideways scrolling.

**Risk to check on your phone:** some mail apps (Gmail on Android in dark mode, Outlook) recolour emails, and a dark email can come out with muddy or inverted colours. The renderer declares light-only today. Before adopting this theme, send one test email to your phone in both light and dark system mode.

### Levels and pips (C3b, added 2026-11-21)

C3b now shows an order's levels where an alert is about a stop. A **Levels** block sits under the to-do steps:

- **Entry.** The reference point for every distance.
- **Each stop.** Its price and its distance from the entry in pips ("34.0 pips below entry"), with a bar showing that distance. The longest distance fills the bar; the level this email is about is highlighted in the severity colour.
- **A missing stop.** Shows **NONE · no limit** in the severity colour, with a dashed bar.

The ARM advice puts the price into the headline and the step ("Move the stop on ORD-DEMO-5 (GBP/USD) to 1.27020, 12.0 pips below the entry"). It also shows how much the move tightens the stop: 22.0 pips closer. The closed missing-stop alert says where the stop was seen (1.08250, 32.0 pips below the entry).

All prices are made up. These are order levels, not market data. The instrument's pip size, spreads and any market price are never shown. How pips would be computed when this is wired in is in the [C2 spec](prototype-C2-spec.md), under "Data the renderer would need".

## Wired in: stage 1 (2026-11-21)

The C3b design is now the HTML part of the real alert emails, behind one switch, `XTNL_EMAIL_THEME`:

- `xtnl` (the default) is the dark XTNL theme.
- `classic` gives back the earlier light layouts without a code change.

The plain-text part, the subjects and the push messages are unchanged, so `tests/notifications/text_baseline.json` and existing mail filters still hold.

| What | Where |
|---|---|
| Theme, building blocks, Oversee-alert page | `src/notifications/email_theme.py` (`render_oversee`, `ticket`) |
| Switch | `src/notifications/settings.py` (`XTNL_EMAIL_THEME`), passed through `factory.build_channels` → `mail.SmtpTransport` / `SesTransport` → `render_alert_html(theme=)` |
| ARM notice page with levels and pips | `src/notifications/email_html_arm.py` (`_render_arm_xtnl`, `render_arm_html(theme=, pip_units=)`) |
| Pip units at send time | `src/db/instrument_reads.py` (`PgPointSizes`), `src/notifications/arm_relay.py` (`ArmRelay(email_theme=, point_sizes=)`), wired in `src/runtime/optional_plan.py` |
| Rule changes | `.claude/rules/riskmgr.md` rule 5 and `.claude/rules/operations.md` §9: a stop's distance from the entry in pips may be shown; the pip size itself never |
| Tests | `tests/notifications/test_email_theme.py` |
| Preview of every context | `docs/design/notifications/gen_preview_xtnl.py` → `preview-xtnl.html` |

**What every email shows today:**
- The status word (ACT / FIX / FYI / DONE), the segment and the audience, worked out from the existing classification of each alert.
- A plain headline for rule alerts, halt changes and the common operations pages; other pages keep their current headline.
- Steps with a place chip.
- The halt or rule-level ladder.
- The facts.
- A short story: what is coming and this email.
- The operator block, with the old subject, the codes, ids, UTC time, node and epoch.

**ARM notices** add the Levels block:
- the entry (only with `show_levels`);
- the current stop and the proposed stop, each with its distance from the entry in pips;
- the size of the move ("22.0 pips tighter").

**Stage 2, not built yet:**
- the full story (first alert time, reminder number, linked halt events from `oversee_events` / `halt_events`);
- the order's symbol and its levels on Oversee alerts;
- the `[ACT]` / `[FIX]` subject tags and reminder threading.

Each of these changes data the engine writes, or the text baseline.
