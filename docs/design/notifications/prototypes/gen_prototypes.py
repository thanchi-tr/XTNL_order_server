"""Generate the three alert-email redesign prototypes (A, B, C) in this folder.

    python docs/design/notifications/prototypes/gen_prototypes.py

Design prototypes only: nothing here is imported by `src/` and nothing in `src/` is imported here. Every value is
made up (order `ORD-DEMO-2`, account `ACC-DEMO-1`, alert ids, times). By rule, no account size, balance, P&L,
money amount, pip / point size or spread appears, not even as a sample.

Each prototype page shows the same six alerts. For each one it shows an inbox row (sender, subject, preview text),
the phone push text where the alert also goes to the phone (SOFT / HARD only), and the email itself. The email part
is email-safe: tables only, inline styles, no <style>, no images, no scripts, no links, max width 600 px.
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
FONT = "Arial,Helvetica,sans-serif"
MONO = "Consolas,Menlo,monospace"
INK = "#101828"
MUTED = "#475467"
SOFT_INK = "#667085"
LINE = "#EAECF0"
PAGE_BG = "#F2F4F7"
READ_ONLY = ("XTNL order-server, Phase 1 (read-only). It watches your StoneX account and never places, "
             "changes or cancels orders. Any change is yours to make at StoneX.")

# Severity in plain words: (colour, label, light tint). Every colour passes WCAG AA with white text.
SEV = {
    "urgent": ("#B42318", "Urgent", "#FEF3F2"),
    "important": ("#C4320A", "Important", "#FFF4ED"),
    "headsup": ("#B54708", "Heads-up", "#FFFAEB"),
    "info": ("#175CD3", "Info", "#EFF8FF"),
    "resolved": ("#067647", "Resolved", "#ECFDF3"),
}


_ID = re.compile(r"\b(ORD-DEMO-\d+|ACC-DEMO-\d+)\b")


def _for(audience: str) -> str:
    """'for You, the trader'; 'For your records' is kept as is."""
    return audience if audience.lower().startswith("for ") else f"for {audience}"


def e(text: str) -> str:
    """Escaped text; an id is kept on one line (a phone would otherwise break `ORD-` / `DEMO-2`)."""
    return _ID.sub(r'<span style="white-space:nowrap;">\1</span>', html.escape(text, quote=True))


@dataclass(frozen=True)
class Stage:
    """One point of the alert's life (prototype C's strip): state is done | now | later."""

    label: str
    time: str
    state: str


@dataclass(frozen=True)
class Sample:
    key: str
    caption: str
    segment: str               # which part of the system sent it (plain)
    segment_tech: str          # the internal component, for the operator
    audience: str              # who it is for
    sev: str                   # SEV key
    stage: str                 # New | Reminder ... | Resolved | Level change ...
    act: str                   # ACT | FYI | DONE (prototype C's status word)
    action_needed: bool
    action: str                # the first line: what to do now
    title_c: str               # prototype C's short title
    steps: tuple[str, ...]     # prototype C's numbered to-do
    what: str
    why: str
    next: str
    facts: tuple[tuple[str, str], ...]
    tech: tuple[tuple[str, str], ...]
    subject: dict[str, str]
    letter: tuple[str, ...]    # prototype B's paragraphs after the bold first line
    letter_ask: str            # prototype B's bold first line
    push: dict[str, tuple[str, str]] = field(default_factory=dict)   # prototype -> (title, body)
    lifecycle: tuple[Stage, ...] = ()
    ladder: int | None = None  # halt level index: 0 NORMAL, 1 SOFT, 2 HARD


SAMPLES: tuple[Sample, ...] = (
    Sample(
        key="new", caption="New soft halt: an order has no stop loss",
        segment="Risk check", segment_tech="Oversee, RiskSweep", audience="You, the trader",
        sev="important", stage="New", act="ACT", action_needed=True,
        action="Add a stop loss to order ORD-DEMO-2 (EUR/USD) at StoneX.",
        title_c="Add a stop loss to ORD-DEMO-2",
        steps=("Open StoneX and find order ORD-DEMO-2 (EUR/USD).",
               "Attach a stop loss to it.",
               "That is all. This alert closes by itself once the server sees the stop."),
        what="Order ORD-DEMO-2 (EUR/USD) has had no stop loss since 20:14. The risk check has marked it as a soft halt.",
        why=("Without a stop, the possible loss on this order has no limit. While this is open, the account's halt "
             "level is SOFT: don't add new risk. If the stop stays missing, the level can rise to HARD."),
        next="A reminder every 30 minutes until a stop is seen (next at 20:44), then an all-clear email.",
        facts=(("Order", "ORD-DEMO-2"), ("Symbol", "EUR/USD"), ("Problem", "No stop loss"), ("Since", "20:14")),
        tech=(("Rule", "missing_stop_loss"), ("Level", "SOFT_HALT, cap NONE"), ("Subject", "order:ORD-DEMO-2"),
              ("Alert", "episode (new), severity HIGH, alert id 5512"),
              ("Sent", "2026-11-20 09:14:03 UTC"),
              ("Old subject", "[XTNL HIGH] SOFT_HALT missing_stop_loss order:ORD-DEMO-2")),
        subject={"A": "Action needed: add a stop loss to ORD-DEMO-2 (EUR/USD)",
                 "B": "Please add a stop loss to order ORD-DEMO-2",
                 "C": "[ACT] Risk check: add a stop loss to ORD-DEMO-2"},
        letter_ask="Please add a stop loss to order ORD-DEMO-2 (EUR/USD) at StoneX.",
        letter=("Since 20:14 this order has had no stop, so the loss it could make has no limit. I have put the "
                "account on a soft halt, which means: don't add new risk for now.",
                "Once the stop is in place I will notice it and send you an all-clear. Until then I will remind you "
                "every 30 minutes. If the stop stays missing, the halt can rise to its highest level (HARD)."),
        push={"A": ("Action needed: add a stop to ORD-DEMO-2", "Risk check, for you. EUR/USD order has no stop loss."),
              "B": ("Please add a stop loss to ORD-DEMO-2", "The EUR/USD order has no stop since 20:14."),
              "C": ("[ACT] Add a stop loss to ORD-DEMO-2", "Risk check, new, important")},
        lifecycle=(Stage("New", "20:14", "now"), Stage("Reminders", "every 30 min", "later"),
                   Stage("Resolved", "when fixed", "later")),
    ),
    Sample(
        key="reminder", caption="30-minute reminder of the same alert",
        segment="Risk check", segment_tech="Oversee, RiskSweep", audience="You, the trader",
        sev="important", stage="Reminder 1, open 30 min", act="ACT", action_needed=True,
        action="Add a stop loss to order ORD-DEMO-2 (EUR/USD) at StoneX.",
        title_c="ORD-DEMO-2 still has no stop loss",
        steps=("Open StoneX and find order ORD-DEMO-2 (EUR/USD).",
               "Attach a stop loss to it.",
               "If the missing stop is on purpose, ask the operator to acknowledge this alert to stop the reminders."),
        what="Order ORD-DEMO-2 (EUR/USD) still has no stop loss. The first alert was at 20:14; it has been open 30 minutes.",
        why="The account stays on a soft halt while this is open, and the halt can rise to HARD if the stop stays missing.",
        next=("Next reminder at 21:14. If the missing stop is on purpose, the operator can acknowledge the alert to stop "
              "the reminders."),
        facts=(("Order", "ORD-DEMO-2"), ("Symbol", "EUR/USD"), ("Open since", "20:14 (30 min)"),
               ("Next reminder", "21:14")),
        tech=(("Rule", "missing_stop_loss"), ("Level", "SOFT_HALT, cap NONE"), ("Subject", "order:ORD-DEMO-2"),
              ("Alert", "repage (still active), severity HIGH, alert id 5519"),
              ("Sent", "2026-11-20 09:44:03 UTC"),
              ("Old subject", "[XTNL HIGH] still SOFT_HALT missing_stop_loss order:ORD-DEMO-2 (cap NONE)")),
        subject={"A": "Reminder: ORD-DEMO-2 still has no stop loss (30 min)",
                 "B": "Reminder: order ORD-DEMO-2 still has no stop loss",
                 "C": "[ACT, reminder 1] Risk check: ORD-DEMO-2 still has no stop loss"},
        letter_ask="Reminder: order ORD-DEMO-2 (EUR/USD) still has no stop loss. Please add one at StoneX.",
        letter=("I first wrote at 20:14, so it has been open for 30 minutes. The account stays on a soft halt until "
                "the stop is in place, and the halt can rise to HARD if it stays missing.",
                "If you left it without a stop on purpose, the operator can acknowledge the alert and I will stop "
                "reminding you. Otherwise the next reminder is at 21:14."),
        push={"A": ("Reminder: ORD-DEMO-2 still has no stop", "Risk check, for you. Open 30 min."),
              "B": ("Reminder: ORD-DEMO-2 still has no stop", "Open since 20:14. Please add a stop loss."),
              "C": ("[ACT, reminder 1] ORD-DEMO-2: no stop", "Risk check, open 30 min")},
        lifecycle=(Stage("New", "20:14", "done"), Stage("Reminder 1", "20:44", "now"),
                   Stage("Resolved", "when fixed", "later")),
    ),
    Sample(
        key="resolved", caption="Resolution of the same alert",
        segment="Risk check", segment_tech="Oversee, RiskSweep", audience="You, the trader (for your records)",
        sev="resolved", stage="Resolved", act="DONE", action_needed=False,
        action="No action needed. The stop is in place.",
        title_c="ORD-DEMO-2 has a stop loss now",
        steps=(),
        what="Order ORD-DEMO-2 (EUR/USD) now has a stop loss. The alert closed at 20:51, 37 minutes after it opened.",
        why="The loss this order can make is limited again.",
        next=("The halt level goes back to NORMAL after a short cool-down, if nothing else is open. You will get a "
              "separate email when it does."),
        facts=(("Order", "ORD-DEMO-2"), ("Symbol", "EUR/USD"), ("Fixed at", "20:51"), ("Was open", "37 min")),
        tech=(("Rule", "missing_stop_loss"), ("Transition", "resolved (was SOFT_HALT, cap NONE)"),
              ("Subject", "order:ORD-DEMO-2"), ("Alert", "info, severity INFO, alert id 5523"),
              ("Sent", "2026-11-20 09:51:22 UTC"),
              ("Old subject", "[XTNL INFO] resolved missing_stop_loss order:ORD-DEMO-2")),
        subject={"A": "Resolved: ORD-DEMO-2 has a stop loss now",
                 "B": "All clear: ORD-DEMO-2 now has a stop loss",
                 "C": "[DONE] Risk check: ORD-DEMO-2 has a stop loss"},
        letter_ask="All clear: order ORD-DEMO-2 (EUR/USD) has a stop loss now. Nothing more to do.",
        letter=("I saw the stop at 20:51, 37 minutes after the first alert, so the loss this order can make is "
                "limited again.",
                "The halt level goes back to NORMAL after a short cool-down if nothing else is open. I will send a "
                "separate email when it does."),
        lifecycle=(Stage("New", "20:14", "done"), Stage("Reminder 1", "20:44", "done"),
                   Stage("Resolved", "20:51", "now")),
    ),
    Sample(
        key="halt", caption="Halt level rises to SOFT",
        segment="Halt level", segment_tech="Oversee, HaltSwitch", audience="You, the trader",
        sev="important", stage="Level change: up", act="ACT", action_needed=True,
        action=("Don't open new trades or add to positions until the halt level is back to NORMAL. "
                "Fix the cause: add a stop loss to ORD-DEMO-2."),
        title_c="Halt level is now SOFT: hold off on new trades",
        steps=("Hold off on new trades and on adding to positions.",
               "Fix the cause: add a stop loss to order ORD-DEMO-2 (EUR/USD) at StoneX.",
               "Wait for the all-clear email. The level goes back to NORMAL after a short cool-down."),
        what="The account's halt level went up from NORMAL to SOFT at 20:14, because order ORD-DEMO-2 has no stop loss.",
        why=("SOFT means: don't add new risk. This server places no trades, so nothing is blocked at StoneX. "
             "The halt is a signal for you to act on."),
        next=("It goes back to NORMAL once nothing is open and a cool-down has passed. If the cause stays open, it can "
              "rise to HARD: stop all trading until the operator has reviewed and cleared it."),
        facts=(("Halt level", "NORMAL to SOFT"), ("Cause", "No stop loss on ORD-DEMO-2"), ("Since", "20:14")),
        tech=(("Event", "HALT SOFT (trip), NONE to SOFT"), ("Cause", "missing_stop_loss order:ORD-DEMO-2"),
              ("Halt event id", "881"), ("Alert", "trip, severity HIGH, alert id 5513"),
              ("Sent", "2026-11-20 09:14:04 UTC"), ("Old subject", "[XTNL HIGH] HALT SOFT")),
        subject={"A": "Halt level is now SOFT: hold off on new trades",
                 "B": "Please hold off on new trades for now",
                 "C": "[ACT] Halt level: NORMAL to SOFT, hold off on new trades"},
        letter_ask="Please hold off on new trades and on adding to positions for now.",
        letter=("At 20:14 the account's halt level went up from NORMAL to SOFT, because order ORD-DEMO-2 (EUR/USD) "
                "has no stop loss. Adding a stop to it fixes the cause.",
                "SOFT means: don't add new risk. I place no trades, so nothing is blocked at StoneX; this is a signal "
                "for you. Once nothing is open and a short cool-down has passed, the level goes back to NORMAL and I "
                "will tell you. If the cause stays open, it can rise to HARD, which means stop all trading until the "
                "operator has reviewed and cleared it."),
        push={"A": ("Halt level SOFT: hold off on new trades", "Cause: no stop loss on ORD-DEMO-2."),
              "B": ("Please hold off on new trades", "Halt level is SOFT: ORD-DEMO-2 has no stop."),
              "C": ("[ACT] Halt level NORMAL to SOFT", "Hold off on new trades. Cause: ORD-DEMO-2 no stop.")},
        ladder=1,
    ),
    Sample(
        key="daily", caption="Daily check FLAG: deposits and withdrawals not confirmed",
        segment="Daily account check", segment_tech="Oversee, daily loss rule", audience="For your records",
        sev="headsup", stage="New, for today", act="FYI", action_needed=False,
        action="No action needed. If you deposited or withdrew money today, check today's result at StoneX yourself.",
        title_c="Today's deposits and withdrawals are not confirmed",
        steps=("Nothing to do if you made no deposit or withdrawal today.",
               "If you did, check today's result at StoneX yourself; the daily loss check may read it wrongly."),
        what=("StoneX did not report today's deposits and withdrawals, so the daily loss check cannot tell a "
              "transfer apart from a trading result for trading day Fri 20 Nov."),
        why=("The daily loss limit still runs, but a deposit or withdrawal today could make it read too high or too "
             "low. This is a heads-up only: nothing is halted."),
        next=("It ends with the trading day. While you have open trades it is repeated every 30 minutes; while you "
              "have none, it is not."),
        facts=(("Trading day", "Fri 20 Nov"), ("Check", "Daily loss limit"), ("Missing", "Deposits and withdrawals")),
        tech=(("Rule", "max_daily_loss"), ("Level", "FLAG, reason transfers_unverified"),
              ("Subject", "account:ACC-DEMO-1:2026-11-20"), ("Alert", "episode (new), severity INFO, alert id 5470"),
              ("Sent", "2026-11-19 21:01:05 UTC"),
              ("Old subject", "[XTNL INFO] FLAG max_daily_loss account:ACC-DEMO-1:2026-11-20")),
        subject={"A": "For your records: today's deposits and withdrawals are not confirmed",
                 "B": "FYI: I could not confirm today's deposits and withdrawals",
                 "C": "[FYI] Daily check: deposits and withdrawals not confirmed for 20 Nov"},
        letter_ask="Nothing to do, unless you deposited or withdrew money today.",
        letter=("StoneX did not report today's deposits and withdrawals (trading day Fri 20 Nov), so my daily loss "
                "check cannot tell a transfer apart from a trading result.",
                "The daily loss limit still runs; nothing is halted. If you did move money today, check today's "
                "result at StoneX yourself. This ends with the trading day."),
        lifecycle=(Stage("New", "08:01", "now"), Stage("Repeats", "only with open trades", "later"),
                   Stage("Ends", "end of trading day", "later")),
    ),
    Sample(
        key="catchup", caption="Broker connection: catch-up after downtime (info)",
        segment="Broker connection and data", segment_tech="Operations, downtime catch-up",
        audience="For your records (operator)", sev="info", stage="Update", act="FYI", action_needed=False,
        action="No action needed. The server is back in sync with StoneX.",
        title_c="Back in sync after 46 min offline",
        steps=(),
        what=("The server was offline from 06:55 to 07:41 (46 min, the PC was asleep). When it woke up it read what "
              "had changed at StoneX: 2 orders updated and 1 position closed. Nothing is missing."),
        why="The risk checks are working from complete data again.",
        next=("If a catch-up ever cannot finish, you will get a separate Important email asking the operator to "
              "check the broker connection."),
        facts=(("Offline", "06:55 to 07:41 (46 min)"), ("Why", "PC was asleep"), ("Orders", "0 new, 2 updated"),
               ("Positions", "0 new, 1 closed")),
        tech=(("Adapter", "stonex"), ("Catch-up", "reason wake, result complete"),
              ("Offline window", "2026-11-19 19:55Z .. 20:41Z"), ("Alert", "info, severity INFO, alert id 5466"),
              ("Sent", "2026-11-19 20:42:10 UTC"), ("Old subject", "[XTNL INFO] downtime catch-up: stonex")),
        subject={"A": "For your records: back in sync with StoneX after 46 min offline",
                 "B": "FYI: I was offline for 46 minutes and have caught up",
                 "C": "[FYI] Broker data: caught up after 46 min offline"},
        letter_ask="Nothing to do: I am back in sync with StoneX.",
        letter=("I was offline from 06:55 to 07:41 (46 minutes, the PC was asleep). When I woke up I read what had "
                "changed at StoneX: 2 orders updated and 1 position closed. Nothing is missing, so the risk checks "
                "are working from complete data again.",
                "If a catch-up ever cannot finish, I will send a separate Important email to the operator."),
        lifecycle=(Stage("Offline", "06:55", "done"), Stage("Back", "07:41", "done"),
                   Stage("Caught up", "07:42", "now")),
    ),
)

TIME_NOTE = "Times are Melbourne time (AEDT); the UTC time is in the technical details."
LADDER = ("NORMAL", "SOFT", "HARD")
LADDER_HINT = ("trade as usual", "don't add new risk", "stop all trading")


# ------------------------------------------------------------------------------------------------ shared parts
def preheader(text: str) -> str:
    return (f'<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;font-size:1px;line-height:1px;'
            f'color:#FFFFFF;opacity:0;">{e(text)}</div>')


def shell(inner_rows: str, *, card_style: str = "") -> str:
    """The email's outer frame: grey page, one centred card of at most 600 px."""
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#F2F4F7" '
            'style="background-color:#F2F4F7;"><tr><td align="center" style="padding:16px 8px;">'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#FFFFFF" '
            f'style="max-width:600px;width:100%;background-color:#FFFFFF;{card_style}">'
            f'{inner_rows}</table></td></tr></table>')


def pill(text: str, color: str, *, solid: bool = True) -> str:
    style = (f"background-color:{color};color:#FFFFFF;" if solid
             else f"background-color:#FFFFFF;color:{color};border:1px solid {color};")
    return (f'<span style="display:inline-block;padding:3px 8px;margin:0 4px 4px 0;border-radius:3px;{style}'
            f'font:700 12px/1.3 {FONT};letter-spacing:.03em;">{e(text)}</span>')


def tech_rows(s: Sample) -> str:
    return "".join(
        f'<tr><td valign="top" style="padding:2px 10px 2px 0;font:12px/1.45 {FONT};color:{SOFT_INK};'
        f'white-space:nowrap;">{e(k)}</td><td style="padding:2px 0;font:12px/1.45 {MONO};color:{MUTED};'
        f'word-break:break-all;">{e(v)}</td></tr>' for k, v in s.tech)


def facts_table(s: Sample) -> str:
    rows = "".join(
        f'<tr><td width="38%" valign="top" style="padding:6px 10px;border-top:1px solid {LINE};font:13px/1.4 {FONT};'
        f'color:{SOFT_INK};">{e(k)}</td><td style="padding:6px 10px;border-top:1px solid {LINE};'
        f'font:600 14px/1.4 {FONT};color:{INK};">{e(v)}</td></tr>' for k, v in s.facts)
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="border-bottom:1px solid {LINE};">{rows}</table>')


def footer_row(extra: str = "") -> str:
    return (f'<tr><td style="padding:14px 20px 18px;border-top:1px solid {LINE};font:12px/1.5 {FONT};'
            f'color:{SOFT_INK};">{e(READ_ONLY)}<br>{e(TIME_NOTE)}{extra}</td></tr>')


# ------------------------------------------------------------------------------------------------ prototype A
def email_a(s: Sample) -> str:
    """A, "Action card": labels strip, one big coloured action box, then three short labelled sections."""
    color, label, tint = SEV[s.sev]
    box_color = color if s.action_needed else SEV["resolved" if s.sev == "resolved" else "info"][0]
    box_tint = tint if s.action_needed else SEV["resolved" if s.sev == "resolved" else "info"][2]
    box_label = "DO THIS NOW" if s.action_needed else "NO ACTION NEEDED"
    box_text = s.action if s.action_needed else s.action.removeprefix("No action needed. ")

    def section(head: str, body: str) -> str:
        return (f'<tr><td style="padding:0 20px 14px;"><div style="font:700 12px/1.3 {FONT};color:{SOFT_INK};'
                f'letter-spacing:.06em;text-transform:uppercase;padding-bottom:4px;">{e(head)}</div>'
                f'<div style="font:15px/1.55 {FONT};color:{INK};">{e(body)}</div></td></tr>')

    rows = [
        f'<tr><td height="6" bgcolor="{color}" style="background-color:{color};font-size:0;line-height:0;">&nbsp;</td></tr>',
        f'<tr><td style="padding:14px 20px 6px;">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td valign="top" style="font:13px/1.6 {FONT};color:{MUTED};padding-bottom:6px;">'
        f'FROM&nbsp; <strong style="color:{INK};">{e(s.segment)}</strong><br>'
        f'FOR&nbsp;&nbsp;&nbsp;&nbsp; <strong style="color:{INK};">{e(s.audience)}</strong></td>'
        f'</tr><tr><td>{pill(label.upper(), color)}{pill(s.stage.upper(), color, solid=False)}</td></tr></table>'
        f'</td></tr>',
        f'<tr><td style="padding:6px 20px 16px;"><table role="presentation" width="100%" cellpadding="0" '
        f'cellspacing="0" border="0"><tr><td bgcolor="{box_tint}" style="background-color:{box_tint};'
        f'border-left:6px solid {box_color};padding:12px 14px;">'
        f'<div style="font:700 12px/1.3 {FONT};color:{box_color};letter-spacing:.08em;padding-bottom:6px;">'
        f'{box_label}</div><div style="font:700 19px/1.4 {FONT};color:{INK};">{e(box_text)}</div>'
        f'</td></tr></table></td></tr>',
        section("What happened", s.what),
        f'<tr><td style="padding:0 20px 16px;">{facts_table(s)}</td></tr>',
        section("Why it matters", s.why),
        section("What happens next", s.next),
        f'<tr><td style="padding:2px 20px 16px;"><table role="presentation" width="100%" cellpadding="0" '
        f'cellspacing="0" border="0"><tr><td bgcolor="#F9FAFB" style="background-color:#F9FAFB;border:1px solid '
        f'{LINE};padding:10px 12px;"><div style="font:700 12px/1.3 {FONT};color:{SOFT_INK};padding-bottom:6px;">'
        f'TECHNICAL DETAILS (for the operator: {e(s.segment_tech)})</div><table role="presentation" '
        f'cellpadding="0" cellspacing="0" border="0">{tech_rows(s)}</table></td></tr></table></td></tr>',
        footer_row(),
    ]
    pre = f"{s.segment} · {_for(s.audience)} · {label} · {s.stage}"
    return preheader(pre) + shell("".join(rows), card_style=f"border:1px solid {LINE};")


# ------------------------------------------------------------------------------------------------ prototype B
def email_b(s: Sample) -> str:
    """B, "Plain note": a memo header (From / For / About / Status), then a short note in plain words."""
    color, label, _tint = SEV[s.sev]
    about = s.facts[0][1] if s.key not in ("halt", "daily", "catchup") else {
        "halt": "Account halt level", "daily": "Trading day Fri 20 Nov", "catchup": "StoneX connection"}[s.key]
    head = [("From", s.segment), ("For", s.audience), ("About", about)]
    memo = "".join(
        f'<tr><td width="70" valign="top" style="padding:3px 0;font:13px/1.5 {FONT};color:{SOFT_INK};">{e(k)}</td>'
        f'<td style="padding:3px 0;font:600 14px/1.5 {FONT};color:{INK};">{e(v)}</td></tr>' for k, v in head)
    memo += (f'<tr><td width="70" valign="top" style="padding:3px 0;font:13px/1.5 {FONT};color:{SOFT_INK};">Status</td>'
             f'<td style="padding:3px 0;font:700 14px/1.5 {FONT};color:{color};">{e(label)} · {e(s.stage)}'
             f'{" · action for you" if s.action_needed else " · no action"}</td></tr>')
    paras = "".join(f'<p style="margin:0 0 14px;font:15px/1.6 {FONT};color:{INK};">{e(p)}</p>' for p in s.letter)
    details = " · ".join(f"{k}: {v}" for k, v in s.tech)
    rows = [
        f'<tr><td style="padding:18px 22px 12px;border-bottom:1px solid {LINE};">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">{memo}</table></td></tr>',
        f'<tr><td style="padding:18px 22px 4px;"><p style="margin:0 0 14px;font:700 17px/1.5 {FONT};color:{INK};">'
        f'{e(s.letter_ask)}</p>{paras}<p style="margin:0 0 16px;font:14px/1.6 {FONT};color:{MUTED};">'
        f'XTNL, watching your StoneX account</p></td></tr>',
        f'<tr><td style="padding:0 22px 16px;font:11px/1.6 {MONO};color:{SOFT_INK};word-break:break-all;">'
        f'For the operator ({e(s.segment_tech)}): {e(details)}</td></tr>',
        footer_row(),
    ]
    pre = s.letter[0][:110]
    return preheader(pre) + shell("".join(rows), card_style=f"border-left:4px solid {color};")


# ------------------------------------------------------------------------------------------------ prototype C
def _stage_strip(s: Sample, color: str) -> str:
    cells = []
    for st in s.lifecycle:
        bar = color if st.state == "now" else "#667085" if st.state == "done" else "#D0D5DD"
        weight = "700" if st.state == "now" else "400"
        ink = INK if st.state != "later" else SOFT_INK
        mark = "now" if st.state == "now" else "done" if st.state == "done" else "later"
        cells.append(
            f'<td width="33%" valign="top" style="padding:0 3px;"><div style="height:6px;line-height:6px;'
            f'font-size:0;background-color:{bar};">&nbsp;</div><div style="padding-top:5px;font:{weight} 13px/1.35 '
            f'{FONT};color:{ink};">{e(st.label)}</div><div style="font:12px/1.35 {FONT};color:{SOFT_INK};">'
            f'{e(st.time)} · {mark}</div></td>')
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'{"".join(cells)}</tr></table>')


def _ladder(level: int, color: str) -> str:
    cells = []
    for i, (name, hint) in enumerate(zip(LADDER, LADDER_HINT, strict=True)):
        on = i == level
        bg = color if on else "#FFFFFF"
        ink = "#FFFFFF" if on else MUTED
        cells.append(
            f'<td width="33%" align="center" bgcolor="{bg}" style="background-color:{bg};border:1px solid '
            f'{color if on else "#D0D5DD"};padding:8px 4px;"><div style="font:700 14px/1.3 {FONT};color:{ink};">'
            f'{e(name)}{" (now)" if on else ""}</div><div style="font:12px/1.3 {FONT};color:{ink};">{e(hint)}</div></td>')
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="4" border="0"><tr>'
            f'{"".join(cells)}</tr></table>')


def email_c(s: Sample) -> str:
    """C, "Status ticket": coloured status band (ACT / FYI / DONE), 2 x 2 label grid, numbered to-do, life strip."""
    color, label, tint = SEV[s.sev]

    def chip(k: str, v: str) -> str:
        return (f'<td width="50%" valign="top" style="padding:4px;"><div style="border:1px solid {LINE};'
                f'background-color:#F9FAFB;padding:6px 8px;"><div style="font:700 11px/1.3 {FONT};color:{SOFT_INK};'
                f'letter-spacing:.06em;">{e(k)}</div><div style="font:600 14px/1.35 {FONT};color:{INK};">{e(v)}'
                f'</div></div></td>')

    grid = (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">'
            f'<tr>{chip("FROM", s.segment)}{chip("FOR", s.audience)}</tr>'
            f'<tr>{chip("SEVERITY", label)}{chip("STAGE", s.stage)}</tr></table>')
    steps = ""
    if s.steps:
        items = "".join(
            f'<tr><td width="30" valign="top" style="padding:0 0 8px;"><div style="width:22px;height:22px;'
            f'line-height:22px;text-align:center;border:2px solid {color};border-radius:4px;font:700 13px {FONT};'
            f'color:{color};">{n}</div></td><td valign="top" style="padding:2px 0 8px;font:15px/1.45 {FONT};'
            f'color:{INK};">{e(t)}</td></tr>' for n, t in enumerate(s.steps, 1))
        head = "TO DO" if s.action_needed else "TO CHECK"
        steps = (f'<tr><td style="padding:4px 20px 6px;"><div style="font:700 12px/1.3 {FONT};color:{SOFT_INK};'
                 f'letter-spacing:.06em;padding-bottom:8px;">{head}</div><table role="presentation" width="100%" '
                 f'cellpadding="0" cellspacing="0" border="0">{items}</table></td></tr>')
    else:
        steps = (f'<tr><td style="padding:4px 20px 12px;"><div style="background-color:{tint};padding:10px 12px;'
                 f'font:600 15px/1.45 {FONT};color:{INK};">{e(s.action)}</div></td></tr>')
    visual = (f'<div style="font:700 12px/1.3 {FONT};color:{SOFT_INK};letter-spacing:.06em;padding-bottom:8px;">'
              f'{"HALT LEVEL" if s.ladder is not None else "THIS ALERT SO FAR"}</div>'
              + (_ladder(s.ladder, color) if s.ladder is not None else _stage_strip(s, color)))
    cells = [f'<td width="50%" valign="top" style="padding:4px 4px 8px;"><div style="font:12px/1.3 {FONT};'
             f'color:{SOFT_INK};">{e(k)}</div><div style="font:600 14px/1.35 {FONT};color:{INK};">{e(v)}</div></td>'
             for k, v in s.facts]
    facts = "</tr><tr>".join("".join(cells[i:i + 2]) for i in range(0, len(cells), 2))
    tech = " | ".join(f"{k}: {v}" for k, v in s.tech)
    rows = [
        f'<tr><td bgcolor="{color}" style="background-color:{color};padding:14px 20px;">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td width="78" valign="middle"><div style="border:2px solid #FFFFFF;border-radius:6px;width:66px;'
        f'text-align:center;font:800 20px/38px {FONT};color:#FFFFFF;letter-spacing:.05em;">{e(s.act)}</div></td>'
        f'<td valign="middle" style="font:13px/1.4 {FONT};color:#FFFFFF;">'
        f'<div style="font:700 15px/1.35 {FONT};color:#FFFFFF;">{e(s.segment)}</div>'
        f'{"Action for you" if s.action_needed else "No action needed"}</td></tr></table></td></tr>',
        f'<tr><td style="padding:16px 20px 8px;font:700 21px/1.35 {FONT};color:{INK};">{e(s.title_c)}</td></tr>',
        f'<tr><td style="padding:0 16px 10px;">{grid}</td></tr>',
        steps,
        f'<tr><td style="padding:6px 20px 14px;">{visual}</td></tr>',
        f'<tr><td style="padding:0 16px 4px;"><table role="presentation" width="100%" cellpadding="0" '
        f'cellspacing="0" border="0"><tr>{facts}</tr></table></td></tr>',
        f'<tr><td style="padding:0 20px 12px;font:15px/1.55 {FONT};color:{INK};"><strong>Why:</strong> '
        f'{e(s.why)}</td></tr>',
        f'<tr><td style="padding:0 20px 14px;font:15px/1.55 {FONT};color:{INK};"><strong>Next:</strong> '
        f'{e(s.next)}</td></tr>',
        f'<tr><td style="padding:0 20px 14px;"><div style="border-top:1px dashed #D0D5DD;padding-top:8px;'
        f'font:11px/1.6 {MONO};color:{SOFT_INK};word-break:break-all;">OPERATOR · {e(s.segment_tech)} | '
        f'{e(tech)}</div></td></tr>',
        footer_row(),
    ]
    pre = f"{s.act} · {s.segment} · {_for(s.audience)} · {s.stage}"
    return preheader(pre) + shell("".join(rows), card_style=f"border:1px solid {LINE};")


# ------------------------------------------------------------------------------------------------ comparison page
PROTOS = {
    "A": ("Action card", "prototype-A-action-card.html", email_a,
          "Opens with one big coloured box: DO THIS NOW or NO ACTION NEEDED. Then three short labelled sections "
          "(what happened, why it matters, what happens next), a small facts table and a grey technical box for "
          "the operator."),
    "B": ("Plain note", "prototype-B-plain-note.html", email_b,
          "Reads like a short note from an assistant. A memo header says From / For / About / Status, the first "
          "bold line is the request, and two short paragraphs explain. Almost no colour; technical ids are one "
          "small line at the bottom."),
    "C": ("Status ticket", "prototype-C-status-ticket.html", email_c,
          "A coloured status band with one word (ACT, FYI or DONE) and the segment, a 2 x 2 label grid "
          "(from, for, severity, stage), numbered to-do steps, and a strip that shows where this alert is in its "
          "life (new, reminder, resolved) or where the halt level is."),
}


def inbox_row(subject: str, pre_text: str) -> str:
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="max-width:600px;margin:0 auto 6px;background-color:#FFFFFF;border:1px solid #D0D5DD;'
            f'table-layout:fixed;">'
            f'<tr><td style="padding:8px 12px;font:13px/1.45 {FONT};color:{INK};">'
            f'<div style="font:12px/1.3 {FONT};color:{SOFT_INK};">Inbox row</div>'
            f'<strong>XTNL Alerts</strong><br><strong>{e(subject)}</strong>'
            f'<div style="color:{SOFT_INK};white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'
            f'{e(pre_text)}</div></td></tr></table>')


def push_row(title: str, body: str) -> str:
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="max-width:600px;margin:0 auto 6px;background-color:#1D2939;border-radius:10px;">'
            f'<tr><td style="padding:8px 12px;font:13px/1.4 {FONT};color:#FFFFFF;">'
            f'<div style="font:11px/1.3 {FONT};color:#98A2B3;">Phone push (ntfy), sent for Important and Urgent only</div>'
            f'<strong>{e(title)}</strong><br><span style="color:#D0D5DD;">{e(body)}</span></td></tr></table>')


def preview_text(p: str, s: Sample) -> str:
    if p == "B":
        return s.letter[0]
    if p == "C":
        return f"{s.act} · {s.segment} · {_for(s.audience)} · {s.stage}"
    return f"{s.segment} · {_for(s.audience)} · {SEV[s.sev][1]} · {s.stage}"


def build(p: str) -> str:
    name, fname, render, blurb = PROTOS[p]
    nav = " · ".join(
        (f"<strong>{q}: {e(PROTOS[q][0])}</strong>" if q == p else
         f'<a href="{PROTOS[q][1]}" style="color:#175CD3;">{q}: {e(PROTOS[q][0])}</a>') for q in PROTOS)
    parts = [
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<meta name="color-scheme" content="light">'
        f'<title>Alert email prototype {p}: {e(name)}</title></head>'
        f'<body style="margin:0;padding:0;background-color:#E4E7EC;">'
        f'<div style="max-width:640px;margin:0 auto;padding:20px 12px;font:14px/1.5 {FONT};color:{INK};">'
        f'<div style="font:12px/1.4 {FONT};color:{SOFT_INK};">XTNL alert email redesign, design prototype (not wired '
        f'into the engine). Made-up sample data.</div>'
        f'<h1 style="margin:6px 0 4px;font:700 24px/1.3 {FONT};">Prototype {p}: {e(name)}</h1>'
        f'<p style="margin:0 0 8px;">{e(blurb)}</p>'
        f'<p style="margin:0 0 18px;font-size:13px;">{nav} · README.md compares them.</p>'
        f'<p style="margin:0 0 18px;font-size:13px;color:{MUTED};">The same six alerts appear in every prototype. '
        f'Each sample shows the inbox row, the phone push where one is sent, then the email itself (the part inside '
        f'the grey frame is what the email sends).</p>',
    ]
    for n, s in enumerate(SAMPLES, 1):
        parts.append(
            f'<h2 style="margin:28px 0 8px;font:700 17px/1.35 {FONT};color:{INK};">{n} of {len(SAMPLES)}: '
            f'{e(s.caption)}</h2>')
        parts.append(inbox_row(s.subject[p], preview_text(p, s)))
        if p in s.push:
            parts.append(push_row(*s.push[p]))
        parts.append(f'<div style="border:1px dashed #98A2B3;">{render(s)}</div>')
    parts.append('</div></body></html>\n')
    return "\n".join(parts)


def main() -> None:
    for p, (_name, fname, _render, _blurb) in PROTOS.items():
        (HERE / fname).write_text(build(p), encoding="utf-8")
        print(f"wrote {fname}")


if __name__ == "__main__":
    main()
