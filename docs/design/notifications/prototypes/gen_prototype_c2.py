"""Generate `prototype-C2-status-ticket.html`: prototype C, second iteration (spec: `prototype-C2-spec.md`).

    python docs/design/notifications/prototypes/gen_prototype_c2.py

Design prototype only: nothing in `src/` imports this and it imports nothing from `src/`. Every value is made up.
No account size, balance, P&L, money amount, pip / point size, spread or price level appears (the ARM sample shows a
placeholder where the real email would show the proposed order level).

What changed from C v1 (prototype-C-status-ticket.html):
* Four status words that also say who acts: ACT (you, at StoneX), FIX (the operator, on the server or the broker
  setup), FYI (nobody), DONE (closed). The colour is the severity, so ACT can be orange (Important) or red (Urgent).
* One header band (status word, segment, audience) replaces the band plus the 2 x 2 grid of v1.
* A meta line (severity, stage, time) and a "Linked" line that ties an alert to its cause or its follow-up.
* Each to-do step says where it is done (at StoneX, on the server PC, nowhere: just wait).
* A reminder keeps its first alert's subject, so a phone mail app groups the reminders in one thread.
* 13 samples covering all six segments, including HARD, FIX and ARM advice.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from gen_prototypes import (  # noqa: E402
    FONT, INK, LINE, MONO, MUTED, SEV, SOFT_INK, e, preheader, shell,
)

OUT = HERE / "prototype-C2-status-ticket.html"
READ_ONLY = ("XTNL order-server, Phase 1 (read-only). It watches your StoneX account and never places, changes or "
             "cancels orders. Any change is yours to make at StoneX.")
TIME_NOTE = "Times are Melbourne time (AEDT); the UTC time is in the operator details."

#: Who acts, per status word (the word is chosen by who acts; the colour by the severity).
WORDS = {
    "ACT": "Action for you",
    "FIX": "Action for the operator",
    "FYI": "No action needed",
    "DONE": "Closed, no action needed",
}
#: Where a to-do step is done.
PLACES = {
    "stonex": ("At StoneX", "#C4320A"),
    "server": ("Server PC", "#3538CD"),
    "wait": ("Just wait", "#475467"),
    "check": ("Check", "#175CD3"),
}
LADDER = (("NORMAL", "trade as usual"), ("SOFT", "don't add new risk"), ("HARD", "stop all trading"))


def for_whom(audience: str) -> str:
    """'for You, the trader'; an audience that already starts with 'For' is kept as is ('For your records')."""
    return audience if audience.lower().startswith("for ") else f"for {audience}"


@dataclass(frozen=True)
class Stage:
    label: str
    time: str
    state: str   # done | now | later


@dataclass(frozen=True)
class Alert:
    caption: str
    word: str                    # ACT | FIX | FYI | DONE
    sev: str                     # SEV key
    segment: str
    audience: str
    stage: str
    when: str                    # Melbourne time of this email
    title: str                   # the headline: an action, or what closed
    subject: str
    steps: tuple[tuple[str, str], ...] = ()   # (place key, text)
    nothing: str = ""            # when there are no steps: the one-line "nothing to do" text
    why: str = ""
    next: str = ""
    facts: tuple[tuple[str, str], ...] = ()
    linked: str = ""
    life: tuple[Stage, ...] = ()
    ladder: int | None = None
    ladder_from: int | None = None
    tech: tuple[tuple[str, str], ...] = ()
    push: tuple[str, str] | None = None
    thread: int = 1              # emails in this inbox thread (a reminder joins its first alert's thread)
    placeholder: tuple[str, str] | None = None   # (label, text) a value the prototype hides on purpose


ALERTS: tuple[Alert, ...] = (
    # ---------------------------------------------------------------- Risk check + Halt level: one story
    Alert(
        caption="Risk check, new: an order has no stop loss",
        word="ACT", sev="important", segment="Risk check", audience="You, the trader", stage="New",
        when="Fri 20 Nov, 20:14", title="Add a stop loss to order ORD-DEMO-2 (EUR/USD)",
        subject="[ACT] Risk check: add a stop loss to ORD-DEMO-2",
        steps=(("stonex", "Open order ORD-DEMO-2 (EUR/USD) and attach a stop loss."),
               ("wait", "Nothing else. This alert closes by itself once the server sees the stop.")),
        why=("Without a stop, the loss this order can make has no limit. While it is open the account is on a soft "
             "halt, and if the stop stays missing the halt can rise to HARD."),
        next="Reminder every 30 minutes until the stop is seen (next at 20:44). Then a DONE email.",
        facts=(("Order", "ORD-DEMO-2"), ("Symbol", "EUR/USD"), ("Problem", "No stop loss"), ("Open since", "20:14")),
        linked="This also raised the account's halt level to SOFT (separate email, 20:14).",
        life=(Stage("New", "20:14", "now"), Stage("Reminders", "every 30 min", "later"),
              Stage("Closed", "when fixed", "later")),
        tech=(("Rule", "missing_stop_loss"), ("Level", "SOFT_HALT, cap NONE"), ("Subject", "order:ORD-DEMO-2"),
              ("Alert", "episode, HIGH, alert id 5512"), ("Sent", "2026-11-20 09:14:03 UTC"),
              ("Old subject", "[XTNL HIGH] SOFT_HALT missing_stop_loss order:ORD-DEMO-2")),
        push=("ACT · Add a stop loss to ORD-DEMO-2", "Risk check · for you · Important · new"),
    ),
    Alert(
        caption="Halt level rises to SOFT (same moment, its own email)",
        word="ACT", sev="important", segment="Halt level", audience="You, the trader", stage="Level up",
        when="Fri 20 Nov, 20:14", title="Hold off on new trades: the halt level is now SOFT",
        subject="[ACT] Halt level: NORMAL to SOFT, hold off on new trades",
        steps=(("stonex", "Don't open new trades or add to positions for now."),
               ("stonex", "Fix the cause: add a stop loss to order ORD-DEMO-2 (EUR/USD)."),
               ("wait", "Wait for the DONE email. The level goes back to NORMAL after a short cool-down.")),
        why=("SOFT means: don't add new risk. The server places no trades, so nothing is blocked at StoneX. "
             "The halt is a signal for you."),
        next="If the cause stays open, the level can rise to HARD: stop all trading until the operator clears it.",
        facts=(("Halt level", "NORMAL to SOFT"), ("Cause", "No stop loss on ORD-DEMO-2"), ("Since", "20:14")),
        linked="Cause: Risk check, no stop loss on ORD-DEMO-2 (separate email, 20:14).",
        ladder=1, ladder_from=0,
        tech=(("Event", "HALT SOFT (trip), NONE to SOFT"), ("Cause", "missing_stop_loss order:ORD-DEMO-2"),
              ("Halt event id", "881"), ("Alert", "trip, HIGH, alert id 5513"), ("Sent", "2026-11-20 09:14:04 UTC"),
              ("Old subject", "[XTNL HIGH] HALT SOFT")),
        push=("ACT · Halt level SOFT: hold off on new trades", "Cause: no stop loss on ORD-DEMO-2"),
    ),
    Alert(
        caption="Risk check, 30-minute reminder (joins the first alert's thread)",
        word="ACT", sev="important", segment="Risk check", audience="You, the trader", stage="Reminder 1, open 30 min",
        when="Fri 20 Nov, 20:44", title="Still open: add a stop loss to order ORD-DEMO-2 (EUR/USD)",
        subject="[ACT] Risk check: add a stop loss to ORD-DEMO-2",
        steps=(("stonex", "Open order ORD-DEMO-2 (EUR/USD) and attach a stop loss."),
               ("server", "If the missing stop is on purpose: the operator acknowledges this alert to stop the "
                          "reminders.")),
        why="The account stays on a soft halt while this is open, and the halt can rise to HARD.",
        next="Next reminder at 21:14.",
        facts=(("Order", "ORD-DEMO-2"), ("Symbol", "EUR/USD"), ("Open since", "20:14 (30 min)"),
               ("Next reminder", "21:14")),
        linked="Same alert as the 20:14 email; this email is in its thread.",
        life=(Stage("New", "20:14", "done"), Stage("Reminder 1", "20:44", "now"), Stage("Closed", "when fixed", "later")),
        tech=(("Rule", "missing_stop_loss"), ("Level", "SOFT_HALT, cap NONE"), ("Subject", "order:ORD-DEMO-2"),
              ("Alert", "repage 1, HIGH, alert id 5519"), ("Sent", "2026-11-20 09:44:03 UTC"),
              ("Old subject", "[XTNL HIGH] still SOFT_HALT missing_stop_loss order:ORD-DEMO-2 (cap NONE)")),
        push=("ACT · Reminder 1: ORD-DEMO-2 still has no stop", "Risk check · for you · open 30 min"),
        thread=2,
    ),
    Alert(
        caption="Risk check, closed",
        word="DONE", sev="resolved", segment="Risk check", audience="You, the trader", stage="Closed",
        when="Fri 20 Nov, 20:51", title="ORD-DEMO-2 has a stop loss now",
        subject="[DONE] Risk check: ORD-DEMO-2 has a stop loss",
        nothing="Nothing to do. The stop is in place.",
        why="The loss this order can make is limited again.",
        next="The halt level goes back to NORMAL after a short cool-down if nothing else is open (separate email).",
        facts=(("Order", "ORD-DEMO-2"), ("Symbol", "EUR/USD"), ("Fixed at", "20:51"), ("Was open", "37 min")),
        life=(Stage("New", "20:14", "done"), Stage("Reminder 1", "20:44", "done"), Stage("Closed", "20:51", "now")),
        tech=(("Rule", "missing_stop_loss"), ("Transition", "resolved, was SOFT_HALT"), ("Subject", "order:ORD-DEMO-2"),
              ("Alert", "info, INFO, alert id 5523"), ("Sent", "2026-11-20 09:51:22 UTC"),
              ("Old subject", "[XTNL INFO] resolved missing_stop_loss order:ORD-DEMO-2")),
    ),
    Alert(
        caption="Halt level back to NORMAL after the cool-down",
        word="DONE", sev="resolved", segment="Halt level", audience="You, the trader", stage="Level down",
        when="Fri 20 Nov, 21:06", title="Halt level is back to NORMAL: trade as usual",
        subject="[DONE] Halt level: back to NORMAL",
        nothing="Nothing to do. You can trade as usual.",
        why="Nothing that raises the halt has been open since 20:51, and the cool-down has passed.",
        facts=(("Halt level", "SOFT to NORMAL"), ("Was SOFT", "20:14 to 21:06"), ("Cleared by", "the system")),
        linked="Ends the 20:14 halt (cause: no stop loss on ORD-DEMO-2).",
        ladder=0, ladder_from=1,
        tech=(("Event", "HALT cleared (cool_down), SOFT to NONE"), ("Halt event id", "884"),
              ("Alert", "info, INFO, alert id 5531"), ("Sent", "2026-11-20 10:06:30 UTC"),
              ("Old subject", "[XTNL INFO] HALT cleared")),
    ),
    Alert(
        caption="Halt level rises to HARD (a different day: the daily loss limit)",
        word="ACT", sev="urgent", segment="Halt level", audience="You, the trader", stage="Level up",
        when="Fri 20 Nov, 22:31", title="Stop trading for today: the halt level is HARD",
        subject="[ACT] Halt level: HARD, stop trading for today",
        steps=(("stonex", "Don't open or add to any trade for the rest of the trading day."),
               ("stonex", "What to do with trades already open is your call, at StoneX."),
               ("server", "The operator reviews the day, then clears the halt. It does not clear by itself.")),
        why="Today's loss went past the daily loss limit. HARD is the highest level.",
        next="Until the operator clears it, the level stays HARD, including after a restart.",
        facts=(("Halt level", "SOFT to HARD"), ("Cause", "Daily loss limit"), ("Trading day", "Fri 20 Nov"),
               ("Since", "22:31")),
        linked="Cause: Daily account check, daily loss limit (separate email, 22:31).",
        ladder=2, ladder_from=1,
        tech=(("Event", "HALT HARD (trip), SOFT to HARD"), ("Cause", "max_daily_loss account:ACC-DEMO-1:2026-11-20"),
              ("Halt event id", "902"), ("Alert", "trip, CRITICAL, alert id 5560"),
              ("Sent", "2026-11-20 11:31:12 UTC"), ("Old subject", "[XTNL CRITICAL] HALT HARD")),
        push=("ACT · URGENT: stop trading for today", "Halt level HARD · cause: daily loss limit"),
    ),
    # ---------------------------------------------------------------- Daily account check
    Alert(
        caption="Daily account check: heads-up (FLAG)",
        word="FYI", sev="headsup", segment="Daily account check", audience="For your records", stage="New, for today",
        when="Fri 20 Nov, 08:01", title="Today's deposits and withdrawals are not confirmed",
        subject="[FYI] Daily check: deposits and withdrawals not confirmed for 20 Nov",
        steps=(("check", "Only if you deposited or withdrew money today: check today's result at StoneX yourself."),),
        why=("StoneX did not report today's deposits and withdrawals, so the daily loss check cannot tell a transfer "
             "apart from a trading result. The limit still runs; nothing is halted."),
        next="It ends with the trading day. Repeated every 30 minutes only while you have open trades.",
        facts=(("Trading day", "Fri 20 Nov"), ("Check", "Daily loss limit"), ("Missing", "Deposits and withdrawals")),
        life=(Stage("New", "08:01", "now"), Stage("Repeats", "only with open trades", "later"),
              Stage("Ends", "end of trading day", "later")),
        tech=(("Rule", "max_daily_loss"), ("Level", "FLAG, reason transfers_unverified"),
              ("Subject", "account:ACC-DEMO-1:2026-11-20"), ("Alert", "episode, INFO, alert id 5470"),
              ("Sent", "2026-11-19 21:01:05 UTC"),
              ("Old subject", "[XTNL INFO] FLAG max_daily_loss account:ACC-DEMO-1:2026-11-20")),
    ),
    # ---------------------------------------------------------------- Broker connection and data
    Alert(
        caption="Broker data: caught up after downtime",
        word="FYI", sev="info", segment="Broker connection and data", audience="For your records", stage="Update",
        when="Fri 20 Nov, 07:42", title="Back in sync with StoneX after 46 min offline",
        subject="[FYI] Broker data: caught up after 46 min offline",
        nothing="Nothing to do. Nothing is missing.",
        why="The server read everything that changed at StoneX while it was offline, so the risk checks have complete "
            "data again.",
        next="If a catch-up ever cannot finish, you get a FIX email instead.",
        facts=(("Offline", "06:55 to 07:41 (46 min)"), ("Why", "PC was asleep"), ("Orders", "0 new, 2 updated"),
               ("Positions", "0 new, 1 closed")),
        life=(Stage("Offline", "06:55", "done"), Stage("Back", "07:41", "done"), Stage("Caught up", "07:42", "now")),
        tech=(("Adapter", "stonex"), ("Catch-up", "reason wake, complete"),
              ("Offline window", "2026-11-19 19:55Z .. 20:41Z"), ("Alert", "info, INFO, alert id 5466"),
              ("Sent", "2026-11-19 20:42:10 UTC"), ("Old subject", "[XTNL INFO] downtime catch-up: stonex")),
    ),
    Alert(
        caption="Broker data: catch-up could not finish (operator)",
        word="FIX", sev="important", segment="Broker connection and data", audience="Operator", stage="New",
        when="Sat 21 Nov, 04:02", title="Check StoneX order history for 02:10 to 03:25",
        subject="[FIX] Broker data: catch-up incomplete, check StoneX history 02:10 to 03:25",
        steps=(("stonex", "Open the StoneX order history for 02:10 to 03:25, the part the server could not read."),
               ("check", "Look for orders or fills there that the server may have missed."),
               ("wait", "The server retries at the next reconnect or start. If StoneX no longer keeps that history, "
                        "your check is the only cover for the gap.")),
        why=("The risk checks may be missing changes from that window. Until a full catch-up, the server treats the "
             "account as 'don't add new risk'."),
        next="A FYI email when a later catch-up completes.",
        facts=(("Gap", "02:10 to 03:25 (75 min)"), ("Read", "4 orders, 2 positions"), ("Odd records", "1"),
               ("Why", "StoneX history incomplete")),
        tech=(("Adapter", "stonex"), ("Catch-up", "reason reconnect, INCOMPLETE (retention)"),
              ("Uncovered window", "2026-11-20 15:10Z .. 16:25Z"), ("Anomaly ids", "an-7"),
              ("Alert", "page, HIGH, alert id 5588"), ("Sent", "2026-11-20 17:02:40 UTC"),
              ("Old subject", "[XTNL HIGH] downtime catch-up incomplete: stonex")),
        push=("FIX · Catch-up incomplete: check StoneX history", "02:10 to 03:25 · for the operator"),
    ),
    Alert(
        caption="Broker connection: StoneX refused the login (operator)",
        word="FIX", sev="urgent", segment="Broker connection and data", audience="Operator", stage="New",
        when="Sat 21 Nov, 05:12", title="StoneX refused the server's login: fix the read-only credential",
        subject="[FIX] Broker connection: StoneX refused the login",
        steps=(("stonex", "Check the read-only StoneX credential: expired, changed or locked?"),
               ("server", "Put the working credential in place on the server PC."),
               ("server", "Re-arm the StoneX connection (operator runbook).")),
        why=("Until it logs in again, the server gets no new orders, positions or prices from StoneX, so the risk checks "
             "run on old data."),
        next="It does not retry a refused login by itself. A DONE email when the connection is re-armed.",
        facts=(("Broker", "StoneX"), ("Since", "05:12"), ("Data from StoneX", "Stopped")),
        tech=(("Adapter", "stonex"), ("State", "AUTH_FAILED, SM4 stopped"), ("Alert", "page, CRITICAL, alert id 5590"),
              ("Sent", "2026-11-20 18:12:05 UTC"), ("Old subject", "[XTNL CRITICAL] broker AUTH_FAILED: stonex")),
        push=("FIX · URGENT: StoneX refused the login", "No new data from StoneX · for the operator"),
    ),
    # ---------------------------------------------------------------- System health
    Alert(
        caption="System health: the server stopped recording (operator)",
        word="FIX", sev="urgent", segment="System health", audience="Operator", stage="New",
        when="Sat 21 Nov, 03:40", title="The server stopped recording StoneX: check the server PC",
        subject="[FIX] System health: the server is not recording",
        steps=(("server", "On the server PC, run deploy/local/status-xtnl.ps1."),
               ("server", "Read the newest log for the cause."),
               ("wait", "It tries to recover by itself. If it has not within a few minutes, restart it by hand.")),
        why="While it is not recording, no risk check runs and no alert about your trades is sent.",
        next="A DONE email when it records again.",
        facts=(("Since", "03:40"), ("Machine", "Server PC (single PC)")),
        life=(Stage("Stopped", "03:40", "now"), Stage("Recovering", "automatic", "later"),
              Stage("Recording", "DONE email", "later")),
        tech=(("Node", "single PC, no leader term"), ("Alert", "direct page, CRITICAL, no outbox id"),
              ("Sent", "2026-11-20 16:40:00 UTC"),
              ("Old subject", "[XTNL CRITICAL] engine not recording on the single PC")),
        push=("FIX · URGENT: the server is not recording", "No risk checks are running · for the operator"),
    ),
    Alert(
        caption="System health: recording again",
        word="DONE", sev="resolved", segment="System health", audience="Operator", stage="Closed",
        when="Sat 21 Nov, 03:52", title="The server is recording again after 12 min",
        subject="[DONE] System health: recording again",
        nothing="Nothing to do. Watch for it happening again.",
        why="Risk checks and alerts are running again. A catch-up email follows if anything changed at StoneX meanwhile.",
        facts=(("Stopped", "03:40 to 03:52 (12 min)"), ("Machine", "Server PC (single PC)")),
        life=(Stage("Stopped", "03:40", "done"), Stage("Recovering", "automatic", "done"),
              Stage("Recording", "03:52", "now")),
        tech=(("Node", "single PC"), ("Alert", "direct page, INFO, no outbox id"), ("Sent", "2026-11-20 16:52:00 UTC"),
              ("Old subject", "[XTNL INFO] engine recording again on the single PC")),
    ),
    # ---------------------------------------------------------------- ARM advice
    Alert(
        caption="ARM advice: move a stop (only when ARM is switched on)",
        word="ACT", sev="important", segment="ARM advice", audience="You, the trader", stage="Final advice",
        when="Fri 20 Nov, 21:16", title="Move the stop on order ORD-DEMO-5 (GBP/USD) to the ARM's level",
        subject="[ACT] ARM advice: move the stop on ORD-DEMO-5",
        steps=(("stonex", "Move the stop on order ORD-DEMO-5 (GBP/USD) to the proposed level below."),
               ("wait", "Or leave it: this is advice. The server changes nothing at StoneX.")),
        why="The ARM's stop rule says the stop can now sit closer, after the 21:15 candle closed.",
        next="The ARM checks again when the next candle closes.",
        facts=(("Order", "ORD-DEMO-5"), ("Symbol", "GBP/USD"), ("Advice", "Move the stop (final)"),
               ("Candle closed", "21:15")),
        placeholder=("Proposed stop", "the order level is shown here in the real email (hidden in this prototype)"),
        tech=(("Notice", "arm.post_adjust, FINAL ADVICE"), ("Route", "route-demo"), ("Notice id", "77"),
              ("Sent", "2026-11-20 10:16:20 UTC"), ("Old subject", "ARM subject: route-demo, ADJUST, notice #77")),
        push=("ACT · ARM: move the stop on ORD-DEMO-5", "GBP/USD · advice only"),
    ),
)


# ------------------------------------------------------------------------------------------------ email parts
def _label(text: str) -> str:
    return (f'<div style="font:700 12px/1.3 {FONT};color:{SOFT_INK};letter-spacing:.06em;padding-bottom:8px;">'
            f'{e(text)}</div>')


def _place_chip(key: str) -> str:
    text, color = PLACES[key]
    return (f'<span style="display:inline-block;padding:1px 6px;margin:0 0 3px;border:1px solid {color};'
            f'border-radius:3px;font:700 11px/1.4 {FONT};color:{color};letter-spacing:.03em;white-space:nowrap;">'
            f'{e(text.upper())}</span>')


def _steps(a: Alert, color: str, tint: str) -> str:
    if not a.steps:
        return (f'<tr><td style="padding:4px 20px 14px;"><div style="background-color:{tint};padding:10px 12px;'
                f'font:600 15px/1.45 {FONT};color:{INK};">{e(a.nothing)}</div></td></tr>')
    items = "".join(
        f'<tr><td width="32" valign="top" style="padding:0 0 10px;"><div style="width:22px;height:22px;'
        f'line-height:22px;text-align:center;border:2px solid {color};border-radius:4px;font:700 13px {FONT};'
        f'color:{color};">{n}</div></td><td valign="top" style="padding:1px 0 10px;">{_place_chip(place)}'
        f'<div style="font:15px/1.45 {FONT};color:{INK};">{e(text)}</div></td></tr>'
        for n, (place, text) in enumerate(a.steps, 1))
    head = "TO DO" if a.word in ("ACT", "FIX") else "ONLY IF IT APPLIES"
    return (f'<tr><td style="padding:4px 20px 4px;">{_label(head)}<table role="presentation" width="100%" '
            f'cellpadding="0" cellspacing="0" border="0">{items}</table></td></tr>')


def _life(a: Alert, color: str) -> str:
    cells = []
    for st in a.life:
        bar = color if st.state == "now" else "#667085" if st.state == "done" else "#D0D5DD"
        weight = "700" if st.state == "now" else "400"
        ink = SOFT_INK if st.state == "later" else INK
        mark = {"now": "this email", "done": "done", "later": "later"}[st.state]
        cells.append(
            f'<td width="33%" valign="top" style="padding:0 3px;"><div style="height:6px;line-height:6px;'
            f'font-size:0;background-color:{bar};">&nbsp;</div><div style="padding-top:5px;font:{weight} 13px/1.35 '
            f'{FONT};color:{ink};">{e(st.label)}</div><div style="font:12px/1.35 {FONT};color:{SOFT_INK};">'
            f'{e(st.time)}<br>{mark}</div></td>')
    return (f'{_label("THIS ALERT SO FAR")}<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'border="0"><tr>{"".join(cells)}</tr></table>')


def _ladder(a: Alert, color: str) -> str:
    assert a.ladder is not None
    cells = []
    for i, (name, hint) in enumerate(LADDER):
        on = i == a.ladder
        was = a.ladder_from is not None and i == a.ladder_from and not on
        bg = color if on else "#FFFFFF"
        ink = "#FFFFFF" if on else MUTED
        border = color if on else "#98A2B3" if was else "#D0D5DD"
        style = "dashed" if was else "solid"
        tag = " (now)" if on else " (was)" if was else ""
        cells.append(
            f'<td width="33%" align="center" bgcolor="{bg}" style="background-color:{bg};border:2px {style} {border};'
            f'padding:8px 4px;"><div style="font:700 14px/1.3 {FONT};color:{ink};">{e(name)}{tag}</div>'
            f'<div style="font:12px/1.3 {FONT};color:{ink};">{e(hint)}</div></td>')
    return (f'{_label("HALT LEVEL")}<table role="presentation" width="100%" cellpadding="0" cellspacing="4" '
            f'border="0"><tr>{"".join(cells)}</tr></table>')


def _facts(a: Alert) -> str:
    cells = [f'<td width="50%" valign="top" style="padding:4px 4px 8px;"><div style="font:12px/1.3 {FONT};'
             f'color:{SOFT_INK};">{e(k)}</div><div style="font:600 14px/1.35 {FONT};color:{INK};">{e(v)}</div></td>'
             for k, v in a.facts]
    rows = "</tr><tr>".join("".join(cells[i:i + 2]) for i in range(0, len(cells), 2))
    return (f'<tr><td style="padding:0 16px 4px;"><table role="presentation" width="100%" cellpadding="0" '
            f'cellspacing="0" border="0"><tr>{rows}</tr></table></td></tr>')


def _placeholder(a: Alert) -> str:
    if a.placeholder is None:
        return ""
    k, v = a.placeholder
    return (f'<tr><td style="padding:0 20px 12px;"><div style="border:2px dashed #98A2B3;padding:8px 10px;">'
            f'<div style="font:12px/1.3 {FONT};color:{SOFT_INK};">{e(k)}</div>'
            f'<div style="font:600 14px/1.4 {FONT};color:{MUTED};">{e(v)}</div></div></td></tr>')


def email(a: Alert) -> str:
    color, sev_label, tint = SEV[a.sev]
    tech = " | ".join(f"{k}: {v}" for k, v in a.tech)
    visual = _ladder(a, color) if a.ladder is not None else _life(a, color) if a.life else ""
    rows = [
        # band: status word + who acts | segment + audience
        f'<tr><td bgcolor="{color}" style="background-color:{color};padding:14px 18px;">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td width="80" valign="middle"><div style="border:2px solid #FFFFFF;border-radius:6px;width:68px;'
        f'text-align:center;font:800 20px/38px {FONT};color:#FFFFFF;letter-spacing:.05em;">{e(a.word)}</div></td>'
        f'<td valign="middle" style="font:13px/1.45 {FONT};color:#FFFFFF;">'
        f'<div style="font:700 16px/1.3 {FONT};color:#FFFFFF;">{e(a.segment)}</div>'
        f'For: <strong>{e(a.audience)}</strong><br>{e(WORDS[a.word])}</td></tr></table></td></tr>',
        # meta line
        f'<tr><td style="padding:12px 20px 0;font:13px/1.5 {FONT};color:{MUTED};">'
        f'<span style="display:inline-block;padding:2px 7px;margin-right:6px;border-radius:3px;'
        f'background-color:{tint};border:1px solid {color};color:{color};font:700 12px/1.3 {FONT};">'
        f'{e(sev_label.upper())}</span>{e(a.stage)} · {e(a.when)}</td></tr>',
        f'<tr><td style="padding:8px 20px 12px;font:700 21px/1.35 {FONT};color:{INK};">{e(a.title)}</td></tr>',
        _steps(a, color, tint),
        _placeholder(a),
        f'<tr><td style="padding:6px 20px 14px;">{visual}</td></tr>' if visual else "",
        _facts(a) if a.facts else "",
        f'<tr><td style="padding:0 20px 10px;font:15px/1.55 {FONT};color:{INK};"><strong>Why:</strong> '
        f'{e(a.why)}</td></tr>' if a.why else "",
        f'<tr><td style="padding:0 20px 10px;font:15px/1.55 {FONT};color:{INK};"><strong>Next:</strong> '
        f'{e(a.next)}</td></tr>' if a.next else "",
        f'<tr><td style="padding:2px 20px 12px;"><div style="border-left:3px solid #98A2B3;padding:4px 10px;'
        f'font:14px/1.5 {FONT};color:{MUTED};"><strong>Linked:</strong> {e(a.linked)}</div></td></tr>'
        if a.linked else "",
        f'<tr><td style="padding:2px 20px 14px;"><div style="border-top:1px dashed #D0D5DD;padding-top:8px;'
        f'font:11px/1.6 {MONO};color:{SOFT_INK};word-break:break-all;">FOR THE OPERATOR | {e(tech)}</div></td></tr>',
        f'<tr><td style="padding:14px 20px 18px;border-top:1px solid {LINE};font:12px/1.5 {FONT};color:{SOFT_INK};">'
        f'{e(READ_ONLY)}<br>{e(TIME_NOTE)}</td></tr>',
    ]
    pre = f"{a.word} · {a.segment} · {for_whom(a.audience)} · {sev_label} · {a.stage}"
    return preheader(pre) + shell("".join(rows), card_style=f"border:1px solid {LINE};")


# ------------------------------------------------------------------------------------------------ page
def _inbox(a: Alert) -> str:
    count = f' <span style="color:{SOFT_INK};font-weight:400;">({a.thread})</span>' if a.thread > 1 else ""
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="max-width:600px;margin:0 auto 6px;background-color:#FFFFFF;border:1px solid #D0D5DD;'
            f'table-layout:fixed;"><tr><td style="padding:8px 12px;font:13px/1.45 {FONT};color:{INK};">'
            f'<div style="font:12px/1.3 {FONT};color:{SOFT_INK};">Inbox row</div>'
            f'<strong>XTNL Alerts</strong>{count}<br><strong>{e(a.subject)}</strong>'
            f'<div style="color:{SOFT_INK};white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'
            f'{e(f"{a.word} · {a.segment} · {for_whom(a.audience)} · {SEV[a.sev][1]} · {a.stage}")}</div></td></tr></table>')


def _push(a: Alert) -> str:
    if a.push is None:
        return ""
    title, body = a.push
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="max-width:600px;margin:0 auto 6px;background-color:#1D2939;border-radius:10px;">'
            f'<tr><td style="padding:8px 12px;font:13px/1.4 {FONT};color:#FFFFFF;">'
            f'<div style="font:11px/1.3 {FONT};color:#98A2B3;">Phone push (ntfy), Important and Urgent only</div>'
            f'<strong>{e(title)}</strong><br><span style="color:#D0D5DD;">{e(body)}</span></td></tr></table>')


def _legend() -> str:
    def row(word: str, text: str, color: str) -> str:
        return (f'<tr><td width="64" style="padding:4px 8px 4px 0;"><span style="display:inline-block;width:52px;'
                f'text-align:center;padding:3px 0;border-radius:4px;background-color:{color};color:#FFFFFF;'
                f'font:800 13px/1.3 {FONT};">{word}</span></td><td style="padding:4px 0;font:14px/1.45 {FONT};'
                f'color:{INK};">{e(text)}</td></tr>')
    sev = " ".join(
        f'<span style="display:inline-block;margin:0 4px 4px 0;padding:2px 7px;border-radius:3px;'
        f'background-color:{c};color:#FFFFFF;font:700 12px/1.3 {FONT};">{e(lbl)}</span>'
        for c, lbl, _t in SEV.values())
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="max-width:600px;margin:0 auto 10px;background-color:#FFFFFF;border:1px solid #D0D5DD;">'
            f'<tr><td style="padding:12px 14px;"><div style="font:700 14px/1.4 {FONT};padding-bottom:6px;">'
            f'How to read the first word</div><table role="presentation" cellpadding="0" cellspacing="0" border="0">'
            f'{row("ACT", "You do something, at StoneX.", "#344054")}'
            f'{row("FIX", "The operator does something, on the server PC or the broker setup.", "#344054")}'
            f'{row("FYI", "Nobody needs to do anything. For your records.", "#344054")}'
            f'{row("DONE", "Something that was open has closed.", "#344054")}</table>'
            f'<div style="font:13px/1.45 {FONT};color:{MUTED};padding-top:8px;">The colour is the severity, '
            f'separately from the word:</div><div style="padding-top:4px;">{sev}</div></td></tr></table>')


def build() -> str:
    parts = [
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<meta name="color-scheme" content="light">'
        '<title>Alert email prototype C2: Status ticket</title></head>'
        f'<body style="margin:0;padding:0;background-color:#E4E7EC;">'
        f'<div style="max-width:640px;margin:0 auto;padding:20px 12px;font:14px/1.5 {FONT};color:{INK};">'
        f'<div style="font:12px/1.4 {FONT};color:{SOFT_INK};">XTNL alert email redesign, design prototype (not wired '
        f'into the engine). Made-up sample data.</div>'
        f'<h1 style="margin:6px 0 4px;font:700 24px/1.3 {FONT};">Prototype C2: Status ticket, second iteration</h1>'
        f'<p style="margin:0 0 8px;">The first word of every subject says who has to act. The coloured band repeats '
        f'it with the part of the system that sent the alert and who it is for. Then come numbered steps, each '
        f'saying where it is done, and a strip showing where the alert is in its life. 13 samples cover all six '
        f'segments.</p>'
        f'<p style="margin:0 0 14px;font-size:13px;">First version: <a href="prototype-C-status-ticket.html" '
        f'style="color:#175CD3;">prototype C</a> · Rules and the full alert map: prototype-C2-spec.md</p>',
        _legend(),
    ]
    for n, a in enumerate(ALERTS, 1):
        parts.append(f'<h2 style="margin:28px 0 8px;font:700 17px/1.35 {FONT};color:{INK};">{n} of {len(ALERTS)}: '
                     f'{e(a.caption)}</h2>')
        parts.append(_inbox(a))
        parts.append(_push(a))
        parts.append(f'<div style="border:1px dashed #98A2B3;">{email(a)}</div>')
    parts.append('</div></body></html>\n')
    return "\n".join(p for p in parts if p)


def main() -> None:
    OUT.write_text(build(), encoding="utf-8")
    print(f"wrote {OUT.name}")


if __name__ == "__main__":
    main()
