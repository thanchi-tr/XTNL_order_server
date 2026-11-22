"""Generate three developments of prototype C2: C3a Glance, C3b Timeline, C3c Status board.

    python docs/design/notifications/prototypes/gen_prototype_c3.py

Design prototypes only: nothing in `src/` imports this and it imports nothing from `src/`. Every value is made up.
No account size, balance, P&L, money amount, pip / point size, spread or price level appears.

All three keep C2's rules (ACT / FIX / FYI / DONE, segment, audience, severity colour, place chips, threaded
reminders; `prototype-C2-spec.md`) and show the same 8 alerts, taken from C2's samples:

* C3a Glance: everything you need on the first phone screen. The headline sits inside the coloured band, the
  first step is a big box, and the rest is one or two short lines.
* C3b Timeline: the alert's whole story in the email: what is coming next, this email, and what happened before
  (including linked halt changes). Made for threaded reminders.
* C3c Status board: the C2 ticket, then a board of the whole account at send time (halt level, open alerts by
  status word, StoneX data, server). Counts and states only.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from gen_prototype_c2 import (  # noqa: E402
    ALERTS, LADDER, READ_ONLY, TIME_NOTE, WORDS, Alert, for_whom, _facts, _inbox, _label, _ladder, _place_chip,
    _placeholder, _push, _steps,
)
from gen_prototypes import FONT, INK, LINE, MONO, MUTED, SEV, SOFT_INK, e, preheader, shell  # noqa: E402

#: The 8 samples, by their index in C2's ALERTS: every status word, five segments.
PICK = (0, 2, 3, 5, 6, 8, 10, 12)


# ------------------------------------------------------------------------------------------------ extra data
@dataclass(frozen=True)
class Event:
    time: str
    label: str
    text: str
    kind: str   # next | now | past | linked


@dataclass(frozen=True)
class Tile:
    label: str
    value: str
    sub: str
    status: str   # ok | warn | soft | bad | unknown


#: C3b: each alert's story. Newest first; "next" events are what is expected, not promised.
TIMELINES: dict[int, tuple[Event, ...]] = {
    0: (Event("20:44", "Reminder", "if the stop is still missing", "next"),
        Event("20:14", "New", "No stop loss seen on ORD-DEMO-2 (EUR/USD). This email.", "now"),
        Event("20:14", "Halt level", "NORMAL to SOFT, because of this order (separate email).", "linked")),
    2: (Event("21:14", "Reminder 2", "if the stop is still missing", "next"),
        Event("20:44", "Reminder 1", "Still no stop loss on ORD-DEMO-2. This email.", "now"),
        Event("20:14", "Halt level", "NORMAL to SOFT, because of this order.", "linked"),
        Event("20:14", "New", "No stop loss seen on ORD-DEMO-2 (EUR/USD).", "past")),
    3: (Event("about 21:06", "Halt level", "back to NORMAL after the cool-down, if nothing else is open", "next"),
        Event("20:51", "Closed", "Stop loss seen on ORD-DEMO-2. This email.", "now"),
        Event("20:44", "Reminder 1", "Still no stop loss.", "past"),
        Event("20:14", "Halt level", "NORMAL to SOFT, because of this order.", "linked"),
        Event("20:14", "New", "No stop loss seen on ORD-DEMO-2 (EUR/USD).", "past")),
    5: (Event("after review", "Operator clear", "the operator reviews the day and clears the halt", "next"),
        Event("22:31", "Halt level", "SOFT to HARD: today's loss went past the daily limit. This email.", "now"),
        Event("22:31", "Daily account check", "Daily loss limit broken (separate email).", "linked"),
        Event("22:12", "Halt level", "NORMAL to SOFT: one trade risks too much (POS-DEMO-9).", "past")),
    6: (Event("end of day", "Ends", "with the trading day; repeats only while you have open trades", "next"),
        Event("08:01", "New", "Deposits and withdrawals not reported for Fri 20 Nov. This email.", "now"),
        Event("07:42", "Broker data", "Back in sync with StoneX after 46 min offline.", "past")),
    8: (Event("next reconnect", "Retry", "the server reads the missing window again, if StoneX still has it", "next"),
        Event("04:02", "Catch-up incomplete", "02:10 to 03:25 could not be read. This email.", "now"),
        Event("03:58", "Reconnected", "StoneX connection back.", "past"),
        Event("02:10", "Connection lost", "StoneX stopped sending data.", "past")),
    10: (Event("a few min", "Recovery", "the server tries to restart recording by itself", "next"),
         Event("03:40", "Stopped", "The server is not recording StoneX. This email.", "now"),
         Event("03:39", "Last data", "Last update received from StoneX.", "past")),
    12: (Event("21:30", "Next check", "when the next candle closes", "next"),
         Event("21:16", "Final advice", "Move the stop on ORD-DEMO-5 (GBP/USD). This email.", "now"),
         Event("21:01", "Heads-up", "Provisional: a move was likely once the 21:15 candle closed.", "past")),
}

#: C3c: the account at send time. (title time, tiles)
BOARDS: dict[int, tuple[str, tuple[Tile, ...]]] = {
    0: ("20:14", (Tile("Halt level", "SOFT", "since 20:14", "soft"),
                  Tile("Waiting on you", "1", "open risk alert", "warn"),
                  Tile("Waiting on operator", "0", "server problems", "ok"),
                  Tile("Heads-ups", "1", "open today", "ok"),
                  Tile("StoneX data", "Live", "updated 2 s ago", "ok"),
                  Tile("Server", "Recording", "since 07:42", "ok"))),
    2: ("20:44", (Tile("Halt level", "SOFT", "since 20:14", "soft"),
                  Tile("Waiting on you", "1", "open 30 min", "warn"),
                  Tile("Waiting on operator", "0", "server problems", "ok"),
                  Tile("Heads-ups", "1", "open today", "ok"),
                  Tile("StoneX data", "Live", "updated 1 s ago", "ok"),
                  Tile("Server", "Recording", "since 07:42", "ok"))),
    3: ("20:51", (Tile("Halt level", "SOFT", "cooling down to NORMAL", "soft"),
                  Tile("Waiting on you", "0", "nothing open", "ok"),
                  Tile("Waiting on operator", "0", "server problems", "ok"),
                  Tile("Heads-ups", "1", "open today", "ok"),
                  Tile("StoneX data", "Live", "updated 1 s ago", "ok"),
                  Tile("Server", "Recording", "since 07:42", "ok"))),
    5: ("22:31", (Tile("Halt level", "HARD", "since 22:31", "bad"),
                  Tile("Waiting on you", "2", "open risk alerts", "bad"),
                  Tile("Waiting on operator", "1", "clear the HARD halt", "warn"),
                  Tile("Heads-ups", "1", "open today", "ok"),
                  Tile("StoneX data", "Live", "updated 1 s ago", "ok"),
                  Tile("Server", "Recording", "since 07:42", "ok"))),
    6: ("08:01", (Tile("Halt level", "NORMAL", "trade as usual", "ok"),
                  Tile("Waiting on you", "0", "nothing open", "ok"),
                  Tile("Waiting on operator", "0", "server problems", "ok"),
                  Tile("Heads-ups", "1", "this one", "warn"),
                  Tile("StoneX data", "Live", "caught up at 07:42", "ok"),
                  Tile("Server", "Recording", "since 07:42", "ok"))),
    8: ("04:02", (Tile("Halt level", "NORMAL", "but see StoneX data", "ok"),
                  Tile("Waiting on you", "0", "nothing open", "ok"),
                  Tile("Waiting on operator", "1", "this one", "warn"),
                  Tile("Heads-ups", "0", "none open", "ok"),
                  Tile("StoneX data", "Gap", "02:10 to 03:25 not read", "bad"),
                  Tile("Server", "Recording", "since 03:52", "ok"))),
    10: ("03:40", (Tile("Halt level", "Unknown", "last known NORMAL", "unknown"),
                   Tile("Waiting on you", "Unknown", "no checks running", "unknown"),
                   Tile("Waiting on operator", "1+", "this one", "bad"),
                   Tile("Heads-ups", "Unknown", "no checks running", "unknown"),
                   Tile("StoneX data", "Unknown", "last update 03:39", "unknown"),
                   Tile("Server", "Stopped", "since 03:40", "bad"))),
    12: ("21:16", (Tile("Halt level", "NORMAL", "since 21:06", "ok"),
                   Tile("Waiting on you", "0", "risk alerts (advice not counted)", "ok"),
                   Tile("Waiting on operator", "0", "server problems", "ok"),
                   Tile("Heads-ups", "1", "open today", "ok"),
                   Tile("StoneX data", "Live", "updated 1 s ago", "ok"),
                   Tile("Server", "Recording", "since 07:42", "ok"))),
}

STATUS_COLOR = {"ok": "#067647", "warn": "#B54708", "soft": "#C4320A", "bad": "#B42318", "unknown": "#667085"}
STATUS_WORD = {"ok": "OK", "warn": "LOOK", "soft": "SOFT", "bad": "PROBLEM", "unknown": "UNKNOWN"}


def _first_sentence(text: str) -> str:
    i = text.find(". ")
    return text if i < 0 else text[:i + 1]


def _operator(a: Alert) -> str:
    tech = " | ".join(f"{k}: {v}" for k, v in a.tech)
    return (f'<tr><td style="padding:2px 20px 14px;"><div style="border-top:1px dashed #D0D5DD;padding-top:8px;'
            f'font:11px/1.6 {MONO};color:{SOFT_INK};word-break:break-all;">FOR THE OPERATOR | {e(tech)}</div></td></tr>')


def _footer() -> str:
    return (f'<tr><td style="padding:14px 20px 18px;border-top:1px solid {LINE};font:12px/1.5 {FONT};'
            f'color:{SOFT_INK};">{e(READ_ONLY)}<br>{e(TIME_NOTE)}</td></tr>')


def _band(a: Alert, color: str) -> str:
    """C2's band: status word box, segment, audience, who acts."""
    return (f'<tr><td bgcolor="{color}" style="background-color:{color};padding:14px 18px;">'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td width="80" valign="middle"><div style="border:2px solid #FFFFFF;border-radius:6px;width:68px;'
            f'text-align:center;font:800 20px/38px {FONT};color:#FFFFFF;letter-spacing:.05em;">{e(a.word)}</div></td>'
            f'<td valign="middle" style="font:13px/1.45 {FONT};color:#FFFFFF;">'
            f'<div style="font:700 16px/1.3 {FONT};color:#FFFFFF;">{e(a.segment)}</div>'
            f'For: <strong>{e(a.audience)}</strong><br>{e(WORDS[a.word])}</td></tr></table></td></tr>')


def _meta(a: Alert, color: str, tint: str) -> str:
    return (f'<tr><td style="padding:12px 20px 0;font:13px/1.5 {FONT};color:{MUTED};">'
            f'<span style="display:inline-block;padding:2px 7px;margin-right:6px;border-radius:3px;'
            f'background-color:{tint};border:1px solid {color};color:{color};font:700 12px/1.3 {FONT};">'
            f'{e(SEV[a.sev][1].upper())}</span>{e(a.stage)} · {e(a.when)}</td></tr>')


def _headline(a: Alert) -> str:
    return f'<tr><td style="padding:8px 20px 12px;font:700 21px/1.35 {FONT};color:{INK};">{e(a.title)}</td></tr>'


def _prose(label: str, text: str) -> str:
    return (f'<tr><td style="padding:0 20px 10px;font:15px/1.55 {FONT};color:{INK};"><strong>{e(label)}:</strong> '
            f'{e(text)}</td></tr>') if text else ""


# ------------------------------------------------------------------------------------------------ C3a Glance
def _mini_ladder(a: Alert, color: str) -> str:
    assert a.ladder is not None
    cells = []
    for i, (name, _hint) in enumerate(LADDER):
        on = i == a.ladder
        bg = color if on else "#FFFFFF"
        ink = "#FFFFFF" if on else SOFT_INK
        cells.append(f'<td align="center" bgcolor="{bg}" style="background-color:{bg};border:1px solid '
                     f'{color if on else "#D0D5DD"};padding:4px 2px;font:700 12px/1.3 {FONT};color:{ink};">'
                     f'{e(name)}</td>')
    return (f'<tr><td style="padding:0 20px 10px;"><table role="presentation" width="100%" cellpadding="0" '
            f'cellspacing="3" border="0"><tr><td width="70" style="font:12px/1.3 {FONT};color:{SOFT_INK};">'
            f'Halt level</td>{"".join(cells)}</tr></table></td></tr>')


def email_glance(a: Alert) -> str:
    color, sev_label, tint = SEV[a.sev]
    kicker = f"{a.word} · {a.segment} · {for_whom(a.audience)}"
    rows = [
        f'<tr><td bgcolor="{color}" style="background-color:{color};padding:12px 18px 14px;">'
        f'<div style="font:700 12px/1.4 {FONT};color:#FFFFFF;letter-spacing:.05em;">{e(kicker.upper())}</div>'
        f'<div style="padding-top:4px;font:700 20px/1.3 {FONT};color:#FFFFFF;">{e(a.title)}</div></td></tr>',
        _meta(a, color, tint),
    ]
    if a.steps:
        place, text = a.steps[0]
        rows.append(
            f'<tr><td style="padding:10px 20px 8px;"><div style="border:2px solid {color};background-color:{tint};'
            f'padding:10px 12px;">{_place_chip(place)}<div style="font:700 17px/1.4 {FONT};color:{INK};">'
            f'{e(text)}</div></div></td></tr>')
        if len(a.steps) > 1:
            rest = "".join(
                f'<div style="padding:2px 0;">{n}. {e(t)}</div>' for n, (_p, t) in enumerate(a.steps[1:], 2))
            rows.append(f'<tr><td style="padding:0 20px 8px;font:14px/1.45 {FONT};color:{INK};">'
                        f'<span style="color:{SOFT_INK};">Then:</span>{rest}</td></tr>')
    else:
        rows.append(f'<tr><td style="padding:10px 20px 8px;"><div style="background-color:{tint};padding:10px 12px;'
                    f'font:700 16px/1.4 {FONT};color:{INK};">{e(a.nothing)}</div></td></tr>')
    if a.ladder is not None:
        rows.append(_mini_ladder(a, color))
    rows.append(f'<tr><td style="padding:0 20px 6px;font:14px/1.5 {FONT};color:{INK};">'
                f'<strong>Why:</strong> {e(_first_sentence(a.why))}</td></tr>')
    if a.next:
        rows.append(f'<tr><td style="padding:0 20px 8px;font:14px/1.5 {FONT};color:{INK};">'
                    f'<strong>Next:</strong> {e(_first_sentence(a.next))}</td></tr>')
    facts = " · ".join(f"{k}: {v}" for k, v in a.facts)
    rows += [
        f'<tr><td style="padding:4px 20px 10px;font:13px/1.5 {FONT};color:{MUTED};">{e(facts)}</td></tr>',
        _placeholder(a),
        f'<tr><td style="padding:0 20px 8px;font:13px/1.5 {FONT};color:{MUTED};">'
        f'<strong>Linked:</strong> {e(a.linked)}</td></tr>' if a.linked else "",
        _operator(a), _footer(),
    ]
    return preheader(f"{a.word} · {a.segment} · {for_whom(a.audience)} · {sev_label} · {a.stage}") + \
        shell("".join(rows), card_style=f"border:1px solid {LINE};")


# ------------------------------------------------------------------------------------------------ C3b Timeline
def _timeline(events: tuple[Event, ...], color: str) -> str:
    out = []
    for n, ev in enumerate(events):
        last = n == len(events) - 1
        if ev.kind == "now":
            dot = (f'<div style="width:14px;height:14px;border-radius:7px;background-color:{color};'
                   f'border:2px solid {color};"></div>')
            ink, weight, bg = INK, "700", "#F9FAFB"
        elif ev.kind == "next":
            dot = '<div style="width:12px;height:12px;border-radius:7px;border:2px dashed #98A2B3;"></div>'
            ink, weight, bg = SOFT_INK, "600", "#FFFFFF"
        elif ev.kind == "linked":
            dot = '<div style="width:12px;height:12px;border-radius:2px;border:2px solid #475467;"></div>'
            ink, weight, bg = INK, "600", "#FFFFFF"
        else:
            dot = '<div style="width:12px;height:12px;border-radius:7px;background-color:#667085;border:2px solid #667085;"></div>'
            ink, weight, bg = INK, "600", "#FFFFFF"
        tag = {"next": "COMING", "now": "THIS EMAIL", "linked": "LINKED", "past": ""}[ev.kind]
        tag_html = (f' <span style="font:700 10px/1.3 {FONT};color:{color if ev.kind == "now" else SOFT_INK};'
                    f'letter-spacing:.05em;">{tag}</span>') if tag else ""
        # The rail is the text cell's left border (email-safe: no negative margins, no images); the dot sits
        # against it. The last event's rail stops with its text.
        rail = "#D0D5DD" if not last else "#FFFFFF"
        out.append(
            f'<tr><td width="58" valign="top" style="padding:3px 6px 0 0;font:{weight} 13px/1.35 {FONT};'
            f'color:{ink};text-align:right;">{e(ev.time)}</td>'
            f'<td width="18" valign="top" align="right" style="padding:4px 0 0;">{dot}</td>'
            f'<td valign="top" bgcolor="{bg}" style="background-color:{bg};border-left:2px solid {rail};'
            f'padding:2px 8px 12px 10px;"><div style="font:{weight} 14px/1.35 {FONT};color:{ink};">{e(ev.label)}'
            f'{tag_html}</div><div style="font:14px/1.45 {FONT};color:{ink if ev.kind != "next" else SOFT_INK};'
            f'{"font-style:italic;" if ev.kind == "next" else ""}">{e(ev.text)}</div></td></tr>')
    return (f'<tr><td style="padding:6px 20px 8px;">{_label("THE STORY SO FAR (newest first)")}'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">{"".join(out)}'
            f'</table></td></tr>')


def email_timeline(a: Alert, idx: int) -> str:
    color, sev_label, tint = SEV[a.sev]
    rows = [
        _band(a, color), _meta(a, color, tint), _headline(a), _steps(a, color, tint), _placeholder(a),
        _timeline(TIMELINES[idx], color),
        f'<tr><td style="padding:0 20px 14px;">{_ladder(a, color)}</td></tr>' if a.ladder is not None else "",
        _facts(a) if a.facts else "",
        _prose("Why", a.why),
        _operator(a), _footer(),
    ]
    return preheader(f"{a.word} · {a.segment} · {for_whom(a.audience)} · {sev_label} · {a.stage}") + \
        shell("".join(rows), card_style=f"border:1px solid {LINE};")


# ------------------------------------------------------------------------------------------------ C3c Status board
def _board(idx: int) -> str:
    at, tiles = BOARDS[idx]
    cells = []
    for t in tiles:
        c = STATUS_COLOR[t.status]
        word = "" if t.value.upper() == STATUS_WORD[t.status] else STATUS_WORD[t.status]
        cells.append(
            f'<td width="50%" valign="top" style="padding:4px;"><div style="border:1px solid {LINE};'
            f'border-left:5px solid {c};background-color:#FFFFFF;padding:8px 10px;">'
            f'<div style="font:12px/1.3 {FONT};color:{SOFT_INK};">{e(t.label)}</div>'
            f'<div style="font:700 18px/1.3 {FONT};color:{INK};">{e(t.value)} '
            f'<span style="font:700 10px/1.3 {FONT};color:{c};letter-spacing:.05em;vertical-align:middle;">'
            f'{word}</span></div>'
            f'<div style="font:12px/1.35 {FONT};color:{MUTED};">{e(t.sub)}</div></div></td>')
    rows = "</tr><tr>".join("".join(cells[i:i + 2]) for i in range(0, len(cells), 2))
    unknown = any(t.status == "unknown" for t in tiles)
    note = ("Grey tiles are unknown: with the server not recording, nothing is being checked."
            if unknown else "Counts and states only, read when this email was written.")
    return (f'<tr><td style="padding:6px 16px 6px;"><table role="presentation" width="100%" cellpadding="0" '
            f'cellspacing="0" border="0" bgcolor="#F2F4F7" style="background-color:#F2F4F7;border-radius:6px;">'
            f'<tr><td style="padding:10px 8px 4px 12px;font:700 12px/1.3 {FONT};color:{SOFT_INK};letter-spacing:.06em;">'
            f'YOUR ACCOUNT AT {e(at)}</td></tr><tr><td style="padding:0 4px;"><table role="presentation" '
            f'width="100%" cellpadding="0" cellspacing="0" border="0"><tr>{rows}</tr></table></td></tr>'
            f'<tr><td style="padding:4px 12px 10px;font:12px/1.4 {FONT};color:{MUTED};">{e(note)}</td></tr>'
            f'</table></td></tr>')


def email_board(a: Alert, idx: int) -> str:
    color, sev_label, tint = SEV[a.sev]
    at, tiles = BOARDS[idx]
    summary = " · ".join(f"{t.label} {t.value}" for t in tiles[:3])
    rows = [
        _band(a, color), _meta(a, color, tint), _headline(a), _steps(a, color, tint), _placeholder(a),
        _prose("Why", a.why), _prose("Next", a.next),
        _board(idx),
        _facts(a) if a.facts else "",
        _operator(a), _footer(),
    ]
    return preheader(f"{a.word} · {a.segment} · {sev_label} · account at {at}: {summary}") + \
        shell("".join(rows), card_style=f"border:1px solid {LINE};")


# ------------------------------------------------------------------------------------------------ pages
PROTOS = {
    "a": ("C3a", "Glance", "prototype-C3a-glance.html",
          "Everything you need fits on the first phone screen. The headline sits inside the coloured band, the "
          "first step is one big box, and the rest is one-line why, next and facts. The full detail stays in "
          "the operator line."),
    "b": ("C3b", "Timeline", "prototype-C3b-timeline.html",
          "The alert's whole story in the email, newest first: what is coming next, this email, linked halt "
          "changes and what happened before. Made for threaded reminders: open the latest email and you have "
          "the history."),
    "c": ("C3c", "Status board", "prototype-C3c-status-board.html",
          "The C2 ticket, then a board of the whole account at the moment of sending: halt level, open alerts "
          "by who acts, StoneX data and the server. Any email answers 'is everything else OK?'. Counts and "
          "states only."),
}


def render(p: str, a: Alert, idx: int) -> str:
    if p == "a":
        return email_glance(a)
    if p == "b":
        return email_timeline(a, idx)
    return email_board(a, idx)


def build(p: str) -> str:
    code, name, _fname, blurb = PROTOS[p]
    nav = " · ".join(
        (f"<strong>{c}: {e(n)}</strong>" if q == p else f'<a href="{f}" style="color:#175CD3;">{c}: {e(n)}</a>')
        for q, (c, n, f, _b) in PROTOS.items())
    parts = [
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<meta name="color-scheme" content="light">'
        f'<title>Alert email prototype {code}: {e(name)}</title></head>'
        f'<body style="margin:0;padding:0;background-color:#E4E7EC;">'
        f'<div style="max-width:640px;margin:0 auto;padding:20px 12px;font:14px/1.5 {FONT};color:{INK};">'
        f'<div style="font:12px/1.4 {FONT};color:{SOFT_INK};">XTNL alert email redesign, design prototype (not wired '
        f'into the engine). Made-up sample data. Developed from '
        f'<a href="prototype-C2-status-ticket.html" style="color:#175CD3;">C2</a>.</div>'
        f'<h1 style="margin:6px 0 4px;font:700 24px/1.3 {FONT};">Prototype {code}: {e(name)}</h1>'
        f'<p style="margin:0 0 8px;">{e(blurb)}</p>'
        f'<p style="margin:0 0 18px;font-size:13px;">{nav} · README.md compares them.</p>',
    ]
    for n, idx in enumerate(PICK, 1):
        a = ALERTS[idx]
        parts.append(f'<h2 style="margin:28px 0 8px;font:700 17px/1.35 {FONT};color:{INK};">{n} of {len(PICK)}: '
                     f'{e(a.caption)}</h2>')
        parts.append(_inbox(a))
        parts.append(_push(a))
        parts.append(f'<div style="border:1px dashed #98A2B3;">{render(p, a, idx)}</div>')
    parts.append('</div></body></html>\n')
    return "\n".join(x for x in parts if x)


#: C3b's own page is now the XTNL-theme one (gen_prototype_c3b.py); the light C3b is kept under this name.
C3B_LIGHT = "prototype-C3b-timeline-v1-light.html"


def main() -> None:
    for p, (_c, _n, fname, _b) in PROTOS.items():
        out = C3B_LIGHT if p == "b" else fname
        (HERE / out).write_text(build(p), encoding="utf-8")
        print(f"wrote {out}")


if __name__ == "__main__":
    main()
