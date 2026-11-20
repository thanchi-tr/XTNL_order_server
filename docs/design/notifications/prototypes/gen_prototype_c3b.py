"""Generate `prototype-C3b-timeline.html`: C3b Timeline in the XTNL Solutions theme (www.xtnl-solutions.com).

    python docs/design/notifications/prototypes/gen_prototype_c3b.py

The first, light C3b is kept as `prototype-C3b-timeline-v1-light.html` (built by gen_prototype_c3.py).
Design prototype only: nothing in `src/` imports this and it imports nothing from `src/`. Made-up data.
Shown: order levels (entry, current stop, proposed stop: order state, not market data), each with its distance from
the entry in pips (owner request 2026-11-21). Never shown: account size, balance, P&L, money amounts, the
instrument's pip / point size itself, spreads, or any market price (tick, quote, bar, current price).

Theme, read from the site's own CSS variables (2026-11-21):
  surfaces  --base #04080f (page), --card #0b1622 (email card), --raised #0f1e2e (panels, this-email row)
  ink       --ink-0 #eef2f8 (headings, values), --ink-1 #9ab0c8 (body, labels); --ink-2 #5a7490 and
            --ink-3 #2a3d52 are used for lines and dots only (3.8:1 and 1.6:1 on the card: too faint for text)
  accents   --green #00cc7a, --red #f03a57, --amber #f0a030, --blue #4d9cf5
  type      Inter for text, JetBrains Mono for labels, ids and figures (falls back to system fonts in mail apps;
            no web fonts are loaded, an email cannot rely on them)
Severity on this theme (fills carry dark text, as the site's green button does; every pair >= 4.5:1):
  Urgent red, Important amber, Heads-up blue, Info slate (--ink-1), Resolved green.
Email-safety is unchanged: tables, inline styles, no <style>, images, scripts or links, solid colours only (the
site's rgba lines are pre-blended onto the card), max 600 px.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, replace
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from gen_prototype_c2 import ALERTS, LADDER, PLACES, Alert, for_whom  # noqa: E402
from gen_prototype_c3 import PICK, PROTOS, TIMELINES, Event  # noqa: E402
from gen_prototypes import e  # noqa: E402

OUT = HERE / "prototype-C3b-timeline.html"

# ---------------------------------------------------------------------------------------------------- theme
CANVAS = "#020508"
BASE = "#04080f"
CARD = "#0b1622"
RAISED = "#0f1e2e"
LINE = "#1a242f"       # --line (#ffffff0f) blended onto --card
LINE_HI = "#26303a"    # --line-hi (#ffffff1c) blended onto --card
INK0 = "#eef2f8"
INK1 = "#9ab0c8"
INK2 = "#5a7490"       # lines and dots only
INK3 = "#2a3d52"       # rail only
GREEN = "#00cc7a"
ON_FILL = "#04080f"    # dark text on an accent fill
SANS = "Inter,-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
MONO = "'JetBrains Mono',Consolas,Menlo,monospace"

#: severity -> (accent, label)
SEVT = {
    "urgent": ("#f03a57", "Urgent"),
    "important": ("#f0a030", "Important"),
    "headsup": ("#4d9cf5", "Heads-up"),
    "info": ("#9ab0c8", "Info"),
    "resolved": ("#00cc7a", "Resolved"),
}
WHO = {"ACT": "Action for you", "FIX": "Action for the operator", "FYI": "No action needed",
       "DONE": "Closed, no action needed"}
READ_ONLY = ("Phase 1 is read-only: this server watches your StoneX account and never places, changes or cancels "
             "orders. Any change is yours to make at StoneX.")


def blend(hex_color: str, alpha: float, under: str = CARD) -> str:
    """`hex_color` at `alpha` over `under`, as a solid colour (mail apps drop rgba unevenly)."""
    def rgb(h: str) -> tuple[int, int, int]:
        h = h.lstrip("#")
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    top, bot = rgb(hex_color), rgb(under)
    return "#" + "".join(f"{round(t * alpha + b * (1 - alpha)):02x}" for t, b in zip(top, bot, strict=True))


def mono_label(text: str, color: str = INK1, size: int = 11) -> str:
    return (f'<div style="font:700 {size}px/1.4 {MONO};color:{color};letter-spacing:.14em;text-transform:uppercase;'
            f'padding-bottom:8px;">{e(text)}</div>')


def chip(text: str, *, fill: str | None = None, edge: str = LINE_HI, ink: str = INK0) -> str:
    style = (f"background-color:{fill};color:{ON_FILL};border:1px solid {fill};" if fill
             else f"background-color:{CARD};color:{ink};border:1px solid {edge};")
    return (f'<span style="display:inline-block;padding:2px 8px;margin:0 6px 4px 0;border-radius:4px;{style}'
            f'font:700 11px/1.5 {MONO};letter-spacing:.1em;text-transform:uppercase;white-space:nowrap;">'
            f'{e(text)}</span>')


def row(inner: str, pad: str = "0 22px 14px") -> str:
    return f'<tr><td style="padding:{pad};">{inner}</td></tr>'


# ---------------------------------------------------------------------------------------------------- parts
def header(a: Alert, accent: str) -> str:
    brand = (f'<span style="font:800 16px/1 {MONO};color:{GREEN};">&#x2715;</span>'
             f'<span style="font:800 15px/1 {MONO};color:{INK0};letter-spacing:.3em;padding-left:8px;">XTNL</span>')
    ticker = (f'<span style="color:{GREEN};">&#x25CF;</span> READ-ONLY &middot; PHASE 1 &middot; STONEX')
    return (f'<tr><td bgcolor="{BASE}" style="background-color:{BASE};border-top:3px solid {accent};'
            f'padding:14px 22px 12px;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'border="0"><tr><td valign="middle">{brand}</td><td align="right" valign="middle" '
            f'style="font:600 10px/1.4 {MONO};color:{INK1};letter-spacing:.12em;">{ticker}</td></tr></table>'
            f'</td></tr>')


def masthead(a: Alert, accent: str) -> str:
    kicker = f"{a.segment} · {for_whom(a.audience)}"
    sev = SEVT[a.sev][1]
    return (row(f'{chip(a.word, fill=accent)}<span style="font:700 11px/1.5 {MONO};color:{INK1};'
                f'letter-spacing:.14em;text-transform:uppercase;">{e(kicker)}</span>', "18px 22px 6px")
            + row(f'<div style="font:800 23px/1.3 {SANS};color:{INK0};letter-spacing:-.01em;">{e(a.title)}</div>',
                  "4px 22px 10px")
            + row(f'{chip(sev, edge=accent, ink=accent if a.sev != "urgent" else INK0)}'
                  f'<span style="font:13px/1.6 {SANS};color:{INK1};">{e(a.stage)} &middot; {e(a.when)} &middot; '
                  f'{e(WHO[a.word])}</span>', "0 22px 16px"))


def steps(a: Alert, accent: str) -> str:
    if not a.steps:
        return row(f'<div style="background-color:{blend(accent, .10)};border-left:3px solid {accent};'
                   f'padding:12px 14px;font:600 15px/1.5 {SANS};color:{INK0};">{e(a.nothing)}</div>')
    items = "".join(
        f'<tr><td width="34" valign="top" style="padding:0 0 12px;"><div style="width:24px;height:24px;'
        f'line-height:24px;text-align:center;border:1px solid {accent};border-radius:4px;'
        f'background-color:{blend(accent, .10)};font:700 12px/24px {MONO};color:{INK0};">{n:02d}</div></td>'
        f'<td valign="top" style="padding:1px 0 12px;">{chip(PLACES[p][0])}'
        f'<div style="font:15px/1.5 {SANS};color:{INK0};">{e(t)}</div></td></tr>'
        for n, (p, t) in enumerate(a.steps, 1))
    head = "To do" if a.word in ("ACT", "FIX") else "Only if it applies"
    return row(f'{mono_label(head)}<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
               f'border="0">{items}</table>', "4px 22px 6px")


def placeholder(a: Alert) -> str:
    if a.placeholder is None:
        return ""
    k, v = a.placeholder
    return row(f'<div style="border:1px dashed {INK2};padding:10px 12px;">'
               f'<div style="font:700 11px/1.4 {MONO};color:{INK1};letter-spacing:.12em;text-transform:uppercase;">'
               f'{e(k)}</div><div style="font:14px/1.5 {SANS};color:{INK1};">{e(v)}</div></div>')


def timeline(events: tuple[Event, ...], accent: str) -> str:
    out = []
    for n, ev in enumerate(events):
        last = n == len(events) - 1
        now = ev.kind == "now"
        if now:
            dot = (f'<div style="width:12px;height:12px;border-radius:7px;background-color:{accent};'
                   f'border:2px solid {blend(accent, .35)};"></div>')
        elif ev.kind == "next":
            dot = f'<div style="width:10px;height:10px;border-radius:6px;border:2px dashed {INK2};"></div>'
        elif ev.kind == "linked":
            dot = f'<div style="width:10px;height:10px;border-radius:2px;border:2px solid {INK1};"></div>'
        else:
            dot = f'<div style="width:10px;height:10px;border-radius:6px;background-color:{INK2};border:2px solid {INK2};"></div>'
        tag = {"next": "Coming", "now": "This email", "linked": "Linked", "past": ""}[ev.kind]
        tag_html = (f'&nbsp;<span style="font:700 10px/1.4 {MONO};color:{INK0 if now else INK1};letter-spacing:.12em;'
                    f'text-transform:uppercase;">&middot; {e(tag)}</span>') if tag else ""
        bg = RAISED if now else CARD
        edge = accent if now else (INK3 if not last else CARD)
        text_ink = INK0 if ev.kind != "next" else INK1
        out.append(
            f'<tr><td width="62" valign="top" style="padding:3px 8px 0 0;text-align:right;font:{"700" if now else "500"} '
            f'12px/1.4 {MONO};color:{INK0 if now else INK1};">{e(ev.time)}</td>'
            f'<td width="16" valign="top" align="right" style="padding:4px 0 0;">{dot}</td>'
            f'<td valign="top" bgcolor="{bg}" style="background-color:{bg};border-left:2px solid {edge};'
            f'padding:3px 10px 12px 12px;"><div style="font:700 14px/1.4 {SANS};color:{text_ink};">{e(ev.label)}'
            f'{tag_html}</div><div style="font:{"italic " if ev.kind == "next" else ""}14px/1.5 {SANS};'
            f'color:{text_ink if now else INK1};">{e(ev.text)}</div></td></tr>')
    return row(f'{mono_label("The story so far · newest first")}<table role="presentation" width="100%" '
               f'cellpadding="0" cellspacing="0" border="0">{"".join(out)}</table>', "8px 22px 10px")


def ladder(a: Alert, accent: str) -> str:
    if a.ladder is None:
        return ""
    cells = []
    for i, (name, hint) in enumerate(LADDER):
        on = i == a.ladder
        was = a.ladder_from is not None and i == a.ladder_from and not on
        if on:
            style, ink = f"background-color:{accent};border:1px solid {accent};", ON_FILL
        elif was:
            style, ink = f"background-color:{CARD};border:1px dashed {INK1};", INK1
        else:
            style, ink = f"background-color:{CARD};border:1px solid {LINE_HI};", INK1
        tag = "now" if on else "was" if was else "&nbsp;"
        cells.append(f'<td width="33%" align="center" valign="top" style="{style}padding:8px 4px 9px;">'
                     f'<div style="font:700 10px/1.4 {MONO};color:{ink};letter-spacing:.14em;text-transform:uppercase;">'
                     f'{tag}</div>'
                     f'<div style="font:800 14px/1.3 {MONO};color:{ink};letter-spacing:.08em;">{e(name)}</div>'
                     f'<div style="font:12px/1.35 {SANS};color:{ink};">{e(hint)}</div></td>')
    return row(f'{mono_label("Halt level")}<table role="presentation" width="100%" cellpadding="0" cellspacing="4" '
               f'border="0"><tr>{"".join(cells)}</tr></table>', "4px 22px 14px")


def facts(a: Alert) -> str:
    if not a.facts:
        return ""
    cells = [f'<td width="50%" valign="top" style="padding:10px 12px;border-top:1px solid {LINE};">'
             f'<div style="font:700 10px/1.4 {MONO};color:{INK1};letter-spacing:.14em;text-transform:uppercase;">'
             f'{e(k)}</div><div style="font:700 16px/1.35 {MONO};color:{INK0};padding-top:2px;">{e(v)}</div></td>'
             for k, v in a.facts]
    rows = "</tr><tr>".join("".join(cells[i:i + 2]) for i in range(0, len(cells), 2))
    return row(f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
               f'bgcolor="{RAISED}" style="background-color:{RAISED};border:1px solid {LINE};"><tr>{rows}</tr></table>',
               "4px 22px 14px")


def why(a: Alert) -> str:
    if not a.why:
        return ""
    return row(f'<div style="font:15px/1.6 {SANS};color:{INK1};"><span style="color:{INK0};font-weight:700;">'
               f'Why it matters.</span> {e(a.why)}</div>')


def operator(a: Alert) -> str:
    tech = "".join(f'<div><span style="color:{INK1};">{e(k)}</span>&nbsp; <span style="color:{INK0};">{e(v)}</span>'
                   f'</div>' for k, v in a.tech)
    return row(f'<div style="background-color:{BASE};border:1px solid {LINE};padding:10px 12px;">'
               f'{mono_label("For the operator")}<div style="font:11px/1.7 {MONO};word-break:break-all;">{tech}</div>'
               f'</div>', "2px 22px 16px")


def footer() -> str:
    return (f'<tr><td bgcolor="{BASE}" style="background-color:{BASE};border-top:1px solid {LINE};'
            f'padding:14px 22px 18px;font:12px/1.6 {SANS};color:{INK1};">{e(READ_ONLY)}<br>'
            f'Times are Melbourne time (AEDT); UTC is in the operator details.<br>'
            f'<span style="font:700 10px/2 {MONO};letter-spacing:.2em;color:{INK1};">XTNL SOLUTIONS &middot; '
            f'ORDER-SERVER</span></td></tr>')


# ---------------------------------------------------------------------------------------------------- levels
@dataclass(frozen=True)
class Level:
    """One order level. `pips`: the distance from the entry (None for the entry itself or a missing stop)."""

    label: str
    price: str | None          # None = no stop
    pips: float | None = None
    where: str = ""            # below | above (entry)
    key: bool = False          # the level this email is about (highlighted)


#: idx -> (side, levels, note). Only alerts about an order's stop carry levels. Made-up prices.
LEVELS: dict[int, tuple[str, tuple[Level, ...], str]] = {
    0: ("Buy", (Level("Entry", "1.08570"), Level("Stop loss", None, key=True)),
        "No stop: the loss on this order has no limit."),
    2: ("Buy", (Level("Entry", "1.08570"), Level("Stop loss", None, key=True)),
        "Still no stop after 30 minutes."),
    3: ("Buy", (Level("Entry", "1.08570"), Level("Stop loss", "1.08250", 32.0, "below", key=True)),
        "Stop seen at 20:51."),
    12: ("Buy", (Level("Entry", "1.27140"), Level("Stop now", "1.26800", 34.0, "below"),
                 Level("Proposed stop", "1.27020", 12.0, "below", key=True)),
         "Moves the stop 22.0 pips closer to the entry."),
}

#: The same alerts with the prices written into the action (ARM) and the story (closed, ARM).
_ARM = 12
OVERRIDES: dict[int, dict[str, object]] = {
    _ARM: {"title": "Move the stop on ORD-DEMO-5 (GBP/USD) to 1.27020",
           "steps": (("stonex", "Move the stop on order ORD-DEMO-5 (GBP/USD) to 1.27020, 12.0 pips below the entry."),
                     ("wait", "Or leave it: this is advice. The server changes nothing at StoneX.")),
           "placeholder": None,
           "push": ("ACT · ARM: move the stop on ORD-DEMO-5 to 1.27020", "GBP/USD · 12.0 pips below entry · advice only")},
}
STORY_TEXT: dict[tuple[int, str], str] = {
    (3, "now"): "Stop loss seen on ORD-DEMO-2 at 1.08250, 32.0 pips below the entry. This email.",
    (12, "now"): "Move the stop on ORD-DEMO-5 to 1.27020, 12.0 pips below the entry. This email.",
}


def alert_for(idx: int) -> Alert:
    a = ALERTS[idx]
    return replace(a, **OVERRIDES[idx]) if idx in OVERRIDES else a  # type: ignore[arg-type]


def story_for(idx: int) -> tuple[Event, ...]:
    return tuple(replace(ev, text=STORY_TEXT.get((idx, ev.kind), ev.text)) for ev in TIMELINES[idx])


def _bar(pct: float, color: str) -> str:
    pct = max(4.0, min(100.0, pct))
    rest = "" if pct >= 100 else f'<td bgcolor="{LINE_HI}" style="background-color:{LINE_HI};font-size:0;line-height:0;">&nbsp;</td>'
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="height:6px;">'
            f'<tr><td width="{pct:.0f}%" bgcolor="{color}" style="background-color:{color};height:6px;font-size:0;'
            f'line-height:0;">&nbsp;</td>{rest}</tr></table>')


def levels(idx: int, accent: str) -> str:
    """Entry, stop(s) and their distance from the entry in pips, each stop with a distance bar (longest = full)."""
    if idx not in LEVELS:
        return ""
    side, lv, note = LEVELS[idx]
    longest = max((x.pips for x in lv if x.pips), default=0.0)
    rows = []
    for n, x in enumerate(lv):
        top = "" if n == 0 else f"border-top:1px solid {LINE};"
        bg = blend(accent, .08, RAISED) if x.key else RAISED
        label = (f'<div style="font:700 10px/1.4 {MONO};color:{INK0 if x.key else INK1};letter-spacing:.14em;'
                 f'text-transform:uppercase;">{e(x.label)}</div>')
        if x.price is None:
            price = f'<div style="font:800 17px/1.35 {MONO};color:{accent};">NONE</div>'
            dist = f'<div style="font:700 12px/1.4 {MONO};color:{accent};">no limit</div>'
            bar = (f'<div style="border:1px dashed {accent};height:4px;line-height:4px;font-size:0;">&nbsp;</div>')
        elif x.pips is None:
            price = f'<div style="font:700 17px/1.35 {MONO};color:{INK0};">{e(x.price)}</div>'
            dist = f'<div style="font:12px/1.4 {SANS};color:{INK1};">reference</div>'
            bar = ""
        else:
            price = f'<div style="font:{"800" if x.key else "700"} 17px/1.35 {MONO};color:{INK0};">{e(x.price)}</div>'
            dist = (f'<div style="font:700 14px/1.3 {MONO};color:{INK0};white-space:nowrap;">{x.pips:.1f} pips</div>'
                    f'<div style="font:12px/1.3 {SANS};color:{INK1};">{e(x.where)} entry</div>')
            bar = _bar(100 * x.pips / longest if longest else 100, accent if x.key else INK2)
        edge = f"border-left:3px solid {accent};" if x.key else f"border-left:3px solid {RAISED};"
        rows.append(
            f'<tr><td bgcolor="{bg}" style="background-color:{bg};{top}{edge}padding:9px 12px 10px;">'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td valign="top">{label}{price}</td><td valign="top" align="right" width="42%">{dist}</td></tr>'
            + (f'<tr><td colspan="2" style="padding-top:7px;">{bar}</td></tr>' if bar else "")
            + '</table></td></tr>')
    head = f"Levels · {side}"
    return row(f'{mono_label(head)}<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
               f'style="border:1px solid {LINE};">{"".join(rows)}</table>'
               f'<div style="font:13px/1.5 {SANS};color:{INK1};padding-top:8px;">{e(note)}'
               f'{" Distances are from the entry; no market price is shown." if longest else ""}</div>',
               "4px 22px 14px")


def email(a: Alert, idx: int) -> str:
    accent, sev = SEVT[a.sev]
    pre = f"{a.word} · {a.segment} · {for_whom(a.audience)} · {sev} · {a.stage}"
    body = "".join([header(a, accent), masthead(a, accent), steps(a, accent), placeholder(a), levels(idx, accent),
                    timeline(story_for(idx), accent), ladder(a, accent), facts(a), why(a), operator(a), footer()])
    return (f'<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;font-size:1px;line-height:1px;'
            f'color:{BASE};opacity:0;">{e(pre)}</div>'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{BASE}" '
            f'style="background-color:{BASE};"><tr><td align="center" style="padding:16px 8px;">'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{CARD}" '
            f'style="max-width:600px;width:100%;background-color:{CARD};border:1px solid {LINE};">{body}</table>'
            f'</td></tr></table>')


# ---------------------------------------------------------------------------------------------------- page
def inbox(a: Alert) -> str:
    count = f' <span style="color:{INK1};font-weight:400;">({a.thread})</span>' if a.thread > 1 else ""
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="max-width:600px;margin:0 auto 6px;background-color:{CARD};border:1px solid {LINE_HI};'
            f'table-layout:fixed;"><tr><td style="padding:9px 12px;font:13px/1.45 {SANS};color:{INK0};">'
            f'<div style="font:700 10px/1.4 {MONO};color:{INK1};letter-spacing:.14em;">INBOX ROW</div>'
            f'<strong>XTNL Alerts</strong>{count}<br><strong>{e(a.subject)}</strong>'
            f'<div style="color:{INK1};white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'
            f'{e(f"{a.word} · {a.segment} · {for_whom(a.audience)} · {SEVT[a.sev][1]} · {a.stage}")}</div></td></tr></table>')


def push(a: Alert) -> str:
    if a.push is None:
        return ""
    title, body = a.push
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="max-width:600px;margin:0 auto 6px;background-color:{RAISED};border:1px solid {LINE_HI};'
            f'border-radius:12px;"><tr><td style="padding:9px 12px;font:13px/1.4 {SANS};color:{INK0};">'
            f'<div style="font:700 10px/1.4 {MONO};color:{INK1};letter-spacing:.14em;">PHONE PUSH (NTFY) &middot; '
            f'IMPORTANT AND URGENT ONLY</div><strong>{e(title)}</strong><br>'
            f'<span style="color:{INK1};">{e(body)}</span></td></tr></table>')


def legend() -> str:
    sev = "".join(chip(lbl, fill=c) for c, lbl in SEVT.values())
    words = "".join(f'<div style="padding:3px 0;">{chip(w)}<span style="font:14px/1.5 {SANS};color:{INK0};">'
                    f'{e(t)}</span></div>' for w, t in (
                        ("ACT", "you do something, at StoneX"),
                        ("FIX", "the operator does something, on the server PC or the broker setup"),
                        ("FYI", "nobody needs to act"), ("DONE", "something that was open has closed")))
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="max-width:600px;margin:0 auto 10px;background-color:{CARD};border:1px solid {LINE_HI};">'
            f'<tr><td style="padding:14px 14px 10px;">{mono_label("How to read it")}{words}'
            f'<div style="font:13px/1.5 {SANS};color:{INK1};padding:8px 0 4px;">Severity colour, separate from the '
            f'word:</div>{sev}</td></tr></table>')


def build() -> str:
    nav = " &middot; ".join(
        (f'<strong style="color:{INK0};">{c}: {e(n)}</strong>' if q == "b" else
         f'<a href="{f}" style="color:{GREEN};">{c}: {e(n)}</a>') for q, (c, n, f, _b) in PROTOS.items())
    parts = [
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<meta name="color-scheme" content="dark"><meta name="supported-color-schemes" content="dark">'
        '<title>Alert email prototype C3b: Timeline, XTNL theme</title></head>'
        f'<body style="margin:0;padding:0;background-color:{CANVAS};">'
        f'<div style="max-width:640px;margin:0 auto;padding:22px 12px;font:14px/1.6 {SANS};color:{INK0};">'
        f'<div style="font:700 11px/1.5 {MONO};color:{GREEN};letter-spacing:.16em;">XTNL SOLUTIONS &middot; ALERT '
        f'EMAIL REDESIGN</div>'
        f'<h1 style="margin:8px 0 6px;font:800 28px/1.2 {SANS};letter-spacing:-.01em;color:{INK0};">'
        f'C3b Timeline, <span style="color:{GREEN};">XTNL theme.</span></h1>'
        f'<p style="margin:0 0 8px;color:{INK1};">C3b restyled in the colours and type of xtnl-solutions.com: navy '
        f'surfaces, green brand accent, Inter for text and JetBrains Mono for labels and figures. Each email still '
        f'tells the alert\'s story, newest first: what is coming next, this email, linked halt changes and what '
        f'happened before. Design prototype, made-up data, not wired into the engine.</p>'
        f'<p style="margin:0 0 16px;font-size:13px;color:{INK1};">{nav} &middot; '
        f'<a href="prototype-C3b-timeline-v1-light.html" style="color:{GREEN};">C3b v1 (light)</a></p>',
        legend(),
    ]
    for n, idx in enumerate(PICK, 1):
        a = alert_for(idx)
        parts.append(f'<h2 style="margin:30px 0 8px;font:700 11px/1.5 {MONO};color:{INK1};letter-spacing:.14em;'
                     f'text-transform:uppercase;">{n:02d} / {len(PICK):02d} &middot; {e(a.caption)}</h2>')
        parts += [inbox(a), push(a), f'<div style="border:1px dashed {INK3};">{email(a, idx)}</div>']
    parts.append('</div></body></html>\n')
    return "\n".join(p for p in parts if p)


def main() -> None:
    OUT.write_text(build(), encoding="utf-8")
    print(f"wrote {OUT.name}")


if __name__ == "__main__":
    main()
