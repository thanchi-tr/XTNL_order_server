"""The XTNL theme of the alert emails' HTML part (`XTNL_EMAIL_THEME=xtnl`, the default; `classic` keeps the light
layouts of `email_html` / `email_html_arm`). Design: `docs/design/notifications/prototypes/` (C3b Timeline in the
xtnl-solutions.com theme, spec `prototype-C2-spec.md`).

Every email reads the same way:

1. a dark header with the XTNL mark and "READ-ONLY · PHASE 1", a top line in the severity colour;
2. the **status word**, which says who acts: ACT (you, at the broker), FIX (the operator, on the server or the
   broker setup), FYI (nobody), DONE (something closed); then the segment (Risk check, Halt level, Daily account
   check, Broker connection and data, System health, ARM advice) and the audience;
3. a plain headline, the severity, the stage and the Melbourne time;
4. numbered steps, each with the place it is done (AT THE BROKER, SERVER PC, CHECK, JUST WAIT);
5. where the alert has them: order **levels** (entry, current stop, proposed stop) with each stop's distance from
   the entry in pips (ARM notices; owner decision 2026-11-21, `.claude/rules/riskmgr.md` rule 5), the alert's
   story (what is coming, this email), the halt or rule level ladder, the facts;
6. a block for the operator with the raw codes (old subject, context, ids, UTC time, node and epoch) and the
   read-only footer.

**Content** (`operations.md` §9): ids, codes, counts, order levels and times. Never a tick, bar, quote, spread,
indicator value, favour R, account size, balance, any currency amount or the instrument's pip / point size (a pip
distance is shown; the unit it was computed with is not).

**Mail-client safety:** tables only, every style inline, no `<style>`, images, fonts, scripts or links; solid colours
only (the site's translucent lines are pre-blended onto the card); every dynamic value is cut and `html.escape`d.
Every text colour is at least 4.5:1 on its background; accent fills carry dark text. The page declares
`color-scheme: dark` (it is a dark design), so a client that recolours dark mail may still alter it.
"""
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Final

from src.notifications.email_html import (
    ID_LIMIT,
    LINE_LIMIT,
    MELBOURNE,
    RULE_NAMES,
    VALUE_LIMIT,
    Context,
    FooterSource,
    _e,
    _id,
    _safe_stamp,
    raw_body,
    title_of,
)
from src.oversee.notify import AlertMessage

THEMES: Final = ("xtnl", "classic")
DEFAULT_THEME: Final = "xtnl"

# ---------------------------------------------------------------------------------------------------- palette
CANVAS: Final = "#020508"
BASE: Final = "#04080f"
CARD: Final = "#0b1622"
RAISED: Final = "#0f1e2e"
LINE: Final = "#1a242f"       # #ffffff0f on the card
LINE_HI: Final = "#26303a"    # #ffffff1c on the card
INK0: Final = "#eef2f8"
INK1: Final = "#9ab0c8"
INK2: Final = "#5a7490"       # lines and dots only (3.8:1 on the card)
INK3: Final = "#2a3d52"       # the story rail only
GREEN: Final = "#00cc7a"
ON_FILL: Final = "#04080f"
SANS: Final = "Inter,-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
MONO: Final = "'JetBrains Mono',Consolas,Menlo,monospace"

#: severity key -> (accent, label)
SEVERITY: Final[dict[str, tuple[str, str]]] = {
    "urgent": ("#f03a57", "Urgent"),
    "important": ("#f0a030", "Important"),
    "headsup": ("#4d9cf5", "Heads-up"),
    "info": ("#9ab0c8", "Info"),
    "resolved": ("#00cc7a", "Resolved"),
}
_TOKEN_SEVERITY: Final = {"CRITICAL": "urgent", "FAULT": "urgent", "HIGH": "important", "FINAL": "important",
                          "FLAG": "headsup", "LATE": "headsup", "CLEARED": "resolved"}
WHO: Final = {"ACT": "Action for you", "FIX": "Action for the operator", "FYI": "No action needed",
              "DONE": "Closed, no action needed"}
PLACES: Final = {"broker": "At the broker", "server": "Server PC", "check": "Check", "wait": "Just wait"}
READ_ONLY: Final = ("Phase 1 is read-only: this server watches your broker account and never places, changes or "
                    "cancels orders. Any change is yours to make at the broker.")
HALT_LADDER: Final = (("NORMAL", "trade as usual"), ("SOFT", "don't add new risk"), ("HARD", "stop all trading"))
RULE_LADDER: Final = (("PASS", "fine"), ("FLAG", "heads-up"), ("SOFT", "don't add risk"), ("HARD", "stop trading"))


def blend(color: str, alpha: float, under: str = CARD) -> str:
    """`color` at `alpha` over `under`, as a solid colour (mail clients drop rgba unevenly)."""
    def rgb(h: str) -> tuple[int, int, int]:
        h = h.lstrip("#")
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    top, bot = rgb(color), rgb(under)
    return "#" + "".join(f"{round(t * alpha + b * (1 - alpha)):02x}" for t, b in zip(top, bot, strict=True))


def severity_of(token: str) -> str:
    return _TOKEN_SEVERITY.get(token, "info")


def melbourne_when(ms: int) -> str:
    """`Fri 20 Nov, 20:14 AEDT`; "time not recorded" when it cannot be shown."""
    if ms <= 0:
        return "time not recorded"
    try:
        t = datetime.fromtimestamp(ms / 1000, tz=UTC).astimezone(MELBOURNE)
        return f"{t:%a} {t.day} {t:%b}, {t:%H:%M} {t.tzname()}"
    except (OverflowError, OSError, ValueError):
        return "time not recorded"


def melbourne_clock(ms: int) -> str:
    try:
        return f"{datetime.fromtimestamp(ms / 1000, tz=UTC).astimezone(MELBOURNE):%H:%M}"
    except (OverflowError, OSError, ValueError):
        return "--:--"


# ---------------------------------------------------------------------------------------------------- pips
@dataclass(frozen=True)
class PipUnit:
    """The size of one pip (or point) of an instrument and the word for it. Used to compute a distance; the size
    itself is never shown."""

    size: Decimal
    word: str = "pips"     # "pips" (FX) or "points"


def pip_unit_from(point_size: Decimal | None, symbol: str, asset_class: str | None = None) -> PipUnit | None:
    """The unit from an instrument's `point_size` (the adapters' point is the pip: `stonex/instruments.py`,
    `pepperstone/instruments.py`). "pips" for FX (asset class FX / FOREX / CURRENCY, or an unclassified six-letter
    currency pair), else "points". None when the size is missing or not positive."""
    if point_size is None or point_size <= 0:
        return None
    cls = (asset_class or "").strip().upper()
    if cls:
        fx = cls in ("FX", "FOREX", "CURRENCY", "CURRENCIES")
    else:
        fx = re.fullmatch(r"[A-Z]{3}[/._-]?[A-Z]{3}", symbol.strip().upper()) is not None
    return PipUnit(point_size, "pips" if fx else "points")


def pip_distance(level: Decimal, entry: Decimal, unit: PipUnit) -> tuple[Decimal, str]:
    """(distance in pips, rounded to 0.1, "below" | "above" | "at") of `level` from `entry`."""
    d = ((level - entry) / unit.size).quantize(Decimal("0.1"))
    where = "below" if d < 0 else "above" if d > 0 else "at"
    return abs(d), where


# ---------------------------------------------------------------------------------------------------- blocks
def t(text: object, limit: int = VALUE_LIMIT) -> str:
    """Escaped text (cut to `limit`)."""
    return _e(text, limit)


def row(inner: str, pad: str = "0 22px 14px") -> str:
    return f'<tr><td style="padding:{pad};">{inner}</td></tr>\n'


def mono_label(text: str, color: str = INK1) -> str:
    return (f'<div style="font:700 11px/1.4 {MONO};color:{color};letter-spacing:.14em;text-transform:uppercase;'
            f'padding-bottom:8px;">{t(text, ID_LIMIT)}</div>')


def chip(text: str, *, fill: str | None = None, edge: str = LINE_HI, ink: str = INK0) -> str:
    style = (f"background-color:{fill};color:{ON_FILL};border:1px solid {fill};" if fill
             else f"background-color:{CARD};color:{ink};border:1px solid {edge};")
    return (f'<span style="display:inline-block;padding:2px 8px;margin:0 6px 4px 0;border-radius:4px;{style}'
            f'font:700 11px/1.5 {MONO};letter-spacing:.1em;text-transform:uppercase;white-space:nowrap;">'
            f'{t(text, ID_LIMIT)}</span>')


def header_row(accent: str) -> str:
    brand = (f'<span style="font:800 16px/1 {MONO};color:{GREEN};">&#x2715;</span>'
             f'<span style="font:800 15px/1 {MONO};color:{INK0};letter-spacing:.3em;padding-left:8px;">XTNL</span>')
    ticker = f'<span style="color:{GREEN};">&#x25CF;</span> READ-ONLY &middot; PHASE 1'
    return (f'<tr><td bgcolor="{BASE}" style="background-color:{BASE};border-top:3px solid {accent};'
            f'padding:14px 22px 12px;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'border="0"><tr><td valign="middle">{brand}</td><td align="right" valign="middle" '
            f'style="font:600 10px/1.4 {MONO};color:{INK1};letter-spacing:.12em;">{ticker}</td></tr></table>'
            f'</td></tr>\n')


def masthead_rows(word: str, segment: str, audience: str, title: str, severity: str, stage: str,
                  when: str) -> str:
    accent, label = SEVERITY[severity]
    for_whom = audience if audience.lower().startswith("for ") else f"for {audience}"
    kicker = f"{segment} · {for_whom}"
    sev_chip = chip(label, edge=accent, ink=accent if severity != "urgent" else INK0)
    return (row(f'{chip(word, fill=accent)}<span style="font:700 11px/1.5 {MONO};color:{INK1};letter-spacing:.14em;'
                f'text-transform:uppercase;">{t(kicker, LINE_LIMIT)}</span>', "18px 22px 6px")
            + row(f'<div style="font:800 22px/1.3 {SANS};color:{INK0};">{t(title, LINE_LIMIT)}</div>',
                  "4px 22px 10px")
            + row(f'{sev_chip}<span style="font:13px/1.6 {SANS};color:{INK1};">{t(stage, ID_LIMIT)} &middot; '
                  f'{t(when, ID_LIMIT)} &middot; {t(WHO.get(word, ""), ID_LIMIT)}</span>', "0 22px 16px"))


def steps_rows(steps: Sequence[tuple[str, str]], accent: str, *, heading: str = "To do") -> str:
    if not steps:
        return ""
    items = "".join(
        f'<tr><td width="34" valign="top" style="padding:0 0 12px;"><div style="width:24px;height:24px;'
        f'line-height:24px;text-align:center;border:1px solid {accent};border-radius:4px;'
        f'background-color:{blend(accent, .10)};font:700 12px/24px {MONO};color:{INK0};">{n:02d}</div></td>'
        f'<td valign="top" style="padding:1px 0 12px;">{chip(PLACES.get(place, place))}'
        f'<div style="font:15px/1.5 {SANS};color:{INK0};">{t(text, LINE_LIMIT)}</div></td></tr>'
        for n, (place, text) in enumerate(steps, 1))
    return row(f'{mono_label(heading)}<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
               f'border="0">{items}</table>', "4px 22px 6px")


def nothing_row(text: str, accent: str) -> str:
    return row(f'<div style="background-color:{blend(accent, .10)};border-left:3px solid {accent};padding:12px 14px;'
               f'font:600 15px/1.5 {SANS};color:{INK0};">{t(text, LINE_LIMIT)}</div>')


@dataclass(frozen=True)
class Level:
    """One order level for the levels block. `distance`: (pips, where, word) from the entry, or None."""

    label: str
    price: str | None                       # None: no stop
    distance: tuple[Decimal, str, str] | None = None
    key: bool = False


def _bar(pct: float, color: str) -> str:
    pct = max(4.0, min(100.0, pct))
    rest = ("" if pct >= 100 else
            f'<td bgcolor="{LINE_HI}" style="background-color:{LINE_HI};height:6px;font-size:0;line-height:0;">'
            f'&nbsp;</td>')
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td width="{pct:.0f}%" bgcolor="{color}" style="background-color:{color};height:6px;font-size:0;'
            f'line-height:0;">&nbsp;</td>{rest}</tr></table>')


def levels_rows(heading: str, levels: Sequence[Level], accent: str, note: str = "") -> str:
    """Entry and stops; each stop with its distance from the entry and a bar (the longest distance fills it)."""
    if not levels:
        return ""
    longest = max((float(x.distance[0]) for x in levels if x.distance is not None), default=0.0)
    out = []
    for n, x in enumerate(levels):
        top = "" if n == 0 else f"border-top:1px solid {LINE};"
        bg = blend(accent, .08, RAISED) if x.key else RAISED
        edge = accent if x.key else RAISED
        label = (f'<div style="font:700 10px/1.4 {MONO};color:{INK0 if x.key else INK1};letter-spacing:.14em;'
                 f'text-transform:uppercase;">{t(x.label, ID_LIMIT)}</div>')
        bar = ""
        if x.price is None:
            price = f'<div style="font:800 17px/1.35 {MONO};color:{accent};">NONE</div>'
            dist = f'<div style="font:700 12px/1.4 {MONO};color:{accent};">no limit</div>'
            bar = f'<div style="border:1px dashed {accent};height:4px;line-height:4px;font-size:0;">&nbsp;</div>'
        else:
            price = (f'<div style="font:{"800" if x.key else "700"} 17px/1.35 {MONO};color:{INK0};">'
                     f'{t(x.price, ID_LIMIT)}</div>')
            if x.distance is None:
                ref = "reference" if n == 0 and x.label == "Entry" else ""    # the entry, not a lone stop
                dist = f'<div style="font:12px/1.4 {SANS};color:{INK1};">{ref}</div>'
            else:
                pips, where, word = x.distance
                dist = (f'<div style="font:700 14px/1.3 {MONO};color:{INK0};white-space:nowrap;">{pips} '
                        f'{t(word, ID_LIMIT)}</div><div style="font:12px/1.3 {SANS};color:{INK1};">'
                        f'{t(where, ID_LIMIT)} entry</div>')
                if longest > 0:
                    bar = _bar(100 * float(pips) / longest, accent if x.key else INK2)
        out.append(
            f'<tr><td bgcolor="{bg}" style="background-color:{bg};{top}border-left:3px solid {edge};'
            f'padding:9px 12px 10px;"><table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'border="0"><tr><td valign="top">{label}{price}</td><td valign="top" align="right" width="42%">{dist}'
            f'</td></tr>' + (f'<tr><td colspan="2" style="padding-top:7px;">{bar}</td></tr>' if bar else "")
            + '</table></td></tr>')
    foot = f'<div style="font:13px/1.5 {SANS};color:{INK1};padding-top:8px;">{t(note, LINE_LIMIT)}</div>' if note else ""
    return row(f'{mono_label(heading)}<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
               f'border="0" style="border:1px solid {LINE};">{"".join(out)}</table>{foot}', "4px 22px 14px")


@dataclass(frozen=True)
class Event:
    """One point of the story: kind is next | now | linked | past."""

    time: str
    label: str
    text: str
    kind: str


def story_rows(events: Sequence[Event], accent: str) -> str:
    if len(events) < 2:
        return ""
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
            dot = (f'<div style="width:10px;height:10px;border-radius:6px;background-color:{INK2};'
                   f'border:2px solid {INK2};"></div>')
        tag = {"next": "Coming", "now": "This email", "linked": "Linked"}.get(ev.kind, "")
        tag_html = (f'&nbsp;<span style="font:700 10px/1.4 {MONO};color:{INK0 if now else INK1};letter-spacing:.12em;'
                    f'text-transform:uppercase;">&middot; {tag}</span>') if tag else ""
        bg = RAISED if now else CARD
        edge = accent if now else (INK3 if not last else CARD)
        ink = INK0 if ev.kind != "next" else INK1
        out.append(
            f'<tr><td width="62" valign="top" style="padding:3px 8px 0 0;text-align:right;'
            f'font:{"700" if now else "500"} 12px/1.4 {MONO};color:{INK0 if now else INK1};">{t(ev.time, ID_LIMIT)}'
            f'</td><td width="16" valign="top" align="right" style="padding:4px 0 0;">{dot}</td>'
            f'<td valign="top" bgcolor="{bg}" style="background-color:{bg};border-left:2px solid {edge};'
            f'padding:3px 10px 12px 12px;"><div style="font:700 14px/1.4 {SANS};color:{ink};">'
            f'{t(ev.label, ID_LIMIT)}{tag_html}</div><div style="font:{"italic " if ev.kind == "next" else ""}'
            f'14px/1.5 {SANS};color:{ink if now else INK1};">{t(ev.text, LINE_LIMIT)}</div></td></tr>')
    return row(f'{mono_label("The story so far · newest first")}<table role="presentation" width="100%" '
               f'cellpadding="0" cellspacing="0" border="0">{"".join(out)}</table>', "8px 22px 10px")


def ladder_rows(heading: str, rungs: Sequence[tuple[str, str]], now: int, was: int | None, accent: str) -> str:
    if not 0 <= now < len(rungs):
        return ""
    cells = []
    width = 100 // len(rungs)
    for i, (name, hint) in enumerate(rungs):
        on = i == now
        before = was is not None and i == was and not on
        if on:
            style, ink = f"background-color:{accent};border:1px solid {accent};", ON_FILL
        elif before:
            style, ink = f"background-color:{CARD};border:1px dashed {INK1};", INK1
        else:
            style, ink = f"background-color:{CARD};border:1px solid {LINE_HI};", INK1
        tag = "now" if on else "was" if before else "&nbsp;"
        cells.append(f'<td width="{width}%" align="center" valign="top" style="{style}padding:8px 4px 9px;">'
                     f'<div style="font:700 10px/1.4 {MONO};color:{ink};letter-spacing:.14em;'
                     f'text-transform:uppercase;">{tag}</div><div style="font:800 13px/1.3 {MONO};color:{ink};'
                     f'letter-spacing:.06em;">{t(name, ID_LIMIT)}</div><div style="font:12px/1.35 {SANS};'
                     f'color:{ink};">{t(hint, ID_LIMIT)}</div></td>')
    return row(f'{mono_label(heading)}<table role="presentation" width="100%" cellpadding="0" cellspacing="4" '
               f'border="0"><tr>{"".join(cells)}</tr></table>', "4px 22px 14px")


def facts_rows(facts: Sequence[tuple[str, str]]) -> str:
    if not facts:
        return ""
    cells = [f'<td width="50%" valign="top" style="padding:10px 12px;border-top:1px solid {LINE};">'
             f'<div style="font:700 10px/1.4 {MONO};color:{INK1};letter-spacing:.14em;text-transform:uppercase;">'
             f'{t(k, ID_LIMIT)}</div><div style="font:700 15px/1.4 {MONO};color:{INK0};padding-top:2px;'
             f'word-break:break-word;">{t(v)}</div></td>' for k, v in facts]
    if len(cells) % 2:
        cells.append(f'<td width="50%" style="border-top:1px solid {LINE};">&nbsp;</td>')
    rows = "</tr><tr>".join("".join(cells[i:i + 2]) for i in range(0, len(cells), 2))
    return row(f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
               f'bgcolor="{RAISED}" style="background-color:{RAISED};border:1px solid {LINE};"><tr>{rows}</tr>'
               f'</table>', "4px 22px 14px")


def table_rows(head: Sequence[str], body: Sequence[Sequence[str]], note: str | None = None) -> str:
    if not head or not body:
        return ""
    th = "".join(f'<td style="padding:6px 8px;font:700 10px/1.4 {MONO};color:{INK1};letter-spacing:.12em;'
                 f'text-transform:uppercase;border-bottom:1px solid {LINE_HI};">{t(h, ID_LIMIT)}</td>' for h in head)
    trs = "".join("<tr>" + "".join(f'<td style="padding:6px 8px;font:13px/1.4 {MONO};color:{INK0};'
                                   f'border-bottom:1px solid {LINE};word-break:break-word;">{t(c)}</td>' for c in r)
                  + "</tr>" for r in body)
    extra = f'<div style="padding-top:6px;font:12px/1.4 {SANS};color:{INK1};">{t(note, LINE_LIMIT)}</div>' if note else ""
    return row(f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
               f'bgcolor="{RAISED}" style="background-color:{RAISED};border:1px solid {LINE};"><tr>{th}</tr>{trs}'
               f'</table>{extra}', "4px 22px 14px")


def prose_row(label: str, text: str) -> str:
    if not text:
        return ""
    return row(f'<div style="font:15px/1.6 {SANS};color:{INK1};"><span style="color:{INK0};font-weight:700;">'
               f'{t(label, ID_LIMIT)}</span> {t(text, LINE_LIMIT)}</div>')


def operator_rows(lines: Sequence[tuple[str, str]], detail: str | None = None) -> str:
    body = "".join(f'<div><span style="color:{INK1};">{t(k, ID_LIMIT)}</span>&nbsp; <span style="color:{INK0};">'
                   f'{t(v, LINE_LIMIT)}</span></div>' for k, v in lines)
    if detail:
        body += (f'<div style="padding-top:6px;color:{INK1};white-space:pre-wrap;">{t(detail, 2000)}</div>')
    return row(f'<div style="background-color:{BASE};border:1px solid {LINE};padding:10px 12px;">'
               f'{mono_label("For the operator")}<div style="font:11px/1.7 {MONO};word-break:break-all;">{body}</div>'
               f'</div>', "2px 22px 16px")


def node_epoch(footer: FooterSource | None) -> tuple[str, str]:
    node, epoch = "not recorded", None
    if footer is not None:
        try:
            node, epoch = footer()
        except Exception:   # a footer that cannot be read never costs the email
            node, epoch = "not recorded", None
    return _id(node), _id(epoch) if epoch else "no leader term"


def footer_row() -> str:
    return (f'<tr><td bgcolor="{BASE}" style="background-color:{BASE};border-top:1px solid {LINE};'
            f'padding:14px 22px 18px;font:12px/1.6 {SANS};color:{INK1};">{t(READ_ONLY, LINE_LIMIT)}<br>'
            f'Times are Melbourne time; UTC is in the operator details.<br>'
            f'<span style="font:700 10px/2 {MONO};letter-spacing:.2em;color:{INK1};">XTNL SOLUTIONS &middot; '
            f'ORDER-SERVER</span></td></tr>\n')


MSO_OPEN: Final = ('<!--[if mso]><table role="presentation" width="600" align="center" cellpadding="0" '
                   'cellspacing="0" border="0"><tr><td><![endif]-->')
MSO_CLOSE: Final = "<!--[if mso]></td></tr></table><![endif]-->"


def theme_page(*, title: str, preheader: str, accent: str, rows: Sequence[str]) -> str:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="color-scheme" content="dark"><meta name="supported-color-schemes" content="dark">'
        f"<title>{t(title, LINE_LIMIT)}</title></head>\n"
        f'<body style="margin:0;padding:0;background-color:{BASE};">\n'
        f'<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;color:{BASE};">'
        f'{t(preheader, LINE_LIMIT)}</div>\n'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{BASE}" '
        f'style="background-color:{BASE};"><tr><td align="center" style="padding:16px 8px;">\n{MSO_OPEN}\n'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{CARD}" '
        f'style="max-width:600px;width:100%;background-color:{CARD};border:1px solid {LINE};">\n'
        + header_row(accent) + "".join(rows) + footer_row()
        + f"</table>\n{MSO_CLOSE}\n</td></tr></table>\n</body></html>\n")


# ---------------------------------------------------------------------------------------------------- Oversee path
DAILY_RULES: Final = frozenset({"max_daily_loss", "sod_partial_session"})
BROKER_OPS: Final = frozenset({
    "ops.catchup", "ops.catchup_incomplete", "ops.clock_moved_back", "ops.cash_flow_ledger_incomplete",
    "ops.auth_failed", "ops.pre_open_failed", "ops.ingestion", "ops.consumer", "ops.adapter_rearmed", "ops.calendar",
    "ops.sessions_review", "ops.session_unusable", "ops.sessions_not_saved", "ops.maintenance_unreadable",
    "ops.maintenance",
})
DONE_CONTEXTS: Final = frozenset({"ops.adapter_rearmed", "ops.recording_again", "halt.cleared",
                                  "halt.cleared_by_operator", "oversee.resolved"})
DONE_CODES: Final = frozenset({"integrity_resolved", "quarantine_released", "unlisted_states_resolved"})
#: plain words for a rule (the headline); the rule id stays in the operator block
RULE_PLAIN: Final[dict[str, str]] = {
    "missing_stop_loss": "no stop loss",
    "max_position_risk": "one trade risks too much",
    "max_aggregate_open_risk": "your open trades together risk too much",
    "max_daily_loss": "daily loss limit",
    "margin_usage": "margin use is high",
    "stale_account_image": "account data from the broker is out of date",
    "stale_mark": "prices are out of date",
    "sod_partial_session": "start-of-day snapshot is partial",
}
REASON_PLAIN: Final[dict[str, str]] = {
    "transfers_unverified": "Today's deposits and withdrawals are not confirmed",
    "pnl_unverified": "Today's trading result is not confirmed by the broker",
    "unexplained_balance_delta": "An account change has no matching trade or transfer",
    "equity_divergence": "The server's account mark differs from the broker's",
}
#: plain headlines of operational pages; `{rest}` is the title after its first ": " (an adapter, task or code)
PLAIN_OPS: Final[dict[str, str]] = {
    "ops.auth_failed": "The broker refused the server's login ({rest})",
    "ops.adapter_rearmed": "Broker connection re-armed ({rest})",
    "ops.clock_moved_back": "The PC clock moved back ({rest})",
    "ops.cash_flow_ledger_incomplete": "Deposits and withdrawals could not all be read ({rest})",
    "ops.pre_open_failed": "Not ready for the market open ({rest})",
    "ops.ingestion": "Broker data interrupted ({rest})",
    "ops.not_recording": "The server stopped recording the broker",
    "ops.recording_again": "The server is recording again",
    "ops.safe_hold_stopped": "Automatic recovery stopped on the server PC",
    "ops.safe_hold_resumed": "The server recovered by itself",
    "ops.task_stale": "A server task stopped reporting ({rest})",
    "ops.task_circuit": "A server task keeps failing ({rest})",
    "oversee.feed_lag": "Risk checks are behind the broker feed",
    "integrity.page": "Data integrity: {rest}",
}
_TRANSITION: Final = re.compile(r"^(?P<t>lowered|resolved|capped|uncapped) (?P<rule>\S+) (?P<stype>[^\s:]+):(?P<sid>\S+)")
_CODE: Final = re.compile(r"^(?:oversee|integrity):(?P<code>[a-z_]+)$")


@dataclass(frozen=True)
class Ticket:
    word: str
    segment: str
    audience: str
    severity: str
    title: str
    stage: str
    steps: tuple[tuple[str, str], ...] = ()
    nothing: str = ""
    story: tuple[Event, ...] = ()


def _no_action(text: str | None) -> bool:
    return text is None or text.strip().lower().startswith("no action")


def _place(text: str) -> str:
    low = text.lower()
    if "at the broker" in low or "broker's order history" in low or "credential" in low:
        return "broker"
    if low.startswith(("check", "review", "confirm", "look")):
        return "check"
    return "server"


def _rule_of(msg: AlertMessage, ctx: Context) -> tuple[str, str, str]:
    """(rule id, subject type, subject id) of a rule context, from the episode or the transition title."""
    if msg.episode is not None:
        return msg.episode[0], msg.episode[1], msg.episode[2]
    m = _TRANSITION.match(title_of(msg))
    if m:
        return m.group("rule"), m.group("stype"), m.group("sid")
    return "", "", ""


def _reason(msg: AlertMessage) -> str:
    return raw_body(msg).split(" (", 1)[0].strip()


def _subject_words(stype: str, sid: str) -> str:
    if stype in ("order", "position"):
        return f"{stype} {sid}"
    return f"account {sid.split(':', 1)[0]}" if stype.startswith("account") else f"{stype} {sid}".strip()


def ticket(msg: AlertMessage, ctx: Context) -> Ticket:
    """Status word, segment, audience, severity, plain headline, stage, steps and story of an Oversee-path alert
    (the segment / audience table of `prototype-C2-spec.md`)."""
    name, when = ctx.name, melbourne_clock(msg.created_ms)
    sev = severity_of(ctx.token)
    if name in ("oversee.episode", "oversee.repage", "oversee.sod_partial_session"):
        rule_id, stype, sid = _rule_of(msg, ctx)
        level = ctx.rule_level if ctx.rule_level is not None else 0
        repage = name == "oversee.repage"
        segment = "Daily account check" if rule_id in DAILY_RULES else "Risk check"
        what = _subject_words(stype, sid)
        plain = RULE_PLAIN.get(rule_id, RULE_NAMES.get(rule_id, rule_id or "a risk rule"))
        if level >= 2:
            if rule_id == "missing_stop_loss":
                head = f"Add a stop loss to {what}"
            elif rule_id == "max_daily_loss" and level >= 3:
                head = "Stop trading for today: daily loss limit"
            else:
                head = f"{plain[0].upper()}{plain[1:]}: {what}"
            title = f"Still open: {head[0].lower()}{head[1:]}" if repage else head
            steps: list[tuple[str, str]] = [("broker", ctx.action or "Review this at the broker.")]
            steps.append(("server", "If it is expected, the operator can acknowledge it to stop the reminders.")
                         if repage else ("wait", "Nothing else. This alert closes by itself once the server sees "
                                                 "the change."))
            word, audience = "ACT", "You, the trader"
        else:
            reason = REASON_PLAIN.get(_reason(msg))
            title = (reason or f"Heads-up: {plain} ({what})")
            steps = []
            word, audience = "FYI", "For your records"
        story = (Event("later", "Reminder", "while it stays open", "next"),
                 Event(when, "Reminder" if repage else "New", "Still open. This email." if repage else "This email.",
                       "now"))
        if word == "FYI":
            return Ticket(word, segment, audience, sev, title, "Reminder" if repage else "New",
                          nothing=ctx.action if not _no_action(ctx.action) and ctx.action else "Nothing to do.",
                          story=story)
        return Ticket(word, segment, audience, sev, title, "Reminder, still open" if repage else "New",
                      tuple(steps), story=story)
    if name.startswith("oversee.") and _TRANSITION.match(title_of(msg)):
        rule_id, stype, sid = _rule_of(msg, ctx)
        segment = "Daily account check" if rule_id in DAILY_RULES else "Risk check"
        plain = RULE_PLAIN.get(rule_id, RULE_NAMES.get(rule_id, rule_id))
        what = _subject_words(stype, sid)
        closed = name == "oversee.resolved" or (name == "oversee.lowered" and ctx.rule_level == 0)
        if closed:
            return Ticket("DONE", segment, "You, the trader", "resolved", f"Closed: {plain} ({what})", "Closed",
                          nothing="Nothing to do.", story=())
        return Ticket("FYI", segment, "For your records", sev, f"{ctx.headline}", "Update",
                      nothing=ctx.action if not _no_action(ctx.action) and ctx.action else "Nothing to do.")
    if name in ("halt.soft", "halt.hard"):
        hard = name == "halt.hard"
        if hard:
            steps = [("broker", "Stop trading: don't open or add to any trade."),
                     ("server", "The operator reviews, then clears the halt. It does not clear by itself.")]
            title, coming = "Stop trading: the halt level is HARD", Event("after review", "Operator clear",
                                                                            "the operator reviews and clears it", "next")
        else:
            steps = [("broker", "Don't open new trades or add to positions for now."),
                     ("broker", "Fix the cause named below."),
                     ("wait", "The level goes back to NORMAL after a cool-down once nothing is open.")]
            title, coming = "Hold off on new trades: the halt level is SOFT", Event(
                "later", "Back to NORMAL", "after the cool-down, if nothing else is open", "next")
        return Ticket("ACT", "Halt level", "You, the trader", sev, title, "Level up", tuple(steps),
                      story=(coming, Event(when, "Halt level", "Raised. This email.", "now")))
    if name.startswith("halt.cleared"):
        return Ticket("DONE", "Halt level", "You, the trader", "resolved",
                      "Halt level is back to NORMAL" if name == "halt.cleared" else ctx.headline, "Level down",
                      nothing=ctx.action if not _no_action(ctx.action) and ctx.action else "Nothing to do. You can "
                                                                                         "trade as usual.")
    # operational, integrity, ARM status and generic pages
    code_m = _CODE.match(title_of(msg))
    code = code_m.group("code") if code_m else ""
    if name.startswith("arm.") or name == "md.page":
        segment = "ARM advice"
    elif name in BROKER_OPS:
        segment = "Broker connection and data"
    else:
        segment = "System health"
    if name in DONE_CONTEXTS or code in DONE_CODES:
        word = "DONE"
    elif name == "ops.catchup" and _no_action(ctx.action):
        word = "FYI"
    elif name == "generic":
        word = "FIX" if ctx.token in ("CRITICAL", "HIGH") else "FYI"
    else:
        word = "FYI" if _no_action(ctx.action) else "FIX"
    audience = "For your records" if word == "FYI" else "Operator"
    steps = []
    if word == "FIX":
        if ctx.steps:
            steps = [(_place(s.text), s.text + (f" ({s.command})" if s.command else "")) for s in ctx.steps]
        elif ctx.action:
            steps = [(_place(ctx.action), ctx.action)]
    nothing = "" if steps else (ctx.action if not _no_action(ctx.action) and ctx.action else "Nothing to do.")
    headline = ctx.headline
    if name in PLAIN_OPS and headline == title_of(msg):   # only where the classic headline is the raw title
        raw = title_of(msg)
        rest = raw.split(": ", 1)[1] if ": " in raw else raw.split(":", 1)[-1]
        if name == "integrity.page":
            rest = rest.replace("_", " ")
        headline = PLAIN_OPS[name].format(rest=_id(rest)).replace(" ()", "")
    return Ticket(word, segment, audience, "resolved" if word == "DONE" else sev, headline,
                  {"DONE": "Closed", "FYI": "Update"}.get(word, "New"), tuple(steps), nothing)


def _context_facts(ctx: Context) -> list[tuple[str, str]]:
    skip = {"Alert type", "Level", "Rule"}
    facts = [(f.key, f.value) for f in ctx.fields if f.key not in skip]
    facts += [(tl.label, tl.number) for tl in ctx.tiles]
    facts += [(label, text) for label, text in ctx.window]
    return facts


def render_oversee(msg: AlertMessage, ctx: Context, footer: FooterSource | None = None) -> str:
    """The XTNL-theme page of an Oversee-path alert (every context `email_html.classify` knows)."""
    tk = ticket(msg, ctx)
    accent = SEVERITY[tk.severity][0]
    rows = [masthead_rows(tk.word, tk.segment, tk.audience, tk.title, tk.severity, tk.stage,
                          melbourne_when(msg.created_ms))]
    if tk.steps:
        rows.append(steps_rows(tk.steps, accent))
    else:
        rows.append(nothing_row(tk.nothing or "Nothing to do.", accent))
    rows.append(story_rows(tk.story, accent))
    if ctx.halt_level is not None:
        # no "was" rung: the alert does not carry the level it rose from
        rows.append(ladder_rows("Halt level", HALT_LADDER, ctx.halt_level, None, accent))
    elif ctx.rule_level is not None:
        rows.append(ladder_rows("Rule level", RULE_LADDER, ctx.rule_level, None, accent))
    rows.append(facts_rows(_context_facts(ctx)))
    rows.append(table_rows(ctx.table_head, ctx.table, ctx.table_note))
    node, epoch = node_epoch(footer)
    ops = [("Subject", msg.subject), ("Context", f"{ctx.name} · {ctx.badge} · {ctx.tag}"),
           ("Alert", f"{msg.kind}, {msg.severity.upper()}" + (f", alert #{msg.outbox_id}" if msg.outbox_id else "")),
           ("Raised", _safe_stamp(msg.created_ms) if msg.created_ms > 0 else "time not recorded"),
           ("Node", f"{node} · epoch {epoch}")]
    if msg.episode is not None:
        ops.append(("Episode", " ".join(str(x) for x in msg.episode)))
    rows.append(operator_rows(ops, ctx.detail))
    pre = f"{tk.word} · {tk.segment} · {SEVERITY[tk.severity][1]} · {tk.title}"
    return theme_page(title=msg.subject, preheader=pre, accent=accent, rows=rows)


__all__ = [
    "BASE", "CARD", "DEFAULT_THEME", "Event", "INK0", "INK1", "Level", "PipUnit", "SEVERITY", "THEMES", "Ticket",
    "chip", "facts_rows", "ladder_rows", "levels_rows", "masthead_rows", "melbourne_clock", "melbourne_when",
    "mono_label", "nothing_row", "operator_rows", "pip_distance", "pip_unit_from", "prose_row", "render_oversee",
    "row", "severity_of", "steps_rows", "story_rows", "table_rows", "theme_page", "ticket",
]
