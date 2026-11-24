"""Email-safe visual elements of the alert emails (design `docs/design/notifications/`, preview `preview.html`).

Pure HTML helpers used by `src.notifications.email_html` (layouts L1 to L4) and `email_html_arm` (L5, L6):

* V1 `hero_row`: a glyph tile beside the badges and the headline.
* V2 / V3 `rule_ladder` / `halt_ladder` (both on `level_ladder`): the rule level PASS · FLAG · SOFT · HARD and the
  halt level NONE · SOFT · HARD, the current level marked.
* V4 `stop_ladder`: the order-state stop levels a card already shows (proposed stop, current stop, entry; only with
  `show_levels`, since the card shows no current stop level without it) as rungs in price order, **ordinal**: fixed
  spacing, labelled "not to scale". Only `<`, `>` and `==` are used on the levels; no distance is computed or drawn.
* V5 `advice_steps`: Heads-up -> Final advice (-> Released) with the current step.
* V6 `count_tiles`, V7 `time_strip` (two ordered times, fixed height), V8 `side_chip` (neutral LONG / SHORT),
  V9 `step_list` + `command_chip`, V10 `summary_chips`.

**Rules:** tables, `td`, `span` and `br` only, `bgcolor` plus `background-color` on every cell, inline styles, no
`border-radius`, no images, SVG, gradients or positioning. Every text cell has a light background with dark text; a
token colour is only a border, a rail or a decorative glyph beside a text label. Every width and height is a fixed
constant, never computed from a price, a count or a time. Every dynamic value is cut and escaped (`_e`). A helper
returns `""` when its input is missing or unparsed; callers fence each call with `safe_part`, so a failing visual is
left out and the plain fields stay. Each visual carries an inert `class="xv-<name>"` marker (the email has no
`<style>`), read by the tests and the preview.

**Content:** the visuals show only what the page already carries (ids, codes, counts, order-state levels, times and
states). They never encode a price distance, risk, margin, P&L or any money amount.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from decimal import Decimal
from typing import Final, Protocol

from src.notifications.email_html import (
    _LEVEL_NAMES,
    FONT,
    ID_LIMIT,
    INK,
    LINE,
    LINE_LIMIT,
    MONO,
    MUTED,
    TOKENS,
    VALUE_LIMIT,
    Context,
    Step,
    Tile,
    _color,
    _ctx_tag,
    _e,
    badge,
    clock,
)

#: Light tint per token: the token colour, INK and MUTED on it all reach >= 4.5:1.
TINTS: Final[dict[str, str]] = {
    "CRITICAL": "#FEF3F2", "FAULT": "#FEF3F2", "HIGH": "#FFF4ED", "FLAG": "#FFFAEB", "LATE": "#FFFAEB",
    "INFO": "#EFF8FF", "CLEARED": "#ECFDF3", "PROVISIONAL": "#F2F4F7", "HOLD": "#F2F4F7", "FINAL": "#EEF4FF"}
NEUTRAL_BG: Final = "#F2F4F7"
SOFT_BG: Final = "#F9FAFB"
WHITE: Final = "#FFFFFF"
RULE_GREY: Final = "#D0D5DD"
DASH_GREY: Final = "#98A2B3"
PILL_INK: Final = "#344054"
GLYPHS: Final = frozenset("▲▼►●○■→↓")          # plus ASCII ! and i
VISUAL_BUDGET: Final = 45_000                    # bytes of ARM card HTML after which cards skip optional visuals
BREAK_EVEN_KIND: Final = "break_even_on_risk_close"
SIDE_CHIPS: Final[dict[str, str]] = {"BUY": "LONG (BUY)", "SELL": "SHORT (SELL)"}
SIDE_WORDS: Final[dict[str, str]] = {"BUY": "long", "SELL": "short"}
SUMMARY_ORDER: Final = ("FAULT", "FINAL", "PROVISIONAL", "FLAG", "HOLD", "LATE", "CLEARED", "INFO")

UP: Final = "▲"
DOWN: Final = "▼"
NEXT: Final = "►"
DOT: Final = "●"
RING: Final = "○"
SQUARE: Final = "■"
ARROW: Final = "→"
DOWN_ARROW: Final = "↓"
PAUSE: Final = "pause"
_ZERO: Final = "font-size:0;line-height:0;"


def safe_part(fn: Callable[..., str], *args: object, **kw: object) -> str:
    """`fn(*args)`, or `""` if it raises or returns something else than a string: a visual never costs the email."""
    try:
        out = fn(*args, **kw)
    except Exception:
        return ""
    return out if isinstance(out, str) else ""


def tint(token: str) -> str:
    return TINTS.get(token, NEUTRAL_BG)


def _td(inner: str, bg: str, style: str = "", attrs: str = "") -> str:
    """One cell: `bgcolor` and `background-color` always (Outlook reads the attribute, the rest the style)."""
    a = f"{attrs} " if attrs else ""
    return f'<td {a}bgcolor="{bg}" style="{style}background-color:{bg};">{inner}</td>'


def _table(rows: str, *, css_class: str = "", spacing: int = 0, style: str = "") -> str:
    cls = f'class="{css_class}" ' if css_class else ""
    st = f' style="{style}"' if style else ""
    return (f'<table {cls}role="presentation" width="100%" cellpadding="0" cellspacing="{spacing}" border="0"{st}>'
            f"{rows}</table>")


def _vrow(inner: str, padding: str = "0 20px 14px") -> str:
    return f"<tr>{_td(inner, WHITE, f'padding:{padding};')}</tr>\n"


# ---------------------------------------------------------------------------------------------------- V1 hero
_GLYPH_BY_CONTEXT: Final[dict[str, str]] = {
    "oversee.episode": UP, "oversee.repage": UP, "oversee.resolved": DOWN, "oversee.lowered": DOWN,
    "oversee.capped": SQUARE, "oversee.sod_partial_session": SQUARE, "oversee.uncapped": RING,
    "halt.soft": PAUSE, "halt.hard": PAUSE, "halt.cleared": DOWN, "halt.cleared_by_operator": DOWN,
}
_GLYPH_BY_LAYOUT: Final[dict[str, str]] = {"L1": UP, "L2": PAUSE, "L3": "i", "L4": "!", "L5": DOT, "L6": DOT}


def glyph_for(ctx: Context) -> str:
    """The hero glyph of an Oversee-path context (its name and layout; no data)."""
    if ctx.layout in ("L1", "L2") and ctx.name in _GLYPH_BY_CONTEXT:
        return _GLYPH_BY_CONTEXT[ctx.name]
    return _GLYPH_BY_LAYOUT.get(ctx.layout, "i")


def notice_glyph(token: str, layout: str) -> str:
    """The hero glyph of an ARM notice by its notice token."""
    if token == "FAULT":
        return "!"
    if layout == "L5":
        return {"FINAL": ARROW, "PROVISIONAL": RING}.get(token, DOT)
    return DOT


def _bar(bg: str, w: int) -> str:
    return (f'<td width="{w}" height="18" bgcolor="{bg}" style="width:{w}px;height:18px;background-color:{bg};'
            f'{_ZERO}">&nbsp;</td>')


def glyph_tile(glyph: str, token: str) -> str:
    """A 44 px square tile (tint, token border) with a glyph, or two bars for "pause" (no font involved)."""
    c, t = _color(token), tint(token)
    if glyph == PAUSE:
        inner = ('<table role="presentation" align="center" cellpadding="0" cellspacing="0" border="0"><tr>'
                 + _bar(c, 6) + _bar(t, 4) + _bar(c, 6) + "</tr></table>")
    elif glyph in GLYPHS or glyph in ("!", "i"):
        inner = glyph
    else:
        return ""
    return ('<table role="presentation" width="44" cellpadding="0" cellspacing="0" border="0" '
            'style="width:44px;min-width:44px;"><tr>'
            + _td(inner, t, f"width:40px;height:40px;border:2px solid {c};color:{c};font:700 22px/40px {FONT};"
                  "mso-line-height-rule:exactly;",
                  'width="44" height="44" align="center" valign="middle" aria-hidden="true"')
            + "</tr></table>")


def hero_row(badges: Sequence[tuple[str, str]], tag: str, headline: str, token: str, glyph: str,
             size: int = 20) -> str:
    """V1: glyph tile + the badges (the existing `badge()` spans) + the headline, as one page row."""
    marks = "".join(badge(lbl, tok) for lbl, tok in badges) + (_ctx_tag(tag) if tag else "")
    text_rows = (f"<tr>{_td(marks, WHITE, 'padding:0 0 4px;')}</tr>" if marks else "") + (
        f"<tr>{_td(_e(headline, LINE_LIMIT), WHITE, f'font:600 {size}px/1.35 {FONT};color:{INK};'
                   'word-break:break-word;overflow-wrap:anywhere;')}</tr>")
    text = _table(text_rows)
    tile = glyph_tile(glyph, token)
    if tile:
        inner = _table("<tr>" + _td(tile, WHITE, "width:56px;min-width:56px;padding:0 12px 0 0;", 'width="56" valign="top"')
                       + _td(text, WHITE, "", 'valign="top"') + "</tr>", css_class="xv-hero")
    else:
        inner = _table("<tr>" + _td(text, WHITE, "", 'valign="top"') + "</tr>", css_class="xv-hero")
    return _vrow(inner, "16px 20px 12px")


# ---------------------------------------------------------------------------------------------------- V2 / V3
_LADDER_WIDTHS: Final[dict[int, tuple[str, ...]]] = {3: ("33%", "33%", "34%"), 4: ("25%", "25%", "25%", "25%")}


def level_ladder(cells: Sequence[tuple[str, str]], current: int, token: str, now_text: str, caption: str,
                 dashed: int | None = None, css_class: str = "xv-level-ladder") -> str:
    """A segmented level bar: every cell the same geometry; the current one tinted with a token border, a dot and
    `now_text`; `dashed` (a capped level) drawn with a dashed grey border. A page row."""
    widths = _LADDER_WIDTHS.get(len(cells))
    if widths is None or not 0 <= current < len(cells):
        return ""
    c, t = _color(token), tint(token)
    tds: list[str] = []
    for i, ((label, sub), w) in enumerate(zip(cells, widths, strict=True)):
        attrs = f'width="{w}" align="center" valign="top"'
        sub_html = f'<br><span style="font:400 11px/1.3 {FONT};">{_e(sub, ID_LIMIT)}</span>' if sub else ""
        if i == current:
            sub_html = (f'<br><span style="font:400 11px/1.3 {FONT};color:{INK};">{_e(sub, ID_LIMIT)}</span>'
                        if sub else "")
            inner = (f'<span style="color:{c};">{DOT}</span> {_e(label, ID_LIMIT)}{sub_html}'
                     f'<br><span style="font:700 11px/1.3 {FONT};color:{INK};">{_e(now_text, ID_LIMIT)}</span>')
            tds.append(_td(inner, t, f"padding:8px 2px;border:2px solid {c};border-top:5px solid {c};"
                                     f"font:700 13px/1.3 {FONT};color:{INK};", attrs))
        elif i == dashed:
            tds.append(_td(_e(label, ID_LIMIT) + sub_html, WHITE,
                           f"padding:8px 2px;border:2px dashed {DASH_GREY};border-top:5px dashed {DASH_GREY};"
                           f"font:600 12px/1.3 {FONT};color:{MUTED};", attrs))
        else:
            tds.append(_td(_e(label, ID_LIMIT) + sub_html, NEUTRAL_BG,
                           f"padding:8px 2px;border:2px solid {NEUTRAL_BG};border-top:5px solid {RULE_GREY};"
                           f"font:600 12px/1.3 {FONT};color:{MUTED};", attrs))
    bar = _table("<tr>" + "".join(tds) + "</tr>", css_class=css_class, spacing=4)
    cap = f'<div style="font:12px/1.4 {FONT};color:{MUTED};">{_e(caption, LINE_LIMIT)}</div>' if caption else ""
    return _vrow(bar + cap)


_RULE_CELLS: Final = ("PASS", "FLAG", "SOFT", "HARD")
_RULE_TOKENS: Final = ("CLEARED", "FLAG", "HIGH", "CRITICAL")


def rule_ladder(ctx: Context) -> str:
    """V2: the rule level of an Oversee rule alert (episode, re-page, transitions); HARD dashed when capped at SOFT."""
    lvl = ctx.rule_level
    if lvl is None or not 0 <= lvl <= 3:
        return ""
    dashed = 3 if ctx.cap_soft and lvl < 3 else None
    cells = [(name, "capped" if i == dashed else "") for i, name in enumerate(_RULE_CELLS)]
    cap = next((f.value for f in ctx.fields if f.key == "Cap"), None)
    caption = (f"Rule level {_LEVEL_NAMES[lvl]}" + (f" ({ctx.level_note})" if ctx.level_note else "")
               + (f" · cap {cap}" if cap is not None else ""))
    now = f"now · {ctx.level_note}" if ctx.level_note else "now"
    return level_ladder(cells, lvl, _RULE_TOKENS[lvl], now, caption, dashed, "xv-rule-ladder")


_HALT_CELLS: Final = (("NONE", "no halt"), ("SOFT", "risk-reducing only"), ("HARD", "all denied"))
_HALT_TOKENS: Final = ("CLEARED", "HIGH", "CRITICAL")


def halt_ladder(ctx: Context) -> str:
    """V3: the halt level after a halt change (no previous level: no message carries it)."""
    lvl = ctx.halt_level
    if lvl is None or not 0 <= lvl <= 2:
        return ""
    name = _HALT_CELLS[lvl][0]
    caption = f"Halt level now {name}" + (f" ({ctx.level_note})" if ctx.level_note else "")
    now = f"now · {ctx.level_note}" if ctx.level_note else "now"
    return level_ladder(_HALT_CELLS, lvl, _HALT_TOKENS[lvl], now, caption, None, "xv-halt-ladder")


# ---------------------------------------------------------------------------------------------------- V4
class LadderItem(Protocol):
    """The ARM item fields the stop ladder reads (an `ItemBody`; this module never imports `src.riskmgr`)."""

    @property
    def proposed_stop(self) -> Decimal | None: ...
    @property
    def stop_now(self) -> Decimal | None: ...
    @property
    def entry(self) -> Decimal | None: ...
    @property
    def side(self) -> str: ...
    @property
    def stop_read_at_ms(self) -> int | None: ...
    @property
    def strategy_kind(self) -> str: ...


class LevelSettings(Protocol):
    @property
    def show_levels(self) -> bool: ...


def _clock_or_none(ms: int) -> str | None:
    try:
        return clock(ms)
    except (OverflowError, OSError, ValueError):
        return None


def stop_ladder(item: LadderItem, settings: LevelSettings, token: str, pre: bool) -> str:
    """V4: proposed stop, current stop and entry as rungs in price order, higher at top, with fixed 12 px spacing
    (ordinal, not to scale). Equal levels share a rung. Only with `show_levels` (Q13; §B8 "entry and current stop
    only if Q13"): without it the card shows no current stop level, so the ladder is left out. Also left out when a
    level is missing or not finite."""
    proposed, current = item.proposed_stop, item.stop_now
    if not settings.show_levels or proposed is None or current is None:
        return ""
    roles: list[tuple[str, Decimal]] = [("P", proposed), ("C", current)]
    if item.entry is not None:
        roles.append(("E", item.entry))
    if not all(isinstance(v, Decimal) and v.is_finite() for _, v in roles):
        return ""
    groups: list[tuple[Decimal, list[str]]] = []
    for role, v in roles:
        for g in groups:
            if g[0] == v:
                g[1].append(role)
                break
        else:
            groups.append((v, [role]))
    groups.sort(key=lambda g: g[0], reverse=True)
    names = {"P": "PROPOSED STOP (provisional)" if pre else "PROPOSED STOP", "C": "CURRENT STOP", "E": "ENTRY"}
    ip = next(i for i, g in enumerate(groups) if "P" in g[1])
    ic = next(i for i, g in enumerate(groups) if "C" in g[1])
    lo, hi = min(ip, ic), max(ip, ic)
    c, t = _color(token), tint(token)
    read = _clock_or_none(item.stop_read_at_ms) if item.stop_read_at_ms is not None else None
    side = SIDE_WORDS.get(item.side, "side not recorded")
    head = f"Stop levels in price order, higher at top · {_e(side, ID_LIMIT)} · not to scale"
    trs = ["<tr>" + _td(head, SOFT_BG, f"padding:6px 10px;font:600 12px/1.4 {FONT};color:{MUTED};", 'colspan="4"')
           + "</tr>"]
    for n, (value, rs) in enumerate(groups):
        label = " = ".join(names[r] for r in rs)
        if "P" in rs and "E" in rs and item.strategy_kind == BREAK_EVEN_KIND:
            label += " (break-even)"
        if "P" in rs and "C" in rs:
            sub = "same level"
        elif "P" in rs:
            sub = f"{UP} above current stop" if proposed > current else f"{DOWN} below current stop"
        elif "C" in rs and read is not None:
            sub = f"as read {read}"
        else:
            sub = ""
        if "P" in rs:
            bg, glyph, gc = t, SQUARE, c
        elif "C" in rs:
            bg, glyph, gc = SOFT_BG, RING, MUTED
        else:
            bg, glyph, gc = WHITE, DOT, MUTED
        rail = c if lo <= n <= hi else RULE_GREY
        sub_html = f'<br><span style="font:400 12px/1.35 {FONT};color:{MUTED};">{_e(sub, LINE_LIMIT)}</span>' \
            if sub else ""
        trs.append(
            "<tr>" + _td("&nbsp;", rail, f"width:6px;{_ZERO}", 'width="6"')
            + _td(glyph, bg, f"width:28px;font:700 14px/1 {FONT};color:{gc};",
                  'width="28" align="center" aria-hidden="true"')
            + _td(_e(label, LINE_LIMIT) + sub_html, bg, f"padding:8px;font:700 13px/1.35 {FONT};color:{INK};")
            + _td(_e(format(value, "f"), ID_LIMIT), bg,
                  f"padding:8px 10px;font:700 16px/1.3 {MONO};color:{INK};word-break:break-all;",
                  'width="34%" align="right"')
            + "</tr>")
        if n + 1 < len(groups):
            spacer = c if lo <= n and n + 1 <= hi else RULE_GREY
            trs.append("<tr>" + _td("&nbsp;", spacer, f"width:6px;height:12px;{_ZERO}mso-line-height-rule:exactly;",
                                    'width="6" height="12"')
                       + _td("&nbsp;", WHITE, f"height:12px;{_ZERO}mso-line-height-rule:exactly;",
                             'colspan="3" height="12"') + "</tr>")
    return _table("".join(trs), css_class="xv-stop-ladder", style=f"border:1px solid {LINE};border-collapse:collapse;")


# ---------------------------------------------------------------------------------------------------- V5
class StepItem(Protocol):
    @property
    def outcome(self) -> str: ...
    @property
    def event(self) -> str: ...
    @property
    def late(self) -> bool: ...
    @property
    def revision(self) -> int: ...
    @property
    def strategy_kind(self) -> str: ...


_FINAL_EVENTS: Final = frozenset({"POST", "LATE", "CORRECTED", "REOPEN"})


def advice_steps(item: StepItem, visuals: bool = True) -> str:
    """V5: Heads-up -> Final advice (-> Released for break-even and COMPLETE), the item's step current. Earlier steps
    are never marked done (the item does not record whether a heads-up went out)."""
    if not visuals:
        return ""
    if item.outcome == "ADJUST" and item.event == "PRE":
        cur, token = 0, "PROVISIONAL"
    elif item.outcome == "ADJUST" and item.event in _FINAL_EVENTS:
        cur, token = 1, "FINAL"
    elif item.outcome == "COMPLETE":
        cur, token = 2, "CLEARED"
    else:
        return ""
    final_sub = "after bar end"
    if item.event == "LATE" or item.late:
        final_sub += " · late"
    if item.event == "CORRECTED":
        final_sub += f" · revision {item.revision}"
    steps = [("Heads-up", "before bar end"), ("Final advice", final_sub)]
    if item.strategy_kind == BREAK_EVEN_KIND or item.outcome == "COMPLETE":
        steps.append(("Released", "ARM stops managing"))
    if cur >= len(steps):
        return ""
    widths, gap = (("31%", "32%", "31%"), "3%") if len(steps) == 3 else (("48%", "48%"), "4%")
    c, t = _color(token), tint(token)
    tds: list[str] = []
    for n, ((name, sub), w) in enumerate(zip(steps, widths, strict=True)):
        if n:
            tds.append(_td(NEXT, WHITE, f"font:700 11px/1 {FONT};color:{MUTED};",
                           f'width="{gap}" align="center" valign="middle" aria-hidden="true"'))
        here = n == cur
        mark = f'<span style="color:{c};">{DOT}</span> ' if here else ""
        now = f'<br><span style="font:700 11px/1.3 {FONT};">now</span>' if here else ""
        tds.append(_td(f'{mark}{n + 1} · {_e(name, ID_LIMIT)}<br><span style="font:400 11px/1.3 {FONT};">'
                       f"{_e(sub, ID_LIMIT)}</span>{now}", t if here else SOFT_BG,
                       f"padding:6px;border-top:4px solid {c if here else RULE_GREY};"
                       f"font:{700 if here else 600} 12px/1.3 {FONT};color:{INK if here else MUTED};",
                       f'width="{w}" valign="top"'))
    return _table("<tr>" + "".join(tds) + "</tr>", css_class="xv-advice-steps")


# ---------------------------------------------------------------------------------------------------- V6
_TILE_WIDTHS: Final[dict[int, tuple[str, ...]]] = {
    1: ("100%",), 2: ("48%", "48%"), 3: ("33%", "33%", "34%"), 4: ("25%", "25%", "25%", "25%")}


def count_tiles(tiles: Sequence[Tile]) -> str:
    """V6: big number over its label, at most 4 per row; an accent (a count needing attention) tints the tile."""
    if not tiles:
        return ""
    out: list[str] = []
    for start in range(0, len(tiles), 4):
        chunk = tiles[start:start + 4]
        tds: list[str] = []
        for tile, w in zip(chunk, _TILE_WIDTHS[len(chunk)], strict=True):
            accent = tile.accent if tile.accent in TOKENS else None
            bg = tint(accent) if accent else SOFT_BG
            top = _color(accent) if accent else RULE_GREY
            mark = f'<span style="color:{_color(accent)};">{SQUARE}</span> ' if accent else ""
            tds.append(_td(f'<span style="font:700 26px/30px {FONT};color:{INK};">{_e(tile.number, ID_LIMIT)}</span>'
                           f"<br>{mark}{_e(tile.label, ID_LIMIT)}", bg,
                           f"padding:10px 4px;border:1px solid {LINE};border-top:4px solid {top};"
                           f"font:600 12px/16px {FONT};color:{MUTED};", f'width="{w}" align="center" valign="top"'))
        out.append(_table("<tr>" + "".join(tds) + "</tr>", css_class="xv-count-tiles", spacing=4))
    return "".join(out)


# ---------------------------------------------------------------------------------------------------- V7
def time_strip(points: Sequence[tuple[str, str]], gap: str, token: str) -> str:
    """V7: two times in order, top to bottom, joined by a fixed-height connector (`gap` is the body's own duration
    text, or nothing). Not to scale."""
    if len(points) != 2 or any(not lbl or not tm or tm == "not recorded" for lbl, tm in points):
        return ""
    c = _color(token)

    def point(label: str, tm: str) -> str:
        return ("<tr>" + _td("&nbsp;", c, f"width:4px;{_ZERO}", 'width="4"')
                + _td(f'{_e(label, ID_LIMIT)}<br><span style="font:700 13px/1.4 {MONO};color:{INK};'
                      f'word-break:break-word;overflow-wrap:anywhere;">{_e(tm, ID_LIMIT)}</span>', SOFT_BG,
                      f"padding:6px 10px;font:12px/1.4 {FONT};color:{MUTED};") + "</tr>")

    link = f"{DOWN_ARROW} {_e(gap, ID_LIMIT)}" if gap else DOWN_ARROW
    rows = (point(*points[0])
            + "<tr>" + _td("&nbsp;", RULE_GREY, f"width:4px;{_ZERO}", 'width="4"')
            + _td(link, WHITE, f"padding:4px 10px;font:600 13px/1.3 {FONT};color:{INK};") + "</tr>"
            + point(*points[1])
            + "<tr>" + _td("Times in order · not to scale", WHITE, f"padding:4px 0 0;font:11px/1.3 {FONT};"
                           f"color:{MUTED};", 'colspan="2"') + "</tr>")
    return _table(rows, css_class="xv-time-strip")


# ---------------------------------------------------------------------------------------------------- V8
def side_chip(code: str | None) -> str:
    """V8: a neutral side pill (same style for both sides, no arrows, no green / red); `""` for any other code."""
    if code not in SIDE_CHIPS:
        return ""
    return (f'<span class="xv-side" style="display:inline-block;padding:1px 6px;border:1px solid {DASH_GREY};'
            f'background-color:{NEUTRAL_BG};color:{PILL_INK};font:700 11px/1.4 {FONT};letter-spacing:.04em;'
            'word-break:normal;overflow-wrap:normal;">'
            f'{SIDE_CHIPS[code]}</span>')


# ---------------------------------------------------------------------------------------------------- V9
def command_chip(cmd: str) -> str:
    return (f'<span style="font:13px/1.4 {MONO};background-color:{WHITE};border:1px solid {RULE_GREY};'
            f'padding:1px 4px;color:{INK};word-break:break-word;overflow-wrap:anywhere;">{_e(cmd, VALUE_LIMIT)}</span>')


def step_list(steps: Sequence[Step], token: str, critical: bool = False) -> str:
    """V9: "What to do" as numbered steps (only where the action is already assembled from parts)."""
    if not steps:
        return ""
    c = _color(token)
    bg = "#FEF3F2" if critical else "#FFFAEB"
    edge = f"border:2px solid {c};border-left:6px solid {c};" if critical else f"border-left:4px solid {c};"
    rows = ["<tr>" + _td("What to do", bg, f"padding:0 0 6px;font:700 14px/1.4 {FONT};color:{INK};", 'colspan="2"')
            + "</tr>"]
    for n, step in enumerate(steps, 1):
        num = ('<table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>'
               + _td(str(n), WHITE, f"width:18px;height:18px;border:1px solid {c};color:{c};"
                     f"font:700 12px/18px {FONT};mso-line-height-rule:exactly;",
                     'width="20" height="20" align="center" valign="middle"') + "</tr></table>")
        text = _e(step.text, LINE_LIMIT) + (" " + command_chip(step.command) if step.command else "")
        rows.append("<tr>" + _td(num, bg, "width:30px;padding:0 8px 6px 0;", 'width="30" valign="top"')
                    + _td(text, bg, f"padding:0 0 6px;font:14px/1.45 {FONT};color:{INK};", 'valign="top"')
                    + "</tr>")
    return _table("<tr>" + _td(_table("".join(rows)), bg, f"{edge}padding:10px 12px;") + "</tr>",
                  css_class="xv-steps")


# ---------------------------------------------------------------------------------------------------- V10
def summary_chips(entries: Sequence[tuple[str, str, int]], order: Sequence[str] = SUMMARY_ORDER) -> str:
    """V10: one chip per distinct card label with its count, in `order` of the label's token. A page row."""
    counts: dict[str, int] = {}
    tokens: dict[str, str] = {}
    for label, token, n in entries:
        if n <= 0:
            continue
        counts[label] = counts.get(label, 0) + n
        tokens.setdefault(label, token)
    if not counts:
        return ""
    rank = {tok: i for i, tok in enumerate(order)}
    chips = []
    for label in sorted(counts, key=lambda lb: rank.get(tokens[lb], len(order))):
        c, t = _color(tokens[label]), tint(tokens[label])
        chips.append(f'<span style="display:inline-block;margin:0 6px 4px 0;padding:3px 4px;border:1px solid {c};'
                     f'background-color:{t};color:{INK};font:700 12px/1.3 {FONT};">&nbsp;<span style="color:{c};">'
                     f"{SQUARE}</span> {counts[label]} {_e(label, ID_LIMIT)}&nbsp;</span>")
    return (f'<tr><td class="xv-summary" bgcolor="{WHITE}" style="padding:0 20px 12px;background-color:{WHITE};'
            f'font:12px/28px {FONT};mso-line-height-rule:exactly;">' + " ".join(chips) + "</td></tr>\n")


__all__ = [
    "BREAK_EVEN_KIND", "GLYPHS", "SIDE_CHIPS", "TINTS", "VISUAL_BUDGET", "advice_steps", "command_chip",
    "count_tiles", "glyph_for", "glyph_tile", "halt_ladder", "hero_row", "level_ladder", "notice_glyph",
    "rule_ladder", "safe_part", "side_chip", "step_list", "stop_ladder", "summary_chips", "time_strip", "tint",
]
