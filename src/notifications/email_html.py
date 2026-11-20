"""The HTML part of every alert email: one layout per notification context (Oversee, halts, operational pages,
critical pings, ARM advice and ARM status). Design: `docs/design/notifications/` (preview: `preview.html`).

Pure functions, no I/O:

* `classify(msg) -> Context`: the context of an Oversee-path `AlertMessage`. Episode and re-page rows are read from
  `msg.episode` (rule, subject, level), never from text; every other context is matched on its title pattern
  (`TITLE_RULES`). Body parsing is best effort: a body that does not parse is shown escaped as the detail block, so an
  email is never refused.
* `render_alert_html(msg, footer) -> str`: the page for an Oversee-path alert (layouts L1 to L4).
* The ARM notice pages (layouts L5 and L6) are `src.notifications.email_html_arm.render_arm_html`, built from the
  structured `NoticeBody` by the ARM relay at send time on this module's template. They live apart so the email
  transports (also used by the dead-man receiver) never import `src.riskmgr`.

**Content** (operations.md §9 "Notification content", design §B8): the HTML shows only what the text part already
carries (ids, codes, counts, order-state levels and times), plus the order-state current stop of the SL block. It
never shows a tick, bar, quote, spread, indicator value, favour R, favour / risk, account size, balance or any
currency amount. The ARM favour figures are never computed here.

**Mail-client safety:** tables only, every style inline, no `<style>`, no images, fonts, scripts or links; dark text
on explicitly light cells (a forced dark-mode inversion keeps the contrast); the light card is intentional, so the
page declares `color-scheme: light` only (it has no dark styles); a text label on every colour. Every
dynamic value is cut to its length limit and then `html.escape`d (`_e`).

**Visual elements** (hero glyph tile, level ladders, count tiles, time strip, numbered steps, side chips) are built by
`src.notifications.email_visuals` from presentation-only `Context` fields filled here; each one is fenced (`safe_part`)
and left out when its input is missing, and a field row it replaces is dropped only after it rendered.
"""
from __future__ import annotations

import html
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final
from zoneinfo import ZoneInfo

from src.oversee.notify import AlertMessage

FooterSource = Callable[[], tuple[str, str | None]]
"""(node id, epoch label or None). None epoch = a page sent with no leader term (single-PC direct pages)."""

MELBOURNE: Final = ZoneInfo("Australia/Melbourne")
ID_LIMIT: Final = 64          # ids and codes (as `TEXT_LIMIT` of the ARM text)
VALUE_LIMIT: Final = 300      # a field value
LINE_LIMIT: Final = 600       # a headline or a "what to do" line
DETAIL_LIMIT: Final = 2000    # the detail block

FONT: Final = "Arial,Helvetica,sans-serif"
MONO: Final = "Consolas,Menlo,monospace"
INK: Final = "#101828"
MUTED: Final = "#475467"
PAGE_BG: Final = "#F2F4F7"
LINE: Final = "#EAECF0"
READ_ONLY_LINE: Final = "XTNL Phase 1, read-only: this server sends no orders."

#: Severity tokens: (colour, default label). Every colour passes WCAG AA (>= 4.5:1) with white text.
TOKENS: Final[dict[str, tuple[str, str]]] = {
    "CRITICAL": ("#B42318", "CRITICAL"),
    "HIGH": ("#C4320A", "HIGH"),
    "FLAG": ("#B54708", "FLAG"),
    "INFO": ("#175CD3", "INFO"),
    "CLEARED": ("#067647", "CLEARED"),
    "PROVISIONAL": ("#475467", "PROVISIONAL"),
    "FINAL": ("#3538CD", "FINAL ADVICE"),
    "HOLD": ("#475467", "HOLD"),
    "FAULT": ("#B42318", "FAULT"),
    "LATE": ("#B54708", "LATE"),
}
_SEVERITY_TOKEN: Final = {"info": "INFO", "high": "HIGH", "critical": "CRITICAL"}

LAYOUTS: Final[dict[str, str]] = {
    "L1": "Risk alert", "L2": "Halt change", "L3": "Operational / info", "L4": "Critical ping",
    "L5": "ARM advice", "L6": "ARM status",
}


# ---------------------------------------------------------------------------------------------------- values
@dataclass(frozen=True)
class Field:
    """One key / value row. `mono` for ids and codes; `big` for the proposed stop; `chip` a raw side code (BUY /
    SELL) shown as a side chip after the value."""

    key: str
    value: str
    mono: bool = False
    big: bool = False
    chip: str | None = None


@dataclass(frozen=True)
class Tile:
    """One count tile: the count as text, its label; `accent` a token when the count needs attention."""

    number: str
    label: str
    accent: str | None = None


@dataclass(frozen=True)
class Step:
    """One numbered "what to do" step; `command` shown as a monospace chip after the text."""

    text: str
    command: str | None = None


@dataclass(frozen=True)
class Context:
    """What one alert email shows. Every text is raw here and escaped once when the page is built."""

    name: str
    layout: str
    token: str
    badge: str
    tag: str
    headline: str
    action: str | None = None
    fields: tuple[Field, ...] = ()
    detail: str | None = None
    tags: tuple[tuple[str, str], ...] = ()          # extra badges: (label, token)
    table_head: tuple[str, ...] = ()
    table: tuple[tuple[str, ...], ...] = ()
    table_note: str | None = None
    # presentation only (visual elements, `email_visuals`): never new data, all read from the same message
    rule_level: int | None = None                  # 0..3, the rule-level ladder
    halt_level: int | None = None                  # 0..2, the halt-level ladder
    level_note: str = ""                           # raised | still | resolved | lowered | capped | cap lifted | cleared
    cap_soft: bool = False
    tiles: tuple[Tile, ...] = ()
    window: tuple[tuple[str, str], ...] = ()       # ordered (label, time text) points, exactly 2
    window_gap: str = ""                           # the body's own duration text; "" = arrow only
    replaces: frozenset[str] = frozenset()         # field keys dropped only when the tiles / strip rendered
    steps: tuple[Step, ...] = ()
    side_col: int | None = None                    # table column shown as side chips


def _clean(text: str) -> str:
    """Control and non-printing characters (but newlines) shown as escapes, so data can never shape the page."""
    out: list[str] = []
    for ch in text:
        if ch == "\n" or ch.isprintable():
            out.append(ch)
        else:
            cp = ord(ch)
            out.append(f"\\x{cp:02x}" if cp < 0x100 else f"\\u{cp:04x}" if cp < 0x10000 else f"\\U{cp:08x}")
    return "".join(out)


def _cut(text: object, limit: int) -> str:
    s = str(text)
    return s if len(s) <= limit else s[:max(0, limit - 3)] + "..."


def _e(text: object, limit: int = VALUE_LIMIT) -> str:
    """Cut to `limit`, then escaped (`html.escape`, quotes too)."""
    return html.escape(_clean(_cut(text, limit)), quote=True)


def _id(text: object) -> str:
    """An id or code, cut to `ID_LIMIT` (raw; escaped when the page is built)."""
    return _cut(text, ID_LIMIT)


def stamp(ms: int) -> str:
    """`YYYY-MM-DD HH:MM:SS UTC / HH:MM:SS Melbourne (AEDT|AEST)`."""
    utc = datetime.fromtimestamp(ms / 1000, tz=UTC)
    mel = utc.astimezone(MELBOURNE)
    return f"{utc:%Y-%m-%d %H:%M:%S} UTC / {mel:%H:%M:%S} Melbourne ({mel.tzname()})"


def clock(ms: int) -> str:
    """`HH:MM:SS UTC / HH:MM:SS Melbourne` (the ARM text's own clock form)."""
    utc = datetime.fromtimestamp(ms / 1000, tz=UTC)
    return f"{utc:%H:%M:%S} UTC / {utc.astimezone(MELBOURNE):%H:%M:%S} Melbourne"


def _safe_stamp(ms: int) -> str:
    try:
        return stamp(ms)
    except (OverflowError, OSError, ValueError):
        return "not recorded"


# ---------------------------------------------------------------------------------------------------- page parts
def _color(token: str) -> str:
    return TOKENS.get(token, TOKENS["INFO"])[0]


def badge(label: str, token: str) -> str:
    c = _color(token)
    return (f'<span style="display:inline-block;padding:3px 8px;margin:0 6px 4px 0;border-radius:3px;'
            f'background-color:{c};color:#FFFFFF;font:700 12px/1.2 {FONT};letter-spacing:.04em;">'
            f'{_e(label, ID_LIMIT)}</span>')


def _ctx_tag(text: str) -> str:
    return (f'<span style="display:inline-block;padding:2px 7px;margin:0 6px 4px 0;border-radius:3px;'
            f'background-color:#FFFFFF;color:#344054;border:1px solid #D0D5DD;font:600 12px/1.2 {FONT};'
            f'letter-spacing:.04em;">{_e(text, ID_LIMIT)}</span>')


def _row(inner: str, padding: str = "0 20px 14px") -> str:
    return f'<tr><td style="padding:{padding};">{inner}</td></tr>\n'


def _badges_row(badges: Sequence[tuple[str, str]], tag: str) -> str:
    inner = "".join(badge(lbl, tok) for lbl, tok in badges) + (_ctx_tag(tag) if tag else "")
    return _row(inner, "16px 20px 4px")


def _headline_row(text: str, size: int = 20) -> str:
    return _row(f'<div style="font:600 {size}px/1.35 {FONT};color:{INK};">{_e(text, LINE_LIMIT)}</div>',
                "4px 20px 12px")


def _box(text_html: str, *, border: str, bg: str = "#FFFAEB", full_border: bool = False) -> str:
    edge = f"border:2px solid {border};border-left:6px solid {border};" if full_border \
        else f"border-left:4px solid {border};"
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td bgcolor="{bg}" style="background-color:{bg};{edge}padding:10px 12px;font:14px/1.45 {FONT};'
            f'color:{INK};">{text_html}</td></tr></table>')


def _action_row(text: str, token: str, *, critical: bool = False) -> str:
    bg = "#FEF3F2" if critical else "#FFFAEB"
    return _row(_box(f"<strong>What to do:</strong> {_e(text, LINE_LIMIT)}", border=_color(token), bg=bg,
                     full_border=critical))


def _banner_row(text: str, token: str) -> str:
    return _row(_box(f"<strong>{_e(text, LINE_LIMIT)}</strong>", border=_color(token), bg="#F2F4F7"))


_SIDE_FALLBACK: Final = {"BUY": "long", "SELL": "short"}


def _field_value(f: Field) -> str:
    value = _e(f.value)
    if f.chip:
        chip = _v.safe_part(_v.side_chip, f.chip)
        value += f" {chip}" if chip else _e(f" · {_SIDE_FALLBACK.get(f.chip, f.chip)}", ID_LIMIT)
    return value


def _fields_table(fields: Sequence[Field]) -> str:
    if not fields:
        return ""
    trs: list[str] = []
    for n, f in enumerate(fields):
        bg = "#F9FAFB" if n % 2 == 0 else "#FFFFFF"
        if f.big:
            vstyle = f"font:700 22px/1.3 {MONO};"
        elif f.mono:
            vstyle = f"font:14px/1.4 {MONO};"
        else:
            vstyle = f"font:14px/1.4 {FONT};"
        trs.append(
            f'<tr><td width="36%" valign="top" bgcolor="{bg}" style="padding:7px 10px;background-color:{bg};'
            f'color:{MUTED};font:13px/1.4 {FONT};">{_e(f.key, ID_LIMIT)}</td>'
            f'<td valign="top" bgcolor="{bg}" style="padding:7px 10px;background-color:{bg};color:{INK};{vstyle}'
            f'word-break:break-word;">{_field_value(f)}</td></tr>')
    return ('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="border:1px solid {LINE};border-collapse:collapse;">' + "".join(trs) + "</table>")


def _fields_row(fields: Sequence[Field]) -> str:
    return _row(_fields_table(fields), "4px 20px 14px") if fields else ""


def _detail_row(detail: str | None, caption: str = "Detail") -> str:
    if not detail:
        return ""
    return _row(f'<div style="font:600 12px/1.4 {FONT};color:{MUTED};padding:0 0 4px;">{_e(caption, ID_LIMIT)}</div>'
                f'<div style="white-space:pre-wrap;word-break:break-word;font:12px/1.5 {MONO};color:#344054;'
                f'background-color:#F9FAFB;border:1px solid {LINE};padding:10px;">{_e(detail, DETAIL_LIMIT)}</div>')


def _table_cell(value: str, chip: bool, side_words: Mapping[str, str] | None = None) -> str:
    if chip:
        html_chip = _v.safe_part(_v.side_chip, value)
        if html_chip:
            return html_chip
        if side_words is not None:
            return _e(side_words.get(value, value), ID_LIMIT)
    return _e(value, ID_LIMIT)


def _table_row(head: Sequence[str], rows: Sequence[Sequence[str]], note: str | None,
               side_col: int | None = None, side_words: Mapping[str, str] | None = None) -> str:
    """A small table; `side_col` is rendered as a side chip (V8). When the chip cannot be built the cell shows the raw
    code, or `side_words[code]` for a table that showed a side word before the chips (the ARM pending table)."""
    if not rows:
        return ""
    th = "".join(f'<td bgcolor="#F2F4F7" style="padding:6px 8px;background-color:#F2F4F7;color:{MUTED};'
                 f'font:600 12px/1.3 {FONT};">{_e(h, ID_LIMIT)}</td>' for h in head)
    body = []
    for n, r in enumerate(rows):
        bg = "#FFFFFF" if n % 2 == 0 else "#F9FAFB"
        body.append("<tr>" + "".join(
            f'<td bgcolor="{bg}" style="padding:6px 8px;background-color:{bg};color:{INK};font:13px/1.35 {MONO};'
            f'word-break:break-word;">{_table_cell(c, i == side_col, side_words)}</td>' for i, c in enumerate(r))
            + "</tr>")
    extra = f'<div style="padding:6px 0 0;font:12px/1.4 {FONT};color:{MUTED};">{_e(note, LINE_LIMIT)}</div>' \
        if note else ""
    return _row('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
                f'style="border:1px solid {LINE};border-collapse:collapse;"><tr>{th}</tr>' + "".join(body)
                + f"</table>{extra}")


def _footer_rows(footer: FooterSource | None, created_ms: int, alert_ref: str | None) -> str:
    node, epoch = "not recorded", None
    if footer is not None:
        try:
            node, epoch = footer()
        except Exception:   # a footer that cannot be read never costs the email
            node, epoch = "not recorded", None
    parts = [f"node {_id(node)}", f"epoch {_id(epoch) if epoch else 'no leader term'}"]
    if alert_ref:
        parts.append(alert_ref)
    raised = f"raised {_safe_stamp(created_ms)}" if created_ms > 0 else "raised: time not recorded"
    lines = [" · ".join(parts), raised, READ_ONLY_LINE]
    inner = "<br>".join(_e(x, LINE_LIMIT) for x in lines)
    return (f'<tr><td bgcolor="#F9FAFB" style="padding:12px 20px;background-color:#F9FAFB;border-top:1px solid {LINE};'
            f'border-radius:0 0 6px 6px;font:12px/1.6 {FONT};color:{MUTED};">{inner}</td></tr>\n')


MSO_OPEN: Final = ('<!--[if mso]><table role="presentation" width="600" align="center" cellpadding="0" '
                   'cellspacing="0" border="0"><tr><td><![endif]-->')
MSO_CLOSE: Final = "<!--[if mso]></td></tr></table><![endif]-->"
"""Outlook-only (conditional comments) fixed 600 px column: the Word engine ignores `max-width`, so without it the
card and its percentage-wide visuals would stretch across the reading pane. Every other client skips the comment."""


def page(*, title: str, preheader: str, token: str, rows: Sequence[str], footer_html: str,
         strip_px: int = 6) -> str:
    """The shared skeleton (§2): a 600 px card on a light page, severity strip, the rows, the footer."""
    c = _color(token)
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="color-scheme" content="light"><meta name="supported-color-schemes" content="light">'
        f"<title>{_e(title, LINE_LIMIT)}</title></head>\n"
        f'<body style="margin:0;padding:0;background-color:{PAGE_BG};">\n'
        f'<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;">{_e(preheader, LINE_LIMIT)}</div>\n'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="{PAGE_BG}" '
        f'style="background-color:{PAGE_BG};"><tr><td align="center" style="padding:16px 8px;">\n'
        f"{MSO_OPEN}\n"
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#FFFFFF" '
        'style="max-width:600px;width:100%;background-color:#FFFFFF;border:1px solid #D0D5DD;border-radius:6px;">\n'
        f'<tr><td height="{strip_px}" bgcolor="{c}" style="background-color:{c};height:{strip_px}px;font-size:0;'
        'line-height:0;border-radius:6px 6px 0 0;">&nbsp;</td></tr>\n'
        + "".join(rows) + footer_html
        + f"</table>\n{MSO_CLOSE}\n</td></tr></table>\n</body></html>\n")


# ---------------------------------------------------------------------------------------------------- classify
_SUBJECT_RE: Final = re.compile(r"^\[XTNL (?:INFO|HIGH|CRITICAL)\] (.*)$", re.S)

RULE_NAMES: Final[dict[str, str]] = {
    "max_position_risk": "Max position risk",
    "max_aggregate_open_risk": "Aggregate open risk",
    "missing_stop_loss": "Missing stop loss",
    "max_daily_loss": "Daily loss",
    "margin_usage": "Margin usage",
    "stale_account_image": "Stale account image",
    "stale_mark": "Stale prices",
}
UNKNOWN_REASONS: Final = frozenset({"risk_unknown", "mark_unknown", "live_unknown", "evaluation_failed",
                                    "stop_kind_inconsistent"})
UNKNOWN_ACTION: Final = ("An input is unknown; capped at SOFT until it is known. Check instruments, prices and the "
                         "account feed.")
REPAGE_ACK: Final = "Acknowledge it to silence re-pages if it is expected."
_LEVEL_NAMES: Final = {0: "PASS", 1: "FLAG", 2: "SOFT_HALT", 3: "HARD_HALT"}
SOD_PARTIAL: Final = "sod_partial_session"


def title_of(msg: AlertMessage) -> str:
    m = _SUBJECT_RE.match(msg.subject)
    return m.group(1) if m else msg.subject


def raw_body(msg: AlertMessage) -> str:
    """The row's own body, without the trailer `render_message` adds (kind, severity, created_ms, ...)."""
    i = msg.body.rfind("\n\nkind: ")
    return msg.body[:i] if i >= 0 else msg.body


def _is_soft_cap(cap: str) -> bool:
    return cap.strip().upper() in ("SOFT", "SOFT_HALT")


def _level_token(level: int) -> str:
    return "CRITICAL" if level >= 3 else "HIGH" if level >= 2 else "FLAG"


def _cap_text(cap: str) -> str:
    return "none" if cap.strip().upper() == "NONE" else cap.strip()


def rule_action(rule_id: str, level: int, reason: str, subject_type: str) -> str | None:
    """The "what to do" line of a rule state (§4 context A)."""
    if any(code in UNKNOWN_REASONS for code in re.split(r"[\s,;:()]+", reason)):
        return UNKNOWN_ACTION
    soft = level >= 2
    if rule_id == "max_position_risk":
        return ("Risk-increasing actions are blocked. Reduce size or tighten the stop at the broker, or acknowledge "
                "if expected.") if soft else "Review this trade's size or stop; no halt."
    if rule_id == "max_aggregate_open_risk":
        return ("Risk-increasing actions are blocked. Reduce the account's open risk or tighten stops at the broker, "
                "or acknowledge if expected.") if soft else "Review the account's open risk; no halt."
    if rule_id == "missing_stop_loss":
        what = "order" if subject_type == "order" else "position" if subject_type == "position" else "entity"
        return f"Attach a stop loss to this {what} at the broker."
    if rule_id == "max_daily_loss":
        if level >= 3:
            return ("Trading halted for the day. Only an operator clear with the reviewed halt event releases the "
                    "latch.")
        return ("Daily loss is past its limit on inputs that are not yet confirmed; risk-increasing actions are "
                "blocked. Review the account.") if soft else "Review the day's loss; no halt."
    if rule_id == "margin_usage":
        return "Margin usage is above its band; reduce exposure."
    if rule_id == "stale_account_image":
        return "The broker stream for this account is fenced; results are capped at SOFT. Check the adapter."
    if rule_id == "stale_mark":
        return "Prices are stale; expected while the market is closed."
    return None


def _rule_label(rule_id: str) -> str:
    return RULE_NAMES.get(rule_id, rule_id)


_EPISODE_BODY: Final = re.compile(r"^(?P<reason>.*) \(cap (?P<cap>[^,()]+), sweep (?P<sweep>[^()]+)\)$", re.S)
_REPAGE_BODY: Final = re.compile(r"^(?P<reason>.*) \(cap (?P<cap>[^()]+)\)$", re.S)
_TRANSITION_TITLE: Final = re.compile(r"^(?P<t>lowered|resolved|capped|uncapped) (?P<rule>\S+) (?P<stype>[^\s:]+):"
                                      r"(?P<sid>\S+)$")
_TRANSITION_BODY: Final = re.compile(r"^(?P<reason>.*) \(level (?P<level>[A-Z_]+), cap (?P<cap>[^()]+)\)$", re.S)


def _raised(msg: AlertMessage) -> Field:
    return Field("Raised", _safe_stamp(msg.created_ms) if msg.created_ms > 0 else "not recorded")


def _rule_context(msg: AlertMessage, body: str) -> Context | None:
    ep = msg.episode
    if ep is None or msg.kind not in ("episode", "repage"):
        return None
    rule_id, stype, sid, _act, lvl = ep
    if rule_id == SOD_PARTIAL:
        account, _, day = sid.rpartition(":")
        account = account or (msg.account_id or sid)
        return Context(
            name="oversee.sod_partial_session", layout="L1", token="HIGH", badge="HIGH", tag="OVERSEE · DAY ROLL",
            headline=f"Start-of-day marks partial: account {_id(account)}, trading day {_id(day)}",
            action="The daily-loss rule is capped at SOFT for this day. No action unless the gap is unexpected.",
            fields=(Field("Account", _id(account), mono=True), Field("Trading day", _id(day), mono=True),
                    Field("Reason codes", body.strip() or "partial_session", mono=True), _raised(msg)))
    level = _LEVEL_NAMES.get(lvl, str(lvl))
    repage = msg.kind == "repage"
    m = (_REPAGE_BODY if repage else _EPISODE_BODY).match(body.strip())
    reason = m.group("reason") if m else body.strip()
    name = _rule_label(rule_id)
    action = rule_action(rule_id, lvl, reason, stype)
    steps = (Step(action), Step(REPAGE_ACK)) if repage and action is not None else ()
    if repage:
        action = f"{action} {REPAGE_ACK}" if action else REPAGE_ACK
    fields = [Field("Rule", f"{name} ({_id(rule_id)})"), Field("Level", level, mono=True),
              Field("Subject", f"{_id(stype)}:{_id(sid)}", mono=True)]
    if msg.account_id:
        fields.append(Field("Account", _id(msg.account_id), mono=True))
    fields.append(Field("Reason code", _id(reason), mono=True))
    if m:
        fields.append(Field("Cap", _cap_text(m.group("cap"))))
        if not repage:
            fields.append(Field("Sweep id", _id(m.group("sweep")), mono=True))
    fields.append(Field("Alert type", "re-page (still active)" if repage else "new episode (activated or raised)"))
    fields.append(_raised(msg))
    tok = _level_token(lvl)
    return Context(
        name="oversee.repage" if repage else "oversee.episode", layout="L1", token=tok, badge=level,
        tag="OVERSEE · RULE",
        headline=(f"Still {level}: {name} {_id(sid)}" if repage else f"{name} {level}: {_id(stype)} {_id(sid)}"),
        action=action, fields=tuple(fields), detail=None if m else body,
        tags=(("RE-PAGE", tok),) if repage else (),
        rule_level=lvl if isinstance(lvl, int) and 0 <= lvl <= 3 else None, level_note="still" if repage else "raised",
        cap_soft=m is not None and _is_soft_cap(m.group("cap")), steps=steps)


_LEVEL_CODES: Final = {name: code for code, name in _LEVEL_NAMES.items()}
_TRANSITION_NOTE: Final = {"resolved": "resolved", "lowered": "lowered", "capped": "capped", "uncapped": "cap lifted"}
_TRANSITION_STYLE: Final = {
    "resolved": ("CLEARED", "RESOLVED", "No action."),
    "lowered": ("INFO", "LOWERED", "No action."),
    "capped": ("FLAG", "CAPPED", "Inputs went stale; capped at SOFT until they are current."),
    "uncapped": ("INFO", "UNCAPPED", "Inputs current again."),
}


def _transition_context(msg: AlertMessage, title: str, body: str) -> Context | None:
    t = _TRANSITION_TITLE.match(title)
    if t is None:
        return None
    token, label, action = _TRANSITION_STYLE[t.group("t")]
    m = _TRANSITION_BODY.match(body.strip())
    name = _rule_label(t.group("rule"))
    fields = [Field("Rule", f"{name} ({_id(t.group('rule'))})"),
              Field("Subject", f"{_id(t.group('stype'))}:{_id(t.group('sid'))}", mono=True)]
    if msg.account_id:
        fields.append(Field("Account", _id(msg.account_id), mono=True))
    if m:
        fields += [Field("Level", _id(m.group("level")), mono=True),
                   Field("Reason code", _id(m.group("reason")), mono=True), Field("Cap", _cap_text(m.group("cap")))]
    fields += [Field("Alert type", f"state change ({t.group('t')})"), _raised(msg)]
    return Context(name=f"oversee.{t.group('t')}", layout="L1", token=token, badge=label, tag="OVERSEE · RULE",
                   headline=f"{name} {t.group('t')}: {_id(t.group('stype'))} {_id(t.group('sid'))}",
                   action=action, fields=tuple(fields), detail=None if m else body,
                   rule_level=_LEVEL_CODES.get(m.group("level")) if m else None,
                   level_note=_TRANSITION_NOTE[t.group("t")], cap_soft=m is not None and _is_soft_cap(m.group("cap")))


_CLEARED_BY: Final = re.compile(r"^HALT cleared by (?P<op>.+)$")
_CLEARED_BODY: Final = re.compile(r"^(?P<why>.*) -> (?P<target>[A-Z_]+)$", re.S)
_HALT_LEVELS: Final = {"NONE": 0, "SOFT": 1, "SOFT_HALT": 1, "HARD": 2, "HARD_HALT": 2}


def _halt_context(msg: AlertMessage, title: str, body: str) -> Context | None:
    tag = "OVERSEE · HALT"
    if title in ("HALT SOFT", "HALT HARD"):
        hard = title == "HALT HARD"
        rule, sep, reason = body.strip().partition(": ")
        fields = [Field("Halt level", f"→ {'HARD' if hard else 'SOFT'} (raised)", big=True)]
        if sep:
            fields += [Field("Rule", "-" if rule == "-" else _id(rule), mono=True),
                       Field("Reason", _id(reason), mono=True)]
        fields.append(_raised(msg))
        return Context(
            name="halt.hard" if hard else "halt.soft", layout="L2", token="CRITICAL" if hard else "HIGH",
            badge=title, tag=tag, headline=f"Halt level raised to {'HARD' if hard else 'SOFT'}",
            action=("The gate denies every action. After review, an operator clear with the reviewed halt event id "
                    "releases the latch.") if hard
            else "Only risk-reducing actions pass the gate. Review the active SOFT states.",
            fields=tuple(fields), detail=None if sep else body, halt_level=2 if hard else 1, level_note="raised")
    if title == "HALT cleared":
        return Context(name="halt.cleared", layout="L2", token="CLEARED", badge="CLEARED", tag=tag,
                       headline="Halt cleared after cool-down (level NONE)", action="No action.",
                       fields=(Field("Halt level", "→ NONE", big=True), Field("Reason", _id(body.strip()),
                                                                                   mono=True), _raised(msg)),
                       halt_level=0, level_note="cleared")
    c = _CLEARED_BY.match(title)
    if c is None:
        return None
    m = _CLEARED_BODY.match(body.strip())
    target = m.group("target") if m else "?"
    fields = [Field("Operator", _id(c.group("op")), mono=True)]
    if m:
        fields += [Field("Why", m.group("why")), Field("Halt level", f"→ {target}", big=True)]
    fields.append(_raised(msg))
    return Context(name="halt.cleared_by_operator", layout="L2", token="CLEARED", badge="CLEARED", tag=tag,
                   headline=f"Halt cleared by {_id(c.group('op'))} → {_id(target)}",
                   action="Confirm this clear was intended.", fields=tuple(fields), detail=None if m else body,
                   tags=(("BY OPERATOR", "HIGH"),), halt_level=_HALT_LEVELS.get(target) if m else None,
                   level_note="cleared")


_PING_TITLE: Final = re.compile(r"^ARM FAILED(?: re-ping (?P<n>\d+))? \[(?P<ping>[^\]]+)\] (?P<kind>\S+) (?P<id>.+)$")


def _ping_context(msg: AlertMessage, title: str, body: str) -> Context | None:
    t = _PING_TITLE.match(title)
    if t is None:
        return None
    fields: list[Field] = []
    rows: list[tuple[str, ...]] = []
    note = None
    since = clear_cmd = None
    in_positions = False
    labels = {"scope": "Scope", "reason": "Reason", "edge": "Edge", "counters": "Counters",
              "first counted fault": "First counted fault", "also FAILED with it": "Also FAILED with"}
    for line in body.splitlines():
        if in_positions and line.startswith("  "):
            parts = line.split()
            if parts and parts[0] == "and":
                note = line.strip()
            elif len(parts) == 4:
                rows.append(tuple(_id(p) for p in parts))
            else:
                rows.append(("", _id(line.strip()), "", ""))
            continue
        in_positions = False
        if line.startswith("positions ("):
            in_positions = True
            continue
        if line.startswith("no advice for these positions"):
            s = re.search(r"issued since (.+)$", line)
            since = s.group(1) if s else None
            continue
        if line.startswith("clear, after fixing the cause: "):
            clear_cmd = line[len("clear, after fixing the cause: "):]
            continue
        key, sep, value = line.partition(": ")
        if sep and key in labels:
            fields.append(Field(labels[key], value, mono=key in ("scope", "reason", "edge", "counters")))
    fields.insert(0, Field("Ping id", _id(t.group("ping")), mono=True))
    fields.append(_raised(msg))
    steps: tuple[Step, ...] = ()
    if since or clear_cmd:
        action = (f"Do not act on ARM advice for these positions issued since {since or 'the first fault'}. "
                  f"Fix the cause, then run {clear_cmd or 'arm failed clear'}.")
        steps = (Step(f"Do not act on ARM advice for these positions issued since {since or 'the first fault'}."),
                 Step("Fix the cause."), Step("Run", command=clear_cmd or "arm failed clear"))
    else:
        action = "Do not act on ARM advice for the positions named; fix the cause, then clear the FAILED scope."
    n = t.group("n")
    parsed = bool(rows or since or clear_cmd or len(fields) > 2)
    return Context(
        name="arm.failed_ping", layout="L4", token="CRITICAL", badge="ARM FAILED", tag="ARM · FAILED",
        headline=f"ARM FAILED: {_id(t.group('kind'))} {_id(t.group('id'))}", action=action, fields=tuple(fields),
        detail=None if parsed else body, tags=((f"RE-PING {n}", "CRITICAL"),) if n else (),
        table_head=("Account", "Position", "Symbol", "Side"), table=tuple(rows), table_note=note, steps=steps,
        side_col=3)


Visuals = tuple[tuple[Tile, ...], tuple[tuple[str, str], ...], str, frozenset[str]]
"""(count tiles, time-strip points, time-strip gap text, field keys the tiles / strip replace)."""


@dataclass(frozen=True)
class TitleRule:
    """One title pattern of an operational or critical page (§4 L3 / L4)."""

    name: str
    pattern: re.Pattern[str]
    action: str | Callable[[re.Match[str], str], str | None] | None = None
    headline: Callable[[re.Match[str], str], str] | None = None
    tag: str = "OPERATIONS"
    fields: Callable[[re.Match[str], str], list[Field]] | None = None
    visuals: Callable[[re.Match[str], str], Visuals] | None = None


def _r(p: str) -> re.Pattern[str]:
    return re.compile(p)


_OFFLINE: Final = re.compile(r"engine was offline from (\S+) to (\S+) \(([^)]*)\)")
_CATCHUP_COUNTS: Final = re.compile(r"(\d+) orders new, (\d+) updated, (\d+) positions closed while offline; "
                                    r"(\d+) positions new, (\d+) cash flows new")
_SEEN_COUNTS: Final = re.compile(r"\((\w+)\): (\d+) orders / (\d+) positions seen, (\d+) anomalies")
_UNCOVERED: Final = re.compile(r"uncovered window ([^\s;]+) \.\. ([^\s;]+)")


def _catchup_fields(m: re.Match[str], body: str) -> list[Field]:
    out: list[Field] = [Field("Adapter", _id(m.group("aid")), mono=True)]
    r = re.search(r"catch-up \((\w+)\)", body)
    if r:
        out.append(Field("Reason", _id(r.group(1)), mono=True))
    off = _OFFLINE.search(body)
    if off:
        out.append(Field("Offline", f"{off.group(1)} to {off.group(2)} ({off.group(3)})"))
    c = _CATCHUP_COUNTS.search(body)
    if c:
        out += [Field("Orders new / updated", f"{c.group(1)} / {c.group(2)}"),
                Field("Positions new / closed", f"{c.group(4)} / {c.group(3)}"),
                Field("Cash flows new", c.group(5))]
    return out


def _catchup_incomplete_fields(m: re.Match[str], body: str) -> list[Field]:
    out: list[Field] = [Field("Adapter", _id(m.group("aid")), mono=True)]
    r = _SEEN_COUNTS.search(body)
    if r:
        out += [Field("Reason", _id(r.group(1)), mono=True),
                Field("Orders / positions seen", f"{r.group(2)} / {r.group(3)}"), Field("Anomalies", r.group(4))]
    u = _UNCOVERED.search(body)
    if u:
        out.append(Field("Uncovered window", f"{u.group(1)} .. {u.group(2)}", mono=True))
    a = re.search(r"anomalies: ([^;]+)", body)
    if a:
        out.append(Field("Anomaly ids", a.group(1).strip(), mono=True))
    return out


def _catchup_visuals(m: re.Match[str], body: str) -> Visuals:
    """Count tiles (orders new / updated, positions new / closed) and the offline window of a catch-up page."""
    tiles: tuple[Tile, ...] = ()
    window: tuple[tuple[str, str], ...] = ()
    gap = ""
    replaces: set[str] = set()
    c = _CATCHUP_COUNTS.search(body)
    if c:
        tiles = (Tile(str(int(c.group(1))), "orders new"), Tile(str(int(c.group(2))), "orders updated"),
                 Tile(str(int(c.group(4))), "positions new"), Tile(str(int(c.group(3))), "positions closed"))
        replaces |= {"Orders new / updated", "Positions new / closed"}
    off = _OFFLINE.search(body)
    if off:
        window, gap = (("Offline from", off.group(1)), ("to", off.group(2))), off.group(3)
        replaces.add("Offline")
    return tiles, window, gap, frozenset(replaces)


def _catchup_incomplete_visuals(m: re.Match[str], body: str) -> Visuals:
    """Count tiles (orders / positions seen, anomalies) and the uncovered window of an incomplete catch-up page."""
    tiles: tuple[Tile, ...] = ()
    window: tuple[tuple[str, str], ...] = ()
    replaces: set[str] = set()
    r = _SEEN_COUNTS.search(body)
    if r:
        anomalies = int(r.group(4))
        tiles = (Tile(str(int(r.group(2))), "orders seen"), Tile(str(int(r.group(3))), "positions seen"),
                 Tile(str(anomalies), "anomalies", "FLAG" if anomalies > 0 else None))
        replaces |= {"Orders / positions seen", "Anomalies"}
    u = _UNCOVERED.search(body)
    if u:
        window = (("Uncovered from", u.group(1)), ("to", u.group(2)))
        replaces.add("Uncovered window")
    return tiles, window, "", frozenset(replaces)


def _account_field(m: re.Match[str], body: str) -> list[Field]:
    a = re.search(r"account=(\S+)", body)
    return [Field("Account", _id(a.group(1)), mono=True)] if a and a.group(1) not in ("None", "") else []


def _aid_field(m: re.Match[str], body: str) -> list[Field]:
    g = m.groupdict()
    out = []
    if g.get("aid"):
        out.append(Field("Adapter", _id(g["aid"]), mono=True))
    if g.get("sym"):
        out.append(Field("Symbol", _id(g["sym"]), mono=True))
    if g.get("stream"):
        out.append(Field("Stream", _id(g["stream"]), mono=True))
    if g.get("name"):
        out.append(Field("Name", _id(g["name"]), mono=True))
    return out


def _calendar_fields(m: re.Match[str], body: str) -> list[Field]:
    aid, sep, sym = body.strip().partition(":")
    return [Field("Adapter", _id(aid), mono=True), Field("Symbol", _id(sym), mono=True)] if sep and aid else []


OVERSEE_ACTIONS: Final[dict[str, str]] = {
    "slow_pass": "A RiskSweep pass took longer than its interval; check PostgreSQL and the engine load.",
    "sod_missing": "Daily-loss is UNKNOWN for this account until the start-of-day row exists.",
    "evaluation_failed": "This account's pass is failing; it is capped at SOFT. Check the logs for the account.",
    "evaluation_failed_unwritten": "The failing account's state could not be written; check PostgreSQL.",
    "quarantine_release_failed": "The account stays quarantined; check PostgreSQL, then run release_account.",
    "repage_failed": "Re-pages could not be queued; check PostgreSQL.",
    "unlisted_states_unresolved": "Rule states of an account no longer listed are still open; review them.",
    "ruleset_install_failed": "The ruleset could not be installed; the previous one stays in force. Check it.",
    "oversight_lapsed": "The account is quarantined; investigate, then run release_account.",
    "quarantine_unwritten": "The gate is denying; check PostgreSQL.",
    "halt_decision_failed": "The gate is denying; check PostgreSQL.",
    "postgres_unavailable": "The gate is denying; check PostgreSQL.",
    "quarantine_released": "No action.",
    "unlisted_states_resolved": "No action.",
    "dependent_alerts_summary": "No action.",
    "dependent_alerts_escalation": "Check the adapter reconnect for the named accounts.",
}
INTEGRITY_ACTIONS: Final[dict[str, str]] = {
    "dlq_quarantined": "Review the quarantined DLQ row.",
    "dlq_parked_15min": "An entity has been parked for 15 min; check the adapter and the DLQ worker.",
    "halt_state_ahead": "Redis halt state was ahead of PostgreSQL and was re-applied; check for a restore.",
    "digest_repaired": "The Redis digest was repaired; no action unless it repeats.",
    "redis_ahead": "A Redis key was ahead of PostgreSQL and was re-applied; check for a restore.",
    "redis_orphan": "An orphan Redis key was removed; no action unless it repeats.",
    "spill_torn_tail": "A torn spill-file tail was moved aside; review it.",
    "patroni_reinit_failed": "Re-initialise the damaged PostgreSQL member by hand.",
    "integrity_resolved": "No action.",
    "integrity_halted": "Follow the database-damage runbook.",
    "integrity_manual_intervention": "Follow the database-damage runbook.",
    "dlq_cash_flow_unverified": "Transfers for the day are unverified; refresh cash flows.",
    "spill_file_non_empty": "Check the spill file drain on the named node.",
}
ARM_OPS_ACTIONS: Final[tuple[tuple[str, str], ...]] = (
    ("ARM: paused for more than 6 h", "ARM advice is paused: run arm resume when ready."),
    ("ARM: awaiting broker bar", "Advice waits for the broker's bar; no action unless it persists."),
    ("ARM: bars unresolved", "Check the market-data bars for the named series; advice is held."),
    ("ARM: armed position not decided", "Check arm decisions and the BarClock."),
    ("ARM: assigned position not armed", "Check the position and the ARM configuration."),
    ("ARM: decision loop stalled", "The ARM decision loop is stalled; check the engine log."),
    ("ARM: arm-tracker stalled", "The arm-tracker is stalled; check the engine log."),
    ("ARM: notice relay backlog", "ARM notices are not going out; check the ARM channels."),
    ("ARM: decisions without a notice", "Check arm decisions; some advice was not noticed."),
    ("ARM: engine restarted after a PC sleep", "No action; the restart after a sleep is expected."),
    ("ARM: position", "Run arm rearm after checking the position."),
    ("ARM: configuration", "Review the ARM configuration."),
    ("ARM channel", "Check the ARM channel credentials and provider."),
    ("ARM notice #", "An ADJUST notice was never delivered: check arm decisions."),
    ("ARM notices expired undelivered", "ARM notices expired before delivery: check arm decisions and the channels."),
    ("ARM idle", "ARM advice is idle: accept a configuration when ready."),
    ("ARM paused", "ARM advice is paused: run arm resume when ready."),
    ("ARM configuration tampered", "Do not use ARM advice; verify the configuration chain."),
    ("ARM configuration", "Check the ARM configuration source."),
    ("ARM FAILED spool damaged", "Review the FAILED spool file; FAILED episodes may not have been recorded."),
)
MD_TITLES: Final = (
    "broker bars unavailable", "broker bar reads failing", "tick cross-check skipped", "indicator threads hung",
    "indicator faulted", "redis-md memory high", "redis-md refuses writes for memory", "redis-md memory shedding step",
)
MD_ACTION: Final = "Advice for the affected series is held; no action unless it persists."


def _oversee_action(m: re.Match[str], body: str) -> str | None:
    return OVERSEE_ACTIONS.get(m.group("code"))


def _oversee_headline(m: re.Match[str], body: str) -> str:
    code = m.group("code")
    if code == "dependent_alerts_escalation":
        return "Still fenced after takeover settle"
    a = re.search(r"account=(\S+)", body)
    acct = a.group(1) if a and a.group(1) not in ("None", "") else None
    return f"Oversee: {_id(code)}" + (f" (account {_id(acct)})" if acct else "")


def _integrity_action(m: re.Match[str], body: str) -> str | None:
    return INTEGRITY_ACTIONS.get(m.group("code"))


def _arm_ops_action(m: re.Match[str], body: str) -> str | None:
    title = m.string
    for prefix, action in ARM_OPS_ACTIONS:
        if title.startswith(prefix):
            return action
    return None


def _catchup_headline(m: re.Match[str], body: str) -> str:
    done = "INCOMPLETE" if body.rstrip().endswith("INCOMPLETE") else "complete"
    return f"Catch-up {done}: {_id(m.group('aid'))}"


def _maintenance_headline(m: re.Match[str], body: str) -> str:
    u = re.search(r"until (\S+)", body)
    return f"Broker maintenance: {_id(m.group('aid'))}" + (f" until {_id(u.group(1))}" if u else "")


TITLE_RULES: Final[tuple[TitleRule, ...]] = (
    TitleRule("ops.catchup_incomplete", _r(r"^downtime catch-up incomplete: (?P<aid>\S+)$"),
              lambda m, b: (f"Check the broker's order history for the uncovered window; risk-increasing actions "
                            f"stay denied for {_id(m.group('aid'))}."),
              lambda m, b: f"Catch-up incomplete: {_id(m.group('aid'))}", "OPERATIONS · CATCH-UP",
              _catchup_incomplete_fields, _catchup_incomplete_visuals),
    TitleRule("ops.catchup", _r(r"^downtime catch-up: (?P<aid>\S+)$"),
              lambda m, b: "See the catch-up incomplete page." if b.rstrip().endswith("INCOMPLETE") else "No action.",
              _catchup_headline, "OPERATIONS · CATCH-UP", _catchup_fields, _catchup_visuals),
    TitleRule("ops.clock_moved_back", _r(r"^clock moved back: (?P<aid>\S+)$"),
              "Check the PC clock and the broker's order history.", None, "OPERATIONS · CATCH-UP", _aid_field),
    TitleRule("ops.cash_flow_ledger_incomplete", _r(r"^cash-flow ledger incomplete: (?P<aid>\S+)$"),
              "P&L for the day stays unverified; no action unless it persists.", None, "OPERATIONS · CATCH-UP",
              _aid_field),
    TitleRule("ops.ingestion", _r(r"^ingestion .+: (?P<aid>[^\s:]+)(?::(?P<stream>\S+))?$"),
              "SM4 recovers through BACKOFF; check the adapter if it repeats.", None, "OPERATIONS · INGESTION",
              _aid_field),
    TitleRule("ops.consumer", _r(r"^(?:consumer commit failed|DLQ ring full): (?P<name>\S+)$"),
              "Check PostgreSQL health.", None, "OPERATIONS · INGESTION", _aid_field),
    TitleRule("ops.sessions_review", _r(r"^market sessions need review: (?P<aid>\S+)$"),
              "Review the schedule notes; run the sessions review.", None, "OPERATIONS · CALENDAR", _aid_field),
    TitleRule("ops.session_unusable", _r(r"^market session unusable: (?P<aid>[^\s:]+):(?P<sym>\S+)$"),
              "Check the schedule source.", None, "OPERATIONS · CALENDAR", _aid_field),
    TitleRule("ops.sessions_not_saved", _r(r"^market sessions not saved$"), "Check the schedule source.", None,
              "OPERATIONS · CALENDAR"),
    TitleRule("ops.maintenance_unreadable", _r(r"^maintenance windows unreadable"),
              "Check the maintenance window file.", None, "OPERATIONS · CALENDAR"),
    TitleRule("ops.maintenance", _r(r"^maintenance: (?P<aid>\S+)$"),
              "No action; FENCE alerts are suppressed in the window.", _maintenance_headline,
              "OPERATIONS · CALENDAR", _aid_field),
    TitleRule("ops.calendar", _r(r"^calendar:(?P<flag>\S+)$"),
              "Check the market calendar for the adapter and symbol named.", None, "OPERATIONS · CALENDAR",
              _calendar_fields),
    TitleRule("ops.flag", _r(r"^flag:(?P<code>\S+)$"), "Check the flag.", None),
    TitleRule("ops.adapter_rearmed", _r(r"^adapter re-armed: (?P<aid>\S+)$"), "No action.", None,
              "OPERATIONS · ADAPTER", _aid_field),
    TitleRule("ops.auth_failed", _r(r"^broker AUTH_FAILED: (?P<aid>\S+)$"),
              "Rotate or fix the credential (a new generation) or re-arm the adapter; SM4 is stopped.", None,
              "OPERATIONS · ADAPTER", _aid_field),
    TitleRule("ops.pre_open_failed", _r(r"^PRE_OPEN readiness failed: (?P<aid>\S+)$"),
              "The adapter is not ready for the open; check the listed problems (not streaming, token, chrony).",
              None, "OPERATIONS · CALENDAR", _aid_field),
    TitleRule("oversee.feed_lag", _r(r"^oversee:feed lag$"), "Check feed observer health.", None,
              "OVERSEE · OPERATIONS"),
    TitleRule("oversee.page", _r(r"^oversee:(?P<code>[a-z_]+)$"), _oversee_action, _oversee_headline,
              "OVERSEE · OPERATIONS", _account_field),
    TitleRule("integrity.page", _r(r"^integrity:(?P<code>[a-z_]+)$"), _integrity_action, None, "INTEGRITY"),
    TitleRule("ops.task_circuit", _r(r"^task (?P<name>\S+) circuit (?:still )?open$"),
              "Investigate, then run reset_task.", None, "OPERATIONS · TASKS", _aid_field),
    TitleRule("ops.task_stale", _r(r"^task (?P<name>\S+) stale$"),
              "The task was restarted; check it if this repeats.", None, "OPERATIONS · TASKS", _aid_field),
    TitleRule("ops.risksweep_both", _r(r"^RiskSweep failing on both nodes"),
              "The RiskSweep fails on both nodes; the gate denies. Investigate at once.", None,
              "OPERATIONS · TASKS"),
    TitleRule("ops.optional_services", _r(r"^(?:optional services failed to start|optional services settings|"
                                          r"market-data hook failing)"),
              "Core engine continues; check the optional service settings.", None, "OPERATIONS · OPTIONAL"),
    TitleRule("ops.lifecycle", _r(r"^lifecycle: "), "See the partition-archive runbook.", None,
              "OPERATIONS · LIFECYCLE"),
    TitleRule("ops.ruleset_rejected_init", _r(r"^ruleset v\d+ rejected at LEADER_INIT"), "Fix the active ruleset.",
              None, "OVERSEE · RULESET"),
    TitleRule("ops.ruleset_rejected", _r(r"^ruleset v\d+ rejected"), "Fix and re-add the ruleset.", None,
              "OVERSEE · RULESET"),
    TitleRule("ops.ruleset", _r(r"^ruleset v\d+"), "No action.", None, "OVERSEE · RULESET"),
    TitleRule("ops.safe_hold_stopped", _r(r"^SAFE_HOLD auto-resume stopped on the single PC"),
              "Check deploy/local/status-xtnl.ps1 and the logs, then resume by hand.", None, "SINGLE PC"),
    TitleRule("ops.safe_hold_resumed", _r(r"^SAFE_HOLD auto-resumed"), "No action; watch for repeated flaps.", None,
              "SINGLE PC"),
    TitleRule("ops.not_recording", _r(r"^engine not recording on the single PC"),
              "Broker state is not being recorded; check status-xtnl.ps1 and the newest log.", None, "SINGLE PC"),
    TitleRule("ops.recording_again", _r(r"^engine recording again on the single PC"), "No action.", None,
              "SINGLE PC"),
    TitleRule("arm.failed_not_recorded", _r(r"^ARM FAILED not recorded \[(?P<ping>[^\]]+)\]"),
              "PostgreSQL did not record the FAILED episode; check the database.", None, "ARM · FAILED"),
    TitleRule("arm.operations", _r(r"^ARM(?::| channel | notice| notices| idle| paused| configuration| FAILED spool)"),
              _arm_ops_action, None, "ARM · OPERATIONS"),
    TitleRule("md.page", _r("^(?:" + "|".join(re.escape(t) for t in MD_TITLES) + ")"), MD_ACTION, None,
              "MARKET DATA"),
)


def _ops_context(msg: AlertMessage, title: str, body: str) -> Context:
    sev = _SEVERITY_TOKEN.get(msg.severity, "INFO")
    layout = "L4" if msg.severity == "critical" else "L3"
    for rule in TITLE_RULES:
        m = rule.pattern.match(title)
        if m is None:
            continue
        action = rule.action(m, body) if callable(rule.action) else rule.action
        headline = rule.headline(m, body) if rule.headline is not None else title
        fields = [Field("Alert", _id(title), mono=True)]
        if rule.fields is not None:
            fields += rule.fields(m, body)
        if msg.account_id and not any(f.key == "Account" for f in fields):
            fields.append(Field("Account", _id(msg.account_id), mono=True))
        fields.append(_raised(msg))
        visuals: Visuals = ((), (), "", frozenset())
        if rule.visuals is not None:
            try:
                visuals = rule.visuals(m, body)
            except Exception:       # a visual never costs the page: the fields stay as they are
                visuals = ((), (), "", frozenset())
        tiles, window, gap, replaces = visuals
        return Context(name=rule.name, layout=layout, token=sev, badge=TOKENS[sev][1], tag=rule.tag,
                       headline=headline, action=action, fields=tuple(fields), detail=body or None, tiles=tiles,
                       window=window, window_gap=gap, replaces=replaces)
    fields = [Field("Alert", _id(title), mono=True)]
    if msg.account_id:
        fields.append(Field("Account", _id(msg.account_id), mono=True))
    fields.append(_raised(msg))
    return Context(name="generic", layout=layout, token=sev, badge=TOKENS[sev][1],
                   tag="OVERSEE" if msg.kind in ("episode", "repage") else "OPERATIONS", headline=title,
                   fields=tuple(fields), detail=body or None)


def classify(msg: AlertMessage) -> Context:
    """The context (and layout) of one Oversee-path alert. Never raises on unexpected text: an unmatched title is
    the generic L3 / L4 page with the escaped body."""
    title = title_of(msg)
    body = raw_body(msg)
    for ctx in (_rule_context(msg, body), _transition_context(msg, title, body) if msg.kind == "info" else None,
                _halt_context(msg, title, body), _ping_context(msg, title, body)):
        if ctx is not None:
            return ctx
    return _ops_context(msg, title, body)


def _hero_rows(badges: Sequence[tuple[str, str]], tag: str, headline: str, token: str, glyph: str) -> list[str]:
    """V1 hero row; the old badge and headline rows when it cannot be built."""
    hero = _v.safe_part(_v.hero_row, badges, tag, headline, token, glyph)
    return [hero] if hero else [_badges_row(badges, tag), _headline_row(headline)]


def _context_rows(ctx: Context) -> list[str]:
    """Hero, level ladder (L1 / L2), what to do, count tiles and time strip (L3), fields, table, detail."""
    critical = ctx.layout == "L4"
    rows = _hero_rows(((ctx.badge, ctx.token), *ctx.tags), ctx.tag, ctx.headline, ctx.token,
                      _v.safe_part(_v.glyph_for, ctx))
    if ctx.rule_level is not None:
        rows.append(_v.safe_part(_v.rule_ladder, ctx))
    elif ctx.halt_level is not None:
        rows.append(_v.safe_part(_v.halt_ladder, ctx))
    steps = _v.safe_part(_v.step_list, ctx.steps, ctx.token, critical) if ctx.steps else ""
    if steps:
        rows.append(_row(steps))
    elif ctx.action:
        rows.append(_action_row(ctx.action, ctx.token, critical=critical))
    fields = ctx.fields
    if ctx.tiles or ctx.window:
        tiles = _v.safe_part(_v.count_tiles, ctx.tiles) if ctx.tiles else ""
        strip = _v.safe_part(_v.time_strip, ctx.window, ctx.window_gap, ctx.token) if ctx.window else ""
        rows += [_row(tiles) if tiles else "", _row(strip) if strip else ""]
        if (tiles or not ctx.tiles) and (strip or not ctx.window):
            fields = tuple(f for f in fields if f.key not in ctx.replaces)
    rows.append(_fields_row(fields))
    rows.append(_table_row(ctx.table_head, ctx.table, ctx.table_note, ctx.side_col))
    rows.append(_detail_row(ctx.detail))
    return rows


def render_alert_html(msg: AlertMessage, footer: FooterSource | None = None, *, theme: str = "classic") -> str:
    """The HTML part of an Oversee-path alert: `theme="xtnl"` the XTNL-theme page (`email_theme.render_oversee`),
    otherwise the classic light layouts L1 to L4."""
    ctx = classify(msg)
    if theme == "xtnl":
        from src.notifications.email_theme import render_oversee
        return render_oversee(msg, ctx, footer)
    ref = f"alert #{msg.outbox_id}" if msg.outbox_id is not None else None
    return page(title=msg.subject, preheader=ctx.headline, token=ctx.token, rows=_context_rows(ctx),
                footer_html=_footer_rows(footer, msg.created_ms, ref), strip_px=10 if ctx.layout == "L4" else 6)


__all__ = [
    "Context", "DETAIL_LIMIT", "Field", "FooterSource", "ID_LIMIT", "LAYOUTS", "LINE_LIMIT", "Step", "TITLE_RULES",
    "TOKENS", "Tile", "TitleRule", "Visuals", "badge", "classify", "clock", "page", "raw_body", "render_alert_html",
    "rule_action", "stamp", "title_of",
]

# Imported last, as a module, and used at call time (`_v.name(...)`): `email_visuals` imports this module's names,
# so either import order works, and the tests can replace a helper.
from src.notifications import email_visuals as _v  # noqa: E402
