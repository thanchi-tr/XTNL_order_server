"""The HTML part of ARM notice emails (layouts L5 advice and L6 status; design `docs/design/notifications/`), built
by the ARM relay at send time from the structured `NoticeBody` on the shared template of
`src.notifications.email_html`.

**The SL advice block** (an `ADJUST` item): status (PROVISIONAL heads-up at PRE, FINAL "move stop" after the bar
closes, LATE / CORRECTED tags), symbol and side, position, the pinned entry (with Q13 `show_levels`, as the text),
the current stop the handles held when read (`ItemBody.stop_now`, "not recorded" when absent), the proposed new
stop, the bar it was decided on, the "move only if your stop is still worse" rule line and what to do.

**Content:** order-state levels (entry, current stop, proposed stop), ids, codes, R values and times only. Favour R
and favour / risk are never computed or shown here, whatever the text part carries (F2).
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Final

from src.notifications import email_visuals as _v
from src.notifications.email_html import (
    FONT,
    INK,
    LINE,
    LINE_LIMIT,
    MUTED,
    TOKENS,
    Field,
    FooterSource,
    Tile,
    _action_row,
    _banner_row,
    _box,
    _color,
    _e,
    _fields_row,
    _fields_table,
    _footer_rows,
    _hero_rows,
    _id,
    _row,
    _table_row,
    badge,
    clock,
    page,
)
from src.riskmgr.guards.first_bar import pre_label
from src.riskmgr.render import (
    AWAITING_TEXT,
    CAVEAT_TEXT,
    DELAYED,
    EXTRA_TEXT,
    FIRST_BAR_CONDITIONAL,
    PRE_HEADER,
    STILL_WORSE,
    ExtraBody,
    ItemBody,
    NoticeBody,
    RenderSettings,
    outcome_text,
    times,
)

BREAK_EVEN_KIND: Final = _v.BREAK_EVEN_KIND
SIDE_WORD: Final = {"BUY": "long", "SELL": "short"}
FAULT_OUTCOMES: Final = frozenset({"NO_STOP", "DATA_UNAVAILABLE", "STRATEGY_ERROR", "WRITE_ERROR"})
FAULT_ACTION: Final = "Check arm decisions; no advice for this position this bar."
OUTCOME_BADGES: Final[dict[str, tuple[str, str]]] = {
    "COMPLETE": ("RELEASED", "CLEARED"),
    "PRE_NOT_CONFIRMED": ("NOT CONFIRMED", "FLAG"),
    "AT_TARGET_UNCONFIRMED": ("INFO", "INFO"),
    "LOOSENED_AFTER_COMPLETE": ("STOP MOVED BACK", "FLAG"),
    "BROKER_AHEAD": ("NOTHING TO MOVE", "CLEARED"),
    "STRATEGY_REASON": ("INFO", "INFO"),
    "GUARD_REASON": ("INFO", "INFO"),
    "NO_BROKER_BAR": ("INFO", "INFO"),
    "MARKET_CLOSED": ("HOLD", "HOLD"),
    "PRICE_THROUGH_LEVEL": ("HOLD", "HOLD"),
    "NO_STOP": ("FAULT", "FAULT"),
    "DATA_UNAVAILABLE": ("FAULT", "FAULT"),
    "STRATEGY_ERROR": ("FAULT", "FAULT"),
    "WRITE_ERROR": ("FAULT", "FAULT"),
}
_CATEGORY_TOKEN: Final = {"ACTIONABLE": "FINAL", "INFO": "INFO", "HOLD": "HOLD", "FAULT": "FAULT"}


def _side(item_side: str) -> str:
    return SIDE_WORD.get(item_side, item_side or "side not recorded")


def _event_tags(item: ItemBody) -> list[tuple[str, str]]:
    tags: list[tuple[str, str]] = []
    if item.event == "LATE" or item.late:
        tags.append(("LATE", "LATE"))
    if item.event == "CORRECTED":
        tags.append(("CORRECTED", "LATE"))
    if item.event == "REOPEN":
        tags.append(("REOPEN", "INFO"))
    if DELAYED in item.caveats and item.decided_at_ms > item.bar_end_ms:
        tags.append(("DELAYED", "LATE"))
    return tags


def _symbol_side(symbol: str, side: str, visuals: bool) -> Field:
    """"Symbol · side": the side as a neutral chip (V8) when it is a known code and visuals are on."""
    if visuals and side in SIDE_WORD:
        return Field("Symbol · side", _id(symbol) or "not recorded", chip=side)
    return Field("Symbol · side", f"{_id(symbol) or 'not recorded'} · {_side(side)}")


def _identity(item: ItemBody, route: str, visuals: bool = False) -> list[Field]:
    return [_symbol_side(item.symbol, item.side, visuals),
            Field("Position", f"{_id(item.position_id)} ({_id(item.adapter_id)}) · route {_id(route)}",
                  mono=True)]


def _bar_field(item: ItemBody, bar_spec: str) -> Field:
    spec = f" · {_id(bar_spec)}" if bar_spec else ""
    return Field("Decided on bar", f"bar end {_arm_times(item.bar_end_ms)}{spec}")


def _arm_times(ms: int) -> str:
    try:
        return times(ms)
    except (OverflowError, OSError, ValueError):
        return "not recorded"


def _arm_clock(ms: int) -> str:
    try:
        return clock(ms)
    except (OverflowError, OSError, ValueError):
        return "not recorded"


def _details_table(item: ItemBody) -> list[Field]:
    """The small details table (no favour figures, ever: F2)."""
    out: list[Field] = []
    if item.current_risk_r is not None:
        out.append(Field("Current risk", f"{format(item.current_risk_r, 'f')} R"))
    if item.first_bar_ratio is not None:
        out.append(Field("First-bar ratio", format(item.first_bar_ratio, "f")))
    if item.borderline:
        out.append(Field("Borderline", "yes"))
    if item.stop_basis:
        out.append(Field("Stop basis", _id(item.stop_basis), mono=True))
    if item.trigger_source:
        out.append(Field("Close source", _id(item.trigger_source), mono=True))
    if item.quality_flags:
        out.append(Field("Quality flags", ", ".join(_id(q) for q in item.quality_flags), mono=True))
    caveats = [CAVEAT_TEXT.get(c, _id(c)) for c in item.caveats if c not in (FIRST_BAR_CONDITIONAL, DELAYED)]
    if caveats:
        out.append(Field("Caveats", "; ".join(caveats)))
    if item.strategy_kind:
        out.append(Field("Strategy", f"{_id(item.strategy_kind)} v{item.strategy_impl_version}", mono=True))
    out.append(Field("Decision id", str(item.decision_id), mono=True))
    return out


def _card(rows: Sequence[str], token: str) -> str:
    c = _color(token)
    inner = "".join(rows)
    return _row('<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
                f'style="border:1px solid {LINE};border-left:4px solid {c};">{inner}</table>', "0 20px 16px")


def _card_badges(badges: Sequence[tuple[str, str]]) -> str:
    return _row("".join(badge(lbl, tok) for lbl, tok in badges), "12px 14px 2px")


def _card_head(text: str) -> str:
    return _row(f'<div style="font:600 17px/1.35 {FONT};color:{INK};">{_e(text, LINE_LIMIT)}</div>', "4px 14px 10px")


def _card_block(inner: str) -> str:
    return _row(inner, "0 14px 12px") if inner else ""


def adjust_headline(item: ItemBody) -> str:
    sym, side = _id(item.symbol) or "position", _side(item.side)
    proposed = _dec(item.proposed_stop)
    # §20 Q6 / Q1 (a): every break_even_on_risk_close target is "break-even", at the entry and at the entry plus a
    # buffer (or rounded toward profit), as the single §B8 COMPLETE text (render.COMPLETE_TEXT); the preheader adds
    # the exact level. §B8 fixes no other ADJUST headline.
    be = item.strategy_kind == BREAK_EVEN_KIND
    if item.event == "PRE":
        target = "break-even" if be else (proposed or "a new level")
        return f"Heads-up: {sym} {side} stop would move to {target}"
    return f"Move stop to break-even: {sym} {side}" if be else f"Move stop: {sym} {side}"


def _current_stop(item: ItemBody) -> str:
    level = _dec(item.stop_now)
    parts = [level if level is not None else "not recorded"]
    if item.stop_read_at_ms is not None:
        parts.append(f"as read {_arm_clock(item.stop_read_at_ms)}")
    text = " ".join(parts)
    if item.handles:
        text += " · handles " + ", ".join(_id(h) for h in item.handles)
    return text


def _adjust_strip(item: ItemBody, token: str, visuals: bool) -> tuple[str, str | None]:
    """V7 for an ADJUST card: (strip html, the field key it replaces). PRE: bar end and valid until, in time order;
    a delayed final advice: bar end and decided, with the existing "N min after bar end" text."""
    if not visuals:
        return "", None
    if item.event == "PRE" and item.valid_until_ms is not None:
        pts = sorted([(item.bar_end_ms, "Bar end"), (item.valid_until_ms, "Valid until")])
        strip = _v.safe_part(_v.time_strip, [(lbl, _arm_clock(ms)) for ms, lbl in pts], "", token)
        return strip, "Valid until" if strip else None
    if item.event != "PRE" and DELAYED in item.caveats and item.decided_at_ms > item.bar_end_ms:
        gap = f"{(item.decided_at_ms - item.bar_end_ms) // 60_000} min after bar end"
        strip = _v.safe_part(_v.time_strip, [("Bar end", _arm_clock(item.bar_end_ms)),
                                             ("Decided", _arm_clock(item.decided_at_ms))], gap, token)
        return strip, "Delayed" if strip else None
    return "", None


def _adjust_card(item: ItemBody, bar_spec: str, route: str, settings: RenderSettings, *,
                 head: bool = True, visuals: bool = True) -> tuple[str, str]:
    """The SL advice block (§5): status, identity, entry, current stop, proposed stop, the still-worse rule and what
    to do; with `visuals`, the advice stepper, the time strip and the ordinal stop ladder (V5, V7, V4). Returns (card
    html, its token)."""
    pre = item.event == "PRE"
    token = "PROVISIONAL" if pre else "FINAL"
    badges = [("HEADS-UP" if pre else "MOVE STOP", token), *_event_tags(item)]
    level = _dec(item.proposed_stop)
    proposed = level or "not recorded"
    strip, replaced = _adjust_strip(item, token, visuals)
    fields = _identity(item, route, visuals)
    if settings.show_levels:
        fields.append(Field("Entry", _dec(item.entry) or "not recorded", mono=True))
    else:
        fields.append(Field("Entry", "not shown (show_levels off)"))
    fields.append(Field("Current stop", _current_stop(item)))
    fields.append(Field("Proposed new stop", proposed, big=True))
    fields.append(_bar_field(item, bar_spec))
    fields.append(Field("Provisional / final",
                        "Provisional: decided on the live quote before the bar closed; the final advice follows after "
                        "the bar end." if pre else "Final: decided on the closed broker bar."))
    if pre and item.valid_until_ms is not None and replaced != "Valid until":
        fields.append(Field("Valid until", _arm_clock(item.valid_until_ms)))
    if pre and FIRST_BAR_CONDITIONAL in item.caveats:
        fields.append(Field("Bar-0 label", pre_label(bool(item.trigger_met))))
    if not pre and DELAYED in item.caveats and item.decided_at_ms > item.bar_end_ms and replaced != "Delayed":
        fields.append(Field("Delayed", f"decided {(item.decided_at_ms - item.bar_end_ms) // 60_000} min after bar "
                                       "end"))
    if item.event == "CORRECTED":
        fields.append(Field("Corrected", f"revision {item.revision}"))
    # the boxed rule line needs a level to compare against: left out when the proposed stop is not recorded
    rule = "" if level is None else (
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td bgcolor="#FEF0C7" style="background-color:#FEF0C7;padding:10px 12px;font:700 14px/1.45 {FONT};'
        f'color:{INK};">{_e(still_worse_line(level), LINE_LIMIT)}</td></tr></table>')
    if pre:
        action = "No action required yet; final advice follows after the bar closes."
    elif level is None:
        action = ("The proposed stop was not recorded: check arm decisions before changing the stop at the broker. "
                  "This server sends nothing.")
    else:
        handles = ", ".join(_id(h) for h in item.handles) or "the position's stop"
        action = (f"At the broker, move the stop on handle(s) {handles} to {proposed}, only if it is still worse "
                  "than this. This server sends nothing.")
    what = _box(f"<strong>What to do:</strong> {_e(action, LINE_LIMIT)}", border=_color(token))
    details = _fields_table(_details_table(item))
    steps = _v.safe_part(_v.advice_steps, item, visuals)
    ladder = _v.safe_part(_v.stop_ladder, item, settings, token, pre) if visuals else ""
    rows = [_card_badges(badges), _card_head(adjust_headline(item)) if head else "", _card_block(steps),
            _card_block(_fields_table(fields)), _card_block(strip), _card_block(ladder),
            _card_block(rule), _card_block(what),
            _card_block(f'<div style="font:600 12px/1.4 {FONT};color:{MUTED};padding:0 0 4px;">Details</div>'
                        + details)]
    return _card(rows, token), token


def _outcome_badge(item: ItemBody) -> tuple[str, str]:
    """(label, token) of a non-ADJUST item's card badge."""
    if item.outcome == "UNRESOLVED_RANGE" or item.outcome not in OUTCOME_BADGES:
        return _id(item.category) or "INFO", _CATEGORY_TOKEN.get(item.category, "INFO")
    return OUTCOME_BADGES[item.outcome]


def _outcome_card(item: ItemBody, bar_spec: str, route: str, settings: RenderSettings, *,
                  head: bool = True, visuals: bool = True) -> tuple[str, str]:
    """A compact card for every other outcome: badge, the text's outcome line, (the advice stepper for COMPLETE),
    identity."""
    label, token = _outcome_badge(item)
    badges = [(label, token), *_event_tags(item)]
    rows = [_card_badges(badges), _card_head(outcome_text(item, settings)) if head else "",
            _card_block(_v.safe_part(_v.advice_steps, item, visuals))]
    fields = _identity(item, route, visuals) + [_bar_field(item, bar_spec)]
    if item.event == "CORRECTED":
        withdrawn = "; the earlier advice is withdrawn" if item.supersedes is not None else ""
        fields.append(Field("Corrected", f"revision {item.revision}{withdrawn}"))
    fields.append(Field("Decision id", str(item.decision_id), mono=True))
    rows.append(_card_block(_fields_table(fields)))
    if item.outcome in FAULT_OUTCOMES:
        rows.append(_card_block(_box(f"<strong>What to do:</strong> {_e(FAULT_ACTION)}", border=_color("FAULT"))))
    return _card(rows, token), token


def _extra_token(e: ExtraBody) -> str:
    return "FLAG" if e.kind == "SUSPENDED" else "CLEARED" if e.kind == "FAILED_CLEARED" else "INFO"


def _extra_card(e: ExtraBody, visuals: bool = True) -> tuple[str, str, str | None]:
    token = _extra_token(e)
    what = EXTRA_TEXT.get(e.kind, _id(e.kind))
    fields: list[Field] = []
    if e.position_id:
        fields += [Field("Position", f"{_id(e.position_id)} ({_id(e.adapter_id)})", mono=True),
                   _symbol_side(e.symbol, e.side, visuals)]
    if e.reason:
        fields.append(Field("Reason", _id(e.reason), mono=True))
    if e.at_ms > 0:
        fields.append(Field("At", _arm_clock(e.at_ms)))
    action = "Run arm rearm after checking the position." if e.kind == "SUSPENDED" else None
    rows = [_card_badges([(_id(e.kind).replace("_", " "), token)]), _card_head(what),
            _card_block(_fields_table(fields))]
    if action:
        rows.append(_card_block(_box(f"<strong>What to do:</strong> {_e(action)}", border=_color(token))))
    return _card(rows, token), token, action


def arm_preheader(body: NoticeBody, settings: RenderSettings | None = None) -> str:
    """The inbox preview line (the subject itself is unchanged): the move and its level for a lone ADJUST item."""
    adjust = [i for i in body.items if i.outcome == "ADJUST"]
    if len(adjust) == 1:
        i = adjust[0]
        proposed = _dec(i.proposed_stop)
        return adjust_headline(i) + (f" → {proposed}" if proposed else "")
    return _arm_headline(body, settings or RenderSettings())


def _arm_headline(body: NoticeBody, settings: RenderSettings) -> str:
    items = body.items
    if body.event == "ALIVE" and body.alive is not None:
        a = body.alive
        return f"ARM alive: {a.armed} managed, {a.faulting} in FAULT, {a.unmanaged} not managed"
    if body.event == "TEST":
        return f"Daily test notice on channel {_id(body.channel or 'not recorded')}"
    if body.event == "MD_RESET":
        return f"Market-data store reset (generation {_id(body.generation or 'not recorded')})"
    if body.event == "FAILED_CLEARED":
        return EXTRA_TEXT["FAILED_CLEARED"][0].upper() + EXTRA_TEXT["FAILED_CLEARED"][1:]
    if len(items) == 1:
        i = items[0]
        return adjust_headline(i) if i.outcome == "ADJUST" else outcome_text(i, settings)
    n_adj = sum(1 for i in items if i.outcome == "ADJUST")
    if items:
        lead = "Heads-up" if body.event == "PRE" else "ARM advice"
        move = f", {n_adj} stop move{'s' if n_adj != 1 else ''}" if n_adj else ""
        return f"{lead}: {len(items)} item{'s' if len(items) != 1 else ''}{move} ({_id(body.route)})"
    if body.pending:
        return f"Awaiting the broker's final bar: {len(body.pending)} position(s)"
    if body.extras:
        e = body.extras[0]
        return EXTRA_TEXT.get(e.kind, _id(e.kind)) + (f" ({len(body.extras)} items)" if len(body.extras) > 1 else "")
    return f"ARM {_id(body.event)} notice"


def arm_layout(body: NoticeBody) -> str:
    return "L5" if body.items or body.pending else "L6"


ORDER: Final = _v.SUMMARY_ORDER
"""Significance of the item tokens: the notice badge when no stop move leads, and the summary chip order."""


def _summary_entries(body: NoticeBody) -> list[tuple[str, str, int]]:
    """V10 entries, one per card, labelled as the card badges: (label, token, count)."""
    out: list[tuple[str, str, int]] = []
    for item in body.items:
        if item.outcome == "ADJUST":
            out.append(("HEADS-UP", "PROVISIONAL", 1) if item.event == "PRE" else ("MOVE STOP", "FINAL", 1))
        else:
            out.append((*_outcome_badge(item), 1))
    out.append(("AWAITING BAR", "INFO", len(body.pending)))
    out += [(_id(e.kind).replace("_", " "), _extra_token(e), 1) for e in body.extras]
    out.append(("FAILED", "FAULT", len(body.failed)))
    return out


def _alive_tiles(body: NoticeBody) -> str:
    """V6 for ALIVE: managed / in FAULT / not managed (the counts the headline already gives)."""
    a = body.alive
    if body.event != "ALIVE" or a is None:
        return ""
    tiles = (Tile(str(int(a.armed)), "managed"),
             Tile(str(int(a.faulting)), "in FAULT", "FAULT" if a.faulting > 0 else None),
             Tile(str(int(a.unmanaged)), "not managed"))
    html_tiles = _v.safe_part(_v.count_tiles, tiles)
    return _row(html_tiles) if html_tiles else ""


def _tighter(item: ItemBody) -> bool | None:
    """True when the proposed stop is closer to the entry side than the current one (a long's stop moves up)."""
    if item.proposed_stop is None or item.stop_now is None or item.side not in SIDE_WORD:
        return None
    up = item.proposed_stop > item.stop_now
    return up if item.side == "BUY" else not up


def _xtnl_levels(item: ItemBody, settings: RenderSettings, unit: object, accent: str) -> str:
    """The themed levels block of an ADJUST item: entry (Q13 `show_levels` only), current stop, proposed stop, each
    stop's distance from the entry in pips when the entry is shown and the instrument's pip unit is known (owner
    decision 2026-11-21); with the entry hidden, only the size of the move. The pip unit itself is never shown."""
    from src.notifications import email_theme as th
    pip = unit if isinstance(unit, th.PipUnit) else None
    entry = item.entry if settings.show_levels else None

    def dist(level: Decimal | None) -> tuple[Decimal, str, str] | None:
        if level is None or entry is None or pip is None:
            return None
        d, where = th.pip_distance(level, entry, pip)
        return d, where, pip.word

    levels: list[th.Level] = []
    if entry is not None:
        levels.append(th.Level("Entry", _dec(entry)))
    levels.append(th.Level("Stop now", _dec(item.stop_now) or "not recorded", dist(item.stop_now)))
    if item.proposed_stop is not None:
        levels.append(th.Level("Proposed stop", _dec(item.proposed_stop), dist(item.proposed_stop), key=True))
    note = ""
    tighter = _tighter(item)
    if pip is not None and item.proposed_stop is not None and item.stop_now is not None:
        move = abs((item.proposed_stop - item.stop_now) / pip.size).quantize(Decimal("0.1"))
        how = "" if tighter is None else (" tighter" if tighter else " looser")
        note = f"Moves the stop {move} {pip.word}{how}."
    if not settings.show_levels:
        note = (note + " The entry is not shown (show_levels off).").strip()
    if item.event == "PRE" and FIRST_BAR_CONDITIONAL in item.caveats:
        # §B6.5 / §B8: a bar-0 PRE that proposes a move is shown only with its conditional label, first
        note = f"{pre_label(bool(item.trigger_met))}. {note}".strip()
    sym = f"{_id(item.symbol) or 'position'} {_side(item.side)}"
    return th.levels_rows(f"Levels · {sym}", levels, accent, note)


def _delayed_text(item: ItemBody) -> str | None:
    """§B5.2: a POST decided after `T + post_grace` is labelled DELAYED with its delay (as the text part)."""
    if DELAYED in item.caveats and item.decided_at_ms > item.bar_end_ms:
        return f"decided {(item.decided_at_ms - item.bar_end_ms) // 60_000} min after bar end"
    return None


def _caveat_texts(item: ItemBody) -> list[str]:
    """The item's caveats in words (§B6.3 `be_offset_first_seen`, §B7 S0, Q14 roll, ...), as the text part."""
    return [CAVEAT_TEXT.get(c, _id(c)) for c in item.caveats if c not in (FIRST_BAR_CONDITIONAL, DELAYED)]


def _xtnl_item_facts(item: ItemBody) -> list[tuple[str, str]]:
    """The facts under an ADJUST item's levels block: the §B8 allow-list fields the classic card and the text part
    carry (valid until, DELAYED, current risk R, first-bar ratio, borderline, caveats). Never favour R or favour /
    risk, a pip size or an increment."""
    facts: list[tuple[str, str]] = []
    if item.event == "PRE" and item.valid_until_ms is not None:
        facts.append(("Valid until", _arm_clock(item.valid_until_ms)))
    delayed = _delayed_text(item)
    if delayed is not None:
        facts.append(("Delayed", delayed))
    if item.current_risk_r is not None:
        facts.append(("Current risk", f"{format(item.current_risk_r, 'f')} R"))
    if item.first_bar_ratio is not None:
        facts.append(("First-bar ratio", format(item.first_bar_ratio, "f")))
    if item.borderline:
        facts.append(("Borderline", "yes"))
    facts += [("Caveat", c) for c in _caveat_texts(item)]
    return facts


def _render_arm_xtnl(body: NoticeBody, s: RenderSettings, footer: FooterSource | None, outbox_id: int | None,
                     title: str | None, pip_units: Mapping[tuple[str, str], object] | None) -> str:
    """The XTNL-theme page of one ARM notice (`email_theme`): status word, plain headline, steps, the levels block of
    every ADJUST item (with the bar-0 PRE label, §B6.5) and its facts (valid until, DELAYED, R, ratio, borderline,
    caveats), the other items with their DELAYED line and caveats, pending positions, extras and FAILED scopes, the
    facts and the operator block. Favour R and favour / risk are never shown."""
    from src.notifications import email_theme as th
    adjust = [i for i in body.items if i.outcome == "ADJUST"]
    final = [i for i in adjust if i.event != "PRE"]
    faults = [i for i in body.items if i.outcome in FAULT_OUTCOMES]
    # a stop move leads (as the classic notice badge), except under notice-level FAILED scopes ("do not act on ARM
    # advice"); item faults, MD_RESET and a suspended position are the operator's
    if body.event == "FAILED_CLEARED":
        word, sev = "DONE", "resolved"
    elif final and not body.failed:
        word, sev = "ACT", "important"
    elif body.failed or faults or body.event == "MD_RESET" or any(e.kind == "SUSPENDED" for e in body.extras):
        word, sev = "FIX", "urgent" if (body.failed or faults) else "important"
    else:
        word, sev = "FYI", "headsup" if adjust else "info"
    audience = ("You, the trader" if word == "ACT" or adjust else "Operator" if word in ("FIX", "DONE")
                else "For your records")
    accent = th.SEVERITY[sev][0]
    stage = {"PRE": "Heads-up (provisional)", "POST": "Final advice", "LATE": "Late advice",
             "CORRECTED": "Corrected advice", "REOPEN": "Advice reopened", "ALIVE": "Daily check", "TEST": "Test",
             "MD_RESET": "Market data reset", "FAILED_CLEARED": "Cleared"}.get(body.event, _id(body.event).title())
    headline = _arm_headline(body, s)
    if len(final) == 1 and final[0].proposed_stop is not None and len(body.items) == 1:
        i = final[0]
        headline = f"Move the stop: {_id(i.symbol) or 'position'} {_side(i.side)} to {_dec(i.proposed_stop)}"
    headline = headline[:1].upper() + headline[1:]
    rows = [th.masthead_rows(word, "ARM advice", audience, headline, sev, stage, th.melbourne_when(body.created_ms))]
    steps: list[tuple[str, str]] = []
    units = pip_units if pip_units is not None else {}
    for i in final:
        proposed = _dec(i.proposed_stop)
        handles = ", ".join(_id(h) for h in i.handles) or f"position {_id(i.position_id)}"
        if proposed is None:
            steps.append(("check", "The proposed stop was not recorded: check arm decisions before changing the stop "
                                   "at the broker."))
            continue
        steps.append(("broker", f"Move the stop on {handles} ({_id(i.symbol)}) to {proposed}."))
        steps.append(("check", f"Move {STILL_WORSE.replace('than this', 'than ' + proposed)}."))
    if final:
        steps.append(("wait", "Or leave it: this is advice. The server changes nothing at the broker."))
    for i in faults:
        steps.append(("server", f"{_id(i.symbol) or _id(i.position_id)}: {FAULT_ACTION}"))
    for f in body.failed:
        steps.append(("server", f"See arm failed show: {_id(f.scope_kind)} {_id(f.scope_id)} ({_id(f.reason)}); don't "
                                "act on ARM advice for it."))
    for e in body.extras:
        if e.kind == "SUSPENDED":
            steps.append(("server", "Run arm rearm after checking the position "
                                    f"{_id(e.position_id) or ''}.".replace(" .", ".")))
    if body.event == "MD_RESET":
        steps.append(("wait", "Advice resumes once the series are LIVE."))
    if steps:
        rows.append(th.steps_rows(steps, accent))
    else:
        nothing = ("No action yet: the final advice follows after the bar closes." if adjust else
                   "No action; this proves the channel." if body.event == "TEST" else "Nothing to do.")
        rows.append(th.nothing_row(nothing, accent))
    for i in adjust:
        rows.append(_xtnl_levels(i, s, units.get((i.adapter_id, i.symbol)), accent))
        rows.append(th.facts_rows(_xtnl_item_facts(i)))
    when = th.melbourne_clock(body.created_ms)
    if body.event == "PRE":
        rows.append(th.story_rows([th.Event("bar close", "Final advice", "after the bar closes", "next"),
                                   th.Event(when, "Heads-up", "Provisional. This email.", "now")], accent))
    elif final:
        rows.append(th.story_rows([th.Event("next bar", "Next check", "when the next bar closes", "next"),
                                   th.Event(when, stage, "This email.", "now")], accent))
    others: list[tuple[str, str]] = []
    for i in body.items:
        if i.outcome == "ADJUST":
            continue
        name = f"{_id(i.symbol) or _id(i.position_id)} {_side(i.side)}"
        others.append((name, outcome_text(i, s)))
        delayed = _delayed_text(i)
        if delayed is not None:
            others.append((f"{name} · delayed", delayed))
        others += [(f"{name} · caveat", c) for c in _caveat_texts(i)]
    others += [(f"{_id(p.symbol) or _id(p.position_id)} {_side(p.side)}", AWAITING_TEXT) for p in body.pending]
    others += [(_id(e.kind).replace("_", " ").title(), EXTRA_TEXT.get(e.kind, _id(e.kind))
                + (f" ({_id(e.position_id)})" if e.position_id else "")) for e in body.extras]
    if body.alive is not None and body.event == "ALIVE":
        a = body.alive
        others += [("Managed", str(int(a.armed))), ("In FAULT", str(int(a.faulting))),
                   ("Not managed", str(int(a.unmanaged)))]
    rows.append(th.facts_rows(others))
    part = "item" if body.item_key else (f"part {body.part}" if body.event in ("PRE", "POST") else "-")
    node, epoch = th.node_epoch(footer)
    ops = [("Notice", f"#{body.notice_id}" + (f" · alert #{outbox_id}" if outbox_id is not None else "")),
           ("Route", _id(body.route)), ("Event", f"{_id(body.event)} · {part}"),
           ("Bar end" if body.bar_end_ms > 0 else "Created",
            _arm_times(body.bar_end_ms if body.bar_end_ms > 0 else body.created_ms)),
           ("Node", f"{node} · epoch {epoch}")]
    if body.bar_spec:
        ops.append(("Bar spec", _id(body.bar_spec)))
    for i in body.items:
        ops.append(("Decision", f"{i.decision_id} · {_id(i.position_id)} ({_id(i.adapter_id)}) · {_id(i.outcome)}"
                                f" · {_id(i.strategy_kind) or 'strategy not recorded'}"))
    rows.append(th.operator_rows(ops))
    pre = arm_preheader(body, s)
    return th.theme_page(title=title or pre, preheader=f"{word} · ARM advice · {pre}", accent=accent, rows=rows)


def render_arm_html(body: NoticeBody, *, footer: FooterSource | None = None, settings: RenderSettings | None = None,
                    outbox_id: int | None = None, title: str | None = None, theme: str = "classic",
                    pip_units: Mapping[tuple[str, str], object] | None = None) -> str:
    """The HTML part of one ARM notice (L5 advice, L6 status) from its structured body. Never shows favour R or
    favour / risk; prices shown are order-state levels only (entry, current stop, proposed stop).

    `theme="xtnl"`: the XTNL-theme page (`_render_arm_xtnl`), whose levels block shows each stop's distance from the
    entry in pips when `pip_units` holds the item's `(adapter_id, symbol)` unit (`email_theme.PipUnit`, read by the
    relay at send time); the classic page ignores `pip_units`.

    Size guard: once the card HTML so far reaches `email_visuals.VISUAL_BUDGET` bytes, every later card is built
    without its optional visuals (stepper, time strip, stop ladder, side chip), exactly as before them."""
    s = settings or RenderSettings()
    if theme == "xtnl":
        return _render_arm_xtnl(body, s, footer, outbox_id, title, pip_units)
    cards: list[str] = []
    tokens: list[str] = []
    actions: list[str] = []
    adjust_token: str | None = None
    used = 0

    def visuals() -> bool:
        return used < _v.VISUAL_BUDGET

    for item in body.items:
        if item.outcome == "ADJUST":
            html_card, tok = _adjust_card(item, body.bar_spec, body.route, s, head=len(body.items) > 1,
                                          visuals=visuals())
            adjust_token = adjust_token or tok
        else:
            html_card, tok = _outcome_card(item, body.bar_spec, body.route, s, head=len(body.items) > 1,
                                           visuals=visuals())
        cards.append(html_card)
        used += len(html_card.encode("utf-8"))
        tokens.append(tok)
    if body.pending:
        chips = visuals()
        waiting = [(f"{_id(p.position_id)} ({_id(p.adapter_id)})", _id(p.symbol),
                    p.side if chips and p.side in SIDE_WORD else _side(p.side)) for p in body.pending]
        cards.append(_row(f'<div style="padding:0 0 6px;">{badge("INFO", "INFO")}<span style="font:600 14px/1.4 '
                          f'{FONT};color:{INK};">{_e(AWAITING_TEXT)}</span></div>', "0 20px 4px"))
        cards.append(_table_row(("Position", "Symbol", "Side"), waiting, None, 2 if chips else None, SIDE_WORD))
        used += len(cards[-1].encode("utf-8")) + len(cards[-2].encode("utf-8"))
        tokens.append("INFO")
    for e in body.extras:
        html_card, tok, action = _extra_card(e, visuals())
        cards.append(html_card)
        used += len(html_card.encode("utf-8"))
        tokens.append(tok)
        if action:
            actions.append(action)
    if body.failed:
        lines = [f"FAILED: {_id(f.scope_kind)} {_id(f.scope_id)} ({_id(f.reason)}); see arm failed show"
                 for f in body.failed]
        inner = "".join(f'<div style="padding:3px 0;font:600 13px/1.4 {FONT};color:#B42318;">{_e(x, LINE_LIMIT)}'
                        "</div>" for x in lines)
        cards.append(_row(_box(inner, border=_color("FAULT"), bg="#FEF3F2")))
        tokens.append("FAULT")
    # the notice's own badge (and strip): the first ADJUST's status (FINAL / PROVISIONAL) whenever the notice carries
    # a stop move, so the move stays the headline even beside a FAULT item card (each card keeps its own badge), except
    # that notice-level FAILED scopes ("do not act on ARM advice") keep FAULT; else the most significant item by
    # `ORDER`; status events by event
    if body.event == "MD_RESET":
        token, label = "HIGH", "MD RESET"
    elif body.event == "TEST":
        token, label = "INFO", "TEST"
    elif body.event == "ALIVE":
        token, label = "INFO", "ALIVE"
    elif body.event == "FAILED_CLEARED":
        token, label = "CLEARED", "CLEARED"
    else:
        token = ("FAULT" if body.failed else adjust_token) or next((t for t in ORDER if t in tokens), "INFO")
        label = {"FINAL": "FINAL ADVICE", "PROVISIONAL": "PROVISIONAL"}.get(token, TOKENS[token][1])
        if body.event == "PRE" and token not in ("FAULT",):
            token, label = "PROVISIONAL", "PROVISIONAL"
    headline = _arm_headline(body, s)
    rows = _hero_rows([(label, token)], f"ARM · {_id(body.route)}", headline, token,
                      _v.notice_glyph(token, arm_layout(body)))
    rows.append(_alive_tiles(body))
    if body.event == "PRE":
        rows.append(_banner_row(PRE_HEADER, "PROVISIONAL"))
    status_action = {"TEST": "No action; this proves the channel.",
                     "MD_RESET": "Advice resumes once the series are LIVE."}.get(body.event)
    if body.failed and not status_action:
        status_action = "See arm failed show; do not act on ARM advice for the FAILED scopes."
    if status_action:
        rows.append(_action_row(status_action, token))
    part = "item" if body.item_key else (f"part {body.part}" if body.event in ("PRE", "POST") else "-")
    when = body.bar_end_ms if body.bar_end_ms > 0 else body.created_ms
    rows.append(_fields_row([
        Field("Route", _id(body.route), mono=True), Field("Event", _id(body.event), mono=True),
        Field("Part", part), Field("Bar end" if body.bar_end_ms > 0 else "Created", _arm_times(when)),
        *([Field("Bar spec", _id(body.bar_spec), mono=True)] if body.bar_spec else []),
        Field("Notice id", f"#{body.notice_id}", mono=True)]))
    if len(body.items) + len(body.pending) + len(body.extras) + len(body.failed) >= 2:
        rows.append(_v.safe_part(_v.summary_chips, _summary_entries(body), ORDER))
    rows.extend(cards)
    ref = f"notice #{body.notice_id}" + (f" · alert #{outbox_id}" if outbox_id is not None else "")
    pre = arm_preheader(body, s)
    return page(title=title or pre, preheader=pre, token=token, rows=rows,
                footer_html=_footer_rows(footer, body.created_ms, ref))


def _dec(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def still_worse_line(proposed: str) -> str:
    """The boxed rule line: the text's `STILL_WORSE` qualifier (§B6.3 step 13) with the proposed level (only built
    when a proposed level is recorded; the SL block leaves the line out otherwise)."""
    return f"Move {STILL_WORSE.replace('than this', 'than ' + proposed)}."


__all__ = [
    "BREAK_EVEN_KIND", "FAULT_OUTCOMES", "OUTCOME_BADGES", "adjust_headline", "arm_layout", "arm_preheader",
    "render_arm_html",
]
