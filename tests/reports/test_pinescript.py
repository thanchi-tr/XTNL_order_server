"""Pine Script v6 renderer, position-tool style: per trade a red zone entry -> stop-loss and a green zone entry ->
take-profit over each step (fill and 1 px border), the entry line, entry / exit markers exactly on their anchors,
change dots; Label mode Minimal (default: the trade tag inside its own zone, no text labels, no stacking) and
Detailed (entry / exit text labels laid out, displacement capped); faint pending orders; one table row per trade;
limits (boxes too); symbol guard; determinism; escaping; lint; and the privacy rule (no size or amount ever
written)."""
from __future__ import annotations

import random
import re
from decimal import Decimal

import pytest

from src.dto import OrderDTO, OrderStatus, PositionDTO
from src.reports import lifecycle as lifecycle_mod
from src.reports.export import file_name, plan_from_read, summary_lines
from src.reports.lifecycle import (LcEvent, LogRead, PendingOrder, StateVersion, Step, Trade, Window,
                                   build_lifecycles, build_timelines, build_trades, version_from_row)
from src.reports.pinescript import (COLOURS, DEFAULT_CHAR_BARS, DEFAULT_MIN_BARS, ENTRY_BEFORE_WINDOW, ENTRY_SEEN,
                                    EXIT_CLOSED, EXIT_OPEN, EXIT_SL, EXIT_TP, GAP_LINES, LAYOUT_BUDGET, LEG_SL,
                                    LEG_TP, MAX_BOXES, MAX_LABELS, MAX_LINES, MAX_SHIFT_HEIGHTS, MAX_TABLE_ROWS,
                                    OPEN_MARK, PAD_CHARS, PAD_LINES, TABLE_COLUMNS, TABLE_HEADER_ROWS,
                                    TABLE_POSITIONS, TAG_TICKS, RenderInfo, RenderLimits, TextBox, layout,
                                    pine_float, pine_str, render_symbol, safe_id, trade_elements)

from tests.reports.builders import H, NOW, T0, Chart, d, emulate, lint, log_row, order, pine_arrays, position
from tests.reports.test_trades import bracket_tp, catch_up_trade, child

INFO = RenderInfo(generated_ms=NOW, since_ms=NOW - 7 * 24 * H, until_ms=NOW, adapter="stonex", orders=4,
                  positions=1)
M = 60_000


def book(*versions: StateVersion) -> tuple[list[Trade], list[PendingOrder]]:
    trades, pending = build_trades(build_lifecycles(build_timelines(versions), NOW), NOW)
    return list(trades), list(pending)


def render(*versions: StateVersion, info: RenderInfo = INFO, limits: RenderLimits = RenderLimits()) -> str:
    trades, pending = book(*versions)
    return render_symbol("EURUSD", trades, pending, info, limits).text


def quick(i: int, *, minutes: int = 30, entry: str = "1.1000", side: str = "BUY") -> list[StateVersion]:
    """A position with SL / TP on itself, closed `minutes` later (no closing fill: exit price not recorded)."""
    t = T0 + i * 60 * M
    oid = f"Q{i:04d}"
    sl, tp = ("1.0950", "1.1100") if side == "BUY" else ("1.1050", "1.0900")
    return [order(oid, t, "FILLED", entry, side=side),
            position(oid, t, "OPEN", entry, side=side, sl=sl, tp=tp),
            position(oid, t + minutes * M, "CLOSED", entry, side=side, sl=sl, tp=tp)]


def risk_only(oid: str = "R1", side: str = "BUY") -> list[StateVersion]:
    """A position with a stop-loss and no take-profit."""
    sl = "1.0950" if side == "BUY" else "1.1050"
    return [order(oid, T0, "FILLED", "1.1000", side=side), position(oid, T0, "OPEN", "1.1000", side=side, sl=sl),
            position(oid, T0 + 30 * M, "CLOSED", "1.1000", side=side, sl=sl)]


def bare(oid: str = "N1", side: str = "SELL") -> list[StateVersion]:
    """A position with neither stop-loss nor take-profit."""
    return [order(oid, T0, "FILLED", "1.1000", side=side), position(oid, T0, "OPEN", "1.1000", side=side),
            position(oid, T0 + 30 * M, "CLOSED", "1.1000", side=side)]


def pending_order(i: int) -> list[StateVersion]:
    t = T0 + i * 10 * M
    return [order(f"P{i:04d}", t, "WORKING", "1.0700"), order(f"P{i:04d}", t + 5 * M, "CANCELLED", "1.0700")]


def label_calls(text: str) -> list[str]:
    return [x for x in text.split("\n") if "label.new(" in x]


# ------------------------------------------------------------------------------------------------ structure
def test_structure() -> None:
    text = render(*bracket_tp())
    lines = text.split("\n")
    assert lines[0] == "//@version=6" and "read-only export, no amounts" in text
    ind = next(x for x in lines if x.startswith("indicator("))
    for want in ("overlay = true", "max_lines_count = 500", "max_labels_count = 500", "max_boxes_count = 500"):
        assert want in ind
    assert "barstate.islastconfirmedhistory or barstate.islast" in text and "var bool drawn = false" in text
    # the zone box (tag text on the tag step) and the Minimal fallback tag box
    assert text.count("box.new(") == 2
    assert ('string labelMode = input.string("Minimal", "Label mode", options = ["Minimal", "Detailed"], '
            'group = "Display")') in text
    assert 'bool minimal = labelMode != "Detailed"' in text
    for toggle in ("showZones", "showLevels", "showLabels", "showMarkers", "showAdjust", "showWorking",
                   "showPending", "showTable"):
        assert f"bool {toggle} = input.bool(true" in text
    assert 'bool adjText = input.bool(false, "Text on the change dots"' in text
    assert 'string tzIn = input.string("Australia/Melbourne", "Time zone"' in text
    assert f'int minBars = input.int({DEFAULT_MIN_BARS}, "Minimum trade width (bars)"' in text
    for want in ('int riskFillT = input.int(80, "Risk zone fill transparency"',
                 'int riskEdgeT = input.int(60, "Risk zone border transparency (1 px)"',
                 'int rewardFillT = input.int(85, "Reward zone fill transparency"',
                 'int rewardEdgeT = input.int(60, "Reward zone border transparency (1 px)"'):
        assert want in text, want
    for colour in ("C_RISK = input.color(#F23645", "C_REWARD = input.color(#089981", "C_BUY = input.color(",
                   "C_SELL = input.color(", "C_EXIT = input.color(", "C_PENDING = input.color("):
        assert colour in text
    assert 'int rowsIn = input.int(12, "Table rows (newest trades first)", minval = 1, maxval = 36' in text
    assert "str.format_time(t, timeFmt, tzName)" in text and "str.tostring(p, format.mintick)" in text
    assert re.search(r"^var int\[\] trEntryT = ", text, re.M) and text.count("table.new(") == 1
    assert lint(text) == []


def test_zones_have_a_fill_and_a_1px_border_of_their_colour() -> None:
    text = render(*bracket_tp())
    call = next(x for x in text.split("\n") if "box.new(x1, " in x)
    for want in ("bgcolor = color.new(zc, fillT)", "border_color = color.new(zc, edgeT)", "border_width = 1",
                 "border_style = line.style_solid"):
        assert want in call, want
    assert f"int fillT = leg == {LEG_SL} ? riskFillT : rewardFillT" in text
    assert f"int edgeT = leg == {LEG_SL} ? riskEdgeT : rewardEdgeT" in text
    assert f"color zc = leg == {LEG_SL} ? C_RISK : C_REWARD" in text


# ------------------------------------------------------------------------------------------------ one trade
def test_trade_is_drawn_like_the_position_tool() -> None:
    """The bracket (BUY, SL replaced once, TP hit): two red zones (one per stop step) from the entry to the stop,
    one green zone from the entry to the target, all from the entry time to the exit time; the entry line across;
    a green diamond at the exit; the opening order's working line before the fill."""
    ch = emulate(render(*bracket_tp()))
    e, x = T0 + H, T0 + 3 * H
    assert ch.spans == [(e, x)]
    assert ch.zones == [(0, LEG_SL, e, T0 + 2 * H, 1.0801, 1.075), (0, LEG_SL, T0 + 2 * H, x, 1.0801, 1.078),
                        (0, LEG_TP, e, x, 1.09, 1.0801)]
    assert ch.levels == [(0, e, T0 + 2 * H, 1.075), (0, T0 + 2 * H, x, 1.078), (0, e, x, 1.09)]
    assert ch.entry_lines == [(0, e, x, 1.0801)]
    assert [m[:5] for m in ch.exits] == [(0, x, 1.09, EXIT_TP, "T1")] and ch.exit_segments == []
    assert ch.working == [(0, T0, e, 1.08)]
    assert ch.dots == [(T0 + 2 * H, 1.078, "T1 BUY #O1: SL moved 07 Nov 10:20 @ 1.07800")]


# ------------------------------------------------------------------------------------------------ Minimal: tag
def test_minimal_tag_sits_inside_the_trades_own_reward_zone_at_the_entry_corner() -> None:
    # BUY: the reward zone lies above the entry, so the tag is bottom-left (the entry is the zone's bottom edge)
    ch = emulate(render(*bracket_tp()))
    assert ch.tags == [(0, T0 + H, T0 + 3 * H, 1.09, 1.0801, "T1 BUY", "bottom", False)]
    zone = next(z for z in ch.zones if z[1] == LEG_TP)
    assert ch.tags[0][1:5] == zone[2:6]                                  # exactly the reward zone's box
    # SELL: the reward zone lies below the entry: top-left
    ch = emulate(render(*catch_up_trade(T0, 1038153117)))
    lo, hi = ch.spans[0]
    assert ch.tags == [(0, lo, hi, 1.10225, 1.09764, "T1 SELL", "top", False)]
    text = render(*bracket_tp())
    call = next(x for x in text.split("\n") if "box.new(x1, " in x)
    for want in ("text = tagTxt", "text_size = size.small", "text_color = C_TEXT",
                 "text_halign = tagH, text_valign = tagV"):
        assert want in call, want
    assert ch.tag_align == ["left"]
    assert "bool tagHere = minimal and showLabels and array.get(trTagSt, c) == i" in text
    assert pine_arrays(text)["trTag"] == ['"T1 BUY"'] and pine_arrays(text)["trNo"] == ['"T1"']


def test_minimal_tag_uses_the_risk_zone_without_a_take_profit_and_a_tag_box_without_either() -> None:
    ch = emulate(render(*risk_only()))                                    # BUY: risk zone below the entry
    assert ch.tags == [(0, T0, T0 + 30 * M, 1.1, 1.095, "T1 BUY", "top", False)]
    ch = emulate(render(*risk_only(side="SELL")))                         # SELL: risk zone above the entry
    assert ch.tags == [(0, T0, T0 + 30 * M, 1.105, 1.1, "T1 SELL", "bottom", False)]
    text = render(*bare())
    ch = emulate(text)
    assert ch.zones == [] and ch.tags == [(0, T0, T0 + 30 * M, 1.1, 1.1, "T1 SELL", "top", True)]
    assert pine_arrays(text)["trTagSt"] == ["-1"]
    call = next(x for x in text.split("\n") if "box.new(array.get(trL, c)" in x)
    assert "ty + th, array.get(trR, c), ty - th" in call and f"float th = {TAG_TICKS} * syminfo.mintick" in text
    assert "bgcolor = color.new(C_EXIT, 100), border_color = color.new(C_EXIT, 100)" in call
    assert "text = array.get(trTag, c)" in call and "if array.get(tagged, c) == 0" in text
    # zones hidden: every trade's tag falls back to its tag box; Detailed draws no tag at all
    hidden = emulate(render(*bracket_tp()), show_zones=False)
    assert hidden.tags == [(0, T0 + H, T0 + 3 * H, 1.0801, 1.0801, "T1 BUY", "top", True)]
    assert emulate(render(*bracket_tp()), mode="Detailed").tags == []


def test_tag_box_is_counted() -> None:
    for versions, boxes in ((bracket_tp(), 3), (risk_only(), 1), (bare(), 1), (catch_up_trade(T0, 1), 2)):
        trades, pending = book(*versions)
        res = render_symbol("EURUSD", trades, pending, INFO)
        ch = emulate(res.text)
        assert res.boxes == boxes and len(ch.zones) + sum(1 for t in ch.tags if t[7]) <= res.boxes
        assert len(emulate(res.text, show_zones=False).tags) <= res.boxes


# ------------------------------------------------------------------------------------------------ markers
def test_markers_sit_exactly_on_the_entry_and_exit() -> None:
    ch = emulate(render(*bracket_tp()))
    (c, t, p, style, tip), = ch.entries
    assert (c, t, p, style) == (0, T0 + H, 1.0801, "up")
    assert tip.startswith("T1 BUY #O1 · entry 07 Nov 09:20 @ 1.08010\norders #O1 #S1 #S2 #TP1\n"
                          "placed 07 Nov 08:20 @ 1.08000\n")
    assert tip.endswith("closed 07 Nov 11:20 @ 1.09000 better\nTP hit")
    assert ch.exits == [(0, T0 + 3 * H, 1.09, EXIT_TP, "T1", "T1 exit 07 Nov 11:20 @ 1.09000 · TP hit")]
    # SL hit (SELL, entry time not recorded: the marker sits at the drawn left edge)
    ch = emulate(render(*catch_up_trade(T0, 1038153117)))
    assert [m[:4] for m in ch.entries] == [(0, T0 - DEFAULT_MIN_BARS * M, 1.10225, "down")]
    assert "T1 SELL #1038153117 · entry time not recorded @ 1.10225" in ch.entries[0][4]
    assert ch.exits == [(0, T0, 1.10234, EXIT_SL, "T1", "T1 exit 07 Nov 08:20 @ 1.10234 · SL hit")]
    # closed otherwise, no exit price: at the entry level
    ch = emulate(render(*quick(0)))
    assert ch.exits == [(0, T0 + 30 * M, 1.1, EXIT_CLOSED, "T1",
                         "T1 exit 07 Nov 08:50 @ price not recorded · closed")]
    text = render(*bracket_tp())
    exit_call = next(x for x in label_calls(text) if "label.new(array.get(trExitT, c)" in x)
    assert 'exitY(c), hd == c ? array.get(xTxt, c) : ""' in exit_call and "size = size.tiny" in exit_call
    assert (f"style = k == {EXIT_SL} ? label.style_xcross : k == {EXIT_TP} ? label.style_diamond : "
            "label.style_circle") in exit_call and "tooltip = array.get(xTip, hd)" in exit_call
    entry_call = next(x for x in label_calls(text) if "label.new(array.get(eAt, c), " in x)
    assert '"", xloc = xloc.bar_time, color = sideLine(sd)' in entry_call
    assert "style = sd == 1 ? label.style_triangleup : label.style_triangledown, size = size.tiny" in entry_call
    assert "tooltip = tipE" in entry_call


def test_open_trade_has_an_open_circle_at_its_right_edge() -> None:
    ch = emulate(render(*bracket_tp()[:6]))
    assert ch.spans == [(T0 + H, NOW)]
    assert ch.exits == [(0, NOW, 1.0801, EXIT_OPEN, OPEN_MARK, "T1 still open · at export 12 Nov 08:20")]
    assert ch.rows == [("T1", "09:20 @ 1.08010", "-", "open")]        # no unrealised result
    assert ch.result_tips == ["open: no result until closed; exit: still open"]
    text = render(*bracket_tp()[:6])
    call = next(x for x in label_calls(text) if "label.style_none" in x)
    assert f'label.new(array.get(trR, c), ep, "{OPEN_MARK}"' in call and "tooltip = exitTip(c)" in call
    assert emulate(text, mode="Detailed").labels[1][5] == "at export 12 Nov 08:20\nstill open"


def crowded() -> list[StateVersion]:
    """Trades a few minutes apart at nearly one price, both sides, some with SL moves, plus pending orders: the
    labels of neighbours compete for the same spot on every timeframe."""
    v: list[StateVersion] = []
    for i in range(24):
        t = T0 + i * 7 * M
        oid = f"C{i:03d}"
        side = "BUY" if i % 2 else "SELL"
        sl, tp = ("1.0950", "1.1100") if side == "BUY" else ("1.1050", "1.0900")
        v += [order(oid, t, "FILLED", "1.1000", side=side),
              position(oid, t, "OPEN", "1.1000", side=side, sl=sl, tp=tp),
              position(oid, t + 9 * M, "OPEN", "1.1000", side=side, sl="1.1000", tp=tp),
              position(oid, t + (15 + i % 4) * M, "CLOSED", "1.1000", side=side, sl="1.1000", tp=tp)]
    return v + [x for i in range(5) for x in pending_order(i)]


def test_minimal_has_no_text_labels_and_no_stacking() -> None:
    """Overlapping trades: every marker sits on its own anchor (no offset), every tag inside its own zone, and
    there is no text label to stack."""
    text = render(*crowded())
    trades, _ = book(*crowded())
    ch = emulate(text)
    assert ch.labels == []
    by_no = {t.name: t for t in trades}
    for c, x, y, _, tip in ch.entries:
        t = by_no[tip.split(" ", 1)[0]]
        assert (x, y) == (t.entry_ms, float(t.entry_price or 0))
    for c, x, y, k, own, tip in ch.exits:
        t = by_no[f"T{c + 1}"]
        assert x == t.exit_ms and y == float(t.exit_price if t.exit_price is not None else t.entry_price or 0)
        assert f"T{c + 1} exit " in tip                                     # its own exit is in its tooltip
    # every trade number is printed exactly once, on its group's first exit marker
    printed = [n for *_, own, _ in ch.exits for n in own.split()]
    assert sorted(printed) == sorted(t.name for t in trades)
    assert len(ch.tags) == len(trades) and all(not tg[7] for tg in ch.tags)
    for c, x1, x2, top, bottom, tag, _, _ in ch.tags:
        assert any(z[0] == c and z[2:] == (x1, x2, top, bottom) for z in ch.zones)
        assert tag == f"{by_no[tag.split(' ')[0]].name} {by_no[tag.split(' ')[0]].side}"
    # the script queues text labels (the only input of the layout) in Detailed mode only
    assert "        if showLabels and not minimal and array.size(trEntryT) > 0" in text
    assert "                if adjText and not minimal" in text
    # change text in Minimal: on its own point, no layout
    adj = emulate(text, adj_text=True).labels
    assert adj and all(lb[4] == 3 for lb in adj)
    assert {(lb[0], lb[1]) for lb in adj} == {(ev_t, ev_p) for ev_t, ev_p, _ in ch.dots}


def test_coinciding_trades_never_print_over_each_other() -> None:
    """The real week's T1 / T2 shape (two brackets stopped out 1.5 s apart at one price): on every timeframe each
    marker stays exactly on its own anchor, the exit numbers are printed once, together ('T1 T2'), on the first
    exit marker, both exit tooltips list both exits, the entry tooltips name each other, and the second tag takes
    another corner of its own zone instead of printing over the first."""
    text = render(*catch_up_trade(T0, 1038153117), *catch_up_trade(T0 + 1_500, 1038153178))
    for bar in (M, 5 * M, 15 * M, 60 * M):
        ch = emulate(text, bar_ms=bar)
        assert [(m[1], m[2], m[4]) for m in ch.exits] == [(T0, 1.10234, "T1 T2"), (T0 + 1_500, 1.10234, "")]
        both = "T1 exit 07 Nov 08:20 @ 1.10234 · SL hit\nT2 exit 07 Nov 08:20 @ 1.10234 · SL hit"
        assert [m[5] for m in ch.exits] == [both, both]
        lo = T0 - DEFAULT_MIN_BARS * bar
        assert [(m[1], m[2]) for m in ch.entries] == [(lo, 1.10225), (lo + 1_500, 1.10225)]
        assert all(m[4].endswith("\nentries here: T1 SELL, T2 SELL") for m in ch.entries)
        # both tags inside their own (identical) zones, at different corners
        (z1, z2) = [z for z in ch.zones if z[1] == LEG_TP]
        assert [t[:5] for t in ch.tags] == [(0, *z1[2:]), (1, *z2[2:])]
        corners = [(a, t[6]) for a, t in zip(ch.tag_align, ch.tags, strict=True)]
        assert corners == [("left", "top"), ("left", "bottom")]          # entry end, then the level end
        assert emulate(text, bar_ms=bar, tag_corner="Level end").tag_align == ["left", "left"]
        assert [t[6] for t in emulate(text, bar_ms=bar, tag_corner="Level end").tags] == ["bottom", "top"]
    # trades apart: no group, each exit carries its own number and tooltip only
    ch = emulate(render(*quick(0), *quick(5)))
    assert [m[4] for m in ch.exits] == ["T1", "T2"] and "T2" not in ch.exits[0][5]
    assert not any("entries here" in m[4] for m in ch.entries) and ch.tag_align == ["left", "left"]
    assert [t[6] for t in ch.tags] == ["bottom", "bottom"]                 # BUY: reward zone above the entry
    assert ('string tagCorner = input.string("Entry end", "Minimal: tag corner (a corner another tag holds moves '
            'it to a free corner of its zone)", options = ["Entry end", "Level end"], group = "Display")') in text
    assert lint(text) == []


def test_tag_corners_avoid_each_other_in_a_crowd() -> None:
    """Crowded trades at 1m and 1h: no two tags whose estimated text boxes overlap unless all four corners of
    the later one were taken (then it stays at its preferred corner); every tag still in its own zone."""
    text = render(*crowded())
    for bar in (M, 60 * M):
        ch = emulate(text, bar_ms=bar, line_h=0.0002)
        tw = lambda tag: (len(tag) + PAD_CHARS) * DEFAULT_CHAR_BARS * bar   # noqa: E731
        th = (1 + PAD_LINES) * 0.0002
        boxes = []
        for (c, x1, x2, top, bot, tag, va, fb), al in zip(ch.tags, ch.tag_align, strict=True):
            assert not fb and any(z[0] == c and z[2:] == (x1, x2, top, bot) for z in ch.zones)
            l0 = x2 - tw(tag) if al == "right" else x1
            hi0 = top if va == "top" else bot + th
            boxes.append((l0, l0 + tw(tag), hi0 - th, hi0))
        # 1m: every tag clear; 1h (24 trades within 3 bars at one price): the first four take the four free
        # spots (each side's entry end and level end), the rest have none left and stay at their entry end
        shown = boxes if bar == M else boxes[:4]
        assert not any(b[0] < a[1] and b[1] > a[0] and b[2] < a[3] and b[3] > a[2]
                       for i, a in enumerate(shown) for b in shown[:i])
        assert {t[6] for t in ch.tags} == {"top", "bottom"}


def test_change_dots_tooltip_only_by_default_text_on_input() -> None:
    text = render(*bracket_tp())
    det = emulate(text, mode="Detailed")
    assert [lb[4] for lb in det.labels] == [1, 2]
    with_text = emulate(text, mode="Detailed", adj_text=True)
    adj = [lb for lb in with_text.labels if lb[4] == 3]
    assert [(lb[0], lb[1], lb[5]) for lb in adj] == [(T0 + 2 * H, 1.078, "SL → 1.07800\n07 Nov 10:20")]
    assert with_text.dots == []
    call = next(x for x in label_calls(text) if "label.style_circle, size = size.tiny" in x)
    assert '""' in call and "tooltip = array.get(trHead, c)" in call


def test_pending_orders_are_faint_and_tooltip_only() -> None:
    trades, pending = book(*bracket_tp(), *pending_order(1),
                           order("W1", T0 + 9 * M, "WORKING", "1.0650", sl="1.0600", tp="1.0750"))
    text = render_symbol("EURUSD", trades, pending, INFO).text
    ch = emulate(text)
    assert ch.pending_lines == [(T0 + 10 * M, T0 + 15 * M, 1.07), (T0 + 9 * M, NOW, 1.065)]
    assert ch.pending == [(T0 + 15 * M, 1.07, "#P0001 BUY pending order\nplaced 07 Nov 08:30 @ 1.07000\n"
                                              "cancelled 07 Nov 08:35"),
                          (NOW, 1.065, "#W1 BUY pending order\nplaced 07 Nov 08:29 @ 1.06500\nSL 1.06000\n"
                                       "TP 1.07500\nstill working 12 Nov 08:20")]
    assert len(ch.tags) == 1 and len(ch.exits) == 1                        # pending orders have no tag or marker
    assert len(emulate(text, mode="Detailed").labels) == 2                 # no text label for pending orders
    assert "color = color.new(C_PENDING, 40), style = line.style_dashed, width = 1" in text
    assert "color = color.new(C_PENDING, 20)" in text and "size = size.tiny, tooltip = pendTip(k)" in text


# ------------------------------------------------------------------------------------------------ table
def test_table_one_row_per_trade_newest_first_capped() -> None:
    v = [x for i in range(40) for x in quick(i)]
    trades, pending = book(*v, *pending_order(3))
    res = render_symbol("EURUSD", trades, pending, INFO)
    arr = pine_arrays(res.text)
    assert len(arr["trNewest"]) == MAX_TABLE_ROWS and TABLE_HEADER_ROWS + MAX_TABLE_ROWS <= 40
    ch = emulate(res.text)
    assert len(ch.rows) == 12 and [r[0] for r in ch.rows[:3]] == ["T40", "T39", "T38"]
    # time only in the cells (the side is in the T# tooltip), the full date-time in each cell's tooltip
    assert ch.rows[0] == ("T40", "23:20 @ 1.10000", "23:50 @ -", "n/a (no exit price)")
    assert ch.row_tips[0] == "T40 BUY #Q0039\norders #Q0039"
    assert ch.entry_tips[0] == "entry 08 Nov 23:20 @ 1.10000"
    assert ch.exit_tips[0] == "exit 08 Nov 23:50 @ price not recorded\nclosed"
    assert len(emulate(res.text, rows_in=36).rows) == 36
    assert TABLE_COLUMNS == ("T#", "Entry", "Exit", "Result (R)") and TABLE_HEADER_ROWS == 3
    assert '"Result"' not in res.text and '"Side"' not in res.text and '"BUY" : "SELL"' not in res.text
    for i, h in enumerate(TABLE_COLUMNS):
        assert f'table.cell(tb, {i}, 2, "{h}", text_color = C_TEXT' in res.text
    assert "table.new(tblPos, 4, 3 + math.max(nRows, 1), " in res.text
    assert "table.merge_cells(tb, 0, 0, 3, 0)" in res.text and "table.merge_cells(tb, 0, 1, 3, 1)" in res.text
    assert 'input.string("bottom left", "Table position"' in res.text and "=> position.bottom_left" in res.text
    assert "right" not in " ".join(TABLE_POSITIONS) and not re.search(r"position\.\w*right", res.text)
    assert 'tooltip = array.get(trHead, c) + "\\n" + array.get(trOrders, c))' in res.text
    assert "tooltip = entryLine(c))" in res.text and "tooltip = exitText(c))" in res.text
    assert ('string cellFmt = input.string("HH:mm", "Table time format (entry / exit cells; the full date-time is '
            'in the cell tooltip)", group = "Table")') in res.text
    assert "str.format_time(t, cellFmt, tzName)" in res.text and "fmtC(array.get(trEntryT, c)) + at" in res.text
    rows = emulate(render(*bracket_tp(), *catch_up_trade(T0 + 9 * H, 1038153117))).rows
    assert rows == [("T2", "time n/r @ 1.10225", "17:20 @ 1.10234", "n/a (entry time n/r)"),
                    ("T1", "09:20 @ 1.08010", "11:20 @ 1.09000", "+16.50R")]


def test_table_header_is_title_and_window_only() -> None:
    """Row 0: 'XTNL trades EURUSD' (counts, time zone, symbol check in its tooltip); row 1: only the window; no
    grey symbol / time zone row; a red '(n left out)' only when something was left out."""
    v = [x for i in range(40) for x in quick(i)]
    trades, pending = book(*v, *pending_order(3))
    text = render_symbol("EURUSD", trades, pending, INFO).text
    title = next(x for x in text.split("\n") if "table.cell(tb, 0, 0, " in x)
    assert title.strip().startswith('table.cell(tb, 0, 0, "XTNL trades EURUSD", text_color = C_TEXT, ')
    assert ('tooltip = "read-only export, no amounts" + "\\n" + "trades 40 (drawn 40, left out 0)" + "\\n" + '
            '"pending orders 1 (drawn 1, left out 0)" + "\\ntime zone " + tzName + "\\nchart symbol " + chartSym + '
            '(not symbolOk ? " (mismatch)" : anySymbol ? " (any symbol on)" : " (match)"))') in title
    window = next(x for x in text.split("\n") if "table.cell(tb, 0, 1, " in x)
    assert re.search(r'table\.cell\(tb, 0, 1, "window UTC \d{4}-\d\d-\d\d \d\d:\d\d > \d{4}-\d\d-\d\d \d\d:\d\d", '
                     r"text_color = C_TEXT, ", window)
    assert "trades 40" not in window and "pending" not in window
    assert "chart symbol match" not in text and '"   time zone "' not in text
    # mismatch: the one body row is a red warning
    assert ('table.cell(tb, 0, 3, symbolOk ? "no trades in the window" : "chart symbol mismatch: " + chartSym, '
            "text_color = symbolOk ? C_TEXT : C_RISK, ") in text
    # something left out: the title says so, in red
    small = render_symbol("EURUSD", trades, pending, INFO, RenderLimits(max_labels=20)).text
    assert re.search(r'table\.cell\(tb, 0, 0, "XTNL trades EURUSD  \(\d+ left out\)", text_color = C_RISK, ', small)
    assert lint(text) == [] and lint(small) == []


def _table_cells(text: str, n_rows: int) -> tuple[int, int, set[tuple[int, int]], list[tuple[int, ...]]]:
    """The script's table, read from its own code for `n_rows` drawn trade rows: (columns, rows, the cells it sets,
    the merges)."""
    lines = text.split("\n")
    start = next(i for i, x in enumerate(lines) if x == "    if showTable")
    sec = lines[start:]
    m = re.search(r"table\.new\(tblPos, (\d+), (\d+) \+ math\.max\(nRows, 1\)", "\n".join(sec))
    assert m, "table.new"
    cols, hdr = int(m.group(1)), int(m.group(2))
    rows = hdr + max(n_rows, 1)
    cells: set[tuple[int, int]] = set()
    merges: list[tuple[int, ...]] = []
    branch = ""                                     # "" (both), "empty" (nRows == 0), "rows" (else)
    for x in sec:
        s = x.strip()
        if s == "if nRows == 0":
            branch = "empty"
        elif s == "else" and branch == "empty":
            branch = "rows"
        if branch == "empty" and n_rows > 0 or branch == "rows" and n_rows == 0:
            continue
        if c := re.match(r"table\.cell\(tb, (\d+), (\d+|row), ", s):
            col = int(c.group(1))
            targets = [hdr + r for r in range(n_rows)] if c.group(2) == "row" else [int(c.group(2))]
            assert c.group(2) != "row" or "int row = " + str(hdr) + " + r" in text
            cells |= {(col, r) for r in targets}
        elif mg := re.match(r"table\.merge_cells\(tb, (\d+), (\d+), (\d+), (\d+)\)", s):
            merges.append(tuple(int(v) for v in mg.groups()))
    return cols, rows, cells, merges


@pytest.mark.parametrize("n_rows", [0, 1, 5, MAX_TABLE_ROWS])
def test_table_cells_fill_the_table_exactly(n_rows: int) -> None:
    """Every table.cell / merge_cells index is inside table.new's size, no two merges overlap, and every cell of
    the table is set or merged over (no gaps): header rows, then one row per trade (or the one warning / empty
    row)."""
    text = render(*bracket_tp(), *catch_up_trade(T0 + 9 * H, 1038153117))
    cols, rows, cells, merges = _table_cells(text, n_rows)
    assert cols == len(TABLE_COLUMNS) and rows == TABLE_HEADER_ROWS + max(n_rows, 1)
    assert all(0 <= c < cols and 0 <= r < rows for c, r in cells), sorted(cells)
    covered: set[tuple[int, int]] = set()
    for c1, r1, c2, r2 in merges:
        assert 0 <= c1 <= c2 < cols and 0 <= r1 <= r2 < rows and (c1, r1) in cells
        span = {(c, r) for c in range(c1, c2 + 1) for r in range(r1, r2 + 1)}
        assert not span & covered, "overlapping merges"
        covered |= span
    assert cells | covered == {(c, r) for c in range(cols) for r in range(rows)}
    assert not (cells - {(m[0], m[1]) for m in merges}) & covered        # no cell set under a merge


def test_table_cells_are_time_only_and_tooltips_carry_the_date() -> None:
    """The cell format has no date part (a valid str.format_time pattern); the cell text uses it, the tooltips
    (entryLine / exitText) use the full Time format."""
    from src.reports.pinescript import DEFAULT_CELL_TIME_FMT, DEFAULT_TIME_FMT
    assert re.fullmatch(r"[Hhmsa: ]+", DEFAULT_CELL_TIME_FMT) and "HH" in DEFAULT_CELL_TIME_FMT
    assert {"d", "M"} <= set(DEFAULT_TIME_FMT)
    text = render(*bracket_tp(), *catch_up_trade(T0 + 9 * H, 1038153117))

    def body(name: str) -> str:
        lines = text.split("\n")
        i = next(j for j, x in enumerate(lines) if x.startswith(name + "("))
        out = []
        for x in lines[i + 1:]:
            if not x.startswith("    "):
                break
            out.append(x)
        return "\n".join(out)

    for cell in ("entryCell", "exitCell"):
        assert "fmtC(" in body(cell) and "fmtT(" not in body(cell), cell
    for tip in ("entryLine", "exitText"):
        assert "fmtT(" in body(tip) and "fmtC(" not in body(tip), tip
    assert "str.format_time(t, timeFmt, tzName)" in body("fmtT")
    assert "str.format_time(t, cellFmt, tzName)" in body("fmtC")


def test_text_is_light_and_side_colours_are_for_lines_and_markers() -> None:
    """Text (table, Minimal tags, exit numbers, Detailed entry / exit labels) is C_TEXT (#E6E6E6); BUY / SELL
    colours (light blue / light violet, neither red nor green nor orange) only colour lines and markers."""
    text = render(*bracket_tp(), *catch_up_trade(T0 + 9 * H, 1038153117))
    assert 'color C_TEXT = input.color(#E6E6E6, "Text colour' in text
    assert 'color C_BUY = input.color(#82B1FF, "BUY entry line and marker"' in text
    assert 'color C_SELL = input.color(#B388FF, "SELL entry line and marker"' in text
    for gone in ("C_BUY_TXT", "C_SELL_TXT", "sideTxt", "#FF9800", "#FFB74D", "#2962FF"):
        assert gone not in text, gone
    assert {COLOURS["C_SELL"][0], COLOURS["C_BUY"][0]}.isdisjoint({COLOURS["C_RISK"][0], COLOURS["C_REWARD"][0]})
    # the Result (R) cell alone takes its colour from the result (resCol: C_REWARD / C_RISK / C_TEXT)
    for call in [x for x in text.split("\n") if "table.cell(" in x and "C_RISK" not in x and "resCell(" not in x]:
        assert "text_color = C_TEXT" in call, call
    assert sum("text_color = resCol(c)" in x for x in text.split("\n")) == 1
    exit_mark = next(x for x in label_calls(text) if "label.style_xcross" in x and "exitColor" in x)
    assert "textcolor = C_TEXT" in exit_mark and "color = exitColor(k)" in exit_mark
    assert "color fg = kd == 3 ? evColor(array.get(evCode, rf)) : C_TEXT" in text
    assert text.count("text_color = C_TEXT, text_halign = tagH") == 1 and "text_color = C_TEXT, text_halign = " \
        "text.align_left, text_valign = text.align_top)" in text
    assert "color = sideLine(sd), style = sd == 1 ? label.style_triangleup" in text
    assert "color = sideLine(array.get(trSide, c)), style = line.style_solid, width = 2" in text


# ------------------------------------------------------------------------------------------------ Detailed
def test_detailed_entry_label_left_of_the_zone_exit_label_right_of_it() -> None:
    ch = emulate(render(*bracket_tp()), mode="Detailed")
    entry, exit_ = ch.labels
    assert entry[:5] == (T0 + H, 1.0801, -1, -1, 1)                   # left edge, box left of it; BUY pushes down
    assert entry[5] == "T1 BUY #O1\nentry 07 Nov 09:20 @ 1.08010"
    assert exit_[:5] == (T0 + 3 * H, 1.09, 1, 1, 2)                  # right edge, box right of it
    assert exit_[5] == "exit 07 Nov 11:20 @ 1.09000\nTP hit"
    tip = entry[6]
    assert tip.startswith("T1 BUY #O1\norders #O1 #S1 #S2 #TP1\nplaced 07 Nov 08:20 @ 1.08000\n")
    assert "SL moved 07 Nov 10:20 @ 1.07800" in tip and tip.endswith("closed 07 Nov 11:20 @ 1.09000 better\nTP hit")
    text = render(*bracket_tp())
    call = next(x for x in label_calls(text) if "label.new(array.get(lbX, i)" in x)
    assert "style = mode > 0 ? label.style_label_left : label.style_label_right" in call
    assert "color = C_BG" in call and "textcolor = fg" in call and "textcolor = color.white" not in text
    # the markers are drawn in Detailed mode too
    assert len(ch.entries) == 1 and len(ch.exits) == 1


def test_trade_without_time_span_gets_the_minimum_width_ending_at_its_exit() -> None:
    text = render(*catch_up_trade(T0, 1038153117))
    for bar in (60_000, 300_000, 3_600_000):
        ch = emulate(text, bar_ms=bar)
        assert ch.spans == [(T0 - DEFAULT_MIN_BARS * bar, T0)]
        lo, hi = ch.spans[0]
        assert sorted(ch.zones) == [(0, LEG_SL, lo, hi, 1.10234, 1.10225), (0, LEG_TP, lo, hi, 1.10225, 1.09764)]
        assert [m[:4] for m in ch.exits] == [(0, T0, 1.10234, EXIT_SL)] and ch.entry_lines == [(0, lo, hi, 1.10225)]
        assert ch.tags[0][1:3] == (lo, hi)
    entry, exit_ = emulate(text, mode="Detailed").labels
    assert entry[5] == "T1 SELL #1038153117\nentry time not recorded @ 1.10225"
    assert exit_[5] == "exit 07 Nov 08:20 @ 1.10234\nSL hit"
    assert "orders #1038153117 #1038153118 #1038153119" in entry[6]
    # the hover never claims a fill time the log does not hold (the fill row carries the close time)
    assert "filled" not in entry[6] and "closed 07 Nov 08:20 @ 1.10234" in entry[6]


def test_short_trade_is_widened_to_the_right_with_an_exit_segment() -> None:
    v = [order("Z1", T0, "FILLED", "1.1000"), position("Z1", T0, "OPEN", "1.1000"),
         child("Z1-C", T0 + 2 * M, "FILLED", "1.1010", linked="Z1"), position("Z1", T0 + 2 * M, "CLOSED", "1.1000")]
    ch = emulate(render(*v), bar_ms=3_600_000)
    assert ch.spans == [(T0, T0 + DEFAULT_MIN_BARS * 3_600_000)]
    assert [m[:4] for m in ch.exits] == [(0, T0 + 2 * M, 1.101, EXIT_CLOSED)]      # on the exit, not the edge
    assert ch.exit_segments == [(0, T0 + 2 * M, T0 + DEFAULT_MIN_BARS * 3_600_000, 1.101)]
    assert emulate(render(*v), mode="Detailed").labels[1][5] == "exit 07 Nov 08:22 @ 1.10100\nclosed"


def test_first_seen_open_and_opened_before_window() -> None:
    v = [position("P3", T0, "OPEN", "1.2500", sl="1.2400", tp="1.2700")]
    trades, _ = book(*v)
    assert trade_elements(trades[0], T0 - 1).entry_code == ENTRY_SEEN
    assert trade_elements(trades[0], T0 + 1).entry_code == ENTRY_BEFORE_WINDOW
    late = RenderInfo(generated_ms=NOW, since_ms=T0 + H, until_ms=NOW)
    entry = emulate(render(*v, info=late), mode="Detailed").labels[0]
    assert entry[5] == "T1 BUY #P3\nfirst seen open 07 Nov 08:20 @ 1.25000\nopened before window"
    assert emulate(render(*v), mode="Detailed").labels[0][5] == "T1 BUY #P3\nfirst seen open 07 Nov 08:20 @ 1.25000"
    assert emulate(render(*v)).entries[0][4].startswith("T1 BUY #P3 · first seen open 07 Nov 08:20 @ 1.25000\n")


def test_counts_lines_boxes_labels() -> None:
    trades, pending = book(*bracket_tp())
    res = render_symbol("EURUSD", trades, pending, INFO)
    # boxes: 3 steps (the tag rides on the reward zone); lines: entry + exit segment + 3 levels + working;
    # labels: entry and exit text (Detailed), entry and exit markers, 1 dot
    assert (res.boxes, res.lines, res.labels) == (3, 6, 5)
    te = trade_elements(trades[0], INFO.since_ms)
    assert (te.boxes, te.lines, te.labels, te.tag_step) == (3, 6, 5, 2)


# ------------------------------------------------------------------------------------------------ layout
def _extents(boxes: list[TextBox], ys: list[float | None], line_h: float, char_ms: float
             ) -> list[tuple[float, float, float, float]]:
    out = []
    for b, y in zip(boxes, ys):
        if y is None:
            continue
        w = (b.chars + PAD_CHARS) * char_ms
        h = (b.rows + PAD_LINES) * line_h
        cx = b.x + b.mode * w / 2
        out.append((cx - w / 2, cx + w / 2, y - h / 2, y + h / 2))
    return out


def _overlaps(ext: list[tuple[float, float, float, float]]) -> list[tuple[int, int]]:
    return [(i, j) for i, a in enumerate(ext) for j, b in enumerate(ext[i + 1:], i + 1)
            if a[0] < b[1] and b[0] < a[1] and a[2] < b[3] and b[2] < a[3]]


def _boxes(ch: Chart) -> list[TextBox]:
    return [TextBox(x, y, up, len(t.split("\n")), max(len(r) for r in t.split("\n")), mode)
            for x, y, up, mode, _, t, _ in ch.labels]


def test_layout_moves_a_label_past_the_ones_in_its_way() -> None:
    line_h, char_ms = 0.0001, 60_000.0
    boxes = [TextBox(T0, 1.0830, 1, 2, 30, -1), TextBox(T0 + M, 1.0831, 1, 2, 30, -1),
             TextBox(T0 + 2 * M, 1.0829, -1, 2, 30, 1), TextBox(T0 + 5 * H, 1.0830, 1, 2, 30, 1),
             TextBox(T0 + 5 * H + M, 1.0950, 1, 2, 14, 1)]
    ys = layout(boxes, line_h, char_ms)
    h = (2 + PAD_LINES) * line_h
    assert ys[0] == 1.0830                                             # the first keeps its own price
    assert ys[1] == pytest.approx(1.0830 + h + GAP_LINES * line_h)     # moved just past the first, upwards
    assert ys[2] == 1.0829                                             # right of its time: nothing in its way
    assert ys[3] == 1.0830 and ys[4] == 1.0950                         # far in time / in price: own price
    assert _overlaps(_extents(boxes, ys, line_h, char_ms)) == []
    with pytest.raises(ValueError):
        layout(list(reversed(boxes)), line_h, char_ms)


def test_detailed_displacement_is_capped_then_tooltip_only() -> None:
    """A tower of labels on one spot: each moves past the ones below it until it would move more than
    MAX_SHIFT_HEIGHTS of its own heights; from then on a label is not drawn (and blocks nothing)."""
    line_h, char_ms = 0.0001, 60_000.0
    boxes = [TextBox(T0 + i * M, 1.0830, 1, 2, 30, 1) for i in range(10)]
    ys = layout(boxes, line_h, char_ms)
    h = (2 + PAD_LINES) * line_h
    step = h + GAP_LINES * line_h
    kept = [y for y in ys if y is not None]
    # 2 steps (2 x 2.85 lines) stay within 3 heights (3 x 2.6 lines); a third step would not
    assert ys[:3] == pytest.approx([1.0830 + k * step for k in range(3)]) and ys[3:] == [None] * 7
    assert MAX_SHIFT_HEIGHTS == 3 and all(abs(y - 1.0830) <= MAX_SHIFT_HEIGHTS * h for y in kept)
    assert _overlaps(_extents(boxes, ys, line_h, char_ms)) == []
    # a dropped label leaves no box behind: one far enough away in price is placed on its own price
    assert layout(boxes + [TextBox(T0 + 20 * M, 1.0830 + 5 * step, 1, 2, 30, 1)], line_h, char_ms)[-1] == \
        pytest.approx(1.0830 + 5 * step)
    # without a cap the same tower is drawn whole
    assert None not in layout(boxes, line_h, char_ms, max_shift=1e9)
    text = render(*crowded())
    for line in (f"if math.abs(yy - y0) <= {MAX_SHIFT_HEIGHTS} * h", "float yy = (lo + hi) / 2",
                 "float y0 = array.get(lbY, i)"):
        assert line in text, line
    # the label call is inside the cap's block (indented under it)
    cap_at = text.index(f"if math.abs(yy - y0) <= {MAX_SHIFT_HEIGHTS} * h")
    assert text.index("                    label.new(array.get(lbX, i), yy, txt", cap_at) > cap_at


def test_layout_tower_and_budget() -> None:
    line_h, char_ms = 0.0001, 60_000.0
    boxes = [TextBox(T0 + i * M, 1.0830 + (i % 3) * 0.00002, 1 if i % 2 else -1, 1 + i % 3, 14 + i % 20,
                     (-1, 1, 0)[i % 3]) for i in range(300)]
    ys = layout(boxes, line_h, char_ms)
    assert _overlaps(_extents(boxes, ys, line_h, char_ms)) == []
    for b, y in zip(boxes, ys):
        assert y is None or abs(y - b.anchor) <= MAX_SHIFT_HEIGHTS * (b.rows + PAD_LINES) * line_h + 1e-12
    capped = layout(boxes, line_h, char_ms, budget=50)
    assert capped[:10] == ys[:10] and capped[-1] == boxes[-1].anchor


@pytest.mark.parametrize("line_h,bar_ms", [(0.00008, 60_000), (0.00018, 300_000), (0.0003, 900_000),
                                           (0.0012, 3_600_000), (0.00002, 60_000)])
def test_detailed_labels_never_overlap_on_any_timeframe(line_h: float, bar_ms: int) -> None:
    for versions in (crowded(), bracket_tp() + catch_up_trade(T0 + 3 * H, 1038153117)):
        text = render(*versions)
        for adj in (False, True):
            ch = emulate(text, mode="Detailed", bar_ms=bar_ms, adj_text=adj)
            boxes = _boxes(ch)
            char_ms = DEFAULT_CHAR_BARS * bar_ms
            ys = layout(boxes, line_h, char_ms)
            assert _overlaps(_extents(boxes, ys, line_h, char_ms)) == []
            assert ys[0] == boxes[0].anchor
            # entry labels sit at their trade's left edge, exit labels at its right edge
            for x, _, _, mode, kd, _, _ in ch.labels:
                if kd in (1, 2):
                    assert mode == (-1 if kd == 1 else 1)
                    assert any(x == (span[0] if kd == 1 else span[1]) for span in ch.spans)


def test_script_runs_the_same_layout() -> None:
    text = render(*crowded())
    for line in ("float lineH = spacingTicks > 0 ? spacingTicks * syminfo.mintick : nz(atrNow, 20 * syminfo.mintick)",
                 "float charMs = charBars * barMs", f"float gap = {GAP_LINES} * lineH",
                 'string[] rows = str.split(txt, "\\n")', f"float w = (nChars + {PAD_CHARS}) * charMs",
                 f"float h = (array.size(rows) + {PAD_LINES}) * lineH", "float cx = x + mode * w / 2",
                 "float lo = y0 - h / 2", "float hi = y0 + h / 2", f"int budget = {LAYOUT_BUDGET}",
                 "while j >= 0 and budget > 0", "if x - array.get(pX, j) >= w + wMax", "budget -= 1",
                 "if math.abs(cx - array.get(pC, j)) * 2 < w + array.get(pW, j)",
                 "int[] idx = up > 0 ? array.sort_indices(nLo, order.ascending) : "
                 "array.sort_indices(nHi, order.descending)",
                 "lo := array.get(nHi, q) + gap", "hi := array.get(nLo, q) - gap", "float yy = (lo + hi) / 2",
                 "int[] lbOrd = array.sort_indices(lbKey, order.ascending)",
                 "array.push(lbKey, array.get(trL, c) * 1000.0 + array.size(lbKind))"):
        assert line in text, line
    assert lint(text) == []


# ------------------------------------------------------------------------------------------------ limits
def test_limits_keep_the_newest_trades_and_count_boxes() -> None:
    v = [x for i in range(300) for x in quick(i)]
    trades, pending = book(*v, *[x for i in range(3) for x in pending_order(i)])
    per = trade_elements(trades[0], INFO.since_ms)
    assert (per.lines, per.boxes, per.labels) == (3, 2, 4)
    res = render_symbol("EURUSD", trades, pending, INFO)
    assert res.lines <= MAX_LINES and res.labels <= MAX_LABELS and res.boxes <= MAX_BOXES
    kept = MAX_LABELS // per.labels                                            # labels bind here (4 a trade)
    assert len(res.drawn) == kept and res.left_out == 300 - kept
    assert set(res.drawn) == {t.key for t in trades[:kept]}                    # trades come newest first
    assert res.drawn_pending == () and res.left_out_pending == 3              # no label left for them
    assert f"left out (TradingView drawing limits, oldest first): {300 - kept + 3}" in res.text
    assert f"trades 300 (drawn {kept}, left out {300 - kept})" in res.text and "text_color = C_RISK" in res.text
    # pending orders get what is left (2 labels here): the two newest, the oldest is left out
    more = render_symbol("EURUSD", trades, pending, INFO, RenderLimits(max_labels=MAX_LABELS + 2))
    assert len(more.drawn) == kept and set(more.drawn_pending) == {p.key for p in pending[:2]}
    assert more.left_out_pending == 1
    assert lint(res.text) == []
    # boxes bind when they are the scarcer budget
    tight = render_symbol("EURUSD", trades, pending, INFO, RenderLimits(max_boxes=20))
    assert tight.boxes == 20 and len(tight.drawn) == 10
    # the source-size budget drops pending orders first, then the oldest kept trades
    small = render_symbol("EURUSD", trades, pending, INFO, RenderLimits(max_chars=40_000))
    assert len(small.text) <= 40_000 and 0 < len(small.drawn) < kept
    assert set(small.drawn) == {t.key for t in trades[:len(small.drawn)]}


def test_pending_fill_the_budget_left_by_trades() -> None:
    trades, pending = book(*bracket_tp(), *[x for i in range(10) for x in pending_order(i)])
    res = render_symbol("EURUSD", trades, pending, INFO, RenderLimits(max_labels=10))
    assert len(res.drawn) == 1 and len(res.drawn_pending) == 5 and res.left_out_pending == 5
    assert set(res.drawn_pending) == {p.key for p in pending[:5]}            # the newest pending orders


def test_long_stop_leg_is_trimmed() -> None:
    trades, _ = book(*bracket_tp())
    t = trades[0]
    steps = tuple(Step(T0 + H + i * 10, T0 + H + i * 10 + 10, Decimal(i + 1)) for i in range(150))
    moves = tuple(LcEvent(T0 + H + i * 10 + 5, "SL_MOVED", Decimal(i + 1)) for i in range(150))
    t = Trade(**{**t.__dict__, "stop_loss": steps, "events": t.events + moves})
    te = trade_elements(t, INFO.since_ms, 100)
    sl = [s for s in te.steps if s[0] == LEG_SL]
    assert te.trimmed == 50 and len(sl) == 100 and sl[0][1] == T0 + H + 500       # the newest steps are kept
    assert len([i for i in te.adjust if t.events[i].kind.startswith("SL")]) == 100


def test_deterministic() -> None:
    v = bracket_tp() + crowded() + catch_up_trade(T0 + 9 * H, 1038153117)
    a = render(*v)
    shuffled = list(v)
    random.Random(3).shuffle(shuffled)
    assert render(*shuffled) == a
    trades, pending = book(*v)
    assert render_symbol("EURUSD", list(reversed(trades)), list(reversed(pending)), INFO).text == a


def test_escaping() -> None:
    assert pine_str('a"b\\c\nd') == '"a\\"b\\\\c d"'
    assert safe_id('O"1\\x\ny') == "O_1_x_y" and len(safe_id("X" * 80)) == 32
    assert pine_float(Decimal("1.08500")) == "1.085" and pine_float(Decimal("15000")) == "15000.0"
    assert pine_float(Decimal("1E-7")) == "0.0000001"
    trades, pending = book(*bracket_tp())
    evil = Trade(**{**trades[0].__dict__, "order_id": 'evil") label.delete(x', "key": "stonex:evil"})
    text = render_symbol("EURUSD", [evil], pending, INFO).text
    assert 'evil") label' not in text and not lint(text)


def test_lint_style_check() -> None:
    for text in (render(*bracket_tp()), render_symbol("USDJPY", [], [], INFO).text, render(*crowded()),
                 render(*[x for i in range(300) for x in quick(i)])):
        assert lint(text) == []
    assert lint("//@version=6\nfoo.bar(1)\n") == ["2: function foo.bar not in the allow-list"]
    assert lint("//@version=6\nx = (1\n")[0].endswith("unbalanced brackets")
    assert lint("//@version=6\n\tx = 1\n")[0] == "2: tab"
    assert lint("//@version=6\ncolor c = #FFB74D\n") == []
    assert lint("//@version=6\nf(int a) =>\n    a\nint b = f(1)\n") == []
    assert lint("//@version=6\nint a = 1\nif b\n    a := a + 1\n") == ["3: identifier b is not declared"]


def test_empty_symbol_renders_valid_script() -> None:
    res = render_symbol("USDJPY", [], [], INFO)
    assert "var int[] trEntryT = array.new_int(0)" in res.text and "if barstate.isfirst" not in res.text
    assert res.lines == res.labels == res.boxes == res.left_out == 0
    assert '"no trades in the window"' in res.text


def test_symbol_guard() -> None:
    trades, pending = book(*bracket_tp())
    text = render_symbol("EUR/USD", trades, pending, INFO).text
    assert 'const string DATA_SYMBOL = "EURUSD"' in text
    assert "normSym(str.length(symOverride) > 0 ? symOverride : syminfo.ticker)" in text
    assert "str.startswith(chartSym, DATA_SYMBOL)" in text and "anySymbol or" in text
    assert "    if symbolOk" in text and 'input.string("", "Chart symbol override' in text


# ------------------------------------------------------------------------------------------------ privacy
UNITS = Decimal("123457")
REMAINING = Decimal("98765.4321")
SWAP = Decimal("-3.217")
COMMISSION = Decimal("6.543")
FORBIDDEN = ("123457", "98765", "4321", "3.217", "6.543")
FORBIDDEN_WORDS = re.compile(r"(?i)\b(units?|qty|quantity|lots?|size_|balance|equity|margin|pnl|p&l|profit_|loss_|"
                             r"swap|commission|spread|pips?|points?|amount|r-multiple)\b")


def privacy_rows() -> list[tuple[object, ...]]:
    base = dict(account_id="ACC", adapter_id="stonex", symbol="EURUSD", side="BUY", source="STREAM")
    o1 = OrderDTO(client_order_id="P1", broker_order_id="P1", status=OrderStatus.WORKING, entry_price=d("1.0811"),
                  ordered_units=UNITS, remaining_units=UNITS, raw_qty=UNITS, broker_updated_at_ms=T0, observed_seq=1,
                  seq_id=(T0 << 20) + 1, **base)  # type: ignore[arg-type]
    o2 = OrderDTO(client_order_id="P1", broker_order_id="P1", status=OrderStatus.PARTIALLY_FILLED,
                  entry_price=d("1.0811"), ordered_units=UNITS, remaining_units=REMAINING, raw_qty=UNITS,
                  broker_updated_at_ms=T0 + H, observed_seq=2, seq_id=((T0 + H) << 20) + 2, **base)  # type: ignore[arg-type]
    p1 = PositionDTO(position_id="P1", units=UNITS - REMAINING, entry_price=d("1.0812"), status="OPEN",
                     raw_qty=UNITS, accrued_swap=SWAP, commission=COMMISSION, opened_by_client_order_id="P1",
                     stop_loss=d("1.0761"), take_profit=d("1.0911"),
                     broker_updated_at_ms=T0 + H, observed_seq=3, seq_id=((T0 + H) << 20) + 3, **base)  # type: ignore[arg-type]
    return [log_row(x, T0 + 2 * H) for x in (o1, o2, p1)]


def test_privacy_no_size_or_amount_in_output() -> None:
    versions = tuple(v for v in (version_from_row(r) for r in privacy_rows()) if v is not None)
    assert len(versions) == 3
    read = LogRead(Window(NOW - 7 * 24 * H, NOW), versions, 3, 0, 0, False)
    plan = plan_from_read(read, now_ms=NOW)
    assert [e.file_name for e in plan.exports] == [file_name("EURUSD", NOW)]
    text = plan.exports[0].result.text
    assert "1.0812" in text and "1.0761" in text and "1.0911" in text      # entry, SL and TP levels are drawn
    for n in FORBIDDEN:
        assert n not in text, n
    # the pip unit is named only in the Result (R) inputs' titles ('1R minimum (pips)', 'Pip size override')
    assert not FORBIDDEN_WORDS.search("\n".join(x for x in text.split("\n") if 'group = "Result (R)")' not in x))
    assert [m.group(0).lower() for m in FORBIDDEN_WORDS.finditer(text)] == ["pips", "pips", "pip"]
    summary = "\n".join(summary_lines(plan, [__import__("pathlib").Path("x.pine")]))
    for n in FORBIDDEN:
        assert n not in summary
    # what the chart shows: ids, trade numbers, sides, times, event names and price levels only
    for mode in ("Minimal", "Detailed"):
        ch = emulate(text, mode=mode, adj_text=True)
        shown = "\n".join([lb[5] + "\n" + lb[6] for lb in ch.labels] + ["|".join(r) for r in ch.rows]
                          + [t[5] for t in ch.tags] + [m[4] for m in ch.entries]
                          + [m[4] + "\n" + m[5] for m in ch.exits] + ch.row_tips + ch.result_tips
                          + ch.entry_tips + ch.exit_tips)
        assert shown and not FORBIDDEN_WORDS.search(shown)
        for n in FORBIDDEN:
            assert n not in shown, n
        # the Result (R) cell: a signed R multiple, 'open' or a reason, never money, a pip count or the pip size
        assert all(re.fullmatch(r"[+-]?\d+\.\d\dR|open|n/a \([a-z0-9 />]+\)", r[3]) for r in ch.rows)
        assert all(re.fullmatch(r"(result [+-]?\d+\.\d\dR; 1R from \w+ ATR\(\d+\), clamped [\d.]+-[\d.]+|open: no "
                                r"result until closed|n/a \([a-z0-9 />]+\)); exit: "
                                r"(SL hit|TP hit|closed|still open|position not recorded)(\n.*)*", tip)
                   for tip in ch.result_tips)


def test_read_sql_never_selects_sizes_or_amounts() -> None:
    for name in ("SQL_LOG_VERSIONS", "SQL_CURRENT_ORDERS", "SQL_CURRENT_POSITIONS"):
        sql = getattr(lifecycle_mod, name)
        assert sql.lstrip().upper().startswith("SELECT")
        for col in ("units", "raw_qty", "accrued_swap", "commission", "balance", "amount", "equity", "margin",
                    "pnl", "spread"):
            assert col not in sql, (name, col)


@pytest.mark.parametrize("sym,expected", [("EUR/USD", "EURUSD"), ("eurusd.a", "EURUSDA"), ("", "UNKNOWN")])
def test_file_names(sym: str, expected: str) -> None:
    assert file_name(sym, NOW) == f"{expected}_2026-11-11.pine"
