"""Result (R) table column: (exit - entry) x direction / 1R, where 1R is a price distance set from the ATR of the
ATR-timeframe bar closed before the entry, counted in the symbol's pip (tick x 10 with 3 / 5 price decimals, else the
tick; an input overrides it), rounded to the nearest 0.5 with halves up and clamped to [6, 14]; read without
repainting (a global-scope request.security with lookahead_on and [1]) and taken per trade in one pointer pass; the
open / n/a branches; the header; lint; privacy (no pip count, no pip size, no money)."""
from __future__ import annotations

import dataclasses
import re
from decimal import Decimal

import pytest

from src.reports.lifecycle import Trade
from src.reports.pinescript import (ATR_BEFORE_HISTORY, ATR_CAPTURED, ATR_NOT_REACHED, DEFAULT_ATR_LEN,
                                    DEFAULT_ATR_TF, DEFAULT_R_MAX, DEFAULT_R_MIN, NA_BEFORE_HISTORY, NA_ENTRY_PRICE,
                                    NA_ENTRY_TIME, NA_EXIT_PRICE, NA_NO_ATR, NA_NOT_REACHED, RES_OPEN, RES_OPEN_TIP,
                                    TABLE_COLUMNS, one_r_pips, pip_size, render_symbol, result_r, result_text,
                                    round_half_up)

from tests.reports.builders import H, PINE_RESERVED, T0, Bar, capture_atr, emulate, lint, pine_arrays, tf_text
from tests.reports.test_pinescript import FORBIDDEN_WORDS, INFO, bare, book, quick, render
from tests.reports.test_trades import bracket_tp, catch_up_trade

M = 60_000
Q = 15 * M                                   # the default ATR timeframe, 15 minutes
B = (T0 // Q) * Q                            # a 15-minute boundary near T0
EURUSD_PIP = pip_size(100_000, 0.00001)
#: The only script lines allowed to name the pip unit: the three inputs' titles (never a count or the size itself).
PIP_INPUT_LINES = ('float rMin = input.float(6.0, "1R minimum (pips)", minval = 0.5, step = 0.5, '
                   'group = "Result (R)")',
                   'float rMax = input.float(14.0, "1R maximum (pips)", minval = 0.5, step = 0.5, '
                   'group = "Result (R)")',
                   'float pipOver = input.float(0.0, "Pip size override (0 = auto)", minval = 0, step = 0.00001, '
                   'group = "Result (R)")')
TIP = re.compile(r"(result [+-]?\d+\.\d\dR; 1R from \w+ ATR\(\d+\), clamped [\d.]+-[\d.]+"
                 r"|open: no result until closed|n/a \([a-z0-9 />]+\)); exit: "
                 r"(SL hit|TP hit|closed|still open|position not recorded)(\nentry time = first seen open)?")


# ------------------------------------------------------------------------------------------------ the symbol's pip
@pytest.mark.parametrize("pricescale,mintick,pip", [
    (100_000, 0.00001, 0.0001),       # 5 digits: EURUSD fractional pips
    (1_000, 0.001, 0.01),             # 3 digits: USDJPY fractional pips
    (100, 0.01, 0.01),                # 2 digits: the tick is the pip
    (10_000, 0.0001, 0.0001),         # 4 digits: the tick is the pip
    (10, 0.1, 0.1),                   # 1 digit
])
def test_pip_from_the_symbol(pricescale: int, mintick: float, pip: float) -> None:
    assert pip_size(pricescale, mintick) == pytest.approx(pip, rel=1e-12)


def test_pip_override_wins() -> None:
    assert pip_size(100_000, 0.00001, 0.00005) == 0.00005
    assert pip_size(100, 0.01, 0.1) == 0.1
    assert pip_size(1_000, 0.001, 0.0) == pytest.approx(0.01)              # 0 = auto


def test_pine_derives_the_pip_the_same_way() -> None:
    lines = render(*bracket_tp()).split("\n")
    assert "int symDigits = math.round(math.log10(syminfo.pricescale))" in lines
    assert ("float pipSz = pipOver > 0 ? pipOver : symDigits == 3 or symDigits == 5 ? syminfo.mintick * 10 : "
            "syminfo.mintick") in lines
    assert PIP_INPUT_LINES[2] in lines


# ------------------------------------------------------------------------------------------------ 1R rounding, clamp
@pytest.mark.parametrize("raw,rounded", [(4.2, 4.0), (4.3, 4.5), (6.6, 6.5), (6.8, 7.0), (4.25, 4.5), (6.75, 7.0),
                                         (4.0, 4.0), (4.74, 4.5), (4.75, 5.0), (0.2, 0.0)])
def test_round_half_up_to_the_nearest_half(raw: float, rounded: float) -> None:
    assert round_half_up(raw) == rounded


@pytest.mark.parametrize("atr,one_r", [(0.00042, 6.0), (0.00043, 6.0), (0.00059, 6.0), (0.0006, 6.0),
                                       (0.00062, 6.0), (0.00063, 6.5), (0.00066, 6.5), (0.00068, 7.0),
                                       (0.00102, 10.0), (0.00138, 14.0), (0.00142, 14.0), (0.00143, 14.0),
                                       (0.004, 14.0)])
def test_one_r_rounds_the_atr_in_pips_then_clamps_to_6_14(atr: float, one_r: float) -> None:
    assert (DEFAULT_R_MIN, DEFAULT_R_MAX) == (6.0, 14.0)
    assert one_r_pips(atr, EURUSD_PIP) == one_r


def test_exact_halves_round_up_despite_float_error() -> None:
    """7.25 pips is 0.000725 / 0.0001 = 7.249999999999999 in floats, 0.75 pip over 6 pips is 12.4999999 hundredths:
    the halves-up rule still gives 7.5 and +0.13R (and -0.12R on the losing side: halves go up, towards +)."""
    assert 0.000725 / EURUSD_PIP < 7.25
    assert one_r_pips(0.000725, EURUSD_PIP) == 7.5 and one_r_pips(0.001025, EURUSD_PIP) == 10.5
    assert result_text(result_r(1.10000, 1.100075, "BUY", 0.0004, EURUSD_PIP)) == "+0.13R"
    assert result_text(result_r(1.10000, 1.100075, "SELL", 0.0004, EURUSD_PIP)) == "-0.12R"
    # on an 8-pip 1R: 2 ticks (0.2 pip) are 2.5 hundredths -> 0.03R, 1 tick is 1.25 hundredths -> 0.01R
    assert result_text(result_r(1.10000, 1.10002, "BUY", 0.0008, EURUSD_PIP)) == "+0.03R"
    assert result_text(result_r(1.10000, 1.10001, "BUY", 0.0008, EURUSD_PIP)) == "+0.01R"
    # the epsilon never turns a clear value into the next step
    assert round_half_up(7.2499) == 7.0
    assert result_r(1.10000, 1.1000749, "BUY", 0.0004, EURUSD_PIP) == 0.12      # 0.749 / 6 = 12.48 hundredths


def test_one_r_bounds_are_inputs_and_jpy_uses_its_own_pip() -> None:
    assert one_r_pips(0.00043, EURUSD_PIP, 4.0, 10.0) == 4.5
    assert one_r_pips(0.0003, EURUSD_PIP, 4.0, 10.0) == 4.0 and one_r_pips(0.0012, EURUSD_PIP, 4.0, 10.0) == 10.0
    jpy = pip_size(1_000, 0.001)
    assert one_r_pips(0.068, jpy) == 7.0 and one_r_pips(0.043, jpy) == 6.0 and one_r_pips(0.2, jpy) == 14.0


def test_pine_uses_the_same_rule() -> None:
    text = render(*bracket_tp())
    assert ("    math.max(rMin, math.min(rMax, math.floor(array.get(trAtr, c) / pipSz * 2 + 0.5 + 0.000001) / 2.0)) * "
            "pipSz") in text
    assert "    float dir = array.get(trSide, c) == 1 ? 1.0 : -1.0" in text
    assert ("    math.floor((array.get(trExitP, c) - array.get(trEntryP, c)) * dir / oneR(c) * 100 + 0.5 + 0.000001) "
            "/ 100.0") in text
    # '0.00' (not '#.##'): a value below 1 keeps its leading zero and its two decimals ('-0.12R', '0.00R')
    assert '    (r > 0 ? "+" : "") + str.tostring(r, "0.00") + "R"' in text and '"#.' not in text
    assert f'string atrTf = input.timeframe("{DEFAULT_ATR_TF}", ' in text and DEFAULT_ATR_TF == "15"
    assert f'int atrLen = input.int({DEFAULT_ATR_LEN}, "ATR length"' in text and DEFAULT_ATR_LEN == 14
    assert PIP_INPUT_LINES[0] in text and PIP_INPUT_LINES[1] in text
    # the old stop-size-in-ATR column is gone
    for old in ("stopR", "stopRaw", "stopWhy", "stopCell", "stopTip", "trSlP", "trSlHas", "Stop (R)",
                "Minimum (R", "Maximum (R", "SL at entry"):
        assert old not in text, old


# ------------------------------------------------------------------------------------------------ the result
def test_result_t5_example() -> None:
    """The user's T5: SELL 1.10553 -> 1.10560 with 1R = 6 (an ATR of 4.3 -> 4.5 -> the minimum 6): -0.7 / 6."""
    r = result_r(1.10553, 1.10560, "SELL", 0.00043, EURUSD_PIP)
    assert r == -0.12 and result_text(r) == "-0.12R"


@pytest.mark.parametrize("entry,exit_,side,atr,text", [
    (1.1000, 1.1012, "BUY", 0.0008, "+1.50R"),        # +12 / 8
    (1.1000, 1.0991, "SELL", 0.00055, "+1.50R"),      # +9 / 6 (5.5 -> 6)
    (1.1000, 1.0991, "BUY", 0.00055, "-1.50R"),
    (1.1000, 1.1009, "SELL", 0.00055, "-1.50R"),
    (1.1000, 1.1000, "BUY", 0.0008, "0.00R"),
    (1.1000, 1.10003, "BUY", 0.0008, "+0.04R"),       # +0.3 / 8 = 0.0375
    (1.1000, 1.09999, "BUY", 0.002, "-0.01R"),        # -0.1 / 14 = -0.007
    (1.1000, 1.099995, "BUY", 0.002, "0.00R"),        # -0.05 / 14 = -0.004: '0.00R', never '-0.00R'
    (1.1000, 1.1140, "BUY", 0.002, "+10.00R"),        # 20 -> 14
])
def test_result_signs_and_format(entry: float, exit_: float, side: str, atr: float, text: str) -> None:
    assert result_text(result_r(entry, exit_, side, atr, EURUSD_PIP)) == text


def test_result_on_a_3_digit_symbol() -> None:
    jpy = pip_size(1_000, 0.001)
    assert result_text(result_r(150.000, 150.090, "BUY", 0.07, jpy)) == "+1.29R"      # 9 / 7
    assert result_text(result_r(150.000, 150.090, "SELL", 0.07, jpy)) == "-1.29R"


def _t5(**changes: object) -> Trade:
    """bracket_tp's trade turned into the user's T5: SELL 1.10553 closed at 1.10560, no stop-loss."""
    trades, _ = book(*bracket_tp())
    base: dict[str, object] = dict(side="SELL", entry_price=Decimal("1.10553"), exit_price=Decimal("1.10560"),
                                   exit_kind="CLOSED", stop_loss=(), take_profit=())
    base.update(changes)
    return dataclasses.replace(trades[0], **base)  # type: ignore[arg-type]


def _text(t: Trade) -> str:
    return render_symbol("EURUSD", [t], [], INFO).text


def test_t5_in_the_table() -> None:
    ch = emulate(_text(_t5()), atr=0.00043)
    assert ch.rows[0][3] == "-0.12R" and ch.result_colours == ["C_RISK"]
    assert ch.result_tips == ["result -0.12R; 1R from 15m ATR(14), clamped 6-14; exit: closed"]
    # no stop-loss is no reason for n/a: 1R does not depend on the stop
    assert ch.rows[0][3] != "n/a (no SL)"


def test_cell_colour_and_tooltip() -> None:
    text = render(*bracket_tp())                     # BUY 1.0801 -> TP 1.0900: +99 / 1R
    ch = emulate(text)                               # ATR 5 -> 6
    assert ch.rows[0][3] == "+16.50R" and ch.result_colours == ["C_REWARD"]
    assert ch.result_tips == ["result +16.50R; 1R from 15m ATR(14), clamped 6-14; exit: TP hit"]
    for atr, cell in ((0.001, "+9.90R"), (0.002, "+7.07R"), (0.00068, "+14.14R")):
        assert emulate(text, atr=atr).rows[0][3] == cell, atr
    assert emulate(text, atr=0.001, r_min=4.0, r_max=8.0).rows[0][3] == "+12.38R"               # 10 -> 8
    assert emulate(text, atr=0.001, r_min=4.0, r_max=8.0).result_tips[0].startswith(
        "result +12.38R; 1R from 15m ATR(14), clamped 4-8;")
    assert emulate(text, atr=0.001, r_min=6.5, r_max=12.5).result_tips[0].startswith(
        "result +9.90R; 1R from 15m ATR(14), clamped 6.5-12.5;")
    assert emulate(text, atr=0.001, pip_override=0.0002).rows[0][3] == "+8.25R"                 # 5 -> 6 x 0.0002
    assert emulate(_text(_t5(exit_price=Decimal("1.10553"))), atr=0.00043).rows[0][3] == "0.00R"
    assert emulate(_text(_t5(exit_price=Decimal("1.10553"))), atr=0.00043).result_colours == ["C_TEXT"]
    assert ("table.cell(tb, 3, row, resCell(c), text_color = resCol(c), text_size = tblSize, "
            "tooltip = resTip(c))") in text
    assert "    r > 0 ? C_REWARD : r < 0 ? C_RISK : C_TEXT" in text


def test_exit_kinds_in_the_tooltip() -> None:
    for kind, word in (("SL", "SL hit"), ("TP", "TP hit"), ("CLOSED", "closed")):
        tip = emulate(_text(_t5(exit_kind=kind)), atr=0.00043).result_tips[0]
        assert tip == f"result -0.12R; 1R from 15m ATR(14), clamped 6-14; exit: {word}"


def test_first_seen_open_entries_say_so() -> None:
    ch = emulate(_text(_t5(entry_kind="SEEN_OPEN")), atr=0.00043)
    assert ch.rows[0][3] == "-0.12R" and ch.result_tips[0].endswith("; exit: closed\nentry time = first seen open")


def test_header_is_result_r() -> None:
    text = render(*bracket_tp())
    assert TABLE_COLUMNS[3] == "Result (R)" and 'table.cell(tb, 3, 2, "Result (R)", ' in text
    assert "Stop (R)" not in text
    # the exit kind stays on the exit marker (and in the Result (R) tooltip)
    assert emulate(text).exits[0][5].endswith("· TP hit")


# ------------------------------------------------------------------------------------------------ non-repainting ATR
def test_request_security_at_global_scope_with_lookahead_on_and_one_bar_back() -> None:
    text = render(*bracket_tp())
    lines = text.split("\n")
    calls = [x for x in lines if "request.security(" in x]
    assert calls == ["float atrPrev = request.security(syminfo.tickerid, atrTf, ta.atr(atrLen)[1], "
                     "lookahead = barmerge.lookahead_on)"]                  # column 0: the global scope
    assert "bool atrTfOk = timeframe.in_seconds() <= atrTfS" in lines and "int atrTfS = timeframe.in_seconds(atrTf)" \
        in lines
    assert lines.index(calls[0]) < lines.index("var bool drawn = false")


def test_capture_is_one_pointer_pass_before_the_draw_block() -> None:
    text = render(*[x for i in range(110) for x in quick(i)])            # > 100 trades: chunks filled on bar 1
    lines = text.split("\n")
    loop = "while atrPtr < array.size(trEntryT) and array.get(trEntryT, atrPtr) < time_close"
    whiles = [x for x in lines if x.lstrip().startswith("while ")]
    assert whiles.count(loop) == 1 and len(whiles) == 2          # the capture and the Detailed layout scan
    i = lines.index(loop)
    assert lines[i - 3:i] == ["var float[] trAtr = array.new_float(array.size(trEntryT), na)",
                              "var int[] trAtrAt = array.new_int(array.size(trEntryT), 0)", "var int atrPtr = 0"]
    assert lines[i + 1:i + 4] == [
        f"    array.set(trAtrAt, atrPtr, barstate.isfirst and array.get(trEntryT, atrPtr) < time ? "
        f"{ATR_BEFORE_HISTORY} : {ATR_CAPTURED})",
        "    array.set(trAtr, atrPtr, atrPrev)", "    atrPtr += 1"]
    # after the data arrays are filled (first bar), before anything is drawn; no per-bar loop over all trades
    assert lines.index("if barstate.isfirst") < i < lines.index("var bool drawn = false")
    assert not any(re.match(r"for \w+ = ", x) for x in lines)            # nothing loops at the global scope
    entries = [int(x) for x in pine_arrays(text)["trEntryT"]]
    assert entries == sorted(entries)                                      # the pointer needs entries in order


def _bars(start: int, end: int, chart_ms: int, htf_atr: dict[int, float]) -> list[Bar]:
    """Chart bars [start, end) with the atrPrev TradingView gives `ta.atr(n)[1]` + lookahead_on: the ATR of the
    15-minute bar before the one holding the chart bar (htf_atr: 15-minute bar open -> its ATR)."""
    return [(t, t + chart_ms, htf_atr.get((t // Q) * Q - Q)) for t in range(start, end, chart_ms)]


HTF = {B + k * Q: 0.0001 * (k + 1) for k in range(-4, 12)}          # ATR of each 15-minute bar (distinct)


@pytest.mark.parametrize("chart_ms", [M, 5 * M, Q])
def test_the_bar_closed_before_the_entry_is_used(chart_ms: int) -> None:
    """Entry 01:00:00, 01:00:01, 01:01 or 01:14:59 -> the 00:45-01:00 bar; entry 00:59:59 -> the 00:30-00:45 bar;
    01:15:00 -> the 01:00-01:15 bar (1m, 5m and 15m charts)."""
    one = B + 4 * Q                                                      # '01:00' (B is '00:00')
    entries = [one - 1000, one, one + 1000, one + M, one + Q - 1000, one + Q]
    atr, state = capture_atr(entries, _bars(B, B + 8 * Q, chart_ms, HTF))
    assert state == [ATR_CAPTURED] * 6
    assert atr == [HTF[one - 2 * Q], HTF[one - Q], HTF[one - Q], HTF[one - Q], HTF[one - Q], HTF[one]]


def test_entries_before_the_first_bar_or_after_the_last() -> None:
    bars = _bars(B + Q, B + 3 * Q, M, HTF)
    entries = [B, B + Q - 1, B + Q, B + 3 * Q - 1, B + 3 * Q, B + 9 * Q]
    atr, state = capture_atr(entries, bars)
    assert state == [ATR_BEFORE_HISTORY, ATR_BEFORE_HISTORY, ATR_CAPTURED, ATR_CAPTURED, ATR_NOT_REACHED,
                     ATR_NOT_REACHED]
    assert atr[2] == HTF[B] and atr[3] == HTF[B + Q] and atr[4] is None
    # a gap (no bar holds the entry): taken on the next bar
    gap = [b for b in bars if not B + Q + 10 * M <= b[0] < B + Q + 20 * M]
    assert capture_atr([B + Q + 12 * M], gap) == ([HTF[B + Q]], [ATR_CAPTURED])
    with pytest.raises(ValueError):
        capture_atr([2, 1], bars)


def test_the_captured_atr_sets_1r() -> None:
    """The bar's ATR is the one 1R comes from: bracket_tp enters at T0 + H; on 1-minute bars whose atrPrev is
    0.0008 the result is +99 / 8."""
    text = render(*bracket_tp())
    assert emulate(text, bars=[(T0 + H, T0 + H + M, 0.0008)]).rows[0][3] == "+12.38R"


# ------------------------------------------------------------------------------------------------ open and n/a
def test_open_trade_shows_no_unrealised_result() -> None:
    ch = emulate(render(*bracket_tp()[:6]))
    assert ch.rows[0][3] == RES_OPEN == "open" and ch.result_colours == ["C_TEXT"]
    assert ch.result_tips == [RES_OPEN_TIP + "; exit: still open"]


def test_not_available_branches() -> None:
    text = render(*bracket_tp())
    one = [(T0, T0 + 2 * H, None)]
    assert emulate(render(*catch_up_trade(T0, 1038153117))).rows[0][3] == NA_ENTRY_TIME
    assert emulate(_text(_t5(entry_price=None))).rows[0][3] == NA_ENTRY_PRICE == "n/a (no entry price)"
    assert emulate(render(*quick(0))).rows[0][3] == NA_EXIT_PRICE == "n/a (no exit price)"
    assert emulate(render(*bare())).rows[0][3] == NA_EXIT_PRICE                  # no SL: only the exit price is missing
    assert emulate(_text(_t5(exit_kind="UNKNOWN"))).rows[0][3] == NA_EXIT_PRICE
    assert emulate(text, chart_s=3600).rows[0][3] == "n/a (chart > 15m)"
    assert emulate(text, chart_s=4 * 3600, atr_tf_s=3600).rows[0][3] == "n/a (chart > 1h)"
    assert emulate(text, chart_s=900, atr_tf_s=900).rows[0][3] == "+16.50R"           # equal is fine
    assert emulate(text, bars=[(T0 + 2 * H, T0 + 3 * H, 0.0005)]).rows[0][3] == NA_BEFORE_HISTORY
    assert emulate(text, bars=[(T0, T0 + H, 0.0005)]).rows[0][3] == NA_NOT_REACHED
    assert emulate(text, bars=one).rows[0][3] == NA_NO_ATR
    assert emulate(text, bars=[(T0 + H, T0 + H + M, 0.0005)]).rows[0][3] == "+16.50R"
    ch = emulate(text, bars=[(T0 + 2 * H, T0 + 3 * H, 0.0005)])
    assert ch.result_tips == ["n/a (before chart history); exit: TP hit"] and ch.result_colours == ["C_TEXT"]
    assert [tf_text(s) for s in (60, 900, 3600, 14400, 86400, 30)] == ["1m", "15m", "1h", "4h", "1D", "30s"]
    # weekly / monthly: the input's own text, not '7D' / seconds
    assert emulate(text, chart_s=30 * 86400, atr_tf_s=604800, atr_tf="1W").rows[0][3] == "n/a (chart > 1W)"
    assert "atrTfS >= 604800 ? (str.length(atrTf) > 0 ? atrTf : timeframe.period) : " in text
    for reason in ("open", "n/a (entry time n/r)", "n/a (no entry price)", "n/a (no exit price)",
                   "n/a (before chart history)", "n/a (after last bar)", "n/a (no ATR yet)"):
        assert f'"{reason}"' in text
    assert '"n/a (chart > " + atrTfTxt + ")"' in text and '"n/a (no SL)"' not in text


# ------------------------------------------------------------------------------------------------ lint, privacy
def test_lint_reserved_words_and_privacy() -> None:
    for text in (render(*bracket_tp()), render(*bare()), render(*catch_up_trade(T0, 1038153117)), _text(_t5())):
        assert lint(text) == []
        names = set(re.findall(r"^\s*(?:var\s+)?(?:int|float|string|bool)(?:\[\])?\s+(\w+)\s*=", text, re.M))
        names |= set(re.findall(r"^(\w+)\(", text, re.M))
        assert {"atrPrev", "atrTf", "atrLen", "rMin", "rMax", "pipOver", "symDigits", "pipSz", "trAtr", "trAtrAt",
                "atrPtr", "resWhy", "oneR", "resR", "fmtRes", "fmtNum", "resCell", "resCol", "resTip"} <= names
        assert not names & PINE_RESERVED
        # the pip unit is named only in the three inputs' titles
        lines = text.split("\n")
        assert [x for x in lines if FORBIDDEN_WORDS.search(x)] == list(PIP_INPUT_LINES)
        # neither 1R nor the pip is ever turned into text: only the result, the inputs and the ATR timeframe are
        for never in ("str.tostring(pipSz", "str.tostring(oneR", "str.tostring(array.get(trAtr", "fmtP(oneR",
                      "fmtP(pipSz", "fmtP(array.get(trAtr", "str.tostring(symDigits"):
            assert never not in text, never


@pytest.mark.parametrize("atr,pricescale,mintick", [(0.00043, 100_000, 0.00001), (0.0011, 10_000, 0.0001)])
def test_no_pip_count_or_pip_size_in_what_is_shown(atr: float, pricescale: int, mintick: float) -> None:
    texts = (render(*bracket_tp()), render(*bare()), render(*bracket_tp()[:6]), _text(_t5()),
             _text(_t5(entry_kind="SEEN_OPEN")))
    for text in texts:
        ch = emulate(text, atr=atr, pricescale=pricescale, mintick=mintick)
        for cell, tip in zip([r[3] for r in ch.rows], ch.result_tips, strict=True):
            assert re.fullmatch(r"[+-]?\d+\.\d\dR|open|n/a \([a-z0-9 />]+\)", cell), cell
            assert TIP.fullmatch(tip), tip
            assert not FORBIDDEN_WORDS.search(cell + "\n" + tip)
            assert "0.0001" not in tip and "0.00043" not in tip and "0.0011" not in tip   # no pip size, no ATR
