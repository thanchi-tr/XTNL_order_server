"""Compile trades into one TradingView Pine Script v6 indicator per symbol, drawn like the long / short position
tool.

The script embeds the drawing data as `var` arrays (times as UNIX ms, prices as float literals), filled on the
first bar, and draws once, on `barstate.islastconfirmedhistory` (or `barstate.islast` when that never fires),
with `xloc.bar_time`, so every drawing lands at its exact time whatever the chart timeframe. Times are formatted
in the script (`str.format_time`, time zone and format inputs), prices with `str.tostring(p, format.mintick)`.

One TRADE (`src.reports.lifecycle.Trade`: a position with its opening order, stop-loss / take-profit legs and
closing order) is drawn as:

* a red box from the entry price to the stop-loss over each stop-loss step's time span and a green one from the
  entry to the take-profit over each take-profit step (a moved level starts a new box), each with a fill and a
  1 px border of its own colour (transparency inputs), plus a thin dotted line on each level;
* the entry line (solid, side colour, width 2) from the entry time to the exit time;
* an ENTRY MARKER exactly at the entry time and price (triangle up BUY / down SELL, tiny, no text; tooltip
  `T3 SELL #1038847936 · entry 13 Nov 21:40 @ 1.10400`, the trade's orders and events);
* an EXIT MARKER exactly at the exit time and price (x-cross: SL hit, diamond: TP hit, circle: closed otherwise)
  whose own tiny text is the trade number (`T3`; a group's numbers, see Minimal), tooltip
  `T3 exit 14 Nov 03:10 @ 1.10120 · SL hit`; an open
  trade has an open circle at its right edge instead (tooltip `T3 still open · ...`); a dashed segment at the exit
  price runs to the right edge when the drawing is wider than the trade;
* a small dot on the level at every stop-loss / take-profit change inside the trade (tooltip only; an input puts
  the text on the chart);
* the opening order's working line before the fill, faint and dashed.

Label mode (input):

* MINIMAL (default): the trade's tag `T3 SELL` is the text of its own reward zone box (its risk zone when it has
  no take-profit), so it is anchored to the trade and never floats. It sits at the left corner of the zone's
  preferred end (input `TAG_CORNERS`: the entry end, default, or the level end); when an earlier tag's text box
  (estimated with the text-size inputs) already covers that corner, it takes the first free corner of its own
  zone (left: the other end; right: preferred, other), else stays at the preferred one. A trade with neither zone
  gets a thin transparent box around its entry line just to carry the tag. No text labels, no stacking: markers
  sit exactly on their anchors. Exit markers whose texts would meet (anchors within a text width and a text line)
  form a group: its first marker's text lists them all (`T1 T2`), the others have none, and every tooltip lists
  the group's exits, so coinciding exits never print over each other. Entry markers sharing a spot name each
  other in their tooltips. The table holds the detail.
* DETAILED: the earlier text labels: entry text immediately left of the trade (`T3 SELL #1038847936` /
  `entry 13 Nov 21:40 @ 1.10400`, pointer right), exit text immediately right of it (`exit 14 Nov 03:10 @ 1.10120`
  / `TP hit`, pointer left), laid out against each other when the script draws (`layout` below is the same
  algorithm in Python, for the tests): in time order, a label whose box (width: its longest text line x an input
  bars-per-character x the bar length; height: its text lines x the line height, an input in ticks or, 0 = auto,
  the chart's ATR) would cover a box already drawn is moved past that box, up or down. A label that would move
  more than MAX_SHIFT_HEIGHTS of its own heights is not drawn (its marker's tooltip still tells it).

A trade with no time span (a catch-up read recorded only its close) is drawn `Minimum trade width` bars wide,
ending at its exit; a trade shorter than that is widened to the right.

PENDING-ONLY orders (never filled) are a faint grey dashed line at the order price from placement to cancel (or
now) and a tiny grey mark whose tooltip tells the rest: they never compete with trades.

Table (default bottom-left), kept small: a title row `XTNL trades EURUSD` (its tooltip: read-only export, no
amounts, the trade / pending counts drawn and left out, the time zone and the chart symbol check; `(n left out)`
in red is added only when something was left out), a row with only the window (`window UTC 2026-11-07 00:00 >
2026-11-14 00:00`), the column titles, then one row per trade, newest first:
T# | Entry (time @ price) | Exit (time @ price) | Result (R). Entry / Exit show the time only (input `Table time
format`, default `HH:mm`: `00:46 @ 1.10553`); the cell's tooltip has the full date-time (`entry 14 Nov 00:46 @
1.10553`, the `Time format` input). The side is in the T# tooltip (with the ids) and on the chart. When the chart
symbol does not match, the one body row is a red `chart symbol mismatch: <chart>`.

Colours: text (table cells, the Minimal tags, the exit markers' trade numbers, Detailed entry / exit labels) is
the input `Text colour` (default #E6E6E6, light grey, readable on the dark background); the side colours (BUY
light blue #82B1FF, SELL light violet #B388FF: neither red nor green, so a SELL line never reads as the red
stop-loss zone) are for the entry lines, entry markers and working lines only.

RESULT (R): the trade's result in R, (exit price - entry price) * direction / 1R (direction +1 BUY, -1 SELL;
`result_r`), shown signed with 2 decimals (`-0.12R`, `+1.50R`, `0.00R`: `result_text`), green above 0, red below,
the text colour at 0 / n/a. 1R is a PRICE DISTANCE set from the ATR, never from the stop: the ATR (`ta.atr(length)`
on the ATR timeframe, inputs, default 14 on "15", Wilder) of the ATR-timeframe bar that CLOSED immediately before
the entry (entry 01:01 or exactly 01:00 -> the 00:45-01:00 bar) is counted in the symbol's pip (`pip_size`: the
tick x 10 when the price has 3 or 5 decimals, FX fractional pips, else the tick; an input overrides it), rounded to
the nearest 0.5 with halves up (`round_half_up`: 4.2 -> 4, 4.3 -> 4.5, 4.25 -> 4.5, 6.6 -> 6.5, 6.8 -> 7), clamped to
[min, max] (inputs, default 6 and 14: `one_r_pips`) and turned back into price. Worked example: a SELL from 1.10553
closed at 1.10560 with an ATR of 4.3 pip -> 4.5 -> 6 (the minimum): (1.10560 - 1.10553) * -1 / (6 * 0.0001) =
-0.12R. The script reads the ATR without repainting: `request.security(syminfo.tickerid, atrTf, ta.atr(atrLen)[1],
lookahead = barmerge.lookahead_on)` at the global scope is, on any chart bar, the ATR of the ATR-timeframe bar
before the one holding that chart bar; it is taken per trade on the chart bar holding the entry time (time <= entry
< time_close) in one pass (trades sorted by entry, a pointer advanced while entry < time_close: O(bars + trades)).
An open trade shows `open` (no unrealised result). `n/a (...)` when it cannot be measured: entry time / price or
exit price not recorded, a chart timeframe above the ATR timeframe, an entry before the chart's first bar (or not
reached yet), no ATR value yet. The tooltip: `result -0.12R; 1R from 15m ATR(14), clamped 6-14; exit: closed` (no
pip count and no pip size are ever shown).

Symbol guard: the data symbol and the chart ticker are normalised (upper case, letters and digits only) and
compared exactly or as a prefix (broker suffixes such as `EURUSD.a` / `EURUSDm`); an input can override the
chart ticker or allow any symbol.

Limits: TradingView keeps at most 500 lines / labels / boxes per script. The newest trades are kept until the
next one would not fit (lines, labels, boxes or MAX_SCRIPT_CHARS of source), then the newest pending orders with
what is left; the rest are left out and counted in the header and the table. Output is deterministic.

Privacy: only symbol, broker ids, trade numbers, side, times, status / event names and price levels are written.
No size or amount exists in the input (`src.reports.lifecycle` never reads them); the exit kind is TP / SL / closed /
open, never money. The Result (R) column (user-requested) shows the result in R and how 1R was set (ATR timeframe,
length, clamp): never a size, an amount, money, a pip count or the pip size.
"""
from __future__ import annotations

import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Final

from src.reports.lifecycle import LcEvent, PendingOrder, Trade, normalise_symbol

MAX_LINES: Final = 500
MAX_LABELS: Final = 500
MAX_BOXES: Final = 500
#: Conservative source budget (TradingView refuses very large scripts; the arrays dominate the size).
MAX_SCRIPT_CHARS: Final = 90_000
#: A trade with more stop-loss / take-profit steps keeps the newest steps (and change dots) of each leg.
MAX_STEPS_PER_LEG: Final = 100
ARRAY_CHUNK: Final = 100
#: A chunk line also ends before this many characters, so no source line gets near the lint bound.
ARRAY_CHUNK_CHARS: Final = 2_000
MAX_ID_CHARS: Final = 32
#: Order ids listed in a trade's tooltip (the rest are counted).
MAX_TIP_IDS: Final = 12
#: A trade's tooltip lists the first TIP_HEAD and the last TIP_TAIL events (the rest are counted).
TIP_HEAD: Final = 5
TIP_TAIL: Final = 24
#: Table: header rows + at most MAX_TABLE_ROWS trade rows (TradingView tables stay small: 40 rows).
#: Header rows: title, window, column titles.
TABLE_HEADER_ROWS: Final = 3
TABLE_COLUMNS: Final = ("T#", "Entry", "Exit", "Result (R)")
MAX_TABLE_ROWS: Final = 36
DEFAULT_TABLE_ROWS: Final = 12
#: Time format inputs: the markers / labels / tooltips (`Time format`) and the table's entry / exit cells
#: (`Table time format`: the time only, the date is in the cell's tooltip).
DEFAULT_TIME_FMT: Final = "dd MMM HH:mm"
DEFAULT_CELL_TIME_FMT: Final = "HH:mm"
#: Label layout (script and `layout`): box padding in text lines and characters, gap between boxes in text lines,
#: default bars per character of text (size.small text at TradingView's default bar spacing).
PAD_LINES: Final = 0.6
GAP_LINES: Final = 0.25
PAD_CHARS: Final = 2
DEFAULT_CHAR_BARS: Final = 1.2
DEFAULT_MIN_BARS: Final = 10
#: Zone fill and border transparency (inputs): the risk zone is often thin (a near stop), so it is a little
#: stronger than the reward zone, and both have a 1 px border.
DEFAULT_RISK_TRANSP: Final = 80
DEFAULT_REWARD_TRANSP: Final = 85
DEFAULT_BORDER_TRANSP: Final = 60
#: Label mode input: Minimal (default) / Detailed.
LABEL_MODES: Final = ("Minimal", "Detailed")
#: Detailed mode: a text label moved by its layout more than this many of its own heights is not drawn.
MAX_SHIFT_HEIGHTS: Final = 3
#: Half-height in ticks of the transparent box that carries the tag of a trade with neither zone.
TAG_TICKS: Final = 3
#: Minimal tag corner input: the zone's entry end (default) or its level end; a corner another tag already holds
#: moves the tag to the next free corner of its own zone (left: preferred end, other end; then right: the same).
TAG_CORNERS: Final = ("Entry end", "Level end")
#: Entry markers whose anchors lie within this many characters' width (plus PAD_CHARS) and a text line of an
#: earlier one share its spot: their tooltips name each other.
MARK_CHARS: Final = 1
#: The open-trade marker (an open circle, text-only label centred on its anchor).
OPEN_MARK: Final = "○"
#: Placed boxes the script's layout visits in all, at most (TradingView's loop-time limit).
LAYOUT_BUDGET: Final = 100_000

# step legs
LEG_SL, LEG_TP, LEG_WORK = 1, 2, 3
# entry kinds
ENTRY_FILLED, ENTRY_SEEN, ENTRY_NOT_RECORDED, ENTRY_BEFORE_WINDOW = 1, 2, 3, 4
# exit kinds
EXIT_SL, EXIT_TP, EXIT_CLOSED, EXIT_OPEN, EXIT_UNKNOWN = 1, 2, 3, 4, 5
_EXIT_CODE: Final = {"SL": EXIT_SL, "TP": EXIT_TP, "CLOSED": EXIT_CLOSED, "OPEN": EXIT_OPEN,
                     "UNKNOWN": EXIT_UNKNOWN}
#: The exit label's second line and the exit kind in the tooltips (Result (R) cell too).
EXIT_WORDS: Final = {EXIT_SL: "SL hit", EXIT_TP: "TP hit", EXIT_CLOSED: "closed", EXIT_OPEN: "still open",
                     EXIT_UNKNOWN: "position not recorded"}
#: Result (R) column (inputs): ATR timeframe and length, the 1R clamp bounds (in the symbol's pip), the pip
#: override (0 = from the symbol).
DEFAULT_ATR_TF: Final = "15"
DEFAULT_ATR_LEN: Final = 14
DEFAULT_R_MIN: Final = 6.0
DEFAULT_R_MAX: Final = 14.0
#: Added before the halves-up floor (1R to 0.5 pip, the result to 0.01R) so an exact half that float arithmetic
#: lands just below (7.25 pips -> 7.249999999999999, 0.125R -> 12.4999999 hundredths) still rounds up; far below any
#: real step (a tick over 1R is >= 1 / 280 of a hundredth). Written as a decimal literal in the script.
ROUND_EPS: Final = 0.000001
ROUND_EPS_PINE: Final = "0.000001"
#: Result (R) cell of an open trade (no unrealised result) and its tooltip's first part.
RES_OPEN: Final = "open"
RES_OPEN_TIP: Final = "open: no result until closed"
#: Result (R) n/a reasons (the cell text; the tooltip adds the exit kind).
NA_ENTRY_TIME: Final = "n/a (entry time n/r)"
NA_ENTRY_PRICE: Final = "n/a (no entry price)"
NA_EXIT_PRICE: Final = "n/a (no exit price)"
NA_BEFORE_HISTORY: Final = "n/a (before chart history)"
NA_NOT_REACHED: Final = "n/a (after last bar)"
NA_NO_ATR: Final = "n/a (no ATR yet)"
#: ATR capture state per trade (trAtrAt in the script).
ATR_NOT_REACHED, ATR_CAPTURED, ATR_BEFORE_HISTORY = 0, 1, 2
# pending kinds
PD_CANCELLED, PD_REJECTED, PD_WORKING, PD_FILLED = 1, 2, 3, 4
PD_WORDS: Final = {PD_CANCELLED: "cancelled", PD_REJECTED: "rejected", PD_WORKING: "still working",
                   PD_FILLED: "closing fill, position not in the log"}
# event codes (tooltips and change dots)
EV_PLACED, EV_PRICE, EV_SL_SET, EV_SL_MOVED, EV_SL_REMOVED, EV_TP_SET, EV_TP_MOVED, EV_TP_REMOVED = range(1, 9)
EV_FILLED, EV_OPENED, EV_CLOSED_GOOD, EV_CLOSED_BAD, EV_CLOSED_FLAT, EV_CLOSED_UNKNOWN = range(9, 15)
EV_CANCELLED, EV_REJECTED = 15, 16
_EV_CODE: Final = {"PLACED": EV_PLACED, "PRICE_CHANGED": EV_PRICE, "SL_SET": EV_SL_SET, "SL_MOVED": EV_SL_MOVED,
                   "SL_REMOVED": EV_SL_REMOVED, "TP_SET": EV_TP_SET, "TP_MOVED": EV_TP_MOVED,
                   "TP_REMOVED": EV_TP_REMOVED, "FILLED": EV_FILLED, "OPENED": EV_OPENED,
                   "CANCELLED": EV_CANCELLED, "REJECTED": EV_REJECTED}
_CLOSE_CODE: Final = {"FAVOURABLE": EV_CLOSED_GOOD, "UNFAVOURABLE": EV_CLOSED_BAD, "FLAT": EV_CLOSED_FLAT}
EV_WORDS: Final = {EV_PLACED: "placed", EV_PRICE: "order price moved", EV_SL_SET: "SL set",
                   EV_SL_MOVED: "SL moved", EV_SL_REMOVED: "SL removed", EV_TP_SET: "TP set",
                   EV_TP_MOVED: "TP moved", EV_TP_REMOVED: "TP removed", EV_FILLED: "filled",
                   EV_OPENED: "first seen open", EV_CLOSED_GOOD: "closed", EV_CLOSED_BAD: "closed",
                   EV_CLOSED_FLAT: "closed", EV_CLOSED_UNKNOWN: "closed", EV_CANCELLED: "cancelled",
                   EV_REJECTED: "rejected"}
EV_SUFFIX: Final = {EV_CLOSED_GOOD: " better", EV_CLOSED_BAD: " worse", EV_CLOSED_FLAT: " flat",
                    EV_CLOSED_UNKNOWN: " (price not recorded)"}
#: Default colours (all inputs in the script).
COLOURS: Final = {
    "C_RISK": ("#F23645", "Risk zone and stop-loss (entry to stop-loss)"),
    "C_REWARD": ("#089981", "Reward zone and take-profit (entry to take-profit)"),
    "C_BUY": ("#82B1FF", "BUY entry line and marker"),
    "C_SELL": ("#B388FF", "SELL entry line and marker"),
    "C_TEXT": ("#E6E6E6", "Text colour (table, trade tags, exit marker numbers, entry / exit labels)"),
    "C_EXIT": ("#D1D4DC", "Exit (closed / still open)"),
    "C_PENDING": ("#787B86", "Pending orders"),
    "C_BG": ("color.new(#131722, 10)", "Label / table background"),
}
TIME_ZONES: Final = ("Australia/Melbourne", "UTC", "Europe/London", "America/New_York", "Chart exchange")
TABLE_POSITIONS: Final = {"bottom left": "position.bottom_left", "bottom center": "position.bottom_center",
                          "top left": "position.top_left", "top center": "position.top_center",
                          "middle left": "position.middle_left"}


@dataclass(frozen=True)
class TradeEls:
    """What one trade costs and draws. `steps`: (leg, start, end, price), `adjust`: indexes into trade.events of
    the change dots, `tag_step`: index into `steps` of the zone that carries the trade's tag in Minimal mode (-1:
    none, a transparent tag box is drawn instead)."""

    trade: Trade
    entry_code: int
    steps: tuple[tuple[int, int, int, Decimal], ...]
    adjust: tuple[int, ...]
    trimmed: int
    lines: int
    boxes: int
    labels: int
    tag_step: int = -1


@dataclass(frozen=True)
class PendingEls:
    order: PendingOrder
    code: int
    steps: tuple[tuple[int, int, Decimal], ...]
    lines: int
    labels: int


@dataclass(frozen=True)
class RenderResult:
    symbol: str
    text: str
    orders: int
    positions: int
    trades: int
    pending: int
    drawn: tuple[str, ...]              # keys of the trades drawn (oldest entry first)
    drawn_pending: tuple[str, ...]
    left_out: int                       # trades left out (TradingView drawing limits)
    left_out_pending: int
    lines: int
    labels: int
    boxes: int
    trimmed_steps: int = 0
    events: int = 0
    undrawable: int = 0


@dataclass(frozen=True)
class RenderLimits:
    max_lines: int = MAX_LINES
    max_labels: int = MAX_LABELS
    max_boxes: int = MAX_BOXES
    max_chars: int = MAX_SCRIPT_CHARS
    max_steps_per_leg: int = MAX_STEPS_PER_LEG


@dataclass(frozen=True)
class RenderInfo:
    """Header facts (all deterministic inputs)."""

    generated_ms: int
    since_ms: int
    until_ms: int
    adapter: str | None = None
    orders: int = 0
    positions: int = 0
    notes: tuple[str, ...] = field(default=())


# ------------------------------------------------------------------------------------------------ literals
def pine_float(d: Decimal) -> str:
    """A Pine float literal without exponent (always with a decimal point, so arrays stay float[])."""
    s = format(d.normalize(), "f") if d != 0 else "0"
    if "." not in s:
        s += ".0"
    return s


_SAFE_ID = re.compile(r"[^A-Za-z0-9._:#@/-]")
#: C0 / C1 controls and the Unicode line / paragraph separators (an editor may break a line at any of them).
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f  ]")


def safe_id(s: str) -> str:
    """Broker ids in labels: a conservative character set, at most MAX_ID_CHARS."""
    cleaned = _SAFE_ID.sub("_", s)
    return cleaned if len(cleaned) <= MAX_ID_CHARS else cleaned[: MAX_ID_CHARS - 1] + "~"


def pine_str(s: str) -> str:
    """A double-quoted Pine string literal: control characters dropped, backslash and quote escaped."""
    s = _CONTROL.sub(" ", s)
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def comment_safe(s: str) -> str:
    return _CONTROL.sub(" ", s)


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%SZ")


# ------------------------------------------------------------------------------------------------ elements
def event_code(e: LcEvent) -> int:
    if e.kind == "CLOSED":
        return EV_CLOSED_UNKNOWN if e.price is None else _CLOSE_CODE.get(e.result or "", EV_CLOSED_UNKNOWN)
    return _EV_CODE[e.kind]


def head_text(t: Trade) -> str:
    """'T3 SELL #1038847936': the Detailed entry label's first line and the tooltips' title."""
    return f"{t.name} {t.side} #{safe_id(t.label_id)}"


def tag_text(t: Trade) -> str:
    """'T3 SELL': the Minimal tag inside the trade's zone."""
    return f"{t.name} {t.side}"


def tag_step(t: Trade, steps: Sequence[tuple[int, int, int, Decimal]]) -> int:
    """The zone that carries the tag: the first take-profit step (else stop-loss step) that starts at or before the
    entry and runs past it (or to the exit), so the script draws it from the trade's left edge with a positive
    width on every timeframe; -1 when there is none (or no entry price: no zone is drawn)."""
    if t.entry_price is None:
        return -1
    for want in (LEG_TP, LEG_SL):
        for i, (leg, start, end, _) in enumerate(steps):
            if leg == want and start <= t.entry_ms and (end > t.entry_ms or end >= t.exit_ms):
                return i
    return -1


def orders_text(t: Trade) -> str:
    """'orders #a #b #c[ +n more][ | position #p]' for the tooltips."""
    ids = list(t.order_ids)
    shown = " ".join("#" + safe_id(i) for i in ids[:MAX_TIP_IDS])
    more = f" +{len(ids) - MAX_TIP_IDS} more" if len(ids) > MAX_TIP_IDS else ""
    pos = f" | position #{safe_id(t.position_id)}" if t.position_id and t.position_id not in ids else ""
    return (f"orders {shown}{more}" if ids else "orders not recorded") + pos


def entry_code(t: Trade, since_ms: int) -> int:
    if t.entry_kind == "FILLED":
        return ENTRY_FILLED
    if t.entry_kind == "SEEN_OPEN":
        return ENTRY_BEFORE_WINDOW if t.entry_ms < since_ms else ENTRY_SEEN
    return ENTRY_NOT_RECORDED


def trade_elements(t: Trade, since_ms: int, max_steps_per_leg: int = MAX_STEPS_PER_LEG) -> TradeEls:
    """The drawing of one trade: zone / level steps (the newest `max_steps_per_leg` of each leg), the opening
    order's working steps, the change dots, and what they cost."""
    zero_span = t.entry_ms >= t.exit_ms
    steps: list[tuple[int, int, int, Decimal]] = []
    trimmed = 0
    for leg, legs in ((LEG_SL, t.stop_loss), (LEG_TP, t.take_profit)):
        kept = [s for s in legs if s.end_ms > s.start_ms or zero_span]
        if len(kept) > max_steps_per_leg:
            trimmed += len(kept) - max_steps_per_leg
            kept = kept[-max_steps_per_leg:]
        steps += [(leg, s.start_ms, s.end_ms, s.price) for s in kept]
    work = [(LEG_WORK, s.start_ms, s.end_ms, s.price) for s in t.working if s.end_ms > s.start_ms]
    adjust: list[int] = []
    per_leg: dict[str, list[int]] = {}
    for i, e in enumerate(t.events):
        if e.kind.startswith(("SL_", "TP_")) and t.entry_ms < e.t_ms < t.exit_ms and e.price is not None:
            per_leg.setdefault(e.kind[:2], []).append(i)
    for leg_name in sorted(per_leg):
        adjust += per_leg[leg_name][-max_steps_per_leg:]
    has_entry = t.entry_price is not None
    has_exit_mark = t.exit_kind in ("SL", "TP", "CLOSED")
    tag = tag_step(t, steps)
    # boxes: one per zone step, plus the transparent tag box when no zone carries the tag
    zones = (len(steps) if has_entry else 0) + (0 if tag >= 0 else 1)
    lines = (1 if has_entry else 0) + (1 if has_exit_mark and t.exit_price is not None else 0) + len(steps) \
        + len(work)
    # labels (the larger of the two modes, Detailed): entry and exit text, entry marker, exit marker, change dots
    labels = 4 + len(adjust)
    return TradeEls(t, entry_code(t, since_ms), tuple(steps + work), tuple(sorted(adjust)), trimmed, lines, zones,
                    labels, tag)


def pending_elements(p: PendingOrder) -> PendingEls:
    code = {"CANCELLED": PD_CANCELLED, "REJECTED": PD_REJECTED, "FILLED": PD_FILLED}.get(p.outcome, PD_WORKING)
    steps = tuple((s.start_ms, s.end_ms, s.price) for s in p.steps if s.end_ms > s.start_ms)
    return PendingEls(p, code, steps, len(steps), 1 if p.price is not None else 0)


def drawable(t: Trade) -> bool:
    return t.entry_price is not None or t.exit_price is not None


# ------------------------------------------------------------------------------------------------ Result (R)
def pip_size(pricescale: int, mintick: float, override: float = 0.0) -> float:
    """The symbol's pip as the script derives it: `override` when above 0, else the tick x 10 when the price has
    3 or 5 decimals (round(log10(pricescale)): FX fractional pips, EURUSD 0.00001 -> 0.0001, USDJPY 0.001 -> 0.01),
    else the tick (2 / 4 decimals: the tick is the pip)."""
    if override > 0:
        return override
    digits = round(math.log10(pricescale))
    return mintick * 10 if digits in (3, 5) else mintick


def round_half_up(raw: float) -> float:
    """The nearest 0.5, halves up: the script's `math.floor(raw * 2 + 0.5 + 0.000001) / 2.0` (4.2 -> 4, 4.3 -> 4.5,
    4.25 -> 4.5, 6.6 -> 6.5, 6.75 -> 7, 6.8 -> 7), plus ROUND_EPS so a float just below a half still rounds up."""
    return math.floor(raw * 2 + 0.5 + ROUND_EPS) / 2.0


def one_r_pips(atr: float, pip: float, r_min: float = DEFAULT_R_MIN, r_max: float = DEFAULT_R_MAX) -> float:
    """1R in the symbol's pip: the ATR / pip rounded to the nearest 0.5 (halves up), then clamped to [r_min, r_max]
    (below counts as r_min, above as r_max), as the script computes it. 1R in price is this x pip."""
    return max(r_min, min(r_max, round_half_up(atr / pip)))


def result_r(entry: float, exit_: float, side: str, atr: float, pip: float, r_min: float = DEFAULT_R_MIN,
             r_max: float = DEFAULT_R_MAX) -> float:
    """(exit - entry) x direction / 1R (direction +1 BUY, -1 SELL), rounded to 0.01 with halves up, as the script
    computes it (float arithmetic in the same order, plus ROUND_EPS)."""
    d = 1.0 if side == "BUY" else -1.0
    return math.floor((exit_ - entry) * d / (one_r_pips(atr, pip, r_min, r_max) * pip) * 100 + 0.5 + ROUND_EPS) / 100.0


def result_text(r: float) -> str:
    """'+1.50R', '-0.12R', '0.00R': signed, 2 decimals (the script's '0.00' format with a '+' above 0)."""
    return ("+" if r > 0 else "") + f"{r:.2f}R"


# ------------------------------------------------------------------------------------------------ layout
@dataclass(frozen=True)
class TextBox:
    """A text label as the script sizes it: time, price (the box's vertical centre), the side it moves to when in
    the way (+1 up, -1 down), text lines, the longest line's characters, and where the box lies against its
    time (+1 right of it: an exit / change label, -1 left of it: an entry label, 0 centred)."""

    x: int
    anchor: float
    dir: int
    rows: int
    chars: int
    mode: int = 1


def layout(boxes: Sequence[TextBox], line_h: float, char_ms: float, budget: int = LAYOUT_BUDGET,
           max_shift: float = MAX_SHIFT_HEIGHTS) -> list[float | None]:
    """Detailed mode: the y (vertical centre) of each label, as the script computes it when it draws (`boxes` in
    drawing order: by time), or None when it is not drawn. A box is `(chars + PAD_CHARS) * char_ms` wide, on its
    side of its time (`mode`), and `(rows + PAD_LINES) * line_h` high, centred on its price. For each label, the
    boxes already placed that overlap it in time are taken nearest first on its side (up: by bottom, ascending;
    down: by top, descending); each one it would cover pushes it just past that box (+ GAP_LINES). Labels already
    placed never move. A label pushed more than `max_shift` of its own heights from its price is not drawn and
    not placed (tooltip only: its marker carries it). The scan back stops once a placed label's time is
    `w + wMax` before this one's (no box that far back can reach it). At most `budget` placed boxes are visited in
    all; after that a label stays on its own price."""
    px: list[float] = []
    pc: list[float] = []
    pw: list[float] = []
    plo: list[float] = []
    phi: list[float] = []
    w_max = 0.0
    out: list[float | None] = []
    gap = GAP_LINES * line_h
    left = budget
    for b in boxes:
        if px and b.x < px[-1]:
            raise ValueError("boxes must be in time order")
        w = (b.chars + PAD_CHARS) * char_ms
        h = (b.rows + PAD_LINES) * line_h
        cx = b.x + b.mode * w / 2
        lo, hi = b.anchor - h / 2, b.anchor + h / 2
        nlo: list[float] = []
        nhi: list[float] = []
        j = len(px) - 1
        while j >= 0 and left > 0:
            if b.x - px[j] >= w + w_max:
                break
            left -= 1
            if abs(cx - pc[j]) * 2 < w + pw[j]:
                nlo.append(plo[j])
                nhi.append(phi[j])
            j -= 1
        if b.dir > 0:
            for m in sorted(range(len(nlo)), key=lambda m: (nlo[m], m)):
                if nlo[m] < hi and nhi[m] > lo:
                    lo = nhi[m] + gap
                    hi = lo + h
        else:
            for m in sorted(range(len(nhi)), key=lambda m: (-nhi[m], m)):
                if nlo[m] < hi and nhi[m] > lo:
                    hi = nlo[m] - gap
                    lo = hi - h
        y = (lo + hi) / 2
        if abs(y - b.anchor) > max_shift * h:
            out.append(None)
            continue
        px.append(float(b.x))
        pc.append(cx)
        pw.append(w)
        plo.append(lo)
        phi.append(hi)
        w_max = max(w_max, w)
        out.append(y)
    return out


# ------------------------------------------------------------------------------------------------ text
def _chunks(values: Sequence[str]) -> list[str]:
    """At most ARRAY_CHUNK items and (unless one item alone is longer) ARRAY_CHUNK_CHARS characters each."""
    out: list[str] = []
    cur: list[str] = []
    size = 0
    for v in values:
        if cur and (len(cur) >= ARRAY_CHUNK or size + 2 + len(v) > ARRAY_CHUNK_CHARS):
            out.append(", ".join(cur))
            cur, size = [], 0
        size += len(v) + (2 if cur else 0)
        cur.append(v)
    if cur:
        out.append(", ".join(cur))
    return out


def _array(name: str, pine_type: str, values: Sequence[str]) -> tuple[str, list[str]]:
    """(the `var` declaration with the first chunk, the array.concat lines of the other chunks)."""
    if not values:
        return f"var {pine_type}[] {name} = array.new_{pine_type}(0)", []
    parts = _chunks(values)
    return (f"var {pine_type}[] {name} = array.from({parts[0]})",
            [f"    array.concat({name}, array.from({p}))" for p in parts[1:]])


def _pine_switch(indent: str, cases: Sequence[tuple[int, str]], default: str) -> list[str]:
    return [f"{indent}{k} => {v}" for k, v in cases] + [f"{indent}=> {default}"]


def _opt(p: Decimal | None) -> str:
    return pine_float(p) if p is not None else "0.0"


def _has(p: Decimal | None) -> str:
    return "0" if p is None else "1"


@dataclass
class _Counts:
    trades: int
    pending: int
    left_out: int
    left_out_pending: int
    trimmed: int
    undrawable: int


def _script(symbol: str, info: RenderInfo, trades: Sequence[TradeEls], pending: Sequence[PendingEls],
            n: _Counts) -> str:
    data_sym = normalise_symbol(symbol) or "UNKNOWN"
    c = comment_safe
    n_lines = sum(t.lines for t in trades) + sum(p.lines for p in pending)
    n_labels = sum(t.labels for t in trades) + sum(p.labels for p in pending)
    n_boxes = sum(t.boxes for t in trades)
    n_events = sum(len(t.trade.events) for t in trades)
    left = n.left_out + n.left_out_pending
    head = [
        "//@version=6",
        f"// XTNL trades: {c(symbol)}",
        "// read-only export, no amounts (symbol, ids, trade numbers, sides, times, status / event names and price "
        "levels only)",
        f"// generated: {iso(info.generated_ms)}",
        f"// window: {iso(info.since_ms)} to {iso(info.until_ms)}",
        f"// adapter: {c(info.adapter or 'all')}",
        f"// orders: {info.orders}  positions: {info.positions}  trades: {n.trades}  pending-only orders: "
        f"{n.pending}",
        f"// drawn: trades {len(trades)} of {n.trades}, pending {len(pending)} of {n.pending}  left out "
        f"(TradingView drawing limits, oldest first): {left}",
        f"// lines used: {n_lines} of {MAX_LINES}  labels used: {n_labels} of {MAX_LABELS}  boxes used: "
        f"{n_boxes} of {MAX_BOXES}  events: {n_events}",
    ]
    if n.trimmed:
        head.append(f"// stop-loss / take-profit steps trimmed (oldest of a long leg): {n.trimmed}")
    if n.undrawable:
        head.append(f"// trades without any price level (not drawable): {n.undrawable}")
    head += [f"// note: {c(x)}" for x in info.notes]
    head.append("// Paste into the Pine Editor, then Add to chart. Times are UNIX ms (xloc.bar_time).")
    window = f"window UTC {iso(info.since_ms)[:16]} > {iso(info.until_ms)[:16]}"
    title = f"XTNL trades {data_sym}" + (f"  ({left} left out)" if left else "")
    # the title cell's tooltip: what the export is, the counts, then (in the script) time zone and symbol check
    title_tip = ' + "\\n" + '.join(pine_str(x) for x in (
        "read-only export, no amounts",
        f"trades {n.trades} (drawn {len(trades)}, left out {n.left_out})",
        f"pending orders {n.pending} (drawn {len(pending)}, left out {n.left_out_pending})"))
    tz_opts = ", ".join(pine_str(z) for z in TIME_ZONES)
    pos_opts = ", ".join(pine_str(p) for p in TABLE_POSITIONS)
    body = [
        f'indicator("XTNL trades {data_sym}", shorttitle = "XTNL {data_sym}", overlay = true, '
        f"max_lines_count = {MAX_LINES}, max_labels_count = {MAX_LABELS}, max_boxes_count = {MAX_BOXES})",
        "",
        'bool showZones = input.bool(true, "Risk / reward zones", group = "Layers")',
        'bool showLevels = input.bool(true, "Stop-loss / take-profit level lines", group = "Layers")',
        'bool showLabels = input.bool(true, "Trade tags (Minimal) / entry and exit text (Detailed)", '
        'group = "Layers")',
        'bool showMarkers = input.bool(true, "Entry and exit markers", group = "Layers")',
        'bool showAdjust = input.bool(true, "Stop-loss / take-profit changes (dots)", group = "Layers")',
        'bool showWorking = input.bool(true, "Opening order before the fill", group = "Layers")',
        'bool showPending = input.bool(true, "Pending orders that never filled", group = "Layers")',
        'bool showTable = input.bool(true, "Trade table", group = "Layers")',
        f'string labelMode = input.string("{LABEL_MODES[0]}", "Label mode", options = '
        f'[{", ".join(pine_str(m) for m in LABEL_MODES)}], group = "Display")',
        f'bool minimal = labelMode != "{LABEL_MODES[1]}"',
        'bool adjText = input.bool(false, "Text on the change dots", group = "Display")',
        f'string tzIn = input.string("Australia/Melbourne", "Time zone", options = [{tz_opts}], group = "Display")',
        f'string timeFmt = input.string("{DEFAULT_TIME_FMT}", "Time format", group = "Display")',
        f'int minBars = input.int({DEFAULT_MIN_BARS}, "Minimum trade width (bars)", minval = 1, group = "Display")',
        f'string tagCorner = input.string("{TAG_CORNERS[0]}", "Minimal: tag corner (a corner another tag holds '
        f'moves it to a free corner of its zone)", options = [{", ".join(pine_str(x) for x in TAG_CORNERS)}], '
        'group = "Display")',
        'int spacingTicks = input.int(0, "Text line height (ticks, 0 = auto: ATR)", minval = 0, group = "Display")',
        f'float charBars = input.float({DEFAULT_CHAR_BARS}, "Text width per character (bars, raise when zoomed out)", '
        'minval = 0.1, step = 0.1, group = "Display")',
        f'int rowsIn = input.int({DEFAULT_TABLE_ROWS}, "Table rows (newest trades first)", minval = 1, '
        f'maxval = {MAX_TABLE_ROWS}, group = "Table")',
        f'string tablePosIn = input.string("bottom left", "Table position", options = [{pos_opts}], '
        'group = "Table")',
        'string tableSizeIn = input.string("small", "Table text size", options = ["tiny", "small", "normal"], '
        'group = "Table")',
        f'string cellFmt = input.string("{DEFAULT_CELL_TIME_FMT}", "Table time format (entry / exit cells; the full '
        'date-time is in the cell tooltip)", group = "Table")',
        f'string atrTf = input.timeframe("{DEFAULT_ATR_TF}", "ATR timeframe (1R from the ATR of the bar closed '
        'before the entry)", group = "Result (R)")',
        f'int atrLen = input.int({DEFAULT_ATR_LEN}, "ATR length", minval = 1, group = "Result (R)")',
        f'float rMin = input.float({DEFAULT_R_MIN}, "1R minimum (pips)", minval = 0.5, step = 0.5, '
        'group = "Result (R)")',
        f'float rMax = input.float({DEFAULT_R_MAX}, "1R maximum (pips)", minval = 0.5, step = 0.5, '
        'group = "Result (R)")',
        'float pipOver = input.float(0.0, "Pip size override (0 = auto)", minval = 0, step = 0.00001, '
        'group = "Result (R)")',
        f'int riskFillT = input.int({DEFAULT_RISK_TRANSP}, "Risk zone fill transparency", minval = 0, maxval = 100, '
        'group = "Colours")',
        f'int riskEdgeT = input.int({DEFAULT_BORDER_TRANSP}, "Risk zone border transparency (1 px)", minval = 0, '
        'maxval = 100, group = "Colours")',
        f'int rewardFillT = input.int({DEFAULT_REWARD_TRANSP}, "Reward zone fill transparency", minval = 0, '
        'maxval = 100, group = "Colours")',
        f'int rewardEdgeT = input.int({DEFAULT_BORDER_TRANSP}, "Reward zone border transparency (1 px)", minval = 0, '
        'maxval = 100, group = "Colours")',
        *[f'color {name} = input.color({dflt}, {pine_str(title)}, group = "Colours")'
          for name, (dflt, title) in COLOURS.items()],
        'string symOverride = input.string("", "Chart symbol override (blank = chart ticker)", group = "Symbol")',
        'bool anySymbol = input.bool(false, "Draw on any symbol", group = "Symbol")',
        "",
        f"const string DATA_SYMBOL = {pine_str(data_sym)}",
        "",
        "normSym(string s) =>",
        "    string u = str.upper(s)",
        *[f'    u := str.replace_all(u, "{ch}", "")' for ch in ("/", ".", "_", "-", " ", ":", "!", "+", "#")],
        "    u",
        "",
        "string chartSym = normSym(str.length(symOverride) > 0 ? symOverride : syminfo.ticker)",
        "bool symbolOk = anySymbol or chartSym == DATA_SYMBOL or "
        "(str.length(chartSym) > str.length(DATA_SYMBOL) and str.startswith(chartSym, DATA_SYMBOL))",
        'string tzName = tzIn == "Chart exchange" ? syminfo.timezone : tzIn',
        "string tblPos = switch tablePosIn",
        *[f"    {pine_str(k)} => {v}" for k, v in TABLE_POSITIONS.items() if k != "bottom left"],
        "    => position.bottom_left",
        'string tblSize = tableSizeIn == "tiny" ? size.tiny : tableSizeIn == "normal" ? size.normal : size.small',
        "float atrNow = ta.atr(14)",
        "// Result (R): on any chart bar, the ATR of the ATR-timeframe bar BEFORE the one holding this bar ([1] with",
        "// lookahead_on reads only closed bars: no repainting); valid while the chart timeframe <= the ATR timeframe",
        "float atrPrev = request.security(syminfo.tickerid, atrTf, ta.atr(atrLen)[1], "
        "lookahead = barmerge.lookahead_on)",
        "int atrTfS = timeframe.in_seconds(atrTf)",
        "bool atrTfOk = timeframe.in_seconds() <= atrTfS",
        "// the ATR timeframe as text: '30s', '15m', '1h', '1D'; weekly / monthly as the input names it ('1W', '1M')",
        'string atrTfTxt = atrTfS >= 604800 ? (str.length(atrTf) > 0 ? atrTf : timeframe.period) : '
        'atrTfS % 86400 == 0 ? str.tostring(math.round(atrTfS / 86400)) + "D" : atrTfS % 3600 == 0 ? '
        'str.tostring(math.round(atrTfS / 3600)) + "h" : atrTfS % 60 == 0 ? str.tostring(math.round(atrTfS / 60)) + '
        '"m" : str.tostring(atrTfS) + "s"',
        "// the symbol's pipSz: the tick x 10 with 3 or 5 price decimals (FX fractional), else the tick; the input wins",
        "int symDigits = math.round(math.log10(syminfo.pricescale))",
        "float pipSz = pipOver > 0 ? pipOver : symDigits == 3 or symDigits == 5 ? syminfo.mintick * 10 : "
        "syminfo.mintick",
        "",
    ]
    decls: list[str] = []
    fills: list[str] = []

    def arr(name: str, pine_type: str, values: Sequence[str]) -> None:
        d, f = _array(name, pine_type, values)
        decls.append(d)
        fills.extend(f)

    # ---- trades (oldest entry first), their steps, events and change dots
    tr = [t.trade for t in trades]
    arr("trHead", "string", [pine_str(head_text(t)) for t in tr])
    arr("trTag", "string", [pine_str(tag_text(t)) for t in tr])
    arr("trNo", "string", [pine_str(t.name) for t in tr])
    arr("trOrders", "string", [pine_str(orders_text(t)) for t in tr])
    arr("trSide", "int", ["1" if t.side == "BUY" else "2" for t in tr])
    arr("trEntryT", "int", [str(t.entry_ms) for t in tr])
    arr("trEntryP", "float", [_opt(t.entry_price) for t in tr])
    arr("trEntryHasP", "int", [_has(t.entry_price) for t in tr])
    arr("trEntryKind", "int", [str(t.entry_code) for t in trades])
    arr("trExitT", "int", [str(t.exit_ms) for t in tr])
    arr("trExitP", "float", [_opt(t.exit_price) for t in tr])
    arr("trExitHasP", "int", [_has(t.exit_price) for t in tr])
    arr("trExitKind", "int", [str(_EXIT_CODE[t.exit_kind]) for t in tr])
    firsts: list[int] = []
    evs: list[tuple[LcEvent, int]] = []
    adj: list[int] = []
    steps: list[tuple[int, int, int, int, Decimal]] = []
    tags: list[int] = []
    for ci, te in enumerate(trades):
        base = len(evs)
        firsts.append(base)
        evs += [(e, ci) for e in te.trade.events]
        adj += [base + i for i in te.adjust]
        tags.append(len(steps) + te.tag_step if te.tag_step >= 0 else -1)
        steps += [(ci, leg, x1, x2, p) for leg, x1, x2, p in te.steps]
    # the step (index into the st* arrays) whose zone carries the trade's tag in Minimal mode, -1: a tag box
    arr("trTagSt", "int", [str(x) for x in tags])
    arr("trFirst", "int", [str(x) for x in firsts])
    arr("trCount", "int", [str(len(t.trade.events)) for t in trades])
    newest = sorted(range(len(tr)), key=lambda i: (-tr[i].entry_ms, -i))[:MAX_TABLE_ROWS]
    arr("trNewest", "int", [str(i) for i in newest])
    arr("stTr", "int", [str(s[0]) for s in steps])
    arr("stLeg", "int", [str(s[1]) for s in steps])
    arr("stX1", "int", [str(s[2]) for s in steps])
    arr("stX2", "int", [str(s[3]) for s in steps])
    arr("stP", "float", [pine_float(s[4]) for s in steps])
    arr("evT", "int", [str(e.t_ms) for e, _ in evs])
    arr("evP", "float", [_opt(e.price) for e, _ in evs])
    arr("evHasP", "int", [_has(e.price) for e, _ in evs])
    arr("evCode", "int", [str(event_code(e)) for e, _ in evs])
    arr("evTr", "int", [str(ci) for _, ci in evs])
    arr("adjEv", "int", [str(i) for i in adj])
    # ---- pending orders (oldest end first) and their lines
    po = [p.order for p in pending]
    arr("pdHead", "string", [pine_str(f"#{safe_id(p.order_id)} {p.side}") for p in po])
    arr("pdLegs", "string", [pine_str(("legs " + " ".join("#" + safe_id(x) for x in p.legs[:MAX_TIP_IDS]))
                                      if p.legs else "") for p in po])
    arr("pdT1", "int", [str(p.first_ms) for p in po])
    arr("pdT2", "int", [str(p.end_ms) for p in po])
    arr("pdP", "float", [_opt(p.price) for p in po])
    arr("pdKind", "int", [str(p.code) for p in pending])
    arr("pdPlaced", "int", ["1" if p.placed_seen else "0" for p in po])
    arr("pdSl", "float", [_opt(p.stop_loss) for p in po])
    arr("pdSlHas", "int", [_has(p.stop_loss) for p in po])
    arr("pdTp", "float", [_opt(p.take_profit) for p in po])
    arr("pdTpHas", "int", [_has(p.take_profit) for p in po])
    psteps = [(k, x1, x2, pr) for k, p in enumerate(pending) for x1, x2, pr in p.steps]
    arr("psX1", "int", [str(s[1]) for s in psteps])
    arr("psX2", "int", [str(s[2]) for s in psteps])
    arr("psP", "float", [pine_float(s[3]) for s in psteps])
    body += decls
    if fills:
        body += ["if barstate.isfirst", *fills]
    body += [
        "",
        "// Result (R): each trade's ATR (atrPrev) taken on the chart bar holding its entry (time <= entry < time_close),",
        "// in one pass: the trades are sorted by entry and a pointer walks them once (O(bars + trades)); an entry",
        f"// before the first bar is marked {ATR_BEFORE_HISTORY}, a captured one {ATR_CAPTURED}, "
        f"one not reached yet stays {ATR_NOT_REACHED}",
        "var float[] trAtr = array.new_float(array.size(trEntryT), na)",
        "var int[] trAtrAt = array.new_int(array.size(trEntryT), 0)",
        "var int atrPtr = 0",
        "while atrPtr < array.size(trEntryT) and array.get(trEntryT, atrPtr) < time_close",
        f"    array.set(trAtrAt, atrPtr, barstate.isfirst and array.get(trEntryT, atrPtr) < time ? "
        f"{ATR_BEFORE_HISTORY} : {ATR_CAPTURED})",
        "    array.set(trAtr, atrPtr, atrPrev)",
        "    atrPtr += 1",
    ]
    words = [(k, pine_str(v)) for k, v in sorted(EV_WORDS.items())]
    suffixes = [(k, pine_str(v)) for k, v in sorted(EV_SUFFIX.items())]
    cap = TIP_HEAD + TIP_TAIL
    last_col = len(TABLE_COLUMNS) - 1
    hdr = TABLE_HEADER_ROWS
    body += [
        "",
        "fmtT(int t) =>",
        "    str.format_time(t, timeFmt, tzName)",
        "",
        "// the table's entry / exit time: the time only by default ('00:46')",
        "fmtC(int t) =>",
        "    str.format_time(t, cellFmt, tzName)",
        "",
        "fmtP(float p) =>",
        "    str.tostring(p, format.mintick)",
        "",
        "evWord(int k) =>",
        "    switch k",
        *_pine_switch("        ", words, '"?"'),
        "",
        "evSuffix(int k) =>",
        "    switch k",
        *_pine_switch("        ", suffixes, '""'),
        "",
        "exitWord(int k) =>",
        "    switch k",
        *_pine_switch("        ", [(k, pine_str(v)) for k, v in sorted(EXIT_WORDS.items())], '"?"'),
        "",
        "pendWord(int k) =>",
        "    switch k",
        *_pine_switch("        ", [(k, pine_str(v)) for k, v in sorted(PD_WORDS.items())], '"?"'),
        "",
        "sideLine(int s) =>",
        "    s == 1 ? C_BUY : C_SELL",
        "",
        "exitColor(int k) =>",
        f"    k == {EXIT_SL} ? C_RISK : k == {EXIT_TP} ? C_REWARD : C_EXIT",
        "",
        "evColor(int k) =>",
        f"    k >= {EV_SL_SET} and k <= {EV_SL_REMOVED} ? C_RISK : k >= {EV_TP_SET} and k <= {EV_TP_REMOVED} ? "
        "C_REWARD : C_EXIT",
        "",
        "// '<word> <time>[ @ <price>]<suffix>', e.g. 'SL moved 14 Nov 09:30 @ 1.08000'",
        "evLine(int i) =>",
        "    int k = array.get(evCode, i)",
        '    string at = array.get(evHasP, i) == 1 ? " @ " + fmtP(array.get(evP, i)) : ""',
        '    evWord(k) + " " + fmtT(array.get(evT, i)) + at + evSuffix(k)',
        "",
        "// change text (input): 'SL → <price>' / 'TP → <price>' ('SL removed'), the time on the second line",
        "markerText(int i) =>",
        "    int k = array.get(evCode, i)",
        f'    string tag = k <= {EV_SL_REMOVED} ? "SL" : "TP"',
        f"    bool gone = k == {EV_SL_REMOVED} or k == {EV_TP_REMOVED}",
        '    string top = gone ? tag + " removed" : tag + " → " + fmtP(array.get(evP, i))',
        '    top + "\\n" + fmtT(array.get(evT, i))',
        "",
        "// 'entry 13 Nov 21:40 @ 1.10400' (or first seen open / entry time not recorded / opened before window)",
        "entryLine(int c) =>",
        "    int k = array.get(trEntryKind, c)",
        '    string at = array.get(trEntryHasP, c) == 1 ? " @ " + fmtP(array.get(trEntryP, c)) : '
        '" @ price not recorded"',
        f'    string s = k == {ENTRY_NOT_RECORDED} ? "entry time not recorded" + at : '
        f'(k == {ENTRY_FILLED} ? "entry " : "first seen open ") + fmtT(array.get(trEntryT, c)) + at',
        f'    k == {ENTRY_BEFORE_WINDOW} ? s + "\\nopened before window" : s',
        "",
        "entryText(int c) =>",
        '    array.get(trHead, c) + "\\n" + entryLine(c)',
        "",
        "// 'exit 14 Nov 03:10 @ 1.10120' / 'TP hit' (SL hit, closed, still open)",
        "exitText(int c) =>",
        "    int k = array.get(trExitKind, c)",
        '    string at = array.get(trExitHasP, c) == 1 ? " @ " + fmtP(array.get(trExitP, c)) : '
        '" @ price not recorded"',
        f'    string top = k == {EXIT_OPEN} ? "at export " + fmtT(array.get(trExitT, c)) : k == {EXIT_UNKNOWN} ? '
        '"last seen " + fmtT(array.get(trExitT, c)) : "exit " + fmtT(array.get(trExitT, c)) + at',
        '    top + "\\n" + exitWord(k)',
        "",
        "// a trade's hover: the title line, its orders, its events (capped), its result",
        "tradeTip(int c, string title) =>",
        '    string s = title + "\\n" + array.get(trOrders, c)',
        "    int f = array.get(trFirst, c)",
        "    int n = array.get(trCount, c)",
        f"    // at most {TIP_HEAD} + {TIP_TAIL} event lines (a trailing stop can log hundreds; Pine strings are "
        "capped)",
        "    if n > 0",
        "        for j = f to f + n - 1",
        f"            if n <= {cap} or j < f + {TIP_HEAD} or j >= f + n - {TIP_TAIL}",
        '                s := s + "\\n" + evLine(j)',
        f"            else if j == f + {TIP_HEAD}",
        f'                s := s + "\\n... " + str.tostring(n - {cap}) + " more events ..."',
        '    s + "\\n" + exitWord(array.get(trExitKind, c))',
        "",
        "// entry marker hover: 'T3 SELL #1038847936 · entry 13 Nov 21:40 @ 1.10400', orders, events, result",
        "entryTip(int c) =>",
        '    tradeTip(c, array.get(trHead, c) + " · " + entryLine(c))',
        "",
        "// exit marker hover: 'T3 exit 14 Nov 03:10 @ 1.10120 · SL hit' ('T3 still open · at export <time>')",
        "exitTip(int c) =>",
        "    int k = array.get(trExitKind, c)",
        "    string nm = array.get(trNo, c)",
        "    string tt = fmtT(array.get(trExitT, c))",
        '    string at = array.get(trExitHasP, c) == 1 ? " @ " + fmtP(array.get(trExitP, c)) : '
        '" @ price not recorded"',
        f'    k == {EXIT_OPEN} ? nm + " " + exitWord(k) + " · at export " + tt : k == {EXIT_UNKNOWN} ? nm + " " + '
        'exitWord(k) + " · last seen " + tt : nm + " exit " + tt + at + " · " + exitWord(k)',
        "",
        "// the exit marker's price: the exit price, else the entry price",
        "exitY(int c) =>",
        "    array.get(trExitHasP, c) == 1 or array.get(trEntryHasP, c) == 0 ? array.get(trExitP, c) : "
        "array.get(trEntryP, c)",
        "",
        "// table cells: '00:46 @ 1.10553' (time only; the tooltip, entryLine / exitText, has the date)",
        "entryCell(int c) =>",
        "    int k = array.get(trEntryKind, c)",
        '    string at = array.get(trEntryHasP, c) == 1 ? " @ " + fmtP(array.get(trEntryP, c)) : " @ -"',
        f'    k == {ENTRY_NOT_RECORDED} ? "time n/r" + at : (k == {ENTRY_FILLED} ? "" : "seen ") + '
        "fmtC(array.get(trEntryT, c)) + at",
        "",
        "exitCell(int c) =>",
        "    int k = array.get(trExitKind, c)",
        '    string at = array.get(trExitHasP, c) == 1 ? " @ " + fmtP(array.get(trExitP, c)) : " @ -"',
        f'    k == {EXIT_OPEN} or k == {EXIT_UNKNOWN} ? "-" : fmtC(array.get(trExitT, c)) + at',
        "",
        "// Result (R): '' when it can be measured, else what the cell shows instead (open: no unrealised result)",
        "resWhy(int c) =>",
        "    float a = array.get(trAtr, c)",
        "    int st = array.get(trAtrAt, c)",
        "    int xk = array.get(trExitKind, c)",
        f"    xk == {EXIT_OPEN} ? {pine_str(RES_OPEN)} : array.get(trEntryKind, c) == {ENTRY_NOT_RECORDED} ? "
        f"{pine_str(NA_ENTRY_TIME)} : array.get(trEntryHasP, c) == 0 ? {pine_str(NA_ENTRY_PRICE)} : "
        f"xk == {EXIT_UNKNOWN} or array.get(trExitHasP, c) == 0 ? {pine_str(NA_EXIT_PRICE)} : "
        f'not atrTfOk ? "n/a (chart > " + atrTfTxt + ")" : st == {ATR_BEFORE_HISTORY} ? '
        f"{pine_str(NA_BEFORE_HISTORY)} : st == {ATR_NOT_REACHED} ? {pine_str(NA_NOT_REACHED)} : na(a) or a <= 0 ? "
        f'{pine_str(NA_NO_ATR)} : ""',
        "",
        "// 1R in price: the ATR before the entry / pipSz, nearest 0.5 with halves up (4.2 -> 4, 4.3 -> 4.5),",
        "// clamped to [rMin, rMax], times pipSz",
        f"// (+ {ROUND_EPS_PINE} in both: an exact half that float arithmetic puts just below still rounds up)",
        "oneR(int c) =>",
        "    math.max(rMin, math.min(rMax, math.floor(array.get(trAtr, c) / pipSz * 2 + 0.5 + "
        f"{ROUND_EPS_PINE}) / 2.0)) * pipSz",
        "",
        "// (exit - entry) x direction / 1R (BUY +1, SELL -1), rounded to 0.01 with halves up",
        "resR(int c) =>",
        "    float dir = array.get(trSide, c) == 1 ? 1.0 : -1.0",
        "    math.floor((array.get(trExitP, c) - array.get(trEntryP, c)) * dir / oneR(c) * 100 + 0.5 + "
        f"{ROUND_EPS_PINE}) / 100.0",
        "",
        "// '+1.50R', '-0.12R', '0.00R'",
        "fmtRes(float r) =>",
        '    (r > 0 ? "+" : "") + str.tostring(r, "0.00") + "R"',
        "",
        "// '6', '6.5': the clamp bounds in the tooltip",
        "fmtNum(float v) =>",
        '    v == math.floor(v) ? str.tostring(v, "0") : str.tostring(v, "0.0")',
        "",
        "resCell(int c) =>",
        "    string why = resWhy(c)",
        "    str.length(why) > 0 ? why : fmtRes(resR(c))",
        "",
        "// green above 0, red below, the text colour at 0 and when there is no result",
        "resCol(int c) =>",
        "    float r = str.length(resWhy(c)) > 0 ? 0.0 : resR(c)",
        "    r > 0 ? C_REWARD : r < 0 ? C_RISK : C_TEXT",
        "",
        "// 'result -0.12R; 1R from 15m ATR(14), clamped 6-14; exit: closed'",
        "resTip(int c) =>",
        "    string why = resWhy(c)",
        "    int ek = array.get(trEntryKind, c)",
        '    string ex = "; exit: " + exitWord(array.get(trExitKind, c))',
        f'    string seen = ek == {ENTRY_SEEN} or ek == {ENTRY_BEFORE_WINDOW} ? "\\nentry time = first seen open" : ""',
        f"    str.length(why) > 0 ? (why == {pine_str(RES_OPEN)} ? {pine_str(RES_OPEN_TIP)} : why) + ex : "
        '"result " + fmtRes(resR(c)) + "; 1R from " + atrTfTxt + " ATR(" + str.tostring(atrLen) + "), clamped " + '
        'fmtNum(rMin) + "-" + fmtNum(rMax) + ex + seen',
        "",
        "pendTip(int k) =>",
        f'    string s = array.get(pdHead, k) + (array.get(pdKind, k) == {PD_FILLED} ? "" : " pending order")',
        "    string lg = array.get(pdLegs, k)",
        "    if str.length(lg) > 0",
        '        s := s + "\\n" + lg',
        '    s := s + "\\n" + (array.get(pdPlaced, k) == 1 ? "placed " : "first seen ") + '
        'fmtT(array.get(pdT1, k)) + " @ " + fmtP(array.get(pdP, k))',
        "    if array.get(pdSlHas, k) == 1",
        '        s := s + "\\nSL " + fmtP(array.get(pdSl, k))',
        "    if array.get(pdTpHas, k) == 1",
        '        s := s + "\\nTP " + fmtP(array.get(pdTp, k))',
        '    s + "\\n" + pendWord(array.get(pdKind, k)) + " " + fmtT(array.get(pdT2, k))',
        "",
        "var bool drawn = false",
        "if not drawn and (barstate.islastconfirmedhistory or barstate.islast)",
        "    drawn := true",
        "    if symbolOk",
        "        int barMs = timeframe.in_seconds() * 1000",
        "        int minW = minBars * barMs",
        "        // each trade's drawn span: entry to exit, at least minBars wide (a trade whose entry time is not",
        "        // recorded ends at its exit; a short one is widened to the right)",
        "        int[] trL = array.new_int(0)",
        "        int[] trR = array.new_int(0)",
        "        if array.size(trEntryT) > 0",
        "            for c = 0 to array.size(trEntryT) - 1",
        "                int l = array.get(trEntryT, c)",
        "                int r = array.get(trExitT, c)",
        "                if r - l < minW",
        f"                    if array.get(trEntryKind, c) == {ENTRY_NOT_RECORDED}",
        "                        l := r - minW",
        "                    else",
        "                        r := l + minW",
        "                array.push(trL, l)",
        "                array.push(trR, r)",
        "        // text on this chart's scale (Minimal tags, exit text, Detailed layout): a text line is lineH high (an",
        "        // input in ticks or, 0 = auto, the chart's ATR), a character charMs wide (an input in bars)",
        "        float lineH = spacingTicks > 0 ? spacingTicks * syminfo.mintick : nz(atrNow, 20 * syminfo.mintick)",
        "        float charMs = charBars * barMs",
        f'        bool tagFar = tagCorner == "{TAG_CORNERS[1]}"',
        "        float[] tgL = array.new_float(0)",
        "        float[] tgR = array.new_float(0)",
        "        float[] tgLo = array.new_float(0)",
        "        float[] tgHi = array.new_float(0)",
        "        // zones (entry to level: fill and a 1 px border) and the level lines, one per step; in Minimal mode",
        "        // the trade's tag zone carries 'T3 SELL' as box text at a corner: the preferred end (input) on the",
        "        // left, else the first corner of the zone (left: other end; right: preferred, other) whose text box",
        "        // no earlier tag's covers, else the preferred one; the tag never leaves its own zone",
        "        int[] tagged = array.new_int(array.size(trEntryT), 0)",
        "        if array.size(stTr) > 0",
        "            for i = 0 to array.size(stTr) - 1",
        "                int c = array.get(stTr, i)",
        "                int leg = array.get(stLeg, i)",
        "                float p = array.get(stP, i)",
        f"                if leg == {LEG_WORK}",
        "                    if showWorking",
        "                        line.new(array.get(stX1, i), p, array.get(stX2, i), p, xloc = xloc.bar_time, "
        "color = color.new(sideLine(array.get(trSide, c)), 50), style = line.style_dashed, width = 1)",
        "                else",
        "                    int s = array.get(stX1, i)",
        "                    int e = array.get(stX2, i)",
        "                    int x1 = s <= array.get(trEntryT, c) ? array.get(trL, c) : s",
        "                    int x2 = e >= array.get(trExitT, c) ? array.get(trR, c) : e",
        f"                    color zc = leg == {LEG_SL} ? C_RISK : C_REWARD",
        f"                    int fillT = leg == {LEG_SL} ? riskFillT : rewardFillT",
        f"                    int edgeT = leg == {LEG_SL} ? riskEdgeT : rewardEdgeT",
        "                    if x2 > x1",
        "                        if showZones and array.get(trEntryHasP, c) == 1",
        "                            float ep = array.get(trEntryP, c)",
        "                            float zTop = math.max(p, ep)",
        "                            float zBot = math.min(p, ep)",
        "                            bool tagHere = minimal and showLabels and array.get(trTagSt, c) == i",
        '                            string tagTxt = ""',
        "                            string tagH = text.align_left",
        "                            string tagV = text.align_top",
        "                            if tagHere",
        f"                                float tagW = (str.length(array.get(trTag, c)) + {PAD_CHARS}) * charMs",
        f"                                float tagHt = (1 + {PAD_LINES}) * lineH",
        "                                // corner q % 4: 0 / 1 left, 2 / 3 right, even: the preferred end; q = 4: none",
        "                                // is free, back to corner 0",
        "                                for q = 0 to 4",
        "                                    int cq = q % 4",
        "                                    bool atTop = (p < ep) == ((cq % 2 == 0) != tagFar)",
        "                                    float l0 = cq >= 2 ? x2 - tagW : x1 * 1.0",
        "                                    float hi0 = atTop ? zTop : zBot + tagHt",
        "                                    bool free = true",
        "                                    if q < 4 and array.size(tgL) > 0",
        "                                        for u = 0 to array.size(tgL) - 1",
        "                                            if array.get(tgL, u) < l0 + tagW and array.get(tgR, u) > l0 and "
        "array.get(tgLo, u) < hi0 and array.get(tgHi, u) > hi0 - tagHt",
        "                                                free := false",
        "                                                break",
        "                                    if free",
        "                                        array.push(tgL, l0)",
        "                                        array.push(tgR, l0 + tagW)",
        "                                        array.push(tgLo, hi0 - tagHt)",
        "                                        array.push(tgHi, hi0)",
        "                                        tagTxt := array.get(trTag, c)",
        "                                        tagH := cq >= 2 ? text.align_right : text.align_left",
        "                                        tagV := atTop ? text.align_top : text.align_bottom",
        "                                        array.set(tagged, c, 1)",
        "                                        break",
        "                            box.new(x1, zTop, x2, zBot, xloc = xloc.bar_time, "
        "bgcolor = color.new(zc, fillT), border_color = color.new(zc, edgeT), border_width = 1, "
        "border_style = line.style_solid, text = tagTxt, text_size = size.small, "
        "text_color = C_TEXT, text_halign = tagH, text_valign = tagV)",
        "                        if showLevels",
        "                            line.new(x1, p, x2, p, xloc = xloc.bar_time, color = zc, "
        "style = line.style_dotted, width = 1)",
        "        // Minimal: a trade whose tag no zone carries gets a thin transparent box around its entry line",
        "        if minimal and showLabels and array.size(trEntryT) > 0",
        "            for c = 0 to array.size(trEntryT) - 1",
        "                if array.get(tagged, c) == 0",
        "                    float ty = array.get(trEntryHasP, c) == 1 ? array.get(trEntryP, c) : array.get(trExitP, c)",
        f"                    float th = {TAG_TICKS} * syminfo.mintick",
        "                    box.new(array.get(trL, c), ty + th, array.get(trR, c), ty - th, xloc = xloc.bar_time, "
        "bgcolor = color.new(C_EXIT, 100), border_color = color.new(C_EXIT, 100), border_width = 1, "
        "text = array.get(trTag, c), text_size = size.small, text_color = C_TEXT, "
        "text_halign = text.align_left, text_valign = text.align_top)",
        "        // entry line and exit segment (the markers are drawn last)",
        "        if array.size(trEntryT) > 0",
        "            for c = 0 to array.size(trEntryT) - 1",
        "                float ep = array.get(trEntryP, c)",
        "                if array.get(trEntryHasP, c) == 1",
        "                    line.new(array.get(trL, c), ep, array.get(trR, c), ep, xloc = xloc.bar_time, "
        "color = sideLine(array.get(trSide, c)), style = line.style_solid, width = 2)",
        "                int k = array.get(trExitKind, c)",
        f"                if showMarkers and k <= {EXIT_CLOSED}",
        "                    int xt = array.get(trExitT, c)",
        "                    bool hasX = array.get(trExitHasP, c) == 1",
        "                    float xp = hasX ? array.get(trExitP, c) : ep",
        "                    if hasX and array.get(trR, c) > xt",
        "                        line.new(xt, xp, array.get(trR, c), xp, xloc = xloc.bar_time, color = exitColor(k), "
        "style = line.style_dashed, width = 1)",
        "        // pending-only orders: faint grey dashed line, a tiny mark with everything in its tooltip",
        "        if showPending",
        "            if array.size(psX1) > 0",
        "                for i = 0 to array.size(psX1) - 1",
        "                    line.new(array.get(psX1, i), array.get(psP, i), array.get(psX2, i), array.get(psP, i), "
        "xloc = xloc.bar_time, color = color.new(C_PENDING, 40), style = line.style_dashed, width = 1)",
        "            if array.size(pdT2) > 0",
        "                for k = 0 to array.size(pdT2) - 1",
        "                    int pk = array.get(pdKind, k)",
        "                    label.new(array.get(pdT2, k), array.get(pdP, k), \"\", xloc = xloc.bar_time, "
        f"color = color.new(C_PENDING, 20), style = pk == {PD_WORKING} ? label.style_circle : "
        f"pk == {PD_FILLED} ? label.style_diamond : label.style_xcross, size = size.tiny, tooltip = pendTip(k))",
        "        // Detailed: text labels, entry (left edge) and exit (right edge), laid out below; change text (input,",
        "        // laid out in Detailed, at its own time and price in Minimal); dots without text otherwise",
        "        int[] lbKind = array.new_int(0)",
        "        int[] lbRef = array.new_int(0)",
        "        int[] lbX = array.new_int(0)",
        "        float[] lbY = array.new_float(0)",
        "        int[] lbMode = array.new_int(0)",
        "        int[] lbDir = array.new_int(0)",
        "        float[] lbKey = array.new_float(0)",
        "        if showLabels and not minimal and array.size(trEntryT) > 0",
        "            for c = 0 to array.size(trEntryT) - 1",
        "                bool hasE = array.get(trEntryHasP, c) == 1",
        "                bool hasX = array.get(trExitHasP, c) == 1",
        "                float ey = hasE ? array.get(trEntryP, c) : array.get(trExitP, c)",
        "                float xy = hasX ? array.get(trExitP, c) : ey",
        "                array.push(lbKey, array.get(trL, c) * 1000.0 + array.size(lbKind))",
        "                array.push(lbKind, 1)",
        "                array.push(lbRef, c)",
        "                array.push(lbX, array.get(trL, c))",
        "                array.push(lbY, ey)",
        "                array.push(lbMode, -1)",
        "                array.push(lbDir, array.get(trSide, c) == 1 ? -1 : 1)",
        "                array.push(lbKey, array.get(trR, c) * 1000.0 + array.size(lbKind))",
        "                array.push(lbKind, 2)",
        "                array.push(lbRef, c)",
        "                array.push(lbX, array.get(trR, c))",
        "                array.push(lbY, xy)",
        "                array.push(lbMode, 1)",
        "                array.push(lbDir, xy >= ey ? 1 : -1)",
        "        if showAdjust and array.size(adjEv) > 0",
        "            for i = 0 to array.size(adjEv) - 1",
        "                int e = array.get(adjEv, i)",
        "                int c = array.get(evTr, e)",
        "                float p = array.get(evP, e)",
        "                if adjText and not minimal",
        "                    array.push(lbKey, array.get(evT, e) * 1000.0 + array.size(lbKind))",
        "                    array.push(lbKind, 3)",
        "                    array.push(lbRef, e)",
        "                    array.push(lbX, array.get(evT, e))",
        "                    array.push(lbY, p)",
        "                    array.push(lbMode, 1)",
        "                    array.push(lbDir, p >= array.get(trEntryP, c) ? 1 : -1)",
        "                else if adjText",
        "                    label.new(array.get(evT, e), p, markerText(e), xloc = xloc.bar_time, color = C_BG, "
        "style = label.style_label_left, textcolor = evColor(array.get(evCode, e)), size = size.small, "
        'textalign = text.align_left, tooltip = array.get(trHead, c) + ": " + evLine(e))',
        "                else",
        "                    label.new(array.get(evT, e), p, \"\", xloc = xloc.bar_time, "
        "color = evColor(array.get(evCode, e)), style = label.style_circle, size = size.tiny, "
        'tooltip = array.get(trHead, c) + ": " + evLine(e))',
        "        // Detailed layout on this chart's scale (src.reports.pinescript.layout is the same steps): a box is",
        "        // (longest line + pad) characters wide on its side of its time, (lines + pad) text lines high around",
        "        // its price; one that would cover a box already drawn moves past it, up or down; one that would move",
        f"        // more than {MAX_SHIFT_HEIGHTS} of its own heights is not drawn (its marker's tooltip tells it)",
        f"        float gap = {GAP_LINES} * lineH",
        "        float[] pX = array.new_float(0)",
        "        float[] pC = array.new_float(0)",
        "        float[] pW = array.new_float(0)",
        "        float[] pLo = array.new_float(0)",
        "        float[] pHi = array.new_float(0)",
        "        float wMax = 0.0",
        f"        int budget = {LAYOUT_BUDGET}",
        "        if array.size(lbKind) > 0",
        "            int[] lbOrd = array.sort_indices(lbKey, order.ascending)",
        "            for m0 = 0 to array.size(lbOrd) - 1",
        "                int i = array.get(lbOrd, m0)",
        "                int kd = array.get(lbKind, i)",
        "                int rf = array.get(lbRef, i)",
        "                int c = kd == 3 ? array.get(evTr, rf) : rf",
        "                string txt = kd == 1 ? entryText(rf) : kd == 2 ? exitText(rf) : markerText(rf)",
        '                string[] rows = str.split(txt, "\\n")',
        "                int nChars = 0",
        "                for r = 0 to array.size(rows) - 1",
        "                    nChars := math.max(nChars, str.length(array.get(rows, r)))",
        "                int mode = array.get(lbMode, i)",
        "                int up = array.get(lbDir, i)",
        "                float x = array.get(lbX, i)",
        f"                float w = (nChars + {PAD_CHARS}) * charMs",
        f"                float h = (array.size(rows) + {PAD_LINES}) * lineH",
        "                float cx = x + mode * w / 2",
        "                float y0 = array.get(lbY, i)",
        "                float lo = y0 - h / 2",
        "                float hi = y0 + h / 2",
        "                float[] nLo = array.new_float(0)",
        "                float[] nHi = array.new_float(0)",
        "                int j = array.size(pX) - 1",
        "                while j >= 0 and budget > 0",
        "                    if x - array.get(pX, j) >= w + wMax",
        "                        break",
        "                    budget -= 1",
        "                    if math.abs(cx - array.get(pC, j)) * 2 < w + array.get(pW, j)",
        "                        array.push(nLo, array.get(pLo, j))",
        "                        array.push(nHi, array.get(pHi, j))",
        "                    j -= 1",
        "                if array.size(nLo) > 0",
        "                    int[] idx = up > 0 ? array.sort_indices(nLo, order.ascending) : "
        "array.sort_indices(nHi, order.descending)",
        "                    for m = 0 to array.size(idx) - 1",
        "                        int q = array.get(idx, m)",
        "                        if array.get(nLo, q) < hi and array.get(nHi, q) > lo",
        "                            if up > 0",
        "                                lo := array.get(nHi, q) + gap",
        "                                hi := lo + h",
        "                            else",
        "                                hi := array.get(nLo, q) - gap",
        "                                lo := hi - h",
        "                float yy = (lo + hi) / 2",
        f"                if math.abs(yy - y0) <= {MAX_SHIFT_HEIGHTS} * h",
        "                    array.push(pX, x)",
        "                    array.push(pC, cx)",
        "                    array.push(pW, w)",
        "                    array.push(pLo, lo)",
        "                    array.push(pHi, hi)",
        "                    wMax := math.max(wMax, w)",
        '                    string tip = kd == 3 ? array.get(trHead, c) + ": " + evLine(rf) : '
        "tradeTip(c, array.get(trHead, c))",
        "                    color fg = kd == 3 ? evColor(array.get(evCode, rf)) : C_TEXT",
        "                    label.new(array.get(lbX, i), yy, txt, xloc = xloc.bar_time, color = C_BG, "
        "style = mode > 0 ? label.style_label_left : label.style_label_right, textcolor = fg, "
        "size = size.small, textalign = text.align_left, tooltip = tip)",
        "        // markers that would meet (anchor within a text width and a text line of an earlier group's first",
        "        // marker) form a group; every marker stays exactly on its own anchor. Exit: the group's first marker",
        "        // carries all its trade numbers ('T1 T2'), the others no text, and every tooltip lists the group's",
        "        // exits. Entry: the tooltip names the trades whose entries share the spot.",
        "        int nTr = array.size(trEntryT)",
        "        int[] xHead = array.new_int(nTr, -1)",
        '        string[] xTxt = array.new_string(nTr, "")',
        '        string[] xTip = array.new_string(nTr, "")',
        "        int[] eHead = array.new_int(nTr, -1)",
        "        int[] eAt = array.new_int(nTr, 0)",
        "        int[] eCnt = array.new_int(nTr, 0)",
        '        string[] eNames = array.new_string(nTr, "")',
        "        if showMarkers and nTr > 0",
        "            for c = 0 to nTr - 1",
        f"                if array.get(trExitKind, c) <= {EXIT_CLOSED}",
        "                    int xt = array.get(trExitT, c)",
        "                    int hd = c",
        "                    if c > 0",
        "                        for g = 0 to c - 1",
        f"                            float gw = (str.length(array.get(trNo, g)) + {PAD_CHARS}) * charMs",
        "                            if array.get(xHead, g) == g and math.abs(xt - array.get(trExitT, g)) < gw and "
        "math.abs(exitY(c) - exitY(g)) < lineH",
        "                                hd := g",
        "                                break",
        "                    array.set(xHead, c, hd)",
        '                    array.set(xTxt, hd, array.get(xTxt, hd) + (hd == c ? "" : " ") + array.get(trNo, c))',
        '                    array.set(xTip, hd, array.get(xTip, hd) + (hd == c ? "" : "\\n") + exitTip(c))',
        "                if array.get(trEntryHasP, c) == 1",
        f"                    int et = array.get(trEntryKind, c) == {ENTRY_NOT_RECORDED} ? array.get(trL, c) : "
        "array.get(trEntryT, c)",
        "                    array.set(eAt, c, et)",
        "                    int eh = c",
        "                    if c > 0",
        "                        for g = 0 to c - 1",
        f"                            if array.get(eHead, g) == g and math.abs(et - array.get(eAt, g)) < "
        f"({MARK_CHARS} + {PAD_CHARS}) * charMs and "
        "math.abs(array.get(trEntryP, c) - array.get(trEntryP, g)) < lineH",
        "                                eh := g",
        "                                break",
        "                    array.set(eHead, c, eh)",
        "                    array.set(eCnt, eh, array.get(eCnt, eh) + 1)",
        '                    array.set(eNames, eh, array.get(eNames, eh) + (eh == c ? "" : ", ") + array.get(trTag, c))',
        "        // markers last, exactly on their anchors: entry (triangle) at the entry time and price; exit at the",
        "        // exit time and price (x-cross SL hit, diamond TP hit, circle closed) with its group's trade numbers",
        "        // as its own text; an open trade gets an open circle at its right edge",
        "        if showMarkers and nTr > 0",
        "            for c = 0 to nTr - 1",
        "                int sd = array.get(trSide, c)",
        "                bool hasE = array.get(trEntryHasP, c) == 1",
        "                if hasE",
        "                    int eh = array.get(eHead, c)",
        '                    string tipE = array.get(eCnt, eh) > 1 ? entryTip(c) + "\\nentries here: " + '
        "array.get(eNames, eh) : entryTip(c)",
        '                    label.new(array.get(eAt, c), array.get(trEntryP, c), "", xloc = xloc.bar_time, '
        "color = sideLine(sd), style = sd == 1 ? label.style_triangleup : label.style_triangledown, "
        "size = size.tiny, tooltip = tipE)",
        "                int k = array.get(trExitKind, c)",
        "                float ep = hasE ? array.get(trEntryP, c) : array.get(trExitP, c)",
        f"                if k <= {EXIT_CLOSED}",
        "                    int hd = array.get(xHead, c)",
        '                    label.new(array.get(trExitT, c), exitY(c), hd == c ? array.get(xTxt, c) : "", '
        f"xloc = xloc.bar_time, color = exitColor(k), style = k == {EXIT_SL} ? label.style_xcross : "
        f"k == {EXIT_TP} ? label.style_diamond : label.style_circle, textcolor = C_TEXT, size = size.tiny, "
        "tooltip = array.get(xTip, hd))",
        "                else",
        f'                    label.new(array.get(trR, c), ep, "{OPEN_MARK}", xloc = xloc.bar_time, '
        "color = color.new(C_EXIT, 100), style = label.style_none, textcolor = C_EXIT, size = size.small, "
        "tooltip = exitTip(c))",
        "    if showTable",
        "        int nRows = symbolOk ? math.min(rowsIn, array.size(trNewest)) : 0",
        f"        table tb = table.new(tblPos, {len(TABLE_COLUMNS)}, {hdr} + math.max(nRows, 1), "
        "bgcolor = C_BG, frame_color = C_PENDING, frame_width = 1, border_color = color.new(C_PENDING, 70), "
        "border_width = 1)",
        "        // title (the counts, time zone and symbol check in its tooltip), the window, the column titles",
        f"        table.cell(tb, 0, 0, {pine_str(title)}, text_color = {'C_RISK' if left else 'C_TEXT'}, "
        f"text_size = tblSize, text_halign = text.align_left, tooltip = {title_tip} + "
        '"\\ntime zone " + tzName + "\\nchart symbol " + chartSym + '
        '(not symbolOk ? " (mismatch)" : anySymbol ? " (any symbol on)" : " (match)"))',
        f"        table.merge_cells(tb, 0, 0, {last_col}, 0)",
        f"        table.cell(tb, 0, 1, {pine_str(window)}, text_color = C_TEXT, text_size = tblSize, "
        "text_halign = text.align_left)",
        f"        table.merge_cells(tb, 0, 1, {last_col}, 1)",
        *[f"        table.cell(tb, {i}, {hdr - 1}, {pine_str(h)}, text_color = C_TEXT, text_size = tblSize)"
          for i, h in enumerate(TABLE_COLUMNS)],
        "        if nRows == 0",
        f'            table.cell(tb, 0, {hdr}, symbolOk ? "no trades in the window" : "chart symbol mismatch: " + '
        'chartSym, text_color = symbolOk ? C_TEXT : C_RISK, text_size = tblSize, text_halign = text.align_left, '
        'tooltip = symbolOk ? "" : "nothing is drawn: set Chart symbol override to " + DATA_SYMBOL + '
        '" or tick Draw on any symbol")',
        f"            table.merge_cells(tb, 0, {hdr}, {last_col}, {hdr})",
        "        else",
        "            for r = 0 to nRows - 1",
        "                int c = array.get(trNewest, r)",
        f"                int row = {hdr} + r",
        "                table.cell(tb, 0, row, array.get(trNo, c), text_color = C_TEXT, text_size = tblSize, "
        'text_halign = text.align_left, tooltip = array.get(trHead, c) + "\\n" + array.get(trOrders, c))',
        "                table.cell(tb, 1, row, entryCell(c), text_color = C_TEXT, text_size = tblSize, "
        "text_halign = text.align_left, tooltip = entryLine(c))",
        "                table.cell(tb, 2, row, exitCell(c), text_color = C_TEXT, text_size = tblSize, "
        "text_halign = text.align_left, tooltip = exitText(c))",
        "                table.cell(tb, 3, row, resCell(c), text_color = resCol(c), text_size = tblSize, "
        "tooltip = resTip(c))",
    ]
    return "\n".join(head + [""] + body) + "\n"


# ------------------------------------------------------------------------------------------------ render
def order_trades_newest_first(trades: Iterable[Trade]) -> list[Trade]:
    return sorted(trades, key=lambda t: (-t.entry_ms, t.key))


def order_pending_newest_first(pending: Iterable[PendingOrder]) -> list[PendingOrder]:
    return sorted(pending, key=lambda p: (-p.end_ms, p.key))


def _select(trades: Sequence[Trade], pending: Sequence[PendingOrder], since_ms: int, limits: RenderLimits
            ) -> tuple[list[TradeEls], list[PendingEls], int, int]:
    """Newest first: whole trades while they fit the line / label / box budgets, stopping at the first that does
    not (everything kept is newer than everything left out); then pending orders with what is left. Returns
    (trades kept newest first, pending kept newest first, undrawable trades, undrawable pending orders)."""
    kept: list[TradeEls] = []
    lines = labels = boxes = undrawable = 0
    for t in trades:
        if not drawable(t):
            undrawable += 1
            continue
        te = trade_elements(t, since_ms, limits.max_steps_per_leg)
        if (lines + te.lines > limits.max_lines or labels + te.labels > limits.max_labels
                or boxes + te.boxes > limits.max_boxes):
            break
        kept.append(te)
        lines, labels, boxes = lines + te.lines, labels + te.labels, boxes + te.boxes
    kept_p: list[PendingEls] = []
    undrawable_p = 0
    for p in pending:
        pe = pending_elements(p)
        if pe.labels == 0:
            undrawable_p += 1
            continue
        if lines + pe.lines > limits.max_lines or labels + pe.labels > limits.max_labels:
            break
        kept_p.append(pe)
        lines, labels = lines + pe.lines, labels + pe.labels
    return kept, kept_p, undrawable, undrawable_p


def render_symbol(symbol: str, trades: Sequence[Trade], pending: Sequence[PendingOrder], info: RenderInfo,
                  limits: RenderLimits = RenderLimits()) -> RenderResult:
    """One indicator for `symbol` from its trades and pending orders (any order; sorted newest first here)."""
    ordered = order_trades_newest_first(trades)
    ordered_p = order_pending_newest_first(pending)
    kept, kept_p, undrawable, undrawable_p = _select(ordered, ordered_p, info.since_ms, limits)
    while True:
        chrono = sorted(kept, key=lambda te: (te.trade.entry_ms, te.trade.key))     # oldest entry first
        chrono_p = sorted(kept_p, key=lambda pe: (pe.order.end_ms, pe.order.key))
        counts = _Counts(trades=len(ordered), pending=len(ordered_p),
                         left_out=len(ordered) - undrawable - len(kept),
                         left_out_pending=len(ordered_p) - undrawable_p - len(kept_p),
                         trimmed=sum(te.trimmed for te in kept), undrawable=undrawable)
        text = _script(symbol, info, chrono, chrono_p, counts)
        if len(text) <= limits.max_chars or not (kept or kept_p):
            break
        if kept_p:
            kept_p.pop()                  # drop the oldest kept pending order first, then the oldest trade
        else:
            kept.pop()
    return RenderResult(
        symbol=symbol, text=text, orders=info.orders, positions=info.positions, trades=len(ordered),
        pending=len(ordered_p), drawn=tuple(te.trade.key for te in chrono),
        drawn_pending=tuple(pe.order.key for pe in chrono_p), left_out=counts.left_out,
        left_out_pending=counts.left_out_pending,
        lines=sum(te.lines for te in chrono) + sum(pe.lines for pe in chrono_p),
        labels=sum(te.labels for te in chrono) + sum(pe.labels for pe in chrono_p),
        boxes=sum(te.boxes for te in chrono), trimmed_steps=counts.trimmed,
        events=sum(len(te.trade.events) for te in chrono), undrawable=undrawable)
