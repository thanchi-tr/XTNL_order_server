"""Builders for trade tests: StateVersion rows, DTO -> row emulation of the read's SQL, a small Pine lint (balanced
brackets, indentation, line length, allow-listed v6 functions, declared names) and an emulator of what the script
draws."""
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from src.cache.codec import dto_dict
from src.dto import OrderDTO, PositionDTO
from src.reports.lifecycle import StateVersion

T0 = 1_794_000_000_000          # a fixed epoch-ms base (2026-11)
H = 3_600_000
NOW = T0 + 5 * 24 * H

_seq = [0]


def d(s: str) -> Decimal:
    return Decimal(s)


def order(oid: str, t: int, status: str, price: str | None, *, side: str = "BUY", symbol: str = "EURUSD",
          adapter: str = "stonex", sl: str | None = None, tp: str | None = None, reduce_only: bool = False,
          linked: str | None = None, client: str | None = None, obs: int | None = None, seq: int | None = None,
          commit: int | None = None, broker_seq: int | None = None, source: str = "STREAM",
          account: str = "ACC") -> StateVersion:
    _seq[0] += 1
    return StateVersion(
        kind="ORDER", adapter_id=adapter, entity_id=oid, account_id=account, symbol=symbol,
        side="BUY" if side == "BUY" else "SELL", status=status, price=None if price is None else d(price),
        stop_loss=None if sl is None else d(sl), take_profit=None if tp is None else d(tp),
        is_reduce_only=reduce_only, linked_position_id=linked, client_id=client, broker_seq=broker_seq,
        broker_ms=t, observed_seq=obs if obs is not None else _seq[0],
        seq_id=seq if seq is not None else ((commit or t) << 20) + _seq[0], commit_ms=commit or t, source=source)


def position(pid: str, t: int, status: str, entry: str | None, *, side: str = "BUY", symbol: str = "EURUSD",
             adapter: str = "stonex", sl: str | None = None, tp: str | None = None, client: str | None = None,
             obs: int | None = None, seq: int | None = None, commit: int | None = None, source: str = "STREAM",
             account: str = "ACC") -> StateVersion:
    _seq[0] += 1
    return StateVersion(
        kind="POSITION", adapter_id=adapter, entity_id=pid, account_id=account, symbol=symbol,
        side="BUY" if side == "BUY" else "SELL", status=status, price=None if entry is None else d(entry),
        stop_loss=None if sl is None else d(sl), take_profit=None if tp is None else d(tp), client_id=client,
        broker_ms=t, observed_seq=obs if obs is not None else _seq[0],
        seq_id=seq if seq is not None else ((commit or t) << 20) + _seq[0], commit_ms=commit or t, source=source)


def _s(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def log_row(dto: OrderDTO | PositionDTO, logged_ms: int) -> tuple[object, ...]:
    """What SQL_LOG_VERSIONS returns for this DTO's event_log payload (payload->>'key' per column)."""
    p = dto_dict(dto)
    kind = "ORDER" if isinstance(dto, OrderDTO) else "POSITION"
    return (kind, dto.adapter_id, dto.account_id, _s(p.get("broker_order_id") or p.get("position_id")),
            _s(p["symbol"]), _s(p["side"]), _s(p["status"]), _s(p["entry_price"]), _s(p["stop_loss"]),
            _s(p["take_profit"]), _s(p.get("is_reduce_only")), _s(p.get("linked_position_id")),
            _s(p.get("client_order_id") or p.get("opened_by_client_order_id")), _s(p["broker_seq"]),
            _s(p["broker_updated_at_ms"]), _s(p["observed_seq"]), str(dto.seq_id), str(logged_ms), _s(p["source"]))


# ------------------------------------------------------------------------------------------------ Pine lint
#: Every Pine v6 built-in the generated scripts may call (the script's own `name(...) =>` functions are allowed
#: too).
PINE_FUNCTIONS = frozenset({
    "indicator", "input.bool", "input.string", "input.int", "input.color", "str.upper", "str.replace_all",
    "str.length", "str.startswith", "str.format_time", "str.tostring", "color.new", "array.from", "array.new_int",
    "array.new_float", "array.new_string", "array.concat", "array.size", "array.get", "line.new", "label.new",
    "table.new", "table.cell", "table.merge_cells", "ta.atr", "nz", "math.min", "math.max",
    # runtime label layout
    "input.float", "str.split", "math.abs", "timeframe.in_seconds", "array.push", "array.sort_indices",
    # position-tool zones, the Minimal tag flags
    "box.new", "array.set",
    # marker groups
    "array.new_string",
    # Result (R) column: the ATR of the ATR-timeframe bar before the entry, 1R rounding and clamp, the symbol's pip
    "request.security", "input.timeframe", "math.floor", "math.round", "na", "math.log10",
})
#: Every built-in variable / constant namespace member the scripts may read.
PINE_CONSTANTS = frozenset({
    "color.white", "line.style_dashed", "line.style_solid", "line.style_dotted", "label.style_triangleup",
    "label.style_triangledown", "label.style_xcross", "label.style_label_down", "label.style_label_up",
    "label.style_diamond", "size.tiny", "size.small", "xloc.bar_time", "position.bottom_left",
    "position.bottom_center", "position.top_left", "position.top_center", "position.middle_left",
    "text.align_left", "text.align_right", "format.mintick", "barstate.islastconfirmedhistory",
    "barstate.islast", "barstate.isfirst", "syminfo.ticker", "syminfo.timezone", "syminfo.mintick",
    "size.normal", "order.ascending", "order.descending",
    "label.style_label_left", "label.style_label_right", "label.style_circle",
    # Minimal label mode: tag text inside the zone box, open-trade marker
    "text.align_top", "text.align_bottom", "label.style_none",
    # Result (R) column
    "syminfo.tickerid", "barmerge.lookahead_on", "timeframe.period", "syminfo.pricescale",
})
#: Built-in series variables the scripts may read bare.
PINE_VARIABLES = frozenset({"time", "time_close"})
_KEYWORDS = frozenset({"and", "or", "not", "if", "for", "to", "switch", "while", "else", "var", "break"})
# Every Pine Script v6 reserved word: none may be used as a variable or function name (CE10150 in the editor;
# found when the first generated script named an array `by`).
PINE_RESERVED = frozenset({"and", "by", "const", "else", "enum", "export", "false", "for", "if", "import", "in",
                           "method", "na", "not", "or", "series", "simple", "switch", "to", "true", "type", "var",
                           "varip", "while", "continue", "break"})
_STRING = re.compile(r'"(?:\\.|[^"\\])*"')
_HEX_COLOUR = re.compile(r"#[0-9A-Fa-f]{6}(?:[0-9A-Fa-f]{2})?\b")
_FUNC_DEF = re.compile(r"^(\w+)\((.*)\) =>")


def strip_code(text: str) -> list[str]:
    """Lines with string literals emptied, // comments removed and hex colour literals replaced by 0."""
    out = []
    for line in text.split("\n"):
        line = _STRING.sub('""', line).split("//", 1)[0]
        out.append(_HEX_COLOUR.sub("0", line))
    return out


def lint(text: str) -> list[str]:
    problems: list[str] = []
    lines = text.split("\n")
    if not lines or lines[0] != "//@version=6":
        problems.append("first line is not //@version=6")
    for n, raw in enumerate(lines, 1):
        if "\t" in raw:
            problems.append(f"{n}: tab")
        if len(raw) > 4000:
            problems.append(f"{n}: longer than 4000 characters")
        indent = len(raw) - len(raw.lstrip(" "))
        if raw.strip() and indent % 4:
            problems.append(f"{n}: indentation {indent} is not a multiple of 4")
    stripped = strip_code(text)
    own = {m.group(1) for line in stripped if (m := _FUNC_DEF.match(line))}   # the script's own functions
    for n, line in enumerate(stripped, 1):
        depth = {"(": 0, "[": 0}
        for ch in line:
            if ch in "([":
                depth[ch] += 1
            elif ch == ")":
                depth["("] -= 1
            elif ch == "]":
                depth["["] -= 1
            if depth["("] < 0 or depth["["] < 0:
                problems.append(f"{n}: closing bracket before opening")
                break
        if depth["("] or depth["["]:
            problems.append(f"{n}: unbalanced brackets")
        for m in re.finditer(r"(?<![\w.])([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*\(", line):
            name = m.group(1)
            if name not in PINE_FUNCTIONS and name not in _KEYWORDS and name not in own:
                problems.append(f"{n}: function {name} not in the allow-list")
        for m in re.finditer(r"(?<![\w.])([a-z]+\.[A-Za-z_]\w*)\b(?!\s*\()", line):
            name = m.group(1)
            if name not in PINE_CONSTANTS:
                problems.append(f"{n}: {name} not in the constant allow-list")
    problems += _undeclared(stripped)
    return problems


_TYPES = frozenset({"int", "float", "bool", "string", "color", "table", "line", "label", "const", "var"})
_LITERALS = frozenset({"true", "false", "na"})
_DECL = re.compile(r"^\s*(?:var\s+)?(?:const\s+)?(?:int|float|bool|string|color|table)(?:\[\])?\s+(\w+)\s*=")


def _undeclared(lines: list[str]) -> list[str]:
    """Every bare identifier read must be declared earlier (variable, parameter, loop variable) or be a keyword,
    a type, a literal, a namespace (`x.`), a function name (`f(`) or a named argument (`name = ` in a call)."""
    declared: set[str] = set()
    problems: list[str] = []
    for n, line in enumerate(lines, 1):
        m = _DECL.match(line)
        if m:
            declared.add(m.group(1))
            if m.group(1) in PINE_RESERVED:
                problems.append(f"{n}: {m.group(1)!r} is a Pine reserved word and cannot be a variable name")
        m = re.match(r"^\s*for (\w+) =", line)
        if m:
            declared.add(m.group(1))
        m = re.match(r"^(\w+)\((.*)\) =>", line)
        if m:
            declared.update(re.findall(r"\w+\s+(\w+)", m.group(2)))
        body = line[m.end(1):] if (m := _DECL.match(line)) else line
        for t in re.finditer(r"(?<![\w.])([A-Za-z_]\w*)\b(?!\s*[.(])(?!\s*=(?!=))", body):
            name = t.group(1)
            if name in declared or name in _KEYWORDS or name in _TYPES or name in _LITERALS or name in PINE_VARIABLES:
                continue
            problems.append(f"{n}: identifier {name} is not declared")
    return problems


def pine_arrays(text: str) -> dict[str, list[str]]:
    """name -> literal items of the embedded data arrays (array.from plus array.concat chunks)."""
    out: dict[str, list[str]] = {}
    for m in re.finditer(r"^\s*(?:var )?(?:int|float|string)\[\] (\w+) = array\.(?:from\((.*)\)|new_\w+\(0\))$",
                         text, re.M):
        out[m.group(1)] = _items(m.group(2)) if m.group(2) is not None else []
    for m in re.finditer(r"^\s*array\.concat\((\w+), array\.from\((.*)\)\)$", text, re.M):
        out[m.group(1)] += _items(m.group(2))
    return out


def _items(s: str) -> list[str]:
    items, cur, in_str, esc = [], "", False, False
    for ch in s:
        if in_str:
            cur += ch
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
            cur += ch
        elif ch == ",":
            items.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        items.append(cur.strip())
    return items


# ------------------------------------------------------------------------------------------------ Pine emulation
@dataclass
class Chart:
    """What the script draws with its default inputs (except the ones `emulate` takes), from its data arrays,
    mirroring its draw block: spans, zones (trade, leg, left, right, top, bottom), level lines, entry lines,
    Minimal tags (trade, left, right, top, bottom, text, vertical align, fallback box), entry markers (trade, time,
    price, 'up' / 'down', tooltip), exit markers (trade, time, price, exit kind, own text, tooltip; open trades
    too), exit segments, change dots, pending marks and lines, the text labels in drawing order as (time, price,
    push side, box side, kind 1 entry / 2 exit / 3 change, text, tooltip) (Detailed: before the layout, which
    `src.reports.pinescript.layout` mirrors; Minimal: change text only, on its own point) and the table rows."""

    spans: list[tuple[int, int]] = field(default_factory=list)
    zones: list[tuple[int, int, int, int, float, float]] = field(default_factory=list)
    levels: list[tuple[int, int, int, float]] = field(default_factory=list)
    working: list[tuple[int, int, int, float]] = field(default_factory=list)
    entry_lines: list[tuple[int, int, int, float]] = field(default_factory=list)
    tags: list[tuple[int, int, int, float, float, str, str, bool]] = field(default_factory=list)
    entries: list[tuple[int, int, float, str, str]] = field(default_factory=list)
    exits: list[tuple[int, int, float, int, str, str]] = field(default_factory=list)
    exit_segments: list[tuple[int, int, int, float]] = field(default_factory=list)
    dots: list[tuple[int, float, str]] = field(default_factory=list)
    pending: list[tuple[int, float, str]] = field(default_factory=list)
    pending_lines: list[tuple[int, int, float]] = field(default_factory=list)
    labels: list[tuple[int, float, int, int, int, str, str]] = field(default_factory=list)
    tag_align: list[str] = field(default_factory=list)          # per tag: 'left' / 'right'
    rows: list[tuple[str, str, str, str]] = field(default_factory=list)      # T#, Entry, Exit, Result (R)
    row_tips: list[str] = field(default_factory=list)
    entry_tips: list[str] = field(default_factory=list)         # per row: the Entry cell's tooltip (full time)
    exit_tips: list[str] = field(default_factory=list)          # per row: the Exit cell's tooltip (full time)
    result_tips: list[str] = field(default_factory=list)        # per row: the Result (R) cell's tooltip
    result_colours: list[str] = field(default_factory=list)     # per row: C_REWARD / C_RISK / C_TEXT


#: A chart bar as the capture pass sees it: (time, time_close, atrPrev: the ATR of the ATR-timeframe bar before the
#: one holding this bar, None = na).
Bar = tuple[int, int, float | None]


def capture_atr(entry_t: Sequence[int], bars: Sequence[Bar]) -> tuple[list[float | None], list[int]]:
    """The script's single pass (trades sorted by entry): on each bar, while the pointer's entry is before the bar's
    close, the trade takes this bar's atrPrev; on the first bar an entry before its open is marked before-history
    (2), else captured (1); a trade never reached stays 0. Returns (ATR per trade, state per trade)."""
    from src.reports import pinescript as ps

    if list(entry_t) != sorted(entry_t):
        raise ValueError("entries must be sorted")
    atr: list[float | None] = [None] * len(entry_t)
    state = [ps.ATR_NOT_REACHED] * len(entry_t)
    p = 0
    for b, (t, t_close, a) in enumerate(bars):
        while p < len(entry_t) and entry_t[p] < t_close:
            state[p] = ps.ATR_BEFORE_HISTORY if b == 0 and entry_t[p] < t else ps.ATR_CAPTURED
            atr[p] = a
            p += 1
    return atr, state


def tf_text(seconds: int, name: str = "") -> str:
    """The script's atrTfTxt: '15m', '1h', '1D', '30s'; weekly / monthly: the input's own name ('1W', '1M')."""
    if seconds >= 604800:
        return name
    for unit, name in ((86400, "D"), (3600, "h"), (60, "m")):
        if seconds % unit == 0:
            return f"{seconds // unit}{name}"
    return f"{seconds}s"


def emulate(text: str, *, mode: str = "Minimal", tz: str = "Australia/Melbourne", decimals: int = 5,
            bar_ms: int = 60_000, min_bars: int = 10, adj_text: bool = False, rows_in: int = 12,
            show_zones: bool = True, line_h: float = 0.0002, char_bars: float | None = None,
            tag_corner: str = "Entry end", atr: float = 0.0005, bars: Sequence[Bar] | None = None,
            chart_s: int = 60, atr_tf_s: int = 900, atr_tf: str = "15", atr_len: int = 14,
            r_min: float = 6.0, r_max: float = 14.0, pricescale: int = 100_000, mintick: float = 0.00001,
            pip_override: float = 0.0) -> Chart:
    """`line_h`: the script's lineH (price units: the ATR, or the ticks input), `char_bars`: its charBars input.
    Result (R): `bars` feed the capture pass (`capture_atr`); without them every trade is captured with `atr`;
    `chart_s` / `atr_tf_s` are the chart and ATR timeframes in seconds (`atr_tf` the ATR timeframe input's text),
    `atr_len`, `r_min`, `r_max`, `pip_override` the inputs, `pricescale` / `mintick` the symbol's (syminfo)."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from src.reports import pinescript as ps

    minimal = mode != "Detailed"
    zone = ZoneInfo(tz)
    a = pine_arrays(text)
    ints = {k: [int(x) for x in v] for k, v in a.items() if v and not v[0].startswith('"') and "." not in v[0]}
    for arr_name, v in a.items():
        if not v:
            ints[arr_name] = []

    def i_(name: str) -> list[int]:
        return ints[name]

    def s_(name: str) -> list[str]:
        return [x[1:-1].replace('\\"', '"').replace("\\\\", "\\") for x in a[name]]

    def fmt_t(t: int) -> str:
        return datetime.fromtimestamp(t / 1000, zone).strftime("%d %b %H:%M")

    def fmt_c(t: int) -> str:                                    # the table's time-only cells (cellFmt "HH:mm")
        return datetime.fromtimestamp(t / 1000, zone).strftime("%H:%M")

    def fmt_p(p: float | str) -> str:
        return f"{Decimal(str(p)):.{decimals}f}"

    head, tag, no, orders = s_("trHead"), s_("trTag"), s_("trNo"), s_("trOrders")
    side, e_t, e_has, e_kind = i_("trSide"), i_("trEntryT"), i_("trEntryHasP"), i_("trEntryKind")
    x_t, x_has, x_kind, tag_st = i_("trExitT"), i_("trExitHasP"), i_("trExitKind"), i_("trTagSt")
    e_p, x_p = a["trEntryP"], a["trExitP"]
    ev_t, ev_c, ev_has, ev_tr, ev_p = i_("evT"), i_("evCode"), i_("evHasP"), i_("evTr"), a["evP"]
    first, count = i_("trFirst"), i_("trCount")

    def ev_line(i: int) -> str:
        at = " @ " + fmt_p(ev_p[i]) if ev_has[i] == 1 else ""
        return ps.EV_WORDS[ev_c[i]] + " " + fmt_t(ev_t[i]) + at + ps.EV_SUFFIX.get(ev_c[i], "")

    def marker(i: int) -> str:
        k = ev_c[i]
        t = "SL" if k <= ps.EV_SL_REMOVED else "TP"
        top = t + " removed" if k in (ps.EV_SL_REMOVED, ps.EV_TP_REMOVED) else t + " → " + fmt_p(ev_p[i])
        return top + "\n" + fmt_t(ev_t[i])

    def entry_line(c: int) -> str:
        k = e_kind[c]
        at = " @ " + fmt_p(e_p[c]) if e_has[c] == 1 else " @ price not recorded"
        if k == ps.ENTRY_NOT_RECORDED:
            s = "entry time not recorded" + at
        else:
            s = ("entry " if k == ps.ENTRY_FILLED else "first seen open ") + fmt_t(e_t[c]) + at
        return s + "\nopened before window" if k == ps.ENTRY_BEFORE_WINDOW else s

    def exit_text(c: int) -> str:
        k = x_kind[c]
        at = " @ " + fmt_p(x_p[c]) if x_has[c] == 1 else " @ price not recorded"
        if k == ps.EXIT_OPEN:
            top = "at export " + fmt_t(x_t[c])
        elif k == ps.EXIT_UNKNOWN:
            top = "last seen " + fmt_t(x_t[c])
        else:
            top = "exit " + fmt_t(x_t[c]) + at
        return top + "\n" + ps.EXIT_WORDS[k]

    def exit_tip(c: int) -> str:
        k = x_kind[c]
        at = " @ " + fmt_p(x_p[c]) if x_has[c] == 1 else " @ price not recorded"
        if k == ps.EXIT_OPEN:
            return no[c] + " " + ps.EXIT_WORDS[k] + " · at export " + fmt_t(x_t[c])
        if k == ps.EXIT_UNKNOWN:
            return no[c] + " " + ps.EXIT_WORDS[k] + " · last seen " + fmt_t(x_t[c])
        return no[c] + " exit " + fmt_t(x_t[c]) + at + " · " + ps.EXIT_WORDS[k]

    def trade_tip(c: int, title: str) -> str:
        s = title + "\n" + orders[c]
        f, n = first[c], count[c]
        cap = ps.TIP_HEAD + ps.TIP_TAIL
        for j in range(f, f + n):
            if n <= cap or j < f + ps.TIP_HEAD or j >= f + n - ps.TIP_TAIL:
                s += "\n" + ev_line(j)
            elif j == f + ps.TIP_HEAD:
                s += f"\n... {n - cap} more events ..."
        return s + "\n" + ps.EXIT_WORDS[x_kind[c]]

    chart = Chart()
    min_w = min_bars * bar_ms
    for c in range(len(e_t)):
        lo, hi = e_t[c], x_t[c]
        if hi - lo < min_w:
            if e_kind[c] == ps.ENTRY_NOT_RECORDED:
                lo = hi - min_w
            else:
                hi = lo + min_w
        chart.spans.append((lo, hi))
    char_ms = (ps.DEFAULT_CHAR_BARS if char_bars is None else char_bars) * bar_ms
    far = tag_corner == ps.TAG_CORNERS[1]
    placed: list[tuple[float, float, float, float]] = []                 # tag text boxes: left, right, low, high
    tagged = [False] * len(e_t)
    for i, c in enumerate(i_("stTr")):
        leg, p = i_("stLeg")[i], float(a["stP"][i])
        if leg == ps.LEG_WORK:
            chart.working.append((c, i_("stX1")[i], i_("stX2")[i], p))
            continue
        s, e = i_("stX1")[i], i_("stX2")[i]
        x1 = chart.spans[c][0] if s <= e_t[c] else s
        x2 = chart.spans[c][1] if e >= x_t[c] else e
        if x2 > x1:
            if show_zones and e_has[c] == 1:
                ep = float(e_p[c])
                chart.zones.append((c, leg, x1, x2, max(p, ep), min(p, ep)))
                if minimal and tag_st[c] == i:
                    tw = (len(tag[c]) + ps.PAD_CHARS) * char_ms
                    th = (1 + ps.PAD_LINES) * line_h
                    for q in range(5):
                        cq = q % 4
                        at_top = (p < ep) == ((cq % 2 == 0) != far)
                        l0 = x2 - tw if cq >= 2 else float(x1)
                        hi0 = max(p, ep) if at_top else min(p, ep) + th
                        if q == 4 or not any(a < l0 + tw and b > l0 and lo_ < hi0 and hi_ > hi0 - th
                                             for a, b, lo_, hi_ in placed):
                            placed.append((l0, l0 + tw, hi0 - th, hi0))
                            chart.tags.append((c, x1, x2, max(p, ep), min(p, ep), tag[c],
                                               "top" if at_top else "bottom", False))
                            chart.tag_align.append("right" if cq >= 2 else "left")
                            break
                    tagged[c] = True
            chart.levels.append((c, x1, x2, p))
    if minimal:
        for c in range(len(e_t)):
            if not tagged[c]:
                ty = float(e_p[c]) if e_has[c] == 1 else float(x_p[c])
                chart.tags.append((c, chart.spans[c][0], chart.spans[c][1], ty, ty, tag[c], "top", True))
                chart.tag_align.append("left")
    for c in range(len(e_t)):
        ep = float(e_p[c])
        if e_has[c] == 1:
            chart.entry_lines.append((c, chart.spans[c][0], chart.spans[c][1], ep))
        k = x_kind[c]
        if k <= ps.EXIT_CLOSED:
            xp = float(x_p[c]) if x_has[c] == 1 else ep
            if x_has[c] == 1 and chart.spans[c][1] > x_t[c]:
                chart.exit_segments.append((c, x_t[c], chart.spans[c][1], xp))
    for i in range(len(i_("psX1"))):
        chart.pending_lines.append((i_("psX1")[i], i_("psX2")[i], float(a["psP"][i])))
    pd_head, pd_legs = s_("pdHead"), s_("pdLegs")
    for k in range(len(i_("pdT2"))):
        pk = i_("pdKind")[k]
        tip = pd_head[k] + ("" if pk == ps.PD_FILLED else " pending order")
        if pd_legs[k]:
            tip += "\n" + pd_legs[k]
        tip += ("\nplaced " if i_("pdPlaced")[k] == 1 else "\nfirst seen ") + fmt_t(i_("pdT1")[k]) + " @ " + \
            fmt_p(a["pdP"][k])
        if i_("pdSlHas")[k] == 1:
            tip += "\nSL " + fmt_p(a["pdSl"][k])
        if i_("pdTpHas")[k] == 1:
            tip += "\nTP " + fmt_p(a["pdTp"][k])
        tip += "\n" + ps.PD_WORDS[pk] + " " + fmt_t(i_("pdT2")[k])
        chart.pending.append((i_("pdT2")[k], float(a["pdP"][k]), tip))
    # Detailed text labels: entry / exit per trade, then change text; drawn sorted by (time, index)
    items: list[tuple[int, int, int, float, int, int]] = []        # (x, kind, ref, y, mode, dir)
    if not minimal:
        for c in range(len(e_t)):
            ey = float(e_p[c]) if e_has[c] == 1 else float(x_p[c])
            xy = float(x_p[c]) if x_has[c] == 1 else ey
            items.append((chart.spans[c][0], 1, c, ey, -1, -1 if side[c] == 1 else 1))
            items.append((chart.spans[c][1], 2, c, xy, 1, 1 if xy >= ey else -1))
    for e in i_("adjEv"):
        c = ev_tr[e]
        p = float(ev_p[e])
        if adj_text and not minimal:
            items.append((ev_t[e], 3, e, p, 1, 1 if p >= float(e_p[c]) else -1))
        elif adj_text:
            chart.labels.append((ev_t[e], p, 0, 1, 3, marker(e), head[c] + ": " + ev_line(e)))
        else:
            chart.dots.append((ev_t[e], p, head[c] + ": " + ev_line(e)))
    for _, (x, kd, rf, y, md, up) in sorted(enumerate(items), key=lambda it: (it[1][0], it[0])):
        txt = head[rf] + "\n" + entry_line(rf) if kd == 1 else exit_text(rf) if kd == 2 else marker(rf)
        tip = head[ev_tr[rf]] + ": " + ev_line(rf) if kd == 3 else trade_tip(rf, head[rf])
        chart.labels.append((x, y, up, md, kd, txt, tip))
    # marker groups (anchors within a text width and a text line of an earlier group's first marker)
    n_tr = len(e_t)

    def exit_y(c: int) -> float:
        return float(x_p[c]) if x_has[c] == 1 or e_has[c] == 0 else float(e_p[c])

    x_head, x_txt, x_tip = [-1] * n_tr, [""] * n_tr, [""] * n_tr
    e_head, e_at, e_cnt, e_names = [-1] * n_tr, [0] * n_tr, [0] * n_tr, [""] * n_tr
    for c in range(n_tr):
        if x_kind[c] <= ps.EXIT_CLOSED:
            hd = next((g for g in range(c) if x_head[g] == g
                       and abs(x_t[c] - x_t[g]) < (len(no[g]) + ps.PAD_CHARS) * char_ms
                       and abs(exit_y(c) - exit_y(g)) < line_h), c)
            x_head[c] = hd
            x_txt[hd] += ("" if hd == c else " ") + no[c]
            x_tip[hd] += ("" if hd == c else "\n") + exit_tip(c)
        if e_has[c] == 1:
            e_at[c] = chart.spans[c][0] if e_kind[c] == ps.ENTRY_NOT_RECORDED else e_t[c]
            eh = next((g for g in range(c) if e_head[g] == g
                       and abs(e_at[c] - e_at[g]) < (ps.MARK_CHARS + ps.PAD_CHARS) * char_ms
                       and abs(float(e_p[c]) - float(e_p[g])) < line_h), c)
            e_head[c] = eh
            e_cnt[eh] += 1
            e_names[eh] += ("" if eh == c else ", ") + tag[c]
    # markers, exactly on their anchors
    for c in range(n_tr):
        if e_has[c] == 1:
            tip = trade_tip(c, head[c] + " · " + entry_line(c))
            if e_cnt[e_head[c]] > 1:
                tip += "\nentries here: " + e_names[e_head[c]]
            chart.entries.append((c, e_at[c], float(e_p[c]), "up" if side[c] == 1 else "down", tip))
        k = x_kind[c]
        ep = float(e_p[c]) if e_has[c] == 1 else float(x_p[c])
        if k <= ps.EXIT_CLOSED:
            hd = x_head[c]
            chart.exits.append((c, x_t[c], exit_y(c), k, x_txt[c] if hd == c else "", x_tip[hd]))
        else:
            chart.exits.append((c, chart.spans[c][1], ep, k, ps.OPEN_MARK, exit_tip(c)))
    # Result (R): the capture pass, then the cell (resWhy / oneR / resR / fmtRes), its colour and tooltip (resTip)
    pip = ps.pip_size(pricescale, mintick, pip_override)
    if bars is None:
        atr_c: list[float | None] = [atr] * n_tr
        atr_at = [ps.ATR_CAPTURED] * n_tr
    else:
        atr_c, atr_at = capture_atr(e_t, bars)

    def res_why(c: int) -> str:
        a_c, st = atr_c[c], atr_at[c]
        if x_kind[c] == ps.EXIT_OPEN:
            return ps.RES_OPEN
        if e_kind[c] == ps.ENTRY_NOT_RECORDED:
            return ps.NA_ENTRY_TIME
        if e_has[c] == 0:
            return ps.NA_ENTRY_PRICE
        if x_kind[c] == ps.EXIT_UNKNOWN or x_has[c] == 0:
            return ps.NA_EXIT_PRICE
        if chart_s > atr_tf_s:
            return f"n/a (chart > {tf_text(atr_tf_s, atr_tf)})"
        if st == ps.ATR_BEFORE_HISTORY:
            return ps.NA_BEFORE_HISTORY
        if st == ps.ATR_NOT_REACHED:
            return ps.NA_NOT_REACHED
        if a_c is None or a_c <= 0:
            return ps.NA_NO_ATR
        return ""

    def res_r(c: int) -> float:
        a_c = atr_c[c]
        assert a_c is not None
        return ps.result_r(float(e_p[c]), float(x_p[c]), "BUY" if side[c] == 1 else "SELL", a_c, pip, r_min, r_max)

    def fmt_num(v: float) -> str:
        return str(int(v)) if v == int(v) else f"{v:.1f}"

    for c in i_("trNewest")[:rows_in]:
        k = e_kind[c]
        at = " @ " + fmt_p(e_p[c]) if e_has[c] == 1 else " @ -"
        entry = "time n/r" + at if k == ps.ENTRY_NOT_RECORDED else ("" if k == ps.ENTRY_FILLED else "seen ") + \
            fmt_c(e_t[c]) + at
        xat = " @ " + fmt_p(x_p[c]) if x_has[c] == 1 else " @ -"
        exit_ = "-" if x_kind[c] in (ps.EXIT_OPEN, ps.EXIT_UNKNOWN) else fmt_c(x_t[c]) + xat
        why = res_why(c)
        cell = why or ps.result_text(res_r(c))
        chart.rows.append((no[c], entry, exit_, cell))
        chart.row_tips.append(head[c] + "\n" + orders[c])
        chart.entry_tips.append(entry_line(c))
        chart.exit_tips.append(exit_text(c))
        ex = "; exit: " + ps.EXIT_WORDS[x_kind[c]]
        seen = "\nentry time = first seen open" if k in (ps.ENTRY_SEEN, ps.ENTRY_BEFORE_WINDOW) else ""
        r = 0.0 if why else res_r(c)
        chart.result_colours.append("C_REWARD" if r > 0 else "C_RISK" if r < 0 else "C_TEXT")
        if why:
            chart.result_tips.append((ps.RES_OPEN_TIP if why == ps.RES_OPEN else why) + ex)
        else:
            chart.result_tips.append(f"result {cell}; 1R from {tf_text(atr_tf_s, atr_tf)} ATR({atr_len}), clamped "
                                     f"{fmt_num(r_min)}-{fmt_num(r_max)}" + ex + seen)
    return chart
