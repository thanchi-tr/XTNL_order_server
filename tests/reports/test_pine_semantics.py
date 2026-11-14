"""Pine source robustness: long strings stay on short chunk lines, comment / string literals never carry a
character an editor could take as a line break, no generated name is a reserved word, every index the script reads
at run time is inside its arrays, and a trade's tooltip stays capped."""
from __future__ import annotations

import re
from decimal import Decimal

from src.reports.lifecycle import LcEvent, Trade
from src.reports.pinescript import (ARRAY_CHUNK_CHARS, MAX_BOXES, MAX_LABELS, MAX_LINES, MAX_TABLE_ROWS,
                                    TABLE_HEADER_ROWS, TIP_HEAD, TIP_TAIL, RenderInfo, comment_safe, pine_str,
                                    render_symbol)

from tests.reports.builders import H, NOW, PINE_RESERVED, T0, emulate, lint, order, pine_arrays, position
from tests.reports.test_pinescript import INFO, book, crowded, pending_order, quick, render
from tests.reports.test_trades import bracket_tp, catch_up_trade

_STRINGS = ("trHead", "trTag", "trNo", "trOrders", "pdHead", "pdLegs")
_FLOATS = ("trEntryP", "trExitP", "stP", "evP", "pdP", "pdSl", "pdTp", "psP")


def _long_ids(i: int) -> Trade:
    trades, _ = book(*quick(i))
    t = trades[0]
    oid = f"ORDER{i:05d}" + "X" * 30           # safe_id cuts it to 32 characters
    return Trade(**{**t.__dict__, "order_id": oid, "position_id": "P" + oid, "key": f"stonex:{oid}",
                    "order_ids": tuple(f"{oid}-{k}" for k in range(14))})


def test_long_strings_are_chunked() -> None:
    res = render_symbol("EURUSD", [_long_ids(i) for i in range(60)], [], INFO)
    assert len(res.drawn) == 60 and res.left_out == 0
    arr = pine_arrays(res.text)
    assert len(arr["trHead"]) == len(arr["trOrders"]) == len(arr["trSide"]) == 60
    longest = max(len(x) for x in res.text.split("\n"))
    assert longest <= ARRAY_CHUNK_CHARS + 100, longest
    assert res.text.count("array.concat(trOrders, ") >= 5                # split by characters, not only by count
    assert "if barstate.isfirst\n    array.concat(" in res.text            # extra chunks are added once
    assert "+2 more" in arr["trOrders"][0]                                 # at most MAX_TIP_IDS ids per tooltip
    assert lint(res.text) == []


def test_no_editor_line_breaks_in_comments_or_strings() -> None:
    for sep in (" ", " ", "\x85", "\r", "\x0b"):
        assert sep not in comment_safe(f"EUR{sep}USD") and sep not in pine_str(f"a{sep}b")
    info = RenderInfo(generated_ms=NOW, since_ms=NOW - 24 * H, until_ms=NOW, adapter="stone x",
                      notes=("a b",))
    trades, pending = book(*bracket_tp())
    text = render_symbol("EUR USD", trades, pending, info).text
    assert all(sep not in text for sep in (" ", " ", "\x85", "\r"))
    assert lint(text) == []


def test_no_generated_name_is_a_pine_reserved_word() -> None:
    """Regression: an early export named an array `by`, which TradingView rejects (CE10150)."""
    for text in (render(*bracket_tp()), render(*crowded()), render(*catch_up_trade(T0, 1038153117))):
        declared = set(re.findall(r"^\s*(?:var\s+)?(?:const\s+)?(?:int|float|string|bool|color|line|label|table)"
                                  r"(?:\[\])?\s+([A-Za-z_]\w*)\s*=", text, re.M))
        functions = set(re.findall(r"^([A-Za-z_]\w*)\((.*)\) =>", text, re.M))
        params = {p for _, ps in functions for p in re.findall(r"\w+\s+(\w+)", ps)}
        loops = set(re.findall(r"^\s*for (\w+) =", text, re.M))
        names = declared | {f for f, _ in functions} | params | loops
        assert declared and not (names & PINE_RESERVED), names & PINE_RESERVED
        # namespaces are not variable names either
        assert not (names & {"order", "box", "line", "label", "color", "table", "array", "str", "math"})
        assert lint(text) == []


def _runtime_bounds(text: str) -> None:
    """What the script's loops index at run time stays inside its arrays (Pine stops on an out-of-range get), and
    the drawing and table budgets hold."""
    a = pine_arrays(text)
    ints = {k: [int(x) for x in v] for k, v in a.items() if k not in _STRINGS + _FLOATS}
    n_tr, n_ev, n_pd = len(a["trHead"]), len(a["evT"]), len(a["pdHead"])
    assert all(len(a[k]) == n_tr for k in ("trTag", "trNo", "trOrders", "trSide", "trEntryT", "trEntryP",
                                           "trEntryHasP", "trEntryKind", "trExitT", "trExitP", "trExitHasP",
                                           "trExitKind", "trTagSt", "trFirst", "trCount"))
    assert all(len(a[k]) == n_ev for k in ("evP", "evHasP", "evCode", "evTr"))
    assert all(len(a[k]) == len(a["stTr"]) for k in ("stLeg", "stX1", "stX2", "stP"))
    assert all(len(a[k]) == n_pd for k in ("pdLegs", "pdT1", "pdT2", "pdP", "pdKind", "pdPlaced", "pdSl", "pdSlHas",
                                           "pdTp", "pdTpHas"))
    assert all(len(a[k]) == len(a["psX1"]) for k in ("psX2", "psP"))
    assert all(0 <= c < n_tr for k in ("stTr", "evTr", "trNewest") for c in ints[k])
    assert all(0 <= e < n_ev for e in ints["adjEv"])
    # a trade's tag step is one of its own zone steps (never the opening order's working line), or -1
    assert all(s == -1 or (ints["stTr"][s] == c and ints["stLeg"][s] in (1, 2))
               for c, s in enumerate(ints["trTagSt"]))
    assert all(f + n <= n_ev for f, n in zip(ints["trFirst"], ints["trCount"]))
    assert set(ints["stLeg"]) <= {1, 2, 3} and set(ints["trExitKind"]) <= {1, 2, 3, 4, 5}
    assert set(ints["trEntryKind"]) <= {1, 2, 3, 4} and set(ints["pdKind"]) <= {1, 2, 3, 4}
    assert all(x1 <= x2 for x1, x2 in zip(ints["stX1"], ints["stX2"]))
    assert all(x1 < x2 for x1, x2 in zip(ints["psX1"], ints["psX2"]))
    assert all(e <= x for e, x in zip(ints["trEntryT"], ints["trExitT"]))
    assert len(ints["trNewest"]) <= MAX_TABLE_ROWS and TABLE_HEADER_ROWS + MAX_TABLE_ROWS <= 40
    m = re.search(r"lines used: (\d+) of 500  labels used: (\d+) of 500  boxes used: (\d+) of 500", text)
    assert m is not None
    lines, labels, boxes = (int(g) for g in m.groups())
    assert lines <= MAX_LINES and labels <= MAX_LABELS and boxes <= MAX_BOXES
    # what the emulated script draws (either label mode, zones on or off) never exceeds what the header (and the
    # budget) counted
    for mode in ("Minimal", "Detailed"):
        for zones in (True, False):
            ch = emulate(text, mode=mode, show_zones=zones)
            drawn_lines = len(ch.levels) + len(ch.working) + len(ch.entry_lines) + len(ch.exit_segments) + \
                len(ch.pending_lines)
            drawn_labels = len(ch.labels) + len(ch.entries) + len(ch.exits) + len(ch.dots) + len(ch.pending)
            drawn_boxes = len(ch.zones) + sum(1 for t in ch.tags if t[7])
            assert drawn_lines <= lines and drawn_labels <= labels and drawn_boxes <= boxes


def test_realistic_fixtures_are_valid_pine_and_in_bounds() -> None:
    fixtures = (
        bracket_tp() + catch_up_trade(T0 + 9 * H, 1038153117) + pending_order(1),
        crowded(),
        [x for i in range(300) for x in quick(i)] + [x for i in range(20) for x in pending_order(i)],
        [position("P3", T0 + 4 * H, "OPEN", "1.08200", sl="1.08000", tp="1.08600"),
         order("W1", T0, "WORKING", "1.0700")],
    )
    for v in fixtures:
        text = render(*v)
        assert lint(text) == []
        _runtime_bounds(text)
    empty = render_symbol("USDJPY", [], [], INFO).text
    assert lint(empty) == []
    _runtime_bounds(empty)


def test_trade_tooltip_is_capped() -> None:
    """A trailing stop logs hundreds of moves: the tooltip lists the first TIP_HEAD and last TIP_TAIL events and
    counts the rest."""
    trades, _ = book(*bracket_tp())
    t = trades[0]
    moves = tuple(LcEvent(T0 + H + i * 10 + 5, "SL_MOVED", Decimal("1.07") + Decimal(i) / 10_000)
                  for i in range(400))
    t = Trade(**{**t.__dict__, "events": tuple(sorted(t.events + moves, key=lambda e: e.t_ms))})
    text = render_symbol("EURUSD", [t], [], INFO).text
    assert lint(text) == []
    _runtime_bounds(text)
    cap = TIP_HEAD + TIP_TAIL
    assert f"if n <= {cap} or j < f + {TIP_HEAD} or j >= f + n - {TIP_TAIL}" in text
    tip = emulate(text).entries[0][4]
    assert tip == emulate(text, mode="Detailed").labels[0][6].replace(
        "T1 BUY #O1\n", "T1 BUY #O1 · entry 07 Nov 09:20 @ 1.08010\n", 1)
    assert f"... {len(t.events) - cap} more events ..." in tip
    assert len(tip.split("\n")) == 2 + cap + 1 + 1         # head, orders, events, the count, the result
    assert len(tip) < 4096
