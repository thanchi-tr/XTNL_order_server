"""Generate the two-concurrent-position ARM figure (illustrative sample).

Both long positions share one bid price / time chart. Order 1 is blue, order 2
is purple. Human actions are square chips and colour-filled cells; order-server
actions are outlined cells. The action table runs in one time sequence: order 1
on the left, order 2 on the right.

Usage: python gen_two_positions.py widget|file > out.svg
"""
import sys

import gen_time_price as base

# Order 2's fill lies on the shared path: pin the path to 1.10050 at 10:16:40.
base.ANCH = sorted(base.ANCH + [(1000, 1.10050)])

X0, XR = 70.0, 592.0
PTOP, PBOT, Y0, Y1 = 1.10380, 1.09760, 62.0, 330.0


def x(t):
    return base.x(t)


def y(p):
    return Y0 + (PTOP - p) / (PTOP - PBOT) * (Y1 - Y0)


r2 = base.r2
hm = base.hm

# order id -> (label, widget class, line hex, light fill, light stroke, light text)
ORD = {
    1: ("Order 1", "c-blue", "#378ADD", "#E6F1FB", "#185FA5", "#0C447C"),
    2: ("Order 2", "c-purple", "#7F77DD", "#EEEDFE", "#534AB7", "#3C3489"),
}
LEVELS = {
    1: dict(fill=200, entry=1.10000,
            stop=[(200, 1.09800), (2680, 1.09900), (3840, 1.10000)],
            trig=[(200, 1.10200), (3611, 1.10100), (3840, 1.10000)], release=4571),
    2: dict(fill=1000, entry=1.10050,
            stop=[(1000, 1.09950), (2850, 1.10050)],
            trig=[(1000, 1.10150), (2850, 1.10050)], release=3671),
}
HUMAN = {  # order -> [(tag, t)]
    1: [("A1", 200), ("A2", 2680), ("A3", 3840)],
    2: [("B1", 1000), ("B2", 2850)],
}
HEADSUP = {1: [(2660, 1.10215), (3560, 1.10235)], 2: [(1760, 1.10160), (2660, 1.10215)]}
# per-bar POST pill: (order, bar-end t, label, kind) kind: grey|amber|teal
PILLS = [
    (1, 900, "below 1R", "g"), (1, 1800, "below 1R", "g"), (1, 2700, "not conf.", "g"),
    (1, 3600, "ADJUST", "a"), (1, 4500, "COMPLETE", "t"),
    (2, 1800, "not conf.", "g"), (2, 2700, "ADJUST", "a"), (2, 3600, "COMPLETE", "t"),
    (2, 4500, "silent", "g"),
]
STATES = {
    1: [(202, 911, "Armed", "g"), (911, 3611, "Tracking", "g"), (3611, 4571, "Advice", "a"), (4571, 5400, "Complete", "t")],
    2: [(1002, 1811, "Armed", "g"), (1811, 2711, "Tracking", "g"), (2711, 3671, "Advice", "a"), (3671, 5400, "Complete", "t")],
}
KIND = {  # widget class, light fill, light stroke, light text
    "g": ("c-gray", "#F1EFE8", "#5F5E5A", "#444441"),
    "a": ("c-amber", "#FAEEDA", "#854F0B", "#633806"),
    "t": ("c-teal", "#E1F5EE", "#0F6E56", "#085041"),
}
LANE = {1: {"you": 372, "srv": 398, "state": 424}, 2: {"you": 462, "srv": 488, "state": 514}}

# Swimlane rows: (t, left cell, right cell); a cell is None or (kind, [lines]).
# kind "you" = human (first line is the action, lines starting "Effect" follow);
# kind "srv" = order-server.
ROWS = [
    (200, ("you", ["A1 · You go long: fill 1.10000, stop 1.09800",
                   "Effect: entry and 1R = 0.00200 fixed; armed",
                   "within ~2 s. No notice is sent."]), None),
    (860, ("srv", ["PRE bar 0: 0.22R, below trigger. Email."]), None),
    (911, ("srv", ["POST bar 0: calm (1.18 × ATR14). 0.30R,",
                   "below trigger. Armed → Tracking."]), None),
    (1000, None, ("you", ["B1 · You go long: fill 1.10050, stop 1.09950",
                          "Effect: its own 1R = 0.00100. Its bar 0 is",
                          "10:15–10:30; armed on its own, no notice."])),
    (1760, ("srv", ["PRE bar 1: 0.80R, below trigger."]),
           ("srv", ["PRE bar 0: 1.10R > 1R. Heads-up: may",
                    "advise 1.10050 (first-bar check pending).",
                    "Same PRE email as order 1, one item each."])),
    (1811, ("srv", ["POST bar 1: 0.75R, below trigger."]),
           ("srv", ["POST bar 0: calm (1.25 × ATR14). 1.00R is",
                    "not > 1R (borderline): not confirmed;",
                    "basis 1.09950 stored (= your stop).",
                    "Armed → Tracking."])),
    (2660, ("srv", ["PRE bar 2: 1.08R > 1R. Heads-up: may",
                    "advise 1.10000."]),
           ("srv", ["PRE bar 1: 1.65R > 1R. Heads-up: may",
                    "advise 1.10050. Basis equals your stop,",
                    "so it is dropped (no basis held)."])),
    (2680, ("you", ["A2 · You tighten the stop to 1.09900",
                    "Effect: real risk 0.5R, but the test keeps",
                    "1.09800 until a bar end confirms: the",
                    "heads-up cannot confirm itself."]), None),
    (2711, ("srv", ["POST bar 2: 0.95R vs 1R: not confirmed;",
                    "basis 1.09800 kept. Email ratio 1.900.*"]),
           ("srv", ["POST bar 1: 1.40R > 1R. ADJUST: move the",
                    "stop to 1.10050. Tracking → Advice."])),
    (2850, None, ("you", ["B2 · You follow the advice: stop 1.10050",
                          "Effect: risk 0, at target. No state change",
                          "until the 11:00 bar end; next PRE silent."])),
    (3560, ("srv", ["PRE bar 3: 1.18R vs held basis 1R.",
                    "Heads-up: may advise 1.10000."]),
           ("srv", ["PRE bar 2: at target, no basis: no item."])),
    (3611, ("srv", ["POST bar 3: 1.15R > 1R. ADJUST: move the",
                    "stop to 1.10000. Tracking → Advice."]),
           ("srv", ["POST bar 2: 1.80R > 0R, at target. Release",
                    "held for the verification re-read. Not in",
                    "the 11:00 email (that has order 1 only)."])),
    (3671, None, ("srv", ["Re-read confirms: COMPLETE, released.",
                          "Part-1 email, marked DELAYED.*",
                          "Advice → Complete."])),
    (3840, ("you", ["A3 · You follow the advice: stop 1.10000",
                    "Effect: risk 0, at target. Released at the",
                    "11:15 bar end, after the re-read."]), None),
    (4460, ("srv", ["PRE bar 4: at target, no basis: no item."]),
           ("srv", ["Released: no PRE item."])),
    (4511, ("srv", ["POST bar 4: 1.30R > 0R, at target. Release",
                    "held for the verification re-read."]),
           ("srv", ["Released: COMPLETE again, held for the",
                    "same re-read, then recorded silently."])),
    (4560, ("srv", ["T+60 s: part-0 email lists only order 1,",
                    "as awaiting the broker's final bar.*"]), None),
    (4571, ("srv", ["Re-read confirms: COMPLETE, released.",
                    "Part-1 email, DELAYED.* Advice → Complete."]), None),
]
FOOTNOTES = [
    "Same time on both sides = one shared notice for the route: one email, one item per order.",
    "Each order keeps its own state, stop basis and cursor; they share the bid series, the notice",
    "per bar end and its verification re-read. (A netting account would merge them: not shown.)",
    "* Current behaviour flagged in review (see the note).",
]

LINE_H, PAD = 15, 7


def bold_effect(line, widget):
    """Render a leading 'Effect:' in bold."""
    if not line.startswith("Effect:"):
        return line
    w = 'style="font-weight:500"' if widget else 'font-weight="700"'
    return f'<tspan {w}>Effect:</tspan>{line[len("Effect:"):]}'

TCOL, LCOL, RCOL, CW = 14, 72, 376, 298


def build(theme):
    W = theme == "widget"
    ts = (lambda: 'class="ts"') if W else (lambda: 'fill="#5F5E5A"')
    th = (lambda: 'class="th"') if W else (lambda: 'fill="#2C2C2A" font-weight="600" font-size="13"')
    ink = "var(--p)" if W else "#2C2C2A"
    grid = "var(--b)" if W else "#D3D1C7"
    band = "var(--bg-neutral)" if W else "#F1EFE8"
    o = []

    def chip(cls_tuple, shape, extra_text, sw=0.5):
        """Widget: class-coloured group; file: hard-coded light colours."""
        if W:
            return f'<g class="{cls_tuple[0]}">{shape.format(fill="", stroke="", sw=sw)}{extra_text.format(tfill="", tcls="class=\"ts\"")}</g>'
        return (shape.format(fill=f' fill="{cls_tuple[1]}"', stroke=f' stroke="{cls_tuple[2]}"', sw=sw)
                + extra_text.format(tfill=f' fill="{cls_tuple[3]}"', tcls=""))

    # ---- table geometry first (height) ----
    table_top = 560
    rows_geo = []
    yy = table_top + 30
    for t, lc, rc in ROWS:
        n = max(len(c[1]) if c else 1 for c in (lc, rc))
        h = PAD * 2 + LINE_H * n
        rows_geo.append((t, lc, rc, yy, h))
        yy += h + 4
    foot_y = yy + 14
    H = int(foot_y + LINE_H * len(FOOTNOTES) + 10)

    o.append(f'<svg width="100%" viewBox="0 0 680 {H}" role="img"' + ('' if W else ' xmlns="http://www.w3.org/2000/svg" font-family="Segoe UI, system-ui, sans-serif" font-size="12"') + '>')
    o.append('<title>ARM break-even advice: two concurrent positions</title>')
    o.append('<desc>Illustrative sample: two long positions on one bid price/time chart. Order 1 blue, order 2 purple. Human actions are square chips and filled cells, order-server actions are outlined cells; the action table runs in one time sequence with order 1 on the left and order 2 on the right.</desc>')
    if not W:
        o.append(f'<rect width="680" height="{H}" fill="#ffffff"/>')

    # ---- legend (two rows) ----
    lx = 40
    for k in (1, 2):
        lab, cls, hexc, fl, st, tx = ORD[k]
        o.append(f'<rect x="{lx}" y="14" width="12" height="12" rx="2" fill="{hexc}"/>')
        o.append(f'<text {ts()} x="{lx+18}" y="24">{lab}</text>')
        lx += 80
    for sym, lab, w in (
        (lambda a: f'<line x1="{a}" y1="20" x2="{a+20}" y2="20" stroke="{ink}" stroke-width="1.2"/>', "Bid", 22),
        (lambda a: f'<line x1="{a}" y1="20" x2="{a+20}" y2="20" stroke="#888780" stroke-width="2"/>', "Stop", 28),
        (lambda a: f'<line x1="{a}" y1="20" x2="{a+20}" y2="20" stroke="#888780" stroke-width="1.2" stroke-dasharray="5 3"/>', "Trigger", 44),
        (lambda a: f'<line x1="{a}" y1="20" x2="{a+20}" y2="20" stroke="#888780" stroke-width="1" stroke-dasharray="1 3"/>', "Entry", 32),
    ):
        o.append(sym(lx))
        o.append(f'<text {ts()} x="{lx+26}" y="24">{lab}</text>')
        lx += 26 + w + 14
    # second legend row: human vs server
    o.append(chip(KIND["g"], '<rect x="40" y="33" width="22" height="16" rx="3"{fill}{stroke} stroke-width="1.5"/>', '<text {tcls}{tfill} x="51" y="45" text-anchor="middle">A1</text>', 1.5))
    o.append(f'<text {ts()} x="68" y="45">You (human): square chip, filled cell</text>')
    o.append(f'<rect x="318" y="34" width="40" height="14" rx="7" fill="none" stroke="{grid}" stroke-width="1"/>')
    o.append(f'<text {ts()} x="366" y="45">Order-server: bar-end pill, outlined cell</text>')

    # ---- chart ----
    for i, b in enumerate(base.BARS[:5]):
        xa, xb = x(b[0]), x(b[0] + 900)
        if i % 2 == 0:
            o.append(f'<rect x="{r2(xa)}" y="{Y0}" width="{r2(xb-xa)}" height="{Y1-Y0}" fill="{band}"/>')
        o.append(f'<text {ts()} x="{r2((xa+xb)/2)}" y="{Y0+14}" text-anchor="middle">bar {i}</text>')
    for k in range(6):
        p = 1.09800 + k * 0.001
        o.append(f'<line x1="{X0}" y1="{r2(y(p))}" x2="{XR}" y2="{r2(y(p))}" stroke="{grid}" stroke-width="0.5"/>')
        o.append(f'<text {ts()} x="62" y="{r2(y(p)+4)}" text-anchor="end">{p:.5f}</text>')
    # human guide lines
    for k in (1, 2):
        for tag, t in HUMAN[k]:
            o.append(f'<line x1="{r2(x(t))}" y1="{Y0+40+(22 if k == 2 else 0)}" x2="{r2(x(t))}" y2="{LANE[k]["you"]-10}" stroke="{ORD[k][2]}" stroke-width="1.3" stroke-dasharray="3 2" opacity="0.75"/>')
    # entry / trigger / stop per order
    for k in (1, 2):
        L, c = LEVELS[k], ORD[k][2]
        o.append(f'<line x1="{r2(x(L["fill"]))}" y1="{r2(y(L["entry"]))}" x2="{XR}" y2="{r2(y(L["entry"]))}" stroke="{c}" stroke-width="1" stroke-dasharray="1 3"/>')
        d = f'M{r2(x(L["trig"][0][0]))} {r2(y(L["trig"][0][1]))}'
        for t, p in L["trig"][1:]:
            d += f' H{r2(x(t))} V{r2(y(p))}'
        d += f' H{r2(x(L["release"]))}'
        o.append(f'<path d="{d}" fill="none" stroke="{c}" stroke-width="1.2" stroke-dasharray="5 3"/>')
        d = f'M{r2(x(L["stop"][0][0]))} {r2(y(L["stop"][0][1]))}'
        for t, p in L["stop"][1:]:
            d += f' H{r2(x(t))} V{r2(y(p))}'
        d += f' H{XR}'
        o.append(f'<path d="{d}" fill="none" stroke="{c}" stroke-width="2" stroke-linejoin="round"/>')
    pts = " ".join(f"{r2(x(t))},{r2(y(p))}" for t, p in base.path_points())
    o.append(f'<polyline points="{pts}" fill="none" stroke="{ink}" stroke-width="1.2" stroke-linejoin="round"/>')
    for b in base.BARS[:5]:
        o.append(f'<circle cx="{r2(x(b[0]+900))}" cy="{r2(y(b[4]))}" r="2.5" fill="{ink}"/>')
    # heads-up PRE quotes, ring per order (order 2 outer)
    for k, rad in ((1, 4), (2, 7)):
        for t, p in HEADSUP[k]:
            o.append(f'<circle cx="{r2(x(t))}" cy="{r2(y(p))}" r="{rad}" fill="none" stroke="{ORD[k][2]}" stroke-width="1.5"/>')
    for k in (1, 2):
        L = LEVELS[k]
        o.append(f'<circle cx="{r2(x(L["fill"]))}" cy="{r2(y(L["entry"]))}" r="4.5" fill="{ORD[k][2]}"/>')
    # human chips at top of guide lines
    for k in (1, 2):
        for tag, t in HUMAN[k]:
            cx = x(t)
            cy0 = Y0 + 22 + (22 if k == 2 else 0)
            o.append(chip((ORD[k][1], ORD[k][3], ORD[k][4], ORD[k][5]),
                          f'<rect x="{r2(cx-13)}" y="{cy0}" width="26" height="18" rx="3"{{fill}}{{stroke}} stroke-width="{{sw}}"/>',
                          f'<text {{tcls}}{{tfill}} x="{r2(cx)}" y="{cy0+13}" text-anchor="middle">{tag}</text>', 1.5))
    o.append(f'<line x1="{X0}" y1="{Y1}" x2="{XR}" y2="{Y1}" stroke="{grid}" stroke-width="0.5"/>')
    for m in range(0, 91, 15):
        s = 36000 + m * 60
        o.append(f'<text {ts()} x="{r2(x(m*60))}" y="{Y1+16}" text-anchor="middle">{s//3600:02d}:{s%3600//60:02d}</text>')

    # ---- lanes ----
    for k in (1, 2):
        ln, c = LANE[k], ORD[k][2]
        o.append(f'<rect x="64" y="{ln["you"]-12}" width="3" height="{ln["state"]+11-(ln["you"]-12)}" fill="{c}"/>')
        for key, lab in (("you", "you"), ("srv", "server"), ("state", "state")):
            o.append(f'<text {ts()} x="58" y="{ln[key]+4}" text-anchor="end">O{k} {lab}</text>')
            if key != "state":
                o.append(f'<line x1="{X0}" y1="{ln[key]}" x2="{XR}" y2="{ln[key]}" stroke="{grid}" stroke-width="0.5" stroke-dasharray="2 4"/>')
        for tag, t in HUMAN[k]:
            cx = x(t)
            o.append(chip((ORD[k][1], ORD[k][3], ORD[k][4], ORD[k][5]),
                          f'<rect x="{r2(cx-13)}" y="{ln["you"]-9}" width="26" height="18" rx="3"{{fill}}{{stroke}} stroke-width="{{sw}}"/>',
                          f'<text {{tcls}}{{tfill}} x="{r2(cx)}" y="{ln["you"]+4}" text-anchor="middle">{tag}</text>', 1.5))
        for kk, t, lab, kind in PILLS:
            if kk != k:
                continue
            cx = x(t + 11)
            o.append(chip(KIND[kind], f'<rect x="{r2(cx-36)}" y="{ln["srv"]-9}" width="72" height="18" rx="9"{{fill}}{{stroke}} stroke-width="{{sw}}"/>',
                          f'<text {{tcls}}{{tfill}} x="{r2(cx)}" y="{ln["srv"]+4}" text-anchor="middle">{lab}</text>'))
        for a, b, lab, kind in STATES[k]:
            xa, xb = x(a) + 1, x(b) - 1
            o.append(chip(KIND[kind], f'<rect x="{r2(xa)}" y="{ln["state"]-9}" width="{r2(xb-xa)}" height="18" rx="4"{{fill}}{{stroke}} stroke-width="{{sw}}"/>',
                          f'<text {{tcls}}{{tfill}} x="{r2((xa+xb)/2)}" y="{ln["state"]+4}" text-anchor="middle">{lab}</text>'))

    # ---- swimlane table ----
    o.append(f'<text {th()} x="{TCOL}" y="{table_top+16}">Time</text>')
    for k, cx0 in ((1, LCOL), (2, RCOL)):
        o.append(f'<rect x="{cx0}" y="{table_top+5}" width="{CW}" height="16" rx="3" fill="{ORD[k][2]}" opacity="0.18"/>')
        o.append(f'<text {th()} x="{cx0+8}" y="{table_top+17}">{ORD[k][0]} actions</text>')
    for t, lc, rc, top, h in rows_geo:
        o.append(f'<text {ts()} x="{TCOL}" y="{top+PAD+11}">{hm(t)}</text>')
        o.append(f'<line x1="{TCOL}" y1="{top-2}" x2="674" y2="{top-2}" stroke="{grid}" stroke-width="0.5"/>')
        for k, cell, cx0 in ((1, lc, LCOL), (2, rc, RCOL)):
            if not cell:
                continue
            kind, lines = cell
            if kind == "you":
                cls = (ORD[k][1], ORD[k][3], ORD[k][4], ORD[k][5])
                body = "".join(
                    (f'<text {{tcls}}{{tfill}} x="{cx0+8}" y="{top+PAD+11+LINE_H*i}"' + (' style="font-weight:500"' if i == 0 and W else (' font-weight="600"' if i == 0 else '')) + f'>{bold_effect(ln, W)}</text>')
                    for i, ln in enumerate(lines))
                o.append(chip(cls, f'<rect x="{cx0}" y="{top}" width="{CW}" height="{h}" rx="5"{{fill}}{{stroke}} stroke-width="{{sw}}"/>', body, 1.5))
            else:
                o.append(f'<rect x="{cx0}" y="{top}" width="{CW}" height="{h}" rx="5" fill="none" stroke="{ORD[k][2]}" stroke-width="0.8" stroke-dasharray="3 2"/>')
                for i, ln in enumerate(lines):
                    o.append(f'<text {ts()} x="{cx0+8}" y="{top+PAD+11+LINE_H*i}">{ln}</text>')
    for i, f in enumerate(FOOTNOTES):
        o.append(f'<text {ts()} x="{TCOL}" y="{foot_y+LINE_H*i}">{f}</text>')
    o.append('</svg>')
    return "\n".join(o)


if __name__ == "__main__":
    sys.stdout.write(build(sys.argv[1]))
