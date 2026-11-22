"""Generate design 3 scenario figures (live stop adjustment, Phases 3-5; design only).

One figure per scenario (A-D). Each shares one bid price/time chart between
order 1 (blue, the single-order case) and order 2 (purple, concurrent).
Per order there are lanes for the order-server bar-end outcome, the stop-move
intent in Phases 4/5 and in Phase 3, and the SM11 state. Under the lanes an
event table runs in one time sequence: order 1 left, order 2 right. Human
actions are colour-filled cells, order-server actions dashed outlines, broker
events solid grey outlines.

Usage: python gen_live_scenarios.py <a|b|c|d> <widget|file>
"""
import random
import sys

from live_scenarios_data import SCEN

X0, XR = 70.0, 592.0
PXMIN = (XR - X0) / 90.0  # 10:00-11:30
Y0, Y1 = 64.0, 300.0

ORD = {
    1: ("Order 1", "c-blue", "#378ADD", "#E6F1FB", "#185FA5", "#0C447C"),
    2: ("Order 2", "c-purple", "#7F77DD", "#EEEDFE", "#534AB7", "#3C3489"),
}
KIND = {  # widget class, light fill, light stroke, light text
    "g": ("c-gray", "#F1EFE8", "#5F5E5A", "#444441"),
    "a": ("c-amber", "#FAEEDA", "#854F0B", "#633806"),
    "t": ("c-teal", "#E1F5EE", "#0F6E56", "#085041"),
    "r": ("c-red", "#FCEBEB", "#A32D2D", "#791F1F"),
    "c": ("c-coral", "#FAECE7", "#993C1D", "#712B13"),
}
LINE_H, PAD = 15, 7
TCOL, LCOL, RCOL, CW = 14, 72, 376, 298


def x(t):
    return X0 + t / 60.0 * PXMIN


def r2(v):
    return f"{v:.1f}".rstrip("0").rstrip(".")


def hm(t):
    s = 36000 + int(t)
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"


def path_points(sc):
    """Piecewise-linear bid through the anchors with small seeded noise,
    clamped into each bar's [low, high] so the stated OHLC holds."""
    rnd = random.Random(sc["seed"])
    anch = sorted(sc["anchors"])
    keys = [t for t, _ in anch]
    av = dict(anch)
    times = sorted(set(keys + list(range(0, 5401, 20))))
    out = []
    for t in times:
        if t in av:
            out.append((t, av[t]))
            continue
        i = max(k for k in range(len(keys)) if keys[k] < t)
        a, b = anch[i], anch[i + 1]
        f = (t - a[0]) / (b[0] - a[0])
        p = a[1] + f * (b[1] - a[1]) + rnd.gauss(0, sc.get("noise", 0.00003)) * (4 * f * (1 - f)) ** 0.5
        for bs, hi, lo in sc["bars"]:
            if bs <= t < bs + 900:
                p = min(max(p, lo + 0.00002), hi - 0.00002)
        out.append((t, p))
    return out


def build(key, theme):
    sc = SCEN[key]
    W = theme == "widget"
    ts = (lambda: 'class="ts"') if W else (lambda: 'fill="#5F5E5A"')
    th = (lambda: 'class="th"') if W else (lambda: 'fill="#2C2C2A" font-weight="600" font-size="13"')
    ink = "var(--p)" if W else "#2C2C2A"
    grid = "var(--b)" if W else "#D3D1C7"
    band = "var(--bg-neutral)" if W else "#F1EFE8"
    lo, hi = sc["yrange"]

    def y(p):
        return Y0 + (hi - p) / (hi - lo) * (Y1 - Y0)

    def chip(cls, shape, text, sw=0.5):
        if W:
            return f'<g class="{cls[0]}">{shape.format(fill="", stroke="", sw=sw)}{text.format(tfill="", tcls="class=\"ts\"")}</g>'
        return shape.format(fill=f' fill="{cls[1]}"', stroke=f' stroke="{cls[2]}"', sw=sw) + text.format(tfill=f' fill="{cls[3]}"', tcls="")

    def ocls(k):
        return (ORD[k][1], ORD[k][3], ORD[k][4], ORD[k][5])

    # lanes geometry
    lane_names = [("srv", "server"), ("p45", "P4/5"), ("p3", "P3"), ("state", "state")]
    lanes = {}
    ly = Y1 + 40
    for k in (1, 2):
        lanes[k] = {}
        for nm, _ in lane_names:
            lanes[k][nm] = ly
            ly += 22
        ly += 10
    table_top = ly + 14
    rows_geo, yy = [], table_top + 30
    for t, lc, rc in sorted(sc["rows"], key=lambda r: r[0]):
        n = max(len(c[1]) if c else 1 for c in (lc, rc))
        h = PAD * 2 + LINE_H * n
        rows_geo.append((t, lc, rc, yy, h))
        yy += h + 4
    foot_y = yy + 14
    H = int(foot_y + LINE_H * len(sc["foot"]) + 10)

    o = [f'<svg width="100%" viewBox="0 0 680 {H}" role="img"' + ('' if W else ' xmlns="http://www.w3.org/2000/svg" font-family="Segoe UI, system-ui, sans-serif" font-size="12"') + '>',
         f'<title>{sc["title"]}</title>', f'<desc>{sc["desc"]}</desc>']
    if not W:
        o.append(f'<rect width="680" height="{H}" fill="#ffffff"/>')

    # legend row 1
    lx = 40
    for k in (1, 2):
        o.append(f'<rect x="{lx}" y="12" width="12" height="12" rx="2" fill="{ORD[k][2]}"/><text {ts()} x="{lx+18}" y="22">{ORD[k][0]}</text>')
        lx += 78
    for sym, lab, w in (
        (lambda a: f'<line x1="{a}" y1="18" x2="{a+20}" y2="18" stroke="{ink}" stroke-width="1.2"/>', "Bid", 22),
        (lambda a: f'<line x1="{a}" y1="18" x2="{a+20}" y2="18" stroke="#888780" stroke-width="2"/>', "Stop P4/P5", 64),
        (lambda a: f'<line x1="{a}" y1="18" x2="{a+20}" y2="18" stroke="#888780" stroke-width="1.5" stroke-dasharray="4 2"/>', "Stop P3", 44),
        (lambda a: f'<line x1="{a}" y1="18" x2="{a+20}" y2="18" stroke="#888780" stroke-width="1" stroke-dasharray="1 3"/>', "Entry", 32),
    ):
        o.append(sym(lx) + f'<text {ts()} x="{lx+26}" y="22">{lab}</text>')
        lx += 26 + w + 12
    # legend row 2
    o.append(f'<polygon points="46,36 51,41 46,46 41,41" fill="#5F5E5A"/><text {ts()} x="58" y="45">Broker fills (stop / TP): position closed</text>')
    o.append(f'<rect x="300" y="34" width="22" height="13" rx="3" fill="none" stroke="{grid}" stroke-width="1" stroke-dasharray="3 2"/><text {ts()} x="328" y="45">Server</text>')
    o.append(chip(KIND["g"], '<rect x="384" y="34" width="22" height="13" rx="3"{fill}{stroke} stroke-width="1.5"/>', ''))
    o.append(f'<text {ts()} x="412" y="45">You</text>')
    o.append(f'<rect x="450" y="34" width="22" height="13" rx="3" fill="none" stroke="#888780" stroke-width="1"/><text {ts()} x="478" y="45">Broker</text>')

    # chart
    for i in range(5):
        xa, xb = x(i * 900), x(i * 900 + 900)
        if i % 2 == 0:
            o.append(f'<rect x="{r2(xa)}" y="{Y0}" width="{r2(xb-xa)}" height="{Y1-Y0}" fill="{band}"/>')
        o.append(f'<text {ts()} x="{r2((xa+xb)/2)}" y="{Y0+14}" text-anchor="middle">bar {i}</text>')
    for p in sc["ticks"]:
        o.append(f'<line x1="{X0}" y1="{r2(y(p))}" x2="{XR}" y2="{r2(y(p))}" stroke="{grid}" stroke-width="0.5"/>')
        o.append(f'<text {ts()} x="62" y="{r2(y(p)+4)}" text-anchor="end">{p:.5f}</text>')
    for k in (1, 2):
        od, c = sc["orders"][k], ORD[k][2]
        end = od.get("end", 5400)
        o.append(f'<line x1="{r2(x(od["fill"]))}" y1="{r2(y(od["entry"]))}" x2="{r2(x(end))}" y2="{r2(y(od["entry"]))}" stroke="{c}" stroke-width="1" stroke-dasharray="1 3"/>')
        if od.get("tp"):
            o.append(f'<line x1="{r2(x(od["fill"]))}" y1="{r2(y(od["tp"]))}" x2="{r2(x(end))}" y2="{r2(y(od["tp"]))}" stroke="{c}" stroke-width="1.2" stroke-dasharray="6 2 1 2"/>')
        for nm, dash, wdt in (("stop_p3", ' stroke-dasharray="4 2"', 1.5), ("stop_p45", "", 2)):
            pts = od[nm]
            d = f'M{r2(x(pts[0][0]))} {r2(y(pts[0][1]))}'
            for t, p in pts[1:]:
                d += f' H{r2(x(t))} V{r2(y(p))}'
            d += f' H{r2(x(od.get(nm + "_end", end)))}'
            o.append(f'<path d="{d}" fill="none" stroke="{c}" stroke-width="{wdt}"{dash} stroke-linejoin="round"/>')
    pts = " ".join(f"{r2(x(t))},{r2(y(p))}" for t, p in path_points(sc))
    o.append(f'<polyline points="{pts}" fill="none" stroke="{ink}" stroke-width="1.2" stroke-linejoin="round"/>')
    for k in (1, 2):
        od = sc["orders"][k]
        o.append(f'<circle cx="{r2(x(od["fill"]))}" cy="{r2(y(od["entry"]))}" r="4.5" fill="{ORD[k][2]}"/>')
        for t, p, lab in od.get("closes", []):
            cx, cy = x(t), y(p)
            o.append(f'<polygon points="{r2(cx)},{r2(cy-6)} {r2(cx+6)},{r2(cy)} {r2(cx)},{r2(cy+6)} {r2(cx-6)},{r2(cy)}" fill="{ORD[k][2]}" stroke="{ink}" stroke-width="1"/>')
            if lab:
                o.append(f'<text {ts()} x="{r2(cx+9)}" y="{r2(cy+4 + od.get("close_dy", 0))}">{lab}</text>')
    for note in sc.get("chart_notes", []):
        o.append(f'<text {ts()} x="{note[0]}" y="{note[1]}">{note[2]}</text>')
    o.append(f'<line x1="{X0}" y1="{Y1}" x2="{XR}" y2="{Y1}" stroke="{grid}" stroke-width="0.5"/>')
    for m in range(0, 91, 15):
        s = 36000 + m * 60
        o.append(f'<text {ts()} x="{r2(x(m*60))}" y="{Y1+16}" text-anchor="middle">{s//3600:02d}:{s%3600//60:02d}</text>')

    # lanes
    for k in (1, 2):
        ln, od = lanes[k], sc["orders"][k]
        o.append(f'<rect x="64" y="{ln["srv"]-9}" width="3" height="{ln["state"]-ln["srv"]+18}" fill="{ORD[k][2]}"/>')
        for nm, lab in lane_names:
            o.append(f'<text {ts()} x="58" y="{ln[nm]+4}" text-anchor="end">O{k} {lab}</text>')
            o.append(f'<line x1="{X0}" y1="{ln[nm]}" x2="{XR}" y2="{ln[nm]}" stroke="{grid}" stroke-width="0.5" stroke-dasharray="2 4"/>')
        for t, lab, kind in od["pills"]:
            cx = x(t)
            w = max(44, len(lab) * 6.6 + 12)
            o.append(chip(KIND[kind], f'<rect x="{r2(cx-w/2)}" y="{ln["srv"]-8}" width="{r2(w)}" height="16" rx="8"{{fill}}{{stroke}} stroke-width="{{sw}}"/>',
                          f'<text {{tcls}}{{tfill}} x="{r2(cx)}" y="{ln["srv"]+4}" text-anchor="middle">{lab}</text>'))
        for nm in ("p45", "p3"):
            for item in od[nm]:
                if item[0] == "bar":
                    _, t0, t1, kind = item
                    o.append(f'<rect x="{r2(x(t0))}" y="{ln[nm]-3}" width="{r2(x(t1)-x(t0))}" height="6" rx="3" fill="{KIND[kind][2]}" opacity="0.55"/>')
                    continue
                _, t0, lab, kind, human = item
                xa = x(t0)
                w = len(lab) * 6.6 + 12
                cls = ocls(k) if human else KIND[kind]
                sw = 1.5 if human else 0.5
                o.append(chip(cls, f'<rect x="{r2(xa)}" y="{ln[nm]-8}" width="{r2(w)}" height="16" rx="3"{{fill}}{{stroke}} stroke-width="{{sw}}"/>',
                              f'<text {{tcls}}{{tfill}} x="{r2(xa+w/2)}" y="{ln[nm]+4}" text-anchor="middle">{lab}</text>', sw))
        for t0, t1, lab, kind in od["state"]:
            xa, xb = x(t0) + 1, x(t1) - 1
            o.append(chip(KIND[kind], f'<rect x="{r2(xa)}" y="{ln["state"]-8}" width="{r2(xb-xa)}" height="16" rx="4"{{fill}}{{stroke}} stroke-width="{{sw}}"/>',
                          f'<text {{tcls}}{{tfill}} x="{r2((xa+xb)/2)}" y="{ln["state"]+4}" text-anchor="middle">{lab}</text>'))

    # event table
    o.append(f'<text {th()} x="{TCOL}" y="{table_top+16}">Time</text>')
    for k, cx0 in ((1, LCOL), (2, RCOL)):
        o.append(f'<rect x="{cx0}" y="{table_top+5}" width="{CW}" height="16" rx="3" fill="{ORD[k][2]}" opacity="0.18"/>')
        o.append(f'<text {th()} x="{cx0+8}" y="{table_top+17}">{ORD[k][0]}</text>')
    for t, lc, rc, top, h in rows_geo:
        o.append(f'<text {ts()} x="{TCOL}" y="{top+PAD+11}">{hm(t)}</text>')
        o.append(f'<line x1="{TCOL}" y1="{top-2}" x2="674" y2="{top-2}" stroke="{grid}" stroke-width="0.5"/>')
        for k, cell, cx0 in ((1, lc, LCOL), (2, rc, RCOL)):
            if not cell:
                continue
            kind, lines = cell
            if kind == "you":
                body = "".join(f'<text {{tcls}}{{tfill}} x="{cx0+8}" y="{top+PAD+11+LINE_H*i}"' + ((' style="font-weight:500"' if W else ' font-weight="600"') if i == 0 else '') + f'>{ln}</text>' for i, ln in enumerate(lines))
                o.append(chip(ocls(k), f'<rect x="{cx0}" y="{top}" width="{CW}" height="{h}" rx="5"{{fill}}{{stroke}} stroke-width="{{sw}}"/>', body, 1.5))
                continue
            if kind == "brk":
                o.append(f'<rect x="{cx0}" y="{top}" width="{CW}" height="{h}" rx="5" fill="none" stroke="#888780" stroke-width="1"/>')
            else:
                o.append(f'<rect x="{cx0}" y="{top}" width="{CW}" height="{h}" rx="5" fill="none" stroke="{ORD[k][2]}" stroke-width="0.8" stroke-dasharray="3 2"/>')
            for i, ln in enumerate(lines):
                o.append(f'<text {ts()} x="{cx0+8}" y="{top+PAD+11+LINE_H*i}">{ln}</text>')
    for i, f in enumerate(sc["foot"]):
        o.append(f'<text {ts()} x="{TCOL}" y="{foot_y+LINE_H*i}">{f}</text>')
    o.append('</svg>')
    return "\n".join(o)


if __name__ == "__main__":
    sys.stdout.write(build(sys.argv[1], sys.argv[2]))
