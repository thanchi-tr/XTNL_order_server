"""Generate the ARM break-even time-price figure (illustrative sample).

Writes two SVGs: a themed one for the chat widget (classes / CSS vars) and a
standalone light one for docs/design/arm-sl-sample/time-price.svg.
"""
import random
import sys
from decimal import Decimal, ROUND_HALF_EVEN

E = 1.10000
S0 = 1.09800
R = E - S0

# ---- price anchors (seconds after 10:00:00, bid) --------------------------
ANCH = [
    (0, 1.09985), (90, 1.09960), (200, 1.10000), (400, 1.10030), (660, 1.10090), (860, 1.10045), (900, 1.10060),
    (1020, 1.10040), (1560, 1.10180), (1760, 1.10160), (1800, 1.10150),
    (1980, 1.10120), (2400, 1.10200), (2610, 1.10240), (2660, 1.10215), (2680, 1.10205), (2700, 1.10190),
    (2711, 1.10188), (2820, 1.10170), (3300, 1.10270), (3560, 1.10235), (3600, 1.10230),
    (3611, 1.10225), (3720, 1.10200), (3840, 1.10215), (4140, 1.10290), (4460, 1.10270), (4500, 1.10260),
    (4511, 1.10262), (4571, 1.10255), (4800, 1.10270), (5100, 1.10280), (5400, 1.10275),
]
BARS = [  # start, open, high, low, close
    (0, 1.09985, 1.10090, 1.09960, 1.10060),
    (900, 1.10060, 1.10180, 1.10040, 1.10150),
    (1800, 1.10150, 1.10240, 1.10120, 1.10190),
    (2700, 1.10190, 1.10270, 1.10170, 1.10230),
    (3600, 1.10230, 1.10290, 1.10200, 1.10260),
    (4500, 1.10260, 1.10300, 1.10240, 1.10275),
]


def bar_of(t):
    for b in reversed(BARS):
        if t >= b[0]:
            return b
    return BARS[0]


def path_points():
    rnd = random.Random(7)
    times = sorted(set([t for t, _ in ANCH] + list(range(0, 5401, 20))))
    av = dict(ANCH)
    keys = [t for t, _ in ANCH]
    out = []
    for t in times:
        if t in av:
            out.append((t, av[t]))
            continue
        i = max(k for k in range(len(keys)) if keys[k] < t)
        a, b = ANCH[i], ANCH[i + 1]
        f = (t - a[0]) / (b[0] - a[0])
        base = a[1] + f * (b[1] - a[1])
        noise = rnd.gauss(0, 0.000045) * (4 * f * (1 - f)) ** 0.5
        p = base + noise
        bs, o, h, l, c = bar_of(t)
        p = min(max(p, l + 0.00002), h - 0.00002)
        out.append((t, p))
    return out


# ---- geometry -------------------------------------------------------------
X0, PXMIN = 70.0, 5.8          # 10:00 at x=70, 5.8 px per minute, 90 minutes -> 592
PTOP, PBOT, Y0, Y1 = 1.10380, 1.09760, 50.0, 320.0


def x(t):
    return X0 + t / 60.0 * PXMIN


def y(p):
    return Y0 + (PTOP - p) / (PTOP - PBOT) * (Y1 - Y0)


def r2(v):
    return f"{v:.1f}".rstrip("0").rstrip(".")


def rr(price):
    """Favour in R at 0.01 R, half-even, computed exactly from a price string."""
    v = (Decimal(price) - Decimal("1.10000")) / Decimal("0.00200")
    return str(v.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))


def hm(t):
    s = 36000 + t
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"


# Human actions: (tag, t, action, [effect lines]). Everything the person does.
HUMAN = [
    ("H1", 200, "You go long: fill 1.10000 with the stop at 1.09800.", [
        "Fixes entry 1.10000 and 1R = 0.00200. Armed within ~2 s; no notice is sent.",
        "From now on the order-server checks this position at every bar end.",
    ]),
    ("H2", 2680, "You tighten the stop to 1.09900, acting on heads-up 5.", [
        "Your real risk is now 0.5R, but bar 2 is still tested at 1.09800 (heads-up).",
        "After bar 2, 1.09800 stays the test basis until a bar end confirms the trigger,",
        "so acting on a heads-up cannot confirm itself. Trigger line stays at 1.10200.",
    ]),
    ("H3", 3840, "You follow advice 8: stop to 1.10000 (break-even).", [
        "Risk is 0. No state change until the 11:15 bar end; heads-up 9 stays silent.",
        "That bar end releases the position: COMPLETE at 11:16:11, Advice > Complete.",
        "Without this move the advice stays outstanding and is re-checked every bar.",
    ]),
]
# Order-server actions: (number, lane, t, text). Automatic, read-only, advice only.
EV = [
    ("1", "pre", 860, f"PRE bar 0: live {rr('1.10045')}R, below trigger; first-bar check pending. Email."),
    ("2", "post", 911, "POST bar 0: calm (range 1.18 × ATR14). 0.30R, below trigger. Armed > Tracking."),
    ("3", "pre", 1760, f"PRE bar 1: live {rr('1.10160')}R, below trigger. Provisional email."),
    ("4", "post", 1811, "POST bar 1: close 0.75R, below trigger. Decision row + email."),
    ("5", "pre", 2660, f"PRE bar 2: live {rr('1.10215')}R > 1R. Heads-up: may advise 1.10000. Stop noted."),
    ("6", "post", 2711, "POST bar 2: 0.95R vs 1R: not confirmed, basis 1.09800 kept. Email ratio 1.900*"),
    ("7", "pre", 3560, f"PRE bar 3: live {rr('1.10235')}R vs held basis 1R. Heads-up again (basis held)."),
    ("8", "post", 3611, "POST bar 3: close 1.15R > 1R. ADJUST: move stop to 1.10000. Basis cleared."),
    ("9", "pre", 4460, "PRE bar 4: stop already at target, no basis held. No PRE item, no email."),
    ("10", "post", 4511, "POST bar 4: 1.30R > 0R, at target. Release held for a verification re-read."),
    ("10a", None, 4560, "No decision at T+60 s: part-0 email lists it as awaiting the final bar.*"),
    ("10b", None, 4571, "Re-read confirms: COMPLETE, released. Part-1 email, marked DELAYED.*"),
]
LANE_Y = {"you": 356, "pre": 380, "post": 404}
STATES = [  # start t, end t, label, widget class, light fill, light stroke, light text
    (202, 911, "Armed", "c-purple", "#EEEDFE", "#534AB7", "#3C3489"),
    (911, 3611, "Tracking", "c-purple", "#EEEDFE", "#534AB7", "#3C3489"),
    (3611, 4571, "Advice", "c-pink", "#FBEAF0", "#993556", "#72243E"),
    (4571, 5400, "Complete", "c-teal", "#E1F5EE", "#0F6E56", "#085041"),
]
CORAL = ("c-coral", "#FAECE7", "#993C1D", "#712B13")
HUMAN_LINE = "#D85A30"


def human_blocks(top):
    """Yield (tag, t, action, effects, y_top, height) for the human list."""
    yy = top
    for tag, t, action, effects in HUMAN:
        h = 26 + 16 * len(effects)
        yield tag, t, action, effects, yy, h
        yy += h + 6


def layout():
    hum_head = 472
    blocks = list(human_blocks(hum_head + 10))
    end_h = blocks[-1][4] + blocks[-1][5]
    srv_head = end_h + 28
    srv_first = srv_head + 22
    foot = srv_first + len(EV) * 18 + 6
    return hum_head, blocks, srv_head, srv_first, foot, foot + 20


def build(theme):
    W = theme == "widget"
    txt = (lambda cls: f'class="{cls}"') if W else (lambda cls: 'fill="#444441" font-weight="600"' if cls == "th" else 'fill="#5F5E5A"')
    ink = "var(--p)" if W else "#2C2C2A"
    grid = "var(--b)" if W else "#D3D1C7"
    band = "var(--bg-neutral)" if W else "#F1EFE8"
    hum_head, blocks, srv_head, srv_first, foot, H = layout()
    o = []
    head = f'<svg width="100%" viewBox="0 0 680 {H}" role="img"' + ('' if W else ' xmlns="http://www.w3.org/2000/svg" font-family="Segoe UI, system-ui, sans-serif" font-size="12"') + '>'
    o.append(head)
    o.append('<title>ARM break-even stop advice: time-price sample</title>')
    o.append('<desc>Illustrative long trade from 10:03:20 to 11:30. Bid price path, stop and trigger levels, human actions H1 to H3 highlighted with their effects, and the order-server heads-up (PRE) and bar-end (POST) checks with the SM11 state over time.</desc>')
    if not W:
        o.append(f'<rect width="680" height="{H}" fill="#ffffff"/>')
    # legend
    lx = 70

    def leg(sym, label, w):
        nonlocal lx
        o.append(sym(lx))
        o.append(f'<text {txt("ts")} x="{lx+26}" y="28">{label}</text>')
        lx += 26 + w + 12
    leg(lambda a: f'<line x1="{a}" y1="24" x2="{a+20}" y2="24" stroke="{ink}" stroke-width="1.2"/>', "Bid", 22)
    leg(lambda a: f'<line x1="{a}" y1="24" x2="{a+20}" y2="24" stroke="{HUMAN_LINE}" stroke-width="2"/>', "Your stop", 58)
    leg(lambda a: f'<line x1="{a}" y1="24" x2="{a+20}" y2="24" stroke="#BA7517" stroke-width="1.2" stroke-dasharray="4 3"/>', "Trigger", 44)
    leg(lambda a: f'<line x1="{a}" y1="24" x2="{a+20}" y2="24" stroke="#888780" stroke-width="1" stroke-dasharray="4 3"/>', "Entry", 32)
    leg(lambda a: f'<circle cx="{a+10}" cy="24" r="4" fill="none" stroke="#BA7517" stroke-width="1.5"/>', "PRE quote", 60)
    leg(lambda a: f'<circle cx="{a+10}" cy="24" r="3" fill="{ink}"/>', "Bar close", 56)
    leg(lambda a: f'<line x1="{a+10}" y1="16" x2="{a+10}" y2="32" stroke="{HUMAN_LINE}" stroke-width="1.5" stroke-dasharray="3 2"/>', "You act", 44)
    # bar bands
    for i, b in enumerate(BARS[:5]):
        xa, xb = x(b[0]), x(b[0] + 900)
        if i % 2 == 0:
            o.append(f'<rect x="{r2(xa)}" y="{Y0}" width="{r2(xb-xa)}" height="{Y1-Y0}" fill="{band}"/>')
        o.append(f'<text {txt("ts")} x="{r2((xa+xb)/2)}" y="66" text-anchor="middle">bar {i}</text>')
    # y grid + ticks
    for k in range(6):
        p = 1.09800 + k * 0.001
        yy = y(p)
        o.append(f'<line x1="{X0}" y1="{r2(yy)}" x2="592" y2="{r2(yy)}" stroke="{grid}" stroke-width="0.5"/>')
        o.append(f'<text {txt("ts")} x="62" y="{r2(yy+4)}" text-anchor="end">{p:.5f}</text>')
    # human action guide lines: from the chip down through the chart to the You lane
    for tag, t, *_ in HUMAN:
        xx = r2(x(t))
        o.append(f'<line x1="{xx}" y1="96" x2="{xx}" y2="{LANE_Y["you"] - 11}" stroke="{HUMAN_LINE}" stroke-width="1.5" stroke-dasharray="3 2" opacity="0.8"/>')
    # entry, trigger, stop
    o.append(f'<line x1="{r2(x(200))}" y1="{r2(y(E))}" x2="592" y2="{r2(y(E))}" stroke="#888780" stroke-width="1" stroke-dasharray="4 3"/>')
    trig = f'M{r2(x(200))} {r2(y(1.10200))} H{r2(x(3611))} V{r2(y(1.10100))} H{r2(x(3840))} V{r2(y(E))}'
    o.append(f'<path d="{trig}" fill="none" stroke="#BA7517" stroke-width="1.2" stroke-dasharray="4 3"/>')
    stop = f'M{r2(x(200))} {r2(y(S0))} H{r2(x(2680))} V{r2(y(1.09900))} H{r2(x(3840))} V{r2(y(E))} H592'
    o.append(f'<path d="{stop}" fill="none" stroke="{HUMAN_LINE}" stroke-width="2" stroke-linejoin="round"/>')
    # price path
    pts = " ".join(f"{r2(x(t))},{r2(y(p))}" for t, p in path_points())
    o.append(f'<polyline points="{pts}" fill="none" stroke="{ink}" stroke-width="1.2" stroke-linejoin="round"/>')
    for b in BARS[:5]:
        o.append(f'<circle cx="{r2(x(b[0]+900))}" cy="{r2(y(b[4]))}" r="3" fill="{ink}"/>')
    for t, p in [(860, 1.10045), (1760, 1.10160), (2660, 1.10215), (3560, 1.10235)]:
        o.append(f'<circle cx="{r2(x(t))}" cy="{r2(y(p))}" r="4" fill="none" stroke="#BA7517" stroke-width="1.5"/>')
    # human chips at the top of each guide line
    for tag, t, *_ in HUMAN:
        cx = x(t)
        if W:
            o.append(f'<g class="{CORAL[0]}"><rect x="{r2(cx-15)}" y="76" width="30" height="20" rx="4" stroke-width="1"/><text class="th" x="{r2(cx)}" y="86" text-anchor="middle" dominant-baseline="central">{tag}</text></g>')
        else:
            o.append(f'<rect x="{r2(cx-15)}" y="76" width="30" height="20" rx="4" fill="{CORAL[1]}" stroke="{CORAL[2]}"/><text x="{r2(cx)}" y="90" text-anchor="middle" fill="{CORAL[3]}" font-weight="600">{tag}</text>')
    # axis
    o.append(f'<line x1="{X0}" y1="{Y1}" x2="592" y2="{Y1}" stroke="{grid}" stroke-width="0.5"/>')
    for m in range(0, 91, 15):
        s = 36000 + m * 60
        o.append(f'<text {txt("ts")} x="{r2(x(m*60))}" y="336" text-anchor="middle">{s//3600:02d}:{s%3600//60:02d}</text>')
    # lanes
    for lane, label in (("you", "You"), ("pre", "PRE"), ("post", "POST")):
        yy = LANE_Y[lane]
        o.append(f'<text {txt("th" if lane == "you" else "ts")} x="62" y="{yy+4}" text-anchor="end">{label}</text>')
        o.append(f'<line x1="{X0}" y1="{yy}" x2="592" y2="{yy}" stroke="{HUMAN_LINE if lane == "you" else grid}" stroke-width="0.5" stroke-dasharray="2 4"/>')
    lane_marks = [(tag, "you", t) for tag, t, *_ in HUMAN] + [(n, lane, t) for n, lane, t, _ in EV if lane]
    for n, lane, t in lane_marks:
        yy = LANE_Y[lane]
        cls = {"you": CORAL, "pre": ("c-amber", "#FAEEDA", "#854F0B", "#633806"), "post": ("c-gray", "#F1EFE8", "#5F5E5A", "#444441")}[lane]
        rad = 11 if lane == "you" else 9
        sw = 1 if lane == "you" else 0.5
        if W:
            o.append(f'<g class="{cls[0]}"><circle cx="{r2(x(t))}" cy="{yy}" r="{rad}" stroke-width="{sw}"/><text class="ts" x="{r2(x(t))}" y="{yy}" text-anchor="middle" dominant-baseline="central">{n}</text></g>')
        else:
            o.append(f'<circle cx="{r2(x(t))}" cy="{yy}" r="{rad}" fill="{cls[1]}" stroke="{cls[2]}" stroke-width="{sw}"/><text x="{r2(x(t))}" y="{yy+4}" text-anchor="middle" fill="{cls[3]}">{n}</text>')
    # state band
    o.append(f'<text {txt("ts")} x="62" y="434" text-anchor="end">State</text>')
    for a, b, label, cls, fl, st, tx in STATES:
        xa, xb = x(a) + 1, x(b) - 1
        cx, w = (xa + xb) / 2, xb - xa
        if W:
            o.append(f'<g class="{cls}"><rect x="{r2(xa)}" y="420" width="{r2(w)}" height="22" rx="4" stroke-width="0.5"/><text class="ts" x="{r2(cx)}" y="431" text-anchor="middle" dominant-baseline="central">{label}</text></g>')
        else:
            o.append(f'<rect x="{r2(xa)}" y="420" width="{r2(w)}" height="22" rx="4" fill="{fl}" stroke="{st}" stroke-width="0.5"/><text x="{r2(cx)}" y="435" text-anchor="middle" fill="{tx}">{label}</text>')
    # human action list (highlighted)
    o.append(f'<text {txt("th")} x="40" y="{hum_head}">Human actions (you) and their effect</text>')
    for tag, t, action, effects, top, h in blocks:
        if W:
            o.append(f'<g class="{CORAL[0]}"><rect x="40" y="{top}" width="612" height="{h}" rx="6" stroke-width="1"/>'
                     f'<text class="th" x="52" y="{top+17}">{tag}</text>'
                     f'<text class="th" x="84" y="{top+17}">{hm(t)}</text>'
                     f'<text class="th" x="150" y="{top+17}">{action}</text>'
                     f'<text class="ts" x="84" y="{top+35}">Effect</text>'
                     + "".join(f'<text class="ts" x="150" y="{top+35+16*i}">{line}</text>' for i, line in enumerate(effects))
                     + '</g>')
        else:
            o.append(f'<rect x="40" y="{top}" width="612" height="{h}" rx="6" fill="{CORAL[1]}" stroke="{CORAL[2]}"/>')
            o.append(f'<text x="52" y="{top+17}" fill="{CORAL[3]}" font-weight="600">{tag}</text>')
            o.append(f'<text x="84" y="{top+17}" fill="{CORAL[3]}" font-weight="600">{hm(t)}</text>')
            o.append(f'<text x="150" y="{top+17}" fill="{CORAL[3]}" font-weight="600">{action}</text>')
            o.append(f'<text x="84" y="{top+35}" fill="{CORAL[2]}">Effect</text>')
            for i, line in enumerate(effects):
                o.append(f'<text x="150" y="{top+35+16*i}" fill="{CORAL[2]}">{line}</text>')
    # order-server list
    o.append(f'<text {txt("th")} x="40" y="{srv_head}">Order-server actions (automatic, advice only, nothing sent to the broker)</text>')
    for i, (n, lane, t, text) in enumerate(EV):
        yy = srv_first + i * 18
        o.append(f'<text {txt("th")} x="76" y="{yy}" text-anchor="end">{n}</text>')
        o.append(f'<text {txt("ts")} x="84" y="{yy}">{hm(t)}</text>')
        o.append(f'<text {txt("ts")} x="150" y="{yy}">{text}</text>')
    o.append(f'<text {txt("ts")} x="84" y="{foot}">* Current behaviour flagged in review: ratio uses the moved stop; every release is DELAYED.</text>')
    o.append('</svg>')
    return "\n".join(o)


if __name__ == "__main__":
    which = sys.argv[1]
    sys.stdout.write(build(which))
