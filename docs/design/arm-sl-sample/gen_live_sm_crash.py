"""Generate the design 3 SM13 intent state machine and the crash figure (design only).

Usage: python gen_live_sm_crash.py <sm13|crash> <widget|file>
The widget variant uses the chat host's classes; the file variant embeds an
equivalent light stylesheet so the SVG stands alone.
"""
import sys

FILE_STYLE = """<style>
text{font-family:'Segoe UI',system-ui,sans-serif}
.th{font-size:14px;font-weight:600;fill:#2C2C2A}.ts{font-size:12px;fill:#5F5E5A}
.arr{stroke:#73726C;stroke-width:1.5;fill:none}
.box{fill:#F8F7F3;stroke:#B4B2A9}
.c-gray>rect{fill:#F1EFE8;stroke:#5F5E5A}.c-gray>.th{fill:#444441}.c-gray>.ts{fill:#5F5E5A}
.c-amber>rect{fill:#FAEEDA;stroke:#854F0B}.c-amber>.th{fill:#633806}.c-amber>.ts{fill:#854F0B}
.c-coral>rect{fill:#FAECE7;stroke:#993C1D}.c-coral>.th{fill:#712B13}.c-coral>.ts{fill:#993C1D}
.c-teal>rect{fill:#E1F5EE;stroke:#0F6E56}.c-teal>.th{fill:#085041}.c-teal>.ts{fill:#0F6E56}
.c-red>rect{fill:#FCEBEB;stroke:#A32D2D}.c-red>.th{fill:#791F1F}.c-red>.ts{fill:#A32D2D}
</style>"""
MARKER = '<defs><marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="{ms}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>'


def node(cls, x, y, w, title, sub, h=52, dashed=False):
    dash = ' stroke-dasharray="4 3"' if dashed else ""
    return (f'<g class="{cls}"><rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" stroke-width="0.8"{dash}/>'
            f'<text class="th" x="{x + w / 2}" y="{y + 19}" text-anchor="middle" dominant-baseline="central">{title}</text>'
            f'<text class="ts" x="{x + w / 2}" y="{y + 37}" text-anchor="middle" dominant-baseline="central">{sub}</text></g>')


def arrow(d, label=None, lx=0, ly=0, anchor="middle"):
    s = f'<path d="{d}" class="arr" fill="none" marker-end="url(#arrow)"/>'
    if label:
        s += f'<text class="ts" x="{lx}" y="{ly}" text-anchor="{anchor}">{label}</text>'
    return s


def sm13():
    L, C, R = 40, 265, 475
    LW, CW, RW = 165, 150, 165
    r = [40, 150, 260, 370, 480]
    o = []
    o.append(node("c-coral", L, r[0], LW, "Awaiting approval", "Phase 3: you decide"))
    o.append(node("c-gray", C, r[0], CW, "Proposed", "Bar-end ADJUST"))
    o.append(node("c-gray", R, r[0], RW, "Cancelled · expired", "Superseded, closed, off", dashed=True))
    o.append(node("c-coral", L, r[1], LW, "Approved", "Your OK committed"))
    o.append(node("c-gray", C, r[1], CW, "Checking", "Gate, re-read, price"))
    o.append(node("c-gray", R, r[1], RW, "Held", "Retry every 5 s"))
    o.append(node("c-gray", L, r[2], LW, "Refused", "Final check failed", dashed=True))
    o.append(node("c-amber", C, r[2], CW, "Sending", "Row first, then write"))
    o.append(node("c-amber", R, r[2], RW, "Unknown", "Never re-sent"))
    o.append(node("c-gray", L, r[3], LW, "Broker rejected", "Paged once", dashed=True))
    o.append(node("c-amber", C, r[3], CW, "Acked", "Confirm read ≤ 30 s"))
    o.append(node("c-red", R, r[3], RW, "Operator", "Critical page"))
    o.append(node("c-coral", L, r[4], LW, "Rejected by you", "From awaiting approval", dashed=True))
    o.append(node("c-teal", C, r[4], CW, "Confirmed", "Stop at target"))
    o.append(node("c-gray", R, r[4], RW, "Not applied", "Or abandoned (operator)", dashed=True))
    cx = C + CW / 2
    o.append(arrow(f"M{C} 66 H{L + LW + 2}", "P3", (C + L + LW) / 2, 60))
    o.append(arrow(f"M{L + LW / 2} {r[0] + 52} V{r[1] - 2}", "you approve", L + LW / 2 + 8, 126, "start"))
    o.append(arrow(f"M{L + LW} {r[1] + 26} H{C - 2}"))
    o.append(arrow(f"M{cx} {r[0] + 52} V{r[1] - 2}", "P4 / P5", cx + 8, 126, "start"))
    o.append(arrow(f"M{C + CW} 66 H{R - 2}", "any*", (C + CW + R) / 2, 60))
    o.append(arrow(f"M{C + CW} {r[1] + 18} H{R - 2}", "retry", (C + CW + R) / 2, r[1] + 12))
    o.append(arrow(f"M{R} {r[1] + 36} H{C + CW + 2}"))
    o.append(arrow(f"M{C + 25} {r[1] + 52} V{r[1] + 80} H{L + LW / 2} V{r[2] - 2}", "final fail", C - 8, r[1] + 74, "end"))
    o.append(arrow(f"M{cx} {r[1] + 52} V{r[2] - 2}", "all pass", cx + 8, 236, "start"))
    o.append(arrow(f"M{C + CW} {r[2] + 26} H{R - 2}", "lost", (C + CW + R) / 2, r[2] + 20))
    o.append(arrow(f"M{C + 25} {r[2] + 52} V{r[2] + 80} H{L + LW / 2} V{r[3] - 2}", "refused", C - 8, r[2] + 74, "end"))
    o.append(arrow(f"M{cx} {r[2] + 52} V{r[3] - 2}", "accepted", cx + 8, 346, "start"))
    o.append(arrow(f"M{C + CW} {r[3] + 26} H{R - 2}", "no match", (C + CW + R) / 2, r[3] + 20))
    o.append(arrow(f"M{R + RW / 2} {r[2] + 52} V{r[3] - 2}", "unclear", R + RW / 2 + 8, 346, "start"))
    o.append(arrow(f"M{cx} {r[3] + 52} V{r[4] - 2}", "at target", cx + 8, 456, "start"))
    o.append(arrow(f"M{R + RW / 2} {r[3] + 52} V{r[4] - 2}", "resolves", R + RW / 2 + 8, 456, "start"))
    ly = 560
    legend = [("c-gray", "Before send"), ("c-amber", "In flight"), ("c-coral", "Needs you"), ("c-red", "Needs an operator"), ("c-teal", "Moved")]
    x = 40
    for cls, lab in legend:
        o.append(f'<g class="{cls}"><rect x="{x}" y="{ly}" width="14" height="14" rx="3" stroke-width="0.8"/></g><text class="ts" x="{x + 20}" y="{ly + 11}">{lab}</text>')
        x += 20 + len(lab) * 6.6 + 18
    o.append(f'<rect x="{x}" y="{ly}" width="14" height="14" rx="3" fill="none" stroke="#888780" stroke-dasharray="3 2"/><text class="ts" x="{x + 20}" y="{ly + 11}">Terminal</text>')
    o.append(f'<text class="ts" x="40" y="{ly + 36}">* From any state before Sending (superseded, closed, suspended, failed, disarmed, switch off, at target, expiry);</text>')
    o.append(f'<text class="ts" x="40" y="{ly + 52}">  Acked / Unknown end Cancelled only after a read shows the position closed. Unknown at target → Confirmed.</text>')
    return 620, o, "Design 3 SM13: stop-move intent state machine", "Per stop-move intent: Proposed, Phase 3 approval, checks with retry, sending, acked or unknown, operator, and terminal states."


CRASH = [
    ("K1", "PRE → POST", ["No decision, no move. Broker keeps", "the old stop."], ["Before cap: POST (delayed) or LATE,", "can still move. After cap: next bar."]),
    ("K2", "Intent unsent", ["Nothing sent; old stop stays. P3:", "your approval cannot be processed."], ["Resumes in its saved state and mode;", "P3 never skips approval; may expire."]),
    ("K3", "Sending", ["Old or new stop at the broker:", "both protect (single in-place amend)."], ["Unknown → read: at target = moved;", "else operator page. Never re-sent."]),
    ("K4", "Approval wait", ["Request delivered; approving", "does nothing yet."], ["Approval kept: applied before expiry,", "refused after."]),
    ("K5", "Acked", ["New or old stop: both protect."], ["Confirm window re-checked: at target", "= moved, else operator page."]),
    ("K6", "Moved", ["Stop already at break-even."], ["Next bar end sees the target and", "releases the position."]),
    ("K7", "PC off / asleep", ["Nothing runs until you log on.", "The broker stop protects."], ["Fence → restart; missed bars spanned", "'downtime'; old intents expire."]),
    ("K8", "Crash loop", ["—"], ["SAFE_HOLD: nothing sent, paged. Live", "moves stay off until you re-enable."]),
]
STRIP = [("PRE", "heads-up"), ("POST", "bar end"), ("Intent", "proposed"), ("Approve", "P3 only"), ("Send", "row + write"), ("Ack", "confirm read"), ("Moved", "stop at BE")]
K_AT = {"K1": 0.5, "K2": 2.0, "K4": 3.0, "K3": 4.0, "K5": 5.0, "K6": 6.0}


def crash():
    o = []
    bw, gap, x0, y0 = 76, 10, 40, 56
    for i, (t, s) in enumerate(STRIP):
        x = x0 + i * (bw + gap)
        cls = "c-coral" if t == "Approve" else ("c-teal" if t == "Moved" else ("c-amber" if t in ("Send", "Ack") else "c-gray"))
        o.append(node(cls, x, y0, bw, t, s, h=46))
        if i:
            o.append(arrow(f"M{x - gap} {y0 + 23} H{x - 2}"))
    for k, pos in K_AT.items():
        cx = x0 + pos * (bw + gap) + bw / 2 if pos != 0.5 else x0 + bw + gap / 2
        o.append(f'<g class="c-red"><rect x="{cx - 15}" y="20" width="30" height="20" rx="4" stroke-width="0.8"/><text class="ts" x="{cx}" y="30" text-anchor="middle" dominant-baseline="central">{k}</text></g>')
        o.append(f'<line x1="{cx}" y1="40" x2="{cx}" y2="{y0 - 2}" stroke="#A32D2D" stroke-width="1" stroke-dasharray="2 2"/>')
    o.append('<text class="ts" x="40" y="14">K1–K6 along one stop move; K7, K8 can happen any time.</text>')
    top = 126
    o.append(f'<text class="th" x="40" y="{top}">Crash point</text><text class="th" x="196" y="{top}">Stays down</text><text class="th" x="420" y="{top}">Recovers (restart / failover)</text>')
    y = top + 12
    for k, title, down, rec in CRASH:
        h = 14 + 15 * max(len(down), len(rec))
        o.append(f'<g class="c-red"><rect x="40" y="{y}" width="30" height="20" rx="4" stroke-width="0.8"/><text class="ts" x="55" y="{y + 10}" text-anchor="middle" dominant-baseline="central">{k}</text></g>')
        o.append(f'<text class="ts" x="76" y="{y + 14}">{title}</text>')
        o.append(f'<g class="c-gray"><rect x="190" y="{y}" width="220" height="{h}" rx="6" stroke-width="0.6"/>' + "".join(f'<text class="ts" x="198" y="{y + 15 + 15 * i}">{ln}</text>' for i, ln in enumerate(down)) + '</g>')
        o.append(f'<g class="c-teal"><rect x="416" y="{y}" width="236" height="{h}" rx="6" stroke-width="0.6"/>' + "".join(f'<text class="ts" x="424" y="{y + 15 + 15 * i}">{ln}</text>' for i, ln in enumerate(rec)) + '</g>')
        y += h + 6
    y += 10
    for ln in ["Baseline: the stop rests at the broker, so a crash can lose or delay a move, never remove a stop",
               "(only with a single in-place amend, prerequisite P2). Who tells you if it stays down: the engine's",
               "own page while the PC is up; if the PC is off or the supervisor died, only the off-PC dead-man (P4)."]:
        o.append(f'<text class="ts" x="40" y="{y}">{ln}</text>')
        y += 15
    return y + 10, o, "Design 3: crash with and without recovery", "Crash points K1 to K8 along the stop-move lifecycle, with what happens if the order-server stays down and if it recovers."


def build(which, theme):
    H, body, title, desc = (sm13() if which == "sm13" else crash())
    W = theme == "widget"
    head = f'<svg width="100%" viewBox="0 0 680 {int(H)}" role="img"' + ('' if W else ' xmlns="http://www.w3.org/2000/svg"') + '>'
    parts = [head, f'<title>{title}</title>', f'<desc>{desc}</desc>', MARKER.format(ms="context-stroke" if W else "#73726C")]
    if not W:
        parts += [FILE_STYLE, f'<rect width="680" height="{int(H)}" fill="#ffffff"/>']
    return "\n".join(parts + body + ["</svg>"])


if __name__ == "__main__":
    sys.stdout.write(build(sys.argv[1], sys.argv[2]))
