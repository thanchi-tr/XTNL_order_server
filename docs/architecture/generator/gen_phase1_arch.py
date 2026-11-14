"""Generate docs/architecture/phase1_ingestion_arch.drawio (5 pages).

  1 Architecture    - 5 swimlanes (broker, adapters, bus, Redis/DB/image, Oversee),
                      data flow, pattern stereotypes, lane comments
  2 Design Patterns - UML class view per pattern + catalogue
  3 State Machines  - order, DLQ row, integrity worker, adapter connection,
                      oversee halt switch, alert delivery, position lifecycle,
                      Redis-loss bootstrap
  4 Use Cases       - actors, boundary, include / extend / generalisation
  5 Deployment      - London live node, Melbourne disaster copy / oversight, latency budget

Readability rules
  * every element carries a coloured KIND tag (interface, class, object ...);
    the colour is the same on every page and explained in each page legend
  * every element has a hover tooltip with detail; each lane / frame has a
    visible comment note
  * box heights come from measured text (measured.json, produced by rendering
    the previous build in draw.io) so rows are equal-height and padding is even

Self-checks per page: overlaps, containment, edges through boxes, edge crossings.

usage (from repo root):
  python docs/architecture/generator/gen_phase1_arch.py docs/architecture/phase1_ingestion_arch.drawio docs/architecture/generator/measured.json

See docs/architecture/generator/README.md for re-measuring text sizes after editing labels.
"""
import itertools
import json
import math
import os
import sys
import xml.etree.ElementTree as ET

OUT = sys.argv[1]
MEAS = sys.argv[2] if len(sys.argv) > 2 else None
MEASURED = json.load(open(MEAS, encoding="utf-8")) if MEAS and os.path.exists(MEAS) else {}

# ------------------------------------------------------------------ element kinds
#            tag text              fill       stroke     tag/dark
KINDS = {
    "interface": ("INTERFACE",       "#DAE8FC", "#6C8EBF", "#2B5797"),
    "abstract":  ("ABSTRACT CLASS",  "#E1D5E7", "#9673A6", "#6A3D8F"),
    "class":     ("CLASS",           "#D5E8D4", "#82B366", "#2E7D32"),
    "active":    ("ACTIVE CLASS",    "#FCE4EC", "#C2185B", "#AD1457"),
    "object":    ("OBJECT",          "#D0F0EE", "#2A9D8F", "#1B6F66"),
    "value":     ("VALUE OBJECT",    "#FFE6CC", "#D79B00", "#B45309"),
    "enum":      ("ENUM",            "#F4F4C8", "#A6A62B", "#6E6E00"),
    "store":     ("DATA STORE",      "#EFE3D6", "#A0522D", "#7B3F14"),
    "external":  ("EXTERNAL SYSTEM", "#F2F2F2", "#7A7A7A", "#4D4D4D"),
    "guard":     ("SECURITY GUARD",  "#F8CECC", "#B85450", "#B71C1C"),
    "note":      ("COMMENT",         "#FFF9DB", "#D6B656", "#8A6D00"),
}
KIND_DESC = {
    "interface": "contract only, no state",
    "abstract": "base class with shared behaviour",
    "class": "concrete class",
    "active": "runs its own asyncio task",
    "object": "runtime instance",
    "value": "immutable data (frozen)",
    "enum": "fixed set of values",
    "store": "table / database / backup",
    "external": "outside our process",
    "guard": "blocks order execution",
    "note": "explanation, not code",
}
PAT = "#0B6E99"
LANE = {  # header colour, border colour
    "L1": ("#FFE6CC", "#D79B00"),
    "L2": ("#DAE8FC", "#6C8EBF"),
    "L3": ("#E1D5E7", "#9673A6"),
    "L4": ("#D5E8D4", "#82B366"),
    "L5": ("#FCE4EC", "#C2185B"),
}
RED = "#CC0000"
C_DATA, C_DLQ, C_INT, C_CONN = "#333333", "#7B4F96", "#2E7D32", "#B46504"
C_OVR = "#AD1457"  # oversee / alerts

mxfile = ET.Element("mxfile", host="app.diagrams.net", agent="generated", version="24.7.0")


def ceil10(v):
    return int(math.ceil(v / 10.0) * 10)


def tag(kind, stereo=None, pid=None):
    """Coloured KIND tag + optional pattern stereotype, used as the first label line."""
    name, _, _, dark = KINDS[kind]
    s = (f"<span style='background-color:{dark};color:#FFFFFF;font-size:8px;font-weight:bold;"
         f"padding:1px 4px;border-radius:3px;'>&nbsp;{name}&nbsp;</span>")
    if stereo:
        s += f" <font color='{PAT}' style='font-size:9px'><i>&laquo;{stereo}&raquo;" + (f" {pid}" if pid else "") + "</i></font>"
    return s + "<br>"


def kstyle(kind, extra=""):
    _, fill, stroke, dark = KINDS[kind]
    base = (f"whiteSpace=wrap;html=1;fontSize=10;align=left;verticalAlign=top;spacingLeft=8;spacingRight=6;"
            f"spacingTop=3;fillColor={fill};strokeColor={stroke};strokeWidth=1.5;fontColor=#1A1A1A;")
    if kind == "store":
        return "shape=cylinder3;boundedLbl=1;backgroundOutline=1;size=9;" + base + extra
    if kind == "active":  # UML active class: double vertical borders
        return "shape=process;fixedSize=1;size=7;backgroundOutline=1;" + base + "spacingLeft=14;" + extra
    if kind == "note":
        return "shape=note;size=14;" + base + "spacingRight=14;" + extra
    if kind in ("object",):
        return "rounded=0;" + base + extra
    return "rounded=1;arcSize=5;" + base + extra


def title_style():
    return "text;html=1;align=left;verticalAlign=middle;whiteSpace=wrap;spacingLeft=4;"


# ------------------------------------------------------------------ geometry checks
def _orient(a, b, c):
    v = (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
    return (v > 1e-9) - (v < -1e-9)


def _seg_seg(a, b, c, d):
    o1, o2, o3, o4 = _orient(a, b, c), _orient(a, b, d), _orient(c, d, a), _orient(c, d, b)
    if o1 * o2 < 0 and o3 * o4 < 0:
        return "cross"
    if o1 == o2 == o3 == o4 == 0:
        ax = 0 if abs(b[0] - a[0]) >= abs(b[1] - a[1]) else 1
        lo = max(min(a[ax], b[ax]), min(c[ax], d[ax]))
        hi = min(max(a[ax], b[ax]), max(c[ax], d[ax]))
        if hi - lo > 1:
            return "overlap"
    return None


def _seg_rect(p, q, r, pad=1):
    x0, y0, x1, y1 = r[0] + pad, r[1] + pad, r[0] + r[2] - pad, r[1] + r[3] - pad
    dx, dy = q[0] - p[0], q[1] - p[1]
    t0, t1 = 0.0, 1.0
    for pk, qk in ((-dx, p[0] - x0), (dx, x1 - p[0]), (-dy, p[1] - y0), (dy, y1 - p[1])):
        if pk == 0:
            if qk < 0:
                return False
        else:
            t = qk / pk
            t0, t1 = (max(t0, t), t1) if pk < 0 else (t0, min(t1, t))
    return t0 < t1 - 1e-9


# ------------------------------------------------------------------ page model
class Page:
    def __init__(self, pid, name):
        self.pid, self.name = pid, name
        self.items = []          # ordered cells, serialised at the end
        self.V, self.cont, self.E = {}, {}, []
        self.nocheck = set()
        self.w = self.h = 0

    # ---- sizes
    def H(self, vid, default, pad=14, minimum=0):
        key = f"{self.pid}:{vid}"
        if key in MEASURED:
            return max(minimum, ceil10(MEASURED[key] + pad))
        return max(minimum, default)

    def rowH(self, spec):
        """spec: list of (vid, default) -> common height for an equal-height row."""
        return max(self.H(v, d) for v, d in spec)

    # ---- cells
    def container(self, cid, label, x, y, w, h, style, inset=(0, 30), tooltip=None):
        self.cont[cid] = [x, y, w, h, inset]
        self.items.append(("c", cid, label, style, tooltip))

    def vertex(self, vid, label, x, y, w, h, style, parent=None, tooltip=None, check=True):
        self.V[vid] = [x, y, w, h, parent]
        self.items.append(("v", vid, label, style, tooltip))
        if not check:
            self.nocheck.add(vid)

    def R(self, v):
        return self.V[v][:4]

    def top(self, v, x=None):
        X, Y, W, _ = self.R(v)
        return (X + W / 2 if x is None else x, Y)

    def bot(self, v, x=None):
        X, Y, W, Hh = self.R(v)
        return (X + W / 2 if x is None else x, Y + Hh)

    def left(self, v, y=None):
        X, Y, _, Hh = self.R(v)
        return (X, Y + Hh / 2 if y is None else y)

    def right(self, v, y=None):
        X, Y, W, Hh = self.R(v)
        return (X + W, Y + Hh / 2 if y is None else y)

    def _frac(self, vid, p):
        x, y, w, h = self.R(vid)
        fx, fy = (p[0] - x) / w, (p[1] - y) / h
        ok = (abs(fx) < 1e-6 or abs(fx - 1) < 1e-6 or abs(fy) < 1e-6 or abs(fy - 1) < 1e-6)
        assert ok and -1e-6 <= fx <= 1 + 1e-6 and -1e-6 <= fy <= 1 + 1e-6, \
            f"{self.pid}: point {p} not on border of {vid} {self.R(vid)}"
        return fx, fy

    def edge(self, eid, src, tgt, label, p_from=None, p_to=None, pts=(), color=C_DATA,
             extra="", label_x=None, straight=False, tooltip=None):
        base = ("html=1;fontSize=10;labelBackgroundColor=#FFFFFF;endArrow=block;endFill=1;"
                f"strokeWidth=1.5;strokeColor={color};fontColor={color};")
        if straight:
            style = base + "rounded=0;" + extra
            sx, sy, sw, sh = self.R(src)
            tx, ty, tw, th = self.R(tgt)
            path = [(sx + sw / 2, sy + sh / 2), (tx + tw / 2, ty + th / 2)]
        else:
            ex, ey = self._frac(src, p_from)
            nx, ny = self._frac(tgt, p_to)
            path = [p_from, *pts, p_to]
            for a, b in zip(path, path[1:]):
                assert abs(a[0] - b[0]) < 1e-6 or abs(a[1] - b[1]) < 1e-6, \
                    f"{self.pid}/{eid}: non-orthogonal {a}->{b}"
            style = (base + "edgeStyle=orthogonalEdgeStyle;rounded=1;orthogonalLoop=1;jettySize=auto;"
                     f"exitX={ex:.4f};exitY={ey:.4f};exitDx=0;exitDy=0;exitPerimeter=0;"
                     f"entryX={nx:.4f};entryY={ny:.4f};entryDx=0;entryDy=0;entryPerimeter=0;" + extra)
        self.items.append(("e", eid, label, style, tooltip, src, tgt, list(pts), label_x))
        self.E.append((eid, src, tgt, path))

    def sample_edge(self, eid, label, x, y, color, extra=""):
        self.items.append(("s", eid, label, color, extra, x, y))

    # ---- serialise
    def _cell(self, root, cid, label, style, tooltip, **attrs):
        if tooltip:
            obj = ET.SubElement(root, "UserObject", label=label, tooltip=tooltip, id=cid)
            return ET.SubElement(obj, "mxCell", style=style, **attrs)
        return ET.SubElement(root, "mxCell", id=cid, value=label, style=style, **attrs)

    @staticmethod
    def _geom(cell, **kw):
        g = ET.SubElement(cell, "mxGeometry", {k: str(round(v, 2) if isinstance(v, float) else v)
                                               for k, v in kw.items()})
        g.set("as", "geometry")
        return g

    def emit(self):
        d = ET.SubElement(mxfile, "diagram", id=self.pid, name=self.name)
        m = ET.SubElement(d, "mxGraphModel", dx="1800", dy="1100", grid="1", gridSize="10", guides="1",
                          tooltips="1", connect="1", arrows="1", fold="1", page="1", pageScale="1",
                          pageWidth=str(self.w), pageHeight=str(self.h), math="0", shadow="0",
                          background="#FFFFFF")
        root = ET.SubElement(m, "root")
        ET.SubElement(root, "mxCell", id=f"{self.pid}-0")
        layer = f"{self.pid}-1"
        ET.SubElement(root, "mxCell", id=layer, parent=f"{self.pid}-0")
        gid = lambda x: f"{self.pid}-{x}"
        for it in self.items:
            kind = it[0]
            if kind == "c":
                _, cid, label, style, tip = it
                x, y, w, h, _ = self.cont[cid]
                c = self._cell(root, gid(cid), label, style, tip, vertex="1", parent=layer)
                self._geom(c, x=x, y=y, width=w, height=h)
            elif kind == "v":
                _, vid, label, style, tip = it
                x, y, w, h, par = self.V[vid]
                px, py = (self.cont[par][0], self.cont[par][1]) if par else (0, 0)
                c = self._cell(root, gid(vid), label, style, tip, vertex="1",
                               parent=gid(par) if par else layer)
                self._geom(c, x=x - px, y=y - py, width=w, height=h)
            elif kind == "e":
                _, eid, label, style, tip, src, tgt, pts, label_x = it
                c = self._cell(root, gid(eid), label, style, tip, edge="1", parent=layer,
                               source=gid(src), target=gid(tgt))
                g = self._geom(c, relative=1) if label_x is None else self._geom(c, x=label_x, relative=1)
                if pts:
                    arr = ET.SubElement(g, "Array")
                    arr.set("as", "points")
                    for x, y in pts:
                        ET.SubElement(arr, "mxPoint", x=str(x), y=str(y))
            else:
                _, eid, label, color, extra, x, y = it
                c = ET.SubElement(root, "mxCell", id=gid(eid), value=label, edge="1", parent=layer, style=(
                    "html=1;fontSize=10;endArrow=block;endFill=1;strokeWidth=1.5;align=left;"
                    f"verticalAlign=middle;strokeColor={color};fontColor={color};labelBackgroundColor=none;{extra}"))
                g = self._geom(c, x=1, relative=1)
                for nm, px in (("sourcePoint", x), ("targetPoint", x + 70)):
                    p = ET.SubElement(g, "mxPoint", x=str(px), y=str(y))
                    p.set("as", nm)
                off = ET.SubElement(g, "mxPoint", x="8", y="0")
                off.set("as", "offset")

    def check(self):
        P = []
        boxes = {k: v[:4] for k, v in self.V.items() if k not in self.nocheck}
        for (a, ra), (b, rb) in itertools.combinations(boxes.items(), 2):
            if ra[0] < rb[0] + rb[2] and rb[0] < ra[0] + ra[2] and ra[1] < rb[1] + rb[3] and rb[1] < ra[1] + ra[3]:
                P.append(f"overlap: {a} / {b}")
        for k, (x, y, w, h, par) in self.V.items():
            if par:
                cx, cy, cw, ch, (il, it) = self.cont[par]
                if not (cx + il <= x and x + w <= cx + cw and cy + it <= y and y + h <= cy + ch):
                    P.append(f"{k} not inside {par}")
        for (a, ra), (b, rb) in itertools.combinations(self.cont.items(), 2):
            if ra[0] < rb[0] + rb[2] and rb[0] < ra[0] + ra[2] and ra[1] < rb[1] + rb[3] and rb[1] < ra[1] + ra[3]:
                P.append(f"containers overlap: {a} / {b}")
        for eid, s, t, path in self.E:
            for p, q in zip(path, path[1:]):
                for k, r in boxes.items():
                    if k not in (s, t) and _seg_rect(p, q, r):
                        P.append(f"edge {eid} crosses {k}")
        for (e1, _, _, p1), (e2, _, _, p2) in itertools.combinations(self.E, 2):
            for a, b in zip(p1, p1[1:]):
                for c, d in zip(p2, p2[1:]):
                    hit = _seg_seg(a, b, c, d)
                    if hit:
                        P.append(f"edge {hit}: {e1} x {e2}")
        return sorted(set(P))


def legend(p, x, y, w, kinds, edges=(), cid="legend"):
    """Kind-colour legend: one swatch per kind, optional edge samples underneath."""
    sw, sh, gx = 190, 44, 14
    per_row = max(1, (w - 30) // (sw + gx))
    rows = math.ceil(len(kinds) / per_row)
    eh = 34 if edges else 0
    h = 36 + rows * (sh + 10) + eh + 6
    p.container(cid, "Legend &mdash; element kinds (tag colour = kind)", x, y, w, h,
                "swimlane;startSize=26;html=1;fontSize=12;fontStyle=1;collapsible=0;fillColor=#F5F5F5;"
                "strokeColor=#999999;swimlaneFillColor=#FFFFFF;align=left;spacingLeft=10;", inset=(0, 26))
    for i, k in enumerate(kinds):
        r, c = divmod(i, per_row)
        p.vertex(f"lg_{k}", tag(k) + f"<font style='font-size:9px' color='#555555'>{KIND_DESC[k]}</font>",
                 x + 15 + c * (sw + gx), y + 36 + r * (sh + 10), sw, sh, kstyle(k, "spacingTop=2;"), cid)
    ey = y + 36 + rows * (sh + 10) + 14
    for i, (lbl, color, ex) in enumerate(edges):
        p.sample_edge(f"lge{i}", lbl, x + 15 + i * 260, ey, color, ex)
    return y + h


# =================================================================== PAGE 1
def page_architecture():
    p = Page("arch", "1 · Architecture")
    p.vertex("title",
             "<font style='font-size:22px'><b>XTNL Phase 1 &mdash; Read-Only Broker Ingestion, Pub-Sub &amp; Oversee</b>"
             "</font><br><font style='font-size:12px' color='#555555'>"
             "Broker &rarr; BrokerAdapter &rarr; Order / Position / Price / Account DTO bus &rarr; Redis &gt; PostgreSQL &gt; "
             "Image Backup, watched by <b>Oversee</b> (risk-rule plugins, soft/hard halt, email + mobile alerts). Runs "
             "24/7 on two London nodes with one fenced leader (page 5, SM9). "
             "Coloured tags = element kind (legend at bottom); <font color='" + PAT + "'><i>&laquo;pattern&raquo; Pn</i>"
             "</font> = design pattern on page 2; hover any box for detail.</font>",
             40, 16, 2580, 60, title_style())

    LX, LW = 40, 2580
    COL, CW = [120, 545, 970, 1395, 1820], 380
    X = lambda a: COL[a]
    W = lambda a, b: COL[b] + CW - COL[a]
    cx = lambda a: COL[a] + CW / 2          # 310 735 1160 1585 2010
    CC, CCW, PAD = 2280, 320, 25

    def lane(lid, label, y, h, tip):
        hd, br = LANE[lid]
        p.container(lid, label, LX, y, LW, h, (
            "swimlane;horizontal=0;startSize=40;html=1;fontSize=15;fontStyle=1;collapsible=0;"
            f"fillColor={hd};strokeColor={br};swimlaneFillColor=#FCFCFC;strokeWidth=2;fontColor=#1A1A1A;"),
            inset=(40, 0), tooltip=tip)

    def comment(cid, html, y, h, lid):
        p.vertex(cid, tag("note") + html, CC, y, CCW, h, kstyle("note"), lid)

    # ------------------------------------------------ lane 1  external
    L1y = 90
    ya = L1y + PAD
    hA = p.rowH([("stonex", 190), ("pepper", 190), ("mock", 190)])
    tagH = p.rowH([("tag_stonex", 52), ("tag_pepper", 52), ("tag_mock", 52)])
    tagY = ya + hA + 14
    guardH = max(hA + 14 + tagH, p.H("guard", 250))
    cm1H = max(guardH, p.H("cm1", 200))
    L1h = PAD + cm1H + PAD
    lane("L1", "&#9312; External Broker Layer (read-only)", L1y, L1h,
         "Everything outside our process that we ingest from. Phase 1 only reads.")
    p.vertex("stonex", tag("external", "external system") +
             "<font style='font-size:12px'><b>StoneX API</b></font> <font color='#666666'>(ciapi.cityindex.com)</font><br>"
             "REST JSON <i>/TradingAPI</i> + <i>/v2</i> &middot; Lightstreamer (lightstreamer-4, STREAMINGALL)<br>"
             "<b>Sandbox:</b> ciapipreprod.cityindextest9.co.uk<br>"
             "<b>Read-only calls:</b><br>"
             "&bull; POST /v2/Session (LogOn)<br>"
             "&bull; GET /v2/userAccount/ClientAndTradingAccount<br>"
             "&bull; GetClientAccountMargin v2 (account image)<br>"
             "&bull; GET openpositions &middot; tradehistory<br>"
             "&bull; POST activeorders <i>(read-only POST)</i><br>"
             "<b>Streams:</b> PRICES &middot; ORDERS (orders <i>and</i> open trades) &middot; TRADEMARGIN (margin "
             "only) &middot; CLIENTACCOUNTMARGIN",
             X(0), ya, CW, hA, kstyle("external"), "L1",
             tooltip="StoneX (GAIN Capital / City Index) REST + Lightstreamer API, from docs.labs.gaincapital.com.\n"
                     "ClientAccountMargin (stream + REST) feeds the account image used by Oversee rules.\n"
                     "activeorders is a POST that only reads, so the lockout denies by path, not verb.")
    p.vertex("pepper", tag("external", "external system") +
             "<font style='font-size:12px'><b>Pepperstone &mdash; cTrader Open API</b></font><br>"
             "Spotware ProtoOA over TLS (demo.ctraderapi.com / live.ctraderapi.com :5035, confirm)<br>"
             "OAuth access + refresh token; read-only use requests the <i>accounts</i> scope<br>"
             "<b>Read-only use:</b> reconcile (orders + positions), execution events, spots, trader / balanceVersion, "
             "deposit / withdraw events, symbol schedules<br>"
             "<b>Heartbeat:</b> ProtoHeartbeatEvent every 10 s; honour rate limits",
             X(1), ya, CW, hA, kstyle("external"), "L1",
             tooltip="The older REST / WebSocket endpoints in broker-endpoints.md predate this design; the adapter "
                     "targets the cTrader Open API. Hostnames and limits to be confirmed with Spotware / Pepperstone.")
    p.vertex("mock", tag("external", "test double") +
             "<font style='font-size:12px'><b>Mock Broker</b></font><br>"
             "Deterministic fixtures (114-trade baseline) incl. account snapshots<br>"
             "BROKER_ADAPTER_TYPE=mock &middot; no network I/O<br>"
             "Drives <i>verify_execution.py</i> and rule tests (e.g. 1.2 % risk &rarr; FLAG, 2 % &rarr; SOFT_HALT)",
             X(2), ya, CW, hA, kstyle("external"), "L1",
             tooltip="Fixtures can include rule-breaking orders so every Oversee rule has a deterministic test.")
    lt = kstyle("guard", "align=center;verticalAlign=middle;spacingTop=0;dashed=1;")
    p.vertex("tag_stonex", "&#9940; <b>EXECUTION LOCKOUT</b><br>newtradeorder &middot; newstoplimitorder &middot; "
             "updatetradeorder &middot; updatestoplimitorder &middot; cancel &rarr; BLOCKED", X(0), tagY, CW, tagH, lt, "L1",
             tooltip="StoneX order-changing endpoints, denied by ReadOnlyTransport (P2).")
    p.vertex("tag_pepper", "&#9940; <b>EXECUTION LOCKOUT</b><br>place_order &middot; modify_order &middot; "
             "cancel_order &rarr; BLOCKED<br>ProtoOA allow-list: read / subscribe only", X(1), tagY, CW, tagH, lt, "L1")
    p.vertex("tag_mock", "&#9940; <b>EXECUTION LOCKOUT</b><br>place_order &middot; modify_order &middot; "
             "cancel_order &rarr; BLOCKED", X(2), tagY, CW, tagH, lt, "L1")
    p.vertex("guard", tag("guard", "Protection Proxy", "P2") +
             "<font style='font-size:13px' color='#B71C1C'><b>&#9940; EXECUTION LOCKOUT GUARD</b></font><br>"
             "Live outbound execution is mechanically blocked in Phase 1 by independent layers:<br>"
             "<b>1. PreToolUse hook</b> &mdash; .claude/hooks/pre_tool_guard.py denies place_order / modify_order / cancel_order<br>"
             "<b>2. Interface lock</b> &mdash; BrokerAdapter defines no mutation methods<br>"
             "<b>3. Environment gate</b> &mdash; BROKER_ENV must be <i>sandbox</i> or <i>live_readonly</i> (read-only "
             "credentials / OAuth scope where the broker offers one); anything else is refused at start-up<br>"
             "<b>4. ReadOnlyTransport</b> &mdash; StoneX REST: path deny-list /newtradeorder, /newstoplimitorder, "
             "/updatetradeorder, /updatestoplimitorder, /cancel (by <b>path</b>, not verb); cTrader ProtoOA: "
             "<b>payloadType allow-list</b> (the one list in adapter-isolation.md &sect;4: auth, token refresh, reconcile, "
             "orders, deals, cash flows, symbols, schedule, spots, heartbeat), everything else refused; live_readonly also requires the OAuth scope 'accounts'<br>"
             "<b>5. ActionGate</b> (lane &#9316;) &mdash; when Phase 2 adds sending, every send must also pass the "
             "Oversee halt state<br>"
             "<b>6. Single writer</b> &mdash; only the leader node (etcd lease + PostgreSQL epoch) opens broker "
             "sessions; a leader that loses its lease is hard-killed",
             X(3), ya, W(3, 4), guardH, kstyle("guard", "strokeWidth=3;"), "L1",
             tooltip="Defence in depth. Layers 1-4 make sending impossible in Phase 1; layer 5 is the risk gate "
                     "Phase 2 must keep.")
    comment("cm1", "<b>What this layer is</b><br>The only sources of truth outside our process. Phase 1 <b>reads</b>; "
            "it never writes.<br><br><b>New: account image</b><br>Each broker also provides balance and margin "
            "(equity where available, used only as a cross-check). Rules use the computed equity_mark, e.g. 1R = "
            "1.0 % of equity_mark per trade."
            "<br><br><b>Watch out</b><br>StoneX <i>activeorders</i> is a read-only POST; deny order endpoints by path.",
            ya, cm1H, "L1")

    # ------------------------------------------------ lane 2  adapters
    L2y = L1y + L1h + 20
    yb = L2y + PAD
    hB = p.rowH([("a_stonex", 140), ("a_pepper", 140), ("a_mock", 140), ("dto", 140)])
    yc = yb + hB + 60
    hC = p.H("abc", 110)
    cm2H = max(yc + hC - yb, p.H("cm2", 200))
    L2h = PAD + cm2H + PAD
    lane("L2", "&#9313; Pluggable Adapter Layer", L2y, L2h, "Raw broker payloads in, canonical DTOs out.")
    p.vertex("a_stonex", tag("class", "Adapter &middot; Strategy", "P1") +
             "<font style='font-size:12px'><b>StoneXAdapter</b></font> <i>(new)</i><br>"
             "&bull; LogOn v2 session token; re-auth on expiry<br>"
             "&bull; PRICES &rarr; PriceDTO<br>"
             "&bull; ORDERS &rarr; OrderDTO; the first item with filled units &gt; 0 also emits a <b>PositionDTO</b> "
             "(position_id = OrderId, entry price from GET order/{id}); later items update the OrderDTO "
             "(remaining_units, status) <b>and</b> the PositionDTO (units) until the order is terminal, then the "
             "PositionDTO only; closing trades and filled reduce-only children reduce or close the linked position "
             "instead of opening one. Positions use the item's own timestamp, else the local clock domain<br>"
             "&bull; if-done stop / limit children &rarr; separate reduce-only OrderDTOs (linked_position_id)<br>"
             "&bull; TRADEMARGIN + CLIENTACCOUNTMARGIN &rarr; AccountDTO only<br>"
             "&bull; contracts / stake-per-point &rarr; units (own instrument map)",
             X(0), yb, CW, hB, kstyle("class"), "L2",
             tooltip="Not in the repo yet. TRADEMARGIN has no stop, entry or status, so it never builds a position. "
                     "Stops are never folded into the position inside the adapter: Oversee derives the effective stop "
                     "from the linked reduce-only children.")
    p.vertex("a_pepper", tag("class", "Adapter &middot; Strategy", "P1") +
             "<font style='font-size:12px'><b>PepperstoneAdapter</b></font><br>"
             "&bull; cTrader Open API: ProtoOA (protobuf) over TLS; app + account auth (OAuth token pair); "
             "ProtoHeartbeatEvent every 10 s, no inbound 30 s &rarr; reconnect (values to confirm)<br>"
             "&bull; ORDER_STATUS_ACCEPTED &rarr; WORKING (ORDER_PARTIAL_FILL &rarr; PARTIALLY_FILLED) &middot; "
             "ORDER_STATUS_FILLED &rarr; FILLED<br>"
             "&bull; execution events &rarr; OrderDTO + <b>PositionDTO</b> (native stopLoss / takeProfit)<br>"
             "&bull; closingOrder or SL/TP order type &rarr; is_reduce_only<br>"
             "&bull; trader / balanceVersion &rarr; AccountDTO; DepositWithdraw &rarr; <b>CashFlowDTO</b><br>"
             "&bull; AccountDisconnect / TokenInvalidated / ClientDisconnect &rarr; re-authenticate<br>"
             "&bull; volume / 100 &rarr; units (CENTI_UNITS, to confirm; own instrument map)",
             X(1), yb, CW, hB, kstyle("class"), "L2")
    p.vertex("a_mock", tag("class", "Adapter &middot; Strategy", "P1") +
             "<font style='font-size:12px'><b>MockBrokerAdapter</b></font><br>"
             "&bull; Fixtures with deterministic broker_seq / broker timestamps (seq_id still stamped by the orchestrator)<br>"
             "&bull; Same mapping as live adapters<br>"
             "&bull; Emits account, position and cash-flow fixtures too",
             X(2), yb, CW, hB, kstyle("class"), "L2")
    p.vertex("dto", tag("value", "Canonical Data Model &middot; Value Object", "P3") +
             "<font style='font-size:12px'><b>OrderDTO / PositionDTO / PriceDTO / AccountDTO / CashFlowDTO / "
             "InstrumentSpec</b></font> (src/dto &middot; frozen=True)<br>"
             "<b>Version on every stateful DTO:</b> broker_updated_at_ms, broker_seq?, source STREAM | REST, seq_id "
             "&rarr; key = (broker_seq, broker_ts, observed_seq: the later observation wins a tie); "
             "same checksum = no-op; seq_id breaks a tie only "
             "when the content differs. broker_seq and broker time are each declared present or absent per (adapter, "
             "entity) type; without broker time every row of that type uses local time; an untimed REST row in the "
             "broker domain gets the stream watermark, never the local send time<br>"
             "<b>OrderDTO:</b> client_order_id, broker_order_id, account_id, adapter_id, symbol, side, status, entry_price, "
             "stop_loss?, take_profit?, <b>ordered_units?, remaining_units?, is_reduce_only, linked_position_id?</b>, "
             "instrument_unknown, raw_qty?, version, checksum "
             "&middot; <i>no risk field: Oversee owns risk</i><br>"
             "<b>PositionDTO</b> <i>(new)</i>: position_id, account_id, adapter_id, symbol, side, units?, instrument_unknown, "
             "raw_qty?, entry_price, "
             "stop_loss?, take_profit?, status OPEN | CLOSED, opened_by_client_order_id?, accrued_swap, commission, "
             "version, checksum &middot; <i>no unrealised P&amp;L: marked from its adapter's prices</i><br>"
             "<b>AccountDTO</b> (versioned): account_id, adapter_id, currency, balance, realised_pnl_today, version, "
             "checksum &middot; live fields equity?, margin_used, free_margin, unrealised_pnl &rarr; unversioned, "
             "coalesced <i>account_live:{id}</i> &middot; <b>CashFlowDTO</b> <i>(new)</i>: transaction_id, account_id, "
             "kind DEPOSIT | WITHDRAWAL | TRADE_PNL | FINANCING | COMMISSION | ADJUSTMENT, amount, currency, broker_ts, adapter_id, "
             "seq_id, checksum<br>"
             "<b>PriceDTO:</b> adapter_id, symbol, bid, ask, spread_pips, timestamp_ms, seq_id &middot; <b>InstrumentSpec:</b> "
             "adapter_id, symbol, base_ccy, quote_ccy, pnl_ccy, contract_size, unit_kind, point_size, min_qty, swap_booked_to_balance<br>"
             "<b>seq_id</b> = orchestrator HLC max(epoch_ms &lt;&lt; 20, last + 1), seeded from PostgreSQL: event-log "
             "order and last tie-breaker only",
             X(3), yb, W(3, 4), hB, kstyle("value"), "L2",
             tooltip="Why a version and not seq_id alone: seq_id is stamped on arrival, so a slow REST read could look "
                     "newer than a later stream update. Comparing the broker's own time (or revision) first stops a stale "
                     "snapshot overwriting a newer stop-loss. A broker_ts more than 5 s ahead of local receipt is "
                     "rejected as future_timestamp. ordered_units is the whole trade (per-order 1R check); "
                     "remaining_units is what is still unfilled (aggregate risk), so a partial fill is never counted "
                     "twice. is_reduce_only is set only from explicit broker evidence. An unknown symbol triggers "
                     "fetch_instruments(); if it is still unknown the entity is committed with instrument_unknown = true "
                     "and its raw broker quantity, so the RiskSweep sees it (UNKNOWN &rarr; capped SOFT_HALT) instead of it "
                     "looping through the DLQ.")
    p.vertex("abc", tag("abstract", "Adapter target &middot; Strategy &middot; built by AdapterFactory", "P1") +
             "<font style='font-size:13px'><b>BrokerAdapter</b></font> (src/adapters) &mdash; read-only interface lock<br>"
             "fetch_open_orders() &middot; fetch_order_status(broker_order_id) &middot; <b>fetch_positions() &rarr; "
             "list[PositionDTO]</b> &middot; <b>fetch_position_status(position_id)</b> &middot; "
             "fetch_account_snapshot() &middot; <b>fetch_cash_flows(since_ms)</b> &middot; fetch_instruments() &middot; "
             "stream_ticks(symbols) &middot; ensure_ticks(symbols) &middot; stream_order_updates() &middot; <b>stream_positions()</b> &middot; "
             "stream_account()<br>"
             "<b>Instrument map:</b> fetch_instruments() fills a private read-only <i>self._instruments: dict[str, "
             "InstrumentSpec]</i> in SNAPSHOTTING, before any stream starts; to_notional() reads only that map<br>"
             "<b>Forbidden imports:</b> src.db, src.cache, src.pubsub, src.integrity, src.oversee &middot; never "
             "publishes or writes &middot; "
             "transport = ReadOnlyTransport (P2) &middot; <font color='#B71C1C'><b>no mutation methods</b></font>",
             X(0), yc, W(0, 4), hC, kstyle("abstract", "strokeWidth=2;"), "L2",
             tooltip="All read-only. fetch_position_status() lets resync and DLQ recovery re-read one position by id "
                     "(StoneX GET order/{id}; cTrader deal list by position id).")
    comment("cm2", "<b>Contract</b><br>Adapters are pure translators: no db / cache / pubsub / integrity / oversee imports, no publishing, "
            "no writes.<br><br><b>New methods</b><br>fetch_account_snapshot() and stream_account() turn broker margin "
            "data into AccountDTO, so the server always holds a fresh account image. fetch_instruments() supplies "
            "InstrumentSpec, used to normalise every quantity to units.<br><br><b>Positions</b><br>Once an entry "
            "fills, the open position is its own entity (PositionDTO), so a stop moved or removed after the fill is "
            "still seen.<br><br><b>No risk on the DTO</b><br>"
            "Adapters can't see equity, so they never compute risk.<br><br><b>Extending</b><br>"
            "A new broker is one class registered in AdapterFactory.", yb, cm2H, "L2")

    # ------------------------------------------------ lane 3  bus
    L3y = L2y + L2h + 20
    yo = L3y + PAD
    hO = p.rowH([("orch", 90), ("spool", 90)])
    yq = yo + hO + 80
    QX, QW = [120 + i * 312 for i in range(4)], 294       # four topic columns inside the EventBus group
    qcx = lambda i: QX[i] + QW / 2
    hQ = p.rowH([("q_price", 100), ("q_account", 100), ("q_position", 100), ("q_order", 100), ("dlq", 100),
                 ("recovery", 100)])
    gy0, gy1 = yq - 28, yq + hQ + 12           # EventBus group box
    ys = gy1 + 50
    hS = p.rowH([("sub_price", 80), ("acct_proj", 80), ("pos_cons", 80), ("sub_order", 80), ("bp_note", 80),
                 ("quarantine", 80)])
    cm3H = max(ys + hS - yo, p.H("cm3", 200))
    L3h = PAD + cm3H + PAD
    lane("L3", "&#9314; Non-Blocking Pub-Sub Bus", L3y, L3h,
         "Four isolated bounded queues plus the PostgreSQL-backed dead-letter path.")
    p.vertex("orch", tag("active", "Publisher", "P4") +
             "<font style='font-size:12px'><b>Ingestion Orchestrator</b></font> (sole publisher)<br>"
             "<b>Runs only on the leader node.</b> Consumes adapter async iterators &rarr; validates schema + checksum "
             "&rarr; <b>stamps seq_id</b> (HLC, re-seeded after each epoch bump, "
             "from PostgreSQL) and labels the version (clock domain, stream watermark for untimed REST rows; broker_ts "
             "&gt; receipt + 5 s &rarr; future_timestamp) &rarr; routes by DTO type &middot; put_nowait() only &middot; "
             "never awaits subscribers or the database<br>"
             "QueueFull / invalid &rarr; DlqSpooler.offer() (memory append, no I/O) &middot; <b>never stops reading a "
             "socket</b>: queue + ring both full &rarr; adapter flag backpressure_overflow drops price frames at the "
             "parser and marks a gap, and DELs that stream's health key until resync reconciles; ring &lt; 50 % &rarr; "
             "SM4 RESYNCING<br>"
             "Per-stream health keys conn:{adapter_id}:{prices | orders (incl. positions) | account} (PEXPIRE 30 s), "
             "refreshed on transport "
             "heartbeats <b>only while that subscription is SUBSCRIBED</b> (ack seen, no UNSUBSCRIBED / ERROR frame, socket "
             "still open; cTrader: from ProtoOAAccountAuthRes until AccountDisconnect / TokenInvalidated / "
             "ClientDisconnect); a lost socket resets every subscription on it, DELs its keys and sets stream_health.conn_lost_at "
             "(PostgreSQL; cleared once SUBSCRIBED and reconciled), which Oversee's fence reads; "
             "when flat, a REST fetch_account_snapshot() every 60 s proves the account path end to end &middot; "
             "subscribes stream_ticks() to traded symbols <b>+ the FX crosses</b> for every pnl_ccy &rarr; account ccy; "
             "ensure_ticks() adds a symbol or cross first needed mid-session (page if no clean mid within 30 s)",
             X(0), yo, W(0, 2), hO, kstyle("active"), "L3",
             tooltip="Only component allowed to publish; DLQ recovery re-submits through it too. One seq_id source "
                     "means a restart or reconnect can never reset the counter. Keeping the socket drained means "
                     "heartbeat and control frames are never stuck behind unread ticks. One TTL key per "
                     "subscription means a live price stream can't hide a dead account stream, and Redis expiry "
                     "replaces wall-clock comparisons. FX crosses are derived in SNAPSHOTTING from InstrumentSpec.pnl_ccy "
                     "and AccountDTO.currency, e.g. AUDUSD for EURUSD traded on an AUD account.")
    p.vertex("spool", tag("active", "Spooler &middot; Dead Letter Channel", "P5") +
             "<b>DlqSpooler</b> <i>(new)</i><br>offer() = in-memory append only (no I/O, no fsync)<br>"
             "flush() task: batch INSERT events_dlq; PostgreSQL down &rarr; spill file via asyncio.to_thread "
             "(fsync off the loop); drained into events_dlq (idempotent on a payload hash) as soon as PostgreSQL accepts "
             "writes again, and at start-up; any node drains its own file (append-only, outside the epoch check)<br>"
             "ring full &rarr; the adapter keeps reading but drops price frames (backpressure_overflow); order / position "
             "/ account gap recovered by SM4 RESYNCING",
             X(3), yo, CW, hO, kstyle("active"), "L3",
             tooltip="Keeps the event loop free of database and disk I/O: offer() is a list append. Only the "
                     "flush task touches PostgreSQL or the disk, and the disk write runs in a worker thread. The "
                     "socket is never left unread: pausing it would fill SO_RCVBUF, delay heartbeat frames and drop the "
                     "connection.")
    p.vertex("bus", "<b>EventBus</b> &middot; P4", 105, gy0, 1260, gy1 - gy0,
             "rounded=1;arcSize=3;html=1;fillColor=none;strokeColor=#9673A6;dashed=1;strokeWidth=1.5;align=left;"
             "verticalAlign=top;spacingLeft=8;spacingTop=4;fontSize=11;fontColor=#6A3D8F;", "L3",
             tooltip="All topics. Oversee does NOT subscribe here: it reads the post-commit Redis Stream oversee:feed, "
                     "so it can never see an event before that event's state is committed.", check=False)
    p.vertex("q_price", tag("object", "Topic &middot; Bulkhead", "P4") +
             "<b><u>market.price.dto : Topic[PriceDTO]</u></b><br>asyncio.Queue(maxsize=N)",
             QX[0], yq, QW, hQ, kstyle("object"), "L3")
    p.vertex("q_account", tag("object", "Topic &middot; Bulkhead", "P4") +
             "<b><u>account.state.dto : Topic[AccountDTO | CashFlowDTO]</u></b><br>asyncio.Queue(maxsize=N) &middot; "
             "overflow &rarr; DLQ",
             QX[1], yq, QW, hQ, kstyle("object"), "L3", tooltip="Account image updates; never mixed with orders or prices.")
    p.vertex("q_position", tag("object", "Topic &middot; Bulkhead", "P4") +
             "<b><u>position.state.dto : Topic[PositionDTO]</u></b> <i>(new)</i><br>asyncio.Queue(maxsize=N) &middot; "
             "overflow &rarr; DLQ",
             QX[2], yq, QW, hQ, kstyle("object"), "L3",
             tooltip="Open positions (after a fill) and their stop-loss / take-profit, so Oversee keeps auditing "
                     "risk after the entry order is FILLED.")
    p.vertex("q_order", tag("object", "Topic &middot; Bulkhead", "P4") +
             "<b><u>orders.internal.dto : Topic[OrderDTO]</u></b><br>asyncio.Queue(maxsize=N) &middot; overflow &rarr; DLQ",
             QX[3], yq, QW, hQ, kstyle("object"), "L3")
    p.vertex("dlq", tag("store", "Dead Letter Channel", "P5") +
             "<b>events_dlq</b> (PostgreSQL)<br>order, position, account and cash-flow events parked when unacked, timed out, "
             "invalid, QueueFull or future_timestamp<br>event_type ORDER | POSITION | ACCOUNT | CASH_FLOW &middot; "
             "origin_dlq_id (lineage) &middot; retry_count &middot; "
             "status &middot; next_retry_at_ms &middot; <i>stale versions are not parked: dropped and logged</i>",
             X(3), yq, CW, hQ, kstyle("store"), "L3", tooltip="Lifecycle SM2 on page 3. Price ticks are never parked: "
                                                            "only the latest tick per symbol matters.")
    p.vertex("recovery", tag("active", "Idempotent Retry", "P5") +
             "<b>DLQ Recovery Worker</b><br><b>start-up barrier:</b> leases rows only for adapters in SM4 STREAMING "
             "(authenticated, instruments loaded); broker errors while not STREAMING don't count as retries<br>"
             "&#9312; ask the broker first:<br>"
             "&nbsp;&nbsp;&nbsp;ORDER &rarr; fetch_order_status(broker_order_id)<br>"
             "&nbsp;&nbsp;&nbsp;POSITION &rarr; fetch_position_status(position_id)<br>"
             "&nbsp;&nbsp;&nbsp;ACCOUNT &rarr; fetch_account_snapshot()<br>"
             "&nbsp;&nbsp;&nbsp;CASH_FLOW &rarr; fetch_cash_flows(): converged only if its transaction_id is found<br>"
             "&#9313; re-submit the broker's fresh row through the orchestrator with its lineage (origin_dlq_id); the "
             "parked payload is dropped<br>"
             "&#9314; 5 failures (counted across re-parks of the same entity and error) or illegal transition &rarr; "
             "quarantine",
             X(4), yq, CW, hQ, kstyle("active"), "L3")
    p.vertex("sub_price", tag("class", "Subscriber", "P4") + "<b>PriceSubscriber</b><br>latest PriceDTO per (adapter, symbol) "
             "&rarr; Redis price:{adapter_id}:{symbol} and, for a tick within max_clean_spread, clean_mid:{adapter_id}:{symbol}, "
             "via a light script (no digest, no boot check, no feed entry); the RiskSweep reads both every second",
             QX[0], ys, QW, hS, kstyle("class"), "L3")
    p.vertex("acct_proj", tag("class", "Subscriber &middot; Projector", "P4") +
             "<b>AccountImageProjector</b><br>&#9312; commit account_state (current row; sampled history in "
             "account_snapshots) / cash_flows (dedupe on transaction_id)<br>"
             "&#9313; then apply_state.lua &rarr; Redis account:{id} + oversee:feed; a cash-flow commit XADDs its own ACCOUNT "
             "entry (its seq_id)<br>"
             "&#9314; live margin fields &rarr; account_live:{id} (coalesced, light script; no feed entry)",
             QX[1], ys, QW, hS, kstyle("class"), "L3",
             tooltip="Keeps the server-side account image that Oversee rules read. Redis is written only after the "
                     "PostgreSQL commit, by the same Lua script that appends the oversee:feed entry.")
    p.vertex("pos_cons", tag("class", "Subscriber &middot; Unit of Work", "P4 P6") +
             "<b>PositionStateConsumer</b> <i>(new)</i><br>&#9312; one txn: positions_state + outbox + event log<br>"
             "&#9313; then apply_state.lua &rarr; Redis position:{id} + oversee:feed",
             QX[2], ys, QW, hS, kstyle("class"), "L3")
    p.vertex("sub_order", tag("class", "Subscriber &middot; Unit of Work", "P4 P6") +
             "<b>OrderStateConsumer</b><br>&#9312; one txn: orders_state + outbox + event log<br>"
             "&#9313; then apply_state.lua &rarr; Redis order:{id} + oversee:feed",
             QX[3], ys, QW, hS, kstyle("class"), "L3",
             tooltip="Keeps draining during a Redis heal: there is no symbol pause anywhere in the pipeline.")
    p.vertex("bp_note", tag("note") + "<b>Backpressure</b><br>Order / position / account QueueFull &rarr; DlqSpooler "
             "(never dropped, never awaited); unacked &rarr; DLQ<br>Price QueueFull &rarr; keep latest tick per symbol "
             "<i>(proposed)</i>",
             X(3), ys, CW, hS, kstyle("note"), "L3")
    p.vertex("quarantine", tag("store", "Poison Message", "P5") + "<b>events_quarantine</b> (PostgreSQL)<br>after 5 retries; manual review",
             X(4), ys, CW, hS, kstyle("store"), "L3")
    comment("cm3", "<b>Why four queues</b><br>One bounded queue per entity (Bulkhead): a price burst can't starve "
            "orders, positions or the account image. Cash flows travel with the account.<br><br><b>Never block</b><br>put_nowait() only; slow subscribers can't stall "
            "ingestion, and an overflow is handed to the DlqSpooler in memory, so a PostgreSQL write never sits on "
            "the hot path.<br><br><b>Failure path</b><br>Undeliverable order, position and account events are parked "
            "in events_dlq; recovery asks the broker first (order status, position by id or account snapshot) and "
            "re-submits the broker's fresh row through the orchestrator (still the sole publisher).<br><br><b>Oversee is not on the bus</b><br>It reads the post-commit feed in "
            "Redis, so it never sees an event before that event's state is committed.", yo, cm3H, "L3")

    # ------------------------------------------------ lane 4  state
    L4y = L3y + L3h + 20
    yt = L4y + 50
    hT = p.rowH([("tier1", 130), ("tier2", 130), ("tier3", 130)])
    yw = yt + hT + 90
    hW = p.H("worker", 120)
    rc = yw + hW + 30
    cm4H = max(rc - yt, p.H("cm4", 200))
    L4h = (yt - L4y) + cm4H + PAD
    lane("L4", "&#9315; Tiered State: Redis &gt; PostgreSQL &gt; Image", L4y, L4h,
         "Redis is a deliberate, disposable copy; PostgreSQL is the source of truth; the image is the disaster copy.")
    T1X, T1W, T2X, T2W, T3X, T3W = 120, 520, 840, 660, 1700, 500
    p.vertex("tier1", tag("store", "Cache-aside + Write-through &middot; intentional duplicate", "P7") +
             "<font style='font-size:12px'><b>TIER 1 &mdash; Redis (hot cache)</b></font><br>"
             "Deliberate copy of PostgreSQL for sub-millisecond reads (dashboards, hot-path lookups); the RiskSweep reads "
             "prices and live fields here and entity state from PostgreSQL<br>"
             "<b>Keys:</b> order:{client_order_id} &middot; <b>position:{position_id}</b> &middot; price:{adapter_id}:{symbol} &middot; "
             "account:{account_id} &middot; instrument:{adapter_id}:{symbol} &middot; conn:{adapter_id}:{stream} (TTL) &middot; "
             "state:digest &middot; <b>halt:state</b> = (effective_level, halt_event_id) &middot; <b>redis:boot_id</b> "
             "&middot; <b>oversee:feed</b> (Stream); no timer keys<br>"
             "<b>Every entity write = apply_state.lua</b> (atomic): SET only if the incoming <b>version</b> &gt; stored "
             "(fixed-width strings, never tonumber()); same checksum = no-op that only advances the version; XADD "
             "oversee:feed only if the SET applied; XOR state:digest; boot_id missing &rarr; NO_BOOT (not applied; SM8 re-applies it before boot_id) unless the caller holds "
             "redis:boot_lock<br>FLUSHDB (own database) at every LEADER_INIT; boot_id = run_id:epoch<br>"
             "price:{adapter_id}:{symbol}, clean_mid:{adapter_id}:{symbol} (only clean ticks), account_live:{id} and conn: "
             "use a separate light script (no digest, no boot check, no feed entries)<br>"
             "Feed bounded: trimmed to the oldest un-acked entry, hard cap MAXLEN ~ 1 M &middot; "
             "maxmemory-policy noeviction<br>Never the source of truth; healed in place from Tier 2; rebuilt by SM8 "
             "after a restart (ActionGate fails closed meanwhile)",
             T1X, yt, T1W, hT, kstyle("store", "strokeWidth=2.5;"), "L4",
             tooltip="Intentional duplication: data is written to PostgreSQL first and copied to Redis after commit. "
                     "Because the copy and the feed entry are one Lua call, Oversee can never read a feed entry whose "
                     "state is not already in Redis. The version compare stops a late writer, a stale REST read or the "
                     "healer from "
                     "overwriting newer state. noeviction means a full Redis rejects writes loudly instead of "
                     "silently evicting order:/account: keys; a memory alert fires well before that. "
                     "Losing Redis loses nothing durable (Oversee keeps no timers): halt:state and "
                     "the feed are rebuilt from PostgreSQL "
                     "by SM8, and ActionGate denies everything until then.")
    p.vertex("tier2", tag("store", "CoR handler &middot; Outbox &middot; Event Log", "P7 P6") +
             "<font style='font-size:12px'><b>TIER 2 &mdash; PostgreSQL</b></font> (single source of truth)<br>"
             "114-trade baseline &middot; orders_state &middot; <b>positions_state</b> &middot; outbox_events &middot; "
             "event log &middot; <b>account_state</b> + account_snapshots (history) &middot; <b>cash_flows</b> &middot; <b>sod_marks</b> &middot; "
             "instruments &middot; <b>halt_breaches</b> (rule states) &middot; oversee_events &middot; halt_events &middot; notification_outbox + dedupe &middot; events_dlq / quarantine "
             "&middot; <b>engine_epoch, leader_terms, cluster_state, last_clean_marks</b>, adapter_auth_state, outbound_intent (Phase 2) &middot; Patroni "
             "primary + sync standby",
             T2X, yt, T2W, hT, kstyle("store", "strokeWidth=2.5;"), "L4")
    p.vertex("tier3", tag("store", "Memento", "P7") +
             "<font style='font-size:12px'><b>TIER 3 &mdash; Immutable Image Backup</b></font><br>"
             "Snapshot + WAL archive from both Patroni members &middot; for region DR and operator-run restores only; "
             "never restored automatically",
             T3X, yt, T3W, hT, kstyle("store"), "L4")
    p.vertex("worker", tag("active", "State &middot; Supervisor", "P8") +
             "<font style='font-size:12px'><b>Integrity Reconciliation Worker</b></font> (SM3, page 3)<br>"
             "Every INTEGRITY_CHECK_INTERVAL_SEC (5.0 s)<br>"
             "<b>Step 1</b> Redis vs DB: rows committed in [last &minus; MAX_TXN_MS, now &minus; 500 ms] &rarr; version "
             "+ checksum vs Redis + rotating sweep of K keys; full XOR digest only when quiet; 2 ticks &rarr; Redis "
             "behind: heal in place; Redis <b>ahead</b>: alert, DEL, re-apply; halt:state id behind &rarr; re-apply, "
             "ahead &rarr; alert, DEL, re-apply; boot_id &ne; run_id:epoch &rarr; SM8<br>"
             "<b>Step 2</b> PostgreSQL checksums + seq_id monotonicity (timeline / epoch aware) &rarr; halt + page; "
             "primary-only damage &rarr; Patroni switchover to the checked standby, else operator restore; then "
             "reconcile with the broker "
             "(prices resume first), then FLUSHDB &rarr; SM8; resume after READY<br>"
             "<b>Owns SM8</b> (single-flight, redis:boot_lock)",
             T2X, yw, T2W, hW, kstyle("active"), "L4")
    comment("cm4", "<b>Why duplicate into Redis?</b><br>Dashboards and hot-path lookups need the account image, positions and "
            "latest prices instantly, without loading PostgreSQL on every event.<br><br><b>Safe duplication</b><br>"
            "Write PostgreSQL first, copy to Redis after commit. If they disagree, PostgreSQL wins and Step 1 "
            "heals Redis in place.<br><br><b>Why no pause</b><br>Pausing a symbol while the orchestrator keeps "
            "calling put_nowait() would fill the bounded queue and push valid events into the DLQ. The version "
            "compare in apply_state.lua lets the healer and live writers run at the same time: the newer version "
            "always wins.<br><br><b>If Redis restarts</b><br>Risk state is not lost: ActionGate denies everything "
            "until SM8 writes redis:boot_id, and SM8 rebuilds halt:state and the feed from PostgreSQL. The RiskSweep "
            "never depended on Redis entity keys, so oversight continues meanwhile."
            "<br><br><b>Leader change</b><br>The new leader flushes its Redis and rebuilds it (SM8); nothing in a "
            "Redis from an earlier term is trusted.<br><br><b>Disaster copy</b><br>Tier 3 is only read during an "
            "operator-run restore.", yt, cm4H, "L4")

    # ------------------------------------------------ lane 5  oversee
    L5y = L4y + L4h + 20
    y5 = L5y + PAD
    hI = p.H("rule_if", 70)
    hR = p.rowH([("r_size", 110), ("r_agg", 110), ("r_sl", 110), ("r_loss", 110), ("r_margin", 110)])
    gH = 30 + hI + 55 + hR + 15
    ovH = max(gH, p.H("oversee", 200))
    gH = ovH
    yB = y5 + ovH + 70
    chAB = y5 + ovH + 35
    hB5 = p.rowH([("halt", 120), ("gate", 120), ("execgw", 120), ("disp", 120)])
    yC = yB + hB5 + 50
    hC5 = p.rowH([("ch_email", 70), ("ch_push", 70)])
    yD = yC + hC5 + 50
    hD5 = p.rowH([("x_smtp", 60), ("x_push", 60)])
    notesH = max(yD + hD5 - yC, p.H("verdicts", 150), p.H("rule_cfg", 150))
    yE = yC + notesH + 40
    hE = p.H("ov_rt", 160)
    yF = yE + hE + 30
    hF = p.H("risk_defs", 120)
    cm5H = max(yF + hF - y5, p.H("cm5", 200))
    L5h = PAD + cm5H + PAD
    lane("L5", "&#9316; Oversee &mdash; Risk Rules, Halt &amp; Alerts", L5y, L5h,
         "Queue observer that evaluates pluggable risk rules against the account image.")
    p.vertex("oversee", tag("active", "Observer &middot; Level-triggered sweep", "P9") +
             "<font style='font-size:13px'><b>Oversee</b></font> (observer of <b>committed</b> state)<br>"
             "&bull; <b>feed observer</b>: reads Redis Stream <b>oversee:feed</b> (XREADGROUP), <b>not</b> the in-memory "
             "bus, and wakes the RiskSweep for that account at once; it decides nothing itself<br>"
             "&bull; <b>RiskSweep</b> (every 1 s and on wake-up), per account: a READ ONLY REPEATABLE READ snapshot from "
             "PostgreSQL + Redis price: / clean_mid: / account_live: &rarr; every Rule plugin on every live entity; "
             "<b>most severe level wins</b><br>"
             "&bull; writes, in a separate epoch-checked transaction: rule states (halt_breaches), oversee_events, the "
             "halt level, and the day's sod_marks row at the 17:00 roll<br>"
             "&bull; deadlines are durable timestamps: <b>no timers</b> to lose, push forward or race<br>"
             "&bull; asks the orchestrator (never an adapter) for a cash-flow refresh on an unexplained balance drop<br>"
             "&bull; rules share one set of pure risk functions (note at the bottom of this lane)",
             X(0), y5, W(0, 1), ovH, kstyle("active", "strokeWidth=2;"), "L5",
             tooltip="Supervised asyncio tasks (feed observer, RiskSweep, claim / trim), each stamping its liveness; "
                     "they never block ingestion. The sweep reads entity state from PostgreSQL, so it keeps running "
                     "while SM8 rebuilds Redis; only the feed observer waits for READY.")
    p.vertex("rules", "<b>Rule plugins</b> &middot; Strategy (P9) &middot; loaded by RuleRegistry from config",
             X(2), y5, W(2, 4), gH,
             "rounded=1;arcSize=3;html=1;fillColor=none;strokeColor=" + PAT + ";dashed=1;strokeWidth=1.5;align=left;"
             "verticalAlign=top;spacingLeft=8;spacingTop=4;fontSize=11;fontColor=" + PAT + ";", "L5", check=False)
    p.vertex("rule_if", tag("interface", "Strategy", "P9") +
             "<b>Rule</b> &mdash; evaluate(snapshot, subject) &rarr; Result{level PASS | FLAG | SOFT_HALT | HARD_HALT, "
             "cap (none, or SOFT when an input is stale), reason, risk_pct?} or UNKNOWN &middot; each rule has its own "
             "confirm_ms &middot; subject = the account, or an order / position",
             1250, y5 + 30, 700, hI, kstyle("interface"), "L5")
    ry = y5 + 30 + hI + 55
    RW = 232
    RX = [985 + i * 244 for i in range(5)]   # r_size, r_agg, r_sl, r_loss, r_margin
    p.vertex("r_size", tag("class", "Rule plugin") +
             "<b>MaxPositionRiskRule</b> (1R = 1.0 % equity)<br>"
             "risk() of each order (ordered_units; for a reduce-only order only its opening_units) and each OPEN "
             "position, as % of equity_mark:<br>"
             "&bull; &le; 1.0 % &rarr; PASS<br>"
             "&bull; 1.0&ndash;1.5 % &rarr; <b>FLAG</b> (slippage, spread, SL move: audit)<br>"
             "&bull; &gt; 1.5 % &rarr; <b>SOFT_HALT</b><br>"
             "&bull; UNKNOWN (no spec / no mid) &rarr; capped <b>SOFT_HALT</b> after 30 s",
             RX[0], ry, RW, hR, kstyle("class", "strokeWidth=2;"), "L5",
             tooltip="Banded: the 1R cap (1.0 %) is the SOP limit; up to 1.5 % is tolerated variance and logged; "
                     "above 1.5 % is a breach. Re-evaluated by every sweep, so a widened stop or a falling equity_mark is "
                     "caught without any new event. Only the opening part of an order is checked (reducing units carry no "
                     "risk). The computed risk % is stored with the "
                     "verdict; it is never on the DTO.")
    p.vertex("r_agg", tag("class", "Rule plugin") +
             "<b>MaxAggregateOpenRiskRule</b><br>&Sigma; risk() over the <b>opening</b> units only: remaining "
             "&minus; reducing units of WORKING / PARTIALLY_FILLED orders + OPEN positions; no netting; active once "
             "the sweep has seen it for confirm_ms (1 s), released after 5 s below the band by a 10 % margin:<br>"
             "&bull; &gt; 2.0 % &rarr; <b>FLAG</b><br>&bull; &gt; 3.0 % &rarr; <b>SOFT_HALT</b> <i>(example)</i>",
             RX[1], ry, RW, hR, kstyle("class", "strokeWidth=2;"), "L5",
             tooltip="Catches split or duplicated orders: three 0.9 % orders each pass MaxPositionRiskRule but sum to "
                     "2.7 %. remaining_units (not ordered_units) avoids counting a partial fill twice, and the 1 s "
                     "confirm absorbs the moment when a fill is committed as both an order and a position.")
    p.vertex("r_sl", tag("class", "Rule plugin") +
             "<b>MissingStopLossRule</b><br>for every entity where needs_stop() and effective_stop() is None "
             "(opening orders <b>and</b> OPEN positions):<br>"
             "&bull; the RiskSweep writes effective_stop_missing_since_ms in PostgreSQL the first time it sees it, and "
             "clears it when a stop appears<br>"
             "&bull; now &minus; since &ge; grace (2 s) &rarr; <b>SOFT_HALT</b>; released once an effective stop has been "
             "seen for 5 s (at once if the entity is terminal or CLOSED)<br>"
             "&bull; a linked child change wakes the sweep for the whole account, so the parent is re-checked at once",
             RX[2], ry, RW, hR, kstyle("class"), "L5",
             tooltip="The grace window covers brokers that attach the stop-loss in a second message. The deadline is "
                     "a durable PostgreSQL timestamp checked every second, so a restart or leader change can't lose or "
                     "postpone it, and no further event is needed for it to fire.")
    p.vertex("r_loss", tag("class", "Rule plugin") +
             "<b>MaxDailyLossRule</b><br>(equity_mark &minus; sod_equity &minus; net_transfers_today) / sod_equity "
             "&lt; &minus;3 % &rarr; HARD_HALT <i>(example)</i> once seen for confirm_ms (1 s) with clean, uncapped "
             "inputs; a stale mark or partial_session caps it at SOFT; the subject is (account, trading_day)<br>"
             "&bull; re-run by every sweep against current prices; capped at SOFT while a position change is newer than the "
             "balance (cross-stream skew)<br>"
             "&bull; one marking function all day (clean mid); broker equity is only a cross-check (FLAG "
             "equity_divergence)<br>"
             "&bull; day roll 17:00 America/New_York: the first sweep pass after it writes the sod row (inputs + the "
             "unexplained increase excluded at the roll); an unexplained decrease triggers fetch_cash_flows<br>"
             "&bull; transfers = DEPOSIT / WITHDRAWAL whose effect is not in sod_equity; TRADE_PNL / FINANCING / COMMISSION / "
             "ADJUSTMENT are P&amp;L; an unexplained decrease is a loss, an unexplained increase is ignored until "
             "explained (FLAG); no transfer source &rarr; transfers_unverified", RX[3], ry, RW, hR, kstyle("class"), "L5",
             tooltip="A clean mid ignores the 17:00 rollover spread blow-out, and swaps / commissions are part of the "
                     "mark, so neither can masquerade as a transfer or a loss. A deposit explained only after the roll "
                     "counts as today's transfer, so it can't hide a loss. With no transfer source a real withdrawal "
                     "trips HARD_HALT: that fails safe.")
    p.vertex("r_margin", tag("class", "Rule plugin") +
             "<b>MarginUsageRule</b><br>margin_used / equity_mark &gt; 80 % &rarr; FLAG; &gt; 95 % &rarr; SOFT_HALT "
             "<i>(example)</i><br>evaluated by every sweep from account_live:{id}; a missing or sampled field is "
             "UNKNOWN", RX[4], ry, RW, hR,
             kstyle("class"), "L5")
    p.vertex("halt", tag("class", "Circuit Breaker &middot; kill switch", "P11") +
             "<b>HaltSwitch</b><br>level NONE | SOFT | HARD &middot; PostgreSQL halt_events is the truth<br>"
             "Redis halt:state = (effective_level, halt_event_id), written only by a CAS script (newer event id wins)<br>"
             "set only by the RiskSweep after each complete pass: HARD if a state is HARD or the HARD latch is set; "
             "a SOFT state whose accumulated uncapped time reaches T &rarr; HARD (capped spells pause it); SOFT &rarr; NONE after cool_down with none active<br>"
             "operator clear() releases the latch; refused while the latest pass shows an active HARD state",
             X(0), yB, CW, hB5, kstyle("class"), "L5",
             tooltip="Lifecycle SM5 on page 3. One advisory lock serialises trip() and clear(). Every integrity tick "
                     "compares halt_event_id with MAX(halt_events.id): behind is re-applied through the CAS; ahead "
                     "(only after a restore) is alerted, DELeted and re-applied, so a lost or stale value can't open "
                     "the gate.")
    p.vertex("gate", tag("class", "Gatekeeper", "P11") +
             "<b>ActionGate</b><br>allow(action) before every outbound action<br>"
             "SOFT: deny risk-increasing, allow risk-reducing<br>HARD: deny all until operator reset<br>"
             "<b>fail-closed:</b> deny unless redis:boot_id = run_id:epoch, halt:state is present and its event id = "
             "MAX(halt_events.id), this node's epoch = engine_epoch and its lease is valid; any Redis / PostgreSQL "
             "error &rarr; deny. Risk-increasing also needs the takeover settle window passed, the account reconciled "
             "after the latest open, no stale_mark symbol, no UNKNOWN result, and a RiskSweep pass over the account less than 3 s old "
             "(canonical list: oversee-rules.md &sect;8)",
             X(1), yB, CW, hB5, kstyle("class"), "L5")
    p.vertex("execgw", tag("class", "Phase 2 &middot; LOCKED") +
             "<b>ExecutionGateway</b> <i>(future)</i><br>does not exist in Phase 1. Contract: ActionGate.allow() "
             "&rarr; commit outbound_intent (epoch) &rarr; lease check with no await &rarr; socket write; after a "
             "takeover the gate stays shut for TTL + TCP_USER_TIMEOUT + ack timeout and earlier-epoch SENDING intents "
             "become UNKNOWN (never re-sent)",
             X(2), yB, CW, hB5, kstyle("class", "dashed=1;opacity=70;fillColor=#F2F2F2;strokeColor=#B71C1C;"), "L5",
             tooltip="Shown only to fix the contract for Phase 2. Blocked today by the Execution Lockout Guard.")
    p.vertex("disp", tag("active", "Strategy channels &middot; Outbox", "P10") +
             "<b>NotificationDispatcher</b><br>alert = verdict + rule + account image excerpt<br>"
             "one alert per state episode (activate / rise), re-page every T unless acknowledged; state kept in PostgreSQL so a new leader "
             "doesn't re-send; only the level-raising trip() alert "
             "bypasses it<br>"
             "routing: FLAG &rarr; email &middot; SOFT/HARD &rarr; email + mobile push<br>"
             "reliable via notification_outbox (SM6, page 3); channel credentials from Secrets Manager, re-read on an auth error",
             X(3), yB, W(3, 4), hB5, kstyle("active"), "L5")
    p.vertex("ch_email", tag("class", "NotificationChannel") + "<b>EmailChannel</b><br>SMTP / SES",
             X(3), yC, CW, hC5, kstyle("class"), "L5")
    p.vertex("ch_push", tag("class", "NotificationChannel") + "<b>MobilePushChannel</b><br>FCM / APNs / Telegram bot",
             X(4), yC, CW, hC5, kstyle("class"), "L5")
    p.vertex("x_smtp", tag("external", "external system") + "<b>Email provider</b> (operator, risk manager)",
             X(3), yD, CW, hD5, kstyle("external"), "L5")
    p.vertex("x_push", tag("external", "external system") + "<b>Mobile push provider</b> (on-call phone)",
             X(4), yD, CW, hD5, kstyle("external"), "L5")
    td = "style='border:1px solid #D6B656;padding:2px 4px'"
    p.vertex("verdicts", tag("note") + "<b>Halt level &rarr; effect on actions</b>"
             f"<table style='border-collapse:collapse;font-size:10px;margin-top:3px'>"
             f"<tr><td {td}><b>Level</b></td><td {td}><b>Log</b></td><td {td}><b>Notify</b></td>"
             f"<td {td}><b>New order to API</b></td><td {td}><b>Risk-reducing</b></td></tr>"
             f"<tr><td {td}>PASS</td><td {td}>&ndash;</td><td {td}>&ndash;</td><td {td}>allowed</td><td {td}>allowed</td></tr>"
             f"<tr><td {td}>FLAG</td><td {td}>halt_breaches + oversee_events</td><td {td}>email</td><td {td}>allowed</td><td {td}>allowed</td></tr>"
             f"<tr><td {td}>SOFT_HALT</td><td {td}>+ halt_breaches</td><td {td}>email + push</td><td {td}><b>blocked</b></td><td {td}>allowed</td></tr>"
             f"<tr><td {td}>HARD_HALT</td><td {td}>+ halt_breaches</td><td {td}>email + push (high)</td><td {td}><b>blocked</b></td>"
             f"<td {td}><b>blocked</b></td></tr></table>"
             "FLAG examples: risk 1.0&ndash;1.5 % of equity, stale account image, equity_divergence, "
             "unexplained_balance_delta. "
             "halt_events gets a row only when trip() raises the level or clear() lowers it. "
             "Phase 1 sends nothing, so halts are logged, alerted and held in halt:state for Phase 2.",
             X(0), yC, W(0, 1), notesH, kstyle("note"), "L5")
    p.vertex("rule_cfg", tag("note") + "<b>Example rule config</b><br><font face='monospace' style='font-size:9px'>"
             "rules:<br>&nbsp;&nbsp;max_position_risk:<br>&nbsp;&nbsp;&nbsp;&nbsp;flag_above_pct_equity: 1.0 &nbsp;# 1R<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;halt_above_pct_equity: 1.5<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;on_breach: SOFT_HALT<br>&nbsp;&nbsp;max_aggregate_open_risk:<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;flag_above_pct_equity: 2.0<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;halt_above_pct_equity: 3.0<br>&nbsp;&nbsp;&nbsp;&nbsp;confirm_ms: 1000<br>"
             "&nbsp;&nbsp;missing_stop_loss:<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;grace_ms: 2000<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;on_breach: SOFT_HALT<br>&nbsp;&nbsp;max_daily_loss:<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;limit_pct_of_sod_equity: 3.0<br>&nbsp;&nbsp;&nbsp;&nbsp;confirm_ms: 1000<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;on_breach: HARD_HALT<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;day_roll: \"17:00 America/New_York\"<br>"
             "&nbsp;&nbsp;soft_halt:<br>&nbsp;&nbsp;&nbsp;&nbsp;escalate_after_ms: 900000<br>"
             "&nbsp;&nbsp;&nbsp;&nbsp;cool_down_ms: 300000<br>"
             "marks:<br>&nbsp;&nbsp;max_clean_spread_pips: {EURUSD: 3.0}<br>&nbsp;&nbsp;stale_mark_ms: 5000<br>"
             "sweep:<br>&nbsp;&nbsp;interval_ms: 1000<br>&nbsp;&nbsp;release_ms: 5000 &nbsp;# margin 10 %<br>"
             "&nbsp;&nbsp;unknown_grace_ms: 30000<br>&nbsp;&nbsp;account_fence_timeout_ms: 10000</font>",
             X(2), yC, CW, notesH, kstyle("note"), "L5")
    p.vertex("ov_rt", tag("note") + "<b>Oversee runtime guarantees</b> (P9)<br>"
             "<b>&#9312; Level-triggered, no timers.</b> The RiskSweep re-evaluates every account every second (and at "
             "once when the feed observer wakes it) from one PostgreSQL REPEATABLE READ snapshot plus Redis prices and "
             "live fields read with their timestamps. Rule states (halt_breaches) and the halt level are recomputed, "
             "not accumulated: a state is <b>active</b> once seen for its rule's confirm_ms, takes the latest level "
             "within its cap (a stale or estimated input caps at SOFT; HARD needs uncapped inputs), and lowers or "
             "resolves only after staying below its level by a 10 % release margin for 5 s (no flapping). UNKNOWN "
             "inputs change nothing for 30 s (unknown_since), then give a capped SOFT state. Escalation uses each "
             "state's accumulated uncapped_ms, which capped spells and FLAG lowerings pause but never reset. Every "
             "deadline is a durable PostgreSQL value (stop_watch, halt_breaches, stream_health), so nothing can be "
             "lost, pushed forward or raced. The snapshot is READ ONLY; writes go in a separate epoch-checked "
             "transaction. Passes are per account: one that fails changes nothing for that account, and 10 s without a "
             "good pass gives a capped SOFT evaluation_failed state.<br>"
             "<b>&#9313; Crash recovery.</b> The sweep keeps no state outside PostgreSQL, so a restart or leader change "
             "resumes at the next pass. Feed: on start-up and every 30 s XAUTOCLAIM entries idle &gt; 30 s; an entry "
             "delivered 5 times is quarantined with an alert (it only loses its wake-up; the next pass still runs).<br>"
             "<b>&#9314; Bounded feed.</b> Every 10 s: XTRIM oversee:feed MINID = oldest un-acked entry. XADD also "
             "carries a hard cap MAXLEN ~ 1 000 000; group lag &gt; 50 % of the cap raises an alert.<br>"
             "<b>&#9315; Fence by account-stream health, not image age.</b> The account image counts as current while "
             "every stream of its adapter is SUBSCRIBED and reconciled in this term; stream_health.conn_lost_at records when one left, and every takeover starts with it set (durable). conn:{adapter_id}:account is refreshed by transport heartbeats only while the "
             "account subscription is SUBSCRIBED (cTrader: from ProtoOAAccountAuthRes until AccountDisconnect / "
             "TokenInvalidated / ClientDisconnect); Redis expires it otherwise, and the 60 s REST poll on a flat "
             "account never refreshes it. Lost for longer than ACCOUNT_FENCE_TIMEOUT_MS (10 s) &rarr; FLAG "
             "stale_account_image, and that account's results are capped at SOFT (also during an SM3 incident).<br>"
             "<b>&#9316; Idempotent by construction.</b> Each transition writes one oversee_events row keyed (rule_id, "
             "subject, transition, sweep_id); replays, heals and re-feeds produce no transition, so they can't "
             "double-alert. One alert per state episode, re-paged every T unless acknowledged. A ruleset reload wakes every account and resolves the states of removed rules.<br>"
             "<b>&#9317; Redis restart.</b> redis:boot_id holds Redis's run_id:epoch and is written <b>last</b> by "
             "SM8, after the queued NO_BOOT writes. Until then ActionGate denies everything and the feed observer "
             "waits; the RiskSweep keeps running from PostgreSQL.",
             X(0), yE, W(0, 4), hE, kstyle("note"), "L5")
    p.vertex("risk_defs", tag("note") + "<b>Shared risk definitions</b> (pure functions every rule uses)<br>"
             "<b>risk(e)</b> is directional and floored at 0: BUY (entry &minus; stop) &times; units, SELL (stop &minus; "
             "entry) &times; units, then &times; FX(pnl_ccy &rarr; account currency) at a clean mid. A stop at or "
             "past breakeven counts 0. UNKNOWN only for a missing InstrumentSpec, a WORKING order whose stop is "
             "on the wrong side, or no mid ever seen. Prices are per (adapter, symbol).<br>"
             "<b>effective_stop(e)</b> = e's own stop_loss (cTrader native), else the non-terminal linked "
             "reduce-only stop children, counted only when their remaining units cover e's units, else None.<br>"
             "<b>needs_stop(e)</b> = an OPEN position, or a PENDING / WORKING / PARTIALLY_FILLED order with opening units "
             "(remaining &minus; reducing &gt; 0). Terminal orders and CLOSED positions never need a stop, and their "
             "breaches resolve.<br>"
             "<b>reducing_units(order)</b> = 0 unless is_reduce_only (explicit broker evidence only); then min("
             "remaining_units, units of its linked entity: the OPEN position on the same symbol and opposite side, "
             "or the still-pending parent entry). That part carries 0 risk and needs no stop; any excess is "
             "opening.<br>"
             "<b>clean mid</b> = last mid with spread &le; max_clean_spread (rule config, pips). While the symbol is "
             "closed or in its roll break (SM10), the last clean mid before the break is used with FLAG stale_mark "
             "(never UNKNOWN). While OPEN and ticking but without a clean quote for longer than stale_mark_ms, "
             "positions are marked at the liquidation side of the latest raw quote (bid for longs, ask for shorts); "
             "with no quote at all for that long, the last mid is used with FLAG stale_mark (a frozen feed is never "
             "clean). Every stale case caps results at SOFT; HARD needs a clean mid. Last clean mids persist in PostgreSQL "
             "last_clean_marks, so a new leader is never blind. "
             "<b>equity_mark</b> = balance + &Sigma; OPEN positions (P&amp;L at clean mid + swap and commission not "
             "yet booked to the balance), converted at clean cross mids, minus unexplained balance increases. "
             "<b>sod_equity</b> = equity_mark from the last clean mids before the 17:00 New York roll (mids and "
             "times stored in sod_marks). No mid ever &rarr; UNKNOWN (mark_unknown), never HARD_HALT.",
             X(0), yF, W(0, 4), hF, kstyle("note"), "L5")
    comment("cm5", "<b>What Oversee is</b><br>A queue observer of <b>committed</b> state plus a level-triggered "
            "<b>RiskSweep</b>. The observer reads oversee:feed (appended in the same atomic step that copies state into "
            "Redis) and wakes the sweep for that account; the sweep re-evaluates every account every second from one "
            "PostgreSQL snapshot plus current prices.<br><br><b>Rules are plugins</b><br>Each rule is a Strategy class "
            "with its own thresholds and confirm_ms; adding one never touches Oversee.<br><br><b>Soft vs hard</b><br>"
            "SOFT blocks risk-increasing actions while a SOFT state is active, plus cool-down; a SOFT state whose "
            "accumulated uncapped time reaches T escalates to HARD (capped spells pause the clock). HARD blocks everything until an operator clear(), which is refused while "
            "the condition is still HARD.<br><br><b>Alerts</b><br>Email for flags; email + mobile push for halts."
            "<br><br><b>No timers</b><br>Every deadline is a durable timestamp checked each second, so nothing is lost "
            "on a restart or a leader change, and a replay can't double-alert.<br><br><b>Testing</b><br>Mock broker "
            "fixtures: risk 1.2 % &rarr; FLAG; 2 % &rarr; SOFT_HALT; EUR/USD on an AUD account uses the AUD/USD cross; "
            "no stop-loss after the grace window &rarr; SOFT_HALT, also across a restart; dead account stream &rarr; "
            "FLAG; aggregate 3.2 % &rarr; SOFT, then 1.2 % &rarr; resolved, no escalation.", y5, cm5H, "L5")

    # ------------------------------------------------ legend
    lend = legend(p, LX, L5y + L5h + 30, LW,
                  ["external", "guard", "interface", "abstract", "class", "active", "value", "object", "store", "note"],
                  edges=[("data flow", C_DATA, ""), ("implements", LANE["L2"][1], "dashed=1;endFill=0;"),
                         ("DLQ / recovery", C_DLQ, "dashed=1;"), ("integrity / cache", C_INT, ""),
                         ("oversee / alerts", C_OVR, ""), ("lockout chain", RED, "dashed=1;endArrow=none;")])
    p.w, p.h = 2660, lend + 40

    # ------------------------------------------------ edges
    lock = "dashed=1;endArrow=none;strokeWidth=2;"
    my = tagY + tagH / 2
    p.edge("e_lock1", "tag_stonex", "tag_pepper", "", p.right("tag_stonex"), p.left("tag_pepper"), color=RED, extra=lock)
    p.edge("e_lock2", "tag_pepper", "tag_mock", "", p.right("tag_pepper"), p.left("tag_mock"), color=RED, extra=lock)
    p.edge("e_lock3", "tag_mock", "guard", "", p.right("tag_mock"), p.left("guard", my), color=RED, extra=lock)
    for i, (tg, ad, lbl) in enumerate((("tag_stonex", "a_stonex", "HTTPS JSON + Lightstreamer"),
                                       ("tag_pepper", "a_pepper", "ProtoOA / TLS"),
                                       ("tag_mock", "a_mock", "fixture replay"))):
        p.edge(f"e_raw_{ad}", tg, ad, lbl, p.bot(tg), p.top(ad))
        p.edge(f"e_impl_{ad}", ad, "abc", "implements", p.bot(ad), p.top("abc", cx(i)), color=LANE["L2"][1],
               extra="dashed=1;endFill=0;")
    p.edge("e_dto", "abc", "dto", "returns", p.top("abc", p.bot("dto")[0]), p.bot("dto"), color=LANE["L2"][1],
           extra="dashed=1;endArrow=open;endFill=0;")
    p.edge("e_ingest", "abc", "orch", "async iterators + fetch_*()", p.bot("abc", cx(1)), p.top("orch", cx(1)))
    for q, lbl, i in (("q_price", "put_nowait(PriceDTO)", 0), ("q_account", "put_nowait(AccountDTO)", 1),
                      ("q_position", "put_nowait(PositionDTO)", 2), ("q_order", "put_nowait(OrderDTO)", 3)):
        p.edge(f"e_pub_{q}", "orch", q, lbl, p.bot("orch", qcx(i)), p.top(q))
    dl = "dashed=1;"
    yi = yo + 0.5 * hO
    p.edge("e_invalid", "orch", "spool", "", p.right("orch", yi), p.left("spool", yi), color=C_DLQ, extra=dl,
           tooltip="QueueFull or schema / checksum invalid: offer() is a synchronous in-memory append.")
    p.edge("e_spool", "spool", "dlq", "batch INSERT", p.bot("spool"), p.top("dlq"), color=C_DLQ, extra=dl)
    qm = yq + hQ / 2
    fy = gy1 + 25
    p.edge("e_fail", "bus", "dlq", "unacked / timeout: order, position, account", p.bot("bus", 1290), p.bot("dlq", 1500),
           pts=[(1290, fy), (1500, fy)], color=C_DLQ, extra=dl,
           tooltip="Consumer-side failures (unacked, timed out) on orders.internal.dto, position.state.dto AND "
                   "account.state.dto. "
                   "Price ticks are coalesced instead. QueueFull goes through the DlqSpooler.")
    p.edge("e_due", "dlq", "recovery", "due", p.right("dlq", qm), p.left("recovery", qm), color=C_DLQ, extra=dl)
    p.edge("e_check", "recovery", "abc", "&#9312; fetch_order_status() /<br>fetch_position_status() /<br>"
           "fetch_account_snapshot() /<br>fetch_cash_flows()",
           p.top("recovery", cx(4)), p.bot("abc", cx(4)), color=C_DLQ, extra=dl)
    yr = yo - 12
    p.edge("e_repub", "recovery", "orch", "&#9313; re-submit<br>via publisher", p.top("recovery", 1880), p.top("orch", 1300),
           pts=[(1880, yr), (1300, yr)], color=C_DLQ, extra=dl, label_x=-0.72)
    p.edge("e_quar", "recovery", "quarantine", "&#9314; 5 failures or<br>illegal transition", p.bot("recovery"), p.top("quarantine"),
           color=C_DLQ, extra=dl)
    for q, s in (("q_price", "sub_price"), ("q_account", "acct_proj"), ("q_position", "pos_cons"),
                 ("q_order", "sub_order")):
        p.edge(f"e_sub_{s}", q, s, "subscribe", p.bot(q), p.top(s))
    # bus -> state
    p.edge("e_w_price", "sub_price", "tier1", "price:{adapter}:{symbol}", p.bot("sub_price", 250), p.top("tier1", 250), color=C_INT)
    p.edge("e_w_acct", "acct_proj", "tier1", "account:{id}", p.bot("acct_proj", 600), p.top("tier1", 600), color=C_INT)
    ym = ys + hS + 22
    p.edge("e_w_snap", "acct_proj", "tier2", "account_state", p.bot("acct_proj", 700), p.top("tier2", 870),
           pts=[(700, ym), (870, ym)])
    p.edge("e_w_pos", "pos_cons", "tier2", "positions_state", p.bot("pos_cons", 990), p.top("tier2", 990))
    p.edge("e_w_db", "sub_order", "tier2", "txn write (outbox)", p.bot("sub_order", 1180), p.top("tier2", 1180))
    y30, y70 = yt + 0.3 * hT, yt + 0.7 * hT
    p.edge("e_t12", "tier1", "tier2", "miss &rarr; read DB", p.right("tier1", y30), p.left("tier2", y30), color=C_INT)
    p.edge("e_t21", "tier2", "tier1", "after commit / heal:<br>apply_state.lua", p.left("tier2", y70), p.right("tier1", y70),
           color=C_INT)
    p.edge("e_t23", "tier2", "tier3", "snapshot + WAL", p.right("tier2", y30), p.left("tier3", y30), color=C_INT)
    p.edge("e_t32", "tier3", "tier2", "restore", p.left("tier3", y70), p.right("tier2", y70), color=C_INT)
    wm = yw + hW / 2
    p.edge("e_chk1", "worker", "tier1", "Step 1: compare + heal in place", p.left("worker"), p.bot("tier1", 420),
           pts=[(420, wm)], color=C_INT)
    p.edge("e_chk2", "worker", "tier2", "Step 1 + 2", p.top("worker"), p.bot("tier2", 1170), color=C_INT)
    p.edge("e_chk3", "worker", "tier3", "archive lag + PITR horizon", p.right("worker"), p.bot("tier3", 1950),
           pts=[(1950, wm)], color=C_INT)
    ay = yc + 0.5 * hC
    p.edge("e_reconcile", "worker", "abc", "reconcile after switchover / restore: fetch_instruments(), fetch_open_orders(), fetch_positions(), fetch_account_snapshot(), fetch_cash_flows()", p.bot("worker"),
           p.right("abc", ay), pts=[(1170, rc), (2240, rc), (2240, ay)], color=C_INT, extra="dashed=1;", label_x=-0.55)
    # oversee
    p.edge("e_ov_redis", "tier1", "oversee", "XREADGROUP oversee:feed (post-commit)<br>"
           "read price: / clean_mid: / account_live:<br>write halt:state (CAS)",
           p.bot("tier1", 250), p.top("oversee", 250), color=C_OVR, extra="startArrow=block;startFill=1;")
    iy = y5 + 30 + hI / 2
    p.edge("e_eval", "oversee", "rule_if", "evaluate(snapshot, subject)<br>&rarr; Result | UNKNOWN", p.right("oversee", iy), p.left("rule_if"), color=C_OVR)
    rtop = ry - 25
    ix0, ix1 = 1250 + 20, 1250 + 700 - 20
    for i, rid in enumerate(("r_size", "r_agg", "r_sl", "r_loss", "r_margin")):
        rcx = RX[i] + RW / 2
        if ix0 <= rcx <= ix1:
            p.edge(f"e_r{i + 1}", rid, "rule_if", "", p.top(rid), p.bot("rule_if", rcx), color=LANE["L2"][1],
                   extra="dashed=1;endFill=0;")
        else:
            tx = ix0 if rcx < ix0 else ix1
            p.edge(f"e_r{i + 1}", rid, "rule_if", "", p.top(rid), p.bot("rule_if", tx), pts=[(rcx, rtop), (tx, rtop)],
                   color=LANE["L2"][1], extra="dashed=1;endFill=0;")
    p.edge("e_trip", "oversee", "halt", "SOFT / HARD &rarr; trip()", p.bot("oversee", cx(0)), p.top("halt"), color=C_OVR)
    p.edge("e_alert", "oversee", "disp", "FLAG / SOFT / HARD &rarr; alert", p.bot("oversee", 800), p.top("disp"),
           pts=[(800, chAB), (p.top("disp")[0], chAB)], color=C_OVR)
    p.edge("e_level", "gate", "halt", "level", p.left("gate"), p.right("halt"), color=C_OVR)
    p.edge("e_allow", "execgw", "gate", "allow?", p.left("execgw"), p.right("gate"), color=RED, extra="dashed=1;")
    p.edge("e_email", "disp", "ch_email", "FLAG + halts", p.bot("disp", cx(3)), p.top("ch_email"), color=C_OVR)
    p.edge("e_push", "disp", "ch_push", "SOFT / HARD", p.bot("disp", cx(4)), p.top("ch_push"), color=C_OVR)
    p.edge("e_smtp", "ch_email", "x_smtp", "send", p.bot("ch_email"), p.top("x_smtp"), color=C_OVR)
    p.edge("e_fcm", "ch_push", "x_push", "push", p.bot("ch_push"), p.top("x_push"), color=C_OVR)
    return p


# =================================================================== PAGE 2
REAL = "dashed=1;endArrow=block;endFill=0;endSize=12;"
DEP = "dashed=1;endArrow=open;endFill=0;"
ASSOC = "endArrow=open;endFill=0;"
COMP = "startArrow=diamondThin;startFill=1;startSize=14;endArrow=open;endFill=0;"


def cls(kind, stereo, name, attrs=(), ops=(), obj=False):
    nm = f"<u>{name}</u>" if obj else name
    s = (f"<div style='text-align:center'>{tag(kind)}"
         + (f"<font color='{PAT}' style='font-size:9px'><i>&laquo;{stereo}&raquo;</i></font><br>" if stereo else "")
         + f"<b>{nm}</b></div>")
    if attrs:
        s += "<hr size='1'>" + "<br>".join(attrs)
    if ops:
        s += "<hr size='1'>" + "<br>".join(ops)
    return s


def frame_style(fill):
    return ("swimlane;startSize=30;html=1;fontSize=13;fontStyle=1;collapsible=0;rounded=1;arcSize=2;"
            f"fillColor={fill};strokeColor={PAT};swimlaneFillColor=#FFFFFF;strokeWidth=1.5;dashed=1;"
            "fontColor=#1A1A1A;align=left;spacingLeft=10;")


def page_patterns():
    p = Page("pat", "2 · Design Patterns")
    p.vertex("title",
             "<font style='font-size:22px'><b>Design Patterns</b></font><br>"
             "<font style='font-size:12px' color='#555555'>UML class view of each pattern. P1&ndash;P14 match the "
             "<i>&laquo;pattern&raquo;</i> tags on page 1. Tag colour = element kind (legend at bottom). "
             "Hover any class for detail.</font>",
             40, 16, 2240, 60, title_style())
    FW, GX, GY = 720, 40, 40
    XS = [40, 40 + FW + GX, 40 + 2 * (FW + GX)]
    S = lambda k: kstyle(k, "align=left;spacingTop=2;")
    note = lambda vid, html, x, y, w, h, par: p.vertex(vid, tag("note") + html, x, y, w, h, kstyle("note"), par)

    def F1(ox, oy, F):
        y1 = oy + 46
        h1 = p.rowH([("factory", 120), ("ba", 120)])
        p.vertex("factory", cls("class", "Factory &middot; Registry", "AdapterFactory", ["- _registry: dict[str, type]"],
                                ["+ register(name, cls)", "+ create(BROKER_ADAPTER_TYPE): BrokerAdapter"]),
                 ox + 20, y1, 200, h1, S("class"), F, tooltip="mock | pepperstone_ctrader | stonex -> class.")
        p.vertex("ba", cls("abstract", "Adapter target &middot; Strategy", "BrokerAdapter",
                           ["- _instruments: dict[str, InstrumentSpec]", "&nbsp;&nbsp;(filled in SNAPSHOTTING, read-only)"],
                           ["+ fetch_open_orders(): list[OrderDTO]", "+ fetch_order_status(id): OrderDTO",
                            "+ fetch_positions(): list[PositionDTO]", "+ fetch_position_status(position_id): PositionDTO",
                            "+ fetch_account_snapshot(): AccountDTO", "+ fetch_cash_flows(since_ms): list[CashFlowDTO]",
                            "+ fetch_instruments(): list[InstrumentSpec]",
                            "+ stream_ticks(symbols) &middot; + ensure_ticks(symbols)", "+ stream_order_updates()",
                            "+ stream_positions() &middot; + stream_account()"]),
                 ox + 280, y1, 420, h1, S("abstract"), F, tooltip="Python ABC; read-only by design.")
        y2 = y1 + h1 + 80
        h2 = p.rowH([("sx", 90), ("pp", 90), ("mk", 90)])
        p.vertex("sx", cls("class", "Adapter", "StoneXAdapter", ["- session_token", "- ls: LightstreamerClient"]),
                 ox + 20, y2, 210, h2, S("class"), F)
        p.vertex("pp", cls("class", "Adapter", "PepperstoneAdapter", ["- conn: ProtoOA over TLS (protobuf)", "- auth: app + account (OAuth pair)"]),
                 ox + 250, y2, 210, h2, S("class"), F)
        p.vertex("mk", cls("class", "Adapter &middot; test double", "MockBrokerAdapter", ["- fixtures (114 trades)"]),
                 ox + 480, y2, 220, h2, S("class"), F)
        y3 = y2 + h2 + 50
        h3 = p.rowH([("rp", 100), ("n1", 100)])
        p.vertex("rp", cls("class", "Retry &middot; Heartbeat", "ReconnectPolicy",
                           ["- base 1 s &middot; cap 60 s &middot; jitter U(0, 0.5)", "- heartbeat per adapter: cTrader 10 s,",
                            "&nbsp;&nbsp;StoneX Lightstreamer keepalive;", "&nbsp;&nbsp;no inbound 30 s &rarr; drop"],
                           ["+ next_delay(n) = min(60, 2<sup>n</sup> + jitter)"]),
                 ox + 20, y3, 360, h3, S("class"), F)
        note("n1", "<b>Adapter</b> translates one broker protocol into BrokerAdapter. <b>Strategy</b>: callers depend "
             "only on BrokerAdapter. <b>Factory</b>: a new broker is one registered class.", ox + 400, y3, 300, h3, F)
        p.edge("f1a", "factory", "ba", "&laquo;creates&raquo;", p.right("factory"), p.left("ba", y1 + h1 / 2), extra=DEP)
        p.edge("f1b", "sx", "ba", "", p.top("sx"), p.bot("ba", ox + 320), pts=[(ox + 125, y2 - 40), (ox + 320, y2 - 40)], extra=REAL)
        p.edge("f1c", "pp", "ba", "", p.top("pp"), p.bot("ba", ox + 355), extra=REAL)
        p.edge("f1d", "mk", "ba", "", p.top("mk"), p.bot("ba", ox + 590), extra=REAL)
        p.edge("f1e", "sx", "rp", "uses", p.bot("sx"), p.top("rp", ox + 125), extra=ASSOC)
        p.edge("f1f", "pp", "rp", "uses", p.bot("pp"), p.top("rp", ox + 355), extra=ASSOC)
        return y3 + h3

    def F2(ox, oy, F):
        y1 = oy + 46
        hT = p.H("tr", 70)
        p.vertex("tr", cls("interface", None, "Transport", (), ["+ request(method, path, body)  <i>(REST)</i>", "+ send(payload_type, msg)  <i>(ProtoOA)</i>"]),
                 ox + 200, y1, 320, hT, S("interface"), F)
        y2 = y1 + hT + 80
        hR, hH = p.H("rot", 130), p.H("http", 90)
        p.vertex("rot", cls("class", "Protection Proxy", "ReadOnlyTransport",
                            ["- inner: Transport", "- deny_paths = {/newtradeorder, /newstoplimitorder,",
                             "&nbsp;&nbsp;/updatetradeorder, /updatestoplimitorder, /cancel}",
                             "- allow_payload_types (ProtoOA): the", "&nbsp;&nbsp;canonical list, adapter-isolation.md &sect;4"],
                            ["+ request() / send(): raise ExecutionLockedError", "&nbsp;&nbsp;if path in deny_paths, payloadType not",
                             "&nbsp;&nbsp;allowed, or BROKER_ENV not in", "&nbsp;&nbsp;{sandbox, live_readonly}"]),
                 ox + 20, y2, 340, hR, S("class"), F)
        p.vertex("http", cls("class", "Real Subject", "HttpTransport", ["- httpx / Lightstreamer client"],
                             ["+ request(method, path, body)"]), ox + 440, y2, 260, hH, S("class"), F)
        y3 = y2 + max(hR, hH) + 40
        h3 = p.rowH([("hook", 100), ("n2", 100)])
        p.vertex("hook", cls("guard", "Guard (tooling)", "PreToolUse hook",
                             [".claude/hooks/pre_tool_guard.py", "denies agent tool calls to", "place_order / modify_order / cancel_order"]),
                 ox + 20, y3, 340, h3, S("guard"), F)
        note("n2", "Adapters only ever get a <b>ReadOnlyTransport</b>, so even a buggy adapter can't reach an order "
             "endpoint. Deny by <b>path</b>, not verb: StoneX <i>activeorders</i> is a read-only POST.",
             ox + 390, y3, 310, h3, F)
        p.edge("f2a", "rot", "tr", "", p.top("rot", ox + 190), p.bot("tr", ox + 280),
               pts=[(ox + 190, y2 - 40), (ox + 280, y2 - 40)], extra=REAL)
        p.edge("f2b", "http", "tr", "", p.top("http"), p.bot("tr", ox + 440), pts=[(ox + 570, y2 - 40), (ox + 440, y2 - 40)], extra=REAL)
        dy = y2 + min(hR, hH) / 2
        p.edge("f2c", "rot", "http", "delegates<br>allowed", p.right("rot", dy), p.left("http", dy), extra=ASSOC)
        return y3 + h3

    def F3(ox, oy, F):
        y1 = oy + 46
        hM, hO = p.H("mapper", 130), p.H("odto", 210)
        p.vertex("mapper", cls("class", "Message Translator", "StatusMapper",
                               ["NEW / ACCEPTED &rarr; WORKING", "PARTIALLY_FILLED &rarr; PARTIALLY_FILLED",
                                "FILLED / EXECUTED &rarr; FILLED", "CANCELLED / EXPIRED &rarr; CANCELLED", "REJECTED &rarr; REJECTED"],
                               ["+ to_order_status(raw): OrderStatus"]), ox + 20, y1, 300, hM, S("class"), F)
        p.vertex("odto", cls("value", "frozen dataclass", "OrderDTO",
                             ["client_order_id, broker_order_id: str", "account_id, adapter_id, symbol: str", "side: BUY | SELL",
                              "status: OrderStatus", "entry_price: Decimal", "stop_loss, take_profit: Decimal | None",
                              "<b>ordered_units, remaining_units: Decimal | None</b>", "instrument_unknown: bool &middot; raw_qty?", "<b>is_reduce_only: bool</b> &middot; linked_position_id?",
                              "<b>broker_updated_at_ms, broker_seq?, source</b>", "seq_id: int (HLC) &middot; checksum: str"]),
                 ox + 380, y1, 320, hO, S("value"), F,
                 tooltip="No risk field: adapters can't see equity, so risk is computed only by Oversee and stored with "
                         "the verdict. The version is (broker_seq, broker_updated_at_ms, observed_seq), then seq_id: the later "
                         "observation wins a tie.")
        yS = y1 + hM + 50
        hS = p.H("ostat", 80)
        p.vertex("ostat", cls("enum", None, "OrderStatus",
                              ["PENDING &middot; WORKING &middot; PARTIALLY_FILLED", "FILLED &middot; CANCELLED &middot; REJECTED"]),
                 ox + 20, yS, 300, hS, S("enum"), F)
        yP = y1 + hO + 30
        hP = p.H("pdto", 90)
        p.vertex("pdto", cls("value", "frozen dataclass", "PriceDTO",
                             ["adapter_id, symbol: str", "bid, ask, spread_pips: Decimal", "timestamp_ms, seq_id: int"]),
                 ox + 380, yP, 320, hP, S("value"), F)
        yA = yP + hP + 30
        hA = p.H("adto", 120)
        p.vertex("adto", cls("value", "frozen dataclass &middot; new", "AccountDTO",
                             ["account_id, adapter_id, currency: str", "balance, realised_pnl_today: Decimal",
                              "<i>live, unversioned:</i> equity?, margin_used,", "&nbsp;&nbsp;free_margin, unrealised_pnl",
                              "broker_updated_at_ms, broker_seq?, source, seq_id", "checksum: str"]),
                 ox + 380, yA, 320, hA, S("value"), F, tooltip="Event form of the account image.")
        yPo = yA + hA + 30
        hPo = p.H("posdto", 130)
        p.vertex("posdto", cls("value", "frozen dataclass &middot; new", "PositionDTO",
                               ["position_id, account_id, adapter_id: str", "symbol, side, entry_price", "units: Decimal | None, raw_qty?",
                                "instrument_unknown: bool",
                                "stop_loss, take_profit: Decimal | None", "status: OPEN | CLOSED",
                                "accrued_swap, commission: Decimal", "opened_by_client_order_id?",
                                "broker_updated_at_ms, broker_seq?, source, seq_id", "checksum: str"]),
                 ox + 380, yPo, 320, hPo, S("value"), F,
                 tooltip="No unrealised P&L: positions are marked from their adapter's price: keys, so a position's version changes "
                         "only on units, stop, take-profit or status.")
        yI = yS + hS + 30
        hI = p.H("ispec", 100)
        p.vertex("ispec", cls("value", "frozen dataclass &middot; new", "InstrumentSpec",
                              ["adapter_id, symbol, base_ccy, quote_ccy,", "pnl_ccy, contract_size, point_size, min_qty",
                               "unit_kind: UNITS | CENTI_UNITS | LOTS |", "&nbsp;&nbsp;CONTRACTS | STAKE", "swap_booked_to_balance: bool"],
                              ["+ to_notional(broker_qty): Decimal"]),
                 ox + 20, yI, 300, hI, S("value"), F,
                 tooltip="Fetched by fetch_instruments() at start-up and reconnect. Each adapter keeps its own "
                         "read-only copy (no db / cache import) to normalise quantity; the orchestrator also stores "
                         "it in PostgreSQL instruments and Redis instrument:{adapter_id}:{symbol}.")
        yCf = yI + hI + 30
        hCf = p.H("cfdto", 80)
        p.vertex("cfdto", cls("value", "frozen dataclass &middot; new", "CashFlowDTO",
                              ["transaction_id <i>(dedupe key)</i>", "account_id, kind: DEPOSIT | WITHDRAWAL |",
                               "&nbsp;&nbsp;TRADE_PNL | FINANCING | COMMISSION |", "&nbsp;&nbsp;ADJUSTMENT",
                               "amount, currency, broker_ts",
                               "adapter_id, seq_id, checksum"]),
                 ox + 20, yCf, 300, hCf, S("value"), F,
                 tooltip="Only DEPOSIT / WITHDRAWAL count toward net_transfers_today; the other kinds explain balance "
                         "moves as P&L. cTrader DepositWithdraw; StoneX cash history where one exists.")
        yN = yCf + hCf + 30
        hN = max(p.H("n3", 100), yPo + hPo - yN)
        note("n3", "Every adapter emits the same DTOs, so everything downstream is broker-agnostic. <b>frozen=True</b> + "
             "checksum: safe to share across async tasks, tampering is detectable. <b>Version</b>: the broker's own "
             "revision or time decides which copy is newer, then observed_seq (the later observation wins a tie); a "
             "same-checksum write is a no-op. <b>Units</b> are "
             "one canonical unit for every broker; ordered vs remaining keeps partial fills from being counted twice.",
             ox + 20, yN, 300, hN, F)
        p.edge("f3a", "mapper", "ostat", "maps to", p.bot("mapper"), p.top("ostat", ox + 170), extra=DEP)
        ya_, yb_ = y1 + hO - 20, yS + hS / 2
        p.edge("f3b", "odto", "ostat", "status", p.left("odto", ya_), p.right("ostat", yb_),
               pts=[(ox + 350, ya_), (ox + 350, yb_)], extra=ASSOC)
        return max(yN + hN, yPo + hPo)

    def F4(ox, oy, F):
        y1 = oy + 46
        h1 = p.rowH([("orch", 80), ("bus", 80)])
        p.vertex("orch", cls("active", "Publisher", "IngestionOrchestrator", (), ["+ run()", "- stamp(dto): seq_id = HLC", "- publish(dto): put_nowait", "- QueueFull &rarr; spool.offer()"]),
                 ox + 20, y1, 260, h1, S("active"), F)
        p.vertex("bus", cls("class", "Subject", "EventBus", ["- topics: dict[str, Topic]"],
                            ["+ publish(topic, dto)", "+ subscribe(topic, Subscriber)"]), ox + 340, y1, 360, h1, S("class"), F)
        y2 = y1 + h1 + 60
        h2 = p.rowH([("sub", 70), ("topic", 90)])
        p.vertex("sub", cls("interface", "Observer", "Subscriber", (), ["+ on_event(dto)"]), ox + 20, y2, 260, h2, S("interface"), F,
                 tooltip="Implemented by PriceSubscriber, AccountImageProjector, PositionStateConsumer and "
                         "OrderStateConsumer. Oversee is "
                         "deliberately not a bus subscriber (see P9).")
        p.vertex("topic", cls("class", "Bulkhead", "Topic[T]", ["- queue: asyncio.Queue(maxsize)"], ["+ put_nowait(dto)"]),
                 ox + 340, y2, 360, h2, S("class"), F)
        y3 = y2 + h2 + 60
        h3 = p.rowH([("psub", 60), ("aproj", 60), ("o_price", 60), ("o_acct", 60), ("o_pos", 60), ("o_order", 60)])
        p.vertex("psub", cls("class", None, "PriceSubscriber"), ox + 20, y3, 120, h3, S("class"), F)
        p.vertex("aproj", cls("class", None, "AccountImage Projector"), ox + 150, y3, 150, h3, S("class"), F,
                 tooltip="OrderStateConsumer (P6) implements Subscriber the same way.")
        for i, (o, nm) in enumerate((("o_price", "market.price.dto"), ("o_acct", "account.state.dto"),
                                     ("o_pos", "position.state.dto"), ("o_order", "orders.internal.dto"))):
            p.vertex(o, cls("object", None, f"{nm} :<br>Topic", obj=True), ox + 320 + i * 95, y3, 88, h3, S("object"), F)
        y4 = y3 + h3 + 40
        h4 = p.H("n4", 60)
        note("n4", "<b>Observer</b>: publishers never know who subscribes. Oversee is <b>not</b> a subscriber here: "
             "it reads committed state (P9), so it can't race the consumers. "
             "<b>Bulkhead</b>: one bounded queue per topic, so a price burst cannot starve orders, positions or account "
             "updates.",
             ox + 20, y4, 680, h4, F)
        p.edge("f4a", "orch", "bus", "publish()", p.right("orch"), p.left("bus", y1 + h1 / 2), extra=ASSOC)
        p.edge("f4b", "bus", "topic", "1..*", p.bot("bus", ox + 680), p.top("topic", ox + 680), extra=COMP)
        p.edge("f4c", "topic", "sub", "dispatch", p.left("topic", y2 + h2 / 2), p.right("sub", y2 + h2 / 2), extra=ASSOC)
        p.edge("f4d", "psub", "sub", "", p.top("psub"), p.bot("sub", ox + 80), extra=REAL)
        p.edge("f4e", "aproj", "sub", "", p.top("aproj"), p.bot("sub", ox + 225), extra=REAL)
        for o in ("o_price", "o_acct", "o_pos", "o_order"):
            p.edge(f"f4_{o}", o, "topic", "", p.top(o), p.bot("topic", p.top(o)[0]), extra=DEP, color=KINDS["object"][3])
        return y4 + h4

    def F5(ox, oy, F):
        y0 = oy + 46
        h0 = p.rowH([("spool", 90), ("spill", 90)])
        p.vertex("spool", cls("active", "Spooler", "DlqSpooler", ["- ring: deque(maxlen=N)"],
                              ["+ offer(event, reason)  <i>append only, no I/O</i>", "- flush(): batch INSERT",
                               "- spill(): asyncio.to_thread(write + fsync),", "&nbsp;&nbsp;framed records (length + CRC)"]),
                 ox + 20, y0, 330, h0, S("active"), F,
                 tooltip="The orchestrator only appends to the ring; the spooler's own task does the database write, "
                         "and disk writes run in a worker thread.")
        p.vertex("spill", cls("store", "local file", "var/dlq_spill.jsonl", ["used only when PostgreSQL is down", "written off the event loop", "drained into events_dlq when PostgreSQL", "&nbsp;&nbsp;accepts writes, and at start-up,", "&nbsp;&nbsp;by any node (idempotent insert)"]),
                 ox + 400, y0, 300, h0, S("store"), F)
        p.edge("f5s", "spool", "spill", "DB down", p.right("spool"), p.left("spill", y0 + h0 / 2), extra=ASSOC)
        y1 = y0 + h0 + 60
        h1 = p.rowH([("dlc", 110), ("rw", 110)])
        p.vertex("dlc", cls("store", "Dead Letter Channel", "events_dlq",
                            ["dlq_id, <b>event_type: ORDER | POSITION | ACCOUNT | CASH_FLOW</b>",
                             "client_order_id | position_id | account_id |", "&nbsp;&nbsp;transaction_id, adapter_id",
                             "raw_payload, error_reason", "retry_count, status, next_retry_at_ms"],
                            ["park(event, reason) &middot; due(now)"]), ox + 20, y1, 330, h1, S("store"), F)
        p.vertex("rw", cls("active", "Idempotent Retry", "DlqRecoveryWorker", (),
                           ["+ run()", "- check_before_retry(row):", "&nbsp;&nbsp;ORDER: fetch_order_status()",
                            "&nbsp;&nbsp;POSITION: fetch_position_status()",
                            "&nbsp;&nbsp;ACCOUNT: fetch_account_snapshot()", "&nbsp;&nbsp;CASH_FLOW: fetch_cash_flows()",
                            "&nbsp;&nbsp;then re-submit via orchestrator"]),
                 ox + 400, y1, 300, h1, S("active"), F)
        y2 = y1 + h1 + 70
        h2 = p.rowH([("rtp", 70), ("qs", 70)])
        p.vertex("rtp", cls("class", "Retry", "RetryPolicy", ["MAX_DLQ_RETRIES = 5", "backoff min(60, 2<sup>n</sup> + U(0, 0.5)) s"]),
                 ox + 20, y2, 330, h2, S("class"), F)
        p.vertex("qs", cls("store", "Poison Message", "events_quarantine", ["manual review"]), ox + 400, y2, 300, h2, S("store"), F)
        y3 = y2 + h2 + 40
        h3 = p.H("n5", 60)
        note("n5", "<b>Dead Letter Channel</b>: failed order, position and account events are parked, never lost. "
             "<b>Spooler</b>: parking is an in-memory append, so the publisher never waits on PostgreSQL. "
             "<b>Check-before-retry</b> (Idempotent Receiver): the broker is asked for the real state (order status, or a "
             "position by id, account snapshot or cash flow by transaction_id; that fresh row replaces the parked one) "
             "before re-submitting, so a retry can't duplicate or "
             "reorder state.", ox + 20, y3, 680, h3, F)
        p.edge("f5t", "spool", "dlc", "batch INSERT", p.bot("spool", ox + 185), p.top("dlc", ox + 185), extra=ASSOC)
        p.edge("f5a", "rw", "dlc", "polls", p.left("rw", y1 + h1 / 2), p.right("dlc", y1 + h1 / 2), extra=ASSOC)
        p.edge("f5b", "rw", "rtp", "uses", p.bot("rw", ox + 430), p.top("rtp"), pts=[(ox + 430, y2 - 35), (ox + 185, y2 - 35)], extra=ASSOC)
        p.edge("f5c", "rw", "qs", "retries &ge; 5", p.bot("rw", ox + 600), p.top("qs", ox + 600), extra=ASSOC)
        return y3 + h3

    def F6(ox, oy, F):
        y1 = oy + 46
        hO = p.H("osc", 60)
        p.vertex("osc", cls("class", "Subscriber", "OrderStateConsumer", (), ["+ on_event(order: OrderDTO)"]),
                 ox + 20, y1, 300, hO, S("class"), F)
        y2 = y1 + hO + 50
        hU = max(p.H("uow", 130), 3 * 50 + 2 * 25)
        p.vertex("uow", cls("class", "Unit of Work", "OrderUnitOfWork", (),
                            ["+ upsert_order_state(order)", "+ enqueue_outbox(order)", "+ append_event(order)",
                             "+ commit() &rarr; then apply_state.lua"]), ox + 20, y2, 300, hU, S("class"), F)
        tb = kstyle("store", "align=center;verticalAlign=middle;spacingTop=4;")
        for vid, lbl, yy in (("t_os", "orders_state", y2), ("t_ob", "outbox_events", y2 + (hU - 50) / 2),
                             ("t_el", "event_log <i>(append-only)</i>", y2 + hU - 50)):
            p.vertex(vid, tag("store") + f"<b>{lbl}</b>", ox + 400, yy, 300, 50, tb, F)
        y3 = y2 + hU + 40
        h3 = p.H("n6", 60)
        note("n6", "<b>Transactional Outbox</b>: state, outbox row and event-log row commit atomically. Redis is only "
             "updated <b>after</b> commit, so the duplicate can lag but never lead. The same Lua call appends to "
             "oversee:feed, so Oversee sees an event only once its state is in Redis. <b>Event Log</b>: append-only "
             "history for replay and audit.", ox + 20, y3, 680, h3, F)
        p.edge("f6a", "osc", "uow", "per OrderDTO", p.bot("osc"), p.top("uow"), extra=ASSOC)
        for vid, lbl in (("t_os", "upsert"), ("t_ob", "insert"), ("t_el", "append")):
            p.edge(f"f6_{vid}", "uow", vid, lbl, p.right("uow", p.left(vid)[1]), p.left(vid), extra=ASSOC)
        return y3 + h3

    def F7(ox, oy, F):
        y1 = oy + 46
        hT = p.H("tier", 90)
        p.vertex("tier", cls("abstract", "handler", "StateTier", ["- next: StateTier | None"],
                             ["+ get(key)", "+ digest(): sha256", "+ max_seq_id(): int"]), ox + 190, y1, 330, hT, S("abstract"), F)
        y2 = y1 + hT + 80
        h2 = p.rowH([("t1", 60), ("t2", 60), ("t3", 60)])
        for vid, lbl, st_, x, w in (("t1", "Tier1RedisCache", "cache-aside &middot; duplicate", 20, 180),
                                    ("t2", "Tier2PostgresDB", "source of truth", 260, 180),
                                    ("t3", "Tier3ImageBackup", "Memento &middot; operator only", 500, 200)):
            p.vertex(vid, cls("class", st_, lbl), ox + x, y2, w, h2, S("class"), F)
        y3 = y2 + h2 + 60
        h3 = p.rowH([("rk", 90), ("snap", 90)])
        p.vertex("rk", cls("store", "intentional duplicate", "Redis key layout",
                           ["order:{client_order_id} &rarr; OrderDTO",
                            "position:{position_id} &rarr; PositionDTO",
                            "account:{account_id} &rarr; account image",
                            "halt:state &rarr; (effective_level, halt_event_id)", "redis:boot_id &rarr; run_id:epoch",
                            "instrument:{adapter_id}:{symbol} &middot; state:digest",
                            "conn:{adapter_id}:{stream} &rarr; TTL 30 s",
                            "<i>light script:</i> price: / clean_mid:{adapter_id}:{symbol},", "&nbsp;&nbsp;account_live:{id}, conn: (no feed entries)",
                            "oversee:feed &rarr; Stream (trimmed MINID, cap ~1 M)",
                            "<i>no timer keys: Oversee keeps no timers</i>"],
                           ["apply_state.lua: SET if version newer", "&nbsp;&nbsp;(fixed-width strings); XADD only if applied;"
                            "XOR digest (order-independent)"]),
                 ox + 20, y3, 440, h3, S("store"), F,
                 tooltip="Written only after the PostgreSQL commit; healed in place by integrity Step 1 through the same script.")
        p.vertex("snap", cls("value", "Memento", "Snapshot", ["image + WAL archive"]), ox + 500, y3, 200, h3, S("value"), F)
        y4 = y3 + h3 + 30
        h4 = p.H("n7", 60)
        note("n7", "<b>Chain of Responsibility</b>: a read tries Redis, then PostgreSQL; a PostgreSQL miss is authoritative, so the "
             "read chain ends at Tier 2. <b>Cache-aside / "
             "write-through</b>: Redis is a deliberate, disposable duplicate of PostgreSQL. Every entity write is a version "
             "compare-and-set, so live writers and the healer can run together without regressing a key. "
             "<b>Memento</b>: Tier 3 is never read live; it serves only an operator-run restore.", ox + 20, y4, 680, h4, F)
        p.edge("f7a", "t1", "tier", "", p.top("t1"), p.bot("tier", ox + 290), pts=[(ox + 110, y2 - 40), (ox + 290, y2 - 40)], extra=REAL)
        p.edge("f7b", "t2", "tier", "", p.top("t2"), p.bot("tier", ox + 350), extra=REAL)
        p.edge("f7c", "t3", "tier", "", p.top("t3"), p.bot("tier", ox + 440), pts=[(ox + 600, y2 - 40), (ox + 440, y2 - 40)], extra=REAL)
        p.edge("f7d", "t1", "t2", "next", p.right("t1"), p.left("t2"), extra=ASSOC)
        p.edge("f7e", "t2", "t3", "archives to", p.right("t2"), p.left("t3"), extra=ASSOC)
        p.edge("f7f", "t3", "snap", "stores", p.bot("t3"), p.top("snap"), extra=COMP)
        p.edge("f7g", "t1", "rk", "keys", p.bot("t1"), p.top("rk", ox + 110), extra=COMP)
        return y4 + h4

    def F8(ox, oy, F):
        y1 = oy + 46
        h1 = p.rowH([("iw", 90), ("ws", 70)])
        p.vertex("iw", cls("active", "Context &middot; Supervisor", "IntegrityWorker", ["- state: WorkerState"],
                           ["+ tick(): state.handle(self)", "+ transition(to)"]), ox + 20, y1, 320, h1, S("active"), F)
        p.vertex("ws", cls("interface", "State", "WorkerState", (), ["+ handle(ctx)"]), ox + 400, y1, 300, h1, S("interface"), F)
        y2 = y1 + h1 + 60
        h2 = p.rowH([("ac", 90), ("cs", 90)])
        p.vertex("ac", cls("active", "Context", "AdapterConnection", ["- state: ConnectionState"],
                           ["DISCONNECTED &rarr; AUTHENTICATING &rarr;", "SNAPSHOTTING &rarr; STREAMING &#8644; BACKOFF",
                            "STREAMING &rarr; RESYNCING (re-subscribe lost", "&nbsp;&nbsp;streams, then REST) &rarr; STREAMING",
                            "AUTHENTICATING &rarr; AUTH_FAILED; env &rarr; REFUSED"]),
                 ox + 20, y2, 320, h2, S("active"), F)
        p.vertex("cs", cls("class", "concrete States", "Idle &middot; CheckRedisVsDb &middot; HealingRedis",
                           ["CheckDbIntegrity &middot; Halted &middot; SwitchingOver", "Reconciling &middot; ManualIntervention"]),
                 ox + 400, y2, 300, h2, S("class"), F)
        y3 = y2 + h2 + 40
        h3 = p.H("n8", 60)
        note("n8", "<b>State</b>: each phase is an object with its own handle(); transitions are explicit and testable "
             "(page 3). <b>Supervisor</b>: the worker heals the tiers instead of letting corruption propagate.",
             ox + 20, y3, 680, h3, F)
        p.edge("f8a", "iw", "ws", "state", p.right("iw"), p.left("ws"), extra=ASSOC)
        p.edge("f8b", "cs", "ws", "", p.top("cs"), p.bot("ws"), extra=REAL)
        return y3 + h3

    def F9(ox, oy, F):   # Oversee rule engine
        y1 = oy + 46
        h1 = p.rowH([("ov", 110), ("reg", 110)])
        p.vertex("ov", cls("active", "Observer of committed state &middot; level-triggered", "Oversee",
                           ["- feed: Redis Stream oversee:feed", "- sweep: RiskSweep (1 s + wake-up)",
                            "- rules: list[Rule]", "- states: halt_breaches (PostgreSQL)"],
                           ["+ on_committed(entry): sweep_now(account)", "+ sweep(account): one REPEATABLE",
                            "&nbsp;&nbsp;READ snapshot + prices &rarr; all rules", "- apply_states(): confirm, cap, resolve",
                            "- set_level(): trip / system clear", "- claim_stale(): XAUTOCLAIM", "- trim_feed(): XTRIM MINID"]),
                 ox + 20, y1, 300, h1, S("active"), F)
        p.vertex("reg", cls("class", "Registry &middot; Factory", "RuleRegistry", ["- _rules: dict[str, type[Rule]]"],
                            ["+ register(name, cls)", "+ load(config): list[Rule]"]), ox + 380, y1, 320, h1, S("class"), F)
        y2 = y1 + h1 + 60
        h2 = p.H("rule", 70)
        p.vertex("rule", cls("interface", "Strategy", "Rule", ["+ id &middot; confirm_ms &middot; release margin"],
                             ["+ evaluate(snapshot, subject):", "&nbsp;&nbsp;Result | UNKNOWN"]), ox + 200, y2, 320, h2, S("interface"), F)
        y3 = y2 + h2 + 70
        h3 = p.rowH([("mpr", 100), ("msl", 100), ("mdl", 100)])
        p.vertex("mpr", cls("class", "Rule plugin", "MaxPositionRiskRule", ["flag_above_pct = 1.0 (1R)",
                                                                              "halt_above_pct = 1.5",
                                                                              "risk(): directional, floored at 0",
                                                                              "orders + OPEN positions"]),
                 ox + 20, y3, 210, h3, S("class"), F, tooltip="&le; 1R PASS; 1R to 1.5 % FLAG; above 1.5 % SOFT_HALT.")
        p.vertex("msl", cls("class", "Rule plugin", "MissingStopLossRule", ["grace_ms = 2000",
                                                                              "needs_stop() and no effective_stop()",
                                                                              "on_breach = SOFT_HALT"]),
                 ox + 250, y3, 210, h3, S("class"), F, tooltip="The grace deadline is effective_stop_missing_since_ms in PostgreSQL, checked every second, so it survives a restart.")
        p.vertex("mdl", cls("class", "Rule plugin", "MaxDailyLossRule", ["limit_pct = 3.0 of sod_equity",
                                                                           "equity_mark at clean mid",
                                                                           "on_breach = HARD_HALT"]),
                 ox + 480, y3, 220, h3, S("class"), F)
        y4 = y3 + h3 + 40
        h4 = p.rowH([("verdict", 80), ("aimg", 80)])
        p.vertex("verdict", cls("enum", None, "Result", ["level: PASS &lt; FLAG &lt; SOFT_HALT &lt; HARD_HALT", "cap: none | SOFT &middot; reason"]),
                 ox + 20, y4, 300, h4, S("enum"), F)
        p.vertex("aimg", cls("value", "read model", "AccountSnapshot", ["READ ONLY REPEATABLE READ txn over the",
                                                                    "oversee-rules.md &sect;2 inputs: current rows,",
                                                                    "sod_marks, cash_flows, stop_watch,",
                                                                    "stream_health, halt_breaches, last_clean_marks",
                                                                    "+ Redis price: / clean_mid: / live"]),
                 ox + 380, y4, 320, h4, S("value"), F)
        y5 = y4 + h4 + 40
        h5 = p.H("n9", 60)
        note("n9", "<b>Observer of committed state</b>: Oversee reads oversee:feed, which is appended in the same atomic "
             "Lua step that copies state into Redis, and wakes the <b>RiskSweep</b>, which re-evaluates the account "
             "from one PostgreSQL snapshot (level-triggered, no timers). <b>Strategy</b>: every risk rule is a plugin "
             "with one evaluate(); <b>Registry</b> builds them from config, so adding a rule never changes Oversee. "
             "Levels are ordered; the most severe one wins.", ox + 20, y5, 680, h5, F)
        p.edge("f9a", "ov", "rule", "rules 1..*", p.bot("ov", ox + 260), p.top("rule", ox + 260), extra=ASSOC)
        p.edge("f9b", "reg", "rule", "&laquo;creates&raquo;", p.bot("reg", ox + 460), p.top("rule", ox + 460), extra=DEP)
        p.edge("f9c", "mpr", "rule", "", p.top("mpr"), p.bot("rule", ox + 240), pts=[(ox + 125, y3 - 35), (ox + 240, y3 - 35)], extra=REAL)
        p.edge("f9d", "msl", "rule", "", p.top("msl"), p.bot("rule", ox + 355), extra=REAL)
        p.edge("f9e", "mdl", "rule", "", p.top("mdl"), p.bot("rule", ox + 480), pts=[(ox + 590, y3 - 35), (ox + 480, y3 - 35)], extra=REAL)
        return y5 + h5

    def F10(ox, oy, F):   # notifications
        y1 = oy + 46
        h1 = p.rowH([("disp", 110), ("chan", 110)])
        p.vertex("disp", cls("active", "Dispatcher", "NotificationDispatcher", ["- channels: list[NotificationChannel]"],
                             ["+ notify(alert)", "- dedupe(rule_id, subject, activation id,", "&nbsp;&nbsp;level): one alert per episode", "- route(level) &rarr; channels"]),
                 ox + 20, y1, 320, h1, S("active"), F)
        p.vertex("chan", cls("interface", "Strategy", "NotificationChannel", (), ["+ send(alert): DeliveryResult"]),
                 ox + 400, y1, 300, h1, S("interface"), F)
        y2 = y1 + h1 + 60
        h2 = p.rowH([("nob", 70), ("em", 70), ("mp", 70)])
        p.vertex("nob", cls("store", "Outbox", "notification_outbox", ["alert, channel, attempts, status"]),
                 ox + 20, y2, 320, h2, S("store"), F)
        p.vertex("em", cls("class", None, "EmailChannel"), ox + 400, y2, 140, h2, S("class"), F)
        p.vertex("mp", cls("class", None, "MobilePushChannel"), ox + 560, y2, 140, h2, S("class"), F)
        y3 = y2 + h2 + 50
        h3 = p.rowH([("smtp", 60), ("fcm", 60), ("pol", 60)])
        p.vertex("pol", cls("value", "config", "AlertPolicy", ["FLAG &rarr; email &middot; SOFT/HARD &rarr; email + push"]),
                 ox + 20, y3, 320, h3, S("value"), F)
        p.vertex("smtp", cls("external", None, "SMTP / SES"), ox + 400, y3, 140, h3, S("external"), F)
        p.vertex("fcm", cls("external", None, "FCM / APNs"), ox + 560, y3, 140, h3, S("external"), F)
        y4 = y3 + h3 + 40
        h4 = p.H("n10", 60)
        note("n10", "<b>Strategy</b>: each channel is a plugin behind NotificationChannel. <b>Outbox</b>: alerts are "
             "written first and sent by a worker, so a crash can't lose a halt alert. <b>Dedupe</b> per state episode (plus the rules' release hysteresis) stops a flapping "
             "rule from spamming; only the level-raising trip() alert bypasses dedupe.", ox + 20, y4, 680, h4, F)
        p.edge("f10a", "disp", "chan", "channels *", p.right("disp"), p.left("chan", y1 + h1 / 2), extra=ASSOC)
        p.edge("f10b", "disp", "nob", "writes", p.bot("disp", ox + 180), p.top("nob", ox + 180), extra=ASSOC)
        p.edge("f10c", "em", "chan", "", p.top("em"), p.bot("chan", ox + 470), extra=REAL)
        p.edge("f10d", "mp", "chan", "", p.top("mp"), p.bot("chan", ox + 630), extra=REAL)
        p.edge("f10e", "em", "smtp", "", p.bot("em"), p.top("smtp"), extra=ASSOC)
        p.edge("f10f", "mp", "fcm", "", p.bot("mp"), p.top("fcm"), extra=ASSOC)
        return y4 + h4

    def F11(ox, oy, F):   # kill switch
        y1 = oy + 46
        h1 = p.rowH([("hs", 110), ("ag", 110)])
        p.vertex("hs", cls("class", "Circuit Breaker", "HaltSwitch",
                           ["- truth: PostgreSQL halt_events", "- copy: Redis halt:state", "&nbsp;&nbsp;(effective_level, halt_event_id), CAS"],
                           ["+ trip(level, rule_id, reason): alerts if the level rises",
                            "+ clear(operator, why, reviewed_event_id,", "&nbsp;&nbsp;force?): releases the HARD latch",
                            "&nbsp;&nbsp;both under pg_advisory_xact_lock",
                            "+ apply(target): RiskSweep only, after", "&nbsp;&nbsp;each pass (trip / system clear)",
                            "+ rehydrate(): latest halt_events &rarr; CAS"]),
                 ox + 20, y1, 320, h1, S("class"), F)
        p.vertex("ag", cls("class", "Gatekeeper", "ActionGate", (),
                           ["+ allow(action): bool", "SOFT: deny risk-increasing", "HARD: deny everything",
                            "deny unless boot_id = run_id:epoch, halt:state", "&nbsp;&nbsp;present, event id = MAX(halt_events.id),",
                            "&nbsp;&nbsp;node epoch = engine_epoch, lease valid",
                            "any Redis / PostgreSQL error &rarr; deny", "risk-increasing: settled, reconciled",
                            "&nbsp;&nbsp;after the open, no stale_mark symbol,", "&nbsp;&nbsp;no UNKNOWN; RiskSweep pass &lt; 3 s old"]),
                 ox + 400, y1, 300, h1, S("class"), F)
        y2 = y1 + h1 + 60
        h2 = p.rowH([("hev", 70), ("egw", 70)])
        p.vertex("hev", cls("store", "audit", "halt_events", ["id, level, effective_level, hard_latch", "rule_id, reason, by, at_ms"]),
                 ox + 20, y2, 320, h2, S("store"), F)
        p.vertex("egw", cls("class", "Phase 2 &middot; LOCKED", "ExecutionGateway", (), ["+ send(order) &rarr; ActionGate.allow() first"]),
                 ox + 400, y2, 300, h2, kstyle("class", "align=left;spacingTop=2;dashed=1;opacity=70;fillColor=#F2F2F2;strokeColor=#B71C1C;"), F)
        y3 = y2 + h2 + 40
        h3 = p.H("n11", 60)
        note("n11", "<b>Circuit Breaker</b>: one switch, three positions; tripping it is cheap and idempotent. "
             "<b>Gatekeeper</b>: every outbound action asks ActionGate first, so a halt can't be bypassed. In Phase 1 "
             "nothing is sent; the gate is the contract Phase 2 must keep. <b>Fail-closed</b>: no readable halt state "
             "means no action.", ox + 20, y3, 680, h3, F)
        p.edge("f11a", "ag", "hs", "reads level", p.left("ag", y1 + h1 / 2), p.right("hs", y1 + h1 / 2), extra=ASSOC)
        p.edge("f11b", "hs", "hev", "audit", p.bot("hs"), p.top("hev"), extra=ASSOC)
        p.edge("f11c", "egw", "ag", "allow()?", p.top("egw"), p.bot("ag"), extra=DEP, color=RED)
        return y3 + h3

    def F12(ox, oy, F):
        rows = [
            ("P1", "Adapter + Strategy + Factory", "BrokerAdapter, AdapterFactory", "Swap brokers without touching bus or DB"),
            ("P1", "Retry + jitter, Heartbeat", "ReconnectPolicy", "Recover from drops, no thundering herd"),
            ("P2", "Protection Proxy", "ReadOnlyTransport, PreToolUse hook", "Order endpoints unreachable by construction"),
            ("P3", "Canonical Data Model + Value Object", "OrderDTO / PositionDTO / PriceDTO / AccountDTO / CashFlowDTO", "One schema downstream; safe sharing"),
            ("P4", "Publish&ndash;Subscribe + Bulkhead", "EventBus, 4 topics", "Decoupled; bursts isolated per topic"),
            ("P5", "Dead Letter Channel + Spooler + Idempotent Retry", "DlqSpooler, events_dlq, DlqRecoveryWorker",
             "Failed events parked without blocking; no duplicates"),
            ("P6", "Outbox + Unit of Work + Event Log", "OrderStateConsumer, PositionStateConsumer",
             "State and events commit atomically"),
            ("P7", "Chain of Responsibility + Memento", "Tier 1 &rarr; 2 (reads); Tier 3 Memento", "Redis &gt; DB; image only for an operator restore"),
            ("P7", "Cache-aside / write-through (intentional duplicate) + version CAS", "Tier1RedisCache, apply_state.lua",
             "Instant reads; disposable copy; heal without pausing"),
            ("P8", "State + Supervisor", "IntegrityWorker, AdapterConnection", "Explicit transitions; self-healing"),
            ("P9", "Observer of committed state + level-triggered sweep + Strategy rules + Registry", "Oversee, oversee:feed, RiskSweep, Rule, RuleRegistry",
             "Halt state recomputed every second; no timers to lose; add a rule without touching Oversee"),
            ("P10", "Strategy channels + Outbox", "NotificationDispatcher", "Reliable, de-duplicated email / push"),
            ("P11", "Circuit Breaker + Gatekeeper (fail-closed)", "HaltSwitch (CAS on halt_event_id), ActionGate",
             "One switch that stops every action; no state means no action"),
            ("P12", "Lease + fencing token (single writer)", "LeaseKeeper, engine_epoch, outbound_intent",
             "One writer; a paused old leader can't corrupt state"),
            ("P13", "Health-gated election + anti-flap brake", "HealthRecord, Campaigner, SAFE_HOLD",
             "No failover to a sick node; no failover storms"),
            ("P14", "Market calendar", "SessionSchedule, MarketSession, ReadinessCheck",
             "No false alarms while closed; ready at the open"),
        ]
        td = "style='border:1px solid #BBBBBB;padding:3px;vertical-align:top'"
        th = "style='border:1px solid #BBBBBB;padding:3px;background:#E6F2F8;text-align:left'"
        html = (f"<table style='border-collapse:collapse;width:100%;font-size:10px'><tr><th {th}>#</th>"
                f"<th {th}>Pattern</th><th {th}>Where</th><th {th}>Why</th></tr>")
        for r in rows:
            html += "<tr>" + "".join(f"<td {td}>{c}</td>" for c in r) + "</tr>"
        html += "</table>"
        h = p.H("cat", 330, pad=6)
        p.vertex("cat", html, ox + 20, oy + 46, 680, h, "text;html=1;whiteSpace=wrap;align=left;verticalAlign=top;fontSize=10;", F)
        return oy + 46 + h

    def F13(ox, oy, F):   # lease + fencing
        y1 = oy + 46
        h1 = p.rowH([("lk", 110), ("ep", 110)])
        p.vertex("lk", cls("active", "own thread / sidecar", "LeaseKeeper", ["- lease_deadline (monotonic)", "- TTL 15 s &middot; fence at 12 s"],
                           ["+ renew() every 1 s, next endpoint", "&nbsp;&nbsp;on error", "+ fence(): SIGKILL the engine"]), ox + 20, y1, 320, h1, S("active"), F,
                 tooltip="Runs off the event loop, so a stalled loop can't keep a dead lease alive.")
        p.vertex("ep", cls("store", "fencing token = lease revision", "engine_epoch", ["epoch = (etcd cluster_id, revision)"],
                           ["bump: newer revision only", "every write: token in the statement"]), ox + 400, y1, 300, h1, S("store"), F)
        y2 = y1 + h1 + 70
        h2 = p.rowH([("oi", 90), ("el", 90)])
        p.vertex("oi", cls("store", "Phase 2 contract", "outbound_intent", ["client_order_id (deterministic)",
                           "epoch &middot; SENDING | SENT | UNKNOWN"]), ox + 20, y2, 320, h2, S("store"), F)
        p.vertex("el", cls("active", "SM9", "EngineLeader", (), ["+ init(): epoch + term row &rarr; HLC &rarr; FLUSHDB", "&nbsp;&nbsp;&rarr; sod catch-up &rarr; SM10 init", "&nbsp;&nbsp;&rarr; SM4 &#8741; SM8 (boot_id last)"]),
                 ox + 400, y2, 300, h2, S("active"), F)
        y3 = y2 + h2 + 40
        h3 = p.H("n12", 60)
        note("n12", "<b>Fencing token</b>: every write path checks the epoch, so a paused old leader can't corrupt state, and "
             "the lease keeper kills the process before its lease can expire. Brokers can't check tokens, so Phase 2 sends "
             "are excluded only within lease timing: intent first, lease check with no await, gate settle after a takeover.",
             ox + 20, y3, 680, h3, F)
        p.edge("f13a", "lk", "ep", "epoch on win", p.right("lk"), p.left("ep", y1 + h1 / 2), extra=ASSOC)
        p.edge("f13b", "el", "ep", "bumps", p.top("el", ox + 650), p.bot("ep", ox + 650), extra=DEP)
        p.edge("f13c", "oi", "ep", "epoch-checked", p.top("oi", ox + 300), p.bot("ep", ox + 450),
               pts=[(ox + 300, y2 - 35), (ox + 450, y2 - 35)], extra=DEP)
        return y3 + h3

    def F14(ox, oy, F):   # health-gated election + anti-flap
        y1 = oy + 46
        h1 = p.rowH([("hr", 100), ("cp", 100)])
        p.vertex("hr", cls("value", "published under the node's lease", "HealthRecord",
                           ["<b>hard:</b> pg_writable, etcd_ok,", "&nbsp;&nbsp;redis_writable, chrony &le; 50 ms",
                            "<b>prefer:</b> broker_tls_ok (open markets),", "&nbsp;&nbsp;chrony &lt; 1 ms, hosts PG primary",
                            "<b>leader:</b> RiskSweep circuit &lt; 5 min", "<b>also:</b> spill size; stale after 5 s"]),
                 ox + 20, y1, 320, h1, S("value"), F)
        p.vertex("cp", cls("class", "SM9", "Campaigner", (), ["+ eligible(): hard checks, no back-off,", "&nbsp;&nbsp;no SAFE_HOLD (ignores preferences)",
                           "+ may_campaign(): eligible, lease free,", "&nbsp;&nbsp;and (prefers or free &ge; 15 s)",
                           "+ abdicate(): to an eligible peer;",
                           "&nbsp;&nbsp;hard check failing 30 s, or own net",
                           "&nbsp;&nbsp;path to an OPEN broker down 20 s"]), ox + 400, y1, 300, h1, S("class"), F)
        y2 = y1 + h1 + 70
        h2 = p.rowH([("lt", 80), ("sh", 80)])
        p.vertex("lt", cls("store", "audit", "leader_terms", ["epoch, node, started_at, reached_leader_at,", "ended_at, end_reason: switchover |", "&nbsp;&nbsp;abdicate | lost | init_failed",
                            "written at epoch bump, LEADER, DRAINING"]),
                 ox + 20, y2, 320, h2, S("store"), F)
        p.vertex("sh", cls("class", "Circuit Breaker", "SafeHold", ["3 abnormal ends / 15 min, or", "&gt; 6 epoch changes / 30 min", "durable row in cluster_state"], ["+ resume(operator)"]),
                 ox + 400, y2, 300, h2, S("class"), F)
        y3 = y2 + h2 + 40
        h3 = p.H("n13", 60)
        note("n13", "Winning an etcd election only proves the node can reach etcd. The <b>health gate</b> keeps a node that "
             "fails a hard check (PostgreSQL, etcd, Redis, clock) from becoming leader; broker reach is only a "
             "preference, judged for the leader by its own SM4 state, and the <b>anti-flap brake</b> turns a crash loop "
             "into one page instead of a stream of failovers.", ox + 20, y3, 680, h3, F)
        p.edge("f14a", "cp", "hr", "reads", p.left("cp", y1 + h1 / 2), p.right("hr", y1 + h1 / 2), extra=DEP)
        p.edge("f14b", "sh", "cp", "blocks", p.top("sh", ox + 550), p.bot("cp", ox + 550), extra=DEP)
        p.edge("f14c", "lt", "sh", "counts", p.right("lt", y2 + h2 / 2), p.left("sh", y2 + h2 / 2), extra=ASSOC)
        return y3 + h3

    def F15(ox, oy, F):   # market calendar
        y1 = oy + 46
        h1 = p.rowH([("ss", 100), ("ms", 100)])
        p.vertex("ss", cls("value", "from the brokers, versioned", "SessionSchedule",
                           ["per (adapter, symbol): open, close,", "&nbsp;&nbsp;daily break, holidays", "per adapter: maintenance windows"]), ox + 20, y1, 320, h1, S("value"), F)
        p.vertex("ms", cls("class", "State (SM10)", "MarketSession", ["key (adapter_id, symbol)", "CLOSED &middot; PRE_OPEN &middot; OPEN", "OPEN_BY_EVIDENCE &middot; ROLLOVER",
                            "BROKER_MAINTENANCE (per adapter)"],
                           ["+ on_evidence(tick | execution):", "&nbsp;&nbsp;CLOSED &rarr; OPEN_BY_EVIDENCE"]), ox + 400, y1, 300, h1, S("class"), F)
        y2 = y1 + h1 + 70
        h2 = p.rowH([("rc", 90), ("lcm", 90)])
        p.vertex("rc", cls("active", "PRE_OPEN", "ReadinessCheck", (), ["+ run(): reconnect, account + cash flows,",
                           "&nbsp;&nbsp;tokens &gt; 24 h, chrony, all STREAMING"]), ox + 20, y2, 320, h2, S("active"), F)
        p.vertex("lcm", cls("store", "survives a leader change", "last_clean_marks", ["adapter_id, symbol, bid, ask, ts",
                            "upsert &le; 10 s; forced at transitions"]), ox + 400, y2, 300, h2, S("store"), F)
        y3 = y2 + h2 + 40
        h3 = p.H("n14", 60)
        note("n14", "A closed market mutes only <b>price</b> staleness, and evidence (a tick or an event) overrides the "
             "calendar. The readiness check runs before each open; last clean marks persist, so marks and the start-of-day "
             "equity survive a leader change.", ox + 20, y3, 680, h3, F)
        p.edge("f15a", "ms", "ss", "reads", p.left("ms", y1 + h1 / 2), p.right("ss", y1 + h1 / 2), extra=DEP)
        p.edge("f15b", "rc", "ss", "open &minus; 10 min", p.top("rc", ox + 180), p.bot("ss", ox + 180), extra=DEP)
        p.edge("f15c", "lcm", "ms", "written at<br>transitions", p.top("lcm", ox + 550), p.bot("ms", ox + 550), extra=ASSOC)
        return y3 + h3

    grid = [[("F1", "P1 &middot; Adapter + Strategy + Factory", F1), ("F2", "P2 &middot; Protection Proxy (Execution Lockout)", F2),
             ("F3", "P3 &middot; Canonical Data Model + Value Object", F3)],
            [("F4", "P4 &middot; Publish&ndash;Subscribe (Observer) + Bulkhead", F4),
             ("F5", "P5 &middot; Dead Letter Channel + Idempotent Retry", F5),
             ("F6", "P6 &middot; Transactional Outbox + Unit of Work", F6)],
            [("F7", "P7 &middot; Chain of Responsibility + Redis duplicate + Memento", F7), ("F8", "P8 &middot; State + Supervisor", F8),
             ("F9", "P9 &middot; Oversee: Observer + Strategy rule plugins", F9)],
            [("F10", "P10 &middot; Notifications: Strategy channels + Outbox", F10),
             ("F11", "P11 &middot; Circuit Breaker (kill switch) + Gatekeeper", F11), ("F12", "Pattern catalogue", F12)],
            [("F13", "P12 &middot; Leader lease + fencing token (single writer)", F13),
             ("F14", "P13 &middot; Health-gated election + anti-flap brake", F14),
             ("F15", "P14 &middot; Market calendar (session schedule)", F15)]]
    oy = 100
    for row in grid:
        bottoms = []
        for (fid, label, fn), ox in zip(row, XS):
            red = fid in ("F2", "F11")
            p.container(fid, label, ox, oy, FW, 100, frame_style("#FBE3E3" if red else "#E6F2F8"))
            bottoms.append(fn(ox, oy, fid))
        rh = max(bottoms) - oy + 24
        for fid, _, _ in row:
            p.cont[fid][3] = rh
        oy += rh + GY
    lend = legend(p, 40, oy, 2240, ["interface", "abstract", "class", "active", "object", "value", "enum", "store",
                                    "external", "guard", "note"],
                  edges=[("realizes / implements", "#333333", REAL), ("association", "#333333", ASSOC),
                         ("dependency", "#333333", DEP), ("composition", "#333333", COMP)])
    p.w, p.h = 2320, lend + 40
    return p


# =================================================================== PAGE 3
def state_style(cat):
    fill, stroke = {"normal": ("#FFFFFF", "#4D4D4D"), "transient": ("#FFF2CC", "#B58B00"),
                    "success": ("#D5E8D4", "#2E7D32"), "failure": ("#F8CECC", "#B71C1C"),
                    "terminal": ("#EEEEEE", "#666666")}[cat]
    return ("rounded=1;arcSize=30;whiteSpace=wrap;html=1;fontSize=10;align=center;verticalAlign=middle;"
            f"fillColor={fill};strokeColor={stroke};strokeWidth=1.5;fontColor=#1A1A1A;")


INIT = "ellipse;html=1;fillColor=#1A1A1A;strokeColor=#1A1A1A;"
FINAL = "ellipse;html=1;shape=endState;fillColor=#1A1A1A;strokeColor=#1A1A1A;"


def sname(name, sub=""):
    return f"<b>{name}</b>" + (f"<br><font style='font-size:9px' color='#555555'>{sub}</font>" if sub else "")


def page_states():
    p = Page("sm", "3 · State Machines")
    p.vertex("title",
             "<font style='font-size:22px'><b>State Machines</b></font><br>"
             "<font style='font-size:12px' color='#555555'>Transitions use UML syntax <i>event [guard] / action</i>. "
             "State colour = state category (legend at bottom). Hover a state for detail. Phase 1 only "
             "<b>observes</b> order transitions; it never commands them.</font>",
             40, 16, 2220, 60, title_style())
    fs = lambda c: frame_style(c).replace("dashed=1;", "")
    FW1, FW2, GX = 1100, 1080, 40
    X1, X2 = 40, 40 + FW1 + GX

    tips = {
        "o_pend": "Created when the first event for a client_order_id arrives before the broker acknowledges it.",
        "o_work": "Order is resting at the broker; Oversee evaluates risk once this state is committed (oversee:feed). "
                  "SL / TP amendments keep the status and arrive with a higher version.",
        "o_pf": "0 &lt; remaining_units &lt; ordered_units. Filled units open or grow a PositionDTO (SM7). Each "
                "additional fill must carry a higher version.",
        "o_fill": "The order ends here. The first item with filled units &gt; 0 (partial or full) already opened the "
                  "PositionDTO; on StoneX every later ORDERS item for this OrderId is routed to SM7.",
        "o_rej": "Broker rejected the order; the reason is recorded in event_log.",
        "o_can": "Cancelled or expired at the broker. Terminal.",
        "d_q": "Row in events_dlq waiting for next_retry_at_ms and for its adapter to reach STREAMING.",
        "d_chk": "Asks the broker for the real state before deciding anything: order status for ORDER rows, the "
                 "position by id for POSITION rows, a fresh account snapshot for ACCOUNT rows, and fetch_cash_flows() "
                 "for CASH_FLOW rows, which converge only if the transaction_id is found (otherwise back off; after 5 "
                 "tries quarantine and mark the day transfers_unverified).",
        "d_bo": "Waits with exponential backoff + jitter before the next check.",
        "d_quar": "Poison event moved to events_quarantine for manual review.",
        "i_ccd": "Rows committed in [last check - MAX_TXN_MS, now - 500 ms] (commit time, so a long transaction "
                 "that commits late is still seen) are compared with Redis on version + checksum; Redis behind OR "
                 "ahead of PostgreSQL's current row is a mismatch. A rotating sweep of K keys per tick covers keys "
                 "that rarely change. The commutative XOR digest is compared only when no entity write (apply_state.lua) happened for "
                 "500 ms, so a live write can't make it differ. Each tick also checks halt:state's event id against "
                 "MAX(halt_events.id) and redis:boot_id against INFO run_id + epoch (a mismatch starts SM8).",
        "i_hyd": "Drifted keys are re-applied from PostgreSQL through apply_state.lua, the same script live writers use: "
                 "SET only if the stored version is lower (Redis ahead is resolved by DEL + re-apply). Ingestion and "
                 "consumers keep running; no symbol is paused, "
                 "so the bounded queues can't fill and spill valid events into the DLQ.",
        "i_halt": "All ingestion stops and a critical alert is raised. Primary-only damage leads to a switchover; "
                  "damage on both members leads to MANUAL_INTERVENTION.",
        "i_rest": "Only for damage confined to the primary: the sync standby is checked the same way first, then "
                  "Patroni switches over and the bad member is re-initialised. The engine never restores PostgreSQL "
                  "itself.",
        "i_rec": "Restored state is compared with the broker: open orders, positions and a fresh account snapshot. "
                 "Anything still open locally but absent from the broker lists is re-read by id, never closed on "
                 "absence. Prices resume first (sockets stay drained); after the broker reconcile is committed, Redis "
                 "is flushed and SM8 rebuilds it; ingestion resumes only after SM8 READY. "
                 "Until the reconcile commits, the RiskSweep caps every result at SOFT (FLAG stale_account_image), so "
                 "the restored, possibly stale image can't latch a HARD or clear a real state.",
        "a_snap": "REST snapshot back-fills anything missed while disconnected, including the account image. "
                  "Locally open orders and positions missing from the lists are re-read by id (all of them in a process's "
                  "first snapshot, which has no stream watermark yet).",
        "h_unk": "Start state, and re-entered whenever halt:state is missing or its event id differs from PostgreSQL. "
                 "Left only once SM8 is READY (redis:boot_id = run_id:epoch). "
                 "ActionGate denies everything here. Leaving it only restores halt:state: no new halt_events row and "
                 "no alert, so a restart doesn't re-announce an old halt.",
        "p_open": "Created only from a PositionDTO. Every sweep checks needs_stop() and effective_stop(); no effective "
                  "stop starts the durable grace clock effective_stop_missing_since_ms.",
        "p_closed": "The broker reports the position closed (status Closed or units 0). Terminal.",
        "b_det": "Either signal (apply_state.lua answered NO_BOOT, or the integrity tick saw redis:boot_id differ "
                 "from INFO run_id:epoch) asks the integrity worker; it starts only if SET redis:boot_lock NX PX succeeds, so "
                 "only one bootstrap runs.",
        "b_bar": "Seeded prices keep their original timestamps, so the sweep caps results at SOFT until live ticks "
                 "arrive; a seed never overwrites a fresher value. The wait is bounded: on timeout a missing price is a "
                 "real outage and is handled as one.",
        "b_rdy": "Written last, after the queued NO_BOOT writes, so a crash anywhere earlier simply restarts the "
                 "bootstrap and the gate never opens over an incomplete Redis. In place (no leader change) a fresh "
                 "fetch_account_snapshot() follows, so live fields stop being sampled.",
        "a_ref": "Only sandbox or live_readonly is accepted in Phase 1 (environment gate). live_readonly lets the "
                 "24/7 server watch the real accounts; order endpoints stay blocked by path and by interface.",
        "s_hold": "Cluster-wide brake, kept as a durable cluster_state row: after 3 abnormal term ends within 15 minutes, or more "
                  "than 6 epoch changes in 30 minutes, no node campaigns, the gate "
                  "stays closed and one off-node page goes out. Only an operator command resumes.",
        "s_init": "Strict order. The epoch commit comes first so a stale leader is fenced in PostgreSQL before this "
                  "node does anything else; SM4 runs in parallel with all SM8 steps (its writes queue as NO_BOOT; SM8 writes boot_id last). The lease "
                  "keeper enforces a 120 s deadline: past it the lease is revoked and the node hard-exits (init_failed); "
                  "reaching LEADER sets leader_terms.reached_leader_at.",
        "m_closed": "Only price staleness is muted here. Only symbol-specific evidence (a post-close tick with a new "
                    "price, or an execution on the symbol) overrides the calendar, into OPEN_BY_EVIDENCE.",
        "m_evid": "Opened by symbol-specific evidence against the calendar (FLAG calendar_mismatch). Returns to CLOSED "
                  "after 30 min without evidence; the scheduled PRE_OPEN runs from here too.",
        "m_maint": "A per-adapter overlay, entered from any session state (drawn once from OPEN): an expected "
                   "adapter outage with normal backoff, one pre-acknowledged notice and liveness alerts suppressed. "
                   "When the window ends each symbol returns to its scheduled state, through PRE_OPEN if an open "
                   "passed, and FENCE is at full severity again.",
        "a_af": "Credential errors are not network errors: at most 3 attempts, then stop and page (critical). An "
                "operator fixes the credential. The state is recorded per adapter and credential generation (Secrets "
                "Manager version id or token_generation) in PostgreSQL, so a new leader does not retry until it "
                "changes, and it never triggers an abdication. It is left for AUTHENTICATING only when the credential "
                "generation "
                "changes, or an operator re-arms the adapter (authenticated, audited).",
        "a_rsy": "Entered from STREAMING after a gap (frames dropped on overflow, or a subscription ERROR / "
                 "UNSUBSCRIBED frame while the socket is up) and at each market open (SM10, with jitter). A lost "
                 "subscription is re-subscribed first; the other subscriptions and their consumer tasks are untouched. "
                 "N failed re-subscribes or the deadline lead to BACKOFF.",
        "h_run": "No halt in force; FLAG verdicts are logged and alerted without blocking anything.",
        "h_soft": "Risk-increasing actions denied; closing or reducing risk still allowed.",
        "h_hard": "Kill switch: ActionGate denies every outbound action until an operator resets it. clear() must "
                  "release the HARD latch; it refuses while the latest sweep pass still shows an active HARD state, unless forced "
                  "(with a reason) for a HARD state whose latest results are capped or UNKNOWN.",
        "n_send": "One delivery attempt through the selected channel (email or mobile push).",
        "n_rw": "Waiting before the next attempt; attempts counted in notification_outbox.",
        "n_esc": "Delivery kept failing: the alert is sent on the other channel and shown on the dashboard.",
    }

    def S(vid, html, x, y, w, h, cat, par, tip=None):
        p.vertex(vid, html, x, y, w, h, state_style(cat), par, tooltip=tip or tips.get(vid))

    def pseudo(vid, x, y, final, par):
        p.vertex(vid, "", x, y, 24, 24, FINAL if final else INIT, par)

    def note(vid, html, x, y, w, par, default=60):
        h = p.H(vid, default)
        p.vertex(vid, tag("note") + html, x, y, w, h, kstyle("note"), par)
        return y + h

    def SM1(ox, oy, P):
        pseudo("o_init", ox + 20, oy + 148, False, P)
        S("o_pend", sname("PENDING", "first event seen, no broker ack"), ox + 90, oy + 130, 200, 60, "normal", P)
        S("o_work", sname("WORKING", "resting at broker"), ox + 390, oy + 130, 200, 60, "normal", P)
        S("o_pf", sname("PARTIALLY_FILLED", "0 &lt; remaining &lt; ordered"), ox + 680, oy + 130, 200, 60, "transient", P)
        S("o_fill", sname("FILLED", "order ends;<br>position &rarr; SM7"), ox + 950, oy + 130, 130, 60, "success", P)
        S("o_rej", sname("REJECTED", "terminal"), ox + 90, oy + 380, 200, 60, "failure", P)
        S("o_can", sname("CANCELLED", "terminal (incl. EXPIRED)"), ox + 520, oy + 380, 200, 60, "terminal", P)
        pseudo("o_f1", ox + 1003, oy + 260, True, P)
        pseudo("o_f2", ox + 178, oy + 490, True, P)
        pseudo("o_f3", ox + 608, oy + 490, True, P)
        e = p.edge
        e("o1", "o_init", "o_pend", "", p.right("o_init"), p.left("o_pend"))
        e("o2", "o_pend", "o_work", "NEW / ACCEPTED<br>(or implied<br>by a fill)", p.right("o_pend"), p.left("o_work"))
        e("o3", "o_work", "o_pf", "partial<br>fill", p.right("o_work"), p.left("o_pf"))
        e("o4", "o_pf", "o_fill", "FILLED /<br>EXECUTED", p.right("o_pf"), p.left("o_fill"))
        e("o5", "o_work", "o_fill", "FILLED / EXECUTED", p.top("o_work"), p.top("o_fill", ox + 980),
          pts=[(ox + 490, oy + 95), (ox + 980, oy + 95)])
        e("o6", "o_pend", "o_fill", "FILLED (immediate fill)", p.top("o_pend"), p.top("o_fill", ox + 1050),
          pts=[(ox + 190, oy + 55), (ox + 1050, oy + 55)])
        e("o7", "o_pf", "o_pf", "additional fill or<br>SL / TP amend<br>[version &gt; last]", p.bot("o_pf", ox + 810),
          p.bot("o_pf", ox + 860),
          pts=[(ox + 810, oy + 240), (ox + 860, oy + 240)])
        e("o14", "o_work", "o_work", "SL / TP amend<br>[version &gt; last]", p.bot("o_work", ox + 420), p.bot("o_work", ox + 470),
          pts=[(ox + 420, oy + 240), (ox + 470, oy + 240)])
        e("o8", "o_work", "o_can", "CANCELLED /<br>EXPIRED", p.bot("o_work", ox + 560), p.top("o_can", ox + 560))
        e("o9", "o_pf", "o_can", "remainder<br>CANCELLED / EXPIRED", p.bot("o_pf", ox + 700), p.top("o_can", ox + 700))
        e("o10", "o_pend", "o_rej", "REJECTED<br>/ reason &rarr; event_log", p.bot("o_pend"), p.top("o_rej"))
        e("o15", "o_pend", "o_can", "CANCELLED / EXPIRED<br>before ack", p.bot("o_pend", ox + 280), p.top("o_can", ox + 540),
          pts=[(ox + 280, oy + 350), (ox + 540, oy + 350)])
        e("o11", "o_fill", "o_f1", "", p.bot("o_fill", ox + 1015), p.top("o_f1"))
        e("o12", "o_rej", "o_f2", "", p.bot("o_rej"), p.top("o_f2"))
        e("o13", "o_can", "o_f3", "", p.bot("o_can"), p.top("o_f3"))
        return note("o_note", "<b>Guards on every transition:</b> [newer version] and [checksum valid]. Key = (broker_seq, "
                    "broker_updated_at_ms, observed_seq): at a tie the later observation wins (a stream frame's arrival, "
                    "or the send time of a REST read); the broker's own revision or time decides, so a slow REST "
                    "read can't undo a newer stop-loss. The same checksum is a no-op that only advances the stored version "
                    "(in PostgreSQL and Redis; no feed entry, no history row); seq_id breaks a tie only when the keys are "
                    "equal and the content differs. A <b>lower</b> version is dropped and logged in event_log, not "
                    "parked. <b>Violations</b> (illegal transition such as FILLED &rarr; WORKING, missing client_order_id, "
                    "future_timestamp) &rarr; events_dlq. A stop-loss or take-profit change with the same status and a "
                    "higher version is a legal <b>amend</b>. Every accepted transition is evaluated by Oversee after it "
                    "is committed. <b>Initial</b>: an order is created in whatever status it is first observed in "
                    "(PENDING from a stream; any status from a snapshot or resync), so a first observation is never a "
                    "violation. A partial fill seen while PENDING is applied as an implied ack (PENDING &rarr; WORKING "
                    "&rarr; PARTIALLY_FILLED). Opening fills open or grow the position (SM7); reducing fills (closing trades, "
                    "filled reduce-only children) reduce or close it and never open a new one.",
                    ox + 20, oy + 540, FW1 - 40, P)

    def SM2(ox, oy, P):
        pseudo("d_init", ox + 20, oy + 148, False, P)
        S("d_q", sname("QUEUED", "parked with error_reason"), ox + 70, oy + 130, 160, 60, "normal", P)
        S("d_chk", sname("CHECKING_BROKER", "entry / ORDER: fetch_order_status(id)<br>POSITION: fetch_position_status(id)"
                         "<br>ACCOUNT: fetch_account_snapshot()<br>CASH_FLOW: fetch_cash_flows()"),
          ox + 370, oy + 125, 230, 90, "transient", P)
        S("d_rep", sname("RESUBMITTED", "entry / publish the broker's fresh<br>row (parked payload dropped)"),
          ox + 710, oy + 125, 200, 70, "success", P)
        S("d_bo", sname("BACKOFF", "entry / retry_count += 1;<br>next_retry_at_ms = now + backoff"),
          ox + 370, oy + 370, 230, 70, "transient", P)
        S("d_quar", sname("QUARANTINED", "entry / move to events_quarantine"), ox + 710, oy + 375, 200, 60, "failure", P)
        pseudo("d_f1", ox + 980, oy + 148, True, P)
        pseudo("d_f2", ox + 980, oy + 393, True, P)
        e = lambda *a, **k: p.edge(*a, color=C_DLQ, **k)
        p.edge("d1", "d_init", "d_q", "", p.right("d_init"), p.left("d_q"))
        e("d2", "d_q", "d_chk", "[due and adapter<br>STREAMING] / lease", p.right("d_q"), p.left("d_chk", oy + 160))
        e("d3", "d_chk", "d_rep", "[converged, not<br>illegal]", p.right("d_chk", oy + 160), p.left("d_rep"))
        e("d4", "d_chk", "d_bo", "[STREAMING, not converged<br>or error, retry_count &lt; 5]", p.bot("d_chk", ox + 440),
          p.top("d_bo", ox + 440))
        e("d5", "d_bo", "d_q", "/ re-queue", p.left("d_bo"), p.bot("d_q", ox + 150), pts=[(ox + 150, oy + 405)])
        e("d9", "d_chk", "d_q", "[lease expired or adapter left STREAMING] / no retry++", p.top("d_chk", ox + 420),
          p.top("d_q", ox + 150), pts=[(ox + 420, oy + 95), (ox + 150, oy + 95)])
        e("d6", "d_chk", "d_quar", "[converged and illegal, or<br>failed and retry_count &ge; 5]", p.bot("d_chk", ox + 560), p.top("d_quar", ox + 810),
          pts=[(ox + 560, oy + 300), (ox + 810, oy + 300)])
        p.edge("d7", "d_rep", "d_f1", "", p.right("d_rep"), p.left("d_f1"))
        p.edge("d8", "d_quar", "d_f2", "", p.right("d_quar"), p.left("d_f2"))
        return note("d_note", "<b>Entry:</b> an order, position, account or cash-flow event is parked when unacked, "
                    "timed out, schema/checksum-invalid, QueueFull or future_timestamp (event_type ORDER | POSITION | "
                    "ACCOUNT | CASH_FLOW); a "
                    "stale (lower) version is never parked. QueueFull and invalid events "
                    "arrive through the DlqSpooler, so the publisher never waits. "
                    "<b>[due]</b> = now &ge; next_retry_at_ms. <b>Start-up barrier</b>: a row is leased only while its "
                    "adapter is STREAMING (SM4), so spill-file rows drained after an outage or at start-up can't burn retries "
                    "on an unauthenticated session. <b>[converged]</b> = the broker's row for that id (order status, "
                    "position by id, a fresh account snapshot, or the cash flow found by transaction_id) was read. That "
                    "fresh row is re-submitted with its own version (the higher version wins) and its lineage "
                    "(origin_dlq_id), and the parked payload is dropped; if it is parked again with the same deterministic "
                    "error (schema, checksum, future_timestamp, illegal transition), retry_count keeps counting, so a row "
                    "that is invalid every time is quarantined after 5; transient errors (QueueFull, timeout) start a "
                    "fresh count and page after 15 min. A converged "
                    "row whose error was an illegal transition is quarantined at once. Re-submission goes through the orchestrator, "
                    "which stays the sole publisher. "
                    "State names are proposed values for <i>events_dlq.status</i>.", ox + 20, oy + 540, FW2 - 40, P)

    def SM3(ox, oy, P):
        pseudo("i_init", ox + 20, oy + 153, False, P)
        S("i_idle", sname("IDLE"), ox + 90, oy + 135, 170, 60, "normal", P)
        S("i_ccd", sname("CHECK_REDIS_VS_DB", "Step 1 &middot; commit window + sweep;<br>halt id, boot_id"),
          ox + 360, oy + 130, 210, 70, "normal", P)
        S("i_cdi", sname("CHECK_DB_INTEGRITY", "Step 2 &middot; checksums + seq_id monotonic"), ox + 680, oy + 130, 210, 70, "normal", P)
        S("i_hyd", sname("HEALING_REDIS", "do / re-apply drifted keys via<br>apply_state.lua (version CAS);<br>"
                         "<b>no pause</b>, ingestion continues"), ox + 360, oy + 300, 210, 90, "transient", P)
        S("i_halt", sname("HALTED", "entry / halt ingestion; critical alert"), ox + 680, oy + 310, 210, 70, "failure", P)
        S("i_rest", sname("SWITCHING_OVER", "do / Patroni switchover to the<br>checked sync standby; reinit bad member"),
          ox + 680, oy + 470, 210, 70, "failure", P)
        S("i_rec", sname("RECONCILING", "do / prices resume; reconcile orders,<br>positions, account, cash flows<br>"
                         "(absent &rarr; by id); then FLUSHDB &rarr; SM8"),
          ox + 360, oy + 465, 210, 80, "transient", P)
        S("i_man", sname("MANUAL_<br>INTERVENTION", "page; operator Tier 3<br>restore runbook"), ox + 950, oy + 470, 130, 70, "terminal", P)
        g = C_INT
        p.edge("i1", "i_init", "i_idle", "", p.right("i_init"), p.left("i_idle"))
        p.edge("i2", "i_idle", "i_ccd", "tick<br>every 5 s", p.right("i_idle"), p.left("i_ccd", oy + 165), color=g)
        p.edge("i3", "i_ccd", "i_cdi", "[boot ok; match or<br>first mismatch]", p.right("i_ccd", oy + 150), p.left("i_cdi", oy + 150), color=g)
        p.edge("i13", "i_ccd", "i_idle", "[boot_id &ne; run_id:epoch] / signal SM8", p.bot("i_ccd", ox + 380),
               p.bot("i_idle", ox + 200), pts=[(ox + 380, oy + 240), (ox + 200, oy + 240)], color=RED)
        p.edge("i4", "i_ccd", "i_hyd", "[boot ok; mismatch<br>on 2 ticks]", p.bot("i_ccd"), p.top("i_hyd"), color=g)
        p.edge("i5", "i_hyd", "i_cdi", "healed", p.right("i_hyd"), p.left("i_cdi", oy + 185),
               pts=[(ox + 625, oy + 345), (ox + 625, oy + 185)], color=g)
        p.edge("i6", "i_cdi", "i_idle", "[checksums OK, no seq_id regression]", p.top("i_cdi"), p.top("i_idle"),
               pts=[(ox + 785, oy + 85), (ox + 175, oy + 85)], color=g)
        p.edge("i7", "i_cdi", "i_halt", "[corruption or<br>seq_id regression]", p.bot("i_cdi"), p.top("i_halt"), color=RED)
        p.edge("i8", "i_halt", "i_rest", "[primary-only damage,<br>standby passes]", p.bot("i_halt"), p.top("i_rest"), color=RED)
        p.edge("i15", "i_halt", "i_man", "[standby<br>damaged too]", p.right("i_halt"), p.top("i_man", ox + 1015),
               pts=[(ox + 1015, oy + 345)], color=RED)
        p.edge("i9", "i_rest", "i_rec", "promoted", p.left("i_rest"), p.right("i_rec"), color=g)
        p.edge("i10", "i_rest", "i_man", "[fails]", p.right("i_rest"), p.left("i_man"), color=RED,
               tooltip="The switchover fails: operator-run Tier 3 restore.")
        p.edge("i11", "i_rec", "i_idle", "[reconciled and<br>SM8 READY] / resume", p.left("i_rec"), p.bot("i_idle"),
               pts=[(ox + 175, oy + 505)], color=g)
        p.edge("i12", "i_rec", "i_man", "[divergence persists]", p.bot("i_rec"), p.bot("i_man"),
               pts=[(ox + 465, oy + 580), (ox + 1015, oy + 580)], color=RED)
        p.edge("i14", "i_man", "i_rec", "operator restore runbook done (Patroni pause, pgBackRest restore to an explicit "
               "target on the current timeline, reinit replica, resume)", p.bot("i_man", ox + 1060), p.bot("i_rec", ox + 400),
               pts=[(ox + 1060, oy + 600), (ox + 400, oy + 600)], color=g)
        return note("i_note", "Non-blocking asyncio task every INTEGRITY_CHECK_INTERVAL_SEC (5.0 s). Step 1 looks only at "
                    "rows committed in [last check &minus; MAX_TXN_MS, now &minus; 500 ms] plus a rotating sweep, so it "
                    "stays cheap and a commit still being copied is not a false mismatch; the full XOR digest is compared "
                    "only when writes are quiet. A missing or foreign redis:boot_id hands over to SM8. Redis is a "
                    "disposable duplicate: on mismatch the drifted keys are healed in place with the same version "
                    "compare-and-set script the live writers use, so neither side can overwrite newer state and nothing "
                    "is paused (a pause would fill the bounded queues and flood the DLQ). Heals also append to "
                    "oversee:feed, waking the RiskSweep. Step 2 (real DB "
                    "corruption) halts ingestion and pages. The engine never restores PostgreSQL itself: "
                    "primary-only damage is handled by a Patroni switchover to the checked sync standby; damage on both "
                    "members needs an operator-run Tier 3 restore through Patroni. Either way RECONCILING then re-reads "
                    "the broker, flushes Redis and waits for SM8 READY. The seq_id check is timeline- and epoch-aware.",
                    ox + 20, oy + 640, FW1 - 40, P)

    def SM4(ox, oy, P):
        pseudo("a_init", ox + 20, oy + 153, False, P)
        S("a_disc", sname("DISCONNECTED"), ox + 70, oy + 135, 170, 60, "normal", P)
        S("a_auth", sname("AUTHENTICATING", "entry / skip if known bad; StoneX<br>LogOn v2 or cTrader app + account auth"), ox + 370, oy + 130, 210, 70, "transient", P)
        S("a_snap", sname("SNAPSHOTTING", "entry / fetch_instruments() &rarr; adapter map;<br>fetch_open_orders(), "
                          "fetch_positions(),<br>fetch_account_snapshot(), fetch_cash_flows();<br>subscribe each stream not SUBSCRIBED"),
          ox + 710, oy + 125, 230, 100, "transient", P)
        S("a_str", sname("STREAMING", "do / always drain the socket;<br>ticks, orders, positions, account<br>"
                         "heartbeat per adapter; no inbound 30 s"),
          ox + 710, oy + 340, 230, 80, "success", P)
        S("a_bo", sname("BACKOFF", "entry / sleep min(60, 2<sup>n</sup> + U(0, 0.5)) s"), ox + 370, oy + 340, 210, 80, "transient", P)
        S("a_ref", sname("REFUSED", "lockout: unknown env"), ox + 70, oy + 340, 170, 60, "failure", P)
        pseudo("a_fin", ox + 143, oy + 425, True, P)
        S("a_af", sname("AUTH_FAILED", "credential error; page;<br>no retry loop"), ox + 70, oy + 470, 170, 60, "failure", P)
        o = C_CONN
        p.edge("a1", "a_init", "a_disc", "", p.right("a_init"), p.left("a_disc"))
        p.edge("a2", "a_disc", "a_auth", "start()<br>[env allowed]", p.right("a_disc"), p.left("a_auth", oy + 165), color=o,
               tooltip="BROKER_ENV is sandbox or live_readonly.")
        p.edge("a3", "a_disc", "a_ref", "start() [BROKER_ENV &notin; {sandbox,<br>live_readonly}] / raise ExecutionLockedError", p.bot("a_disc"), p.top("a_ref"), color=RED)
        p.edge("a4", "a_ref", "a_fin", "", p.bot("a_ref"), p.top("a_fin"))
        p.edge("a5", "a_auth", "a_snap", "authenticated", p.right("a_auth", oy + 160), p.left("a_snap", oy + 160), color=o)
        p.edge("a6", "a_snap", "a_str", "snapshot<br>published", p.bot("a_snap", ox + 760), p.top("a_str", ox + 760), color=o)
        p.edge("a15", "a_snap", "a_bo", "REST / socket /<br>subscribe error", p.bot("a_snap", ox + 725), p.top("a_bo", ox + 560),
               pts=[(ox + 725, oy + 300), (ox + 560, oy + 300)], color=o)
        p.edge("a16", "a_snap", "a_auth", "[auth lost]", p.left("a_snap", oy + 190), p.right("a_auth", oy + 190), color=o,
               tooltip="Session expired (StoneX) or AccountDisconnect / TokenInvalidated / ClientDisconnect (cTrader).")
        S("a_rsy", sname("RESYNCING", "do / re-subscribe lost streams; record<br>watermark; REST: orders, positions, "
                         "account,<br>cash flows; absent (version &le; watermark) &rarr; by id"),
          ox + 710, oy + 470, 230, 80, "transient", P)
        p.edge("a14", "a_rsy", "a_rsy", "[dropped again or REST error,<br>ring &lt; 50 %, before deadline] / retry",
               p.bot("a_rsy", ox + 800),
               p.bot("a_rsy", ox + 850), pts=[(ox + 800, oy + 580), (ox + 850, oy + 580)], color=o)
        p.edge("a11", "a_str", "a_rsy", "[gap, lost subscription<br>or SM10 open; ring &lt; 50 %]", p.bot("a_str", ox + 760), p.top("a_rsy", ox + 760),
               color=o, tooltip="After dropped frames, a subscription ERROR / UNSUBSCRIBED frame while the socket is up, "
                                "or the market open: re-subscribe only what is not SUBSCRIBED, then re-read state over "
                                "REST, without re-authenticating.")
        p.edge("a12", "a_rsy", "a_str", "reconciled [all SUBSCRIBED,<br>no drop after watermark]", p.top("a_rsy", ox + 890),
               p.bot("a_str", ox + 890), color=o)
        p.edge("a13", "a_rsy", "a_bo", "socket error, deadline or<br>N failed re-subscribes", p.left("a_rsy", oy + 505), p.bot("a_bo", ox + 475),
               pts=[(ox + 475, oy + 505)], color=o)
        p.edge("a7", "a_str", "a_bo", "heartbeat timeout /<br>socket error", p.left("a_str"), p.right("a_bo"), color=o)
        p.edge("a17", "a_auth", "a_af", "[credential error, 3rd<br>attempt, or known bad]", p.left("a_auth", oy + 190), p.right("a_af", oy + 500),
               pts=[(ox + 300, oy + 190), (ox + 300, oy + 500)], color=RED,
               tooltip="Bad password, PasswordChangeRequired, invalid_grant, or TokenInvalidated after a fresh token read; "
                       "or adapter_auth_state already records AUTH_FAILED for the current credential generation, so "
                       "no LogOn is attempted.")
        p.edge("a18", "a_af", "a_auth", "[re-armed]", p.bot("a_af", ox + 200), p.bot("a_auth", ox + 395),
               pts=[(ox + 200, oy + 560), (ox + 340, oy + 560), (ox + 340, oy + 215), (ox + 395, oy + 215)], color=o,
               tooltip="Secret version or token_generation changed, or an operator re-armed the adapter.")
        p.edge("a8", "a_auth", "a_bo", "net / timeout<br>error", p.bot("a_auth", ox + 430), p.top("a_bo", ox + 430), color=o)
        p.edge("a9", "a_bo", "a_auth", "timer /<br>attempt + 1", p.top("a_bo", ox + 520), p.bot("a_auth", ox + 520), color=o)
        p.edge("a10", "a_str", "a_auth", "auth lost (also from RESYNCING)",
               p.right("a_str"), p.top("a_auth"), tooltip="Drawn once from STREAMING; RESYNCING takes the same "
               "transition. Session expired (StoneX) or AccountDisconnect / TokenInvalidated / ClientDisconnect (cTrader).",
               pts=[(ox + 1000, oy + 380), (ox + 1000, oy + 90), (ox + 475, oy + 90)], color=o)
        return note("a_note", "Every reconnect passes through <b>SNAPSHOTTING</b>, which also refreshes the account image, so "
                    "Oversee never evaluates rules against a stale equity figure. The adapter's instrument map is filled "
                    "before any stream starts, and the FX crosses needed for risk conversion are subscribed with the "
                    "traded symbols. In STREAMING the socket is always drained (control frames are never stuck behind "
                    "ticks); each subscription keeps its own TTL health key alive only while SUBSCRIBED. After a gap (overflow, "
                    "or a subscription lost while the socket is up) and at each market open, <b>RESYNCING</b> first "
                    "re-subscribes only the streams that are not SUBSCRIBED, then re-reads orders, positions, the account "
                    "and cash flows over REST; SUBSCRIBED streams and their consumer tasks are untouched, so no duplicate "
                    "consumer is started. "
                    "Its REST rows are versioned against the stream watermark, so they never overwrite a newer stream "
                    "update; anything still open locally but absent from the lists is re-read by id, never closed on "
                    "absence; it loops with a new watermark if frames were dropped again, and returns to STREAMING only when "
                    "every stream is SUBSCRIBED and nothing was dropped after the watermark. While a gap is open, that "
                    "stream's conn: key is deleted and its stream_health.conn_lost_at is set in PostgreSQL, which Oversee's fence reads. Positions "
                    "share the orders health key. SNAPSHOTTING subscribes each stream that isn't SUBSCRIBED, so a "
                    "re-authenticated socket gets its subscriptions back, and any SNAPSHOTTING error leads to BACKOFF or "
                    "AUTHENTICATING. Credential errors go to AUTH_FAILED (page, no loop), left only when the credential generation "
                    "token_generation changes or an operator re-arms it. Only the leader node opens "
                    "sessions, after a lease check; the cTrader token pair is refreshed only by the leader and saved "
                    "before use. No state can reach an order-changing "
                    "endpoint or message (ReadOnlyTransport, P2).", ox + 20, oy + 620, FW2 - 40, P)

    def SM5(ox, oy, P):   # halt switch (fail-closed start state)
        pseudo("h_init", ox + 20, oy + 283, False, P)
        S("h_unk", sname("UNKNOWN (fail-closed)", "entry / ActionGate denies everything; rehydrate: halt:state &larr; latest "
                         "halt_events row through the CAS script (no new row, no alert); exits only when SM8 is READY"),
          ox + 90, oy + 265, 990, 60, "failure", P)
        S("h_run", sname("RUNNING", "halt:state = NONE"), ox + 120, oy + 140, 200, 60, "success", P)
        S("h_soft", sname("SOFT_HALTED", "risk-increasing actions denied;<br>risk-reducing allowed"), ox + 450, oy + 135, 220, 70, "transient", P)
        S("h_hard", sname("HARD_HALTED", "all outbound actions denied"), ox + 820, oy + 135, 220, 70, "failure", P)
        c = C_OVR
        p.edge("h1", "h_init", "h_unk", "", p.right("h_init"), p.left("h_unk", oy + 295))
        p.edge("hu1", "h_unk", "h_run", "[latest NONE]", p.top("h_unk", ox + 290), p.bot("h_run", ox + 290), color=c)
        p.edge("hu2", "h_unk", "h_soft", "[latest SOFT]", p.top("h_unk", ox + 470), p.bot("h_soft", ox + 470), color=c)
        p.edge("hx", "h_soft", "h_unk", "[halt:state lost]<br>(from any state)", p.bot("h_soft", ox + 560),
               p.top("h_unk", ox + 560), color=RED, extra="dashed=1;",
               tooltip="Drawn once for readability: from RUNNING, SOFT_HALTED or HARD_HALTED, a missing halt:state or "
                       "an event id that differs from MAX(halt_events.id) re-enters UNKNOWN.")
        p.edge("hu3", "h_unk", "h_hard", "[latest HARD]", p.top("h_unk", ox + 930), p.bot("h_hard", ox + 930), color=RED)
        p.edge("h2", "h_run", "h_soft", "[SOFT_HALT]<br>/ trip, alert", p.right("h_run", oy + 165), p.left("h_soft", oy + 165), color=c)
        p.edge("h3", "h_soft", "h_hard", "[HARD_HALT] or<br>escalate_after<br>/ trip(HARD)", p.right("h_soft", oy + 170),
               p.left("h_hard", oy + 170), color=RED,
               tooltip="The RiskSweep trips HARD when a SOFT state's accumulated uncapped time (uncapped_ms) reaches "
                       "escalate_after_ms; capped spells and FLAG lowerings pause the clock, never reset it. Capped "
                       "states never escalate.")
        p.edge("h8", "h_hard", "h_soft", "clear() [latest pass:<br>SOFT active]", p.bot("h_hard", ox + 870),
               p.bot("h_soft", ox + 620), pts=[(ox + 870, oy + 240), (ox + 620, oy + 240)], color=c)
        p.edge("h7", "h_hard", "h_run", "clear() [latest pass: none active] / log who + why",
               p.top("h_hard", ox + 1010), p.top("h_run", ox + 150), pts=[(ox + 1010, oy + 55), (ox + 150, oy + 55)], color=c)
        p.edge("h4", "h_run", "h_hard", "[HARD_HALT] / trip, alert (high priority)", p.top("h_run", ox + 190),
               p.top("h_hard", ox + 960), pts=[(ox + 190, oy + 80), (ox + 960, oy + 80)], color=RED)
        p.edge("h6", "h_soft", "h_run", "[no SOFT state for cool_down] / system clear()", p.top("h_soft", ox + 600),
               p.top("h_run", ox + 260), pts=[(ox + 600, oy + 105), (ox + 260, oy + 105)], color=c)
        p.edge("h5", "h_run", "h_run", "FLAG / log + alert", p.bot("h_run", ox + 140), p.bot("h_run", ox + 180),
               pts=[(ox + 140, oy + 232), (ox + 180, oy + 232)], color=c)
        return note("h_note", "<b>Entry into SOFT or HARD</b> by trip() writes a halt_events row and alerts (SM6), only "
                    "when the level rises; trip() and clear() share a PostgreSQL advisory lock. The level is set only by "
                    "the <b>RiskSweep</b> after each complete pass (every second): HARD if any rule state is HARD or the "
                    "HARD latch is set; else SOFT if a SOFT state is active; else NONE after cool_down with none active. A "
                    "state is active once seen for its rule's confirm_ms and released only after 5 s below a 10 % margin; a "
                    "stale or estimated input caps it at SOFT, and HARD needs uncapped inputs. A SOFT state whose "
                    "accumulated uncapped time reaches T trips HARD and sets the latch; capped states only re-page. A "
                    "failed pass changes nothing. Operator clear() releases the latch and is refused while the latest "
                    "pass shows an active HARD state (unless forced for a capped one); acknowledge() only silences "
                    "re-pages. <b>UNKNOWN</b> is the start state, re-entered from any state when halt:state is "
                    "missing or its halt_event_id differs from MAX(halt_events.id); it is left only once SM8 is READY "
                    "(redis:boot_id = run_id:epoch), and leaving it only restores halt:state (written only by the CAS "
                    "script). In Phase 1 nothing is sent, so the level is recorded and alerted for Phase 2 to enforce.",
                    ox + 20, oy + 360, FW1 - 40, P)

    def SM6(ox, oy, P):   # notification delivery
        pseudo("n_init", ox + 20, oy + 148, False, P)
        S("n_q", sname("QUEUED", "row in notification_outbox"), ox + 70, oy + 130, 160, 60, "normal", P)
        S("n_send", sname("SENDING", "entry / channel.send(alert)"), ox + 330, oy + 125, 200, 70, "transient", P)
        S("n_del", sname("DELIVERED", "provider accepted"), ox + 650, oy + 130, 200, 60, "success", P)
        S("n_rw", sname("RETRY_WAIT", "entry / backoff min(60, 2<sup>n</sup>) s"), ox + 330, oy + 330, 200, 70, "transient", P)
        S("n_esc", sname("ESCALATED", "entry / try the other channel;<br>dashboard alert"), ox + 650, oy + 330, 200, 70, "failure", P)
        pseudo("n_f1", ox + 960, oy + 148, True, P)
        pseudo("n_f2", ox + 960, oy + 353, True, P)
        c = C_OVR
        p.edge("n1", "n_init", "n_q", "", p.right("n_init"), p.left("n_q"))
        p.edge("n2", "n_q", "n_send", "[not a<br>duplicate]", p.right("n_q"), p.left("n_send", oy + 160), color=c)
        p.edge("n3", "n_send", "n_del", "[accepted]", p.right("n_send", oy + 160), p.left("n_del"), color=c)
        p.edge("n4", "n_send", "n_rw", "[error and<br>attempts &lt; 5]", p.bot("n_send", ox + 390), p.top("n_rw", ox + 390), color=c)
        p.edge("n5", "n_rw", "n_send", "timer /<br>attempts + 1", p.top("n_rw", ox + 470), p.bot("n_send", ox + 470), color=c)
        pseudo("n_f3", ox + 138, oy + 250, True, P)
        p.edge("n9", "n_q", "n_f3", "[duplicate]<br>/ suppressed", p.bot("n_q", ox + 150), p.top("n_f3"), color=c)
        p.edge("n6", "n_send", "n_esc", "[error and<br>attempts &ge; 5]", p.bot("n_send", ox + 515), p.top("n_esc"),
               pts=[(ox + 515, oy + 275), (ox + 750, oy + 275)], color=RED)
        p.edge("n7", "n_del", "n_f1", "", p.right("n_del"), p.left("n_f1"))
        p.edge("n8", "n_esc", "n_f2", "", p.right("n_esc"), p.left("n_f2"))
        return note("n_note", "<b>Dedupe</b>: one alert per state episode (when a state activates or rises), then a re-page every "
                    "escalate_after_ms unless acknowledged, so a flapping rule can't spam. Only the level-raising trip() alert skips dedupe (and goes to every "
                    "channel).  Delivery is "
                    "at-least-once (outbox), so a phone may occasionally see a repeat.", ox + 20, oy + 440, FW2 - 40, P)

    def SM7(ox, oy, P):   # position lifecycle
        pseudo("p_init", ox + 20, oy + 148, False, P)
        S("p_open", sname("OPEN", "units &gt; 0; stop / take-profit may change"), ox + 90, oy + 125, 260, 70, "normal", P)
        S("p_closed", sname("CLOSED", "status Closed or units = 0"), ox + 620, oy + 130, 220, 60, "terminal", P)
        pseudo("p_fin", ox + 920, oy + 148, True, P)
        p.edge("p1", "p_init", "p_open", "", p.right("p_init"), p.left("p_open", oy + 160))
        p.edge("p2", "p_open", "p_closed", "closed<br>[version &gt; last]", p.right("p_open", oy + 160), p.left("p_closed"))
        p.edge("p3", "p_closed", "p_fin", "", p.right("p_closed"), p.left("p_fin"))
        p.edge("p4", "p_open", "p_open", "SL / TP moved, partial close [version &gt; last]", p.top("p_open", ox + 150),
               p.top("p_open", ox + 290), pts=[(ox + 150, oy + 90), (ox + 290, oy + 90)])
        p.edge("p5", "p_open", "p_open", "absent from a snapshot / re-read by id", p.bot("p_open", ox + 150),
               p.bot("p_open", ox + 290), pts=[(ox + 150, oy + 235), (ox + 290, oy + 235)])
        return note("p_note", "Created only from a <b>PositionDTO</b>: a cTrader execution event, or, on StoneX, the first "
                    "ORDERS item that reports filled units &gt; 0 (position_id = OrderId; later items for that OrderId "
                    "update both the order and the position until the order is terminal, then only the position). "
                    "Reducing fills reduce or close a position; they never open one. "
                    "The RiskSweep checks needs_stop() and "
                    "effective_stop() every second: an OPEN position with no effective stop gets "
                    "effective_stop_missing_since_ms and, after the grace, a SOFT_HALT state; cancelling a linked StoneX "
                    "stop child wakes the sweep for the account, so a lost stop is seen at once. A position is never closed just because a snapshot "
                    "doesn't list it: it is re-read by id (fetch_position_status).", ox + 20, oy + 290, FW1 - 40, P)

    def SM8(ox, oy, P):   # bootstrap after Redis data loss
        pseudo("b_init", ox + 20, oy + 143, False, P)
        W8 = 180
        row1 = [("b_det", "DETECTED", "boot_lock taken (NX); gate<br>denies all until READY", "failure"),
                ("b_halt", "RESTORE_HALT", "halt:state &larr; latest<br>halt_events (CAS)", "transient"),
                ("b_grp", "CREATE_GROUP", "XGROUP CREATE oversee:feed<br>0 MKSTREAM (BUSYGROUP = ok)", "transient"),
                ("b_bar", "PRICE_BARRIER", "seed price: / clean_mid: / account_live:<br>where absent or older; wait (bounded)", "transient")]
        row2 = [("b_seed", "HEAL", "re-apply every key from PostgreSQL<br>(lock holder only; XADDs wake the sweep)",
                 "transient"),
                ("b_heal", "REPLAY_NO_BOOT", "re-apply the queued NO_BOOT<br>writes (lock holder)", "transient"),
                ("b_rdy", "READY", "write redis:boot_id last; release<br>lock; snapshot if in place", "success")]
        xs = [70, 310, 550, 790]
        for (vid, nm, sub, cat), x in zip(row1, xs):
            S(vid, sname(nm, sub), ox + x, oy + 120, W8, 70, cat, P)
        for (vid, nm, sub, cat), x in zip(row2, [790, 550, 310]):
            S(vid, sname(nm, sub), ox + x, oy + 260, W8, 70, cat, P)
        pseudo("b_fin", ox + 388, oy + 380, True, P)
        c = C_OVR
        p.edge("b0", "b_init", "b_det", "", p.right("b_init"), p.left("b_det", oy + 155))
        for a, b in (("b_det", "b_halt"), ("b_halt", "b_grp"), ("b_grp", "b_bar")):
            p.edge(f"b_{a}", a, b, "", p.right(a), p.left(b), color=c)
        p.edge("b_b_bar", "b_bar", "b_seed", "[prices live<br>or timeout]", p.bot("b_bar"), p.top("b_seed"), color=c)
        p.edge("b_b_seed", "b_seed", "b_heal", "", p.left("b_seed"), p.right("b_heal"), color=c)
        p.edge("b_b_heal", "b_heal", "b_rdy", "", p.left("b_heal"), p.right("b_rdy"), color=c)
        p.edge("b_b_rdy", "b_rdy", "b_fin", "", p.bot("b_rdy"), p.top("b_fin"), color=c)
        return note("b_note", "Run single-flight by the <b>integrity worker</b> (src/integrity) under redis:boot_lock when "
                    "Redis has lost or replaced its data; NO_BOOT elsewhere only signals it. The <b>order</b> matters: the "
                    "halt level is restored first; the consumer group exists before the feed is refilled; seeded prices "
                    "never overwrite fresher ones; queued NO_BOOT writes are re-applied before boot_id, which is written "
                    "last, so a crash at any step simply starts the bootstrap again. ActionGate denies everything until "
                    "READY and the feed observer waits, while the RiskSweep keeps running from PostgreSQL. A failed step "
                    "retries; the gate stays closed.", ox + 20, oy + 430, FW2 - 40, P)

    def SM9(ox, oy, P):   # engine node role
        pseudo("s_ini", ox + 20, oy + 148, False, P)
        S("s_start", sname("STARTING", "chrony synced,<br>PG reachable"), ox + 90, oy + 130, 150, 60, "normal", P)
        S("s_stby", sname("STANDBY", "health record<br>published"), ox + 320, oy + 130, 180, 60, "normal", P)
        S("s_cand", sname("CANDIDATE", "campaign for<br>the etcd lease"), ox + 580, oy + 130, 180, 60, "transient", P)
        S("s_init", sname("LEADER_INIT", "epoch + term row &rarr; HLC &rarr; FLUSHDB<br>&rarr; sod catch-up &rarr; SM10 init &rarr;<br>"
                          "SM4 &#8741; SM8 (boot_id last) &rarr; DLQ barrier"),
          ox + 840, oy + 120, 240, 90, "transient", P)
        S("s_lead", sname("LEADER", "serving; lease renewed<br>by the keeper thread"), ox + 840, oy + 330, 240, 70, "success", P)
        S("s_drain", sname("DRAINING", "stop intake, flush spooler,<br>close sockets, release lease"), ox + 580, oy + 330, 180, 60, "transient", P,
          tip="10 s deadline enforced by the lease keeper; the spooler goes to the local spill file if PostgreSQL "
              "doesn't answer within 2 s; writes this term's end_reason.")
        S("s_hold", sname("SAFE_HOLD", "cluster-wide; gate<br>closed; one page"), ox + 320, oy + 330, 180, 60, "failure", P)
        S("s_fence", sname("FENCED", "process killed<br>(SIGKILL, RST)"), ox + 90, oy + 330, 150, 60, "failure", P)
        c = C_CONN
        p.edge("s1", "s_ini", "s_start", "", p.right("s_ini"), p.left("s_start", oy + 160))
        p.edge("s2", "s_start", "s_stby", "ready", p.right("s_start", oy + 160), p.left("s_stby", oy + 160), color=c)
        p.edge("s3", "s_stby", "s_cand", "[healthy and<br>lease free]", p.right("s_stby", oy + 150), p.left("s_cand", oy + 150), color=c,
               tooltip="may_campaign(): eligible (hard checks pass, no back-off, no SAFE_HOLD), the lease is free, and "
                       "(preferences pass or the lease has been free for 15 s). Abdication targets only need eligible().")
        p.edge("s4", "s_cand", "s_stby", "lost the election", p.top("s_cand", ox + 680), p.top("s_stby", ox + 420),
               pts=[(ox + 680, oy + 95), (ox + 420, oy + 95)], color=c)
        p.edge("s5", "s_cand", "s_init", "won /<br>epoch", p.right("s_cand", oy + 160), p.left("s_init", oy + 160), color=c)
        p.edge("s6", "s_init", "s_lead", "READY", p.bot("s_init", ox + 960), p.top("s_lead", ox + 960), color=c)
        p.edge("s7", "s_lead", "s_drain", "switchover /<br>abdicate", p.left("s_lead", oy + 360), p.right("s_drain", oy + 360), color=c)
        p.edge("s8", "s_drain", "s_stby", "lease released<br>/ 60 s back-off", p.top("s_drain", ox + 600), p.bot("s_stby", ox + 490),
               pts=[(ox + 600, oy + 280), (ox + 490, oy + 280)], color=c)
        p.edge("s9", "s_stby", "s_hold", "[SAFE_HOLD set]", p.bot("s_stby", ox + 360), p.top("s_hold", ox + 360), color=RED)
        p.edge("s10", "s_hold", "s_stby", "resume", p.top("s_hold", ox + 450), p.bot("s_stby", ox + 450), color=c)
        p.edge("s11", "s_lead", "s_fence", "lease not renewed by 12 s / hard exit",
               p.bot("s_lead", ox + 960), p.bot("s_fence", ox + 165), pts=[(ox + 960, oy + 440), (ox + 165, oy + 440)], color=RED,
               label_x=-0.5)
        p.edge("s13", "s_init", "s_fence", "LEADER_INIT fails, passes 120 s or finds SAFE_HOLD due / hard exit",
               p.right("s_init", oy + 180), p.bot("s_fence", ox + 125),
               pts=[(ox + 1095, oy + 180), (ox + 1095, oy + 455), (ox + 125, oy + 455)], color=RED, label_x=0.4,
               tooltip="Finding SAFE_HOLD due in step 1 sets it in the epoch-bump transaction before the exit; on restart "
                       "STANDBY sees it and enters SAFE_HOLD.")
        p.edge("s12", "s_fence", "s_start", "systemd restart<br>(back-off if own<br>term ended)", p.top("s_fence", ox + 165), p.bot("s_start", ox + 165), color=c)
        return note("s_note", "One engine LEADER at a time. Its epoch = (etcd cluster_id, lease revision), committed to "
                    "PostgreSQL engine_epoch before anything else; every engine write checks it in the same statement, so a stale "
                    "leader's writes fail. The lease keeper runs off the event loop: watchdog 3 s &lt; fence 12 s &lt; "
                    "lease TTL 15 s, and the fence is a hard kill, so sockets are reset rather than flushed. A node "
                    "campaigns only while it passes the hard checks (PostgreSQL writable, etcd reachable, Redis "
                    "writable, chrony within 50 ms); broker reach and a tight clock offset are preferences, and once the "
                    "lease has been free for 15 s any node passing the hard checks campaigns. The leader judges brokers by its "
                    "own SM4 state; it abdicates only to an eligible peer (hard checks, no back-off, no SAFE_HOLD; preferences ignored), after a hard check has failed for 30 s "
                    "or its own network path to an OPEN broker has failed while the peer's works. AUTH_FAILED is recorded per adapter and credential generation in "
                    "PostgreSQL and is never a health input. <b>[flapping]</b> = 3 abnormal term ends (lost, init_failed) in 15 min, or more than 6 epoch changes in 30 min, from the leader_terms rows each epoch bump writes; SAFE_HOLD is a durable cluster_state row: no node campaigns until "
                    "an operator resumes.", ox + 20, oy + 480, FW1 - 40, P)

    def SM10(ox, oy, P):   # market session per symbol
        pseudo("m_ini", ox + 20, oy + 60, False, P)
        p.vertex("m_choice", "", ox + 100, oy + 57, 30, 30, "rhombus;html=1;fillColor=#FFFFFF;strokeColor=#1A1A1A;", P,
                 tooltip="Computed at LEADER_INIT from the versioned schedule (plus any maintenance overlay), so a "
                         "mid-week takeover starts in the scheduled state, never blindly in CLOSED.")
        S("m_closed", sname("CLOSED", "weekend / holiday"), ox + 70, oy + 130, 180, 60, "terminal", P)
        S("m_pre", sname("PRE_OPEN", "readiness check<br>(open &minus; 10 min)"), ox + 330, oy + 125, 190, 70, "transient", P)
        S("m_open", sname("OPEN", "normal rules"), ox + 600, oy + 130, 260, 60, "success", P)
        S("m_roll", sname("ROLLOVER", "daily break; price<br>staleness muted"), ox + 700, oy + 330, 200, 60, "transient", P,
          tip="At the 17:00 New York roll the RiskSweep's first pass writes each account's new sod_marks row from the "
              "pre-roll last_clean_marks forced at this transition.")
        S("m_maint", sname("BROKER_MAINTENANCE", "expected adapter outage"), ox + 330, oy + 330, 190, 60, "transient", P)
        S("m_evid", sname("OPEN_BY_EVIDENCE", "FLAG calendar_mismatch"), ox + 70, oy + 330, 180, 60, "transient", P)
        c = C_CONN
        p.edge("m1", "m_ini", "m_choice", "", p.right("m_ini"), p.left("m_choice"))
        p.edge("m1c", "m_choice", "m_closed", "[closed]", p.bot("m_choice"), p.top("m_closed", ox + 115), color=c)
        p.edge("m1o", "m_choice", "m_open", "[open]", p.right("m_choice"), p.top("m_open", ox + 830),
               pts=[(ox + 830, oy + 72)], color=c)
        p.edge("m1r", "m_choice", "m_roll", "[daily break]", p.top("m_choice"), p.right("m_roll", oy + 360),
               pts=[(ox + 115, oy + 50), (ox + 960, oy + 50), (ox + 960, oy + 360)], color=c)
        p.edge("m2", "m_closed", "m_pre", "[now &ge; open<br>&minus; 10 min]", p.right("m_closed", oy + 160), p.left("m_pre", oy + 160), color=c)
        p.edge("m3", "m_pre", "m_open", "[now &ge; open]<br>/ resync", p.right("m_pre", oy + 160), p.left("m_open", oy + 160), color=c)
        p.edge("m4", "m_open", "m_roll", "[daily<br>break]", p.bot("m_open", ox + 740), p.top("m_roll", ox + 740), color=c)
        p.edge("m5", "m_roll", "m_open", "[ends] /<br>readiness", p.top("m_roll", ox + 820), p.bot("m_open", ox + 820), color=c)
        p.edge("m6", "m_open", "m_closed", "[weekly close or holiday]", p.top("m_open", ox + 700), p.top("m_closed", ox + 160),
               pts=[(ox + 700, oy + 95), (ox + 160, oy + 95)], color=c)
        p.edge("m9", "m_closed", "m_evid", "[evidence]", p.bot("m_closed", ox + 100), p.top("m_evid", ox + 100), color=c,
               tooltip="A post-close tick on that symbol with a new price, or an execution on it.")
        p.edge("m10", "m_evid", "m_closed", "[30 min quiet]", p.top("m_evid", ox + 185), p.bot("m_closed", ox + 185), color=c)
        p.edge("m11", "m_evid", "m_pre", "[open &minus; 10 min]", p.right("m_evid", oy + 345), p.left("m_pre", oy + 180),
               pts=[(ox + 290, oy + 345), (ox + 290, oy + 180)], color=c)
        p.edge("m7", "m_open", "m_maint", "[maintenance window]<br>(from any state)", p.bot("m_open", ox + 650), p.right("m_maint", oy + 360),
               pts=[(ox + 650, oy + 360)], color=c)
        p.edge("m8", "m_maint", "m_open", "[window ends] / scheduled state<br>(PRE_OPEN if an open passed)", p.top("m_maint", ox + 480),
               p.bot("m_open", ox + 610), pts=[(ox + 480, oy + 250), (ox + 610, oy + 250)], color=c)
        return note("m_note", "Per <b>(adapter, symbol)</b>, from broker schedules (cTrader symbol schedule and holidays; "
                    "StoneX market information or a reviewed config), versioned in PostgreSQL; BROKER_MAINTENANCE is per "
                    "adapter, and an adapter's market is OPEN when any of its symbols is. <b>Evidence beats the "
                    "calendar</b> only when it is symbol-specific (all guards are level conditions, so a takeover just before an open still runs PRE_OPEN): a tick after the scheduled close with a price that "
                    "differs from the last pre-close quote, or an execution on that symbol (snapshots and account or "
                    "cash-flow events don't count). It leads to OPEN_BY_EVIDENCE, which returns to CLOSED after 30 min "
                    "without evidence; a scheduled-OPEN symbol with no ticks pages for stale prices instead. While CLOSED "
                    "or in ROLLOVER only <b>price</b> staleness is muted. <b>PRE_OPEN</b>: reset backoff and reconnect, "
                    "fresh account snapshot + fetch_cash_flows, tokens valid &gt; 24 h, chrony synced; page if any adapter "
                    "is not STREAMING. At the open, STREAMING adapters run RESYNCING with jitter (open + 30&ndash;60 s); "
                    "others go through SNAPSHOTTING. Phase 2: no risk-increasing action until the account is reconciled "
                    "after the latest open.", ox + 20, oy + 430, FW2 - 40, P)

    rows = [[("S1", "SM1 &middot; OrderDTO.status lifecycle (validator / projection)", X1, FW1, "#DAE8FC", SM1),
             ("S2", "SM2 &middot; events_dlq row lifecycle (check-before-retry)", X2, FW2, "#E1D5E7", SM2)],
            [("S3", "SM3 &middot; Integrity Reconciliation Worker (Supervisor)", X1, FW1, "#D5E8D4", SM3),
             ("S4", "SM4 &middot; Broker adapter stream connection", X2, FW2, "#FFE6CC", SM4)],
            [("S5", "SM5 &middot; Oversee HaltSwitch (kill switch)", X1, FW1, "#FCE4EC", SM5),
             ("S6", "SM6 &middot; Alert delivery (email / mobile push)", X2, FW2, "#FCE4EC", SM6)],
            [("S7", "SM7 &middot; PositionDTO lifecycle (open trade after a fill)", X1, FW1, "#DAE8FC", SM7),
             ("S8", "SM8 &middot; Bootstrap after Redis data loss (fail-closed)", X2, FW2, "#FCE4EC", SM8)],
            [("S9", "SM9 &middot; Engine node role (lease, fencing, failover)", X1, FW1, "#D5E8D4", SM9),
             ("S10", "SM10 &middot; Market session per (adapter, symbol) (calendar)", X2, FW2, "#FFE6CC", SM10)]]
    oy = 100
    for row in rows:
        bottoms = []
        for fid, label, x, w, col, fn in row:
            p.container(fid, label, x, oy, w, 100, fs(col))
            bottoms.append(fn(x, oy, fid))
        rh = max(bottoms) - oy + 20
        for fid, *_ in row:
            p.cont[fid][3] = rh
        oy += rh + 40
    p.container("legend", "Legend &mdash; state categories", 40, oy, 2220, 80,
                "swimlane;startSize=26;html=1;fontSize=12;fontStyle=1;collapsible=0;fillColor=#F5F5F5;"
                "strokeColor=#999999;swimlaneFillColor=#FFFFFF;align=left;spacingLeft=10;", inset=(0, 26))
    for i, (c, txt) in enumerate([("normal", "normal / waiting"), ("transient", "transient / working"),
                                  ("success", "success / healthy"), ("failure", "failure / blocked"),
                                  ("terminal", "neutral terminal")]):
        p.vertex(f"lg_{c}", f"<b>{txt}</b>", 60 + i * 230, oy + 36, 200, 34, state_style(c), "legend")
    tx = "text;html=1;fontSize=10;align=left;verticalAlign=middle;"
    p.vertex("lg_init", "", 60 + 5 * 230, oy + 41, 24, 24, INIT, "legend")
    p.vertex("lg_init_t", "initial", 90 + 5 * 230, oy + 41, 60, 24, tx, "legend")
    p.vertex("lg_fin", "", 160 + 5 * 230, oy + 41, 24, 24, FINAL, "legend")
    p.vertex("lg_fin_t", "final", 190 + 5 * 230, oy + 41, 60, 24, tx, "legend")
    p.w, p.h = 2300, oy + 120
    return p


# =================================================================== PAGE 4
def uc_style(cat):
    fill, stroke, extra = {"base": ("#DAE8FC", "#6C8EBF", ""), "general": ("#E1D5E7", "#9673A6", "fontStyle=2;"),
                           "include": ("#F2F2F2", "#7A7A7A", ""), "extend": ("#FFF2CC", "#B58B00", ""),
                           "blocked": ("#F8CECC", "#B71C1C", "dashed=1;")}[cat]
    return (f"ellipse;whiteSpace=wrap;html=1;fontSize=10;fillColor={fill};strokeColor={stroke};strokeWidth=1.5;"
            f"fontColor=#1A1A1A;spacing=8;{extra}")


def actor_style(system):
    c = "#7A7A7A" if system else "#2B5797"
    return (f"shape=umlActor;verticalLabelPosition=bottom;verticalAlign=top;html=1;outlineConnect=0;fontSize=10;"
            f"fillColor={'#F2F2F2' if system else '#DAE8FC'};strokeColor={c};strokeWidth=1.5;")


INC = "dashed=1;endArrow=open;endFill=0;"
GEN = "endArrow=block;endFill=0;endSize=12;"
LINK = "endArrow=none;"


def page_usecases():
    p = Page("uc", "4 · Use Cases")
    p.vertex("title",
             "<font style='font-size:22px'><b>Use Case Diagram</b></font><br>"
             "<font style='font-size:12px' color='#555555'>Who interacts with the Phase 1 engine. Colour = use-case "
             "role (legend at bottom). Hover a use case for trigger, flow and outcome.</font>",
             40, 16, 2220, 60, title_style())
    BX, BY, BW = 360, 90, 1500
    p.container("sys", "XTNL Phase 1 &mdash; Read-Only Ingestion Engine + Oversee (system boundary)", BX, BY, BW, 1300, (
        "swimlane;startSize=30;html=1;fontSize=14;fontStyle=1;collapsible=0;rounded=0;fillColor=#E6F2F8;"
        f"strokeColor={PAT};swimlaneFillColor=#FFFFFF;strokeWidth=2;fontColor=#1A1A1A;"))
    band = "text;html=1;align=right;verticalAlign=middle;fontSize=12;fontStyle=3;fontColor=" + PAT + ";"
    ux = lambda c: [BX + 120, BX + 580, BX + 1040][c]
    UW = 300

    def uc(uid, text, col, y, cat, tip, h=64):
        p.vertex(uid, text, ux(col), y, UW, h, uc_style(cat), "sys", tooltip=tip)

    y0 = BY + 45
    p.vertex("b1", "Ingestion", BX + BW - 290, y0, 270, 20, band, "sys")
    uc("u_auth", "Authenticate broker session", 0, y0 + 30, "include", "StoneX LogOn v2 / cTrader ProtoOA application + account auth (OAuth token pair, refreshed only by the leader).")
    uc("u_norm", "Normalize payload to canonical DTO", 1, y0 + 30, "include", "Order / Position / Price / Account DTOs with checksum; the orchestrator labels the version and stamps seq_id.")
    uc("u_pub", "Publish to isolated topic (non-blocking)", 2, y0 + 30, "include", "put_nowait() into one of four topics.")
    uc("u_ing", "<b>Ingest broker data</b>", 1, y0 + 150, "general", "Abstract parent of the five ingestion use cases.")
    uc("u_price", "Ingest price ticks", 0, y0 + 270, "base", "Streams -> PriceDTO -> Redis price:{adapter_id}:{symbol}.")
    uc("u_ord", "Ingest order updates", 1, y0 + 270, "base",
       "Streams -> OrderDTO -> PostgreSQL then Redis. A stale (lower-version) update is dropped and logged.")
    uc("u_snap", "Snapshot orders, positions &amp; instruments", 2, y0 + 270, "base",
       "REST snapshot at start-up and after reconnect, incl. InstrumentSpec to convert broker quantities to canonical "
       "units, and the FX crosses to subscribe.")
    uc("u_dlq", "Route failed event to DLQ", 1, y0 + 390, "extend",
       "Order, position or account event: nack / timeout / invalid / QueueFull / future_timestamp.")
    uc("u_pos", "Ingest open positions (stop, take-profit, units)", 0, y0 + 390, "base",
       "cTrader execution events; StoneX: the first ORDERS item with filled units > 0 (and later items for that "
       "OrderId) -> PositionDTO -> positions_state then Redis.")
    uc("u_acct", "Track account image (balance, equity, margin)", 2, y0 + 390, "base",
       "ClientAccountMargin stream + REST -> AccountDTO -> account_state (history: account_snapshots) then Redis "
       "account:{id}; deposits and "
       "withdrawals as CashFlowDTO -> cash_flows.")

    y1 = y0 + 500
    p.vertex("b2", "Recovery &amp; Integrity", BX + BW - 290, y1, 270, 20, band, "sys")
    uc("u_rec", "Recover DLQ event (check-before-retry)", 0, y1 + 30, "base",
       "SM2 on page 3. Only for adapters already STREAMING (start-up barrier).")
    uc("u_qry", "Query broker order / position / account", 1, y1 + 30, "include",
       "ORDER: fetch_order_status(). POSITION: fetch_position_status(). ACCOUNT: fetch_account_snapshot(). "
       "CASH_FLOW: fetch_cash_flows().")
    uc("u_quar", "Quarantine poison event", 0, y1 + 140, "extend", "After 5 failed retries.")
    uc("u_ver", "Verify tiers (Redis &gt; DB &gt; Image)", 0, y1 + 250, "base", "SM3 on page 3.")
    uc("u_hyd", "Heal or rebuild Redis from DB", 1, y1 + 200, "extend",
       "Step 1 mismatch: version compare-and-set via apply_state.lua; ingestion keeps running (no symbol pause). "
       "Redis data lost: SM8 bootstrap rebuilds it while ActionGate fails closed.")
    uc("u_rest", "Switch over or operator restore", 1, y1 + 300, "extend",
       "Step 2 corruption: Patroni switchover to the checked standby, or an operator-run Tier 3 restore.")
    uc("u_recon", "Reconcile with broker", 2, y1 + 300, "include",
       "After a switchover or restore: fetch_open_orders(), fetch_positions(), fetch_account_snapshot(), fetch_cash_flows() "
       "since the last committed broker_ts (or the restore target); anything open locally but "
       "absent is re-read by id.")

    y2 = y1 + 410
    p.vertex("b3", "Oversight &amp; Alerts (Oversee)", BX + BW - 290, y2, 270, 20, band, "sys")
    uc("u_img", "Read snapshot + prices", 0, y2 + 30, "include", "The RiskSweep reads entity state from one PostgreSQL "
       "REPEATABLE READ snapshot per account (current rows, sod_marks, cash_flows) and prices / live fields from Redis "
       "with their timestamps.")
    uc("u_eval", "<b>Evaluate risk rules (RiskSweep)</b>", 0, y2 + 190, "base",
       "Every second, and at once when a committed OrderDTO / PositionDTO / AccountDTO or cash flow wakes it through "
       "oversee:feed, the RiskSweep evaluates every rule on every account and live entity. Deadlines (stop grace, "
       "confirm, escalation, cool-down) are durable timestamps. Orders and open positions are both checked; risk is "
       "directional and converted to the account currency. The most severe level wins.")
    uc("u_flag", "Flag violation (log)", 1, y2 + 110, "extend",
       "Rule state FLAG, e.g. risk between 1.0 % (1R) and 1.5 % of equity, or a stale account image: halt_breaches + oversee_events row + one alert per episode.")
    uc("u_soft", "Soft-halt risk-increasing actions", 1, y2 + 190, "extend",
       "Rule state SOFT_HALT, e.g. risk > 1.5 % of equity, no stop-loss after the grace window, or a capped SOFT from stale or unknown inputs; escalates to HARD after T of uncapped time.")
    uc("u_hard", "Hard-halt all actions (kill switch)", 1, y2 + 270, "extend", "Rule state HARD_HALT or escalation: sets the HARD latch; only an operator clear() releases it, refused while the condition is still HARD.")
    uc("u_notify", "Notify via email / mobile push", 2, y2 + 190, "include", "NotificationDispatcher; SM6 on page 3.")
    uc("u_cfg", "Configure rule plugins &amp; thresholds", 0, y2 + 300, "base", "Enable rules; set the FLAG / halt bands (1.0 % / 1.5 %), grace_ms and on_breach.")
    uc("u_ack", "Acknowledge / clear halt", 0, y2 + 380, "base", "acknowledge(operator, why, breach_id) silences re-pages of a state; clear(operator, why, reviewed_event_id, force?) releases the HARD latch and is refused while the latest sweep pass still shows an active HARD state, unless forced with a reason for a HARD state whose results are capped or UNKNOWN (it is then resolved); all logged.")

    y3 = y2 + 480
    p.vertex("b4", "Observation &amp; Assurance", BX + BW - 290, y3, 270, 20, band, "sys")
    uc("u_mon", "Monitor orders, positions &amp; account", 0, y3 + 30, "base", "Read-only views: entity data from PostgreSQL; live Redis values only when their epoch matches the leader heartbeat, else marked stale.")
    uc("u_rev", "Review quarantined events &amp; flags", 0, y3 + 110, "base", "events_quarantine + oversee_events.")
    uc("u_aud", "Audit event log, halts (114-trade baseline)", 0, y3 + 190, "base", "Append-only history + halt_events.")
    uc("u_rep", "Run deterministic replay &amp; rule tests", 1, y3 + 150, "base", "verify_execution.py + pytest with the mock broker.")

    y4 = y3 + 290
    p.vertex("b5", "Operations (24/7)", BX + BW - 290, y4, 270, 20, band, "sys")
    uc("u_sw", "Planned switchover / rolling upgrade", 0, y4 + 30, "base",
       "Upgrade the standby, switch over to it (named target), upgrade the old leader. Migrations run first, by an operator.")
    uc("u_safe", "Release SAFE_HOLD after flapping", 0, y4 + 130, "base",
       "After 3 abnormal term ends in 15 min no node campaigns; an operator investigates and resumes.")
    uc("u_fo", "Fail over to the standby node", 2, y4 + 30, "base",
       "Lease expires or the leader abdicates; the standby wins, bumps the epoch and runs LEADER_INIT (SM9).")
    uc("u_fence", "Fence the old leader", 1, y4 + 80, "include",
       "Lease keeper hard-kills a leader that can't renew; every PostgreSQL write checks the epoch.")
    uc("u_rdy", "Market-open readiness check", 2, y4 + 130, "base",
       "PRE_OPEN (SM10): reconnect, fresh account and cash flows, tokens, chrony; page if any adapter isn't STREAMING.")
    sys_h = y4 + 130 + 64 + 30 - BY
    p.cont["sys"][3] = sys_h

    AXL, AXR = 160, BX + BW + 140
    p.vertex("x_sched", "<b>Scheduler</b><br>«system» timer", AXL, y1 + 160, 40, 70, actor_style(True))
    p.vertex("x_risk", "<b>Risk Manager</b>", AXL, y2 + 297, 40, 70, actor_style(False))
    p.vertex("x_op", "<b>Operator / Trader</b>", AXL, y3 + 50, 40, 70, actor_style(False))
    p.vertex("x_comp", "<b>Compliance Auditor</b>", AXL, y3 + 170, 40, 70, actor_style(False))
    p.vertex("x_dev", "<b>Developer / CI</b>", AXR, y3 + 150, 40, 70, actor_style(False))
    p.vertex("x_oncall", "<b>On-call operator</b>", AXL, y4 + 60, 40, 70, actor_style(False))
    p.vertex("x_clu", "<b>Cluster manager</b> «system»<br>etcd lease &middot; Patroni", AXR, y4 + 30, 40, 70, actor_style(True))
    p.vertex("x_cal", "<b>Scheduler</b> «system»<br>market calendar", AXR, y4 + 150, 40, 70, actor_style(True))
    p.vertex("x_broker1", "<b>Broker API</b> «system»<br>StoneX &middot; Pepperstone &middot; Mock", AXR, y0 + 267, 40, 70, actor_style(True))
    p.vertex("x_broker2", "<b>Broker API</b> «system»<br>(read-only queries)", AXR, y1 + 120, 40, 70, actor_style(True))
    p.vertex("x_email", "<b>Email service</b> «system»", AXR, y2 + 110, 40, 70, actor_style(True))
    p.vertex("x_push", "<b>Mobile push</b> «system»", AXR, y2 + 260, 40, 70, actor_style(True))
    by = BY + sys_h + 40
    p.vertex("u_block", "<b>Place / modify / cancel order</b><br><i>&laquo;blocked in Phase 1&raquo;</i>",
             BX + 1040, by, UW, 70, uc_style("blocked"), tooltip="Denied by the Execution Lockout Guard (P2).")
    p.vertex("x_agent", "<b>Any caller</b><br>operator &middot; AI agent &middot; code", AXR, by, 40, 70, actor_style(False))
    p.vertex("n_block", tag("note") + "Outside the boundary on purpose: no Phase 1 use case can change broker state. "
             "Oversee halts are recorded now and enforced by ActionGate once Phase 2 adds sending.",
             BX + 120, by, 760, 70, kstyle("note"))

    inc, ext = "&laquo;include&raquo;", "&laquo;extend&raquo;"
    E = lambda *a, **k: p.edge(*a, straight=True, **k)
    E("c1", "u_ing", "u_auth", inc, extra=INC)
    E("c2", "u_ing", "u_norm", inc, extra=INC)
    E("c3", "u_ing", "u_pub", inc, extra=INC)
    for i, ch in enumerate(("u_price", "u_ord", "u_snap", "u_acct", "u_pos")):
        E(f"c4{i}", ch, "u_ing", "", extra=GEN)
    E("c7", "u_dlq", "u_ord", ext + " [nack / timeout / invalid]", extra=INC)
    E("c7b", "u_dlq", "u_acct", ext, extra=INC)
    E("c7c", "u_dlq", "u_pos", ext, extra=INC)
    E("c8", "u_rec", "u_qry", inc, extra=INC)
    E("c9", "u_quar", "u_rec", ext + " [retries &ge; 5]", extra=INC)
    E("c10", "u_hyd", "u_ver", ext + "<br>[Step 1 mismatch]", extra=INC)
    E("c11", "u_rest", "u_ver", ext + "<br>[Step 2 corruption]", extra=INC)
    E("c12", "u_rest", "u_recon", inc, extra=INC)
    E("o1", "u_eval", "u_img", inc, extra=INC)
    E("o2", "u_flag", "u_eval", ext + " [FLAG]", extra=INC)
    E("o3", "u_soft", "u_eval", ext + " [SOFT]", extra=INC)
    E("o4", "u_hard", "u_eval", ext + " [HARD]", extra=INC)
    E("o5", "u_flag", "u_notify", inc, extra=INC)
    E("o6", "u_soft", "u_notify", inc, extra=INC)
    E("o7", "u_hard", "u_notify", inc, extra=INC)
    E("a1", "x_broker1", "u_snap", "", extra=LINK)
    E("a1b", "x_broker1", "u_acct", "", extra=LINK)
    E("a2", "x_broker2", "u_qry", "", extra=LINK)
    E("a3", "x_broker2", "u_recon", "", extra=LINK)
    E("a4", "x_sched", "u_rec", "", extra=LINK)
    E("a5", "x_sched", "u_ver", "", extra=LINK)
    E("a6", "x_op", "u_mon", "", extra=LINK)
    E("a7", "x_op", "u_rev", "", extra=LINK)
    E("a7b", "x_op", "u_ack", "", extra=LINK)
    E("a8", "x_comp", "u_rev", "", extra=LINK)
    E("a9", "x_comp", "u_aud", "", extra=LINK)
    E("a10", "x_dev", "u_rep", "", extra=LINK)
    E("a12", "x_risk", "u_cfg", "", extra=LINK)
    E("a13", "x_email", "u_notify", "", extra=LINK)
    E("a14", "x_push", "u_notify", "", extra=LINK)
    E("a15", "x_oncall", "u_sw", "", extra=LINK)
    E("a16", "x_oncall", "u_safe", "", extra=LINK)
    E("a17", "x_clu", "u_fo", "", extra=LINK)
    E("a18", "x_cal", "u_rdy", "", extra=LINK)
    E("c13", "u_fo", "u_fence", inc, extra=INC)
    E("a11", "x_agent", "u_block", "DENIED", color=RED, extra="dashed=1;endArrow=cross;endSize=10;")

    ly = by + 110
    p.container("legend", "Legend &mdash; use-case roles and actors", 40, ly, 2220, 86,
                "swimlane;startSize=26;html=1;fontSize=12;fontStyle=1;collapsible=0;fillColor=#F5F5F5;"
                "strokeColor=#999999;swimlaneFillColor=#FFFFFF;align=left;spacingLeft=10;", inset=(0, 26))
    for i, (c, txt) in enumerate([("base", "use case (primary goal)"), ("general", "abstract parent (generalised)"),
                                  ("include", "included step"), ("extend", "extension (conditional)"),
                                  ("blocked", "blocked in Phase 1")]):
        p.vertex(f"lg_{c}", txt, 60 + i * 260, ly + 34, 230, 40, uc_style(c), "legend")
    tx = "text;html=1;fontSize=10;align=left;verticalAlign=middle;"
    p.vertex("lg_h", "", 60 + 5 * 260, ly + 32, 22, 44, actor_style(False), "legend", check=False)
    p.vertex("lg_ht", "person", 90 + 5 * 260, ly + 40, 70, 24, tx, "legend")
    p.vertex("lg_s", "", 170 + 5 * 260, ly + 32, 22, 44, actor_style(True), "legend", check=False)
    p.vertex("lg_st", "«system» actor", 200 + 5 * 260, ly + 40, 100, 24, tx, "legend")
    p.w, p.h = 2300, ly + 130
    return p


# =================================================================== PAGE 5
def page_deployment():
    p = Page("dep", "5 · Deployment")
    p.vertex("title",
             "<font style='font-size:22px'><b>Deployment &mdash; 24/7, single writer, two zones</b></font><br>"
             "<font style='font-size:12px' color='#555555'>Two engine nodes in London (one LEADER, one hot STANDBY) "
             "plus a witness, so a node or zone can fail without a split brain; disaster image, dashboard, dead-man's "
             "switch and on-call in Melbourne. Broker server locations and round-trip times are estimates: confirm "
             "with each broker and measure before go-live. Tag colour = element kind (legend at bottom).</font>",
             40, 16, 2220, 60, title_style())
    Y0 = 130
    ext_style = ("swimlane;startSize=30;html=1;fontSize=13;fontStyle=1;collapsible=0;rounded=0;fillColor=#F2F2F2;"
                 "strokeColor=#7A7A7A;swimlaneFillColor=#FFFFFF;strokeWidth=1.5;fontColor=#1A1A1A;align=left;spacingLeft=10;")
    lon_style = ("swimlane;startSize=30;html=1;fontSize=14;fontStyle=1;collapsible=0;rounded=0;fillColor=#DAE8FC;"
                 "strokeColor=#2B5797;swimlaneFillColor=#FCFCFC;strokeWidth=2;fontColor=#1A1A1A;align=left;spacingLeft=10;")
    mel_style = lon_style.replace("#DAE8FC", "#D5E8D4").replace("#2B5797", "#2E7D32")
    az_style = ("rounded=1;arcSize=2;html=1;fillColor=none;strokeColor=#2B5797;dashed=1;strokeWidth=1.5;align=left;"
                "verticalAlign=top;spacingLeft=8;spacingTop=4;fontSize=11;fontColor=#2B5797;")

    EX, EW = 40, 250
    LX, LW = 320, 1400
    MX, MW = 1760, 500
    AX, AW = 340, 430          # AZ a node column
    BX_, BW_ = 800, 430        # AZ b node column
    CX_, CW2 = 1260, 200       # AZ c witness column
    OX, OW = 1490, 210         # off-node alerting (inside London, outside the AZs)
    G = 20

    yB = Y0 + 70                                   # broker row
    hBk = p.rowH([("stonex", 80), ("pepper", 80)])
    yAZ = yB + hBk + 40
    yE = yAZ + 35
    hEng = p.rowH([("engA", 160), ("engB", 160)])
    yK = yE + hEng + G
    hK = p.rowH([("keepA", 70), ("keepB", 70)])
    yT = yK + hK + G
    hT = p.rowH([("etcdA", 60), ("etcdB", 60), ("etcdC", 60)])
    yR = yT + hT + G
    hR = p.rowH([("redisA", 70), ("redisB", 70)])
    yP = yR + hR + G
    hP = p.rowH([("pgA", 90), ("pgB", 90)])
    az_bottom = yP + hP + 15
    y_r = az_bottom + 30
    hS3 = p.H("s3", 100)
    H_all = (y_r + hS3 / 2 + 25) - Y0

    p.container("EXT", "External services", EX, Y0, EW, H_all, ext_style)
    p.container("LON", "London &mdash; AWS eu-west-2 (live engine, two zones + witness)", LX, Y0, LW, H_all, lon_style,
                tooltip="Everything on the hot path lives here, a few milliseconds from the brokers' FX servers.")
    p.container("MEL", "Melbourne &mdash; disaster copy, oversight, people", MX, Y0, MW, H_all, mel_style)

    for vid, lbl, x, w in (("azA", "<b>AZ eu-west-2a</b> &middot; node A (c7i.2xlarge-class)", AX - 10, AW + 20),
                           ("azB", "<b>AZ eu-west-2b</b> &middot; node B (same spec)", BX_ - 10, BW_ + 20),
                           ("azC", "<b>AZ eu-west-2c</b> &middot; witness", CX_ - 10, CW2 + 20)):
        p.vertex(vid, lbl, x, yAZ, w, az_bottom - yAZ, az_style, "LON",
                 tooltip="Each engine node: 8 vCPUs = 4 physical cores, 16 GB ECC, EBS volumes, Ubuntu 24.04 LTS; "
                         "socket readers pinned to a whole physical core. The witness is a small non-burstable "
                         "instance that only runs etcd." if vid != "azC" else
                         "Third etcd member so either engine node can lose its zone and the other still has quorum.",
                 check=False)

    p.vertex("stonex", tag("external", "external system") + "<b>StoneX FX servers</b><br>London area (confirm) &middot; "
             "est. RTT 1&ndash;3 ms", AX, yB, 200, hBk, kstyle("external"), "LON")
    p.vertex("pepper", tag("external", "external system") + "<b>Pepperstone cTrader</b><br>London area (confirm) "
             "&middot; est. RTT 1&ndash;3 ms", AX + 220, yB, 180, hBk, kstyle("external"), "LON")

    p.vertex("engA", tag("active", "LEADER (holds the lease)") + "<b>Engine</b>: orchestrator + adapters (core 0), "
             "consumers, Oversee, integrity worker, DLQ recovery, notification dispatcher<br>the only node with broker "
             "sessions; every PostgreSQL transaction checks its epoch; pushes a heartbeat to Melbourne every 15 s",
             AX, yE, AW, hEng, kstyle("active", "strokeWidth=2.5;"), "LON")
    p.vertex("engB", tag("active", "STANDBY") + "<b>Engine</b> (same build, idle)<br>no broker sessions, no Oversee; "
             "publishes its health record; campaigns only when healthy, the lease is free, it is not in back-off and "
             "the cluster is not in SAFE_HOLD (SM9)",
             BX_, yE, BW_, hEng, kstyle("active"), "LON")
    for vid, x in (("keepA", AX), ("keepB", BX_)):
        p.vertex(vid, tag("active", "own thread / sidecar") + "<b>Lease keeper</b><br>etcd lease TTL 15 s, renewed every "
                 "1 s; hard-exits the engine (SIGKILL) at 12 s without renewal; systemd watchdog 3 s",
                 x, yK, AW if vid == "keepA" else BW_, hK, kstyle("active"), "LON")
    for vid, x, w in (("etcdA", AX, AW), ("etcdB", BX_, BW_), ("etcdC", CX_, CW2)):
        p.vertex(vid, tag("store", "etcd member") + "<b>etcd</b> (own gp3 volume)<br>auto-compaction 1 h, rolling defrag",
                 x, yT, w, hT, kstyle("store"), "LON")
    p.vertex("redisA", tag("store", "core 1") + "<b>Redis 7</b> (leader's copy)<br>FLUSHDB at every LEADER_INIT; "
             "boot_id = run_id:epoch", AX, yR, AW, hR, kstyle("store"), "LON")
    p.vertex("redisB", tag("store", "idle") + "<b>Redis 7</b> (unused while STANDBY)<br>flushed on entering STANDBY; rebuilt by SM8 if "
             "this node becomes leader", BX_, yR, BW_, hR, kstyle("store"), "LON")
    p.vertex("pgA", tag("store", "Patroni primary") + "<b>PostgreSQL 17</b> (EBS io2 / gp3)<br>failsafe_mode on &middot; "
             "synchronous_mode &middot; primary_start_timeout 0 &middot; ttl 15 s, loop_wait 5 s &middot; wal_sender_timeout 10 s &middot; archive_mode=always &middot; the engine "
             "campaign prefers the node hosting the primary; alert if apart &gt; 10 min",
             AX, yP, AW, hP, kstyle("store", "strokeWidth=2;"), "LON")
    p.vertex("pgB", tag("store", "synchronous standby") + "<b>PostgreSQL 17</b><br>sync replica; degrades to async with "
             "a critical alert if lost &middot; archive_mode=always", BX_, yP, BW_, hP, kstyle("store"), "LON")
    hAl = p.H("alerting", 140)
    p.vertex("alerting", tag("external", "managed, off-node") + "<b>Off-node alerting</b><br>CloudWatch / managed "
             "Prometheus + Alertmanager &rarr; SNS<br>no LEADER &gt; 30 s, two LEADERs, etcd / Patroni unhealthy, sync "
             "standby lost, standby not ready, epoch-change rate, non-empty spill file on any node; epoch change inhibits dependent alerts "
             "for RTO + 30 s",
             OX, yE, OW, hAl, kstyle("external"), "LON")

    hT_ = p.H("timesrc", 80)
    p.vertex("timesrc", tag("external", "external system") + "<b>Time source</b><br>Amazon Time Sync (leap smear) "
             "&rarr; chrony on every node; engine starts only after sync", EX + 15, yB, EW - 30, hT_,
             kstyle("external"), "EXT")
    y_prov = yE + hEng / 2
    hPv = p.H("providers", 80)
    p.vertex("providers", tag("external", "external system") + "<b>Alert providers</b><br>Email: SES / SMTP<br>"
             "Push: FCM / APNs", EX + 15, y_prov - hPv / 2, EW - 30, hPv, kstyle("external"), "EXT")

    hD = p.H("dash", 150)
    p.vertex("dash", tag("class", "read-only") + "<b>Operator dashboard</b> + <b>dead-man's switch</b><br>read-only "
             "over a WireGuard VPN (entity views from PostgreSQL; Redis values only for the current epoch); receives the leader's push heartbeat every 15 s and pages, through a channel "
             "outside eu-west-2 (SNS in ap-southeast-4 or a third-party pager), after 60 s of silence, when any adapter "
             "is not STREAMING for 2 min while OPEN, or when a payload age passes its threshold (oldest un-acked feed "
             "entry, oldest per-account RiskSweep pass, per-adapter market_open and maintenance, oldest undelivered alert, SM8 not READY or SM5 UNKNOWN, spill file non-empty, "
             "an open task circuit); daily test page",
             MX + 20, yB, MW - 40, hD, kstyle("class"), "MEL")
    y_al = yE + hAl / 2
    hPh = p.H("phone", 70)
    p.vertex("phone", tag("external", "people") + "<b>Risk manager / on-call</b><br>email, push and SNS pages",
             MX + 20, y_al - hPh / 2, MW - 40, hPh, kstyle("external"), "MEL")
    p.vertex("s3", tag("store", "Tier 3 &middot; Memento") + "<b>Immutable image</b> &mdash; ap-southeast-4 S3, Object "
             "Lock<br>pgBackRest WAL + daily incremental; restores are operator-run only",
             MX + 20, y_r - hS3 / 2, 300, hS3, kstyle("store"), "MEL")

    # ---------------- notes
    yb = Y0 + H_all + 30
    yn = yb + 40
    NW3 = 726
    hN = p.rowH([("ha_note", 300), ("rto_note", 300), ("ops_note", 300)])
    p.vertex("ha_note", tag("note") + "<b>Single writer &amp; failover</b><br>"
             "&bull; <b>Lease + epoch</b>: the leader holds an etcd lease; epoch = (etcd cluster_id, lease revision), "
             "written to PostgreSQL engine_epoch (fencing token = the revision). Every engine write carries the token in the "
             "same statement (no lock), so a stale leader's writes affect 0 rows and fail. The HLC is re-seeded after the bump.<br>"
             "&bull; <b>Self-fence off the event loop</b>: the lease keeper thread hard-kills the engine at 12 s without "
             "renewal (TTL 15 s, renewed every 1 s); the kernel resets its sockets (SO_LINGER 0). Watchdog 3 s &lt; fence "
             "12 s &lt; TTL 15 s, which leaves room for an etcd leader election.<br>"
             "&bull; <b>Health-gated election</b>: hard checks (PostgreSQL writable, etcd, Redis writable, chrony within "
             "50 ms) gate any campaign; broker reach is only a preference, and once the lease has been free for 15 s any "
             "node passing the hard checks campaigns; the node hosting the Patroni primary is preferred. The leader judges "
             "brokers by its own SM4 state (STREAMING is healthy whatever a probe says) and abdicates only to an "
             "eligible peer (hard checks, no back-off, no SAFE_HOLD): on a hard check only after 30 s (for the leader "
             "this includes a RiskSweep circuit open &gt; 5 min, once per incident), on broker grounds only when its own network path "
             "fails and the peer's works. AUTH_FAILED is a cluster-wide fact, never a reason to abdicate. 60 s back-off "
             "after a term ends (waived after a fence when no peer is alive); 3 abnormal term ends in 15 min or more than "
             "6 epoch changes in 30 min &rarr; SAFE_HOLD, a durable row (gate closed, one page, operator resume).<br>"
             "&bull; <b>Phase 2 contract</b>: two senders are excluded only within lease timing. Every send first "
             "commits an outbound_intent (epoch); a lease check with no await precedes the socket write; a new leader "
             "keeps the gate shut for TTL + TCP_USER_TIMEOUT + ack timeout and marks earlier-epoch SENDING intents "
             "UNKNOWN (resolved by a broker read or an operator, never re-sent).",
             40, yn, NW3, hN, kstyle("note"))
    td = "style='border:1px solid #D6B656;padding:2px 4px'"
    p.vertex("rto_note", tag("note") + "<b>Failure cases</b> (targets; measure in drills)"
             f"<table style='border-collapse:collapse;font-size:9px;margin-top:3px'>"
             f"<tr><td {td}><b>Failure</b></td><td {td}><b>What happens</b></td><td {td}><b>Ingestion back</b></td></tr>"
             f"<tr><td {td}>Engine crash / hang</td><td {td}>watchdog or fence kills it; standby wins the lease; LEADER_INIT</td><td {td}>~30&ndash;60 s</td></tr>"
             f"<tr><td {td}>Node or zone A lost</td><td {td}>Patroni promotes B (~15&ndash;25 s) and the lease moves to B</td><td {td}>~45&ndash;90 s</td></tr>"
             f"<tr><td {td}>Standby or zone B lost</td><td {td}>commits stall until Patroni drops sync (&le; ~20 s), then async + critical alert; no failover while the leader is on the primary's node</td><td {td}>&le; ~20 s stall</td></tr>"
             f"<tr><td {td}>Witness or etcd leader's zone lost</td><td {td}>etcd re-elects inside the fence budget; alert</td><td {td}>none</td></tr>"
             f"<tr><td {td}>PostgreSQL process fails on primary</td><td {td}>Patroni promotes the sync standby at once (primary_start_timeout 0)</td><td {td}>~15&ndash;25 s commit stall</td></tr>"
             f"<tr><td {td}>Redis lost on leader</td><td {td}>gate closed; SM8 rebuild in place, no abdication</td><td {td}>~10&ndash;30 s</td></tr>"
             f"<tr><td {td}>Broker outage</td><td {td}>SM4 backoff; FENCE flags; per-adapter page after 2 min not STREAMING while OPEN</td><td {td}>when broker returns</td></tr>"
             f"<tr><td {td}>PostgreSQL corruption</td><td {td}>HALTED + page; switchover to a checked standby or operator restore</td><td {td}>minutes&ndash;hours</td></tr>"
             f"<tr><td {td}>Region lost</td><td {td}>manual DR: isolate London, revoke its broker tokens, restore Tier 3 elsewhere</td><td {td}>2&ndash;4 h</td></tr>"
             "</table>RPO 0 for committed data while the sync standby is healthy; ActionGate stays closed throughout.",
             40 + NW3 + 20, yn, NW3, hN, kstyle("note"))
    p.vertex("ops_note", tag("note") + "<b>24/7 operations</b><br>"
             "&bull; <b>Supervision</b>: systemd Restart=always, MemoryMax; WatchdogSec=3 (SIGKILL) only for "
             "event-loop lag and lease expiry. Every task is restarted with backoff and stamps its liveness against "
             "a per-task max age; waits on PostgreSQL, BACKOFF and AUTH_FAILED count as alive. A stale stamp pages "
             "and restarts that task; N deaths open a per-task circuit (input quarantined, page, 'degraded' in the "
             "dead-man payload) that half-opens every 10 min or closes on an audited reset_task. Blocking SDK / DNS calls run in threads with timeouts.<br>"
             "&bull; <b>Metrics</b>: ingest rate, queue / spool / spill / DLQ depth, feed lag + PEL, sweep pass age, loop lag, clock "
             "offset, replication and WAL-archive lag, Redis memory, disk, REST budget, lease-renew p99, token age, "
             "certificate expiry; JSON logs with ids + epoch, shipped off-node.<br>"
             "&bull; <b>Data</b>: partitions by seq_id range (~monthly; oversee_events by sweep_id, outbox by created_at), verified export (gzip JSONL + sha256, copied to S3) before detach, 90 days hot, "
             "audit tables per compliance; outbox rows deleted after relay; disk alerts 70 / 85 %.<br>"
             "&bull; <b>Change</b>: rolling upgrade by switchover; migrations run by an operator before the switch "
             "(lock_timeout 2 s); rule thresholds versioned and hot-reloaded (sweep_now() for every account); a reload resolves the states "
             "of removed or disabled rules.<br>"
             "&bull; <b>Secrets</b>: Secrets Manager (notification channel credentials too, re-read on auth error); the cTrader token pair is refreshed only by the leader with lease margin, after a 'refresh in flight' row, and saved "
             "with a token_generation compare-and-set; AUTH_FAILED pages instead of looping and is re-armed by a new credential generation ("
             "Secrets Manager version or token_generation) or an audited operator command.<br>"
             "&bull; <b>DR</b>: archive_mode=always on both members, daily incremental backups, quarterly timed drill.",
             40 + 2 * (NW3 + 20), yn, NW3, hN, kstyle("note"))
    yl = yn + hN + 30
    hL = p.H("lat", 170)
    p.vertex("lat", tag("note") + "<b>Why London</b> (estimates; measure from the instances before go-live)"
             f"<table style='border-collapse:collapse;font-size:10px;margin-top:4px'>"
             f"<tr><td {td}><b>Concern</b></td><td {td}><b>Engine in Melbourne</b></td><td {td}><b>Engine in London</b></td></tr>"
             f"<tr><td {td}>RTT to broker FX servers</td><td {td}>~250&ndash;280 ms</td><td {td}>~1&ndash;3 ms</td></tr>"
             f"<tr><td {td}>Two broker round trips inside the 2 s stop-loss grace</td><td {td}>~550 ms (~27 %)</td><td {td}>&lt; 6 ms (&lt; 0.3 %)</td></tr>"
             f"<tr><td {td}>Clock discipline for the HLC</td><td {td}>OS NTP, &plusmn;10&ndash;50 ms</td><td {td}>chrony + cloud PTP, &lt; 0.1 ms</td></tr>"
             f"<tr><td {td}>Phase 2 sending (later)</td><td {td}>slippage from transit time</td><td {td}>near the liquidity</td></tr>"
             "</table>The stop-loss grace runs from effective_stop_missing_since_ms on the local clock, so it is never spent in transit.",
             40, yl, 2220, hL, kstyle("note"))
    lend = legend(p, 40, yl + hL + 30, 2220, ["external", "active", "class", "store", "note"],
                  edges=[("data / control", C_DATA, ""), ("replication / backup", C_INT, ""), ("alerts", C_OVR, ""),
                         ("heartbeat / read-only", LANE["L2"][1], "dashed=1;")])
    p.w, p.h = 2300, int(lend + 40)

    # ---------------- edges
    p.edge("d_brk1", "engA", "stonex", "", p.top("engA", AX + 100), p.bot("stonex", AX + 100), color=C_DATA,
           extra="startArrow=block;startFill=1;")
    p.edge("d_brk2", "engA", "pepper", "", p.top("engA", AX + 310), p.bot("pepper", AX + 310), color=C_DATA,
           extra="startArrow=block;startFill=1;")
    yh = Y0 + 45
    dx = MX + 80
    p.edge("d_dms", "engA", "dash", "push heartbeat every 15 s (node, epoch, gate, halt level, adapter states)",
           p.top("engA", AX + AW - 10), p.top("dash", dx), pts=[(AX + AW - 10, yh), (dx, yh)],
           color=LANE["L2"][1], extra="dashed=1;")
    p.edge("d_prov", "engA", "providers", "send", p.left("engA", y_prov), p.right("providers", y_prov), color=C_OVR)
    p.edge("d_page", "alerting", "phone", "SNS page", p.right("alerting", y_al), p.left("phone", y_al), color=C_OVR)
    p.edge("d_dmspage", "dash", "phone", "page (outside eu-west-2)", p.bot("dash", MX + 250), p.top("phone", MX + 250),
           color=C_OVR)
    p.edge("d_keepA", "keepA", "etcdA", "lease", p.bot("keepA", AX + 60), p.top("etcdA", AX + 60), color=C_DATA)
    p.edge("d_keepB", "keepB", "etcdB", "campaign", p.bot("keepB", BX_ + 60), p.top("etcdB", BX_ + 60), color=C_DATA)
    p.edge("d_etAB", "etcdA", "etcdB", "", p.right("etcdA"), p.left("etcdB"), color=C_INT, extra="endArrow=none;")
    p.edge("d_etBC", "etcdB", "etcdC", "", p.right("etcdB"), p.left("etcdC"), color=C_INT, extra="endArrow=none;")
    p.edge("d_sync", "pgA", "pgB", "sync", p.right("pgA"), p.left("pgB"), color=C_INT)
    pgx = AX + 60
    p.edge("d_wal", "pgA", "s3", "WAL archive (pgBackRest, archive-async; both members archive) &middot; ~250 ms RTT, "
           "off the hot path", p.bot("pgA", pgx), p.left("s3", y_r), pts=[(pgx, y_r)], color=C_INT)
    p.edge("d_phone", "providers", "phone", "email / push", p.bot("providers", EX + 125), p.bot("phone", MX + 400),
           pts=[(EX + 125, yb), (MX + 400, yb)], color=C_OVR)
    return p


# =================================================================== build
pages = [page_architecture(), page_patterns(), page_states(), page_usecases(), page_deployment()]
bad = False
for pg in pages:
    probs = pg.check()
    print(f"[{pg.pid}] {len(pg.V)} vertices, {len(pg.E)} edges, {len(pg.cont)} containers, "
          f"page {pg.w}x{pg.h}: " + ("OK" if not probs else f"{len(probs)} problem(s)"))
    for pr in probs:
        print("    ", pr)
    bad |= bool(probs)
    pg.emit()
ET.indent(mxfile, space="  ")
ET.ElementTree(mxfile).write(OUT, encoding="utf-8", xml_declaration=False)
print("wrote", OUT, "| measured sizes used:", len(MEASURED))
sys.exit(1 if bad else 0)
