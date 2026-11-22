"""Scenario data for design 3 (live stop adjustment, Phases 3-5; design only).

Times are seconds after 10:00:00; prices are illustrative bid. Lane items:
("chip", t, label, kind, human) or ("bar", t0, t1, kind). Kinds: g grey,
a amber, t teal, r red, c coral. Table cells: (kind, lines) with kind
"you" (human), "srv" (order-server) or "brk" (broker).
"""
from gen_time_price import ANCH as BASE_ANCH

BASE_BARS = [(0, 1.10090, 1.09960), (900, 1.10180, 1.10040), (1800, 1.10240, 1.10120),
             (2700, 1.10270, 1.10170), (3600, 1.10290, 1.10200), (4500, 1.10300, 1.10240)]
TICKS_A = [1.09800, 1.09900, 1.10000, 1.10100, 1.10200]
TICKS_BD = [1.09800, 1.09900, 1.10000, 1.10100, 1.10200, 1.10300]
STATE4 = lambda a0, a1, t1, c1: [(a0, a1, "Armed", "g"), (a1, t1, "Tracking", "g"), (t1, c1, "Advice", "a"), (c1, 5400, "Complete", "t")]

SCEN = {}

# ---------------------------------------------------------------- D control
SCEN["d"] = dict(
    title="Design 3 scenario D: control, live stop adjustment",
    desc="Two long orders; nothing special happens. The order-server proposes and (Phase 4/5) sends or (Phase 3) waits for approval before moving each stop to break-even; release follows at the next bar end.",
    seed=7, noise=0.000045, yrange=(1.09760, 1.10340), ticks=TICKS_BD,
    anchors=sorted(BASE_ANCH + [(1000, 1.10050)]), bars=BASE_BARS,
    orders={
        1: dict(fill=200, entry=1.10000, stop_p45=[(200, 1.09800), (3612, 1.10000)], stop_p3=[(200, 1.09800), (3651, 1.10000)],
                pills=[(911, "below 1R", "g"), (1811, "below 1R", "g"), (2711, "not conf.", "g"), (3611, "ADJUST", "a"), (4511, "COMPLETE", "t")],
                p45=[("chip", 3611, "moved 11:00:12", "t", False)],
                p3=[("bar", 3612, 3650, "a"), ("chip", 3650, "you OK 11:00:50", "", True)],
                state=STATE4(202, 911, 3611, 4571)),
        2: dict(fill=1000, entry=1.10050, stop_p45=[(1000, 1.09950), (2712, 1.10050)], stop_p3=[(1000, 1.09950), (2751, 1.10050)],
                pills=[(1811, "not conf.", "g"), (2711, "ADJUST", "a"), (3611, "COMPLETE", "t"), (4511, "silent", "g")],
                p45=[("chip", 2711, "moved 10:45:12", "t", False)],
                p3=[("bar", 2712, 2750, "a"), ("chip", 2750, "you OK 10:45:50", "", True)],
                state=STATE4(1002, 1811, 2711, 3671)),
    },
    rows=[
        (200, ("you", ["A1 · You go long: fill 1.10000, stop 1.09800"]), None),
        (1000, None, ("you", ["B1 · You go long: fill 1.10050, stop 1.09950"])),
        (2660, ("srv", ["PRE bar 2: 1.08R heads-up. Never an intent."]),
               ("srv", ["PRE bar 1: 1.65R heads-up. Never an intent."])),
        (2711, ("srv", ["POST bar 2: 0.95R, not confirmed."]),
               ("srv", ["POST bar 1: 1.40R > 1R → ADJUST.", "Intent I2 PROPOSED, target 1.10050.",
                        "P4/P5: checks → SENT → CONFIRMED 10:45:12."])),
        (2750, None, ("you", ["B2 · P3 only: you approve I2 (10:45:50)", "Checks → sent → CONFIRMED 10:45:51."])),
        (3611, ("srv", ["POST bar 3: 1.15R > 1R → ADJUST.", "Intent I1 PROPOSED, target 1.10000.",
                        "P4/P5: CONFIRMED 11:00:12."]),
               ("srv", ["POST bar 2: at target. Release held for", "the verification re-read."])),
        (3671, None, ("srv", ["Re-read confirms: COMPLETE, released."])),
        (3650, ("you", ["A2 · P3 only: you approve I1 (11:00:50)", "Checks → sent → CONFIRMED 11:00:51."]), None),
        (4511, ("srv", ["POST bar 4: at target → 11:16:11 COMPLETE.", "P3 unapproved: I1 expired 11:01:11;",
                        "ADJUST again; release ≥ 11:31:11."]),
               ("srv", ["Released: recorded silently."])),
    ],
    foot=["Release always comes from a bar end that sees the stop at target, never from the send itself.",
          "The orders share the gate, lease and broker session; sends are serialized; one never blocks the other."],
)

# ---------------------------------------------------------------- A spike
A_ANCH = [a for a in BASE_ANCH if a[0] <= 2660] + [(300, 1.10020),
          (2668, 1.10180), (2680, 1.10020), (2683, 1.09940), (2688, 1.09800), (2690, 1.09790), (2700, 1.09830),
          (2900, 1.09860), (3300, 1.09900), (3600, 1.09880), (4000, 1.09920), (4500, 1.09900), (5000, 1.09930), (5400, 1.09920)]
SCEN["a"] = dict(
    title="Design 3 scenario A: heads-up, then the original stop is hit before bar end",
    desc="A heads-up at 10:44:20, then a spike through both orders' stops before the bar ends. Order 1 never gets an intent; order 2's break-even move (Phase 4/5) limits its loss, while in Phase 3 without approval it closes at the original stop.",
    seed=11, noise=0.00003, yrange=(1.09740, 1.10300), ticks=TICKS_A,
    anchors=sorted(A_ANCH),
    bars=[(0, 1.10090, 1.09960), (900, 1.10180, 1.10040), (1800, 1.10240, 1.09790), (2700, 1.09910, 1.09820),
          (3600, 1.09940, 1.09860), (4500, 1.09950, 1.09880)],
    orders={
        1: dict(fill=200, entry=1.10000, end=2688, stop_p45=[(200, 1.09800)], stop_p3=[(200, 1.09800)],
                closes=[(2688, 1.09800, "")],
                pills=[(911, "below 1R", "g"), (1811, "below 1R", "g"), (2711, "closed", "r")],
                p45=[("chip", 2660, "heads-up only: no intent", "g", False)],
                p3=[("chip", 2660, "heads-up only: no intent", "g", False)],
                state=[(202, 911, "Armed", "g"), (911, 2688, "Tracking", "g"), (2688, 5400, "Closed", "r")]),
        2: dict(fill=300, entry=1.10020, end=2683, stop_p45=[(300, 1.09940), (1812, 1.10020)], stop_p45_end=2680,
                stop_p3=[(300, 1.09940)], stop_p3_end=2683,
                closes=[(2680, 1.10020, ""), (2683, 1.09940, "")],
                pills=[(911, "below 1R", "g"), (1811, "ADJUST", "a"), (2711, "closed", "r")],
                p45=[("chip", 1811, "moved 10:30:12", "t", False)],
                p3=[("bar", 1812, 1871, "a"), ("chip", 1871, "expired 10:31:11", "g", False)],
                state=[(302, 911, "Armed", "g"), (911, 1811, "Tracking", "g"), (1811, 2681, "Advice", "a"), (2681, 5400, "Closed", "r")]),
    },
    rows=[
        (200, ("you", ["A1 · You go long: fill 1.10000, stop 1.09800"]), None),
        (300, None, ("you", ["B1 · You go long: fill 1.10020, stop 1.09940"])),
        (1811, ("srv", ["POST bar 1: 0.75R, below trigger."]),
               ("srv", ["POST bar 1: 1.62R > 1R → ADJUST, intent I2.", "P4/P5: CONFIRMED 10:30:12, stop 1.10020.",
                        "P3: no answer in 60 s → EXPIRED 10:31:11."])),
        (2660, ("srv", ["PRE bar 2: 1.08R heads-up (provisional).", "No intent: a heads-up never acts."]),
               ("srv", ["P4/P5: at target, no PRE item.", "P3: heads-up (2.44R); still no intent sent."])),
        (2680, None, ("brk", ["P4/P5: spike triggers the stop at 1.10020;", "fills at or below it: about break-even."])),
        (2683, None, ("brk", ["P3: spike triggers the original 1.09940;", "about −1R or worse (I2 had expired)."])),
        (2688, ("brk", ["Spike triggers the stop at 1.09800; fills", "at or below it: about −1R. CLOSED."]), None),
        (2711, ("srv", ["Closed: no POST, no intent. The heads-up", "already sent cannot be retracted.*"]),
               ("srv", ["Closed: no POST, no open intent."])),
    ],
    foot=["Only bar-end decisions act, so nothing can move order 1's stop between the heads-up and the spike.",
          "* Proposed 'ended' notice for closes by stop or TP (E19). Stop fills are market fills: slippage possible."],
)

# ---------------------------------------------------------------- B take profit
SCEN["b"] = dict(
    title="Design 3 scenario B: take profit within the first 10 minutes",
    desc="Order 1 hits its take profit during bar 0 and closes before any check; order 2 continues and its stop is moved to break-even at the 10:30 bar end.",
    seed=7, noise=0.000045, yrange=(1.09760, 1.10340), ticks=TICKS_BD,
    anchors=sorted(BASE_ANCH + [(420, 1.10035)]), bars=BASE_BARS,
    orders={
        1: dict(fill=200, entry=1.10000, tp=1.10090, end=660, stop_p45=[(200, 1.09800)], stop_p3=[(200, 1.09800)],
                closes=[(660, 1.10090, "")],
                pills=[(700, "closed by TP", "r")],
                p45=[("chip", 700, "nothing to move", "g", False)],
                p3=[("chip", 700, "nothing to move", "g", False)],
                state=[(202, 660, "Armed", "g"), (660, 5400, "Closed", "r")]),
        2: dict(fill=420, entry=1.10035, stop_p45=[(420, 1.09955), (1812, 1.10035)], stop_p3=[(420, 1.09955), (1851, 1.10035)],
                pills=[(911, "below 1R", "g"), (1811, "ADJUST", "a"), (2711, "COMPLETE", "t"), (3611, "silent", "g"), (4511, "silent", "g")],
                p45=[("chip", 1811, "moved 10:30:12", "t", False)],
                p3=[("bar", 1812, 1850, "a"), ("chip", 1850, "you OK 10:30:50", "", True)],
                state=STATE4(422, 911, 1811, 2771)),
    },
    rows=[
        (200, ("you", ["A1 · You go long: 1.10000, stop 1.09800,", "take profit 1.10090."]), None),
        (420, None, ("you", ["B1 · You go long: fill 1.10035,", "stop 1.09955, no take profit."])),
        (660, ("brk", ["Take profit fills at 1.10090: CLOSED.", "The broker cancels the stop child (OCO)."]), None),
        (662, ("srv", ["Tracker writes CLOSED. No PRE, no POST,", "no intent. Ended notice (proposed).*"]), None),
        (911, None, ("srv", ["POST bar 0: calm, 0.31R, below trigger.", "Order 1's close does not affect it."])),
        (1811, None, ("srv", ["POST bar 1: 1.44R → ADJUST, intent.", "P4/P5: CONFIRMED 10:30:12."])),
        (1850, None, ("you", ["B2 · P3 only: you approve (10:30:50)", "→ CONFIRMED 10:30:51."])),
        (2711, None, ("srv", ["POST bar 2: at target, release held;", "10:46:11 COMPLETE, released."])),
    ],
    foot=["Edge B′: a TP or stop fill within 2 s of entry, before the first tracker pass, is never armed.",
          "* Proposed 'ended' notice (E19); today a plain close sends no ARM notice."],
)

# ---------------------------------------------------------------- C volatile first bar
C_ANCH = [(0, 1.09990), (100, 1.10010), (200, 1.10000), (300, 1.09900), (500, 1.10100), (720, 1.10260), (860, 1.10240), (900, 1.10230),
          (1000, 1.10200), (1080, 1.10220), (1300, 1.10240), (1500, 1.10280), (1760, 1.10255), (1800, 1.10250),
          (2100, 1.10230), (2400, 1.10300), (2660, 1.10285), (2700, 1.10280),
          (3000, 1.10300), (3300, 1.10340), (3560, 1.10335), (3600, 1.10330),
          (3900, 1.10345), (4200, 1.10360), (4460, 1.10355), (4500, 1.10350), (4800, 1.10355), (5400, 1.10360)]
SCEN["c"] = dict(
    title="Design 3 scenario C: first bar too volatile, trailing starts one bar late",
    desc="Order 1's first bar is 3.1 x ATR14, so the first-bar guard holds it and its stop moves one bar later; order 2's own first bar is calm and it trails normally.",
    seed=5, noise=0.00003, yrange=(1.09760, 1.10420), ticks=[1.09800, 1.09900, 1.10000, 1.10100, 1.10200, 1.10300, 1.10400],
    anchors=C_ANCH,
    bars=[(0, 1.10260, 1.09900), (900, 1.10280, 1.10200), (1800, 1.10300, 1.10225), (2700, 1.10345, 1.10290),
          (3600, 1.10365, 1.10340), (4500, 1.10370, 1.10350)],
    orders={
        1: dict(fill=200, entry=1.10000, stop_p45=[(200, 1.09800), (1812, 1.10000)], stop_p3=[(200, 1.09800), (1851, 1.10000)],
                pills=[(911, "bar 0 volatile", "c"), (1811, "ADJUST", "a"), (2711, "COMPLETE", "t"), (3611, "silent", "g"), (4511, "silent", "g")],
                p45=[("chip", 860, "held", "g", False), ("chip", 1811, "moved 10:30:12", "t", False)],
                p3=[("chip", 860, "held", "g", False), ("bar", 1812, 1850, "a"), ("chip", 1850, "you OK 10:30:50", "", True)],
                state=STATE4(202, 911, 1811, 2771)),
        2: dict(fill=1080, entry=1.10220, stop_p45=[(1080, 1.10140), (3612, 1.10220)], stop_p3=[(1080, 1.10140), (3651, 1.10220)],
                pills=[(1811, "below 1R", "g"), (2711, "below 1R", "g"), (3611, "ADJUST", "a"), (4511, "COMPLETE", "t")],
                p45=[("chip", 3611, "moved 11:00:12", "t", False)],
                p3=[("bar", 3612, 3650, "a"), ("chip", 3650, "you OK 11:00:50", "", True)],
                state=STATE4(1082, 1811, 3611, 4571)),
    },
    rows=[
        (200, ("you", ["A1 · You go long: fill 1.10000, stop 1.09800"]), None),
        (860, ("srv", ["PRE bar 0: 1.20R, conditional heads-up", "(first-bar check pending). No intent."]), None),
        (911, ("srv", ["POST bar 0: range 3.1 × ATR14 > 1.318.", "PRE_NOT_CONFIRMED(FIRST_BAR_VOLATILE)", "although 1.15R > 1R. No intent."]), None),
        (1080, None, ("you", ["B1 · You go long: fill 1.10220, stop 1.10140"])),
        (1811, ("srv", ["POST bar 1: 1.25R → ADJUST, intent.", "P4/P5: CONFIRMED 10:30:12."]),
               ("srv", ["POST bar 0: calm (~0.6–0.7 × ATR14), 0.38R.", "Not held: the guard is per position."])),
        (1850, ("you", ["A2 · P3 only: you approve (10:30:50)", "→ CONFIRMED 10:30:51."]), None),
        (2711, ("srv", ["POST bar 2: at target, release held;", "10:46:11 COMPLETE, released."]),
               ("srv", ["POST bar 1: 0.75R, below trigger."])),
        (3611, None, ("srv", ["POST bar 2: 1.38R → ADJUST, intent.", "P4/P5: CONFIRMED 11:00:12."])),
        (3650, None, ("you", ["B2 · P3 only: you approve (11:00:50)", "→ CONFIRMED 11:00:51."])),
        (4511, None, ("srv", ["POST bar 3: at target, release held;", "11:16:11 COMPLETE, released."])),
    ],
    foot=["The volatile first bar delays order 1's move by one bar; its original stop protects meanwhile.",
          "Order 2's bar 0 is its own fill bucket (10:15–10:30), which is calm."],
)
