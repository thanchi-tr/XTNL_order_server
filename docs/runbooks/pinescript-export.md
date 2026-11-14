# Pine Script export of trades (TradingView)

Phase 1 never sends orders. This export only **reads** the engine's own PostgreSQL log (one read-only
transaction, `SET TRANSACTION READ ONLY`, SELECT only); it never contacts a broker and writes no database row
(no audit row is needed). The generated scripts are a read-only picture of what the engine recorded, with
**no amounts**: they contain the symbol, broker order / position ids, trade numbers, sides, times, status /
event names and price levels (order price, entry, stop-loss, take-profit, exit). Never a size, a balance, a
P&L, a money result, a margin or a spread. The one derived figure (asked for by the user) is the table's
**Result (R)**: the trade's result in R, where 1R is a price distance set from the ATR before the entry. No
pip count and no pip size are ever shown.

Code: `src/reports/lifecycle.py` (read, timelines, trades), `src/reports/pinescript.py` (renderer),
`src/reports/export.py` (files), command in `src/ops/cli.py`.

## Trigger

You want to see the last days of trading on a TradingView chart, each trade drawn like TradingView's long /
short position tool: where it entered, where its stop-loss and take-profit were (and how they moved), and
when, where and how it ended (TP hit, SL hit, closed, still open), plus the orders that never filled.

## Checks

* `ORDER_SERVER_DSN` is set to a login that may read `event_log`, `orders_state` and `positions_state` (the
  operator or dashboard login is enough; nothing is written).
* The engine has been running over the window you want: versions are only in `event_log` for the time the
  engine ingested them, or caught up after a downtime (`$OPS catchup show`).

## Commands

```
OPS=".venv/Scripts/python.exe -m src.ops"        # Linux nodes: .venv/bin/python -m src.ops
$OPS pinescript                                   # last 7 days, every symbol, every adapter
$OPS pinescript --days 3 --symbol EURUSD          # EURUSD only (EUR/USD, eurusd are the same)
$OPS pinescript --adapter stonex --out var/pinescript
```

Options: `--days N` (1-90, default 7), `--symbol S`, `--adapter A` (`stonex`, `pepperstone`, ...),
`--out DIR` (default `var/pinescript`, which git ignores). One file per symbol is written:
`<SYMBOL>_<YYYY-MM-DD>.pine` (UTC date of the export). The command prints only file names and counts:

```
var/pinescript/EURUSD_2026-11-14.pine orders=36 positions=5 trades=5 pending=7 drawn=5+7 lines=21 labels=23 boxes=10 left_out=0
```

`drawn=` is trades + pending orders drawn; `left_out=` what the drawing limits left out (both kinds).

Exit codes: 0 written (or nothing in the window), 1 PostgreSQL unreachable / query failed / folder not
writable (the error names its type only, never the DSN password), 2 usage (`--days` out of range, no DSN).

### Add it to a chart

1. Open the chart of the same symbol in TradingView (any timeframe: drawings use `xloc.bar_time` with UNIX
   millisecond times, so they land at the exact times on 1 m, 1 h or 1 D alike).
2. Open the **Pine Editor** (bottom panel), create a new indicator, select all and delete the template.
3. Paste the whole content of the `.pine` file, **Save**, then **Add to chart**.
4. The script draws once, on the last confirmed historical bar. Use the indicator's settings (Inputs) to
   toggle each layer, or the symbol options below.

### What a trade is

A **trade** is one position with every order that belongs to it: its opening order (StoneX: the position id
is the opening order id; cTrader: matched by the fill), its stop-loss and take-profit orders (linked to the
position or to the opening order), their replacements (a stop cancelled and placed again at a new level is
one moved stop) and the closing order or closing trade. Trades are numbered **T1, T2, ...** per symbol in
entry-time order. Orders that never filled are **pending-only** items, drawn faintly (see below).

What a downtime catch-up read leaves out is recovered conservatively: a stop / limit leg that lost its link in
a catch-up row still belongs to its trade; an unlinked order on the other side cancelled with the position's
close is the trade's other bracket leg (one-cancels-other); a StoneX bracket that never filled (entry X, legs
X+1 / X+2 on the other side, cancelled together) is one pending item.

### What is on the chart

Each trade is drawn like the position tool (example, fake prices, time zone Australia/Melbourne):

* a **red** zone from the entry price to the stop-loss, and a **green** one from the entry price to the
  take-profit, both from the entry time to the exit time, each with a translucent fill and a **1 px border**
  of its own colour, so a thin zone (a near stop) stays visible. A stop or target that moved starts a new zone
  at the new level from the time it moved (so a trailed stop is a staircase of red zones). A thin dotted line
  sits on each level;
* the **entry line** (solid, side colour: light blue BUY, light violet SELL, width 2) from the entry to the exit;
* an **entry marker** exactly at the entry time and price: a tiny triangle (up BUY, down SELL, side colour).
  Hover it: `T3 SELL #1038847936 · entry 13 Nov 21:40 @ 1.10400`, then the trade's orders, its events and the
  result. (`first seen open 13 Nov 21:40 @ 1.10400` when the opening fill is not in the log, plus
  `opened before window` when that is before the window; `entry time not recorded @ 1.10400` when a catch-up
  read recorded the trade only at its close: then the marker sits at the drawn left edge.)
* an **exit marker** exactly at the exit time and price, with the trade number as its own tiny text (a
  group's numbers when exits meet, see Minimal):
  red x-cross = SL hit, green diamond = TP hit, grey circle = closed otherwise (manual close, or the close price
  was not recorded: then at the entry level). Hover it: `T3 exit 14 Nov 03:10 @ 1.10120 · SL hit`. A trade
  still open has a small open circle `○` at its right edge (hover: `T3 still open · at export <time>`);
* a small **dot** on the level at every stop-loss / take-profit change inside the trade (set, moved,
  removed); hover it for `SL moved 13 Nov 23:05 @ 1.10450`. Tick **Text on the change dots** to put
  `SL → 1.10450` and the time on the chart instead;
* the opening order's working line before the fill (faint, dashed, side colour).

**Label mode** (Inputs, group Display) decides the text:

* **Minimal** (default). The trade's tag, `T3 SELL`, is written **inside its own green reward zone** (inside
  its red risk zone when it has no take-profit), small, in the light **Text colour**, at the zone's left corner on its
  **entry end** (input **Minimal: tag corner**: `Entry end`, default, or `Level end`, the stop / target end,
  which keeps the tag off the candles around the entry). When another trade's tag already holds that spot (two
  trades opened close together), the tag takes the next free corner **of its own zone** (left: the other end;
  then right: the preferred end, the other end), and stays at its preferred corner only when all four are
  taken. The tag therefore moves and scales with its trade and can never float away from it. A trade with
  neither zone gets a thin transparent box around its entry line just to carry the tag. There are no other text
  labels and nothing is moved: every marker sits exactly on its time and price. When exit markers would meet
  (two trades closed in the same bar at about one price, as T1 / T2 of the week of 13 Nov), the first one's
  text lists every trade number (`T1 T2`), the others print nothing, and each one's tooltip lists all their
  exits; so numbers never print over each other. Entry markers sharing a spot add `entries here: T1 SELL, T2
  SELL` to their tooltips. The full detail is in the **table**.
* **Detailed**. The earlier text labels: entry text immediately left of the trade (pointer right), exit text
  immediately right of it (pointer left), at the entry / exit price:

  ```
  T3 SELL #1038847936              exit 14 Nov 03:10 @ 1.10120
  entry 13 Nov 21:40 @ 1.10400     TP hit
  ```

  Labels that would overlap are stacked (see **No overlap** below), but a label is moved **at most 3 of its own
  heights**; one that would have to move further is not drawn (its marker's tooltip still tells it), so no
  label floats far from its trade.

The SL / TP level that decides the exit kind is the one resting at the exit: the exit is `SL hit` (`TP hit`)
when the exit price is within 0.02 % of the stop-loss (take-profit) level then, which leaves room for a stop's
slippage; otherwise `closed`.

The tooltips list the trade's orders (`orders #1038847936 #1038847937 ...`) and its events with time and price
(placed, filled, SL / TP set, moved, removed, closed), then the result.

A trade with no time span (a catch-up read gave it only its close time) is drawn **Minimum trade width** bars
wide (default 10), ending at its exit; a trade shorter than that is widened to the right, and a dashed
segment at the exit price joins the exit marker (which stays on the exit) to the right edge.

**Pending-only orders** are a faint grey dashed line at the order price from placement to cancel (or to now
while working) and a tiny grey mark (cross: cancelled / rejected, circle: still working). They carry no text;
hover the mark: `#1038847989 SELL pending order`, its legs, `placed <time> @ <price>`, `SL` / `TP` levels,
`cancelled <time>`. A catch-up read that saw only the cancel has no line, only the mark.

| Layer (input) | Drawing |
|---|---|
| Risk / reward zones | the red / green zones (when they are off, Minimal carries each tag in its transparent tag box) |
| Stop-loss / take-profit level lines | the dotted line on each level |
| Trade tags (Minimal) / entry and exit text (Detailed) | the `T3 SELL` tag, or the two text labels per trade |
| Entry and exit markers | triangle at the entry, x-cross / diamond / circle at the exit, open circle while open, and the exit segment |
| Stop-loss / take-profit changes (dots) | the change dots (or their text) |
| Opening order before the fill | the faint working line |
| Pending orders that never filled | grey lines and marks |
| Trade table | see below |

**No overlap (Detailed only).** Minimal mode moves nothing. In Detailed mode the script lays the labels out
when it draws, on the chart's own timeframe: an entry label's box lies left of the trade's left edge, an exit
label's right of its right edge, centred on its price. Going through the labels in time order, a label that
would cover one already drawn is moved just past it (a SELL's entry label up, a BUY's down; an exit label
towards its side of the entry), so no two drawn labels overlap and a label stays on its price whenever nothing
is in the way. A label that would move more than 3 of its own heights is left out (tooltip only, on its
marker). A label's box is estimated from its text: **Text width per character** (bars per character,
default 1.2, for TradingView's default zoom) and **Text line height** (ticks; `0` = auto, the chart's ATR(14));
Minimal uses the same two estimates to find a free tag corner and to group exit markers. The layout is computed once, when the script draws, not on every zoom: if
labels touch after you zoom out, raise the width (or the height); if they sit too far apart, lower it.

**Readability.** All text (table cells, Minimal tags, the exit markers' trade numbers, Detailed entry / exit
labels) is the **Text colour** input, default `#E6E6E6` (light grey), on the dark background: the old orange
SELL text was hard to read. The side colours only colour lines and markers: BUY `#82B1FF` (light blue), SELL
`#B388FF` (light violet; was orange `#FF9800`), so a SELL entry line is neither the red stop-loss zone nor the
green take-profit zone. Exit markers keep their red / green / grey shape colour; Detailed change text
(`SL → ...` / `TP → ...`) stays red / green. Detailed text labels are a dark neutral box (**Label / table
background**); there is never white text on a saturated fill. Every colour is an input (group **Colours**), as
are the fill and border transparency of each zone.

### Trade table

One table, default **bottom left** (the right-hand corners are left free for your own panels; choose
`bottom center`, `top left`, `top center` or `middle left` in **Table position**). It is kept small: a title
row, a window row, the column titles, then **one row per trade**, newest entry first, **Table rows** of them
(default 12; at most 36), in the **Table text size** you choose, all in the light **Text colour**. In Minimal
mode this is where the detail lives (fake prices, time zone Australia/Melbourne):

```
XTNL trades EURUSD
window UTC 2026-11-07 00:00 > 2026-11-14 00:00
T#   Entry                 Exit             Result (R)
T6   01:40 @ 1.10600       -                open
T5   00:46 @ 1.10553       01:31 @ 1.10560  -0.12R
T4   seen 21:40 @ 1.10400  00:50 @ 1.10450  +0.63R
T2   time n/r @ 1.10370    01:31 @ 1.10377  n/a (entry time n/r)
T1   09:30 @ 1.08300       12:10 @ 1.08600  +2.14R
```

* **Title** `XTNL trades EURUSD`. Hover it: `read-only export, no amounts`, `trades 5 (drawn 5, left out 0)`,
  `pending orders 7 (drawn 7, left out 0)`, `time zone Australia/Melbourne`, `chart symbol EURUSD (match)`.
  When something was left out (drawing limits), the title adds `(n left out)` and turns red.
* **Window** row: only `window UTC <from> > <to>`.
* **Entry / Exit**: the **time only** plus price (`00:46 @ 1.10553`; input **Table time format**, default
  `HH:mm`). Hover the cell for the full date-time (`entry 14 Nov 00:46 @ 1.10553`; the exit cell adds the exit
  kind, `TP hit`), in the **Time format** input. A trade can close on a later day than it opened: hover to see
  the dates.
* There is no Side column: the side is in the **T#** tooltip (`T5 SELL #1038900001`, `orders #... #...`), on the
  entry marker (triangle up BUY / down SELL) and in the tag.
* When the chart symbol does not match the data symbol, the one row under the column titles is a red
  `chart symbol mismatch: <chart symbol>` (hover: how to fix it) and nothing is drawn. There is no symbol / time
  zone row otherwise.

`time n/r`: the entry time was not recorded; `seen`: the time the position was first seen open. The stop-loss
changes are in the markers' tooltips (a trade whose entry time was not recorded ran while the engine was off: its
stop-loss changes are unknown, and its hover lists no fill event either, as its fill row carries the close time).
The exit kind (TP hit, SL hit, closed, still open) is on the exit marker, in the Exit cell's tooltip and in the
Result (R) tooltip.

**Result (R)** is the trade's result in R: **(exit price - entry price) x direction / 1R** (direction +1 for a
BUY, -1 for a SELL), shown signed with 2 decimals (`-0.12R`, `+1.50R`, `0.00R`), **green** above 0, **red** below,
the Text colour at 0 and for `open` / `n/a`. **1R is a price distance set from the ATR, not from the stop**
(a trade without a stop-loss still gets a result):

* ATR = `ta.atr(14)` (Wilder) on the **ATR timeframe** (default `15`, 15 minutes) of the ATR bar that **closed
  immediately before the entry**: an entry at 01:01, or exactly at 01:00, uses the 00:45-01:00 bar. The script
  reads it without repainting, `request.security(syminfo.tickerid, atrTf, ta.atr(atrLen)[1], lookahead =
  barmerge.lookahead_on)` at the global scope, and takes it per trade on the chart bar holding the entry time in
  one pass (trades sorted by entry, a pointer advanced bar by bar);
* the ATR is counted in the symbol's **pip**: the tick x 10 when the price has 3 or 5 decimals (FX fractional
  pips: EURUSD tick 0.00001 -> pip 0.0001, USDJPY 0.001 -> 0.01), otherwise the tick itself (2 or 4 decimals).
  The decimals come from `syminfo.pricescale`. **Pip size override** (0 = auto) replaces it for an unusual symbol;
* that pip count is rounded to the nearest 0.5 with halves up (4.2 -> 4, 4.3 -> 4.5, 4.25 -> 4.5, 6.6 -> 6.5,
  6.8 -> 7; a tiny 0.000001 is added before the floor so an exact half that float arithmetic puts just below,
  e.g. 7.25 -> 7.249999999999999, still rounds up), then clamped to **6..14** (lower counts as 6, higher as 14; inputs **1R minimum (pips)** /
  **1R maximum (pips)**), then turned back into price (x pip);
* worked example (the T5 row above): SELL at 1.10553, closed at 1.10560, ATR before the entry 4.3 pips -> 4.5 ->
  clamped to 6, so 1R = 6 x 0.0001 = 0.0006; result = (1.10560 - 1.10553) x -1 / 0.0006 = -0.00007 / 0.0006 =
  **-0.12R** (red). The result is rounded to 0.01R with halves up, with the same tiny addition (0.125R ->
  +0.13R, -0.125R -> -0.12R, never `-0.00R`);
* hover: `result -0.12R; 1R from 15m ATR(14), clamped 6-14; exit: closed` (plus `entry time = first seen open`
  when the opening fill is not in the log: the ATR is then taken at that time). The pip count of 1R and the pip
  size are never printed.

An **open** trade shows `open` (no unrealised result; hover `open: no result until closed; exit: still open`).
`n/a (...)` when it cannot be measured: `entry time n/r`, `no entry price`, `no exit price` (no closing fill
recorded, or the position is not in the log), `chart > 15m` (the chart timeframe is above the ATR timeframe:
pick 15 m or lower), `before chart history` (the entry is before the first bar TradingView loaded: scroll back,
or use a higher chart timeframe that is still <= the ATR timeframe), `after last bar`, `no ATR yet` (fewer than
14 ATR bars before the entry).

### Inputs

| Group | Input | Default |
|---|---|---|
| Layers | zones, level lines, trade tags / text labels, entry and exit markers, change dots, opening order before the fill, pending orders, trade table | all on |
| Display | Label mode: `Minimal` (tag inside the zone, markers, table) or `Detailed` (text labels, stacked, at most 3 heights) | Minimal |
| Display | Minimal: tag corner (`Entry end` / `Level end`; a taken corner moves the tag to a free corner of its zone) | Entry end |
| Display | Text on the change dots | off |
| Display | Time zone: `Australia/Melbourne`, `UTC`, `Europe/London`, `America/New_York`, `Chart exchange` (the symbol's exchange zone) | Australia/Melbourne |
| Display | Time format (TradingView `str.format_time` pattern, e.g. `dd MMM HH:mm:ss`) | `dd MMM HH:mm` |
| Display | Minimum trade width (bars) | 10 |
| Display | Text line height (ticks, 0 = auto: ATR) | 0 |
| Display | Text width per character (bars, raise when zoomed out) | 1.2 |
| Table | Table rows (newest trades first), Table position, Table text size (`tiny`, `small`, `normal`) | 12, bottom left, small |
| Table | Table time format (entry / exit cells; the full date-time is in the cell tooltip) | `HH:mm` |
| Result (R) | ATR timeframe, ATR length, 1R minimum (pips), 1R maximum (pips), Pip size override (0 = auto) | `15`, 14, 6, 14, 0 |
| Colours | risk zone fill / 1 px border transparency, reward zone fill / 1 px border transparency | 80 / 60, 85 / 60 |
| Colours | risk zone / stop-loss, reward zone / take-profit, BUY / SELL entry line and marker, Text colour (table, tags, exit numbers, entry / exit labels), exit, pending orders, background | `#F23645` red, `#089981` green, `#82B1FF` light blue / `#B388FF` light violet, `#E6E6E6` light grey, `#D1D4DC` light grey, `#787B86` grey, dark box |
| Symbol | Chart symbol override, Draw on any symbol | blank, off |

Times in the script are UNIX milliseconds; the script formats them in the chosen time zone, and prices with the
symbol's tick size (`format.mintick`).

**Symbol guard.** The script draws only when the chart ticker matches the data symbol after normalising both
(upper case, `/ . _ - :` removed; a broker suffix such as `EURUSD.a` or `EURUSDm` still matches `EURUSD`). If
the table says `chart symbol mismatch: ...`, set **Chart symbol override** to the data symbol, or tick **Draw on any symbol**.

## Limits

* TradingView keeps at most 500 lines, 500 labels and 500 boxes per script. A trade uses one box and one dotted
  line per stop-loss / take-profit step (plus the transparent tag box when no zone carries its tag), the entry
  line, the exit segment, the opening order's working line(s), four labels (the two Detailed text labels and the
  entry and exit markers, counted in either mode) and one dot per change; a pending order one line per working
  price and one mark. When a window has more, the **newest** trades (by entry) are kept until the next would not fit, then the
  newest pending orders with what is left; the rest are left out. The header comment, the table title (`(n left out)`
  in red, counts in its tooltip) and the command output (`left_out=`) say how many. Export a shorter `--days` window or one
  `--symbol` to see older ones. A very large script is trimmed the same way to stay under ~90 000
  characters, dropping the oldest pending orders before any trade. A stop that moved more than 100 times keeps
  its 100 newest steps and dots (the trade's tooltip still counts every event).
* Stops and targets: Pepperstone carries them on the position (and on the order before the fill). On StoneX
  they are child orders; a child priced below the entry of a BUY (above for a SELL) is the stop-loss, otherwise
  the take-profit (a stop trailed past the entry keeps the role it started with).
* Pepperstone opening orders carry no position id: the position of the same account, symbol and side first
  seen within 5 s of the fill is taken as the one it opened.
* An exit price is known only from a recorded closing fill (closing order, filled stop or target). Otherwise
  the exit label says `price not recorded` and the result is `closed`.
* Order history comes from `event_log` versions committed in the window (DROPPED_STALE rows are not drawn),
  plus the current rows of touched, linked and still-live entities. Times are broker times; a row the broker
  gave no time falls back to its commit time.
* Output is deterministic: the same data and export time give the same text.

## Rollback

Nothing to roll back: the export writes only the `.pine` files in `--out`. Delete them, or remove the
indicator from the chart.
