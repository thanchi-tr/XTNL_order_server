# Proposed change (S39): `.claude/rules/adapter-isolation.md`, StoneX broker bars

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.
It amends the S03 proposal (`S03-price-bars-and-stream-fields.md`); approve the two together, with this text
replacing the S03 sentence about StoneX.

## Why

The S03 proposal adds `fetch_price_bars` to §4 "Read-Only Interface Lock" and says StoneX keeps the base default
until the PR3 probe, with a separate `probe_price_bars` read for the probe. The user ran the probe on 2026-11-17
(design §A11 PR3). S39 then enabled the capability on StoneX and removed `probe_price_bars` (the probe now reads
through `fetch_price_bars`), so that sentence is out of date. The new text records the confirmed endpoints, the bar
time convention (`BarDate` = bucket start), that StoneX bars carry no side tag, and the probe's `--check-stops`
option. It is still a GET through the adapter's existing `ReadOnlyTransport` (under `rest_get_only()`). Nothing here
narrows `FORBIDDEN_METHOD_NAMES`, the `ReadOnlyTransport` deny-list or the hook's `FORBIDDEN_PATTERNS`.

## Diff (on top of the S03 proposal)

```diff
--- a/.claude/rules/adapter-isolation.md
+++ b/.claude/rules/adapter-isolation.md
@@ §4 Read-Only Interface Lock
-   - `fetch_price_bars(...) -> PriceBarsResponse` (optional, design PR3): ... StoneX keeps the default until the PR3 probe's results are recorded (S39); its `probe_price_bars` (same signature) serves only the probe (`python -m src.adapters.stonex.probe_bars`) and sends GET requests only (`rest_get_only()`), through the adapter's `ReadOnlyTransport` and REST budget.
+   - `fetch_price_bars(...) -> PriceBarsResponse` (optional, design PR3): ... Pepperstone keeps the default (not bar-capable in v1). StoneX implements it since S39 (PR3 probe, live_readonly, 2026-11-17): `GET market/{id}/barhistory` (count form) and `GET market/{id}/barhistorybetween` (range form), GET only (`rest_get_only()`), through the adapter's `ReadOnlyTransport` and REST budget at the caller's priority (`md-bars` reads at `BARS`); `BarDate` is the bucket start; bars carry no side tag (`BrokerBar.side` is `None`, the side is the request's). The PR3 probe (`python -m src.adapters.stonex.probe_bars`) reads through it; its `--check-stops` option compares the open positions' stop children with `fetch_active_stop_orders()` by order id only.
```

The `...` in the diff stands for the unchanged text of the same line.
