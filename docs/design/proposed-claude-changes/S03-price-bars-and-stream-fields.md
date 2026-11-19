# Proposed change (S03): `.claude/rules/adapter-isolation.md` and `dto-contracts.md`, price bars and price stream fields

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.

## Why

S03 added an optional, read-only adapter capability and a probe read (design PR3), price stream fields (PR2) and
per-market stream events (PR6). The adapter rule's §4 "Read-Only Interface Lock" lists the only methods an adapter
may implement, so without this change it no longer names `fetch_price_bars` or the StoneX `probe_price_bars`. Both
are reads: `fetch_price_bars` has a base default that answers "not supported" (StoneX keeps it until S39, Pepperstone
is not bar-capable in v1, `MockBrokerAdapter` scripts it), and `probe_price_bars` sends only GET requests through the
adapter's existing `ReadOnlyTransport` (under `rest_get_only()`). The DTO rule should name the new transport types
and say they are never persisted. Nothing here narrows `FORBIDDEN_METHOD_NAMES`, the `ReadOnlyTransport` deny-list or
the hook's `FORBIDDEN_PATTERNS`.

## Diff

```diff
--- a/.claude/rules/adapter-isolation.md
+++ b/.claude/rules/adapter-isolation.md
@@ §4 Read-Only Interface Lock
    - `stream_ticks(symbols)`, `ensure_ticks(symbols)` (...), `stream_order_updates()`, `stream_positions()`, `stream_account()`.
+   - `fetch_price_bars(symbol, side, timeframe_s, *, count=None, from_ms=None, to_ms=None) -> PriceBarsResponse` (optional, design PR3): the broker's own bars of one series in the count form (`count` complete bars, plus the forming one, which never counts towards `count`) or the range form, oldest first, the forming bar flagged `partial`. Base default: "not supported" (`supported=False`). Errors raise `BrokerError` (`PriceBarsDecodeError` for an answer that cannot be decoded). Never persisted, never logged with prices. StoneX keeps the default until the PR3 probe's results are recorded (S39); its `probe_price_bars` (same signature) serves only the probe (`python -m src.adapters.stonex.probe_bars`) and sends GET requests only (`rest_get_only()`), through the adapter's `ReadOnlyTransport` and REST budget.
@@ §5 Liveness
+   - Price streams (design PR2, PR6): each `PriceDTO` carries `ts_source` (`BROKER` when the broker's tick time was read, `RECEIPT` when `timestamp_ms` fell back to receipt time) and `received_ms` (StoneX: stamped on the Lightstreamer thread when the frame arrives; cTrader: stamped by the socket reader). A per-market price subscription event names its canonical symbol in `StreamEvent.market` (StoneX PRICES subscriptions; the raw MarketId for a market without a known instrument, never `None`; `None` for whole-stream events such as a lost socket or the cTrader batch subscribe). Lost price updates are counted per market (`StoneXAdapter.dropped_price_frames`, with frames dropped for a full channel), never a gap. `XTNL_STONEX_PRICE_UNFILTERED=1` (off by default, turned on only after the PR3 probe) asks `setRequestedMaxFrequency("unfiltered")` on PRICES subscriptions only.

--- a/.claude/rules/dto-contracts.md
+++ b/.claude/rules/dto-contracts.md
@@ §3 Immutable Canonical Fields
    - `InstrumentSpec`: ...
+   - `BrokerBar` (`bucket_start_ms`, `open`, `high`, `low`, `close`, the response's own `side` tag or `None`, `partial`) and `PriceBarsResponse` (`bars`, `truncated`, `received_ms`, the request's form, `supported`, `note`, `field_names`) in `src/dto/bars.py` are **transport types only**: an adapter's answer to `fetch_price_bars`, never published on a bus topic, never written to PostgreSQL, Redis payloads of the four topics, logs or pages. Their `repr` hides the OHLC values. Validation (design §A4) is the consumer's job.
```

The `...` in the diff stands for the unchanged text of the same line.
