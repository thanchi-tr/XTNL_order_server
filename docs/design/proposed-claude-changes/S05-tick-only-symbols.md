# Proposed change (S05): `.claude/rules/adapter-isolation.md` §5 and `operations.md` §10, tick-only symbols and per-market price subscriptions

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.

## Why

S05 (design PR5, PR6; IN-8, IN-11, IN-12) added a second, separate set of price symbols to the ingestion orchestrator
(`set_md_tick_symbols`, the market-data layer's `XTNL_TICK_SYMBOLS`, empty by default), per-market price subscription
intervals from `StreamEvent.market`, the `on_price_subscription` hook and per-market drop counts. The rules describe
only the held symbols and their crosses, and §10 says "an adapter's market is OPEN when any of its subscribed symbols
is OPEN", which would now be ambiguous: a tick-only symbol is subscribed on the same stream but must never decide the
adapter's OPEN. With the set empty nothing changes; the behaviour is tested in `tests/pubsub/test_runtime_hooks.py`
and `tests/runtime/test_md_tick_symbols.py`.

## Diff

```diff
--- a/.claude/rules/adapter-isolation.md
+++ b/.claude/rules/adapter-isolation.md
@@ §5 SNAPSHOTTING
      - `stream_ticks(symbols)` includes the FX crosses needed for every `pnl_ccy` → account currency;
+     - it also includes the market-data layer's **tick-only** symbols (`XTNL_TICK_SYMBOLS`, a separate orchestrator set, design PR5). They are never part of the held / required set: they never decide SM10's adapter OPEN, are never SM10 evidence, are not watched for stale prices or clean mids, and their ticks go only to the price hook, never to `market.price.dto` (so never to Redis price keys or `last_clean_marks`). Their `ensure_ticks()` is a call of its own whose failure is logged and retried; a per-market UNSUBSCRIBED / ERROR on one of them closes its interval and never opens the prices gap, and its SUBSCRIBED never marks the stream subscribed after a required market or the whole stream failed. When no required symbol is asked for, the stream starts with the empty required set, the tick-only symbols get their own `ensure_ticks()`, and the stream is trivially subscribed. A symbol removed from the set stays tick-only (retired) for the adapter's life, because the adapters subscribe every market they ever had again at each stream start;
      - while STREAMING, the orchestrator calls `ensure_ticks(symbols)` (an allowed read-only subscribe, rate-limited) when a committed order or position needs a symbol or cross that is not subscribed yet, so a pair first traded mid-session gets prices; no clean mid within 30 s pages;
@@ §5 Liveness
      - A subscription is `SUBSCRIBED` from the broker's ack until an `UNSUBSCRIBED` / `ERROR` frame **or the loss of its socket**. ...
+     - Price subscriptions are also tracked **per market** (design PR6): a `StreamEvent` naming a `market` opens or closes that market's interval and resets only that market's subscription-snapshot tracking; a whole-stream SUBSCRIBED opens every market the adapter reports in `subscribed_price_markets` (else every requested market); every open / close timestamp is the orchestrator's clock; a socket / session loss, the end or restart of the prices pump and a whole-stream UNSUBSCRIBED / ERROR close them all. A downtime catch-up's fence hold never closes one. cTrader reports a symbol added by `ensure_ticks()` with its own SUBSCRIBED. Dropped price frames and lost updates are counted per market (never a gap).

--- a/.claude/rules/operations.md
+++ b/.claude/rules/operations.md
@@ §10
-    - SM10 is keyed by **(adapter_id, symbol)** ... An adapter's market is OPEN when any of its subscribed symbols is OPEN for it.
+    - SM10 is keyed by **(adapter_id, symbol)** ... An adapter's market is OPEN when any of its subscribed **held** symbols (orders, positions, configured `tick_symbols` and their crosses) is OPEN for it; a tick-only symbol (PR5) never decides it.
```

## Open point for S32 (not a rule change yet)

Once S32 wires `md_schedule_symbols`, tick-only symbols get a stored broker schedule. The calendar then knows them
like any previously held symbol whose schedule is still stored: they take part in `latest_open_ms` (the gate's
reconcile bound), in the open-transition RESYNCING request, and in `deciding_sessions`'s fallback to every scheduled
symbol when no wanted symbol is scheduled. S32 should either keep tick-only symbols out of those three paths or accept
and document it before `XTNL_MD_ENABLED` is turned on.
