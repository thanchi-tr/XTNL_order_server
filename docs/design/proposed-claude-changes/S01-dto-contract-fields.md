# Proposed change (S01): `.claude/rules/dto-contracts.md`, the new DTO fields

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it.

## Why

S01 added optional fields to the canonical DTOs (design PR1, PR2, PR7, PR8; `src/dto/entities.py`,
`src/dto/versioning.py`). The rule file lists the canonical fields of each DTO, so without this change it no longer
matches the code. The new fields change no behaviour: every producer still emits `None`, the optional payload fields
count in `payload_checksum` only when set, and the version labels never count (so no stored checksum changes).

## Diff

```diff
--- a/.claude/rules/dto-contracts.md
+++ b/.claude/rules/dto-contracts.md
@@ §2 Versioning
    - Every stateful DTO (`OrderDTO`, `PositionDTO`, `AccountDTO`) carries `broker_updated_at_ms`, `broker_seq: Optional[int]` and `source: Literal["STREAM", "REST"]`.
+   - They also carry two non-payload version labels (never in the checksum, like `source`): `origin: Optional[Literal["stream", "snapshot", "history", "dlq", "stop_read"]]`, the path that produced the version, and `broker_time_source: Optional[Literal["BROKER", "WATERMARK"]]`, whether `broker_updated_at_ms` is a real broker time or a watermark label. `event_log.origin` stores the first and `event_log.broker_time_ms` holds the broker time only when it is `BROKER` (migration 0015). Set by the orchestrator only; `None` until then.
@@ §3 Immutable Canonical Fields
-   - `OrderDTO`: `client_order_id`, ..., `is_reduce_only: bool`, `linked_position_id: Optional[str]`, the version fields, `seq_id`, `checksum`.
+   - `OrderDTO`: `client_order_id`, ..., `is_reduce_only: bool`, `linked_position_id: Optional[str]`, `order_kind: Optional[Literal["STOP", "LIMIT"]]` (the broker order type; `None` = not known), the version fields, `seq_id`, `checksum`.
+     - **Optional payload fields** (`order_kind`; on `InstrumentSpec` `price_increment`, `min_stop_distance`, `asset_class`): part of the checksum only when set (`OPTIONAL_PAYLOAD_FIELDS` in `src/dto/versioning.py`), so a row without them keeps its checksum. The JSON encodings leave a `None` one out.
@@
-   - `PriceDTO`: `adapter_id`, `symbol`, `bid`, `ask`, `spread_pips`, `timestamp_ms`, `seq_id`. Prices are per (adapter, symbol): ...
+   - `PriceDTO`: `adapter_id`, `symbol`, `bid`, `ask`, `spread_pips`, `timestamp_ms`, `seq_id`, `ts_source: Optional[Literal["BROKER", "RECEIPT"]]` (what `timestamp_ms` is) and `received_ms: Optional[int]` (local receipt time). Prices are per (adapter, symbol): ...
@@
-   - `InstrumentSpec`: `adapter_id`, ..., `min_qty`, `swap_booked_to_balance: bool`. Mark thresholds such as `max_clean_spread_pips` live in rule config, not here.
+   - `InstrumentSpec`: `adapter_id`, ..., `min_qty`, `swap_booked_to_balance: bool`, and the optional metadata `price_increment`, `min_stop_distance` (`Optional[Decimal]`) and `asset_class: Optional[str]` (instrument metadata, not market data; never shown in a notice, page or log line; `point_size` is never a fallback for `price_increment`). Mark thresholds such as `max_clean_spread_pips` live in rule config, not here.
```

The `...` in the diff stands for the unchanged text of the same line.
