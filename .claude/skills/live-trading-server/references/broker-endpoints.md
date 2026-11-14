# Broker Adapter Normalization & Endpoint Reference (Read-Only Phase 1)

## 1. Environment Variables (Required)
All adapters must read credentials strictly from environment variables and default to demo/sandbox hosts:
- `BROKER_ADAPTER_TYPE`: `mock` | `pepperstone_ctrader`
- `BROKER_ENV`: `sandbox` (default; reject `live` in Phase 1)
- `BROKER_WS_URL`: `wss://demo-api.pepperstone.com/ws`
- `BROKER_REST_URL`: `https://demo-api.pepperstone.com/v1`
- `BROKER_API_KEY`: Loaded via `os.environ["BROKER_API_KEY"]`
- `BROKER_API_SECRET`: Loaded via `os.environ["BROKER_API_SECRET"]`

## 2. Canonical Status Mapping (`Broker Status -> OrderDTO.status`)
| Raw Broker Status | Canonical `OrderStatus` | Trigger DLQ on Missing `client_order_id`? |
|---|---|---|
| `NEW` / `ACCEPTED` | `WORKING` | Yes |
| `PARTIALLY_FILLED` | `PARTIALLY_FILLED` | Yes |
| `FILLED` / `EXECUTED` | `FILLED` | Yes |
| `CANCELLED` / `EXPIRED` | `CANCELLED` | Yes |
| `REJECTED` | `REJECTED` | Yes (Log reason to SOP audit table) |

## 3. Rate Limits & Reconnection Policy
- **REST Polling (`fetch_open_orders`, `fetch_order_status`):** Max 5 requests/sec per symbol; honor `Retry-After` HTTP 429 headers.
- **WebSocket Streams (`stream_ticks`, `stream_order_updates`):** Heartbeat ping every `15s`; reconnect on missed pong within `5s` using `min(60.0, 1.0 * (2 ** attempt) + random.uniform(0, 0.5))`.