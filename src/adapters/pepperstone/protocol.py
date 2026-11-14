"""cTrader Open API (ProtoOA) JSON framing and the read-only payloadType allow-list
(adapter-isolation.md §4, lockout layer 4 for cTrader).

Frames are JSON text messages ``{"clientMsgId": str, "payloadType": int, "payload": {...}}``.
Every outbound frame is built by :func:`encode_request`, which refuses any payloadType that is
not on :data:`OUTBOUND_ALLOW_LIST` with ExecutionLockedError before anything reaches the socket.
Order-mutating payload types are not on the allow-list and are deliberately not named here.
The numeric values follow ProtoOAPayloadType / ProtoPayloadType (to confirm against the API version).
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from src.config import ExecutionLockedError

JsonObj = Mapping[str, Any]

# --- common (ProtoPayloadType) ---------------------------------------------------------------------
PT_ERROR_RES: Final = 50
PT_HEARTBEAT_EVENT: Final = 51

# --- outbound requests on the allow-list (ProtoOAPayloadType) ------------------------------------
PT_APPLICATION_AUTH_REQ: Final = 2100
PT_ACCOUNT_AUTH_REQ: Final = 2102
PT_VERSION_REQ: Final = 2104
PT_ASSET_LIST_REQ: Final = 2112
PT_SYMBOLS_LIST_REQ: Final = 2114
PT_SYMBOL_BY_ID_REQ: Final = 2116
PT_TRADER_REQ: Final = 2121
PT_RECONCILE_REQ: Final = 2124
PT_SUBSCRIBE_SPOTS_REQ: Final = 2127
PT_UNSUBSCRIBE_SPOTS_REQ: Final = 2129
PT_DEAL_LIST_REQ: Final = 2133
PT_CASH_FLOW_HISTORY_REQ: Final = 2143
PT_ACCOUNTS_BY_TOKEN_REQ: Final = 2149
PT_REFRESH_TOKEN_REQ: Final = 2173
PT_ORDER_LIST_REQ: Final = 2175
PT_DEAL_LIST_BY_POSITION_REQ: Final = 2179
PT_ORDER_DETAILS_REQ: Final = 2181

# --- responses -------------------------------------------------------------------------------------
PT_APPLICATION_AUTH_RES: Final = 2101
PT_ACCOUNT_AUTH_RES: Final = 2103
PT_VERSION_RES: Final = 2105
PT_ASSET_LIST_RES: Final = 2113
PT_SYMBOLS_LIST_RES: Final = 2115
PT_SYMBOL_BY_ID_RES: Final = 2117
PT_TRADER_RES: Final = 2122
PT_RECONCILE_RES: Final = 2125
PT_SUBSCRIBE_SPOTS_RES: Final = 2128
PT_UNSUBSCRIBE_SPOTS_RES: Final = 2130
PT_DEAL_LIST_RES: Final = 2134
PT_OA_ERROR_RES: Final = 2142
PT_CASH_FLOW_HISTORY_RES: Final = 2144
PT_ACCOUNTS_BY_TOKEN_RES: Final = 2150
PT_REFRESH_TOKEN_RES: Final = 2174
PT_ORDER_LIST_RES: Final = 2176
PT_DEAL_LIST_BY_POSITION_RES: Final = 2180
PT_ORDER_DETAILS_RES: Final = 2182

# --- server events ---------------------------------------------------------------------------------
PT_TRAILING_SL_CHANGED_EVENT: Final = 2107
PT_SYMBOL_CHANGED_EVENT: Final = 2120
PT_TRADER_UPDATED_EVENT: Final = 2123
PT_EXECUTION_EVENT: Final = 2126
PT_SPOT_EVENT: Final = 2131
PT_ORDER_ERROR_EVENT: Final = 2132
PT_MARGIN_CHANGED_EVENT: Final = 2141
PT_TOKEN_INVALIDATED_EVENT: Final = 2147
PT_CLIENT_DISCONNECT_EVENT: Final = 2148
PT_ACCOUNT_DISCONNECT_EVENT: Final = 2164

#: The one canonical outbound allow-list (adapter-isolation.md §4). Anything else is refused.
OUTBOUND_ALLOW_LIST: Final[frozenset[int]] = frozenset({
    PT_APPLICATION_AUTH_REQ,
    PT_ACCOUNT_AUTH_REQ,
    PT_ACCOUNTS_BY_TOKEN_REQ,
    PT_REFRESH_TOKEN_REQ,
    PT_VERSION_REQ,
    PT_RECONCILE_REQ,
    PT_ORDER_DETAILS_REQ,
    PT_ORDER_LIST_REQ,
    PT_DEAL_LIST_REQ,
    PT_DEAL_LIST_BY_POSITION_REQ,
    PT_CASH_FLOW_HISTORY_REQ,
    PT_TRADER_REQ,
    PT_SYMBOLS_LIST_REQ,
    PT_SYMBOL_BY_ID_REQ,
    PT_ASSET_LIST_REQ,
    PT_SUBSCRIBE_SPOTS_REQ,
    PT_UNSUBSCRIBE_SPOTS_REQ,
    PT_HEARTBEAT_EVENT,
})

#: Historical requests share the tighter broker rate limit (5 / s, to confirm).
HISTORICAL_REQUESTS: Final[frozenset[int]] = frozenset({
    PT_DEAL_LIST_REQ, PT_DEAL_LIST_BY_POSITION_REQ, PT_CASH_FLOW_HISTORY_REQ, PT_ORDER_LIST_REQ,
})

#: Server errors meaning the access token is no longer accepted (refresh, then retry once).
TOKEN_ERROR_CODES: Final[frozenset[str]] = frozenset({
    "CH_ACCESS_TOKEN_INVALID", "ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED", "INVALID_ACCESS_TOKEN",
})
#: Credential errors that no retry fixes (AUTH_FAILED, adapter-isolation.md §5).
CREDENTIAL_ERROR_CODES: Final[frozenset[str]] = frozenset({
    "CH_CLIENT_AUTH_FAILURE", "CH_OA_CLIENT_NOT_FOUND", "CH_CTID_TRADER_ACCOUNT_NOT_FOUND",
    "ACCOUNT_NOT_AUTHORIZED", "INVALID_GRANT", "INVALID_REFRESH_TOKEN", "CH_REFRESH_TOKEN_INVALID",
})
ALREADY_DONE_CODES: Final[frozenset[str]] = frozenset({
    "CH_CLIENT_ALREADY_AUTHENTICATED", "ALREADY_LOGGED_IN", "ALREADY_SUBSCRIBED",
})
NOT_FOUND_CODES: Final[frozenset[str]] = frozenset({
    "ORDER_NOT_FOUND", "POSITION_NOT_FOUND", "ENTITY_NOT_FOUND",
})

#: ProtoOAClientPermissionScope: the 'accounts' OAuth scope is SCOPE_VIEW.
SCOPE_VIEW: Final = "SCOPE_VIEW"
_SCOPES: Final[dict[int, str]] = {0: "SCOPE_VIEW", 1: "SCOPE_TRADE"}


def scope_name(raw: object) -> str | None:
    """permissionScope as its enum name (JSON may carry the number or the name)."""
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, int):
        return _SCOPES.get(raw, str(raw))
    text = str(raw).strip().upper()
    if text.isdigit():
        return _SCOPES.get(int(text), text)
    return text or None


def assert_allowed(payload_type: int) -> None:
    """Lockout layer 4: refuse every payloadType that is not on the read-only allow-list."""
    if isinstance(payload_type, bool) or not isinstance(payload_type, int) \
            or payload_type not in OUTBOUND_ALLOW_LIST:
        raise ExecutionLockedError(
            f"cTrader payloadType {payload_type!r} is not on the Phase 1 read-only allow-list")


def encode_request(payload_type: int, payload: JsonObj, client_msg_id: str | None) -> str:
    """Build one outbound JSON frame. The allow-list check is the first thing that happens."""
    assert_allowed(payload_type)
    frame: dict[str, Any] = {"payloadType": payload_type, "payload": dict(payload)}
    if client_msg_id is not None:
        frame["clientMsgId"] = client_msg_id
    return json.dumps(frame, separators=(",", ":"))


@dataclass(frozen=True)
class Frame:
    """One decoded inbound frame."""

    payload_type: int
    payload: JsonObj
    client_msg_id: str | None


def decode_frame(raw: str | bytes) -> Frame | None:
    """Parse one inbound JSON frame; None when it is not a well-formed ProtoMessage."""
    try:
        obj = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(obj, dict):
        return None
    pt = obj.get("payloadType")
    if isinstance(pt, bool):
        return None
    if isinstance(pt, str) and pt.strip().isdigit():
        pt = int(pt)
    if not isinstance(pt, int):
        return None
    payload = obj.get("payload")
    msg_id = obj.get("clientMsgId")
    return Frame(pt, payload if isinstance(payload, dict) else {},
                 None if msg_id is None else str(msg_id))


def is_error(payload_type: int) -> bool:
    return payload_type in (PT_ERROR_RES, PT_OA_ERROR_RES)
