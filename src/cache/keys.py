"""Redis key naming (tier-persistence.md §1).

Entity keys are `{type}:{dto.entity_key}`, i.e. adapter-scoped, so they match the PostgreSQL primary
keys ((adapter_id, broker_order_id), (adapter_id, position_id), (adapter_id, account_id)) and never
collide between brokers or depend on a nullable client_order_id.
"""
from __future__ import annotations

from typing import Final, Literal

from src.dto import AccountDTO, OrderDTO, PositionDTO

EntityType = Literal["ORDER", "POSITION", "ACCOUNT", "INSTRUMENT"]
StreamKind = Literal["prices", "orders", "account"]

HALT_STATE: Final = "halt:state"
BOOT_ID: Final = "redis:boot_id"
BOOT_LOCK: Final = "redis:boot_lock"
STATE_DIGEST: Final = "state:digest"
FEED: Final = "oversee:feed"
FEED_MAXLEN: Final = 1_000_000
CONN_TTL_MS: Final = 30_000

# Prefixes whose entity writes produce a feed entry; instrument: is never fed.
FEED_PREFIXES: Final = ("order:", "position:", "account:")


def order_key(adapter_id: str, broker_order_id: str) -> str:
    return f"order:{adapter_id}:{broker_order_id}"


def position_key(adapter_id: str, position_id: str) -> str:
    return f"position:{adapter_id}:{position_id}"


def account_key(adapter_id: str, account_id: str) -> str:
    return f"account:{adapter_id}:{account_id}"


def instrument_key(adapter_id: str, symbol: str) -> str:
    return f"instrument:{adapter_id}:{symbol}"


def price_key(adapter_id: str, symbol: str) -> str:
    return f"price:{adapter_id}:{symbol}"


def clean_mid_key(adapter_id: str, symbol: str) -> str:
    return f"clean_mid:{adapter_id}:{symbol}"


def account_live_key(account_id: str) -> str:
    return f"account_live:{account_id}"


def conn_key(adapter_id: str, stream: StreamKind) -> str:
    return f"conn:{adapter_id}:{stream}"


def entity_type_of(dto: OrderDTO | PositionDTO | AccountDTO) -> EntityType:
    if isinstance(dto, OrderDTO):
        return "ORDER"
    if isinstance(dto, PositionDTO):
        return "POSITION"
    return "ACCOUNT"


def entity_key_of(dto: OrderDTO | PositionDTO | AccountDTO) -> str:
    """Redis key of a stateful DTO."""
    if isinstance(dto, OrderDTO):
        return order_key(dto.adapter_id, dto.broker_order_id)
    if isinstance(dto, PositionDTO):
        return position_key(dto.adapter_id, dto.position_id)
    return account_key(dto.adapter_id, dto.account_id)


def is_fed(key: str) -> bool:
    return key.startswith(FEED_PREFIXES)
