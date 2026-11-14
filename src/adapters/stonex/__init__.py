"""Read-only StoneX (GAIN Capital CIAPI) adapter package (adapter-isolation.md §2 StoneX)."""
from src.adapters.stonex.adapter import ACCOUNT_FIELDS, StoneXAdapter
from src.adapters.stonex.mapping import AccountDirectory, ClientAccount, parse_account_directory
from src.adapters.stonex.orders import ORDER_FIELDS, OrderItem, OrderStreamMapper
from src.adapters.stonex.rest import StoneXRestClient
from src.adapters.stonex.streaming import FrameChannel, FrameSink, LightstreamerTransport, StreamTransport

__all__ = [
    "ACCOUNT_FIELDS", "ORDER_FIELDS", "AccountDirectory", "ClientAccount", "FrameChannel", "FrameSink",
    "LightstreamerTransport", "OrderItem", "OrderStreamMapper", "StoneXAdapter", "StoneXRestClient",
    "StreamTransport", "parse_account_directory",
]
