"""LocalStateReader (src/pubsub/orchestrator.py, src/ports.py) over PostgreSQL (adapter-isolation.md §5).

Before an adapter's first snapshot of a term, the orchestrator reads the locally stored non-terminal
orders and OPEN positions of that adapter so ids absent from the broker snapshot are re-read by id
(never closed on absence). Plain reads, no fence (they write nothing).
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from src.db.epoch import EngineDb
from src.db.rows import ORDER_COLS, POSITION_COLS, order_from_row, position_from_row
from src.dto import OrderDTO, PositionDTO

SQL_ADAPTER_OPEN_ORDERS: Final = (
    f"SELECT {ORDER_COLS} FROM orders_state WHERE adapter_id = $1 "
    "AND status IN ('PENDING', 'WORKING', 'PARTIALLY_FILLED') ORDER BY broker_order_id"
)
SQL_ADAPTER_OPEN_POSITIONS: Final = (
    f"SELECT {POSITION_COLS} FROM positions_state WHERE adapter_id = $1 AND status = 'OPEN' ORDER BY position_id"
)


class PgLocalStateReader:
    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def open_entities(self, adapter_id: str) -> tuple[Sequence[OrderDTO], Sequence[PositionDTO]]:
        async with self._db.source.acquire() as conn:
            orders = [order_from_row(r) for r in await conn.fetch(SQL_ADAPTER_OPEN_ORDERS, adapter_id)]
            positions = [position_from_row(r) for r in await conn.fetch(SQL_ADAPTER_OPEN_POSITIONS, adapter_id)]
        return orders, positions
