"""Instrument reads for the alert emails: the `point_size` of the instruments an ARM notice names, read by the ARM
relay at send time so the XTNL-theme email can show a stop's distance from the entry in pips (owner decision
2026-11-21, `.claude/rules/riskmgr.md` rule 5). Instrument metadata, not market data; the size is never stored in a
notice, printed or logged."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Final

from src.db.epoch import EngineDb

SQL_POINT_SIZES: Final = (
    "SELECT adapter_id, symbol, point_size FROM instruments WHERE adapter_id = ANY($1::text[]) "
    "AND symbol = ANY($2::text[])")


class PgPointSizes:
    """The `point_size` of each wanted `(adapter_id, symbol)` instrument (the adapters' point is the pip), read by
    the ARM relay at send time so the themed email can show a stop's distance from the entry in pips (owner decision
    2026-11-21, `.claude/rules/riskmgr.md` rule 5). Instrument metadata, not market data; never stored in a notice
    and never logged. Pairs without a row are absent from the answer."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def __call__(self, keys: Sequence[tuple[str, str]]) -> dict[tuple[str, str], Any]:
        wanted = set(keys)
        if not wanted:
            return {}
        async with self._db.source.acquire() as conn:
            rows = await conn.fetch(SQL_POINT_SIZES, sorted({a for a, _ in wanted}), sorted({s for _, s in wanted}))
        return {(str(r[0]), str(r[1])): r[2] for r in rows if (str(r[0]), str(r[1])) in wanted}
