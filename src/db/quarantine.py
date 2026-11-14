"""QuarantineSink (src/oversee/feed.py) over PostgreSQL `events_quarantine` (oversee-rules.md §3): a feed
entry delivered 5 times (or malformed) is recorded with an alert, then XACKed by the observer. Fenced:
only the leader consumes the feed."""
from __future__ import annotations

import json
from typing import Final

from src.db.epoch import EngineDb
from src.db.fence import FENCE
from src.db.oversee_errors import oversee_errors
from src.oversee.feed import FeedEntry

SQL_FEED_QUARANTINE: Final = (
    "INSERT INTO events_quarantine (source, feed_entry_id, event_type, entity_id, account_id, raw_payload, "
    "error_reason, retry_count) "
    f"SELECT 'feed', $2::text, $3::text, $4::text, $5::text, $6::text, $7::text, $8::int WHERE {FENCE}"
)


class PgQuarantineSink:
    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def quarantine(self, entry: FeedEntry, reason: str) -> None:
        f = entry.fields
        async with oversee_errors():
            async with self._db.transaction() as tx:
                await tx.write(SQL_FEED_QUARANTINE, entry.entry_id, f.get("entity_type"), f.get("entity_id"),
                               entry.account_id, json.dumps(f, sort_keys=True), reason, entry.deliveries, must=True)
