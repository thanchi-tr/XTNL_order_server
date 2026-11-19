"""`md_rehash`: rewrite every stored bar under the current content-hash key (design §A13 Key rotation, §B13; plan
S16, R11-M13).

Rotation order (runbook `docs/runbooks/marketdata.md`): install the new key as current and keep the old one as
`XTNL_MD_HMAC_KEY_PREV`; file `md_rehash`; retire the previous key only after `md show` reports the re-hash complete
**and** one correction window (`decision_cap`) has passed (`MdRehash.retire_allowed`).

The re-hash runs inside `md-writer` (`MdWriterService` work item), one chunk per step, so no flush interleaves with
it. For every known series (the writer's memory, `md:registry:series` and the stored-series index), page by page:

* a member whose hash is under the current key id is already done;
* a member that decodes and verifies under its **old, retained** key id (§A4 checks 1, 2, 4 and the keyed hash) is
  rewritten under the current id through the ordinary `FLUSH` script as a **silent** re-write: same bar, same
  revision, no `bar_revised` event and no rollback entry (the bar did not change), then `WAITAOF` like any closed bar;
  a rollback entry holding the old member (in `md:rb` and in the writer's memory) is rewritten in place with the new
  one, so after the old key retires the pull cursor and the next bootstrap still see only held key ids (an entry for
  an older, superseded revision keeps its old id: the newer revision's entry follows it);
* a member that does not decode, fails a check or has a key id the ring no longer holds is left as it is and counted
  (`skipped`): the read path deals with it (quarantine on a mismatch, `missing` otherwise); a rotation never
  quarantines anything itself;
* a bucket with a revision waiting in the writer's closed-bar queue is skipped (the queued bar is written under the
  current key anyway).

Progress is recorded per series in `md:meta` (`rehash_key` = the current key id, set with the series' last flush), so
`md show` can report completion without this process; `status()` gives the same in memory. Store errors retry on the
next step; 3 in a row refuse the request (`refused:store_unavailable`).

Nothing here logs or formats a price.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, Protocol

from src.marketdata.codec import DecodedMember, decode_member
from src.marketdata.hashing import HashStatus, Keyring, content_hash, key_id_of
from src.marketdata.store import scripts as S
from src.marketdata.store.client import WriteResult
from src.marketdata.store.reads import StoreReader
from src.marketdata.store.runner import ScriptOk, ScriptRefused, StoreUnavailable
from src.marketdata.types import SeriesId
from src.marketdata.validate import check_stored
from src.marketdata.writer import MdWriter

log = logging.getLogger(__name__)

PAGE: Final = 128
"""Members read and rewritten per step (at most one `FLUSH` of 128 closed bars)."""
STORE_ERROR_LIMIT: Final = 3
STEP_BUDGET_S: Final = 3.0
REHASH_META: Final = "rehash_key"


class RehashWritePort(Protocol):
    async def write_and_wait(self, calls: list[S.ScriptCall]) -> WriteResult: ...


@dataclass(frozen=True)
class RehashStatus:
    """What `md show` reports: counts only."""

    active: bool
    complete: bool
    key_id: int
    series_total: int
    series_done: int
    rewritten: int
    skipped: int
    completed_at_ms: int | None


class MdRehash:
    """The re-hash runner (`RehashPort` of `MdWriterService`)."""

    def __init__(self, writer: MdWriter, client: RehashWritePort, reader: StoreReader, *, keyring: Keyring,
                 clock_ms: Callable[[], int], page: int = PAGE) -> None:
        self._writer = writer
        self._client = client
        self._reader = reader
        self._keyring = keyring
        self._clock_ms = clock_ms
        self._page = page
        self._active = False
        self._series: list[SeriesId] | None = None
        self._index = 0
        self._after = 0
        self._done = 0
        self._rewritten = 0
        self._skipped = 0
        self._errors = 0
        self._completed_at: int | None = None

    # -------------------------------------------------------------------------------------------- control
    def needed(self) -> bool:
        """Only during a rotation: the ring retains a previous key."""
        return len(self._keyring.key_ids) > 1

    @property
    def active(self) -> bool:
        return self._active

    def start(self) -> None:
        self._active = True
        self._series = None
        self._index = self._after = self._done = self._rewritten = self._skipped = self._errors = 0
        self._completed_at = None

    def status(self) -> RehashStatus:
        total = 0 if self._series is None else len(self._series)
        return RehashStatus(self._active, self._completed_at is not None, self._keyring.current_id, total, self._done,
                            self._rewritten, self._skipped, self._completed_at)

    def retire_allowed(self, now_ms: int, correction_window_ms: int) -> bool:
        """The previous key may be retired: the re-hash completed and one correction window has passed since."""
        return self._completed_at is not None and now_ms >= self._completed_at + correction_window_ms

    # -------------------------------------------------------------------------------------------- steps
    async def step(self) -> str | None:
        """One page. None while running; `applied` when complete; `refused:<code>` after repeated store errors."""
        if not self._active:
            return None
        try:
            async with asyncio.timeout(STEP_BUDGET_S):
                finished = await self._step()
        except (StoreUnavailable, TimeoutError) as e:
            self._errors += 1
            kind = e.kind if isinstance(e, StoreUnavailable) else "timeout"
            log.info("md rehash: store unavailable (%s); retrying", kind)
            if self._errors >= STORE_ERROR_LIMIT:
                self._active = False
                return "refused:store_unavailable"
            return None
        self._errors = 0
        if not finished:
            return None
        self._active = False
        self._completed_at = self._clock_ms()
        log.info("md rehash: complete (%d series, %d rewritten, %d skipped)", self._done, self._rewritten,
                 self._skipped)
        return "applied"

    async def _known(self) -> list[SeriesId]:
        found: set[SeriesId] = set(self._writer.known_series())
        registry = await self._reader.registry_series()
        index = await self._reader.stored_index()
        for text in (*registry.values(), *index):
            try:
                found.add(SeriesId.parse(text))
            except ValueError:
                continue
        return sorted(found)

    async def _step(self) -> bool:
        if self._series is None:
            self._series = await self._known()
        if self._index >= len(self._series):
            return True
        series = self._series[self._index]
        page = await self._reader.bars_page(series, self._after, self._page)
        current = self._keyring.current_id
        now = self._clock_ms()
        writes: list[S.ClosedBarWrite] = []
        renamed: dict[str, str] = {}
        for start, member in page:
            decoded = decode_member(series, member)
            if not isinstance(decoded, DecodedMember):
                self._skipped += 1
                continue
            if key_id_of(decoded.content_hash) == current:
                continue
            checked = check_stored(decoded.bar, decoded.content_hash, self._keyring, now_ms=now)
            if not (checked.result.ok and checked.hash_status is HashStatus.VALID) or start != decoded.bar.bucket_start_ms:
                self._skipped += 1
                continue
            if self._writer.is_queued(series, start):
                continue
            bar = decoded.bar
            fresh = content_hash(self._keyring, series.canonical, bar.bucket_end_ms, bar.open, bar.high, bar.low,
                                 bar.close)
            write = S.ClosedBarWrite(bar, fresh, silent=True)
            writes.append(write)
            renamed[member] = write.member
        last_page = len(page) < self._page
        meta = ((REHASH_META, str(current)),) if last_page else ()
        if writes or meta:
            boot = self._writer.boot
            if boot is None:
                raise StoreUnavailable("error", "no boot sentinel")
            call = S.flush_call(self._writer.token, boot, series.adapter_id, series.symbol, now, [],
                                [S.SeriesFlush(series, closed=tuple(writes), meta=meta)],
                                rollback_n=self._writer.rollback_n)
            result = await self._client.write_and_wait([call])
            reply = result.replies[0] if result.replies else None
            if isinstance(reply, ScriptRefused) and reply.code in ("NO_BOOT", "NO_WRITER"):
                self._writer.note_lost_boot(reply.code)   # the writer bootstraps (even when idle); retried then
                return False
            if not isinstance(reply, ScriptOk) or not result.aof_confirmed:
                raise StoreUnavailable("error", "re-hash write not confirmed")
            self._writer.rehash_rollback(series, renamed)
            self._rewritten += len(writes)
        if page:
            self._after = page[-1][0] + 1
        if last_page:
            self._index += 1
            self._after = 0
            self._done += 1
        return self._index >= len(self._series)
