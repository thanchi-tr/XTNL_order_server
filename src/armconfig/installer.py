"""The `arm-config` task: poll, check, validate and install accepted documents only (design §B9 Acceptance (Q12), §B13
Pause, §B5.1; plan S33).

`ConfigInstaller` is the arm-tracker's `AcceptedConfigPort` (`accepted()`): the **cached accepted document**, never a
Supabase read. Every `poll_s` (120 s; stamp age 120 s) one `poll()`:

1. **Local state** (ARM pool, `ArmConfigStore`): `arm_config_cache` (the installed document, the seen-row hashes and
   chain head) and the latest `arm_accepted` row (written only by `arm config accept` on the engine host: the only
   trust boundary). At start-up the cached document is restored once the known ids are ready; **its hash must equal
   the latest accepted hash**, else the ARM **pauses** with reason `config_hash` (`pause_reason()`, §B13: no decision
   is made; the tracker keeps the cached document for arming and ending) until a document with the accepted hash is
   installed. No accepted row: the ARM is **idle** (`accepted()` None, one page). While the known ids are not loaded
   (an adapter's instrument map is missing, for example an adapter stuck in AUTH_FAILED) nothing is restored or read;
   after 15 min of that one page goes out (`known_ids`), and the state (`starting` until then) is in the counters.
   `pause_reason()` is this module's pause only: the operator pause in `arm_controls` is read by the BarClock, which
   also takes `pause_reason` as its `config_pause` port (no decision while either holds).
2. **Read** (`ConfigSource`, `src/armconfig/client.py`): a failure is counted; unreachable for 15 min pages once; a
   failed connection self-check, or a pinned CA that cannot be loaded (`tls_ca`, a configuration fault, not an
   outage), pages at once (fail closed). The accepted document stays in force either way.
3. **Chain** (`src/armconfig/chain.py`) against the cached seen rows: **tampered** (a seen row changed or vanished, or
   the stored chain does not verify) pages at once and is audited; nothing installs. A newer `arm.schema_version`
   (`schema_ahead`) pages and installs nothing.
4. **Validation** (`validate_document` with the seen rows and now): an invalid document pages by row id and hash and
   is audited; the full read does not install, but its **accepted subset** (below) still may, so rows added after
   the accept can never block the accepted document.
5. **Pending** (rows not seen before) stay pending until accepted; a page follows after 1 day. The **candidate** is
   the read itself when its hash is the latest accepted hash, else the **accepted subset**: the rows set at or before
   a cut-off when those hash to it. `set_at_ms` is the Supabase server's clock and `accepted_at_ms` the engine
   host's, so the cut-offs tried are `accepted_at_ms` and every distinct `set_at_ms` within `subset_skew_ms` (5 min)
   of it (at most `SUBSET_CUTOFFS`); the hash comparison is the check, the cut-off only finds the rows (rows added
   after the accept stay pending).
6. **Install at `T + post_grace`** (the first bar end `T` of the 15m grid whose `T + post_grace` is after now), never
   mid-bar: at that time the latest accepted hash is read again and must still equal the candidate's; then the cache
   (document, the candidate's rows added to the seen chain) is saved (fenced, `must=False`), an `installed` audit row
   is written and the candidate becomes `accepted()`; the `config_hash` pause ends. A `config_hash` pause still held
   after the first install slot following the accept (or at start-up, on an older accept) pages `high` at once
   (`ARM paused past its install slot`, round 12 / S38c), so no later Supabase row can silently pause advice.
   **The pause is bounded** (S38c review): `pause_bound_slots` (4) install slots after that point, while a document
   is installed, the state becomes HELD: the pause ends and advice resumes under the installed document (the tracker
   already arms and ends under it during the pause, and every armed position keeps its pinned strategy and guards;
   a configuration change never re-pins a position), the accepted document stays a candidate that installs at its
   next slot once a read yields it, and a `high` page says so once per accepted hash. With nothing installed the
   pause is not bounded (there is no document to keep in force).

Automatic acceptance (Q12 "automatic") is not built: an `arm_accepted` row with mode `automatic` is treated as a
manual accept of its hash.

Pages, logs and audit rows name rows only as `table:id:hash` and carry fixed codes and counts, never row text, a
DSN, host or password (§B9). A lost fence (the injected `fatal` predicate) raises; every other error is counted and
retried at the next poll, so the task never dies with a driver message.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Final, Protocol

from src.armconfig.chain import ChainError, ChainState, ChainVerdict, SeenRows, TamperedRow, check_rows
from src.armconfig.client import ConfigSource, ConfigSourceError, SelfCheckError, safe_source_error
from src.armconfig.document import (
    SCHEMA_VERSION_KEY,
    TABLES,
    ConfigRow,
    Document,
    DocumentProblem,
    canonical_json,
    document_hash,
    int_value,
    parse_document,
)
from src.armconfig.validate import KnownIds, ValidationStatus, ValidDocument, validate_document
from src.db.arm_ports import AcceptedRow, ArmConfigStore, ConfigAuditRow, ConfigCacheRow, is_lost_fence
from src.riskmgr.registry import REGISTRY, ArmRegistry

log = logging.getLogger(__name__)

TASK: Final = "arm-config"
PAUSE_CONFIG_HASH: Final = "config_hash"
PAGE_REFS: Final = 10
MS_PER_DAY: Final = 86_400_000
SUBSET_CUTOFFS: Final = 64
"""At most this many `set_at_ms` cut-offs are tried for the accepted subset in one poll."""


@dataclass(frozen=True, kw_only=True)
class InstallerSettings:
    poll_s: float = 120.0
    read_timeout_s: float = 60.0
    store_timeout_s: float = 5.0
    post_grace_ms: int = 60_000
    bar_ms: int = 900_000
    pending_page_ms: int = MS_PER_DAY
    unreachable_page_ms: int = 900_000
    known_page_ms: int = 900_000
    subset_skew_ms: int = 300_000
    pause_bound_slots: int = 4
    """Install slots after the lingering page before a `config_hash` pause is bounded (HELD, S38c review)."""


class ConfigState(str, Enum):
    STARTING = "starting"          # the cache is not restored yet
    IDLE = "idle"                  # no accepted document
    PAUSED = "paused"              # the cached hash is not the latest accepted hash (config_hash)
    HELD = "held"                  # PAUSED past its bound: the installed document stays in force, no pause
    INSTALLED = "installed"


class ConfigPager(Protocol):
    def page(self, severity: str, title: str, body: str) -> None: ...


class ConfigStamp(Protocol):
    def stamp(self) -> None: ...

    def set_exempt(self, reason: str | None) -> None: ...


@dataclass
class ConfigCounters:
    polls: int = 0
    reads: int = 0
    read_errors: int = 0
    self_check_failures: int = 0
    tampered: int = 0
    invalid: int = 0
    schema_ahead: int = 0
    pending: int = 0
    installs: int = 0
    install_refused: int = 0
    store_errors: int = 0
    audit_errors: int = 0
    known_waits: int = 0


@dataclass
class _Scheduled:
    doc: ValidDocument
    rows: tuple[ConfigRow, ...] = field(repr=False)
    due_ms: int = 0


def install_due_ms(now_ms: int, bar_ms: int, post_grace_ms: int) -> int:
    """The first `T + post_grace` after `now_ms`, `T` a bar end on the UTC grid of `bar_ms`."""
    t = ((now_ms - post_grace_ms) // bar_ms + 1) * bar_ms
    return t + post_grace_ms


def document_json(schema_version: int, rows: tuple[ConfigRow, ...]) -> str:
    """The cache's `document` text: the canonical JSON of the raw document (re-reading it gives the same hashes)."""
    raw: dict[str, object] = {SCHEMA_VERSION_KEY: schema_version}
    for table in TABLES:
        raw[table] = [r.columns() for r in rows if r.table == table]
    return canonical_json(raw)


def _set_at(row: ConfigRow) -> int:
    return int_value(row.columns().get("set_at_ms"), 0, 2**63 - 1) or 0


def subset_document(doc: Document, at_ms: int) -> Document:
    """The rows of `doc` set at or before `at_ms` (the append-only guard's `set_at_ms`), as their own document."""
    rows = tuple(r for r in doc.rows if _set_at(r) <= at_ms)
    return Document(schema_version=doc.schema_version, rows=rows, doc_hash=document_hash(doc.schema_version, rows))


def accepted_subset(doc: Document, doc_hash: str, accepted_at_ms: int, skew_ms: int,
                    limit: int = SUBSET_CUTOFFS) -> Document | None:
    """The subset of `doc` (rows set at or before a cut-off) that hashes to `doc_hash`, else None. The cut-offs are
    `accepted_at_ms` (engine host clock) and each distinct `set_at_ms` (Supabase server clock) within `skew_ms` of
    it, nearest first, at most `limit` of them: clock skew between the two hosts only changes which cut-off finds
    the rows, never what installs (the hash decides)."""
    near = {t for t in (_set_at(r) for r in doc.rows) if abs(t - accepted_at_ms) <= skew_ms}
    cutoffs = [accepted_at_ms, *sorted(near - {accepted_at_ms}, key=lambda t: (abs(t - accepted_at_ms), t))]
    tried: set[tuple[str, ...]] = set()
    for cut in cutoffs[:limit]:
        sub = subset_document(doc, cut)
        refs = tuple(r.ref for r in sub.rows)
        if refs in tried:
            continue
        tried.add(refs)
        if sub.doc_hash == doc_hash:
            return sub
    return None


def _audit_refs(doc: Document, refs: tuple[str, ...]) -> tuple[str, ...]:
    hashes = doc.row_hashes()
    return tuple(f"{r}:{hashes[r]}" if r in hashes else r for r in refs)


def _tamper_refs(tampered: tuple[TamperedRow, ...], seen: dict[str, str]) -> tuple[str, ...]:
    return tuple(f"{t.ref}:{seen[t.ref]}" if t.ref in seen else t.ref for t in tampered)


class ConfigInstaller:
    """The installer and the `AcceptedConfigPort` (module docstring)."""

    def __init__(self, *, source: ConfigSource, store: ArmConfigStore, pager: ConfigPager,
                 known: Callable[[], KnownIds | None], clock_ms: Callable[[], int], node: str,
                 fatal: Callable[[BaseException], bool] = is_lost_fence, registry: ArmRegistry = REGISTRY,
                 settings: InstallerSettings | None = None) -> None:
        self._source = source
        self._store = store
        self._pager = pager
        self._known = known
        self._clock_ms = clock_ms
        self._node = node
        self._fatal = fatal
        self._registry = registry
        self.settings = settings or InstallerSettings()
        self.counters = ConfigCounters()
        self.state = ConfigState.STARTING
        self._installed: ValidDocument | None = None
        self._cache: ConfigCacheRow | None = None
        self._accepted: AcceptedRow | None = None
        self._scheduled: _Scheduled | None = None
        self._paged: set[str] = set()
        self._audited: set[str] = set()
        self._unreachable_since: int | None = None
        self._pending_since: int | None = None
        self._known_missing_since: int | None = None
        self._restored = False
        self.last_read_ms: int | None = None

    # ------------------------------------------------------------------------------------------------ the port
    def accepted(self) -> ValidDocument | None:
        """The installed (cached accepted) document; None while there is none (idle or not restored)."""
        if self.state in (ConfigState.STARTING, ConfigState.IDLE):
            return None
        return self._installed

    def pause_reason(self) -> str | None:
        """`config_hash` while PAUSED, else None (HELD is a bounded pause that has ended, S38c review). The BarClock's
        `config_pause` port (§B13: no decision, no ARM notice, no D7 counting); the operator pause (`arm_controls`) is
        separate and read by the BarClock."""
        return PAUSE_CONFIG_HASH if self.state is ConfigState.PAUSED else None

    def installed_hash(self) -> str | None:
        doc = self._installed
        return None if doc is None else doc.doc_hash

    def scheduled_due_ms(self) -> int | None:
        s = self._scheduled
        return None if s is None else s.due_ms

    def snapshot(self) -> tuple[tuple[str, int], ...]:
        """Counters for `arm_status` (`config_*`, counts only), with the state as one flag per state
        (`config_state_<state>`: 1 for the current one)."""
        c = self.counters
        pairs = [("polls", c.polls), ("reads", c.reads), ("read_errors", c.read_errors),
                 ("self_check_failures", c.self_check_failures), ("tampered", c.tampered), ("invalid", c.invalid),
                 ("schema_ahead", c.schema_ahead), ("pending", c.pending), ("installs", c.installs),
                 ("install_refused", c.install_refused), ("store_errors", c.store_errors),
                 ("audit_errors", c.audit_errors), ("known_waits", c.known_waits)]
        pairs.extend((f"state_{st.value}", int(st is self.state)) for st in ConfigState)
        return tuple(sorted((f"config_{k}", v) for k, v in pairs))

    # ------------------------------------------------------------------------------------------------ helpers
    def _page_once(self, key: str, severity: str, title: str, body: str) -> None:
        if key in self._paged:
            return
        self._paged.add(key)
        try:
            self._pager.page(severity, title, body[:1500])
        except Exception as exc:
            log.warning("%s: page not sent (%s)", TASK, safe_source_error(exc))

    async def _guard(self, what: str, work: Callable[[], Awaitable[None]]) -> bool:
        """Run a store call; a lost fence raises, any other error is counted and logged by class only."""
        try:
            await asyncio.wait_for(work(), self.settings.store_timeout_s)
            return True
        except Exception as exc:
            if self._fatal(exc):
                raise
            self.counters.store_errors += 1
            log.warning("%s: %s failed (%s)", TASK, what, safe_source_error(exc))
            return False

    async def _audit(self, key: str, action: str, doc_hash: str | None, refs: tuple[str, ...]) -> None:
        """One `arm_config_audit` row per distinct event (`key`), named by id and hash only."""
        if key in self._audited:
            return
        row = ConfigAuditRow(at_ms=self._clock_ms(), node=self._node, action=action, doc_hash=doc_hash,
                             row_refs=refs[:200])
        try:
            await asyncio.wait_for(self._store.audit(row), self.settings.store_timeout_s)
            self._audited.add(key)
        except Exception as exc:
            if self._fatal(exc):
                raise
            self.counters.audit_errors += 1
            log.warning("%s: audit row not written (%s)", TASK, safe_source_error(exc))

    async def _read_local(self) -> bool:
        """The cache and the latest accepted row; False when either is unreadable (state kept)."""
        try:
            cache = await asyncio.wait_for(self._store.cached(), self.settings.store_timeout_s)
            accepted = await asyncio.wait_for(self._store.latest_accepted(), self.settings.store_timeout_s)
        except Exception as exc:
            if self._fatal(exc):
                raise
            self.counters.store_errors += 1
            log.warning("%s: local config state unreadable (%s)", TASK, safe_source_error(exc))
            return False
        self._cache, self._accepted = cache, accepted
        return True

    def _seen(self) -> tuple[tuple[tuple[str, str], ...], str, int]:
        cache = self._cache
        if cache is None:
            empty = SeenRows()
            return (), empty.head, 0
        return cache.row_hashes, cache.chain_head, cache.chain_rows

    # ------------------------------------------------------------------------------------------------ start-up
    def _restore(self, known: KnownIds, now: int) -> None:
        """Restore the cached document once (when the known ids are ready); `_settle` then sets the state."""
        cache = self._cache
        if cache is not None and self._installed is None and not self._restored:
            self._installed = self._decode_cache(cache, known, now)
        self._restored = True

    def _settle(self) -> None:
        """IDLE without an accepted row; INSTALLED when the installed hash is the latest accepted one; while an
        install of the accepted hash is scheduled the state is kept (the installed document stays in force until
        `T + post_grace`); otherwise PAUSED (`config_hash`, §B9 / §B13)."""
        accepted = self._accepted
        if accepted is None:
            self.state = ConfigState.IDLE
            self._page_once("idle", "warning", "ARM idle: no accepted configuration",
                            "no arm_accepted row: the ARM arms nothing until `arm config accept <hash>`")
            return
        if self.installed_hash() == accepted.doc_hash:
            self.state = ConfigState.INSTALLED
            return
        held = self._scheduled
        if held is not None and held.doc.doc_hash == accepted.doc_hash and self.state in (ConfigState.INSTALLED,
                                                                                            ConfigState.HELD):
            return
        now = self._clock_ms()
        if self._installed is not None and now >= self.bound_after_ms(accepted):
            self.state = ConfigState.HELD
            self._page_once(f"config_hash_bounded:{accepted.doc_hash}", "high",
                            "ARM pause bounded: installed configuration kept",
                            f"the accepted document {accepted.doc_hash[:12]} is still not installed "
                            f"{self.settings.pause_bound_slots} install slots after its first slot; advice resumes "
                            f"under the installed document {(self.installed_hash() or '')[:12]} (armed positions keep "
                            "their pins) and the accepted one installs once a read yields it. Check `arm config show`")
            return
        self.state = ConfigState.PAUSED
        self._page_once(f"config_hash:{accepted.doc_hash}", "warning", "ARM paused: configuration hash",
                        f"the cached document {(self.installed_hash() or 'none')[:12]} is not the latest accepted "
                        f"{accepted.doc_hash[:12]}; no decision until it is installed")
        if now >= self.lingering_after_ms(accepted):
            self._page_once(f"config_hash_lingering:{accepted.doc_hash}", "high",
                            "ARM paused past its install slot",
                            f"the accepted document {accepted.doc_hash[:12]} is still not installed after its first "
                            "install slot; no advice until it is. Check `arm config show` for an invalid, tampered or "
                            "pending read and the arm-config counters")

    def lingering_after_ms(self, accepted: AcceptedRow) -> int:
        """When a `config_hash` pause has outlived the first install slot after the accept (round 12, plan S38c):
        the first `T + post_grace` after the accept plus one poll (the poll that finds the candidate), plus one more
        poll for a retried install. At start-up a pause on an older accept is past it at once. A pause still held
        then pages `high` once per accepted hash, so a row added after an accept, or any other cause, can never
        silently pause all advice for hours (the warning page above goes out at once; `paused > 6 h` stays)."""
        poll_ms = int(self.settings.poll_s * 1000)
        slot = install_due_ms(accepted.accepted_at_ms + poll_ms, self.settings.bar_ms, self.settings.post_grace_ms)
        return slot + poll_ms

    def bound_after_ms(self, accepted: AcceptedRow) -> int:
        """When a `config_hash` pause is bounded (HELD, S38c review): `pause_bound_slots` install slots after
        `lingering_after_ms`."""
        return self.lingering_after_ms(accepted) + self.settings.pause_bound_slots * self.settings.bar_ms

    def _decode_cache(self, cache: ConfigCacheRow, known: KnownIds, now: int) -> ValidDocument | None:
        try:
            raw = json.loads(cache.document_json, parse_float=Decimal, parse_int=int)
        except (ValueError, RecursionError):
            raw = None
        doc = parse_document(raw)
        if isinstance(doc, DocumentProblem) or doc.doc_hash != cache.doc_hash:
            self._page_once(f"cache:{cache.doc_hash}", "high", "ARM configuration cache unreadable",
                            f"the cached document does not hash to {cache.doc_hash[:12]}; it is not used")
            return None
        result = validate_document(doc, known, self._registry, seen=dict(cache.row_hashes), at_ms=now)
        if result.document is None:
            codes = "; ".join(p.page_text for p in result.problems[:PAGE_REFS])
            self._page_once(f"cache_invalid:{cache.doc_hash}", "high", "ARM cached configuration refused",
                            f"document {cache.doc_hash[:12]}: {codes}")
            return None
        return result.document

    # ------------------------------------------------------------------------------------------------ one poll
    async def poll(self) -> None:
        """One poll (module docstring). Never raises except on a lost fence."""
        self.counters.polls += 1
        now = self._clock_ms()
        if not await self._read_local():
            return
        known = self._known()
        if known is None:
            self._known_missing(now)
            return
        if self._known_missing_since is not None:
            log.info("%s: known ids loaded", TASK)
            self._paged.discard("known_ids")
        self._known_missing_since = None
        self._restore(known, now)
        try:
            await self._check_source(known, now)
        finally:
            self._settle()

    def _known_missing(self, now: int) -> None:
        """The known ids are not loaded (an adapter's instrument map is missing): nothing is restored or checked; one
        page once that has lasted `known_page_ms` (an adapter stuck in AUTH_FAILED would otherwise keep the ARM
        configuration in STARTING, unseen, for the whole term)."""
        self.counters.known_waits += 1
        if self._known_missing_since is None:
            self._known_missing_since = now
        waited = now - self._known_missing_since
        log.info("%s: known ids not loaded yet; nothing checked", TASK)
        if waited >= self.settings.known_page_ms:
            self._page_once("known_ids", "high", "ARM configuration waiting for instruments",
                            f"an adapter's instrument map has not loaded for {waited // 60_000} min "
                            f"(state {self.state.value}); no configuration is restored, checked or installed")

    async def _check_source(self, known: KnownIds, now: int) -> None:
        doc = await self._read_source(now)
        if doc is None:
            return
        if isinstance(doc, DocumentProblem):
            self.counters.invalid += 1
            key = f"read:{doc.code}:{doc.table}:{doc.row_id}"
            self._page_once(key, "high", "ARM configuration read refused",
                            f"{doc.page_text}; the accepted document stays in force")
            await self._audit(key, "read_refused", None, (doc.page_text,))
            return
        stored_rows, head, count = self._seen()
        verdict = check_rows(stored_rows, head, count, doc.row_hashes(), schema_ahead=doc.schema_ahead)
        if verdict.state is ChainState.TAMPERED:
            await self._tampered(doc, verdict)
            return
        if doc.schema_ahead:
            self.counters.schema_ahead += 1
            key = f"schema_ahead:{doc.schema_version}"
            self._page_once(key, "high", "ARM configuration schema ahead",
                            f"arm.schema_version {doc.schema_version} is newer than this engine reads; "
                            "the accepted document stays in force")
            await self._audit(key, "schema_ahead", None, ())
            return
        seen = dict(stored_rows)
        result = validate_document(doc, known, self._registry, seen=seen, at_ms=now)
        if result.status is not ValidationStatus.VALID or result.document is None:
            self.counters.invalid += 1
            key = f"invalid:{doc.doc_hash}"
            codes = "; ".join(p.page_text for p in result.problems[:PAGE_REFS])
            self._page_once(key, "high", "ARM configuration invalid",
                            f"document {doc.doc_hash[:12]}: {codes}; the accepted document stays in force")
            await self._audit(key, "invalid", doc.doc_hash,
                              tuple(p.ref for p in result.problems if p.row_hash))
            await self._consider_subset(doc, known, seen, now)
            return
        self._pending(verdict, doc, now)
        await self._consider(doc, result.document, known, seen, now)

    async def _consider_subset(self, doc: Document, known: KnownIds, seen: dict[str, str], now: int) -> None:
        """The full read is invalid: its accepted subset may still install (rows added after the accept, valid or
        not, never block the accepted document)."""
        accepted = self._accepted
        if accepted is None or doc.doc_hash == accepted.doc_hash:
            return
        sub = accepted_subset(doc, accepted.doc_hash, accepted.accepted_at_ms, self.settings.subset_skew_ms)
        if sub is None:
            return
        checked = validate_document(sub, known, self._registry, seen=seen, at_ms=now)
        if checked.status is ValidationStatus.VALID and checked.document is not None:
            await self._consider(sub, checked.document, known, seen, now)

    async def _read_source(self, now: int) -> Document | DocumentProblem | None:
        try:
            doc = await asyncio.wait_for(self._source.read(), self.settings.read_timeout_s)
        except SelfCheckError as exc:
            self.counters.self_check_failures += 1
            self._unreachable(now, exc.stage)
            self._page_once(f"self_check:{','.join(exc.problems)}", "high", "ARM configuration self-check failed",
                            f"{', '.join(exc.problems[:20])}; the accepted document stays in force")
            return None
        except ConfigSourceError as exc:
            self.counters.read_errors += 1
            if exc.stage == "tls_ca":           # a configuration fault, not an outage: page at once
                self._page_once("tls_ca", "high", "ARM configuration CA unusable",
                                f"XTNL_ARM_CONFIG_CA_FILE could not be loaded ({exc.reason}); "
                                "nothing is read, the accepted document stays in force")
            self._unreachable(now, exc.stage)
            log.info("%s: read failed (%s)", TASK, str(exc))
            return None
        except Exception as exc:
            if self._fatal(exc):
                raise
            self.counters.read_errors += 1
            reason = "timeout" if isinstance(exc, TimeoutError) else safe_source_error(exc)
            self._unreachable(now, reason)
            log.info("%s: read failed (%s)", TASK, reason)
            return None
        self.counters.reads += 1
        self.last_read_ms = now
        if self._unreachable_since is not None:
            log.info("%s: configuration source reachable again", TASK)
            self._paged.discard("unreachable")
        self._unreachable_since = None
        return doc

    def _unreachable(self, now: int, reason: str) -> None:
        if self._unreachable_since is None:
            self._unreachable_since = now
        if now - self._unreachable_since >= self.settings.unreachable_page_ms:
            minutes = (now - self._unreachable_since) // 60_000
            self._page_once("unreachable", "high", "ARM configuration source unreachable",
                            f"no configuration read for {minutes} min ({reason}); the accepted document stays in force")

    async def _tampered(self, doc: Document, verdict: ChainVerdict) -> None:
        self.counters.tampered += 1
        refs = _tamper_refs(verdict.tampered, dict(self._seen()[0]))
        kinds = sorted({t.kind.value for t in verdict.tampered})
        key = "tampered:" + ",".join(refs)
        self._page_once(key, "critical", "ARM configuration tampered",
                        f"{len(refs)} seen row(s) {', '.join(kinds)}: {', '.join(refs[:PAGE_REFS])}; "
                        "the accepted document stays in force and nothing installs")
        await self._audit(key, "tampered", doc.doc_hash, refs)
        self._scheduled = None

    def _pending(self, verdict: ChainVerdict, doc: Document, now: int) -> None:
        accepted = self._accepted
        if verdict.state is not ChainState.PENDING or (accepted is not None and accepted.doc_hash == doc.doc_hash):
            self._pending_since = None
            return
        self.counters.pending += 1
        if self._pending_since is None:
            self._pending_since = now
        if now - self._pending_since >= self.settings.pending_page_ms:
            self._page_once(f"pending:{self._pending_since}", "warning", "ARM configuration pending for a day",
                            f"{len(verdict.pending)} new row(s) not accepted: "
                            f"{', '.join(_audit_refs(doc, verdict.pending)[:PAGE_REFS])}; "
                            f"`arm config show --pending` then `arm config accept {doc.doc_hash}`")

    # ------------------------------------------------------------------------------------------------ install
    async def _consider(self, doc: Document, valid: ValidDocument, known: KnownIds, seen: dict[str, str],
                        now: int) -> None:
        accepted = self._accepted
        if accepted is None:
            self._scheduled = None
            return
        candidate: tuple[ValidDocument, tuple[ConfigRow, ...]] | None = None
        if doc.doc_hash == accepted.doc_hash:
            candidate = (valid, doc.rows)
        else:
            sub = accepted_subset(doc, accepted.doc_hash, accepted.accepted_at_ms, self.settings.subset_skew_ms)
            if sub is not None:
                checked = validate_document(sub, known, self._registry, seen=seen, at_ms=now)
                if checked.document is not None:
                    candidate = (checked.document, sub.rows)
        if candidate is None:
            self._scheduled = None
            return
        cand, rows = candidate
        if cand.doc_hash == self.installed_hash():
            self._scheduled = None
            if self.state is not ConfigState.INSTALLED:
                self.state = ConfigState.INSTALLED
            return
        held = self._scheduled
        if held is None or held.doc.doc_hash != cand.doc_hash:
            due = install_due_ms(now, self.settings.bar_ms, self.settings.post_grace_ms)
            self._scheduled = _Scheduled(cand, rows, due)
            log.info("%s: accepted document %s installs at %d", TASK, cand.doc_hash[:12], due)
        else:
            held.doc, held.rows = cand, rows
        await self.install_due()

    async def install_due(self) -> bool:
        """Install the scheduled candidate when its time has come and it is still the latest accepted document (at
        install the hashes are compared again, §B9); the state is settled afterwards."""
        try:
            return await self._install_due()
        finally:
            if self._restored:
                self._settle()

    async def _install_due(self) -> bool:
        held = self._scheduled
        now = self._clock_ms()
        if held is None or now < held.due_ms:
            return False
        if not await self._read_local():
            return False
        accepted = self._accepted
        if accepted is None or accepted.doc_hash != held.doc.doc_hash:
            self.counters.install_refused += 1
            self._scheduled = None
            log.info("%s: scheduled document is no longer the latest accepted; not installed", TASK)
            return False
        stored_rows, _head, _count = self._seen()
        try:
            seen = SeenRows.build(stored_rows).extend({r.ref: r.row_hash for r in held.rows})
        except ChainError:
            self.counters.install_refused += 1
            self._scheduled = None
            self._page_once(f"chain:{held.doc.doc_hash}", "critical", "ARM configuration tampered",
                            "a seen row changed before the install; nothing installs")
            return False
        row = ConfigCacheRow(doc_hash=held.doc.doc_hash, schema_version=held.doc.schema_version,
                             document_json=document_json(held.doc.schema_version, held.rows),
                             row_hashes=seen.rows, chain_head=seen.head, chain_rows=seen.count, updated_at_ms=now)
        saved = False

        async def save() -> None:
            nonlocal saved
            saved = await self._store.save_cache(row)

        if not await self._guard("cache save", save) or not saved:
            log.warning("%s: cache not saved; the install is retried", TASK)
            return False
        self._cache = row
        self._installed = held.doc
        self._scheduled = None
        self.state = ConfigState.INSTALLED
        self.counters.installs += 1
        for key in [k for k in self._paged if k.startswith(("config_hash:", "config_hash_lingering:")) or k == "idle"]:
            self._paged.discard(key)
        await self._audit(f"installed:{held.doc.doc_hash}:{now}", "installed", held.doc.doc_hash,
                          tuple(f"{r.ref}:{r.row_hash}" for r in held.rows))
        log.info("%s: document %s installed", TASK, held.doc.doc_hash[:12])
        return True

    # ------------------------------------------------------------------------------------------------ the loop
    async def run(self, stop: asyncio.Event, stamp: ConfigStamp) -> None:
        """The task body: a poll every `poll_s`, the scheduled install at its time; stamped between slices; a lost
        fence raises (the process exits), any other error is counted and the loop goes on."""
        next_poll = 0.0
        loop_now = _monotonic
        while not stop.is_set():
            stamp.stamp()
            try:
                if loop_now() >= next_poll:
                    stamp.set_exempt("postgres")
                    try:
                        await self.poll()
                    finally:
                        stamp.set_exempt(None)
                    next_poll = loop_now() + self.settings.poll_s
                elif self._scheduled is not None:
                    stamp.set_exempt("postgres")
                    try:
                        await self.install_due()
                    finally:
                        stamp.set_exempt(None)
            except Exception as exc:
                if self._fatal(exc):
                    raise
                self.counters.store_errors += 1
                log.warning("%s: poll failed (%s)", TASK, safe_source_error(exc))
                next_poll = loop_now() + self.settings.poll_s
            stamp.stamp()
            wait = max(0.0, next_poll - loop_now())
            due = self.scheduled_due_ms()
            if due is not None:
                wait = min(wait, max(0.0, (due - self._clock_ms()) / 1000))
            try:
                await asyncio.wait_for(stop.wait(), timeout=min(max(wait, 0.01), 1.0))
            except TimeoutError:
                pass


def _monotonic() -> float:
    return time.monotonic()


__all__ = [
    "ConfigCounters", "ConfigInstaller", "ConfigPager", "ConfigStamp", "ConfigState", "InstallerSettings",
    "PAUSE_CONFIG_HASH", "SUBSET_CUTOFFS", "TASK", "accepted_subset", "document_json", "install_due_ms",
    "subset_document",
]
