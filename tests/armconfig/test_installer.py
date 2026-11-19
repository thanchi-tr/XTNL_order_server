"""The `arm-config` installer (plan S33): accept-only install at `T + post_grace`, the config-hash pause, idle,
tamper, schema ahead, pending, unreachable and self-check pages, and no secret in any log, page or audit row."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

import pytest

from src.armconfig.client import ConfigSourceError, SelfCheckError, SourceSettings, SupabaseConfigSource
from src.armconfig.document import Document, DocumentProblem
from src.armconfig.installer import (
    PAUSE_CONFIG_HASH,
    ConfigInstaller,
    ConfigState,
    InstallerSettings,
    accepted_subset,
    install_due_ms,
    subset_document,
)
from src.armconfig.validate import KnownIds
from src.db.arm_ports import AcceptedRow, ConfigAuditRow, ConfigCacheRow, ControlsRow
from src.db.fence import EpochMismatch
from tests.armconfig.docs import KNOWN, META, assignment_row, parsed, raw_doc, route_row, strategy_row

BAR = 900_000
T = 1_700_000_100_000 // BAR * BAR          # a bar end on the 15m grid
DSN = "postgresql://xtnl_arm_reader:Pl4nted-Secret@db.planted-host.example:6543/postgres"
SECRETS = ("Pl4nted-Secret", "planted-host", DSN)


@dataclass
class Store:
    cache: ConfigCacheRow | None = None
    accepted_rows: list[AcceptedRow] = field(default_factory=list)
    audits: list[ConfigAuditRow] = field(default_factory=list)
    fail: Exception | None = None
    saves: int = 0

    async def cached(self) -> ConfigCacheRow | None:
        if self.fail is not None:
            raise self.fail
        return self.cache

    async def save_cache(self, row: ConfigCacheRow) -> bool:
        if self.fail is not None:
            raise self.fail
        self.cache = row
        self.saves += 1
        return True

    async def audit(self, row: ConfigAuditRow) -> bool:
        self.audits.append(row)
        return True

    async def latest_accepted(self) -> AcceptedRow | None:
        return self.accepted_rows[-1] if self.accepted_rows else None

    async def controls(self) -> ControlsRow | None:
        return None

    def accept(self, doc: Document, at_ms: int) -> None:
        self.accepted_rows.append(AcceptedRow(accepted_id=len(self.accepted_rows) + 1, doc_hash=doc.doc_hash,
                                              schema_version=doc.schema_version, mode="manual",
                                              accepted_at_ms=at_ms, accepted_by="operator"))


class Source:
    def __init__(self, doc: Document | DocumentProblem | None = None) -> None:
        self.doc = doc
        self.error: Exception | None = None
        self.reads = 0

    async def read(self) -> Document | DocumentProblem:
        self.reads += 1
        if self.error is not None:
            raise self.error
        assert self.doc is not None
        return self.doc


class Pages:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []

    def page(self, severity: str, title: str, body: str) -> None:
        self.sent.append((severity, title, body))

    def titles(self) -> list[str]:
        return [t for _s, t, _b in self.sent]


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


@dataclass
class Rig:
    inst: ConfigInstaller
    store: Store
    source: Source
    pages: Pages
    clock: Clock


def rig(doc: Document | DocumentProblem | None = None, *, store: Store | None = None, now: int = T + 30_000,
        known: KnownIds | None = KNOWN) -> Rig:
    st = store or Store()
    src = Source(doc)
    pages = Pages()
    clock = Clock(now)
    inst = ConfigInstaller(source=src, store=st, pager=pages, known=lambda: known, clock_ms=clock, node="n1",
                           settings=InstallerSettings(poll_s=0.01))
    return Rig(inst, st, src, pages, clock)


DOC_A = parsed(raw_doc())


async def installed_rig(doc: Document = DOC_A) -> Rig:
    r = rig(doc)
    r.store.accept(doc, T)
    await r.inst.poll()
    r.clock.now = T + 60_000
    assert await r.inst.install_due()
    assert r.inst.state is ConfigState.INSTALLED
    return r


# ---------------------------------------------------------------------------------------------------- idle, install
def test_install_due_is_the_first_bar_end_plus_post_grace_after_now() -> None:
    assert install_due_ms(T + 30_000, BAR, 60_000) == T + 60_000
    assert install_due_ms(T + 60_000, BAR, 60_000) == T + BAR + 60_000
    assert install_due_ms(T - 1, BAR, 60_000) == T + 60_000


async def test_no_accepted_row_is_idle_with_one_page() -> None:
    r = rig(DOC_A)
    await r.inst.poll()
    await r.inst.poll()
    assert r.inst.state is ConfigState.IDLE and r.inst.accepted() is None
    assert r.pages.titles() == ["ARM idle: no accepted configuration"]
    assert r.store.saves == 0


async def test_an_accepted_document_installs_only_at_t_plus_post_grace() -> None:
    r = rig(DOC_A)
    r.store.accept(DOC_A, T)
    await r.inst.poll()
    # nothing installed yet at start-up: the ARM pauses on the config hash until the install
    assert r.inst.state is ConfigState.PAUSED and r.inst.pause_reason() == PAUSE_CONFIG_HASH
    assert r.inst.accepted() is None and r.inst.scheduled_due_ms() == T + 60_000
    r.clock.now = T + 59_999
    assert not await r.inst.install_due()
    r.clock.now = T + 60_000
    assert await r.inst.install_due()
    doc = r.inst.accepted()
    assert doc is not None and doc.doc_hash == DOC_A.doc_hash and r.inst.pause_reason() is None
    cache = r.store.cache
    assert cache is not None and cache.doc_hash == DOC_A.doc_hash and cache.chain_rows == 3
    assert dict(cache.row_hashes) == DOC_A.row_hashes()
    (audit,) = [a for a in r.store.audits if a.action == "installed"]
    assert audit.doc_hash == DOC_A.doc_hash and all(ref.count(":") == 2 for ref in audit.row_refs)
    # the next poll changes nothing
    await r.inst.poll()
    assert r.store.saves == 1 and r.inst.scheduled_due_ms() is None


async def test_a_restart_restores_the_cache_when_its_hash_is_the_latest_accepted() -> None:
    first = await installed_rig()
    r = rig(DOC_A, store=first.store, now=T + 2 * BAR)
    await r.inst.poll()
    assert r.inst.state is ConfigState.INSTALLED and r.inst.installed_hash() == DOC_A.doc_hash
    assert r.inst.accepted() is not None and r.store.saves == 1


async def test_cache_hash_not_the_latest_accepted_pauses_until_the_new_one_installs() -> None:
    first = await installed_rig()
    doc_b = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    first.store.accept(doc_b, T + BAR)
    r = rig(doc_b, store=first.store, now=T + BAR + 30_000)
    await r.inst.poll()
    assert r.inst.state is ConfigState.PAUSED and r.inst.pause_reason() == PAUSE_CONFIG_HASH
    assert r.inst.installed_hash() == DOC_A.doc_hash                     # the cached document stays for arming
    assert "ARM paused: configuration hash" in r.pages.titles()
    r.clock.now = T + BAR + 60_000
    assert await r.inst.install_due()
    assert r.inst.state is ConfigState.INSTALLED and r.inst.installed_hash() == doc_b.doc_hash


async def test_a_new_accept_keeps_the_installed_document_until_its_bar_boundary() -> None:
    r = await installed_rig()
    doc_b = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    r.store.accept(doc_b, T + 100_000)
    r.source.doc = doc_b
    r.clock.now = T + 100_000
    await r.inst.poll()
    assert r.inst.state is ConfigState.INSTALLED and r.inst.installed_hash() == DOC_A.doc_hash
    assert r.inst.scheduled_due_ms() == T + BAR + 60_000
    r.clock.now = T + BAR + 60_000
    assert await r.inst.install_due() and r.inst.installed_hash() == doc_b.doc_hash


async def test_rows_added_after_the_accept_stay_pending_and_the_accepted_subset_installs() -> None:
    late = {**META, "set_at_ms": T + 500_000}
    doc_ax = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    doc_axy = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B"),
                                          {**assignment_row(3, symbol="SYN.A", adapter="mock"), **late}]))
    assert subset_document(doc_axy, T + 10_000).doc_hash == doc_ax.doc_hash
    r = rig(doc_axy)
    r.store.accept(doc_ax, T + 10_000)
    await r.inst.poll()
    r.clock.now = T + 60_000
    assert await r.inst.install_due() and r.inst.installed_hash() == doc_ax.doc_hash
    cache = r.store.cache
    assert cache is not None and "assignments:3" not in dict(cache.row_hashes)   # never seen: still pending
    before = r.inst.counters.pending
    for k in (1, 2):
        r.clock.now = T + 60_000 + k * 120_000
        await r.inst.poll()
        assert r.inst.counters.pending == before + k                   # counted on every poll while pending
        cache = r.store.cache
        assert cache is not None and "assignments:3" not in dict(cache.row_hashes)
    assert r.store.saves == 1 and r.inst.installed_hash() == doc_ax.doc_hash
    assert r.inst.state is ConfigState.INSTALLED and r.inst.scheduled_due_ms() is None


async def test_the_accepted_subset_is_found_across_clock_skew_between_supabase_and_the_engine() -> None:
    # Supabase's clock is 10 s ahead of the engine's: the accepted row's set_at_ms is after accepted_at_ms
    skewed = {**META, "set_at_ms": T + 20_000}
    late = {**META, "set_at_ms": T + 500_000}
    doc_ax = parsed(raw_doc(assignments=[assignment_row(), {**assignment_row(2, symbol="SYN.B"), **skewed}]))
    doc_axy = parsed(raw_doc(assignments=[assignment_row(), {**assignment_row(2, symbol="SYN.B"), **skewed},
                                          {**assignment_row(3, symbol="SYN.A", adapter="mock"), **late}]))
    assert subset_document(doc_axy, T + 10_000).doc_hash != doc_ax.doc_hash
    found = accepted_subset(doc_axy, doc_ax.doc_hash, T + 10_000, 300_000)
    assert found is not None and found.doc_hash == doc_ax.doc_hash
    assert accepted_subset(doc_axy, doc_ax.doc_hash, T + 10_000, 5_000) is None   # outside the skew window
    r = rig(doc_axy)
    r.store.accept(doc_ax, T + 10_000)
    await r.inst.poll()
    r.clock.now = T + 60_000
    assert await r.inst.install_due() and r.inst.installed_hash() == doc_ax.doc_hash


# ---------------------------------------------------------------------------------------------------- invalid rows
TYPO_ROWS = [{**assignment_row(2, symbol="TYPO"), **{**META, "set_at_ms": T + 100_000}},
             {**assignment_row(3, symbol="TYPO", enabled=False, effective_from_ms=2_000),
              **{**META, "set_at_ms": T + 110_000}}]


async def test_a_typo_row_tombstoned_after_the_accept_never_blocks_a_later_install() -> None:
    r = await installed_rig()
    # the writer appends a typo and its tombstone (append-only: they can never be deleted), then a valid row
    with_typo = parsed(raw_doc(assignments=[assignment_row(), *TYPO_ROWS]))
    r.source.doc = with_typo
    r.clock.now = T + 200_000
    await r.inst.poll()
    assert "ARM configuration invalid" not in r.pages.titles()            # the typo is not live: nothing refused
    doc_b = parsed(raw_doc(assignments=[assignment_row(), *TYPO_ROWS,
                                        {**assignment_row(4, symbol="SYN.B"), **{**META, "set_at_ms": T + 300_000}}]))
    r.source.doc = doc_b
    r.store.accept(doc_b, T + 400_000)
    r.clock.now = T + 400_000
    await r.inst.poll()
    assert r.inst.scheduled_due_ms() == T + BAR + 60_000
    r.clock.now = T + BAR + 60_000
    assert await r.inst.install_due() and r.inst.installed_hash() == doc_b.doc_hash
    assert r.inst.state is ConfigState.INSTALLED and "ARM configuration invalid" not in r.pages.titles()


async def test_a_live_invalid_row_added_after_the_accept_still_lets_the_accepted_subset_install() -> None:
    doc_ax = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    bad = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B"),
                                      {**assignment_row(3, symbol="TYPO"), **{**META, "set_at_ms": T + 500_000}}]))
    r = rig(bad)
    r.store.accept(doc_ax, T + 10_000)
    await r.inst.poll()
    _s, _t, body = next(p for p in r.pages.sent if p[1] == "ARM configuration invalid")
    assert f"symbol_unknown assignments:3:{bad.row_hashes()['assignments:3']}" in body and "TYPO" not in body
    assert r.inst.scheduled_due_ms() == T + 60_000
    r.clock.now = T + 60_000
    assert await r.inst.install_due() and r.inst.installed_hash() == doc_ax.doc_hash
    cache = r.store.cache
    assert cache is not None and "assignments:3" not in dict(cache.row_hashes)
    # an accept of the invalid document itself never installs it
    r.store.accept(bad, T + BAR)
    r.clock.now = T + BAR + 30_000
    await r.inst.poll()
    r.clock.now = T + 2 * BAR + 60_000
    await r.inst.install_due()
    assert r.inst.installed_hash() == doc_ax.doc_hash and r.inst.state is ConfigState.PAUSED


async def test_pending_rows_never_install_and_page_after_a_day() -> None:
    r = await installed_rig()
    r.source.doc = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    for hours in (1, 12, 23):
        r.clock.now = T + hours * 3_600_000
        await r.inst.poll()
    assert "ARM configuration pending for a day" not in r.pages.titles()
    r.clock.now = T + 3_600_000 + 86_400_000
    await r.inst.poll()
    await r.inst.poll()
    assert r.pages.titles().count("ARM configuration pending for a day") == 1
    assert r.inst.installed_hash() == DOC_A.doc_hash and r.store.saves == 1


# ---------------------------------------------------------------------------------------------------- refusals
async def test_a_tampered_row_pages_at_once_and_keeps_the_accepted_document() -> None:
    r = await installed_rig()
    changed = parsed(raw_doc(strategies=[strategy_row(params={"guards": [], "x": 1})]))
    r.source.doc = changed
    r.clock.now = T + BAR
    await r.inst.poll()
    await r.inst.poll()
    sev, title, body = next(p for p in r.pages.sent if p[1] == "ARM configuration tampered")
    assert sev == "critical" and "strategies:1:" in body and "changed" in body
    assert r.pages.titles().count("ARM configuration tampered") == 1
    assert r.inst.installed_hash() == DOC_A.doc_hash and r.inst.state is ConfigState.INSTALLED
    (audit,) = [a for a in r.store.audits if a.action == "tampered"]
    assert audit.row_refs == (f"strategies:1:{DOC_A.row_hashes()['strategies:1']}",)
    # even a local accept of the tampered document never installs it
    r.store.accept(changed, T + BAR)
    await r.inst.poll()
    r.clock.now = T + 3 * BAR
    await r.inst.install_due()
    assert r.inst.installed_hash() == DOC_A.doc_hash
    # a vanished seen row is tampering too
    r.source.doc = parsed(raw_doc(route_policy=[]))
    await r.inst.poll()
    assert any("vanished" in b for _s, t, b in r.pages.sent if t == "ARM configuration tampered")


async def test_schema_ahead_pages_and_installs_nothing() -> None:
    r = await installed_rig()
    ahead = parsed(raw_doc(schema_version=2))
    r.source.doc = ahead
    r.store.accept(ahead, T + BAR)
    r.clock.now = T + BAR + 30_000
    await r.inst.poll()
    assert "ARM configuration schema ahead" in r.pages.titles()
    r.clock.now = T + 2 * BAR
    await r.inst.install_due()
    assert r.inst.installed_hash() == DOC_A.doc_hash


async def test_an_invalid_document_pages_by_row_ref_and_keeps_the_accepted_one() -> None:
    r = await installed_rig()
    bad = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="UNKNOWN.SYM")]))
    r.source.doc = bad
    r.clock.now = T + BAR
    await r.inst.poll()
    _s, _t, body = next(p for p in r.pages.sent if p[1] == "ARM configuration invalid")
    assert f"symbol_unknown assignments:2:{bad.row_hashes()['assignments:2']}" in body and "UNKNOWN.SYM" not in body
    assert r.inst.installed_hash() == DOC_A.doc_hash
    r.source.doc = DocumentProblem("row_too_large", "strategies", 9)
    await r.inst.poll()
    assert "ARM configuration read refused" in r.pages.titles()


async def test_known_ids_not_loaded_checks_nothing_and_pages_once_after_15_minutes() -> None:
    known: list[KnownIds | None] = [None]
    st = Store()
    st.accept(DOC_A, T)
    pages = Pages()
    clock = Clock(T + 30_000)
    inst = ConfigInstaller(source=Source(DOC_A), store=st, pager=pages, known=lambda: known[0], clock_ms=clock,
                           node="n1")
    for minutes in (0, 5, 14):
        clock.now = T + 30_000 + minutes * 60_000
        await inst.poll()
    assert inst.state is ConfigState.STARTING and inst.accepted() is None and pages.sent == []
    counters = dict(inst.snapshot())
    assert counters["config_state_starting"] == 1 and counters["config_known_waits"] == 3
    assert sum(v for k, v in counters.items() if k.startswith("config_state_")) == 1
    for minutes in (15, 20):
        clock.now = T + 30_000 + minutes * 60_000
        await inst.poll()
    assert [t for _s, t, _b in pages.sent] == ["ARM configuration waiting for instruments"]
    known[0] = KNOWN
    await inst.poll()
    counters = dict(inst.snapshot())
    assert counters["config_state_starting"] == 0 and counters["config_state_paused"] == 1


# ---------------------------------------------------------------------------------------------------- source failures
async def test_unreachable_for_15_minutes_pages_once_and_keeps_the_accepted_document() -> None:
    r = await installed_rig()
    r.source.error = ConfigSourceError("connect", "OSError")
    start = T + BAR
    for minutes in (0, 5, 10, 14):
        r.clock.now = start + minutes * 60_000
        await r.inst.poll()
    assert "ARM configuration source unreachable" not in r.pages.titles()
    for minutes in (15, 20):
        r.clock.now = start + minutes * 60_000
        await r.inst.poll()
    assert r.pages.titles().count("ARM configuration source unreachable") == 1
    assert r.inst.state is ConfigState.INSTALLED and r.inst.installed_hash() == DOC_A.doc_hash
    r.source.error = None
    await r.inst.poll()
    assert r.inst.counters.read_errors == 6 and r.inst.counters.reads >= 2


async def test_a_failed_self_check_pages_at_once_and_fails_closed() -> None:
    r = await installed_rig()
    r.source.error = SelfCheckError(("role_bypassrls", "rls_not_forced:strategies"))
    r.clock.now = T + BAR
    await r.inst.poll()
    _s, _t, body = next(p for p in r.pages.sent if p[1] == "ARM configuration self-check failed")
    assert "role_bypassrls" in body and r.inst.installed_hash() == DOC_A.doc_hash


async def test_an_unusable_pinned_ca_pages_at_once() -> None:
    r = await installed_rig()
    r.source.error = ConfigSourceError("tls_ca", "FileNotFoundError")
    r.clock.now = T + BAR
    await r.inst.poll()
    await r.inst.poll()
    assert r.pages.titles().count("ARM configuration CA unusable") == 1
    assert "ARM configuration source unreachable" not in r.pages.titles()
    _s, _t, body = next(p for p in r.pages.sent if p[1] == "ARM configuration CA unusable")
    assert "FileNotFoundError" in body and r.inst.installed_hash() == DOC_A.doc_hash


async def test_a_lost_fence_raises_and_other_store_errors_are_retried() -> None:
    r = rig(DOC_A)
    r.store.accept(DOC_A, T)
    r.store.fail = RuntimeError("store down")
    await r.inst.poll()
    assert r.inst.counters.store_errors == 1
    r.store.fail = EpochMismatch("fenced")
    with pytest.raises(EpochMismatch):
        await r.inst.poll()


# ---------------------------------------------------------------------------------------------------- no secrets
class PlantedError(Exception):
    sqlstate = "28P01"


async def test_a_planted_dsn_host_and_password_reach_no_log_page_or_audit_row(
        caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    planted = f"could not connect to {DSN}: password authentication failed for user at planted-host"
    mode: dict[str, Any] = {"connect": OSError(planted)}

    class Conn:
        async def execute(self, query: str, *args: Any) -> str:
            return "OK"

        async def fetch(self, query: str, *args: Any) -> list[Any]:
            raise PlantedError(planted)

        async def close(self) -> None:
            raise PlantedError(planted)

        def terminate(self) -> None:
            raise PlantedError(planted)

    async def connect(dsn: str, **_kw: Any) -> Conn:
        err = mode["connect"]
        if err is not None:
            raise err
        return Conn()

    import certifi

    source = SupabaseConfigSource(SourceSettings(dsn=DSN, ca_file=certifi.where()), connect)
    store = Store()
    store.accept(DOC_A, T)
    pages = Pages()
    clock = Clock(T + 30_000)
    inst = ConfigInstaller(source=source, store=store, pager=pages, known=lambda: KNOWN, clock_ms=clock, node="n1")
    for step in range(20):
        mode["connect"] = OSError(planted) if step % 2 == 0 else None
        await inst.poll()
        clock.now += 120_000
    assert "ARM configuration source unreachable" in [t for _s, t, _b in pages.sent]
    texts = [r.getMessage() for r in caplog.records] + [f"{s} {t} {b}" for s, t, b in pages.sent]
    texts += [repr(a) for a in store.audits] + [repr(inst.snapshot()), repr(source), repr(inst.counters)]
    for text in texts:
        for secret in SECRETS:
            assert secret not in text, (secret, text)
    assert any("PlantedError(28P01)" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------------------------------- the loop
async def test_run_polls_installs_at_its_time_and_stops() -> None:
    r = rig(DOC_A)
    r.store.accept(DOC_A, T)
    stamps: list[str | None] = []

    class Stamp:
        def stamp(self) -> None:
            stamps.append("s")

        def set_exempt(self, reason: str | None) -> None:
            stamps.append(reason)

    stop = asyncio.Event()
    task = asyncio.create_task(r.inst.run(stop, Stamp()))
    for _ in range(200):
        if r.inst.scheduled_due_ms() is not None:
            break
        await asyncio.sleep(0.005)
    assert r.inst.state is ConfigState.PAUSED
    r.clock.now = T + 60_000
    for _ in range(200):
        if r.inst.state is ConfigState.INSTALLED:
            break
        await asyncio.sleep(0.005)
    assert r.inst.installed_hash() == DOC_A.doc_hash and "postgres" in stamps
    stop.set()
    await asyncio.wait_for(task, 2.0)
    r.store.fail = EpochMismatch("fenced")
    with pytest.raises(EpochMismatch):
        await asyncio.wait_for(r.inst.run(asyncio.Event(), Stamp()), 2.0)


async def test_route_policy_rows_resolve_from_the_installed_document() -> None:
    doc = parsed(raw_doc(route_policy=[route_row(), route_row(2, route="second", mode="actionable")]))
    r = await installed_rig(doc)
    installed = r.inst.accepted()
    assert installed is not None and {p.route for p in installed.route_policies} == {"main", "second"}


# ---------------------------------------------------------------------------------------------------- S38c
async def test_a_config_hash_pause_past_its_install_slot_pages_high_once() -> None:
    """Round 12 / S38c: a pause that outlives the first install slot after the accept (here the read never yields
    the accepted document) pages `high` at once instead of silently pausing advice for hours; before the slot only
    the warning goes out."""
    first = await installed_rig()
    doc_b = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    first.store.accept(doc_b, T + BAR)
    r = rig(DOC_A, store=first.store, now=T + BAR + 30_000)           # the read still holds the old document only
    await r.inst.poll()
    assert r.inst.state is ConfigState.PAUSED
    assert "ARM paused: configuration hash" in r.pages.titles()
    assert "ARM paused past its install slot" not in r.pages.titles()
    accepted = first.store.accepted_rows[-1]
    r.clock.now = r.inst.lingering_after_ms(accepted)
    await r.inst.poll()
    await r.inst.poll()
    lingering = [p for p in r.pages.sent if p[1] == "ARM paused past its install slot"]
    assert len(lingering) == 1 and lingering[0][0] == "high"
    assert doc_b.doc_hash[:12] in lingering[0][2] and "SYN" not in lingering[0][2]


async def test_a_restart_on_an_old_accept_that_still_pauses_pages_at_once() -> None:
    first = await installed_rig()
    doc_b = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    first.store.accept(doc_b, T + BAR)
    r = rig(DOC_A, store=first.store, now=T + 3 * BAR)          # past its install slot, before the bound
    await r.inst.poll()
    assert r.inst.state is ConfigState.PAUSED
    assert "ARM paused past its install slot" in r.pages.titles()


async def test_a_config_hash_pause_is_bounded_and_advice_resumes_under_the_installed_document() -> None:
    """S38c review: past `pause_bound_slots` install slots after the lingering page the pause ends (HELD): the
    installed document stays in force, `pause_reason()` is None so the BarClock decides again, one `high` page says
    so; the accepted document still installs once a read yields it."""
    first = await installed_rig()
    doc_b = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    first.store.accept(doc_b, T + BAR)
    r = rig(DOC_A, store=first.store, now=T + BAR + 30_000)
    await r.inst.poll()
    accepted = first.store.accepted_rows[-1]
    r.clock.now = r.inst.bound_after_ms(accepted) - 1
    await r.inst.poll()
    assert r.inst.state is ConfigState.PAUSED and r.inst.pause_reason() == PAUSE_CONFIG_HASH
    r.clock.now = r.inst.bound_after_ms(accepted)
    await r.inst.poll()
    await r.inst.poll()
    assert r.inst.state is ConfigState.HELD and r.inst.pause_reason() is None
    assert r.inst.accepted() is not None and r.inst.installed_hash() == DOC_A.doc_hash
    bounded = [p for p in r.pages.sent if p[1] == "ARM pause bounded: installed configuration kept"]
    assert len(bounded) == 1 and bounded[0][0] == "high" and "SYN" not in bounded[0][2]
    counters = dict(r.inst.snapshot())
    assert counters["config_state_held"] == 1 and counters["config_state_paused"] == 0


async def test_with_nothing_installed_the_pause_is_not_bounded() -> None:
    r = rig(DOC_A)
    doc_b = parsed(raw_doc(assignments=[assignment_row(), assignment_row(2, symbol="SYN.B")]))
    r.store.accept(doc_b, T)
    accepted = r.store.accepted_rows[-1]
    r.clock.now = r.inst.bound_after_ms(accepted) + BAR
    await r.inst.poll()
    assert r.inst.state is ConfigState.PAUSED and r.inst.pause_reason() == PAUSE_CONFIG_HASH
