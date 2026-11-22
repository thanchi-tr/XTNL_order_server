"""Regression tests for the findings of the multi-lens review of the live view build (2026-11-21)."""
from __future__ import annotations

import contextlib
import socket
import struct
import subprocess
import sys
import threading
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

import src.liveview.__main__ as M
from src.liveview import sql as Q
from src.liveview.probe import EngineProbe, ProbeAnswer
from src.liveview.settings import LiveViewSettings
from tests.liveview.fakedb import FakeDB, entity
from tests.liveview.test_narrate import H, oversee, order, position, sh, stonex_trade, term

ROOT = Path(__file__).resolve().parents[2]


# ---- a LiveView over the fake database ---------------------------------------------------------------------------
class FlakyDB(FakeDB):
    """FakeDB that raises a connection error on the Nth statement from now, or on the first statement with a given
    text (once)."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_after: int | None = None
        self.fail_on: str | None = None

    async def fetch(self, sql: str, *args: object) -> Any:
        if self.fail_on is not None and sql == self.fail_on:
            self.fail_on = None
            raise ConnectionResetError("dropped")
        if self.fail_after is not None:
            if self.fail_after == 0:
                self.fail_after = None
                raise ConnectionResetError("dropped")
            self.fail_after -= 1
        return await super().fetch(sql, *args)


class FakePool:
    def __init__(self, db: FakeDB) -> None:
        self.db = db

    @contextlib.asynccontextmanager
    async def _acq(self) -> AsyncIterator[Any]:
        yield self

    def acquire(self) -> Any:
        return self._acq()

    @contextlib.asynccontextmanager
    async def transaction(self, **_kw: Any) -> AsyncIterator[None]:
        yield None

    async def fetch(self, sql: str, *args: object) -> Any:
        return await self.db.fetch(sql, *args)

    async def fetchval(self, sql: str, *args: object) -> Any:
        return await self.db.fetchval(sql, *args)

    async def close(self) -> None:
        return None


def live_view(db: FakeDB) -> M.LiveView:
    async def factory(_dsn: str) -> FakePool:
        return FakePool(db)
    s = LiveViewSettings(dsn="postgresql://x@127.0.0.1:1/x", host="127.0.0.1", port=1, metrics=None)
    return M.LiveView(s, pool_factory=factory, probe=EngineProbe(None))


def ring_titles(lv: M.LiveView) -> list[str]:
    return [d["title"] for (_n, kind, d, _s) in lv.hub.ring if kind == "activity"]


async def test_a_db_error_mid_poll_loses_no_row() -> None:
    db = FlakyDB()
    lv = live_view(db)
    await lv.poll_once()                        # start
    assert lv.tailer.started
    for i in range(3):
        entity(db, key=f"stonex:{i}", facts={"broker_order_id": str(i), "symbol": "EURUSD", "side": "BUY",
                                             "status": "WORKING", "entry_level": "1.1", "leg_kind": "LIMIT"})
    db.now += 1000
    calls = len(db.calls)
    await lv.poll_once()                        # learn how many statements one poll runs
    per_poll = len(db.calls) - calls
    for i in range(3, 6):
        entity(db, key=f"stonex:{i}", facts={"broker_order_id": str(i), "symbol": "EURUSD", "side": "BUY",
                                             "status": "WORKING", "entry_level": "1.1", "leg_kind": "LIMIT"})
    for fail_at in range(1, per_poll, 3):       # fail the poll at many different statements
        db.now += 1000
        db.fail_after = fail_at
        await lv.poll_once()
        assert not lv.db_ok
        lv.pool = None
    db.now += 1000
    await lv.poll_once()
    assert lv.db_ok
    placed = [t for t in ring_titles(lv) if "order placed" in t]
    assert len(placed) == 6 and len(set(placed)) == 6


async def test_a_failed_start_is_redone_in_full() -> None:
    db = FlakyDB()
    db.add("terms", cluster_id="c1", epoch_revision=7, node="pc1", started_at=db.now - 10_000_000,
           reached_leader_at=db.now - 10_000_000, ended_at=None, end_reason=None)
    entity(db, key="stonex:1", facts={"broker_order_id": "1", "symbol": "EURUSD", "side": "BUY", "status": "WORKING",
                                      "entry_level": "1.1", "leg_kind": "LIMIT"}, at=db.now - 60_000)
    titles: list[str] = []
    # fail after Tailer.start has finished (the halt-before and state reads), the case the fix is about
    for fail_on in (Q.SQL_HALT_BEFORE, Q.SQL_BREACHES_OPEN, Q.SQL_OPEN_POSITIONS, Q.SQL_TERM_LATEST):
        lv = live_view(db)
        db.fail_on = fail_on
        await lv.poll_once()
        assert db.fail_on is None, "the start never reached the failing statement"
        assert not lv.tailer.started and lv.hub.n == 0 and not lv.narrator.terms
        lv.pool = None
        await lv.poll_once()
        assert lv.tailer.started
        titles = ring_titles(lv)
        assert titles.count("Term 1 started on pc1 (epoch 7)") == 1
        assert sum("order placed" in t for t in titles) == 1


async def test_a_bug_is_not_reported_as_database_down() -> None:
    db = FakeDB()
    lv = live_view(db)
    await lv.poll_once()

    async def broken(*_a: Any) -> Any:
        raise KeyError("bug")
    lv.tailer.poll = broken                     # type: ignore[method-assign]
    with pytest.raises(KeyError):
        await lv.poll_once()


def test_is_db_error_classifies() -> None:
    class PgErr(Exception):
        sqlstate = "57014"
    assert M.is_db_error(ConnectionResetError()) and M.is_db_error(TimeoutError()) and M.is_db_error(PgErr())
    assert not M.is_db_error(KeyError("x")) and not M.is_db_error(ValueError("x"))


async def test_a_halt_row_from_before_midnight_read_after_it_sets_the_level_at_midnight() -> None:
    db = FakeDB()
    lv = live_view(db)
    await lv.poll_once()
    day = lv.day.start_ms
    db.add("halt", level="SOFT", effective_level="SOFT", hard_latch=False, rule_id="max_position_risk",
           reason_code="x", by_whom="system", at_ms=day - 5, reviewed_event_id=None, forced=False)
    db.now += 1000
    await lv.poll_once()
    assert lv.day.halt[0] == {"from_ms": day, "to_ms": None, "level": "SOFT", "hard_latch": False}
    # a later row with an engine clock behind midnight still follows id order
    db.add("halt", level="NONE", effective_level="NONE", hard_latch=False, rule_id=None, reason_code=None,
           by_whom="june", at_ms=day - 60_000, reviewed_event_id=None, forced=False)
    db.now += 1000
    await lv.poll_once()
    assert lv.day.halt[-1]["level"] == "NONE"


async def test_late_rows_enter_the_watched_sets() -> None:
    from src.liveview.tailer import Tailer
    db = FakeDB()
    t = Tailer(poll_ms=1000)
    await t.start(db, db.now, db.now - 3_600_000)
    slow = db.reserve("dlq")
    db.add("dlq", origin_dlq_id=None, event_type="ORDER", entity_id="1", adapter_id="stonex", dlq_reason="x",
           retry_count=0, status="QUEUED", parked_at_ms=db.now, updated_at_ms=db.now)
    for _ in range(120):
        db.now += 1000
        await t.poll(db, db.now)
    db.commit("dlq", slow, {"origin_dlq_id": None, "event_type": "ORDER", "entity_id": "2", "adapter_id": "stonex",
                            "dlq_reason": "x", "retry_count": 0, "status": "QUEUED", "parked_at_ms": db.now,
                            "updated_at_ms": db.now})
    late = []
    for _ in range(70):
        db.now += 1000
        late += (await t.poll(db, db.now)).late.get("dlq", [])
    assert [r["dlq_id"] for r in late] == [slow]
    assert slow in t.dlq.rows


async def test_watched_reads_are_chunked_and_never_truncated() -> None:
    from src.liveview import tailer as T
    db = FakeDB()
    t = T.Tailer(poll_ms=1000)
    await t.start(db, db.now, db.now - 3_600_000)
    for _ in range(T.WATCHED_CHUNK * 2 + 5):
        db.add("dlq", origin_dlq_id=None, event_type="ORDER", entity_id="1", adapter_id="stonex", dlq_reason="x",
               retry_count=0, status="QUEUED", parked_at_ms=db.now, updated_at_ms=db.now)
    db.now += 1000
    await t.poll(db, db.now)
    assert len(t.dlq.rows) == T.WATCHED_CHUNK * 2 + 5
    db.now += 1000
    await t.poll(db, db.now)
    assert len(t.dlq.rows) == T.WATCHED_CHUNK * 2 + 5
    assert sum(1 for c in db.calls[-200:] if c == Q.SQL_DLQ_WATCHED) >= 3


def test_stdin_eof_reader_uses_the_raw_descriptor() -> None:
    src = (ROOT / "src" / "liveview" / "__main__.py").read_text(encoding="utf-8")
    assert "os.read(fd, 4096)" in src and "stdin.buffer.read" not in src


def _refusing_postgres() -> tuple[int, threading.Thread]:
    """A local stub that speaks just enough of the PostgreSQL protocol to refuse a login with SQLSTATE 28P01."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    srv.settimeout(60)

    def serve() -> None:
        try:
            while True:
                conn, _ = srv.accept()
                with conn:
                    while True:
                        head = conn.recv(8)
                        if len(head) < 8:
                            break
                        size, code = struct.unpack("!ii", head)
                        if size == 8 and code == 80877103:       # SSLRequest: no SSL
                            conn.sendall(b"N")
                            continue
                        rest = size - 8
                        while rest > 0:
                            rest -= len(conn.recv(rest))
                        fields = b"SFATAL\0VFATAL\0C28P01\0Mpassword authentication failed\0\0"
                        conn.sendall(b"E" + struct.pack("!i", len(fields) + 4) + fields)
                        break
        except OSError:
            pass

    th = threading.Thread(target=serve, daemon=True)
    th.start()
    return srv.getsockname()[1], th


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_exit_78_reaches_the_supervisor_with_stdin_held_open() -> None:
    """The login refusal comes from the poller, after "listening" (the stdin watcher thread is running) and with an
    open stdin pipe: the process still exits 78 (the buffered reader crashed the interpreter at exit instead)."""
    import os
    pg_port, _th = _refusing_postgres()
    env = {"SYSTEMROOT": os.environ.get("SYSTEMROOT", ""), "PATH": os.environ["PATH"],
           "XTNL_LIVEVIEW_DSN": f"postgresql://xtnl_liveview:x@127.0.0.1:{pg_port}/xtnl",
           "XTNL_LIVEVIEW_ADDR": f"127.0.0.1:{_free_port()}", "XTNL_LIVEVIEW_ENGINE_METRICS": "off"}
    proc = subprocess.Popen([sys.executable, "-m", "src.liveview", "--stop-on-stdin-eof"], cwd=ROOT, env=env,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    killer = threading.Timer(90, proc.kill)      # never hang the suite; stdin stays open until the process ends
    killer.start()
    try:
        out = proc.stdout.read() if proc.stdout else b""
        code = proc.wait(timeout=10)
    finally:
        killer.cancel()
        if proc.stdin:
            proc.stdin.close()
    text = out.decode("utf-8", "replace")
    assert '"event":"listening"' in text and "28P01" in text
    assert code == 78, (code, text[-500:])


# ---- narration fixes -----------------------------------------------------------------------------------------------
def test_a_close_hold_released_without_the_close_keeps_the_absorbed_cancel() -> None:
    h = H()
    stonex_trade(h)
    order(h, "101", "FILLED", side="BUY", entry=None, kind="STOP", reduce=True, linked="100")
    order(h, "102", "CANCELLED", side="BUY", entry="1.09303", kind="LIMIT", reduce=True, linked="100")
    for _ in range(12):
        h.tick()
    titles = h.titles(include_debug=True)
    assert "EURUSD SELL 100: stop-loss filled" in titles
    assert "EURUSD SELL 100: take-profit cancelled with the close" in titles
    cancel = next(e for e in h.events if e["title"].endswith("take-profit cancelled with the close"))
    assert cancel["ids"]["broker_order_id"] == "102"


def test_an_oversee_row_in_the_backfill_for_a_position_open_at_start_keeps_its_story() -> None:
    h = H()
    h.n.open_at_start = {"pos:stonex:100"}
    h.n.backfilling = True
    oversee(h, "activated", "SOFT", subject="stonex:100", activation=5)
    h.n.backfilling = False
    assert h.events[-1]["story"] == "pos:stonex:100"
    h.n.seed_open([], [{"adapter_id": "stonex", "position_id": "100", "account_id": "acc1", "symbol": "EURUSD",
                        "side": "SELL", "entry_level": "1.1", "stop_level": None, "target_level": None,
                        "opened_by_client_order_id": None, "committed_at_ms": h.now}], h.now)
    assert "POSITION|stonex:100" in h.index.get("pos:stonex:100").members


def test_an_oversee_row_for_an_unknown_entity_stands_alone() -> None:
    h = H()
    oversee(h, "activated", "SOFT", subject="stonex:999")
    assert h.events[-1]["story"] is None
    assert "pos:stonex:999" not in h.index.stories
    assert "999" in h.events[-1]["title"]


def test_a_dlq_lineage_merge_keeps_the_origin_story_state() -> None:
    h = H()
    origin = {"dlq_id": 3, "origin_dlq_id": None, "event_type": "ORDER", "entity_id": "1", "adapter_id": "stonex",
              "dlq_reason": "illegal_transition", "retry_count": 1, "status": "BACKOFF", "parked_at_ms": h.now,
              "updated_at_ms": h.now}
    child = {**origin, "dlq_id": 4, "status": "QUEUED", "retry_count": 0}
    h.n.dlq_rows([origin, child], h.now)
    assert h.index.get("dlq:4") is not None
    h.n.dlq_changes([(child, {**child, "origin_dlq_id": 3, "status": "RESOLVED"})], h.now)
    assert h.index.get("dlq:3").state == "queued"
    assert h.index.get("dlq:4").merged_into == "dlq:3"
    assert h.titles()[-1].startswith("ORDER stonex:1 parked again (same lineage)")


@pytest.mark.parametrize("seen_before", [True, False])
def test_a_merge_that_re_arms_a_resolved_origin_keeps_both_stories_right(seen_before: bool) -> None:
    """The production shape: the origin is RESOLVED, then one poll reads the origin RESOLVED -> BACKOFF and the child
    gaining origin_dlq_id; with and without the rows having been read before (restart)."""
    h = H()
    origin = {"dlq_id": 5, "origin_dlq_id": None, "event_type": "ORDER", "entity_id": "1", "adapter_id": "stonex",
              "dlq_reason": "illegal_transition", "retry_count": 1, "status": "RESOLVED", "parked_at_ms": h.now,
              "updated_at_ms": h.now}
    child = {**origin, "dlq_id": 6, "status": "QUEUED", "retry_count": 0}
    if seen_before:
        h.n.dlq_rows([origin, child], h.now)
    h.n.dlq_changes([(origin, {**origin, "status": "BACKOFF", "retry_count": 2}),
                     (child, {**child, "origin_dlq_id": 5, "status": "RESOLVED"})], h.now)
    assert h.index.get("dlq:5").state == "queued"
    assert h.index.get("dlq:6").state == "resolved" and h.index.get("dlq:6").merged_into == "dlq:5"


async def test_the_tailer_follows_an_origin_named_by_a_lineage() -> None:
    from src.liveview.tailer import Tailer
    db = FakeDB()
    o = db.add("dlq", origin_dlq_id=None, event_type="ORDER", entity_id="1", adapter_id="stonex", dlq_reason="x",
               retry_count=1, status="RESOLVED", parked_at_ms=db.now, updated_at_ms=db.now)
    t = Tailer(poll_ms=1000)
    await t.start(db, db.now, db.now - 3_600_000)              # a restart: the RESOLVED origin is not seeded
    db.add("dlq", origin_dlq_id=o["dlq_id"], event_type="ORDER", entity_id="1", adapter_id="stonex", dlq_reason="x",
           retry_count=0, status="QUEUED", parked_at_ms=db.now, updated_at_ms=db.now)
    db.now += 1000
    await t.poll(db, db.now)
    assert o["dlq_id"] in t.dlq.rows                           # adopted silently
    db.rows["dlq"][o["dlq_id"]].update(status="BACKOFF", retry_count=2)
    db.now += 1000
    out = await t.poll(db, db.now)
    assert [(a["status"], b["status"]) for a, b in out.dlq_changed if b["dlq_id"] == o["dlq_id"]] == [
        ("RESOLVED", "BACKOFF")]


def test_a_ctrader_dlq_order_never_links_to_a_position_of_the_same_number() -> None:
    h = H()
    position(h, "555", "OPEN", adapter="pepperstone")
    h.n.dlq_rows([{"dlq_id": 1, "origin_dlq_id": None, "event_type": "ORDER", "entity_id": "555",
                   "adapter_id": "pepperstone", "dlq_reason": "x", "retry_count": 0, "status": "QUEUED",
                   "parked_at_ms": h.now, "updated_at_ms": h.now}], h.now)
    assert h.index.get("dlq:1").related == []


def test_loss_reconcile_and_new_loss_between_two_polls() -> None:
    h = H()
    term(h)
    rec = sh("stonex", "orders", None, h.now, 7)
    h.n.stream_changes([(sh("stonex", "orders", h.now - 10, None, 6), rec)], h.now)
    h.tick()
    t1 = h.now
    lost = sh("stonex", "orders", t1, rec["reconciled_at"], 7)
    h.n.stream_changes([(rec, lost)], h.now)
    h.tick(5000)
    r1, t2 = h.now - 3000, h.now - 1000
    h.n.stream_changes([(lost, sh("stonex", "orders", t2, r1, 7))], h.now)
    assert h.index.get(f"out:stonex:orders:{t1}").state == "reconciled"
    assert h.index.get(f"out:stonex:orders:{t2}").state == "lost"
    assert h.n.outages[("stonex", "orders")] == f"out:stonex:orders:{t2}"

    # a loss never seen, its reconcile, then a new loss, all between two polls (no outage open before)
    g = H()
    term(g)
    base = sh("stonex", "orders", None, g.now, 7)
    g.n.stream_changes([(sh("stonex", "orders", g.now - 10, None, 6), base)], g.now)
    g.tick(5000)
    r2, t3 = g.now - 2000, g.now - 500
    g.n.stream_changes([(base, sh("stonex", "orders", t3, r2, 7))], g.now)
    assert g.index.get(f"out:stonex:orders:r{r2}").state == "reconciled"
    assert g.index.get(f"out:stonex:orders:{t3}").state == "lost"


def test_after_a_restarted_line_an_engine_still_down_alerts_60s_later() -> None:
    h = H()

    def probe(down: bool) -> ProbeAnswer:
        return ProbeAnswer(ok=not down, at_ms=h.now, poll_no=h.n.poll_no, down=down, last_ok_ms=h.now - 3000)
    h.n.probe_result(probe(True), h.now, False, None)
    h.tick(10_000)
    h.n.run_rows([{"run_id": "r2", "node_id": "pc1", "started_at_ms": h.now, "last_seen_ms": h.now,
                   "stopped_at_ms": None, "end_code": None}], [], h.now)
    h.n.probe_result(probe(True), h.now, False, None)
    assert [e["kind"] for e in h.events] == ["engine.restarted"]
    for _ in range(61):
        h.tick()
        h.n.probe_result(probe(True), h.now, False, None)
    assert [e["kind"] for e in h.events] == ["engine.restarted", "engine.not_responding"]


def test_backfill_downgrades_only_within_a_terms_start_window() -> None:
    h = H()
    h.n.backfilling = True
    t0 = h.now - 10 * 3_600_000
    h.n.term_rows([{"term_id": 1, "cluster_id": "c1", "epoch_revision": 7, "node": "pc1", "started_at": t0,
                    "reached_leader_at": t0 + 5000, "ended_at": None, "end_reason": None}], h.now)
    assert h.index.get("term:1").state == "done"
    for at, expect in ((t0 + 30_000, "info"), (t0 + 3_600_000, "warn")):
        h.n.oversee_rows([{"event_id": 1, "sweep_id": at << 20, "rule_id": "margin_usage", "subject_type": "ACCOUNT",
                           "subject_id": "acc1", "transition": "activated", "adapter_id": "stonex",
                           "account_id": "acc1", "level": "SOFT", "cap": "NONE", "reason_code": "stale_account_image",
                           "at_ms": at, "activation_id": 1}], h.now)
        assert h.events[-1]["severity"] == expect
    h.n.backfilling = False


def test_a_seeded_term_past_its_window_never_raises_still_lost() -> None:
    h = H()
    old = {"term_id": 4, "cluster_id": "c1", "epoch_revision": 9, "node": "pc1", "started_at": h.now - 3_600_000,
           "reached_leader_at": None, "ended_at": None, "end_reason": None}
    h.n.term_rows([old], h.now, seed=True)
    h.n.close_startups(h.now + 61_000, [sh("stonex", "orders", h.now - 3_600_000, None, 8)])
    assert not any(e["kind"] == "stream.still_lost" for e in h.events)
    assert h.index.get("term:4").state == "done"


def test_prune_bounds_the_narrator_maps() -> None:
    h = H()
    for i in range(30_000):
        h.n.episode_lines[f"r|POSITION|s:{i}|0"] = ("sid", None)
    h.n.alerts[1] = (None, None, {"created_at_ms": h.now - 8 * 86_400_000})
    h.n.alert_lines_done.add((1, "alert.retrying"))
    h.n.halt_lines[h.now - 8 * 86_400_000] = ("sid", None)
    h.n.prune(h.now)
    assert len(h.n.episode_lines) == 20_000 and not h.n.alerts and not h.n.alert_lines_done and not h.n.halt_lines


def test_a_filled_kindless_order_never_puts_a_level_in_the_story() -> None:
    """With the view's rule the row carries no entry_level; the story and header then show none."""
    h = H()
    order(h, "77", "FILLED", entry=None, kind=None, reduce=False)
    st = h.index.get("ord:stonex:77")
    assert st.levels["entry"] is None and " at " not in st.header
