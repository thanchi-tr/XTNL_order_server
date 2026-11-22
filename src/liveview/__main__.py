"""`python -m src.liveview [--check-config] [--stop-on-stdin-eof]`: the local read-only live view.

Exit codes: 0 clean stop; 78 configuration (a missing / refused setting, a role that is not the view-only
`xtnl_liveview`, a login refused by PostgreSQL): the supervisor leaves it off until the next start; 75 a temporary
failure (port in use, ...). --stop-on-stdin-eof: stop gracefully when stdin closes (deploy/local/start-xtnl.ps1).
Logs are JSON lines on stdout: never a row value, a DSN or an exception message (the type only). The live view never
sends orders and has no write path of any kind.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import os
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, TextIO

from aiohttp import web

from src.db.pg import init_connection, is_connection_error, sqlstate
from src.db.readonly_role import ReadOnlyRoleError, verify_read_only_connection, verify_view_only_role
from src.db.safe_errors import safe_db_error
from src.liveview import sql as Q
from src.liveview.events import STATS
from src.liveview.narrate import Narrator
from src.liveview.probe import EngineProbe, ProbeAnswer
from src.liveview.server import Hub, build_app, load_static
from src.liveview.settings import DSN_VAR, LiveViewConfigError, LiveViewSettings
from src.liveview.state import StateRows, build_state, read_state, series_values
from src.liveview.stories import DayModel, Series, StoryIndex, local_day_start
from src.liveview.tailer import Backfill, Row, Tailer, as_row
from src.observability.logs import configure_logging as _observability_logging

EX_OK: Final = 0
EX_TEMP: Final = 75
EX_CONFIG: Final = 78
ROLE: Final = "xtnl_liveview"
APP_NAME: Final = "xtnl-liveview"

_STD_ATTRS: Final = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {"message", "asctime"}
log = logging.getLogger("xtnl.liveview")


class JsonFormatter(logging.Formatter):
    """One JSON object per line; exception text and stack traces are left out on purpose (the type is kept)."""

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(), "logger": record.name, "msg": record.getMessage()}
        for k, v in vars(record).items():
            if k not in _STD_ATTRS and not k.startswith("_"):
                out[k] = v if isinstance(v, (str, int, float, bool)) or v is None else str(v)
        if record.exc_info and record.exc_info[0] is not None:
            out["exc_type"] = record.exc_info[0].__name__
        return json.dumps(out, separators=(",", ":"), ensure_ascii=True)


def configure_logging(stream: TextIO | None = None) -> None:
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    _observability_logging("INFO", root, fmt="text")        # URL redaction on every handler
    handler.setFormatter(JsonFormatter())
    for noisy in ("asyncio", "aiohttp.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


class ConfigFailure(Exception):
    """A configuration problem found after start (exit 78); the message names the variable, never a value."""


def is_login_refusal(exc: BaseException) -> bool:
    code = sqlstate(exc) or ""
    return code.startswith("28") or code == "3D000"


def is_db_error(exc: BaseException) -> bool:
    """A database problem (unreachable, a dropped connection, a statement error or timeout): "db: down" and retry.
    Anything else is a bug: it fails the poller, so the process exits 75 and the supervisor restarts it."""
    if isinstance(exc, (OSError, TimeoutError)) or is_connection_error(exc) or sqlstate(exc) is not None:
        return True
    try:
        asyncpg = importlib.import_module("asyncpg")
    except ImportError:
        return False
    return isinstance(exc, (asyncpg.PostgresError, asyncpg.InterfaceError))


@dataclass
class StartRead:
    """Everything the start reads, before any of it is narrated."""

    db_now: int
    day_start: int
    bf: Backfill
    before: Row | None
    day_before: Row | None
    state_rows: StateRows


async def create_pool(dsn: str) -> Any:
    """asyncpg pool (min 1, max 2), read-only session settings, every connection checked: a read-only role
    (shared check) and the view-only `xtnl_liveview` (no table readable, every lv_ view readable)."""
    asyncpg = importlib.import_module("asyncpg")

    async def init(conn: Any) -> None:
        await init_connection(conn)
        await verify_read_only_connection(conn, dsn_var=DSN_VAR, role_hint=ROLE)
        await verify_view_only_role(conn, views=sorted(Q.ALL_VIEWS), role_name=ROLE, dsn_var=DSN_VAR)

    return await asyncpg.create_pool(dsn, min_size=1, max_size=2, timeout=5.0, init=init,
                                     server_settings={"application_name": APP_NAME,
                                                      "default_transaction_read_only": "on",
                                                      "statement_timeout": "5000"})


SOURCE_RANK: Final = {"terms": 0, "catchups": 1, "entity": 2, "oversee": 3, "halt": 4, "operator": 5, "dlq": 6,
                      "alerts": 7}
TIME_OF: Final = {"terms": "started_at", "catchups": "finished_at_ms", "oversee": "at_ms", "halt": "at_ms",
                  "operator": "at_ms", "dlq": "parked_at_ms", "alerts": "created_at_ms"}


def _row_time(source: str, r: Row) -> int:
    if source == "entity":
        return int(r.get("broker_ms") or r.get("logged_at_ms") or 0)
    return int(r.get(TIME_OF[source]) or 0)


class LiveView:
    def __init__(self, settings: LiveViewSettings, *, pool_factory: Any = create_pool,
                 probe: EngineProbe | None = None) -> None:
        self.settings = settings
        self.pool_factory = pool_factory
        self.pool: Any = None
        self.probe = probe or EngineProbe(settings.metrics)
        now = int(time.time() * 1000)
        self.index = StoryIndex()
        self.day = DayModel(local_day_start(now))
        self.series = Series()
        self.hub = Hub(history=settings.history, index=self.index, day=self.day, series=self.series)
        self.narrator = Narrator(self.index, self.hub.emit, self.hub.merge)
        self.tailer = Tailer(poll_ms=settings.poll_ms, history=settings.history, backfill_h=settings.backfill_h)
        self.db_ok = False
        self.db_down_since: int | None = None
        self.last_poll_ms: int | None = None
        self.backoff = 1.0
        self.halt_before: Row | None = None
        self.halt_today: list[Row] = []
        self.last_state_rows: StateRows | None = None
        self.last_streams: list[Row] = []
        self.last_quarantine: list[Row] = []
        self.probe_answer: ProbeAnswer | None = None
        self.poll_no = 0

    # ---- database ---------------------------------------------------------------------------------------------
    async def ensure_pool(self) -> bool:
        if self.pool is not None:
            return True
        try:
            self.pool = await self.pool_factory(self.settings.dsn)
            log.info("database connected", extra={"event": "db_up"})
            return True
        except ReadOnlyRoleError as exc:
            raise ConfigFailure(str(exc)) from None
        except Exception as exc:
            if is_login_refusal(exc):
                raise ConfigFailure(f"PostgreSQL refused the login of {DSN_VAR} ({safe_db_error(exc)}): apply "
                                    f"migration 0021, then run deploy/local/new-liveview-credentials.ps1") from None
            self._db_down(exc)
            return False

    def _db_down(self, exc: BaseException | None) -> None:
        if self.db_ok or self.db_down_since is None:
            self.db_down_since = self.last_poll_ms or int(time.time() * 1000)
            log.warning("database unreachable", extra={"event": "db_down",
                                                        "error": safe_db_error(exc) if exc else "unknown"})
        self.db_ok = False

    async def close_pool(self) -> None:
        pool, self.pool = self.pool, None
        if pool is not None:
            try:
                await asyncio.wait_for(pool.close(), 3)
            except Exception:
                try:
                    pool.terminate()
                except Exception:
                    pass

    # ---- start ------------------------------------------------------------------------------------------------
    def _new_tailer(self) -> Tailer:
        return Tailer(poll_ms=self.settings.poll_ms, history=self.settings.history,
                      backfill_h=self.settings.backfill_h)

    async def read_start(self, conn: Any) -> StartRead:
        """Every database read of the start, in the poll's transaction. Narration waits until all of them have
        succeeded (apply_start), so a start that fails part-way leaves nothing behind and is redone in full."""
        db_now = int(await conn.fetchval(Q.SQL_NOW))
        day_start = local_day_start(db_now)
        bf = await self.tailer.start(conn, db_now, day_start)
        since = min(db_now - self.settings.backfill_h * 3_600_000, day_start)
        before = await conn.fetch(Q.SQL_HALT_BEFORE, since)
        day_before = await conn.fetch(Q.SQL_HALT_BEFORE, day_start)
        state_rows = await read_state(conn, db_now, day_start)
        return StartRead(db_now=db_now, day_start=day_start, bf=bf,
                         before=as_row(before[0]) if before else None,
                         day_before=as_row(day_before[0]) if day_before else None, state_rows=state_rows)

    def apply_start(self, s: StartRead) -> None:
        db_now, day_start, bf, state_rows = s.db_now, s.day_start, s.bf, s.state_rows
        if day_start != self.day.start_ms:
            self._new_day(day_start)
        self.narrator.seed_halt(s.before)
        self.halt_before = s.day_before
        self.halt_today = [r for r in bf.rows.get("halt", []) if int(r.get("at_ms") or 0) >= day_start]
        # the trade stories of entities open now exist before the backfill, so Oversee rows read in the backfill for
        # a position opened before the backfill window still land in its story
        self.narrator.open_at_start = {f"pos:{p['adapter_id']}:{p['position_id']}" for p in state_rows.positions} | {
            f"ord:{o['adapter_id']}:{o['broker_order_id']}" for o in state_rows.orders}
        self._backfill(bf)
        self.narrator.seed_open(state_rows.orders, state_rows.positions, db_now)
        self.narrator.seed_runs(bf.runs)
        self.narrator.term_rows([t for t in bf.terms_open if int(t["term_id"]) not in self.narrator.terms], db_now,
                                seed=True)
        self.narrator.seed_auth(bf.auth)
        self.day.seed_streams(bf.streams, bf.rows.get("catchups", []),
                              {tid: int(t["started_at"]) for tid, t in self.narrator.terms.items()
                               if t.get("started_at") is not None})
        for src, t in bf.partial_from.items():
            self.day.mark_partial(t)
        self.last_streams, self.last_quarantine = bf.streams, bf.quarantine
        self.last_state_rows = state_rows
        log.info("started reading", extra={"event": "backfill", "events": self.hub.n})

    def _backfill(self, bf: Backfill) -> None:
        # Interleave the sources by time, but never reorder one source's rows: each source keeps its id order (a
        # skewed engine clock must not replay halt or term rows out of order), so its times are made non-decreasing.
        merged: list[tuple[int, int, int, str, Row]] = []
        for src, rows in bf.rows.items():
            floor = 0
            for i, r in enumerate(rows):
                floor = max(floor, _row_time(src, r))
                merged.append((floor, SOURCE_RANK[src], i, src, r))
        merged.sort(key=lambda x: (x[0], x[1], x[2]))
        n = self.narrator
        n.backfilling = True
        try:
            for t, _rank, _i, src, r in merged:
                n.tick_holds(t)
                if src == "terms":
                    n.term_rows([r], bf.db_now)
                elif src == "catchups":
                    n.catchup_rows([r], bf.db_now)
                elif src == "entity":
                    n.entity_rows([r], bf.db_now)
                elif src == "oversee":
                    n.oversee_rows([r], bf.db_now)
                elif src == "halt":
                    n.halt_rows([r], bf.db_now)
                elif src == "operator":
                    n.operator_rows([r], bf.db_now)
                elif src == "dlq":
                    n.dlq_rows([r], bf.db_now)
                elif src == "alerts":
                    n.alert_rows([r], bf.db_now)
            n.tick_holds(bf.db_now + 10**9)
        finally:
            n.backfilling = False
        for st in self.index.stories.values():
            if st.kind == "engine" and st.state == "starting":
                term = n.terms.get(int(st.key.split(":")[1]))
                if term is not None and (term.get("reached_leader_at") is not None or term.get("ended_at")):
                    st.state = "done"
        n.refresh_headers()

    def _new_day(self, day_start: int) -> None:
        old = self.day
        self.day = DayModel(day_start)
        for adapter, streams in old.streams.items():
            for stream, segs in streams.items():
                if segs:
                    self.day.streams.setdefault(adapter, {})[stream] = [
                        {"from_ms": day_start, "to_ms": None, "state": segs[-1]["state"]}]
        self.day.inactive = set(old.inactive)
        self.hub.day = self.day
        self.halt_before = (self.halt_today[-1] if self.halt_today else self.halt_before)
        self.halt_today = []

    # ---- each poll --------------------------------------------------------------------------------------------
    async def poll_once(self) -> None:
        self.poll_no += 1
        self.narrator.poll_no = self.poll_no
        wall = int(time.time() * 1000)
        probe = await self.probe.poll(self.last_poll_ms or wall, self.poll_no)
        self.probe_answer = probe
        if not await self.ensure_pool():
            self._publish_poll(wall)
            return
        fresh = not self.tailer.started
        saved = None if fresh else self.tailer.checkpoint()
        start: StartRead | None = None
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction(isolation="repeatable_read", readonly=True):
                    if fresh:
                        start = await self.read_start(conn)
                    else:
                        db_now = int(await conn.fetchval(Q.SQL_NOW))
                        day_start = local_day_start(db_now)
                        rows = await self.tailer.poll(conn, db_now)
                        state_rows = await read_state(conn, db_now, day_start)
        except ConfigFailure:
            raise
        except Exception as exc:
            # nothing of a failed poll is kept: the cursors, watched sets and diff state go back to where they were,
            # so the next poll reads the same ids again (no row is lost); a failed start is redone in full
            if fresh:
                self.tailer = self._new_tailer()
            elif saved is not None:
                self.tailer.restore(saved)
            if is_login_refusal(exc):
                raise ConfigFailure(f"PostgreSQL refused the login of {DSN_VAR} ({safe_db_error(exc)})") from None
            if isinstance(exc, ReadOnlyRoleError):
                raise ConfigFailure(str(exc)) from None
            if not is_db_error(exc):
                raise
            self._db_down(exc)
            await self.close_pool()
            self._publish_poll(wall)
            return
        if start is not None:
            self.apply_start(start)
            self.db_ok = True
            self.db_down_since = None
            self.last_poll_ms = start.db_now
            self.backoff = 1.0
            self._publish_poll(start.db_now)
            return
        if day_start != self.day.start_ms:
            self._new_day(day_start)
        if self.db_down_since is not None and self.last_poll_ms is not None:
            gap_from, gap_to = self.db_down_since, db_now
            if gap_to - gap_from > 3 * self.settings.poll_ms:
                self.narrator.emit("engine.not_reading", {"since": gap_from, "until": gap_to}, at_ms=gap_from,
                                   clock="server", detected_ms=db_now, source="liveview")
            log.info("database reachable again", extra={"event": "db_up"})
        self.db_down_since = None
        self.db_ok = True
        self.last_poll_ms = db_now
        self.backoff = 1.0
        self._narrate(rows, state_rows, db_now, probe)

    def _narrate(self, rows: Any, s: StateRows, db_now: int, probe: ProbeAnswer) -> None:
        n = self.narrator
        n.run_rows(rows.runs_new, rows.runs_changed, db_now)
        n.term_rows(rows.tail.get("terms", []) + rows.late.get("terms", []), db_now)
        n.term_changes(rows.terms_changed, db_now, list(n.terms.values()))
        n.catchup_rows(rows.tail.get("catchups", []), db_now)
        n.catchup_rows(rows.late.get("catchups", []), db_now, late_rows=True)
        n.stream_changes(rows.streams_changed, db_now, self.day)
        n.entity_rows(rows.tail.get("entity", []), db_now)
        n.entity_rows(rows.late.get("entity", []), db_now, late_rows=True)
        n.set_breaches(s.breaches, s.timing)
        n.oversee_rows(rows.tail.get("oversee", []), db_now)
        n.oversee_rows(rows.late.get("oversee", []), db_now, late_rows=True)
        halt_new = rows.tail.get("halt", []) + rows.late.get("halt", [])
        n.halt_rows(halt_new, db_now)
        # every new row joins today's band, whatever its engine-clock at_ms: set_halt orders by id and clamps a time
        # before midnight to 00:00, so the level always follows id order (a skewed clock cannot misplace it)
        self.halt_today += sorted(halt_new, key=lambda x: x["id"])
        n.operator_rows(rows.tail.get("operator", []) + rows.late.get("operator", []), db_now)
        n.dlq_rows(rows.tail.get("dlq", []) + rows.late.get("dlq", []), db_now)
        n.dlq_changes(rows.dlq_changed, db_now)
        n.alert_rows(rows.tail.get("alerts", []) + rows.late.get("alerts", []), db_now)
        n.alert_changes(rows.alerts_changed, db_now)
        n.cluster_change(rows.cluster_changed, db_now)
        n.epoch_change(rows.epoch_changed, db_now)
        n.quarantine_changes(rows.quarantine_changed, db_now)
        n.auth_changes(rows.auth_changed, db_now)
        quarantined = any(q.get("released_at_ms") is None for q in rows.quarantine)
        reached = s.term.get("reached_leader_at") if s.term else None
        n.probe_result(probe, db_now, quarantined, reached)
        n.tick_holds(db_now)
        n.close_startups(db_now, rows.streams)
        self.last_streams, self.last_quarantine, self.last_state_rows = rows.streams, rows.quarantine, s
        state = build_state(n, s, db_now=db_now, probe=probe, streams=rows.streams, cluster=rows.cluster,
                            quarantine=rows.quarantine, db_ok=True)
        self.hub.set_state(state)
        self.day.set_halt(self.halt_before, self.halt_today, db_now)
        self.day.set_engine(self.tailer.runs.values(), db_now, None if probe.off else not probe.down)
        if n.active_adapters is not None:
            known = {r["adapter_id"] for r in rows.streams}
            self.day.set_inactive(known - n.active_adapters, db_now)
        if self.series.sample(db_now, series_values(state)):
            self.hub.push_series()
        self.hub.push_stories()
        self.hub.push_day(db_now)
        self.index.drop_old(db_now)
        if self.poll_no % 60 == 0:
            n.prune(db_now)
        if STATS.dropped or STATS.blanked:
            log.info("privacy guard counts", extra={"event": "guard", "dropped": sum(STATS.dropped.values()),
                                                    "blanked": sum(STATS.blanked.values())})
            STATS.dropped.clear()
            STATS.blanked.clear()
        self._publish_poll(db_now, backlog=rows.backlog)

    def _publish_poll(self, at_ms: int, backlog: int = 0) -> None:
        p = self.probe_answer
        engine = "off" if p is None or p.off else ("down" if p.down else "ok")
        self.hub.set_poll({"at_ms": self.last_poll_ms or at_ms if self.db_ok else self.last_poll_ms,
                           "db": "ok" if self.db_ok else "down", "engine": engine, "backlog": backlog})

    async def loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            started = time.monotonic()
            await self.poll_once()
            if self.db_ok:
                delay = max(0.05, self.settings.poll_ms / 1000 - (time.monotonic() - started))
            else:
                delay, self.backoff = self.backoff, min(10.0, self.backoff * 2)
            try:
                await asyncio.wait_for(stop.wait(), delay)
            except TimeoutError:
                pass


def _watch_stdin(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    try:
        fd = sys.stdin.fileno()
        while os.read(fd, 4096):   # the raw descriptor: a buffered read would hold a lock that crashes the exit
            pass
    except Exception:
        pass
    loop.call_soon_threadsafe(stop.set)


async def run(settings: LiveViewSettings, *, stop_on_stdin_eof: bool = False) -> int:
    try:
        static = load_static()
    except OSError as exc:
        log.error("static page files missing", extra={"event": "config_error", "exc_type": type(exc).__name__})
        return EX_CONFIG
    lv = LiveView(settings)
    app = build_app(lv.hub, static, host=settings.host, port=settings.port, max_clients=settings.max_clients,
                    db_ok=lambda: lv.db_ok, now_ms=lambda: int(time.time() * 1000))
    runner = web.AppRunner(app, handler_cancellation=True, shutdown_timeout=2, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, settings.host, settings.port)
    try:
        await site.start()
    except OSError as exc:
        log.error("cannot listen", extra={"event": "listen_failed", "exc_type": type(exc).__name__,
                                          "addr": f"{settings.host}:{settings.port}"})
        await runner.cleanup()
        return EX_TEMP
    stop = asyncio.Event()
    if stop_on_stdin_eof:
        threading.Thread(target=_watch_stdin, args=(asyncio.get_running_loop(), stop), daemon=True,
                         name="stdin-eof").start()
    log.info("listening", extra={"event": "listening", "addr": f"{settings.host}:{settings.port}"})
    code = EX_OK
    poller = asyncio.create_task(lv.loop(stop))
    stopper = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait({poller, stopper}, return_when=asyncio.FIRST_COMPLETED)
        failure = poller.exception() if poller in done else None
        if failure is not None:
            if isinstance(failure, ConfigFailure):
                log.error("configuration refused: %s", failure, extra={"event": "config_error"})
                code = EX_CONFIG
            else:
                log.error("poller failed", extra={"event": "poller_failed", "exc_type": type(failure).__name__})
                code = EX_TEMP
        else:
            log.info("stopping (stdin closed)", extra={"event": "stopping"})
    except asyncio.CancelledError:
        log.info("stopping (Ctrl+C)", extra={"event": "stopping"})
    finally:
        stop.set()
        for t in (poller, stopper):
            t.cancel()
        lv.hub.close_all()
        try:
            await asyncio.wait_for(runner.cleanup(), 3)
        except (TimeoutError, asyncio.CancelledError):
            pass
        await lv.close_pool()
        log.info("stopped", extra={"event": "stopped"})
    return code


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    p = argparse.ArgumentParser(prog="python -m src.liveview", description="XTNL live view (read-only, local)")
    p.add_argument("--check-config", action="store_true", help="check the settings and exit")
    p.add_argument("--stop-on-stdin-eof", action="store_true", help="stop gracefully when stdin closes")
    ns = p.parse_args(list(argv) if argv is not None else None)
    try:
        settings = LiveViewSettings.from_env()
    except LiveViewConfigError as exc:
        log.error("configuration refused: %s", exc, extra={"event": "config_error"})
        return EX_CONFIG
    if ns.check_config:
        log.info("configuration ok: %s", settings.describe(), extra={"event": "config_ok"})
        return EX_OK
    log.info("starting: %s", settings.describe(), extra={"event": "starting"})
    started = time.monotonic()
    try:
        code = asyncio.run(run(settings, stop_on_stdin_eof=ns.stop_on_stdin_eof))
    except KeyboardInterrupt:
        code = EX_OK
    log.info("exit", extra={"event": "exit", "code": code, "uptime_s": int(time.monotonic() - started)})
    return code


if __name__ == "__main__":
    sys.exit(main())
