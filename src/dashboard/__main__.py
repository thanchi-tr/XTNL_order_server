"""`python -m src.dashboard`: the Melbourne read-only dashboard process (IMPLEMENTATION_PLAN 5.6).

Exit 78 (EX_CONFIG) on a bad configuration. The PostgreSQL pool is created lazily (a database outage at start
leaves the page answering 503 until it is back). SIGTERM / SIGINT stop it.

The leader heartbeat listener (src/dashboard/heartbeat.py, IMPLEMENTATION_PLAN 7.4) runs when its file exists
(`--heartbeat-file`, default /etc/xtnl/dashboard/heartbeat.json); without it every Redis live value is stale.
An existing file that the process cannot read, or whose TLS certificate / key cannot be loaded, is a
configuration error (exit 78, also under `--check-config`, the unit's ExecStartPre; IMPLEMENTATION_PLAN 8.2).
Install: deploy/env/dashboard-heartbeat.json.example, docs/runbooks/install-node.md "Melbourne dashboard host".
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from collections.abc import Sequence
from typing import Any

from src.dashboard.server import (
    APPLICATION_NAME,
    DEFAULT_PORT,
    DashboardApp,
    DashboardConfigError,
    DashboardSettings,
    create_read_only_source,
    redis_clients,
)
from src.dashboard.heartbeat import (
    DEFAULT_HEARTBEAT_FILE,
    HeartbeatBoard,
    HeartbeatConfigError,
    HeartbeatListener,
    HeartbeatListenSettings,
)
from src.dashboard.views import DashboardViews
from src.db.pg import AsyncpgSource
from src.observability.http import parse_addr, start_server
from src.observability.logs import configure_logging
from src.runtime.pg_ports import LazyPgSource

log = logging.getLogger("xtnl.dashboard")
EX_CONFIG = 78


async def _ro_factory(dsn: str, **_: Any) -> AsyncpgSource:
    return await create_read_only_source(dsn)


def heartbeat_board(beat: HeartbeatListenSettings | None) -> HeartbeatBoard:
    """The board the views read: unconfigured (every live value stale) without a heartbeat file."""
    if beat is None:
        return HeartbeatBoard(configured=False)
    return HeartbeatBoard(max_age_ms=beat.max_age_ms)


async def serve(settings: DashboardSettings, beat: HeartbeatListenSettings | None = None) -> None:
    source = LazyPgSource(settings.dsn, application_name=APPLICATION_NAME, max_size=4, factory=_ro_factory)
    clients = redis_clients(settings.redis_urls)
    board = heartbeat_board(beat)
    app = DashboardApp(DashboardViews(source, clients, fresh_ms=settings.fresh_ms, heartbeats=board))
    host, port = parse_addr(settings.addr, DEFAULT_PORT)
    beat_server = await HeartbeatListener(beat, board).start() if beat is not None else None
    if beat is None:
        log.warning("no leader heartbeat file: every Redis live value is shown as stale")
    server = await start_server(app.handle, host, port)
    log.info("dashboard listening on %s:%d (read-only)", host, port)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        await stop.wait()
    finally:
        server.close()
        await server.wait_closed()
        if beat_server is not None:
            beat_server.close()
            await beat_server.wait_closed()
        for c in clients.values():
            await c.aclose()
        await source.close()


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m src.dashboard", description="XTNL read-only dashboard")
    p.add_argument("--check-config", action="store_true", help="validate the settings and exit")
    p.add_argument("--heartbeat-file", default=DEFAULT_HEARTBEAT_FILE,
                   help=f"leader heartbeat listener settings (JSON; default {DEFAULT_HEARTBEAT_FILE}; absent = none)")
    args = p.parse_args(argv)
    configure_logging(os.environ.get("XTNL_LOG_LEVEL", "INFO"))
    try:
        settings = DashboardSettings.from_env(os.environ)
        beat = HeartbeatListenSettings.from_file(args.heartbeat_file)
        if beat is not None:
            beat.check_files()
    except (DashboardConfigError, HeartbeatConfigError) as exc:
        print(f"config: {exc}", file=sys.stderr)
        return EX_CONFIG
    if args.check_config:
        return 0
    asyncio.run(serve(settings, beat))
    return 0


if __name__ == "__main__":
    sys.exit(main())
