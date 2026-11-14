"""Engine entrypoint: `python -m src.main` (operations.md §3, §4; IMPLEMENTATION_PLAN 3.1).

* Validates BROKER_ENV (sandbox / live_readonly only, adapter-isolation.md §5) and the runtime config.
* `--check-config` builds the full composition (src/runtime/wiring.py) without connecting anything.
* Any node drains its own DLQ spill file at start-up (tier-persistence.md §5).
* uvloop on Linux (never imported on Windows); the stdlib loop elsewhere.
* Runs the NodeController (SM9) plus the event-loop watchdog feeding systemd `WATCHDOG=1`.
* SIGTERM / SIGINT -> graceful DRAINING (planned switchover) when leader, then exit.
* Structured JSON logs (src/observability/logs.py) and the Prometheus exporter on XTNL_METRICS_ADDR
  (src/observability/exporter.py, src/runtime/telemetry.py) in every role (IMPLEMENTATION_PLAN 5.6).
* Engine uptime (migration 0012, src/runtime/uptime.py) in every role: an `engine_runs` row at start, a light
  heartbeat every XTNL_UPTIME_PING_S, sleep detection (which makes the leader re-subscribe and catch up), and the
  stop time + reason (`SIGINT` / `SIGTERM` / `stopped` / `fenced` / `error:<type>`) on a clean stop.

Phase 1 is read-only: nothing reachable from here sends orders.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import signal
import sys
import time
from collections.abc import Callable, Mapping, Sequence

from src.config import ExecutionLockedError
from src.observability.exporter import MetricsExporter, metrics_addr
from src.observability.logs import configure_logging as _configure_logging
from src.observability.redact import (  # re-exported: the names src.main has always offered
    QUIET_HTTP_LOGGERS,
    REDACTED_SCHEMES,
    UrlRedactingFilter,
    redact_urls,
)
from src.runtime.config import RuntimeConfig
from src.runtime.etcd import HttpEtcdClient
from src.runtime.health import HealthRecord
from src.runtime.node import NodeController
from src.runtime.roles import NodeRole
from src.runtime.probes import ADDRESSES_TTL_S, local_addresses
from src.runtime.sdnotify import SdNotifier
from src.runtime.supervisor import LoopWatchdog
from src.runtime.telemetry import Telemetry, asyncpg_connect, build_telemetry, certs_from_env
from src.runtime.uptime import UptimeTracker
from src.runtime.wiring import Engine, WiringError, build_engine, check_composition, drain_spill_at_startup

log = logging.getLogger("xtnl.main")

__all__ = ["EX_CONFIG", "QUIET_HTTP_LOGGERS", "REDACTED_SCHEMES", "UrlRedactingFilter", "configure_logging",
           "engine_telemetry", "event_loop_factory", "main", "parse_args", "redact_urls", "run_engine"]

EX_CONFIG = 78


def configure_logging(level: str, root: logging.Logger | None = None, *, node: str | None = None,
                      environ: Mapping[str, str] | None = None) -> str:
    """Root logging for the engine (src/observability/logs.py): JSON lines on Linux (XTNL_LOG_FORMAT
    overrides), every handler redacting URLs / DSNs, the HTTP client loggers at WARNING. Returns the format."""
    return _configure_logging(level, root, node=node, environ=environ)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python -m src.main",
        description="XTNL Phase 1 engine node: read-only broker ingestion, Oversee, leader election (SM9).")
    p.add_argument("--node-id", help="this node's id (default: $XTNL_NODE_ID or the hostname)")
    p.add_argument("--etcd-endpoints", help="comma-separated etcd endpoints (default: $XTNL_ETCD_ENDPOINTS)")
    p.add_argument("--deadman-url", help="Melbourne dead-man receiver URL (default: $XTNL_DEADMAN_URL)")
    p.add_argument("--spill-path", help="DLQ spill file (default: $XTNL_SPILL_PATH or var/dlq_spill.jsonl)")
    p.add_argument("--log-level", help="logging level (default: $XTNL_LOG_LEVEL or INFO)")
    p.add_argument("--check-config", action="store_true",
                   help="validate the configuration and the full engine composition (connects nothing) and exit")
    return p.parse_args(argv)


def event_loop_factory() -> tuple[str, Callable[[], asyncio.AbstractEventLoop] | None]:
    """uvloop on the Linux engine nodes; the default loop on Windows / macOS dev machines.
    uvloop is imported only when sys.platform != 'win32' (it does not build on Windows)."""
    if sys.platform != "win32":
        try:
            uvloop = importlib.import_module("uvloop")
        except ImportError:
            return "asyncio", None
        factory: Callable[[], asyncio.AbstractEventLoop] = uvloop.new_event_loop
        return "uvloop", factory
    return "asyncio", None


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, controller: NodeController,
                             on_stop_signal: Callable[[str], None] | None = None) -> None:
    def on_signal(name: str) -> None:
        log.warning("termination signal %s: draining", name)
        if on_stop_signal is not None:
            try:
                on_stop_signal(name)
            except Exception:
                log.exception("stop-signal hook failed")
        controller.request_drain("switchover", exit_after=True)

    for sig in (signal.SIGTERM, signal.SIGINT):
        name = sig.name
        try:
            loop.add_signal_handler(sig, on_signal, name)
        except (NotImplementedError, RuntimeError, ValueError):   # Windows: no loop signal handlers
            signal.signal(sig, lambda *_, n=name: loop.call_soon_threadsafe(on_signal, n))


def engine_telemetry(engine: Engine, watchdog: LoopWatchdog | None) -> Telemetry:
    """The metrics registry + readers of this process (deploy/observability/metrics.md)."""
    controller = engine.controller
    etcd = engine.etcd
    status = etcd.cluster_status if isinstance(etcd, HttpEtcdClient) else None
    chrony = engine.chrony

    def peers() -> Sequence[HealthRecord]:
        view = controller.last_view
        return list(view.peers) if view is not None else []

    hosts: list[tuple[float, frozenset[str]]] = []

    def local_hosts() -> frozenset[str]:
        """This node's addresses, re-resolved every ADDRESSES_TTL_S. Blocking (getaddrinfo): telemetry calls it
        through asyncio.to_thread, never on the event loop."""
        now = time.monotonic()
        if not hosts or now - hosts[0][0] >= ADDRESSES_TTL_S:
            hosts[:] = [(now, local_addresses())]
        return hosts[0][1]

    telemetry = build_telemetry(
        node_id=controller.node_id, controller=controller, keeper=controller.keeper, watchdog=watchdog,
        chrony_offset_s=(lambda: chrony.signed_offset_s) if chrony is not None else None,
        peers=peers,
        control=engine.control, redis=engine.redis, patroni=engine.patroni, dsn=engine.settings.storage.dsn,
        connect=asyncpg_connect, local_hosts=local_hosts, etcd_status=status, certs=certs_from_env(engine.environ))
    # uptime + downtime catch-up series (any role; in-memory, no I/O at scrape time)
    if engine.uptime is not None:
        telemetry.registry.register("uptime", engine.uptime.samples)
    telemetry.registry.register("catchup", engine.context.catchup_metrics.samples)
    return telemetry


def stop_reason(controller: NodeController, uptime: UptimeTracker, error: BaseException | None) -> str:
    """engine_runs.end_reason of a clean stop: the signal that asked for it, else why the controller ended."""
    if uptime.stop_reason is not None:
        return uptime.stop_reason
    if error is not None:
        return f"error:{type(error).__name__}"
    if controller.role is NodeRole.FENCED:
        return "fenced"
    return "stopped"


async def run_engine(controller: NodeController, engine: Engine | None = None) -> None:
    if engine is not None:
        drained = await drain_spill_at_startup(engine)
        if drained:
            log.info("start-up spill drain delivered %d DLQ rows", drained)
    keeper = controller.keeper
    watchdog = LoopWatchdog(SdNotifier(), lease_ok=lambda: not keeper.holds_lease() or keeper.lease_valid())
    stop = asyncio.Event()
    uptime = engine.uptime if engine is not None else None
    _install_signal_handlers(asyncio.get_running_loop(), controller,
                             uptime.note_stop_reason if uptime is not None else None)
    uptime_task = asyncio.create_task(uptime.run(stop), name="uptime") if uptime is not None else None
    exporter: MetricsExporter | None = None
    telemetry_task: asyncio.Task[None] | None = None
    if engine is not None:
        try:
            addr = metrics_addr(engine.environ)
        except ValueError:
            log.error("XTNL_METRICS_ADDR is not host:port; metrics export disabled")
            addr = None
        if addr is not None:
            telemetry = engine_telemetry(engine, watchdog)
            exporter = MetricsExporter(telemetry.registry, *addr)
            try:
                await exporter.start()
            except OSError as exc:      # a taken port never stops the engine: the off-node alert sees no data
                log.error("metrics exporter not started on %s:%d: %s", addr[0], addr[1], exc)
                exporter = None
            telemetry_task = asyncio.create_task(telemetry.run(stop), name="telemetry")
    keeper.start()
    wd = asyncio.create_task(watchdog.run(stop), name="loop-watchdog")
    error: BaseException | None = None
    try:
        await controller.run()
    except BaseException as exc:
        error = exc
        raise
    finally:
        stop.set()
        await wd
        if uptime is not None and uptime_task is not None:
            await asyncio.gather(uptime_task, return_exceptions=True)
            await uptime.stop(stop_reason(controller, uptime, error))
        if telemetry_task is not None:
            await telemetry_task
        if exporter is not None:
            await exporter.stop()
        keeper.stop()
        if engine is not None:
            await engine.aclose()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = RuntimeConfig.from_env(node_id=args.node_id, etcd_endpoints=args.etcd_endpoints,
                                        deadman_url=args.deadman_url, spill_path=args.spill_path,
                                        log_level=args.log_level)
    except ExecutionLockedError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EX_CONFIG
    configure_logging(config.log_level, node=config.node_id)
    if args.check_config:
        problems = asyncio.run(check_composition(config))
        for p in problems:
            print(f"config: {p}", file=sys.stderr)
        return EX_CONFIG if problems else 0
    problems = config.problems()
    if problems:
        for p in problems:
            log.error("config: %s", p)
        return EX_CONFIG
    loop_kind, loop_factory = event_loop_factory()
    try:
        engine = build_engine(config)
    except (WiringError, ExecutionLockedError) as exc:
        log.error("%s", exc)
        return EX_CONFIG
    log.info("starting (%s): %s", loop_kind, engine.settings.describe())
    asyncio.run(run_engine(engine.controller, engine), loop_factory=loop_factory)
    return 0


if __name__ == "__main__":
    sys.exit(main())
