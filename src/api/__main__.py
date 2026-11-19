"""`python -m src.api [--stop-on-stdin-eof]`: run the order-server API until Ctrl+C (SIGINT) or stdin EOF.

Exit codes: 0 clean stop; 78 configuration (missing / weak XTNL_API_SECRET, bad setting, certificate or key
missing or unreadable): not worth restarting; 1 anything else (port in use, ...): the supervisor restarts it.
--stop-on-stdin-eof: stop gracefully when stdin closes (deploy/local/start-xtnl.ps1 closes it to stop the API
without a console Ctrl+C). Logs are JSON lines on stdout, never a secret. Phase 1 never sends orders.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, TextIO

from src.api.auth import Authenticator
from src.api.pinescript import PinescriptService, pg_compiler
from src.api.server import ApiServer
from src.api.settings import ApiConfigError, ApiSettings
from src.api.tls import fingerprint, server_context

EX_OK: Final = 0
EX_ERROR: Final = 1
EX_CONFIG: Final = 78
ROOT: Final = Path(__file__).resolve().parents[2]

_STD_ATTRS: Final = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line: ts, level, logger, msg and the record's extra fields. Exception text and stack
    traces are left out on purpose (they may carry connection strings); the exception type is kept."""

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
    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    for noisy in ("asyncio", "redis"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


log = logging.getLogger("xtnl.api")


def _watch_stdin(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    try:
        while sys.stdin.buffer.read(4096):
            pass
    except Exception:
        pass
    loop.call_soon_threadsafe(stop.set)


async def run(settings: ApiSettings, *, stop_on_stdin_eof: bool = False) -> int:
    from src.cache.client import create_redis

    try:
        ssl_ctx = server_context(settings.cert_path, settings.key_path)
    except (OSError, ValueError) as exc:
        log.error("TLS certificate or key unusable", extra={"event": "config_error", "exc_type": type(exc).__name__,
                                                             "cert": str(settings.cert_path)})
        return EX_CONFIG
    redis = create_redis(settings.redis_url)
    service = PinescriptService(redis, pg_compiler(settings.dsn))
    api = ApiServer(Authenticator(settings.secret), service, rate_per_min=settings.rate_per_min)
    try:
        server = await api.serve(settings.bind, settings.port, ssl_ctx)
    except OSError as exc:
        log.error("cannot listen", extra={"event": "listen_failed", "exc_type": type(exc).__name__,
                                          "bind": settings.bind, "port": settings.port})
        await redis.aclose()
        return EX_ERROR
    stop = asyncio.Event()
    if stop_on_stdin_eof:
        threading.Thread(target=_watch_stdin, args=(asyncio.get_running_loop(), stop), daemon=True,
                         name="stdin-eof").start()
    log.info("listening", extra={"event": "listening", "bind": settings.bind, "port": settings.port,
                                 "fingerprint_sha256": fingerprint(settings.cert_path)})
    try:
        await stop.wait()
        log.info("stopping (stdin closed)", extra={"event": "stopping"})
    except asyncio.CancelledError:
        log.info("stopping (Ctrl+C)", extra={"event": "stopping"})
    finally:
        server.close()
        close_clients = getattr(server, "close_clients", None)
        if callable(close_clients):
            close_clients()
        try:
            await asyncio.wait_for(server.wait_closed(), 5)
        except (TimeoutError, asyncio.CancelledError):
            pass
        await service.close()
        try:
            await redis.aclose()
        except Exception:
            pass
        log.info("stopped", extra={"event": "stopped"})
    return EX_OK


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None) -> int:
    configure_logging()
    p = argparse.ArgumentParser(prog="python -m src.api", description="XTNL order-server API (read-only)")
    p.add_argument("--stop-on-stdin-eof", action="store_true", help="stop gracefully when stdin closes")
    ns = p.parse_args(list(argv) if argv is not None else None)
    try:
        settings = ApiSettings.from_env(environ if environ is not None else os.environ, root=ROOT)
    except ApiConfigError as exc:
        log.error("configuration refused: %s", exc, extra={"event": "config_error"})
        return EX_CONFIG
    log.info("starting: %s", settings.describe(), extra={"event": "starting", "pid": os.getpid()})
    started = time.monotonic()
    try:
        code = asyncio.run(run(settings, stop_on_stdin_eof=ns.stop_on_stdin_eof))
    except KeyboardInterrupt:
        code = EX_OK
    log.info("exit", extra={"event": "exit", "code": code, "uptime_s": int(time.monotonic() - started)})
    return code


if __name__ == "__main__":
    sys.exit(main())
