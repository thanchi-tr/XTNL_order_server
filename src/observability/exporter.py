"""Prometheus exporter: `GET /metrics` on `XTNL_METRICS_ADDR` (operations.md §9; deploy/observability/metrics.md).

Runs in every engine process and every SM9 role (STANDBY included), on the event loop (asyncio streams, no
thread), read-only. The default address is loopback on the contract port (9464); a deployed node sets
`XTNL_METRICS_ADDR=<node VPC ip>:9464` so the off-node Prometheus / CloudWatch agent can scrape it (VPC
only, no inbound internet, operations.md §16). `/healthz` answers 200 for a local liveness probe.
"""
from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Mapping
from typing import Final

from src.observability.http import Request, Response, bound_port, parse_addr, start_server
from src.observability.metrics import MetricsRegistry

log = logging.getLogger(__name__)

METRICS_ADDR_VAR: Final = "XTNL_METRICS_ADDR"
DEFAULT_METRICS_PORT: Final = 9464           # deploy/observability/metrics.md
DEFAULT_METRICS_ADDR: Final = f"127.0.0.1:{DEFAULT_METRICS_PORT}"
CONTENT_TYPE: Final = "text/plain; version=0.0.4; charset=utf-8"


def metrics_addr(environ: Mapping[str, str] | None = None) -> tuple[str, int] | None:
    """The bind address; None when XTNL_METRICS_ADDR=off (tests, a second process on one host)."""
    env = os.environ if environ is None else environ
    raw = (env.get(METRICS_ADDR_VAR) or DEFAULT_METRICS_ADDR).strip()
    if raw.lower() in ("off", "none", "0"):
        return None
    return parse_addr(raw, DEFAULT_METRICS_PORT)


class MetricsExporter:
    def __init__(self, registry: MetricsRegistry, host: str = "127.0.0.1", port: int = DEFAULT_METRICS_PORT) -> None:
        self.registry = registry
        self.host = host
        self.port = port
        self._server: asyncio.Server | None = None
        self.scrapes = 0

    async def handle(self, req: Request) -> Response:
        if req.path == "/metrics":
            self.scrapes += 1
            return Response.text(200, self.registry.render(), CONTENT_TYPE)
        if req.path == "/healthz":
            return Response.text(200, "ok\n")
        return Response.text(404, "not found\n")

    async def start(self) -> int:
        """Bind and serve in the background; returns the bound port (0 asks the OS for one)."""
        self._server = await start_server(self.handle, self.host, self.port)
        self.port = bound_port(self._server)
        log.info("metrics exporter listening on %s:%d", self.host, self.port)
        return self.port

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), 2.0)
            except TimeoutError:
                pass
            self._server = None


__all__ = ["CONTENT_TYPE", "DEFAULT_METRICS_ADDR", "METRICS_ADDR_VAR", "MetricsExporter", "metrics_addr"]
