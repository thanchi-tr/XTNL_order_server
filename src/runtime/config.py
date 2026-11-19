"""Runtime configuration (operations.md §1, §9, §13).

Read from environment variables (systemd `Environment=` / AWS Secrets Manager injection) and CLI
overrides; nothing here reads .env files. No secret lives in this object.
"""
from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from src.config import BrokerEnv, broker_env


@dataclass(frozen=True)
class RuntimeConfig:
    node_id: str
    broker_env: BrokerEnv
    etcd_endpoints: tuple[str, ...] = ()
    deadman_url: str | None = None
    spill_path: Path = Path("var/dlq_spill.jsonl")
    log_level: str = "INFO"
    adapters: tuple[str, ...] = field(default=())
    single_pc: bool = False
    """`XTNL_SINGLE_PC=1` (set by deploy/local/start-xtnl.ps1 only): a single Windows PC with no off-node
    dead-man receiver, so an empty `XTNL_DEADMAN_URL` is allowed there (operations.md §17)."""

    @staticmethod
    def from_env(environ: Mapping[str, str] | None = None, **overrides: object) -> "RuntimeConfig":
        e = dict(environ if environ is not None else os.environ)
        endpoints = tuple(x.strip() for x in e.get("XTNL_ETCD_ENDPOINTS", "").split(",") if x.strip())
        adapters = tuple(x.strip() for x in e.get("XTNL_ADAPTERS", "").split(",") if x.strip())
        cfg = RuntimeConfig(
            node_id=e.get("XTNL_NODE_ID") or socket.gethostname(),
            broker_env=broker_env(e),
            etcd_endpoints=endpoints,
            deadman_url=e.get("XTNL_DEADMAN_URL") or None,
            spill_path=Path(e.get("XTNL_SPILL_PATH", "var/dlq_spill.jsonl")),
            log_level=e.get("XTNL_LOG_LEVEL", "INFO"),
            adapters=adapters,
            single_pc=(e.get("XTNL_SINGLE_PC") or "").strip() == "1",
        )
        clean = {k: v for k, v in overrides.items() if v is not None}
        if "etcd_endpoints" in clean and isinstance(clean["etcd_endpoints"], str):
            clean["etcd_endpoints"] = tuple(x.strip() for x in str(clean["etcd_endpoints"]).split(",") if x.strip())
        if "spill_path" in clean:
            clean["spill_path"] = Path(str(clean["spill_path"]))
        return cfg if not clean else _replace(cfg, clean)

    def problems(self) -> list[str]:
        """Configuration errors that must stop start-up."""
        out: list[str] = []
        if len(self.etcd_endpoints) < 3:
            out.append("XTNL_ETCD_ENDPOINTS must list all three etcd endpoints (operations.md s1)")
        if self.deadman_url is None and not self.single_pc:
            # A single PC has no off-node receiver (a dead-man on the same PC cannot report the PC being down):
            # the heartbeat is then disabled with one logged line instead of refusing to start (2026-11-19).
            out.append("XTNL_DEADMAN_URL (Melbourne dead-man receiver) is not set (operations.md s9)")
        return out


def _replace(cfg: RuntimeConfig, changes: Mapping[str, object]) -> RuntimeConfig:
    import dataclasses
    return dataclasses.replace(cfg, **changes)  # type: ignore[arg-type]
