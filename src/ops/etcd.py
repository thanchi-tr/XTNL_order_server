"""etcd access for the operator CLI (XTNL_ETCD_ENDPOINTS, comma separated, all three members).

* SAFE_HOLD mirror `/xtnl/safe_hold`: read for `safe-hold status`, written after `safe-hold resume` in
  exactly the format the leader writes (`src/runtime/cluster.py mirror_json`).
* `header()`: the cluster_id / revision etcd reports, so `epoch show` / `epoch adopt` compare PostgreSQL's
  `engine_epoch.cluster_id` with the etcd cluster the nodes actually talk to.
* `get_prefix()`: every key under a prefix, e.g. the nodes' health records under `HEALTH_PREFIX` (the one constant,
  defined in `src/runtime/etcd.py`; parse with `src/runtime/health.py HealthRecord.from_json`), which carry each
  node's ARM registry tokens (design §B6.1, PR13; read by `arm config accept`, S33). The implementation is the
  runtime client's (`HttpEtcdClient.get_prefix`).

The client is synchronous (`src/runtime/etcd.py`); the CLI calls it through `asyncio.to_thread`.
"""
from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any, Final, Protocol

from src.runtime.etcd import HEALTH_PREFIX, SAFE_HOLD_KEY, HttpEtcdClient, KeyValue

ENDPOINTS_VAR: Final = "XTNL_ETCD_ENDPOINTS"


class OpsEtcd(Protocol):
    def get(self, key: str, *, timeout_s: float) -> KeyValue | None: ...

    def put(self, key: str, value: str, *, lease_id: int = 0, timeout_s: float) -> int: ...

    def get_prefix(self, prefix: str, *, timeout_s: float) -> list[KeyValue]: ...

    def header(self, *, timeout_s: float) -> tuple[str, int]:
        """(cluster_id, current revision) from a range response header."""
        ...

    def close(self) -> None: ...


def endpoints_from_env(environ: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(x.strip() for x in environ.get(ENDPOINTS_VAR, "").split(",") if x.strip())


class OpsEtcdClient(HttpEtcdClient):
    """HttpEtcdClient (endpoint failover inside the time budget) plus the response header."""

    def header(self, *, timeout_s: float) -> tuple[str, int]:
        key = base64.b64encode(SAFE_HOLD_KEY.encode()).decode("ascii")
        body: dict[str, Any] = self._post("/v3/kv/range", {"key": key, "count_only": True}, timeout_s)
        h = body.get("header") or {}
        return str(h.get("cluster_id", "")), int(h.get("revision", 0))


def make_etcd(endpoints: Sequence[str]) -> OpsEtcd | None:
    return OpsEtcdClient(endpoints) if endpoints else None


__all__ = ["ENDPOINTS_VAR", "HEALTH_PREFIX", "OpsEtcd", "OpsEtcdClient", "endpoints_from_env", "make_etcd"]
