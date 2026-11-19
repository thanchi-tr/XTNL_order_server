"""A single PC may leave XTNL_DEADMAN_URL empty (2026-11-19: an empty value refused the start with exit 78)."""
from __future__ import annotations

from src.runtime.config import RuntimeConfig

BASE = {"XTNL_ETCD_ENDPOINTS": "http://a:2379,http://b:2379,http://c:2379", "BROKER_ENV": "sandbox"}


def _deadman_problems(env: dict[str, str]) -> list[str]:
    return [p for p in RuntimeConfig.from_env(env).problems() if "XTNL_DEADMAN_URL" in p]


def test_missing_deadman_url_still_refuses_outside_single_pc() -> None:
    assert _deadman_problems(dict(BASE))
    assert _deadman_problems({**BASE, "XTNL_DEADMAN_URL": ""})
    assert _deadman_problems({**BASE, "XTNL_SINGLE_PC": "0"})


def test_single_pc_may_leave_the_deadman_url_empty() -> None:
    assert _deadman_problems({**BASE, "XTNL_SINGLE_PC": "1"}) == []
    assert _deadman_problems({**BASE, "XTNL_SINGLE_PC": "1", "XTNL_DEADMAN_URL": ""}) == []
    assert RuntimeConfig.from_env({**BASE, "XTNL_SINGLE_PC": "1"}).deadman_url is None


def test_single_pc_still_requires_three_etcd_endpoints() -> None:
    env = {"BROKER_ENV": "sandbox", "XTNL_SINGLE_PC": "1", "XTNL_ETCD_ENDPOINTS": "http://a:2379"}
    assert any("XTNL_ETCD_ENDPOINTS" in p for p in RuntimeConfig.from_env(env).problems())
