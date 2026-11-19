"""The uptime / catch-up settings (migration 0012) are documented as assignments in both env examples, with values
the engine accepts."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.runtime.catchup import CatchupSettings
from src.runtime.uptime import ping_s_from_env

ENV = Path(__file__).resolve().parents[2] / "deploy" / "env"
VARS = ("XTNL_UPTIME_PING_S", "XTNL_CATCHUP_OVERLAP_S", "XTNL_CATCHUP_FIRST_RUN_DAYS")


@pytest.mark.parametrize("name", ["engine.env.example", "local.env.example", "local-live.env.example"])
def test_examples_assign_valid_uptime_settings(name: str) -> None:
    assigned: dict[str, str] = {}
    for raw in (ENV / name).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            assigned[k.strip()] = v.strip()
    assert set(VARS) <= assigned.keys()
    ping = ping_s_from_env(assigned)
    s = CatchupSettings.from_env(assigned, ping_s=ping)
    assert ping == 900 and s.overlap_ms >= ping * 1000 and s.first_run_days == 7
