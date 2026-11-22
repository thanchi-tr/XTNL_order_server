"""etcd rebuild fix (home-server plan W18; docs/runbooks/single-pc-autostart.md "Rebuild etcd with a new cluster token").

A new etcd cluster's id depends only on the peer URL and --initial-cluster-token, so the live compose file must let a
rebuild set a new token (`${XTNL_ETCD_CLUSTER_TOKEN:-xtnl-local}`), keeping `xtnl-local` as the default so an existing
volume and a plain `up` are unchanged. The runbook's steps must match the code they rely on: the fenced epoch bump
(`revision < $2`), `epoch adopt`'s `already_adopted` refusal, the CLI's real arguments, and the live view's refusal of
any XTNL_*TOKEN name (why the token is set in one shell only, never in local.env or the User environment).
Static checks, plus `docker compose config` when the Docker CLI is on PATH (read-only; no daemon state is touched).
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path

import pytest

from src.db.epoch import SQL_BUMP_EPOCH
from src.liveview.settings import refused_names
from src.ops.cli import build_parser
from src.ops.model import OpsUsageError
from tests.deploy.test_deploy_assets import _assignments, read_yaml
from tests.deploy.test_single_pc_autostart import _list

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy" / "local" / "docker-compose.local.yml"
RUNBOOK = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
LIVE_ENV = ROOT / "deploy" / "env" / "local-live.env.example"
TOKEN_VAR = "XTNL_ETCD_CLUSTER_TOKEN"
DEFAULT_TOKEN = "xtnl-local"
TOKEN_FLAG = "--initial-cluster-token=${XTNL_ETCD_CLUSTER_TOKEN:-xtnl-local}"

_INTERP = re.compile(r"\$\{([A-Z_][A-Z0-9_]*):-([^}]*)\}")


def _interpolate(value: str, env: Mapping[str, str]) -> str:
    """Compose's `${NAME:-default}`: the default when NAME is unset OR empty, else NAME's value."""
    return _INTERP.sub(lambda m: env.get(m.group(1)) or m.group(2), value)


def _etcd_command() -> list[str]:
    return _list(read_yaml(COMPOSE), "services.etcd.command")


def _runbook_section(start: str, end: str) -> str:
    text = RUNBOOK.read_text(encoding="utf-8")
    return text[text.index(start):text.index(end)]


# ---------------------------------------------------------------------------------------------- compose
def test_etcd_token_is_settable_with_the_old_default() -> None:
    cmd = _etcd_command()
    tokens = [c for c in cmd if c.startswith("--initial-cluster-token=")]
    assert tokens == [TOKEN_FLAG]
    # the rest of the bootstrap is unchanged: one member, the same peer URL, a new cluster
    assert "--initial-cluster-state=new" in cmd
    assert "--initial-cluster=etcd-local=http://localhost:2380" in cmd
    assert "--initial-advertise-peer-urls=http://localhost:2380" in cmd
    assert _list(read_yaml(COMPOSE), "services.etcd.volumes") == ["xtnl_local_etcd:/etcd-data"]


@pytest.mark.parametrize(("env", "expected"), [
    ({}, DEFAULT_TOKEN),                                  # unset: an existing volume and a plain `up` are unchanged
    ({TOKEN_VAR: ""}, DEFAULT_TOKEN),                     # empty counts as unset (`:-`), never an empty token
    ({TOKEN_VAR: "xtnl-local-r1"}, "xtnl-local-r1"),      # the rebuild's new token
])
def test_compose_interpolates_the_token(env: dict[str, str], expected: str) -> None:
    flag = next(c for c in _etcd_command() if c.startswith("--initial-cluster-token="))
    assert _interpolate(flag, env) == f"--initial-cluster-token={expected}"


def test_only_the_etcd_service_reads_the_token() -> None:
    text = COMPOSE.read_text(encoding="utf-8")
    code = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
    assert code.count(TOKEN_VAR) == 1
    etcd = re.search(r"^  etcd:\n(.*?)(?=^  \S)", text, re.M | re.S)
    assert etcd is not None and TOKEN_VAR in etcd.group(1)
    # the comment above it says why, and that the flag matters only for a new volume
    assert "W18" in etcd.group(1) and "data directory is empty" in etcd.group(1)


def _docker_compose_config(env_token: str | None) -> list[str]:
    env = {k: v for k, v in os.environ.items() if k.upper() != TOKEN_VAR}
    if env_token is not None:
        env[TOKEN_VAR] = env_token
    out = subprocess.run(["docker", "compose", "-f", str(COMPOSE), "config", "--format", "json"],
                         capture_output=True, text=True, timeout=60, env=env, check=False)
    if out.returncode != 0:
        pytest.skip(f"docker compose config unavailable (exit {out.returncode})")
    command = json.loads(out.stdout)["services"]["etcd"]["command"]
    assert isinstance(command, list)
    return [str(c) for c in command]


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker CLI not on PATH")
def test_docker_compose_config_interpolates_the_token() -> None:
    assert "--initial-cluster-token=xtnl-local" in _docker_compose_config(None)
    assert "--initial-cluster-token=xtnl-local" in _docker_compose_config("")
    assert "--initial-cluster-token=xtnl-local-r1" in _docker_compose_config("xtnl-local-r1")


# ---------------------------------------------------------------------------------------------- the code the runbook relies on
def test_the_fence_and_the_refusal_the_runbook_describes_exist() -> None:
    assert "revision < $2" in SQL_BUMP_EPOCH and "cluster_id = $1" in SQL_BUMP_EPOCH
    service = (ROOT / "src" / "ops" / "service.py").read_text(encoding="utf-8")
    for code in ("already_adopted", "cluster_id_not_reported_by_etcd", "cluster_id_changed"):
        assert f'"{code}"' in service, code


def test_the_live_view_refuses_the_token_name() -> None:
    # Why the runbook sets the token in one shell only: start-xtnl.ps1's live view child would inherit it from
    # deploy\env\local.env or the User environment and exit 78 (src/liveview/settings.py REFUSED_PATTERN).
    assert refused_names({TOKEN_VAR: "xtnl-local-r1"}) == [TOKEN_VAR]
    _, assigned = _assignments(LIVE_ENV)
    assert TOKEN_VAR not in assigned


# ---------------------------------------------------------------------------------------------- runbook
def test_runbook_rebuild_steps_in_order() -> None:
    sec = _runbook_section("## Rebuild etcd with a new cluster token", "## Rollback")
    steps = ["stop-xtnl.ps1", "Disable-ScheduledTask -TaskName 'XTNL engine'", "-m src.ops epoch show",
             "docker compose -f deploy\\local\\docker-compose.local.yml rm -s -f etcd",
             "docker volume rm xtnl_local_etcd",
             f"$env:{TOKEN_VAR} = 'xtnl-local-r20261122-0930'",
             "docker compose -f deploy\\local\\docker-compose.local.yml up -d --wait etcd",
             f"Remove-Item Env:{TOKEN_VAR}",
             "-m src.ops epoch adopt <etcd_cluster_id> --from-cluster-id <cluster_id>",
             "Enable-ScheduledTask -TaskName 'XTNL engine'", "Start-ScheduledTask -TaskName 'XTNL engine'"]
    at = [sec.index(s) for s in steps]
    assert at == sorted(at), [s for s, i in zip(steps, at, strict=True)]
    # the check between the rebuild and the adoption: a second `epoch show` must report a different cluster_id
    assert sec.index("-m src.ops epoch show", at[6]) < at[8]
    assert "--operator <you> --approver <second person>" in sec
    assert "already_adopted" in sec and "revision < $2" in sec
    # review round 1: never a token any earlier volume used, on any PC (the cluster_id depends on token + peer URL)
    flat_rule = re.sub(r"\s+", " ", sec)
    assert "Never reuse a token that any earlier etcd volume used, on any PC" in flat_rule
    assert "xtnl-local-rYYYYMMDD" in sec and "the token was used before" in flat_rule
    flat = re.sub(r"\s+", " ", sec)
    assert "only when the volume is created" in flat
    # never in the env file or the User environment (the live view would exit 78)
    assert "deploy\\env\\local.env" in flat and "Windows User environment" in flat and "exit 78" in flat
    # the alternative: carry the etcd volume over with PostgreSQL from the same moment
    assert "Or carry the etcd volume over" in flat and "from the same moment" in flat
    blocks = "\n".join(re.findall(r"```\n(.*?)```", sec, re.S))
    code = "\n".join(ln.split("#", 1)[0] for ln in blocks.splitlines())     # the commands, not their comments
    assert "down -v" not in code
    assert "volume rm xtnl_local_pg" not in code


def test_runbook_rebuild_commands_parse_with_the_real_cli() -> None:
    parser = build_parser()
    ns = parser.parse_args(["epoch", "show"])
    assert (ns.group, ns.cmd) == ("epoch", "show")
    ns = parser.parse_args(["epoch", "adopt", "111", "--from-cluster-id", "222", "--operator", "a",
                            "--approver", "b", "--reason", "etcd rebuilt with token xtnl-local-r1"])
    assert (ns.group, ns.cmd) == ("epoch", "adopt")
    assert ns.cluster_id == "111" and ns.from_cluster_id == "222" and ns.approver == "b"
    with pytest.raises(OpsUsageError):     # the approver is required for an adoption
        parser.parse_args(["epoch", "adopt", "111", "--from-cluster-id", "222", "--operator", "a",
                           "--reason", "etcd rebuilt with token xtnl-local-r1"])


def test_rollback_no_longer_claims_a_new_volume_means_a_new_cluster_id() -> None:
    text = re.sub(r"\s+", " ", RUNBOOK.read_text(encoding="utf-8"))
    assert "A new etcd volume means a new `cluster_id`" not in text
    rollback = text[text.index("## Rollback"):]
    assert "does **not** get a new `cluster_id`" in rollback and "Rebuild etcd with a new cluster token" in rollback
