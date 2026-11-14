"""IMPLEMENTATION_PLAN 8.2 deploy / runbook invariants.

* The Melbourne dashboard host install deploys the leader heartbeat listener: heartbeat.json (0640 root:xtnl),
  its TLS certificate / key, port 8444 open from both engine nodes only, and a unit that can read the file.
* The dead-man receiver's token rotation is documented as implemented (current / next / overlap, secrets source).
* The maintenance runbook states the end-of-window behaviour and the hold-back as the engine implements them
  (no 30 s settle after the window).
Stdlib + the project's own modules only; nothing here opens a network connection."""
from __future__ import annotations

import re
from pathlib import Path

from src.calendar import readiness as R
from src.dashboard import heartbeat as H
from src.deadman_receiver import settings as RS
from src.deadman_receiver import tokens as T
from src.oversee import inhibit as I
from tests.deploy.test_deploy_assets import read_unit

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
RUNBOOKS = ROOT / "docs" / "runbooks"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    nxt = text.find("\n## ", start + len(heading))
    return text[start:] if nxt < 0 else text[start:nxt]


# ============================================================================================ dashboard host
def test_dashboard_install_deploys_the_heartbeat_listener() -> None:
    sec = _section(_text(RUNBOOKS / "install-node.md"), "## Melbourne dashboard host")
    hb = H.DEFAULT_HEARTBEAT_FILE
    assert hb == "/etc/xtnl/dashboard/heartbeat.json"
    # the file and both TLS files are installed 0640 root:xtnl, from the shipped example
    for target in (hb, "/etc/xtnl/dashboard/heartbeat.crt", "/etc/xtnl/dashboard/heartbeat.key"):
        assert re.search(rf"install -m 0640 -o root -g xtnl [^\n]+ {re.escape(target)}", sec), target
    assert "deploy/env/dashboard-heartbeat.json.example" in sec
    assert "XTNL_HEARTBEAT_COPY_TOKEN" in sec and "XTNL_HEARTBEAT_COPY_URL" in sec
    # the directory the unit's user traverses, and a readability check as that user
    assert "install -d -m 0750 -o root -g xtnl /etc/xtnl /etc/xtnl/dashboard" in sec
    assert f"sudo -u xtnl test -r {hb}" in sec
    # firewall: 8444 from node A and node B only
    assert str(H.DEFAULT_HEARTBEAT_PORT) == "8444"
    assert "8444 from node A and node B only" in sec
    assert "${NODE_A_IP}/32" in sec and "${NODE_B_IP}/32" in sec
    assert ":8444/xtnl/heartbeat" in sec and "prints `401`" in sec
    # the old check that could not pass without a listener is now backed by one
    assert '"stale": false' in sec


def test_the_dashboard_unit_reads_the_heartbeat_file_as_its_group() -> None:
    cp, multi = read_unit("xtnl-dashboard.service")
    svc = cp["Service"]
    assert (svc["User"], svc["Group"]) == ("xtnl", "xtnl")   # 0640 root:xtnl: readable through the group
    assert "DynamicUser" not in svc                          # a dynamic user would not be in group xtnl
    for key in ("ExecStartPre", "ExecStart"):
        assert f"--heartbeat-file {H.DEFAULT_HEARTBEAT_FILE}" in svc[key], key
    assert "--check-config" in svc["ExecStartPre"]
    assert svc["ProtectSystem"] == "strict"                  # read-only /etc still readable
    for path_key in ("InaccessiblePaths", "TemporaryFileSystem"):
        assert "/etc" not in " ".join(multi["Service"].get(path_key, [])), path_key
    assert "AF_INET" in svc["RestrictAddressFamilies"] and svc["CapabilityBoundingSet"] == ""   # 8444 > 1024
    assert svc["RestartPreventExitStatus"] == "78"


def test_the_readme_and_env_example_list_the_heartbeat_file() -> None:
    readme = _text(DEPLOY / "README.md")
    row = next(ln for ln in readme.splitlines() if ln.startswith("| `env/dashboard-heartbeat.json.example`"))
    assert H.DEFAULT_HEARTBEAT_FILE in row and "0640 root:xtnl" in row and ":8444" in row and "Melbourne" in row
    assert "`DASHBOARD_VPN_IP`" in readme
    assert H.DEFAULT_HEARTBEAT_FILE in _text(DEPLOY / "env" / "dashboard.env.example")
    assert "8444" in _text(DEPLOY / "env" / "engine.env.example")


# ============================================================================================ receiver token
def test_receiver_token_rotation_is_documented_as_implemented() -> None:
    env = _text(DEPLOY / "env" / "deadman-receiver.env.example")
    for name in (T.TOKEN_VAR, T.TOKEN_NEXT_VAR, T.TOKEN_OVERLAP_VAR, "XTNL_SECRETS_SOURCE", "XTNL_SECRETS_REGION"):
        assert name in env, name
    assert f"{T.TOKEN_OVERLAP_VAR}={T.DEFAULT_OVERLAP_S}" in env
    rb = _section(_text(RUNBOOKS / "deadman-alert.md"), "## Token rotation")
    assert T.TOKEN_NEXT_VAR in rb and "Never update the engine first" in rb and "no refused heartbeat" in rb
    assert RS.FORBIDDEN_REGION == "eu-west-2"
    engine = _text(DEPLOY / "env" / "engine.env.example")
    assert "receiver first" in engine and "Token rotation" in engine


def test_null_health_fields_are_documented_as_paging_at_once() -> None:
    rb = _text(RUNBOOKS / "deadman-alert.md")
    assert "null in the latest beat (at once, no grace)" in rb and "null for > 60 s" not in rb


# ============================================================================================ maintenance
def test_maintenance_runbook_describes_the_end_of_window_as_implemented() -> None:
    rb = _text(RUNBOOKS / "broker-maintenance.md")
    assert "30 s settle" not in rb and "plus 30 s" not in rb
    assert "only while the window lasts" in rb and "no settle" in rb
    end = rb[rb.index("**At the end of the window**"):]
    assert "at once" in end and "info summary" in end
    assert f"{int(R.AFTER_MAINTENANCE_WAIT_S)} s" in end and "PRE_OPEN" in end
    # the code it describes: no settle constant, the inhibitor's docstring says the hold-back ends at once
    assert not hasattr(I, "MAINTENANCE_SETTLE_MS")
    assert "ends the hold-back at once" in (I.__doc__ or "")


# ============================================================================================ skeptic re-check
def test_the_dashboard_install_commands_are_runnable_as_written() -> None:
    """The curl check is one command line (a literal newline had split its `-w` format across two lines), and the
    readability check covers all three files the unit's --check-config loads."""
    sec = _section(_text(RUNBOOKS / "install-node.md"), "## Melbourne dashboard host")
    assert r"-w '%{http_code}\n' --cacert /etc/xtnl/pki/ca-bundle.crt -X POST https://" in sec
    assert not re.search(r"%\{http_code\}\s*\n\s*'", sec)
    for f in ("heartbeat.json", "heartbeat.crt", "heartbeat.key"):
        assert f"sudo -u xtnl test -r /etc/xtnl/dashboard/{f}" in sec, f


def test_receiver_rotation_waits_for_the_receiver_and_keeps_next_out_of_the_env_file() -> None:
    """Step 2 (switch the engine) only after the receiver logged the next token (the message TokenRing logs), and
    with aws-sm the next token is not left in the env file, where removing it from the secret would not retire it."""
    rb = _section(_text(RUNBOOKS / "deadman-alert.md"), "## Token rotation")
    assert "tokens rotated: current + next" in rb
    src = _text(ROOT / "src" / "deadman_receiver" / "tokens.py")
    assert "dead-man receiver tokens rotated: %s" in src and '"current + next" if new.next' in src
    assert f"keep `{T.TOKEN_NEXT_VAR}` out of `/etc/xtnl/deadman-receiver.env`" in rb
