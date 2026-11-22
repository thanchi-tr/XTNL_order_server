"""Home-server review round 1: operator text and start-up warnings that go with the fixes.

* W1: deadman-alert.md documents the engine's own page, its info page, XTNL_DEADMAN_ALERT_AFTER_S (60..3600, exit
  78), the start-up token ERROR and the three gauges; the env examples document the variable; single-pc-autostart.md
  lists the new exit-78 causes.
* W2: single-pc-autostart.md says the heartbeat copy names belong in the User environment and that backup prefixes
  are refused; local-live.env.example says so in a comment, without an assignment.
* W19: start-xtnl.ps1 warns while XTNL_NODE_ID is an example's name.
* W12: the runbook describes the countdown abort.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.deploy.test_deploy_assets import _assignments
from tests.deploy.test_single_pc_autostart import _code_lines

ROOT = Path(__file__).resolve().parents[2]
RUNBOOKS = ROOT / "docs" / "runbooks"
START = ROOT / "deploy" / "local" / "start-xtnl.ps1"


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def test_deadman_alert_documents_the_engine_page() -> None:
    text = (RUNBOOKS / "deadman-alert.md").read_text(encoding="utf-8")
    sec = text[text.index("## Receiver not acknowledging (engine page)"):text.index("## Token rotation")]
    flat = _flat(sec)
    for needle in ("PAGE[high] dead-man receiver not acknowledging", "PAGE[info] dead-man receiver acknowledging again",
                   "XTNL_DEADMAN_ALERT_AFTER_S", "60..3600", "default 300", "exit 78",
                   "XTNL_DEADMAN_URL is set but XTNL_DEADMAN_TOKEN is empty", "/healthz", "HTTP 401", "Tailscale",
                   "notification_dedupe", "xtnl_deadman_configured", "xtnl_deadman_consecutive_failures",
                   "xtnl_deadman_last_ok_age_seconds", "monotonic", "deadman-receiver-home.md"):
        assert needle in flat, needle
    home = (RUNBOOKS / "deadman-receiver-home.md").read_text(encoding="utf-8")
    assert '"Receiver not acknowledging (engine page)"' in _flat(home)


def test_the_page_body_names_the_runbook_section() -> None:
    from src.runtime.deadman import ACK_RUNBOOK
    text = (RUNBOOKS / "deadman-alert.md").read_text(encoding="utf-8")
    assert ACK_RUNBOOK == "deadman-alert.md, receiver not acknowledging"
    assert f"({ACK_RUNBOOK})" in _flat(text)


def test_env_examples_document_the_threshold() -> None:
    engine_text, engine_assigned = _assignments(ROOT / "deploy" / "env" / "engine.env.example")
    assert engine_assigned["XTNL_DEADMAN_ALERT_AFTER_S"] == "300"
    live_text, live_assigned = _assignments(ROOT / "deploy" / "env" / "local-live.env.example")
    assert "XTNL_DEADMAN_ALERT_AFTER_S" not in live_assigned
    assert "# XTNL_DEADMAN_ALERT_AFTER_S=300" in live_text
    for text in (engine_text, live_text):
        assert "60..3600" in text and "exit 78" in text


def test_live_env_example_says_the_heartbeat_copy_belongs_in_the_user_environment() -> None:
    text, assigned = _assignments(ROOT / "deploy" / "env" / "local-live.env.example")
    assert "XTNL_HEARTBEAT_COPY_URL" not in assigned and "XTNL_HEARTBEAT_COPY_TOKEN" not in assigned
    flat = _flat(text)
    assert "XTNL_HEARTBEAT_COPY_URL" in flat and "Windows User environment only" in flat


def test_single_pc_runbook_lists_the_new_refusals_and_exit_78_causes() -> None:
    flat = _flat((RUNBOOKS / "single-pc-autostart.md").read_text(encoding="utf-8"))
    for needle in ("XTNL_HEARTBEAT_COPY_URL", "XTNL_HEARTBEAT_COPY_TOKEN", "`XTNL_BACKUP_`, `RCLONE_` or `B2_`",
                   "$ForbiddenPrefixesInFile", "warning: this account's environment holds",
                   "XTNL_DEADMAN_ALERT_AFTER_S` outside 60..3600",
                   "XTNL_DEADMAN_URL is set but XTNL_DEADMAN_TOKEN is empty",
                   "warning: XTNL_NODE_ID is the example's 'pc1'", "shutdown aborted (shutdown /a)"):
        assert needle in flat, needle


def test_start_xtnl_warns_about_an_example_node_id_without_refusing() -> None:
    code = "\n".join(_code_lines(START.read_text(encoding="utf-8")))
    assert "$ExampleNodeIds = @('pc1', 'node-a')" in code
    warn = code[code.index("if ($ExampleNodeIds -contains $nodeId)"):]
    warn = warn[:warn.index("}")]
    assert "Write-Log (\"warning: XTNL_NODE_ID is the example's" in warn and "ExitCode" not in warn
    # after the env file is loaded into the process, before the User environment step
    assert code.index("env file loaded:") < code.index("$ExampleNodeIds -contains $nodeId") < code.index(
        "foreach ($n in $UserVarNames)")


def test_runbook_index_lists_the_new_runbooks() -> None:
    index = (RUNBOOKS / "README.md").read_text(encoding="utf-8")
    assert "](home-backup-restore.md)" in index and "](deadman-receiver-home.md)" in index
