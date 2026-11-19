"""Single-PC self-healing assets (operations.md §4 / §17; docs/runbooks/single-pc-autostart.md).

start-xtnl.ps1 and run-local.ps1 start the engine with XTNL_SINGLE_PC=1 (unless the env file sets it); the AWS
systemd units and engine.env.example never turn it on; the start path never keeps the engine off silently (a clock
wait that gives up starts the engine anyway, failed attempts and restart loops log ALERT lines). Text checks only.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.deploy.test_deploy_assets import _assignments, env_example
from tests.deploy.test_single_pc_autostart import _code_lines, _script

ROOT = Path(__file__).resolve().parents[2]
FLAG = "XTNL_SINGLE_PC"


def _attempt(code: str) -> str:
    return code[code.index("function Invoke-Attempt"):code.index("function Invoke-Cleanup")]


def test_start_script_sets_the_single_pc_flag_for_the_engine_before_starting_it() -> None:
    code = "\n".join(_code_lines(_script("start-xtnl.ps1")))
    main = code[code.index("function Invoke-Main"):code.index("function Get-ExitMeaning")]
    set_at = main.index(f"[Environment]::SetEnvironmentVariable('{FLAG}', '1', 'Process')")
    assert main.index("Read-EnvFile $EnvFile") < set_at < main.index("Invoke-Attempt")
    # only when neither the env file nor the Windows User environment set it
    assert f"GetEnvironmentVariable('{FLAG}', 'Process')).Trim()" in main and "if ($singlePc -eq '')" in main
    # the engine child keeps it: it is not in the engine's dropped names
    drop = re.search(r"\$EngineDropEnv = @\(([^)]*)\)", code)
    assert drop is not None and FLAG not in drop.group(1)


def test_run_local_sets_the_flag_unless_given() -> None:
    code = "\n".join(_code_lines(_script("run-local.ps1")))
    assert f"[Environment]::SetEnvironmentVariable('{FLAG}', '1', 'Process')" in code
    assert code.index(f"'{FLAG}', '1'") < code.index("& $py -m src.main --check-config")


def test_aws_never_turns_it_on() -> None:
    for unit in (ROOT / "deploy" / "systemd").glob("*.service"):
        assert FLAG not in unit.read_text(encoding="utf-8"), unit.name
    _, assigned = env_example()
    assert assigned[FLAG] == "0"
    text, _ = env_example()
    assert "NEVER 1 on the AWS nodes" in text
    for name in ("XTNL_SINGLE_PC_RESUME_FIRST_S", "XTNL_SINGLE_PC_RESUME_MAX_S", "XTNL_SINGLE_PC_RESUME_LIMIT",
                 "XTNL_SINGLE_PC_RESUME_WINDOW_S", "XTNL_SINGLE_PC_IDLE_PAGE_S"):
        assert name in assigned, name


def test_live_env_example_turns_it_on() -> None:
    _, live = _assignments(ROOT / "deploy" / "env" / "local-live.env.example")
    assert live[FLAG] == "1"


def test_clock_give_up_starts_the_engine_instead_of_looping() -> None:
    code = "\n".join(_code_lines(_script("start-xtnl.ps1")))
    attempt = _attempt(code)
    clock = attempt[attempt.index("if (-not (Wait-Clock))"):attempt.index("$RunLocal")]
    assert "if ($NoRetry) { $script:ExitCode = 13; return }" in clock
    assert "ALERT: clock not within the limit" in clock and "starting the engine anyway" in clock
    assert "if (-not (Wait-Clock)) { $script:ExitCode = 13; return }" not in attempt


def test_failed_attempts_and_restart_loops_log_alert_lines() -> None:
    start = "\n".join(_code_lines(_script("start-xtnl.ps1")))
    assert re.search(r"\[int\]\$AlertAfterMinutes = 30\b", _script("start-xtnl.ps1"))
    assert "ALERT: the engine has not run for" in start and "AddMinutes(60)" in start
    assert "ALERT: configuration refused (exit 78" in start
    run_local = "\n".join(_code_lines(_script("run-local.ps1")))
    assert "ALERT: engine restart loop" in run_local and "$hardExits.Count -ge 5" in run_local
    runbook = (ROOT / "docs" / "runbooks" / "single-pc-autostart.md").read_text(encoding="utf-8")
    for needle in ("ALERT:", "SAFE_HOLD on this PC", "system.single-pc-auto-resume", "XTNL_SINGLE_PC=1",
                   "engine not recording on the single PC", "SAFE_HOLD auto-resume stopped"):
        assert needle in runbook, needle


def test_status_lists_alert_and_critical_page_lines_from_every_log() -> None:
    st = "\n".join(_code_lines(_script("status-xtnl.ps1")))
    assert re.search(r"\[int\]\$AlertLines = 20\b", _script("status-xtnl.ps1"))
    section = st[st.index("ALERT / PAGE[critical]"):st.index("Show-Title 'engine uptime")]
    assert "Select-String -LiteralPath" in section and "Sort-Object LastWriteTime" in section
    assert "-Pattern 'ALERT:|PAGE\\[critical\\]|direct page'" in section
    assert "Select-Object -Last $AlertLines" in section and "Format-LogLine $_.Line" in section
    assert "if ($Text -match $Hide)" in st                 # the same credential guard as the log tail


def test_rules_name_the_single_pc_deviation() -> None:
    ops = (ROOT / ".claude" / "rules" / "operations.md").read_text(encoding="utf-8")
    assert "**Single-PC deviation**" in ops and "XTNL_SINGLE_PC=1" in ops
    assert "**Self-healing start**" in ops
    assert "**Single-PC exception**" in ops and "DirectChannelPager" in ops    # §2: critical pages without a leader
