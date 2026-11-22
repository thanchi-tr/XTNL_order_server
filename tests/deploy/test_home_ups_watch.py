"""UPS pre-shutdown (home-server plan W12; docs/runbooks/single-pc-autostart.md "UPS: clean stop before the battery runs
out").

The preferred path is PowerPanel Personal (documented in the runbook); deploy/local/ups-watch.ps1 is the fallback: it
runs in the engine user's session, polls Win32_Battery, and only on battery below a threshold drains the engine with
stop-xtnl.ps1 -Containers and then shuts Windows down. It never acts on mains power, does nothing without a battery,
and -DryRun never acts. install-ups-watch.ps1 registers it like install-autostart.ps1 (AtLogOn, Interactive).
Static checks of the scripts and the runbook; the behaviour tests run a copy of the watcher with stubbed readings, a
stub stop-xtnl.ps1 and a stub shutdown command under Windows PowerShell when it is available (never the real ones).
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LOCAL = ROOT / "deploy" / "local"
WATCH = LOCAL / "ups-watch.ps1"
INSTALL = LOCAL / "install-ups-watch.ps1"
RUNBOOK = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
FORBIDDEN_CALLS = ("place_order", "modify_order", "cancel_order")


def _code_lines(text: str) -> list[str]:
    """PowerShell lines outside comment-based help and line comments."""
    text = re.sub(r"<#.*?#>", "", text, flags=re.S)
    return [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]


def _code(path: Path) -> str:
    return "\n".join(_code_lines(path.read_text(encoding="utf-8")))


def _function(code: str, name: str) -> str:
    """The body of a top-level PowerShell function (up to the next top-level line)."""
    m = re.search(rf"^function {re.escape(name)}\b.*?\n(.*?)(?=^\S)", code + "\n#END", re.M | re.S)
    assert m, f"function {name} missing"
    return m.group(1)


def _param_defaults(code: str) -> dict[str, str]:
    block = code[code.index("param("):code.index(")\n$ErrorActionPreference")]
    return dict(re.findall(r"\[int\]\$(\w+) = (\d+)", block))


def _ranges(code: str) -> set[str]:
    return set(re.findall(r"'(-\w+ must be [^']+)'", code))


# ---------------------------------------------------------------------------------------------- ups-watch.ps1
def test_watch_parameters_and_defaults() -> None:
    code = _code(WATCH)
    assert _param_defaults(code) == {"PollSeconds": "30", "ChargePercent": "45", "RuntimeMinutes": "10",
                                     "ShutdownDelayS": "60", "StopBudgetS": "300"}
    for switch in ("NoShutdown", "DryRun", "Once"):
        assert f"[switch]${switch}" in code, switch


def test_watch_reads_win32_battery_and_needs_two_sources_for_on_battery() -> None:
    code = _code(WATCH)
    assert "Get-CimInstance -ClassName Win32_Battery" in code
    assert "$Discharging = 1" in code and "$RunTimeUnknown = 71582788" in code
    power = _function(code, "Get-Power")
    for prop in ("BatteryStatus", "EstimatedChargeRemaining", "EstimatedRunTime"):
        assert prop in power, prop
    # a discharging battery AND a power line that is not Online: one odd reading never acts on mains power
    assert "$r.OnBattery = $discharging -and ($r.AcLine -ne 'Online')" in power
    assert "PowerLineStatus" in _function(code, "Get-AcLine")
    low = _function(code, "Test-Low")
    assert "$P.Charge -le $ChargePercent" in low and "$P.RuntimeMin -le $RuntimeMinutes" in low
    assert "$ChargePercent -gt 0" in low and "$RuntimeMinutes -gt 0" in low      # 0 turns a trigger off


def test_watch_acts_only_on_battery_and_in_the_right_order() -> None:
    code = _code(WATCH)
    mains = _function(code, "Step-OnMains")
    assert "Invoke-StopXtnl" not in mains and "Invoke-Shutdown" not in mains   # never acts on mains power
    battery = _function(code, "Step-OnBattery")
    dry = battery.index("\n    if ($DryRun) {")
    low = battery.index("if (-not (Test-Low $P)) { return $false }")
    stop = battery.index("$code = Invoke-StopXtnl")
    reread = battery.index("$again = Get-Power")
    no_shutdown = battery.index("\n    if ($NoShutdown) {")
    shutdown = battery.index("$code = Invoke-Shutdown")
    assert low < dry < stop < reread < no_shutdown < shutdown
    assert battery[dry:stop].count("return $false") >= 1                     # a dry run returns before acting
    assert "ALERT: UPS battery low" in battery
    # the main loop: no battery -> one log line, nothing else; mains -> Step-OnMains (after battery only once two
    # readings in a row agree, review round 3); battery (or an unconfirmed mains reading) -> Step-OnBattery
    loop = code[code.index("while ($true) {"):]
    assert "if (-not $p.Present)" in loop and "NoBatteryLogged" in loop
    assert loop.index("if (-not $p.Present)") < loop.index("Step-OnMains $mains") < loop.index("Step-OnBattery $battery")


def test_watch_runs_stop_xtnl_with_containers_then_shutdown() -> None:
    code = _code(WATCH)
    assert "$StopScript = Join-Path $PSScriptRoot 'stop-xtnl.ps1'" in code
    stop = _function(code, "Invoke-StopXtnl")
    assert "-NoProfile -ExecutionPolicy Bypass -File \"{0}\" -Containers" in stop
    assert "$psi.UseShellExecute = $false" in stop and "WaitForExit($StopBudgetS * 1000)" in stop
    shut = _function(code, "Invoke-Shutdown")
    assert "@('/s', '/t', [string]$ShutdownDelayS" in shut
    assert "$ShutdownExe = Join-Path $env:SystemRoot 'System32\\shutdown.exe'" in code
    assert "$ShutdownAlready = @(1115, 1190)" in code


def test_watch_restarts_the_engine_only_if_it_stopped_it() -> None:
    code = _code(WATCH)
    battery = _function(code, "Step-OnBattery")
    assert battery.index("$script:S.WasRunning = Test-EngineRunning") < battery.index("$code = Invoke-StopXtnl")
    mains = _function(code, "Step-OnMains")
    assert "if ($script:S.Stopped)" in mains and "if ($script:S.WasRunning) { Start-EngineTask }" in mains
    assert "Start-ScheduledTask -TaskName $TaskName" in _function(code, "Start-EngineTask")
    assert "$TaskName = 'XTNL engine'" in code


def test_watch_logs_to_the_daily_log_and_has_exit_codes() -> None:
    code = _code(WATCH)
    log = _function(code, "Write-Log")
    assert "[ups-watch]" in log and "'xtnl-{0}.log'" in log and "AppendAllText" in log
    assert "$LogDir = Join-Path $Root 'var\\logs'" in code
    assert "exit 78" in code and "exit 0" in code
    assert "$MutexName = 'Global\\XTNL-ups-watch'" in code and "if (-not $DryRun)" in code


def test_watch_is_powershell_51_and_changes_nothing_persistent() -> None:
    for path in (WATCH, INSTALL):
        lines = _code_lines(path.read_text(encoding="utf-8"))
        for ln in lines:
            stripped = re.sub(r"'[^']*'|\"[^\"]*\"", "''", ln)            # ignore string contents
            for token in ("&&", "||", "??", "?."):
                assert token not in stripped, f"{path.name}: {ln.strip()}"
            assert not re.search(r"\s\?\s.*\s:\s", stripped), f"{path.name}: ternary: {ln.strip()}"
            for forbidden in ("powercfg", "Set-ItemProperty", "reg add", "setx", "SetEnvironmentVariable",
                              "Set-Service", "w32tm"):
                assert forbidden.lower() not in ln.lower(), f"{path.name}: {ln.strip()}"
        text = path.read_text(encoding="utf-8")
        assert "never sends orders" in text.lower()
        for call in FORBIDDEN_CALLS:
            assert call not in text, call


# ---------------------------------------------------------------------------------------------- install-ups-watch.ps1
def test_install_registers_the_task_like_the_engine_task() -> None:
    code = _code(INSTALL)
    assert "$TaskName = 'XTNL UPS watch'" in code
    assert "New-ScheduledTaskTrigger -AtLogOn -User $User" in code and "$trigger.Delay = 'PT15S'" in code
    assert "-LogonType Interactive" in code and "-RunLevel Limited" in code
    assert "-RestartInterval (New-TimeSpan -Minutes 1) -RestartCount 999" in code
    assert "-ExecutionTimeLimit ([TimeSpan]::Zero)" in code
    for flag in ("-AllowStartIfOnBatteries", "-DontStopIfGoingOnBatteries", "-StartWhenAvailable",
                 "-MultipleInstances IgnoreNew"):
        assert flag in code, flag
    assert "Register-ScheduledTask -TaskName $TaskName -Force" in code
    assert "'-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File \"{0}\"' -f $WatchScript" in code
    assert "$WatchScript = Join-Path $Root 'deploy\\local\\ups-watch.ps1'" in code
    assert "' -PollSeconds {0} -ChargePercent {1} -RuntimeMinutes {2} -ShutdownDelayS {3}' -f" in code
    assert "if ($NoShutdown) { $PsArgs += ' -NoShutdown' }" in code and "if ($DryRun) { $PsArgs += ' -DryRun' }" in code
    assert "'XTNL engine'" not in code                       # it never touches the engine task


def test_install_uninstall_and_shared_parameter_ranges() -> None:
    code = _code(INSTALL)
    uninstall = code[code.index("if ($Uninstall) {"):code.index("$Root =")]
    assert "Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false" in uninstall and "exit 0" in uninstall
    defaults = _param_defaults(code)
    watch_defaults = _param_defaults(_code(WATCH))
    for name in ("PollSeconds", "ChargePercent", "RuntimeMinutes", "ShutdownDelayS"):
        assert defaults[name] == watch_defaults[name], name
    # the installer refuses what the watcher would refuse with exit 78 (a task would hide that exit)
    assert {r for r in _ranges(_code(WATCH)) if not r.startswith("-StopBudgetS")} == _ranges(code)


# ---------------------------------------------------------------------------------------------- runbook
def test_runbook_documents_powerpanel_and_the_fallback() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    sec = text[text.index("## UPS: clean stop before the battery runs out"):text.index("## Checks")]
    flat = re.sub(r"\s+", " ", sec)
    for needle in ("PowerPanel Personal", "runtime threshold", "Run command before shutdown", "stop-xtnl.ps1",
                   "-Containers", "Turn off UPS output after shutdown", "standby-timeout-dc 0",
                   "hibernate-timeout-dc 0", "disk-timeout-dc 0", "SUB_BATTERY BATACTIONLOW 0",
                   "SUB_BATTERY BATACTIONCRIT 0", "Do nothing", "RTC wake", "Restore on AC power loss",
                   "same Windows session", "install-ups-watch.ps1 -StartNow",
                   "install-ups-watch.ps1 -RuntimeMinutes 15 -NoShutdown -StartNow", "install-ups-watch.ps1 -Uninstall",
                   "ups-watch.ps1 -DryRun -Once", "Win32_Battery", "71582788", "Unverified", "SIGINT",
                   "shutdown /s /t 60", "XTNL UPS watch"):
        assert needle in flat, needle
    assert "## UPS: clean stop before the battery runs out" in text
    assert "deploy/local/ups-watch.ps1" in text            # in the file table
    assert "install-ups-watch.ps1 -Uninstall" in text[text.index("## Rollback"):]


# ---------------------------------------------------------------------------------------------- behaviour (stubbed)
POWERSHELL = shutil.which("powershell.exe") or shutil.which("powershell")
STATE_MARKER = "# ------------------------------------------------------------------------------------------------ state machine"

# Readings: on battery (runtime 5 min) for the first SIM_MAINS_AFTER calls (all calls when unset), then mains.
STUBS = r"""
$script:Calls = 0
function Get-Power {
    $script:Calls++
    if ($env:SIM_NO_BATTERY) { return @{ Ok = $true; Present = $false; OnBattery = $false; Charge = $null; RuntimeMin = $null; AcLine = 'Online'; Error = '' } }
    $onb = $true
    if ($env:SIM_MAINS_AFTER -and $script:Calls -gt [int]$env:SIM_MAINS_AFTER) { $onb = $false }
    $line = 'Online'
    if ($onb) { $line = 'Offline' }
    return @{ Ok = $true; Present = $true; OnBattery = $onb; Charge = 80; RuntimeMin = [int]$env:SIM_RUNTIME; AcLine = $line; Error = '' }
}
function Test-EngineRunning { return [bool]$env:SIM_WAS_RUNNING }
function Start-EngineTask { Write-Log 'SIM: Start-ScheduledTask XTNL engine' }
$ShutdownExe = Join-Path $Root 'fake-shutdown.cmd'
$MutexName = 'Local\XTNL-ups-watch-test-' + $PID
"""


def _run_watch(tmp_path: Path, args: list[str], **sim: str) -> tuple[int, str]:
    local = tmp_path / "deploy" / "local"
    local.mkdir(parents=True, exist_ok=True)
    text = WATCH.read_text(encoding="utf-8")
    assert STATE_MARKER in text
    (local / "ups-watch.ps1").write_text(text.replace(STATE_MARKER, STUBS + "\n" + STATE_MARKER), encoding="utf-8-sig")
    (local / "stop-xtnl.ps1").write_text("param([switch]$Containers)\nWrite-Host \"stub stop containers=$Containers\"\n",
                                         encoding="utf-8")
    (tmp_path / "fake-shutdown.cmd").write_text("@echo %* >> \"%~dp0shutdown-called.txt\"\r\n@exit /b 0\r\n",
                                                encoding="ascii")
    env = {k: v for k, v in os.environ.items() if not k.startswith("SIM_")}
    env.update({"SIM_RUNTIME": "5", **sim})
    assert POWERSHELL is not None
    out = subprocess.run([POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                          str(local / "ups-watch.ps1"), "-PollSeconds", "5", *args],
                         capture_output=True, text=True, timeout=120, env=env, check=False)
    logs = sorted((tmp_path / "var" / "logs").glob("xtnl-*.log"))
    log = "".join(p.read_text(encoding="utf-8") for p in logs)
    return out.returncode, log


needs_powershell = pytest.mark.skipif(POWERSHELL is None or os.name != "nt", reason="Windows PowerShell not available")


@needs_powershell
def test_low_battery_stops_then_shuts_down(tmp_path: Path) -> None:
    code, log = _run_watch(tmp_path, [])
    assert code == 0
    assert "ALERT: UPS battery low" in log and "[stop-xtnl] stub stop containers=True" in log
    assert log.index("[stop-xtnl] stub stop") < log.index("shutdown /s /t 60 issued")
    assert (tmp_path / "shutdown-called.txt").read_text(encoding="ascii").split()[:3] == ["/s", "/t", "60"]


@needs_powershell
def test_above_the_thresholds_nothing_happens(tmp_path: Path) -> None:
    code, log = _run_watch(tmp_path, ["-Once"], SIM_RUNTIME="30")
    assert code == 0
    assert "on battery: charge 80 %, runtime 30 min" in log
    assert "ALERT" not in log and "[stop-xtnl]" not in log and not (tmp_path / "shutdown-called.txt").exists()


@needs_powershell
def test_dry_run_never_acts(tmp_path: Path) -> None:
    code, log = _run_watch(tmp_path, ["-DryRun", "-Once"])
    assert code == 0
    assert "DRY RUN: threshold reached" in log
    assert "[stop-xtnl]" not in log and not (tmp_path / "shutdown-called.txt").exists()


@needs_powershell
def test_no_battery_logs_once_and_does_nothing(tmp_path: Path) -> None:
    code, log = _run_watch(tmp_path, ["-Once"], SIM_NO_BATTERY="1")
    assert code == 0
    assert log.count("no battery reported by Win32_Battery") == 1
    assert "[stop-xtnl]" not in log and not (tmp_path / "shutdown-called.txt").exists()


@needs_powershell
def test_mains_back_during_the_stop_skips_the_shutdown_and_restarts_the_engine(tmp_path: Path) -> None:
    code, log = _run_watch(tmp_path, ["-Once"], SIM_MAINS_AFTER="1", SIM_WAS_RUNNING="1")
    assert code == 0
    assert "mains power returned while stopping: no shutdown" in log
    assert "SIM: Start-ScheduledTask XTNL engine" in log
    assert not (tmp_path / "shutdown-called.txt").exists()


@needs_powershell
def test_no_shutdown_only_drains(tmp_path: Path) -> None:
    code, log = _run_watch(tmp_path, ["-NoShutdown", "-Once"])
    assert code == 0
    assert "[stop-xtnl] stub stop containers=True" in log and "left to PowerPanel" in log
    assert not (tmp_path / "shutdown-called.txt").exists()


@needs_powershell
def test_out_of_range_parameters_exit_78(tmp_path: Path) -> None:
    code, log = _run_watch(tmp_path, ["-ChargePercent", "0", "-RuntimeMinutes", "0", "-Once"])
    assert code == 78 and "refused (exit 78)" in log
