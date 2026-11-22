"""W12 review round 3: one odd mains reading on battery is not mains coming back (deploy/local/ups-watch.ps1).

Before, a single non-battery reading after the stop restarted the engine task and reset the state, and the next
poll drained it again out of the remaining battery runtime. Now mains after battery needs two readings in a row
(the countdown's rule), in the main loop and in the re-check right after the stop.

The behaviour tests run the real script with a scripted Get-Power (one letter per reading: B battery and low, M
mains); after the script the stub exits, so a watcher that would run for ever ends.
"""
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

from tests.deploy.test_home_ups_watch import POWERSHELL, STATE_MARKER, WATCH, needs_powershell

SEQ_STUBS = r"""
$script:Calls = 0
function Get-Power {
    $script:Calls++
    $seq = $env:SIM_SEQ -split ','
    if ($script:Calls -gt $seq.Count) { Write-Log 'SIM: end of readings'; exit 0 }
    $onb = $seq[$script:Calls - 1] -eq 'B'
    $line = 'Online'
    if ($onb) { $line = 'Offline' }
    return @{ Ok = $true; Present = $true; OnBattery = $onb; Charge = 80; RuntimeMin = 5; AcLine = $line; Error = '' }
}
function Test-EngineRunning { return $true }
function Start-EngineTask { Write-Log 'SIM: Start-ScheduledTask XTNL engine' }
$ShutdownExe = Join-Path $Root 'fake-shutdown.cmd'
$MutexName = 'Local\XTNL-ups-watch-test-' + $PID
"""


def _run(tmp_path: Path, seq: str, args: list[str]) -> tuple[int, str]:
    local = tmp_path / "deploy" / "local"
    local.mkdir(parents=True, exist_ok=True)
    text = WATCH.read_text(encoding="utf-8")
    assert STATE_MARKER in text
    (local / "ups-watch.ps1").write_text(text.replace(STATE_MARKER, SEQ_STUBS + "\n" + STATE_MARKER),
                                         encoding="utf-8-sig")
    (local / "stop-xtnl.ps1").write_text("param([switch]$Containers)\nWrite-Host \"stub stop containers=$Containers\"\n",
                                         encoding="utf-8")
    (tmp_path / "fake-shutdown.cmd").write_text("@echo %* >> \"%~dp0shutdown-called.txt\"\r\n@exit /b 0\r\n",
                                                encoding="ascii")
    env = {k: v for k, v in os.environ.items() if not k.startswith("SIM_")}
    env["SIM_SEQ"] = seq
    assert POWERSHELL is not None
    out = subprocess.run([POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                          str(local / "ups-watch.ps1"), "-PollSeconds", "5", *args],
                         capture_output=True, text=True, timeout=120, env=env, check=False)
    log = "".join(p.read_text(encoding="utf-8") for p in sorted((tmp_path / "var" / "logs").glob("xtnl-*.log")))
    return out.returncode, log


def _code() -> str:
    text = re.sub(r"<#.*?#>", "", WATCH.read_text(encoding="utf-8"), flags=re.S)
    return "\n".join(ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#"))


def test_mains_after_battery_is_confirmed_like_the_countdown() -> None:
    code = _code()
    confirm = code[code.index("function Confirm-Mains"):code.index("function Test-Low")]
    assert "$AbortAfterReadings" in confirm and "Start-Sleep -Seconds $CountdownPollS" in confirm
    battery = code[code.index("function Step-OnBattery"):code.index("$problems = @()")]
    assert "$again = Confirm-Mains $again" in battery
    main = code[code.index("while ($true) {"):]
    assert "if ($script:S.Mode -eq 'battery') { $mains = Confirm-Mains $p }" in main
    assert "Step-OnMains $p" not in main                   # never on one reading


@needs_powershell
def test_one_odd_reading_after_the_stop_does_not_cancel_the_shutdown(tmp_path: Path) -> None:
    # 1 main loop: battery, low -> stop; 2 re-check after the stop: mains (a glitch); 3 its confirmation: battery
    code, log = _run(tmp_path, "B,M,B", ["-ShutdownDelayS", "0"])
    assert code == 0, log
    assert log.count("[stop-xtnl] stub stop containers=True") == 1, log
    assert "one mains reading after the stop was not repeated" in log
    assert "mains power returned while stopping" not in log and "SIM: Start-ScheduledTask" not in log
    assert (tmp_path / "shutdown-called.txt").read_text(encoding="ascii").split()[:3] == ["/s", "/t", "0"]


@needs_powershell
def test_one_odd_reading_in_the_main_loop_neither_restarts_nor_drains_again(tmp_path: Path) -> None:
    # -NoShutdown keeps the watcher running after the stop: 1 stop, 2 re-check battery, 3 a mains glitch,
    # 4 its confirmation battery, 5 battery; then the stub ends the run
    code, log = _run(tmp_path, "B,B,M,B,B", ["-NoShutdown"])
    assert code == 0, log
    assert log.count("[stop-xtnl] stub stop containers=True") == 1, log
    assert "one mains reading on battery was not repeated" in log
    assert "SIM: Start-ScheduledTask" not in log and "mains power back" not in log
    assert "SIM: end of readings" in log


@needs_powershell
def test_two_mains_readings_in_a_row_start_the_engine_again(tmp_path: Path) -> None:
    code, log = _run(tmp_path, "B,B,M,M", ["-NoShutdown"])
    assert code == 0, log
    assert log.count("[stop-xtnl] stub stop containers=True") == 1, log
    assert "mains power back" in log and log.count("SIM: Start-ScheduledTask XTNL engine") == 1
