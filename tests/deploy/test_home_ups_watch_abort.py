"""W12 review round 1: mains back during the shutdown countdown aborts the shutdown (deploy/local/ups-watch.ps1).

Without it, a cut that ends inside the -ShutdownDelayS countdown still powers the box off, and it stays off (BIOS
"restore on AC loss" never sees an AC loss). The watcher now keeps reading the power during the countdown and runs
`shutdown /a` after two readings in a row on mains, then starts the engine task again.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.deploy.test_home_ups_watch import WATCH, _run_watch, needs_powershell


def _code() -> str:
    text = re.sub(r"<#.*?#>", "", WATCH.read_text(encoding="utf-8"), flags=re.S)
    return "\n".join(ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#"))


def test_the_countdown_is_watched_and_aborted_on_mains() -> None:
    code = _code()
    assert "$CountdownPollS = 5" in code and "$AbortAfterReadings = 2" in code
    wait = code[code.index("function Wait-ShutdownCountdown"):code.index("function Start-EngineTask")]
    assert "Get-Power" in wait and "Invoke-ShutdownAbort" in wait and "Step-OnMains $p" in wait
    assert "@('/a')" in code
    battery = code[code.index("function Step-OnBattery"):]
    assert battery.index("$code = Invoke-Shutdown") < battery.index("if (Wait-ShutdownCountdown) { return $false }")
    help_text = WATCH.read_text(encoding="utf-8").split("#>", 1)[0]
    assert "shutdown /a" in help_text


@needs_powershell
def test_mains_back_during_the_countdown_aborts_the_shutdown_and_restarts_the_engine(tmp_path: Path) -> None:
    # readings: 1 main loop (battery, low) -> stop; 2 after the stop (battery) -> shutdown /s; 3.. mains
    code, log = _run_watch(tmp_path, ["-Once", "-ShutdownDelayS", "30"], SIM_MAINS_AFTER="2", SIM_WAS_RUNNING="1")
    assert code == 0, log
    calls = (tmp_path / "shutdown-called.txt").read_text(encoding="ascii").splitlines()
    assert [c.split()[0] for c in calls] == ["/s", "/a"], calls
    assert "shutdown aborted (shutdown /a)" in log and "SIM: Start-ScheduledTask XTNL engine" in log
    assert "countdown ran out" not in log


@needs_powershell
def test_still_on_battery_the_countdown_runs_out_and_the_watcher_exits(tmp_path: Path) -> None:
    code, log = _run_watch(tmp_path, ["-ShutdownDelayS", "12"])
    assert code == 0, log
    calls = (tmp_path / "shutdown-called.txt").read_text(encoding="ascii").splitlines()
    assert [c.split()[0] for c in calls] == ["/s"]
    assert "shutdown countdown ran out; ups-watch exits" in log
