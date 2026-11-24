"""Live review round 2: a Windows Shut down with Fast Startup on is a log-off, not a sleep.

docs/runbooks/single-pc-autostart.md 'What happens on reboot, sleep, Windows update' already says a log-off is a
hard stop and the next log-on starts the task with a catch-up (operations.md s17: a hard exit leaves the
engine_runs row open). With Fast Startup on, Shut down logs Kernel-Power 42 "entering sleep", which reads like the
'Sleep / hibernate' bullet. The runbook names that case and its evidence, and Prerequisites step 5 turns Fast
Startup off as the home-server plan s4.3 asks (`powercfg /hibernate off`). Text checks only.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
PLAN = ROOT / "docs" / "design" / "home-server-plan.md"


def _section(text: str, heading: str) -> str:
    m = re.search(rf"^## {re.escape(heading)}\n(.*?)(?=^## |\Z)", text, re.M | re.S)
    assert m, heading
    return m.group(1)


def _bullets(section: str) -> list[str]:
    return [b.strip() for b in re.split(r"^\* ", section, flags=re.M)[1:]]


def test_fast_startup_shutdown_bullet_follows_log_off() -> None:
    bullets = _bullets(_section(RUNBOOK.read_text(encoding="utf-8"), "What happens on reboot, sleep, Windows update"))
    heads = [b.split(":**", 1)[0] for b in bullets]
    i_logoff = next(i for i, h in enumerate(heads) if h.startswith("**Log-off"))
    fast = bullets[i_logoff + 1]
    assert fast.startswith("**Shut down while Windows Fast Startup is on"), fast[:80]
    one_line = " ".join(fast.split())
    # The misleading evidence and where the real times are.
    for needle in ("Winlogon 7002", "Kernel-Power 187", "42", '"entering sleep"', "Application API",
                   "Power-Troubleshooter event 1", "506/507", "powercfg /a"):
        assert needle in one_line, needle
    # What actually happened to the engine: the existing 'Log-off' behaviour (operations.md s17).
    for needle in ('as in "Log-off"', "hard stop", "`engine_runs` row open", "`suspended_gaps` 0",
                   "no sleep fence", "`resume_after_sleep`", "`startup` catch-up"):
        assert needle in one_line, needle
    # The fix the home-server plan already asks for, and the resulting behaviour.
    assert "`powercfg /hibernate off`" in one_line and "§4.3" in one_line
    assert '"Reboot"' in one_line
    # No unsupported claim about the wake source.
    assert "Power Button" not in one_line and "power button" not in one_line


def test_sleep_bullet_is_a_real_suspend_only() -> None:
    bullets = _bullets(_section(RUNBOOK.read_text(encoding="utf-8"), "What happens on reboot, sleep, Windows update"))
    sleep = next(b for b in bullets if b.startswith("**Sleep / hibernate"))
    assert "Fast Startup" in " ".join(sleep.split())


def test_prerequisites_turn_fast_startup_off_as_the_plan_asks() -> None:
    assert "powercfg /hibernate off" in PLAN.read_text(encoding="utf-8")       # home-server plan s4.3
    pre = _section(RUNBOOK.read_text(encoding="utf-8"), "Prerequisites")
    step5 = re.search(r"^5\. \*\*Power settings\*\*(.*?)(?=^6\. )", pre, re.M | re.S)
    assert step5
    text = " ".join(step5.group(1).split())
    assert "`powercfg /hibernate off`" in text and "§4.3" in text
    assert "Fast Startup" in text and "`powercfg /a`" in text
    assert "scripts never change Windows settings" in text                      # still set by hand
