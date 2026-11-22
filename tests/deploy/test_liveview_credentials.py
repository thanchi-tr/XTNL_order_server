"""Live view credentials script (docs/design/live-view.md s5; docs/runbooks/live-view.md): it gives the read-only role
xtnl_liveview (migration 0021) a login and keeps the DSN in the Windows User environment only, never printed or
written to a file. Text checks only: nothing here runs PowerShell or Docker; the owner runs the script."""
from __future__ import annotations

import re
from pathlib import Path

from tests.deploy.test_single_pc_autostart import _code_lines

ROOT = Path(__file__).resolve().parents[2]
CRED = ROOT / "deploy" / "local" / "new-liveview-credentials.ps1"
RUNBOOK = ROOT / "docs" / "runbooks" / "live-view.md"


def _code() -> str:
    return "\n".join(_code_lines(CRED.read_text(encoding="utf-8")))


def test_liveview_credentials_script_shape() -> None:
    text = CRED.read_text(encoding="utf-8")
    assert text.isascii() and "never sends orders" in text
    code = _code()
    for needle in ("Set-Location -LiteralPath $Root", "RandomNumberGenerator", "New-Object byte[] 32",
                   "$_.ToString('x2')", "$DsnName = 'XTNL_LIVEVIEW_DSN'", "$PgHostPort = '127.0.0.1:55433'"):
        assert needle in code, needle
    # an existing DSN is kept unless -Rotate, which forces a new password
    assert "[switch]$Rotate" in code and "if ($haveDsn -and -not $Rotate)" in code
    # the role is checked before any password is made
    fn = code[code.index("function New-LiveViewCredential"):code.index("Write-Host 'XTNL live view credentials")]
    assert fn.index("rolname = 'xtnl_liveview'") < fn.index("RandomNumberGenerator")
    # the closing note: restart start-xtnl after -Rotate (the User environment is read once at start)
    tail = code[code.index("Write-Host 'XTNL live view credentials"):]
    assert "if ($stored -and $Rotate)" in tail and "stop-xtnl.ps1" in tail and "Start-ScheduledTask" in tail


def test_liveview_credentials_script_never_prints_or_files_the_secret() -> None:
    lines = _code_lines(CRED.read_text(encoding="utf-8"))
    writes = re.compile(r"(Set-Content|Add-Content|Out-File|WriteAllText|AppendAllText|>>|\s>\s|Set-Clipboard)")
    prints = re.compile(r"(Write-Host|Write-Output|Write-Warning|Write-Verbose|Write-Information|Console\]::)"
                        r"[^;{}]*\$(pw|dsn|bytes)\b")
    env_sets = [ln for ln in lines if "SetEnvironmentVariable" in ln]
    assert len(env_sets) == 1 and "[Environment]::SetEnvironmentVariable($DsnName, $dsn, 'User')" in env_sets[0]
    for ln in lines:
        assert not writes.search(ln), ln.strip()
        assert not prints.search(ln), ln.strip()
        assert not re.search(r"\bsetx\b", ln, re.I), ln.strip()
        assert not re.search(r"\$(pw|dsn)\s*\|", ln), ln.strip()               # never piped anywhere
    code = "\n".join(lines)
    assert "$pw = $null; $dsn = $null" in code


def test_liveview_credentials_script_sets_the_read_only_role() -> None:
    code = _code()
    fn = code[code.index("function New-LiveViewCredential"):code.index("Write-Host 'XTNL live view credentials")]
    assert "ALTER ROLE xtnl_liveview LOGIN PASSWORD" in fn
    assert "$psi.RedirectStandardInput = $true" in fn and "StandardInput.WriteLine" in fn   # never on a command line
    args = [ln for ln in fn.splitlines() if "$psi.Arguments" in ln]
    assert len(args) == 1 and "$pw" not in args[0] and "PASSWORD" not in args[0]
    assert "-tAc" in fn and "$pw" not in fn[:fn.index("RandomNumberGenerator")]
    assert "'postgresql://xtnl_liveview:' + $pw + '@' + $PgHostPort + '/xtnl'" in fn
    # only this role: no other role is ever given a login here
    assert set(re.findall(r"ALTER ROLE (\w+)", fn)) == {"xtnl_liveview"}
    assert "CREATE ROLE" not in code and "GRANT" not in code.upper().replace("GRANTED", "")


def test_liveview_credentials_script_is_not_a_supervisor_script() -> None:
    # SCRIPTS of test_single_pc_autostart.py allows only 'Process' environment writes; this one writes the User
    # environment, so it is checked here instead (docs/design/live-view.md s5).
    from tests.deploy.test_single_pc_autostart import SCRIPTS
    assert CRED.name not in SCRIPTS


def test_runbook_gives_the_owner_commands() -> None:
    flat = re.sub(r"\s+", " ", RUNBOOK.read_text(encoding="utf-8"))
    for needle in ("never sends orders", "XTNL_MIGRATIONS_ACCEPTED_UPTO=21", "deploy\\env\\local.env",
                   "new-liveview-credentials.ps1", "new-liveview-credentials.ps1 -Rotate", "stop-xtnl.ps1",
                   "Start-ScheduledTask -TaskName 'XTNL engine'", "http://127.0.0.1:8090/",
                   "http://127.0.0.1:8090/edge", "-NoLiveView", "Exit 78", "db: down", "28P01",
                   "[Environment]::SetEnvironmentVariable('XTNL_LIVEVIEW_DSN', $null, 'User')", "status-xtnl.ps1"):
        assert needle in flat, needle
