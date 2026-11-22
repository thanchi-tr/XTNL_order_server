"""Home-server plan W2 (docs/design/home-server-plan.md s3.1, s3.2, s6): start-xtnl.ps1 secret handling.

- XTNL_HEARTBEAT_COPY_URL is a capability URL (on the home server a Healthchecks.io ping URL: anyone holding it can
  keep check A green). It and XTNL_HEARTBEAT_COPY_TOKEN come only from the Windows User environment, are refused in
  the local env file, reach the engine only, and both Python children refuse them.
- Backup credentials (XTNL_BACKUP_*, RCLONE_*, B2_*) belong to the separate backup Windows account: refused in the
  env file, removed from all three children (engine, API, live view) and refused by both Python settings.
- Nothing a child needs is caught by the new names or prefixes.

Text checks of the script plus calls into src/api/settings.py and src/liveview/settings.py; nothing runs PowerShell.
Names only, never values, appear in any message (the values used here are dummies)."""
from __future__ import annotations

import fnmatch
import re
from pathlib import Path

import pytest

from src.api import settings as api
from src.liveview import settings as lv
from tests.deploy.test_deploy_assets import _assignments
from tests.deploy.test_single_pc_autostart import _code_lines

ROOT = Path(__file__).resolve().parents[2]
START = ROOT / "deploy" / "local" / "start-xtnl.ps1"
LOCAL_ENV_EXAMPLE = ROOT / "deploy" / "env" / "local-live.env.example"
AWS_ENV_EXAMPLE = ROOT / "deploy" / "env" / "engine.env.example"

HEARTBEAT = ("XTNL_HEARTBEAT_COPY_URL", "XTNL_HEARTBEAT_COPY_TOKEN")
BACKUP_PREFIXES = ("XTNL_BACKUP_", "RCLONE_", "B2_")
# Names the backup job could plausibly carry (rclone reads RCLONE_*, B2 tools B2_*), lower case included.
BACKUP_NAMES = ("XTNL_BACKUP_B2_KEY", "XTNL_BACKUP_STATE_DIR", "RCLONE_CONFIG", "RCLONE_CONFIG_PASS",
                "RCLONE_B2_KEY", "B2_APPLICATION_KEY", "B2_APPLICATION_KEY_ID", "b2_application_key",
                "rclone_config_pass")
DUMMY = "dummy-value-0123456789"
DROP_LISTS = ("ApiDropEnv", "EngineDropEnv", "LiveViewDropEnv")


def _code() -> str:
    return "\n".join(_code_lines(START.read_text(encoding="utf-8")))


def _ps_list(var: str) -> list[str]:
    m = re.search(rf"\${var} = @\(([^)]*)\)", _code())
    assert m is not None, var
    return re.findall(r"'([^']+)'", m.group(1))


def _like(name: str, pattern: str) -> bool:
    """PowerShell `-like` ignores case: compare both sides upper-cased."""
    return fnmatch.fnmatchcase(name.upper(), pattern.upper())


def _dropped(name: str, var: str) -> bool:
    return any(_like(name, p) for p in _ps_list(var))


# ---------------------------------------------------------------------------------------------- heartbeat copy
@pytest.mark.parametrize("name", HEARTBEAT)
def test_heartbeat_copy_names_come_from_the_user_environment_only_and_reach_the_engine_only(name: str) -> None:
    assert name in _ps_list("UserVarNames")              # the engine reads them (src/runtime/wiring.py)
    assert name in _ps_list("ForbiddenInFile")           # never in deploy/env/local.env (exit 78)
    assert name in _ps_list("ApiDropEnv")                # exact names, never an XTNL_HEARTBEAT_* wildcard
    assert name in _ps_list("LiveViewDropEnv")
    assert not _dropped(name, "EngineDropEnv")           # the engine keeps them
    assert name not in _ps_list("RequiredUserVars")      # optional: the copy is off when the URL is unset


@pytest.mark.parametrize("name", HEARTBEAT)
def test_both_python_children_refuse_the_heartbeat_copy_names(name: str) -> None:
    assert name in api.FORBIDDEN_NAMES and name in lv.REFUSED_NAMES
    assert api.forbidden_env_names({name: DUMMY}) == [name]
    assert lv.refused_names({name: DUMMY, lv.DSN_VAR: "postgresql://x@127.0.0.1/xtnl"}) == [name]
    env = {api.SECRET_VAR: "a" * 64, api.DSN_VAR: "postgresql://xtnl_api@127.0.0.1:55433/xtnl", name: DUMMY}
    with pytest.raises(api.ApiConfigError) as ae:
        api.ApiSettings.from_env(env)
    assert name in str(ae.value) and DUMMY not in str(ae.value)
    with pytest.raises(lv.LiveViewConfigError) as le:
        lv.LiveViewSettings.from_env({lv.DSN_VAR: "postgresql://x@127.0.0.1/xtnl", name: DUMMY})
    assert name in str(le.value) and DUMMY not in str(le.value)


def test_local_env_example_never_assigns_the_heartbeat_copy_names_but_the_aws_one_still_does() -> None:
    """The refusal applies to the PC's local env file only: the AWS engine.env keeps both (the dashboard heartbeat
    copy, deploy/env/engine.env.example), and nothing in start-xtnl.ps1 reads that file."""
    _, local = _assignments(LOCAL_ENV_EXAMPLE)
    _, aws = _assignments(AWS_ENV_EXAMPLE)
    for name in HEARTBEAT:
        assert name not in local, f"{name} in local-live.env.example would make start-xtnl.ps1 exit 78"
        assert name in aws, f"{name}: the AWS path still sets it in its env file"
    assert "engine.env" not in _code()


# ---------------------------------------------------------------------------------------------- backup credentials
def test_backup_prefixes_are_refused_in_the_env_file_and_dropped_from_every_child() -> None:
    assert tuple(_ps_list("ForbiddenPrefixesInFile")) == BACKUP_PREFIXES
    for var in DROP_LISTS:
        drop = _ps_list(var)
        for prefix in BACKUP_PREFIXES:
            assert prefix + "*" in drop, f"{prefix}* missing from ${var}"
    assert set(BACKUP_PREFIXES) <= set(api.FORBIDDEN_PREFIXES)
    assert set(BACKUP_PREFIXES) <= set(lv.REFUSED_PREFIXES)
    # explicit prefixes only: each ends in '_', a wildcard only at the very end, never a bare '*'
    for prefix in BACKUP_PREFIXES:
        assert prefix.endswith("_") and "*" not in prefix and prefix == prefix.upper()
    for var in DROP_LISTS:
        for p in _ps_list(var):
            assert "*" not in p[:-1] and p != "*", f"${var}: {p}"


@pytest.mark.parametrize("name", BACKUP_NAMES)
def test_every_backup_name_is_dropped_and_refused(name: str) -> None:
    for var in DROP_LISTS:
        assert _dropped(name, var), f"{name} would reach the child of ${var}"
    assert api.forbidden_env_names({name: DUMMY}) == [name]
    assert lv.refused_names({name: DUMMY}) == [name]
    # the env-file check: Test-BackupName upper-cases and compares ordinally against $ForbiddenPrefixesInFile
    assert name.upper().startswith(tuple(_ps_list("ForbiddenPrefixesInFile")))


def test_real_local_env_has_no_backup_names_when_present() -> None:
    """Like test_single_pc_autostart.py's credential check: one such name in local.env would stop the engine at its
    next start (exit 78). Names only, never values."""
    path = ROOT / "deploy" / "env" / "local.env"
    if not path.exists():
        pytest.skip("deploy/env/local.env is git-ignored and only exists on the PC")
    _, assigned = _assignments(path)
    found = sorted(k for k in assigned if k.upper().startswith(BACKUP_PREFIXES))
    assert not found, f"backup names must not be in local.env (start-xtnl.ps1 refuses them): {found}"


def test_start_script_refuses_backup_names_in_the_env_file_with_exit_78() -> None:
    code = _code()
    fn = code[code.index("function Test-BackupName"):code.index("function Find-Docker")]
    assert "$Name.ToUpperInvariant()" in fn and "foreach ($p in $ForbiddenPrefixesInFile)" in fn
    assert "StartsWith($p, [System.StringComparison]::Ordinal)" in fn
    main = code[code.index("function Invoke-Main"):code.index("function Get-ExitMeaning")]
    check = main.index("$badBackup = @($fileVars.Keys | Where-Object { Test-BackupName $_ })")
    # after the exact-name refusal, before anything from the file is put in the process environment
    assert main.index("$bad = @($ForbiddenInFile") < check < main.index(
        "foreach ($k in $fileVars.Keys) { [Environment]::SetEnvironmentVariable(")
    block = main[check:main.index("foreach ($k in $fileVars.Keys)")]
    assert "$script:ExitCode = 78; return" in block
    assert "separate backup Windows account" in block and "$fileVars[" not in block      # names, never values


def test_start_script_names_inherited_backup_names_without_values() -> None:
    main = _code()[_code().index("function Invoke-Main"):_code().index("function Get-ExitMeaning")]
    i = main.index("$inheritedBackup = @(Get-ChildItem Env:")
    assert main.index("Windows User environment loaded (names only)") < i < main.index("Invoke-Attempt")
    block = main[i:main.index("if ($missing.Count -gt 0)", i)]
    assert "ForEach-Object { $_.Name }" in block and "$_.Value" not in block
    assert "Write-Log (\"warning:" in block and "$script:ExitCode" not in block      # a warning, not a refusal


# ---------------------------------------------------------------------------------------------- nothing needed caught
NEW_PATTERNS = ("XTNL_HEARTBEAT_COPY_URL", "XTNL_HEARTBEAT_COPY_TOKEN") + tuple(p + "*" for p in BACKUP_PREFIXES)


def _new_patterns_in(var: str) -> list[str]:
    return [p for p in _ps_list(var) if p in NEW_PATTERNS]


def test_engine_keeps_everything_it_reads() -> None:
    names = _ps_list("UserVarNames")
    own_drop = {"XTNL_API_SECRET", "XTNL_API_DSN", "XTNL_LIVEVIEW_DSN"}
    _, local = _assignments(LOCAL_ENV_EXAMPLE)
    needed = (set(names) - own_drop) | set(local) | {"XTNL_SINGLE_PC", "XTNL_DEADMAN_URL", "ORDER_SERVER_DSN",
                                                     "REDIS_URL", "XTNL_MD_ENABLED", "XTNL_ARM_ENABLED",
                                                     "PYTHONUNBUFFERED", "PYTHONIOENCODING", "XTNL_ARCHIVE_DIR"}
    for name in sorted(needed):
        assert not _dropped(name, "EngineDropEnv"), f"{name} would be removed from the engine"
        assert not name.upper().startswith(BACKUP_PREFIXES), f"{name} in the env file would be refused"


def test_api_keeps_its_own_settings() -> None:
    own = ("XTNL_API_SECRET", "XTNL_API_DSN", "XTNL_API_REDIS_URL", "XTNL_API_PORT", "XTNL_API_BIND",
           "XTNL_API_TLS_CERT", "XTNL_API_TLS_KEY", "XTNL_API_REDIS_DB", "XTNL_API_RATE_PER_MIN",
           "XTNL_LIVEVIEW_ADDR", "PYTHONUNBUFFERED", "SystemRoot", "PATH")
    for name in own:
        assert not [p for p in _new_patterns_in("ApiDropEnv") if _like(name, p)], name
        assert not _dropped(name, "ApiDropEnv"), name
        assert api.forbidden_env_names({name: DUMMY}) == [], name
    env = {api.SECRET_VAR: "a" * 64, api.DSN_VAR: "postgresql://xtnl_api@127.0.0.1:55433/xtnl"}
    assert api.ApiSettings.from_env(env).dsn == env[api.DSN_VAR]


def test_live_view_keeps_its_own_settings() -> None:
    own = ("XTNL_LIVEVIEW_DSN", "XTNL_LIVEVIEW_ADDR", "XTNL_LIVEVIEW_POLL_MS", "XTNL_LIVEVIEW_HISTORY",
           "XTNL_LIVEVIEW_BACKFILL_H", "XTNL_LIVEVIEW_MAX_CLIENTS", "XTNL_LIVEVIEW_ENGINE_METRICS", "SystemRoot",
           "PATH")
    for name in own:
        assert not _dropped(name, "LiveViewDropEnv"), name
    env = {name: "1000" for name in own if name not in (lv.DSN_VAR, lv.ADDR_VAR, lv.METRICS_VAR)}
    env.update({lv.DSN_VAR: "postgresql://xtnl_liveview@127.0.0.1:55433/xtnl", lv.ADDR_VAR: "127.0.0.1:8090",
                lv.METRICS_VAR: "off", "XTNL_LIVEVIEW_HISTORY": "2000", "XTNL_LIVEVIEW_BACKFILL_H": "24",
                "XTNL_LIVEVIEW_MAX_CLIENTS": "4"})
    assert lv.refused_names(env) == []
    assert lv.LiveViewSettings.from_env(env).dsn == env[lv.DSN_VAR]


def test_header_describes_the_new_names() -> None:
    header = START.read_text(encoding="utf-8").split("#>", 1)[0]
    flat = re.sub(r"\s+", " ", header)
    for needle in ("XTNL_HEARTBEAT_COPY_URL", "XTNL_HEARTBEAT_COPY_TOKEN", "$ForbiddenPrefixesInFile",
                   "XTNL_BACKUP_*", "RCLONE_*", "B2_*", "capability URL", "backup Windows account"):
        assert needle in flat, needle
