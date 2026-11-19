"""Single-PC autostart assets (operations.md s17; docs/runbooks/single-pc-autostart.md).

The live stack (deploy/local/docker-compose.local.yml) persists PostgreSQL and etcd, restarts on its own, binds to
loopback only and never shares a port, container or volume with the throwaway test stack; the live env example
holds no credential and keeps the read-only, measured-clock settings; the PowerShell scripts start the right
things in the right order, register the scheduled task as specified and never write or print a credential.
Text checks only: nothing here runs Docker, PowerShell or Task Scheduler.
"""
from __future__ import annotations

import contextlib
import io
import re
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from src.main import EX_CONFIG
from src.runtime.health import CHRONY_HARD_MS
from tests.deploy.test_deploy_assets import _assignments, env_vars_read_by_code, read_yaml
from tests.deploy.test_redis_md_compose import _command, _service

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
LOCAL = DEPLOY / "local"
COMPOSE = LOCAL / "docker-compose.local.yml"
DEV_COMPOSE = DEPLOY / "docker-compose.dev.yml"
LIVE_ENV = DEPLOY / "env" / "local-live.env.example"
DEV_ENV = DEPLOY / "env" / "local.env.example"
SERVICES = ("postgres", "redis", "redis-api", "etcd", "redis-md")
CREDENTIALS = ("STONE_X_USERNAME", "STONE_X_PASS", "STONE_X_APP_KEY")
SCRIPTS = ("start-xtnl.ps1", "install-autostart.ps1", "uninstall-autostart.ps1", "status-xtnl.ps1",
           "stop-xtnl.ps1", "send-ctrl-c.ps1")
TASK = "'XTNL engine'"
# Design C2 Secrets (plan S36): never in the env file, only in the Windows User environment on the PC.
MD_ARM_SECRETS = {"XTNL_MD_REDIS_URL", "XTNL_MD_REDIS_READER_URL", "XTNL_MD_HMAC_KEY", "XTNL_MD_HMAC_KEY_PREV",
                  "XTNL_ARM_CONFIG_DSN", "XTNL_ARM_SMTP_USERNAME", "XTNL_ARM_SMTP_PASSWORD",
                  "XTNL_ARM_SES_ACCESS_KEY_ID", "XTNL_ARM_SES_SECRET_ACCESS_KEY", "XTNL_ARM_SES_SESSION_TOKEN",
                  "XTNL_ARM_PUSH_TOKEN", "XTNL_ARM_PUSH_USER"}
OVERSEE_SECRETS = {"XTNL_SMTP_USERNAME", "XTNL_SMTP_PASSWORD", "XTNL_SES_ACCESS_KEY_ID", "XTNL_SES_SECRET_ACCESS_KEY",
                   "XTNL_SES_SESSION_TOKEN", "XTNL_PUSH_TOKEN", "XTNL_PUSH_USER", "XTNL_DEADMAN_TOKEN"}


def _compose(path: Path = COMPOSE) -> dict[str, str]:
    return read_yaml(path)


def _list(cfg: dict[str, str], prefix: str) -> list[str]:
    out: list[str] = []
    while f"{prefix}[{len(out)}]" in cfg:
        out.append(cfg[f"{prefix}[{len(out)}]"])
    return out


def _host_ports(cfg: dict[str, str]) -> dict[str, tuple[str, int]]:
    out: dict[str, tuple[str, int]] = {}
    for svc in SERVICES:
        if f"services.{svc}.image" not in cfg:
            continue                     # redis-api exists in the live stack only (the dev stack has no API)
        ports = _list(cfg, f"services.{svc}.ports")
        assert len(ports) == 1, svc
        host, port, _container = ports[0].split(":")
        out[svc] = (host, int(port))
    return out


def _script(name: str) -> str:
    return (LOCAL / name).read_text(encoding="utf-8")


def _code_lines(text: str) -> list[str]:
    """PowerShell lines outside comment-based help and line comments."""
    text = re.sub(r"<#.*?#>", "", text, flags=re.S)
    return [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]


# ---------------------------------------------------------------------------------------------- compose
def test_compose_is_a_separate_project_with_the_pinned_images() -> None:
    cfg = _compose()
    assert cfg["name"] == "xtnl-local"
    assert _compose(DEV_COMPOSE)["name"] == "xtnl-dev"
    assert cfg["services.postgres.image"] == "postgres:17"
    assert cfg["services.redis.image"] == "redis:7.4"
    assert cfg["services.etcd.image"] == "gcr.io/etcd-development/etcd:v3.5.17"


def test_compose_restarts_and_healthchecks_every_service() -> None:
    cfg = _compose()
    for svc in SERVICES:
        assert cfg[f"services.{svc}.restart"] == "unless-stopped", svc
        assert cfg[f"services.{svc}.healthcheck.test"].startswith("["), svc


def test_compose_ports_are_loopback_only_and_differ_from_the_test_stack() -> None:
    live, dev = _host_ports(_compose()), _host_ports(_compose(DEV_COMPOSE))
    assert {svc: p for svc, (_, p) in live.items()} == {"postgres": 55433, "redis": 56380, "redis-api": 56381,
                                                        "etcd": 52380, "redis-md": 56382}
    assert all(host == "127.0.0.1" for host, _ in live.values())
    assert not {p for _, p in live.values()} & {p for _, p in dev.values()}
    assert "56382" not in DEV_COMPOSE.read_text(encoding="utf-8")       # the PC redis-md port is not the dev one


def test_compose_persists_postgres_and_etcd_on_named_volumes() -> None:
    cfg = _compose()
    text = COMPOSE.read_text(encoding="utf-8")
    assert not re.search(r"^\s*tmpfs\s*:", text, re.M), "the live stack never keeps data on tmpfs"
    assert _list(cfg, "services.postgres.volumes") == ["xtnl_local_pg:/var/lib/postgresql/data"]
    assert _list(cfg, "services.etcd.volumes") == ["xtnl_local_etcd:/etcd-data"]
    assert "--data-dir=/etcd-data" in _list(cfg, "services.etcd.command")
    # explicit names: the volumes are exactly these, never project-prefixed, never shared with xtnl-dev
    assert cfg["volumes.xtnl_local_pg.name"] == "xtnl_local_pg"
    assert cfg["volumes.xtnl_local_etcd.name"] == "xtnl_local_etcd"
    assert cfg["volumes.xtnl_local_redis_md.name"] == "xtnl_local_redis_md"
    assert _list(cfg, "services.redis-md.volumes")[0] == "xtnl_local_redis_md:/data"
    assert "xtnl_local" not in DEV_COMPOSE.read_text(encoding="utf-8")
    assert "down -v" in text      # the header warns that -v deletes the history
    never = re.search(r"NEVER `down -v` here:.*?\n#\s*\n", text, re.S)
    assert never is not None
    for volume in ("xtnl_local_pg", "xtnl_local_etcd", "xtnl_local_redis_md"):
        assert volume in never.group(0), volume


def test_compose_postgres_small_box_tuning() -> None:
    cfg = _compose()
    cmd = _list(cfg, "services.postgres.command")
    for flag in ("shared_buffers=128MB", "max_connections=40", "idle_in_transaction_session_timeout=10s",
                 "timezone=UTC"):
        assert flag in cmd and cmd[cmd.index(flag) - 1] == "-c", flag
    assert cfg["services.postgres.environment.POSTGRES_INITDB_ARGS"] == "--data-checksums"
    assert cfg["services.postgres.environment.POSTGRES_DB"] == "xtnl"
    assert cfg["services.postgres.environment.POSTGRES_USER"] == "xtnl"
    text = COMPOSE.read_text(encoding="utf-8")
    password = cfg["services.postgres.environment.POSTGRES_PASSWORD"]
    assert "local_only" in password and re.search(r"local-only password", text, re.I)
    assert password not in DEV_COMPOSE.read_text(encoding="utf-8")


def test_compose_redis_is_memory_only_noeviction() -> None:
    cmd = _list(_compose(), "services.redis.command")
    assert cmd[cmd.index("--maxmemory") + 1] == "256mb"
    assert cmd[cmd.index("--maxmemory-policy") + 1] == "noeviction"
    assert cmd[cmd.index("--save") + 1] == "" and cmd[cmd.index("--appendonly") + 1] == "no"


def test_compose_etcd_single_member_advertises_its_host_port() -> None:
    cfg = _compose()
    cmd = _list(cfg, "services.etcd.command")
    assert "--advertise-client-urls=http://127.0.0.1:52380" in cmd
    assert "--heartbeat-interval=250" in cmd and "--election-timeout=2500" in cmd
    assert "--auto-compaction-mode=periodic" in cmd and "--auto-compaction-retention=1h" in cmd
    initial = next(c for c in cmd if c.startswith("--initial-cluster="))
    assert len(initial.split("=", 1)[1].split(",")) == 1


def test_compose_header_explains_the_difference_from_the_dev_stack() -> None:
    header = COMPOSE.read_text(encoding="utf-8").split("\nname:", 1)[0]
    assert "docker-compose.dev.yml" in header
    assert "tmpfs" in header and "tests" in header.lower()


# ---------------------------------------------------------------------------------------------- env
def test_live_env_example_holds_no_credentials() -> None:
    text, assigned = _assignments(LIVE_ENV)
    for name in CREDENTIALS:
        assert name not in assigned, f"{name} must come from the Windows User environment"
    assert not [k for k in assigned if re.search(r"(PASS|SECRET|TOKEN|APP_KEY)", k)]
    assert "Windows User environment" in text and "never" in text.lower()


def test_live_env_example_settings() -> None:
    _, a = _assignments(LIVE_ENV)
    assert a["BROKER_ENV"] == "live_readonly"
    assert a["XTNL_DEV_SKIP_CLOCK_CHECK"] == "0"
    assert a["XTNL_USE_MOCK_BROKER"] == "0"
    assert a["XTNL_NODE_ID"] == "pc1"
    assert a["XTNL_ETCD_ENDPOINTS"].split(",") == ["http://127.0.0.1:52380"] * 3
    assert a["XTNL_SPILL_PATH"] == "var/dlq_spill.jsonl"
    assert a["XTNL_LOG_FORMAT"] == "json"
    assert a["XTNL_METRICS_ADDR"] == "127.0.0.1:9464"
    assert urlsplit(a["XTNL_DEADMAN_URL"]).hostname == "127.0.0.1"
    assert a["XTNL_UPTIME_PING_S"] == "900"
    assert a["STONE_X_STOP_LIMIT_HISTORY"] == "1" and a["STONE_X_HISTORY_PAGING_CONFIRMED"] == "1"


def test_live_env_example_points_at_the_live_stack() -> None:
    _, a = _assignments(LIVE_ENV)
    cfg = _compose()
    ports = _host_ports(cfg)
    dsn = urlsplit(a["ORDER_SERVER_DSN"])
    assert (dsn.hostname, dsn.port) == ports["postgres"]
    assert dsn.username == cfg["services.postgres.environment.POSTGRES_USER"]
    assert dsn.password == cfg["services.postgres.environment.POSTGRES_PASSWORD"]
    assert dsn.path == "/" + cfg["services.postgres.environment.POSTGRES_DB"]
    redis = urlsplit(a["REDIS_URL"])
    assert (redis.hostname, redis.port, redis.path) == (*ports["redis"], "/0")
    etcd = urlsplit(a["XTNL_ETCD_ENDPOINTS"].split(",")[0])
    assert (etcd.hostname, etcd.port) == ports["etcd"]


def test_live_env_example_covers_the_dev_example_and_names_only_real_settings() -> None:
    _, live = _assignments(LIVE_ENV)
    _, dev = _assignments(DEV_ENV)
    assert set(dev) <= set(live), f"local-live.env.example lacks: {sorted(set(dev) - set(live))}"
    read = env_vars_read_by_code()
    unknown = sorted(k for k in live if k not in read)
    assert not unknown, f"not read by the engine (typo?): {unknown}"


def _forbidden_in_file() -> set[str]:
    """start-xtnl.ps1's $ForbiddenInFile: an env file assigning any of these is refused (exit 78)."""
    m = re.search(r"\$ForbiddenInFile = @\(([^)]*)\)", _script("start-xtnl.ps1"))
    assert m is not None
    return set(re.findall(r"'([A-Z_]+)'", m.group(1)))


def test_real_local_env_has_no_credentials_when_present() -> None:
    """Every name start-xtnl.ps1 refuses in the env file (not only STONE_X_*): one of them in local.env would stop
    the 24/7 engine at its next start (exit 78)."""
    path = DEPLOY / "env" / "local.env"
    if not path.exists():
        pytest.skip("deploy/env/local.env is git-ignored and only exists on the PC")
    _, assigned = _assignments(path)
    forbidden = _forbidden_in_file() | set(CREDENTIALS)
    found = sorted(name for name in forbidden if name in assigned)   # names only, never values
    assert not found, f"credentials must not be in local.env (start-xtnl.ps1 refuses them): {found}"


# ---------------------------------------------------------------------------------------------- scripts
@pytest.mark.parametrize("name", SCRIPTS)
def test_scripts_exist_and_say_phase1_never_sends_orders(name: str) -> None:
    text = _script(name)
    assert "never sends orders" in text, name
    assert text.isascii(), f"{name}: Windows PowerShell 5.1 reads BOM-less files as ANSI; keep them ASCII"


def test_start_script_runs_the_steps_in_order() -> None:
    text = _script("start-xtnl.ps1")
    code = "\n".join(_code_lines(text))
    main = code[code.index("function Invoke-Main"):code.index("function Invoke-Cleanup")]
    steps = ["System.Threading.Mutex", "Read-EnvFile $EnvFile", "GetEnvironmentVariable($n, 'User')",
             "Wait-Docker", "'compose'", "'src.db.migrate'", "Wait-Clock", "$RunLocal"]
    pos = [main.index(s) for s in steps]
    assert pos == sorted(pos), dict(zip(steps, pos))
    assert "deploy\\local\\docker-compose.local.yml" in code
    assert "deploy\\local\\run-local.ps1" in code and "'-EnvFile'" in code
    assert "deploy\\env\\local.env" in code
    assert re.search(r"'up', '-d', '--wait'", code)
    assert "@('info'" in code
    assert "Global\\XTNL-engine-start" in code and "start-xtnl.pid" in code


def test_start_script_waits_and_logs_as_specified() -> None:
    text = _script("start-xtnl.ps1")
    assert re.search(r"\[int\]\$DockerWaitMinutes = 10\b", text)
    assert re.search(r"\[int\]\$ClockWaitMinutes = 15\b", text)
    assert re.search(r"\[int\]\$LogKeepDays = 14\b", text)
    assert "var\\logs" in text and '"xtnl-{0}.log"' in text and "'xtnl-*.log'" in text
    assert "AddMinutes(1)" in text            # one Docker wait line per minute
    assert "w32tm /resync" in text and "not resyncing" in text


def test_start_script_clock_probe_is_the_engines_own_check() -> None:
    text = _script("start-xtnl.ps1")
    m = re.search(r"^\$ClockProbe = (.*?)\n\s*\n", text, re.M | re.S)
    assert m is not None
    probe = "".join(re.findall(r'"([^"]*)"', m.group(1)))
    assert "from src.runtime.probes import clock_reader" in probe and "CHRONY_HARD_MS" in probe

    class Reader:
        def __init__(self, ms: float | None) -> None:
            self.ms = ms
            self.signed_offset_s = None if ms is None else -ms / 1000.0

        async def offset_ms(self) -> float | None:
            return self.ms

    import src.runtime.probes as probes
    for ms, expected in ((5.0, 0), (CHRONY_HARD_MS, 0), (CHRONY_HARD_MS + 0.1, 1), (None, 1)):
        out = io.StringIO()
        with pytest.MonkeyPatch.context() as mp, contextlib.redirect_stdout(out):
            mp.setattr(probes, "clock_reader", lambda ms=ms: Reader(ms))
            with pytest.raises(SystemExit) as exc:
                exec(compile(probe, "<clock probe>", "exec"), {"__name__": "__main__"})
        assert exc.value.code == expected, (ms, out.getvalue())
        assert out.getvalue().startswith("offset_ms=")


def test_start_script_exit_codes_match_the_engine() -> None:
    text = _script("start-xtnl.ps1")
    assert f"$script:ExitCode = {EX_CONFIG}" in text
    for code in (10, 11, 12, 13):
        assert f"$script:ExitCode = {code}" in text
    # Ctrl+C / a stop request is a clean stop (0): the task's restart-on-failure must not restart it
    assert "if ($null -eq $script:ExitCode)" in text and "[Environment]::Exit([int]$script:ExitCode)" in text


def test_start_script_retries_its_own_failures() -> None:
    # Task Scheduler's restart-on-failure covers a failed launch, not a non-zero exit: the script retries steps 4-8
    # itself and ends only on a clean stop (0), a configuration problem (EX_CONFIG) or -NoRetry.
    text = _script("start-xtnl.ps1")
    code = "\n".join(_code_lines(text))
    assert re.search(r"\[int\]\$RetryFirstS = 60\b", text) and re.search(r"\[int\]\$RetryMaxS = 900\b", text)
    assert "[switch]$NoRetry" in text
    main = code[code.index("function Invoke-Main"):code.index("function Invoke-Attempt")]
    attempt = code[code.index("function Invoke-Attempt"):code.index("function Invoke-Cleanup")]
    assert "while ($true)" in main and "Invoke-Attempt" in main
    assert f"if ($NoRetry -or $code -eq 0 -or $code -eq {EX_CONFIG}) {{ return }}" in main
    assert "$script:ExitCode = $null" in main          # Ctrl+C while waiting for the retry is a clean stop
    assert "[Math]::Min($delay * 2, $RetryMaxS)" in main
    for step in ("Wait-Docker", "'compose'", "'src.db.migrate'", "Wait-Clock", "$RunLocal"):
        assert step in attempt and step not in main, step
    runbook = (ROOT / "docs" / "runbooks" / "single-pc-autostart.md").read_text(encoding="utf-8")
    assert "attempt ended (exit N)" in runbook and "attempt ended (exit {0})" in text


def test_start_script_drains_on_ctrl_c_and_leaves_no_orphans() -> None:
    start, helper, stop = _script("start-xtnl.ps1"), _script("send-ctrl-c.ps1"), _script("stop-xtnl.ps1")
    assert "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE" in start and "0x2000" in start
    assert "SetConsoleCtrlHandler([IntPtr]::Zero, $false)" in start
    assert "ActiveProcesses()" in start          # waits for the draining engine before it exits
    # the engine drains on SIGINT (src/main.py); CTRL_C_EVENT (0) raises it, CTRL_BREAK would not
    assert "GenerateConsoleCtrlEvent(0, 0)" in helper
    assert "send-ctrl-c.ps1" in stop and "start-xtnl\\.ps1" in stop


def test_start_script_refuses_credentials_in_the_env_file() -> None:
    text = _script("start-xtnl.ps1")
    assert _forbidden_in_file() == (set(CREDENTIALS) | {"XTNL_API_SECRET", "XTNL_API_DSN"}
                                    | MD_ARM_SECRETS | OVERSEE_SECRETS)
    m = re.search(r"\$UserVarNames = @\(([^)]*)\)", text, re.S)
    assert m is not None
    names = set(re.findall(r"'([A-Z_]+)'", m.group(1)))
    assert set(CREDENTIALS) | {"BROKER_ENV", "STONE_X_BASE_URL", "STONE_X_STREAM_URL"} <= names
    # design C2: every market-data / ARM secret and Oversee channel credential is copied from the User environment
    assert MD_ARM_SECRETS | OVERSEE_SECRETS <= names


@pytest.mark.parametrize("name", SCRIPTS)
def test_no_script_writes_or_prints_a_credential(name: str) -> None:
    lines = _code_lines(_script(name))
    writes = re.compile(r"(Set-Content|Add-Content|Out-File|WriteAllText|AppendAllText|>>|\s>\s)")
    prints = re.compile(r"(Write-Log|Write-RawLog|Write-Host|Write-Output|Write-Warning|Console\]::)")
    for ln in lines:
        if writes.search(ln) or prints.search(ln):
            assert "STONE_X_" not in ln or "names a credential" in ln or "assigns" in ln, f"{name}: {ln.strip()}"
            assert not re.search(r"\$env:STONE_X_", ln), f"{name}: {ln.strip()}"
            assert not re.search(r"\$v\b", ln), f"{name}: logs a User environment value: {ln.strip()}"
        # environment changes stay in this process: never the persistent User / Machine environment
        if "SetEnvironmentVariable" in ln:
            assert "'Process'" in ln, f"{name}: {ln.strip()}"
        assert not re.search(r"\bsetx\b", ln, re.I), f"{name}: {ln.strip()}"


def test_install_registers_the_task_as_specified() -> None:
    text = _script("install-autostart.ps1")
    code = "\n".join(_code_lines(text))
    assert f"$TaskName = {TASK}" in code
    assert "New-ScheduledTaskTrigger -AtLogOn -User $User" in code and "$trigger.Delay = 'PT60S'" in code
    assert "-RestartInterval (New-TimeSpan -Minutes 1) -RestartCount 999" in code
    assert "-ExecutionTimeLimit ([TimeSpan]::Zero)" in code
    for flag in ("-AllowStartIfOnBatteries", "-DontStopIfGoingOnBatteries", "-StartWhenAvailable"):
        assert flag in code, flag
    assert "-LogonType Interactive" in code            # run only when logged on (Docker Desktop is per user)
    assert "-RunLevel Limited" in code
    assert "Register-ScheduledTask -TaskName $TaskName -Force" in code      # idempotent: replaces
    assert "'-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File \"{0}\"' -f $StartScript" in code
    assert "powershell.exe" in code and "deploy\\local\\start-xtnl.ps1" in code
    assert "Write-Host" in code                        # prints what it did
    # it changes no Windows / power / Docker setting itself
    for forbidden in ("powercfg", "Set-ItemProperty", "reg add", "Set-Service", "w32tm"):
        assert forbidden not in code, forbidden


def test_uninstall_and_status_scripts() -> None:
    un = "\n".join(_code_lines(_script("uninstall-autostart.ps1")))
    assert f"$TaskName = {TASK}" in un and "Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false" in un
    assert "stop-xtnl.ps1" in un
    assert "down -v" not in un.replace("never -v", "")
    st = "\n".join(_code_lines(_script("status-xtnl.ps1")))
    assert f"$TaskName = {TASK}" in st and "Get-ScheduledTaskInfo" in st
    assert "docker compose -f $ComposeFile --profile md ps" in st      # redis-md (profile md) is listed too
    assert "-m src.ops uptime show --limit 3" in st and "-m src.ops catchup show --limit 1" in st
    assert "deploy\\env\\local.env" in st and "-Tail $Lines" in st
    assert "if ($k -match $Hide) { continue }" in st   # never loads a credential-like setting


def test_runbook_covers_install_power_and_removal() -> None:
    text = re.sub(r"\s+", " ", (ROOT / "docs" / "runbooks" / "single-pc-autostart.md").read_text(encoding="utf-8"))
    for needle in ("Start Docker Desktop when you sign in", "Never", "Do nothing", "Active hours",
                   "install-autostart.ps1", "uninstall-autostart.ps1", "status-xtnl.ps1", "stop-xtnl.ps1",
                   "var\\logs\\xtnl-<yyyy-MM-dd>.log", "Reboot", "Sleep", "Windows update"):
        assert needle in text, needle


def test_runbook_carry_over_restores_with_the_dashboard_role() -> None:
    # Roles are cluster-wide and not in pg_dump; the dump GRANTs to xtnl_dashboard (migration 0011). The carry-over
    # must create the role first (from the migration itself) and restore with --exit-on-error, then migrate.
    text = (ROOT / "docs" / "runbooks" / "single-pc-autostart.md").read_text(encoding="utf-8")
    carry = text[text.index("## Carry over"):text.index("## Rollback")]
    role = carry.index("0011_dashboard_role.sql")
    restore = carry.index("pg_restore")
    assert role < restore
    assert "--exit-on-error" in carry and "--no-owner" in carry
    assert "DROP DATABASE IF EXISTS xtnl" in carry
    assert carry.index("src.db.migrate") > restore
    assert "--from-cluster-id" in carry and "--approver" in carry
    # The role step pipes the migration up to the end of its DO block (line 26 is "$$;").
    lines = (ROOT / "src" / "db" / "migrations" / "0011_dashboard_role.sql").read_text(encoding="utf-8").splitlines()
    assert "-TotalCount 26" in carry and lines[25].strip() == "$$;"


# ---------------------------------------------------------------------------------------------- redis-md (plan S36)
def _md_block() -> str:
    """The redis-md service block (the last service: it ends at the top-level `volumes:`)."""
    text = COMPOSE.read_text(encoding="utf-8")
    m = re.search(r"^  redis-md:\n(.*?)(?=^\S|^  \S)", text, re.M | re.S)
    assert m, "service redis-md missing"
    return m.group(1)


def test_redis_md_is_in_profile_md_only_and_the_plain_up_is_unchanged() -> None:
    text = COMPOSE.read_text(encoding="utf-8")
    block = _md_block()
    assert re.search(r'^    profiles: \["md"\]$', block, re.M), "redis-md must be in profile md only"
    for other in ("postgres", "redis", "redis-api", "etcd"):
        assert "profiles:" not in _service(text, other), f"{other} must stay in the plain up -d --wait"
    cfg = _compose()
    assert cfg["services.redis-md.image"] == "redis:7.4"
    assert cfg["services.redis-md.restart"] == "unless-stopped"
    assert cfg["services.redis-md.mem_limit"] == "2g"
    assert cfg["services.redis-md.ports[0]"] == "127.0.0.1:56382:6379"


def test_redis_md_persists_with_aof_and_never_evicts() -> None:
    cmd = _command(_md_block())
    joined = " ".join(cmd)
    for setting in ("--appendonly yes", "--appendfsync everysec", "--aof-load-truncated yes", "--maxmemory 1gb",
                    "--maxmemory-policy noeviction", "--aclfile /etc/redis-md/users.acl"):
        assert setting in joined, setting
    assert cmd[cmd.index("--save") + 1] == ""                 # AOF only, no RDB snapshots
    assert "--user" not in cmd                                # the users come from the ACL file only
    assert "dev_only" not in _md_block() and ">" not in joined   # no password in the compose file


def test_redis_md_acl_file_is_a_read_only_bind_outside_the_repository() -> None:
    block = _md_block()
    vol = block.split("    volumes:\n", 1)[1].split("\n    mem_limit:", 1)[0]
    assert "- xtnl_local_redis_md:/data" in vol
    bind = vol.split("- type: bind", 1)[1]
    assert re.search(r"^        source: \$\{LOCALAPPDATA\}/XTNL/redis-md/users\.acl$", bind, re.M)
    assert re.search(r"^        target: /etc/redis-md/users\.acl$", bind, re.M)
    assert re.search(r"^        read_only: true$", bind, re.M)
    assert re.search(r"^          create_host_path: false$", bind, re.M)   # never a directory in its place


def test_redis_md_healthcheck_needs_no_secret() -> None:
    test = _compose()["services.redis-md.healthcheck.test"]
    assert test.startswith("[") and '"md_health"' in test and '"ping"' in test
    assert "md_writer" not in test and "md_reader" not in test


def test_stop_and_status_pass_the_md_profile() -> None:
    stop = "\n".join(_code_lines(_script("stop-xtnl.ps1")))
    assert "& docker compose -f $ComposeFile --profile md stop" in stop
    st = "\n".join(_code_lines(_script("status-xtnl.ps1")))
    assert "--profile md ps redis-md" in st
    assert "& $Py -m src.ops md show" in st and "& $Py -m src.ops arm failed show" in st
    assert "& $Py -c $ClockProbe" in st and "CHRONY_HARD_MS" in st    # the Windows time-sync offset
    assert "'XTNL\\redis-md\\users.acl'" in st
    # md show gets the reader URL and the keys from the User environment for that call only, then they are removed
    m = re.search(r"\$MdReaderVars = @\(([^)]*)\)", st)
    assert m is not None
    assert set(re.findall(r"'([A-Z_]+)'", m.group(1))) == {"XTNL_MD_REDIS_READER_URL", "XTNL_MD_HMAC_KEY",
                                                           "XTNL_MD_HMAC_KEY_PREV"}
    md = st[st.index("$MdReaderVars) {"):st.index("Show-Title 'ARM")]
    assert md.index("& $Py -m src.ops md show") < md.index("SetEnvironmentVariable($n, $null, 'Process')")
    # md show runs only with the flag on, read as the engine reads it (the process value trimmed, exactly 1);
    # the env loader never loads a secret-like name
    assert "$mdRaw = ([string]$env:XTNL_MD_ENABLED).Trim()" in st
    assert "$mdOn = $opsReady -and ($mdRaw -eq '1')" in st and "if ($mdOn) {" in st
    hide = re.search(r"^\$Hide = '(.*)'$", st, re.M)
    assert hide is not None
    pattern = re.compile(hide.group(1).replace("(?i)", ""), re.I)
    for name in MD_ARM_SECRETS | OVERSEE_SECRETS:
        assert pattern.search(name), name
    for name in ("XTNL_MD_HMAC_KEY_ID", "XTNL_MD_HMAC_KEY_PREV_ID", "XTNL_MD_ENABLED", "ORDER_SERVER_DSN"):
        assert not pattern.search(name), name


@pytest.mark.parametrize("path", [LIVE_ENV, DEPLOY / "env" / "engine.env.example"], ids=lambda p: p.name)
def test_env_examples_keep_both_flags_off_and_no_md_or_arm_secret(path: Path) -> None:
    text, a = _assignments(path)
    assert a["XTNL_MD_ENABLED"] == "0" and a["XTNL_ARM_ENABLED"] == "0"
    assert a["XTNL_STONEX_PRICE_UNFILTERED"] == "0" and a["XTNL_ARM_STOP_READS"] == "0"
    assert a["XTNL_TICK_SYMBOLS"] == ""
    for name in MD_ARM_SECRETS:
        assert name not in a, name
        assert re.search(rf"\b{name}\b", text), f"{name} must be documented (as a comment)"


@pytest.mark.parametrize("path", [LIVE_ENV, DEPLOY / "env" / "engine.env.example"], ids=lambda p: p.name)
def test_env_example_md_and_arm_values_are_the_parsers_defaults(path: Path) -> None:
    from src.marketdata.config import MdSettings, parse_md_settings
    from src.riskmgr.config import ArmSettings, parse_arm_settings
    _, a = _assignments(path)
    md_vars = {k: v for k, v in a.items() if k.startswith("XTNL_MD_") and k != "XTNL_MD_ENABLED"}
    synthetic = {"XTNL_MD_REDIS_URL": "redis://md_writer:x@127.0.0.1:56382/0", "XTNL_MD_HMAC_KEY": "ab" * 32}
    md = parse_md_settings({**md_vars, **synthetic})
    assert isinstance(md, MdSettings), md
    defaults = MdSettings(redis_url=md.redis_url, keyring=md.keyring)
    assert md == defaults and md.keyring.current_id == 1
    arm_vars = {k: v for k, v in a.items() if k.startswith("XTNL_ARM_") and k != "XTNL_ARM_ENABLED"}
    arm = parse_arm_settings(arm_vars)
    assert arm == ArmSettings(), arm
