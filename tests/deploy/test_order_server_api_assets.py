"""Order-server API deployment assets (docs/runbooks/order-server-api.md): the credentials script keeps the secret
in the Windows User environment only, the supervisor runs and stops the API as specified, status checks it with a
signed pinned request, the env examples never assign the secret and agree with src/api, and the runbook gives the
exact commands the user runs. Text checks only: nothing here runs PowerShell, OpenSSL or Docker."""
from __future__ import annotations

import fnmatch
import re
from pathlib import Path

import pytest

from src.api import auth, pinescript, server, settings
from tests.deploy.test_deploy_assets import _assignments, read_yaml
from tests.deploy.test_single_pc_autostart import _code_lines

ROOT = Path(__file__).resolve().parents[2]
LOCAL = ROOT / "deploy" / "local"
RUNBOOK = ROOT / "docs" / "runbooks" / "order-server-api.md"
CRED = LOCAL / "new-api-credentials.ps1"
FIREWALL = ("New-NetFirewallRule -DisplayName 'XTNL order-server API' -Direction Inbound -Protocol TCP "
            "-LocalPort 47443 -Action Allow -Profile Private,Public")


def _code(path: Path) -> str:
    return "\n".join(_code_lines(path.read_text(encoding="utf-8")))


def test_credentials_script_shape() -> None:
    text = CRED.read_text(encoding="utf-8")
    assert text.isascii() and "never sends orders" in text
    code = _code(CRED)
    for needle in ("Git\\usr\\bin\\openssl.exe", "ec_paramgen_curve:P-256", "/CN=xtnl-order-server",
                   "subjectAltName=$san", "var\\api-tls", "RandomNumberGenerator", "New-Object byte[] 32",
                   "SetAccessRuleProtection($true, $false)", "-fingerprint', '-sha256'",
                   "ORDER_SERVER_URL", "ORDER_SERVER_TLS_FINGERPRINT", "ORDER_SERVER_API_SECRET"):
        assert needle in code, needle
    # existing credentials are kept unless -Rotate
    assert "[switch]$Rotate" in code and "if ($haveCert -and -not $Rotate)" in code
    assert "elseif ($haveSecret -and -not $Rotate)" in code
    assert code.count("Set-Clipboard -Value $value") == 1 and code.index("if ($CopySecret)") < code.index("Set-Clipboard -Value $value")


def test_credentials_script_uses_the_certificate_the_api_loads() -> None:
    # Run from any folder (an elevated window starts in C:\Windows\system32), and without -OutDir it reads
    # XTNL_API_TLS_CERT / XTNL_API_TLS_KEY from local.env, so it never mints a second certificate the API ignores.
    code = _code(CRED)
    assert "Set-Location -LiteralPath $Root" in code
    assert "[string]$EnvFile" in code and "deploy\\env\\local.env" in code
    assert "XTNL_API_TLS_CERT|XTNL_API_TLS_KEY" in code
    order = [code.index(s) for s in ("if ($OutDir) {", "$envTls.ContainsKey('XTNL_API_TLS_CERT')", "'var\\api-tls\\server.crt'")]
    assert order == sorted(order)
    assert code.index("$OutDir = Split-Path -Parent $Crt") < code.index("$haveCert = ")


def test_credentials_script_never_prints_or_files_the_secret() -> None:
    lines = _code_lines(CRED.read_text(encoding="utf-8"))
    writes = re.compile(r"(Set-Content|Add-Content|Out-File|WriteAllText|AppendAllText|>>|\s>\s)")
    prints = re.compile(r"(Write-Host|Write-Output|Write-Warning|Write-Verbose|Console\]::)[^;{}]*\$(text|value|bytes)\b")
    for ln in lines:
        assert not writes.search(ln), ln.strip()
        assert not prints.search(ln), ln.strip()
        if "SetEnvironmentVariable" in ln:
            assert "($SecretName, $text, 'User')" in ln or "($DsnName, $dsn, 'User')" in ln, ln.strip()
        assert not re.search(r"\bsetx\b", ln, re.I), ln.strip()


@pytest.mark.parametrize("path", sorted(LOCAL.glob("*.ps1")), ids=lambda p: p.name)
def test_no_script_changes_firewall_router_or_tasks_it_should_not(path: Path) -> None:
    code = _code(path)
    for forbidden in ("New-NetFirewallRule", "Set-NetFirewall", "netsh", "Remove-NetFirewallRule", "powercfg",
                      "Add-MpPreference", "New-NetNat", "upnp"):
        assert forbidden.lower() not in code.lower(), (path.name, forbidden)
    if path.name != "install-autostart.ps1":
        assert "Register-ScheduledTask" not in code, path.name


def test_supervisor_runs_the_api_as_a_second_child() -> None:
    code = _code(LOCAL / "start-xtnl.ps1")
    assert "@('-m', 'src.api', '--stop-on-stdin-eof') '[api] ' -Stdin" in code
    assert "$psi.RedirectStandardInput = [bool]$Stdin" in code
    assert re.search(r"\[int\]\$ApiRetryFirstS = 5\b", code) and re.search(r"\[int\]\$ApiRetryMaxS = 300\b", code)
    assert "[Math]::Min($a.Delay * 2, $ApiRetryMaxS)" in code
    assert "if ($code -eq 78)" in code and "$a.Enabled = $false" in code       # configuration refused: disabled
    attempt = code[code.index("function Invoke-Attempt"):code.index("function Invoke-Cleanup")]
    assert attempt.index("$RunLocal") < attempt.index("Step-Api") < attempt.index("Stop-Api")
    cleanup = code[code.index("function Invoke-Cleanup"):]
    assert "Stop-Api" in cleanup
    stop = code[code.index("function Stop-Api"):code.index("function ", code.index("function Stop-Api") + 1)]
    assert "StandardInput.Close()" in stop and "Stop-Tree" in stop
    main = code[code.index("function Invoke-Main"):code.index("function Invoke-Attempt")]
    assert "$loaded -contains 'XTNL_API_SECRET'" in main and "$script:Api.Enabled = $true" in main
    assert "order-server API disabled: XTNL_API_SECRET is not set" in main


def test_api_child_gets_no_broker_credentials_or_engine_storage() -> None:
    """Regression (least privilege): Start-Api used to hand the API child this script's whole environment, so the
    one network-facing process held the StoneX credentials, the engine's superuser DSN and its Redis."""
    code = _code(LOCAL / "start-xtnl.ps1")
    m = re.search(r"\$ApiDropEnv = @\(([^)]*)\)", code)
    assert m is not None
    drop = re.findall(r"'([^']+)'", m.group(1))
    assert set(drop) == {p + "*" for p in settings.FORBIDDEN_PREFIXES} | set(settings.FORBIDDEN_NAMES)
    assert "-Stdin -DropEnv $ApiDropEnv" in code                      # the API child
    assert "'' -DropEnv $EngineDropEnv" in code                       # the engine child
    m = re.search(r"\$EngineDropEnv = @\(([^)]*)\)", code)
    assert m is not None and set(re.findall(r"'([^']+)'", m.group(1))) == {"XTNL_API_SECRET", "XTNL_API_DSN"}
    pumped = code[code.index("function Start-Pumped"):code.index("function Wait-Pumped")]
    assert "[string[]]$DropEnv = @()" in pumped
    assert "$psi.EnvironmentVariables.Remove($name)" in pumped and "$name -like $pattern" in pumped
    assert pumped.index("EnvironmentVariables.Remove") < pumped.index("[System.Diagnostics.Process]::Start($psi)")
    main = code[code.index("function Invoke-Main"):code.index("function Invoke-Attempt")]
    assert "-not ($loaded -contains 'XTNL_API_DSN')" in main                   # no DSN: the API stays disabled


def test_credentials_script_sets_the_read_only_db_role() -> None:
    code = _code(CRED)
    assert "[switch]$DbRole" in code and "$DsnName = 'XTNL_API_DSN'" in code
    fn = code[code.index("function New-DbRoleCredential"):code.index("Write-Host 'XTNL order-server API credentials")]
    assert "rolname = 'xtnl_api'" in fn and "ALTER ROLE xtnl_api LOGIN PASSWORD" in fn
    assert "$psi.RedirectStandardInput = $true" in fn and "StandardInput.WriteLine" in fn   # never on a command line
    assert "$pw" not in fn.split("$psi.Arguments", 1)[1].split("\n", 1)[0]
    assert "postgresql://xtnl_api:" in fn and "127.0.0.1:55433" in code


def test_api_redis_is_its_own_instance() -> None:
    cfg = read_yaml(ROOT / "deploy" / "local" / "docker-compose.local.yml")
    assert cfg["services.redis-api.ports[0]"] == "127.0.0.1:56381:6379"
    for name in ("engine.env.example", "local-live.env.example"):
        _, a = _assignments(ROOT / "deploy" / "env" / name)
        assert a["XTNL_API_REDIS_URL"] == "redis://127.0.0.1:56381/2", name
        assert "XTNL_API_DSN" not in a, name


def test_status_and_stop_cover_the_api() -> None:
    st = _code(LOCAL / "status-xtnl.ps1")
    assert "Get-NetTCPConnection -State Listen -LocalPort $apiPort" in st
    assert "& $Py -m src.api.selfcheck" in st and "& $Py -m src.api.selfcheck --fingerprint" in st
    assert "[Environment]::GetEnvironmentVariable('XTNL_API_SECRET', 'User')" in st
    assert "[Environment]::SetEnvironmentVariable('XTNL_API_SECRET', $null, 'Process')" in st
    assert "order-server API" in (LOCAL / "stop-xtnl.ps1").read_text(encoding="utf-8")


@pytest.mark.parametrize("name", ["engine.env.example", "local-live.env.example"])
def test_env_examples_document_the_api_and_never_assign_its_secret(name: str) -> None:
    text, a = _assignments(ROOT / "deploy" / "env" / name)
    assert "XTNL_API_SECRET" in text and "XTNL_API_SECRET" not in a
    assert "Windows User" in text
    assert a["XTNL_API_PORT"] == str(settings.DEFAULT_PORT) == "47443"
    assert a["XTNL_API_BIND"] == settings.DEFAULT_BIND
    assert a["XTNL_API_REDIS_DB"] == str(settings.DEFAULT_REDIS_DB) == "2"
    assert a["XTNL_API_RATE_PER_MIN"] == str(settings.DEFAULT_RATE_PER_MIN) == "30"
    assert a["XTNL_API_TLS_CERT"] == settings.DEFAULT_CERT and a["XTNL_API_TLS_KEY"] == settings.DEFAULT_KEY


def test_local_redis_has_database_2() -> None:
    cmd = [v for k, v in read_yaml(ROOT / "deploy" / "local" / "docker-compose.local.yml").items()
           if k.startswith("services.redis.command[")]
    assert "--databases" not in cmd          # Redis default: 16 databases, so database 2 exists


def test_api_listens_on_all_addresses_but_nothing_else_does() -> None:
    compose = (ROOT / "deploy" / "local" / "docker-compose.local.yml").read_text(encoding="utf-8")
    assert not re.search(r'"0\.0\.0\.0:', compose)
    _, live = _assignments(ROOT / "deploy" / "env" / "local-live.env.example")
    assert live["XTNL_METRICS_ADDR"].startswith("127.0.0.1:")


def test_contract_constants() -> None:
    assert auth.CLOCK_WINDOW_MS == 60_000 and auth.NONCE_TTL_MS == 120_000
    assert server.Limits().max_body_bytes == 4096
    assert pinescript.CACHE_TTL_S == 8 * 86_400 and pinescript.WEEK_KEY == "pinescript:week:{week}"
    assert server.ROUTES == {("GET", "/v1/health"), ("POST", "/v1/pinescript/compile"),
                             ("GET", "/v1/pinescript/latest")}


def test_runbook_gives_the_exact_user_commands() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    flat = re.sub(r"\s+", " ", text)
    for needle in (FIREWALL, "Remove-NetFirewallRule -DisplayName 'XTNL order-server API'", "TCP", "47443",
                   "DHCP reservation", "https://api.ipify.org", "carrier-grade NAT", "100.64.0.0/10", "DDNS",
                   "ORDER_SERVER_URL", "ORDER_SERVER_API_SECRET", "ORDER_SERVER_TLS_FINGERPRINT", "NEXT_PUBLIC_",
                   "new-api-credentials.ps1 -Rotate", "-CopySecret", "[Environment]::SetEnvironmentVariable('XTNL_API_SECRET', $null, 'User')",
                   "redis-cli -n 2 FLUSHDB", "never FLUSHALL", "status-xtnl.ps1", "src.api.selfcheck",
                   "X-XTNL-Signature", "60 s", "120 s", "30 requests a minute", "OneDrive"):
        assert needle in flat, needle


# ---------------------------------------------------------------------------------------------- plan S36 (design C2)
MD_CRED = LOCAL / "new-md-credentials.ps1"


def _ps_list(code: str, var: str) -> list[str]:
    m = re.search(rf"\${var} = @\(([^)]*)\)", code)
    assert m is not None, var
    return re.findall(r"'([^']+)'", m.group(1))


def test_every_user_environment_name_but_the_apis_own_is_dropped_from_the_api_child() -> None:
    """Rule (R11-m, design C2): whatever start-xtnl.ps1 copies from the Windows User environment reaches the engine
    only; the network-facing API child gets none of it except its own XTNL_API_* settings."""
    code = _code(LOCAL / "start-xtnl.ps1")
    drop = _ps_list(code, "ApiDropEnv")
    names = _ps_list(code, "UserVarNames")
    assert "XTNL_DEADMAN_TOKEN" in names and "XTNL_MD_HMAC_KEY" in names
    for name in names:
        if name.startswith("XTNL_API_"):
            continue
        assert any(fnmatch.fnmatchcase(name, p) for p in drop), f"{name} would reach the API child"
    for prefix in ("XTNL_MD_*", "XTNL_ARM_*", "XTNL_SMTP_*", "XTNL_SES_*", "XTNL_PUSH_*"):
        assert prefix in drop, prefix
    assert "XTNL_DEADMAN_TOKEN" in drop
    # and the API itself refuses each of them (names only in the message)
    for name in names:
        if not name.startswith("XTNL_API_"):
            assert settings.forbidden_env_names({name: "x"}) == [name], name


def test_redis_md_step_runs_only_with_xtnl_md_enabled_and_is_never_fatal() -> None:
    code = _code(LOCAL / "start-xtnl.ps1")
    main = code[code.index("function Invoke-Main"):code.index("function Invoke-Attempt")]
    # the flag is read as the engine reads it (src/runtime/optional_settings.py): the process value after the env
    # file is applied (so a Windows User environment value counts too), trimmed, exactly 1
    read = "$mdRaw = ([string][Environment]::GetEnvironmentVariable('XTNL_MD_ENABLED', 'Process')).Trim()"
    assert read in main and "$script:MdEnabled = $mdRaw -eq $MdFlagOn" in main
    assert "$MdFlagOn = '1'" in code and "$MdFlagOff = @('', '0')" in code
    applied = "foreach ($k in $fileVars.Keys) { [Environment]::SetEnvironmentVariable($k, $fileVars[$k], 'Process') }"
    assert main.index("Read-EnvFile $EnvFile") < main.index(applied) < main.index(read)
    attempt = code[code.index("function Invoke-Attempt"):code.index("function Invoke-Cleanup")]
    guard = "if ($script:MdEnabled) { Start-RedisMd $docker } else { Stop-RedisMd $docker }"
    assert attempt.count("Start-RedisMd") == 1 and guard in attempt
    assert attempt.index("'up', '-d', '--wait'") < attempt.index(guard) < attempt.index("'src.db.migrate'")
    # the plain up never names the profile, so it starts and waits for postgres, redis, redis-api and etcd only
    plain = attempt[:attempt.index(guard)]
    assert "--profile" not in plain and "redis-md" not in plain
    fn = code[code.index("function Start-RedisMd"):code.index("function Stop-RedisMd")]
    assert "'--profile', 'md', 'up', '-d', 'redis-md'" in fn and "'--wait'" not in fn
    # a failure is logged, never fatal: $ErrorActionPreference is Stop, so the whole body sits in try / catch
    assert "$ErrorActionPreference = 'Stop'" in code
    body = fn[fn.index("{") + 1:fn.rindex("}")].strip()
    assert body.startswith("try {") and "} catch {" in body and "the engine starts without it" in body
    assert "ExitCode" not in fn and "throw" not in fn
    # with the flag off a redis-md left running (restart: unless-stopped) is stopped, also never fatal
    stop = code[code.index("function Stop-RedisMd"):code.index("function Invoke-Main")]
    body = stop[stop.index("{") + 1:stop.rindex("}")].strip()
    assert body.startswith("try {") and "} catch {" in body
    assert "'--profile', 'md', 'ps', '-q', 'redis-md'" in stop and "'--profile', 'md', 'stop', 'redis-md'" in stop
    assert "ExitCode" not in stop and "throw" not in stop and "'down'" not in stop and "'-v'" not in stop
    assert "Test-Path -LiteralPath $MdAclFile" in fn and "new-md-credentials.ps1" in fn
    assert r"$MdAclFile = Join-Path $env:LOCALAPPDATA 'XTNL\redis-md\users.acl'" in code
    assert "$script:MdEnabled = $false" in code                   # default: off


def test_md_credentials_script_shape() -> None:
    text = MD_CRED.read_text(encoding="utf-8")
    assert text.isascii() and "never sends orders" in text
    code = _code(MD_CRED)
    for needle in ("RandomNumberGenerator", "New-Object byte[] 32", "SHA256", r"$env:LOCALAPPDATA 'XTNL\redis-md'",
                   "'users.acl'", "$HostPort = '127.0.0.1:56382'", "'redis://md_writer:'", "'redis://md_reader:'",
                   "user default off", "user md_writer on #{0} {1}", "user md_reader on #{0} {1}",
                   "user md_health on {0}", "$env:OneDrive", "Set-Location -LiteralPath $Root"):
        assert needle in code, needle
    # existing credentials are kept unless asked; the key is rotated only on its own switch, never with -Rotate
    assert "[switch]$Rotate" in code and "[switch]$RotateKey" in code and "[switch]$RetirePrevKey" in code
    assert "-not $Rotate)" in code
    assert "elseif ($havePrev)" in code       # a second rotation is refused while the previous key is kept
    from tests.deploy.test_redis_md_compose import HEALTH_RULES, READER_RULES, WRITER_RULES
    rules = {v: "".join(re.findall(r"'([^']*)'", code.split(f"${v} = ", 1)[1].split("\n$", 1)[0]))
             for v in ("WriterRules", "ReaderRules", "HealthRules")}
    assert rules == {"WriterRules": WRITER_RULES, "ReaderRules": READER_RULES, "HealthRules": HEALTH_RULES}
    # the ACL file: UTF-8 without BOM, written to a temporary name then moved into place
    assert "New-Object System.Text.UTF8Encoding($false)" in code
    assert "[System.IO.File]::WriteAllText($tmp, $acl, $Utf8NoBom)" in code


def test_md_credentials_script_never_prints_or_files_a_secret() -> None:
    lines = _code_lines(MD_CRED.read_text(encoding="utf-8"))
    writes = re.compile(r"(Set-Content|Add-Content|Out-File|WriteAllText|AppendAllText|>>|\s>\s)")
    prints = re.compile(r"(Write-Host|Write-Output|Write-Warning|Write-Verbose|Console\]::)[^;{}]*"
                        r"\$(text|url|writerPw|readerPw|current|acl)\b")
    allowed_env = {"($WriterName, $url, 'User')", "($ReaderName, $url, 'User')", "($Name, $text, 'User')",
                   "($PrevKeyName, $current, 'User')", "($PrevKeyName, $null, 'User')"}
    for ln in lines:
        if writes.search(ln):
            # the one file written is the ACL file (hashes only), never a password, URL or key
            assert "WriteAllText($tmp, $acl, $Utf8NoBom)" in ln, ln.strip()
        assert not prints.search(ln), ln.strip()
        if "SetEnvironmentVariable" in ln:
            assert any(a in ln for a in allowed_env), ln.strip()
        assert not re.search(r"\bsetx\b", ln, re.I), ln.strip()
        assert "Set-Clipboard" not in ln, ln.strip()
    # the ACL text carries the SHA-256 of each password, never the password itself
    code = "\n".join(lines)
    acl = code[code.index("$acl = "):code.index("New-Item -ItemType Directory")]
    assert "(Get-Sha256Hex $writerPw)" in acl and "(Get-Sha256Hex $readerPw)" in acl
    assert re.search(r"\$(writerPw|readerPw)\b(?!\))", acl.replace("(Get-Sha256Hex $writerPw)", "")
                     .replace("(Get-Sha256Hex $readerPw)", "")) is None
    assert ">" not in acl                                       # Redis' plain-text password form is never used


def test_md_credentials_acl_file_matches_the_compose_bind() -> None:
    code = _code(MD_CRED)
    compose = (ROOT / "deploy" / "local" / "docker-compose.local.yml").read_text(encoding="utf-8")
    assert "source: ${LOCALAPPDATA}/XTNL/redis-md/users.acl" in compose
    assert r"$AclDir = Join-Path $env:LOCALAPPDATA 'XTNL\redis-md'" in code
    assert "$AclFile = Join-Path $AclDir 'users.acl'" in code
    assert "--aclfile" in compose and "/etc/redis-md/users.acl" in compose
