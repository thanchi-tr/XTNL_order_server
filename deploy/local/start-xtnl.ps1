<#
.SYNOPSIS
  Start the XTNL engine on this PC unattended (single-PC mode). This is what the 'XTNL engine' scheduled task runs.

.DESCRIPTION
  Phase 1 never sends orders: the engine only reads the broker (BROKER_ENV=live_readonly, read-only credentials).
  Runbook: docs/runbooks/single-pc-autostart.md. Rules: .claude/rules/operations.md s17.

  Steps, each logged to var/logs/xtnl-<yyyy-MM-dd>.log (one file per day, files older than 14 days deleted):
    1. single-instance guard (named mutex Global\XTNL-engine-start + var/logs/start-xtnl.pid with this PID);
    2. loads deploy/env/local.env (refused if it assigns a credential of $ForbiddenInFile: STONE_X_USERNAME /
       STONE_X_PASS / STONE_X_APP_KEY, the API's, the alert channels', the dead-man token, the heartbeat copy URL /
       token or a market-data / ARM secret; or any name starting with a backup prefix of $ForbiddenPrefixesInFile:
       XTNL_BACKUP_, RCLONE_, B2_); warns (never refuses) while XTNL_NODE_ID is an example's name (pc1, node-a),
       because the dead-man receiver tells two engines apart by node and epoch (W19);
    3. copies STONE_X_*, BROKER_ENV, XTNL_API_SECRET, XTNL_API_DSN, XTNL_LIVEVIEW_DSN, the alert channel
       credentials, the dead-man token, the heartbeat copy URL / token and the market-data / ARM secrets
       ($UserVarNames) from the Windows User environment into this process (names logged, values never).
       XTNL_HEARTBEAT_COPY_URL is a capability URL (on the home server a Healthchecks.io ping URL: anyone holding
       it can keep the check green), so it is a secret like the dead-man token: User environment only, engine only.
       XTNL_HEARTBEAT_COPY_TOKEN follows the same rule (on the home server leave it unset: it would be sent to
       the copy URL's host). Backup credentials (XTNL_BACKUP_*, RCLONE_*, B2_*) belong to the separate backup
       Windows account (docs/design/home-server-plan.md s3.2) and are never copied; when this process inherits
       any, the log names them (never values) and they are removed from every child (engine, API, live view);
    4. waits up to 10 min for the Docker engine (`docker info`), one log line per minute;
    5. `docker compose -f deploy/local/docker-compose.local.yml up -d --wait` (postgres, redis, redis-api, etcd);
       5b. only when XTNL_MD_ENABLED is 1 as the engine reads it (trimmed, from the env file or inherited from the
       Windows User environment; src/runtime/optional_settings.py): `--profile md up -d redis-md` (the market-data
       store; needs deploy/local/new-md-credentials.ps1 first). Never fatal and never waited for. With the flag
       off, a redis-md container still running from earlier (restart: unless-stopped) is stopped (non-fatal);
    6. applies the migrations (`python -m src.db.migrate`, idempotent) up to the accepted level
       XTNL_MIGRATIONS_ACCEPTED_UPTO (from the env file or the Windows User environment; default 14, plan S38f):
       later migrations are held back, named in one [migrate] line, and the start continues on the schema as it is;
    7. waits up to 15 min for the clock check the engine itself applies (|offset| <= 50 ms, measured with
       src.runtime.probes.clock_reader, i.e. w32tm on Windows), logging the offset. It never resyncs the clock
       (that needs admin: w32tm /resync from an elevated PowerShell). When the wait gives up it logs an ALERT line
       and starts the engine anyway: the engine applies the same check itself, so it records nothing until the
       clock passes and then becomes leader at once, without another 15-min retry cycle (-NoRetry: exit 13);
    8. runs deploy/local/run-local.ps1 -EnvFile deploy/env/local.env, which restarts the engine after a hard
       exit (codes 70-79). Its output and the engine's go into the same daily log.
       With it, when XTNL_API_SECRET is set in the Windows User environment (deploy/local/new-api-credentials.ps1)
       and -NoApi is not given, `python -m src.api --stop-on-stdin-eof` (the order-server API, the one
       network-facing listener: docs/runbooks/order-server-api.md) runs as a second supervised child in the same
       job object, its lines prefixed [api] in the same daily log. It is restarted after any exit after 5 s,
       doubling to 5 min (reset after a run of 10 min or more); exit 78 (its configuration refused) disables it
       until the next start. It is stopped (stdin closed, then ended after 15 s) when the engine run ends.
       Without XTNL_API_SECRET or XTNL_API_DSN the log says the API is disabled and nothing listens.
       Least privilege: the API child is started WITHOUT the broker credentials and the engine's storage
       ($ApiDropEnv: STONE_X_*, CTRADER_*, ORDER_SERVER_DSN, REDIS_URL, BROKER_ENV, XTNL_MD_*, XTNL_ARM_*,
       XTNL_SMTP_*, XTNL_SES_*, XTNL_PUSH_*, XTNL_DEADMAN_TOKEN, XTNL_HEARTBEAT_COPY_URL,
       XTNL_HEARTBEAT_COPY_TOKEN, XTNL_LIVEVIEW_DSN, XTNL_BACKUP_*, RCLONE_*, B2_* are removed from its
       environment; it uses XTNL_API_DSN = read-only role xtnl_api and XTNL_API_REDIS_URL = its own Redis, and
       refuses to start if any removed name is present), and the engine child is started without
       XTNL_API_SECRET / XTNL_API_DSN / XTNL_LIVEVIEW_DSN or any backup name ($EngineDropEnv).
       After the API, when XTNL_LIVEVIEW_DSN is set in the Windows User environment
       (deploy/local/new-liveview-credentials.ps1) and -NoLiveView is not given, `python -m src.liveview
       --stop-on-stdin-eof` (the local read-only live view on 127.0.0.1:8090, docs/runbooks/live-view.md; it never
       sends orders and has no write path) runs as a third supervised child, lines prefixed [liveview], with the
       same restart back-off (5 s doubling to 5 min) and the same exit 78 rule. It is started inside step 8 only,
       so it is down whenever docker, the migrations or the clock wait are retrying. Its environment lacks every
       name of $LiveViewDropEnv (the broker credentials, the engine's storage and DSNs, the API's, market-data /
       ARM and alert channel settings and secrets, the dead-man token, the heartbeat copy URL and token, the
       backup names XTNL_BACKUP_* / RCLONE_* / B2_*); it keeps XTNL_LIVEVIEW_DSN (read-only role xtnl_liveview,
       migration 0021).
       The engine is started with XTNL_SINGLE_PC=1 (unless the env file or the Windows User environment sets it):
       a SAFE_HOLD is resumed by the engine itself after a cool-down (operations.md s4 / s17; runbook
       single-pc-autostart.md "SAFE_HOLD on this PC").

  Stop: Ctrl+C in the window running this script, or deploy/local/stop-xtnl.ps1 (sends Ctrl+C to this script's
  hidden console). The engine drains on SIGINT and the API and the live view close their listeners; this script waits up to 60 s. Every child process is in a
  kill-on-close job object, so when the task is ended hard (Stop-ScheduledTask) nothing is left orphaned.

  Retries: Task Scheduler's restart-on-failure covers a task that fails to launch, not a script that exits
  non-zero, so this script retries steps 4-8 itself: after 10 / 11 / 12 / 13 or any engine exit other than 0 and
  78 it logs the code and starts again from step 4 after 60 s, doubling to 15 min (reset after a run of 10 min
  or more). Only a clean stop (0), a configuration problem (78) or Ctrl+C ends it. -NoRetry makes one attempt.
  While attempts keep failing, an 'ALERT: the engine has not run for N min' line is logged after
  -AlertAfterMinutes (30) min and again every hour; a configuration stop (78) logs an ALERT line too. On this PC the
  log is the only channel before the engine runs (once it runs, its critical single-PC pages also go straight to the
  configured alert channels); status-xtnl.ps1 lists the last ALERT and PAGE[critical] lines of every log.

  Exit codes: 0 clean stop (or another copy is already running); 10 Docker engine not up within the wait;
  11 compose up failed; 12 migrations failed; 13 clock not within 50 ms within the wait (10-13 only with
  -NoRetry); 78 configuration (missing env file / Windows User variables, credentials or backup names in the
  env file, BROKER_ENV mismatch, no .venv, or run-local.ps1's --check-config refused); otherwise (-NoRetry)
  run-local.ps1's (the engine's) exit code.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\start-xtnl.ps1
#>
param(
    [string]$EnvFile,
    [int]$DockerWaitMinutes = 10,
    [int]$ClockWaitMinutes = 15,
    [int]$LogKeepDays = 14,
    [int]$RetryFirstS = 60,
    [int]$RetryMaxS = 900,
    [int]$ApiRetryFirstS = 5,
    [int]$ApiRetryMaxS = 300,
    [int]$LiveViewRetryFirstS = 5,
    [int]$LiveViewRetryMaxS = 300,
    [int]$AlertAfterMinutes = 30,
    [switch]$NoRetry,
    [switch]$NoApi,
    [switch]$NoLiveView
)
$ErrorActionPreference = 'Stop'

$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Set-Location $Root
if (-not $EnvFile) { $EnvFile = Join-Path $Root 'deploy\env\local.env' }
$ComposeFile = Join-Path $Root 'deploy\local\docker-compose.local.yml'
$RunLocal = Join-Path $Root 'deploy\local\run-local.ps1'
$Py = Join-Path $Root '.venv\Scripts\python.exe'
$PsExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$LogDir = Join-Path $Root 'var\logs'
$PidFile = Join-Path $LogDir 'start-xtnl.pid'
$MutexName = 'Global\XTNL-engine-start'
# Copied from the Windows User environment into this process (and so into the engine). Values are never logged.
# Besides the broker and the API: the Oversee channel credentials and the dead-man token, and the market-data /
# ARM secrets of design C2 (deploy/local/new-md-credentials.ps1 sets the redis-md URLs and the content-hash key).
$UserVarNames = @('STONE_X_USERNAME', 'STONE_X_PASS', 'STONE_X_APP_KEY', 'STONE_X_BASE_URL',
                  'STONE_X_BASE_URL_DEPRECATED', 'STONE_X_STREAM_URL', 'BROKER_ENV', 'XTNL_API_SECRET',
                  'XTNL_API_DSN',
                  'XTNL_SMTP_USERNAME', 'XTNL_SMTP_PASSWORD', 'XTNL_SES_ACCESS_KEY_ID', 'XTNL_SES_SECRET_ACCESS_KEY',
                  'XTNL_SES_SESSION_TOKEN', 'XTNL_PUSH_TOKEN', 'XTNL_PUSH_USER', 'XTNL_DEADMAN_TOKEN',
                  'XTNL_MD_REDIS_URL', 'XTNL_MD_REDIS_READER_URL', 'XTNL_MD_HMAC_KEY', 'XTNL_MD_HMAC_KEY_PREV',
                  'XTNL_ARM_CONFIG_DSN', 'XTNL_ARM_SMTP_USERNAME', 'XTNL_ARM_SMTP_PASSWORD',
                  'XTNL_ARM_SES_ACCESS_KEY_ID', 'XTNL_ARM_SES_SECRET_ACCESS_KEY', 'XTNL_ARM_SES_SESSION_TOKEN',
                  'XTNL_ARM_PUSH_TOKEN', 'XTNL_ARM_PUSH_USER', 'XTNL_LIVEVIEW_DSN',
                  'XTNL_HEARTBEAT_COPY_URL', 'XTNL_HEARTBEAT_COPY_TOKEN')
$RequiredUserVars = @('STONE_X_USERNAME', 'STONE_X_PASS', 'STONE_X_APP_KEY', 'BROKER_ENV')
# Credentials live only in the Windows User environment, never in a file. The heartbeat copy URL is one: on the
# home server it is a Healthchecks.io capability URL (holding it is enough to keep the check green). The AWS
# engine.env keeps both heartbeat copy names in its own env file: this refusal is for the local env file only.
$ForbiddenInFile = @('STONE_X_USERNAME', 'STONE_X_PASS', 'STONE_X_APP_KEY', 'XTNL_API_SECRET', 'XTNL_API_DSN',
                     'XTNL_SMTP_USERNAME', 'XTNL_SMTP_PASSWORD', 'XTNL_SES_ACCESS_KEY_ID', 'XTNL_SES_SECRET_ACCESS_KEY',
                     'XTNL_SES_SESSION_TOKEN', 'XTNL_PUSH_TOKEN', 'XTNL_PUSH_USER', 'XTNL_DEADMAN_TOKEN',
                     'XTNL_MD_REDIS_URL', 'XTNL_MD_REDIS_READER_URL', 'XTNL_MD_HMAC_KEY', 'XTNL_MD_HMAC_KEY_PREV',
                     'XTNL_ARM_CONFIG_DSN', 'XTNL_ARM_SMTP_USERNAME', 'XTNL_ARM_SMTP_PASSWORD',
                     'XTNL_ARM_SES_ACCESS_KEY_ID', 'XTNL_ARM_SES_SECRET_ACCESS_KEY', 'XTNL_ARM_SES_SESSION_TOKEN',
                     'XTNL_ARM_PUSH_TOKEN', 'XTNL_ARM_PUSH_USER', 'XTNL_LIVEVIEW_DSN',
                     'XTNL_HEARTBEAT_COPY_URL', 'XTNL_HEARTBEAT_COPY_TOKEN')
# Backup credentials (B2 key, rclone config, backup job settings) belong to the separate backup Windows account
# (docs/design/home-server-plan.md s3.2) and never to this one: an env file assigning a name with one of these
# prefixes is refused (exit 78), and every child drops them (the '<prefix>*' entries of the three drop lists
# below). Explicit prefixes only, each ending in '_', so none can match a needed name such as XTNL_LIVEVIEW_DSN.
$ForbiddenPrefixesInFile = @('XTNL_BACKUP_', 'RCLONE_', 'B2_')
# The backup names that are not secret (review round 7). One of them alone in this account's environment is
# still removed from every child and named in a warning, but needs no rotation and distrusts no check C run.
$NonSecretBackupNames = @('XTNL_BACKUP_B2_BUCKET', 'XTNL_BACKUP_AGE_RECIPIENT', 'XTNL_BACKUP_EXCLUDE_MARKS',
                          'XTNL_BACKUP_ARCHIVE_HOST')
# Removed from the order-server API child's environment (wildcards; the same list src/api/settings.py refuses):
# the network-facing process never holds the broker credentials, the engine's writable DSN or its Redis, the
# market-data / ARM settings and secrets, the alert channel credentials, the dead-man token, the heartbeat copy
# URL / token, the live view's DSN or a backup credential. Every $UserVarNames name other than XTNL_API_* is
# covered (tests/deploy/test_order_server_api_assets.py).
$ApiDropEnv = @('STONE_X_*', 'CTRADER_*', 'XTNL_MD_*', 'XTNL_ARM_*', 'XTNL_SMTP_*', 'XTNL_SES_*', 'XTNL_PUSH_*',
                'ORDER_SERVER_DSN', 'REDIS_URL', 'XTNL_DEADMAN_TOKEN', 'BROKER_ENV', 'XTNL_LIVEVIEW_DSN',
                'XTNL_HEARTBEAT_COPY_URL', 'XTNL_HEARTBEAT_COPY_TOKEN', 'XTNL_BACKUP_*', 'RCLONE_*', 'B2_*')
# Removed from the engine child's environment: the API's own credentials, the live view's DSN and the backup
# credentials (the engine reads none of them).
$EngineDropEnv = @('XTNL_API_SECRET', 'XTNL_API_DSN', 'XTNL_LIVEVIEW_DSN', 'XTNL_BACKUP_*', 'RCLONE_*', 'B2_*')
# Removed from the live view child's environment: only the explicit prefixes and names of the refuse-list of
# src/liveview/settings.py (docs/design/live-view.md s4.1). Never a wildcard such as XTNL_*DSN*: -like would also
# remove XTNL_LIVEVIEW_DSN and the child would exit 78 on every start. The generic
# XTNL_*(PASS|SECRET|TOKEN|DSN|KEY) rule lives only in settings.py. Every $UserVarNames name other than
# XTNL_LIVEVIEW_DSN is covered (tests/deploy/test_order_server_api_assets.py).
$LiveViewDropEnv = @('STONE_X_*', 'CTRADER_*', 'XTNL_MD_*', 'XTNL_ARM_*', 'XTNL_SMTP_*', 'XTNL_SES_*', 'XTNL_PUSH_*',
                     'XTNL_API_*', 'ORDER_SERVER_DSN', 'REDIS_URL', 'BROKER_ENV', 'XTNL_DEADMAN_TOKEN',
                     'XTNL_HEARTBEAT_COPY_TOKEN', 'XTNL_DASHBOARD_DSN', 'XTNL_HEARTBEAT_COPY_URL',
                     'XTNL_BACKUP_*', 'RCLONE_*', 'B2_*')
# Node names of the env examples (local-live.env.example, engine.env.example): still set on a box means it shares
# its name with every other box started from the example (warning only; W19, Invoke-Main step 2).
$ExampleNodeIds = @('pc1', 'node-a')
# redis-md's ACL file (deploy/local/new-md-credentials.ps1), bound read-only into the container; outside the
# repository and OneDrive.
$MdAclFile = Join-Path $env:LOCALAPPDATA 'XTNL\redis-md\users.acl'
# The engine's own hard clock check (src/runtime/health.py CHRONY_HARD_MS), read with its own clock reader.
$ClockProbe = "import asyncio, sys; from src.runtime.probes import clock_reader; " +
              "from src.runtime.health import CHRONY_HARD_MS; r = clock_reader(); a = asyncio.run(r.offset_ms()); " +
              "s = r.signed_offset_s; print('offset_ms=%s limit_ms=%.0f' % ('unreadable' if s is None else " +
              "'%+.1f' % (s * 1000.0), CHRONY_HARD_MS)); sys.exit(0 if a is not None and a <= CHRONY_HARD_MS else 1)"

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$script:LogDay = ''
$script:ExitCode = $null
$script:Engine = $null
# Order-server API supervision state (Step-Api); Enabled only when XTNL_API_SECRET is set.
$script:Api = @{ Enabled = $false; St = $null; Delay = $ApiRetryFirstS; NextStart = (Get-Date); StartedAt = (Get-Date) }
# Live view supervision state (Step-LiveView); Enabled only when XTNL_LIVEVIEW_DSN is set and -NoLiveView is not given.
$script:LiveView = @{ Enabled = $false; St = $null; Delay = $LiveViewRetryFirstS; NextStart = (Get-Date); StartedAt = (Get-Date) }
# redis-md (compose profile md) is started only when XTNL_MD_ENABLED is on as the engine reads it
# (src/runtime/optional_settings.py optional_flags_from_env: the process value trimmed, exactly 1 is on; unset or 0
# is off; anything else is off and paged; Start-RedisMd). With the flag off a running redis-md is stopped (Stop-RedisMd).
$script:MdEnabled = $false
$MdFlagOn = '1'
$MdFlagOff = @('', '0')
$script:Mutex = $null
$script:MutexOwned = $false
$script:NativeOk = $false

# ------------------------------------------------------------------------------------------------ logging
function Remove-OldLogs {
    $cutoff = (Get-Date).AddDays(-$LogKeepDays)
    Get-ChildItem -Path $LogDir -Filter 'xtnl-*.log' -File -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $cutoff } |
        ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
}

function Write-RawLog([string]$Line) {
    $day = (Get-Date).ToString('yyyy-MM-dd')
    if ($day -ne $script:LogDay) {
        $script:LogDay = $day
        Remove-OldLogs
    }
    $path = Join-Path $LogDir ("xtnl-{0}.log" -f $day)
    for ($i = 0; $i -lt 3; $i++) {
        try { [System.IO.File]::AppendAllText($path, $Line + "`r`n", $Utf8NoBom); break }
        catch { Start-Sleep -Milliseconds 50 }
    }
    try { [Console]::Out.WriteLine($Line) } catch { }
}

function Write-Log([string]$Message) {
    Write-RawLog ('{0} [start-xtnl] {1}' -f (Get-Date).ToString('yyyy-MM-dd"T"HH:mm:ss.fffzzz'), $Message)
}

# ------------------------------------------------------------------------------------------------ child processes
function Format-Arg([string]$Value) {
    if ($Value -eq '') { return '""' }
    if ($Value -notmatch '[\s"]') { return $Value }
    $s = $Value -replace '(\\*)"', '$1$1\"'
    $s = $s -replace '(\\+)$', '$1$1'
    return '"' + $s + '"'
}

# Starts a child on this console (so Ctrl+C reaches it) with stdout / stderr piped back to this script.
# -Stdin: stdin is a pipe this script holds; closing it asks the child to stop (python -m src.api --stop-on-stdin-eof).
# -DropEnv: variable names (wildcards allowed, case-insensitive) left out of the child's environment.
function Start-Pumped([string]$Exe, [string[]]$Arguments, [string]$Prefix, [switch]$Quiet, [switch]$Stdin,
                      [string[]]$DropEnv = @()) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $Exe
    $psi.Arguments = (($Arguments | ForEach-Object { Format-Arg $_ }) -join ' ')
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.RedirectStandardInput = [bool]$Stdin
    $psi.StandardOutputEncoding = $Utf8NoBom
    $psi.StandardErrorEncoding = $Utf8NoBom
    $psi.WorkingDirectory = $Root
    if ($DropEnv.Count -gt 0) {
        foreach ($name in @($psi.EnvironmentVariables.Keys)) {
            foreach ($pattern in $DropEnv) {
                if ($name -like $pattern) { $psi.EnvironmentVariables.Remove($name); break }
            }
        }
    }
    $proc = [System.Diagnostics.Process]::Start($psi)
    $st = @{ Proc = $proc; Out = $proc.StandardOutput; Err = $proc.StandardError; Prefix = $Prefix
             Quiet = [bool]$Quiet; Lines = (New-Object System.Collections.Generic.List[string]) }
    $st.TOut = $st.Out.ReadLineAsync()
    $st.TErr = $st.Err.ReadLineAsync()
    return $st
}

# Copies the child's output into the log until both streams close (every holder of the pipes has exited) or
# the timeout passes. Returns $true when the streams closed, $false on timeout. Resumable after Ctrl+C.
function Wait-Pumped($St, [int]$TimeoutS = 0) {
    $deadline = if ($TimeoutS -gt 0) { (Get-Date).AddSeconds($TimeoutS) } else { [DateTime]::MaxValue }
    while (($null -ne $St.TOut) -or ($null -ne $St.TErr)) {
        $pending = New-Object System.Collections.Generic.List[System.Threading.Tasks.Task]
        if ($null -ne $St.TOut) { $pending.Add($St.TOut) }
        if ($null -ne $St.TErr) { $pending.Add($St.TErr) }
        [void][System.Threading.Tasks.Task]::WaitAny($pending.ToArray(), 1000)
        foreach ($k in @('TOut', 'TErr')) {
            $t = $St[$k]
            if ($null -eq $t -or -not $t.IsCompleted) { continue }
            $line = $null
            try { $line = $t.Result } catch { $line = $null }
            if ($null -eq $line) { $St[$k] = $null; continue }
            if ($St.Quiet) { $St.Lines.Add($line) } else { Write-RawLog ($St.Prefix + $line) }
            if ($k -eq 'TOut') { $St[$k] = $St.Out.ReadLineAsync() } else { $St[$k] = $St.Err.ReadLineAsync() }
        }
        if ((Get-Date) -gt $deadline) { return $false }
    }
    [void]$St.Proc.WaitForExit(10000)
    return $true
}

# Copies the output the child already has ready into the log, without waiting. $true once both streams closed.
function Step-Pumped($St) {
    foreach ($k in @('TOut', 'TErr')) {
        while ($null -ne $St[$k] -and $St[$k].IsCompleted) {
            $line = $null
            try { $line = $St[$k].Result } catch { $line = $null }
            if ($null -eq $line) { $St[$k] = $null; break }
            if ($St.Quiet) { $St.Lines.Add($line) } else { Write-RawLog ($St.Prefix + $line) }
            if ($k -eq 'TOut') { $St[$k] = $St.Out.ReadLineAsync() } else { $St[$k] = $St.Err.ReadLineAsync() }
        }
    }
    return (($null -eq $St.TOut) -and ($null -eq $St.TErr))
}

function Get-PumpedExit($St) {
    if ($St.Proc.HasExited) { return $St.Proc.ExitCode }
    return $null
}

function Stop-Tree($Proc) {
    try { if (-not $Proc.HasExited) { & taskkill.exe /PID $Proc.Id /T /F 2>$null | Out-Null } } catch { }
}

# One pumped command with a timeout: returns its exit code, or -1 on timeout (the process tree is ended).
function Invoke-Pumped([string]$Exe, [string[]]$Arguments, [string]$Prefix, [int]$TimeoutS) {
    $st = Start-Pumped $Exe $Arguments $Prefix
    if (-not (Wait-Pumped $st $TimeoutS)) {
        Stop-Tree $st.Proc
        return -1
    }
    $code = Get-PumpedExit $st
    if ($null -eq $code) { return -1 }
    return [int]$code
}

# ------------------------------------------------------------------------------------------------ order-server API
# python -m src.api: the second supervised child (docs/runbooks/order-server-api.md). Restarted with back-off;
# exit 78 (configuration refused) disables it until the next start.
function Start-Api {
    Write-Log 'starting the order-server API: python -m src.api --stop-on-stdin-eof'
    $script:Api.St = Start-Pumped $Py @('-m', 'src.api', '--stop-on-stdin-eof') '[api] ' -Stdin -DropEnv $ApiDropEnv
    $script:Api.StartedAt = Get-Date
}

function Step-Api {
    $a = $script:Api
    if (-not $a.Enabled) { return }
    if ($null -ne $a.St) {
        if (-not (Step-Pumped $a.St)) { return }
        [void]$a.St.Proc.WaitForExit(5000)
        $code = Get-PumpedExit $a.St
        $a.St = $null
        if ($code -eq 78) {
            Write-Log 'order-server API refused its configuration (exit 78; see the [api] lines); disabled until the next start'
            $a.Enabled = $false
            return
        }
        if (((Get-Date) - $a.StartedAt).TotalMinutes -ge 10) { $a.Delay = $ApiRetryFirstS }
        Write-Log ("order-server API exited (exit {0}); restarting it in {1} s" -f $code, $a.Delay)
        $a.NextStart = (Get-Date).AddSeconds($a.Delay)
        $a.Delay = [Math]::Min($a.Delay * 2, $ApiRetryMaxS)
        return
    }
    if ((Get-Date) -ge $a.NextStart) { Start-Api }
}

# Graceful stop: close its stdin (it closes the listener and exits), end it after -TimeoutS.
function Stop-Api([int]$TimeoutS = 15) {
    $a = $script:Api
    if ($null -eq $a.St) { return }
    try { $a.St.Proc.StandardInput.Close() } catch { }
    if (-not (Wait-Pumped $a.St $TimeoutS)) {
        Write-Log "order-server API still running after $TimeoutS s; ending it"
        Stop-Tree $a.St.Proc
    }
    Write-Log 'order-server API stopped'
    $a.St = $null
}

# ------------------------------------------------------------------------------------------------ live view
# python -m src.liveview: the third supervised child (docs/runbooks/live-view.md), local read-only page on
# 127.0.0.1:8090; it never sends orders. Restarted with back-off; exit 78 (configuration or login refused) disables
# it until the next start.
function Start-LiveView {
    Write-Log 'starting the live view: python -m src.liveview --stop-on-stdin-eof'
    $script:LiveView.St = Start-Pumped $Py @('-m', 'src.liveview', '--stop-on-stdin-eof') '[liveview] ' -Stdin -DropEnv $LiveViewDropEnv
    $script:LiveView.StartedAt = Get-Date
}

function Step-LiveView {
    $l = $script:LiveView
    if (-not $l.Enabled) { return }
    if ($null -ne $l.St) {
        if (-not (Step-Pumped $l.St)) { return }
        [void]$l.St.Proc.WaitForExit(5000)
        $code = Get-PumpedExit $l.St
        $l.St = $null
        if ($code -eq 78) {
            Write-Log 'live view refused its configuration or its login (exit 78; see the [liveview] lines); disabled until the next start'
            $l.Enabled = $false
            return
        }
        if (((Get-Date) - $l.StartedAt).TotalMinutes -ge 10) { $l.Delay = $LiveViewRetryFirstS }
        Write-Log ("live view exited (exit {0}); restarting it in {1} s" -f $code, $l.Delay)
        $l.NextStart = (Get-Date).AddSeconds($l.Delay)
        $l.Delay = [Math]::Min($l.Delay * 2, $LiveViewRetryMaxS)
        return
    }
    if ((Get-Date) -ge $l.NextStart) { Start-LiveView }
}

# Graceful stop: close its stdin (it closes its streams and the listener and exits), end it after -TimeoutS.
function Stop-LiveView([int]$TimeoutS = 15) {
    $l = $script:LiveView
    if ($null -eq $l.St) { return }
    try { $l.St.Proc.StandardInput.Close() } catch { }
    if (-not (Wait-Pumped $l.St $TimeoutS)) {
        Write-Log "live view still running after $TimeoutS s; ending it"
        Stop-Tree $l.St.Proc
    }
    Write-Log 'live view stopped'
    $l.St = $null
}

# ------------------------------------------------------------------------------------------------ native helpers
function Initialize-Native {
    if (-not ('XtnlStartNative' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class XtnlStartNative {
    [StructLayout(LayoutKind.Sequential)]
    struct BasicLimit {
        public long PerProcessUserTimeLimit; public long PerJobUserTimeLimit; public uint LimitFlags;
        public UIntPtr MinimumWorkingSetSize; public UIntPtr MaximumWorkingSetSize; public uint ActiveProcessLimit;
        public UIntPtr Affinity; public uint PriorityClass; public uint SchedulingClass;
    }
    [StructLayout(LayoutKind.Sequential)]
    struct IoCounters { public ulong R, W, O, RB, WB, OB; }
    [StructLayout(LayoutKind.Sequential)]
    struct ExtendedLimit {
        public BasicLimit Basic; public IoCounters Io; public UIntPtr ProcessMemoryLimit;
        public UIntPtr JobMemoryLimit; public UIntPtr PeakProcessMemoryUsed; public UIntPtr PeakJobMemoryUsed;
    }
    [StructLayout(LayoutKind.Sequential)]
    struct BasicAccounting {
        public long TotalUserTime; public long TotalKernelTime; public long ThisPeriodTotalUserTime;
        public long ThisPeriodTotalKernelTime; public uint TotalPageFaultCount; public uint TotalProcesses;
        public uint ActiveProcesses; public uint TotalTerminatedProcesses;
    }
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern IntPtr CreateJobObject(IntPtr attributes, string name);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool SetInformationJobObject(IntPtr job, int infoClass, ref ExtendedLimit info, uint length);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool QueryInformationJobObject(IntPtr job, int infoClass, out BasicAccounting info, uint length, IntPtr returned);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll")]
    static extern IntPtr GetCurrentProcess();
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool SetConsoleCtrlHandler(IntPtr handler, bool add);

    static IntPtr job = IntPtr.Zero;   // kept open for the life of this process

    // Puts this process (and so every child started later) in a job that is killed when this process ends.
    public static string KillChildrenWithThisProcess() {
        IntPtr j = CreateJobObject(IntPtr.Zero, null);
        if (j == IntPtr.Zero) return "CreateJobObject failed (" + Marshal.GetLastWin32Error() + ")";
        ExtendedLimit info = new ExtendedLimit();
        info.Basic.LimitFlags = 0x2000;   // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if (!SetInformationJobObject(j, 9, ref info, (uint)Marshal.SizeOf(typeof(ExtendedLimit))))
            return "SetInformationJobObject failed (" + Marshal.GetLastWin32Error() + ")";
        if (!AssignProcessToJobObject(j, GetCurrentProcess()))
            return "AssignProcessToJobObject failed (" + Marshal.GetLastWin32Error() + ")";
        job = j;
        return null;
    }

    // Processes alive in the job, this one included; -1 without a job.
    public static int ActiveProcesses() {
        if (job == IntPtr.Zero) return -1;
        BasicAccounting a;
        if (!QueryInformationJobObject(job, 1, out a, (uint)Marshal.SizeOf(typeof(BasicAccounting)), IntPtr.Zero))
            return -1;
        return (int)a.ActiveProcesses;
    }
}
'@
    }
    # A task may start this process with Ctrl+C ignored; children inherit that. Turn Ctrl+C handling back on so
    # stop-xtnl.ps1's Ctrl+C reaches the engine (SIGINT, graceful drain).
    [void][XtnlStartNative]::SetConsoleCtrlHandler([IntPtr]::Zero, $false)
    $err = [XtnlStartNative]::KillChildrenWithThisProcess()
    if ($err) { Write-Log "warning: no kill-on-close job ($err); children may outlive a hard task stop" }
    $script:NativeOk = $true
}

# ------------------------------------------------------------------------------------------------ steps
function Read-EnvFile([string]$Path) {
    $vars = [ordered]@{}
    foreach ($line in Get-Content -LiteralPath $Path) {
        $t = $line.Trim()
        if ($t -eq '' -or $t.StartsWith('#')) { continue }
        $i = $t.IndexOf('=')
        if ($i -lt 1) { continue }
        $vars[$t.Substring(0, $i).Trim()] = $t.Substring($i + 1).Trim()
    }
    return $vars
}

# True for a name starting with one of $ForbiddenPrefixesInFile (case-insensitive, as Windows names are).
function Test-BackupName([string]$Name) {
    $upper = $Name.ToUpperInvariant()
    foreach ($p in $ForbiddenPrefixesInFile) {
        if ($upper.StartsWith($p, [System.StringComparison]::Ordinal)) { return $true }
    }
    return $false
}

function Find-Docker {
    $cmd = Get-Command 'docker.exe' -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $fallback = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'
    if (Test-Path -LiteralPath $fallback) { return $fallback }
    return $null
}

function Wait-Docker {
    $start = Get-Date
    $deadline = $start.AddMinutes($DockerWaitMinutes)
    $nextNote = $start
    while ($true) {
        $docker = Find-Docker
        if ($docker) {
            $st = Start-Pumped $docker @('info', '--format', '{{.ServerVersion}}') '' -Quiet
            $done = Wait-Pumped $st 30
            if (-not $done) { Stop-Tree $st.Proc }
            elseif ((Get-PumpedExit $st) -eq 0) {
                Write-Log ("Docker engine is up (server {0})" -f (($st.Lines | Where-Object { $_ }) -join ' '))
                return $docker
            }
        }
        $now = Get-Date
        if ($now -ge $deadline) {
            Write-Log ("Docker engine not up after {0} min (is Docker Desktop running? 'Start Docker Desktop when you sign in' must be on); giving up" -f $DockerWaitMinutes)
            return $null
        }
        if ($now -ge $nextNote) {
            $what = if ($docker) { 'docker info fails' } else { 'docker.exe not found yet' }
            Write-Log ("waiting for the Docker engine ({0}): {1:N0} of {2} min" -f $what, ($now - $start).TotalMinutes, $DockerWaitMinutes)
            $nextNote = $nextNote.AddMinutes(1)
        }
        Start-Sleep -Seconds 5
    }
}

function Wait-Clock {
    $start = Get-Date
    $deadline = $start.AddMinutes($ClockWaitMinutes)
    while ($true) {
        $code = Invoke-Pumped $Py @('-c', $ClockProbe) '[clock] ' 60
        if ($code -eq 0) { Write-Log 'clock check passed (within the engine hard limit)'; return $true }
        $now = Get-Date
        if ($now -ge $deadline) {
            Write-Log ("clock not within the limit after {0} min; not resyncing (needs admin: w32tm /resync). Giving up" -f $ClockWaitMinutes)
            return $false
        }
        Write-Log ("clock check not passed yet ({0:N1} of {1} min); w32time resyncs every 15 min; next check in 30 s" -f ($now - $start).TotalMinutes, $ClockWaitMinutes)
        Start-Sleep -Seconds 30
    }
}

# Step 5b: redis-md, the market-data store (compose profile md), run only when XTNL_MD_ENABLED is on.
# Never fatal and never waited for: the engine does not need it (design A2), and without it the optional
# market-data services page and stay idle while ingestion and Oversee run as before.
function Start-RedisMd([string]$Docker) {
    try {
        if (-not (Test-Path -LiteralPath $MdAclFile)) {
            Write-Log "redis-md not started: its ACL file is missing ($MdAclFile); run deploy\local\new-md-credentials.ps1"
            return
        }
        Write-Log "starting redis-md (XTNL_MD_ENABLED=1): docker compose -f $ComposeFile --profile md up -d redis-md"
        $code = Invoke-Pumped $Docker @('compose', '--ansi', 'never', '-f', $ComposeFile, '--profile', 'md', 'up', '-d', 'redis-md') '[compose md] ' 120
        if ($code -ne 0) { Write-Log "redis-md did not start (exit $code); the engine starts without it" }
    } catch {
        Write-Log ("redis-md step failed: {0}; the engine starts without it" -f $_.Exception.GetType().Name)
    }
}

# Step 5b with the flag off: redis-md has restart: unless-stopped, so once started Docker brings it back after
# every reboot. Stop it (its data volume is kept) so it holds no memory while the market-data layer is off.
# Never fatal; the engine does not use it.
function Stop-RedisMd([string]$Docker) {
    try {
        $st = Start-Pumped $Docker @('compose', '--ansi', 'never', '-f', $ComposeFile, '--profile', 'md', 'ps', '-q', 'redis-md') '' -Quiet
        if (-not (Wait-Pumped $st 60)) { Stop-Tree $st.Proc; return }
        if (@($st.Lines | Where-Object { $_.Trim() }).Count -eq 0) { return }
        Write-Log "XTNL_MD_ENABLED is off but redis-md is running; stopping it: docker compose -f $ComposeFile --profile md stop redis-md"
        $code = Invoke-Pumped $Docker @('compose', '--ansi', 'never', '-f', $ComposeFile, '--profile', 'md', 'stop', 'redis-md') '[compose md] ' 120
        if ($code -ne 0) { Write-Log "redis-md did not stop (exit $code); stop it by hand with the command above" }
    } catch {
        Write-Log ("redis-md stop step failed: {0}; the engine starts regardless" -f $_.Exception.GetType().Name)
    }
}

function Invoke-Main {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    Write-Log ("start-xtnl starting (pid {0}, root {1})" -f $PID, $Root)

    # 1. single instance
    $script:Mutex = New-Object System.Threading.Mutex($false, $MutexName)
    try { $script:MutexOwned = $script:Mutex.WaitOne(0) }
    catch {
        if ($_.Exception.GetBaseException() -is [System.Threading.AbandonedMutexException]) { $script:MutexOwned = $true }
        else { throw }
    }
    if (-not $script:MutexOwned) {
        $other = if (Test-Path -LiteralPath $PidFile) { (Get-Content -LiteralPath $PidFile -TotalCount 1) } else { '?' }
        Write-Log "another start-xtnl is already running (pid $other); this copy exits"
        $script:ExitCode = 0
        return
    }
    [System.IO.File]::WriteAllText($PidFile, [string]$PID, $Utf8NoBom)
    Initialize-Native

    # 2. env file (no credentials allowed in it)
    if (-not (Test-Path -LiteralPath $EnvFile)) {
        Write-Log "env file missing: $EnvFile (copy deploy\env\local-live.env.example to deploy\env\local.env)"
        $script:ExitCode = 78; return
    }
    $fileVars = Read-EnvFile $EnvFile
    $bad = @($ForbiddenInFile | Where-Object { $fileVars.Contains($_) })
    if ($bad.Count -gt 0) {
        Write-Log ("refused: {0} assigns {1}; credentials come only from the Windows User environment. Remove those lines." -f $EnvFile, ($bad -join ', '))
        $script:ExitCode = 78; return
    }
    $badBackup = @($fileVars.Keys | Where-Object { Test-BackupName $_ })
    if ($badBackup.Count -gt 0) {
        Write-Log ("refused: {0} assigns {1}; backup credentials belong to the separate backup Windows account, never to the engine's. Remove those lines." -f $EnvFile, ($badBackup -join ', '))
        $script:ExitCode = 78; return
    }
    foreach ($k in $fileVars.Keys) { [Environment]::SetEnvironmentVariable($k, $fileVars[$k], 'Process') }
    Write-Log ("env file loaded: {0} ({1} settings)" -f $EnvFile, $fileVars.Count)
    # W19: the receiver tells engines apart by (node, epoch), and the epoch is only the lease revision, so two boxes
    # started from the same env file on fresh etcd volumes can look like one engine (home-server plan s4.8, s4.12).
    # A warning, not a refusal: an existing PC keeps running while the operator picks a name per box.
    $nodeId = ([string][Environment]::GetEnvironmentVariable('XTNL_NODE_ID', 'Process')).Trim()
    if ($ExampleNodeIds -contains $nodeId) {
        Write-Log ("warning: XTNL_NODE_ID is the example's '{0}'; give each machine its own name (for example home1 on the home server, from COMPUTERNAME {1}) so the dead-man receiver can tell two engines apart (deadman-alert.md, two engines); after the rename, one 'beating node changed' notice is expected and clears after 5 min" -f $nodeId, $env:COMPUTERNAME)
    }
    # Read as the engine reads it (src/runtime/optional_settings.py): the process value, i.e. the env file's or one
    # inherited from the Windows User environment, trimmed; exactly 1 is on, anything else off.
    $mdRaw = ([string][Environment]::GetEnvironmentVariable('XTNL_MD_ENABLED', 'Process')).Trim()
    $script:MdEnabled = $mdRaw -eq $MdFlagOn
    if ($script:MdEnabled) { Write-Log 'XTNL_MD_ENABLED=1: redis-md (market data) is started after the containers' }
    elseif ($MdFlagOff -contains $mdRaw) { Write-Log 'XTNL_MD_ENABLED is off: redis-md (market data) is not started' }
    else { Write-Log 'warning: XTNL_MD_ENABLED is neither 0 nor 1; treated as 0 (the engine pages it): redis-md is not started' }

    # 3. Windows User environment -> this process (names only in the log)
    $loaded = @(); $missing = @()
    foreach ($n in $UserVarNames) {
        $v = [Environment]::GetEnvironmentVariable($n, 'User')
        if ([string]::IsNullOrEmpty($v)) {
            if ($RequiredUserVars -contains $n) { $missing += $n }
            continue
        }
        if ($n -eq 'BROKER_ENV' -and $fileVars.Contains('BROKER_ENV') -and $fileVars['BROKER_ENV'] -ne $v) {
            Write-Log ("refused: BROKER_ENV in the Windows User environment differs from the one in {0}" -f $EnvFile)
            $script:ExitCode = 78; return
        }
        [Environment]::SetEnvironmentVariable($n, $v, 'Process')
        $loaded += $n
    }
    $v = $null
    Write-Log ("Windows User environment loaded (names only): {0}" -f ($loaded -join ', '))
    # Never copied, but this process inherits the account's User environment: name (never show) any backup
    # credential found there. Start-Pumped removes them from every child, so this is a warning, not a refusal.
    $inheritedBackup = @(Get-ChildItem Env: | ForEach-Object { $_.Name } | Where-Object { Test-BackupName $_ } | Sort-Object)
    # Review round 7: only these backup names are not secret (home-backup-restore.md "If a backup secret leaked"
    # step 5). Any other XTNL_BACKUP_* / RCLONE_* / B2_* name counts as a secret, an unknown one included.
    $secretBackup = @($inheritedBackup | Where-Object { $NonSecretBackupNames -notcontains $_ })
    if ($secretBackup.Count -gt 0) {
        # Review round 5: anything running as this account can read them from its User environment, so a secret
        # found here has leaked. Deleting it is not enough: rotate it at once, and distrust check C runs recorded
        # from the earliest time it may have leaked until the rotation; runs from before that time stay trusted
        # (home-backup-restore.md "Rotation", "If a backup secret leaked"; review round 7).
        Write-Log ("warning: this account's environment holds {0}; backup credentials belong to the separate backup Windows account: delete them here and rotate every secret among them now (a new check C ping URL for XTNL_BACKUP_HC_URL, a new B2 upload key for XTNL_BACKUP_B2_ACCOUNT / XTNL_BACKUP_B2_KEY; home-backup-restore.md 'If a backup secret leaked'); write the earliest time they may have leaked in the rotation note: check C runs recorded from that time until the rotation are not trusted for a restore, runs from before it stay trusted. They are removed from the engine, API and live view" -f ($inheritedBackup -join ', '))
    } elseif ($inheritedBackup.Count -gt 0) {
        Write-Log ("warning: non-secret backup names found in this account's environment: {0}; they belong to the separate backup Windows account: delete them here. None of them is secret, so nothing needs rotating and no check C run is distrusted (home-backup-restore.md 'If a backup secret leaked', step 5). They are removed from the engine, API and live view" -f ($inheritedBackup -join ', '))
    }
    if ($missing.Count -gt 0) {
        Write-Log ("refused: not set in the Windows User environment: {0}" -f ($missing -join ', '))
        $script:ExitCode = 78; return
    }
    Write-Log ("BROKER_ENV={0}; Phase 1 never sends orders" -f $env:BROKER_ENV)
    if ($NoApi) {
        Write-Log 'order-server API disabled (-NoApi)'
    } elseif (($loaded -contains 'XTNL_API_SECRET') -and -not ($loaded -contains 'XTNL_API_DSN')) {
        Write-Log 'order-server API disabled: XTNL_API_DSN (read-only role xtnl_api) is not set in the Windows User environment (deploy\local\new-api-credentials.ps1 -DbRole)'
    } elseif ($loaded -contains 'XTNL_API_SECRET') {
        $script:Api.Enabled = $true
        Write-Log 'order-server API enabled (XTNL_API_SECRET and XTNL_API_DSN are set); it runs alongside the engine without the broker credentials'
    } else {
        Write-Log 'order-server API disabled: XTNL_API_SECRET is not set in the Windows User environment (deploy\local\new-api-credentials.ps1)'
    }
    if ($NoLiveView) {
        Write-Log 'live view disabled (-NoLiveView)'
    } elseif ($loaded -contains 'XTNL_LIVEVIEW_DSN') {
        $script:LiveView.Enabled = $true
        $lvAddr = ([string][Environment]::GetEnvironmentVariable('XTNL_LIVEVIEW_ADDR', 'Process')).Trim()
        if ($lvAddr -eq '') { $lvAddr = '127.0.0.1:8090' }
        Write-Log ("live view enabled (XTNL_LIVEVIEW_DSN is set): http://{0}/ after the engine starts; read-only, never sends orders" -f $lvAddr)
    } else {
        Write-Log 'live view disabled: XTNL_LIVEVIEW_DSN is not set in the Windows User environment (deploy\local\new-liveview-credentials.ps1)'
    }
    $env:PYTHONUNBUFFERED = '1'
    $env:PYTHONIOENCODING = 'utf-8'
    if (-not (Test-Path -LiteralPath $Py)) {
        Write-Log "refused: $Py not found (python -m venv .venv; pip install -e .[dev])"
        $script:ExitCode = 78; return
    }

    # Single-PC mode for the engine this script starts (src/runtime/single_pc.py): SAFE_HOLD auto-resume after a
    # cool-down and the 'not recording' page. A value from the env file or the Windows User environment wins.
    $singlePc = ([string][Environment]::GetEnvironmentVariable('XTNL_SINGLE_PC', 'Process')).Trim()
    if ($singlePc -eq '') {
        [Environment]::SetEnvironmentVariable('XTNL_SINGLE_PC', '1', 'Process')
        $singlePc = '1'
    }
    if ($singlePc -eq '1') { Write-Log 'XTNL_SINGLE_PC=1: the engine resumes a SAFE_HOLD itself after a cool-down (operations.md s4 / s17)' }
    else { Write-Log 'warning: XTNL_SINGLE_PC is not 1 (set in the env file or the Windows User environment): a SAFE_HOLD waits for an operator resume' }

    # 4-8, retried here: Task Scheduler does not restart a task whose action exits non-zero.
    $delay = $RetryFirstS
    $downSince = $null       # start of the first failed attempt of the current streak (the engine not running since)
    $nextAlert = $null
    while ($true) {
        $started = Get-Date
        try { Invoke-Attempt }
        catch {
            Write-Log ("error: {0}: {1}" -f $_.Exception.GetType().Name, $_.Exception.Message)
            $script:ExitCode = 1
        }
        $code = $script:ExitCode
        if ($code -eq 78) {
            Write-Log 'ALERT: configuration refused (exit 78; see the lines above): the engine stays off and records nothing until the cause is fixed and the task is started again'
        }
        if ($NoRetry -or $code -eq 0 -or $code -eq 78) { return }
        if (((Get-Date) - $started).TotalMinutes -ge 10) { $delay = $RetryFirstS; $downSince = $null; $nextAlert = $null }
        if ($null -eq $downSince) { $downSince = $started; $nextAlert = $started.AddMinutes($AlertAfterMinutes) }
        if ((Get-Date) -ge $nextAlert) {
            Write-Log ("ALERT: the engine has not run for {0:N0} min (last attempt exit {1}: {2}); still retrying. See the lines above and docs\runbooks\single-pc-autostart.md" -f ((Get-Date) - $downSince).TotalMinutes, $code, (Get-ExitMeaning $code))
            $nextAlert = (Get-Date).AddMinutes(60)
        }
        Write-Log ("attempt ended (exit {0}); retrying from the Docker check in {1} s (Ctrl+C or stop-xtnl.ps1 stops)" -f $code, $delay)
        $script:ExitCode = $null    # Ctrl+C while waiting is a clean stop
        $script:Engine = $null
        Start-Sleep -Seconds $delay
        $delay = [Math]::Min($delay * 2, $RetryMaxS)
    }
}

function Get-ExitMeaning([int]$Code) {
    switch ($Code) {
        10 { return 'Docker engine not up' }
        11 { return 'docker compose up failed' }
        12 { return 'migrations failed' }
        13 { return 'clock not within the limit' }
        1 { return 'script or engine error' }
        default { return 'engine / run-local.ps1 exit' }
    }
}

# Steps 4-8: one attempt. Sets $script:ExitCode.
function Invoke-Attempt {
    # 4. Docker engine
    $docker = Wait-Docker
    if (-not $docker) { $script:ExitCode = 10; return }

    # 5. containers
    Write-Log "starting containers: docker compose -f $ComposeFile up -d --wait"
    $code = Invoke-Pumped $docker @('compose', '--ansi', 'never', '-f', $ComposeFile, 'up', '-d', '--wait', '--wait-timeout', '300') '[compose] ' 360
    if ($code -ne 0) { Write-Log "docker compose up failed (exit $code)"; $script:ExitCode = 11; return }

    # 5b. redis-md, only with XTNL_MD_ENABLED=1 (non-fatal, not waited for); stopped when the flag is off
    if ($script:MdEnabled) { Start-RedisMd $docker } else { Stop-RedisMd $docker }

    # 6. migrations (idempotent), only up to the accepted level (src/db/migrate.py; later ones are held back and named)
    $upto = ([string][Environment]::GetEnvironmentVariable('XTNL_MIGRATIONS_ACCEPTED_UPTO', 'Process')).Trim()
    if ($upto -eq '') { $upto = '14 (default)' }
    Write-Log ("applying migrations up to XTNL_MIGRATIONS_ACCEPTED_UPTO={0}: python -m src.db.migrate" -f $upto)
    $code = Invoke-Pumped $Py @('-m', 'src.db.migrate') '[migrate] ' 300
    if ($code -ne 0) { Write-Log "migrations failed (exit $code)"; $script:ExitCode = 12; return }

    # 7. clock. Giving up never keeps the engine off (-NoRetry aside): it applies the same hard check itself, records
    # nothing while the clock is off and becomes leader as soon as it passes.
    if (-not (Wait-Clock)) {
        if ($NoRetry) { $script:ExitCode = 13; return }
        Write-Log ("ALERT: clock not within the limit after {0} min; starting the engine anyway: it records nothing until its own clock check passes (an elevated resync fixes it now)" -f $ClockWaitMinutes)
    }

    # 8. engine (run-local.ps1 restarts it after hard exits) and, when enabled, the order-server API and then the
    # live view beside it
    Write-Log "running deploy\local\run-local.ps1 -EnvFile $EnvFile"
    $script:Engine = Start-Pumped $PsExe @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $RunLocal, '-EnvFile', $EnvFile) '' -DropEnv $EngineDropEnv
    $script:Api.NextStart = Get-Date
    $script:Api.Delay = $ApiRetryFirstS
    $script:LiveView.NextStart = Get-Date
    $script:LiveView.Delay = $LiveViewRetryFirstS
    while (-not (Step-Pumped $script:Engine)) {
        Step-Api
        Step-LiveView
        $pending = New-Object System.Collections.Generic.List[System.Threading.Tasks.Task]
        foreach ($st in @($script:Engine, $script:Api.St, $script:LiveView.St)) {
            if ($null -eq $st) { continue }
            if ($null -ne $st.TOut) { $pending.Add($st.TOut) }
            if ($null -ne $st.TErr) { $pending.Add($st.TErr) }
        }
        if ($pending.Count -gt 0) { [void][System.Threading.Tasks.Task]::WaitAny($pending.ToArray(), 1000) }
    }
    [void]$script:Engine.Proc.WaitForExit(10000)
    Stop-Api
    Stop-LiveView
    $code = Get-PumpedExit $script:Engine
    if ($null -eq $code) { $code = 1 }
    Write-Log "run-local.ps1 ended (exit $code)"
    $script:ExitCode = [int]$code
}

function Invoke-Cleanup {
    try { Stop-Api } catch { }
    try { Stop-LiveView } catch { }
    if ($null -ne $script:Engine) {
        if (-not (Wait-Pumped $script:Engine 60)) { Write-Log 'engine output still open after 60 s' }
    }
    if ($script:NativeOk) {
        $deadline = (Get-Date).AddSeconds(60)
        $n = [XtnlStartNative]::ActiveProcesses()
        if ($n -gt 1) { Write-Log 'waiting up to 60 s for the engine to drain and exit' }
        while ($n -gt 1 -and (Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 500
            $n = [XtnlStartNative]::ActiveProcesses()
        }
        if ($n -gt 1) { Write-Log 'child processes still running after 60 s; they end with this script (the next start-up catch-up re-reads what changed)' }
    }
    if ($script:MutexOwned) {
        try {
            if ((Test-Path -LiteralPath $PidFile) -and ((Get-Content -LiteralPath $PidFile -TotalCount 1) -eq [string]$PID)) {
                Remove-Item -LiteralPath $PidFile -Force
            }
        } catch { }
        try { $script:Mutex.ReleaseMutex() } catch { }
    }
    if ($null -ne $script:Mutex) { $script:Mutex.Dispose() }
}

try {
    Invoke-Main
}
catch {
    # Type and message only; nothing here carries a credential (they are never logged or echoed).
    Write-Log ("error: {0}: {1}" -f $_.Exception.GetType().Name, $_.Exception.Message)
    $script:ExitCode = 1
}
finally {
    if ($null -eq $script:ExitCode) {
        # Reached only on Ctrl+C / a stop request: a clean stop, so the task does not restart it.
        Write-Log 'stop requested (Ctrl+C); draining'
        $script:ExitCode = 0
    }
    try { Invoke-Cleanup } catch { }
    Write-Log ("start-xtnl exits ({0})" -f $script:ExitCode)
    [Environment]::Exit([int]$script:ExitCode)
}
