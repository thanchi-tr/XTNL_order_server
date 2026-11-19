<#
.SYNOPSIS
  Show the state of the single-PC XTNL engine: scheduled task, start-xtnl.ps1, containers, last log lines,
  and the engine's own uptime / catch-up views. Read-only; prints no secrets.

.DESCRIPTION
  * Scheduled task 'XTNL engine': state, last run time and result, next run time.
  * start-xtnl.ps1: the PID in var\logs\start-xtnl.pid, if alive.
  * Containers of compose project xtnl-local (deploy\local\docker-compose.local.yml).
  * The last -Lines lines of the newest var\logs\xtnl-*.log. Lines naming a credential variable are hidden and
    any password in a URL is masked (the engine already redacts; this is a second guard).
  * The last -AlertLines 'ALERT:' (start-xtnl.ps1 / run-local.ps1), 'PAGE[critical]' and 'direct page' lines (the
    engine's critical pages while it holds no leader term, and whether the alert channels took them) over every
    var\logs\xtnl-*.log, oldest file first, masked the same way: on this PC they are what says the engine is not
    recording (docs/runbooks/single-pc-autostart.md).
  * python -m src.ops uptime show --limit 3 and catchup show --limit 1 against the local DSN from
    deploy\env\local.env (counts, ids and times only, never an amount).
  * The order-server API (docs/runbooks/order-server-api.md): whether TCP XTNL_API_PORT (default 47443) listens,
    and a signed GET /v1/health over https://127.0.0.1 pinned to XTNL_API_TLS_CERT from deploy\env\local.env
    (default var\api-tls\server.crt; python -m
    src.api.selfcheck, with XTNL_API_SECRET copied from the Windows User environment, never shown), plus the
    certificate fingerprint to compare with Vercel's ORDER_SERVER_TLS_FINGERPRINT.
  * Windows time sync: the offset the engine's own clock check measures (w32tm on Windows; limit 50 ms).
  * redis-md, the market-data store (compose profile md): XTNL_MD_ENABLED from the env file (or inherited from the
    Windows User environment; on only when it is 1 after trimming, as the engine reads it), whether the ACL file
    of deploy\local\new-md-credentials.ps1 exists, the container state, and with the flag on `python -m src.ops md
    show` (counts and states only, never a value; XTNL_MD_REDIS_READER_URL and the content-hash keys are copied
    from the Windows User environment into this process only for that call, never shown).
  * The ARM (advice only): XTNL_ARM_ENABLED from the env file and its FAILED scopes (`python -m src.ops arm failed
    show`).
  Phase 1 never sends orders. Runbook: docs/runbooks/single-pc-autostart.md.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\status-xtnl.ps1
#>
param(
    [int]$Lines = 30,
    [int]$AlertLines = 20,
    [string]$EnvFile
)
$ErrorActionPreference = 'Continue'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Set-Location $Root
if (-not $EnvFile) { $EnvFile = Join-Path $Root 'deploy\env\local.env' }
$TaskName = 'XTNL engine'
$ComposeFile = Join-Path $Root 'deploy\local\docker-compose.local.yml'
$LogDir = Join-Path $Root 'var\logs'
$PidFile = Join-Path $LogDir 'start-xtnl.pid'
$Py = Join-Path $Root '.venv\Scripts\python.exe'
$Hide = '(?i)(STONE_X_(USERNAME|PASS|APP_KEY)|password|passwd|app_?key|secret|token|username|access_key|push_user|config_dsn|hmac_key(?!_(prev_)?id)|md_redis_)'
$MdAclFile = Join-Path $env:LOCALAPPDATA 'XTNL\redis-md\users.acl'
# Copied from the Windows User environment for `src.ops md show` only, then removed again (never shown).
$MdReaderVars = @('XTNL_MD_REDIS_READER_URL', 'XTNL_MD_HMAC_KEY', 'XTNL_MD_HMAC_KEY_PREV')
# The engine's own clock check (as start-xtnl.ps1 step 7): prints the measured offset and the limit.
$ClockProbe = "import asyncio, sys; from src.runtime.probes import clock_reader; " +
              "from src.runtime.health import CHRONY_HARD_MS; r = clock_reader(); a = asyncio.run(r.offset_ms()); " +
              "s = r.signed_offset_s; print('offset_ms=%s limit_ms=%.0f' % ('unreadable' if s is None else " +
              "'%+.1f' % (s * 1000.0), CHRONY_HARD_MS)); sys.exit(0 if a is not None and a <= CHRONY_HARD_MS else 1)"

function Show-Title([string]$Text) { Write-Host ''; Write-Host "== $Text" }
function Format-LogLine([string]$Text) {
    if ($Text -match $Hide) { return '[line hidden: names a credential]' }
    return ($Text -replace '://([^:/@\s]+):[^@\s]+@', '://$1:***@')
}

Show-Title "scheduled task '$TaskName'"
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -eq $task) {
    Write-Host 'not registered (deploy\local\install-autostart.ps1 registers it)'
} else {
    $info = Get-ScheduledTaskInfo -TaskName $TaskName
    Write-Host ("state {0}; last run {1}; last result {2} (0x{2:X}); next run {3}" -f `
        $task.State, $info.LastRunTime, $info.LastTaskResult, $info.NextRunTime)
}

Show-Title 'start-xtnl.ps1'
$alive = $false
if (Test-Path -LiteralPath $PidFile) {
    $id = 0
    if ([int]::TryParse(([string](Get-Content -LiteralPath $PidFile -TotalCount 1)).Trim(), [ref]$id)) {
        $p = Get-CimInstance Win32_Process -Filter "ProcessId = $id" -ErrorAction SilentlyContinue
        if ($null -ne $p -and $p.CommandLine -match 'start-xtnl\.ps1') {
            $alive = $true
            Write-Host ("running, pid {0}, since {1}" -f $id, (Get-Process -Id $id).StartTime)
        }
    }
}
if (-not $alive) { Write-Host 'not running' }

Show-Title 'containers (xtnl-local, redis-md included)'
$haveDocker = [bool](Get-Command docker -ErrorAction SilentlyContinue)
if ($haveDocker) {
    & docker compose -f $ComposeFile --profile md ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}\t{{.Ports}}'
} else {
    Write-Host 'docker not found on PATH'
}

Show-Title "last $Lines log lines"
$log = Get-ChildItem -Path $LogDir -Filter 'xtnl-*.log' -File -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($null -eq $log) {
    Write-Host 'no log yet (var\logs\xtnl-<date>.log)'
} else {
    Write-Host $log.FullName
    Get-Content -LiteralPath $log.FullName -Tail $Lines | ForEach-Object { Format-LogLine $_ } | Write-Host
}

Show-Title "last $AlertLines ALERT / PAGE[critical] / direct page lines (all var\logs\xtnl-*.log)"
$allLogs = @(Get-ChildItem -Path $LogDir -Filter 'xtnl-*.log' -File -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime)
if ($allLogs.Count -eq 0) {
    Write-Host 'no log yet'
} else {
    $hits = @(Select-String -LiteralPath ($allLogs | ForEach-Object { $_.FullName }) `
        -Pattern 'ALERT:|PAGE\[critical\]|direct page' -ErrorAction SilentlyContinue |
        Select-Object -Last $AlertLines)
    if ($hits.Count -eq 0) {
        Write-Host 'none'
    } else {
        $hits | ForEach-Object { '{0}:{1}: {2}' -f $_.Filename, $_.LineNumber, (Format-LogLine $_.Line) } | Write-Host
    }
}

Show-Title 'engine uptime / catch-up (python -m src.ops, read-only)'
$opsReady = $false
if (-not (Test-Path -LiteralPath $EnvFile)) {
    Write-Host "env file missing: $EnvFile"
} elseif (-not (Test-Path -LiteralPath $Py)) {
    Write-Host "not found: $Py"
} else {
    $opsReady = $true
    foreach ($line in Get-Content -LiteralPath $EnvFile) {
        $t = $line.Trim()
        if ($t -eq '' -or $t.StartsWith('#')) { continue }
        $i = $t.IndexOf('=')
        if ($i -lt 1) { continue }
        $k = $t.Substring(0, $i).Trim()
        if ($k -match $Hide) { continue }   # never load anything credential-like
        [Environment]::SetEnvironmentVariable($k, $t.Substring($i + 1).Trim(), 'Process')
    }
    & $Py -m src.ops uptime show --limit 3
    & $Py -m src.ops catchup show --limit 1
}

Show-Title 'order-server API (python -m src.api)'
$apiPort = 47443
if ($env:XTNL_API_PORT -match '^[0-9]+$') { $apiPort = [int]$env:XTNL_API_PORT }
$listen = @(Get-NetTCPConnection -State Listen -LocalPort $apiPort -ErrorAction SilentlyContinue)
if ($listen.Count -gt 0) {
    Write-Host ("listening: {0}" -f (($listen | ForEach-Object { '{0}:{1} (pid {2})' -f $_.LocalAddress, $_.LocalPort, $_.OwningProcess }) -join ', '))
} else {
    Write-Host "nothing listens on TCP $apiPort"
}
$apiSecret = [Environment]::GetEnvironmentVariable('XTNL_API_SECRET', 'User')
if ([string]::IsNullOrEmpty($apiSecret)) {
    Write-Host 'XTNL_API_SECRET is not set in the Windows User environment: the API is disabled (deploy\local\new-api-credentials.ps1)'
} elseif (-not (Test-Path -LiteralPath $Py)) {
    Write-Host "not found: $Py"
} else {
    [Environment]::SetEnvironmentVariable('XTNL_API_SECRET', $apiSecret, 'Process')
    & $Py -m src.api.selfcheck
    & $Py -m src.api.selfcheck --fingerprint
    [Environment]::SetEnvironmentVariable('XTNL_API_SECRET', $null, 'Process')
}
$apiSecret = $null

Show-Title 'Windows time sync (the engine clock check: w32tm offset, limit 50 ms)'
if (Test-Path -LiteralPath $Py) { & $Py -c $ClockProbe } else { Write-Host "not found: $Py" }

Show-Title 'redis-md (market data, compose profile md)'
# Read as the engine and start-xtnl.ps1 read it (src/runtime/optional_settings.py): the process value (the env file's
# or one inherited from the Windows User environment), trimmed; exactly 1 is on, anything else off.
$mdRaw = ([string]$env:XTNL_MD_ENABLED).Trim()
$mdOn = $opsReady -and ($mdRaw -eq '1')
$mdFlag = if (-not $opsReady) { '?' } elseif ($mdOn) { 'on' } elseif (@('', '0') -contains $mdRaw) { 'off' } else { 'off (not 0 or 1; paged)' }
Write-Host ("XTNL_MD_ENABLED is {0} (only 1 starts redis-md and the market-data services)" -f $mdFlag)
if (Test-Path -LiteralPath $MdAclFile) { Write-Host "ACL file present: $MdAclFile" }
else { Write-Host "ACL file missing: $MdAclFile (deploy\local\new-md-credentials.ps1 creates it)" }
if ($haveDocker) {
    & docker compose -f $ComposeFile --profile md ps redis-md --format '{{.Service}}: {{.State}} {{.Status}}'
}
if ($mdOn) {
    $copied = @()
    foreach ($n in $MdReaderVars) {
        $val = [Environment]::GetEnvironmentVariable($n, 'User')
        if (-not [string]::IsNullOrEmpty($val)) { [Environment]::SetEnvironmentVariable($n, $val, 'Process'); $copied += $n }
    }
    $val = $null
    if ($copied -contains 'XTNL_MD_REDIS_READER_URL') { & $Py -m src.ops md show }
    else { Write-Host 'XTNL_MD_REDIS_READER_URL is not set in the Windows User environment (deploy\local\new-md-credentials.ps1)' }
    foreach ($n in $copied) { [Environment]::SetEnvironmentVariable($n, $null, 'Process') }
}

Show-Title 'ARM (advice only; python -m src.ops, read-only)'
$armFlag = if ($opsReady) { [string]$env:XTNL_ARM_ENABLED } else { '?' }
Write-Host ("XTNL_ARM_ENABLED={0} in the env file (only 1 with XTNL_MD_ENABLED=1 starts the ARM)" -f $armFlag)
if ($opsReady) { & $Py -m src.ops arm failed show }
