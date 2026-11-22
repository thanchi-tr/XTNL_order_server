<#
.SYNOPSIS
  Stop the XTNL engine started by start-xtnl.ps1 (the 'XTNL engine' scheduled task) gracefully.

.DESCRIPTION
  Sends Ctrl+C to start-xtnl.ps1's hidden console (send-ctrl-c.ps1): the engine gets SIGINT and drains, records
  its stop in engine_runs (end_reason SIGINT) and exits; the order-server API (python -m src.api, when enabled)
  and the live view (python -m src.liveview, when enabled; read-only, docs/runbooks/live-view.md) on the same
  console close their listeners and exit too; start-xtnl.ps1 waits for them and exits 0, so the task
  does not restart it. If it has not stopped within -TimeoutS, the task is ended hard (Stop-ScheduledTask /
  Stop-Process); the kill-on-close job ends the engine with it and the next start-up catch-up re-reads what
  changed. The containers keep running unless -Containers is given (`docker compose --profile md stop`, data
  kept; the profile makes it stop redis-md, the market-data store, too).
  Phase 1 never sends orders. Runbook: docs/runbooks/single-pc-autostart.md.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1
#>
param(
    [int]$TimeoutS = 90,
    [switch]$Containers
)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$PidFile = Join-Path $Root 'var\logs\start-xtnl.pid'
$Helper = Join-Path $PSScriptRoot 'send-ctrl-c.ps1'
$ComposeFile = Join-Path $Root 'deploy\local\docker-compose.local.yml'
$PsExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$TaskName = 'XTNL engine'

function Get-StartXtnl {
    if (-not (Test-Path -LiteralPath $PidFile)) { return $null }
    $raw = (Get-Content -LiteralPath $PidFile -TotalCount 1)
    $id = 0
    if (-not [int]::TryParse(([string]$raw).Trim(), [ref]$id)) { return $null }
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $id" -ErrorAction SilentlyContinue
    # Only a PowerShell running start-xtnl.ps1 counts (a stale pid file may name a reused PID).
    if ($null -eq $proc -or $proc.Name -ne 'powershell.exe' -or $proc.CommandLine -notmatch 'start-xtnl\.ps1') { return $null }
    return $id
}

$target = Get-StartXtnl
if ($null -eq $target) {
    Write-Host 'start-xtnl.ps1 is not running (no live pid in var\logs\start-xtnl.pid).'
} else {
    Write-Host "sending Ctrl+C to start-xtnl.ps1 (pid $target), the engine, the order-server API and the live view on its console"
    $helperArgs = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', ('"{0}"' -f $Helper), '-TargetPid', [string]$target)
    $h = Start-Process -FilePath $PsExe -ArgumentList $helperArgs -WindowStyle Hidden -Wait -PassThru
    if ($h.ExitCode -ne 0) { Write-Host "Ctrl+C could not be delivered (helper exit $($h.ExitCode))" }
    else { Write-Host "Ctrl+C sent; waiting up to $TimeoutS s for the engine to drain and start-xtnl.ps1 to exit" }
    $gone = $false
    if ($h.ExitCode -eq 0) {
        try { Wait-Process -Id $target -Timeout $TimeoutS -ErrorAction Stop; $gone = $true }
        catch { $gone = -not (Get-Process -Id $target -ErrorAction SilentlyContinue) }
    }
    if ($gone) {
        Write-Host 'stopped cleanly (engine, order-server API and live view).'
    } else {
        Write-Host 'not stopped in time: ending it hard (its kill-on-close job ends the engine too).'
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        if ($null -ne $task -and $task.State -eq 'Running') { Stop-ScheduledTask -TaskName $TaskName }
        Stop-Process -Id $target -Force -ErrorAction SilentlyContinue
        Write-Host 'ended. The next start-up runs a catch-up from the last recorded coverage.'
    }
}

if ($Containers) {
    # --profile md: redis-md (market data) is stopped too; a plain `stop` leaves profile services running.
    Write-Host "stopping containers (data kept): docker compose -f $ComposeFile --profile md stop"
    & docker compose -f $ComposeFile --profile md stop
}
