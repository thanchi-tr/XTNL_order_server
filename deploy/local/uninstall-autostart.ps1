<#
.SYNOPSIS
  Remove the 'XTNL engine' scheduled task (stops the engine gracefully first if it is running).

.DESCRIPTION
  Stops start-xtnl.ps1 with deploy\local\stop-xtnl.ps1 (Ctrl+C, the engine drains), then unregisters the task.
  The containers and their data volumes are left alone; the runbook shows how to stop or remove them.
  Phase 1 never sends orders. Runbook: docs/runbooks/single-pc-autostart.md.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\uninstall-autostart.ps1
#>
param([switch]$KeepRunning)
$ErrorActionPreference = 'Stop'
$TaskName = 'XTNL engine'
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $KeepRunning) {
    & (Join-Path $PSScriptRoot 'stop-xtnl.ps1')
}
if ($null -eq $task) {
    Write-Host "no scheduled task '$TaskName' registered; nothing to remove."
} else {
    if ($task.State -eq 'Running' -and -not $KeepRunning) { Stop-ScheduledTask -TaskName $TaskName }
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "removed scheduled task '$TaskName'."
}
Write-Host 'containers and volumes (xtnl_local_pg, xtnl_local_etcd) left as they are:'
Write-Host '  stop them:   docker compose -f deploy\local\docker-compose.local.yml stop'
Write-Host '  remove them: docker compose -f deploy\local\docker-compose.local.yml down   (volumes kept; never -v unless you mean to delete the history)'
