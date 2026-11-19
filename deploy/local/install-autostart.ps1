<#
.SYNOPSIS
  Register the 'XTNL engine' scheduled task: start the engine at log-on of the current user (single-PC mode).

.DESCRIPTION
  Run it yourself, once, from a PowerShell window in the project (it is never run automatically):
      powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-autostart.ps1
  Idempotent: re-running replaces the task (Register-ScheduledTask -Force). It changes no Windows, power or
  Docker Desktop setting; the runbook lists the ones you set yourself. Phase 1 never sends orders.

  Task 'XTNL engine', for the current user:
    * trigger: At log on of this user, 60 s delay (Docker Desktop starts first);
    * action:  powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File <root>\deploy\local\start-xtnl.ps1
      (-ViaConhost wraps it in conhost.exe, for when Windows Terminal is the default terminal and leaves a window open);
    * run only when the user is logged on (Docker Desktop runs per user), normal limited rights, normal priority;
    * restart on failure every 1 min, up to 999 times (a failed launch; start-xtnl.ps1 retries its own
      failures, since a non-zero exit does not restart a task); no execution time limit;
    * start on batteries and do not stop on batteries; start when available (a missed start runs later);
    * a second start while one runs is ignored (start-xtnl.ps1 also has its own single-instance guard).
  Runbook: docs/runbooks/single-pc-autostart.md. Remove with deploy\local\uninstall-autostart.ps1.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-autostart.ps1 -StartNow
#>
param(
    [switch]$StartNow,
    [switch]$ViaConhost
)
$ErrorActionPreference = 'Stop'
$TaskName = 'XTNL engine'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$StartScript = Join-Path $Root 'deploy\local\start-xtnl.ps1'
if (-not (Test-Path -LiteralPath $StartScript)) { throw "not found: $StartScript" }
if (-not (Test-Path -LiteralPath (Join-Path $Root 'deploy\env\local.env'))) {
    Write-Warning 'deploy\env\local.env is missing: copy deploy\env\local-live.env.example to it before the first start.'
}
$User = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$PsExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$PsArgs = '-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}"' -f $StartScript

if ($ViaConhost) {
    $action = New-ScheduledTaskAction -Execute (Join-Path $env:SystemRoot 'System32\conhost.exe') `
        -Argument ('"{0}" {1}' -f $PsExe, $PsArgs) -WorkingDirectory $Root
} else {
    $action = New-ScheduledTaskAction -Execute $PsExe -Argument $PsArgs -WorkingDirectory $Root
}
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $User
$trigger.Delay = 'PT60S'
$settings = New-ScheduledTaskSettingsSet `
    -RestartInterval (New-TimeSpan -Minutes 1) -RestartCount 999 `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -Priority 4
# Interactive = "Run only when user is logged on": Docker Desktop runs in the user's session.
$principal = New-ScheduledTaskPrincipal -UserId $User -LogonType Interactive -RunLevel Limited

$task = Register-ScheduledTask -TaskName $TaskName -Force `
    -Description 'XTNL Phase 1 read-only engine (single-PC mode): deploy\local\start-xtnl.ps1. Never sends orders. Runbook docs\runbooks\single-pc-autostart.md.' `
    -Action $action -Trigger $trigger -Settings $settings -Principal $principal

$t = Get-ScheduledTask -TaskName $TaskName
$s = $t.Settings
Write-Host "registered scheduled task '$TaskName' (replaces any earlier one)"
Write-Host ("  user:        {0} (run only when logged on, {1})" -f $t.Principal.UserId, $t.Principal.RunLevel)
Write-Host ("  trigger:     at log on, delay {0}" -f $t.Triggers[0].Delay)
Write-Host ("  action:      {0} {1}" -f $t.Actions[0].Execute, $t.Actions[0].Arguments)
Write-Host ("  working dir: {0}" -f $t.Actions[0].WorkingDirectory)
Write-Host ("  restart:     every {0}, up to {1} times; time limit {2} (PT0S = none)" -f $s.RestartInterval, $s.RestartCount, $s.ExecutionTimeLimit)
Write-Host ("  batteries:   start {0}, stop when on batteries {1}; start when available {2}; priority {3}" -f `
    (-not $s.DisallowStartIfOnBatteries), $s.StopIfGoingOnBatteries, $s.StartWhenAvailable, $s.Priority)
Write-Host '  logs:        var\logs\xtnl-<date>.log (14 days); status: deploy\local\status-xtnl.ps1'
if ($StartNow) {
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "started '$TaskName' now."
} else {
    Write-Host "not started now: it runs at your next log-on, or: Start-ScheduledTask -TaskName '$TaskName'"
}
