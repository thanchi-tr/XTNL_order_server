<#
.SYNOPSIS
  Register (or with -Uninstall remove) the 'XTNL UPS watch' scheduled task: deploy\local\ups-watch.ps1 at log-on
  of the current user (home-server plan W12, the fallback when PowerPanel cannot drain the engine itself).

.DESCRIPTION
  Run it yourself, once, as the engine user (xtnl), from a PowerShell window in the project (it is never run
  automatically):
      powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-ups-watch.ps1
  Idempotent: re-running replaces the task (Register-ScheduledTask -Force). A watcher that is already running is
  stopped first (Stop-ScheduledTask) and started again with the new definition, because a running copy keeps the
  arguments it was started with (a -DryRun copy would otherwise go on only logging). It changes no Windows, power, UPS or
  PowerPanel setting; the runbook (docs\runbooks\single-pc-autostart.md "UPS: clean stop before the battery runs
  out") lists the ones you set yourself. Phase 1 never sends orders.

  Install it only on the PC behind the UPS: on a laptop the watcher would shut the laptop down when its own
  battery runs low on battery power.

  Task 'XTNL UPS watch', for the current user, mirroring install-autostart.ps1's 'XTNL engine':
    * trigger: At log on of this user, 15 s delay;
    * action:  powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File <root>\deploy\local\ups-watch.ps1
      plus the thresholds below (-ViaConhost wraps it in conhost.exe, as for the engine task);
    * run only when the user is logged on (Interactive): stop-xtnl.ps1's Ctrl+C helper only reaches the engine's
      console from the same session, so the watcher must run in the engine user's session, like the engine;
    * normal limited rights, normal priority; restart on a failed launch every 1 min, up to 999 times; no time limit;
    * start on batteries and do NOT stop on batteries (the whole point: it must keep running on the UPS battery);
      start when available; a second start while one runs is ignored (ups-watch.ps1 has its own mutex too).
  Parameters passed to ups-watch.ps1: -PollSeconds (30), -ChargePercent (45), -RuntimeMinutes (10),
  -ShutdownDelayS (60), and -NoShutdown / -DryRun when given (see ups-watch.ps1).

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-ups-watch.ps1 -DryRun -StartNow
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-ups-watch.ps1 -RuntimeMinutes 15 -NoShutdown
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\install-ups-watch.ps1 -Uninstall
#>
param(
    [int]$PollSeconds = 30,
    [int]$ChargePercent = 45,
    [int]$RuntimeMinutes = 10,
    [int]$ShutdownDelayS = 60,
    [switch]$NoShutdown,
    [switch]$DryRun,
    [switch]$StartNow,
    [switch]$ViaConhost,
    [switch]$Uninstall
)
$ErrorActionPreference = 'Stop'
$TaskName = 'XTNL UPS watch'

if ($Uninstall) {
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -eq $task) {
        Write-Host "no scheduled task '$TaskName' registered; nothing to remove."
    } else {
        if ($task.State -eq 'Running') { Stop-ScheduledTask -TaskName $TaskName }
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "removed scheduled task '$TaskName'. PowerPanel (if installed) is unchanged."
    }
    exit 0
}

$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$WatchScript = Join-Path $Root 'deploy\local\ups-watch.ps1'
if (-not (Test-Path -LiteralPath $WatchScript)) { throw "not found: $WatchScript" }
# The same ranges ups-watch.ps1 enforces (it exits 78 otherwise, which a scheduled task would hide).
if ($PollSeconds -lt 5 -or $PollSeconds -gt 600) { throw '-PollSeconds must be 5..600' }
if ($ChargePercent -lt 0 -or $ChargePercent -gt 99) { throw '-ChargePercent must be 0..99 (0 = off)' }
if ($RuntimeMinutes -lt 0 -or $RuntimeMinutes -gt 240) { throw '-RuntimeMinutes must be 0..240 (0 = off)' }
if ($ChargePercent -eq 0 -and $RuntimeMinutes -eq 0) { throw 'at least one of -ChargePercent / -RuntimeMinutes must be above 0' }
if ($ShutdownDelayS -lt 0 -or $ShutdownDelayS -gt 600) { throw '-ShutdownDelayS must be 0..600' }

$batteries = @(Get-CimInstance -ClassName Win32_Battery -ErrorAction SilentlyContinue)
if ($batteries.Count -eq 0) {
    Write-Warning 'Win32_Battery reports no battery now: connect the UPS USB cable. The watcher logs that once and keeps checking.'
}

$User = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$PsExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$PsArgs = '-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}"' -f $WatchScript
$PsArgs += ' -PollSeconds {0} -ChargePercent {1} -RuntimeMinutes {2} -ShutdownDelayS {3}' -f `
    $PollSeconds, $ChargePercent, $RuntimeMinutes, $ShutdownDelayS
if ($NoShutdown) { $PsArgs += ' -NoShutdown' }
if ($DryRun) { $PsArgs += ' -DryRun' }

if ($ViaConhost) {
    $action = New-ScheduledTaskAction -Execute (Join-Path $env:SystemRoot 'System32\conhost.exe') `
        -Argument ('"{0}" {1}' -f $PsExe, $PsArgs) -WorkingDirectory $Root
} else {
    $action = New-ScheduledTaskAction -Execute $PsExe -Argument $PsArgs -WorkingDirectory $Root
}
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $User
$trigger.Delay = 'PT15S'
$settings = New-ScheduledTaskSettingsSet `
    -RestartInterval (New-TimeSpan -Minutes 1) -RestartCount 999 `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -Priority 4
# Interactive = "Run only when user is logged on": the Ctrl+C helper needs the engine user's session.
$principal = New-ScheduledTaskPrincipal -UserId $User -LogonType Interactive -RunLevel Limited

# A watcher that is already running keeps the arguments it was started with: Register-ScheduledTask -Force only
# replaces the definition, and IgnoreNew would also ignore -StartNow while it runs. So stop it first (review round 2
# of the home-server work: the runbook's dry run, then the re-install without -DryRun, left the dry-run watcher
# running, and the real plug-pull test was watched by a copy that only logs). It is started again below with the
# new definition, so a re-install never leaves the PC without a watcher until the next log-on.
$wasRunning = $false
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $existing -and $existing.State -eq 'Running') {
    $wasRunning = $true
    Stop-ScheduledTask -TaskName $TaskName
    $deadline = (Get-Date).AddSeconds(15)
    while ((Get-ScheduledTask -TaskName $TaskName).State -eq 'Running' -and (Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 500
    }
    if ((Get-ScheduledTask -TaskName $TaskName).State -eq 'Running') {
        throw "the running '$TaskName' did not stop within 15 s; end it in Task Scheduler (End), then run this again."
    }
    Write-Host "stopped the running '$TaskName' (it kept its old arguments); the new definition starts below."
}

$task = Register-ScheduledTask -TaskName $TaskName -Force `
    -Description 'XTNL UPS watcher (home-server plan W12): deploy\local\ups-watch.ps1 drains the engine (stop-xtnl.ps1 -Containers) and shuts Windows down before the UPS battery runs out. Never sends orders. Runbook docs\runbooks\single-pc-autostart.md.' `
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
Write-Host ("  batteries now reported by Win32_Battery: {0}" -f $batteries.Count)
Write-Host '  logs:        var\logs\xtnl-<date>.log, lines [ups-watch]; status: deploy\local\status-xtnl.ps1'
if ($DryRun) { Write-Host '  DRY RUN: the watcher only logs what it would do. Re-install without -DryRun after the test (that stops this copy).' }
if ($StartNow -or $wasRunning) {
    Start-ScheduledTask -TaskName $TaskName
    $why = if ($StartNow) { '-StartNow' } else { 'it was running before the re-install' }
    Write-Host "started '$TaskName' now ($why)."
    Write-Host "  check its mode: the newest 'ups-watch started' line in var\logs\xtnl-<date>.log must say $(if ($DryRun) { 'DRY RUN' } elseif ($NoShutdown) { '-NoShutdown' } else { 'shutdown /s' })."
} else {
    Write-Host "not started now: it runs at your next log-on, or: Start-ScheduledTask -TaskName '$TaskName'"
}
