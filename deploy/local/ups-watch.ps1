<#
.SYNOPSIS
  Fallback UPS watcher (home-server plan W12): before the UPS battery runs out, drain the engine with
  stop-xtnl.ps1 -Containers and shut Windows down, so a stopped engine never sits on a running box.

.DESCRIPTION
  The preferred path is PowerPanel Personal itself (runtime-threshold shutdown, "run command before shutdown"
  pointing at stop-xtnl.ps1, UPS output off after the shutdown; runbook docs/runbooks/single-pc-autostart.md
  "UPS: clean stop before the battery runs out"). This script is the fallback, for a UPS or a PowerPanel version
  without the command option, or one that runs the command outside the engine user's session.

  It must run in the engine user's (xtnl) session, which is why deploy\local\install-ups-watch.ps1 registers it
  AtLogOn, Interactive: stop-xtnl.ps1 drains the engine through send-ctrl-c.ps1, which attaches to start-xtnl.ps1's
  hidden console, and a console can only be attached from the same session. From any other session the helper
  fails and stop-xtnl.ps1 ends the engine hard (no drain).

  Every -PollSeconds (30) it reads Win32_Battery (Windows shows a USB UPS as a battery):
    * no battery reported (no UPS, or its USB cable is out): it logs that once and does nothing, checking again;
    * on mains: it does nothing. "On battery" needs both a discharging Win32_Battery (BatteryStatus 1) and a
      power line status that is not Online (GetSystemPowerStatus), so one odd reading never acts on mains power;
    * on battery with EstimatedChargeRemaining <= -ChargePercent (45) or EstimatedRunTime <= -RuntimeMinutes (10):
      it logs an ALERT line, runs stop-xtnl.ps1 -Containers (the engine drains on SIGINT, end_reason SIGINT; the
      containers stop with their data kept; at most -StopBudgetS, 300 s, then it is ended), reads the power again
      and runs `shutdown /s /t -ShutdownDelayS` (60 s). The runtime is the main trigger: the charge a UPS reports
      is often non-linear, so the charge threshold is set high as a backstop. A value of 0 turns that trigger off.
    * mains back before the shutdown was issued: no shutdown; if the engine task was running before this script
      stopped it, `Start-ScheduledTask 'XTNL engine'` starts it again (its compose up brings the containers back).
      After a battery reading, mains counts as back only after two mains readings in a row ($AbortAfterReadings),
      5 s apart, as in the countdown below (in the main loop and in the re-check right after the stop): one odd
      reading must not restart a stopped engine only for the next poll to drain it again out of the remaining
      battery runtime.
    * mains back during the -ShutdownDelayS countdown: it keeps reading the power every 5 s while the shutdown is
      pending; two readings in a row on mains run `shutdown /a` (abort), log it, and go on as above (the engine
      task starts again), so a short cut never leaves the box off. Once the countdown has passed this script exits;
      the box then stays off until the BIOS RTC wake, Wake-on-LAN or a power cycle, because Windows cannot tell the
      UPS to cut its output (PowerPanel can: path A).
    * -NoShutdown: it only drains the engine at the threshold and leaves the shutdown (and the UPS output off) to
      PowerPanel, whose own threshold must be lower; on mains return it starts the engine task again as above.
    * -DryRun: it logs what it would do, once per battery episode, and does nothing (no stop, no shutdown, no
      task start). -Once: one reading, then it exits (with -DryRun: a safe check of what the UPS reports).
  Battery charge and runtime are UPS readings, not account values; nothing else is logged.

  Log: var\logs\xtnl-<yyyy-MM-dd>.log, the engine's daily log, lines prefixed [ups-watch] (and [stop-xtnl] for
  stop-xtnl.ps1's output), so status-xtnl.ps1 lists its ALERT: lines with the others. One copy runs at a time
  (named mutex Global\XTNL-ups-watch; a -DryRun copy skips it, since it never acts).
  It changes no Windows, power or UPS setting. Phase 1 never sends orders: this touches only local processes.

  Exit codes: 0 shutdown issued, -Once done, or another copy is already running; 78 configuration (a parameter out
  of range, or stop-xtnl.ps1 / shutdown.exe missing). Otherwise it runs until the session ends.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\ups-watch.ps1 -DryRun -Once
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\ups-watch.ps1 -RuntimeMinutes 15 -NoShutdown
#>
param(
    [int]$PollSeconds = 30,
    [int]$ChargePercent = 45,
    [int]$RuntimeMinutes = 10,
    [int]$ShutdownDelayS = 60,
    [int]$StopBudgetS = 300,
    [switch]$NoShutdown,
    [switch]$DryRun,
    [switch]$Once
)
$ErrorActionPreference = 'Stop'

$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$LogDir = Join-Path $Root 'var\logs'
$PidFile = Join-Path $LogDir 'start-xtnl.pid'
$StopScript = Join-Path $PSScriptRoot 'stop-xtnl.ps1'
$PsExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$ShutdownExe = Join-Path $env:SystemRoot 'System32\shutdown.exe'
$TaskName = 'XTNL engine'
$MutexName = 'Global\XTNL-ups-watch'
# Win32_Battery.BatteryStatus 1 = "Other", which Windows reports for a discharging battery (2 = on mains).
$Discharging = 1
# Win32_Battery.EstimatedRunTime 71582788 (0x4444444) means "unknown" (Windows reports it while on mains).
$RunTimeUnknown = 71582788
# shutdown.exe exit codes meaning a shutdown is already under way: ERROR_SHUTDOWN_IN_PROGRESS (1115),
# ERROR_SHUTDOWN_IS_SCHEDULED (1190).
$ShutdownAlready = @(1115, 1190)
# During the shutdown countdown the power is read this often, and this many readings in a row on mains abort it
# (`shutdown /a`): one odd reading must not keep a box with a flat UPS running.
$CountdownPollS = 5
$AbortAfterReadings = 2
# The countdown is watched until this many seconds before its end: an abort that late could race the shutdown.
$CountdownMarginS = 3
# While on battery, the readings are logged at most this often (and at every change of state).
$BatteryLogEveryS = 300
$ShutdownComment = 'XTNL ups-watch: UPS battery low; the engine was stopped first (stop-xtnl.ps1 -Containers).'

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

# ------------------------------------------------------------------------------------------------ logging
function Write-Log([string]$Message) {
    $line = '{0} [ups-watch] {1}' -f (Get-Date).ToString('yyyy-MM-dd"T"HH:mm:ss.fffzzz'), $Message
    $path = Join-Path $LogDir ('xtnl-{0}.log' -f (Get-Date).ToString('yyyy-MM-dd'))
    # start-xtnl.ps1 appends to the same file: retry briefly when it holds it.
    for ($i = 0; $i -lt 5; $i++) {
        try { [System.IO.File]::AppendAllText($path, $line + "`r`n", $Utf8NoBom); break }
        catch { Start-Sleep -Milliseconds 50 }
    }
    try { [Console]::Out.WriteLine($line) } catch { }
}

# ------------------------------------------------------------------------------------------------ power readings
$script:FormsOk = $false
try { Add-Type -AssemblyName System.Windows.Forms; $script:FormsOk = $true } catch { }

# 'Online', 'Offline' or 'Unknown' (GetSystemPowerStatus through SystemInformation.PowerStatus).
function Get-AcLine {
    if (-not $script:FormsOk) { return 'Unknown' }
    try { return [string][System.Windows.Forms.SystemInformation]::PowerStatus.PowerLineStatus } catch { return 'Unknown' }
}

function Get-Power {
    $r = @{ Ok = $true; Present = $false; OnBattery = $false; Charge = $null; RuntimeMin = $null
            AcLine = 'Unknown'; Error = '' }
    try { $bats = @(Get-CimInstance -ClassName Win32_Battery -ErrorAction Stop) }
    catch { $r.Ok = $false; $r.Error = $_.Exception.GetType().Name; return $r }
    $r.AcLine = Get-AcLine
    if ($bats.Count -eq 0) { return $r }
    $r.Present = $true
    $discharging = $false
    foreach ($b in $bats) {
        if ($null -ne $b.BatteryStatus -and [int]$b.BatteryStatus -eq $Discharging) { $discharging = $true }
        if ($null -ne $b.EstimatedChargeRemaining) {
            $c = [int]$b.EstimatedChargeRemaining
            if ($null -eq $r.Charge -or $c -lt $r.Charge) { $r.Charge = $c }
        }
        if ($null -ne $b.EstimatedRunTime) {
            $m = [long]$b.EstimatedRunTime
            if ($m -lt $RunTimeUnknown -and ($null -eq $r.RuntimeMin -or $m -lt $r.RuntimeMin)) { $r.RuntimeMin = [int]$m }
        }
    }
    # Both sources must say "battery": a single odd reading must never shut the box down on mains power.
    $r.OnBattery = $discharging -and ($r.AcLine -ne 'Online')
    return $r
}

function Format-Power($P) {
    $charge = 'unknown'
    if ($null -ne $P.Charge) { $charge = '{0} %' -f $P.Charge }
    $runtime = 'unknown'
    if ($null -ne $P.RuntimeMin) { $runtime = '{0} min' -f $P.RuntimeMin }
    return ('charge {0}, runtime {1}, power line {2}' -f $charge, $runtime, $P.AcLine)
}

# A usable reading that says mains (both power sources agree it is not on battery).
function Test-Mains($P) {
    return [bool]($P.Ok -and $P.Present -and -not $P.OnBattery)
}

# $P is one mains reading taken while the last state was battery. Reads the power again, $CountdownPollS apart,
# until $AbortAfterReadings readings in a row said mains (the countdown's rule): returns the last reading when all
# of them did, else the first one that did not, so the caller can tell a glitch from mains really coming back.
function Confirm-Mains($P) {
    $q = $P
    for ($i = 1; $i -lt $AbortAfterReadings; $i++) {
        Start-Sleep -Seconds $CountdownPollS
        $q = Get-Power
        if (-not (Test-Mains $q)) { return $q }
    }
    return $q
}

function Test-Low($P) {
    if ($ChargePercent -gt 0 -and $null -ne $P.Charge -and $P.Charge -le $ChargePercent) { return $true }
    if ($RuntimeMinutes -gt 0 -and $null -ne $P.RuntimeMin -and $P.RuntimeMin -le $RuntimeMinutes) { return $true }
    return $false
}

# ------------------------------------------------------------------------------------------------ actions
# Same test as stop-xtnl.ps1: a live PowerShell running start-xtnl.ps1 under the pid in var\logs\start-xtnl.pid.
function Test-EngineRunning {
    if (-not (Test-Path -LiteralPath $PidFile)) { return $false }
    $raw = (Get-Content -LiteralPath $PidFile -TotalCount 1)
    $id = 0
    if (-not [int]::TryParse(([string]$raw).Trim(), [ref]$id)) { return $false }
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $id" -ErrorAction SilentlyContinue
    if ($null -eq $proc -or $proc.Name -ne 'powershell.exe' -or $proc.CommandLine -notmatch 'start-xtnl\.ps1') { return $false }
    return $true
}

# Runs stop-xtnl.ps1 -Containers in this session (so its Ctrl+C helper can reach start-xtnl.ps1's console) and
# copies its output into the log. Returns its exit code, or -1 when it did not finish within -StopBudgetS.
function Invoke-StopXtnl {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $PsExe
    $psi.Arguments = ('-NoProfile -ExecutionPolicy Bypass -File "{0}" -Containers' -f $StopScript)
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.StandardOutputEncoding = $Utf8NoBom
    $psi.StandardErrorEncoding = $Utf8NoBom
    $psi.WorkingDirectory = $Root
    $p = [System.Diagnostics.Process]::Start($psi)
    $tOut = $p.StandardOutput.ReadToEndAsync()
    $tErr = $p.StandardError.ReadToEndAsync()
    $code = -1
    if ($p.WaitForExit($StopBudgetS * 1000)) {
        $p.WaitForExit()
        $code = $p.ExitCode
    } else {
        try { & taskkill.exe /PID $p.Id /T /F 2>$null | Out-Null } catch { }
        Write-Log ("ALERT: stop-xtnl.ps1 did not finish within {0} s; it was ended, going on to the shutdown" -f $StopBudgetS)
    }
    foreach ($t in @($tOut, $tErr)) {
        # A grandchild still holding the pipe must not hang the watcher: wait a little, then move on.
        if (-not $t.Wait(5000)) { continue }
        foreach ($line in ($t.Result -split "\r?\n")) {
            if ($line.Trim() -ne '') { Write-Log ('[stop-xtnl] ' + $line) }
        }
    }
    return $code
}

function Invoke-Shutdown {
    $shutdownArgs = @('/s', '/t', [string]$ShutdownDelayS, '/c', ('"{0}"' -f $ShutdownComment))
    $p = Start-Process -FilePath $ShutdownExe -ArgumentList $shutdownArgs -WindowStyle Hidden -Wait -PassThru
    return [int]$p.ExitCode
}

function Invoke-ShutdownAbort {
    $p = Start-Process -FilePath $ShutdownExe -ArgumentList @('/a') -WindowStyle Hidden -Wait -PassThru
    return [int]$p.ExitCode
}

# After `shutdown /s /t N`: keep reading the power while the countdown runs. Returns $true when mains came back and
# the shutdown was aborted (the caller goes on watching), $false when the countdown ran out (the caller exits).
function Wait-ShutdownCountdown {
    $end = (Get-Date).AddSeconds($ShutdownDelayS - $CountdownMarginS)
    $onMains = 0
    while ((Get-Date) -lt $end) {
        Start-Sleep -Seconds $CountdownPollS
        $p = Get-Power
        if ($p.Ok -and $p.Present -and -not $p.OnBattery) { $onMains++ } else { $onMains = 0 }
        if ($onMains -lt $AbortAfterReadings) { continue }
        $code = Invoke-ShutdownAbort
        if ($code -eq 0) {
            Write-Log ('mains power back during the shutdown countdown ({0}): shutdown aborted (shutdown /a)' -f (Format-Power $p))
            Step-OnMains $p
            return $true
        }
        Write-Log ('ALERT: mains power back but shutdown /a failed (exit {0}); the box shuts down: it comes back with the BIOS RTC wake, Wake-on-LAN or a power cycle' -f $code)
        return $false
    }
    return $false
}

function Start-EngineTask {
    try {
        $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
        if ($task.State -eq 'Disabled') { Write-Log "the '$TaskName' task is disabled: not started; start the engine yourself"; return }
        Start-ScheduledTask -TaskName $TaskName
        Write-Log "started the '$TaskName' task again (its compose up brings the containers back)"
    } catch {
        Write-Log ("ALERT: could not start the '{0}' task ({1}); start it yourself: Start-ScheduledTask -TaskName '{0}'" -f $TaskName, $_.Exception.GetType().Name)
    }
}

# ------------------------------------------------------------------------------------------------ state machine
$script:S = @{ Mode = 'unknown'; NoBatteryLogged = $false; ErrorLogged = $false; NoReadingLogged = $false
               Stopped = $false; WasRunning = $false; DryRunLogged = $false; NoShutdownLogged = $false
               LastBatteryLog = [DateTime]::MinValue }

function Step-OnMains($P) {
    if ($script:S.Mode -eq 'battery') { Write-Log ('mains power back (' + (Format-Power $P) + ')') }
    if ($script:S.Mode -ne 'mains' -and $script:S.Mode -ne 'battery') { Write-Log ('on mains power (' + (Format-Power $P) + ')') }
    if ($script:S.Stopped) {
        if ($script:S.WasRunning) { Start-EngineTask }
        else { Write-Log 'the engine was not running before the stop: not starting it' }
    }
    $script:S.Mode = 'mains'
    $script:S.Stopped = $false
    $script:S.WasRunning = $false
    $script:S.DryRunLogged = $false
    $script:S.NoShutdownLogged = $false
    $script:S.NoReadingLogged = $false
}

# Returns $true when the shutdown has been issued and its countdown ran out (the caller exits).
function Step-OnBattery($P) {
    $now = Get-Date
    if ($script:S.Mode -ne 'battery') {
        Write-Log ('on battery: {0}; acts at charge <= {1} % or runtime <= {2} min (0 = off){3}' -f (Format-Power $P),
                   $ChargePercent, $RuntimeMinutes, $(if ($DryRun) { ' [dry run]' } else { '' }))
        $script:S.Mode = 'battery'
        $script:S.LastBatteryLog = $now
    } elseif (($now - $script:S.LastBatteryLog).TotalSeconds -ge $BatteryLogEveryS) {
        Write-Log ('still on battery: ' + (Format-Power $P))
        $script:S.LastBatteryLog = $now
    }
    if ($null -eq $P.Charge -and $null -eq $P.RuntimeMin -and -not $script:S.NoReadingLogged) {
        Write-Log 'ALERT: on battery but the UPS reports neither charge nor runtime: this watcher cannot act; PowerPanel or the BIOS settings must'
        $script:S.NoReadingLogged = $true
    }
    if (-not (Test-Low $P)) { return $false }

    if ($DryRun) {
        if (-not $script:S.DryRunLogged) {
            $what = 'then shutdown /s /t {0}' -f $ShutdownDelayS
            if ($NoShutdown) { $what = 'and leave the shutdown to PowerPanel (-NoShutdown)' }
            Write-Log ('DRY RUN: threshold reached ({0}): would run stop-xtnl.ps1 -Containers, {1}' -f (Format-Power $P), $what)
            $script:S.DryRunLogged = $true
        }
        return $false
    }

    if (-not $script:S.Stopped) {
        $script:S.WasRunning = Test-EngineRunning
        Write-Log ('ALERT: UPS battery low ({0}): running stop-xtnl.ps1 -Containers{1}' -f (Format-Power $P),
                   $(if ($NoShutdown) { '' } else { (', then shutdown /s /t {0}' -f $ShutdownDelayS) }))
        $code = Invoke-StopXtnl
        Write-Log ('stop-xtnl.ps1 ended (exit {0})' -f $code)
        $script:S.Stopped = $true
        # The stop takes up to a few minutes: never shut down if mains came back meanwhile.
        $again = Get-Power
        if (Test-Mains $again) {
            $again = Confirm-Mains $again
            if (Test-Mains $again) {
                Write-Log 'mains power returned while stopping: no shutdown'
                Step-OnMains $again
                return $false
            }
            Write-Log ('one mains reading after the stop was not repeated ({0}): still on battery, going on' -f (Format-Power $again))
        }
    }
    if ($NoShutdown) {
        if (-not $script:S.NoShutdownLogged) {
            Write-Log 'engine stopped; -NoShutdown: the shutdown and the UPS output off are left to PowerPanel'
            $script:S.NoShutdownLogged = $true
        }
        return $false
    }
    $code = Invoke-Shutdown
    if ($code -eq 0 -or $ShutdownAlready -contains $code) {
        Write-Log ('shutdown /s /t {0} issued (exit {1}); watching for mains during the countdown' -f $ShutdownDelayS, $code)
        if (Wait-ShutdownCountdown) { return $false }
        Write-Log 'shutdown countdown ran out; ups-watch exits'
        return $true
    }
    Write-Log ('ALERT: shutdown /s failed (exit {0}); trying again in {1} s' -f $code, $PollSeconds)
    return $false
}

# ------------------------------------------------------------------------------------------------ main
if (-not (Test-Path -LiteralPath $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

$problems = @()
if ($PollSeconds -lt 5 -or $PollSeconds -gt 600) { $problems += '-PollSeconds must be 5..600' }
if ($ChargePercent -lt 0 -or $ChargePercent -gt 99) { $problems += '-ChargePercent must be 0..99 (0 = off)' }
if ($RuntimeMinutes -lt 0 -or $RuntimeMinutes -gt 240) { $problems += '-RuntimeMinutes must be 0..240 (0 = off)' }
if ($ChargePercent -eq 0 -and $RuntimeMinutes -eq 0) { $problems += 'at least one of -ChargePercent / -RuntimeMinutes must be above 0' }
if ($ShutdownDelayS -lt 0 -or $ShutdownDelayS -gt 600) { $problems += '-ShutdownDelayS must be 0..600' }
if ($StopBudgetS -lt 30 -or $StopBudgetS -gt 1800) { $problems += '-StopBudgetS must be 30..1800' }
if (-not (Test-Path -LiteralPath $StopScript)) { $problems += "not found: $StopScript" }
if (-not (Test-Path -LiteralPath $ShutdownExe)) { $problems += "not found: $ShutdownExe" }
if ($problems.Count -gt 0) {
    Write-Log ('refused (exit 78): ' + ($problems -join '; '))
    exit 78
}

$mutex = $null
if (-not $DryRun) {
    $mutex = New-Object System.Threading.Mutex($false, $MutexName)
    $owned = $false
    try { $owned = $mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $owned = $true }
    if (-not $owned) {
        Write-Log 'another ups-watch.ps1 is already running; exiting'
        exit 0
    }
}

$mode = 'stop-xtnl.ps1 -Containers, then shutdown /s /t {0}' -f $ShutdownDelayS
if ($NoShutdown) { $mode = 'stop-xtnl.ps1 -Containers only (-NoShutdown: PowerPanel shuts down)' }
if ($DryRun) { $mode = 'DRY RUN, no action (would: ' + $mode + ')' }
$started = 'ups-watch started (pid {0}): every {1} s; acts on battery at charge <= {2} % or runtime <= {3} min (0 = off); {4}'
Write-Log ($started -f $PID, $PollSeconds, $ChargePercent, $RuntimeMinutes, $mode)

while ($true) {
    $p = Get-Power
    if (-not $p.Ok) {
        if (-not $script:S.ErrorLogged) {
            Write-Log ('Win32_Battery not readable ({0}): no action; checking again every {1} s' -f $p.Error, $PollSeconds)
            $script:S.ErrorLogged = $true
        }
    } else {
        if ($script:S.ErrorLogged) { Write-Log 'Win32_Battery readable again'; $script:S.ErrorLogged = $false }
        if (-not $p.Present) {
            if (-not $script:S.NoBatteryLogged) {
                Write-Log ('no battery reported by Win32_Battery (no UPS, or its USB cable is out): nothing to watch, no action; checking again every {0} s' -f $PollSeconds)
                $script:S.NoBatteryLogged = $true
            }
        } else {
            if ($script:S.NoBatteryLogged) { Write-Log 'a battery is reported now: watching it'; $script:S.NoBatteryLogged = $false }
            $battery = $p
            if (-not $p.OnBattery) {
                $battery = $null
                $mains = $p
                # After battery, one mains reading is not enough (see Confirm-Mains).
                if ($script:S.Mode -eq 'battery') { $mains = Confirm-Mains $p }
                if (Test-Mains $mains) {
                    Step-OnMains $mains
                } else {
                    Write-Log ('one mains reading on battery was not repeated ({0}): still on battery' -f (Format-Power $mains))
                    if ($mains.Ok -and $mains.Present -and $mains.OnBattery) { $battery = $mains }
                }
            }
            if ($null -ne $battery) {
                if (Step-OnBattery $battery) {
                    if ($null -ne $mutex) { $mutex.ReleaseMutex() }
                    exit 0
                }
            }
        }
    }
    if ($Once) { break }
    Start-Sleep -Seconds $PollSeconds
}
if ($null -ne $mutex) { $mutex.ReleaseMutex() }
exit 0
