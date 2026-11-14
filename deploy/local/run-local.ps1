<#
.SYNOPSIS
  Run the XTNL engine on this machine against the throwaway containers (Stage 0, development only).

.DESCRIPTION
  Loads deploy/env/local.env (or local.env.example when local.env is missing) into this process's
  environment, optionally applies the migrations, then starts `python -m src.main`. Ctrl+C drains and exits.
  Phase 1 never sends orders. Start the containers first:
      docker compose -f deploy/docker-compose.dev.yml up -d --wait

  Single-PC mode (.claude/rules/operations.md s17, docs/runbooks/downtime-catchup.md): the engine is restarted
  after a hard exit (exit codes 70-79: fence, LEADER_INIT / DRAINING deadline, critical task, watchdog), which
  is what happens when the PC slept longer than the lease. Each restart records a new engine_runs row and runs a
  start-up catch-up from the last recorded coverage. A clean stop (Ctrl+C) or a configuration error ends the
  script. -NoRestart runs the engine once.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File deploy/local/run-local.ps1 -Migrate
#>
param(
    [switch]$Migrate,
    [switch]$CheckOnly,
    [string]$EnvFile,
    [switch]$NoRestart
)
$ErrorActionPreference = 'Stop'
$root = Resolve-Path (Join-Path $PSScriptRoot '..\..')
Set-Location $root
if (-not $EnvFile) {
    $EnvFile = Join-Path $root 'deploy\env\local.env'
    if (-not (Test-Path $EnvFile)) { $EnvFile = Join-Path $root 'deploy\env\local.env.example' }
}
Write-Host "env file: $EnvFile"
foreach ($line in Get-Content $EnvFile) {
    $t = $line.Trim()
    if ($t -eq '' -or $t.StartsWith('#')) { continue }
    $i = $t.IndexOf('=')
    if ($i -lt 1) { continue }
    [Environment]::SetEnvironmentVariable($t.Substring(0, $i).Trim(), $t.Substring($i + 1).Trim(), 'Process')
}
$py = Join-Path $root '.venv\Scripts\python.exe'
if ($Migrate) { & $py -m src.db.migrate; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } }
& $py -m src.main --check-config
if ($LASTEXITCODE -ne 0 -or $CheckOnly) { exit $LASTEXITCODE }
$delay = 5
while ($true) {
    $started = Get-Date
    & $py -m src.main
    $code = $LASTEXITCODE
    # 70-79 = src/runtime/exits.py EXIT_CODES (hard exits); anything else is a clean stop or a refusal.
    if ($NoRestart -or $code -lt 70 -or $code -gt 79) { exit $code }
    if (((Get-Date) - $started).TotalSeconds -ge 600) { $delay = 5 }
    Write-Host "engine hard exit (code $code); restarting in $delay s (its start-up catch-up re-reads what changed meanwhile). Ctrl+C stops."
    Start-Sleep -Seconds $delay
    $delay = [Math]::Min($delay * 2, 120)
}
