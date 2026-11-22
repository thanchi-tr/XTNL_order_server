<#
.SYNOPSIS
  Give the live view's read-only database role (xtnl_liveview, migration 0021) a login password and store its DSN
  in the Windows User environment as XTNL_LIVEVIEW_DSN.

.DESCRIPTION
  The live view (python -m src.liveview, docs/runbooks/live-view.md) is a local read-only page on 127.0.0.1:8090:
  it never sends orders and has no write path. It reads PostgreSQL only as role xtnl_liveview, which migration 0021
  creates NOLOGIN with SELECT on its lv_ views only. This script changes no firewall, router or other Windows
  setting; the owner runs it (never an automated job).

  Runs from any folder (it switches to the repository root itself).

  1. Checks that role xtnl_liveview exists in the local stack's postgres container (migration 0021 applied:
     XTNL_MIGRATIONS_ACCEPTED_UPTO raised to 21 in deploy\env\local.env, then a start-xtnl.ps1 run).
  2. Makes a random 32-byte password (hex) and runs ALTER ROLE xtnl_liveview LOGIN PASSWORD inside the container
     (docker exec ... psql, the statement sent on stdin, never on a command line).
  3. Stores postgresql://xtnl_liveview:<password>@127.0.0.1:55433/xtnl ONLY in the Windows User environment as
     XTNL_LIVEVIEW_DSN (start-xtnl.ps1 copies it into the live view process only). The password and the DSN are
     never printed and never written to a file.

  An existing XTNL_LIVEVIEW_DSN is kept unless -Rotate is given (then the role gets a new password). The Windows
  User environment is read once when start-xtnl.ps1 starts, so after creating or rotating restart the engine task
  (stop-xtnl.ps1, then Start-ScheduledTask -TaskName 'XTNL engine'); until then a running live view whose
  password was rotated has its next login refused and stops (exit 78).

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-liveview-credentials.ps1
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-liveview-credentials.ps1 -Rotate
#>
param(
    [switch]$Rotate
)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Set-Location -LiteralPath $Root
$DsnName = 'XTNL_LIVEVIEW_DSN'
$RoleName = 'xtnl_liveview'
$ComposeFile = Join-Path $Root 'deploy\local\docker-compose.local.yml'
$PgHostPort = '127.0.0.1:55433'

function Find-Docker {
    $cmd = Get-Command 'docker.exe' -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $fallback = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'
    if (Test-Path -LiteralPath $fallback) { return $fallback }
    return $null
}

# Gives role xtnl_liveview (migration 0021) a random password and stores its DSN in the Windows User environment.
# Returns $true when stored. The password goes to psql on stdin only, and is never shown or written to a file.
function New-LiveViewCredential {
    $docker = Find-Docker
    if (-not $docker) { Write-Host "docker.exe not found: $DsnName not set (start Docker Desktop, then run this script again)"; return $false }
    $cid = @(& $docker compose -f $ComposeFile ps -q postgres 2>$null) | Where-Object { $_ } | Select-Object -First 1
    if (-not $cid) { Write-Host "the local stack's postgres is not running: $DsnName not set (start-xtnl.ps1 starts it; then run this script again)"; return $false }
    $exists = @(& $docker exec $cid psql -U xtnl -d xtnl -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_liveview'" 2>$null) -join ''
    if ($exists.Trim() -ne '1') {
        Write-Host "role $RoleName not found: apply migration 0021 first (set XTNL_MIGRATIONS_ACCEPTED_UPTO=21 in deploy\env\local.env, restart start-xtnl.ps1), then run this script again"
        return $false
    }
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    $pw = -join ($bytes | ForEach-Object { $_.ToString('x2') })
    [Array]::Clear($bytes, 0, $bytes.Length)
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $docker
    $psi.Arguments = "exec -i $cid psql -v ON_ERROR_STOP=1 -q -U xtnl -d xtnl"
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardInput = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $proc = [System.Diagnostics.Process]::Start($psi)
    $errTask = $proc.StandardError.ReadToEndAsync()
    $proc.StandardInput.WriteLine("ALTER ROLE xtnl_liveview LOGIN PASSWORD '" + $pw + "';")
    $proc.StandardInput.Close()
    [void]$proc.StandardOutput.ReadToEnd()
    $proc.WaitForExit()
    [void]$errTask.Result
    if ($proc.ExitCode -ne 0) { $pw = $null; Write-Host "psql failed (exit $($proc.ExitCode)): $DsnName not set"; return $false }
    $dsn = 'postgresql://xtnl_liveview:' + $pw + '@' + $PgHostPort + '/xtnl'
    [Environment]::SetEnvironmentVariable($DsnName, $dsn, 'User')
    $pw = $null; $dsn = $null
    Write-Host "$DsnName stored in the Windows User environment (role $RoleName, SELECT on the lv_ views only; password not shown)"
    return $true
}

# ------------------------------------------------------------------------------------------------ main
Write-Host 'XTNL live view credentials (read-only live view: it never sends orders and has no write path)'
$haveDsn = -not [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($DsnName, 'User'))
$stored = $false
if ($haveDsn -and -not $Rotate) {
    Write-Host "$DsnName kept (already set in the Windows User environment; use -Rotate to give $RoleName a new password)"
} else {
    $stored = [bool](New-LiveViewCredential)
}

Write-Host ''
if ($stored -and $Rotate) {
    Write-Host 'Password rotated. Restart the engine task now: the live view reads XTNL_LIVEVIEW_DSN only when start-xtnl.ps1 starts,'
    Write-Host 'and a running live view has its next login refused (exit 78) until the restart:'
} else {
    Write-Host 'Restart the engine task so the live view starts with this login (start-xtnl.ps1 reads the Windows User environment once):'
}
Write-Host '  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1'
Write-Host "  Start-ScheduledTask -TaskName 'XTNL engine'"
Write-Host 'Then open http://127.0.0.1:8090/ (runbook: docs\runbooks\live-view.md)'
