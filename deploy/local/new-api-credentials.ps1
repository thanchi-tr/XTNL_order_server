<#
.SYNOPSIS
  Create the order-server API credentials: a self-signed TLS certificate and the shared HMAC secret.

.DESCRIPTION
  The order-server API (python -m src.api, docs/runbooks/order-server-api.md) is read-only: Phase 1 never sends
  orders. This script prepares what it and the XTNL thesis website (Vercel) need; it changes no firewall, router
  or other Windows setting.

  Runs from any folder (it switches to the repository root itself).

  1. TLS: server.key + server.crt in -OutDir, else at XTNL_API_TLS_CERT / XTNL_API_TLS_KEY of -EnvFile (default
     deploy\env\local.env: the files the API loads), else var\api-tls\ (EC P-256, -Days days, CN xtnl-order-server, subjectAltName =
     127.0.0.1 plus every -Hosts entry), made with OpenSSL (Git for Windows ships openssl.exe under
     C:\Program Files\Git\usr\bin; found automatically). The key file is readable by the current user only
     (one access rule, inheritance removed). var\ is git-ignored.
  2. Secret: 32 random bytes as 64 hex characters, stored ONLY in the Windows User environment as
     XTNL_API_SECRET (start-xtnl.ps1 copies it into the API process). Never printed, never written to a file.
  3. Database role (least privilege): the API reads PostgreSQL as role xtnl_api (migration 0014: SELECT on the
     three tables the compile reads, read-only sessions), never as the engine's role. This step gives the role a
     random 32-byte password inside the local stack's postgres container (docker exec ... psql, the statement
     sent on stdin, never on a command line) and stores the DSN ONLY in the Windows User environment as
     XTNL_API_DSN (start-xtnl.ps1 copies it into the API process). Never printed, never written to a file.
     Needs the local stack running and the migrations applied (start-xtnl.ps1 steps 5-6); otherwise it says so
     and the rest still runs. Skipped when XTNL_API_DSN is already set, unless -DbRole or -Rotate.
  4. Prints the certificate's SHA-256 fingerprint (not secret) and the values to enter in Vercel.

  Existing credentials are kept unless -Rotate is given (then both are replaced; -CertOnly limits it to the
  certificate). -CopySecret copies XTNL_API_SECRET to the clipboard so it can be pasted into Vercel's
  ORDER_SERVER_API_SECRET; clear the clipboard afterwards. After creating or rotating, restart the engine task
  (stop-xtnl.ps1, then Start-ScheduledTask -TaskName 'XTNL engine') so the API picks them up, and update Vercel.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-api-credentials.ps1 -Hosts 203.0.113.10
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-api-credentials.ps1 -CopySecret
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-api-credentials.ps1 -DbRole
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-api-credentials.ps1 -Rotate -Hosts xtnl.example.net
#>
param(
    [string[]]$Hosts = @(),
    [switch]$Rotate,
    [switch]$CertOnly,
    [switch]$CopySecret,
    [switch]$DbRole,
    [int]$Days = 825,
    [int]$Port = 47443,
    [string]$OutDir,
    [string]$EnvFile
)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Set-Location -LiteralPath $Root
# Without -OutDir the files are the ones the API actually loads: XTNL_API_TLS_CERT / XTNL_API_TLS_KEY from
# -EnvFile (default deploy\env\local.env; relative paths are from the repository root), else var\api-tls. So
# running this from any folder never mints a second certificate the running API does not use.
if (-not $EnvFile) { $EnvFile = Join-Path $Root 'deploy\env\local.env' }
$envTls = @{}
if (Test-Path -LiteralPath $EnvFile) {
    foreach ($line in Get-Content -LiteralPath $EnvFile) {
        if ($line -match '^\s*(XTNL_API_TLS_CERT|XTNL_API_TLS_KEY)\s*=\s*(.+?)\s*$') { $envTls[$Matches[1]] = $Matches[2] }
    }
}
function Resolve-RepoPath([string]$P) { if ([System.IO.Path]::IsPathRooted($P)) { $P } else { Join-Path $Root $P } }
if ($OutDir) {
    $Crt = Join-Path $OutDir 'server.crt'
    $Key = Join-Path $OutDir 'server.key'
} elseif ($envTls.ContainsKey('XTNL_API_TLS_CERT') -and $envTls.ContainsKey('XTNL_API_TLS_KEY')) {
    $Crt = Resolve-RepoPath $envTls['XTNL_API_TLS_CERT']
    $Key = Resolve-RepoPath $envTls['XTNL_API_TLS_KEY']
    Write-Host "certificate paths from $EnvFile"
} else {
    $Crt = Join-Path $Root 'var\api-tls\server.crt'
    $Key = Join-Path $Root 'var\api-tls\server.key'
}
$OutDir = Split-Path -Parent $Crt
$SecretName = 'XTNL_API_SECRET'
$DsnName = 'XTNL_API_DSN'
$ComposeFile = Join-Path $Root 'deploy\local\docker-compose.local.yml'
$PgHostPort = '127.0.0.1:55433'

function Find-OpenSsl {
    $cmd = Get-Command 'openssl.exe' -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    foreach ($c in @((Join-Path $env:ProgramFiles 'Git\usr\bin\openssl.exe'),
                     (Join-Path $env:ProgramFiles 'Git\mingw64\bin\openssl.exe'))) {
        if (Test-Path -LiteralPath $c) { return $c }
    }
    return $null
}

# Runs openssl with its output captured (it reports progress on stderr). Returns @(exit code, stdout, stderr).
function Invoke-OpenSsl([string]$Exe, [string[]]$Arguments) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $Exe
    $psi.Arguments = (($Arguments | ForEach-Object { if ($_ -match '[\s"]') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ } }) -join ' ')
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $proc = [System.Diagnostics.Process]::Start($psi)
    $errTask = $proc.StandardError.ReadToEndAsync()
    $out = $proc.StandardOutput.ReadToEnd()
    $proc.WaitForExit()
    return @($proc.ExitCode, $out, $errTask.Result)
}

function Get-SanList {
    $items = New-Object System.Collections.Generic.List[string]
    $items.Add('IP:127.0.0.1')
    foreach ($h in ($Hosts | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
        $ip = $null
        if ([System.Net.IPAddress]::TryParse($h, [ref]$ip)) { $entry = "IP:$h" }
        elseif ($h -match '^(?=.{1,253}$)([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$') { $entry = "DNS:$h" }
        else { throw "not an IP address or DNS name: $h" }
        if (-not $items.Contains($entry)) { $items.Add($entry) }
    }
    return ($items -join ',')
}

function New-Certificate([string]$OpenSsl) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
    $san = Get-SanList
    $tmpKey = Join-Path $OutDir ('.new-{0}.key' -f [Guid]::NewGuid().ToString('N'))
    $tmpCrt = Join-Path $OutDir ('.new-{0}.crt' -f [Guid]::NewGuid().ToString('N'))
    try {
        $r = Invoke-OpenSsl $OpenSsl @('req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes',
            '-keyout', $tmpKey, '-out', $tmpCrt, '-days', [string]$Days, '-subj', '/CN=xtnl-order-server',
            '-addext', "subjectAltName=$san", '-addext', 'basicConstraints=critical,CA:FALSE',
            '-addext', 'keyUsage=critical,digitalSignature', '-addext', 'extendedKeyUsage=serverAuth')
        if ($r[0] -ne 0 -or -not (Test-Path -LiteralPath $tmpCrt) -or -not (Test-Path -LiteralPath $tmpKey)) {
            $tail = (($r[2] -split "`n") | Select-Object -Last 3) -join ' '
            throw ("openssl req failed (exit {0}): {1}" -f $r[0], $tail)
        }
        # the private key: one access rule for the current user, nothing inherited, no other entry (OpenSSL from
        # Git for Windows writes explicit POSIX-style entries for SYSTEM / Administrators / Users; all replaced)
        $me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
        $acl = New-Object System.Security.AccessControl.FileSecurity
        $acl.SetAccessRuleProtection($true, $false)
        $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($me, 'Read, Write, Delete', 'Allow')))
        (Get-Item -LiteralPath $tmpKey).SetAccessControl($acl)
        Move-Item -LiteralPath $tmpKey -Destination $Key -Force
        Move-Item -LiteralPath $tmpCrt -Destination $Crt -Force
    } finally {
        foreach ($t in @($tmpKey, $tmpCrt)) { if (Test-Path -LiteralPath $t) { Remove-Item -LiteralPath $t -Force } }
    }
    Write-Host "certificate written: $Crt (subjectAltName $san, $Days days)"
    Write-Host "private key written: $Key (readable by the current user only)"
}

function New-Secret {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    $text = -join ($bytes | ForEach-Object { $_.ToString('x2') })
    [Array]::Clear($bytes, 0, $bytes.Length)
    [Environment]::SetEnvironmentVariable($SecretName, $text, 'User')
    $text = $null
    Write-Host "$SecretName stored in the Windows User environment (64 hex characters; not shown)"
}

function Find-Docker {
    $cmd = Get-Command 'docker.exe' -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $fallback = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'
    if (Test-Path -LiteralPath $fallback) { return $fallback }
    return $null
}

# Gives role xtnl_api (migration 0014) a random password and stores its DSN in the Windows User environment.
# Returns $true when stored. The password goes to psql on stdin only, and is never shown or written to a file.
function New-DbRoleCredential {
    $docker = Find-Docker
    if (-not $docker) { Write-Host "docker.exe not found: $DsnName not set (start Docker Desktop, then run this script with -DbRole)"; return $false }
    $cid = @(& $docker compose -f $ComposeFile ps -q postgres 2>$null) | Where-Object { $_ } | Select-Object -First 1
    if (-not $cid) { Write-Host "the local stack's postgres is not running: $DsnName not set (start-xtnl.ps1 starts it; then run this script with -DbRole)"; return $false }
    $exists = @(& $docker exec $cid psql -U xtnl -d xtnl -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'xtnl_api'" 2>$null) -join ''
    if ($exists.Trim() -ne '1') { Write-Host "role xtnl_api not found: apply the migrations first (start-xtnl.ps1 step 6, or python -m src.db.migrate), then run this script with -DbRole"; return $false }
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
    $proc.StandardInput.WriteLine("ALTER ROLE xtnl_api LOGIN PASSWORD '" + $pw + "';")
    $proc.StandardInput.Close()
    [void]$proc.StandardOutput.ReadToEnd()
    $proc.WaitForExit()
    [void]$errTask.Result
    if ($proc.ExitCode -ne 0) { $pw = $null; Write-Host "psql failed (exit $($proc.ExitCode)): $DsnName not set"; return $false }
    $dsn = 'postgresql://xtnl_api:' + $pw + '@' + $PgHostPort + '/xtnl'
    [Environment]::SetEnvironmentVariable($DsnName, $dsn, 'User')
    $pw = $null; $dsn = $null
    Write-Host "$DsnName stored in the Windows User environment (role xtnl_api, SELECT only; password not shown)"
    return $true
}

# ------------------------------------------------------------------------------------------------ main
Write-Host 'XTNL order-server API credentials (read-only API: Phase 1 never sends orders)'
$haveCert = (Test-Path -LiteralPath $Crt) -and (Test-Path -LiteralPath $Key)
$haveSecret = -not [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($SecretName, 'User'))
$haveDsn = -not [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($DsnName, 'User'))

$openssl = Find-OpenSsl
if ($haveCert -and -not $Rotate) {
    Write-Host "certificate kept: $Crt (use -Rotate to replace it)"
} else {
    if (-not $openssl) { throw 'openssl.exe not found (install Git for Windows, or put openssl.exe on PATH)' }
    New-Certificate $openssl
}

if ($CertOnly) {
    Write-Host "secret untouched (-CertOnly)"
} elseif ($haveSecret -and -not $Rotate) {
    Write-Host "$SecretName kept (already set in the Windows User environment; use -Rotate to replace it)"
} else {
    New-Secret
}

if ($CertOnly) {
    Write-Host "$DsnName untouched (-CertOnly)"
} elseif ($haveDsn -and -not ($Rotate -or $DbRole)) {
    Write-Host "$DsnName kept (already set in the Windows User environment; use -DbRole to give xtnl_api a new password)"
} else {
    [void](New-DbRoleCredential)
}

if ($CopySecret) {
    $value = [Environment]::GetEnvironmentVariable($SecretName, 'User')
    if ([string]::IsNullOrEmpty($value)) { Write-Host "$SecretName is not set; nothing copied" }
    else {
        Set-Clipboard -Value $value
        Write-Host "$SecretName copied to the clipboard: paste it into Vercel as ORDER_SERVER_API_SECRET, then clear the clipboard (Set-Clipboard -Value ' '; also clear Win+V history if it is on)"
    }
    $value = $null
}

$fp = '(unreadable)'
if ($openssl -and (Test-Path -LiteralPath $Crt)) {
    $r = Invoke-OpenSsl $openssl @('x509', '-in', $Crt, '-noout', '-fingerprint', '-sha256')
    if ($r[0] -eq 0 -and $r[1] -match '=([0-9A-Fa-f:]+)') { $fp = $Matches[1].ToUpper() }
}
$urlHost = '<public-ip-or-ddns-name>'
$first = @($Hosts | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ }) | Select-Object -First 1
if ($first) { $urlHost = $first }

Write-Host ''
Write-Host 'Vercel (Project Settings, Environment Variables; server-side only, never NEXT_PUBLIC_):'
Write-Host ("  ORDER_SERVER_URL             = https://{0}:{1}" -f $urlHost, $Port)
Write-Host ("  ORDER_SERVER_TLS_FINGERPRINT = {0}" -f $fp)
Write-Host  '  ORDER_SERVER_API_SECRET      = (never printed: run this script with -CopySecret and paste)'
Write-Host ''
Write-Host 'Then restart the engine task so the API starts with these credentials:'
Write-Host '  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1'
Write-Host "  Start-ScheduledTask -TaskName 'XTNL engine'"
Write-Host 'Firewall, router and DNS steps (you run them yourself): docs\runbooks\order-server-api.md'
