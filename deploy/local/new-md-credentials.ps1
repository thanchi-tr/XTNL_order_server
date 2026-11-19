<#
.SYNOPSIS
  Create the redis-md (market-data store) credentials: two passwords, the ACL file and the content-hash key.

.DESCRIPTION
  redis-md (deploy/local/docker-compose.local.yml, compose profile md; docs/design/automated-risk-manager.md A2,
  A13, C2) holds ticks, bars and indicator values for the optional market-data layer and the ARM, which give
  advice only: Phase 1 never sends orders. This script prepares what redis-md and the engine need; it changes no
  firewall, router or other Windows setting, and it starts nothing.

  Runs from any folder (it switches to the repository root itself).

  1. Passwords: one random 32-byte password each for the redis-md users md_writer (the engine) and md_reader (the
     ops CLI and later read-only consumers), stored ONLY in the Windows User environment as the URLs
       XTNL_MD_REDIS_URL         redis://md_writer:<password>@127.0.0.1:56382/0
       XTNL_MD_REDIS_READER_URL  redis://md_reader:<password>@127.0.0.1:56382/0
     (start-xtnl.ps1 copies them into the engine process). Never printed, never written to a file.
  2. ACL file: %LOCALAPPDATA%\XTNL\redis-md\users.acl (outside the repository and OneDrive; mounted read-only into
     the container), written with the design's users: default off; md_writer and md_reader with their SHA-256
     password hashes (#<sha256> entries only, never a password); md_health nopass with +ping only (the
     healthcheck). Rewritten together with the passwords, so the two always match.
  3. Content-hash key: 32 random bytes as 64 hex characters, stored ONLY in the Windows User environment as
     XTNL_MD_HMAC_KEY (the key of every content hash stored in PostgreSQL). Its id is the non-secret
     XTNL_MD_HMAC_KEY_ID of the env file (1 for the first key). Never printed, never written to a file.

  Existing credentials are kept unless -Rotate is given (new passwords and ACL file; restart redis-md and the
  engine afterwards). The content-hash key is never replaced by -Rotate:
    -RotateKey keeps the current key as XTNL_MD_HMAC_KEY_PREV and creates a new XTNL_MD_HMAC_KEY. Then set, in
      the env file, XTNL_MD_HMAC_KEY_PREV_ID to the old id and XTNL_MD_HMAC_KEY_ID to a higher one (the script
      prints both), restart the engine and file
      `python -m src.ops md rehash --operator <you> --reason <ticket>`. Refused while a previous key is still kept.
    -RetirePrevKey removes XTNL_MD_HMAC_KEY_PREV, only after `python -m src.ops md show` reports the re-hash
      complete and one correction window has passed (design A13); then remove XTNL_MD_HMAC_KEY_PREV_ID from the
      env file and restart the engine.

  Afterwards: set XTNL_MD_ENABLED=1 in deploy\env\local.env and restart the engine task (stop-xtnl.ps1, then
  Start-ScheduledTask -TaskName 'XTNL engine'); start-xtnl.ps1 then starts redis-md (compose profile md).

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-md-credentials.ps1
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-md-credentials.ps1 -Rotate
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-md-credentials.ps1 -RotateKey
#>
param(
    [switch]$Rotate,
    [switch]$RotateKey,
    [switch]$RetirePrevKey,
    [string]$EnvFile
)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Set-Location -LiteralPath $Root
if (-not $EnvFile) { $EnvFile = Join-Path $Root 'deploy\env\local.env' }
$WriterName = 'XTNL_MD_REDIS_URL'
$ReaderName = 'XTNL_MD_REDIS_READER_URL'
$KeyName = 'XTNL_MD_HMAC_KEY'
$PrevKeyName = 'XTNL_MD_HMAC_KEY_PREV'
$HostPort = '127.0.0.1:56382'
$AclDir = Join-Path $env:LOCALAPPDATA 'XTNL\redis-md'
$AclFile = Join-Path $AclDir 'users.acl'
# The design's ACL rules (A2), the same as the dev service in deploy/docker-compose.dev.yml.
$WriterRules = 'resetkeys ~md:* resetchannels -@all +@string +@hash +@sortedset +@stream +del +unlink +exists ' +
               '+pexpire +evalsha +script|load +info +waitaof +ping +select +client|setinfo'
$ReaderRules = 'resetkeys %R~md:* resetchannels -@all +get +hgetall +zrange +zrangebyscore +xrange +xrevrange ' +
               '+xread +exists +ping +select +client|setinfo'
$HealthRules = 'nopass resetkeys resetchannels -@all +ping'
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

# The ACL file must stay out of the repository and out of OneDrive (synced folders leave the PC).
function Assert-AclLocation {
    $full = [System.IO.Path]::GetFullPath($AclFile)
    $banned = @($Root, $env:OneDrive, $env:OneDriveConsumer, $env:OneDriveCommercial) | Where-Object { $_ }
    foreach ($b in $banned) {
        $prefix = [System.IO.Path]::GetFullPath($b).TrimEnd('\') + '\'
        if ($full.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "refused: the ACL file would be inside $b (it must be outside the repository and OneDrive)"
        }
    }
}

# 32 random bytes as 64 lower-case hex characters.
function New-HexSecret {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    $text = -join ($bytes | ForEach-Object { $_.ToString('x2') })
    [Array]::Clear($bytes, 0, $bytes.Length)
    return $text
}

# SHA-256 of the password text, as Redis expects after '#'.
function Get-Sha256Hex([string]$Text) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { $hash = $sha.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Text)) } finally { $sha.Dispose() }
    return (-join ($hash | ForEach-Object { $_.ToString('x2') }))
}

# New md_writer / md_reader passwords: the ACL file first (hashes only), then the two URLs in the User environment.
function New-Passwords {
    Assert-AclLocation
    $writerPw = New-HexSecret
    $readerPw = New-HexSecret
    $acl = "user default off`n" +
           ('user md_writer on #{0} {1}' -f (Get-Sha256Hex $writerPw), $WriterRules) + "`n" +
           ('user md_reader on #{0} {1}' -f (Get-Sha256Hex $readerPw), $ReaderRules) + "`n" +
           ('user md_health on {0}' -f $HealthRules) + "`n"
    New-Item -ItemType Directory -Force -Path $AclDir | Out-Null
    $tmp = Join-Path $AclDir ('.new-{0}.acl' -f [Guid]::NewGuid().ToString('N'))
    try {
        [System.IO.File]::WriteAllText($tmp, $acl, $Utf8NoBom)   # LF, no BOM: Redis reads it as is
        Move-Item -LiteralPath $tmp -Destination $AclFile -Force
    } finally {
        if (Test-Path -LiteralPath $tmp) { Remove-Item -LiteralPath $tmp -Force }
    }
    $acl = $null
    Write-Host "ACL file written: $AclFile (password hashes only)"
    $url = 'redis://md_writer:' + $writerPw + '@' + $HostPort + '/0'
    [Environment]::SetEnvironmentVariable($WriterName, $url, 'User')
    $url = 'redis://md_reader:' + $readerPw + '@' + $HostPort + '/0'
    [Environment]::SetEnvironmentVariable($ReaderName, $url, 'User')
    $url = $null; $writerPw = $null; $readerPw = $null
    Write-Host "$WriterName and $ReaderName stored in the Windows User environment (passwords not shown)"
}

function New-Key([string]$Name) {
    $text = New-HexSecret
    [Environment]::SetEnvironmentVariable($Name, $text, 'User')
    $text = $null
    Write-Host "$Name stored in the Windows User environment (64 hex characters; not shown)"
}

# The current key id from the env file (a non-secret setting); 1 when the file does not set it.
function Get-KeyId {
    if (Test-Path -LiteralPath $EnvFile) {
        foreach ($line in Get-Content -LiteralPath $EnvFile) {
            if ($line -match '^\s*XTNL_MD_HMAC_KEY_ID\s*=\s*([0-9]+)\s*$') { return [int]$Matches[1] }
        }
    }
    return 1
}

# ------------------------------------------------------------------------------------------------ main
Write-Host 'XTNL redis-md credentials (market data for advice only: Phase 1 never sends orders)'
$haveWriter = -not [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($WriterName, 'User'))
$haveReader = -not [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($ReaderName, 'User'))
$haveKey = -not [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($KeyName, 'User'))
$havePrev = -not [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($PrevKeyName, 'User'))

if ($RetirePrevKey) {
    if (-not $havePrev) { Write-Host "$PrevKeyName is not set; nothing to retire" }
    else {
        [Environment]::SetEnvironmentVariable($PrevKeyName, $null, 'User')
        Write-Host "$PrevKeyName removed from the Windows User environment"
        Write-Host 'Now remove XTNL_MD_HMAC_KEY_PREV_ID from the env file and restart the engine task.'
    }
    return
}

if ($haveWriter -and $haveReader -and (Test-Path -LiteralPath $AclFile) -and -not $Rotate) {
    Write-Host "$WriterName, $ReaderName and the ACL file kept (use -Rotate to replace the passwords)"
} else {
    New-Passwords
}

if ($RotateKey) {
    if (-not $haveKey) { New-Key $KeyName }
    elseif ($havePrev) {
        throw "refused: $PrevKeyName is still set. Retire it first (-RetirePrevKey) once md show reports the re-hash complete and one correction window has passed"
    } else {
        $old = Get-KeyId
        $current = [Environment]::GetEnvironmentVariable($KeyName, 'User')
        [Environment]::SetEnvironmentVariable($PrevKeyName, $current, 'User')
        $current = $null
        Write-Host "$PrevKeyName stored in the Windows User environment (the previous key; not shown)"
        New-Key $KeyName
        Write-Host ("Now set in the env file: XTNL_MD_HMAC_KEY_PREV_ID={0} and XTNL_MD_HMAC_KEY_ID={1}; restart the engine task; then run: python -m src.ops md rehash --operator <you> --reason <ticket>" -f $old, ($old + 1))
    }
} elseif ($haveKey) {
    Write-Host "$KeyName kept (already set in the Windows User environment; -RotateKey starts a key rotation)"
} else {
    New-Key $KeyName
    Write-Host 'Its id is XTNL_MD_HMAC_KEY_ID in the env file (1 for the first key).'
}

Write-Host ''
Write-Host 'Then set XTNL_MD_ENABLED=1 in deploy\env\local.env and restart the engine task, which starts redis-md:'
Write-Host '  powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\stop-xtnl.ps1'
Write-Host "  Start-ScheduledTask -TaskName 'XTNL engine'"
Write-Host 'After -Rotate with redis-md already running, also restart it so it reads the new ACL file:'
Write-Host '  docker compose -f deploy\local\docker-compose.local.yml --profile md restart redis-md'
