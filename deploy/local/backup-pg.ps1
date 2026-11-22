<#
.SYNOPSIS
  Nightly encrypted off-site backup of the local PostgreSQL (home-server plan 3.2 option A, work item W3).
  Run under the separate backup account (for example xtnl-backup), never under the engine account, and only from
  the protected job folder that install-backup-task.ps1 installs (default C:\Program Files\XTNL\backup-job).

.DESCRIPTION
  What the 'XTNL backup' scheduled task runs (deploy\local\install-backup-task.ps1). Phase 1 never sends orders and
  this script only reads: it runs the compose job service `backup` (profile backup) of the docker-compose.local.yml
  next to it once:
      docker compose -f <job folder>\docker-compose.local.yml --profile backup run --rm --no-deps -T -e NAME ... backup
  The job (backup\entrypoint.sh next to it) dumps PostgreSQL to tmpfs, checks the dump with pg_restore -l,
  age-encrypts it and the roles dump to the age PUBLIC key, uploads them with their .sha256 to Backblaze B2
  (daily\<UTC timestamp>\, the month's first run also monthly\<YYYY-MM>\), then age-encrypts and uploads the new
  partition archive files (archive\). Runbook: docs\runbooks\home-backup-restore.md.

  It runs from a COPY of this script, the compose file, backup\Dockerfile and backup\entrypoint.sh in a folder only
  administrators can change (install-backup-task.ps1 copies them and sets the ACL). The repository checkout belongs
  to the engine account: code run from it with the B2 key loaded could be rewritten by the engine, the API or the
  live view (plan 3.2: none of them may ever see the key). Re-run install-backup-task.ps1 (admin) after the
  repository's copies change.

  Steps, each logged to <RepoRoot>\var\logs\backup-<yyyy-MM-dd>.log (files older than -LogKeepDays deleted; when
  that folder is not writable for this account, or no -RepoRoot is known, %LOCALAPPDATA%\XTNL\backup-logs instead).
  Names are logged, values never:
    1. single-instance guard (named mutex Global\XTNL-backup): a second copy exits 0 at once;
    2. refuses (exit 78) when it runs as the engine account (-EngineAccount, default xtnl), or when the engine's
       broker variables (STONE_X_*, CTRADER_*) are visible in this process or this account's User environment:
       that is the wrong account. Also refuses when any variable holds an age PRIVATE key (AGE-SECRET-KEY-...):
       that key never belongs on this PC;
    3. refuses (exit 78) unless the job files and their folders can be changed only by Administrators, SYSTEM or
       TrustedInstaller (owner and every write / delete / ACL right; Test-JobFilesLocked). This runs before any
       backup variable is read, so an unprotected copy never loads the key;
    4. reads the backup variables from the CURRENT account's Windows User environment: required
       XTNL_BACKUP_AGE_RECIPIENT (age1...), XTNL_BACKUP_B2_ACCOUNT, XTNL_BACKUP_B2_KEY, XTNL_BACKUP_B2_BUCKET;
       optional XTNL_BACKUP_HC_URL (Healthchecks.io check C) and XTNL_BACKUP_EXCLUDE_MARKS (0 / 1). The non-secret
       ones reach the container through `-e NAME` (the name only; docker compose takes the value from this
       process). The B2 secrets (XTNL_BACKUP_B2_ACCOUNT, XTNL_BACKUP_B2_KEY) are removed from this process's
       environment, so the docker CLI never inherits them, and are written to the job's stdin (NAME=value lines,
       then END; XTNL_BACKUP_SECRETS=stdin), so they are in no container configuration that `docker inspect`
       shows and on no command line. XTNL_BACKUP_HC_URL never leaves this process (not -e, not stdin): this script
       sends every check C ping itself (below);
    5. finds the partition archive folder: -ArchiveDir, else the User variable XTNL_BACKUP_ARCHIVE_HOST, else the
       XTNL_ARCHIVE_DIR line of <RepoRoot>\deploy\env\local.env (only that line is read), else C:\xtnl\archive. It
       must exist (exit 78 otherwise: create it per the runbook); it is mounted read-only;
    6. waits up to -DockerWaitMinutes for `docker info` (exit 10). Docker Desktop runs in the engine account's
       session; this account reaches it through its docker-users membership;
    7. `docker compose ... --profile backup build --quiet backup` (exit 11 on failure; cached after the first run);
    8. runs the job (-Check: `backup check`, which tests the settings, PostgreSQL and a bucket listing and uploads
       nothing) and returns its exit code. After -TimeoutMinutes the job's container is removed (`docker rm -f`,
       so no stopped container is left holding the job's configuration) and the script exits 75, or 66 when the
       job had already logged `DUMP SAFE:` (the dump is uploaded and this script has sent check C's success ping;
       no /fail).
  Check C (home-server review round 2). Only this script pings Healthchecks.io check C; the job never sees its URL.
  The job runs on Docker Desktop in the engine account's session, where anything running as the engine account
  can read what the container gets (runbook "Accepted deviation"); with the URL it could record a whole fake
  "good run" (/start, a forged upload, success) and Restore step 2 would take the forged dump. This script runs
  as the backup account outside Docker, where the engine account cannot read its memory. With XTNL_BACKUP_HC_URL
  set and not -Check, it pings:
    * <url>/start right before it starts the job (step 8);
    * <url> (success) once, when the job logs `xtnl-backup: DUMP SAFE: ` (the dump and globals are in
      daily/<ts>/), before the monthly copy and the partition archive;
    * <url>/fail when it fails itself after step 4 (exit 10, 11, 78), when the job ends with any code other than
      0, 65 or 66 (or times out) before `DUMP SAFE:`, and when the job ends 0, 65 or 66 without that line (exit
      70). After `DUMP SAFE:` nothing pings /fail: 65, 66 and a timeout (66) keep the success ping.
  The refusals of steps 2 and 3 (wrong account, broker variables or an age private key visible, the job files not
  protected) come before the URL is read, so they ping nothing: check C goes red only after its period and grace.

  Worst case 10 (Docker wait) + 30 (build) + 90 (job) min: the task's time limit (3 h, install-backup-task.ps1) is
  above it, so Task Scheduler never kills this script before it removes a hung container and pings /fail.

  Exit codes: 0 done (or another copy is running); 10 Docker not reachable; 11 the image build failed;
  66 the job timed out after its dump was uploaded; 70 the job ended 0, 65 or 66 without its `DUMP SAFE:` line;
  75 the job timed out (or rclone failed: try again); 78 configuration (wrong account, the job files not
  protected, a missing or malformed variable, the archive folder missing); otherwise the job's exit code
  (entrypoint.sh: 64 usage, 65 the dump was uploaded (DUMP SAFE:, check C success) but archive files were refused,
  66 the dump was uploaded (DUMP SAFE:, check C success) but a later step failed (monthly copy or archive),
  69 PostgreSQL not reachable within 5 min, 70 a dump check failed, 73 work folders, 75 upload / listing failed, 78
  configuration; docker compose itself: 1, 125-127). For 65 and 66 this script logs an ALERT: line and pings
  nothing more: the night's dump is good (review rounds 4 and 1), the refused files or the failed step are named
  in the [job] lines.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Program Files\XTNL\backup-job\backup-pg.ps1" -RepoRoot C:\xtnl\order-server -Check
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File "C:\Program Files\XTNL\backup-job\backup-pg.ps1" -RepoRoot C:\xtnl\order-server
#>
param(
    [switch]$Check,
    [string]$ArchiveDir,
    [string]$RepoRoot,
    [string]$EngineAccount = 'xtnl',
    [int]$DockerWaitMinutes = 10,
    [int]$TimeoutMinutes = 90,
    [int]$LogKeepDays = 30
)
$ErrorActionPreference = 'Stop'

# The job folder: this script, docker-compose.local.yml and backup\ (Dockerfile, entrypoint.sh) side by side, as
# in the repository's deploy\local and in the protected copy.
$JobRoot = $PSScriptRoot
Set-Location $JobRoot
$ComposeFile = Join-Path $JobRoot 'docker-compose.local.yml'
$JobFiles = @($PSCommandPath, $ComposeFile, (Join-Path $JobRoot 'backup\Dockerfile'),
              (Join-Path $JobRoot 'backup\entrypoint.sh'))
$JobFolders = @($JobRoot, (Join-Path $JobRoot 'backup'), (Split-Path -Parent $JobRoot))
if (-not $RepoRoot) {
    # Run from the repository itself (deploy\local): its root is two levels up. The protected copy gets -RepoRoot
    # from the task's arguments.
    $guess = Join-Path $JobRoot '..\..'
    if (Test-Path -LiteralPath (Join-Path $guess 'deploy\env')) { $RepoRoot = (Resolve-Path -LiteralPath $guess).Path }
}
$LocalEnvFile = $null
if ($RepoRoot) { $LocalEnvFile = Join-Path $RepoRoot 'deploy\env\local.env' }
$MutexName = 'Global\XTNL-backup'
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
# Passed to the container by name only. Taken from this account's Windows User environment, never from a file.
$BackupVars = @('XTNL_BACKUP_AGE_RECIPIENT', 'XTNL_BACKUP_B2_ACCOUNT', 'XTNL_BACKUP_B2_KEY', 'XTNL_BACKUP_B2_BUCKET',
                'XTNL_BACKUP_HC_URL', 'XTNL_BACKUP_EXCLUDE_MARKS')
$RequiredVars = @('XTNL_BACKUP_AGE_RECIPIENT', 'XTNL_BACKUP_B2_ACCOUNT', 'XTNL_BACKUP_B2_KEY', 'XTNL_BACKUP_B2_BUCKET')
# Never in this process's environment once read, never `-e`: they go to the job on its stdin only.
$SecretVars = @('XTNL_BACKUP_B2_ACCOUNT', 'XTNL_BACKUP_B2_KEY')
# Never in this process's environment once read, never `-e`, never on the job's stdin: the check C URL stays in
# this script, which sends every check C ping (home-server review round 2: the job's container is reachable by the
# engine account, and the URL would let it record a fake good run).
$WrapperOnlyVars = @('XTNL_BACKUP_HC_URL')
# The engine's broker credentials. Visible here means this is the engine account (or a copy of its variables):
# the B2 key must never sit next to them (plan 3.2), so the backup refuses to run.
$EngineOnlyPatterns = @('STONE_X_*', 'CTRADER_*')
# Who may own or change the job files (well-known SIDs: Administrators, SYSTEM, TrustedInstaller).
$TrustedSids = @('S-1-5-32-544', 'S-1-5-18', 'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464')
# Rights that let a holder change a job file or folder: write / append data (add file / subfolder), write
# attributes and extended attributes, delete child, delete, change permissions, take ownership, generic write /
# all. Read and execute are fine.
$WriteRightsMask = [int64]0x500D0156
# The job's exit codes after which its dump is safe in daily/<ts>/ (0 done, 65 archive files refused, 66 a later
# step failed): good nights, provided the job logged `DUMP SAFE:`. Every other code pings /fail.
$GoodJobCodes = @(0, 65, 66)
$script:HcUrl = $null
# Set when the job logs `DUMP SAFE:` (entrypoint.sh step 6): the night's dump is in daily/<ts>/ and this script has
# pinged check C's success, so a later timeout must not ping /fail (review round 1: the restore would reject it).
$script:DumpSafe = $false
# True only while the backup job runs (not -Check, not docker info / build): the `DUMP SAFE:` line then sends the
# success ping, once.
$script:WatchDumpSafe = $false
$script:Secrets = @{}
$script:Mutex = $null
$script:LogDir = $null

# ------------------------------------------------------------------------------------------------ logging
function Initialize-Log {
    $fallback = Join-Path $env:LOCALAPPDATA 'XTNL\backup-logs'
    $script:LogDir = $fallback
    if ($RepoRoot) {
        $candidate = Join-Path $RepoRoot 'var\logs'
        try {
            if (-not (Test-Path -LiteralPath $candidate)) { New-Item -ItemType Directory -Path $candidate | Out-Null }
            $probe = Join-Path $candidate ("backup-{0}.log" -f (Get-Date).ToString('yyyy-MM-dd'))
            [System.IO.File]::AppendAllText($probe, '', $Utf8NoBom)
            $script:LogDir = $candidate
        } catch {
            # var\logs belongs to the engine account; grant this account write access (runbook) or use the fallback.
            $script:LogDir = $fallback
        }
    }
    if (-not (Test-Path -LiteralPath $script:LogDir)) { New-Item -ItemType Directory -Path $script:LogDir | Out-Null }
    $cutoff = (Get-Date).AddDays(-$LogKeepDays)
    Get-ChildItem -Path $script:LogDir -Filter 'backup-*.log' -File -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $cutoff } |
        ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
}

function Write-RawLog([string]$Line) {
    $path = Join-Path $script:LogDir ("backup-{0}.log" -f (Get-Date).ToString('yyyy-MM-dd'))
    for ($i = 0; $i -lt 3; $i++) {
        try { [System.IO.File]::AppendAllText($path, $Line + "`r`n", $Utf8NoBom); break }
        catch { Start-Sleep -Milliseconds 50 }
    }
    try { [Console]::Out.WriteLine($Line) } catch { }
}

function Write-Log([string]$Message) {
    Write-RawLog ('{0} [backup-pg] {1}' -f (Get-Date).ToString('yyyy-MM-dd"T"HH:mm:ss.fffzzz'), $Message)
}

# The Healthchecks.io URL is a secret (whoever holds it can record a fake good run): never logged, never handed to
# a child process, errors swallowed. $Suffix: '/start', '' (success) or '/fail'. -Check is a manual test: it never
# pings check C.
function Send-HcPing([string]$Suffix) {
    if (-not $script:HcUrl -or $Check) { return }
    $label = if ($Suffix) { $Suffix } else { 'success' }
    for ($i = 0; $i -lt 3; $i++) {
        try {
            [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
            Invoke-WebRequest -Uri ($script:HcUrl.TrimEnd('/') + $Suffix) -UseBasicParsing -TimeoutSec 10 -Method Get | Out-Null
            Write-Log ("healthcheck ping {0} sent" -f $label)
            return
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    Write-Log ("WARNING: healthcheck ping {0} failed" -f $label)
}

function Send-HcFail { Send-HcPing '/fail' }

function Exit-Backup([int]$Code, [string]$Message, [switch]$Ping) {
    if ($Message) { Write-Log $Message }
    if ($Ping) { Send-HcFail }
    Write-Log ("backup-pg exit {0}" -f $Code)
    if ($null -ne $script:Mutex) {
        try { $script:Mutex.ReleaseMutex() } catch { }
        $script:Mutex.Dispose()
    }
    exit $Code
}

# ------------------------------------------------------------------------------------------------ job files
# Owners and allow-ACEs with a write right that are not in $TrustedSids, as 'path: SID' strings (empty = locked).
# Inherit-only ACEs are skipped: they do not apply to the object itself, and every child is checked on its own.
function Get-JobFileOffenders([string[]]$Paths) {
    $out = @()
    $sidType = [System.Security.Principal.SecurityIdentifier]
    foreach ($p in $Paths) {
        if (-not (Test-Path -LiteralPath $p)) { $out += ("{0}: missing" -f $p); continue }
        $acl = Get-Acl -LiteralPath $p
        $owner = $acl.GetOwner($sidType).Value
        if ($TrustedSids -notcontains $owner) { $out += ("{0}: owner {1}" -f $p, $owner) }
        foreach ($rule in $acl.GetAccessRules($true, $true, $sidType)) {
            if ($rule.AccessControlType -ne [System.Security.AccessControl.AccessControlType]::Allow) { continue }
            if (($rule.PropagationFlags -band [System.Security.AccessControl.PropagationFlags]::InheritOnly) -ne 0) { continue }
            if (([int64]$rule.FileSystemRights -band $WriteRightsMask) -eq 0) { continue }
            $sid = $rule.IdentityReference.Value
            if ($TrustedSids -notcontains $sid) { $out += ("{0}: {1} may write" -f $p, $sid) }
        }
    }
    return $out
}

# ------------------------------------------------------------------------------------------------ child processes
function Format-Arg([string]$Value) {
    if ($Value -eq '') { return '""' }
    if ($Value -notmatch '[\s"]') { return $Value }
    $s = $Value -replace '(\\*)"', '$1$1\"'
    $s = $s -replace '(\\+)$', '$1$1'
    return '"' + $s + '"'
}

# Runs one command with stdout / stderr copied line by line into the log. Returns its exit code, or -1 when it
# did not finish within the timeout (the process tree is then ended). The child inherits this process's
# environment, which is how `-e NAME` gets its value. $StdinText (optional) is written to the child's stdin as
# UTF-8 bytes and stdin is closed: the secrets' only way into the job. Untyped on purpose: a [string] parameter
# turns $null into '', which would redirect stdin for every call.
function Invoke-Logged([string]$Exe, [string[]]$Arguments, [string]$Prefix, [int]$TimeoutS, $StdinText = $null) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $Exe
    $psi.Arguments = (($Arguments | ForEach-Object { Format-Arg $_ }) -join ' ')
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.RedirectStandardInput = ($null -ne $StdinText)
    $psi.StandardOutputEncoding = $Utf8NoBom
    $psi.StandardErrorEncoding = $Utf8NoBom
    $psi.WorkingDirectory = $JobRoot
    # Windows PowerShell 5.1 (.NET Framework) builds the child's StandardInput as a StreamWriter on
    # [Console]::InputEncoding and writes that encoding's preamble into the pipe at Process.Start. With a UTF-8
    # console input code page (chcp 65001, a profile, or the Windows 'Beta: Use Unicode UTF-8' option) that is a
    # BOM before the first secret line, and the job refused it with exit 78 (review round 3). A UTF-8 encoding
    # without a BOM is set for the Start only, then the old one is put back.
    $prevInEnc = $null
    $inEncSet = $false
    if ($null -ne $StdinText) {
        try {
            $prevInEnc = [Console]::InputEncoding
            [Console]::InputEncoding = $Utf8NoBom
            $inEncSet = $true
        } catch {
            Write-Log ("WARNING: the console input encoding could not be set ({0}); the job strips a leading BOM itself" -f $_.Exception.GetType().Name)
        }
    }
    try {
        $proc = [System.Diagnostics.Process]::Start($psi)
    } finally {
        if ($inEncSet) { try { [Console]::InputEncoding = $prevInEnc } catch { } }
    }
    if ($null -ne $StdinText) {
        try {
            $bytes = $Utf8NoBom.GetBytes($StdinText)
            $proc.StandardInput.BaseStream.Write($bytes, 0, $bytes.Length)
            $proc.StandardInput.BaseStream.Flush()
        } catch {
            Write-Log ("WARNING: the job's settings could not be written to its stdin ({0}); it will refuse with exit 78" -f $_.Exception.GetType().Name)
        } finally {
            try { $proc.StandardInput.Close() } catch { }
        }
    }
    $tasks = @{ Out = $proc.StandardOutput.ReadLineAsync(); Err = $proc.StandardError.ReadLineAsync() }
    $deadline = (Get-Date).AddSeconds($TimeoutS)
    while (($null -ne $tasks.Out) -or ($null -ne $tasks.Err)) {
        $pending = @($tasks.Values | Where-Object { $null -ne $_ })
        [void][System.Threading.Tasks.Task]::WaitAny([System.Threading.Tasks.Task[]]$pending, 1000)
        foreach ($k in @('Out', 'Err')) {
            $t = $tasks[$k]
            if ($null -eq $t -or -not $t.IsCompleted) { continue }
            $line = $null
            try { $line = $t.Result } catch { $line = $null }
            if ($null -eq $line) { $tasks[$k] = $null; continue }
            Write-RawLog ($Prefix + $line)
            if ($script:WatchDumpSafe -and -not $script:DumpSafe -and $line -match 'xtnl-backup: DUMP SAFE: ') {
                $script:DumpSafe = $true
                Send-HcPing ''      # check C's success: the night's dump and globals are in daily/<ts>/
            }
            if ($k -eq 'Out') { $tasks[$k] = $proc.StandardOutput.ReadLineAsync() }
            else { $tasks[$k] = $proc.StandardError.ReadLineAsync() }
        }
        if ((Get-Date) -gt $deadline) {
            try { & taskkill.exe /PID $proc.Id /T /F 2>$null | Out-Null } catch { }
            return -1
        }
    }
    [void]$proc.WaitForExit(10000)
    if (-not $proc.HasExited) { return -1 }
    return [int]$proc.ExitCode
}

function Find-Docker {
    $cmd = Get-Command 'docker.exe' -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $fallback = Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'
    if (Test-Path -LiteralPath $fallback) { return $fallback }
    return $null
}

# The XTNL_ARCHIVE_DIR value of <RepoRoot>\deploy\env\local.env, reading only that one line (the file holds no
# secret by design, but nothing else of it is needed here).
function Get-EngineArchiveDir {
    if (-not $LocalEnvFile) { return $null }
    if (-not (Test-Path -LiteralPath $LocalEnvFile)) { return $null }
    try {
        $line = Select-String -LiteralPath $LocalEnvFile -Pattern '^\s*XTNL_ARCHIVE_DIR\s*=' | Select-Object -Last 1
    } catch { return $null }
    if ($null -eq $line) { return $null }
    $value = $line.Line.Substring($line.Line.IndexOf('=') + 1).Trim()
    if ($value) { return $value }
    return $null
}

# ------------------------------------------------------------------------------------------------ main
Initialize-Log
$mode = if ($Check) { 'check' } else { 'backup' }
Write-Log ("backup-pg starting (mode {0}); job folder {1}; log folder {2}" -f $mode, $JobRoot, $script:LogDir)

# Step 1: single instance.
$script:Mutex = New-Object System.Threading.Mutex($false, $MutexName)
$owned = $false
try { $owned = $script:Mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $owned = $true }
if (-not $owned) {
    $script:Mutex.Dispose()
    $script:Mutex = $null
    Exit-Backup 0 'another backup-pg is running; this copy exits'
}

# Step 2: the right account.
$me = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$shortName = ($me -split '\\')[-1]
if ($shortName -ieq $EngineAccount) {
    Exit-Backup 78 ("refused: running as the engine account '{0}'. Run it as the backup account (runbook home-backup-restore.md); the B2 key never enters the engine account" -f $shortName)
}
$visible = @()
foreach ($scope in @('Process', 'User')) {
    foreach ($name in @([Environment]::GetEnvironmentVariables($scope).Keys)) {
        foreach ($pattern in $EngineOnlyPatterns) {
            if ($name -like $pattern) { $visible += [string]$name }
        }
    }
}
if ($visible.Count -gt 0) {
    Exit-Backup 78 ("refused: the engine's broker variables are visible to account '{0}' ({1}): wrong account" -f $shortName, (($visible | Sort-Object -Unique) -join ', '))
}
foreach ($scope in @('Process', 'User')) {
    $vars = [Environment]::GetEnvironmentVariables($scope)
    foreach ($name in @($vars.Keys)) {
        $value = [string]$vars[$name]
        if ($value.TrimStart().StartsWith('AGE-SECRET-KEY-', [System.StringComparison]::OrdinalIgnoreCase)) {
            Exit-Backup 78 ("refused: variable {0} ({1} environment) holds an age PRIVATE key; it must never be on this PC (keep it in the password manager and on paper)" -f $name, $scope)
        }
    }
}
$value = $null
Write-Log ("account {0}: not the engine account, no broker variables visible" -f $me)

# Step 3: the job files can be changed by administrators only (checked before any backup variable is read).
$offenders = @(Get-JobFileOffenders ($JobFiles + $JobFolders))
if ($offenders.Count -gt 0) {
    Exit-Backup 78 ("refused: the job files are not protected ({0}). Run the copy that install-backup-task.ps1 installs (admin), never the repository's: code the engine account can change must never run with the B2 key loaded (runbook home-backup-restore.md 'Job folder')" -f ($offenders -join '; '))
}
Write-Log ("job files protected (owner and write rights: Administrators, SYSTEM, TrustedInstaller only): {0}" -f $JobRoot)

# Step 4: the backup variables, from this account's User environment only. The secret ones leave this process's
# environment at once (the docker CLI must not inherit them) and travel on the job's stdin.
$missing = @()
$passed = @()
foreach ($name in $BackupVars) {
    $v = [Environment]::GetEnvironmentVariable($name, 'User')
    [Environment]::SetEnvironmentVariable($name, $null, 'Process')
    if ([string]::IsNullOrWhiteSpace($v)) {
        if ($RequiredVars -contains $name) { $missing += $name }
        continue
    }
    if ($WrapperOnlyVars -contains $name) { $script:HcUrl = $v.Trim() }
    elseif ($SecretVars -contains $name) { $script:Secrets[$name] = $v.Trim() }
    else { [Environment]::SetEnvironmentVariable($name, $v.Trim(), 'Process') }
    $passed += $name
}
$v = $null
if ($script:HcUrl -and -not $script:HcUrl.StartsWith('https://')) {
    $script:HcUrl = $null
    Exit-Backup 78 'XTNL_BACKUP_HC_URL must be an https:// URL'
}
if ($missing.Count -gt 0) {
    Exit-Backup 78 ("missing in this account's Windows User environment: {0} (runbook home-backup-restore.md 'Backup account')" -f ($missing -join ', ')) -Ping
}
if (-not ([Environment]::GetEnvironmentVariable('XTNL_BACKUP_AGE_RECIPIENT', 'Process')).StartsWith('age1')) {
    Exit-Backup 78 'XTNL_BACKUP_AGE_RECIPIENT is not an age public key (age1...)' -Ping
}
foreach ($name in @($script:Secrets.Keys)) {
    if ($script:Secrets[$name] -match '[\r\n]') {
        Exit-Backup 78 ("{0} holds a line break; set it again (runbook 'Backup account')" -f $name) -Ping
    }
}
# Non-secret switch for the job: read the secrets from stdin, never from its environment.
[Environment]::SetEnvironmentVariable('XTNL_BACKUP_SECRETS', 'stdin', 'Process')
Write-Log ("backup variables found (names only): {0}; B2 secrets go to the job on stdin, the check C URL stays here" -f ($passed -join ', '))

# Step 5: the partition archive folder (the engine's XTNL_ARCHIVE_DIR, W10), mounted read-only.
$archiveSource = 'parameter'
if (-not $ArchiveDir) {
    $ArchiveDir = [Environment]::GetEnvironmentVariable('XTNL_BACKUP_ARCHIVE_HOST', 'User')
    $archiveSource = 'User variable XTNL_BACKUP_ARCHIVE_HOST'
}
if (-not $ArchiveDir) {
    $ArchiveDir = Get-EngineArchiveDir
    $archiveSource = 'XTNL_ARCHIVE_DIR in deploy\env\local.env'
}
if (-not $ArchiveDir) {
    $ArchiveDir = 'C:\xtnl\archive'
    $archiveSource = 'default'
}
if (-not [System.IO.Path]::IsPathRooted($ArchiveDir)) {
    if (-not $RepoRoot) {
        Exit-Backup 78 ("the archive folder {0} ({1}) is relative and no -RepoRoot is known; give an absolute folder" -f $ArchiveDir, $archiveSource) -Ping
    }
    $ArchiveDir = Join-Path $RepoRoot $ArchiveDir
}
if (-not (Test-Path -LiteralPath $ArchiveDir -PathType Container)) {
    Exit-Backup 78 ("partition archive folder not found: {0} ({1}); create it as the runbook says (home-backup-restore.md 'Archive folder')" -f $ArchiveDir, $archiveSource) -Ping
}
$ArchiveDir = (Resolve-Path -LiteralPath $ArchiveDir).Path
if ($ArchiveDir -match '\\OneDrive') {
    Write-Log ("WARNING: the archive folder {0} is under OneDrive; the plan keeps it outside OneDrive (C:\xtnl\archive)" -f $ArchiveDir)
}
# Read by compose for the read-only bind of the backup service (forward slashes for Docker Desktop).
[Environment]::SetEnvironmentVariable('XTNL_BACKUP_ARCHIVE_HOST', ($ArchiveDir -replace '\\', '/'), 'Process')
Write-Log ("partition archive folder: {0} ({1}), mounted read-only" -f $ArchiveDir, $archiveSource)

# Step 6: Docker (Docker Desktop runs in the engine account's session; docker-users membership gives access).
$docker = $null
$deadline = (Get-Date).AddMinutes($DockerWaitMinutes)
while ($true) {
    $docker = Find-Docker
    if ($docker -and (Invoke-Logged $docker @('info', '--format', 'docker server {{.ServerVersion}}') '[docker] ' 60) -eq 0) { break }
    if ((Get-Date) -ge $deadline) {
        Exit-Backup 10 ("Docker not reachable after {0} min: is the engine account signed in with Docker Desktop running, and is this account in docker-users (sign out and in after adding it)?" -f $DockerWaitMinutes) -Ping
    }
    Start-Sleep -Seconds 15
}

# Step 7: the job image (cached after the first build).
$compose = @('compose', '--ansi', 'never', '-f', $ComposeFile, '--profile', 'backup')
Write-Log 'building the backup image: docker compose ... --profile backup build --quiet backup'
$code = Invoke-Logged $docker ($compose + @('build', '--quiet', 'backup')) '[build] ' 1800
if ($code -ne 0) { Exit-Backup 11 ("the backup image build failed (exit {0})" -f $code) -Ping }

# Step 8: the job. Non-secret variables go by NAME only (docker compose reads the value from this process); the
# secrets go on stdin, never as -e (docker inspect would show them to every docker-users member).
$runArgs = $compose + @('run', '--rm', '--no-deps', '-T')
foreach ($name in ($BackupVars + @('XTNL_BACKUP_SECRETS'))) {
    if ($SecretVars -contains $name) { continue }
    if ($WrapperOnlyVars -contains $name) { continue }
    if ([Environment]::GetEnvironmentVariable($name, 'Process')) { $runArgs += @('-e', $name) }
}
$runArgs += @('backup', $mode)
$stdin = ''
foreach ($name in $SecretVars) {
    if ($script:Secrets.ContainsKey($name)) { $stdin += ("{0}={1}`n" -f $name, $script:Secrets[$name]) }
}
$stdin += "END`n"
Write-Log ("running: docker {0} (secrets on stdin: {1})" -f ($runArgs -join ' '), ((@($script:Secrets.Keys) | Sort-Object) -join ', '))
# Check C's run starts here, from this script (never from the job): /start, then success on `DUMP SAFE:`.
Send-HcPing '/start'
$script:WatchDumpSafe = (-not $Check)
$code = Invoke-Logged $docker $runArgs '[job] ' ($TimeoutMinutes * 60) $stdin
$script:WatchDumpSafe = $false
$stdin = $null
if ($code -eq -1) {
    # Killing the docker CLI does not stop the container, and the CLI that would have done --rm is gone: remove
    # it (running or stopped), so a hung job neither holds tomorrow's run nor stays behind with its configuration.
    $ids = @()
    try {
        $ids = @(& $docker ps -aq --filter 'label=com.docker.compose.project=xtnl-local' --filter 'label=com.docker.compose.service=backup' 2>$null)
    } catch { $ids = @() }
    foreach ($id in $ids) { if ($id) { Invoke-Logged $docker @('rm', '-f', $id) '[docker] ' 120 | Out-Null } }
    if ($script:DumpSafe) {
        Exit-Backup 66 ("ALERT: the backup job did not finish within {0} min (its container was removed) after the dump was uploaded; check C keeps the success ping (runbook home-backup-restore.md exit 66)" -f $TimeoutMinutes)
    }
    Exit-Backup 75 ("the backup job did not finish within {0} min; its container was removed" -f $TimeoutMinutes) -Ping
}
if (($GoodJobCodes -contains $code) -and -not $Check -and -not $script:DumpSafe) {
    # A good exit code without the dump's `DUMP SAFE:` line: no success was pinged, so nothing proves the night's
    # dump is in daily/<ts>/. Treated as a failed night.
    Exit-Backup 70 ("ALERT: the backup job ended with exit {0} but never logged DUMP SAFE:; treated as a failed night" -f $code) -Ping
}
if ($code -eq 0) {
    Exit-Backup 0 ("backup job finished (mode {0})" -f $mode)
}
if ($code -eq 65) {
    # The job uploaded the dump and this script pinged check C's success on its DUMP SAFE line; only archive files
    # were refused. No /fail ping: check C must keep recording the night as good, so Restore step 2 accepts its dump
    # (review round 4).
    Exit-Backup 65 'ALERT: backup dump uploaded, but partition archive file(s) were refused (ARCHIVE REFUSED in the [job] lines; runbook home-backup-restore.md exit 65)'
}
if ($code -eq 66) {
    # The job uploaded the dump (success pinged on its DUMP SAFE line), then a later step failed (the monthly copy
    # or the partition archive). No /fail ping, as for 65: the night's daily/<ts>/ is good (review round 1).
    Exit-Backup 66 'ALERT: backup dump uploaded, but a later step failed (LATE FAILURE in the [job] lines; runbook home-backup-restore.md exit 66)'
}
if ($code -eq 78) {
    Exit-Backup 78 'backup job refused its configuration (exit 78; did its secrets reach it on stdin?); see the [job] lines above' -Ping
}
# Every other code: the job failed before its dump was safe, or docker compose could not create or start it
# (1, 125-127). The job never pings, so this script pings /fail for all of them.
Exit-Backup $code ("backup job failed (exit {0}); see the [job] lines above (1, 125-127: docker compose could not create or start it)" -f $code) -Ping
