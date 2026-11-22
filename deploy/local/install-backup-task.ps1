<#
.SYNOPSIS
  Install the protected backup job folder and register (or with -Uninstall remove) the 'XTNL backup' scheduled task:
  backup-pg.ps1 once a day under the separate backup account (home-server plan 3.2 option A, work item W3).

.DESCRIPTION
  Run it yourself, once and after every change to the job files, from an ELEVATED PowerShell (writing under Program
  Files and registering a task for another account need admin rights); it is never run automatically. Run it from
  an ADMIN-ONLY CLONE of the repository (runbook home-backup-restore.md Set-up 7), never from the engine's checkout:
      powershell -NoProfile -ExecutionPolicy Bypass -File C:\xtnl-admin\order-server\deploy\local\install-backup-task.ps1 -RepoRoot C:\xtnl\order-server
  Idempotent: re-running refreshes the job folder and replaces the task (Register-ScheduledTask -Force). Phase 1
  never sends orders.

  Where the code comes from (review round 2). The engine's checkout (-RepoRoot) is writable by the engine account,
  so anything running as it could change a job file, or this installer, and an elevated run would then copy (or
  execute) that code as administrator. So this script:
    * refuses to run when this script, the four job files or their folders can be changed by anyone other than
      Administrators, SYSTEM, TrustedInstaller or the admin running it (the engine's checkout always fails this);
    * when the source is a git work tree: refuses uncommitted changes to the job files, prints the commit (HEAD) and
      `git verify-commit` for you to compare with the commit you reviewed;
    * prints, per job file, whether it differs from the installed copy (SHA-256 old -> new) and, unless -Yes is
      given, asks you to type yes before anything is copied.
  What is left (accepted, runbook): the admin clone is only as good as the commit you compare it with.

  Job folder (-JobDir, default C:\Program Files\XTNL\backup-job). The task never runs the repository's files: the
  repository belongs to the engine account, and anything running as that account (the engine, the network-facing
  API, the live view) could otherwise add a line to backup-pg.ps1 or entrypoint.sh and have it run with the B2 key
  loaded (plan 3.2: none of them may ever see the key). So this script:
    * refuses a -JobDir inside the repository;
    * copies backup-pg.ps1, docker-compose.local.yml, backup\Dockerfile and backup\entrypoint.sh from this script's
      own deploy\local (the admin clone) into the job folder (the same layout), and checks each copy's SHA-256
      against its source (that proves the copy, not the source: the checks above cover the source);
    * sets the folder's ACL with icacls: owner Administrators, inheritance removed, Full control for Administrators
      and SYSTEM, Read & execute for the backup account, nobody else (recursive inside the job folder only). The
      parent folder (C:\Program Files\XTNL by default) gets the same owner and entries on the folder object alone
      (no /T, nothing propagates below it), so nobody else can rename the job folder away. That is why the parent
      must hold nothing but the job folder: a -JobDir whose parent is a drive root, %ProgramFiles%, %SystemRoot%,
      %ProgramData%, a user profile or the profiles folder, the repository's folder (C:\xtnl), or any folder with
      other items in it, is refused before anything is copied.
      backup-pg.ps1 refuses (exit 78) to load a secret unless the files and folders are protected like this.
  The only Windows change it makes is that ACL on the job folder and its parent; the runbook
  (docs\runbooks\home-backup-restore.md) lists the ones you make yourself (the backup account, its docker-users
  membership, its User variables, the archive folder rights). Accepted deviation (runbook): the job still runs on
  the engine account's Docker Desktop, so anything with Docker access can read the key from the running container.

  Task 'XTNL backup', for -User (default <this computer>\xtnl-backup; refused if it is the engine account):
    * trigger: daily at -At (default 03:15 local time). Chosen away from the engine start: the engine starts at
      log-on (boot, Windows Update restarts in the Saturday window) and applies migrations with lock_timeout 2 s,
      which a running pg_dump's locks could delay; 03:15 Melbourne is also clear of the 17:00 New York roll
      (07:00-09:00 Melbourne), when the RiskSweep writes sod_marks;
    * action:  powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File <job folder>\backup-pg.ps1
               -RepoRoot <-RepoRoot: the engine's checkout> (for the log folder var\logs and the XTNL_ARCHIVE_DIR
               line of local.env only; no code is read from it), working folder the job folder;
    * runs whether the backup account is signed in or not: -LogonType S4U (default: no password stored; local
      access only, which is all it needs: the Docker named pipe and outbound HTTPS) or Password (prompts for the
      account's password once; Task Scheduler stores it). The backup account is never signed in interactively;
      Docker Desktop runs in the engine account's session;
    * normal limited rights, below-normal priority, at most 3 h (above backup-pg.ps1's own worst case: 10 min Docker
      wait + 30 min build + 90 min job, so the script always gets to remove a hung container and ping /fail), a
      missed run starts when the PC is next on, a second start while one runs is ignored (backup-pg.ps1 also has its
      own single-instance guard), runs on batteries too;
    * restart on a failed launch every 15 min, up to 3 times (a non-zero exit of backup-pg.ps1 does not restart
      the task: the next night's run and Healthchecks.io check C cover that).

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File C:\xtnl-admin\order-server\deploy\local\install-backup-task.ps1 -RepoRoot C:\xtnl\order-server -User "$env:COMPUTERNAME\xtnl-backup" -At 03:15
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File C:\xtnl-admin\order-server\deploy\local\install-backup-task.ps1 -Uninstall
#>
param(
    [string]$User = "$env:COMPUTERNAME\xtnl-backup",
    [string]$At = '03:15',
    [ValidateSet('S4U', 'Password')]
    [string]$LogonType = 'S4U',
    [string]$EngineAccount = 'xtnl',
    [string]$JobDir = (Join-Path $env:ProgramFiles 'XTNL\backup-job'),
    [string]$RepoRoot = 'C:\xtnl\order-server',
    [switch]$Yes,
    [switch]$Uninstall,
    [switch]$RunNow
)
$ErrorActionPreference = 'Stop'
$TaskName = 'XTNL backup'
# backup-pg.ps1's worst case is 10 + 30 + 90 = 130 min; the task limit must stay above it (it kills the wrapper).
$TaskLimitHours = 3
# The job files, relative to deploy\local and to the job folder (the same layout in both).
$JobFileNames = @('backup-pg.ps1', 'docker-compose.local.yml', 'backup\Dockerfile', 'backup\entrypoint.sh')
# Who may own or change the installer and the job sources (well-known SIDs: Administrators, SYSTEM,
# TrustedInstaller; the admin running this is added below). The same rights mask as backup-pg.ps1's job-file check.
$TrustedSids = @('S-1-5-32-544', 'S-1-5-18', 'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464')
$WriteRightsMask = [int64]0x500D0156

# Owners and allow-ACEs with a write right that are not trusted, as 'path: SID' strings (empty = only trusted
# accounts can change it). Inherit-only ACEs are skipped: every child is checked on its own.
function Get-SourceOffenders([string[]]$Paths, [string[]]$Trusted) {
    $out = @()
    $sidType = [System.Security.Principal.SecurityIdentifier]
    foreach ($p in $Paths) {
        if (-not (Test-Path -LiteralPath $p)) { $out += ("{0}: missing" -f $p); continue }
        $acl = Get-Acl -LiteralPath $p
        $owner = $acl.GetOwner($sidType).Value
        if ($Trusted -notcontains $owner) { $out += ("{0}: owner {1}" -f $p, $owner) }
        foreach ($rule in $acl.GetAccessRules($true, $true, $sidType)) {
            if ($rule.AccessControlType -ne [System.Security.AccessControl.AccessControlType]::Allow) { continue }
            if (($rule.PropagationFlags -band [System.Security.AccessControl.PropagationFlags]::InheritOnly) -ne 0) { continue }
            if (([int64]$rule.FileSystemRights -band $WriteRightsMask) -eq 0) { continue }
            $sid = $rule.IdentityReference.Value
            if ($Trusted -notcontains $sid) { $out += ("{0}: {1} may write" -f $p, $sid) }
        }
    }
    return $out
}

if ($Uninstall) {
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -eq $task) {
        Write-Host "no scheduled task '$TaskName' registered; nothing to remove."
    } else {
        if ($task.State -eq 'Running') { Stop-ScheduledTask -TaskName $TaskName }
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "removed scheduled task '$TaskName'. The B2 bucket, its objects and the backup account are unchanged."
    }
    Write-Host "the job folder $JobDir is left in place; remove it yourself (admin) if you no longer need it."
    exit 0
}

# $Root: the clone this script runs from (the source of the job files). $RepoRoot: the engine's checkout, which the
# task passes to backup-pg.ps1 for its log folder and the XTNL_ARCHIVE_DIR line only.
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$SourceDir = Join-Path $Root 'deploy\local'
if (-not (Test-Path -LiteralPath $RepoRoot -PathType Container)) {
    throw "-RepoRoot $RepoRoot (the engine's checkout) not found; give the folder the engine runs from."
}
$RepoRoot = (Resolve-Path -LiteralPath $RepoRoot).Path
$shortName = ($User -split '\\')[-1]
if ($shortName -ieq $EngineAccount) {
    throw "refused: '$User' is the engine account. The backup runs under its own account so the B2 key never enters the engine's environment (runbook home-backup-restore.md)."
}
$time = [DateTime]::MinValue
if (-not [DateTime]::TryParseExact($At, 'HH:mm', [Globalization.CultureInfo]::InvariantCulture,
                                   [Globalization.DateTimeStyles]::None, [ref]$time)) {
    throw "-At must be a 24-hour time HH:mm (got '$At')"
}
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    throw 'not elevated: installing the protected job folder and registering a task for another account need an elevated PowerShell.'
}

# ---- where the code comes from (review round 2) ---------------------------------------------------------------
# The installer and the job sources must be changeable only by administrators (and the admin running this): an
# elevated run of code the engine account can edit would give that account administrator rights, and the job
# folder would get its code with a matching SHA-256.
$JobDir = [System.IO.Path]::GetFullPath($JobDir).TrimEnd('\')
foreach ($tree in @($Root, $RepoRoot)) {
    $treeFull = [System.IO.Path]::GetFullPath($tree).TrimEnd('\') + '\'
    if (($JobDir + '\').StartsWith($treeFull, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "refused: -JobDir $JobDir is inside the repository ($tree), which is not an admin-only folder."
    }
}
# The parent folder gets an ACL too (nobody else may rename the job folder away), so it must be a folder that
# holds the job folder and nothing else (review round 2 of the home-server work: an earlier version re-ACL'd the
# whole parent recursively, so -JobDir C:\xtnl\backup-job would have locked the engine out of C:\xtnl\order-server).
# Refused: a parent that is a drive root, a system or profile folder (or inside %SystemRoot% or the profiles
# folder), or that holds anything other than the job folder.
$JobParent = Split-Path -Parent $JobDir
$JobLeaf = Split-Path -Leaf $JobDir
if (-not $JobParent -or [System.IO.Path]::GetPathRoot($JobDir).TrimEnd('\') -ieq $JobParent.TrimEnd('\')) {
    throw "refused: -JobDir $JobDir sits directly under a drive root; use a folder of its own, for example C:\Program Files\XTNL\backup-job."
}
$JobParent = $JobParent.TrimEnd('\')
$profilesDir = Split-Path -Parent $env:USERPROFILE
$systemFolders = @($env:ProgramFiles, ${env:ProgramFiles(x86)}, $env:ProgramW6432, $env:ProgramData, $env:SystemRoot,
                   $env:USERPROFILE, $env:PUBLIC, $profilesDir, $Root, $RepoRoot, (Split-Path -Parent $RepoRoot))
foreach ($f in $systemFolders) {
    if (-not $f) { continue }
    if ([System.IO.Path]::GetFullPath($f).TrimEnd('\') -ieq $JobParent) {
        throw "refused: the parent of -JobDir ($JobParent) is a shared folder ($f); its ACL would be replaced. Use a folder of its own, for example C:\Program Files\XTNL\backup-job."
    }
}
foreach ($f in @($env:SystemRoot, $profilesDir)) {
    if (-not $f) { continue }
    $fFull = [System.IO.Path]::GetFullPath($f).TrimEnd('\') + '\'
    if (($JobParent + '\').StartsWith($fFull, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "refused: the parent of -JobDir ($JobParent) is inside $f; use a folder of its own, for example C:\Program Files\XTNL\backup-job."
    }
}
$parentCreated = -not (Test-Path -LiteralPath $JobParent)
if (-not $parentCreated) {
    $others = @(Get-ChildItem -LiteralPath $JobParent -Force | Where-Object { $_.Name -ine $JobLeaf })
    if ($others.Count -gt 0) {
        throw ("refused: the parent of -JobDir ({0}) holds other items ({1}); its ACL would be replaced. Use a parent folder that holds only the job folder, for example C:\Program Files\XTNL\backup-job." -f $JobParent, (($others | Select-Object -First 5 | ForEach-Object { $_.Name }) -join ', '))
    }
}
foreach ($name in $JobFileNames) {
    $src = Join-Path $SourceDir $name
    if (-not (Test-Path -LiteralPath $src)) { throw "not found: $src" }
}
$me = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$sourcePaths = @($PSCommandPath, $PSScriptRoot, (Join-Path $SourceDir 'backup'), $Root)
foreach ($name in $JobFileNames) { $sourcePaths += (Join-Path $SourceDir $name) }
$offenders = @(Get-SourceOffenders $sourcePaths ($TrustedSids + @($me)))
if ($offenders.Count -gt 0) {
    throw ("refused: this installer or the job sources can be changed by other accounts ({0}). Run it from an admin-only clone, never elevated from the engine's checkout (runbook home-backup-restore.md Set-up 7)." -f ($offenders -join '; '))
}
Write-Host "sources: $SourceDir (installer and job files changeable by administrators only)"
$git = Get-Command 'git.exe' -ErrorAction SilentlyContinue
# Runs git in the clone; returns @{ Code; Out }. Windows PowerShell 5.1 turns a native command's stderr into error
# records, which 'Stop' would make fatal: git's own exit code decides here instead.
function Invoke-Git([string[]]$GitArgs) {
    $ErrorActionPreference = 'Continue'
    $out = @(& $git.Source -C $Root @GitArgs 2>$null)
    return @{ Code = $LASTEXITCODE; Out = $out }
}
$inGit = $false
if ($git) {
    $r = Invoke-Git @('rev-parse', '--is-inside-work-tree')
    $inGit = ($r.Code -eq 0 -and ($r.Out -join '') -eq 'true')
}
if ($inGit) {
    $rel = @($JobFileNames | ForEach-Object { 'deploy/local/' + ($_ -replace '\\', '/') })
    $r = Invoke-Git (@('status', '--porcelain', '--') + $rel)
    if ($r.Code -ne 0) { throw "git status failed in $Root (exit $($r.Code)); a clone owned by another account needs git's safe.directory" }
    if ($r.Out.Count -gt 0) {
        throw ("refused: uncommitted changes to job files in {0}: {1}. Install only a reviewed commit." -f $Root, ($r.Out -join '; '))
    }
    $head = ((Invoke-Git @('rev-parse', 'HEAD')).Out -join '')
    Write-Host "  commit (HEAD): $head  <- compare with the commit you reviewed"
    if ((Invoke-Git @('verify-commit', 'HEAD')).Code -eq 0) { Write-Host '  git verify-commit HEAD: good signature' }
    else { Write-Host '  git verify-commit HEAD: no valid signature (compare the commit id by hand)' }
} else {
    Write-Host '  WARNING: the sources are not a git work tree (or git is not installed): compare each SHA-256 below with the reviewed files yourself'
}
$changed = 0
foreach ($name in $JobFileNames) {
    $new = (Get-FileHash -LiteralPath (Join-Path $SourceDir $name) -Algorithm SHA256).Hash
    $dst = Join-Path $JobDir $name
    if (Test-Path -LiteralPath $dst) {
        $old = (Get-FileHash -LiteralPath $dst -Algorithm SHA256).Hash
        if ($old -eq $new) { Write-Host ("  unchanged {0} (SHA-256 {1})" -f $name, $new); continue }
        Write-Host ("  CHANGED   {0}: installed {1} -> new {2}" -f $name, $old, $new)
    } else {
        Write-Host ("  NEW       {0} (SHA-256 {1})" -f $name, $new)
    }
    $changed += 1
}
if ($changed -gt 0 -and -not $Yes) {
    Write-Host '  read the changes first (git diff <installed commit> HEAD -- deploy/local/backup-pg.ps1 deploy/local/docker-compose.local.yml deploy/local/backup)'
    $answer = Read-Host 'install these job files into the protected folder? type yes'
    if ($answer -ne 'yes') { throw 'not confirmed: nothing was copied.' }
}

# ---- the protected job folder -------------------------------------------------------------------------------
New-Item -ItemType Directory -Force -Path (Join-Path $JobDir 'backup') | Out-Null
foreach ($name in $JobFileNames) {
    $src = Join-Path $SourceDir $name
    $dst = Join-Path $JobDir $name
    Copy-Item -LiteralPath $src -Destination $dst -Force
    $a = (Get-FileHash -LiteralPath $src -Algorithm SHA256).Hash
    $b = (Get-FileHash -LiteralPath $dst -Algorithm SHA256).Hash
    if ($a -ne $b) { throw "copy of $name does not match its source (SHA-256 differs)" }
    Write-Host ("  copied {0} (SHA-256 {1})" -f $name, $a)
}
# Owner Administrators; no inherited entries; Administrators and SYSTEM full control, the backup account read and
# execute; nobody else. The parent too, so nobody else can rename the job folder away and put another in its place:
# on the parent folder OBJECT only (no /T, entries for this folder only, so nothing propagates to anything under
# it); the checks above make sure it holds nothing but the job folder. Recursive (/T) only inside the job folder.
$parentGrants = @('*S-1-5-32-544:F', '*S-1-5-18:F', ('{0}:RX' -f $User))
& icacls.exe $JobParent /setowner '*S-1-5-32-544' /C /Q | Out-Null
if ($LASTEXITCODE -ne 0) { throw "icacls /setowner failed on $JobParent (exit $LASTEXITCODE)" }
& icacls.exe $JobParent /inheritance:r /grant:r $parentGrants[0] $parentGrants[1] $parentGrants[2] /C /Q | Out-Null
if ($LASTEXITCODE -ne 0) { throw "icacls /grant failed on $JobParent (exit $LASTEXITCODE)" }
$grants = @('*S-1-5-32-544:(OI)(CI)F', '*S-1-5-18:(OI)(CI)F', ('{0}:(OI)(CI)RX' -f $User))
& icacls.exe $JobDir /setowner '*S-1-5-32-544' /T /C /Q | Out-Null
if ($LASTEXITCODE -ne 0) { throw "icacls /setowner failed on $JobDir (exit $LASTEXITCODE)" }
& icacls.exe $JobDir /inheritance:r /grant:r $grants[0] $grants[1] $grants[2] /T /C /Q | Out-Null
if ($LASTEXITCODE -ne 0) { throw "icacls /grant failed on $JobDir (exit $LASTEXITCODE)" }
$created = if ($parentCreated) { 'created now' } else { 'held only the job folder' }
Write-Host "parent folder $JobParent ($created): owner Administrators; this folder only, no inherited entries"
Write-Host "job folder $JobDir installed: owner Administrators; Administrators and SYSTEM full control, $User read and execute"

# ---- the task ----------------------------------------------------------------------------------------------
$BackupScript = Join-Path $JobDir 'backup-pg.ps1'
$PsExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$PsArgs = '-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}" -RepoRoot "{1}"' -f $BackupScript, $RepoRoot
$action = New-ScheduledTaskAction -Execute $PsExe -Argument $PsArgs -WorkingDirectory $JobDir
$trigger = New-ScheduledTaskTrigger -Daily -At $time
$settings = New-ScheduledTaskSettingsSet `
    -RestartInterval (New-TimeSpan -Minutes 15) -RestartCount 3 `
    -ExecutionTimeLimit (New-TimeSpan -Hours $TaskLimitHours) `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -Priority 7
$description = 'XTNL nightly encrypted off-site PostgreSQL backup (home-server plan W3): the protected copy of backup-pg.ps1 under the backup account. Read-only; never sends orders. Runbook docs\runbooks\home-backup-restore.md.'

if ($LogonType -eq 'S4U') {
    $principal = New-ScheduledTaskPrincipal -UserId $User -LogonType S4U -RunLevel Limited
    Register-ScheduledTask -TaskName $TaskName -Force -Description $description `
        -Action $action -Trigger $trigger -Settings $settings -Principal $principal | Out-Null
} else {
    # Task Scheduler needs the account's password to store it; it is held only in this process and never printed.
    $cred = Get-Credential -UserName $User -Message "Password of $User (stored by Task Scheduler for the 'XTNL backup' task)"
    if ($null -eq $cred) { throw 'no credential given' }
    Register-ScheduledTask -TaskName $TaskName -Force -Description $description `
        -Action $action -Trigger $trigger -Settings $settings `
        -User $cred.UserName -Password $cred.GetNetworkCredential().Password -RunLevel Limited | Out-Null
}

$t = Get-ScheduledTask -TaskName $TaskName
$s = $t.Settings
Write-Host "registered scheduled task '$TaskName' (replaces any earlier one)"
Write-Host ("  user:        {0} (logon type {1}, {2})" -f $t.Principal.UserId, $t.Principal.LogonType, $t.Principal.RunLevel)
Write-Host ("  trigger:     daily at {0} (local time)" -f $time.ToString('HH:mm'))
Write-Host ("  action:      {0} {1}" -f $t.Actions[0].Execute, $t.Actions[0].Arguments)
Write-Host ("  working dir: {0}" -f $t.Actions[0].WorkingDirectory)
Write-Host ("  limits:      {0} (time limit), restart on a failed launch every {1}, up to {2} times; priority {3}" -f `
    $s.ExecutionTimeLimit, $s.RestartInterval, $s.RestartCount, $s.Priority)
Write-Host '  logs:        <repository>\var\logs\backup-<date>.log (or %LOCALAPPDATA%\XTNL\backup-logs of the backup account)'
Write-Host ('  before the first night: as the backup account, run "{0}" -RepoRoot "{1}" -Check (runbook).' -f $BackupScript, $RepoRoot)
Write-Host '  after a reviewed change to a job file: pull it into the admin clone and run this script again from there (admin).'
if ($RunNow) {
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "started '$TaskName' now."
} else {
    Write-Host "not started now: it runs daily at $($time.ToString('HH:mm')), or: Start-ScheduledTask -TaskName '$TaskName'"
}
