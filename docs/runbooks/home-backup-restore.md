# Home server: nightly encrypted off-site backup and restore (single-PC mode)

The home PC's PostgreSQL is the single source of truth, and before this job it had no backup at all
(`deploy/jobs/xtnl-pg-backup.sh` and pgBackRest are AWS only). This runbook sets up and runs option A of the
home-server plan (`docs/design/home-server-plan.md` §3.2, work items W3 and W10): every night a run-once container
dumps PostgreSQL, checks the dump, encrypts it on the PC to an age **public** key and uploads it to a Backblaze B2
bucket with Object Lock. The PC never holds the decryption key, so a stolen PC cannot read its own backups. The
job only reads PostgreSQL and the partition archive folder; like the engine in Phase 1 it never sends orders
and touches no broker.

What is where:

| Piece | File |
|---|---|
| Job image (postgres:17 + age + rclone 1.69.1) | `deploy/local/backup/Dockerfile` |
| Job script (POSIX sh) | `deploy/local/backup/entrypoint.sh` |
| Compose service `backup`, profile `backup` (a plain `up` never runs it) | `deploy/local/docker-compose.local.yml` |
| Wrapper the task runs (PowerShell 5.1) | `deploy/local/backup-pg.ps1` |
| Scheduled task `XTNL backup` and the protected job folder | `deploy/local/install-backup-task.ps1` |
| The copy the task actually runs (admin-only folder) | `C:\Program Files\XTNL\backup-job\` (the four files above) |
| Partition archive folder (W10) | `XTNL_ARCHIVE_DIR` in `deploy/env/local.env` |

Layout in the bucket (every object is age-encrypted, next to a `.sha256` of the encrypted file):

```
daily/<UTC yyyyMMddTHHmmssZ>/xtnl-<ts>.dump.age        pg_dump -Fc of database xtnl
daily/<UTC yyyyMMddTHHmmssZ>/globals-<ts>.sql.age      pg_dumpall --globals-only --no-role-passwords
monthly/<yyyy-MM>/...                                  the same two files, first good run of each UTC month
archive/<table>/<partition>.jsonl.gz.<sha256 prefix>.tar.age   partition export + its sidecar, in one tar
```

**Not backed up** (plan §3.2): the market-data store's volume `xtnl_local_redis_md`, Docker's
`docker_data.vhdx` (which contains it), the engine Redis and `redis-api` (disposable, rebuilt by the engine).
RPO is 24 h: rows written after the last dump are lost on a restore (see Restore step 8). Run a backup by hand
before raising `XTNL_MIGRATIONS_ACCEPTED_UPTO` or any other change you might want to roll back.

## Trigger

* Setting up the home PC (plan §4, gate "before relying on the box"): Set-up 1-8 below, once.
* Healthchecks.io check C is red, or a `backup-pg exit` line other than 0 is in the log: Checks.
* The PC died, was stolen, or the disk failed: Restore.
* Every quarter: the restore drill (Quarterly drill).
* A key may have leaked (theft, a shared screen): Rotation.

## Prerequisites

* The single-PC engine runs as the engine account `xtnl` (docs/runbooks/single-pc-autostart.md), with Docker
  Desktop started in that account's session (autologon, plan §4.2). The repo is at `C:\xtnl\order-server`, outside
  OneDrive (plan §4.7).
* An admin account on the PC (used only to elevate), and your own computer (not the home PC) for the B2 account
  and the age key.
* A password manager, and paper for the age private key.
* A named second person who can approve an `epoch adopt` by phone (Restore step 5). Write the name here:
  `approver: ______________________`.

## Set-up (once)

### 1. B2 bucket, lifecycle rules and the upload key (on your own computer)

Create the bucket and keys from your own computer: the B2 master key never goes near the home PC. Web console:
Buckets > Create a Bucket: **Private**, **Object Lock: Enable** (only possible at creation), default encryption
SSE-B2 (optional, the files are age-encrypted anyway). Then on the bucket:

* **Object Lock default retention: Compliance, 367 days.** Nobody, you included, can delete or shorten a locked
  version before it expires, which is the point: ransomware or a thief with the upload key cannot destroy a
  backup inside its first 367 days. Why 367 and not 30 (review round 2): the upload key's `writeFiles` also lets
  its holder **hide** a file or upload a new version over its name, and the lifecycle rules then delete the old
  version once it is hidden and no longer locked. With a 30-day lock the 12 monthly backups (meant to last a year)
  could be destroyed with the upload key alone after their first 30 days. The lock must therefore be as long as
  the longest retention you count on: 366 days for `monthly/`, plus a day. The lock is per bucket, so the daily
  copies are kept 367 days too (see the cost note below). Plan §3.2 still says 30 days: this runbook is the
  corrected value (a plan update is pending).
* **Lifecycle rules** ("Use custom lifecycle rules"):

  | File name prefix | Days till hide | Days till delete (after hiding) |
  |---|---|---|
  | `daily/` | 31 | 337 |
  | `monthly/` | 366 | 2 |
  | `archive/` | no rule: kept | - |

  A daily copy is hidden after 31 days (so a listing of `daily/` shows the last 30) and deleted at day 368; a
  monthly copy is hidden after 366 and deleted at day 368: both just after the 367-day lock ends, so the
  lifecycle never has to delete a locked version in normal running. A version someone hides or replaces early
  stays (hidden) until its lock ends; B2 deletes it only then (**unverified**: whether B2's lifecycle retries
  such a version once its lock has ended, or it stays hidden for good; either way it is never deleted early).
  **Cost:** about 365 daily copies are stored instead of 30. With a dump of a few tens of MB that stays inside or
  near B2's free 10 GB (plan §3.2 cost row); check the bucket size in the B2 console monthly. If the dumps grow
  large, move `monthly/` to a second bucket with its own 367-day lock and go back to 30 days here (needs a second
  upload key and a job change: not built).
* **Application key for the upload** (App Keys > Add a New Application Key): access to **this bucket only**,
  capabilities `listBuckets`, `listFiles`, `writeFiles`, and **no** `deleteFiles`, no `writeFileRetentions`, no
  `bypassGovernance`. The job never deletes or overwrites: every object has a new name. Note the key id and the
  key (shown once) in the password manager. What its holder can still do: add objects (a forged backup among
  them: Restore step 2 picks the backup from an independent record), hide existing files or upload over their
  names (the old version is kept until its 367-day lock ends), and add storage cost. What they cannot do: delete
  a version, or shorten its lock.

The same with the `b2` command-line tool (v4; the option names have changed between versions, so check
`b2 bucket create --help` first; **unverified**):

```
b2 account authorize
b2 bucket create --file-lock-enabled --default-server-side-encryption SSE-B2 <bucket> allPrivate
b2 bucket update --default-retention-mode compliance --default-retention-period 367days `
  --lifecycle-rule '{"fileNamePrefix": "daily/", "daysFromUploadingToHiding": 31, "daysFromHidingToDeleting": 337}' `
  --lifecycle-rule '{"fileNamePrefix": "monthly/", "daysFromUploadingToHiding": 366, "daysFromHidingToDeleting": 2}' `
  <bucket>
b2 key create --bucket <bucket> xtnl-backup-upload listBuckets,listFiles,writeFiles
```

If the job's first `-Check` or run reports an authorisation error on upload, add `readFiles` to the key (some
rclone versions read back what they wrote; **unverified** for 1.69.1). `readFiles` only lets the key holder
download encrypted files.

### 2. age key pair (on your own computer)

```
age-keygen -o xtnl-backup.agekey
# prints:  Public key: age1...
```

* The **public** key (`age1...`) goes to the PC as `XTNL_BACKUP_AGE_RECIPIENT` (step 4). It can only encrypt.
* The **private** key is the `AGE-SECRET-KEY-1...` line in the file. Put it in the password manager **and** on
  paper (written or printed, kept with the BitLocker recovery key), then delete the file
  (`Remove-Item xtnl-backup.agekey`). Never copy it onto the home PC: `backup-pg.ps1` and the job both refuse to
  run when any variable holds an `AGE-SECRET-KEY-` value. Without this key no backup can be read, ever.

### 3. The backup account (admin)

A separate local Standard account, so the B2 key never enters the engine account's environment and neither the
engine, the API nor the live view can see it (plan §3.2):

```
net user xtnl-backup * /add                 # prompts for a password: keep it in the password manager
net localgroup docker-users xtnl-backup /add
```

* Not an administrator, not in Remote Desktop Users. Never sign in to it over RDP: there is only one interactive
  session on Windows Pro and you would disconnect `xtnl`. At the PC itself, **Switch user** (fast user switching)
  keeps the `xtnl` session running; from inside the `xtnl` session use `runas` (Set-up 4 says which to use when).
* **How it reaches Docker:** Docker Desktop runs in the `xtnl` session (it starts at `xtnl`'s sign-in). Members of
  `docker-users` may use its named pipe, so the backup account's `docker` commands reach the same engine and the
  same `xtnl-local` project. If `xtnl` is not signed in (after a reboot, before autologon), Docker is down and the
  backup fails with exit 10; check C then goes red.
* **Job folder.** The task never runs the repository's files. The repository belongs to `xtnl` (and, before the
  folder-rights step below, under the default `C:\` rights any authenticated user can write it), so anything running as `xtnl` (the engine, the
  network-facing API, the live view) could add a line to `backup-pg.ps1` or `entrypoint.sh` and have it run with
  the B2 key loaded. `install-backup-task.ps1` (Set-up 7) therefore copies the four job files to
  `C:\Program Files\XTNL\backup-job\` and sets its ACL: owner Administrators, Administrators and SYSTEM full
  control, `xtnl-backup` read and execute, nobody else. `backup-pg.ps1` refuses with exit 78 ("the job files are not
  protected") before it reads any backup variable when its own folder, the parent folder or one of the files can be
  changed by anyone else. The copy is only as good as its source, so the installer itself never runs from the
  engine's checkout: it runs elevated from an **admin-only clone** (Set-up 7), refuses to run when its own file or
  the job sources can be changed by anyone but administrators, refuses uncommitted changes to the job files, prints
  the commit and the per-file changes against the installed copy and asks you to type `yes`. After a reviewed
  change to a job file, pull it into the admin clone and run the installer again from there: the task runs the
  copy only.
* **Accepted deviation (the key and Docker).** Plan §3.2 says the engine, the API and the live view can never see
  the B2 key. That holds for their environments, files and the job's code, but not for Docker: the job runs on
  Docker Desktop in the `xtnl` session, and every `docker-users` member, `xtnl` included, controls that engine. While
  the job runs, anything running as `xtnl` could read the key from the container (`docker exec ... cat` of the
  tmpfs `rclone.conf`) or swap an image the job builds on. What the job does to narrow this: the secrets travel on
  the job's **stdin**, never as `-e` values (so `docker inspect` does not show them and no stopped container keeps
  them), they are never in the docker CLI's environment, rclone reads them from a 0600 file on tmpfs that is
  removed on exit, the check C URL never enters the container at all (`backup-pg.ps1` sends every check C ping
  itself, outside Docker as `xtnl-backup`; review round 2), and a timed-out container is removed with
  `docker rm -f`. The full fix is a separate runtime for the backup (native `pg_dump`, `age` and `rclone` under
  `xtnl-backup` against `127.0.0.1:55433`, with temp files in an ACL'd folder of the backup profile); it is
  recorded as a follow-up before P3 (operator decision). Until then the upload key's limits are what matter: no
  `deleteFiles`, no `readFiles` (unless Set-up 1 had to add it) and a 367-day compliance lock. So a stolen key can
  add objects, hide existing ones or upload over their names, but cannot delete any version inside its 367 days
  or read the backups (they are age-encrypted anyway). Residual risks recorded with this deviation (review round
  2): (a) a **forged backup**: age does not authenticate the sender and the `.sha256` beside an object is written
  by whoever uploaded it, so someone holding the upload key and the (public) age recipient can upload a newer
  `daily/<ts>/` that decrypts and checks fine; Restore step 2 therefore picks the backup from records the key
  cannot write (check C, which only `backup-pg.ps1` pings, and the B2 upload time), so a forged upload counts only
  inside a real run's window, and signing each dump with a key kept off the engine account comes with the
  separate runtime;
  (b) the **installer source**: the admin clone is trusted as far as the commit you compare it with (Set-up 7);
  (c) anything running as `xtnl` on the `xtnl` desktop can read that desktop's clipboard and keystrokes, which is
  why Set-up 4 sets the secrets from the backup account's own console session or types them, never pastes;
  (d) the **other direction: the backup account reaches the engine's files** (review round 3). Docker Desktop
  serves Windows bind mounts with the file rights of the user who runs it, which is `xtnl`. So `xtnl-backup`, as a
  `docker-users` member, can run `docker run -v C:/xtnl/order-server:/r ...` and, with `xtnl`'s rights, change
  `deploy\local\start-xtnl.ps1`, `src\` or `.venv` (that code then runs at the next engine start with the
  `STONE_X_*` credentials, `XTNL_DEADMAN_TOKEN` and check A's URL in its environment), or read
  `var\api-tls\server.key` and `deploy\env\local.env`. The folder rights below do **not** stop this: NTFS sees
  `xtnl`, not `xtnl-backup`. Until the separate runtime is built (it drops `xtnl-backup` from `docker-users`),
  the two accounts are **not** separated: the B2 key and the broker credentials can meet through Docker, and the
  account split only keeps the key out of the engine's environment and files. Whoever can sign in as
  `xtnl-backup` should be treated as able to act as `xtnl`.
* **Folder rights.** With Windows' default `C:\` permissions a folder made under `C:\` is readable by every local
  user and writable by every authenticated user. That default is wrong for both of the engine's folders, so take
  them off it here.
  * **The engine's checkout** (review round 4). Under the default, every local account could rewrite `src\` or
    `.venv` (that code runs at the next engine start with the `STONE_X_*` credentials in its environment) and
    read `var\api-tls\server.key` (the network-facing API's TLS key), `deploy\env\local.env`, `var\logs` and the
    spill file. The lock-down closes direct file access for every account but `xtnl` and the administrators,
    `xtnl-backup` included. It does **not** keep the B2 key and the broker credentials apart: `xtnl-backup`
    still reaches these files with `xtnl`'s rights through a Docker Desktop bind mount (accepted deviation, risk
    (d) above, until the separate runtime drops it from `docker-users`). So the checkout gets owner
    Administrators, Administrators and SYSTEM full control, `xtnl` modify, and no inheritance from `C:\` (no
    `Authenticated Users` or `Users` entries). Modify does not include changing the rights, and `xtnl` is not the
    owner, so `xtnl` cannot put the old entries back. Git then refuses the checkout for `xtnl` ("dubious
    ownership": its owner is not the user running git), so allow this one folder once, as `xtnl`:
    `git config --global --add safe.directory C:/xtnl/order-server`. Two narrow grants
    for the backup job, which reads only these two places of the checkout (`-RepoRoot`): create its own log files
    in `var\logs` (`CREATOR OWNER` then gives it modify on the files it creates, and nothing on the engine's logs),
    and read `deploy\env\local.env` (for its `XTNL_ARCHIVE_DIR` line; it holds no secret, `start-xtnl.ps1`
    refuses secrets in it). Without the `var\logs` grant `backup-pg.ps1` logs to the backup account's
    `%LOCALAPPDATA%\XTNL\backup-logs` instead, which the Checks commands below do not read.
  * **The archive folder** holds account data: `xtnl` writes it, `xtnl-backup` reads it.

  From an elevated PowerShell (**unverified** on the new box: run the `icacls` check lines and compare):

```
$repo = 'C:\xtnl\order-server'
New-Item -ItemType Directory -Force "$repo\var\logs" | Out-Null
icacls $repo /setowner "*S-1-5-32-544" /T /C /Q
icacls $repo /inheritance:r /grant:r "*S-1-5-32-544:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "xtnl:(OI)(CI)M" /T /C /Q
icacls "$repo\var\logs" /grant "xtnl-backup:(RX,WD)" "*S-1-3-0:(OI)(IO)M" /C /Q
icacls "$repo\deploy\env\local.env" /grant "xtnl-backup:(R)" /C /Q
icacls $repo                                   # expect only Administrators (F), SYSTEM (F), xtnl (M)
icacls "$repo\var\api-tls\server.key"          # the same three, no Users / Authenticated Users
icacls "$repo\var\logs"                        # the three, plus xtnl-backup (RX,WD) and CREATOR OWNER (OI)(IO)(M)
(Get-Acl $repo).Owner                          # expect BUILTIN\Administrators

New-Item -ItemType Directory -Force C:\xtnl\archive
icacls C:\xtnl\archive /inheritance:r /grant:r "SYSTEM:(OI)(CI)F" "Administrators:(OI)(CI)F" "xtnl:(OI)(CI)M" "xtnl-backup:(OI)(CI)RX"
icacls C:\xtnl\archive
```

  `S-1-3-0` is `CREATOR OWNER`. Run the `local.env` line again whenever `local.env` is created anew (a new file
  inherits only the checkout's three entries). After the lock-down, start the engine as `xtnl` once
  (`Start-ScheduledTask 'XTNL engine'`, then `deploy\local\status-xtnl.ps1`): the engine, the API and the live view
  must still start.

### 4. The backup account's User variables (as `xtnl-backup`)

**Where to type the secrets** (review round 2). A window opened with `runas` from the `xtnl` session still sits on
the `xtnl` desktop: the engine, the API and the live view run there too, and any process on that desktop can read
its clipboard (and, with more effort, its keystrokes); Windows clipboard history and cloud clipboard sync can keep
what was copied. So, in order of preference:

1. **Best: the backup account's own console session.** At the PC itself, Start > your account picture > **Switch
   user**, sign in as `xtnl-backup` (fast user switching leaves the `xtnl` session, Docker Desktop and the engine
   running; never use Remote Desktop for this, it disconnects `xtnl`), open `powershell -NoProfile`, run the block
   below, then sign out of `xtnl-backup`. Its clipboard is not the `xtnl` session's.
2. **Otherwise: runas from the `xtnl` session, and TYPE the secrets** (read them from the password manager on your
   phone or your own computer). Never paste them there. From a normal PowerShell in the `xtnl` session (not a
   packaged terminal):

   ```
   runas /user:xtnl-backup "powershell -NoProfile"
   ```

   If you pasted one anyway: overwrite the clipboard at once (`Set-Clipboard -Value ' '` in the `xtnl` session),
   clear the clipboard history (Win+V > Clear all), and keep Settings > System > Clipboard > **Clipboard history**
   and **Sync across your devices** off for the `xtnl` account. A pasted secret has leaked: a process on that
   desktop may have read it (review round 8). Rotate that secret **at once** and write a rotation note, as in
   Rotation, "If a backup secret leaked". Its earliest leak time is when you pasted it. Check C runs from then
   until the rotation are not trusted for a restore.

In the PowerShell running as `xtnl-backup` (its own profile), set the variables. The secret ones go through a
hidden prompt, so they never reach the screen, the PowerShell history or a file (the prompt does not protect the
clipboard: that is what the choice above is for):

```
function Set-Secret([string]$Name) {
    $s = Read-Host "$Name" -AsSecureString
    $b = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($s)
    try { [Environment]::SetEnvironmentVariable($Name, [Runtime.InteropServices.Marshal]::PtrToStringBSTR($b), 'User') }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($b) }
}
Set-Secret XTNL_BACKUP_B2_ACCOUNT      # the key id
Set-Secret XTNL_BACKUP_B2_KEY          # the application key
Set-Secret XTNL_BACKUP_HC_URL          # Healthchecks.io check C ping URL (step 5)
[Environment]::SetEnvironmentVariable('XTNL_BACKUP_B2_BUCKET', '<bucket>', 'User')
[Environment]::SetEnvironmentVariable('XTNL_BACKUP_AGE_RECIPIENT', 'age1...', 'User')
# optional: the archive folder when it is not the engine's XTNL_ARCHIVE_DIR / C:\xtnl\archive
# [Environment]::SetEnvironmentVariable('XTNL_BACKUP_ARCHIVE_HOST', 'C:\xtnl\archive', 'User')
# optional, plan 3.2 "Decision for you": 1 leaves out the table data of last_clean_marks (a partial_session FLAG
# on restore day); default 0 keeps it
# [Environment]::SetEnvironmentVariable('XTNL_BACKUP_EXCLUDE_MARKS', '1', 'User')
```

| Variable (backup account's User environment only) | Required | What |
|---|---|---|
| `XTNL_BACKUP_AGE_RECIPIENT` | yes | age public key `age1...` |
| `XTNL_BACKUP_B2_ACCOUNT` | yes | B2 application key id |
| `XTNL_BACKUP_B2_KEY` | yes | B2 application key (secret) |
| `XTNL_BACKUP_B2_BUCKET` | yes | bucket name |
| `XTNL_BACKUP_HC_URL` | recommended | Healthchecks.io check C (secret: it can record a fake good run; never leaves `backup-pg.ps1`) |
| `XTNL_BACKUP_EXCLUDE_MARKS` | no | `1` = `--exclude-table-data=last_clean_marks` |
| `XTNL_BACKUP_ARCHIVE_HOST` | no | archive folder, when not the engine's `XTNL_ARCHIVE_DIR` |

**Never** set any of them in the `xtnl` account or in `deploy\env\local.env`: `start-xtnl.ps1` refuses an env file
that assigns an `XTNL_BACKUP_*`, `RCLONE_*` or `B2_*` name and keeps them out of the engine, the API and the live
view. If one turns up there anyway (a `runas` slip; `start-xtnl.ps1` logs `warning: this account's environment
holds <names>`), delete it from the `xtnl` account and rotate every secret among them at once (Rotation, "If a
backup secret leaked"); deleting alone is not enough (review round 5). How they reach the job: `backup-pg.ps1` reads them from this account's User environment, removes the three
secret ones (`XTNL_BACKUP_B2_ACCOUNT`, `XTNL_BACKUP_B2_KEY`, `XTNL_BACKUP_HC_URL`) from its own environment, passes
the others by name (`-e NAME`) and writes the two B2 ones to the job's stdin (`NAME=value` lines, then `END`;
`XTNL_BACKUP_SECRETS=stdin`). `XTNL_BACKUP_HC_URL` never reaches the job (not `-e`, not stdin): `backup-pg.ps1`
sends every check C ping itself (step 5), and refuses a URL that is not `https://`. The job refuses a key or key
id with characters a B2 key never has, and a `XTNL_BACKUP_HC_URL` line on its stdin. Still in the `xtnl-backup`
PowerShell, check Docker access:

```
docker info --format "{{.ServerVersion}}"
```

### 5. Healthchecks.io check C

In the same Healthchecks.io project as checks A and B (plan §3.1): add check **C, "xtnl backup"**, period
**1 day**, grace **3 hours**, notifications to Pushover and email. Copy its ping URL into `XTNL_BACKUP_HC_URL`
(step 4). Only `backup-pg.ps1` pings it, never the job (review round 2): the job's container runs on the `xtnl`
session's Docker Desktop, where anything running as `xtnl` can read what the container gets (Set-up 3's accepted
deviation), and with the URL it could record a whole fake good run (`/start`, a forged upload, success) that
Restore step 2 would accept. `backup-pg.ps1` runs as `xtnl-backup` outside Docker, where `xtnl` cannot read its
memory. It pings `<url>/start` right before it starts the job, `<url>` (success) once when the job logs
`DUMP SAFE:` (the dump and globals are in `daily/<ts>/`), and `<url>/fail` when the night fails: Docker down, the
image build, a missing or malformed variable, the archive folder missing, any job exit other than 0, 65 or 66
before `DUMP SAFE:`, a timeout before it, or a job exit 0, 65 or 66 without that line (`backup-pg exit 70`). The
refusals that come before it reads the URL (wrong account, `STONE_X_*` visible, an age private key in the
environment, the job files not protected) ping nothing: check C turns red only after its period and grace. The URL
is a secret; it is never logged and never handed to a child process.

### 6. Partition archive folder (W10, as `xtnl`)

Set the engine's archive folder in `deploy\env\local.env` (the example `deploy/env/local-live.env.example` has the
line and its comment):

```
XTNL_ARCHIVE_DIR=C:\xtnl\archive
```

Then restart the engine (`deploy\local\stop-xtnl.ps1`, then `Start-ScheduledTask 'XTNL engine'`). How it is used:
the leader's lifecycle task (after it becomes leader, then every 6 h) writes
`C:\xtnl\archive\<table>\<partition>.jsonl.gz` and a `.sha256` sidecar for each `event_log`,
`account_snapshots`, `events_dlq_history` and `notification_outbox` partition whose month ended more than 90 days
ago, reads it back, records it in `partition_archive` and only then detaches the partition. It creates missing
folders itself (`mkdir` with parents, `src/db/lifecycle.py`), but the backup needs the folder from the start
(step 3). Without the setting nothing is ever detached and an info page "lifecycle: partitions past 90 days await
export" starts once a month is past 90 days (about **2027-02-12** for a database created on 2026-11-14). The files
are **plain** gzip JSONL of account data: never copy them anywhere as they are; the job age-encrypts each one
before upload. Details and the drop of a detached table: [partition-archive.md](partition-archive.md) (its
`aws s3 cp` step is for AWS; on the home PC the backup job is that copy).

### 7. Job folder and scheduled task (admin)

**Never run the installer elevated from the engine's checkout** (`C:\xtnl\order-server`): `xtnl`, and under the
default `C:\` rights every authenticated user, can change it, so an elevated run would install (or execute) their
code as administrator. Run it from an **admin-only clone** instead (review round 2; the installer refuses
otherwise). Once, from an elevated PowerShell, make the clone folder admin-only **before** cloning into it:

```
New-Item -ItemType Directory -Force C:\xtnl-admin | Out-Null
icacls C:\xtnl-admin /setowner "*S-1-5-32-544" /T /C /Q
icacls C:\xtnl-admin /inheritance:r /grant:r "*S-1-5-32-544:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" /T /C /Q
git clone <the repository's remote URL> C:\xtnl-admin\order-server     # from the remote, never from C:\xtnl\order-server
```

Each time (first install, and after a reviewed change to a job file), from an elevated PowerShell:

```
git -C C:\xtnl-admin\order-server pull --ff-only
git -C C:\xtnl-admin\order-server log -1 --format="%H %s"     # must be the commit you reviewed
powershell -NoProfile -ExecutionPolicy Bypass -File C:\xtnl-admin\order-server\deploy\local\install-backup-task.ps1 -RepoRoot C:\xtnl\order-server -User "$env:COMPUTERNAME\xtnl-backup"
icacls "C:\Program Files\XTNL\backup-job"     # expect only Administrators, SYSTEM (F) and xtnl-backup (RX)
```

The installer refuses when its own file, the job files or their folders can be changed by anyone but
Administrators, SYSTEM, TrustedInstaller or you; refuses uncommitted changes to the job files; prints the commit
(HEAD) and whether `git verify-commit` finds a good signature; prints each job file as `unchanged`, `NEW` or
`CHANGED` (installed SHA-256 -> new) and asks you to type `yes` (`-Yes` skips the question). Read the changes
before you answer (`git -C C:\xtnl-admin\order-server diff <commit installed last time> HEAD -- deploy/local/backup-pg.ps1 deploy/local/docker-compose.local.yml deploy/local/backup`).
Then it copies the job files to `C:\Program Files\XTNL\backup-job\`, checks each copy's SHA-256, sets the folder
ACL with `icacls` and registers the task with `-RepoRoot` (the engine's checkout: only its `var\logs` and the
`XTNL_ARCHIVE_DIR` line of `local.env` are read, never code). The ACL is recursive inside the job folder only; the
parent (`C:\Program Files\XTNL`) gets the same owner and entries on the folder itself, nothing below it. So keep
the default `-JobDir`: the installer refuses one whose parent is a drive root, `Program Files` itself,
`%SystemRoot%`, `%ProgramData%`, a user profile, `C:\xtnl`, or any folder that holds anything besides the job
folder (review round 2: `-JobDir C:\xtnl\backup-job` would otherwise have locked `xtnl` out of its checkout).
**Unverified** on the new box: the `icacls` lines on
`C:\xtnl-admin` (check with `icacls C:\xtnl-admin\order-server\deploy\local`: only Administrators, SYSTEM and your
admin account), and git's `safe.directory` prompt for a clone owned by Administrators. Residual (accepted
deviation, Set-up 3): the clone is trusted as far as the commit you compare it with.

Defaults: daily at **03:15** local time (`-At HH:mm` to change), away from the engine start (log-on, Windows
Update restarts in the Saturday window; the migration step uses `lock_timeout 2s`) and from the 17:00 New York
roll (07:00-09:00 Melbourne). Time limit **3 h**: above `backup-pg.ps1`'s own worst case (10 min Docker wait +
30 min build + 90 min job), so Task Scheduler never kills the wrapper before it removes a hung container and pings
`/fail`. Logon type **S4U**: runs without the account being signed in and stores no password; `-LogonType Password`
asks for the password once if S4U does not get Docker access or the User variables on your box (**unverified**
which one a fresh Windows 11 Pro needs: run the task once with `Start-ScheduledTask 'XTNL backup'` and read the
log). Task Scheduler > Enable All Tasks History (plan §4.9) shows its runs. Re-run the "each time" block after any
reviewed change to `backup-pg.ps1`, `docker-compose.local.yml` or `deploy/local/backup/`.

### 8. First run by hand (as `xtnl-backup`, in its PowerShell from Set-up 4)

```
$job = 'C:\Program Files\XTNL\backup-job\backup-pg.ps1'
powershell -NoProfile -ExecutionPolicy Bypass -File $job -RepoRoot C:\xtnl\order-server -Check
powershell -NoProfile -ExecutionPolicy Bypass -File $job -RepoRoot C:\xtnl\order-server
```

If `-Check` ends with exit 78 "the secrets on stdin did not end with END (is the job's stdin attached?)", the
job's stdin did not reach the container through `docker compose run -T` on your Docker version (**unverified**):
the job read end-of-input at once, before any secret line. Send the `[job]` lines with the Docker Desktop version.
An exit 78 "missing setting(s)" for the secret names is a different case: the `END` line arrived but a secret line
was missing, so check the secret names and values in Set-up 4. An exit 78 "unexpected line on stdin" is a third
cause: something before the first secret line. A UTF-8 byte order mark (written by Windows PowerShell when the
console input code page is UTF-8, `chcp 65001` or the Windows "Beta: Use Unicode UTF-8" option) is handled on
both sides since review round 3 (`backup-pg.ps1` starts the job with a BOM-free input encoding, and the job
drops a leading BOM), so if it still appears, check that the job folder holds the current files (re-run Set-up 7)
and send the `[job]` lines.

The first run builds the image (it downloads `postgres:17` layers if missing, `rclone/rclone:1.69.1` and the
Debian `age` package). `-Check` tests the settings, PostgreSQL and a bucket listing and uploads nothing;
it never pings check C. Then record the tool versions once (the age version depends on the Debian release under
`postgres:17` at build time):

```
docker compose -f 'C:\Program Files\XTNL\backup-job\docker-compose.local.yml' --profile backup run --rm --no-deps -T backup versions
```

Always the job folder's compose file here, by its full path (review round 3): a relative
`deploy\local\docker-compose.local.yml` does not resolve from the backup account's profile folder, and the
repository's copy would build the image tag the task uses (`xtnl-local-backup:1`) from files `xtnl` can change.

In the B2 web console check that `daily/<ts>/` holds four files and that each shows **a retention date 367 days
ahead (compliance)**, and that `monthly/<yyyy-MM>/` got the same four. Then restore this first backup onto the
spare before you trust it (Quarterly drill; plan §4.13 step 5).

## Checks

```
Get-Content C:\xtnl\order-server\var\logs\backup-$(Get-Date -Format yyyy-MM-dd).log -Tail 40   # or the backup account's %LOCALAPPDATA%\XTNL\backup-logs
Select-String -Path C:\xtnl\order-server\var\logs\backup-*.log -Pattern 'backup-pg exit' | Select-Object -Last 7
Get-ScheduledTaskInfo -TaskName 'XTNL backup' | Format-List LastRunTime, LastTaskResult, NextRunTime
```

A good night has `[backup-pg] healthcheck ping /start sent` before the job and ends with
`[job] ... xtnl-backup: backup <ts> done` and `backup-pg exit 0`; the job logs
`DUMP SAFE: dump and globals uploaded to daily/<ts>/` as soon as the daily upload is done, before the monthly
copy and the partition archive, and `backup-pg.ps1` logs `healthcheck ping success sent` right after it. `backup-pg exit 65` (refused archive files,
row 65) and `backup-pg exit 66` (a later step failed, row 66) are good nights for the dump; check C stays green for
both, so look for them in the `backup-pg exit` lines (`status-xtnl.ps1` lists only the engine's
`xtnl-*.log` alerts, not this log). The log holds names, counts, file names and times only: never a variable's value, an amount
or a row. Exit codes:

| Exit | Meaning | What to do |
|---|---|---|
| 0 | done (or another copy was running) | - |
| 1, 125, 126, 127 | `docker compose run` could not create or start the job container (network `xtnl-local_default` missing, a bind source refused, an image or entrypoint error); `backup-pg.ps1` pings `/fail` (it sends every check C ping; the job never pings) | Read the `[job]` lines; `docker compose -f deploy\local\docker-compose.local.yml ps` (the stack must be up); re-run Set-up 7 if a job file is missing |
| 10 | Docker not reachable within 10 min | Is `xtnl` signed in and Docker Desktop up (`status-xtnl.ps1`)? Is `xtnl-backup` in `docker-users`? |
| 11 | image build failed | Read the `[build]` lines; usually no internet or a Docker Hub hiccup: run again |
| 64 | job usage error | only after editing the scripts |
| 65 | the dump and globals **were** uploaded and check C recorded the night as good, but one or more partition archive files were refused (`ARCHIVE REFUSED` lines in the `[job]` output name each; `backup-pg.ps1` logs an `ALERT:` line and pings nothing). The other archive files were sent | As `xtnl`, per refused file. **`tar of ... failed`** with **No space left on device**: raise `shm_size` as in row 70 (one partition file must fit the tmpfs), then the next run sends it. **An unexpected name** (a hand-made copy such as `event_log_p202608 (1).jsonl.gz`): move it out of `C:\xtnl\archive`. **A malformed sidecar or a failed `.sha256` check** (the file is not in the bucket yet): move the file and its sidecar aside (`Rename-Item <file> <file>.stale`, the same for `<file>.sha256`; the job only picks up `*.jsonl.gz`), then get a fresh export: if the partition is still attached, `archive forget` it ([partition-archive.md](partition-archive.md) "Count mismatch", two people); if it is already detached, `archive forget` refuses, so first re-attach it as partition-archive.md "Rollback" says (**unverified** on the home PC), then forget it. Never drop such a detached table: until it is re-exported and uploaded, its only copy outside PostgreSQL is the nightly dump. **A file already in the bucket** is not hashed again (it is skipped), so bit rot on it never fails a night. If you find such a copy damaged later (a detached partition's file included; `archive forget` is not needed for it): move it and its sidecar aside as `.stale` as above, then put the bucket copy back with Restore step 6's loop (as `xtnl-backup`, from the job folder's compose file, the private key typed as step 2 says). Not urgent: the bucket copy is the off-site copy meanwhile. A sidecar damaged that way is refused every night (exit 65, never 70) until it is moved aside |
| 66 | the dump and globals **were** uploaded to `daily/<ts>/` and check C recorded the night as good (`DUMP SAFE:` line), but a later step failed: the monthly copy, the partition archive (a listing, an encryption, an upload, or an archive folder that could not be read), or the job ran over 90 min after the dump (its container was removed). The `[job]` lines show the failing step and a `LATE FAILURE (exit N)` line; `backup-pg.ps1` logs an `ALERT:` line and pings nothing | The night's dump is good and counts for Restore step 2. **`partition archive: cannot read /archive`** in the `[job]` lines (with `LATE FAILURE (exit 70)`): a file or folder under `C:\xtnl\archive` cannot be read through the Docker Desktop bind. Running again will not help, and no partition file reaches the bucket meanwhile while the engine keeps detaching exported partitions, so fix it the same day: check the rights of the folder and its subfolders (Set-up 3: `xtnl-backup` `(OI)(CI)RX`, inherited by every subfolder; `icacls C:\xtnl\archive /T` lists them), fix them, then confirm the next run logs `N file(s) uploaded` for the archive. Otherwise usually Internet or B2 trouble: run again by hand (a new `daily/<ts>/` and the missing monthly or archive objects are sent; nothing is overwritten). Repeated: as row 75 |
| 69 | PostgreSQL not reachable within 5 min (the job tries `pg_isready` every 10 s, so a run that starts right after a reboot or a power cut waits for postgres to finish starting or recovering) | `docker compose -f deploy\local\docker-compose.local.yml ps`; the engine's stack must be up |
| 70 | a dump failed or failed its `pg_restore -l` check, or an internal refusal. (An archive folder that cannot be read ends as 66, not 70: the archive step runs after the dump is safe, review round 3) | Nothing was uploaded for the failing piece. **No space left on device** in the `[job]` lines (`pg_dump failed`): the dump does not fit the tmpfs; raise `shm_size` (2 GB) in the compose service, then re-run Set-up 7 so the job folder gets the change (the restore container uses the same setting: Restore step 2). Otherwise run again. A damaged archive file, or an archive tar that does not fit the tmpfs, is exit 65, not 70 (review round 4) |
| 73 | the work folders (tmpfs `/dev/shm`, `/var/tmp`) could not be created or written | The container's tmpfs or `/var/tmp` is not writable: an edited compose service (a `read_only` root without the tmpfs, another user) or a broken image; rebuild it (`docker image rm xtnl-local-backup:1`, then run again) |
| 75 | upload or bucket listing failed before the dump was safe, or the job ran over 90 min before it (its container was removed with `docker rm -f`); a failure after the daily upload is 66 | Internet or B2 trouble: run again by hand. Repeated: check the key's capabilities |
| 78 | configuration | The log names the variable or the cause: wrong account (engine account, or `STONE_X_*` visible), the job files not protected (run the job folder's copy; re-run Set-up 7), a missing variable, a secret that did not arrive on stdin, an age private key in the environment, the archive folder missing. **Which ones ping:** a missing or malformed variable, the archive folder missing and the job's own 78 come after `backup-pg.ps1` has read the check C URL, so it pings `/fail` and check C turns red at once. The wrong account, `STONE_X_*` visible, an age private key in the environment and the job files not protected are refused **before** the URL is read (on purpose: no secret is loaded until the account and the job files pass), and so is an `XTNL_BACKUP_HC_URL` that is not `https://`: nothing is pinged, and check C turns red only after its period plus grace (about 27 h). After a change to the job folder or the backup account, read the next night's `backup-pg exit` line yourself |

Monthly (plan §5.1): in the B2 console, last night's objects exist and have their lock date.

## Restore

On the target PC (the spare for a drill, or a replacement): the repo, `.venv`, Docker Desktop as `xtnl` per plan
§4. Works the same on a fresh PC. Every step below reads the backup; the only writes are into the target's own
empty stack.

1. **No other engine anywhere.** The old PC is off, or its task is uninstalled and its broker variables removed
   (plan §4.12). Two engines would both alert and hide each other's death. On the target the `XTNL engine` task is
   **not** installed. Start the empty stack:

   ```
   cd C:\xtnl\order-server
   docker compose -f deploy\local\docker-compose.local.yml up -d --wait
   ```

2. **Download, check, decrypt: inside the job container** (PowerShell 5.1 corrupts binary pipes, and this keeps
   every plain file on the container's tmpfs). Create a **separate read key** for the bucket on your own computer
   (`listBuckets,listFiles,readFiles`; delete it after the restore). The service mounts the archive folder, so
   create `C:\xtnl\archive` first (Set-up 3; an empty folder is fine).

   **Which compose file** (review round 4). The container you type the private key into must be built from files
   only an administrator can change: always the protected job folder's copy, by its full path,
   `C:\Program Files\XTNL\backup-job\docker-compose.local.yml`, never `deploy\local\docker-compose.local.yml` of
   the engine's checkout. A relative path does not resolve from the backup account's profile folder anyway, and the
   checkout's copy would build `xtnl-local-backup:1` (the tag the nightly task runs) from a Dockerfile and
   entrypoint that `xtnl` can change: a wrapped `age`, `stty` or `/bin/sh` there would capture the identity, which
   decrypts every backup ever made and cannot be rotated for past objects. The job folder is readable only by
   Administrators, SYSTEM and `xtnl-backup`, so run steps 2 and 6 **as `xtnl-backup`**. On a PC without the job
   folder yet (a replacement, or a fresh spare), first do Set-up 3 (the backup account and folder rights; its User
   variables are not needed for a restore) and Set-up 7 (admin-only clone, installer) on this PC; on the drill
   spare, then remove the nightly task again with `install-backup-task.ps1 -Uninstall` (from the admin clone; it
   leaves the job folder in place). Before Set-up 7 has run, that admin-only clone (`C:\xtnl-admin`, through the
   installer) is the only source for the restore image: never build or run it from `C:\xtnl\order-server`, not even
   once "just for the restore". The backup account cannot read the admin clone itself (administrators only), which
   is why the installer's job folder is used. The `build --pull` line below rebuilds `xtnl-local-backup:1` from the
   job folder, so an image built earlier from other files (a run from a checkout, an earlier drill) is not reused.

   **Where to enter the age identity and the read key** (review round 3). The `AGE-SECRET-KEY-1...` line decrypts
   every backup, and Set-up 2 keeps it off the home PC. Set-up 4's rule applies to it, with more reason: anything
   on the desktop where you enter it can read that desktop's clipboard, and Windows clipboard history (Win+V) and
   **Sync across your devices** keep what was copied; on a real replacement the same `xtnl` desktop then runs the
   engine, the API and the live view (step 7). So, in order of preference:
   1. **TYPE** the identity (and the read key) from the paper or password-manager copy (your phone or your own
      computer): never copy or paste them on this PC.
   2. Run this step and step 6 from the backup account's own console session (**Switch user**, Set-up 4), which
      never hosts the engine, with clipboard history and sync off; or with `runas /user:xtnl-backup` from the
      `xtnl` session **before** that session has run anything else (and TYPE, as in 1).
   3. If you pasted anyway: at once overwrite the clipboard (`Set-Clipboard -Value ' '`), clear the clipboard history
      (Win+V > **Clear all**), and keep Settings > System > Clipboard > **Clipboard history** and **Sync across your
      devices** off for that account, all **before step 7 starts the engine**. On the drill spare do the same before
      you leave it.

   Then, as `xtnl-backup`, build the image from the job folder and open a shell in it:

   ```
   docker compose -f 'C:\Program Files\XTNL\backup-job\docker-compose.local.yml' --profile backup build --pull backup
   docker compose -f 'C:\Program Files\XTNL\backup-job\docker-compose.local.yml' --profile backup run --rm --no-deps --entrypoint sh backup
   ```

   In the container shell (it runs as `postgres`; `/dev/shm` and `/var/tmp` disappear with the container). The
   encrypted files are downloaded to `/var/tmp` (the container's disk; they are encrypted) and only the plain dump
   and globals go to the `/dev/shm` tmpfs, so the restore needs as much tmpfs as the nightly backup (one plain
   dump) and fits the same `shm_size` (review round 4). If the backup needed a larger `shm_size` (exit 70 row), the
   job folder's compose file carries it after Set-up 7. **A larger database** (a plain dump near 2 GB or above)
   needs a larger `shm_size` for this container too: about the plain dump's size plus 25 %. `docker compose run`
   has no `--shm-size` option (**unverified** for your Compose version: `docker compose run --help`), so raise
   `shm_size` in the compose service (repository, then admin clone), re-run Set-up 7, then this step.

   ```
   umask 077
   export RCLONE_CONFIG=/dev/shm/rclone.conf; : > "$RCLONE_CONFIG"
   export RCLONE_CONFIG_R_TYPE=b2
   printf 'restore key id: ';  read -r RCLONE_CONFIG_R_ACCOUNT; export RCLONE_CONFIG_R_ACCOUNT
   printf 'restore key: ';     stty -echo; read -r RCLONE_CONFIG_R_KEY; stty echo; echo; export RCLONE_CONFIG_R_KEY
   rclone lsf r:<bucket>/daily/ | tail -n 5          # candidates only: see "Which backup" below
   TS=<yyyyMMddTHHmmssZ>                             # the newest timestamp that matches a recorded good run
   mkdir -p /var/tmp/enc && cd /var/tmp/enc
   rclone copy r:<bucket>/daily/$TS/ /var/tmp/enc/
   sha256sum -c xtnl-$TS.dump.age.sha256 globals-$TS.sql.age.sha256
   stty -echo; cat > /dev/shm/identity; stty echo    # TYPE each needed AGE-SECRET-KEY-1... line (see below), Enter after each, then Ctrl+D
   age -d -i /dev/shm/identity -o /dev/shm/xtnl.dump xtnl-$TS.dump.age
   age -d -i /dev/shm/identity -o /dev/shm/globals.sql globals-$TS.sql.age
   rm -f xtnl-$TS.dump.age globals-$TS.sql.age && cd /dev/shm
   pg_restore -l xtnl.dump | grep -c 'TABLE DATA'    # the dump is readable
   grep -E '^(CREATE|ALTER) ROLE' globals.sql        # the roles the old cluster had (reference only)
   ```

   **Which private keys** (review round 4). `/dev/shm/identity` may hold several `AGE-SECRET-KEY-1...` lines, one
   per line; `age -d` tries each. After an age key rotation (Rotation), type the current key **and every older key
   on the password manager's key list** whose backups you need: a dump from before the rotation, and every
   `archive/` object (kept for ever), is encrypted to the key that was current when it was uploaded and fails to
   decrypt with any other.

   **Which backup** (review round 2). Never just take the newest folder. Whoever holds the upload key can upload a
   newer, forged `daily/<ts>/` with a matching `.sha256`, encrypted to the public age key, and both checks above
   would pass; `pg_restore` then runs its contents as the stack superuser. Pick `TS` from records the upload key
   cannot write, when you set `TS` in the block above (its `rclone lsf` line only lists candidates):
   * **Healthchecks.io check C** (its Events / log page): every good night has a `/start` ping followed by a
     success ping within the job's run time. It survives the PC. Only `backup-pg.ps1` can ping it: it runs as
     `xtnl-backup` outside Docker and never hands the URL to the job's container (Set-up 5), so someone running as
     `xtnl` who reaches the container gets the upload key but not the check C URL, and cannot record a run of
     their own (review round 2). A run normally starts at the task's time (`-At`, 03:15 plus the Docker wait), or
     when the PC is next on after a missed night, one per night; a `/start` at another time with no matching
     `Start-ScheduledTask` or manual run of yours is a warning sign: treat it like a folder with no matching
     good run.
   * **The upload time B2 recorded** for each file of the candidate folder (B2 web console, Browse Files: the
     "Uploaded" date). The uploader cannot set it. `rclone lsf --format tp` shows a modification time the
     uploader *can* set: do not use it for this.

   **The backup log is a hint only, never one of these records** (review round 3): it is `var\logs\backup-*.log` in
   the engine's checkout, which `xtnl` (and, without Set-up 3's lock-down, every authenticated user) can write, and
   the forged-backup attacker is exactly someone running as `xtnl` while the job holds the key (Set-up 3's accepted
   deviation), who can append a matching `backup <ts> done` and `backup-pg exit 0`. Use it to find the candidate
   night quickly; decide with check C and the B2 upload time.

   Restore the newest `TS` whose files were uploaded inside a run that check C records as good.
   A good run is one with a success ping after its `/start`: `backup-pg.ps1` sends it when the job logs
   `DUMP SAFE:`, as soon as the dump and globals are in `daily/<ts>/`, before the monthly copy and the partition
   archive. A night that ended with
   `backup-pg exit 65` (refused archive files, Checks row 65) or `backup-pg exit 66` (a later step failed, row 66)
   is such a run, so its `daily/<ts>/` counts as good. A `/fail` after that run's success ping does not undo it:
   what counts is a success ping between the run's `/start` and the next `/start`.
   Reject a folder with no matching good run, a second folder in one night's run, or files uploaded at another
   time than the rest of their folder, and report it as a possible key compromise (Rotation: B2 upload key).
   Judge the version you read, not only the newest one: a folder from a good run that is now hidden, or has a
   newer version on top, still holds its good version (see "Hidden or replaced files" below).
   Then look at the dump's table of contents before restoring it: `pg_restore -l xtnl.dump | grep -v '^;' | grep -v -E
   ' (TABLE|TABLE DATA|INDEX|CONSTRAINT|FK CONSTRAINT|SEQUENCE|SEQUENCE SET|SEQUENCE OWNED BY|DEFAULT|VIEW|COMMENT|SCHEMA|ACL|TRIGGER|FUNCTION|TYPE|EXTENSION|DATABASE PROPERTIES) '`
   shows anything else; compare a `FUNCTION` / `TRIGGER` list with the migrations (`src/db/migrations/`).
   **Unverified:** the exact Events wording in Healthchecks.io and the B2 console column name. The residual
   (a forged dump uploaded during a real run's window, by someone who reaches that run's container) is recorded
   in Set-up 3's accepted deviation.

   **Hidden or replaced files** (review round 7). The upload key can hide a file or upload a new version over its
   name (Set-up 1). The old version stays in B2 under its 367-day lock, and the restore key (`listFiles`,
   `readFiles`) can still read it. But plain `rclone lsf` and `rclone copy` read only the newest version that is
   not hidden. So a hidden folder does not show at all, `rclone copy` of it copies nothing and `sha256sum -c`
   fails; a replaced folder shows the new upload time. Before you reject a folder from a good run, or stop
   because none is left, look at its versions:

   ```
   rclone lsf --b2-versions r:<bucket>/daily/<ts>/            # every version, hidden ones too
   rclone lsf --b2-version-at <time> r:<bucket>/daily/        # the folders as they were at <time>
   rclone copy --b2-version-at <time> r:<bucket>/daily/$TS/ /var/tmp/enc/   # in place of the plain copy above
   ```

   * `--b2-versions` adds each older version to the list, with its B2 upload time in its name
     (`...-vYYYY-MM-DD-HHMMSS-mmm...`). The B2 web console's file versions view (Browse Files, show all
     versions) shows the same upload times. Choose the version whose upload time falls inside a good run.
   * `<time>` is in UTC, written like `2026-11-20T03:40:00Z`: a time just after that run's success ping, and
     before anything uploaded later. `--b2-version-at` then reads each file as it was at that time, even if it
     was hidden or replaced later. Use it in place of the plain `rclone copy` line, then go on with `sha256sum -c`.
   * Check the B2 upload time of every file you fetched (version list or console) against the good run, as
     above. Report the hidden or newer versions as a key compromise (Rotation: B2 upload key).
   * **Unverified:** the exact version-name format, the console's wording, and that `--b2-version-at` reads a
     file that was hidden after `<time>`. Try both flags in the restore drill.

   **A leaked check C URL** (review round 5). If the check C URL may have leaked (it was ever set in the `xtnl`
   account, in `local.env`, given to a job built before review round 2, **pasted into a window on the `xtnl`
   desktop** (a runas window included; Set-up 4), or **the PC was stolen**), its runs from the leak until its
   rotation prove nothing: whoever held the URL could record a fake good run around a forged upload, and
   regenerating the URL keeps the check's history, fake runs included. Rotating now, at restore time, does not
   make them trusted. Look up the leak window on the password manager's rotation note (Rotation, "If a backup
   secret leaked"); with no note, the window starts when check C was created and ends now. Treat every run inside
   it like a folder with no matching good run: take `TS` only from runs recorded after the rotation (or from
   before the leak, when its start is known), each still checked against the B2 upload times. A folder from a
   good run before the leak that now looks hidden, or carries a newer upload time, is not rejected for that: read
   it at its pre-leak version ("Hidden or replaced files" above). When no such folder exists, even among the
   hidden and older versions, stop and report it as a possible key compromise: do not restore a folder from
   inside the window.
   **After a theft** (review round 6) the window starts at the last time the PC was known to be in your hands
   (Rotation, "Theft of the PC"). Reject every check C run and every `daily/` folder after that time and before
   the rotation, however good it looks: the thief held the URL and the upload key, and the stolen box's own job may
   have run on a database the thief changed. Restore the newest good folder from before the theft. The thief may
   have hidden it or uploaded over it with the stolen key: then read it at its pre-theft version with
   `--b2-version-at` ("Hidden or replaced files" above). Do not reject it for looking hidden or for a newer
   upload time.

   Keep this shell open for step 3's `pg_restore`.

3. **Roles first, then the data.** Do **not** replay `globals.sql`: it contains the `xtnl` superuser, so it fails,
   and the API and live-view roles would come back without passwords. The dump holds the GRANTs to every role a
   migration creates, but neither the roles, their `ALTER ROLE ... SET` values nor their memberships (those are
   cluster-wide, in `globals.sql` only), so `pg_restore --exit-on-error` stops at the first GRANT to a missing role
   (`role "xtnl_arm" does not exist`) and every GRANT after it is missing too. Create the roles with the
   migrations' own statements, as in docs/runbooks/single-pc-autostart.md "Carry over" (0011 `xtnl_dashboard`,
   0014 `xtnl_api`, 0017 `xtnl_arm`, 0021 `xtnl_liveview`), from a second PowerShell on the host:

   ```
   Get-Content src\db\migrations\0011_dashboard_role.sql -TotalCount 30 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
   Get-Content src\db\migrations\0014_api_role.sql -TotalCount 30 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
   Get-Content src\db\migrations\0017_arm.sql | Select-Object -Skip 398 -First 15 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
   Get-Content src\db\migrations\0021_liveview_role.sql -TotalCount 32 | docker exec -i xtnl-local-postgres-1 psql -U xtnl -d xtnl -v ON_ERROR_STOP=1 -q
   ```

   Each line pipes the migration's role block through its last `ALTER ROLE` line (0011, 0014: line 30; 0021: line
   32), before the grants that name tables not restored yet. 0017's role block is its `DO $$ ... $$;` at lines
   399-413: it creates `xtnl_arm` and grants it to `xtnl` (`GRANT xtnl_arm TO current_user`), the membership the
   ARM pool's `SET ROLE xtnl_arm` needs and that is not in the dump. A line for a migration the dump predates is
   harmless (the role is created NOLOGIN; the later `src.db.migrate` run finds it and adds its grants), so run all
   four. Then in the container shell, into the stack's empty database, and leave the container:

   ```
   pg_restore -h postgres -U xtnl -d xtnl --no-owner --exit-on-error /dev/shm/xtnl.dump
   exit
   ```

   Back on the host: apply any newer migrations (use the accepted level of the old `local.env`) and compare row
   counts with the old PC (drill) or with what you expect:

   ```
   $env:ORDER_SERVER_DSN = 'postgresql://xtnl:xtnl_local_only@127.0.0.1:55433/xtnl'
   $env:XTNL_MIGRATIONS_ACCEPTED_UPTO = '<old value>'
   .venv\Scripts\python.exe -m src.db.migrate
   docker exec xtnl-local-postgres-1 psql -U xtnl -d xtnl -c "SELECT 'orders_state', count(*) FROM orders_state UNION ALL SELECT 'positions_state', count(*) FROM positions_state UNION ALL SELECT 'event_log', count(*) FROM event_log UNION ALL SELECT 'catchup_runs', count(*) FROM catchup_runs UNION ALL SELECT 'engine_runs', count(*) FROM engine_runs UNION ALL SELECT 'rulesets', count(*) FROM rulesets"
   ```

4. **etcd and the epoch fence.** A fresh etcd volume from the same compose file almost certainly gets the **same**
   etcd cluster_id (it comes from the initial cluster token and the peer URL) with its revision back near 1, so the
   engine's epoch bump changes 0 rows and the engine is **fenced**, and `epoch adopt` refuses with
   `already_adopted` because the cluster_id did not change. The fix is work item **W18**, documented in
   docs/runbooks/single-pc-autostart.md: rebuild etcd with a **new** initial cluster token as that runbook says:
   one that **no** earlier etcd volume used on **any** PC (the old desktop, the spare's earlier drills), because the
   cluster_id depends only on the token and the peer URL; use the date and time of the rebuild, `xtnl-local-rYYYYMMDD-HHMM` (a date alone repeats on a second rebuild the same day). Then check that the cluster_id differs and adopt it, audited, with a
   second person:

   ```
   $env:ORDER_SERVER_DSN = 'postgresql://xtnl:xtnl_local_only@127.0.0.1:55433/xtnl'
   $env:XTNL_ETCD_ENDPOINTS = 'http://127.0.0.1:52380,http://127.0.0.1:52380,http://127.0.0.1:52380'
   .venv\Scripts\python.exe -m src.ops epoch show       # etcd cluster_id must differ from the stored one
   .venv\Scripts\python.exe -m src.ops epoch adopt <new etcd cluster_id> --from-cluster-id <stored cluster_id> `
       --operator <you> --approver <second person> --reason "restore from B2 backup <ts>"
   ```

   (Only on a box-to-box move with both PCs at hand can you instead copy the etcd volume together with the
   PostgreSQL data from the same moment; an off-site backup has no etcd copy.)

5. **The approver.** The code refuses an approver name equal to the operator name, but it only compares the two
   strings and authenticates nobody. The second-person check is a procedure: phone the person named in
   Prerequisites, tell them the old and new cluster_id and the reason, and type their name only after they agree.

6. **Partition archive files** (only if `archive/` has objects): restore them into the archive folder so the
   `partition_archive` rows' `archive_uri` (`file:///C:/xtnl/archive/...`) point at real files. In a job container
   shell as in step 2, as `xtnl-backup` from the job folder's compose file (never the checkout's: step 2 "Which
   compose file"; it runs the image step 2's `build --pull` line made), with a writable bind of the folder, here
   `-v C:/xtnl/archive:/restore-archive`:

   ```
   docker compose -f 'C:\Program Files\XTNL\backup-job\docker-compose.local.yml' --profile backup run --rm --no-deps -v C:/xtnl/archive:/restore-archive --entrypoint sh backup
   ```

   (**Unverified:** whether Docker Desktop writes the bind as `xtnl`, whose session runs it (expected, and the
   reason for Set-up 3's risk (d)), or needs `xtnl-backup`
   to have write access to `C:\xtnl\archive`, which Set-up 3 gives it read access only. If the copy fails with a
   permission error, grant `xtnl-backup:(OI)(CI)M` on the folder for the restore and remove it afterwards.)

   **Driven by the ledger, not by the bucket listing** (review round 3). `archive/` can hold more than one object
   for one partition: a re-export after `archive forget` (exit 70 row), or an export written again after a failed
   `partition_archive` insert whose first copy was already uploaded, is sent as a new object with another sha256
   prefix. And whoever holds the upload key can add a forged one: age does not authenticate the sender, and the
   `.sha256` beside an object and the sidecar inside its tar are written by the uploader. Extracting every object
   would leave whichever came last in `find` order, and the sidecar check would still pass. So the loop below reads
   the restored database's `partition_archive` rows (step 3) and, for each, takes only the object named after the
   row's own checksum, `archive/<parent_table>/<partition_name>.jsonl.gz.<first 16 hex of checksum>.tar.age`,
   extracts only that partition's file from it, and checks the file's sha256 against the row's `checksum`
   (`sha256:<hex>`), not against a sidecar. The sidecar in the archive folder is then written from the row. Any
   other object is listed as `UNEXPECTED` and never extracted.

   In the container, after the rclone and identity lines of step 2 (TYPE every still-needed private key into
   `/dev/shm/identity`, one `AGE-SECRET-KEY-1...` line each: step 2 "Which private keys"; it reaches the stack's PostgreSQL like step 3's `pg_restore`; each object is decrypted to a file
   first, so a failed `age` is counted instead of hidden by a pipe). Only the objects the ledger names are
   downloaded, one at a time, to `/var/tmp` (review round 4: `archive/` never expires, so copying the whole prefix
   into the 2 GB tmpfs fails as it grows); the bucket listing (names only) finds the other objects.

   **Hidden or replaced archive objects** (review round 7). As in step 2, plain `rclone lsf` and `rclone copyto`
   read only the newest version that is not hidden. A hidden object then shows as `MISSING` or `FAILED`, and a
   replaced one fails the checksum against the ledger. After a theft or a leak of the upload key, or when the loop
   reports `MISSING` or `FAILED`, set `V` to `--b2-version-at <time>`: the same `<time>` you used for the dump in
   step 2 (just after its good run). Every object the restored ledger names was uploaded before that run, so it is
   read at its genuine version. `rclone lsf --b2-versions -R "$B/archive/"` lists every version, hidden ones too.
   Leave `V` empty in a normal restore.

   ```
   B=r:<bucket>                                      # your bucket name after r:
   V=                                                # or V="--b2-version-at <time>" (see above); no spaces in <time>
   rclone lsf $V -R --files-only "$B/archive/" > /dev/shm/remote.lst || echo "LISTING FAILED: run this step again"
   psql -X -A -t -F ' ' -v ON_ERROR_STOP=1 -c "SELECT parent_table, partition_name, checksum, CASE WHEN to_regclass(quote_ident(partition_name)) IS NULL THEN 'dropped' ELSE 'in-db' END FROM partition_archive ORDER BY 1, 2" > /dev/shm/ledger.txt
   mkdir -p /var/tmp/archive && cd /var/tmp/archive
   n=0; ok=0; bad=0; miss=0; : > /dev/shm/used.lst
   while read -r parent part sum where; do
       n=$((n + 1)); hex=${sum#sha256:}; rel="$parent/$part.jsonl.gz"
       obj="$rel.$(printf '%s' "$hex" | cut -c1-16).tar.age"
       echo "$obj" >> /dev/shm/used.lst
       if ! grep -Fxq "$obj" /dev/shm/remote.lst; then
           if [ "$where" = in-db ]; then miss=$((miss + 1)); echo "MISSING $obj (its table is still in the database)"
           else bad=$((bad + 1)); echo "FAILED $obj: not in the bucket and its table was dropped"; fi
           continue
       fi
       rm -rf /dev/shm/x; mkdir /dev/shm/x; mkdir -p "$(dirname "$obj")"
       if rclone copyto $V "$B/archive/$obj" "$obj" < /dev/null && rclone copyto $V "$B/archive/$obj.sha256" "$obj.sha256" < /dev/null \
           && (cd "$(dirname "$obj")" && sha256sum -c --quiet "$(basename "$obj").sha256") \
           && age -d -i /dev/shm/identity -o /dev/shm/part.tar "$obj" \
           && tar -x -C /dev/shm/x -f /dev/shm/part.tar "$rel" \
           && [ -f "/dev/shm/x/$rel" ] && [ ! -L "/dev/shm/x/$rel" ] \
           && [ "$(sha256sum < "/dev/shm/x/$rel" | cut -c1-64)" = "$hex" ] \
           && mkdir -p "/restore-archive/$parent" && cp "/dev/shm/x/$rel" "/restore-archive/$rel" \
           && printf '%s  %s\n' "$hex" "$part.jsonl.gz" > "/restore-archive/$rel.sha256"; then ok=$((ok + 1))
       else bad=$((bad + 1)); echo "FAILED $obj"; fi
       rm -rf /dev/shm/part.tar /dev/shm/x "$obj" "$obj.sha256"
   done < /dev/shm/ledger.txt
   grep '\.tar\.age$' /dev/shm/remote.lst | sort > /dev/shm/all.lst; sort /dev/shm/used.lst > /dev/shm/used.srt
   comm -23 /dev/shm/all.lst /dev/shm/used.srt | sed 's/^/UNEXPECTED (not extracted) /'
   unexp=$(comm -23 /dev/shm/all.lst /dev/shm/used.srt | wc -l | tr -d ' ')
   echo "ledger rows: $n, restored: $ok, failed: $bad, missing: $miss, unexpected: $unexp"   # failed must be 0
   cd /restore-archive && for s in $(find . -name '*.sha256'); do (cd "$(dirname "$s")" && sha256sum -c "$(basename "$s")"); done
   ```

   * `FAILED`: a damaged download, a missing private key (an object uploaded before an age key rotation needs
     the key of its time: step 2 "Which private keys"), or an object whose content does not match the ledger's
     checksum (a forged or stale object under the right name). Run the loop again; never count the drill as passed
     while `failed` is not 0 (after a detached partition was dropped, these files are its only copy).
   * `MISSING`: the row's object is not in the bucket, but the partition's table is still in the database (so in the
     dump): it was exported after the last backup run. Not a failure; the next nightly run uploads it.
   * `UNEXPECTED`: an object no ledger row names. Expected cases: a stale export (the partition was re-exported;
     the ledger names the newer one), and an export whose `partition_archive` row is newer than the dump (the job
     dumps first and uploads the archive after; such a partition was not yet detached, so its rows are in the dump).
     Check each against `archive forget` entries in `operator_audit` and the partition list; any other one is a
     possible upload-key compromise (Rotation: B2 upload key). It is never extracted, so it cannot replace a file
     the ledger names.

   (A table detached after its export is still a table in the database until an operator drops it, so it is in
   the dump too.)

7. **Start the engine** (a real replacement only; on the drill spare there are no broker credentials, plan §5.3):
   set the Windows User environment and `deploy\env\local.env` per plan §4.8, then
   `deploy\local\install-autostart.ps1 -StartNow`. The `startup` catch-up re-reads broker history from
   `adapter_coverage.last_streaming_ms - 900 s`; wait until it reports complete:

   ```
   .venv\Scripts\python.exe -m src.ops catchup show
   ```

   **Credentials:** the restored roles have no passwords. Run, then restart the task:

   ```
   powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-liveview-credentials.ps1 -Rotate
   powershell -NoProfile -ExecutionPolicy Bypass -File deploy\local\new-api-credentials.ps1 -DbRole
   ```

   A bare run of the API script does not reset the role; `-DbRole` does. Delete the restore read key in B2.

   **Tailnet tag** (review round 3): a replacement box signed in to Tailscale under your login is an untagged
   member device, so the receiver policy gives it port 22 and Tailscale SSH to the receiver VPS, and its beats
   still succeed over 443, so nothing shows the gap. In the Tailscale admin console, **Machines**: on the
   replacement box **Edit ACL tags** and set `tag:xtnl-home`, and **Remove** the old box from the tailnet. Then,
   from the replacement box, repeat the refusal checks of [deadman-receiver-home.md](deadman-receiver-home.md)
   step 3 (`ssh <user>@vps-a` refused, ports 22 and 8443 closed). Set check A's ping URL
   (`XTNL_HEARTBEAT_COPY_URL`) in its User environment too (after a theft, a new one: Rotation).

8. **Lost for good:** audit rows written after the dump (`oversee_events`, `halt_events`, `operator_audit`,
   `arm_decisions`, `engine_runs`). Acceptable while Phase 1 is read-only; not once approvals exist, which is why
   WAL archiving (option B, W4) comes before P3. Broker history (orders, positions, cash flows) comes back through
   the catch-up.

## Quarterly drill

Plan §5.3, on the spare, every quarter and before a long holiday:

* **No live credentials on the spare:** no `STONE_X_*`, no `XTNL_DEADMAN_TOKEN`, no `XTNL engine` task. Without
  `STONE_X_*` even a mistaken `start-xtnl.ps1` exits 78.
* Follow Restore steps 1-6 against the spare with last night's backup, including the new etcd token and
  `epoch adopt` with the approver's phone call (steps 4-5). Do not start the engine against live brokers unless the
  main PC is off.
* **Record** (in your own notes, never in a page or a chat): the time to restore, the row counts against
  production, whether `epoch adopt` and the approver call worked, the tool versions.
* **A failed drill means the backups don't count** until a drill passes. Fix the cause (this runbook, the scripts
  or the keys) and run the drill again.
* Afterwards remove the restored data from the spare, on the **spare only**, never on the production PC:
  * the database volumes: `docker compose -f deploy\local\docker-compose.local.yml down -v`;
  * the decrypted partition archive files that step 6 copied into `C:\xtnl\archive` (plain gzip JSONL of
    account data; `down -v` removes Docker volumes only, not this host folder):
    `Get-ChildItem C:\xtnl\archive -Recurse -File | Remove-Item -Force`, then check that
    `(Get-ChildItem C:\xtnl\archive -Recurse -File).Count` is 0;
  * the read key.

## Rotation

* **B2 upload key** (theft, a leak, yearly): create a new key with the same capabilities (Set-up 1), set
  `XTNL_BACKUP_B2_ACCOUNT` / `XTNL_BACKUP_B2_KEY` in the backup account (Set-up 4), run the job folder's
  `backup-pg.ps1 -RepoRoot C:\xtnl\order-server -Check` (Set-up 8), then delete the old key in B2. Locked objects
  are not affected.
* **Healthchecks.io URL:** regenerate check C's ping URL, update `XTNL_BACKUP_HC_URL`. Regenerating keeps the
  check and its history: runs recorded with the old URL stay there.
* **If a backup secret leaked** (review round 5; for example `start-xtnl.ps1` logged `warning: this account's
  environment holds <names>`, a secret was set in `local.env`, or a secret was **pasted into a window on the
  `xtnl` desktop**, a runas window included, as in Set-up 4): do it **at once**, not at restore time.
  1. Delete the names from the account that should not hold them (in that account:
     `[Environment]::SetEnvironmentVariable('<name>', $null, 'User')`). After a paste there is nothing to delete:
     the secret is in `xtnl-backup`, where it belongs. Go to step 2.
  2. Rotate every secret among them: the Healthchecks.io URL and the B2 upload key above. `XTNL_BACKUP_B2_BUCKET`
     and `XTNL_BACKUP_AGE_RECIPIENT` are not secret.
  3. Write a rotation note next to the key list in the password manager: which secret, the earliest time it may
     have leaked (when unsure, when check C was created; after a theft, the last time the PC was known to be in
     your hands) and the time of the new URL. Not in the engine's checkout
     or its logs: `xtnl` can write those.
  4. Check C runs recorded inside that window are never trusted for a restore (Restore step 2, "A leaked check C
     URL"). Runs recorded before the window stay trusted. The rotation only protects runs recorded after it.
     Until the next good run after the rotation, the newest trusted backup is the last good one before the leak,
     if its start is known. If that folder was hidden or uploaded over with the leaked key, read it at its
     pre-leak version (Restore step 2, "Hidden or replaced files").
  5. When the names were only non-secret ones (`XTNL_BACKUP_B2_BUCKET`, `XTNL_BACKUP_AGE_RECIPIENT`,
     `XTNL_BACKUP_EXCLUDE_MARKS`, `XTNL_BACKUP_ARCHIVE_HOST`; `start-xtnl.ps1` then logs `warning: non-secret
     backup names found in this account's environment`), step 1 is all: nothing needs rotating, no rotation note
     is needed and no check C run is distrusted.
* **age key** (the private key may have leaked): generate a new pair (Set-up 2) and change
  `XTNL_BACKUP_AGE_RECIPIENT`. Keep the **old** private key (password manager and paper) until every backup
  encrypted to it has expired: 368 days for `daily/` and `monthly/`, and **for ever** for `archive/`. The job never
  re-sends an archive file under a new key (each object is named after the file's sha256 and skipped once it is in
  the bucket, and the upload key cannot delete), so an archive object stays encrypted to the key of its upload day;
  there is no re-upload procedure. Keep a **key list** next to the keys in the password manager, one line per key:
  the date it became current, the date it was replaced, and its public key (`age1...`). A restore types every key
  on that list whose backups it needs (Restore step 2 "Which private keys", and step 6).
* **Theft of the PC** (plan §5.4): the backups are off-site, encrypted and locked, and the PC never held the
  decryption key. **Treat the theft as a leak** of `XTNL_BACKUP_HC_URL` and the B2 upload key (review round 6):
  both sit in `xtnl-backup`'s User environment on the stolen disk, and the box boots without a PIN (TPM-only
  BitLocker, autologon). Follow "If a backup secret leaked" above, at once. Its rotation note's window starts at
  the **last time the PC was known to be in your hands**, not at the time you found out. Every check C run and
  every `daily/` folder from that time until the rotation is rejected for a restore (Restore step 2, "A leaked
  check C URL"): it came from the thief, or from the stolen box's own job on a database the thief could change,
  so it proves nothing. If the thief hid the pre-theft folders or uploaded over them, read them at their
  pre-theft version (Restore step 2, "Hidden or replaced files"): the locked versions are still in B2. Rotate the
  B2 key and the check C URL at once, with the engine's own list (StoneX read-only
  credentials, dead-man and receiver tokens, API secret). Rotate the engine's **alert channel credentials** too
  (review round 4): they were in the same Windows User environment. That is the Pushover application token and
  user key (`XTNL_PUSH_TOKEN` / `XTNL_PUSH_USER`), the SMTP relay login (`XTNL_SMTP_USERNAME` /
  `XTNL_SMTP_PASSWORD`) or the SES key (`XTNL_SES_*`), and, when the ARM is on, `XTNL_ARM_PUSH_*`,
  `XTNL_ARM_SMTP_*` and `XTNL_ARM_SES_*`. Whoever holds them can send false alerts, misuse the relay or use up the
  Pushover message quota. Make the new values in Pushover, the relay or AWS, set them in the replacement box's
  `xtnl` User environment, and revoke the old ones. The VPS receiver has its own Pushover token and relay login
  ([deadman-receiver-home.md](deadman-receiver-home.md) Prerequisites), so it keeps paging; if it ever shared
  the engine's, rotate the receiver's `XTNL_RECEIVER_PUSH_*` / `XTNL_RECEIVER_SMTP_*` as well. Regenerate **check A's ping URL** too
  (`XTNL_HEARTBEAT_COPY_URL`, review round 3): it was in the stolen box's Windows User environment, and whoever
  holds it can keep check A green while the new engine is dead. Regenerate it in Healthchecks.io, then set the new
  URL in the replacement box's `xtnl` User environment ([deadman-receiver-home.md](deadman-receiver-home.md)
  step 7). Remove the stolen node from the tailnet (Tailscale admin console, **Machines**, the old box, **Remove**),
  and tag the replacement as Restore step 7 says.

## Rollback

Stop the nightly backup (the bucket and its locked objects stay; they expire through the lifecycle rules):

```
powershell -NoProfile -ExecutionPolicy Bypass -File C:\xtnl-admin\order-server\deploy\local\install-backup-task.ps1 -Uninstall   # elevated, from the admin clone (Set-up 7)
docker image rm xtnl-local-backup:1
Remove-Item -Recurse -Force 'C:\Program Files\XTNL\backup-job'     # admin; the job folder copy
```

Then, as `xtnl-backup` (Set-up 4), clear its variables
(`[Environment]::SetEnvironmentVariable('XTNL_BACKUP_B2_KEY', $null, 'User')` and the others), delete the upload
key in B2, and pause check C. Removing `XTNL_ARCHIVE_DIR` from `local.env` stops further exports (already
detached partitions stay detached; their files stay in the folder). Nothing here changes the engine, its data or
its task.

## Unverified on the home PC (check during Set-up 7-8 and the first drill)

* S4U versus Password logon for the task: Docker named-pipe access and the User variables under S4U.
* That `docker compose run -T` passes the wrapper's stdin to the job (the secrets): `-Check` fails with exit 78
  if it does not, so nothing runs without them.
* `icacls` with `(OI)(CI)` on the copied files, and the this-folder-only entries (no `/T`) on the parent
  `C:\Program Files\XTNL`; `backup-pg.ps1`'s own ACL check then has to pass (it logs "job files protected").
* Check C pinged from `backup-pg.ps1` (`Invoke-WebRequest` under S4U, no profile loaded): the first night's log
  must show `healthcheck ping /start sent` and `healthcheck ping success sent`, and check C's Events the same two.
* `docker compose` loading the copied compose file from `C:\Program Files\XTNL\backup-job` (other services'
  relative paths are only parsed, never used, by `run backup`).
* The `b2` CLI option names, and whether rclone 1.69.1 needs `readFiles` on the upload key.
* The archive folder ACL (`icacls`) and the default `C:\` inheritance on the new box.
* Set-up 3's lock-down of the engine's checkout: not run on the box yet. Check its `icacls` and owner lines, that
  the engine, the API and the live view still start, that git works for `xtnl` after `safe.directory`, and that
  the backup log lands in `var\logs`.
* The age version the image gets (Debian release under `postgres:17` at build time).
* Restore step 6's loop (tested with stub tools only; its `psql` ledger query against a real restore), the 0017
  role step of Restore step 3, and the W18 etcd step (proved in the first drill, plan §6 W18).
