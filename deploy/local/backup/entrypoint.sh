#!/bin/sh
# XTNL local backup job (home-server plan 3.2 option A, work item W3). Runs inside the `backup` service of
# deploy/local/docker-compose.local.yml (compose profile `backup`), started by deploy/local/backup-pg.ps1 under
# the separate backup Windows account. Runbook: docs/runbooks/home-backup-restore.md.
# Phase 1 is read-only and so is this job: it only reads PostgreSQL and the partition archive folder.
#
# A backup run, in order (a failing step up to the daily upload ends the run with a non-zero exit; a failure after
# it is exit 66, see step 8):
#   1. checks its settings (only variable NAMES are ever printed, never a value) and waits for postgres:5432: it
#      tries pg_isready every 10 s for up to 5 min (review round 1: a run started right after a reboot or a power
#      cut found postgres still starting or recovering, failed at once with 69 and lost the night);
#   2. pg_dump -Fc of the database into the container's tmpfs (/dev/shm), never onto a disk;
#   3. pg_restore -l of that file: a dump that cannot be listed, or lists no table data, aborts the run before
#      anything is uploaded;
#   4. pg_dumpall --globals-only --no-role-passwords (the roles, without passwords), also on tmpfs;
#   5. age-encrypts both to the PUBLIC key in XTNL_BACKUP_AGE_RECIPIENT (the private key is never on this PC, so
#      the PC cannot read its own backups), deletes each plain file at once, and writes a .sha256 of each
#      encrypted file (sha256sum -c format);
#   6. uploads the encrypted files and their .sha256 with rclone to <bucket>/daily/<UTC timestamp>/ and logs
#      `DUMP SAFE:` at once (backup-pg.ps1 sends check C's success ping when it reads that line). The first
#      successful run of a UTC month (no dump in <bucket>/monthly/<YYYY-MM>/ yet) then uploads them there too;
#   7. partition archive (W10, XTNL_ARCHIVE_DIR mounted read-only at /archive): every *.jsonl.gz whose .sha256
#      sidecar exists and checks out, and that is not in <bucket>/archive/ yet, is packed with its sidecar into a
#      tar on tmpfs, age-encrypted, and uploaded to archive/<parent>/<file>.<sha256 prefix>.tar.age (+ .sha256).
#      The bucket listing is the record of what was sent: the object name comes from the sidecar, so an object
#      whose .sha256 is already there is skipped BEFORE the file is hashed again (review round 4: a file sent long
#      ago that later rots on disk is not re-checked every night), and a re-export with other content has another
#      sha256 prefix, so it is sent as a new object. A file without its sidecar is still being exported by the
#      engine and waits for the next run. A file that cannot be sent (an unexpected name, a malformed sidecar, a
#      failed sha256 check, a failed tar) is REFUSED: logged and counted, never uploaded, and the other files go on;
#   8. never pings Healthchecks.io check C and never sees its URL (home-server review round 2): whoever holds the
#      URL can record a whole fake "good run" (/start, success), and anything running as the engine account can
#      reach this container (Accepted deviation). backup-pg.ps1, outside Docker under the backup account, sends
#      /start before it starts this job, the success ping when it reads the `DUMP SAFE:` line, and /fail for any
#      other outcome. Success means the dump and the globals were uploaded, so `DUMP SAFE:` is logged as soon as
#      daily/<ts>/ is complete (step 6), and nothing later in the run counts as a failed night: refused archive
#      files (review round 4) exit 65, and any later failure (a monthly or archive listing or upload, an archive
#      encryption, a signal; home-server review round 1) exits 66, both after `DUMP SAFE:`, so the wrapper logs an
#      ALERT: line, the task shows the code and the restore still takes that night's dump.
# Only encrypted outputs (*.age and the *.age.sha256 next to them, under the output folder) are ever uploaded:
# upload() refuses anything else, and encrypt() checks the age header before the plain file is removed. Plain
# files live only on tmpfs; the EXIT trap removes the work and output folders whatever happens.
# Only PostgreSQL and the partition archive folder are backed up. Plan 3.2 "Do not back up": the market-data
# store, its Docker volume and the engine / API caches are disposable or rebuilt from the broker.
#
# Secrets: XTNL_BACKUP_B2_ACCOUNT and XTNL_BACKUP_B2_KEY (never the check C URL, step 8). backup-pg.ps1 sets
# XTNL_BACKUP_SECRETS=stdin and writes them on the job's stdin (NAME=value lines, then END), so they are never in
# the container's configuration (`docker inspect` shows every -e value to any docker-users member) and never on a
# command line. Without XTNL_BACKUP_SECRETS=stdin (a manual run) they are read from the environment. Either way
# they are kept in shell variables only (not exported, so no child process inherits them): the rclone remote is
# written to a 0600 rclone.conf on the tmpfs work folder (removed on exit; no rclone.conf with the key exists in
# the image or on a volume). Accepted limit (runbook
# "Accepted deviation"): Docker Desktop runs in the engine account's session, so anything with Docker access can
# still read the key from the running container (docker exec); this only narrows the exposure.
# The upload key needs no delete right: every object gets a new name, uploads use --no-check-dest, and nothing
# is ever deleted or overwritten here; the bucket's Object Lock and lifecycle rules do the retention.
#
# Settings (environment): XTNL_BACKUP_AGE_RECIPIENT (age1...), XTNL_BACKUP_B2_BUCKET, optional
# XTNL_BACKUP_EXCLUDE_MARKS (1 = --exclude-table-data=last_clean_marks, plan 3.2 "Decision for you"; default 0)
# and XTNL_BACKUP_SECRETS (stdin | env). Secrets (stdin, or environment): XTNL_BACKUP_B2_ACCOUNT (key id),
# XTNL_BACKUP_B2_KEY (application key). XTNL_BACKUP_HC_URL is not for this job: a line for it on stdin is refused
# (78), and in the environment it is dropped unread.
# PGHOST / PGPORT / PGUSER / PGPASSWORD / PGDATABASE come from the compose service (the local-only login).
#
# Modes: backup (default); check (settings, PostgreSQL and a bucket listing: uploads nothing);
# versions (tool versions).
#
# Exit codes: 0 done; 64 usage; 65 the dump and globals were uploaded (DUMP SAFE: logged) but one or more
# archive files were refused (ARCHIVE REFUSED lines); 66 the dump and globals were uploaded (DUMP SAFE: logged)
# but a later step failed (the monthly copy, or the partition archive including an archive folder that could not
# be read; LATE FAILURE line); 69 PostgreSQL not reachable within 5 min; 70 a dump failed or failed its
# pg_restore -l check, or an internal refusal (nothing plain is uploaded);
# 73 the work folders (tmpfs /dev/shm, /var/tmp) could not be created or written; 75 an upload or a bucket listing failed (try again later);
# 78 configuration (a variable missing or malformed, or an age private key in the environment).
set -eu
umask 077

MODE=${1:-backup}
WORK_BASE=${XTNL_BACKUP_WORK_DIR:-/dev/shm}
OUT_BASE=${XTNL_BACKUP_OUT_DIR:-/var/tmp}
ARCHIVE_SRC=${XTNL_BACKUP_ARCHIVE_SRC:-/archive}
WORK=""
OUT=""
DONE=0
DUMP_UP=0
REFUSED=0
PG_WAIT_S=300
PG_WAIT_STEP_S=10
RCLONE_FLAGS="--retries 3 --low-level-retries 10 --stats 0 -q"

log() { printf '%s xtnl-backup: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
die() { code=$1; shift; log "ERROR: $*"; exit "$code"; }

cleanup() {
    rc=$?
    trap - EXIT INT TERM HUP
    [ -z "$WORK" ] || rm -rf "$WORK" || :
    [ -z "$OUT" ] || rm -rf "$OUT" || :
    if [ "$MODE" = backup ] && [ "$DONE" -ne 1 ] && [ "$DUMP_UP" -eq 1 ]; then
        # The night's dump is safe in daily/<ts>/ and the wrapper has sent check C's success ping on the DUMP SAFE
        # line: a later failure must not turn check C red, or the restore would reject a good dump (review round 1).
        log "LATE FAILURE (exit $rc) after the dump and globals were uploaded to daily/${TS:-?}/; check C keeps the success ping; exit 66 (runbook home-backup-restore.md, exit 66)"
        exit 66
    fi
    if [ "$MODE" = backup ] && [ "$DONE" -ne 1 ]; then
        [ "$rc" -ne 0 ] || rc=70
        log "backup FAILED (exit $rc); plain and encrypted temp files removed (backup-pg.ps1 pings check C /fail)"
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

SECRET_NAMES="XTNL_BACKUP_B2_ACCOUNT XTNL_BACKUP_B2_KEY"

# The two secrets into shell-only variables. stdin mode: NAME=value lines (only the two names), then END; a CR
# before the line end is dropped, and so is a UTF-8 BOM before the first line. env mode: the inherited values, un-exported. No value is ever printed. The check
# C URL is never this job's (step 8): dropped from the environment unread, refused on stdin like any other line.
read_secrets() {
    if [ -n "${XTNL_BACKUP_HC_URL:-}" ]; then
        log "XTNL_BACKUP_HC_URL in the environment ignored: backup-pg.ps1 sends the check C pings"
    fi
    unset XTNL_BACKUP_HC_URL
    case "${XTNL_BACKUP_SECRETS:-env}" in
        stdin)
            # shellcheck disable=SC2086
            unset $SECRET_NAMES
            cr=$(printf '\r')
            # A UTF-8 BOM before the first line (Windows PowerShell writes one into the pipe when the console
            # input code page is UTF-8; review round 3) is dropped, on the first line only.
            bom=$(printf '\357\273\277')
            first=1
            ended=0
            while IFS= read -r line; do
                if [ "$first" -eq 1 ]; then
                    line=${line#"$bom"}
                    first=0
                fi
                line=${line%"$cr"}
                case "$line" in
                    END) ended=1; break ;;
                    XTNL_BACKUP_B2_ACCOUNT=*) XTNL_BACKUP_B2_ACCOUNT=${line#*=} ;;
                    XTNL_BACKUP_B2_KEY=*) XTNL_BACKUP_B2_KEY=${line#*=} ;;
                    *) die 78 "unexpected line on stdin (expected the secret NAME=value lines, then END)" ;;
                esac
            done
            line=""
            [ "$ended" -eq 1 ] || die 78 "the secrets on stdin did not end with END (is the job's stdin attached?)"
            ;;
        env)
            for n in $SECRET_NAMES; do
                eval "v=\${$n:-}"
                unset "$n"
                eval "$n=\$v"
            done
            v=""
            ;;
        *) die 78 "XTNL_BACKUP_SECRETS must be stdin or env" ;;
    esac
}

check_config() {
    read_secrets
    missing=""
    for n in XTNL_BACKUP_AGE_RECIPIENT XTNL_BACKUP_B2_ACCOUNT XTNL_BACKUP_B2_KEY XTNL_BACKUP_B2_BUCKET; do
        eval "v=\${$n:-}"
        [ -n "$v" ] || missing="$missing $n"
    done
    [ -z "$missing" ] || die 78 "missing setting(s):$missing"
    # The private key must never be on this PC: refuse to run if any variable holds one (its name is not printed
    # either, the value never).
    if env | grep -q 'AGE-SECRET-KEY-'; then
        die 78 "an age PRIVATE key is in the environment: refused (keep it off this PC, runbook 'age key')"
    fi
    case "$XTNL_BACKUP_B2_ACCOUNT$XTNL_BACKUP_B2_KEY" in
        *AGE-SECRET-KEY-*) die 78 "an age PRIVATE key was passed as a secret: refused (runbook 'age key')" ;;
    esac
    # They go into rclone.conf lines: anything a B2 key id / key never contains is refused (no config injection).
    case "$XTNL_BACKUP_B2_ACCOUNT$XTNL_BACKUP_B2_KEY" in
        *[!A-Za-z0-9+/=_-]*) die 78 "XTNL_BACKUP_B2_ACCOUNT / XTNL_BACKUP_B2_KEY hold characters a B2 key never has" ;;
    esac
    case "$XTNL_BACKUP_AGE_RECIPIENT" in
        age1*) ;;
        *) die 78 "XTNL_BACKUP_AGE_RECIPIENT is not an age public key (age1...)" ;;
    esac
    case "$XTNL_BACKUP_B2_BUCKET" in
        *[!A-Za-z0-9-]*) die 78 "XTNL_BACKUP_B2_BUCKET is not a B2 bucket name (letters, digits, '-')" ;;
    esac
    case "${XTNL_BACKUP_EXCLUDE_MARKS:-0}" in
        0|1) ;;
        *) die 78 "XTNL_BACKUP_EXCLUDE_MARKS must be 0 or 1" ;;
    esac
    : "${PGDATABASE:=xtnl}"
}

make_dirs() {
    WORK=$(mktemp -d "$WORK_BASE/xtnl-backup.XXXXXX") || die 73 "cannot create the work folder under $WORK_BASE"
    OUT=$(mktemp -d "$OUT_BASE/xtnl-backup-out.XXXXXX") || die 73 "cannot create the output folder under $OUT_BASE"
    # The remote, key included, lives only in this 0600 file on tmpfs (umask 077; removed by the EXIT trap). Its
    # path is exported, never its contents: rclone inherits no secret through its environment.
    RCLONE_CONFIG="$WORK/rclone.conf"
    {
        printf '[xtnlb2]\n'
        printf 'type = b2\n'
        printf 'account = %s\n' "$XTNL_BACKUP_B2_ACCOUNT"
        printf 'key = %s\n' "$XTNL_BACKUP_B2_KEY"
        printf 'hard_delete = false\n'
    } > "$RCLONE_CONFIG" || die 73 "cannot write the rclone config under $WORK_BASE"
    export RCLONE_CONFIG
    unset XTNL_BACKUP_B2_ACCOUNT XTNL_BACKUP_B2_KEY
    REMOTE="xtnlb2:$XTNL_BACKUP_B2_BUCKET"
}

# pg_isready makes ONE attempt (-t bounds only that attempt) and answers at once while the server starts up or
# recovers (1) or refuses the connection (2): retry every PG_WAIT_STEP_S for up to PG_WAIT_S (review round 1).
wait_pg() {
    waited=0
    until pg_isready -q -t 10; do
        [ "$waited" -lt "$PG_WAIT_S" ] \
            || die 69 "PostgreSQL at ${PGHOST:-?}:${PGPORT:-5432} not reachable after $waited s (is the stack up?)"
        [ "$waited" -gt 0 ] || log "PostgreSQL not ready yet (starting or recovering?); trying again for up to $PG_WAIT_S s"
        sleep "$PG_WAIT_STEP_S"
        waited=$((waited + PG_WAIT_STEP_S))
    done
}

# $1 plain file (must be under the tmpfs work folder), $2 encrypted output (under the output folder). The plain
# file is removed once the output is checked to be an age file; then a .sha256 of the output is written.
encrypt() {
    case "$1" in "$WORK"/*) ;; *) die 70 "refusing to encrypt a file outside the work folder" ;; esac
    case "$2" in "$OUT"/*.age) ;; *) die 70 "refusing an encrypted output outside the output folder" ;; esac
    age -r "$XTNL_BACKUP_AGE_RECIPIENT" -o "$2" "$1" || die 70 "age encryption of $(basename "$1") failed"
    [ "$(head -c 21 "$2")" = "age-encryption.org/v1" ] || die 70 "$(basename "$2") is not an age file: refused"
    rm -f "$1"
    (cd "$(dirname "$2")" && sha256sum "$(basename "$2")" > "$(basename "$2").sha256") \
        || die 70 "sha256 of $(basename "$2") failed"
}

# $1 local file, $2 remote folder. Only encrypted outputs are ever uploaded.
upload() {
    case "$1" in
        "$OUT"/*.age|"$OUT"/*.age.sha256) ;;
        *) die 70 "refusing to upload $(basename "$1"): only encrypted outputs are uploaded" ;;
    esac
    # shellcheck disable=SC2086
    rclone copyto --no-check-dest $RCLONE_FLAGS "$1" "$2/$(basename "$1")" \
        || die 75 "upload of $(basename "$1") failed"
}

# $1 remote folder, $2 label for the log, $3 output file: the file names under the folder (recursive). An
# absent folder (rclone exit 3, nothing uploaded there yet) is an empty list; any other failure ends the run.
remote_list() {
    rc=0
    # shellcheck disable=SC2086
    rclone lsf -R --files-only $RCLONE_FLAGS "$1/" > "$3" || rc=$?
    case $rc in
        0) ;;
        3) : > "$3" ;;
        *) die 75 "listing $2 in the bucket failed (rclone exit $rc)" ;;
    esac
}

archive_step() {
    if [ ! -d "$ARCHIVE_SRC" ]; then
        log "partition archive: no folder mounted at $ARCHIVE_SRC, skipped"
        return 0
    fi
    remote_list "$REMOTE/archive" "archive/" "$OUT/archive-remote.lst"
    # No pipe: POSIX sh has no pipefail, and a failing find must end the run, not look like an empty folder.
    find "$ARCHIVE_SRC" -type f -name '*.jsonl.gz' > "$OUT/archive-found.lst" \
        || die 70 "partition archive: cannot read $ARCHIVE_SRC"
    sort "$OUT/archive-found.lst" > "$OUT/archive-local.lst"
    sent=0
    held=0
    pending=0
    while IFS= read -r f; do
        rel=${f#"$ARCHIVE_SRC"/}
        # The name goes into an object name and the log: anything else is refused without printing it, and the
        # other files go on (review round 4).
        case "$rel" in
            *[!A-Za-z0-9_./-]*) refuse "a file with an unexpected name (only A-Z a-z 0-9 _ . / -)"; continue ;;
        esac
        if [ ! -f "$f.sha256" ]; then
            pending=$((pending + 1))
            log "partition archive: $rel has no .sha256 sidecar yet (export in progress), next run"
            continue
        fi
        prefix=$(cut -c1-16 "$f.sha256") || prefix=""
        case "$prefix" in *[!0-9a-f]*|"") refuse "$rel: malformed .sha256 sidecar"; continue ;; esac
        if [ "${#prefix}" -ne 16 ]; then refuse "$rel: malformed .sha256 sidecar"; continue; fi
        obj="$rel.$prefix.tar.age"
        # Already in the bucket: skipped without hashing the file again (review round 4). It was checked when it
        # was sent; a copy that rots on disk afterwards must not fail every later night.
        if grep -Fxq "$obj.sha256" "$OUT/archive-remote.lst"; then
            held=$((held + 1))
            continue
        fi
        if ! (cd "$(dirname "$f")" && sha256sum -c --status "$(basename "$f").sha256"); then
            refuse "$rel fails its .sha256 check (damaged, or changed after its export): not uploaded"
            continue
        fi
        sub=$(dirname "$rel")
        if [ "$sub" = . ]; then dest="$REMOTE/archive"; else dest="$REMOTE/archive/$sub"; fi
        if ! tar -C "$ARCHIVE_SRC" -cf "$WORK/archive.tar" "$rel" "$rel.sha256"; then
            rm -f "$WORK/archive.tar"
            refuse "tar of $rel failed (a read error, or no space left on the tmpfs): not uploaded"
            continue
        fi
        mkdir -p "$OUT/archive/$sub"
        encrypt "$WORK/archive.tar" "$OUT/archive/$obj"
        upload "$OUT/archive/$obj" "$dest"
        upload "$OUT/archive/$obj.sha256" "$dest"
        rm -f "$OUT/archive/$obj" "$OUT/archive/$obj.sha256"
        sent=$((sent + 1))
    done < "$OUT/archive-local.lst"
    log "partition archive: $sent file(s) uploaded, $held already in the bucket, $pending awaiting their sidecar, $REFUSED refused"
}

# One archive file that cannot be sent: logged and counted; the run goes on (the dump is already uploaded).
refuse() {
    REFUSED=$((REFUSED + 1))
    log "ARCHIVE REFUSED: $1 (runbook home-backup-restore.md, exit 65)"
}

run_backup() {
    check_config
    TS=$(date -u +%Y%m%dT%H%M%SZ)
    MONTH=$(date -u +%Y-%m)
    make_dirs
    wait_pg
    log "backup $TS started (database $PGDATABASE)"

    dump="$WORK/xtnl-$TS.dump"
    if [ "${XTNL_BACKUP_EXCLUDE_MARKS:-0}" = 1 ]; then
        log "XTNL_BACKUP_EXCLUDE_MARKS=1: table data of last_clean_marks left out"
        pg_dump -Fc --no-password --exclude-table-data=last_clean_marks -f "$dump" -d "$PGDATABASE" \
            || die 70 "pg_dump failed"
    else
        pg_dump -Fc --no-password -f "$dump" -d "$PGDATABASE" || die 70 "pg_dump failed"
    fi
    pg_restore -l "$dump" > "$WORK/toc.lst" || die 70 "pg_restore -l cannot read the dump: nothing uploaded"
    entries=$(grep -c ' TABLE DATA ' "$WORK/toc.lst" || :)
    [ "${entries:-0}" -gt 0 ] || die 70 "the dump lists no table data: nothing uploaded"
    rm -f "$WORK/toc.lst"
    log "dump checked with pg_restore -l: $entries table data entries"

    globals="$WORK/globals-$TS.sql"
    pg_dumpall --globals-only --no-role-passwords --no-password -f "$globals" \
        || die 70 "pg_dumpall --globals-only failed"

    encrypt "$dump" "$OUT/xtnl-$TS.dump.age"
    encrypt "$globals" "$OUT/globals-$TS.sql.age"
    log "dump and globals encrypted; plain copies removed"

    for f in "$OUT/xtnl-$TS.dump.age" "$OUT/globals-$TS.sql.age"; do
        upload "$f" "$REMOTE/daily/$TS"
        upload "$f.sha256" "$REMOTE/daily/$TS"
    done
    DUMP_UP=1
    # backup-pg.ps1 sends check C's success ping when it reads this line (step 8): keep its wording.
    log "DUMP SAFE: dump and globals uploaded to daily/$TS/"

    remote_list "$REMOTE/monthly/$MONTH" "monthly/$MONTH/" "$OUT/monthly-remote.lst"
    if grep -q '\.dump\.age\.sha256$' "$OUT/monthly-remote.lst"; then
        log "monthly/$MONTH/ already holds this month's copy"
    else
        for f in "$OUT/xtnl-$TS.dump.age" "$OUT/globals-$TS.sql.age"; do
            upload "$f" "$REMOTE/monthly/$MONTH"
            upload "$f.sha256" "$REMOTE/monthly/$MONTH"
        done
        log "first backup of $MONTH: also uploaded to monthly/$MONTH/"
    fi

    archive_step

    DONE=1
    log "backup $TS done"
    if [ "$REFUSED" -gt 0 ]; then
        # The dump is safe (DUMP SAFE: was logged); the refused archive files are a separate outcome (exit 65).
        log "backup $TS: $REFUSED partition archive file(s) refused (see ARCHIVE REFUSED above)"
        exit 65
    fi
}

run_check() {
    check_config
    make_dirs
    wait_pg
    log "PostgreSQL reachable as ${PGUSER:-?} (database $PGDATABASE)"
    # shellcheck disable=SC2086
    rclone lsf --max-depth 1 $RCLONE_FLAGS "$REMOTE/" > /dev/null || die 75 "cannot list the bucket (key or bucket wrong?)"
    log "bucket listing works"
    if [ -d "$ARCHIVE_SRC" ]; then
        log "partition archive folder mounted at $ARCHIVE_SRC"
    else
        log "WARNING: no partition archive folder mounted at $ARCHIVE_SRC"
    fi
    log "check passed (nothing uploaded)"
}

case "$MODE" in
    backup) run_backup ;;
    check) run_check ;;
    versions)
        pg_dump --version
        age --version
        rclone version | head -n 1
        ;;
    *) die 64 "usage: xtnl-backup [backup|check|versions]" ;;
esac
