#!/usr/bin/env bash
# xtnl-pg-backup: the scheduled pgBackRest backups of the Tier 3 image (operations.md §8: "daily incremental /
# differential backups"; IMPLEMENTATION_PLAN 6.4). Phase 1 never sends orders; this touches only pgBackRest.
#
# Install as /usr/local/bin/xtnl-pg-backup (0755 root) on both PostgreSQL members, with
# deploy/systemd/xtnl-pg-backup.service + .timer (timer enabled on both). Runs as `postgres`.
#
#   xtnl-pg-backup [auto|incr|diff|full]
#       auto (the timer): a full backup on XTNL_BACKUP_FULL_DOW (date +%u, default 6 = Saturday, FX closed),
#       an incremental backup every other day. pgBackRest itself promotes the first backup of a stanza to full.
#
# Only the Patroni primary backs up: the replica's run exits 0 ("not the primary"), so enabling the timer on both
# members follows a failover without any change. A failed backup is retried (XTNL_BACKUP_ATTEMPTS, default 3,
# XTNL_BACKUP_RETRY_S apart, default 300 s): the usual cause is the PITR guard's immediate backup holding the
# stanza lock (pgBackRest refuses a second concurrent backup). After the last attempt the unit fails and the
# journal carries a `crit` line (tag xtnl-pg-backup); retention / expire runs inside `pgbackrest backup`.
# Never restores anything: restores are operator-run through Patroni (disaster-restore-melbourne.md).
set -euo pipefail

STANZA="${XTNL_PGBACKREST_STANZA:-xtnl}"
FULL_DOW="${XTNL_BACKUP_FULL_DOW:-6}"
ATTEMPTS="${XTNL_BACKUP_ATTEMPTS:-3}"
RETRY_S="${XTNL_BACKUP_RETRY_S:-300}"
STATE_DIR="${XTNL_BACKUP_STATE_DIR:-/var/lib/xtnl-pg-backup}"
PSQL=(psql -X -q -A -t -h /var/run/postgresql -U postgres -d postgres)

log() {   # log <priority> <message>
    logger -t xtnl-pg-backup -p "daemon.$1" -- "$2" 2>/dev/null || true
    echo "xtnl-pg-backup: $2" >&2
}

is_primary() {
    [ "$("${PSQL[@]}" -c 'SELECT NOT pg_is_in_recovery()' 2>/dev/null)" = "t" ]
}

backup_type() {   # backup_type <mode>
    case "$1" in
        incr|diff|full) echo "$1" ;;
        auto) if [ "$(date -u +%u)" = "$FULL_DOW" ]; then echo full; else echo incr; fi ;;
        *) return 2 ;;
    esac
}

run() {
    local mode="${1:-auto}" type attempt=1
    if ! type="$(backup_type "$mode")"; then
        echo "usage: xtnl-pg-backup [auto|incr|diff|full]" >&2
        return 2
    fi
    mkdir -p "$STATE_DIR"
    exec 9>"$STATE_DIR/scheduled.lock"          # one scheduled run at a time on this member
    if ! flock -n 9; then
        log notice "a scheduled backup is already running on this member; skipped"
        return 0
    fi
    if ! is_primary; then
        log info "not the primary: the primary's timer takes the $type backup"
        return 0
    fi
    while true; do
        log info "starting $type backup (attempt $attempt/$ATTEMPTS)"
        if pgbackrest --stanza="$STANZA" --type="$type" backup; then
            date -u +%s >"$STATE_DIR/last_success_$type"
            log notice "$type backup done"
            return 0
        fi
        if [ "$attempt" -ge "$ATTEMPTS" ]; then
            log crit "$type backup FAILED after $ATTEMPTS attempts: run it by hand (disaster-restore-melbourne.md)"
            return 1
        fi
        log warning "$type backup failed (attempt $attempt/$ATTEMPTS); retrying in ${RETRY_S}s"
        sleep "$RETRY_S"
        attempt=$((attempt + 1))
        if ! is_primary; then                        # a failover during the wait: the new primary owns it
            log notice "no longer the primary: the new primary's timer takes the backup"
            return 0
        fi
    done
}

run "${1:-auto}"
