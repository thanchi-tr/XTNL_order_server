#!/usr/bin/env bash
# xtnl-pitr-guard: the automatic incremental backup behind the "PITR horizon broken" alert
# (operations.md §8: "any dropped WAL or timeline switch triggers an immediate incremental backup and a
# critical 'PITR horizon broken' alert"). Phase 1 never sends orders; this touches only pgBackRest.
#
# Install as /usr/local/bin/xtnl-pitr-guard (0755 root) on both PostgreSQL members. Runs as `postgres`
# (local socket, pgBackRest config readable by postgres). Two entry points:
#
#   xtnl-pitr-guard check
#       every minute from xtnl-pitr-guard.timer: compares pg_stat_archiver.failed_count and the timeline with
#       the last run; a new archive failure or a timeline switch -> incremental backup (on the primary only).
#   xtnl-pitr-guard on_role_change <role> <scope>
#       Patroni callback (patroni.yml postgresql.callbacks.on_role_change): a promotion to primary is a
#       timeline switch -> incremental backup, started detached so Patroni is never blocked.
#
# The alert itself is raised off-node (XtnlPitrHorizonBroken, from xtnl_wal_archive_failed_total and
# xtnl_pg_timeline); this script only takes the backup and logs to the journal (tag xtnl-pitr-guard).
set -euo pipefail

STANZA="${XTNL_PGBACKREST_STANZA:-xtnl}"
STATE_DIR="${XTNL_PITR_STATE_DIR:-/var/lib/xtnl-pitr-guard}"
PSQL=(psql -X -q -A -t -h /var/run/postgresql -U postgres -d postgres)

log() {   # log <priority> <message>
    logger -t xtnl-pitr-guard -p "daemon.$1" -- "$2" 2>/dev/null || true
    echo "xtnl-pitr-guard: $2" >&2
}

is_primary() {
    [ "$("${PSQL[@]}" -c 'SELECT NOT pg_is_in_recovery()' 2>/dev/null)" = "t" ]
}

backup() {   # backup <reason>
    local reason="$1" waited=0
    mkdir -p "$STATE_DIR"
    exec 9>"$STATE_DIR/backup.lock"
    if ! flock -n 9; then
        log notice "incremental backup already running; skipped ($reason)"
        return 0
    fi
    while ! is_primary; do                      # a promotion may still be finishing recovery
        if [ "$waited" -ge 60 ]; then
            log notice "not the primary: the primary's guard takes the backup ($reason)"
            return 0
        fi
        sleep 5
        waited=$((waited + 5))
    done
    log crit "PITR horizon broken ($reason): taking an immediate incremental backup"
    if pgbackrest --stanza="$STANZA" --type=incr backup; then
        log notice "incremental backup done ($reason)"
    else
        log crit "incremental backup FAILED ($reason): run it by hand (disaster-restore-melbourne.md)"
        return 1
    fi
}

check() {
    mkdir -p "$STATE_DIR"
    local failed tli prev_failed prev_tli reason=""
    failed="$("${PSQL[@]}" -c 'SELECT failed_count FROM pg_stat_archiver')"
    tli="$("${PSQL[@]}" -c 'SELECT timeline_id FROM pg_control_checkpoint()')"
    prev_failed="$(cat "$STATE_DIR/failed_count" 2>/dev/null || echo "$failed")"
    prev_tli="$(cat "$STATE_DIR/timeline" 2>/dev/null || echo "$tli")"
    if [ "$failed" -gt "$prev_failed" ]; then
        reason="WAL archive failures $prev_failed -> $failed"
    fi
    if [ "$tli" != "$prev_tli" ]; then
        reason="${reason:+$reason; }timeline $prev_tli -> $tli"
    fi
    if [ -n "$reason" ]; then
        # the new baseline is recorded only after the backup succeeded (or another member's guard owns it),
        # so a failed backup is retried by the next timer run instead of being forgotten
        backup "$reason" || return 1
    fi
    echo "$failed" >"$STATE_DIR/failed_count"
    echo "$tli" >"$STATE_DIR/timeline"
}

case "${1:-check}" in
    check)
        check
        ;;
    backup)
        backup "${2:-operator request}"
        ;;
    on_role_change)
        role="${2:-}"
        if [ "$role" = "primary" ] || [ "$role" = "master" ]; then
            setsid -f "$0" backup "promoted to primary (timeline switch)" </dev/null >/dev/null 2>&1
        fi
        ;;
    on_start|on_stop|on_restart|on_reload)
        ;;
    *)
        echo "usage: xtnl-pitr-guard check | backup [reason] | on_role_change <role> <scope>" >&2
        exit 2
        ;;
esac
