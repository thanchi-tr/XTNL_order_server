#!/usr/bin/env bash
# xtnl-etcd-defrag: the scheduled ROLLING etcd defrag (operations.md §1: "a scheduled rolling defrag"; one member
# at a time, only while 3/3 are healthy; IMPLEMENTATION_PLAN 6.4). Phase 1 never sends orders; this touches only
# the local etcd member's storage.
#
# Install as /usr/local/bin/xtnl-etcd-defrag (0755 root) on all three etcd hosts (node A, node B, witness), with
# deploy/systemd/xtnl-etcd-defrag.service + .timer and /etc/xtnl/etcd-defrag.env:
#     XTNL_ETCD_SELF_ENDPOINT=https://<this member's ip>:2379
#     XTNL_ETCD_ENDPOINTS=https://<a>:2379,https://<b>:2379,https://<w>:2379
#     ETCDCTL_CACERT / ETCDCTL_CERT / ETCDCTL_KEY   (paths, read by etcdctl itself)
#
#   xtnl-etcd-defrag            take the cluster-wide etcd lock, then run `locked`
#   xtnl-etcd-defrag locked     (inside the lock) check 3/3 healthy, move leadership away if this member leads,
#                               defrag this member only, wait until it is healthy again
#
# Rolling: every member's timer fires at the same time; `etcdctl lock` serialises them, and each one re-checks
# 3/3 health inside the lock, so a member is never defragmented while another is down or still recovering. The
# etcd leader hands leadership to a healthy follower first (`move-leader`, as for any planned member work,
# operations.md §3), so the engine's lease renewals never wait on the defragmenting leader. Not 3/3 healthy:
# nothing is defragmented and the unit fails (exit 1, a `crit` journal line); the etcd quota alert at 50 % is
# the backstop. Output parsing uses etcdctl's `simple` format (no jq / python needed on the witness).
set -euo pipefail

export ETCDCTL_API=3
SELF="${XTNL_ETCD_SELF_ENDPOINT:?XTNL_ETCD_SELF_ENDPOINT is not set}"
ENDPOINTS="${XTNL_ETCD_ENDPOINTS:?XTNL_ETCD_ENDPOINTS is not set}"
LOCK_NAME="${XTNL_ETCD_DEFRAG_LOCK:-/xtnl/ops/etcd-defrag}"
SETTLE_S="${XTNL_ETCD_DEFRAG_SETTLE_S:-30}"
MEMBERS=3

log() {   # log <priority> <message>
    logger -t xtnl-etcd-defrag -p "daemon.$1" -- "$2" 2>/dev/null || true
    echo "xtnl-etcd-defrag: $2" >&2
}

healthy_count() {   # members reporting healthy (simple format: "<endpoint> is healthy: ...")
    { etcdctl --endpoints="$ENDPOINTS" endpoint health 2>&1 || true; } | grep -c " is healthy" || true
}

# Leadership comes from `endpoint status -w fields` ("MemberID" and "Leader" of the answering member: the same
# two lines in etcd 3.4, 3.5 and 3.6), never from a column position of the `simple` status table, whose columns
# changed between versions (3.6 inserted storage version / in use / quota before "is leader"): a misread there
# would let the leader defragment itself without handing leadership away.
self_is_leader() {
    local fields me lead
    fields="$(etcdctl --endpoints="$SELF" endpoint status -w fields)"
    me="$(printf '%s\n' "$fields" | awk -F' : ' '$1 == "\"MemberID\"" {print $2}')"
    lead="$(printf '%s\n' "$fields" | awk -F' : ' '$1 == "\"Leader\"" {print $2}')"
    if [ -z "$me" ] || [ -z "$lead" ]; then
        log crit "cannot read this member's leadership from etcd: no defrag"
        return 2
    fi
    [ "$me" = "$lead" ]
}

follower_id() {   # the hex member id of a started, non-learner member that is not this one
    # member list, simple format (unchanged since 3.4): "<id hex>, <status>, <name>, <peer urls>, <client urls>,
    # <is learner>"; several urls in one field are joined by "," without a space. 3/3 health was checked first.
    etcdctl --endpoints="$ENDPOINTS" member list 2>/dev/null \
        | awk -F', ' -v self="$SELF" '
            !found && $2 == "started" && $6 == "false" {
                n = split($5, urls, ","); mine = 0
                for (i = 1; i <= n; i++) if (urls[i] == self) mine = 1
                if (!mine) { print $1; found = 1 }
            }'
    # (no early `exit` in awk: under pipefail a SIGPIPE to etcdctl would fail the whole function)
}

locked() {
    local n target waited=0
    n="$(healthy_count)"
    if [ "$n" -lt "$MEMBERS" ]; then
        log crit "only $n/$MEMBERS etcd members healthy: no defrag (operations.md §1 needs 3/3)"
        return 1
    fi
    local leader=0 rc=0
    self_is_leader || rc=$?
    if [ "$rc" -eq 2 ]; then
        return 1
    fi
    [ "$rc" -eq 0 ] && leader=1
    if [ "$leader" -eq 1 ]; then
        target="$(follower_id)"
        if [ -z "$target" ]; then
            log crit "this member leads and no healthy follower was found: no defrag"
            return 1
        fi
        log notice "this member is the etcd leader: moving leadership to $target first"
        etcdctl --endpoints="$SELF" move-leader "$target"
    fi
    log notice "defragmenting $SELF"
    etcdctl --endpoints="$SELF" --command-timeout=120s defrag
    while ! etcdctl --endpoints="$SELF" endpoint health >/dev/null 2>&1; do
        if [ "$waited" -ge 120 ]; then
            log crit "$SELF not healthy 120 s after its defrag: check the member before anything else"
            return 1
        fi
        sleep 5
        waited=$((waited + 5))
    done
    if [ -n "$(etcdctl --endpoints="$SELF" alarm list 2>/dev/null)" ]; then
        log crit "etcd alarm still raised after the defrag: see off-node-alerts.md (etcd alarm)"
    fi
    sleep "$SETTLE_S"                              # let the member catch up before the next one takes the lock
    log notice "defrag of $SELF done"
}

case "${1:-run}" in
    run)
        # --ttl: the lock is released if this host dies while holding it
        exec etcdctl --endpoints="$ENDPOINTS" lock --ttl=60 "$LOCK_NAME" "$0" locked
        ;;
    locked)
        locked
        ;;
    *)
        echo "usage: xtnl-etcd-defrag [run|locked]" >&2
        exit 2
        ;;
esac
