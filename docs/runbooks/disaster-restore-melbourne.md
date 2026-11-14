# Database damage and region disaster restore

Phase 1 never sends orders. The engine never restores PostgreSQL by itself; every step here is an operator
action. RTO 2-4 h; RPO <= the WAL archive window (`archive_timeout` 60 s).

## Trigger

* Integrity Step 2 HALTED page: checksum failure or `seq_id` regression.
* Damage on both members (MANUAL_INTERVENTION), or the London region is lost.

## Checks

The Tier 3 repo is encrypted. Every `pgbackrest` command below (and WAL archive-push / archive-get under
Patroni) reads the passphrase from `/etc/pgbackrest/conf.d/xtnl-cipher.conf` (0640 root:postgres, from Secrets
Manager; `deploy/patroni/pgbackrest.conf`), so they run as `postgres` with no environment variable. "unable to
decrypt" / "cipher" errors mean that file is missing or wrong on this host.

```
patronictl -c /etc/patroni/patroni.yml list                 # which member is damaged? is the standby in sync?
journalctl -u xtnl-engine --since "-30 min" | grep -E "HALTED|SWITCHING_OVER|MANUAL_INTERVENTION|regression"
sudo -u postgres pgbackrest --stanza=xtnl info               # newest backup, archived WAL range
sudo -u postgres pgbackrest --stanza=xtnl check              # archive path healthy
```

Decide: damage on the primary only -> A; on both members -> B; London region unavailable -> C.

## A. Primary-only damage (SWITCHING_OVER)

```
patronictl -c /etc/patroni/patroni.yml list
patronictl -c /etc/patroni/patroni.yml switchover --leader <damaged> --candidate <standby>
patronictl -c /etc/patroni/patroni.yml reinit xtnl-pg <damaged>
```

Only switch to a standby that passed the same integrity check. Expected: engine commits resume on the new
primary; the reinitialised member returns as Sync Standby.

## B. Tier 3 restore through Patroni (both members damaged)

1. Stop both engines: `sudo systemctl stop xtnl-engine` (A and B). Ingestion stops; nothing else changes at
   the brokers.
2. `patronictl pause xtnl-pg`, stop PostgreSQL on both members.
3. Pick an explicit target on the current timeline (never "latest" blindly):
   `sudo -u postgres pgbackrest --stanza=xtnl info` and choose a time / LSN before the damage.
4. On the chosen primary: `sudo -u postgres pgbackrest --stanza=xtnl --delta --type=time
   --target="<UTC time>" --target-action=promote restore`, start PostgreSQL, verify, then `patronictl resume`.
5. `patronictl reinit xtnl-pg <other member>`.
6. Start the engines. LEADER_INIT runs SM8 (Redis is flushed and rebuilt from the restored data; Redis
   "ahead" keys are deleted and re-applied).

## C. Region disaster (Melbourne, manual only)

1. **Isolate London first:** stop or network-isolate every London node, then revoke the London broker
   credentials and cTrader refresh tokens (account owners, in the broker portals). Two engines must never
   run against the same accounts.
2. Restore Tier 3 (ap-southeast-4 S3, Object Lock) into the pre-provisioned stack in the DR region (B above,
   with the DR stack's own pgBackRest repo config). The DR members need the **same** repo passphrase in
   `/etc/pgbackrest/conf.d/xtnl-cipher.conf` (from the ap-southeast-4 copy of the secret) before the restore;
   check with `sudo -u postgres pgbackrest --stanza=xtnl info` (it lists the backups only when it can decrypt).
3. Start the DR etcd cluster. It has a **new `cluster_id`**: the first LEADER_INIT finds a different
   `cluster_id` in `engine_epoch` and goes FENCED with a critical page. Adopt it only by operator sign-off
   (two people, ticket), and only after step 1 (London isolated):
   ```
   XTNL_ETCD_ENDPOINTS=<the three DR etcd members> $OPS epoch show
       # cluster_id (PostgreSQL, the old cluster) vs etcd_cluster_id (the DR cluster); note both
   XTNL_ETCD_ENDPOINTS=<the three DR etcd members> $OPS epoch adopt <etcd_cluster_id> \
       --from-cluster-id <cluster_id> --operator <you> --approver <second person> --reason "<DR ticket>"
   ```
   The command refuses unless etcd itself reports `<etcd_cluster_id>` and `engine_epoch` still holds the
   reviewed `<cluster_id>`. It sets `engine_epoch` to (`<etcd_cluster_id>`, revision 0) in one transaction
   with its `operator_audit` row: every node of the old cluster is fenced from then on (its token no longer
   matches), and the DR node's next LEADER_INIT bump succeeds. Then confirm with `$OPS epoch show` that a
   new term started.
4. Issue fresh read-only broker credentials for the DR stack (`BROKER_ENV=live_readonly`).
5. Any dropped WAL or timeline switch: take an immediate incremental backup
   (`sudo -u postgres /usr/local/bin/xtnl-pitr-guard backup "<DR ticket>"`: an incremental pgBackRest backup on
   the primary); the "PITR horizon broken" alert is expected.

## Expected outcome

One LEADER in the DR region, epoch adopted, SM8 READY, adapters STREAMING, dead-man heartbeats resumed.
Data after the restore target is re-read from the brokers by the snapshot / reconcile cycle.

## Rollback

Before step B4 you can return to the damaged cluster (`patronictl resume`). After a promote, the restored
timeline is authoritative; return to London only by a fresh planned migration, never by resuming the old
London nodes.
