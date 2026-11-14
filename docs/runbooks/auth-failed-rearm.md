# Adapter AUTH_FAILED and re-arm

Phase 1 never sends orders. The credentials involved are read-only (`BROKER_ENV=live_readonly`) or sandbox.

## Trigger

Critical page "adapter <id> AUTH_FAILED" after at most 3 attempts: bad password, PasswordChangeRequired,
`invalid_grant`, or TokenInvalidated after a fresh read. AUTH_FAILED is cluster-wide per (adapter,
credential generation), so the standby will not retry the same credentials and no node abdicates over it.

## Checks

```
$PSQL -c "SELECT adapter_id, credential_generation, state, attempts, detail, failed_at_ms, rearmed_by, rearmed_at_ms
          FROM adapter_auth_state ORDER BY failed_at_ms DESC LIMIT 10"
journalctl -u xtnl-engine --since "-30 min" | grep -E "AUTH_FAILED|LogOn|invalid_grant|TokenInvalidated"
```

* StoneX: log on to the StoneX web platform with the same user. A password-change prompt or a locked
  account means the credential must be rotated, not retried.
* cTrader: check the app in the cTrader Open API portal; `invalid_grant` means the refresh token is dead and
  the account owner must re-authorise the app.

## Commands

1. Fix the credential at the source (password change by the account owner, new read-only app key, or a new
   cTrader authorisation). The account owner does this; operators never handle the password in chat.
2. Store it in AWS Secrets Manager (new version). For cTrader, the leader saves the new token pair with a
   bumped `token_generation`.
3. A new credential generation re-arms automatically: SM4 leaves AUTH_FAILED for AUTHENTICATING on its own.
4. Same credentials, transient provider fault confirmed (e.g. a StoneX outage that returned an auth error):
   re-arm through the audited operator command:
   ```
   $OPS adapter rearm <adapter_id> <credential_generation> --operator <you> --reason "<ticket>: <evidence>"
   ```
   It sets the `AUTH_FAILED` row to `REARMED` with `rearmed_by`, in one transaction with its
   `operator_audit` row, fenced on the current epoch. `refused: not_auth_failed` means that (adapter,
   generation) is not AUTH_FAILED (wrong generation, or already re-armed): copy both values from the
   check query above.

## Expected outcome

The adapter goes AUTHENTICATING -> SNAPSHOTTING -> STREAMING; the dead-man payload shows it STREAMING; the
`stream_health` fence clears after reconcile.

## Rollback

If the new credential also fails, the adapter returns to AUTH_FAILED after 3 attempts and pages again; no
retry loop runs. Restore the previous Secrets Manager version only if it is known good.
