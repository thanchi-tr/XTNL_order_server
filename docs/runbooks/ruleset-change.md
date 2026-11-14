# Oversee ruleset change (thresholds)

Phase 1 never sends orders. A ruleset only sets the thresholds the RiskSweep evaluates against
(oversee-rules.md s6-s7); changing it enables no order action, and ActionGate keeps every condition.

## Trigger

* A reviewed change of a risk threshold (position risk band, aggregate risk, daily loss, margin usage,
  missing-stop grace, clean-spread pips per symbol), with a ticket.
* Page "ruleset vN invalid" (the engine kept the last good ruleset): activate a valid version.
* Rolling back a change: activate the previous version.

## Checks

Write the document as JSON sections over the spec defaults (`{}` is the spec ruleset; keys you leave out
keep the spec value; numbers as strings keep them exact, e.g. `"0.8"`). Validate it offline first; this
uses the engine's own parser and touches no database:

```
$OPS ruleset validate /tmp/rules-INC-77.json          # exit 0 valid, 3 refused (every problem listed)
$OPS ruleset show                                     # versions, which one is active, valid / invalid
$OPS ruleset show --version <version-id>              # one version: headline thresholds + full effective JSON
```

`sweep.interval_ms` is a deployment setting: a running term keeps its own value (and pages if the
ruleset differs), so leave it out.

## Commands

1. Store it as the next version (inactive; one person):
   ```
   $OPS ruleset add /tmp/rules-INC-77.json --operator <you> --reason "<ticket>: <what changes and why>"
   ```
   The CLI re-validates, stores the complete, explicit ruleset (`ruleset_json`), re-parses the stored row
   before commit, and audits `ruleset.add`. Refusals (exit 3, audited): `invalid_ruleset`,
   `roundtrip_mismatch`.
2. Activate it (two people):
   ```
   $OPS ruleset activate <version-id> --operator <you> --approver <second person> \
        --reason "<ticket>: reviewed thresholds"
   ```
   The stored row is validated again; the previous active row is deactivated in the same transaction.
   Refusals: `invalid_ruleset`, `already_active`, `not_found`.

## Expected outcome

Within one ruleset poll (about 5 s) the leader installs the version at a sweep boundary, writes a
`ruleset_audit` row (`installed`) and sweeps every account. States of rules the change removes or disables
resolve with `resolved_by = reload`. Check:

```
$OPS ruleset show
$OPS audit tail --command ruleset.activate
journalctl -u xtnl-engine --since "-5 min" | grep -E "ruleset v"
```

## Rollback

Activate the previous version (two people), with the ticket as the reason:

```
$OPS ruleset activate <previous-version-id> --operator <you> --approver <second person> \
     --reason "<ticket>: roll back to the previous thresholds"
```

Stored versions are never edited or deleted; every add and activation stays in `operator_audit`.
