# Halt review and operator clear (HARD latch)

Phase 1 never sends orders. A halt is recorded and alerted; clearing it only lowers the **recorded** level.
It enables no order action: nothing in Phase 1 sends orders, and ActionGate keeps every other condition
(oversee-rules.md s8). Acting at the broker is a human decision taken in the broker's own platform.

## Trigger

* Page "HALT HARD" (a HARD trip always sets the HARD latch, which only an operator clear releases).
* A SOFT halt that will not cool down although the condition is understood (the clear restarts every
  state's escalation clock, so a reviewed condition gets a full `escalate_after_ms` again).

## Checks

```
$OPS halt status                         # effective level, latch, halt_event_id, every active rule state
$OPS halt status --account <account>     # only that account's states
journalctl -u xtnl-engine --since "-30 min" | grep -E "trip|HALT|escalat|evaluation_failed"
$OPS audit tail --command halt.clear     # earlier clears
```

Review each active HARD / SOFT state: rule, subject, cap (SOFT = an input is stale or estimated), UNKNOWN,
reason. Note the `reviewed_event_id_for_clear` value: the clear is refused if a newer `halt_events` row
appears after your review.

## Commands

* **Latch only** (no active HARD state any more; the level drops to the sweep's target, SOFT or NONE):
  ```
  $OPS halt clear <account> --reviewed-event-id <id> --operator <you> --reason "<ticket>: <what you reviewed>"
  ```
* **Forced** (the account's HARD state cannot be confirmed either way: its latest results are capped or
  UNKNOWN), two people:
  ```
  $OPS halt clear <account> --reviewed-event-id <id> --force --operator <you> --approver <second person> \
       --reason "<ticket>: <why the HARD input is not trustworthy>"
  ```
  The forced clear resolves that HARD state (`resolved_by = operator`) and writes one `oversee_events`
  `resolved` row per state. It is refused while an active HARD state of **another** account exists
  (`active_hard_state_on_other_account`): review and clear that account separately.

Everything runs under `HALT_LOCK` in one transaction fenced on the current epoch: the `halt_breaches`
updates (`clear_generation` bumped, so an in-flight sweep write built before the clear is recomputed), the
`halt_events` row (`by` = you), the alert, the `oversee_events` rows and the `operator_audit` row.

Refusals (exit 3, audited): `newer_halt_event` (re-run `halt status` and review again),
`active_hard_state` (an uncapped HARD is still active: the condition is real; a clear is not the fix),
`hard_state_uncapped` (`--force` cannot resolve a confirmed HARD), `unknown_account`, `epoch_moved`.

## Acknowledge instead of clear

A capped SOFT, UNKNOWN-origin or `evaluation_failed` state that is understood but cannot be fixed yet re-pages
every `escalate_after_ms`. Acknowledge it (oversee-rules.md s4 `acknowledge(operator, why, breach_id)`) to
silence its re-pages until its level, cap or reason changes; it changes no state and not the halt level:
```
$OPS halt status                                   # the breach_id column of the active state
$OPS halt ack <breach_id> --operator <you> --reason "<ticket>: <why it is understood>"
$OPS audit tail --command halt.ack
```
The `breach_acks` row and the `operator_audit` row commit together under `HALT_LOCK`, fenced on the epoch.
Repeating the same acknowledgement is a no-op (`outcome: noop`, audited). Refusals: `not_found`, `resolved`,
`not_active` (a pending or released state does not re-page), `epoch_moved`.

## Expected outcome

`$OPS halt status` shows the new `halt_event_id` with `by` = you and the latch released; the leader's
integrity tick re-applies Redis `halt:state` within one tick. The RiskSweep keeps evaluating: a condition
that persists re-activates (a capped or UNKNOWN result only up to SOFT; only an uncapped result reaches HARD
again) and pages as a new episode.

## Rollback

None: a clear cannot be undone, and none is needed, because the sweep re-trips any condition that is still
true on its next pass. If a clear was made in error, record it in the ticket; the audit row stays.
