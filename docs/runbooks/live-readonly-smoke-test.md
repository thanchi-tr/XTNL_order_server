# Live read-only smoke test (StoneX probe)

Phase 1 never sends orders. The probe logs on, reads, prints and logs off. It makes no order-changing call:
the read-only transport denies mutation paths and `BROKER_ENV` only accepts `sandbox` or `live_readonly`.

## Trigger

First contact with a real account using read-only credentials, after a credential rotation, or before
switching a node from `sandbox` to `live_readonly`.

## Who runs it

**The account owner, on their own machine, with their own environment.** Credentials are never sent to or
typed by anyone else, never put in a file in the repository, never pasted into chat or a ticket.

## Checks (before running)

* The project venv exists: `.venv/Scripts/python.exe -m pytest -q tests/adapters` passes (no network).
* The credentials are the **read-only** ones issued for this system, not a trading login.
* `BROKER_ENV` is `sandbox` for the first run; switch to `live_readonly` only after the sandbox run passes.
* Note the open-position and working-order counts in the StoneX platform, to compare with the probe.

## Commands (PowerShell 7+, project root; `-MaskInput` hides what is typed)

```
$env:BROKER_ENV = "sandbox"            # first; then "live_readonly" for the real accounts
$env:STONE_X_USERNAME = Read-Host "StoneX user"
$env:STONE_X_PASS = Read-Host "StoneX password" -MaskInput
$env:STONE_X_APP_KEY = Read-Host "StoneX app key" -MaskInput
.venv/Scripts/python.exe -m src.adapters.stonex.probe --instruments 5
Remove-Item Env:STONE_X_PASS, Env:STONE_X_APP_KEY, Env:STONE_X_USERNAME
```

## Expected outcome (exit code 0)

* One line per client account: id, currency, trading account ids.
* One `margin` line per trading account: balance, equity, margin used, free margin, unrealised P&L.
* `open positions: N  active orders: M  instruments: K`; compare N and M with the StoneX platform.
* Up to 5 instrument lines (symbol, market id, P&L currency, point size, min quantity).
* No password, app key or session token appears anywhere in the output.

## Failure

* Exit 1 with `StoneX probe failed: <ErrorType>`: auth errors -> `auth-failed-rearm.md` (fix at the source);
  network errors -> check the endpoint for the chosen `BROKER_ENV`.
* `refused: BROKER_ENV=...`: only `sandbox` and `live_readonly` are allowed, by design.
* Counts differ from the platform: record both numbers in the ticket; do not change anything at the broker.

## Rollback

Nothing to undo: the probe changes no broker or database state. Clear the environment variables (last
command above) and close the shell.
