"""Alert delivery for Phase 1 (oversee-rules.md §10, operations.md §3 / §9).

* `settings`: channel configuration from environment variables (XTNL_SMTP_*, XTNL_SES_*, XTNL_PUSH_*,
  XTNL_ALERT_EMAIL_TO). A channel without complete settings is disabled and reported, never fatal.
* `mail`: SMTP (stdlib smtplib in a worker thread) and Amazon SES v2 SendEmail (httpx + `sigv4`).
* `push`: ntfy-style or Pushover-style HTTPS push (httpx).
* `relay`: `OutboxRelay`, the supervised loop that drains `notification_outbox` (SM6): episode dedupe,
  per-channel status, retries with min(60, 2^n) s back-off, escalation to the other channel after 5
  failures, the delivered flag, and a re-page cadence that follows the installed ruleset.
* `factory`: builds the channels from settings.
* `rotation`: `RotatingChannels`, channels rebuilt when the credentials read through the secrets source
  change (periodic refresh, re-read on an auth error).

Nothing here touches a broker; sending an alert is the only externally visible action, and only the
leader does it (the relay checks the lease synchronously before every send).
"""
