"""Melbourne read-only dashboard (operations.md §9 Dashboard; IMPLEMENTATION_PLAN 5.6).

`python -m src.dashboard` (its own process and systemd unit, `deploy/systemd/xtnl-dashboard.service`): a small
server-rendered HTML page plus JSON endpoints over

* PostgreSQL entity views (orders, positions, accounts, halt state, active rule states, DLQ depth, stream
  health, leader term, escalated alerts) through a read-only connection (role `xtnl_dashboard`, every session
  `default_transaction_read_only = on`), and
* Redis live values (prices, account live fields) only from the Redis whose `redis:boot_id` epoch equals the
  epoch of the current leader term in PostgreSQL, each with its age and hidden once older than the freshness
  limit; otherwise they are marked stale.

There is no write path: GET / HEAD only, read-only SQL, Redis reads only. Phase 1 never sends orders.
"""
