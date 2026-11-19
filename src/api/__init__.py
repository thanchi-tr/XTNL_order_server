"""Order-server API: the one network-facing listener of the single-PC server (docs/runbooks/order-server-api.md).

A small stdlib asyncio HTTPS server (python -m src.api) for the XTNL thesis website. It exposes three read-only
endpoints and nothing else:

    GET  /v1/health               liveness ({"ok": true, "service": "xtnl-order-server", "api": 1})
    POST /v1/pinescript/compile   start (or reuse) the compile of this ISO week's Pine Script order history
    GET  /v1/pinescript/latest    this week's compile: status and, when ready, one script per symbol

Phase 1 never sends orders: nothing here reaches a broker, and PostgreSQL is read in READ ONLY transactions
through the existing report code (src/reports). The compiled scripts live only in Redis (database
XTNL_API_REDIS_DB, default 2, never the engine's database), with an 8-day TTL; nothing is written to disk or to
PostgreSQL.

Every request is authenticated with an HMAC-SHA256 signature over method, path, timestamp, nonce and body hash
(shared secret XTNL_API_SECRET, never logged), within a 60 s clock window and a 120 s replay cache; requests are
rate limited per client IP, bounded in size and time (slow clients are cut off), and only the three routes
exist. Responses carry no internal detail. TLS 1.2+ with a self-signed certificate the website pins.
"""
