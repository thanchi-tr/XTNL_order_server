"""Melbourne dead-man receiver (operations.md §9 "Push dead-man's switch"; IMPLEMENTATION_PLAN 6.4).

A small, separate stdlib-asyncio service (`python -m src.deadman_receiver`, unit
`deploy/systemd/xtnl-deadman-receiver.service`) on the Melbourne host. It accepts the engine leader's 15 s
heartbeat (`src.runtime.deadman.HttpsDeadmanTransport`: HTTPS POST, bearer token, `DeadmanPayload` JSON),
evaluates freshness and the payload's health fields (`evaluate`), and pages through the existing
`src.notifications` transports configured for a channel outside eu-west-2 (`settings`, `pager`), with a daily
test page (`service`).

It is independent of the engine, the outbox and London: it never touches PostgreSQL or Redis, never reaches a
broker, and imports nothing from `src.db`, `src.cache` or `src.runtime` (a test asserts it).
"""
