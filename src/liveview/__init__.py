"""Local read-only live view of what the order-server is doing (docs/design/live-view.md).

A separate process (`python -m src.liveview`) on 127.0.0.1 that polls the privacy-safe `lv_` views of migration 0021
through the read-only `xtnl_liveview` login and the engine's loopback metrics, and serves one page plus a static
Phase 2 shell. It never sends orders and has no write path of any kind.
"""
