"""Audited operator CLI (backlog 4.6): `python -m src.ops <group> <command>` (see src/ops/cli.py).

Phase 1 boundary: no command contacts a broker or enables any order action. Every mutating command is
written with its `operator_audit` row in one transaction, fenced on the current engine epoch.
"""
