"""Shared fixtures for the whole suite.

The Oversee stop rule is a process-wide setting (`XTNL_OVERSEE_STOP_AT_ENTRY`, plan S38f): off by default, the exact
pre-S07 rule. Every test starts with it off and gets it reset afterwards; a test of the S07 / PR1 kind rules asks for
`stop_at_entry_on` (or marks its module with `pytest.mark.usefixtures("stop_at_entry_on")`).
"""
from __future__ import annotations

from collections.abc import Iterator

import pytest

from src.oversee import risk


@pytest.fixture(autouse=True)
def _stop_rule_default() -> Iterator[None]:
    before = risk.stop_at_entry()
    risk.set_stop_at_entry(False)
    try:
        yield
    finally:
        risk.set_stop_at_entry(before)


@pytest.fixture
def stop_at_entry_on(_stop_rule_default: None) -> Iterator[None]:
    """The S07 / PR1 Oversee stop rule (`XTNL_OVERSEE_STOP_AT_ENTRY=1`) for one test."""
    risk.set_stop_at_entry(True)
    yield
