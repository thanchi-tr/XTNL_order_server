"""Live review round 2: push.fit_lines keeps a single hard-cut path (no unreachable duplicate after its return).

Requirement: src/notifications/push.py module doc (a body over the backend limit is cut on a line boundary by
fit_lines and marked); the round-1 fix for 'Pushover push silently truncates multi-item ARM notices'.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from src.notifications import push


def _unreachable_after_return(source: str, func_name: str) -> list[int]:
    """Line numbers of statements that follow a `return` in the same block of `func_name`."""
    tree = ast.parse(source)
    found: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            for block_owner in ast.walk(node):
                for field in ("body", "orelse", "finalbody"):
                    block = getattr(block_owner, field, None)
                    if not isinstance(block, list):
                        continue
                    for i, stmt in enumerate(block):
                        if isinstance(stmt, ast.Return) and i + 1 < len(block):
                            found.extend(s.lineno for s in block[i + 1:])
    return found


def test_fit_lines_has_no_code_after_its_final_return() -> None:
    source = textwrap.dedent(inspect.getsource(push))
    assert _unreachable_after_return(source, "fit_lines") == []


def test_fit_lines_hard_cut_still_marked_and_within_limit() -> None:
    # the one remaining hard-cut path: a first line too long on its own is cut and still marked
    body = "x" * 300 + "\nsecond line\nthird line"
    out = push.fit_lines(body, 120)
    assert len(out) <= 120
    assert out.endswith(push.TRUNCATION_MARKER.format(n=2))
    out_b = push.fit_lines("é" * 300 + "\nmore", 120, in_bytes=True)
    assert len(out_b.encode("utf-8")) <= 120
    assert out_b.endswith(push.TRUNCATION_MARKER.format(n=1))
