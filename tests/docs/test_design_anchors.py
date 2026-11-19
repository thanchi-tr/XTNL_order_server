"""Every `path::symbol` anchor in the ARM design resolves (plan S38b / S38c).

The design (docs/design/automated-risk-manager.md) cites code as `path::symbol` anchors. This test parses each
cited Python file with `ast` and checks that the symbol is a def, class or assignment there; a dotted symbol
(`Class.member`) is looked up inside the class body. Stdlib only; nothing is imported from the cited files.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DESIGN = ROOT / "docs" / "design" / "automated-risk-manager.md"

ANCHOR = re.compile(r"`([A-Za-z0-9_./-]+\.py)::([A-Za-z0-9_.]+)`")
BARE_ANCHOR = re.compile(r"`([A-Za-z0-9_-]+\.py)::[A-Za-z0-9_.]+`")


def _anchors() -> list[tuple[str, str]]:
    found = ANCHOR.findall(DESIGN.read_text(encoding="utf-8"))
    return sorted(set(found))


def _names(body: list[ast.stmt]) -> dict[str, ast.stmt]:
    names: dict[str, ast.stmt] = {}
    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names[node.name] = node
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names[target.id] = node
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names[node.target.id] = node
    return names


def _resolves(path: str, symbol: str) -> bool:
    source = ROOT / path
    if not source.is_file():
        return False
    body: list[ast.stmt] = ast.parse(source.read_text(encoding="utf-8")).body
    parts = symbol.split(".")
    for i, part in enumerate(parts):
        node = _names(body).get(part)
        if node is None:
            return False
        if i < len(parts) - 1:
            if not isinstance(node, ast.ClassDef):
                return False
            body = node.body
    return True


def test_design_has_anchors() -> None:
    assert len(_anchors()) >= 40


def test_no_bare_filename_anchor() -> None:
    bare = BARE_ANCHOR.findall(DESIGN.read_text(encoding="utf-8"))
    assert bare == [], f"anchors must give the full path: {bare}"


@pytest.mark.parametrize(("path", "symbol"), _anchors())
def test_anchor_resolves(path: str, symbol: str) -> None:
    assert _resolves(path, symbol), f"`{path}::{symbol}` does not resolve to a def, class or assignment"
