"""The PreToolUse hook's copy of the ARM lockout name ban equals `src/riskmgr/lockout.py` (design §B3; plan S11).

The hook is loaded with `importlib.util`. Until the user approves the S11 hook diff
(`docs/design/proposed-claude-changes/S11-lockout-scanner.md`), the hook has no lockout copy and the parity tests are
skipped with "hook update pending"; the pytest lockout scan of the real tree (`tests/riskmgr/test_lockout.py`) still
gates every stage. The repo-wide `FORBIDDEN_PATTERNS` and the `src/adapters/mock.py` case are checked either way.
"""
from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

from src.riskmgr import lockout

REPO = Path(__file__).resolve().parents[2]
HOOK_PATH = REPO / ".claude" / "hooks" / "pre_tool_guard.py"

# The repo-wide patterns as they stand; the hook may add to them but never narrow them (plan §1).
ORIGINAL_FORBIDDEN = (
    r"\bdef\s+(place_order|modify_order|cancel_order|submit_order|close_position|amend_order|update_order)\b",
    r"/(newtradeorder|newstoplimitorder|updatetradeorder|updatestoplimitorder|cancel)\b(?!_)",
    r"\bProtoOA(NewOrderReq|AmendOrderReq|AmendPositionSLTPReq|ClosePositionReq|CancelOrderReq)\b",
)

NAME_CORPUS = (
    "_place_order", "ModifyStop", "trail_stops", "StopHandleReadPortImpl", "HTTPServer", "ModifySLTP",
    "__place__order_", "held_positions", "arm_row_update", "mark_arm_closed", "break_even_on_risk_close",
    "close_eligible", "pre_basis", "StopBasis", "set_order", "open_positions", "NewTrade", "x", "_", "A1B2",
    "place_order2", "modify_stop2", "close_positions2", "Place0rder", "Sha256Digest",
)
PATH_CORPUS = (
    "C:\\repo\\src\\riskmgr\\x.py", "/repo/src/marketdata/store/k.py", "src/armconfig/__init__.py",
    "/repo/src/runtime/md_ports.py", "src/adapters/mock.py", "/repo/src/runtime/session.py", "src/riskmgrx/a.py",
    "tests/riskmgr/t.py", "src/riskmgr/lockout.py", "C:/r/SRC/RiskMgr/x.py", "C:\\R\\Src\\Runtime\\MD_Ports.py",
)


def _load(path: Path = HOOK_PATH) -> ModuleType:
    spec = importlib.util.spec_from_file_location("pre_tool_guard_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def hook() -> ModuleType:
    return _load()


@pytest.fixture
def updated_hook(hook: ModuleType) -> ModuleType:
    if not hasattr(hook, "LOCKOUT_SCOPE"):
        pytest.skip("hook update pending")
    return hook


def _run(hook: ModuleType, monkeypatch: pytest.MonkeyPatch, file_path: str, **tool_input: object) -> int:
    payload = {"tool_name": "Edit", "tool_input": {"file_path": file_path, **tool_input}}
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    result = hook.main()
    assert isinstance(result, int)
    return result


# ---------------------------------------------------------------------------------------------------- always checked
def test_forbidden_patterns_are_never_narrowed(hook: ModuleType) -> None:
    patterns = [p for p, _ in hook.FORBIDDEN_PATTERNS]
    for original in ORIGINAL_FORBIDDEN:
        assert original in patterns, original


def test_mock_adapter_set_order_edit_passes_the_hook(hook: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    path = str(REPO / "src" / "adapters" / "mock.py")
    text = "    def set_order(self, order: OrderDTO) -> None:\n        self._orders[order.key] = order\n"
    assert _run(hook, monkeypatch, path, old_string="x", new_string=text) == 0
    assert _run(hook, monkeypatch, path, old_string="x", new_string="    def set_position(self, p): ...\n") == 0


# ---------------------------------------------------------------------------------------------------- parity
def test_scope_and_pattern_equal_the_scanner(updated_hook: ModuleType) -> None:
    assert tuple(updated_hook.LOCKOUT_SCOPE) == lockout.LOCKOUT_SCOPE
    assert tuple(updated_hook.NAME_BAN_FILES) == lockout.NAME_BAN_FILES
    assert updated_hook.NAME_BAN_PATTERN.pattern == lockout.NAME_BAN_PATTERN.pattern
    assert updated_hook.DEF_NAME_RE.pattern == lockout.DEF_NAME_RE.pattern


def test_normaliser_and_scope_test_equal_the_scanner(updated_hook: ModuleType) -> None:
    for name in (*NAME_CORPUS, *lockout_plan_names()):
        assert updated_hook.normalise_name(name) == lockout.normalise_name(name), name
    for path in PATH_CORPUS:
        assert updated_hook.in_name_ban_scope(path) == lockout.in_name_ban_scope(path), path


def lockout_plan_names() -> tuple[str, ...]:
    from tests.riskmgr.test_lockout import PLAN_NAMES

    return PLAN_NAMES


@pytest.mark.parametrize("rel, text", [
    ("src/riskmgr/x.py", "def _place_order(self):\n    pass\n"),
    ("src/riskmgr/x.py", "class ModifyStop:\n    pass\n"),
    ("src/riskmgr/x.py", "def trail_stops(p):\n    pass\n"),
    ("src/marketdata/store/x.py", "async def send_trade():\n    pass\n"),
    ("src/armconfig/x.py", "class NewPositions:\n    pass\n"),
    ("src/runtime/md_ports.py", "class X:\n    def move_stop(self): ...\n"),
    ("src/riskmgr/x.py", "def place_order2(self):\n    pass\n"),
    ("SRC/RiskMgr/x.py", "def trail_stops(p):\n    pass\n"),       # NTFS resolves this to src/riskmgr
])
def test_hook_refuses_planted_names_in_scope(updated_hook: ModuleType, monkeypatch: pytest.MonkeyPatch,
                                             rel: str, text: str) -> None:
    path = str(REPO / rel)
    assert _run(updated_hook, monkeypatch, path, old_string="x", new_string=text) == 2
    assert _run(updated_hook, monkeypatch, path, content=text) == 2
    edits = [{"old_string": "a", "new_string": "x = 1"}, {"old_string": "b", "new_string": text}]
    assert _run(updated_hook, monkeypatch, path, edits=edits) == 2


@pytest.mark.parametrize("rel, text", [
    ("src/riskmgr/x.py", "def held_positions(self):\n    pass\nclass StopBasis:\n    pass\n"),
    ("src/riskmgr/x.py", "def arm_row_update(): ...\ndef mark_arm_closed(): ...\ndef close_eligible(): ...\n"),
    ("src/runtime/md_ports.py", "class StopHandleReadPortImpl:\n    pass\n"),
    ("src/runtime/session.py", "def trail_stops(p):\n    pass\n"),          # outside the scope
    ("src/adapters/mock.py", "def set_order(self, o):\n    pass\n"),          # outside the scope
    ("src/riskmgr/notes.md", "def trail_stops(p):\n"),                       # not Python
])
def test_hook_allows_convention_names_and_files_outside_scope(updated_hook: ModuleType,
                                                              monkeypatch: pytest.MonkeyPatch,
                                                              rel: str, text: str) -> None:
    assert _run(updated_hook, monkeypatch, str(REPO / rel), old_string="x", new_string=text) == 0


def test_multiedit_forbidden_pattern_is_blocked_anywhere_under_src(updated_hook: ModuleType,
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    # In the JSON encoding of `edits` an escaped newline leaves no word boundary before `def`; the hook reads each
    # edit's new_string as well (review S11).
    path = str(REPO / "src" / "adapters" / "x.py")
    edits = [{"old_string": "a", "new_string": "x = 1\ndef place_order(self)"}]
    assert _run(updated_hook, monkeypatch, path, edits=edits) == 2
    assert _run(updated_hook, monkeypatch, path, edits=[{"old_string": "a", "new_string": "x = 1\n"}]) == 0
