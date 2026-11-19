#!/usr/bin/env python3
"""PreToolUse guard: blocks outbound order execution in Phase 1 (read-only).

Paths are normalised (Windows backslashes -> '/') before matching, so the guard
applies on every platform.

Two layers:
* FORBIDDEN_PATTERNS apply to every file under src/ (unchanged, repo-wide).
* The ARM read-only lockout name ban (design §B3) applies only to files in
  LOCKOUT_SCOPE and NAME_BAN_FILES. It is a copy of src/riskmgr/lockout.py
  (LOCKOUT_SCOPE, NAME_BAN_FILES, NAME_BAN_PATTERN, DEF_NAME_RE, normalise_name,
  in_name_ban_scope); tests/hooks/test_pre_tool_guard_parity.py keeps them equal.
"""
import json
import re
import sys

# Any of these, written anywhere under src/, is an order-changing capability.
FORBIDDEN_PATTERNS = [
    (r"\bdef\s+(place_order|modify_order|cancel_order|submit_order|close_position|amend_order|update_order)\b",
     "Phase 1 Read-Only Lockout: outbound order execution methods are prohibited."),
    # StoneX order-changing endpoints (adapter-isolation.md §4) and cTrader order messages.
    (r"/(newtradeorder|newstoplimitorder|updatetradeorder|updatestoplimitorder|cancel)\b(?!_)",
     "Phase 1 Read-Only Lockout: order-changing broker endpoints may only appear in the deny-list module."),
    (r"\bProtoOA(NewOrderReq|AmendOrderReq|AmendPositionSLTPReq|ClosePositionReq|CancelOrderReq)\b",
     "Phase 1 Read-Only Lockout: cTrader order-changing messages are prohibited."),
]
# The deny-list itself must name the blocked paths.
DENY_LIST_FILES = ("src/adapters/transport.py",)

# ---- ARM read-only lockout name ban: copy of src/riskmgr/lockout.py (keep equal; the parity test checks it).
LOCKOUT_SCOPE = ("src/riskmgr", "src/marketdata", "src/armconfig")
NAME_BAN_FILES = ("src/runtime/md_ports.py",)
NAME_BAN_PATTERN = re.compile(
    r"(^|_)(place|modify|cancel|submit|close|amend|update|move|set|replace|adjust|trail|send|execute|edit|open"
    r"|create|new)_(order|stop|stop_loss|sl|sltp|position|trade)s?($|_)"
)
DEF_NAME_RE = re.compile(r"\b(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)")
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|(?<=[A-Za-z])(?=[0-9])")
_UNDERSCORE_RUN = re.compile(r"_+")


def normalise_name(name: str) -> str:
    """Leading underscores stripped, CamelCase split into snake_case, runs of `_` collapsed, lower-cased."""
    return _UNDERSCORE_RUN.sub("_", _CAMEL_BOUNDARY.sub("_", name.lstrip("_"))).lower()


def in_name_ban_scope(path: str) -> bool:
    """True for a path inside LOCKOUT_SCOPE or equal to a NAME_BAN_FILES entry (any absolute prefix, either slash,
    any case: NTFS resolves SRC/RiskMgr to src/riskmgr)."""
    p = "/" + path.replace("\\", "/").lower()
    return any(f"/{d}/" in p for d in LOCKOUT_SCOPE) or any(p.endswith(f"/{f}") for f in NAME_BAN_FILES)


def _norm(path: str) -> str:
    return path.replace("\\", "/")


def _new_text(tool_input: dict) -> str:
    """The text a Write / Edit / MultiEdit would add (each edit's new_string, not its JSON encoding)."""
    parts = [tool_input.get("new_string") or "", tool_input.get("content") or ""]
    for edit in tool_input.get("edits", []) or []:
        if isinstance(edit, dict):
            parts.append(edit.get("new_string") or "")
    return "\n".join(p for p in parts if isinstance(p, str))


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool_input = payload.get("tool_input", {}) or {}
    file_path = _norm(tool_input.get("file_path", "") or tool_input.get("path", "") or "")
    # Each edit's plain new_string as well as the JSON form: in JSON an escaped newline leaves no \b before `def`.
    content = _new_text(tool_input) + "\n" + json.dumps(tool_input.get("edits", []))

    base = file_path.rsplit("/", 1)[-1]
    if base.startswith(".env"):
        print("BLOCKED: direct modification of .env files is prohibited.", file=sys.stderr)
        return 2

    if "/src/" in f"/{file_path}" and file_path.endswith(".py"):
        for pattern, message in FORBIDDEN_PATTERNS:
            if pattern.startswith("/(") and any(file_path.endswith(f) for f in DENY_LIST_FILES):
                continue
            if re.search(pattern, content):
                print(f"HOOK BLOCKED [{file_path}]: {message}", file=sys.stderr)
                return 2

    if file_path.endswith(".py") and in_name_ban_scope(file_path):
        banned = sorted({m.group(1) for m in DEF_NAME_RE.finditer(_new_text(tool_input))
                         if NAME_BAN_PATTERN.search(normalise_name(m.group(1)))})
        if banned:
            print(f"HOOK BLOCKED [{file_path}]: Phase 1 Read-Only Lockout: the ARM / market-data name ban refuses "
                  f"{', '.join(banned)} (design §B3; naming convention in .claude/rules/riskmgr.md).", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
