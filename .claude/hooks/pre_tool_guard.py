#!/usr/bin/env python3
"""PreToolUse guard: blocks outbound order execution in Phase 1 (read-only).

Paths are normalised (Windows backslashes -> '/') before matching, so the guard
applies on every platform.
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


def _norm(path: str) -> str:
    return path.replace("\\", "/")


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool_input = payload.get("tool_input", {}) or {}
    file_path = _norm(tool_input.get("file_path", "") or tool_input.get("path", "") or "")
    content = (
        tool_input.get("new_string", "")
        or tool_input.get("content", "")
        or json.dumps(tool_input.get("edits", []))
    )

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
    return 0


if __name__ == "__main__":
    sys.exit(main())
