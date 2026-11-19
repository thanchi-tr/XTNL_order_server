#!/usr/bin/env python3
"""PostToolUse verifier: runs the deterministic invariant suite when core modules change.

Paths are normalised (Windows backslashes -> '/'). The suite is run with the
project's virtualenv interpreter when one exists, else the current interpreter.
"""
import json
import os
import subprocess
import sys

WATCHED_DIRS = ("src/dto/", "src/adapters/", "src/pubsub/", "src/integrity/", "src/oversee/", "src/cache/", "src/db/",
                "src/riskmgr/", "src/marketdata/", "src/armconfig/", "src/runtime/", "src/notifications/")
SCRIPT = os.path.join(".claude", "skills", "live-trading-server", "scripts", "verify_execution.py")


def _python() -> str:
    for cand in (os.path.join(".venv", "Scripts", "python.exe"), os.path.join(".venv", "bin", "python")):
        if os.path.exists(cand):
            return cand
    return sys.executable


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool_input = payload.get("tool_input", {}) or {}
    file_path = (tool_input.get("file_path", "") or tool_input.get("path", "") or "").replace("\\", "/")

    if file_path.endswith(".py") and any(watched in file_path for watched in WATCHED_DIRS):
        result = subprocess.run([_python(), SCRIPT], capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"POST-TOOL INTEGRITY VERIFICATION FAILED:\n{result.stdout}\n{result.stderr}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
