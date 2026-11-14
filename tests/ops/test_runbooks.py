"""The runbooks use the real operator CLI commands (no 'operator CLI pending' interim steps left)."""
from __future__ import annotations

import re
import shlex
from pathlib import Path

from src.ops.cli import build_parser

RUNBOOKS = Path(__file__).resolve().parents[2] / "docs" / "runbooks"


def test_no_pending_cli_interim_steps_remain() -> None:
    for f in RUNBOOKS.glob("*.md"):
        text = f.read_text(encoding="utf-8")
        assert not re.search(r"CLI pending|\(pending|operator CLI exists", text), f.name
        assert "UPDATE cluster_state" not in text, f"{f.name}: hand-written SAFE_HOLD SQL"


def test_halt_clear_runbook_shape_and_index() -> None:
    text = (RUNBOOKS / "halt-clear.md").read_text(encoding="utf-8")
    assert "never sends orders" in text and "```" in text
    headings = {h.strip().lower() for h in re.findall(r"^##\s+(.+)$", text, re.M)}
    assert {"trigger", "checks", "commands", "rollback"} <= headings
    assert "](halt-clear.md)" in (RUNBOOKS / "README.md").read_text(encoding="utf-8")


def test_every_documented_ops_command_parses() -> None:
    """Each `$OPS ...` line in the runbooks is a valid command line (placeholders filled in)."""
    parser = build_parser()
    seen = 0
    for f in RUNBOOKS.glob("*.md"):
        text = f.read_text(encoding="utf-8").replace("\\\n", " ")
        for line in re.findall(r"^\s*(?:XTNL_ETCD_ENDPOINTS=\S+(?: [^$]*)?\s+)?\$OPS (.+)$", text, re.M):
            line = line.split("#", 1)[0].strip()
            if "|" in line or line.startswith("<group>"):
                continue                                   # the README's synopsis lines
            line = re.sub(r"<[^>]*id[^>]*>", "7", line)    # numeric ids
            line = re.sub(r"<[^>]+>", "x-value", line)
            line = line.replace("[--approver x-value]", "")
            argv = shlex.split(line)
            parser.parse_args(argv)
            seen += 1
    assert seen >= 10
