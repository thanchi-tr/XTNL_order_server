"""Home-server review round 7 findings, docs side.

1. Backup Set-up 8 names the real exit-78 line when the job's stdin does not reach the container
   ("did not end with END"), and keeps "missing setting(s)" for END arriving without a secret line.
2. deadman-alert "Two engines": a second engine more than 5 min off is recorded per source (listed in the page,
   can raise node_changed) but cannot raise two_engines; only an unparseable body never counts as a source.
3. operations.md W1 rule carries the 6 h exception for an inherited receiver-down episode.
4. deadman-alert token rotation with XTNL_SECRETS_SOURCE=env: no `tokens rotated` line is logged; the gate is the
   restart, and the env readers are sent to deadman-receiver-home.md "Token rotation".
5. The W18 etcd token form is unique per rebuild (date and time), not a date alone.

Text checks only, plus a cross-check of the quoted strings against the code that prints them.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESTORE = ROOT / "docs" / "runbooks" / "home-backup-restore.md"
ALERT = ROOT / "docs" / "runbooks" / "deadman-alert.md"
SINGLE_PC = ROOT / "docs" / "runbooks" / "single-pc-autostart.md"
OPERATIONS = ROOT / ".claude" / "rules" / "operations.md"
ENTRYPOINT = ROOT / "deploy" / "local" / "backup" / "entrypoint.sh"
DEADMAN = ROOT / "src" / "runtime" / "deadman.py"
EVALUATE = ROOT / "src" / "deadman_receiver" / "evaluate.py"
RECEIVER_MAIN = ROOT / "src" / "deadman_receiver" / "__main__.py"


def _flat(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def _section(text: str, start: str, end: str) -> str:
    flat = _flat(text)
    i = flat.index(start)
    return flat[i:flat.index(end, i + len(start))]


# 1. The stdin-not-attached case names the line the job really prints.

STDIN_LINE = "the secrets on stdin did not end with END (is the job's stdin attached?)"


def test_setup8_names_the_real_line_for_an_unattached_stdin() -> None:
    hint = _section(RESTORE.read_text(encoding="utf-8"), "If `-Check` ends with exit 78",
                    "The first run builds the image")
    assert hint.startswith(f'If `-Check` ends with exit 78 "{STDIN_LINE}", the job\'s stdin did not reach')
    # "missing setting(s)" is kept only for END arriving without a secret line
    assert '"missing setting(s)" for the secret names is a different case' in hint
    assert "the `END` line arrived but a secret line was missing" in hint
    assert hint.index(STDIN_LINE) < hint.index("missing setting(s)")


def test_setup8_quotes_match_the_entrypoint() -> None:
    src = ENTRYPOINT.read_text(encoding="utf-8")
    assert f'die 78 "{STDIN_LINE}"' in src
    assert 'die 78 "missing setting(s):' in src
    # the END check comes before the missing-settings check, so an empty stdin never reaches it
    assert src.index(STDIN_LINE) < src.index('"missing setting(s):')


# 2. A skewed second engine is a source; only an unparseable one is not.

def test_two_engines_skewed_second_engine_is_recorded_per_source() -> None:
    text = _flat(ALERT.read_text(encoding="utf-8"))
    assert "whose beats do not parse (an older checkout that lacks a field) or whose clock is more than" not in text
    assert "or whose clock is more than 5 min off never counts as a source" not in text
    entry = _section(ALERT.read_text(encoding="utf-8"), "A second engine whose clock is more than 5 min off",
                     "That page resolves 40 s after")
    assert "is still recorded per source" in entry
    assert "`beats in the last 60 s: <node> epoch <epoch> xN`" in entry
    assert "can raise `beating node changed`" in entry
    assert "It cannot raise `two engines beating`, because its `sent_at_ms` are more than 5 min apart" in entry
    assert "Only a second engine whose beats do not parse" in entry and "never counts as a source" in entry


def test_evaluator_records_skewed_beats_per_source() -> None:
    src = EVALUATE.read_text(encoding="utf-8")
    assert "including one refused as out of order or skewed" in _flat(src)


# 3. The W1 rule carries the 6 h exception.

def test_operations_w1_rule_has_the_six_hour_exception() -> None:
    rule = _section(OPERATIONS.read_text(encoding="utf-8"), "**Receiver not acknowledging**",
                    "A set `XTNL_DEADMAN_URL`")
    assert "so a new term neither re-pages it nor loses its recovery page." not in rule
    assert "a new term within 6 h of the episode's page neither re-pages it nor loses its recovery page" in rule
    assert "an inherited episode paged more than 6 h earlier" in rule
    assert "is paged again by a term whose own beats fail for `XTNL_DEADMAN_ALERT_AFTER_S`" in rule


def test_six_hour_exception_matches_the_code() -> None:
    src = DEADMAN.read_text(encoding="utf-8")
    m = re.search(r"INHERITED_EPISODE_MAX_AGE_MS\b[^=\n]*=\s*([0-9_ *]+)", src)
    assert m, "INHERITED_EPISODE_MAX_AGE_MS not found"
    assert eval(m.group(1).replace("_", "")) == 6 * 3600 * 1000      # noqa: S307 - a literal from our own source


# 4. Env-source token rotation: no `tokens rotated` line; the restart is the gate.

def test_env_source_rotation_does_not_wait_for_tokens_rotated() -> None:
    text = ALERT.read_text(encoding="utf-8")
    assert "With `XTNL_SECRETS_SOURCE=env` (the env file) the same three steps apply" not in _flat(text)
    para = _section(text, "With `XTNL_SECRETS_SOURCE=env` (the env file)", "### Dashboard heartbeat copy token")
    assert "logs no `tokens rotated: current + next` line, not even after a restart" in para
    assert "Step 1's gate is the `systemctl restart xtnl-deadman-receiver` itself" in para
    assert "`authenticated with the next token` appears only after the engine is restarted" in para
    assert '[deadman-receiver-home.md](deadman-receiver-home.md) "Token rotation" only' in para


def test_env_source_token_ring_has_no_reload() -> None:
    src = RECEIVER_MAIN.read_text(encoding="utf-8")
    rings = [ln for ln in src.splitlines() if "TokenRing(" in ln]
    # the env source builds its ring with no `load`, so TokenRing.refresh() never logs "tokens rotated"
    assert any("load=" not in ln for ln in rings), rings
    assert "tokens rotated" in (ROOT / "src" / "deadman_receiver" / "tokens.py").read_text(encoding="utf-8")


# 5. The etcd token is unique per rebuild.

def test_w18_token_form_is_unique_per_rebuild() -> None:
    text = SINGLE_PC.read_text(encoding="utf-8")
    flat = _flat(text)
    assert "which no earlier rebuild can have used" not in flat
    assert "(a new date form)" not in flat
    assert "`xtnl-local-rYYYYMMDD-HHMM`" in flat and "unique per rebuild" in flat
    assert "a second rebuild on the same day" in flat
    assert "$env:XTNL_ETCD_CLUSTER_TOKEN = 'xtnl-local-r20261122-0930'" in text
    assert "if that form was already used, add a suffix such as `-2`" in flat
    restore = _flat(RESTORE.read_text(encoding="utf-8"))
    assert "use today's date, `xtnl-local-rYYYYMMDD`." not in restore
    assert "`xtnl-local-rYYYYMMDD-HHMM` (a date alone repeats on a second rebuild the same day)" in restore
