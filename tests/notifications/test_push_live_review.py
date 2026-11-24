"""Push bodies over the backend limit are cut on a line boundary with a marker, never mid-line (live review F1).

Pushover takes 1024 characters, ntfy 4096 bytes (`NTFY_MAX_BODY` 4000, measured in UTF-8 bytes). A multi-item ARM
notice can be longer; a silent `[:1024]` cut dropped later items mid-line with no sign the push was incomplete.
"""
from __future__ import annotations

import re
import urllib.parse

import httpx

from src.notifications.push import (
    NTFY_MAX_BODY,
    PUSHOVER_MAX_MESSAGE,
    NtfyTransport,
    PushoverTransport,
    fit_lines,
)
from src.notifications.settings import PushSettings
from src.oversee.notify import AlertMessage

MARK = re.compile(r"\n\.\.\. \(truncated: (\d+) more lines, see email\)\Z")


def _client(handler: object) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]


def _lines(n: int, width: int, fill: str = "y") -> str:
    return "\n".join(f"line {k:03d} " + fill * width for k in range(n))


def _check_cut(original: str, cut: str, limit: int, *, in_bytes: bool = False) -> None:
    size = len(cut.encode("utf-8")) if in_bytes else len(cut)
    assert size <= limit
    m = MARK.search(cut)
    assert m is not None, cut[-80:]
    kept = cut[:m.start()].split("\n")
    orig = original.split("\n")
    assert kept == orig[:len(kept)], "every kept line is a whole line of the original"
    assert int(m.group(1)) == sum(1 for ln in orig[len(kept):] if ln.strip())


def test_a_body_within_the_limit_is_unchanged() -> None:
    body = _lines(5, 20)
    assert fit_lines(body, PUSHOVER_MAX_MESSAGE) == body
    exact = "z" * PUSHOVER_MAX_MESSAGE
    assert fit_lines(exact, PUSHOVER_MAX_MESSAGE) == exact


def test_a_long_multi_line_body_is_cut_on_a_line_boundary() -> None:
    body = _lines(30, 60)
    assert len(body) > PUSHOVER_MAX_MESSAGE
    cut = fit_lines(body, PUSHOVER_MAX_MESSAGE)
    _check_cut(body, cut, PUSHOVER_MAX_MESSAGE)
    assert "line 000" in cut and "line 029" not in cut


def test_blank_lines_are_not_counted_as_dropped() -> None:
    body = _lines(20, 70) + "\n\nkind: arm_notice\nseverity: high"
    cut = fit_lines(body, PUSHOVER_MAX_MESSAGE)
    _check_cut(body, cut, PUSHOVER_MAX_MESSAGE)


def test_a_single_overlong_line_is_hard_cut_and_still_marked() -> None:
    cut = fit_lines("q" * 3000 + "\nsecond", PUSHOVER_MAX_MESSAGE)
    assert len(cut) == PUSHOVER_MAX_MESSAGE
    assert cut.endswith("\n... (truncated: 1 more lines, see email)")


def test_multibyte_text_is_fitted_in_bytes_for_ntfy() -> None:
    body = _lines(60, 40, fill="é")  # 2 bytes per é: well over 4000 bytes but under 4000 characters
    assert len(body) < NTFY_MAX_BODY * 2 and len(body.encode("utf-8")) > NTFY_MAX_BODY
    cut = fit_lines(body, NTFY_MAX_BODY, in_bytes=True)
    _check_cut(body, cut, NTFY_MAX_BODY, in_bytes=True)
    one_line = fit_lines("é" * 5000, NTFY_MAX_BODY, in_bytes=True)
    assert len(one_line.encode("utf-8")) <= NTFY_MAX_BODY and MARK.search(one_line)


async def test_pushover_sends_the_line_cut_body_and_never_a_partial_line() -> None:
    seen: list[dict[str, list[str]]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(urllib.parse.parse_qs(req.content.decode()))
        return httpx.Response(200, json={"status": 1, "request": "r"})

    lines = [f"P{k}: HOLD, the bar end did not meet the trigger " + "." * 150 for k in range(6)]
    lines.insert(2, "P1a: ADJUST: move the stop to the proposed level on handles O1, O2 (read 10:15:03)")
    body = "\n".join(lines) + "\n\nkind: arm_notice\nseverity: high\nalert id: 7"
    s = PushSettings("pushover", "https://api.pushover.net/1/messages.json", token="t", user="u")
    await PushoverTransport(s, _client(handler)).deliver(AlertMessage("[XTNL] ARM #7", body, "high", "info"))
    sent = seen[0]["message"][0]
    _check_cut(body, sent, PUSHOVER_MAX_MESSAGE)
    for ln in sent[:MARK.search(sent).start()].split("\n"):  # type: ignore[union-attr]
        assert ln in lines or ln == "", "no line is cut mid-way"
    assert "ADJUST: move the stop" in sent


async def test_ntfy_sends_the_byte_fitted_body() -> None:
    sent: list[bytes] = []

    def handler(req: httpx.Request) -> httpx.Response:
        sent.append(req.content)
        return httpx.Response(200)

    body = _lines(80, 40, fill="é")
    s = PushSettings("ntfy", "https://ntfy.example/topic")
    await NtfyTransport(s, _client(handler)).deliver(AlertMessage("t", body, "info", "info"))
    assert len(sent[0]) <= NTFY_MAX_BODY
    _check_cut(body, sent[0].decode("utf-8"), NTFY_MAX_BODY, in_bytes=True)
