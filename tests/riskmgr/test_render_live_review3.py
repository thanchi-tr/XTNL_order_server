"""Live review round 3: round 2's second COMPLETE text is reverted (design §B8 "Release texts", §20 Q6, §B6.1).

- §B8 has one `COMPLETE` text; Q6 calls the target "break-even" at the entry and at the entry plus a buffer. No
  body key chooses another text, and a body is never written with `release_at_entry`.
- A body the round-2 engine already wrote with `release_at_entry` still decodes (the key is accepted and ignored),
  renders the §B8 text, and re-encodes without the key.
- The §B6.1 `SlStrategy` interface has no `target_is_entry`, and the pinned `BreakEvenOnRiskClose` v1 does not
  provide one (`riskmgr.md` §6: a pinned version is not edited).

Synthetic ids and levels only.
"""
from __future__ import annotations

import json

import pytest

import src.riskmgr.notices as notices
import src.riskmgr.render as render
from src.riskmgr.render import (
    COMPLETE_TEXT,
    ITEM_KEYS,
    ItemBody,
    NoticeBody,
    RenderSettings,
    decode_body,
    encode_body,
    outcome_text,
    q10_text,
)
from src.riskmgr.strategies.break_even_on_risk_close import BREAK_EVEN_ON_RISK_CLOSE_V1
from src.riskmgr.strategy import SlStrategy

TF = 900_000


def _complete(side: str = "BUY", policy: str | None = "TELL_ONLY") -> ItemBody:
    return ItemBody(decision_id=1, decision_bar_end_ms=TF, adapter_id="mock", position_id="P1", symbol="SYN.A",
                    side=side, event="POST", bar_end_ms=TF, outcome="COMPLETE", category="INFO",
                    loosen_policy=policy)


def _round2_body(release_at_entry: object) -> str:
    """A body as the round-2 engine wrote it: the current encoding plus its additive `release_at_entry` key."""
    raw = json.loads(encode_body(NoticeBody(notice_id=9, route="main", event="POST", bar_end_ms=TF, created_ms=TF,
                                            items=(_complete(),))))
    raw["items"][0]["release_at_entry"] = release_at_entry
    return json.dumps(raw, sort_keys=True, separators=(",", ":"))


@pytest.mark.parametrize("stored", [True, False, None])
def test_a_round2_body_with_release_at_entry_decodes_and_renders_the_b8_text(stored: object) -> None:
    again = decode_body(_round2_body(stored))
    assert outcome_text(again.items[0], RenderSettings()) == f"{COMPLETE_TEXT} ({q10_text('BUY', False)})"
    assert "release_at_entry" not in encode_body(again)                  # ignored on decode, never written again
    assert not hasattr(again.items[0], "release_at_entry")


def test_no_body_key_selects_a_second_release_text() -> None:
    assert "release_at_entry" not in ITEM_KEYS
    assert not hasattr(render, "COMPLETE_TARGET_TEXT") and "COMPLETE_TARGET_TEXT" not in render.__all__
    assert not hasattr(notices, "pinned_target_is_entry") and "pinned_target_is_entry" not in notices.__all__
    body = encode_body(NoticeBody(notice_id=9, route="main", event="POST", bar_end_ms=TF, created_ms=TF,
                                  items=(_complete("SELL", "REARM"),)))
    assert "release_at_entry" not in body


def test_the_strategy_contract_has_no_target_is_entry() -> None:
    assert "target_is_entry" not in vars(SlStrategy)
    assert not hasattr(BREAK_EVEN_ON_RISK_CLOSE_V1, "target_is_entry")
