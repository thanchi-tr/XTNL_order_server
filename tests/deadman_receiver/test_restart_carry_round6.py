"""Review round 6: a condition that carries on through an engine restart or a failover stays one episode.

The finding: since round 5 each (node, epoch) source kept its own duration start times. An engine restart gives a
new epoch, so the new term started every duration from zero, while the old term's view kept the condition active
only until it was dropped, SOURCE_LIVE_MS after its last beat. An adapter stuck in BACKOFF across a restart then
gave a false resolved notice and a second critical page about 110 s later. Now the new source takes over the start
times of the conditions its beat also shows, when the old source's last beat came before the new source's first.
"""
from __future__ import annotations

from typing import Any

from src.deadman_receiver.evaluate import Evaluator
from src.runtime.deadman import AdapterBeat
from tests.deadman_receiver.test_evaluate import T0
from tests.deadman_receiver.test_two_engines_conditions_round5 import S, feed, flips, interleaved

BACKOFF = {"stonex": AdapterBeat("BACKOFF", True, None)}
STREAMING = {"stonex": AdapterBeat("STREAMING", True, None)}
NOT_STREAMING = "adapter_not_streaming:stonex"

Plan = list[tuple[int, dict[str, Any]]]


def restart(old: dict[str, Any], new: dict[str, Any], *, old_until_s: int = 300, new_from_s: int = 330,
            until_s: int = 900) -> Plan:
    """`old` beats every 15 s from 0 s to `old_until_s`, then `new` beats every 15 s from `new_from_s`."""
    plan = [(k * 15 * S, old) for k in range(old_until_s // 15 + 1)]
    plan += [(t * S, new) for t in range(new_from_s, until_s, 15)]
    return plan


def test_adapter_not_streaming_through_an_engine_restart_pages_once() -> None:
    """The finding's reproduction: same node, epoch 5 then epoch 6, adapter in BACKOFF throughout."""
    plan = restart({"node": "pc1", "epoch": "5", "adapters": BACKOFF},
                   {"node": "pc1", "epoch": "6", "adapters": BACKOFF})
    assert flips(plan, NOT_STREAMING, until_s=900) == [(120, True)]


def test_spill_and_sm8_through_an_engine_restart_page_once() -> None:
    for key, kw in (("spill_nonempty", {"spill_bytes": 10}), ("sm8_not_ready", {"sm8_state": "REBUILDING"}),
                    ("sm5_unknown", {"sm5_state": "UNKNOWN"})):
        plan = restart({"node": "pc1", "epoch": "5", **kw}, {"node": "pc1", "epoch": "6", **kw})
        assert flips(plan, key, until_s=900) == [(60, True)], key


def test_a_condition_that_began_before_the_restart_is_not_delayed() -> None:
    """BACKOFF from 270 s: the page comes 120 s later (390 s), not a full threshold after the new term's start."""
    plan = [(k * 15 * S, {"node": "pc1", "epoch": "5", "adapters": BACKOFF if k * 15 >= 270 else STREAMING})
            for k in range(21)]
    plan += [(t * S, {"node": "pc1", "epoch": "6", "adapters": BACKOFF}) for t in range(330, 900, 15)]
    assert flips(plan, NOT_STREAMING, until_s=900) == [(390, True)]


def test_through_a_failover_to_another_node_pages_once() -> None:
    plan = restart({"node": "aws-a", "epoch": "5", "adapters": BACKOFF},
                   {"node": "aws-b", "epoch": "9", "adapters": BACKOFF})
    assert flips(plan, NOT_STREAMING, until_s=900) == [(120, True)]


def test_a_condition_the_new_term_does_not_show_still_resolves() -> None:
    """The new term streams: nothing carries over, the old term's page resolves SOURCE_LIVE_MS after its beat."""
    plan = restart({"node": "pc1", "epoch": "5", "adapters": BACKOFF},
                   {"node": "pc1", "epoch": "6", "adapters": STREAMING})
    changes = flips(plan, NOT_STREAMING, until_s=900)
    assert [c[1] for c in changes] == [True, False]
    assert changes[1][0] <= 300 + 41


def test_a_new_condition_in_the_new_term_starts_its_own_duration() -> None:
    """Only keys the old term showed carry over: a fault first seen in the new term waits its full threshold."""
    plan = restart({"node": "pc1", "epoch": "5"}, {"node": "pc1", "epoch": "6", "spill_bytes": 10})
    assert flips(plan, "spill_nonempty", until_s=900) == [(390, True)]


def test_interleaving_engines_keep_separate_start_times() -> None:
    """Two engines (W19): the spare's spill condition starts its own duration, and the main box's beats (which do
    not show it) do not reset it. Round 9: one start time per condition key, held while any engine shows it."""
    ev = Evaluator(started_ms=T0)
    plan = interleaved({"node": "pc1", "epoch": "5"}, {"node": "pc1", "epoch": "3", "spill_bytes": 10},
                       until_s=300)
    for t, kw in plan:
        feed(ev, rx=T0 + t, **kw)
    ep = ev._episodes["spill_nonempty"]
    assert ep.since == T0 + 8 * S
    assert (ep.beat.node, ep.beat.epoch) == ("pc1", "3")
