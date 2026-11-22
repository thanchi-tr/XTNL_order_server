"""Heartbeat parsing and the paging rules of the Melbourne dead-man receiver (operations.md §9).

Pure and synchronous: no I/O, no clock of its own (every call takes `now_ms`, the receiver's wall clock, and may
take `rx_ms`, the receiver's monotonic clock; without it `now_ms` is used for both). Every timer runs on `rx_ms`
(review round 7): silence, the condition episodes, the two-engine rules and the latches. Wall time is only shown in
texts and used by the skew gate before the first accepted beat or after a silence (Skew, below), so a receiver clock
step (an NTP correction) never starts or ends a page.

Skew (review rounds 8 and 9). A beat is refused as skewed (`payload_invalid`) when its `sent_at_ms` is more than
`MAX_CLOCK_SKEW_MS` (5 min) off its reference:

* while the engine is live (an accepted beat within `silence_ms`), the engine clock carried on from the latest
  accepted beat (that beat's `sent_at_ms` plus the receiver monotonic time since) is the only reference;
* after a silence, a beat within 5 min of either the carried engine clock or the receiver's wall clock passes;
* before the first accepted beat, a beat within 5 min of either the wall clock or the receiver's start wall time
  carried on by its monotonic clock passes (round 10: before, the wall clock alone decided, so a wall step between
  the receiver's start and the engine's first beat refused every beat for good: `payload_invalid`, then `silence`).

The gate stops a far-future `sent_at_ms` from becoming the newest beat (every later beat would be refused as "not
newer") and a sender whose own clock is off. The carried engine clock cannot be moved by a receiver wall step. Round
9: before, a beat within 5 min of the wall clock always passed, so after a receiver wall step of 4-10 min a second
sender 400 s off was refused only as "not newer" (no `payload_invalid`: a missed page or a false resolved notice),
or, 400 s ahead, became the newest beat and locked the healthy engine out (a false resolved notice, then a false
`silence` page that did not clear while the step stayed). An engine whose own clock steps by more than 5 min is
refused until `silence_ms` has passed without an accepted beat, then judged against the wall clock again.

`parse_beat` validates one POSTed payload against the engine's `src.runtime.deadman.DeadmanPayload` (every key
present, values of the right type; a JSON null is a value, an absent key is a malformed beat). The optional ARM
fields (`OPTIONAL_FIELDS`, design PR4 / §B14, plan S08) may be absent: the engine leaves them out while the ARM is
not running, so a beat without them (an older engine, or the ARM off) parses with them None.

`Evaluator` keeps the receiver's view and returns the set of **active conditions** at any instant:

| key                              | active when (thresholds from operations.md §9)                                   |
|----------------------------------|----------------------------------------------------------------------------------|
| `silence`                        | no accepted beat for > 60 s (counted from receiver start before the first beat)  |
| `payload_invalid`                | a POST was malformed, or its `sent_at_ms` is > 5 min off the receiver (Skew,      |
|                                  | below); held until an accepted beat came after it and none has come for           |
|                                  | `SOURCE_LIVE_MS` (below)                                                         |
| `adapter_not_streaming:<a>`      | SM4 not STREAMING for >= 2 min while `market_open` is true and not in maintenance |
| `adapter_fields_missing:<a>`     | `sm4_state` or `market_open` null in the latest beat (pages at once)              |
| `fields_missing:<field>`         | a health field null (or `adapters` empty) in the latest beat (pages at once)      |
| `feed_lag`                       | oldest un-acked feed entry > 30 s                                                 |
| `risksweep_pass_age`             | any account's RiskSweep pass > 10 s old                                           |
| `notification_undelivered`       | oldest undelivered alert > 60 s                                                   |
| `sm8_not_ready`                  | SM8 not READY for > 60 s                                                          |
| `sm5_unknown`                    | SM5 UNKNOWN for > 60 s                                                            |
| `spill_nonempty`                 | spill file non-empty for > 60 s                                                   |
| `circuit_open:<task>`            | the task is in the `degraded` list (its circuit is open)                          |
| `arm_failed`                     | `arm_failed.count` > 0 (ARM scopes in FAILED, from engine memory; pages at once)  |
| `arm_failed_unrecorded`          | `arm_failed.unrecorded` > 0 (FAILED episodes PostgreSQL has not recorded, held in |
|                                  | that node's memory and spool only; pages at once). Latched when the leader       |
|                                  | changes before the node reported them recorded: the episode may be lost with the |
|                                  | old leader, so it stays active for `arm_unrecorded_latch_ms` (1 h) or until a    |
|                                  | beat shows a FAILED scope again (S38c review)                                    |
| `arm_overdue_positions`          | `arm_overdue_positions` > 0 (no final ARM decision for > 2 OPEN bars)             |
| `arm_notification_undelivered`   | oldest undelivered ARM notice > 60 s                                              |
| `two_engines`                    | two engine sources (node, epoch) beat at the same time (W19, critical; below)     |
| `node_changed`                   | the beating node changed in the last 5 min (W19, severity high; below)            |

"When those fields are missing" (operations.md §9) pages on the first beat that lacks them (IMPLEMENTATION_PLAN
8.2; a key absent from the body is already a malformed beat, `payload_invalid`, at once). The engine starts
beating only after LEADER_INIT step 7, when every health field is known, so a null is a real fault.

Episodes (rounds 9 and 10). Each condition key has **one** episode, shared by every engine source: a start time
(`since`, the first beat that showed it), the latest beat that showed it (`last_rx`, its `sent_at_ms`, and that beat
for the texts) and an `active` latch. Every beat that counts (below) and shows the condition extends its episode,
or starts one. There is no per-source condition state. A duration condition ("for > 60 s", `need`) activates once
it has held for `need`, counted on the receiver's monotonic clock:

* up to now while the newest accepted beat shows it;
* else up to its last showing beat, so an engine that no longer shows a condition, or that stopped, never starts a
  new page and a condition that held for less than `need` never pages (I3);
* while two engines run (`two_engines` active or held, below), up to the showing engine's next expected beat at
  most (its last showing beat plus the interval before it): the other engine's beat may be the newest, so the
  condition still pages on time, but it never counts past the beat that would have shown it again (round 10:
  before, it counted up to now for the whole 40 s tail, so a condition one engine showed for 30-45 s of a 60 s
  threshold paged after it had ended).

A condition only one engine shows therefore pages within one beat of its threshold (I4, I5). Conditions that page at
once have `need` 0. Once active, a condition stays active until its episode ends.

An episode ends when a later beat did not show the condition **and** none has shown it for `SOURCE_LIVE_MS` (40 s)
on the receive clock. So a condition resolves 40 s (plus one tick) after the last beat that showed it once a beat
without it has come, within one beat period + `SOURCE_LIVE_MS` of the last showing beat (I3); with no later beat at
all (silence, or the gap of an engine restart or failover) it carries on. A condition that clears for less than 40 s
and comes back is one episode. A showing beat handled when the receive-clock gap has run out still continues the
episode when its own `sent_at_ms` is within 40 s of the episode's last showing beat (round 10): beats queued behind a
stalled receiver are handled at one instant, and the gap is then the stall's, not the engine's (before, the other
engine's queued beat counted as "a later beat without it" and the showing engine's queued beat started a new
episode: a false resolved notice and a second page during a two-engine incident). Why one beat without it is not
enough: it may come from a second engine (the same source, or another one not yet detected), which the receiver
cannot tell from the same engine's next beat. Consequences, each a regression of the per-source design of review
rounds 5-8:

* a condition that holds through an engine restart (new epoch) or a failover, whatever the clocks, the order in
  which a stalled receiver handles the beats or a refused first beat of the new term, is one episode: one page, no
  false resolved notice (I1, I2, I6), and one that starts just before the change pages on time (I4);
* two engines beating together never switch a condition only one of them shows on and off, and the other engine's
  beats never reset its start time (I5): an engine never goes 40 s without a beat;
* a malformed POST is not a beat and never touches the episodes (I7).

Which beats count: every accepted beat, and a beat refused as "not newer" (an engine whose clock runs a little
behind, or two engines sharing one source). A refused beat more than `LATE_BEAT_MS` (15 s) behind the engine clock
is **late**: it may extend an episode that still runs, but it never starts one and is no evidence that a condition
ended. The engine clock (`_engine_now`, round 10) is carried on from the least delayed of the last `CLOCK_REFS`
accepted beats, not from the newest one alone, whose own delivery delay (up to the engine's 5 s POST timeout, more
behind a stalled receiver) would make it lag real time. A late beat is the last beat of a leader that stopped,
whose bytes sat in the socket behind `tailscale serve`: a condition only that engine showed ended 40 s after its
previous beat, and the late beat must not page it again (I10). It still counts for the two-engine rules. A skewed
beat counts for the two-engine rules only.

Round 10 also dropped the round-7 "fast pair" test (two beats of one source < 12 s apart marked it as shared by two
engines for a window and made durations count up to now): a single engine that restarts within seconds under the
same (node, epoch) makes such a pair, and an ended condition then paged. The rate rule (below) still detects two
engines on one source, and the one-episode design keeps them from flapping a condition before it does.

A condition last shown by another source than the newest accepted beat's says "(from <node> epoch <epoch>)" in its
text. `arm_failed_unrecorded` is not an episode: it comes from the newest accepted beat and the leader-change latch
only. The receiver pages on each condition's activation, re-pages while it stays active and sends a resolved notice
when it clears (`src/deadman_receiver/service.py`).

Two engines (W19, home-server-plan §4.12 / §6). Only the leader beats, so beats from two engines at once mean two
engines run (an old desktop left with its scheduled task, a restored spare, a split brain). A beat is refused as
"not newer" when its `sent_at_ms` is not after the newest accepted beat's, so an engine whose clock runs behind the
other's has all its beats refused, and one live engine would hide the other's death. Two engines whose clocks agree
both have their beats accepted, in turn. So every authenticated, parsed beat, including one refused as out of order
or skewed, is also recorded per **source** (node, epoch) by receiver time: at most `MAX_SOURCES` sources (the one
heard from least recently is dropped first) and the last `BEATS_PER_SOURCE` receive times of each.

Malformed beats (review round 5). A body that does not parse names no source, so it never reaches the two-engine
rules. Before, the next accepted beat from any engine cleared `payload_invalid` at once: a second, older engine
whose beats lack a field (an old desktop left with its scheduled task, on an older checkout), or a second engine
more than 5 min off, switched it on at each of its POSTs and off at the other engine's next beat (a critical page
plus a resolved notice about every 15 s). Now `payload_invalid` stays active until an accepted beat has come after
the last malformed or skewed POST **and** no such POST has come for `SOURCE_LIVE_MS` (40 s). With no accepted beat
since, it stays active as before. Once an accepted beat has come between two malformed POSTs of one episode, the
text says so: valid beats are also arriving, so look for a second sender. A single bad POST followed by good beats
resolves 40 s after it instead of at the next beat.

* `two_engines` (critical): within `two_engines_window_ms` (XTNL_RECEIVER_TWO_ENGINES_WINDOW_S, default 60 s), two
  sources each sent at least `TWO_ENGINE_MIN_BEATS` (2) beats inside the other's first-to-last span: their beats
  interleave, so both run. This test looks back over at least `OVERLAP_MIN_SPAN_MS` (90 s, or the window when
  longer) so that it still sees four beats per engine at the slower cadence of an engine whose PostgreSQL does not
  answer. A clean failover never trips it: the old leader stops within ~15 s, so at most one of
  its beats falls inside the new leader's span; a fail-back A -> B -> A puts no A beat inside B's span either.
  The interleave must hold on both clocks (review round 4): on the receive time and on the beats' own
  `sent_at_ms`. A stalled receiver that handles the old term's last beats and the new term's first beats in the
  same millisecond sees them "inside" each other on receive time, but their sent times keep the clean handover's
  order; two real engines interleave on both (an engine leads only after its clock hard check, so the two
  engines' clocks agree to well under the 15 s cadence).
  The epoch is part of the source, but it is only the engine's lease revision (`Epoch.label`, not the etcd
  cluster_id), and two home PCs started from the same env file share one node name (XTNL_NODE_ID=pc1). Two boxes
  each on a fresh etcd volume from the same compose file get the same cluster_id and start from the same low
  revisions, so they can beat as one source. That is not rare (a restore drill on the spare while the main box
  runs), so `two_engines` also fires on the **rate** of one source: at least `RATE_MIN_BEATS` (6) beats inside the
  window whose mean receive interval is under `RATE_MAX_MEAN_INTERVAL_MS` (12 s), **and** whose own `sent_at_ms`
  values (sorted) are also under 12 s apart on average. One engine beats every 15 s or slower (the next beat waits
  the full interval after the POST), so even with a 5 s delivery delay on one beat its mean over 5 intervals stays
  above 14 s; two engines under one source average about 7.5 s on both clocks. start-xtnl.ps1 also warns while
  XTNL_NODE_ID is the example's `pc1`, so each box can be given its own name.

  **Hold** (review round 4): once detected, `two_engines` stays active until a full window has passed without a
  detection (`_two_engines_hold`). The engine's real cadence is a little over 15 s (the next beat waits the full
  interval after the POST, which itself can take seconds), so a window holds only 2 or 3 beats per source and the
  interleave test drops out between beats and comes back at the next one. Without the hold each drop and return
  was a resolved notice plus a new critical page: a page storm during the very incident. With it the alarm pages
  once and resolves about one window after the last instant both engines were seen together.

  Receive times for these rules are **monotonic** (`rx_ms`, round-2 review): the receiver's wall clock may step
  back (an NTP correction), which would put one engine's later beats inside the window twice and make it look
  twice as fast. And a stalled receiver may handle several queued beats in the same millisecond (the engine gave
  up after its 5 s timeout but the bytes sat in the socket behind `tailscale serve`): their receive gaps are then
  near zero, but their `sent_at_ms` keep the engine's 15 s cadence, so the second (sender clock) test stops a
  single engine from tripping the rate rule. Two engines interleave on both clocks. The texts show wall times.
* `node_changed` (high, a notice): the node of the latest beat differs from the one before. Expected after an AWS
  failover, unexpected on the single home PC. Active for `node_change_hold_ms` (5 min) after the last change. An
  epoch change on the same node (an engine restart) is not a node change.

Both carry node names, epochs, counts and receiver times only.
"""
from __future__ import annotations

import json
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import combinations
from typing import Any, Final, Literal, NamedTuple

STREAMING: Final = "STREAMING"
READY: Final = "READY"
UNKNOWN: Final = "UNKNOWN"

#: Every key of src.runtime.deadman.DeadmanPayload (a test keeps the two equal).
PAYLOAD_FIELDS: Final = (
    "node", "epoch", "sent_at_ms", "gate_state", "halt_level", "adapters", "last_committed_broker_event_age_ms",
    "oldest_unacked_feed_age_ms", "oldest_risksweep_pass_age_ms", "calendar_version",
    "oldest_undelivered_notification_age_ms", "sm8_state", "sm5_state", "spill_bytes", "degraded",
)
#: Keys of DeadmanPayload that may be absent (None): the ARM's fields, sent only while the ARM runs.
OPTIONAL_FIELDS: Final = ("arm_oldest_undelivered_notification_age_ms", "arm_overdue_positions", "arm_failed")
ADAPTER_FIELDS: Final = ("sm4_state", "market_open", "maintenance_until_ms")
ARM_FAILED_FIELDS: Final = ("count", "global_failed")
ARM_FAILED_OPTIONAL: Final = ("unrecorded",)
"""`arm_failed` keys an older engine leaves out (S38c review): absent means 0."""
_STR_OR_NULL: Final = ("epoch", "gate_state", "halt_level", "sm8_state", "sm5_state")
_INT_OR_NULL: Final = ("last_committed_broker_event_age_ms", "oldest_unacked_feed_age_ms",
                       "oldest_risksweep_pass_age_ms", "calendar_version", "oldest_undelivered_notification_age_ms",
                       "spill_bytes")
#: Health fields that must not be null: a null here means the engine cannot report it (paged at once, 8.2).
#: The age fields may legitimately be null (nothing un-acked / undelivered / no account yet) and are not listed.
NON_NULL_FIELDS: Final = ("epoch", "gate_state", "halt_level", "calendar_version", "sm8_state", "sm5_state",
                          "spill_bytes")
MAX_BODY_BYTES: Final = 65_536
MAX_CLOCK_SKEW_MS: Final = 300_000
#: W19: engine sources (node, epoch) tracked at once. Two is the alarm; the bound only stops a flood of invented
#: node names (an authenticated but broken sender) from growing the receiver's memory.
MAX_SOURCES: Final = 8
#: W19: receive times kept per source: 48 beats cover the widest window (600 s) at the engine's 15 s cadence.
#: A sender beating faster keeps a shorter span; a shorter span can only clear the alarm sooner, never invent one.
BEATS_PER_SOURCE: Final = 48
#: W19: beats each source must send inside the other's span for `two_engines` (one stray beat of a stopping
#: leader after the new leader's first beat is a normal failover).
TWO_ENGINE_MIN_BEATS: Final = 2
#: W19 rate rule: one source counts as two engines when at least this many of its beats inside the window arrive
#: with a mean interval under RATE_MAX_MEAN_INTERVAL_MS, on the receiver's monotonic clock and on the beats' own
#: `sent_at_ms`. Six beats of two 15 s engines span ~37.5 s, inside the smallest window (45 s); one 15 s engine with
#: one beat delayed by the 5 s POST timeout still averages >= 14 s, and queued beats handled at once keep their
#: 15 s `sent_at_ms` gaps.
RATE_MIN_BEATS: Final = 6
RATE_MAX_MEAN_INTERVAL_MS: Final = 12_000
#: W19 interleave rule (review round 4): it looks back over at least this long, or the window when that is longer.
#: The engine's real cadence is 15 s plus the POST and its PostgreSQL reads (about 22 s while PostgreSQL does not
#: answer); a 45 s window then holds two beats per engine, too few to see two of each inside the other's span.
#: 90 s holds four. A clean failover never interleaves, so a longer look-back cannot invent the alarm.
OVERLAP_MIN_SPAN_MS: Final = 90_000
#: Round 9: how long a condition stays active after the last beat that showed it, once a later beat did not show
#: it (Episodes, module docstring). The engine beats every 15 s plus the POST and its PostgreSQL reads (about 22 s
#: while PostgreSQL does not answer, up to 5 s more for a POST that times out), so a running engine that still shows
#: a condition always shows it again within this time.
SOURCE_LIVE_MS: Final = 40_000
#: Round 9 / 10: a beat refused as "not newer" whose `sent_at_ms` is more than this behind the engine clock
#: (`_engine_now`: the best estimate from the latest accepted beats, so one delayed newest beat does not hold it back)
#: is a late beat (the last beat of a leader that stopped, whose bytes sat behind `tailscale serve`, or a beat queued
#: behind a stalled receiver). It may extend a condition episode that still runs but never starts one, and it is no
#: evidence that a condition ended. A condition only a stopped leader showed ends SOURCE_LIVE_MS after its previous
#: beat, which was at most one beat period (22 s) before the late one, so the late beat is then at least 18 s old;
#: a second engine whose clock runs behind is within a few seconds (an engine leads only after its clock hard check).
LATE_BEAT_MS: Final = 15_000
#: Round 10: accepted beats `_engine_now` estimates the engine clock from (the least delayed one wins).
CLOCK_REFS: Final = 4


@dataclass(frozen=True)
class Thresholds:
    """operations.md §9, in milliseconds."""

    silence_ms: int = 60_000
    not_streaming_ms: int = 120_000
    fields_missing_ms: int = 0                 # "when those fields are missing": no grace (8.2)
    feed_age_ms: int = 30_000
    pass_age_ms: int = 10_000
    notification_age_ms: int = 60_000
    sm8_sm5_ms: int = 60_000
    spill_ms: int = 60_000
    arm_notification_age_ms: int = 60_000     # same bound as the Oversee alerts' notification_age_ms
    arm_unrecorded_latch_ms: int = 3_600_000  # an unrecorded FAILED episode left behind by a leader change
    two_engines_window_ms: int = 60_000       # W19: beats looked at for `two_engines` (receiver time)
    node_change_hold_ms: int = 300_000        # W19: how long `node_changed` stays active after a change


@dataclass(frozen=True)
class AdapterState:
    sm4_state: str | None
    market_open: bool | None
    maintenance_until_ms: int | None


@dataclass(frozen=True)
class ArmFailedState:
    count: int
    global_failed: bool
    unrecorded: int = 0
    """FAILED episodes PostgreSQL has not recorded (held in that node's memory and spool only); an older engine
    leaves the key out (0)."""


@dataclass(frozen=True)
class Beat:
    node: str
    epoch: str | None
    sent_at_ms: int
    gate_state: str | None
    halt_level: str | None
    adapters: Mapping[str, AdapterState]
    last_committed_broker_event_age_ms: int | None
    oldest_unacked_feed_age_ms: int | None
    oldest_risksweep_pass_age_ms: int | None
    calendar_version: int | None
    oldest_undelivered_notification_age_ms: int | None
    sm8_state: str | None
    sm5_state: str | None
    spill_bytes: int | None
    degraded: tuple[str, ...]
    arm_oldest_undelivered_notification_age_ms: int | None = None
    arm_overdue_positions: int | None = None
    arm_failed: ArmFailedState | None = None


class BeatError(ValueError):
    """A malformed heartbeat. The message names fields only (never values): safe to log and to return."""


def _is_int(v: object) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _text(v: object, name: str, *, limit: int = 200) -> str:
    if not isinstance(v, str) or not v or len(v) > limit:
        raise BeatError(f"{name} must be a non-empty string")
    return v


def _opt_text(d: Mapping[str, Any], name: str) -> str | None:
    v = d[name]
    return None if v is None else _text(v, name)


def _opt_int(d: Mapping[str, Any], name: str) -> int | None:
    v = d[name]
    if v is None:
        return None
    if not _is_int(v):
        raise BeatError(f"{name} must be an integer or null")
    return int(v)


def _adapter(name: str, v: object) -> AdapterState:
    if not isinstance(v, dict):
        raise BeatError(f"adapters.{name} must be an object")
    missing = [f for f in ADAPTER_FIELDS if f not in v]
    if missing:
        raise BeatError(f"adapters.{name} lacks {', '.join(missing)}")
    sm4 = v["sm4_state"]
    if sm4 is not None and not isinstance(sm4, str):
        raise BeatError(f"adapters.{name}.sm4_state must be a string or null")
    mo = v["market_open"]
    if mo is not None and not isinstance(mo, bool):
        raise BeatError(f"adapters.{name}.market_open must be a boolean or null")
    mu = v["maintenance_until_ms"]
    if mu is not None and not _is_int(mu):
        raise BeatError(f"adapters.{name}.maintenance_until_ms must be an integer or null")
    return AdapterState(sm4, mo, mu)


def _opt_count(d: Mapping[str, Any], name: str) -> int | None:
    """An optional non-negative integer: absent or null -> None."""
    v = d.get(name)
    if v is None:
        return None
    if not _is_int(v) or v < 0:
        raise BeatError(f"{name} must be a non-negative integer or null")
    return int(v)


def _arm_failed(v: object) -> ArmFailedState | None:
    if v is None:
        return None
    if not isinstance(v, dict) or any(f not in v for f in ARM_FAILED_FIELDS):
        raise BeatError(f"arm_failed must be an object with {', '.join(ARM_FAILED_FIELDS)} or null")
    count, glob, unrecorded = v["count"], v["global_failed"], v.get(ARM_FAILED_OPTIONAL[0], 0)
    if not _is_int(count) or count < 0 or not isinstance(glob, bool):
        raise BeatError("arm_failed.count must be a non-negative integer and arm_failed.global_failed a boolean")
    if not _is_int(unrecorded) or unrecorded < 0:
        raise BeatError("arm_failed.unrecorded must be a non-negative integer")
    return ArmFailedState(int(count), glob, int(unrecorded))


def parse_beat(body: bytes) -> Beat:
    """Decode and validate one heartbeat body (UTF-8 JSON object). Raises BeatError."""
    if len(body) > MAX_BODY_BYTES:
        raise BeatError("body too large")
    try:
        d = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise BeatError("body is not UTF-8 JSON") from None
    if not isinstance(d, dict):
        raise BeatError("body is not a JSON object")
    missing = [f for f in PAYLOAD_FIELDS if f not in d]
    if missing:
        raise BeatError(f"missing fields: {', '.join(missing)}")
    if not _is_int(d["sent_at_ms"]):
        raise BeatError("sent_at_ms must be an integer")
    adapters = d["adapters"]
    if not isinstance(adapters, dict):
        raise BeatError("adapters must be an object")
    degraded = d["degraded"]
    if not isinstance(degraded, list) or not all(isinstance(t, str) and t for t in degraded):
        raise BeatError("degraded must be a list of task names")
    texts = {n: _opt_text(d, n) for n in _STR_OR_NULL}
    ints = {n: _opt_int(d, n) for n in _INT_OR_NULL}
    return Beat(
        node=_text(d["node"], "node"), epoch=texts["epoch"], sent_at_ms=int(d["sent_at_ms"]),
        gate_state=texts["gate_state"], halt_level=texts["halt_level"],
        adapters={_text(k, "adapter id"): _adapter(k, v) for k, v in adapters.items()},
        last_committed_broker_event_age_ms=ints["last_committed_broker_event_age_ms"],
        oldest_unacked_feed_age_ms=ints["oldest_unacked_feed_age_ms"],
        oldest_risksweep_pass_age_ms=ints["oldest_risksweep_pass_age_ms"],
        calendar_version=ints["calendar_version"],
        oldest_undelivered_notification_age_ms=ints["oldest_undelivered_notification_age_ms"],
        sm8_state=texts["sm8_state"], sm5_state=texts["sm5_state"], spill_bytes=ints["spill_bytes"],
        degraded=tuple(str(t)[:100] for t in degraded),
        arm_oldest_undelivered_notification_age_ms=_opt_count(d, "arm_oldest_undelivered_notification_age_ms"),
        arm_overdue_positions=_opt_count(d, "arm_overdue_positions"),
        arm_failed=_arm_failed(d.get("arm_failed")),
    )


ConditionSeverity = Literal["high", "critical"]


@dataclass(frozen=True)
class Condition:
    """One active paging condition. `summary` is the page title, `detail` its body line (no secrets). `severity`
    is the severity of its activation page and re-pages: critical, except the W19 notice (high)."""

    key: str
    summary: str
    detail: str
    severity: ConditionSeverity = "critical"


Source = tuple[str, str | None]
"""(node, epoch) of an engine that beats (W19)."""


def _hms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).strftime("%H:%M:%S UTC")


def _label(src: Source) -> str:
    return f"{src[0]} epoch {src[1]}"


def _order(src: Source) -> tuple[str, str]:
    return src[0], src[1] or ""


def _inside(ta: list[Rx], tb: list[Rx], clock: str) -> int:
    """How many beats of `ta` fall inside the first-to-last span of `tb` on one clock (`rx_ms` or `sent_ms`).
    Min / max rather than first / last: within one source sent times need not be sorted (two engines sharing it)."""
    vals = [getattr(r, clock) for r in tb]
    lo, hi = min(vals), max(vals)
    return sum(1 for r in ta if lo <= getattr(r, clock) <= hi)


class Rx(NamedTuple):
    """W19: one parsed beat of a source. `rx_ms` (monotonic) orders and windows it, `wall_ms` is shown in texts,
    `sent_ms` is the engine's own clock (the rate rule's second test)."""

    rx_ms: int
    wall_ms: int
    sent_ms: int


def _mean_under(values: list[int], n: int) -> bool:
    """True when `n` values (n >= 2) span less than RATE_MAX_MEAN_INTERVAL_MS per interval."""
    return (max(values) - min(values)) < RATE_MAX_MEAN_INTERVAL_MS * (n - 1)


@dataclass(frozen=True)
class NodeChange:
    """W19: the beating node changed (wall times for the text; `at_rx_ms` monotonic for the hold)."""

    old: str
    old_last_ms: int
    new: str
    at_ms: int
    at_rx_ms: int


@dataclass
class Episode:
    """Round 9: one condition key, shared by every engine source (one start time per key). Times are receiver
    monotonic: `since` of the first beat that showed it in this episode, `last_rx` of the latest; `beat` is that
    latest beat (texts); `need` the duration the condition must hold before it pages (0: at once); `active` latches
    once it has held for `need`."""

    since: int
    last_rx: int
    need: int
    beat: Beat
    last_sent: int = 0
    """Round 10: the latest `sent_at_ms` of a beat that showed it (engine clock; the continuity test of `_record`)."""
    prev_rx: int | None = None
    """Round 10: monotonic receive time of the showing beat before `last_rx` (the expected next showing, Two
    engines in the module docstring)."""
    active: bool = False


@dataclass
class Evaluator:
    """The receiver's state between beats. Not thread-safe: the receiver calls it from its event loop only."""

    started_ms: int
    thresholds: Thresholds = field(default_factory=Thresholds)
    started_rx: int | None = None
    """Review round 7: the receiver's monotonic time at start (default `started_ms`, for callers that drive both
    clocks from one value). Silence before the first beat counts from it."""
    last_rx_ms: int | None = None
    """Receiver wall time of the latest accepted beat (texts only)."""
    _last_rx_mono: int | None = None
    """Review round 7: receiver monotonic time of the latest accepted beat (the silence timer)."""
    last_beat: Beat | None = None
    invalid: str | None = None
    beats: int = 0
    rejected: int = 0
    _invalid_rx: int = 0
    """Review round 5: monotonic receive time of the last malformed or skewed POST (meaningful while `invalid`)."""
    _good_after_invalid: bool = False
    """Review round 5: an accepted beat came after the last malformed or skewed POST."""
    _invalid_interleaved: bool = False
    """Review round 5: in this `payload_invalid` episode an accepted beat came between two malformed POSTs."""
    _episodes: dict[str, Episode] = field(default_factory=dict)
    """Round 9: the condition episodes, one per key (Episodes, module docstring)."""
    _evidence_rx: int | None = None
    """Round 9: monotonic receive time of the latest beat that counts as fresh evidence for the conditions."""
    _clock_refs: deque[tuple[int, int]] = field(default_factory=lambda: deque(maxlen=CLOCK_REFS))
    """Round 10: (monotonic receive time, `sent_at_ms`) of the latest accepted beats (`_engine_now`)."""
    _unrecorded_at: tuple[str, str | None] | None = None
    """(node, epoch) of the last beat that reported unrecorded FAILED episodes (cleared once it reports none)."""
    _lost: tuple[str, str | None, int] | None = None
    """(node, epoch, receiver monotonic time) of unrecorded episodes left behind by a leader change (latched)."""
    _sources: dict[Source, deque[Rx]] = field(default_factory=dict)
    """W19: the last beats per source, from every parsed beat (refused ones included)."""
    _node: tuple[str, int] | None = None
    """W19: (node, receiver wall time) of the latest parsed beat."""
    _node_change: NodeChange | None = None
    _two_engines_hold: tuple[int, Condition] | None = None
    """W19 (review round 4): (monotonic receive time until which `two_engines` stays active, the condition of the
    latest detection). Re-armed on every detection; see the module docstring, Hold."""

    # ------------------------------------------------------------------ input
    def accept(self, beat: Beat, now_ms: int, *, rx_ms: int | None = None) -> None:
        """Record a valid beat received at `now_ms` (wall; `rx_ms` monotonic) (raises BeatError for a stale /
        skewed `sent_at_ms`). The beat counts for the two-engine rules (W19) even when it is refused, and for the
        conditions when it is refused only as not newer, unless it is a late beat (LATE_BEAT_MS)."""
        self.observe(beat, now_ms, rx_ms=rx_ms)
        rx = now_ms if rx_ms is None else rx_ms
        off = self._skew_ms(beat.sent_at_ms, now_ms, rx)
        if off is not None:
            self.reject(f"sent_at_ms is {off // 1000} s off the receiver clock "
                        f"(from {_label((beat.node, beat.epoch))})", now_ms, rx_ms=rx)
            raise BeatError("sent_at_ms is too far from the receiver clock")
        if self.last_beat is not None and beat.sent_at_ms <= self.last_beat.sent_at_ms:
            self.rejected += 1
            # another engine (or one sharing the source) beating at the same time; a late beat only extends
            self._record(beat, rx, late=self._engine_now(rx) - beat.sent_at_ms > LATE_BEAT_MS)
            raise BeatError("sent_at_ms is not newer than the last accepted beat")
        self.last_beat = beat
        self.last_rx_ms = now_ms
        self._last_rx_mono = rx
        self._clock_refs.append((rx, beat.sent_at_ms))
        self._good_after_invalid = True          # payload_invalid clears SOURCE_LIVE_MS after the last bad POST
        self.beats += 1
        self._track_unrecorded(beat, rx)
        self._record(beat, rx)

    def _carried(self, rx_now: int) -> int:
        """Review round 8: the engine clock carried on from the newest accepted beat (its `sent_at_ms` plus the
        receiver monotonic time since). Only called once a beat was accepted."""
        assert self.last_beat is not None and self._last_rx_mono is not None
        return self.last_beat.sent_at_ms + (rx_now - self._last_rx_mono)

    def _engine_now(self, rx_now: int) -> int:
        """Round 10: the engine clock now, carried on from the least delayed of the latest accepted beats received
        within `silence_ms` (the newest one alone lags real time by its own delivery delay, up to the engine's 5 s
        POST timeout or more behind a stalled receiver). Only called once a beat was accepted."""
        refs = [s + (rx_now - r) for r, s in self._clock_refs if rx_now - r <= self.thresholds.silence_ms]
        return max([self._carried(rx_now), *refs])

    def _skew_ms(self, sent_at_ms: int, now_ms: int, rx_now: int) -> int | None:
        """How far `sent_at_ms` is off when the beat is refused as skewed, else None. Round 9: while the engine is
        live (an accepted beat within the silence threshold) the engine clock carried on from it is the only
        reference, whatever the receiver's wall clock says, so a wall step can neither let a skewed sender in nor
        lock the engine out; after a silence a beat within MAX_CLOCK_SKEW_MS of either clock passes (review round
        8). Before the first accepted beat (round 10) a beat within MAX_CLOCK_SKEW_MS of either the wall clock or the
        receiver's start wall time carried on by its monotonic clock passes: a wall step between the receiver's start
        and the engine's first beat then refuses nothing, and a wall clock corrected after a wrong start still
        works."""
        wall_off = abs(sent_at_ms - now_ms)
        if self.last_beat is None or self._last_rx_mono is None:
            start_rx = self.started_ms if self.started_rx is None else self.started_rx
            start_off = abs(sent_at_ms - (self.started_ms + rx_now - start_rx))
            off = min(wall_off, start_off)
            return off if off > MAX_CLOCK_SKEW_MS else None
        carried_off = abs(sent_at_ms - self._carried(rx_now))
        if carried_off <= MAX_CLOCK_SKEW_MS:
            return None
        if rx_now - self._last_rx_mono <= self.thresholds.silence_ms or wall_off > MAX_CLOCK_SKEW_MS:
            return carried_off
        return None

    def _paired(self, rx_now: int) -> bool:
        """Review round 5: `two_engines` is active or held (its last detection was at most a window ago)."""
        return self._two_engines_hold is not None and rx_now <= self._two_engines_hold[0]

    def _ended(self, ep: Episode, rx_now: int) -> bool:
        """Round 9: a later beat did not show the condition and none has shown it for SOURCE_LIVE_MS. With no later
        beat at all (silence, or the gap of a restart) it carries on."""
        return (self._evidence_rx is not None and self._evidence_rx > ep.last_rx
                and rx_now - ep.last_rx > SOURCE_LIVE_MS)

    def _record(self, beat: Beat, rx_now: int, *, late: bool = False) -> None:
        """Round 9 / 10: the beat counts for the conditions: extend (or start) the episode of every condition it
        shows. A beat handled after the episode's receive-clock gap ran out still extends it when its own
        `sent_at_ms` is within SOURCE_LIVE_MS of the episode's last showing beat (beats queued behind a stalled
        receiver: the gap is the stall's, not the engine's). A `late` beat (LATE_BEAT_MS) only extends a running
        episode and is no evidence."""
        for key, need in self._shown(beat).items():
            ep = self._episodes.get(key)
            if ep is not None and self._ended(ep, rx_now) and beat.sent_at_ms - ep.last_sent > SOURCE_LIVE_MS:
                del self._episodes[key]
                ep = None
            if ep is None:
                if late:
                    continue
                ep = self._episodes[key] = Episode(rx_now, rx_now, need, beat, beat.sent_at_ms)
            elif rx_now >= ep.last_rx:
                if rx_now > ep.last_rx:
                    ep.prev_rx = ep.last_rx
                ep.last_rx, ep.beat = rx_now, beat
                ep.last_sent = max(ep.last_sent, beat.sent_at_ms)
            if rx_now - ep.since >= ep.need:
                ep.active = True
        if not late and (self._evidence_rx is None or rx_now > self._evidence_rx):
            self._evidence_rx = rx_now

    def observe(self, beat: Beat, now_ms: int, *, rx_ms: int | None = None) -> None:
        """W19: record which engine sent a parsed, authenticated beat, whether or not it is then accepted.
        `rx_ms` is the receiver's monotonic clock (default `now_ms`): it only moves forward, so a wall clock step
        cannot put a beat inside the window twice."""
        rx = now_ms if rx_ms is None else rx_ms
        src: Source = (beat.node, beat.epoch)
        times = self._sources.get(src)
        if times is None:
            if len(self._sources) >= MAX_SOURCES:
                stalest = min(self._sources, key=lambda s: self._sources[s][-1].rx_ms)
                del self._sources[stalest]
            times = self._sources[src] = deque(maxlen=BEATS_PER_SOURCE)
        times.append(Rx(rx, now_ms, beat.sent_at_ms))
        if self._node is not None and self._node[0] != beat.node:
            self._node_change = NodeChange(self._node[0], self._node[1], beat.node, now_ms, rx)
        self._node = (beat.node, now_ms)

    @property
    def tracked_sources(self) -> int:
        """How many engine sources the receiver currently remembers (bounded by MAX_SOURCES)."""
        return len(self._sources)

    def _overlap_span_ms(self) -> int:
        return max(self.thresholds.two_engines_window_ms, OVERLAP_MIN_SPAN_MS)

    def _recent(self, rx_now: int, span_ms: int | None = None) -> dict[Source, list[Rx]]:
        """Beats per source inside the last `span_ms` (default the two-engine window; monotonic receive time). A
        source with no beat inside the longest span used (`_overlap_span_ms`) is forgotten; one with none inside a
        shorter span maps to an empty list."""
        forget = rx_now - self._overlap_span_ms()
        for src in [s for s, times in self._sources.items() if times[-1].rx_ms < forget]:
            del self._sources[src]
        since = rx_now - (self.thresholds.two_engines_window_ms if span_ms is None else span_ms)
        return {s: [r for r in times if r.rx_ms >= since] for s, times in self._sources.items()}

    def _overlapping(self, rx_now: int) -> dict[Source, list[Rx]]:
        """The sources whose beats interleave with another source's inside the window (W19 `two_engines`), on the
        receiver's monotonic clock **and** on the beats' own `sent_at_ms` (review round 4: beats handled in the same
        millisecond by a stalled receiver are inside each other's span on receive time only)."""
        recent = self._recent(rx_now, self._overlap_span_ms())
        out: dict[Source, list[Rx]] = {}
        for a, b in combinations(sorted((s for s in recent if recent[s]), key=_order), 2):
            ta, tb = recent[a], recent[b]
            if all(_inside(x, y, clock) >= TWO_ENGINE_MIN_BEATS
                   for clock in ("rx_ms", "sent_ms") for x, y in ((ta, tb), (tb, ta))):
                out[a], out[b] = ta, tb
        return out

    def _too_fast(self, rx_now: int) -> dict[Source, list[Rx]]:
        """W19 rate rule: sources beating so fast that two engines must share the (node, epoch). Fast on the
        receiver's monotonic clock **and** on the beats' own `sent_at_ms`: a stalled receiver that handles one
        engine's queued beats at once sees them bunched, but their sent times keep the engine's 15 s cadence."""
        out: dict[Source, list[Rx]] = {}
        for src, beats in self._recent(rx_now).items():
            n = len(beats)
            if n >= RATE_MIN_BEATS and _mean_under([r.rx_ms for r in beats], n) \
                    and _mean_under([r.sent_ms for r in beats], n):
                out[src] = beats
        return out

    def _engine_conditions(self, now_ms: int, rx_now: int) -> dict[str, Condition]:
        """W19: `two_engines` (critical) and the `node_changed` notice (high)."""
        t = self.thresholds
        out: dict[str, Condition] = {}
        both = self._overlapping(rx_now)
        fast = {s: ts for s, ts in self._too_fast(rx_now).items() if s not in both}
        if both or fast:
            span_ms = self._overlap_span_ms() if both else t.two_engines_window_ms
            names = ", ".join(sorted({s[0] for s in both} | {s[0] for s in fast}))
            parts = [f"{_label(s)}: {len(both[s])} beats, last {_hms(both[s][-1].wall_ms)}"
                     for s in sorted(both, key=_order)]
            for s in sorted(fast, key=_order):
                ts = fast[s]
                mean_s = (ts[-1].rx_ms - ts[0].rx_ms) // (len(ts) - 1) // 1000
                parts.append(f"{_label(s)}: {len(ts)} beats, mean interval {mean_s} s (one engine beats every "
                             f"15 s), last {_hms(ts[-1].wall_ms)}")
            out["two_engines"] = Condition(
                "two_engines", f"dead-man: two engines beating ({names})",
                f"{'; '.join(parts)} (within {span_ms // 1000} s): only one engine may run; stop the one that should "
                "not (deadman-alert.md, two engines)")
            self._two_engines_hold = (rx_now + t.two_engines_window_ms, out["two_engines"])
        elif self._two_engines_hold is not None:
            until, held = self._two_engines_hold
            if rx_now <= until:
                out["two_engines"] = held      # between two beats the interleave can drop out: keep it (Hold)
            else:
                self._two_engines_hold = None
        ch = self._node_change
        if ch is not None and rx_now - ch.at_rx_ms > t.node_change_hold_ms:
            self._node_change = ch = None
        if ch is not None:
            out["node_changed"] = Condition(
                "node_changed", f"dead-man: beating node changed ({ch.old} -> {ch.new})",
                f"beats now from {ch.new} (since {_hms(ch.at_ms)}), before from {ch.old} "
                f"(last {_hms(ch.old_last_ms)}); expected after an AWS failover, unexpected on the single home PC "
                "(deadman-alert.md, node changed)", "high")
        return out

    def _sources_text(self, rx_now: int) -> str:
        recent = {s: ts for s, ts in self._recent(rx_now).items() if ts}
        if not recent:
            return ""
        listed = ", ".join(f"{_label(s)} x{len(recent[s])}" for s in sorted(recent, key=_order))
        return f"; beats in the last {self.thresholds.two_engines_window_ms // 1000} s: {listed}"

    def _track_unrecorded(self, beat: Beat, rx_now: int) -> None:
        """S38c review (AWS leader change): unrecorded FAILED episodes live only in the reporting node's memory and
        spool. A beat from another node or epoch while they were outstanding means the leader changed before they
        were recorded: latch `arm_failed_unrecorded` (the new leader starts with no episode and fresh counters)."""
        failed = beat.arm_failed
        here = (beat.node, beat.epoch)
        if self._unrecorded_at is not None and self._unrecorded_at != here:
            self._lost = (self._unrecorded_at[0], self._unrecorded_at[1], rx_now)
        self._unrecorded_at = here if failed is not None and failed.unrecorded > 0 else None
        if self._lost is not None and failed is not None and failed.count > 0 and self._unrecorded_at is None:
            self._lost = None           # a FAILED scope is open (and recorded) again: the `arm_failed` page covers it

    def reject(self, reason: str, now_ms: int, *, rx_ms: int | None = None) -> None:
        """A malformed or skewed POST received at `now_ms` (wall; `rx_ms` monotonic, default `now_ms`). The reason
        names fields only. See the module docstring, Malformed beats."""
        rx = now_ms if rx_ms is None else rx_ms
        if self.invalid is not None and self._good_after_invalid:
            self._invalid_interleaved = True     # a valid beat came between two bad POSTs: two senders
        self.invalid = reason
        self._invalid_rx = rx
        self._good_after_invalid = False
        self.rejected += 1

    def _invalid_condition(self, rx_now: int) -> Condition | None:
        """Review round 5: `payload_invalid`, held until an accepted beat came after the last malformed or skewed
        POST and none has come for SOURCE_LIVE_MS."""
        if self.invalid is None:
            return None
        if self._good_after_invalid and rx_now - self._invalid_rx > SOURCE_LIVE_MS:
            self.invalid = None
            self._invalid_interleaved = False
            return None
        detail = self.invalid
        b = self.last_beat
        if self._invalid_interleaved and b is not None:
            detail += (f"; valid beats are also arriving (latest from {_label((b.node, b.epoch))}): possibly a "
                       "second, older engine; look for a second sender (deadman-alert.md, malformed heartbeat)")
        return Condition("payload_invalid", "dead-man: malformed heartbeat", detail)

    # ------------------------------------------------------------------ rules
    def _shown(self, b: Beat) -> dict[str, int]:
        """Every condition of the table one beat shows, with the duration it must hold before it pages (0: at
        once). `arm_failed_unrecorded` is not among them: the newest beat and the leader-change latch rule it."""
        t = self.thresholds
        out: dict[str, int] = {}
        for a, s in sorted(b.adapters.items()):
            in_maintenance = s.maintenance_until_ms is not None and s.maintenance_until_ms > b.sent_at_ms
            if s.market_open is True and not in_maintenance and s.sm4_state != STREAMING:
                out[f"adapter_not_streaming:{a}"] = t.not_streaming_ms
            if s.sm4_state is None or s.market_open is None:
                out[f"adapter_fields_missing:{a}"] = t.fields_missing_ms
        for name in NON_NULL_FIELDS:
            if getattr(b, name) is None:
                out[f"fields_missing:{name}"] = t.fields_missing_ms
        if not b.adapters:
            out["fields_missing:adapters"] = t.fields_missing_ms
        if b.sm8_state != READY:
            out["sm8_not_ready"] = t.sm8_sm5_ms
        if b.sm5_state is None or b.sm5_state == UNKNOWN:
            out["sm5_unknown"] = t.sm8_sm5_ms
        if b.spill_bytes is not None and b.spill_bytes > 0:
            out["spill_nonempty"] = t.spill_ms
        if b.oldest_unacked_feed_age_ms is not None and b.oldest_unacked_feed_age_ms > t.feed_age_ms:
            out["feed_lag"] = 0
        if b.oldest_risksweep_pass_age_ms is not None and b.oldest_risksweep_pass_age_ms > t.pass_age_ms:
            out["risksweep_pass_age"] = 0
        undelivered = b.oldest_undelivered_notification_age_ms
        if undelivered is not None and undelivered > t.notification_age_ms:
            out["notification_undelivered"] = 0
        for task in set(b.degraded):
            out[f"circuit_open:{task}"] = 0
        if b.arm_failed is not None and b.arm_failed.count > 0:
            out["arm_failed"] = 0
        if b.arm_overdue_positions is not None and b.arm_overdue_positions > 0:
            out["arm_overdue_positions"] = 0
        age = b.arm_oldest_undelivered_notification_age_ms
        if age is not None and age > t.arm_notification_age_ms:
            out["arm_notification_undelivered"] = 0
        return out

    def active(self, now_ms: int, *, rx_ms: int | None = None) -> dict[str, Condition]:
        """The conditions active at `now_ms` (wall; `rx_ms` the monotonic clock, default `now_ms`)."""
        t = self.thresholds
        out: dict[str, Condition] = {}
        rx = now_ms if rx_ms is None else rx_ms
        start_rx = self.started_ms if self.started_rx is None else self.started_rx
        ref = self._last_rx_mono if self._last_rx_mono is not None else start_rx
        silent_ms = rx - ref                     # review round 7: monotonic, a wall step cannot fake silence
        if silent_ms > t.silence_ms:
            what = "no heartbeat since the receiver started" if self.last_rx_ms is None else "no heartbeat"
            out["silence"] = Condition("silence", f"dead-man: {what} for {silent_ms // 1000} s",
                                       "the engine leader POSTs every 15 s; check for a leader (deadman-alert.md)")
        invalid = self._invalid_condition(rx)
        if invalid is not None:
            out["payload_invalid"] = invalid
        out.update(self._engine_conditions(now_ms, rx))
        b = self.last_beat
        if b is None:
            return out
        if self._lost is not None and rx - self._lost[2] > t.arm_unrecorded_latch_ms:
            self._lost = None
        # Round 10: a duration holds up to now while the newest accepted beat shows it, else up to its last showing
        # beat; while two engines run, up to the showing engine's next expected beat at most (Episodes).
        paired = self._paired(rx)
        current = self._shown(b)
        cur: Source = (b.node, b.epoch)
        for key, ep in list(self._episodes.items()):
            if self._ended(ep, rx):
                del self._episodes[key]
                continue
            if key in current:
                upto = rx
            elif paired and ep.prev_rx is not None:
                upto = min(rx, 2 * ep.last_rx - ep.prev_rx)
            else:
                upto = ep.last_rx
            held = upto - ep.since
            if held >= ep.need:
                ep.active = True
            if not ep.active:
                continue
            cond = self._condition(key, ep.beat, held)
            src: Source = (ep.beat.node, ep.beat.epoch)
            if src != cur:
                cond = Condition(key, cond.summary, f"{cond.detail} (from {_label(src)})", cond.severity)
            out[key] = cond
        unrecorded = self._unrecorded_condition(b)
        if unrecorded is not None:
            out[unrecorded.key] = unrecorded
        return out

    def _condition(self, key: str, b: Beat, held_ms: int) -> Condition:
        """The page of condition `key` as beat `b` (its latest showing) shows it; `held_ms` for the timed ones."""
        kind, _, subject = key.partition(":")
        if kind == "feed_lag":
            return Condition(key, "dead-man: oversee feed entry un-acked > 30 s",
                             f"oldest un-acked feed entry {(b.oldest_unacked_feed_age_ms or 0) // 1000} s")
        if kind == "risksweep_pass_age":
            return Condition(key, "dead-man: RiskSweep pass older than 10 s",
                             f"oldest per-account pass {(b.oldest_risksweep_pass_age_ms or 0) / 1000:.1f} s old")
        if kind == "notification_undelivered":
            return Condition(key, "dead-man: alert undelivered > 60 s",
                             f"oldest undelivered notification {(b.oldest_undelivered_notification_age_ms or 0) // 1000} s")
        if kind == "circuit_open":
            return Condition(key, f"dead-man: task circuit open ({subject})", f"degraded: {subject}")
        if kind == "arm_failed":
            failed = b.arm_failed or ArmFailedState(0, False)
            scope = "global ARM scope included" if failed.global_failed else "no global scope"
            return Condition(key, f"dead-man: ARM FAILED ({failed.count} scopes)",
                             f"{failed.count} ARM scopes in FAILED, {scope} (runbook arm.md FAILED)")
        if kind == "arm_overdue_positions":
            return Condition(key, "dead-man: ARM positions overdue",
                             f"{b.arm_overdue_positions} positions with no final ARM decision for more than 2 OPEN bars")
        if kind == "arm_notification_undelivered":
            bound_s = self.thresholds.arm_notification_age_ms // 1000
            return Condition(key, f"dead-man: ARM notice undelivered > {bound_s} s",
                             f"oldest undelivered ARM notice {(b.arm_oldest_undelivered_notification_age_ms or 0) // 1000} s")
        return self._describe(key, b, held_ms)

    def _unrecorded_condition(self, b: Beat) -> Condition | None:
        """S38c: unrecorded FAILED episodes reported by the newest beat, or latched by a leader change."""
        failed = b.arm_failed
        if failed is not None and failed.unrecorded > 0:
            return Condition(
                "arm_failed_unrecorded", f"dead-man: ARM FAILED not recorded ({failed.unrecorded} episodes)",
                f"{failed.unrecorded} FAILED episodes held only on node {b.node} (PostgreSQL write failing); a leader "
                "change now loses them (runbook arm.md FAILED)")
        if self._lost is not None:
            node, epoch, _ = self._lost
            return Condition(
                "arm_failed_unrecorded", "dead-man: ARM FAILED episode lost in a leader change",
                f"node {node} epoch {epoch} held unrecorded FAILED episodes when the leader changed; the new leader "
                "starts without them: check `arm failed show` and that node's spool (runbook arm.md FAILED)")
        return None

    def _describe(self, key: str, b: Beat, held_ms: int) -> Condition:
        held = f"for {held_ms // 1000} s"
        kind, _, subject = key.partition(":")
        if kind == "adapter_not_streaming":
            s = b.adapters[subject]
            return Condition(key, f"dead-man: adapter {subject} not STREAMING while its market is open",
                             f"SM4 {s.sm4_state} {held} (market_open, no maintenance window)")
        if kind == "adapter_fields_missing":
            return Condition(key, f"dead-man: adapter {subject} state missing", f"sm4_state / market_open null {held}")
        if kind == "fields_missing":
            return Condition(key, f"dead-man: heartbeat field {subject} missing", f"{subject} null or empty {held}")
        if kind == "sm8_not_ready":
            return Condition(key, "dead-man: SM8 not READY > 60 s", f"SM8 {b.sm8_state} {held}")
        if kind == "sm5_unknown":
            return Condition(key, "dead-man: SM5 UNKNOWN > 60 s", f"SM5 {b.sm5_state} {held}")
        return Condition(key, "dead-man: spill file non-empty > 60 s", f"{b.spill_bytes} bytes {held}")

    def status_line(self, now_ms: int, *, rx_ms: int | None = None) -> str:
        """One line for the daily test page and the log (no secrets)."""
        b = self.last_beat
        rx = now_ms if rx_ms is None else rx_ms
        sources = self._sources_text(rx)
        if b is None or self._last_rx_mono is None:
            return f"no heartbeat yet ({self.rejected} rejected){sources}"
        return (f"last beat {(rx - self._last_rx_mono) // 1000} s ago from {b.node} epoch {b.epoch}, "
                f"gate {b.gate_state}, halt {b.halt_level}, {self.beats} accepted, {self.rejected} rejected"
                f"{sources}")
