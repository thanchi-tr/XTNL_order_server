"""The ARM go-live gates (design §0b "Go-live gates"; plan S38e).

The ARM may advise on real positions only when every go-live item is **wired** (built and tested in this tree) or
**explicitly accepted by the user**. Two places enforce it:

* `XTNL_ARM_ENABLED=1`: `optional-bootstrap` leaves the ARM idle for the term, with one page ("ARM not started"
  naming the gate ids), while any gate is OPEN and not named in `XTNL_ARM_GOLIVE_ACCEPT` (a comma-separated list of
  gate ids, a local setting, never Supabase). The market-data layer still runs; ingestion and Oversee are untouched.
  `python -m src.ops arm gates` prints the same verdict offline (exit 3 while enabling would be refused).
* **S39** (making StoneX bar-capable, `bar_capable_adapters()` below; `src/ops/arm.py::BAR_CAPABLE_ADAPTERS`):
  `s39_blockers()` lists the gates S39 itself does not close;
  `tests/riskmgr/test_s38e.py::test_s39_waits_for_every_gate_it_does_not_close_itself` fails while an adapter is
  bar-capable and any of them is still OPEN in this table. A user acceptance for S39 is recorded here (state ACCEPTED,
  with the date and the user's words in `detail`), never inferred; an `XTNL_ARM_GOLIVE_ACCEPT` entry never makes an
  adapter bar-capable. S39 is built (2026-11-17): StoneX `fetch_price_bars` serves bars and `pr3_finality` is wired,
  but `bar_capable_adapters()` is derived from this table, so it stays empty until no S39 blocker is OPEN here. Both
  readers use it: `arm config accept` / admission (`src/ops/arm.py`) and the runtime arm-tracker's `bar_sides`
  (`src/runtime/optional_plan.py::_bars_capable`: the adapter's `price_bars_supported` **and** membership here), so
  no assignment, wildcard or adapter-scoped, can arm a StoneX position while it is empty (S39 review, major).

A gate is OPEN until the stage that closes it changes its state here (and its test proves the wiring). The table
holds ids, titles and states only.
Plan S39b wired the six engineering gates (`uptime_suspend`, `receipt_gap_feed`, `replay_rerun`,
`conformance_probe_partial`, `entry_time_source`, `corrections_restart`; `tests/riskmgr/test_s39b.py`); the open ones
left are user decisions and PR3 measurements.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final

ACCEPT_VAR: Final = "XTNL_ARM_GOLIVE_ACCEPT"
"""Local setting: the OPEN gate ids the user explicitly accepts for `XTNL_ARM_ENABLED=1` (comma-separated)."""


class GateState(str, Enum):
    WIRED = "wired"
    OPEN = "open"
    ACCEPTED = "accepted"
    """Accepted by the user in this table (S39 and the runtime both honour it)."""


@dataclass(frozen=True)
class Gate:
    gate_id: str
    title: str
    state: GateState
    detail: str
    closed_by_s39: bool = False
    """S39 itself closes this gate (it is not an S39 blocker)."""


GATES: Final[tuple[Gate, ...]] = (
    # ---------------------------------------------------------------- wired (built and tested)
    Gate("clear_probes", "every FAILED scope can be cleared", GateState.WIRED,
         "S38e: series_reread, fresh_bar and strategy_conformance probes wired (ClearProbes has no unwired port)"),
    Gate("d7_triggers", "D7 unresolved and refetch_failed triggers fire", GateState.WIRED,
         "S38e: md-bars feeds note_unresolved / note_final / note_no_trade and emits refetch_failed"),
    Gate("d4_corrections", "D4 correction re-checks", GateState.WIRED,
         "S38d: events_since + check_decision in the BarClock; a release waits for the verification re-read"),
    Gate("md_reset_notice", "MD_RESET info notice", GateState.WIRED, "S38d: the bootstrap's reset listener"),
    Gate("broker_retention", "BROKER bar retention (1 year)", GateState.WIRED,
         "S38e: RETAIN_BROKER for every stored BROKER series; measured memory in the demand ceiling"),
    Gate("redis_md_grace", "redis-md unreachable or loading is not a FAILED", GateState.WIRED,
         "S38e: unreachable / LOADING / BUSY bootstraps are retried uncounted; fail_md_down_s applies"),
    # ---------------------------------------------------------------- open: user decisions
    Gate("oversee_c4", "S07 Oversee stop rule confirmed (C4)", GateState.ACCEPTED,
         "accepted by the user 2026-11-18 (\"Yes, turn it on\"): C4 confirmed; the S07 stop rule still runs only "
         "with XTNL_OVERSEE_STOP_AT_ENTRY=1, which the user sets in their own environment"),
    Gate("s21_c1", "S21 envelope pipeline accepted, C1 confirmed", GateState.ACCEPTED,
         "accepted by the user 2026-11-18 (\"Pre-move stop\"): S21 accepted and C1 = keep the pre-move stop "
         "(pre_basis) until a bar end confirms"),
    Gate("q1_q2", "Q1 and Q2 answered", GateState.ACCEPTED,
         "answered by the user 2026-11-18: Q1 (a) \"Straight to break-even\"; Q2 \"Range, ATR incl. bar 0\" "
         "(high - low > 1.318 x ATR(14) including bar 0)"),
    Gate("c2_c3_c5", "C2, C3 and C5 confirmed", GateState.ACCEPTED,
         "accepted by the user 2026-11-18 (\"Accept all three\"): C2 keep recording ticks while FAILED; C3 re-read "
         "and decide at the reopen; C5 no favour figures on PRE"),
    Gate("claude_rules", "the pending .claude/ proposals approved", GateState.ACCEPTED,
         "approved by the user 2026-11-18 (\"Apply all as written\"): all 16 proposals under "
         "docs/design/proposed-claude-changes/ applied to .claude/ (hooks, rules, verify_execution checks 7 and 8)"),
    Gate("s25_registry", "S25 registry change signed off", GateState.ACCEPTED,
         "signed off by the user 2026-11-18 (\"Sign off\"): src/riskmgr/registry.py parse_params(new_pin=True) "
         "refuses a strategy whose trigger bar spec has no timing row"),
    # ---------------------------------------------------------------- S39 / PR3
    Gate("pr3_finality", "PR3 probe run; broker-bar finality and stop reads set from it", GateState.WIRED,
         "S39: PR3 run 2026-11-17 (StoneX 1m / 15m bid and ask bars, BarDate = bucket start, final by +5 s); "
         "StoneX fetch_price_bars enabled; finality stays two equal reads (trusts_next_bucket / "
         "trusts_partial_flag False), XTNL_STONEX_PRICE_UNFILTERED 0 (delivery filtered)",
         closed_by_s39=True),
    Gate("pr3_stop_children", "PR9 stop children confirmed in activestoplimitorders", GateState.OPEN,
         "not measured by PR3: XTNL_ARM_STOP_READS stays 0; run probe_bars --check-stops with an open position "
         "that has a stop; manual stop moves on ORDERS and LastChangedDateTimeUTC on amendments are unmeasured"),
    Gate("pr3_roll_prints", "StoneX bars across the daily roll compared", GateState.OPEN,
         "not measured by PR3: whether 15m bid bars covering 16:45-17:15 New York carry break prints (several "
         "days against TICK bars); roll_affected is set on every bar 0 overlapping 17:00 New York meanwhile"),
    # ---------------------------------------------------------------- S39b: the six engineering gates (wired)
    Gate("uptime_suspend", "BarClock uptime suspend signal", GateState.WIRED,
         "S39b: the uptime tracker's suspend signal reaches the BarClock and every strategy job "
         "(JobRunner.note_suspended, and its own monotonic-jump check); a job whose run spans a sleep gets a fresh "
         "awake cap and is never a job_timeout or a strategy fault (job_suspended); the bars the sleep covered are "
         "spanned DOWNTIME"),
    Gate("receipt_gap_feed", "receipt-gap detector heartbeat feed", GateState.WIRED,
         "S39b: the orchestrator's per-adapter frame hook (on_stream_frame, every prices-stream heartbeat) feeds "
         "MdHook.note_frame and the watch is on per adapter (RECEIPT_GAP_MS); a silence blinds the TICK buckets "
         "it overlaps"),
    Gate("replay_rerun", "arm replay re-runs the pipeline", GateState.WIRED,
         "S39b: arm replay --rerun re-runs the real S21 evaluation and job runner for each past bar over as-of "
         "ports (src/riskmgr/rerun.py, src/ops/arm_rerun.py), read-only, no notice: MATCH, DIFF or UNVERIFIABLE "
         "by inputs_digest and outcome"),
    Gate("conformance_probe_partial", "strategy_conformance probe runs the version's own code", GateState.WIRED,
         "S39b: the probe job runs the declaration checks and the pinned version's own code against its "
         "conformance vectors (src/riskmgr/vectors.py) under the job cap; a hung, raising or wrong version fails it"),
    Gate("entry_time_source", "history execution-time source wired", GateState.WIRED,
         "S39b: the arm-tracker upgrades an ESTIMATED entry time to BROKER from the trade-history execution time "
         "(IngestionOrchestrator.history_execution_time: read-only fetch_history at the reconcile priority, StoneX "
         "ExecutedDateTimeUtc), in its own task, until bar 0's decision_cap; entry_time_ambiguous stays False"),
    Gate("corrections_restart", "correction window survives a restart", GateState.WIRED,
         "S39b: at a term's first correction pass the BarClock rebuilds its recheck set from the committed final "
         "decisions still inside their decision_cap window (stored input hashes, RecentDecisionReader); memory "
         "only, no notice re-sent; a revision found later is a new CORRECTED item"),
)

BY_ID: Final[Mapping[str, Gate]] = {g.gate_id: g for g in GATES}


@dataclass(frozen=True)
class GateVerdict:
    """What `XTNL_ARM_ENABLED=1` meets: the OPEN gates not accepted (refused while any), the accepted ones, and
    problems with the setting (unknown ids: ids only, never refused silently)."""

    refused: tuple[Gate, ...]
    accepted: tuple[str, ...]
    problems: tuple[str, ...]

    @property
    def allowed(self) -> bool:
        return not self.refused


def accepted_ids(environ: Mapping[str, str]) -> tuple[frozenset[str], list[str]]:
    """The gate ids `XTNL_ARM_GOLIVE_ACCEPT` names, and the problems (an unknown or a WIRED id; ids only)."""
    raw = environ.get(ACCEPT_VAR) or ""
    ids: set[str] = set()
    problems: list[str] = []
    for item in (p.strip() for p in raw.split(",")):
        if not item:
            continue
        gate = BY_ID.get(item)
        if gate is None:
            problems.append(f"{ACCEPT_VAR}: {item[:32]!r} is not a go-live gate id")
            continue
        if gate.state is GateState.WIRED:
            problems.append(f"{ACCEPT_VAR}: {item} is already wired (nothing to accept)")
            continue
        ids.add(item)
    return frozenset(ids), problems


STOP_AT_ENTRY_VAR: Final = "XTNL_OVERSEE_STOP_AT_ENTRY"
"""The S07 Oversee stop rule's local switch (`src.oversee.risk.STOP_AT_ENTRY_VAR`, plan S38f)."""
STOP_RULE_GATE: Final = "oversee_c4"


def verdict(environ: Mapping[str, str], gates: tuple[Gate, ...] = GATES, *,
            stop_rule_in_force: bool | None = None) -> GateVerdict:
    """The go-live verdict for `XTNL_ARM_ENABLED=1` on this node's local settings. Accepting `oversee_c4` means
    running the S07 stop rule, so it counts only together with `XTNL_OVERSEE_STOP_AT_ENTRY=1` (plan S38f): the ARM
    always reads stops by the S07 rule and must never advise on a rule Oversee is not running.
    `stop_rule_in_force`: the rule Oversee actually runs in this process (`src.oversee.risk.stop_at_entry()`, set
    once by `build_engine`); the engine passes it, so a setting read differently or changed in-process never accepts
    `oversee_c4` while Oversee runs the pre-S07 rule (R14 review). None (the offline `arm gates` report): the setting
    alone, which is what the engine would read at its start."""
    ids, problems = accepted_ids(environ)
    switch_on = (environ.get(STOP_AT_ENTRY_VAR) or "").strip() == "1" and stop_rule_in_force is not False
    if STOP_RULE_GATE in ids and (environ.get(STOP_AT_ENTRY_VAR) or "").strip() != "1":
        ids = ids - {STOP_RULE_GATE}
        problems.append(f"{ACCEPT_VAR}: {STOP_RULE_GATE} counts only with {STOP_AT_ENTRY_VAR}=1")
    elif STOP_RULE_GATE in ids and stop_rule_in_force is False:
        ids = ids - {STOP_RULE_GATE}
        problems.append(f"{ACCEPT_VAR}: {STOP_RULE_GATE} counts only while Oversee runs the S07 stop rule "
                        f"({STOP_AT_ENTRY_VAR}=1 read at the engine start)")
    refused = tuple(g for g in gates if g.state is GateState.OPEN and g.gate_id not in ids)
    # A table acceptance of oversee_c4 (C4 confirmed) still counts only while Oversee runs the S07 stop rule: the
    # ARM must never advise on a stop rule Oversee is not running (2026-11-18, user accepted C4 in the table).
    c4 = next((g for g in gates if g.gate_id == STOP_RULE_GATE), None)
    if c4 is not None and c4.state is GateState.ACCEPTED and not switch_on:
        refused = refused + (c4,)
        problems.append(f"{STOP_RULE_GATE} is accepted but counts only while Oversee runs the S07 stop rule "
                        f"({STOP_AT_ENTRY_VAR}=1 read at the engine start)")
    accepted = tuple(sorted(g.gate_id for g in gates if g.state is GateState.OPEN and g.gate_id in ids))
    return GateVerdict(refused, accepted, tuple(problems))


def s39_blockers(gates: tuple[Gate, ...] = GATES) -> tuple[Gate, ...]:
    """The OPEN gates S39 does not close itself: S39 (an adapter in `BAR_CAPABLE_ADAPTERS`) waits for them."""
    return tuple(g for g in gates if g.state is GateState.OPEN and not g.closed_by_s39)


#: Adapters whose `fetch_price_bars` is built and confirmed by the PR3 probe (`BrokerAdapter.price_bars_supported`):
#: StoneX since S39; Pepperstone is not bar-capable in v1. A reviewed constant (the ops package never imports an
#: adapter, tests/ops/test_cli.py); the ARM uses an adapter's bars only through `bar_capable_adapters()`.
BROKER_BAR_ADAPTERS: Final[frozenset[str]] = frozenset({"stonex"})


def bar_capable_adapters(blockers: Sequence[object] | None = None) -> frozenset[str]:
    """The adapters the ARM treats as bar-capable (admission, `arm config accept` and the arm-tracker's
    `bar_sides`): `BROKER_BAR_ADAPTERS` once no go-live gate S39 waits for is OPEN in this table (`s39_blockers()`;
    a user acceptance is recorded here, never inferred, and never taken from `XTNL_ARM_GOLIVE_ACCEPT`), else none
    (plan S38e / S39)."""
    waiting = s39_blockers() if blockers is None else blockers
    return frozenset() if waiting else BROKER_BAR_ADAPTERS


def refusal_text(v: GateVerdict) -> str:
    """The page body / ops line: gate ids only."""
    return (f"{len(v.refused)} go-live gate(s) open and not accepted ({ACCEPT_VAR}): "
            + ", ".join(g.gate_id for g in v.refused))


__all__ = ["ACCEPT_VAR", "BROKER_BAR_ADAPTERS", "BY_ID", "GATES", "Gate", "GateState", "GateVerdict", "accepted_ids", "bar_capable_adapters",
           "refusal_text",
           "s39_blockers", "verdict"]
