"""AlertInhibitor: engine-side inhibition of the dependent Oversee alerts (operations.md §9 Inhibition, §10
BROKER_MAINTENANCE).

* During an adapter's declared maintenance window (SM10 BROKER_MAINTENANCE overlay) its accounts' FENCE alerts
  are suppressed ("liveness / FENCE alerts suppressed for that adapter").
* For `settle_ms` (RTO + 30 s; 120 s, the same bound as `deploy/observability` RTO_PLUS_30_S) after an epoch
  change, the dependent alerts (stale / FENCE) of every account are suppressed: a takeover fences every stream at
  LEADER_INIT, so each SNAPSHOTTING longer than ACCOUNT_FENCE_TIMEOUT_MS would otherwise send an activated and a
  resolved alert per account.
* A maintenance window's end ends the hold-back at once: "FENCE is at full severity from then on" (§10, 7.2; the
  earlier 30 s settle after the window is gone). From that pass on the adapter's stale / FENCE transitions alert
  normally and its still-active input FLAG states re-page every escalate_after_ms (`repages_due`; an episode held
  back in the window re-pages from its `active_since`).
* When the suppression of an account ends, one page is sent for all accounts whose alerts were held back at that
  moment (§9 "then sends one summary or one escalation"):
  - an **escalation** (high) when an account is still fenced and its alerts were held back for an **epoch change**
    (the takeover's reconnect took longer than RTO + 30 s);
  - otherwise an info **summary**. An account still fenced when its adapter's **maintenance window** ends is named in
    it: the adapter only starts reconnecting when the window ends, so that fence is expected; from then on it is at
    full severity (its own alerts and re-pages, and the liveness pages, which are no longer suppressed either).
  - An account still holding a `stale_mark` symbol is named in the summary: that FLAG is info severity, and a
    closed market (weekend, daily break) keeps it active by design.

Only the **alert rows** are held back. The transitions themselves are still written (halt_breaches,
oversee_events), and risk alerts are never touched: dependent alerts are the transitions of the input FLAG rules
(`stale_account_image`, `stale_mark`) and the `capped` / `uncapped` info rows of any rule (an input going stale
or coming back). Activations, raises, lowers and resolutions of every other rule always alert.

Pure bookkeeping, no I/O; owned by the RiskSweep's event loop. `split()` holds nothing back by itself: the
held-back transitions are counted by `record()` once their pass is written (a CAS retry or a failed write counts
nothing). Memory is bounded by the listed accounts and the adapters.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Final, Literal

from src.oversee.rules import INPUT_FLAG_RULES, STALE_ACCOUNT_IMAGE, STALE_MARK
from src.oversee.types import BreachState, Page, Transition

log = logging.getLogger(__name__)

EPOCH_SETTLE_MS: Final = 120_000          # RTO (~45-90 s, operations.md §6) + 30 s
DEPENDENT_TRANSITIONS: Final = frozenset({"capped", "uncapped"})
SUMMARY_CODE: Final = "dependent_alerts_summary"
ESCALATION_CODE: Final = "dependent_alerts_escalation"

Why = Literal["maintenance", "epoch_change"]


def is_dependent(t: Transition) -> bool:
    """A stale / FENCE alert: any transition of an input FLAG rule, or a capped / uncapped info row."""
    return t.rule_id in INPUT_FLAG_RULES or t.transition in DEPENDENT_TRANSITIONS


@dataclass
class _Held:
    adapter_id: str
    count: int = 0
    reasons: set[str] = field(default_factory=set)


class AlertInhibitor:
    """`in_maintenance(adapter_id)`: SM10's overlay (`MarketCalendar.in_maintenance`). `epoch_changed_at_ms()`:
    when this node's current term began (its LEADER_INIT); None = unknown, no epoch suppression. A raising
    callable never suppresses (fail loud). Maintenance suppression lasts exactly as long as the overlay."""

    def __init__(self, *, in_maintenance: Callable[[str], bool] | None = None,
                 epoch_changed_at_ms: Callable[[], int | None] | None = None,
                 settle_ms: int = EPOCH_SETTLE_MS) -> None:
        self._in_maintenance = in_maintenance
        self._epoch_changed_at = epoch_changed_at_ms
        self._settle_ms = settle_ms
        self._held: dict[str, _Held] = {}      # account -> alerts held back since its suppression began

    def suppressed(self, adapter_id: str, now_ms: int) -> Why | None:
        """Why the adapter's dependent alerts are held back now, or None."""
        if self._in_maintenance is not None:
            try:
                if self._in_maintenance(adapter_id):
                    return "maintenance"
            except Exception:
                log.exception("in_maintenance(%s) failed: alerts not suppressed", adapter_id)
        if self._epoch_changed_at is not None:
            try:
                since = self._epoch_changed_at()
            except Exception:
                log.exception("epoch_changed_at failed: alerts not suppressed")
                since = None
            if since is not None and since <= now_ms < since + self._settle_ms:
                return "epoch_change"
        return None

    def split(self, adapter_id: str, transitions: Iterable[Transition],
              now_ms: int) -> tuple[list[Transition], list[Transition], Why | None]:
        """(alerted, held back, why): every transition alerts, except the dependent ones while the adapter is
        suppressed. The adapter is checked on every call, so a window's end is seen even by a pass without
        dependent transitions."""
        why = self.suppressed(adapter_id, now_ms)
        ts = list(transitions)
        if why is None:
            return ts, [], None
        return [t for t in ts if not is_dependent(t)], [t for t in ts if is_dependent(t)], why

    def record(self, account_id: str, adapter_id: str, held: Sequence[Transition], why: Why | None) -> None:
        """Count the held-back transitions of a written pass for the account's summary."""
        if not held or why is None:
            return
        h = self._held.setdefault(account_id, _Held(adapter_id))
        h.adapter_id = adapter_id
        h.count += len(held)
        h.reasons.add(why)

    def filter(self, account_id: str, adapter_id: str, transitions: Iterable[Transition],
               now_ms: int) -> list[Transition]:
        """`split()` then `record()`, for a caller whose write is not retried."""
        out, held, why = self.split(adapter_id, transitions, now_ms)
        self.record(account_id, adapter_id, held, why)
        return out

    @property
    def holding(self) -> frozenset[str]:
        """Accounts with alerts held back whose summary has not been sent yet."""
        return frozenset(self._held)

    def forget(self, account_ids: Iterable[str]) -> None:
        """Accounts gone from the sweep: nothing to summarise for them."""
        for a in account_ids:
            self._held.pop(a, None)

    def summaries(self, states: Sequence[BreachState], now_ms: int) -> list[Page]:
        """Called after each pass with every committed state: one page for all accounts whose suppression has
        ended since the last call. An escalation (high) when any of them is still fenced (an active
        `stale_account_image` state) after an epoch-change hold-back; else an info summary, which names the
        accounts still fenced at the end of a maintenance window (FENCE at full severity from now on) and those
        still holding a `stale_mark` symbol."""
        ended = sorted(a for a, h in self._held.items() if self.suppressed(h.adapter_id, now_ms) is None)
        if not ended:
            return []
        held = {a: self._held.pop(a) for a in ended}

        def active(rule_id: str) -> list[str]:
            return sorted({s.account_id for s in states
                           if s.account_id in held and s.active and s.rule_id == rule_id})

        fenced, stale = active(STALE_ACCOUNT_IMAGE), active(STALE_MARK)
        escalate = [a for a in fenced if "epoch_change" in held[a].reasons]
        after_window = [a for a in fenced if a not in escalate]
        reasons = sorted({r for h in held.values() for r in h.reasons})
        total = sum(h.count for h in held.values())
        detail = (f"{total} stale/FENCE alerts held back ({', '.join(reasons)}) for "
                  f"{len(held)} account(s): {', '.join(ended)}")
        if stale:
            detail += f"; stale_mark still active: {', '.join(stale)}"
        if after_window:
            detail += (f"; still fenced after the maintenance window (reconnecting; FENCE alerts at full severity "
                       f"from now on): {', '.join(after_window)}")
        if escalate:
            return [Page("high", ESCALATION_CODE, None, f"{detail}; still fenced: {', '.join(escalate)}",
                         now_ms, {"accounts": ",".join(escalate)})]
        tail = "" if after_window else "; no account fenced"
        return [Page("info", SUMMARY_CODE, None, f"{detail}{tail}", now_ms,
                     {"accounts": ",".join(after_window)} if after_window else {})]


__all__ = ["AlertInhibitor", "DEPENDENT_TRANSITIONS", "EPOCH_SETTLE_MS", "ESCALATION_CODE", "SUMMARY_CODE", "Why",
           "is_dependent"]
