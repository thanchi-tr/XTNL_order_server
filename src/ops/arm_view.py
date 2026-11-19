"""Read-only ARM views of the operator CLI (design §C2 Operator views, §B6.1 Version lifecycle, §B8, §B9; plan S35).

    arm show [<adapter:position>] [--all] [--limit N]       arm_positions: state, pins, cursor, times
    arm decisions [<adapter:position>] [--from-ms N] [--to-ms N] [--limit N]
    arm config resolve --position <adapter:position>        the assignment in force at arming and now
    arm replay [<adapter:position>] [--from-ms N] [--to-ms N] [--limit N] [--rerun]
               [--request-backfill --operator A --reason "..."]    (src/ops/arm_replay.py, src/ops/arm_rerun.py)
    arm registry check --against-db                         every pinned version still loads (exit 3 if not)

None of these writes anything except `arm replay --request-backfill`, which files one audited `md_backfill` request
per contiguous run of missing buckets (`arm_replay.backfill_ranges`: at most `BACKFILL_CHUNK_MAX` buckets each, at
most `BACKFILL_MAX` per run, and a series' next run only once its previous request is answered; the S34 path:
heartbeat, budget and pending checks, fenced audit row). The views select
ids, codes, counts, hashes' presence and times only: never an entry or S0 level, pinned units, `pre_basis`, a proposed
stop, a risk R or the first-bar ratio (order state that stays in PostgreSQL), and never a market value. Text read from
the database or the store is escaped (control characters as `\\xNN`) and bounded.
"""
from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final

from src.armconfig.document import DocumentProblem, parse_document
from src.armconfig.resolver import PositionScope, resolve, route_policy_at
from src.armconfig.validate import ValidationStatus, escape_text, validate_document
from src.db.arm_config_store import SQL_CONFIG_CACHE_GET, cache_from_row
from src.riskmgr.envelope import EnvelopeStamp
from src.riskmgr.registry import ArmRegistry

from src.ops import arm_sql as A
from src.ops.arm import ArmOps, check_position_target
from src.ops.arm_registry import PinTokens, missing_tokens, pin_tokens, token
from src.ops.arm_replay import BackfillRange, DecisionReplay, Result, StoredInputs, backfill_ranges, replay_one
from src.ops.arm_rerun import Rerunner, target_of
from src.riskmgr.rerun import RerunResult, RerunVerdict
from src.ops.md_view import MdViews, OpsMdReader, series_arg
from src.ops.model import Actor, OpsRefused, OpsUsageError, Report, iso_ms

if TYPE_CHECKING:
    from src.ops.service import OpsService

RESOURCE: Final = "STOP_LEVEL"
LIMIT_MAX: Final = 500
REPLAY_LIMIT_MAX: Final = 200
BACKFILL_MAX: Final = 8
"""At most this many `md_backfill` requests per replay run (the request budget is 20 pending per epoch)."""
TIME_MAX: Final = 2**62
TEXT_LIMIT: Final = 100


def _target(text: str | None) -> tuple[str | None, str | None]:
    if text is None:
        return None, None
    check_position_target(text)
    adapter, _, position = text.partition(":")
    return adapter, position


def _range(from_ms: int | None, to_ms: int | None) -> tuple[int, int]:
    lo = 0 if from_ms is None else from_ms
    hi = TIME_MAX if to_ms is None else to_ms
    if not 0 <= lo <= hi <= TIME_MAX:
        raise OpsUsageError("--from-ms and --to-ms are bar ends in epoch ms with from <= to")
    return lo, hi


def _limit(n: int, hi: int = LIMIT_MAX) -> int:
    if not 1 <= n <= hi:
        raise OpsUsageError(f"--limit must be between 1 and {hi}")
    return n


def _esc(v: object, limit: int = TEXT_LIMIT) -> str | None:
    return None if v is None else escape_text(str(v), limit)


def _texts(v: object) -> tuple[str, ...]:
    return tuple(str(x) for x in v) if isinstance(v, (list, tuple)) else ()


def _hash12(v: object) -> str | None:
    return None if v is None else escape_text(str(v).strip(), 64)[:12]


# ---------------------------------------------------------------------------------------------------- rows
def position_row(r: Sequence[Any]) -> dict[str, Any]:
    """One `SQL_POSITIONS_VIEW` row as shown (column order of `POSITION_VIEW_COLS`)."""
    return {
        "position": _esc(f"{r[0]}:{r[1]}", 200), "account": _esc(r[2], 64), "symbol": _esc(r[3], 64), "side": r[4],
        "state": r[5], "prior_state": r[6], "state_reason": _esc(r[7], 64), "edge": _esc(r[8], 32),
        "attribution": r[9], "strategy": f"{_esc(r[10], 32)}:v{r[11]}", "pin": f"{_esc(r[12], 64)}:{r[13]}",
        "guards_hash": _hash12(r[14]), "bar_spec": _esc(r[15], 16), "config": _hash12(r[16]),
        "entry_time": iso_ms(r[17]), "entry_time_source": r[18], "s0_source": r[19], "cursor": iso_ms(r[20]),
        "cursor_hash": bool(r[21]), "pending": None if r[22] is None else f"{r[22]}@{iso_ms(r[23])}",
        "reopen": iso_ms(r[24]), "decision_seq": r[25], "pre_basis": bool(r[26]), "armed": iso_ms(r[27]),
        "updated": iso_ms(r[28]), "ended": iso_ms(r[29]), "epoch": r[30], "row_version": r[31],
    }


def decision_row(r: Sequence[Any]) -> dict[str, Any]:
    """One `SQL_DECISIONS_VIEW` row as shown (no level, R value or ratio; input counts, not hashes)."""
    return {
        "decision": r[0], "bar_end": iso_ms(r[1]), "position": _esc(f"{r[2]}:{r[3]}", 200),
        "bar_spec": _esc(r[4], 16), "event": r[5], "revision": r[6], "seq": r[7], "decided": iso_ms(r[8]),
        "epoch": r[9], "outcome": r[10], "category": r[11], "reason": _esc(r[12], 64),
        "reason_kind": _esc(r[13], 64), "trigger_met": r[14], "borderline": r[15], "advice": r[16],
        "quality": [_esc(x, 32) for x in _texts(r[17])] or None, "caveats": [_esc(x, 64) for x in _texts(r[18])] or None,
        "close_source": r[19], "stop_basis": r[20], "bar_inputs": len(_texts(r[21])),
        "indicator_inputs": len(_texts(r[22])), "digest": r[23] is not None,
        "envelope": f"v{r[24]}:{_esc(r[25], 64)}", "strategy": f"{_esc(r[26], 64)}:{r[27]}",
        "supersedes": r[28], "range_from": iso_ms(r[29]),
        "notice": "suppressed" if r[30] == 0 else r[30],
    }


def stored_inputs(r: Sequence[Any]) -> StoredInputs:
    return StoredInputs(
        decision_id=int(r[0]), bar_end_ms=int(r[1]), position=f"{r[2]}:{r[3]}", event=str(r[5]),
        revision=int(r[6]), outcome=str(r[10]), decided_at_ms=int(r[8]), series_inputs=_texts(r[21]),
        indicator_inputs=_texts(r[22]), inputs_digest=None if r[23] is None else str(r[23]).strip(),
        envelope=EnvelopeStamp(int(r[24]), str(r[25])))


@dataclass
class DocTokens:
    tokens: set[str]
    unreadable: list[str]


def document_tokens(document_json: str, registry: ArmRegistry) -> DocTokens:
    """The `KIND:VERSION` tokens a cached document's strategy rows reference (strategies, guards and, where the
    params parse on this node, required indicators); rows that do not read are named by ref."""
    out = DocTokens(set(), [])
    try:
        raw = json.loads(document_json, parse_float=Decimal, parse_int=int)
    except (ValueError, RecursionError):
        out.unreadable.append("cached document: not JSON")
        return out
    doc = parse_document(raw)
    if isinstance(doc, DocumentProblem):
        out.unreadable.append(f"cached document: {doc.page_text}")
        return out
    for row in doc.table("strategies"):
        cols = row.columns()
        kind, impl, params = cols.get("kind"), cols.get("impl_version"), cols.get("params")
        pins = pin_tokens(kind, impl, None, None, where=row.ref)
        guards = params.get("guards", []) if isinstance(params, dict) else None
        if not isinstance(guards, list):
            pins.unreadable.append(f"{row.ref}: guards")
            guards = []
        for g in guards:
            t = token(g.get("kind"), g.get("version")) if isinstance(g, dict) else None
            if t is None:
                pins.unreadable.append(f"{row.ref}: guards entry")
            else:
                pins.tokens.add(t)
        out.tokens |= pins.tokens
        out.unreadable.extend(pins.unreadable)
        strategy = None if not isinstance(kind, str) or not isinstance(impl, int) else registry.strategy(kind, impl)
        if strategy is None or not isinstance(params, dict) or not isinstance(kind, str) or not isinstance(impl, int):
            continue
        try:
            parsed = registry.parse_params(kind, impl, params, new_pin=False)
            needs = list(strategy.required_indicators(parsed))
            for g in parsed.guards:
                guard = registry.guard(g.kind, g.version)
                if guard is not None:
                    needs += guard.required_indicators(g)
        except Exception:   # a row this node cannot parse names no indicator; its own token decides
            continue
        for n in needs:
            t = token(n.kind, n.version)
            if t is None:
                out.unreadable.append(f"{row.ref}: required indicator")
            else:
                out.tokens.add(t)
    return out


# ---------------------------------------------------------------------------------------------------- the views
class ArmViews:
    """The S35 read-only ARM commands over one `OpsService` and its `ArmOps` (context, known ids, request filing)."""

    def __init__(self, ops: ArmOps) -> None:
        self._ops = ops
        self._svc: OpsService = ops.service
        self._ctx = ops.context

    async def show(self, target: str | None, *, include_ended: bool, limit: int) -> Report:
        adapter, position = _target(target)
        rep = Report("arm.show")
        rows = await self._svc.conn.fetch(A.SQL_POSITIONS_VIEW, adapter, position, include_ended or target is not None,
                                          _limit(limit))
        rep.rows.extend(position_row(r) for r in rows)
        states: dict[str, int] = {}
        for r in rows:
            states[str(r[5])] = states.get(str(r[5]), 0) + 1
        rep.fields["positions"] = len(rows)
        rep.fields["states"] = ",".join(f"{k}={v}" for k, v in sorted(states.items())) or None
        if target is not None and not rows:
            rep.warnings.append("no arm_positions row for this position (not armed by the ARM)")
        if states.get("FAILED"):
            rep.warnings.append("FAILED positions get no advice until the scope is cleared (`arm failed show`)")
        return rep

    async def decisions(self, target: str | None, *, from_ms: int | None, to_ms: int | None, limit: int) -> Report:
        adapter, position = _target(target)
        lo, hi = _range(from_ms, to_ms)
        rep = Report("arm.decisions")
        rows = await self._svc.conn.fetch(A.SQL_DECISIONS_VIEW, adapter, position, lo, hi, _limit(limit))
        rep.rows.extend(decision_row(r) for r in rows)
        rep.fields["decisions"] = len(rows)
        return rep

    async def config_resolve(self, target: str) -> Report:
        adapter, position = _target(target)
        rep = Report("arm.config.resolve")
        rows = await self._svc.conn.fetch(A.SQL_POSITIONS_VIEW, adapter, position, True, 1)
        if not rows:
            rep.outcome = "refused"
            rep.fields.update({"refused": "not_armed", "detail": "no arm_positions row for this position"})
            return rep
        r = rows[0]
        account, symbol, edge, armed_at = str(r[2]), str(r[3]), str(r[8]), int(r[27])
        rep.fields.update({"position": _esc(target, 200), "armed": iso_ms(armed_at), "state": r[5],
                           "pinned_strategy": f"{_esc(r[10], 32)}:v{r[11]}", "pin": f"{_esc(r[12], 64)}:{r[13]}",
                           "armed_under": _hash12(r[16])})
        cached = await self._svc.conn.fetchrow(SQL_CONFIG_CACHE_GET)
        if cached is None:
            rep.outcome = "refused"
            rep.fields.update({"refused": "no_cached_document",
                               "detail": "no document is installed (`arm config show`)"})
            return rep
        cache = cache_from_row(cached)
        rep.fields["installed"] = cache.doc_hash[:12]
        if cache.doc_hash != str(r[16]).strip():
            rep.warnings.append("the installed document is not the one this position was armed under: the "
                                "resolution below is against the installed document")
        try:
            known, _, notes = await self._ops.local_ids()
        except OpsRefused as exc:
            rep.outcome = "refused"
            rep.fields.update({"refused": exc.code, "detail": exc.detail})
            return rep
        rep.warnings.extend(notes)
        try:
            raw = json.loads(cache.document_json, parse_float=Decimal, parse_int=int)
        except (ValueError, RecursionError):
            raw = None
        doc = parse_document(raw)
        if isinstance(doc, DocumentProblem):
            rep.outcome = "refused"
            rep.fields.update({"refused": "cache_unreadable", "detail": doc.page_text})
            return rep
        now = self._svc.clock_ms()
        result = validate_document(doc, known, self._ctx.registry, seen=dict(cache.row_hashes), at_ms=now)
        valid = result.document
        if result.status is not ValidationStatus.VALID or valid is None:
            rep.outcome = "refused"
            rep.fields.update({"refused": "cache_invalid",
                               "detail": "; ".join(p.page_text for p in result.problems[:20])})
            return rep
        scope = PositionScope(adapter_id=str(adapter), account_id=account, symbol=symbol, edge_code=edge)
        for label, at in (("at_arming", armed_at), ("now", now)):
            got = resolve(valid, scope, RESOURCE, at)
            if got is None:
                rep.sections[label] = [{"assignment": None, "strategy": None}]
                continue
            a, s = got.assignment, got.strategy
            rep.sections[label] = [{
                "assignment": f"assignments:{a.row_id}", "effective_from": iso_ms(a.effective_from_ms),
                "scope": ",".join(n for n, on in zip(("edge", "symbol", "account", "adapter"), a.specificity) if on)
                or "wildcard", "strategy": f"{escape_text(s.code, 32)}:v{s.version}",
                "pin": f"{escape_text(s.kind, 64)}:{s.impl_version}",
                "matches_pin": (s.code, s.version) == (str(r[10]), int(r[11]))}]
        policy = route_policy_at(valid, f"{adapter}:{account}", now)
        rep.fields.update({"route_policy": f"route_policy:{policy.row_id}" if policy.row_id else "default",
                           "mode": policy.mode, "pre_channels": list(policy.pre_channels),
                           "post_channels": list(policy.post_channels)})
        rep.warnings.append("the pin is read from arm_positions, never from the document: a newer row or a tombstone "
                            "never changes an armed position (`arm repin` moves it to another version)")
        return rep

    async def registry_check(self) -> Report:
        """`arm registry check --against-db` (module docstring): refused (exit 3) when a referenced version is not
        in this node's registry, or a stored pin cannot be read (fail closed)."""
        rep = Report("arm.registry.check")
        conn = self._svc.conn
        registry = self._ctx.registry
        pins = PinTokens()
        live: set[str] = set()
        positions = 0
        for r in await conn.fetch(A.SQL_PINNED_VERSIONS):
            group = pin_tokens(r[0], r[1], r[2], r[3], where=f"arm_positions {_esc(r[0], 64)}:{r[1]}")
            pins.tokens |= group.tokens
            pins.unreadable.extend(group.unreadable)
            positions += int(r[4])
            if int(r[5]):
                live |= group.tokens
        decided: set[str] = set()
        for r in await conn.fetch(A.SQL_DECISION_VERSIONS):
            t = token(r[0], r[1])
            if t is None:
                pins.unreadable.append(f"arm_decisions {_esc(r[0], 64)}:{r[1]}")
            else:
                decided.add(t)
        cached = await conn.fetchrow(SQL_CONFIG_CACHE_GET)
        doc = DocTokens(set(), []) if cached is None else document_tokens(cache_from_row(cached).document_json,
                                                                           registry)
        wanted = pins.tokens | decided | doc.tokens
        missing = missing_tokens(wanted, registry)
        rep.fields.update({"registry": list(registry.registry_tokens()), "referenced": sorted(wanted),
                           "positions": positions, "live_pins": sorted(live), "decision_versions": sorted(decided),
                           "document_versions": sorted(doc.tokens), "missing": missing or None})
        for t in missing:
            where = [n for n, s in (("live position", live), ("position", pins.tokens), ("decision", decided),
                                    ("installed document", doc.tokens)) if t in s]
            rep.rows.append({"missing": escape_text(t, 100), "referenced_by": ",".join(where)})
        unreadable = pins.unreadable + doc.unreadable
        if unreadable:
            rep.sections["unreadable"] = [{"where": escape_text(u, 200)} for u in unreadable[:50]]
        if missing or unreadable:
            rep.outcome = "refused"
            rep.fields["refused"] = "registry_missing" if missing else "pin_unreadable"
            rep.warnings.append("this node cannot evaluate, replay or correct every stored pin: deploy a build "
                                "whose registry holds every referenced KIND:VERSION (never remove a pinned version)")
        return rep

    # ------------------------------------------------------------------------------------------------ replay
    async def _reader(self) -> OpsMdReader:
        try:
            reader = self._ctx.md_reader()
        except Exception as exc:
            raise OpsRefused("md_reader_settings", type(exc).__name__) from None
        if reader is None:
            raise OpsRefused("no_md_reader", "XTNL_MD_REDIS_READER_URL is not set: redis-md cannot be read")
        if isinstance(reader, list):
            raise OpsRefused("md_reader_settings", "; ".join(reader))
        return reader

    async def md(self, command: str, *, series: str | None = None, newest: int = 16,
                 indicator_id: str | None = None) -> Report:
        """`md show | bars | indicators` over the `md_reader` client (src/ops/md_view.py)."""
        sid = series_arg(series) if command == "bars" and series is not None else None
        try:
            reader = await self._reader()
        except OpsRefused as exc:
            rep = Report(f"md.{command}")
            rep.outcome = "refused"
            rep.fields.update({"refused": exc.code, "detail": exc.detail})
            return rep
        try:
            views = MdViews(reader, self._svc.clock_ms)
            if command == "show":
                return await views.show()
            if command == "bars" and sid is not None:
                return await views.bars(sid, newest=newest)
            return await views.indicators(indicator_id=indicator_id)
        except OpsRefused as exc:
            rep = Report(f"md.{command}")
            rep.outcome = "refused"
            rep.fields.update({"refused": exc.code, "detail": exc.detail})
            return rep
        finally:
            await reader.aclose()

    async def replay(self, target: str | None, *, from_ms: int | None, to_ms: int | None, limit: int,
                     request_backfill: bool, actor: Actor | None, wait_s: float, rerun: bool = False) -> Report:
        """Verify the recorded inputs as of each decision's time; with `rerun` also re-run the real pipeline for
        each past bar (`src/ops/arm_rerun.py`, plan S39b: read-only, no notice) and report MATCH, DIFF or
        UNVERIFIABLE by the recomputed `inputs_digest` and the outcome."""
        adapter, position = _target(target)
        lo, hi = _range(from_ms, to_ms)
        _limit(limit, REPLAY_LIMIT_MAX)
        if request_backfill:
            if actor is None:
                raise OpsUsageError("--request-backfill files audited md_backfill requests: add --operator and "
                                    "--reason")
            actor.validate(two_person=False)
        rep = Report("arm.replay")
        try:
            reader = await self._reader()
            keyring = reader.require_keyring()
        except OpsRefused as exc:
            rep.outcome = "refused"
            rep.fields.update({"refused": exc.code, "detail": exc.detail})
            return rep
        replays: list[DecisionReplay] = []
        reruns: dict[int, RerunVerdict] = {}
        rerunner: Rerunner | None = None
        rerun_error: str | None = None
        try:
            rows = await self._svc.conn.fetch(A.SQL_DECISIONS_VIEW, adapter, position, lo, hi, limit)
            now = self._svc.clock_ms()
            for r in rows:
                replays.append(await replay_one(reader, keyring, stored_inputs(r), now, self._ctx.envelope))
            if rerun and self._ctx.arm_settings is not None:
                try:
                    rerunner = await Rerunner.build(self._svc.conn, reader, keyring, self._ctx.arm_settings)
                except OpsRefused:
                    raise
                except Exception as exc:              # the inputs report still stands
                    rerun_error = type(exc).__name__
            if rerunner is not None:
                for r in rows:
                    t = target_of(r)
                    try:
                        reruns[t.decision_id] = await rerunner.run(t)
                    except OpsRefused:
                        raise
                    except Exception as exc:          # one decision's re-run never ends the report
                        reruns[t.decision_id] = RerunVerdict(RerunResult.UNVERIFIABLE,
                                                             f"error:{type(exc).__name__}")
        except OpsRefused as exc:
            rep.outcome = "refused"
            rep.fields.update({"refused": exc.code, "detail": exc.detail, "replayed": len(replays)})
            return rep
        finally:
            if rerunner is not None:
                rerunner.close()
            await reader.aclose()
        counts: dict[str, int] = {}
        changed_env = 0
        for rp in replays:
            text = rp.result.value + (f"({rp.reason})" if rp.reason else "")
            counts[text] = counts.get(text, 0) + 1
            changed_env += rp.envelope == "ENVELOPE_CHANGED"
            rep.rows.append({
                "decision": rp.stored.decision_id, "bar_end": iso_ms(rp.stored.bar_end_ms),
                "position": _esc(rp.stored.position, 200), "event": rp.stored.event, "revision": rp.stored.revision,
                "outcome": rp.stored.outcome, "result": text, "envelope": rp.envelope,
                "inputs": ",".join(f"{_esc(v.series_id, 120)}@{iso_ms(v.bar_end_ms)}={v.text}"
                                   for v in rp.inputs) or None,
                "digest": None if rp.digest is None else rp.digest.result.value + (
                    f"({rp.digest.reason})" if rp.digest.reason else ""),
                "indicator_inputs": len(rp.stored.indicator_inputs) or None,
                **({} if rp.stored.decision_id not in reruns else {
                    "rerun": reruns[rp.stored.decision_id].text,
                    "rerun_outcome": reruns[rp.stored.decision_id].outcome})})
        rep.fields.update({"replayed": len(replays), "results": ",".join(f"{k}={v}" for k, v in sorted(counts.items()))
                           or None, "envelope_changed": changed_env})
        if rerun:
            rerun_counts: dict[str, int] = {}
            for v in reruns.values():
                rerun_counts[v.text] = rerun_counts.get(v.text, 0) + 1
            rep.fields["reruns"] = ",".join(f"{k}={n}" for k, n in sorted(rerun_counts.items())) or None
            if self._ctx.arm_settings is None:
                rep.warnings.append("--rerun: the local ARM settings do not parse; the pipeline was not re-run")
            if rerun_error is not None:
                rep.warnings.append(f"--rerun: the re-run could not be set up ({rerun_error}); the pipeline was "
                                    "not re-run")
            if any(v.result is RerunResult.DIFF for v in reruns.values()):
                rep.warnings.append("DIFF: re-run on the same inputs, the pipeline gave another outcome than the "
                                    "stored decision (the order state is read as of now except S_at_T: compare "
                                    "`arm decisions` and the position's stop changes since)")
        if self._ctx.envelope is None:
            rep.warnings.append("the local ARM settings do not parse (" + "; ".join(self._ctx.envelope_problems)
                                + "): the envelope cannot be compared")
        if changed_env:
            rep.warnings.append("ENVELOPE_CHANGED: a re-run today would use another envelope (version or "
                                "XTNL_ARM_MIN_GAP_R) than the decision recorded")
        if any(rp.result in (Result.CHANGED, Result.DIGEST_MISMATCH) for rp in replays):
            rep.warnings.append("CHANGED / DIGEST_MISMATCH: a stored decision no longer matches its inputs as of "
                                "its decision time; compare `md bars <series>` (quarantine, revisions) and the "
                                "decision's arm_correction_audit rows")
        if any(rp.stored.indicator_inputs for rp in replays):
            rep.warnings.append("indicator inputs are checked through the inputs digest only (their window bars "
                                "are not re-read)")
        if not rerun:
            rep.warnings.append("the replay verifies each decision's recorded inputs as of its decision time; add "
                                "--rerun to re-run the pipeline for each bar (read-only, no notice)")
        ranges = backfill_ranges(replays)
        if ranges and not request_backfill:
            names = sorted({b.series_id for b in ranges})
            rep.warnings.append(f"bars_missing: re-run with --request-backfill --operator A --reason \"...\" to file "
                                f"{len(ranges)} md_backfill request(s) for "
                                + ", ".join(escape_text(s, 120) for s in names[:8]))
        if ranges and request_backfill and actor is not None:
            filed: list[dict[str, Any]] = []
            rest: list[BackfillRange] = []
            held: set[str] = set()      # one pending md_backfill per series: the leader refuses a second one
            for b in ranges:
                if len(filed) >= BACKFILL_MAX or b.series_id in held:
                    rest.append(b)
                    continue
                got = await self._ops.md_backfill(b.series_id, b.first_bucket_ms, b.last_bucket_ms, actor,
                                                  wait_s=wait_s)
                filed.append({"series": escape_text(b.series_id, 120), "from": iso_ms(b.first_bucket_ms),
                              "to": iso_ms(b.last_bucket_ms), "outcome": got.outcome,
                              "request": got.fields.get("request"), "refused": got.fields.get("refused"),
                              "audit_id": got.audit_id})
                if got.outcome == "refused" or got.fields.get("request") == "pending":
                    held.add(b.series_id)
            rep.sections["backfill"] = filed
            if rest:
                rep.sections["backfill_not_filed"] = [
                    {"series": escape_text(b.series_id, 120), "from": iso_ms(b.first_bucket_ms),
                     "to": iso_ms(b.last_bucket_ms)} for b in rest[:50]]
                rep.warnings.append(f"{len(rest)} more md_backfill range(s) are needed (at most {BACKFILL_MAX} per "
                                    "run, one pending per series): run the replay again once these are answered")
        return rep


__all__ = ["ArmViews", "DocTokens", "decision_row", "document_tokens", "position_row", "stored_inputs"]
