# Proposed change (S35): a pinned-registry check in `verify_execution.py` (design §B6.1 Version lifecycle, PR13)

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it:
the same assertion runs in the pytest leg of the green gate today
(`tests/ops/test_arm_views.py::test_pinned_tokens_all_load_in_this_registry`), so a build whose registry lacks a
released version already fails G. Nothing here narrows a hook, a deny-list or `FORBIDDEN_PATTERNS`.

## Why

Design §B6.1: a strategy `(KIND, IMPL_VERSION)`, guard `(KIND, VERSION)` or indicator `(KIND, VERSION)` is never removed
from the registry while any `arm_positions` row or accepted document references it, and "`verify_execution.py` and
`python -m src.ops arm registry check --against-db` fail a deploy that lacks a pinned version". S35 builds both halves:

* `src/ops/arm_registry.py` holds `PINNED_TOKENS`, the reviewed, append-only list of every `KIND:VERSION` released for
  pinning (today `break_even_on_risk_close:1`, `first_bar:1`, `ATR:1`), and `pinned_registry_problems()`.
* `python -m src.ops arm registry check --against-db` (`src/ops/arm_view.py`) checks the versions the database really
  references (positions, decisions, the installed document) on a running deploy (exit 3 when one is missing).

`verify_execution.py` runs without a database, so its check is the static half: every token of `PINNED_TOKENS` loads in
this build's registry, and the list is well formed. A release that adds a version adds its token to `PINNED_TOKENS`;
taking a token out is a review-visible change to that list.

The check body below was run against today's tree (passes: the three tokens load).

## 1. `.claude/skills/live-trading-server/scripts/verify_execution.py` (unified diff)

```diff
@@ Checks (current design, .claude/rules/*):
   6. Oversee: ActionGate fails closed; no Oversee timers exist (no oversee:timers usage).
+  7. ARM registry: every released (pinned) strategy / guard / indicator version still loads (design §B6.1).
 """
@@
 @check("oversee: ActionGate exists and fails closed on missing state")
 def _() -> None:
     from src.oversee import gate as gatemod
 
     assert hasattr(gatemod, "ActionGate"), "src/oversee/gate.py must define ActionGate"
 
 
+@check("arm: every pinned KIND:VERSION is still in the registry")
+def _() -> None:
+    from src.ops.arm_registry import PINNED_TOKENS, pinned_registry_problems
+
+    assert PINNED_TOKENS, "PINNED_TOKENS must list the released versions"
+    for t in PINNED_TOKENS:
+        kind, _, version = t.rpartition(":")
+        assert kind and version.isdigit() and int(version) >= 1, f"malformed pinned token {t!r}"
+    problems = pinned_registry_problems()
+    assert not problems, "; ".join(problems)
+
+
 def main() -> int:
```

With the check, `verify_execution.py --strict` reports 12 / 12 (the module exists, so it is never a SKIP).

## 2. Nothing else

No hook, rule or `CLAUDE.md` text changes for S35. (The runbook and rule wording for the views lands with S37.)
