"""S24 static checks: FENCE in every ARM store write, the §B3 write ban on `arm_controls` / `arm_accepted` and on
`cleared_at_ms` / `clear_request_id` (set, or named by an `arm_failures` INSERT / UPDATE / MERGE) outside `src/ops`
(except the clear handler's statement), and the `arm_ports` import rule. The database backs the second rule: the
`xtnl_arm` role has INSERT and UPDATE on `arm_failures` only for the other columns (0017, `ARM_COLUMN_GRANTS`)."""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from types import ModuleType

import pytest

from src.db import arm_config_store, arm_failures_store, arm_ports, arm_status_store, arm_store, arm_versions
from src.db.fence import FENCE

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
FENCED_MODULES: tuple[ModuleType, ...] = (arm_store, arm_failures_store, arm_config_store, arm_versions)
WRITE_VERBS = ("INSERT", "UPDATE", "DELETE")

#: An identifier may be double-quoted and schema-qualified (`public.` / `"public".`).
_Q = r'"?'
_TABLE_PREFIX = rf"\s+(?:ONLY\s+)?(?:{_Q}public{_Q}\.)?{_Q}"
OPERATOR_TABLE_WRITE = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?|MERGE\s+INTO)" + _TABLE_PREFIX
    + r"arm_(?:controls|accepted)\b", re.IGNORECASE)
SETS_CLEARED = re.compile(r"\b(?:cleared_at_ms|clear_request_id)\b\"?\s*=(?!=)", re.IGNORECASE)
#: A statement that writes `arm_failures` (any form: a SET list, a row SET, an INSERT column list, MERGE) ...
WRITES_FAILURES = re.compile(r"\b(?:INSERT\s+INTO|UPDATE|MERGE\s+INTO)" + _TABLE_PREFIX + r"arm_failures\b",
                             re.IGNORECASE)
#: ... may name the owner-only columns only in an `IS NULL` read (the open-episode filter).
OWNER_ONLY_NULL_READ = re.compile(r"\b(?:\w+\.)?\"?(?:cleared_at_ms|clear_request_id)\"?\s+IS\s+NULL\b",
                                  re.IGNORECASE)
OWNER_ONLY_COLUMN = re.compile(r"\b(?:cleared_at_ms|clear_request_id)\b", re.IGNORECASE)
#: Stands in for an f-string field or a name that is not a module-level string constant.
UNRESOLVED = " ? "
#: The one statement outside src/ops that may set cleared_at_ms (the arm_clear_failed handler's, engine pool).
CLEAR_EXCEPTION = ("src/db/arm_failures_store.py", "SQL_FAILURE_CLEAR")


def _placeholders(sql: str) -> set[int]:
    return {int(m) for m in re.findall(r"\$(\d+)", sql)}


@pytest.mark.parametrize("module", FENCED_MODULES, ids=lambda m: m.__name__)
def test_every_write_statement_is_fenced(module: ModuleType) -> None:
    for name in dir(module):
        sql = getattr(module, name)
        if name.startswith("SQL_") and isinstance(sql, str) and sql.lstrip("(").split()[0] in WRITE_VERBS:
            assert FENCE in sql, name
            assert 1 in _placeholders(sql), name


def test_arm_status_is_the_named_unfenced_exception() -> None:
    writes = [n for n in dir(arm_status_store) if n.startswith("SQL_")
              and getattr(arm_status_store, n).split()[0] in WRITE_VERBS]
    assert writes == ["SQL_MD_RECORD_SAVE", "SQL_STATUS_UPSERT"]
    for name in writes:
        assert "INTO arm_status " in getattr(arm_status_store, name)
    # neither write names the other's columns, so the 5 s status upsert never overwrites the md record
    assert "md_" not in arm_status_store.SQL_STATUS_UPSERT
    set_list = arm_status_store.SQL_MD_RECORD_SAVE.split("DO UPDATE SET", 1)[1].split(" WHERE ", 1)[0]
    assert set(re.findall(r"(\w+) = EXCLUDED", set_list)) == {"md_sentinel", "md_last_write_ms"}


def test_the_arm_versions_module_only_reads() -> None:
    for name in dir(arm_versions):
        sql = getattr(arm_versions, name)
        if name.startswith("SQL_") and isinstance(sql, str):
            assert sql.lstrip("(").startswith("SELECT"), name


# ---- §B3: nothing outside src/ops writes arm_controls / arm_accepted or sets cleared_at_ms ---------------------------
def _assign_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
        return node.targets[0].id
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return node.target.id
    return None


def _text(node: ast.AST, consts: dict[str, str], *, strict: bool) -> str | None:
    """The string `node` builds: constants, f-strings (each field resolved when it names a module-level string
    constant) and `+` chains. `strict`: None when any part is unknown; otherwise `UNRESOLVED` stands in for it."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else (None if strict else UNRESOLVED)
    if isinstance(node, ast.Name):
        got = consts.get(node.id)
        return got if got is not None or strict else UNRESOLVED
    if isinstance(node, ast.FormattedValue):
        return _text(node.value, consts, strict=strict)
    if isinstance(node, ast.JoinedStr):
        parts = [_text(v, consts, strict=strict) for v in node.values]
        return None if any(p is None for p in parts) else "".join(p for p in parts if p is not None)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _text(node.left, consts, strict=strict), _text(node.right, consts, strict=strict)
        return None if left is None or right is None else left + right
    return None if strict else UNRESOLVED


def _module_strings(tree: ast.Module) -> dict[str, str]:
    """Module-level names bound to a string built only from constants and earlier such names."""
    consts: dict[str, str] = {}
    for stmt in tree.body:
        name = _assign_name(stmt)
        value = getattr(stmt, "value", None)
        if name is not None and value is not None:
            text = _text(value, consts, strict=True)
            if text is not None:
                consts[name] = text
    return consts


def _has_string(node: ast.AST) -> bool:
    return any(isinstance(n, ast.JoinedStr) or (isinstance(n, ast.Constant) and isinstance(n.value, str))
               for n in ast.walk(node))


def _problems(text: str) -> list[str]:
    out: list[str] = []
    if OPERATOR_TABLE_WRITE.search(text):
        out.append("writes arm_controls / arm_accepted")
    if SETS_CLEARED.search(text):
        out.append("sets cleared_at_ms")
    if WRITES_FAILURES.search(text) and OWNER_ONLY_COLUMN.search(OWNER_ONLY_NULL_READ.sub(" ", text)):
        out.append("writes arm_failures naming cleared_at_ms / clear_request_id")
    return out


def sql_write_violations(src_root: Path) -> list[str]:
    """Every string in `src_root/**/*.py` (outside `ops/`) that writes `arm_controls` / `arm_accepted`, sets
    `cleared_at_ms` / `clear_request_id`, or writes `arm_failures` naming either owner-only column anywhere but an
    `IS NULL` read, except `CLEAR_EXCEPTION`. Each string constant, f-string and `+` chain is checked whole: implicit
    concatenation is joined by the parser, `+` operands are joined here, and f-string fields and `+` operands that
    name a module-level string constant are replaced by its text. Identifiers may be double-quoted."""
    found: list[str] = []
    for path in sorted(src_root.rglob("*.py")):
        rel = path.relative_to(src_root.parent).as_posix()
        if rel.startswith("src/ops/"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        consts = _module_strings(tree)
        allowed: set[int] = set()
        for node in ast.walk(tree):
            if (rel, _assign_name(node)) == CLEAR_EXCEPTION:
                allowed |= {id(n) for n in ast.walk(node)}
        for node in ast.walk(tree):
            if id(node) in allowed:
                continue
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                text: str | None = node.value
            elif isinstance(node, ast.JoinedStr) or (
                    isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add) and _has_string(node)):
                text = _text(node, consts, strict=False)
            else:
                continue
            for problem in _problems(text or ""):
                found.append(f"{rel}:{node.lineno}: {problem}")
    return sorted(set(found))


def test_no_sql_outside_ops_writes_operator_rows_or_clears_failed() -> None:
    assert sql_write_violations(SRC) == []


def test_the_clear_exception_exists_and_sets_cleared_at_ms() -> None:
    """The exception names a real statement that the scan would otherwise report (so the allow-list is not stale)."""
    assert SETS_CLEARED.search(arm_failures_store.SQL_FAILURE_CLEAR)
    assert _problems(arm_failures_store.SQL_FAILURE_CLEAR)


def test_open_episode_filters_are_not_reported() -> None:
    for sql in (arm_failures_store.SQL_FAILURE_REPING, arm_failures_store.SQL_FAILURE_PROBE,
                arm_failures_store.SQL_FAILURES_OPEN, arm_failures_store.SQL_FAILURE_INSERT):
        assert _problems(sql) == [], sql


@pytest.mark.parametrize("planted", [
    'SQL_X = "UPDATE arm_controls SET paused = true WHERE id = 1"\n',
    'SQL_X = (\n    "INSERT INTO arm_accepted (doc_hash) "\n    "VALUES ($1)"\n)\n',
    'SQL_X = "INSERT INTO public.arm_accepted (doc_hash) VALUES ($1)"\n',
    'SQL_X = f"UPDATE arm_failures SET cleared_at_ms = $2 WHERE {1}"\n',
    'SQL_FAILURE_CLEAR = "UPDATE arm_failures SET cleared_at_ms = $2"\n',          # the name, but not the file
    # an INSERT of an episode already cleared (the column list names the owner-only columns)
    'SQL_X = "INSERT INTO arm_failures (failure_id, cleared_at_ms) VALUES ($1, $2)"\n',
    'SQL_X = "INSERT INTO arm_failures (failure_id, clear_request_id) VALUES ($1, $2)"\n',
    # a row SET (no `cleared_at_ms =` text)
    'SQL_X = "UPDATE arm_failures SET (cleared_at_ms, clear_request_id) = ($2, $3)"\n',
    # `+` concatenation
    'SQL_X = "UPDATE " + "arm_controls SET paused = true"\n',
    'def f(w: str) -> str:\n    return "UPDATE " + "arm_failures SET last_ping_at_ms = 1, " + w + "cleared_at_ms = 2"\n',
    # a module-level name inside an f-string, and in a `+` chain
    'T = "arm_controls"\nSQL_X = f"UPDATE {T} SET paused = true"\n',
    'T = "arm_" + "controls"\nSQL_X = "UPDATE " + T + " SET paused = true"\n',
    # double-quoted identifiers
    'SQL_X = \'UPDATE "arm_controls" SET paused = true\'\n',
    'SQL_X = \'UPDATE "public"."arm_failures" SET "cleared_at_ms" = 1\'\n',
    'SQL_X = \'MERGE INTO "arm_failures" f USING x ON true WHEN NOT MATCHED THEN INSERT (clear_request_id) '
    'VALUES (1)\'\n',
])
def test_planted_violations_are_found(tmp_path: Path, planted: str) -> None:
    pkg = tmp_path / "src" / "riskmgr"
    pkg.mkdir(parents=True)
    (pkg / "bad.py").write_text(planted, encoding="utf-8")
    (tmp_path / "src" / "ops").mkdir()
    (tmp_path / "src" / "ops" / "fine.py").write_text(planted, encoding="utf-8")   # src/ops may
    found = sql_write_violations(tmp_path / "src")
    assert found and all(f.startswith("src/riskmgr/bad.py") for f in found)


@pytest.mark.parametrize("fine", [
    'SQL_X = "UPDATE arm_failures SET ping_count = ping_count + 1 WHERE failure_id = $2 AND cleared_at_ms IS NULL"\n',
    'SQL_X = "SELECT cleared_at_ms, clear_request_id FROM arm_failures"\n',
    'SQL_X = "UPDATE arm_positions SET state = $2"\n',
])
def test_reads_and_other_writes_are_not_reported(tmp_path: Path, fine: str) -> None:
    pkg = tmp_path / "src" / "riskmgr"
    pkg.mkdir(parents=True)
    (pkg / "ok.py").write_text(fine, encoding="utf-8")
    assert sql_write_violations(tmp_path / "src") == []


# ---- arm_ports: the one src.db module the scoped packages import --------------------------------------------------
def test_arm_ports_imports_only_the_standard_library_and_src_dto() -> None:
    tree = ast.parse(Path(arm_ports.__file__).read_text(encoding="utf-8"))
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names.append(node.module or "")
    for name in names:
        top = name.split(".")[0]
        assert name == "src.dto" or name.startswith("src.dto.") or (
            top in sys.stdlib_module_names and top not in ("asyncpg",)), name
    assert not any(n.startswith(("src.riskmgr", "src.marketdata", "src.armconfig", "src.db")) for n in names)
