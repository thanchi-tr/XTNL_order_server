"""Static checks of the live view page files (docs/design/live-view.md v9 §6 "page:").

The page is plain HTML / CSS / JavaScript served as bytes by src/liveview/server.py, so these checks read the five
files in src/liveview/static/ as text. JavaScript structure (handler bodies, try blocks, enclosing functions) is
found with a small lexer that blanks comments and string contents, so a word inside a comment or a string never
counts as code and a brace inside a string never unbalances a block.
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "src" / "liveview" / "static"
OVERSEE = ROOT / "src" / "oversee"
DLQ_ROWS = ROOT / "src" / "pubsub" / "dlq_rows.py"
HOOK = ROOT / ".claude" / "hooks" / "pre_tool_guard.py"

PAGE_FILES = ("index.html", "app.js", "edge.html", "edge.js")
FORBIDDEN_NAMES = (
    "place_order", "modify_order", "cancel_order", "submit_order", "close_position", "amend_order", "update_order",
    "newtradeorder", "newstoplimitorder", "updatetradeorder", "updatestoplimitorder", "ProtoOANewOrderReq",
    "ProtoOAAmendOrderReq", "ProtoOAAmendPositionSLTPReq", "ProtoOAClosePositionReq", "ProtoOACancelOrderReq",
)
THEME_TOKENS = ("--canvas", "--base", "--sub", "--card", "--raised", "--lift", "--ink-0", "--ink-1", "--ink-2",
                "--ink-3", "--green", "--red", "--amber", "--blue", "--line", "--line-hi", "--line-act")


def read(name: str) -> str:
    return (STATIC / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------- JavaScript lexing helpers

def mask_js(src: str) -> str:
    """Same length as src: comments become spaces and string / template contents become 'x' (quotes and newlines
    kept). The page scripts keep regex literals free of quotes, braces and `//`, so `/` needs no regex handling."""
    out = list(src)
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c == "/" and i + 1 < n and src[i + 1] == "/":
            while i < n and src[i] != "\n":
                out[i] = " "
                i += 1
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "*":
            end = src.find("*/", i + 2)
            end = n if end < 0 else end + 2
            for j in range(i, end):
                if src[j] != "\n":
                    out[j] = " "
            i = end
            continue
        if c in "'\"`":
            j = i + 1
            while j < n and src[j] != c:
                if src[j] == "\\":
                    out[j] = "x"
                    j += 1
                if j < n and src[j] != "\n":
                    out[j] = "x"
                j += 1
            i = j + 1
            continue
        i += 1
    return "".join(out)


def match_brace(masked: str, open_idx: int) -> int:
    assert masked[open_idx] == "{"
    depth = 0
    for j in range(open_idx, len(masked)):
        if masked[j] == "{":
            depth += 1
        elif masked[j] == "}":
            depth -= 1
            if depth == 0:
                return j
    raise AssertionError("unbalanced braces")


def bodies(src: str, head_regex: str) -> list[tuple[int, int]]:
    """(open, close) brace positions of every block whose head (ending at its `{`) matches head_regex."""
    masked = mask_js(src)
    out = []
    for m in re.finditer(head_regex, src):
        open_idx = m.end() - 1
        assert src[open_idx] == "{", head_regex
        if masked[open_idx] != "{":
            continue                      # the match was inside a comment or string
        out.append((open_idx, match_brace(masked, open_idx)))
    return out


def named_functions(src: str) -> list[tuple[str, int, int]]:
    masked = mask_js(src)
    out = []
    for m in re.finditer(r"\bfunction\s+([A-Za-z_$][\w$]*)\s*\([^)]*\)\s*\{", masked):
        open_idx = m.end() - 1
        out.append((m.group(1), open_idx, match_brace(masked, open_idx)))
    return out


def innermost_named(src: str, pos: int) -> tuple[str, int, int] | None:
    inside = [f for f in named_functions(src) if f[1] < pos < f[2]]
    return min(inside, key=lambda f: f[2] - f[1]) if inside else None


def try_depth_violations(src: str, word: str) -> list[int]:
    """Line numbers where `word` (as code) is not inside a `try { ... }` block: a brace-depth scan that marks each
    `{` opened right after the keyword `try`."""
    masked = mask_js(src)
    stack: list[bool] = []
    bad = []
    i = 0
    while i < len(masked):
        ch = masked[i]
        if ch == "{":
            stack.append(re.search(r"\btry\s*$", masked[:i]) is not None)
        elif ch == "}":
            if stack:
                stack.pop()
        elif masked.startswith(word, i) and not (i and (masked[i - 1].isalnum() or masked[i - 1] in "_$")):
            if not any(stack):
                bad.append(masked.count("\n", 0, i) + 1)
            i += len(word)
            continue
        i += 1
    return bad


# ---------------------------------------------------------------- CSS helpers

def css_rules(css: str) -> list[tuple[str, str, dict[str, str]]]:
    """(media condition or '', selector, declarations) for every rule, one level of @media nesting."""
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    rules: list[tuple[str, str, dict[str, str]]] = []

    def walk(text: str, media: str) -> None:
        i = 0
        while True:
            o = text.find("{", i)
            if o < 0:
                return
            depth, j = 0, o
            while j < len(text):
                if text[j] == "{":
                    depth += 1
                elif text[j] == "}":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            head = text[i:o].strip()
            body = text[o + 1:j]
            if head.startswith("@"):
                walk(body, head)
            else:
                decls = {}
                for part in body.split(";"):
                    if ":" in part:
                        k, v = part.split(":", 1)
                        decls[k.strip()] = v.strip()
                rules.append((media, head, decls))
            i = j + 1

    walk(css, "")
    return rules


def rule_for(rules: list[tuple[str, str, dict[str, str]]], selector: str, media: str = "") -> dict[str, str]:
    for m, sel, decls in rules:
        if m == media and selector in [s.strip() for s in sel.split(",")]:
            return decls
    raise AssertionError(f"no rule {selector!r} in {media or 'top level'}")


def root_vars(rules: list[tuple[str, str, dict[str, str]]]) -> dict[str, str]:
    return {k: v for k, v in rule_for(rules, ":root").items() if k.startswith("--")}


def family(value: str, tokens: dict[str, str]) -> str:
    m = re.fullmatch(r"var\((--[\w-]+)\)", value)
    return tokens.get(m.group(1), "") if m else value


def px(value: str) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)px", value)
    assert m, f"font-size {value!r} is not in px"
    return float(m.group(1))


# ---------------------------------------------------------------- Oversee reason codes

def _constants(paths: list[Path]) -> dict[str, str]:
    """Module-level string constants (`NAME = "x"`, `NAME: Final = "x"`), to resolve `Unknown(CONST, ...)`."""
    out = {}
    for p in paths:
        for m in re.finditer(r'^([A-Z][A-Z0-9_]*)\s*(?::\s*[\w\[\], ]+)?=\s*"([^"]+)"', p.read_text(encoding="utf-8"),
                             re.M):
            out[m.group(1)] = m.group(2)
    return out


def oversee_codes() -> tuple[set[str], set[str]]:
    """(codes, composite codes) the Oversee rules can store as a breach / halt `reason` or `rule_id`.

    Each regex names one way a code is written in src/oversee, so a new rule or reason written the same way is
    picked up without editing this test:
      1. `rule_id: str = "x"` / `= CONST`               a rule's id (rules.py dataclass defaults)
      2. `Result(Level.X, "x"` / `Result(Level.X, CONST` a rule result's reason
      3. `Unknown("x"` / `RiskUnknown("x"` / CONST       an UNKNOWN reason (rules.py, risk.py, dayroll.py)
      4. `band_result(..., "x", ...)`                    the reason a band rule passes to band_result
      5. `reason = "x"` / `reason="x"`                    a reason assigned (daily loss) or passed by keyword
                                                         (sweep's evaluation_failed, the cool-down clear)
      6. `LevelDecision(..., "trip"|"clear", "x"` and `... else "x"`  halt-level reasons (escalation, latch)
      7. `out.append("x")` in rules.py                   daily-loss flag and cap reasons
      8. `f"daily_loss({','.join(caps)})"`              a composite, expanded once per cap reason that
                                                         `_cap_reasons` appends
    """
    files = sorted(OVERSEE.glob("*.py"))
    consts = _constants(files)

    def val(lit: str | None, name: str | None) -> str | None:
        if lit:
            return lit
        return consts.get(name or "")

    codes: set[str] = set()
    composites: set[str] = set()
    for p in files:
        t = p.read_text(encoding="utf-8")
        for m in re.finditer(r'\brule_id:\s*str\s*=\s*(?:"([a-z_]+)"|([A-Z][A-Z0-9_]+))', t):
            codes.add(val(m.group(1), m.group(2)) or "")
        for m in re.finditer(r'\bResult\(\s*Level\.\w+\s*,\s*(?:"([a-z_]+)"|([A-Z][A-Z0-9_]+))', t):
            codes.add(val(m.group(1), m.group(2)) or "")
        for m in re.finditer(r'\b(?:Risk)?Unknown\(\s*(?:"([a-z_]+)"|([A-Z][A-Z0-9_]+))', t):
            codes.add(val(m.group(1), m.group(2)) or "")
        for m in re.finditer(r'(?<!def )\bband_result\([^"]*?"([a-z_]+)"', t):
            codes.add(m.group(1))
        for m in re.finditer(r'\breason\s*=\s*"([a-z_]+)"', t):
            codes.add(m.group(1))
        for m in re.finditer(r'\bLevelDecision\([^()]*?"(?:trip|clear)",\s*(?:[\w.]+\s+if\s+\w+\s+else\s+)?"([a-z_]+)"', t):
            codes.add(m.group(1))
        if p.name == "rules.py":
            for m in re.finditer(r'\bout\.append\("([a-z_]+)"\)', t):
                codes.add(m.group(1))
            cap_fn = re.search(r"def _cap_reasons\(.*?(?=\n    def |\nclass |\Z)", t, re.S)
            caps = re.findall(r'out\.append\("([a-z_]+)"\)', cap_fn.group(0)) if cap_fn else []
            for m in re.finditer(r'f"([a-z_]+)\(\{\'\s*,\s*\'\.join\(caps\)\}\)"', t):
                composites.update(f"{m.group(1)}({c})" for c in caps)
    codes.discard("")
    return codes, composites


def glossary_keys() -> set[str]:
    src = read("app.js")
    found = bodies(src, r"\bvar\s+GLOSSARY\s*=\s*\{")
    assert len(found) == 1, "app.js must define exactly one `var GLOSSARY = {...}`"
    o, c = found[0]
    return set(re.findall(r"^\s*'((?:[^'\\]|\\.)+)'\s*:", src[o + 1:c], re.M))


# ================================================================ tests

def test_all_static_files_exist() -> None:
    for name in ("index.html", "app.js", "app.css", "edge.html", "edge.js"):
        assert (STATIC / name).is_file(), name


@pytest.mark.parametrize("name", ["app.js", "edge.js"])
def test_no_html_injection_or_eval(name: str) -> None:
    masked = mask_js(read(name))
    for word in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"):
        assert word not in read(name), f"{name} contains {word}"
    assert not re.search(r"\beval\s*\(", masked), f"{name} calls eval"
    assert not re.search(r"\bnew\s+Function\b", masked), f"{name} uses new Function"
    assert not re.search(r"\.setAttribute\(\s*'style'", read(name)), f"{name} writes a style attribute"


def test_app_js_closes_the_stream_on_resync_and_on_every_error() -> None:
    src = read("app.js")
    masked = mask_js(src)
    resync = bodies(src, r"\.addEventListener\(\s*'resync'\s*,\s*function\s*\([^)]*\)\s*\{")
    onerror = bodies(src, r"\.onerror\s*=\s*function\s*\([^)]*\)\s*\{")
    assert resync, "no resync handler"
    assert onerror, "no onerror handler"
    for o, c in resync + onerror:
        body = masked[o + 1:c]
        assert re.search(r"\b\w+\.close\(\)", body), "handler does not close the EventSource"
        # close() comes first, before any early return, so it runs whatever the readyState
        assert body.find(".close()") < (body.find("return") if "return" in body else len(body))


def test_app_js_opens_streams_only_after_a_snapshot_fetch() -> None:
    src = read("app.js")
    masked = mask_js(src)
    opens = [m.start() for m in re.finditer(r"\bnew\s+EventSource\s*\(", masked)]
    assert len(opens) == 1, "exactly one place may open an EventSource"
    assert re.search(r"new\s+EventSource\(\s*url\s*\)", src)
    assert "'/api/stream?instance=' + encodeURIComponent(snap.instance) + '&after='" in src
    assert "if (isNum(snap.last_n)) return snap.last_n;" in src, "after= must come from snapshot.last_n"
    assert "(Array.isArray(snap.merges) ? snap.merges : []).forEach(addMerge);" in src
    opener = innermost_named(src, opens[0])
    assert opener is not None, "the EventSource must be opened inside a named function"
    name = opener[0]
    calls = [m.start() for m in re.finditer(rf"\b{re.escape(name)}\s*\(", masked)
             if not re.search(r"function\s+$", masked[:m.start()])]
    assert calls, f"{name} is never called"
    for pos in calls:
        caller = innermost_named(src, pos)
        assert caller is not None, f"{name} called outside a named function"
        body = src[caller[1]:pos]
        assert "fetch('/api/snapshot'" in body, f"{name} is called from {caller[0]} without a snapshot fetch before it"
        # the call is inside a promise callback of that fetch
        assert body.rfind(".then(") > body.find("fetch('/api/snapshot'")


def test_app_js_backs_off_and_flags_slow_snapshots() -> None:
    src = read("app.js")
    assert "backoff = Math.min(backoff * 2, 30 * SEC)" in src
    assert "if (openedAt && Date.now() - openedAt >= MIN) backoff = SEC;" in src
    assert "too many live-view tabs open in this browser?" in src
    assert re.search(r"setTimeout\(function \(\) \{ UI\.slow = true; renderSlow\(\); \}, 5 \* SEC\)", src)


@pytest.mark.parametrize("name", ["index.html", "edge.html"])
def test_no_inline_script_or_style(name: str) -> None:
    html = read(name)
    low = html.lower()
    assert "<style" not in low
    assert not re.search(r"\sstyle\s*=", low), "style attribute"
    assert not re.search(r"\son[a-z]+\s*=", low), "event-handler attribute"
    assert "javascript:" not in low
    scripts = re.findall(r"<script\b([^>]*)>(.*?)</script>", html, re.S | re.I)
    assert scripts, "no script tag"
    for attrs, content in scripts:
        assert re.search(r'\bsrc="/static/(app|edge)\.js"', attrs), attrs
        assert content.strip() == "", "inline script"
    assert '<link rel="stylesheet" href="/static/app.css">' in html


def test_script_set_per_page() -> None:
    assert re.search(r'<script src="/static/app\.js" defer></script>', read("index.html"))
    assert "edge.js" not in read("index.html")
    assert re.search(r'<script src="/static/edge\.js" defer></script>', read("edge.html"))
    assert "app.js" not in read("edge.html")


def test_glossary_covers_every_oversee_reason_code() -> None:
    codes, composites = oversee_codes()
    # the parse must find the known codes, so a broken regex cannot pass vacuously
    for known in ("missing_stop_loss", "max_position_risk", "max_aggregate_open_risk", "max_daily_loss",
                  "margin_usage", "risk_unknown", "stale_account_image", "stale_mark", "evaluation_failed"):
        assert known in codes, f"parser lost {known}"
    assert "daily_loss(partial_session)" in composites
    keys = glossary_keys()
    missing = sorted((codes | composites) - keys)
    assert not missing, f"glossary misses Oversee codes: {missing}"


def test_glossary_covers_dlq_codes_and_listed_codes() -> None:
    keys = glossary_keys()
    dlq = set(_constants([DLQ_ROWS]).values())
    assert {"QueueFull", "schema_invalid", "checksum", "future_timestamp", "illegal_transition"} <= dlq
    assert not sorted(dlq - keys), f"glossary misses DLQ codes: {sorted(dlq - keys)}"
    listed = {"missing_stop_loss", "max_position_risk", "max_aggregate_open_risk", "max_daily_loss", "margin_usage",
              "risk_unknown", "stale_account_image", "stale_mark", "daily_loss(partial_session)", "partial_session",
              "illegal_transition", "schema_invalid", "checksum", "future_timestamp", "QueueFull"}
    assert not sorted(listed - keys)


def test_unknown_code_shows_as_the_code() -> None:
    src = read("app.js")
    found = bodies(src, r"\bfunction\s+gloss\s*\([^)]*\)\s*\{")
    assert len(found) == 1
    o, c = found[0]
    assert re.search(r"return code;\s*$", src[o + 1:c].strip() + "\n", re.M) or src[o + 1:c].rstrip().endswith("return code;")


def test_css_theme_tokens_light_variant_and_no_web_font() -> None:
    css = read("app.css")
    rules = css_rules(css)
    tokens = root_vars(rules)
    for t in THEME_TOKENS:
        assert t in tokens, f":root lacks {t}"
    light = rule_for(rules, ':root[data-theme="light"]')
    for t in ("--canvas", "--base", "--card", "--ink-0", "--ink-1", "--ink-2", "--green", "--red", "--amber",
              "--blue", "--line"):
        assert t in light, f"light variant lacks {t}"
    assert tokens["--base"] != light["--base"]
    low = re.sub(r"/\*.*?\*/", "", css, flags=re.S).lower()
    assert "@font-face" not in low
    assert "@import" not in low
    for m in re.finditer(r"url\(([^)]*)\)", low):
        assert m.group(1).strip().startswith("#"), f"url({m.group(1)}) loads a resource"
    assert ".woff" not in low and ".ttf" not in low and ".otf" not in low


@pytest.mark.parametrize("name", ["app.js", "edge.js"])
def test_every_local_storage_access_is_inside_try(name: str) -> None:
    src = read(name)
    assert "localStorage" in mask_js(src), f"{name} should read the stored theme"
    assert try_depth_violations(src, "localStorage") == [], f"{name}: localStorage outside try"
    assert "'lv-theme'" in src


def test_try_scan_finds_an_unprotected_access() -> None:
    bad = "function f() {\n  return window.localStorage.getItem('x');\n}\n"
    good = "function f() {\n  try {\n    return window.localStorage.getItem('x');\n  } catch (e) { return null; }\n}\n"
    assert try_depth_violations(bad, "localStorage") == [2]
    assert try_depth_violations(good, "localStorage") == []
    assert try_depth_violations("// localStorage in a comment\nvar s = 'localStorage';\n", "localStorage") == []


EXPECT = {
    ".t1": ("Inter", (26, 26), "700", "var(--ink-0)"),   # the site's page heading
    ".t2": ("JetBrains Mono", (11, 11), "700", "var(--ink-2)"),
    ".t2h": ("Inter", (16, 16), "700", "var(--ink-0)"),
    ".t3": ("Inter", (14, 14), "600", "var(--ink-0)"),   # the site's card title, raised to 14px
    ".t4": ("Inter", (13, 14), "400", "var(--ink-1)"),
    ".t5": ("JetBrains Mono", (11, 12), "400", "var(--ink-2)"),
    ".hero": ("JetBrains Mono", (22, 26), "700", None),
}


def test_text_hierarchy_classes() -> None:
    rules = css_rules(read("app.css"))
    tokens = root_vars(rules)
    for sel, (face, (lo, hi), weight, colour) in EXPECT.items():
        d = rule_for(rules, sel)
        fam = family(d.get("font-family", ""), tokens)
        assert fam.startswith(f'"{face}"'), f"{sel} font-family {fam!r}"
        assert lo <= px(d["font-size"]) <= hi, f"{sel} font-size {d['font-size']}"
        assert d.get("font-weight") == weight, f"{sel} font-weight"
        if colour:
            assert d.get("color") == colour, f"{sel} color"
    assert rule_for(rules, ".t1")["letter-spacing"] == "-0.03em"
    t2 = rule_for(rules, ".t2")
    assert t2["text-transform"] == "uppercase"
    assert t2["letter-spacing"] in (".14em", "0.14em")
    assert rule_for(rules, ".t4")["line-height"] == "1.5"
    assert px(rule_for(rules, ".t1", "@media (max-width: 640px)")["font-size"]) == 22


def test_font_sizes_are_px_and_at_least_11() -> None:
    rules = css_rules(read("app.css"))
    seen = 0
    for media, sel, decls in rules:
        for k, v in decls.items():
            if k == "font-size":
                if v == "inherit":
                    continue
                assert px(v) >= 11, f"{sel} font-size {v} is below 11px"
                seen += 1
            if k == "font":
                assert v == "inherit" or not re.search(r"\d(?:em|rem|%|pt|vw|vh|ex|ch)\b", v), f"{sel} font {v}"
                for size in re.findall(r"(\d+(?:\.\d+)?)px", v):
                    assert float(size) >= 11, f"{sel} font {v}"
    assert seen >= 7


@pytest.mark.parametrize("name, current", [("index.html", "/"), ("edge.html", "/edge")])
def test_nav_links_are_real_routes(name: str, current: str) -> None:
    html = read(name)
    assert 'href="/"' in html and 'href="/edge"' in html
    assert "#/" not in html
    assert re.search(rf'<a href="{re.escape(current)}" aria-current="page">', html)
    assert len(re.findall(r'aria-current="page"', html)) == 1


@pytest.mark.parametrize("name", ["app.js", "edge.js"])
def test_scripts_have_no_hash_routes(name: str) -> None:
    assert "#/" not in read(name)


def test_edge_html_has_no_form_controls_and_inert_buttons() -> None:
    html = read("edge.html")
    low = html.lower()
    for tag in ("<form", "<input", "<select", "<textarea"):
        assert tag not in low, tag
    for m in re.finditer(r"<button\b[^>]*>", low):
        assert re.search(r"\sdisabled(\s|>|=)", m.group(0)), m.group(0)
        assert 'aria-disabled="true"' in m.group(0), m.group(0)
    assert '<h1 class="t1" id="edgeTitle">Edge &amp; automated trading</h1>' in html
    assert "Phase 2 shell · not connected · nothing on this page can send an order" in html
    for section in ("Edges", "Signals", "Execution intents", "Risk budget", "Automation controls"):
        assert f">{section}</div>" in html, section
    assert html.count("locked · Phase 2") >= 1
    assert ">DENY<" in html and ">not connected<" in html


def test_edge_js_reads_only_the_snapshot() -> None:
    src = read("edge.js")
    assert src.count("fetch(") == 1
    assert len(re.findall(r"\bfetch\('/api/snapshot'\)", src)) == 1, "fetch must take only the literal '/api/snapshot'"
    for word in ("EventSource", "WebSocket", "XMLHttpRequest", "sendBeacon", "innerHTML", "outerHTML",
                 "insertAdjacentHTML", "document.write", "onclick", "method", "body:"):
        assert word not in src, word
    assert not re.search(r"addEventListener\(\s*['\"]click", src)
    assert "allow" not in src.lower(), "edge.js must not contain the string ALLOW"
    # the gate shows one fixed value, set from a constant equal to DENY
    assert re.search(r"^\s*var GATE = 'DENY';$", src, re.M)
    sets = re.findall(r"byId\('gateValue'\)\.textContent\s*=\s*([^;]+);", src)
    assert sets == ["GATE"]
    assert not re.search(r"\bGATE\s*=(?!=)", src.replace("var GATE = 'DENY';", ""))
    for reason in ("Phase 1: no order sender exists", "'halt ' + lv", "database unreachable", "status not current"):
        assert reason in src, reason
    assert "STALE_MS = 10000" in src and "PERIOD_MS = 5000" in src and "MAX_BACKOFF_MS = 30000" in src


def _load_hook() -> ModuleType:
    spec = importlib.util.spec_from_file_location("pre_tool_guard_for_liveview_test", HOOK)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_forbidden_hook_names_absent_from_static_files() -> None:
    for name in PAGE_FILES + ("app.css",):
        low = read(name).lower()
        for word in FORBIDDEN_NAMES:
            assert word.lower() not in low, f"{name} contains {word}"


def test_forbidden_names_match_the_hook() -> None:
    """Parity: each name listed here is blocked by some FORBIDDEN_PATTERNS regex of the hook (as a `def`, a
    `/endpoint` path or a bare message name), so this list cannot fall behind the hook."""
    patterns = [p for p, _msg in _load_hook().FORBIDDEN_PATTERNS]
    for word in FORBIDDEN_NAMES:
        probes = (f"def {word}(", f"/{word}", word)
        assert any(re.search(p, probe) for p in patterns for probe in probes), f"{word} not in FORBIDDEN_PATTERNS"


def test_hook_patterns_find_nothing_in_liveview() -> None:
    patterns = [p for p, _msg in _load_hook().FORBIDDEN_PATTERNS]
    files = sorted((ROOT / "src" / "liveview").glob("*.py")) + [STATIC / n for n in PAGE_FILES]
    for f in files:
        text = f.read_text(encoding="utf-8")
        for p in patterns:
            assert not re.search(p, text), f"{f.name} matches hook pattern {p}"
