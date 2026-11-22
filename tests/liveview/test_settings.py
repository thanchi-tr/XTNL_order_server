"""Live view settings (docs/design/live-view.md s4.1, s6 "settings"): literal loopback only, bounded numbers, the
refused environment (names only, never a value), `--check-config`, and the module layout rules that keep the
deploy env-var scanner honest (no other module reads the environment or holds an env-looking string tuple)."""
from __future__ import annotations

import ast
import logging
import re
from pathlib import Path
from typing import Any

import pytest

from src.liveview import settings as S
from src.liveview.settings import LiveViewConfigError, LiveViewSettings, parse_loopback, refused_names

ROOT = Path(__file__).resolve().parents[2]
LIVEVIEW = ROOT / "src" / "liveview"
ENV_NAME = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$")     # tests/deploy/test_deploy_assets.py ENV_NAME
DSN = "postgresql://xtnl_liveview:pw@127.0.0.1:55433/xtnl"
SECRET = "s3cr3t-VALUE-9q7w"


def env(**extra: str) -> dict[str, str]:
    return {S.DSN_VAR: DSN, **extra}


# ---- addresses ---------------------------------------------------------------------------------------------------
@pytest.mark.parametrize(("raw", "want"), [
    ("127.0.0.1:8090", ("127.0.0.1", 8090)),
    ("127.0.0.2:1", ("127.0.0.2", 1)),
    ("127.255.255.254:65535", ("127.255.255.254", 65535)),
    ("[::1]:8090", ("::1", 8090)),
    ("  127.0.0.1:9464 ", ("127.0.0.1", 9464)),
])
def test_literal_loopback_addresses_are_accepted(raw: str, want: tuple[str, int]) -> None:
    assert parse_loopback(S.ADDR_VAR, raw) == want


@pytest.mark.parametrize("raw", [
    "localhost:8090", "LOCALHOST:8090", "myhost:8090", "0.0.0.0:8090", "10.0.0.1:8090", "192.168.1.5:8090",
    "128.0.0.1:8090", "[::]:8090", "[::2]:8090", "[fe80::1]:8090", "::1:8090", "127.0.0.1", "127.0.0.1:",
    "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:123456", "127.1:8090", "", "http://127.0.0.1:8090",
    "127.0.0.1:8090/x", "999.0.0.1:80",
])
def test_names_wildcards_and_other_addresses_are_refused(raw: str) -> None:
    with pytest.raises(LiveViewConfigError) as ei:
        parse_loopback(S.ADDR_VAR, raw)
    assert S.ADDR_VAR in str(ei.value)


def test_addr_refusal_through_from_env_names_the_variable() -> None:
    with pytest.raises(LiveViewConfigError, match=S.ADDR_VAR):
        LiveViewSettings.from_env(env(**{S.ADDR_VAR: "localhost:8090"}))
    with pytest.raises(LiveViewConfigError, match=S.METRICS_VAR):
        LiveViewSettings.from_env(env(**{S.METRICS_VAR: "0.0.0.0:9464"}))


def test_defaults() -> None:
    s = LiveViewSettings.from_env(env())
    assert (s.host, s.port) == ("127.0.0.1", 8090)
    assert (s.poll_ms, s.history, s.backfill_h, s.max_clients) == (1000, 2000, 24, 4)
    assert s.metrics == ("127.0.0.1", 9464)
    assert DSN not in s.describe() and "pw" not in s.describe()


@pytest.mark.parametrize("off", ["off", "OFF", " Off "])
def test_engine_metrics_off_gives_none(off: str) -> None:
    s = LiveViewSettings.from_env(env(**{S.METRICS_VAR: off}))
    assert s.metrics is None
    assert "engine_metrics=off" in s.describe()


def test_ipv6_listen_address() -> None:
    s = LiveViewSettings.from_env(env(**{S.ADDR_VAR: "[::1]:8091", S.METRICS_VAR: "[::1]:9999"}))
    assert (s.host, s.port, s.metrics) == ("::1", 8091, ("::1", 9999))


# ---- bounds ------------------------------------------------------------------------------------------------------
BOUNDS = [(S.POLL_VAR, "poll_ms", 250, 10000), (S.HISTORY_VAR, "history", 200, 20000),
          (S.BACKFILL_VAR, "backfill_h", 1, 168), (S.CLIENTS_VAR, "max_clients", 1, 16)]


@pytest.mark.parametrize(("var", "attr", "low", "high"), BOUNDS)
def test_each_numeric_setting_accepts_its_bounds(var: str, attr: str, low: int, high: int) -> None:
    for v in (low, high, (low + high) // 2):
        assert getattr(LiveViewSettings.from_env(env(**{var: str(v)})), attr) == v
    assert getattr(LiveViewSettings.from_env(env(**{var: f" {low} "})), attr) == low


@pytest.mark.parametrize(("var", "attr", "low", "high"), BOUNDS)
def test_each_numeric_setting_refuses_out_of_bounds_and_junk(var: str, attr: str, low: int, high: int) -> None:
    for bad in (str(low - 1), str(high + 1), "-5", "1e3", "12.5", "abc", "0x10", "9999999"):
        with pytest.raises(LiveViewConfigError) as ei:
            LiveViewSettings.from_env(env(**{var: bad}))
        assert var in str(ei.value)
    for junk in ("abc", "0x10", "1e3", "12.5", "7-7"):
        with pytest.raises(LiveViewConfigError) as ei:
            LiveViewSettings.from_env(env(**{var: junk}))
        assert junk not in str(ei.value)


@pytest.mark.parametrize(("var", "attr", "low", "high"), BOUNDS)
def test_an_empty_numeric_setting_takes_the_default(var: str, attr: str, low: int, high: int) -> None:
    default = getattr(LiveViewSettings.from_env(env()), attr)
    assert getattr(LiveViewSettings.from_env(env(**{var: ""})), attr) == default


# ---- DSN ---------------------------------------------------------------------------------------------------------
def test_missing_or_non_postgres_dsn_is_refused_without_the_value() -> None:
    with pytest.raises(LiveViewConfigError, match=S.DSN_VAR):
        LiveViewSettings.from_env({})
    with pytest.raises(LiveViewConfigError, match=S.DSN_VAR):
        LiveViewSettings.from_env({S.DSN_VAR: "   "})
    with pytest.raises(LiveViewConfigError) as ei:
        LiveViewSettings.from_env({S.DSN_VAR: f"mysql://u:{SECRET}@h/db"})
    assert S.DSN_VAR in str(ei.value) and SECRET not in str(ei.value)


def test_the_live_view_dsn_itself_is_not_refused() -> None:
    assert refused_names({S.DSN_VAR: DSN}) == []
    assert refused_names({S.DSN_VAR.lower(): DSN}) == []
    assert LiveViewSettings.from_env(env()).dsn == DSN


# ---- refused environment -----------------------------------------------------------------------------------------
REFUSED = sorted(S.REFUSED_NAMES) + [p + "ANYTHING" for p in S.REFUSED_PREFIXES] + [
    "STONE_X_USERNAME", "STONE_X_PASSWORD", "CTRADER_CLIENT_SECRET", "XTNL_MD_ENABLED", "XTNL_ARM_ENABLED",
    "XTNL_SMTP_HOST", "XTNL_SES_REGION", "XTNL_PUSH_TOPIC", "XTNL_API_ADDR",
    # the generic XTNL_*(PASS|SECRET|TOKEN|DSN|KEY) rule
    "XTNL_PASSWORD", "XTNL_DB_PASS", "XTNL_SOME_SECRET", "XTNL_NEW_TOKEN", "XTNL_OTHER_DSN", "XTNL_HMAC_KEY",
    "XTNL_KEYRING", "XTNL_X_TOKENS", "xtnl_lower_secret", "XTNL_LIVEVIEW_DSN_OLD", "XTNL_LIVEVIEW_KEY",
]


def test_the_refused_lists_are_the_design_lists() -> None:
    assert set(S.REFUSED_PREFIXES) == {"STONE_X_", "CTRADER_", "XTNL_MD_", "XTNL_ARM_", "XTNL_SMTP_", "XTNL_SES_",
                                       "XTNL_PUSH_", "XTNL_API_",
                                       # home-server plan W2: backup credentials of the separate backup account
                                       "XTNL_BACKUP_", "RCLONE_", "B2_"}
    assert S.REFUSED_NAMES == {"ORDER_SERVER_DSN", "REDIS_URL", "BROKER_ENV", "XTNL_DEADMAN_TOKEN",
                               "XTNL_HEARTBEAT_COPY_TOKEN", "XTNL_DASHBOARD_DSN", "XTNL_HEARTBEAT_COPY_URL"}
    assert all(p.endswith("_") for p in S.REFUSED_PREFIXES)
    assert S.REFUSED_PATTERN.pattern == r"^XTNL_\w*(PASS|SECRET|TOKEN|DSN|KEY)"


@pytest.mark.parametrize("name", REFUSED)
def test_every_refused_name_raises_naming_the_variable_never_its_value(name: str) -> None:
    assert refused_names(env(**{name: SECRET})) == [name]
    with pytest.raises(LiveViewConfigError) as ei:
        LiveViewSettings.from_env(env(**{name: SECRET}))
    msg = str(ei.value)
    assert name in msg
    assert SECRET not in msg and DSN not in msg


def test_several_refused_names_are_all_named_sorted() -> None:
    e = env(REDIS_URL=SECRET, STONE_X_PASSWORD=SECRET, XTNL_X_SECRET=SECRET)
    assert refused_names(e) == ["REDIS_URL", "STONE_X_PASSWORD", "XTNL_X_SECRET"]


@pytest.mark.parametrize("name", ["GITHUB_TOKEN", "NPM_TOKEN", "AWS_SECRET_ACCESS_KEY", "PATH", "XTNL_LIVEVIEW_ADDR",
                                  "XTNL_LIVEVIEW_POLL_MS", "XTNL_METRICS_ADDR", "XTNL_LOG_LEVEL", "MY_XTNL_TOKEN",
                                  "USERPROFILE", "ORDER_SERVER_TEST_DSN"])
def test_unrelated_names_are_not_refused(name: str) -> None:
    assert refused_names(env(**{name: SECRET})) == []


# ---- --check-config ----------------------------------------------------------------------------------------------
@pytest.fixture
def lv_main(monkeypatch: pytest.MonkeyPatch) -> Any:
    """src.liveview.__main__ with its root-logger reconfiguration disabled (it would remove pytest's handlers and
    leave a stdout handler behind for the following tests)."""
    import src.liveview.__main__ as M
    monkeypatch.setattr(M, "configure_logging", lambda stream=None: None)
    return M


def _clean_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    for name in list(os.environ):
        if refused_names({name: ""}) or name.upper().startswith("XTNL_LIVEVIEW_"):
            monkeypatch.delenv(name, raising=False)


def test_check_config_returns_0_with_a_good_environment(lv_main: Any, monkeypatch: pytest.MonkeyPatch,
                                                        caplog: pytest.LogCaptureFixture) -> None:
    _clean_environ(monkeypatch)
    monkeypatch.setenv(S.DSN_VAR, DSN)
    monkeypatch.setenv(S.METRICS_VAR, "off")
    monkeypatch.setenv("GITHUB_TOKEN", SECRET)
    with caplog.at_level(logging.INFO):
        assert lv_main.main(["--check-config"]) == 0
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "configuration ok" in text and DSN not in text and SECRET not in text


@pytest.mark.parametrize("name", ["REDIS_URL", "STONE_X_PASSWORD", "XTNL_NEW_TOKEN", "XTNL_DASHBOARD_DSN"])
def test_check_config_returns_78_with_a_refused_environment(lv_main: Any, monkeypatch: pytest.MonkeyPatch,
                                                            caplog: pytest.LogCaptureFixture, name: str) -> None:
    _clean_environ(monkeypatch)
    monkeypatch.setenv(S.DSN_VAR, DSN)
    monkeypatch.setenv(name, SECRET)
    with caplog.at_level(logging.INFO):
        assert lv_main.main(["--check-config"]) == 78
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert name in text and SECRET not in text and DSN not in text


def test_check_config_returns_78_without_a_dsn_or_with_a_bad_address(lv_main: Any,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_environ(monkeypatch)
    assert lv_main.main(["--check-config"]) == 78
    monkeypatch.setenv(S.DSN_VAR, DSN)
    monkeypatch.setenv(S.ADDR_VAR, "localhost:8090")
    assert lv_main.main(["--check-config"]) == 78


# ---- module layout (the deploy env-var scanner, tests/deploy/test_deploy_assets.py) ------------------------------
def _modules() -> list[Path]:
    return sorted(LIVEVIEW.glob("*.py"))


def test_the_liveview_package_has_its_modules() -> None:
    names = {p.name for p in _modules()}
    assert {"settings.py", "sql.py", "events.py", "probe.py", "server.py", "__main__.py"} <= names


@pytest.mark.parametrize("path", [p for p in sorted(LIVEVIEW.glob("*.py")) if p.name != "settings.py"],
                         ids=lambda p: p.name)
def test_no_other_module_reads_the_environment_or_defines_an_env_constant(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert "environ" not in text, path.name
    assert "getenv" not in text, path.name
    tree = ast.parse(text, filename=path.name)
    for node in tree.body:
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        for t in targets:
            assert not (isinstance(t, ast.Name) and t.id.endswith(("_VAR", "_ENV"))), f"{path.name}: {t.id}"


def _env_like_tuples(path: Path) -> list[tuple[int, list[str]]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.name)
    out: list[tuple[int, list[str]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Tuple):
            hits = [e.value for e in node.elts
                    if isinstance(e, ast.Constant) and isinstance(e.value, str) and ENV_NAME.match(e.value)]
            if hits:
                out.append((node.lineno, hits))
    return out


def _tuple_params() -> list[Any]:
    out: list[Any] = []
    for p in _modules():
        out.append(pytest.param(p, id=p.name))
    return out


@pytest.mark.parametrize("path", _tuple_params())
def test_no_module_holds_a_tuple_of_env_looking_strings(path: Path) -> None:
    assert _env_like_tuples(path) == [], path.name


def test_the_scanner_rule_would_catch_an_env_looking_tuple(tmp_path: Path) -> None:
    bad = tmp_path / "bad.py"
    bad.write_text('X = ("PARTIALLY_FILLED", "open")\nY = frozenset({"REDIS_URL"})\n', encoding="utf-8")
    assert _env_like_tuples(bad) == [(1, ["PARTIALLY_FILLED"])]


def test_the_deploy_scanner_sees_exactly_the_settings_variables() -> None:
    from tests.deploy.test_deploy_assets import env_vars_read_by_code
    found = env_vars_read_by_code(LIVEVIEW)
    assert set(found) == {S.DSN_VAR, S.ADDR_VAR, S.POLL_VAR, S.HISTORY_VAR, S.BACKFILL_VAR, S.CLIENTS_VAR,
                          S.METRICS_VAR}
    assert set(found.values()) == {"src/liveview/settings.py"}
