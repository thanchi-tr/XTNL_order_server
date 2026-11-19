"""Read-only lockout scanner (design §B3; plan S11).

Planted samples are written under `tmp_path` (never under `src/`). The last tests scan the real `LOCKOUT_SCOPE` tree,
so a name-ban or import violation in any later stage fails the pytest leg of the green gate (plan §1).
"""
from __future__ import annotations

import textwrap
import types
from pathlib import Path

import pytest

from src.riskmgr import lockout
from src.riskmgr.lockout import (
    LOCKOUT_SCOPE,
    NAME_BAN_FILES,
    PHASE1_SINKS,
    SCANNER_SELF,
    LockoutViolation,
    Rule,
    ScanIncomplete,
    ScanResult,
    SinkSetError,
    Violation,
    assert_sinks,
    banned_names_in_text,
    in_name_ban_scope,
    is_banned_name,
    normalise_name,
    require_clean,
    scan,
    scan_source,
)

REPO = Path(__file__).resolve().parents[2]

RISKMGR = "src/riskmgr/sample.py"
MARKETDATA = "src/marketdata/sample.py"
ARMCONFIG = "src/armconfig/sample.py"
STORE = "src/marketdata/store/sample.py"
MD_PORTS = "src/runtime/md_ports.py"


def _plant(root: Path, rel: str, source: str) -> ScanResult:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return scan([root], modules={})


def _check(rel: str, source: str) -> tuple[Violation, ...]:
    return scan_source(textwrap.dedent(source), rel)


def _refused(found: tuple[Violation, ...], rule: Rule, *needles: str) -> None:
    hits = [v for v in found if v.rule is rule and all(n in v.detail for n in needles)]
    assert hits, f"expected {rule.value} {needles}, got {[str(v) for v in found]}"


# ---------------------------------------------------------------------------------------------------- name ban
@pytest.mark.parametrize("source, name", [
    ("def _place_order():\n    pass\n", "_place_order"),
    ("class ModifyStop:\n    pass\n", "ModifyStop"),
    ("def trail_stops():\n    pass\n", "trail_stops"),
    ("async def send_trade(x):\n    pass\n", "send_trade"),
    ("class X:\n    def set_stop_loss(self):\n        pass\n", "set_stop_loss"),
    ("def __Amend__SLTP():\n    pass\n", "__Amend__SLTP"),
    ("class NewPositions:\n    pass\n", "NewPositions"),
    ("def risk_close_positions_now():\n    pass\n", "risk_close_positions_now"),
    ("def place_order2():\n    pass\n", "place_order2"),
    ("class X:\n    def modify_stop2(self):\n        pass\n", "modify_stop2"),
    ("def close_positions2():\n    pass\n", "close_positions2"),
])
def test_planted_sender_names_are_refused(tmp_path: Path, source: str, name: str) -> None:
    result = _plant(tmp_path, RISKMGR, source)
    _refused(result.violations, Rule.NAME_BAN, repr(name))
    assert not result.ok


@pytest.mark.parametrize("name, normalised", [
    ("_place_order", "place_order"),
    ("ModifyStop", "modify_stop"),
    ("StopHandleReadPortImpl", "stop_handle_read_port_impl"),
    ("HTTPServer", "http_server"),
    ("ModifySLTP", "modify_sltp"),
    ("__place__order_", "place_order_"),
    ("place_order2", "place_order_2"),
    ("ModifyStop2", "modify_stop_2"),
    ("Sha256Digest", "sha_256_digest"),
])
def test_normaliser(name: str, normalised: str) -> None:
    assert normalise_name(name) == normalised


# Every name this plan introduces in the scope and in src/runtime/md_ports.py (plan S11 fixture), so a later stage is
# never blocked by its own plan names.
PLAN_NAMES = (
    "StopHandleReadPortImpl", "StopBasis", "break_even_on_risk_close", "close_eligible", "held_positions",
    "arm_row_update", "mark_arm_closed", "pre_basis",
    # S12 market-data core
    "BarSpec", "Side", "Source", "SeriesId", "Bar", "Quality", "Quote", "Liveness", "MdGeneration", "BarEndEvent",
    "DataUnavailable", "Unsupported", "Transient", "CursorExpired", "decision_cap", "Keyring", "content_hash",
    "safe_redis_error", "MdSettings", "MarketDataPort", "BrokerBarsPort", "StopHandleReadPort", "BarsResult",
    "StopHandleRead", "StampPort", "CalendarPort", "AdapterHealthPort", "NodeRecordPort",
    # S13-S18
    "AofWaiter", "MdHook", "on_price", "events_since", "atr_exact", "Count", "Anchored", "register",
    # S19-S22, S26-S28
    "BarContext", "SlStrategy", "Evaluation", "Proposal", "Guard", "registry_tokens", "EnvelopeParams", "next_state",
    "Refused", "FirstBarGuard", "parse_params", "migrate_state", "propose", "at_target", "trigger_met", "Sink",
    "on_decisions", "Pager", "Clock", "PgHealthPort", "FailedSpoolPort",
    # S31 md_ports.py
    "BrokerBarsPortImpl", "CalendarPortImpl", "AdapterHealthPortImpl", "PgHealthPortImpl", "StampPortImpl",
    "PagerWrapper", "NodeRecordPortImpl", "FailedSpoolPortImpl", "log_mirror", "read_stop_handles",
    # this module
    "assert_sinks", "require_clean", "normalise_name", "scan_source",
)


@pytest.mark.parametrize("name", PLAN_NAMES)
def test_plan_names_pass_the_name_ban(name: str) -> None:
    assert not is_banned_name(name), name


def test_plan_names_pass_as_definitions(tmp_path: Path) -> None:
    body = "".join(f"def {n}() -> None:\n    pass\n" if n[0].islower() else f"class {n}:\n    pass\n"
                   for n in PLAN_NAMES)
    assert _plant(tmp_path, RISKMGR, body).ok
    assert _plant(tmp_path, MD_PORTS, body).ok


def test_text_form_used_by_the_hook() -> None:
    text = "x = 1\nasync def trail_stops(a):\n    pass\nclass Fine:\n    def held_positions(self): ...\n"
    assert banned_names_in_text(text) == ["trail_stops"]
    assert banned_names_in_text("def set_order(self): ...") == ["set_order"]  # the scope decides, not the text


def test_name_ban_scope() -> None:
    for p in ("C:\\repo\\src\\riskmgr\\x.py", "/repo/src/marketdata/store/k.py", "src/armconfig/__init__.py",
              "/repo/src/runtime/md_ports.py", SCANNER_SELF, "C:/r/SRC/RiskMgr/x.py",
              "C:\\R\\Src\\Runtime\\MD_Ports.py"):
        assert in_name_ban_scope(p), p
    for p in ("src/adapters/mock.py", "/repo/src/runtime/session.py", "src/riskmgrx/a.py", "tests/riskmgr/t.py"):
        assert not in_name_ban_scope(p), p


def test_names_outside_the_scope_are_not_scanned(tmp_path: Path) -> None:
    # MockBrokerAdapter.set_order / set_position and the StoneX REST open_positions stay legal outside the scope.
    result = _plant(tmp_path, "src/adapters/mock.py", "def set_order():\n    pass\ndef open_positions():\n    pass\n")
    assert result.ok and result.scanned == ()


def test_md_ports_gets_the_name_ban_only(tmp_path: Path) -> None:
    ok = _plant(tmp_path, MD_PORTS, "import os\nfrom src.pubsub.orchestrator import Orchestrator\nx = os._exit\n")
    assert ok.ok, ok.violations
    bad = _plant(tmp_path, MD_PORTS, "class StopPlacer:\n    def move_stop(self):\n        pass\n")
    _refused(bad.violations, Rule.NAME_BAN, "'move_stop'")


# ---------------------------------------------------------------------------------------------------- imports
@pytest.mark.parametrize("rel, source, needle", [
    (RISKMGR, "import socket\n", "network access"),
    (RISKMGR, "import src.adapters\n", "broker adapters"),
    (RISKMGR, "from src.adapters.base import BrokerAdapter\n", "broker adapters"),
    (RISKMGR, "import zlib\n", "zlib is allowed only in src/marketdata"),
    (RISKMGR, "import sys\n", "interpreter internals"),
    (MARKETDATA, "import sys\n", "interpreter internals"),
    (RISKMGR, "import os.path\n", "process and filesystem"),
    (RISKMGR, "import importlib\n", "dynamic import"),
    (RISKMGR, "from src.runtime.session import EngineSession\n", "runtime and orchestrator"),
    (RISKMGR, "import src.pubsub.orchestrator\n", "event bus"),
    (RISKMGR, "from src.cache import codec\n", "engine Redis"),
    (RISKMGR, "import src.notifications\n", "notification channels"),
    (RISKMGR, "from src.calendar import readiness\n", "readiness"),
    (RISKMGR, "from src.calendar.readiness import readiness_check\n", "readiness"),
    (RISKMGR, "import ssl\n", "TLS"),
    (RISKMGR, "import asyncpg\n", "database driver"),
    (RISKMGR, "import redis\n", "redis is allowed only in src/marketdata/store"),
    (MARKETDATA, "import redis.asyncio\n", "redis is allowed only in src/marketdata/store"),
    (RISKMGR, "import httpx\n", "HTTP client"),
    (ARMCONFIG, "import aiohttp\n", "HTTP client"),
    (RISKMGR, "import uuid\n", "not on the import allow-list"),
    (RISKMGR, "import random\n", "not on the import allow-list"),
    (RISKMGR, "from src.db.pg import AsyncpgSource\n", "not on the import allow-list"),
    (RISKMGR, "from src.oversee.sweep import RiskSweep\n", "not on the import allow-list"),
    (RISKMGR, "import logging.handlers\n", "logging handlers"),
    (RISKMGR, "from logging import config\n", "logging config"),
    (MARKETDATA, "import concurrent.futures\n", "concurrent.futures is allowed only in src/riskmgr"),
    (MARKETDATA, "from src.riskmgr import types\n", "never imports src.riskmgr"),
    (MARKETDATA, "from ..riskmgr.types import X\n", "never imports src.riskmgr"),
    (MARKETDATA, "import src.armconfig\n", "never imports"),
    (RISKMGR, "from . import lockout\n", "the scanner is imported by no scoped module"),
    (RISKMGR, "from src.riskmgr.lockout import scan\n", "the scanner is imported by no scoped module"),
    (ARMCONFIG, "from src.riskmgr.jobs import Runner\n", "not on the import allow-list"),
    (RISKMGR, "import asyncio.subprocess\n", "not on the asyncio allow-list"),
    (RISKMGR, "from src.dto import *\n", "star imports"),
])
def test_refused_imports(tmp_path: Path, rel: str, source: str, needle: str) -> None:
    result = _plant(tmp_path, rel, source)
    hits = [v for v in result.violations if v.rule in (Rule.IMPORT, Rule.MEMBER) and needle in v.detail]
    assert hits, [str(v) for v in result.violations]


@pytest.mark.parametrize("rel, source", [
    (RISKMGR, "from __future__ import annotations\nimport asyncio, bisect, collections.abc, contextlib, json\n"
              "import dataclasses, datetime, decimal, enum, functools, hashlib, hmac, itertools, logging, math\n"
              "import re, struct, time, typing, zoneinfo, abc\n"),
    (RISKMGR, "from src.dto import OrderDTO\nfrom src.oversee.risk import effective_stop\n"
              "from src.calendar.sessions import x\nfrom src.calendar import schedule, machine\n"
              "from src.db.arm_ports import ArmRow\nfrom src.marketdata.types import Bar\n"
              "from src.armconfig.resolve import resolve\nfrom . import types\nfrom .strategy import SlStrategy\n"),
    (RISKMGR, "from concurrent.futures import ThreadPoolExecutor\nimport concurrent.futures\n"
              "e = concurrent.futures.ThreadPoolExecutor(max_workers=1)\nfrom concurrent import futures\n"
              "f = futures.ThreadPoolExecutor\n"),
    (MARKETDATA, "import zlib\nfrom zlib import crc32\nd = zlib.crc32(b'x')\n"),
    (MARKETDATA, "from src.marketdata import types\nfrom .types import Bar\n"),
    (STORE, "import redis\nimport redis.asyncio as aredis\nfrom redis.exceptions import ResponseError\n"),
    (ARMCONFIG, "import ssl\nimport asyncpg\nfrom src.riskmgr.registry import lookup\n"
                "from src.marketdata.indicators.registry import lookup as ind\nfrom src.armconfig import doc\n"),
    (RISKMGR, "import asyncio\nloop = asyncio.get_running_loop()\nf = asyncio.wrap_future\n"),
    (RISKMGR, "import asyncio as aio\nq = aio.Queue()\nasync def f():\n"
              "    await aio.sleep(1)\n    async with aio.timeout(1):\n        pass\n"
              "    t = aio.create_task(f())\n    await aio.gather(t)\n    await aio.wait_for(t, 1)\n"
              "    await aio.to_thread(print)\n    e, lk = aio.Event(), aio.Lock()\n"
              "    try:\n        pass\n    except aio.CancelledError:\n        raise\n"),
    (RISKMGR, "import logging\nlog = logging.getLogger(__name__)\n"),
])
def test_allowed_imports(tmp_path: Path, rel: str, source: str) -> None:
    result = _plant(tmp_path, rel, source)
    assert result.ok, [str(v) for v in result.violations]


# ---------------------------------------------------------------------------------------------------- members
@pytest.mark.parametrize("rel, source, needle", [
    (RISKMGR, "import asyncio\nasync def f():\n    await asyncio.open_connection('h', 1)\n",
     "asyncio.open_connection"),
    (RISKMGR, "from asyncio import create_connection\n", "asyncio.create_connection"),
    (RISKMGR, "import asyncio as a\nasync def f():\n    await a.create_subprocess_exec('x')\n",
     "asyncio.create_subprocess_exec"),
    (MARKETDATA, "import asyncio\nf = asyncio.wrap_future\n", "asyncio.wrap_future"),
    (MARKETDATA, "from asyncio import get_running_loop\n", "asyncio.get_running_loop"),
    (MARKETDATA, "import zlib\nz = zlib.compress(b'x')\n", "zlib.compress"),
    (MARKETDATA, "from zlib import decompress\n", "zlib.decompress"),
    (RISKMGR, "import concurrent.futures\np = concurrent.futures.ProcessPoolExecutor()\n",
     "concurrent.futures.ProcessPoolExecutor"),
    (RISKMGR, "from concurrent.futures import ProcessPoolExecutor\n", "concurrent.futures.ProcessPoolExecutor"),
    (RISKMGR, "import concurrent.futures as cf\nw = cf.wait\n", "concurrent.futures.wait"),
    (RISKMGR, "import logging\nh = logging.handlers.SysLogHandler\n", "logging.handlers"),
    (RISKMGR, "import logging\nlogging.config.dictConfig({})\n", "logging.config"),
    # refused modules reached through an allowed one (review S11: prefix matching alone missed these)
    (RISKMGR, "import logging\nlogging.os.system('x')\n", "'os'"),
    (RISKMGR, "from logging import os\n", "logging.os"),
    (RISKMGR, "from contextlib import sys\n", "contextlib.sys"),
    (RISKMGR, "import typing\nm = typing.sys.modules\n", "'sys'"),
    (RISKMGR, "import dataclasses\nf = dataclasses.inspect.currentframe\n", "'inspect'"),
    (RISKMGR, "import decimal\ns = decimal.sys\n", "'sys'"),
    (RISKMGR, "import collections\ns = collections.sys\n", "'sys'"),
    (RISKMGR, "import enum\ns = enum.sys\n", "'sys'"),
    (RISKMGR, "from src.oversee import risk\nx = risk.logging\n", "'logging'"),
    (RISKMGR, "from src.dto import threading\n", "src.dto.threading"),
    (RISKMGR, "import functools\nw = functools.weakref\n", "'weakref'"),
    (RISKMGR, "import dataclasses\nd = dataclasses.copy.deepcopy\n", "copy module"),
    (RISKMGR, "from src.oversee import types\n", "src.oversee.types"),
    (MARKETDATA, "import json\nt = json.types\n", "'types'"),
    (RISKMGR, "import logging\nlg = logging\nh = lg.FileHandler('x')\n", "module 'logging' used as a value"),
    (RISKMGR, "def f():\n    import asyncio\n    a = asyncio\n", "module 'asyncio' used as a value"),
    (RISKMGR, "from concurrent import futures\nf = futures\n", "module 'futures' used as a value"),
    # event-loop network and process methods on any receiver (design §B3)
    (RISKMGR, "import asyncio\nasync def f():\n    loop = asyncio.get_running_loop()\n"
              "    await loop.create_connection(None, 'h', 1)\n", "'create_connection'"),
    (RISKMGR, "import asyncio\nasyncio.get_running_loop().subprocess_exec(None, 'x')\n", "'subprocess_exec'"),
    (RISKMGR, "def f(loop):\n    return loop.sock_connect, loop.getaddrinfo, loop.start_tls\n", "'sock_connect'"),
    (RISKMGR, "class T:\n    def g(self):\n        return self.open_connection()\n", "'open_connection'"),
    (ARMCONFIG, "def f(loop):\n    return loop.create_server\n", "'create_server'"),
    (RISKMGR, "from src.dto import create_subprocess_exec\n", "src.dto.create_subprocess_exec"),
    # plain logging: no handlers, files or root logger
    (RISKMGR, "import logging\nh = logging.FileHandler('x')\n", "logging.FileHandler"),
    (RISKMGR, "import logging\nlogging.basicConfig(filename='x')\n", "logging.basicConfig"),
    (RISKMGR, "import logging\nh = logging.getLogger().handlers\n", "logger handlers"),
    (RISKMGR, "import logging\nm = logging.root.manager\n", "logging.root"),
    (RISKMGR, "from logging import StreamHandler\n", "logging.StreamHandler"),
    (RISKMGR, "import logging\nlog = logging.getLogger(__name__)\nlog.addHandler(None)\n", "logger handlers"),
])
def test_refused_members(tmp_path: Path, rel: str, source: str, needle: str) -> None:
    result = _plant(tmp_path, rel, source)
    _refused(result.violations, Rule.MEMBER, needle)


# ---------------------------------------------------------------------------------------------------- reflection
@pytest.mark.parametrize("source, rule, needle", [
    ("def f(x):\n    return getattr(x, '_y')\n", Rule.REFLECTION, "private name '_y'"),
    ("def f(x, n):\n    return getattr(x, n)\n", Rule.REFLECTION, "getattr() with a non-literal name"),
    ("def f(x):\n    setattr(x, 'a' + 'b', 1)\n", Rule.REFLECTION, "setattr() with a non-literal name"),
    ("def f(x):\n    return hasattr(x, '__dict__')\n", Rule.REFLECTION, "hasattr() with the private name"),
    ("def f(x):\n    delattr(x, '_z')\n", Rule.REFLECTION, "delattr() with the private name"),
    ("def f(x, *a):\n    return getattr(*a)\n", Rule.REFLECTION, "non-literal name"),
    ("def f(x):\n    return vars(x)\n", Rule.REFLECTION, "vars() used without a literal key"),
    ("def f(x):\n    return dir(x)\n", Rule.REFLECTION, "dir() used without a literal key"),
    ("def f(k):\n    return globals()[k]\n", Rule.REFLECTION, "globals() used without a literal key"),
    ("def f():\n    return globals()['_secret']\n", Rule.REFLECTION, "private key '_secret'"),
    ("def f(x):\n    return x._y\n", Rule.PRIVATE_ATTRIBUTE, "'_y'"),
    ("def f(x):\n    return x.__class__.__mro__\n", Rule.PRIVATE_ATTRIBUTE, "'__class__'"),
    ("def f(x):\n    return type(x).__subclasses__()\n", Rule.PRIVATE_ATTRIBUTE, "'__subclasses__'"),
    ("m = __import__('os')\n", Rule.DYNAMIC_CODE, "__import__()"),
    ("eval('1')\n", Rule.DYNAMIC_CODE, "eval()"),
    ("exec('x = 1')\n", Rule.DYNAMIC_CODE, "exec()"),
    ("o = __builtins__\n", Rule.DYNAMIC_CODE, "__builtins__"),
    # aliases of the builtins the call checks cover (review S11)
    ("imp = __import__\nm = imp('socket')\n", Rule.DYNAMIC_CODE, "__import__ used as a value"),
    ("e = eval\ne('1')\n", Rule.DYNAMIC_CODE, "eval used as a value"),
    ("def f(x):\n    g = getattr\n    return g(x, '_y')\n", Rule.REFLECTION, "getattr used as a value"),
    ("import functools\np = functools.partial(getattr)\n", Rule.REFLECTION, "getattr used as a value"),
    ("def f(x, n):\n    return list(map(getattr, [x], [n]))\n", Rule.REFLECTION, "getattr used as a value"),
    ("import functools\ndef f(x, ns):\n    return functools.reduce(getattr, ns, x)\n", Rule.REFLECTION,
     "getattr used as a value"),
    ("g = (lambda: getattr)()\n", Rule.REFLECTION, "getattr used as a value"),
    ("def f():\n    return (g := getattr)\n", Rule.REFLECTION, "getattr used as a value"),
    ("def f(getattr=getattr):\n    return getattr\n", Rule.REFLECTION, "getattr used as a value"),
    ("v = vars\n", Rule.REFLECTION, "vars used as a value"),
    ("def f(x):\n    return getattr(x, 'sys')\n", Rule.REFLECTION, "refused name 'sys'"),
    ("def f(x):\n    return getattr(x, 'create_connection')\n", Rule.REFLECTION, "refused name"),
    ("import logging\ndef f():\n    return globals()['logging']\n", Rule.REFLECTION, "refused key 'logging'"),
    # refused builtins: filesystem, debugger, stdin
    ("f = open('x', 'w')\n", Rule.REFUSED_BUILTIN, "open()"),
    ("breakpoint()\n", Rule.REFUSED_BUILTIN, "breakpoint()"),
    ("s = input()\n", Rule.REFUSED_BUILTIN, "input()"),
    ("o = open\n", Rule.REFUSED_BUILTIN, "open used as a value"),
    # private attributes read without an Attribute node, or on a `self` that is not a method's receiver
    ("def f(x):\n    match x:\n        case object(__class__=c):\n            return c\n", Rule.PRIVATE_ATTRIBUTE,
     "'__class__'"),
    ("def f(x):\n    return '{0.__class__}'.format(x)\n", Rule.PRIVATE_ATTRIBUTE, "template reads"),
    ("def f(x):\n    return '{a._b}'.format_map({'a': x})\n", Rule.PRIVATE_ATTRIBUTE, "template reads"),
    ("def f(t, x):\n    return t.format(x)\n", Rule.REFLECTION, "other than a string literal"),
    ("def f(x):\n    return str.format('{0}', x)\n", Rule.REFLECTION, "other than a string literal"),
    ("def peek(self):\n    return self._secret\n", Rule.PRIVATE_ATTRIBUTE, "'_secret'"),
    ("class C:\n    @staticmethod\n    def p(self):\n        return self._s\n", Rule.PRIVATE_ATTRIBUTE, "'_s'"),
    ("class C:\n    def p(self, o):\n        self = o\n        return self._s\n", Rule.PRIVATE_ATTRIBUTE, "'_s'"),
    ("class C:\n    def p(self, o):\n        return (lambda self: self._s)(o)\n", Rule.PRIVATE_ATTRIBUTE, "'_s'"),
    ("class C:\n    def p(x, self):\n        return self._s\n", Rule.PRIVATE_ATTRIBUTE, "'_s'"),
    ("class C:\n    x = cls._s\n", Rule.PRIVATE_ATTRIBUTE, "'_s'"),
    ("class C:\n    def m(self, o):\n        return super(C, o)._x\n", Rule.PRIVATE_ATTRIBUTE, "'_x'"),
])
def test_reflection_rules(tmp_path: Path, source: str, rule: Rule, needle: str) -> None:
    _refused(_plant(tmp_path, RISKMGR, source).violations, rule, needle)


@pytest.mark.parametrize("rel, source", [
    (RISKMGR, "def f(d: dict[str, int]) -> dict[str, int]:\n    return d.copy()\n"),
    (MARKETDATA, "def f(bar: object) -> object:\n    open = bar.open\n    for input in (1, 2):\n        pass\n"
                 "    return open, input\n"),
    (RISKMGR, "def f(input: str, dir: str) -> str:\n    return input.strip() + dir\n"),
    (RISKMGR, "def outer() -> object:\n    vars = {}\n    def inner() -> object:\n        return vars\n"
              "    return inner\n"),
    (RISKMGR, "from src.marketdata import types\nfrom . import types as t\nimport src.riskmgr.types\n"
              "x = src.riskmgr.types.Bar\n"),
    (RISKMGR, "class C:\n    def m(self) -> object:\n        return sorted([1], key=lambda v: self._k)\n"
              "    async def a(self) -> object:\n        def inner() -> object:\n            return self._k\n"
              "        return inner\n"),
    (RISKMGR, "x = '{0} {1:.2f} {name}'.format(1, 2.0, name='n')\n"),
    (RISKMGR, "import logging\nlog = logging.getLogger(__name__)\nlog.info('x %s', 1)\nlevel = logging.INFO\n"
              "class D:\n    def __init__(self) -> None:\n        self.handlers: dict[str, int] = {}\n"),
    (RISKMGR, "def f(x: object) -> object:\n    match x:\n        case object(real=r):\n            return r\n"
              "    return None\n"),
    (STORE, "import redis\nimport redis.asyncio as aredis\nfrom redis import asyncio as ar\n"
            "x = redis.asyncio.Redis\n"),
    (ARMCONFIG, "import ssl\nimport asyncpg\nc = ssl.create_default_context()\n"),
])
def test_allowed_forms_after_review(tmp_path: Path, rel: str, source: str) -> None:
    result = _plant(tmp_path, rel, source)
    assert result.ok, [str(v) for v in result.violations]


def test_reflection_allowed_forms(tmp_path: Path) -> None:
    source = """
        from dataclasses import dataclass, asdict, fields
        import dataclasses

        @dataclass(frozen=True)
        class Row:
            a: int

        class Base:
            def __init__(self) -> None:
                self._x = 1

        class Child(Base):
            _count = 0

            def __init__(self) -> None:
                super().__init__()
                self._y = self._x
                type(self)._count  # noqa

            @classmethod
            def make(cls) -> int:
                return cls._count

        def f(row: Row, x: object) -> object:
            return (getattr(x, "name", None), hasattr(x, "value"), vars(row)["a"], globals()["Row"],
                    asdict(row), fields(row), dataclasses.asdict(row))
    """
    result = _plant(tmp_path, RISKMGR, source)
    assert [v for v in result.violations if v.rule is not Rule.PRIVATE_ATTRIBUTE] == []
    # `type(self)._count` is a read on something other than self / cls and stays refused
    assert [v.detail for v in result.violations] == ["attribute '_count' is read on something other than self / cls"]


@pytest.mark.parametrize("source, needle", [
    ("import dataclasses\ndef f(port: 'MarketDataPort'):\n    return dataclasses.asdict(port)\n",
     "dataclasses.asdict() applied to a port"),
    ("from dataclasses import fields\ndef f(p: StampPort | None):\n    return fields(p)\n",
     "dataclasses.fields() applied to a port"),
    ("def f(pager: Pager):\n    return vars(pager)['x']\n", "vars() applied to a port"),
    ("from typing import Protocol\nclass Feed(Protocol):\n    pass\n"
     "from dataclasses import asdict\ndef f(feed: Feed):\n    return asdict(feed)\n", "asdict() applied to a port"),
    ("from dataclasses import asdict\nclass T:\n    def g(self):\n        return asdict(self._pager)\n",
     "asdict() applied to a port"),
])
def test_port_reflection_is_refused(tmp_path: Path, source: str, needle: str) -> None:
    _refused(_plant(tmp_path, RISKMGR, source).violations, Rule.PORT_REFLECTION, needle)


# ---------------------------------------------------------------------------------------------------- the scanner itself
def test_scanner_passes_only_under_its_own_name(tmp_path: Path) -> None:
    source = (REPO / SCANNER_SELF).read_text(encoding="utf-8")
    assert scan_source(source, SCANNER_SELF) == ()
    copy = scan_source(source, "src/riskmgr/lockout_copy.py")
    _refused(copy, Rule.IMPORT, "import ast")
    _refused(copy, Rule.IMPORT, "import sys", "interpreter internals")
    _refused(copy, Rule.IMPORT, "from pathlib import Path")
    _refused(copy, Rule.REFLECTION, "private name '__file__'")
    # the same copy in the planted tree is refused too
    assert not _plant(tmp_path, "src/marketdata/scanner.py", source).ok


def test_scanner_self_has_its_own_allow_list(tmp_path: Path) -> None:
    found = _check(SCANNER_SELF, "import os\nimport sys\nimport json\nx = sys.path\ny = sys.modules\n")
    _refused(found, Rule.IMPORT, "import os", "scanner's own allow-list")
    _refused(found, Rule.IMPORT, "import json", "scanner's own allow-list")
    _refused(found, Rule.MEMBER, "sys.path")
    assert not [v for v in found if "sys.modules" in v.detail]
    # the name ban still applies to it
    _refused(_check(SCANNER_SELF, "def place_stop():\n    pass\n"), Rule.NAME_BAN, "'place_stop'")
    # reflection and private attributes are exempt there
    assert _check(SCANNER_SELF, "import sys\ndef f(m: object) -> object:\n    return getattr(m, '__file__')\n") == ()
    # dynamic import / code, refused builtins and __builtins__ are not (design §B3 exempts only the three rules above)
    found = _check(SCANNER_SELF, 'import sys\nm = __import__("socket")\ne = eval("1")\nb = __builtins__\n'
                                 "x = compile\nf = open('x')\n")
    _refused(found, Rule.DYNAMIC_CODE, "__import__()")
    _refused(found, Rule.DYNAMIC_CODE, "eval()")
    _refused(found, Rule.DYNAMIC_CODE, "__builtins__")
    _refused(found, Rule.DYNAMIC_CODE, "compile used as a value")
    _refused(found, Rule.REFUSED_BUILTIN, "open()")
    # nor the refused attribute names and event-loop methods
    _refused(_check(SCANNER_SELF, "import re\nx = re.sys\n"), Rule.MEMBER, "'sys'")
    _refused(_check(SCANNER_SELF, "def f(loop):\n    return loop.create_connection\n"), Rule.MEMBER,
             "'create_connection'")


# ---------------------------------------------------------------------------------------------------- tree, loaded modules, files
def test_unreadable_files_make_the_scan_incomplete(tmp_path: Path) -> None:
    base = tmp_path / "src" / "riskmgr"
    base.mkdir(parents=True)
    (base / "bad_bytes.py").write_bytes(b"x = '\xff\xfe\xfa'\n")
    (base / "bad_syntax.py").write_text("def f(:\n", encoding="utf-8")
    (base / "folder.py").mkdir()
    (base / "good.py").write_text("x = 1\n", encoding="utf-8")
    result = scan([tmp_path], modules={})
    assert result.violations == ()
    joined = " ".join(result.incomplete)
    assert "bad_bytes.py: UnicodeDecodeError" in joined
    assert "bad_syntax.py: SyntaxError at line 1" in joined
    assert "folder.py:" in joined
    assert any(s.endswith("good.py") for s in result.scanned)
    with pytest.raises(ScanIncomplete):
        require_clean(result)


@pytest.mark.parametrize("source", [
    "x = " + "-" * 100_000 + "1\n",          # too deep for the parser (MemoryError)
    "x = a" + ".b" * 200_000 + "\n",         # parses, but too deep for the AST walk (RecursionError)
])
def test_too_deep_nesting_is_incomplete_not_raised(tmp_path: Path, source: str) -> None:
    base = tmp_path / "src" / "riskmgr"
    base.mkdir(parents=True)
    (base / "deep.py").write_text(source, encoding="utf-8")
    result = scan([tmp_path], modules={})
    assert result.violations == ()
    assert len(result.incomplete) == 1 and "deep.py: " in result.incomplete[0]
    assert result.incomplete[0].endswith(("MemoryError", "RecursionError"))
    with pytest.raises(ScanIncomplete):
        require_clean(result)


def test_read_error_is_incomplete(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "src" / "armconfig").mkdir(parents=True)
    (tmp_path / "src" / "armconfig" / "a.py").write_text("x = 1\n", encoding="utf-8")

    def locked(self: Path) -> bytes:
        raise PermissionError("locked")

    monkeypatch.setattr(Path, "read_bytes", locked)
    result = scan([tmp_path], modules={})
    assert result.incomplete and "PermissionError" in result.incomplete[0]
    assert not result.ok


def test_violation_wins_over_incomplete(tmp_path: Path) -> None:
    _plant(tmp_path, "src/riskmgr/x.py", "def f(:\n")
    result = _plant(tmp_path, RISKMGR, "import socket\n")
    with pytest.raises(LockoutViolation):
        require_clean(result)


def test_compiled_files_in_scope_are_refused(tmp_path: Path) -> None:
    base = tmp_path / "src" / "marketdata"
    (base / "__pycache__").mkdir(parents=True)
    (base / "__pycache__" / "a.cpython-312.pyc").write_bytes(b"\0")
    assert scan([tmp_path], modules={}).ok
    (base / "native.pyd").write_bytes(b"\0")
    (base / "loose.pyc").write_bytes(b"\0")
    result = scan([tmp_path], modules={})
    assert sorted(Path(v.path).name for v in result.violations if v.rule is Rule.COMPILED_FILE) == [
        "loose.pyc", "native.pyd"]


def test_loaded_modules_of_the_scope_are_scanned(tmp_path: Path) -> None:
    elsewhere = tmp_path / "elsewhere" / "ghost.py"
    elsewhere.parent.mkdir(parents=True)
    elsewhere.write_text("import socket\n", encoding="utf-8")
    ghost = types.ModuleType("src.riskmgr.ghost")
    ghost.__file__ = str(elsewhere)
    sourceless = types.ModuleType("src.marketdata.sourceless")
    compiled = types.ModuleType("src.armconfig.native")
    compiled.__file__ = str(tmp_path / "native.pyd")
    unrelated = types.ModuleType("src.adapters.anything")
    unrelated.__file__ = str(elsewhere)
    modules = {m.__name__: m for m in (ghost, sourceless, compiled, unrelated)}
    result = scan([tmp_path / "empty-root"], modules=modules)
    _refused(result.violations, Rule.IMPORT, "import socket")
    _refused(result.violations, Rule.LOADED_MODULE, "no source file")
    _refused(result.violations, Rule.LOADED_MODULE, "not Python source")
    assert [s for s in result.scanned] == [elsewhere.as_posix()]


def test_loaded_module_rules_follow_its_package(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("import zlib\n", encoding="utf-8")
    md = types.ModuleType("src.marketdata.x")
    md.__file__ = str(f)
    assert scan([], modules={"src.marketdata.x": md}).ok
    rm = types.ModuleType("src.riskmgr.x")
    rm.__file__ = str(f)
    _refused(scan([], modules={"src.riskmgr.x": rm}).violations, Rule.IMPORT, "zlib")


def test_scan_source_refuses_paths_outside_the_scope() -> None:
    with pytest.raises(ValueError):
        scan_source("x = 1\n", "src/adapters/mock.py")


# ---------------------------------------------------------------------------------------------------- sinks
def test_assert_sinks() -> None:
    assert_sinks(PHASE1_SINKS)
    assert_sinks(["decision", "notice"])
    for bad in ((), ("notice", "decision"), ("decision",), ("decision", "notice", "supabase"), ("decision", "order")):
        with pytest.raises(SinkSetError):
            assert_sinks(bad)


# ---------------------------------------------------------------------------------------------------- the real tree (every G)
def test_scope_constants() -> None:
    assert LOCKOUT_SCOPE == ("src/riskmgr", "src/marketdata", "src/armconfig")
    assert NAME_BAN_FILES == ("src/runtime/md_ports.py",)
    assert SCANNER_SELF == "src/riskmgr/lockout.py"
    for d in LOCKOUT_SCOPE:
        assert (REPO / d / "__init__.py").is_file(), d


def test_real_scope_tree_passes() -> None:
    """Scans the real `LOCKOUT_SCOPE` tree (and `src/runtime/md_ports.py` once it exists) plus every loaded module of
    the scoped packages. This runs in every green gate from S11 on, approval of the `.claude/` diff or not."""
    result = scan([REPO])
    assert result.violations == (), "\n".join(str(v) for v in result.violations)
    assert result.incomplete == (), result.incomplete
    scanned = {Path(s).resolve() for s in result.scanned}
    assert (REPO / SCANNER_SELF).resolve() in scanned
    for d in LOCKOUT_SCOPE:
        for p in (REPO / d).rglob("*.py"):
            if "__pycache__" not in p.parts:
                assert p.resolve() in scanned, p
    md_ports = REPO / NAME_BAN_FILES[0]
    if md_ports.exists():
        assert md_ports.resolve() in scanned
    require_clean(result)


def test_module_uses_only_its_own_allow_list() -> None:
    assert lockout.SCANNER_SELF_ALLOWED == frozenset(
        {"__future__", "ast", "dataclasses", "enum", "pathlib", "re", "sys", "typing"})
