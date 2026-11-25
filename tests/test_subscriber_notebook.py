"""The Python example from a Databricks notebook (examples/subscribers/python/notebook.py): the notebook's own token is
exchanged for one the Subscription API's App accepts, the notebook installs exactly what the example imports at the
versions the repo locks (and the Python they need), and it runs subscriber.main as the user with its own rules."""

import ast
import importlib.metadata
import re
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qsl

import pytest
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

pytest.importorskip("confluent_kafka")

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / "examples/subscribers/python"
NOTEBOOK = PY / "notebook.py"
sys.path.insert(0, str(PY))

import build  # noqa: E402

build.ensure()

import api  # noqa: E402
import httpx2  # noqa: E402
import subscriber  # noqa: E402
from hl7_subscriptions_client.api.service import health  # noqa: E402

from subscription_api.rules import parse  # noqa: E402

NOW = 1_793_880_000.0
HOST = "https://adb-1.azuredatabricks.net"
APP_URL = "https://hl7-subscriptions-dev-1.azure.databricksapps.com"


# --- The token exchange: a notebook's token can't call a Databricks App ----------------------------------------------
def test_the_notebooks_token_is_exchanged_for_one_with_the_app_as_its_audience():
    seen, clock, subject = [], [NOW], iter(["notebook-1", "notebook-2"])

    def answer(request):
        seen.append(request)
        return httpx2.Response(200, json={"access_token": f"app-{len(seen)}", "token_type": "Bearer",
                                          "expires_in": 3600})  # fmt: skip

    token = api.Exchanged(HOST + "/", lambda: next(subject), "app-client-id", transport=httpx2.MockTransport(answer),
                          clock=lambda: clock[0])  # fmt: skip
    assert token() == "app-1"
    (r,) = seen
    assert (r.method, str(r.url)) == ("POST", f"{HOST}/oidc/v1/token")
    assert r.headers["content-type"] == "application/x-www-form-urlencoded"
    assert dict(parse_qsl(r.content.decode())) == {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": "notebook-1",
        "subject_token_type": "urn:databricks:params:oauth:token-type:personal-access-token",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "scope": "all-apis",
        "audience": "app-client-id",
    }
    clock[0] += 3600 - api.EXCHANGE_MARGIN - 1
    assert token() == "app-1" and len(seen) == 1, "cached"
    clock[0] += 2
    assert token() == "app-2", "exchanged again shortly before it expires"
    assert dict(parse_qsl(seen[1].content.decode()))["subject_token"] == "notebook-2", "with the notebook's token now"


def test_a_refused_exchange_says_why():
    error = {"error": "invalid_request", "error_description": "unknown audience"}
    refuse = httpx2.MockTransport(lambda r: httpx2.Response(400, json=error))
    with pytest.raises(RuntimeError, match="400 invalid_request: unknown audience"):
        api.Exchanged(HOST, lambda: "notebook", "nope", transport=refuse)()
    no_token = httpx2.MockTransport(lambda r: httpx2.Response(200, json={"token_type": "Bearer"}))
    with pytest.raises(RuntimeError, match="no access_token"):
        api.Exchanged(HOST, lambda: "notebook", "app-client-id", transport=no_token)()


# --- Which token the API gets -----------------------------------------------------------------------------------------
def _workspace(lookups):
    """A stand-in WorkspaceClient: its host, its own token, and the Subscription API's App."""
    app = SimpleNamespace(url=APP_URL, oauth2_app_client_id="app-client-id")
    return SimpleNamespace(config=SimpleNamespace(host=HOST, authenticate=lambda: {"Authorization": "Bearer own"}),
                           apps=SimpleNamespace(get=lambda name: lookups.append(name) or app))  # fmt: skip


def _sent(client):
    """The request the client sends for one call."""
    seen = []
    original = client.get_httpx_client()
    answer = httpx2.MockTransport(lambda r: seen.append(r) or httpx2.Response(200, json={"ok": True}))
    client.set_httpx_client(httpx2.Client(base_url=original.base_url, auth=original.auth, transport=answer))
    health.sync_detailed(client=client)
    return seen[0]


@pytest.fixture
def exchanges(monkeypatch):
    """Records each api.Exchanged built (host, the subject token it reads, audience); each one answers 'app-token'."""
    made = []

    class Exchanged:
        def __init__(self, host, subject_token, audience):
            made.append((host, subject_token(), audience))

        def __call__(self):
            return "app-token"

    monkeypatch.setattr(subscriber.api, "Exchanged", Exchanged)
    return made


def test_in_a_notebook_the_api_gets_the_exchanged_token(monkeypatch, exchanges):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.4.0")
    lookups = []
    r = _sent(subscriber.api_client(_workspace(lookups), {"api.app": "hl7-subscriptions-dev"}))
    assert lookups == ["hl7-subscriptions-dev"], "one lookup: the App's URL and its OAuth client ID"
    assert exchanges == [(HOST, "own", "app-client-id")], "the notebook's own token, for this App"
    assert (str(r.url), r.headers["Authorization"]) == (f"{APP_URL}/api/v1/health", "Bearer app-token")


def test_elsewhere_or_with_a_profile_the_api_gets_the_identitys_own_token(monkeypatch, exchanges):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert _sent(subscriber.api_client(_workspace([]), {"api.app": "x"})).headers["Authorization"] == "Bearer own"
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.4.0")
    cfg = {"api.app": "x", "databricks.profile": "me"}
    assert _sent(subscriber.api_client(_workspace([]), cfg)).headers["Authorization"] == "Bearer own"
    assert exchanges == []


def test_in_a_notebook_the_api_is_named_by_its_app(monkeypatch, exchanges):
    """The exchange needs the App's OAuth client ID, which only its name finds: api.url alone isn't enough."""
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.4.0")
    with pytest.raises(ValueError, match="api.app"):
        subscriber.api_client(_workspace([]), {"api.url": "https://api.example"})
    cfg = {"api.url": "https://api.example", "api.app": "hl7-subscriptions-dev"}
    assert str(_sent(subscriber.api_client(_workspace([]), cfg)).url).startswith("https://api.example/"), "url wins"


# --- The notebook -----------------------------------------------------------------------------------------------------
def _cells() -> list[str]:
    text = NOTEBOOK.read_text()
    assert text.startswith("# Databricks notebook source\n")
    return [c.strip() for c in text.removeprefix("# Databricks notebook source\n").split("# COMMAND ----------")]


def _canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _pins() -> dict[str, str]:
    (line,) = [c for c in _cells() if c.startswith("# MAGIC %pip install ")]
    assert "\n" not in line, "one %pip install line"
    pins = re.findall(r"\s([A-Za-z0-9_.-]+)==(\S+)", line)
    assert len(pins) == len(line.split()) - 4, f"every package pinned with ==: {line}"
    return {_canonical(n): v for n, v in pins}


def _third_party_imports() -> set[str]:
    """Top-level modules the example (all but the notebook) and its generated client import from outside both."""
    files = [p for p in PY.glob("*.py") if p != NOTEBOOK] + list((build.OUT / build.PACKAGE).rglob("*.py"))
    names = set()
    for f in files:
        for node in ast.walk(ast.parse(f.read_text())):
            if isinstance(node, ast.Import):
                names |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                names.add(node.module.split(".")[0])
    return names - {p.stem for p in PY.glob("*.py")} - {build.PACKAGE} - set(sys.stdlib_module_names)


def test_the_notebook_installs_exactly_what_the_example_imports_at_the_locked_versions():
    pins = _pins()
    locked = {p["name"]: p["version"] for p in tomllib.loads((ROOT / "uv.lock").read_text())["package"]}
    for name, version in pins.items():
        assert locked.get(name) == version, f"{name}=={version}, but uv.lock has {locked.get(name)}"
    dev = [Requirement(r) for r in tomllib.loads((ROOT / "pyproject.toml").read_text())["dependency-groups"]["dev"]]
    for req in dev:
        if _canonical(req.name) in pins:
            assert req.specifier.contains(pins[_canonical(req.name)]), f"{req} in pyproject.toml"

    providers = importlib.metadata.packages_distributions()
    needed = {m: {_canonical(d) for d in providers[m]} for m in _third_party_imports()}
    assert {"confluent_kafka", "databricks", "httpx2", "attrs"} <= set(needed)
    for module, dists in needed.items():
        assert dists & set(pins), f"the notebook doesn't install {module} ({sorted(dists)})"
    used = set().union(*needed.values()) | {"openapi-python-client"}  # build.py runs the generator
    assert set(pins) <= used, f"installed but not imported: {sorted(set(pins) - used)}"
    assert {"databricks-sdk", "openapi-python-client"} <= set(pins), "a serverless notebook's own SDK can be too old"


def test_the_notebook_needs_the_python_its_packages_need():
    """Serverless environment version 1 is Python 3.10; the generator, httpx2, and the generated client need 3.11."""
    guard = re.search(r"sys\.version_info < \((\d+), (\d+)\)", _cells()[1])
    assert guard, "the first code cell checks the Python version before installing anything"
    minimum = tuple(int(x) for x in guard.groups())
    specs = [SpecifierSet(importlib.metadata.metadata(n)["Requires-Python"] or "") for n in _pins()]
    needed = next((3, m) for m in range(8, 20) if all(s.contains(f"3.{m}.0") for s in specs))
    assert minimum == needed, f"the guard says {minimum}, the pinned packages need {needed}"
    assert "Environment version 4" in _cells()[0], "the markdown says which serverless environment to pick"


def test_the_notebook_runs_the_example_as_you_with_its_rules_then_cancels_them(monkeypatch, capsys):
    """Every cell, in order, with dbutils and spark stood in for and subscriber.main recorded."""
    monkeypatch.chdir(PY)  # a notebook's working directory is its folder in the Git folder
    calls, widgets = [], {}

    def main(argv, rules=None):
        calls.append((argv, rules))
        raise SystemExit(0)

    monkeypatch.setattr(subscriber, "main", main)
    dbutils = SimpleNamespace(
        library=SimpleNamespace(restartPython=lambda: None),
        widgets=SimpleNamespace(text=lambda name, default, label=None: widgets.setdefault(name, default),
                                get=lambda name: widgets[name]),
    )  # fmt: skip
    spark = SimpleNamespace(sql=lambda q: SimpleNamespace(first=lambda: ["someone@example.com"]))
    namespace = {"dbutils": dbutils, "spark": spark, "__name__": "__main__"}
    exec(compile(NOTEBOOK.read_text(), str(NOTEBOOK), "exec"), namespace)

    (run, rules), (cleanup, none) = calls
    assert run == ["api.app=hl7-subscriptions-dev", "identity=user", "consumer.group=someone-notebook",
                   "--duration=300"]  # fmt: skip
    assert rules is namespace["RULES"] and none == {}, "its own rules, then an empty set: every one cancelled"
    assert "identity=user" in cleanup and "api.app=hl7-subscriptions-dev" in cleanup
    for sql in rules.values():
        assert parse(sql, {"epic_realtime"}, "cat.sub_me").source == "epic_realtime", "a shape the API takes"
    assert sys.version.split()[0] in capsys.readouterr().out, "prints the Python it runs on"


def test_subscriber_imports_e2e_only_for_the_checks():
    """e2e.py isn't for notebooks; subscriber.py loads it only under --e2e."""
    tree = ast.parse((PY / "subscriber.py").read_text())
    top = {a.name for n in tree.body if isinstance(n, ast.Import) for a in n.names}
    top |= {n.module for n in tree.body if isinstance(n, ast.ImportFrom)}
    assert "e2e" not in top
