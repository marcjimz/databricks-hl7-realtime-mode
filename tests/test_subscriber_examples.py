"""The two example subscribers (examples/subscribers/python and java) are one app in two languages: the same e2e checks
in the same order, a rule the app's own grant can run, cleanup of every e2e-* rule, and the app's own hub's heartbeat.
The patient monitor exits from its main thread when the hub can't run a census edit."""

import re
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("confluent_kafka")

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / "examples/subscribers/python"
JAVA = ROOT / "examples/subscribers/java"
sys.path.insert(0, str(PY))

import build  # noqa: E402

build.ensure()

import e2e  # noqa: E402
import monitor  # noqa: E402

from hl7rt import subscriptions  # noqa: E402
from subscription_api.rules import columns, parse  # noqa: E402


def _check_names(path: Path) -> list[str]:
    """The checks a source file records, in order (a check recorded on two branches counts once; "run", the row for
    an aborted run, is left out)."""
    found = re.findall(r"\b(?:record|refused|skip)\(\s*\"([^\"]+)\"", path.read_text())
    return [n for n in dict.fromkeys(found) if n != "run"]


def test_both_examples_run_the_same_checks_in_the_same_order():
    checks = _check_names(PY / "e2e.py")
    assert checks == _check_names(JAVA / "src/main/java/com/databricks/hl7/subscriber/e2e/E2E.java")
    assert len(checks) == 26 and "This app's hub" in checks and checks[-1] == "Hub heartbeat"
    for readme in (PY / "README.md", JAVA / "README.md"):
        text = readme.read_text()
        assert re.search(r"\b26\b", text) and not re.search(r"\b25\b", text), readme


def test_the_source_not_granted_is_one_outside_the_apps_grant():
    assert set(e2e.SOURCES) == set(subscriptions.SOURCES), "the e2e knows every silver source"
    assert e2e.ungranted_source(["silver_observations", "silver_diagnoses"]) == "silver_encounters"
    assert e2e.ungranted_source(["silver_encounters", "silver_observations"]) == "silver_orders"
    assert e2e.ungranted_source(list(e2e.SOURCES)) is None, "granted every source: the check is skipped"


def _template_fields() -> list[str]:
    """The ed-respiratory-monitoring template's columns (infra/azure/terraform.tfvars.example)."""
    example = (ROOT / "infra/azure/terraform.tfvars.example").read_text()
    block = example[example.index('"ed-respiratory-monitoring" = {') :]
    return re.findall(r'"([^"]+)"', re.search(r"fields\s*=\s*\[([^\]]*)\]", block).group(1))


def test_the_e2e_rule_reads_only_what_the_app_is_granted():
    fields = _template_fields()
    assert "observation_cd_desc" not in fields and "alt_observation_cd" in fields
    good, changed = e2e.potassium(fields)
    for sql in (good, changed):
        r = parse(sql, set(subscriptions.SOURCES), "cat.sub_app")
        assert set(r.fields) | columns(r.where) <= set(fields), sql
    assert "alt_observation_cd = '2823-3'" in good, "by LOINC when the test's name column isn't granted"
    assert "units" in changed and "units" not in good

    named, _ = e2e.potassium([*fields, "observation_cd_desc"])
    assert "observation_cd_desc = 'Potassium'" in named
    assert "observation_cd_desc = 'Potassium'" in e2e.potassium([])[0], "an API without `fields`: as before"


def test_cleanup_cancels_every_e2e_rule_left_enabled(monkeypatch):
    subs = [SimpleNamespace(name="e2e-refused", enabled=True), SimpleNamespace(name="e2e-potassium", enabled=False),
            SimpleNamespace(name="critical-k", enabled=True), SimpleNamespace(name="e2e-ok", enabled=True)]  # fmt: skip
    assert e2e.leftovers(subs) == ["e2e-ok", "e2e-refused"], "the app's own rules are left alone"

    cancelled = []

    def call(operation, client, **kw):
        if operation is e2e.get_status:
            return SimpleNamespace(subscriptions=subs)
        cancelled.append(kw["name"])

    monkeypatch.setattr(e2e.api, "call", call)
    e2e.cleanup(None)
    assert cancelled == ["e2e-ok", "e2e-refused"]

    def unreachable(operation, client, **kw):
        if operation is e2e.get_status:
            raise RuntimeError("API down")
        cancelled.append(kw["name"])

    cancelled.clear()
    monkeypatch.setattr(e2e.api, "call", unreachable)
    e2e.cleanup(None)
    assert cancelled == list(e2e.NAMES), "status unreachable: cancel every name the checks use"


def test_the_heartbeat_is_the_apps_own_hubs():
    beats = [SimpleNamespace(hub="edp_shared", tier="realtime", source="silver_observations",
                             last_batch_at="2026-11-05T12:00:00Z"),
             SimpleNamespace(hub="bu_demo", tier="deferred", source="silver_observations",
                             last_batch_at="2026-11-05T12:00:09Z"),
             SimpleNamespace(hub="bu_demo", tier="realtime", source="silver_observations",
                             last_batch_at="2026-11-05T12:00:05Z")]  # fmt: skip
    assert e2e.heartbeat(beats, "bu_demo", "silver_observations") == "2026-11-05T12:00:05Z"
    assert e2e.heartbeat(beats, "edp_shared", "silver_observations") == "2026-11-05T12:00:00Z"
    assert e2e.heartbeat(beats, "edp_shared", "silver_orders") is None
    assert e2e.heartbeat(None, "bu_demo", "silver_observations") is None


def test_a_skipped_check_is_reported_and_doesnt_fail_the_run(capsys):
    t = e2e.E2E(None, None, None, {})
    t.record("Sign in", "profile x", "a service principal", "sp", True)
    t.skip("Source not granted", "a source this app isn't granted", "TABLE_OR_VIEW_NOT_FOUND",
           "this app is granted every source")  # fmt: skip
    t.print()
    out = capsys.readouterr().out
    assert "| 2 | Source not granted | a source this app isn't granted | TABLE_OR_VIEW_NOT_FOUND | this app is " \
           "granted every source | SKIP |" in out  # fmt: skip
    assert "2 checks, 0 failed, 1 skipped" in out and t.ok()
    t.record("Cancel", "-", "CANCELLED", "PENDING", False)
    assert not t.ok()


# --- The patient monitor (monitor.py) ---------------------------------------------------------------------------------
class _Quiet:
    """A consumer with nothing to read."""

    def consume(self, num_messages, timeout):
        time.sleep(min(timeout, 0.05))
        return []


def test_a_hub_failure_during_a_census_edit_ends_the_run_with_exit_3(monkeypatch):
    calls = []

    def call(operation, client, **kw):
        calls.append((operation, kw.get("name")))
        if operation is monitor.put_subscription:
            return SimpleNamespace(subscription_id=f"id-{kw['name']}", revision=2)
        if operation is monitor.get_status:
            return SimpleNamespace(state="FAILED", problems=["ed-patient-results: Spark said no"], subscriptions=[])
        return None  # cancel

    monkeypatch.setattr(monitor.api, "call", call)
    m = monitor.Monitor(None, None)
    with pytest.raises(monitor.HubCantRun):
        m.wait_active({"ed-patient-results": 2})

    m.census.registered({"patient_id": "MRN000001", "visit_id": "V1"}, 0.0)  # census v1: one edit per patient rule
    stop = threading.Event()
    threading.Thread(target=m.apply_loop, args=(stop,), daemon=True).start()  # as main() runs it
    started = time.monotonic()
    monitor.listen(m, _Quiet(), stop, time.time() + 30)
    assert time.monotonic() - started < 10, "the main thread stops listening once the apply thread fails"
    assert m.failure == (3, "the hub can't run ['ed-patient-results: Spark said no']")

    code, message = monitor.finish(m)
    assert (code, message) == m.failure, "exit 3 from the main thread, not a silently ended apply thread"
    cancelled = {name for op, name in calls if op is monitor.cancel}
    assert cancelled == set(monitor.patient_rules.PATIENT_RULES) | set(monitor.patient_rules.BASE), "nothing left"


def test_an_api_error_during_a_census_edit_ends_the_run_too(monkeypatch):
    def call(operation, client, **kw):
        if operation is monitor.put_subscription:
            raise monitor.api.ApiError(500, None, b'{"code": "INTERNAL", "message": "registry unavailable"}')
        return None

    monkeypatch.setattr(monitor.api, "call", call)
    m = monitor.Monitor(None, None)
    m.census.registered({"patient_id": "MRN000001", "visit_id": "V1"}, 0.0)
    stop = threading.Event()
    m.apply_loop(stop)
    assert stop.is_set() and m.failure[0] == 3 and "registry unavailable" in m.failure[1]
