"""The Python example subscriber (examples/subscribers/python): its client is generated from docs/api/openapi.json and
must work against the real Subscription API; its delivery rules match the Java example's."""

import io
import json
import re
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("confluent_kafka")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "examples/subscribers/python"))

import build  # noqa: E402

build.ensure()

import api  # noqa: E402
import eventhubs  # noqa: E402
import notifications  # noqa: E402
import subscriber  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from hl7_subscriptions_client.api.subscriptions import (  # noqa: E402
    cancel,
    declare,
    get_connection,
    get_status,
    validate,
)

from subscription_api import app as server  # noqa: E402
from test_subscription_api import POTASSIUM, fake_databricks  # noqa: E402

NOW = 1_793_880_000.0  # 2026-11-05T12:00:00Z


def record(nid, sid, revision, source_ts="2026-11-05T11:59:57Z", **payload):
    return json.dumps({"notificationId": nid, "subscriptionId": sid, "subscription": "critical-k", "revision": revision,
                       "source": "silver_observations", "sourcePartition": 0, "sourceOffset": 1, "source_ts": source_ts,
                       "payload": payload or {"value_num": 7.1}})  # fmt: skip


# --- The generated client ------------------------------------------------------------------------------------------
def test_every_operation_in_the_contract_is_generated():
    contract = json.loads((ROOT / "docs/api/openapi.json").read_text())
    ops = {op["operationId"] for path in contract["paths"].values() for op in path.values()}
    generated = {p.stem for p in (build.OUT / build.PACKAGE / "api").rglob("*.py") if p.stem != "__init__"}
    assert {re.sub(r"(?<!^)(?=[A-Z])", "_", o).lower() for o in ops} <= generated


def test_the_client_is_regenerated_when_the_contract_changes(tmp_path, monkeypatch):
    contract = tmp_path / "openapi.json"
    contract.write_text((ROOT / "docs/api/openapi.json").read_text())
    monkeypatch.setattr(build, "CONTRACT", contract)
    monkeypatch.setattr(build, "OUT", tmp_path / "generated")
    assert build.ensure() is True, "missing: generated"
    assert build.ensure() is False, "up to date: left alone"
    contract.touch()
    future = contract.stat().st_mtime + 10
    import os

    os.utime(contract, (future, future))
    assert build.ensure() is True, "the contract is newer: regenerated"


@pytest.fixture
def client(pg, monkeypatch):
    """The generated client against the real API in-process; the test client stands in for the Apps proxy."""
    monkeypatch.setattr(server, "pool", server.Pool(lambda: pg("api")))
    monkeypatch.setattr(server, "CATALOG", "cat")
    monkeypatch.setattr(server, "EVENTHUBS_NAMESPACE", "ns1")
    monkeypatch.setattr(server, "compile_with_databricks", fake_databricks)

    def as_app(sp):
        c = api.connect("https://testserver", lambda: "token")
        c.set_httpx_client(TestClient(server.app, base_url="https://testserver", headers={"X-Forwarded-Email": sp}))
        return c

    return as_app


def test_the_generated_client_runs_the_whole_handshake(client):
    c = client("sp-a")
    conn = api.call(get_connection, c)
    assert (conn.topic, conn.credential) == ("cat.subscribers.app-a", "sub_app_a_eventhubs")
    assert conn.bootstrap_servers == "ns1.servicebus.windows.net:9093"

    declared = api.call(declare, c, body=api.declared({"critical-k": POTASSIUM}))
    (accepted,) = declared.accepted
    assert accepted.state == "ACCEPTED" and accepted.destination == conn.topic

    status = api.call(get_status, c, wait=0)
    assert [(s.name, s.revision) for s in status.subscriptions] == [("critical-k", accepted.revision)]

    assert api.call(validate, c, body=api.declared({"critical-k": POTASSIUM})).valid is True
    assert api.call(cancel, c, name="critical-k").name == "critical-k"


def test_refusals_carry_databricks_errors_and_apply_nothing(client):
    c = client("sp-a")
    with pytest.raises(api.ApiError) as e:
        rules = {
            "ok": POTASSIUM,
            "typo": "SELECT patient_id FROM silver_observations WHERE value_numm > 6",
            "join": "SELECT o.patient_id FROM silver_observations o JOIN silver_encounters e "
            "ON o.patient_id = e.patient_id",
        }
        api.call(declare, c, body=api.declared(rules))
    assert e.value.status == 422
    assert {r.name: r.code.split(".")[0] for r in e.value.refusals} == {"typo": "UNRESOLVED_COLUMN",
                                                                         "join": "SQL_NOT_ALLOWED"}  # fmt: skip
    names = {s.name for s in api.call(get_status, c, wait=0).subscriptions if s.enabled}
    assert "ok" not in names, "one refusal applies nothing"


def test_an_app_that_isnt_onboarded_is_told_so(client):
    with pytest.raises(api.ApiError) as e:
        api.call(get_connection, client("not-onboarded"))
    assert (e.value.status, e.value.code) == (403, "APP_NOT_ONBOARDED")


def test_connect_sends_a_fresh_token_on_every_call_and_needs_https():
    import httpx2

    tokens, seen = iter(["t1", "t2"]), []
    c = api.connect("https://api.example", lambda: next(tokens))

    def answer(request):
        seen.append(request.headers["Authorization"])
        return httpx2.Response(200, json={"ok": True})

    auth = c.get_httpx_client().auth
    c.set_httpx_client(httpx2.Client(base_url="https://api.example", auth=auth, transport=httpx2.MockTransport(answer)))
    from hl7_subscriptions_client.api.service import health

    health.sync_detailed(client=c)
    health.sync_detailed(client=c)
    assert seen == ["Bearer t1", "Bearer t2"]
    with pytest.raises(ValueError):
        api.connect("http://api.example", lambda: "t")


# --- Delivery rules (the same as the Java example's Listener) -------------------------------------------------------
def listen(current=None):
    out = io.StringIO()
    return notifications.Listener(current or {"s1": 3}, "my-group", out, NOW), out


def test_live_matches_are_printed_once_each():
    lst, out = listen()
    lst.on_record(record("n1", "s1", 3), NOW)
    lst.on_record(record("n1", "s1", 3), NOW)  # redelivered: at-least-once
    lines = [x for x in out.getvalue().splitlines() if "critical-k" in x]
    assert len(lines) == 1 and lines[0].endswith('rev 3  {"value_num":7.1}')


def test_other_subscriptions_and_earlier_revisions_are_counted_as_skipped():
    lst, out = listen()
    lst.on_record(record("n1", "s1", 2), NOW)
    lst.on_record(record("n2", "s0", 3), NOW)
    lst.on_tick(NOW + 11)
    assert "skipped     2 notifications for other subscriptions or earlier revisions" in out.getvalue()
    assert "critical-k" not in out.getvalue()


def test_the_backlog_is_summarized_then_live_ones_printed():
    lst, out = listen()
    lst.on_record(record("n1", "s1", 3, "2026-11-05T11:00:00Z"), NOW)
    lst.on_record(record("n2", "s1", 3, "2026-11-05T11:01:40Z"), NOW)
    lst.on_record(record("n3", "s1", 3, "2026-11-05T11:59:55Z"), NOW)
    lst.on_record(record("n4", "s1", 3, "2026-11-05T11:03:20Z"), NOW)  # a late old one after going live
    lines = out.getvalue().splitlines()
    assert lines[0].startswith("catching up from 2026-11-05T11:00:00Z, where consumer group my-group")
    assert lines[1].startswith("caught up   2 backlog notifications")
    assert sum("critical-k" in x for x in lines) == 2, "live from here: every match"
    assert sum(x.startswith("caught up") for x in lines) == 1, "caught up once, never flaps"


def test_catch_up_progress_is_reported():
    lst, out = listen()
    lst.on_record(record("n1", "s1", 3, "2026-11-05T11:00:00Z"), NOW)
    lst.on_tick(NOW + 11)
    assert "catching up 1 so far, at 2026-11-05T11:00:00Z" in out.getvalue()


def test_the_hub_envelope_parses_into_the_contract_model():
    n = notifications.parse(record("n1", "s1", 3, patient_id="P1", value_num=6.4))
    assert (n.notification_id, n.subscription, n.revision) == ("n1", "critical-k", 3)
    assert notifications.payload_json(n) == '{"patient_id":"P1","value_num":6.4}', "printed as the hub wrote it"
    assert notifications.is_current(n, {"s1": 3})
    assert not notifications.is_current(n, {"s1": 2}), "an earlier revision still in the hub's backlog"
    assert not notifications.is_current(n, {"s0": 3}), "a cancelled or replaced subscription with the same name"
    assert notifications.is_backlog(n, NOW + 300, 120) and not notifications.is_backlog(n, NOW, 120)
    with pytest.raises(ValueError):
        notifications.parse('{"payload": {}}')


# --- Event Hubs: a Unity Catalog token, Microsoft's client settings, no keys ----------------------------------------
AZURE_IDLE_MS = 240_000


def test_the_token_is_cached_until_close_to_expiry():
    clock, fetched = [NOW], []
    tokens = eventhubs.Tokens(lambda: fetched.append(1) or (f"t{len(fetched)}", clock[0] + 3600), lambda: clock[0])
    assert tokens.get()[0] == "t1"
    clock[0] += 3600 - 301
    assert tokens.get()[0] == "t1"
    clock[0] += 2
    assert tokens.get()[0] == "t2", "refreshed inside the 5-minute margin"
    with pytest.raises(RuntimeError):
        eventhubs.Tokens(lambda: ("", NOW + 3600), lambda: NOW).get()


def test_the_consumer_uses_entra_tokens_and_follows_event_hubs_guidance():
    cfg = eventhubs.consumer_config(
        "ns1.servicebus.windows.net:9093", "g1", eventhubs.Tokens(lambda: ("t", NOW + 3600))
    )
    assert cfg["security.protocol"] == "SASL_SSL" and cfg["sasl.mechanism"] == "OAUTHBEARER"
    assert cfg["oauth_cb"](None) == ("t", NOW + 3600)
    assert int(cfg["metadata.max.age.ms"]) < AZURE_IDLE_MS and int(cfg["connections.max.idle.ms"]) < AZURE_IDLE_MS
    assert cfg["enable.auto.commit"] is False
    assert not {k for k in cfg if "password" in k or "username" in k}, "no keys or connection strings"


# --- Settings ---------------------------------------------------------------------------------------------------------
def test_settings_are_found_in_the_shared_apps_folder(tmp_path):
    apps = tmp_path / "apps"
    apps.mkdir()
    (apps / "sp-app-a.properties").write_text("databricks.profile = sp-app-a\napi.app = hl7-subscriptions-dev\n")
    assert subscriber.settings_file(None, [tmp_path]) == apps / "sp-app-a.properties"
    (apps / "sp-app-b.properties").write_text("")
    with pytest.raises(ValueError, match="several apps"):
        subscriber.settings_file(None, [tmp_path])
    assert subscriber.settings_file(apps / "sp-app-b.properties", [tmp_path]) == apps / "sp-app-b.properties"
    assert subscriber.settings_file(None, [tmp_path / "nowhere"]) is None


def test_arguments_override_the_file_and_secrets_are_refused(tmp_path):
    f = tmp_path / "x.properties"
    f.write_text("# a comment\napi.app = a\nconsumer.group = g1\n")
    cfg = subscriber.settings([str(f), "consumer.group=g2", "--duration=40"])
    assert (cfg["api.app"], cfg["consumer.group"]) == ("a", "g2") and "--duration" not in cfg
    with pytest.raises(ValueError, match="secret"):
        subscriber.settings([str(f), "client_secret=x"])


def test_the_api_is_found_by_app_name_unless_its_url_is_set():
    assert subscriber.api_url(None, {"api.url": " https://api "}) == "https://api", "api.url wins: no lookup"
    with pytest.raises(ValueError, match="api.app"):
        subscriber.api_url(None, {})


def test_only_a_service_principal_may_run_the_app():
    assert subscriber.is_service_principal("38fafc55-d28b-4f48-8d17-299f8d2698ee")
    assert not subscriber.is_service_principal("someone@example.com")


def test_durations_read_like_the_java_examples():
    assert subscriber.duration(["--duration=40"]) == 40
    assert subscriber.duration(["--duration=PT1M30S"]) == 90
    assert subscriber.duration([]) is None
