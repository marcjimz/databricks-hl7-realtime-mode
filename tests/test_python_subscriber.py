"""The Python example subscriber (examples/subscribers/python): its client is generated from docs/api/openapi.json and
must work against the real Subscription API; its delivery rules match the Java example's."""

import io
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

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


def test_the_connection_names_the_apps_hub_and_its_grant(client):
    """What the e2e reads: the hub that runs the app's realtime rules (edp_shared unless the platform assigned it its
    own), the sources and columns it may read, and that hub's heartbeats."""
    c = client("sp-a")
    conn = api.call(get_connection, c)
    assert conn.hub == "edp_shared"
    assert list(conn.sources) == ["silver_observations"]
    assert {"patient_id", "value_num", "observation_cd_desc"} <= set(conn.fields)
    status = api.call(get_status, c, wait=0)
    assert status.hub_name == "edp_shared" and isinstance(status.heartbeats, list)


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


def test_the_producer_authenticates_like_the_consumer_and_is_not_a_consumer():
    tokens = eventhubs.Tokens(lambda: ("t", NOW + 3600))
    p = eventhubs.producer_config("ns1.servicebus.windows.net:9093", tokens)
    c = eventhubs.consumer_config("ns1.servicebus.windows.net:9093", "g1", tokens)
    for k in ("bootstrap.servers", "security.protocol", "sasl.mechanism", "metadata.max.age.ms",
              "connections.max.idle.ms"):  # fmt: skip
        assert p[k] == c[k], k
    assert not {"group.id", "enable.auto.commit", "auto.offset.reset", "session.timeout.ms"} & set(p)
    assert (p["request.timeout.ms"], p["message.timeout.ms"]) == (20_000, 30_000), "as the Java example's producer()"


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


def test_settings_need_no_file(tmp_path, monkeypatch):
    """A notebook has no .properties: key=value arguments are all the settings."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subscriber, "HERE", tmp_path / "examples/subscribers/python")
    cfg = subscriber.settings(["api.app=hl7-subscriptions-dev", "identity=user", "consumer.group=me-notebook",
                               "--duration=300"])  # fmt: skip
    assert cfg == {"api.app": "hl7-subscriptions-dev", "identity": "user", "consumer.group": "me-notebook"}


def test_only_a_service_principal_may_run_the_app():
    assert subscriber.is_service_principal("00000000-0000-4000-8000-000000000001")
    assert not subscriber.is_service_principal("someone@example.com")


# --- Who runs it: a service principal by default, a person only with identity=user (the Java example's signIn) -------
SP = "00000000-0000-4000-8000-000000000001"


@pytest.fixture
def signing_in(monkeypatch):
    """sign_in against a stand-in WorkspaceClient that signs in as `user_name`; returns how each one was built."""

    def as_(user_name, display_name="Someone"):
        built = []

        def workspace(**kwargs):
            built.append(kwargs)
            me = SimpleNamespace(user_name=user_name, display_name=display_name)
            return SimpleNamespace(current_user=SimpleNamespace(me=lambda: me))

        monkeypatch.setattr(subscriber, "WorkspaceClient", workspace)
        return built

    return as_


def test_a_person_is_refused_unless_identity_is_user(signing_in, capsys):
    built = signing_in("someone@example.com")
    with pytest.raises(SystemExit) as e:
        subscriber.sign_in({"databricks.profile": "me"})
    assert e.value.code == 5
    assert built == [{"profile": "me", "auth_type": "oauth-m2m"}], "a service principal's profile: OAuth M2M only"
    out = capsys.readouterr().out
    assert out.startswith("ERROR       signed in as someone@example.com, not a service principal: check profile me")
    assert "identity=user" in out, "says how to run as yourself"


def test_identity_user_signs_in_as_yourself(signing_in, capsys):
    built = signing_in("someone@example.com", "Some One")
    subscriber.sign_in({"databricks.profile": "me", "identity": "user"})
    assert built == [{"profile": "me"}], "a user's profile signs in its own way (databricks-cli, U2M): not pinned"
    assert capsys.readouterr().out == "signed in   as Some One (someone@example.com) as a user\n"

    built = signing_in("someone@example.com", "Some One")
    subscriber.sign_in({"identity": "user"})
    assert built == [{}], "no profile: the ambient identity, e.g. a notebook's user"


def test_identity_user_is_not_a_service_principal(signing_in, capsys):
    signing_in(SP)
    with pytest.raises(SystemExit) as e:
        subscriber.sign_in({"identity": "user"})
    assert e.value.code == 5
    assert f"signed in as {SP}, a service principal" in capsys.readouterr().out, "explicit both ways"


def test_identity_problems_read_like_the_java_examples():
    assert subscriber.identity_problem("service_principal", "someone@example.com", "me") == (
        "signed in as someone@example.com, not a service principal: check profile me "
        "(to run as yourself, set identity=user)")  # fmt: skip
    assert subscriber.identity_problem("user", SP, "") == (
        f"signed in as {SP}, a service principal, but identity=user (drop identity=user)")  # fmt: skip
    assert subscriber.identity_problem("user", "someone@example.com", "") is None
    assert subscriber.identity_problem("service_principal", SP, "") is None


def test_a_service_principal_signs_in_as_before(signing_in, capsys):
    built = signing_in(SP, "sp-hl7-app")
    subscriber.sign_in({})
    subscriber.sign_in({"identity": "service_principal"})
    assert built == [{}, {}]
    assert capsys.readouterr().out == f"signed in   as sp-hl7-app ({SP})\n" * 2
    with pytest.raises(ValueError, match="identity"):
        subscriber.sign_in({"identity": "person"})


# --- main(argv, rules): the rules a caller (a notebook) passes, else my_subscriptions.py ------------------------------
@pytest.fixture
def run_main(client, monkeypatch, tmp_path):
    """subscriber.main against the real API in-process, signed in as app `sp`, with no settings file around and no hub
    running (so a declare that is accepted ends with exit 4, not confirmed)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subscriber, "HERE", tmp_path / "examples/subscribers/python")
    monkeypatch.setattr(subscriber, "WAIT_SECONDS", 1)
    monkeypatch.setattr(subscriber, "sign_in", lambda cfg: None)

    def run(sp, argv, **kwargs):
        monkeypatch.setattr(subscriber, "api_client", lambda workspace, cfg: client(sp))
        with pytest.raises(SystemExit) as e:
            subscriber.main(argv, **kwargs)
        return e.value.code

    return run


def enabled(client, sp) -> set[str]:
    return {s.name for s in api.call(get_status, client(sp), wait=0).subscriptions if s.enabled}


def test_main_declares_the_rules_it_is_given_and_an_empty_set_cancels_them(run_main, client, capsys):
    assert run_main("sp-c", ["api.app=x", "identity=user"], rules={"critical-k": POTASSIUM}) == 4, "no hub here"
    assert "subscribed  critical-k             revision 1 ACCEPTED" in capsys.readouterr().out
    assert enabled(client, "sp-c") == {"critical-k"}

    assert run_main("sp-c", ["api.app=x", "identity=user"], rules={}) == 0
    out = capsys.readouterr().out
    assert "cancelled   critical-k             (no longer in your rules)" in out
    assert out.rstrip().endswith("done        no subscriptions declared in your rules")
    assert enabled(client, "sp-c") == set(), "nothing left behind"


def test_main_without_rules_declares_my_subscriptions(run_main, client, monkeypatch, capsys):
    monkeypatch.setattr(subscriber.my_subscriptions, "all", lambda: {"from-the-file": POTASSIUM})
    assert run_main("sp-c", []) == 4
    assert "subscribed  from-the-file" in capsys.readouterr().out
    assert enabled(client, "sp-c") == {"from-the-file"}


def test_a_refusal_names_where_the_rules_came_from(run_main, monkeypatch, capsys):
    typo = {"typo": "SELECT patient_id FROM silver_observations WHERE value_numm > 6"}
    assert run_main("sp-c", [], rules=typo) == 2
    assert capsys.readouterr().out.rstrip().endswith("1 subscription(s) refused; fix your rules")
    monkeypatch.setattr(subscriber.my_subscriptions, "all", lambda: typo)
    assert run_main("sp-c", []) == 2
    assert capsys.readouterr().out.rstrip().endswith("1 subscription(s) refused; fix my_subscriptions.py")


def test_durations_read_like_the_java_examples():
    assert subscriber.duration(["--duration=40"]) == 40
    assert subscriber.duration(["--duration=PT1M30S"]) == 90
    assert subscriber.duration([]) is None
