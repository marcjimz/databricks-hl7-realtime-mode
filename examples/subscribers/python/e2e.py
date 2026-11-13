"""End-to-end checks against the live Subscription API, hub, and Event Hubs, as this app's service principal: the same
25 checks as the Java example's e2e/E2E. Every check has an input, an expected result, and the actual result; the run
prints them as a table and exits 1 if any fails.

    uv run python examples/subscribers/python/subscriber.py --e2e

It only uses subscriptions named e2e-* (one at a time, cancelled at the end) and its own consumer group, so it leaves
the app's own subscriptions alone. Settings (written by `make registry`): e2e.forbidden_credential, e2e.forbidden_topic,
e2e.own_topic, e2e.base_schema, e2e.data_api_url.
"""

import contextlib
import time
import uuid
from datetime import UTC, datetime, timedelta

import api
import eventhubs
import httpx2
import notifications
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from hl7_subscriptions_client.api.subscriptions import cancel, declare, get_status, put_subscription, validate
from hl7_subscriptions_client.models import One
from subscriber import bearer, first_line, is_service_principal

WAIT = 180  # seconds
GOOD = ("SELECT patient_id, value_num FROM silver_observations "
        "WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5")  # fmt: skip
CHANGED = ("SELECT patient_id, value_num, units FROM silver_observations "
           "WHERE observation_cd_desc = 'Potassium' AND value_num >= 7.0")  # fmt: skip


class E2E:
    def __init__(self, workspace, client, connection, cfg):
        self.workspace, self.client, self.connection, self.cfg = workspace, client, connection, cfg
        self.checks: list[tuple[str, str, str, str, bool]] = []

    def setting(self, key):
        return (self.cfg.get(key) or "").strip() or None

    def record(self, name, input_, expected, actual, passed):
        passed = passed(actual) if callable(passed) else bool(passed)
        self.checks.append((name, input_, expected, actual, passed))
        print(f"{'PASS' if passed else 'FAIL'}  {name}", flush=True)

    def all(self):
        c, conn = self.client, self.connection
        me = self.workspace.current_user.me().user_name
        profile = self.cfg.get("databricks.profile", "(ambient identity)")
        self.record("Sign in", f"profile {profile}", "a service principal", me, is_service_principal(me))

        # Isolation on Event Hubs and the registry: only this app's own credential and hub, read-only; no way around.
        own = self.setting("e2e.own_topic")
        self.record("Connection from the API", "GET /api/v1/connection", "this app's own topic and credential",
                    f"{conn.topic} via {conn.credential} @ {conn.bootstrap_servers}",
                    (own is None or own == conn.topic) and bool(conn.credential))  # fmt: skip
        scope = conn.token_scope
        self.record("Own Event Hubs credential", conn.credential, "token issued",
                    attempt(lambda: "token, expires " + iso(eventhubs.mint(self.workspace, conn.credential, scope)[1])),
                    lambda s: s.startswith("token"))  # fmt: skip
        forbidden_credential = self.setting("e2e.forbidden_credential")
        if forbidden_credential:
            self.record("Another credential", forbidden_credential, "denied (no ACCESS)",
                        attempt(lambda: eventhubs.mint(self.workspace, forbidden_credential, scope) and "token issued"),
                        lambda s: "ACCESS" in s or "PERMISSION_DENIED" in s or "does not have" in s)  # fmt: skip
        self.tokens = eventhubs.tokens_for(self.workspace, conn.credential, scope)
        forbidden_topic = self.setting("e2e.forbidden_topic")
        if forbidden_topic:
            self.record("Read a hub that isn't this app's", forbidden_topic, "not authorized",
                        self.read_once(forbidden_topic), lambda s: "not authorized" in s)  # fmt: skip
        self.record("Write to its own hub (forge a notification)", conn.topic, "not authorized (Receiver only)",
                    self.write_once(conn.topic), lambda s: "not authorized" in s)  # fmt: skip
        data_api = self.setting("e2e.data_api_url")
        if data_api:

            def skip_the_api():
                r = httpx2.get(f"{data_api}/subscription_api/rpc/list_subscriptions", timeout=30,
                               headers={"Authorization": f"Bearer {bearer(self.workspace)}"})  # fmt: skip
                return f"answered {r.status_code}" if r.status_code // 100 == 2 else f"refused {r.status_code}"

            self.record("Skip the API (the registry's Data API)", f"list_subscriptions via {data_api}", "not available",
                        attempt(skip_the_api), lambda s: not s.startswith("answered"))  # fmt: skip

        # Refused in the declare call itself: Databricks' own errors, or a shape the hub won't run.
        self.refused("Column not granted", "SELECT patient_id FROM silver_observations WHERE msg_sending_facility = "
                     "'NORTH'", "UNRESOLVED_COLUMN")  # fmt: skip
        self.refused("Source not granted", "SELECT patient_id FROM silver_encounters", "TABLE_OR_VIEW_NOT_FOUND")
        self.refused("Misspelled column", "SELECT patient_id FROM silver_observations WHERE value_numm > 6",
                     "UNRESOLVED_COLUMN")  # fmt: skip
        self.refused(
            "Syntax error", "SELECT patient_id FROM silver_observations WHER value_num > 6", "PARSE_SYNTAX_ERROR"
        )
        self.refused("Subquery", "SELECT patient_id FROM silver_observations WHERE value_num > "
                     "(SELECT max(value_num) FROM silver_observations)", "SQL_NOT_ALLOWED")  # fmt: skip
        self.refused("Join", "SELECT o.patient_id FROM silver_observations o JOIN silver_encounters e ON "
                     "o.patient_id = e.patient_id", "SQL_NOT_ALLOWED")  # fmt: skip
        self.refused("Function that reaches outside the record", "SELECT patient_id FROM silver_observations WHERE "
                     "http_request('GET', 'https://example.com') IS NOT NULL", "SQL_NOT_ALLOWED")  # fmt: skip
        base = self.setting("e2e.base_schema")
        if base:
            self.refused("Table outside this app's views", f"SELECT patient_id FROM {base}.silver_observations",
                         "SQL_NOT_ALLOWED")  # fmt: skip
        try:
            api.call(declare, c, body=api.declared({"e2e-ok": GOOD, "e2e-bad": "SELECT patient_id FROM "
                                                    "silver_observations WHERE value_numm > 6"}))  # fmt: skip
            atomic = 200
        except api.ApiError as e:
            atomic = e.status
        ok = self.registered("e2e-ok")
        self.record("One refusal applies nothing", "declare e2e-ok (valid) + e2e-bad",
                    "422, e2e-ok not registered", f"{atomic}, e2e-ok {'registered' if ok else 'not registered'}",
                    atomic == 422 and not ok)  # fmt: skip
        valid = api.call(validate, c, body=api.declared({"e2e-ok": GOOD})).valid is True
        ok = self.registered("e2e-ok")
        self.record(
            "Validate applies nothing",
            "validate e2e-ok",
            "valid, not registered",
            f"{'valid' if valid else 'invalid'}, {'registered' if ok else 'not registered'}",
            valid and not ok,
        )

        # Register, receive, change, receive the change, cancel.
        group = f"{self.cfg.get('databricks.profile', 'subscriber')}-e2e-{uuid.uuid4()}"
        consumer = Consumer(eventhubs.consumer_config(conn.bootstrap_servers, group, self.tokens))
        try:
            r1 = api.call(put_subscription, c, name="e2e-potassium", body=One(sql=GOOD))
            self.record("Register", GOOD, "ACCEPTED, canonical SQL", f"{r1.state} revision {r1.revision}: {r1.query}",
                        r1.state == "ACCEPTED")  # fmt: skip
            self.record(
                "Hub runs it",
                f"e2e-potassium revision {r1.revision}",
                "ACTIVE",
                self.await_state(r1.subscription_id, r1.revision, "ACTIVE"),
                lambda s: s == "ACTIVE",
            )
            consumer.subscribe([r1.destination])
            n1 = self.await_notification(consumer, r1)
            self.record(
                "Notification on this app's hub",
                r1.destination,
                "payload {patient_id, value_num}, value_num >= 6.5",
                describe(n1),
                n1 is not None and keys(n1) == {"patient_id", "value_num"} and number(n1, "value_num") >= 6.5,
            )

            again = api.call(put_subscription, c, name="e2e-potassium", body=One(sql=GOOD))
            self.record("Same SQL again", GOOD, "same revision, unchanged",
                        f"revision {again.revision}, {'unchanged' if again.unchanged else 'changed'}",
                        again.revision == r1.revision and again.unchanged is True)  # fmt: skip

            r2 = api.call(put_subscription, c, name="e2e-potassium", body=One(sql=CHANGED))
            state = self.await_state(r2.subscription_id, r2.revision, "ACTIVE")
            self.record("Change the rule", CHANGED, "next revision, then ACTIVE",
                        f"revision {r2.revision}, then {state}", r2.revision == r1.revision + 1)  # fmt: skip
            n2 = self.await_notification(consumer, r2)
            self.record("Notification reflects the change", r2.destination,
                        f"revision {r2.revision}, payload {{patient_id, value_num, units}}, value_num >= 7.0",
                        describe(n2), n2 is not None and keys(n2) == {"patient_id", "value_num", "units"}
                        and number(n2, "value_num") >= 7.0)  # fmt: skip

            cancelled = api.call(cancel, c, name="e2e-potassium")
            self.record("Cancel", "DELETE e2e-potassium", "CANCELLED by the hub",
                        self.await_state(cancelled.subscription_id, cancelled.revision, "CANCELLED"),
                        lambda s: s == "CANCELLED")  # fmt: skip
        finally:
            consumer.close()
        hub = api.call(get_status, c, wait=0).hub.additional_properties
        last = hub.get("silver_observations", "never")
        self.record("Hub heartbeat", "silver_observations", "a batch within 2 minutes", last,
                    lambda s: s != "never" and datetime.fromisoformat(s) > datetime.now(UTC)
                    - timedelta(seconds=120))  # fmt: skip

    def refused(self, name, sql, code):
        try:
            api.call(put_subscription, self.client, name="e2e-refused", body=One(sql=sql))
            actual = "ACCEPTED"
        except api.ApiError as e:
            first = (e.refusals or [None])[0]
            actual = f"{first.code}: {first_line(first.message)}" if first else f"{e.code}: {first_line(e.message)}"
        self.record(name, sql, code, actual, actual.startswith(code))

    def registered(self, name) -> bool:
        return any(s.name == name and s.enabled for s in api.call(get_status, self.client, wait=0).subscriptions)

    def await_state(self, subscription_id, revision, wanted) -> str:
        deadline, last = time.monotonic() + WAIT, "PENDING"
        while time.monotonic() < deadline:
            for s in api.call(get_status, self.client, wait=0).subscriptions:
                if s.subscription_id == subscription_id and s.revision == revision:
                    last = s.state
            if last in (wanted, "FAILED"):
                return last
            time.sleep(1)
        return f"{last} after {WAIT // 60} min"

    @staticmethod
    def await_notification(consumer, receipt):
        deadline, current = time.monotonic() + WAIT, {receipt.subscription_id: receipt.revision}
        while time.monotonic() < deadline:
            for m in consumer.consume(num_messages=500, timeout=1.0):
                if m.error():
                    continue
                n = notifications.parse(m.value())
                if notifications.is_current(n, current):
                    return n
        return None

    def read_once(self, topic) -> str:
        """Try to read `topic`. librdkafka reports a topic it may not read only in the topic's metadata (subscribing
        alone just stays silent), so ask for that first, then try to read records."""
        consumer = Consumer(eventhubs.consumer_config(self.connection.bootstrap_servers,
                                                      f"e2e-forbidden-{uuid.uuid4()}", self.tokens))  # fmt: skip
        try:
            meta = consumer.list_topics(topic, timeout=30).topics.get(topic)
            if meta is not None and meta.error is not None:
                if meta.error.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                    return f"not authorized: {meta.error.name()}"
                return f"{meta.error.name()}: {meta.error.str()}"
            consumer.subscribe([topic])
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                m = consumer.poll(2.0)
                if m is None:
                    continue
                if m.error():
                    if m.error().code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                        return f"not authorized: {m.error().name()}"
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    return f"{m.error().name()}: {m.error().str()}"
                return "read records"
            return "no error and no records in 60 s"
        except KafkaException as e:
            return not_authorized(e)
        finally:
            consumer.close()

    def write_once(self, topic) -> str:
        producer, result = Producer(eventhubs.producer_config(self.connection.bootstrap_servers, self.tokens)), []
        try:
            producer.produce(topic, key="e2e", value='{"forged":true}', on_delivery=lambda err, _m: result.append(err))
            producer.flush(45)
        except KafkaException as e:
            return not_authorized(e)
        if not result:
            return "no delivery report in 45 s"
        err = result[0]
        if err is None:
            return "written"
        if err.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            return f"not authorized: {err.name()}"
        return f"{err.name()}: {err.str()}"

    def print(self):
        print("\n| # | Check | Input | Expected | Actual | Result |\n|---|---|---|---|---|---|")
        for i, (name, input_, expected, actual, passed) in enumerate(self.checks, 1):
            print(f"| {i} | {name} | {cell(input_)} | {cell(expected)} | {cell(actual)} | "
                  f"{'PASS' if passed else '**FAIL**'} |")  # fmt: skip
        print(f"\n{len(self.checks)} checks, {sum(not c[4] for c in self.checks)} failed")


def run(workspace, client, connection, cfg) -> int:
    e2e = E2E(workspace, client, connection, cfg)
    try:
        e2e.all()
    except Exception as e:  # noqa: BLE001
        e2e.record("run", "-", "completes", f"aborted: {e!r}", False)
    finally:
        for name in ("e2e-potassium", "e2e-ok"):
            with contextlib.suppress(api.ApiError):  # 404: nothing to clean up
                api.call(cancel, client, name=name)
    e2e.print()
    return 0 if all(c[4] for c in e2e.checks) else 1


def attempt(fn) -> str:
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {e}"


def not_authorized(e: KafkaException) -> str:
    err = e.args[0] if e.args and isinstance(e.args[0], KafkaError) else None
    if err is not None and err.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
        return f"not authorized: {err.name()}"
    return f"KafkaException: {e}"


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat().replace("+00:00", "Z")


def keys(n) -> set[str]:
    return set(n.payload.to_dict())


def number(n, field) -> float:
    v = n.payload.to_dict().get(field)
    return float(v) if isinstance(v, int | float) else float("nan")


def describe(n) -> str:
    return f"none within {WAIT // 60} min" if n is None else f"revision {n.revision} {notifications.payload_json(n)}"


def cell(s) -> str:
    t = ("" if s is None else str(s)).replace("|", "\\|").replace("\n", " ")
    return t[:157] + "..." if len(t) > 160 else t
