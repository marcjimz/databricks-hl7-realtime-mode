"""Dynamic patient monitoring (patient_rules.py), run from the repo root as an onboarded subscriber app:

    uv run python examples/subscribers/python/monitor.py [file.properties] [--duration=600]

Registers the base rule, then follows each ED registration it receives: the patient joins the patient rules, is
evaluated on their chief complaint, and leaves on discharge, transfer, or not qualifying. Every census change is applied
as one edit per patient rule (putSubscription, nothing else touched) and timed until the hub runs it (ACTIVE). It prints
each change, each patient record it receives, and a summary: edit -> ACTIVE p50/p99, delivery (source -> app) p50/p99,
and any record delivered for a patient after their removal went live (there should be none).

It uses only subscriptions named ed-*, so it can share an app with others, but it is easiest on an app of its own.
Exit codes as subscriber.py.
"""

import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import build  # noqa: E402

build.ensure()

import api  # noqa: E402
import eventhubs  # noqa: E402
import notifications  # noqa: E402
import patient_rules  # noqa: E402
from hl7_subscriptions_client.api.subscriptions import cancel, get_connection, get_status, put_subscription  # noqa: E402
from hl7_subscriptions_client.models import One  # noqa: E402
from subscriber import api_url, bearer, duration, exit_with, first_line, settings, sign_in  # noqa: E402

APPLY_EVERY = 1.0  # seconds: census changes in this window are one edit per rule
WAIT_SECONDS = 60


class Monitor:
    def __init__(self, client, template: str | None):
        self.client, self.template = client, template
        self.census = patient_rules.Census()
        self.lock = threading.Lock()
        self.applied = 0  # census version the hub runs
        self.live_since: dict[str, int] = {}  # subscription name -> its revision the hub runs
        self.removed_rev: dict[tuple[str, str], int] = {}  # (rule, mrn) -> first revision without the patient
        self.ids: dict[str, str] = {}  # subscriptionId -> name
        self.edit_to_active: list[float] = []
        self.delivery_ms: list[float] = []
        self.after_removal: list[str] = []
        self.received = 0

    def put(self, name: str, sql: str | None):
        if sql is None:
            try:
                api.call(cancel, self.client, name=name)
            except api.ApiError as e:
                if e.status != 404:
                    raise
            return None
        body = One(sql=sql, template=self.template) if self.template else One(sql=sql)  # tier: realtime
        r = api.call(put_subscription, self.client, name=name, body=body)
        self.ids[r.subscription_id] = name
        return r

    def wait_active(self, revisions: dict[str, int]) -> float | None:
        """Seconds until the hub runs these revisions (name -> revision), or None if not within WAIT_SECONDS."""
        started = time.monotonic()
        while time.monotonic() - started < WAIT_SECONDS:
            status = api.call(get_status, self.client, wait=10)
            live = {s.name: s.revision for s in status.subscriptions if s.enabled and s.state == "ACTIVE"}
            if all(live.get(n) == r for n, r in revisions.items()):
                return time.monotonic() - started
            failed = [p for p in status.problems if p.split(":")[0] in revisions] if status.state == "FAILED" else []
            if failed:
                exit_with(3, f"the hub can't run {failed}")
        return None

    def apply_loop(self, stop: threading.Event):
        """Apply each new census version to the patient rules, then time it until the hub runs it."""
        while not stop.is_set():
            with self.lock:
                version, ids, removed = self.census.version, self.census.ids(), dict(self.census.removed)
            if version == self.applied:
                stop.wait(APPLY_EVERY)
                continue
            started = time.monotonic()
            revisions = {}
            wanted = patient_rules.patient_rules(ids)
            with ThreadPoolExecutor(len(wanted)) as pool:  # one edit per rule, at once
                done = dict(zip(wanted, pool.map(lambda kv: self.put(*kv), wanted.items()), strict=True))
            for name, r in done.items():
                if r is not None:
                    revisions[name] = r.revision
                    for mrn in removed:
                        self.removed_rev.setdefault((name, mrn), r.revision)
            put_s = time.monotonic() - started
            took = self.wait_active(revisions) if revisions else 0.0
            self.applied = version
            if took is None:
                print(f"WARNING     census v{version} not ACTIVE within {WAIT_SECONDS} s", flush=True)
                continue
            total = put_s + took
            self.edit_to_active.append(total)
            print(f"live        census v{version}: {len(ids)} patients, {len(revisions)} rules, edit -> ACTIVE "
                  f"{total:.1f} s (put {put_s:.1f} s)", flush=True)  # fmt: skip

    def on_record(self, value, now: float):
        n = notifications.parse(value)
        name = self.ids.get(n.subscription_id) or n.subscription
        payload = n.payload.to_dict()
        mrn = payload.get("patient_id")
        with self.lock:
            if name in patient_rules.BASE:
                what = self.census.registered(payload, now)
                if what:
                    for rule in patient_rules.PATIENT_RULES:
                        self.removed_rev.pop((rule, mrn), None)
                    print(f"registered  {mrn} at {payload.get('patient_point_of_care')}: "
                          f"{payload.get('chief_complaint')!r} -> watching", flush=True)  # fmt: skip
                return
            if name not in patient_rules.PATIENT_RULES:
                return
            self.received += 1
            self.delivery_ms.append(now * 1000 - datetime.fromisoformat(n.source_ts).timestamp() * 1000)
            gone = self.removed_rev.get((name, mrn))
            if gone is not None and n.revision >= gone:
                self.after_removal.append(f"{name} rev {n.revision} {mrn}")
            what = self.census.on_patient_record(name, payload)
        print(f"{n.source_ts}  {mrn}  {name:<22} {notifications.payload_json(n)[:110]}", flush=True)
        if what:
            print(f"{what:<11} {mrn} ({len(self.census.patients)} watched)", flush=True)

    def summary(self):
        def pct(xs, q):
            if len(xs) < 2:
                return xs[0] if xs else float("nan")
            return statistics.quantiles(xs, n=100, method="inclusive")[q - 1]

        e, d = self.edit_to_active, self.delivery_ms
        print(f"summary     {len(e)} census edits: edit -> ACTIVE p50 {pct(e, 50):.1f} s, p99 {pct(e, 99):.1f} s, "
              f"max {max(e, default=float('nan')):.1f} s", flush=True)  # fmt: skip
        print(f"summary     {self.received} patient records: delivery p50 {pct(d, 50):.0f} ms, p99 {pct(d, 99):.0f} "
              f"ms, max {max(d, default=float('nan')):.0f} ms", flush=True)  # fmt: skip
        print(f"summary     {len(self.after_removal)} records after a removal went live"
              + (f": {self.after_removal[:5]}" if self.after_removal else ""), flush=True)  # fmt: skip
        return 0 if not self.after_removal else 1


def main(argv: list[str]):
    cfg = settings(argv)
    workspace = sign_in(cfg)
    client = api.connect(api_url(workspace, cfg), lambda: bearer(workspace))
    try:
        connection = api.call(get_connection, client)
    except api.ApiError as e:
        exit_with(6 if e.status in (403, 503) else 3, f"{e.code}: {e.message}")
    template = cfg.get("monitor.template", patient_rules.TEMPLATE) or None
    m = Monitor(client, template)
    base = {}
    try:
        for name, sql in patient_rules.BASE.items():
            base[name] = m.put(name, sql).revision
            print(f"subscribed  {name} revision {base[name]}" + (f" (template {template})" if template else ""))
        for name, sql in patient_rules.patient_rules([]).items():
            m.put(name, sql)  # start with an empty census
    except api.ApiError as e:
        for r in e.refusals:
            print(f"refused     {r.name if isinstance(r.name, str) else '':<22} {r.code}: {first_line(r.message)}")
        exit_with(2, f"{e.code}: {first_line(e.message)}")
    if m.wait_active(base) is None:
        exit_with(4, "the base rule wasn't ACTIVE in time (are the subscription hub pipelines running?)")
    print("hub         base rule ACTIVE; following ED registrations", flush=True)

    from confluent_kafka import Consumer, KafkaError, KafkaException

    tokens = eventhubs.tokens_for(workspace, connection.credential, connection.token_scope)
    group = cfg.get("consumer.group") or f"{cfg.get('databricks.profile') or 'subscriber'}-monitor"
    consumer = Consumer(eventhubs.consumer_config(connection.bootstrap_servers, group, tokens))
    consumer.subscribe([connection.topic])
    stop = threading.Event()
    applier = threading.Thread(target=m.apply_loop, args=(stop,), daemon=True)
    applier.start()
    run_for = duration(argv) or float("inf")
    stop_at = time.time() + run_for
    try:
        while time.time() < stop_at:
            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                m.on_record(msg.value(), time.time())
    except KeyboardInterrupt:
        pass
    stop.set()
    consumer.close()
    code = m.summary()
    for name in patient_rules.PATIENT_RULES + tuple(patient_rules.BASE):
        m.put(name, None)  # leave nothing behind
    exit_with(code, "stopped; ed-* subscriptions cancelled")


if __name__ == "__main__":
    main(sys.argv[1:])
