"""Synthetic Epic JSON events in the shape the outside system publishes, at a steady rate.

A small share of events are deliberately bad (malformed JSON, missing msg_control_id) so the
quarantine table has something to show.
"""

import argparse
import json
import random
import time
from datetime import UTC, datetime

from hl7rt import eventhubs

FACILITIES = ["FAC01", "FAC02", "FAC03"]
MESSAGE_TYPES = [("ADT^A04", 3), ("ORU^R01", 5), ("ORM^O01", 2), ("MDM^T02", 1)]
LABS = [("2823-3", "Potassium"), ("2345-7", "Glucose"), ("718-7", "Hemoglobin")]


def payload(n: int, rng: random.Random, bad_ratio: float = 0.01) -> tuple[str, str]:
    """Return (key, value) for event number n."""
    now = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    mrn = f"MRN{rng.randint(1, 500):05d}"
    if rng.random() < bad_ratio:
        bad = rng.choice(['{"schema": "EPIC", "columns": [', json.dumps({"schema": "EPIC", "columns": []})])
        return mrn, bad
    msg_type = rng.choices([t for t, _ in MESSAGE_TYPES], weights=[w for _, w in MESSAGE_TYPES])[0]
    fac = rng.choice(FACILITIES)
    cols = {
        "account_id": f"ACCT{n:09d}",
        "admit_dt_tm": now,
        "date_of_birth_dt": "19800101",
        "patient_class": "E" if msg_type == "ADT^A04" else "I",
        "patient_facility": fac,
        "patient_id": mrn,
        "patient_id_namespace": "EPIC",
        "patient_point_of_care": "ED",
        "sex": rng.choice(["F", "M"]),
        "visit_id": f"V{n:09d}",
        "msg_control_id": f"CTRL{n:010d}",
        "msg_dt_tm": now,
        "msg_id": f"MSG{n:010d}",
        "msg_sending_facility": fac,
        "msg_seq": str(n),
        "msg_type": msg_type,
    }
    if msg_type == "ORU^R01":
        code, desc = rng.choice(LABS)
        cols.update(observation_cd_system="LN", observation_cd=code, observation_cd_desc=desc)
    elif msg_type == "ORM^O01":
        cols.update(order_cd_system="CPT", order_cd="85025", order_cd_desc="Complete Blood Count")
    elif msg_type == "MDM^T02":
        cols.update(document_completion_status="AU")
    body = {
        "introspector": {"source": "epic"},
        "schema": "EPIC",
        "table": "HL7_EVENTS",
        "txType": "INSERT",
        "columns": [{"name": k, "value": v} for k, v in cols.items()],
    }
    return mrn, json.dumps(body)


def main() -> None:
    from confluent_kafka import Producer
    from databricks.sdk.runtime import dbutils

    p = argparse.ArgumentParser(description=__doc__)
    for a in ["--namespace", "--service-credential", "--topic"]:
        p.add_argument(a, required=True)
    p.add_argument("--rate", type=float, default=50, help="events per second")
    p.add_argument("--bad-ratio", type=float, default=0.01)
    args = p.parse_args()

    credential = dbutils.credentials.getServiceCredentialsProvider(args.service_credential)
    producer = Producer(eventhubs.producer_config(args.namespace, credential))
    rng, n, interval = random.Random(), 0, 1.0 / args.rate
    next_at = time.monotonic()
    while True:
        key, value = payload(n, rng, args.bad_ratio)
        producer.produce(args.topic, key=key, value=value)
        producer.poll(0)
        n += 1
        if n % 10_000 == 0:
            print(f"produced {n:,} events to {args.topic}", flush=True)
        next_at += interval
        time.sleep(max(0.0, next_at - time.monotonic()))


if __name__ == "__main__":
    main()
