"""Notifications built on the driver, byte-identical to the Spark envelope (hl7rt.subscriptions._envelope).

The Spark envelope's value is to_json(struct(notificationId, subscriptionId, subscription, revision, source,
sourcePartition, sourceOffset, source_ts, payload)), with payload = parse_json(<the rule's fields as JSON>): a VARIANT,
whose to_json renders object keys sorted and each value as Spark renders it (1.0E7, "NaN", ...), whatever its siblings.
So the batch read renders each selected column's member once, with Spark (members()), and the driver only joins
members: no number or timestamp is formatted in Python (tests/test_matching.py compares the bytes).
"""

import hashlib
import json

from pyspark.sql import Column
from pyspark.sql import functions as F

from hl7rt import subscriptions
from subscription_api.rules import identifier, literal

PREFIX = "_m_"  # collected column holding a field's rendered member ("name":value)
TS = "_m__source_ts"


def _strip(js: str) -> str:
    return f"substring({js}, 2, char_length({js}) - 2)"  # {"a":1} -> "a":1


def members(source: str, columns=None) -> list[Column]:
    """Columns to collect with a batch of subscriptions.events: each field's member as the envelope's payload renders
    it (`columns`: the fields some rule selects; all of the source's by default), and source_ts as the envelope does."""
    out = []
    for c in [f.name for f in subscriptions.SOURCES[source]] if columns is None else sorted(columns):
        js = (f"to_json(parse_json(to_json(named_struct({literal(c)}, {identifier(c)}), "
              f"map('ignoreNullFields', 'false'))))")  # fmt: skip
        out.append(F.expr(_strip(js)).alias(PREFIX + c))
    out.append(F.expr(_strip("to_json(named_struct('source_ts', _source_ts))")).alias(TS))
    return out


def notification_id(subscription_id: str, revision: int, partition: int, offset: int) -> str:
    """Stable across redeliveries: the envelope's sha2(concat_ws(':', sid, rev, partition, offset), 256)."""
    return hashlib.sha256(f"{subscription_id}:{int(revision)}:{partition}:{offset}".encode()).hexdigest()


def notification(rule: subscriptions.Rule, source: str, rec: dict) -> tuple[str | None, str]:
    """(Kafka key, value) of one match, the envelope's bytes as text; its topic is rule.topic. `rec`: a collected
    record with the members of rule.fields (members())."""
    payload = "{" + ",".join(rec[PREFIX + f] for f in sorted(set(rule.fields))) + "}"
    nid = notification_id(rule.subscription_id, rule.revision, rec["_partition"], rec["_offset"])
    value = (
        f'{{"notificationId":{json.dumps(nid)},"subscriptionId":{json.dumps(rule.subscription_id)},'
        f'"subscription":{json.dumps(rule.name, ensure_ascii=False)},"revision":{int(rule.revision)},'
        f'"source":{json.dumps(source)},"sourcePartition":{int(rec["_partition"])},'
        f'"sourceOffset":{int(rec["_offset"])},{rec[TS]},"payload":{payload}}}'
    )
    return rec.get("patient_id"), value
