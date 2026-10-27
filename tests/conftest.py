import json
import os
import time

import pytest
from pyspark.sql import SparkSession

# Collected timestamps are converted to the process zone; keep it equal to the session zone.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("hl7rt-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")  # set by the pipeline; see epic_json.SPARK_CONF
        .getOrCreate()
    )
    yield session
    session.stop()


def epic_payload(columns: dict, **envelope) -> str:
    """Build an Epic Event Hubs JSON payload in the shape the outside system emits."""
    body = {
        "introspector": {"source": "epic"},
        "schema": envelope.get("schema", "EPIC"),
        "table": envelope.get("table", "HL7_EVENTS"),
        "txType": envelope.get("txType", "INSERT"),
        "columns": [{"name": k, "value": v} for k, v in columns.items()],
    }
    return json.dumps(body)


@pytest.fixture
def bronze_df(spark):
    """Factory: rows of event_value strings -> a DataFrame shaped like the bronze table."""

    def _make(*event_values: str):
        rows = [
            (
                f"key{i}",
                v,
                "real_time_mode_demo_catalog.hl7_realtime.epic_near_realtime_json_from_eventhub",
                0,
                i,
                "2026-10-23 12:00:00",
                1,
                "2026-10-23 12:00:01",
            )
            for i, v in enumerate(event_values)
        ]
        return spark.createDataFrame(
            rows,
            "event_key string, event_value string, topic string, partition int, offset long, "
            "kafka_timestamp string, kafka_timestamp_type int, _ingested_at string",
        ).selectExpr(
            "event_key",
            "event_value",
            "topic",
            "partition",
            "offset",
            "cast(kafka_timestamp as timestamp) as kafka_timestamp",
            "kafka_timestamp_type",
            "cast(_ingested_at as timestamp) as _ingested_at",
        )

    return _make
