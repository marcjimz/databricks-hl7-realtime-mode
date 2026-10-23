"""Pipeline configuration. Every value is set by the bundle (resources/**) under the `hl7.` prefix."""

from pyspark.sql import SparkSession

PREFIX = "hl7."


def conf(spark: SparkSession, key: str, default: str | None = None) -> str:
    value = spark.conf.get(PREFIX + key, default)
    if value is None:
        raise KeyError(f"missing pipeline configuration '{PREFIX}{key}'")
    return value
