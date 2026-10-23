"""HL7 v2 DTM (and ISO-8601) timestamp parsing as a native Spark expression."""

from pyspark.sql import Column
from pyspark.sql import functions as F

# HL7 v2 DTM is YYYY[MM[DD[HH[MM[SS[.S[S[S[S]]]]]]]]][+/-ZZZZ]; fractional seconds are dropped.
# The format is chosen from the value's shape so each value is parsed exactly once (trying
# formats in turn is ~5x slower). Anything else falls back to ISO-8601; unparseable -> NULL.
_HL7_SHAPES = [
    (r"^\d{14}[+-]\d{4}$", "yyyyMMddHHmmssZ"),
    (r"^\d{14}$", "yyyyMMddHHmmss"),
    (r"^\d{12}$", "yyyyMMddHHmm"),
    (r"^\d{8}$", "yyyyMMdd"),
]


def hl7_timestamp(c: Column) -> Column:
    v = F.regexp_replace(F.trim(c), r"^(\d{14})\.\d+", "$1")
    parsed = F.when(v == "", None)
    for shape, fmt in _HL7_SHAPES:
        parsed = parsed.when(v.rlike(shape), F.try_to_timestamp(v, F.lit(fmt)))
    return parsed.otherwise(F.try_to_timestamp(v))
