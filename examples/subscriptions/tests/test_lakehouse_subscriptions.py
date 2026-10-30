"""Parked from tests/test_lakehouse.py."""

def test_gold_tables_carry_the_same_meta_as_subscription_views(spark):
    """So a subscription's SQL runs unchanged in a SQL editor against the gold Delta tables."""
    from hl7rt import subscriptions

    silver = _kafka(spark, [(_silver_value(spark, s.oru_r01(), T0, T0 + timedelta(milliseconds=20)), T0)])
    obs = gold.to_gold_records(silver, "e", "o", "r").first()
    table = lakehouse.gold_rows(_kafka(spark, [(obs.value, T0 + timedelta(milliseconds=40))]), gold.OBSERVATION_SCHEMA)
    view = subscriptions._view_schema("gold_observations")[subscriptions.META]
    assert table.schema[subscriptions.META].dataType.simpleString() == view.dataType.simpleString()  # names + types
    meta = table.select(subscriptions.META).first()[0]
    assert meta.record_id == "0-0-1" and meta.gold_ts == T0 + timedelta(milliseconds=40) and meta.raw_ts is not None
