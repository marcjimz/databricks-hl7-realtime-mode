"""Subscription rules are Databricks SQL in one shape: SELECT <columns> FROM <one gold source> [WHERE <condition>].

Databricks compiles the SQL (and its errors pass through unchanged); these tests cover the shape check, the only rule
logic of our own: what the hub may run with its own permissions.
"""

import pytest

from subscription_api.rules import RuleError, columns, parse

SOURCES = {"gold_observations", "gold_encounters"}
SCHEMA = "cat.sub_app_a"


def ok(sql):
    return parse(sql, SOURCES, SCHEMA)


def refused(sql):
    with pytest.raises(RuleError) as e:
        parse(sql, SOURCES, SCHEMA)
    return e.value.code


def test_a_rule_is_one_select_from_one_source():
    r = ok("SELECT patient_id, value_num FROM gold_observations WHERE observation_cd_desc = 'Potassium' "
           "AND value_num >= 6.5")  # fmt: skip
    assert r.source == "gold_observations"
    assert r.fields == ("patient_id", "value_num")
    assert r.where == "observation_cd_desc = 'Potassium' AND value_num >= 6.5"
    assert r.query == ("SELECT patient_id, value_num FROM gold_observations "
                       "WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5")  # fmt: skip


def test_databricks_sql_built_ins_and_operators_are_allowed():
    r = ok("SELECT patient_id FROM gold_observations WHERE lower(units) = 'mmol/l' AND abs(value_num) BETWEEN 6 AND 9 "
           "AND startswith(patient_id, 'MRN0') AND units IN ('a', 'b') AND abnormal_flag IS NOT NULL "
           "AND NOT (value_num < 1) AND observation_dt_tm > TIMESTAMP '2026-01-01' AND units LIKE 'mmol%'")  # fmt: skip
    assert columns(r.where) == {"units", "value_num", "patient_id", "abnormal_flag", "observation_dt_tm"}


def test_the_callers_own_view_may_be_named_in_full_so_the_editor_query_works_as_is():
    r = ok("SELECT patient_id FROM cat.sub_app_a.gold_observations WHERE value_num > 6")
    assert r.source == "gold_observations"
    assert r.query == "SELECT patient_id FROM gold_observations WHERE value_num > 6"


def test_table_qualified_columns_are_normalized():
    r = ok("SELECT o.patient_id FROM gold_observations AS o WHERE o.value_num > 6")
    assert r.fields == ("patient_id",) and r.where == "value_num > 6"


def test_no_where_means_every_record():
    assert ok("SELECT patient_id FROM gold_observations").where is None


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT patient_id FROM gold_observations WHERE value_num > (SELECT max(value_num) FROM gold_observations)",
        "SELECT patient_id FROM gold_observations WHERE patient_id IN (SELECT patient_id FROM gold_encounters)",
        "SELECT patient_id FROM gold_observations WHERE EXISTS (SELECT 1 FROM gold_encounters)",
        "SELECT o.patient_id FROM gold_observations o JOIN gold_encounters e ON o.patient_id = e.patient_id",
        "SELECT patient_id FROM gold_observations, gold_encounters",
        "WITH x AS (SELECT patient_id FROM gold_observations) SELECT patient_id FROM x",
        "SELECT patient_id FROM gold_observations UNION SELECT patient_id FROM gold_encounters",
        "SELECT patient_id FROM gold_observations GROUP BY patient_id",
        "SELECT patient_id FROM gold_observations ORDER BY patient_id",
        "SELECT patient_id FROM gold_observations LIMIT 5",
        "SELECT DISTINCT patient_id FROM gold_observations",
        "SELECT * FROM gold_observations",
        "SELECT upper(patient_id) FROM gold_observations",
        "SELECT patient_id AS p FROM gold_observations",
        "SELECT patient_id FROM read_files('/Volumes/x')",
        "SELECT patient_id FROM gold_observations TABLESAMPLE (10 PERCENT)",
        "SELECT patient_id FROM gold_observations; DROP TABLE gold_observations",
        "SELECT patient_id FROM gold_observations WHERE value_num > :threshold",
    ],
)
def test_anything_but_one_filter_over_one_source_is_refused(sql):
    assert refused(sql) == "SQL_NOT_ALLOWED"


@pytest.mark.parametrize(
    "condition",
    [
        "http_request('GET', 'https://example.com') IS NOT NULL",
        "secret('scope', 'key') = patient_id",
        "reflect('java.lang.Runtime', 'getRuntime') IS NULL",
        "java_method('java.lang.System', 'exit', 0) IS NULL",
        "ai_query('model', patient_id) = 'x'",
        "ai_classify(patient_id, array('a')) = 'a'",
        "cat.sch.my_udf(patient_id) = 1",
        "current_user() = 'x'",
    ],
)
def test_functions_that_reach_outside_the_record_are_refused(condition):
    assert refused(f"SELECT patient_id FROM gold_observations WHERE {condition}") == "SQL_NOT_ALLOWED"


def test_only_this_apps_views_and_gold_sources():
    assert refused("SELECT patient_id FROM cat.sub_app_b.gold_observations") == "SQL_NOT_ALLOWED"  # another app's
    assert refused("SELECT patient_id FROM cat.hl7_realtime.gold_observations") == "SQL_NOT_ALLOWED"  # the base table
    assert refused("SELECT patient_id FROM silver_hl7") == "SQL_NOT_ALLOWED"  # not a subscription source


def test_unparseable_sql_is_left_to_databricks_to_explain():
    assert refused("SELECT patient_id FROM gold_observations WHER value_num > 6") == "PARSE"
