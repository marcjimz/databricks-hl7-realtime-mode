"""The dynamic patient monitoring example (examples/subscribers/python/patient_rules.py): its rules are ones the API
accepts and the hub indexes on the patient list, and its census follows an ED encounter from registration to release."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples/subscribers/python"))
import patient_rules as pr  # noqa: E402

from hl7rt import subscriptions  # noqa: E402
from hl7rt.subscriptions import Rule  # noqa: E402
from subscription_api.rules import columns, parse  # noqa: E402

CENSUS = [f"MRN{50_000 + i:06d}" for i in range(1, 501)]


def _rule(name, sql):
    r = parse(sql, set(subscriptions.SOURCES), "cat.sub_ed_monitor")
    everything = frozenset(f.name for f in subscriptions.SOURCES[r.source])
    return r, Rule(name, name, 1, r.source, r.where, list(r.fields), "t", everything)


def test_every_rule_is_one_the_api_accepts_and_reads_only_the_templates_columns():
    """The example's terraform.tfvars template (infra/azure/terraform.tfvars.example) covers what its rules read."""
    example = (Path(__file__).resolve().parents[1] / "infra/azure/terraform.tfvars.example").read_text()
    block = example[example.index('"ed-respiratory-monitoring" = {') :]
    for name, sql in {**pr.BASE, **pr.patient_rules(CENSUS)}.items():
        r, _ = _rule(name, sql)
        assert f'"{r.source}"' in block.split("fields")[0], (name, r.source)
        for col in set(r.fields) | columns(r.where):
            assert f'"{col}"' in block, (name, col)


@pytest.mark.parametrize("name", pr.PATIENT_RULES)
def test_each_patient_rule_is_indexed_on_the_patient_list(spark, name):
    _, rule = _rule(name, pr.patient_rules(CENSUS)[name])
    c = subscriptions.compile_rules([rule], spark=spark)
    assert c.data_rules == 1 and c.scan is None
    assert {r.key_col for r in c.index.collect()} == {"patient_id"}
    assert c.index.count() == len(CENSUS) * c.rules_table.count(), "one index row per patient, per branch"


def test_an_empty_census_cancels_the_patient_rules():
    assert pr.patient_rules([]) == dict.fromkeys(pr.PATIENT_RULES)


def test_a_respiratory_patient_is_watched_until_discharge():
    c = pr.Census()
    assert c.registered({"patient_id": "MRN1", "visit_id": "V1", "chief_complaint": "Cough"}, 0.0) == "added"
    assert c.registered({"patient_id": "MRN1", "visit_id": "V1"}, 1.0) is None, "a repeat isn't a change"
    v = c.version
    assert c.on_patient_record("ed-patient-encounters", {"patient_id": "MRN1", "msg_type": "ADT^A08",
                                                        "chief_complaint": "Cough"}) is None  # fmt: skip
    assert c.patients["MRN1"].qualified is True and c.version == v, "qualifying changes no rule"
    assert c.on_patient_record("ed-patient-results", {"patient_id": "MRN1", "value_num": 7.0}) is None
    assert c.on_patient_record("ed-patient-encounters", {"patient_id": "MRN1", "msg_type": "ADT^A03"}) == (
        "removed: discharged"
    )
    assert c.ids() == [] and c.removed == {"MRN1": "discharged"}


def test_a_patient_who_doesnt_qualify_is_released_at_once_and_a_transfer_ends_monitoring():
    c = pr.Census()
    c.registered({"patient_id": "MRN2", "chief_complaint": "Ankle injury"}, 0.0)
    c.registered({"patient_id": "MRN3", "chief_complaint": "Shortness of breath"}, 0.0)
    update = {"patient_id": "MRN2", "msg_type": "ADT^A08", "chief_complaint": "Ankle injury"}
    assert c.on_patient_record("ed-patient-encounters", update) == "removed: not qualified"
    assert c.on_patient_record("ed-patient-encounters", {"patient_id": "MRN3", "msg_type": "ADT^A02"}) == (
        "removed: transferred"
    )
    assert c.on_patient_record("ed-patient-results", {"patient_id": "MRN2"}) is None, "released patients are ignored"


def test_qualification_reads_the_complaint_or_the_admit_diagnosis():
    assert pr.respiratory("Shortness of breath") and pr.respiratory(diagnosis_cd="J18.9")
    assert not pr.respiratory("Laceration") and not pr.respiratory(diagnosis_cd="S93.401A")


def test_the_examples_patient_rules_match_what_the_generator_emits():
    """The rules ask for the labs, vitals, and points of care the synthetic feed produces."""
    from hl7rt.generators import hl7v2 as gen

    assert set(pr.LABS) == {c for c, *_ in gen.LABS} and set(pr.VITALS) == {c for c, *_ in gen.VITALS}
    assert pr.ED_POINTS_OF_CARE == gen.ED_POINTS_OF_CARE
    assert {code for code, _, resp in gen.CHIEF_COMPLAINTS if resp} == {
        code for code, text, _ in gen.CHIEF_COMPLAINTS if pr.respiratory(text)
    }
