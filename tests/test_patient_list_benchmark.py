"""The patient-list benchmark (examples/benchmarks/patient_lists.py): its rules are the monitor's, as the API stores
them, sized as configured, matching the intended share of the background population, and indexed on the patient list."""

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples/benchmarks"))
import patient_lists as bench  # noqa: E402

from hl7rt import matching, subscriptions  # noqa: E402
from hl7rt.subscriptions import Rule  # noqa: E402
from subscription_api.rules import MAX_SQL  # noqa: E402


def test_a_census_has_the_configured_size_and_share_of_patients_with_traffic():
    c = bench.census(3, 2_000, 50)
    assert len(c) == len(set(c)) == 2_000
    live = [p for p in c if int(p[3:]) <= bench.POPULATION]
    assert len(live) == 50, "0.1% of the 50,000 background patients"
    assert bench.census(3, 2_000, 50) == c and bench.census(4, 2_000, 50)[50:] != c[50:], "deterministic, distinct"


def test_a_swap_keeps_the_census_size_and_changes_one_patient_with_traffic():
    c = bench.census(0, 2_000, 50)
    s = bench.swap(c, random.Random(1), 50)
    assert len(s) == 2_000 and sum(a != b for a, b in zip(c, s, strict=True)) <= 1 and s[50:] == c[50:]


def test_each_census_is_the_monitors_five_rules_as_the_api_stores_them_indexed_on_patients():
    rules = bench.rules_for(7, bench.census(7, 2_000, 50), "cat.sub_ed_monitor")
    assert sorted(rules) == [f"perf-007-{n}" for n in ("diagnoses", "encounters", "notes", "orders", "results")]
    for name, r in rules.items():
        assert len(r.query) < MAX_SQL and "MRN1000000" not in r.query  # census 7's quiet IDs start after census 6's
        everything = frozenset(f.name for f in subscriptions.SOURCES[r.source])
        m = matching.Matcher()  # the hub matches them on the driver (it raises RuleNotSimple otherwise)
        m.place(subscriptions.runnable([Rule(name, name, 1, r.source, r.where, list(r.fields), "t", everything)])[0][0],
                "app")  # fmt: skip
        assert not m._scan and set(m._columns) == {"patient_id"}, name
        branches = 3 if name.endswith("results") else 1  # labs OR vitals OR radiology: the list is in each branch
        assert m.index_size() == 2_000 * branches, name
