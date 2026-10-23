"""The Epic JSON generator produces payloads the silver transform accepts (and a few it rejects)."""

import random

from hl7rt import epic_json
from hl7rt.generators import epic_json as gen


def test_generated_payloads_normalize(bronze_df):
    rng = random.Random(7)
    values = [gen.payload(n, rng, bad_ratio=0.0)[1] for n in range(50)]
    silver = epic_json.to_silver(bronze_df(*values))
    assert silver.count() == 50
    assert silver.where("msg_dt_tm IS NULL OR msg_control_id IS NULL").count() == 0


def test_bad_ratio_routes_to_quarantine(bronze_df):
    rng = random.Random(7)
    values = [gen.payload(n, rng, bad_ratio=1.0)[1] for n in range(20)]
    assert epic_json.to_silver(bronze_df(*values)).count() == 0
    assert epic_json.to_quarantine(bronze_df(*values)).count() == 20
