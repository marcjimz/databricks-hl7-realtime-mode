"""What this app receives: subscription name -> Databricks SQL. Edit, then run. On start the app declares exactly these
(and cancels any you deleted); Databricks compiles each one on the spot, and the hub applies it at its next
micro-batch, with no redeploy.

A rule is SELECT <columns> FROM <gold source> WHERE <filter>, with any Databricks SQL built-in in the filter. Try it
first in the SQL editor against your own view, e.g. hl7_rtm_demo.sub_sp_hl7_realtime_demo.gold_observations: it returns
the records the hub will deliver.

The app declares its service principal's whole set, so this starts with the same rule as the Java example
(MySubscriptions.java): run either one as the same SP and they agree.
"""


def all() -> dict[str, str]:  # noqa: A001 - mirrors MySubscriptions.all()
    return {
        "critical-potassium": """
            SELECT patient_id, value_num, units FROM gold_observations
            WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5""",
    }
