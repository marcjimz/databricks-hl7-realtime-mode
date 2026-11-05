package com.databricks.hl7.subscriber;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What this app receives: subscription name -> Databricks SQL. Edit, rebuild, run. On start the app declares exactly
 * these (and cancels any you deleted); Databricks compiles each one on the spot, and the hub applies it at its next
 * micro-batch, with no redeploy.
 *
 * <p>A rule is SELECT &lt;columns&gt; FROM &lt;gold source&gt; WHERE &lt;filter&gt;, with any Databricks SQL built-in in
 * the filter. Try it first in the SQL editor against your own view, e.g. hl7_rtm_demo.sub_sp_hl7_realtime_demo.gold_observations:
 * it returns the records the hub will deliver.
 */
public final class MySubscriptions {
    private MySubscriptions() { }

    public static Map<String, String> all() {
        var rules = new LinkedHashMap<String, String>();
        rules.put("critical-potassium", """
                SELECT patient_id, value_num, units FROM gold_observations
                WHERE observation_cd_desc = 'Potassium' AND value_num >= 6.5""");
        return rules;
    }
}
