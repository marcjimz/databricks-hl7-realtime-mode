package com.databricks.hl7.subscriber;

import static com.databricks.hl7.subscriber.subscriptions.Predicate.field;

import com.databricks.hl7.subscriber.subscriptions.Subscription;
import java.util.List;

/**
 * Edit this file to change what this app receives, rebuild, and run it again. On start the app registers exactly these
 * subscriptions (and cancels any you deleted). The registry accepts or refuses each one on the spot, and the hub applies
 * it at its next micro-batch, with no redeploy.
 *
 * <p>source: a gold table this app is granted (gold_encounters, gold_observations, gold_orders). where: which records,
 * over that table's columns. fields: what to deliver. Both are limited to the columns this app is granted.
 */
public final class MySubscriptions {
    private MySubscriptions() { }

    public static List<Subscription> all() {
        return List.of(
                new Subscription("critical-potassium", "gold_observations",
                        field("observation_cd_desc").eq("Potassium").and(field("value_num").ge(6.5)),
                        List.of("patient_id", "value_num", "units")));
    }
}
