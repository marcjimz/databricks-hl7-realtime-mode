package com.databricks.hl7.subscriber;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** The code is the source of truth: subscribe everything it declares, cancel what it no longer declares. */
public final class Sync {
    private Sync() { }

    public record Plan(List<Subscription> subscribe, List<String> unsubscribe) { }

    public static Plan plan(List<Subscription> desired, List<RegistryClient.Status> registered) {
        Set<String> names = new HashSet<>();
        for (Subscription s : desired) {
            if (!names.add(s.name())) throw new IllegalArgumentException("duplicate subscription name: " + s.name());
        }
        List<String> cancel = registered.stream()
                .filter(r -> r.enabled() && !names.contains(r.name()))
                .map(RegistryClient.Status::name)
                .toList();
        return new Plan(List.copyOf(desired), cancel);
    }
}
