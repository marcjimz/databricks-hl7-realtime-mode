package com.databricks.hl7.subscriber;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Whether the hub runs exactly what this app declared, and whether the hub itself is running. No silent states. */
public final class Activation {
    private Activation() { }

    public enum State { READY, PENDING, FAILED }

    public record Result(State state, List<String> problems) { }

    /** declared: subscription name -> the revision the registry accepted for it. */
    public static Result check(Map<String, Integer> declared, List<RegistryClient.Status> statuses) {
        List<String> failed = new ArrayList<>();
        List<String> pending = new ArrayList<>();
        for (var e : declared.entrySet()) {
            var s = statuses.stream().filter(x -> x.name().equals(e.getKey())).findFirst().orElse(null);
            if (s != null && s.revision() == e.getValue() && "FAILED".equals(s.state())) {
                failed.add(e.getKey() + ": " + s.error());
            } else if (s == null || s.revision() != e.getValue() || !"ACTIVE".equals(s.state())) {
                pending.add(e.getKey());
            }
        }
        if (!failed.isEmpty()) return new Result(State.FAILED, failed);
        return pending.isEmpty() ? new Result(State.READY, List.of()) : new Result(State.PENDING, pending);
    }

    /** Sources whose hub flow hasn't finished a batch within maxAge (or ever). */
    public static List<String> staleSources(Map<String, Instant> hub, Set<String> sources, Instant now, Duration maxAge) {
        List<String> stale = new ArrayList<>();
        for (String source : new TreeSet<>(sources)) {
            Instant last = hub.get(source);
            if (last == null || last.isBefore(now.minus(maxAge))) stale.add(source);
        }
        return stale;
    }
}
