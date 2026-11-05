package com.databricks.hl7.subscriber.hub;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
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

    /** How waiting for the hub ended: READY, FAILED (it can't run a revision), or TIMED_OUT (unconfirmed or stopped). */
    public enum Outcome { READY, FAILED, TIMED_OUT }

    public record Wait(Outcome outcome, String message) { }

    /**
     * Wait until the hub runs exactly the accepted revisions (subscription name -> revision) and is processing their
     * sources now, printing each confirmation. ACTIVE can be left over from an earlier run, so a fresh heartbeat is
     * required too; a hub that just restarted reports ACTIVE before its first heartbeat, so that is waited for as well.
     */
    public static Wait await(RegistryClient registry, Map<String, Integer> accepted, Set<String> sources, Duration timeout,
            Duration staleAfter, PrintStream out) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        Set<String> reported = new HashSet<>();
        boolean waitingForHub = false;
        while (true) {
            var listing = registry.list();
            for (var s : listing.subscriptions()) {
                if (Integer.valueOf(s.revision()).equals(accepted.get(s.name())) && !"PENDING".equals(s.state())
                        && reported.add(s.name())) {
                    out.printf("hub         %-22s revision %d %s%s%n", s.name(), s.revision(), s.state(),
                            s.error() == null ? "" : ": " + s.error());
                }
            }
            var result = check(accepted, listing.subscriptions());
            if (result.state() == State.READY) {
                var stale = staleSources(listing.hub(), sources, Instant.now(), staleAfter);
                if (stale.isEmpty()) return new Wait(Outcome.READY, "");
                if (!waitingForHub) {
                    out.printf("waiting     the hub hasn't finished a batch of %s in %d minutes; waiting up to "
                            + "%d minutes%n", stale, staleAfter.toMinutes(), timeout.toMinutes());
                    waitingForHub = true;
                }
            }
            if (result.state() == State.FAILED) return new Wait(Outcome.FAILED, "the hub could not run: " + result.problems());
            if (Instant.now().isAfter(deadline)) {
                var stale = staleSources(listing.hub(), sources, Instant.now(), staleAfter);
                return new Wait(Outcome.TIMED_OUT, stale.isEmpty()
                        ? "the hub hasn't confirmed " + result.problems() + " within " + timeout.toMinutes() + " min"
                        : "the hub isn't processing " + stale + ": start the subscription_hub pipeline (make demo dev)");
            }
            Thread.sleep(1000);
        }
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
