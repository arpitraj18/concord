package dev.concord.resolver;

import java.util.EnumMap;
import java.util.Map;

/** What one sweep did, by action. */
public final class SweepReport {

    public enum Action {
        CHECKED, APPLIED, VERIFIED, CONFIRMED, SUSPECTED_WRONG, CORRECTED, MARKED_UNKNOWN, UNAVAILABLE,
        CAPTURED, RELEASED, PARKED, SKIPPED_NOT_DUE
    }

    private final Map<Action, Integer> counts = new EnumMap<>(Action.class);

    void add(Action action) {
        counts.merge(action, 1, Integer::sum);
    }

    public int count(Action action) {
        return counts.getOrDefault(action, 0);
    }

    public Map<Action, Integer> counts() {
        return Map.copyOf(counts);
    }

    @Override
    public String toString() {
        return counts.toString();
    }
}
