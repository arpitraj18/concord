package dev.concord.engine.machine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Concord's one payment model, shared by every gateway.
 *
 * <pre>
 * Normal path:  CREATED -> PENDING -> AUTHORIZED -> CAPTURED -> SETTLED
 * Exceptions:   PENDING -> UNKNOWN          UNKNOWN -> AUTHORIZED | CAPTURED | FAILED (once confirmed)
 *               PENDING -> FAILED           AUTHORIZED -> FAILED
 *               CAPTURED -> REFUNDED        SETTLED -> REFUNDED
 * </pre>
 *
 * The graph has no cycles, so "is X before Y in a payment's life" is well defined: Y is reachable
 * from X. Because there are no backward edges, a payment can never go back to an earlier state.
 */
public enum CanonicalState {
    CREATED,
    PENDING,
    UNKNOWN,
    AUTHORIZED,
    CAPTURED,
    SETTLED,
    FAILED,
    REFUNDED;

    private static final Map<CanonicalState, Set<CanonicalState>> NEXT = new EnumMap<>(CanonicalState.class);
    private static final Map<CanonicalState, Set<CanonicalState>> REACHABLE = new EnumMap<>(CanonicalState.class);

    static {
        NEXT.put(CREATED, EnumSet.of(PENDING));
        NEXT.put(PENDING, EnumSet.of(AUTHORIZED, UNKNOWN, FAILED));
        NEXT.put(UNKNOWN, EnumSet.of(AUTHORIZED, CAPTURED, FAILED));
        NEXT.put(AUTHORIZED, EnumSet.of(CAPTURED, FAILED));
        NEXT.put(CAPTURED, EnumSet.of(SETTLED, REFUNDED));
        NEXT.put(SETTLED, EnumSet.of(REFUNDED));
        NEXT.put(FAILED, EnumSet.noneOf(CanonicalState.class));
        NEXT.put(REFUNDED, EnumSet.noneOf(CanonicalState.class));
        for (CanonicalState s : values()) {
            Set<CanonicalState> seen = EnumSet.noneOf(CanonicalState.class);
            Deque<CanonicalState> todo = new ArrayDeque<>(NEXT.get(s));
            while (!todo.isEmpty()) {
                CanonicalState n = todo.pop();
                if (seen.add(n)) {
                    todo.addAll(NEXT.get(n));
                }
            }
            REACHABLE.put(s, seen);
        }
    }

    /** One legal step. */
    public boolean canMoveTo(CanonicalState next) {
        return NEXT.get(this).contains(next);
    }

    /** {@code later} can happen after this state through some chain of legal steps. */
    public boolean precedes(CanonicalState later) {
        return REACHABLE.get(this).contains(later);
    }

    public boolean isTerminal() {
        return NEXT.get(this).isEmpty();
    }

    public Set<CanonicalState> next() {
        return Collections.unmodifiableSet(NEXT.get(this));
    }

    /**
     * Shortest chain of legal steps from this state to {@code target}, both ends included; empty if
     * there is none. Deterministic. UNKNOWN is tried last: it is an exception state, so a dropped
     * webhook must not make Concord infer "the payment was in an unknown state" when the normal path
     * (e.g. via AUTHORIZED) is just as short.
     */
    public List<CanonicalState> pathTo(CanonicalState target) {
        if (this == target) {
            return List.of(this);
        }
        Map<CanonicalState, CanonicalState> parent = new EnumMap<>(CanonicalState.class);
        Deque<CanonicalState> queue = new ArrayDeque<>(List.of(this));
        while (!queue.isEmpty()) {
            CanonicalState s = queue.poll();
            for (CanonicalState n : normalPathFirst(NEXT.get(s))) {
                if (n != this && !parent.containsKey(n)) {
                    parent.put(n, s);
                    if (n == target) {
                        List<CanonicalState> path = new ArrayList<>();
                        for (CanonicalState at = target; at != this; at = parent.get(at)) {
                            path.add(at);
                        }
                        path.add(this);
                        Collections.reverse(path);
                        return path;
                    }
                    queue.add(n);
                }
            }
        }
        return List.of();
    }

    private static List<CanonicalState> normalPathFirst(Set<CanonicalState> states) {
        List<CanonicalState> ordered = new ArrayList<>(states);
        ordered.sort((a, b) -> Boolean.compare(a == UNKNOWN, b == UNKNOWN));
        return ordered;
    }
}
