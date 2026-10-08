package dev.concord.simulator.model;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The true state of a payment inside the simulated world.
 *
 * <p>This is the simulator's own vocabulary, not Concord's canonical state machine. Each gateway
 * dialect renders these states into its own status names and webhook event types. Keeping the
 * world model separate from the system under test means a bug in Concord's state machine cannot
 * be hidden by the same bug in the simulator.
 */
public enum SimState {
    CREATED,
    PENDING,
    AUTHORIZED,
    CAPTURED,
    SETTLED,
    FAILED,
    REFUNDED;

    private static final Map<SimState, Set<SimState>> NEXT = new EnumMap<>(SimState.class);

    static {
        NEXT.put(CREATED, EnumSet.of(PENDING, AUTHORIZED, CAPTURED, FAILED));
        NEXT.put(PENDING, EnumSet.of(AUTHORIZED, CAPTURED, FAILED));
        NEXT.put(AUTHORIZED, EnumSet.of(CAPTURED, FAILED));
        NEXT.put(CAPTURED, EnumSet.of(SETTLED, REFUNDED));
        NEXT.put(SETTLED, EnumSet.of(REFUNDED));
        NEXT.put(FAILED, EnumSet.noneOf(SimState.class));
        NEXT.put(REFUNDED, EnumSet.noneOf(SimState.class));
    }

    /** Whether the real world can move from this state to {@code next}. Faults never change this. */
    public boolean canMoveTo(SimState next) {
        return NEXT.get(this).contains(next);
    }

    public boolean isTerminal() {
        return NEXT.get(this).isEmpty();
    }

    /** True once money has actually moved to the merchant at some point. */
    public boolean impliesMoneyCaptured() {
        return this == CAPTURED || this == SETTLED || this == REFUNDED;
    }
}
