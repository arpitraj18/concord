package dev.concord.simulator.model;

import java.util.Objects;

/** One real transition of a payment: it entered {@code state} at {@code atMs} (virtual ms since run start). */
public record Step(SimState state, long atMs) {
    public Step {
        Objects.requireNonNull(state, "step.state is required");
    }
}
