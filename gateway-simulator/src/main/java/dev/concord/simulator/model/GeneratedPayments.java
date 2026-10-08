package dev.concord.simulator.model;

/**
 * Asks the planner to generate {@code count} payments with random but legal lifecycles taken from the
 * gateway's lifecycle catalogue.
 *
 * @param count     number of payments
 * @param spreadMs  first step of each payment lands uniformly in [0, spreadMs]
 * @param stepGapMs gap between consecutive steps is uniform in [1, stepGapMs + 1]
 */
public record GeneratedPayments(int count, long spreadMs, long stepGapMs) {
}
