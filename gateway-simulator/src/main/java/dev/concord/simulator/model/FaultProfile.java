package dev.concord.simulator.model;

/**
 * Random faults applied to every webhook that is not targeted by an explicit {@link Directive}.
 * All randomness is drawn from the scenario seed, so the same seed always yields the same faults.
 *
 * @param dropRate            probability a webhook is never delivered
 * @param duplicateRate       probability a webhook is delivered more than once
 * @param maxExtraCopies      upper bound on extra copies when duplicated (at least 1)
 * @param newEventIdRate      probability that a duplicate carries a fresh event id (defeats naive dedup)
 * @param maxDelayMs          each webhook is delayed by uniform [0, maxDelayMs]; this causes reordering
 * @param maxDuplicateDelayMs each extra copy arrives uniform [0, maxDuplicateDelayMs] after the original
 * @param contradictionRate   probability per payment of one extra webhook claiming a state that never happened
 */
public record FaultProfile(
        double dropRate,
        double duplicateRate,
        int maxExtraCopies,
        double newEventIdRate,
        long maxDelayMs,
        long maxDuplicateDelayMs,
        double contradictionRate) {

    public static FaultProfile none() {
        return new FaultProfile(0, 0, 0, 0, 0, 0, 0);
    }

    public int effectiveMaxExtraCopies() {
        return Math.max(1, maxExtraCopies);
    }
}
