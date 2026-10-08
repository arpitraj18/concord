package dev.concord.simulator.model;

/**
 * An explicit, hand-written fault on one webhook, identified by payment and the state it reports.
 * When any directive targets a webhook, random faults are not applied to that webhook, so scripted
 * scenarios stay exact.
 *
 * <p>All times are virtual ms relative to the step's own {@code atMs}.
 */
public record Directive(String paymentId, SimState state, Action action, long delayMs, SimState claimedState) {

    public enum Action {
        /** The webhook is never delivered. */
        DROP,
        /** The webhook is delivered {@code delayMs} after the state change instead of immediately. */
        DELAY,
        /** An extra copy with the same event id is delivered {@code delayMs} after the state change. */
        DUPLICATE,
        /** An extra copy with a new event id is delivered {@code delayMs} after the state change. */
        DUPLICATE_NEW_ID,
        /** An extra webhook claiming {@code claimedState} (which is false) is delivered {@code delayMs} after the state change. */
        CONTRADICT
    }
}
