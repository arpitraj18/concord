package dev.concord.engine.store;

/** What one event did to its payment; stored with the event in the payment's history. */
public enum EventOutcome {
    APPLIED,
    SAME,
    STALE,
    CONTRADICTION,
    /** The mapping has no rule for this event type. */
    UNMAPPED
}
