package dev.concord.engine.store;

/** What one event did to its payment; stored with the event in the payment's history. */
public enum EventOutcome {
    APPLIED,
    SAME,
    STALE,
    CONTRADICTION,
    /** The mapping has no rule for this event type. */
    UNMAPPED,
    /** The merchant registered the payment at order creation; Concord now knows it exists. */
    REGISTERED,
    /** The status API confirmed the current state; a contradiction flag, if any, was cleared. */
    VERIFIED,
    /**
     * The status API, confirmed twice, showed the current state was wrong (a lie consistent with a
     * legal path, or the wrong side of a contradiction). The only kind of change that may move a payment
     * outside the normal transitions; always authoritative, audited, and delivered as a correction.
     */
    CORRECTED
}
