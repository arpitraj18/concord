package dev.concord.ingest.dedup;

public enum ClaimOutcome {
    /** First time this event is seen: the caller owns it and must enqueue it. */
    CLAIMED,
    /** An earlier claim was abandoned (crash between claim and enqueue); the caller now owns it. */
    TAKEN_OVER,
    /** Already enqueued: a duplicate delivery, acknowledge and do nothing. */
    DUPLICATE,
    /** Another delivery of the same event is being processed right now; ask the gateway to retry. */
    IN_FLIGHT
}
