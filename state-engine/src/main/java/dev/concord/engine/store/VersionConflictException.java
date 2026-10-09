package dev.concord.engine.store;

/** Someone else changed the payment since we read it. Re-read and decide again. */
public class VersionConflictException extends RuntimeException {
    public VersionConflictException(String paymentKey) {
        super("version conflict on " + paymentKey);
    }
}
