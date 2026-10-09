package dev.concord.engine.store;

/** This event is already in the payment's history (e.g. SQS delivered the message twice). */
public class AlreadyProcessedException extends RuntimeException {
    public AlreadyProcessedException(String dedupKey) {
        super("already processed " + dedupKey);
    }
}
