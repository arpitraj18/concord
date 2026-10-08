package dev.concord.ingest.envelope;

public class UnparseableWebhookException extends RuntimeException {
    public UnparseableWebhookException(String reason) {
        super(reason);
    }
}
