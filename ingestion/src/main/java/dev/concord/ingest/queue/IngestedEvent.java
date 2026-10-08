package dev.concord.ingest.queue;

/**
 * The message put on the queue for the state engine. It carries the raw body so the engine never has
 * to read S3 on the hot path; {@code archiveKey} points to the full delivery for audits and replays.
 */
public record IngestedEvent(
        int schemaVersion,
        String dedupKey,
        String gateway,
        String paymentId,
        String eventType,
        String gatewayEventId,
        String receivedAt,
        String archiveKey,
        String body) {

    public static final int SCHEMA_VERSION = 1;

    /** Per-payment ordering: SQS FIFO keeps messages with the same group id in order. */
    public String messageGroupId() {
        return gateway + "#" + paymentId;
    }
}
