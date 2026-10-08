package dev.concord.ingest.envelope;

import dev.concord.ingest.Gateway;

/**
 * The minimum ingestion needs to know about a webhook: who it is about (for per-payment ordering) and
 * what identifies it (for dedup). Interpreting the status is the state engine's job, not ingestion's.
 *
 * @param gatewayEventId the gateway's own event id, or null when the gateway sends none
 * @param dedupKey       identity of this event across deliveries; see {@link EnvelopeExtractor}
 */
public record Envelope(Gateway gateway, String paymentId, String eventType, String gatewayEventId, String dedupKey) {

    /** Payment ids are only unique within one gateway, so the gateway is part of the ordering key. */
    public String orderingKey() {
        return gateway + "#" + paymentId;
    }
}
