package dev.concord.engine.mapping;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import java.time.Instant;

/**
 * An ingested webhook translated into Concord's vocabulary.
 *
 * @param claimedState null when the mapping has no rule for this event (it is recorded, not applied)
 * @param amountMinor  amount in minor units (paise, cents), null if the payload does not carry it
 * @param occurredAt   the gateway's own timestamp; kept for audits, never used to order events
 * @param receivedAt   when ingestion received it; used to order replays
 */
public record CanonicalEvent(
        Gateway gateway,
        String paymentId,
        String dedupKey,
        String eventType,
        CanonicalState claimedState,
        Long amountMinor,
        String currency,
        String orderId,
        Instant occurredAt,
        Instant receivedAt,
        String mappingId,
        String archiveKey) {

    public String paymentKey() {
        return gateway + "#" + paymentId;
    }
}
