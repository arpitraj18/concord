package dev.concord.engine.store;

import dev.concord.engine.machine.CanonicalState;
import java.time.Instant;

/**
 * A state change to tell the merchant about, written in the <em>same transaction</em> as the change
 * itself ({@code SK=OUTBOX#<version>}). Phase 3's delivery worker reads these from DynamoDB Streams.
 * Writing state and then separately publishing would risk losing or inventing events on a crash.
 *
 * @param idempotencyKey gateway#paymentId#state#version; the merchant dedupes on it
 */
public record OutboxEntry(
        String paymentKey,
        long version,
        CanonicalState fromState,
        CanonicalState toState,
        String idempotencyKey,
        Instant createdAt) {
}
