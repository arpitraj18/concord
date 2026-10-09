package dev.concord.engine.store;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import java.time.Instant;

/**
 * A state change to tell the merchant about, written in the <em>same transaction</em> as the change
 * itself ({@code SK=OUTBOX#<version>}) and picked up from DynamoDB Streams by the delivery worker.
 * Writing state and then separately publishing would risk losing or inventing events on a crash.
 *
 * <p>It is a snapshot: it carries the order id and amount as they were at this version, so delivery
 * never has to read the (possibly newer) current state.
 *
 * @param idempotencyKey gateway#paymentId#state#version; the merchant dedupes on it
 */
public record OutboxEntry(
        Gateway gateway,
        String paymentId,
        long version,
        Kind kind,
        CanonicalState fromState,
        CanonicalState toState,
        String idempotencyKey,
        String orderId,
        Long amountMinor,
        String currency,
        Instant createdAt) {

    public enum Kind {
        /** A normal forward transition. */
        TRANSITION,
        /** Concord had told the merchant something wrong; this replaces it (see EventOutcome.CORRECTED). */
        CORRECTION
    }

    public String paymentKey() {
        return gateway + "#" + paymentId;
    }

    public static OutboxEntry of(PaymentRecord after, Kind kind, CanonicalState from, Instant now) {
        return new OutboxEntry(after.gateway(), after.paymentId(), after.version(), kind, from, after.state(),
                after.key() + "#" + after.state() + "#" + after.version(), after.orderId(), after.amountMinor(),
                after.currency(), now);
    }
}
