package dev.concord.engine.store;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import java.time.Instant;

/**
 * The current canonical state of one payment (DynamoDB item {@code PK=PAY#<gateway>#<id>, SK=STATE}).
 *
 * @param version            starts at 1 and goes up by one on every change; writes are conditional on it
 * @param needsVerification  set when the gateway contradicted itself; the resolver checks the status API
 */
public record PaymentRecord(
        Gateway gateway,
        String paymentId,
        CanonicalState state,
        long version,
        Long amountMinor,
        String currency,
        String orderId,
        boolean needsVerification,
        String verificationReason,
        Instant createdAt,
        Instant updatedAt) {

    public String key() {
        return gateway + "#" + paymentId;
    }

    /** The state a payment is in before Concord has seen any event for it. Never stored. */
    public static PaymentRecord unseen(Gateway gateway, String paymentId) {
        return new PaymentRecord(gateway, paymentId, CanonicalState.CREATED, 0, null, null, null, false, null, null, null);
    }

    public boolean exists() {
        return version > 0;
    }
}
