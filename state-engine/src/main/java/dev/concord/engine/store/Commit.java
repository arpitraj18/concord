package dev.concord.engine.store;

import java.util.Optional;

/**
 * Everything one event changes, written atomically.
 *
 * @param expectedVersion version the decision was based on (0 = payment not stored yet)
 * @param newRecord       the updated record, or empty if the event changed nothing
 */
public record Commit(
        String paymentKey,
        long expectedVersion,
        Optional<PaymentRecord> newRecord,
        HistoryEntry history,
        Optional<OutboxEntry> outbox) {
}
