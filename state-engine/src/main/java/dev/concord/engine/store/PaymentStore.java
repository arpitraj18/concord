package dev.concord.engine.store;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface PaymentStore {

    Optional<PaymentRecord> find(Gateway gateway, String paymentId);

    /**
     * Writes the commit atomically, only if the payment is still at {@code expectedVersion} and the
     * event is not yet in its history.
     *
     * @throws AlreadyProcessedException if the event was already recorded (checked first)
     * @throws VersionConflictException  if the payment changed since it was read
     */
    void commit(Commit commit);

    /** Event history, ordered by receive time. */
    List<HistoryEntry> history(Gateway gateway, String paymentId);

    List<OutboxEntry> outbox(Gateway gateway, String paymentId);

    /** Payments in {@code state} not updated since {@code before}: the resolver's work list (Phase 3). */
    List<PaymentRecord> findInStateSince(CanonicalState state, Instant before, int limit);

    /** Every payment record; used to compare a replay with live state. */
    List<PaymentRecord> all();
}
