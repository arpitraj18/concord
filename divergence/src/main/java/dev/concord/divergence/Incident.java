package dev.concord.divergence;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import java.time.Instant;

/**
 * One divergence on one payment, from when it was first seen until it went away. At most one OPEN
 * incident per (payment, type); severity is recomputed on every check, so it rises with age.
 *
 * @param firstSeenAt  when the detector first observed the divergence (start of its age)
 * @param raisedAt     when it had lasted longer than the grace period and became an incident
 * @param explanation  plain-language summary (templated now; written by the AI layer in Phase 5)
 */
public record Incident(
        Gateway gateway,
        String paymentId,
        String orderId,
        DivergenceType type,
        Status status,
        Severity severity,
        long score,
        Long amountMinor,
        String currency,
        CanonicalState gatewayState,
        CanonicalState concordState,
        String merchantStatus,
        Instant firstSeenAt,
        Instant raisedAt,
        Instant updatedAt,
        Instant resolvedAt,
        String resolution,
        String explanation,
        int occurrences) {

    public enum Status { OPEN, RESOLVED }

    public String paymentKey() {
        return gateway + "#" + paymentId;
    }

    public boolean open() {
        return status == Status.OPEN;
    }
}
