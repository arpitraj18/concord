package dev.concord.divergence;

import java.time.Instant;
import java.util.Map;

/**
 * The detector's memory of one payment: when it was last checked, and since when each divergence it
 * currently shows has been seen (a divergence becomes an incident only once it outlasts the grace period).
 */
public record DetectorState(String paymentKey, Instant lastCheckedAt, Map<DivergenceType, Instant> suspectedSince) {

    public DetectorState {
        suspectedSince = Map.copyOf(suspectedSince);
    }

    public static DetectorState fresh(String paymentKey) {
        return new DetectorState(paymentKey, null, Map.of());
    }
}
