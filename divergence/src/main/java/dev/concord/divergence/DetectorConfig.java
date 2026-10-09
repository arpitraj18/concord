package dev.concord.divergence;

import java.time.Duration;

/**
 * @param grace            a divergence must still be there this long after first being seen to become an
 *                         incident: events in flight (a delivery not yet applied) are not divergences
 * @param settleDelay      after an outbox change, wait this long before checking (let the merchant apply it)
 * @param window           the periodic sweep looks at payments changed within this window
 * @param recheckInterval  the sweep re-checks a payment at most this often (skips recently verified ones)
 * @param pageSize         DynamoDB page size for the sweep, and merchant feed page size
 */
public record DetectorConfig(Duration grace, Duration settleDelay, Duration window, Duration recheckInterval, int pageSize) {

    public static DetectorConfig defaults() {
        return new DetectorConfig(Duration.ofMinutes(2), Duration.ofSeconds(30), Duration.ofDays(2), Duration.ofMinutes(15), 200);
    }
}
