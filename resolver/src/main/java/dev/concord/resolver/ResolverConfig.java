package dev.concord.resolver;

import java.time.Duration;

/**
 * When the resolver looks at a payment, and how persistently.
 *
 * @param checkAfter         a payment in CREATED, PENDING, UNKNOWN or AUTHORIZED with no change for this long is checked
 * @param unknownAfter       PENDING this long with the status API unreachable becomes UNKNOWN ("uncertain, do not cancel")
 * @param settleCheckAfter   CAPTURED this long is checked for settlement (only gateways that report SETTLED)
 * @param actOnAuthorizedAfter AUTHORIZED this long (confirmed by the status API) triggers capture or release
 * @param verifyFinalAfter   a payment that reached a final state (CAPTURED, SETTLED, FAILED, REFUNDED) is
 *                           checked once this long after, which catches lies that fit a legal path
 * @param verifyWindow       how far back that one-off verification looks
 * @param confirmDelay       a correction needs two status-API observations at least this far apart
 * @param backoffBase        first retry delay; doubles each attempt, with full jitter
 * @param backoffMax         cap on the retry delay
 * @param parkAfter          a payment unresolved for this long is parked for a human. A time budget, not an
 *                           attempt count: with exponential backoff an attempt count is a give-up time nobody
 *                           chose, and it can silently come before unknownAfter or actOnAuthorizedAfter.
 * @param batchSize          payments looked at per state per sweep
 */
public record ResolverConfig(
        Duration checkAfter,
        Duration unknownAfter,
        Duration settleCheckAfter,
        Duration actOnAuthorizedAfter,
        Duration verifyFinalAfter,
        Duration verifyWindow,
        Duration confirmDelay,
        Duration backoffBase,
        Duration backoffMax,
        Duration parkAfter,
        int batchSize) {

    public ResolverConfig {
        if (parkAfter.compareTo(unknownAfter) <= 0 || parkAfter.compareTo(actOnAuthorizedAfter) <= 0) {
            throw new IllegalArgumentException("parkAfter must be longer than unknownAfter and actOnAuthorizedAfter, "
                    + "or payments are parked before they can be marked UNKNOWN or captured/released");
        }
        if (backoffMax.compareTo(backoffBase) < 0) {
            throw new IllegalArgumentException("backoffMax must be >= backoffBase");
        }
    }

    public static ResolverConfig defaults() {
        return new ResolverConfig(Duration.ofMinutes(2), Duration.ofMinutes(10), Duration.ofHours(24),
                Duration.ofMinutes(30), Duration.ofMinutes(1), Duration.ofHours(24), Duration.ofSeconds(30),
                Duration.ofSeconds(30), Duration.ofMinutes(30), Duration.ofHours(24), 500);
    }
}
