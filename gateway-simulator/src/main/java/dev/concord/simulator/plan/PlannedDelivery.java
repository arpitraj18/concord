package dev.concord.simulator.plan;

import dev.concord.simulator.model.SimState;

/**
 * One webhook the gateway will (or, if {@code dropped}, will not) send.
 *
 * @param seq          creation order inside the plan; stable tie-breaker
 * @param claimedState the state this webhook reports (false for {@link Kind#CONTRADICTION})
 * @param happenedAtMs virtual time of the event the webhook describes
 * @param deliverAtMs  virtual time of the first delivery attempt
 * @param cause        why this delivery looks the way it does, e.g. "random:delay+812" or "directive:DROP"
 */
public record PlannedDelivery(
        int seq,
        String paymentId,
        SimState claimedState,
        String eventType,
        String eventId,
        Kind kind,
        boolean dropped,
        long happenedAtMs,
        long deliverAtMs,
        String cause) {

    public enum Kind {
        ORIGINAL,
        DUPLICATE,
        CONTRADICTION
    }
}
