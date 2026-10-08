package dev.concord.simulator.dialect;

import dev.concord.simulator.model.SimState;
import dev.concord.simulator.plan.PaymentTruth;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;

/**
 * How one gateway speaks: its id formats, status names, webhook payloads and signature scheme.
 *
 * <p>The three dialects are deliberately different (integer minor units vs decimal major units,
 * event id in a header vs in the body vs absent, three signature schemes) because Concord's job is
 * to normalise exactly this mess.
 */
public interface GatewayDialect {

    /** Short id, e.g. "A". */
    String id();

    String description();

    /** States this gateway sends webhooks for; scenario steps may only use these. */
    Set<SimState> webhookStates();

    /** Realistic lifecycles used when generating random payments. */
    List<List<SimState>> lifecycles();

    String newPaymentId(SplittableRandom rng);

    String newOrderId(SplittableRandom rng);

    String newEventId(SplittableRandom rng);

    /** Gateway-specific webhook event name for a state. */
    String eventType(SimState state);

    Map<String, Object> webhookBody(WebhookContext ctx);

    /** Headers added to the webhook POST, including the signature over the exact body bytes. */
    Map<String, String> webhookHeaders(String secret, byte[] body, String eventId, Instant sentAt);

    /** Response body of the status API for a payment currently in {@code current}. */
    Map<String, Object> statusBody(PaymentTruth payment, SimState current, Instant createdAt);

    default boolean supports(SimState state) {
        return webhookStates().contains(state);
    }

    record WebhookContext(PaymentTruth payment, SimState claimedState, String eventType, String eventId,
                          Instant eventTime, Instant paymentCreatedAt) {
    }
}
