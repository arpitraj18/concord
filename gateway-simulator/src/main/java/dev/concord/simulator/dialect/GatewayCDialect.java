package dev.concord.simulator.dialect;

import static dev.concord.simulator.model.SimState.AUTHORIZED;
import static dev.concord.simulator.model.SimState.CAPTURED;
import static dev.concord.simulator.model.SimState.FAILED;
import static dev.concord.simulator.model.SimState.PENDING;
import static dev.concord.simulator.model.SimState.REFUNDED;

import dev.concord.simulator.crypto.Hmac;
import dev.concord.simulator.model.ScenarioException;
import dev.concord.simulator.model.SimState;
import dev.concord.simulator.plan.PaymentTruth;
import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;

/**
 * Gateway C, loosely modelled on Stripe PaymentIntents: event id inside the body, lowercase currency,
 * a refund is reported on a different object (a charge) and leaves the intent's status as
 * "succeeded", signature header {@code t=<unix>,v1=hex(HMAC-SHA256(secret, t + "." + body))}.
 */
public final class GatewayCDialect implements GatewayDialect {

    public static final String SIGNATURE_HEADER = "GwC-Signature";

    @Override
    public String id() {
        return "C";
    }

    @Override
    public String description() {
        return "PaymentIntent-style gateway; event id in body; refund reported on charge; t=,v1= signature header";
    }

    @Override
    public Set<SimState> webhookStates() {
        return EnumSet.of(PENDING, AUTHORIZED, CAPTURED, FAILED, REFUNDED);
    }

    @Override
    public List<List<SimState>> lifecycles() {
        return List.of(
                List.of(PENDING, CAPTURED),
                List.of(AUTHORIZED, CAPTURED),
                List.of(CAPTURED),
                List.of(AUTHORIZED, FAILED),
                List.of(FAILED),
                List.of(AUTHORIZED),
                List.of(PENDING, CAPTURED, REFUNDED));
    }

    @Override
    public String newPaymentId(SplittableRandom rng) {
        return "pi_" + Ids.base62(rng, 24);
    }

    @Override
    public String newOrderId(SplittableRandom rng) {
        return "ord_" + Ids.base62(rng, 10);
    }

    @Override
    public String newEventId(SplittableRandom rng) {
        return "evt_" + Ids.base62(rng, 24);
    }

    @Override
    public String eventType(SimState state) {
        return switch (state) {
            case PENDING -> "payment_intent.processing";
            case AUTHORIZED -> "payment_intent.amount_capturable_updated";
            case CAPTURED -> "payment_intent.succeeded";
            case FAILED -> "payment_intent.canceled";
            case REFUNDED -> "charge.refunded";
            default -> throw new ScenarioException("gateway C sends no webhook for " + state);
        };
    }

    private static String status(SimState state) {
        return switch (state) {
            case CREATED -> "requires_payment_method";
            case PENDING -> "processing";
            case AUTHORIZED -> "requires_capture";
            case CAPTURED, SETTLED, REFUNDED -> "succeeded";
            case FAILED -> "canceled";
        };
    }

    private static Map<String, Object> intent(PaymentTruth p, SimState state, Instant createdAt) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", p.paymentId());
        m.put("object", "payment_intent");
        m.put("amount", p.amount());
        m.put("amount_received", state.impliesMoneyCaptured() ? p.amount() : 0);
        m.put("amount_refunded", state == REFUNDED ? p.amount() : 0);
        m.put("currency", p.currency().toLowerCase(Locale.ROOT));
        m.put("status", status(state));
        m.put("created", createdAt.getEpochSecond());
        m.put("metadata", Map.of("order_id", p.orderId()));
        return m;
    }

    @Override
    public Map<String, Object> webhookBody(WebhookContext ctx) {
        PaymentTruth p = ctx.payment();
        Map<String, Object> object;
        if (ctx.claimedState() == REFUNDED) {
            object = new LinkedHashMap<>();
            object.put("id", "ch_" + p.paymentId().substring(3));
            object.put("object", "charge");
            object.put("payment_intent", p.paymentId());
            object.put("amount", p.amount());
            object.put("amount_refunded", p.amount());
            object.put("refunded", true);
            object.put("currency", p.currency().toLowerCase(Locale.ROOT));
        } else {
            object = intent(p, ctx.claimedState(), ctx.paymentCreatedAt());
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("id", ctx.eventId());
        body.put("object", "event");
        body.put("type", ctx.eventType());
        body.put("created", ctx.eventTime().getEpochSecond());
        body.put("livemode", false);
        body.put("data", Map.of("object", object));
        return body;
    }

    @Override
    public Map<String, String> webhookHeaders(String secret, byte[] body, String eventId, Instant sentAt) {
        String t = Long.toString(sentAt.getEpochSecond());
        String v1 = Hmac.sha256Hex(secret, Hmac.concat(t + ".", body));
        return Map.of(SIGNATURE_HEADER, "t=" + t + ",v1=" + v1);
    }

    @Override
    public Map<String, Object> statusBody(PaymentTruth payment, SimState current, Instant createdAt) {
        return intent(payment, current, createdAt);
    }
}
