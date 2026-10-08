package dev.concord.simulator.dialect;

import static dev.concord.simulator.model.SimState.AUTHORIZED;
import static dev.concord.simulator.model.SimState.CAPTURED;
import static dev.concord.simulator.model.SimState.FAILED;
import static dev.concord.simulator.model.SimState.REFUNDED;
import static dev.concord.simulator.model.SimState.SETTLED;

import dev.concord.simulator.crypto.Hmac;
import dev.concord.simulator.model.ScenarioException;
import dev.concord.simulator.model.SimState;
import dev.concord.simulator.plan.PaymentTruth;
import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;

/**
 * Gateway A, loosely modelled on Razorpay: separate authorize and capture, amounts in integer minor
 * units (paise), event id in a header, signature = hex(HMAC-SHA256(secret, body)).
 */
public final class GatewayADialect implements GatewayDialect {

    public static final String SIGNATURE_HEADER = "X-GwA-Signature";
    public static final String EVENT_ID_HEADER = "X-GwA-Event-Id";

    @Override
    public String id() {
        return "A";
    }

    @Override
    public String description() {
        return "Authorize/capture gateway; paise amounts; hex HMAC-SHA256 of body in " + SIGNATURE_HEADER;
    }

    @Override
    public Set<SimState> webhookStates() {
        return EnumSet.of(AUTHORIZED, CAPTURED, SETTLED, FAILED, REFUNDED);
    }

    @Override
    public List<List<SimState>> lifecycles() {
        return List.of(
                List.of(AUTHORIZED, CAPTURED, SETTLED),
                List.of(AUTHORIZED, CAPTURED, SETTLED),
                List.of(AUTHORIZED, CAPTURED),
                List.of(FAILED),
                List.of(AUTHORIZED, FAILED),
                List.of(AUTHORIZED),
                List.of(AUTHORIZED, CAPTURED, REFUNDED),
                List.of(AUTHORIZED, CAPTURED, SETTLED, REFUNDED));
    }

    @Override
    public String newPaymentId(SplittableRandom rng) {
        return "pay_" + Ids.base62(rng, 14);
    }

    @Override
    public String newOrderId(SplittableRandom rng) {
        return "order_" + Ids.base62(rng, 14);
    }

    @Override
    public String newEventId(SplittableRandom rng) {
        return "evt_" + Ids.base62(rng, 14);
    }

    @Override
    public String eventType(SimState state) {
        return switch (state) {
            case AUTHORIZED -> "payment.authorized";
            case CAPTURED -> "payment.captured";
            case FAILED -> "payment.failed";
            case SETTLED -> "settlement.processed";
            case REFUNDED -> "refund.processed";
            default -> throw new ScenarioException("gateway A sends no webhook for " + state);
        };
    }

    private static String status(SimState state) {
        return switch (state) {
            case CREATED -> "created";
            case PENDING -> "created";
            case AUTHORIZED -> "authorized";
            case CAPTURED, SETTLED -> "captured";
            case FAILED -> "failed";
            case REFUNDED -> "refunded";
        };
    }

    private static Map<String, Object> paymentEntity(PaymentTruth p, SimState state, Instant createdAt) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("id", p.paymentId());
        e.put("entity", "payment");
        e.put("amount", p.amount());
        e.put("currency", p.currency());
        e.put("status", status(state));
        e.put("order_id", p.orderId());
        e.put("captured", state.impliesMoneyCaptured());
        e.put("amount_refunded", state == REFUNDED ? p.amount() : 0);
        e.put("settled", state == SETTLED);
        e.put("created_at", createdAt.getEpochSecond());
        return e;
    }

    @Override
    public Map<String, Object> webhookBody(WebhookContext ctx) {
        Map<String, Object> payment = new LinkedHashMap<>();
        payment.put("entity", paymentEntity(ctx.payment(), ctx.claimedState(), ctx.paymentCreatedAt()));
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("payment", payment);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("entity", "event");
        body.put("account_id", "acc_simA");
        body.put("event", ctx.eventType());
        body.put("contains", List.of("payment"));
        body.put("payload", payload);
        body.put("created_at", ctx.eventTime().getEpochSecond());
        return body;
    }

    @Override
    public Map<String, String> webhookHeaders(String secret, byte[] body, String eventId, Instant sentAt) {
        Map<String, String> h = new LinkedHashMap<>();
        h.put(EVENT_ID_HEADER, eventId);
        h.put(SIGNATURE_HEADER, Hmac.sha256Hex(secret, body));
        return h;
    }

    @Override
    public Map<String, Object> statusBody(PaymentTruth payment, SimState current, Instant createdAt) {
        return paymentEntity(payment, current, createdAt);
    }
}
