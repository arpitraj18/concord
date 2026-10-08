package dev.concord.simulator.dialect;

import static dev.concord.simulator.model.SimState.CAPTURED;
import static dev.concord.simulator.model.SimState.FAILED;
import static dev.concord.simulator.model.SimState.PENDING;
import static dev.concord.simulator.model.SimState.REFUNDED;

import dev.concord.simulator.crypto.Hmac;
import dev.concord.simulator.model.ScenarioException;
import dev.concord.simulator.model.SimState;
import dev.concord.simulator.plan.PaymentTruth;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;

/**
 * Gateway B, loosely modelled on Cashfree: auto-capture (no separate authorize), one order may have
 * several payment attempts, amounts as decimal major units (rupees), <b>no event id at all</b>, and
 * signature = base64(HMAC-SHA256(secret, timestamp + body)).
 */
public final class GatewayBDialect implements GatewayDialect {

    public static final String SIGNATURE_HEADER = "x-gwb-signature";
    public static final String TIMESTAMP_HEADER = "x-gwb-timestamp";

    @Override
    public String id() {
        return "B";
    }

    @Override
    public String description() {
        return "Auto-capture gateway; rupee decimal amounts; no event id; base64 HMAC-SHA256 of timestamp+body";
    }

    @Override
    public Set<SimState> webhookStates() {
        return EnumSet.of(PENDING, CAPTURED, FAILED, REFUNDED);
    }

    @Override
    public List<List<SimState>> lifecycles() {
        return List.of(
                List.of(PENDING, CAPTURED),
                List.of(PENDING, CAPTURED),
                List.of(CAPTURED),
                List.of(PENDING, FAILED),
                List.of(FAILED),
                List.of(PENDING),
                List.of(CAPTURED, REFUNDED));
    }

    @Override
    public String newPaymentId(SplittableRandom rng) {
        return Ids.digits(rng, 10);
    }

    @Override
    public String newOrderId(SplittableRandom rng) {
        return "order_" + Ids.base62(rng, 12);
    }

    @Override
    public String newEventId(SplittableRandom rng) {
        // Never sent on the wire; only identifies the event inside the plan.
        return "b_" + Ids.base62(rng, 12);
    }

    @Override
    public String eventType(SimState state) {
        return switch (state) {
            case PENDING -> "PAYMENT_PENDING_WEBHOOK";
            case CAPTURED -> "PAYMENT_SUCCESS_WEBHOOK";
            case FAILED -> "PAYMENT_FAILED_WEBHOOK";
            case REFUNDED -> "REFUND_STATUS_WEBHOOK";
            default -> throw new ScenarioException("gateway B sends no webhook for " + state);
        };
    }

    private static String paymentStatus(SimState state) {
        return switch (state) {
            case CREATED -> "NOT_ATTEMPTED";
            case PENDING, AUTHORIZED -> "PENDING";
            case CAPTURED, SETTLED, REFUNDED -> "SUCCESS";
            case FAILED -> "FAILED";
        };
    }

    private static BigDecimal major(long minor) {
        return BigDecimal.valueOf(minor, 2);
    }

    private static Map<String, Object> payment(PaymentTruth p, SimState state, Instant time) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("cf_payment_id", p.paymentId());
        m.put("payment_status", paymentStatus(state));
        m.put("payment_amount", major(p.amount()));
        m.put("payment_currency", p.currency());
        m.put("payment_time", time.toString());
        return m;
    }

    @Override
    public Map<String, Object> webhookBody(WebhookContext ctx) {
        PaymentTruth p = ctx.payment();
        Map<String, Object> data = new LinkedHashMap<>();
        if (ctx.claimedState() == REFUNDED) {
            Map<String, Object> refund = new LinkedHashMap<>();
            refund.put("cf_payment_id", p.paymentId());
            refund.put("order_id", p.orderId());
            refund.put("refund_id", "rf_" + p.paymentId());
            refund.put("refund_status", "SUCCESS");
            refund.put("refund_amount", major(p.amount()));
            refund.put("refund_currency", p.currency());
            data.put("refund", refund);
        } else {
            Map<String, Object> order = new LinkedHashMap<>();
            order.put("order_id", p.orderId());
            order.put("order_amount", major(p.amount()));
            order.put("order_currency", p.currency());
            data.put("order", order);
            data.put("payment", payment(p, ctx.claimedState(), ctx.eventTime()));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("data", data);
        body.put("event_time", ctx.eventTime().toString());
        body.put("type", ctx.eventType());
        return body;
    }

    @Override
    public Map<String, String> webhookHeaders(String secret, byte[] body, String eventId, Instant sentAt) {
        String ts = Long.toString(sentAt.toEpochMilli());
        Map<String, String> h = new LinkedHashMap<>();
        h.put(TIMESTAMP_HEADER, ts);
        h.put(SIGNATURE_HEADER, Hmac.sha256Base64(secret, Hmac.concat(ts, body)));
        return h;
    }

    @Override
    public Map<String, Object> statusBody(PaymentTruth p, SimState current, Instant createdAt) {
        Map<String, Object> m = payment(p, current, createdAt);
        m.put("order_id", p.orderId());
        m.put("refund_status", current == REFUNDED ? "SUCCESS" : null);
        return m;
    }
}
