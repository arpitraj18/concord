package dev.concord.ingest.envelope;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.ingest.Crypto;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.InboundWebhook;
import java.io.IOException;

/**
 * Pulls the {@link Envelope} out of each gateway's payload.
 *
 * <h2>Dedup key</h2>
 * The key identifies one <em>event</em>, so that a gateway retrying the same event is recognised:
 * <ul>
 *   <li>Gateways A and C send an event id: the key is {@code gateway#eventId}.</li>
 *   <li>Gateway B sends no event id, so the key is a hash of what the event means:
 *       payment id + event type + reported status (+ refund id for refunds).</li>
 * </ul>
 * The key is deliberately <b>not</b> "payment + state" for every gateway: a payment can legitimately get
 * two {@code refund.processed} events for two partial refunds, and collapsing them would lose money
 * movements. Two different events that mean the same thing (e.g. a duplicate sent with a fresh id) are
 * passed through; the state engine (Phase 2) treats a repeated state as a no-op.
 */
public final class EnvelopeExtractor {

    private final ObjectMapper json;

    public EnvelopeExtractor(ObjectMapper json) {
        this.json = json;
    }

    public Envelope extract(InboundWebhook webhook) {
        JsonNode root;
        try {
            root = json.readTree(webhook.body());
        } catch (IOException e) {
            throw new UnparseableWebhookException("body is not JSON");
        }
        if (root == null || !root.isObject()) {
            throw new UnparseableWebhookException("body is not a JSON object");
        }
        return switch (webhook.gateway()) {
            case A -> gatewayA(webhook, root);
            case B -> gatewayB(root);
            case C -> gatewayC(root);
        };
    }

    private Envelope gatewayA(InboundWebhook webhook, JsonNode root) {
        String eventId = webhook.header("x-gwa-event-id");
        String type = text(root, "/event");
        String paymentId = text(root, "/payload/payment/entity/id");
        require(eventId, "x-gwa-event-id header");
        return new Envelope(Gateway.A, paymentId, type, eventId, "A#" + eventId);
    }

    private Envelope gatewayB(JsonNode root) {
        String type = text(root, "/type");
        JsonNode refund = root.at("/data/refund");
        String paymentId;
        String meaning;
        if (!refund.isMissingNode()) {
            paymentId = text(root, "/data/refund/cf_payment_id");
            meaning = text(root, "/data/refund/refund_id") + "|" + text(root, "/data/refund/refund_status");
        } else {
            paymentId = text(root, "/data/payment/cf_payment_id");
            meaning = text(root, "/data/payment/payment_status");
        }
        String hash = Crypto.sha256Hex(paymentId + "|" + type + "|" + meaning);
        return new Envelope(Gateway.B, paymentId, type, null, "B#h:" + hash);
    }

    private Envelope gatewayC(JsonNode root) {
        String eventId = text(root, "/id");
        String type = text(root, "/type");
        String objectType = text(root, "/data/object/object");
        String paymentId = switch (objectType) {
            case "payment_intent" -> text(root, "/data/object/id");
            case "charge" -> text(root, "/data/object/payment_intent");
            default -> throw new UnparseableWebhookException("unexpected object type " + objectType);
        };
        return new Envelope(Gateway.C, paymentId, type, eventId, "C#" + eventId);
    }

    private static String text(JsonNode root, String pointer) {
        JsonNode node = root.at(pointer);
        if (node.isMissingNode() || node.isNull() || !node.isValueNode() || node.asText().isBlank()) {
            throw new UnparseableWebhookException("missing " + pointer);
        }
        return node.asText();
    }

    private static void require(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new UnparseableWebhookException("missing " + what);
        }
    }
}
