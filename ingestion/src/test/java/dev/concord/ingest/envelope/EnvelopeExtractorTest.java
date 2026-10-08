package dev.concord.ingest.envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.InboundWebhook;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EnvelopeExtractorTest {

    final EnvelopeExtractor extractor = new EnvelopeExtractor(new ObjectMapper());

    Envelope extract(Gateway g, Map<String, String> headers, String body) {
        return extractor.extract(new InboundWebhook(g, headers, body.getBytes(StandardCharsets.UTF_8), Instant.EPOCH));
    }

    @Test
    void gatewayAUsesHeaderEventId() {
        Envelope e = extract(Gateway.A, Map.of("X-GwA-Event-Id", "evt_1"),
                "{\"event\":\"payment.captured\",\"payload\":{\"payment\":{\"entity\":{\"id\":\"pay_1\",\"status\":\"captured\"}}}}");
        assertThat(e).isEqualTo(new Envelope(Gateway.A, "pay_1", "payment.captured", "evt_1", "A#evt_1"));
        assertThat(e.orderingKey()).isEqualTo("A#pay_1");
    }

    @Test
    void gatewayBDerivesKeyFromMeaningBecauseThereIsNoEventId() {
        String success = "{\"data\":{\"payment\":{\"cf_payment_id\":\"123\",\"payment_status\":\"SUCCESS\"}},"
                + "\"event_time\":\"2026-01-01T00:00:00Z\",\"type\":\"PAYMENT_SUCCESS_WEBHOOK\"}";
        String successRetriedLater = success.replace("2026-01-01T00:00:00Z", "2026-01-01T00:00:09Z");
        String failed = success.replace("SUCCESS\"}", "FAILED\"}").replace("PAYMENT_SUCCESS", "PAYMENT_FAILED");

        Envelope a = extract(Gateway.B, Map.of(), success);
        assertThat(a.paymentId()).isEqualTo("123");
        assertThat(a.gatewayEventId()).isNull();
        assertThat(a.dedupKey()).startsWith("B#h:");
        assertThat(extract(Gateway.B, Map.of(), successRetriedLater).dedupKey()).isEqualTo(a.dedupKey());
        assertThat(extract(Gateway.B, Map.of(), failed).dedupKey()).isNotEqualTo(a.dedupKey());
    }

    @Test
    void gatewayBRefundsAreKeyedByRefundIdSoPartialRefundsStayDistinct() {
        String refund1 = "{\"data\":{\"refund\":{\"cf_payment_id\":\"123\",\"refund_id\":\"rf_1\",\"refund_status\":\"SUCCESS\"}},"
                + "\"type\":\"REFUND_STATUS_WEBHOOK\"}";
        Envelope r1 = extract(Gateway.B, Map.of(), refund1);
        Envelope r2 = extract(Gateway.B, Map.of(), refund1.replace("rf_1", "rf_2"));
        assertThat(r1.paymentId()).isEqualTo("123");
        assertThat(r1.dedupKey()).isNotEqualTo(r2.dedupKey());
    }

    @Test
    void gatewayCFindsPaymentOnIntentOrCharge() {
        Envelope intent = extract(Gateway.C, Map.of(),
                "{\"id\":\"evt_9\",\"type\":\"payment_intent.succeeded\",\"data\":{\"object\":{\"id\":\"pi_1\",\"object\":\"payment_intent\"}}}");
        Envelope charge = extract(Gateway.C, Map.of(),
                "{\"id\":\"evt_10\",\"type\":\"charge.refunded\",\"data\":{\"object\":{\"id\":\"ch_1\",\"object\":\"charge\",\"payment_intent\":\"pi_1\"}}}");
        assertThat(intent.paymentId()).isEqualTo("pi_1");
        assertThat(intent.dedupKey()).isEqualTo("C#evt_9");
        assertThat(charge.paymentId()).isEqualTo("pi_1");
    }

    @Test
    void missingFieldsAreUnparseable() {
        assertThatThrownBy(() -> extract(Gateway.A, Map.of(), "{\"event\":\"payment.captured\"}"))
                .isInstanceOf(UnparseableWebhookException.class);
        assertThatThrownBy(() -> extract(Gateway.C, Map.of(), "not json")).isInstanceOf(UnparseableWebhookException.class);
        assertThatThrownBy(() -> extract(Gateway.C, Map.of(), "[1,2]")).isInstanceOf(UnparseableWebhookException.class);
    }
}
