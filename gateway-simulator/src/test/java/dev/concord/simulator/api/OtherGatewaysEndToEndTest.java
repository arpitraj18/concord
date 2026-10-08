package dev.concord.simulator.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.simulator.crypto.Hmac;
import dev.concord.simulator.dialect.GatewayBDialect;
import dev.concord.simulator.dialect.GatewayCDialect;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;

/** Checks that gateways B and C put the right bytes and signatures on the wire. */
class OtherGatewaysEndToEndTest {

    abstract static class Base {
        @Autowired
        TestRestTemplate rest;

        @Autowired
        ObjectMapper json;

        List<RecordingReceiver.Received> runAndCollect(List<Map<String, Object>> steps, int expected) throws Exception {
            try (RecordingReceiver receiver = new RecordingReceiver()) {
                Map<String, Object> scenario = Map.of("seed", 1, "targetUrl", receiver.url(), "timeScale", 20,
                        "payments", List.of(Map.of("amount", 12345, "steps", steps)));
                rest.postForEntity("/sim/runs", scenario, String.class);
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (receiver.received().size() < expected && System.nanoTime() < deadline) {
                    Thread.sleep(20);
                }
                return receiver.received();
            }
        }

        JsonNode tree(String s) throws Exception {
            return json.readTree(s);
        }

        /** Webhooks a few ms apart are sent concurrently and may arrive in either order, so look them up by type. */
        JsonNode byType(List<RecordingReceiver.Received> got, String field, String type) throws Exception {
            for (var r : got) {
                JsonNode n = tree(r.body());
                if (n.get(field).asText().equals(type)) {
                    return n;
                }
            }
            throw new AssertionError("no " + type + " webhook in " + got);
        }
    }

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {"simulator.gateway=B", "simulator.webhook-secret=b_secret"})
    class GatewayB extends Base {
        @Test
        void signsTimestampPlusBodyAndUsesDecimalAmounts() throws Exception {
            var got = runAndCollect(List.of(Map.of("state", "PENDING", "atMs", 0), Map.of("state", "CAPTURED", "atMs", 100)), 2);
            assertThat(got).hasSize(2);
            for (var r : got) {
                String ts = r.header(GatewayBDialect.TIMESTAMP_HEADER);
                byte[] signed = (ts + r.body()).getBytes(StandardCharsets.UTF_8);
                assertThat(r.header(GatewayBDialect.SIGNATURE_HEADER)).isEqualTo(Hmac.sha256Base64("b_secret", signed));
            }
            JsonNode success = byType(got, "type", "PAYMENT_SUCCESS_WEBHOOK");
            assertThat(success.at("/data/payment/payment_status").asText()).isEqualTo("SUCCESS");
            assertThat(success.at("/data/payment/payment_amount").decimalValue()).isEqualByComparingTo("123.45");
        }
    }

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {"simulator.gateway=C", "simulator.webhook-secret=c_secret"})
    class GatewayC extends Base {
        @Test
        void signsWithTimestampedHeaderAndPutsEventIdInBody() throws Exception {
            var got = runAndCollect(List.of(Map.of("state", "AUTHORIZED", "atMs", 0), Map.of("state", "CAPTURED", "atMs", 100),
                    Map.of("state", "REFUNDED", "atMs", 200)), 3);
            assertThat(got).hasSize(3);
            for (var r : got) {
                String[] parts = r.header(GatewayCDialect.SIGNATURE_HEADER).split(",");
                String t = parts[0].substring(2);
                String v1 = parts[1].substring(3);
                assertThat(v1).isEqualTo(Hmac.sha256Hex("c_secret", (t + "." + r.body()).getBytes(StandardCharsets.UTF_8)));
                assertThat(tree(r.body()).get("id").asText()).startsWith("evt_");
            }
            JsonNode refund = byType(got, "type", "charge.refunded");
            assertThat(refund.at("/data/object/object").asText()).isEqualTo("charge");
        }
    }
}
