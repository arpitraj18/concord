package dev.concord.merchant;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class OrderServiceTest {

    static EmbeddedMerchant merchant;
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    static void start() {
        merchant = new EmbeddedMerchant("m_secret");
    }

    @AfterAll
    static void stop() {
        merchant.close();
    }

    static OrderService.ConcordEvent event(String pay, String state, long version) {
        return new OrderService.ConcordEvent("A#" + pay + "#" + state + "#" + version, "TRANSITION", "A", pay, "o_" + pay,
                state, "CREATED", version, 100L, "INR", Instant.now().toString());
    }

    static long effects(String orderId, String effect) {
        return merchant.jdbc().queryForObject("SELECT count(*) FROM effects WHERE order_id = ? AND effect = ?",
                Long.class, orderId, effect);
    }

    static String status(String orderId) {
        return (String) merchant.orders().find(orderId).orElseThrow().get("status");
    }

    @Test
    void duplicateDeliveryHasNoSecondEffect() {
        merchant.orders().create("o_p1", "A", "p1", 100, "INR");
        assertThat(merchant.orders().apply(event("p1", "CAPTURED", 2))).isEqualTo(OrderService.Outcome.APPLIED);
        assertThat(merchant.orders().apply(event("p1", "CAPTURED", 2))).isEqualTo(OrderService.Outcome.DUPLICATE);
        assertThat(effects("o_p1", "FULFIL")).isEqualTo(1);
        assertThat(status("o_p1")).isEqualTo("PAID");
    }

    @Test
    void twoDifferentEventsMeaningPaidShipOnce() {
        merchant.orders().create("o_p2", "A", "p2", 100, "INR");
        merchant.orders().apply(event("p2", "CAPTURED", 2));
        merchant.orders().apply(event("p2", "SETTLED", 3));
        assertThat(effects("o_p2", "FULFIL")).isEqualTo(1);
    }

    @Test
    void olderVersionAfterNewerIsIgnored() {
        merchant.orders().create("o_p3", "A", "p3", 100, "INR");
        merchant.orders().apply(event("p3", "REFUNDED", 4));
        assertThat(merchant.orders().apply(event("p3", "CAPTURED", 3))).isEqualTo(OrderService.Outcome.STALE);
        assertThat(status("o_p3")).isEqualTo("REFUNDED");
    }

    @Test
    void correctionStopsFulfilment() {
        merchant.orders().create("o_p4", "A", "p4", 100, "INR");
        merchant.orders().apply(event("p4", "CAPTURED", 2));
        merchant.orders().apply(new OrderService.ConcordEvent("A#p4#AUTHORIZED#3", "CORRECTION", "A", "p4", "o_p4",
                "AUTHORIZED", "CAPTURED", 3, 100L, "INR", Instant.now().toString()));
        assertThat(status("o_p4")).isEqualTo("AWAITING_PAYMENT");
        assertThat(effects("o_p4", "STOP_FULFILMENT")).isEqualTo(1);
    }

    @Test
    void cancelledOrderStaysCancelledButRecordsThePayment() {
        merchant.orders().create("o_p5", "A", "p5", 100, "INR");
        assertThat(merchant.orders().cancel("o_p5")).isTrue();
        merchant.orders().apply(event("p5", "CAPTURED", 2));
        var order = merchant.orders().find("o_p5").orElseThrow();
        assertThat(order.get("status")).isEqualTo("CANCELLED");
        assertThat(order.get("payment_state")).as("visible to the divergence detector").isEqualTo("CAPTURED");
    }

    @Test
    void sameEventDeliveredTwentyTimesConcurrentlyHasExactlyOneEffect() throws Exception {
        merchant.orders().create("o_p6", "A", "p6", 100, "INR");
        CountDownLatch go = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(20)) {
            List<Future<OrderService.Outcome>> results = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                results.add(pool.submit(() -> {
                    go.await();
                    return merchant.orders().apply(event("p6", "CAPTURED", 2));
                }));
            }
            go.countDown();
            long applied = 0;
            for (Future<OrderService.Outcome> f : results) {
                applied += f.get() == OrderService.Outcome.APPLIED ? 1 : 0;
            }
            assertThat(applied).isEqualTo(1);
        }
        assertThat(effects("o_p6", "FULFIL")).isEqualTo(1);
        assertThat(merchant.stats()).containsEntry("duplicateEffects", 0L).containsEntry("ordersFulfilledWrongly", 0L);
    }

    @Test
    void httpEndpointRejectsBadSignaturesAndAcceptsGoodOnes() throws Exception {
        merchant.orders().create("o_p7", "A", "p7", 100, "INR");
        String body = "{\"idempotencyKey\":\"A#p7#CAPTURED#2\",\"kind\":\"TRANSITION\",\"gateway\":\"A\",\"paymentId\":\"p7\","
                + "\"orderId\":\"o_p7\",\"state\":\"CAPTURED\",\"previousState\":\"AUTHORIZED\",\"version\":2,"
                + "\"amountMinor\":100,\"currency\":\"INR\",\"occurredAt\":\"2026-10-09T10:00:00Z\"}";
        assertThat(post(body, "t=1,v1=00")).isEqualTo(401);
        String t = Long.toString(Instant.now().getEpochSecond());
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec("m_secret".getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        String v1 = HexFormat.of().formatHex(mac.doFinal((t + "." + body).getBytes(StandardCharsets.UTF_8)));
        assertThat(post(body, "t=" + t + ",v1=" + v1)).isEqualTo(200);
        assertThat(status("o_p7")).isEqualTo("PAID");
    }

    static int post(String body, String sig) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(merchant.baseUrl() + "/concord/events"))
                .header("Content-Type", "application/json").header("Concord-Signature", sig)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }
}
