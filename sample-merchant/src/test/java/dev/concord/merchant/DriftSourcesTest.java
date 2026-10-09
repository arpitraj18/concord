package dev.concord.merchant;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The merchant-side ways orders drift from payment truth, and the change feed the detector reads. */
class DriftSourcesTest {

    static EmbeddedMerchant merchant;

    @BeforeAll
    static void start() {
        merchant = new EmbeddedMerchant("m_secret", "--merchant.checkout-timeout-ms=1500",
                "--merchant.timeout-check-interval-ms=200", "--merchant.reject-events-for-orders=o_reject");
    }

    @AfterAll
    static void stop() {
        merchant.close();
    }

    static String status(String orderId) {
        return (String) merchant.orders().find(orderId).orElseThrow().get("status");
    }

    @Test
    void checkoutTimeoutCancelsUnpaidOrdersEvenIfThePaymentIsJustLate() throws Exception {
        merchant.orders().create("o_late", "A", "p_late", 49_900, "INR");
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!status("o_late").equals("CANCELLED") && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertThat(status("o_late")).isEqualTo("CANCELLED");
        // ...and when the capture finally arrives, the order stays cancelled: "paid but cancelled".
        merchant.orders().apply(new OrderService.ConcordEvent("A#p_late#CAPTURED#2", "TRANSITION", "A", "p_late", "o_late",
                "CAPTURED", "AUTHORIZED", 2, 49_900L, "INR", Instant.now().toString()));
        Map<String, Object> order = merchant.orders().find("o_late").orElseThrow();
        assertThat(order).containsEntry("status", "CANCELLED").containsEntry("payment_state", "CAPTURED");
    }

    @Test
    void redirectBugMarksPaidAndShipsWithoutAPayment() {
        merchant.orders().create("o_redirect", "A", "p_redirect", 100, "INR");
        assertThat(merchant.orders().markPaidFromRedirect("o_redirect")).isTrue();
        assertThat(status("o_redirect")).isEqualTo("PAID");
        assertThat(merchant.jdbc().queryForObject("SELECT count(*) FROM effects WHERE order_id = 'o_redirect' AND effect = 'FULFIL'",
                Long.class)).isEqualTo(1);
    }

    @Test
    void changeFeedPagesThroughEveryChangeWithoutSkipsWhileOrdersKeepChanging() throws Exception {
        for (int i = 0; i < 300; i++) {
            merchant.orders().create("o_feed_" + i, "B", "p_feed_" + i, 100, "INR");
        }
        AtomicBoolean writing = new AtomicBoolean(true);
        try (ExecutorService pool = Executors.newSingleThreadExecutor()) {
            pool.submit(() -> {
                int i = 0;
                while (writing.get()) {
                    merchant.orders().cancel("o_feed_" + (i++ % 300));
                }
            });
            Thread.sleep(300);
            writing.set(false);
        }
        Thread.sleep(2200); // the feed only shows rows older than its 2 s safety lag

        Set<String> seen = new HashSet<>();
        List<String> order = new ArrayList<>();
        String since = "1970-01-01T00:00:00Z";
        String after = "";
        while (true) {
            var page = merchant.orders().changedSince(since, after, 37, Duration.ofSeconds(2));
            if (page.isEmpty()) {
                break;
            }
            for (var row : page) {
                order.add((String) row.get("updated_at") + "|" + row.get("order_id"));
                seen.add((String) row.get("order_id"));
            }
            since = (String) page.getLast().get("updated_at");
            after = (String) page.getLast().get("order_id");
        }
        for (int i = 0; i < 300; i++) {
            assertThat(seen).contains("o_feed_" + i);
        }
        assertThat(order).as("strictly increasing cursor: no repeats").doesNotHaveDuplicates().isSorted();
    }

    @Test
    void changeFeedUsesTheIndexOnABigTable() {
        merchant.jdbc().update("INSERT INTO orders (order_id, gateway, payment_id, amount_minor, currency, status, updated_at) "
                + "SELECT 'bulk_' || g, 'C', 'pb_' || g, 100, 'INR', 'PAID', now() - make_interval(secs => g) "
                + "FROM generate_series(1, 20000) g");
        merchant.jdbc().execute("ANALYZE orders");
        // The exact query the feed runs (same columns), not a simplified one.
        List<String> plan = merchant.jdbc().queryForList("EXPLAIN SELECT " + OrderService.ORDER_COLUMNS + " FROM orders "
                + "WHERE (orders.updated_at, order_id) > ('2026-01-01T00:00:00Z'::timestamptz, '') "
                + "AND orders.updated_at < clock_timestamp() - interval '2 seconds' "
                + "ORDER BY orders.updated_at, order_id LIMIT 200", String.class);
        System.out.println("EXPLAIN change feed page:\n  " + String.join("\n  ", plan));
        assertThat(String.join("\n", plan)).containsPattern("Index (Only )?Scan using orders_by_change")
                .doesNotContain("Seq Scan").doesNotContain("Sort  ");
    }

    @Test
    void injectedFaultRejectsEventsForOneOrder() throws Exception {
        merchant.orders().create("o_reject", "A", "p_reject", 100, "INR");
        String body = "{\"idempotencyKey\":\"A#p_reject#CAPTURED#2\",\"kind\":\"TRANSITION\",\"gateway\":\"A\","
                + "\"paymentId\":\"p_reject\",\"orderId\":\"o_reject\",\"state\":\"CAPTURED\",\"previousState\":\"AUTHORIZED\","
                + "\"version\":2,\"amountMinor\":100,\"currency\":\"INR\",\"occurredAt\":\"2026-10-09T10:00:00Z\"}";
        String t = Long.toString(Instant.now().getEpochSecond());
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec("m_secret".getBytes(), "HmacSHA256"));
        String v1 = java.util.HexFormat.of().formatHex(mac.doFinal((t + "." + body).getBytes()));
        var r = java.net.http.HttpClient.newHttpClient().send(java.net.http.HttpRequest.newBuilder(
                        java.net.URI.create(merchant.baseUrl() + "/concord/events"))
                .header("Content-Type", "application/json").header("Concord-Signature", "t=" + t + ",v1=" + v1)
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build(), java.net.http.HttpResponse.BodyHandlers.discarding());
        assertThat(r.statusCode()).isEqualTo(503);
    }
}
