package dev.concord.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.ingest.Crypto;
import dev.concord.ingest.Gateway;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;

class DeliveryWorkerTest {

    static final ObjectMapper JSON = new ObjectMapper();
    static final CanonicalState[] PATH = {CanonicalState.PENDING, CanonicalState.AUTHORIZED, CanonicalState.CAPTURED,
            CanonicalState.SETTLED, CanonicalState.REFUNDED};

    /** A merchant endpoint whose answers we control; records what it accepted, per payment, in order. */
    static final class Merchant implements AutoCloseable {
        final HttpServer server;
        final Map<String, List<Long>> accepted = new ConcurrentHashMap<>();
        final List<String> badSignatures = new CopyOnWriteArrayList<>();
        volatile IntSupplier status = () -> 200;

        Merchant() throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 1024);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/concord/events", ex -> {
                byte[] body = ex.getRequestBody().readAllBytes();
                String sig = ex.getRequestHeaders().getFirst(DeliverySigner.SIGNATURE_HEADER);
                String t = sig.split(",")[0].substring(2);
                String expected = HexFormat.of().formatHex(Crypto.hmacSha256("m_secret", (t + ".").getBytes(StandardCharsets.UTF_8), body));
                if (!sig.endsWith("v1=" + expected)) {
                    badSignatures.add(sig);
                }
                int code = status.getAsInt();
                if (code == 200) {
                    JsonNode e = JSON.readTree(body);
                    accepted.computeIfAbsent(e.get("gateway").asText() + "#" + e.get("paymentId").asText(),
                            k -> new CopyOnWriteArrayList<>()).add(e.get("version").asLong());
                }
                ex.sendResponseHeaders(code, -1);
                ex.close();
            });
            server.start();
        }

        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/concord/events";
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    static OutboxEntry entry(String paymentId, int version) {
        CanonicalState from = version == 1 ? CanonicalState.CREATED : PATH[version - 2];
        return new OutboxEntry(Gateway.A, paymentId, version, OutboxEntry.Kind.TRANSITION, from, PATH[version - 1],
                "A#" + paymentId + "#" + PATH[version - 1] + "#" + version, "order_" + paymentId, 100L, "INR", Instant.now());
    }

    static DeliveryWorker worker(String url, int maxAttempts, List<String> dlq) {
        return new DeliveryWorker(new DeliveryWorker.Settings(url, maxAttempts, Duration.ofMillis(5), Duration.ofMillis(80),
                Duration.ofSeconds(2)), new DeliverySigner("m_secret"), (e, why) -> dlq.add(e.idempotencyKey()), JSON,
                Clock.systemUTC());
    }

    static void awaitIdle(DeliveryWorker w) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (w.pending() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
    }

    @Test
    void flakyMerchantStillGetsEveryEventInOrderPerPayment() throws Exception {
        try (Merchant m = new Merchant()) {
            m.status = () -> ThreadLocalRandom.current().nextInt(10) < 4 ? 503 : 200;
            List<String> dlq = new CopyOnWriteArrayList<>();
            try (DeliveryWorker w = worker(m.url(), 50, dlq)) {
                for (int v = 1; v <= 5; v++) {
                    for (int p = 0; p < 40; p++) {
                        w.submit(entry("p" + p, v));
                    }
                }
                w.submit(entry("p0", 1)); // the feed may hand over the same entry again
                awaitIdle(w);
                assertThat(w.delivered()).isEqualTo(200);
                assertThat(w.attempts()).as("retries happened").isGreaterThan(200);
            }
            assertThat(dlq).isEmpty();
            assertThat(m.badSignatures).isEmpty();
            assertThat(m.accepted).hasSize(40);
            m.accepted.values().forEach(versions -> assertThat(versions).containsExactly(1L, 2L, 3L, 4L, 5L));
        }
    }

    @Test
    void merchantDownForGoodIsDeadLetteredAndTheLaneMovesOn() throws Exception {
        try (Merchant m = new Merchant()) {
            m.status = () -> 500;
            List<String> dlq = new CopyOnWriteArrayList<>();
            try (DeliveryWorker w = worker(m.url(), 3, dlq)) {
                w.submit(entry("x", 1));
                w.submit(entry("x", 2));
                awaitIdle(w);
                assertThat(w.deadLettered()).isEqualTo(2);
                assertThat(w.attempts()).isEqualTo(6);
            }
            assertThat(dlq).containsExactly("A#x#PENDING#1", "A#x#AUTHORIZED#2");
        }
    }

    @Test
    void alreadyDeliveredVersionsAreNotSentAgain() throws Exception {
        try (Merchant m = new Merchant()) {
            List<String> dlq = new ArrayList<>();
            try (DeliveryWorker w = worker(m.url(), 3, dlq)) {
                w.submit(entry("y", 1));
                w.submit(entry("y", 2));
                awaitIdle(w);
                w.submit(entry("y", 1));
                w.submit(entry("y", 2));
                awaitIdle(w);
                assertThat(w.attempts()).isEqualTo(2);
            }
        }
    }
}
