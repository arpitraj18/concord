package dev.concord.local;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sun.net.httpserver.HttpExchange;
import dev.concord.delivery.DeliverySigner;
import dev.concord.divergence.DetectorConfig;
import dev.concord.divergence.DetectorStateStore;
import dev.concord.divergence.DivergenceDetector;
import dev.concord.divergence.InMemoryIncidentStore;
import dev.concord.delivery.DeliveryWorker;
import dev.concord.engine.StateEngine;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.InMemoryPaymentStore;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.local.LocalIngestServer;
import dev.concord.ingest.queue.IngestedEvent;
import dev.concord.resolver.Resolver;
import dev.concord.resolver.ResolverConfig;
import dev.concord.resolver.merchant.HttpMerchantOrders;
import dev.concord.resolver.merchant.MerchantOrders;
import dev.concord.resolver.state.InMemoryResolverStateStore;
import dev.concord.resolver.status.GatewayEndpoint;
import dev.concord.resolver.status.HttpGatewayClient;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The whole of Concord in one process, in memory: ingestion, state engine, resolver and delivery.
 * For local development and demos; in AWS each part runs separately on DynamoDB, SQS and Streams.
 *
 * <pre>
 * GATEWAY_&lt;X&gt;_SECRETS       webhook secrets (enables ingestion for gateway X)
 * GATEWAY_&lt;X&gt;_STATUS_URL    status API base URL (enables the resolver for gateway X); GATEWAY_&lt;X&gt;_API_KEY
 * MERCHANT_URL               merchant base URL: events go to /concord/events, order lookups to /orders/{id}
 * CONCORD_DELIVERY_SECRET    signs deliveries to the merchant
 * RESOLVER_PROFILE           "demo" (seconds) or "default" (minutes)
 *
 * POST /webhooks/{a|b|c}            ingestion
 * POST /payments/register           {gateway, paymentId, orderId, amountMinor, currency}
 * GET  /local/payments/{gw}/{id}    canonical state, history and outbox
 * GET  /local/stats  /local/engine  /local/resolver  /local/delivery
 * GET  /                            the dashboard (React, no build step)
 * GET  /api/summary  /api/incidents  /api/payments  /api/payments/{gw}/{id}
 * </pre>
 */
public final class LocalConcord {

    private LocalConcord() {
    }

    static final DetectorConfig DETECTOR_DEMO = new DetectorConfig(Duration.ofSeconds(5), Duration.ofSeconds(3),
            Duration.ofDays(1), Duration.ofSeconds(60), 200);

    static final ResolverConfig DEMO = new ResolverConfig(Duration.ofSeconds(5), Duration.ofSeconds(20),
            Duration.ofSeconds(10), Duration.ofSeconds(60), Duration.ofSeconds(5), Duration.ofHours(1),
            Duration.ofSeconds(5), Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMinutes(30), 500);

    public static void main(String[] args) throws IOException {
        Map<String, String> env = System.getenv();
        ObjectMapper json = new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        Clock clock = Clock.systemUTC();
        IngestConfig config = IngestConfig.fromEnv(env);
        int port = Integer.parseInt(env.getOrDefault("PORT", "8080"));
        LocalIngestServer server = LocalIngestServer.inMemory(port, config, clock);
        InMemoryPaymentStore store = new InMemoryPaymentStore();
        Normalizer normalizer = Normalizer.withBundledMappings(json);
        StateEngine engine = new StateEngine(normalizer, store, clock);

        // ingestion queue -> engine
        AtomicInteger consumed = new AtomicInteger();
        Thread.ofPlatform().daemon().name("engine").start(() -> {
            while (true) {
                List<IngestedEvent> batch = server.memory().queue().messagesFrom(consumed.get());
                for (IngestedEvent e : batch) {
                    try {
                        engine.process(e);
                    } catch (RuntimeException ex) {
                        System.err.println("engine failed on " + e.dedupKey() + ": " + ex);
                    }
                    consumed.incrementAndGet();
                }
                sleep(batch.isEmpty() ? 20 : 0);
            }
        });

        // outbox -> merchant
        String merchantUrl = env.get("MERCHANT_URL");
        DeliveryWorker delivery = null;
        List<String> deadLetters = new CopyOnWriteArrayList<>();
        if (merchantUrl != null && !merchantUrl.isBlank()) {
            delivery = new DeliveryWorker(new DeliveryWorker.Settings(merchantUrl + "/concord/events", 12,
                    Duration.ofSeconds(1), Duration.ofMinutes(2), Duration.ofSeconds(5)),
                    new DeliverySigner(env.getOrDefault("CONCORD_DELIVERY_SECRET", "whsec_concord_dev")),
                    (e, why) -> deadLetters.add(e.idempotencyKey() + ": " + why), json, clock);
            store.onOutbox(delivery::submit);
        }

        // resolver
        Map<Gateway, GatewayEndpoint> endpoints = new EnumMap<>(Gateway.class);
        for (Gateway g : Gateway.values()) {
            String url = env.get("GATEWAY_" + g + "_STATUS_URL");
            if (url != null && !url.isBlank()) {
                endpoints.put(g, new GatewayEndpoint(url, env.get("GATEWAY_" + g + "_API_KEY")));
            }
        }
        Map<String, Integer> resolverTotals = new java.util.concurrent.ConcurrentHashMap<>();
        InMemoryResolverStateStore resolverStates = new InMemoryResolverStateStore();
        if (!endpoints.isEmpty()) {
            HttpGatewayClient gateways = new HttpGatewayClient(endpoints, normalizer, json, Duration.ofSeconds(3),
                    HttpGatewayClient.Limits.defaults());
            MerchantOrders orders = merchantUrl == null ? id -> Optional.empty()
                    : new HttpMerchantOrders(merchantUrl, json);
            ResolverConfig rc = "default".equals(env.get("RESOLVER_PROFILE")) ? ResolverConfig.defaults() : DEMO;
            Resolver resolver = new Resolver(engine, normalizer, gateways, gateways, orders, resolverStates, rc, clock);
            Thread.ofPlatform().daemon().name("resolver").start(() -> {
                while (true) {
                    try {
                        resolver.sweep().counts().forEach((k, v) -> resolverTotals.merge(k.name(), v, Integer::sum));
                    } catch (RuntimeException ex) {
                        System.err.println("sweep failed: " + ex);
                    }
                    sleep(1000);
                }
            });
        }

        // divergence detector: outbox changes, merchant change feed, periodic sweep
        InMemoryIncidentStore incidents = new InMemoryIncidentStore();
        DivergenceDetector detector = null;
        Map<String, Integer> detectorTotals = new java.util.concurrent.ConcurrentHashMap<>();
        if (!endpoints.isEmpty() && merchantUrl != null && !merchantUrl.isBlank()) {
            HttpGatewayClient statusForDetector = new HttpGatewayClient(endpoints, normalizer, json, Duration.ofSeconds(3),
                    HttpGatewayClient.Limits.defaults());
            DetectorConfig dc = "default".equals(env.get("RESOLVER_PROFILE")) ? DetectorConfig.defaults() : DETECTOR_DEMO;
            DivergenceDetector d = new DivergenceDetector(engine, statusForDetector, new HttpMerchantOrders(merchantUrl, json),
                    incidents, new DetectorStateStore.InMemory(), dc, clock);
            store.onOutbox(d::onOutbox);
            detector = d;
            Thread.ofPlatform().daemon().name("detector").start(() -> {
                long lastSweep = 0;
                while (true) {
                    try {
                        detectorTotals.merge("merchantChanges", d.pollMerchantChanges(), Integer::sum);
                        detectorTotals.merge("triggeredChecks", d.runDue(), Integer::sum);
                        if (System.currentTimeMillis() - lastSweep > 10_000) {
                            detectorTotals.merge("sweptChecks", d.sweep(), Integer::sum);
                            lastSweep = System.currentTimeMillis();
                        }
                    } catch (RuntimeException ex) {
                        System.err.println("detector failed: " + ex);
                    }
                    sleep(500);
                }
            });
        }

        DeliveryWorker deliveryRef = delivery;
        Api api = new Api(json, store, incidents, resolverStates, resolverTotals, detectorTotals, consumed, deliveryRef, deadLetters);
        server.addHandler("/api/", api::handle);
        server.addHandler("/", Api::serveDashboard);

        server.addHandler("/payments/register", ex -> {
            JsonNode b = json.readTree(ex.getRequestBody().readAllBytes());
            Optional<Gateway> g = Gateway.fromPath(b.path("gateway").asText(null));
            if (g.isEmpty() || !b.hasNonNull("paymentId")) {
                reply(ex, 400, "{\"error\":\"gateway and paymentId are required\"}");
                return;
            }
            var r = engine.register(g.get(), b.get("paymentId").asText(), b.path("orderId").asText(null),
                    b.hasNonNull("amountMinor") ? b.get("amountMinor").asLong() : null, b.path("currency").asText(null));
            reply(ex, 200, "{\"outcome\":\"" + r.outcome() + "\"}");
        });
        server.addHandler("/local/engine", ex -> reply(ex, 200, json.writeValueAsString(Map.of(
                "consumed", consumed.get(), "payments", store.all().size(),
                "needsVerification", store.all().stream().filter(PaymentRecord::needsVerification).count()))));
        server.addHandler("/local/resolver", ex -> reply(ex, 200, json.writeValueAsString(Map.of(
                "totals", resolverTotals, "parked", resolverStates.parked()))));
        final DeliveryWorker d = delivery;
        server.addHandler("/local/delivery", ex -> reply(ex, 200, d == null ? "{\"enabled\":false}" : json.writeValueAsString(Map.of(
                "delivered", d.delivered(), "attempts", d.attempts(), "pending", d.pending(), "deadLettered", deadLetters))));
        server.addHandler("/local/payments/", ex -> {
            String[] parts = ex.getRequestURI().getPath().split("/");
            Optional<Gateway> gateway = parts.length == 5 ? Gateway.fromPath(parts[3]) : Optional.empty();
            Optional<PaymentRecord> rec = gateway.flatMap(g -> store.find(g, parts[4]));
            if (rec.isEmpty()) {
                reply(ex, 404, "{\"error\":\"use /local/payments/{gateway}/{paymentId} for a known payment\"}");
                return;
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("payment", rec.get());
            body.put("history", store.history(gateway.get(), parts[4]));
            body.put("outbox", store.outbox(gateway.get(), parts[4]));
            body.put("resolver", resolverStates.get(rec.get().key()).orElse(null));
            reply(ex, 200, json.writeValueAsString(body));
        });
        System.out.printf("concord (local, in memory) on :%d; ingestion %s; resolver %s; delivery %s; detector %s; dashboard http://localhost:%d/%n",
                server.port(), config.secrets().keySet(), endpoints.keySet(), merchantUrl == null ? "off" : merchantUrl,
                detector == null ? "off" : "on", server.port());
    }

    static void reply(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
