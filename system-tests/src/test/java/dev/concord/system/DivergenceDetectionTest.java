package dev.concord.system;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.delivery.DeliverySigner;
import dev.concord.delivery.DeliveryWorker;
import dev.concord.delivery.DynamoDbStreamsOutboxReader;
import dev.concord.divergence.DetectorConfig;
import dev.concord.divergence.DivergenceDetector;
import dev.concord.divergence.DivergenceType;
import dev.concord.divergence.DynamoDbDivergenceStores;
import dev.concord.divergence.Incident;
import dev.concord.engine.StateEngine;
import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.aws.PaymentsTable;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.local.LocalIngestServer;
import dev.concord.ingest.pipeline.SimulatorHarness;
import dev.concord.ingest.queue.IngestedEvent;
import dev.concord.ingest.verify.Secrets;
import dev.concord.merchant.EmbeddedMerchant;
import dev.concord.resolver.Resolver;
import dev.concord.resolver.ResolverConfig;
import dev.concord.resolver.merchant.HttpMerchantOrders;
import dev.concord.resolver.state.InMemoryResolverStateStore;
import dev.concord.resolver.status.GatewayEndpoint;
import dev.concord.resolver.status.HttpGatewayClient;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Phase 4 "done when": an injected "missed webhook, order timed out" scenario is flagged with the
 * correct classification within minutes (here: seconds, with scaled timings), next to the two other
 * classic merchant bugs, and with no incident at all on clean payments.
 *
 * <pre>
 *  pay_late:     authorized; capture at 5 s, its webhook dropped; merchant's 3 s checkout timeout cancels the order
 *                -> Concord recovers the capture from the status API -> PAID_BUT_CANCELLED
 *  pay_redirect: authorized only; the shop trusts the browser redirect and marks the order paid
 *                -> UNPAID_BUT_FULFILLED, which resolves itself when the resolver captures the payment
 *  pay_refund:   captured, merchant then loses every event for this order; refund at 10 s
 *                -> REFUNDED_BUT_ACTIVE
 *  60 clean payments -> nothing
 * </pre>
 */
class DivergenceDetectionTest {

    static final ObjectMapper JSON = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();

    static final ResolverConfig RESOLVER = new ResolverConfig(Duration.ofMillis(300), Duration.ofSeconds(1),
            Duration.ofMillis(500), Duration.ofSeconds(8), Duration.ofMillis(500), Duration.ofSeconds(120),
            Duration.ofMillis(300), Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofSeconds(90), 1000);
    static final DetectorConfig DETECTOR = new DetectorConfig(Duration.ofSeconds(1), Duration.ofSeconds(1),
            Duration.ofHours(1), Duration.ofSeconds(30), 50);

    @Test
    void classicMerchantBugsAreFlaggedCorrectlyAndCleanPaymentsAreNot() throws Exception {
        var local = DynamoDBEmbedded.create();
        var dynamo = local.dynamoDbClient();
        PaymentsTable.create(dynamo, "payments");
        DynamoDbPaymentStore store = new DynamoDbPaymentStore(dynamo, "payments");
        DynamoDbDivergenceStores divergenceStores = new DynamoDbDivergenceStores(dynamo, "payments");
        Normalizer normalizer = Normalizer.withBundledMappings(JSON);
        StateEngine engine = new StateEngine(normalizer, store, Clock.systemUTC());
        AtomicBoolean running = new AtomicBoolean(true);
        List<Thread> threads = new ArrayList<>();

        try (EmbeddedMerchant merchant = new EmbeddedMerchant("whsec_out", "--merchant.checkout-timeout-ms=3000",
                     "--merchant.timeout-check-interval-ms=200");
             SimulatorHarness sim = new SimulatorHarness("A", "whsec_in");
             LocalIngestServer ingest = LocalIngestServer.inMemory(0, new IngestConfig(Map.of(Gateway.A, Secrets.of("whsec_in")),
                     Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ofDays(7), null, null, null), Clock.systemUTC())) {

            Map<String, Object> scenario = Map.of(
                    "name", "phase4-done-when", "seed", 2026_10_11, "timeScale", 10,
                    "targetUrl", "http://127.0.0.1:" + ingest.port() + "/webhooks/a",
                    "payments", List.of(
                            Map.of("paymentId", "pay_late", "orderId", "order_late", "amount", 249_900, "steps", List.of(
                                    Map.of("state", "AUTHORIZED", "atMs", 0), Map.of("state", "CAPTURED", "atMs", 50_000))),
                            Map.of("paymentId", "pay_redirect", "orderId", "order_redirect", "amount", 89_900, "steps", List.of(
                                    Map.of("state", "AUTHORIZED", "atMs", 0))),
                            Map.of("paymentId", "pay_refund", "orderId", "order_refund", "amount", 1_499_900, "steps", List.of(
                                    Map.of("state", "AUTHORIZED", "atMs", 0), Map.of("state", "CAPTURED", "atMs", 1000),
                                    Map.of("state", "REFUNDED", "atMs", 100_000)))),
                    "directives", List.of(Map.of("paymentId", "pay_late", "state", "CAPTURED", "action", "DROP")),
                    "generate", Map.of("count", 60, "spreadMs", 8000, "stepGapMs", 2000));

            JsonNode plan = sim.plan(scenario);
            for (JsonNode p : plan.get("payments")) {
                merchant.orders().create(p.get("orderId").asText(), "A", p.get("paymentId").asText(), p.get("amount").asLong(), "INR");
                engine.register(Gateway.A, p.get("paymentId").asText(), p.get("orderId").asText(), p.get("amount").asLong(), "INR");
            }

            // ---- Concord: engine, delivery + detector on the outbox stream, resolver, detector loop ----
            threads.add(Thread.ofPlatform().daemon().start(() -> {
                int consumed = 0;
                while (running.get()) {
                    List<IngestedEvent> batch = ingest.memory().queue().messagesFrom(consumed);
                    batch.forEach(engine::process);
                    consumed += batch.size();
                    ExactlyOnceUnderHeavyLossTest.sleep(batch.isEmpty() ? 20 : 0);
                }
            }));
            HttpGatewayClient gateway = new HttpGatewayClient(Map.of(Gateway.A, new GatewayEndpoint(sim.baseUrl(), null)),
                    normalizer, JSON, Duration.ofSeconds(2), new HttpGatewayClient.Limits(2000, 50f, 20, Duration.ofSeconds(1)));
            HttpMerchantOrders merchantAdapter = new HttpMerchantOrders(merchant.baseUrl(), JSON);
            DivergenceDetector detector = new DivergenceDetector(engine, gateway, merchantAdapter, divergenceStores,
                    divergenceStores, DETECTOR, Clock.systemUTC());
            DeliveryWorker delivery = new DeliveryWorker(new DeliveryWorker.Settings(merchant.baseUrl() + "/concord/events",
                    5, Duration.ofMillis(50), Duration.ofMillis(500), Duration.ofSeconds(3)),
                    new DeliverySigner("whsec_out"), (e, why) -> { }, JSON, Clock.systemUTC());
            String arn = dynamo.describeTable(b -> b.tableName("payments")).table().latestStreamArn();
            DynamoDbStreamsOutboxReader outbox = new DynamoDbStreamsOutboxReader(local.dynamoDbStreamsClient(), arn, e -> {
                delivery.submit(e);
                detector.onOutbox(e);
            });
            threads.add(Thread.ofPlatform().daemon().start(outbox));
            Resolver resolver = new Resolver(engine, normalizer, gateway, gateway, merchantAdapter,
                    new InMemoryResolverStateStore(), RESOLVER, Clock.systemUTC());
            threads.add(Thread.ofPlatform().daemon().start(() -> {
                while (running.get()) {
                    resolver.sweep();
                    ExactlyOnceUnderHeavyLossTest.sleep(100);
                }
            }));
            threads.add(Thread.ofPlatform().daemon().start(() -> {
                long lastSweep = 0;
                while (running.get()) {
                    detector.pollMerchantChanges();
                    detector.runDue();
                    if (System.currentTimeMillis() - lastSweep > 2000) {
                        detector.sweep();
                        lastSweep = System.currentTimeMillis();
                    }
                    ExactlyOnceUnderHeavyLossTest.sleep(100);
                }
            }));

            // ---- go, and inject the merchant-side bugs ----
            String runId = sim.startRun(scenario);
            Instant started = Instant.parse(sim.get("/sim/runs/" + runId).get("startedAt").asText());
            Thread.sleep(1000);
            post(merchant.baseUrl() + "/orders/order_redirect/mark-paid");
            Thread.sleep(1000);
            post(merchant.baseUrl() + "/faults/reject-events/order_refund");

            Instant captureHappened = started.plusMillis(50_000 / 10);
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline && !(raised(divergenceStores, "pay_late", DivergenceType.PAID_BUT_CANCELLED)
                    && raised(divergenceStores, "pay_refund", DivergenceType.REFUNDED_BUT_ACTIVE)
                    && resolved(divergenceStores, "pay_redirect", DivergenceType.UNPAID_BUT_FULFILLED))) {
                Thread.sleep(200);
            }
            running.set(false);
            outbox.stop();

            // ---- the done-when ----
            Incident late = divergenceStores.find("A#pay_late", DivergenceType.PAID_BUT_CANCELLED).orElseThrow();
            Duration latency = Duration.between(captureHappened, late.raisedAt());
            System.out.printf("missed webhook + order timed out: %s %s, raised %d ms after the capture really happened%n",
                    late.severity(), late.type(), latency.toMillis());
            assertThat(late.merchantStatus()).isEqualTo("CANCELLED");
            assertThat(late.gatewayState()).isEqualTo(dev.concord.engine.machine.CanonicalState.CAPTURED);
            assertThat(latency).as("flagged within minutes (here: seconds)").isLessThan(Duration.ofSeconds(30));

            assertThat(raised(divergenceStores, "pay_refund", DivergenceType.REFUNDED_BUT_ACTIVE)).isTrue();
            Incident redirect = divergenceStores.find("A#pay_redirect", DivergenceType.UNPAID_BUT_FULFILLED).orElseThrow();
            assertThat(redirect.status()).as("captured by the resolver, so the divergence went away").isEqualTo(Incident.Status.RESOLVED);

            // No incident on any clean payment, ever (open or resolved).
            List<String> unexpected = new ArrayList<>();
            for (PaymentRecord r : store.all()) {
                if (!List.of("pay_late", "pay_redirect", "pay_refund").contains(r.paymentId())) {
                    divergenceStores.forPayment(r.key()).forEach(i -> unexpected.add(r.paymentId() + " " + i.type()));
                }
            }
            assertThat(unexpected).as("false positives").isEmpty();
            assertThat(store.all()).hasSize(63);
            delivery.close();
        } finally {
            running.set(false);
            for (Thread t : threads) {
                t.join(2000);
            }
            local.shutdown();
        }
    }

    static boolean raised(DynamoDbDivergenceStores stores, String paymentId, DivergenceType type) {
        return stores.find("A#" + paymentId, type).isPresent();
    }

    static boolean resolved(DynamoDbDivergenceStores stores, String paymentId, DivergenceType type) {
        Optional<Incident> i = stores.find("A#" + paymentId, type);
        return i.isPresent() && i.get().status() == Incident.Status.RESOLVED;
    }

    static void post(String url) throws Exception {
        HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).as(url + " -> " + r.body()).isEqualTo(200);
    }
}
