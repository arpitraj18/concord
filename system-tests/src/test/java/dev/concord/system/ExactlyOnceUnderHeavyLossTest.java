package dev.concord.system;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.delivery.DeliverySigner;
import dev.concord.delivery.DeliveryWorker;
import dev.concord.delivery.DynamoDbStreamsOutboxReader;
import dev.concord.engine.StateEngine;
import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.aws.PaymentsTable;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.OutboxEntry;
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
import dev.concord.resolver.SweepReport;
import dev.concord.resolver.merchant.HttpMerchantOrders;
import dev.concord.resolver.state.InMemoryResolverStateStore;
import dev.concord.resolver.status.GatewayEndpoint;
import dev.concord.resolver.status.HttpGatewayClient;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Phase 3 "done when": with webhooks dropped at a high rate, every payment still reaches its correct
 * final state, and the merchant's processed-keys table shows zero duplicate effects.
 *
 * <pre>
 * simulator ──webhooks (50% dropped, dup, reordered, 5% lies)──> ingestion ──> queue ──> engine ──> DynamoDB (Local)
 *     ^                                                                                          │ Streams
 *     └── status API (10% errors + an outage) <── resolver (sweeps) ── capture / release         v
 *                                                       │                             delivery worker ──> merchant
 *                                                       └── merchant order adapter ──────────────────────  (PostgreSQL)
 * </pre>
 *
 * Everything real except the clock speed: HTTP between every part, DynamoDB transactions and Streams,
 * PostgreSQL transactions.
 */
class ExactlyOnceUnderHeavyLossTest {

    static final ObjectMapper JSON = new ObjectMapper();

    static final ResolverConfig FAST = new ResolverConfig(
            Duration.ofMillis(300),   // checkAfter
            Duration.ofSeconds(1),    // unknownAfter
            Duration.ofMillis(500),   // settleCheckAfter
            Duration.ofSeconds(3),    // actOnAuthorizedAfter (every planned step is over by then)
            Duration.ofMillis(500),   // verifyFinalAfter
            Duration.ofSeconds(120),  // verifyWindow
            Duration.ofMillis(300),   // confirmDelay
            Duration.ofMillis(100),   // backoffBase
            Duration.ofSeconds(1),    // backoffMax
            Duration.ofSeconds(90),   // parkAfter
            1000);

    @ParameterizedTest
    @EnumSource(Gateway.class)
    void everyPaymentReachesItsTrueFinalStateAndNothingIsDoneTwice(Gateway gateway) throws Exception {
        String inSecret = "whsec_in_" + gateway;
        String outSecret = "whsec_out_" + gateway;
        var local = DynamoDBEmbedded.create();
        var dynamo = local.dynamoDbClient();
        PaymentsTable.create(dynamo, "payments");
        DynamoDbPaymentStore store = new DynamoDbPaymentStore(dynamo, "payments");
        Normalizer normalizer = Normalizer.withBundledMappings(JSON);
        StateEngine engine = new StateEngine(normalizer, store, Clock.systemUTC());
        AtomicBoolean running = new AtomicBoolean(true);
        List<Thread> threads = new ArrayList<>();

        try (EmbeddedMerchant merchant = new EmbeddedMerchant(outSecret);
             SimulatorHarness sim = new SimulatorHarness(gateway.name(), inSecret);
             LocalIngestServer ingest = LocalIngestServer.inMemory(0, new IngestConfig(Map.of(gateway, Secrets.of(inSecret)),
                     Duration.ofMinutes(5), Duration.ofSeconds(30), Duration.ofDays(7), null, null, null), Clock.systemUTC())) {

            Map<String, Object> scenario = Map.of(
                    "name", "phase3-done-when", "seed", 2026_10_10, "timeScale", 20,
                    "targetUrl", "http://127.0.0.1:" + ingest.port() + "/webhooks/" + gateway.name().toLowerCase(),
                    "generate", Map.of("count", 300, "spreadMs", 20000, "stepGapMs", 3000),
                    "faults", Map.of("dropRate", 0.5, "duplicateRate", 0.3, "maxExtraCopies", 2, "newEventIdRate", 0.2,
                            "maxDelayMs", 4000, "maxDuplicateDelayMs", 4000, "contradictionRate", 0.05),
                    "statusApi", Map.of("errorRate", 0.1, "outages", List.of(Map.of("fromMs", 5000, "toMs", 15000))));

            // ---- the merchant creates its orders and registers each payment with Concord, before any webhook ----
            JsonNode plan = sim.plan(scenario);
            int cancelled = 0;
            for (JsonNode p : plan.get("payments")) {
                String paymentId = p.get("paymentId").asText();
                String orderId = p.get("orderId").asText();
                merchant.orders().create(orderId, gateway.name(), paymentId, p.get("amount").asLong(), "INR");
                engine.register(gateway, paymentId, orderId, p.get("amount").asLong(), "INR");
                JsonNode steps = p.get("steps");
                boolean stuckAuthorized = steps.size() == 1 && steps.get(0).get("state").asText().equals("AUTHORIZED");
                if (stuckAuthorized && cancelled++ % 2 == 0) {
                    merchant.orders().cancel(orderId); // the merchant's checkout timeout: release this one
                }
            }

            // ---- Concord's workers ----
            threads.add(Thread.ofPlatform().daemon().start(() -> {
                int consumed = 0;
                while (running.get()) {
                    List<IngestedEvent> batch = ingest.memory().queue().messagesFrom(consumed);
                    batch.forEach(engine::process);
                    consumed += batch.size();
                    sleep(batch.isEmpty() ? 20 : 0);
                }
            }));
            List<String> deadLetters = new CopyOnWriteArrayList<>();
            DeliveryWorker delivery = new DeliveryWorker(new DeliveryWorker.Settings(merchant.baseUrl() + "/concord/events",
                    20, Duration.ofMillis(20), Duration.ofMillis(500), Duration.ofSeconds(3)),
                    new DeliverySigner(outSecret), (e, why) -> deadLetters.add(e.idempotencyKey() + ": " + why), JSON, Clock.systemUTC());
            String streamArn = dynamo.describeTable(b -> b.tableName("payments")).table().latestStreamArn();
            DynamoDbStreamsOutboxReader outbox = new DynamoDbStreamsOutboxReader(local.dynamoDbStreamsClient(), streamArn, delivery::submit);
            threads.add(Thread.ofPlatform().daemon().start(outbox));

            HttpGatewayClient gatewayClient = new HttpGatewayClient(Map.of(gateway, new GatewayEndpoint(sim.baseUrl(), null)),
                    normalizer, JSON, Duration.ofSeconds(2), new HttpGatewayClient.Limits(2000, 50f, 20, Duration.ofSeconds(1)));
            InMemoryResolverStateStore resolverStates = new InMemoryResolverStateStore();
            Resolver resolver = new Resolver(engine, normalizer, gatewayClient, gatewayClient,
                    new HttpMerchantOrders(merchant.baseUrl(), JSON), resolverStates, FAST, Clock.systemUTC());
            Map<SweepReport.Action, Integer> resolverTotals = new ConcurrentHashMap<>();
            threads.add(Thread.ofPlatform().daemon().start(() -> {
                while (running.get()) {
                    resolver.sweep().counts().forEach((k, v) -> resolverTotals.merge(k, v, Integer::sum));
                    sleep(100);
                }
            }));

            // ---- go ----
            String runId = sim.startRun(scenario);
            List<String> mismatches = List.of("not checked yet");
            long deadline = System.nanoTime() + Duration.ofSeconds(80).toNanos();
            int stableRounds = 0;
            while (System.nanoTime() < deadline && stableRounds < 3) {
                sleep(500);
                mismatches = mismatches(sim, runId, store, merchant, gateway, delivery);
                stableRounds = mismatches.isEmpty() ? stableRounds + 1 : 0;
            }
            running.set(false);
            outbox.stop();

            // ---- the delivery worker "crashes and restarts": a fresh reader starts again from the oldest stream
            //      record (positions were in memory), so the merchant gets every event a second time ----
            long effectsBefore = (Long) merchant.stats().get("effects");
            DeliveryWorker restarted = new DeliveryWorker(new DeliveryWorker.Settings(merchant.baseUrl() + "/concord/events",
                    20, Duration.ofMillis(20), Duration.ofMillis(500), Duration.ofSeconds(3)),
                    new DeliverySigner(outSecret), (e, why) -> deadLetters.add(e.idempotencyKey() + ": " + why), JSON, Clock.systemUTC());
            DynamoDbStreamsOutboxReader replay = new DynamoDbStreamsOutboxReader(local.dynamoDbStreamsClient(), streamArn, restarted::submit);
            while (replay.pollOnce() > 0) {
                // drain the stream
            }
            long replayDeadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (restarted.pending() > 0 && System.nanoTime() < replayDeadline) {
                sleep(50);
            }
            assertThat(restarted.delivered()).as("everything was redelivered").isEqualTo(delivery.delivered());
            assertThat(merchant.stats().get("duplicatesAbsorbed")).isEqualTo(restarted.delivered());
            assertThat(merchant.stats().get("effects")).as("redelivery changed nothing").isEqualTo(effectsBefore);
            restarted.close();

            // ---- report ----
            JsonNode run = sim.get("/sim/runs/" + runId);
            long corrections = 0;
            for (PaymentRecord r : store.all()) {
                corrections += store.outbox(gateway, r.paymentId()).stream().filter(o -> o.kind() == OutboxEntry.Kind.CORRECTION).count();
            }
            Map<String, Object> merchantStats = merchant.stats();
            System.out.printf("gateway %s: %d payments; simulator planned %s; resolver %s; %d corrections; delivery %d "
                            + "delivered / %d attempts / %d dead-lettered; merchant %s; parked %d%n",
                    gateway, plan.get("payments").size(), run.get("plan"), resolverTotals, corrections, delivery.delivered(),
                    delivery.attempts(), delivery.deadLettered(), merchantStats, resolverStates.parked().size());

            // ---- the done-when ----
            assertThat(mismatches).as("payments not at their true final state").isEmpty();
            assertThat(merchantStats.get("duplicateEffects")).as("an event with two effects").isEqualTo(0L);
            assertThat(merchantStats.get("ordersFulfilledWrongly")).as("goods shipped twice").isEqualTo(0L);
            assertThat(deadLetters).isEmpty();
            // and the scenario really was hostile:
            assertThat(run.get("plan").get("dropped").asLong()).isGreaterThan(100);
            assertThat(resolverTotals.getOrDefault(SweepReport.Action.APPLIED, 0)).isGreaterThan(50);
            assertThat(resolverTotals.getOrDefault(SweepReport.Action.UNAVAILABLE, 0)).isPositive();
            delivery.close();
        } finally {
            running.set(false);
            for (Thread t : threads) {
                t.join(2000);
            }
            local.shutdown();
        }
    }

    /** Every payment whose Concord state or merchant record does not (yet) match the truth. */
    static List<String> mismatches(SimulatorHarness sim, String runId, DynamoDbPaymentStore store, EmbeddedMerchant merchant,
                                   Gateway gateway, DeliveryWorker delivery) throws Exception {
        List<String> out = new ArrayList<>();
        if (!sim.get("/sim/runs/" + runId).get("status").asText().equals("COMPLETED")) {
            out.add("simulator still sending");
        }
        if (delivery.pending() > 0) {
            out.add(delivery.pending() + " deliveries pending");
        }
        Map<String, String> merchantSeen = new HashMap<>();
        merchant.jdbc().queryForList("SELECT payment_id, payment_state FROM orders").forEach(
                row -> merchantSeen.put((String) row.get("payment_id"), (String) row.get("payment_state")));
        for (JsonNode t : sim.get("/sim/runs/" + runId + "/truth")) {
            String id = t.get("paymentId").asText();
            CanonicalState truth = CanonicalState.valueOf(t.get("finalState").asText());
            PaymentRecord rec = store.find(gateway, id).orElse(null);
            CanonicalState concord = rec == null ? null : rec.state();
            // UNKNOWN is Concord's "pending, but the gateway did not answer in time": it matches a payment still pending.
            boolean stateOk = concord == truth || (concord == CanonicalState.UNKNOWN && truth == CanonicalState.PENDING);
            if (!stateOk || rec.needsVerification()) {
                out.add(id + ": concord " + concord + (rec != null && rec.needsVerification() ? " (flagged)" : "") + ", truth " + truth);
            } else if (!concord.name().equals(merchantSeen.get(id))) {
                out.add(id + ": merchant has " + merchantSeen.get(id) + ", concord " + concord);
            }
        }
        return out;
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
