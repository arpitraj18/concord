package dev.concord.engine.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.StateEngine;
import dev.concord.engine.aws.DynamoDbPaymentStore;
import dev.concord.engine.aws.PaymentsTable;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.replay.ArchiveReplayer;
import dev.concord.engine.replay.StateComparison;
import dev.concord.engine.store.InMemoryPaymentStore;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.aws.DynamoDbLocal;
import dev.concord.ingest.local.LocalIngestServer;
import dev.concord.ingest.pipeline.SimulatorHarness;
import dev.concord.ingest.queue.IngestedEvent;
import dev.concord.ingest.verify.Secrets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Phase 2 "done when", part 2, end to end for every gateway:
 * simulator (1,000 payments, duplicated / reordered / dropped / contradicted webhooks)
 * -> ingestion -> FIFO queue -> state engine on DynamoDB (Local).
 *
 * <ul>
 *   <li>Every payment's state is exactly what an independent oracle, computed from the simulator's own
 *       plan, says it must be: the furthest state any delivered webhook claimed, unless the delivered
 *       webhooks contradict each other, in which case it must be flagged.</li>
 *   <li>No payment ever took an illegal step.</li>
 *   <li>Replaying the raw archive into an empty store rebuilds identical state.</li>
 * </ul>
 */
class SimulatorToEngineTest {

    static final ObjectMapper JSON = new ObjectMapper();
    static DynamoDbClient dynamo;

    @BeforeAll
    static void startDynamo() {
        dynamo = DynamoDbLocal.client();
    }

    @AfterAll
    static void stopDynamo() {
        dynamo.close();
    }

    @ParameterizedTest
    @EnumSource(Gateway.class)
    void engineStateMatchesTheOracleAndReplayRebuildsIt(Gateway gateway) throws Exception {
        String secret = "whsec_" + gateway;
        IngestConfig config = new IngestConfig(Map.of(gateway, Secrets.of(secret)), Duration.ofMinutes(5),
                Duration.ofSeconds(30), Duration.ofDays(7), null, null, null);
        String table = "concord-payments-" + gateway;
        PaymentsTable.create(dynamo, table);
        DynamoDbPaymentStore live = new DynamoDbPaymentStore(dynamo, table);
        Normalizer normalizer = Normalizer.withBundledMappings(JSON);

        try (LocalIngestServer ingest = LocalIngestServer.inMemory(0, config, Clock.systemUTC());
             SimulatorHarness sim = new SimulatorHarness(gateway.name(), secret)) {
            String target = "http://127.0.0.1:" + ingest.port() + "/webhooks/" + gateway.name().toLowerCase();
            Map<String, Object> scenario = new HashMap<>(Map.of(
                    "name", "phase2-done-when", "seed", 2026_10_09, "targetUrl", target, "timeScale", 50,
                    "generate", Map.of("count", 1000, "spreadMs", 30000, "stepGapMs", 3000),
                    "faults", Map.of("dropRate", 0.15, "duplicateRate", 0.4, "maxExtraCopies", 2,
                            "newEventIdRate", 0.2, "maxDelayMs", 6000, "maxDuplicateDelayMs", 8000,
                            "contradictionRate", 0.05)));
            String runId = sim.startRun(scenario);
            sim.awaitCompleted(runId, Duration.ofSeconds(60));
            JsonNode plan = sim.get("/sim/runs/" + runId + "/plan");

            // ---- live: drain the queue in FIFO order through the engine ----
            StateEngine engine = new StateEngine(normalizer, live, Clock.systemUTC());
            for (IngestedEvent e : ingest.memory().queue().messages()) {
                engine.process(e);
            }

            // ---- oracle, from the simulator's plan only ----
            Map<String, Set<CanonicalState>> delivered = new HashMap<>();
            Map<String, Long> amounts = new HashMap<>();
            Map<String, CanonicalState> truth = new HashMap<>();
            for (JsonNode p : plan.get("payments")) {
                amounts.put(p.get("paymentId").asText(), p.get("amount").asLong());
                JsonNode steps = p.get("steps");
                truth.put(p.get("paymentId").asText(), CanonicalState.valueOf(steps.get(steps.size() - 1).get("state").asText()));
            }
            for (JsonNode d : plan.get("deliveries")) {
                if (!d.get("dropped").asBoolean()) {
                    delivered.computeIfAbsent(d.get("paymentId").asText(), k -> EnumSet.noneOf(CanonicalState.class))
                            .add(CanonicalState.valueOf(d.get("claimedState").asText()));
                }
            }

            int exact = 0;
            int flagged = 0;
            int behindTruth = 0;
            int aheadOfTruth = 0;
            List<String> wrong = new ArrayList<>();
            for (Map.Entry<String, Set<CanonicalState>> e : delivered.entrySet()) {
                PaymentRecord rec = live.find(gateway, e.getKey()).orElse(null);
                assertThat(rec).as("payment %s has delivered webhooks", e.getKey()).isNotNull();
                boolean contradictory = contradictory(e.getValue());
                if (rec.needsVerification() != contradictory) {
                    wrong.add(e.getKey() + " flag=" + rec.needsVerification() + " claims=" + e.getValue());
                } else if (contradictory) {
                    flagged++;
                    assertThat(e.getValue()).contains(rec.state());
                } else if (rec.state() != furthest(e.getValue())) {
                    wrong.add(e.getKey() + " state=" + rec.state() + " claims=" + e.getValue());
                } else {
                    exact++;
                    CanonicalState t = truth.get(e.getKey());
                    if (rec.state() != t && rec.state().precedes(t)) {
                        behindTruth++;
                    } else if (rec.state() != t) {
                        aheadOfTruth++;
                    }
                }
                assertThat(rec.amountMinor()).as("amount of %s in minor units", e.getKey()).isEqualTo(amounts.get(e.getKey()));
                assertLegalChain(live.outbox(gateway, e.getKey()), rec);
            }
            assertThat(wrong).isEmpty();
            assertThat(live.all()).hasSize(delivered.size());

            // ---- replay the raw archive into an empty store ----
            InMemoryPaymentStore rebuilt = new InMemoryPaymentStore();
            ArchiveReplayer.Report report = new ArchiveReplayer(JSON).replay(
                    (prefix, consumer) -> ingest.memory().archive().objects().forEach((k, v) -> {
                        if (k.startsWith(prefix)) {
                            consumer.accept(k, v);
                        }
                    }),
                    new StateEngine(normalizer, rebuilt, Clock.systemUTC()));
            StateComparison.Result cmp = StateComparison.compare(live.all(), rebuilt.all());

            System.out.printf("gateway %s: %d payments, %d match oracle, %d flagged contradictions; vs truth: %d behind "
                            + "(webhook dropped), %d ahead (lie consistent with a legal path); replay of %d deliveries "
                            + "(%d redeliveries): %d mismatches, %d flagged holding the other claim%n",
                    gateway, delivered.size(), exact, flagged, behindTruth, aheadOfTruth, report.deliveries(), report.redeliveries(),
                    cmp.mismatches().size(), cmp.flaggedHoldingDifferentClaim().size());
            assertThat(cmp.identical()).as("%s", cmp).isTrue();
            assertThat(flagged).as("the scenario does produce contradictions").isPositive();
            // Webhooks alone cannot tell these from the truth; only the status API can (Phases 3-4).
            assertThat(behindTruth + aheadOfTruth).isPositive();
        }
    }

    static boolean contradictory(Set<CanonicalState> claims) {
        for (CanonicalState x : claims) {
            for (CanonicalState y : claims) {
                if (x != y && !x.precedes(y) && !y.precedes(x)) {
                    return true;
                }
            }
        }
        return false;
    }

    static CanonicalState furthest(Set<CanonicalState> claims) {
        CanonicalState max = CanonicalState.CREATED;
        for (CanonicalState s : claims) {
            if (max.precedes(s)) {
                max = s;
            }
        }
        return max;
    }

    static void assertLegalChain(List<OutboxEntry> outbox, PaymentRecord rec) {
        CanonicalState at = CanonicalState.CREATED;
        for (OutboxEntry o : outbox) {
            assertThat(o.fromState()).isEqualTo(at);
            assertThat(at.precedes(o.toState())).isTrue();
            at = o.toState();
        }
        assertThat(at).isEqualTo(rec.state());
    }
}
