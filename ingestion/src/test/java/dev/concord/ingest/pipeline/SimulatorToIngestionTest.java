package dev.concord.ingest.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.local.LocalIngestServer;
import dev.concord.ingest.queue.IngestedEvent;
import dev.concord.ingest.verify.Secrets;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Phase 1 "done when": 1,000 payments' worth of duplicated, shuffled, partly dropped webhooks from the
 * real simulator result in each unique event enqueued exactly once, and every delivery archived.
 */
class SimulatorToIngestionTest {

    static Map<String, Object> chaosScenario(String targetUrl) {
        return Map.of(
                "name", "phase1-done-when",
                "seed", 20261008,
                "targetUrl", targetUrl,
                "timeScale", 50,
                "generate", Map.of("count", 1000, "spreadMs", 30000, "stepGapMs", 3000),
                "faults", Map.of(
                        "dropRate", 0.03,
                        "duplicateRate", 0.5,
                        "maxExtraCopies", 3,
                        "newEventIdRate", 0.1,
                        "maxDelayMs", 5000,
                        "maxDuplicateDelayMs", 10000,
                        "contradictionRate", 0.02));
    }

    @ParameterizedTest
    @EnumSource(Gateway.class)
    void eachUniqueEventIsEnqueuedExactlyOnceAndEveryDeliveryIsArchived(Gateway gateway) throws Exception {
        String secret = "whsec_" + gateway;
        IngestConfig config = new IngestConfig(Map.of(gateway, Secrets.of(secret)), Duration.ofMinutes(5),
                Duration.ofSeconds(30), Duration.ofDays(7), null, null, null);

        try (LocalIngestServer ingest = LocalIngestServer.inMemory(0, config, Clock.systemUTC());
             SimulatorHarness sim = new SimulatorHarness(gateway.name(), secret)) {
            String target = "http://127.0.0.1:" + ingest.port() + "/webhooks/" + gateway.name().toLowerCase();
            String runId = sim.startRun(chaosScenario(target));
            JsonNode run = sim.awaitCompleted(runId, Duration.ofSeconds(60));
            JsonNode plan = sim.get("/sim/runs/" + runId + "/plan");

            List<JsonNode> attempts = list(run.get("attempts"));
            List<JsonNode> delivered = list(plan.get("deliveries")).stream()
                    .filter(d -> !d.get("dropped").asBoolean()).toList();
            List<IngestedEvent> enqueued = ingest.memory().queue().messages();

            // The scenario really is hostile.
            assertThat(delivered.size()).isGreaterThan(enqueued.size());
            assertThat(attempts.size()).isGreaterThanOrEqualTo(delivered.size());

            // Every planned delivery was eventually acknowledged (IN_FLIGHT 503s were retried).
            Map<Integer, Boolean> acknowledged = new HashMap<>();
            attempts.forEach(a -> acknowledged.merge(a.get("seq").asInt(),
                    a.get("httpStatus").isInt() && a.get("httpStatus").asInt() == 200, Boolean::logicalOr));
            assertThat(acknowledged).hasSize(delivered.size()).doesNotContainValue(false);

            // Every delivery that reached ingestion, duplicates included, is in the raw archive. Attempts that
            // failed at connection level (burst overflowed the accept queue) never reached us; the gateway
            // retried them, which the "acknowledged" check above proves. A request can also be archived and
            // then lose its response, hence a range rather than an equality.
            long responded = attempts.stream().filter(a -> !a.get("httpStatus").isNull()).count();
            assertThat(ingest.memory().archive().size()).isBetween((int) responded, attempts.size());

            // Exactly once: no dedup key twice, and no redundant send calls either.
            Set<String> keys = enqueued.stream().map(IngestedEvent::dedupKey).collect(Collectors.toSet());
            assertThat(keys).hasSize(enqueued.size());
            assertThat(ingest.memory().queue().sendCalls()).isEqualTo(enqueued.size());

            // ...and the enqueued set is exactly the set of distinct events that were delivered.
            if (gateway == Gateway.B) {
                // No event id on the wire: distinct events are distinct (payment, reported state).
                Set<String> expected = delivered.stream()
                        .map(d -> d.get("paymentId").asText() + "|" + d.get("claimedState").asText())
                        .collect(Collectors.toSet());
                assertThat(enqueued).hasSize(expected.size());
            } else {
                Set<String> expected = delivered.stream().map(d -> d.get("eventId").asText()).collect(Collectors.toSet());
                assertThat(enqueued.stream().map(IngestedEvent::gatewayEventId).collect(Collectors.toSet()))
                        .isEqualTo(expected);
            }

            // Per-payment ordering key is the gateway's payment id.
            Set<String> paymentIds = new HashSet<>(delivered.stream().map(d -> d.get("paymentId").asText()).toList());
            assertThat(enqueued).allSatisfy(e -> assertThat(paymentIds).contains(e.paymentId()));
            Map<String, Long> perGroup = enqueued.stream()
                    .collect(Collectors.groupingBy(IngestedEvent::messageGroupId, Collectors.counting()));
            assertThat(perGroup.keySet()).allMatch(g -> g.startsWith(gateway + "#"));
        }
    }

    private static List<JsonNode> list(JsonNode array) {
        return StreamSupport.stream(array.spliterator(), false).map(Function.<JsonNode>identity()).toList();
    }
}
