package dev.concord.resolver.status;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.pipeline.SimulatorHarness;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** The status mappings and the client, against the real Phase 0 simulator. */
class HttpGatewayClientTest {

    static final ObjectMapper JSON = new ObjectMapper();
    static final Normalizer NORMALIZER = Normalizer.withBundledMappings(JSON);

    static HttpGatewayClient client(Gateway g, String baseUrl) {
        return new HttpGatewayClient(Map.of(g, new GatewayEndpoint(baseUrl, null)), NORMALIZER, JSON,
                Duration.ofSeconds(2), new HttpGatewayClient.Limits(1000, 50f, 10, Duration.ofSeconds(30)));
    }

    @ParameterizedTest
    @EnumSource(Gateway.class)
    void statusApiOfEveryGatewayMapsToTheTrueCanonicalState(Gateway gateway) throws Exception {
        try (SimulatorHarness sim = new SimulatorHarness(gateway.name(), "s")) {
            String runId = sim.startRun(Map.of("seed", 77, "timeScale", 20, "targetUrl", "http://127.0.0.1:9/unused",
                    "generate", Map.of("count", 80, "spreadMs", 1000, "stepGapMs", 500)));
            Thread.sleep(600); // every planned step has happened
            HttpGatewayClient client = client(gateway, sim.baseUrl());
            int checked = 0;
            for (JsonNode p : sim.get("/sim/runs/" + runId + "/truth")) {
                StatusResult r = client.fetch(gateway, p.get("paymentId").asText());
                assertThat(r).as("payment %s", p).isInstanceOf(StatusResult.Found.class);
                CanonicalState expected = CanonicalState.valueOf(p.get("finalState").asText());
                assertThat(((StatusResult.Found) r).state()).as("payment %s", p).isEqualTo(expected);
                checked++;
            }
            assertThat(checked).isEqualTo(80);
            assertThat(client.fetch(gateway, "no_such_payment")).isInstanceOf(StatusResult.NotFound.class);
        }
    }

    @Test
    void captureAndReleaseGoThroughTheGateway() throws Exception {
        try (SimulatorHarness sim = new SimulatorHarness("A", "s")) {
            sim.startRun(Map.of("seed", 1, "timeScale", 20, "targetUrl", "http://127.0.0.1:9/unused",
                    "payments", List.of(
                            Map.of("paymentId", "pay_keep", "steps", List.of(Map.of("state", "AUTHORIZED", "atMs", 0))),
                            Map.of("paymentId", "pay_drop", "steps", List.of(Map.of("state", "AUTHORIZED", "atMs", 0))))));
            Thread.sleep(100);
            HttpGatewayClient client = client(Gateway.A, sim.baseUrl());
            assertThat(client.capture(Gateway.A, "pay_keep")).isEqualTo(CanonicalState.CAPTURED);
            assertThat(client.release(Gateway.A, "pay_drop")).isEqualTo(CanonicalState.FAILED);
            assertThat(client.capture(Gateway.A, "pay_keep")).as("409: already captured").isNull();
        }
    }

    @Test
    void outageOpensTheCircuitSoTheGatewayIsNotHammered() throws Exception {
        try (SimulatorHarness sim = new SimulatorHarness("C", "s")) {
            sim.startRun(Map.of("seed", 2, "timeScale", 1, "targetUrl", "http://127.0.0.1:9/unused",
                    "payments", List.of(Map.of("paymentId", "pi_down", "steps", List.of(Map.of("state", "PENDING", "atMs", 0)))),
                    "statusApi", Map.of("outages", List.of(Map.of("fromMs", 0, "toMs", 600000)))));
            Thread.sleep(50);
            HttpGatewayClient client = client(Gateway.C, sim.baseUrl());
            for (int i = 0; i < 10; i++) {
                assertThat(client.fetch(Gateway.C, "pi_down")).isInstanceOf(StatusResult.Unavailable.class);
            }
            assertThat(client.circuitState(Gateway.C)).isEqualTo(CircuitBreaker.State.OPEN);
            StatusResult r = client.fetch(Gateway.C, "pi_down");
            assertThat(((StatusResult.Unavailable) r).reason()).contains("circuit open");
        }
    }

    @Test
    void clientSideRateLimitRefusesInsteadOfQueueing() {
        HttpGatewayClient client = new HttpGatewayClient(Map.of(Gateway.A, new GatewayEndpoint("http://127.0.0.1:9", null)),
                NORMALIZER, JSON, Duration.ofMillis(200), new HttpGatewayClient.Limits(2, 50f, 100, Duration.ofSeconds(30)));
        client.fetch(Gateway.A, "x");
        client.fetch(Gateway.A, "x");
        StatusResult third = client.fetch(Gateway.A, "x");
        assertThat(((StatusResult.Unavailable) third).reason()).contains("rate limited");
    }
}
