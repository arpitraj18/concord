package dev.concord.ingest.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.simulator.GatewaySimulatorApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** Runs the Phase 0 gateway simulator inside the test JVM and drives it over its control API. */
public final class SimulatorHarness implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final ConfigurableApplicationContext app;
    private final String baseUrl;
    private final HttpClient http = HttpClient.newHttpClient();

    public SimulatorHarness(String gateway, String secret) {
        // Command-line arguments, not builder.properties(): those are only defaults and lose to the
        // simulator's own application.yml, which silently made every run gateway A.
        app = new SpringApplicationBuilder(GatewaySimulatorApplication.class).run(
                "--server.port=0",
                "--simulator.gateway=" + gateway,
                "--simulator.webhook-secret=" + secret,
                "--simulator.retry.max-attempts=8",
                "--simulator.retry.initial-backoff-ms=500",
                "--simulator.dev-sink.enabled=false",
                "--spring.main.banner-mode=off");
        baseUrl = "http://127.0.0.1:" + app.getEnvironment().getProperty("local.server.port");
    }

    public String baseUrl() {
        return baseUrl;
    }

    public String startRun(Map<String, Object> scenario) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/sim/runs"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(scenario))).build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 201) {
            throw new IllegalStateException("simulator refused scenario: " + r.body());
        }
        return JSON.readTree(r.body()).get("runId").asText();
    }

    /** Dry run: the plan the same scenario would produce, without sending anything. */
    public JsonNode plan(Map<String, Object> scenario) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/sim/plan"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(scenario))).build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) {
            throw new IllegalStateException("simulator refused scenario: " + r.body());
        }
        return JSON.readTree(r.body());
    }

    public JsonNode get(String path) throws Exception {
        return JSON.readTree(http.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).build(),
                HttpResponse.BodyHandlers.ofString()).body());
    }

    public JsonNode awaitCompleted(String runId, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        JsonNode run;
        do {
            Thread.sleep(100);
            run = get("/sim/runs/" + runId);
            if (run.get("status").asText().equals("COMPLETED")) {
                return run;
            }
        } while (System.nanoTime() < deadline);
        throw new AssertionError("run " + runId + " did not complete; outstanding=" + run.get("outstandingDeliveries"));
    }

    @Override
    public void close() {
        app.close();
    }
}
