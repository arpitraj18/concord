package dev.concord.simulator.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.simulator.TestScenarios;
import dev.concord.simulator.crypto.Hmac;
import dev.concord.simulator.dialect.GatewayADialect;
import dev.concord.simulator.model.Scenario;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "simulator.gateway=A",
        "simulator.webhook-secret=test_secret",
        "simulator.retry.initial-backoff-ms=200",
})
class SimulatorEndToEndTest {

    @Autowired
    TestRestTemplate rest;

    @Autowired
    ObjectMapper json;

    RecordingReceiver receiver;

    @BeforeEach
    void startReceiver() {
        receiver = new RecordingReceiver();
    }

    @AfterEach
    void stopReceiver() {
        receiver.close();
    }

    @Test
    void doneWhenScenarioReplaysIdenticallyOverHttp() throws Exception {
        List<String> first = runDoneWhen();
        receiver.reset();
        List<String> second = runDoneWhen();

        assertThat(first).hasSize(3);
        assertThat(first.get(0)).startsWith("payment.captured/");
        assertThat(first.get(1)).startsWith("payment.authorized/");
        assertThat(first.get(2)).isEqualTo(first.get(1));
        assertThat(second).isEqualTo(first);
    }

    private List<String> runDoneWhen() throws Exception {
        Scenario s = TestScenarios.load("phase0-done-when.json");
        String runId = startRun(withTarget(s, 5.0));
        awaitCompleted(runId);

        List<RecordingReceiver.Received> got = receiver.received();
        for (RecordingReceiver.Received r : got) {
            String expected = Hmac.sha256Hex("test_secret", r.body().getBytes(StandardCharsets.UTF_8));
            assertThat(r.header(GatewayADialect.SIGNATURE_HEADER)).isEqualTo(expected);
        }
        return got.stream()
                .map(r -> readTree(r.body()).get("event").asText() + "/" + r.header(GatewayADialect.EVENT_ID_HEADER))
                .toList();
    }

    @Test
    void gatewayRetriesWhenMerchantReturns500() throws Exception {
        receiver.respondWith(500, 503);
        Scenario s = new Scenario("retry", 3L, receiver.url(), 10.0, null, null,
                List.of(new dev.concord.simulator.model.PaymentSpec("pay_retry", null, null, null,
                        List.of(new dev.concord.simulator.model.Step(dev.concord.simulator.model.SimState.FAILED, 0)))),
                null, null);
        String runId = startRun(s);
        JsonNode run = awaitCompleted(runId);

        assertThat(receiver.received()).hasSize(3);
        JsonNode attempts = run.get("attempts");
        assertThat(attempts).hasSize(3);
        assertThat(attempts.get(2).get("httpStatus").asInt()).isEqualTo(200);
        assertThat(attempts.get(2).get("attempt").asInt()).isEqualTo(3);
    }

    @Test
    void statusApiServesTruthEvenWhenWebhookIsDropped() throws Exception {
        Map<String, Object> scenario = Map.of(
                "name", "missed-capture",
                "seed", 11,
                "targetUrl", receiver.url(),
                "timeScale", 10,
                "payments", List.of(Map.of("paymentId", "pay_truth", "steps", List.of(
                        Map.of("state", "AUTHORIZED", "atMs", 0),
                        Map.of("state", "CAPTURED", "atMs", 3000)))),
                "directives", List.of(Map.of("paymentId", "pay_truth", "state", "CAPTURED", "action", "DROP")));
        startRun(scenario);

        assertThat(status("pay_truth").get("status").asText()).isEqualTo("authorized");
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        String state = "";
        while (System.nanoTime() < deadline && !state.equals("captured")) {
            Thread.sleep(50);
            state = status("pay_truth").get("status").asText();
        }
        assertThat(state).isEqualTo("captured");
        assertThat(receiver.received()).extracting(r -> readTree(r.body()).get("event").asText())
                .containsExactly("payment.authorized");
    }

    @Test
    void statusApiOutageReturns503() {
        Map<String, Object> scenario = Map.of(
                "seed", 5,
                "targetUrl", receiver.url(),
                "payments", List.of(Map.of("paymentId", "pay_outage", "steps", List.of(Map.of("state", "AUTHORIZED", "atMs", 0)))),
                "statusApi", Map.of("outages", List.of(Map.of("fromMs", 0, "toMs", 600000))));
        startRun(scenario);

        ResponseEntity<String> r = rest.getForEntity("/v1/payments/pay_outage", String.class);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(rest.getForEntity("/v1/payments/pay_unknown", String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void invalidScenarioIsRejectedWithReason() {
        Map<String, Object> bad = Map.of("seed", 1, "payments", List.of(Map.of("steps", List.of(
                Map.of("state", "CAPTURED", "atMs", 0), Map.of("state", "AUTHORIZED", "atMs", 5)))));
        ResponseEntity<String> r = rest.postForEntity("/sim/runs", bad, String.class);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(r.getBody()).contains("CAPTURED -> AUTHORIZED");

        ResponseEntity<String> typo = rest.postForEntity("/sim/runs", Map.of("seed", 1, "paymnets", List.of()), String.class);
        assertThat(typo.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void dryRunPlanIsDeterministic() {
        Scenario chaos = TestScenarios.load("chaos-1000.json");
        String a = rest.postForObject("/sim/plan", chaos, String.class);
        String b = rest.postForObject("/sim/plan", chaos, String.class);
        assertThat(a).isEqualTo(b).contains("\"payments\"");
    }

    private Scenario withTarget(Scenario s, double timeScale) {
        return new Scenario(s.name(), s.seed(), receiver.url(), timeScale, s.faults(), s.statusApi(), s.payments(),
                s.generate(), s.directives());
    }

    private String startRun(Object scenario) {
        ResponseEntity<String> r = rest.postForEntity("/sim/runs", scenario, String.class);
        assertThat(r.getStatusCode()).as(r.getBody()).isEqualTo(HttpStatus.CREATED);
        return readTree(r.getBody()).get("runId").asText();
    }

    private JsonNode awaitCompleted(String runId) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        JsonNode run = null;
        while (System.nanoTime() < deadline) {
            run = readTree(rest.getForObject("/sim/runs/" + runId, String.class));
            if (run.get("status").asText().equals("COMPLETED")) {
                return run;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("run did not complete: " + run);
    }

    private JsonNode status(String paymentId) {
        return readTree(rest.getForObject("/v1/payments/" + paymentId, String.class));
    }

    private JsonNode readTree(String body) {
        try {
            return json.readTree(body);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
