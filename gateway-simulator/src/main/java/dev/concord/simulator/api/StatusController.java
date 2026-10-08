package dev.concord.simulator.api;

import dev.concord.simulator.config.SimulatorProperties;
import dev.concord.simulator.dialect.GatewayDialect;
import dev.concord.simulator.model.StatusApiFaults;
import dev.concord.simulator.plan.PaymentTruth;
import dev.concord.simulator.run.RunRegistry;
import dev.concord.simulator.run.SimulationRun;
import java.time.Clock;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * The gateway's status API: the source of truth. It always reflects the real timeline, never the
 * webhook faults, but it can itself be slow or down.
 */
@RestController
public class StatusController {

    private final RunRegistry registry;
    private final GatewayDialect dialect;
    private final SimulatorProperties props;
    private final Clock clock;

    public StatusController(RunRegistry registry, GatewayDialect dialect, SimulatorProperties props, Clock clock) {
        this.registry = registry;
        this.dialect = dialect;
        this.props = props;
        this.clock = clock;
    }

    @GetMapping("/v1/payments/{paymentId}")
    public ResponseEntity<Map<String, Object>> status(
            @PathVariable String paymentId,
            @RequestHeader(value = "Authorization", required = false) String authorization) throws InterruptedException {
        if (props.apiKey() != null && !props.apiKey().isBlank()
                && !("Bearer " + props.apiKey()).equals(authorization)) {
            return error(HttpStatus.UNAUTHORIZED, "invalid api key");
        }
        SimulationRun run = registry.ownerOf(paymentId).orElse(null);
        long now = run == null ? -1 : run.virtualElapsedMs(clock.instant());
        if (run == null || now < 0) {
            return error(HttpStatus.NOT_FOUND, "no such payment");
        }

        StatusApiFaults faults = run.scenario().statusApi();
        if (faults.latencyMs() > 0) {
            Thread.sleep(faults.latencyMs());
        }
        if (faults.inOutage(now) || run.statusApiRandomFailure(faults.errorRate())) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .header("Retry-After", "1")
                    .body(Map.of("error", "status api unavailable"));
        }
        PaymentTruth payment = run.plan().payment(paymentId).orElseThrow();
        return ResponseEntity.ok(dialect.statusBody(payment, payment.stateAt(now), run.startedAt()));
    }

    private static ResponseEntity<Map<String, Object>> error(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(Map.of("error", message));
    }
}
