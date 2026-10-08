package dev.concord.simulator.api;

import dev.concord.simulator.dialect.GatewayDialect;
import dev.concord.simulator.model.Scenario;
import dev.concord.simulator.model.SimState;
import dev.concord.simulator.plan.DeliveryPlan;
import dev.concord.simulator.plan.PaymentTruth;
import dev.concord.simulator.run.RunRegistry;
import dev.concord.simulator.run.RunService;
import dev.concord.simulator.run.SimulationRun;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Control API used by tests and humans to drive the simulated world. Not part of the fake gateway's public surface. */
@RestController
@RequestMapping("/sim")
public class ControlController {

    private final RunService service;
    private final RunRegistry registry;
    private final GatewayDialect dialect;
    private final Clock clock;

    public ControlController(RunService service, RunRegistry registry, GatewayDialect dialect, Clock clock) {
        this.service = service;
        this.registry = registry;
        this.dialect = dialect;
        this.clock = clock;
    }

    @GetMapping("/info")
    public Map<String, Object> info() {
        return Map.of(
                "gateway", dialect.id(),
                "description", dialect.description(),
                "webhookStates", dialect.webhookStates(),
                "eventTypes", dialect.webhookStates().stream().collect(
                        java.util.stream.Collectors.toMap(SimState::name, dialect::eventType)),
                "lifecycles", dialect.lifecycles());
    }

    /** Plans a scenario without running it. Same input + seed returns the same plan, byte for byte. */
    @PostMapping("/plan")
    public DeliveryPlan plan(@RequestBody Scenario scenario) {
        return service.dryRun(scenario);
    }

    @PostMapping("/runs")
    @ResponseStatus(HttpStatus.CREATED)
    public RunView create(@RequestBody Scenario scenario, @RequestParam(defaultValue = "true") boolean start) {
        return RunView.of(service.create(scenario, start), false);
    }

    @GetMapping("/runs")
    public List<RunView> list() {
        return registry.all().stream().map(r -> RunView.of(r, false)).toList();
    }

    @GetMapping("/runs/{runId}")
    public RunView get(@PathVariable String runId) {
        return RunView.of(find(runId), true);
    }

    @GetMapping("/runs/{runId}/plan")
    public DeliveryPlan plan(@PathVariable String runId) {
        return find(runId).plan();
    }

    /** Ground truth for checking Concord's answers: final state and state right now for every payment. */
    @GetMapping("/runs/{runId}/truth")
    public List<Map<String, Object>> truth(@PathVariable String runId) {
        SimulationRun run = find(runId);
        long now = run.virtualElapsedMs(clock.instant());
        return run.plan().payments().stream().map(p -> truthRow(p, now)).toList();
    }

    private static Map<String, Object> truthRow(PaymentTruth p, long now) {
        return Map.of(
                "paymentId", p.paymentId(),
                "orderId", p.orderId(),
                "amount", p.amount(),
                "currentState", now < 0 ? "NOT_STARTED" : p.stateAt(now).name(),
                "finalState", p.finalState().name(),
                "steps", p.steps());
    }

    @PostMapping("/runs/{runId}/start")
    public RunView start(@PathVariable String runId) {
        SimulationRun run = find(runId);
        if (!service.start(run)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "run is " + run.status());
        }
        return RunView.of(run, false);
    }

    @PostMapping("/runs/{runId}/cancel")
    public RunView cancel(@PathVariable String runId) {
        SimulationRun run = find(runId);
        if (!service.cancel(run)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "run is " + run.status());
        }
        return RunView.of(run, false);
    }

    private SimulationRun find(String runId) {
        return registry.get(runId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no run " + runId));
    }
}
