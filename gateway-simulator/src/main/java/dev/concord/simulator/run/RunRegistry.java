package dev.concord.simulator.run;

import dev.concord.simulator.model.Scenario;
import dev.concord.simulator.plan.DeliveryPlan;
import dev.concord.simulator.plan.PaymentTruth;
import java.time.Clock;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * In-memory store of runs and of which run owns each payment id. If two runs use the same payment
 * id (same seed twice), the most recently created run wins for the status API.
 */
@Component
public class RunRegistry {

    private final Map<String, SimulationRun> runs = new ConcurrentHashMap<>();
    private final Map<String, SimulationRun> byPaymentId = new ConcurrentHashMap<>();
    private final Clock clock;

    public RunRegistry(Clock clock) {
        this.clock = clock;
    }

    public SimulationRun register(Scenario scenario, DeliveryPlan plan) {
        String id = "run_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        SimulationRun run = new SimulationRun(id, scenario, plan, clock.instant());
        runs.put(id, run);
        for (PaymentTruth p : plan.payments()) {
            byPaymentId.put(p.paymentId(), run);
        }
        return run;
    }

    public Optional<SimulationRun> get(String runId) {
        return Optional.ofNullable(runs.get(runId));
    }

    public Optional<SimulationRun> ownerOf(String paymentId) {
        return Optional.ofNullable(byPaymentId.get(paymentId));
    }

    public List<SimulationRun> all() {
        Collection<SimulationRun> values = runs.values();
        return values.stream().sorted(Comparator.comparing(SimulationRun::createdAt).reversed()).toList();
    }
}
