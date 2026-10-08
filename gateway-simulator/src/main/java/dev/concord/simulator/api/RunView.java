package dev.concord.simulator.api;

import dev.concord.simulator.plan.DeliveryPlan;
import dev.concord.simulator.run.DeliveryAttempt;
import dev.concord.simulator.run.RunStatus;
import dev.concord.simulator.run.SimulationRun;
import java.time.Instant;
import java.util.List;

public record RunView(
        String runId,
        String scenario,
        String gateway,
        long seed,
        RunStatus status,
        String targetUrl,
        double timeScale,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        DeliveryPlan.Summary plan,
        int outstandingDeliveries,
        List<DeliveryAttempt> attempts) {

    static RunView of(SimulationRun run, boolean withAttempts) {
        return new RunView(run.id(), run.plan().scenarioName(), run.plan().gateway(), run.plan().seed(), run.status(),
                run.targetUrl(), run.timeScale(), run.createdAt(), run.startedAt(), run.finishedAt(),
                run.plan().summary(), run.outstanding(), withAttempts ? run.attempts() : null);
    }
}
