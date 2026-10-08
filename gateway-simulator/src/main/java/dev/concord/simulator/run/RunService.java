package dev.concord.simulator.run;

import dev.concord.simulator.config.SimulatorProperties;
import dev.concord.simulator.model.Scenario;
import dev.concord.simulator.model.ScenarioException;
import dev.concord.simulator.plan.DeliveryPlan;
import dev.concord.simulator.plan.DeliveryPlanner;
import java.time.Clock;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Service;

@Service
public class RunService {

    private final DeliveryPlanner planner;
    private final RunRegistry registry;
    private final WebhookDispatcher dispatcher;
    private final SimulatorProperties props;
    private final Clock clock;

    public RunService(DeliveryPlanner planner, RunRegistry registry, WebhookDispatcher dispatcher,
                      SimulatorProperties props, Clock clock) {
        this.planner = planner;
        this.registry = registry;
        this.dispatcher = dispatcher;
        this.props = props;
        this.clock = clock;
    }

    /** Fills in seed, target and time scale. A missing seed is chosen at random and reported back so the run can be replayed. */
    public Scenario withDefaults(Scenario scenario) {
        Scenario s = scenario.withDefaults(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE),
                props.defaultTargetUrl(), props.defaultTimeScale());
        if (!(s.timeScale() > 0 && s.timeScale() <= 1000)) {
            throw new ScenarioException("timeScale must be in (0, 1000]");
        }
        if (!s.targetUrl().startsWith("http://") && !s.targetUrl().startsWith("https://")) {
            throw new ScenarioException("targetUrl must be an http(s) URL");
        }
        return s;
    }

    public DeliveryPlan dryRun(Scenario scenario) {
        return planner.plan(withDefaults(scenario));
    }

    public SimulationRun create(Scenario scenario, boolean start) {
        Scenario s = withDefaults(scenario);
        DeliveryPlan plan = planner.plan(s);
        SimulationRun run = registry.register(s, plan);
        if (start) {
            dispatcher.start(run);
        }
        return run;
    }

    public boolean start(SimulationRun run) {
        return dispatcher.start(run);
    }

    public boolean cancel(SimulationRun run) {
        return run.cancel(clock.instant());
    }
}
