package dev.concord.simulator.plan;

import dev.concord.simulator.model.SimState;
import dev.concord.simulator.model.Step;
import java.util.List;

/** What really happened to one payment. The status API serves this; webhooks only report it, unreliably. */
public record PaymentTruth(String paymentId, String orderId, long amount, String currency, List<Step> steps) {

    public PaymentTruth {
        steps = List.copyOf(steps);
    }

    public SimState stateAt(long virtualMs) {
        SimState state = SimState.CREATED;
        for (Step step : steps) {
            if (step.atMs() > virtualMs) {
                break;
            }
            state = step.state();
        }
        return state;
    }

    public SimState finalState() {
        return steps.isEmpty() ? SimState.CREATED : steps.getLast().state();
    }

    public long lastStepAtMs() {
        return steps.isEmpty() ? 0 : steps.getLast().atMs();
    }

    public boolean everReached(SimState state) {
        return steps.stream().anyMatch(s -> s.state() == state);
    }
}
