package dev.concord.simulator.plan;

import static dev.concord.simulator.plan.Seeds.chance;
import static dev.concord.simulator.plan.Seeds.jitter;

import dev.concord.simulator.dialect.GatewayDialect;
import dev.concord.simulator.model.Directive;
import dev.concord.simulator.model.FaultProfile;
import dev.concord.simulator.model.GeneratedPayments;
import dev.concord.simulator.model.PaymentSpec;
import dev.concord.simulator.model.Scenario;
import dev.concord.simulator.model.ScenarioException;
import dev.concord.simulator.model.SimState;
import dev.concord.simulator.model.Step;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;

/**
 * Turns a scenario into a {@link DeliveryPlan}. Pure function: no clock, no I/O, no shared random
 * state. This is what makes runs reproducible; the dispatcher only executes the plan.
 */
public final class DeliveryPlanner {

    public static final int MAX_PAYMENTS = 100_000;
    private static final long DEFAULT_AMOUNT = 50_000;
    private static final String DEFAULT_CURRENCY = "INR";

    private final GatewayDialect dialect;

    public DeliveryPlanner(GatewayDialect dialect) {
        this.dialect = dialect;
    }

    public DeliveryPlan plan(Scenario scenario) {
        if (scenario.seed() == null) {
            throw new ScenarioException("seed is required for planning");
        }
        long seed = scenario.seed();
        validateFaults(scenario.faults());

        List<PaymentTruth> payments = materialise(scenario, seed);
        validatePayments(payments);
        Map<EventKey, List<Directive>> directives = indexDirectives(scenario.directives(), payments);

        Builder out = new Builder();
        for (int i = 0; i < payments.size(); i++) {
            PaymentTruth payment = payments.get(i);
            SplittableRandom rng = Seeds.rng(seed, "faults", i);
            for (Step step : payment.steps()) {
                String eventId = dialect.newEventId(rng);
                List<Directive> ds = directives.get(new EventKey(payment.paymentId(), step.state()));
                if (ds != null) {
                    applyDirectives(out, payment, step, eventId, ds, rng);
                } else {
                    applyRandomFaults(out, payment, step, eventId, scenario.faults(), rng);
                }
            }
            if (chance(rng, scenario.faults().contradictionRate()) && !payment.steps().isEmpty()) {
                SimState lie = contradictionFor(payment);
                long at = payment.lastStepAtMs() + jitter(rng, scenario.faults().maxDelayMs());
                out.add(payment, lie, dialect.newEventId(rng), PlannedDelivery.Kind.CONTRADICTION, false,
                        payment.lastStepAtMs(), at, "random:contradiction");
            }
        }

        List<PlannedDelivery> deliveries = out.deliveries;
        deliveries.sort(Comparator.comparingLong(PlannedDelivery::deliverAtMs).thenComparingInt(PlannedDelivery::seq));
        return new DeliveryPlan(scenario.name(), seed, dialect.id(), payments, deliveries);
    }

    private void applyRandomFaults(Builder out, PaymentTruth p, Step step, String eventId,
                                   FaultProfile f, SplittableRandom rng) {
        if (chance(rng, f.dropRate())) {
            out.add(p, step.state(), eventId, PlannedDelivery.Kind.ORIGINAL, true, step.atMs(), step.atMs(), "random:drop");
            return;
        }
        long delay = jitter(rng, f.maxDelayMs());
        long deliverAt = step.atMs() + delay;
        out.add(p, step.state(), eventId, PlannedDelivery.Kind.ORIGINAL, false, step.atMs(), deliverAt,
                delay == 0 ? "on-time" : "random:delay+" + delay);
        if (chance(rng, f.duplicateRate())) {
            int copies = 1 + rng.nextInt(f.effectiveMaxExtraCopies());
            for (int c = 0; c < copies; c++) {
                boolean newId = chance(rng, f.newEventIdRate());
                long dupDelay = jitter(rng, f.maxDuplicateDelayMs());
                out.add(p, step.state(), newId ? dialect.newEventId(rng) : eventId, PlannedDelivery.Kind.DUPLICATE,
                        false, step.atMs(), deliverAt + dupDelay,
                        (newId ? "random:duplicate-new-id+" : "random:duplicate+") + dupDelay);
            }
        }
    }

    private void applyDirectives(Builder out, PaymentTruth p, Step step, String eventId,
                                 List<Directive> ds, SplittableRandom rng) {
        boolean dropped = false;
        long deliverAt = step.atMs();
        List<String> causes = new ArrayList<>();
        List<Runnable> extras = new ArrayList<>();
        for (Directive d : ds) {
            long at = step.atMs() + d.delayMs();
            switch (d.action()) {
                case DROP -> {
                    dropped = true;
                    causes.add("directive:DROP");
                }
                case DELAY -> {
                    deliverAt = at;
                    causes.add("directive:DELAY+" + d.delayMs());
                }
                case DUPLICATE -> extras.add(() -> out.add(p, step.state(), eventId, PlannedDelivery.Kind.DUPLICATE,
                        false, step.atMs(), at, "directive:DUPLICATE+" + d.delayMs()));
                case DUPLICATE_NEW_ID -> {
                    String newId = dialect.newEventId(rng);
                    extras.add(() -> out.add(p, step.state(), newId, PlannedDelivery.Kind.DUPLICATE,
                            false, step.atMs(), at, "directive:DUPLICATE_NEW_ID+" + d.delayMs()));
                }
                case CONTRADICT -> {
                    String lieId = dialect.newEventId(rng);
                    extras.add(() -> out.add(p, d.claimedState(), lieId, PlannedDelivery.Kind.CONTRADICTION,
                            false, step.atMs(), at, "directive:CONTRADICT+" + d.delayMs()));
                }
            }
        }
        out.add(p, step.state(), eventId, PlannedDelivery.Kind.ORIGINAL, dropped, step.atMs(), deliverAt,
                causes.isEmpty() ? "on-time" : String.join(",", causes));
        extras.forEach(Runnable::run);
    }

    /** A state the payment never reached, chosen to be as harmful as possible. */
    private SimState contradictionFor(PaymentTruth p) {
        if (p.steps().stream().anyMatch(s -> s.state().impliesMoneyCaptured())) {
            return SimState.FAILED;
        }
        return SimState.CAPTURED;
    }

    private List<PaymentTruth> materialise(Scenario scenario, long seed) {
        List<PaymentTruth> out = new ArrayList<>();
        List<PaymentSpec> specs = scenario.payments();
        for (int i = 0; i < specs.size(); i++) {
            PaymentSpec spec = specs.get(i);
            SplittableRandom rng = Seeds.rng(seed, "ids", i);
            String paymentId = spec.paymentId() != null ? spec.paymentId() : dialect.newPaymentId(rng);
            String orderId = spec.orderId() != null ? spec.orderId() : dialect.newOrderId(rng);
            long amount = spec.amount() != null ? spec.amount() : DEFAULT_AMOUNT;
            String currency = spec.currency() != null ? spec.currency() : DEFAULT_CURRENCY;
            out.add(new PaymentTruth(paymentId, orderId, amount, currency, spec.steps()));
        }

        GeneratedPayments gen = scenario.generate();
        if (gen != null) {
            if (gen.count() < 0 || gen.spreadMs() < 0 || gen.stepGapMs() < 0) {
                throw new ScenarioException("generate.count, spreadMs and stepGapMs must be >= 0");
            }
            if ((long) gen.count() + specs.size() > MAX_PAYMENTS) {
                throw new ScenarioException("at most " + MAX_PAYMENTS + " payments per scenario");
            }
            List<List<SimState>> lifecycles = dialect.lifecycles();
            for (int j = 0; j < gen.count(); j++) {
                SplittableRandom rng = Seeds.rng(seed, "generated", j);
                List<SimState> lifecycle = lifecycles.get(rng.nextInt(lifecycles.size()));
                long t = jitter(rng, gen.spreadMs());
                List<Step> steps = new ArrayList<>();
                for (SimState state : lifecycle) {
                    steps.add(new Step(state, t));
                    t += 1 + jitter(rng, gen.stepGapMs());
                }
                long amount = 100 + rng.nextLong(500_000);
                out.add(new PaymentTruth(dialect.newPaymentId(rng), dialect.newOrderId(rng), amount, DEFAULT_CURRENCY, steps));
            }
        }
        if (out.isEmpty()) {
            throw new ScenarioException("scenario has no payments; add 'payments' or 'generate'");
        }
        return out;
    }

    private void validateFaults(FaultProfile f) {
        requireProbability("faults.dropRate", f.dropRate());
        requireProbability("faults.duplicateRate", f.duplicateRate());
        requireProbability("faults.newEventIdRate", f.newEventIdRate());
        requireProbability("faults.contradictionRate", f.contradictionRate());
        if (f.maxDelayMs() < 0 || f.maxDuplicateDelayMs() < 0 || f.maxExtraCopies() < 0) {
            throw new ScenarioException("fault delays and maxExtraCopies must be >= 0");
        }
        if (f.maxExtraCopies() > 20) {
            throw new ScenarioException("faults.maxExtraCopies must be <= 20");
        }
    }

    static void requireProbability(String field, double p) {
        if (!(p >= 0 && p <= 1)) {
            throw new ScenarioException(field + " must be between 0 and 1, got " + p);
        }
    }

    private void validatePayments(List<PaymentTruth> payments) {
        Set<String> ids = new HashSet<>();
        for (PaymentTruth p : payments) {
            if (!ids.add(p.paymentId())) {
                throw new ScenarioException("duplicate paymentId " + p.paymentId());
            }
            if (p.amount() <= 0) {
                throw new ScenarioException(p.paymentId() + ": amount must be positive");
            }
            if (p.steps().isEmpty()) {
                throw new ScenarioException(p.paymentId() + ": at least one step is required");
            }
            SimState current = SimState.CREATED;
            long lastAt = 0;
            for (Step step : p.steps()) {
                if (!dialect.supports(step.state())) {
                    throw new ScenarioException(p.paymentId() + ": gateway " + dialect.id()
                            + " has no " + step.state() + " state; supported: " + dialect.webhookStates());
                }
                if (!current.canMoveTo(step.state())) {
                    throw new ScenarioException(p.paymentId() + ": illegal real-world transition "
                            + current + " -> " + step.state());
                }
                if (step.atMs() < lastAt) {
                    throw new ScenarioException(p.paymentId() + ": steps must be in time order");
                }
                current = step.state();
                lastAt = step.atMs();
            }
        }
    }

    private Map<EventKey, List<Directive>> indexDirectives(List<Directive> directives, List<PaymentTruth> payments) {
        Map<String, PaymentTruth> byId = new HashMap<>();
        payments.forEach(p -> byId.put(p.paymentId(), p));
        Map<EventKey, List<Directive>> index = new LinkedHashMap<>();
        for (Directive d : directives) {
            if (d.action() == null || d.paymentId() == null || d.state() == null) {
                throw new ScenarioException("directive needs paymentId, state and action: " + d);
            }
            PaymentTruth p = byId.get(d.paymentId());
            if (p == null || !p.everReached(d.state())) {
                throw new ScenarioException("directive targets a webhook that does not exist: "
                        + d.paymentId() + "/" + d.state());
            }
            if (d.delayMs() < 0) {
                throw new ScenarioException("directive delayMs must be >= 0");
            }
            if (d.action() == Directive.Action.CONTRADICT) {
                if (d.claimedState() == null || !dialect.supports(d.claimedState())) {
                    throw new ScenarioException("CONTRADICT needs a claimedState supported by gateway " + dialect.id());
                }
                if (p.everReached(d.claimedState())) {
                    throw new ScenarioException("CONTRADICT claimedState " + d.claimedState()
                            + " really happened for " + p.paymentId() + "; that is not a contradiction");
                }
            }
            index.computeIfAbsent(new EventKey(d.paymentId(), d.state()), k -> new ArrayList<>()).add(d);
        }
        return index;
    }

    private record EventKey(String paymentId, SimState state) {
    }

    private final class Builder {
        final List<PlannedDelivery> deliveries = new ArrayList<>();

        void add(PaymentTruth p, SimState claimed, String eventId, PlannedDelivery.Kind kind, boolean dropped,
                 long happenedAt, long deliverAt, String cause) {
            deliveries.add(new PlannedDelivery(deliveries.size(), p.paymentId(), claimed, dialect.eventType(claimed),
                    eventId, kind, dropped, happenedAt, deliverAt, cause));
        }
    }
}
