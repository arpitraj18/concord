package dev.concord.simulator.plan;

import static org.assertj.core.api.Assertions.assertThat;

import dev.concord.simulator.dialect.Dialects;
import dev.concord.simulator.dialect.GatewayDialect;
import dev.concord.simulator.model.FaultProfile;
import dev.concord.simulator.model.GeneratedPayments;
import dev.concord.simulator.model.Scenario;
import dev.concord.simulator.model.SimState;
import dev.concord.simulator.model.Step;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

/** Invariants that must hold for every seed and every fault profile, on every gateway. */
class DeliveryPlannerPropertiesTest {

    @Property(tries = 200)
    void planIsAPureFunctionOfScenario(@ForAll("scenarios") Scenario s, @ForAll("gateways") String gw) {
        GatewayDialect dialect = Dialects.forId(gw);
        assertThat(new DeliveryPlanner(dialect).plan(s)).isEqualTo(new DeliveryPlanner(dialect).plan(s));
    }

    @Property(tries = 200)
    void everyRealStepHasExactlyOneOriginalWebhook(@ForAll("scenarios") Scenario s, @ForAll("gateways") String gw) {
        DeliveryPlan plan = new DeliveryPlanner(Dialects.forId(gw)).plan(s);
        Map<String, Long> originals = plan.deliveries().stream()
                .filter(d -> d.kind() == PlannedDelivery.Kind.ORIGINAL)
                .collect(Collectors.groupingBy(d -> d.paymentId() + "/" + d.claimedState(), Collectors.counting()));
        long realSteps = plan.payments().stream().mapToLong(p -> p.steps().size()).sum();
        assertThat(originals).hasSize((int) realSteps);
        assertThat(originals.values()).allMatch(c -> c == 1);
    }

    @Property(tries = 200)
    void nothingIsDeliveredBeforeItHappens(@ForAll("scenarios") Scenario s, @ForAll("gateways") String gw) {
        DeliveryPlan plan = new DeliveryPlanner(Dialects.forId(gw)).plan(s);
        assertThat(plan.deliveries()).allMatch(d -> d.deliverAtMs() >= d.happenedAtMs());
        assertThat(plan.deliveries()).isSortedAccordingTo(
                Comparator.comparingLong(PlannedDelivery::deliverAtMs).thenComparingInt(PlannedDelivery::seq));
    }

    @Property(tries = 200)
    void onlyContradictionsLie(@ForAll("scenarios") Scenario s, @ForAll("gateways") String gw) {
        DeliveryPlan plan = new DeliveryPlanner(Dialects.forId(gw)).plan(s);
        Map<String, PaymentTruth> truth = plan.paymentsById();
        for (PlannedDelivery d : plan.deliveries()) {
            boolean happened = truth.get(d.paymentId()).everReached(d.claimedState());
            assertThat(happened).as("%s", d).isEqualTo(d.kind() != PlannedDelivery.Kind.CONTRADICTION);
        }
    }

    @Property(tries = 100)
    void generatedLifecyclesAreLegal(@ForAll("scenarios") Scenario s, @ForAll("gateways") String gw) {
        GatewayDialect dialect = Dialects.forId(gw);
        for (PaymentTruth p : new DeliveryPlanner(dialect).plan(s).payments()) {
            SimState current = SimState.CREATED;
            for (Step step : p.steps()) {
                assertThat(current.canMoveTo(step.state())).isTrue();
                assertThat(dialect.supports(step.state())).isTrue();
                current = step.state();
            }
        }
    }

    @Property(tries = 50)
    void noFaultsMeansEverythingOnTimeExactlyOnce(@ForAll long seed, @ForAll @IntRange(min = 1, max = 50) int count,
                                                  @ForAll("gateways") String gw) {
        Scenario s = new Scenario("clean", seed, null, null, FaultProfile.none(), null, null,
                new GeneratedPayments(count, 10_000, 1_000), null);
        DeliveryPlan plan = new DeliveryPlanner(Dialects.forId(gw)).plan(s);
        assertThat(plan.deliveries()).allMatch(d -> d.kind() == PlannedDelivery.Kind.ORIGINAL && !d.dropped()
                && d.deliverAtMs() == d.happenedAtMs());
    }

    @Provide
    Arbitrary<String> gateways() {
        return Arbitraries.of(List.of("A", "B", "C"));
    }

    @Provide
    Arbitrary<Scenario> scenarios() {
        Arbitrary<Double> p = Arbitraries.doubles().between(0, 1);
        Arbitrary<FaultProfile> faults = Combinators.combine(
                        p, p, Arbitraries.integers().between(0, 4), p,
                        Arbitraries.longs().between(0, 10_000), Arbitraries.longs().between(0, 10_000), p)
                .as(FaultProfile::new);
        Arbitrary<GeneratedPayments> gen = Combinators.combine(
                        Arbitraries.integers().between(1, 40),
                        Arbitraries.longs().between(0, 20_000),
                        Arbitraries.longs().between(0, 5_000))
                .as(GeneratedPayments::new);
        return Combinators.combine(Arbitraries.longs(), faults, gen)
                .as((seed, f, g) -> new Scenario("prop", seed, null, null, f, null, null, g, null));
    }
}
