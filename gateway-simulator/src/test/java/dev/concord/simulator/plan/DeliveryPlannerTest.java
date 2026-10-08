package dev.concord.simulator.plan;

import static dev.concord.simulator.plan.PlannedDelivery.Kind.DUPLICATE;
import static dev.concord.simulator.plan.PlannedDelivery.Kind.ORIGINAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import dev.concord.simulator.TestScenarios;
import dev.concord.simulator.dialect.GatewayADialect;
import dev.concord.simulator.dialect.GatewayBDialect;
import dev.concord.simulator.model.Directive;
import dev.concord.simulator.model.FaultProfile;
import dev.concord.simulator.model.GeneratedPayments;
import dev.concord.simulator.model.PaymentSpec;
import dev.concord.simulator.model.Scenario;
import dev.concord.simulator.model.ScenarioException;
import dev.concord.simulator.model.SimState;
import dev.concord.simulator.model.Step;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeliveryPlannerTest {

    private final DeliveryPlanner gatewayA = new DeliveryPlanner(new GatewayADialect());

    @Test
    void phase0DoneWhen_capturedFirst_duplicateAuthorized_droppedSettled() {
        DeliveryPlan plan = gatewayA.plan(TestScenarios.load("phase0-done-when.json"));

        assertThat(plan.deliveries())
                .extracting(PlannedDelivery::eventType, PlannedDelivery::kind, PlannedDelivery::dropped,
                        PlannedDelivery::deliverAtMs)
                .containsExactly(
                        tuple("payment.captured", ORIGINAL, false, 1000L),
                        tuple("payment.authorized", ORIGINAL, false, 4000L),
                        tuple("payment.authorized", DUPLICATE, false, 4500L),
                        tuple("settlement.processed", ORIGINAL, true, 6000L));

        PlannedDelivery authorized = plan.deliveries().get(1);
        PlannedDelivery copy = plan.deliveries().get(2);
        assertThat(copy.eventId()).as("a retry carries the same event id").isEqualTo(authorized.eventId());
        assertThat(plan.payments().getFirst().finalState()).isEqualTo(SimState.SETTLED);
    }

    @Test
    void sameScenarioAndSeedReplaysIdentically() {
        Scenario chaos = TestScenarios.load("chaos-1000.json");
        DeliveryPlan first = gatewayA.plan(chaos);
        DeliveryPlan second = new DeliveryPlanner(new GatewayADialect()).plan(chaos);

        assertThat(second).isEqualTo(first);
        assertThat(first.summary().payments()).isEqualTo(1000);
        assertThat(first.summary().duplicates()).isPositive();
        assertThat(first.summary().dropped()).isPositive();
        assertThat(first.summary().contradictions()).isPositive();
    }

    @Test
    void differentSeedGivesDifferentPlan() {
        Scenario chaos = TestScenarios.load("chaos-1000.json");
        Scenario other = new Scenario(chaos.name(), chaos.seed() + 1, null, null, chaos.faults(), chaos.statusApi(),
                chaos.payments(), chaos.generate(), chaos.directives());
        assertThat(gatewayA.plan(other)).isNotEqualTo(gatewayA.plan(chaos));
    }

    @Test
    void directiveOnOnePaymentDoesNotShiftFaultsOfOthers() {
        FaultProfile faults = new FaultProfile(0.2, 0.5, 2, 0.2, 3000, 3000, 0.1);
        List<PaymentSpec> payments = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            payments.add(new PaymentSpec("pay_" + i, null, null, null,
                    List.of(new Step(SimState.AUTHORIZED, i * 100L), new Step(SimState.CAPTURED, i * 100L + 50))));
        }
        Scenario base = new Scenario("iso", 99L, null, null, faults, null, payments, null, null);
        Scenario withDirective = new Scenario("iso", 99L, null, null, faults, null, payments, null,
                List.of(new Directive("pay_0", SimState.CAPTURED, Directive.Action.DROP, 0, null)));

        List<PlannedDelivery> a = othersThanPay0(gatewayA.plan(base));
        List<PlannedDelivery> b = othersThanPay0(gatewayA.plan(withDirective));
        assertThat(b).extracting(PlannedDelivery::eventId, PlannedDelivery::deliverAtMs, PlannedDelivery::dropped)
                .containsExactlyElementsOf(
                        a.stream().map(d -> tuple(d.eventId(), d.deliverAtMs(), d.dropped())).toList());
    }

    private static List<PlannedDelivery> othersThanPay0(DeliveryPlan plan) {
        return plan.deliveries().stream().filter(d -> !d.paymentId().equals("pay_0")).toList();
    }

    @Test
    void rejectsIllegalRealWorldTransition() {
        Scenario s = single(List.of(new Step(SimState.CAPTURED, 0), new Step(SimState.AUTHORIZED, 10)));
        assertThatThrownBy(() -> gatewayA.plan(s))
                .isInstanceOf(ScenarioException.class)
                .hasMessageContaining("CAPTURED -> AUTHORIZED");
    }

    @Test
    void rejectsStateTheGatewayDoesNotHave() {
        Scenario s = single(List.of(new Step(SimState.AUTHORIZED, 0)));
        assertThatThrownBy(() -> new DeliveryPlanner(new GatewayBDialect()).plan(s))
                .isInstanceOf(ScenarioException.class)
                .hasMessageContaining("gateway B has no AUTHORIZED");
    }

    @Test
    void rejectsStepsOutOfTimeOrder() {
        Scenario s = single(List.of(new Step(SimState.AUTHORIZED, 500), new Step(SimState.CAPTURED, 100)));
        assertThatThrownBy(() -> gatewayA.plan(s)).hasMessageContaining("time order");
    }

    @Test
    void rejectsDirectiveForWebhookThatDoesNotExist() {
        Scenario s = new Scenario("x", 1L, null, null, null, null,
                List.of(new PaymentSpec("pay_1", null, null, null, List.of(new Step(SimState.AUTHORIZED, 0)))),
                null, List.of(new Directive("pay_1", SimState.SETTLED, Directive.Action.DROP, 0, null)));
        assertThatThrownBy(() -> gatewayA.plan(s)).hasMessageContaining("does not exist");
    }

    @Test
    void rejectsContradictionThatIsActuallyTrue() {
        Scenario s = new Scenario("x", 1L, null, null, null, null,
                List.of(new PaymentSpec("pay_1", null, null, null,
                        List.of(new Step(SimState.AUTHORIZED, 0), new Step(SimState.CAPTURED, 10)))),
                null, List.of(new Directive("pay_1", SimState.CAPTURED, Directive.Action.CONTRADICT, 0, SimState.AUTHORIZED)));
        assertThatThrownBy(() -> gatewayA.plan(s)).hasMessageContaining("not a contradiction");
    }

    @Test
    void rejectsProbabilityOutOfRange() {
        Scenario s = new Scenario("x", 1L, null, null, new FaultProfile(1.5, 0, 0, 0, 0, 0, 0), null, null,
                new GeneratedPayments(1, 0, 0), null);
        assertThatThrownBy(() -> gatewayA.plan(s)).hasMessageContaining("dropRate");
    }

    @Test
    void contradictDirectiveSendsAFalseState() {
        Scenario s = new Scenario("x", 1L, null, null, null, null,
                List.of(new PaymentSpec("pay_1", null, null, null,
                        List.of(new Step(SimState.AUTHORIZED, 0), new Step(SimState.CAPTURED, 10)))),
                null, List.of(new Directive("pay_1", SimState.CAPTURED, Directive.Action.CONTRADICT, 200, SimState.FAILED)));
        DeliveryPlan plan = gatewayA.plan(s);
        assertThat(plan.deliveries().getLast())
                .extracting(PlannedDelivery::eventType, PlannedDelivery::kind, PlannedDelivery::deliverAtMs)
                .containsExactly("payment.failed", PlannedDelivery.Kind.CONTRADICTION, 210L);
        assertThat(plan.payments().getFirst().finalState()).isEqualTo(SimState.CAPTURED);
    }

    private static Scenario single(List<Step> steps) {
        return new Scenario("x", 1L, null, null, null, null,
                List.of(new PaymentSpec("pay_1", null, null, null, steps)), null, null);
    }
}
