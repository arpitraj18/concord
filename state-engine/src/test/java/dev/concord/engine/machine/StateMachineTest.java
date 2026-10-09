package dev.concord.engine.machine;

import static dev.concord.engine.machine.CanonicalState.AUTHORIZED;
import static dev.concord.engine.machine.CanonicalState.CAPTURED;
import static dev.concord.engine.machine.CanonicalState.CREATED;
import static dev.concord.engine.machine.CanonicalState.FAILED;
import static dev.concord.engine.machine.CanonicalState.PENDING;
import static dev.concord.engine.machine.CanonicalState.REFUNDED;
import static dev.concord.engine.machine.CanonicalState.SETTLED;
import static dev.concord.engine.machine.CanonicalState.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StateMachineTest {

    @Test
    void transitionTableMatchesTheDesign() {
        assertThat(CREATED.next()).containsExactly(PENDING);
        assertThat(PENDING.next()).containsExactlyInAnyOrder(AUTHORIZED, UNKNOWN, FAILED);
        assertThat(UNKNOWN.next()).containsExactlyInAnyOrder(AUTHORIZED, CAPTURED, FAILED);
        assertThat(AUTHORIZED.next()).containsExactlyInAnyOrder(CAPTURED, FAILED);
        assertThat(CAPTURED.next()).containsExactlyInAnyOrder(SETTLED, REFUNDED);
        assertThat(SETTLED.next()).containsExactly(REFUNDED);
        assertThat(FAILED.isTerminal()).isTrue();
        assertThat(REFUNDED.isTerminal()).isTrue();
    }

    @Test
    void forwardJumpIsAppliedWithInferredStates() {
        Decision d = StateMachine.decide(CREATED, CAPTURED);
        assertThat(d.kind()).isEqualTo(Decision.Kind.APPLY);
        assertThat(d.path()).containsExactly(CREATED, PENDING, AUTHORIZED, CAPTURED);
        assertThat(d.inferred()).containsExactly(PENDING, AUTHORIZED);
    }

    @Test
    void lateWebhookIsStale() {
        assertThat(StateMachine.decide(CAPTURED, AUTHORIZED).kind()).isEqualTo(Decision.Kind.STALE);
        assertThat(StateMachine.decide(SETTLED, PENDING).kind()).isEqualTo(Decision.Kind.STALE);
        assertThat(StateMachine.decide(FAILED, AUTHORIZED).kind()).isEqualTo(Decision.Kind.STALE);
    }

    @Test
    void failedAfterCapturedIsAContradictionNotATransition() {
        assertThat(StateMachine.decide(CAPTURED, FAILED).kind()).isEqualTo(Decision.Kind.CONTRADICTION);
        assertThat(StateMachine.decide(FAILED, CAPTURED).kind()).isEqualTo(Decision.Kind.CONTRADICTION);
        assertThat(StateMachine.decide(REFUNDED, FAILED).kind()).isEqualTo(Decision.Kind.CONTRADICTION);
    }

    @Test
    void sameStateIsANoOp() {
        assertThat(StateMachine.decide(CAPTURED, CAPTURED).kind()).isEqualTo(Decision.Kind.SAME);
    }

    @Test
    void onlyFailedVersusMoneyMovedIsIncomparable() {
        for (CanonicalState a : CanonicalState.values()) {
            for (CanonicalState b : CanonicalState.values()) {
                boolean incomparable = a != b && !a.precedes(b) && !b.precedes(a);
                boolean expected = (a == FAILED && b.ordinal() >= CAPTURED.ordinal() && b != FAILED)
                        || (b == FAILED && a.ordinal() >= CAPTURED.ordinal() && a != FAILED);
                assertThat(incomparable).as("%s vs %s", a, b).isEqualTo(expected);
            }
        }
    }
}
