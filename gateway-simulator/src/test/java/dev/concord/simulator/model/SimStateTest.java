package dev.concord.simulator.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SimStateTest {

    @Test
    void realWorldTransitionsAreForwardOnly() {
        assertThat(SimState.CREATED.canMoveTo(SimState.AUTHORIZED)).isTrue();
        assertThat(SimState.AUTHORIZED.canMoveTo(SimState.CAPTURED)).isTrue();
        assertThat(SimState.CAPTURED.canMoveTo(SimState.SETTLED)).isTrue();
        assertThat(SimState.SETTLED.canMoveTo(SimState.REFUNDED)).isTrue();

        assertThat(SimState.CAPTURED.canMoveTo(SimState.FAILED)).isFalse();
        assertThat(SimState.CAPTURED.canMoveTo(SimState.AUTHORIZED)).isFalse();
        assertThat(SimState.FAILED.canMoveTo(SimState.CAPTURED)).isFalse();
        assertThat(SimState.AUTHORIZED.canMoveTo(SimState.AUTHORIZED)).isFalse();
    }

    @Test
    void terminalStates() {
        assertThat(SimState.FAILED.isTerminal()).isTrue();
        assertThat(SimState.REFUNDED.isTerminal()).isTrue();
        assertThat(SimState.CAPTURED.isTerminal()).isFalse();
    }
}
