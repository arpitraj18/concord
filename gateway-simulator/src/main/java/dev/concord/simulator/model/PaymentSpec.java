package dev.concord.simulator.model;

import java.util.List;

/**
 * A payment written by hand in a scenario. Ids, amount and currency are optional and are filled in
 * deterministically from the seed when absent.
 */
public record PaymentSpec(String paymentId, String orderId, Long amount, String currency, List<Step> steps) {
    public PaymentSpec {
        steps = steps == null ? List.of() : List.copyOf(steps);
    }
}
