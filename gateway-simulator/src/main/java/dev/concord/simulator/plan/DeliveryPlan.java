package dev.concord.simulator.plan;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The complete, deterministic outcome of planning a scenario: the true payment timelines and every
 * webhook delivery, sorted by delivery time. Two plans from the same scenario and seed are equal.
 */
public record DeliveryPlan(
        String scenarioName,
        long seed,
        String gateway,
        List<PaymentTruth> payments,
        List<PlannedDelivery> deliveries) {

    public DeliveryPlan {
        payments = List.copyOf(payments);
        deliveries = List.copyOf(deliveries);
    }

    public Optional<PaymentTruth> payment(String paymentId) {
        return payments.stream().filter(p -> p.paymentId().equals(paymentId)).findFirst();
    }

    public Map<String, PaymentTruth> paymentsById() {
        return payments.stream().collect(Collectors.toMap(PaymentTruth::paymentId, Function.identity()));
    }

    public List<PlannedDelivery> toSend() {
        return deliveries.stream().filter(d -> !d.dropped()).toList();
    }

    public Summary summary() {
        long originals = deliveries.stream().filter(d -> d.kind() == PlannedDelivery.Kind.ORIGINAL).count();
        long dropped = deliveries.stream().filter(PlannedDelivery::dropped).count();
        long duplicates = deliveries.stream().filter(d -> d.kind() == PlannedDelivery.Kind.DUPLICATE).count();
        long contradictions = deliveries.stream().filter(d -> d.kind() == PlannedDelivery.Kind.CONTRADICTION).count();
        long lastDeliveryAtMs = deliveries.stream().mapToLong(PlannedDelivery::deliverAtMs).max().orElse(0);
        return new Summary(payments.size(), originals, dropped, duplicates, contradictions,
                deliveries.size() - dropped, lastDeliveryAtMs);
    }

    public record Summary(
            int payments,
            long originalWebhooks,
            long dropped,
            long duplicates,
            long contradictions,
            long deliveriesToSend,
            long lastDeliveryAtMs) {
    }
}
