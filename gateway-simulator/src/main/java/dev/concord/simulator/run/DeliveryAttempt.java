package dev.concord.simulator.run;

import java.time.Instant;

/** One HTTP attempt to deliver a planned webhook. {@code httpStatus} is null when the request itself failed. */
public record DeliveryAttempt(
        int seq,
        int attempt,
        String paymentId,
        String eventType,
        String eventId,
        String kind,
        Instant sentAt,
        Integer httpStatus,
        String error,
        long latencyMs) {

    public boolean succeeded() {
        return httpStatus != null && httpStatus >= 200 && httpStatus < 300;
    }
}
