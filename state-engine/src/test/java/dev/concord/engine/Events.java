package dev.concord.engine;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.mapping.CanonicalEvent;
import dev.concord.engine.mapping.EventSource;
import dev.concord.ingest.Gateway;
import java.time.Instant;

public final class Events {

    private Events() {
    }

    public static CanonicalEvent claim(String paymentId, CanonicalState state, String dedupKey, long receivedAtMs) {
        return new CanonicalEvent(Gateway.A, paymentId, dedupKey, "test." + (state == null ? "unmapped" : state),
                state, 49_900L, "INR", "order_1", null, Instant.ofEpochMilli(receivedAtMs), "A.v1", "raw/x", EventSource.WEBHOOK);
    }
}
