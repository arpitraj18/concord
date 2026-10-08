package dev.concord.simulator.model;

import java.util.List;

/**
 * Faults for the gateway's status API (the source of truth Concord falls back to).
 *
 * @param errorRate probability that any single status request fails with 503
 * @param outages   virtual-time windows during which every status request fails with 503
 * @param latencyMs real added latency per request
 */
public record StatusApiFaults(double errorRate, List<Window> outages, long latencyMs) {

    public StatusApiFaults {
        outages = outages == null ? List.of() : List.copyOf(outages);
    }

    public static StatusApiFaults none() {
        return new StatusApiFaults(0, List.of(), 0);
    }

    public boolean inOutage(long virtualMs) {
        return outages.stream().anyMatch(w -> virtualMs >= w.fromMs() && virtualMs < w.toMs());
    }

    public record Window(long fromMs, long toMs) {
    }
}
