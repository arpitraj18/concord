package dev.concord.ingest.dedup;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/** Same semantics as the DynamoDB store, for tests and local runs. Atomicity comes from {@code compute}. */
public final class InMemoryDedupStore implements DedupStore {

    private record Entry(String token, boolean enqueued, Instant claimedAt, Instant expiresAt) {
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final Duration lease;
    private final Duration ttl;

    public InMemoryDedupStore(Duration lease, Duration ttl) {
        this.lease = lease;
        this.ttl = ttl;
    }

    @Override
    public ClaimOutcome claim(String key, String token, Instant now) {
        ClaimOutcome[] outcome = new ClaimOutcome[1];
        entries.compute(key, (k, existing) -> {
            Entry fresh = new Entry(token, false, now, now.plus(ttl));
            if (existing == null || !existing.expiresAt().isAfter(now)) {
                outcome[0] = ClaimOutcome.CLAIMED;
                return fresh;
            }
            if (existing.enqueued()) {
                outcome[0] = ClaimOutcome.DUPLICATE;
                return existing;
            }
            if (existing.claimedAt().plus(lease).isBefore(now)) {
                outcome[0] = ClaimOutcome.TAKEN_OVER;
                return fresh;
            }
            outcome[0] = ClaimOutcome.IN_FLIGHT;
            return existing;
        });
        return outcome[0];
    }

    @Override
    public void markEnqueued(String key, String token, Instant now) {
        entries.computeIfPresent(key, (k, e) ->
                e.token().equals(token) ? new Entry(token, true, e.claimedAt(), e.expiresAt()) : e);
    }

    @Override
    public void release(String key, String token) {
        entries.computeIfPresent(key, (k, e) -> e.token().equals(token) && !e.enqueued() ? null : e);
    }

    public int size() {
        return entries.size();
    }
}
