package dev.concord.engine.store;

import dev.concord.engine.machine.CanonicalState;
import java.time.Instant;
import java.util.List;

/**
 * One event in a payment's history ({@code SK=EVT#<dedupKey>}). Every event is recorded, applied or
 * not, so the history explains every decision. Keying by dedup key (not by time) is what makes the
 * consumer idempotent: processing the same event twice fails the "must not exist" condition.
 *
 * @param path states passed through when applied; more than two means some were inferred
 */
public record HistoryEntry(
        String dedupKey,
        String eventType,
        CanonicalState claimedState,
        EventOutcome outcome,
        CanonicalState stateBefore,
        CanonicalState stateAfter,
        long versionAfter,
        List<CanonicalState> path,
        Instant receivedAt,
        Instant occurredAt,
        Instant processedAt,
        String mappingId,
        String archiveKey) {

    public HistoryEntry {
        path = path == null ? List.of() : List.copyOf(path);
    }
}
