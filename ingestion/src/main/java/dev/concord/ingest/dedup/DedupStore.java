package dev.concord.ingest.dedup;

import java.time.Instant;

/**
 * Remembers which events have been enqueued.
 *
 * <p>A plain "insert if absent" is not enough. If ingestion records an event and then crashes before
 * enqueueing it, every retry from the gateway would be treated as a duplicate and the event would be
 * lost forever. So a record is first <b>claimed</b> (with a token and a time), then marked
 * <b>enqueued</b>. A claim that is never completed expires after a lease and can be taken over.
 *
 * <p>All operations are conditional on the claim token, so a slow instance that lost its lease cannot
 * overwrite the record of the instance that took over.
 */
public interface DedupStore {

    ClaimOutcome claim(String dedupKey, String token, Instant now);

    /** Records that the event is enqueued. Only the current claim holder can do this. */
    void markEnqueued(String dedupKey, String token, Instant now);

    /** Gives up a claim after a failed enqueue, so the gateway's retry can proceed at once. */
    void release(String dedupKey, String token);
}
