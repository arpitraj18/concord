package dev.concord.ingest.queue;

/**
 * Hands verified, deduplicated events to the state engine. Implementations must use the dedup key as
 * the queue's own deduplication id, so that a re-send after a crash (see DedupStore) is dropped by the
 * queue itself within its dedup window.
 */
public interface EventQueue {

    void send(IngestedEvent event);
}
