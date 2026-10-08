package dev.concord.ingest.queue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Behaves like SQS FIFO for the parts ingestion relies on: a message whose deduplication id was seen
 * in the last five minutes is accepted but silently dropped.
 */
public final class InMemoryFifoQueue implements EventQueue {

    public static final Duration DEDUP_WINDOW = Duration.ofMinutes(5);

    private final Clock clock;
    private final List<IngestedEvent> messages = new ArrayList<>();
    private final Map<String, Instant> seen = new HashMap<>();
    private final AtomicInteger sendCalls = new AtomicInteger();

    public InMemoryFifoQueue(Clock clock) {
        this.clock = clock;
    }

    @Override
    public synchronized void send(IngestedEvent event) {
        sendCalls.incrementAndGet();
        Instant now = clock.instant();
        Instant last = seen.get(event.dedupKey());
        if (last != null && last.plus(DEDUP_WINDOW).isAfter(now)) {
            return;
        }
        seen.put(event.dedupKey(), now);
        messages.add(event);
    }

    public synchronized List<IngestedEvent> messages() {
        return List.copyOf(messages);
    }

    /** How many times send was called, including calls the dedup window absorbed. */
    public int sendCalls() {
        return sendCalls.get();
    }
}
