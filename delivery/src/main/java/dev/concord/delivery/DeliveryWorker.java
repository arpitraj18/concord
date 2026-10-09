package dev.concord.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.store.OutboxEntry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers outbox entries to the merchant's webhook.
 *
 * <ul>
 *   <li><b>In order per payment</b>: each payment has its own lane and at most one delivery in flight,
 *       so a merchant never sees REFUNDED before CAPTURED. Different payments go in parallel.</li>
 *   <li><b>At least once</b>: an entry leaves its lane only after a 2xx. Failures are retried with
 *       exponential backoff and full jitter (so a merchant coming back up is not hit by a synchronised
 *       burst). After {@code maxAttempts} the entry is dead-lettered and the lane moves on.</li>
 *   <li><b>Exactly-once effect</b> is the merchant's half of the contract: it dedupes on the
 *       idempotency key and ignores versions older than the last one applied.</li>
 *   <li>Entries already delivered (the feed is at-least-once too) are skipped by version.</li>
 * </ul>
 */
public final class DeliveryWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeliveryWorker.class);

    public record Settings(String merchantUrl, int maxAttempts, Duration backoffBase, Duration backoffMax, Duration timeout) {
    }

    private final class Lane {
        final Deque<OutboxEntry> queue = new ArrayDeque<>();
        boolean busy;
        int attempts;
        long lastDelivered;
    }

    private final Settings settings;
    private final DeliverySigner signer;
    private final DeadLetterSink deadLetters;
    private final ObjectMapper json;
    private final Clock clock;
    private final HttpClient http;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Lane> lanes = new ConcurrentHashMap<>();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong attempts = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();
    private final AtomicLong pending = new AtomicLong();

    public DeliveryWorker(Settings settings, DeliverySigner signer, DeadLetterSink deadLetters, ObjectMapper json, Clock clock) {
        this.settings = settings;
        this.signer = signer;
        this.deadLetters = deadLetters;
        this.json = json;
        this.clock = clock;
        this.http = HttpClient.newBuilder().connectTimeout(settings.timeout())
                .executor(Executors.newVirtualThreadPerTaskExecutor()).build();
        this.scheduler = Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());
    }

    /** Called by the outbox feed for every committed entry (in version order per payment). */
    public void submit(OutboxEntry entry) {
        Lane lane = lanes.computeIfAbsent(entry.paymentKey(), k -> new Lane());
        boolean start;
        synchronized (lane) {
            if (entry.version() <= lane.lastDelivered || lane.queue.stream().anyMatch(e -> e.version() == entry.version())) {
                return;
            }
            lane.queue.addLast(entry);
            pending.incrementAndGet();
            start = !lane.busy;
            lane.busy = true;
        }
        if (start) {
            scheduler.execute(() -> deliverHead(lane));
        }
    }

    private void deliverHead(Lane lane) {
        OutboxEntry entry;
        synchronized (lane) {
            entry = lane.queue.peekFirst();
            if (entry == null) {
                lane.busy = false;
                return;
            }
        }
        String failure = send(entry);
        Duration retryIn = null;
        synchronized (lane) {
            if (failure == null) {
                lane.queue.pollFirst();
                lane.lastDelivered = entry.version();
                lane.attempts = 0;
                delivered.incrementAndGet();
                pending.decrementAndGet();
            } else if (++lane.attempts >= settings.maxAttempts()) {
                log.warn("dead-lettering {} after {} attempts: {}", entry.idempotencyKey(), lane.attempts, failure);
                lane.queue.pollFirst();
                lane.attempts = 0;
                deadLettered.incrementAndGet();
                pending.decrementAndGet();
                deadLetters.deadLetter(entry, failure);
            } else {
                retryIn = backoff(lane.attempts);
            }
        }
        if (retryIn == null) {
            scheduler.execute(() -> deliverHead(lane));
        } else {
            scheduler.schedule(() -> deliverHead(lane), retryIn.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** @return null on success, otherwise why it failed */
    private String send(OutboxEntry entry) {
        attempts.incrementAndGet();
        try {
            byte[] body = json.writeValueAsBytes(MerchantEvent.of(entry));
            HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(settings.merchantUrl()))
                    .timeout(settings.timeout())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body));
            signer.headers(body, entry.idempotencyKey(), clock.instant()).forEach(req::header);
            int status = http.send(req.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
            return status >= 200 && status < 300 ? null : "HTTP " + status;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "interrupted";
        } catch (Exception e) {
            return e.getClass().getSimpleName();
        }
    }

    private Duration backoff(int attempt) {
        long base = settings.backoffBase().toMillis();
        long cap = Math.min(settings.backoffMax().toMillis(), base * (1L << Math.min(20, attempt)));
        return Duration.ofMillis(base + ThreadLocalRandom.current().nextLong(Math.max(1, cap - base + 1)));
    }

    public long delivered() {
        return delivered.get();
    }

    public long attempts() {
        return attempts.get();
    }

    public long deadLettered() {
        return deadLettered.get();
    }

    /** Entries accepted but not yet delivered or dead-lettered. */
    public long pending() {
        return pending.get();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
