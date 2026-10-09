package dev.concord.engine;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.machine.Decision;
import dev.concord.engine.machine.StateMachine;
import dev.concord.engine.mapping.CanonicalEvent;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.AlreadyProcessedException;
import dev.concord.engine.store.Commit;
import dev.concord.engine.store.EventOutcome;
import dev.concord.engine.store.HistoryEntry;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.engine.store.PaymentStore;
import dev.concord.engine.store.VersionConflictException;
import dev.concord.ingest.queue.IngestedEvent;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies one ingested event to its payment: read, decide, write conditionally, retry on conflict.
 *
 * <p>Optimistic concurrency: the write succeeds only if the payment's version is still the one the
 * decision was based on. Conflicts on one payment are rare (SQS FIFO serialises a payment's events),
 * so this is cheaper than locking; the loser simply re-reads and decides again.
 */
public final class StateEngine {

    private static final Logger log = LoggerFactory.getLogger(StateEngine.class);
    static final int MAX_ATTEMPTS = 20;

    private final Normalizer normalizer;
    private final PaymentStore store;
    private final Clock clock;

    public StateEngine(Normalizer normalizer, PaymentStore store, Clock clock) {
        this.normalizer = normalizer;
        this.store = store;
        this.clock = clock;
    }

    public ProcessResult process(IngestedEvent event) {
        return process(normalizer.normalize(event));
    }

    public ProcessResult process(CanonicalEvent event) {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            PaymentRecord current = store.find(event.gateway(), event.paymentId())
                    .orElse(PaymentRecord.unseen(event.gateway(), event.paymentId()));
            Commit commit = decide(current, event, clock.instant());
            try {
                store.commit(commit);
                return new ProcessResult(commit.history().outcome(), commit.newRecord().orElse(current), attempt);
            } catch (AlreadyProcessedException e) {
                return new ProcessResult(null, current, attempt);
            } catch (VersionConflictException e) {
                backoff(attempt);
            }
        }
        throw new IllegalStateException("gave up after " + MAX_ATTEMPTS + " version conflicts on " + event.paymentKey());
    }

    /** Pure: the commit for one event against one snapshot of the payment. */
    static Commit decide(PaymentRecord current, CanonicalEvent event, Instant now) {
        String key = event.paymentKey();
        if (event.claimedState() == null) {
            return new Commit(key, current.version(), Optional.empty(),
                    history(event, EventOutcome.UNMAPPED, current, current, List.of(), now), Optional.empty());
        }

        Decision d = StateMachine.decide(current.state(), event.claimedState());
        return switch (d.kind()) {
            case APPLY -> {
                PaymentRecord next = new PaymentRecord(current.gateway(), current.paymentId(), d.to(),
                        current.version() + 1,
                        firstNonNull(current.amountMinor(), event.amountMinor()),
                        firstNonNull(current.currency(), event.currency()),
                        firstNonNull(current.orderId(), event.orderId()),
                        current.needsVerification(), current.verificationReason(),
                        current.exists() ? current.createdAt() : now, now);
                OutboxEntry outbox = new OutboxEntry(key, next.version(), d.from(), d.to(),
                        key + "#" + d.to() + "#" + next.version(), now);
                yield new Commit(key, current.version(), Optional.of(next),
                        history(event, EventOutcome.APPLIED, current, next, d.path(), now), Optional.of(outbox));
            }
            case CONTRADICTION -> {
                String reason = "gateway claimed " + event.claimedState() + " (" + event.eventType() + ", "
                        + event.dedupKey() + ") while payment is " + current.state();
                if (current.needsVerification()) {
                    reason = current.verificationReason();
                }
                PaymentRecord flagged = new PaymentRecord(current.gateway(), current.paymentId(), current.state(),
                        current.version() + 1, current.amountMinor(), current.currency(), current.orderId(),
                        true, reason, current.exists() ? current.createdAt() : now, now);
                log.warn("contradiction on {}: {}", key, reason);
                yield new Commit(key, current.version(), Optional.of(flagged),
                        history(event, EventOutcome.CONTRADICTION, current, flagged, List.of(), now), Optional.empty());
            }
            case SAME -> new Commit(key, current.version(), Optional.empty(),
                    history(event, EventOutcome.SAME, current, current, List.of(), now), Optional.empty());
            case STALE -> new Commit(key, current.version(), Optional.empty(),
                    history(event, EventOutcome.STALE, current, current, List.of(), now), Optional.empty());
        };
    }

    private static HistoryEntry history(CanonicalEvent e, EventOutcome outcome, PaymentRecord before,
                                        PaymentRecord after, List<CanonicalState> path, Instant now) {
        return new HistoryEntry(e.dedupKey(), e.eventType(), e.claimedState(), outcome, before.state(), after.state(),
                after.version(), path, e.receivedAt(), e.occurredAt(), now, e.mappingId(), e.archiveKey());
    }

    private static <T> T firstNonNull(T a, T b) {
        return a != null ? a : b;
    }

    private static void backoff(int attempt) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(0, Math.min(50, 1L << attempt)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
