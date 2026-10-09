package dev.concord.engine;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.engine.machine.Decision;
import dev.concord.engine.machine.StateMachine;
import dev.concord.engine.mapping.CanonicalEvent;
import dev.concord.engine.mapping.EventSource;
import dev.concord.engine.mapping.Normalizer;
import dev.concord.engine.store.AlreadyProcessedException;
import dev.concord.engine.store.Commit;
import dev.concord.engine.store.EventOutcome;
import dev.concord.engine.store.HistoryEntry;
import dev.concord.engine.store.OutboxEntry;
import dev.concord.engine.store.PaymentRecord;
import dev.concord.engine.store.PaymentStore;
import dev.concord.engine.store.VersionConflictException;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.queue.IngestedEvent;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies events to payments: read, decide, write conditionally, retry on conflict.
 *
 * <p>Optimistic concurrency: a write succeeds only if the payment's version is still the one the
 * decision was based on. A webhook and a status-API result racing for the same payment both go through
 * this same conditional write: the first commit wins and the second is re-decided against the new
 * state (usually becoming a no-op). No special handling is needed for the race.
 *
 * <p>Four entry points:
 * <ul>
 *   <li>{@link #process} – webhooks, forward status-API results, timeouts: normal transition rules.</li>
 *   <li>{@link #register} – the merchant announces a payment at order creation.</li>
 *   <li>{@link #verify} – the status API agrees with Concord: clears a contradiction flag.</li>
 *   <li>{@link #correct} – the status API, confirmed twice, says Concord is wrong. The only change that
 *       may leave the normal transitions; always audited and delivered as a correction.</li>
 * </ul>
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

    public PaymentStore store() {
        return store;
    }

    public ProcessResult process(IngestedEvent event) {
        return process(normalizer.normalize(event));
    }

    public ProcessResult process(CanonicalEvent event) {
        return withRetry(event.gateway(), event.paymentId(), current -> decide(current, event, clock.instant()));
    }

    /**
     * Records that the payment exists, before any webhook. Without this, a payment whose webhooks were
     * all dropped would be invisible to Concord and could never be resolved.
     */
    public ProcessResult register(Gateway gateway, String paymentId, String orderId, Long amountMinor, String currency) {
        return withRetry(gateway, paymentId, current -> {
            if (current.exists()) {
                return null;
            }
            Instant now = clock.instant();
            PaymentRecord created = new PaymentRecord(gateway, paymentId, CanonicalState.CREATED, 1, amountMinor,
                    currency, orderId, false, null, now, now);
            HistoryEntry h = internalHistory("REGISTER#" + gateway + "#" + paymentId, "concord.registered",
                    CanonicalState.CREATED, EventOutcome.REGISTERED, current, created, List.of(), now, EventSource.REGISTRATION);
            return new Commit(current.key(), 0, Optional.of(created), h, Optional.empty());
        });
    }

    /** The status API reports {@code observed}, which equals the current state: clear the flag if set. */
    public ProcessResult verify(Gateway gateway, String paymentId, CanonicalState observed, String evidence) {
        return withRetry(gateway, paymentId, current -> {
            if (!current.exists() || current.state() != observed || !current.needsVerification()) {
                return null;
            }
            Instant now = clock.instant();
            PaymentRecord cleared = withState(current, current.state(), false, null, now);
            HistoryEntry h = internalHistory("VERIFY#" + UUID.randomUUID(), "concord.verified: " + evidence, observed,
                    EventOutcome.VERIFIED, current, cleared, List.of(), now, EventSource.STATUS_API);
            return new Commit(current.key(), current.version(), Optional.of(cleared), h, Optional.empty());
        });
    }

    /**
     * Marks a payment for verification from outside (e.g. the divergence detector saw the gateway
     * disagree with Concord). The resolver picks flagged payments up and settles them.
     */
    public ProcessResult flag(Gateway gateway, String paymentId, String reason) {
        return withRetry(gateway, paymentId, current -> {
            if (!current.exists() || current.needsVerification()) {
                return null;
            }
            Instant now = clock.instant();
            PaymentRecord flagged = withState(current, current.state(), true, reason, now);
            HistoryEntry h = internalHistory("FLAG#" + UUID.randomUUID(), "concord.flagged: " + reason, null,
                    EventOutcome.CONTRADICTION, current, flagged, List.of(), now, EventSource.STATUS_API);
            return new Commit(current.key(), current.version(), Optional.of(flagged), h, Optional.empty());
        });
    }

    /**
     * Replaces a wrong state with the status API's. Only applied if the payment is still at
     * {@code expectedVersion} (the version the resolver's two confirmations were made against); if
     * anything happened since, the resolver must look again.
     *
     * @return null outcome if the payment changed since the resolver decided
     */
    public ProcessResult correct(Gateway gateway, String paymentId, CanonicalState truth, long expectedVersion,
                                 String evidence) {
        PaymentRecord current = store.find(gateway, paymentId).orElse(PaymentRecord.unseen(gateway, paymentId));
        if (current.version() != expectedVersion || current.state() == truth) {
            return new ProcessResult(null, current, 1);
        }
        Instant now = clock.instant();
        PaymentRecord corrected = withState(current, truth, false, null, now);
        log.warn("correcting {} from {} to {}: {}", current.key(), current.state(), truth, evidence);
        HistoryEntry h = internalHistory("CORRECT#" + UUID.randomUUID(), "concord.corrected: " + evidence, truth,
                EventOutcome.CORRECTED, current, corrected, List.of(current.state(), truth), now, EventSource.STATUS_API);
        try {
            store.commit(new Commit(current.key(), expectedVersion, Optional.of(corrected), h,
                    Optional.of(OutboxEntry.of(corrected, OutboxEntry.Kind.CORRECTION, current.state(), now))));
            return new ProcessResult(EventOutcome.CORRECTED, corrected, 1);
        } catch (VersionConflictException e) {
            return new ProcessResult(null, store.find(gateway, paymentId).orElse(current), 1);
        }
    }

    private ProcessResult withRetry(Gateway gateway, String paymentId, Function<PaymentRecord, Commit> decision) {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            PaymentRecord current = store.find(gateway, paymentId).orElse(PaymentRecord.unseen(gateway, paymentId));
            Commit commit = decision.apply(current);
            if (commit == null) {
                return new ProcessResult(EventOutcome.SAME, current, attempt);
            }
            try {
                store.commit(commit);
                return new ProcessResult(commit.history().outcome(), commit.newRecord().orElse(current), attempt);
            } catch (AlreadyProcessedException e) {
                return new ProcessResult(null, current, attempt);
            } catch (VersionConflictException e) {
                backoff(attempt);
            }
        }
        throw new IllegalStateException("gave up after " + MAX_ATTEMPTS + " version conflicts on " + gateway + "#" + paymentId);
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
                yield new Commit(key, current.version(), Optional.of(next),
                        history(event, EventOutcome.APPLIED, current, next, d.path(), now),
                        Optional.of(OutboxEntry.of(next, OutboxEntry.Kind.TRANSITION, d.from(), now)));
            }
            case CONTRADICTION -> {
                String reason = current.needsVerification() ? current.verificationReason()
                        : "gateway claimed " + event.claimedState() + " (" + event.eventType() + ", "
                        + event.dedupKey() + ") while payment is " + current.state();
                PaymentRecord flagged = withState(current, current.state(), true, reason, now);
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

    private static PaymentRecord withState(PaymentRecord r, CanonicalState state, boolean flagged, String reason, Instant now) {
        return new PaymentRecord(r.gateway(), r.paymentId(), state, r.version() + 1, r.amountMinor(), r.currency(),
                r.orderId(), flagged, reason, r.exists() ? r.createdAt() : now, now);
    }

    private static HistoryEntry history(CanonicalEvent e, EventOutcome outcome, PaymentRecord before,
                                        PaymentRecord after, List<CanonicalState> path, Instant now) {
        return new HistoryEntry(e.dedupKey(), e.eventType(), e.claimedState(), outcome, before.state(), after.state(),
                after.version(), path, e.receivedAt(), e.occurredAt(), now, e.mappingId(), e.archiveKey(), e.source());
    }

    private static HistoryEntry internalHistory(String key, String type, CanonicalState claimed, EventOutcome outcome,
                                                PaymentRecord before, PaymentRecord after, List<CanonicalState> path,
                                                Instant now, EventSource source) {
        return new HistoryEntry(key, type, claimed, outcome, before.state(), after.state(), after.version(), path,
                now, null, now, "concord", null, source);
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
