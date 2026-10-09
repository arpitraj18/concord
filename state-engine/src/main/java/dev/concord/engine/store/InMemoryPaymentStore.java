package dev.concord.engine.store;

import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** Same contract as the DynamoDB store, for fast tests and local runs. One lock per payment. */
public final class InMemoryPaymentStore implements PaymentStore {

    private static final class Partition {
        PaymentRecord record;
        final Map<String, HistoryEntry> history = new LinkedHashMap<>();
        final List<OutboxEntry> outbox = new ArrayList<>();
    }

    private final ConcurrentHashMap<String, Partition> partitions = new ConcurrentHashMap<>();
    private final List<Consumer<OutboxEntry>> outboxListeners = new CopyOnWriteArrayList<>();

    /**
     * Called for every committed outbox entry, inside the payment's lock, so entries of one payment
     * arrive in version order. Plays the role DynamoDB Streams plays in AWS.
     */
    public void onOutbox(Consumer<OutboxEntry> listener) {
        outboxListeners.add(listener);
    }

    private Partition partition(String key) {
        return partitions.computeIfAbsent(key, k -> new Partition());
    }

    private static String key(Gateway g, String id) {
        return g + "#" + id;
    }

    @Override
    public Optional<PaymentRecord> find(Gateway gateway, String paymentId) {
        Partition p = partitions.get(key(gateway, paymentId));
        if (p == null) {
            return Optional.empty();
        }
        synchronized (p) {
            return Optional.ofNullable(p.record);
        }
    }

    @Override
    public void commit(Commit commit) {
        Partition p = partition(commit.paymentKey());
        synchronized (p) {
            if (p.history.containsKey(commit.history().dedupKey())) {
                throw new AlreadyProcessedException(commit.history().dedupKey());
            }
            long current = p.record == null ? 0 : p.record.version();
            if (current != commit.expectedVersion()) {
                throw new VersionConflictException(commit.paymentKey());
            }
            commit.newRecord().ifPresent(r -> p.record = r);
            p.history.put(commit.history().dedupKey(), commit.history());
            commit.outbox().ifPresent(o -> {
                p.outbox.add(o);
                outboxListeners.forEach(l -> l.accept(o));
            });
        }
    }

    @Override
    public List<HistoryEntry> history(Gateway gateway, String paymentId) {
        Partition p = partitions.get(key(gateway, paymentId));
        if (p == null) {
            return List.of();
        }
        synchronized (p) {
            return p.history.values().stream()
                    .sorted(Comparator.comparing(HistoryEntry::receivedAt).thenComparing(HistoryEntry::dedupKey))
                    .toList();
        }
    }

    @Override
    public List<OutboxEntry> outbox(Gateway gateway, String paymentId) {
        Partition p = partitions.get(key(gateway, paymentId));
        if (p == null) {
            return List.of();
        }
        synchronized (p) {
            return List.copyOf(p.outbox);
        }
    }

    @Override
    public List<PaymentRecord> findInStateBetween(CanonicalState state, Instant from, Instant to, int limit) {
        return all().stream()
                .filter(r -> r.state() == state && !r.updatedAt().isBefore(from) && r.updatedAt().isBefore(to))
                .sorted(Comparator.comparing(PaymentRecord::updatedAt))
                .limit(limit)
                .toList();
    }

    @Override
    public List<PaymentRecord> findFlagged(int limit) {
        return all().stream().filter(PaymentRecord::needsVerification)
                .sorted(Comparator.comparing(PaymentRecord::updatedAt)).limit(limit).toList();
    }

    @Override
    public List<PaymentRecord> all() {
        Map<String, PaymentRecord> out = new HashMap<>();
        partitions.forEach((k, p) -> {
            synchronized (p) {
                if (p.record != null) {
                    out.put(k, p.record);
                }
            }
        });
        return List.copyOf(out.values());
    }
}
