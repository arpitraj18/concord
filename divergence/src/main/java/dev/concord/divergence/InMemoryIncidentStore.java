package dev.concord.divergence;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryIncidentStore implements IncidentStore {

    private final Map<String, Incident> incidents = new ConcurrentHashMap<>();

    private static String key(String paymentKey, DivergenceType type) {
        return paymentKey + "#" + type;
    }

    @Override
    public Optional<Incident> find(String paymentKey, DivergenceType type) {
        return Optional.ofNullable(incidents.get(key(paymentKey, type)));
    }

    @Override
    public void put(Incident incident) {
        incidents.put(key(incident.paymentKey(), incident.type()), incident);
    }

    @Override
    public List<Incident> forPayment(String paymentKey) {
        return incidents.values().stream().filter(i -> i.paymentKey().equals(paymentKey)).toList();
    }

    @Override
    public List<Incident> open(int limit) {
        return incidents.values().stream().filter(Incident::open)
                .sorted(Comparator.comparing(Incident::severity).thenComparingLong(Incident::score).reversed())
                .limit(limit).toList();
    }

    public List<Incident> all() {
        return List.copyOf(incidents.values());
    }
}
